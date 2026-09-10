"""The live feed: the hub's fan-out and each session's queue, limits, and close codes, over a
hand-fed bus and in-memory sockets."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Final

import msgspec
import pytest

from tape.api import ClientSession, LiveHub, MarketDirectory, ServeConfig
from tape.api.contract import (
    CLOSE_GOING_AWAY,
    CLOSE_POLICY_VIOLATION,
    CLOSE_TOO_SLOW,
    MAX_CLIENT_MESSAGE_BYTES,
    BookMessage,
    DeltaMessage,
    ErrorMessage,
    HelloMessage,
    Rejection,
    ResyncMessage,
    ServerMessage,
    SnapshotMessage,
    SubscribedMessage,
    TickerMessage,
    TradeMessage,
)
from tape.bus import BusEnvelope, encode_bus_envelope
from tape.events import (
    BookDelta,
    BookRefresh,
    BookSnapshot,
    BusEvent,
    CatalogEntry,
    ConnectionReport,
    Level,
    MarketCatalog,
    Receipt,
    Side,
    StatusReport,
    Ticker,
    Trade,
)
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import NS_PER_S, FrozenClock, Ms, Ns
from tests.fakes import FakeSubscriber

EPOCH: Final = 1_789_000_000_000_000_000
RECEIPT: Final = Receipt(conn_id=2, recv_mono_ns=Ns(1), recv_wall_ns=Ns(EPOCH))
ORIGIN: Final = "http://localhost:5173"
TS: Final = Ms(1_789_000_000_000)

_server_messages: Final = msgspec.json.Decoder(ServerMessage)


def lvl(price: int, count: int) -> Level:
    return Level(PriceE4(price), CountE2(count))


def refresh(
    ticker: str, bids: Sequence[Level] = (lvl(4000, 100),), *, stale: bool = False
) -> BookRefresh:
    return BookRefresh(
        ticker=ticker, ts_ms=TS, receipt=RECEIPT, stale=stale, bids=tuple(bids), asks=()
    )


def delta(ticker: str, price: int, change: int, side: Side = Side.BID) -> BookDelta:
    return BookDelta(
        ticker=ticker,
        ts_ms=Ms(TS + 1),
        receipt=RECEIPT,
        sid=1,
        seq=None,
        side=side,
        price=PriceE4(price),
        delta=change,
    )


def exchange_snapshot(ticker: str, bids: Sequence[Level]) -> BookSnapshot:
    return BookSnapshot(
        ticker=ticker, ts_ms=TS, receipt=RECEIPT, sid=1, seq=1, bids=tuple(bids), asks=()
    )


def trade(ticker: str) -> Trade:
    return Trade(
        ticker=ticker,
        ts_ms=TS,
        receipt=RECEIPT,
        sid=3,
        seq=1,
        trade_id="t-1",
        price=PriceE4(4000),
        count=CountE2(300),
        taker_side=Side.ASK,
        is_block=False,
    )


def ticker_update(ticker: str) -> Ticker:
    return Ticker(
        ticker=ticker,
        ts_ms=TS,
        receipt=RECEIPT,
        sid=1,
        last=PriceE4(4000),
        bid=PriceE4(3900),
        ask=None,
        bid_size=None,
        ask_size=None,
        volume=CountE2(12_300),
        open_interest=CountE2(0),
    )


def catalog(*tickers: str) -> MarketCatalog:
    return MarketCatalog(
        markets=tuple(
            CatalogEntry(
                ticker=ticker,
                series_ticker="KX",
                event_ticker="KX-1",
                volume_24h=CountE2(0),
                close_ts=None,
                showcase=False,
            )
            for ticker in tickers
        )
    )


def snapshot_of(
    ticker: str, bids: Sequence[tuple[int, int]], book: str = "fresh", *, ts_ms: int = TS
) -> SnapshotMessage:
    assert book in ("fresh", "stale")
    return SnapshotMessage(
        ticker=ticker,
        book="fresh" if book == "fresh" else "stale",
        ts_ms=ts_ms,
        bids=tuple((PriceE4(price), CountE2(count)) for price, count in bids),
        asks=(),
    )


def subscribed(tickers: Sequence[str], *rejected: Rejection) -> SubscribedMessage:
    return SubscribedMessage(tickers=tuple(tickers), rejected=rejected)


class FakeSocket:
    """An accepted WebSocket in memory. Setting :attr:`gate` makes every send wait for it."""

    def __init__(self) -> None:
        self.inbound: asyncio.Queue[str | bytes | None] = asyncio.Queue()
        self.sent: list[ServerMessage] = []
        self.closed_with: int | None = None
        self.gate: asyncio.Event | None = None

    async def receive(self) -> str | bytes | None:
        return await self.inbound.get()

    async def send(self, text: str) -> bool:
        if self.gate is not None:
            await self.gate.wait()
        if self.closed_with is not None:
            return False
        self.sent.append(_server_messages.decode(text))
        return True

    async def close(self, code: int) -> None:
        self.closed_with = code


@dataclass
class Client:
    socket: FakeSocket
    session: ClientSession
    task: asyncio.Task[None]
    read: int = 0

    async def say(self, message: object) -> None:
        text = message if isinstance(message, str | bytes) else json.dumps(message)
        await self.socket.inbound.put(text)

    async def subscribe(self, *tickers: str) -> None:
        await self.say({"op": "subscribe", "tickers": list(tickers)})

    async def next(self, count: int) -> list[ServerMessage]:
        """The next ``count`` messages, and nothing more once the queue is empty."""
        await until(lambda: len(self.socket.sent) >= self.read + count)
        await self.flushed()
        messages = self.socket.sent[self.read :]
        self.read = len(self.socket.sent)
        assert len(messages) == count, messages
        return messages

    async def flushed(self) -> None:
        await until(lambda: self.session.queued == 0)
        # One more turn for a reply to a message the reader is still handling.
        await asyncio.sleep(0)
        await until(lambda: self.session.queued == 0)


@dataclass
class Live:
    """A hub fed envelope by envelope, with in-memory clients."""

    max_clients: int = 3
    max_tickers: int = 2
    queue_max: int = 100
    clock: FrozenClock = field(default_factory=lambda: FrozenClock(mono_ns=1))
    requested: list[str] = field(default_factory=list)
    seq: int = 0

    def __post_init__(self) -> None:
        self.subscriber = FakeSubscriber()
        self.hub = LiveHub(
            self.subscriber,
            directory=MarketDirectory(),
            config=ServeConfig(
                allowed_origins=frozenset({ORIGIN}),
                max_clients=self.max_clients,
                max_tickers=self.max_tickers,
                client_queue_max=self.queue_max,
                bus_refresh_s=10,
            ),
            clock=self.clock,
            request_metadata=lambda entries: self.requested.extend(e.ticker for e in entries),
        )

    def publish(self, *events: BusEvent, skip: int = 0, epoch: int = EPOCH) -> None:
        for event in events:
            self.seq += 1 + skip
            skip = 0
            envelope = BusEnvelope(bus_epoch=epoch, bus_seq=self.seq, event=event)
            self.hub.receive(encode_bus_envelope(envelope))

    async def connect(self) -> Client:
        socket = FakeSocket()
        session = self.hub.admit(socket)
        assert session is not None
        client = Client(socket, session, asyncio.create_task(self._serve(session)))
        (hello,) = await client.next(1)
        assert hello == HelloMessage(protocol=1, max_tickers=self.max_tickers, bus_refresh_s=10)
        return client

    async def _serve(self, session: ClientSession) -> None:
        try:
            await session.run()
        finally:
            self.hub.detach(session)


async def until(predicate: Callable[[], bool], *, timeout_s: float = 5.0) -> None:
    async with asyncio.timeout(timeout_s):
        while not predicate():
            await asyncio.sleep(0.001)


async def test_a_subscription_is_answered_with_what_it_accepted_and_what_it_rejected() -> None:
    live = Live(max_tickers=2)
    live.publish(catalog("KXA", "KXB", "KXC"))
    client = await live.connect()

    await client.subscribe("KXA", "KXNOPE", "KXA", "KXB", "KXC")

    assert await client.next(1) == [
        subscribed(
            ["KXA", "KXB"],
            Rejection(ticker="KXNOPE", code="unknown_ticker"),
            Rejection(ticker="KXC", code="too_many_tickers"),
        )
    ]
    assert client.session.subscriptions == ("KXA", "KXB")
    assert live.requested == ["KXA", "KXB"]


async def test_a_book_goes_out_whole_when_it_becomes_known_and_then_as_deltas() -> None:
    live = Live()
    live.publish(catalog("KXA", "KXB"))
    client = await live.connect()
    await client.subscribe("KXA")
    assert await client.next(1) == [subscribed(["KXA"])]

    live.publish(refresh("KXA", [lvl(4000, 100), lvl(3900, 50)]), refresh("KXB"))
    live.publish(delta("KXA", 4100, 25), delta("KXB", 3000, 1), delta("KXA", 3900, -50))

    assert await client.next(3) == [
        snapshot_of("KXA", [(4000, 100), (3900, 50)]),
        DeltaMessage(ticker="KXA", ts_ms=TS + 1, side="bid", price_e4=PriceE4(4100), delta_e2=25),
        DeltaMessage(ticker="KXA", ts_ms=TS + 1, side="bid", price_e4=PriceE4(3900), delta_e2=-50),
    ]


async def test_subscribing_to_known_books_sends_snapshots_only_for_markets_new_to_the_set() -> None:
    live = Live()
    live.publish(catalog("KXA", "KXB"), refresh("KXA"), refresh("KXB", [lvl(100, 1)]))
    client = await live.connect()

    await client.subscribe("KXA")
    assert await client.next(2) == [subscribed(["KXA"]), snapshot_of("KXA", [(4000, 100)])]
    await client.subscribe("KXA", "KXB")
    assert await client.next(2) == [subscribed(["KXA", "KXB"]), snapshot_of("KXB", [(100, 1)])]
    await client.subscribe()
    assert await client.next(1) == [subscribed([])]

    live.publish(delta("KXA", 4000, 1), trade("KXB"))
    await client.flushed()
    assert client.socket.sent[client.read :] == []


async def test_freshness_changes_are_marked_and_a_snapshot_replaces_a_stale_book() -> None:
    live = Live()
    live.publish(catalog("KXA"))
    client = await live.connect()
    await client.subscribe("KXA")
    await client.next(1)

    live.publish(refresh("KXA"))
    live.publish(refresh("KXA", stale=True))
    live.publish(delta("KXA", 4000, 5))  # a stale copy ignores it
    live.publish(exchange_snapshot("KXA", [lvl(4200, 7)]))
    live.publish(exchange_snapshot("KXA", [lvl(4300, 8)]))
    live.publish(refresh("KXA", [lvl(4300, 8)]))  # the same book: nothing to say

    assert await client.next(4) == [
        snapshot_of("KXA", [(4000, 100)]),
        BookMessage(ticker="KXA", book="stale"),
        snapshot_of("KXA", [(4200, 7)]),
        snapshot_of("KXA", [(4300, 8)]),
    ]


async def test_a_stale_book_that_becomes_known_is_sent_as_a_stale_snapshot() -> None:
    live = Live()
    live.publish(catalog("KXA"))
    client = await live.connect()
    await client.subscribe("KXA")
    await client.next(1)

    live.publish(refresh("KXA", stale=True))

    assert await client.next(1) == [snapshot_of("KXA", [(4000, 100)], book="stale")]


async def test_bus_loss_resyncs_followed_books_until_refresh_images_bring_them_back() -> None:
    live = Live()
    live.publish(catalog("KXA", "KXB", "KXC"), refresh("KXA"), refresh("KXB"), refresh("KXC"))
    client = await live.connect()
    await client.subscribe("KXA", "KXB")
    await client.next(3)

    live.publish(ticker_update("KXB"), skip=1)
    live.publish(delta("KXA", 4000, 1))
    live.publish(refresh("KXA", [lvl(4000, 101)]))

    assert await client.next(4) == [
        ResyncMessage(ticker="KXA", reason="bus_loss"),
        ResyncMessage(ticker="KXB", reason="bus_loss"),
        TickerMessage(
            ticker="KXB",
            ts_ms=TS,
            bid_e4=PriceE4(3900),
            ask_e4=None,
            last_e4=PriceE4(4000),
            volume_e2=CountE2(12_300),
        ),
        snapshot_of("KXA", [(4000, 101)]),
    ]
    health = live.hub.bus_health()
    assert (health.epoch, health.last_seq, health.resets, health.missed) == (str(EPOCH), 8, 2, 1)
    assert health.books_known == 1

    live.publish(refresh("KXB"), epoch=EPOCH + 1)
    assert await client.next(2) == [
        ResyncMessage(ticker="KXA", reason="bus_loss"),
        snapshot_of("KXB", [(4000, 100)]),
    ]


async def test_a_copy_that_breaks_a_book_invariant_is_resynced() -> None:
    live = Live()
    live.publish(catalog("KXA"))
    client = await live.connect()
    await client.subscribe("KXA")
    await client.next(1)

    live.publish(refresh("KXA"), delta("KXA", 3000, -1))

    assert await client.next(2) == [
        snapshot_of("KXA", [(4000, 100)]),
        ResyncMessage(ticker="KXA", reason="bus_loss"),
    ]


async def test_trades_and_tickers_are_forwarded_whatever_the_book_and_in_bus_order() -> None:
    live = Live()
    live.publish(catalog("KXA"))
    client = await live.connect()
    await client.subscribe("KXA")
    await client.next(1)

    live.publish(trade("KXA"), ticker_update("KXA"), refresh("KXA"), delta("KXA", 3900, 5))
    live.publish(trade("KXA"), refresh("KXA", [lvl(4000, 100), lvl(3900, 5)], stale=True))
    live.publish(ticker_update("KXA"), exchange_snapshot("KXA", [lvl(1, 1)]), trade("KXA"))

    messages = await client.next(9)
    assert [type(message).__name__ for message in messages] == [
        "TradeMessage",
        "TickerMessage",
        "SnapshotMessage",
        "DeltaMessage",
        "TradeMessage",
        "BookMessage",
        "TickerMessage",
        "SnapshotMessage",
        "TradeMessage",
    ]
    assert messages[0] == TradeMessage(
        ticker="KXA", ts_ms=TS, price_e4=PriceE4(4000), count_e2=CountE2(300), taker_side="ask"
    )


async def test_a_lagging_client_is_resynchronized_and_closed_at_its_third_lag_in_a_minute() -> None:
    live = Live(queue_max=4)
    live.publish(catalog("KXA", "KXB"), refresh("KXA"), refresh("KXB"))
    client = await live.connect()
    await client.subscribe("KXA", "KXB")
    await client.next(3)

    # The client reads nothing: five deltas, offered without a pause, overflow a queue of four.
    gate = client.socket.gate = asyncio.Event()
    for _ in range(5):
        live.publish(delta("KXA", 3000, 1))
    assert client.session.lags == 1
    gate.set()
    assert await client.next(4) == [
        ResyncMessage(ticker="KXA", reason="client_lag"),
        snapshot_of("KXA", [(4000, 100), (3000, 5)], ts_ms=TS + 1),
        ResyncMessage(ticker="KXB", reason="client_lag"),
        snapshot_of("KXB", [(4000, 100)]),
    ]

    # A reply whose offer overflows the queue survives, ahead of the resynchronization.
    gate = client.socket.gate = asyncio.Event()
    live.publish(delta("KXA", 3000, 1))
    await until(lambda: client.session.queued == 0)  # the writer holds it, waiting to send
    for _ in range(4):
        live.publish(delta("KXA", 3000, 1))
    live.clock.advance(59 * NS_PER_S)
    await client.subscribe("KXB")
    await until(lambda: client.session.lags == 2)
    gate.set()
    assert await client.next(4) == [
        DeltaMessage(ticker="KXA", ts_ms=TS + 1, side="bid", price_e4=PriceE4(3000), delta_e2=1),
        subscribed(["KXB"]),
        ResyncMessage(ticker="KXB", reason="client_lag"),
        snapshot_of("KXB", [(4000, 100)]),
    ]

    live.clock.advance(1 * NS_PER_S - 1)
    for _ in range(5):
        live.publish(delta("KXB", 3000, 1))
    assert client.session.close_code == CLOSE_TOO_SLOW
    await asyncio.wait_for(client.task, timeout=5)
    assert client.socket.closed_with == CLOSE_TOO_SLOW
    assert live.hub.clients == 0


async def test_only_lags_within_a_minute_of_each_other_count_toward_closing() -> None:
    live = Live(queue_max=4)
    live.publish(catalog("KXA"), refresh("KXA"))
    client = await live.connect()
    await client.subscribe("KXA")
    await client.next(2)
    client.socket.gate = asyncio.Event()
    for _ in range(5):
        live.publish(delta("KXA", 3000, 1))
    assert client.session.lags == 1  # at 0 s; the queue now holds a resync and a snapshot

    # Each step overflows exactly once: two deltas fill the queue and a third overflows it.
    for advance_s in (30, 61, 9):
        live.clock.advance(advance_s * NS_PER_S)
        for _ in range(3):
            live.publish(delta("KXA", 3000, 1))
    # Lags at 0, 30, 91, and 100 s: never three within one minute.
    assert (client.session.lags, client.session.close_code) == (4, None)

    live.clock.advance(50 * NS_PER_S)
    for _ in range(3):
        live.publish(delta("KXA", 3000, 1))
    assert (client.session.lags, client.session.close_code) == (5, CLOSE_TOO_SLOW)
    client.socket.gate.set()
    await asyncio.wait_for(client.task, timeout=5)


@pytest.mark.parametrize(
    ("frame", "code"),
    [
        ("not json", "malformed_json"),
        (b"\xff", "malformed_json"),
        ('{"op": "unsubscribe", "tickers": []}', "unknown_op"),
        ('{"tickers": ["KXA"]}', "invalid_message"),
        ('{"op": 5}', "invalid_message"),
        ("[1, 2]", "invalid_message"),
        ('{"op": "subscribe", "tickers": "KXA"}', "invalid_message"),
        ("x" * MAX_CLIENT_MESSAGE_BYTES, "malformed_json"),
    ],
)
async def test_a_message_the_server_cannot_accept_is_answered_and_the_connection_stays(
    frame: str | bytes, code: str
) -> None:
    live = Live()
    live.publish(catalog("KXA"))
    client = await live.connect()

    await client.say(frame)
    (error,) = await client.next(1)
    assert isinstance(error, ErrorMessage)
    assert error.code == code
    assert error.message

    await client.subscribe("KXA")
    assert await client.next(1) == [subscribed(["KXA"])]


async def test_a_message_over_4_kb_closes_the_connection_with_1008() -> None:
    live = Live()
    client = await live.connect()

    await client.say(json.dumps({"op": "subscribe", "tickers": ["K" * MAX_CLIENT_MESSAGE_BYTES]}))

    await asyncio.wait_for(client.task, timeout=5)
    assert client.socket.closed_with == CLOSE_POLICY_VIOLATION


async def test_an_eleventh_message_within_a_second_closes_the_connection_with_1008() -> None:
    live = Live()
    live.publish(catalog("KXA"))
    client = await live.connect()

    for _ in range(10):
        await client.subscribe("KXA")
    await client.next(10)
    live.clock.advance(NS_PER_S)
    for _ in range(10):
        await client.subscribe()
    await client.next(10)
    assert client.socket.closed_with is None

    await client.subscribe("KXA")
    await asyncio.wait_for(client.task, timeout=5)
    assert client.socket.closed_with == CLOSE_POLICY_VIOLATION


async def test_admission_stops_at_max_clients_and_a_departure_frees_a_place() -> None:
    live = Live(max_clients=2)
    first = await live.connect()
    await live.connect()
    assert live.hub.admit(FakeSocket()) is None

    await first.socket.inbound.put(None)
    await asyncio.wait_for(first.task, timeout=5)
    assert live.hub.clients == 1
    assert first.socket.closed_with is None
    assert live.hub.admit(FakeSocket()) is not None


async def test_closing_every_session_sends_going_away_and_detaches_them() -> None:
    live = Live()
    live.publish(catalog("KXA"))
    clients = [await live.connect(), await live.connect()]
    await clients[0].subscribe("KXA")
    await clients[0].next(1)

    live.hub.close_sessions(CLOSE_GOING_AWAY)

    await asyncio.wait_for(asyncio.gather(*(c.task for c in clients)), timeout=5)
    assert [c.socket.closed_with for c in clients] == [CLOSE_GOING_AWAY, CLOSE_GOING_AWAY]
    assert live.hub.clients == 0
    live.publish(refresh("KXA"))  # no follower is left to offer it to


async def test_the_hub_follows_every_topic_until_closed_and_updates_the_directory(
    caplog: pytest.LogCaptureFixture,
) -> None:
    live = Live()
    following = asyncio.create_task(live.hub.run())
    report = StatusReport(
        interval_s=60,
        universe_size=1,
        subscribed_markets=1,
        connections=(
            ConnectionReport(
                conn_id=2, taped=True, frames=3, gaps=0, reconnects=0, stale_books=0, sink_dropped=0
            ),
        ),
    )
    events: tuple[BusEvent, ...] = (catalog("KXA"), report, ticker_update("KXA"))
    envelopes = [
        BusEnvelope(bus_epoch=EPOCH, bus_seq=seq, event=event)
        for seq, event in enumerate(events, start=2)
    ]
    live.subscriber.push(b"\x00not an envelope")
    live.subscriber.push(b"also not one")
    for envelope in envelopes:
        live.subscriber.push(encode_bus_envelope(envelope))

    directory = live.hub.directory
    await until(lambda: live.hub.bus_health().messages == 3)
    live.hub.close()
    await asyncio.wait_for(following, timeout=5)

    assert live.subscriber.prefixes == [b""]
    assert live.hub.malformed == 2
    assert len([r for r in caplog.records if r.getMessage().startswith("bus message not")]) == 1
    assert caplog.records[0].levelno == logging.WARNING
    entry = directory.entry("KXA")
    assert entry is not None
    status = directory.service_status(now_mono_ns=1, bus=live.hub.bus_health(), clients=0)
    assert status.recording
    assert status.recorder is not None
    assert status.recorder.connections[0].frames == 3
    assert live.hub.bus_health().resets == 1  # the first message, not the malformed ones
