"""Connection supervisor against the fake exchange over real sockets."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import msgspec
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.client.ws import (
    SubscribeCommand,
    UnsubscribeCommand,
    UpdateSubscriptionCommand,
    WsSession,
    encode_command,
)
from tape.errors import KalshiTransportError
from tape.events import (
    BookDelta,
    BookSnapshot,
    GapEvent,
    Level,
    Lifecycle,
    MarketEvent,
    Side,
    Ticker,
    Trade,
)
from tape.fixedpoint import CountE2, PriceE4
from tape.recorder.planner import Group
from tape.recorder.supervisor import (
    FIREHOSE_GROUP_ID,
    ConnectionSupervisor,
    SupervisorConfig,
    backoff_delay_s,
)
from tape.recorder.writer import SegmentSink
from tape.segment import Record, RecordKind, SegmentHeader, SegmentReader, SubscriptionInfo
from tape.timeutil import NS_PER_MS, NS_PER_S, FrozenClock
from tests.fakes import FakeConnection, FakeKalshiWs

CONN_ID = 2
NOON = int(datetime(2026, 9, 10, 12, tzinfo=UTC).timestamp()) * NS_PER_S
BOOK_SID = 1
TRADE_SID = 2
GROUP = Group(group_id="g0002", conn_id=CONN_ID, tickers=frozenset({"KXA-1", "KXA-2"}))
WIDER_GROUP = msgspec.structs.replace(GROUP, tickers=GROUP.tickers | {"KXA-3"})


class StubSigner:
    """Satisfies the ``Signer`` protocol; the fake exchange checks no signature."""

    key_id = "test-key"

    def sign(self, timestamp_ms: int, method: str, path: str) -> str:
        return f"{timestamp_ms}{method}{path}"

    def headers(self, method: str, path: str, *, now_ms: int) -> dict[str, str]:
        return {"KALSHI-ACCESS-KEY": self.key_id, "X-Test": self.sign(now_ms, method, path)}


class Harness:
    """A supervisor wired to a fake exchange, a real sink, and recording test doubles."""

    def __init__(
        self,
        url: str,
        root: Path,
        *,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        on_event: Callable[[MarketEvent], None] | None = None,
        deadline_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        **config: Any,  # SupervisorConfig fields under test
    ) -> None:
        self.root = root
        self.clock = FrozenClock(mono_ns=1, wall_ns=NOON)
        self.events: list[MarketEvent] = []
        self.sleeps: list[float] = []
        self.config = SupervisorConfig(
            conn_id=CONN_ID, **{"backoff_initial_ns": 2 * NS_PER_S, **config}
        )
        self.sink = (
            SegmentSink(
                root,
                conn_id=CONN_ID,
                header_factory=self.header,
                clock=self.clock,
                poll_interval_ns=2 * NS_PER_MS,
            )
            if self.config.persist
            else None
        )
        self.supervisor = ConnectionSupervisor(
            self.config,
            session_factory=lambda: WsSession(
                url, StubSigner(), self.clock, conn_id=CONN_ID, connect_timeout_ns=2 * NS_PER_S
            ),
            clock=self.clock,
            sleep=self.record_sleep if sleep is None else sleep,
            jitter=lambda: 0.5,
            sink=self.sink,
            on_event=self.events.append if on_event is None else on_event,
            deadline_sleep=deadline_sleep,
        )
        if self.sink is not None:
            self.sink.start()
        self.task: asyncio.Task[None] | None = None
        self._closed = False

    def header(self) -> SegmentHeader:
        return SegmentHeader(
            created_wall_ns=int(self.clock.wall_ns()),
            host="test",
            env="demo",
            conn_id=CONN_ID,
            ws_url="ws://fake",
            use_yes_price=self.config.use_yes_price,
            subscriptions=list(self.supervisor.subscriptions),
            software_version="0.1.0",
            spec_versions={},
        )

    async def record_sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)

    def start(self) -> None:
        self.task = asyncio.create_task(self.supervisor.run())

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self.supervisor.stop()
        if self.task is not None:
            await asyncio.wait_for(self.task, timeout=5)
        if self.sink is not None:
            self.sink.close()

    def tape(self) -> list[list[Record]]:
        files = sorted(self.root.glob("raw/*/*/*.tape.zst"))
        out: list[list[Record]] = []
        for path in files:
            with SegmentReader(path) as reader:
                out.append(list(reader.records()))
        return out

    def fresh(self, *tickers: str) -> bool:
        books = self.supervisor.books
        return all(ticker in books and not books[ticker].is_stale() for ticker in tickers)

    def count(self, event_type: type[MarketEvent]) -> int:
        return sum(1 for event in self.events if isinstance(event, event_type))


@asynccontextmanager
async def recording(url: str, root: Path, **kwargs: Any) -> AsyncIterator[Harness]:
    harness = Harness(url, root, **kwargs)
    try:
        yield harness
    finally:
        await harness.close()


async def until(predicate: Callable[[], bool], *, timeout_s: float = 5.0) -> None:
    """Wait for a condition the supervisor cannot signal, without a fixed sleep."""
    async with asyncio.timeout(timeout_s):
        while not predicate():
            await asyncio.sleep(0.001)


def set_books(fake: FakeKalshiWs) -> None:
    fake.set_book("KXA-1", yes=[("0.4000", "10.00")], no=[("0.6000", "5.00")])
    fake.set_book("KXA-2", yes=[("0.2000", "1.00")])


def delta(ticker: str, price: str, count: str, *, side: str = "yes") -> dict[str, Any]:
    return {
        "market_ticker": ticker,
        "price_dollars": price,
        "delta_fp": count,
        "side": side,
        "ts_ms": 1_789_000_000_000,
    }


def trade(ticker: str) -> dict[str, Any]:
    return {
        "trade_id": "t-1",
        "market_ticker": ticker,
        "yes_price_dollars": "0.4000",
        "no_price_dollars": "0.6000",
        "count_fp": "3.00",
        "taker_book_side": "bid",
        "ts_ms": 1_789_000_000_000,
    }


def level(price: int, count: int) -> Level:
    return Level(PriceE4(price), CountE2(count))


def decoded(record: Record) -> dict[str, Any]:
    payload: dict[str, Any] = msgspec.json.decode(record.payload, type=dict[str, Any])
    return payload


async def subscribed(fake: FakeKalshiWs, harness: Harness) -> FakeConnection:
    connection = await fake.wait_for_connection()
    await until(lambda: len(harness.supervisor.subscriptions) == 2)
    return connection


async def test_a_malformed_frame_is_recorded_before_it_fails_to_decode(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await connection.push(b"{not json")
        await connection.push_sequenced("trade", {"market_ticker": "KXA-1"}, sid=TRADE_SID)
        await connection.push_sequenced("trade", trade("KXA-1"), sid=TRADE_SID)
        await until(lambda: harness.count(Trade) == 1)
        assert harness.supervisor.stats.decode_errors == 2
        frames = harness.supervisor.stats.frames

    recorded = [r for f in harness.tape() for r in f if r.kind is RecordKind.FRAME]
    assert len(recorded) == frames
    assert b"{not json" in [record.payload for record in recorded]
    assert harness.supervisor.stats.records_not_persisted == 0


async def test_one_subscribe_for_two_channels_builds_a_two_sid_table(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        assert harness.supervisor.subscriptions == (
            SubscriptionInfo(sid=BOOK_SID, channel="orderbook_delta", group_id=GROUP.group_id),
            SubscriptionInfo(sid=TRADE_SID, channel="trade", group_id=GROUP.group_id),
        )
        assert list(connection.commands) == [
            {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta", "trade"],
                    "market_tickers": ["KXA-1", "KXA-2"],
                    "use_yes_price": True,
                },
            }
        ]

    (records,) = harness.tape()
    assert (records[0].kind, decoded(records[0])) == (
        RecordKind.CONNECTION,
        {"event": "open", "detail": "connected"},
    )
    subscribe = SubscribeCommand(
        channels=("orderbook_delta", "trade"),
        market_tickers=("KXA-1", "KXA-2"),
        use_yes_price=True,
    )
    assert (records[1].kind, records[1].payload) == (
        RecordKind.COMMAND,
        encode_command(subscribe, 1),
    )
    assert decoded(records[-1]) == {"event": "close", "detail": "stopped"}


@pytest.mark.parametrize("use_yes_price", [True, False])
async def test_snapshot_then_deltas_yield_the_correct_book(
    tmp_path: Path, use_yes_price: bool
) -> None:
    # The same YES-space book either way: NO bids arrive on the YES scale only when asked.
    no_price = "0.6000" if use_yes_price else "0.4000"
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, use_yes_price=use_yes_price) as harness,
    ):
        fake.set_book("KXA-1", yes=[("0.4000", "10.00")], no=[(no_price, "5.00")])
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await until(lambda: harness.fresh("KXA-1"))
        assert harness.supervisor.books["KXA-1"].levels(Side.ASK) == [level(6000, 500)]
        await connection.push_sequenced(
            "orderbook_delta", delta("KXA-1", "0.4000", "2.50"), sid=BOOK_SID
        )
        await connection.push_sequenced(
            "orderbook_delta", delta("KXA-1", "0.4100", "1.00"), sid=BOOK_SID
        )
        await connection.push_sequenced(
            "orderbook_delta", delta("KXA-1", no_price, "-5.00", side="no"), sid=BOOK_SID
        )
        await until(lambda: harness.count(BookDelta) == 3)

        book = harness.supervisor.books["KXA-1"]
        assert book.levels(Side.BID) == [level(4100, 100), level(4000, 1250)]
        assert book.levels(Side.ASK) == []
        assert [type(event) for event in harness.events] == [
            BookSnapshot,
            BookDelta,
            BookDelta,
            BookDelta,
        ]
        assert connection.commands[0]["params"]["use_yes_price"] is use_yes_price


async def test_a_live_only_connection_writes_nothing_but_publishes_everything(
    tmp_path: Path,
) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path, persist=False) as harness:
        set_books(fake)
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await until(lambda: harness.fresh("KXA-1", "KXA-2"))
        await connection.push_sequenced("trade", trade("KXA-1"), sid=TRADE_SID)
        await connection.push_message(
            "ticker", {"market_ticker": "KXA-1", "ts_ms": 1, "volume_fp": "7.00"}, sid=TRADE_SID
        )
        await connection.push_sequenced(
            "market_lifecycle_v2",
            {"event_type": "determined", "market_ticker": "KXA-1", "result": "yes"},
            sid=TRADE_SID,
        )
        await until(lambda: harness.count(Lifecycle) == 1)

    assert [type(event) for event in harness.events] == [
        BookSnapshot,
        BookSnapshot,
        Trade,
        Ticker,
        Lifecycle,
    ]
    assert list(tmp_path.iterdir()) == []
    assert harness.sink is None


async def test_a_gap_stales_every_book_on_the_connection_and_resnapshots_all_its_markets(
    tmp_path: Path,
) -> None:
    """KXA-3 joined by ``add_markets``, not by the subscribe, and shares the one orderbook sid."""
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        set_books(fake)
        fake.set_book("KXA-3", yes=[("0.1000", "2.00")])
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await harness.supervisor.set_group(WIDER_GROUP)
        await until(lambda: harness.fresh("KXA-1", "KXA-2", "KXA-3"))

        connection.go_silent()  # hold the snapshots back so the stale window is observable
        connection.skip_seq(BOOK_SID)
        got_seq = await connection.push_sequenced(
            "orderbook_delta", delta("KXA-1", "0.4000", "1.00"), sid=BOOK_SID
        )
        commands = await connection.wait_for_commands(4)
        get_snapshot = UpdateSubscriptionCommand(
            sid=BOOK_SID, action="get_snapshot", market_tickers=("KXA-1", "KXA-2", "KXA-3")
        )
        assert commands[3] == msgspec.json.decode(encode_command(get_snapshot, 4))
        stats = harness.supervisor.stats
        assert (stats.gaps, stats.snapshots_requested, stats.stale_books) == (1, 1, 3)
        # The delta that revealed the gap was not applied to a book whose base is unknown.
        assert harness.supervisor.books["KXA-1"].levels(Side.BID) == [level(4000, 1000)]
        gaps = [event for event in harness.events if isinstance(event, GapEvent)]
        assert [(g.sid, g.expected_seq, g.got_seq) for g in gaps] == [
            (BOOK_SID, got_seq - 1, got_seq)
        ]

        await connection.push_sequenced(
            "orderbook_snapshot",
            {"market_ticker": "KXA-1", "yes_dollars_fp": [["0.4500", "3.00"]]},
            sid=BOOK_SID,
        )
        await until(lambda: harness.fresh("KXA-1"))
        assert harness.supervisor.stats.stale_books == 2
        for ticker in ("KXA-2", "KXA-3"):
            await connection.push_sequenced(
                "orderbook_snapshot",
                {"market_ticker": ticker, "yes_dollars_fp": [["0.2000", "1.00"]]},
                sid=BOOK_SID,
            )
        await connection.push_sequenced(
            "orderbook_delta", delta("KXA-1", "0.4500", "1.00"), sid=BOOK_SID
        )
        await until(lambda: harness.count(BookDelta) == 1)
        assert harness.supervisor.books["KXA-1"].levels(Side.BID) == [level(4500, 400)]
        stats = harness.supervisor.stats
        assert (stats.gaps, stats.duplicates, stats.stale_books) == (1, 0, 0)

    records = [record for f in harness.tape() for record in f]
    gap_index = next(i for i, r in enumerate(records) if r.kind is RecordKind.GAP)
    assert decoded(records[gap_index]) == {
        "sid": BOOK_SID,
        "expected_seq": got_seq - 1,
        "got_seq": got_seq,
    }
    assert decoded(records[gap_index - 1])["seq"] == got_seq  # right behind the frame
    assert records[gap_index + 1].payload == encode_command(get_snapshot, 4)


async def test_a_gap_on_the_trade_channel_is_recorded_but_leaves_books_alone(
    tmp_path: Path,
) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        set_books(fake)
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await until(lambda: harness.fresh("KXA-1", "KXA-2"))
        await connection.push_sequenced("trade", trade("KXA-1"), sid=TRADE_SID)
        connection.skip_seq(TRADE_SID, 3)
        await connection.push_sequenced("trade", trade("KXA-1"), sid=TRADE_SID)
        await until(lambda: harness.count(Trade) == 2)
        stats = harness.supervisor.stats
        assert (stats.gaps, stats.stale_books, stats.snapshots_requested) == (1, 0, 0)
        assert len(connection.commands) == 1

    gaps = [decoded(r) for f in harness.tape() for r in f if r.kind is RecordKind.GAP]
    assert gaps == [{"sid": TRADE_SID, "expected_seq": 2, "got_seq": 5}]


async def test_a_repeated_seq_is_never_fatal_and_resnapshots_the_group(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        set_books(fake)
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await until(lambda: harness.fresh("KXA-1", "KXA-2"))
        # The initial snapshots took seq 1 and 2; a replayed 2 must not be applied twice.
        await connection.push_message(
            "orderbook_delta", delta("KXA-1", "0.4000", "1.00"), sid=BOOK_SID, seq=2
        )
        await connection.wait_for_commands(2)
        await until(lambda: harness.count(BookSnapshot) == 4 and harness.fresh("KXA-1", "KXA-2"))
        stats = harness.supervisor.stats
        assert (stats.duplicates, stats.gaps, stats.snapshots_requested) == (1, 0, 1)
        assert harness.supervisor.books["KXA-1"].levels(Side.BID) == [level(4000, 1000)]
        assert harness.count(BookDelta) == 0


async def test_a_delta_that_breaks_the_book_stales_it_and_requests_its_snapshot(
    tmp_path: Path,
) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        set_books(fake)
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await until(lambda: harness.fresh("KXA-1", "KXA-2"))
        await connection.push_sequenced(
            "orderbook_delta", delta("KXA-1", "0.4000", "-20.00"), sid=BOOK_SID
        )
        commands = await connection.wait_for_commands(2)
        assert commands[1]["params"] == {
            "sid": BOOK_SID,
            "action": "get_snapshot",
            "market_tickers": ["KXA-1"],
        }
        await until(lambda: harness.count(BookSnapshot) == 3 and harness.fresh("KXA-1"))
        assert harness.supervisor.books["KXA-1"].levels(Side.BID) == [level(4000, 1000)]
        stats = harness.supervisor.stats
        assert (stats.book_errors, stats.snapshots_requested, stats.gaps) == (1, 1, 0)


async def test_an_abrupt_close_reconnects_after_backoff_and_resubscribes_from_the_table(
    tmp_path: Path,
) -> None:
    release = asyncio.Event()
    sleeps: list[float] = []

    async def held_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await release.wait()

    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path, sleep=held_sleep) as harness:
        set_books(fake)
        await harness.supervisor.set_group(GROUP)
        harness.start()
        first = await subscribed(fake, harness)
        await until(lambda: harness.fresh("KXA-1", "KXA-2"))

        await first.close_abruptly()
        await until(lambda: len(sleeps) == 1)
        # 2 s nominal, jitter 0.5; frames had arrived, so the failure count had reset.
        assert sleeps == [pytest.approx(1.5)]
        assert harness.supervisor.stats.stale_books == 2
        assert list(harness.supervisor.subscriptions) == []

        release.set()
        second = await fake.wait_for_connection(1)
        await until(lambda: harness.count(BookSnapshot) == 4 and harness.fresh("KXA-1", "KXA-2"))
        assert list(second.commands) == list(first.commands)
        assert len(harness.supervisor.subscriptions) == 2
        assert harness.supervisor.stats.reconnects == 1

    first_file, second_file = harness.tape()
    first_events = [decoded(r) for r in first_file if r.kind is RecordKind.CONNECTION]
    assert [event["event"] for event in first_events] == ["open", "close"]
    assert "closed by peer" in first_events[1]["detail"]
    second_events = [decoded(r) for r in second_file if r.kind is RecordKind.CONNECTION]
    assert second_events == [
        {"event": "open", "detail": "connected"},
        {"event": "close", "detail": "stopped"},
    ]


async def test_repeated_failures_back_off_up_to_the_cap_then_raise(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake:
        url = fake.url
    harness = Harness(
        url,
        tmp_path,
        backoff_initial_ns=NS_PER_S,
        backoff_max_ns=3 * NS_PER_S // 2,
        max_consecutive_failures=2,
    )
    with pytest.raises(KalshiTransportError):
        await asyncio.wait_for(harness.supervisor.run(), timeout=10)
    # Jitter 0.5 keeps three quarters of the nominal 1 s, then of 2 s capped at 1.5 s.
    assert harness.sleeps == [pytest.approx(0.75), pytest.approx(1.125)]
    assert harness.supervisor.stats.reconnects == 2
    assert harness.sink is not None
    harness.sink.close()
    (records,) = harness.tape()
    assert [decoded(record)["event"] for record in records] == ["error", "error", "error"]


async def test_stop_interrupts_a_backoff_that_would_never_end(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake:
        url = fake.url
    entered = asyncio.Event()

    async def endless_sleep(seconds: float) -> None:
        entered.set()
        await asyncio.Event().wait()

    harness = Harness(url, tmp_path, sleep=endless_sleep, persist=False)
    harness.start()
    async with asyncio.timeout(5):
        await entered.wait()
    await harness.close()
    await harness.supervisor.stop()
    assert harness.task is not None
    assert harness.task.exception() is None
    assert harness.supervisor.stats.reconnects == 0


async def test_a_supervisor_stopped_before_it_runs_never_connects_and_is_single_use() -> None:
    def no_session() -> WsSession:
        raise AssertionError("a stopped supervisor must not connect")

    async def no_sleep(seconds: float) -> None:
        raise AssertionError("a stopped supervisor must not back off")

    supervisor = ConnectionSupervisor(
        SupervisorConfig(conn_id=CONN_ID, persist=False),
        session_factory=no_session,
        clock=FrozenClock(),
        sleep=no_sleep,
        jitter=lambda: 0.0,
    )
    await supervisor.stop()
    await supervisor.stop()
    await asyncio.wait_for(supervisor.run(), timeout=1)
    with pytest.raises(RuntimeError, match="single-use"):
        await supervisor.run()


async def test_changing_membership_reaches_both_channel_sids(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        set_books(fake)
        fake.set_book("KXA-3", yes=[("0.1000", "2.00")])
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)

        await harness.supervisor.set_group(WIDER_GROUP)
        added = [
            UpdateSubscriptionCommand(sid=sid, action="add_markets", market_tickers=("KXA-3",))
            for sid in (BOOK_SID, TRADE_SID)
        ]
        commands = await connection.wait_for_commands(3)
        assert commands[1:] == [
            msgspec.json.decode(encode_command(command, command_id))
            for command_id, command in enumerate(added, start=2)
        ]
        await until(lambda: harness.fresh("KXA-3"))
        assert harness.supervisor.group == WIDER_GROUP

        await harness.supervisor.set_group(GROUP)
        commands = await connection.wait_for_commands(5)
        assert [(c["params"]["action"], c["params"]["sid"]) for c in commands[3:]] == [
            ("delete_markets", BOOK_SID),
            ("delete_markets", TRADE_SID),
        ]
        assert "KXA-3" not in harness.supervisor.books

        await harness.supervisor.set_group(None)
        commands = await connection.wait_for_commands(6)
        assert commands[5] == {"id": 6, "cmd": "unsubscribe", "params": {"sids": [1, 2]}}
        assert list(harness.supervisor.subscriptions) == []
        assert dict(harness.supervisor.books) == {}

    sent = [r.payload for f in harness.tape() for r in f if r.kind is RecordKind.COMMAND]
    assert sent[1:3] == [encode_command(command, i) for i, command in enumerate(added, start=2)]
    assert sent[5] == encode_command(UnsubscribeCommand(sids=(1, 2)), 6)


async def test_markets_added_to_a_live_subscription_all_get_books_and_nothing_stays_pending(
    tmp_path: Path,
) -> None:
    """The 2026-09-10 smoke scenario: markets reach one connection in two batches.

    The second batch goes out as ``update_subscription``, never as a second subscribe, and
    every market of both batches ends up with a book.
    """
    tickers = [f"KXS-{index}" for index in range(6)]
    first = Group(group_id="g0002", conn_id=CONN_ID, tickers=frozenset(tickers[:4]))
    everything = msgspec.structs.replace(first, tickers=frozenset(tickers))
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        for ticker in tickers:
            fake.set_book(ticker, yes=[("0.3000", "1.00")])
        await harness.supervisor.set_group(first)
        harness.start()
        connection = await subscribed(fake, harness)
        await until(lambda: harness.fresh(*tickers[:4]))

        await harness.supervisor.set_group(everything)
        await until(lambda: harness.fresh(*tickers))
        assert sorted(harness.supervisor.books) == tickers
        assert [info["market_tickers"] for info in connection.subscriptions.values()] == [
            tickers,
            tickers,
        ]
        commands = await connection.wait_for_commands(3)
        assert [command["cmd"] for command in commands] == [
            "subscribe",
            "update_subscription",
            "update_subscription",
        ]

        # Nothing is left pending, so the next change is sent at once rather than deferred.
        await harness.supervisor.set_group(first)
        commands = await connection.wait_for_commands(5)
        assert [(c["params"]["action"], c["params"]["sid"]) for c in commands[3:]] == [
            ("delete_markets", BOOK_SID),
            ("delete_markets", TRADE_SID),
        ]


async def test_a_subscribe_the_exchange_merges_is_resolved_and_adopts_the_merged_membership(
    tmp_path: Path,
) -> None:
    """The fake keeps the subscription whose unsubscribe it never processed, then merges.

    The ``ok`` replies name the live sids, so the supervisor binds them, adopts the merged
    membership, drops the market it no longer wants, and snapshots the market that was
    already subscribed and so got no initial snapshot.
    """
    other = msgspec.structs.replace(GROUP, tickers=frozenset({"KXA-2", "KXA-3"}))
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        set_books(fake)
        fake.set_book("KXA-3", yes=[("0.1000", "2.00")])
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await until(lambda: harness.fresh("KXA-1", "KXA-2"))

        connection.go_silent()
        await harness.supervisor.set_group(None)
        await connection.wait_for_commands(2)
        connection.resume()
        await harness.supervisor.set_group(other)
        await until(lambda: harness.fresh("KXA-2", "KXA-3"))
        commands = await connection.wait_for_commands(6)

        assert [(c["cmd"], c["params"]) for c in commands[2:]] == [
            (
                "subscribe",
                {
                    "channels": ["orderbook_delta", "trade"],
                    "market_tickers": ["KXA-2", "KXA-3"],
                    "use_yes_price": True,
                },
            ),
            (
                "update_subscription",
                {"sid": BOOK_SID, "action": "get_snapshot", "market_tickers": ["KXA-2", "KXA-3"]},
            ),
            (
                "update_subscription",
                {"sid": BOOK_SID, "action": "delete_markets", "market_tickers": ["KXA-1"]},
            ),
            (
                "update_subscription",
                {"sid": TRADE_SID, "action": "delete_markets", "market_tickers": ["KXA-1"]},
            ),
        ]
        assert harness.supervisor.subscriptions == (
            SubscriptionInfo(sid=BOOK_SID, channel="orderbook_delta", group_id=GROUP.group_id),
            SubscriptionInfo(sid=TRADE_SID, channel="trade", group_id=GROUP.group_id),
        )
        assert sorted(harness.supervisor.books) == ["KXA-2", "KXA-3"]


async def test_merge_replies_resolve_a_subscribe_in_the_order_production_sent_them(
    tmp_path: Path,
) -> None:
    """The recorded ``ok`` frames answered the trade sid before the orderbook sid."""
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.start()
        connection = await fake.wait_for_connection()
        connection.go_silent()
        await harness.supervisor.set_group(GROUP)
        await connection.wait_for_commands(1)
        await connection.push(
            b'{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":1}}'
        )
        await connection.push(b'{"id":1,"type":"subscribed","msg":{"channel":"trade","sid":2}}')
        await until(lambda: len(harness.supervisor.subscriptions) == 2)
        await harness.supervisor.set_group(None)
        await harness.supervisor.set_group(WIDER_GROUP)
        await connection.wait_for_commands(3)

        merged = b'"type":"ok","msg":{"market_tickers":["KXA-1","KXA-2","KXA-3","KXA-9"]}}'
        await connection.push(b'{"id":3,"sid":2,"seq":1,' + merged)
        await until(lambda: len(harness.supervisor.subscriptions) == 1)
        assert len(connection.commands) == 3  # the orderbook channel has not answered yet
        await connection.push(b'{"id":3,"sid":1,"seq":1,' + merged)
        commands = await connection.wait_for_commands(6)
        assert [(c["params"]["action"], c["params"]["sid"]) for c in commands[3:]] == [
            ("get_snapshot", BOOK_SID),
            ("delete_markets", BOOK_SID),
            ("delete_markets", TRADE_SID),
        ]
        assert commands[3]["params"]["market_tickers"] == ["KXA-1", "KXA-2", "KXA-3"]
        assert commands[4]["params"]["market_tickers"] == ["KXA-9"]
        assert len(harness.supervisor.subscriptions) == 2


async def test_a_subscribe_without_replies_fails_the_connection_after_its_deadline(
    tmp_path: Path,
) -> None:
    deadlines: list[float] = []
    expire = asyncio.Event()

    async def deadline_sleep(seconds: float) -> None:
        deadlines.append(seconds)
        # Only the first connection's deadline is let run out.
        await (expire.wait() if len(deadlines) == 1 else asyncio.Event().wait())

    async with (
        FakeKalshiWs() as fake,
        recording(
            fake.url,
            tmp_path,
            deadline_sleep=deadline_sleep,
            subscribe_timeout_ns=3 * NS_PER_S,
        ) as harness,
    ):
        set_books(fake)
        harness.start()
        first = await fake.wait_for_connection()
        first.go_silent()
        await harness.supervisor.set_group(GROUP)
        await first.wait_for_commands(1)
        await until(lambda: deadlines == [3.0])
        assert harness.supervisor.stats.reconnects == 0

        expire.set()
        second = await fake.wait_for_connection(1)
        await until(lambda: harness.fresh("KXA-1", "KXA-2"))
        assert list(second.commands) == list(first.commands)
        assert harness.sleeps == [pytest.approx(1.5)]
        assert harness.supervisor.stats.reconnects == 1

    first_file, _ = harness.tape()
    events = [decoded(r) for r in first_file if r.kind is RecordKind.CONNECTION]
    assert events == [
        {"event": "open", "detail": "connected"},
        {
            "event": "close",
            "detail": f"connection {CONN_ID} subscribe 1 had no reply for orderbook_delta, trade "
            f"within {3 * NS_PER_S} ns",
        },
    ]


async def test_membership_changes_wait_until_every_sid_of_the_group_is_known(
    tmp_path: Path,
) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.start()
        connection = await fake.wait_for_connection()
        connection.go_silent()
        await harness.supervisor.set_group(GROUP)
        # The subscribe may come from run() once connected; the change must follow it.
        await connection.wait_for_commands(1)
        await harness.supervisor.set_group(WIDER_GROUP)
        await connection.push(b'{"id":1,"type":"subscribed","msg":{"channel":"trade","sid":2}}')
        await until(lambda: len(harness.supervisor.subscriptions) == 1)
        # An update now would reach sid 2 and miss the orderbook sid nobody knows yet.
        assert len(connection.commands) == 1
        await connection.push(
            b'{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":1}}'
        )
        commands = await connection.wait_for_commands(3)
        assert [(c["params"]["action"], c["params"]["sid"]) for c in commands[1:]] == [
            ("add_markets", 1),
            ("add_markets", 2),
        ]
        await connection.push(b'{"id":7,"type":"subscribed","msg":{"channel":"trade","sid":9}}')
        await connection.push_sequenced("trade", trade("KXA-1"), sid=TRADE_SID)
        await until(lambda: harness.count(Trade) == 1)
        assert len(harness.supervisor.subscriptions) == 2


async def test_a_refused_subscribe_is_surfaced_and_retried_on_the_next_change(
    tmp_path: Path,
) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.start()
        connection = await fake.wait_for_connection()
        connection.go_silent()
        await harness.supervisor.set_group(GROUP)
        await connection.wait_for_commands(1)
        await connection.push_error(9, "Authentication required", command_id=1)
        await until(lambda: harness.supervisor.stats.errors_by_code == {9: 1})
        (error,) = harness.supervisor.last_errors
        assert (error.code, error.message, error.sid) == (9, "Authentication required", None)
        await harness.supervisor.set_group(GROUP)
        commands = await connection.wait_for_commands(2)
        assert [(command["id"], command["cmd"]) for command in commands] == [
            (1, "subscribe"),
            (2, "subscribe"),
        ]


async def test_capacity_errors_are_counted_and_surfaced_with_their_sid(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await connection.push_error(25, "Subscription buffer overflow", sid=BOOK_SID)
        await connection.push_error(27, "Too many requests", command_id=99)
        await until(lambda: sum(harness.supervisor.stats.errors_by_code.values()) == 2)
        assert harness.supervisor.stats.errors_by_code == {25: 1, 27: 1}
        assert [(e.code, e.sid) for e in harness.supervisor.last_errors] == [
            (25, BOOK_SID),
            (27, None),
        ]


async def test_a_firehose_connection_subscribes_every_market_once(tmp_path: Path) -> None:
    async with (
        FakeKalshiWs() as fake,
        recording(
            fake.url, tmp_path, book_channels=(), firehose_channels=("ticker",), persist=False
        ) as harness,
    ):
        harness.start()
        connection = await fake.wait_for_connection()
        commands = await connection.wait_for_commands(1)
        assert commands == [{"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"]}}]
        await until(lambda: len(harness.supervisor.subscriptions) == 1)
        assert harness.supervisor.subscriptions == (
            SubscriptionInfo(sid=1, channel="ticker", group_id=FIREHOSE_GROUP_ID),
        )
        await connection.push_message("ticker", {"market_ticker": "KXB-9", "ts_ms": 5}, sid=1)
        await until(lambda: harness.count(Ticker) == 1)
        with pytest.raises(ValueError, match="no book channels"):
            await harness.supervisor.set_group(GROUP)


async def test_a_refused_firehose_subscribe_is_asked_again_on_the_next_change(
    tmp_path: Path,
) -> None:
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, firehose_channels=("ticker",)) as harness,
    ):
        harness.start()
        connection = await fake.wait_for_connection()
        connection.go_silent()
        await connection.wait_for_commands(1)
        await connection.push_error(8, "Unknown channel name", command_id=1)
        await until(lambda: harness.supervisor.stats.errors_by_code == {8: 1})
        await harness.supervisor.set_group(GROUP)
        commands = await connection.wait_for_commands(3)
        assert [command["params"]["channels"] for command in commands] == [
            ["ticker"],
            ["ticker"],
            ["orderbook_delta", "trade"],
        ]


async def test_a_failing_consumer_is_counted_and_recording_continues(tmp_path: Path) -> None:
    def broken_consumer(event: MarketEvent) -> None:
        raise RuntimeError("consumer bug")

    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, on_event=broken_consumer) as harness,
    ):
        set_books(fake)
        await harness.supervisor.set_group(GROUP)
        harness.start()
        connection = await subscribed(fake, harness)
        await until(lambda: harness.supervisor.stats.callback_errors == 2)
        await connection.push_sequenced(
            "orderbook_delta", delta("KXA-2", "0.2000", "1.00"), sid=BOOK_SID
        )
        await until(lambda: harness.supervisor.stats.callback_errors == 3)
        assert harness.supervisor.books["KXA-2"].levels(Side.BID) == [level(2000, 200)]


async def test_a_group_for_another_connection_is_refused(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        elsewhere = msgspec.structs.replace(GROUP, conn_id=CONN_ID + 1)
        with pytest.raises(ValueError, match="is for connection"):
            await harness.supervisor.set_group(elsewhere)
        assert harness.supervisor.group is None


def test_a_sink_is_required_exactly_when_persisting(tmp_path: Path) -> None:
    async def no_sleep(seconds: float) -> None:
        return None

    def build(config: SupervisorConfig, sink: SegmentSink | None) -> ConnectionSupervisor:
        return ConnectionSupervisor(
            config,
            session_factory=lambda: WsSession("ws://x", StubSigner(), FrozenClock()),
            clock=FrozenClock(),
            sleep=no_sleep,
            jitter=lambda: 0.0,
            sink=sink,
        )

    with pytest.raises(ValueError, match="no sink"):
        build(SupervisorConfig(conn_id=CONN_ID), None)
    harness = Harness("ws://127.0.0.1:1/unused", tmp_path)
    assert harness.sink is not None
    with pytest.raises(ValueError, match="live-only"):
        build(SupervisorConfig(conn_id=CONN_ID, persist=False), harness.sink)
    harness.sink.close()
    assert build(SupervisorConfig(conn_id=CONN_ID, persist=False), None).config.persist is False


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"conn_id": -1}, "conn_id"),
        ({"book_channels": ()}, "channels"),
        ({"backoff_initial_ns": 0}, "backoff"),
        ({"backoff_max_ns": 1}, "backoff"),
        ({"max_consecutive_failures": -1}, "max_consecutive_failures"),
        ({"subscribe_timeout_ns": 0}, "subscribe_timeout_ns must be positive"),
        ({"firehose_channels": ("trade",)}, r"\['trade'\] are both book and firehose"),
    ],
)
def test_config_rejects_what_cannot_work(overrides: dict[str, Any], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        SupervisorConfig(**{"conn_id": CONN_ID, **overrides})


@given(
    st.integers(0, 200),
    st.integers(1, 10**12),
    st.integers(0, 10**12),
    st.floats(0.0, 1.0, exclude_max=True),
)
@settings(max_examples=500)
def test_backoff_is_a_capped_doubling_jittered_into_its_upper_half(
    failures: int, initial_ns: int, headroom_ns: int, jitter: float
) -> None:
    max_ns = initial_ns + headroom_ns
    delay = backoff_delay_s(failures, initial_ns=initial_ns, max_ns=max_ns, jitter=jitter)
    nominal_s = min(max_ns, initial_ns * 2**failures) / NS_PER_S
    assert nominal_s / 2 * (1 - 1e-12) <= delay <= nominal_s * (1 + 1e-12)
    later = backoff_delay_s(failures + 1, initial_ns=initial_ns, max_ns=max_ns, jitter=jitter)
    assert later >= delay
    assert backoff_delay_s(10**6, initial_ns=initial_ns, max_ns=max_ns, jitter=jitter) <= (
        max_ns / NS_PER_S
    )


@pytest.mark.parametrize(
    ("failures", "initial_ns", "max_ns", "jitter", "match"),
    [
        (-1, 1, 1, 0.0, "consecutive_failures"),
        (0, 1, 1, 1.0, "jitter"),
        (0, 1, 1, -0.1, "jitter"),
        (0, 0, 1, 0.0, "initial_ns"),
        (0, 2, 1, 0.0, "initial_ns"),
    ],
)
def test_backoff_rejects_impossible_inputs(
    failures: int, initial_ns: int, max_ns: int, jitter: float, match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        backoff_delay_s(failures, initial_ns=initial_ns, max_ns=max_ns, jitter=jitter)
