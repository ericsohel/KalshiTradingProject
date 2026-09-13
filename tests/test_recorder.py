"""The recorder orchestrator: a fake exchange over real sockets, mocked REST, virtual time."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import httpx
import msgspec
import pytest

from tape import __version__
from tape.book import Book, books_from_keyframe_rows
from tape.bus import (
    BOOK_FRESH,
    BOOK_UNKNOWN,
    CATALOG_TOPIC,
    RESET_GAP,
    RESET_START,
    STATUS_TOPIC,
    LiveBooks,
    Observation,
    Publisher,
    ZmqPublisher,
    ZmqSubscriber,
    decode_bus_envelope,
)
from tape.client.ratelimit import Bucket, BucketLimits
from tape.client.rest import KalshiRest
from tape.client.ws import WsSession
from tape.errors import KalshiHttpError
from tape.events import (
    BookDelta,
    BookRefresh,
    CatalogEntry,
    ConnectionReport,
    GapEvent,
    Level,
    Lifecycle,
    MarketCatalog,
    Side,
    StatusReport,
    Ticker,
)
from tape.fixedpoint import CountE2, PriceE4
from tape.recorder.recorder import (
    CLOCK_JUMP_THRESHOLD_NS,
    CLOSE_TICK_DELAY_S,
    CONTROL_CONN_ID,
    FIRST_BOOK_CONN_ID,
    NEAR_PRICE_FOLLOW_UPS,
    RELIST_DEBOUNCE_S,
    RELIST_MIN_INTERVAL_S,
    TICKER_CONN_ID,
    TICKER_GROUP_ID,
    BusStatus,
    PeriodicTask,
    Recorder,
    RecorderConfig,
    check_connection_budget,
    is_clock_jump,
    keyframe_path,
    refresh_slices,
    universe_retry_delay_s,
)
from tape.recorder.tap import BookImage, TapWindow
from tape.recorder.universe import UniverseGroup, UniversePolicy
from tape.recorder.writer import HeaderFactory, SegmentSink
from tape.segment import Record, RecordKind, SegmentHeader, SegmentReader, read_keyframe
from tape.timeutil import NS_PER_MS, NS_PER_S, FrozenClock, Ns
from tests.fakes import FakeConnection, FakeKalshiWs, RecordingPublisher
from tests.fakes.rest_payloads import series_payload

NOON: Final = int(datetime(2026, 9, 10, 12, tzinfo=UTC).timestamp()) * NS_PER_S
REST_URL: Final = "https://rest.test/trade-api/v2"
TEST_CATEGORY: Final = "Tests"
POLICY: Final = UniversePolicy(
    min_volume_24h=CountE2(100_000),
    max_l2_markets=4,
    groups=(
        UniverseGroup(name="showcase", series=("KXSHOW",), events=1, markets_per_event=1),
        UniverseGroup(name="busiest", category=TEST_CATEGORY, events=1, markets_per_event=3),
    ),
)
"""A series group for ``KXSHOW``, then the busiest event of the ``KXA`` and ``KXLOW`` category."""
LIMITS: Final = {
    "usage_tier": "advanced",
    "read": {"refill_rate": 300, "bucket_capacity": 600},
    "write": {"refill_rate": 150, "bucket_capacity": 300},
    "grants": [],
}
LOOPS: Final = 3
"""Universe, keyframe, and status loops, each parked in the injected sleep between rounds."""

PROBE: Final = b"ctl.probe"
"""Topic of the messages a test publishes to learn that its bus subscriber has connected."""


def market(
    ticker: str,
    volume: str,
    *,
    status: str = "active",
    close_time: str = "2026-12-31T00:00:00Z",
    bid: str = "0.4000",
    ask: str = "0.6000",
    last: str = "0.5000",
) -> dict[str, object]:
    return {
        "ticker": ticker,
        "event_ticker": ticker.rsplit("-", 1)[0],
        "market_type": "binary",
        "yes_sub_title": "Yes",
        "no_sub_title": "No",
        "created_time": "2026-01-01T00:00:00Z",
        "updated_time": "2026-01-01T00:00:00Z",
        "open_time": "2026-01-01T00:00:00Z",
        "close_time": close_time,
        "latest_expiration_time": close_time,
        "settlement_timer_seconds": 60,
        "status": status,
        "notional_value_dollars": "1.0000",
        "yes_bid_dollars": bid,
        "yes_ask_dollars": ask,
        "no_bid_dollars": "0.4000",
        "no_ask_dollars": "0.6000",
        "yes_bid_size_fp": "1.00",
        "yes_ask_size_fp": "1.00",
        "last_price_dollars": last,
        "previous_yes_bid_dollars": "0.4000",
        "previous_yes_ask_dollars": "0.6000",
        "previous_price_dollars": "0.5000",
        "volume_fp": volume,
        "volume_24h_fp": volume,
        "open_interest_fp": "1.00",
        "result": "",
        "can_close_early": False,
        "expiration_value": "",
        "rules_primary": "",
        "rules_secondary": "",
        "price_level_structure": "linear_cent",
        "price_ranges": [],
        "exchange_index": 0,
    }


MARKETS: Final = (
    market("KXA-1", "5000.00"),
    market("KXA-2", "4000.00"),
    market("KXA-3", "3000.00"),
    market("KXSHOW-1", "0.00"),
    market("KXLOW-1", "1.00"),
    market("KXDONE-1", "9000.00", status="finalized"),
)
"""Dealt to two connections of two: ``KXA-1, KXA-3`` on connection 2, ``KXA-2, KXSHOW-1`` on 3."""


class StubSigner:
    """Satisfies the ``Signer`` protocol; the fake exchange checks no signature."""

    key_id = "test-key"

    def sign(self, timestamp_ms: int, method: str, path: str) -> str:
        return f"{timestamp_ms}{method}{path}"

    def headers(self, method: str, path: str, *, now_ms: int) -> dict[str, str]:
        return {"KALSHI-ACCESS-KEY": self.key_id, "X-Test": self.sign(now_ms, method, path)}


class RecordingLimiter:
    """Never waits; remembers every resize."""

    def __init__(self) -> None:
        self.resizes: list[tuple[BucketLimits, BucketLimits]] = []

    async def acquire(self, cost: int, *, bucket: Bucket) -> None:
        _ = (cost, bucket)

    def resize(self, *, read: BucketLimits, write: BucketLimits) -> None:
        self.resizes.append((read, write))


class FakeRest:
    """Canned REST answers by path, and for a ``/markets`` listing filtered by series by that
    series; a queue serves in order, then repeats its last answer."""

    def __init__(self, markets: Sequence[Mapping[str, object]]) -> None:
        self.requests: list[httpx.Request] = []
        # A path listed here answers only once its event is set, so a test can hold a request
        # in flight, for example a universe refresh that a shutdown catches mid-listing.
        self.holds: dict[str, asyncio.Event] = {}
        self.series_answers: dict[str, list[httpx.Response]] = {}
        self.answers: dict[str, list[httpx.Response]] = {
            "/exchange/status": [
                httpx.Response(200, json={"exchange_active": True, "trading_active": True})
            ],
            "/account/limits": [httpx.Response(200, json=LIMITS)],
            "/markets": [httpx.Response(200, json={"markets": list(markets), "cursor": ""})],
            "/series": [
                httpx.Response(
                    200,
                    json={
                        "series": [
                            series_payload("KXA", TEST_CATEGORY),
                            series_payload("KXLOW", TEST_CATEGORY),
                        ]
                    },
                )
            ],
        }

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path.removeprefix("/trade-api/v2")
        hold = self.holds.get(path)
        if hold is not None:
            await hold.wait()
        series = request.url.params.get("series_ticker")
        queue = self.answers[path] if series is None else self.series_answers[series]
        return queue.pop(0) if len(queue) > 1 else queue[0]

    def listings(self, series: str | None = None) -> int:
        """``/markets`` requests so far: full listings, or those filtered by ``series``."""
        return sum(
            1
            for request in self.requests
            if request.url.path.endswith("/markets")
            and request.url.params.get("series_ticker") == series
        )


class HostClock:
    """The recorder's clock: a frozen clock whose wall reading also jumps while the host sleeps.

    Monotonic clocks stop during sleep on macOS and Linux; wall clocks keep going.
    """

    def __init__(self, frozen: FrozenClock) -> None:
        self._frozen = frozen
        self._slept_ns = 0

    def mono_ns(self) -> Ns:
        return self._frozen.mono_ns()

    def wall_ns(self) -> Ns:
        return Ns(self._frozen.wall_ns() + self._slept_ns)

    def sleep(self, duration_ns: int) -> None:
        self._slept_ns += duration_ns


class VirtualTime:
    """The injected sleep: a sleeper wakes only when the test moves the clock past its deadline."""

    def __init__(self, clock: FrozenClock) -> None:
        self.clock = clock
        self.requested: list[float] = []
        self._sleepers: list[tuple[int, asyncio.Future[None]]] = []

    @property
    def sleepers(self) -> int:
        return sum(1 for _, future in self._sleepers if not future.done())

    async def sleep(self, seconds: float) -> None:
        self.requested.append(seconds)
        entry = (
            int(self.clock.mono_ns()) + round(seconds * NS_PER_S),
            asyncio.get_running_loop().create_future(),
        )
        self._sleepers.append(entry)
        try:
            await entry[1]
        finally:
            self._sleepers.remove(entry)

    def advance(self, seconds: int) -> None:
        self.clock.advance(seconds * NS_PER_S)
        for deadline, future in self._sleepers:
            if deadline <= self.clock.mono_ns() and not future.done():
                future.set_result(None)


class Harness:
    """A recorder wired to the fake exchange, a fake REST API, real sinks, and virtual time."""

    def __init__(
        self,
        ws_url: str,
        root: Path,
        *,
        markets: Sequence[Mapping[str, object]] = MARKETS,
        periodic_tasks: Sequence[PeriodicTask] = (),
        publisher: Publisher | None = None,
        broken_conn: int | None = None,
        broken_headers: bool = False,
        **config: Any,  # RecorderConfig fields under test
    ) -> None:
        self.root = root
        self.clock = FrozenClock(mono_ns=1, wall_ns=NOON)
        self.host_clock = HostClock(self.clock)
        self.time = VirtualTime(self.clock)
        self.limiter = RecordingLimiter()
        self.rest = FakeRest(markets)
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(self.rest), base_url=REST_URL)
        self.sinks: dict[int, SegmentSink] = {}
        self.broken_conn = broken_conn
        self.broken_headers = broken_headers
        # With a bus, the refresh cycle is one more loop parked between its slices.
        self.loops = LOOPS if publisher is None else LOOPS + 1
        settings: dict[str, Any] = {  # Any: RecorderConfig field values of several types
            "env": "demo",
            "ws_url": ws_url,
            "data_dir": root,
            "host": "test-host",
            "universe": POLICY,
            "book_connections": 2,
            "group_size": 2,
            "shutdown_timeout_s": 2,
        }
        self.recorder = Recorder(
            RecorderConfig(**(settings | config)),
            clock=self.host_clock,
            rest=KalshiRest(REST_URL, self.http, self.limiter, self.clock),
            limiter=self.limiter,
            session_builder=self.session,
            sink_builder=self.sink,
            sleep=self.time.sleep,
            jitter=lambda: 0.5,
            periodic_tasks=periodic_tasks,
            publisher=publisher,
        )
        self.task: asyncio.Task[None] | None = None

    def session(self, url: str, *, conn_id: int) -> WsSession:
        if conn_id == self.broken_conn:
            raise RuntimeError(f"no session for connection {conn_id}")
        # The query string tells the fake which connection it is.
        return WsSession(
            f"{url}?conn={conn_id}",
            StubSigner(),
            FrozenClock(mono_ns=1, wall_ns=NOON),
            conn_id=conn_id,
            connect_timeout_ns=2 * NS_PER_S,
        )

    def sink(self, *, conn_id: int, header_factory: HeaderFactory) -> SegmentSink:
        def broken_header() -> SegmentHeader:
            raise RuntimeError("header bug")

        sink = SegmentSink(
            self.root,
            conn_id=conn_id,
            header_factory=broken_header if self.broken_headers else header_factory,
            clock=self.clock,
            poll_interval_ns=2 * NS_PER_MS,
        )
        self.sinks[conn_id] = sink
        return sink

    def start(self) -> None:
        self.task = asyncio.create_task(self.recorder.run())

    async def close(self) -> None:
        await self.recorder.stop()
        if self.task is not None:
            await asyncio.wait({self.task}, timeout=10)
        await self.http.aclose()

    async def connection(self, fake: FakeKalshiWs, conn_id: int) -> FakeConnection:
        suffix = f"?conn={conn_id}"
        await until(lambda: any(c.path.endswith(suffix) for c in fake.connections))
        return next(c for c in fake.connections if c.path.endswith(suffix))

    async def subscribed(self, *conn_ids: int) -> None:
        supervisors = self.recorder.supervisors
        await until(lambda: all(supervisors[conn_id].subscriptions for conn_id in conn_ids))

    async def parked(self) -> None:
        await until(lambda: self.time.sleepers == self.loops)

    def tape(self, conn_id: int) -> list[tuple[SegmentHeader, list[Record], bool]]:
        segments = []
        for path in sorted(self.root.glob(f"raw/*/*/conn-{conn_id:02d}-*.tape.zst")):
            with SegmentReader(path) as reader:
                records = list(reader.records())
                segments.append((reader.header, records, reader.truncated))
        return segments


@asynccontextmanager
async def recording(ws_url: str, root: Path, **kwargs: Any) -> AsyncIterator[Harness]:
    harness = Harness(ws_url, root, **kwargs)
    try:
        yield harness
    finally:
        await harness.close()


async def until(predicate: Callable[[], bool], *, timeout_s: float = 5.0) -> None:
    """Wait for a condition the recorder cannot signal, without a fixed sleep."""
    async with asyncio.timeout(timeout_s):
        while not predicate():
            await asyncio.sleep(0.001)


def fresh(books: Mapping[str, Book], *tickers: str) -> bool:
    return all(ticker in books and not books[ticker].is_stale() for ticker in tickers)


def frame_types(records: Sequence[Record]) -> list[str]:
    return [
        msgspec.json.decode(record.payload, type=dict[str, Any])["type"]
        for record in records
        if record.kind is RecordKind.FRAME
    ]


def logged(caplog: pytest.LogCaptureFixture, message: str) -> list[logging.LogRecord]:
    return [record for record in caplog.records if record.getMessage().startswith(message)]


async def test_startup_sizes_the_limiter_and_subscribes_the_planned_groups(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.start()
        ticker = await harness.connection(fake, TICKER_CONN_ID)
        control = await harness.connection(fake, CONTROL_CONN_ID)
        first_books = await harness.connection(fake, 2)
        second_books = await harness.connection(fake, 3)

        # The plan's markets, never every market (ADR 0027), two per command at group_size 2.
        assert await ticker.wait_for_commands(2) == [
            {
                "id": 1,
                "cmd": "subscribe",
                "params": {"channels": ["ticker"], "market_tickers": ["KXA-1", "KXA-2"]},
            },
            {
                "id": 2,
                "cmd": "update_subscription",
                "params": {
                    "sid": 1,
                    "action": "add_markets",
                    "market_tickers": ["KXA-3", "KXSHOW-1"],
                },
            },
        ]
        assert ticker.subscriptions == {
            1: {"channel": "ticker", "market_tickers": ["KXA-1", "KXA-2", "KXA-3", "KXSHOW-1"]}
        }
        assert await control.wait_for_commands(1) == [
            {"id": 1, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}
        ]
        for connection, tickers in (
            (first_books, ["KXA-1", "KXA-3"]),
            (second_books, ["KXA-2", "KXSHOW-1"]),
        ):
            (command,) = await connection.wait_for_commands(1)
            assert command["params"] == {
                "channels": ["orderbook_delta", "trade"],
                "market_tickers": tickers,
                "use_yes_price": True,
            }

        assert harness.limiter.resizes == [
            (
                BucketLimits(refill_per_s=300, capacity=600),
                BucketLimits(refill_per_s=150, capacity=300),
            )
        ]
        paths = [request.url.path for request in harness.rest.requests]
        assert paths == [
            "/trade-api/v2/exchange/status",
            "/trade-api/v2/account/limits",
            "/trade-api/v2/markets",
            "/trade-api/v2/series",
        ]
        listing = harness.rest.requests[2].url.params
        assert (listing["status"], listing["limit"], listing["mve_filter"]) == (
            "open",
            "1000",
            "exclude",
        )
        assert dict(harness.rest.requests[3].url.params) == {"category": TEST_CATEGORY}
        universe = harness.recorder.universe
        assert universe is not None
        assert universe.l2_tickers == {"KXA-1", "KXA-2", "KXA-3", "KXSHOW-1"}
        assert (universe.reason_counts["below_volume"], universe.reason_counts["not_active"]) == (
            1,
            1,
        )
        assert harness.recorder.sink_for("KXSHOW-1") is harness.sinks[3]
        assert harness.recorder.sink_for("KXLOW-1") is None
        assert sorted(harness.sinks) == [CONTROL_CONN_ID, 2, 3]


async def test_capture_tapes_lifecycle_and_books_keyframes_them_and_never_tapes_tickers(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        fake.set_book("KXA-1", yes=[("0.4000", "10.00")], no=[("0.6000", "5.00")])
        fake.set_book("KXSHOW-1", yes=[("0.2000", "1.00")])
        harness.start()
        ticker = await harness.connection(fake, TICKER_CONN_ID)
        control = await harness.connection(fake, CONTROL_CONN_ID)
        books = await harness.connection(fake, 2)
        await harness.subscribed(TICKER_CONN_ID, CONTROL_CONN_ID, 2, 3)
        await until(lambda: fresh(harness.recorder.books(), "KXA-1", "KXSHOW-1"))

        await ticker.push_message(
            "ticker", {"market_ticker": "KXA-1", "ts_ms": 5, "volume_fp": "7.00"}, sid=1
        )
        await control.push_sequenced(
            "market_lifecycle_v2", {"event_type": "activated", "market_ticker": "KXNEW-1"}, sid=1
        )
        await books.push_sequenced(
            "orderbook_delta",
            {
                "market_ticker": "KXA-1",
                "price_dollars": "0.4100",
                "delta_fp": "1.00",
                "side": "yes",
                "ts_ms": 1_789_000_000_000,
            },
            sid=1,
        )
        await until(lambda: "KXA-1" in harness.recorder.latest_tickers())
        await until(
            lambda: (
                harness.recorder.books()["KXA-1"].best_bid() == Level(PriceE4(4100), CountE2(100))
            )
        )
        await until(lambda: harness.recorder.supervisors[CONTROL_CONN_ID].stats.frames == 2)

        await harness.parked()
        harness.time.advance(300)
        periodic = tmp_path / "keyframes" / "2026-09-10" / "12" / "05.parquet"
        await until(periodic.exists)
        rebuilt = books_from_keyframe_rows(read_keyframe(periodic))
        live = harness.recorder.books()
        assert sorted(rebuilt) == ["KXA-1", "KXSHOW-1"]
        for name, book in rebuilt.items():
            for side in (Side.BID, Side.ASK):
                assert book.levels(side) == live[name].levels(side)

        await harness.parked()
        harness.time.advance(90)  # only the status loop is due
        await until(lambda: len(logged(caplog, "recorder status")) == 2)
        await harness.parked()
        await harness.recorder.stop()
        await harness.recorder.stop()
        assert harness.task is not None
        assert harness.task.done()
        assert harness.task.exception() is None

    status = logged(caplog, "recorder status")[-1].__dict__
    assert [c["conn_id"] for c in status["connections"]] == [0, 1, 2, 3]
    assert [c["taped"] for c in status["connections"]] == [False, True, True, True]
    assert (status["universe_size"], status["subscribed_markets"], status["live_tickers"]) == (
        4,
        4,
        1,
    )

    final = tmp_path / "keyframes" / "2026-09-10" / "12" / "06.parquet"
    assert {row.as_of_recv_ns for row in read_keyframe(final)} == {NOON + 390 * NS_PER_S}
    assert {row.as_of_recv_ns for row in read_keyframe(periodic)} == {NOON + 300 * NS_PER_S}

    assert list(tmp_path.glob("raw/*/*/conn-00-*")) == []
    everything = b"".join(
        record.payload
        for conn_id in (CONTROL_CONN_ID, 2, 3)
        for _, records, _ in harness.tape(conn_id)
        for record in records
    )
    assert b'"type":"ticker"' not in everything

    ((header, records, truncated),) = harness.tape(CONTROL_CONN_ID)
    assert not truncated
    assert "market_lifecycle_v2" in frame_types(records)
    assert (header.conn_id, header.env, header.host, header.ws_url) == (
        CONTROL_CONN_ID,
        "demo",
        "test-host",
        fake.url,
    )
    assert (header.software_version, header.spec_versions) == (
        __version__,
        {"openapi": "3.30.0", "asyncapi": "2.0.0"},
    )
    ((header, records, truncated),) = harness.tape(2)
    assert not truncated
    assert header.use_yes_price
    assert frame_types(records).count("orderbook_delta") == 1
    assert "orderbook_snapshot" in frame_types(records)
    assert harness.sinks[2].put(records[0]) is False  # closed


async def test_a_book_tap_spans_the_connections_the_plan_assigns_its_markets_to(
    tmp_path: Path,
) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        fake.set_book("KXA-1", yes=[("0.4000", "10.00")])
        fake.set_book("KXSHOW-1", yes=[("0.2000", "1.00")])
        harness.start()
        books = await harness.connection(fake, 2)
        await harness.subscribed(2, 3)
        await until(lambda: fresh(harness.recorder.books(), "KXA-1", "KXSHOW-1"))
        supervisors = harness.recorder.supervisors
        with pytest.raises(ValueError, match="max_events must be positive"):
            harness.recorder.open_book_tap(["KXA-1"], max_events=0)
        assert all(not supervisor.tapped_tickers for supervisor in supervisors.values())

        tap = harness.recorder.open_book_tap(
            ["KXSHOW-1", "KXA-1", "KXLOW-1", "KXA-1"], max_events=5
        )
        assert (supervisors[2].tapped_tickers, supervisors[3].tapped_tickers) == (
            frozenset({"KXA-1"}),
            frozenset({"KXSHOW-1"}),
        )
        await books.push_sequenced(
            "orderbook_delta",
            {
                "market_ticker": "KXA-1",
                "price_dollars": "0.4100",
                "delta_fp": "1.00",
                "side": "yes",
                "ts_ms": 1_789_000_000_000,
            },
            sid=1,
        )
        await until(
            lambda: (
                harness.recorder.books()["KXA-1"].best_bid() == Level(PriceE4(4100), CountE2(100))
            )
        )
        windows = tap.close()
        assert tap.close() is windows
        assert all(not supervisor.tapped_tickers for supervisor in supervisors.values())

    assert sorted(windows) == ["KXA-1", "KXLOW-1", "KXSHOW-1"]
    tapped = windows["KXA-1"]
    assert tapped.start == BookImage(bids=(Level(PriceE4(4000), CountE2(1000)),), asks=())
    assert [(type(e), e.price) for e in tapped.events if isinstance(e, BookDelta)] == [
        (BookDelta, PriceE4(4100))
    ]
    assert (len(tapped.events), tapped.fault) == (1, None)
    assert (windows["KXSHOW-1"].events, windows["KXSHOW-1"].fault) == ((), None)
    # A market the plan does not place has no book anywhere.
    assert windows["KXLOW-1"] == TapWindow.absent("KXLOW-1")


class FailingTask:
    async def run(self, *, stop: asyncio.Event) -> None:
        _ = stop
        raise RuntimeError("auditor bug")


class PatientTask:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.stopped = False

    async def run(self, *, stop: asyncio.Event) -> None:
        self.started.set()
        await stop.wait()
        self.stopped = True


async def test_a_failing_periodic_task_is_logged_and_capture_continues(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    patient = PatientTask()
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, periodic_tasks=[FailingTask(), patient]) as harness,
    ):
        harness.start()
        control = await harness.connection(fake, CONTROL_CONN_ID)
        await until(lambda: bool(logged(caplog, "periodic task failed")))
        async with asyncio.timeout(5):
            await patient.started.wait()
        await harness.subscribed(CONTROL_CONN_ID)
        await control.push_sequenced(
            "market_lifecycle_v2", {"event_type": "settled", "market_ticker": "KXA-1"}, sid=1
        )
        await until(lambda: harness.recorder.supervisors[CONTROL_CONN_ID].stats.frames == 2)
        await harness.recorder.stop()
        assert harness.task is not None
        assert harness.task.exception() is None

    assert patient.stopped
    (failure,) = logged(caplog, "periodic task failed")
    assert failure.__dict__["task"] == "FailingTask-0"
    ((_, records, _),) = harness.tape(CONTROL_CONN_ID)
    assert "market_lifecycle_v2" in frame_types(records)


async def test_a_supervisor_crash_shuts_everything_down_and_is_raised(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path, broken_conn=3) as harness:
        harness.start()
        assert harness.task is not None
        with pytest.raises(RuntimeError, match="no session for connection 3"):
            await asyncio.wait_for(harness.task, timeout=10)

    (failure,) = logged(caplog, "recorder component failed")
    assert failure.__dict__["component"] == "supervisor-3"
    for conn_id in (CONTROL_CONN_ID, 2):
        assert all(not truncated for _, _, truncated in harness.tape(conn_id))
    assert all(
        sink.put(
            Record(kind=RecordKind.GAP, conn_id=1, recv_mono_ns=0, recv_wall_ns=0, payload=b"")
        )
        is False
        for sink in harness.sinks.values()
    )


async def test_a_dead_writer_thread_stops_the_recorder_at_the_next_status_check(
    tmp_path: Path,
) -> None:
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, broken_headers=True) as harness,
    ):
        harness.start()
        await until(lambda: harness.sinks[CONTROL_CONN_ID].failure is not None)
        await harness.parked()
        harness.time.advance(60)
        assert harness.task is not None
        with pytest.raises(RuntimeError, match="segment sink 1 failed") as raised:
            await asyncio.wait_for(harness.task, timeout=10)
    assert isinstance(raised.value.__cause__, RuntimeError)
    assert str(raised.value.__cause__) == "header bug"


async def test_a_failed_universe_refresh_keeps_the_plan_and_retries_on_a_short_backoff(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.rest.answers["/markets"][:0] = [httpx.Response(503), httpx.Response(503)]
        harness.start()
        books = await harness.connection(fake, 2)
        await until(lambda: bool(logged(caplog, "universe refresh failed")))
        assert harness.recorder.universe is None
        assert harness.recorder.supervisors[2].group is None

        # With jitter 0.5 the first retry waits 0.75 of 15 s, not the 300-second interval.
        await harness.parked()
        harness.time.advance(11)
        await harness.parked()
        assert len(logged(caplog, "universe refresh failed")) == 1
        harness.time.advance(1)
        await until(lambda: len(logged(caplog, "universe refresh failed")) == 2)

        await harness.parked()
        harness.time.advance(23)
        (command,) = await books.wait_for_commands(1)
        assert command["params"]["market_tickers"] == ["KXA-1", "KXA-3"]
        await until(lambda: harness.recorder.universe is not None)
        # Success resets the backoff: the next refresh is a full interval away.
        await harness.parked()
        assert harness.time.requested[-1] == 300

    failures = logged(caplog, "universe refresh failed")
    assert [f.__dict__["retry_in_s"] for f in failures] == [11.25, 22.5]
    assert [f.__dict__["consecutive_failures"] for f in failures] == [1, 2]


def test_universe_retries_double_from_15_seconds_and_cap_at_the_refresh_interval() -> None:
    nominal = [2 * universe_retry_delay_s(k, refresh_s=300, jitter=0.0) for k in range(7)]
    assert nominal == [15.0, 30.0, 60.0, 120.0, 240.0, 300.0, 300.0]
    assert universe_retry_delay_s(10_000, refresh_s=300, jitter=0.0) == 150.0
    assert universe_retry_delay_s(0, refresh_s=300, jitter=0.5) == 11.25
    # An interval shorter than the first retry caps every retry, the first one included.
    assert universe_retry_delay_s(0, refresh_s=10, jitter=0.0) == 5.0
    with pytest.raises(ValueError, match="non-negative"):
        universe_retry_delay_s(-1, refresh_s=300, jitter=0.5)
    with pytest.raises(ValueError, match="jitter"):
        universe_retry_delay_s(0, refresh_s=300, jitter=1.0)


def listing(*markets: Mapping[str, object]) -> list[httpx.Response]:
    """A ``/markets`` answer holding exactly these markets on one page."""
    return [httpx.Response(200, json={"markets": list(markets), "cursor": ""})]


async def test_the_ticker_subscription_and_table_follow_the_plan_while_the_bus_gets_everything(
    tmp_path: Path,
) -> None:
    """ADR 0027: growth adds a market to the one ticker subscription, shrinkage removes it
    from the subscription and from the table, and every update still reaches the bus."""
    publisher = RecordingPublisher()
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, publisher=publisher) as harness,
    ):
        recorder = harness.recorder

        def published_tickers() -> list[str]:
            return [e.event.ticker for e in publisher.envelopes if isinstance(e.event, Ticker)]

        async def push_tickers(*names: str) -> None:
            before = len(published_tickers())
            for name in names:
                await ticker.push_message("ticker", {"market_ticker": name, "ts_ms": 5}, sid=1)
            await until(lambda: len(published_tickers()) == before + len(names))

        harness.start()
        ticker = await harness.connection(fake, TICKER_CONN_ID)
        await ticker.wait_for_commands(2)
        await harness.subscribed(TICKER_CONN_ID, 2, 3)
        # KXLOW-1 is below the volume floor: published, but not kept.
        await push_tickers("KXA-1", "KXA-3", "KXLOW-1")
        assert sorted(recorder.latest_tickers()) == ["KXA-1", "KXA-3"]

        # KXA-3 leaves the universe and KXA-4 joins it.
        harness.rest.answers["/markets"] = listing(
            market("KXA-1", "5000.00"),
            market("KXA-2", "4000.00"),
            market("KXA-4", "3500.00"),
            market("KXSHOW-1", "0.00"),
        )
        await harness.parked()
        harness.time.advance(300)
        commands = await ticker.wait_for_commands(4)
        assert [c["params"] for c in commands[2:]] == [
            {"sid": 1, "action": "delete_markets", "market_tickers": ["KXA-3"]},
            {"sid": 1, "action": "add_markets", "market_tickers": ["KXA-4"]},
        ]
        await until(lambda: "KXA-3" not in recorder.latest_tickers())
        assert sorted(ticker.subscriptions[1]["market_tickers"]) == [
            "KXA-1",
            "KXA-2",
            "KXA-4",
            "KXSHOW-1",
        ]
        ticker_group = recorder.supervisors[TICKER_CONN_ID].group
        assert ticker_group is not None
        assert (ticker_group.group_id, ticker_group.tickers) == (
            TICKER_GROUP_ID,
            frozenset({"KXA-1", "KXA-2", "KXA-4", "KXSHOW-1"}),
        )
        # A straggler for the market just removed is published, and never kept again.
        await push_tickers("KXA-3", "KXA-4")
        assert sorted(recorder.latest_tickers()) == ["KXA-1", "KXA-4"]
        status = recorder.status()
        # The ticker connection's group is not counted among the book subscriptions.
        assert (status.universe_size, status.subscribed_markets, status.live_tickers) == (4, 4, 2)

        # An empty universe leaves nothing to subscribe and nothing to keep.
        harness.rest.answers["/markets"] = listing()
        await harness.parked()
        harness.time.advance(300)
        commands = await ticker.wait_for_commands(5)
        assert commands[4] == {"id": 5, "cmd": "unsubscribe", "params": {"sids": [1]}}
        await until(lambda: not recorder.latest_tickers())
        assert recorder.status().live_tickers == 0

    assert published_tickers() == ["KXA-1", "KXA-3", "KXLOW-1", "KXA-3", "KXA-4"]


def test_a_clock_jump_is_wall_time_outrunning_monotonic_time_by_over_five_seconds() -> None:
    minute = 60 * NS_PER_S
    assert CLOCK_JUMP_THRESHOLD_NS == 5 * NS_PER_S
    assert not is_clock_jump(wall_ns_delta=minute, mono_ns_delta=minute)
    assert not is_clock_jump(wall_ns_delta=minute + 5 * NS_PER_S, mono_ns_delta=minute)
    assert is_clock_jump(wall_ns_delta=minute + 5 * NS_PER_S + 1, mono_ns_delta=minute)
    assert is_clock_jump(wall_ns_delta=6 * minute, mono_ns_delta=minute)
    # A wall clock stepped back loses nothing from the tape, so it is not a jump.
    assert not is_clock_jump(wall_ns_delta=0, mono_ns_delta=minute)
    assert is_clock_jump(wall_ns_delta=2, mono_ns_delta=0, threshold_ns=1)


async def test_a_host_sleep_writes_one_clock_jump_record_to_each_taped_connection(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.start()

        async def status_lines(count: int) -> None:
            await until(lambda: len(logged(caplog, "recorder status")) == count)

        await harness.subscribed(TICKER_CONN_ID, CONTROL_CONN_ID, 2, 3)
        for tick, slept_s in enumerate((0, 300, 0), start=1):
            await harness.parked()
            harness.host_clock.sleep(slept_s * NS_PER_S)
            harness.time.advance(60)
            await status_lines(tick)
        await harness.parked()

    (warning,) = logged(caplog, "wall clock jumped")
    assert warning.levelno == logging.WARNING
    expected = {
        "event": "clock_jump",
        "wall_ns_delta": 360 * NS_PER_S,
        "mono_ns_delta": 60 * NS_PER_S,
    }
    assert (warning.__dict__["wall_ns_delta"], warning.__dict__["mono_ns_delta"]) == (
        expected["wall_ns_delta"],
        expected["mono_ns_delta"],
    )
    for conn_id in (CONTROL_CONN_ID, 2, 3):
        events = [
            msgspec.json.decode(record.payload, type=dict[str, Any])
            for _, records, _ in harness.tape(conn_id)
            for record in records
            if record.kind is RecordKind.CONNECTION
        ]
        assert [e for e in events if e["event"] == "clock_jump"] == [expected]
    assert list(tmp_path.glob("raw/*/*/conn-00-*")) == []


async def test_a_reconnected_ticker_connection_resubscribes_the_current_plan_in_batches(
    tmp_path: Path,
) -> None:
    def ticker_connections() -> list[FakeConnection]:
        return [c for c in fake.connections if c.path.endswith(f"?conn={TICKER_CONN_ID}")]

    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.start()
        first = await harness.connection(fake, TICKER_CONN_ID)
        await first.wait_for_commands(2)
        harness.rest.answers["/markets"] = listing(
            market("KXA-1", "5000.00"), market("KXA-2", "4000.00"), market("KXA-4", "3500.00")
        )
        await harness.parked()
        harness.time.advance(300)
        await first.wait_for_commands(4)
        await harness.parked()

        await first.close_abruptly()
        await until(lambda: harness.time.sleepers == LOOPS + 1)  # the reconnect backoff
        harness.time.advance(1)
        await until(lambda: len(ticker_connections()) == 2)
        second = ticker_connections()[1]
        commands = await second.wait_for_commands(2)
        # Rebuilt from the plan as it is now, not replayed from the first connection.
        assert [(c["cmd"], c["params"]) for c in commands] == [
            ("subscribe", {"channels": ["ticker"], "market_tickers": ["KXA-1", "KXA-2"]}),
            (
                "update_subscription",
                {"sid": 1, "action": "add_markets", "market_tickers": ["KXA-4"]},
            ),
        ]
        assert second.subscriptions == {
            1: {"channel": "ticker", "market_tickers": ["KXA-1", "KXA-2", "KXA-4"]}
        }
        assert harness.recorder.supervisors[TICKER_CONN_ID].stats.reconnects == 1


async def test_listing_quirks_are_logged_and_never_fatal(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    unreadable = market("KXBAD-1", "lots")
    async with (
        FakeKalshiWs() as fake,
        recording(
            fake.url,
            tmp_path,
            universe=msgspec.structs.replace(POLICY, exclude_mve=False),
            max_market_pages=1,
        ) as harness,
    ):
        harness.rest.answers["/exchange/status"] = [
            httpx.Response(200, json={"exchange_active": False, "trading_active": False})
        ]
        harness.rest.answers["/markets"] = [
            httpx.Response(
                200, json={"markets": [market("KXA-1", "5000.00"), unreadable], "cursor": "more"}
            )
        ]
        harness.start()
        await until(lambda: harness.recorder.universe is not None)

    (status,) = logged(caplog, "exchange status")
    assert status.levelno == logging.WARNING
    (skipped,) = logged(caplog, "markets skipped")
    assert skipped.__dict__["skipped"] == 1
    assert logged(caplog, "market listing cut short by the page cap")
    (listing,) = [r for r in harness.rest.requests if r.url.path.endswith("/markets")]
    assert "mve_filter" not in listing.url.params
    assert harness.recorder.universe is not None
    assert harness.recorder.universe.l2_tickers == {"KXA-1"}


async def test_groups_drive_the_plan_and_the_ticker_subscription_as_categories_arrive(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """ADR 0028 with ADR 0027: until the series categories arrive only the series group
    records; once they do, the category group's markets join the book and ticker
    subscriptions, and the categories are not requested again within the hour."""
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")

    def series_requests() -> int:
        return sum(1 for request in harness.rest.requests if request.url.path.endswith("/series"))

    def refreshes() -> list[dict[str, Any]]:  # Any: structured log fields of several types
        return [record.__dict__ for record in logged(caplog, "universe refreshed")]

    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        recorder = harness.recorder
        harness.rest.answers["/series"].insert(0, httpx.Response(503))
        harness.start()
        ticker = await harness.connection(fake, TICKER_CONN_ID)
        (subscribe,) = await ticker.wait_for_commands(1)
        assert subscribe["params"] == {"channels": ["ticker"], "market_tickers": ["KXSHOW-1"]}
        assert recorder.universe is not None
        assert recorder.universe.l2_tickers == {"KXSHOW-1"}
        (unknown,) = logged(caplog, "series categories unknown; category groups admit nothing")
        assert unknown.__dict__["categories"] == [TEST_CATEGORY]

        await harness.parked()
        harness.time.advance(300)
        commands = await ticker.wait_for_commands(3)
        assert [command["params"] for command in commands[1:]] == [
            {"sid": 1, "action": "add_markets", "market_tickers": ["KXA-1", "KXA-2"]},
            {"sid": 1, "action": "add_markets", "market_tickers": ["KXA-3"]},
        ]
        await until(lambda: len(refreshes()) == 2)
        universe = recorder.universe
        assert universe is not None
        assert universe.group_of == {
            "KXSHOW-1": "showcase",
            "KXA-1": "busiest",
            "KXA-2": "busiest",
            "KXA-3": "busiest",
        }
        assert universe.showcase == {"KXSHOW-1"}
        planned: set[str] = set()
        for conn_id in (2, 3):
            group = recorder.supervisors[conn_id].group
            assert group is not None
            planned |= group.tickers
        assert planned == universe.l2_tickers
        # Moving a market costs a resnapshot, so the one planned first stays put.
        assert recorder.sink_for("KXSHOW-1") is harness.sinks[2]

        await harness.parked()
        harness.time.advance(300)
        await until(lambda: len(refreshes()) == 3)
        assert series_requests() == 2
        await harness.parked()
        harness.time.advance(3600)
        await until(lambda: len(refreshes()) == 4)
        assert series_requests() == 3

    first, second = refreshes()[:2]
    assert first["groups"] == {
        "showcase": {"admitted": 1, "events": 1, "skipped_for_budget": 0},
        "busiest": {"admitted": 0, "events": 0, "skipped_for_budget": 0},
    }
    assert first["reason_counts"]["no_group"] == 4
    assert second["groups"]["busiest"] == {"admitted": 3, "events": 1, "skipped_for_budget": 0}
    assert (second["reason_counts"]["below_volume"], second["reason_counts"]["no_group"]) == (1, 0)


async def test_a_universe_without_category_groups_never_requests_series(tmp_path: Path) -> None:
    series_only = msgspec.structs.replace(POLICY, groups=POLICY.groups[:1])
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, universe=series_only) as harness,
    ):
        harness.start()
        await until(lambda: harness.recorder.universe is not None)
        universe = harness.recorder.universe
        assert universe is not None
        assert universe.l2_tickers == {"KXSHOW-1"}
    assert not any(request.url.path.endswith("/series") for request in harness.rest.requests)


async def test_a_universe_without_groups_warns_at_every_refresh(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    empty = msgspec.structs.replace(POLICY, groups=())
    warning = "no universe groups are configured; no market will be recorded"
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, universe=empty) as harness,
    ):
        harness.start()
        await until(lambda: harness.recorder.universe is not None)
        await harness.parked()
        harness.time.advance(300)
        await until(lambda: len(logged(caplog, warning)) == 2)
        universe = harness.recorder.universe
        assert universe is not None
        assert universe.l2_tickers == frozenset()
        # Every active market: KXA-1 to KXA-3, KXSHOW-1, and KXLOW-1.
        assert universe.reason_counts["no_group"] == 5

    assert all(record.levelno == logging.WARNING for record in logged(caplog, warning))


async def test_a_startup_failure_is_raised_before_anything_connects(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.rest.answers["/account/limits"] = [httpx.Response(401)]
        with pytest.raises(KalshiHttpError) as raised:
            await harness.recorder.run()
        assert raised.value.status == 401
        await harness.recorder.stop()
        assert list(fake.connections) == []
    assert list(tmp_path.iterdir()) == []


async def test_a_recorder_stopped_before_it_runs_does_nothing_and_is_single_use(
    tmp_path: Path,
) -> None:
    async with recording("ws://127.0.0.1:1/unused", tmp_path) as harness:
        await harness.recorder.stop()
        await asyncio.wait_for(harness.recorder.run(), timeout=5)
        assert harness.rest.requests == []
        with pytest.raises(RuntimeError, match="single-use"):
            await harness.recorder.run()


def test_keyframe_paths_floor_to_the_slot_in_utc() -> None:
    root = Path("/data")
    moment = NOON + (7 * 60 + 30) * NS_PER_S
    assert (
        keyframe_path(root, moment, interval_s=300) == root / "keyframes/2026-09-10/12/05.parquet"
    )
    assert keyframe_path(root, moment, interval_s=60) == root / "keyframes/2026-09-10/12/07.parquet"
    assert (
        keyframe_path(root, NOON - 1, interval_s=3600)
        == root / "keyframes/2026-09-10/11/00.parquet"
    )
    with pytest.raises(ValueError, match="non-negative"):
        keyframe_path(root, -1, interval_s=300)
    with pytest.raises(ValueError, match="divides an hour"):
        keyframe_path(root, NOON, interval_s=30)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"book_connections": 0}, "at least 1"),
        ({"book_connections": 3, "max_connections": 4}, "needs 5 connections"),
        ({"keyframe_interval_s": 600 + 1}, "divides an hour"),
        ({"group_size": 501}, r"group_size must be in \[1, 500\]"),
        (
            {"group_size": 1, "book_connections": 3},
            r"max_l2_markets = 4 needs 4 book connections at group_size = 1",
        ),
        ({"universe_refresh_s": 0}, "universe_refresh_s must be positive"),
        ({"status_interval_s": -1}, "status_interval_s must be positive"),
        ({"max_market_pages": 0}, "max_market_pages must be positive"),
        ({"shutdown_timeout_s": 0}, "shutdown_timeout_s must be positive"),
        ({"keyframe_write_timeout_s": 0}, "keyframe_write_timeout_s must be positive"),
        ({"bus_refresh_s": 0}, "bus_refresh_s must be positive"),
    ],
)
def test_a_recorder_config_that_cannot_work_is_refused(
    overrides: dict[str, int], message: str
) -> None:
    fields: dict[str, Any] = {  # Any: RecorderConfig field values of several types
        "env": "demo",
        "ws_url": "ws://x",
        "data_dir": Path("data"),
        "host": "h",
        "universe": POLICY,
    }
    with pytest.raises(ValueError, match=message):
        RecorderConfig(**(fields | overrides))
    check_connection_budget(book_connections=14, max_connections=16)


async def test_stop_does_not_wait_for_a_universe_refresh_in_flight(tmp_path: Path) -> None:
    """A shutdown cancels a refresh caught mid-listing instead of waiting it out.

    A production listing runs to a hundred-odd pages. Before the fix the refresh ran to
    completion during shutdown; held open here, it would last the whole 30-second
    shutdown deadline, so a prompt stop proves the refresh was cancelled.
    """

    def listings() -> int:
        return sum(1 for request in harness.rest.requests if request.url.path.endswith("/markets"))

    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, shutdown_timeout_s=30) as harness,
    ):
        harness.start()
        await harness.connection(fake, 2)
        await until(lambda: harness.recorder.universe is not None)
        await harness.parked()
        harness.rest.holds["/markets"] = asyncio.Event()
        before = listings()
        harness.time.advance(300)
        await until(lambda: listings() > before)
        await asyncio.wait_for(harness.recorder.stop(), timeout=5.0)


def delta_msg(ticker: str, price: str, delta: str) -> dict[str, object]:
    return {
        "market_ticker": ticker,
        "price_dollars": price,
        "delta_fp": delta,
        "side": "yes",
        "ts_ms": 1_789_000_000_000,
    }


def test_refresh_slices_spread_a_cycle_evenly_and_never_leave_it_without_a_pause() -> None:
    assert refresh_slices([], interval_s=10) == ((),)
    assert refresh_slices(["A", "B"], interval_s=10) == (("A",), ("B",))
    tickers = [f"KXM-{index:04d}" for index in range(2005)]
    slices = refresh_slices(tickers, interval_s=10)
    assert len(slices) == 100  # 10 slices a second: a step every 100 ms
    assert [ticker for piece in slices for ticker in piece] == tickers
    assert {len(piece) for piece in slices} == {20, 21}
    assert len(refresh_slices(tickers[:150], interval_s=1)) == 10
    with pytest.raises(ValueError, match="interval_s must be positive"):
        refresh_slices(["A"], interval_s=0)


async def test_the_bus_carries_every_event_in_order_and_paced_refresh_images_of_every_book(
    tmp_path: Path,
) -> None:
    supervisors_stopped: list[bool] = []

    def on_close() -> None:
        supervisors = harness.recorder.supervisors.values()
        supervisors_stopped.append(all(not s.subscriptions for s in supervisors))

    publisher = RecordingPublisher(on_close=on_close)
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, publisher=publisher, bus_refresh_s=4) as harness,
    ):
        fake.set_book("KXA-1", yes=[("0.4000", "10.00")], no=[("0.6000", "5.00")])
        fake.set_book("KXSHOW-1", yes=[("0.2000", "1.00")])
        harness.start()
        ticker = await harness.connection(fake, TICKER_CONN_ID)
        control = await harness.connection(fake, CONTROL_CONN_ID)
        books = await harness.connection(fake, 2)
        await harness.subscribed(TICKER_CONN_ID, CONTROL_CONN_ID, 2, 3)
        await until(lambda: fresh(harness.recorder.books(), "KXA-1", "KXSHOW-1"))
        await ticker.push_message(
            "ticker", {"market_ticker": "KXA-1", "ts_ms": 5, "volume_fp": "7.00"}, sid=1
        )
        await control.push_sequenced(
            "market_lifecycle_v2", {"event_type": "activated", "market_ticker": "KXNEW-1"}, sid=1
        )
        await books.push_sequenced("orderbook_delta", delta_msg("KXA-1", "0.4100", "1.00"), sid=1)
        await until(lambda: len(publisher.messages) == 6)
        # Publishing is added to the ticker connection's consumer, not swapped in for it.
        assert "KXA-1" in harness.recorder.latest_tickers()

        # The first cycle started with no books and waits out its interval; the next opens with
        # the catalog and spreads two books over it, one every two seconds.
        await harness.parked()
        harness.time.advance(4)
        await until(lambda: len(publisher.messages) == 8)
        await harness.parked()
        harness.time.advance(2)
        await until(lambda: len(publisher.messages) == 9)

        # A gap stales KXA-1, silently: the snapshot requested is never answered.
        books.go_silent()
        books.skip_seq(1)
        await books.push_sequenced("orderbook_delta", delta_msg("KXA-1", "0.4100", "1.00"), sid=1)
        await until(lambda: not fresh(harness.recorder.books(), "KXA-1"))
        await harness.parked()
        harness.time.advance(2)
        await until(lambda: len(publisher.messages) == 12)
        status = harness.recorder.status().bus
        live = harness.recorder.books()["KXA-1"]
        await harness.recorder.stop()

    envelopes = publisher.envelopes
    assert [envelope.bus_seq for envelope in envelopes] == list(range(1, 13))
    assert {envelope.bus_epoch for envelope in envelopes} == {NOON}
    events = [envelope.event for envelope in envelopes]
    # The first cycle's catalog goes out as soon as capture starts, before any frame arrives.
    assert isinstance(events[0], MarketCatalog)
    assert sorted(type(event).__name__ for event in events[1:6]) == [
        "BookDelta",
        "BookSnapshot",
        "BookSnapshot",
        "Lifecycle",
        "Ticker",
    ]
    assert publisher.topics[6:] == [
        b"ctl.catalog",
        b"md.KXA-1",
        b"md.KXSHOW-1",
        b"ctl.gap",
        b"ctl.catalog",
        b"md.KXA-1",
    ]
    first, second, gap, stale = (events[7], events[8], events[9], events[11])
    assert isinstance(first, BookRefresh)
    assert isinstance(second, BookRefresh)
    assert isinstance(gap, GapEvent)
    assert isinstance(stale, BookRefresh)
    assert (first.receipt.conn_id, first.receipt.recv_wall_ns, first.stale) == (
        2,
        NOON + 4 * NS_PER_S,
        False,
    )
    assert (first.bids, first.asks) == (tuple(live.levels(Side.BID)), tuple(live.levels(Side.ASK)))
    assert first.bids[0] == Level(PriceE4(4100), CountE2(100))
    assert first.ts_ms == 1_789_000_000_000
    assert (second.receipt.conn_id, second.receipt.recv_wall_ns, second.stale) == (
        3,
        NOON + 6 * NS_PER_S,
        False,
    )
    assert gap.receipt.conn_id == 2
    assert (stale.receipt.recv_wall_ns, stale.stale, stale.bids) == (
        NOON + 8 * NS_PER_S,
        True,
        first.bids,
    )
    # Slices are paced by the injected sleep: an idle cycle of 4 s, then 2 s per book.
    assert [s for s in harness.time.requested if s in (2.0, 4.0)] == [4.0, 2.0, 2.0, 2.0]
    assert status == BusStatus(
        bus_epoch=NOON, bus_seq=12, sent=12, dropped=0, errors=0, refreshes=3
    )
    assert (publisher.closes, supervisors_stopped) == (1, [True])


async def test_each_refresh_cycle_opens_with_the_catalog_and_the_status_follows_its_interval(
    tmp_path: Path,
) -> None:
    publisher = RecordingPublisher()
    async with (
        FakeKalshiWs() as fake,
        recording(
            fake.url, tmp_path, publisher=publisher, bus_refresh_s=4, status_interval_s=6
        ) as harness,
    ):
        harness.start()
        await harness.subscribed(TICKER_CONN_ID, CONTROL_CONN_ID, 2, 3)
        await harness.parked()
        harness.time.advance(4)
        await until(lambda: publisher.topics.count(CATALOG_TOPIC) == 2)
        await harness.parked()
        harness.time.advance(2)
        await until(lambda: STATUS_TOPIC in publisher.topics)
        await harness.recorder.stop()

    close_ts = int(datetime(2026, 12, 31, tzinfo=UTC).timestamp())

    def recorded(ticker: str, volume: int, *, showcase: bool = False) -> CatalogEntry:
        return CatalogEntry(
            ticker=ticker,
            series_ticker=ticker.split("-", 1)[0],
            event_ticker=ticker.rsplit("-", 1)[0],
            volume_24h=CountE2(volume),
            close_ts=close_ts,
            showcase=showcase,
        )

    catalogs = [e.event for e in publisher.envelopes if isinstance(e.event, MarketCatalog)]
    # The selected markets, in ticker order; the low-volume and finalized ones are not recorded.
    assert catalogs == 2 * [
        MarketCatalog(
            markets=(
                recorded("KXA-1", 500_000),
                recorded("KXA-2", 400_000),
                recorded("KXA-3", 300_000),
                recorded("KXSHOW-1", 0, showcase=True),
            )
        )
    ]
    (report,) = [e.event for e in publisher.envelopes if isinstance(e.event, StatusReport)]
    assert isinstance(report, StatusReport)
    assert (report.interval_s, report.universe_size, report.subscribed_markets) == (6, 4, 4)
    assert [(c.conn_id, c.taped) for c in report.connections] == [
        (0, False),
        (1, True),
        (2, True),
        (3, True),
    ]
    assert all(isinstance(c, ConnectionReport) and c.frames > 0 for c in report.connections)


async def test_a_failing_bus_publisher_costs_its_messages_and_never_capture(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    publisher = RecordingPublisher()
    publisher.failure = RuntimeError("publisher bug")
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, publisher=publisher) as harness,
    ):
        fake.set_book("KXA-1", yes=[("0.4000", "10.00")])
        harness.start()
        books = await harness.connection(fake, 2)
        await harness.subscribed(2)
        await until(lambda: fresh(harness.recorder.books(), "KXA-1"))
        await books.push_sequenced("orderbook_delta", delta_msg("KXA-1", "0.4100", "1.00"), sid=1)
        await until(
            lambda: (
                harness.recorder.books()["KXA-1"].best_bid() == Level(PriceE4(4100), CountE2(100))
            )
        )

        def refreshed() -> bool:
            bus = harness.recorder.status().bus
            return bus is not None and bus.refreshes == 1

        await harness.parked()
        harness.time.advance(10)
        await until(refreshed)
        await harness.parked()
        harness.time.advance(50)
        await until(lambda: bool(logged(caplog, "recorder status")))
        await harness.parked()
        await harness.recorder.stop()
        assert harness.task is not None
        assert harness.task.exception() is None

    bus = logged(caplog, "recorder status")[-1].__dict__["bus"]
    assert bus["bus_seq"] >= 3  # two snapshots or more, the delta, and a refresh image
    assert (bus["sent"], bus["dropped"], bus["errors"]) == (0, 0, bus["bus_seq"])
    assert bus["refreshes"] == 1
    assert all(s.stats.callback_errors == 0 for s in harness.recorder.supervisors.values())
    assert len(logged(caplog, "bus message not published")) == 1
    assert not logged(caplog, "bus refresh failed")
    ((_, records, _),) = harness.tape(2)
    assert frame_types(records).count("orderbook_delta") == 1
    assert publisher.closes == 1


class BusConsumer:
    """A live consumer as ``tape serve`` runs one: every topic, decoded, through ``LiveBooks``.

    Setting :attr:`lose_next` discards the next message as if ZeroMQ had dropped it.
    """

    def __init__(self, messages: AsyncIterator[tuple[bytes, bytes]]) -> None:
        self.messages = messages
        self.live = LiveBooks()
        self.observations: list[Observation] = []
        self.last_seen = 0
        self.lose_next = False

    async def run(self) -> None:
        async for topic, payload in self.messages:
            if topic == PROBE:
                continue
            envelope = decode_bus_envelope(payload)
            self.last_seen = envelope.bus_seq
            if self.lose_next:
                self.lose_next = False
                continue
            self.observations.append(self.live.observe(envelope))


def mirrors(live: LiveBooks, books: Mapping[str, Book]) -> bool:
    """Whether a consumer holds exactly the recorder's books, each fresh and level for level."""
    held = live.books()
    return set(held) == set(books) and all(
        live.status(ticker) == BOOK_FRESH
        and all(held[ticker].levels(side) == book.levels(side) for side in Side)
        for ticker, book in books.items()
    )


async def test_a_bus_consumer_rebuilds_the_recorder_s_books_through_loss_and_refresh(
    tmp_path: Path, ipc_dir: Path
) -> None:
    endpoint = f"ipc://{ipc_dir / 'bus.sock'}"
    publisher = ZmqPublisher(endpoint, send_hwm=10_000)
    subscriber = ZmqSubscriber(endpoint, receive_hwm=10_000)
    subscriber.subscribe(b"")
    messages = subscriber.messages()
    # Connect before the recorder publishes anything, so that no loss here is accidental.
    probe = asyncio.ensure_future(anext(messages))
    async with asyncio.timeout(5):
        while not probe.done():
            publisher.publish(PROBE, b"")
            await asyncio.wait({probe}, timeout=0.01)
    consumer = BusConsumer(messages)
    consuming = asyncio.create_task(consumer.run())
    try:
        async with (
            FakeKalshiWs() as fake,
            recording(fake.url, tmp_path, publisher=publisher, bus_refresh_s=4) as harness,
        ):
            recorder = harness.recorder

            def bus_status() -> BusStatus:
                status = recorder.status().bus
                assert status is not None
                return status

            def caught_up() -> bool:
                return consumer.last_seen == bus_status().bus_seq

            async def advance_until(predicate: Callable[[], bool]) -> None:
                # Three books over four seconds: every book is refreshed within a dozen seconds
                # of virtual time, whatever the phase of the cycle.
                for _ in range(12):
                    await until(caught_up)
                    if predicate():
                        return
                    await harness.parked()
                    harness.time.advance(1)
                await until(caught_up)
                assert predicate()

            fake.set_book("KXA-1", yes=[("0.4000", "10.00")], no=[("0.6000", "5.00")])
            fake.set_book("KXA-3", yes=[("0.3000", "2.00")])
            fake.set_book("KXSHOW-1", yes=[("0.2000", "1.00")])
            harness.start()
            books = await harness.connection(fake, 2)
            await harness.subscribed(2, 3)
            await until(lambda: fresh(recorder.books(), "KXA-1", "KXA-3", "KXSHOW-1"))
            await until(caught_up)
            # Snapshots arrived, but a consumer knows no book before its refresh image.
            assert consumer.observations[0].reset == RESET_START
            assert {consumer.live.status(t) for t in recorder.books()} == {BOOK_UNKNOWN}

            await advance_until(lambda: mirrors(consumer.live, recorder.books()))
            refreshes = bus_status().refreshes
            await books.push_sequenced(
                "orderbook_delta", delta_msg("KXA-1", "0.4100", "3.00"), sid=1
            )
            await books.push_sequenced(
                "orderbook_delta", delta_msg("KXA-3", "0.3000", "-1.00"), sid=1
            )
            await until(caught_up)
            await until(lambda: recorder.books()["KXA-3"].size_at(Side.BID, PriceE4(3000)) == 100)
            await until(caught_up)
            # Deltas alone keep the copies exact between refresh images.
            assert bus_status().refreshes == refreshes
            assert recorder.books()["KXA-1"].best_bid() == Level(PriceE4(4100), CountE2(300))
            assert mirrors(consumer.live, recorder.books())

            consumer.lose_next = True
            await books.push_sequenced(
                "orderbook_delta", delta_msg("KXA-1", "0.4100", "1.00"), sid=1
            )
            await until(lambda: not consumer.lose_next)
            await books.push_sequenced(
                "orderbook_delta", delta_msg("KXA-3", "0.2900", "1.00"), sid=1
            )
            await until(lambda: recorder.books()["KXA-3"].size_at(Side.BID, PriceE4(2900)) == 100)
            await until(caught_up)
            lost = consumer.observations[-1]
            assert (lost.reset, lost.missed, lost.applied) == (RESET_GAP, 1, False)
            assert dict(consumer.live.books()) == {}

            await advance_until(lambda: mirrors(consumer.live, recorder.books()))
            assert consumer.live.stats.book_errors == 0
    finally:
        subscriber.close()
        await asyncio.wait_for(consuming, timeout=5)


# ------------------------------------------------------------- reacting to closes (ADR 0029)

NOON_S: Final = NOON // NS_PER_S
HOURLY: Final = "KXHOUR"
CLOSING_POLICY: Final = UniversePolicy(
    min_volume_24h=CountE2(100_000),
    max_l2_markets=4,
    groups=(
        UniverseGroup(name="hourly", series=(HOURLY,), events=1, markets_per_event=1),
        UniverseGroup(name="busiest", category=TEST_CATEGORY, events=1, markets_per_event=2),
    ),
)
"""The nearest hourly event's one market, then the two busiest markets of the ``KXA`` event."""


def closing(ticker: str, volume: str, *, after_s: int) -> dict[str, object]:
    """A listed market that closes ``after_s`` seconds after noon."""
    moment = datetime.fromtimestamp(NOON_S + after_s, tz=UTC)
    return market(ticker, volume, close_time=f"{moment:%Y-%m-%dT%H:%M:%SZ}")


HOUR_12: Final = closing("KXHOUR-26SEP1012-T1", "10.00", after_s=60)
HOUR_13: Final = closing("KXHOUR-26SEP1013-T1", "10.00", after_s=3_600)
HOUR_14: Final = closing("KXHOUR-26SEP1014-T1", "10.00", after_s=7_200)
CLOSING_MARKETS: Final = (
    HOUR_12,
    HOUR_13,
    closing("KXA-1", "5000.00", after_s=120),
    market("KXA-2", "4000.00"),
    market("KXA-3", "3000.00"),
)
"""At noon the universe is ``KXHOUR-26SEP1012-T1``, closing a minute later, ``KXA-1``, closing
two minutes later, and ``KXA-2``; ``KXA-3`` is one market too many for its event."""


def planned(recorder: Recorder) -> frozenset[str]:
    """Every market the book connections are meant to carry."""
    groups = [
        supervisor.group
        for conn_id, supervisor in recorder.supervisors.items()
        if conn_id >= FIRST_BOOK_CONN_ID
    ]
    return frozenset(ticker for group in groups if group is not None for ticker in group.tickers)


async def push_lifecycle(
    control: FakeConnection, event_type: str, ticker: str, **fields: object
) -> None:
    await control.push_sequenced(
        "market_lifecycle_v2", {"event_type": event_type, "market_ticker": ticker, **fields}, sid=1
    )


async def sleeps_again(harness: Harness, seconds: float, *, since: int) -> None:
    """Wait until the universe loop, woken by an event, parks again for ``seconds``."""
    time = harness.time
    await until(
        lambda: (
            len(time.requested) > since
            and time.requested[-1] == seconds
            and time.sleepers == harness.loops
        )
    )


async def test_a_close_leaves_the_plan_at_its_tick_and_only_its_series_group_is_listed_again(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, markets=CLOSING_MARKETS, universe=CLOSING_POLICY) as harness,
    ):
        recorder = harness.recorder
        harness.rest.series_answers[HOURLY] = listing(HOUR_13)
        harness.start()
        books = [await harness.connection(fake, conn_id) for conn_id in (2, 3)]
        await harness.subscribed(2, 3)
        await harness.parked()
        assert planned(recorder) == {"KXHOUR-26SEP1012-T1", "KXA-1", "KXA-2"}
        # The loop waits for the first close and a few seconds, not for the next full refresh.
        assert 60 + CLOSE_TICK_DELAY_S in harness.time.requested

        harness.time.advance(60 + CLOSE_TICK_DELAY_S - 1)
        await harness.parked()
        assert harness.rest.listings(HOURLY) == 0
        harness.time.advance(1)
        await until(lambda: planned(recorder) == {"KXHOUR-26SEP1013-T1", "KXA-1", "KXA-2"})
        await harness.parked()
        deleted = [
            command["params"]["market_tickers"]
            for connection in books
            for command in connection.commands
            if command["params"].get("action") == "delete_markets"
        ]
        # One command per channel of the book subscription the market was on.
        assert deleted == 2 * [["KXHOUR-26SEP1012-T1"]]
        assert (harness.rest.listings(), harness.rest.listings(HOURLY)) == (1, 1)
        (relisting,) = [r for r in harness.rest.requests if "series_ticker" in r.url.params]
        assert dict(relisting.url.params) == {
            "series_ticker": HOURLY,
            "status": "open",
            "limit": "1000",
            "mve_filter": "exclude",
        }

        # A category group's market leaves at its close too, and nothing takes its place.
        harness.time.advance(60)
        await until(lambda: "KXA-1" not in planned(recorder))
        await harness.parked()
        assert planned(recorder) == {"KXHOUR-26SEP1013-T1", "KXA-2"}
        assert (harness.rest.listings(), harness.rest.listings(HOURLY)) == (1, 1)
        universe = recorder.universe
        assert universe is not None
        assert universe.group_of == {"KXHOUR-26SEP1013-T1": "hourly", "KXA-2": "busiest"}

        # Until the full refresh, 300 seconds after the one at startup.
        harness.time.advance(300 - 123)
        await until(lambda: "KXA-3" in planned(recorder))
        assert planned(recorder) == {"KXHOUR-26SEP1013-T1", "KXA-2", "KXA-3"}
        assert harness.rest.listings() == 2

    first, second = logged(caplog, "closed markets removed from the universe")
    assert (first.__dict__["closed"], first.__dict__["relisting"]) == (
        ["KXHOUR-26SEP1012-T1"],
        ["hourly"],
    )
    assert (second.__dict__["closed"], second.__dict__["relisting"]) == (["KXA-1"], [])
    assert second.__dict__["groups"]["busiest"] == {
        "admitted": 1,
        "events": 1,
        "skipped_for_budget": 0,
    }
    (relisted,) = logged(caplog, "universe groups re-listed")
    fields = relisted.__dict__
    assert (fields["relisted"], fields["series"], fields["listed"]) == (["hourly"], [HOURLY], 1)
    assert (fields["added"], fields["removed"], fields["selected"]) == (
        ["KXHOUR-26SEP1013-T1"],
        [],
        3,
    )


async def test_lifecycle_events_tick_at_once_and_new_markets_are_listed_after_a_debounce(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, markets=CLOSING_MARKETS, universe=CLOSING_POLICY) as harness,
    ):
        recorder = harness.recorder
        # The first re-listing still shows the determined market open; it is not admitted again.
        harness.rest.series_answers[HOURLY] = [
            listing(HOUR_12, HOUR_13)[0],
            listing(HOUR_13, HOUR_14)[0],
        ]
        harness.start()
        control = await harness.connection(fake, CONTROL_CONN_ID)
        await harness.subscribed(CONTROL_CONN_ID, 2, 3)
        await harness.parked()

        # A determined category-group market leaves at once, with no listing and no clock change.
        await push_lifecycle(control, "determined", "KXA-2", result="yes")
        await until(lambda: "KXA-2" not in planned(recorder))
        assert harness.rest.listings(HOURLY) == 0
        # A settled series-group market leaves at once too, and its group is listed again.
        await push_lifecycle(control, "settled", "KXHOUR-26SEP1012-T1")
        await until(lambda: planned(recorder) == {"KXHOUR-26SEP1013-T1", "KXA-1"})
        await harness.parked()
        assert harness.rest.listings(HOURLY) == 1
        # Events about markets no series group names, or not planned, change nothing.
        since = len(harness.time.requested)
        await push_lifecycle(control, "determined", "KXA-3")
        await push_lifecycle(control, "created", "KXOTHER-26SEP10-T1")
        await push_lifecycle(control, "deactivated", "KXA-1")
        await until(lambda: recorder.supervisors[CONTROL_CONN_ID].stats.frames == 6)
        await harness.parked()
        assert len(harness.time.requested) == since

        # A new market in the group's series is listed once the debounce has passed.
        harness.time.advance(RELIST_MIN_INTERVAL_S)
        await harness.parked()
        since = len(harness.time.requested)
        await push_lifecycle(control, "created", "KXHOUR-26SEP1014-T1", close_ts=NOON_S + 7_200)
        await sleeps_again(harness, RELIST_DEBOUNCE_S, since=since)
        harness.time.advance(RELIST_DEBOUNCE_S - 1)
        await harness.parked()
        assert harness.rest.listings(HOURLY) == 1
        harness.time.advance(1)
        await until(lambda: harness.rest.listings(HOURLY) == 2)
        await harness.parked()

        # A burst right after waits out the minimum interval and costs one re-listing.
        since = len(harness.time.requested)
        await push_lifecycle(control, "activated", "KXHOUR-26SEP1014-T1")
        await sleeps_again(harness, RELIST_MIN_INTERVAL_S, since=since)
        harness.time.advance(RELIST_MIN_INTERVAL_S - 1)
        await harness.parked()
        since = len(harness.time.requested)
        await push_lifecycle(control, "created", "KXHOUR-26SEP1014-T2")
        await sleeps_again(harness, 1, since=since)
        assert harness.rest.listings(HOURLY) == 2
        harness.time.advance(1)
        await until(lambda: harness.rest.listings(HOURLY) == 3)
        await harness.parked()
        assert harness.rest.listings(HOURLY) == 3
        assert harness.rest.listings() == 1
        assert planned(recorder) == {"KXHOUR-26SEP1013-T1", "KXA-1"}

    removals = logged(caplog, "closed markets removed from the universe")
    assert [record.__dict__["closed"] for record in removals] == [
        ["KXA-2"],
        ["KXHOUR-26SEP1012-T1"],
    ]
    assert [r.__dict__["relisted"] for r in logged(caplog, "universe groups re-listed")] == 3 * [
        ["hourly"]
    ]


async def test_a_moved_close_time_moves_the_tick_and_every_catalog_follows_lifecycle_events(
    tmp_path: Path,
) -> None:
    publisher = RecordingPublisher()
    async with (
        FakeKalshiWs() as fake,
        recording(
            fake.url,
            tmp_path,
            markets=CLOSING_MARKETS,
            universe=CLOSING_POLICY,
            publisher=publisher,
            bus_refresh_s=4,
        ) as harness,
    ):
        recorder = harness.recorder
        harness.rest.series_answers[HOURLY] = listing(HOUR_13)
        harness.start()
        control = await harness.connection(fake, CONTROL_CONN_ID)
        await harness.subscribed(CONTROL_CONN_ID, 2, 3)
        await harness.parked()

        since = len(harness.time.requested)
        await push_lifecycle(control, "close_date_updated", "KXA-2", close_ts=NOON_S + 10)
        # Ten seconds after noon, and the few seconds every tick waits.
        await sleeps_again(harness, 10 + CLOSE_TICK_DELAY_S, since=since)
        await push_lifecycle(control, "determined", "KXA-1")
        await until(lambda: "KXA-1" not in planned(recorder))
        await harness.parked()

        harness.time.advance(4)
        await until(lambda: publisher.topics.count(CATALOG_TOPIC) == 2)
        catalog = [e.event for e in publisher.envelopes if isinstance(e.event, MarketCatalog)][-1]
        assert [(entry.ticker, entry.close_ts) for entry in catalog.markets] == [
            ("KXA-2", NOON_S + 10),
            ("KXHOUR-26SEP1012-T1", NOON_S + 60),
        ]
        await harness.parked()
        harness.time.advance(10 + CLOSE_TICK_DELAY_S - 4 - 1)
        await harness.parked()
        assert "KXA-2" in planned(recorder)
        harness.time.advance(1)
        await until(lambda: "KXA-2" not in planned(recorder))
        await harness.recorder.stop()

    lifecycles = [e.event for e in publisher.envelopes if isinstance(e.event, Lifecycle)]
    assert [(event.event_type, event.close_ts) for event in lifecycles] == [
        ("close_date_updated", NOON_S + 10),
        ("determined", None),
    ]


async def test_a_failed_relisting_is_logged_and_retried_after_the_interval_while_capture_goes_on(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, markets=CLOSING_MARKETS, universe=CLOSING_POLICY) as harness,
    ):
        recorder = harness.recorder
        harness.rest.series_answers[HOURLY] = [httpx.Response(503), *listing(HOUR_13)]
        fake.set_book("KXA-1", yes=[("0.4000", "10.00")])
        harness.start()
        control = await harness.connection(fake, CONTROL_CONN_ID)
        await harness.subscribed(CONTROL_CONN_ID, 2, 3)
        await until(lambda: fresh(recorder.books(), "KXA-1"))
        await harness.parked()

        await push_lifecycle(control, "determined", "KXHOUR-26SEP1012-T1")
        await until(lambda: bool(logged(caplog, "targeted re-listing failed")))
        await harness.parked()
        assert planned(recorder) == {"KXA-1", "KXA-2"}
        book_conn_id = next(
            c for c, sink in harness.sinks.items() if sink is recorder.sink_for("KXA-1")
        )
        books = await harness.connection(fake, book_conn_id)
        await books.push_sequenced("orderbook_delta", delta_msg("KXA-1", "0.4100", "1.00"), sid=1)
        await until(
            lambda: recorder.books()["KXA-1"].best_bid() == Level(PriceE4(4100), CountE2(100))
        )

        harness.time.advance(RELIST_MIN_INTERVAL_S - 1)
        await harness.parked()
        assert harness.rest.listings(HOURLY) == 1
        harness.time.advance(1)
        await until(lambda: "KXHOUR-26SEP1013-T1" in planned(recorder))
        assert harness.task is not None
        assert not harness.task.done()
        assert all(s.stats.callback_errors == 0 for s in recorder.supervisors.values())

    (failure,) = logged(caplog, "targeted re-listing failed")
    assert failure.levelno == logging.ERROR
    assert (failure.__dict__["relisting"], failure.__dict__["retry_in_s"]) == (
        ["hourly"],
        RELIST_MIN_INTERVAL_S,
    )
    assert harness.rest.listings(HOURLY) == 2
    ((_, records, _),) = harness.tape(book_conn_id)
    assert frame_types(records).count("orderbook_delta") == 1


@pytest.mark.parametrize("caught", ["waiting for a close", "listing a series"])
async def test_stop_does_not_wait_for_a_close_or_a_relisting_in_flight(
    tmp_path: Path, caught: str
) -> None:
    async with (
        FakeKalshiWs() as fake,
        recording(
            fake.url,
            tmp_path,
            markets=CLOSING_MARKETS,
            universe=CLOSING_POLICY,
            shutdown_timeout_s=30,
        ) as harness,
    ):
        harness.start()
        control = await harness.connection(fake, CONTROL_CONN_ID)
        await harness.subscribed(CONTROL_CONN_ID, 2, 3)
        await harness.parked()
        if caught == "listing a series":
            harness.rest.holds["/markets"] = asyncio.Event()
            harness.rest.series_answers[HOURLY] = listing(HOUR_13)
            await push_lifecycle(control, "settled", "KXHOUR-26SEP1012-T1")
            await until(lambda: harness.rest.listings(HOURLY) == 1)
        # Within the 30-second shutdown deadline only if the universe loop raced the stop.
        await asyncio.wait_for(harness.recorder.stop(), timeout=5.0)


# ------------------------------------------------ near-price events listed before their quotes

NEAR_PRICE_POLICY: Final = UniversePolicy(
    min_volume_24h=CountE2(100_000),
    max_l2_markets=4,
    groups=(
        UniverseGroup(
            name="hourly",
            series=(HOURLY,),
            events=1,
            markets_per_event=2,
            market_order="near_price",
        ),
    ),
)


def strike(
    ticker: str, *, bid: str = "0.0000", ask: str = "1.0000", last: str = "0.0000", after_s: int
) -> dict[str, object]:
    """A threshold strike of an hourly event; by default it has neither quotes nor a trade."""
    moment = datetime.fromtimestamp(NOON_S + after_s, tz=UTC)
    listed = market(
        ticker,
        "10.00",
        close_time=f"{moment:%Y-%m-%dT%H:%M:%SZ}",
        bid=bid,
        ask=ask,
        last=last,
    )
    return listed | {"strike_type": "greater"}


UNQUOTED_13: Final = tuple(
    strike(f"KXHOUR-26SEP1013-T{price}", after_s=3_600) for price in (67599, 68099, 77199, 77299)
)
"""A new hourly event: nothing quoted, so near-price order falls back to ticker order."""
QUOTED_13: Final = (
    strike("KXHOUR-26SEP1013-T67599", bid="0.9900", ask="1.0000", last="0.9900", after_s=3_600),
    strike("KXHOUR-26SEP1013-T68099", after_s=3_600),
    strike("KXHOUR-26SEP1013-T77199", bid="0.6000", ask="0.6200", after_s=3_600),
    strike("KXHOUR-26SEP1013-T77299", bid="0.3500", ask="0.3900", after_s=3_600),
)
"""The same event a minute later: the strikes around the price are quoted near 50 cents."""


def follow_up_logs(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str, int, str]]:
    """Each follow-up request and ending: event, message, attempt number or count, and outcome."""
    logs: list[tuple[str, str, int, str]] = []
    for record in caplog.records:
        message = record.getMessage()
        if message.startswith("near-price follow-up"):
            fields = record.__dict__
            count = fields["attempt"] if "attempt" in fields else fields["attempts"]
            logs.append((str(fields["event"]), message, int(count), str(fields.get("outcome", ""))))
    return logs


async def relistings(harness: Harness, count: int) -> None:
    """Wait until the hourly series has been listed alone ``count`` times."""
    await until(lambda: harness.rest.listings(HOURLY) == count)


async def test_an_unquoted_near_price_event_is_listed_again_until_its_strikes_near_the_price(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, markets=UNQUOTED_13, universe=NEAR_PRICE_POLICY) as harness,
    ):
        recorder = harness.recorder
        harness.rest.series_answers[HOURLY] = [listing(*UNQUOTED_13)[0], listing(*QUOTED_13)[0]]
        harness.start()
        await harness.subscribed(2)
        await harness.parked()
        # Ticker order among unpriced strikes: the lowest, far from the price.
        assert planned(recorder) == {"KXHOUR-26SEP1013-T67599", "KXHOUR-26SEP1013-T68099"}
        assert RELIST_MIN_INTERVAL_S in harness.time.requested

        harness.time.advance(RELIST_MIN_INTERVAL_S - 1)
        await harness.parked()
        assert harness.rest.listings(HOURLY) == 0
        harness.time.advance(1)
        await until(lambda: harness.rest.listings(HOURLY) == 1)
        await harness.parked()
        # Still unquoted: another attempt, again at the minimum interval.
        assert harness.time.requested[-1] == RELIST_MIN_INTERVAL_S
        harness.time.advance(RELIST_MIN_INTERVAL_S)
        near = {"KXHOUR-26SEP1013-T77199", "KXHOUR-26SEP1013-T77299"}
        await until(lambda: planned(recorder) == near)
        await harness.parked()
        assert planned(recorder) == near

        # Priced: no more follow-ups, only the full refresh.
        harness.time.advance(300 - 2 * RELIST_MIN_INTERVAL_S - 1)
        await harness.parked()
        assert (harness.rest.listings(), harness.rest.listings(HOURLY)) == (1, 2)

    assert follow_up_logs(caplog) == [
        ("KXHOUR-26SEP1013", "near-price follow-up re-listing requested", 1, ""),
        ("KXHOUR-26SEP1013", "near-price follow-up re-listing requested", 2, ""),
        ("KXHOUR-26SEP1013", "near-price follow-ups ended", 2, "priced"),
    ]
    (first, *_) = logged(caplog, "near-price follow-up re-listing requested")
    assert (first.__dict__["group"], first.__dict__["max_attempts"]) == ("hourly", 4)
    assert first.__dict__["unpriced"] == ["KXHOUR-26SEP1013-T67599", "KXHOUR-26SEP1013-T68099"]
    assert first.__dict__["due_in_s"] == RELIST_MIN_INTERVAL_S


async def test_near_price_follow_ups_stop_at_their_bound_and_are_counted(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, markets=UNQUOTED_13, universe=NEAR_PRICE_POLICY) as harness,
    ):
        harness.rest.series_answers[HOURLY] = listing(*UNQUOTED_13)
        harness.start()
        await harness.subscribed(2)
        for attempt in range(1, NEAR_PRICE_FOLLOW_UPS + 1):
            await harness.parked()
            harness.time.advance(RELIST_MIN_INTERVAL_S)
            await relistings(harness, attempt)
        await until(lambda: bool(logged(caplog, "near-price follow-ups ended")))
        # Nothing more until the full refresh, which does not start them again.
        await harness.parked()
        harness.time.advance(300 - NEAR_PRICE_FOLLOW_UPS * RELIST_MIN_INTERVAL_S)
        await until(lambda: harness.rest.listings() == 2)
        await harness.parked()
        harness.time.advance(2 * RELIST_MIN_INTERVAL_S)
        await harness.parked()
        assert harness.rest.listings(HOURLY) == NEAR_PRICE_FOLLOW_UPS

    requested = [
        attempt for _, message, attempt, _ in follow_up_logs(caplog) if "requested" in message
    ]
    assert requested == [1, 2, 3, 4]
    (ended,) = logged(caplog, "near-price follow-ups ended")
    assert ended.levelno == logging.INFO
    assert (ended.__dict__["outcome"], ended.__dict__["attempts"]) == ("gave_up", 4)
    assert ended.__dict__["given_up_total"] == 1
    assert ended.__dict__["unpriced"] == ["KXHOUR-26SEP1013-T67599", "KXHOUR-26SEP1013-T68099"]


async def test_a_volume_group_never_follows_up_an_unquoted_event(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    by_volume = msgspec.structs.replace(
        NEAR_PRICE_POLICY,
        groups=(msgspec.structs.replace(NEAR_PRICE_POLICY.groups[0], market_order="volume"),),
    )
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, markets=UNQUOTED_13, universe=by_volume) as harness,
    ):
        harness.start()
        await harness.subscribed(2)
        await harness.parked()
        harness.time.advance(2 * RELIST_MIN_INTERVAL_S)
        await harness.parked()
        assert harness.rest.listings(HOURLY) == 0
    assert follow_up_logs(caplog) == []


async def test_an_unquoted_event_that_leaves_the_plan_ends_its_follow_ups(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    closing_soon = tuple(
        strike(f"KXHOUR-26SEP1012-T{price}", after_s=10) for price in (77199, 77299)
    )
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, markets=closing_soon, universe=NEAR_PRICE_POLICY) as harness,
    ):
        recorder = harness.recorder
        # The close's re-listing finds the next event already quoted.
        harness.rest.series_answers[HOURLY] = listing(*QUOTED_13)
        harness.start()
        await harness.subscribed(2)
        await harness.parked()
        harness.time.advance(10 + CLOSE_TICK_DELAY_S)
        await until(lambda: "KXHOUR-26SEP1013-T77199" in planned(recorder))
        await harness.parked()
        harness.time.advance(2 * RELIST_MIN_INTERVAL_S)
        await harness.parked()
        # The follow-up asked for at startup was covered by the close's re-listing.
        assert harness.rest.listings(HOURLY) == 1

    assert follow_up_logs(caplog) == [
        ("KXHOUR-26SEP1012", "near-price follow-up re-listing requested", 1, ""),
        ("KXHOUR-26SEP1012", "near-price follow-ups ended", 1, "left_plan"),
    ]


async def test_a_full_refresh_that_prices_the_event_ends_its_pending_follow_up(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger="tape.recorder.recorder")
    async with (
        FakeKalshiWs() as fake,
        recording(
            fake.url,
            tmp_path,
            markets=UNQUOTED_13,
            universe=NEAR_PRICE_POLICY,
            universe_refresh_s=RELIST_MIN_INTERVAL_S // 2,
        ) as harness,
    ):
        recorder = harness.recorder
        harness.rest.answers["/markets"] = [listing(*UNQUOTED_13)[0], listing(*QUOTED_13)[0]]
        harness.start()
        await harness.subscribed(2)
        await harness.parked()
        harness.time.advance(RELIST_MIN_INTERVAL_S // 2)
        await until(lambda: "KXHOUR-26SEP1013-T77199" in planned(recorder))
        await harness.parked()
        harness.time.advance(RELIST_MIN_INTERVAL_S)
        await harness.parked()
        assert harness.rest.listings(HOURLY) == 0
        assert harness.rest.listings() >= 2

    assert follow_up_logs(caplog) == [
        ("KXHOUR-26SEP1013", "near-price follow-up re-listing requested", 1, ""),
        ("KXHOUR-26SEP1013", "near-price follow-ups ended", 1, "priced"),
    ]
