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
from tape.client.ratelimit import Bucket, BucketLimits
from tape.client.rest import KalshiRest
from tape.client.ws import WsSession
from tape.errors import KalshiHttpError
from tape.events import Level, Side
from tape.fixedpoint import CountE2, PriceE4
from tape.recorder.planner import Group
from tape.recorder.recorder import (
    CLOCK_JUMP_THRESHOLD_NS,
    CONTROL_CONN_ID,
    TICKER_CONN_ID,
    TICKER_RETENTION_NS,
    PeriodicTask,
    Recorder,
    RecorderConfig,
    check_connection_budget,
    is_clock_jump,
    keyframe_path,
    universe_retry_delay_s,
)
from tape.recorder.universe import UniversePolicy
from tape.recorder.writer import HeaderFactory, SegmentSink
from tape.segment import Record, RecordKind, SegmentHeader, SegmentReader, read_keyframe
from tape.timeutil import NS_PER_MS, NS_PER_S, FrozenClock, Ns
from tests.fakes import FakeConnection, FakeKalshiWs

NOON: Final = int(datetime(2026, 9, 10, 12, tzinfo=UTC).timestamp()) * NS_PER_S
REST_URL: Final = "https://rest.test/trade-api/v2"
POLICY: Final = UniversePolicy(
    min_volume_24h=CountE2(100_000), max_l2_markets=4, showcase_series=frozenset({"KXSHOW"})
)
LIMITS: Final = {
    "usage_tier": "advanced",
    "read": {"refill_rate": 300, "bucket_capacity": 600},
    "write": {"refill_rate": 150, "bucket_capacity": 300},
    "grants": [],
}
LOOPS: Final = 3
"""Universe, keyframe, and status loops, each parked in the injected sleep between rounds."""


def market(ticker: str, volume: str, *, status: str = "active") -> dict[str, object]:
    return {
        "ticker": ticker,
        "event_ticker": ticker.rsplit("-", 1)[0],
        "market_type": "binary",
        "yes_sub_title": "Yes",
        "no_sub_title": "No",
        "created_time": "2026-01-01T00:00:00Z",
        "updated_time": "2026-01-01T00:00:00Z",
        "open_time": "2026-01-01T00:00:00Z",
        "close_time": "2026-12-31T00:00:00Z",
        "latest_expiration_time": "2026-12-31T00:00:00Z",
        "settlement_timer_seconds": 60,
        "status": status,
        "notional_value_dollars": "1.0000",
        "yes_bid_dollars": "0.4000",
        "yes_ask_dollars": "0.6000",
        "no_bid_dollars": "0.4000",
        "no_ask_dollars": "0.6000",
        "yes_bid_size_fp": "1.00",
        "yes_ask_size_fp": "1.00",
        "last_price_dollars": "0.5000",
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
    """Canned REST answers by path; a queue serves in order, then repeats its last answer."""

    def __init__(self, markets: Sequence[Mapping[str, object]]) -> None:
        self.requests: list[httpx.Request] = []
        self.answers: dict[str, list[httpx.Response]] = {
            "/exchange/status": [
                httpx.Response(200, json={"exchange_active": True, "trading_active": True})
            ],
            "/account/limits": [httpx.Response(200, json=LIMITS)],
            "/markets": [httpx.Response(200, json={"markets": list(markets), "cursor": ""})],
        }

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        queue = self.answers[request.url.path.removeprefix("/trade-api/v2")]
        return queue.pop(0) if len(queue) > 1 else queue[0]


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
        broken_conn: int | None = None,
        broken_headers: bool = False,
        **config: Any,  # RecorderConfig fields under test
    ) -> None:
        self.root = root
        self.clock = FrozenClock(mono_ns=1, wall_ns=NOON)
        self.host_clock = HostClock(self.clock)
        self.time = VirtualTime(self.clock)
        self.silence_timeouts: dict[int, list[int | None]] = {}
        self.limiter = RecordingLimiter()
        self.rest = FakeRest(markets)
        self.http = httpx.AsyncClient(transport=httpx.MockTransport(self.rest), base_url=REST_URL)
        self.sinks: dict[int, SegmentSink] = {}
        self.broken_conn = broken_conn
        self.broken_headers = broken_headers
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
        )
        self.task: asyncio.Task[None] | None = None

    def session(self, url: str, *, conn_id: int, silence_timeout_ns: int | None) -> WsSession:
        if conn_id == self.broken_conn:
            raise RuntimeError(f"no session for connection {conn_id}")
        self.silence_timeouts.setdefault(conn_id, []).append(silence_timeout_ns)
        # A clock of the session's own that never moves, so advancing virtual time cannot
        # trip the ticker connection's silence timeout; the query string tells the fake
        # which connection it is.
        return WsSession(
            f"{url}?conn={conn_id}",
            StubSigner(),
            FrozenClock(mono_ns=1, wall_ns=NOON),
            conn_id=conn_id,
            silence_timeout_ns=silence_timeout_ns,
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
        await until(lambda: self.time.sleepers == LOOPS)

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

        assert await ticker.wait_for_commands(1) == [
            {"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"]}}
        ]
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
        ]
        listing = harness.rest.requests[2].url.params
        assert (listing["status"], listing["limit"], listing["mve_filter"]) == (
            "open",
            "1000",
            "exclude",
        )
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


async def test_only_the_live_only_ticker_connection_has_a_data_silence_timeout(
    tmp_path: Path,
) -> None:
    async with (
        FakeKalshiWs() as fake,
        recording(fake.url, tmp_path, ticker_silence_timeout_s=45) as harness,
    ):
        harness.start()
        await harness.subscribed(TICKER_CONN_ID, CONTROL_CONN_ID, 2, 3)
        # An idle book connection, and a reconnected one, are built without one too.
        (books,) = [c for c in fake.connections if c.path.endswith("?conn=2")]
        await books.close_abruptly()
        await until(lambda: harness.time.sleepers == LOOPS + 1)  # the reconnect backoff
        harness.time.advance(1)
        await until(lambda: len(harness.silence_timeouts[2]) == 2)

    assert harness.silence_timeouts == {
        TICKER_CONN_ID: [45 * NS_PER_S],
        CONTROL_CONN_ID: [None],
        2: [None, None],
        3: [None],
    }


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


async def test_a_ticker_value_is_forgotten_a_day_after_its_last_update(tmp_path: Path) -> None:
    async with FakeKalshiWs() as fake, recording(fake.url, tmp_path) as harness:
        harness.start()
        ticker = await harness.connection(fake, TICKER_CONN_ID)
        await harness.subscribed(TICKER_CONN_ID)
        await ticker.push_message("ticker", {"market_ticker": "KXA-9", "ts_ms": 5}, sid=1)
        await until(lambda: "KXA-9" in harness.recorder.latest_tickers())
        await harness.parked()
        harness.time.advance(TICKER_RETENTION_NS // NS_PER_S + 300)
        await until(lambda: not harness.recorder.latest_tickers())


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


async def test_showcase_markets_beyond_book_capacity_are_left_out_and_logged(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    showcase = [market(f"KXSHOW-{index}", "0.00") for index in (1, 2, 3)]
    async with (
        FakeKalshiWs() as fake,
        recording(
            fake.url,
            tmp_path,
            markets=showcase,
            universe=msgspec.structs.replace(POLICY, max_l2_markets=2),
            group_size=1,
        ) as harness,
    ):
        harness.start()
        await until(lambda: harness.recorder.universe is not None)
        supervisors = harness.recorder.supervisors
        assert [supervisors[conn_id].group for conn_id in (2, 3)] == [
            Group(group_id="g0000", conn_id=2, tickers=frozenset({"KXSHOW-1"})),
            Group(group_id="g0001", conn_id=3, tickers=frozenset({"KXSHOW-2"})),
        ]
        assert harness.recorder.sink_for("KXSHOW-3") is None

    (left_out,) = logged(caplog, "markets left without a book connection")
    assert left_out.levelno == logging.ERROR
    assert (left_out.__dict__["unplaced"], left_out.__dict__["first_unplaced"]) == (1, "KXSHOW-3")
    assert left_out.__dict__["book_capacity"] == 2


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
        ({"ticker_silence_timeout_s": 0}, "ticker_silence_timeout_s must be positive"),
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
