"""Run ``tape record``: every connection, the universe, keyframes, and an orderly shutdown.

Responsibility: orchestrate the recorder process (docs/ARCHITECTURE.md 4, 5, and 7.1) out
of parts tested on their own. At start it reads the exchange status and sizes the rate
limiter from the account's tier; then it runs one ``ConnectionSupervisor`` per WebSocket
connection, lists and selects the order-book universe and hands each book connection its
subscription groups every ``universe_refresh_s``, writes keyframes, logs a status line,
and runs auxiliary periodic tasks such as the auditor. Dependencies arrive fully built, so
the orchestration is tested against a fake exchange in virtual time.

Connection layout (ADR 0018): connection 0 is live-only and carries the unfiltered
``ticker`` channel, whose latest value per market is kept in memory and never written;
connection 1 is taped and carries ``market_lifecycle_v2``; connections 2 onwards are taped
and carry the planner's ``orderbook_delta`` and ``trade`` groups with ``use_yes_price``.

Invariants: a supervisor, sink, or internal loop that fails ends the run with its
exception after a full shutdown, never silently; a periodic task that fails is logged and
capture continues; shutdown stops auxiliary work first, writes a final keyframe while the
books are still live, stops every supervisor, then closes every sink so that every record
accepted is on disk; shutdown runs once however often it is requested; every wait is raced
against the stop request; and the module sleeps, draws randomness, and reads time only
through what was injected.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Final, Protocol

import msgspec
import pyarrow as pa

from tape import __version__
from tape.book import Book, KeyframeRow
from tape.client.ratelimit import BucketLimits, RateLimiter
from tape.client.rest import KalshiRest
from tape.client.ws import WsSession
from tape.errors import FixedPointError, KalshiError, WireError
from tape.events import MarketEvent, Ticker
from tape.recorder.planner import Group, Plan, plan
from tape.recorder.supervisor import ORDERBOOK_CHANNEL, ConnectionSupervisor, SupervisorConfig
from tape.recorder.universe import MarketSummary, UniverseDecision, UniversePolicy, select
from tape.recorder.writer import HeaderFactory, SegmentSink
from tape.segment import SegmentHeader, write_keyframe
from tape.timeutil import NS_PER_S, Clock, Ns, wall_ns_to_datetime

__all__ = [
    "BOOK_CHANNELS",
    "CONTROL_CONN_ID",
    "DEFAULT_KEYFRAME_WRITE_TIMEOUT_S",
    "DEFAULT_MAX_MARKET_PAGES",
    "DEFAULT_SHUTDOWN_TIMEOUT_S",
    "FIRST_BOOK_CONN_ID",
    "LIFECYCLE_CHANNEL",
    "MARKET_PAGE_LIMIT",
    "MAX_GROUP_SIZE",
    "PINNED_SPEC_VERSIONS",
    "TICKER_CHANNEL",
    "TICKER_CONN_ID",
    "TICKER_RETENTION_NS",
    "ConnectionStatus",
    "PeriodicTask",
    "Recorder",
    "RecorderConfig",
    "RecorderStatus",
    "SessionBuilder",
    "SinkBuilder",
    "check_connection_budget",
    "check_keyframe_interval",
    "keyframe_path",
]

TICKER_CONN_ID: Final = 0
"""The live-only connection carrying the unfiltered ``ticker`` channel (ADR 0018)."""

CONTROL_CONN_ID: Final = 1
"""The taped connection carrying ``market_lifecycle_v2``, which accepts no market filter."""

FIRST_BOOK_CONN_ID: Final = 2
"""Book connections take ids from here; planner connection ``i`` is connection ``2 + i``."""

TICKER_CHANNEL: Final = "ticker"
LIFECYCLE_CHANNEL: Final = "market_lifecycle_v2"
BOOK_CHANNELS: Final = (ORDERBOOK_CHANNEL, "trade")

MAX_GROUP_SIZE: Final = 500
"""Most markets in one subscription group (ADR 0010)."""

PINNED_SPEC_VERSIONS: Final[Mapping[str, str]] = MappingProxyType(
    {"openapi": "3.30.0", "asyncapi": "2.0.0"}
)
"""Versions of the Kalshi specifications in ``specs/`` that ``tape.wire`` mirrors."""

MARKET_PAGE_LIMIT: Final = 1000
"""Markets per ``GET /markets`` page, the largest Kalshi serves."""

DEFAULT_MAX_MARKET_PAGES: Final = 500
"""Page cap on one universe listing (docs/ENGINEERING_STANDARDS.md 3.6)."""

DEFAULT_SHUTDOWN_TIMEOUT_S: Final = 30
"""Deadline for each shutdown stage before stragglers are cancelled."""

DEFAULT_KEYFRAME_WRITE_TIMEOUT_S: Final = 60
"""Deadline for writing one keyframe file."""

TICKER_RETENTION_NS: Final = 24 * 3_600 * NS_PER_S
"""A market's latest ``ticker`` value is forgotten after a day without an update."""

_SECONDS_PER_MINUTE: Final = 60
_SECONDS_PER_HOUR: Final = 3_600
_OPEN_MARKET_STATUS: Final = "open"
_MVE_FILTER_EXCLUDE: Final = "exclude"


def check_connection_budget(*, book_connections: int, max_connections: int) -> None:
    """Check that the connection layout fits under the connection ceiling.

    Args:
        book_connections: Connections carrying order-book groups.
        max_connections: Ceiling on connections the recorder may open.

    Raises:
        ValueError: If there is no book connection, or the ticker connection, the control
            connection, and the book connections together exceed ``max_connections``.
    """
    if book_connections < 1:
        raise ValueError(f"book_connections must be at least 1, got {book_connections}")
    needed = FIRST_BOOK_CONN_ID + book_connections
    if needed > max_connections:
        raise ValueError(
            f"book_connections = {book_connections} needs {needed} connections (one live-only "
            f"ticker connection, one control connection, and the book connections), but "
            f"max_connections = {max_connections}"
        )


def check_keyframe_interval(interval_s: int) -> None:
    """Check that keyframes land on the same minutes of every hour.

    Args:
        interval_s: Seconds between keyframes.

    Raises:
        ValueError: Unless the interval is a positive whole number of minutes that divides
            an hour, so that ``MM.parquet`` names are unique and an hour always starts with
            a keyframe (docs/DATA_FORMATS.md 5).
    """
    if interval_s <= 0 or interval_s % _SECONDS_PER_MINUTE or _SECONDS_PER_HOUR % interval_s:
        raise ValueError(
            f"keyframe_interval_s must be a whole number of minutes that divides an hour, "
            f"got {interval_s}"
        )


def keyframe_path(root: Path, wall_ns: int, *, interval_s: int) -> Path:
    """Return where a keyframe taken at ``wall_ns`` belongs (docs/DATA_FORMATS.md 5).

    Args:
        root: Data directory; keyframes live under ``root / "keyframes"``.
        wall_ns: Wall-clock nanoseconds when the books were read.
        interval_s: Slot length. The UTC minute in the name is ``wall_ns`` floored to it.

    Returns:
        ``root/keyframes/YYYY-MM-DD/HH/MM.parquet``.

    Raises:
        ValueError: If ``wall_ns`` is negative or ``interval_s`` fails
            :func:`check_keyframe_interval`.
    """
    check_keyframe_interval(interval_s)
    if wall_ns < 0:
        raise ValueError(f"wall_ns must be non-negative, got {wall_ns}")
    slot_ns = interval_s * NS_PER_S
    moment = wall_ns_to_datetime(wall_ns - wall_ns % slot_ns)
    return root / "keyframes" / f"{moment:%Y-%m-%d}" / f"{moment:%H}" / f"{moment:%M}.parquet"


class RecorderConfig(msgspec.Struct, frozen=True, kw_only=True):
    """What ``tape record`` runs: validated, free of secrets, and independent of the shell.

    Attributes:
        env: Kalshi environment name, written into every segment header.
        ws_url: WebSocket endpoint every connection uses.
        data_dir: Root of ``raw/`` and ``keyframes/``.
        host: Host name written into every segment header.
        universe: Which markets earn order-book capture.
        max_connections: Ceiling on connections.
        book_connections: Connections carrying order-book groups.
        group_size: Most markets per subscription group.
        keyframe_interval_s: Seconds between keyframes.
        universe_refresh_s: Seconds between market listings.
        status_interval_s: Seconds between status log lines.
        max_market_pages: Page cap on one market listing.
        shutdown_timeout_s: Deadline for each shutdown stage.
        keyframe_write_timeout_s: Deadline for writing one keyframe file.

    Raises:
        ValueError: On a layout over ``max_connections``, a group size outside
            ``[1, MAX_GROUP_SIZE]``, a keyframe interval that does not tile an hour, or a
            non-positive interval, cap, or timeout.
    """

    env: str
    ws_url: str
    data_dir: Path
    host: str
    universe: UniversePolicy
    max_connections: int = 16
    book_connections: int = 2
    group_size: int = MAX_GROUP_SIZE
    keyframe_interval_s: int = 300
    universe_refresh_s: int = 300
    status_interval_s: int = 60
    max_market_pages: int = DEFAULT_MAX_MARKET_PAGES
    shutdown_timeout_s: int = DEFAULT_SHUTDOWN_TIMEOUT_S
    keyframe_write_timeout_s: int = DEFAULT_KEYFRAME_WRITE_TIMEOUT_S

    def __post_init__(self) -> None:
        check_connection_budget(
            book_connections=self.book_connections, max_connections=self.max_connections
        )
        check_keyframe_interval(self.keyframe_interval_s)
        if not 1 <= self.group_size <= MAX_GROUP_SIZE:
            raise ValueError(f"group_size must be in [1, {MAX_GROUP_SIZE}], got {self.group_size}")
        for name in (
            "universe_refresh_s",
            "status_interval_s",
            "max_market_pages",
            "shutdown_timeout_s",
            "keyframe_write_timeout_s",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value}")


class ConnectionStatus(msgspec.Struct, frozen=True, kw_only=True):
    """One connection's line in the status log.

    Attributes:
        conn_id: Connection id.
        taped: Whether the connection writes to a segment sink.
        subscriptions: Subscription ids live on the current connection.
        frames: Frames received since start.
        gaps: Sequence gaps observed since start.
        reconnects: Reconnections since start.
        stale_books: Books awaiting a snapshot now.
        sink_dropped: Records that never reached a segment; zero on a live-only connection.
    """

    conn_id: int
    taped: bool
    subscriptions: int
    frames: int
    gaps: int
    reconnects: int
    stale_books: int
    sink_dropped: int


class RecorderStatus(msgspec.Struct, frozen=True, kw_only=True):
    """The whole recorder at a glance, logged every ``status_interval_s``.

    Attributes:
        connections: Every connection, by ascending id.
        universe_size: Markets the last universe selection chose.
        subscribed_markets: Markets in groups whose subscriptions are live now.
        live_tickers: Markets with a latest ``ticker`` value in memory.
    """

    connections: tuple[ConnectionStatus, ...]
    universe_size: int
    subscribed_markets: int
    live_tickers: int


class SessionBuilder(Protocol):
    """Builds a new, unconnected WebSocket session; sessions are single-use."""

    def __call__(self, url: str, *, conn_id: int) -> WsSession:
        """Return a session for ``url`` whose errors name connection ``conn_id``."""
        ...


class SinkBuilder(Protocol):
    """Builds the (not yet started) segment sink of one taped connection."""

    def __call__(self, *, conn_id: int, header_factory: HeaderFactory) -> SegmentSink:
        """Return the sink for ``conn_id``, whose new files take headers from the factory."""
        ...


class PeriodicTask(Protocol):
    """Auxiliary work that runs beside capture, for example the REST auditor."""

    async def run(self, *, stop: asyncio.Event) -> None:
        """Work until ``stop`` is set, then return promptly."""
        ...


class Recorder:
    """Orchestrates ``tape record``. See the module docstring for layout and invariants.

    Not thread-safe; every method runs on one event loop, except the header factories it
    gives the sinks, which read only immutable snapshots. Single-use.

    Args:
        config: What to record and how.
        clock: Time for the universe, keyframes, and segment headers.
        rest: REST client; it must wait on ``limiter``.
        limiter: The limiter inside ``rest``, resized from ``GET /account/limits``.
        session_builder: Builds a session for one connection attempt.
        sink_builder: Builds the sink of each taped connection.
        sleep: Waits the given seconds; drives every loop and every reconnect backoff.
        jitter: Returns a draw from ``[0, 1)`` for each reconnect backoff.
        periodic_tasks: Auxiliary work; a failure in one is logged and capture continues.
        logger: Destination for logs; defaults to this module's logger.
    """

    def __init__(
        self,
        config: RecorderConfig,
        *,
        clock: Clock,
        rest: KalshiRest,
        limiter: RateLimiter,
        session_builder: SessionBuilder,
        sink_builder: SinkBuilder,
        sleep: Callable[[float], Awaitable[None]],
        jitter: Callable[[], float],
        periodic_tasks: Sequence[PeriodicTask] = (),
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._clock = clock
        self._rest = rest
        self._limiter = limiter
        self._sleep = sleep
        self._periodic = tuple(periodic_tasks)
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._tickers: dict[str, Ticker] = {}
        self._plan = Plan(groups=())
        self._universe: UniverseDecision | None = None
        self._conn_of: dict[str, int] = {}
        self._stop_requested = asyncio.Event()
        self._periodic_stop = asyncio.Event()
        self._finished = asyncio.Event()
        self._run_started = False
        self._shutdown_future: asyncio.Future[None] | None = None
        self._failures: list[BaseException] = []
        self._supervisor_tasks: dict[int, asyncio.Task[None]] = {}
        self._loop_tasks: list[asyncio.Task[None]] = []
        self._periodic_tasks: list[asyncio.Task[None]] = []
        self._supervisors: dict[int, ConnectionSupervisor] = {}
        self._sinks: dict[int, SegmentSink] = {}
        for supervisor_config in self._layout():
            conn_id = supervisor_config.conn_id
            sink = (
                sink_builder(conn_id=conn_id, header_factory=self._header_factory(conn_id))
                if supervisor_config.persist
                else None
            )
            if sink is not None:
                self._sinks[conn_id] = sink
            self._supervisors[conn_id] = ConnectionSupervisor(
                supervisor_config,
                session_factory=functools.partial(session_builder, config.ws_url, conn_id=conn_id),
                clock=clock,
                sleep=sleep,
                jitter=jitter,
                sink=sink,
                on_event=self._remember_ticker if conn_id == TICKER_CONN_ID else None,
            )

    # ------------------------------------------------------------------- read-only views

    @property
    def config(self) -> RecorderConfig:
        """The configuration this recorder was built with."""
        return self._config

    @property
    def supervisors(self) -> Mapping[int, ConnectionSupervisor]:
        """Every connection's supervisor, by connection id."""
        return MappingProxyType(self._supervisors)

    @property
    def universe(self) -> UniverseDecision | None:
        """The last universe selection, or ``None`` before the first listing succeeds."""
        return self._universe

    @property
    def periodic_tasks(self) -> tuple[PeriodicTask, ...]:
        """The auxiliary tasks this recorder runs beside capture, in the order given."""
        return self._periodic

    def books(self) -> Mapping[str, Book]:
        """Every order book across the book connections, by ticker.

        A market briefly subscribed on two connections while it moves between them is
        reported by whichever copy is not stale.

        Returns:
            A read-only mapping of live, mutable books; check ``is_stale()`` before use.
        """
        merged: dict[str, Book] = {}
        for conn_id in self._book_conn_ids():
            for ticker, book in self._supervisors[conn_id].books.items():
                held = merged.get(ticker)
                if held is None or (held.is_stale() and not book.is_stale()):
                    merged[ticker] = book
        return MappingProxyType(merged)

    def sink_for(self, ticker: str) -> SegmentSink | None:
        """Return the sink of the book connection that the current plan assigns a market to.

        Args:
            ticker: Market ticker.

        Returns:
            That connection's sink, or ``None`` if the market is not in the plan.
        """
        conn_id = self._conn_of.get(ticker)
        return None if conn_id is None else self._sinks.get(conn_id)

    def latest_tickers(self) -> Mapping[str, Ticker]:
        """The latest ``ticker`` value per market from the live-only connection; never taped.

        Returns:
            A read-only mapping by market ticker.
        """
        return MappingProxyType(self._tickers)

    def status(self) -> RecorderStatus:
        """Summarize every connection and the universe.

        Returns:
            The counters logged by the status loop.
        """
        connections: list[ConnectionStatus] = []
        subscribed = 0
        for conn_id, supervisor in sorted(self._supervisors.items()):
            stats = supervisor.stats
            sink = self._sinks.get(conn_id)
            live_groups = {info.group_id for info in supervisor.subscriptions}
            subscribed += sum(
                len(g.tickers) for g in supervisor.groups if g.group_id in live_groups
            )
            connections.append(
                ConnectionStatus(
                    conn_id=conn_id,
                    taped=sink is not None,
                    subscriptions=len(supervisor.subscriptions),
                    frames=stats.frames,
                    gaps=stats.gaps,
                    reconnects=stats.reconnects,
                    stale_books=stats.stale_books,
                    sink_dropped=0 if sink is None else sink.stats.records_dropped,
                )
            )
        return RecorderStatus(
            connections=tuple(connections),
            universe_size=0 if self._universe is None else len(self._universe.l2_tickers),
            subscribed_markets=subscribed,
            live_tickers=len(self._tickers),
        )

    # ------------------------------------------------------------------------ control

    async def run(self) -> None:
        """Record until :meth:`stop` or :meth:`request_stop`, then shut down in order.

        Raises:
            RuntimeError: If :meth:`run` was already called; a recorder is single-use.
            KalshiError: If the exchange status or account limits cannot be read at start.
            WireError: If one of those responses does not decode.
            Exception: Whatever ended a supervisor, a sink's writer thread, or an internal
                loop before a stop was requested, re-raised after the shutdown completes.
        """
        if self._run_started:
            raise RuntimeError("a recorder is single-use")
        self._run_started = True
        try:
            if await self._until_stopped(self._start):
                await self._capture()
        finally:
            try:
                await self._shutdown()
            finally:
                # Set even if this run is cancelled mid-shutdown; stop() then waits for the
                # shielded shutdown itself.
                self._finished.set()
        if self._failures:
            raise self._failures[0]

    def request_stop(self) -> None:
        """Ask :meth:`run` to shut down. Non-blocking, idempotent, safe in a signal handler."""
        self._stop_requested.set()

    async def stop(self) -> None:
        """Request a shutdown and return once it has finished. Idempotent.

        A recorder that never ran has nothing to capture; its sinks are closed here instead.
        Failures are reported by :meth:`run`, not here.
        """
        self.request_stop()
        if self._run_started:
            # Bounded: run() reaches its shutdown at once, and every shutdown stage has a
            # deadline.
            await self._finished.wait()
        await self._shutdown()
        self._finished.set()

    # ------------------------------------------------------------------------ startup

    def _layout(self) -> list[SupervisorConfig]:
        """The supervisor configuration of every connection, by ascending id."""
        return [
            SupervisorConfig(
                conn_id=TICKER_CONN_ID,
                book_channels=(),
                firehose_channels=(TICKER_CHANNEL,),
                persist=False,
            ),
            SupervisorConfig(
                conn_id=CONTROL_CONN_ID, book_channels=(), firehose_channels=(LIFECYCLE_CHANNEL,)
            ),
            *(
                SupervisorConfig(conn_id=conn_id, book_channels=BOOK_CHANNELS, use_yes_price=True)
                for conn_id in self._book_conn_ids()
            ),
        ]

    def _book_conn_ids(self) -> range:
        return range(FIRST_BOOK_CONN_ID, FIRST_BOOK_CONN_ID + self._config.book_connections)

    def _header_factory(self, conn_id: int) -> HeaderFactory:
        """Build the segment header factory of one taped connection.

        The factory runs on the sink's writer thread. It reads the supervisor's
        subscription tuple, which the supervisor replaces rather than mutates, and a
        supervisor table that is complete before any sink starts.
        """

        def header() -> SegmentHeader:
            supervisor = self._supervisors[conn_id]
            return SegmentHeader(
                created_wall_ns=int(self._clock.wall_ns()),
                host=self._config.host,
                env=self._config.env,
                conn_id=conn_id,
                ws_url=self._config.ws_url,
                use_yes_price=supervisor.config.use_yes_price,
                subscriptions=list(supervisor.subscriptions),
                software_version=__version__,
                spec_versions=dict(PINNED_SPEC_VERSIONS),
            )

        return header

    async def _start(self) -> None:
        """Check the exchange, size the rate limiter from the account tier, pick a universe.

        Raises:
            KalshiError: If the exchange status or account limits cannot be fetched.
            WireError: If either response does not decode.
        """
        status = await self._rest.exchange_status()
        self._log.log(
            logging.INFO if status.exchange_active else logging.WARNING,
            "exchange status",
            extra={
                "exchange_active": status.exchange_active,
                "trading_active": status.trading_active,
            },
        )
        limits = await self._rest.account_limits()
        read = BucketLimits(
            refill_per_s=limits.read.refill_rate, capacity=limits.read.bucket_capacity
        )
        write = BucketLimits(
            refill_per_s=limits.write.refill_rate, capacity=limits.write.bucket_capacity
        )
        self._limiter.resize(read=read, write=write)
        self._log.info(
            "rate limiter sized from the account tier",
            extra={
                "usage_tier": limits.usage_tier,
                "read_refill_per_s": read.refill_per_s,
                "read_capacity": read.capacity,
                "write_refill_per_s": write.refill_per_s,
                "write_capacity": write.capacity,
            },
        )
        await self._refresh_universe_or_log()

    async def _capture(self) -> None:
        """Start sinks, supervisors, loops, and periodic tasks; return on stop or on a failure."""
        for sink in self._sinks.values():
            sink.start()
        for conn_id, supervisor in self._supervisors.items():
            self._supervisor_tasks[conn_id] = asyncio.create_task(
                supervisor.run(), name=f"supervisor-{conn_id}"
            )
        for name, loop in (
            ("universe", self._universe_loop),
            ("keyframes", self._keyframe_loop),
            ("status", self._status_loop),
        ):
            self._loop_tasks.append(asyncio.create_task(loop(), name=f"recorder-{name}"))
        for index, periodic in enumerate(self._periodic):
            name = f"{type(periodic).__name__}-{index}"
            self._periodic_tasks.append(
                asyncio.create_task(self._run_periodic(periodic, name), name=name)
            )
        critical = [*self._supervisor_tasks.values(), *self._loop_tasks]
        stop_waiter = asyncio.ensure_future(self._stop_requested.wait())
        try:
            await asyncio.wait([*critical, stop_waiter], return_when=asyncio.FIRST_COMPLETED)
        finally:
            await _cancel(stop_waiter)
        for task in critical:
            if task.done() and not (self._stop_requested.is_set() and _returned(task)):
                self._fail(task.get_name(), _outcome(task))

    # ---------------------------------------------------------------------- loops

    async def _universe_loop(self) -> None:
        while await self._pause(self._config.universe_refresh_s):
            await self._refresh_universe_or_log()

    async def _keyframe_loop(self) -> None:
        interval_s = self._config.keyframe_interval_s
        interval_ns = interval_s * NS_PER_S
        while True:
            # Aligned to the interval so keyframes land on :00, :05, ... of every hour.
            delay_ns = interval_ns - int(self._clock.wall_ns()) % interval_ns
            if not await self._pause(delay_ns / NS_PER_S):
                return
            await self._write_keyframe(interval_s=interval_s)

    async def _status_loop(self) -> None:
        while await self._pause(self._config.status_interval_s):
            for conn_id, sink in self._sinks.items():
                if sink.failure is not None:
                    # Every later record would be refused; stopping loudly lets the process
                    # be restarted instead of recording nothing while it looks healthy.
                    raise RuntimeError(f"segment sink {conn_id} failed") from sink.failure
            self._log.info("recorder status", extra=msgspec.to_builtins(self.status()))

    async def _run_periodic(self, task: PeriodicTask, name: str) -> None:
        try:
            await task.run(stop=self._periodic_stop)
        except Exception as exc:
            self._log.exception(
                "periodic task failed; capture continues", extra={"task": name, "error": repr(exc)}
            )
            return
        if not self._periodic_stop.is_set():
            self._log.warning(
                "periodic task returned before the recorder stopped", extra={"task": name}
            )

    async def _pause(self, seconds: float) -> bool:
        """Sleep with the injected sleep. Returns ``False`` if a stop came first."""
        slept = await self._until_stopped(functools.partial(self._sleep, seconds))
        return slept and not self._stop_requested.is_set()

    async def _until_stopped(self, work: Callable[[], Awaitable[None]]) -> bool:
        """Run ``work`` unless a stop is requested first, in which case cancel it.

        Returns:
            ``True`` if the work finished; ``False`` if a stop came first.

        Raises:
            Exception: Whatever the work raised.
        """
        if self._stop_requested.is_set():
            return False
        work_future = asyncio.ensure_future(work())
        stop_future = asyncio.ensure_future(self._stop_requested.wait())
        try:
            # Bounded without a deadline of its own: every call site's work has one.
            await asyncio.wait((work_future, stop_future), return_when=asyncio.FIRST_COMPLETED)
        finally:
            await _cancel(stop_future)
            await _cancel(work_future)
        if work_future.cancelled():
            return False
        work_future.result()
        return True

    # --------------------------------------------------------------------- universe

    async def _refresh_universe_or_log(self) -> None:
        """Refresh the universe; an exchange or decoding failure keeps the current plan."""
        try:
            await self._refresh_universe()
        except (KalshiError, WireError) as exc:
            self._log.error(
                "universe refresh failed; keeping the current plan", extra={"error": repr(exc)}
            )

    async def _refresh_universe(self) -> None:
        """List open markets, select the universe, replan, and hand each connection its groups.

        Raises:
            KalshiError: If a listing page cannot be fetched.
            WireError: If a listing page does not decode.
        """
        summaries, truncated = await self._list_markets()
        now_wall_ns = int(self._clock.wall_ns())
        decision = select(summaries, self._config.universe, now_ts=now_wall_ns // NS_PER_S)
        desired = plan(
            decision.l2_tickers,
            shard_of={summary.ticker: summary.exchange_index for summary in summaries},
            max_per_group=self._config.group_size,
            max_connections=self._config.book_connections,
            previous=self._plan,
        )
        groups_by_conn: dict[int, list[Group]] = {conn_id: [] for conn_id in self._book_conn_ids()}
        for group in desired.groups:
            # The planner numbers book connections from zero; the recorder's start at 2.
            conn_id = FIRST_BOOK_CONN_ID + group.conn_id
            groups_by_conn[conn_id].append(msgspec.structs.replace(group, conn_id=conn_id))
        for conn_id, groups in groups_by_conn.items():
            await self._supervisors[conn_id].set_groups(groups)
        self._plan = desired
        self._universe = decision
        self._conn_of = {
            ticker: FIRST_BOOK_CONN_ID + group.conn_id
            for group in desired.groups
            for ticker in group.tickers
        }
        self._forget_old_tickers(now_wall_ns)
        self._log.info(
            "universe refreshed",
            extra={
                "listed": len(summaries),
                "truncated": truncated,
                "selected": len(decision.l2_tickers),
                "showcase": len(decision.showcase),
                "dropped_for_cap": decision.dropped_for_cap,
                "reason_counts": dict(decision.reason_counts),
                "groups": len(desired.groups),
            },
        )

    async def _list_markets(self) -> tuple[list[MarketSummary], bool]:
        """Page through the open markets, up to the page cap.

        A market that does not convert is skipped and counted rather than failing the whole
        listing, because one malformed entry must not unsubscribe every other market.

        Returns:
            The summaries, and whether the page cap cut the listing short.

        Raises:
            KalshiError: If a page cannot be fetched.
            WireError: If a page does not decode.
        """
        mve_filter = _MVE_FILTER_EXCLUDE if self._config.universe.exclude_mve else None
        summaries: list[MarketSummary] = []
        unreadable = 0
        first_error = ""
        cursor: str | None = None
        truncated = True
        for _ in range(self._config.max_market_pages):
            page = await self._rest.markets(
                status=_OPEN_MARKET_STATUS,
                cursor=cursor,
                limit=MARKET_PAGE_LIMIT,
                mve_filter=mve_filter,
            )
            for market in page.items:
                try:
                    summaries.append(MarketSummary.from_wire(market))
                except (WireError, FixedPointError) as exc:
                    unreadable += 1
                    first_error = first_error or repr(exc)
            if not page.cursor:
                truncated = False
                break
            cursor = page.cursor
        if unreadable:
            self._log.warning(
                "markets skipped because they did not convert",
                extra={"skipped": unreadable, "first_error": first_error},
            )
        if truncated:
            self._log.warning(
                "market listing cut short by the page cap; the universe is partial",
                extra={"max_market_pages": self._config.max_market_pages},
            )
        return summaries, truncated

    def _remember_ticker(self, event: MarketEvent) -> None:
        if isinstance(event, Ticker):
            self._tickers[event.ticker] = event

    def _forget_old_tickers(self, now_wall_ns: int) -> None:
        """Bound the ticker table to markets heard from within :data:`TICKER_RETENTION_NS`."""
        cutoff = now_wall_ns - TICKER_RETENTION_NS
        for ticker in [t for t, e in self._tickers.items() if e.receipt.recv_wall_ns < cutoff]:
            del self._tickers[ticker]

    # --------------------------------------------------------------------- keyframes

    async def _write_keyframe(self, *, interval_s: int) -> None:
        """Write every book to its keyframe slot off the event loop; a failure is logged.

        Args:
            interval_s: Slot length the file name is floored to.
        """
        books = self.books()
        if not books:
            return
        as_of_ns = int(self._clock.wall_ns())
        rows = [
            row for book in books.values() for row in book.to_keyframe(as_of_recv_ns=Ns(as_of_ns))
        ]
        path = keyframe_path(self._config.data_dir, as_of_ns, interval_s=interval_s)
        try:
            async with asyncio.timeout(self._config.keyframe_write_timeout_s):
                written = await asyncio.to_thread(_write_keyframe_file, path, rows)
        except (OSError, pa.ArrowException, TimeoutError) as exc:
            self._log.error("keyframe not written", extra={"path": str(path), "error": repr(exc)})
            return
        self._log.info(
            "keyframe written", extra={"path": str(path), "books": len(books), "rows": written}
        )

    # ---------------------------------------------------------------------- shutdown

    async def _shutdown(self) -> None:
        """Run the shutdown sequence once; later calls wait for the first to finish."""
        if self._shutdown_future is None:
            self._shutdown_future = asyncio.ensure_future(self._shutdown_once())
        await asyncio.shield(self._shutdown_future)

    async def _shutdown_once(self) -> None:
        self._stop_requested.set()
        self._periodic_stop.set()
        await self._drain(self._periodic_tasks)
        await self._drain(self._loop_tasks)
        if self._supervisor_tasks:
            # The shutdown keyframe is floored to its minute, not to the keyframe interval,
            # so it never replaces the periodic image of its slot.
            await self._write_keyframe(interval_s=_SECONDS_PER_MINUTE)
        # One deadline for both: a supervisor that has not exited by then is cancelled, and
        # its stop() returns as soon as its run() does.
        stopping = asyncio.ensure_future(
            asyncio.gather(*(s.stop() for s in self._supervisors.values()), return_exceptions=True)
        )
        await self._drain(list(self._supervisor_tasks.values()))
        await asyncio.wait({stopping}, timeout=self._config.shutdown_timeout_s)
        await _cancel(stopping)
        await self._close_sinks()
        self._log.info("recorder stopped", extra={"failures": len(self._failures)})

    async def _drain(self, tasks: Sequence[asyncio.Task[None]]) -> None:
        """Wait for tasks to end, cancel stragglers, and record every failure."""
        if not tasks:
            return
        timeout_s = self._config.shutdown_timeout_s
        _, pending = await asyncio.wait(tasks, timeout=timeout_s)
        for task in pending:
            self._log.warning(
                "task did not stop in time; cancelling", extra={"task": task.get_name()}
            )
            task.cancel()
        if pending:
            await asyncio.wait(pending, timeout=timeout_s)
        for task in tasks:
            if task.done() and not task.cancelled() and task.exception() is not None:
                self._fail(task.get_name(), _outcome(task))

    async def _close_sinks(self) -> None:
        """Close every sink on a worker thread, so draining the queues never blocks the loop."""
        sinks = list(self._sinks.items())
        results = await asyncio.gather(
            *(asyncio.to_thread(sink.close) for _, sink in sinks), return_exceptions=True
        )
        for (conn_id, sink), result in zip(sinks, results, strict=True):
            if isinstance(result, BaseException):
                self._fail(f"sink-{conn_id}", result)
            elif sink.failure is not None:
                self._fail(f"sink-{conn_id}", sink.failure)

    def _fail(self, component: str, exc: BaseException) -> None:
        """Record a component failure once, however many paths observe it."""
        if any(exc is seen for seen in self._failures):
            return
        self._failures.append(exc)
        self._log.error(
            "recorder component failed",
            extra={"component": component, "error": repr(exc)},
            exc_info=exc,
        )


def _returned(task: asyncio.Task[None]) -> bool:
    """Whether a finished task returned normally rather than raising or being cancelled."""
    return not task.cancelled() and task.exception() is None


def _outcome(task: asyncio.Task[None]) -> BaseException:
    """Describe why a critical task ended: its exception, or an unrequested return."""
    if task.cancelled():
        return asyncio.CancelledError(f"{task.get_name()} was cancelled")
    exc = task.exception()
    if exc is not None:
        return exc
    return RuntimeError(f"{task.get_name()} ended before the recorder was stopped")


async def _cancel[T](future: asyncio.Future[T]) -> None:
    """Cancel a future unless it is done, and wait for the cancellation to land."""
    if not future.done():
        future.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await future


def _write_keyframe_file(path: Path, rows: list[KeyframeRow]) -> int:
    """Write rows in canonical order so the same books always give the same file.

    Runs on a worker thread.

    Raises:
        OSError: If the directory or file cannot be written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    return write_keyframe(path, sorted(rows, key=lambda r: (r.ticker, r.side, r.price_e4)))
