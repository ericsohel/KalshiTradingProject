"""Run ``tape record``: every connection, the universe, keyframes, and an orderly shutdown.

Responsibility: orchestrate the recorder process (docs/ARCHITECTURE.md 4, 5, and 7.1) out
of parts tested on their own. At start it reads the exchange status and sizes the rate
limiter from the account's tier; then it runs one ``ConnectionSupervisor`` per WebSocket
connection, lists and selects the order-book universe and hands each book connection its
subscription groups every ``universe_refresh_s`` (sooner, on a capped backoff, while a
refresh is failing), writes keyframes, logs a status line, tapes a ``clock_jump`` record
when the host slept, and runs auxiliary periodic tasks such as the auditor, which reads books,
sinks, and book taps through it (:meth:`Recorder.open_book_tap`). With a bus publisher it also
publishes every event its supervisors decode; every ``bus_refresh_s``, the catalog of the
markets it records followed by a refresh image of each book it holds, paced in slices across
the interval (ADR 0022); and its status every ``status_interval_s`` (ADR 0023). Dependencies
arrive fully built, so the orchestration is tested against a fake exchange in virtual time.

Connection layout (ADR 0018): connection 0 is live-only and carries the ``ticker`` channel
for every market of the current plan, one group that follows each replan (ADR 0027), whose
latest value per market is kept in memory and never written; connection 1 is taped and
carries ``market_lifecycle_v2``; connections 2 onwards are taped and each carries one planner
group, its whole market set, on ``orderbook_delta`` and ``trade`` with ``use_yes_price``,
because Kalshi keeps one subscription per channel per connection (ADR 0020). No connection
has a data-silence timeout; every one relies on the transport keepalive (ADR 0019).

Invariants: the latest-ticker table holds only markets the ticker connection is meant to
carry, so it never outgrows the plan; a supervisor, sink, or internal loop that fails ends
the run with its exception after a full shutdown, never silently; a periodic task or the
bus refresh cycle that fails is logged and capture continues, and nothing published can
raise into a supervisor or wait on a consumer; a refresh image is read and published
without an await in between, so it reflects exactly the bus messages numbered before it;
shutdown stops auxiliary work and the refresh cycle first, writes a final keyframe while
the books are still live, stops every supervisor, closes the bus publisher, then closes
every sink so that every record accepted is on disk; shutdown runs once however often it
is requested; every wait is raced against the stop request; and the module sleeps, draws
randomness, and reads time only through what was injected.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from pathlib import Path
from types import MappingProxyType
from typing import Final, Protocol

import msgspec
import pyarrow as pa

from tape import __version__
from tape.book import Book, KeyframeRow
from tape.bus.envelope import SequencedPublisher
from tape.bus.ports import Publisher
from tape.client.ratelimit import BucketLimits, RateLimiter
from tape.client.rest import KalshiRest
from tape.client.ws import WsSession
from tape.errors import FixedPointError, KalshiError, WireError
from tape.events import (
    BookRefresh,
    CatalogEntry,
    ConnectionReport,
    MarketCatalog,
    MarketEvent,
    Receipt,
    Side,
    StatusReport,
    Ticker,
)
from tape.recorder.planner import Group, Plan, plan
from tape.recorder.supervisor import (
    ORDERBOOK_CHANNEL,
    ConnectionSupervisor,
    SupervisorConfig,
    backoff_delay_s,
)
from tape.recorder.tap import CompositeBookTap
from tape.recorder.universe import MarketSummary, UniverseDecision, UniversePolicy, select
from tape.recorder.writer import HeaderFactory, SegmentSink
from tape.segment import Record, RecordKind, SegmentHeader, write_keyframe
from tape.timeutil import NS_PER_S, Clock, Ns, wall_ns_to_datetime

__all__ = [
    "BOOK_CHANNELS",
    "BUS_REFRESH_SLICES_PER_S",
    "CLOCK_JUMP_THRESHOLD_NS",
    "CONTROL_CONN_ID",
    "DEFAULT_BUS_REFRESH_S",
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
    "TICKER_GROUP_ID",
    "UNIVERSE_RETRY_INITIAL_S",
    "BusStatus",
    "ConnectionStatus",
    "PeriodicTask",
    "Recorder",
    "RecorderConfig",
    "RecorderStatus",
    "SessionBuilder",
    "SinkBuilder",
    "check_book_capacity",
    "check_connection_budget",
    "check_keyframe_interval",
    "is_clock_jump",
    "keyframe_path",
    "refresh_slices",
    "universe_retry_delay_s",
]

TICKER_CONN_ID: Final = 0
"""The live-only connection carrying the ``ticker`` channel for the plan's markets (ADR 0027)."""

CONTROL_CONN_ID: Final = 1
"""The taped connection carrying ``market_lifecycle_v2``, which accepts no market filter."""

FIRST_BOOK_CONN_ID: Final = 2
"""Book connections take ids from here; planner connection ``i`` is connection ``2 + i``."""

TICKER_CHANNEL: Final = "ticker"
LIFECYCLE_CHANNEL: Final = "market_lifecycle_v2"
BOOK_CHANNELS: Final = (ORDERBOOK_CHANNEL, "trade")

TICKER_GROUP_ID: Final = "tickers"
"""``group_id`` of the ticker connection's group, which holds every market of the plan."""

MAX_GROUP_SIZE: Final = 500
"""Most markets on one order-book connection, all in its one subscription (ADR 0020)."""

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

UNIVERSE_RETRY_INITIAL_S: Final = 15
"""Nominal wait before retrying a failed universe refresh; it doubles up to the interval."""

CLOCK_JUMP_THRESHOLD_NS: Final = 5 * NS_PER_S
"""Excess of wall over monotonic time between status ticks that counts as a clock jump.

Monotonic clocks stop while macOS and Linux hosts sleep, and wall clocks do not. The margin
absorbs scheduling delay and NTP slewing, which move the two by milliseconds, not seconds.
"""

DEFAULT_BUS_REFRESH_S: Final = 10
"""Seconds between two refresh images of the same book on the bus (ADR 0022)."""

BUS_REFRESH_SLICES_PER_S: Final = 10
"""Most refresh slices per second of the cycle, so many books go out in steps of 100 ms."""

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


def check_book_capacity(*, max_l2_markets: int, group_size: int, book_connections: int) -> None:
    """Check that the book connections can carry the order-book universe.

    Each book connection carries at most ``group_size`` markets (ADR 0020), so the budget
    needs ``ceil(max_l2_markets / group_size)`` of them. Showcase markets may still exceed
    the budget at run time; the recorder logs the markets it has no room for.

    Args:
        max_l2_markets: Budget of order-book markets.
        group_size: Most markets on one book connection; positive.
        book_connections: Connections carrying order-book groups.

    Raises:
        ValueError: If ``book_connections * group_size < max_l2_markets``; the message names
            the settings that would fix it.
    """
    capacity = book_connections * group_size
    if capacity >= max_l2_markets:
        return
    needed = -(-max_l2_markets // group_size)
    raise ValueError(
        f"max_l2_markets = {max_l2_markets} needs {needed} book connections at group_size = "
        f"{group_size}, but book_connections = {book_connections} carry only {capacity} "
        f"markets; set book_connections to at least {needed} (and max_connections to at "
        f"least {FIRST_BOOK_CONN_ID + needed}) or lower max_l2_markets to {capacity}"
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


def universe_retry_delay_s(consecutive_failures: int, *, refresh_s: int, jitter: float) -> float:
    """Return how long to wait before retrying a failed universe refresh.

    The delay is ``min(refresh_s, UNIVERSE_RETRY_INITIAL_S * 2**consecutive_failures)``
    scaled into its upper half by ``jitter``, so a transient failure costs seconds of book
    capture rather than a whole refresh interval, and a persistent one never polls the
    exchange more often than a jittered interval would.

    Args:
        consecutive_failures: Failed refreshes in a row before this retry, starting at zero.
        refresh_s: The refresh interval, which caps the nominal delay.
        jitter: A draw from ``[0, 1)``.

    Returns:
        Seconds to wait, in ``[nominal / 2, nominal)``.

    Raises:
        ValueError: If ``consecutive_failures`` is negative, ``refresh_s`` is not positive,
            or ``jitter`` is outside ``[0, 1)``.
    """
    max_ns = refresh_s * NS_PER_S
    return backoff_delay_s(
        consecutive_failures,
        # An interval shorter than the initial delay caps every retry at the interval.
        initial_ns=min(UNIVERSE_RETRY_INITIAL_S * NS_PER_S, max_ns),
        max_ns=max_ns,
        jitter=jitter,
    )


def is_clock_jump(
    *, wall_ns_delta: int, mono_ns_delta: int, threshold_ns: int = CLOCK_JUMP_THRESHOLD_NS
) -> bool:
    """Whether wall time outran monotonic time by more than ``threshold_ns``.

    Between two readings on an awake host both clocks advance alike. When the host sleeps
    the monotonic clock stops while the wall clock does not, so the excess is how long
    nothing could be recorded; a forward step of the wall clock looks the same.

    Args:
        wall_ns_delta: Wall-clock nanoseconds between the two readings.
        mono_ns_delta: Monotonic nanoseconds between the same two readings.
        threshold_ns: Excess that counts as a jump.

    Returns:
        ``True`` if ``wall_ns_delta - mono_ns_delta > threshold_ns``.
    """
    return wall_ns_delta - mono_ns_delta > threshold_ns


def refresh_slices(tickers: Sequence[str], *, interval_s: int) -> tuple[tuple[str, ...], ...]:
    """Split one bus refresh cycle's markets into slices published one pause apart.

    A cycle publishes a slice, waits ``interval_s / len(slices)``, and moves on, so it lasts
    ``interval_s`` whatever the number of books and never publishes them in one burst. There
    is one slice per market up to :data:`BUS_REFRESH_SLICES_PER_S` slices per second; with no
    markets there is a single empty slice, so an idle cycle still waits out its interval.

    Args:
        tickers: The markets to refresh, in publishing order.
        interval_s: Length of the cycle in seconds.

    Returns:
        Slices whose concatenation is ``tickers`` and whose sizes differ by at most one.

    Raises:
        ValueError: If ``interval_s`` is not positive.
    """
    if interval_s <= 0:
        raise ValueError(f"interval_s must be positive, got {interval_s}")
    total = len(tickers)
    count = max(1, min(total, interval_s * BUS_REFRESH_SLICES_PER_S))
    return tuple(
        tuple(tickers[index * total // count : (index + 1) * total // count])
        for index in range(count)
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
        book_connections: Connections carrying order-book groups, one group each.
        group_size: Most markets on one book connection, and in one subscription command on
            any connection.
        keyframe_interval_s: Seconds between keyframes.
        universe_refresh_s: Seconds between market listings; a failed listing is retried
            sooner, see :func:`universe_retry_delay_s`.
        status_interval_s: Seconds between status log lines and clock-jump checks.
        max_market_pages: Page cap on one market listing.
        shutdown_timeout_s: Deadline for each shutdown stage.
        keyframe_write_timeout_s: Deadline for writing one keyframe file.
        bus_refresh_s: Seconds between refresh images of each book on the bus; used only when
            the recorder is given a publisher.

    Raises:
        ValueError: On a layout over ``max_connections``, a group size outside
            ``[1, MAX_GROUP_SIZE]``, book connections too few for ``universe.max_l2_markets``,
            a keyframe interval that does not tile an hour, or a non-positive interval, cap,
            or timeout.
    """

    env: str
    ws_url: str
    data_dir: Path
    host: str
    universe: UniversePolicy
    max_connections: int = 16
    book_connections: int = 4
    group_size: int = MAX_GROUP_SIZE
    keyframe_interval_s: int = 300
    universe_refresh_s: int = 300
    status_interval_s: int = 60
    max_market_pages: int = DEFAULT_MAX_MARKET_PAGES
    shutdown_timeout_s: int = DEFAULT_SHUTDOWN_TIMEOUT_S
    keyframe_write_timeout_s: int = DEFAULT_KEYFRAME_WRITE_TIMEOUT_S
    bus_refresh_s: int = DEFAULT_BUS_REFRESH_S

    def __post_init__(self) -> None:
        check_connection_budget(
            book_connections=self.book_connections, max_connections=self.max_connections
        )
        check_keyframe_interval(self.keyframe_interval_s)
        if not 1 <= self.group_size <= MAX_GROUP_SIZE:
            raise ValueError(f"group_size must be in [1, {MAX_GROUP_SIZE}], got {self.group_size}")
        check_book_capacity(
            max_l2_markets=self.universe.max_l2_markets,
            group_size=self.group_size,
            book_connections=self.book_connections,
        )
        for name in (
            "universe_refresh_s",
            "status_interval_s",
            "max_market_pages",
            "shutdown_timeout_s",
            "keyframe_write_timeout_s",
            "bus_refresh_s",
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


class BusStatus(msgspec.Struct, frozen=True, kw_only=True):
    """The bus publisher's part of the status log.

    Attributes:
        bus_epoch: The epoch every message of this run carries.
        bus_seq: Number of the latest message attempted; ``sent + dropped + errors`` equals it.
        sent: Messages handed to ZeroMQ. A slow subscriber can still lose its copy, which it
            alone sees, as a gap in ``bus_seq``.
        dropped: Messages the transport refused outright.
        errors: Messages that failed for any other reason.
        refreshes: Book refresh images published.
    """

    bus_epoch: int
    bus_seq: int
    sent: int
    dropped: int
    errors: int
    refreshes: int


class RecorderStatus(msgspec.Struct, frozen=True, kw_only=True):
    """The whole recorder at a glance, logged every ``status_interval_s``.

    Attributes:
        connections: Every connection, by ascending id.
        universe_size: Markets the last universe selection chose.
        subscribed_markets: Markets of book connections whose subscriptions are live now.
        live_tickers: Markets of the current plan with a latest ``ticker`` value in memory;
            never more than the plan holds (ADR 0027).
        bus: The bus publisher's counters, or ``None`` when the recorder has no bus.
    """

    connections: tuple[ConnectionStatus, ...]
    universe_size: int
    subscribed_markets: int
    live_tickers: int
    bus: BusStatus | None = None


class SessionBuilder(Protocol):
    """Builds a new, unconnected WebSocket session; sessions are single-use."""

    def __call__(self, url: str, *, conn_id: int) -> WsSession:
        """Return a session for ``url`` whose errors name connection ``conn_id``.

        Every connection is built alike: liveness is the transport keepalive, and no
        session has a data-silence timeout (ADR 0019, ADR 0027).
        """
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
        jitter: Returns a draw from ``[0, 1)`` for each reconnect or universe retry backoff.
        periodic_tasks: Auxiliary work; a failure in one is logged and capture continues.
        publisher: Where every decoded event and each book's refresh image go, or ``None`` for
            no bus. Its start time is taken now as the bus epoch, and the recorder closes it
            after the supervisors stop.
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
        publisher: Publisher | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._clock = clock
        self._rest = rest
        self._limiter = limiter
        self._sleep = sleep
        self._jitter = jitter
        self._periodic = tuple(periodic_tasks)
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._bus = (
            None
            if publisher is None
            else SequencedPublisher(publisher, epoch=int(clock.wall_ns()), logger=logger)
        )
        self._bus_refreshes = 0
        self._tickers: dict[str, Ticker] = {}
        self._plan = Plan(groups=())
        self._universe: UniverseDecision | None = None
        self._conn_of: dict[str, int] = {}
        self._universe_failures = 0
        self._universe_wait_s: float = config.universe_refresh_s
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
                on_event=self._event_consumer(conn_id),
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
        return MappingProxyType({ticker: book for ticker, (_, book) in self._held_books().items()})

    def sink_for(self, ticker: str) -> SegmentSink | None:
        """Return the sink of the book connection that the current plan assigns a market to.

        Args:
            ticker: Market ticker.

        Returns:
            That connection's sink, or ``None`` if the market is not in the plan.
        """
        conn_id = self._conn_of.get(ticker)
        return None if conn_id is None else self._sinks.get(conn_id)

    def open_book_tap(self, tickers: Iterable[str], *, max_events: int) -> CompositeBookTap:
        """Open one tap over markets on whichever book connections the plan assigns them to.

        Each market is tapped on the connection that :meth:`sink_for` also names, so its audit
        record lands beside the frames the tap saw. A market the plan does not place has no
        book to observe and gets an absent window.

        Args:
            tickers: Markets to observe; duplicates are ignored.
            max_events: Most book changes held per market.

        Returns:
            A tap whose :meth:`CompositeBookTap.close` closes every connection's tap.

        Raises:
            ValueError: If ``max_events`` is not positive; no tap is opened then.
        """
        if max_events <= 0:
            raise ValueError(f"max_events must be positive, got {max_events}")
        by_conn: dict[int, list[str]] = {}
        uncovered: list[str] = []
        for ticker in sorted(set(tickers)):
            conn_id = self._conn_of.get(ticker)
            if conn_id is None:
                uncovered.append(ticker)
            else:
                by_conn.setdefault(conn_id, []).append(ticker)
        taps = [
            self._supervisors[conn_id].open_tap(group, max_events=max_events)
            for conn_id, group in sorted(by_conn.items())
        ]
        return CompositeBookTap(taps, uncovered=uncovered)

    def latest_tickers(self) -> Mapping[str, Ticker]:
        """The latest ``ticker`` value per market from the live-only connection; never taped.

        Only markets of the current plan are held; a market is dropped at the universe
        refresh that removes it (ADR 0027).

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
            group = supervisor.group
            if (
                conn_id in self._book_conn_ids()
                and group is not None
                and any(info.group_id == group.group_id for info in supervisor.subscriptions)
            ):
                subscribed += len(group.tickers)
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
            bus=None if self._bus is None else self._bus_status(self._bus),
        )

    def _bus_status(self, bus: SequencedPublisher) -> BusStatus:
        stats = bus.stats
        return BusStatus(
            bus_epoch=bus.epoch,
            bus_seq=bus.last_seq,
            sent=stats.sent,
            dropped=stats.dropped,
            errors=stats.errors,
            refreshes=self._bus_refreshes,
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
        """The supervisor configuration of every connection, by ascending id.

        No command names more than ``group_size`` markets on any connection, so the ticker
        connection, whose group is the whole plan, reaches it in batches that join one
        subscription (ADR 0020, ADR 0027).
        """
        batch = self._config.group_size
        return [
            SupervisorConfig(
                conn_id=TICKER_CONN_ID,
                group_channels=(TICKER_CHANNEL,),
                persist=False,
                max_markets_per_command=batch,
            ),
            SupervisorConfig(
                conn_id=CONTROL_CONN_ID, group_channels=(), firehose_channels=(LIFECYCLE_CHANNEL,)
            ),
            *(
                SupervisorConfig(
                    conn_id=conn_id,
                    group_channels=BOOK_CHANNELS,
                    use_yes_price=True,
                    max_markets_per_command=batch,
                )
                for conn_id in self._book_conn_ids()
            ),
        ]

    def _book_conn_ids(self) -> range:
        return range(FIRST_BOOK_CONN_ID, FIRST_BOOK_CONN_ID + self._config.book_connections)

    def _event_consumer(self, conn_id: int) -> Callable[[MarketEvent], None] | None:
        """What one connection's supervisor hands each decoded event to.

        The ticker connection's events update the latest-ticker table; with a bus, every
        connection's events are also published, including ticker updates the table does not
        keep. ``SequencedPublisher.publish`` never raises, and the supervisor would count and
        log it if it did.
        """
        bus = self._bus
        if conn_id != TICKER_CONN_ID:
            return None if bus is None else bus.publish
        if bus is None:
            return self._remember_ticker

        def remember_and_publish(event: MarketEvent) -> None:
            self._remember_ticker(event)
            bus.publish(event)

        return remember_and_publish

    def _held_books(self) -> dict[str, tuple[int, Book]]:
        """Every book across the book connections, with the connection holding it.

        A market briefly subscribed on two connections while it moves between them is held by
        whichever copy is not stale, and by the lower connection id when neither or both are.
        """
        merged: dict[str, tuple[int, Book]] = {}
        for conn_id in self._book_conn_ids():
            for ticker, book in self._supervisors[conn_id].books.items():
                held = merged.get(ticker)
                if held is None or (held[1].is_stale() and not book.is_stale()):
                    merged[ticker] = (conn_id, book)
        return merged

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
        if self._bus is not None:
            self._periodic_tasks.append(
                asyncio.create_task(self._run_bus_refresh(self._bus), name="bus-refresh")
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
        # The wait is set by the refresh before it, so a failure at startup is retried soon.
        # The refresh itself also races the stop: listing a hundred-odd pages must not hold
        # up shutdown, and a plan left half applied is harmless when every connection stops.
        while await self._pause(self._universe_wait_s):
            if not await self._until_stopped(self._refresh_universe_or_log):
                return

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
        mono_ns, wall_ns = int(self._clock.mono_ns()), int(self._clock.wall_ns())
        while await self._pause(self._config.status_interval_s):
            mono_ns, wall_ns = self._note_clock_jump(since_mono_ns=mono_ns, since_wall_ns=wall_ns)
            for conn_id, sink in self._sinks.items():
                if sink.failure is not None:
                    # Every later record would be refused; stopping loudly lets the process
                    # be restarted instead of recording nothing while it looks healthy.
                    raise RuntimeError(f"segment sink {conn_id} failed") from sink.failure
            status = self.status()
            self._log.info("recorder status", extra=msgspec.to_builtins(status))
            if self._bus is not None:
                # Never raises, like every publish; a consumer learns of capture health here.
                self._bus.publish(_status_report(status, interval_s=self._config.status_interval_s))

    def _note_clock_jump(self, *, since_mono_ns: int, since_wall_ns: int) -> tuple[int, int]:
        """Tape a ``clock_jump`` on every taped connection if the host slept since a reading.

        Nothing notices a sleep otherwise: timers run on the monotonic clock, which stops
        with the host, so the tape would show a gap with no cause. The record is stamped
        when the jump is noticed, after the gap; its deltas say how long the gap was.

        Args:
            since_mono_ns: Monotonic reading at the previous check.
            since_wall_ns: Wall-clock reading at the previous check.

        Returns:
            The current monotonic and wall-clock readings, the baseline for the next check.
        """
        mono_ns, wall_ns = int(self._clock.mono_ns()), int(self._clock.wall_ns())
        mono_ns_delta, wall_ns_delta = mono_ns - since_mono_ns, wall_ns - since_wall_ns
        if not is_clock_jump(wall_ns_delta=wall_ns_delta, mono_ns_delta=mono_ns_delta):
            return mono_ns, wall_ns
        self._log.warning(
            "wall clock jumped ahead of the monotonic clock; the host probably slept",
            extra={"wall_ns_delta": wall_ns_delta, "mono_ns_delta": mono_ns_delta},
        )
        payload = msgspec.json.encode(
            {"event": "clock_jump", "wall_ns_delta": wall_ns_delta, "mono_ns_delta": mono_ns_delta}
        )
        for conn_id, sink in self._sinks.items():
            # A refusal is counted by the sink and reported in the status line.
            sink.put(
                Record(
                    kind=RecordKind.CONNECTION,
                    conn_id=conn_id,
                    recv_mono_ns=mono_ns,
                    recv_wall_ns=wall_ns,
                    payload=payload,
                )
            )
        return mono_ns, wall_ns

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

    async def _run_bus_refresh(self, bus: SequencedPublisher) -> None:
        """Run the bus refresh cycle; as for a periodic task, a failure is logged, not raised."""
        try:
            await self._bus_refresh_loop(bus)
        except Exception as exc:
            self._log.exception(
                "bus refresh failed; capture continues without refresh images",
                extra={"error": repr(exc)},
            )

    async def _bus_refresh_loop(self, bus: SequencedPublisher) -> None:
        interval_s = self._config.bus_refresh_s
        while True:
            # The catalog opens every cycle, so a consumer that has just started knows the
            # recorded markets within one interval, as it knows the books (ADR 0023).
            if self._universe is not None:
                bus.publish(_catalog(self._universe))
            # Markets are fixed per cycle; one that appears mid-cycle waits for the next.
            slices = refresh_slices(sorted(self._held_books()), interval_s=interval_s)
            for tickers in slices:
                self._publish_refreshes(bus, tickers)
                if not await self._pause(interval_s / len(slices)):
                    return

    def _publish_refreshes(self, bus: SequencedPublisher, tickers: Iterable[str]) -> None:
        """Publish the refresh image of each market still held, without awaiting.

        No frame can be applied between reading a book and publishing its image, so the image
        is the book as of every bus message numbered before it (ADR 0022). A market no longer
        held is skipped.
        """
        held = self._held_books()
        mono_ns, wall_ns = self._clock.mono_ns(), self._clock.wall_ns()
        for ticker in tickers:
            entry = held.get(ticker)
            if entry is None:
                continue
            conn_id, book = entry
            receipt = Receipt(conn_id=conn_id, recv_mono_ns=mono_ns, recv_wall_ns=wall_ns)
            bus.publish(
                BookRefresh(
                    ticker=ticker,
                    ts_ms=book.last_ts_ms,
                    receipt=receipt,
                    stale=book.is_stale(),
                    bids=tuple(book.levels(Side.BID)),
                    asks=tuple(book.levels(Side.ASK)),
                )
            )
            self._bus_refreshes += 1

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
        """Refresh the universe and set the wait before the next refresh.

        An exchange or decoding failure keeps the current plan and schedules a retry on a
        capped, jittered backoff (:func:`universe_retry_delay_s`); a success waits the full
        interval and resets the backoff.
        """
        try:
            await self._refresh_universe()
        except (KalshiError, WireError) as exc:
            retry_in_s = universe_retry_delay_s(
                self._universe_failures,
                refresh_s=self._config.universe_refresh_s,
                jitter=self._jitter(),
            )
            self._universe_failures += 1
            self._universe_wait_s = retry_in_s
            self._log.error(
                "universe refresh failed; keeping the current plan and retrying",
                extra={
                    "error": repr(exc),
                    "consecutive_failures": self._universe_failures,
                    "retry_in_s": retry_in_s,
                },
            )
            return
        self._universe_failures = 0
        self._universe_wait_s = self._config.universe_refresh_s

    async def _refresh_universe(self) -> None:
        """List open markets, select the universe, replan, and hand each connection its group.

        Book connections get their planner groups; the ticker connection gets every market
        of the plan, and the latest-ticker table drops the markets it no longer carries.

        Raises:
            KalshiError: If a listing page cannot be fetched.
            WireError: If a listing page does not decode.
        """
        summaries, truncated = await self._list_markets()
        now_wall_ns = int(self._clock.wall_ns())
        decision = select(summaries, self._config.universe, now_ts=now_wall_ns // NS_PER_S)
        desired = plan(
            decision.l2_tickers,
            max_per_group=self._config.group_size,
            max_connections=self._config.book_connections,
            previous=self._plan,
        )
        # The planner numbers book connections from zero; the recorder's start at 2.
        group_of_conn: dict[int, Group] = {
            FIRST_BOOK_CONN_ID + group.conn_id: msgspec.structs.replace(
                group, conn_id=FIRST_BOOK_CONN_ID + group.conn_id
            )
            for group in desired.groups
        }
        for conn_id in self._book_conn_ids():
            await self._supervisors[conn_id].set_group(group_of_conn.get(conn_id))
        await self._supervisors[TICKER_CONN_ID].set_group(_ticker_group(desired))
        self._forget_unrecorded_tickers()
        unplaced = decision.l2_tickers - desired.tickers
        if unplaced:
            # Possible only when showcase markets alone exceed the budget, which configuration
            # validation otherwise guarantees the book connections can carry.
            self._log.error(
                "markets left without a book connection; raise book_connections",
                extra={
                    "unplaced": len(unplaced),
                    "first_unplaced": sorted(unplaced)[0],
                    "book_capacity": self._config.book_connections * self._config.group_size,
                },
            )
        self._plan = desired
        self._universe = decision
        self._conn_of = {
            ticker: FIRST_BOOK_CONN_ID + group.conn_id
            for group in desired.groups
            for ticker in group.tickers
        }
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
                "unplaced": len(unplaced),
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

    def _ticker_markets(self) -> frozenset[str]:
        """Markets the ticker connection is meant to carry: the plan's, as last handed to it."""
        group = self._supervisors[TICKER_CONN_ID].group
        return frozenset() if group is None else group.tickers

    def _remember_ticker(self, event: MarketEvent) -> None:
        """Keep a ticker update as its market's latest value, if the market is recorded.

        An update can still arrive for a market just removed from the subscription, before
        the exchange applies the removal; it is published, but not kept.
        """
        if isinstance(event, Ticker) and event.ticker in self._ticker_markets():
            self._tickers[event.ticker] = event

    def _forget_unrecorded_tickers(self) -> None:
        """Drop the latest values of markets the ticker connection no longer carries."""
        recorded = self._ticker_markets()
        for ticker in [t for t in self._tickers if t not in recorded]:
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
        if self._bus is not None:
            # After the supervisors, whose last events still go out; closing never waits.
            self._bus.close()
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


def _ticker_group(desired: Plan) -> Group | None:
    """The ticker connection's market set: every market of the plan, or ``None`` for none."""
    tickers = desired.tickers
    if not tickers:
        return None
    return Group(group_id=TICKER_GROUP_ID, conn_id=TICKER_CONN_ID, tickers=tickers)


def _catalog(decision: UniverseDecision) -> MarketCatalog:
    """The bus catalog of a universe decision: one entry per recorded market, in ticker order."""
    return MarketCatalog(
        markets=tuple(
            CatalogEntry(
                ticker=market.ticker,
                series_ticker=market.series_ticker,
                event_ticker=market.event_ticker,
                volume_24h=market.volume_24h,
                close_ts=market.close_ts,
                showcase=market.ticker in decision.showcase,
            )
            for market in decision.markets
        )
    )


def _status_report(status: RecorderStatus, *, interval_s: int) -> StatusReport:
    """The part of a status line that the bus carries to live consumers (ADR 0023)."""
    return StatusReport(
        interval_s=interval_s,
        universe_size=status.universe_size,
        subscribed_markets=status.subscribed_markets,
        connections=tuple(
            ConnectionReport(
                conn_id=connection.conn_id,
                taped=connection.taped,
                frames=connection.frames,
                gaps=connection.gaps,
                reconnects=connection.reconnects,
                stale_books=connection.stale_books,
                sink_dropped=connection.sink_dropped,
            )
            for connection in status.connections
        ),
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
