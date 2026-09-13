"""Run ``tape record``: every connection, the universe, keyframes, and an orderly shutdown.

Responsibility: orchestrate the recorder process (docs/ARCHITECTURE.md 4, 5, and 7.1) out
of parts tested on their own. At start it reads the exchange status and sizes the rate
limiter from the account's tier; then it runs one ``ConnectionSupervisor`` per WebSocket
connection, lists the open markets, selects the order-book universe by its groups (ADR 0028),
and hands each book connection its subscription groups every ``universe_refresh_s`` (sooner, on
a capped backoff, while a refresh is failing). Between those full refreshes it reacts to market
closes (ADR 0029): a few seconds after a planned market's close time, or as soon as a lifecycle
event says it was determined or settled, the market leaves the plan without a listing, and the
series groups that lost a market, or whose series gained one, are listed again and re-applied
alone. It also writes keyframes, logs a status line, tapes a ``clock_jump`` record when the host
slept, and runs auxiliary periodic tasks such as the auditor, which reads books, sinks, and book
taps through it (:meth:`Recorder.open_book_tap`). With a bus publisher it also publishes every
event its supervisors decode; every ``bus_refresh_s``, the catalog of the markets it records
followed by a refresh image of each book it holds, paced in slices across the interval (ADR
0022); and its status every ``status_interval_s`` (ADR 0023). Dependencies arrive fully built, so
the orchestration is tested against a fake exchange in virtual time.

Connection layout (ADR 0018): connection 0 is live-only and carries the ``ticker`` channel
for every market of the current plan, one group that follows each replan (ADR 0027), whose
latest value per market is kept in memory and never written; connection 1 is taped and
carries ``market_lifecycle_v2``; connections 2 onwards are taped and each carries one planner
group, its whole market set, on ``orderbook_delta`` and ``trade`` with ``use_yes_price``,
because Kalshi keeps one subscription per channel per connection (ADR 0020). No connection
has a data-silence timeout; every one relies on the transport keepalive (ADR 0019).

Invariants: the latest-ticker table holds only markets the ticker connection is meant to
carry, so it never outgrows the plan; only the universe loop changes the plan, one step at a
time; a lifecycle event is noted without awaiting, and noting it never raises into the control
connection's supervisor; a targeted re-listing starts at least :data:`RELIST_MIN_INTERVAL_S`
after the previous one started, and only a full refresh replaces a category group's markets;
a near-price group is listed again for one event at most :data:`NEAR_PRICE_FOLLOW_UPS` times
while its admitted markets are not all priced;
every catalog leaves out the markets reported determined or settled and gives close times as
lifecycle events last moved them; a supervisor, sink, or internal loop that fails ends
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
from collections.abc import Awaitable, Callable, Collection, Iterable, Mapping, Sequence
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
from tape.errors import KalshiError, WireError
from tape.events import (
    LIFECYCLE_ACTIVATED,
    LIFECYCLE_CLOSE_DATE_UPDATED,
    LIFECYCLE_CREATED,
    LIFECYCLE_DETERMINED,
    LIFECYCLE_SETTLED,
    BookRefresh,
    CatalogEntry,
    ConnectionReport,
    Lifecycle,
    MarketCatalog,
    MarketEvent,
    Receipt,
    Side,
    StatusReport,
    Ticker,
)
from tape.recorder.listing import (
    DEFAULT_MAX_MARKET_PAGES,
    MarketListing,
    SeriesCategories,
    list_open_markets,
    list_series_markets,
)
from tape.recorder.planner import Group, Plan, plan
from tape.recorder.supervisor import (
    ORDERBOOK_CHANNEL,
    ConnectionSupervisor,
    SupervisorConfig,
    backoff_delay_s,
)
from tape.recorder.tap import CompositeBookTap
from tape.recorder.universe import (
    MARKET_ORDER_NEAR_PRICE,
    MarketSummary,
    UniverseDecision,
    UniversePolicy,
    select,
    series_of,
    yes_mid,
)
from tape.recorder.writer import HeaderFactory, SegmentSink
from tape.segment import Record, RecordKind, SegmentHeader, write_keyframe
from tape.timeutil import NS_PER_S, Clock, Ns, wall_ns_to_datetime

__all__ = [
    "BOOK_CHANNELS",
    "BUS_REFRESH_SLICES_PER_S",
    "CLOCK_JUMP_THRESHOLD_NS",
    "CLOSE_TICK_DELAY_S",
    "CONTROL_CONN_ID",
    "DEFAULT_BUS_REFRESH_S",
    "DEFAULT_KEYFRAME_WRITE_TIMEOUT_S",
    "DEFAULT_SHUTDOWN_TIMEOUT_S",
    "FIRST_BOOK_CONN_ID",
    "LIFECYCLE_CHANNEL",
    "MAX_GROUP_SIZE",
    "NEAR_PRICE_FOLLOW_UPS",
    "PINNED_SPEC_VERSIONS",
    "RELIST_DEBOUNCE_S",
    "RELIST_MAX_PAGES",
    "RELIST_MIN_INTERVAL_S",
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

CLOSE_TICK_DELAY_S: Final = 3
"""Seconds after a planned market's close time before the tick that removes it (ADR 0029).

A listing taken at the very second of a close can still show the closed market open and its
successor not yet open; a few seconds later both have settled.
"""

RELIST_DEBOUNCE_S: Final = 5
"""Seconds after a ``created`` or ``activated`` event in a series group's series before that group
is listed again, so the markets of a new event, which appear together, are listed together."""

RELIST_MIN_INTERVAL_S: Final = 30
"""Least seconds between the starts of two targeted re-listings, so a burst costs one."""

RELIST_MAX_PAGES: Final = 10
"""Page cap per series on a targeted re-listing; one series lists a few hundred open markets."""

NEAR_PRICE_FOLLOW_UPS: Final = 4
"""Most follow-up re-listings of a ``near_price`` series group for one event whose admitted
markets are not all priced (ADR 0029).

A new event is often listed before its first quotes, when near-price order can only fall back to
volume and ticker; its quotes usually arrive within a minute or two, which four attempts at the
minimum interval span.
"""

_SECONDS_PER_MINUTE: Final = 60
_SECONDS_PER_HOUR: Final = 3_600
_NO_CATEGORIES: Final[Mapping[str, str]] = MappingProxyType({})
_ENDED: Final = frozenset({LIFECYCLE_DETERMINED, LIFECYCLE_SETTLED})
"""Lifecycle events after which a market is closed whatever its close time says."""
_OPENED: Final = frozenset({LIFECYCLE_CREATED, LIFECYCLE_ACTIVATED})
"""Lifecycle events after which a series may have a market its groups have not listed."""


class _PendingRelist(msgspec.Struct, frozen=True, kw_only=True):
    """A series group waiting for a targeted re-listing.

    Attributes:
        due_ns: Monotonic time from which the re-listing may run.
        requested_ns: Monotonic time of the latest request; a re-listing that started later
            covers it.
    """

    due_ns: int
    requested_ns: int


class _FollowUp(msgspec.Struct, frozen=True, kw_only=True):
    """Follow-up re-listings of one ``near_price`` group for one event not yet all priced.

    Attributes:
        attempts: Follow-up re-listings requested so far, at most :data:`NEAR_PRICE_FOLLOW_UPS`.
        gave_up: Whether the attempts ran out with the event still not all priced.
    """

    attempts: int
    gave_up: bool


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
        universe_refresh_s: Seconds between full market listings; a failed listing is retried
            sooner, see :func:`universe_retry_delay_s`. Closes are handled between them (ADR 0029).
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
        # Series categories cost a request per category an hour, so only category groups ask.
        self._categories = (
            SeriesCategories(
                rest, categories=config.universe.categories, clock=clock, logger=logger
            )
            if config.universe.categories
            else None
        )
        self._conn_of: dict[str, int] = {}
        self._universe_failures = 0
        self._refresh_due_ns = 0
        # What lifecycle events said since the last full listing, for the universe loop (ADR 0029).
        self._universe_wake = asyncio.Event()
        self._ended: set[str] = set()
        self._moved_close_ts: dict[str, int] = {}
        self._relists: dict[str, _PendingRelist] = {}
        self._relist_allowed_ns = 0
        self._series_groups = _series_groups_by_series(config.universe)
        # Near-price events admitted before their quotes, by group and event ticker.
        self._follow_ups: dict[tuple[str, str], _FollowUp] = {}
        self._follow_ups_given_up = 0
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

        The ticker connection's events update the latest-ticker table, and the control
        connection's lifecycle events are noted for the universe loop (ADR 0029); with a bus,
        every connection's events are also published, including those nothing here keeps. Neither
        consumer awaits or raises, ``SequencedPublisher.publish`` never raises, and the supervisor
        would count and log it if one did.
        """
        consumers: dict[int, Callable[[MarketEvent], None]] = {
            TICKER_CONN_ID: self._remember_ticker,
            CONTROL_CONN_ID: self._note_lifecycle,
        }
        consume = consumers.get(conn_id)
        bus = self._bus
        if bus is None:
            return consume
        if consume is None:
            return bus.publish

        def consume_and_publish(event: MarketEvent) -> None:
            consume(event)
            bus.publish(event)

        return consume_and_publish

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
        # Each pass waits for the first universe work due: the full refresh, whose time the
        # refresh before it set, so a failure at startup is retried soon; the close of a planned
        # market; or a targeted re-listing (ADR 0029). A noted lifecycle event cuts the wait
        # short, so the loop looks again. The work races the stop like the wait: listing a
        # hundred-odd pages must not hold up shutdown, and a plan left half applied is harmless
        # when every connection stops.
        while True:
            self._universe_wake.clear()
            if not await self._pause_unless_woken(self._universe_delay_ns() / NS_PER_S):
                return
            if not await self._until_stopped(self._universe_step):
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
                bus.publish(
                    _catalog(self._universe, ended=self._ended, moved_close_ts=self._moved_close_ts)
                )
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

    async def _pause_unless_woken(self, seconds: float) -> bool:
        """Sleep like :meth:`_pause`, but end early once a lifecycle event wakes the universe loop.

        Returns:
            ``False`` if a stop came first; ``True`` once the time passed or the loop was woken,
            at once when ``seconds`` is not positive.
        """
        if seconds <= 0:
            return not self._stop_requested.is_set()

        async def sleep_until_woken() -> None:
            sleeping = asyncio.ensure_future(self._sleep(seconds))
            waking = asyncio.ensure_future(self._universe_wake.wait())
            try:
                await asyncio.wait((sleeping, waking), return_when=asyncio.FIRST_COMPLETED)
            finally:
                await _cancel(waking)
                await _cancel(sleeping)
            if not sleeping.cancelled():
                sleeping.result()

        slept = await self._until_stopped(sleep_until_woken)
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

    def _universe_delay_ns(self) -> int:
        """Nanoseconds until the first universe work is due; zero when some is due now.

        The work is the full refresh; the tick that removes a planned market, due
        :data:`CLOSE_TICK_DELAY_S` after its close time, or at once when a lifecycle event
        reported it determined or settled; and, once a full refresh has succeeded, a pending
        targeted re-listing, when both its debounce and the minimum interval allow.
        """
        now_mono_ns = int(self._clock.mono_ns())
        delays = [self._refresh_due_ns - now_mono_ns]
        if self._universe is not None:
            if any(ticker in self._conn_of for ticker in self._ended):
                delays.append(0)
            planned = self._as_last_reported(self._universe.markets)
            close_ts = min((m.close_ts for m in planned if m.close_ts is not None), default=None)
            if close_ts is not None:
                close_tick_ns = (close_ts + CLOSE_TICK_DELAY_S) * NS_PER_S
                delays.append(close_tick_ns - int(self._clock.wall_ns()))
            if self._relists:
                due_ns = min(pending.due_ns for pending in self._relists.values())
                delays.append(max(due_ns, self._relist_allowed_ns) - now_mono_ns)
        return max(0, min(delays))

    async def _universe_step(self) -> None:
        """Do the universe work due now: the full refresh, or the close tick and a re-listing."""
        if int(self._clock.mono_ns()) >= self._refresh_due_ns:
            await self._refresh_universe_or_log()
            return
        previous = self._universe
        if previous is None:
            return
        current = await self._remove_closed_markets(previous)
        now_ns = int(self._clock.mono_ns())
        due = [
            group.name
            for group in self._config.universe.groups
            if (pending := self._relists.get(group.name)) is not None and pending.due_ns <= now_ns
        ]
        if due and now_ns >= self._relist_allowed_ns:
            await self._relist_or_log(current, due)

    async def _refresh_universe_or_log(self) -> None:
        """Refresh the universe and set when the next full refresh is due.

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
            self._refresh_due_ns = int(self._clock.mono_ns()) + round(retry_in_s * NS_PER_S)
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
        refresh_ns = self._config.universe_refresh_s * NS_PER_S
        self._refresh_due_ns = int(self._clock.mono_ns()) + refresh_ns

    async def _refresh_universe(self) -> None:
        """List every open market, select the universe, and apply it.

        Series categories are read first when a category group needs them (ADR 0028). The
        listing can predate a lifecycle event, so markets reported determined or settled are left
        out and moved close times replace the listed ones; a market stays reported only while a
        listing still shows it open. The refresh covers every targeted re-listing requested
        before it started.

        Raises:
            KalshiError: If a listing page cannot be fetched.
            WireError: If a listing page does not decode.
        """
        started_ns = int(self._clock.mono_ns())
        if not self._config.universe.groups:
            self._log.warning("no universe groups are configured; no market will be recorded")
        listing = await self._list_markets()
        categories = await self._series_categories()
        now_wall_ns = int(self._clock.wall_ns())
        decision = select(
            self._as_last_reported(listing.markets),
            self._config.universe,
            now_ts=now_wall_ns // NS_PER_S,
            categories=categories,
        )
        await self._apply_universe(decision)
        self._ended.intersection_update(market.ticker for market in listing.markets)
        self._forget_relists(list(self._relists), started_ns=started_ns)
        self._follow_up_unpriced(
            decision, [group.name for group in self._config.universe.groups], started_ns=started_ns
        )
        self._log.info(
            "universe refreshed",
            extra={
                "listed": len(listing.markets),
                "truncated": listing.truncated,
                "selected": len(decision.l2_tickers),
                "showcase": len(decision.showcase),
                "dropped_for_cap": decision.dropped_for_cap,
                "reason_counts": dict(decision.reason_counts),
                "groups": _group_counts(decision),
                "book_groups": len(self._plan.groups),
            },
        )

    async def _remove_closed_markets(self, previous: UniverseDecision) -> UniverseDecision:
        """Take the planned markets that closed out of the plan, without a listing (ADR 0029).

        A market has closed once its close time, as last moved, has passed, or once a lifecycle
        event reported it determined or settled. Every group is pinned to the markets it
        admitted, so nothing takes a closed market's place here: a series group that lost one is
        due a targeted re-listing at once, and a category group waits for the next full refresh.

        Args:
            previous: The decision the plan follows now.

        Returns:
            The decision the plan follows afterwards: ``previous`` when no market closed.
        """
        now_ts = int(self._clock.wall_ns()) // NS_PER_S
        remaining = self._as_last_reported(previous.markets)
        still_open = {m.ticker for m in remaining if m.close_ts is None or m.close_ts > now_ts}
        closed = sorted(previous.l2_tickers - still_open)
        if not closed:
            return previous
        emptied = {previous.group_of[ticker] for ticker in closed}
        relisting = [
            group.name
            for group in self._config.universe.groups
            if group.series is not None and group.name in emptied
        ]
        now_ns = int(self._clock.mono_ns())
        for name in relisting:
            self._request_relist(name, due_ns=now_ns, now_ns=now_ns)
        decision = select(
            remaining, self._config.universe, now_ts=now_ts, pinned=_admitted_by_group(previous)
        )
        await self._apply_universe(decision)
        self._log.info(
            "closed markets removed from the universe",
            extra={
                "closed": closed,
                "selected": len(decision.l2_tickers),
                "groups": _group_counts(decision),
                "relisting": relisting,
                "book_groups": len(self._plan.groups),
            },
        )
        return decision

    async def _relist_or_log(self, previous: UniverseDecision, names: Sequence[str]) -> None:
        """Re-list some series groups; a failure keeps the plan and the groups' requests.

        The next re-listing may start :data:`RELIST_MIN_INTERVAL_S` after this one started,
        whatever its outcome, so a failure is retried then and a burst costs one re-listing.

        Args:
            previous: The decision the plan follows now.
            names: The series groups to re-list, in policy order.
        """
        started_ns = int(self._clock.mono_ns())
        self._relist_allowed_ns = started_ns + RELIST_MIN_INTERVAL_S * NS_PER_S
        try:
            await self._relist(previous, names, started_ns=started_ns)
        except (KalshiError, WireError) as exc:
            self._log.error(
                "targeted re-listing failed; keeping the current plan and retrying",
                extra={
                    "error": repr(exc),
                    "relisting": list(names),
                    "retry_in_s": RELIST_MIN_INTERVAL_S,
                },
            )

    async def _relist(
        self, previous: UniverseDecision, names: Sequence[str], *, started_ns: int
    ) -> None:
        """List the series of some series groups again, re-apply those groups, and replan.

        The listing replaces what was known of the markets of every series it lists. Every other
        group is pinned to the markets it admitted, so a category group gains nothing here.

        Args:
            previous: The decision the plan follows now.
            names: The series groups to re-list, in policy order.
            started_ns: Monotonic time the re-listing started, which covers earlier requests.

        Raises:
            KalshiError: If a listing page cannot be fetched.
            WireError: If a listing page does not decode.
        """
        policy = self._config.universe
        relisted = frozenset(names)
        series = tuple(
            dict.fromkeys(
                name
                for group in policy.groups
                if group.name in relisted
                for name in group.series or ()
            )
        )
        listing = await list_series_markets(
            self._rest, series, exclude_mve=policy.exclude_mve, max_pages=RELIST_MAX_PAGES
        )
        unlisted = [market for market in previous.markets if market.series_ticker not in series]
        decision = select(
            self._as_last_reported([*listing.markets, *unlisted]),
            policy,
            now_ts=int(self._clock.wall_ns()) // NS_PER_S,
            pinned={
                name: tickers
                for name, tickers in _admitted_by_group(previous).items()
                if name not in relisted
            },
        )
        await self._apply_universe(decision)
        self._forget_relists(names, started_ns=started_ns)
        self._follow_up_unpriced(decision, names, started_ns=started_ns)
        self._log.info(
            "universe groups re-listed",
            extra={
                "relisted": list(names),
                "series": list(series),
                "listed": len(listing.markets),
                "truncated": listing.truncated,
                "unreadable": listing.unreadable,
                "first_error": listing.first_error,
                "added": sorted(decision.l2_tickers - previous.l2_tickers),
                "removed": sorted(previous.l2_tickers - decision.l2_tickers),
                "selected": len(decision.l2_tickers),
                "groups": _group_counts(decision),
                "book_groups": len(self._plan.groups),
            },
        )

    async def _apply_universe(self, decision: UniverseDecision) -> None:
        """Replan for a decision, with the previous plan, and hand each connection its group.

        Book connections get their planner groups; the ticker connection gets every market of the
        plan, and the latest-ticker table drops the markets it no longer carries. The book
        connections carry every selected market, because a selection never exceeds
        ``max_l2_markets`` and the configuration guarantees room for that many. Moved close times
        are kept only for planned markets, and near-price follow-ups only for events still
        admitted by their group.

        Args:
            decision: The universe to record from now on.
        """
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
        self._plan = desired
        self._universe = decision
        self._conn_of = {
            ticker: FIRST_BOOK_CONN_ID + group.conn_id
            for group in desired.groups
            for ticker in group.tickers
        }
        self._moved_close_ts = {
            ticker: close_ts
            for ticker, close_ts in self._moved_close_ts.items()
            if ticker in self._conn_of
        }
        admitted = _admitted_events(decision)
        for key in [key for key in self._follow_ups if key not in admitted]:
            follow_up = self._follow_ups.pop(key)
            if not follow_up.gave_up:
                self._log_follow_ups_ended(key, follow_up, outcome="left_plan")

    def _note_lifecycle(self, event: MarketEvent) -> None:
        """Note what a lifecycle event means for the universe, and wake its loop (ADR 0029).

        A planned market reported determined or settled is due for removal at once, and a planned
        market's moved close time replaces its listed one. A market created or activated in a
        series that a series group names makes that group due a targeted re-listing
        :data:`RELIST_DEBOUNCE_S` after the first such event. Anything else is ignored. It runs
        as the control connection's event consumer, so it never awaits, and nothing in it raises.

        Args:
            event: An event the control connection decoded.
        """
        if not isinstance(event, Lifecycle):
            return
        planned = event.ticker in self._conn_of
        groups = self._series_groups.get(series_of(event.ticker), ())
        if event.event_type in _ENDED and planned:
            self._ended.add(event.ticker)
        elif (
            event.event_type == LIFECYCLE_CLOSE_DATE_UPDATED
            and planned
            and event.close_ts is not None
        ):
            self._moved_close_ts[event.ticker] = event.close_ts
        elif event.event_type in _OPENED and groups:
            now_ns = int(self._clock.mono_ns())
            for name in groups:
                self._request_relist(
                    name, due_ns=now_ns + RELIST_DEBOUNCE_S * NS_PER_S, now_ns=now_ns
                )
        else:
            return
        self._universe_wake.set()

    def _request_relist(self, name: str, *, due_ns: int, now_ns: int) -> None:
        """Ask for a targeted re-listing of a series group, keeping an earlier due time."""
        pending = self._relists.get(name)
        self._relists[name] = _PendingRelist(
            due_ns=due_ns if pending is None else min(pending.due_ns, due_ns),
            requested_ns=now_ns,
        )

    def _forget_relists(self, names: Iterable[str], *, started_ns: int) -> None:
        """Drop the requests of groups that a listing started at ``started_ns`` covered.

        A request made while the listing ran stays, because the listing may have missed what
        it was made for.
        """
        for name in names:
            pending = self._relists.get(name)
            if pending is not None and pending.requested_ns <= started_ns:
                del self._relists[name]

    def _follow_up_unpriced(
        self, decision: UniverseDecision, names: Collection[str], *, started_ns: int
    ) -> None:
        """List a near-price series group again while an event it admitted is not all priced.

        A new event is often listed before its first quotes, when near-price order can only fall
        back to volume and ticker (ADR 0029). For each group of ``names`` that selects by series
        with ``market_order = "near_price"``, an admitted event with an admitted market that has
        no :func:`yes_mid` makes the group due a targeted re-listing :data:`RELIST_MIN_INTERVAL_S`
        after this listing started, so the minimum interval still holds, at most
        :data:`NEAR_PRICE_FOLLOW_UPS` times for that group and event. An event whose admitted
        markets are all priced ends its follow-ups. Never awaits and never raises.

        Args:
            decision: The decision the listing produced, already applied.
            names: The groups the listing covered; a pinned group was not listed.
            started_ns: Monotonic time the listing started.
        """
        followed = {
            group.name
            for group in self._config.universe.groups
            if group.name in names
            and group.series is not None
            and group.market_order == MARKET_ORDER_NEAR_PRICE
        }
        summaries = {market.ticker: market for market in decision.markets}
        due_ns = started_ns + RELIST_MIN_INTERVAL_S * NS_PER_S
        for key, tickers in _admitted_events(decision).items():
            if key[0] in followed:
                unpriced = [ticker for ticker in tickers if yes_mid(summaries[ticker]) is None]
                self._follow_up_event(key, unpriced, due_ns=due_ns)

    def _follow_up_event(self, key: tuple[str, str], unpriced: list[str], *, due_ns: int) -> None:
        """Request, end, or give up the follow-ups of one group's event after a listing.

        Args:
            key: The group's name and the event ticker.
            unpriced: The event's admitted markets without a YES mid, in admission order.
            due_ns: Monotonic time from which a follow-up re-listing may run.
        """
        held = self._follow_ups.get(key)
        if not unpriced:
            if held is not None:
                del self._follow_ups[key]
                if not held.gave_up:
                    self._log_follow_ups_ended(key, held, outcome="priced")
            return
        if held is not None and held.gave_up:
            return
        attempts = 0 if held is None else held.attempts
        if attempts >= NEAR_PRICE_FOLLOW_UPS:
            gave_up = _FollowUp(attempts=attempts, gave_up=True)
            self._follow_ups[key] = gave_up
            self._follow_ups_given_up += 1
            self._log_follow_ups_ended(key, gave_up, outcome="gave_up", unpriced=unpriced)
            return
        self._follow_ups[key] = _FollowUp(attempts=attempts + 1, gave_up=False)
        now_ns = int(self._clock.mono_ns())
        self._request_relist(key[0], due_ns=due_ns, now_ns=now_ns)
        self._log.info(
            "near-price follow-up re-listing requested",
            extra={
                "group": key[0],
                "event": key[1],
                "attempt": attempts + 1,
                "max_attempts": NEAR_PRICE_FOLLOW_UPS,
                "unpriced": unpriced,
                "due_in_s": max(0, due_ns - now_ns) / NS_PER_S,
            },
        )

    def _log_follow_ups_ended(
        self,
        key: tuple[str, str],
        follow_up: _FollowUp,
        *,
        outcome: str,
        unpriced: Sequence[str] = (),
    ) -> None:
        """Log how an event's near-price follow-ups ended: priced, left the plan, or gave up.

        A give-up also carries the markets still unpriced and the events given up on since start.
        """
        extra: dict[str, object] = {
            "group": key[0],
            "event": key[1],
            "attempts": follow_up.attempts,
            "outcome": outcome,
        }
        if follow_up.gave_up:
            extra |= {"unpriced": list(unpriced), "given_up_total": self._follow_ups_given_up}
        self._log.info("near-price follow-ups ended", extra=extra)

    def _as_last_reported(self, markets: Iterable[MarketSummary]) -> list[MarketSummary]:
        """Markets as lifecycle events last reported them.

        Markets reported determined or settled are left out, and moved close times replace the
        listed ones.
        """
        moved = self._moved_close_ts
        return [
            msgspec.structs.replace(market, close_ts=moved[market.ticker])
            if market.ticker in moved
            else market
            for market in markets
            if market.ticker not in self._ended
        ]

    async def _list_markets(self) -> MarketListing:
        """Page through the open markets, logging a listing that skipped markets or was cut short.

        Returns:
            The listing.

        Raises:
            KalshiError: If a page cannot be fetched.
            WireError: If a page does not decode.
        """
        listing = await list_open_markets(
            self._rest,
            exclude_mve=self._config.universe.exclude_mve,
            max_pages=self._config.max_market_pages,
        )
        if listing.unreadable:
            self._log.warning(
                "markets skipped because they did not convert",
                extra={"skipped": listing.unreadable, "first_error": listing.first_error},
            )
        if listing.truncated:
            self._log.warning(
                "market listing cut short by the page cap; the universe is partial",
                extra={"max_market_pages": self._config.max_market_pages},
            )
        return listing

    async def _series_categories(self) -> Mapping[str, str]:
        """The category of each series for category groups; nothing when there is none.

        Until a fetch succeeds the map is empty, category groups admit nothing, and every
        refresh says so.
        """
        if self._categories is None:
            return _NO_CATEGORIES
        categories = await self._categories.current()
        if not self._categories.known:
            self._log.warning(
                "series categories unknown; category groups admit nothing",
                extra={"categories": list(self._categories.categories)},
            )
        return categories

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


def _catalog(
    decision: UniverseDecision, *, ended: Collection[str], moved_close_ts: Mapping[str, int]
) -> MarketCatalog:
    """The bus catalog of a universe decision: one entry per recorded market, in ticker order.

    Lifecycle events noted since the decision count at once (ADR 0029): a market reported
    determined or settled is left out, and a moved close time replaces the listed one, so a
    consumer that takes each catalog whole never shows a market the recorder knows has closed.

    Args:
        decision: The latest universe decision.
        ended: Markets reported determined or settled.
        moved_close_ts: Close times lifecycle events moved, by ticker.
    """
    return MarketCatalog(
        markets=tuple(
            CatalogEntry(
                ticker=market.ticker,
                series_ticker=market.series_ticker,
                event_ticker=market.event_ticker,
                volume_24h=market.volume_24h,
                close_ts=moved_close_ts.get(market.ticker, market.close_ts),
                showcase=market.ticker in decision.showcase,
            )
            for market in decision.markets
            if market.ticker not in ended
        )
    )


def _admitted_events(decision: UniverseDecision) -> dict[tuple[str, str], list[str]]:
    """Every group's admitted markets by group name and event ticker, in admission order."""
    event_of = {market.ticker: market.event_ticker for market in decision.markets}
    events: dict[tuple[str, str], list[str]] = {}
    for selection in decision.groups:
        for ticker in selection.tickers:
            events.setdefault((selection.name, event_of[ticker]), []).append(ticker)
    return events


def _admitted_by_group(decision: UniverseDecision) -> dict[str, tuple[str, ...]]:
    """What each group admitted, in admission order: the pins of a decision between listings."""
    return {group.name: group.tickers for group in decision.groups}


def _series_groups_by_series(policy: UniversePolicy) -> dict[str, tuple[str, ...]]:
    """The series groups that name each series, in policy order, by series ticker."""
    names: dict[str, list[str]] = {}
    for group in policy.groups:
        for series in group.series or ():
            names.setdefault(series, []).append(group.name)
    return {series: tuple(groups) for series, groups in names.items()}


def _group_counts(decision: UniverseDecision) -> dict[str, dict[str, int]]:
    """What each universe group admitted, by group name, for the universe log line (ADR 0028)."""
    return {
        group.name: {
            "admitted": len(group.tickers),
            "events": group.events,
            "skipped_for_budget": group.skipped_for_budget,
        }
        for group in decision.groups
    }


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
