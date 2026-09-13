"""Supervise one Kalshi WebSocket connection: record it, decode it, resynchronize it, revive it.

Responsibility: own the life of one WebSocket connection for the recorder
(docs/ARCHITECTURE.md 7.1 and 9, docs/INTERFACES.md 8.4). Every inbound frame is handed to
the segment sink before anything parses it (ADR 0001); then its envelope is read and its
sequence number checked per ``sid``; only then is the payload decoded into books and
events. Kalshi keeps one subscription per channel per connection and merges every further
``subscribe`` into it (ADR 0020), so a connection subscribes its group channels once, for its
whole market set, and changes membership with ``update_subscription``. Books are kept only
for ``orderbook_delta``; a group on ``ticker`` alone, as on the live-only ticker connection
(ADR 0027), yields events and no books. No command names more than
``max_markets_per_command`` markets: a larger group is subscribed with its first batch, and
the rest join that subscription once its ``sid``s are known. An ``ok`` reply to a
``subscribe`` is such a merge and is bound to the existing ``sid``; a subscribe left without
a reply for any channel past its deadline fails the connection. A sequence gap on a book
subscription is written into the tape, every book of the connection is marked stale, and a
``get_snapshot`` names all of the connection's markets. A lost connection is written into
the tape, every book is marked stale, the segment is rotated, and after a jittered
exponential backoff the connection is rebuilt and resubscribed from the supervisor's own
group, because ``sid``s do not survive a connection. For the auditor, :meth:`open_tap` opens
a :class:`tape.recorder.tap.LiveBookTap` over some markets; every snapshot and delta applied
to a tapped market's book is handed to it, and a closed tap is forgotten.

Invariants: per frame the order is sink, envelope, sequence check, everything else, so a
decoder can never lose a frame; a book is touched only by a frame on an orderbook
subscription of the current connection for a market in the connection's market set, and it
leaves the stale state only through such a snapshot; the subscription table holds only
``sid``s the current connection assigned; a membership change is sent to every ``sid`` of the
group, and never while a subscribe still awaits a reply; at most one subscribe awaits replies
at a time for the group; no command names more than ``max_markets_per_command`` markets; no
subscribe awaits a reply longer than ``subscribe_timeout_ns`` on a live connection; the
reconnect loop ends on :meth:`ConnectionSupervisor.stop` or after
``max_consecutive_failures``; and the module sleeps, draws randomness, and reads time only
through what was injected.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final, assert_never

import msgspec

from tape.book import Book
from tape.client.ws import (
    Command,
    RawFrame,
    SubscribeCommand,
    UpdateSubscriptionCommand,
    WsSession,
    encode_command,
)
from tape.errors import (
    BookInvariantError,
    FixedPointError,
    KalshiTransportError,
    WireError,
    WsClosedError,
    WsProtocolError,
)
from tape.events import GapEvent, MarketEvent, Receipt
from tape.recorder.gaps import Duplicate, Gap, GapTracker
from tape.recorder.planner import (
    AddGroup,
    AddMarkets,
    Group,
    Plan,
    PlanChange,
    RemoveGroup,
    RemoveMarkets,
    diff,
    split_change,
    to_commands,
)
from tape.recorder.tap import BookChange, LiveBookTap
from tape.recorder.writer import SegmentSink
from tape.segment import Record, RecordKind, SubscriptionInfo
from tape.timeutil import NS_PER_S, Clock
from tape.wire import (
    Envelope,
    ErrorMsg,
    MarketLifecycleV2Msg,
    OkMsg,
    OrderbookDeltaMsg,
    OrderbookSnapshotMsg,
    SubscribedMsg,
    TickerMsg,
    TradeMsg,
    decode_envelope,
    decode_msg,
    to_book_delta,
    to_book_snapshot,
    to_lifecycle,
    to_ticker,
    to_trade,
)

__all__ = [
    "CAPACITY_ERROR_CODES",
    "DEFAULT_BACKOFF_INITIAL_NS",
    "DEFAULT_BACKOFF_MAX_NS",
    "DEFAULT_MAX_MARKETS_PER_COMMAND",
    "DEFAULT_SUBSCRIBE_TIMEOUT_NS",
    "FIREHOSE_GROUP_ID",
    "MAX_RECENT_ERRORS",
    "ORDERBOOK_CHANNEL",
    "ConnectionSupervisor",
    "SupervisorConfig",
    "SupervisorStats",
    "backoff_delay_s",
]

ORDERBOOK_CHANNEL: Final = "orderbook_delta"
"""The one channel whose subscriptions maintain books and answer ``get_snapshot``."""

CAPACITY_ERROR_CODES: Final = frozenset({25, 26, 27})
"""Buffer overflow, market limit, command rate limit: the recorder may reshard on these."""

FIREHOSE_GROUP_ID: Final = "*"
"""``group_id`` recorded for the subscription of ``SupervisorConfig.firehose_channels``."""

DEFAULT_BACKOFF_INITIAL_NS: Final = NS_PER_S // 2
DEFAULT_BACKOFF_MAX_NS: Final = 30 * NS_PER_S
"""The reconnect cap from docs/ARCHITECTURE.md 7.1."""

DEFAULT_SUBSCRIBE_TIMEOUT_NS: Final = 10 * NS_PER_S
"""How long a subscribe may wait for a reply on every channel before the connection fails."""

DEFAULT_MAX_MARKETS_PER_COMMAND: Final = 500
"""Most markets one subscribe or ``update_subscription`` names: the order-book group cap."""

MAX_RECENT_ERRORS: Final = 64
"""Error frames kept in :attr:`ConnectionSupervisor.last_errors`; older ones are only counted."""

_MAX_BACKOFF_DOUBLINGS: Final = 62
_SNAPSHOT_TYPE: Final = "orderbook_snapshot"
_STOPPED: Final = "stopped"


def backoff_delay_s(
    consecutive_failures: int, *, initial_ns: int, max_ns: int, jitter: float
) -> float:
    """Return how long to wait before the next connection attempt.

    The delay is ``min(max_ns, initial_ns * 2**consecutive_failures)`` scaled into its upper
    half by ``jitter``, so reconnecting connections spread out without any of them waiting
    less than half the nominal delay.

    Args:
        consecutive_failures: Failures in a row before this one, starting at zero.
        initial_ns: Nominal delay after the first failure.
        max_ns: Cap on the nominal delay.
        jitter: A draw from ``[0, 1)``.

    Returns:
        Seconds to wait, in ``[nominal / 2, nominal)``.

    Raises:
        ValueError: If ``consecutive_failures`` is negative, ``jitter`` is outside
            ``[0, 1)``, ``initial_ns`` is not positive, or ``max_ns < initial_ns``.
    """
    if consecutive_failures < 0:
        raise ValueError(f"consecutive_failures must be non-negative, got {consecutive_failures}")
    if not 0.0 <= jitter < 1.0:
        raise ValueError(f"jitter must be in [0, 1), got {jitter}")
    if initial_ns <= 0 or max_ns < initial_ns:
        raise ValueError(f"need 0 < initial_ns <= max_ns, got {initial_ns} and {max_ns}")
    nominal_ns = min(max_ns, initial_ns << min(consecutive_failures, _MAX_BACKOFF_DOUBLINGS))
    return nominal_ns * (0.5 + jitter / 2) / NS_PER_S


class SupervisorConfig(msgspec.Struct, frozen=True, kw_only=True):
    """What one connection carries and how hard it tries to stay up.

    Attributes:
        conn_id: Connection id; the group given to the supervisor must carry it.
        group_channels: Channels the connection's group subscribes, one ``sid`` each. Books
            are kept for ``orderbook_delta`` only, so ``("ticker",)`` yields ticker events
            for the group's markets and no books.
        firehose_channels: Channels subscribed once per connection with no market filter,
            for example ``("market_lifecycle_v2",)``, which accepts none. A :class:`Group`
            cannot express "every market", so these live outside the group.
        use_yes_price: Request YES-leg prices for both book sides and convert accordingly
            (ADR 0006). Sent only when the group channels include ``orderbook_delta``.
        persist: Write frames, commands, gaps, and connection events to the sink. When
            false the connection is live-only: decoded and published, never written.
        max_markets_per_command: Most markets one subscribe or ``update_subscription``
            names; a larger change is sent as several commands on the same subscription.
        backoff_initial_ns: Nominal delay after the first consecutive failure.
        backoff_max_ns: Cap on the nominal delay.
        max_consecutive_failures: Failures in a row tolerated before :meth:`run` raises;
            ``None`` retries until stopped.
        subscribe_timeout_ns: How long a subscribe may wait for a ``subscribed`` or ``ok``
            reply on every channel it names before the connection is failed.

    Raises:
        ValueError: On a negative ``conn_id``, no channels at all, a channel that is both a
            group and a firehose channel, a non-positive market limit, initial backoff, or
            subscribe timeout, a backoff cap below the initial backoff, or a negative
            failure limit.
    """

    conn_id: int
    group_channels: tuple[str, ...] = (ORDERBOOK_CHANNEL, "trade")
    firehose_channels: tuple[str, ...] = ()
    use_yes_price: bool = True
    persist: bool = True
    max_markets_per_command: int = DEFAULT_MAX_MARKETS_PER_COMMAND
    backoff_initial_ns: int = DEFAULT_BACKOFF_INITIAL_NS
    backoff_max_ns: int = DEFAULT_BACKOFF_MAX_NS
    max_consecutive_failures: int | None = None
    subscribe_timeout_ns: int = DEFAULT_SUBSCRIBE_TIMEOUT_NS

    def __post_init__(self) -> None:
        if self.conn_id < 0:
            raise ValueError(f"conn_id must be non-negative, got {self.conn_id}")
        if not self.group_channels and not self.firehose_channels:
            raise ValueError("a connection needs group_channels or firehose_channels")
        shared = set(self.group_channels) & set(self.firehose_channels)
        if shared:
            # Kalshi would merge the filtered and unfiltered subscriptions into one (ADR 0020).
            raise ValueError(
                f"channels {sorted(shared)} are both group and firehose channels; a connection "
                f"holds one subscription per channel"
            )
        if self.max_markets_per_command <= 0:
            raise ValueError(
                f"max_markets_per_command must be positive, got {self.max_markets_per_command}"
            )
        if self.backoff_initial_ns <= 0 or self.backoff_max_ns < self.backoff_initial_ns:
            raise ValueError(
                f"need 0 < backoff_initial_ns <= backoff_max_ns, got "
                f"{self.backoff_initial_ns} and {self.backoff_max_ns}"
            )
        limit = self.max_consecutive_failures
        if limit is not None and limit < 0:
            raise ValueError(f"max_consecutive_failures must be non-negative, got {limit}")
        if self.subscribe_timeout_ns <= 0:
            raise ValueError(
                f"subscribe_timeout_ns must be positive, got {self.subscribe_timeout_ns}"
            )


class SupervisorStats(msgspec.Struct, frozen=True, kw_only=True):
    """Counters for one supervisor, across every connection it has made.

    Attributes:
        frames: Frames received.
        records_not_persisted: Records the sink refused (its own stats say why).
        decode_errors: Frames whose envelope or payload could not be decoded.
        gaps: Sequence gaps observed.
        duplicates: Sequence numbers that did not advance.
        reconnects: Connection attempts made after a failure.
        snapshots_requested: ``get_snapshot`` commands sent.
        stale_books: Books currently awaiting a snapshot.
        book_errors: Snapshots or deltas that broke a book invariant.
        callback_errors: Exceptions raised by ``on_event``.
        errors_by_code: Error frames received, by code.
    """

    frames: int
    records_not_persisted: int
    decode_errors: int
    gaps: int
    duplicates: int
    reconnects: int
    snapshots_requested: int
    stale_books: int
    book_errors: int
    callback_errors: int
    errors_by_code: dict[int, int]


@dataclass(slots=True)
class _PendingSubscribe:
    """A ``subscribe`` that some of its channels have not answered yet.

    Attributes:
        group_id: Group the subscribe was sent for.
        channels: Channels still awaiting a ``subscribed`` or ``ok`` reply.
        expiry: Task that fails the connection if the replies do not arrive in time.
    """

    group_id: str
    channels: set[str]
    expiry: asyncio.Task[None]


class ConnectionSupervisor:
    """Keeps one WebSocket connection recorded, decoded, and subscribed. See the module docstring.

    Not thread-safe; every method runs on one event loop. :attr:`subscriptions` is the one
    exception, published as an immutable tuple so a segment header factory on the writer
    thread may read it.

    Args:
        config: What the connection carries and its retry policy.
        session_factory: Builds a new, unconnected session for each attempt; sessions are
            single-use.
        clock: Stamps commands and connection events.
        sleep: Waits the given seconds between attempts.
        jitter: Returns a draw from ``[0, 1)`` for each backoff.
        sink: Where records go. Required exactly when ``config.persist`` is true.
        on_event: Receives every trade, ticker, lifecycle event, and gap, and every snapshot
            and delta that was applied to a book. Exceptions it raises are logged and
            counted, never propagated, because nothing downstream may stop the recorder.
        deadline_sleep: Waits out ``config.subscribe_timeout_ns``, given in seconds. Kept
            apart from ``sleep`` so reply deadlines never interleave with reconnect backoff.
        logger: Destination for logs; defaults to this module's logger.

    Raises:
        ValueError: If a sink is missing for a persisting connection or given to a
            live-only one.
    """

    def __init__(
        self,
        config: SupervisorConfig,
        *,
        session_factory: Callable[[], WsSession],
        clock: Clock,
        sleep: Callable[[float], Awaitable[None]],
        jitter: Callable[[], float],
        sink: SegmentSink | None = None,
        on_event: Callable[[MarketEvent], None] | None = None,
        deadline_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        logger: logging.Logger | None = None,
    ) -> None:
        if config.persist and sink is None:
            raise ValueError(f"connection {config.conn_id} persists but was given no sink")
        if not config.persist and sink is not None:
            raise ValueError(f"connection {config.conn_id} is live-only but was given a sink")
        self._config = config
        self._session_factory = session_factory
        self._clock = clock
        self._sleep = sleep
        self._jitter = jitter
        self._sink = sink
        self._on_event = on_event
        self._deadline_sleep = deadline_sleep
        self._log = logger if logger is not None else logging.getLogger(__name__)
        # Survives reconnects: the desired group and the books it feeds.
        self._desired: Group | None = None
        self._books: dict[str, Book] = {}
        # Open taps by tapped ticker; empty unless the auditor is inside a window, so an applied
        # change costs one failed membership check.
        self._taps: dict[str, list[LiveBookTap]] = {}
        # Rebuilt for every connection, because sids and sequence numbers are connection-scoped.
        self._session: WsSession | None = None
        self._reply_failure: asyncio.Future[WsClosedError] | None = None
        self._tracker = GapTracker()
        self._applied: Group | None = None
        self._subscriptions: dict[int, SubscriptionInfo] = {}
        self._subscription_infos: tuple[SubscriptionInfo, ...] = ()
        self._channel_of_sid: dict[int, str] = {}
        self._pending: dict[int, _PendingSubscribe] = {}
        self._resyncing: dict[int, set[str]] = {}
        self._reconcile_deferred = False
        self._firehose_requested = False
        self._frames_this_connection = 0
        # Serializes everything that sends commands or matches their responses.
        self._command_lock = asyncio.Lock()
        self._stop_requested = asyncio.Event()
        self._exited = asyncio.Event()
        self._started = False
        self._consecutive_failures = 0
        self._frames = 0
        self._records_not_persisted = 0
        self._decode_errors = 0
        self._gaps = 0
        self._duplicates = 0
        self._reconnects = 0
        self._snapshots_requested = 0
        self._book_errors = 0
        self._callback_errors = 0
        self._errors_by_code: Counter[int] = Counter()
        self._last_errors: deque[WsProtocolError] = deque(maxlen=MAX_RECENT_ERRORS)

    # ------------------------------------------------------------------- read-only views

    @property
    def config(self) -> SupervisorConfig:
        """The configuration this supervisor was built with."""
        return self._config

    @property
    def books(self) -> Mapping[str, Book]:
        """Books by ticker for every market with a snapshot; check ``is_stale()`` before use."""
        return MappingProxyType(self._books)

    @property
    def group(self) -> Group | None:
        """The desired group, applied now or on the next connection; ``None`` for no markets."""
        return self._desired

    @property
    def subscriptions(self) -> tuple[SubscriptionInfo, ...]:
        """Subscriptions of the current connection by ascending ``sid``. Safe from any thread."""
        return self._subscription_infos

    @property
    def last_errors(self) -> tuple[WsProtocolError, ...]:
        """The most recent error frames, oldest first, at most ``MAX_RECENT_ERRORS``."""
        return tuple(self._last_errors)

    @property
    def tapped_tickers(self) -> frozenset[str]:
        """Markets some open tap observes; empty when no tap is open."""
        return frozenset(self._taps)

    @property
    def stats(self) -> SupervisorStats:
        """Current counters; see :class:`SupervisorStats`."""
        return SupervisorStats(
            frames=self._frames,
            records_not_persisted=self._records_not_persisted,
            decode_errors=self._decode_errors,
            gaps=self._gaps,
            duplicates=self._duplicates,
            reconnects=self._reconnects,
            snapshots_requested=self._snapshots_requested,
            stale_books=sum(1 for book in self._books.values() if book.is_stale()),
            book_errors=self._book_errors,
            callback_errors=self._callback_errors,
            errors_by_code=dict(self._errors_by_code),
        )

    # ------------------------------------------------------------------------ control

    async def run(self) -> None:
        """Record the connection until :meth:`stop`, reconnecting whenever it is lost.

        Raises:
            WsClosedError: If the connection was lost more than
                ``max_consecutive_failures`` times in a row; the last loss is raised.
            KalshiTransportError: As above, when the last failure was a transport error.
            RuntimeError: If :meth:`run` was already called; a supervisor is single-use.
        """
        if self._started:
            raise RuntimeError(f"supervisor {self._config.conn_id} is single-use")
        self._started = True
        try:
            while not self._stop_requested.is_set():
                failure = await self._run_connection()
                if failure is None:
                    return
                await self._back_off(failure)
        finally:
            self._exited.set()

    async def stop(self) -> None:
        """Close the session and return once :meth:`run` has exited. Idempotent.

        Frames already received are still recorded before :meth:`run` returns.
        """
        self._stop_requested.set()
        session = self._session
        if session is not None:
            await session.close()
        if self._started:
            # Bounded: run() leaves its backoff at once, and a closed session ends the
            # frame stream after the frames already buffered.
            await self._exited.wait()

    async def set_group(self, group: Group | None) -> None:
        """Replace the connection's market set and move the live connection to it.

        The difference is sent as ``planner`` commands: a subscribe when the connection has
        no markets yet, ``update_subscription`` for a membership change, and an unsubscribe
        when ``group`` is ``None``. If the connection is down, or a subscribe is still
        waiting for its replies, it is applied as soon as that changes. If a command cannot
        be sent, the connection is dropped so that the reconnect resubscribes from the
        desired group rather than from a half-applied change.

        Args:
            group: Every market this connection should carry, or ``None`` for none.

        Raises:
            ValueError: If the group belongs to another connection, or is given to a
                connection without group channels.
        """
        if group is not None:
            if group.conn_id != self._config.conn_id:
                raise ValueError(
                    f"group {group.group_id} is for connection {group.conn_id}, "
                    f"not {self._config.conn_id}"
                )
            if not self._config.group_channels:
                raise ValueError(f"connection {self._config.conn_id} has no group channels")
        self._desired = group
        async with self._command_lock:
            session = self._session
            try:
                await self._reconcile_locked()
            except (WsClosedError, KalshiTransportError) as exc:
                self._log.warning(
                    "subscription change not sent; reconnecting",
                    extra={"conn_id": self._config.conn_id, "detail": str(exc)},
                )
                if session is not None:
                    await session.close()
            self._prune_books()

    def open_tap(self, tickers: Iterable[str], *, max_events: int) -> LiveBookTap:
        """Start observing every change this supervisor applies to some markets' books.

        The tap copies each market's book now and then receives, in order, every snapshot and
        delta applied to it, until :meth:`LiveBookTap.close` releases it. Opening and closing
        never await, so no frame is applied between copying a book and watching it.

        Args:
            tickers: Markets to observe; one with no fresh book gets a ``no_book`` window.
            max_events: Most changes held per market before its window faults.

        Returns:
            The open tap.

        Raises:
            ValueError: If ``max_events`` is not positive.
        """
        tap = LiveBookTap(
            tickers, books=self.books, max_events=max_events, on_close=self._release_tap
        )
        for ticker in tap.tickers:
            self._taps.setdefault(ticker, []).append(tap)
        return tap

    # ---------------------------------------------------------------- connection epochs

    async def _run_connection(self) -> WsClosedError | KalshiTransportError | None:
        """Connect, subscribe, and consume until the connection ends.

        Returns:
            The failure that ended the connection, or ``None`` if it ended because
            :meth:`stop` was called.
        """
        session = self._session_factory()
        async with self._command_lock:
            self._begin_connection(session)
        opened = False
        failure: WsClosedError | KalshiTransportError | None = None
        completed = False
        try:
            opened = await self._unless_stopped(session.connect())
            if opened:
                self._record_connection_event("open", "connected")
                async with self._command_lock:
                    await self._reconcile_locked()
                failure = await self._consume(session)
                if failure is None and not self._stop_requested.is_set():
                    failure = WsClosedError(
                        f"connection {self._config.conn_id} frame stream ended unrequested"
                    )
            completed = True
        except (WsClosedError, KalshiTransportError) as exc:
            failure = exc
            completed = True
        finally:
            if self._stop_requested.is_set():
                failure = None
            if not completed:
                detail = "supervisor failed"
            else:
                detail = _STOPPED if failure is None else str(failure)
            await self._end_connection(session, opened=opened, detail=detail)
        return failure

    async def _consume(self, session: WsSession) -> WsClosedError | None:
        """Handle frames until the stream ends or a subscribe outlives its reply deadline.

        Returns:
            The deadline failure, or ``None`` if the frame stream ended on its own.

        Raises:
            WsClosedError: If the frame stream failed.
            KalshiTransportError: If a command sent while handling a frame timed out.
        """
        reply_failure = self._reply_failure
        frames = asyncio.ensure_future(self._handle_frames(session))
        waiters: set[asyncio.Future[None] | asyncio.Future[WsClosedError]] = {frames}
        if reply_failure is not None:
            waiters.add(reply_failure)
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            await _cancel(frames)
        if not frames.cancelled() or reply_failure is None:
            frames.result()
            return None
        return reply_failure.result()

    async def _handle_frames(self, session: WsSession) -> None:
        async for frame in session.frames():
            await self._on_frame(frame)

    def _begin_connection(self, session: WsSession) -> None:
        """Adopt a new session with no connection-scoped state. Call with the lock held."""
        self._forget_connection()
        self._session = session
        self._reply_failure = asyncio.get_running_loop().create_future()

    def _forget_connection(self) -> None:
        """Drop everything scoped to a connection: sids, sequences, pending commands.

        Call with the lock held, so that a reconciliation in flight never sees the tables
        change under it.
        """
        self._session = None
        self._reply_failure = None
        self._tracker = GapTracker()
        self._applied = None
        self._subscriptions.clear()
        self._publish_subscriptions()
        self._channel_of_sid.clear()
        for pending in self._pending.values():
            pending.expiry.cancel()
        self._pending.clear()
        self._resyncing.clear()
        self._reconcile_deferred = False
        self._firehose_requested = False
        self._frames_this_connection = 0

    async def _end_connection(self, session: WsSession, *, opened: bool, detail: str) -> None:
        """Close the session and record why; an opened connection also stales and rotates."""
        await session.close()
        async with self._command_lock:
            # Cleared now rather than at the next connection, so nothing, a segment header
            # opened during the backoff included, reports sids that no longer exist.
            self._forget_connection()
        if opened:
            self._record_connection_event("close", detail)
            for book in self._books.values():
                book.mark_stale()
            if self._sink is not None:
                self._sink.rotate()
        elif detail != _STOPPED:
            self._record_connection_event("error", detail)

    async def _back_off(self, failure: WsClosedError | KalshiTransportError) -> None:
        """Wait before the next attempt, or give up once the failure limit is exceeded.

        Raises:
            WsClosedError: The failure, if the limit is exceeded.
            KalshiTransportError: The failure, if the limit is exceeded.
        """
        if self._frames_this_connection > 0:
            self._consecutive_failures = 0
        delay_s = backoff_delay_s(
            self._consecutive_failures,
            initial_ns=self._config.backoff_initial_ns,
            max_ns=self._config.backoff_max_ns,
            jitter=self._jitter(),
        )
        self._consecutive_failures += 1
        limit = self._config.max_consecutive_failures
        context = {
            "conn_id": self._config.conn_id,
            "detail": str(failure),
            "consecutive_failures": self._consecutive_failures,
        }
        if limit is not None and self._consecutive_failures > limit:
            self._log.error("connection abandoned after repeated failures", extra=context)
            raise failure
        self._log.warning("connection lost; reconnecting", extra={**context, "delay_s": delay_s})
        if await self._unless_stopped(self._sleep(delay_s)):
            self._reconnects += 1

    async def _unless_stopped(self, work: Awaitable[None]) -> bool:
        """Await ``work`` unless :meth:`stop` is called first, in which case cancel it.

        Returns:
            ``True`` if the work finished; ``False`` if it was cancelled by a stop.

        Raises:
            Exception: Whatever the work raised.
        """
        work_future = asyncio.ensure_future(work)
        stop_future = asyncio.ensure_future(self._stop_requested.wait())
        try:
            # Bounded without a deadline of its own: connecting has one, and the injected
            # sleep waits a finite delay.
            await asyncio.wait((work_future, stop_future), return_when=asyncio.FIRST_COMPLETED)
        finally:
            await _cancel(stop_future)
            await _cancel(work_future)
        if work_future.cancelled():
            return False
        work_future.result()
        return True

    # ----------------------------------------------------------------------- hot path

    async def _on_frame(self, frame: RawFrame) -> None:
        """Record, then check the sequence, then interpret one frame, in that order."""
        self._frames += 1
        self._frames_this_connection += 1
        self._put(RecordKind.FRAME, frame.recv_mono_ns, frame.recv_wall_ns, frame.payload)
        try:
            envelope = decode_envelope(frame.payload)
        except WireError as exc:
            self._decode_error(exc, None)
            return
        receipt = Receipt(
            conn_id=self._config.conn_id,
            recv_mono_ns=frame.recv_mono_ns,
            recv_wall_ns=frame.recv_wall_ns,
        )
        sid = envelope.sid
        if sid is not None:
            if envelope.type == _SNAPSHOT_TYPE and sid in self._resyncing:
                # Kalshi does not document whether a resnapshot continues or restarts the
                # sequence (docs/ARCHITECTURE.md 12). Taking the resnapshot as a new baseline
                # is right either way; trusting the old one would report every later message
                # of a restarted sequence as a duplicate and hide real gaps behind them.
                self._tracker.reset(sid)
            verdict = self._tracker.observe(sid, envelope.seq)
            if isinstance(verdict, Gap):
                await self._on_gap(sid, verdict, receipt)
            elif isinstance(verdict, Duplicate):
                await self._on_duplicate(sid, verdict)
        try:
            await self._dispatch(envelope, receipt)
        except (WireError, FixedPointError) as exc:
            self._decode_error(exc, envelope.type)

    async def _dispatch(self, envelope: Envelope, receipt: Receipt) -> None:
        """Decode a frame's payload and act on it by message type.

        Raises:
            WireError: If the payload does not match its struct.
            FixedPointError: If a price or count in it is malformed.
        """
        match envelope.type:
            case "orderbook_snapshot":
                self._on_snapshot(envelope, receipt)
            case "orderbook_delta":
                await self._on_delta(envelope, receipt)
            case "trade":
                self._emit(to_trade(decode_msg(envelope, TradeMsg), envelope, receipt))
            case "ticker":
                self._emit(to_ticker(decode_msg(envelope, TickerMsg), envelope, receipt))
            case "market_lifecycle_v2":
                lifecycle = decode_msg(envelope, MarketLifecycleV2Msg)
                self._emit(to_lifecycle(lifecycle, envelope, receipt))
            case "subscribed":
                await self._on_subscribed(envelope)
            case "ok":
                await self._on_ok(envelope)
            case "unsubscribed":
                if envelope.sid is not None:
                    self._tracker.forget(envelope.sid)
            case "error":
                await self._on_error(envelope)
            case _:
                # Types this supervisor does not interpret are already in the tape; there
                # is nothing to apply.
                return

    async def _on_gap(self, sid: int, gap: Gap, receipt: Receipt) -> None:
        """Record a gap and, on a book subscription, stale the connection and resnapshot it."""
        self._gaps += 1
        payload = msgspec.json.encode(
            {"sid": sid, "expected_seq": gap.expected, "got_seq": gap.got}
        )
        self._put(RecordKind.GAP, receipt.recv_mono_ns, receipt.recv_wall_ns, payload)
        info = self._subscriptions.get(sid)
        self._log.warning(
            "sequence gap",
            extra={
                "conn_id": self._config.conn_id,
                "sid": sid,
                "channel": None if info is None else info.channel,
                "expected_seq": gap.expected,
                "got_seq": gap.got,
            },
        )
        self._emit(GapEvent(receipt=receipt, sid=sid, expected_seq=gap.expected, got_seq=gap.got))
        # The hole may have swallowed a snapshot already requested, so ask for every market.
        await self._resync_connection(sid, ask_again=True)

    async def _on_duplicate(self, sid: int, duplicate: Duplicate) -> None:
        """Count a sequence number that did not advance, and resynchronize its books.

        Never fatal, and never trusted either: a replayed delta applied twice leaves a book
        wrong with nothing to show for it, so a book subscription is resnapshotted as for a
        gap (docs/ENGINEERING_STANDARDS.md 3.5). Nothing was lost, so markets already
        awaiting a snapshot are not asked for again, and a replayed burst sends one request.
        """
        self._duplicates += 1
        self._log.warning(
            "sequence did not advance",
            extra={
                "conn_id": self._config.conn_id,
                "sid": sid,
                "expected_seq": duplicate.expected,
                "got_seq": duplicate.got,
            },
        )
        await self._resync_connection(sid, ask_again=False)

    async def _resync_connection(self, sid: int, *, ask_again: bool) -> None:
        """Stale every book of the connection and request snapshots for all of its markets.

        The whole market set shares the one orderbook subscription (ADR 0020), so a sequence
        fault on it may have touched any of them. Only book subscriptions can be repaired:
        ``get_snapshot`` exists for the orderbook channel alone, and a missed trade or
        lifecycle message leaves no book wrong.

        Args:
            sid: Subscription whose sequence misbehaved.
            ask_again: Request markets already awaiting a snapshot as well.

        Raises:
            WsClosedError: If the request cannot be sent because the connection closed.
            KalshiTransportError: If the request times out.
        """
        info = self._subscriptions.get(sid)
        applied = self._applied
        if info is None or info.channel != ORDERBOOK_CHANNEL or applied is None:
            return
        for ticker in applied.tickers:
            book = self._books.get(ticker)
            if book is not None:
                book.mark_stale()
        if ask_again:
            self._resyncing.pop(sid, None)
        await self._request_snapshot(sid, applied.tickers)

    def _on_snapshot(self, envelope: Envelope, receipt: Receipt) -> None:
        """Replace a book with a snapshot and clear its stale flag."""
        snapshot = to_book_snapshot(
            decode_msg(envelope, OrderbookSnapshotMsg),
            envelope,
            receipt,
            use_yes_price=self._config.use_yes_price,
        )
        self._finish_resync(snapshot.sid, snapshot.ticker)
        book = self._book_for(snapshot.sid, snapshot.ticker, create=True)
        if book is None:
            return
        tapped = snapshot.ticker in self._taps
        # Read before applying: a snapshot is the only way out of the stale state, so it is
        # where a tap learns that its book went stale inside the window.
        was_stale = tapped and book.is_stale()
        try:
            book.apply_snapshot(snapshot.bids, snapshot.asks, ts_ms=snapshot.ts_ms)
        except BookInvariantError as exc:
            # Not re-requested here: an exchange that reports a crossed book would answer
            # the same way forever. The book stays stale until a gap or delta error asks.
            self._book_error(exc, snapshot.sid)
            return
        if tapped:
            self._record_in_taps(snapshot, was_stale=was_stale)
        self._emit(snapshot)

    async def _on_delta(self, envelope: Envelope, receipt: Receipt) -> None:
        """Apply a delta; a broken invariant stales the book and requests its snapshot."""
        delta = to_book_delta(
            decode_msg(envelope, OrderbookDeltaMsg),
            envelope,
            receipt,
            use_yes_price=self._config.use_yes_price,
        )
        book = self._book_for(delta.sid, delta.ticker, create=False)
        if book is None:
            return
        try:
            applied = book.apply_delta(delta.side, delta.price, delta.delta, ts_ms=delta.ts_ms)
        except BookInvariantError as exc:
            self._book_error(exc, delta.sid)
            await self._request_snapshot(delta.sid, (delta.ticker,))
            return
        if applied:
            if delta.ticker in self._taps:
                self._record_in_taps(delta, was_stale=False)
            self._emit(delta)

    def _book_for(self, sid: int, ticker: str, *, create: bool) -> Book | None:
        """Return the book a book message may touch, or ``None`` if it may touch none.

        A message may touch a book only if it arrived on a live orderbook subscription of
        this connection and names a market in the connection's market set, however that
        market joined it; anything else is a straggler from a subscription or market already
        retired, and applying it could revive a book no one updates.
        """
        info = self._subscriptions.get(sid)
        applied = self._applied
        if info is None or info.channel != ORDERBOOK_CHANNEL:
            return None
        if applied is None or ticker not in applied.tickers:
            return None
        book = self._books.get(ticker)
        if book is None and create:
            book = self._books[ticker] = Book(ticker)
        return book

    async def _request_snapshot(self, sid: int, tickers: Iterable[str]) -> None:
        """Send ``get_snapshot`` for the markets not already awaiting one on this sid."""
        awaiting = self._resyncing.setdefault(sid, set())
        wanted = tuple(sorted(set(tickers) - awaiting))
        if not wanted:
            return
        awaiting.update(wanted)
        self._snapshots_requested += 1
        session = self._session
        if session is None:
            raise WsClosedError(f"connection {self._config.conn_id} is not connected")
        await self._send(
            session,
            UpdateSubscriptionCommand(sid=sid, action="get_snapshot", market_tickers=wanted),
        )

    def _finish_resync(self, sid: int, ticker: str) -> None:
        awaiting = self._resyncing.get(sid)
        if awaiting is None:
            return
        awaiting.discard(ticker)
        if not awaiting:
            del self._resyncing[sid]

    # ------------------------------------------------------------ subscriptions

    async def _reconcile_locked(self) -> None:
        """Send what turns this connection's subscriptions into the desired group.

        Call with the command lock held. While any subscribe still awaits a reply the work
        is deferred until the last one arrives, because a membership change sent then would
        miss the channel whose ``sid`` is still unknown. For the same reason the markets of
        a group too large for one subscribe wait for that subscribe's replies.

        Raises:
            WsClosedError: If the connection closes while sending.
            KalshiTransportError: If a send times out.
        """
        session = self._session
        if session is None or not session.is_open:
            return
        if self._pending:
            self._reconcile_deferred = True
            return
        firehose = self._config.firehose_channels
        if firehose and not self._firehose_requested:
            self._firehose_requested = True
            command_id = await self._send(session, SubscribeCommand(channels=firehose))
            self._await_replies(command_id, FIREHOSE_GROUP_ID, firehose)
        channels = self._config.group_channels
        use_yes_price = self._config.use_yes_price if ORDERBOOK_CHANNEL in channels else None
        limit = self._config.max_markets_per_command
        for change in diff(_plan_of(self._applied), _plan_of(self._desired)):
            for piece in split_change(change, max_markets=limit):
                if isinstance(piece, AddMarkets | RemoveMarkets) and self._subscribing(
                    piece.group_id
                ):
                    # Resumed by the last reply, from the applied group, not from this diff.
                    self._reconcile_deferred = True
                    return
                commands = to_commands(
                    (piece,), channels=channels, use_yes_price=use_yes_price, sid_of=self._sid_of()
                )
                for command in commands:
                    command_id = await self._send(session, command)
                    if isinstance(piece, AddGroup):
                        self._await_replies(command_id, piece.group.group_id, channels)
                self._record_change(piece)

    def _subscribing(self, group_id: str) -> bool:
        """Whether a subscribe for the group still awaits a reply on some channel."""
        return any(pending.group_id == group_id for pending in self._pending.values())

    def _record_change(self, change: PlanChange) -> None:
        """Update the applied group once a change's commands are on the wire."""
        if isinstance(change, AddGroup):
            self._applied = change.group
        elif isinstance(change, RemoveGroup):
            group = self._applied_group(change.group_id)
            self._applied = None
            for sid in [s for s, i in self._subscriptions.items() if i.group_id == group.group_id]:
                del self._subscriptions[sid]
                self._resyncing.pop(sid, None)
            self._publish_subscriptions()
            self._release_books(group.tickers)
        elif isinstance(change, AddMarkets):
            group = self._applied_group(change.group_id)
            self._applied = msgspec.structs.replace(
                group, tickers=group.tickers | frozenset(change.tickers)
            )
        elif isinstance(change, RemoveMarkets):
            group = self._applied_group(change.group_id)
            self._applied = msgspec.structs.replace(
                group, tickers=group.tickers - frozenset(change.tickers)
            )
            self._release_books(change.tickers)
        else:
            assert_never(change)

    def _applied_group(self, group_id: str) -> Group:
        """Return the applied group a change names.

        Raises:
            RuntimeError: If no such group is applied; ``diff`` of the applied group never
                names another, so this is a supervisor bug.
        """
        applied = self._applied
        if applied is None or applied.group_id != group_id:
            raise RuntimeError(f"connection {self._config.conn_id} has no applied {group_id}")
        return applied

    def _release_books(self, tickers: Iterable[str]) -> None:
        """Stale books of markets still wanted here; drop books of markets leaving."""
        wanted = _tickers_of(self._desired)
        for ticker in tickers:
            if ticker in wanted:
                book = self._books.get(ticker)
                if book is not None:
                    book.mark_stale()
            else:
                self._books.pop(ticker, None)

    def _prune_books(self) -> None:
        """Drop books of markets neither desired nor still subscribed."""
        keep = _tickers_of(self._desired) | _tickers_of(self._applied)
        for ticker in [t for t in self._books if t not in keep]:
            del self._books[ticker]

    def _await_replies(self, command_id: int, group_id: str, channels: Iterable[str]) -> None:
        """Track a subscribe just sent until every channel it names has answered."""
        expiry = asyncio.create_task(
            self._expire_unanswered(command_id),
            name=f"subscribe-deadline-{self._config.conn_id}-{command_id}",
        )
        self._pending[command_id] = _PendingSubscribe(
            group_id=group_id, channels=set(channels), expiry=expiry
        )

    async def _expire_unanswered(self, command_id: int) -> None:
        """Fail the connection if a subscribe still lacks a reply once its deadline passes.

        Left pending, the subscribe would defer every later change on the connection
        forever. A reconnect starts from a clean subscription table instead. The task is
        cancelled as soon as the subscribe is answered, refused, or its connection ends.
        """
        timeout_ns = self._config.subscribe_timeout_ns
        await self._deadline_sleep(timeout_ns / NS_PER_S)
        pending = self._pending.get(command_id)
        reply_failure = self._reply_failure
        if pending is None or reply_failure is None or reply_failure.done():
            return
        channels = ", ".join(sorted(pending.channels))
        self._log.error(
            "subscribe unanswered; reconnecting",
            extra={
                "conn_id": self._config.conn_id,
                "command_id": command_id,
                "channels": sorted(pending.channels),
                "timeout_ns": timeout_ns,
            },
        )
        reply_failure.set_result(
            WsClosedError(
                f"connection {self._config.conn_id} subscribe {command_id} had no reply for "
                f"{channels} within {timeout_ns} ns"
            )
        )

    def _answer(self, command_id: int, pending: _PendingSubscribe, sid: int, channel: str) -> None:
        """Bind a channel's ``sid`` to the subscribe it answers. Call with the lock held."""
        self._subscriptions[sid] = SubscriptionInfo(
            sid=sid, channel=channel, group_id=pending.group_id
        )
        self._publish_subscriptions()
        pending.channels.discard(channel)
        if not pending.channels:
            del self._pending[command_id]
            pending.expiry.cancel()

    async def _on_subscribed(self, envelope: Envelope) -> None:
        """Bind a new ``sid`` to the group whose subscribe it answers."""
        msg = decode_msg(envelope, SubscribedMsg)
        async with self._command_lock:
            # Remembered even when unmatched: a sid names one channel for its connection's life.
            self._channel_of_sid[msg.sid] = msg.channel
            pending = None if envelope.id is None else self._pending.get(envelope.id)
            if envelope.id is None or pending is None:
                self._log.warning(
                    "subscribed response matches no pending subscribe",
                    extra={"conn_id": self._config.conn_id, "command_id": envelope.id},
                )
                return
            self._answer(envelope.id, pending, msg.sid, msg.channel)
            await self._resume_reconcile()

    async def _on_ok(self, envelope: Envelope) -> None:
        """Treat an ``ok`` that answers a subscribe as a merge into an existing subscription.

        Kalshi keeps one subscription per channel per connection; a subscribe naming a
        channel the connection already has is merged into it and answered with ``ok``,
        carrying that ``sid`` and the merged membership (ADR 0020). ``ok`` replies to
        ``update_subscription`` need nothing: the change was recorded when it was sent.
        """
        async with self._command_lock:
            command_id, sid = envelope.id, envelope.sid
            pending = None if command_id is None else self._pending.get(command_id)
            if command_id is None or pending is None:
                return
            channel = None if sid is None else self._channel_of_sid.get(sid)
            if sid is None or channel is None or channel not in pending.channels:
                # Left to the reply deadline: the channel it answers cannot be known.
                self._log.warning(
                    "ok reply to a subscribe names no subscription it could merge into",
                    extra={"conn_id": self._config.conn_id, "command_id": command_id, "sid": sid},
                )
                return
            msg = decode_msg(envelope, OkMsg)
            self._log.info(
                "subscribe merged into an existing subscription",
                extra={"conn_id": self._config.conn_id, "command_id": command_id, "sid": sid},
            )
            self._answer(command_id, pending, sid, channel)
            await self._adopt_membership(pending, sid, channel, msg.market_tickers)
            await self._resume_reconcile()

    async def _adopt_membership(
        self,
        pending: _PendingSubscribe,
        sid: int,
        channel: str,
        market_tickers: list[str] | None,
    ) -> None:
        """Make a merged subscription's membership the connection's market set.

        The merged subscription may hold markets the subscribe did not name, so the next
        reconciliation compares it with the desired group. Which markets of a merge Kalshi
        snapshots is undocumented, and a market the subscription already held may get none,
        so a snapshot is requested for every merged market wanted here without a fresh book.

        Raises:
            WsClosedError: If the snapshot request cannot be sent because the connection closed.
            KalshiTransportError: If the snapshot request times out.
        """
        applied = self._applied
        if not market_tickers or applied is None or applied.group_id != pending.group_id:
            return
        merged = frozenset(market_tickers)
        if merged != applied.tickers:
            self._applied = msgspec.structs.replace(applied, tickers=merged)
            self._reconcile_deferred = True
        if channel != ORDERBOOK_CHANNEL:
            return
        wanted = merged & _tickers_of(self._desired)
        await self._request_snapshot(sid, [ticker for ticker in wanted if not self._fresh(ticker)])

    def _fresh(self, ticker: str) -> bool:
        book = self._books.get(ticker)
        return book is not None and not book.is_stale()

    async def _on_error(self, envelope: Envelope) -> None:
        """Count and surface an error frame; a refused subscribe or snapshot may be retried."""
        msg = decode_msg(envelope, ErrorMsg)
        self._errors_by_code[msg.code] += 1
        self._last_errors.append(WsProtocolError(msg.code, msg.msg, envelope.sid))
        self._log.log(
            logging.ERROR if msg.code in CAPACITY_ERROR_CODES else logging.WARNING,
            "exchange error frame",
            extra={
                "conn_id": self._config.conn_id,
                "code": msg.code,
                "error_message": msg.msg,
                "sid": envelope.sid,
                "command_id": envelope.id,
            },
        )
        if envelope.sid is not None:
            # A refused get_snapshot will never be answered; let the next trigger ask again.
            self._resyncing.pop(envelope.sid, None)
        if envelope.id is None:
            return
        async with self._command_lock:
            pending = self._pending.pop(envelope.id, None)
            if pending is None:
                return
            pending.expiry.cancel()
            if pending.group_id == FIREHOSE_GROUP_ID:
                self._firehose_requested = False
            elif all(info.group_id != pending.group_id for info in self._subscriptions.values()):
                # Nothing was subscribed, so the group is not applied; the next
                # reconciliation subscribes it again.
                self._applied = None
            await self._resume_reconcile()

    async def _resume_reconcile(self) -> None:
        """Run a deferred reconciliation once no subscribe awaits a reply. Lock held."""
        if self._reconcile_deferred and not self._pending:
            self._reconcile_deferred = False
            await self._reconcile_locked()

    def _sid_of(self) -> dict[str, tuple[int, ...]]:
        sids: dict[str, list[int]] = {}
        for sid, info in sorted(self._subscriptions.items()):
            sids.setdefault(info.group_id, []).append(sid)
        return {group_id: tuple(group_sids) for group_id, group_sids in sids.items()}

    def _publish_subscriptions(self) -> None:
        # Replaced, never mutated, so a reader on another thread sees a consistent tuple.
        self._subscription_infos = tuple(info for _, info in sorted(self._subscriptions.items()))

    # -------------------------------------------------------------------- outputs

    async def _send(self, session: WsSession, command: Command) -> int:
        """Send a command and record the exact bytes that went out.

        Raises:
            WsClosedError: If the connection is closed.
            KalshiTransportError: If the send times out.
        """
        # Stamped before the send so the record sorts ahead of the response it provokes.
        mono_ns, wall_ns = self._clock.mono_ns(), self._clock.wall_ns()
        command_id = await session.send(command)
        self._put(RecordKind.COMMAND, mono_ns, wall_ns, encode_command(command, command_id))
        return command_id

    def _put(self, kind: RecordKind, mono_ns: int, wall_ns: int, payload: bytes) -> None:
        if self._sink is None:
            return
        record = Record(
            kind=kind,
            conn_id=self._config.conn_id,
            recv_mono_ns=mono_ns,
            recv_wall_ns=wall_ns,
            payload=payload,
        )
        if not self._sink.put(record):
            self._records_not_persisted += 1

    def _record_connection_event(self, event: str, detail: str) -> None:
        payload = msgspec.json.encode({"event": event, "detail": detail})
        self._put(RecordKind.CONNECTION, self._clock.mono_ns(), self._clock.wall_ns(), payload)

    def _emit(self, event: MarketEvent) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(event)
        except Exception as exc:
            # A consumer bug must never stop recording (docs/ARCHITECTURE.md 3.6).
            self._callback_errors += 1
            self._log.exception(
                "event consumer failed",
                extra={"conn_id": self._config.conn_id, "error": repr(exc)},
            )

    def _record_in_taps(self, change: BookChange, *, was_stale: bool) -> None:
        for tap in self._taps.get(change.ticker, ()):
            tap.record(change, was_stale=was_stale)

    def _release_tap(self, tap: LiveBookTap) -> None:
        """Stop routing changes to a tap that has closed."""
        for ticker in tap.tickers:
            remaining = [open_tap for open_tap in self._taps.get(ticker, ()) if open_tap is not tap]
            if remaining:
                self._taps[ticker] = remaining
            else:
                self._taps.pop(ticker, None)

    def _decode_error(self, exc: Exception, message_type: str | None) -> None:
        self._decode_errors += 1
        self._log.warning(
            "frame not decoded; it is already in the tape",
            extra={"conn_id": self._config.conn_id, "type": message_type, "error": repr(exc)},
        )

    def _book_error(self, exc: BookInvariantError, sid: int) -> None:
        self._book_errors += 1
        self._log.error(
            "book invariant broken; book is stale",
            extra={
                "conn_id": self._config.conn_id,
                "sid": sid,
                "ticker": exc.ticker,
                "detail": exc.detail,
            },
        )


def _plan_of(group: Group | None) -> Plan:
    """The one-group plan ``diff`` compares; a connection carries at most one group."""
    return Plan(groups=() if group is None else (group,))


def _tickers_of(group: Group | None) -> frozenset[str]:
    return frozenset() if group is None else group.tickers


async def _cancel[T](future: asyncio.Future[T]) -> None:
    """Cancel a future unless it is done, and wait for the cancellation to land."""
    if not future.done():
        future.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await future
