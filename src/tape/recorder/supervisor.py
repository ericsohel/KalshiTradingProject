"""Supervise one Kalshi WebSocket connection: record it, decode it, resynchronize it, revive it.

Responsibility: own the life of one WebSocket connection for the recorder
(docs/ARCHITECTURE.md 7.1 and 9, docs/INTERFACES.md 8.4). Every inbound frame is handed to
the segment sink before anything parses it (ADR 0001); then its envelope is read and its
sequence number checked per ``sid``; only then is the payload decoded into books and
events. A sequence gap on a book subscription is written into the tape, the group's books
are marked stale, and a ``get_snapshot`` is requested. A lost connection is written into
the tape, every book is marked stale, the segment is rotated, and after a jittered
exponential backoff the connection is rebuilt and every group resubscribed from the
supervisor's own group table, because ``sid``s do not survive a connection.

Invariants: per frame the order is sink, envelope, sequence check, everything else, so a
decoder can never lose a frame; a book leaves the stale state only through a snapshot
that arrived on a subscription of the current connection; the subscription table holds
only ``sid``s the current connection assigned; a membership change is sent to every
``sid`` of its group, and never while one of them is still unknown; the reconnect loop
ends on :meth:`ConnectionSupervisor.stop` or after ``max_consecutive_failures``; and the
module sleeps, draws randomness, and reads time only through what was injected.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
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
    group_sort_key,
    to_commands,
)
from tape.recorder.writer import SegmentSink
from tape.segment import Record, RecordKind, SubscriptionInfo
from tape.timeutil import NS_PER_S, Clock
from tape.wire import (
    Envelope,
    ErrorMsg,
    MarketLifecycleV2Msg,
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
        conn_id: Connection id; every group given to the supervisor must carry it.
        book_channels: Channels each group subscribes, one ``sid`` each.
        firehose_channels: Channels subscribed once per connection with no market filter,
            for example ``("ticker",)`` on the control connection. A :class:`Group` cannot
            express "every market", so these live outside the group table.
        use_yes_price: Request YES-leg prices for both book sides and convert accordingly
            (ADR 0006).
        persist: Write frames, commands, gaps, and connection events to the sink. When
            false the connection is live-only: decoded and published, never written.
        backoff_initial_ns: Nominal delay after the first consecutive failure.
        backoff_max_ns: Cap on the nominal delay.
        max_consecutive_failures: Failures in a row tolerated before :meth:`run` raises;
            ``None`` retries until stopped.

    Raises:
        ValueError: On a negative ``conn_id``, no channels at all, a non-positive initial
            backoff, a cap below it, or a negative failure limit.
    """

    conn_id: int
    book_channels: tuple[str, ...] = (ORDERBOOK_CHANNEL, "trade")
    firehose_channels: tuple[str, ...] = ()
    use_yes_price: bool = True
    persist: bool = True
    backoff_initial_ns: int = DEFAULT_BACKOFF_INITIAL_NS
    backoff_max_ns: int = DEFAULT_BACKOFF_MAX_NS
    max_consecutive_failures: int | None = None

    def __post_init__(self) -> None:
        if self.conn_id < 0:
            raise ValueError(f"conn_id must be non-negative, got {self.conn_id}")
        if not self.book_channels and not self.firehose_channels:
            raise ValueError("a connection needs book_channels or firehose_channels")
        if self.backoff_initial_ns <= 0 or self.backoff_max_ns < self.backoff_initial_ns:
            raise ValueError(
                f"need 0 < backoff_initial_ns <= backoff_max_ns, got "
                f"{self.backoff_initial_ns} and {self.backoff_max_ns}"
            )
        limit = self.max_consecutive_failures
        if limit is not None and limit < 0:
            raise ValueError(f"max_consecutive_failures must be non-negative, got {limit}")


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
    """A ``subscribe`` whose ``subscribed`` responses have not all arrived."""

    group_id: str
    channels: set[str]


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
        self._log = logger if logger is not None else logging.getLogger(__name__)
        # Survives reconnects: the desired groups and the books they feed.
        self._desired = Plan(groups=())
        self._books: dict[str, Book] = {}
        # Rebuilt for every connection, because sids and sequence numbers are connection-scoped.
        self._session: WsSession | None = None
        self._tracker = GapTracker()
        self._applied: dict[str, Group] = {}
        self._subscriptions: dict[int, SubscriptionInfo] = {}
        self._subscription_infos: tuple[SubscriptionInfo, ...] = ()
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
    def groups(self) -> tuple[Group, ...]:
        """The desired groups, applied now or on the next connection."""
        return self._desired.groups

    @property
    def subscriptions(self) -> tuple[SubscriptionInfo, ...]:
        """Subscriptions of the current connection by ascending ``sid``. Safe from any thread."""
        return self._subscription_infos

    @property
    def last_errors(self) -> tuple[WsProtocolError, ...]:
        """The most recent error frames, oldest first, at most ``MAX_RECENT_ERRORS``."""
        return tuple(self._last_errors)

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

    async def set_groups(self, groups: Sequence[Group]) -> None:
        """Replace the group table and move the live connection to it.

        The difference is sent as ``planner`` commands; if the connection is down, or a
        subscribe is still waiting for its ``sid``s, it is applied as soon as that
        changes. If a command cannot be sent, the connection is dropped so that the
        reconnect resubscribes from the table rather than from a half-applied change.

        Args:
            groups: Every group this connection should carry.

        Raises:
            ValueError: If a group belongs to another connection, two groups share an id
                or a ticker, or groups are given to a connection without book channels.
        """
        for group in groups:
            if group.conn_id != self._config.conn_id:
                raise ValueError(
                    f"group {group.group_id} is for connection {group.conn_id}, "
                    f"not {self._config.conn_id}"
                )
        if groups and not self._config.book_channels:
            raise ValueError(f"connection {self._config.conn_id} has no book channels")
        self._desired = Plan(groups=tuple(sorted(groups, key=group_sort_key)))
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
                async for frame in session.frames():
                    await self._on_frame(frame)
                if not self._stop_requested.is_set():
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

    def _begin_connection(self, session: WsSession) -> None:
        """Adopt a new session with no connection-scoped state. Call with the lock held."""
        self._forget_connection()
        self._session = session

    def _forget_connection(self) -> None:
        """Drop everything scoped to a connection: sids, sequences, pending commands.

        Call with the lock held, so that a reconciliation in flight never sees the tables
        change under it.
        """
        self._session = None
        self._tracker = GapTracker()
        self._applied = {}
        self._subscriptions.clear()
        self._publish_subscriptions()
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
            for future in (stop_future, work_future):
                if not future.done():
                    future.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await future
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
            case "unsubscribed":
                if envelope.sid is not None:
                    self._tracker.forget(envelope.sid)
            case "error":
                await self._on_error(envelope)
            case _:
                # ``ok`` acknowledgements and types this supervisor does not interpret are
                # already in the tape; there is nothing to apply.
                return

    async def _on_gap(self, sid: int, gap: Gap, receipt: Receipt) -> None:
        """Record a gap and, on a book subscription, stale its group and ask for snapshots."""
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
        await self._resync_group(sid, ask_again=True)

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
        await self._resync_group(sid, ask_again=False)

    async def _resync_group(self, sid: int, *, ask_again: bool) -> None:
        """Stale every book of a book subscription's group and request their snapshots.

        Only book subscriptions can be repaired: ``get_snapshot`` exists for the orderbook
        channel alone, and a missed trade or lifecycle message leaves no book wrong.

        Args:
            sid: Subscription whose sequence misbehaved.
            ask_again: Request markets already awaiting a snapshot as well.

        Raises:
            WsClosedError: If the request cannot be sent because the connection closed.
            KalshiTransportError: If the request times out.
        """
        info = self._subscriptions.get(sid)
        group = None if info is None else self._applied.get(info.group_id)
        if info is None or group is None or info.channel != ORDERBOOK_CHANNEL:
            return
        for ticker in group.tickers:
            book = self._books.get(ticker)
            if book is not None:
                book.mark_stale()
        if ask_again:
            self._resyncing.pop(sid, None)
        await self._request_snapshot(sid, group.tickers)

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
        try:
            book.apply_snapshot(snapshot.bids, snapshot.asks, ts_ms=snapshot.ts_ms)
        except BookInvariantError as exc:
            # Not re-requested here: an exchange that reports a crossed book would answer
            # the same way forever. The book stays stale until a gap or delta error asks.
            self._book_error(exc, snapshot.sid)
            return
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
            self._emit(delta)

    def _book_for(self, sid: int, ticker: str, *, create: bool) -> Book | None:
        """Return the book a book message may touch, or ``None`` if it may touch none.

        A message may touch a book only if it arrived on a live orderbook subscription of
        this connection whose group holds the market; anything else is a straggler from a
        subscription already retired, and applying it could revive a book no one updates.
        """
        info = self._subscriptions.get(sid)
        if info is None or info.channel != ORDERBOOK_CHANNEL:
            return None
        group = self._applied.get(info.group_id)
        if group is None or ticker not in group.tickers:
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
        """Send what turns this connection's subscriptions into the desired groups.

        Call with the command lock held. While any subscribe still awaits a ``sid`` the
        work is deferred until the last one arrives, because a membership change sent then
        would miss the channel whose ``sid`` is still unknown.

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
            self._pending[command_id] = _PendingSubscribe(FIREHOSE_GROUP_ID, set(firehose))
        applied = Plan(groups=tuple(sorted(self._applied.values(), key=group_sort_key)))
        for change in diff(applied, self._desired):
            commands = to_commands(
                (change,),
                channels=self._config.book_channels,
                use_yes_price=self._config.use_yes_price,
                sid_of=self._sid_of(),
            )
            for command in commands:
                command_id = await self._send(session, command)
                if isinstance(change, AddGroup):
                    self._pending[command_id] = _PendingSubscribe(
                        change.group.group_id, set(self._config.book_channels)
                    )
            self._record_change(change)

    def _record_change(self, change: PlanChange) -> None:
        """Update the applied table once a change's commands are on the wire."""
        if isinstance(change, AddGroup):
            self._applied[change.group.group_id] = change.group
        elif isinstance(change, RemoveGroup):
            group = self._applied.pop(change.group_id)
            for sid in [s for s, i in self._subscriptions.items() if i.group_id == group.group_id]:
                del self._subscriptions[sid]
                self._resyncing.pop(sid, None)
            self._publish_subscriptions()
            self._release_books(group.tickers)
        elif isinstance(change, AddMarkets):
            group = self._applied[change.group_id]
            self._applied[change.group_id] = msgspec.structs.replace(
                group, tickers=group.tickers | frozenset(change.tickers)
            )
        elif isinstance(change, RemoveMarkets):
            group = self._applied[change.group_id]
            self._applied[change.group_id] = msgspec.structs.replace(
                group, tickers=group.tickers - frozenset(change.tickers)
            )
            self._release_books(change.tickers)
        else:
            assert_never(change)

    def _release_books(self, tickers: Iterable[str]) -> None:
        """Stale books of markets moving to another group; drop books of markets leaving."""
        wanted = self._desired.tickers
        for ticker in tickers:
            if ticker in wanted:
                book = self._books.get(ticker)
                if book is not None:
                    book.mark_stale()
            else:
                self._books.pop(ticker, None)

    def _prune_books(self) -> None:
        """Drop books of markets neither desired nor still subscribed."""
        keep = self._desired.tickers | {t for g in self._applied.values() for t in g.tickers}
        for ticker in [t for t in self._books if t not in keep]:
            del self._books[ticker]

    async def _on_subscribed(self, envelope: Envelope) -> None:
        """Bind a new ``sid`` to the group whose subscribe it answers."""
        msg = decode_msg(envelope, SubscribedMsg)
        async with self._command_lock:
            pending = None if envelope.id is None else self._pending.get(envelope.id)
            if envelope.id is None or pending is None:
                self._log.warning(
                    "subscribed response matches no pending subscribe",
                    extra={"conn_id": self._config.conn_id, "command_id": envelope.id},
                )
                return
            self._subscriptions[msg.sid] = SubscriptionInfo(
                sid=msg.sid, channel=msg.channel, group_id=pending.group_id
            )
            self._publish_subscriptions()
            pending.channels.discard(msg.channel)
            if not pending.channels:
                del self._pending[envelope.id]
                await self._resume_reconcile()

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
            if pending.group_id == FIREHOSE_GROUP_ID:
                self._firehose_requested = False
            elif all(info.group_id != pending.group_id for info in self._subscriptions.values()):
                # Nothing was subscribed, so the group is not applied; the next
                # reconciliation subscribes it again.
                self._applied.pop(pending.group_id, None)
            await self._resume_reconcile()

    async def _resume_reconcile(self) -> None:
        """Run a deferred reconciliation once no subscribe awaits a ``sid``. Lock held."""
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
