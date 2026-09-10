"""Audit local books against Kalshi's REST orderbooks over a request window, and tape the result.

Responsibility: implement docs/ARCHITECTURE.md 7.1 step 6, the ``Auditor`` of
docs/INTERFACES.md 8.5, and the window-consistency definition of ADR 0021, which produces one
of the three published daily integrity numbers (docs/DATA_FORMATS.md 7, docs/TESTING.md 7).
Every ``interval_s`` it rotates deterministically through the recorder's known local books,
skips ones currently marked stale, and audits the rest in batches of at most 100 tickers. Per
batch it opens a book tap, waits ``lead_ns`` so the window starts before the request, fetches
the REST orderbooks, waits ``settle_ns`` after the reply, closes the tap, and then classifies
each market with :func:`classify_window`:

- ``exact``: the REST book equals the local book as of the reply's receive time;
- ``consistent``: not exact, but equal to the tap's starting copy or to the book after some
  change inside the window;
- ``inconsistent``: equal to no state in the window, a finding to investigate;
- ``undecidable``: the tap cannot vouch for every state in the window (no fresh book when it
  opened, a stale book inside it, or more changes than it may hold).

CRITICAL: the WebSocket order book is subscribed with ``use_yes_price=true``, so local books
are already in YES space, but the REST orderbook endpoints carry no such flag.
``yes_dollars`` are YES bids at YES prices; ``no_dollars`` are NO bids at NO-leg prices, and a
NO bid at price ``q`` is a YES ask at ``1 - q`` (docs/DATA_FORMATS.md 1.3, 2.2).
:func:`tape.wire.convert.rest_orderbook_levels` performs that complement; comparing REST to
local without it would make every audit a mismatch.

Invariants: two books are judged equal only when :func:`tape.book.diff` finds no difference,
so a checksum collision can never pass an audit; every tap opened is closed, whatever happens
to its batch; one bad REST batch, one malformed REST snapshot, or one market without a usable
window never stops a round or raises past :meth:`Auditor.audit_once`; every counter in
:class:`AuditStats` only grows, with ``books_sampled == books_exact + books_consistent +
books_inconsistent`` and ``books_mismatched == books_consistent + books_inconsistent``;
:func:`round_robin_choice` and :func:`classify_window` are pure and deterministic.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Literal

import msgspec

from tape.book import Book, diff
from tape.client.rest import KalshiRest
from tape.errors import BookInvariantError, FixedPointError, KalshiHttpError, KalshiTransportError
from tape.events import BookSnapshot, Side
from tape.recorder.tap import (
    FAULT_NO_BOOK,
    BookChange,
    BookImage,
    BookTapOpener,
    TapFault,
    TapWindow,
)
from tape.recorder.writer import RecordSink
from tape.segment import Record, RecordKind
from tape.timeutil import NS_PER_S, Clock
from tape.wire.convert import rest_orderbook_levels
from tape.wire.rest import MarketOrderbookFp

__all__ = [
    "AuditOutcome",
    "AuditResult",
    "AuditStats",
    "Auditor",
    "WindowVerdict",
    "classify_window",
    "round_robin_choice",
]

type AuditOutcome = Literal["exact", "consistent", "inconsistent", "undecidable"]
"""How a REST snapshot relates to the local book states in its request window (ADR 0021)."""

_MAX_BATCH_TICKERS: Final = 100
"""``GET /markets/orderbooks`` accepts at most this many tickers per call.

See docs/DATA_FORMATS.md 2.2.
"""


class AuditResult(msgspec.Struct, frozen=True, kw_only=True):
    """One market's REST orderbook judged against the local book states in its window.

    Attributes:
        ticker: Market audited.
        outcome: The classification; see the module docstring.
        window_open_mono_ns: Monotonic time the tap opened.
        send_mono_ns: Monotonic time just before the REST request was sent.
        send_wall_ns: Wall-clock time just before the REST request was sent.
        recv_mono_ns: Monotonic time the REST response was received.
        recv_wall_ns: Wall-clock time the REST response was received.
        window_close_mono_ns: Monotonic time just before the tap closed.
        window_events: Book changes the tap recorded for this market.
        match_index: Window changes applied before the state that equals the REST book,
            ``0`` meaning the starting copy; when several states match, the one nearest the
            reply, the earlier on a tie. ``None`` unless exact or consistent.
        levels_rest: Non-empty price levels in the REST snapshot, both sides.
        levels_local: Non-empty price levels in the local book as of the reply, both sides;
            ``None`` when undecidable.
        mismatched_levels: Price levels whose count differs between the REST book and the
            local book as of the reply (``tape.book.diff``); zero exactly when exact, ``None``
            when undecidable.
        max_abs_diff_e2: Largest absolute count difference across those levels, in
            ``CountE2`` units; zero when exact, ``None`` when undecidable.
        fault: Why the window cannot vouch for every state it spans, for an undecidable
            audit whose tap reported a reason: ``"no_book"``, ``"stale"``, or
            ``"overflow"``. ``None`` otherwise.
    """

    ticker: str
    outcome: AuditOutcome
    window_open_mono_ns: int
    send_mono_ns: int
    send_wall_ns: int
    recv_mono_ns: int
    recv_wall_ns: int
    window_close_mono_ns: int
    window_events: int
    match_index: int | None
    levels_rest: int
    levels_local: int | None
    mismatched_levels: int | None
    max_abs_diff_e2: int | None
    fault: TapFault | None = None

    @property
    def exact(self) -> bool:
        """Whether the REST book equals the local book as of the reply."""
        return self.outcome == "exact"


class AuditStats(msgspec.Struct, frozen=True, kw_only=True):
    """Cumulative counters across every completed :meth:`Auditor.audit_once` call.

    No field here is a float: docs/ENGINEERING_STANDARDS.md 3.1 forbids float on any path
    feeding a published number, and the audit ratio is one of the three daily integrity
    numbers (docs/DATA_FORMATS.md 7). The ratios are exact integer fractions instead.

    Attributes:
        rounds: Completed calls to :meth:`Auditor.audit_once`.
        books_sampled: Decidable audits: exact, consistent, or inconsistent.
        books_exact: Audits whose REST book equals the local book as of the reply.
        books_consistent: Audits not exact but equal to some other state in the window.
        books_inconsistent: Audits equal to no state in the window.
        books_mismatched: Decidable audits that differ from the local book as of the reply,
            ``books_consistent + books_inconsistent``.
        books_undecidable: Audits whose window cannot vouch for every state it spans; not
            part of ``books_sampled``.
        books_skipped_stale: Chosen tickers whose local book was stale and so were never
            fetched.
        books_missing_local: Of ``books_undecidable``, those with no fresh local book when the
            tap opened.
        books_invalid_rest: REST snapshots that were malformed or crossed, and were skipped
            rather than audited.
        levels_mismatched: Sum of ``mismatched_levels`` across every decidable audit.
    """

    rounds: int
    books_sampled: int
    books_exact: int
    books_consistent: int
    books_inconsistent: int
    books_mismatched: int
    books_undecidable: int
    books_skipped_stale: int
    books_missing_local: int
    books_invalid_rest: int
    levels_mismatched: int

    @property
    def exact_ratio(self) -> tuple[int, int]:
        """Exact ``(numerator, denominator)`` for ``books_exact / books_sampled``.

        Returns:
            ``(books_exact, books_sampled)``. ``(0, 0)`` means "no data", not "zero ratio",
            and must be handled as such rather than divided.
        """
        return (self.books_exact, self.books_sampled)

    @property
    def consistency_ratio(self) -> tuple[int, int]:
        """The published audit number (ADR 0021) as an exact ``(numerator, denominator)``.

        Returns:
            ``(exact + consistent, exact + consistent + inconsistent)``; undecidable audits
            are in neither. ``(0, 0)`` means "no data" and must not be divided.
        """
        passed = self.books_exact + self.books_consistent
        return (passed, passed + self.books_inconsistent)


class WindowVerdict(msgspec.Struct, frozen=True, kw_only=True):
    """What :func:`classify_window` concluded about one window.

    Attributes:
        outcome: The classification.
        match_index: Window changes applied before the matching state nearest the reply;
            ``None`` unless exact or consistent.
        reply_state: The local book as of the reply; ``None`` when undecidable.
    """

    outcome: AuditOutcome
    match_index: int | None
    reply_state: BookImage | None


_UNDECIDABLE: Final = WindowVerdict(outcome="undecidable", match_index=None, reply_state=None)


def round_robin_choice(
    tickers: Sequence[str], count: int, cursor: int
) -> tuple[tuple[str, ...], int]:
    """Choose the next ``count`` tickers from a sorted, deterministic rotation.

    Pure and reads no clock or randomness, so repeated calls with an advancing ``cursor``
    visit every distinct ticker exactly once per full rotation, in the same order on every
    run (docs/ARCHITECTURE.md 7.1 step 6).

    Args:
        tickers: Candidate tickers for this rotation. Duplicates are ignored; order does not
            matter, since the tickers are sorted before choosing.
        count: Maximum number of tickers to return.
        cursor: Position to resume from, as returned by a previous call. Any integer is
            accepted; it is wrapped modulo the number of distinct tickers.

    Returns:
        The chosen tickers, ascending from ``cursor``'s wrapped position, and the cursor to
        pass to the next call. Fewer than ``count`` tickers, with no repeats, come back when
        there are fewer than ``count`` distinct tickers; ``((), 0)`` when ``tickers`` is
        empty.

    Raises:
        ValueError: If ``count`` is negative.
    """
    if count < 0:
        raise ValueError(f"count must be non-negative, got {count}")
    unique = tuple(sorted(set(tickers)))
    total = len(unique)
    if total == 0:
        return (), 0
    take = min(count, total)
    start = cursor % total
    chosen = tuple(unique[(start + i) % total] for i in range(take))
    new_cursor = (start + take) % total
    return chosen, new_cursor


def classify_window(
    window: TapWindow,
    rest_book: Book,
    *,
    reply_mono_ns: int,
    checksum: Callable[[Book], int] = Book.checksum,
) -> WindowVerdict:
    """Classify a REST book against every local book state a tap window spans (ADR 0021).

    The states are the window's starting copy and the book after each of its events, in
    order; the state as of the reply is the copy plus the leading events received at or
    before ``reply_mono_ns``. Pure: the window and ``rest_book`` are not modified.

    A state equals the REST book only if their level counts agree, their checksums agree,
    and :func:`tape.book.diff` then finds no difference, so a checksum collision can never
    count as a match. Replay stops once no later state could be a nearer match.

    Args:
        window: What the tap saw of the market.
        rest_book: The REST snapshot, already in YES space.
        reply_mono_ns: Monotonic time the REST response was received.
        checksum: The fast pre-comparison; injectable so tests can force collisions.

    Returns:
        ``undecidable`` when the window has a fault; otherwise ``exact`` when the state as of
        the reply matches, ``consistent`` when another state matches, ``inconsistent`` when
        none does, with the matching state nearest the reply (the earlier on a tie) and the
        state as of the reply.

    Raises:
        ValueError: If ``rest_book`` is for another market than ``window``.
        BookInvariantError: If the window's events do not replay onto its starting copy,
            which a window without a fault never does.
    """
    if rest_book.ticker != window.ticker:
        raise ValueError(f"cannot classify {rest_book.ticker!r} against {window.ticker!r}")
    if window.fault is not None or window.start is None:
        return _UNDECIDABLE
    events = window.events
    reply_index = _received_by(events, reply_mono_ns)
    rest_checksum = checksum(rest_book)
    book = window.start.to_book(window.ticker)
    nearest: int | None = None
    # States up to the reply: each match is nearer the reply than any before it.
    for index in range(reply_index + 1):
        if index > 0:
            _replay(book, events[index - 1])
        if _same_levels(book, rest_book, rest_checksum, checksum):
            nearest = index
    reply_state = BookImage.of(book)
    if nearest == reply_index:
        return WindowVerdict(outcome="exact", match_index=nearest, reply_state=reply_state)
    # States after the reply: only those strictly nearer than the match found before it can
    # replace it, because a tie goes to the earlier state.
    last = len(events) if nearest is None else min(len(events), 2 * reply_index - nearest - 1)
    for index in range(reply_index + 1, last + 1):
        _replay(book, events[index - 1])
        if _same_levels(book, rest_book, rest_checksum, checksum):
            return WindowVerdict(outcome="consistent", match_index=index, reply_state=reply_state)
    if nearest is None:
        return WindowVerdict(outcome="inconsistent", match_index=None, reply_state=reply_state)
    return WindowVerdict(outcome="consistent", match_index=nearest, reply_state=reply_state)


def _received_by(events: Sequence[BookChange], reply_mono_ns: int) -> int:
    """Count the leading events received at or before ``reply_mono_ns``."""
    count = 0
    for event in events:
        if event.receipt.recv_mono_ns > reply_mono_ns:
            break
        count += 1
    return count


def _replay(book: Book, change: BookChange) -> None:
    """Apply one recorded change to a rebuilt book.

    Raises:
        BookInvariantError: If the change does not apply.
    """
    if isinstance(change, BookSnapshot):
        book.apply_snapshot(change.bids, change.asks, ts_ms=change.ts_ms)
    else:
        # Never ignored as stale: the rebuilt book leaves fresh from its copy, and any
        # change that would have staled it raised instead.
        book.apply_delta(change.side, change.price, change.delta, ts_ms=change.ts_ms)


def _same_levels(
    local: Book, rest: Book, rest_checksum: int, checksum: Callable[[Book], int]
) -> bool:
    """Whether two books hold identical levels; ``diff`` has the final word."""
    # Level counts are a necessary condition and cost nothing, so most states never pay for a
    # checksum, and only a checksum match pays for a diff.
    for side in (Side.BID, Side.ASK):
        if local.level_count(side) != rest.level_count(side):
            return False
    return checksum(local) == rest_checksum and diff(rest, local).is_empty


def _levels_json(image: BookImage) -> list[dict[str, int]]:
    """Both sides of a book image as JSON-ready level dicts, best first, for a tape record."""
    return [
        {"side": int(side), "price_e4": int(level.price), "count_e2": int(level.count)}
        for side, levels in ((Side.BID, image.bids), (Side.ASK, image.asks))
        for level in levels
    ]


@dataclass(frozen=True, slots=True)
class _Timing:
    """When one batch's window opened and closed, and its request went out and came back."""

    window_open_mono_ns: int
    send_mono_ns: int
    send_wall_ns: int
    recv_mono_ns: int
    recv_wall_ns: int
    window_close_mono_ns: int


class Auditor:
    """Audits local books against Kalshi's REST orderbooks. See the module docstring.

    Args:
        rest: Client used to fetch REST orderbooks; only ``orderbooks`` is called.
        books: Returns the current local books across every connection, keyed by ticker;
            called at the start of a round to choose candidates and skip stale ones.
        clock: Source of the monotonic and wall-clock times that bound each window and stamp
            results and tape records.
        sink_for: Returns the sink of the connection that owns a ticker, or ``None`` if the
            ticker is unknown or its connection is unrecorded; a result is still counted
            when this returns ``None``, but nothing is written to tape for it. A record
            carries that sink's connection id.
        open_tap: Opens a tap over a batch's markets wherever their books live.
        sample_size: Maximum tickers chosen per round.
        lead_ns: How long the window is open before the request is sent.
        settle_ns: How long the window stays open after the reply arrives.
        tap_max_events: Most book changes a tap holds per market before that market's audit
            is undecidable.
        window_sleep: Waits out ``lead_ns`` and ``settle_ns``, given in seconds; injected so
            tests control time. Kept apart from :meth:`run`'s interval sleep.
        logger: Destination for logs; defaults to this module's logger.

    Raises:
        ValueError: If ``sample_size`` or ``tap_max_events`` is not positive, or ``lead_ns``
            or ``settle_ns`` is negative.
    """

    def __init__(
        self,
        rest: KalshiRest,
        books: Callable[[], Mapping[str, Book]],
        clock: Clock,
        *,
        sink_for: Callable[[str], RecordSink | None],
        open_tap: BookTapOpener,
        sample_size: int,
        lead_ns: int,
        settle_ns: int,
        tap_max_events: int,
        window_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        logger: logging.Logger | None = None,
    ) -> None:
        for name, value in (("sample_size", sample_size), ("tap_max_events", tap_max_events)):
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value}")
        for name, value in (("lead_ns", lead_ns), ("settle_ns", settle_ns)):
            if value < 0:
                raise ValueError(f"{name} must be non-negative, got {value}")
        self._rest = rest
        self._books = books
        self._clock = clock
        self._sink_for = sink_for
        self._open_tap = open_tap
        self._sample_size = sample_size
        self._lead_ns = lead_ns
        self._settle_ns = settle_ns
        self._tap_max_events = tap_max_events
        self._window_sleep = window_sleep
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._cursor = 0
        self._rounds = 0
        self._outcomes: dict[AuditOutcome, int] = dict.fromkeys(
            ("exact", "consistent", "inconsistent", "undecidable"), 0
        )
        self._books_skipped_stale = 0
        self._books_missing_local = 0
        self._books_invalid_rest = 0
        self._levels_mismatched = 0

    @property
    def stats(self) -> AuditStats:
        """Current cumulative counters; see :class:`AuditStats`."""
        exact = self._outcomes["exact"]
        consistent = self._outcomes["consistent"]
        inconsistent = self._outcomes["inconsistent"]
        return AuditStats(
            rounds=self._rounds,
            books_sampled=exact + consistent + inconsistent,
            books_exact=exact,
            books_consistent=consistent,
            books_inconsistent=inconsistent,
            books_mismatched=consistent + inconsistent,
            books_undecidable=self._outcomes["undecidable"],
            books_skipped_stale=self._books_skipped_stale,
            books_missing_local=self._books_missing_local,
            books_invalid_rest=self._books_invalid_rest,
            levels_mismatched=self._levels_mismatched,
        )

    async def audit_once(self) -> tuple[AuditResult, ...]:
        """Sample, fetch, and classify one round of books.

        Chooses up to ``sample_size`` tickers with :func:`round_robin_choice` over the
        current local books, skipping (and counting) any that are stale, and audits the rest
        in batches of at most 100 tickers, one window per batch. A batch that fails outright
        (transport or HTTP error) is logged and skipped; the remaining batches still run.

        Returns:
            One :class:`AuditResult` per REST snapshot audited, in batch order.
        """
        current_books = self._books()
        chosen, self._cursor = round_robin_choice(
            tuple(current_books.keys()), self._sample_size, self._cursor
        )
        to_fetch: list[str] = []
        for ticker in chosen:
            book = current_books.get(ticker)
            if book is None or book.is_stale():
                self._books_skipped_stale += 1
                continue
            to_fetch.append(ticker)
        results: list[AuditResult] = []
        for start in range(0, len(to_fetch), _MAX_BATCH_TICKERS):
            batch = to_fetch[start : start + _MAX_BATCH_TICKERS]
            results.extend(await self._audit_batch(batch))
        self._rounds += 1
        return tuple(results)

    async def run(
        self,
        *,
        interval_s: float,
        stop: asyncio.Event,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Run :meth:`audit_once` on a loop until ``stop`` is set.

        Args:
            interval_s: Seconds to wait between rounds.
            stop: Set (from elsewhere) to end the loop promptly: the wait between rounds and
                a round in progress are both cancelled when it fires. A batch cut off
                mid-window writes no audit records, and its book tap is still closed.
            sleep: Waits the given number of seconds; injected so tests control time without
                a real delay. Defaults to ``asyncio.sleep``.

        Raises:
            ValueError: If ``interval_s`` is negative.
        """
        if interval_s < 0:
            raise ValueError(f"interval_s must be non-negative, got {interval_s}")
        while not stop.is_set():
            # Every wait races the stop event, so a shutdown never waits out an audit
            # interval or a window's lead and settle. A round cancelled mid-batch writes
            # nothing for that batch, and its tap is still closed by _audit_batch.
            if not await _until_stopped(self.audit_once(), stop):
                return
            if not await _until_stopped(sleep(interval_s), stop):
                return

    async def _audit_batch(self, batch: Sequence[str]) -> tuple[AuditResult, ...]:
        """Audit one batch inside one tap window; the tap is closed on every path.

        Raises nothing but cancellation: a transport or HTTP failure for the whole batch is
        logged and the batch yields no results.
        """
        window_open_mono_ns = int(self._clock.mono_ns())
        tap = self._open_tap(batch, max_events=self._tap_max_events)
        try:
            fetched = await self._fetch_in_window(batch, window_open_mono_ns=window_open_mono_ns)
        finally:
            windows = tap.close()
        if fetched is None:
            return ()
        orderbooks, timing = fetched
        results: list[AuditResult] = []
        for entry in orderbooks:
            window = windows.get(entry.ticker)
            result = self._audit_entry(
                entry, TapWindow.absent(entry.ticker) if window is None else window, timing
            )
            if result is not None:
                results.append(result)
        return tuple(results)

    async def _fetch_in_window(
        self, batch: Sequence[str], *, window_open_mono_ns: int
    ) -> tuple[list[MarketOrderbookFp], _Timing] | None:
        """Wait the lead, fetch the batch, and wait the settle, stamping each boundary.

        Returns:
            The orderbooks and the window's timing, or ``None`` if the request failed, in
            which case there is nothing to settle.
        """
        await self._window_sleep(self._lead_ns / NS_PER_S)
        send_mono_ns, send_wall_ns = int(self._clock.mono_ns()), int(self._clock.wall_ns())
        try:
            orderbooks = await self._rest.orderbooks(batch)
        except (KalshiHttpError, KalshiTransportError) as exc:
            self._log.warning(
                "audit batch failed", extra={"batch_size": len(batch), "error": repr(exc)}
            )
            return None
        recv_mono_ns, recv_wall_ns = int(self._clock.mono_ns()), int(self._clock.wall_ns())
        await self._window_sleep(self._settle_ns / NS_PER_S)
        timing = _Timing(
            window_open_mono_ns=window_open_mono_ns,
            send_mono_ns=send_mono_ns,
            send_wall_ns=send_wall_ns,
            recv_mono_ns=recv_mono_ns,
            recv_wall_ns=recv_wall_ns,
            window_close_mono_ns=int(self._clock.mono_ns()),
        )
        return orderbooks, timing

    def _audit_entry(
        self, entry: MarketOrderbookFp, window: TapWindow, timing: _Timing
    ) -> AuditResult | None:
        """Classify one REST snapshot against its window, count it, and tape it.

        Returns:
            The result, or ``None`` if the REST snapshot itself was malformed or crossed:
            that ticker is logged and skipped rather than raised.
        """
        rest_book = self._rest_book(entry)
        if rest_book is None:
            return None
        verdict = self._classify(window, rest_book, reply_mono_ns=timing.recv_mono_ns)
        result = _result(window, rest_book, verdict, timing)
        self._count(result, window)
        self._write_record(result, BookImage.of(rest_book), verdict.reply_state)
        return result

    def _rest_book(self, entry: MarketOrderbookFp) -> Book | None:
        """Convert a REST snapshot into a YES-space book, or count it invalid and log it."""
        book = Book(entry.ticker)
        try:
            bids, asks = rest_orderbook_levels(entry.orderbook_fp)
            book.apply_snapshot(bids, asks, ts_ms=None)
        except (BookInvariantError, FixedPointError) as exc:
            self._log.warning(
                "audit rest snapshot invalid", extra={"ticker": entry.ticker, "error": repr(exc)}
            )
            self._books_invalid_rest += 1
            return None
        return book

    def _classify(self, window: TapWindow, rest_book: Book, *, reply_mono_ns: int) -> WindowVerdict:
        """Run :func:`classify_window`, treating a window that does not replay as undecidable."""
        try:
            return classify_window(window, rest_book, reply_mono_ns=reply_mono_ns)
        except BookInvariantError as exc:
            # A tap never records a history that does not replay, so this is a bug; it must
            # neither pass nor fail an audit.
            self._log.error(
                "audit window did not replay", extra={"ticker": window.ticker, "error": repr(exc)}
            )
            return _UNDECIDABLE

    def _count(self, result: AuditResult, window: TapWindow) -> None:
        self._outcomes[result.outcome] += 1
        if result.mismatched_levels is not None:
            self._levels_mismatched += result.mismatched_levels
        if result.outcome == "inconsistent":
            self._log.warning(
                "audit inconsistent with every book state in its window",
                extra={
                    "ticker": result.ticker,
                    "window_events": result.window_events,
                    "mismatched_levels": result.mismatched_levels,
                },
            )
        elif result.outcome == "undecidable":
            if window.fault == FAULT_NO_BOOK:
                self._books_missing_local += 1
            self._log.info(
                "audit undecidable", extra={"ticker": result.ticker, "fault": window.fault}
            )

    def _write_record(
        self, result: AuditResult, rest_levels: BookImage, reply_state: BookImage | None
    ) -> None:
        """Write ``result`` as an ``AUDIT`` record to its ticker's sink, if any."""
        sink = self._sink_for(result.ticker)
        if sink is None:
            return
        payload: dict[str, object] = {"ticker": result.ticker, "levels_rest": result.levels_rest}
        if result.outcome != "undecidable":
            payload["levels_local"] = result.levels_local
            payload["mismatched_levels"] = result.mismatched_levels
            payload["max_abs_diff_e2"] = result.max_abs_diff_e2
        payload |= {
            "outcome": result.outcome,
            "send_mono_ns": result.send_mono_ns,
            "send_wall_ns": result.send_wall_ns,
            "window_open_mono_ns": result.window_open_mono_ns,
            "window_close_mono_ns": result.window_close_mono_ns,
            "window_events": result.window_events,
        }
        if result.match_index is not None:
            payload["match_index"] = result.match_index
        if result.fault is not None:
            # The reason lives in the tape itself, so an undecidable audit is explainable
            # without the process's logs.
            payload["fault"] = result.fault
        if result.outcome == "inconsistent" and reply_state is not None:
            payload["rest_levels"] = _levels_json(rest_levels)
            payload["local_levels"] = _levels_json(reply_state)
        sink.put(
            Record(
                kind=RecordKind.AUDIT,
                conn_id=sink.conn_id,
                recv_mono_ns=result.recv_mono_ns,
                recv_wall_ns=result.recv_wall_ns,
                payload=msgspec.json.encode(payload),
            )
        )


def _result(
    window: TapWindow, rest_book: Book, verdict: WindowVerdict, timing: _Timing
) -> AuditResult:
    """Assemble an :class:`AuditResult`, comparing the REST book to the state as of the reply."""
    levels_local: int | None = None
    mismatched_levels: int | None = None
    max_abs_diff_e2: int | None = None
    if verdict.reply_state is not None:
        reply_state = verdict.reply_state
        differences = diff(rest_book, reply_state.to_book(window.ticker)).differences
        levels_local = len(reply_state.bids) + len(reply_state.asks)
        mismatched_levels = len(differences)
        max_abs_diff_e2 = max((abs(d.count_a - d.count_b) for d in differences), default=0)
    return AuditResult(
        ticker=window.ticker,
        outcome=verdict.outcome,
        window_open_mono_ns=timing.window_open_mono_ns,
        send_mono_ns=timing.send_mono_ns,
        send_wall_ns=timing.send_wall_ns,
        recv_mono_ns=timing.recv_mono_ns,
        recv_wall_ns=timing.recv_wall_ns,
        window_close_mono_ns=timing.window_close_mono_ns,
        window_events=len(window.events),
        match_index=verdict.match_index,
        levels_rest=rest_book.level_count(Side.BID) + rest_book.level_count(Side.ASK),
        levels_local=levels_local,
        mismatched_levels=mismatched_levels,
        max_abs_diff_e2=max_abs_diff_e2,
        fault=window.fault if verdict.outcome == "undecidable" else None,
    )


async def _until_stopped(work: Awaitable[object], stop: asyncio.Event) -> bool:
    """Await ``work`` unless ``stop`` fires first, in which case cancel it.

    Args:
        work: The awaitable to run, for example an audit round or a wait.
        stop: The event that ends the auditor's loop.

    Returns:
        ``True`` if the work finished; ``False`` if ``stop`` was already set or fired first.

    Raises:
        Exception: Whatever the work raised.
    """
    work_future = asyncio.ensure_future(work)
    if stop.is_set():
        work_future.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await work_future
        return False
    stop_future = asyncio.ensure_future(stop.wait())
    try:
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
