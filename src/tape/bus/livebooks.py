"""Rebuild a publisher's books from a lossy bus: the consumer rule of ADR 0022.

Responsibility: hold, for a bus consumer such as ``tape serve``, a copy of every book the
recorder publishes, and say which copies can be trusted. :meth:`LiveBooks.observe` takes one
decoded :class:`tape.bus.envelope.BusEnvelope` at a time, in arrival order, and reports what
the message changed. Nothing here performs I/O or reads a clock.

The rule. The consumer follows the publisher's ``bus_epoch`` and last ``bus_seq``. At its first
message, at a new epoch, and at any number that is not the next one, every book becomes
*unknown* and is dropped, because a lost message may have changed any of them. A book becomes
known at its next :class:`tape.events.BookRefresh`, which replaces it whole; snapshots and
deltas with higher numbers then apply to it. A known book is *fresh*, or *stale* when the image
it came from said so; like the publisher's, a stale book ignores deltas until a snapshot. Book
messages for an unknown book are ignored.

Invariants: a book is held exactly when its status is fresh or stale; while every message since
a book's refresh image has been observed with contiguous numbers, a book reported stale is stale
at the publisher, and a fresh book has the publisher's levels as of the last message observed
whenever the publisher's book is fresh too (the publisher can go stale without a message, after
a disconnect, and says so at the next image); every status change is reported once, by the
observation of the message that caused it.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final, Literal

import msgspec

from tape.book import Book
from tape.bus.envelope import FIRST_BUS_SEQ, BusEnvelope
from tape.errors import BookInvariantError
from tape.events import BookDelta, BookRefresh, BookSnapshot, BusEvent

__all__ = [
    "BOOK_FRESH",
    "BOOK_STALE",
    "BOOK_UNKNOWN",
    "RESET_EPOCH",
    "RESET_GAP",
    "RESET_START",
    "BookStatus",
    "LiveBooks",
    "LiveBooksStats",
    "Observation",
    "ResetReason",
    "StatusChange",
]

type BookStatus = Literal["unknown", "fresh", "stale"]
"""How far a consumer can trust its copy of one book."""

BOOK_UNKNOWN: Final = "unknown"
"""No copy is held: none has arrived since the consumer started or last lost a message."""

BOOK_FRESH: Final = "fresh"
"""The copy follows the publisher's book."""

BOOK_STALE: Final = "stale"
"""The copy is the publisher's book, which awaits a snapshot from the exchange."""

type ResetReason = Literal["start", "epoch", "gap"]
"""Why every book was declared unknown."""

RESET_START: Final = "start"
"""The first message this consumer observed."""

RESET_EPOCH: Final = "epoch"
"""The publisher restarted: the message carries a ``bus_epoch`` not seen before."""

RESET_GAP: Final = "gap"
"""The message's ``bus_seq`` is not the one after the previous message's."""


class StatusChange(msgspec.Struct, frozen=True, kw_only=True):
    """One book's status before and after a message.

    Attributes:
        ticker: The market.
        before: Status before the message.
        after: Status after it; never equal to ``before``.
    """

    ticker: str
    before: BookStatus
    after: BookStatus


class Observation(msgspec.Struct, frozen=True, kw_only=True):
    """What one message did to a consumer's books.

    Attributes:
        reset: Why every book was declared unknown before the message's event was considered,
            or ``None`` when the message followed the previous one.
        missed: Messages known to be lost just before this one: the numbers a gap skipped, or
            the numbers before this one in a new epoch. Zero on a consumer's first message,
            whose history it never had.
        applied: Whether the event reached a held book: a refresh image adopted, or a snapshot
            or delta applied. Always ``False`` for events that are not about books, which a
            consumer forwards regardless of any book's status.
        changes: Every status change the message caused, those of the reset first, each set
            in ticker order.
    """

    reset: ResetReason | None
    missed: int
    applied: bool
    changes: tuple[StatusChange, ...]


class LiveBooksStats(msgspec.Struct, frozen=True, kw_only=True):
    """Counters since the consumer started.

    Attributes:
        messages: Messages observed.
        resets: Times every book was declared unknown, the first message included.
        missed: Messages known to be lost, summed over every observation.
        refreshes: Refresh images adopted.
        ignored: Snapshots and deltas that reached no book: unknown, or stale for a delta.
        book_errors: Images, snapshots, or deltas that broke a book invariant; the book was
            dropped, because the copy had diverged from the publisher's.
    """

    messages: int
    resets: int
    missed: int
    refreshes: int
    ignored: int
    book_errors: int


class LiveBooks:
    """A consumer's copies of the publisher's books. See the module docstring.

    Mutable and single-owner; not thread-safe.
    """

    def __init__(self) -> None:
        self._books: dict[str, Book] = {}
        self._epoch: int | None = None
        self._last_seq: int | None = None
        self._messages = 0
        self._resets = 0
        self._missed = 0
        self._refreshes = 0
        self._ignored = 0
        self._book_errors = 0

    # ------------------------------------------------------------------- read-only views

    @property
    def epoch(self) -> int | None:
        """The ``bus_epoch`` of the latest message; ``None`` before the first."""
        return self._epoch

    @property
    def last_seq(self) -> int | None:
        """The ``bus_seq`` of the latest message; ``None`` before the first."""
        return self._last_seq

    @property
    def stats(self) -> LiveBooksStats:
        """Current counters; see :class:`LiveBooksStats`."""
        return LiveBooksStats(
            messages=self._messages,
            resets=self._resets,
            missed=self._missed,
            refreshes=self._refreshes,
            ignored=self._ignored,
            book_errors=self._book_errors,
        )

    def status(self, ticker: str) -> BookStatus:
        """How far the copy of one market's book can be trusted.

        Args:
            ticker: Market ticker; any string.

        Returns:
            ``"unknown"`` when no copy is held, otherwise ``"stale"`` or ``"fresh"``.
        """
        book = self._books.get(ticker)
        if book is None:
            return BOOK_UNKNOWN
        return BOOK_STALE if book.is_stale() else BOOK_FRESH

    def books(self) -> Mapping[str, Book]:
        """Every held copy, fresh or stale, by ticker.

        Returns:
            A read-only mapping of live books that later observations mutate; read them, never
            change them.
        """
        return MappingProxyType(self._books)

    # ------------------------------------------------------------------------ updates

    def observe(self, envelope: BusEnvelope) -> Observation:
        """Account for one message, in arrival order.

        Args:
            envelope: The decoded message.

        Returns:
            What the message did; see :class:`Observation`.
        """
        self._messages += 1
        changes: list[StatusChange] = []
        reset, missed = self._follow(envelope)
        if reset is not None:
            self._resets += 1
            self._missed += missed
            changes.extend(
                StatusChange(ticker=ticker, before=self.status(ticker), after=BOOK_UNKNOWN)
                for ticker in sorted(self._books)
            )
            self._books.clear()
        applied = self._apply(envelope.event, changes)
        return Observation(reset=reset, missed=missed, applied=applied, changes=tuple(changes))

    def _follow(self, envelope: BusEnvelope) -> tuple[ResetReason | None, int]:
        """Adopt the message's numbers and say whether, and why, the books must be reset.

        Returns:
            The reset reason or ``None``, and the number of messages known to be lost.
        """
        epoch, seq = envelope.bus_epoch, envelope.bus_seq
        previous_epoch, previous_seq = self._epoch, self._last_seq
        self._epoch, self._last_seq = epoch, seq
        if previous_epoch is None or previous_seq is None:
            return RESET_START, 0
        if epoch != previous_epoch:
            return RESET_EPOCH, seq - FIRST_BUS_SEQ
        if seq != previous_seq + 1:
            # A number that does not advance cannot come from ZeroMQ PUB/SUB, which neither
            # duplicates nor reorders; it is treated as loss because nothing else is safe.
            return RESET_GAP, max(seq - previous_seq - 1, 0)
        return None, 0

    def _apply(self, event: BusEvent, changes: list[StatusChange]) -> bool:
        """Apply a book event to the copy it concerns, recording any status change."""
        if not isinstance(event, BookRefresh | BookSnapshot | BookDelta):
            return False
        before = self.status(event.ticker)
        applied = self._adopt(event) if isinstance(event, BookRefresh) else self._change(event)
        after = self.status(event.ticker)
        if after != before:
            changes.append(StatusChange(ticker=event.ticker, before=before, after=after))
        return applied

    def _adopt(self, refresh: BookRefresh) -> bool:
        """Replace a copy with a refresh image; an image no book can hold drops the copy."""
        book = Book(refresh.ticker)
        try:
            book.apply_snapshot(refresh.bids, refresh.asks, ts_ms=refresh.ts_ms)
        except BookInvariantError:
            self._book_errors += 1
            self._books.pop(refresh.ticker, None)
            return False
        if refresh.stale:
            book.mark_stale()
        self._books[refresh.ticker] = book
        self._refreshes += 1
        return True

    def _change(self, change: BookSnapshot | BookDelta) -> bool:
        """Apply a snapshot or delta to a held copy; a broken invariant drops the copy."""
        book = self._books.get(change.ticker)
        if book is None:
            self._ignored += 1
            return False
        try:
            if isinstance(change, BookSnapshot):
                book.apply_snapshot(change.bids, change.asks, ts_ms=change.ts_ms)
                applied = True
            else:
                applied = book.apply_delta(
                    change.side, change.price, change.delta, ts_ms=change.ts_ms
                )
        except BookInvariantError:
            self._book_errors += 1
            del self._books[change.ticker]
            return False
        if not applied:
            self._ignored += 1
        return applied
