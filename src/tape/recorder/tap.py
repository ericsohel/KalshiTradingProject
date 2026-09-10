"""Observe, over a bounded window, every book change a supervisor applies to chosen markets.

Responsibility: give the auditor every state a local book passed through while a REST order
book was in flight (ADR 0021). A ``ConnectionSupervisor`` opens a :class:`LiveBookTap` over
some of its markets: the tap copies each market's book as it opens, then keeps, in order,
every snapshot and delta the supervisor applies to that market until it is closed. A
:class:`CompositeBookTap` closes the taps of several connections as one. Closing either
yields one frozen :class:`TapWindow` per market. The auditor depends only on the
:class:`BookTap` and :class:`BookTapOpener` ports and the data types defined here.

Invariants: a window without a fault holds a copy of a fresh book taken as the tap opened and
every change applied to that book until the tap closed, in application order, so replaying
its events from the copy passes through every state the local book held in between; any
window that cannot promise that carries a fault instead, never a partial history presented as
whole: ``no_book`` when no fresh book existed at opening, ``stale`` when the book went stale
or disappeared before closing, and ``overflow`` when more than ``max_events`` changes arrived;
a closed tap records nothing, holds no book, and returns the same windows however often it is
closed.

Stale detection rests on a supervisor invariant (docs/INTERFACES.md 8.4): a book leaves the
stale state only through a snapshot, and a book the supervisor creates starts stale. A book
that went stale inside the window is therefore either stale or gone when the tap closes, or
was stale just before some snapshot applied inside the window, which the supervisor reports
through :meth:`LiveBookTap.record`. No other stale transition needs a hook.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Final, Literal, Protocol

import msgspec

from tape.book import Book
from tape.events import BookDelta, BookSnapshot, Level, Side

__all__ = [
    "FAULT_NO_BOOK",
    "FAULT_OVERFLOW",
    "FAULT_STALE",
    "BookChange",
    "BookImage",
    "BookTap",
    "BookTapOpener",
    "CompositeBookTap",
    "LiveBookTap",
    "TapFault",
    "TapWindow",
]

type BookChange = BookSnapshot | BookDelta
"""A change a supervisor applies to a book."""

type TapFault = Literal["no_book", "stale", "overflow"]
"""Why a window cannot vouch for every state its book held."""

FAULT_NO_BOOK: Final = "no_book"
"""No fresh local book existed when the tap opened."""

FAULT_STALE: Final = "stale"
"""The book went stale, or was dropped, before the tap closed."""

FAULT_OVERFLOW: Final = "overflow"
"""More changes arrived than the tap was allowed to hold."""


class BookImage(msgspec.Struct, frozen=True, kw_only=True):
    """An immutable copy of a book's levels, best first on each side.

    Attributes:
        bids: YES bids, highest price first.
        asks: YES asks, lowest price first.
    """

    bids: tuple[Level, ...]
    asks: tuple[Level, ...]

    @classmethod
    def of(cls, book: Book) -> BookImage:
        """Copy the levels of ``book`` as they stand now.

        Args:
            book: The book to copy; its stale flag is not part of the image.

        Returns:
            The copy.
        """
        return cls(bids=tuple(book.levels(Side.BID)), asks=tuple(book.levels(Side.ASK)))

    def to_book(self, ticker: str) -> Book:
        """Build a fresh book holding exactly these levels.

        Args:
            ticker: Market the book is for.

        Returns:
            A book that is not stale.

        Raises:
            BookInvariantError: If the levels are crossed, repeat a price, or hold a
                non-positive count, which no image of a valid book does.
        """
        book = Book(ticker)
        book.apply_snapshot(self.bids, self.asks, ts_ms=None)
        return book


class TapWindow(msgspec.Struct, frozen=True, kw_only=True):
    """What one tap saw of one market between opening and closing.

    Attributes:
        ticker: The market.
        start: The book's levels when the tap opened; ``None`` exactly when the fault is
            ``no_book``.
        events: Changes applied to the book while the tap was open, in application order,
            each with its receipt. Receive times never decrease along the tuple while there
            is no fault, because one connection delivers them and a reconnect stales the
            book. At most the tap's ``max_events``; empty when the fault is ``no_book``.
        fault: Why the window cannot vouch for every state the book held, or ``None``.

    Raises:
        ValueError: If ``start`` is absent without a ``no_book`` fault, present with one, or
            a ``no_book`` window holds events.
    """

    ticker: str
    start: BookImage | None
    events: tuple[BookChange, ...]
    fault: TapFault | None

    def __post_init__(self) -> None:
        if (self.start is None) != (self.fault == FAULT_NO_BOOK):
            raise ValueError(
                f"window for {self.ticker}: a start copy must be absent exactly when the fault "
                f"is {FAULT_NO_BOOK}, got fault {self.fault}"
            )
        if self.start is None and self.events:
            raise ValueError(
                f"window for {self.ticker}: no start copy but {len(self.events)} events"
            )

    @classmethod
    def absent(cls, ticker: str) -> TapWindow:
        """The window of a market that had no fresh local book when the tap opened.

        Args:
            ticker: The market.

        Returns:
            A window with no start copy, no events, and the ``no_book`` fault.
        """
        return cls(ticker=ticker, start=None, events=(), fault=FAULT_NO_BOOK)


class BookTap(Protocol):
    """An open observation of book changes, as the auditor holds it."""

    def close(self) -> Mapping[str, TapWindow]:
        """Stop observing and release everything held; idempotent.

        Returns:
            One window per market the tap was opened for; every call returns the same.
        """
        ...


class BookTapOpener(Protocol):
    """Opens a tap over markets wherever their books live."""

    def __call__(self, tickers: Collection[str], *, max_events: int) -> BookTap:
        """Open a tap that holds at most ``max_events`` changes per market.

        Raises:
            ValueError: If ``max_events`` is not positive.
        """
        ...


@dataclass(slots=True)
class _Recording:
    """One market's recording inside an open tap.

    Attributes:
        book: The book copied at opening, kept to notice it being dropped; ``None`` when
            there was no fresh book.
        start: The copy of that book.
        events: Changes recorded so far.
        fault: The first fault observed, after which nothing more is recorded.
    """

    book: Book | None
    start: BookImage | None
    events: list[BookChange] = field(default_factory=list)
    fault: TapFault | None = None


class LiveBookTap:
    """A supervisor's tap over some of its books. See the module docstring.

    Built by ``ConnectionSupervisor.open_tap``, which calls :meth:`record` after each change it
    applies to a tapped market. Not thread-safe; used on the supervisor's event loop.

    Args:
        tickers: Markets to observe; duplicates are ignored.
        books: The supervisor's live books by ticker, read at opening and at closing.
        max_events: Most changes held per market before the window faults with ``overflow``.
        on_close: Called once, on the first :meth:`close`, so the supervisor stops routing
            changes here.

    Raises:
        ValueError: If ``max_events`` is not positive.
    """

    def __init__(
        self,
        tickers: Iterable[str],
        *,
        books: Mapping[str, Book],
        max_events: int,
        on_close: Callable[[LiveBookTap], None],
    ) -> None:
        if max_events <= 0:
            raise ValueError(f"max_events must be positive, got {max_events}")
        self._tickers = frozenset(tickers)
        self._books = books
        self._max_events = max_events
        self._on_close = on_close
        self._recordings = {ticker: self._open(ticker) for ticker in sorted(self._tickers)}
        self._windows: Mapping[str, TapWindow] | None = None

    @property
    def tickers(self) -> frozenset[str]:
        """The markets this tap was opened for."""
        return self._tickers

    @property
    def closed(self) -> bool:
        """Whether :meth:`close` has been called."""
        return self._windows is not None

    def record(self, change: BookChange, *, was_stale: bool) -> None:
        """Record a change the supervisor has just applied to a book.

        Ignored when the tap is closed, the market is not tapped, or its window already
        faulted.

        Args:
            change: The snapshot or delta, as applied.
            was_stale: Whether the book was stale just before the change was applied, which
                only a snapshot can follow; the window then faults with ``stale``.
        """
        recording = self._recordings.get(change.ticker)
        if recording is None or recording.fault is not None:
            return
        if was_stale:
            recording.fault = FAULT_STALE
        elif len(recording.events) >= self._max_events:
            recording.fault = FAULT_OVERFLOW
        else:
            recording.events.append(change)

    def close(self) -> Mapping[str, TapWindow]:
        """Stop recording, release every book and buffer, and return the windows; idempotent.

        Returns:
            One window per tapped market; every call returns the same mapping.
        """
        if self._windows is None:
            self._windows = MappingProxyType(
                {ticker: self._finish(ticker, rec) for ticker, rec in self._recordings.items()}
            )
            self._recordings = {}
            self._on_close(self)
        return self._windows

    def _open(self, ticker: str) -> _Recording:
        book = self._books.get(ticker)
        if book is None or book.is_stale():
            return _Recording(book=None, start=None, fault=FAULT_NO_BOOK)
        return _Recording(book=book, start=BookImage.of(book))

    def _finish(self, ticker: str, recording: _Recording) -> TapWindow:
        fault = recording.fault
        current = self._books.get(ticker)
        if fault is None and (
            current is None or current is not recording.book or current.is_stale()
        ):
            fault = FAULT_STALE
        return TapWindow(
            ticker=ticker, start=recording.start, events=tuple(recording.events), fault=fault
        )


class CompositeBookTap:
    """Several taps closed as one, plus markets no tap covers.

    Args:
        taps: The taps to close together; they must cover disjoint markets.
        uncovered: Markets with no book anywhere; each gets :meth:`TapWindow.absent`.
    """

    def __init__(self, taps: Sequence[BookTap], *, uncovered: Iterable[str] = ()) -> None:
        self._taps = tuple(taps)
        self._uncovered = frozenset(uncovered)
        self._windows: Mapping[str, TapWindow] | None = None

    def close(self) -> Mapping[str, TapWindow]:
        """Close every tap and merge their windows; idempotent.

        Returns:
            One window per market of every tap and every uncovered market; every call returns
            the same mapping.
        """
        if self._windows is None:
            windows = {ticker: TapWindow.absent(ticker) for ticker in sorted(self._uncovered)}
            for tap in self._taps:
                windows.update(tap.close())
            self._windows = MappingProxyType(windows)
            self._taps = ()
        return self._windows
