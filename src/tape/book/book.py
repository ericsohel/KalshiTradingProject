"""A single market's order book in YES space, maintained from snapshots and deltas.

Invariants (checked in code, not only in tests):
    * every resting count is positive; a level with zero count does not exist;
    * ``best_bid < best_ask`` whenever both sides are non-empty;
    * a snapshot replaces every prior level;
    * a book is *stale* from construction until its first snapshot, and again after any
      invariant violation, until the next snapshot. Deltas applied to a stale book are
      ignored, because their base is unknown.

The structure is a mutable, single-owner value; it is not thread-safe. Level storage
is a dictionary per side keyed by ``PriceE4`` with cached best pointers; the best
pointer is recomputed in O(levels) only when the best level itself is removed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Final

import msgspec

from tape.errors import BookInvariantError
from tape.events import Level, Side
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import Ms, Ns

__all__ = ["Book", "BookDiff", "KeyframeRow", "LevelDiff", "books_from_keyframe_rows", "diff"]

_MASK64: Final = (1 << 64) - 1
_EMPTY_SIDE: Final = -1


class KeyframeRow(msgspec.Struct, frozen=True, kw_only=True):
    """One row of a keyframe file (docs/DATA_FORMATS.md 5).

    ``side`` is ``Side.BID``/``Side.ASK`` as an int, or ``-1`` for the single row that
    records an empty book, so emptiness is distinguishable from absence.
    """

    ticker: str
    side: int
    price_e4: int
    count_e2: int
    as_of_recv_ns: int
    last_ts_ms: int | None
    stale: bool


class LevelDiff(msgspec.Struct, frozen=True, kw_only=True):
    """A level present or sized differently in two books."""

    side: Side
    price: PriceE4
    count_a: CountE2
    count_b: CountE2


class BookDiff(msgspec.Struct, frozen=True, kw_only=True):
    """Result of comparing two books level by level."""

    ticker: str
    differences: tuple[LevelDiff, ...]

    @property
    def is_empty(self) -> bool:
        """True when the two books have identical levels."""
        return not self.differences


def _mix64(value: int) -> int:
    """Finalizer of splitmix64: a cheap, well-distributed 64-bit integer hash."""
    value &= _MASK64
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & _MASK64
    return value ^ (value >> 31)


def _level_key(side: Side, price: int, count: int) -> int:
    """Pack a level into one integer for hashing (side: 1 bit, price: 19 bits, count: 44 bits)."""
    return (int(side) << 63) | ((price & 0x7FFFF) << 44) | (count & ((1 << 44) - 1))


class Book:
    """Order book for one market. See the module docstring for invariants."""

    __slots__ = ("_asks", "_best_ask", "_best_bid", "_bids", "_stale", "last_ts_ms", "ticker")

    def __init__(self, ticker: str) -> None:
        self.ticker = ticker
        self._bids: dict[int, int] = {}
        self._asks: dict[int, int] = {}
        self._best_bid: int | None = None
        self._best_ask: int | None = None
        self._stale = True
        self.last_ts_ms: Ms | None = None

    # ----------------------------------------------------------------- mutation

    def apply_snapshot(
        self, bids: Iterable[Level], asks: Iterable[Level], *, ts_ms: Ms | None
    ) -> None:
        """Replace every level with the given ones and clear the stale flag.

        Args:
            bids: YES bids. Prices must be unique; counts must be positive.
            asks: YES asks (NO bids on the YES scale). Same constraints.
            ts_ms: Exchange time of the snapshot, if known.

        Raises:
            BookInvariantError: On a non-positive count, a duplicate price, or a
                crossed book. The book is left stale and unchanged.
        """
        new_bids = self._collect(bids, "bid")
        new_asks = self._collect(asks, "ask")
        best_bid = max(new_bids) if new_bids else None
        best_ask = min(new_asks) if new_asks else None
        if best_bid is not None and best_ask is not None and best_bid >= best_ask:
            self._stale = True
            raise BookInvariantError(
                self.ticker, f"crossed snapshot: bid {best_bid} >= ask {best_ask}"
            )
        self._bids = new_bids
        self._asks = new_asks
        self._best_bid = best_bid
        self._best_ask = best_ask
        self._stale = False
        self.last_ts_ms = ts_ms

    def apply_delta(self, side: Side, price: PriceE4, delta: int, *, ts_ms: Ms | None) -> bool:
        """Add ``delta`` (signed, in ``CountE2`` units) to one level.

        Returns:
            ``True`` if the delta was applied; ``False`` if the book is stale and the
            delta was ignored.

        Raises:
            BookInvariantError: If the level would become negative or the book would
                cross. The book is marked stale before raising.
        """
        if self._stale:
            return False
        levels = self._bids if side is Side.BID else self._asks
        current = levels.get(price, 0)
        updated = current + delta
        if updated < 0:
            self._stale = True
            raise BookInvariantError(
                self.ticker,
                f"{side.name.lower()} level {price} would go negative: {current} + {delta}",
            )
        if updated == 0:
            levels.pop(price, None)
            self._drop_best_if_removed(side, price)
        else:
            levels[price] = updated
            self._raise_best_if_better(side, price)
        if (
            self._best_bid is not None
            and self._best_ask is not None
            and self._best_bid >= self._best_ask
        ):
            self._stale = True
            raise BookInvariantError(
                self.ticker, f"crossed after delta: bid {self._best_bid} >= ask {self._best_ask}"
            )
        if ts_ms is not None:
            self.last_ts_ms = ts_ms
        return True

    def mark_stale(self) -> None:
        """Declare the book unreliable until the next snapshot (gap or disconnect)."""
        self._stale = True

    # ------------------------------------------------------------------ queries

    def is_stale(self) -> bool:
        """True while the book awaits a snapshot."""
        return self._stale

    def best_bid(self) -> Level | None:
        """Highest YES bid, or ``None`` when the bid side is empty."""
        if self._best_bid is None:
            return None
        return Level(PriceE4(self._best_bid), CountE2(self._bids[self._best_bid]))

    def best_ask(self) -> Level | None:
        """Lowest YES ask, or ``None`` when the ask side is empty."""
        if self._best_ask is None:
            return None
        return Level(PriceE4(self._best_ask), CountE2(self._asks[self._best_ask]))

    def size_at(self, side: Side, price: PriceE4) -> CountE2:
        """Resting count at a price on a side; zero when the level does not exist."""
        levels = self._bids if side is Side.BID else self._asks
        return CountE2(levels.get(price, 0))

    def depth(self, side: Side, n: int) -> list[Level]:
        """Up to ``n`` levels on a side, best first.

        Raises:
            ValueError: If ``n`` is negative.
        """
        if n < 0:
            raise ValueError("depth must be non-negative")
        return self.levels(side)[:n]

    def levels(self, side: Side) -> list[Level]:
        """All levels on a side, best first (bids descending, asks ascending)."""
        if side is Side.BID:
            return [
                Level(PriceE4(p), CountE2(c)) for p, c in sorted(self._bids.items(), reverse=True)
            ]
        return [Level(PriceE4(p), CountE2(c)) for p, c in sorted(self._asks.items())]

    def level_count(self, side: Side) -> int:
        """Number of non-empty levels on a side."""
        return len(self._bids if side is Side.BID else self._asks)

    def checksum(self) -> int:
        """Order-independent 64-bit checksum over every level.

        Equal books have equal checksums; changing any single level's count or
        presence changes the value with overwhelming probability. Used by audits and
        determinism tests; not a cryptographic hash.
        """
        total = 0
        for price, count in self._bids.items():
            total += _mix64(_level_key(Side.BID, price, count))
        for price, count in self._asks.items():
            total += _mix64(_level_key(Side.ASK, price, count))
        return total & _MASK64

    def to_keyframe(self, *, as_of_recv_ns: Ns) -> list[KeyframeRow]:
        """Serialize every level as keyframe rows; an empty book yields one ``side=-1`` row."""
        last_ts_ms = None if self.last_ts_ms is None else int(self.last_ts_ms)

        def row(side: int, price: int, count: int) -> KeyframeRow:
            return KeyframeRow(
                ticker=self.ticker,
                side=side,
                price_e4=price,
                count_e2=count,
                as_of_recv_ns=int(as_of_recv_ns),
                last_ts_ms=last_ts_ms,
                stale=self._stale,
            )

        rows = [row(int(Side.BID), p, c) for p, c in self._bids.items()]
        rows.extend(row(int(Side.ASK), p, c) for p, c in self._asks.items())
        if not rows:
            rows.append(row(_EMPTY_SIDE, 0, 0))
        return rows

    # ----------------------------------------------------------------- internals

    def _collect(self, levels: Iterable[Level], side_name: str) -> dict[int, int]:
        out: dict[int, int] = {}
        for level in levels:
            if level.count <= 0:
                self._stale = True
                raise BookInvariantError(
                    self.ticker, f"{side_name} level {level.price} has non-positive count"
                )
            if level.price in out:
                self._stale = True
                raise BookInvariantError(self.ticker, f"duplicate {side_name} price {level.price}")
            out[level.price] = level.count
        return out

    def _raise_best_if_better(self, side: Side, price: int) -> None:
        if side is Side.BID:
            if self._best_bid is None or price > self._best_bid:
                self._best_bid = price
        elif self._best_ask is None or price < self._best_ask:
            self._best_ask = price

    def _drop_best_if_removed(self, side: Side, price: int) -> None:
        if side is Side.BID:
            if self._best_bid == price:
                self._best_bid = max(self._bids) if self._bids else None
        elif self._best_ask == price:
            self._best_ask = min(self._asks) if self._asks else None


def diff(a: Book, b: Book) -> BookDiff:
    """Compare two books of the same market level by level.

    Raises:
        ValueError: If the books belong to different markets.
    """
    if a.ticker != b.ticker:
        raise ValueError(f"cannot diff books for {a.ticker!r} and {b.ticker!r}")
    differences: list[LevelDiff] = []
    for side in (Side.BID, Side.ASK):
        levels_a: Mapping[int, int] = {lvl.price: lvl.count for lvl in a.levels(side)}
        levels_b: Mapping[int, int] = {lvl.price: lvl.count for lvl in b.levels(side)}
        for price in sorted(set(levels_a) | set(levels_b)):
            count_a = levels_a.get(price, 0)
            count_b = levels_b.get(price, 0)
            if count_a != count_b:
                differences.append(
                    LevelDiff(
                        side=side,
                        price=PriceE4(price),
                        count_a=CountE2(count_a),
                        count_b=CountE2(count_b),
                    )
                )
    return BookDiff(ticker=a.ticker, differences=tuple(differences))


def books_from_keyframe_rows(rows: Iterable[KeyframeRow]) -> dict[str, Book]:
    """Rebuild books from keyframe rows, grouped by ticker.

    Raises:
        BookInvariantError: If rows for one ticker violate book invariants.
    """
    grouped: dict[str, list[KeyframeRow]] = {}
    for row in rows:
        grouped.setdefault(row.ticker, []).append(row)
    books: dict[str, Book] = {}
    for ticker, ticker_rows in grouped.items():
        book = Book(ticker)
        bids = [
            Level(PriceE4(r.price_e4), CountE2(r.count_e2))
            for r in ticker_rows
            if r.side == int(Side.BID)
        ]
        asks = [
            Level(PriceE4(r.price_e4), CountE2(r.count_e2))
            for r in ticker_rows
            if r.side == int(Side.ASK)
        ]
        first = ticker_rows[0]
        book.apply_snapshot(
            bids, asks, ts_ms=None if first.last_ts_ms is None else Ms(first.last_ts_ms)
        )
        if first.stale:
            book.mark_stale()
        books[ticker] = book
    return books
