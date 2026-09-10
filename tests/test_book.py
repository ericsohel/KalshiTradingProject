"""Order book invariants, best-pointer maintenance, checksums, and keyframes."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.book import Book, books_from_keyframe_rows, diff
from tape.errors import BookInvariantError
from tape.events import Level, Side
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import Ms, Ns


def lvl(price: int, count: int) -> Level:
    return Level(PriceE4(price), CountE2(count))


def make_book() -> Book:
    book = Book("X")
    book.apply_snapshot(
        [lvl(4000, 100), lvl(4500, 50)], [lvl(5000, 70), lvl(5500, 10)], ts_ms=Ms(1)
    )
    return book


def test_new_book_is_stale_and_ignores_deltas() -> None:
    book = Book("X")
    assert book.is_stale()
    assert book.apply_delta(Side.BID, PriceE4(4000), 100, ts_ms=None) is False
    assert book.best_bid() is None


def test_snapshot_sets_best_levels_and_clears_stale() -> None:
    book = make_book()
    assert not book.is_stale()
    assert book.best_bid() == lvl(4500, 50)
    assert book.best_ask() == lvl(5000, 70)
    assert book.depth(Side.BID, 5) == [lvl(4500, 50), lvl(4000, 100)]
    assert book.depth(Side.ASK, 1) == [lvl(5000, 70)]
    assert book.size_at(Side.BID, PriceE4(4000)) == 100
    assert book.size_at(Side.ASK, PriceE4(1)) == 0
    assert book.level_count(Side.ASK) == 2
    assert book.last_ts_ms == 1


def test_snapshot_rejects_bad_levels() -> None:
    book = Book("X")
    with pytest.raises(BookInvariantError):
        book.apply_snapshot([lvl(4000, 0)], [], ts_ms=None)
    with pytest.raises(BookInvariantError):
        book.apply_snapshot([lvl(4000, 1), lvl(4000, 2)], [], ts_ms=None)
    with pytest.raises(BookInvariantError):
        book.apply_snapshot([lvl(5000, 1)], [lvl(5000, 1)], ts_ms=None)
    assert book.is_stale()


def test_delta_adds_removes_and_moves_best() -> None:
    book = make_book()
    assert book.apply_delta(Side.BID, PriceE4(4700), 5, ts_ms=Ms(2))
    assert book.best_bid() == lvl(4700, 5)
    assert book.apply_delta(Side.BID, PriceE4(4700), -5, ts_ms=Ms(3))
    assert book.best_bid() == lvl(4500, 50)
    assert book.apply_delta(Side.ASK, PriceE4(5000), -70, ts_ms=Ms(4))
    assert book.best_ask() == lvl(5500, 10)
    assert book.apply_delta(Side.ASK, PriceE4(5500), -10, ts_ms=Ms(5))
    assert book.best_ask() is None
    assert book.last_ts_ms == 5


def test_delta_below_zero_marks_stale() -> None:
    book = make_book()
    with pytest.raises(BookInvariantError):
        book.apply_delta(Side.BID, PriceE4(4000), -101, ts_ms=None)
    assert book.is_stale()
    assert book.size_at(Side.BID, PriceE4(4000)) == 100


def test_crossing_delta_marks_stale() -> None:
    book = make_book()
    with pytest.raises(BookInvariantError):
        book.apply_delta(Side.BID, PriceE4(5000), 1, ts_ms=None)
    assert book.is_stale()


def test_depth_rejects_negative() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        make_book().depth(Side.BID, -1)


def test_diff_reports_differences_only() -> None:
    a = make_book()
    b = make_book()
    assert diff(a, b).is_empty
    b.apply_delta(Side.BID, PriceE4(4000), -1, ts_ms=None)
    b.apply_delta(Side.ASK, PriceE4(6000), 3, ts_ms=None)
    result = diff(a, b)
    assert [(d.side, d.price, d.count_a, d.count_b) for d in result.differences] == [
        (Side.BID, 4000, 100, 99),
        (Side.ASK, 6000, 0, 3),
    ]
    with pytest.raises(ValueError, match="cannot diff"):
        diff(a, Book("Y"))


def test_keyframe_round_trip_including_empty_book() -> None:
    book = make_book()
    rows = book.to_keyframe(as_of_recv_ns=Ns(99))
    empty = Book("E")
    empty_rows = empty.to_keyframe(as_of_recv_ns=Ns(99))
    assert len(empty_rows) == 1
    assert empty_rows[0].side == -1
    rebuilt = books_from_keyframe_rows(rows + empty_rows)
    assert diff(rebuilt["X"], book).is_empty
    assert rebuilt["X"].checksum() == book.checksum()
    assert rebuilt["X"].last_ts_ms == 1
    assert not rebuilt["X"].is_stale()
    assert rebuilt["E"].is_stale()
    assert rebuilt["E"].best_bid() is None


levels_strategy = st.lists(
    st.tuples(st.integers(0, 10_000), st.integers(1, 10**9)),
    unique_by=lambda t: t[0],
    max_size=40,
)


@given(levels_strategy, levels_strategy)
@settings(max_examples=200)
def test_checksum_is_order_independent_and_sensitive(
    bids: list[tuple[int, int]], asks: list[tuple[int, int]]
) -> None:
    bid_max = max((p for p, _ in bids), default=-1)
    asks = [(p, c) for p, c in asks if p > bid_max]
    a = Book("X")
    a.apply_snapshot([lvl(p, c) for p, c in bids], [lvl(p, c) for p, c in asks], ts_ms=None)
    b = Book("X")
    b.apply_snapshot(
        [lvl(p, c) for p, c in reversed(bids)], [lvl(p, c) for p, c in reversed(asks)], ts_ms=None
    )
    assert a.checksum() == b.checksum()
    if bids:
        price = bids[0][0]
        b.apply_delta(Side.BID, PriceE4(price), 1, ts_ms=None)
        assert a.checksum() != b.checksum()
        b.apply_delta(Side.BID, PriceE4(price), -1, ts_ms=None)
        assert a.checksum() == b.checksum()


@given(
    levels_strategy,
    st.lists(st.tuples(st.integers(0, 10_000), st.integers(-50, 50)), max_size=60),
)
@settings(max_examples=200)
def test_deltas_then_snapshot_equals_snapshot_alone(
    bids: list[tuple[int, int]], deltas: list[tuple[int, int]]
) -> None:
    reference = Book("X")
    reference.apply_snapshot([lvl(p, c) for p, c in bids], [], ts_ms=None)
    subject = Book("X")
    subject.apply_snapshot([lvl(5000, 1)], [], ts_ms=None)
    for price, delta in deltas:
        try:
            subject.apply_delta(Side.BID, PriceE4(price), delta, ts_ms=None)
        except BookInvariantError:
            assert subject.is_stale()
    subject.apply_snapshot([lvl(p, c) for p, c in bids], [], ts_ms=None)
    assert diff(reference, subject).is_empty
    assert subject.best_bid() == reference.best_bid()


@given(st.lists(st.tuples(st.integers(0, 4999), st.integers(1, 100)), max_size=50))
@settings(max_examples=200)
def test_best_bid_matches_max_after_any_delta_sequence(ops: list[tuple[int, int]]) -> None:
    book = Book("X")
    book.apply_snapshot([], [lvl(5000, 1)], ts_ms=None)
    shadow: dict[int, int] = {}
    for price, delta in ops:
        signed = delta if len(shadow) < 5 or price not in shadow else -min(delta, shadow[price])
        book.apply_delta(Side.BID, PriceE4(price), signed, ts_ms=None)
        shadow[price] = shadow.get(price, 0) + signed
        if shadow[price] == 0:
            del shadow[price]
        expected = max(shadow) if shadow else None
        best = book.best_bid()
        assert (best.price if best else None) == expected
