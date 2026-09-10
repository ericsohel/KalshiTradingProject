"""Book taps: starting copies, ordered changes, faults, release, and composition."""

from __future__ import annotations

from collections.abc import Iterable

import pytest

from tape.book import Book
from tape.errors import BookInvariantError
from tape.events import BookDelta, BookSnapshot, Level, Receipt, Side
from tape.fixedpoint import CountE2, PriceE4
from tape.recorder.tap import (
    FAULT_NO_BOOK,
    FAULT_OVERFLOW,
    FAULT_STALE,
    BookImage,
    CompositeBookTap,
    LiveBookTap,
    TapWindow,
)
from tape.timeutil import Ns


def lvl(price: int, count: int) -> Level:
    return Level(PriceE4(price), CountE2(count))


def fresh(ticker: str, bids: Iterable[Level] = (), asks: Iterable[Level] = ()) -> Book:
    book = Book(ticker)
    book.apply_snapshot(bids, asks, ts_ms=None)
    return book


def receipt(mono_ns: int) -> Receipt:
    return Receipt(conn_id=2, recv_mono_ns=Ns(mono_ns), recv_wall_ns=Ns(mono_ns))


def apply_delta(book: Book, price: int, change: int, *, mono_ns: int = 1) -> BookDelta:
    """Apply a bid delta to a live book, as a supervisor would, and return its event."""
    delta = BookDelta(
        ticker=book.ticker,
        ts_ms=None,
        receipt=receipt(mono_ns),
        sid=1,
        seq=None,
        side=Side.BID,
        price=PriceE4(price),
        delta=change,
    )
    assert book.apply_delta(delta.side, delta.price, delta.delta, ts_ms=None)
    return delta


class Releases:
    """The supervisor's side of ``on_close``: remembers every tap released."""

    def __init__(self) -> None:
        self.taps: list[LiveBookTap] = []

    def __call__(self, tap: LiveBookTap) -> None:
        self.taps.append(tap)


def open_tap(
    books: dict[str, Book], *tickers: str, max_events: int = 10, releases: Releases | None = None
) -> LiveBookTap:
    return LiveBookTap(
        tickers,
        books=books,
        max_events=max_events,
        on_close=Releases() if releases is None else releases,
    )


def test_a_tap_copies_each_book_at_opening_and_keeps_its_changes_in_order() -> None:
    book = fresh("T-1", [lvl(3000, 100)], [lvl(6000, 70)])
    other = fresh("T-2", [lvl(2000, 5)])
    tap = open_tap({"T-1": book, "T-2": other}, "T-1", "T-1")
    first = apply_delta(book, 3000, 10, mono_ns=1)
    tap.record(first, was_stale=False)
    tap.record(apply_delta(other, 2000, 1), was_stale=False)  # not tapped
    second = apply_delta(book, 2900, 5, mono_ns=2)
    tap.record(second, was_stale=False)

    assert tap.tickers == frozenset({"T-1"})
    windows = tap.close()
    assert dict(windows) == {
        "T-1": TapWindow(
            ticker="T-1",
            start=BookImage(bids=(lvl(3000, 100),), asks=(lvl(6000, 70),)),
            events=(first, second),
            fault=None,
        )
    }


@pytest.mark.parametrize("stale", [True, False])
def test_a_market_without_a_fresh_book_at_opening_gets_a_no_book_window(stale: bool) -> None:
    books = {"T-1": fresh("T-1", [lvl(3000, 100)])}
    if stale:
        books["T-1"].mark_stale()
    else:
        del books["T-1"]
    tap = open_tap(books, "T-1")
    books["T-1"] = fresh("T-1", [lvl(3000, 100)])
    tap.record(apply_delta(books["T-1"], 3000, 10), was_stale=False)
    assert tap.close()["T-1"] == TapWindow.absent("T-1")
    assert TapWindow.absent("T-1").fault == FAULT_NO_BOOK


def test_more_changes_than_max_events_fault_the_window_with_overflow() -> None:
    book = fresh("T-1", [lvl(3000, 100)])
    tap = open_tap({"T-1": book}, "T-1", max_events=2)
    kept = [apply_delta(book, 3000, 1, mono_ns=n) for n in (1, 2)]
    for change in kept:
        tap.record(change, was_stale=False)
    tap.record(apply_delta(book, 3000, 1, mono_ns=3), was_stale=False)
    tap.record(apply_delta(book, 3000, 1, mono_ns=4), was_stale=False)
    window = tap.close()["T-1"]
    assert (window.fault, window.events) == (FAULT_OVERFLOW, tuple(kept))


def test_a_book_still_stale_at_closing_faults_the_window() -> None:
    book = fresh("T-1", [lvl(3000, 100)])
    tap = open_tap({"T-1": book}, "T-1")
    book.mark_stale()  # a gap or a reconnect
    assert tap.close()["T-1"].fault == FAULT_STALE


def test_a_snapshot_onto_a_stale_book_faults_the_window_though_the_book_is_fresh_again() -> None:
    book = fresh("T-1", [lvl(3000, 100)])
    tap = open_tap({"T-1": book}, "T-1")
    book.mark_stale()
    was_stale = book.is_stale()
    snapshot = BookSnapshot(
        ticker="T-1",
        ts_ms=None,
        receipt=receipt(5),
        sid=1,
        seq=None,
        bids=(lvl(3000, 90),),
        asks=(),
    )
    book.apply_snapshot(snapshot.bids, snapshot.asks, ts_ms=None)
    tap.record(snapshot, was_stale=was_stale)
    tap.record(apply_delta(book, 3000, 1), was_stale=False)
    window = tap.close()["T-1"]
    assert not book.is_stale()
    assert (window.fault, window.events) == (FAULT_STALE, ())


@pytest.mark.parametrize("replacement", [None, "fresh copy"])
def test_a_book_dropped_or_replaced_inside_the_window_faults_it(replacement: str | None) -> None:
    books = {"T-1": fresh("T-1", [lvl(3000, 100)])}
    tap = open_tap(books, "T-1")
    del books["T-1"]
    if replacement is not None:
        books["T-1"] = fresh("T-1", [lvl(3000, 100)])
    assert tap.close()["T-1"].fault == FAULT_STALE


def test_close_is_idempotent_releases_the_tap_once_and_ends_recording() -> None:
    book = fresh("T-1", [lvl(3000, 100)])
    releases = Releases()
    tap = open_tap({"T-1": book}, "T-1", releases=releases)
    open_before = not tap.closed
    windows = tap.close()
    tap.record(apply_delta(book, 3000, 10), was_stale=False)
    assert tap.close() is windows
    assert (open_before, tap.closed) == (True, True)
    assert releases.taps == [tap]
    assert windows["T-1"].events == ()
    assert windows["T-1"].fault is None


def test_max_events_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_events must be positive"):
        open_tap({}, "T-1", max_events=0)


def test_a_window_has_a_start_copy_exactly_when_it_is_not_a_no_book_window() -> None:
    image = BookImage(bids=(), asks=())
    with pytest.raises(ValueError, match="absent exactly when"):
        TapWindow(ticker="T-1", start=None, events=(), fault=None)
    with pytest.raises(ValueError, match="absent exactly when"):
        TapWindow(ticker="T-1", start=image, events=(), fault=FAULT_NO_BOOK)
    book = fresh("T-1", [lvl(3000, 100)])
    with pytest.raises(ValueError, match="no start copy but 1 events"):
        TapWindow(
            ticker="T-1", start=None, events=(apply_delta(book, 3000, 1),), fault=FAULT_NO_BOOK
        )


def test_a_book_image_copies_levels_best_first_and_rebuilds_a_fresh_book() -> None:
    book = fresh("T-1", [lvl(2000, 1), lvl(3000, 2)], [lvl(7000, 3), lvl(6000, 4)])
    book.mark_stale()
    image = BookImage.of(book)
    assert image == BookImage(bids=(lvl(3000, 2), lvl(2000, 1)), asks=(lvl(6000, 4), lvl(7000, 3)))
    rebuilt = image.to_book("T-1")
    assert not rebuilt.is_stale()
    assert BookImage.of(rebuilt) == image
    with pytest.raises(BookInvariantError, match="crossed"):
        BookImage(bids=(lvl(6000, 1),), asks=(lvl(5000, 1),)).to_book("T-1")


def test_a_composite_tap_closes_every_tap_once_and_adds_absent_windows() -> None:
    first_book = fresh("T-A", [lvl(3000, 100)])
    second_book = fresh("T-B", [lvl(2000, 5)])
    releases = Releases()
    first = open_tap({"T-A": first_book}, "T-A", releases=releases)
    second = open_tap({"T-B": second_book}, "T-B", releases=releases)
    composite = CompositeBookTap([first, second], uncovered=["T-C"])
    change = apply_delta(first_book, 3000, 10)
    first.record(change, was_stale=False)

    windows = composite.close()
    assert sorted(windows) == ["T-A", "T-B", "T-C"]
    assert windows["T-A"].events == (change,)
    assert windows["T-B"] == TapWindow(
        ticker="T-B", start=BookImage(bids=(lvl(2000, 5),), asks=()), events=(), fault=None
    )
    assert windows["T-C"] == TapWindow.absent("T-C")
    assert composite.close() is windows
    assert releases.taps == [first, second]
