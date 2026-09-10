"""The consumer rule: unknown until a refresh image, reset on loss, and never a wrong fresh book."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, Literal

from hypothesis import given, settings
from hypothesis import strategies as st

from tape.book import Book
from tape.bus import (
    BOOK_FRESH,
    BOOK_STALE,
    BOOK_UNKNOWN,
    RESET_EPOCH,
    RESET_GAP,
    RESET_START,
    BookStatus,
    BusEnvelope,
    LiveBooks,
    LiveBooksStats,
    Observation,
    StatusChange,
)
from tape.errors import BookInvariantError
from tape.events import (
    BookDelta,
    BookRefresh,
    BookSnapshot,
    GapEvent,
    Level,
    MarketEvent,
    Receipt,
    Side,
    Trade,
)
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import Ms, Ns

EPOCH: Final = 1_789_000_000_000_000_000
RECEIPT: Final = Receipt(conn_id=2, recv_mono_ns=Ns(1), recv_wall_ns=Ns(EPOCH))
TICKERS: Final = ("A", "B")


def lvl(price: int, count: int) -> Level:
    return Level(PriceE4(price), CountE2(count))


def refresh(
    ticker: str,
    bids: Sequence[Level] = (),
    asks: Sequence[Level] = (),
    *,
    stale: bool = False,
    ts_ms: int | None = None,
) -> BookRefresh:
    return BookRefresh(
        ticker=ticker,
        ts_ms=None if ts_ms is None else Ms(ts_ms),
        receipt=RECEIPT,
        stale=stale,
        bids=tuple(bids),
        asks=tuple(asks),
    )


def snapshot(
    ticker: str, bids: Sequence[Level] = (), asks: Sequence[Level] = (), *, ts_ms: int = 1
) -> BookSnapshot:
    return BookSnapshot(
        ticker=ticker,
        ts_ms=Ms(ts_ms),
        receipt=RECEIPT,
        sid=1,
        seq=None,
        bids=tuple(bids),
        asks=tuple(asks),
    )


def delta(ticker: str, price: int, change: int, side: Side = Side.BID) -> BookDelta:
    return BookDelta(
        ticker=ticker,
        ts_ms=Ms(2),
        receipt=RECEIPT,
        sid=1,
        seq=None,
        side=side,
        price=PriceE4(price),
        delta=change,
    )


def trade(ticker: str) -> Trade:
    return Trade(
        ticker=ticker,
        ts_ms=Ms(3),
        receipt=RECEIPT,
        sid=2,
        seq=None,
        trade_id="t",
        price=PriceE4(5000),
        count=CountE2(100),
        taker_side=Side.BID,
        is_block=False,
    )


def levels(book: Book) -> tuple[list[Level], list[Level]]:
    return book.levels(Side.BID), book.levels(Side.ASK)


def change(ticker: str, before: BookStatus, after: BookStatus) -> StatusChange:
    return StatusChange(ticker=ticker, before=before, after=after)


class Feed:
    """Numbers events as one publisher epoch does, and can lose some on the way."""

    def __init__(self, live: LiveBooks, *, epoch: int = EPOCH) -> None:
        self.live = live
        self.epoch = epoch
        self.seq = 0

    def send(self, event: MarketEvent) -> Observation:
        self.seq += 1
        return self.live.observe(BusEnvelope(bus_epoch=self.epoch, bus_seq=self.seq, event=event))

    def lose(self, count: int) -> None:
        self.seq += count


def test_a_book_is_unknown_until_its_refresh_image_and_then_follows_every_change() -> None:
    live = LiveBooks()
    feed = Feed(live)
    assert (live.epoch, live.last_seq, live.status("A")) == (None, None, BOOK_UNKNOWN)

    assert feed.send(delta("A", 4000, 100)) == Observation(
        reset=RESET_START, missed=0, applied=False, changes=()
    )
    assert feed.send(snapshot("A", [lvl(4000, 1)])).applied is False
    assert live.status("A") == BOOK_UNKNOWN
    assert dict(live.books()) == {}

    adopted = feed.send(refresh("A", [lvl(4000, 100)], [lvl(6000, 50)], ts_ms=7))
    assert adopted == Observation(
        reset=None, missed=0, applied=True, changes=(change("A", BOOK_UNKNOWN, BOOK_FRESH),)
    )
    book = live.books()["A"]
    assert levels(book) == ([lvl(4000, 100)], [lvl(6000, 50)])
    assert book.last_ts_ms == 7

    assert feed.send(delta("A", 4000, 50)) == Observation(
        reset=None, missed=0, applied=True, changes=()
    )
    assert book.size_at(Side.BID, PriceE4(4000)) == 150
    assert feed.send(snapshot("A", [lvl(4100, 5)], ts_ms=9)).applied
    assert (levels(book), book.last_ts_ms) == (([lvl(4100, 5)], []), 9)
    # Events that are not about books are forwarded by the caller, never applied here.
    assert feed.send(trade("A")) == Observation(reset=None, missed=0, applied=False, changes=())
    assert feed.send(GapEvent(receipt=RECEIPT, sid=1, expected_seq=2, got_seq=4)).applied is False

    assert (live.epoch, live.last_seq) == (EPOCH, 7)
    assert live.stats == LiveBooksStats(
        messages=7, resets=1, missed=0, refreshes=1, ignored=2, book_errors=0
    )


def test_a_gap_makes_every_book_unknown_and_reports_how_much_was_missed() -> None:
    live = LiveBooks()
    feed = Feed(live)
    feed.send(refresh("B", [lvl(1000, 1)], stale=True))
    feed.send(refresh("A", [lvl(4000, 100)]))
    feed.lose(3)

    observation = feed.send(refresh("A", [lvl(4200, 1)]))

    assert observation == Observation(
        reset=RESET_GAP,
        missed=3,
        applied=True,
        changes=(
            change("A", BOOK_FRESH, BOOK_UNKNOWN),
            change("B", BOOK_STALE, BOOK_UNKNOWN),
            change("A", BOOK_UNKNOWN, BOOK_FRESH),
        ),
    )
    assert sorted(live.books()) == ["A"]
    assert levels(live.books()["A"]) == ([lvl(4200, 1)], [])
    assert (live.stats.resets, live.stats.missed) == (2, 3)


def test_a_new_epoch_resets_like_a_gap_and_counts_the_numbers_before_it_as_missed() -> None:
    live = LiveBooks()
    Feed(live).send(refresh("A", [lvl(4000, 100)]))
    restarted = Feed(live, epoch=EPOCH + 1)
    restarted.lose(2)

    observation = restarted.send(delta("A", 4000, 1))

    assert observation == Observation(
        reset=RESET_EPOCH, missed=2, applied=False, changes=(change("A", BOOK_FRESH, BOOK_UNKNOWN),)
    )
    assert (live.epoch, live.last_seq) == (EPOCH + 1, 3)


def test_a_number_that_does_not_advance_is_treated_as_loss() -> None:
    live = LiveBooks()
    live.observe(BusEnvelope(bus_epoch=EPOCH, bus_seq=4, event=refresh("A")))
    assert live.status("A") == BOOK_FRESH

    repeated = live.observe(BusEnvelope(bus_epoch=EPOCH, bus_seq=4, event=trade("A")))

    assert (repeated.reset, repeated.missed) == (RESET_GAP, 0)
    assert live.status("A") == BOOK_UNKNOWN


def test_a_stale_image_is_held_ignores_deltas_and_becomes_fresh_at_the_next_snapshot() -> None:
    live = LiveBooks()
    feed = Feed(live)
    assert feed.send(refresh("A", [lvl(4000, 100)], stale=True)).changes == (
        change("A", BOOK_UNKNOWN, BOOK_STALE),
    )

    assert feed.send(delta("A", 4000, 5)).applied is False
    assert levels(live.books()["A"]) == ([lvl(4000, 100)], [])

    freshened = feed.send(snapshot("A", [lvl(3000, 2)]))
    assert (freshened.applied, freshened.changes) == (True, (change("A", BOOK_STALE, BOOK_FRESH),))
    assert live.stats.ignored == 1


def test_a_refresh_image_replaces_a_known_copy_whole() -> None:
    live = LiveBooks()
    feed = Feed(live)
    feed.send(refresh("A", [lvl(4000, 100)], [lvl(6000, 1)]))

    replaced = feed.send(refresh("A", [lvl(4500, 3)]))

    assert (replaced.applied, replaced.changes) == (True, ())
    assert levels(live.books()["A"]) == ([lvl(4500, 3)], [])
    assert live.stats.refreshes == 2


def test_a_copy_that_would_break_a_book_invariant_is_dropped() -> None:
    live = LiveBooks()
    feed = Feed(live)
    feed.send(refresh("A", [lvl(4000, 100)], [lvl(5000, 1)]))
    feed.send(refresh("B", [lvl(4000, 100)]))

    crossed = feed.send(delta("A", 5000, 1))
    negative = feed.send(delta("B", 4000, -101))
    invalid_image = feed.send(refresh("A", [lvl(5000, 1)], [lvl(5000, 1)]))
    invalid_snapshot_base = feed.send(refresh("B", [lvl(1000, 1)]))
    invalid_snapshot = feed.send(snapshot("B", [lvl(1000, 0)]))

    assert (crossed.applied, crossed.changes) == (False, (change("A", BOOK_FRESH, BOOK_UNKNOWN),))
    assert (negative.applied, negative.changes) == (False, (change("B", BOOK_FRESH, BOOK_UNKNOWN),))
    assert (invalid_image.applied, invalid_image.changes) == (False, ())
    assert invalid_snapshot_base.applied
    assert invalid_snapshot.changes == (change("B", BOOK_FRESH, BOOK_UNKNOWN),)
    assert dict(live.books()) == {}
    assert live.stats.book_errors == 4


# ------------------------------------------------------------------------ property

type StepKind = Literal["snapshot", "delta", "stale", "refresh", "trade", "restart"]


@dataclass(frozen=True, slots=True)
class Step:
    """One thing that happens at the publisher, and whether the consumer loses its message."""

    kind: StepKind
    ticker: str = "A"
    bids: tuple[Level, ...] = ()
    asks: tuple[Level, ...] = ()
    side: Side = Side.BID
    price: int = 0
    change: int = 0
    lost: bool = False


class ModelPublisher:
    """The recorder's side of the bus in miniature.

    Books change the way a supervisor changes them: a snapshot or delta that breaks an invariant
    is not published and leaves the book stale (a crossing delta also leaves it changed), a delta
    on a stale book is ignored, and a disconnect stales a book without any message. A restart
    starts a new epoch with no books.
    """

    def __init__(self) -> None:
        self.epoch = EPOCH
        self.seq = 0
        self.books = {ticker: Book(ticker) for ticker in TICKERS}

    def step(self, step: Step) -> MarketEvent | None:
        if step.kind == "restart":
            self.epoch += 1
            self.seq = 0
            self.books = {ticker: Book(ticker) for ticker in TICKERS}
            return None
        book = self.books[step.ticker]
        if step.kind == "stale":
            book.mark_stale()
            return None
        if step.kind == "refresh":
            return refresh(
                step.ticker, book.levels(Side.BID), book.levels(Side.ASK), stale=book.is_stale()
            )
        if step.kind == "trade":
            return trade(step.ticker)
        return self._change(book, step)

    def _change(self, book: Book, step: Step) -> BookSnapshot | BookDelta | None:
        try:
            if step.kind == "snapshot":
                book.apply_snapshot(step.bids, step.asks, ts_ms=None)
                return snapshot(step.ticker, step.bids, step.asks)
            applied = book.apply_delta(step.side, PriceE4(step.price), step.change, ts_ms=None)
        except BookInvariantError:
            return None
        return delta(step.ticker, step.price, step.change, step.side) if applied else None

    def envelope(self, event: MarketEvent) -> BusEnvelope:
        self.seq += 1
        return BusEnvelope(bus_epoch=self.epoch, bus_seq=self.seq, event=event)


prices = st.integers(1, 19).map(lambda tick: tick * 500)
counts = st.integers(1, 4).map(lambda lots: lots * 100)


def side_levels(low: int, high: int) -> st.SearchStrategy[tuple[Level, ...]]:
    return st.dictionaries(st.integers(low, high).map(lambda t: t * 500), counts, max_size=3).map(
        lambda sizes: tuple(lvl(price, count) for price, count in sizes.items())
    )


tickers = st.sampled_from(TICKERS)
losses = st.sampled_from([False, False, False, True])
steps = st.one_of(
    st.builds(
        Step,
        kind=st.just("snapshot"),
        ticker=tickers,
        # Overlapping ranges, so some snapshots cross and are refused by the publisher.
        bids=side_levels(1, 11),
        asks=side_levels(9, 19),
        lost=losses,
    ),
    st.builds(
        Step,
        kind=st.just("delta"),
        ticker=tickers,
        side=st.sampled_from(Side),
        price=prices,
        change=st.sampled_from([-300, -100, 100, 200]),
        lost=losses,
    ),
    st.builds(Step, kind=st.just("refresh"), ticker=tickers, lost=losses),
    st.builds(Step, kind=st.just("stale"), ticker=tickers),
    st.builds(Step, kind=st.just("trade"), ticker=tickers, lost=losses),
    st.builds(Step, kind=st.just("restart")),
)


@given(st.lists(steps, max_size=80))
@settings(max_examples=400)
def test_a_known_book_matches_the_publisher_s_for_any_losses(history: list[Step]) -> None:
    publisher = ModelPublisher()
    live = LiveBooks()
    reported: dict[str, BookStatus] = dict.fromkeys(TICKERS, BOOK_UNKNOWN)

    for step in history:
        event = publisher.step(step)
        if event is None:
            continue
        envelope = publisher.envelope(event)
        if step.lost:
            continue
        observation = live.observe(envelope)

        for status_change in observation.changes:
            assert reported[status_change.ticker] == status_change.before
            reported[status_change.ticker] = status_change.after
        assert reported == {ticker: live.status(ticker) for ticker in TICKERS}

        for ticker in TICKERS:
            status = live.status(ticker)
            published = publisher.books[ticker]
            if status == BOOK_STALE:
                assert published.is_stale()
            if status != BOOK_UNKNOWN and not published.is_stale():
                assert status == BOOK_FRESH
                assert levels(live.books()[ticker]) == levels(published)
        if isinstance(event, BookRefresh):
            # An image of a valid book is always adopted, in the state it was taken in.
            adopted = live.books().get(event.ticker)
            assert adopted is not None or observation.applied is False
            if adopted is not None:
                assert levels(adopted) == levels(publisher.books[event.ticker])
                assert adopted.is_stale() == event.stale
