"""The catalog: books rebuilt from keyframes and baked changes, and queries over baked tables.

The M4 definition of done is checked here on a synthetic tape: rebuilding every book from one
keyframe plus the baked changes up to the next keyframe's instant gives that next keyframe, for
every market, including those a gap left stale (docs/ROADMAP.md).
"""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass
from pathlib import Path

import pytest

from tape.bake import DataLayout, HourKey, bake_hour, record_bake
from tape.bake.layout import NS_PER_HOUR
from tape.book import Book, books_from_keyframe_rows, diff
from tape.errors import ArchiveError, BookInvariantError
from tape.events import Level, Side
from tape.fixedpoint import CountE2, PriceE4
from tape.segment import read_keyframe, write_keyframe
from tape.store import Catalog
from tape.timeutil import Ms, Ns
from tests.fakes.synthetic_tape import HOUR, MS, SECOND, SegmentScript

TICKERS = ("KXA-26SEP10-T1", "KXB-26SEP10-T2", "KXC-26SEP10-T3", "KXD-26SEP10-T4")
LATE = TICKERS[3]


def price_text(price_e4: int) -> str:
    return f"{price_e4 // 10_000}.{price_e4 % 10_000:04d}"


def count_text(count_e2: int) -> str:
    sign = "-" if count_e2 < 0 else ""
    return f"{sign}{abs(count_e2) // 100}.{abs(count_e2) % 100:02d}"


@dataclass(frozen=True)
class Image:
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    stale: bool

    @classmethod
    def of(cls, book: Book) -> Image:
        return cls(tuple(book.levels(Side.BID)), tuple(book.levels(Side.ASK)), book.is_stale())


class Recording:
    """Writes frames and, beside them, applies them to books as the recorder's supervisor does."""

    def __init__(self, layout: DataLayout, hour: HourKey = HOUR) -> None:
        self.layout = layout
        self.hour = hour
        self.script = SegmentScript(conn_id=2)
        self.books: dict[str, Book] = {}
        self.seq = 0
        self.keyframes: list[int] = []
        self.images: dict[tuple[str, int], Image] = {}

    def wall(self, ms: int) -> int:
        return self.hour.start_wall_ns + ms * MS

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def open(self) -> None:
        self.script.opened(self.wall(0))
        self.script.subscribed(self.wall(1), channel="orderbook_delta", sid=1)

    def snapshot(
        self,
        ms: int,
        ticker: str,
        bids: list[tuple[int, int]],
        asks: list[tuple[int, int]],
        *,
        sid: int = 1,
        seq: int | None = None,
    ) -> None:
        self.script.snapshot(
            self.wall(ms),
            sid=sid,
            seq=self.next_seq() if seq is None else seq,
            ticker=ticker,
            yes=[(price_text(p), count_text(c)) for p, c in bids],
            no=[(price_text(p), count_text(c)) for p, c in asks],
        )
        book = self.books.setdefault(ticker, Book(ticker))
        book.apply_snapshot(
            [Level(PriceE4(p), CountE2(c)) for p, c in bids],
            [Level(PriceE4(p), CountE2(c)) for p, c in asks],
            ts_ms=None,
        )

    def delta(
        self,
        ms: int,
        ticker: str,
        side: Side,
        price: int,
        delta: int,
        *,
        sid: int = 1,
        seq: int | None = None,
    ) -> None:
        wall = self.wall(ms)
        self.script.delta(
            wall,
            sid=sid,
            seq=self.next_seq() if seq is None else seq,
            ticker=ticker,
            price=price_text(price),
            delta=count_text(delta),
            side="yes" if side is Side.BID else "no",
        )
        book = self.books.get(ticker)
        if book is None:
            return
        try:
            book.apply_delta(side, PriceE4(price), delta, ts_ms=Ms(wall // MS))
        except BookInvariantError:
            return

    def gap(self, ms: int) -> None:
        self.script.gap(self.wall(ms), sid=1, expected_seq=self.seq + 1, got_seq=self.seq + 3)
        self.seq += 2
        for book in self.books.values():
            book.mark_stale()

    def keyframe(self, ms: int) -> None:
        wall = self.wall(ms)
        rows = [
            row for book in self.books.values() for row in book.to_keyframe(as_of_recv_ns=Ns(wall))
        ]
        directory = self.layout.keyframe_dir(self.hour)
        directory.mkdir(parents=True, exist_ok=True)
        name = f"{(wall - self.hour.start_wall_ns) // (60 * SECOND):02d}.parquet"
        write_keyframe(directory / name, sorted(rows, key=lambda r: (r.ticker, r.side, r.price_e4)))
        self.keyframes.append(wall)

    def remember(self, ms: int) -> None:
        for ticker, book in self.books.items():
            self.images[(ticker, self.wall(ms))] = Image.of(book)

    def finish(self) -> None:
        self.script.write(self.layout, self.hour, 0)
        report = bake_hour(self.layout, self.hour, software_version="0.1.0", max_part_rows=2_000)
        record_bake(self.layout, report, software_version="0.1.0", clock_offset_ms=None)


def random_walk(
    recording: Recording, rng: random.Random, start_ms: int, end_ms: int, step_ms: int
) -> None:
    """Deltas that keep every book valid: bids below 0.5000, asks above it."""
    for ms in range(start_ms, end_ms, step_ms):
        ticker = rng.choice([t for t in TICKERS if t in recording.books])
        side = rng.choice((Side.BID, Side.ASK))
        price = (
            rng.randrange(1_000, 4_901, 100)
            if side is Side.BID
            else rng.randrange(5_100, 9_001, 100)
        )
        current = recording.books[ticker].size_at(side, PriceE4(price))
        change = -rng.randint(1, current) if current and rng.random() < 0.5 else rng.randint(1, 900)
        recording.delta(ms, ticker, side, price, change)


def record_a_session(layout: DataLayout) -> Recording:
    # A seeded generator, so every run records the same session; nothing here needs secrecy.
    rng = random.Random(20260910)  # noqa: S311
    recording = Recording(layout)
    recording.open()
    for index, ticker in enumerate(TICKERS[:3]):
        recording.snapshot(
            1_000 + index, ticker, bids=[(4_000, 1_000), (3_900, 500)], asks=[(6_000, 700)]
        )
    random_walk(recording, rng, 2_000, 60_000, 40)
    recording.keyframe(60_000)
    random_walk(recording, rng, 60_010, 100_000, 40)
    recording.snapshot(100_000, LATE, bids=[(2_000, 100)], asks=[(8_000, 100)])
    random_walk(recording, rng, 100_010, 120_000, 40)
    recording.keyframe(120_000)
    random_walk(recording, rng, 120_010, 150_000, 40)
    recording.gap(150_000)
    random_walk(recording, rng, 150_010, 151_000, 40)
    recording.snapshot(151_000, TICKERS[0], bids=[(4_500, 300)], asks=[(5_500, 300)])
    random_walk(recording, rng, 151_010, 180_000, 40)
    recording.keyframe(180_000)
    recording.remember(180_000)
    random_walk(recording, rng, 180_010, 190_000, 40)
    recording.snapshot(190_000, TICKERS[1], bids=[(1_500, 200)], asks=[(9_500, 200)])
    recording.snapshot(200_000, TICKERS[2], bids=[(3_000, 200)], asks=[(7_000, 200)])
    recording.snapshot(200_001, LATE, bids=[], asks=[])
    random_walk(recording, rng, 200_010, 240_000, 40)
    recording.keyframe(240_000)
    random_walk(recording, rng, 240_010, 250_000, 40)
    recording.remember(250_000)
    recording.script.trade(recording.wall(250_001), sid=2, seq=1, ticker=TICKERS[0], trade_id="t-1")
    recording.script.trade(recording.wall(250_002), sid=2, seq=2, ticker=TICKERS[1], trade_id="t-2")
    recording.finish()
    return recording


@pytest.fixture
def layout(tmp_path: Path) -> DataLayout:
    return DataLayout.under(tmp_path / "data")


@pytest.fixture
def recording(layout: DataLayout) -> Recording:
    return record_a_session(layout)


def keyframe_books(layout: DataLayout, wall: int) -> dict[str, Book]:
    name = f"{(wall - HOUR.start_wall_ns) // (60 * SECOND):02d}.parquet"
    return books_from_keyframe_rows(read_keyframe(layout.keyframe_dir(HOUR) / name))


def test_replaying_from_each_keyframe_to_the_next_gives_the_next_keyframe(
    layout: DataLayout, recording: Recording
) -> None:
    catalog = Catalog(layout)
    for earlier, later in zip(recording.keyframes, recording.keyframes[1:], strict=False):
        expected = keyframe_books(layout, later)
        rebuilt = catalog.books_at(later, start_wall_ns=earlier)
        assert set(rebuilt) == set(expected)
        for ticker, book in expected.items():
            assert rebuilt[ticker].is_stale() == book.is_stale(), (ticker, later)
            assert diff(rebuilt[ticker], book).is_empty, (ticker, later)
    # The gap left two books stale at the third keyframe, and one market joined late.
    third = keyframe_books(layout, recording.keyframes[2])
    assert [third[t].is_stale() for t in TICKERS] == [False, True, True, True]
    assert LATE not in keyframe_books(layout, recording.keyframes[0])


def test_a_book_between_keyframes_equals_the_recorders_book(
    layout: DataLayout, recording: Recording
) -> None:
    catalog = Catalog(layout)
    for (ticker, wall), image in recording.images.items():
        assert Image.of(catalog.book_at(ticker, wall)) == image, (ticker, wall)


def test_a_market_absent_from_the_keyframe_starts_stale_until_its_snapshot(
    layout: DataLayout, recording: Recording
) -> None:
    catalog = Catalog(layout)
    before = catalog.book_at(LATE, recording.wall(99_000))
    after = catalog.book_at(LATE, recording.wall(100_000))
    assert before.is_stale()
    assert not after.is_stale()
    assert after.best_bid() == Level(PriceE4(2_000), CountE2(100))
    unknown = catalog.book_at("KXNEVER-26SEP10", recording.wall(100_000))
    assert unknown.is_stale()


def test_without_a_keyframe_in_the_lookback_the_replay_starts_from_nothing(
    layout: DataLayout, recording: Recording
) -> None:
    for path in layout.keyframe_dir(HOUR).iterdir():
        path.unlink()
    catalog = Catalog(layout, keyframe_lookback_hours=1)
    # Every market was resnapshotted after the gap, so replaying the hour alone rebuilds it.
    late = recording.wall(250_000)
    for ticker in TICKERS:
        assert Image.of(catalog.book_at(ticker, late)) == recording.images[(ticker, late)]
    # Before the gap no snapshot of the late market has been seen yet.
    assert catalog.book_at(LATE, recording.wall(90_000)).is_stale()


def test_a_wall_clock_stepping_back_cannot_reorder_a_subscription(layout: DataLayout) -> None:
    ticker = TICKERS[0]
    recording = Recording(layout)
    recording.open()
    recording.snapshot(1_000, ticker, bids=[(4_000, 100)], asks=[(6_000, 100)])
    recording.keyframe(1_500)
    recording.delta(2_000, ticker, Side.BID, 4_100, 300)
    # The wall clock steps back 100 ms: the next deltas are stamped before the one applied first.
    recording.script.step_wall_clock(-100 * MS)
    recording.delta(1_905, ticker, Side.BID, 4_100, -300)
    recording.delta(1_950, ticker, Side.ASK, 5_900, 50)
    recording.remember(2_000)
    # A reconnect gets sid 1 again and restarts its sequence, so neither orders changes.
    recording.snapshot(3_000, ticker, bids=[(3_000, 10)], asks=[(7_000, 10)], sid=1, seq=1)
    recording.delta(3_100, ticker, Side.BID, 3_000, 5, sid=1, seq=2)
    recording.remember(3_100)
    recording.finish()
    catalog = Catalog(layout)

    for (name, wall), image in recording.images.items():
        assert Image.of(catalog.book_at(name, wall)) == image
    assert recording.images[(ticker, recording.wall(3_100))].bids == (
        Level(PriceE4(3_000), CountE2(15)),
    )


def test_a_delta_that_breaks_the_book_leaves_it_stale_until_a_snapshot(layout: DataLayout) -> None:
    recording = Recording(layout)
    recording.open()
    recording.snapshot(1_000, TICKERS[0], bids=[(4_000, 100)], asks=[(6_000, 100)])
    recording.keyframe(1_500)
    recording.delta(2_000, TICKERS[0], Side.BID, 4_000, -500)
    recording.delta(2_500, TICKERS[0], Side.BID, 4_100, 100)
    recording.snapshot(3_000, TICKERS[0], bids=[(4_200, 50)], asks=[])
    recording.finish()
    catalog = Catalog(layout)

    broken = catalog.book_at(TICKERS[0], recording.wall(2_600))
    assert broken.is_stale()
    assert recording.books[TICKERS[0]].is_stale() is False
    healed = catalog.book_at(TICKERS[0], recording.wall(3_000))
    assert not healed.is_stale()
    assert healed.levels(Side.BID) == [Level(PriceE4(4_200), CountE2(50))]


def test_deltas_and_trades_come_back_in_receive_order_within_the_range(
    layout: DataLayout, recording: Recording
) -> None:
    catalog = Catalog(layout)
    t0, t1 = recording.wall(60_000), recording.wall(61_000)
    deltas = catalog.deltas(TICKERS[0], t0, t1)
    received = [wall for wall in deltas["recv_wall_ns"].to_pylist() if wall is not None]
    assert len(received) == deltas.num_rows
    assert received == sorted(received)
    assert all(t0 <= wall < t1 for wall in received)
    assert set(deltas["ticker"].to_pylist()) <= {TICKERS[0]}
    everything = catalog.deltas(
        TICKERS[0], recording.hour.start_wall_ns, recording.hour.end_wall_ns
    )
    assert everything.num_rows > deltas.num_rows > 0
    trades = catalog.trades(TICKERS[1], recording.hour.start_wall_ns, recording.hour.end_wall_ns)
    assert trades["trade_id"].to_pylist() == ["t-2"]
    assert catalog.trades(TICKERS[1], t0, t0).num_rows == 0
    with pytest.raises(ValueError, match="before t0"):
        catalog.deltas(TICKERS[0], t1, t0)
    later = HourKey(HOUR.date, 20)
    assert (
        catalog.deltas(TICKERS[0], later.start_wall_ns, later.start_wall_ns + NS_PER_HOUR).num_rows
        == 0
    )


def test_integrity_is_the_days_manifest(layout: DataLayout, recording: Recording) -> None:
    catalog = Catalog(layout)
    manifest = catalog.integrity(HOUR.date)
    assert manifest.gaps.count == 1
    assert manifest.hour_bake(HOUR.hour) is not None
    with pytest.raises(ArchiveError, match="no manifest"):
        catalog.integrity(dt.date(2026, 9, 11))
    with pytest.raises(ValueError, match="after at_wall_ns"):
        catalog.books_at(recording.wall(1), start_wall_ns=recording.wall(2))
    with pytest.raises(ValueError, match="non-negative"):
        Catalog(layout, keyframe_lookback_hours=-1)
