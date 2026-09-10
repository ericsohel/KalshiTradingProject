"""The market directory: catalog replacement, ranking, nulls, depth, and recorder health."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

import pytest

from tape.api import UNRESOLVED, MarketDirectory, MarketMetadata
from tape.api.contract import (
    BusHealth,
    ConnectionHealth,
    Depth,
    MarketDetail,
    MarketRow,
    PriceRange,
    RecorderHealth,
    ServiceStatus,
)
from tape.book import Book
from tape.events import (
    CatalogEntry,
    ConnectionReport,
    Level,
    MarketCatalog,
    Receipt,
    StatusReport,
    Ticker,
)
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import NS_PER_S, Ms, Ns

RECEIPT: Final = Receipt(conn_id=0, recv_mono_ns=Ns(1), recv_wall_ns=Ns(1))
BUS: Final = BusHealth(epoch=7, last_seq=9, messages=9, resets=1, missed=0, books_known=2)
CLOSE_TS: Final = 1_800_000_000
METADATA: Final = MarketMetadata(
    title="Highest temperature in NYC today?",
    subtitle="84° to 85°",
    category="Climate and Weather",
    price_ranges=(PriceRange(start_e4=PriceE4(100), end_e4=PriceE4(9900), step_e4=PriceE4(100)),),
)


def entry(ticker: str, volume: int = 100, *, showcase: bool = False) -> CatalogEntry:
    return CatalogEntry(
        ticker=ticker,
        series_ticker=ticker.split("-", 1)[0],
        event_ticker=ticker.rsplit("-", 1)[0],
        volume_24h=CountE2(volume),
        close_ts=CLOSE_TS,
        showcase=showcase,
    )


def catalog(*entries: CatalogEntry) -> MarketCatalog:
    return MarketCatalog(markets=entries)


def ticker_update(ticker: str, *, bid: int, ask: int, last: int) -> Ticker:
    return Ticker(
        ticker=ticker,
        ts_ms=Ms(5),
        receipt=RECEIPT,
        sid=1,
        last=PriceE4(last),
        bid=PriceE4(bid),
        ask=PriceE4(ask),
        bid_size=None,
        ask_size=None,
        volume=CountE2(1_000),
        open_interest=CountE2(0),
    )


def book(bids: Sequence[int], asks: Sequence[int], *, stale: bool = False) -> Book:
    held = Book("KXA-26-B1")
    held.apply_snapshot(
        [Level(PriceE4(price), CountE2(100)) for price in bids],
        [Level(PriceE4(price), CountE2(200)) for price in asks],
        ts_ms=Ms(1_789_000_000_000),
    )
    if stale:
        held.mark_stale()
    return held


def test_markets_rank_by_volume_then_ticker_and_the_limit_takes_the_top() -> None:
    directory = MarketDirectory()
    directory.apply_catalog(catalog(entry("KXB-1", 500), entry("KXA-1", 500), entry("KXC-1", 900)))

    assert [e.ticker for e in directory.top(3)] == ["KXC-1", "KXA-1", "KXB-1"]
    assert [e.ticker for e in directory.top(2)] == ["KXC-1", "KXA-1"]
    assert [e.ticker for e in directory.top(200)] == ["KXC-1", "KXA-1", "KXB-1"]
    with pytest.raises(ValueError, match="limit must be positive"):
        directory.top(0)


def test_a_catalog_replaces_the_previous_one_whole() -> None:
    directory = MarketDirectory()
    assert directory.top(10) == ()
    directory.apply_catalog(catalog(entry("KXA-1"), entry("KXB-1")))
    directory.apply_catalog(catalog(entry("KXB-1", 7), entry("KXC-1")))

    assert "KXA-1" not in directory
    assert directory.entry("KXA-1") is None
    assert directory.entry("KXB-1") == entry("KXB-1", 7)
    assert len(directory) == 2


def test_a_row_is_null_until_a_ticker_update_and_metadata_arrive() -> None:
    directory = MarketDirectory()
    market = entry("KXA-26-B1", 12_345, showcase=True)
    directory.apply_catalog(catalog(market))

    assert directory.row(market, metadata=UNRESOLVED, book=None) == MarketRow(
        ticker="KXA-26-B1",
        event_ticker="KXA-26",
        series_ticker="KXA",
        title=None,
        subtitle=None,
        category=None,
        showcase=True,
        volume_24h_e2=CountE2(12_345),
        close_ts=CLOSE_TS,
        bid_e4=None,
        ask_e4=None,
        last_e4=None,
        book="unknown",
    )

    directory.apply_ticker(ticker_update("KXA-26-B1", bid=4_000, ask=4_100, last=4_050))
    row = directory.row(market, metadata=METADATA, book=book([4_000], [4_100]))
    assert (row.title, row.subtitle, row.category) == (
        "Highest temperature in NYC today?",
        "84° to 85°",
        "Climate and Weather",
    )
    assert (row.bid_e4, row.ask_e4, row.last_e4, row.book) == (4_000, 4_100, 4_050, "fresh")


def test_ticker_updates_follow_the_catalog_once_one_has_arrived() -> None:
    directory = MarketDirectory()
    # Before any catalog every update is kept, so a market is priced as soon as it is listed.
    directory.apply_ticker(ticker_update("KXA-1", bid=1, ask=2, last=1))
    directory.apply_catalog(catalog(entry("KXA-1"), entry("KXB-1")))
    assert directory.row(entry("KXA-1"), metadata=UNRESOLVED, book=None).bid_e4 == 1

    directory.apply_ticker(ticker_update("KXZ-1", bid=5, ask=6, last=5))
    directory.apply_catalog(catalog(entry("KXB-1")))
    directory.apply_catalog(catalog(entry("KXA-1"), entry("KXZ-1")))
    assert directory.row(entry("KXA-1"), metadata=UNRESOLVED, book=None).bid_e4 is None
    assert directory.row(entry("KXZ-1"), metadata=UNRESOLVED, book=None).bid_e4 is None


def test_detail_carries_the_price_grid_and_the_best_twenty_levels_per_side() -> None:
    directory = MarketDirectory()
    market = entry("KXA-26-B1")
    directory.apply_catalog(catalog(market))
    bids = range(100, 2_600, 100)  # 25 levels
    held = book(bids, [4_000, 3_000, 5_000])

    detail = directory.detail(market, metadata=METADATA, book=held)

    assert isinstance(detail, MarketDetail)
    assert detail.price_ranges == METADATA.price_ranges
    assert detail.depth == Depth(
        ts_ms=1_789_000_000_000,
        bids=tuple((PriceE4(price), CountE2(100)) for price in range(2_500, 500, -100)),
        asks=tuple((PriceE4(price), CountE2(200)) for price in (3_000, 4_000, 5_000)),
    )
    assert detail.depth is not None
    assert len(detail.depth.bids) == 20
    assert detail.book == "fresh"

    stale = directory.detail(market, metadata=UNRESOLVED, book=book([100], [200], stale=True))
    assert (stale.book, stale.price_ranges) == ("stale", None)
    assert stale.depth is not None
    unknown = directory.detail(market, metadata=UNRESOLVED, book=None)
    assert (unknown.book, unknown.depth) == ("unknown", None)


def test_the_recorder_counts_as_recording_while_its_report_is_within_two_intervals() -> None:
    directory = MarketDirectory()
    assert directory.service_status(now_mono_ns=5, bus=BUS, clients=3) == ServiceStatus(
        recording=False, recorder_status_age_ms=None, recorder=None, bus=BUS, clients=3
    )

    report = StatusReport(
        interval_s=60,
        universe_size=4,
        subscribed_markets=3,
        connections=(
            ConnectionReport(
                conn_id=2,
                taped=True,
                frames=10,
                gaps=1,
                reconnects=2,
                stale_books=0,
                sink_dropped=5,
            ),
        ),
    )
    directory.apply_status(report, received_mono_ns=1_000)
    at_limit = directory.service_status(now_mono_ns=1_000 + 120 * NS_PER_S, bus=BUS, clients=0)
    assert at_limit == ServiceStatus(
        recording=True,
        recorder_status_age_ms=120_000,
        recorder=RecorderHealth(
            universe_size=4,
            subscribed_markets=3,
            connections=(
                ConnectionHealth(
                    conn_id=2,
                    taped=True,
                    frames=10,
                    gaps=1,
                    reconnects=2,
                    stale_books=0,
                    sink_dropped=5,
                ),
            ),
        ),
        bus=BUS,
        clients=0,
    )
    late = directory.service_status(now_mono_ns=1_001 + 120 * NS_PER_S, bus=BUS, clients=0)
    assert (late.recording, late.recorder) == (False, at_limit.recorder)
