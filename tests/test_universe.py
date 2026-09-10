"""Universe selection: the filters, the volume ranking, the showcase override, accounting."""

from __future__ import annotations

import calendar
from collections.abc import Iterable

import msgspec
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.errors import FixedPointError, WireError
from tape.fixedpoint import CountE2
from tape.recorder import (
    ACTIVE_STATUS,
    DEFAULT_EXCHANGE_INDEX,
    REASON_BELOW_VOLUME,
    REASON_BEYOND_HORIZON,
    REASON_CLOSED,
    REASON_DUPLICATE,
    REASON_MVE,
    REASON_NOT_ACTIVE,
    REASON_OVER_CAP,
    REASONS,
    MarketSummary,
    UniversePolicy,
    select,
)
from tape.wire.rest import Market

NOW = 1_800_000_000
HOUR = 3_600
MIDNIGHT_2026_09_10 = calendar.timegm((2026, 9, 10, 0, 0, 0))


def summary(
    ticker: str,
    *,
    volume: int = 1_000,
    status: str = ACTIVE_STATUS,
    close_ts: int | None = NOW + HOUR,
    is_mve: bool = False,
    series: str = "SER",
) -> MarketSummary:
    return MarketSummary(
        ticker=ticker,
        series_ticker=series,
        event_ticker=f"{series}-EVT",
        exchange_index=0,
        status=status,
        volume_24h=CountE2(volume),
        close_ts=close_ts,
        is_mve=is_mve,
    )


def policy(
    *,
    floor: int = 100,
    cap: int = 10,
    showcase: Iterable[str] = (),
    exclude_mve: bool = True,
    horizon: int | None = None,
) -> UniversePolicy:
    return UniversePolicy(
        min_volume_24h=CountE2(floor),
        max_l2_markets=cap,
        showcase_series=frozenset(showcase),
        exclude_mve=exclude_mve,
        max_seconds_to_close=horizon,
    )


def wire_market(**overrides: object) -> Market:
    fields: dict[str, object] = {
        "ticker": "KXBTC15M-26SEP092130-00",
        "event_ticker": "KXBTC15M-26SEP092130",
        "market_type": "binary",
        "yes_sub_title": "Yes",
        "no_sub_title": "No",
        "created_time": "2026-09-09T00:00:00Z",
        "updated_time": "2026-09-09T00:00:00Z",
        "open_time": "2026-09-09T00:00:00Z",
        "close_time": "2026-09-10T00:00:00Z",
        "latest_expiration_time": "2026-09-10T00:00:00Z",
        "settlement_timer_seconds": 60,
        "status": "active",
        "notional_value_dollars": "1.0000",
        "yes_bid_dollars": "0.5000",
        "yes_ask_dollars": "0.5100",
        "no_bid_dollars": "0.4900",
        "no_ask_dollars": "0.5000",
        "yes_bid_size_fp": "10.00",
        "yes_ask_size_fp": "10.00",
        "last_price_dollars": "0.5000",
        "previous_yes_bid_dollars": "0.5000",
        "previous_yes_ask_dollars": "0.5000",
        "previous_price_dollars": "0.5000",
        "volume_fp": "100.00",
        "volume_24h_fp": "10.00",
        "open_interest_fp": "50.00",
        "result": "",
        "can_close_early": False,
        "expiration_value": "",
        "rules_primary": "rules",
        "rules_secondary": "rules2",
        "price_level_structure": "linear_cent",
        "price_ranges": [{"start": "0.01", "end": "0.99", "step": "0.01"}],
    }
    fields.update(overrides)
    return msgspec.convert(fields, Market)


# ------------------------------------------------------------------- adapting


def test_from_wire_reads_fixed_point_fields_and_derives_the_series() -> None:
    market = wire_market(exchange_index=2, volume_24h_fp="1234.56")
    assert MarketSummary.from_wire(market, is_mve=True) == MarketSummary(
        ticker="KXBTC15M-26SEP092130-00",
        series_ticker="KXBTC15M",
        event_ticker="KXBTC15M-26SEP092130",
        exchange_index=2,
        status="active",
        volume_24h=CountE2(123_456),
        close_ts=MIDNIGHT_2026_09_10,
        is_mve=True,
    )


def test_from_wire_defaults_an_absent_shard_and_keeps_a_dashless_ticker_whole() -> None:
    summary_ = MarketSummary.from_wire(wire_market(ticker="PLAIN", status="new"))
    assert summary_.series_ticker == "PLAIN"
    assert summary_.exchange_index == DEFAULT_EXCHANGE_INDEX
    assert summary_.status == "new"
    assert summary_.is_mve is False


@pytest.mark.parametrize(
    ("close_time", "expected"),
    [
        ("", None),
        ("0001-01-01T00:00:00Z", None),
        ("2026-09-10T00:00:00", MIDNIGHT_2026_09_10),
        ("2026-09-10T02:00:00+02:00", MIDNIGHT_2026_09_10),
        ("2026-09-10T00:00:00.999Z", MIDNIGHT_2026_09_10),
    ],
)
def test_from_wire_converts_the_close_time(close_time: str, expected: int | None) -> None:
    market = wire_market(close_time=close_time)
    assert MarketSummary.from_wire(market).close_ts == expected


def test_from_wire_rejects_malformed_values() -> None:
    with pytest.raises(WireError, match="unparsable close_time 'tomorrow'"):
        MarketSummary.from_wire(wire_market(close_time="tomorrow"))
    with pytest.raises(FixedPointError):
        MarketSummary.from_wire(wire_market(volume_24h_fp="1e3"))


def test_policy_rejects_values_that_would_silently_empty_the_universe() -> None:
    with pytest.raises(ValueError, match="min_volume_24h"):
        policy(floor=-1)
    with pytest.raises(ValueError, match="max_l2_markets"):
        policy(cap=-1)
    with pytest.raises(ValueError, match="max_seconds_to_close"):
        policy(horizon=0)


# ------------------------------------------------------------------ selecting


def test_each_filter_removes_and_counts_its_own_markets() -> None:
    markets = [
        summary("KEEP"),
        summary("DUP"),
        summary("DUP", volume=9_999),
        summary("OLD", status="closed"),
        summary("NEW", status="a_status_kalshi_added_later"),
        summary("MVE", is_mve=True),
        summary("SHUT", close_ts=NOW),
        summary("FAR", close_ts=NOW + 2 * HOUR + 1),
        summary("THIN", volume=99),
    ]
    decision = select(markets, policy(horizon=2 * HOUR), now_ts=NOW)
    assert decision.l2_tickers == frozenset({"KEEP", "DUP"})
    assert decision.showcase == frozenset()
    assert decision.dropped_for_cap == 0
    assert decision.reason_counts == {
        REASON_DUPLICATE: 1,
        REASON_NOT_ACTIVE: 2,
        REASON_MVE: 1,
        REASON_CLOSED: 1,
        REASON_BEYOND_HORIZON: 1,
        REASON_BELOW_VOLUME: 1,
        REASON_OVER_CAP: 0,
    }
    assert tuple(decision.reason_counts) == REASONS


def test_a_market_failing_several_filters_is_counted_once_under_the_first() -> None:
    doomed = summary("X", status="closed", is_mve=True, close_ts=NOW - 1, volume=0)
    decision = select([doomed], policy(horizon=1), now_ts=NOW)
    assert decision.reason_counts[REASON_NOT_ACTIVE] == 1
    assert sum(decision.reason_counts.values()) == 1


def test_the_horizon_is_inclusive_and_an_unknown_close_passes_the_time_filters() -> None:
    markets = [summary("EDGE", close_ts=NOW + HOUR), summary("UNKNOWN", close_ts=None)]
    decision = select(markets, policy(horizon=HOUR), now_ts=NOW)
    assert decision.l2_tickers == frozenset({"EDGE", "UNKNOWN"})


def test_multivariate_markets_are_kept_when_not_excluded() -> None:
    decision = select([summary("MVE", is_mve=True)], policy(exclude_mve=False), now_ts=NOW)
    assert decision.l2_tickers == frozenset({"MVE"})


def test_the_budget_goes_to_the_highest_volume_with_ties_broken_by_ticker() -> None:
    markets = [
        summary("C", volume=500),
        summary("B", volume=500),
        summary("A", volume=100),
        summary("D", volume=900),
    ]
    decision = select(markets, policy(cap=2), now_ts=NOW)
    assert decision.l2_tickers == frozenset({"D", "B"})
    assert decision.dropped_for_cap == 2
    assert decision.reason_counts[REASON_OVER_CAP] == 2


def test_a_zero_budget_captures_nothing_but_the_showcase() -> None:
    markets = [summary("SHOW-1", series="SHOW"), summary("BIG", volume=10_000)]
    decision = select(markets, policy(cap=0, showcase=["SHOW"]), now_ts=NOW)
    assert decision.l2_tickers == frozenset({"SHOW-1"})
    assert decision.dropped_for_cap == 1


def test_showcase_markets_skip_the_floor_and_are_admitted_before_the_budget() -> None:
    markets = [
        summary("BIG", volume=10_000),
        summary("MID", volume=5_000),
        summary("SHOW-1", series="SHOW", volume=0),
    ]
    decision = select(markets, policy(cap=2, showcase=["SHOW"]), now_ts=NOW)
    assert decision.l2_tickers == frozenset({"SHOW-1", "BIG"})
    assert decision.showcase == frozenset({"SHOW-1"})
    assert decision.dropped_for_cap == 1
    assert decision.reason_counts[REASON_BELOW_VOLUME] == 0


def test_a_showcase_over_the_cap_is_kept_whole_and_everything_else_is_dropped() -> None:
    showcase = [summary(f"SHOW-{index}", series="SHOW") for index in range(3)]
    markets = [*showcase, summary("BIG", volume=10_000)]
    decision = select(markets, policy(cap=2, showcase=["SHOW"]), now_ts=NOW)
    assert decision.l2_tickers == decision.showcase == frozenset({"SHOW-0", "SHOW-1", "SHOW-2"})
    assert decision.dropped_for_cap == 1


def test_showcase_markets_still_face_the_eligibility_filters() -> None:
    markets = [summary("SHOW-1", series="SHOW", close_ts=NOW - HOUR)]
    decision = select(markets, policy(showcase=["SHOW"]), now_ts=NOW)
    assert decision.l2_tickers == frozenset()
    assert decision.reason_counts[REASON_CLOSED] == 1


# ------------------------------------------------------------------ properties

SERIES = ("S1", "S2", "S3")

market_summaries = st.builds(
    MarketSummary,
    ticker=st.text(alphabet="abcdef", min_size=1, max_size=4),
    series_ticker=st.sampled_from(SERIES),
    event_ticker=st.just("EVT"),
    exchange_index=st.integers(0, 2),
    status=st.sampled_from([ACTIVE_STATUS, ACTIVE_STATUS, "closed", "initialized", "unheard_of"]),
    volume_24h=st.integers(0, 1_000).map(CountE2),
    close_ts=st.none() | st.integers(NOW - HOUR, NOW + 2 * HOUR),
    is_mve=st.booleans(),
)
listings = st.lists(market_summaries, unique_by=lambda market: market.ticker, max_size=40)
policies = st.builds(
    UniversePolicy,
    min_volume_24h=st.integers(0, 1_000).map(CountE2),
    max_l2_markets=st.integers(0, 15),
    showcase_series=st.frozensets(st.sampled_from(SERIES)),
    exclude_mve=st.booleans(),
    max_seconds_to_close=st.none() | st.integers(1, 2 * HOUR),
)


def is_eligible(market: MarketSummary, rules: UniversePolicy) -> bool:
    """Reference model of the filters that run before showcase and ranking."""
    if market.status != ACTIVE_STATUS or (rules.exclude_mve and market.is_mve):
        return False
    if market.close_ts is None:
        return True
    within_horizon = (
        rules.max_seconds_to_close is None or market.close_ts - NOW <= rules.max_seconds_to_close
    )
    return market.close_ts > NOW and within_horizon


@given(listings, policies, st.data())
@settings(max_examples=300)
def test_select_does_not_depend_on_the_order_markets_arrive_in(
    markets: list[MarketSummary], rules: UniversePolicy, data: st.DataObject
) -> None:
    shuffled = data.draw(st.permutations(markets))
    assert select(shuffled, rules, now_ts=NOW) == select(markets, rules, now_ts=NOW)


@given(listings, policies)
@settings(max_examples=300)
def test_select_never_exceeds_the_budget_and_accounts_for_every_market(
    markets: list[MarketSummary], rules: UniversePolicy
) -> None:
    decision = select(markets, rules, now_ts=NOW)
    by_ticker = {market.ticker: market for market in markets}
    ranked = decision.l2_tickers - decision.showcase
    assert decision.showcase <= decision.l2_tickers
    assert len(ranked) <= max(0, rules.max_l2_markets - len(decision.showcase))
    assert len(decision.l2_tickers) <= max(rules.max_l2_markets, len(decision.showcase))
    assert sum(decision.reason_counts.values()) + len(decision.l2_tickers) == len(markets)
    assert decision.dropped_for_cap == decision.reason_counts[REASON_OVER_CAP]
    for ticker in decision.l2_tickers:
        market = by_ticker[ticker]
        assert is_eligible(market, rules)
        assert (ticker in decision.showcase) == (market.series_ticker in rules.showcase_series)
        assert ticker in decision.showcase or market.volume_24h >= rules.min_volume_24h


@given(listings, policies)
@settings(max_examples=300)
def test_select_spends_the_budget_on_the_best_ranked_markets(
    markets: list[MarketSummary], rules: UniversePolicy
) -> None:
    decision = select(markets, rules, now_ts=NOW)
    showcase = [
        market
        for market in markets
        if is_eligible(market, rules) and market.series_ticker in rules.showcase_series
    ]
    qualified = [
        market
        for market in markets
        if is_eligible(market, rules)
        and market.series_ticker not in rules.showcase_series
        and market.volume_24h >= rules.min_volume_24h
    ]
    admitted = [market for market in qualified if market.ticker in decision.l2_tickers]
    left_out = [market for market in qualified if market.ticker not in decision.l2_tickers]
    assert decision.showcase == frozenset(market.ticker for market in showcase)
    assert len(admitted) == min(len(qualified), max(0, rules.max_l2_markets - len(showcase)))
    assert decision.dropped_for_cap == len(left_out)
    if admitted and left_out:
        worst_admitted = max((-market.volume_24h, market.ticker) for market in admitted)
        best_left_out = min((-market.volume_24h, market.ticker) for market in left_out)
        assert worst_admitted < best_left_out


def test_duplicate_ticker_resolution_does_not_depend_on_page_order() -> None:
    """The same market twice in one listing, with different volume, must resolve the
    same way whichever page arrived first."""
    rules = policy(floor=1_000, cap=10)
    stale = summary("KXDUP-1", volume=500)
    fresh = summary("KXDUP-1", volume=5_000)
    forward = select([stale, fresh], rules, now_ts=NOW)
    backward = select([fresh, stale], rules, now_ts=NOW)
    assert forward == backward
    assert forward.l2_tickers == frozenset({"KXDUP-1"})
    assert forward.reason_counts[REASON_DUPLICATE] == 1
