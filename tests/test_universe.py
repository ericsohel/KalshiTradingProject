"""Universe selection: eligibility, the rule groups of ADR 0028, the budget, and accounting."""

from __future__ import annotations

import calendar
from collections import Counter
from typing import Any, Final

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
    REASON_EVENT_BEYOND_HORIZON,
    REASON_EVENT_NOT_CHOSEN,
    REASON_MVE,
    REASON_NO_GROUP,
    REASON_NOT_ACTIVE,
    REASON_OVER_CAP,
    REASON_OVER_EVENT_CAP,
    REASON_OVER_GROUP_CAP,
    REASONS,
    GroupSelection,
    MarketSummary,
    UniverseDecision,
    UniverseGroup,
    UniversePolicy,
    select,
)
from tape.wire.rest import Market

NOW = 1_800_000_000
HOUR = 3_600
MIDNIGHT_2026_09_10 = calendar.timegm((2026, 9, 10, 0, 0, 0))
CATEGORIES: Final = {"NFL": "Sports", "MLB": "Sports", "NHL": "Sports", "PRES": "Politics"}


def summary(
    ticker: str,
    *,
    volume: int = 1_000,
    status: str = ACTIVE_STATUS,
    close_ts: int | None = NOW + HOUR,
    is_mve: bool = False,
) -> MarketSummary:
    """A market whose series and event follow from its ticker, as Kalshi's do: ``S-E-M``."""
    return MarketSummary(
        ticker=ticker,
        series_ticker=ticker.split("-", 1)[0],
        event_ticker=ticker.rsplit("-", 1)[0],
        exchange_index=0,
        status=status,
        volume_24h=CountE2(volume),
        close_ts=close_ts,
        is_mve=is_mve,
    )


def by_series(
    name: str,
    *series: str,
    events: int = 1,
    per_event: int = 10,
    max_markets: int | None = None,
    hours: int | None = None,
) -> UniverseGroup:
    return UniverseGroup(
        name=name,
        series=series,
        events=events,
        markets_per_event=per_event,
        max_markets=max_markets,
        max_hours_to_close=hours,
    )


def by_category(
    name: str,
    category: str,
    *,
    events: int = 10,
    per_event: int = 10,
    max_markets: int | None = None,
    hours: int | None = None,
) -> UniverseGroup:
    return UniverseGroup(
        name=name,
        category=category,
        events=events,
        markets_per_event=per_event,
        max_markets=max_markets,
        max_hours_to_close=hours,
    )


def policy(
    *groups: UniverseGroup,
    floor: int = 100,
    cap: int = 10,
    exclude_mve: bool = True,
    horizon: int | None = None,
) -> UniversePolicy:
    return UniversePolicy(
        min_volume_24h=CountE2(floor),
        max_l2_markets=cap,
        groups=groups,
        exclude_mve=exclude_mve,
        max_seconds_to_close=horizon,
    )


EVERYTHING_IN_SER: Final = by_series("all", "SER", events=10)


def admitted(decision: UniverseDecision) -> dict[str, tuple[str, ...]]:
    """Each group's admitted markets, in admission order."""
    return {group.name: group.tickers for group in decision.groups}


def reasons(decision: UniverseDecision) -> dict[str, int]:
    """The reasons counted at least once."""
    return {reason: count for reason, count in decision.reason_counts.items() if count}


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


# --------------------------------------------------------------------- policy


def test_policy_rejects_values_that_would_silently_empty_the_universe() -> None:
    with pytest.raises(ValueError, match="min_volume_24h"):
        policy(floor=-1)
    with pytest.raises(ValueError, match="max_l2_markets"):
        policy(cap=-1)
    with pytest.raises(ValueError, match="max_seconds_to_close"):
        policy(horizon=0)


def test_group_names_are_unique_because_every_report_is_by_name() -> None:
    with pytest.raises(ValueError, match=r"names must be unique; repeated: 'twice'$"):
        policy(by_series("twice", "A"), by_category("once", "Sports"), by_series("twice", "B"))


@pytest.mark.parametrize(
    ("fields", "message"),
    [
        (
            {"series": ("A",), "category": "Sports"},
            r"^universe group 'g' must set exactly one of series and category$",
        ),
        ({}, r"^universe group 'g' must set exactly one of series and category$"),
        ({"series": ()}, r"'g': series must list at least one non-empty series ticker"),
        ({"series": ("A", "")}, r"'g': series must list at least one non-empty series ticker"),
        ({"series": ("A", "B", "A", "B")}, r"'g' lists A, B more than once"),
        ({"category": ""}, r"'g': category must not be empty"),
        ({"series": ("A",), "events": 0}, r"'g': events must be positive, got 0"),
        ({"series": ("A",), "markets_per_event": -1}, r"'g': markets_per_event must be positive"),
        ({"series": ("A",), "max_markets": 0}, r"'g': max_markets must be positive, got 0"),
        (
            {"category": "Sports", "max_hours_to_close": 0},
            r"'g': max_hours_to_close must be positive, got 0",
        ),
        ({"series": ("A",), "name": ""}, "a universe group needs a non-empty name"),
    ],
)
def test_a_group_selects_by_exactly_one_rule_with_positive_counts(
    fields: dict[str, Any],  # Any: group fields of several types
    message: str,
) -> None:
    defaults: dict[str, Any] = {"name": "g", "events": 1, "markets_per_event": 1}  # Any: as above
    with pytest.raises(ValueError, match=message):
        UniverseGroup(**(defaults | fields))


def test_a_policy_needs_categories_only_for_its_category_groups() -> None:
    mixed = policy(
        by_series("a", "A"),
        by_category("sports", "Sports"),
        by_category("politics", "Politics"),
        by_category("more sports", "Sports"),
    )
    assert mixed.categories == frozenset({"Sports", "Politics"})
    assert policy(by_series("a", "A")).categories == frozenset()


# ----------------------------------------------------------------- eligibility


def test_each_filter_removes_and_counts_its_own_markets() -> None:
    markets = [
        summary("SER-E-KEEP"),
        summary("SER-E-DUP"),
        summary("SER-E-DUP", volume=9_999),
        summary("SER-E-OLD", status="closed"),
        summary("SER-E-NEW", status="a_status_kalshi_added_later"),
        summary("SER-E-MVE", is_mve=True),
        summary("SER-E-SHUT", close_ts=NOW),
        summary("SER-E-FAR", close_ts=NOW + 2 * HOUR + 1),
        summary("ELSE-E-UNGROUPED"),
    ]
    decision = select(markets, policy(EVERYTHING_IN_SER, horizon=2 * HOUR), now_ts=NOW)
    assert decision.l2_tickers == frozenset({"SER-E-KEEP", "SER-E-DUP"})
    assert decision.dropped_for_cap == 0
    assert decision.reason_counts == {
        REASON_DUPLICATE: 1,
        REASON_NOT_ACTIVE: 2,
        REASON_MVE: 1,
        REASON_CLOSED: 1,
        REASON_BEYOND_HORIZON: 1,
        REASON_NO_GROUP: 1,
        REASON_EVENT_BEYOND_HORIZON: 0,
        REASON_BELOW_VOLUME: 0,
        REASON_EVENT_NOT_CHOSEN: 0,
        REASON_OVER_EVENT_CAP: 0,
        REASON_OVER_GROUP_CAP: 0,
        REASON_OVER_CAP: 0,
    }
    assert tuple(decision.reason_counts) == REASONS


def test_a_market_failing_several_filters_is_counted_once_under_the_first() -> None:
    doomed = summary("SER-E-X", status="closed", is_mve=True, close_ts=NOW - 1, volume=0)
    decision = select([doomed], policy(EVERYTHING_IN_SER, horizon=1), now_ts=NOW)
    assert reasons(decision) == {REASON_NOT_ACTIVE: 1}


def test_the_horizon_is_inclusive_and_an_unknown_close_passes_the_time_filters() -> None:
    markets = [summary("SER-E-EDGE", close_ts=NOW + HOUR), summary("SER-E-UNKNOWN", close_ts=None)]
    decision = select(markets, policy(EVERYTHING_IN_SER, horizon=HOUR), now_ts=NOW)
    assert decision.l2_tickers == frozenset({"SER-E-EDGE", "SER-E-UNKNOWN"})


def test_multivariate_markets_are_kept_when_not_excluded() -> None:
    rules = policy(EVERYTHING_IN_SER, exclude_mve=False)
    decision = select([summary("SER-E-MVE", is_mve=True)], rules, now_ts=NOW)
    assert decision.l2_tickers == frozenset({"SER-E-MVE"})


def test_exclusions_apply_before_any_group_so_an_excluded_market_takes_no_place() -> None:
    """The nearest event's busiest market has closed, and a wholly closed event is nearer
    still; the group looks past both."""
    markets = [
        summary("A-GONE-M", volume=9_000, close_ts=NOW - 60),
        summary("A-SOON-BIG", volume=9_000, close_ts=NOW),
        summary("A-SOON-SMALL", volume=10, close_ts=NOW + 60),
        summary("A-LATER-M", volume=5_000, close_ts=NOW + HOUR),
    ]
    decision = select(markets, policy(by_series("a", "A", per_event=1)), now_ts=NOW)
    assert admitted(decision) == {"a": ("A-SOON-SMALL",)}
    assert reasons(decision) == {REASON_CLOSED: 2, REASON_EVENT_NOT_CHOSEN: 1}


def test_duplicate_ticker_resolution_does_not_depend_on_page_order() -> None:
    """The same market twice in one listing, with different volume, must resolve the
    same way whichever page arrived first."""
    rules = policy(EVERYTHING_IN_SER)
    stale = summary("SER-E-DUP", volume=500)
    fresh = summary("SER-E-DUP", volume=5_000)
    forward = select([stale, fresh], rules, now_ts=NOW)
    backward = select([fresh, stale], rules, now_ts=NOW)
    assert forward == backward
    assert forward.l2_tickers == frozenset({"SER-E-DUP"})
    assert forward.markets == (fresh,)
    assert forward.reason_counts[REASON_DUPLICATE] == 1


# --------------------------------------------------------------- series groups


def test_a_series_group_takes_the_nearest_events_of_each_series_in_the_order_listed() -> None:
    markets = [
        summary("A-E1-M", close_ts=NOW + 3 * HOUR),
        summary("A-E2-M", close_ts=NOW + HOUR),
        summary("A-E3-M", close_ts=None),
        summary("B-E1-M", close_ts=NOW + 2 * HOUR),
        summary("B-E2-M", close_ts=NOW + 5 * HOUR),
        summary("B-E3-M", close_ts=NOW + 6 * HOUR),
    ]
    decision = select(markets, policy(by_series("g", "B", "A", events=2, per_event=1)), now_ts=NOW)
    # B first because it is listed first; an event with no known close ranks last.
    assert admitted(decision) == {"g": ("B-E1-M", "B-E2-M", "A-E2-M", "A-E1-M")}
    assert decision.groups[0].events == 4
    assert reasons(decision) == {REASON_EVENT_NOT_CHOSEN: 2}


def test_an_event_is_as_near_as_its_earliest_closing_market() -> None:
    markets = [
        summary("A-WIDE-EARLY", volume=1, close_ts=NOW + 60),
        summary("A-WIDE-LATE", volume=9_000, close_ts=NOW + 9 * HOUR),
        summary("A-NARROW-M", volume=9_000, close_ts=NOW + HOUR),
    ]
    decision = select(markets, policy(by_series("g", "A", per_event=1)), now_ts=NOW)
    assert admitted(decision) == {"g": ("A-WIDE-LATE",)}
    assert reasons(decision) == {REASON_OVER_EVENT_CAP: 1, REASON_EVENT_NOT_CHOSEN: 1}


def test_a_series_group_ignores_the_volume_floor() -> None:
    rules = policy(by_series("g", "A"), floor=1_000)
    decision = select([summary("A-E-QUIET", volume=0)], rules, now_ts=NOW)
    assert decision.l2_tickers == frozenset({"A-E-QUIET"})


# ------------------------------------------------------------- category groups


def test_a_category_group_takes_the_events_with_the_highest_summed_volume() -> None:
    markets = [
        summary("NFL-G1-HOME", volume=300),
        summary("NFL-G1-AWAY", volume=300),
        summary("MLB-G1-HOME", volume=500),
        summary("NHL-G1-HOME", volume=200),
        summary("NHL-G1-AWAY", volume=100),
        summary("PRES-E-X", volume=90_000),
        summary("UNKNOWN-E-X", volume=90_000),
    ]
    rules = policy(by_category("sports", "Sports", events=1))
    decision = select(markets, rules, now_ts=NOW, categories=CATEGORIES)
    # NFL-G1 sums 600 and beats MLB-G1, whose one market is the busiest of all.
    assert admitted(decision) == {"sports": ("NFL-G1-AWAY", "NFL-G1-HOME")}
    assert decision.groups[0].events == 1
    assert reasons(decision) == {REASON_NO_GROUP: 2, REASON_EVENT_NOT_CHOSEN: 3}


def test_the_category_floor_applies_to_an_event_s_summed_volume_and_is_inclusive() -> None:
    markets = [
        summary("NFL-EVEN-A", volume=50),
        summary("NFL-EVEN-B", volume=50),
        summary("NFL-SHORT-A", volume=50),
        summary("NFL-SHORT-B", volume=49),
    ]
    rules = policy(by_category("sports", "Sports"), floor=100)
    decision = select(markets, rules, now_ts=NOW, categories=CATEGORIES)
    assert admitted(decision) == {"sports": ("NFL-EVEN-A", "NFL-EVEN-B")}
    assert reasons(decision) == {REASON_BELOW_VOLUME: 2}


def test_category_groups_admit_nothing_until_the_categories_are_known() -> None:
    markets = [summary("NFL-G1-HOME", volume=10_000), summary("A-E-M")]
    decision = select(
        markets, policy(by_category("sports", "Sports"), by_series("a", "A")), now_ts=NOW
    )
    assert admitted(decision) == {"sports": (), "a": ("A-E-M",)}
    assert reasons(decision) == {REASON_NO_GROUP: 1}


def test_without_groups_nothing_is_recorded() -> None:
    decision = select([summary("A-E-M")], policy(), now_ts=NOW)
    assert (decision.l2_tickers, decision.groups) == (frozenset(), ())
    assert reasons(decision) == {REASON_NO_GROUP: 1}


def test_tied_events_break_by_earliest_close_then_by_event_ticker() -> None:
    markets = [
        summary("NFL-LATE-M", volume=500, close_ts=NOW + 2 * HOUR),
        summary("NFL-SOON-M", volume=500, close_ts=NOW + HOUR),
        summary("MLB-ZULU-M", volume=500, close_ts=NOW + HOUR),
        summary("MLB-ALFA-M", volume=500, close_ts=NOW + HOUR),
    ]
    rules = policy(by_category("sports", "Sports", events=3))
    decision = select(markets, rules, now_ts=NOW, categories=CATEGORIES)
    assert admitted(decision) == {"sports": ("MLB-ALFA-M", "MLB-ZULU-M", "NFL-SOON-M")}


# ------------------------------------------------------------------------ caps


def test_markets_per_event_keeps_each_event_s_busiest_ties_by_close_then_ticker() -> None:
    markets = [
        summary("A-E-LOW", volume=10),
        summary("A-E-LATE", volume=40, close_ts=NOW + 2 * HOUR),
        summary("A-E-SOON", volume=40, close_ts=NOW + HOUR),
        summary("A-E-MIDB", volume=30),
        summary("A-E-MIDA", volume=30),
    ]
    decision = select(markets, policy(by_series("g", "A", per_event=4)), now_ts=NOW)
    assert admitted(decision) == {"g": ("A-E-SOON", "A-E-LATE", "A-E-MIDA", "A-E-MIDB")}
    assert reasons(decision) == {REASON_OVER_EVENT_CAP: 1}


def test_max_markets_caps_a_group_in_the_order_it_admits() -> None:
    markets = [
        summary(f"{series}-E-{index}", volume=100 * index)
        for series in ("A", "B")
        for index in (1, 2, 3)
    ]
    rules = policy(by_series("g", "A", "B", per_event=3, max_markets=4))
    decision = select(markets, rules, now_ts=NOW)
    assert admitted(decision) == {"g": ("A-E-3", "A-E-2", "A-E-1", "B-E-3")}
    assert decision.groups[0].events == 2
    assert reasons(decision) == {REASON_OVER_GROUP_CAP: 2}


# -------------------------------------------------------------------- horizons


def test_a_series_group_horizon_sets_aside_events_before_it_takes_the_nearest() -> None:
    markets = [
        summary("A-SOON-M", close_ts=NOW + 2 * HOUR),
        summary("A-SOON-LATER", close_ts=NOW + 99 * HOUR),
        summary("A-EDGE-M", close_ts=NOW + 3 * HOUR),
        summary("A-PAST-M", close_ts=NOW + 3 * HOUR + 1),
        summary("A-NEVER-M", close_ts=None),
    ]
    decision = select(markets, policy(by_series("g", "A", events=5, hours=3)), now_ts=NOW)
    # An event is as near as its earliest close, the bound is inclusive, and an event whose
    # close is unknown is never within a horizon.
    assert admitted(decision) == {"g": ("A-SOON-M", "A-SOON-LATER", "A-EDGE-M")}
    assert reasons(decision) == {REASON_EVENT_BEYOND_HORIZON: 2}


def test_a_series_group_whose_nearest_event_is_beyond_its_horizon_admits_nothing() -> None:
    decision = select(
        [summary("A-E-M", close_ts=NOW + 5 * HOUR)],
        policy(by_series("g", "A", hours=4)),
        now_ts=NOW,
    )
    assert admitted(decision) == {"g": ()}
    assert reasons(decision) == {REASON_EVENT_BEYOND_HORIZON: 1}


def test_a_category_group_horizon_keeps_long_lived_events_out_of_the_ranking() -> None:
    year = 365 * 24 * HOUR
    markets = [
        summary("NFL-FUTURES-A", volume=50_000, close_ts=NOW + year),
        summary("NFL-FUTURES-B", volume=50_000, close_ts=NOW + year),
        summary("NFL-TONIGHT-HOME", volume=900, close_ts=NOW + 5 * HOUR),
        summary("MLB-TOMORROW-HOME", volume=800, close_ts=NOW + 48 * HOUR),
        summary("NHL-LATER-HOME", volume=5_000, close_ts=NOW + 48 * HOUR + 1),
    ]
    rules = policy(by_category("sports", "Sports", events=2, per_event=1, hours=48))
    decision = select(markets, rules, now_ts=NOW, categories=CATEGORIES)
    assert admitted(decision) == {"sports": ("NFL-TONIGHT-HOME", "MLB-TOMORROW-HOME")}
    assert reasons(decision) == {REASON_EVENT_BEYOND_HORIZON: 3}


def test_a_market_beyond_one_group_s_horizon_can_still_be_admitted_by_a_later_group() -> None:
    markets = [summary("NFL-FUTURES-A", volume=50_000, close_ts=NOW + 1_000 * HOUR)]
    rules = policy(by_category("games", "Sports", hours=48), by_category("futures", "Sports"))
    decision = select(markets, rules, now_ts=NOW, categories=CATEGORIES)
    assert admitted(decision) == {"games": (), "futures": ("NFL-FUTURES-A",)}
    assert reasons(decision) == {}


# ------------------------------------------------------ several groups, one budget


def test_a_market_an_earlier_group_chose_is_not_counted_again_by_a_later_one() -> None:
    markets = [
        summary("A-NEAR-1", volume=900, close_ts=NOW + HOUR),
        summary("A-NEAR-2", volume=800, close_ts=NOW + HOUR),
        summary("A-NEAR-3", volume=700, close_ts=NOW + HOUR),
        summary("A-FAR-1", volume=5_000, close_ts=NOW + 9 * HOUR),
    ]
    rules = policy(
        by_series("nearest", "A", per_event=1),
        by_category("busiest", "Crypto", events=2, per_event=2),
    )
    decision = select(markets, rules, now_ts=NOW, categories={"A": "Crypto"})
    # The later group sees only what the earlier one left, so A-NEAR-1 takes none of its
    # places and it admits both remaining A-NEAR markets.
    assert admitted(decision) == {
        "nearest": ("A-NEAR-1",),
        "busiest": ("A-FAR-1", "A-NEAR-2", "A-NEAR-3"),
    }
    assert decision.group_of == {
        "A-NEAR-1": "nearest",
        "A-FAR-1": "busiest",
        "A-NEAR-2": "busiest",
        "A-NEAR-3": "busiest",
    }
    # Every market one group passed over, the other admitted.
    assert reasons(decision) == {}


def test_a_market_several_groups_pass_over_is_counted_under_the_first_reason() -> None:
    markets = [summary("A-E-BIG", volume=900), summary("A-E-SMALL", volume=10)]
    rules = policy(by_series("one", "A", per_event=1), by_category("two", "Crypto"), floor=100)
    decision = select(markets, rules, now_ts=NOW, categories={"A": "Crypto"})
    assert admitted(decision) == {"one": ("A-E-BIG",), "two": ()}
    assert reasons(decision) == {REASON_OVER_EVENT_CAP: 1}


def test_groups_apply_in_order_until_the_budget_is_reached_and_the_rest_are_skipped() -> None:
    markets = [
        *(summary(f"A-E-{index}", volume=100 * index) for index in (1, 2)),
        *(summary(f"B-E-{index}", volume=100 * index) for index in (1, 2, 3)),
        *(summary(f"C-E-{index}", volume=100 * index) for index in (1, 2)),
    ]
    rules = policy(
        by_series("first", "A"), by_series("second", "B"), by_series("third", "C"), cap=3
    )
    decision = select(markets, rules, now_ts=NOW)
    assert decision.groups == (
        GroupSelection(name="first", tickers=("A-E-2", "A-E-1"), events=1, skipped_for_budget=0),
        GroupSelection(name="second", tickers=("B-E-3",), events=1, skipped_for_budget=2),
        GroupSelection(name="third", tickers=(), events=0, skipped_for_budget=2),
    )
    assert len(decision.l2_tickers) == 3
    assert decision.dropped_for_cap == 4
    assert reasons(decision) == {REASON_OVER_CAP: 4}


def test_a_zero_budget_records_nothing_and_every_chosen_market_is_skipped() -> None:
    decision = select(
        [summary("A-E-1"), summary("B-E-1")], policy(by_series("g", "A"), cap=0), now_ts=NOW
    )
    assert (decision.l2_tickers, decision.showcase) == (frozenset(), frozenset())
    assert reasons(decision) == {REASON_OVER_CAP: 1, REASON_NO_GROUP: 1}


def test_only_markets_admitted_by_series_groups_carry_the_showcase_flag() -> None:
    markets = [summary("NFL-G-HOME", volume=10_000), summary("A-E-M")]
    rules = policy(by_series("a", "A"), by_category("sports", "Sports"))
    decision = select(markets, rules, now_ts=NOW, categories=CATEGORIES)
    assert decision.l2_tickers == frozenset({"A-E-M", "NFL-G-HOME"})
    assert decision.showcase == frozenset({"A-E-M"})
    assert [market.ticker for market in decision.markets] == ["A-E-M", "NFL-G-HOME"]


# ------------------------------------------------------------------ properties

SERIES = ("S1", "S2", "S3")
EVENTS = ("E1", "E2", "E3")
CATEGORY_NAMES = ("C1", "C2")


@st.composite
def market_summaries(draw: st.DrawFn) -> MarketSummary:
    series = draw(st.sampled_from(SERIES))
    event = f"{series}-{draw(st.sampled_from(EVENTS))}"
    return MarketSummary(
        ticker=f"{event}-{draw(st.text(alphabet='abcdef', min_size=1, max_size=3))}",
        series_ticker=series,
        event_ticker=event,
        exchange_index=draw(st.integers(0, 2)),
        status=draw(st.sampled_from([ACTIVE_STATUS, ACTIVE_STATUS, "closed", "unheard_of"])),
        volume_24h=CountE2(draw(st.integers(0, 1_000))),
        close_ts=draw(st.none() | st.integers(NOW - HOUR, NOW + 2 * HOUR)),
        is_mve=draw(st.booleans()),
    )


listings = st.lists(market_summaries(), unique_by=lambda market: market.ticker, max_size=40)
positive = st.integers(1, 3)
caps = st.none() | st.integers(1, 5)
horizons = st.none() | st.integers(1, 2)
"""Hours; closes are drawn within two hours of ``NOW``, so both bounds cut."""
group_rules = st.one_of(
    st.builds(
        UniverseGroup,
        name=st.just("group"),
        series=st.lists(st.sampled_from(SERIES), min_size=1, max_size=3, unique=True).map(tuple),
        events=positive,
        markets_per_event=positive,
        max_markets=caps,
        max_hours_to_close=horizons,
    ),
    st.builds(
        UniverseGroup,
        name=st.just("group"),
        category=st.sampled_from(CATEGORY_NAMES),
        events=positive,
        markets_per_event=positive,
        max_markets=caps,
        max_hours_to_close=horizons,
    ),
)
policies = st.builds(
    UniversePolicy,
    min_volume_24h=st.integers(0, 1_000).map(CountE2),
    max_l2_markets=st.integers(0, 15),
    groups=st.lists(group_rules, max_size=4).map(
        lambda rules: tuple(
            msgspec.structs.replace(rule, name=f"g{index}") for index, rule in enumerate(rules)
        )
    ),
    exclude_mve=st.booleans(),
    max_seconds_to_close=st.none() | st.integers(1, 2 * HOUR),
)
category_maps = st.dictionaries(st.sampled_from(SERIES), st.sampled_from((*CATEGORY_NAMES, "C3")))


def is_eligible(market: MarketSummary, rules: UniversePolicy) -> bool:
    """Reference model of the filters that run before any group."""
    if market.status != ACTIVE_STATUS or (rules.exclude_mve and market.is_mve):
        return False
    if market.close_ts is None:
        return True
    within_horizon = (
        rules.max_seconds_to_close is None or market.close_ts - NOW <= rules.max_seconds_to_close
    )
    return market.close_ts > NOW and within_horizon


@given(listings, policies, category_maps, st.data())
@settings(max_examples=300)
def test_select_does_not_depend_on_the_order_markets_arrive_in(
    markets: list[MarketSummary],
    rules: UniversePolicy,
    categories: dict[str, str],
    data: st.DataObject,
) -> None:
    shuffled = data.draw(st.permutations(markets))
    assert select(shuffled, rules, now_ts=NOW, categories=categories) == select(
        markets, rules, now_ts=NOW, categories=categories
    )


@given(listings, policies, category_maps)
@settings(max_examples=300)
def test_select_respects_every_cap_and_accounts_for_every_market(
    markets: list[MarketSummary], rules: UniversePolicy, categories: dict[str, str]
) -> None:
    decision = select(markets, rules, now_ts=NOW, categories=categories)
    by_ticker = {market.ticker: market for market in markets}
    assert len(decision.l2_tickers) <= rules.max_l2_markets
    assert sum(decision.reason_counts.values()) + len(decision.l2_tickers) == len(markets)
    skipped = sum(group.skipped_for_budget for group in decision.groups)
    assert decision.dropped_for_cap == decision.reason_counts[REASON_OVER_CAP] == skipped
    assert [market.ticker for market in decision.markets] == sorted(decision.l2_tickers)
    assert [group.name for group in decision.groups] == [group.name for group in rules.groups]
    everywhere = [ticker for group in decision.groups for ticker in group.tickers]
    assert len(everywhere) == len(set(everywhere))
    assert set(everywhere) == decision.l2_tickers == set(decision.group_of)
    for group, selection in zip(rules.groups, decision.groups, strict=True):
        members = [by_ticker[ticker] for ticker in selection.tickers]
        assert all(decision.group_of[market.ticker] == group.name for market in members)
        assert all(is_eligible(market, rules) for market in members)
        events = {(market.series_ticker, market.event_ticker) for market in members}
        if group.series is not None:
            assert all(market.series_ticker in group.series for market in members)
            assert set(selection.tickers) <= decision.showcase
            assert all(n <= group.events for n in Counter(s for s, _ in events).values())
        else:
            assert all(categories.get(m.series_ticker) == group.category for m in members)
            assert not set(selection.tickers) & decision.showcase
            assert len(events) <= group.events
        per_event = Counter(market.event_ticker for market in members)
        assert all(n <= group.markets_per_event for n in per_event.values())
        assert group.max_markets is None or len(members) <= group.max_markets
        assert selection.events == len(events)
        if group.max_hours_to_close is not None:
            # Every admitted event has an eligible market closing within the horizon.
            latest_close_ts = NOW + group.max_hours_to_close * HOUR
            for key in events:
                assert any(
                    (market.series_ticker, market.event_ticker) == key
                    and is_eligible(market, rules)
                    and market.close_ts is not None
                    and market.close_ts <= latest_close_ts
                    for market in markets
                )


@given(listings, policies, category_maps)
@settings(max_examples=200)
def test_the_budget_is_spent_in_group_order_and_later_groups_never_change_earlier_ones(
    markets: list[MarketSummary], rules: UniversePolicy, categories: dict[str, str]
) -> None:
    decision = select(markets, rules, now_ts=NOW, categories=categories)
    for count in range(len(rules.groups)):
        prefix = msgspec.structs.replace(rules, groups=rules.groups[:count])
        earlier = select(markets, prefix, now_ts=NOW, categories=categories)
        assert earlier.groups == decision.groups[:count]
    short = [index for index, group in enumerate(decision.groups) if group.skipped_for_budget]
    if short:
        assert len(decision.l2_tickers) == rules.max_l2_markets
        assert all(not group.tickers for group in decision.groups[short[0] + 1 :])
