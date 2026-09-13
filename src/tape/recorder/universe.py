"""Which markets earn full order-book capture, and why every other one did not.

Responsibility: turn a REST market listing into the set of tickers the recorder subscribes to
on ``orderbook_delta`` and ``trade`` (docs/ARCHITECTURE.md 5.1) by applying the configured
universe groups in order (ADR 0028), with the counts that explain every market not recorded
and what each group admitted. The module is pure: it reads no clock and performs no I/O, so
``now_ts`` and the series categories are supplied by the caller and the same input always
produces the same decision.

Invariants: a rejected market never appears in the decision; every admitted market was
admitted by exactly one group, the first in order that chose it, and no group sees a market an
earlier group chose; the decision holds at most ``max_l2_markets`` markets; a group admits at
most ``markets_per_event`` markets of one event, at most ``max_markets`` in all, and at most
``events`` events, counted per series in a series group, and, when it sets
``max_hours_to_close``, only events whose earliest close is within that horizon; the reason
counts plus the size of
``l2_tickers`` account for every market handed in; the decision's summaries describe exactly
the markets in ``l2_tickers``; and nothing depends on the order the markets arrive in.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Final

import msgspec

from tape.errors import WireError
from tape.fixedpoint import CountE2, parse_count
from tape.wire.rest import Market

__all__ = [
    "ACTIVE_STATUS",
    "DEFAULT_EXCHANGE_INDEX",
    "REASONS",
    "REASON_BELOW_VOLUME",
    "REASON_BEYOND_HORIZON",
    "REASON_CLOSED",
    "REASON_DUPLICATE",
    "REASON_EVENT_BEYOND_HORIZON",
    "REASON_EVENT_NOT_CHOSEN",
    "REASON_MVE",
    "REASON_NOT_ACTIVE",
    "REASON_NO_GROUP",
    "REASON_OVER_CAP",
    "REASON_OVER_EVENT_CAP",
    "REASON_OVER_GROUP_CAP",
    "GroupSelection",
    "MarketSummary",
    "UniverseDecision",
    "UniverseGroup",
    "UniversePolicy",
    "select",
]

ACTIVE_STATUS: Final = "active"
"""The only market status worth an order-book subscription; everything else is idle."""

DEFAULT_EXCHANGE_INDEX: Final = 0
"""Shard assumed when a market omits ``exchange_index``.

The pinned spec marks the field ``x-omitempty: false``, so the server always sends it;
a payload without one predates sharding and therefore belongs to the main exchange.
"""

REASON_DUPLICATE: Final = "duplicate"
REASON_NOT_ACTIVE: Final = "not_active"
REASON_MVE: Final = "mve"
REASON_CLOSED: Final = "closed"
REASON_BEYOND_HORIZON: Final = "beyond_horizon"
REASON_NO_GROUP: Final = "no_group"
"""Eligible, but no group selects it: its series is in no series group, and its category, if
known, is in no category group."""
REASON_EVENT_BEYOND_HORIZON: Final = "event_beyond_horizon"
"""Its event's earliest close is unknown or beyond the group's ``max_hours_to_close``."""
REASON_BELOW_VOLUME: Final = "below_volume"
"""In a category group, its event's 24-hour volume, summed over the event, is under the floor."""
REASON_EVENT_NOT_CHOSEN: Final = "event_not_chosen"
"""Its event ranked beyond the group's ``events``."""
REASON_OVER_EVENT_CAP: Final = "over_markets_per_event"
"""Its event was chosen, but it ranked beyond the group's ``markets_per_event``."""
REASON_OVER_GROUP_CAP: Final = "over_max_markets"
"""The group chose it beyond its ``max_markets``."""
REASON_OVER_CAP: Final = "over_cap"
"""A group chose it after ``max_l2_markets`` was reached: skipped for budget."""

REASONS: Final = (
    REASON_DUPLICATE,
    REASON_NOT_ACTIVE,
    REASON_MVE,
    REASON_CLOSED,
    REASON_BEYOND_HORIZON,
    REASON_NO_GROUP,
    REASON_EVENT_BEYOND_HORIZON,
    REASON_BELOW_VOLUME,
    REASON_EVENT_NOT_CHOSEN,
    REASON_OVER_EVENT_CAP,
    REASON_OVER_GROUP_CAP,
    REASON_OVER_CAP,
)
"""Every reason a listed market is not recorded, in the order selection applies them.

Reported with a zero count when unused. A market that several groups pass over is counted under
the reason of the first; one that a later group chooses is not counted under any.
"""

_SERIES_SEPARATOR: Final = "-"
_UNIX_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_SECOND: Final = timedelta(seconds=1)
_GO_ZERO_YEAR: Final = 1
"""Year of Go's zero time, which Kalshi sends for a timestamp it does not have."""
_SECONDS_PER_HOUR: Final = 3_600
_NO_CATEGORIES: Final[Mapping[str, str]] = MappingProxyType({})


class UniverseGroup(msgspec.Struct, frozen=True, kw_only=True):
    """One rule of the universe: which markets it selects, and how many (ADR 0028).

    Attributes:
        name: Names the group in the universe log and the preview; unique within a policy.
        events: Events admitted. A series group counts per series: the nearest ``events``
            open events of each series, by the earliest close among each event's markets. A
            category group counts across the group: the ``events`` events with the highest
            24-hour volume, summed over their markets, at or above the policy's floor.
        markets_per_event: Most markets admitted from one event, highest 24-hour volume first.
        series: Series tickers the group selects from, in the order their events are admitted.
            Exactly one of ``series`` and ``category`` is set.
        category: Kalshi series category the group selects from, for example ``Sports``.
        max_markets: Most markets the group admits in all, or ``None`` for no such cap.
        max_hours_to_close: Horizon of the group. When set, an event is eligible for the group
            only if the earliest close among the markets the group can see is at most this many
            hours after ``now_ts``, inclusive; an event with no known close is not. It applies
            before events are ranked, so a series group takes the nearest events within it and
            a category group ranks only events within it. ``None`` sets no horizon.

    Raises:
        ValueError: On an empty name, both selectors or neither, an empty or repeated series, an
            empty category, or a count or horizon that is not positive. The message names the
            group.
    """

    name: str
    events: int
    markets_per_event: int
    series: tuple[str, ...] | None = None
    category: str | None = None
    max_markets: int | None = None
    max_hours_to_close: int | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("a universe group needs a non-empty name")
        label = f"universe group {self.name!r}"
        if (self.series is None) == (self.category is None):
            raise ValueError(f"{label} must set exactly one of series and category")
        if self.series is not None:
            if not self.series or not all(self.series):
                raise ValueError(f"{label}: series must list at least one non-empty series ticker")
            repeated = sorted({series for series in self.series if self.series.count(series) > 1})
            if repeated:
                raise ValueError(f"{label} lists {', '.join(repeated)} more than once")
        if self.category == "":
            raise ValueError(f"{label}: category must not be empty")
        for field, value in (
            ("events", self.events),
            ("markets_per_event", self.markets_per_event),
            ("max_markets", self.max_markets),
            ("max_hours_to_close", self.max_hours_to_close),
        ):
            if value is not None and value <= 0:
                raise ValueError(f"{label}: {field} must be positive, got {value}")


class UniversePolicy(msgspec.Struct, frozen=True, kw_only=True):
    """The configured shape of the L2 universe (docs/INTERFACES.md 17).

    Attributes:
        min_volume_24h: Floor on an event's 24-hour volume, summed over its markets, for a
            category group to choose it. Series groups do not apply it.
        max_l2_markets: Budget for order-book subscriptions. Groups are applied in order
            until it is reached.
        groups: The universe groups, in the order they are applied. With none, nothing is
            captured.
        exclude_mve: Drop markets belonging to a multivariate event collection; their
            books are derived from their legs and are not worth the subscription.
        max_seconds_to_close: Horizon. When set, only markets closing within this many
            seconds of ``now_ts`` are captured; ``None`` captures every horizon.

    Raises:
        ValueError: On a negative volume floor or budget, or a non-positive horizon, each of
            which would silently empty the universe, or on two groups with the same name.
    """

    min_volume_24h: CountE2
    max_l2_markets: int
    groups: tuple[UniverseGroup, ...]
    exclude_mve: bool = True
    max_seconds_to_close: int | None = None

    def __post_init__(self) -> None:
        if self.min_volume_24h < 0:
            raise ValueError(f"min_volume_24h must be non-negative, got {self.min_volume_24h}")
        if self.max_l2_markets < 0:
            raise ValueError(f"max_l2_markets must be non-negative, got {self.max_l2_markets}")
        if self.max_seconds_to_close is not None and self.max_seconds_to_close <= 0:
            raise ValueError(
                f"max_seconds_to_close must be positive, got {self.max_seconds_to_close}"
            )
        names = [group.name for group in self.groups]
        repeated = sorted({name for name in names if names.count(name) > 1})
        if repeated:
            raise ValueError(
                f"universe group names must be unique; repeated: {', '.join(map(repr, repeated))}"
            )

    @property
    def categories(self) -> frozenset[str]:
        """The categories that category groups select from; empty when there is no such group."""
        return frozenset(group.category for group in self.groups if group.category is not None)


class MarketSummary(msgspec.Struct, frozen=True, kw_only=True):
    """The part of a market the selector reads, already in fixed point.

    Attributes:
        ticker: Market ticker; the primary key everywhere (docs/DATA_FORMATS.md 1.4).
        series_ticker: Series this market belongs to; series groups select by it, and category
            groups by its category.
        event_ticker: Event this market belongs to; groups count and cap markets per event.
        exchange_index: Exchange shard. It matters for collateral and order routing, not
            for market data, so the planner ignores it (ADR 0020).
        status: Kalshi's market status, verbatim.
        volume_24h: Contracts traded in the last 24 hours.
        close_ts: Unix seconds at which the market closes, or ``None`` when unknown.
        is_mve: Whether the market is a leg of a multivariate event collection.
    """

    ticker: str
    series_ticker: str
    event_ticker: str
    exchange_index: int
    status: str
    volume_24h: CountE2
    close_ts: int | None
    is_mve: bool

    @classmethod
    def from_wire(cls, market: Market, *, is_mve: bool = False) -> MarketSummary:
        """Adapt a REST market into the selector's view.

        ``tape.wire.rest.Market`` mirrors the pinned spec, which carries no
        ``series_ticker``, so the series is the ticker's first dash-separated segment,
        the convention every Kalshi ticker follows. It also omits
        ``mve_collection_ticker``, so membership of a multivariate collection cannot be
        read off the payload and arrives as ``is_mve`` from the caller that knows;
        the universe query passes ``mve_filter=exclude`` anyway, so the flag is a second
        line of defence rather than the first.

        Args:
            market: Decoded ``GET /markets`` entry.
            is_mve: Whether this market is a leg of a multivariate event collection.

        Returns:
            The summary of that market.

        Raises:
            WireError: If ``close_time`` is present but is not an ISO-8601 instant.
            FixedPointError: If ``volume_24h_fp`` is not an exact fixed-point count.
        """
        return cls(
            ticker=market.ticker,
            series_ticker=market.ticker.split(_SERIES_SEPARATOR, 1)[0],
            event_ticker=market.event_ticker,
            exchange_index=(
                DEFAULT_EXCHANGE_INDEX if market.exchange_index is None else market.exchange_index
            ),
            status=market.status,
            volume_24h=parse_count(market.volume_24h_fp),
            close_ts=_parse_close_ts(market.close_time, ticker=market.ticker),
            is_mve=is_mve,
        )


class GroupSelection(msgspec.Struct, frozen=True, kw_only=True):
    """What one universe group admitted.

    Attributes:
        name: The group's name.
        tickers: Markets the group admitted, in the order it ranked them: event by event, and
            within an event by descending 24-hour volume.
        events: Events with at least one admitted market.
        skipped_for_budget: Markets the group chose that did not fit in ``max_l2_markets``.
    """

    name: str
    tickers: tuple[str, ...]
    events: int
    skipped_for_budget: int


class UniverseDecision(msgspec.Struct, frozen=True, kw_only=True):
    """What the recorder subscribes to, and the arithmetic behind it.

    Attributes:
        l2_tickers: Every market to capture on ``orderbook_delta`` and ``trade``; never more
            than ``max_l2_markets``.
        showcase: The subset admitted by series groups, which the catalog flags (ADR 0028).
        markets: The summary of every market in ``l2_tickers``, in ticker order; for a ticker
            listed twice, the copy selection kept. The recorder publishes them as its catalog
            (ADR 0023).
        groups: What each group admitted, one entry per group in policy order.
        group_of: The name of the group that admitted each market of ``l2_tickers``.
        dropped_for_cap: Markets that groups chose after the budget was reached; the sum of
            every group's ``skipped_for_budget``.
        reason_counts: Count per entry of :data:`REASONS`, zeros included, so every report has
            the same shape. Summed with ``len(l2_tickers)`` it equals the number of markets
            handed in.
    """

    l2_tickers: frozenset[str]
    showcase: frozenset[str]
    markets: tuple[MarketSummary, ...]
    groups: tuple[GroupSelection, ...]
    group_of: Mapping[str, str]
    dropped_for_cap: int
    reason_counts: Mapping[str, int]


def _parse_close_ts(close_time: str, *, ticker: str) -> int | None:
    """Convert an ISO-8601 close time to Unix seconds.

    Args:
        close_time: The market's ``close_time``, possibly empty or Go's zero time.
        ticker: Market the value came from, for the error message.

    Returns:
        Unix seconds, or ``None`` when Kalshi expressed "no close time known".

    Raises:
        WireError: If the value is non-empty and cannot be parsed.
    """
    if not close_time:
        return None
    try:
        parsed = datetime.fromisoformat(close_time)
    except ValueError as exc:
        raise WireError(f"{ticker}: unparsable close_time {close_time!r}") from exc
    if parsed.year == _GO_ZERO_YEAR:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return (parsed - _UNIX_EPOCH) // _ONE_SECOND


def _rejection(market: MarketSummary, policy: UniversePolicy, *, now_ts: int) -> str | None:
    """Return the reason this market is not eligible, or ``None`` if it is.

    Filters run in the order of :data:`REASONS`. A market with no known close time
    passes both time filters: an unknown close is not evidence of a closed market.

    Args:
        market: The market under consideration.
        policy: The configured universe shape.
        now_ts: Unix seconds the decision is being made at.

    Returns:
        A member of :data:`REASONS`, or ``None`` when the market is eligible.
    """
    if market.status != ACTIVE_STATUS:
        return REASON_NOT_ACTIVE
    if policy.exclude_mve and market.is_mve:
        return REASON_MVE
    if market.close_ts is not None:
        if market.close_ts <= now_ts:
            return REASON_CLOSED
        if (
            policy.max_seconds_to_close is not None
            and market.close_ts - now_ts > policy.max_seconds_to_close
        ):
            return REASON_BEYOND_HORIZON
    return None


def select(
    markets: Sequence[MarketSummary],
    policy: UniversePolicy,
    *,
    now_ts: int,
    categories: Mapping[str, str] = _NO_CATEGORIES,
) -> UniverseDecision:
    """Choose the markets to capture in full.

    Eligibility is decided first (status, multivariate legs, closed markets, the close
    horizon). A ticker repeated across pages, which happens when a market is updated
    mid-pagination, is counted once under :data:`REASON_DUPLICATE`. The copy kept is chosen by
    content, not arrival order: the larger 24-hour volume wins, because volume only grows while
    a market trades, and any remaining tie is broken by comparing every other field.

    The groups then apply in order, each to the eligible markets that no earlier group chose.
    A group with ``max_hours_to_close`` first sets aside every event whose earliest close is
    unknown or later than that many hours after ``now_ts``. A series group takes its series in
    the order listed and, of each, the ``events`` events whose earliest market close is nearest;
    an event with no known close ranks last. A category
    group takes the ``events`` events of its category with the highest 24-hour volume summed
    over the event, among those at or above ``min_volume_24h``. From each chosen event a group
    takes at most ``markets_per_event`` markets by descending volume, and of all those, the
    first ``max_markets``. Ties break by close time, then ticker, so the result does not depend
    on the order the pages arrived in. Chosen markets are admitted in that order until
    ``max_l2_markets`` is reached; the rest, and everything later groups choose, are skipped for
    budget.

    Args:
        markets: Summaries of every market the recorder knows about.
        policy: The configured universe shape.
        now_ts: Unix seconds the decision is being made at.
        categories: Category of each series, by series ticker. Only category groups read it,
            and a series missing from it matches none of them.

    Returns:
        The decision, with the reason counts and what each group admitted.
    """
    counts = dict.fromkeys(REASONS, 0)
    eligible = _eligible(markets, policy, now_ts=now_ts, counts=counts)
    unchosen = {market.ticker: market for market in eligible}
    passed_over: dict[str, str] = {}
    group_of: dict[str, str] = {}
    showcase: set[str] = set()
    selections: list[GroupSelection] = []
    budget = policy.max_l2_markets
    for group in policy.groups:
        candidates = [m for m in unchosen.values() if _selects(group, m, categories)]
        chosen = _choose(
            group,
            candidates,
            floor=policy.min_volume_24h,
            now_ts=now_ts,
            rejected=passed_over,
        )
        admitted = chosen[:budget]
        budget -= len(admitted)
        for market in chosen:
            del unchosen[market.ticker]
        for market in admitted:
            group_of[market.ticker] = group.name
        if group.series is not None:
            showcase.update(market.ticker for market in admitted)
        skipped = len(chosen) - len(admitted)
        counts[REASON_OVER_CAP] += skipped
        selections.append(
            GroupSelection(
                name=group.name,
                tickers=tuple(market.ticker for market in admitted),
                events=len({_event_key(market) for market in admitted}),
                skipped_for_budget=skipped,
            )
        )
    for ticker in unchosen:
        counts[passed_over.get(ticker, REASON_NO_GROUP)] += 1
    return UniverseDecision(
        l2_tickers=frozenset(group_of),
        showcase=frozenset(showcase),
        # ``eligible`` is in ticker order and holds one copy per ticker.
        markets=tuple(market for market in eligible if market.ticker in group_of),
        groups=tuple(selections),
        group_of=group_of,
        dropped_for_cap=counts[REASON_OVER_CAP],
        reason_counts=counts,
    )


def _eligible(
    markets: Sequence[MarketSummary],
    policy: UniversePolicy,
    *,
    now_ts: int,
    counts: dict[str, int],
) -> list[MarketSummary]:
    """Keep one copy of each ticker and drop ineligible markets, counting each under its reason.

    Args:
        markets: Every market handed to :func:`select`.
        policy: The configured universe shape.
        now_ts: Unix seconds the decision is being made at.
        counts: The reason counts, incremented in place.

    Returns:
        The eligible markets, in ticker order.
    """
    newest: dict[str, MarketSummary] = {}
    for market in markets:
        held = newest.get(market.ticker)
        if held is None:
            newest[market.ticker] = market
            continue
        counts[REASON_DUPLICATE] += 1
        if _freshness(market) > _freshness(held):
            newest[market.ticker] = market
    eligible: list[MarketSummary] = []
    for market in sorted(newest.values(), key=lambda summary: summary.ticker):
        reason = _rejection(market, policy, now_ts=now_ts)
        if reason is None:
            eligible.append(market)
        else:
            counts[reason] += 1
    return eligible


def _selects(group: UniverseGroup, market: MarketSummary, categories: Mapping[str, str]) -> bool:
    """Whether a market is in a group's selector: its series, or its series' category."""
    if group.series is not None:
        return market.series_ticker in group.series
    return categories.get(market.series_ticker) == group.category


def _choose(
    group: UniverseGroup,
    candidates: Sequence[MarketSummary],
    *,
    floor: CountE2,
    now_ts: int,
    rejected: dict[str, str],
) -> list[MarketSummary]:
    """Rank a group's candidates and pick the markets it would admit with budget to spare.

    Args:
        group: The group.
        candidates: The eligible markets in its selector that no earlier group chose, in
            ticker order.
        floor: The policy's floor on an event's summed 24-hour volume, for category groups.
        now_ts: Unix seconds the decision is being made at, for the group's horizon.
        rejected: Reason each candidate was passed over, by ticker; filled in for candidates
            not chosen, keeping any reason an earlier group recorded.

    Returns:
        The chosen markets, in the order the group admits them.
    """
    grouped: dict[tuple[str, str], list[MarketSummary]] = {}
    for market in candidates:
        grouped.setdefault(_event_key(market), []).append(market)
    events = _within_horizon(grouped.values(), group, now_ts=now_ts, rejected=rejected)
    if group.series is None:
        chosen_events = _busiest_events(events, group, floor=floor, rejected=rejected)
    else:
        chosen_events = _nearest_events(events, group, group.series, rejected=rejected)
    chosen: list[MarketSummary] = []
    for event in chosen_events:
        ranked = sorted(event, key=_busiest_market_first)
        chosen.extend(ranked[: group.markets_per_event])
        _reject(ranked[group.markets_per_event :], REASON_OVER_EVENT_CAP, rejected)
    if group.max_markets is not None:
        _reject(chosen[group.max_markets :], REASON_OVER_GROUP_CAP, rejected)
        del chosen[group.max_markets :]
    return chosen


def _within_horizon(
    events: Iterable[list[MarketSummary]],
    group: UniverseGroup,
    *,
    now_ts: int,
    rejected: dict[str, str],
) -> list[list[MarketSummary]]:
    """The events whose earliest known close is within the group's horizon, if it has one."""
    if group.max_hours_to_close is None:
        return list(events)
    latest_close_ts = now_ts + group.max_hours_to_close * _SECONDS_PER_HOUR
    within: list[list[MarketSummary]] = []
    for event in events:
        close_ts = _earliest_close(event)
        if close_ts is not None and close_ts <= latest_close_ts:
            within.append(event)
        else:
            _reject(event, REASON_EVENT_BEYOND_HORIZON, rejected)
    return within


def _nearest_events(
    events: Iterable[list[MarketSummary]],
    group: UniverseGroup,
    series: Sequence[str],
    *,
    rejected: dict[str, str],
) -> list[list[MarketSummary]]:
    """The ``events`` nearest events of each series, series in the order listed."""
    by_series: dict[str, list[list[MarketSummary]]] = {}
    for event in events:
        by_series.setdefault(event[0].series_ticker, []).append(event)
    chosen: list[list[MarketSummary]] = []
    for name in series:
        ranked = sorted(by_series.get(name, ()), key=_nearest_event_first)
        chosen.extend(ranked[: group.events])
        for event in ranked[group.events :]:
            _reject(event, REASON_EVENT_NOT_CHOSEN, rejected)
    return chosen


def _busiest_events(
    events: Iterable[list[MarketSummary]],
    group: UniverseGroup,
    *,
    floor: CountE2,
    rejected: dict[str, str],
) -> list[list[MarketSummary]]:
    """The ``events`` events with the highest summed 24-hour volume at or above the floor."""
    qualified: list[list[MarketSummary]] = []
    for event in events:
        if _event_volume(event) < floor:
            _reject(event, REASON_BELOW_VOLUME, rejected)
        else:
            qualified.append(event)
    ranked = sorted(qualified, key=_busiest_event_first)
    for event in ranked[group.events :]:
        _reject(event, REASON_EVENT_NOT_CHOSEN, rejected)
    return ranked[: group.events]


def _reject(markets: Iterable[MarketSummary], reason: str, rejected: dict[str, str]) -> None:
    """Record why markets were passed over, unless an earlier group already said why."""
    for market in markets:
        rejected.setdefault(market.ticker, reason)


def _event_key(market: MarketSummary) -> tuple[str, str]:
    """An event's identity. The series is part of it so no event can straddle two series."""
    return (market.series_ticker, market.event_ticker)


def _event_volume(event: Sequence[MarketSummary]) -> int:
    """An event's 24-hour volume: the sum over the markets a group can see."""
    return sum(market.volume_24h for market in event)


def _close_order(close_ts: int | None) -> tuple[int, int]:
    """Sort key for a close time: sooner first, unknown last."""
    return (1, 0) if close_ts is None else (0, close_ts)


def _earliest_close(event: Sequence[MarketSummary]) -> int | None:
    """The earliest known close among an event's markets, or ``None`` if none is known."""
    return min((m.close_ts for m in event if m.close_ts is not None), default=None)


def _nearest_event_first(event: Sequence[MarketSummary]) -> tuple[tuple[int, int], str]:
    """Series-group order: earliest close first, then event ticker."""
    return (_close_order(_earliest_close(event)), event[0].event_ticker)


def _busiest_event_first(
    event: Sequence[MarketSummary],
) -> tuple[int, tuple[int, int], str, str]:
    """Category-group order: highest summed volume, then earliest close, then event and series."""
    first = event[0]
    return (
        -_event_volume(event),
        _close_order(_earliest_close(event)),
        first.event_ticker,
        first.series_ticker,
    )


def _busiest_market_first(market: MarketSummary) -> tuple[int, tuple[int, int], str]:
    """Order within an event: highest volume, then earliest close, then ticker."""
    return (-market.volume_24h, _close_order(market.close_ts), market.ticker)


def _freshness(market: MarketSummary) -> tuple[int, int, str, str, str, int, bool]:
    """Total order used to pick one copy of a ticker seen twice in one listing.

    Volume first, because it only grows while a market trades, so the larger value is the
    later observation. The remaining fields make the order total, so the winner never
    depends on which page arrived first.
    """
    return (
        market.volume_24h,
        -1 if market.close_ts is None else market.close_ts,
        market.status,
        market.event_ticker,
        market.series_ticker,
        market.exchange_index,
        market.is_mve,
    )
