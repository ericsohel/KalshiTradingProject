"""Which markets earn full order-book capture, and why every other one did not.

Responsibility: turn a REST market listing into the set of tickers the recorder
subscribes to on ``orderbook_delta`` and ``trade`` (docs/ARCHITECTURE.md 5.1), plus the
counts that explain every exclusion so the daily manifest can account for the whole
listing. The module is pure: it reads no clock and performs no I/O, so ``now_ts`` is
supplied by the caller and the same input always produces the same decision.

Invariants: a rejected market never appears in the decision; a showcase market that
passed the filters is always captured, even when that puts the count over
``max_l2_markets``, because a showcase series is a reason the tape exists; the
reason counts plus the size of ``l2_tickers`` account for every market handed in; and
the decision's summaries describe exactly the markets in ``l2_tickers``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
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
    "REASON_MVE",
    "REASON_NOT_ACTIVE",
    "REASON_OVER_CAP",
    "MarketSummary",
    "UniverseDecision",
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
REASON_BELOW_VOLUME: Final = "below_volume"
REASON_OVER_CAP: Final = "over_cap"

REASONS: Final = (
    REASON_DUPLICATE,
    REASON_NOT_ACTIVE,
    REASON_MVE,
    REASON_CLOSED,
    REASON_BEYOND_HORIZON,
    REASON_BELOW_VOLUME,
    REASON_OVER_CAP,
)
"""Every exclusion reason, in filter order. Reported with a zero count when unused."""

_SERIES_SEPARATOR: Final = "-"
_UNIX_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_ONE_SECOND: Final = timedelta(seconds=1)
_GO_ZERO_YEAR: Final = 1
"""Year of Go's zero time, which Kalshi sends for a timestamp it does not have."""


class UniversePolicy(msgspec.Struct, frozen=True, kw_only=True):
    """The configured shape of the L2 universe (docs/INTERFACES.md 17).

    Attributes:
        min_volume_24h: Floor on ``volume_24h_fp``. A market below it is captured only
            because its series is a showcase series.
        max_l2_markets: Budget for order-book subscriptions. Showcase markets are
            admitted before the budget is applied and may exceed it.
        showcase_series: Series tickers captured whatever their volume.
        exclude_mve: Drop markets belonging to a multivariate event collection; their
            books are derived from their legs and are not worth the subscription.
        max_seconds_to_close: Horizon. When set, only markets closing within this many
            seconds of ``now_ts`` are captured; ``None`` captures every horizon.

    Raises:
        ValueError: On a negative volume floor or budget, or a non-positive horizon;
            each would silently empty the universe.
    """

    min_volume_24h: CountE2
    max_l2_markets: int
    showcase_series: frozenset[str]
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


class MarketSummary(msgspec.Struct, frozen=True, kw_only=True):
    """The part of a market the selector reads, already in fixed point.

    Attributes:
        ticker: Market ticker; the primary key everywhere (docs/DATA_FORMATS.md 1.4).
        series_ticker: Series this market belongs to; matched against the showcase list.
        event_ticker: Event this market belongs to; carried for the manifest.
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


class UniverseDecision(msgspec.Struct, frozen=True, kw_only=True):
    """What the recorder subscribes to, and the arithmetic behind it.

    Attributes:
        l2_tickers: Every market to capture on ``orderbook_delta`` and ``trade``.
        showcase: The subset admitted because their series is a showcase series; always
            contained in ``l2_tickers``.
        markets: The summary of every market in ``l2_tickers``, in ticker order; for a ticker
            listed twice, the copy selection kept. The recorder publishes them as its catalog
            (ADR 0023).
        dropped_for_cap: Markets that passed every filter but did not fit the budget.
            When the showcase alone fills the budget this is every other qualifying
            market, and ``len(l2_tickers)`` is then above ``max_l2_markets``.
        reason_counts: Count per entry of :data:`REASONS`, zeros included, so the daily
            manifest has a fixed schema. Summed with ``len(l2_tickers)`` it equals the
            number of markets handed in.
    """

    l2_tickers: frozenset[str]
    showcase: frozenset[str]
    markets: tuple[MarketSummary, ...]
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
    markets: Sequence[MarketSummary], policy: UniversePolicy, *, now_ts: int
) -> UniverseDecision:
    """Choose the markets to capture in full.

    Eligibility is decided first (status, multivariate legs, closed markets, the close
    horizon). Showcase-series markets are then admitted unconditionally, and the
    remaining budget goes to the highest 24-hour volume above the floor, ties broken by
    ticker so the result does not depend on the order the pages arrived in. A ticker
    repeated across pages, which happens when a market is updated mid-pagination, is
    counted once under :data:`REASON_DUPLICATE`. The copy kept is chosen by content, not
    arrival order: the larger 24-hour volume wins, because volume only grows while a
    market trades, and any remaining tie is broken by comparing every other field.

    Args:
        markets: Summaries of every market the recorder knows about.
        policy: The configured universe shape.
        now_ts: Unix seconds the decision is being made at.

    Returns:
        The decision, with the reason counts for the manifest.
    """
    counts = dict.fromkeys(REASONS, 0)
    eligible: list[MarketSummary] = []
    newest: dict[str, MarketSummary] = {}
    for market in markets:
        held = newest.get(market.ticker)
        if held is None:
            newest[market.ticker] = market
            continue
        counts[REASON_DUPLICATE] += 1
        if _freshness(market) > _freshness(held):
            newest[market.ticker] = market
    for market in sorted(newest.values(), key=lambda summary: summary.ticker):
        reason = _rejection(market, policy, now_ts=now_ts)
        if reason is None:
            eligible.append(market)
        else:
            counts[reason] += 1

    showcase = frozenset(
        market.ticker for market in eligible if market.series_ticker in policy.showcase_series
    )
    qualified: list[MarketSummary] = []
    for market in eligible:
        if market.ticker in showcase:
            continue
        if market.volume_24h < policy.min_volume_24h:
            counts[REASON_BELOW_VOLUME] += 1
        else:
            qualified.append(market)

    qualified.sort(key=lambda market: (-market.volume_24h, market.ticker))
    budget = max(0, policy.max_l2_markets - len(showcase))
    admitted = qualified[:budget]
    counts[REASON_OVER_CAP] = len(qualified) - len(admitted)
    l2_tickers = showcase | frozenset(market.ticker for market in admitted)
    return UniverseDecision(
        l2_tickers=l2_tickers,
        showcase=showcase,
        # ``eligible`` is in ticker order and holds one copy per ticker.
        markets=tuple(market for market in eligible if market.ticker in l2_tickers),
        dropped_for_cap=counts[REASON_OVER_CAP],
        reason_counts=counts,
    )


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
