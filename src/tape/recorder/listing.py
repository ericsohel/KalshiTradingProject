"""What universe selection reads from Kalshi: the open markets and the series categories.

Responsibility: page through ``GET /markets`` into the selector's summaries
(:func:`list_open_markets`), and keep the map from series to category that category groups
select by (ADR 0028), read from ``GET /series`` filtered by category
(:class:`SeriesCategories`). Every request goes through :class:`tape.client.rest.KalshiRest`,
whose rate limiter paces it, and ``tape record`` and ``tape universe preview`` share this
module, so a preview lists exactly what the recorder would.

Invariants: a listing fetches at most ``max_pages`` pages and says whether that cap cut it
short; a market that does not convert is skipped and counted, never fatal; the category map
holds only the configured categories and changes only when every one of them was fetched, so a
failed fetch keeps the last known map whole; after a success, no request is made until
``refresh_s`` has passed on the monotonic clock; a failed fetch is logged, never raised, and
tried again at the next call; and nothing here reads time except through the injected clock.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Final

import msgspec

from tape.client.rest import KalshiRest
from tape.errors import FixedPointError, KalshiError, WireError
from tape.recorder.universe import MarketSummary
from tape.timeutil import NS_PER_S, Clock

__all__ = [
    "CATEGORY_REFRESH_S",
    "DEFAULT_MAX_MARKET_PAGES",
    "MARKET_PAGE_LIMIT",
    "MarketListing",
    "SeriesCategories",
    "list_open_markets",
    "series_per_category",
]

MARKET_PAGE_LIMIT: Final = 1000
"""Markets per ``GET /markets`` page, the largest Kalshi serves."""

DEFAULT_MAX_MARKET_PAGES: Final = 500
"""Page cap on one universe listing (docs/ENGINEERING_STANDARDS.md 3.6)."""

CATEGORY_REFRESH_S: Final = 3_600
"""Seconds after a successful fetch before the series categories are fetched again (ADR 0028).

A series rarely changes category; a new one joins its category group within the hour."""

_OPEN_MARKET_STATUS: Final = "open"
_MVE_FILTER_EXCLUDE: Final = "exclude"
_NOTHING_KNOWN: Final[Mapping[str, str]] = MappingProxyType({})


class MarketListing(msgspec.Struct, frozen=True, kw_only=True):
    """The open markets, as one listing read them.

    Attributes:
        markets: A summary of every market that converted, in the order the pages arrived.
        truncated: Whether the page cap cut the listing short, leaving the universe partial.
        unreadable: Markets skipped because they did not convert.
        first_error: Why the first skipped market did not convert; empty when none was.
    """

    markets: tuple[MarketSummary, ...]
    truncated: bool
    unreadable: int
    first_error: str


async def list_open_markets(
    rest: KalshiRest, *, exclude_mve: bool, max_pages: int
) -> MarketListing:
    """Page through the open markets, up to the page cap.

    A market that does not convert is skipped and counted rather than failing the whole
    listing, because one malformed entry must not unsubscribe every other market.

    Args:
        rest: The REST client; its limiter paces every page.
        exclude_mve: Ask Kalshi to leave out legs of multivariate event collections.
        max_pages: Most pages fetched; positive.

    Returns:
        The summaries, and whether the listing was cut short or skipped any market.

    Raises:
        ValueError: If ``max_pages`` is not positive.
        KalshiError: If a page cannot be fetched.
        WireError: If a page does not decode.
    """
    if max_pages <= 0:
        raise ValueError(f"max_pages must be positive, got {max_pages}")
    mve_filter = _MVE_FILTER_EXCLUDE if exclude_mve else None
    markets: list[MarketSummary] = []
    unreadable = 0
    first_error = ""
    cursor: str | None = None
    truncated = True
    for _ in range(max_pages):
        page = await rest.markets(
            status=_OPEN_MARKET_STATUS,
            cursor=cursor,
            limit=MARKET_PAGE_LIMIT,
            mve_filter=mve_filter,
        )
        for market in page.items:
            try:
                markets.append(MarketSummary.from_wire(market))
            except (WireError, FixedPointError) as exc:
                unreadable += 1
                first_error = first_error or repr(exc)
        if not page.cursor:
            truncated = False
            break
        cursor = page.cursor
    return MarketListing(
        markets=tuple(markets),
        truncated=truncated,
        unreadable=unreadable,
        first_error=first_error,
    )


class SeriesCategories:
    """The category of every series in the configured categories, cached for ``refresh_s``.

    See the module docstring. Not thread-safe; used on one event loop.

    Args:
        rest: The REST client; its limiter paces every request.
        categories: Kalshi series categories to fetch, such as ``Sports``; at least one.
        clock: Time for the refresh interval.
        refresh_s: Seconds after a successful fetch before the next one; positive.
        logger: Destination for logs; defaults to this module's logger.

    Raises:
        ValueError: If no category is given, one is empty, or ``refresh_s`` is not positive.
    """

    def __init__(
        self,
        rest: KalshiRest,
        *,
        categories: Iterable[str],
        clock: Clock,
        refresh_s: int = CATEGORY_REFRESH_S,
        logger: logging.Logger | None = None,
    ) -> None:
        wanted = tuple(sorted(set(categories)))
        if not wanted or not all(wanted):
            raise ValueError(f"at least one non-empty category is required, got {wanted}")
        if refresh_s <= 0:
            raise ValueError(f"refresh_s must be positive, got {refresh_s}")
        self._rest = rest
        self._categories = wanted
        self._clock = clock
        self._refresh_ns = refresh_s * NS_PER_S
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._by_series = _NOTHING_KNOWN
        self._fetched_ns: int | None = None

    @property
    def categories(self) -> tuple[str, ...]:
        """The configured categories, in name order."""
        return self._categories

    @property
    def known(self) -> bool:
        """Whether a fetch has succeeded; until one has, the map is empty."""
        return self._fetched_ns is not None

    async def current(self) -> Mapping[str, str]:
        """Map series tickers to their categories, fetching first when a fetch is due.

        A fetch is due until one succeeds, and then again ``refresh_s`` after it. It sends one
        ``GET /series?category=<category>`` per configured category and asks for nothing
        optional, such as volumes. If any request fails or does not decode, the failure is
        logged and the last known map is returned unchanged.

        Returns:
            A read-only map from series ticker to category, holding only the configured
            categories; empty until a fetch succeeds.
        """
        now_ns = int(self._clock.mono_ns())
        if self._fetched_ns is not None and now_ns - self._fetched_ns < self._refresh_ns:
            return self._by_series
        try:
            fetched = await self._fetch()
        except (KalshiError, WireError) as exc:
            self._log.warning(
                "series categories not fetched; keeping the last known",
                extra={
                    "error": repr(exc),
                    "categories": list(self._categories),
                    "known": self.known,
                    "series": len(self._by_series),
                },
            )
            return self._by_series
        self._by_series = MappingProxyType(fetched)
        self._fetched_ns = now_ns
        self._log.info(
            "series categories fetched",
            extra={"series_by_category": series_per_category(fetched, self._categories)},
        )
        return self._by_series

    async def _fetch(self) -> dict[str, str]:
        """Fetch every configured category, or raise on the first failure.

        Raises:
            KalshiError: If a request fails.
            WireError: If a response does not decode.
        """
        wanted = frozenset(self._categories)
        by_series: dict[str, str] = {}
        for category in self._categories:
            for series in await self._rest.series(category=category):
                # Kept under the category the series reports, not the one requested, so a
                # filter the server ignored cannot put a series in the wrong group.
                if series.category in wanted:
                    by_series[series.ticker] = series.category
        return by_series


def series_per_category(by_series: Mapping[str, str], categories: Iterable[str]) -> dict[str, int]:
    """Count the series known in each category, zeros included, so a misspelled one shows.

    Args:
        by_series: Series ticker to category, as :meth:`SeriesCategories.current` returns it.
        categories: The categories to report, in the order to report them.

    Returns:
        The number of series per category.
    """
    counts = dict.fromkeys(categories, 0)
    for category in by_series.values():
        if category in counts:
            counts[category] += 1
    return counts
