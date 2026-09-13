"""The universe's inputs from Kalshi: the open-market listing and the cached series categories."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from typing import Final

import httpx
import pytest

from tape.client.ratelimit import Bucket, BucketLimits
from tape.client.rest import KalshiRest
from tape.recorder.listing import (
    CATEGORY_REFRESH_S,
    MARKET_PAGE_LIMIT,
    SeriesCategories,
    list_open_markets,
    list_series_markets,
    series_per_category,
)
from tape.timeutil import NS_PER_S, FrozenClock
from tests.fakes.rest_payloads import market_payload, series_payload

BASE_URL: Final = "https://rest.test/trade-api/v2"
LOGGER: Final = "tape.recorder.listing"

type Answer = httpx.Response | Callable[[httpx.Request], httpx.Response]


class FreeLimiter:
    """Never waits."""

    async def acquire(self, cost: int, *, bucket: Bucket) -> None:
        _ = (cost, bucket)

    def resize(self, *, read: BucketLimits, write: BucketLimits) -> None:
        """Never called here; present only to satisfy the protocol."""


class Exchange:
    """Canned answers by path, and for ``/series`` by category; a queue serves in order, then
    repeats its last answer."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.answers: dict[str, list[Answer]] = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path.removeprefix("/trade-api/v2")
        category = request.url.params.get("category")
        queue = self.answers[path if category is None else f"{path}?category={category}"]
        answer = queue.pop(0) if len(queue) > 1 else queue[0]
        return answer if isinstance(answer, httpx.Response) else answer(request)


def page(*markets: dict[str, object], cursor: str = "") -> httpx.Response:
    return httpx.Response(200, json={"markets": list(markets), "cursor": cursor})


def series_list(*entries: tuple[str, str]) -> httpx.Response:
    return httpx.Response(200, json={"series": [series_payload(t, c) for t, c in entries]})


def refused(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("connection refused", request=request)


@pytest.fixture
def exchange() -> Exchange:
    return Exchange()


@pytest.fixture
async def rest(exchange: Exchange) -> AsyncIterator[KalshiRest]:
    transport = httpx.MockTransport(exchange)
    async with httpx.AsyncClient(transport=transport, base_url=BASE_URL) as http:
        yield KalshiRest(BASE_URL, http, FreeLimiter(), FrozenClock())


# -------------------------------------------------------------------- listing


async def test_a_listing_follows_the_cursor_and_counts_markets_that_do_not_convert(
    exchange: Exchange, rest: KalshiRest
) -> None:
    exchange.answers["/markets"] = [
        page(market_payload("A-E-1", volume_24h="12.34"), cursor="page-2"),
        page(
            market_payload("B-E-1", volume_24h="lots"),
            market_payload("B-E-2", close_time="tomorrow"),
            market_payload("B-E-3"),
        ),
    ]
    listing = await list_open_markets(rest, exclude_mve=True, max_pages=5)

    assert [market.ticker for market in listing.markets] == ["A-E-1", "B-E-3"]
    assert listing.markets[0].volume_24h == 1234
    assert (listing.truncated, listing.unreadable) == (False, 2)
    assert listing.first_error.startswith("FixedPointError(")
    first, second = (dict(request.url.params) for request in exchange.requests)
    assert first == {"status": "open", "limit": str(MARKET_PAGE_LIMIT), "mve_filter": "exclude"}
    assert second == first | {"cursor": "page-2"}


async def test_a_listing_cut_short_by_the_page_cap_says_so(
    exchange: Exchange, rest: KalshiRest
) -> None:
    exchange.answers["/markets"] = [page(market_payload("A-E-1"), cursor="more")]
    listing = await list_open_markets(rest, exclude_mve=False, max_pages=2)

    assert listing.truncated
    assert len(listing.markets) == 2
    assert (listing.unreadable, listing.first_error) == (0, "")
    assert len(exchange.requests) == 2
    assert not any("mve_filter" in request.url.params for request in exchange.requests)
    with pytest.raises(ValueError, match="max_pages must be positive, got 0"):
        await list_open_markets(rest, exclude_mve=True, max_pages=0)


async def test_a_series_listing_filters_each_series_and_keeps_only_its_markets(
    exchange: Exchange, rest: KalshiRest
) -> None:
    """ADR 0029: one filtered listing per series, capped per series, merged in the order named."""

    def by_series(request: httpx.Request) -> httpx.Response:
        series = request.url.params["series_ticker"]
        if series == "KXA":
            cursor = request.url.params.get("cursor")
            if cursor is None:
                return page(market_payload("KXA-E-1"), cursor="page-2")
            return page(market_payload("KXA-E-2"), market_payload("KXA-E-BAD", volume_24h="x"))
        # A filter the server ignored brings in another series' market, which is left out.
        return page(market_payload("KXB-E-1"), market_payload("KXOTHER-E-1"), cursor="more")

    exchange.answers["/markets"] = [by_series]
    listing = await list_series_markets(rest, ["KXB", "KXA", "KXB"], exclude_mve=True, max_pages=2)

    assert [market.ticker for market in listing.markets] == [
        "KXB-E-1",
        "KXB-E-1",
        "KXA-E-1",
        "KXA-E-2",
    ]
    assert (listing.truncated, listing.unreadable) == (True, 1)
    assert listing.first_error.startswith("FixedPointError(")
    params = [dict(request.url.params) for request in exchange.requests]
    base = {"status": "open", "limit": str(MARKET_PAGE_LIMIT), "mve_filter": "exclude"}
    assert params == [
        base | {"series_ticker": "KXB"},
        base | {"series_ticker": "KXB", "cursor": "more"},
        base | {"series_ticker": "KXA"},
        base | {"series_ticker": "KXA", "cursor": "page-2"},
    ]
    with pytest.raises(ValueError, match="max_pages must be positive, got 0"):
        await list_series_markets(rest, ["KXA"], exclude_mve=True, max_pages=0)


# ----------------------------------------------------------------- categories


async def test_categories_are_fetched_per_category_asking_for_nothing_optional_and_kept_an_hour(
    exchange: Exchange, rest: KalshiRest, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.INFO, logger=LOGGER)
    clock = FrozenClock()
    exchange.answers["/series?category=Politics"] = [
        series_list(("PRES", "Politics")),
        series_list(("PRES", "Politics"), ("SENATE", "Politics")),
    ]
    # A server that ignored the filter would send other categories too; they are not kept.
    exchange.answers["/series?category=Sports"] = [series_list(("NFL", "Sports"), ("FED", "Econ"))]
    categories = SeriesCategories(rest, categories=["Sports", "Politics", "Sports"], clock=clock)
    assert (categories.categories, categories.known) == (("Politics", "Sports"), False)

    assert dict(await categories.current()) == {"PRES": "Politics", "NFL": "Sports"}
    assert categories.known
    assert [dict(request.url.params) for request in exchange.requests] == [
        {"category": "Politics"},
        {"category": "Sports"},
    ]
    (fetched,) = [r for r in caplog.records if r.getMessage() == "series categories fetched"]
    assert fetched.__dict__["series_by_category"] == {"Politics": 1, "Sports": 1}

    clock.advance((CATEGORY_REFRESH_S - 1) * NS_PER_S)
    assert dict(await categories.current()) == {"PRES": "Politics", "NFL": "Sports"}
    assert len(exchange.requests) == 2
    clock.advance(NS_PER_S)
    assert dict(await categories.current()) == {
        "PRES": "Politics",
        "SENATE": "Politics",
        "NFL": "Sports",
    }
    assert len(exchange.requests) == 4


async def test_a_failed_fetch_keeps_the_last_known_map_whole_and_is_retried_at_the_next_call(
    exchange: Exchange, rest: KalshiRest, caplog: pytest.LogCaptureFixture
) -> None:
    clock = FrozenClock()
    exchange.answers["/series?category=Politics"] = [
        series_list(("PRES", "Politics")),
        series_list(("SENATE", "Politics")),
    ]
    exchange.answers["/series?category=Sports"] = [
        series_list(("NFL", "Sports")),
        httpx.Response(503),
        series_list(("NBA", "Sports")),
    ]
    categories = SeriesCategories(rest, categories=["Politics", "Sports"], clock=clock)
    known = dict(await categories.current())
    clock.advance(CATEGORY_REFRESH_S * NS_PER_S)

    # Politics answers anew, but Sports fails, so neither change is taken.
    assert dict(await categories.current()) == known == {"PRES": "Politics", "NFL": "Sports"}
    (failure,) = [r for r in caplog.records if r.getMessage().startswith("series categories not")]
    assert failure.levelno == logging.WARNING
    assert (failure.__dict__["known"], failure.__dict__["series"]) == (True, 2)
    assert failure.__dict__["error"].startswith("KalshiHttpError(")
    # A failure does not wait out the hour: the next call tries again.
    assert dict(await categories.current()) == {"SENATE": "Politics", "NBA": "Sports"}
    assert len(exchange.requests) == 6


@pytest.mark.parametrize(
    "failure",
    [
        httpx.Response(500),
        httpx.Response(200, json={"series": [{"ticker": "NFL"}]}),
        refused,
    ],
    ids=["http-error", "undecodable", "transport-error"],
)
async def test_before_any_success_the_map_is_empty_and_every_call_tries_again(
    exchange: Exchange, rest: KalshiRest, failure: Answer
) -> None:
    clock = FrozenClock()
    exchange.answers["/series?category=Sports"] = [failure, failure, series_list(("NFL", "Sports"))]
    categories = SeriesCategories(rest, categories=["Sports"], clock=clock)
    for _ in range(2):
        assert dict(await categories.current()) == {}
        assert not categories.known
    assert dict(await categories.current()) == {"NFL": "Sports"}
    assert categories.known
    assert len(exchange.requests) == 3


async def test_series_categories_refuse_settings_that_cannot_work(rest: KalshiRest) -> None:
    clock = FrozenClock()
    with pytest.raises(ValueError, match="at least one non-empty category"):
        SeriesCategories(rest, categories=[], clock=clock)
    with pytest.raises(ValueError, match="at least one non-empty category"):
        SeriesCategories(rest, categories=["Sports", ""], clock=clock)
    with pytest.raises(ValueError, match="refresh_s must be positive, got 0"):
        SeriesCategories(rest, categories=["Sports"], clock=clock, refresh_s=0)


def test_series_per_category_reports_zeros_so_a_misspelled_category_shows() -> None:
    by_series = {"NFL": "Sports", "NBA": "Sports", "PRES": "Politics"}
    assert series_per_category(by_series, ["sports", "Sports"]) == {"sports": 0, "Sports": 2}
