"""Metadata resolution: nulls until resolved, one request per event and series, deduplication,
TTL, pacing, failure backoff, and bounds, against a mock exchange in virtual time."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Final

import httpx
import pytest

from tape.api import UNRESOLVED, MarketMetadata, MetadataResolver, metadata_limits
from tape.api.contract import PriceRange
from tape.client.ratelimit import BucketLimits, BucketRateLimiter
from tape.client.rest import KalshiRest
from tape.events import CatalogEntry
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import NS_PER_S, FrozenClock

BASE_URL: Final = "https://kalshi.test/trade-api/v2"
EVENT: Final = "KXHIGHNY-26SEP10"
SERIES: Final = "KXHIGHNY"
GRID: Final = (("0.0100", "0.9900", "0.0100"),)


def entry(ticker: str, *, event: str = EVENT, series: str = SERIES) -> CatalogEntry:
    return CatalogEntry(
        ticker=ticker,
        series_ticker=series,
        event_ticker=event,
        volume_24h=CountE2(0),
        close_ts=None,
        showcase=False,
    )


B84: Final = entry(f"{EVENT}-B84.5")
B86: Final = entry(f"{EVENT}-B86.5")


def market(ticker: str, subtitle: str, grid: tuple[tuple[str, str, str], ...] = GRID) -> object:
    return {
        "ticker": ticker,
        "event_ticker": ticker.rsplit("-", 1)[0],
        "market_type": "binary",
        "yes_sub_title": subtitle,
        "no_sub_title": subtitle,
        "created_time": "2026-09-09T14:00:00Z",
        "updated_time": "2026-09-09T14:00:00Z",
        "open_time": "2026-09-09T14:00:00Z",
        "close_time": "2026-09-11T04:59:00Z",
        "latest_expiration_time": "2026-09-18T14:00:00Z",
        "settlement_timer_seconds": 1800,
        "status": "active",
        "notional_value_dollars": "1.0000",
        "yes_bid_dollars": "0.4000",
        "yes_ask_dollars": "0.4200",
        "no_bid_dollars": "0.5800",
        "no_ask_dollars": "0.6000",
        "yes_bid_size_fp": "10.00",
        "yes_ask_size_fp": "10.00",
        "last_price_dollars": "0.4100",
        "previous_yes_bid_dollars": "0.4000",
        "previous_yes_ask_dollars": "0.4200",
        "previous_price_dollars": "0.4100",
        "volume_fp": "100.00",
        "volume_24h_fp": "100.00",
        "open_interest_fp": "50.00",
        "result": "",
        "can_close_early": True,
        "expiration_value": "",
        "rules_primary": "",
        "rules_secondary": "",
        "price_level_structure": "linear_cent",
        "price_ranges": [{"start": s, "end": e, "step": t} for s, e, t in grid],
    }


@dataclass
class Exchange:
    """Kalshi's public event and series endpoints, scripted; it also records request times."""

    clock: FrozenClock
    title: str = "Highest temperature in NYC today?"
    category: str = "Climate and Weather"
    markets: list[object] = field(
        default_factory=lambda: [
            market(B84.ticker, "84° to 85°"),
            market(B86.ticker, "86° to 87°"),
        ]
    )
    failures: dict[str, httpx.Response] = field(default_factory=dict)
    hold: asyncio.Event | None = None
    requests: list[tuple[str, int]] = field(default_factory=list)

    @property
    def paths(self) -> list[str]:
        return [path for path, _ in self.requests]

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/trade-api/v2")
        self.requests.append((path, int(self.clock.mono_ns())))
        if self.hold is not None:
            await self.hold.wait()
        if path in self.failures:
            return self.failures[path]
        kind, _, ticker = path.strip("/").partition("/")
        if kind == "events":
            return httpx.Response(200, json=self.event_body(ticker))
        return httpx.Response(200, json=self.series_body(ticker))

    def event_body(self, event_ticker: str) -> object:
        return {
            "event": {
                "event_ticker": event_ticker,
                "series_ticker": event_ticker.split("-", 1)[0],
                "sub_title": "On Sep 10, 2026",
                "title": self.title,
                "collateral_return_type": "",
                "mutually_exclusive": True,
                "settlement_sources": None,
            },
            "markets": self.markets,
        }

    def series_body(self, series_ticker: str) -> object:
        return {
            "series": {
                "ticker": series_ticker,
                "frequency": "daily",
                "title": "Highest temperature in NYC",
                "category": self.category,
                "tags": None,
                "settlement_sources": None,
                "contract_url": "",
                "contract_terms_url": "",
                "fee_type": "quadratic",
                "fee_multiplier": 1,
                "additional_prohibitions": None,
            }
        }


@dataclass
class Setup:
    resolver: MetadataResolver
    rest: KalshiRest
    exchange: Exchange
    clock: FrozenClock
    slept: list[float]

    async def settled(self, requests: int) -> None:
        """Wait until the resolver has attempted this many requests and has nothing queued."""
        await until(
            lambda: self.resolver.stats.requests == requests and self.resolver.stats.pending == 0
        )


@asynccontextmanager
async def running(
    *, requests_per_s: int = 2, ttl_s: int = 3600, run: bool = True
) -> AsyncIterator[Setup]:
    clock = FrozenClock(mono_ns=1)
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        clock.advance(round(seconds * NS_PER_S))
        await asyncio.sleep(0)

    exchange = Exchange(clock)
    limits = metadata_limits(requests_per_s)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(exchange), base_url=BASE_URL
    ) as http:
        limiter = BucketRateLimiter(clock, read=limits, write=limits, sleep=sleep)
        rest = KalshiRest(BASE_URL, http, limiter, clock)
        resolver = MetadataResolver(rest, clock=clock, ttl_s=ttl_s)
        task = asyncio.create_task(resolver.run()) if run else None
        try:
            yield Setup(resolver, rest, exchange, clock, slept)
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


async def until(predicate: Callable[[], bool], *, timeout_s: float = 5.0) -> None:
    async with asyncio.timeout(timeout_s):
        while not predicate():
            await asyncio.sleep(0.001)


async def test_metadata_is_null_until_one_event_and_one_series_request_resolve_it() -> None:
    async with running() as setup:
        resolver = setup.resolver
        assert resolver.lookup(B84) == UNRESOLVED

        resolver.request([B84, B86])
        assert resolver.lookup(B84) == UNRESOLVED
        await setup.settled(requests=2)

        assert resolver.lookup(B84) == MarketMetadata(
            title="Highest temperature in NYC today?",
            subtitle="84° to 85°",
            category="Climate and Weather",
            price_ranges=(
                PriceRange(start_e4=PriceE4(100), end_e4=PriceE4(9900), step_e4=PriceE4(100)),
            ),
        )
        assert resolver.lookup(B86).subtitle == "86° to 87°"
        # A market the event does not list yet still gets the event's title and category.
        late = resolver.lookup(entry(f"{EVENT}-B88.5"))
        assert (late.title, late.subtitle, late.category, late.price_ranges) == (
            "Highest temperature in NYC today?",
            None,
            "Climate and Weather",
            None,
        )
        assert setup.exchange.paths == [f"/events/{EVENT}", f"/series/{SERIES}"]
        stats = resolver.stats
        assert (stats.requests, stats.failures, stats.events, stats.series) == (2, 0, 1, 1)


async def test_an_event_or_series_is_requested_once_while_its_request_is_in_flight() -> None:
    async with running() as setup:
        hold = setup.exchange.hold = asyncio.Event()
        setup.resolver.request([B84])
        await until(lambda: setup.exchange.paths == [f"/events/{EVENT}"])

        setup.resolver.request([B84, B86])
        setup.resolver.request([B86])
        assert setup.resolver.stats.pending == 2
        hold.set()
        await setup.settled(requests=2)

        assert setup.exchange.paths == [f"/events/{EVENT}", f"/series/{SERIES}"]


async def test_values_are_refetched_only_after_the_ttl_and_served_until_replaced() -> None:
    async with running(ttl_s=60) as setup:
        resolver = setup.resolver
        resolver.request([B84])
        await setup.settled(requests=2)

        setup.clock.advance(59 * NS_PER_S)
        resolver.request([B84])
        assert resolver.stats.pending == 0

        setup.exchange.title = "Highest temperature in NYC on Sep 10?"
        setup.clock.advance(1 * NS_PER_S)
        resolver.request([B84])
        assert resolver.stats.pending == 2
        assert resolver.lookup(B84).title == "Highest temperature in NYC today?"
        await setup.settled(requests=4)

        assert resolver.lookup(B84).title == "Highest temperature in NYC on Sep 10?"


async def test_requests_are_paced_by_the_resolver_s_own_token_bucket() -> None:
    async with running(requests_per_s=2) as setup:
        start_ns = int(setup.clock.mono_ns())
        setup.resolver.request(
            [entry(f"KXE{n}-1", event=f"KXE{n}", series=f"KXS{n}") for n in range(3)]
        )
        await setup.settled(requests=6)

    offsets = [(at_ns - start_ns) / NS_PER_S for _, at_ns in setup.exchange.requests]
    # Two requests of burst, then one every half second.
    assert offsets == [0.0, 0.0, 0.5, 1.0, 1.5, 2.0]
    assert setup.slept == [0.5, 0.5, 0.5, 0.5]


async def test_a_failure_is_logged_and_counted_and_retried_only_after_a_doubling_backoff(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="tape.api.metadata")
    async with running() as setup:
        resolver, exchange = setup.resolver, setup.exchange
        exchange.failures[f"/events/{EVENT}"] = httpx.Response(503)
        resolver.request([B84])
        await setup.settled(requests=2)

        metadata = resolver.lookup(B84)
        assert (metadata.title, metadata.category) == (None, "Climate and Weather")
        assert resolver.stats.failures == 1
        (warning,) = [r for r in caplog.records if r.getMessage().startswith("metadata not")]
        assert (warning.__dict__["kind"], warning.__dict__["key"]) == ("event", EVENT)
        assert (warning.__dict__["consecutive_failures"], warning.__dict__["retry_in_s"]) == (1, 30)

        for wait_s, queued in ((29, 0), (1, 1)):
            setup.clock.advance(wait_s * NS_PER_S)
            resolver.request([B84])
            assert resolver.stats.pending == queued
        await setup.settled(requests=3)
        assert caplog.records[-1].__dict__["retry_in_s"] == 60

        del exchange.failures[f"/events/{EVENT}"]
        for wait_s, queued in ((59, 0), (1, 1)):
            setup.clock.advance(wait_s * NS_PER_S)
            resolver.request([B84])
            assert resolver.stats.pending == queued
        await setup.settled(requests=4)
        assert resolver.lookup(B84).title == "Highest temperature in NYC today?"
        assert resolver.stats.failures == 2


async def test_a_body_that_does_not_decode_is_a_failure_like_any_other() -> None:
    async with running() as setup:
        setup.exchange.failures[f"/series/{SERIES}"] = httpx.Response(200, content=b"{}")
        setup.resolver.request([B84])
        await setup.settled(requests=2)

    assert setup.resolver.lookup(B84).category is None
    assert setup.resolver.stats.failures == 1


async def test_an_unparsable_price_grid_leaves_only_that_market_without_one() -> None:
    async with running() as setup:
        setup.exchange.markets = [
            market(B84.ticker, "84° to 85°", grid=(("0.0100", "1.5000", "0.0100"),)),
            market(B86.ticker, "86° to 87°"),
        ]
        setup.resolver.request([B84])
        await setup.settled(requests=2)

    broken, intact = setup.resolver.lookup(B84), setup.resolver.lookup(B86)
    assert (broken.subtitle, broken.price_ranges) == ("84° to 85°", None)
    assert intact.price_ranges is not None
    assert setup.resolver.stats.unparsable == 1


async def test_the_queue_and_the_cache_are_bounded() -> None:
    async with running(run=False) as setup:
        resolver = MetadataResolver(
            setup.rest, clock=setup.clock, ttl_s=60, max_pending=2, max_cached=3
        )
        resolver.request([entry(f"KXE{n}-1", event=f"KXE{n}") for n in range(4)])

    stats = resolver.stats
    # KXE0 and the shared series fill the queue; KXE1 to KXE3 are refused, and KXE1, the least
    # recently requested entry that is not queued, is forgotten.
    assert (stats.pending, stats.refused, stats.events, stats.series) == (2, 3, 3, 1)


async def test_metadata_limits_and_resolver_bounds_are_checked() -> None:
    assert metadata_limits(2) == BucketLimits(refill_per_s=20, capacity=20)
    with pytest.raises(ValueError, match="requests_per_s must be positive"):
        metadata_limits(0)
    async with running(run=False) as setup:
        with pytest.raises(ValueError, match="ttl_s must be positive"):
            MetadataResolver(setup.rest, clock=setup.clock, ttl_s=0)
        with pytest.raises(ValueError, match="retry_max_s must be at least retry_initial_s"):
            MetadataResolver(setup.rest, clock=setup.clock, ttl_s=1, retry_max_s=1)
