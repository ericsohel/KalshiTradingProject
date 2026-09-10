"""Live smoke tests against Kalshi's public production endpoints.

These are the only tests that leave the machine. They need no credentials, because
Kalshi's market-data endpoints answer unauthenticated, and they exist to catch the one
class of bug the offline suite cannot: the published specification disagreeing with the
running exchange. That is not hypothetical. On 2026-09-10 the fee-changes endpoint
returned a ``fee_type`` absent from the pinned enum, which is why ADR 0017 exists.

They are skipped unless ``TAPE_TEST_ENV=prod`` so that an offline run, or a run during
a Kalshi outage, never fails the build.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest

from tape.client.ratelimit import BucketRateLimiter
from tape.client.rest import KalshiRest, build_client
from tape.timeutil import SystemClock

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("TAPE_TEST_ENV") != "prod",
        reason="set TAPE_TEST_ENV=prod to run live public smoke tests",
    ),
]

PROD_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"


@pytest.fixture
async def rest() -> AsyncIterator[KalshiRest]:
    """A client pointed at production with no signer; public endpoints only."""
    clock = SystemClock()
    client = build_client(PROD_BASE_URL)
    async with client:
        yield KalshiRest(PROD_BASE_URL, client, BucketRateLimiter(clock), clock)


async def test_exchange_status_decodes(rest: KalshiRest) -> None:
    status = await rest.exchange_status()
    assert isinstance(status.exchange_active, bool)
    assert isinstance(status.trading_active, bool)


async def test_markets_paginate_and_decode(rest: KalshiRest) -> None:
    page = await rest.markets(status="open", limit=5, mve_filter="exclude")
    assert 0 < len(page.items) <= 5
    market = page.items[0]
    assert market.ticker
    assert market.status
    # Fixed-point strings, not numbers: the wire layer must not have coerced them.
    assert isinstance(market.yes_bid_dollars, str)
    assert isinstance(market.volume_24h_fp, str)


async def test_orderbook_and_batch_orderbooks_decode(rest: KalshiRest) -> None:
    page = await rest.markets(status="open", limit=5, mve_filter="exclude")
    tickers = [m.ticker for m in page.items]
    single = await rest.orderbook(tickers[0])
    for level in (single.yes_dollars or []) + (single.no_dollars or []):
        assert len(level) == 2
    batch = await rest.orderbooks(tickers)
    assert {book.ticker for book in batch} <= set(tickers)


async def test_series_and_fee_changes_decode(rest: KalshiRest) -> None:
    """The regression guard for ADR 0017: live fee types need not be in the spec."""
    series = await rest.series()
    assert len(series) > 1000
    assert all(s.fee_type for s in series)
    changes = await rest.fee_changes(show_historical=True)
    assert all(change.series_ticker and change.fee_type for change in changes)


async def test_iter_markets_follows_cursors(rest: KalshiRest) -> None:
    seen: set[str] = set()
    async for market in rest.iter_markets(status="open", limit=200, mve_filter="exclude"):
        seen.add(market.ticker)
        if len(seen) >= 400:
            break
    assert len(seen) >= 400, "pagination stopped early or repeated a page"
