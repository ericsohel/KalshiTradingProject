"""``KalshiRest``: signing, rate limiting, pagination, batch limits, and error mapping."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from pathlib import Path
from typing import Final

import httpx
import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from tape.client.auth import HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP, RsaPssSigner
from tape.client.ratelimit import DEFAULT_TOKEN_COST, Bucket, BucketLimits
from tape.client.rest import KalshiRest, build_client
from tape.errors import KalshiHttpError, KalshiTransportError, RateLimitedError, WireError
from tape.fixedpoint import CountE2
from tape.timeutil import NS_PER_MS, FrozenClock
from tape.wire.rest import CreateOrderV2Request

BASE_URL: Final = "https://api.example.com/trade-api/v2"

Action = httpx.Response | Callable[[httpx.Request], httpx.Response]


class FakeRateLimiter:
    """Records every ``acquire`` call instead of actually limiting anything."""

    def __init__(self) -> None:
        self.calls: list[tuple[int, Bucket]] = []

    async def acquire(self, cost: int, *, bucket: Bucket) -> None:
        self.calls.append((cost, bucket))

    def resize(self, *, read: BucketLimits, write: BucketLimits) -> None:
        """Never exercised by the REST client; present only to satisfy the protocol."""


class Router:
    """A queue of canned responses (or raised exceptions) keyed by method and path."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._queues: dict[tuple[str, str], list[Action]] = {}

    def add(self, method: str, path: str, action: Action) -> None:
        self._queues.setdefault((method, path), []).append(action)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        # httpx always appends the request path to the client's base_url path
        # (docs/DATA_FORMATS.md 2.2 base URLs end in "/trade-api/v2"), so requests
        # arrive with that prefix; routes are registered without it.
        path = request.url.path.removeprefix("/trade-api/v2")
        key = (request.method, path)
        queue = self._queues.get(key)
        if not queue:
            raise AssertionError(f"unexpected request: {key}")
        action = queue.pop(0) if len(queue) > 1 else queue[0]
        if isinstance(action, httpx.Response):
            return action
        return action(request)


def make_client(router: Router, base_url: str = BASE_URL) -> httpx.AsyncClient:
    """Build an ``httpx.AsyncClient`` whose transport is ``router``, no network."""
    return httpx.AsyncClient(transport=httpx.MockTransport(router), base_url=base_url)


def _raise(exc: httpx.HTTPError) -> Callable[[httpx.Request], httpx.Response]:
    def _raiser(request: httpx.Request) -> httpx.Response:
        exc.request = request
        raise exc

    return _raiser


def _market_json(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "ticker": "T-1",
        "event_ticker": "E-1",
        "market_type": "binary",
        "yes_sub_title": "Yes",
        "no_sub_title": "No",
        "created_time": "2024-01-01T00:00:00Z",
        "updated_time": "2024-01-01T00:00:00Z",
        "open_time": "2024-01-01T00:00:00Z",
        "close_time": "2024-01-02T00:00:00Z",
        "latest_expiration_time": "2024-01-02T00:00:00Z",
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
    base.update(overrides)
    return base


def _candlestick_json(**overrides: object) -> dict[str, object]:
    distribution = {
        "open_dollars": "0.10",
        "low_dollars": "0.10",
        "high_dollars": "0.10",
        "close_dollars": "0.10",
    }
    base: dict[str, object] = {
        "end_period_ts": 1,
        "yes_bid": distribution,
        "yes_ask": distribution,
        "price": {},
        "volume_fp": "0.00",
        "open_interest_fp": "0.00",
    }
    base.update(overrides)
    return base


def _settlement_json(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "ticker": "T",
        "exchange_index": 0,
        "event_ticker": "E",
        "market_result": "yes",
        "yes_count_fp": "1.00",
        "yes_total_cost_dollars": "1.00",
        "no_count_fp": "0.00",
        "no_total_cost_dollars": "0.00",
        "revenue": 100,
        "settled_time": "2024-01-01T00:00:00Z",
        "fee_cost": "0.01",
    }
    base.update(overrides)
    return base


def _fill_json(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "fill_id": "f1",
        "exchange_index": 0,
        "trade_id": "t1",
        "order_id": "o1",
        "ticker": "T",
        "market_ticker": "T",
        "outcome_side": "yes",
        "book_side": "bid",
        "count_fp": "1.00",
        "yes_price_dollars": "0.50",
        "no_price_dollars": "0.50",
        "is_taker": True,
        "fee_cost": "0.01",
    }
    base.update(overrides)
    return base


@pytest.fixture(scope="module")
def rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture
def signer(tmp_path: Path, rsa_key: rsa.RSAPrivateKey) -> RsaPssSigner:
    path = tmp_path / "key.pem"
    path.write_bytes(
        rsa_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    return RsaPssSigner("kid-1", path)


def _verify_signature(
    rsa_key: rsa.RSAPrivateKey, request: httpx.Request, expected_path: str, now_ms: int
) -> None:
    assert request.headers[HEADER_KEY] == "kid-1"
    assert request.headers[HEADER_TIMESTAMP] == str(now_ms)
    message = f"{now_ms}{request.method}{expected_path}".encode()
    rsa_key.public_key().verify(
        base64.b64decode(request.headers[HEADER_SIGNATURE]),
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256.digest_size),
        hashes.SHA256(),
    )


# --- construction -------------------------------------------------------------------


def test_build_client_sets_base_url_and_timeout() -> None:
    client = build_client(BASE_URL, timeout_s=5.0)
    assert str(client.base_url) == f"{BASE_URL}/"  # httpx normalizes with a trailing slash
    assert client.timeout == httpx.Timeout(5.0)


# --- signing --------------------------------------------------------------------------


async def test_public_endpoint_needs_no_signer_and_sends_no_auth_headers() -> None:
    router = Router()
    router.add(
        "GET",
        "/exchange/status",
        httpx.Response(200, json={"exchange_active": True, "trading_active": True}),
    )
    limiter = FakeRateLimiter()
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, limiter, FrozenClock(), signer=None)
        status = await rest.exchange_status()
    assert status.exchange_active is True
    assert status.trading_active is True
    assert HEADER_KEY not in router.requests[0].headers
    assert limiter.calls == [(DEFAULT_TOKEN_COST, "read")]


async def test_signed_request_signs_path_with_prefix_and_excludes_query_string(
    signer: RsaPssSigner, rsa_key: rsa.RSAPrivateKey
) -> None:
    router = Router()
    router.add("GET", "/markets", httpx.Response(200, json={"markets": [], "cursor": ""}))
    wall_ns = 1_700_000_000_123_456_789
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock(wall_ns=wall_ns), signer)
        await rest.markets(cursor="abc123", limit=5)
    request = router.requests[0]
    assert request.url.params.get("cursor") == "abc123"
    _verify_signature(rsa_key, request, "/trade-api/v2/markets", wall_ns // NS_PER_MS)


async def test_signer_absent_means_no_headers_even_on_private_endpoint() -> None:
    router = Router()
    router.add(
        "GET",
        "/account/limits",
        httpx.Response(
            200,
            json={
                "usage_tier": "basic",
                "read": {"refill_rate": 200, "bucket_capacity": 400},
                "write": {"refill_rate": 100, "bucket_capacity": 100},
                "grants": [],
            },
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock(), signer=None)
        limits = await rest.account_limits()
    assert limits.usage_tier == "basic"
    assert HEADER_KEY not in router.requests[0].headers


# --- rate limiting ----------------------------------------------------------------


async def test_reads_cost_default_tokens_from_the_read_bucket() -> None:
    router = Router()
    router.add("GET", "/markets", httpx.Response(200, json={"markets": [], "cursor": ""}))
    limiter = FakeRateLimiter()
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, limiter, FrozenClock())
        await rest.markets()
    assert limiter.calls == [(DEFAULT_TOKEN_COST, "read")]


async def test_create_order_costs_default_tokens_from_the_write_bucket() -> None:
    router = Router()
    router.add(
        "POST",
        "/portfolio/events/orders",
        httpx.Response(
            201,
            json={"order_id": "o1", "fill_count": "0.00", "remaining_count": "10.00", "ts_ms": 1},
        ),
    )
    limiter = FakeRateLimiter()
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, limiter, FrozenClock())
        req = CreateOrderV2Request(
            ticker="T",
            side="bid",
            count="10.00",
            price="0.5600",
            time_in_force="good_till_canceled",
            self_trade_prevention_type="maker",
        )
        await rest.create_order(req)
    assert limiter.calls == [(DEFAULT_TOKEN_COST, "write")]


async def test_cancel_order_and_cancel_all_cost_two_tokens() -> None:
    router = Router()
    router.add(
        "DELETE",
        "/portfolio/events/orders/o1",
        httpx.Response(200, json={"order_id": "o1", "reduced_by": "10.00", "ts_ms": 1}),
    )
    router.add("DELETE", "/portfolio/events/orders", httpx.Response(204, content=b""))
    limiter = FakeRateLimiter()
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, limiter, FrozenClock())
        await rest.cancel_order("o1", market_ticker="T")
        await rest.cancel_all()
    assert limiter.calls == [(2, "write"), (2, "write")]


# --- pagination ---------------------------------------------------------------------


async def test_markets_page_reports_next_cursor() -> None:
    router = Router()
    router.add(
        "GET",
        "/markets",
        httpx.Response(200, json={"markets": [_market_json(ticker="A")], "cursor": "page2"}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        page = await rest.markets()
    assert [m.ticker for m in page.items] == ["A"]
    assert page.cursor == "page2"


async def test_iter_markets_follows_cursor_across_pages_and_stops_on_empty_cursor() -> None:
    router = Router()
    router.add(
        "GET",
        "/markets",
        httpx.Response(200, json={"markets": [_market_json(ticker="A")], "cursor": "page2"}),
    )
    router.add(
        "GET",
        "/markets",
        httpx.Response(200, json={"markets": [_market_json(ticker="B")], "cursor": ""}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        tickers = [m.ticker async for m in rest.iter_markets()]
    assert tickers == ["A", "B"]
    assert len(router.requests) == 2


async def test_iter_markets_stops_silently_at_the_max_pages_cap() -> None:
    router = Router()
    router.add(
        "GET",
        "/markets",
        httpx.Response(200, json={"markets": [_market_json(ticker="A")], "cursor": "page2"}),
    )
    router.add(
        "GET",
        "/markets",
        httpx.Response(200, json={"markets": [_market_json(ticker="B")], "cursor": "page3"}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        tickers = [m.ticker async for m in rest.iter_markets(max_pages=1)]
    assert tickers == ["A"]
    assert len(router.requests) == 1


async def test_iter_markets_rejects_non_positive_max_pages() -> None:
    router = Router()
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(ValueError, match="max_pages"):
            async for _ in rest.iter_markets(max_pages=0):
                pass
    assert router.requests == []


async def test_iter_trades_follows_cursor_across_pages() -> None:
    router = Router()
    trade_a = {
        "trade_id": "t1",
        "ticker": "T",
        "count_fp": "1.00",
        "yes_price_dollars": "0.50",
        "no_price_dollars": "0.50",
        "taker_outcome_side": "yes",
        "taker_book_side": "bid",
        "created_time": "2024-01-01T00:00:00Z",
        "is_block_trade": False,
    }
    trade_b = {**trade_a, "trade_id": "t2"}
    router.add(
        "GET", "/markets/trades", httpx.Response(200, json={"trades": [trade_a], "cursor": "p2"})
    )
    router.add(
        "GET", "/markets/trades", httpx.Response(200, json={"trades": [trade_b], "cursor": ""})
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        ids = [t.trade_id async for t in rest.iter_trades(ticker="T")]
    assert ids == ["t1", "t2"]


async def test_iter_events_and_iter_fills_and_iter_settlements_paginate() -> None:
    router = Router()
    router.add(
        "GET",
        "/events",
        httpx.Response(
            200,
            json={
                "events": [
                    {
                        "event_ticker": "E1",
                        "series_ticker": "S",
                        "sub_title": "s",
                        "title": "t",
                        "collateral_return_type": "binary",
                        "mutually_exclusive": True,
                        "settlement_sources": None,
                    }
                ],
                "cursor": "",
            },
        ),
    )
    router.add(
        "GET", "/portfolio/fills", httpx.Response(200, json={"fills": [_fill_json()], "cursor": ""})
    )
    router.add(
        "GET",
        "/portfolio/settlements",
        httpx.Response(200, json={"settlements": [_settlement_json()], "cursor": ""}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        events = [e async for e in rest.iter_events()]
        fills = [f async for f in rest.iter_fills()]
        settlements = [s async for s in rest.iter_settlements()]
    assert [e.event_ticker for e in events] == ["E1"]
    assert [f.fill_id for f in fills] == ["f1"]
    assert [s.ticker for s in settlements] == ["T"]


async def test_settlements_cursor_defaults_to_none_when_absent_from_body() -> None:
    router = Router()
    router.add(
        "GET",
        "/portfolio/settlements",
        httpx.Response(200, json={"settlements": [_settlement_json()]}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        page = await rest.settlements()
    assert page.cursor is None


# --- batch limits ---------------------------------------------------------------------


async def test_orderbooks_rejects_more_than_100_tickers_without_truncating() -> None:
    router = Router()
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(ValueError, match="100"):
            await rest.orderbooks([f"T{i}" for i in range(101)])
    assert router.requests == []


async def test_orderbooks_rejects_empty_batch() -> None:
    router = Router()
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(ValueError, match="at least one"):
            await rest.orderbooks([])


async def test_candlesticks_rejects_more_than_100_tickers_without_truncating() -> None:
    router = Router()
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(ValueError, match="100"):
            await rest.candlesticks(
                [f"T{i}" for i in range(101)], start_ts=0, end_ts=1, period_min=1
            )
    assert router.requests == []


async def test_orderbooks_sends_one_repeated_query_param_per_ticker() -> None:
    router = Router()
    router.add(
        "GET",
        "/markets/orderbooks",
        httpx.Response(
            200,
            json={
                "orderbooks": [
                    {"ticker": "A", "orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
                    {"ticker": "B", "orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
                ]
            },
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        books = await rest.orderbooks(["A", "B"])
    assert [b.ticker for b in books] == ["A", "B"]
    assert router.requests[0].url.params.get_list("tickers") == ["A", "B"]


async def test_candlesticks_sends_comma_joined_tickers() -> None:
    router = Router()
    router.add(
        "GET",
        "/markets/candlesticks",
        httpx.Response(
            200, json={"markets": [{"market_ticker": "A", "candlesticks": [_candlestick_json()]}]}
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        result = await rest.candlesticks(["A", "B"], start_ts=1, end_ts=2, period_min=60)
    assert result[0].market_ticker == "A"
    assert router.requests[0].url.params.get("market_tickers") == "A,B"
    assert router.requests[0].url.params.get("period_interval") == "60"


# --- error mapping --------------------------------------------------------------------


async def test_400_with_body_carries_code_message_and_details() -> None:
    router = Router()
    router.add(
        "GET",
        "/exchange/status",
        httpx.Response(400, json={"code": "bad_request", "message": "nope", "details": "x"}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(KalshiHttpError) as exc_info:
            await rest.exchange_status()
    err = exc_info.value
    assert err.status == 400
    assert err.code == "bad_request"
    assert err.message == "nope"
    assert err.details == "x"


async def test_400_without_a_body_has_no_code_or_message() -> None:
    router = Router()
    router.add("GET", "/exchange/status", httpx.Response(400, content=b""))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(KalshiHttpError) as exc_info:
            await rest.exchange_status()
    err = exc_info.value
    assert err.status == 400
    assert err.code is None
    assert err.message is None
    assert err.details is None


async def test_401_maps_to_kalshi_http_error() -> None:
    router = Router()
    router.add("GET", "/account/limits", httpx.Response(401, content=b""))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(KalshiHttpError) as exc_info:
            await rest.account_limits()
    assert exc_info.value.status == 401


async def test_429_raises_rate_limited_error_which_is_also_an_http_error() -> None:
    router = Router()
    router.add("GET", "/exchange/status", httpx.Response(429, content=b""))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(RateLimitedError) as exc_info:
            await rest.exchange_status()
    assert isinstance(exc_info.value, KalshiHttpError)
    assert exc_info.value.status == 429


async def test_500_with_body_maps_to_kalshi_http_error() -> None:
    router = Router()
    router.add("GET", "/exchange/status", httpx.Response(500, json={"message": "internal error"}))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(KalshiHttpError) as exc_info:
            await rest.exchange_status()
    assert exc_info.value.status == 500
    assert exc_info.value.message == "internal error"


async def test_timeout_exception_maps_to_kalshi_transport_error() -> None:
    router = Router()
    router.add("GET", "/exchange/status", _raise(httpx.ConnectTimeout("timed out")))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(KalshiTransportError):
            await rest.exchange_status()


async def test_connect_error_maps_to_kalshi_transport_error() -> None:
    router = Router()
    router.add("GET", "/exchange/status", _raise(httpx.ConnectError("refused")))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(KalshiTransportError):
            await rest.exchange_status()


async def test_malformed_success_body_raises_wire_error() -> None:
    router = Router()
    # Missing the required "trading_active" field.
    router.add("GET", "/exchange/status", httpx.Response(200, json={"exchange_active": True}))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        with pytest.raises(WireError):
            await rest.exchange_status()


# --- decoding, one realistic payload per endpoint family -----------------------------


async def test_market_decodes() -> None:
    router = Router()
    router.add("GET", "/markets/T-1", httpx.Response(200, json={"market": _market_json()}))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        market = await rest.market("T-1")
    assert market.ticker == "T-1"
    assert market.price_ranges[0].start == "0.01"


async def test_orderbook_decodes() -> None:
    router = Router()
    router.add(
        "GET",
        "/markets/T-1/orderbook",
        httpx.Response(
            200,
            json={"orderbook_fp": {"yes_dollars": [["0.15", "100.00"]], "no_dollars": []}},
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        book = await rest.orderbook("T-1", depth=10)
    assert book.yes_dollars == [("0.15", "100.00")]
    assert router.requests[0].url.params.get("depth") == "10"


async def test_series_and_fee_changes_decode() -> None:
    router = Router()
    router.add(
        "GET",
        "/series",
        httpx.Response(
            200,
            json={
                "series": [
                    {
                        "ticker": "S",
                        "frequency": "daily",
                        "title": "T",
                        "category": "c",
                        "tags": None,
                        "settlement_sources": None,
                        "contract_url": "u",
                        "contract_terms_url": "u2",
                        "fee_type": "flat",
                        "fee_multiplier": 1.0,
                        "additional_prohibitions": None,
                    }
                ]
            },
        ),
    )
    router.add(
        "GET",
        "/series/fee_changes",
        httpx.Response(
            200,
            json={
                "series_fee_change_arr": [
                    {
                        "id": "1",
                        "series_ticker": "S",
                        "fee_type": "flat",
                        "fee_multiplier": 0.5,
                        "scheduled_ts": "2024-01-01T00:00:00Z",
                    }
                ]
            },
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        series = await rest.series(min_updated_ts=1)
        fee_changes = await rest.fee_changes(show_historical=True)
    assert series[0].ticker == "S"
    assert fee_changes[0].fee_multiplier == 0.5
    assert router.requests[1].url.params.get("show_historical") == "true"


async def test_events_decodes_with_nested_markets_flag() -> None:
    router = Router()
    router.add(
        "GET",
        "/events",
        httpx.Response(
            200,
            json={
                "events": [
                    {
                        "event_ticker": "E1",
                        "series_ticker": "S",
                        "sub_title": "s",
                        "title": "t",
                        "collateral_return_type": "binary",
                        "mutually_exclusive": False,
                        "settlement_sources": None,
                        "markets": [_market_json()],
                    }
                ],
                "cursor": "next",
            },
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        page = await rest.events(with_nested_markets=True)
    assert page.cursor == "next"
    assert page.items[0].markets is not None
    assert page.items[0].markets[0].ticker == "T-1"
    assert router.requests[0].url.params.get("with_nested_markets") == "true"


async def test_account_limits_and_api_keys_decode() -> None:
    router = Router()
    router.add(
        "GET",
        "/account/limits",
        httpx.Response(
            200,
            json={
                "usage_tier": "expert",
                "read": {"refill_rate": 200, "bucket_capacity": 400},
                "write": {"refill_rate": 100, "bucket_capacity": 200},
                "grants": [
                    {"exchange_instance": "event_contract", "level": "expert", "source": "volume"}
                ],
            },
        ),
    )
    router.add(
        "GET",
        "/api_keys",
        httpx.Response(
            200,
            json={
                "api_keys": [{"api_key_id": "k1", "name": "n", "scopes": ["read", "write::trade"]}],
                "api_key_region_expiration_ts": 123,
            },
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        limits = await rest.account_limits()
        keys = await rest.api_keys()
    assert limits.write.bucket_capacity == 200
    assert limits.grants[0].level == "expert"
    assert keys.api_keys[0].scopes == ["read", "write::trade"]


async def test_create_order_sends_json_body_matching_the_request_struct() -> None:
    router = Router()
    router.add(
        "POST",
        "/portfolio/events/orders",
        httpx.Response(
            201,
            json={
                "order_id": "o1",
                "fill_count": "0.00",
                "remaining_count": "10.00",
                "ts_ms": 42,
            },
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        req = CreateOrderV2Request(
            ticker="T",
            side="bid",
            count="10.00",
            price="0.5600",
            time_in_force="good_till_canceled",
            self_trade_prevention_type="maker",
        )
        resp = await rest.create_order(req)
    assert resp.order_id == "o1"
    assert resp.ts_ms == 42
    sent = json.loads(router.requests[0].content)
    assert sent["ticker"] == "T"
    assert sent["price"] == "0.5600"
    assert sent["self_trade_prevention_type"] == "maker"
    assert router.requests[0].headers["content-type"] == "application/json"


async def test_cancel_order_decodes() -> None:
    router = Router()
    router.add(
        "DELETE",
        "/portfolio/events/orders/o1",
        httpx.Response(200, json={"order_id": "o1", "reduced_by": "10.00", "ts_ms": 1}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        resp = await rest.cancel_order("o1", market_ticker="T")
    assert resp.reduced_by == "10.00"
    assert router.requests[0].url.params.get("market_ticker") == "T"


async def test_decrease_order_formats_count_and_decodes_response() -> None:
    router = Router()
    router.add(
        "POST",
        "/portfolio/events/orders/o1/decrease",
        httpx.Response(200, json={"order_id": "o1", "remaining_count": "8.00", "ts_ms": 1}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        resp = await rest.decrease_order("o1", reduce_by=CountE2(200), market_ticker="T")
    assert resp.remaining_count == "8.00"
    sent = json.loads(router.requests[0].content)
    assert sent["reduce_by"] == "2.00"
    assert sent["market_ticker"] == "T"


async def test_cancel_all_accepts_an_empty_204_body() -> None:
    router = Router()
    router.add("DELETE", "/portfolio/events/orders", httpx.Response(204, content=b""))
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        await rest.cancel_all()
    assert [(r.method, r.url.path) for r in router.requests] == [
        ("DELETE", "/trade-api/v2/portfolio/events/orders")
    ]


async def test_balance_decodes() -> None:
    router = Router()
    router.add(
        "GET",
        "/portfolio/balance",
        httpx.Response(
            200,
            json={
                "balance": 500,
                "balance_dollars": "5.00",
                "portfolio_value": 500,
                "updated_ts": 1,
            },
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        balance = await rest.balance(exchange_index=0)
    assert balance.balance == 500
    assert router.requests[0].url.params.get("exchange_index") == "0"


async def test_unknown_inbound_enum_values_still_decode() -> None:
    """Kalshi extends its own enums without notice; decoding must survive that.

    On 2026-09-10 the live fee-changes endpoint returned
    ``margin_market_maker_program_fees``, which the pinned OpenAPI ``FeeType`` enum does
    not contain. Recognizing fee types is the domain layer's job (ADR 0017); the wire
    layer's job is to not lose the response.
    """
    router = Router()
    router.add(
        "GET",
        "/series/fee_changes",
        httpx.Response(
            200,
            json={
                "series_fee_change_arr": [
                    {
                        "id": "1",
                        "series_ticker": "KXGOLDPERP",
                        "fee_type": "margin_market_maker_program_fees",
                        "fee_multiplier": 1.0,
                        "scheduled_ts": "2026-09-10T00:00:00Z",
                    }
                ]
            },
        ),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        changes = await rest.fee_changes(show_historical=True)
    assert changes[0].fee_type == "margin_market_maker_program_fees"


async def test_unknown_market_status_still_decodes() -> None:
    """A status Kalshi adds must not cost us the rest of the market metadata."""
    router = Router()
    router.add(
        "GET",
        "/markets/KX-1",
        httpx.Response(200, json={"market": {**_market_json(), "status": "some_new_status"}}),
    )
    async with make_client(router) as client:
        rest = KalshiRest(BASE_URL, client, FakeRateLimiter(), FrozenClock())
        market = await rest.market("KX-1")
    assert market.status == "some_new_status"
    assert market.ticker == _market_json()["ticker"]
