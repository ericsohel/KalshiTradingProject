"""The live API through Starlette's test client: routes, error bodies, headers, CORS, and the
WebSocket handshake's origin and capacity checks."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from typing import Final

import httpx
import msgspec
import pytest
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from tape.api import (
    LiveHub,
    MarketDirectory,
    MarketMetadata,
    MetadataResolver,
    ServeConfig,
    create_app,
)
from tape.api.contract import (
    BusHealth,
    MarketDetail,
    MarketRow,
    MarketsResponse,
    ServiceStatus,
)
from tape.bus import BusEnvelope, encode_bus_envelope
from tape.client.ratelimit import NullRateLimiter
from tape.client.rest import KalshiRest
from tape.events import (
    BookDelta,
    BookRefresh,
    BusEvent,
    CatalogEntry,
    Level,
    MarketCatalog,
    Receipt,
    Side,
    StatusReport,
    Ticker,
)
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import NS_PER_MS, FrozenClock, Ms, Ns
from tests.fakes import FakeSubscriber

BASE_URL: Final = "https://kalshi.test/trade-api/v2"
ORIGIN: Final = "http://localhost:5173"
EPOCH: Final = 1_789_000_000_000_000_000
TS: Final = Ms(1_789_000_000_000)
RECEIPT: Final = Receipt(conn_id=2, recv_mono_ns=Ns(1), recv_wall_ns=Ns(EPOCH))
LIVE: Final = "/api/v1/live"


class Api:
    """The live API over a hand-fed bus; its resolver is never run, so metadata stays null."""

    def __init__(
        self, *, max_clients: int = 2, resolver_type: type[MetadataResolver] = MetadataResolver
    ) -> None:
        self.clock = FrozenClock(mono_ns=1)
        http = httpx.AsyncClient(transport=httpx.MockTransport(_no_network), base_url=BASE_URL)
        rest = KalshiRest(BASE_URL, http, NullRateLimiter(), self.clock)
        self.resolver = resolver_type(rest, clock=self.clock, ttl_s=60)
        self.hub = LiveHub(
            FakeSubscriber(),
            directory=MarketDirectory(),
            config=ServeConfig(
                allowed_origins=frozenset({ORIGIN}),
                max_clients=max_clients,
                max_tickers=10,
                client_queue_max=100,
                bus_refresh_s=10,
            ),
            clock=self.clock,
            request_metadata=self.resolver.request,
        )
        self.app = create_app(hub=self.hub, resolver=self.resolver, clock=self.clock)
        self.seq = 0

    def envelope(self, event: BusEvent) -> bytes:
        self.seq += 1
        return encode_bus_envelope(BusEnvelope(bus_epoch=EPOCH, bus_seq=self.seq, event=event))

    def publish(self, *events: BusEvent) -> None:
        """Feed the hub directly; only safe while no live session runs on the client's loop."""
        for event in events:
            self.hub.receive(self.envelope(event))


def _no_network(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"unexpected request to {request.url}")


def catalog(*markets: tuple[str, int]) -> MarketCatalog:
    return MarketCatalog(
        markets=tuple(
            CatalogEntry(
                ticker=ticker,
                series_ticker="KX",
                event_ticker=ticker.rsplit("-", 1)[0],
                volume_24h=CountE2(volume),
                close_ts=1_800_000_000,
                showcase=False,
            )
            for ticker, volume in markets
        )
    )


def refresh(ticker: str) -> BookRefresh:
    return BookRefresh(
        ticker=ticker,
        ts_ms=TS,
        receipt=RECEIPT,
        stale=False,
        bids=(Level(PriceE4(4000), CountE2(100)),),
        asks=(),
    )


def delta(ticker: str) -> BookDelta:
    return BookDelta(
        ticker=ticker,
        ts_ms=Ms(TS + 1),
        receipt=RECEIPT,
        sid=1,
        seq=None,
        side=Side.BID,
        price=PriceE4(4100),
        delta=25,
    )


def ticker_update(ticker: str) -> Ticker:
    return Ticker(
        ticker=ticker,
        ts_ms=TS,
        receipt=RECEIPT,
        sid=1,
        last=PriceE4(4000),
        bid=PriceE4(3900),
        ask=PriceE4(4100),
        bid_size=None,
        ask_size=None,
        volume=CountE2(0),
        open_interest=CountE2(0),
    )


def handshake(client: TestClient, headers: dict[str, str]) -> None:
    with client.websocket_connect(LIVE, headers=headers) as live:
        live.receive_json()


def fields(struct: type[msgspec.Struct]) -> Sequence[str]:
    return struct.__struct_fields__


def test_markets_rank_by_volume_carry_nulls_until_known_and_are_never_cached() -> None:
    api = Api()
    api.publish(catalog(("KXB-1", 500), ("KXA-1", 900), ("KXC-1", 100)), ticker_update("KXA-1"))
    with TestClient(api.app) as client:
        listed = client.get("/api/v1/markets")
        limited = client.get("/api/v1/markets", params={"limit": "2"})

    assert listed.status_code == 200
    assert listed.headers["cache-control"] == "no-store"
    assert listed.headers["content-type"] == "application/json"
    rows = msgspec.json.decode(listed.content, type=MarketsResponse).markets
    assert [row.ticker for row in rows] == ["KXA-1", "KXB-1", "KXC-1"]
    assert (rows[0].title, rows[0].subtitle, rows[0].category) == (None, None, None)
    assert (rows[0].bid_e4, rows[0].ask_e4, rows[0].last_e4, rows[0].book) == (
        3900,
        4100,
        4000,
        "unknown",
    )
    assert rows[1].bid_e4 is None
    assert list(json.loads(listed.content)["markets"][0]) == list(fields(MarketRow))
    limited_rows = msgspec.json.decode(limited.content, type=MarketsResponse).markets
    assert [row.ticker for row in limited_rows] == ["KXA-1", "KXB-1"]
    # Every listed market asked for its metadata once: three events and their shared series.
    assert api.resolver.stats.pending == 4


@pytest.mark.parametrize("limit", ["0", "201", "abc", "-1", "1.5", ""])
def test_a_limit_outside_1_to_200_is_a_bad_request(limit: str) -> None:
    api = Api()
    with TestClient(api.app) as client:
        response = client.get("/api/v1/markets", params={"limit": limit})
        widest = client.get("/api/v1/markets", params={"limit": "200"})

    assert response.status_code == 400
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "error": {
            "code": "bad_request",
            "message": f"limit must be an integer from 1 to 200, got {limit!r}",
        }
    }
    assert widest.status_code == 200


def test_a_market_detail_has_depth_once_its_book_is_known_and_404_when_not_recorded() -> None:
    api = Api()
    api.publish(catalog(("KXA-1", 900)))
    with TestClient(api.app) as client:
        before = msgspec.json.decode(client.get("/api/v1/markets/KXA-1").content, type=MarketDetail)
        api.publish(refresh("KXA-1"))
        response = client.get("/api/v1/markets/KXA-1")
        missing = client.get("/api/v1/markets/KXNOPE-1")

    assert (before.book, before.depth, before.price_ranges) == ("unknown", None, None)
    after = msgspec.json.decode(response.content, type=MarketDetail)
    assert after.book == "fresh"
    assert after.depth is not None
    assert (after.depth.ts_ms, after.depth.bids, after.depth.asks) == (TS, ((4000, 100),), ())
    assert list(json.loads(response.content)) == list(fields(MarketDetail))
    assert response.headers["cache-control"] == "no-store"
    assert missing.status_code == 404
    assert missing.headers["cache-control"] == "no-store"
    assert missing.json() == {
        "error": {"code": "unknown_ticker", "message": "market 'KXNOPE-1' is not recorded"}
    }


def test_status_shows_the_recorder_the_bus_and_the_open_live_connections() -> None:
    api = Api()
    with TestClient(api.app) as client:
        empty = msgspec.json.decode(client.get("/api/v1/status").content, type=ServiceStatus)
        api.publish(
            StatusReport(interval_s=60, universe_size=4, subscribed_markets=3, connections=())
        )
        api.clock.advance(1_500 * NS_PER_MS)
        with client.websocket_connect(LIVE, headers={"origin": ORIGIN}) as live:
            live.receive_json()
            response = client.get("/api/v1/status")

    assert empty == ServiceStatus(
        recording=False,
        recorder_status_age_ms=None,
        recorder=None,
        bus=BusHealth(epoch=None, last_seq=None, messages=0, resets=0, missed=0, books_known=0),
        clients=0,
    )
    status = msgspec.json.decode(response.content, type=ServiceStatus)
    assert (status.recording, status.recorder_status_age_ms, status.clients) == (True, 1500, 1)
    assert status.recorder is not None
    assert (status.recorder.universe_size, status.recorder.subscribed_markets) == (4, 3)
    assert (status.bus.epoch, status.bus.last_seq, status.bus.messages) == (EPOCH, 1, 1)
    assert response.headers["cache-control"] == "no-store"


def test_unknown_paths_and_methods_are_answered_with_error_bodies() -> None:
    api = Api()
    with TestClient(api.app) as client:
        missing = client.get("/api/v1/nope")
        wrong = client.post("/api/v1/markets")

    assert (missing.status_code, missing.json()["error"]["code"]) == (404, "not_found")
    assert (wrong.status_code, wrong.json()["error"]["code"]) == (405, "method_not_allowed")
    assert "GET" in wrong.headers["allow"]
    assert missing.headers["cache-control"] == wrong.headers["cache-control"] == "no-store"


def test_an_unexpected_failure_is_an_internal_error_body() -> None:
    class BrokenResolver(MetadataResolver):
        def lookup(self, entry: CatalogEntry) -> MarketMetadata:
            raise RuntimeError(f"no metadata for {entry.ticker}")

    api = Api(resolver_type=BrokenResolver)
    api.publish(catalog(("KXA-1", 900)))
    with TestClient(api.app, raise_server_exceptions=False) as client:
        response = client.get("/api/v1/markets")

    assert response.status_code == 500
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"error": {"code": "internal_error", "message": "internal error"}}


def test_cors_answers_only_the_allowed_origins() -> None:
    api = Api()
    foreign = "https://elsewhere.example"
    preflight = {"access-control-request-method": "GET"}
    with TestClient(api.app) as client:
        allowed = client.get("/api/v1/status", headers={"origin": ORIGIN})
        refused = client.get("/api/v1/status", headers={"origin": foreign})
        allowed_preflight = client.options(
            "/api/v1/markets", headers={"origin": ORIGIN, **preflight}
        )
        refused_preflight = client.options(
            "/api/v1/markets", headers={"origin": foreign, **preflight}
        )

    assert allowed.headers["access-control-allow-origin"] == ORIGIN
    assert "access-control-allow-origin" not in refused.headers
    assert allowed_preflight.status_code == 200
    assert allowed_preflight.headers["access-control-allow-origin"] == ORIGIN
    assert refused_preflight.status_code == 400


@pytest.mark.parametrize("headers", [{}, {"origin": "https://elsewhere.example"}])
def test_the_live_feed_closes_the_handshake_of_any_other_origin_unaccepted(
    headers: dict[str, str],
) -> None:
    # A real server answers this with 403; tests/test_serve.py checks the status on the wire.
    api = Api()
    with TestClient(api.app) as client, pytest.raises(WebSocketDisconnect):
        handshake(client, headers)

    assert api.hub.clients == 0


def test_the_live_feed_closes_with_1013_when_max_clients_are_open() -> None:
    api = Api(max_clients=1)
    with (
        TestClient(api.app) as client,
        client.websocket_connect(LIVE, headers={"origin": ORIGIN}) as first,
    ):
        first.receive_json()
        with pytest.raises(WebSocketDisconnect) as refused:
            handshake(client, {"origin": ORIGIN})
        assert refused.value.code == 1013
        assert api.hub.clients == 1


def test_the_live_feed_serves_a_subscription_through_the_app() -> None:
    api = Api()
    api.publish(catalog(("KXA-1", 900)), refresh("KXA-1"))
    with TestClient(api.app) as client:
        portal = client.portal
        assert portal is not None
        with client.websocket_connect(LIVE, headers={"origin": ORIGIN}) as live:
            assert live.receive_json() == {
                "t": "hello",
                "protocol": 1,
                "max_tickers": 10,
                "bus_refresh_s": 10,
            }
            live.send_json({"op": "subscribe", "tickers": ["KXA-1", "KXB-1"]})
            assert live.receive_json() == {
                "t": "subscribed",
                "tickers": ["KXA-1"],
                "rejected": [{"ticker": "KXB-1", "code": "unknown_ticker"}],
            }
            assert live.receive_json() == {
                "t": "snapshot",
                "ticker": "KXA-1",
                "book": "fresh",
                "ts_ms": TS,
                "bids": [[4000, 100]],
                "asks": [],
            }
            # The session runs on the client's event loop, so the bus is fed on it too.
            portal.call(api.hub.receive, api.envelope(delta("KXA-1")))
            assert live.receive_json() == {
                "t": "delta",
                "ticker": "KXA-1",
                "ts_ms": TS + 1,
                "side": "bid",
                "price_e4": 4100,
                "delta_e2": 25,
            }
            live.send_text("{")
            assert live.receive_json()["code"] == "malformed_json"
        for _ in range(500):
            if api.hub.clients == 0:
                break
            portal.call(asyncio.sleep, 0.01)
        assert api.hub.clients == 0
