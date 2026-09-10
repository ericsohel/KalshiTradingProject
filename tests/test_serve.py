"""``tape serve`` end to end, and its wiring: a real ZeroMQ bus over ipc, a real HTTP server on
localhost, and a WebSocket client that receives a published book as a snapshot and deltas."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Final

import httpx
import msgspec
import pytest
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.typing import Origin

from tape.api import ServeConfig
from tape.api.contract import (
    DeltaMessage,
    HelloMessage,
    MarketDetail,
    ServerMessage,
    ServiceStatus,
    SnapshotMessage,
    SubscribedMessage,
)
from tape.bus import SequencedPublisher, ZmqPublisher
from tape.cli import build_api, listen_socket, serve_api, serve_config
from tape.config import Settings, load_settings
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
)
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import FrozenClock, Ms, Ns
from tests.fakes import FakeSubscriber

ORIGIN: Final = "http://localhost:5173"
EPOCH: Final = 1_789_000_000_000_000_000
TS: Final = Ms(1_789_000_000_000)
RECEIPT: Final = Receipt(conn_id=2, recv_mono_ns=Ns(1), recv_wall_ns=Ns(EPOCH))
CATALOG: Final = MarketCatalog(
    markets=(
        CatalogEntry(
            ticker="KXA-1",
            series_ticker="KXA",
            event_ticker="KXA",
            volume_24h=CountE2(100),
            close_ts=None,
            showcase=True,
        ),
    )
)

_messages: Final = msgspec.json.Decoder(ServerMessage)


def write_settings(tmp_path: Path, endpoint: str) -> Settings:
    # tape serve holds no credentials (ADR 0023), so its configuration names no key at all.
    config = tmp_path / "tape.toml"
    config.write_text(
        '[kalshi]\nenv = "demo"\n\n'
        f'[recorder]\ndata_dir = "data"\nbus_endpoint = "{endpoint}"\nbus_refresh_s = 5\n\n'
        f'[serve]\nallowed_origins = ["{ORIGIN}"]\nmax_tickers_per_client = 3\n'
    )
    return load_settings(config, environ={})


def book_delta(price: int, change: int) -> BookDelta:
    return BookDelta(
        ticker="KXA-1",
        ts_ms=Ms(TS + 1),
        receipt=RECEIPT,
        sid=1,
        seq=None,
        side=Side.BID,
        price=PriceE4(price),
        delta=change,
    )


async def keep_publishing[T](
    bus: SequencedPublisher,
    event: BusEvent,
    read: Callable[[], Awaitable[T]],
    done: Callable[[T], bool],
) -> T:
    """Publish an event until what the API reports satisfies a condition.

    The subscriber connects asynchronously and PUB drops what it sends before then, so the first
    copies may be lost; once one arrives, later messages follow in order.
    """
    async with asyncio.timeout(10):
        while True:
            bus.publish(event)
            try:
                value = await read()
            except httpx.TransportError:
                await asyncio.sleep(0.02)
                continue
            if done(value):
                return value
            await asyncio.sleep(0.02)


async def receive(socket: ClientConnection) -> ServerMessage:
    async with asyncio.timeout(5):
        message: ServerMessage = _messages.decode(await socket.recv())
    return message


def test_serve_config_and_build_api_follow_the_settings(tmp_path: Path, ipc_dir: Path) -> None:
    settings = write_settings(tmp_path, f"ipc://{ipc_dir / 'bus.sock'}")

    config = serve_config(settings)
    assert config == ServeConfig(
        allowed_origins=frozenset({ORIGIN}),
        max_clients=200,
        max_tickers=3,
        client_queue_max=5000,
        bus_refresh_s=5,
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(404)))
    api = build_api(settings, http=http, clock=FrozenClock(), subscriber=FakeSubscriber())
    assert api.hub.config == config
    assert [getattr(route, "path", None) for route in api.app.routes] == [
        "/api/v1/markets",
        "/api/v1/markets/{ticker}",
        "/api/v1/status",
        "/api/v1/live",
    ]


def test_listen_socket_refuses_a_port_that_is_taken() -> None:
    with listen_socket("127.0.0.1", 0) as taken:
        taken.listen()
        with pytest.raises(OSError, match="in use"):
            listen_socket("127.0.0.1", taken.getsockname()[1])


async def test_a_published_book_reaches_a_websocket_client_through_tape_serve(
    tmp_path: Path, ipc_dir: Path
) -> None:
    endpoint = f"ipc://{ipc_dir / 'bus.sock'}"
    settings = write_settings(tmp_path, endpoint)
    bus = SequencedPublisher(ZmqPublisher(endpoint, send_hwm=1000), epoch=EPOCH)
    public_requests: list[str] = []

    def kalshi(request: httpx.Request) -> httpx.Response:
        public_requests.append(request.url.path)
        return httpx.Response(404)

    stop = asyncio.Event()
    rest_url = settings.kalshi.endpoints.rest_url
    with listen_socket("127.0.0.1", 0) as sock:
        port = sock.getsockname()[1]
        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(kalshi), base_url=rest_url) as http,
            httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as api,
        ):
            serving = asyncio.create_task(
                serve_api(settings, http=http, clock=FrozenClock(mono_ns=1), stop=stop, sock=sock)
            )
            try:

                async def status() -> ServiceStatus:
                    response = await api.get("/api/v1/status")
                    return msgspec.json.decode(response.content, type=ServiceStatus)

                async def detail() -> MarketDetail:
                    response = await api.get("/api/v1/markets/KXA-1")
                    return msgspec.json.decode(response.content, type=MarketDetail)

                await keep_publishing(bus, CATALOG, status, lambda s: s.bus.messages > 0)
                report = StatusReport(
                    interval_s=60, universe_size=1, subscribed_markets=1, connections=()
                )
                recording = await keep_publishing(bus, report, status, lambda s: s.recording)
                assert recording.recorder is not None
                refresh = BookRefresh(
                    ticker="KXA-1",
                    ts_ms=TS,
                    receipt=RECEIPT,
                    stale=False,
                    bids=(Level(PriceE4(4000), CountE2(100)),),
                    asks=(Level(PriceE4(4200), CountE2(50)),),
                )
                await keep_publishing(bus, refresh, detail, lambda d: d.book == "fresh")

                url = f"ws://127.0.0.1:{port}/api/v1/live"
                for origin in (None, Origin("https://elsewhere.example")):
                    with pytest.raises(InvalidStatus) as refused:
                        await connect(url, origin=origin)
                    assert refused.value.response.status_code == 403

                async with connect(
                    f"ws://127.0.0.1:{port}/api/v1/live", origin=Origin(ORIGIN)
                ) as live:
                    assert await receive(live) == HelloMessage(
                        protocol=1, max_tickers=3, bus_refresh_s=5
                    )
                    await live.send(json.dumps({"op": "subscribe", "tickers": ["KXA-1"]}))
                    assert await receive(live) == SubscribedMessage(tickers=("KXA-1",), rejected=())
                    assert await receive(live) == SnapshotMessage(
                        ticker="KXA-1",
                        book="fresh",
                        ts_ms=TS,
                        bids=((PriceE4(4000), CountE2(100)),),
                        asks=((PriceE4(4200), CountE2(50)),),
                    )
                    bus.publish(book_delta(4100, 300))
                    bus.publish(book_delta(4000, -100))
                    assert [await receive(live), await receive(live)] == [
                        DeltaMessage(
                            ticker="KXA-1",
                            ts_ms=TS + 1,
                            side="bid",
                            price_e4=PriceE4(4100),
                            delta_e2=300,
                        ),
                        DeltaMessage(
                            ticker="KXA-1",
                            ts_ms=TS + 1,
                            side="bid",
                            price_e4=PriceE4(4000),
                            delta_e2=-100,
                        ),
                    ]

                    stop.set()
                    with pytest.raises(ConnectionClosed) as closed:
                        await receive(live)
                assert closed.value.rcvd is not None
                assert closed.value.rcvd.code == 1001
                await asyncio.wait_for(serving, timeout=30)
            finally:
                stop.set()
                await asyncio.wait({serving}, timeout=30)
                bus.close()

    # Metadata is fetched from the public endpoints only, and its failures never reach the feed.
    assert set(public_requests) <= {"/trade-api/v2/events/KXA", "/trade-api/v2/series/KXA"}
