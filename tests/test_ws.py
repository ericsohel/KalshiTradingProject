"""WebSocket session: handshake signing, command encoding, frame delivery, and death."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import AsyncGenerator, Callable
from contextlib import aclosing
from pathlib import Path
from typing import Any, cast

import msgspec
import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from tape.client.auth import HEADER_KEY, HEADER_SIGNATURE, HEADER_TIMESTAMP, RsaPssSigner, Signer
from tape.client.ws import (
    WS_SIGN_PATH,
    ListSubscriptionsCommand,
    RawFrame,
    SubscribeCommand,
    UnsubscribeCommand,
    UpdateSubscriptionCommand,
    WsSession,
    encode_command,
)
from tape.errors import KalshiTransportError, WsClosedError
from tape.timeutil import NS_PER_MS, Clock, FrozenClock, SystemClock
from tape.wire import decode_envelope
from tests.fakes import FakeKalshiWs

KEY_ID = "9d2f8c6a-test"
WALL_NS = 1_757_000_000_123_456_789
SIGNED_TIMESTAMP = str(WALL_NS // NS_PER_MS)


@pytest.fixture(scope="session")
def private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def signer(private_key: rsa.RSAPrivateKey, tmp_path_factory: pytest.TempPathFactory) -> Signer:
    path: Path = tmp_path_factory.mktemp("keys") / "read.pem"
    path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return RsaPssSigner(KEY_ID, path)


@pytest.fixture
def clock() -> FrozenClock:
    """A clock that moves only when a test moves it, so no timing decision is flaky."""
    return FrozenClock(mono_ns=1_000, wall_ns=WALL_NS)


def session_for(fake: FakeKalshiWs, signer: Signer, clock: Clock, **kwargs: Any) -> WsSession:
    return WsSession(fake.url, signer, clock, conn_id=7, **kwargs)


async def take(session: WsSession, count: int, *, timeout_s: float = 5.0) -> list[RawFrame]:
    """Collect ``count`` frames, or fewer if the stream ends first. Always bounded."""
    frames: list[RawFrame] = []
    # frames() is an async generator; the cast only tells mypy what aclosing needs.
    stream = cast("AsyncGenerator[RawFrame, None]", session.frames())
    async with aclosing(stream), asyncio.timeout(timeout_s):
        async for frame in stream:
            frames.append(frame)
            if len(frames) == count:
                break
    return frames


async def until(predicate: Callable[[], bool], *, timeout_s: float = 5.0) -> None:
    """Wait for a condition the double cannot signal directly, without a fixed sleep."""
    async with asyncio.timeout(timeout_s):
        while not predicate():
            await asyncio.sleep(0.001)


def payload_of(frame: RawFrame) -> dict[str, Any]:
    decoded: dict[str, Any] = msgspec.json.decode(frame.payload, type=dict[str, Any])
    return decoded


def verify(public_key: rsa.RSAPublicKey, signature: str, message: str) -> None:
    public_key.verify(
        base64.b64decode(signature),
        message.encode(),
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=hashes.SHA256.digest_size),
        hashes.SHA256(),
    )


async def test_handshake_carries_the_three_signed_headers(
    signer: Signer, clock: FrozenClock, private_key: rsa.RSAPrivateKey
) -> None:
    async with FakeKalshiWs() as fake, session_for(fake, signer, clock):
        connection = await fake.wait_for_connection()

    headers = {name.lower(): value for name, value in connection.handshake_headers.items()}
    assert headers[HEADER_KEY.lower()] == KEY_ID
    assert headers[HEADER_TIMESTAMP.lower()] == SIGNED_TIMESTAMP
    assert connection.path == WS_SIGN_PATH
    signature = headers[HEADER_SIGNATURE.lower()]
    verify(private_key.public_key(), signature, f"{SIGNED_TIMESTAMP}GET{WS_SIGN_PATH}")
    with pytest.raises(InvalidSignature):
        verify(private_key.public_key(), signature, f"{SIGNED_TIMESTAMP}GET/trade-api/v2")


def test_commands_serialize_exactly_as_the_spec_shows() -> None:
    assert encode_command(
        SubscribeCommand(("orderbook_delta", "trade"), ("KXA-1", "KXA-2"), True), 1
    ) == (
        b'{"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta","trade"],'
        b'"market_tickers":["KXA-1","KXA-2"],"use_yes_price":true}}'
    )
    assert encode_command(UpdateSubscriptionCommand(7, "add_markets", ("KXA-3",)), 2) == (
        b'{"id":2,"cmd":"update_subscription",'
        b'"params":{"sid":7,"action":"add_markets","market_tickers":["KXA-3"]}}'
    )
    assert encode_command(UnsubscribeCommand((7,)), 3) == (
        b'{"id":3,"cmd":"unsubscribe","params":{"sids":[7]}}'
    )
    assert encode_command(ListSubscriptionsCommand(), 4) == b'{"id":4,"cmd":"list_subscriptions"}'
    assert encode_command(UpdateSubscriptionCommand(7, "get_snapshot", ("KXA-1",)), 5) == (
        b'{"id":5,"cmd":"update_subscription",'
        b'"params":{"sid":7,"action":"get_snapshot","market_tickers":["KXA-1"]}}'
    )


def test_commands_reject_arguments_the_server_would_refuse() -> None:
    with pytest.raises(ValueError, match="channel"):
        SubscribeCommand(())
    with pytest.raises(ValueError, match="market_tickers"):
        SubscribeCommand(("trade",), ())
    # The spec documents get_snapshot only with market_tickers (asyncapi.yaml,
    # updateSubscriptionGetSnapshotCommand); the bare form is refused like the others.
    with pytest.raises(ValueError, match="get_snapshot needs at least one market ticker"):
        UpdateSubscriptionCommand(1, "get_snapshot")
    with pytest.raises(ValueError, match="get_snapshot needs at least one market ticker"):
        UpdateSubscriptionCommand(1, "get_snapshot", ())
    with pytest.raises(ValueError, match="market ticker"):
        UpdateSubscriptionCommand(1, "add_markets")
    with pytest.raises(ValueError, match="sid"):
        UnsubscribeCommand(())
    with pytest.raises(ValueError, match="positive"):
        encode_command(ListSubscriptionsCommand(), 0)


async def test_command_ids_increment_per_connection(signer: Signer, clock: FrozenClock) -> None:
    async with FakeKalshiWs() as fake, session_for(fake, signer, clock) as session:
        connection = await fake.wait_for_connection()
        assert await session.send(SubscribeCommand(("orderbook_delta",), ("KXA-1",), True)) == 1
        assert await session.send(UpdateSubscriptionCommand(1, "add_markets", ("KXA-2",))) == 2
        assert await session.send(ListSubscriptionsCommand()) == 3
        assert await session.send(UnsubscribeCommand((1,))) == 4
        received = await connection.wait_for_commands(4)

    assert [command["id"] for command in received] == [1, 2, 3, 4]
    assert received[0] == {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_tickers": ["KXA-1"],
            "use_yes_price": True,
        },
    }
    assert received[2] == {"id": 3, "cmd": "list_subscriptions"}
    assert connection.subscriptions == {}


async def test_subscribe_replies_arrive_as_frames(signer: Signer, clock: FrozenClock) -> None:
    async with FakeKalshiWs() as fake, session_for(fake, signer, clock) as session:
        await fake.wait_for_connection()
        await session.send(SubscribeCommand(("orderbook_delta", "trade"), ("KXA-1",), True))
        frames = await take(session, 2)

    envelopes = [decode_envelope(frame.payload) for frame in frames]
    assert [envelope.type for envelope in envelopes] == ["subscribed", "subscribed"]
    assert [payload_of(frame)["msg"]["sid"] for frame in frames] == [1, 2]
    assert all(envelope.id == 1 for envelope in envelopes)


async def test_frames_arrive_in_order_with_local_timestamps(signer: Signer) -> None:
    system_clock = SystemClock()
    before_mono, before_wall = system_clock.mono_ns(), system_clock.wall_ns()
    async with FakeKalshiWs() as fake, session_for(fake, signer, system_clock) as session:
        connection = await fake.wait_for_connection()
        for index in range(5):
            await connection.push_message("trade", {"n": index}, sid=1, seq=index + 1)
        frames = await take(session, 5)
    after_mono, after_wall = system_clock.mono_ns(), system_clock.wall_ns()

    assert [payload_of(frame)["msg"]["n"] for frame in frames] == [0, 1, 2, 3, 4]
    assert [payload_of(frame)["seq"] for frame in frames] == [1, 2, 3, 4, 5]
    mono = [frame.recv_mono_ns for frame in frames]
    wall = [frame.recv_wall_ns for frame in frames]
    assert mono == sorted(mono)
    assert wall == sorted(wall)
    assert before_mono <= mono[0]
    assert mono[-1] <= after_mono
    assert before_wall <= wall[0]
    assert wall[-1] <= after_wall


async def test_error_frame_is_an_ordinary_frame(signer: Signer, clock: FrozenClock) -> None:
    async with FakeKalshiWs() as fake, session_for(fake, signer, clock) as session:
        connection = await fake.wait_for_connection()
        await connection.push_message("trade", {"n": 0}, sid=3, seq=1)
        await connection.push_error(25, "subscription buffer overflow", sid=3)
        await connection.push_message("trade", {"n": 1}, sid=3, seq=2)
        frames = await take(session, 3)

    assert [decode_envelope(frame.payload).type for frame in frames] == ["trade", "error", "trade"]
    assert payload_of(frames[1])["msg"] == {"code": 25, "msg": "subscription buffer overflow"}


async def test_abrupt_close_raises_ws_closed(signer: Signer, clock: FrozenClock) -> None:
    async with FakeKalshiWs() as fake:
        session = session_for(fake, signer, clock)
        await session.connect()
        connection = await fake.wait_for_connection()
        await connection.close_abruptly()
        with pytest.raises(WsClosedError, match="closed by peer"):
            await take(session, 1)
        assert session.is_open is False
        await session.close()


async def test_graceful_close_by_peer_raises_ws_closed(signer: Signer, clock: FrozenClock) -> None:
    async with FakeKalshiWs() as fake:
        session = session_for(fake, signer, clock)
        await session.connect()
        connection = await fake.wait_for_connection()
        await connection.push_message("trade", {"n": 0}, sid=1, seq=1)
        await connection.close_gracefully()
        with pytest.raises(WsClosedError, match="closed by peer"):
            await take(session, 2)
        await session.close()


async def test_silence_past_the_timeout_raises_ws_closed(
    signer: Signer, clock: FrozenClock
) -> None:
    silence_timeout_ns = 50 * NS_PER_MS
    async with FakeKalshiWs() as fake:
        session = session_for(fake, signer, clock, silence_timeout_ns=silence_timeout_ns)
        await session.connect()
        connection = await fake.wait_for_connection()
        connection.go_silent()
        # Only the injected clock decides this; the wall-clock duration of the test
        # never does, so the outcome cannot depend on scheduling.
        clock.advance(silence_timeout_ns + 1)
        with pytest.raises(WsClosedError, match="silent"):
            await take(session, 1)
        await session.close()


async def test_a_still_connection_stays_open_while_the_clock_does_not_move(
    signer: Signer, clock: FrozenClock
) -> None:
    async with FakeKalshiWs() as fake:
        session = session_for(fake, signer, clock, silence_timeout_ns=NS_PER_MS)
        await session.connect()
        connection = await fake.wait_for_connection()
        clock.advance(NS_PER_MS // 2)
        await asyncio.sleep(0.02)
        assert session.is_open is True
        await connection.push_message("trade", {"n": 0}, sid=1, seq=1)
        assert len(await take(session, 1)) == 1
        await session.close()


async def test_slow_consumer_drops_the_oldest_frames(signer: Signer, clock: FrozenClock) -> None:
    async with (
        FakeKalshiWs() as fake,
        session_for(fake, signer, clock, max_buffered_frames=2) as session,
    ):
        connection = await fake.wait_for_connection()
        for index in range(5):
            await connection.push_message("trade", {"n": index}, sid=1, seq=index + 1)
        await until(lambda: session.frames_dropped == 3)
        frames = await take(session, 2)

    # The newest frames survive: a resync after a gap should start from fresh state.
    assert [payload_of(frame)["msg"]["n"] for frame in frames] == [3, 4]
    assert session.frames_dropped == 3


async def test_close_is_idempotent_and_ends_the_stream(signer: Signer, clock: FrozenClock) -> None:
    async with FakeKalshiWs() as fake:
        session = session_for(fake, signer, clock)
        await session.connect()
        connection = await fake.wait_for_connection()
        await connection.push_message("trade", {"n": 0}, sid=1, seq=1)
        assert len(await take(session, 1)) == 1
        await session.close()
        await session.close()
        assert session.is_open is False
        # A deliberate close ends the stream instead of raising: only an unrequested
        # end of the connection is an error.
        assert await take(session, 5) == []
        with pytest.raises(WsClosedError, match="closed"):
            await session.send(ListSubscriptionsCommand())


async def test_session_is_an_async_context_manager(signer: Signer, clock: FrozenClock) -> None:
    async with FakeKalshiWs() as fake:
        async with session_for(fake, signer, clock) as session:
            assert session.conn_id == 7
            open_inside_block = session.is_open
            await fake.wait_for_connection()
        assert open_inside_block is True
        assert session.is_open is False
        with pytest.raises(RuntimeError, match="single-use"):
            await session.connect()


async def test_frames_before_connect_is_a_programming_error(
    signer: Signer, clock: FrozenClock
) -> None:
    session = WsSession("ws://127.0.0.1:1/trade-api/ws/v2", signer, clock)
    with pytest.raises(RuntimeError, match="not connected"):
        await take(session, 1)
    with pytest.raises(RuntimeError, match="not connected"):
        await session.send(ListSubscriptionsCommand())


async def test_unreachable_endpoint_raises_transport_error(
    signer: Signer, clock: FrozenClock
) -> None:
    async with FakeKalshiWs() as fake:
        url = fake.url
    session = WsSession(url, signer, clock)
    with pytest.raises(KalshiTransportError, match="failed"):
        await session.connect()
    await session.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"silence_timeout_ns": 0},
        {"connect_timeout_ns": -1},
        {"send_timeout_ns": 0},
        {"close_timeout_ns": 0},
        {"max_buffered_frames": 0},
    ],
)
def test_bounds_must_be_positive(
    signer: Signer, clock: FrozenClock, kwargs: dict[str, int]
) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        WsSession("ws://127.0.0.1:1/x", signer, clock, **kwargs)


async def test_no_frame_is_lost_when_polls_cancel_recv(signer: Signer, clock: FrozenClock) -> None:
    """The silence poll cancels a pending ``recv`` constantly; no frame may be lost.

    The reader wraps ``recv`` in a timeout, so a quiet connection cancels and reissues
    it many times a second. If ``websockets`` consumed a frame before honoring the
    cancellation, the recorder would lose data with no gap record to show for it. The
    clock never advances here, so silence never trips and only the cancellation churn
    is under test.
    """
    total = 400
    async with FakeKalshiWs() as fake:
        session = session_for(fake, signer, clock, silence_timeout_ns=200_000)
        await session.connect()
        connection = await fake.wait_for_connection()
        received: list[RawFrame] = []

        async def consume() -> None:
            stream = cast("AsyncGenerator[RawFrame, None]", session.frames())
            async with aclosing(stream):
                async for frame in stream:
                    received.append(frame)
                    if len(received) >= total:
                        return

        consumer = asyncio.create_task(consume())
        for seq in range(total):
            await connection.push(msgspec.json.encode({"type": "trade", "sid": 1, "seq": seq}))
            if seq % 20 == 0:
                await asyncio.sleep(0)
        try:
            await asyncio.wait_for(consumer, timeout=10.0)
        finally:
            consumer.cancel()
            await session.close()

    assert session.frames_dropped == 0
    assert [payload_of(frame)["seq"] for frame in received] == list(range(total))
