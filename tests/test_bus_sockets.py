"""ZeroMQ transport: endpoints, send accounting, ipc path claims, and real round trips."""

from __future__ import annotations

import asyncio
import logging
import socket
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Final

import pytest
import zmq

from tape.bus import (
    BOOK_FRESH,
    RESET_GAP,
    LiveBooks,
    PublisherStats,
    SequencedPublisher,
    SubscriberStats,
    ZmqPublisher,
    ZmqSubscriber,
    check_endpoint,
    decode_bus_envelope,
)
from tape.errors import BusError
from tape.events import BookDelta, BookRefresh, Level, Receipt, Side
from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import Ns

PROBE: Final = b"md.PROBE"
RECEIPT: Final = Receipt(conn_id=2, recv_mono_ns=Ns(1), recv_wall_ns=Ns(1))
HWM: Final = 1000


def endpoint_in(directory: Path, name: str = "bus.sock") -> str:
    return f"ipc://{directory / name}"


@pytest.fixture
def publisher(ipc_dir: Path) -> Iterator[ZmqPublisher]:
    bound = ZmqPublisher(endpoint_in(ipc_dir), send_hwm=HWM)
    try:
        yield bound
    finally:
        bound.close()


async def connect(publisher: ZmqPublisher, messages: AsyncIterator[tuple[bytes, bytes]]) -> None:
    """Publish probes until one arrives.

    A subscriber connects asynchronously and PUB drops what it sends before then; once a probe
    has arrived, everything published later arrives after the probes still in flight.
    """
    receiving = asyncio.ensure_future(anext(messages))
    async with asyncio.timeout(5):
        while not receiving.done():
            publisher.publish(PROBE, b"")
            await asyncio.wait({receiving}, timeout=0.01)
    assert receiving.result() == (PROBE, b"")


async def next_message(messages: AsyncIterator[tuple[bytes, bytes]]) -> tuple[bytes, bytes]:
    """The next message that is not a probe."""
    async with asyncio.timeout(5):
        message = await anext(messages)
        while message[0] == PROBE:
            message = await anext(messages)
    return message


@pytest.mark.parametrize(
    "endpoint",
    ["ipc:///run/tape/bus.sock", "tcp://127.0.0.1:5555", "tcp://*:5555", "tcp://[::1]:5555"],
)
def test_absolute_ipc_paths_and_tcp_addresses_are_endpoints(endpoint: str) -> None:
    check_endpoint(endpoint)


@pytest.mark.parametrize(
    ("endpoint", "message"),
    [
        ("ipc://bus.sock", "an ipc path must be absolute"),
        ("ipc:///" + "x" * zmq.IPC_PATH_MAX_LEN, f"allows at most {zmq.IPC_PATH_MAX_LEN}"),
        ("tcp://127.0.0.1", "must be ipc:///absolute/path or tcp://host:port"),
        ("inproc://bus", "must be ipc:///absolute/path or tcp://host:port"),
        ("", "must be ipc:///absolute/path or tcp://host:port"),
    ],
)
def test_other_endpoints_are_refused_before_anything_binds(endpoint: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        check_endpoint(endpoint)
    with pytest.raises(ValueError, match=message):
        ZmqPublisher(endpoint, send_hwm=HWM)
    with pytest.raises(ValueError, match=message):
        ZmqSubscriber(endpoint, receive_hwm=HWM)


def test_queue_bounds_must_be_positive(ipc_dir: Path) -> None:
    with pytest.raises(ValueError, match="send_hwm must be positive"):
        ZmqPublisher(endpoint_in(ipc_dir), send_hwm=0)
    with pytest.raises(ValueError, match="receive_hwm must be positive"):
        ZmqSubscriber(endpoint_in(ipc_dir), receive_hwm=0)
    assert list(ipc_dir.iterdir()) == []


def test_with_no_subscriber_every_message_is_sent_and_after_close_counted_as_an_error(
    publisher: ZmqPublisher, ipc_dir: Path
) -> None:
    for _ in range(10_000):
        publisher.publish(b"md.KXA-1", b"x" * 100)
    assert publisher.stats == PublisherStats(sent=10_000, dropped=0, errors=0)
    assert publisher.endpoint == endpoint_in(ipc_dir)

    publisher.close()
    publisher.close()
    publisher.publish(b"md.KXA-1", b"x")
    assert publisher.stats == PublisherStats(sent=10_000, dropped=0, errors=1)
    # The path is free again for the next process.
    ZmqPublisher(endpoint_in(ipc_dir), send_hwm=HWM).close()


def test_refused_and_failed_sends_are_counted_and_never_raised(
    publisher: ZmqPublisher, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    outcomes: Iterator[zmq.ZMQError | None] = iter(
        [zmq.Again(), zmq.ZMQError(zmq.EFSM), zmq.ZMQError(zmq.ENOTSOCK), None]
    )

    def send_multipart(self: zmq.Socket[bytes], frames: object, flags: int = 0) -> None:
        _ = (self, frames, flags)
        outcome = next(outcomes)
        if outcome is not None:
            raise outcome

    monkeypatch.setattr(zmq.Socket, "send_multipart", send_multipart)
    for _ in range(4):
        publisher.publish(b"md.KXA-1", b"payload")

    assert publisher.stats == PublisherStats(sent=1, dropped=1, errors=2)
    (logged,) = [r for r in caplog.records if r.getMessage().startswith("bus send failed")]
    assert logged.levelno == logging.ERROR


def test_a_stale_socket_is_replaced_but_a_live_one_or_any_other_file_is_left_alone(
    ipc_dir: Path,
) -> None:
    path = ipc_dir / "bus.sock"
    # A process that died leaves its socket file behind, with nothing listening on it.
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as leftover:
        leftover.bind(str(path))
    assert path.is_socket()

    first = ZmqPublisher(f"ipc://{path}", send_hwm=HWM)
    try:
        with pytest.raises(BusError, match="in use by a running publisher"):
            ZmqPublisher(f"ipc://{path}", send_hwm=HWM)
        assert path.is_socket()
    finally:
        first.close()

    notes = ipc_dir / "notes.txt"
    notes.write_text("keep me")
    with pytest.raises(BusError, match="exists and is not a socket"):
        ZmqPublisher(f"ipc://{notes}", send_hwm=HWM)
    assert notes.read_text() == "keep me"

    with pytest.raises(BusError, match="cannot bind the bus to"):
        ZmqPublisher(endpoint_in(ipc_dir / "missing"), send_hwm=HWM)


async def test_messages_arrive_in_order_filtered_by_topic_prefix(
    publisher: ZmqPublisher, ipc_dir: Path
) -> None:
    subscriber = ZmqSubscriber(endpoint_in(ipc_dir), receive_hwm=HWM)
    try:
        subscriber.subscribe(b"md.KXA-")
        subscriber.subscribe(b"md.PROBE")
        subscriber.subscribe(b"ctl.gap")
        messages = subscriber.messages()
        await connect(publisher, messages)
        for topic, payload in (
            (b"md.KXB-1", b"filtered"),
            (b"md.KXA-1", b"one"),
            (b"ctl.lifecycle", b"filtered"),
            (b"ctl.gap", b"two"),
            (b"md.KXA-22", b"three"),
        ):
            publisher.publish(topic, payload)
        received = [await next_message(messages) for _ in range(3)]
    finally:
        subscriber.close()

    # A prefix matches every topic that starts with it, so md.KXA- also receives md.KXA-22.
    assert received == [(b"md.KXA-1", b"one"), (b"ctl.gap", b"two"), (b"md.KXA-22", b"three")]
    assert subscriber.stats.malformed == 0


async def test_a_message_without_two_frames_is_skipped_and_counted(ipc_dir: Path) -> None:
    endpoint = endpoint_in(ipc_dir)
    context: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
    raw = context.socket(zmq.PUB)
    raw.setsockopt(zmq.LINGER, 0)
    raw.bind(endpoint)
    subscriber = ZmqSubscriber(endpoint, receive_hwm=HWM)
    try:
        subscriber.subscribe(b"")
        messages = subscriber.messages()
        receiving = asyncio.ensure_future(anext(messages))
        async with asyncio.timeout(5):
            while not receiving.done():
                raw.send_multipart((b"md.A", b"payload", b"extra"))
                raw.send_multipart((b"md.A", b"payload"))
                await asyncio.wait({receiving}, timeout=0.01)
        assert receiving.result() == (b"md.A", b"payload")
        assert subscriber.stats.malformed >= 1
        assert subscriber.stats.received == 1
    finally:
        subscriber.close()
        raw.close()
        context.term()


async def test_closing_a_subscriber_ends_its_stream_and_refuses_new_subscriptions(
    ipc_dir: Path,
) -> None:
    subscriber = ZmqSubscriber(endpoint_in(ipc_dir), receive_hwm=HWM)
    subscriber.subscribe(b"")
    receiving = asyncio.ensure_future(anext(subscriber.messages()))
    await asyncio.sleep(0.01)
    assert not receiving.done()

    subscriber.close()
    subscriber.close()

    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(receiving, timeout=5)
    with pytest.raises(StopAsyncIteration):
        await anext(subscriber.messages())
    with pytest.raises(BusError, match="cannot subscribe"):
        subscriber.subscribe(b"md.")
    assert subscriber.stats == SubscriberStats(received=0, malformed=0)


async def test_a_slow_subscriber_loses_messages_alone_sees_the_gap_and_recovers_at_a_refresh(
    ipc_dir: Path,
) -> None:
    endpoint = endpoint_in(ipc_dir)
    publisher = ZmqPublisher(endpoint, send_hwm=10)
    subscriber = ZmqSubscriber(endpoint, receive_hwm=10)
    bus = SequencedPublisher(publisher, epoch=1)
    live = LiveBooks()
    image = BookRefresh(
        ticker="PROBE",
        ts_ms=None,
        receipt=RECEIPT,
        stale=False,
        bids=(Level(PriceE4(4000), CountE2(100)),),
        asks=(),
    )
    bump = BookDelta(
        ticker="PROBE",
        ts_ms=None,
        receipt=RECEIPT,
        sid=1,
        seq=None,
        side=Side.BID,
        price=PriceE4(3000),
        delta=100,
    )
    try:
        subscriber.subscribe(b"")
        messages = subscriber.messages()
        receiving = asyncio.ensure_future(anext(messages))
        async with asyncio.timeout(5):
            while not receiving.done():
                bus.publish(image)
                await asyncio.wait({receiving}, timeout=0.01)
        live.observe(decode_bus_envelope(receiving.result()[1]))

        # The subscriber reads nothing while the publisher sends far more than every queue
        # between them holds. Every send returns at once and none is refused.
        for _ in range(50_000):
            bus.publish(bump)
        burst_end = bus.last_seq
        assert bus.stats == PublisherStats(sent=burst_end, dropped=0, errors=0)

        # Reading resumes: what was queued arrives, then nothing; the publisher's next refresh
        # image, repeated until the subscriber has room for one, reveals the loss and repairs it.
        gaps = 0
        async with asyncio.timeout(10):
            while True:
                receiving = asyncio.ensure_future(anext(messages))
                await asyncio.wait({receiving}, timeout=0.01)
                while not receiving.done():
                    bus.publish(image)
                    await asyncio.wait({receiving}, timeout=0.01)
                envelope = decode_bus_envelope(receiving.result()[1])
                gaps += live.observe(envelope).reset == RESET_GAP
                if envelope.bus_seq > burst_end:
                    break
        assert bus.stats.dropped == bus.stats.errors == 0
    finally:
        subscriber.close()
        publisher.close()

    assert gaps >= 1
    assert live.stats.missed > 0
    assert live.status("PROBE") == BOOK_FRESH
    assert live.books()["PROBE"].levels(Side.BID) == list(image.bids)
