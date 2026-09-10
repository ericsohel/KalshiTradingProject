"""One authenticated Kalshi WebSocket connection: commands out, raw frames in.

Responsibility: own a single WebSocket connection, sign its upgrade request, serialize
commands onto it, and hand every inbound frame to the caller as raw bytes stamped with
local receipt times. It decodes nothing, because the recorder writes bytes to disk
before it parses them (ADR 0001); it tracks no sequence numbers and never reconnects,
because gap handling and reconnect policy belong to the recorder
(docs/ARCHITECTURE.md 7.1).

Invariants: command ids start at 1, increase strictly, and are assigned in send order;
frames reach ``frames()`` in receive order; the buffer between the socket reader and the
consumer is bounded, and on overflow the oldest frame is dropped so that the socket is
always drained (a blocked reader would trip the server's own buffer overflow, error 25);
every wait on the socket has a deadline, and a connection that says nothing for
``silence_timeout_ns`` is declared dead.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import ClassVar, Final, Literal

import msgspec
from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from tape.client.auth import Signer
from tape.errors import KalshiTransportError, WsClosedError
from tape.timeutil import NS_PER_MS, NS_PER_S, Clock, Ns

__all__ = [
    "DEFAULT_CLOSE_TIMEOUT_NS",
    "DEFAULT_CONNECT_TIMEOUT_NS",
    "DEFAULT_MAX_BUFFERED_FRAMES",
    "DEFAULT_SEND_TIMEOUT_NS",
    "DEFAULT_SILENCE_TIMEOUT_NS",
    "WS_SIGN_METHOD",
    "WS_SIGN_PATH",
    "Command",
    "ListSubscriptionsCommand",
    "RawFrame",
    "SubscribeCommand",
    "UnsubscribeCommand",
    "UpdateSubscriptionAction",
    "UpdateSubscriptionCommand",
    "WsSession",
    "encode_command",
]

WS_SIGN_METHOD: Final = "GET"
"""Method signed on the upgrade request, regardless of the connection URL."""

WS_SIGN_PATH: Final = "/trade-api/ws/v2"
"""Path signed on the upgrade request (docs/DATA_FORMATS.md 3.1)."""

DEFAULT_CONNECT_TIMEOUT_NS: Final = 10 * NS_PER_S
DEFAULT_SEND_TIMEOUT_NS: Final = 10 * NS_PER_S
DEFAULT_CLOSE_TIMEOUT_NS: Final = 5 * NS_PER_S
DEFAULT_SILENCE_TIMEOUT_NS: Final = 30 * NS_PER_S
"""Three times the server's 10-second heartbeat interval (docs/ARCHITECTURE.md 7.1)."""

DEFAULT_MAX_BUFFERED_FRAMES: Final = 4096
"""Frames held between the socket reader and a slow consumer before the oldest is dropped."""

_SOCKET_QUEUE_FRAMES: Final = 64
"""Bound on the ``websockets`` library's own receive buffer; our reader drains it at once."""

_MAX_POLL_NS: Final = NS_PER_S
"""Longest wait between silence checks, so the check is never more than a second late."""

UpdateSubscriptionAction = Literal["add_markets", "delete_markets", "get_snapshot"]


class RawFrame(msgspec.Struct, frozen=True, kw_only=True):
    """One inbound WebSocket frame, undecoded, with the times it was received.

    Attributes:
        payload: The frame exactly as it arrived, UTF-8 bytes for a text frame. This is
            what the recorder writes to disk before anything parses it.
        recv_mono_ns: Monotonic reading taken as the frame left the socket.
        recv_wall_ns: Wall-clock reading taken as the frame left the socket.
    """

    payload: bytes
    recv_mono_ns: Ns
    recv_wall_ns: Ns


class SubscribeCommand(msgspec.Struct, frozen=True, omit_defaults=True):
    """``subscribe``: open one subscription per channel (docs/DATA_FORMATS.md 3.2).

    Attributes:
        channels: Channel names, for example ``("orderbook_delta", "trade")``.
        market_tickers: Markets to subscribe; omitted to receive every market the
            channel allows.
        use_yes_price: Orderbook channels only; ``True`` puts both sides on the YES
            price scale (ADR 0006).
        send_initial_snapshot: Orderbook channels only; whether the server sends a
            snapshot per market before the deltas.

    Raises:
        ValueError: If ``channels`` is empty, or ``market_tickers`` is present but
            empty; the server would answer with an error frame instead of a
            subscription.
    """

    cmd: ClassVar[str] = "subscribe"

    channels: tuple[str, ...]
    market_tickers: tuple[str, ...] | None = None
    use_yes_price: bool | None = None
    send_initial_snapshot: bool | None = None

    def __post_init__(self) -> None:
        if not self.channels:
            raise ValueError("subscribe needs at least one channel")
        if self.market_tickers is not None and not self.market_tickers:
            raise ValueError("market_tickers must be omitted rather than empty")


class UpdateSubscriptionCommand(msgspec.Struct, frozen=True, omit_defaults=True):
    """``update_subscription``: change or resnapshot one live subscription.

    Attributes:
        sid: Subscription id the server assigned; connection-scoped.
        action: ``add_markets``, ``delete_markets``, or ``get_snapshot``.
        market_tickers: Markets to add or delete; omitted for ``get_snapshot``.

    Raises:
        ValueError: If the markets do not match the action, which the server rejects.
    """

    cmd: ClassVar[str] = "update_subscription"

    sid: int
    action: UpdateSubscriptionAction
    market_tickers: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        if self.action == "get_snapshot":
            if self.market_tickers is not None:
                raise ValueError("get_snapshot takes no market_tickers")
        elif not self.market_tickers:
            raise ValueError(f"{self.action} needs at least one market ticker")


class UnsubscribeCommand(msgspec.Struct, frozen=True, omit_defaults=True):
    """``unsubscribe``: close subscriptions by id.

    Attributes:
        sids: Subscription ids to close.

    Raises:
        ValueError: If ``sids`` is empty.
    """

    cmd: ClassVar[str] = "unsubscribe"

    sids: tuple[int, ...]

    def __post_init__(self) -> None:
        if not self.sids:
            raise ValueError("unsubscribe needs at least one sid")


class ListSubscriptionsCommand(msgspec.Struct, frozen=True, omit_defaults=True):
    """``list_subscriptions``: ask for this connection's subscriptions. Takes no params."""

    cmd: ClassVar[str] = "list_subscriptions"


Command = (
    SubscribeCommand | UpdateSubscriptionCommand | UnsubscribeCommand | ListSubscriptionsCommand
)
"""Every command this session can send."""


def encode_command(command: Command, command_id: int) -> bytes:
    """Serialize a command to the exact JSON Kalshi expects.

    The result is ``{"id", "cmd", "params"}`` in that order, with ``None`` fields
    omitted and ``params`` itself omitted when the command has none. The recorder also
    writes these bytes into the segment as a ``COMMAND`` record, so the encoding is
    part of the tape.

    Args:
        command: The command to send.
        command_id: Id the session assigned; the server echoes it in the response.

    Returns:
        UTF-8 JSON bytes.

    Raises:
        ValueError: If ``command_id`` is not positive; Kalshi's ids start at 1.
    """
    if command_id <= 0:
        raise ValueError(f"command id must be positive, got {command_id}")
    body: dict[str, object] = {"id": command_id, "cmd": command.cmd}
    params = msgspec.to_builtins(command)
    if params:
        body["params"] = params
    return msgspec.json.encode(body)


class _EndOfStream:
    """Sentinel queued once, after the last frame, to wake a waiting consumer."""


_END_OF_STREAM: Final = _EndOfStream()


def _closed(detail: str, cause: BaseException | None = None) -> WsClosedError:
    """Build a ``WsClosedError`` that keeps the underlying cause for logs."""
    error = WsClosedError(detail)
    error.__cause__ = cause
    return error


def _seconds(duration_ns: int) -> float:
    """Convert a nanosecond duration to the seconds asyncio wants."""
    return duration_ns / NS_PER_S


class WsSession:
    """An authenticated WebSocket session that yields undecoded frames.

    The session connects once. It does not reconnect, decode, or interpret anything:
    a server error frame is an ordinary frame, delivered in order like any other, and
    the recorder decides what it means.

    Backpressure: a reader task drains the socket into a bounded buffer. When a
    consumer of :meth:`frames` falls behind and the buffer is full, the oldest frame is
    dropped and :attr:`frames_dropped` counts it. Dropping is preferred to blocking
    because a blocked reader stalls the socket, which makes the server overflow its own
    buffer and drop far more (docs/ARCHITECTURE.md 7.1).

    Liveness: Kalshi pings every 10 seconds and ``websockets`` answers those pongs
    itself, so no client keepalive is configured here. The library does not surface
    ping frames, so the silence window is measured over inbound frames only; the
    30-second default is three heartbeats, and a false positive costs one reconnect.

    Args:
        url: Endpoint to connect to, for example
            ``wss://external-api-ws.kalshi.com/trade-api/ws/v2``.
        signer: Signs the upgrade request. Required: Kalshi refuses unauthenticated
            connections even for public channels (docs/DATA_FORMATS.md 3.1).
        clock: The only time source this session reads (ADR 0004). It stamps receipts,
            the handshake timestamp, and the silence check.
        conn_id: Identifier for this connection, copied onto records the recorder writes.
        silence_timeout_ns: Inbound silence after which the connection is declared dead.
        connect_timeout_ns: Deadline for the TCP, TLS, and upgrade handshake.
        send_timeout_ns: Deadline for writing one command.
        close_timeout_ns: Deadline for the closing handshake before the socket is dropped.
        max_buffered_frames: Frames held for a slow consumer before the oldest is dropped.

    Raises:
        ValueError: If a timeout or buffer bound is not positive.
    """

    def __init__(
        self,
        url: str,
        signer: Signer,
        clock: Clock,
        *,
        conn_id: int = 0,
        silence_timeout_ns: int = DEFAULT_SILENCE_TIMEOUT_NS,
        connect_timeout_ns: int = DEFAULT_CONNECT_TIMEOUT_NS,
        send_timeout_ns: int = DEFAULT_SEND_TIMEOUT_NS,
        close_timeout_ns: int = DEFAULT_CLOSE_TIMEOUT_NS,
        max_buffered_frames: int = DEFAULT_MAX_BUFFERED_FRAMES,
    ) -> None:
        for name, value in (
            ("silence_timeout_ns", silence_timeout_ns),
            ("connect_timeout_ns", connect_timeout_ns),
            ("send_timeout_ns", send_timeout_ns),
            ("close_timeout_ns", close_timeout_ns),
            ("max_buffered_frames", max_buffered_frames),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value}")
        self.conn_id = conn_id
        self._url = url
        self._signer = signer
        self._clock = clock
        self._silence_timeout_ns = silence_timeout_ns
        self._connect_timeout_ns = connect_timeout_ns
        self._send_timeout_ns = send_timeout_ns
        self._close_timeout_ns = close_timeout_ns
        self._max_buffered_frames = max_buffered_frames
        # One spare slot is reserved so the end-of-stream sentinel always fits.
        self._queue: asyncio.Queue[RawFrame | _EndOfStream] = asyncio.Queue(
            maxsize=max_buffered_frames + 1
        )
        self._connection: ClientConnection | None = None
        self._reader: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self._next_command_id = 1
        self._frames_dropped = 0
        # The silence window opens when the socket does, not when the reader task is
        # first scheduled, so a busy loop cannot postpone the liveness check.
        self._last_frame_ns = 0
        self._failure: WsClosedError | None = None
        self._started = False
        self._closing = False
        self._drained = False

    @property
    def frames_dropped(self) -> int:
        """Frames discarded because the consumer of :meth:`frames` fell behind."""
        return self._frames_dropped

    @property
    def is_open(self) -> bool:
        """Whether the socket is connected and has neither failed nor been closed."""
        return self._connection is not None and self._failure is None and not self._closing

    async def connect(self) -> None:
        """Open the connection, signing the upgrade, and start draining the socket.

        Raises:
            KalshiTransportError: If the handshake fails, is refused, or times out.
            RuntimeError: If :meth:`connect` was already called. A session is
                single-use, failed attempts included; the recorder builds a new one per
                attempt so that a retry cannot inherit stale state.
        """
        if self._started:
            raise RuntimeError(f"session {self.conn_id} is single-use and already connected")
        self._started = True
        now_ms = int(self._clock.wall_ns()) // NS_PER_MS
        headers = self._signer.headers(WS_SIGN_METHOD, WS_SIGN_PATH, now_ms=now_ms)
        try:
            self._connection = await ws_connect(
                self._url,
                additional_headers=headers,
                # The server drives the heartbeat and the library answers its pings;
                # a second, client-driven keepalive would only duplicate that.
                ping_interval=None,
                open_timeout=_seconds(self._connect_timeout_ns),
                close_timeout=_seconds(self._close_timeout_ns),
                max_queue=_SOCKET_QUEUE_FRAMES,
            )
        except TimeoutError as exc:
            raise KalshiTransportError(f"websocket handshake to {self._url} timed out") from exc
        except (OSError, WebSocketException) as exc:
            raise KalshiTransportError(f"websocket connect to {self._url} failed: {exc}") from exc
        self._last_frame_ns = int(self._clock.mono_ns())
        self._reader = asyncio.create_task(
            self._read_loop(self._connection), name=f"ws-reader-{self.conn_id}"
        )

    async def send(self, command: Command) -> int:
        """Assign the next command id, serialize the command, and write it.

        Args:
            command: The command to send.

        Returns:
            The id assigned to this command. The server echoes it on the response.

        Raises:
            WsClosedError: If the session is closed, has failed, or the socket closes
                mid-write.
            KalshiTransportError: If the write does not complete within the send timeout.
            RuntimeError: If :meth:`connect` has not been called.
        """
        connection = self._require_connection()
        async with self._send_lock:
            if self._failure is not None:
                raise self._failure
            command_id = self._next_command_id
            # The id is consumed even if the write fails: the frame may still have
            # reached the server, and a reused id would misattribute its response.
            self._next_command_id += 1
            payload = encode_command(command, command_id)
            try:
                async with asyncio.timeout(_seconds(self._send_timeout_ns)):
                    # Kalshi's endpoint speaks text frames, so the JSON is sent as str.
                    await connection.send(payload.decode())
            except ConnectionClosed as exc:
                raise _closed(f"connection {self.conn_id} closed while sending", exc) from exc
            except TimeoutError as exc:
                raise KalshiTransportError(
                    f"connection {self.conn_id} send timed out after {self._send_timeout_ns} ns"
                ) from exc
            return command_id

    async def frames(self) -> AsyncIterator[RawFrame]:
        """Yield every received frame, in receive order, until the stream ends.

        Exactly one consumer may iterate a session; two would split the stream between
        them. Iteration ends without an error only when :meth:`close` was called; after
        that the frames already buffered are still yielded before it ends.

        Yields:
            Each inbound frame, undecoded.

        Raises:
            WsClosedError: If the peer closed the connection, the connection failed, or
                nothing arrived within the silence timeout.
            RuntimeError: If :meth:`connect` has not been called.
        """
        if not self._started:
            raise RuntimeError(f"session {self.conn_id} is not connected")
        while not self._drained:
            # Bounded by construction: the reader queues the sentinel on every exit
            # path, and it declares silence within the silence timeout.
            item = await self._queue.get()
            if isinstance(item, _EndOfStream):
                self._drained = True
                break
            yield item
        if self._failure is not None:
            raise self._failure

    async def close(self) -> None:
        """Stop reading and close the socket. Idempotent and safe after a failure."""
        self._closing = True
        reader, self._reader = self._reader, None
        if reader is not None:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader
        connection, self._connection = self._connection, None
        if connection is not None:
            # The library aborts the socket after its own close_timeout; this deadline
            # is the backstop that keeps shutdown bounded even if that does not fire.
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(_seconds(self._close_timeout_ns)):
                    await connection.close()

    async def __aenter__(self) -> WsSession:
        """Connect and return the session."""
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Close the session, whatever happened inside the block."""
        await self.close()

    def _require_connection(self) -> ClientConnection:
        if self._failure is not None:
            raise self._failure
        if self._closing:
            raise _closed(f"session {self.conn_id} is closed")
        if self._connection is None:
            raise RuntimeError(f"session {self.conn_id} is not connected")
        return self._connection

    async def _read_loop(self, connection: ClientConnection) -> None:
        """Drain the socket into the bounded buffer until it closes or goes silent."""
        # Each poll cancels the pending ``recv``. That is safe: ``websockets`` restores
        # its assembler state on cancellation, so no frame is consumed and lost. The
        # behavior is not part of the library's documented contract, so
        # ``test_no_frame_is_lost_when_polls_cancel_recv`` pins it and will fail loudly
        # if an upgrade changes it.
        poll_s = _seconds(min(self._silence_timeout_ns, _MAX_POLL_NS))
        try:
            while True:
                try:
                    async with asyncio.timeout(poll_s):
                        payload = await connection.recv(decode=False)
                except TimeoutError:
                    silent_ns = int(self._clock.mono_ns()) - self._last_frame_ns
                    if silent_ns >= self._silence_timeout_ns:
                        self._failure = _closed(
                            f"connection {self.conn_id} silent for {silent_ns} ns, over the "
                            f"{self._silence_timeout_ns} ns limit"
                        )
                        return
                    continue
                self._last_frame_ns = int(self._clock.mono_ns())
                self._offer(
                    RawFrame(
                        payload=payload,
                        recv_mono_ns=Ns(self._last_frame_ns),
                        recv_wall_ns=self._clock.wall_ns(),
                    )
                )
        except ConnectionClosed as exc:
            self._failure = _closed(f"connection {self.conn_id} closed by peer", exc)
        except Exception as exc:
            # A bug in the reader must surface as a dead connection, never as a stream
            # that quietly stops: the recorder reconnects, and no frames go missing
            # without a gap record (docs/ENGINEERING_STANDARDS.md 3.5).
            self._failure = _closed(f"connection {self.conn_id} reader failed: {exc!r}", exc)
        finally:
            self._queue.put_nowait(_END_OF_STREAM)

    def _offer(self, frame: RawFrame) -> None:
        """Buffer a frame, dropping the oldest rather than ever blocking the socket."""
        if self._queue.qsize() >= self._max_buffered_frames:
            self._queue.get_nowait()
            self._frames_dropped += 1
        self._queue.put_nowait(frame)
