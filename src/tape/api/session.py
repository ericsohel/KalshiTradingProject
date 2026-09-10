"""One live WebSocket connection: its subscriptions, its bounded queue, and its limits.

Responsibility: carry the messages of docs/FRONTEND.md 4.2 to one client in the order they were
offered, and accept that client's messages within the contract's limits. The feed, a
:class:`SessionFeed` such as :class:`tape.api.hub.LiveHub`, decides what a subscription contains
and offers messages; the session queues, sends, and polices. The transport is a
:class:`LiveSocket`, so the session is tested without a server.

Backpressure. At most ``queue_max`` messages wait. An offer that does not fit discards everything
queued and the offer itself, except the offer's own replies (``subscribed`` or ``error``), and
queues, per subscribed market, ``resync`` with ``client_lag`` and the feed's current snapshot
when it holds the book; the third such lag within 60 seconds closes the connection with 4000.

Client limits. A message over 4096 UTF-8 bytes, or an eleventh message within one second, closes
the connection with 1008. Malformed JSON, an unknown ``op``, or a message that does not fit its
schema is answered with ``error``, and the connection stays open.

Invariants: messages leave in the order offered; nothing is sent after a close; every send and
close has a deadline; and time is read only through the injected clock.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Sequence
from typing import Final, Protocol

import msgspec

from tape.api.contract import (
    CLOSE_POLICY_VIOLATION,
    CLOSE_TOO_SLOW,
    ERROR_INVALID_MESSAGE,
    ERROR_MALFORMED_JSON,
    ERROR_UNKNOWN_OP,
    LAG_LIMIT,
    LAG_WINDOW_S,
    MAX_CLIENT_MESSAGE_BYTES,
    MAX_CLIENT_MESSAGES_PER_S,
    OP_SUBSCRIBE,
    RESYNC_CLIENT_LAG,
    ErrorMessage,
    ResyncMessage,
    ServerMessage,
    SnapshotMessage,
    SubscribedMessage,
    SubscribeRequest,
)
from tape.timeutil import NS_PER_S, Clock

__all__ = [
    "CLOSE_TIMEOUT_S",
    "SEND_TIMEOUT_S",
    "ClientSession",
    "LiveSocket",
    "SessionFeed",
]

SEND_TIMEOUT_S: Final = 10
"""A send that takes longer means the client stopped reading; the connection is abandoned."""

CLOSE_TIMEOUT_S: Final = 5
"""Deadline for sending a close frame."""


class LiveSocket(Protocol):
    """The transport of one live connection, already accepted."""

    async def receive(self) -> str | bytes | None:
        """The next frame from the client, or ``None`` once the client has disconnected."""
        ...

    async def send(self, text: str) -> bool:
        """Send one text frame.

        Returns:
            ``False`` if the client has already disconnected, so nothing was sent.
        """
        ...

    async def close(self, code: int) -> None:
        """Close the connection with a code; nothing happens if the client is already gone."""
        ...


class SessionFeed(Protocol):
    """What a session asks of the feed that serves it."""

    def subscribe(self, session: ClientSession, tickers: Sequence[str]) -> None:
        """Replace the session's subscription set and offer the reply and any snapshots."""
        ...

    def snapshot(self, ticker: str) -> SnapshotMessage | None:
        """The whole book of a market now, or ``None`` when the feed holds no book for it."""
        ...


class _OpProbe(msgspec.Struct, frozen=True):
    op: str


_encoder: Final = msgspec.json.Encoder()
_op_decoder: Final = msgspec.json.Decoder(_OpProbe)
_subscribe_decoder: Final = msgspec.json.Decoder(SubscribeRequest)


class ClientSession:
    """One live connection. See the module docstring.

    Not thread-safe; used on one event loop. Single-use: :meth:`run` once.

    Args:
        socket: The accepted connection.
        feed: Serves the session's subscriptions.
        clock: Time for the client's message rate and the lag window.
        queue_max: Most messages queued; positive.
        logger: Destination for logs; defaults to this module's logger.

    Raises:
        ValueError: If ``queue_max`` is not positive.
    """

    def __init__(
        self,
        socket: LiveSocket,
        feed: SessionFeed,
        *,
        clock: Clock,
        queue_max: int,
        logger: logging.Logger | None = None,
    ) -> None:
        if queue_max < 1:
            raise ValueError(f"queue_max must be positive, got {queue_max}")
        self._socket = socket
        self._feed = feed
        self._clock = clock
        self._queue_max = queue_max
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._queue: deque[ServerMessage] = deque()
        self._wake = asyncio.Event()
        self._subscriptions: tuple[str, ...] = ()
        self._received_ns: deque[int] = deque()
        self._lags_ns: deque[int] = deque()
        self._lags = 0
        self._close_code: int | None = None
        self._gone = False

    @property
    def subscriptions(self) -> tuple[str, ...]:
        """The markets this connection follows, in the order the client named them."""
        return self._subscriptions

    @property
    def close_code(self) -> int | None:
        """The code the server closed the connection with, or ``None``."""
        return self._close_code

    @property
    def lags(self) -> int:
        """Times the queue overflowed since the connection opened."""
        return self._lags

    @property
    def queued(self) -> int:
        """Messages waiting to be sent."""
        return len(self._queue)

    def replace_subscriptions(self, tickers: Sequence[str]) -> None:
        """Record the subscription set the feed accepted.

        Args:
            tickers: The accepted markets, in the client's order.
        """
        self._subscriptions = tuple(tickers)

    def offer(self, messages: Sequence[ServerMessage]) -> None:
        """Queue messages for sending, or resynchronize the client if they do not fit.

        Never waits. Ignored once the connection is closing or gone.

        Args:
            messages: The messages, in the order they must arrive.
        """
        if self._close_code is not None or self._gone or not messages:
            return
        if len(self._queue) + len(messages) <= self._queue_max:
            self._queue.extend(messages)
            self._wake.set()
            return
        self._lag([m for m in messages if isinstance(m, SubscribedMessage | ErrorMessage)])

    def close(self, code: int) -> None:
        """Discard what is queued and close the connection with a code. Idempotent.

        Args:
            code: The WebSocket close code.
        """
        if self._close_code is not None:
            return
        self._close_code = code
        self._queue.clear()
        self._wake.set()

    async def run(self) -> None:
        """Send queued messages and read the client's until either side closes."""
        reader = asyncio.create_task(self._read(), name="live-session-reader")
        try:
            await self._write()
        finally:
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader

    # --------------------------------------------------------------------- outbound

    def _lag(self, replies: Sequence[ServerMessage]) -> None:
        """Replace the backlog with a resynchronization, or close a client that lags too often."""
        now_ns = int(self._clock.mono_ns())
        self._lags += 1
        self._lags_ns.append(now_ns)
        while now_ns - self._lags_ns[0] >= LAG_WINDOW_S * NS_PER_S:
            self._lags_ns.popleft()
        if len(self._lags_ns) >= LAG_LIMIT:
            self._log.info(
                "live client closed for lagging",
                extra={"lags": len(self._lags_ns), "window_s": LAG_WINDOW_S},
            )
            self.close(CLOSE_TOO_SLOW)
            return
        self._queue.clear()
        self._queue.extend(replies)
        for ticker in self._subscriptions:
            self._queue.append(ResyncMessage(ticker=ticker, reason=RESYNC_CLIENT_LAG))
            snapshot = self._feed.snapshot(ticker)
            if snapshot is not None:
                self._queue.append(snapshot)
        self._wake.set()

    async def _write(self) -> None:
        while True:
            while self._queue:
                if not await self._send(_encoder.encode(self._queue.popleft()).decode()):
                    return
            if self._gone:
                return
            if self._close_code is not None:
                try:
                    async with asyncio.timeout(CLOSE_TIMEOUT_S):
                        await self._socket.close(self._close_code)
                except TimeoutError:
                    self._log.info("live client did not take its close frame in time")
                return
            self._wake.clear()
            await self._wake.wait()

    async def _send(self, text: str) -> bool:
        """Send one frame; ``False`` means the client is gone or stopped reading."""
        try:
            async with asyncio.timeout(SEND_TIMEOUT_S):
                delivered = await self._socket.send(text)
        except TimeoutError:
            self._log.info("live client stopped reading; abandoning the connection")
            delivered = False
        if not delivered:
            self._gone = True
        return delivered

    # ---------------------------------------------------------------------- inbound

    async def _read(self) -> None:
        while True:
            frame = await self._socket.receive()
            if frame is None:
                self._gone = True
                self._wake.set()
                return
            if not self._within_limits(frame):
                return
            self._handle(frame)

    def _within_limits(self, frame: str | bytes) -> bool:
        """Count a client message against the limits, closing with 1008 on a violation."""
        size = len(frame.encode()) if isinstance(frame, str) else len(frame)
        now_ns = int(self._clock.mono_ns())
        self._received_ns.append(now_ns)
        while now_ns - self._received_ns[0] >= NS_PER_S:
            self._received_ns.popleft()
        too_large = size > MAX_CLIENT_MESSAGE_BYTES
        if too_large or len(self._received_ns) > MAX_CLIENT_MESSAGES_PER_S:
            self._log.info(
                "live client closed for breaking the message limits",
                extra={"bytes": size, "too_large": too_large},
            )
            self.close(CLOSE_POLICY_VIOLATION)
            return False
        return True

    def _handle(self, frame: str | bytes) -> None:
        """Act on one client message, answering anything unacceptable with ``error``."""
        try:
            probe = _op_decoder.decode(frame)
        except msgspec.ValidationError as exc:
            self._error(ERROR_INVALID_MESSAGE, str(exc))
            return
        except msgspec.DecodeError as exc:
            self._error(ERROR_MALFORMED_JSON, str(exc))
            return
        if probe.op != OP_SUBSCRIBE:
            self._error(ERROR_UNKNOWN_OP, f"unknown op {probe.op!r}; the only op is 'subscribe'")
            return
        try:
            request = _subscribe_decoder.decode(frame)
        except msgspec.ValidationError as exc:
            self._error(ERROR_INVALID_MESSAGE, str(exc))
            return
        self._feed.subscribe(self, request.tickers)

    def _error(self, code: str, message: str) -> None:
        self.offer([ErrorMessage(code=code, message=message)])
