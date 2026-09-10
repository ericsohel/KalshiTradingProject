"""A local WebSocket server that speaks Kalshi's command protocol.

The double covers docs/DATA_FORMATS.md 3.2 well enough that a client cannot tell the
difference for the parts we depend on: it records the handshake headers, answers
``subscribe`` with one ``subscribed`` per channel carrying an incrementing sid,
answers ``update_subscription`` with ``ok`` and ``unsubscribe`` with ``unsubscribed``,
and keeps a per-sid ``seq`` counter. Everything a test needs to provoke is explicit:
pushing arbitrary frames (malformed ones included), pushing an error frame, skipping a
sequence number, going silent, no longer reading the socket (so pings go unanswered), and
dropping the socket without a close handshake.

Order books are opt-in: a market given a book with :meth:`FakeKalshiWs.set_book` gets a
sequenced ``orderbook_snapshot`` on its ``orderbook_delta`` sid when it is subscribed,
added, or named by ``get_snapshot``. Markets without a book get none, so tests that never
set one see exactly the replies they always did.

It is deliberately more than the WebSocket session needs, because the recorder tests
reuse it. Waiting is event-driven (``wait_for_connection``, ``wait_for_commands``) so
tests never sleep to synchronize.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any, Final, Self

import msgspec
from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

__all__ = [
    "ORDERBOOK_CHANNEL",
    "UNKNOWN_COMMAND_ERROR_CODE",
    "UNKNOWN_SID_ERROR_CODE",
    "UNSUPPORTED_ACTION_ERROR_CODE",
    "FakeConnection",
    "FakeKalshiWs",
]

DEFAULT_WAIT_S: Final = 5.0
"""Every wait in the double is bounded, so a broken client fails the test instead of hanging."""

UNKNOWN_COMMAND_ERROR_CODE: Final = 5
"""Error code for an unknown command name (specs/asyncapi.yaml error table)."""

UNKNOWN_SID_ERROR_CODE: Final = 7
"""Error code for an unknown subscription id (specs/asyncapi.yaml error table)."""

UNSUPPORTED_ACTION_ERROR_CODE: Final = 13
"""Error code for an unsupported update_subscription action (specs/asyncapi.yaml)."""

ORDERBOOK_CHANNEL: Final = "orderbook_delta"
"""The only channel this double sends snapshots on, as Kalshi does."""


class FakeConnection:
    """One accepted connection: what it saw, and what a test makes it do.

    Args:
        connection: The live server-side connection.
        books: Snapshot payload fields by market ticker, shared with the server so a
            book set after the connection opened is still used.
    """

    def __init__(
        self,
        connection: ServerConnection,
        *,
        books: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self._connection = connection
        self._books: Mapping[str, Mapping[str, Any]] = {} if books is None else books
        self._commands: list[dict[str, Any]] = []
        self._progress = asyncio.Event()
        self._subscriptions: dict[int, dict[str, Any]] = {}
        self._seq_by_sid: dict[int, int] = {}
        self._next_sid = 1
        self._silent = False
        self._reading = True
        request = connection.request
        self._handshake_headers: dict[str, str] = (
            {} if request is None else dict(request.headers.raw_items())
        )
        self._path = "" if request is None else request.path

    @property
    def handshake_headers(self) -> Mapping[str, str]:
        """Headers of the upgrade request, as the server received them."""
        return self._handshake_headers

    @property
    def path(self) -> str:
        """Request target of the upgrade request, for example ``/trade-api/ws/v2``."""
        return self._path

    @property
    def commands(self) -> Sequence[Mapping[str, Any]]:
        """Every command received on this connection, decoded, in arrival order."""
        return self._commands

    @property
    def subscriptions(self) -> Mapping[int, Mapping[str, Any]]:
        """Live subscriptions by sid: ``{"channel": str, "market_tickers": list[str]}``."""
        return self._subscriptions

    def go_silent(self) -> None:
        """Stop answering commands. Pushes still work; the connection stays open."""
        self._silent = True

    def resume(self) -> None:
        """Answer commands again after :meth:`go_silent`."""
        self._silent = False

    def stop_reading(self) -> None:
        """Stop reading the socket, as a hung peer whose kernel keeps the TCP connection open.

        Nothing the client sends is processed until :meth:`resume_reading`: commands go
        unhandled and keepalive pings unanswered. ``websockets`` answers every ping it
        reads and has no switch to withhold pongs, so pausing the transport is the only
        way to miss one; the library's flow control resumes reading only after pausing it
        itself, so this pause holds. Pushes still work.
        """
        self._reading = False
        self._connection.transport.pause_reading()

    def resume_reading(self) -> None:
        """Read the socket again after :meth:`stop_reading`. Idempotent."""
        if not self._reading:
            self._reading = True
            self._connection.transport.resume_reading()

    async def push(self, payload: bytes | str) -> None:
        """Send one frame exactly as given, without touching it."""
        await self._connection.send(payload)

    async def push_message(
        self,
        message_type: str,
        msg: Mapping[str, Any],
        *,
        sid: int | None = None,
        seq: int | None = None,
    ) -> None:
        """Send one data frame in the standard envelope (docs/DATA_FORMATS.md 3.3)."""
        frame: dict[str, Any] = {"type": message_type}
        if sid is not None:
            frame["sid"] = sid
        if seq is not None:
            frame["seq"] = seq
        frame["msg"] = dict(msg)
        await self.push(msgspec.json.encode(frame))

    async def push_sequenced(self, message_type: str, msg: Mapping[str, Any], *, sid: int) -> int:
        """Send a data frame carrying the sid's next ``seq``, as the exchange numbers them.

        Returns:
            The ``seq`` the frame carried.
        """
        seq = self._next_seq(sid)
        await self.push_message(message_type, msg, sid=sid, seq=seq)
        return seq

    def skip_seq(self, sid: int, count: int = 1) -> None:
        """Consume ``count`` sequence numbers without sending, so the next frame shows a gap."""
        self._seq_by_sid[sid] = self._seq_by_sid.get(sid, 0) + count

    async def push_error(
        self,
        code: int,
        message: str,
        *,
        command_id: int | None = None,
        sid: int | None = None,
    ) -> None:
        """Send an error frame. To the client this is an ordinary frame."""
        frame: dict[str, Any] = {}
        if command_id is not None:
            frame["id"] = command_id
        if sid is not None:
            frame["sid"] = sid
        frame["type"] = "error"
        frame["msg"] = {"code": code, "msg": message}
        await self.push(msgspec.json.encode(frame))

    async def close_abruptly(self) -> None:
        """Drop the TCP connection with no closing handshake, as a dead peer would."""
        self._connection.transport.abort()
        await asyncio.sleep(0)

    async def close_gracefully(self, code: int = 1000, reason: str = "") -> None:
        """Close with a normal WebSocket closing handshake."""
        await self._connection.close(code, reason)

    async def wait_for_commands(
        self, count: int, *, timeout_s: float = DEFAULT_WAIT_S
    ) -> Sequence[Mapping[str, Any]]:
        """Wait until ``count`` commands have been handled, then return all of them.

        Raises:
            TimeoutError: If they do not arrive in time.
        """
        async with asyncio.timeout(timeout_s):
            while len(self._commands) < count:
                await self._progress.wait()
                self._progress.clear()
        return list(self._commands)

    async def handle(self, message: bytes | str) -> None:
        """Record one command and answer it unless the connection has gone silent."""
        command: dict[str, Any] = msgspec.json.decode(
            message.encode() if isinstance(message, str) else message, type=dict[str, Any]
        )
        self._commands.append(command)
        if not self._silent:
            for reply in self._replies(command):
                await self.push(msgspec.json.encode(reply))
        self._progress.set()

    def _replies(self, command: Mapping[str, Any]) -> list[dict[str, Any]]:
        command_id = command.get("id")
        params: Mapping[str, Any] = command.get("params") or {}
        match command.get("cmd"):
            case "subscribe":
                return self._subscribe(command_id, params)
            case "update_subscription":
                return self._update_subscription(command_id, params)
            case "unsubscribe":
                return self._unsubscribe(command_id, params)
            case "list_subscriptions":
                return [self._list_subscriptions(command_id)]
            case unknown:
                return [
                    self._error(command_id, UNKNOWN_COMMAND_ERROR_CODE, f"unknown cmd {unknown!r}")
                ]

    def _subscribe(self, command_id: Any, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        replies: list[dict[str, Any]] = []
        for channel in params.get("channels", []):
            sid = self._next_sid
            self._next_sid += 1
            self._subscriptions[sid] = {
                "channel": channel,
                "market_tickers": list(params.get("market_tickers") or []),
            }
            self._seq_by_sid[sid] = 0
            replies.append(
                {"id": command_id, "type": "subscribed", "msg": {"channel": channel, "sid": sid}}
            )
            if params.get("send_initial_snapshot") is not False:
                replies.extend(self._snapshots(sid, params.get("market_tickers") or []))
        return replies

    def _update_subscription(
        self, command_id: Any, params: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        sid = params.get("sid")
        subscription = self._subscriptions.get(sid) if isinstance(sid, int) else None
        if subscription is None:
            return [self._error(command_id, UNKNOWN_SID_ERROR_CODE, f"unknown sid {sid}", sid=sid)]
        tickers: list[str] = list(subscription["market_tickers"])
        incoming: list[str] = list(params.get("market_tickers") or [])
        snapshot_tickers: list[str] = []
        match params.get("action"):
            case "add_markets":
                snapshot_tickers = [t for t in incoming if t not in tickers]
                tickers = tickers + snapshot_tickers
            case "delete_markets":
                tickers = [t for t in tickers if t not in incoming]
            case "get_snapshot":
                snapshot_tickers = [t for t in incoming if t in tickers]
            case unknown:
                return [
                    self._error(
                        command_id,
                        UNSUPPORTED_ACTION_ERROR_CODE,
                        f"unknown action {unknown!r}",
                        sid=sid,
                    )
                ]
        subscription["market_tickers"] = tickers
        assert isinstance(sid, int)
        ok = {
            "id": command_id,
            "sid": sid,
            "seq": self._next_seq(sid),
            "type": "ok",
            "msg": {"market_tickers": tickers},
        }
        return [ok, *self._snapshots(sid, snapshot_tickers)]

    def _snapshots(self, sid: int, tickers: Sequence[str]) -> list[dict[str, Any]]:
        """Build a sequenced snapshot for every named market that has a book."""
        if self._subscriptions[sid]["channel"] != ORDERBOOK_CHANNEL:
            return []
        return [
            {
                "type": "orderbook_snapshot",
                "sid": sid,
                "seq": self._next_seq(sid),
                "msg": {"market_ticker": ticker, **self._books[ticker]},
            }
            for ticker in tickers
            if ticker in self._books
        ]

    def _unsubscribe(self, command_id: Any, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        replies: list[dict[str, Any]] = []
        for sid in params.get("sids", []):
            if not isinstance(sid, int) or sid not in self._subscriptions:
                replies.append(
                    self._error(command_id, UNKNOWN_SID_ERROR_CODE, f"unknown sid {sid}", sid=sid)
                )
                continue
            seq = self._next_seq(sid)
            del self._subscriptions[sid]
            replies.append({"id": command_id, "type": "unsubscribed", "sid": sid, "seq": seq})
        return replies

    def _list_subscriptions(self, command_id: Any) -> dict[str, Any]:
        # The AsyncAPI document does not pin this response; tests should assert on the
        # sids it reports, never on the exact shape.
        return {
            "id": command_id,
            "type": "subscriptions",
            "msg": {
                "subscriptions": [
                    {"sid": sid, "channel": info["channel"]}
                    for sid, info in sorted(self._subscriptions.items())
                ]
            },
        }

    def _error(
        self, command_id: Any, code: int, message: str, *, sid: Any = None
    ) -> dict[str, Any]:
        frame: dict[str, Any] = {"id": command_id}
        if sid is not None:
            frame["sid"] = sid
        frame["type"] = "error"
        frame["msg"] = {"code": code, "msg": message}
        return frame

    def _next_seq(self, sid: int) -> int:
        seq = self._seq_by_sid.get(sid, 0) + 1
        self._seq_by_sid[sid] = seq
        return seq


class FakeKalshiWs:
    """A ``ws://`` server on localhost that behaves like Kalshi's WebSocket endpoint.

    Use it as an async context manager; :attr:`url` is valid inside the block::

        async with FakeKalshiWs() as fake:
            session = WsSession(fake.url, signer, clock)
            ...

    Args:
        path: Request target appended to the URL, matching the real endpoint.
        ping_interval_s: Interval for server-driven pings, mirroring Kalshi's 10-second
            heartbeat. ``None`` disables them, which keeps most tests deterministic.
    """

    def __init__(
        self, *, path: str = "/trade-api/ws/v2", ping_interval_s: float | None = None
    ) -> None:
        self._path = path
        self._ping_interval_s = ping_interval_s
        self._server: Server | None = None
        self._connections: list[FakeConnection] = []
        self._books: dict[str, dict[str, Any]] = {}
        self._accepted = asyncio.Event()
        self._url = ""

    @property
    def url(self) -> str:
        """The ``ws://`` URL to connect to. Empty until the server has started."""
        return self._url

    @property
    def connections(self) -> Sequence[FakeConnection]:
        """Every connection accepted so far, in the order they were accepted."""
        return self._connections

    def set_book(
        self,
        ticker: str,
        *,
        yes: Sequence[tuple[str, str]] = (),
        no: Sequence[tuple[str, str]] = (),
    ) -> None:
        """Give a market the book its snapshots report, on every connection.

        Args:
            ticker: Market ticker.
            yes: ``(price_dollars, count_fp)`` pairs for ``yes_dollars_fp``.
            no: ``(price_dollars, count_fp)`` pairs for ``no_dollars_fp``. A side with no
                levels is omitted from the payload, as Kalshi omits it.
        """
        book: dict[str, Any] = {}
        if yes:
            book["yes_dollars_fp"] = [list(level) for level in yes]
        if no:
            book["no_dollars_fp"] = [list(level) for level in no]
        self._books[ticker] = book

    async def start(self) -> None:
        """Bind an ephemeral port on the loopback interface and start serving."""
        self._server = await serve(
            self._serve_connection,
            "127.0.0.1",
            0,
            ping_interval=self._ping_interval_s,
            ping_timeout=None,
        )
        port = self._server.sockets[0].getsockname()[1]
        self._url = f"ws://127.0.0.1:{port}{self._path}"

    async def stop(self) -> None:
        """Close every connection and stop serving. Idempotent."""
        server, self._server = self._server, None
        if server is not None:
            # A connection that is not reading would never see the closing handshake
            # and would hold shutdown for the whole close timeout.
            for connection in self._connections:
                connection.resume_reading()
            server.close()
            await server.wait_closed()

    async def wait_for_connection(
        self, index: int = 0, *, timeout_s: float = DEFAULT_WAIT_S
    ) -> FakeConnection:
        """Wait until connection ``index`` has been accepted and return it.

        Raises:
            TimeoutError: If no such connection arrives in time.
        """
        async with asyncio.timeout(timeout_s):
            while len(self._connections) <= index:
                await self._accepted.wait()
                self._accepted.clear()
        return self._connections[index]

    async def __aenter__(self) -> Self:
        """Start the server and return it."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop the server, whatever happened inside the block."""
        await self.stop()

    async def _serve_connection(self, connection: ServerConnection) -> None:
        fake = FakeConnection(connection, books=self._books)
        self._connections.append(fake)
        self._accepted.set()
        try:
            async for message in connection:
                await fake.handle(message)
        except ConnectionClosed:
            pass
