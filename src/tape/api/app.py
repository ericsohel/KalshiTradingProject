"""The live API's routes on Starlette (docs/FRONTEND.md 4.1 and 4.2, ADR 0023).

Responsibility: map ``GET /api/v1/markets``, ``GET /api/v1/markets/{ticker}``,
``GET /api/v1/status``, and ``WS /api/v1/live`` onto the hub, the directory, and the metadata
resolver, and nothing more: no route calls Kalshi, and every body is a contract struct encoded by
msgspec. A market list or detail asks the resolver for the metadata its markets still need and
answers at once with whatever is resolved.

Access. CORS answers only the configured origins, for ``GET``. A WebSocket handshake whose
``Origin`` is not one of them is refused with 403, and one beyond ``max_clients`` is accepted and
closed at once with 1013.

Invariants: every HTTP response, errors included, is JSON with ``Cache-Control: no-store``; every
error body is ``{"error": {"code", "message"}}``; and a live session is attached to the hub for
exactly as long as its handler runs.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Mapping
from typing import Final

import msgspec
from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from tape.api.contract import (
    CLOSE_TRY_AGAIN_LATER,
    DEFAULT_MARKETS_LIMIT,
    ERROR_BAD_REQUEST,
    ERROR_INTERNAL,
    ERROR_METHOD_NOT_ALLOWED,
    ERROR_NOT_FOUND,
    ERROR_UNKNOWN_TICKER,
    MAX_MARKETS_LIMIT,
    ErrorBody,
    ErrorResponse,
    MarketsResponse,
)
from tape.api.hub import LiveHub
from tape.api.metadata import MetadataResolver
from tape.timeutil import Clock

__all__ = ["API_PREFIX", "create_app"]

API_PREFIX: Final = "/api/v1"
"""Every route is under this path; the version changes only with an incompatible contract."""

_HTTP_OK: Final = 200
_HTTP_BAD_REQUEST: Final = 400
_HTTP_NOT_FOUND: Final = 404
_HTTP_METHOD_NOT_ALLOWED: Final = 405
_HTTP_INTERNAL_ERROR: Final = 500
_JSON: Final = "application/json"
_NO_STORE: Final = {"Cache-Control": "no-store"}
_LIMIT: Final = re.compile(r"[0-9]{1,4}")

_encoder: Final = msgspec.json.Encoder()


def create_app(*, hub: LiveHub, resolver: MetadataResolver, clock: Clock) -> Starlette:
    """Build the live API.

    Args:
        hub: The bus follower; its directory and configuration serve every route.
        resolver: Resolves and looks up market metadata.
        clock: Time for the recorder status age.

    Returns:
        A Starlette application; the caller runs the hub and the resolver beside it.
    """
    directory = hub.directory
    allowed_origins = hub.config.allowed_origins

    async def markets(request: Request) -> Response:
        text = request.query_params.get("limit")
        limit = DEFAULT_MARKETS_LIMIT if text is None else _parse_limit(text)
        if limit is None:
            return _error(
                _HTTP_BAD_REQUEST,
                ERROR_BAD_REQUEST,
                f"limit must be an integer from 1 to {MAX_MARKETS_LIMIT}, got {text!r}",
            )
        entries = directory.top(limit)
        resolver.request(entries)
        rows = tuple(
            directory.row(entry, metadata=resolver.lookup(entry), book=hub.book(entry.ticker))
            for entry in entries
        )
        return _json(MarketsResponse(markets=rows))

    async def market(request: Request) -> Response:
        ticker: str = request.path_params["ticker"]
        entry = directory.entry(ticker)
        if entry is None:
            return _error(
                _HTTP_NOT_FOUND, ERROR_UNKNOWN_TICKER, f"market {ticker!r} is not recorded"
            )
        resolver.request((entry,))
        return _json(
            directory.detail(entry, metadata=resolver.lookup(entry), book=hub.book(ticker))
        )

    async def status(request: Request) -> Response:
        _ = request
        return _json(
            directory.service_status(
                now_mono_ns=int(clock.mono_ns()), bus=hub.bus_health(), clients=hub.clients
            )
        )

    async def live(websocket: WebSocket) -> None:
        if websocket.headers.get("origin") not in allowed_origins:
            # Closing before accepting answers the handshake with 403; a browser shows a refused
            # handshake no body, so none is sent.
            await websocket.close()
            return
        session = hub.admit(_StarletteSocket(websocket))
        if session is None:
            await websocket.accept()
            await websocket.close(code=CLOSE_TRY_AGAIN_LATER)
            return
        try:
            await websocket.accept()
            await session.run()
        finally:
            hub.detach(session)

    return Starlette(
        routes=[
            Route(f"{API_PREFIX}/markets", markets, methods=["GET"]),
            Route(f"{API_PREFIX}/markets/{{ticker}}", market, methods=["GET"]),
            Route(f"{API_PREFIX}/status", status, methods=["GET"]),
            WebSocketRoute(f"{API_PREFIX}/live", live),
        ],
        middleware=[
            Middleware(CORSMiddleware, allow_origins=sorted(allowed_origins), allow_methods=["GET"])
        ],
        exception_handlers={HTTPException: _http_error, Exception: _internal_error},
    )


def _parse_limit(text: str) -> int | None:
    """The ``limit`` a query names, or ``None`` when it is not an integer in range."""
    if not _LIMIT.fullmatch(text):
        return None
    limit = int(text)
    return limit if 1 <= limit <= MAX_MARKETS_LIMIT else None


def _json(
    body: msgspec.Struct,
    status_code: int = _HTTP_OK,
    headers: Mapping[str, str] | None = None,
) -> Response:
    return Response(
        _encoder.encode(body),
        status_code=status_code,
        media_type=_JSON,
        headers={**(headers or {}), **_NO_STORE},
    )


def _error(
    status_code: int, code: str, message: str, headers: Mapping[str, str] | None = None
) -> Response:
    return _json(ErrorResponse(error=ErrorBody(code=code, message=message)), status_code, headers)


async def _http_error(request: Request, exc: Exception) -> Response:
    """Render a routing error, such as an unknown path, as a contract error body."""
    _ = request
    if not isinstance(exc, HTTPException):
        return await _internal_error(request, exc)
    codes = {_HTTP_NOT_FOUND: ERROR_NOT_FOUND, _HTTP_METHOD_NOT_ALLOWED: ERROR_METHOD_NOT_ALLOWED}
    code = codes.get(exc.status_code, ERROR_BAD_REQUEST)
    # The headers keep, for example, a 405's Allow.
    return _error(exc.status_code, code, exc.detail, exc.headers)


async def _internal_error(request: Request, exc: Exception) -> Response:
    """Render an unexpected failure; the server logs the exception itself."""
    _ = (request, exc)
    return _error(_HTTP_INTERNAL_ERROR, ERROR_INTERNAL, "internal error")


class _StarletteSocket:
    """A Starlette WebSocket as a :class:`tape.api.session.LiveSocket`."""

    def __init__(self, websocket: WebSocket) -> None:
        self._websocket = websocket

    async def receive(self) -> str | bytes | None:
        message = await self._websocket.receive()
        if message["type"] == "websocket.disconnect":
            return None
        text: str | None = message.get("text")
        if text is not None:
            return text
        data: bytes | None = message.get("bytes")
        return b"" if data is None else data

    async def send(self, text: str) -> bool:
        if self._websocket.application_state is not WebSocketState.CONNECTED:
            return False
        try:
            await self._websocket.send_text(text)
        except WebSocketDisconnect:
            return False
        return True

    async def close(self, code: int) -> None:
        if self._websocket.application_state is not WebSocketState.CONNECTED:
            return
        with contextlib.suppress(WebSocketDisconnect):
            await self._websocket.close(code=code)
