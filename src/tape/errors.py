"""Error hierarchy for the package.

Every failure raised by ``tape`` code is a ``TapeError``. Subclasses are specific
enough that callers can decide what to do without inspecting messages. Errors carry
structured context as attributes, never only as formatted text.
"""

from __future__ import annotations

__all__ = [
    "ArchiveError",
    "BookInvariantError",
    "BusError",
    "ConfigError",
    "FixedPointError",
    "KalshiError",
    "KalshiHttpError",
    "KalshiTransportError",
    "RateLimitedError",
    "SequenceGapError",
    "TapeCorruptionError",
    "TapeError",
    "WireError",
    "WsClosedError",
    "WsProtocolError",
]


class TapeError(Exception):
    """Base class for every error raised by this package."""


class FixedPointError(TapeError, ValueError):
    """A string could not be converted exactly into a fixed-point integer."""


class WireError(TapeError, ValueError):
    """A wire payload could not be decoded into its struct."""


class BookInvariantError(TapeError):
    """An order-book operation would violate an invariant.

    Attributes:
        ticker: Market whose book was being updated.
        detail: Human-readable description of the violated invariant.
    """

    def __init__(self, ticker: str, detail: str) -> None:
        super().__init__(f"{ticker}: {detail}")
        self.ticker = ticker
        self.detail = detail


class TapeCorruptionError(TapeError):
    """A raw segment or keyframe file is malformed beyond a truncated tail."""


class ArchiveError(TapeError):
    """The archive cannot safely do what was asked (docs/adr/0025-prune-raw-after-verified-bake.md).

    Raised when an hour is not closed yet, raw segments change while they are baked, an hour
    whose segments were already pruned would be baked again, another bake or prune holds the
    archive, or a manifest that must exist does not.
    """


class ConfigError(TapeError):
    """Configuration is missing, malformed, or inconsistent."""


class BusError(TapeError):
    """The event bus could not be opened or used: a bad endpoint, one in use, or a socket error.

    Publishing never raises it; a failed send is counted instead (ADR 0022).
    """


class KalshiError(TapeError):
    """Base class for errors originating from the exchange or its transport."""


class KalshiHttpError(KalshiError):
    """The REST API answered with an error status.

    Attributes:
        status: HTTP status code.
        code: Kalshi error code from the response body, if any.
        message: Kalshi error message from the response body, if any.
        details: Additional details from the response body, if any.
    """

    def __init__(
        self,
        status: int,
        code: str | None = None,
        message: str | None = None,
        details: str | None = None,
    ) -> None:
        super().__init__(f"HTTP {status}: {code or ''} {message or ''}".strip())
        self.status = status
        self.code = code
        self.message = message
        self.details = details


class RateLimitedError(KalshiHttpError):
    """The REST API answered 429. Kalshi sends no Retry-After header."""

    def __init__(self) -> None:
        super().__init__(429, code="too_many_requests", message="too many requests")


class KalshiTransportError(KalshiError):
    """A network-level failure (DNS, TLS, timeout, connection reset)."""


class WsProtocolError(KalshiError):
    """The WebSocket server answered a command with an error frame.

    Attributes:
        code: Numeric error code from the server (see docs/DATA_FORMATS.md 3.2).
        message: Server-provided message.
        sid: Subscription id the error is scoped to, if any.
    """

    def __init__(self, code: int, message: str, sid: int | None = None) -> None:
        super().__init__(f"ws error {code}: {message}")
        self.code = code
        self.message = message
        self.sid = sid


class WsClosedError(KalshiError):
    """The WebSocket connection closed or went silent past the heartbeat timeout."""


class SequenceGapError(KalshiError):
    """A sequenced channel skipped or repeated a sequence number.

    Attributes:
        sid: Subscription id on which the gap occurred.
        expected: Sequence number that was expected.
        got: Sequence number that arrived.
    """

    def __init__(self, sid: int, expected: int, got: int) -> None:
        super().__init__(f"sid {sid}: expected seq {expected}, got {got}")
        self.sid = sid
        self.expected = expected
        self.got = got
