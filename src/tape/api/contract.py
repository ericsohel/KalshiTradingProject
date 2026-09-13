"""The live API's public types: every REST body and WebSocket message (docs/FRONTEND.md 4).

Responsibility: define, as msgspec structs, exactly what ``tape serve`` sends and accepts, and the
numbers the contract fixes, so that ``scripts/gen_api_schema.py`` can publish one JSON Schema from
which the front end's types are generated (ADR 0023). Nothing here performs I/O or knows where a
value comes from.

Conventions follow docs/DATA_FORMATS.md 1: prices are ``price_e4``, counts ``count_e2``, exchange
times ``ts_ms``, close times Unix seconds, every price a YES price, and ``null`` means none is
known. A price level is the array ``[price_e4, count_e2]``. Server messages are tagged by ``t`` and
client messages by ``op``.

Invariants: a type here embeds no internal type, so an internal change cannot alter the contract
unnoticed; every change here is a change to docs/FRONTEND.md 4 and to the committed schema.
"""

from __future__ import annotations

from typing import Final, Literal

import msgspec

from tape.fixedpoint import CountE2, PriceE4

__all__ = [
    "CLOSE_GOING_AWAY",
    "CLOSE_POLICY_VIOLATION",
    "CLOSE_TOO_SLOW",
    "CLOSE_TRY_AGAIN_LATER",
    "DEFAULT_MARKETS_LIMIT",
    "DEPTH_LEVELS",
    "ERROR_BAD_REQUEST",
    "ERROR_INTERNAL",
    "ERROR_INVALID_MESSAGE",
    "ERROR_MALFORMED_JSON",
    "ERROR_METHOD_NOT_ALLOWED",
    "ERROR_NOT_FOUND",
    "ERROR_UNKNOWN_OP",
    "ERROR_UNKNOWN_TICKER",
    "LAG_LIMIT",
    "LAG_WINDOW_S",
    "MAX_CLIENT_MESSAGES_PER_S",
    "MAX_CLIENT_MESSAGE_BYTES",
    "MAX_MARKETS_LIMIT",
    "OP_SUBSCRIBE",
    "PROTOCOL_VERSION",
    "REJECT_TOO_MANY_TICKERS",
    "REJECT_UNKNOWN_TICKER",
    "RESYNC_BUS_LOSS",
    "RESYNC_CLIENT_LAG",
    "BookMessage",
    "BookSide",
    "BookState",
    "BusHealth",
    "ClientMessage",
    "ConnectionHealth",
    "DeltaMessage",
    "Depth",
    "ErrorBody",
    "ErrorMessage",
    "ErrorResponse",
    "HelloMessage",
    "KnownBookState",
    "MarketDetail",
    "MarketRow",
    "MarketsResponse",
    "PriceLevel",
    "PriceRange",
    "RecorderHealth",
    "Rejection",
    "RejectionCode",
    "ResyncMessage",
    "ResyncReason",
    "ServerMessage",
    "ServiceStatus",
    "SnapshotMessage",
    "SubscribeRequest",
    "SubscribedMessage",
    "TickerMessage",
    "TradeMessage",
]

PROTOCOL_VERSION: Final = 1
"""Version of the live feed's messages, announced in ``hello``."""

DEFAULT_MARKETS_LIMIT: Final = 50
MAX_MARKETS_LIMIT: Final = 200
"""``GET /markets`` returns ``limit`` rows, from 1 to this many, and 50 by default."""

DEPTH_LEVELS: Final = 20
"""Levels per side in a market's ``depth``, best first."""

MAX_CLIENT_MESSAGE_BYTES: Final = 4096
"""Largest client message, in UTF-8 bytes; a larger one closes the connection with 1008."""

MAX_CLIENT_MESSAGES_PER_S: Final = 10
"""Most client messages in any one second; one more closes the connection with 1008."""

LAG_LIMIT: Final = 3
LAG_WINDOW_S: Final = 60
"""A connection that lags :data:`LAG_LIMIT` times within this many seconds is closed with 4000."""

CLOSE_GOING_AWAY: Final = 1001
"""The server is shutting down."""

CLOSE_POLICY_VIOLATION: Final = 1008
"""A client message was too large or came too soon."""

CLOSE_TRY_AGAIN_LATER: Final = 1013
"""Every live connection the server allows is open."""

CLOSE_TOO_SLOW: Final = 4000
"""The client lagged too often to be kept."""

ERROR_BAD_REQUEST: Final = "bad_request"
ERROR_INTERNAL: Final = "internal_error"
ERROR_METHOD_NOT_ALLOWED: Final = "method_not_allowed"
ERROR_NOT_FOUND: Final = "not_found"
ERROR_UNKNOWN_TICKER: Final = "unknown_ticker"
"""Error codes of REST responses."""

ERROR_MALFORMED_JSON: Final = "malformed_json"
ERROR_UNKNOWN_OP: Final = "unknown_op"
ERROR_INVALID_MESSAGE: Final = "invalid_message"
"""Error codes of ``error`` messages on the live feed."""

REJECT_UNKNOWN_TICKER: Final = "unknown_ticker"
REJECT_TOO_MANY_TICKERS: Final = "too_many_tickers"
RESYNC_CLIENT_LAG: Final = "client_lag"
RESYNC_BUS_LOSS: Final = "bus_loss"

OP_SUBSCRIBE: Final = "subscribe"
"""The ``op`` of a subscription request, the only client message."""

BookState = Literal["unknown", "fresh", "stale"]
"""How far the server's copy of a book can be trusted; ``unknown`` means it holds none."""

KnownBookState = Literal["fresh", "stale"]
"""The state of a book the server holds; a stale book receives no deltas until a snapshot."""

BookSide = Literal["bid", "ask"]
"""A side of the YES book; ``bid`` buys YES."""

ResyncReason = Literal["client_lag", "bus_loss"]
RejectionCode = Literal["unknown_ticker", "too_many_tickers"]

PriceLevel = tuple[PriceE4, CountE2]
"""One level, ``[price_e4, count_e2]``."""


# ------------------------------------------------------------------------------ REST


class PriceRange(msgspec.Struct, frozen=True, kw_only=True):
    """One band of a market's price grid.

    Attributes:
        start_e4: Lowest price of the band.
        end_e4: Highest price of the band.
        step_e4: Tick size within the band.
    """

    start_e4: PriceE4
    end_e4: PriceE4
    step_e4: PriceE4


class MarketRow(msgspec.Struct, frozen=True, kw_only=True):
    """One recorded market, as ``GET /markets`` lists it.

    Attributes:
        ticker: Market ticker.
        event_ticker: Event the market belongs to.
        series_ticker: Series the market belongs to.
        title: The event's title, or ``null`` until resolved.
        subtitle: The market's YES subtitle, or ``null`` until resolved.
        category: The series category, or ``null`` until resolved.
        showcase: Whether a series group admitted the market to the recorded universe (ADR 0028).
        volume_24h_e2: Contracts traded in the 24 hours before the recorder's latest listing.
        close_ts: Unix seconds at which the market closes, or ``null`` when unknown.
        bid_e4: Best YES bid from the latest ticker update, or ``null``.
        ask_e4: Best YES ask from the latest ticker update, or ``null``.
        last_e4: Last trade price from the latest ticker update, or ``null``.
        book: How far the server's copy of the book can be trusted.
    """

    ticker: str
    event_ticker: str
    series_ticker: str
    title: str | None
    subtitle: str | None
    category: str | None
    showcase: bool
    volume_24h_e2: CountE2
    close_ts: int | None
    bid_e4: PriceE4 | None
    ask_e4: PriceE4 | None
    last_e4: PriceE4 | None
    book: BookState


class Depth(msgspec.Struct, frozen=True, kw_only=True):
    """The top of a book the server holds.

    Attributes:
        ts_ms: Exchange time of the last change applied to the book, or ``null`` when unknown.
        bids: Up to :data:`DEPTH_LEVELS` YES bids, highest first.
        asks: Up to :data:`DEPTH_LEVELS` YES asks, lowest first.
    """

    ts_ms: int | None
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]


class MarketDetail(MarketRow, frozen=True, kw_only=True):
    """One recorded market with its price grid and depth, from ``GET /markets/{ticker}``.

    Attributes:
        price_ranges: The market's price grid, or ``null`` until resolved.
        depth: The best levels per side, or ``null`` unless the book is known.
    """

    price_ranges: tuple[PriceRange, ...] | None
    depth: Depth | None


class MarketsResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /markets``: recorded markets by 24-hour volume, highest first, then by ticker.

    Attributes:
        markets: At most ``limit`` rows.
    """

    markets: tuple[MarketRow, ...]


class ConnectionHealth(msgspec.Struct, frozen=True, kw_only=True):
    """One recorder connection's counters.

    Attributes:
        conn_id: Connection id.
        taped: Whether the connection writes to the tape.
        frames: Frames received since the recorder started.
        gaps: Sequence gaps observed since start.
        reconnects: Reconnections since start.
        stale_books: Books awaiting a snapshot now.
        sink_dropped: Records that never reached the tape.
    """

    conn_id: int
    taped: bool
    frames: int
    gaps: int
    reconnects: int
    stale_books: int
    sink_dropped: int


class RecorderHealth(msgspec.Struct, frozen=True, kw_only=True):
    """The recorder's latest status report.

    Attributes:
        universe_size: Markets the recorder chose to record.
        subscribed_markets: Markets whose order-book subscriptions are live.
        connections: Every connection, by ascending id.
    """

    universe_size: int
    subscribed_markets: int
    connections: tuple[ConnectionHealth, ...]


class BusHealth(msgspec.Struct, frozen=True, kw_only=True):
    """How the server follows the recorder's bus.

    Attributes:
        epoch: The recorder run the latest message came from, or ``null`` before any: its
            ``bus_epoch``, a wall-clock nanosecond count, as a decimal string. It identifies a run
            and exceeds what a JavaScript number holds exactly, so clients only compare it.
        last_seq: Number of the latest message, or ``null`` before any.
        messages: Messages received.
        resets: Times every book was dropped: at the first message, a recorder restart, or loss.
        missed: Messages known to be lost.
        books_known: Books the server holds now.
    """

    epoch: str | None
    last_seq: int | None
    messages: int
    resets: int
    missed: int
    books_known: int


class ServiceStatus(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /status``.

    Attributes:
        recording: Whether a recorder status report arrived within two of its intervals.
        recorder_status_age_ms: Milliseconds since that report arrived, or ``null`` before any.
        recorder: The latest report, or ``null`` before any.
        bus: The server's view of the bus.
        clients: Open live connections.
    """

    recording: bool
    recorder_status_age_ms: int | None
    recorder: RecorderHealth | None
    bus: BusHealth
    clients: int


class ErrorBody(msgspec.Struct, frozen=True, kw_only=True):
    """What went wrong.

    Attributes:
        code: A stable, machine-readable code such as ``unknown_ticker``.
        message: A human-readable explanation.
    """

    code: str
    message: str


class ErrorResponse(msgspec.Struct, frozen=True, kw_only=True):
    """The body of every REST error.

    Attributes:
        error: The error.
    """

    error: ErrorBody


# ----------------------------------------------------------------- live feed, server


class HelloMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="hello"):
    """The first message on every connection.

    Attributes:
        protocol: :data:`PROTOCOL_VERSION`.
        max_tickers: Most markets one subscription may name.
        bus_refresh_s: Longest wait for a snapshot after ``resync`` with ``bus_loss``.
    """

    protocol: int
    max_tickers: int
    bus_refresh_s: int


class Rejection(msgspec.Struct, frozen=True, kw_only=True):
    """A ticker a subscription could not include.

    Attributes:
        ticker: The ticker as the client sent it.
        code: ``unknown_ticker`` when the market is not recorded, ``too_many_tickers`` beyond
            ``max_tickers``.
    """

    ticker: str
    code: RejectionCode


class SubscribedMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="subscribed"):
    """The reply to each ``subscribe``.

    Attributes:
        tickers: The new subscription set, in the order the client named the markets.
        rejected: The tickers left out, and why.
    """

    tickers: tuple[str, ...]
    rejected: tuple[Rejection, ...]


class SnapshotMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="snapshot"):
    """A market's whole book, replacing whatever the client holds.

    Attributes:
        ticker: Market ticker.
        book: Whether the book is fresh or stale.
        ts_ms: Exchange time of the last change applied to the book, or ``null`` when unknown.
        bids: Every YES bid, highest first.
        asks: Every YES ask, lowest first.
    """

    ticker: str
    book: KnownBookState
    ts_ms: int | None
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]


class DeltaMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="delta"):
    """A signed change at one level, applied to the latest snapshot.

    Attributes:
        ticker: Market ticker.
        ts_ms: Exchange time of the change, or ``null`` when unknown.
        side: The side of the level.
        price_e4: The level's price.
        delta_e2: Signed change in the level's resting count; the level is removed at zero.
    """

    ticker: str
    ts_ms: int | None
    side: BookSide
    price_e4: PriceE4
    delta_e2: int


class BookMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="book"):
    """A book's freshness changed without a snapshot.

    Attributes:
        ticker: Market ticker.
        book: The new state; a stale book receives no deltas until its next snapshot.
    """

    ticker: str
    book: KnownBookState


class ResyncMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="resync"):
    """Discard this market's book; a snapshot follows once the server's book is known.

    Attributes:
        ticker: Market ticker.
        reason: ``client_lag`` when this connection fell behind, and the snapshot follows at
            once; ``bus_loss`` when the server lost its book, and the snapshot follows within
            ``bus_refresh_s``.
    """

    ticker: str
    reason: ResyncReason


class TradeMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="trade"):
    """A public trade.

    Attributes:
        ticker: Market ticker.
        ts_ms: Exchange time of the trade.
        price_e4: YES price of the trade.
        count_e2: Contracts traded.
        taker_side: ``bid`` when the taker bought YES.
    """

    ticker: str
    ts_ms: int
    price_e4: PriceE4
    count_e2: CountE2
    taker_side: BookSide


class TickerMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="ticker"):
    """Top of book and cumulative volume.

    Attributes:
        ticker: Market ticker.
        ts_ms: Exchange time of the update.
        bid_e4: Best YES bid, or ``null``.
        ask_e4: Best YES ask, or ``null``.
        last_e4: Last trade price, or ``null``.
        volume_e2: Contracts traded since the market opened.
    """

    ticker: str
    ts_ms: int
    bid_e4: PriceE4 | None
    ask_e4: PriceE4 | None
    last_e4: PriceE4 | None
    volume_e2: CountE2


class ErrorMessage(msgspec.Struct, frozen=True, kw_only=True, tag_field="t", tag="error"):
    """A client message the server could not accept; the connection stays open.

    Attributes:
        code: ``malformed_json``, ``unknown_op``, or ``invalid_message``.
        message: A human-readable explanation.
    """

    code: str
    message: str


ServerMessage = (
    HelloMessage
    | SubscribedMessage
    | SnapshotMessage
    | DeltaMessage
    | BookMessage
    | ResyncMessage
    | TradeMessage
    | TickerMessage
    | ErrorMessage
)
"""Every message the server sends on the live feed."""


# ----------------------------------------------------------------- live feed, client


class SubscribeRequest(msgspec.Struct, frozen=True, kw_only=True, tag_field="op", tag=OP_SUBSCRIBE):
    """Replace the subscription set; an empty list unsubscribes from everything.

    Attributes:
        tickers: The markets to follow.
    """

    tickers: tuple[str, ...]


ClientMessage = SubscribeRequest
"""Every message a client may send on the live feed."""
