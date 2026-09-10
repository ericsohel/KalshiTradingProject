"""Market-data event structs shared by the recorder, bus, API, and engine.

These are the typed, fixed-point form of what Kalshi sends, plus what only the recorder
knows: its periodic image of a live book (:class:`BookRefresh`, ADR 0022), the markets it
records (:class:`MarketCatalog`), and its health (:class:`StatusReport`, ADR 0023). They are
frozen and tagged for encoding on the bus; the market events carry both the exchange
timestamp and the local receipt.
Private events (fills, order updates, acknowledgements, timers) live in
``tape.engine`` because only the engine consumes them.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Final

import msgspec

from tape.fixedpoint import CountE2, PriceE4
from tape.timeutil import Ms, Ns

__all__ = [
    "MARKET_EVENT_TYPES",
    "BookDelta",
    "BookRefresh",
    "BookSnapshot",
    "BusEvent",
    "CatalogEntry",
    "ConnectionReport",
    "GapEvent",
    "Level",
    "Lifecycle",
    "MarketCatalog",
    "MarketEvent",
    "Receipt",
    "Side",
    "StatusReport",
    "Ticker",
    "Trade",
]


class Side(IntEnum):
    """Side of the consolidated YES-space book."""

    BID = 0
    ASK = 1


class Receipt(msgspec.Struct, frozen=True, kw_only=True):
    """Where and when a frame was received locally."""

    conn_id: int
    recv_mono_ns: Ns
    recv_wall_ns: Ns


class Level(msgspec.Struct, frozen=True, array_like=True):
    """One price level: ``price`` in ``PriceE4`` and resting ``count`` in ``CountE2``."""

    price: PriceE4
    count: CountE2


class BookSnapshot(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """A complete book image in YES space. Replaces all prior levels."""

    ticker: str
    ts_ms: Ms | None
    receipt: Receipt
    sid: int
    seq: int | None
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


class BookDelta(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """A signed change to one level on one side, in YES space."""

    ticker: str
    ts_ms: Ms | None
    receipt: Receipt
    sid: int
    seq: int | None
    side: Side
    price: PriceE4
    delta: int
    own_client_order_id: str | None = None


class Trade(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """A public trade print. ``taker_side`` is the taker's book side."""

    ticker: str
    ts_ms: Ms
    receipt: Receipt
    sid: int
    seq: int | None
    trade_id: str
    price: PriceE4
    count: CountE2
    taker_side: Side
    is_block: bool


class Ticker(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """Top-of-book and volume summary for one market."""

    ticker: str
    ts_ms: Ms
    receipt: Receipt
    sid: int
    last: PriceE4 | None
    bid: PriceE4 | None
    ask: PriceE4 | None
    bid_size: CountE2 | None
    ask_size: CountE2 | None
    volume: CountE2
    open_interest: CountE2


class Lifecycle(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """A market lifecycle transition with the raw payload preserved."""

    ticker: str
    receipt: Receipt
    sid: int
    seq: int | None
    event_type: str
    payload_json: str


class BookRefresh(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """The recorder's image of one live book, published on the bus (ADR 0022).

    Unlike a snapshot it comes from the recorder, not the exchange: it is the book as the
    recorder holds it between two frames, so it reflects exactly the bus messages published
    before it. It carries no ``sid`` or ``seq`` because it answers no subscription.

    Attributes:
        ticker: Market ticker.
        ts_ms: Exchange time of the last snapshot or delta applied to the book, if known.
        receipt: The connection holding the book, and when the image was taken.
        stale: Whether the book awaited a snapshot when the image was taken.
        bids: YES bids, highest price first.
        asks: YES asks, lowest price first.
    """

    ticker: str
    ts_ms: Ms | None
    receipt: Receipt
    stale: bool
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


class GapEvent(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """A sequence discontinuity on one subscription."""

    receipt: Receipt
    sid: int
    expected_seq: int
    got_seq: int


class CatalogEntry(msgspec.Struct, frozen=True, kw_only=True):
    """One recorded market, as the recorder's latest universe decision describes it.

    Attributes:
        ticker: Market ticker.
        series_ticker: Series the market belongs to.
        event_ticker: Event the market belongs to.
        volume_24h: Contracts traded in the 24 hours before the listing the decision read.
        close_ts: Unix seconds at which the market closes, or ``None`` when unknown.
        showcase: Whether the market is recorded because its series is a showcase series.
    """

    ticker: str
    series_ticker: str
    event_ticker: str
    volume_24h: CountE2
    close_ts: int | None
    showcase: bool


class MarketCatalog(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """Every market the recorder records, published once per bus refresh cycle (ADR 0023).

    Each catalog replaces the previous one whole, so a consumer that has just started, or that
    lost messages, knows the recorded markets again within one cycle.

    Attributes:
        markets: One entry per market of the latest universe decision, in ticker order.
    """

    markets: tuple[CatalogEntry, ...]


class ConnectionReport(msgspec.Struct, frozen=True, kw_only=True):
    """One connection's counters in a :class:`StatusReport`.

    Attributes:
        conn_id: Connection id.
        taped: Whether the connection writes to the tape.
        frames: Frames received since the recorder started.
        gaps: Sequence gaps observed since start.
        reconnects: Reconnections since start.
        stale_books: Books awaiting a snapshot now.
        sink_dropped: Records that never reached a segment; zero on a live-only connection.
    """

    conn_id: int
    taped: bool
    frames: int
    gaps: int
    reconnects: int
    stale_books: int
    sink_dropped: int


class StatusReport(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """The recorder's health, published every status interval (ADR 0023).

    Attributes:
        interval_s: Seconds between two reports, so a consumer can tell a recorder that stopped
            reporting from one that reports rarely without sharing its configuration.
        universe_size: Markets the latest universe decision chose.
        subscribed_markets: Markets of book connections whose subscriptions are live now.
        connections: Every connection, by ascending id.
    """

    interval_s: int
    universe_size: int
    subscribed_markets: int
    connections: tuple[ConnectionReport, ...]


MarketEvent = BookSnapshot | BookDelta | Trade | Ticker | Lifecycle | GapEvent | BookRefresh
"""Union of every event about market data: what the exchange sent, and refresh images."""

MARKET_EVENT_TYPES: Final = (
    BookSnapshot,
    BookDelta,
    Trade,
    Ticker,
    Lifecycle,
    GapEvent,
    BookRefresh,
)
"""The members of :data:`MarketEvent`, for ``isinstance`` checks."""

BusEvent = MarketEvent | MarketCatalog | StatusReport
"""Union of every event published on the bus, on the ``md.`` and ``ctl.`` topics."""
