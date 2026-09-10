"""Market-data event structs shared by the recorder, bus, API, and engine.

These are the typed, fixed-point form of what Kalshi sends. They are frozen, tagged
for encoding on the bus, and carry both the exchange timestamp and the local receipt.
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
    "BookDelta",
    "BookSnapshot",
    "GapEvent",
    "Level",
    "Lifecycle",
    "MarketEvent",
    "Receipt",
    "Side",
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


class GapEvent(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """A sequence discontinuity on one subscription."""

    receipt: Receipt
    sid: int
    expected_seq: int
    got_seq: int


MarketEvent = BookSnapshot | BookDelta | Trade | Ticker | Lifecycle | GapEvent
"""Union of every event published on the ``md.`` and ``ctl.`` bus topics."""

MARKET_EVENT_TYPES: Final = (BookSnapshot, BookDelta, Trade, Ticker, Lifecycle, GapEvent)
