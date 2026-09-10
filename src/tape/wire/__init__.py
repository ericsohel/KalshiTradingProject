"""Typed structs for Kalshi wire payloads and exact conversion into events.

``tape.wire.ws`` and ``tape.wire.rest`` mirror the pinned specifications in ``specs/``
field for field, keeping Kalshi's names and string encodings. ``tape.wire.convert``
turns them into the fixed-point events in ``tape.events``. Unknown fields are ignored
on decode so that additive API changes never break capture (docs/DATA_FORMATS.md 9).
"""

from tape.wire.convert import (
    to_book_delta,
    to_book_snapshot,
    to_lifecycle,
    to_ticker,
    to_trade,
)
from tape.wire.ws import (
    Envelope,
    ErrorMsg,
    FillMsg,
    MarketLifecycleV2Msg,
    OrderbookDeltaMsg,
    OrderbookSnapshotMsg,
    SubscribedMsg,
    TickerMsg,
    TradeMsg,
    UserOrderMsg,
    decode_envelope,
    decode_msg,
)

__all__ = [
    "Envelope",
    "ErrorMsg",
    "FillMsg",
    "MarketLifecycleV2Msg",
    "OrderbookDeltaMsg",
    "OrderbookSnapshotMsg",
    "SubscribedMsg",
    "TickerMsg",
    "TradeMsg",
    "UserOrderMsg",
    "decode_envelope",
    "decode_msg",
    "to_book_delta",
    "to_book_snapshot",
    "to_lifecycle",
    "to_ticker",
    "to_trade",
]
