"""WebSocket message structs, mirroring ``specs/asyncapi.yaml``.

Every inbound frame is ``{"type": ..., "sid"?: ..., "seq"?: ..., "id"?: ..., "msg": ...}``.
``decode_envelope`` reads only the envelope and leaves ``msg`` as raw bytes; that is
the only decoding permitted on the recorder's hot path (docs/INTERFACES.md 3).
``decode_msg`` materializes the payload later.
"""

from __future__ import annotations

from typing import Literal

import msgspec

from tape.errors import WireError

__all__ = [
    "Envelope",
    "ErrorMsg",
    "FillMsg",
    "MarketLifecycleV2Msg",
    "OkMsg",
    "OrderbookDeltaMsg",
    "OrderbookSnapshotMsg",
    "PriceRangeMsg",
    "SubscribedMsg",
    "TickerMsg",
    "TradeMsg",
    "UserOrderMsg",
    "decode_envelope",
    "decode_msg",
]

MarketSide = Literal["yes", "no"]
BookSide = Literal["bid", "ask"]


class Envelope(msgspec.Struct, frozen=True, kw_only=True):
    """The outer frame. ``msg`` stays undecoded until ``decode_msg`` is called."""

    type: str
    sid: int | None = None
    seq: int | None = None
    id: int | None = None
    msg: msgspec.Raw = msgspec.Raw()


class SubscribedMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of ``type: subscribed``."""

    channel: str
    sid: int


class OkMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of ``type: ok`` after an ``update_subscription``."""

    market_tickers: list[str] | None = None
    market_ids: list[str] | None = None


class ErrorMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of ``type: error``. Codes are listed in docs/DATA_FORMATS.md 3.2."""

    code: int
    msg: str


class OrderbookSnapshotMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of ``orderbook_snapshot``. A side's key is absent when it is empty."""

    market_ticker: str
    market_id: str | None = None
    yes_dollars_fp: list[tuple[str, str]] | None = None
    no_dollars_fp: list[tuple[str, str]] | None = None


class OrderbookDeltaMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of ``orderbook_delta``. ``client_order_id`` appears only for own orders."""

    market_ticker: str
    price_dollars: str
    delta_fp: str
    side: MarketSide
    market_id: str | None = None
    client_order_id: str | None = None
    subaccount: int | None = None
    ts_ms: int | None = None


class TradeMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of ``trade``."""

    trade_id: str
    market_ticker: str
    yes_price_dollars: str
    no_price_dollars: str
    count_fp: str
    taker_book_side: BookSide
    ts_ms: int
    taker_outcome_side: MarketSide | None = None
    is_block_trade: bool = False


class TickerMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of ``ticker``. Best prices are absent when a side is empty."""

    market_ticker: str
    ts_ms: int
    market_id: str | None = None
    price_dollars: str | None = None
    yes_bid_dollars: str | None = None
    yes_ask_dollars: str | None = None
    yes_bid_size_fp: str | None = None
    yes_ask_size_fp: str | None = None
    last_trade_size_fp: str | None = None
    volume_fp: str | None = None
    open_interest_fp: str | None = None
    dollar_volume: int | None = None
    dollar_open_interest: int | None = None


class PriceRangeMsg(msgspec.Struct, frozen=True, kw_only=True):
    """A ``{start, end, step}`` price range in dollar strings."""

    start: str
    end: str
    step: str


class MarketLifecycleV2Msg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of ``market_lifecycle_v2``. Optional keys depend on ``event_type``."""

    event_type: str
    market_ticker: str
    exchange_index: int | None = None
    open_ts: int | None = None
    close_ts: int | None = None
    result: str | None = None
    determination_ts: int | None = None
    settlement_value: str | None = None
    settled_ts: int | None = None
    is_deactivated: bool | None = None
    price_level_structure: str | None = None
    price_ranges: list[PriceRangeMsg] | None = None
    additional_metadata: dict[str, object] | None = None
    strike_type: str | None = None
    floor_strike: float | None = None
    cap_strike: float | None = None
    yes_sub_title: str | None = None


class FillMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of the private ``fill`` channel."""

    trade_id: str
    order_id: str
    market_ticker: str
    exchange_index: int
    is_taker: bool
    yes_price_dollars: str
    count_fp: str
    fee_cost: str
    book_side: BookSide
    ts_ms: int
    post_position_fp: str
    outcome_side: MarketSide | None = None
    client_order_id: str | None = None
    subaccount: int | None = None


class UserOrderMsg(msgspec.Struct, frozen=True, kw_only=True):
    """Payload of the private ``user_orders`` channel."""

    order_id: str
    ticker: str
    exchange_index: int
    status: Literal["resting", "canceled", "executed"]
    book_side: BookSide
    yes_price_dollars: str
    fill_count_fp: str
    remaining_count_fp: str
    initial_count_fp: str
    created_ts_ms: int
    client_order_id: str | None = None
    taker_fees_dollars: str | None = None
    maker_fees_dollars: str | None = None
    expiration_ts_ms: int | None = None
    last_update_reason: str | None = None


_envelope_decoder = msgspec.json.Decoder(Envelope)

_decoders: dict[type[msgspec.Struct], msgspec.json.Decoder[msgspec.Struct]] = {}


def decode_envelope(raw: bytes | str) -> Envelope:
    """Decode only the envelope of a frame, leaving ``msg`` as raw bytes.

    Raises:
        WireError: If the frame is not a JSON object with a string ``type``.
    """
    try:
        return _envelope_decoder.decode(raw)
    except msgspec.DecodeError as exc:
        raise WireError(f"bad envelope: {exc}") from exc
    except msgspec.ValidationError as exc:
        raise WireError(f"bad envelope: {exc}") from exc


def decode_msg[T: msgspec.Struct](envelope: Envelope, struct_type: type[T]) -> T:
    """Decode the ``msg`` payload of an envelope into ``struct_type``.

    Raises:
        WireError: If the payload does not match the struct.
    """
    decoder = _decoders.get(struct_type)
    if decoder is None:
        decoder = msgspec.json.Decoder(struct_type)
        _decoders[struct_type] = decoder
    try:
        decoded = decoder.decode(envelope.msg)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise WireError(f"bad {envelope.type} payload: {exc}") from exc
    if not isinstance(decoded, struct_type):  # pragma: no cover - decoder guarantees type
        raise WireError(f"decoder returned {type(decoded).__name__}")
    return decoded
