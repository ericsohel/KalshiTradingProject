"""Exact conversion from wire structs to fixed-point events.

This is the boundary where strings become integers (ADR 0002) and where the NO side of
the book is placed on the YES price scale (ADR 0006).
"""

from __future__ import annotations

from collections.abc import Iterable

import msgspec

from tape.errors import WireError
from tape.events import (
    BookDelta,
    BookSnapshot,
    Level,
    Lifecycle,
    Receipt,
    Side,
    Ticker,
    Trade,
)
from tape.fixedpoint import (
    CountE2,
    PriceE4,
    complement,
    parse_count,
    parse_price,
    parse_signed_count,
)
from tape.timeutil import Ms
from tape.wire.rest import OrderbookCountFp
from tape.wire.ws import (
    Envelope,
    MarketLifecycleV2Msg,
    OrderbookDeltaMsg,
    OrderbookSnapshotMsg,
    TickerMsg,
    TradeMsg,
)

__all__ = [
    "rest_orderbook_levels",
    "to_book_delta",
    "to_book_snapshot",
    "to_lifecycle",
    "to_ticker",
    "to_trade",
]


def _levels(pairs: Iterable[tuple[str, str]] | None, *, to_yes: bool) -> tuple[Level, ...]:
    """Parse ``[price, count]`` string pairs, dropping zero-count entries.

    Args:
        pairs: The raw side, or ``None`` when Kalshi omitted an empty side.
        to_yes: When true, prices are NO-leg and are complemented onto the YES scale.
    """
    if not pairs:
        return ()
    out: list[Level] = []
    for price_text, count_text in pairs:
        price = parse_price(price_text)
        count = parse_count(count_text)
        if count == 0:
            continue
        out.append(Level(complement(price) if to_yes else price, count))
    return tuple(out)


def rest_orderbook_levels(book: OrderbookCountFp) -> tuple[tuple[Level, ...], tuple[Level, ...]]:
    """Convert a REST orderbook into YES-space ``(bids, asks)``.

    Unlike the WebSocket orderbook channel, ``GET /markets/orderbooks`` and
    ``GET /markets/{ticker}/orderbook`` carry no ``use_yes_price`` flag: ``yes_dollars``
    are YES bids already on the YES price scale, but ``no_dollars`` are NO bids priced
    on the NO leg, which this always complements onto the YES scale, because a NO bid
    at price ``q`` is a YES ask at ``1 - q`` (docs/DATA_FORMATS.md 1.3, 2.2). Zero-count
    levels are dropped, matching the WebSocket conversion in :func:`to_book_snapshot`.

    Args:
        book: The REST orderbook, still in Kalshi's decimal-string form.

    Returns:
        ``(bids, asks)`` in YES space, ready to compare against a local ``Book``.

    Raises:
        FixedPointError: If a price or count string is malformed.
    """
    bids = _levels(book.yes_dollars, to_yes=False)
    asks = _levels(book.no_dollars, to_yes=True)
    return bids, asks


def to_book_snapshot(
    msg: OrderbookSnapshotMsg,
    envelope: Envelope,
    receipt: Receipt,
    *,
    use_yes_price: bool,
) -> BookSnapshot:
    """Convert an ``orderbook_snapshot`` into a YES-space ``BookSnapshot``.

    Args:
        msg: Decoded payload.
        envelope: The frame envelope, for ``sid`` and ``seq``.
        receipt: Local receipt of the frame.
        use_yes_price: Whether the subscription requested YES-leg pricing for the NO
            side. When false, NO-side prices are complemented.

    Raises:
        WireError: If the envelope carries no ``sid``.
        FixedPointError: If any price or count string is malformed.
    """
    if envelope.sid is None:
        raise WireError("orderbook_snapshot without sid")
    return BookSnapshot(
        ticker=msg.market_ticker,
        ts_ms=None,
        receipt=receipt,
        sid=envelope.sid,
        seq=envelope.seq,
        bids=_levels(msg.yes_dollars_fp, to_yes=False),
        asks=_levels(msg.no_dollars_fp, to_yes=not use_yes_price),
    )


def to_book_delta(
    msg: OrderbookDeltaMsg,
    envelope: Envelope,
    receipt: Receipt,
    *,
    use_yes_price: bool,
) -> BookDelta:
    """Convert an ``orderbook_delta`` into a YES-space ``BookDelta``.

    A ``side: "yes"`` delta touches the bid side at its price. A ``side: "no"`` delta
    touches the ask side, at its price when ``use_yes_price`` is true and at the
    complement otherwise.

    Raises:
        WireError: If the envelope carries no ``sid``.
        FixedPointError: If the price or delta string is malformed.
    """
    if envelope.sid is None:
        raise WireError("orderbook_delta without sid")
    price = parse_price(msg.price_dollars)
    if msg.side == "yes":
        side = Side.BID
    else:
        side = Side.ASK
        if not use_yes_price:
            price = complement(price)
    return BookDelta(
        ticker=msg.market_ticker,
        ts_ms=Ms(msg.ts_ms) if msg.ts_ms is not None else None,
        receipt=receipt,
        sid=envelope.sid,
        seq=envelope.seq,
        side=side,
        price=price,
        delta=parse_signed_count(msg.delta_fp),
        own_client_order_id=msg.client_order_id,
    )


def to_trade(msg: TradeMsg, envelope: Envelope, receipt: Receipt) -> Trade:
    """Convert a ``trade`` payload. Price is the YES-leg price.

    Raises:
        WireError: If the envelope carries no ``sid``.
        FixedPointError: If a price or count string is malformed.
    """
    if envelope.sid is None:
        raise WireError("trade without sid")
    return Trade(
        ticker=msg.market_ticker,
        ts_ms=Ms(msg.ts_ms),
        receipt=receipt,
        sid=envelope.sid,
        seq=envelope.seq,
        trade_id=msg.trade_id,
        price=parse_price(msg.yes_price_dollars),
        count=parse_count(msg.count_fp),
        taker_side=Side.BID if msg.taker_book_side == "bid" else Side.ASK,
        is_block=msg.is_block_trade,
    )


def _opt_price(text: str | None) -> PriceE4 | None:
    return None if text is None else parse_price(text)


def _opt_count(text: str | None) -> CountE2 | None:
    return None if text is None else parse_count(text)


def to_ticker(msg: TickerMsg, envelope: Envelope, receipt: Receipt) -> Ticker:
    """Convert a ``ticker`` payload. Absent prices become ``None``.

    Raises:
        WireError: If the envelope carries no ``sid``.
        FixedPointError: If a present price or count string is malformed.
    """
    if envelope.sid is None:
        raise WireError("ticker without sid")
    return Ticker(
        ticker=msg.market_ticker,
        ts_ms=Ms(msg.ts_ms),
        receipt=receipt,
        sid=envelope.sid,
        last=_opt_price(msg.price_dollars),
        bid=_opt_price(msg.yes_bid_dollars),
        ask=_opt_price(msg.yes_ask_dollars),
        bid_size=_opt_count(msg.yes_bid_size_fp),
        ask_size=_opt_count(msg.yes_ask_size_fp),
        volume=_opt_count(msg.volume_fp) or CountE2(0),
        open_interest=_opt_count(msg.open_interest_fp) or CountE2(0),
    )


def to_lifecycle(msg: MarketLifecycleV2Msg, envelope: Envelope, receipt: Receipt) -> Lifecycle:
    """Convert a ``market_lifecycle_v2`` payload, preserving the raw payload as JSON.

    Raises:
        WireError: If the envelope carries no ``sid``.
    """
    if envelope.sid is None:
        raise WireError("market_lifecycle_v2 without sid")
    return Lifecycle(
        ticker=msg.market_ticker,
        receipt=receipt,
        sid=envelope.sid,
        seq=envelope.seq,
        event_type=msg.event_type,
        payload_json=bytes(envelope.msg).decode("utf-8"),
        close_ts=msg.close_ts,
    )


def raw_json_text(raw: msgspec.Raw) -> str:
    """Return the UTF-8 text of a raw payload (for lifecycle archiving and tests)."""
    return bytes(raw).decode("utf-8")
