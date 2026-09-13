"""Envelope decoding and exact conversion into events."""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest

from tape.errors import FixedPointError, WireError
from tape.events import Receipt, Side
from tape.wire import (
    Envelope,
    ErrorMsg,
    MarketLifecycleV2Msg,
    OrderbookDeltaMsg,
    OrderbookSnapshotMsg,
    TickerMsg,
    TradeMsg,
    decode_envelope,
    decode_msg,
    rest_orderbook_levels,
    to_book_delta,
    to_book_snapshot,
    to_lifecycle,
    to_ticker,
    to_trade,
)
from tape.wire.convert import raw_json_text
from tape.wire.rest import OrderbookCountFp

SNAPSHOT = {
    "type": "orderbook_snapshot",
    "sid": 2,
    "seq": 2,
    "msg": {
        "market_ticker": "FED-23DEC-T3.00",
        "market_id": "9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1",
        "yes_dollars_fp": [["0.0800", "300.00"], ["0.2200", "333.00"]],
        "no_dollars_fp": [["0.5400", "20.00"], ["0.5600", "146.00"]],
    },
}


def test_decode_envelope_leaves_msg_raw() -> None:
    env = decode_envelope(json.dumps(SNAPSHOT))
    assert env.type == "orderbook_snapshot"
    assert env.sid == 2
    assert env.seq == 2
    assert env.id is None
    assert json.loads(bytes(env.msg)) == SNAPSHOT["msg"]


def test_decode_envelope_without_msg() -> None:
    env = decode_envelope(b'{"id": 102, "sid": 2, "seq": 7, "type": "unsubscribed"}')
    assert env.type == "unsubscribed"
    assert bytes(env.msg) == b""


@pytest.mark.parametrize("raw", [b"not json", b"[]", b'{"sid": 1}', b'{"type": 5}'])
def test_decode_envelope_rejects(raw: bytes) -> None:
    with pytest.raises(WireError):
        decode_envelope(raw)


def test_decode_msg_rejects_wrong_shape() -> None:
    env = decode_envelope(b'{"type": "error", "msg": {"code": "x"}}')
    with pytest.raises(WireError):
        decode_msg(env, ErrorMsg)


def test_snapshot_with_yes_pricing(receipt: Receipt) -> None:
    env = decode_envelope(json.dumps(SNAPSHOT))
    snap = to_book_snapshot(decode_msg(env, OrderbookSnapshotMsg), env, receipt, use_yes_price=True)
    assert [(lvl.price, lvl.count) for lvl in snap.bids] == [(800, 30_000), (2200, 33_300)]
    assert [(lvl.price, lvl.count) for lvl in snap.asks] == [(5400, 2_000), (5600, 14_600)]
    assert snap.sid == 2
    assert snap.receipt == receipt


def test_snapshot_with_no_pricing_complements_the_ask_side(receipt: Receipt) -> None:
    env = decode_envelope(json.dumps(SNAPSHOT))
    snap = to_book_snapshot(
        decode_msg(env, OrderbookSnapshotMsg), env, receipt, use_yes_price=False
    )
    assert [lvl.price for lvl in snap.asks] == [4600, 4400]


def test_snapshot_drops_zero_levels_and_missing_sides(receipt: Receipt) -> None:
    payload = {
        "type": "orderbook_snapshot",
        "sid": 1,
        "seq": 1,
        "msg": {"market_ticker": "X", "market_id": "m", "yes_dollars_fp": [["0.10", "0.00"]]},
    }
    env = decode_envelope(json.dumps(payload))
    snap = to_book_snapshot(decode_msg(env, OrderbookSnapshotMsg), env, receipt, use_yes_price=True)
    assert snap.bids == ()
    assert snap.asks == ()


def test_delta_sides_and_pricing(receipt: Receipt) -> None:
    payload = {
        "type": "orderbook_delta",
        "sid": 2,
        "seq": 3,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "market_id": "m",
            "price_dollars": "0.960",
            "delta_fp": "-54.00",
            "side": "no",
            "ts_ms": 1669149841000,
            "client_order_id": "mine",
        },
    }
    env = decode_envelope(json.dumps(payload))
    msg = decode_msg(env, OrderbookDeltaMsg)
    yes_priced = to_book_delta(msg, env, receipt, use_yes_price=True)
    assert yes_priced.side is Side.ASK
    assert yes_priced.price == 9600
    assert yes_priced.delta == -5400
    assert yes_priced.own_client_order_id == "mine"
    assert yes_priced.ts_ms == 1669149841000
    no_priced = to_book_delta(msg, env, receipt, use_yes_price=False)
    assert no_priced.price == 400
    bid_msg = OrderbookDeltaMsg(market_ticker="X", price_dollars="0.5", delta_fp="1", side="yes")
    assert to_book_delta(bid_msg, env, receipt, use_yes_price=False).side is Side.BID


def test_conversions_require_sid(receipt: Receipt) -> None:
    env = decode_envelope(b'{"type": "trade", "msg": {}}')
    with pytest.raises(WireError):
        to_book_delta(
            OrderbookDeltaMsg(market_ticker="X", price_dollars="0.5", delta_fp="1", side="yes"),
            env,
            receipt,
            use_yes_price=True,
        )


def test_bad_price_string_raises_fixed_point_error(receipt: Receipt) -> None:
    env = decode_envelope(b'{"type": "orderbook_delta", "sid": 1, "seq": 1, "msg": {}}')
    msg = OrderbookDeltaMsg(market_ticker="X", price_dollars="1.5", delta_fp="1", side="yes")
    with pytest.raises(FixedPointError):
        to_book_delta(msg, env, receipt, use_yes_price=True)


def test_trade(receipt: Receipt) -> None:
    payload = {
        "type": "trade",
        "sid": 11,
        "seq": 2,
        "msg": {
            "trade_id": "d91bc706",
            "market_ticker": "HIGHNY-22DEC23-B53.5",
            "yes_price_dollars": "0.3600",
            "no_price_dollars": "0.6400",
            "count_fp": "136.00",
            "taker_side": "no",
            "taker_outcome_side": "no",
            "taker_book_side": "ask",
            "is_block_trade": False,
            "ts": 1669149841,
            "ts_ms": 1669149841000,
        },
    }
    env = decode_envelope(json.dumps(payload))
    trade = to_trade(decode_msg(env, TradeMsg), env, receipt)
    assert trade.price == 3600
    assert trade.count == 13_600
    assert trade.taker_side is Side.ASK
    assert trade.is_block is False


def test_ticker_with_missing_prices(receipt: Receipt) -> None:
    payload = {
        "type": "ticker",
        "sid": 11,
        "msg": {
            "market_ticker": "FED-23DEC-T3.00",
            "market_id": "m",
            "price_dollars": "0.480",
            "yes_bid_dollars": "0.450",
            "volume_fp": "33896.00",
            "open_interest_fp": "20422.00",
            "dollar_volume": 16948,
            "dollar_open_interest": 10211,
            "yes_bid_size_fp": "300.00",
            "ts_ms": 1669149841000,
        },
    }
    env = decode_envelope(json.dumps(payload))
    ticker = to_ticker(decode_msg(env, TickerMsg), env, receipt)
    assert ticker.last == 4800
    assert ticker.bid == 4500
    assert ticker.ask is None
    assert ticker.ask_size is None
    assert ticker.volume == 3_389_600


def test_lifecycle_preserves_payload(receipt: Receipt) -> None:
    payload = {
        "type": "market_lifecycle_v2",
        "sid": 13,
        "seq": 3,
        "msg": {
            "market_ticker": "INXD-23SEP14-B4487",
            "event_type": "created",
            "exchange_index": 0,
            "open_ts": 1694635200,
            "close_ts": 1694721600,
            "price_level_structure": "linear_cent",
            "additional_metadata": {"strike_type": "greater", "floor_strike": 4487},
        },
    }
    env = decode_envelope(json.dumps(payload))
    msg = decode_msg(env, MarketLifecycleV2Msg)
    assert msg.close_ts == 1694721600
    life = to_lifecycle(msg, env, receipt)
    assert (life.event_type, life.close_ts) == ("created", 1694721600)
    assert json.loads(life.payload_json) == payload["msg"]
    determined = MarketLifecycleV2Msg(event_type="determined", market_ticker="X", result="yes")
    assert to_lifecycle(determined, env, receipt).close_ts is None


@pytest.mark.parametrize(
    "convert",
    [
        lambda env, receipt: to_book_snapshot(
            OrderbookSnapshotMsg(market_ticker="X"), env, receipt, use_yes_price=True
        ),
        lambda env, receipt: to_ticker(TickerMsg(market_ticker="X", ts_ms=1), env, receipt),
        lambda env, receipt: to_lifecycle(
            MarketLifecycleV2Msg(event_type="created", market_ticker="X"), env, receipt
        ),
        lambda env, receipt: to_trade(
            TradeMsg(
                trade_id="t",
                market_ticker="X",
                yes_price_dollars="0.5",
                no_price_dollars="0.5",
                count_fp="1",
                taker_book_side="bid",
                ts_ms=1,
            ),
            env,
            receipt,
        ),
    ],
)
def test_every_conversion_requires_sid(
    receipt: Receipt, convert: Callable[[Envelope, Receipt], object]
) -> None:
    env = decode_envelope(b'{"type": "x", "msg": {}}')
    with pytest.raises(WireError, match="without sid"):
        convert(env, receipt)


def test_raw_json_text_round_trips_bytes() -> None:
    env = decode_envelope(b'{"type": "x", "sid": 1, "msg": {"a": 1}}')
    assert raw_json_text(env.msg) == '{"a": 1}'


def test_rest_orderbook_levels_keeps_yes_side_as_is() -> None:
    book = OrderbookCountFp(yes_dollars=[("0.0800", "300.00")], no_dollars=[])
    bids, asks = rest_orderbook_levels(book)
    assert [(lvl.price, lvl.count) for lvl in bids] == [(800, 30_000)]
    assert asks == ()


def test_rest_orderbook_levels_complements_the_no_side() -> None:
    # A NO bid at 0.4000 is a YES ask at 1 - 0.4000 = 0.6000 (docs/DATA_FORMATS.md 1.3).
    book = OrderbookCountFp(yes_dollars=[], no_dollars=[("0.4000", "12.00")])
    bids, asks = rest_orderbook_levels(book)
    assert bids == ()
    assert [(lvl.price, lvl.count) for lvl in asks] == [(6000, 1_200)]


def test_rest_orderbook_levels_drops_zero_count_levels() -> None:
    book = OrderbookCountFp(yes_dollars=[("0.1000", "0.00")], no_dollars=[("0.2000", "0.00")])
    assert rest_orderbook_levels(book) == ((), ())
