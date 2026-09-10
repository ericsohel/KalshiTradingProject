"""Typed structs for Kalshi REST payloads, mirroring ``specs/openapi.yaml``.

Every struct here matches Kalshi's field names and string encodings exactly, field for
field with the pinned OpenAPI spec (docs/DATA_FORMATS.md 2.2). Prices, counts, and
dollar amounts stay as the strings Kalshi sends (``"0.5600"``, ``"10.00"``); converting
them into ``PriceE4``/``CountE2``/``DollarsE6`` is the caller's job
(``tape.client.rest``), not this module's. Unknown fields are ignored on decode so
additive API changes never break capture (docs/DATA_FORMATS.md 9).

``Page`` is a plain, non-wire helper: callers assemble it from a cursor-paginated
response's items and cursor. It is never decoded from JSON directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import msgspec

__all__ = [
    "ApiKey",
    "ApiUsageLevelGrant",
    "BatchGetMarketCandlesticksResponse",
    "BidAskDistribution",
    "BookSide",
    "BucketLimit",
    "CancelOrderV2Response",
    "CreateOrderV2Request",
    "CreateOrderV2Response",
    "DecreaseOrderV2Request",
    "DecreaseOrderV2Response",
    "ErrorResponse",
    "EventData",
    "ExchangeIndexStatus",
    "ExchangeStatus",
    "FeeType",
    "Fill",
    "GetAccountApiLimitsResponse",
    "GetApiKeysResponse",
    "GetBalanceResponse",
    "GetEventsResponse",
    "GetFillsResponse",
    "GetHistoricalCutoffResponse",
    "GetMarketOrderbookResponse",
    "GetMarketOrderbooksResponse",
    "GetMarketResponse",
    "GetMarketsResponse",
    "GetSeriesFeeChangesResponse",
    "GetSeriesListResponse",
    "GetSettlementsResponse",
    "GetTradesResponse",
    "Market",
    "MarketCandlestick",
    "MarketCandlesticksResponse",
    "MarketOrderbookFp",
    "MarketResult",
    "MarketStatus",
    "MarketType",
    "OrderbookCountFp",
    "OutcomeSide",
    "Page",
    "PriceDistribution",
    "PriceLevelDollarsCountFp",
    "PriceRange",
    "SelfTradePreventionType",
    "Series",
    "SeriesFeeChange",
    "Settlement",
    "SettlementResult",
    "SettlementSource",
    "TimeInForce",
]

# Inbound taxonomies are plain ``str``, even where the spec publishes an enum: on
# 2026-09-10 the live fee-changes endpoint returned "margin_market_maker_program_fees",
# which the pinned OpenAPI FeeType enum does not contain. A strict enum turns any such
# addition into a decode failure, and additive changes must never break capture
# (ADR 0017, docs/DATA_FORMATS.md 9). Recognizing these values is the domain layer's
# job, and it refuses to act on one it does not know.
FeeType = str
MarketType = str
MarketStatus = str
MarketResult = str
SettlementResult = str

# Directional bits stay closed. An unrecognized side must fail loudly rather than be
# guessed at, because guessing puts an order or a fill on the wrong side of the book.
BookSide = Literal["bid", "ask"]
OutcomeSide = Literal["yes", "no"]

# Request-only enums stay closed: they constrain what this client may send.
TimeInForce = Literal["fill_or_kill", "good_till_canceled", "immediate_or_cancel"]
SelfTradePreventionType = Literal["taker_at_cross", "maker"]

PriceLevelDollarsCountFp = tuple[str, str]
"""A ``[price_dollars, count_fp]`` pair, e.g. ``("0.1500", "100.00")``."""


@dataclass(frozen=True, slots=True)
class Page[T]:
    """One page of a cursor-paginated response.

    Not a wire struct: callers build this from a decoded response's items array and
    ``cursor`` field. It is the return type of every paginated ``KalshiRest`` method.

    Attributes:
        items: The page's items, in server order.
        cursor: Opaque cursor for the next page, or ``None`` when this was the last
            page (Kalshi signals that with an empty string).
    """

    items: tuple[T, ...]
    cursor: str | None


class ExchangeIndexStatus(msgspec.Struct, frozen=True, kw_only=True):
    """Status of one exchange shard, nested in ``ExchangeStatus``."""

    exchange_index: int
    description: str
    exchange_active: bool
    trading_active: bool
    intra_exchange_transfers_active: bool


class ExchangeStatus(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /exchange/status`` response: the health gate checked before recording."""

    exchange_active: bool
    trading_active: bool
    intra_exchange_transfers_active: bool | None = None
    exchange_estimated_resume_time: str | None = None
    exchange_index_statuses: list[ExchangeIndexStatus] | None = None


class PriceRange(msgspec.Struct, frozen=True, kw_only=True):
    """A ``{start, end, step}`` price range in dollar strings."""

    start: str
    end: str
    step: str


class Market(msgspec.Struct, frozen=True, kw_only=True):
    """A single market, as returned by ``GET /markets`` and ``GET /markets/{ticker}``."""

    ticker: str
    event_ticker: str
    market_type: MarketType
    yes_sub_title: str
    no_sub_title: str
    created_time: str
    updated_time: str
    open_time: str
    close_time: str
    latest_expiration_time: str
    settlement_timer_seconds: int
    status: MarketStatus
    notional_value_dollars: str
    yes_bid_dollars: str
    yes_ask_dollars: str
    no_bid_dollars: str
    no_ask_dollars: str
    yes_bid_size_fp: str
    yes_ask_size_fp: str
    last_price_dollars: str
    previous_yes_bid_dollars: str
    previous_yes_ask_dollars: str
    previous_price_dollars: str
    volume_fp: str
    volume_24h_fp: str
    open_interest_fp: str
    result: MarketResult
    can_close_early: bool
    expiration_value: str
    rules_primary: str
    rules_secondary: str
    price_level_structure: str
    price_ranges: list[PriceRange]
    exchange_index: int | None = None
    strike_type: str | None = None
    floor_strike: float | None = None
    cap_strike: float | None = None
    settlement_value_dollars: str | None = None
    settlement_ts: str | None = None


class GetMarketsResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /markets`` response."""

    markets: list[Market]
    cursor: str


class GetMarketResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /markets/{ticker}`` response."""

    market: Market


class OrderbookCountFp(msgspec.Struct, frozen=True, kw_only=True):
    """A market's resting bids, both sides, with fixed-point contract counts.

    Kalshi returns only bids (a YES bid at ``X`` is a NO ask at ``1 - X``); there is no
    separate ask array.
    """

    yes_dollars: list[PriceLevelDollarsCountFp]
    no_dollars: list[PriceLevelDollarsCountFp]


class GetMarketOrderbookResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /markets/{ticker}/orderbook`` response."""

    orderbook_fp: OrderbookCountFp


class MarketOrderbookFp(msgspec.Struct, frozen=True, kw_only=True):
    """One market's orderbook within a batch response."""

    ticker: str
    orderbook_fp: OrderbookCountFp


class GetMarketOrderbooksResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /markets/orderbooks`` response."""

    orderbooks: list[MarketOrderbookFp]


class Trade(msgspec.Struct, frozen=True, kw_only=True):
    """A single completed trade, as returned by ``GET /markets/trades``."""

    trade_id: str
    ticker: str
    count_fp: str
    yes_price_dollars: str
    no_price_dollars: str
    taker_outcome_side: OutcomeSide
    taker_book_side: BookSide
    created_time: str
    is_block_trade: bool
    taker_side: OutcomeSide | None = None


class GetTradesResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /markets/trades`` response."""

    trades: list[Trade]
    cursor: str


class SettlementSource(msgspec.Struct, frozen=True, kw_only=True):
    """One official settlement source for a series or event."""

    name: str | None = None
    url: str | None = None


class Series(msgspec.Struct, frozen=True, kw_only=True):
    """A series (the template a recurring event follows), from ``GET /series``."""

    ticker: str
    frequency: str
    title: str
    category: str
    tags: list[str] | None
    settlement_sources: list[SettlementSource] | None
    contract_url: str
    contract_terms_url: str
    fee_type: FeeType
    fee_multiplier: float
    additional_prohibitions: list[str] | None
    volume_fp: str | None = None
    last_updated_ts: str | None = None
    exchange_index: int | None = None


class GetSeriesListResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /series`` response."""

    series: list[Series]


class SeriesFeeChange(msgspec.Struct, frozen=True, kw_only=True):
    """A scheduled fee change for a series, from ``GET /series/fee_changes``."""

    id: str
    series_ticker: str
    fee_type: FeeType
    fee_multiplier: float
    scheduled_ts: str


class GetSeriesFeeChangesResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /series/fee_changes`` response."""

    series_fee_change_arr: list[SeriesFeeChange]


class EventData(msgspec.Struct, frozen=True, kw_only=True):
    """An event (a group of mutually related markets), from ``GET /events``."""

    event_ticker: str
    series_ticker: str
    sub_title: str
    title: str
    collateral_return_type: str
    mutually_exclusive: bool
    settlement_sources: list[SettlementSource] | None
    strike_date: str | None = None
    strike_period: str | None = None
    markets: list[Market] | None = None
    last_updated_ts: str | None = None
    fee_type_override: str | None = None
    fee_multiplier_override: float | None = None
    exchange_index: int | None = None


class GetEventsResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /events`` response."""

    events: list[EventData]
    cursor: str


class BidAskDistribution(msgspec.Struct, frozen=True, kw_only=True):
    """OHLC offer prices for one side of the book over a candlestick period."""

    open_dollars: str
    low_dollars: str
    high_dollars: str
    close_dollars: str


class PriceDistribution(msgspec.Struct, frozen=True, kw_only=True):
    """OHLC and summary trade prices over a candlestick period.

    Every field is ``None`` when no trade occurred during the period.
    """

    open_dollars: str | None = None
    low_dollars: str | None = None
    high_dollars: str | None = None
    close_dollars: str | None = None
    mean_dollars: str | None = None
    previous_dollars: str | None = None
    min_dollars: str | None = None
    max_dollars: str | None = None


class MarketCandlestick(msgspec.Struct, frozen=True, kw_only=True):
    """One candlestick period for a market."""

    end_period_ts: int
    yes_bid: BidAskDistribution
    yes_ask: BidAskDistribution
    price: PriceDistribution
    volume_fp: str
    open_interest_fp: str


class MarketCandlesticksResponse(msgspec.Struct, frozen=True, kw_only=True):
    """One market's candlesticks within a ``GET /markets/candlesticks`` batch response."""

    market_ticker: str
    candlesticks: list[MarketCandlestick]


class BatchGetMarketCandlesticksResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /markets/candlesticks`` response: one entry per requested market."""

    markets: list[MarketCandlesticksResponse]


class BucketLimit(msgspec.Struct, frozen=True, kw_only=True):
    """Token-bucket budget for one rate-limit bucket, from ``GET /account/limits``."""

    refill_rate: int
    bucket_capacity: int


class ApiUsageLevelGrant(msgspec.Struct, frozen=True, kw_only=True):
    """One active API usage level grant, nested in ``GetAccountApiLimitsResponse``."""

    exchange_instance: str
    level: str
    source: str
    expires_ts: int | None = None


class GetAccountApiLimitsResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /account/limits`` response, used to seed the client-side rate limiter."""

    usage_tier: str
    read: BucketLimit
    write: BucketLimit
    grants: list[ApiUsageLevelGrant]


class ApiKey(msgspec.Struct, frozen=True, kw_only=True):
    """One API key belonging to the authenticated user."""

    api_key_id: str
    name: str
    scopes: list[str]
    subaccount: int | None = None
    fcm_subtrader_id: str | None = None


class GetApiKeysResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /api_keys`` response, used for the attestation check."""

    api_keys: list[ApiKey]
    api_key_region_expiration_ts: int | None = None


class CreateOrderV2Request(msgspec.Struct, frozen=True, kw_only=True):
    """Body of ``POST /portfolio/events/orders`` (docs/DATA_FORMATS.md 2.3)."""

    ticker: str
    side: BookSide
    count: str
    price: str
    time_in_force: TimeInForce
    self_trade_prevention_type: SelfTradePreventionType
    client_order_id: str | None = None
    expiration_time: int | None = None
    post_only: bool | None = None
    cancel_order_on_pause: bool | None = None
    reduce_only: bool | None = None
    subaccount: int | None = None
    order_group_id: str | None = None
    exchange_index: int | None = None


class CreateOrderV2Response(msgspec.Struct, frozen=True, kw_only=True):
    """201 response to ``POST /portfolio/events/orders``."""

    order_id: str
    fill_count: str
    remaining_count: str
    ts_ms: int
    client_order_id: str | None = None
    average_fill_price: str | None = None
    average_fee_paid: str | None = None


class CancelOrderV2Response(msgspec.Struct, frozen=True, kw_only=True):
    """Response to ``DELETE /portfolio/events/orders/{order_id}``."""

    order_id: str
    reduced_by: str
    ts_ms: int
    client_order_id: str | None = None


class DecreaseOrderV2Request(msgspec.Struct, frozen=True, kw_only=True):
    """Body of ``POST /portfolio/events/orders/{order_id}/decrease``.

    Exactly one of ``reduce_by`` or ``reduce_to`` must be set.
    """

    reduce_by: str | None = None
    reduce_to: str | None = None
    exchange_index: int | None = None
    market_ticker: str | None = None


class DecreaseOrderV2Response(msgspec.Struct, frozen=True, kw_only=True):
    """Response to ``POST /portfolio/events/orders/{order_id}/decrease``."""

    order_id: str
    remaining_count: str
    ts_ms: int
    client_order_id: str | None = None


class Fill(msgspec.Struct, frozen=True, kw_only=True):
    """A single fill, from ``GET /portfolio/fills``."""

    fill_id: str
    exchange_index: int
    trade_id: str
    order_id: str
    ticker: str
    market_ticker: str
    outcome_side: OutcomeSide
    book_side: BookSide
    count_fp: str
    yes_price_dollars: str
    no_price_dollars: str
    is_taker: bool
    fee_cost: str
    created_time: str | None = None
    subaccount_number: int | None = None
    ts: int | None = None


class GetFillsResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /portfolio/fills`` response."""

    fills: list[Fill]
    cursor: str


class Settlement(msgspec.Struct, frozen=True, kw_only=True):
    """A single market settlement, from ``GET /portfolio/settlements``."""

    ticker: str
    exchange_index: int
    event_ticker: str
    market_result: SettlementResult
    yes_count_fp: str
    yes_total_cost_dollars: str
    no_count_fp: str
    no_total_cost_dollars: str
    revenue: int
    settled_time: str
    fee_cost: str
    value: int | None = None


class GetSettlementsResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /portfolio/settlements`` response."""

    settlements: list[Settlement]
    cursor: str | None = None


class GetBalanceResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /portfolio/balance`` response."""

    balance: int
    balance_dollars: str
    portfolio_value: int
    updated_ts: int


class GetHistoricalCutoffResponse(msgspec.Struct, frozen=True, kw_only=True):
    """``GET /historical/cutoff`` response: the live/historical data boundary."""

    market_settled_ts: str
    trades_created_ts: str
    orders_updated_ts: str
    market_positions_last_updated_ts: str | None = None


class ErrorResponse(msgspec.Struct, frozen=True, kw_only=True):
    """Error body Kalshi attaches to a non-2xx response, when it sends one at all."""

    code: str | None = None
    message: str | None = None
    details: str | None = None
