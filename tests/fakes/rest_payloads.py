"""Kalshi REST payloads with every field the pinned spec requires, for httpx.MockTransport."""

from __future__ import annotations


def market_payload(
    ticker: str,
    *,
    volume_24h: str = "0.00",
    close_time: str = "2030-01-01T00:00:00Z",
    status: str = "active",
) -> dict[str, object]:
    """One ``GET /markets`` entry; its event is the ticker without its last dash-separated part.

    Args:
        ticker: Market ticker, such as ``KXNFLGAME-30JAN01NYJBUF-NYJ``.
        volume_24h: ``volume_24h_fp``, a fixed-point count string.
        close_time: ``close_time``, an ISO-8601 instant.
        status: Kalshi's market status.
    """
    return {
        "ticker": ticker,
        "event_ticker": ticker.rsplit("-", 1)[0],
        "market_type": "binary",
        "yes_sub_title": "Yes",
        "no_sub_title": "No",
        "created_time": "2026-01-01T00:00:00Z",
        "updated_time": "2026-01-01T00:00:00Z",
        "open_time": "2026-01-01T00:00:00Z",
        "close_time": close_time,
        "latest_expiration_time": close_time,
        "settlement_timer_seconds": 60,
        "status": status,
        "notional_value_dollars": "1.0000",
        "yes_bid_dollars": "0.4000",
        "yes_ask_dollars": "0.6000",
        "no_bid_dollars": "0.4000",
        "no_ask_dollars": "0.6000",
        "yes_bid_size_fp": "1.00",
        "yes_ask_size_fp": "1.00",
        "last_price_dollars": "0.5000",
        "previous_yes_bid_dollars": "0.4000",
        "previous_yes_ask_dollars": "0.6000",
        "previous_price_dollars": "0.5000",
        "volume_fp": volume_24h,
        "volume_24h_fp": volume_24h,
        "open_interest_fp": "1.00",
        "result": "",
        "can_close_early": False,
        "expiration_value": "",
        "rules_primary": "",
        "rules_secondary": "",
        "price_level_structure": "linear_cent",
        "price_ranges": [{"start": "0.00", "end": "1.00", "step": "0.01"}],
        "exchange_index": 0,
    }


def series_payload(ticker: str, category: str) -> dict[str, object]:
    """One ``GET /series`` entry.

    Args:
        ticker: Series ticker, such as ``KXNFLGAME``.
        category: The series category, such as ``Sports``.
    """
    return {
        "ticker": ticker,
        "frequency": "daily",
        "title": ticker,
        "category": category,
        "tags": None,
        "settlement_sources": None,
        "contract_url": "https://kalshi.test/contract",
        "contract_terms_url": "https://kalshi.test/terms",
        "fee_type": "quadratic",
        "fee_multiplier": 1.0,
        "additional_prohibitions": None,
    }
