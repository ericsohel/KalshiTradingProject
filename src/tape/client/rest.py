"""Async REST client for the Kalshi Predictions API (docs/INTERFACES.md 6.3).

``KalshiRest`` is the only place the package calls the REST API. It signs requests
when a ``Signer`` is present, waits on the injected ``RateLimiter`` before every call,
follows cursors with a bounded page cap, and maps every failure to a ``KalshiError``
subclass so no ``httpx`` exception ever escapes. It performs no fixed-point conversion:
every method returns the ``tape.wire.rest`` struct Kalshi actually sent, string prices
and all, so parsing happens exactly once, at whatever calls this client
(docs/ENGINEERING_STANDARDS.md 3.2).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from typing import Final

import httpx
import msgspec

from tape.client.auth import Signer
from tape.client.ratelimit import DEFAULT_TOKEN_COST, Bucket, RateLimiter
from tape.errors import KalshiHttpError, KalshiTransportError, RateLimitedError, WireError
from tape.fixedpoint import CountE2, format_count
from tape.timeutil import Clock, ns_to_ms
from tape.wire.rest import (
    BatchGetMarketCandlesticksResponse,
    CancelOrderV2Response,
    CreateOrderV2Request,
    CreateOrderV2Response,
    DecreaseOrderV2Request,
    DecreaseOrderV2Response,
    ErrorResponse,
    EventData,
    ExchangeStatus,
    Fill,
    GetAccountApiLimitsResponse,
    GetApiKeysResponse,
    GetBalanceResponse,
    GetEventsResponse,
    GetFillsResponse,
    GetMarketOrderbookResponse,
    GetMarketOrderbooksResponse,
    GetMarketResponse,
    GetMarketsResponse,
    GetSeriesFeeChangesResponse,
    GetSeriesListResponse,
    GetSettlementsResponse,
    GetTradesResponse,
    Market,
    MarketCandlesticksResponse,
    MarketOrderbookFp,
    OrderbookCountFp,
    Page,
    Series,
    SeriesFeeChange,
    Settlement,
    Trade,
)

__all__ = ["DEFAULT_MAX_PAGES", "KalshiRest", "build_client"]

type QueryValue = str | int | float | bool | None
type QueryParams = dict[str, QueryValue | list[str]]

_API_TIMEOUT_S: Final = 10.0
_HTTP_TOO_MANY_REQUESTS: Final = 429
_HTTP_BAD_REQUEST: Final = 400
_CANCEL_TOKEN_COST: Final = 2
"""Cancelling one order or all orders costs 2 tokens; every other write costs 10."""
_MAX_BATCH_TICKERS: Final = 100
"""Kalshi rejects a batch orderbook or candlestick request over 100 tickers."""

DEFAULT_MAX_PAGES: Final = 1000
"""Default safety bound for the ``iter_*`` generators (docs/ENGINEERING_STANDARDS.md 3.6)."""


def build_client(base_url: str, *, timeout_s: float = _API_TIMEOUT_S) -> httpx.AsyncClient:
    """Construct the ``httpx.AsyncClient`` a real ``KalshiRest`` should use.

    ``KalshiRest`` never constructs its own transport (docs/ENGINEERING_STANDARDS.md
    2.2: no module-level singletons or clients); the composition root calls this once
    and passes the result in, or a test passes an ``httpx.AsyncClient`` built on
    ``httpx.MockTransport`` instead.

    Args:
        base_url: API root including the version prefix, for example
            ``"https://api.elections.kalshi.com/trade-api/v2"``.
        timeout_s: Timeout applied to connect, read, write, and pool acquisition.
            Kalshi endpoints have no documented SLA, so this is deliberately generous.

    Returns:
        A client with an explicit timeout and default redirect/retry behavior; retries
        are ``KalshiRest``'s responsibility, not the transport's.
    """
    return httpx.AsyncClient(base_url=base_url, timeout=httpx.Timeout(timeout_s))


def _drop_none(params: dict[str, QueryValue | list[str]]) -> QueryParams:
    """Remove ``None`` values so httpx does not send them as empty query strings."""
    return {key: value for key, value in params.items() if value is not None}


def _check_batch(tickers: Sequence[str]) -> None:
    """Validate a ticker batch for ``orderbooks`` and ``candlesticks``.

    Raises:
        ValueError: If ``tickers`` is empty or larger than Kalshi's 100-ticker limit.
            The caller must split the batch itself; this client never truncates.
    """
    if not tickers:
        raise ValueError("at least one ticker is required")
    if len(tickers) > _MAX_BATCH_TICKERS:
        raise ValueError(f"at most {_MAX_BATCH_TICKERS} tickers per request, got {len(tickers)}")


def _error_from_response(response: httpx.Response) -> KalshiHttpError:
    """Build a ``KalshiHttpError`` from a non-2xx response, parsing the body if present."""
    code: str | None = None
    message: str | None = None
    details: str | None = None
    try:
        body = msgspec.json.decode(response.content, type=ErrorResponse)
    except (msgspec.DecodeError, msgspec.ValidationError):
        body = None
    if body is not None:
        code, message, details = body.code, body.message, body.details
    return KalshiHttpError(response.status_code, code=code, message=message, details=details)


def _decode[T](response: httpx.Response, struct_type: type[T]) -> T:
    """Decode a 2xx response body into ``struct_type``.

    Raises:
        WireError: If the body does not match ``struct_type``.
    """
    try:
        return msgspec.json.decode(response.content, type=struct_type)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise WireError(f"bad response body for {struct_type.__name__}: {exc}") from exc


class KalshiRest:
    """Concrete REST adapter implementing docs/INTERFACES.md 6.3.

    INTERFACES.md 6.3 sketches ``KalshiRest`` as a ``Protocol``; this is its one real
    implementation. Nothing else in the package talks to Kalshi over HTTP, so a
    separate protocol type would only add indirection: tests substitute the fake
    ``RateLimiter`` and an ``httpx.MockTransport``-backed client instead.

    Args:
        base_url: API root including the version prefix, matching the ``httpx.AsyncClient``
            passed as ``http`` (used to compute the path that gets signed).
        http: Transport to use. Construct it with ``build_client`` in production, or
            with ``httpx.MockTransport`` in tests; this class never builds its own.
        limiter: Rate limiter to wait on before every request.
        clock: Time source for signing timestamps (ADR 0004: only the composition
            root reads a real clock).
        signer: Request signer. ``None`` restricts the client to public endpoints.
    """

    def __init__(
        self,
        base_url: str,
        http: httpx.AsyncClient,
        limiter: RateLimiter,
        clock: Clock,
        signer: Signer | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._http = http
        self._limiter = limiter
        self._clock = clock
        self._signer = signer

    def _sign_path(self, path: str) -> str:
        """Return the root-relative path Kalshi expects to be signed, no query string."""
        prefix = httpx.URL(self._base_url).path.rstrip("/")
        return f"{prefix}{path}"

    async def _request(
        self,
        method: str,
        path: str,
        *,
        bucket: Bucket,
        params: QueryParams | None = None,
        body: msgspec.Struct | None = None,
        cost: int = DEFAULT_TOKEN_COST,
    ) -> httpx.Response:
        """Send one signed, rate-limited request and map failures to ``KalshiError``.

        Raises:
            KalshiHttpError: The response status was 4xx or 5xx (other than 429).
            RateLimitedError: The response status was 429.
            KalshiTransportError: A network-level failure (timeout, DNS, TLS, reset).
        """
        await self._limiter.acquire(cost, bucket=bucket)
        headers: dict[str, str] = {}
        content: bytes | None = None
        if body is not None:
            content = msgspec.json.encode(body)
            headers["Content-Type"] = "application/json"
        if self._signer is not None:
            now_ms = ns_to_ms(self._clock.wall_ns())
            headers.update(self._signer.headers(method, self._sign_path(path), now_ms=now_ms))
        try:
            response = await self._http.request(
                method, path, params=params, content=content, headers=headers
            )
        except httpx.TimeoutException as exc:
            raise KalshiTransportError(f"timeout on {method} {path}: {exc}") from exc
        except httpx.TransportError as exc:
            raise KalshiTransportError(f"transport error on {method} {path}: {exc}") from exc
        if response.status_code == _HTTP_TOO_MANY_REQUESTS:
            raise RateLimitedError()
        if response.status_code >= _HTTP_BAD_REQUEST:
            raise _error_from_response(response)
        return response

    async def _iter_pages[T](
        self,
        page_fn: Callable[[str | None], Awaitable[Page[T]]],
        *,
        max_pages: int,
    ) -> AsyncIterator[T]:
        """Follow a cursor-paginated endpoint until it empties or ``max_pages`` is hit.

        Args:
            page_fn: Fetches one page given the previous page's cursor (``None`` for
                the first page).
            max_pages: Maximum pages to fetch. Iteration stops silently, without
                raising, if the cursor is still non-empty after this many pages
                (docs/ENGINEERING_STANDARDS.md 3.6: every pagination loop has a cap).

        Yields:
            Each item across all fetched pages, in server order.

        Raises:
            ValueError: If ``max_pages`` is not positive.
        """
        if max_pages <= 0:
            raise ValueError(f"max_pages must be positive, got {max_pages}")
        cursor: str | None = None
        for _ in range(max_pages):
            page = await page_fn(cursor)
            for item in page.items:
                yield item
            if not page.cursor:
                return
            cursor = page.cursor

    async def exchange_status(self) -> ExchangeStatus:
        """Fetch exchange and per-shard trading status.

        Returns:
            The current ``ExchangeStatus``.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        response = await self._request("GET", "/exchange/status", bucket="read")
        return _decode(response, ExchangeStatus)

    async def markets(
        self,
        *,
        status: str | None = None,
        cursor: str | None = None,
        limit: int = 1000,
        **filters: QueryValue,
    ) -> Page[Market]:
        """List markets, one page at a time.

        Args:
            status: Market status filter (``"unopened"``, ``"open"``, ``"closed"``,
                ``"settled"``); ``None`` returns markets of any status.
            cursor: Cursor from a previous page, or ``None`` for the first page.
            limit: Page size, 1 to 1000.
            **filters: Extra query parameters forwarded verbatim, for example
                ``series_ticker``, ``event_ticker``, ``min_updated_ts``, or
                ``mve_filter``.

        Returns:
            One page of markets and the cursor for the next page.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        return await self._markets_page(status=status, cursor=cursor, limit=limit, filters=filters)

    async def _markets_page(
        self,
        *,
        status: str | None,
        cursor: str | None,
        limit: int,
        filters: dict[str, QueryValue],
    ) -> Page[Market]:
        """Shared implementation of :meth:`markets` and :meth:`iter_markets`.

        A plain, fully-typed ``filters`` parameter (rather than re-spreading a
        ``**filters`` catch-all into another ``**filters`` catch-all) is what lets
        mypy verify this call; spreading a ``dict[str, QueryValue]`` into a call with
        both named keyword parameters and a ``**kwargs`` catch-all is not something
        mypy can check key-by-key.
        """
        params = _drop_none({"status": status, "cursor": cursor, "limit": limit, **filters})
        response = await self._request("GET", "/markets", bucket="read", params=params)
        body = _decode(response, GetMarketsResponse)
        return Page(items=tuple(body.markets), cursor=body.cursor or None)

    async def iter_markets(
        self,
        *,
        status: str | None = None,
        limit: int = 1000,
        max_pages: int = DEFAULT_MAX_PAGES,
        **filters: QueryValue,
    ) -> AsyncIterator[Market]:
        """Yield every market across all pages, following ``cursor`` until it empties.

        Args:
            status: See :meth:`markets`.
            limit: See :meth:`markets`.
            max_pages: Safety bound on the number of pages fetched.
            **filters: Forwarded to :meth:`markets`, for example ``series_ticker``.

        Yields:
            Each ``Market`` in server order.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """

        async def _page(cursor: str | None) -> Page[Market]:
            return await self._markets_page(
                status=status, cursor=cursor, limit=limit, filters=filters
            )

        async for item in self._iter_pages(_page, max_pages=max_pages):
            yield item

    async def market(self, ticker: str) -> Market:
        """Fetch one market by ticker.

        Args:
            ticker: Market ticker, for example ``"INXD-24JAN01-B4487"``.

        Returns:
            The market's current state.

        Raises:
            KalshiHttpError: Non-2xx response, including 404 for an unknown ticker.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        response = await self._request("GET", f"/markets/{ticker}", bucket="read")
        return _decode(response, GetMarketResponse).market

    async def orderbook(self, ticker: str, *, depth: int = 0) -> OrderbookCountFp:
        """Fetch one market's resting bids on both sides.

        Args:
            ticker: Market ticker.
            depth: Number of price levels per side; 0 or negative means all levels.

        Returns:
            The orderbook with fixed-point contract count strings.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params = _drop_none({"depth": depth})
        response = await self._request(
            "GET", f"/markets/{ticker}/orderbook", bucket="read", params=params
        )
        return _decode(response, GetMarketOrderbookResponse).orderbook_fp

    async def orderbooks(self, tickers: Sequence[str]) -> list[MarketOrderbookFp]:
        """Fetch orderbooks for multiple markets in one request.

        Args:
            tickers: Market tickers to fetch, 1 to 100 of them.

        Returns:
            One orderbook per requested ticker, in the order Kalshi returns them.

        Raises:
            ValueError: If ``tickers`` is empty or has more than 100 entries; this
                client never truncates a batch silently.
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        _check_batch(tickers)
        params: QueryParams = {"tickers": list(tickers)}
        response = await self._request("GET", "/markets/orderbooks", bucket="read", params=params)
        return list(_decode(response, GetMarketOrderbooksResponse).orderbooks)

    async def trades(
        self,
        *,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        cursor: str | None = None,
    ) -> Page[Trade]:
        """List completed trades, one page at a time.

        Args:
            ticker: Restrict to one market, or ``None`` for all markets.
            min_ts: Inclusive lower bound on trade creation time, Unix seconds.
            max_ts: Inclusive upper bound on trade creation time, Unix seconds.
            cursor: Cursor from a previous page, or ``None`` for the first page.

        Returns:
            One page of trades and the cursor for the next page.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params = _drop_none(
            {"ticker": ticker, "min_ts": min_ts, "max_ts": max_ts, "cursor": cursor, "limit": 1000}
        )
        response = await self._request("GET", "/markets/trades", bucket="read", params=params)
        body = _decode(response, GetTradesResponse)
        return Page(items=tuple(body.trades), cursor=body.cursor or None)

    async def iter_trades(
        self,
        *,
        ticker: str | None = None,
        min_ts: int | None = None,
        max_ts: int | None = None,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[Trade]:
        """Yield every trade across all pages, following ``cursor`` until it empties.

        Args: see :meth:`trades`; ``max_pages`` bounds the number of pages fetched.

        Yields:
            Each ``Trade`` in server order.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """

        async def _page(cursor: str | None) -> Page[Trade]:
            return await self.trades(ticker=ticker, min_ts=min_ts, max_ts=max_ts, cursor=cursor)

        async for item in self._iter_pages(_page, max_pages=max_pages):
            yield item

    async def series(self, *, min_updated_ts: int | None = None) -> list[Series]:
        """List series (fee regime and category metadata for recurring events).

        Args:
            min_updated_ts: Only series updated after this Unix timestamp, or ``None``
                for all series.

        Returns:
            Every matching series. This endpoint is not paginated.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params = _drop_none({"min_updated_ts": min_updated_ts})
        response = await self._request("GET", "/series", bucket="read", params=params)
        return list(_decode(response, GetSeriesListResponse).series)

    async def fee_changes(self, *, show_historical: bool = False) -> list[SeriesFeeChange]:
        """List scheduled (and optionally past) series-level fee changes.

        Args:
            show_historical: If true, include fee changes already in effect.

        Returns:
            Every matching fee change. This endpoint is not paginated.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params: QueryParams = {"show_historical": show_historical}
        response = await self._request("GET", "/series/fee_changes", bucket="read", params=params)
        return list(_decode(response, GetSeriesFeeChangesResponse).series_fee_change_arr)

    async def events(
        self,
        *,
        status: str | None = None,
        with_nested_markets: bool = False,
        cursor: str | None = None,
    ) -> Page[EventData]:
        """List events, one page at a time.

        Args:
            status: Event status filter (``"unopened"``, ``"open"``, ``"closed"``,
                ``"settled"``); ``None`` returns events of any status.
            with_nested_markets: If true, each event includes its markets.
            cursor: Cursor from a previous page, or ``None`` for the first page.

        Returns:
            One page of events and the cursor for the next page.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params = _drop_none(
            {
                "status": status,
                "with_nested_markets": with_nested_markets,
                "cursor": cursor,
                "limit": 200,
            }
        )
        response = await self._request("GET", "/events", bucket="read", params=params)
        body = _decode(response, GetEventsResponse)
        return Page(items=tuple(body.events), cursor=body.cursor or None)

    async def iter_events(
        self,
        *,
        status: str | None = None,
        with_nested_markets: bool = False,
        max_pages: int = DEFAULT_MAX_PAGES,
    ) -> AsyncIterator[EventData]:
        """Yield every event across all pages, following ``cursor`` until it empties.

        Args: see :meth:`events`; ``max_pages`` bounds the number of pages fetched.

        Yields:
            Each ``EventData`` in server order.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """

        async def _page(cursor: str | None) -> Page[EventData]:
            return await self.events(
                status=status, with_nested_markets=with_nested_markets, cursor=cursor
            )

        async for item in self._iter_pages(_page, max_pages=max_pages):
            yield item

    async def candlesticks(
        self, tickers: Sequence[str], *, start_ts: int, end_ts: int, period_min: int
    ) -> list[MarketCandlesticksResponse]:
        """Fetch candlesticks for multiple markets in one request.

        Args:
            tickers: Market tickers to fetch, 1 to 100 of them.
            start_ts: Range start, Unix seconds.
            end_ts: Range end, Unix seconds.
            period_min: Candlestick period in minutes (Kalshi documents 1, 60, 1440).

        Returns:
            One entry per requested market, each carrying its own candlestick series.

        Raises:
            ValueError: If ``tickers`` is empty or has more than 100 entries; this
                client never truncates a batch silently.
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        _check_batch(tickers)
        params: QueryParams = {
            "market_tickers": ",".join(tickers),
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_min,
        }
        response = await self._request("GET", "/markets/candlesticks", bucket="read", params=params)
        return list(_decode(response, BatchGetMarketCandlesticksResponse).markets)

    async def account_limits(self) -> GetAccountApiLimitsResponse:
        """Fetch the authenticated user's usage tier and token-bucket limits.

        Used to seed ``RateLimiter.resize`` after a key is loaded.

        Returns:
            The account's usage tier and both bucket limits.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        response = await self._request("GET", "/account/limits", bucket="read")
        return _decode(response, GetAccountApiLimitsResponse)

    async def api_keys(self) -> GetApiKeysResponse:
        """Fetch every API key on the authenticated account, for the attestation check.

        Returns:
            The account's API keys and region-attestation expiration.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        response = await self._request("GET", "/api_keys", bucket="read")
        return _decode(response, GetApiKeysResponse)

    async def create_order(self, req: CreateOrderV2Request) -> CreateOrderV2Response:
        """Submit an order.

        Args:
            req: The order request. ``price`` and ``count`` are Kalshi's fixed-point
                strings; format them with ``tape.fixedpoint`` before calling.

        Returns:
            The created order's id and immediate fill state.

        Raises:
            KalshiHttpError: Non-2xx response, including 409 on a conflicting order.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        response = await self._request("POST", "/portfolio/events/orders", bucket="write", body=req)
        return _decode(response, CreateOrderV2Response)

    async def cancel_order(self, order_id: str, *, market_ticker: str) -> CancelOrderV2Response:
        """Cancel one resting order.

        Args:
            order_id: Id of the order to cancel.
            market_ticker: Market the order is on, used to auto-route to the correct
                exchange shard.

        Returns:
            The order id and the count that was still resting when it was cancelled.

        Raises:
            KalshiHttpError: Non-2xx response, including 404 for an unknown order.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params: QueryParams = {"market_ticker": market_ticker}
        response = await self._request(
            "DELETE",
            f"/portfolio/events/orders/{order_id}",
            bucket="write",
            params=params,
            cost=_CANCEL_TOKEN_COST,
        )
        return _decode(response, CancelOrderV2Response)

    async def decrease_order(
        self, order_id: str, *, reduce_by: CountE2, market_ticker: str
    ) -> DecreaseOrderV2Response:
        """Decrease the remaining count of one resting order.

        Args:
            order_id: Id of the order to decrease.
            reduce_by: Number of contracts to remove from the resting count.
            market_ticker: Market the order is on, used to auto-route to the correct
                exchange shard.

        Returns:
            The order id and its remaining count after the decrease.

        Raises:
            KalshiHttpError: Non-2xx response, including 404 for an unknown order.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        body = DecreaseOrderV2Request(
            reduce_by=format_count(reduce_by), market_ticker=market_ticker
        )
        response = await self._request(
            "POST", f"/portfolio/events/orders/{order_id}/decrease", bucket="write", body=body
        )
        return _decode(response, DecreaseOrderV2Response)

    async def cancel_all(self) -> None:
        """Cancel every resting order on the account, across all exchange shards.

        This is the kill switch. Kalshi answers with ``204 No Content`` and reports no
        count of cancelled orders anywhere in the response (checked against the pinned
        spec in ``specs/openapi.yaml``), so there is nothing to return; returning a
        constant would imply a count the exchange never gave. A caller that needs the
        number must read resting orders before calling this.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        await self._request(
            "DELETE", "/portfolio/events/orders", bucket="write", cost=_CANCEL_TOKEN_COST
        )

    async def fills(self, *, min_ts: int | None = None, cursor: str | None = None) -> Page[Fill]:
        """List the account's fills, one page at a time.

        Args:
            min_ts: Only fills created after this Unix timestamp, or ``None`` for all.
            cursor: Cursor from a previous page, or ``None`` for the first page.

        Returns:
            One page of fills and the cursor for the next page.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params = _drop_none({"min_ts": min_ts, "cursor": cursor})
        response = await self._request("GET", "/portfolio/fills", bucket="read", params=params)
        body = _decode(response, GetFillsResponse)
        return Page(items=tuple(body.fills), cursor=body.cursor or None)

    async def iter_fills(
        self, *, min_ts: int | None = None, max_pages: int = DEFAULT_MAX_PAGES
    ) -> AsyncIterator[Fill]:
        """Yield every fill across all pages, following ``cursor`` until it empties.

        Args: see :meth:`fills`; ``max_pages`` bounds the number of pages fetched.

        Yields:
            Each ``Fill`` in server order.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """

        async def _page(cursor: str | None) -> Page[Fill]:
            return await self.fills(min_ts=min_ts, cursor=cursor)

        async for item in self._iter_pages(_page, max_pages=max_pages):
            yield item

    async def settlements(
        self, *, min_ts: int | None = None, cursor: str | None = None
    ) -> Page[Settlement]:
        """List the account's settlements, one page at a time.

        Args:
            min_ts: Only settlements after this Unix timestamp, or ``None`` for all.
            cursor: Cursor from a previous page, or ``None`` for the first page.

        Returns:
            One page of settlements and the cursor for the next page.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params = _drop_none({"min_ts": min_ts, "cursor": cursor})
        response = await self._request(
            "GET", "/portfolio/settlements", bucket="read", params=params
        )
        body = _decode(response, GetSettlementsResponse)
        return Page(items=tuple(body.settlements), cursor=body.cursor or None)

    async def iter_settlements(
        self, *, min_ts: int | None = None, max_pages: int = DEFAULT_MAX_PAGES
    ) -> AsyncIterator[Settlement]:
        """Yield every settlement across all pages, following ``cursor`` until empty.

        Args: see :meth:`settlements`; ``max_pages`` bounds the number of pages fetched.

        Yields:
            Each ``Settlement`` in server order.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """

        async def _page(cursor: str | None) -> Page[Settlement]:
            return await self.settlements(min_ts=min_ts, cursor=cursor)

        async for item in self._iter_pages(_page, max_pages=max_pages):
            yield item

    async def balance(self, *, exchange_index: int | None = None) -> GetBalanceResponse:
        """Fetch the account's balance and portfolio value.

        Args:
            exchange_index: Restrict to one exchange shard, or ``None`` for the total
                across all shards.

        Returns:
            The current balance and portfolio value.

        Raises:
            KalshiHttpError: Non-2xx response.
            RateLimitedError: The exchange answered 429.
            KalshiTransportError: A network-level failure.
        """
        params = _drop_none({"exchange_index": exchange_index})
        response = await self._request("GET", "/portfolio/balance", bucket="read", params=params)
        return _decode(response, GetBalanceResponse)
