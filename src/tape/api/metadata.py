"""Titles, categories, and price grids from Kalshi's public endpoints, fetched lazily (ADR 0023).

Responsibility: resolve, for the markets a response or a subscription needs, the event title,
the market's YES subtitle, its price grid, and the series category, without credentials and
without ever delaying live data. :meth:`MetadataResolver.request` never waits: it queues what is
missing or expired, and :meth:`MetadataResolver.run` fetches the queue one request at a time,
one ``GET /events/{event_ticker}`` per event (which carries every market in it) and one
``GET /series/{series_ticker}`` per series, paced by the token bucket inside its own REST client
(:func:`metadata_limits`). :meth:`MetadataResolver.lookup` answers from memory at once.

Invariants: an event or series is queued at most once while its request is pending or in flight;
a value is fetched again only after ``ttl_s``, and until then, or while a refetch is pending, the
last value is served; a failed request is logged and counted, never raised, and not retried
before a backoff that doubles per consecutive failure up to a cap; the queue and the cache are
bounded, the cache forgetting the least recently requested entries first; and nothing here reads
time except through the injected clock.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Final, Literal

import msgspec

from tape.api.contract import PriceRange
from tape.api.directory import MarketMetadata
from tape.client.ratelimit import DEFAULT_TOKEN_COST, BucketLimits
from tape.client.rest import KalshiRest
from tape.errors import FixedPointError, KalshiError, WireError
from tape.events import CatalogEntry
from tape.fixedpoint import parse_price
from tape.timeutil import NS_PER_S, Clock
from tape.wire.rest import Market

__all__ = [
    "DEFAULT_MAX_CACHED",
    "DEFAULT_MAX_PENDING",
    "DEFAULT_RETRY_INITIAL_S",
    "DEFAULT_RETRY_MAX_S",
    "MetadataResolver",
    "ResolverStats",
    "metadata_limits",
]

DEFAULT_RETRY_INITIAL_S: Final = 30
"""Wait before retrying an event or series after its first failure; it doubles per failure."""

DEFAULT_RETRY_MAX_S: Final = 900
"""Longest wait before retrying an event or series that keeps failing."""

DEFAULT_MAX_PENDING: Final = 10_000
"""Most events and series queued at once; more are refused and counted until the queue drains."""

DEFAULT_MAX_CACHED: Final = 20_000
"""Most events, and separately most series, remembered; far above a universe of 2,000 markets."""

_MAX_DOUBLINGS: Final = 16
"""Doublings of the retry delay beyond which only the cap applies."""

type _Kind = Literal["event", "series"]
_EVENT: Final = "event"
_SERIES: Final = "series"


def metadata_limits(requests_per_s: int) -> BucketLimits:
    """The token bucket that paces the resolver's REST client at a number of requests per second.

    Every request costs :data:`tape.client.ratelimit.DEFAULT_TOKEN_COST` tokens, so the bucket
    refills that many times the rate and bursts at most one second of requests.

    Args:
        requests_per_s: Requests per second; positive.

    Returns:
        Limits for both buckets of a ``BucketRateLimiter``.

    Raises:
        ValueError: If ``requests_per_s`` is not positive.
    """
    if requests_per_s < 1:
        raise ValueError(f"requests_per_s must be positive, got {requests_per_s}")
    tokens = requests_per_s * DEFAULT_TOKEN_COST
    return BucketLimits(refill_per_s=tokens, capacity=tokens)


class ResolverStats(msgspec.Struct, frozen=True, kw_only=True):
    """Counters since the resolver was built.

    Attributes:
        requests: Requests attempted.
        failures: Requests that failed; each is logged.
        refused: Events and series not queued because the queue was full.
        unparsable: Markets whose price grid did not parse; they show none.
        pending: Events and series queued or in flight now.
        events: Events remembered now.
        series: Series remembered now.
    """

    requests: int
    failures: int
    refused: int
    unparsable: int
    pending: int
    events: int
    series: int


class _MarketInfo(msgspec.Struct, frozen=True, kw_only=True):
    subtitle: str
    price_ranges: tuple[PriceRange, ...] | None


class _EventInfo(msgspec.Struct, frozen=True, kw_only=True):
    title: str
    markets: Mapping[str, _MarketInfo]


@dataclass(slots=True)
class _Slot[T]:
    """One event's or series' cache entry: its value, when it expires, and its retry state."""

    value: T | None = None
    expires_ns: int = 0
    failures: int = 0
    retry_at_ns: int = 0
    queued: bool = False


class MetadataResolver:
    """Resolves public market metadata in the background. See the module docstring.

    Not thread-safe; used on one event loop.

    Args:
        rest: A REST client without a signer whose limiter paces it, for example at
            :func:`metadata_limits`; it serves only this resolver.
        clock: Time for expiry and backoff.
        ttl_s: Seconds a resolved value is served before it is fetched again; positive.
        retry_initial_s: Wait before the first retry of a failed event or series; positive.
        retry_max_s: Cap on that wait as it doubles; at least ``retry_initial_s``.
        max_pending: Most events and series queued at once; positive.
        max_cached: Most events, and separately most series, remembered; positive.
        logger: Destination for logs; defaults to this module's logger.

    Raises:
        ValueError: If a duration or bound is out of range.
    """

    def __init__(
        self,
        rest: KalshiRest,
        *,
        clock: Clock,
        ttl_s: int,
        retry_initial_s: int = DEFAULT_RETRY_INITIAL_S,
        retry_max_s: int = DEFAULT_RETRY_MAX_S,
        max_pending: int = DEFAULT_MAX_PENDING,
        max_cached: int = DEFAULT_MAX_CACHED,
        logger: logging.Logger | None = None,
    ) -> None:
        for name, value in (
            ("ttl_s", ttl_s),
            ("retry_initial_s", retry_initial_s),
            ("max_pending", max_pending),
            ("max_cached", max_cached),
        ):
            if value < 1:
                raise ValueError(f"{name} must be positive, got {value}")
        if retry_max_s < retry_initial_s:
            raise ValueError(
                f"retry_max_s must be at least retry_initial_s = {retry_initial_s}, "
                f"got {retry_max_s}"
            )
        self._rest = rest
        self._clock = clock
        self._ttl_ns = ttl_s * NS_PER_S
        self._retry_initial_s = retry_initial_s
        self._retry_max_s = retry_max_s
        self._max_pending = max_pending
        self._max_cached = max_cached
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._events: dict[str, _Slot[_EventInfo]] = {}
        self._series: dict[str, _Slot[str]] = {}
        self._pending: deque[tuple[_Kind, str]] = deque()
        self._wake = asyncio.Event()
        self._requests = 0
        self._failures = 0
        self._refused = 0
        self._unparsable = 0

    @property
    def stats(self) -> ResolverStats:
        """Current counters; see :class:`ResolverStats`."""
        return ResolverStats(
            requests=self._requests,
            failures=self._failures,
            refused=self._refused,
            unparsable=self._unparsable,
            pending=sum(1 for slot in self._events.values() if slot.queued)
            + sum(1 for slot in self._series.values() if slot.queued),
            events=len(self._events),
            series=len(self._series),
        )

    def request(self, entries: Iterable[CatalogEntry]) -> None:
        """Queue what the given markets still need, without waiting.

        An event or series is queued when it was never fetched, has expired, or is past its
        retry time, unless it is already queued.

        Args:
            entries: The markets a response or subscription needs.
        """
        now_ns = int(self._clock.mono_ns())
        for entry in entries:
            self._want(self._events, _EVENT, entry.event_ticker, now_ns)
            self._want(self._series, _SERIES, entry.series_ticker, now_ns)

    def lookup(self, entry: CatalogEntry) -> MarketMetadata:
        """What is resolved about one market now, expired values included.

        Args:
            entry: The market.

        Returns:
            Its metadata, with ``None`` for whatever is not resolved.
        """
        event_slot = self._events.get(entry.event_ticker)
        series_slot = self._series.get(entry.series_ticker)
        event = None if event_slot is None else event_slot.value
        market = None if event is None else event.markets.get(entry.ticker)
        return MarketMetadata(
            title=None if event is None else event.title,
            subtitle=None if market is None else market.subtitle,
            category=None if series_slot is None else series_slot.value,
            price_ranges=None if market is None else market.price_ranges,
        )

    async def run(self) -> None:
        """Fetch queued events and series, one request at a time, until cancelled."""
        while True:
            while not self._pending:
                self._wake.clear()
                await self._wake.wait()
            kind, key = self._pending.popleft()
            if kind == _EVENT:
                await self._fetch(self._events, kind, key, self._fetch_event)
            else:
                await self._fetch(self._series, kind, key, self._fetch_category)

    def _want[T](self, slots: dict[str, _Slot[T]], kind: _Kind, key: str, now_ns: int) -> None:
        """Mark an entry as recently needed and queue it if it needs fetching."""
        slot = slots.pop(key, None)
        # Reinserted at the end, so the dictionary's order is least recently requested first.
        slots[key] = slot = _Slot() if slot is None else slot
        fresh = slot.value is not None and now_ns < slot.expires_ns
        if not (slot.queued or fresh or now_ns < slot.retry_at_ns):
            if len(self._pending) >= self._max_pending:
                self._refused += 1
            else:
                slot.queued = True
                self._pending.append((kind, key))
                self._wake.set()
        excess = len(slots) - self._max_cached
        if excess > 0:
            idle = (k for k, s in slots.items() if not s.queued)
            for evicted in list(itertools.islice(idle, excess)):
                del slots[evicted]

    async def _fetch[T](
        self,
        slots: dict[str, _Slot[T]],
        kind: _Kind,
        key: str,
        fetch: Callable[[str], Awaitable[T]],
    ) -> None:
        """Fetch one queued entry and settle its slot; a failure schedules a retry instead."""
        # A queued slot is never evicted, so it is still there.
        slot = slots[key]
        self._requests += 1
        try:
            value = await fetch(key)
        except (KalshiError, WireError) as exc:
            slot.failures += 1
            retry_in_s = min(
                self._retry_max_s,
                self._retry_initial_s << min(slot.failures - 1, _MAX_DOUBLINGS),
            )
            slot.retry_at_ns = int(self._clock.mono_ns()) + retry_in_s * NS_PER_S
            self._failures += 1
            self._log.warning(
                "metadata not resolved; retrying after a backoff",
                extra={
                    "kind": kind,
                    "key": key,
                    "error": repr(exc),
                    "consecutive_failures": slot.failures,
                    "retry_in_s": retry_in_s,
                },
            )
            return
        finally:
            slot.queued = False
        slot.value = value
        slot.expires_ns = int(self._clock.mono_ns()) + self._ttl_ns
        slot.failures = 0
        slot.retry_at_ns = 0

    async def _fetch_event(self, event_ticker: str) -> _EventInfo:
        response = await self._rest.event(event_ticker)
        return _EventInfo(
            title=response.event.title,
            markets={
                market.ticker: _MarketInfo(
                    subtitle=market.yes_sub_title, price_ranges=self._price_ranges(market)
                )
                for market in response.markets
            },
        )

    async def _fetch_category(self, series_ticker: str) -> str:
        return (await self._rest.series_by_ticker(series_ticker)).category

    def _price_ranges(self, market: Market) -> tuple[PriceRange, ...] | None:
        """A market's price grid in fixed point, or ``None`` if it does not parse.

        A malformed grid is counted and logged; it must not cost the rest of its event.
        """
        try:
            return tuple(
                PriceRange(
                    start_e4=parse_price(band.start),
                    end_e4=parse_price(band.end),
                    step_e4=parse_price(band.step),
                )
                for band in market.price_ranges
            )
        except FixedPointError as exc:
            self._unparsable += 1
            self._log.warning(
                "price grid not parsed; the market shows none",
                extra={"ticker": market.ticker, "error": repr(exc)},
            )
            return None
