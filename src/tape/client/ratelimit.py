"""Client-side mirror of Kalshi's token buckets (ADR 0016).

Kalshi meters the API with two token buckets per account, one for reads and one for
writes. Each request costs tokens (10 by default), buckets refill at a fixed rate, and
capacity above the refill rate is burst headroom. A 429 carries no ``Retry-After``, so
the only way to behave well is to model the buckets locally and wait before sending.

``TokenBucket`` is pure: it never reads a clock, so it is exhaustively testable.
``RateLimiter`` is the async adapter that actually sleeps.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Final, Literal, Protocol

import msgspec

from tape.timeutil import NS_PER_S, Clock, Ns

__all__ = [
    "BASIC_READ",
    "BASIC_WRITE",
    "DEFAULT_TOKEN_COST",
    "Bucket",
    "BucketLimits",
    "BucketRateLimiter",
    "NullRateLimiter",
    "RateLimiter",
    "TokenBucket",
]

Bucket = Literal["read", "write"]

DEFAULT_TOKEN_COST: Final = 10
"""Tokens charged for a request Kalshi does not price differently."""


class BucketLimits(msgspec.Struct, frozen=True, kw_only=True):
    """Refill rate and capacity of one bucket, as reported by ``GET /account/limits``."""

    refill_per_s: int
    capacity: int


BASIC_READ: Final = BucketLimits(refill_per_s=200, capacity=400)
"""Basic tier read bucket: 200 tokens per second with two seconds of burst."""

BASIC_WRITE: Final = BucketLimits(refill_per_s=100, capacity=100)
"""Basic tier write bucket: 100 tokens per second with one second of capacity."""


class TokenBucket:
    """A refilling token bucket. Pure: every method takes the current time.

    Args:
        limits: Refill rate and capacity.
        now_ns: Time at which the bucket starts full.

    Raises:
        ValueError: If the refill rate or capacity is not positive, or capacity is
            below the refill rate, which Kalshi's model never produces.
    """

    __slots__ = ("_capacity", "_last_ns", "_refill_per_s", "_tokens_e9")

    def __init__(self, limits: BucketLimits, *, now_ns: Ns) -> None:
        if limits.refill_per_s <= 0 or limits.capacity <= 0:
            raise ValueError(f"bucket limits must be positive: {limits}")
        if limits.capacity < limits.refill_per_s:
            raise ValueError(f"capacity below refill rate: {limits}")
        self._refill_per_s = limits.refill_per_s
        self._capacity = limits.capacity
        # Tokens are tracked in billionths so refill is exact integer arithmetic.
        self._tokens_e9 = limits.capacity * NS_PER_S
        self._last_ns = int(now_ns)

    @property
    def capacity(self) -> int:
        """Maximum tokens the bucket holds."""
        return self._capacity

    def tokens(self, now_ns: Ns) -> int:
        """Whole tokens available at ``now_ns``, after refilling."""
        self._refill(now_ns)
        return self._tokens_e9 // NS_PER_S

    def try_take(self, tokens: int, now_ns: Ns) -> bool:
        """Take ``tokens`` if they are available at ``now_ns``.

        Returns:
            ``True`` when the tokens were taken, ``False`` when the bucket is short.

        Raises:
            ValueError: If ``tokens`` is not positive or exceeds the bucket capacity,
                which could never succeed and would deadlock a caller that waits.
        """
        self._check(tokens)
        self._refill(now_ns)
        cost_e9 = tokens * NS_PER_S
        if self._tokens_e9 < cost_e9:
            return False
        self._tokens_e9 -= cost_e9
        return True

    def wait_ns(self, tokens: int, now_ns: Ns) -> Ns:
        """Nanoseconds until ``tokens`` are available; zero when they already are.

        Raises:
            ValueError: If ``tokens`` is not positive or exceeds the capacity.
        """
        self._check(tokens)
        self._refill(now_ns)
        shortfall_e9 = tokens * NS_PER_S - self._tokens_e9
        if shortfall_e9 <= 0:
            return Ns(0)
        # Round up so the caller never wakes a nanosecond early.
        return Ns(-((-shortfall_e9) // self._refill_per_s))

    def _check(self, tokens: int) -> None:
        if tokens <= 0:
            raise ValueError(f"tokens must be positive, got {tokens}")
        if tokens > self._capacity:
            raise ValueError(f"request of {tokens} tokens exceeds capacity {self._capacity}")

    def _refill(self, now_ns: Ns) -> None:
        elapsed = int(now_ns) - self._last_ns
        if elapsed <= 0:
            # A clock that does not advance simply adds nothing; it never removes tokens.
            self._last_ns = max(self._last_ns, int(now_ns))
            return
        self._last_ns = int(now_ns)
        self._tokens_e9 = min(
            self._capacity * NS_PER_S, self._tokens_e9 + elapsed * self._refill_per_s
        )


class RateLimiter(Protocol):
    """Blocks a caller until a request's tokens are available."""

    async def acquire(self, cost: int, *, bucket: Bucket) -> None:
        """Wait until ``cost`` tokens can be taken from ``bucket``, then take them."""
        ...

    def resize(self, *, read: BucketLimits, write: BucketLimits) -> None:
        """Replace both buckets, for example after reading ``GET /account/limits``."""
        ...


class BucketRateLimiter:
    """Two token buckets with a lock per bucket so concurrent callers queue fairly.

    Args:
        clock: Time source; only this object reads a clock (ADR 0004).
        read: Read bucket limits. Defaults to the Basic tier.
        write: Write bucket limits. Defaults to the Basic tier.
        sleep: Waits the given seconds while a bucket refills, so that pacing can be tested in
            virtual time; ``asyncio.sleep`` by default, looked up when the limiter is built.
    """

    def __init__(
        self,
        clock: Clock,
        *,
        read: BucketLimits = BASIC_READ,
        write: BucketLimits = BASIC_WRITE,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._clock = clock
        self._sleep = asyncio.sleep if sleep is None else sleep
        self._buckets: dict[Bucket, TokenBucket] = {
            "read": TokenBucket(read, now_ns=clock.mono_ns()),
            "write": TokenBucket(write, now_ns=clock.mono_ns()),
        }
        self._locks: dict[Bucket, asyncio.Lock] = {"read": asyncio.Lock(), "write": asyncio.Lock()}

    async def acquire(self, cost: int, *, bucket: Bucket) -> None:
        """Wait until ``cost`` tokens are available in ``bucket``, then take them.

        The lock keeps callers in arrival order, so a burst of requests does not let a
        late caller overtake an early one that is already waiting.

        Raises:
            ValueError: If ``cost`` is not positive or exceeds the bucket capacity.
        """
        target = self._buckets[bucket]
        async with self._locks[bucket]:
            while not target.try_take(cost, self._clock.mono_ns()):
                delay_ns = target.wait_ns(cost, self._clock.mono_ns())
                await self._sleep(delay_ns / NS_PER_S)

    def resize(self, *, read: BucketLimits, write: BucketLimits) -> None:
        """Replace both buckets with freshly filled ones at the new limits."""
        now = self._clock.mono_ns()
        self._buckets = {
            "read": TokenBucket(read, now_ns=now),
            "write": TokenBucket(write, now_ns=now),
        }


class NullRateLimiter:
    """A limiter that never waits. For tests and for replaying recorded traffic."""

    async def acquire(self, cost: int, *, bucket: Bucket) -> None:
        """Return immediately.

        Raises:
            ValueError: If ``cost`` is not positive, so tests still catch bad costs.
        """
        if cost <= 0:
            raise ValueError(f"tokens must be positive, got {cost}")
        _ = bucket

    def resize(self, *, read: BucketLimits, write: BucketLimits) -> None:
        """Ignore new limits."""
        _ = (read, write)
