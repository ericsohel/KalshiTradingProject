"""Token buckets: exact refill, ordering under contention, and capacity guards."""

from __future__ import annotations

import asyncio

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.client.ratelimit import (
    BASIC_READ,
    BASIC_WRITE,
    DEFAULT_TOKEN_COST,
    BucketLimits,
    BucketRateLimiter,
    NullRateLimiter,
    RateLimiter,
    TokenBucket,
)
from tape.timeutil import NS_PER_S, FrozenClock, Ns

SMALL = BucketLimits(refill_per_s=10, capacity=20)


def test_bucket_starts_full_and_spends_down() -> None:
    bucket = TokenBucket(SMALL, now_ns=Ns(0))
    assert bucket.capacity == 20
    assert bucket.tokens(Ns(0)) == 20
    assert bucket.try_take(20, Ns(0)) is True
    assert bucket.tokens(Ns(0)) == 0
    assert bucket.try_take(1, Ns(0)) is False


def test_refill_is_proportional_and_capped() -> None:
    bucket = TokenBucket(SMALL, now_ns=Ns(0))
    bucket.try_take(20, Ns(0))
    assert bucket.tokens(Ns(NS_PER_S // 2)) == 5
    assert bucket.tokens(Ns(NS_PER_S)) == 10
    assert bucket.tokens(Ns(100 * NS_PER_S)) == 20


def test_wait_ns_is_zero_when_available_and_exact_otherwise() -> None:
    bucket = TokenBucket(SMALL, now_ns=Ns(0))
    assert bucket.wait_ns(20, Ns(0)) == 0
    bucket.try_take(20, Ns(0))
    # 10 tokens at 10 per second is exactly one second.
    assert bucket.wait_ns(10, Ns(0)) == NS_PER_S
    assert bucket.try_take(10, Ns(bucket.wait_ns(10, Ns(0)))) is True


def test_wait_ns_rounds_up_so_a_caller_never_wakes_early() -> None:
    bucket = TokenBucket(BucketLimits(refill_per_s=3, capacity=3), now_ns=Ns(0))
    bucket.try_take(3, Ns(0))
    delay = bucket.wait_ns(1, Ns(0))
    assert bucket.try_take(1, Ns(delay)) is True
    # One nanosecond earlier would still be short, which is what "round up" means.
    fresh = TokenBucket(BucketLimits(refill_per_s=3, capacity=3), now_ns=Ns(0))
    fresh.try_take(3, Ns(0))
    assert fresh.try_take(1, Ns(delay - 1)) is False


def test_backwards_clock_never_creates_or_destroys_tokens() -> None:
    bucket = TokenBucket(SMALL, now_ns=Ns(10 * NS_PER_S))
    bucket.try_take(20, Ns(10 * NS_PER_S))
    assert bucket.tokens(Ns(0)) == 0
    assert bucket.tokens(Ns(11 * NS_PER_S)) == 10


@pytest.mark.parametrize("tokens", [0, -1, 21])
def test_impossible_requests_raise_rather_than_wait_forever(tokens: int) -> None:
    bucket = TokenBucket(SMALL, now_ns=Ns(0))
    with pytest.raises(ValueError, match=r"tokens|capacity"):
        bucket.try_take(tokens, Ns(0))
    with pytest.raises(ValueError, match=r"tokens|capacity"):
        bucket.wait_ns(tokens, Ns(0))


@pytest.mark.parametrize(
    "limits",
    [
        BucketLimits(refill_per_s=0, capacity=10),
        BucketLimits(refill_per_s=10, capacity=0),
        BucketLimits(refill_per_s=10, capacity=5),
    ],
)
def test_invalid_limits_are_rejected(limits: BucketLimits) -> None:
    with pytest.raises(ValueError, match=r"positive|capacity"):
        TokenBucket(limits, now_ns=Ns(0))


def test_documented_basic_tier_matches_kalshi() -> None:
    assert (BASIC_READ.refill_per_s, BASIC_READ.capacity) == (200, 400)
    assert (BASIC_WRITE.refill_per_s, BASIC_WRITE.capacity) == (100, 100)
    assert DEFAULT_TOKEN_COST == 10


@given(
    st.integers(min_value=1, max_value=1000),
    st.lists(st.tuples(st.integers(0, 5 * NS_PER_S), st.integers(1, 50)), max_size=60),
)
@settings(max_examples=200)
def test_bucket_never_exceeds_capacity_and_wait_is_sufficient(
    refill: int, steps: list[tuple[int, int]]
) -> None:
    limits = BucketLimits(refill_per_s=refill, capacity=max(refill, 50))
    bucket = TokenBucket(limits, now_ns=Ns(0))
    now = 0
    for delta, cost in steps:
        now += delta
        assert bucket.tokens(Ns(now)) <= limits.capacity
        if cost > limits.capacity:
            continue
        wait = bucket.wait_ns(cost, Ns(now))
        assert bucket.try_take(cost, Ns(now + wait)) is True
        now += wait


@pytest.fixture
def instant_sleep(monkeypatch: pytest.MonkeyPatch) -> FrozenClock:
    """A clock that jumps forward by exactly what the limiter tries to sleep."""
    clock = FrozenClock()

    async def fake_sleep(seconds: float) -> None:
        clock.advance(max(0, round(seconds * NS_PER_S)))

    monkeypatch.setattr("tape.client.ratelimit.asyncio.sleep", fake_sleep)
    return clock


async def test_limiter_waits_only_when_the_bucket_is_empty(instant_sleep: FrozenClock) -> None:
    limiter = BucketRateLimiter(instant_sleep, read=SMALL, write=SMALL)
    await limiter.acquire(20, bucket="read")
    assert instant_sleep.mono_ns() == 0
    await limiter.acquire(10, bucket="read")
    assert instant_sleep.mono_ns() >= NS_PER_S


async def test_buckets_are_independent(instant_sleep: FrozenClock) -> None:
    limiter = BucketRateLimiter(instant_sleep, read=SMALL, write=SMALL)
    await limiter.acquire(20, bucket="read")
    await limiter.acquire(20, bucket="write")
    assert instant_sleep.mono_ns() == 0


async def test_concurrent_callers_are_served_in_arrival_order(
    instant_sleep: FrozenClock,
) -> None:
    limiter = BucketRateLimiter(instant_sleep, read=SMALL, write=SMALL)
    await limiter.acquire(20, bucket="read")
    order: list[int] = []

    async def caller(index: int) -> None:
        await limiter.acquire(10, bucket="read")
        order.append(index)

    await asyncio.gather(*(caller(i) for i in range(3)))
    assert order == [0, 1, 2]


async def test_resize_replaces_both_buckets(instant_sleep: FrozenClock) -> None:
    limiter = BucketRateLimiter(instant_sleep, read=SMALL, write=SMALL)
    await limiter.acquire(20, bucket="read")
    limiter.resize(read=BASIC_READ, write=BASIC_WRITE)
    await limiter.acquire(400, bucket="read")
    assert instant_sleep.mono_ns() == 0


async def test_limiter_rejects_a_cost_no_bucket_could_ever_serve(
    instant_sleep: FrozenClock,
) -> None:
    limiter = BucketRateLimiter(instant_sleep, read=SMALL, write=SMALL)
    with pytest.raises(ValueError, match="capacity"):
        await limiter.acquire(21, bucket="read")


async def test_null_limiter_never_waits_but_still_validates() -> None:
    limiter: RateLimiter = NullRateLimiter()
    await limiter.acquire(10_000, bucket="write")
    limiter.resize(read=BASIC_READ, write=BASIC_WRITE)
    with pytest.raises(ValueError, match="positive"):
        await limiter.acquire(0, bucket="read")
