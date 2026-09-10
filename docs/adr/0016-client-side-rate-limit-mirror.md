# 0016. Mirror Kalshi's token buckets client-side

Status: accepted. Date: 2026-09-10.

## Context

Kalshi meters the API with two token buckets per account, one for reads and one for
writes. A request costs tokens (10 by default), buckets refill at a fixed rate, and
capacity above that rate is burst headroom. Tier limits are readable at
`GET /account/limits`. A rejected request returns 429 with no `Retry-After` header and
no rate-limit headers of any kind, so the response carries no information about when
to try again.

The recorder's REST audits and the quoter's requote cycles both run continuously, so
this is not a rare edge case: without pacing, the client would discover the limit by
being refused, repeatedly.

## Alternatives considered

1. **Send and react to 429.** Simplest. Rejected: with no `Retry-After` hint the only
   recovery is guessing a backoff, which either wastes budget or thrashes; a burst of
   concurrent requests all fail together and then all retry together.
2. **A fixed delay between requests.** Trivial. Rejected: throws away the burst
   capacity Kalshi grants, so audits and requotes become needlessly slow, and it still
   breaks when several call sites run concurrently.
3. **A semaphore capping concurrency.** Rejected: concurrency is not what Kalshi
   meters. Ten cheap reads and ten order placements have very different costs.
4. **A shared limiter in an external service.** Rejected: one process holds the key,
   so there is nothing to share; it would be infrastructure for a problem that does
   not exist yet.

## Decision

Model both buckets locally. `TokenBucket` is pure: it takes the current time as an
argument, tracks tokens in billionths so refill is exact integer arithmetic, and
exposes `try_take` and `wait_ns`. `BucketRateLimiter` is the async adapter that holds
one bucket and one lock per side; the lock keeps waiters in arrival order so a late
caller cannot overtake an early one. Limits default to the documented Basic tier and
are replaced by `resize()` once `GET /account/limits` has been read. A request that
asks for more tokens than the bucket can ever hold raises rather than waiting forever.

A 429 that arrives anyway is still handled, as a `RateLimitedError` with jittered
backoff, because the mirror can drift from the server's view.

## Consequences

The client paces itself and 429s become an anomaly worth alerting on rather than
normal operation. The pure bucket is exhaustively testable without sleeping. The
mirror can drift if Kalshi changes limits mid-session, which `resize()` and the 429
path both cover.

## What would reverse it

Kalshi returning `Retry-After` or rate-limit headers, which would make reacting to
the server's own accounting more accurate than modeling it.
