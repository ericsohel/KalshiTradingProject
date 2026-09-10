# Testing strategy

Tests exist to make the honesty claims in the README true. The pyramid below runs
from milliseconds to days.

## 1. Unit tests

Every pure function and every `core` class. Fast, deterministic, no I/O. Fixed-point
codecs, book operations, fee arithmetic, gap tracking, subscription planning, the
simulator's matching rules, and the strategy's quoting math each have dedicated test
modules.

## 2. Property-based tests (`hypothesis`)

| Property | Module |
|---|---|
| `format(parse(s)) == canonical(s)` and `parse(format(x)) == x` for all valid strings | `fixedpoint` |
| Applying a random sequence of deltas then a snapshot equals applying the snapshot alone | `book` |
| Applying deltas in any interleaving that preserves per-level order yields the same book | `book` |
| `checksum` is order-independent and changes for any single-level change | `book` |
| Fee is monotone in count, symmetric in `P` and `1-P`, zero at `P` in {0, 1}, and within Kalshi's published range | `fees` |
| `GapTracker` reports a gap exactly when `seq` skips or decreases | `recorder` |
| `SubscriptionPlanner.diff(plan(a), plan(b))` applied to `a` yields `b` | `recorder` |
| Segment writer then reader round-trips any record sequence, including a truncated tail | `segment` |
| Simulator never fills a post-only order at a crossing price; never fills after close | `sim` |

## 3. Contract tests

- **Wire structs against spec examples.** Every example payload in the pinned OpenAPI
  and AsyncAPI documents decodes into its struct without error; a CI job re-downloads
  the specs weekly, diffs them, and fails on schema drift so the change is reviewed.
- **Protocol contracts.** For each `Protocol` in [INTERFACES.md](INTERFACES.md), one
  parametrized test suite runs against the real adapter (when credentials exist) and
  against the fake used by other tests. The fake cannot diverge from reality unnoticed.
- **Public API.** The FastAPI OpenAPI document is committed; a test fails if a change
  alters it without a version bump, and the TypeScript types are regenerated from it.

## 4. Recorder tests with a fake exchange

A local WebSocket server (`tests/fakes/fake_kalshi_ws.py`) speaks the protocol in
[DATA_FORMATS.md](DATA_FORMATS.md): heartbeat pings, subscribe/ok/error responses,
sequenced snapshots and deltas, and scripted faults (gap, disconnect, buffer-overflow
error, slow drain). Tests assert that the recorder writes every frame before decoding,
records a gap marker, issues `get_snapshot`, resubscribes after reconnect from its own
table, and never blocks the socket reader.

## 5. Replay determinism

Replay a fixture tape through the engine twice with the same strategy and configuration;
assert identical intent hashes and identical simulator fills. Replay it with a
different fill model; assert only fills differ. This test runs on every PR.

## 6. Integration tests (opt-in)

Marked `integration` and skipped unless `TAPE_TEST_ENV=demo|prod` and keys are
configured. Demo: order placement, expiration, cancel, cancel-all, and the fill
channel round trip. Production, public only: `GET /exchange/status`, market pagination,
one orderbook fetch, and a 30-second WebSocket capture asserting heartbeat handling
and at least one sequenced message.

## 7. Production integrity metrics (continuous tests)

These are tests that run against reality every day and are published:

| Metric | Definition | Target |
|---|---|---|
| Uptime | seconds with an open control connection / seconds in the day | >= 99.5% |
| Gap share | market-seconds with a book marked stale / total subscribed market-seconds | < 0.5% |
| Audit consistency ratio | sampled books that equal the REST snapshot at the reply, or at some state within the request window (ADR 0021) / sampled books | >= 99.5%; every inconsistent audit investigated |
| Impossible-trade rate | trades that could not have executed at the reconstructed best price at `ts_ms` / trades | < 0.1% |
| Candle agreement | 1-minute candles rebuilt from recorded trades that match Kalshi's `volume_fp` and OHLC | >= 99.5% |

A day is "green" when every metric meets its target; the status page shows green
days per month.

## 8. Calibration tests (probe stage)

Fee engine: `fee_cost` on every real fill equals the model to the micro-dollar; any
discrepancy becomes a regression test with the fill as a fixture. Fill model: over
the probe sample, realized fills lie within the pessimistic-optimistic band on at
least 95% of orders, and the calibrated model's predicted fill probability is within
10 percentage points of realized in every decile.

## 9. Coverage thresholds

| Package | Line coverage |
|---|---|
| `fixedpoint`, `book`, `fees`, `sim`, `engine`, `recorder` (pure parts) | >= 95% |
| `client`, `segment`, `bake`, `store`, `api`, `bus` | >= 85% |
| Repository total | >= 90% |

Coverage is a floor, not a goal; a test that does not assert behavior does not count.

## 10. Performance tests

Benchmarks (`pytest-benchmark`, run on demand and nightly): book delta application
(target > 2,000,000 per second per core), raw frame append (> 200,000 per second),
segment read and decode (> 500,000 records per second), and a one-day, 100-market
replay in under five minutes on the production ARM instance.
