# 0005. Event-sourced engine with decision hashing

Status: accepted. Date: 2026-09-09.

## Context

A backtest is only credible if the code that ran in the backtest is the code that
runs live. Most trading projects diverge here (a vectorized backtester and a separate
live bot) and then cannot explain why live results differ.

## Alternatives considered

1. **Separate backtester and live bot.** Fastest to write; vectorized pandas
   backtests are quick. Rejected: two implementations of every rule, guaranteed
   drift, and no way to prove the live system does what the backtest did.
2. **A general backtesting framework** (backtrader, vectorbt, nautilus). Mature.
   Rejected: none model Kalshi's binary settlement, post-only cross cancel, queue
   rules, fee types with scheduled changes, or token-bucket limits; adapting one is
   more work than a purpose-built loop and hides the semantics that matter.
3. **Multi-threaded engine** for parallelism across markets. Rejected for v1:
   nondeterministic interleavings defeat the hash proof; one core handles the
   expected event rates.

## Decision

The engine is a single-threaded loop over a totally ordered event stream. Strategies
see only `Event`s through a `StrategyContext` and emit only `Intent`s. Gateways
(live, shadow, simulated) turn intents into events. Time is derived from the stream. A
blake2b hash over emitted intents is logged hourly and must reproduce on replay.

## Consequences

Live, shadow, and replay are one code path. Any nondeterminism is caught by a test.
Latency must be modeled explicitly because the loop has none. Strategy code is
constrained (no clock, no randomness, no I/O), which is a feature.

## What would reverse it

Needing more throughput than one core provides. The path then is sharding by market
group, each shard a deterministic loop with its own hash, not a threaded loop.
