# 0005. Event-sourced engine with decision hashing

Status: accepted. Date: 2026-09-09.

## Context

A backtest is only credible if the code that ran in the backtest is the code that
runs live. Most trading projects diverge here and then cannot explain live results.

## Decision

The engine is a single-threaded loop over a totally ordered event stream. Strategies
see only `Event`s through a `StrategyContext` and emit only `Intent`s. Execution
gateways (live, shadow, simulated) turn intents into events. Time is `ctx.now_ns()`,
derived from the event stream. A blake2b hash over emitted intents is logged hourly;
replaying the same tape with the same configuration must reproduce it.

## Consequences

Live, shadow, and replay are one code path. Any nondeterminism (a strategy reading
the clock, iterating a set) is caught by the hash test. Latency modeling has to be
explicit because the loop itself has none.
