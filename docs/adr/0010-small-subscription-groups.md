# 0010. Subscription groups of at most 500 markets

Status: accepted. Date: 2026-09-09.

## Context

Kalshi assigns one subscription id (`sid`) per channel per subscribe command and
sequences messages per `sid`. A sequence gap invalidates every market in that
subscription until a snapshot arrives.

## Alternatives considered

1. **One subscription for all markets.** Fewest commands. Rejected: every gap stales
   thousands of books and triggers a resnapshot of all of them.
2. **One subscription per market.** Perfect isolation. Rejected: thousands of `sid`s to
   track, thousands of commands on every reconnect, and bookkeeping that dominates the
   hot path.
3. **Group by series.** Natural, but series sizes vary from one to hundreds of
   markets and message rates vary more; grouping by shard and observed rate balances
   load better.

## Decision

Markets are partitioned into groups of at most 500 tickers, grouped by exchange shard
and observed message rate, each subscribed separately. Membership changes via
`update_subscription`. Group size is configurable and revisited after the first
week's measurements.

## Consequences

More `sid`s to track; gaps are contained; the planner is a pure, tested component.
The command-rate limit (10,000 per second) is far above what this produces.

## What would reverse it

Week-one data. If gaps are rare, groups grow; if frequent, they shrink. The number
500 is a starting point, not a finding.
