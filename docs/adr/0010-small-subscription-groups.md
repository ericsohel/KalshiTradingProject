# 0010. Subscription groups of at most 500 markets

Status: accepted. Date: 2026-09-09.

## Context

Kalshi assigns one subscription id (`sid`) per channel per subscribe command and
sequences messages per `sid`. A sequence gap invalidates every market in that
subscription until a snapshot arrives. A single subscription covering thousands of
markets would turn every gap into a large stale epoch and a large resnapshot.

## Decision

Markets are partitioned into groups of at most 500 tickers, grouped by exchange shard
and observed message rate, each subscribed separately. Group membership changes via
`update_subscription` rather than resubscription. Group size is configurable and
revisited after the first week's measurements.

## Consequences

More subscribe commands and more `sid`s to track; gaps are contained; the planner
must be a pure, tested component. The command-rate limit (10,000 per second) is far
above what this produces.
