# 0020. One market set per channel per connection

Status: accepted. Date: 2026-09-10. Supersedes ADR 0010.

## Context

ADR 0010 split the recorded universe into groups of at most 500 markets, each subscribed
with its own `subscribe` command, on the assumption that each command yields its own
subscription id per channel. The point was isolation: a sequence gap would invalidate
only the group whose `sid` skipped.

A production smoke test on 2026-09-10 showed the assumption is false. On one connection
the recorder sent `subscribe` for `orderbook_delta` and `trade` with 174 markets, and
received two `subscribed` responses carrying new `sid`s. It then sent a second
`subscribe` for the same channels with 23 more markets, which the planner had put in a
separate group because they were on a different exchange shard. Kalshi did not create new
subscriptions. It merged the 23 markets into the existing ones and answered with two `ok`
responses, each listing all 197 markets. This behavior is not in the published AsyncAPI
specification, which describes `ok` only as the response to `update_subscription`.

The recorder expected `subscribed` for the second command, so it never built books for
those 23 markets, and the command stayed pending forever, which also deferred every later
universe change on that connection. The raw frames for all 200 markets were still on the
tape, because frames are written before anything interprets them (ADR 0001), so nothing
was lost that replay cannot rebuild.

## Alternatives considered

1. **Keep several groups per connection and treat an `ok` to `subscribe` as a merge.**
   Fixes the stranded markets. Rejected as the model: the groups would share `sid`s, so the
   isolation they exist for is already gone, and the code would keep a concept the exchange
   does not have.
2. **One connection per group.** Restores per-group isolation by construction. This is in
   effect what the decision below does, stated in terms of connections rather than groups.
3. **Server-side sharding with `shard_factor` and `shard_key`.** The specification's error
   codes show the subscribe command accepts these. They split an unfiltered stream across
   connections, which may help scale the ticker firehose later, but they do not address
   per-market book subscriptions.

## Decision

Each order-book connection carries exactly one subscription per book channel, and its
membership is the connection's whole market set. The planner assigns markets to
connections, at most `group_size` per connection, keeping each market on its connection
across replans because moving one costs a resnapshot. Markets on different exchange shards
may share a connection: shards matter for collateral and order routing, not for market
data, and the exchange accepted exactly such a merged subscription.

Membership changes after the first subscribe use `update_subscription`. The supervisor also
handles an `ok` reply to `subscribe` as a merge into the existing `sid`s, so an unexpected
merge can never strand markets again, and a subscribe that receives no reply within a
deadline is treated as a connection failure rather than left pending. Book frames are
routed by membership in the connection's market set, not by which command added a market.

A sequence gap therefore stales every book on its connection and requests snapshots for all
of that connection's markets. The recorder needs at least `ceil(max_l2_markets / group_size)`
book connections, and configuration validation enforces it.

## Consequences

Isolation is per connection, bounded by `group_size`, which is the same bound ADR 0010
intended. The recorder uses more connections for a large universe: 2,000 markets at 500 per
connection need four. The fake exchange used in tests must reproduce the real merge
behavior, because it previously created new `sid`s for every subscribe, which is why the
test suite did not catch this. The per-subscription market limit behind error 26 is still
unknown; exceeding it surfaces as that error.

## What would reverse it

Kalshi documenting, and the live exchange showing, independent subscriptions for the same
channel on one connection.
