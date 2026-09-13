# 0027. The ticker channel covers the recorded markets, not every market

Status: accepted. Date: 2026-09-12. Amends ADR 0018 (scope of the ticker subscription)
and ADR 0019 (data-silence timeout).

## Context

ADR 0018 subscribes the `ticker` channel with no market filter, on a live-only
connection. It keeps each market's latest value in memory for 24 hours and publishes
every update on the bus. ADR 0019 gives that connection alone a data-silence timeout,
because an unfiltered channel always carries traffic.

Measured on the production host (ADR 0024) after about five hours:

- the latest-value table held 517,739 markets
- the recorder's resident memory was about 516 MB of the host's 892 MB, and the host
  was swapping
- the API process used about half a vCPU decoding updates, and load averaged about 3
  on two vCPUs
- the ticker connection had reconnected 23 times after keepalive timeouts, carrying
  about 1,250 frames per second

Only recorded markets' values are ever read. The API's market rows and the live feed's
ticker messages concern recorded markets, and nothing else reads the table.

## Alternatives considered

1. **Shorter retention.** Bounds memory but not CPU: every frame is still parsed by the
   recorder and again by the API. Rejected on its own.
2. **Filter updates in the recorder before publishing.** Saves the API's CPU and the bus
   traffic, but not the recorder's parsing. Rejected.
3. **Drop the ticker channel.** Loses 24-hour volume, which books and trades do not
   provide. Rejected.
4. **Subscribe the ticker channel for the recorded markets only**, and update the
   subscription whenever the universe changes. Chosen.

## Decision

- The live-only connection subscribes `ticker` with `market_tickers` set to the markets
  of the current plan. When a universe refresh changes the plan, the subscription is
  updated to match. Batches follow the merge behavior recorded in ADR 0020, so the
  connection keeps one ticker subscription.
- The latest-value table holds only markets in the current universe. A market that leaves
  the universe is dropped from it at the refresh that removes it.
- The ticker connection no longer has a data-silence timeout. A filtered channel can be
  quiet at night, so that connection relies on transport keepalive, like every other
  connection (ADR 0019).
- There is no setting to restore the unfiltered subscription; no feature needs it.

## Consequences

- **Memory** for ticker values grows with the recorded universe (hundreds to a few
  thousand entries), not with every market on the exchange.
- **CPU:** the recorder and the API parse only the updates of recorded markets.
- **Universe changes** cost subscription updates on the live-only connection, which is
  never taped.
- **Unrecorded markets** have no live ticker values, and nothing displays them.
- **Staleness detection:** without the silence timeout, a ticker connection that stays
  connected but stops sending is noticed only by the absence of updates, just as on the
  other connections.

## What would reverse it

A feature that needs live prices for markets outside the recorded universe, such as
ranking the whole exchange by live volume. That would call for a sampled or REST-based
source, not the unfiltered channel on a 1 GB host.
