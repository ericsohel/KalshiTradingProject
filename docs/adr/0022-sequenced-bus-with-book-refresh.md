# 0022. Live consumers recover from a sequenced bus and periodic book refreshes

Status: accepted. Date: 2026-09-10.

## Context

ADR 0008 makes the bus lossy on purpose: the recorder's PUB socket never blocks, so a
slow subscriber loses messages. A consumer that builds books from deltas therefore
needs two things the bus does not yet give it: a way to notice that it lost a message,
and a way to get a correct book back without the recorder serving requests. The same
need arises when the API starts while the recorder is already running, which will be
the normal case, because the recorder restarts rarely and the API restarts whenever it
is deployed. Kalshi's per-subscription sequence numbers do not cover this: they
describe the exchange's stream, not what reached a given subscriber, and trades,
lifecycle events, and ticker updates travel on subscriptions of their own.

## Alternatives considered

1. **Snapshot requests to the recorder** (ZeroMQ REQ/REP or ROUTER). A new consumer
   has a book immediately. Rejected: it puts a request path driven by outside demand
   into the recorder's event loop, so a burst of viewers becomes recorder work, which
   is exactly what ADR 0008 exists to prevent.
2. **Bootstrap from keyframes on disk, then splice buffered bus events.** No recorder
   change. Rejected: keyframes are written every 300 s, splicing disk state into a live
   stream needs per-book sequence bookkeeping across files, and the API becomes coupled
   to the storage layout, the reason ADR 0008 rejected tailing segments.
3. **The API fetches order books from Kalshi's REST API.** Instant and independent of
   the recorder. Rejected: the API would need network access to the exchange and would
   share the host's rate budget, and a REST book carries no sequence number to splice
   with bus deltas, the very problem ADR 0021 had to work around for audits.
4. **Replayable log (NATS JetStream, Kafka).** Rejected for the reasons in ADR 0008.

## Decision

The incremental-plus-refresh pattern that exchange market data feeds use for recovery:

- **Sequenced envelope.** Every bus message carries `bus_epoch` (the publisher's start
  time, wall ns) and `bus_seq` (a counter starting at 1 per epoch, incremented for every
  message the publisher attempts to send, including messages ZeroMQ then drops).
  A consumer that sees a gap in `bus_seq` or a new `bus_epoch` knows its state is
  suspect.
- **Everything live, nothing new.** The recorder publishes each event its supervisors
  already hand to `on_event`: book snapshots and applied deltas, trades, ticker updates,
  lifecycle events, and gaps. Publishing sits after book apply and never touches the
  tape path.
- **Book refresh cycle.** Every `bus_refresh_s` (default 10 s) the recorder publishes a
  refresh image of every book it holds, paced in slices across the interval so the
  cycle never becomes a burst. A refresh image is the recorder's live book, taken
  between frames on its event loop, so it reflects exactly the deltas with a lower
  `bus_seq`. It also states whether the book is stale.
- **Consumer rule.** On a sequence gap or epoch change a consumer marks every book
  unknown and tells its own clients to resynchronize; each book becomes known again at
  its next refresh image, and deltas with a higher `bus_seq` apply on top. A consumer
  that has just started behaves the same way.
- **Never at the recorder's expense.** Publishing is non-blocking with a bounded
  high-water mark. Send failures and drops are counted in the recorder's status and
  never raised.

## Consequences

A freshly started API, or one that lost messages, serves a market within one refresh
interval rather than immediately; a viewer sees a resync state for at most about 10 s.
The refresh cycle adds bus traffic on the order of 0.1 MB/s at 2,000 books with tens of
levels each, local to the host. The recorder gains a pyzmq dependency and one periodic
task, and deploying the publisher requires one recorder restart, which the tape records
as a gap. Consumers own all recovery logic and can be restarted freely.

## What would reverse it

Viewers needing a cold API to serve instantly across many markets, or a measurable
effect of the refresh cycle on capture latency. The replacement would be a separate
book-cache process that subscribes to the bus and answers snapshot requests, keeping
request handling out of the recorder.
