# 0018. The unfiltered ticker channel is live-only, not taped

Status: accepted; subscription scope amended by ADR 0027 (recorded markets only). Date: 2026-09-10.

## Context

ADR 0001 says every inbound frame is written to the tape before it is decoded, because
the recorder's data cannot be re-fetched. The design also subscribed the unfiltered
`ticker` channel, top-of-book and volume for every market on the exchange, on a control
connection so the universe selector and viewer could see the whole exchange.

The first production capture, 65 seconds at 02:15 ET on a Thursday, measured what that
costs. The unfiltered `ticker` channel was 99% of frames (506 per second) and 99% of
bytes. Order books and trades for the 50 highest-volume markets were 3.3 deltas per
second. At zstd level 3 the whole stream extrapolated to about 1.7 GB per day, almost
all of it `ticker`, and that was a quiet hour. At that rate the 200 GB free-tier disk
fills in about four months and the 10 GB free backup tier in under a week, which breaks
the project's budget.

The question is whether ADR 0001's reason applies to this channel. For markets outside
the recorded set, Kalshi serves one-minute top-of-book history through
`GET /markets/candlesticks` and its historical counterpart. For markets inside the set,
top-of-book is derivable from the recorded book. The only thing that cannot be
recovered later is sub-minute top-of-book for the long tail of markets nobody chose to
record in depth.

## Alternatives considered

1. **Tape everything.** Keeps ADR 0001 unconditional. Rejected: spends almost the whole
   storage budget on the lowest-value data, and would force paid storage.
2. **Tape a thinned ticker** (one snapshot per market every few seconds). Rejected: it
   stores a sample the project invented rather than what the exchange sent, which is the
   exact failure ADR 0001 exists to prevent, and it adds a sampler to maintain and test.
3. **Tape `ticker` only for recorded markets.** Rejected: fully redundant with the book
   and trades already recorded for those markets.
4. **Drop the channel entirely and poll REST.** Rejected: the viewer and the universe
   selector benefit from live values, and a five-minute poll makes the market picker
   stale.

## Decision

The unfiltered `ticker` channel runs on its own connection with persistence switched off
(`SupervisorConfig.persist = False`). Its frames are decoded, held as the latest value
per market in memory, and published on the bus for the API and viewer, but never written
to the tape. Every other channel, including `market_lifecycle_v2`, which is small and
essential for replay, runs on taped connections, where ADR 0001 applies without
exception.

This keeps the raw-first rule intact as a property of a connection rather than weakening
it into a per-channel judgment call: a connection is either taped, in which case nothing
it receives is decoded before it is on disk, or it is live-only, in which case nothing it
receives is stored.

## Consequences

Taped volume becomes the order-book, trade, and lifecycle traffic, roughly 1% of the
measured stream. The `tickers` baked table in docs/DATA_FORMATS.md is not produced.
Sub-minute top-of-book for markets outside the recorded set is not preserved; one-minute
history for them remains available from Kalshi. The viewer's market picker and the
universe selector read live values from memory, with the five-minute REST market poll as
the fallback after a restart.

## What would reverse it

A research question that needs sub-minute top-of-book for markets outside the recorded
set, or storage becoming abundant enough that 2 to 10 GB per day is immaterial. Kalshi
publishing sub-minute historical top-of-book would make the question moot.
