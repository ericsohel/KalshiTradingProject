# 0001. Record raw frames before decoding

Status: accepted. Date: 2026-09-09.

## Context

The recorder's data cannot be recreated: Kalshi exposes no historical order-book
depth and keeps only about three months of live data. Any bug in decoding, book
maintenance, or storage that runs before bytes reach disk loses data permanently.
Kalshi's API changes often (306 changelog entries in 17 months), so decoders will
break at some point.

## Alternatives considered

1. **Decode, then store typed rows only.** Smallest storage, immediately queryable.
   Rejected: a decoder bug or an unannounced field change silently corrupts or drops
   data with no way back.
2. **Store raw and typed synchronously on the hot path.** Complete, but doubles the
   work done between socket read and the next read, which is exactly where the
   server's buffer-overflow error (code 25) is triggered.
3. **Rely on Kalshi's candles and trade history.** No engineering cost. Rejected: no
   depth, one-minute granularity, and the live window is three months.

## Decision

Every inbound frame is appended to the raw segment byte-for-byte with local receive
timestamps before anything beyond the envelope (`type`, `sid`, `seq`) is decoded.
Decoding and book maintenance happen after the frame is queued for disk. Baking to
typed tables is a separate, repeatable batch process.

## Consequences

Storage is roughly two to three times a decoded-only format (mitigated by zstd).
Every downstream artifact can be regenerated. A decoder change never requires
re-recording. The tape format is trivially simple, which is what makes it durable.

## What would reverse it

Kalshi shipping a historical depth API, or storage becoming the binding constraint
after decoders have been proven correct for months. Even then, raw retention for
showcase markets would stay.
