# 0001. Record raw frames before decoding

Status: accepted. Date: 2026-09-09.

## Context

The recorder's data cannot be recreated: Kalshi exposes no historical order-book
depth. Any bug in decoding, book maintenance, or storage logic that runs before bytes
reach disk would lose data permanently. Kalshi's API changes often (306 changelog
entries in 17 months), so decoders will break.

## Decision

Every inbound WebSocket frame is appended to the raw segment, byte-for-byte, with
local receive timestamps, before any decoding beyond the envelope fields needed for
gap detection. Decoding and book maintenance happen after the frame is queued for
disk. Baking is a separate, repeatable process over the raw segments.

## Consequences

Storage is larger than a decoded-only format (mitigated by zstd). Every downstream
artifact can be regenerated. A decoder change never requires re-recording. The tape
format is trivially simple, which makes it durable.
