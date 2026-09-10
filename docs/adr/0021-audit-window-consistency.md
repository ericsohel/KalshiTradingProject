# 0021. An audit passes when the snapshot matches any book state in its request window

Status: accepted. Date: 2026-09-10. Refines the audit definition in docs/TESTING.md 7.

## Context

The auditor fetches a market's order book over REST and compares it with the recorder's
book at the moment the response arrives. The share of exact matches is one of the three
integrity numbers the project publishes.

The second production smoke test audited 177 markets 708 times and matched 690, 97.5%.
Grouped by how busy each market was over the run, the pattern is unambiguous:

| Order-book updates per second | Exact audits |
|---|---|
| under 1 | 622 of 624 (99.7%) |
| 1 to 10 | 42 of 44 (95.5%) |
| 10 or more | 26 of 40 (65.0%) |

The busiest market, a 15-minute Bitcoin contract at 738 updates a second, mismatched all 4
times. A pricing or book-maintenance bug would hit quiet markets as hard as busy ones; this
tracks activity, which means updates landing during the REST round trip. The REST snapshot
reflects the exchange at some instant between sending the request and receiving the reply,
and the live feed may deliver that instant slightly earlier or later than the reply.

Two consequences make the naive definition unacceptable. The published number would
punish recording the markets that matter most, and a genuine error on a busy market would be
indistinguishable from ordinary timing, so the audit could not do its job exactly where the
tape is densest.

## Alternatives considered

1. **Keep the comparison and report it by activity bucket.** Honest, cheap. Rejected: a real
   error on a busy market still hides among timing mismatches.
2. **Audit only quiet markets.** Rejected: it stops checking the markets with the most data
   and the most money traded.
3. **Re-fetch a mismatching book once.** Rejected: a busy book keeps moving, so the second
   comparison is confounded the same way, and it doubles the REST cost.
4. **Window consistency.** Chosen.

## Decision

The auditor records the local book when the request is sent and every delta the recorder
applies to that market until a short settling period after the reply arrives. The REST
snapshot is **consistent** if it equals the book at the start of that window or after any of
those deltas. The window begins a small allowance before the request is sent and ends a small
allowance after the reply arrives, both configurable and both recorded, to absorb the live
feed arriving slightly ahead of or behind the snapshot.

Every audit is classified as exactly one of:

- **exact**: equal at the moment the reply arrived, as today;
- **consistent**: not equal then, but equal to some state inside the window;
- **inconsistent**: equal to no state in the window, which is a finding to investigate.

The published integrity number is `(exact + consistent) / sampled`, kept as an integer
pair. Inconsistent audits are reported separately and always carry both level lists in the
tape. Each audit record also carries the request's send and receive times and, when
consistent, how many deltas into the window the match occurred, so the window can be
re-evaluated offline from the tape.

## Consequences

"Mismatch explained by in-flight updates" becomes a checked property instead of a
presumption, so an inconsistent audit on any market, busy or quiet, is a real signal. The
auditor needs a bounded per-market delta tap from the supervisor for the sampled markets and
holds each judgment open for the settling period. Memory is bounded by the sample size times
the deltas in one window, a few thousand comparisons even for the busiest market observed.

## What would reverse it

A REST order-book response that carried the live feed's sequence number, which would allow
comparing at exactly one known state instead of searching a window.
