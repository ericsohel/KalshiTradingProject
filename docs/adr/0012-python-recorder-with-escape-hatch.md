# 0012. Python and asyncio for the recorder, with a measured escape hatch

Status: accepted. Date: 2026-09-09.

## Context

A market-data recorder is the kind of component people reach for Rust or Go to build.
The owner is a solo developer learning as the project progresses, the rest of the
system (simulation, analysis, API) is naturally Python, and the actual load is
unmeasured: a few thousand subscribed markets producing JSON frames of a few hundred
bytes each.

## Alternatives considered

1. **Rust.** Fastest, safest, and the industry answer for feed handlers. Rejected for
   v1: the learning curve would delay the recorder by weeks, and the tape compounds
   daily; a slower recorder started now beats a faster one started later.
2. **Go.** Excellent concurrency, easy deployment. Rejected: splits the codebase, and
   the data-science half of the project would still be Python.
3. **Node.** Good WebSocket ecosystem. Rejected: no advantage over Python for this
   workload and a worse fit for the analytical half.

## Decision

Python 3.12 with `asyncio`, `uvloop`, `websockets`, and `msgspec`. The hot path per
frame is deliberately tiny: read, enqueue raw bytes to a writer thread, decode three
envelope fields, check the sequence number. Full decoding and book maintenance happen
after the frame is safe. Connections are spread across processes if one process
saturates. Message rates, decode latency, queue depth, and drop counts are metrics
from day one.

## Consequences

Faster delivery and one language. Throughput is bounded by one Python core per
process; the design accepts this and measures it. `msgspec` decodes JSON at roughly
one to two million small messages per second per core, which is far above the
expected per-connection rate, but that expectation is verified in week one, not
assumed.

## What would reverse it

Measured saturation: writer-queue drops, sustained p99 decode latency above a few
milliseconds, or Kalshi buffer-overflow errors that adding connections and processes
cannot cure. The escape hatch is a Rust extension (PyO3) for the frame loop and book
apply, behind the same `WsSession` and `Book` interfaces, not a rewrite.
