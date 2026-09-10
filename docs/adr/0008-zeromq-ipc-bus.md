# 0008. ZeroMQ PUB/SUB over ipc between recorder and consumers

Status: accepted. Date: 2026-09-09.

## Context

The API and later the engine need the recorder's decoded events in real time. Running
them inside the recorder process would let a slow HTTP client or a strategy bug
endanger capture. Tailing segment files adds latency and couples consumers to the
storage format.

## Decision

The recorder publishes decoded events on a ZeroMQ PUB socket bound to an `ipc://`
path, topic-prefixed by ticker. Consumers subscribe with their own high-water marks.
PUB/SUB never blocks the publisher; a slow subscriber loses messages and must
resynchronize from a snapshot, which the API and engine are designed to do.

## Consequences

Strict failure isolation for the recorder. Consumers must treat the bus as lossy and
own their resync logic. A single-host assumption (ipc) is acceptable for v1; the same
code can switch to `tcp://` on localhost if a second host is ever needed.
