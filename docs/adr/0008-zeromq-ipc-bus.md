# 0008. ZeroMQ PUB/SUB over ipc between recorder and consumers

Status: accepted. Date: 2026-09-09.

## Context

The API and later the engine need the recorder's decoded events in real time. Running
them inside the recorder process would let a slow HTTP client or a strategy bug
endanger capture. Everything runs on one small host.

## Alternatives considered

1. **In-process fan-out** (asyncio queues in the recorder). Simplest. Rejected: one
   process, one failure domain; violates "the recorder is sacred".
2. **Tail the segment files.** No new dependency. Rejected: seconds of latency, and
   consumers become coupled to the storage format and rotation.
3. **Redis pub/sub.** Familiar. Rejected: another always-on service on a 12 GB free
   instance for the same lossy semantics ZeroMQ provides in-process.
4. **Kafka or NATS JetStream.** Durable, replayable, multi-host. Rejected for v1:
   operational weight far beyond the need; durability is already provided by the tape.

## Decision

The recorder publishes decoded events on a ZeroMQ PUB socket bound to an `ipc://`
path, topic-prefixed by ticker. Consumers subscribe with their own high-water marks.
PUB/SUB never blocks the publisher; a slow subscriber loses messages and must
resynchronize from a snapshot, which the API and engine are designed to do.

## Consequences

Strict failure isolation for the recorder. Consumers treat the bus as lossy and own
resync. Single-host assumption (ipc); the same code switches to `tcp://` on localhost
or a private network if a second host appears.

## What would reverse it

A second host, or a need to replay live events with delivery guarantees. The
replacement would be NATS JetStream, and the `Publisher`/`Subscriber` protocols are
the seam.
