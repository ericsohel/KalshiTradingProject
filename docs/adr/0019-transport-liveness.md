# 0019. Liveness is measured at the transport, not by data traffic

Status: accepted. Date: 2026-09-10. Refines ADR 0001's operating assumptions for
connections, not its recording rule.

## Context

`WsSession` declared a connection dead after 30 seconds without an inbound data frame.
Kalshi's heartbeat pings, every 10 seconds, are answered by the `websockets` library and
never surface as frames, so the check could only see market traffic.

The first production smoke test on 2026-09-10 showed what that costs. Two order-book
connections had no markets assigned yet, and the lifecycle connection carries a handful
of events a minute. All three were declared dead every 30 seconds, reconnected, and
rotated to new segment files that contained only open and close records. The same would
happen overnight to any group of quiet markets. A reconnect marks books stale and costs
a resnapshot, so a false death is not free.

## Alternatives considered

1. **Keep the data-silence check with a longer timeout.** Rejected: any finite timeout
   is wrong for a connection that is legitimately idle, such as one with no markets yet,
   and a long one detects a real failure too late.
2. **Surface the server's pings and count them as traffic.** Rejected: the library does
   not expose received pings through a public interface, and depending on its internals
   would break silently on upgrade.
3. **Skip the check for connections with no subscriptions.** Rejected: fixes the empty
   book connections but not a low-volume channel, and still treats traffic as liveness.
4. **Client keepalive pings with a pong deadline.** Chosen. It measures exactly what
   liveness means, that the other end is still there and responding, independent of how
   much the market is trading.

## Decision

Every session sends a ping every 10 seconds and closes the connection if no pong arrives
within 20 seconds, using the `websockets` library's own keepalive. A missed pong surfaces
as `WsClosedError`, like any other close, and the supervisor reconnects as before.

The data-silence timeout becomes optional and is disabled by default. It stays enabled,
at 60 seconds, only on the live-only unfiltered `ticker` connection, which always
carries hundreds of messages a second; there, silence with a healthy transport means the
subscription itself stopped delivering, a failure keepalive cannot see.

## Consequences

Idle and quiet connections stay connected. A dead peer is detected within about 35 seconds regardless of traffic: the 10-second
ping interval, the 20-second pong deadline, and the library's 5-second close timeout. The client sends a small ping every 10 seconds per
connection, which is negligible. The regression test that pins `recv` cancellation
safety still matters for the one connection that polls for silence.

## What would reverse it

Kalshi exposing an application-level heartbeat message that arrives as a data frame, or
the library exposing received pings publicly, would allow liveness from the server's own
heartbeat instead of the client's.
