# 0004. Functional core, imperative shell

Status: accepted. Date: 2026-09-09.

## Context

The most important logic (book maintenance, fee arithmetic, exchange simulation,
strategy decisions) must be exhaustively testable and replayable deterministically.
Mixing it with sockets, disks, and clocks prevents both.

## Alternatives considered

1. **Conventional layered application** (controllers, services, repositories) where
   services perform I/O. Familiar. Rejected: every test of business logic needs a
   mocked I/O layer, and determinism is not enforced by structure.
2. **Actor model** (one actor per market or connection). Good isolation. Rejected:
   message-passing overhead and ordering subtleties for a system whose core is a
   single ordered stream; harder to reason about in replay.
3. **One monolithic asyncio application.** Least code. Rejected: no boundary stops a
   slow HTTP client or a strategy bug from stalling capture.

## Decision

Modules are classified as core, adapter, or shell. Core modules perform no I/O, read
no clock, and import only the standard library, `msgspec`, and `numpy`. Adapters
implement `Protocol`s that core code depends on. The composition root wires them. A
CI script enforces the import rule.

## Consequences

Core code is trivially unit- and property-testable and runs identically in replay,
shadow, and live. Adapters need contract tests against fakes and the real thing.
There is no ambient config or logger in core code, which costs some convenience and
buys a lot of certainty.

## What would reverse it

Nothing anticipated. This is the foundation the testing strategy rests on.
