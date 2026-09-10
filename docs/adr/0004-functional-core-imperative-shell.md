# 0004. Functional core, imperative shell

Status: accepted. Date: 2026-09-09.

## Context

The most important logic (book maintenance, fee arithmetic, exchange simulation,
strategy decisions) must be testable exhaustively and replayable deterministically.
Mixing it with sockets, disks, and clocks makes both impossible.

## Decision

Modules are classified as core, adapter, or shell. Core modules perform no I/O, read
no clock, and import only the standard library, `msgspec`, and `numpy`. Adapters
implement `Protocol`s that core code depends on. The composition root (`cli.py`)
wires them. A CI script enforces the import rule.

## Consequences

Core code is trivially unit- and property-testable and runs identically in replay,
shadow, and live. Adapters need contract tests against fakes and the real thing.
Some convenience is lost (no ambient config or logger in core code); this is
intentional.
