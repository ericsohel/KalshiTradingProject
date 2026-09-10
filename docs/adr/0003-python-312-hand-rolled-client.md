# 0003. Python 3.12 with a spec-pinned, hand-rolled client

Status: accepted. Date: 2026-09-09.

## Context

The project needs async, WebSocket-first access with a client-side token-bucket
mirror, exact fixed-point codecs, and raw-frame access. Kalshi publishes OpenAPI and
AsyncAPI specs and advises production users to generate their own client because the
SDK may lag.

## Alternatives considered

1. **Official `kalshi_python_async`.** Maintained weekly by Kalshi. Rejected: requires
   Python 3.13, proprietary license, generated code with a large surface, no raw
   WebSocket frame access, and it lags the API by design.
2. **Community client (`pykalshi`).** Async, popular. Rejected: an external
   maintainer's priorities and release cadence become the project's risk, for a
   surface the project needs only a small part of.
3. **Full code generation from the spec (`openapi-python-client`).** Rejected: a
   hundred endpoints of generated code to review for the dozen used, and it covers
   REST only.

## Decision

Target Python 3.12 (broad wheel availability on aarch64 Linux and macOS). Write a
thin client (`httpx`, `websockets`, `cryptography`) covering only the endpoints in
`DATA_FORMATS.md`, against the pinned OpenAPI 3.30.0 and AsyncAPI 2.0.0 documents in
`specs/`. A weekly CI job re-downloads the specs and fails on drift.

## Consequences

The client is a few hundred lines, fully understood and fully tested. Spec changes
need manual updates, which the drift job makes deliberate rather than surprising. No
dependency on anyone's release cadence or license.

## What would reverse it

An official SDK that is permissively licensed, supports 3.12, exposes raw WebSocket
frames, and tracks the API on the day of change. Two of those four would not be enough.
