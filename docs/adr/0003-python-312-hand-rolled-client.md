# 0003. Python 3.12 with a spec-pinned, hand-rolled client

Status: accepted. Date: 2026-09-09.

## Context

Kalshi's official Python SDK requires Python 3.13, carries a proprietary license, is
generated weekly from the OpenAPI spec, and Kalshi itself advises production users to
generate their own client from the specs because the SDK may lag. The project needs
async, WebSocket-first access with a client-side token-bucket mirror, fixed-point
codecs, and raw-frame access, none of which the SDK provides.

## Decision

Target Python 3.12 (broad wheel availability on aarch64 Linux and macOS). Write a
thin client (`httpx`, `websockets`, `cryptography`) against the pinned OpenAPI 3.30.0
and AsyncAPI 2.0.0 documents stored in `specs/`. A weekly CI job re-downloads the
specs and fails on drift so changes are reviewed deliberately.

## Consequences

The client is small and fully under the project's control. Spec changes require
manual updates, which the drift job makes visible. The project does not depend on
the SDK's release cadence or license.
