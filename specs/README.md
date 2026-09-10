# Pinned Kalshi API specifications

`openapi.yaml` (REST, version 3.30.0) and `asyncapi.yaml` (WebSocket, version 2.0.0)
as published at `https://docs.kalshi.com/openapi.yaml` and
`https://docs.kalshi.com/asyncapi.yaml`, fetched 2026-09-09. They are the contract the
`tape.wire` structs are tested against. A weekly CI job re-downloads both and fails
on any difference so that changes are reviewed deliberately (ADR 0003).

To update: replace the files, run the contract tests, update `tape.wire` if needed,
and record the new versions here and in `PINNED_SPEC_VERSIONS` in
`tape.recorder.recorder`, which every segment header copies.
