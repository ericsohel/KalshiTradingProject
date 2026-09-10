# 0011. uv-managed monorepo with src layout, MIT license

Status: accepted. Date: 2026-09-09.

## Context

The project spans a Python package, a TypeScript application, deployment files, and
design documents that must stay consistent with each other. The owner wants the
project public and easy for others to adopt.

## Decision

One repository. Python lives in `src/tape/` with `uv` managing the interpreter,
dependencies, and lockfile. The web application lives in `web/` with its own
lockfile. Deployment assets live in `deploy/`. Design documents and ADRs live in
`docs/`. The license is MIT.

## Consequences

One place for issues, CI, and history; cross-cutting changes land in one pull
request. CI runs two toolchains. MIT permits any reuse, including commercial, which
the owner accepts in exchange for maximum adoption.
