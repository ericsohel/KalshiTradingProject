# 0011. uv-managed monorepo with src layout, MIT license

Status: accepted. Date: 2026-09-09.

## Context

The project spans a Python package, a TypeScript application, deployment files, and
design documents that must stay consistent. The owner wants it public and adoptable.

## Alternatives considered

1. **Separate repositories** for Python and web. Cleaner toolchains. Rejected: a
   change to an API response touches both; two pull requests for one change invites
   drift, and the API types are generated from the Python side.
2. **pip-tools or Poetry.** Widely used. Rejected: `uv` also manages the interpreter
   version, resolves in seconds, and produces a lockfile that CI installs with
   `--frozen`; on a solo project, speed of iteration matters.
3. **Apache-2.0.** Adds an explicit patent grant. Rejected as unnecessary friction for
   a project with no patents; MIT is the more common expectation.
4. **AGPL.** Protects against closed forks. Rejected: deters the adoption the project
   wants, and the tape (the only thing worth protecting) is private by ADR 0007.

## Decision

One repository. Python in `src/tape/` under `uv`; web in `web/` with its own lockfile;
deployment in `deploy/`; documents and ADRs in `docs/`. MIT license.

## Consequences

One place for issues, CI, and history; cross-cutting changes land together. CI runs
two toolchains. MIT permits any reuse, including commercial.

## What would reverse it

The web application growing its own contributors and release cadence.
