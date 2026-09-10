# 0007. Raw tape private; code, method, and results public

Status: accepted. Date: 2026-09-09.

## Context

Kalshi's data terms prohibit providing archived or cached data sets containing Kalshi
data to others without written consent. The project's value as a public artifact
comes from its code, its measured integrity, and its reproducible studies.

## Decision

The raw tape, keyframes, and baked tables are never redistributed. The repository is
public and MIT-licensed. The status page publishes aggregate integrity metrics; the
live viewer streams current market data, which is public information served in real
time rather than an archive. Studies publish figures and aggregates with the exact
commands needed to reproduce them from a reader's own recording. Written consent from
Kalshi would be required before any archive is shared.

## Consequences

No dataset downloads. Reproducibility is "run the recorder yourself". The live viewer
must not offer bulk historical export beyond what is needed to render a window on
screen; the `tape` route is capped accordingly.
