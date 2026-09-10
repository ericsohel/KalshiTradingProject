# 0007. Raw tape private; code, method, and results public

Status: accepted. Date: 2026-09-09.

## Context

Kalshi's data terms prohibit providing archived or cached data sets containing Kalshi
data to others without written consent. The project's public value comes from its
code, its measured integrity, and its reproducible studies; an open archive would be
more valuable to the community but is not the project's to give.

## Alternatives considered

1. **Publish the archive** (daily Parquet drops on object storage). Highest community
   value; the most-starred Kalshi repository is a static trade dataset. Rejected:
   violates the terms without consent and would put the project's account at risk.
2. **Publish aggregated tables** (per-minute depth summaries). Less sensitive.
   Rejected for now: still a data set containing Kalshi data; the line is unclear and
   the downside is the same.
3. **Seek written consent first.** Kept as a future option, not a dependency.

## Decision

The raw tape, keyframes, and baked tables are never redistributed. The repository is
public and MIT-licensed. The status page publishes aggregate integrity metrics; the
live viewer streams current market data in real time, which is public information
displayed rather than an archive delivered. Studies publish figures and the exact
commands to reproduce them from a reader's own recording.

## Consequences

No dataset downloads. Reproducibility means "run the recorder yourself", which is a
stronger claim than a downloaded file anyway. The `tape` API route is capped to what
a screen can show, so the viewer cannot become a bulk export path.

## What would reverse it

Written consent from Kalshi for a research archive.
