# 0026. The catalog reads baked Parquet with pyarrow datasets, not DuckDB

Status: accepted. Date: 2026-09-12. Amends ADR 0013 (query engine).

## Context

ADR 0013 chose Hive-partitioned Parquet files with DuckDB querying them in-process. When
the catalog was built (M4), two facts were new. The production host has 1 GB of memory
(ADR 0024), shared with the recorder and the API. And every catalog query names one
market and a time range: rebuild a book at an instant, or fetch a market's deltas or
trades in a window. None needs joins or ad hoc aggregation.

## Alternatives considered

1. **DuckDB, as ADR 0013 planned.** Rejected for the catalog: a second query engine and
   its own memory to budget on a 1 GB host, for queries the Parquet readers already
   answer.
2. **pyarrow datasets with filter pushdown.** pyarrow is already a dependency (keyframes
   and baked tables). Chosen.

## Decision

`tape.store.Catalog` reads baked tables and keyframes with `pyarrow.dataset`, pushing the
market and time filters down to partition pruning and row-group statistics. `ticker` is
written as a plain string column rather than dictionary-encoded, so row-group min and max
statistics exist for it. On the development archive that let a single-market query skip
30 of 31 row groups and run about six times faster than with a dictionary-typed column.

## Consequences

The project keeps one Parquet engine and adds no dependency. Ad hoc analysis in
notebooks can still use DuckDB over the same files, since the format is unchanged.
Queries that do need joins or aggregation across markets, such as the future study,
must either be written against pyarrow or bring DuckDB in for that tool alone.

## What would reverse it

Catalog queries that span many markets or need joins in the API or engine, or a host
with memory to spare for a second engine.
