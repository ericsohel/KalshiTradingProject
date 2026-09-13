# 0013. Parquet files and DuckDB instead of a database server

Status: accepted; query engine amended by ADR 0026 (pyarrow datasets, not DuckDB). Date: 2026-09-09.

## Context

The tape is append-only, written by one process, and read analytically (scans over
time ranges and tickers). The production host is a free instance with 12 GB of RAM
and no budget for managed databases.

## Alternatives considered

1. **PostgreSQL or TimescaleDB.** Transactions, indexes, familiar. Rejected:
   row-oriented storage is a poor fit for columnar scans over millions of deltas; a
   server to run, tune, back up, and keep alive on a small box; no benefit from
   transactions the workload does not need.
2. **ClickHouse.** Superb for this shape of data. Rejected for v1: a memory-hungry
   service on a 12 GB host that must first and foremost keep the recorder alive.
3. **SQLite.** Zero-ops. Rejected for the tape: poor for columnar scans at this
   volume; kept as a candidate for small operational state (engine positions).

## Decision

Raw segments are custom append-only files. Baked data is Hive-partitioned zstd
Parquet with integer columns. DuckDB queries the files directly, in-process, from the
baker, the API, and notebooks. There is no database server.

## Consequences

Nothing to administer; files are portable and trivially backed up; query performance
on time-range scans is excellent. No transactions and no concurrent writers, which
the single-writer design does not need. Point lookups (book at an instant) are served
by keyframes rather than indexes.

## What would reverse it

Multiple writers, transactional updates, or a query pattern dominated by point
lookups. The likely replacement would be ClickHouse on a larger host.
