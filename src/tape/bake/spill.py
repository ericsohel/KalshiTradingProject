"""Write a table's rows as sorted Parquet parts while holding a bounded number of rows in memory.

Responsibility: take one table's rows one at a time, however many there are, and produce part
files each sorted by the table's sort keys (docs/DATA_FORMATS.md 6), within the memory of the
1 GB production host (ADR 0024).

How memory stays bounded: rows are buffered up to ``flush_rows`` and then spilled to an Arrow IPC
file, one record batch per spill bucket, where a row's bucket is a stable hash of its partition key
(the market ticker, or the connection for gaps). When the input ends, buckets are laid out in order
into parts of at most ``max_part_rows`` rows: a bucket that fits goes whole into one part, so all
rows of its markets share a file, and a bucket larger than the bound is cut, in the order its rows
were added, into ranges of at most ``max_part_rows`` rows. Each part alone is read back, sorted by
index, and written one sorted row group at a time.

Invariants: the parts hold exactly the rows added, each once; no part holds more than
``max_part_rows`` rows; within a part, rows are sorted by the sort keys and ties keep the order rows
were added; how rows are assigned to buckets and parts depends only on the rows and their order,
never on ``flush_rows`` or timing, so equal input gives byte-identical parts; and the spill file is
removed when the writer finishes or is closed.
"""

from __future__ import annotations

import zlib
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Final, Literal

import msgspec
import pyarrow as pa
import pyarrow.compute as pc
from pyarrow import ipc

from tape.bake.files import sha256_file
from tape.bake.tables import (
    ROW_GROUP_ROWS,
    TABLE_NAMES,
    TABLES,
    Row,
    TableName,
    TableSpec,
    open_part,
)

__all__ = [
    "DEFAULT_FLUSH_ROWS",
    "SPILL_BUCKETS",
    "BucketRange",
    "HourTables",
    "PartFile",
    "TableWriter",
    "bucket_of",
    "plan_parts",
]

SPILL_BUCKETS: Final = 64
"""Buckets a table's rows are hashed into. More buckets spread markets over more, smaller parts and
lengthen the spill file's batch index."""

DEFAULT_FLUSH_ROWS: Final = 50_000
"""Rows buffered as Python objects before they are spilled; tens of megabytes at most."""

_ORDINAL: Final = "_ordinal"
_SPILL_COMPRESSION: Final = "lz4"

type BucketRange = tuple[int, int, int]
"""``(bucket, first_row, end_row)``: rows of one bucket, counted in the order they were added."""


class PartFile(msgspec.Struct, frozen=True, kw_only=True):
    """One part file written.

    Attributes:
        path: Where it was written.
        rows: Rows in it.
        bytes: File size.
        sha256: Hash of its content.
    """

    path: Path
    rows: int
    bytes: int
    sha256: str


def bucket_of(value: object) -> int:
    """The spill bucket of a partition key value: CRC-32 of its text, modulo the bucket count."""
    return zlib.crc32(str(value).encode("utf-8")) % SPILL_BUCKETS


def plan_parts(
    bucket_rows: Sequence[int], max_part_rows: int
) -> tuple[tuple[BucketRange, ...], ...]:
    """Lay buckets out in order into parts of at most ``max_part_rows`` rows.

    A bucket that fits the room left in the current part joins it, and one that does not starts the
    next part. A bucket larger than the bound is cut into ranges of ``max_part_rows`` rows, each a
    part of its own, and its remainder starts the next part.

    Args:
        bucket_rows: Rows in each bucket.
        max_part_rows: The bound.

    Returns:
        The bucket ranges of each part, in order.

    Raises:
        ValueError: If ``max_part_rows`` is not positive.
    """
    if max_part_rows <= 0:
        raise ValueError("max_part_rows must be positive")
    parts: list[tuple[BucketRange, ...]] = []
    current: list[BucketRange] = []
    current_rows = 0
    for bucket, rows in enumerate(bucket_rows):
        if rows == 0:
            continue
        if current_rows + rows > max_part_rows and current:
            parts.append(tuple(current))
            current, current_rows = [], 0
        first = 0
        while rows - first > max_part_rows:
            parts.append(((bucket, first, first + max_part_rows),))
            first += max_part_rows
        current.append((bucket, first, rows))
        current_rows += rows - first
    if current:
        parts.append(tuple(current))
    return tuple(parts)


class TableWriter:
    """Accumulates one table's rows and writes them as sorted part files.

    Args:
        spec: The table.
        work_dir: Existing directory for the spill file.
        max_part_rows: Most rows in one part file.
        flush_rows: Rows buffered in memory before they are spilled.

    Raises:
        ValueError: If ``max_part_rows`` or ``flush_rows`` is not positive.
    """

    def __init__(
        self,
        spec: TableSpec,
        *,
        work_dir: Path,
        max_part_rows: int,
        flush_rows: int = DEFAULT_FLUSH_ROWS,
    ) -> None:
        if max_part_rows <= 0 or flush_rows <= 0:
            raise ValueError("max_part_rows and flush_rows must be positive")
        self._spec = spec
        self._max_part_rows = max_part_rows
        self._flush_rows = flush_rows
        self._spill_schema = spec.schema.append(pa.field(_ORDINAL, pa.int64(), nullable=False))
        self._key_index = spec.schema.get_field_index(spec.partition_key)
        self._width = len(spec.schema)
        self._spill_path = work_dir / f"{spec.name}.spill.arrow"
        self._writer: ipc.RecordBatchFileWriter | None = None
        self._buffer: list[Row] = []
        self._added = 0
        self._bucket_rows = [0] * SPILL_BUCKETS
        self._batch_buckets: list[int] = []
        self._batch_rows: list[int] = []
        self._bucket_cache: dict[object, int] = {}
        self._done = False

    @property
    def rows(self) -> int:
        """Rows added so far."""
        return self._added + len(self._buffer)

    def add(self, row: Row) -> None:
        """Take one row, in the table's column order.

        Raises:
            ValueError: If the writer has finished, or the row has the wrong number of values.
        """
        if self._done:
            raise ValueError(f"{self._spec.name} writer has finished")
        if len(row) != self._width:
            raise ValueError(
                f"{self._spec.name}: a row has {len(row)} values, expected {self._width}"
            )
        self._buffer.append(row)
        if len(self._buffer) >= self._flush_rows:
            self._spill()

    def finish(self, out_dir: Path) -> tuple[PartFile, ...]:
        """Write every row added as sorted part files ``part-0.parquet``, ``part-1.parquet``, ...

        Args:
            out_dir: Existing directory for the parts; nothing is written when no row was added.

        Returns:
            The parts, in part order.

        Raises:
            ValueError: If the writer has finished, or a row has the wrong types.
            OSError: If a file cannot be written.
        """
        if self._done:
            raise ValueError(f"{self._spec.name} writer has finished")
        self._spill()
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        parts: list[PartFile] = []
        try:
            for index, ranges in enumerate(plan_parts(self._bucket_rows, self._max_part_rows)):
                path = out_dir / f"part-{index}.parquet"
                rows = self._write_part(path, ranges)
                parts.append(
                    PartFile(
                        path=path, rows=rows, bytes=path.stat().st_size, sha256=sha256_file(path)
                    )
                )
        finally:
            self.close()
        return tuple(parts)

    def close(self) -> None:
        """Discard buffered rows and remove the spill file. Idempotent."""
        self._done = True
        self._buffer = []
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        self._spill_path.unlink(missing_ok=True)

    def _spill(self) -> None:
        """Move the buffered rows into the spill file, one batch per bucket, keeping row order."""
        if not self._buffer:
            return
        count = len(self._buffer)
        columns = list(zip(*self._buffer, strict=True))
        arrays = [
            pa.array(column, type=spec_field.type)
            for column, spec_field in zip(columns, self._spec.schema, strict=True)
        ]
        arrays.append(pa.array(range(self._added, self._added + count), type=pa.int64()))
        buckets = [self._bucket(key) for key in columns[self._key_index]]
        batch = pa.RecordBatch.from_arrays(arrays, schema=self._spill_schema)
        # A stable sort, so the rows of a bucket keep the order they were added in.
        batch = batch.take(pc.sort_indices(pa.array(buckets, type=pa.int32())))
        counts = [0] * SPILL_BUCKETS
        for bucket in buckets:
            counts[bucket] += 1
        writer = self._writer
        if writer is None:
            options = ipc.IpcWriteOptions(compression=_SPILL_COMPRESSION)
            writer = ipc.new_file(str(self._spill_path), self._spill_schema, options=options)
            self._writer = writer
        offset = 0
        for bucket, rows in enumerate(counts):
            if rows == 0:
                continue
            writer.write_batch(batch.slice(offset, rows))
            self._batch_buckets.append(bucket)
            self._batch_rows.append(rows)
            self._bucket_rows[bucket] += rows
            offset += rows
        self._added += count
        self._buffer = []

    def _bucket(self, key: object) -> int:
        bucket = self._bucket_cache.get(key)
        if bucket is None:
            bucket = self._bucket_cache[key] = bucket_of(key)
        return bucket

    def _write_part(self, path: Path, ranges: Sequence[BucketRange]) -> int:
        """Sort one part's rows and write them a row group at a time.

        The part's rows are read back from the spill file into one contiguous table and sorted by
        index; each row group is then taken and written on its own, so memory holds the part's rows,
        their order, and a single sorted row group, never a second sorted copy of the part.

        Returns:
            Rows written.
        """
        keys: list[tuple[str, Literal["ascending", "descending"]]] = [
            (key, "ascending") for key in (*self._spec.sort_keys, _ORDINAL)
        ]
        with pa.memory_map(str(self._spill_path)) as source:
            reader = ipc.open_file(source)
            table = pa.Table.from_batches(
                self._batches(reader, ranges), schema=self._spill_schema
            ).combine_chunks()
            order = pc.sort_indices(table, sort_keys=keys)
            with open_part(path, self._spec) as writer:
                for start in range(0, table.num_rows, ROW_GROUP_ROWS):
                    group = table.take(order.slice(start, ROW_GROUP_ROWS))
                    writer.write_table(
                        group.drop_columns([_ORDINAL]), row_group_size=ROW_GROUP_ROWS
                    )
            return table.num_rows

    def _batches(
        self, reader: ipc.RecordBatchFileReader, ranges: Sequence[BucketRange]
    ) -> list[pa.RecordBatch]:
        """The spilled rows a part's bucket ranges cover, as batch slices in spill order."""
        wanted: dict[int, list[tuple[int, int]]] = {}
        for bucket, first, end in ranges:
            wanted.setdefault(bucket, []).append((first, end))
        seen = [0] * SPILL_BUCKETS
        pieces: list[pa.RecordBatch] = []
        for index, bucket in enumerate(self._batch_buckets):
            size = self._batch_rows[index]
            batch_first = seen[bucket]
            seen[bucket] += size
            for first, end in wanted.get(bucket, ()):
                low, high = max(first, batch_first), min(end, batch_first + size)
                if low < high:
                    pieces.append(reader.get_batch(index).slice(low - batch_first, high - low))
        return pieces


class HourTables:
    """The six table writers of one bake, used as the interpreter's row sink.

    Args:
        work_dir: Existing directory for spill files.
        max_part_rows: Passed to every :class:`TableWriter`.
        flush_rows: Passed to every :class:`TableWriter`.
    """

    def __init__(
        self, work_dir: Path, *, max_part_rows: int, flush_rows: int = DEFAULT_FLUSH_ROWS
    ) -> None:
        self._writers = {
            name: TableWriter(
                TABLES[name], work_dir=work_dir, max_part_rows=max_part_rows, flush_rows=flush_rows
            )
            for name in TABLE_NAMES
        }

    def add(self, table: TableName, row: Row) -> None:
        """Take one row of a table."""
        self._writers[table].add(row)

    def finish(self, out_dir: Callable[[TableName], Path]) -> dict[TableName, tuple[PartFile, ...]]:
        """Write every table's parts.

        Args:
            out_dir: The existing directory each table's parts go to.

        Returns:
            Each table's parts.

        Raises:
            OSError: If a file cannot be written.
        """
        try:
            return {name: self._writers[name].finish(out_dir(name)) for name in TABLE_NAMES}
        finally:
            self.close()

    def close(self) -> None:
        """Discard every writer's rows and spill files. Idempotent."""
        for writer in self._writers.values():
            writer.close()
