"""The version-1 baked tables: Arrow schemas, sort orders, and the Parquet writer settings.

Responsibility: define each table of docs/DATA_FORMATS.md 6 once, so that the baker writes and
the catalog reads the same columns in the same order, and write one sorted part file with
settings fixed here, so that equal rows always produce equal bytes
(docs/ENGINEERING_STANDARDS.md 3.7).

Invariants: every column is an integer, a boolean, or a string, never floating point; strings are
plain Arrow strings, because row-group statistics on a plain string column let a reader skip every
row group of other markets, which a dictionary-typed Arrow column does not; each column's Parquet
encoding (dictionary, delta, or plain) is fixed per table, chosen by measuring the recorded archive
(docs/DATA_FORMATS.md 6); a part's rows are sorted by its table's sort keys; and every writer
setting (compression, level, row-group size, encodings, sorting metadata) is a constant.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Final, Literal

import pyarrow as pa
import pyarrow.parquet as pq

__all__ = [
    "EMPTY_BOOK_SIDE",
    "PARQUET_COMPRESSION",
    "PARQUET_COMPRESSION_LEVEL",
    "ROW_GROUP_ROWS",
    "SNAPSHOT_INITIAL",
    "SNAPSHOT_RECONNECT",
    "SNAPSHOT_RESYNC",
    "TABLES",
    "TABLE_NAMES",
    "Row",
    "TableName",
    "TableSpec",
    "open_part",
]

type TableName = Literal["deltas", "snapshots", "trades", "lifecycle", "gaps", "audits"]
"""A version-1 baked table."""

type Row = tuple[object, ...]
"""One row, with values in its table's column order."""

TABLE_NAMES: Final[tuple[TableName, ...]] = (
    "deltas",
    "snapshots",
    "trades",
    "lifecycle",
    "gaps",
    "audits",
)
"""Every table a bake writes, in the order the manifest lists them."""

SNAPSHOT_INITIAL: Final = 0
"""``snapshots.reason``: the first image of a market on its subscription."""

SNAPSHOT_RESYNC: Final = 1
"""``snapshots.reason``: an image that answers ``get_snapshot`` or repeats a market's image."""

SNAPSHOT_RECONNECT: Final = 2
"""``snapshots.reason``: the first image of a market after its connection was lost."""

EMPTY_BOOK_SIDE: Final = -1
"""``snapshots.side`` of the one row that records an empty book, as in keyframes."""

PARQUET_COMPRESSION: Final = "zstd"

PARQUET_COMPRESSION_LEVEL: Final = 6
"""Measured on the recorded archive (docs/OPERATIONS.md 5): level 9 saved under 1% over level 6
and doubled write time; level 3 cost about 3%."""

ROW_GROUP_ROWS: Final = 65_536
"""Rows per row group. Small enough that a query for one market reads few rows of others; larger
groups compressed under 0.2% better."""

_DELTA_ENCODING: Final = "DELTA_BINARY_PACKED"


@dataclass(frozen=True, slots=True)
class TableSpec:
    """One baked table.

    Attributes:
        name: The table.
        schema: Its columns, in file order.
        sort_keys: Columns rows are sorted by within a part, ascending, nulls last.
        partition_key: The column whose value chooses a row's spill bucket; every row of one
            value lands in the same part file.
        dictionary_columns: Columns Parquet dictionary-encodes: few distinct values, or values
            repeated across neighbouring rows.
        delta_columns: Columns Parquet delta-encodes: values that rise within a market, such as
            receive times and sequence numbers. Every other column is plain.
    """

    name: TableName
    schema: pa.Schema
    sort_keys: tuple[str, ...]
    partition_key: str
    dictionary_columns: tuple[str, ...]
    delta_columns: tuple[str, ...]


def _required(name: str, kind: pa.DataType) -> pa.Field[pa.DataType]:
    return pa.field(name, kind, nullable=False)


def _optional(name: str, kind: pa.DataType) -> pa.Field[pa.DataType]:
    return pa.field(name, kind, nullable=True)


_DELTAS: Final = TableSpec(
    name="deltas",
    schema=pa.schema(
        [
            _required("ticker", pa.string()),
            _optional("ts_ms", pa.int64()),
            _required("recv_mono_ns", pa.int64()),
            _required("recv_wall_ns", pa.int64()),
            _required("conn_id", pa.int16()),
            _required("sid", pa.int32()),
            _optional("seq", pa.int64()),
            _required("side", pa.int8()),
            _required("price_e4", pa.int32()),
            _required("delta_e2", pa.int64()),
            _optional("own_client_order_id", pa.string()),
        ]
    ),
    sort_keys=("ticker", "ts_ms", "seq", "recv_wall_ns"),
    partition_key="ticker",
    dictionary_columns=(
        "ticker",
        "conn_id",
        "sid",
        "side",
        "price_e4",
        "delta_e2",
        "own_client_order_id",
    ),
    delta_columns=("ts_ms", "recv_mono_ns", "recv_wall_ns", "seq"),
)

_SNAPSHOTS: Final = TableSpec(
    name="snapshots",
    schema=pa.schema(
        [
            _required("ticker", pa.string()),
            _required("recv_wall_ns", pa.int64()),
            _required("recv_mono_ns", pa.int64()),
            _required("conn_id", pa.int16()),
            _required("sid", pa.int32()),
            _optional("seq", pa.int64()),
            _required("side", pa.int8()),
            _required("price_e4", pa.int32()),
            _required("count_e2", pa.int64()),
            _required("reason", pa.int8()),
        ]
    ),
    sort_keys=("ticker", "recv_wall_ns", "seq", "side", "price_e4"),
    partition_key="ticker",
    dictionary_columns=(
        "ticker",
        "recv_wall_ns",
        "recv_mono_ns",
        "conn_id",
        "sid",
        "side",
        "reason",
    ),
    delta_columns=(),
)

_TRADES: Final = TableSpec(
    name="trades",
    schema=pa.schema(
        [
            _required("ticker", pa.string()),
            _required("trade_id", pa.string()),
            _required("ts_ms", pa.int64()),
            _required("recv_wall_ns", pa.int64()),
            _required("sid", pa.int32()),
            _optional("seq", pa.int64()),
            _required("price_e4", pa.int32()),
            _required("count_e2", pa.int64()),
            _required("taker_side", pa.int8()),
            _required("is_block", pa.bool_()),
        ]
    ),
    sort_keys=("ticker", "ts_ms", "seq", "trade_id"),
    partition_key="ticker",
    dictionary_columns=("ticker", "sid", "price_e4", "taker_side"),
    delta_columns=("recv_wall_ns", "seq"),
)

_LIFECYCLE: Final = TableSpec(
    name="lifecycle",
    schema=pa.schema(
        [
            _required("ticker", pa.string()),
            _required("msg_type", pa.string()),
            _optional("event_type", pa.string()),
            _optional("ts_s", pa.int64()),
            _required("recv_wall_ns", pa.int64()),
            _required("sid", pa.int32()),
            _optional("seq", pa.int64()),
            _required("payload_json", pa.string()),
        ]
    ),
    sort_keys=("ticker", "recv_wall_ns", "seq"),
    partition_key="ticker",
    dictionary_columns=("ticker", "msg_type", "event_type"),
    delta_columns=(),
)

_GAPS: Final = TableSpec(
    name="gaps",
    schema=pa.schema(
        [
            _required("conn_id", pa.int16()),
            _required("sid", pa.int32()),
            _required("recv_wall_ns", pa.int64()),
            _required("recv_mono_ns", pa.int64()),
            _required("expected_seq", pa.int64()),
            _required("got_seq", pa.int64()),
            _optional("resolved_recv_wall_ns", pa.int64()),
        ]
    ),
    sort_keys=("conn_id", "recv_wall_ns", "sid"),
    partition_key="conn_id",
    dictionary_columns=(),
    delta_columns=(),
)

_AUDITS: Final = TableSpec(
    name="audits",
    schema=pa.schema(
        [
            _required("ticker", pa.string()),
            _required("recv_wall_ns", pa.int64()),
            _required("conn_id", pa.int16()),
            _required("outcome", pa.string()),
            _required("send_wall_ns", pa.int64()),
            _required("send_mono_ns", pa.int64()),
            _required("recv_mono_ns", pa.int64()),
            _required("window_open_mono_ns", pa.int64()),
            _required("window_close_mono_ns", pa.int64()),
            _required("window_events", pa.int32()),
            _optional("match_index", pa.int32()),
            _required("levels_rest", pa.int32()),
            _optional("levels_local", pa.int32()),
            _optional("mismatched_levels", pa.int32()),
            _optional("max_abs_diff_e2", pa.int64()),
            _optional("fault", pa.string()),
            _optional("rest_levels_json", pa.string()),
            _optional("local_levels_json", pa.string()),
        ]
    ),
    sort_keys=("ticker", "recv_wall_ns"),
    partition_key="ticker",
    dictionary_columns=("ticker", "outcome", "fault"),
    delta_columns=(),
)

TABLES: Final[Mapping[TableName, TableSpec]] = MappingProxyType(
    {spec.name: spec for spec in (_DELTAS, _SNAPSHOTS, _TRADES, _LIFECYCLE, _GAPS, _AUDITS)}
)
"""Every table by name."""


def open_part(path: Path, spec: TableSpec) -> pq.ParquetWriter:
    """Open one Parquet part file of a table with the fixed writer settings.

    The caller writes the rows in ``spec.sort_keys`` order, each row group with
    ``write_table(rows, row_group_size=ROW_GROUP_ROWS)``, and closes the writer; as a context
    manager it closes itself. Writing rows whose schema is not ``spec.schema`` raises
    ``ValueError``.

    Args:
        path: File to create; an existing file is replaced.
        spec: The table the rows belong to.

    Returns:
        The open writer.

    Raises:
        OSError: If the file cannot be created.
    """
    sorting = [pq.SortingColumn(spec.schema.get_field_index(key)) for key in spec.sort_keys]
    return pq.ParquetWriter(
        path,
        spec.schema,
        compression=PARQUET_COMPRESSION,
        compression_level=PARQUET_COMPRESSION_LEVEL,
        # A column list is valid for both options; the stubs accept only the boolean form.
        use_dictionary=list(spec.dictionary_columns),  # type: ignore[arg-type]
        column_encoding=dict.fromkeys(spec.delta_columns, _DELTA_ENCODING),
        write_statistics=True,
        sorting_columns=sorting,
    )
