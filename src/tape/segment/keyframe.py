"""Keyframe files: periodic full-book images as Parquet (docs/DATA_FORMATS.md 5)."""

from __future__ import annotations

from collections.abc import Collection, Iterable
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tape.book import KeyframeRow
from tape.errors import TapeCorruptionError

__all__ = ["KEYFRAME_SCHEMA", "read_keyframe", "write_keyframe"]

KEYFRAME_SCHEMA: Final = pa.schema(
    [
        pa.field("ticker", pa.dictionary(pa.int32(), pa.string()), nullable=False),
        pa.field("side", pa.int8(), nullable=False),
        pa.field("price_e4", pa.int32(), nullable=False),
        pa.field("count_e2", pa.int64(), nullable=False),
        pa.field("as_of_recv_ns", pa.int64(), nullable=False),
        pa.field("last_ts_ms", pa.int64(), nullable=True),
        pa.field("stale", pa.bool_(), nullable=False),
    ]
)


def write_keyframe(path: Path, rows: Iterable[KeyframeRow]) -> int:
    """Write rows to a Parquet file with ``KEYFRAME_SCHEMA``. Returns the row count.

    The write is atomic: data goes to a temporary sibling that is renamed into place.
    """
    materialized = list(rows)
    table = pa.Table.from_pydict(
        {
            "ticker": [r.ticker for r in materialized],
            "side": [r.side for r in materialized],
            "price_e4": [r.price_e4 for r in materialized],
            "count_e2": [r.count_e2 for r in materialized],
            "as_of_recv_ns": [r.as_of_recv_ns for r in materialized],
            "last_ts_ms": [r.last_ts_ms for r in materialized],
            "stale": [r.stale for r in materialized],
        },
        schema=KEYFRAME_SCHEMA,
    )
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)
    return len(materialized)


def read_keyframe(path: Path, *, tickers: Collection[str] | None = None) -> list[KeyframeRow]:
    """Read a keyframe file back into rows.

    Args:
        path: The keyframe file.
        tickers: Read only these markets' rows; ``None`` reads every row.

    Raises:
        TapeCorruptionError: If the file's schema does not match ``KEYFRAME_SCHEMA``.
    """
    if tickers is None:
        table = pq.read_table(path)
    else:
        wanted = pa.array(sorted(set(tickers)), type=pa.string())
        table = pq.read_table(path, filters=pc.field("ticker").isin(wanted))
    if not table.schema.equals(KEYFRAME_SCHEMA):
        raise TapeCorruptionError(f"{path}: unexpected keyframe schema {table.schema}")
    columns = table.to_pydict()
    return [
        KeyframeRow(
            ticker=str(columns["ticker"][i]),
            side=int(columns["side"][i]),
            price_e4=int(columns["price_e4"][i]),
            count_e2=int(columns["count_e2"][i]),
            as_of_recv_ns=int(columns["as_of_recv_ns"][i]),
            last_ts_ms=None if columns["last_ts_ms"][i] is None else int(columns["last_ts_ms"][i]),
            stale=bool(columns["stale"][i]),
        )
        for i in range(table.num_rows)
    ]
