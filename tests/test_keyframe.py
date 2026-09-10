"""Keyframe Parquet round trips and schema enforcement."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tape.book import Book, KeyframeRow, books_from_keyframe_rows, diff
from tape.errors import TapeCorruptionError
from tape.events import Level
from tape.fixedpoint import CountE2, PriceE4
from tape.segment import read_keyframe, write_keyframe
from tape.timeutil import Ms, Ns


def test_round_trip(tmp_path: Path) -> None:
    book = Book("KXTEST")
    book.apply_snapshot(
        [Level(PriceE4(4000), CountE2(5))], [Level(PriceE4(6000), CountE2(7))], ts_ms=Ms(42)
    )
    empty = Book("EMPTY")
    rows = book.to_keyframe(as_of_recv_ns=Ns(1)) + empty.to_keyframe(as_of_recv_ns=Ns(1))
    path = tmp_path / "00.parquet"
    assert write_keyframe(path, rows) == 3
    assert not (tmp_path / "00.parquet.tmp").exists()
    back = read_keyframe(path)
    assert back == rows
    rebuilt = books_from_keyframe_rows(back)
    assert diff(rebuilt["KXTEST"], book).is_empty
    assert rebuilt["EMPTY"].is_stale()


def test_wrong_schema_is_corruption(tmp_path: Path) -> None:
    path = tmp_path / "bad.parquet"
    pq.write_table(pa.table({"ticker": ["X"]}), path)
    with pytest.raises(TapeCorruptionError, match="schema"):
        read_keyframe(path)


def test_null_last_ts_round_trips(tmp_path: Path) -> None:
    row = KeyframeRow(
        ticker="X", side=-1, price_e4=0, count_e2=0, as_of_recv_ns=5, last_ts_ms=None, stale=True
    )
    path = tmp_path / "n.parquet"
    write_keyframe(path, [row])
    assert read_keyframe(path) == [row]
