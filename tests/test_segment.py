"""Segment writer and reader: round trips, truncated tails, corruption."""

from __future__ import annotations

import struct
from pathlib import Path

import pytest
import zstandard
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.errors import TapeCorruptionError
from tape.segment import (
    Record,
    RecordKind,
    SegmentHeader,
    SegmentReader,
    SegmentWriter,
    SubscriptionInfo,
)


def header() -> SegmentHeader:
    return SegmentHeader(
        created_wall_ns=1,
        host="test",
        env="demo",
        conn_id=3,
        ws_url="wss://example/ws",
        use_yes_price=True,
        subscriptions=[SubscriptionInfo(sid=1, channel="orderbook_delta", group_id="g0")],
        software_version="0.1.0",
        spec_versions={"asyncapi": "2.0.0", "openapi": "3.30.0"},
    )


def record(i: int, payload: bytes = b'{"type":"x"}') -> Record:
    return Record(
        kind=RecordKind.FRAME, conn_id=3, recv_mono_ns=i, recv_wall_ns=1000 + i, payload=payload
    )


def test_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "a.tape.zst"
    with SegmentWriter(path, header()) as writer:
        for i in range(100):
            writer.append(record(i, payload=bytes([i]) * i))
        writer.flush()
    assert writer.records_written == 100
    with SegmentReader(path) as reader:
        assert reader.header == header()
        records = list(reader.records())
        assert reader.truncated is False
    assert len(records) == 100
    assert records[7] == record(7, payload=bytes([7]) * 7)


def test_truncated_tail_is_tolerated(tmp_path: Path) -> None:
    path = tmp_path / "b.tape.zst"
    writer = SegmentWriter(path, header())
    for i in range(10):
        writer.append(record(i))
    writer.flush()
    writer.append(record(99, payload=b"never flushed"))
    snapshot = path.read_bytes()
    writer.close()
    crashed = tmp_path / "crashed.tape.zst"
    crashed.write_bytes(snapshot)
    with SegmentReader(crashed) as reader:
        records = list(reader.records())
    assert [r.recv_mono_ns for r in records] == list(range(10))
    assert reader.truncated is True


def test_bad_magic_and_version_raise(tmp_path: Path) -> None:
    bad = tmp_path / "bad.tape.zst"
    bad.write_bytes(zstandard.ZstdCompressor().compress(b"NOPE" + b"\x00" * 6))
    with pytest.raises(TapeCorruptionError, match="magic"):
        SegmentReader(bad)
    wrong_version = tmp_path / "v.tape.zst"
    wrong_version.write_bytes(zstandard.ZstdCompressor().compress(b"TAPE\x02\x00\x00\x00\x00\x00"))
    with pytest.raises(TapeCorruptionError, match="version"):
        SegmentReader(wrong_version)
    empty = tmp_path / "e.tape.zst"
    empty.write_bytes(zstandard.ZstdCompressor().compress(b""))
    with pytest.raises(TapeCorruptionError, match="shorter"):
        SegmentReader(empty)


def test_unknown_record_kind_is_corruption(tmp_path: Path) -> None:
    path = tmp_path / "k.tape.zst"
    hdr = (
        b'{"created_wall_ns":1,"host":"h","env":"demo","conn_id":1,"ws_url":"w",'
        b'"use_yes_price":true,"subscriptions":[],"software_version":"0","spec_versions":{}}'
    )
    body = struct.pack("<4sHI", b"TAPE", 1, len(hdr)) + hdr + struct.pack("<BHQQI", 200, 1, 0, 0, 0)
    path.write_bytes(zstandard.ZstdCompressor().compress(body))
    with SegmentReader(path) as reader, pytest.raises(TapeCorruptionError, match="record kind"):
        list(reader.records())


def test_writer_refuses_after_close(tmp_path: Path) -> None:
    writer = SegmentWriter(tmp_path / "c.tape.zst", header())
    writer.close()
    writer.close()
    with pytest.raises(ValueError, match="closed"):
        writer.append(record(1))


records_strategy = st.lists(
    st.builds(
        Record,
        kind=st.sampled_from(list(RecordKind)),
        conn_id=st.integers(0, 65_535),
        recv_mono_ns=st.integers(0, 2**63 - 1),
        recv_wall_ns=st.integers(0, 2**63 - 1),
        payload=st.binary(max_size=2_000),
    ),
    max_size=50,
)


@given(records_strategy)
@settings(max_examples=50, deadline=None)
def test_any_record_sequence_round_trips(
    tmp_path_factory: pytest.TempPathFactory, records: list[Record]
) -> None:
    path = tmp_path_factory.mktemp("seg") / "p.tape.zst"
    with SegmentWriter(path, header()) as writer:
        for r in records:
            writer.append(r)
    with SegmentReader(path) as reader:
        assert list(reader.records()) == records
        assert reader.truncated is False
