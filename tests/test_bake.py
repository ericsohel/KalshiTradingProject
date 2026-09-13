"""Baking closed hours: sorted parts, byte-identical re-bakes, accounting, and refusals."""

from __future__ import annotations

import collections
import datetime as dt
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
import zstandard

import tape.bake.bake as bake_module
from tape.bake import (
    BAKE_VERSION,
    DataLayout,
    HourKey,
    archive_lock,
    bake_hour,
    hours_to_bake,
    read_manifest,
    record_bake,
)
from tape.bake.files import sha256_file
from tape.bake.interpret import HourInterpreter
from tape.bake.layout import NS_PER_HOUR, closed_hours
from tape.bake.manifest import PrunedSegment, with_pruned, write_manifest
from tape.bake.spill import SPILL_BUCKETS, TableWriter, bucket_of, plan_parts
from tape.bake.tables import TABLE_NAMES, TABLES, Row, TableName
from tape.errors import ArchiveError
from tape.segment import SegmentReader
from tests.fakes.synthetic_tape import HOUR, MS, SECOND, START, SegmentScript

TICKERS = tuple(f"KXT{index}-26SEP10-T{index}" for index in range(12))


@pytest.fixture
def layout(tmp_path: Path) -> DataLayout:
    return DataLayout.under(tmp_path / "data")


def at(seconds: int, ms: int = 0, *, hour: HourKey = HOUR) -> int:
    return hour.start_wall_ns + seconds * SECOND + ms * MS


def write_busy_hour(layout: DataLayout, hour: HourKey = HOUR) -> None:
    """Several connections, reconnects, a gap, audits, and deltas whose exchange times disagree
    with the order they were received in."""
    control = SegmentScript(conn_id=1)
    control.opened(at(0, hour=hour))
    control.command(at(0, 1, hour=hour), "subscribe", 1, channels=["market_lifecycle_v2"])
    control.subscribed(at(0, 2, hour=hour), channel="market_lifecycle_v2", sid=1)
    for index, ticker in enumerate(TICKERS[:4]):
        control.lifecycle(
            at(5 + index, hour=hour), sid=1, seq=index + 1, ticker=ticker, event_type="activated"
        )
    control.write(layout, hour, 0)

    book = SegmentScript(conn_id=2)
    book.opened(at(1, hour=hour))
    book.subscribed(at(1, 1, hour=hour), channel="orderbook_delta", sid=1)
    book.subscribed(at(1, 2, hour=hour), channel="trade", sid=2)
    for index, ticker in enumerate(TICKERS):
        book.snapshot(
            at(2, index, hour=hour),
            sid=1,
            seq=index + 1,
            ticker=ticker,
            yes=[("0.3000", "5.00"), ("0.3100", "7.00")],
            no=[("0.6000", "4.00")],
        )
    seq = len(TICKERS)
    for step in range(2_400):
        seq += 1
        ticker = TICKERS[(step * 7) % len(TICKERS)]
        wall = at(3 + step // 10, step % 10, hour=hour)
        # Exchange times run backwards within each burst of ten, so sorting must reorder them.
        book.delta(
            wall,
            sid=1,
            seq=seq,
            ticker=ticker,
            price="0.3000",
            delta="1.00",
            ts_ms=wall // MS - step % 10,
        )
        if step % 97 == 0:
            book.trade(wall + 1, sid=2, seq=step // 97 + 1, ticker=ticker, trade_id=f"t{step}")
    book.gap(at(400, hour=hour), sid=1, expected_seq=seq + 1, got_seq=seq + 5)
    for index, ticker in enumerate(TICKERS):
        book.snapshot(at(401, index, hour=hour), sid=1, seq=seq + 5 + index, ticker=ticker)
    for index, outcome in enumerate(("exact", "consistent", "inconsistent", "undecidable")):
        book.audit(at(500, index, hour=hour), ticker=TICKERS[index], outcome=outcome)
    book.closed(at(600, hour=hour), "connection 2 closed by peer")
    book.write(layout, hour, 0)

    reconnected = SegmentScript(conn_id=2)
    reconnected.opened(at(610, hour=hour))
    reconnected.subscribed(at(610, 1, hour=hour), channel="orderbook_delta", sid=1)
    for index, ticker in enumerate(TICKERS[:3]):
        reconnected.snapshot(at(611, index, hour=hour), sid=1, seq=index + 1, ticker=ticker)
        reconnected.delta(at(612, index, hour=hour), sid=1, seq=index + 10, ticker=ticker)
    reconnected.write(layout, hour, 1)

    crashed = SegmentScript(conn_id=3)
    crashed.opened(at(700, hour=hour))
    crashed.subscribed(at(700, 1, hour=hour), channel="orderbook_delta", sid=4)
    crashed.snapshot(at(701, hour=hour), sid=4, seq=1, ticker="KXCRASH-26SEP10")
    crashed.delta(at(702, hour=hour), sid=4, seq=2, ticker="KXCRASH-26SEP10")
    crashed.write_truncated(layout, hour, 0, cut=7)


def read_rows(layout: DataLayout, table: TableName, hour: HourKey = HOUR) -> list[list[Row]]:
    directory = layout.partition_dir(table, hour)
    parts = sorted(directory.glob("part-*.parquet"), key=lambda p: int(p.stem.split("-")[1]))
    names = TABLES[table].schema.names
    return [
        list(zip(*(pq.read_table(path)[name].to_pylist() for name in names), strict=True))
        for path in parts
    ]


def interpreted_rows(layout: DataLayout, hour: HourKey = HOUR) -> dict[str, list[Row]]:
    """The rows interpreting the hour's segments directly yields, for comparison with the parts."""
    collected: dict[str, list[Row]] = collections.defaultdict(list)

    class Sink:
        def add(self, table: TableName, row: Row) -> None:
            collected[table].append(row)

    interpreter = HourInterpreter(hour, Sink())
    for path in layout.segment_files(hour):
        with SegmentReader(path) as reader:
            interpreter.read_segment(path.name, reader.header, reader.records())
    return collected


def test_a_bake_writes_every_row_once_in_parts_sorted_by_the_table_keys(layout: DataLayout) -> None:
    write_busy_hour(layout)
    report = bake_hour(layout, HOUR, software_version="0.1.0", max_part_rows=500, flush_rows=64)
    expected = interpreted_rows(layout)

    for name in TABLE_NAMES:
        parts = read_rows(layout, name)
        baked = [row for part in parts for row in part]
        assert collections.Counter(baked) == collections.Counter(expected[name]), name
        assert report.rows[name] == len(baked)
        assert [entry.rows for entry in report.parts[name]] == [len(part) for part in parts]
        spec = TABLES[name]
        for path in layout.partition_dir(name, HOUR).glob("part-*.parquet"):
            table = pq.read_table(path)
            assert table.schema.equals(spec.schema)
            assert table.equals(table.sort_by([(key, "ascending") for key in spec.sort_keys]))
    deltas = read_rows(layout, "deltas")
    assert len(deltas) > 1
    tickers_by_part = [{row[0] for row in part} for part in deltas]
    assert sum(len(tickers) for tickers in tickers_by_part) == len(set().union(*tickers_by_part))
    assert not layout.staging_dir(HOUR).parent.exists()


def test_a_recorded_bake_lists_segments_parts_and_accounting_in_the_manifest(
    layout: DataLayout,
) -> None:
    write_busy_hour(layout)
    report = bake_hour(layout, HOUR, software_version="0.1.0", max_part_rows=500)
    manifest = record_bake(layout, report, software_version="0.1.0", clock_offset_ms=-3)

    assert read_manifest(layout.manifest_path(HOUR.date)) == manifest
    assert manifest.segments == report.segments
    for segment in manifest.segments:
        path = layout.segment_path(segment.path)
        assert (segment.bytes, segment.sha256) == (path.stat().st_size, sha256_file(path))
    for part in manifest.hour_parts(HOUR.hour):
        assert part.sha256 == sha256_file(layout.part_path(part.path))
    entry = manifest.hour_bake(HOUR.hour)
    assert entry is not None
    assert entry.bake_version == BAKE_VERSION
    assert entry.accounting.total == sum(segment.records for segment in manifest.segments)
    assert manifest.clock.chrony_offset_ms == -3
    assert manifest.gaps.count == 1
    assert manifest.audits.books_sampled == 3


def test_accounting_totals_equal_the_records_the_segments_hold(layout: DataLayout) -> None:
    write_busy_hour(layout)
    report = bake_hour(layout, HOUR, software_version="0.1.0")
    records = 0
    for path in layout.segment_files(HOUR):
        with SegmentReader(path) as reader:
            records += sum(1 for _ in reader.records())
    accounting = report.bake.accounting
    assert accounting.total == records == sum(segment.records for segment in report.segments)
    outcomes = (
        sum(accounting.baked.values())
        + sum(accounting.not_baked.values())
        + sum(accounting.decode_failures.values())
    )
    assert outcomes == records
    assert accounting.failures == 0


def test_rebaking_writes_byte_identical_parts_whatever_the_buffer_size(layout: DataLayout) -> None:
    write_busy_hour(layout)
    first = bake_hour(layout, HOUR, software_version="0.1.0", max_part_rows=700, flush_rows=50)
    first_bytes = {path: path.read_bytes() for path in layout.baked.rglob("*.parquet")}
    second = bake_hour(layout, HOUR, software_version="0.1.0", max_part_rows=700, flush_rows=5_000)

    assert second.parts == first.parts
    assert second.segments == first.segments
    assert {path: path.read_bytes() for path in layout.baked.rglob("*.parquet")} == first_bytes


def test_a_rebake_with_fewer_parts_removes_the_old_ones(layout: DataLayout) -> None:
    write_busy_hour(layout)
    bake_hour(layout, HOUR, software_version="0.1.0", max_part_rows=300)
    many = len(list(layout.partition_dir("deltas", HOUR).glob("*.parquet")))
    report = bake_hour(layout, HOUR, software_version="0.1.0", max_part_rows=1_000_000)

    assert many > 1
    assert sorted(p.name for p in layout.partition_dir("deltas", HOUR).iterdir()) == [
        "part-0.parquet"
    ]
    assert len(report.parts["deltas"]) == 1


def test_a_truncated_final_record_is_tolerated_and_marked(layout: DataLayout) -> None:
    write_busy_hour(layout)
    report = bake_hour(layout, HOUR, software_version="0.1.0")
    by_name = {segment.path.rsplit("/", 1)[1]: segment for segment in report.segments}

    crashed = by_name["conn-03-0000.tape.zst"]
    assert crashed.truncated is True
    assert crashed.records == 3
    assert crashed.last_recv_wall_ns == at(701)
    assert [segment.truncated for segment in report.segments].count(True) == 1
    assert report.bake.accounting.failures == 0


def test_an_hour_of_empty_segments_bakes_to_no_files(layout: DataLayout) -> None:
    SegmentScript(conn_id=2).write(layout, HOUR, 0)
    report = bake_hour(layout, HOUR, software_version="0.1.0")
    manifest = record_bake(layout, report, software_version="0.1.0", clock_offset_ms=None)

    assert report.rows == dict.fromkeys(TABLE_NAMES, 0)
    assert all(not layout.partition_dir(name, HOUR).exists() for name in TABLE_NAMES)
    assert manifest.segments[0].records == 0
    assert manifest.segments[0].first_recv_wall_ns is None
    entry = manifest.hour_bake(HOUR.hour)
    assert entry is not None
    assert entry.accounting.total == 0


def test_a_corrupt_segment_is_counted_and_blocks_nothing_else(layout: DataLayout) -> None:
    write_busy_hour(layout)
    garbage = layout.hour_dir(HOUR) / "conn-09-0000.tape.zst"
    garbage.write_bytes(zstandard.ZstdCompressor().compress(b"NOPE" + bytes(20)))
    report = bake_hour(layout, HOUR, software_version="0.1.0")

    assert report.bake.accounting.corrupt_segments == 1
    assert report.bake.accounting.failures == 1
    assert report.rows["deltas"] > 0
    assert [failure.segment for failure in report.failures] == [
        "2026-09-10/12/conn-09-0000.tape.zst"
    ]


def test_a_damaged_segment_is_corrupt_although_its_records_are_baked(layout: DataLayout) -> None:
    write_busy_hour(layout)
    book = layout.hour_dir(HOUR) / "conn-02-0000.tape.zst"
    book.write_bytes(book.read_bytes() + b"written after the recorder closed the file")
    report = bake_hour(layout, HOUR, software_version="0.1.0")

    assert report.bake.accounting.corrupt_segments == 1
    assert report.rows["deltas"] == len(interpreted_rows(layout)["deltas"])
    (failure,) = report.failures
    assert failure.segment == "2026-09-10/12/conn-02-0000.tape.zst"
    assert "bytes followed the frame" in failure.detail


def test_an_hour_with_a_pruned_segment_is_never_baked_again(layout: DataLayout) -> None:
    write_busy_hour(layout)
    report = bake_hour(layout, HOUR, software_version="0.1.0")
    manifest = record_bake(layout, report, software_version="0.1.0", clock_offset_ms=None)
    first = manifest.segments[0]
    write_manifest(
        layout.manifest_path(HOUR.date),
        with_pruned(
            manifest,
            [
                PrunedSegment(
                    path=first.path, bytes=first.bytes, sha256=first.sha256, pruned_wall_ns=1
                )
            ],
        ),
    )
    layout.segment_path(first.path).unlink()
    later = HOUR.end_wall_ns + 10 * NS_PER_HOUR

    with pytest.raises(ArchiveError, match="pruned segments"):
        bake_hour(layout, HOUR, software_version="0.1.0")
    assert hours_to_bake(layout, now_wall_ns=later, grace_ns=0) == ()
    assert hours_to_bake(layout, now_wall_ns=later, grace_ns=0, force=True) == ()


def test_a_segment_that_changes_while_it_is_baked_fails_the_bake_and_keeps_the_old_tables(
    layout: DataLayout, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_busy_hour(layout)
    bake_hour(layout, HOUR, software_version="0.1.0")
    before = {path: path.read_bytes() for path in layout.baked.rglob("*.parquet")}
    victim = layout.segment_files(HOUR)[0]

    def hash_then_append(path: Path) -> str:
        digest = sha256_file(path)
        if path == victim:
            with path.open("ab") as handle:
                handle.write(b"late bytes")
        return digest

    monkeypatch.setattr(bake_module, "sha256_file", hash_then_append)
    with pytest.raises(ArchiveError, match="changed while it was baked"):
        bake_hour(layout, HOUR, software_version="0.1.0")
    assert {path: path.read_bytes() for path in layout.baked.rglob("*.parquet")} == before
    assert not layout.staging_dir(HOUR).parent.exists()


def test_hours_to_bake_are_closed_and_unbaked_changed_stale_or_missing_a_part(
    layout: DataLayout, monkeypatch: pytest.MonkeyPatch
) -> None:
    earlier = HourKey(dt.date(2026, 9, 10), 11)
    write_busy_hour(layout, earlier)
    write_busy_hour(layout)
    now = HOUR.end_wall_ns + 60 * SECOND

    assert closed_hours((earlier, HOUR), now_wall_ns=now, grace_ns=120 * SECOND) == (earlier,)
    assert hours_to_bake(layout, now_wall_ns=now, grace_ns=120 * SECOND) == (earlier,)
    assert hours_to_bake(layout, now_wall_ns=now, grace_ns=0) == (earlier, HOUR)
    assert hours_to_bake(layout, now_wall_ns=now, grace_ns=0, candidates=[HOUR]) == (HOUR,)

    for hour in (earlier, HOUR):
        record_bake(
            layout,
            bake_hour(layout, hour, software_version="0.1.0"),
            software_version="0.1.0",
            clock_offset_ms=None,
        )
    assert hours_to_bake(layout, now_wall_ns=now, grace_ns=0) == ()
    assert hours_to_bake(layout, now_wall_ns=now, grace_ns=0, force=True) == (earlier, HOUR)

    SegmentScript(conn_id=7).opened(at(900)).write(layout, HOUR, 0)
    assert hours_to_bake(layout, now_wall_ns=now, grace_ns=0) == (HOUR,)
    record_bake(
        layout,
        bake_hour(layout, HOUR, software_version="0.1.0"),
        software_version="0.1.0",
        clock_offset_ms=None,
    )

    part = next(layout.partition_dir("trades", earlier).glob("*.parquet"))
    part.unlink()
    assert hours_to_bake(layout, now_wall_ns=now, grace_ns=0) == (earlier,)
    record_bake(
        layout,
        bake_hour(layout, earlier, software_version="0.1.0"),
        software_version="0.1.0",
        clock_offset_ms=None,
    )

    monkeypatch.setattr(bake_module, "BAKE_VERSION", BAKE_VERSION + 1)
    assert hours_to_bake(layout, now_wall_ns=now, grace_ns=0) == (earlier, HOUR)


def test_recording_one_hour_keeps_the_other_hours_of_its_day(layout: DataLayout) -> None:
    earlier = HourKey(dt.date(2026, 9, 10), 11)
    write_busy_hour(layout, earlier)
    write_busy_hour(layout)
    for hour in (earlier, HOUR):
        record_bake(
            layout,
            bake_hour(layout, hour, software_version="0.1.0"),
            software_version="0.1.0",
            clock_offset_ms=None,
        )
    manifest = record_bake(
        layout,
        bake_hour(layout, HOUR, software_version="0.1.0"),
        software_version="0.1.0",
        clock_offset_ms=None,
    )

    assert [entry.hour for entry in manifest.bake.hours] == [11, 12]
    assert {segment.hour for segment in manifest.segments} == {11, 12}
    assert manifest.tables["deltas"].rows == 2 * len(interpreted_rows(layout)["deltas"])


def test_the_archive_lock_admits_one_holder(layout: DataLayout) -> None:
    with archive_lock(layout), pytest.raises(ArchiveError, match="holds"), archive_lock(layout):
        pass
    with archive_lock(layout):
        pass


def test_parts_keep_buckets_whole_up_to_the_bound_and_cut_only_larger_buckets() -> None:
    assert plan_parts([0, 5, 0, 3, 4, 10, 1], 8) == (
        ((1, 0, 5), (3, 0, 3)),
        ((4, 0, 4),),
        ((5, 0, 8),),
        ((5, 8, 10), (6, 0, 1)),
    )
    assert plan_parts([16, 2], 8) == (((0, 0, 8),), ((0, 8, 16),), ((1, 0, 2),))
    assert plan_parts([0] * SPILL_BUCKETS, 8) == ()
    assert plan_parts([2, 2, 2], 100) == (((0, 0, 2), (1, 0, 2), (2, 0, 2)),)
    with pytest.raises(ValueError, match="positive"):
        plan_parts([1], 0)


def test_a_bucket_larger_than_a_part_is_split_into_sorted_parts_whatever_the_buffer(
    tmp_path: Path,
) -> None:
    def write(flush_rows: int) -> list[tuple[int, bytes, list[Any]]]:  # Any: Arrow values
        work = tmp_path / f"flush-{flush_rows}"
        work.mkdir()
        writer = TableWriter(
            TABLES["gaps"], work_dir=work, max_part_rows=1_000, flush_rows=flush_rows
        )
        for index in range(2_500):
            writer.add((1, index % 3, 10_000 - index, index, index, index + 2, None))
        written = []
        for part in writer.finish(work):
            walls = pq.read_table(part.path)["recv_wall_ns"].to_pylist()
            written.append((part.rows, part.path.read_bytes(), walls))
        return written

    parts = write(flush_rows=300)
    assert [rows for rows, _, _ in parts] == [1_000, 1_000, 500]
    assert all(walls == sorted(walls) for _, _, walls in parts)
    assert sorted(wall for _, _, walls in parts for wall in walls) == list(range(7_501, 10_001))
    # The first part holds the first thousand rows added: the cut follows the order of the rows.
    assert parts[0][2] == list(range(9_001, 10_001))
    assert [data for _, data, _ in write(flush_rows=7)] == [data for _, data, _ in parts]


def test_buckets_are_stable_and_in_range() -> None:
    assert bucket_of("KXBTC15M-26SEP092130-00") == bucket_of("KXBTC15M-26SEP092130-00")
    assert all(0 <= bucket_of(ticker) < SPILL_BUCKETS for ticker in TICKERS)
    assert bucket_of(3) == bucket_of("3")


def test_a_table_writer_refuses_malformed_rows_and_use_after_finish(tmp_path: Path) -> None:
    writer = TableWriter(TABLES["gaps"], work_dir=tmp_path, max_part_rows=10, flush_rows=2)
    writer.add((1, 2, 3, 4, 5, 6, None))
    with pytest.raises(ValueError, match="values"):
        writer.add((1, 2, 3))
    writer.close()
    with pytest.raises(ValueError, match="finished"):
        writer.add((1, 2, 3, 4, 5, 6, None))
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(ValueError, match="positive"):
        TableWriter(TABLES["gaps"], work_dir=tmp_path, max_part_rows=0)


def test_an_hour_starts_at_its_utc_hour_and_parses_its_label() -> None:
    assert HOUR.start_wall_ns == START
    assert HourKey.of_wall_ns(START + NS_PER_HOUR - 1) == HOUR
    assert HourKey.parse("2026-09-10T12") == HOUR
    assert HOUR.label == "2026-09-10T12"
    for bad in ("2026-09-10 12", "2026-02-30T01", "2026-09-10T24"):
        with pytest.raises(ValueError, match=r"hour|date|YYYY"):
            HourKey.parse(bad)
