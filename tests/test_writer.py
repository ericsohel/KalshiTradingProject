"""Segment sink: file layout, rotation, overflow recorded in the tape, flushing, shutdown."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import msgspec
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.errors import TapeCorruptionError
from tape.recorder.writer import (
    MAX_SEGMENTS_PER_HOUR,
    OVERFLOW_EVENT,
    HeaderFactory,
    SegmentSink,
    segment_path,
)
from tape.segment import Record, RecordKind, SegmentHeader, SegmentReader, SubscriptionInfo
from tape.timeutil import NS_PER_MS, NS_PER_S, FrozenClock

CONN_ID = 3
POLL_NS = 2 * NS_PER_MS
HOUR_NS = 3_600 * NS_PER_S


def wall(year: int, month: int, day: int, hour: int, *, minute: int = 0, second: int = 0) -> int:
    moment = datetime(year, month, day, hour, minute, second, tzinfo=UTC)
    return int(moment.timestamp()) * NS_PER_S


NOON = wall(2026, 9, 10, 12)
ONE_SECOND_BEFORE_ELEVEN = wall(2026, 9, 10, 10, minute=59, second=59)


def header(subscriptions: list[SubscriptionInfo] | None = None) -> SegmentHeader:
    return SegmentHeader(
        created_wall_ns=NOON,
        host="test",
        env="demo",
        conn_id=CONN_ID,
        ws_url="ws://127.0.0.1/trade-api/ws/v2",
        use_yes_price=True,
        subscriptions=list(subscriptions or []),
        software_version="0.1.0",
        spec_versions={"asyncapi": "2.0.0"},
    )


def frame(n: int) -> Record:
    return Record(
        kind=RecordKind.FRAME,
        conn_id=CONN_ID,
        recv_mono_ns=n,
        recv_wall_ns=NOON + n,
        payload=b'{"n":%d}' % n,
    )


def make_sink(
    root: Path,
    clock: FrozenClock,
    *,
    header_factory: HeaderFactory = header,
    max_queued_records: int = 1_000,
) -> SegmentSink:
    return SegmentSink(
        root,
        conn_id=CONN_ID,
        header_factory=header_factory,
        clock=clock,
        max_queued_records=max_queued_records,
        poll_interval_ns=POLL_NS,
    )


def wait_until(predicate: Callable[[], bool], *, timeout_s: float = 5.0) -> None:
    """Wait on the writer thread without a fixed sleep; always bounded."""
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError("condition not reached")
        time.sleep(0.001)


def segments(root: Path) -> list[Path]:
    return sorted(root.glob("raw/*/*/*.tape.zst"))


def read(path: Path) -> tuple[SegmentHeader, list[Record], bool]:
    with SegmentReader(path) as reader:
        records = list(reader.records())
        return reader.header, records, reader.truncated


def finished(path: Path) -> bool:
    """Whether the file is a complete zstd frame; an unflushed file has no header yet."""
    try:
        return read(path)[2] is False
    except TapeCorruptionError:
        return False


def overflow_notice(dropped: int) -> bytes:
    return msgspec.json.encode({"event": OVERFLOW_EVENT, "dropped": dropped})


def test_segment_path_follows_the_documented_layout() -> None:
    root = Path("/data")
    assert segment_path(root, 3, wall(2026, 9, 10, 7, minute=5, second=1) + 123, 12) == Path(
        "/data/raw/2026-09-10/07/conn-03-0012.tape.zst"
    )
    assert segment_path(root, 123, 0, 0) == Path("/data/raw/1970-01-01/00/conn-123-0000.tape.zst")
    with pytest.raises(ValueError, match="conn_id"):
        segment_path(root, -1, 0, 0)
    with pytest.raises(ValueError, match="wall_ns"):
        segment_path(root, 0, -1, 0)
    with pytest.raises(ValueError, match="counter"):
        segment_path(root, 0, 0, MAX_SEGMENTS_PER_HOUR)


@given(
    st.integers(0, 2**62),
    st.integers(0, 65_535),
    st.integers(0, MAX_SEGMENTS_PER_HOUR - 1),
)
@settings(max_examples=300)
def test_a_segment_lives_in_the_directory_of_its_utc_hour(
    wall_ns: int, conn_id: int, counter: int
) -> None:
    path = segment_path(Path("/r"), conn_id, wall_ns, counter)
    moment = datetime.fromtimestamp(wall_ns // NS_PER_S, tz=UTC)
    assert path.parts[-3:-1] == (moment.strftime("%Y-%m-%d"), moment.strftime("%H"))
    hour_start = wall_ns - wall_ns % HOUR_NS
    assert path.parent == segment_path(Path("/r"), 0, hour_start, 0).parent
    assert path.parent == segment_path(Path("/r"), 0, hour_start + HOUR_NS - 1, 0).parent
    _, conn_text, counter_text = path.name.removesuffix(".tape.zst").split("-")
    assert (int(conn_text), int(counter_text)) == (conn_id, counter)


def test_rotate_closes_the_file_and_the_next_one_carries_a_fresh_header(tmp_path: Path) -> None:
    subscriptions: list[SubscriptionInfo] = []
    clock = FrozenClock(wall_ns=NOON)
    with make_sink(tmp_path, clock, header_factory=lambda: header(subscriptions)) as sink:
        assert sink.put(frame(0))
        assert sink.put(frame(1))
        # Files open lazily on the writer thread; let the first header be taken first.
        wait_until(lambda: sink.stats.records_written == 2)
        sink.rotate()
        sink.rotate()  # a second rotation with nothing between is the same rotation
        subscriptions.append(SubscriptionInfo(sid=1, channel="orderbook_delta", group_id="g"))
        assert sink.put(frame(2))

    first, second = segments(tmp_path)
    assert first == segment_path(tmp_path, CONN_ID, NOON, 0)
    assert second == segment_path(tmp_path, CONN_ID, NOON, 1)
    first_header, first_records, first_truncated = read(first)
    second_header, second_records, second_truncated = read(second)
    assert (first_records, first_truncated, first_header.subscriptions) == (
        [frame(0), frame(1)],
        False,
        [],
    )
    assert (second_records, second_truncated) == ([frame(2)], False)
    assert second_header.subscriptions == subscriptions
    stats = sink.stats
    assert (stats.records_written, stats.records_dropped, stats.files_opened) == (3, 0, 2)
    assert stats.bytes_written > 0
    assert stats.write_errors == 0


def test_a_new_utc_hour_starts_a_new_file(tmp_path: Path) -> None:
    clock = FrozenClock(wall_ns=ONE_SECOND_BEFORE_ELEVEN)
    with make_sink(tmp_path, clock) as sink:
        sink.put(frame(0))
        wait_until(lambda: sink.stats.records_written == 1)
        clock.advance(NS_PER_S)
        sink.put(frame(1))

    assert segments(tmp_path) == [
        tmp_path / "raw/2026-09-10/10/conn-03-0000.tape.zst",
        tmp_path / "raw/2026-09-10/11/conn-03-0000.tape.zst",
    ]
    assert [read(path)[1] for path in segments(tmp_path)] == [[frame(0)], [frame(1)]]


def test_a_file_is_finished_when_its_hour_ends_even_if_nothing_else_arrives(
    tmp_path: Path,
) -> None:
    clock = FrozenClock(wall_ns=ONE_SECOND_BEFORE_ELEVEN)
    with make_sink(tmp_path, clock) as sink:
        sink.put(frame(0))
        wait_until(lambda: sink.stats.records_written == 1)
        (path,) = segments(tmp_path)
        clock.advance(NS_PER_S)
        # Finished means a complete zstd frame: the baker may take the file now.
        wait_until(lambda: finished(path))
        assert read(path)[1] == [frame(0)]
    assert sink.stats.files_opened == 1


def test_existing_files_are_never_overwritten(tmp_path: Path) -> None:
    clock = FrozenClock(wall_ns=NOON)
    existing = segment_path(tmp_path, CONN_ID, NOON, 0)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"precious")

    with make_sink(tmp_path, clock) as first_run:
        first_run.put(frame(0))
    with make_sink(tmp_path, clock) as second_run:
        second_run.put(frame(1))

    assert existing.read_bytes() == b"precious"
    assert read(segment_path(tmp_path, CONN_ID, NOON, 1))[1] == [frame(0)]
    assert read(segment_path(tmp_path, CONN_ID, NOON, 2))[1] == [frame(1)]


def test_overflow_is_written_into_the_tape_where_it_happened(tmp_path: Path) -> None:
    clock = FrozenClock(wall_ns=NOON)
    sink = make_sink(tmp_path, clock, max_queued_records=2)
    # Not started yet, so nothing drains and the bound is exact.
    assert [sink.put(frame(n)) for n in range(4)] == [True, True, False, False]
    assert sink.stats.records_dropped == 2
    sink.start()
    wait_until(lambda: sink.stats.records_written == 2)
    assert sink.put(frame(4))
    sink.close()

    (path,) = segments(tmp_path)
    records = read(path)[1]
    assert records[:2] == [frame(0), frame(1)]
    assert (records[2].kind, records[2].conn_id, records[2].payload) == (
        RecordKind.CONNECTION,
        CONN_ID,
        overflow_notice(2),
    )
    assert records[3] == frame(4)
    stats = sink.stats
    assert (stats.records_written, stats.records_dropped) == (4, 2)


def test_refusals_not_yet_reported_are_written_on_close(tmp_path: Path) -> None:
    sink = make_sink(tmp_path, FrozenClock(wall_ns=NOON), max_queued_records=1)
    assert sink.put(frame(0))
    assert not sink.put(frame(1))
    sink.close()  # never started: drained on this thread

    records = read(segments(tmp_path)[0])[1]
    assert [record.payload for record in records] == [frame(0).payload, overflow_notice(1)]


def test_a_rotation_requested_while_full_lands_before_the_next_record(tmp_path: Path) -> None:
    sink = make_sink(tmp_path, FrozenClock(wall_ns=NOON), max_queued_records=1)
    assert sink.put(frame(0))
    sink.rotate()
    assert not sink.put(frame(1))
    sink.start()
    wait_until(lambda: sink.stats.records_written == 1)
    assert sink.put(frame(2))
    sink.close()

    first, second = segments(tmp_path)
    assert [record.payload for record in read(first)[1]] == [frame(0).payload, overflow_notice(1)]
    assert read(second)[1] == [frame(2)]


def test_an_idle_queue_is_still_flushed_once_the_interval_passes(tmp_path: Path) -> None:
    clock = FrozenClock(wall_ns=NOON)
    with make_sink(tmp_path, clock) as sink:
        sink.put(frame(0))
        wait_until(lambda: sink.stats.records_written == 1)
        time.sleep(10 * POLL_NS / NS_PER_S)
        assert sink.stats.flushes == 0  # only the injected clock paces flushes
        clock.advance(NS_PER_S)
        wait_until(lambda: sink.stats.flushes == 1)
        _, records, truncated = read(segments(tmp_path)[0])
        # Readable although the file is still open, which is what a crash would leave.
        assert (records, truncated) == ([frame(0)], True)


def test_close_is_idempotent_and_later_records_are_refused(tmp_path: Path) -> None:
    sink = make_sink(tmp_path, FrozenClock(wall_ns=NOON))
    sink.start()
    with pytest.raises(RuntimeError, match="twice"):
        sink.start()
    assert sink.put(frame(0))
    sink.close()
    sink.close()
    assert not sink.put(frame(1))
    sink.rotate()
    with pytest.raises(RuntimeError, match="twice"):
        sink.start()
    assert (sink.stats.records_written, sink.stats.records_dropped) == (1, 1)
    assert read(segments(tmp_path)[0])[1] == [frame(0)]


def test_a_sink_closed_before_it_was_started_still_writes_what_it_accepted(
    tmp_path: Path,
) -> None:
    sink = make_sink(tmp_path, FrozenClock(wall_ns=NOON))
    sink.put(frame(0))
    sink.close()
    sink.close()
    assert read(segments(tmp_path)[0])[1] == [frame(0)]


def test_disk_errors_are_counted_and_do_not_stop_the_thread(tmp_path: Path) -> None:
    not_a_directory = tmp_path / "file"
    not_a_directory.write_bytes(b"")
    with make_sink(not_a_directory, FrozenClock(wall_ns=NOON)) as sink:
        assert sink.put(frame(0))
        assert sink.put(frame(1))
        wait_until(lambda: sink.stats.write_errors == 2)
        assert sink.put(frame(2))
    stats = sink.stats
    assert (stats.records_written, stats.records_dropped, stats.write_errors) == (0, 3, 3)
    assert sink.failure is None


def test_a_failing_header_factory_ends_the_thread_and_records_are_refused(
    tmp_path: Path,
) -> None:
    def broken_header() -> SegmentHeader:
        raise RuntimeError("header bug")

    with make_sink(tmp_path, FrozenClock(wall_ns=NOON), header_factory=broken_header) as sink:
        assert sink.put(frame(0))
        wait_until(lambda: sink.failure is not None)
        assert not sink.put(frame(1))
    assert isinstance(sink.failure, RuntimeError)
    assert sink.stats.records_dropped == 1


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("max_queued_records", 0),
        ("flush_interval_ns", 0),
        ("poll_interval_ns", -1),
        ("close_timeout_ns", 0),
        ("conn_id", -1),
    ],
)
def test_bounds_must_be_positive(tmp_path: Path, name: str, value: int) -> None:
    bounds = {
        "conn_id": CONN_ID,
        "max_queued_records": 1,
        "flush_interval_ns": 1,
        "poll_interval_ns": 1,
        "close_timeout_ns": 1,
    }
    bounds[name] = value
    with pytest.raises(ValueError, match=name):
        SegmentSink(
            tmp_path,
            header_factory=header,
            clock=FrozenClock(),
            conn_id=bounds["conn_id"],
            max_queued_records=bounds["max_queued_records"],
            flush_interval_ns=bounds["flush_interval_ns"],
            poll_interval_ns=bounds["poll_interval_ns"],
            close_timeout_ns=bounds["close_timeout_ns"],
        )


@given(
    st.integers(1, 4),
    st.lists(st.sampled_from(["put", "rotate"]), max_size=40),
)
@settings(max_examples=60, deadline=None)
def test_every_accepted_record_reaches_the_tape_in_order_and_every_refusal_is_reported(
    tmp_path_factory: pytest.TempPathFactory, max_queued_records: int, operations: list[str]
) -> None:
    root = tmp_path_factory.mktemp("sink")
    sink = make_sink(root, FrozenClock(wall_ns=NOON), max_queued_records=max_queued_records)
    accepted: list[Record] = []
    refused = 0
    for index, operation in enumerate(operations):
        if operation == "rotate":
            sink.rotate()
            continue
        record = frame(index)
        if sink.put(record):
            accepted.append(record)
        else:
            refused += 1
    sink.close()

    written = [record for path in segments(root) for record in read(path)[1]]
    assert [record for record in written if record.kind is RecordKind.FRAME] == accepted
    reported = [
        msgspec.json.decode(record.payload)["dropped"]
        for record in written
        if record.kind is RecordKind.CONNECTION
    ]
    assert sum(reported) == refused
    assert all(read(path)[1] for path in segments(root))
    assert sink.stats.records_dropped == refused
    assert sink.stats.records_written == len(written)
