"""The daily manifest: round trips, strict decoding, merging hours and prunes, integrity numbers."""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import msgspec
import pytest

from tape.bake.layout import NS_PER_HOUR, HourKey
from tape.bake.manifest import (
    SECONDS_PER_DAY,
    AuditFacts,
    ConnectionSpan,
    GapFacts,
    HourBake,
    HourIntegrity,
    Manifest,
    PartEntry,
    PrunedSegment,
    RecordAccounting,
    SegmentEntry,
    Sleep,
    decode_manifest,
    encode_manifest,
    new_manifest,
    parse_chrony_tracking,
    read_manifest,
    summarize_audits,
    summarize_gaps,
    summarize_uptime,
    with_hour,
    with_pruned,
    write_manifest,
)
from tape.errors import TapeCorruptionError
from tape.timeutil import NS_PER_S

DAY = dt.date(2026, 9, 10)
MIDNIGHT = HourKey(DAY, 0).start_wall_ns
NO_GAPS = GapFacts(count=0, stale_market_ns=0, observed_market_ns=0)
NO_AUDITS = AuditFacts(exact=0, consistent=0, inconsistent=0, undecidable=0, levels_mismatched=0)


def seconds(value: int) -> int:
    return MIDNIGHT + value * NS_PER_S


def span(
    conn_id: int, start: int, end: int, *, opened: bool = True, closed: bool = True
) -> ConnectionSpan:
    return ConnectionSpan(
        conn_id=conn_id,
        start_wall_ns=seconds(start),
        end_wall_ns=seconds(end),
        opened=opened,
        closed=closed,
    )


def hour_bake(
    hour: int,
    *,
    spans: Sequence[ConnectionSpan] = (),
    sleeps: Sequence[Sleep] = (),
    gaps: GapFacts = NO_GAPS,
    audits: AuditFacts = NO_AUDITS,
    frames: int = 3,
    failures: int = 0,
) -> HourBake:
    decode_failures = {"frame_payload": failures} if failures else {}
    return HourBake(
        hour=hour,
        bake_version=1,
        software_version="0.1.0",
        accounting=RecordAccounting(
            records={"frame": frames + failures},
            baked={"orderbook_delta": frames},
            not_baked={},
            decode_failures=decode_failures,
        ),
        integrity=HourIntegrity(
            spans=tuple(spans),
            sleeps=tuple(sleeps),
            gaps=gaps,
            audits=audits,
            writer_overflow_dropped=0,
        ),
    )


def segment(hour: int, name: str = "conn-02-0000", *, digest: str = "a") -> SegmentEntry:
    return SegmentEntry(
        path=f"{DAY.isoformat()}/{hour:02d}/{name}.tape.zst",
        hour=hour,
        bytes=100,
        sha256=digest * 64,
        records=3,
        first_recv_wall_ns=1,
        last_recv_wall_ns=2,
        truncated=False,
    )


def part(hour: int, table: str = "deltas", index: int = 0, rows: int = 3) -> PartEntry:
    return PartEntry(
        path=f"{table}/dt={DAY.isoformat()}/hour={hour:02d}/part-{index}.parquet",
        hour=hour,
        rows=rows,
        bytes=50,
        sha256="b" * 64,
    )


def baked(*hours: int) -> Manifest:
    manifest = new_manifest(DAY, software_version="0.1.0")
    for hour in hours:
        manifest = with_hour(
            manifest,
            bake=hour_bake(hour, spans=[span(2, hour * 3600, hour * 3600 + 60)]),
            segments=[segment(hour)],
            parts={"deltas": [part(hour)], "trades": [part(hour, "trades", rows=1)]},
            software_version="0.1.0",
            clock_offset_ms=None,
        )
    return manifest


def test_a_manifest_round_trips_and_encodes_the_same_bytes_every_time() -> None:
    manifest = baked(3, 1)
    data = encode_manifest(manifest)

    assert decode_manifest(data) == manifest
    assert encode_manifest(decode_manifest(data)) == data
    document = json.loads(data)
    assert list(document) == [
        "version",
        "date",
        "software_version",
        "segments",
        "tables",
        "bake",
        "uptime",
        "gaps",
        "audits",
        "clock",
        "pruned",
    ]
    assert document["date"] == "2026-09-10"
    assert sorted(document["tables"]) == [
        "audits",
        "deltas",
        "gaps",
        "lifecycle",
        "snapshots",
        "trades",
    ]
    assert [entry["hour"] for entry in document["bake"]["hours"]] == [1, 3]
    assert document["tables"]["deltas"]["rows"] == 6
    assert document["clock"] == {"chrony_offset_ms": None}


def mutated(change: Any) -> bytes:  # Any: a JSON tree edited in place
    document = json.loads(encode_manifest(baked(1)))
    change(document)
    return json.dumps(document).encode()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda d: d.update(surprise=1), "unknown field `surprise`"),
        (lambda d: d["uptime"].update(extra=1), r"unknown field `extra`"),
        (lambda d: d.update(version=2), r"\$\.version"),
        (lambda d: d.pop("pruned"), "missing required field `pruned`"),
        (lambda d: d["tables"].pop("gaps"), "tables must be exactly"),
        (lambda d: d["tables"]["deltas"].update(rows=99), "differ from the sum"),
        (lambda d: d["segments"][0].update(sha256="xyz"), r"\$\.segments\[0\]\.sha256"),
        (lambda d: d["segments"][0].update(path="../../etc/passwd"), r"\$\.segments\[0\]\.path"),
        (lambda d: d["segments"][0].update(hour=2), "not in hour 2"),
        (lambda d: d["bake"]["hours"].append(d["bake"]["hours"][0]), "unique and in order"),
        (
            lambda d: d["bake"]["hours"][0]["accounting"]["records"].update(frame=4),
            "4 records read but 3 accounted for",
        ),
        (
            lambda d: d["bake"]["hours"][0]["accounting"]["records"].update(ticks=0),
            "unknown record kinds",
        ),
        (lambda d: d["bake"]["hours"][0].update(bake_version=0), r"bake_version"),
        (
            lambda d: d["pruned"].append(
                {
                    "path": d["segments"][0]["path"],
                    "bytes": 1,
                    "sha256": "a" * 64,
                    "pruned_wall_ns": 5,
                }
            ),
            "does not match a baked segment",
        ),
        (
            lambda d: d["tables"]["trades"]["files"][0].update(
                path="deltas/dt=2026-09-10/hour=01/part-9.parquet"
            ),
            "is not under",
        ),
        (lambda d: d["bake"].update(hours=[]), "belongs to hour 1, not baked"),
    ],
)
def test_decoding_refuses_anything_but_a_consistent_version_1_manifest(
    change: Any, message: str
) -> None:
    with pytest.raises(TapeCorruptionError, match=message):
        decode_manifest(mutated(change))


def test_malformed_json_is_corruption() -> None:
    with pytest.raises(TapeCorruptionError, match=r"where\.json"):
        decode_manifest(b"{", source="where.json")


def test_a_new_bake_of_an_hour_replaces_only_that_hour() -> None:
    manifest = baked(1, 2)
    rebaked = with_hour(
        manifest,
        bake=hour_bake(2, frames=7),
        segments=[segment(2, "conn-02-0001", digest="c")],
        parts={"deltas": [part(2, index=0, rows=4), part(2, index=1, rows=3)]},
        software_version="0.2.0",
        clock_offset_ms=4,
    )

    assert [s.path for s in rebaked.segments] == [
        "2026-09-10/01/conn-02-0000.tape.zst",
        "2026-09-10/02/conn-02-0001.tape.zst",
    ]
    assert rebaked.tables["deltas"].rows == 3 + 7
    assert rebaked.tables["trades"].files == (part(1, "trades", rows=1),)
    entry = rebaked.hour_bake(2)
    assert entry is not None
    assert entry.accounting.total == 7
    assert rebaked.hour_bake(1) == manifest.hour_bake(1)
    assert (rebaked.software_version, rebaked.clock.chrony_offset_ms) == ("0.2.0", 4)


def test_an_hour_with_pruned_segments_cannot_be_merged_again() -> None:
    manifest = baked(1)
    first = manifest.segments[0]
    pruned = with_pruned(
        manifest,
        [PrunedSegment(path=first.path, bytes=first.bytes, sha256=first.sha256, pruned_wall_ns=9)],
    )
    with pytest.raises(ValueError, match="pruned segments"):
        with_hour(
            pruned,
            bake=hour_bake(1),
            segments=[],
            parts={},
            software_version="0",
            clock_offset_ms=None,
        )
    with pytest.raises(ValueError, match="must belong to hour 02"):
        with_hour(
            manifest,
            bake=hour_bake(2),
            segments=[segment(1)],
            parts={},
            software_version="0",
            clock_offset_ms=None,
        )


def test_a_segment_pruned_twice_keeps_its_first_record_and_stays_listed() -> None:
    manifest = baked(1)
    first = manifest.segments[0]
    record = PrunedSegment(
        path=first.path, bytes=first.bytes, sha256=first.sha256, pruned_wall_ns=9
    )
    once = with_pruned(manifest, [record])
    twice = with_pruned(once, [msgspec.structs.replace(record, pruned_wall_ns=99)])

    assert twice.pruned == (record,)
    assert twice.segments == manifest.segments
    with pytest.raises(ValueError, match="does not match"):
        with_pruned(manifest, [msgspec.structs.replace(record, sha256="d" * 64)])


def test_uptime_unions_connections_joins_continuations_and_subtracts_sleep() -> None:
    hours = [
        hour_bake(
            0,
            spans=[span(1, 100, 3599, closed=False), span(2, 200, 1000), span(2, 900, 1500)],
            sleeps=[Sleep(start_wall_ns=seconds(1200), end_wall_ns=seconds(1300))],
        ),
        # Continues connection 1 across the hour; no close came between.
        hour_bake(1, spans=[span(1, 3600, 4000, opened=False)]),
        hour_bake(23, spans=[span(3, 86_000, 87_000, closed=False)]),
    ]
    uptime = summarize_uptime(DAY, hours)
    # 100 s to 4000 s, less 100 s asleep, and the last span clipped at midnight.
    expected = (4000 - 100) - 100 + (SECONDS_PER_DAY - 86_000)
    assert (uptime.seconds_recording, uptime.seconds_in_day) == (expected, SECONDS_PER_DAY)
    assert uptime.ratio == (expected, SECONDS_PER_DAY)


def test_a_new_connection_after_an_unclosed_span_is_not_joined() -> None:
    hours = [hour_bake(0, spans=[span(1, 0, 100, closed=False), span(1, 200, 300, opened=True)])]
    assert summarize_uptime(DAY, hours).seconds_recording == 200


def test_gap_and_audit_ratios_are_exact_integer_pairs() -> None:
    hours = [
        hour_bake(
            0,
            gaps=GapFacts(
                count=2, stale_market_ns=3 * NS_PER_S + 1, observed_market_ns=100 * NS_PER_S
            ),
            audits=AuditFacts(
                exact=5, consistent=2, inconsistent=1, undecidable=4, levels_mismatched=6
            ),
        ),
        hour_bake(
            1,
            gaps=GapFacts(count=1, stale_market_ns=NS_PER_S, observed_market_ns=50 * NS_PER_S),
            audits=AuditFacts(
                exact=1, consistent=0, inconsistent=0, undecidable=0, levels_mismatched=0
            ),
        ),
    ]
    gaps = summarize_gaps(hours)
    assert (gaps.count, gaps.market_seconds_affected, gaps.market_seconds_observed) == (3, 4, 150)
    assert gaps.share == (4, 150)
    audits = summarize_audits(hours)
    assert (audits.books_sampled, audits.books_undecidable, audits.levels_mismatched) == (9, 4, 6)
    assert audits.exact_ratio == (6, 9)
    assert audits.consistency_ratio == (8, 9)
    empty = new_manifest(DAY, software_version="0.1.0")
    assert empty.audits.consistency_ratio == (0, 0)
    assert empty.gaps.share == (0, 0)
    assert empty.uptime.seconds_recording == 0


def test_accounting_counts_decode_failures_toward_the_total() -> None:
    entry = hour_bake(0, frames=2, failures=3)
    assert (entry.accounting.total, entry.accounting.failures) == (5, 3)
    with pytest.raises(ValueError, match="accounted for"):
        RecordAccounting(records={"frame": 2}, baked={}, not_baked={}, decode_failures={})


def test_reading_a_missing_manifest_is_none_and_writing_leaves_no_temporary(tmp_path: Path) -> None:
    path = tmp_path / "manifests" / "2026-09-10.json"
    assert read_manifest(path) is None
    manifest = baked(1)
    write_manifest(path, manifest)
    assert read_manifest(path) == manifest
    assert sorted(p.name for p in path.parent.iterdir()) == ["2026-09-10.json"]


@pytest.mark.parametrize(
    ("text", "offset_ms"),
    [
        (
            "A29FC87B,ntp.example,3,1789000000.123,0.000001234,-0.000002,0.00001,-12.3,0.001,0.02,0.01,0.001,64.2,Normal\n",
            0,
        ),
        ("A29FC87B,ntp.example,3,1789000000.123,0.0125,0,0,0,0,0,0,0,0,Normal", 12),
        ("A29FC87B,ntp.example,3,1789000000.123,-0.0135,0,0,0,0,0,0,0,0,Normal", -14),
        ("", None),
        ("506 Cannot talk to daemon", None),
        ("a,b,c,d,not-a-number,f", None),
        ("a,b,c,d,NaN,f", None),
    ],
)
def test_the_chrony_offset_is_read_from_the_system_time_field(
    text: str, offset_ms: int | None
) -> None:
    assert parse_chrony_tracking(text) == offset_ms


def test_an_hour_key_is_the_manifest_hour() -> None:
    assert HourKey(DAY, 5).start_wall_ns == MIDNIGHT + 5 * NS_PER_HOUR
    with pytest.raises(ValueError, match="hour must be"):
        HourKey(DAY, 24)
