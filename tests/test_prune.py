"""Pruning raw segments (ADR 0025): the decision as a property, fault injection, and deletion."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import msgspec
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import tape.bake.prune as prune_module
from tape.bake import BAKE_VERSION, DataLayout, HourKey, bake_hour, read_manifest, record_bake
from tape.bake.files import sha256_file
from tape.bake.layout import NS_PER_HOUR
from tape.bake.manifest import (
    AuditFacts,
    GapFacts,
    HourBake,
    HourIntegrity,
    Manifest,
    PartEntry,
    PrunedSegment,
    RecordAccounting,
    SegmentEntry,
    new_manifest,
    with_hour,
    with_pruned,
)
from tape.bake.prune import (
    PRUNE_REASONS,
    HourFacts,
    PartOnDisk,
    PruneDecision,
    PruneReason,
    SegmentOnDisk,
    apply,
    decide,
    survey,
)
from tape.errors import ArchiveError
from tape.segment import write_keyframe
from tape.timeutil import FrozenClock
from tests.fakes.synthetic_tape import HOUR, SECOND, SegmentScript

RETENTION_HOURS = 72
PAST_WINDOW = HOUR.end_wall_ns + RETENTION_HOURS * NS_PER_HOUR + 1
INSIDE_WINDOW = HOUR.end_wall_ns + RETENTION_HOURS * NS_PER_HOUR

# ---------------------------------------------------------------- the decision, as a property

SegmentState = str  # match | resized | rehashed | added | missing | pruned
PartState = str  # ok | missing | changed


def _digest(index: int, salt: str = "a") -> str:
    return (salt + format(index, "063x"))[:64]


def scenario(
    *,
    baked: bool,
    current: bool,
    failures: bool,
    segments: list[SegmentState],
    parts: list[PartState],
    hashed: bool,
) -> tuple[HourFacts, Manifest | None]:
    """Build what the manifest says and what is on disk for one combination of conditions."""
    listed: list[SegmentEntry] = []
    on_disk: list[SegmentOnDisk] = []
    pruned: list[PrunedSegment] = []
    for index, state in enumerate(segments):
        path = f"2026-09-10/12/conn-02-{index:04d}.tape.zst"
        entry = SegmentEntry(
            path=path,
            hour=12,
            bytes=100 + index,
            sha256=_digest(index),
            records=1,
            first_recv_wall_ns=1,
            last_recv_wall_ns=1,
            truncated=False,
        )
        if state != "added" and baked:
            listed.append(entry)
        if state == "pruned" and baked:
            pruned.append(
                PrunedSegment(path=path, bytes=entry.bytes, sha256=entry.sha256, pruned_wall_ns=1)
            )
        if state in ("missing", "pruned"):
            continue
        size = entry.bytes + (1 if state == "resized" else 0)
        digest = _digest(index, "f") if state == "rehashed" else entry.sha256
        on_disk.append(SegmentOnDisk(path=path, bytes=size, sha256=digest if hashed else None))
    part_entries = [
        PartEntry(
            path=f"deltas/dt=2026-09-10/hour=12/part-{index}.parquet",
            hour=12,
            rows=1,
            bytes=10,
            sha256=_digest(index, "b"),
        )
        for index in range(len(parts))
    ]
    part_facts = tuple(
        PartOnDisk(
            path=entry.path,
            exists=state != "missing",
            sha256=None
            if not hashed or state == "missing"
            else (_digest(99, "c") if state == "changed" else entry.sha256),
        )
        for entry, state in zip(part_entries, parts, strict=True)
    )
    facts = HourFacts(hour=HOUR, segments=tuple(on_disk), parts=part_facts)
    if not baked:
        return facts, None
    bake = HourBake(
        hour=12,
        bake_version=BAKE_VERSION if current else BAKE_VERSION - 1 or BAKE_VERSION + 1,
        software_version="0.1.0",
        accounting=RecordAccounting(
            records={"frame": 2},
            baked={"orderbook_delta": 1 if failures else 2},
            not_baked={},
            decode_failures={"frame_payload": 1} if failures else {},
        ),
        integrity=HourIntegrity(
            spans=(),
            sleeps=(),
            gaps=GapFacts(count=0, stale_market_ns=0, observed_market_ns=0),
            audits=AuditFacts(
                exact=0, consistent=0, inconsistent=0, undecidable=0, levels_mismatched=0
            ),
            writer_overflow_dropped=0,
        ),
    )
    manifest = with_hour(
        new_manifest(HOUR.date, software_version="0.1.0"),
        bake=bake,
        segments=listed,
        parts={"deltas": part_entries},
        software_version="0.1.0",
        clock_offset_ms=None,
    )
    return facts, with_pruned(manifest, pruned)


@given(
    past=st.booleans(),
    baked=st.booleans(),
    current=st.booleans(),
    failures=st.booleans(),
    segments=st.lists(
        st.sampled_from(["match", "resized", "rehashed", "added", "missing", "pruned"]),
        min_size=1,
        max_size=4,
    ),
    parts=st.lists(st.sampled_from(["ok", "missing", "changed"]), max_size=3),
    hashed=st.booleans(),
)
@settings(max_examples=400, deadline=None)
def test_an_hour_is_prunable_if_and_only_if_every_condition_holds(
    *,
    past: bool,
    baked: bool,
    current: bool,
    failures: bool,
    segments: list[SegmentState],
    parts: list[PartState],
    hashed: bool,
) -> None:
    facts, manifest = scenario(
        baked=baked,
        current=current,
        failures=failures,
        segments=segments,
        parts=parts,
        hashed=hashed,
    )
    decision = decide(
        facts,
        manifest,
        now_wall_ns=PAST_WINDOW if past else INSIDE_WINDOW,
        retention_hours=RETENTION_HOURS,
        bake_version=BAKE_VERSION,
    )

    something_to_verify = any(s in ("match", "rehashed") for s in segments) or any(
        p in ("ok", "changed") for p in parts
    )
    all_hold = (
        past
        and baked
        and current
        and not failures
        and all(state in ("match", "pruned") for state in segments)
        and all(state == "ok" for state in parts)
        and (hashed or not something_to_verify)
    )
    assert decision.prunable is all_hold

    expected: set[PruneReason] = set()
    if not past:
        expected.add("inside_retention_window")
    if not baked:
        expected.add("not_baked")
    else:
        if not current:
            expected.add("bake_version_stale")
        if failures:
            expected.add("decode_failures")
        if "added" in segments:
            expected.add("segment_added")
        if "resized" in segments or (hashed and "rehashed" in segments):
            expected.add("segment_changed")
        if "missing" in segments:
            expected.add("segment_missing")
        if "missing" in parts:
            expected.add("baked_file_missing")
        if hashed and "changed" in parts:
            expected.add("baked_file_changed")
        if not expected and not hashed and something_to_verify:
            expected.add("unverified")
    assert set(decision.reasons) == expected
    assert list(decision.reasons) == [reason for reason in PRUNE_REASONS if reason in expected]
    assert decision.segments == (facts.segments if all_hold else ())


def test_a_manifest_of_another_day_is_not_a_bake() -> None:
    facts, manifest = scenario(
        baked=True, current=True, failures=False, segments=["match"], parts=[], hashed=True
    )
    assert manifest is not None
    other_day = msgspec.structs.replace(facts, hour=HourKey(dt.date(2026, 9, 11), 12))
    decision = decide(
        other_day,
        manifest,
        now_wall_ns=PAST_WINDOW * 2,
        retention_hours=RETENTION_HOURS,
        bake_version=BAKE_VERSION,
    )
    assert decision.reasons == ("not_baked",)


# ---------------------------------------------------------------------- the archive on disk


@pytest.fixture
def layout(tmp_path: Path) -> DataLayout:
    return DataLayout.under(tmp_path / "data")


def at(seconds: int, hour: HourKey = HOUR) -> int:
    return hour.start_wall_ns + seconds * SECOND


def write_hour(layout: DataLayout, hour: HourKey = HOUR, *, bad_frame: bool = False) -> None:
    for conn_id in (2, 3):
        script = SegmentScript(conn_id=conn_id)
        script.opened(at(1, hour))
        script.subscribed(at(2, hour), channel="orderbook_delta", sid=1)
        script.snapshot(at(3, hour), sid=1, seq=1, ticker=f"KX{conn_id}-26SEP10")
        for step in range(20):
            script.delta(at(4 + step, hour), sid=1, seq=step + 2, ticker=f"KX{conn_id}-26SEP10")
        if bad_frame:
            script.frame(at(30, hour), b"{broken")
        script.write(layout, hour, 0)


def bake_and_record(layout: DataLayout, hour: HourKey = HOUR) -> Manifest:
    report = bake_hour(layout, hour, software_version="0.1.0")
    return record_bake(layout, report, software_version="0.1.0", clock_offset_ms=None)


def decisions(
    layout: DataLayout, *, now_wall_ns: int = PAST_WINDOW, bake_version: int = BAKE_VERSION
) -> dict[str, PruneDecision]:
    found = survey(
        layout, now_wall_ns=now_wall_ns, retention_hours=RETENTION_HOURS, bake_version=bake_version
    )
    return {decision.hour.label: decision for decision in found}


def snapshot_files(root: Path) -> dict[Path, bytes]:
    return {path: path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def write_a_keyframe(layout: DataLayout) -> None:
    directory = layout.keyframe_dir(HOUR)
    directory.mkdir(parents=True)
    write_keyframe(directory / "05.parquet", [])


def test_a_verified_hour_past_the_window_is_prunable_and_nothing_is_deleted_without_apply(
    layout: DataLayout,
) -> None:
    write_hour(layout)
    bake_and_record(layout)
    before = snapshot_files(layout.raw.parent)

    found = decisions(layout)
    assert found[HOUR.label].prunable
    assert found[HOUR.label].reasons == ()
    assert [segment.path for segment in found[HOUR.label].segments] == [
        "2026-09-10/12/conn-02-0000.tape.zst",
        "2026-09-10/12/conn-03-0000.tape.zst",
    ]
    assert all(segment.sha256 is not None for segment in found[HOUR.label].segments)
    assert snapshot_files(layout.raw.parent) == before


def test_apply_records_every_deletion_and_touches_nothing_but_raw_segments(
    layout: DataLayout,
) -> None:
    earlier = HourKey(HOUR.date, 11)
    write_hour(layout, earlier)
    write_hour(layout)
    bake_and_record(layout, earlier)
    bake_and_record(layout)
    write_a_keyframe(layout)
    kept = {**snapshot_files(layout.baked), **snapshot_files(layout.keyframes)}
    hashes = {layout.segment_name(path): sha256_file(path) for path in layout.segment_files(HOUR)}
    # Only the later hour is past the window.
    now = HOUR.end_wall_ns + RETENTION_HOURS * NS_PER_HOUR + 1
    now_minus = now - NS_PER_HOUR

    found = survey(
        layout, now_wall_ns=now, retention_hours=RETENTION_HOURS + 1, bake_version=BAKE_VERSION
    )
    assert [d.prunable for d in found] == [True, False]
    found = survey(
        layout,
        now_wall_ns=now_minus + 2 * NS_PER_HOUR,
        retention_hours=RETENTION_HOURS + 1,
        bake_version=BAKE_VERSION,
    )
    pruned = apply(layout, found, clock=FrozenClock(wall_ns=777))

    assert {record.path for record in pruned} == set(hashes) | {
        f"2026-09-10/11/conn-0{conn}-0000.tape.zst" for conn in (2, 3)
    }
    assert not layout.hour_dir(HOUR).exists()
    assert not layout.hour_dir(HOUR).parent.exists()
    manifest = read_manifest(layout.manifest_path(HOUR.date))
    assert manifest is not None
    assert {
        record.path: record.sha256 for record in manifest.pruned if record.path in hashes
    } == hashes
    assert {record.pruned_wall_ns for record in manifest.pruned} == {777}
    assert len(manifest.segments) == 4
    assert {**snapshot_files(layout.baked), **snapshot_files(layout.keyframes)} == kept
    assert decisions(layout) == {}


def test_the_current_hour_and_hours_inside_the_window_are_never_pruned(layout: DataLayout) -> None:
    write_hour(layout)
    bake_and_record(layout)
    for now in (HOUR.start_wall_ns + 10 * SECOND, INSIDE_WINDOW):
        found = survey(
            layout, now_wall_ns=now, retention_hours=RETENTION_HOURS, bake_version=BAKE_VERSION
        )
        assert found[0].reasons == ("inside_retention_window",)
        assert apply(layout, found, clock=FrozenClock()) == ()
    assert len(layout.segment_files(HOUR)) == 2


def test_an_hour_never_baked_is_not_prunable(layout: DataLayout) -> None:
    write_hour(layout)
    assert decisions(layout)[HOUR.label].reasons == ("not_baked",)


@pytest.mark.parametrize(
    ("fault", "reason"),
    [
        ("append_to_segment", "segment_changed"),
        ("rewrite_segment_byte", "segment_changed"),
        ("add_segment", "segment_added"),
        ("delete_segment", "segment_missing"),
        ("delete_part", "baked_file_missing"),
        ("rewrite_part_byte", "baked_file_changed"),
    ],
)
def test_every_injected_fault_blocks_pruning_with_its_reason(
    layout: DataLayout, fault: str, reason: PruneReason
) -> None:
    write_hour(layout)
    manifest = bake_and_record(layout)
    segment = layout.segment_files(HOUR)[0]
    part = layout.part_path(manifest.hour_parts(HOUR.hour)[0].path)
    match fault:
        case "append_to_segment":
            with segment.open("ab") as handle:
                handle.write(b"x")
        case "rewrite_segment_byte":
            data = bytearray(segment.read_bytes())
            data[-1] ^= 0xFF
            segment.write_bytes(bytes(data))
        case "add_segment":
            SegmentScript(conn_id=9).opened(at(50)).write(layout, HOUR, 0)
        case "delete_segment":
            segment.unlink()
        case "delete_part":
            part.unlink()
        case "rewrite_part_byte":
            data = bytearray(part.read_bytes())
            data[len(data) // 2] ^= 0xFF
            part.write_bytes(bytes(data))
    before = snapshot_files(layout.raw)

    found = survey(
        layout, now_wall_ns=PAST_WINDOW, retention_hours=RETENTION_HOURS, bake_version=BAKE_VERSION
    )
    assert found[0].reasons == (reason,)
    assert apply(layout, found, clock=FrozenClock()) == ()
    assert snapshot_files(layout.raw) == before


def test_a_decode_failure_blocks_pruning(layout: DataLayout) -> None:
    write_hour(layout, bad_frame=True)
    manifest = bake_and_record(layout)
    entry = manifest.hour_bake(HOUR.hour)
    assert entry is not None
    assert entry.accounting.decode_failures == {"frame_envelope": 2}
    assert decisions(layout)[HOUR.label].reasons == ("decode_failures",)


def test_a_bake_version_bump_blocks_pruning_until_the_hour_is_baked_again(
    layout: DataLayout,
) -> None:
    write_hour(layout)
    bake_and_record(layout)
    assert decisions(layout, bake_version=BAKE_VERSION + 1)[HOUR.label].reasons == (
        "bake_version_stale",
    )


def test_a_crash_between_recording_and_deleting_is_finished_by_the_next_run(
    layout: DataLayout, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_hour(layout)
    bake_and_record(layout)
    found = survey(
        layout, now_wall_ns=PAST_WINDOW, retention_hours=RETENTION_HOURS, bake_version=BAKE_VERSION
    )

    def crash(self: Path, missing_ok: bool = False) -> None:
        raise OSError("power lost")

    with monkeypatch.context() as patched:
        patched.setattr(Path, "unlink", crash)
        with pytest.raises(OSError, match="power lost"):
            apply(layout, found, clock=FrozenClock(wall_ns=1))

    manifest = read_manifest(layout.manifest_path(HOUR.date))
    assert manifest is not None
    assert len(manifest.pruned) == 2
    assert len(layout.segment_files(HOUR)) == 2

    again = survey(
        layout, now_wall_ns=PAST_WINDOW, retention_hours=RETENTION_HOURS, bake_version=BAKE_VERSION
    )
    assert again[0].prunable
    apply(layout, again, clock=FrozenClock(wall_ns=2))
    finished = read_manifest(layout.manifest_path(HOUR.date))
    assert finished is not None
    assert {record.pruned_wall_ns for record in finished.pruned} == {1}
    assert layout.segment_files(HOUR) == ()


def test_apply_skips_decisions_that_are_not_prunable_and_refuses_unverified_segments(
    layout: DataLayout,
) -> None:
    write_hour(layout)
    bake_and_record(layout)
    path = layout.segment_name(layout.segment_files(HOUR)[0])
    unverified = SegmentOnDisk(path=path, bytes=1)
    refused = PruneDecision(
        hour=HOUR, prunable=False, reasons=("not_baked",), segments=(unverified,)
    )
    assert apply(layout, [refused], clock=FrozenClock()) == ()
    assert len(layout.segment_files(HOUR)) == 2
    with pytest.raises(ArchiveError, match="not verified"):
        apply(
            layout,
            [PruneDecision(hour=HOUR, prunable=True, reasons=(), segments=(unverified,))],
            clock=FrozenClock(),
        )
    assert len(layout.segment_files(HOUR)) == 2


def test_files_are_hashed_only_for_hours_nothing_cheaper_rules_out(
    layout: DataLayout, monkeypatch: pytest.MonkeyPatch
) -> None:
    write_hour(layout)
    bake_and_record(layout)
    hashed: list[Path] = []

    def counting(path: Path) -> str:
        hashed.append(path)
        return sha256_file(path)

    monkeypatch.setattr(prune_module, "sha256_file", counting)
    survey(
        layout,
        now_wall_ns=INSIDE_WINDOW,
        retention_hours=RETENTION_HOURS,
        bake_version=BAKE_VERSION,
    )
    assert hashed == []
    survey(
        layout, now_wall_ns=PAST_WINDOW, retention_hours=RETENTION_HOURS, bake_version=BAKE_VERSION
    )
    assert len(hashed) == 2 + len(list(layout.baked.rglob("*.parquet")))
