"""Delete raw segments only from hours a verified bake has made safe to lose (ADR 0025).

Responsibility: decide, for each hour of raw segments, whether it is prunable and every reason it
is not, as a pure function of what the day's manifest says and what is on disk (:func:`decide`);
gather those facts from disk (:func:`survey`); and delete the segments of prunable hours, recording
each deletion in the manifest first (:func:`apply`).

Invariants: an hour is prunable if and only if it ended more than the retention window ago; its
manifest holds a bake of it by the current bake version; every segment on disk is listed with the
same size and hash, and every listed segment is on disk or recorded as pruned; the bake accounted
for every record with no decode failure; and every part file of the bake is on disk with its
recorded hash. Nothing is deleted except by :func:`apply`. A deletion is written to the manifest
and synced before the file is unlinked, so a crash never leaves a deleted segment unrecorded. Only
raw segment files are deleted, with the hour and day directories they leave empty; keyframes,
baked tables, and manifests never are. A file is hashed only when nothing cheaper already rules
its hour out, so a run hashes little more than the hours that newly left the window.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections.abc import Iterable, Sequence
from typing import Final, Literal

import msgspec

from tape.bake.files import sha256_file, sync_dir
from tape.bake.layout import NS_PER_HOUR, DataLayout, HourKey
from tape.bake.manifest import Manifest, PrunedSegment, read_manifest, with_pruned, write_manifest
from tape.errors import ArchiveError
from tape.timeutil import Clock

__all__ = [
    "PRUNE_REASONS",
    "HourFacts",
    "PartOnDisk",
    "PruneDecision",
    "PruneReason",
    "SegmentOnDisk",
    "apply",
    "decide",
    "survey",
]

type PruneReason = Literal[
    "inside_retention_window",
    "not_baked",
    "bake_version_stale",
    "segment_added",
    "segment_changed",
    "segment_missing",
    "decode_failures",
    "baked_file_missing",
    "baked_file_changed",
    "unverified",
]
"""Why an hour is not prunable."""

PRUNE_REASONS: Final[tuple[PruneReason, ...]] = (
    "inside_retention_window",
    "not_baked",
    "bake_version_stale",
    "segment_added",
    "segment_changed",
    "segment_missing",
    "decode_failures",
    "baked_file_missing",
    "baked_file_changed",
    "unverified",
)
"""Every reason, in the order a decision lists them."""

_log = logging.getLogger(__name__)


class SegmentOnDisk(msgspec.Struct, frozen=True, kw_only=True):
    """A raw segment file present now.

    Attributes:
        path: Its name relative to ``raw/``.
        bytes: Its size.
        sha256: Hash of its content, or ``None`` when not computed.
    """

    path: str
    bytes: int
    sha256: str | None = None


class PartOnDisk(msgspec.Struct, frozen=True, kw_only=True):
    """A part file the manifest lists, as found on disk.

    Attributes:
        path: Its name relative to ``baked/``.
        exists: Whether the file is there.
        sha256: Hash of its content, or ``None`` when missing or not computed.
    """

    path: str
    exists: bool
    sha256: str | None = None


class HourFacts(msgspec.Struct, frozen=True, kw_only=True):
    """What is on disk for one hour.

    Attributes:
        hour: The hour.
        segments: Its raw segment files.
        parts: The part files its manifest lists for it.
    """

    hour: HourKey
    segments: tuple[SegmentOnDisk, ...]
    parts: tuple[PartOnDisk, ...]


class PruneDecision(msgspec.Struct, frozen=True, kw_only=True):
    """Whether one hour may be pruned.

    Attributes:
        hour: The hour.
        prunable: Whether every condition of ADR 0025 holds.
        reasons: Every condition that does not hold, in :data:`PRUNE_REASONS` order; empty exactly
            when prunable.
        segments: The segments pruning deletes, each with its hash; empty unless prunable.
    """

    hour: HourKey
    prunable: bool
    reasons: tuple[PruneReason, ...]
    segments: tuple[SegmentOnDisk, ...]

    @property
    def bytes(self) -> int:
        """Bytes pruning frees."""
        return sum(segment.bytes for segment in self.segments)


def decide(
    facts: HourFacts,
    manifest: Manifest | None,
    *,
    now_wall_ns: int,
    retention_hours: int,
    bake_version: int,
) -> PruneDecision:
    """Decide whether an hour may be pruned, and every reason it may not.

    A hash left out of ``facts`` is a check not yet made: the hour is not prunable, and
    ``unverified`` is given as the reason only when no other reason already applies.

    Args:
        facts: What is on disk for the hour.
        manifest: The manifest of the hour's day, or ``None`` when there is none.
        now_wall_ns: The current wall-clock time.
        retention_hours: How long after it ends an hour's raw segments are kept.
        bake_version: The current bake version.

    Returns:
        The decision.
    """
    hour = facts.hour
    reasons: set[PruneReason] = set()
    unverified = False
    if hour.end_wall_ns + retention_hours * NS_PER_HOUR >= now_wall_ns:
        reasons.add("inside_retention_window")
    entry = (
        None if manifest is None or manifest.date != hour.date else manifest.hour_bake(hour.hour)
    )
    if manifest is None or entry is None:
        reasons.add("not_baked")
    else:
        if entry.bake_version != bake_version:
            reasons.add("bake_version_stale")
        if entry.accounting.failures:
            reasons.add("decode_failures")
        unverified |= _check_segments(facts, manifest, reasons)
        unverified |= _check_parts(facts, manifest, reasons)
    if unverified and not reasons:
        reasons.add("unverified")
    ordered = tuple(reason for reason in PRUNE_REASONS if reason in reasons)
    return PruneDecision(
        hour=hour,
        prunable=not ordered,
        reasons=ordered,
        segments=() if ordered else facts.segments,
    )


def _check_segments(facts: HourFacts, manifest: Manifest, reasons: set[PruneReason]) -> bool:
    """Compare the segments on disk with those baked (ADR 0025, condition 2).

    Returns:
        Whether a hash needed for the comparison was not computed.
    """
    baked = {segment.path: segment for segment in manifest.hour_segments(facts.hour.hour)}
    pruned = {segment.path for segment in manifest.pruned}
    on_disk = {segment.path for segment in facts.segments}
    unverified = False
    for segment in facts.segments:
        expected = baked.get(segment.path)
        if expected is None:
            reasons.add("segment_added")
        elif segment.bytes != expected.bytes:
            reasons.add("segment_changed")
        elif segment.sha256 is None:
            unverified = True
        elif segment.sha256 != expected.sha256:
            reasons.add("segment_changed")
    if any(path not in on_disk and path not in pruned for path in baked):
        reasons.add("segment_missing")
    return unverified


def _check_parts(facts: HourFacts, manifest: Manifest, reasons: set[PruneReason]) -> bool:
    """Compare the part files on disk with those the bake wrote (ADR 0025, condition 4).

    Returns:
        Whether a hash needed for the comparison was not computed.
    """
    found = {part.path: part for part in facts.parts}
    unverified = False
    for expected in manifest.hour_parts(facts.hour.hour):
        part = found.get(expected.path)
        if part is None or not part.exists:
            reasons.add("baked_file_missing")
        elif part.sha256 is None:
            unverified = True
        elif part.sha256 != expected.sha256:
            reasons.add("baked_file_changed")
    return unverified


def survey(
    layout: DataLayout, *, now_wall_ns: int, retention_hours: int, bake_version: int
) -> tuple[PruneDecision, ...]:
    """Decide every hour that still has raw segments on disk.

    Facts are gathered without hashes first; files are hashed only for an hour whose one remaining
    reason is that they were not.

    Args:
        layout: The archive.
        now_wall_ns: The current wall-clock time.
        retention_hours: How long after it ends an hour's raw segments are kept.
        bake_version: The current bake version.

    Returns:
        One decision per hour, in time order.

    Raises:
        TapeCorruptionError: If a manifest does not decode.
        OSError: If a file cannot be inspected.
    """
    manifests: dict[dt.date, Manifest | None] = {}
    decisions: list[PruneDecision] = []
    for hour in layout.raw_hours():
        files = layout.segment_files(hour)
        if not files:
            continue
        if hour.date not in manifests:
            manifests[hour.date] = read_manifest(layout.manifest_path(hour.date))
        manifest = manifests[hour.date]
        facts = HourFacts(
            hour=hour,
            segments=tuple(
                SegmentOnDisk(path=layout.segment_name(path), bytes=path.stat().st_size)
                for path in files
            ),
            parts=_parts_on_disk(layout, manifest, hour, hashed=False),
        )
        options = {
            "now_wall_ns": now_wall_ns,
            "retention_hours": retention_hours,
            "bake_version": bake_version,
        }
        decision = decide(facts, manifest, **options)
        if decision.reasons == ("unverified",):
            hashed = HourFacts(
                hour=hour,
                segments=tuple(
                    msgspec.structs.replace(
                        segment, sha256=sha256_file(layout.segment_path(segment.path))
                    )
                    for segment in facts.segments
                ),
                parts=_parts_on_disk(layout, manifest, hour, hashed=True),
            )
            decision = decide(hashed, manifest, **options)
        decisions.append(decision)
    return tuple(decisions)


def _parts_on_disk(
    layout: DataLayout, manifest: Manifest | None, hour: HourKey, *, hashed: bool
) -> tuple[PartOnDisk, ...]:
    if manifest is None or manifest.date != hour.date:
        return ()
    found: list[PartOnDisk] = []
    for part in manifest.hour_parts(hour.hour):
        path = layout.part_path(part.path)
        exists = path.is_file()
        digest = sha256_file(path) if exists and hashed else None
        found.append(PartOnDisk(path=part.path, exists=exists, sha256=digest))
    return tuple(found)


def apply(
    layout: DataLayout, decisions: Iterable[PruneDecision], *, clock: Clock
) -> tuple[PrunedSegment, ...]:
    """Delete the segments of every prunable decision, recording each deletion first.

    For each day, the manifest gains every deletion and is synced to disk before any file of that
    day is unlinked. A segment recorded as pruned by an earlier run that stopped before deleting it
    is deleted now, its first record kept.

    Args:
        layout: The archive.
        decisions: Decisions from :func:`survey` in the same :func:`tape.bake.bake.archive_lock`;
            those not prunable are skipped.
        clock: Stamps each deletion.

    Returns:
        The segments deleted.

    Raises:
        ArchiveError: If a prunable decision's day has no manifest, or a segment lacks its hash.
        TapeCorruptionError: If a manifest does not decode.
        OSError: If the manifest cannot be written or a file cannot be deleted.
    """
    by_day: dict[dt.date, list[PruneDecision]] = {}
    for decision in decisions:
        if decision.prunable and decision.segments:
            by_day.setdefault(decision.hour.date, []).append(decision)
    deleted: list[PrunedSegment] = []
    for date, day_decisions in sorted(by_day.items()):
        deleted.extend(_prune_day(layout, date, day_decisions, clock=clock))
    return tuple(deleted)


def _prune_day(
    layout: DataLayout, date: dt.date, decisions: Sequence[PruneDecision], *, clock: Clock
) -> list[PrunedSegment]:
    path = layout.manifest_path(date)
    manifest = read_manifest(path)
    if manifest is None:
        raise ArchiveError(f"no manifest for {date.isoformat()}; nothing there can be pruned")
    pruned_wall_ns = int(clock.wall_ns())
    records: list[PrunedSegment] = []
    for decision in decisions:
        for segment in decision.segments:
            if segment.sha256 is None:
                raise ArchiveError(f"{segment.path} was not verified and cannot be pruned")
            records.append(
                PrunedSegment(
                    path=segment.path,
                    bytes=segment.bytes,
                    sha256=segment.sha256,
                    pruned_wall_ns=pruned_wall_ns,
                )
            )
    try:
        updated = with_pruned(manifest, records)
    except ValueError as exc:
        raise ArchiveError(f"{date.isoformat()}: {exc}") from exc
    write_manifest(path, updated)
    for record in records:
        layout.segment_path(record.path).unlink(missing_ok=True)
        _log.info("segment pruned", extra={"path": record.path, "bytes": record.bytes})
    for decision in decisions:
        _remove_if_empty(layout, decision.hour)
    return records


def _remove_if_empty(layout: DataLayout, hour: HourKey) -> None:
    """Remove the hour's raw directory, and its day's, when pruning left them empty."""
    hour_dir = layout.hour_dir(hour)
    sync_dir(hour_dir)
    for directory in (hour_dir, hour_dir.parent):
        try:
            directory.rmdir()
        except OSError:
            return
