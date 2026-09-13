"""Bake closed hours of raw segments into the version-1 tables and record each bake in its manifest.

Responsibility: for one closed hour (ADR 0025), hash every segment, interpret every record into
rows (``tape.bake.interpret``), write the tables in bounded memory (``tape.bake.spill``), put the
new part files in place of the hour's old ones, and merge the bake into the day's manifest
(``tape.bake.manifest``). Also decides which hours need a bake, and holds the lock that lets one
bake or prune run at a time.

Invariants: an hour with a pruned segment is never baked again, since its tables would lose rows; a
segment whose size or modification time changes while it is read fails the bake before anything is
replaced; an hour's part files are swapped in only once all are written, and the manifest is
written after them, so a crash in between leaves files the manifest does not describe, which prune
refuses and the next bake repairs; and baking the same segments with the same bake version, part
size, and pyarrow version produces byte-identical part files.
"""

from __future__ import annotations

import contextlib
import fcntl
import logging
import shutil
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Final

import msgspec

from tape.bake.files import replace_dir, sha256_file
from tape.bake.interpret import DecodeFailure, HourInterpreter, SegmentFacts
from tape.bake.layout import DataLayout, HourKey, closed_hours
from tape.bake.manifest import (
    HourBake,
    Manifest,
    PartEntry,
    SegmentEntry,
    new_manifest,
    read_manifest,
    segment_hour,
    with_hour,
    write_manifest,
)
from tape.bake.spill import DEFAULT_FLUSH_ROWS, HourTables
from tape.bake.tables import TABLE_NAMES, TableName
from tape.errors import ArchiveError, TapeCorruptionError
from tape.segment import SegmentReader

__all__ = [
    "BAKE_VERSION",
    "DEFAULT_MAX_PART_ROWS",
    "BakeReport",
    "archive_lock",
    "bake_hour",
    "bake_needed",
    "hours_to_bake",
    "record_bake",
]

BAKE_VERSION: Final = 1
"""The baker's output version. Raise it whenever the same segments would bake to different rows;
every hour baked by an older version is then baked again before it can be pruned."""

DEFAULT_MAX_PART_ROWS: Final = 500_000
"""Most rows in one part file, and so in memory while a part is sorted; ``bake.max_part_rows``.
On the busiest recorded hour (docs/OPERATIONS.md 5) writing peaked near 340 MB at this size and
280 MB at half of it, at the same speed; reading the hour's records needs about 160 MB."""

_log = logging.getLogger(__name__)


class BakeReport(msgspec.Struct, frozen=True, kw_only=True):
    """One completed bake of one hour, before it is recorded in the manifest.

    Attributes:
        hour: The hour baked.
        bake: The hour's entry for the manifest's bake section.
        segments: Every segment read.
        parts: Every part file written, by table.
        failures: Samples of the decode failures found, for investigation.
    """

    hour: HourKey
    bake: HourBake
    segments: tuple[SegmentEntry, ...]
    parts: dict[TableName, tuple[PartEntry, ...]]
    failures: tuple[DecodeFailure, ...]

    @property
    def rows(self) -> dict[TableName, int]:
        """Rows written per table."""
        return {name: sum(part.rows for part in self.parts[name]) for name in TABLE_NAMES}


@contextlib.contextmanager
def archive_lock(layout: DataLayout) -> Iterator[None]:
    """Hold the archive's lock, so one bake or prune runs at a time.

    Args:
        layout: The archive; the lock file lives beside its manifests.

    Yields:
        Nothing; the lock is held until the block exits.

    Raises:
        ArchiveError: If another process holds the lock.
    """
    layout.manifests.mkdir(parents=True, exist_ok=True)
    with layout.lock_path.open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ArchiveError(f"another tape bake or tape prune holds {layout.lock_path}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def bake_hour(
    layout: DataLayout,
    hour: HourKey,
    *,
    software_version: str,
    max_part_rows: int = DEFAULT_MAX_PART_ROWS,
    flush_rows: int = DEFAULT_FLUSH_ROWS,
) -> BakeReport:
    """Bake every segment of one hour into its tables, replacing any earlier bake's files.

    The caller checks that the hour is closed and holds :func:`archive_lock`; the manifest is not
    written here but by :func:`record_bake`.

    Args:
        layout: The archive.
        hour: The hour to bake.
        software_version: The package version, recorded with the bake.
        max_part_rows: Rows a part holds before the next spill bucket starts another.
        flush_rows: Rows buffered in memory before they are spilled.

    Returns:
        What was read and written.

    Raises:
        ArchiveError: If a segment of the hour was already pruned, or a segment changed while it
            was read.
        TapeCorruptionError: If the day's manifest does not decode.
        OSError: If a file cannot be read or written.
    """
    manifest = read_manifest(layout.manifest_path(hour.date))
    if manifest is not None and _has_pruned(manifest, hour):
        raise ArchiveError(f"{hour.label} has pruned segments; baking it again would lose rows")
    staging = layout.staging_dir(hour)
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    tables = HourTables(staging, max_part_rows=max_part_rows, flush_rows=flush_rows)
    try:
        interpreter = HourInterpreter(hour, tables)
        segments = tuple(
            _read_segment(layout, path, hour, interpreter) for path in layout.segment_files(hour)
        )
        result = interpreter.finish()
        for name in TABLE_NAMES:
            (staging / name).mkdir()
        written = tables.finish(lambda name: staging / name)
        for name in TABLE_NAMES:
            replace_dir(layout.partition_dir(name, hour), staging / name if written[name] else None)
    finally:
        tables.close()
        shutil.rmtree(staging, ignore_errors=True)
        with contextlib.suppress(OSError):
            staging.parent.rmdir()  # only when no other hour's staging is left in it
    parts = {
        name: tuple(
            PartEntry(
                path=layout.part_name(layout.partition_dir(name, hour) / part.path.name),
                hour=hour.hour,
                rows=part.rows,
                bytes=part.bytes,
                sha256=part.sha256,
            )
            for part in written[name]
        )
        for name in TABLE_NAMES
    }
    for failure in result.failures:
        _log.warning("record not baked", extra=msgspec.to_builtins(failure))
    return BakeReport(
        hour=hour,
        bake=HourBake(
            hour=hour.hour,
            bake_version=BAKE_VERSION,
            software_version=software_version,
            accounting=result.accounting,
            integrity=result.integrity,
        ),
        segments=segments,
        parts=parts,
        failures=result.failures,
    )


def record_bake(
    layout: DataLayout,
    report: BakeReport,
    *,
    software_version: str,
    clock_offset_ms: int | None,
) -> Manifest:
    """Merge a bake into its day's manifest and write the manifest atomically.

    Args:
        layout: The archive.
        report: The bake.
        software_version: The package version writing the manifest.
        clock_offset_ms: The host clock's offset now, or ``None`` when unknown.

    Returns:
        The manifest written.

    Raises:
        ArchiveError: If a segment of the hour was pruned since the bake read the manifest.
        TapeCorruptionError: If the existing manifest does not decode.
        OSError: If the manifest cannot be written.
    """
    hour = report.hour
    path = layout.manifest_path(hour.date)
    manifest = read_manifest(path) or new_manifest(hour.date, software_version=software_version)
    try:
        updated = with_hour(
            manifest,
            bake=report.bake,
            segments=report.segments,
            parts=report.parts,
            software_version=software_version,
            clock_offset_ms=clock_offset_ms,
        )
    except ValueError as exc:
        raise ArchiveError(f"{hour.label}: {exc}") from exc
    write_manifest(path, updated)
    return updated


def bake_needed(
    manifest: Manifest | None,
    hour: HourKey,
    *,
    segments_on_disk: Mapping[str, int],
    parts_present: bool,
) -> bool:
    """Whether an hour needs a bake, from what its manifest says and what is on disk.

    An hour with a pruned segment never does. Otherwise it does when it was never baked, was baked
    by an older bake version, its segment names or sizes differ from those baked, or a part file of
    its bake is missing. Content hashes are left to ``tape prune``, which checks them before it
    deletes anything.

    Args:
        manifest: The day's manifest, or ``None`` when there is none.
        hour: The hour.
        segments_on_disk: Size of each segment on disk, by name.
        parts_present: Whether every part file the manifest lists for the hour exists.

    Returns:
        Whether to bake the hour.
    """
    if manifest is None:
        return bool(segments_on_disk)
    if _has_pruned(manifest, hour):
        return False
    entry = manifest.hour_bake(hour.hour)
    if entry is None:
        return bool(segments_on_disk)
    if entry.bake_version != BAKE_VERSION or not parts_present:
        return True
    baked = {segment.path: segment.bytes for segment in manifest.hour_segments(hour.hour)}
    return baked != dict(segments_on_disk)


def hours_to_bake(
    layout: DataLayout,
    *,
    now_wall_ns: int,
    grace_ns: int,
    force: bool = False,
    candidates: Iterable[HourKey] | None = None,
) -> tuple[HourKey, ...]:
    """Every closed hour of raw segments that needs a bake (:func:`bake_needed`), in time order.

    Args:
        layout: The archive.
        now_wall_ns: The current wall-clock time.
        grace_ns: How long after an hour ends its files may still be written.
        force: Include every closed hour with segments on disk and none pruned, needed or not.
        candidates: Consider only these hours; ``None`` considers every raw hour on disk.

    Returns:
        The hours.

    Raises:
        TapeCorruptionError: If a manifest does not decode.
    """
    manifests: dict[object, Manifest | None] = {}
    needed: list[HourKey] = []
    considered = layout.raw_hours() if candidates is None else sorted(set(candidates))
    for hour in closed_hours(considered, now_wall_ns=now_wall_ns, grace_ns=grace_ns):
        if hour.date not in manifests:
            manifests[hour.date] = read_manifest(layout.manifest_path(hour.date))
        manifest = manifests[hour.date]
        on_disk = {
            layout.segment_name(path): path.stat().st_size for path in layout.segment_files(hour)
        }
        if force:
            if on_disk and (manifest is None or not _has_pruned(manifest, hour)):
                needed.append(hour)
            continue
        parts_present = manifest is None or all(
            layout.part_path(part.path).is_file() for part in manifest.hour_parts(hour.hour)
        )
        if bake_needed(manifest, hour, segments_on_disk=on_disk, parts_present=parts_present):
            needed.append(hour)
    return tuple(needed)


def _has_pruned(manifest: Manifest, hour: HourKey) -> bool:
    return any(segment_hour(pruned.path) == (hour.date, hour.hour) for pruned in manifest.pruned)


def _read_segment(
    layout: DataLayout, path: Path, hour: HourKey, interpreter: HourInterpreter
) -> SegmentEntry:
    """Hash one segment, then interpret its records.

    Raises:
        ArchiveError: If the file's size or modification time changed while it was read.
        OSError: If the file cannot be read.
    """
    name = layout.segment_name(path)
    before = path.stat()
    digest = sha256_file(path)
    truncated = False
    try:
        with SegmentReader(path) as reader:
            facts = interpreter.read_segment(name, reader.header, reader.records())
            truncated = reader.truncated
            if reader.damaged:
                # Unlike a truncated tail, damage can hide records after it; the hour must not
                # be pruned until someone has looked.
                interpreter.corrupt_segment(
                    name,
                    detail="decompression failed, or bytes followed the frame, before the end",
                    record_index=facts.records,
                )
    except TapeCorruptionError as exc:
        interpreter.corrupt_segment(name, detail=str(exc))
        facts = SegmentFacts(records=0, first_recv_wall_ns=None, last_recv_wall_ns=None)
    after = path.stat()
    if (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
        raise ArchiveError(f"{name} changed while it was baked; is its hour still being written?")
    return SegmentEntry(
        path=name,
        hour=hour.hour,
        bytes=before.st_size,
        sha256=digest,
        records=facts.records,
        first_recv_wall_ns=facts.first_recv_wall_ns,
        last_recv_wall_ns=facts.last_recv_wall_ns,
        truncated=truncated,
    )
