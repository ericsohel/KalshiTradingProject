"""The daily manifest: what one UTC day captured and baked, what was pruned, and how well it went.

Responsibility: define the manifest of docs/DATA_FORMATS.md 7 as strict structs; merge one
hour's bake, or a batch of pruned segments, into a day's manifest; derive the day's integrity
numbers from the per-hour facts it holds; and read and write the file.

Invariants: a manifest decodes only if it has exactly the documented fields, version 1, and
consistent contents: every hour baked at most once, every segment and part file belonging to a
baked hour of that day, each table's rows equal to the sum over its files, each hour's records
all accounted for, and every pruned segment listed among the segments with the same size and
hash. Every ratio is an exact integer pair, and ``(0, 0)`` means no data. The integrity numbers
are recomputed from the hours on every change, never edited in place. A pruned segment stays
listed among the segments, so the record of what was captured outlives the file (ADR 0025). A
write replaces the file whole, so a reader never sees half a manifest.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable, Mapping, Sequence
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Final, Literal

import msgspec

from tape.bake.files import write_atomic
from tape.bake.layout import HourKey
from tape.bake.tables import TABLE_NAMES, TableName
from tape.errors import TapeCorruptionError
from tape.timeutil import NS_PER_S

__all__ = [
    "MANIFEST_VERSION",
    "RECORD_KINDS",
    "SECONDS_PER_DAY",
    "AuditFacts",
    "AuditSummary",
    "ClockSection",
    "ConnectionSpan",
    "GapFacts",
    "GapSummary",
    "HourBake",
    "HourIntegrity",
    "Manifest",
    "PartEntry",
    "PrunedSegment",
    "RecordAccounting",
    "SegmentEntry",
    "Sleep",
    "TableEntry",
    "Uptime",
    "decode_manifest",
    "encode_manifest",
    "new_manifest",
    "parse_chrony_tracking",
    "read_manifest",
    "segment_hour",
    "summarize_audits",
    "summarize_gaps",
    "summarize_uptime",
    "with_hour",
    "with_pruned",
    "write_manifest",
]

MANIFEST_VERSION: Final = 1

SECONDS_PER_DAY: Final = 86_400

RECORD_KINDS: Final = ("frame", "command", "gap", "connection", "audit")
"""Keys of :attr:`RecordAccounting.records`: the record kinds of docs/DATA_FORMATS.md 4."""

_CHRONY_SYSTEM_TIME_FIELD: Final = 4
"""Index of "System time", the clock's current offset in seconds, in ``chronyc -c tracking``."""

_MS_EXPONENT: Final = 3
_SEGMENT_NAME: Final = re.compile(r"(\d{4}-\d{2}-\d{2})/(\d{2})/[A-Za-z0-9._-]+\.tape\.zst")

NonNegative = Annotated[int, msgspec.Meta(ge=0)]
HourOfDay = Annotated[int, msgspec.Meta(ge=0, le=23)]
BakeVersion = Annotated[int, msgspec.Meta(ge=1)]
Sha256 = Annotated[str, msgspec.Meta(pattern=r"^[0-9a-f]{64}$")]
SegmentName = Annotated[
    str, msgspec.Meta(pattern=r"^\d{4}-\d{2}-\d{2}/\d{2}/[A-Za-z0-9._-]+\.tape\.zst$")
]
PartName = Annotated[
    str, msgspec.Meta(pattern=r"^[a-z]+/dt=\d{4}-\d{2}-\d{2}/hour=\d{2}/part-\d+\.parquet$")
]
Counts = dict[str, NonNegative]
Ratio = tuple[NonNegative, NonNegative]


class RecordAccounting(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """How every data record of one hour was accounted for (ADR 0025, condition 3).

    Attributes:
        records: Data records read, by kind: ``frame``, ``command``, ``gap``, ``connection``,
            ``audit``.
        baked: Records that became table rows, by source: a frame's message type, ``gap``, or
            ``audit``.
        not_baked: Records intentionally not baked, by reason: ``command``, ``connection``, a
            reply's message type (``subscribed``, ``ok``, ``unsubscribed``, ``error``), or
            ``ticker``.
        decode_failures: Records that did not decode, by class (docs/DATA_FORMATS.md 7).
        corrupt_segments: Segments that could not be read to their end for a reason other than
            a truncated final record; records after the damage are in no count.

    Raises:
        ValueError: If a kind is unknown, or the records read differ from those accounted for.
    """

    records: Counts
    baked: Counts
    not_baked: Counts
    decode_failures: Counts
    corrupt_segments: NonNegative = 0

    def __post_init__(self) -> None:
        unknown = set(self.records) - set(RECORD_KINDS)
        if unknown:
            raise ValueError(f"unknown record kinds {sorted(unknown)}")
        accounted = (
            sum(self.baked.values())
            + sum(self.not_baked.values())
            + sum(self.decode_failures.values())
        )
        if self.total != accounted:
            raise ValueError(f"{self.total} records read but {accounted} accounted for")

    @property
    def total(self) -> int:
        """Data records read."""
        return sum(self.records.values())

    @property
    def failures(self) -> int:
        """Decode failures plus corrupt segments; pruning requires zero (ADR 0025)."""
        return sum(self.decode_failures.values()) + self.corrupt_segments


class ConnectionSpan(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """A stretch during which one connection was open, as one segment shows it.

    Attributes:
        conn_id: The connection.
        start_wall_ns: The ``open`` record, or the start of the hour when the segment continues
            a connection that was already open.
        end_wall_ns: The ``close`` record, or the segment's last record when none was written.
        opened: Whether the span starts at an ``open`` record.
        closed: Whether the span ends at a ``close`` record.

    Raises:
        ValueError: If the span ends before it starts.
    """

    conn_id: NonNegative
    start_wall_ns: int
    end_wall_ns: int
    opened: bool
    closed: bool

    def __post_init__(self) -> None:
        if self.end_wall_ns < self.start_wall_ns:
            raise ValueError(f"span ends at {self.end_wall_ns} before it starts")


class Sleep(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """A stretch the host slept, from a ``clock_jump`` record (docs/INTERFACES.md 8.6).

    Attributes:
        start_wall_ns: When the wall clock left the monotonic clock behind.
        end_wall_ns: When the jump was noticed.

    Raises:
        ValueError: If the stretch ends before it starts.
    """

    start_wall_ns: int
    end_wall_ns: int

    def __post_init__(self) -> None:
        if self.end_wall_ns < self.start_wall_ns:
            raise ValueError(f"sleep ends at {self.end_wall_ns} before it starts")


class GapFacts(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One hour's sequence gaps and the book time they cost.

    Attributes:
        count: ``GAP`` records.
        stale_market_ns: Summed over markets, the time from a gap on a market's book
            subscription until the market's next snapshot, or the end of its segment.
        observed_market_ns: Summed over markets, the time from a market's first book message in
            a segment until the segment's last record, or until the market left the subscription.
    """

    count: NonNegative
    stale_market_ns: NonNegative
    observed_market_ns: NonNegative


class AuditFacts(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One hour's audit records by outcome (ADR 0021).

    Attributes:
        exact: Equal to the local book as of the reply.
        consistent: Equal to another state in the request window.
        inconsistent: Equal to no state in the window.
        undecidable: The window could not vouch for every state it spans.
        levels_mismatched: ``mismatched_levels`` summed over decidable audits.
    """

    exact: NonNegative
    consistent: NonNegative
    inconsistent: NonNegative
    undecidable: NonNegative
    levels_mismatched: NonNegative


class HourIntegrity(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """What one hour contributes to the day's integrity numbers.

    Attributes:
        spans: Open stretches of every taped connection, per segment.
        sleeps: Host sleeps noticed in the hour.
        gaps: Sequence gaps and their cost.
        audits: Audits by outcome.
        writer_overflow_dropped: Records the recorder refused because its writer queue was full.
    """

    spans: tuple[ConnectionSpan, ...]
    sleeps: tuple[Sleep, ...]
    gaps: GapFacts
    audits: AuditFacts
    writer_overflow_dropped: NonNegative


class HourBake(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One completed bake of one hour.

    Attributes:
        hour: UTC hour of the manifest's day.
        bake_version: The baker's output version; a bake by an older version is stale.
        software_version: The package version that baked it.
        accounting: How every record was accounted for.
        integrity: The hour's integrity facts.
    """

    hour: HourOfDay
    bake_version: BakeVersion
    software_version: str
    accounting: RecordAccounting
    integrity: HourIntegrity


class SegmentEntry(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One raw segment as a bake read it.

    Attributes:
        path: Name relative to ``raw/``, ``YYYY-MM-DD/HH/conn-NN-UUUU.tape.zst``.
        hour: The hour directory it lives in.
        bytes: File size.
        sha256: Hash of the file's content.
        records: Data records read from it.
        first_recv_wall_ns: Wall time of its first record; ``None`` when it holds none.
        last_recv_wall_ns: Wall time of its last record; ``None`` when it holds none.
        truncated: Whether it ended inside a record or an unfinished compressed frame.
    """

    path: SegmentName
    hour: HourOfDay
    bytes: NonNegative
    sha256: Sha256
    records: NonNegative
    first_recv_wall_ns: int | None
    last_recv_wall_ns: int | None
    truncated: bool


class PartEntry(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One baked part file.

    Attributes:
        path: Name relative to ``baked/``, ``<table>/dt=YYYY-MM-DD/hour=HH/part-<n>.parquet``.
        hour: The hour it was baked from.
        rows: Rows in the file.
        bytes: File size.
        sha256: Hash of the file's content (ADR 0025, condition 4).
    """

    path: PartName
    hour: HourOfDay
    rows: NonNegative
    bytes: NonNegative
    sha256: Sha256


class TableEntry(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One table's part files for the day.

    Attributes:
        rows: Rows across every file.
        files: The part files, in path order.

    Raises:
        ValueError: If ``rows`` is not the sum of the files' rows.
    """

    rows: NonNegative
    files: tuple[PartEntry, ...]

    def __post_init__(self) -> None:
        if self.rows != sum(part.rows for part in self.files):
            raise ValueError(f"table rows {self.rows} differ from the sum over its files")


class BakeSection(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Every baked hour of the day.

    Attributes:
        hours: One entry per baked hour, in hour order.

    Raises:
        ValueError: If an hour appears twice or out of order.
    """

    hours: tuple[HourBake, ...]

    def __post_init__(self) -> None:
        numbers = [entry.hour for entry in self.hours]
        if numbers != sorted(set(numbers)):
            raise ValueError(f"baked hours must be unique and in order, got {numbers}")


class Uptime(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Seconds of the day with a taped connection open, excluding host sleep.

    Attributes:
        seconds_recording: Whole seconds recorded.
        seconds_in_day: 86,400.
        ratio: ``(seconds_recording, seconds_in_day)``.
    """

    seconds_recording: NonNegative
    seconds_in_day: NonNegative
    ratio: Ratio


class GapSummary(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The day's sequence gaps.

    Attributes:
        count: Gap records.
        market_seconds_affected: Whole market-seconds a book was stale because of a gap.
        market_seconds_observed: Whole market-seconds books were observed.
        share: ``(market_seconds_affected, market_seconds_observed)``.
    """

    count: NonNegative
    market_seconds_affected: NonNegative
    market_seconds_observed: NonNegative
    share: Ratio


class AuditSummary(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The day's audits (ADR 0021).

    Attributes:
        books_sampled: Decidable audits: exact, consistent, or inconsistent.
        books_exact: Exact audits.
        books_consistent: Consistent audits.
        books_inconsistent: Inconsistent audits, each a finding to investigate.
        books_undecidable: Undecidable audits, in neither ratio.
        levels_mismatched: Mismatched levels summed over decidable audits.
        exact_ratio: ``(books_exact, books_sampled)``.
        consistency_ratio: ``(books_exact + books_consistent, books_sampled)``, the number
            published (ADR 0021).
    """

    books_sampled: NonNegative
    books_exact: NonNegative
    books_consistent: NonNegative
    books_inconsistent: NonNegative
    books_undecidable: NonNegative
    levels_mismatched: NonNegative
    exact_ratio: Ratio
    consistency_ratio: Ratio


class ClockSection(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The host clock as the latest bake found it.

    Attributes:
        chrony_offset_ms: The system clock's offset that ``chronyc`` reported, rounded to whole
            milliseconds; ``None`` when chrony was unavailable.
    """

    chrony_offset_ms: int | None


class PrunedSegment(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """A raw segment deleted by ``tape prune`` (ADR 0025).

    Attributes:
        path: Name relative to ``raw/``.
        bytes: Size of the deleted file.
        sha256: Hash of the deleted file.
        pruned_wall_ns: When the deletion was recorded, just before the file was deleted.
    """

    path: SegmentName
    bytes: NonNegative
    sha256: Sha256
    pruned_wall_ns: int


class Manifest(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """One UTC day of the archive (docs/DATA_FORMATS.md 7).

    Attributes:
        version: Manifest format version, 1.
        date: The UTC day.
        software_version: The package version that wrote the manifest last.
        segments: Every raw segment baked for the day, pruned or not, in path order.
        tables: Every table's part files.
        bake: Every baked hour with its record accounting and integrity facts.
        uptime: Recording time over the day.
        gaps: Sequence gaps over the day.
        audits: Audits over the day.
        clock: The host clock at the latest bake.
        pruned: Every pruned segment, in path order.

    Raises:
        ValueError: If the contents are inconsistent; see the module docstring.
    """

    version: Literal[1]
    date: dt.date
    software_version: str
    segments: tuple[SegmentEntry, ...]
    tables: dict[TableName, TableEntry]
    bake: BakeSection
    uptime: Uptime
    gaps: GapSummary
    audits: AuditSummary
    clock: ClockSection
    pruned: tuple[PrunedSegment, ...]

    def __post_init__(self) -> None:
        if set(self.tables) != set(TABLE_NAMES):
            raise ValueError(f"tables must be exactly {list(TABLE_NAMES)}")
        hours = {entry.hour for entry in self.bake.hours}
        _check_sorted_unique("segments", [segment.path for segment in self.segments])
        for segment in self.segments:
            if segment_hour(segment.path) != (self.date, segment.hour):
                raise ValueError(f"segment {segment.path} is not in hour {segment.hour} of the day")
            if segment.hour not in hours:
                raise ValueError(
                    f"segment {segment.path} belongs to hour {segment.hour}, not baked"
                )
        for name in TABLE_NAMES:
            files = self.tables[name].files
            _check_sorted_unique(name, [part.path for part in files])
            for part in files:
                prefix = f"{name}/dt={self.date.isoformat()}/hour={part.hour:02d}/"
                if not part.path.startswith(prefix):
                    raise ValueError(f"part {part.path} is not under {prefix}")
                if part.hour not in hours:
                    raise ValueError(f"part {part.path} belongs to hour {part.hour}, not baked")
        _check_sorted_unique("pruned", [segment.path for segment in self.pruned])
        listed = {segment.path: segment for segment in self.segments}
        for pruned in self.pruned:
            entry = listed.get(pruned.path)
            if entry is None or (entry.bytes, entry.sha256) != (pruned.bytes, pruned.sha256):
                raise ValueError(f"pruned segment {pruned.path} does not match a baked segment")

    def hour_bake(self, hour: int) -> HourBake | None:
        """The bake of one hour of the day, or ``None`` when it was never baked."""
        return next((entry for entry in self.bake.hours if entry.hour == hour), None)

    def hour_segments(self, hour: int) -> tuple[SegmentEntry, ...]:
        """The segments a bake of one hour read."""
        return tuple(segment for segment in self.segments if segment.hour == hour)

    def hour_parts(self, hour: int) -> tuple[PartEntry, ...]:
        """Every part file baked from one hour, across every table."""
        return tuple(
            part for name in TABLE_NAMES for part in self.tables[name].files if part.hour == hour
        )


def _check_sorted_unique(what: str, paths: Sequence[str]) -> None:
    if list(paths) != sorted(set(paths)):
        raise ValueError(f"{what} paths must be unique and in order")


def segment_hour(name: str) -> tuple[dt.date, int]:
    """The day and hour a segment name places the segment in.

    Args:
        name: ``YYYY-MM-DD/HH/<file>.tape.zst``.

    Returns:
        ``(date, hour)``.

    Raises:
        ValueError: If the name has another shape or names no real date.
    """
    match = _SEGMENT_NAME.fullmatch(name)
    if match is None:
        raise ValueError(f"not a segment name: {name!r}")
    return dt.date.fromisoformat(match.group(1)), int(match.group(2))


def new_manifest(date: dt.date, *, software_version: str) -> Manifest:
    """A manifest for a day with nothing baked.

    Args:
        date: The UTC day.
        software_version: The package version writing it.

    Returns:
        A manifest with no segments, files, hours, or pruned segments.
    """
    return _assemble(
        date,
        software_version=software_version,
        segments=(),
        tables=dict.fromkeys(TABLE_NAMES, ()),
        hours=(),
        clock=ClockSection(chrony_offset_ms=None),
        pruned=(),
    )


def with_hour(
    manifest: Manifest,
    *,
    bake: HourBake,
    segments: Sequence[SegmentEntry],
    parts: Mapping[TableName, Sequence[PartEntry]],
    software_version: str,
    clock_offset_ms: int | None,
) -> Manifest:
    """Replace everything a manifest says about one hour with a new bake of that hour.

    Args:
        manifest: The day's manifest.
        bake: The new bake.
        segments: Every segment the bake read.
        parts: Every part file the bake wrote, by table.
        software_version: The package version writing the manifest.
        clock_offset_ms: The host clock's offset now, or ``None`` when unknown.

    Returns:
        The new manifest, with the day's integrity numbers recomputed.

    Raises:
        ValueError: If a segment of the hour was already pruned, because a bake of a partly
            pruned hour would replace complete tables with incomplete ones, or if an entry does
            not belong to the hour.
    """
    hour = bake.hour
    if any(segment_hour(pruned.path)[1] == hour for pruned in manifest.pruned):
        raise ValueError(f"hour {hour:02d} has pruned segments and cannot be baked again")
    if any(segment.hour != hour for segment in segments) or any(
        part.hour != hour for files in parts.values() for part in files
    ):
        raise ValueError(f"every segment and part file must belong to hour {hour:02d}")
    kept_segments = [segment for segment in manifest.segments if segment.hour != hour]
    tables = {
        name: (
            *(part for part in manifest.tables[name].files if part.hour != hour),
            *parts.get(name, ()),
        )
        for name in TABLE_NAMES
    }
    hours = [entry for entry in manifest.bake.hours if entry.hour != hour]
    return _assemble(
        manifest.date,
        software_version=software_version,
        segments=(*kept_segments, *segments),
        tables=tables,
        hours=(*hours, bake),
        clock=ClockSection(chrony_offset_ms=clock_offset_ms),
        pruned=manifest.pruned,
    )


def with_pruned(manifest: Manifest, pruned: Iterable[PrunedSegment]) -> Manifest:
    """Record segments as pruned; a segment recorded already keeps its first record.

    Args:
        manifest: The day's manifest.
        pruned: Segments about to be deleted.

    Returns:
        The new manifest.

    Raises:
        ValueError: If a segment is not among the manifest's segments with the same size and hash.
    """
    recorded = {entry.path: entry for entry in manifest.pruned}
    for entry in pruned:
        recorded.setdefault(entry.path, entry)
    return _assemble(
        manifest.date,
        software_version=manifest.software_version,
        segments=manifest.segments,
        tables={name: manifest.tables[name].files for name in TABLE_NAMES},
        hours=manifest.bake.hours,
        clock=manifest.clock,
        pruned=tuple(recorded.values()),
    )


def _assemble(
    date: dt.date,
    *,
    software_version: str,
    segments: Iterable[SegmentEntry],
    tables: Mapping[TableName, Iterable[PartEntry]],
    hours: Iterable[HourBake],
    clock: ClockSection,
    pruned: Iterable[PrunedSegment],
) -> Manifest:
    """Build a manifest in canonical order with its integrity numbers derived from ``hours``."""
    ordered_hours = tuple(sorted(hours, key=lambda entry: entry.hour))
    table_entries: dict[TableName, TableEntry] = {}
    for name in TABLE_NAMES:
        files = tuple(sorted(tables[name], key=lambda part: part.path))
        table_entries[name] = TableEntry(rows=sum(part.rows for part in files), files=files)
    return Manifest(
        version=MANIFEST_VERSION,
        date=date,
        software_version=software_version,
        segments=tuple(sorted(segments, key=lambda segment: segment.path)),
        tables=table_entries,
        bake=BakeSection(hours=ordered_hours),
        uptime=summarize_uptime(date, ordered_hours),
        gaps=summarize_gaps(ordered_hours),
        audits=summarize_audits(ordered_hours),
        clock=clock,
        pruned=tuple(sorted(pruned, key=lambda entry: entry.path)),
    )


def summarize_uptime(date: dt.date, hours: Sequence[HourBake]) -> Uptime:
    """Seconds of a day with at least one taped connection open, less the time the host slept.

    A span left open at the end of its segment is joined to the span of the same connection that
    continues it in a later segment without an ``open`` record, since no ``close`` came between.

    Args:
        date: The UTC day.
        hours: The day's baked hours.

    Returns:
        The day's uptime, clipped to the day.
    """
    day_start = HourKey(date, 0).start_wall_ns
    day_end = day_start + SECONDS_PER_DAY * NS_PER_S
    covered = _clip(_union(_joined_spans(hours)), day_start, day_end)
    asleep = _union(
        (sleep.start_wall_ns, sleep.end_wall_ns) for h in hours for sleep in h.integrity.sleeps
    )
    recording_ns = _length(covered) - _overlap(covered, asleep)
    seconds = recording_ns // NS_PER_S
    return Uptime(
        seconds_recording=seconds,
        seconds_in_day=SECONDS_PER_DAY,
        ratio=(seconds, SECONDS_PER_DAY),
    )


def summarize_gaps(hours: Sequence[HourBake]) -> GapSummary:
    """The day's gaps and the share of observed market time spent stale because of them.

    Args:
        hours: The day's baked hours.

    Returns:
        Counts in whole market-seconds, and their exact ratio.
    """
    count = sum(entry.integrity.gaps.count for entry in hours)
    affected = sum(entry.integrity.gaps.stale_market_ns for entry in hours) // NS_PER_S
    observed = sum(entry.integrity.gaps.observed_market_ns for entry in hours) // NS_PER_S
    return GapSummary(
        count=count,
        market_seconds_affected=affected,
        market_seconds_observed=observed,
        share=(affected, observed),
    )


def summarize_audits(hours: Sequence[HourBake]) -> AuditSummary:
    """The day's audits and their exact ratios (ADR 0021).

    Args:
        hours: The day's baked hours.

    Returns:
        Counts by outcome; undecidable audits are in neither ratio.
    """
    exact = sum(entry.integrity.audits.exact for entry in hours)
    consistent = sum(entry.integrity.audits.consistent for entry in hours)
    inconsistent = sum(entry.integrity.audits.inconsistent for entry in hours)
    sampled = exact + consistent + inconsistent
    return AuditSummary(
        books_sampled=sampled,
        books_exact=exact,
        books_consistent=consistent,
        books_inconsistent=inconsistent,
        books_undecidable=sum(entry.integrity.audits.undecidable for entry in hours),
        levels_mismatched=sum(entry.integrity.audits.levels_mismatched for entry in hours),
        exact_ratio=(exact, sampled),
        consistency_ratio=(exact + consistent, sampled),
    )


def _joined_spans(hours: Sequence[HourBake]) -> list[tuple[int, int]]:
    by_conn: dict[int, list[ConnectionSpan]] = {}
    for entry in hours:
        for span in entry.integrity.spans:
            by_conn.setdefault(span.conn_id, []).append(span)
    joined: list[tuple[int, int]] = []
    for spans in by_conn.values():
        start: int | None = None
        end = 0
        closed = True
        for span in sorted(spans, key=lambda s: (s.start_wall_ns, s.end_wall_ns)):
            if start is not None and not closed and not span.opened:
                end = max(end, span.end_wall_ns)
                closed = span.closed
                continue
            if start is not None:
                joined.append((start, end))
            start, end, closed = span.start_wall_ns, span.end_wall_ns, span.closed
        if start is not None:
            joined.append((start, end))
    return joined


def _union(intervals: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def _clip(intervals: Iterable[tuple[int, int]], low: int, high: int) -> list[tuple[int, int]]:
    return [
        (max(start, low), min(end, high)) for start, end in intervals if end > low and start < high
    ]


def _length(intervals: Iterable[tuple[int, int]]) -> int:
    return sum(end - start for start, end in intervals)


def _overlap(a: Sequence[tuple[int, int]], b: Sequence[tuple[int, int]]) -> int:
    """Total length covered by both of two sorted, disjoint interval lists."""
    total = 0
    i = j = 0
    while i < len(a) and j < len(b):
        low, high = max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
        if high > low:
            total += high - low
        if a[i][1] < b[j][1]:
            i += 1
        else:
            j += 1
    return total


_encoder: Final = msgspec.json.Encoder(order="deterministic")
_decoder: Final = msgspec.json.Decoder(Manifest)


def encode_manifest(manifest: Manifest) -> bytes:
    """The manifest as indented JSON with a trailing newline; equal manifests give equal bytes."""
    return msgspec.json.format(_encoder.encode(manifest), indent=2) + b"\n"


def decode_manifest(data: bytes, *, source: str = "manifest") -> Manifest:
    """Decode a manifest strictly.

    Args:
        data: The JSON document.
        source: Where it came from, for the error message.

    Returns:
        The manifest.

    Raises:
        TapeCorruptionError: If the document is not valid JSON, has an unknown or missing field, a
            wrong type or version, or inconsistent contents.
    """
    try:
        return _decoder.decode(data)
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise TapeCorruptionError(f"{source}: {exc}") from exc


def read_manifest(path: Path) -> Manifest | None:
    """Read a day's manifest.

    Args:
        path: The manifest file.

    Returns:
        The manifest, or ``None`` when the file does not exist, meaning nothing was baked that day.

    Raises:
        TapeCorruptionError: If the file does not decode.
        OSError: If the file exists but cannot be read.
    """
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return None
    return decode_manifest(data, source=str(path))


def write_manifest(path: Path, manifest: Manifest) -> None:
    """Replace a day's manifest atomically.

    Raises:
        OSError: If the file cannot be written.
    """
    write_atomic(path, encode_manifest(manifest))


def parse_chrony_tracking(text: str) -> int | None:
    """The clock offset in the output of ``chronyc -c tracking``, in whole milliseconds.

    Args:
        text: The command's standard output: one comma-separated line.

    Returns:
        Its "System time" field, seconds by which the system clock differs from true time,
        rounded half to even to milliseconds; ``None`` when the text has no such decimal field.
    """
    fields = text.strip().split(",")
    if len(fields) <= _CHRONY_SYSTEM_TIME_FIELD:
        return None
    try:
        seconds = Decimal(fields[_CHRONY_SYSTEM_TIME_FIELD])
    except InvalidOperation:
        return None
    if not seconds.is_finite():
        return None
    milliseconds = seconds.scaleb(_MS_EXPONENT).to_integral_value(rounding=ROUND_HALF_EVEN)
    return int(milliseconds)
