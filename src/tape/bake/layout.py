"""Where the archive's files live, and which hours of raw segments exist.

Responsibility: name every path the baker, the pruner, and the catalog read or write
(docs/DATA_FORMATS.md 4 to 7), and list the hours of raw segments on disk, so that no other
module builds an archive path by hand. Nothing here writes.

Invariants: an hour is a UTC date and hour; its segments are exactly the ``*.tape.zst`` files
directly inside ``raw/YYYY-MM-DD/HH/``, in name order; a name recorded in a manifest is a POSIX
path relative to the root it belongs to (``raw/`` or ``baked/``), so a manifest stays valid when
the data directory moves; and an hour is closed only once it ended more than the grace period
ago, so no file in it can still be written (docs/INTERFACES.md 8.4).
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Final

import msgspec

from tape.timeutil import NS_PER_S, wall_ns_to_datetime

__all__ = [
    "HOURS_PER_DAY",
    "NS_PER_HOUR",
    "SEGMENT_SUFFIX",
    "DataLayout",
    "HourKey",
    "closed_hours",
]

SEGMENT_SUFFIX: Final = ".tape.zst"
"""Suffix of every raw segment file (docs/DATA_FORMATS.md 4)."""

HOURS_PER_DAY: Final = 24

NS_PER_HOUR: Final = 3_600 * NS_PER_S

_HOUR_LABEL: Final = re.compile(r"(\d{4}-\d{2}-\d{2})T(\d{2})")
_DATE_DIR: Final = re.compile(r"\d{4}-\d{2}-\d{2}")
_HOUR_DIR: Final = re.compile(r"\d{2}")
_EPOCH: Final = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
_ONE_SECOND: Final = dt.timedelta(seconds=1)
_STAGING: Final = ".staging"
_LOCK: Final = ".lock"


class HourKey(msgspec.Struct, frozen=True, order=True):
    """One UTC hour of the archive; raw segments are grouped, baked, and pruned by it.

    Attributes:
        date: UTC date.
        hour: UTC hour, 0 to 23.

    Raises:
        ValueError: If ``hour`` is outside 0 to 23.
    """

    date: dt.date
    hour: int

    def __post_init__(self) -> None:
        if not 0 <= self.hour < HOURS_PER_DAY:
            raise ValueError(f"hour must be in [0, 23], got {self.hour}")

    @classmethod
    def of_wall_ns(cls, wall_ns: int) -> HourKey:
        """The hour holding a wall-clock instant.

        Args:
            wall_ns: Nanoseconds since the Unix epoch.

        Returns:
            The UTC hour that contains ``wall_ns``.
        """
        moment = wall_ns_to_datetime(wall_ns)
        return cls(moment.date(), moment.hour)

    @classmethod
    def parse(cls, text: str) -> HourKey:
        """Parse a label such as ``2026-09-11T01``.

        Args:
            text: ``YYYY-MM-DDTHH``, in UTC.

        Returns:
            The hour it names.

        Raises:
            ValueError: If the text is not such a label or names no real date.
        """
        match = _HOUR_LABEL.fullmatch(text)
        if match is None:
            raise ValueError(f"expected an hour as YYYY-MM-DDTHH, got {text!r}")
        try:
            date = dt.date.fromisoformat(match.group(1))
        except ValueError as exc:
            raise ValueError(f"{text!r} names no date: {exc}") from exc
        return cls(date, int(match.group(2)))

    @property
    def label(self) -> str:
        """``YYYY-MM-DDTHH``, the form :meth:`parse` reads."""
        return f"{self.date.isoformat()}T{self.hour:02d}"

    @property
    def start_wall_ns(self) -> int:
        """Wall-clock nanoseconds at the first instant of the hour."""
        start = dt.datetime.combine(self.date, dt.time(self.hour), tzinfo=dt.UTC)
        return ((start - _EPOCH) // _ONE_SECOND) * NS_PER_S

    @property
    def end_wall_ns(self) -> int:
        """Wall-clock nanoseconds at the first instant of the next hour."""
        return self.start_wall_ns + NS_PER_HOUR


class DataLayout(msgspec.Struct, frozen=True, kw_only=True):
    """The four roots of an archive (docs/DATA_FORMATS.md 4 to 7).

    They normally share one data directory (:meth:`under`); they are separate so that raw
    segments can be read from one place while tables and manifests are written to another.

    Attributes:
        raw: Root of ``YYYY-MM-DD/HH/*.tape.zst``.
        keyframes: Root of ``YYYY-MM-DD/HH/MM.parquet``.
        baked: Root of ``<table>/dt=YYYY-MM-DD/hour=HH/part-<n>.parquet``.
        manifests: Root of ``YYYY-MM-DD.json``.
    """

    raw: Path
    keyframes: Path
    baked: Path
    manifests: Path

    @classmethod
    def under(cls, data_dir: Path) -> DataLayout:
        """The layout of one data directory, as the recorder writes it.

        Args:
            data_dir: The ``recorder.data_dir`` setting.

        Returns:
            ``raw/``, ``keyframes/``, ``baked/``, and ``manifests/`` under ``data_dir``.
        """
        return cls(
            raw=data_dir / "raw",
            keyframes=data_dir / "keyframes",
            baked=data_dir / "baked",
            manifests=data_dir / "manifests",
        )

    @property
    def lock_path(self) -> Path:
        """File whose lock one bake or prune holds at a time."""
        return self.manifests / _LOCK

    def hour_dir(self, hour: HourKey) -> Path:
        """``raw/YYYY-MM-DD/HH``."""
        return self.raw / hour.date.isoformat() / f"{hour.hour:02d}"

    def segment_files(self, hour: HourKey) -> tuple[Path, ...]:
        """Every segment file of an hour, in name order; empty when the directory is absent."""
        directory = self.hour_dir(hour)
        if not directory.is_dir():
            return ()
        return tuple(
            sorted(
                path
                for path in directory.iterdir()
                if path.name.endswith(SEGMENT_SUFFIX) and path.is_file()
            )
        )

    def raw_hours(self) -> tuple[HourKey, ...]:
        """Every hour with a raw directory, in time order; entries with other names are ignored."""
        if not self.raw.is_dir():
            return ()
        hours: list[HourKey] = []
        for date_dir in self.raw.iterdir():
            if not (_DATE_DIR.fullmatch(date_dir.name) and date_dir.is_dir()):
                continue
            for hour_dir in date_dir.iterdir():
                if not (_HOUR_DIR.fullmatch(hour_dir.name) and hour_dir.is_dir()):
                    continue
                try:
                    hours.append(HourKey.parse(f"{date_dir.name}T{hour_dir.name}"))
                except ValueError:
                    continue
        return tuple(sorted(hours))

    def segment_name(self, path: Path) -> str:
        """A segment's name in a manifest: its POSIX path relative to ``raw``."""
        return path.relative_to(self.raw).as_posix()

    def segment_path(self, name: str) -> Path:
        """The file a manifest's segment name refers to."""
        return self.raw / PurePosixPath(name)

    def partition_dir(self, table: str, hour: HourKey) -> Path:
        """``baked/<table>/dt=YYYY-MM-DD/hour=HH``."""
        return self.baked / table / f"dt={hour.date.isoformat()}" / f"hour={hour.hour:02d}"

    def part_name(self, path: Path) -> str:
        """A part file's name in a manifest: its POSIX path relative to ``baked``."""
        return path.relative_to(self.baked).as_posix()

    def part_path(self, name: str) -> Path:
        """The file a manifest's part name refers to."""
        return self.baked / PurePosixPath(name)

    def staging_dir(self, hour: HourKey) -> Path:
        """Scratch space of one bake, beside the tables it replaces so a rename moves them."""
        return self.baked / _STAGING / hour.label

    def manifest_path(self, date: dt.date) -> Path:
        """``manifests/YYYY-MM-DD.json``."""
        return self.manifests / f"{date.isoformat()}.json"

    def keyframe_dir(self, hour: HourKey) -> Path:
        """``keyframes/YYYY-MM-DD/HH``."""
        return self.keyframes / hour.date.isoformat() / f"{hour.hour:02d}"


def closed_hours(
    hours: Iterable[HourKey], *, now_wall_ns: int, grace_ns: int
) -> tuple[HourKey, ...]:
    """The hours that ended more than a grace period ago, in the order given.

    Args:
        hours: Candidate hours.
        now_wall_ns: The current wall-clock time.
        grace_ns: How long after an hour ends its files may still be written.

    Returns:
        Every hour whose end plus ``grace_ns`` is before ``now_wall_ns``.

    Raises:
        ValueError: If ``grace_ns`` is negative.
    """
    if grace_ns < 0:
        raise ValueError(f"grace_ns must be non-negative, got {grace_ns}")
    return tuple(hour for hour in hours if hour.end_wall_ns + grace_ns < now_wall_ns)
