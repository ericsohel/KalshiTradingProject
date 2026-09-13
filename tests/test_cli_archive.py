"""The ``bake`` and ``prune`` commands: exit codes, summaries, and what they change on disk."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from tape.bake import DataLayout, HourKey, archive_lock, bake_hour, read_manifest
from tape.bake.bake import BakeReport
from tape.bake.layout import NS_PER_HOUR
from tape.cli import bake_archive, chrony_offset_ms, main, prune_archive
from tape.config import load_settings
from tape.errors import ArchiveError
from tape.timeutil import FrozenClock
from tests.fakes.synthetic_tape import HOUR, SECOND, SegmentScript

EARLIER = HourKey(HOUR.date, 11)
NOTHING_BAKED: dict[str, list[object]] = {"baked": [], "failed": []}


@pytest.fixture
def root_logger() -> Iterator[logging.Logger]:
    """Restore the root logger that the commands configure."""
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield root
    for handler in list(root.handlers):
        root.removeHandler(handler)
    for handler in handlers:
        root.addHandler(handler)
    root.setLevel(level)


@pytest.fixture
def memory_pool() -> Iterator[None]:
    """Restore the Arrow memory pool that ``tape bake`` replaces."""
    previous = pa.default_memory_pool()
    yield
    pa.set_memory_pool(previous)


@pytest.fixture
def config(tmp_path: Path) -> Path:
    path = tmp_path / "tape.toml"
    path.write_text('[kalshi]\nenv = "demo"\n\n[recorder]\ndata_dir = "data"\n')
    return path


@pytest.fixture
def layout(tmp_path: Path) -> DataLayout:
    return DataLayout.under(tmp_path / "data")


def write_hour(layout: DataLayout, hour: HourKey) -> None:
    for conn_id in (2, 3):
        script = SegmentScript(conn_id=conn_id)
        script.opened(hour.start_wall_ns + SECOND)
        script.subscribed(hour.start_wall_ns + 2 * SECOND, channel="orderbook_delta", sid=1)
        script.snapshot(hour.start_wall_ns + 3 * SECOND, sid=1, seq=1, ticker=f"KX{conn_id}-X")
        script.delta(hour.start_wall_ns + 4 * SECOND, sid=1, seq=2, ticker=f"KX{conn_id}-X")
        script.write(layout, hour, 0)


def run(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, dict[str, Any]]:
    """Run a command and decode the JSON summary it prints."""
    status = main(argv)
    out = capsys.readouterr().out
    summary: dict[str, Any] = json.loads(out) if out else {}  # Any: a decoded JSON document
    return status, summary


def baked_hours(summary: dict[str, Any]) -> list[str]:  # Any: a decoded JSON document
    return [entry["hour"] for entry in summary["baked"]]


@pytest.mark.usefixtures("root_logger", "memory_pool")
def test_bake_bakes_closed_hours_once_and_prune_deletes_only_with_apply(
    config: Path,
    layout: DataLayout,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_hour(layout, EARLIER)
    write_hour(layout, HOUR)
    bake = ["bake", "--config", str(config)]

    status, summary = run(bake, capsys)
    assert status == 0
    assert baked_hours(summary) == [EARLIER.label, HOUR.label]
    assert summary["failed"] == []
    first = summary["baked"][0]
    assert (first["records"], first["decode_failures"], first["rows"]["deltas"]) == (8, 0, 2)
    assert first["baked_bytes"] > 0
    assert pa.default_memory_pool().backend_name == "system"
    assert run(bake, capsys) == (0, NOTHING_BAKED)
    status, summary = run([*bake, "--hour", HOUR.label, "--force"], capsys)
    assert baked_hours(summary) == [HOUR.label]

    # Both hours ended long before the test runs, so a day's retention has passed for them.
    monkeypatch.setenv("TAPE_BAKE__RAW_RETENTION_HOURS", "24")
    status, report = run(["prune", "--config", str(config)], capsys)
    assert status == 0
    assert (report["applied"], report["prunable_hours"], report["pruned_segments"]) == (False, 2, 0)
    assert report["retention_hours"] == 24
    assert len(layout.segment_files(HOUR)) == 2

    status, report = run(["prune", "--config", str(config), "--apply"], capsys)
    assert (status, report["pruned_segments"]) == (0, 4)
    assert layout.raw_hours() == ()
    manifest = read_manifest(layout.manifest_path(HOUR.date))
    assert manifest is not None
    assert len(manifest.pruned) == 4
    assert run([*bake, "--hour", HOUR.label, "--force"], capsys) == (0, NOTHING_BAKED)


@pytest.mark.usefixtures("root_logger", "memory_pool")
def test_bake_refuses_an_open_hour_a_held_lock_and_a_bad_label(
    config: Path, layout: DataLayout, capsys: pytest.CaptureFixture[str]
) -> None:
    settings = load_settings(config, environ={})
    write_hour(layout, HOUR)
    just_after = FrozenClock(wall_ns=HOUR.end_wall_ns + 10 * SECOND)
    with pytest.raises(ArchiveError, match="is not closed"):
        bake_archive(settings, clock=just_after, hour=HOUR, force=False, clock_offset_ms=None)
    nothing = bake_archive(settings, clock=just_after, hour=None, force=False, clock_offset_ms=None)
    assert nothing.baked == ()

    assert main(["bake", "--config", str(config), "--hour", "2999-01-01T00"]) == 1
    with archive_lock(layout):
        assert main(["bake", "--config", str(config)]) == 1
        assert main(["prune", "--config", str(config)]) == 1
    with pytest.raises(SystemExit) as usage:
        main(["bake", "--config", str(config), "--hour", "yesterday"])
    assert usage.value.code == 2
    assert "YYYY-MM-DDTHH" in capsys.readouterr().err


@pytest.mark.usefixtures("root_logger", "memory_pool")
def test_a_failing_hour_is_reported_and_the_others_are_still_baked(
    config: Path,
    layout: DataLayout,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    write_hour(layout, EARLIER)
    write_hour(layout, HOUR)

    def failing(archive: DataLayout, hour: HourKey, **options: Any) -> BakeReport:  # Any: kwargs
        if hour == EARLIER:
            raise OSError("disk full")
        return bake_hour(archive, hour, **options)

    monkeypatch.setattr("tape.cli.bake_hour", failing)
    status, summary = run(["bake", "--config", str(config)], capsys)

    assert status == 1
    assert summary["failed"] == [{"hour": EARLIER.label, "error": "disk full"}]
    assert baked_hours(summary) == [HOUR.label]


def test_a_manifest_that_does_not_decode_stops_bake_and_prune(
    config: Path, layout: DataLayout, root_logger: logging.Logger
) -> None:
    write_hour(layout, HOUR)
    layout.manifests.mkdir(parents=True)
    layout.manifest_path(HOUR.date).write_text("{}")
    assert main(["bake", "--config", str(config)]) == 1
    assert main(["prune", "--config", str(config)]) == 1
    assert root_logger.handlers


def test_prune_reports_every_reason_an_hour_is_kept(config: Path, layout: DataLayout) -> None:
    settings = load_settings(config, environ={})
    write_hour(layout, HOUR)
    later = FrozenClock(wall_ns=HOUR.end_wall_ns + NS_PER_HOUR)
    summary = prune_archive(settings, clock=later, apply=True)
    assert [(hour.hour, hour.prunable, hour.reasons) for hour in summary.hours] == [
        (HOUR.label, False, ("inside_retention_window", "not_baked"))
    ]
    assert (summary.prunable_hours, summary.pruned_segments) == (0, 0)
    assert len(layout.segment_files(HOUR)) == 2


def test_the_chrony_offset_is_none_unless_chronyc_answers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(shutil, "which", lambda _name: None)
    assert chrony_offset_ms() is None

    monkeypatch.setattr(shutil, "which", lambda _name: "/usr/bin/chronyc")
    tracking = "A,b,2,1.0,-0.0125,0,0,0,0,0,0,0,0,Normal\n"
    answers: list[subprocess.CompletedProcess[str] | Exception] = [
        subprocess.CompletedProcess(args=[], returncode=0, stdout=tracking),
        subprocess.CompletedProcess(args=[], returncode=1, stdout=""),
        subprocess.TimeoutExpired(cmd="chronyc", timeout=5),
    ]

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert chrony_offset_ms() == -12
    assert chrony_offset_ms() is None
    assert chrony_offset_ms() is None
