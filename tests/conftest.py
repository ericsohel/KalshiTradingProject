"""Shared fixtures."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from tape.events import Receipt
from tape.timeutil import Ns


@pytest.fixture
def receipt() -> Receipt:
    """A fixed local receipt for converted events."""
    return Receipt(conn_id=1, recv_mono_ns=Ns(1_000), recv_wall_ns=Ns(1_700_000_000_000_000_000))


@pytest.fixture
def ipc_dir() -> Iterator[Path]:
    """A fresh directory for ZeroMQ ipc sockets, removed afterwards.

    Unix socket paths are capped at 103 bytes on macOS, which pytest's ``tmp_path`` can exceed,
    so this lives directly under the system temporary directory.
    """
    path = Path(tempfile.mkdtemp(prefix="tape-bus-"))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
