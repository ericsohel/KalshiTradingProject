"""Shared fixtures."""

from __future__ import annotations

import pytest

from tape.events import Receipt
from tape.timeutil import Ns


@pytest.fixture
def receipt() -> Receipt:
    """A fixed local receipt for converted events."""
    return Receipt(conn_id=1, recv_mono_ns=Ns(1_000), recv_wall_ns=Ns(1_700_000_000_000_000_000))
