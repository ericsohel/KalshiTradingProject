"""Clocks and timestamp conversions."""

from __future__ import annotations

from datetime import UTC

import pytest

from tape.timeutil import (
    FrozenClock,
    Ms,
    Ns,
    SystemClock,
    ms_to_ns,
    ns_to_ms,
    s_to_ms,
    wall_ns_to_datetime,
)


def test_conversions() -> None:
    assert ms_to_ns(Ms(1)) == 1_000_000
    assert ns_to_ms(Ns(1_999_999)) == 1
    assert s_to_ms(3) == 3_000


def test_frozen_clock_advances_both_readings() -> None:
    clock = FrozenClock(mono_ns=10, wall_ns=20)
    clock.advance(5)
    assert clock.mono_ns() == 15
    assert clock.wall_ns() == 25


def test_frozen_clock_refuses_to_go_backwards() -> None:
    with pytest.raises(ValueError, match="backwards"):
        FrozenClock().advance(-1)


def test_system_clock_is_monotonic_and_roughly_now() -> None:
    clock = SystemClock()
    a = clock.mono_ns()
    b = clock.mono_ns()
    assert b >= a
    assert clock.wall_ns() > 1_600_000_000 * 1_000_000_000


def test_wall_ns_to_datetime_keeps_microseconds() -> None:
    dt = wall_ns_to_datetime(Ns(1_700_000_000_123_456_789))
    assert dt.tzinfo is UTC
    assert dt.microsecond == 123_456
    assert dt.year == 2023
