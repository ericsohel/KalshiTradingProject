"""Typed timestamps and the only clock abstraction in the package.

Three clocks have three jobs (docs/DATA_FORMATS.md 1.2): exchange event time
(``Ms``, milliseconds since the Unix epoch, from Kalshi payloads), local monotonic time
(``Ns``, for latency and ordering), and local wall time (``Ns``, for file placement and
display). ``SystemClock`` is the only place ``time`` is called; everything else receives
a ``Clock`` (ADR 0004).
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Final, NewType, Protocol

__all__ = [
    "NS_PER_MS",
    "NS_PER_S",
    "Clock",
    "FrozenClock",
    "Ms",
    "Ns",
    "SystemClock",
    "ms_to_ns",
    "ns_to_ms",
    "s_to_ms",
    "wall_ns_to_datetime",
]

Ms = NewType("Ms", int)
Ns = NewType("Ns", int)

NS_PER_MS: Final = 1_000_000
NS_PER_S: Final = 1_000_000_000
_MS_PER_S: Final = 1_000


class Clock(Protocol):
    """Source of monotonic and wall-clock time in nanoseconds."""

    def mono_ns(self) -> Ns:
        """Monotonic nanoseconds; meaningful only for differences within a process."""
        ...

    def wall_ns(self) -> Ns:
        """Wall-clock nanoseconds since the Unix epoch."""
        ...


class SystemClock:
    """The real clock. Construct it only in a composition root."""

    def mono_ns(self) -> Ns:
        """Return ``time.monotonic_ns()``."""
        return Ns(time.monotonic_ns())

    def wall_ns(self) -> Ns:
        """Return ``time.time_ns()``."""
        return Ns(time.time_ns())


class FrozenClock:
    """A clock that moves only when told to. For tests and replay.

    Args:
        mono_ns: Initial monotonic reading.
        wall_ns: Initial wall-clock reading.
    """

    def __init__(self, mono_ns: int = 0, wall_ns: int = 0) -> None:
        self._mono = Ns(mono_ns)
        self._wall = Ns(wall_ns)

    def mono_ns(self) -> Ns:
        """Return the current frozen monotonic reading."""
        return self._mono

    def wall_ns(self) -> Ns:
        """Return the current frozen wall reading."""
        return self._wall

    def advance(self, delta_ns: int) -> None:
        """Move both readings forward by ``delta_ns``.

        Raises:
            ValueError: If ``delta_ns`` is negative; clocks do not run backwards.
        """
        if delta_ns < 0:
            raise ValueError("clock cannot move backwards")
        self._mono = Ns(self._mono + delta_ns)
        self._wall = Ns(self._wall + delta_ns)


def ms_to_ns(ms: Ms | int) -> Ns:
    """Convert milliseconds to nanoseconds exactly."""
    return Ns(ms * NS_PER_MS)


def ns_to_ms(ns: Ns | int) -> Ms:
    """Convert nanoseconds to milliseconds, rounding toward negative infinity."""
    return Ms(ns // NS_PER_MS)


def s_to_ms(seconds: int) -> Ms:
    """Convert whole seconds (Kalshi lifecycle timestamps) to milliseconds."""
    return Ms(seconds * _MS_PER_S)


def wall_ns_to_datetime(ns: Ns | int) -> datetime:
    """Convert wall-clock nanoseconds to an aware UTC ``datetime`` (microsecond precision)."""
    return datetime.fromtimestamp(ns // NS_PER_S, tz=UTC).replace(
        microsecond=(ns % NS_PER_S) // 1_000
    )
