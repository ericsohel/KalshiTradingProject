"""Sequence-gap detection for the recorder's subscriptions.

Responsibility: classify every inbound message's ``seq`` against the last one seen on
its ``sid`` (docs/DATA_FORMATS.md 3.3), so the recorder can write a gap record and ask
for a snapshot the moment a subscription skips (docs/ARCHITECTURE.md 7.1), and count
what it saw for the daily manifest. The tracker is pure bookkeeping: it reads no clock,
performs no I/O, and never raises on anything the exchange might send. One tracker
serves one connection, because ``sid`` values are connection-scoped.

Invariants: a sid's baseline only ever moves forward; the first sequenced message on a
sid (or the first after :meth:`GapTracker.reset`) establishes the baseline and is never
a gap; a skipped range is reported exactly once, by the message that ends it; an
unsequenced message (``seq is None``) never changes the baseline; and for every sid
``gaps + duplicates <= messages``.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Final

import msgspec

__all__ = [
    "Duplicate",
    "FirstMessage",
    "Gap",
    "GapTracker",
    "GapVerdict",
    "Ok",
]


class Ok(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """The message is the next one expected, or carries no sequence number at all."""


class FirstMessage(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """The first sequenced message on the sid; its ``seq`` is now the baseline."""


class Gap(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """One or more messages were skipped; every book on the sid is now suspect.

    Attributes:
        expected: The sequence number that should have arrived.
        got: The sequence number that did arrive, greater than ``expected``.
    """

    expected: int
    got: int


class Duplicate(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """The sequence number did not advance: a replay, or a reset the tracker was not told of.

    Kalshi's documentation does not say whether a snapshot restarts ``seq``, so a
    decrease is reported rather than trusted or treated as fatal.

    Attributes:
        expected: The sequence number that should have arrived.
        got: The sequence number that did arrive, less than ``expected``.
    """

    expected: int
    got: int


GapVerdict = Ok | FirstMessage | Gap | Duplicate
"""The tracker's classification of one message."""

_OK: Final = Ok()
_FIRST_MESSAGE: Final = FirstMessage()


@dataclass(slots=True)
class _SidState:
    """What the tracker remembers about one subscription."""

    last_seq: int | None = None
    messages: int = 0
    gaps: int = 0
    duplicates: int = 0


class GapTracker:
    """Per-sid sequence tracking for one WebSocket connection.

    Deliberately mutable and single-owner, like ``Book``: the connection supervisor
    feeds it every message in receive order. It is not thread-safe.
    """

    __slots__ = ("_states",)

    def __init__(self) -> None:
        self._states: dict[int, _SidState] = {}

    def observe(self, sid: int, seq: int | None) -> GapVerdict:
        """Classify one message and advance the sid's baseline.

        Args:
            sid: Subscription id the message arrived on.
            seq: The message's sequence number, or ``None`` on an unsequenced channel
                such as ``ticker`` or ``fill``.

        Returns:
            :class:`Ok` for the expected successor or an unsequenced message;
            :class:`FirstMessage` when no baseline exists yet; :class:`Gap` when numbers
            were skipped, after which the baseline is ``seq`` so the gap is reported
            once; :class:`Duplicate` when ``seq`` did not advance, which leaves the
            baseline where it was.
        """
        state = self._states.get(sid)
        if state is None:
            state = self._states[sid] = _SidState()
        state.messages += 1
        if seq is None:
            return _OK
        last_seq = state.last_seq
        if last_seq is None:
            state.last_seq = seq
            return _FIRST_MESSAGE
        expected = last_seq + 1
        if seq == expected:
            state.last_seq = seq
            return _OK
        if seq > expected:
            state.last_seq = seq
            state.gaps += 1
            return Gap(expected=expected, got=seq)
        state.duplicates += 1
        return Duplicate(expected=expected, got=seq)

    def reset(self, sid: int) -> None:
        """Forget a sid's baseline so its next sequenced message is a :class:`FirstMessage`.

        Call it when the numbering is known to restart, for example after a
        resubscribe. The counters are kept, because they describe the whole session.
        Resetting an unknown sid does nothing.

        Args:
            sid: Subscription whose baseline to clear.
        """
        state = self._states.get(sid)
        if state is not None:
            state.last_seq = None

    def forget(self, sid: int) -> None:
        """Drop everything known about a sid, counters included, after it is unsubscribed.

        Forgetting an unknown sid does nothing.

        Args:
            sid: Subscription to drop.
        """
        self._states.pop(sid, None)

    def sids(self) -> Iterator[int]:
        """Iterate over every sid observed and not forgotten, in ascending order.

        Returns:
            The tracked sids.
        """
        return iter(sorted(self._states))

    def messages(self, sid: int) -> int:
        """Count the messages observed on a sid, sequenced or not.

        Args:
            sid: Subscription to report on.

        Returns:
            The count, or zero for a sid never observed or since forgotten.
        """
        state = self._states.get(sid)
        return 0 if state is None else state.messages

    def gaps(self, sid: int) -> int:
        """Count the :class:`Gap` verdicts issued for a sid.

        Args:
            sid: Subscription to report on.

        Returns:
            The count, or zero for a sid never observed or since forgotten.
        """
        state = self._states.get(sid)
        return 0 if state is None else state.gaps

    def duplicates(self, sid: int) -> int:
        """Count the :class:`Duplicate` verdicts issued for a sid.

        Args:
            sid: Subscription to report on.

        Returns:
            The count, or zero for a sid never observed or since forgotten.
        """
        state = self._states.get(sid)
        return 0 if state is None else state.duplicates
