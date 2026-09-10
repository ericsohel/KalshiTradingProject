"""Sequence-gap classification per subscription, and the counters behind the manifest."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from tape.recorder import Duplicate, FirstMessage, Gap, GapTracker, Ok


def test_first_sequenced_message_sets_the_baseline() -> None:
    tracker = GapTracker()
    assert tracker.observe(7, 42) == FirstMessage()
    assert tracker.observe(7, 43) == Ok()
    assert tracker.messages(7) == 2
    assert tracker.gaps(7) == 0
    assert tracker.duplicates(7) == 0


def test_a_skip_is_reported_once_and_moves_the_baseline() -> None:
    tracker = GapTracker()
    tracker.observe(1, 1)
    assert tracker.observe(1, 5) == Gap(expected=2, got=5)
    assert tracker.observe(1, 6) == Ok()
    assert tracker.gaps(1) == 1


def test_a_repeat_or_decrease_is_a_duplicate_and_keeps_the_baseline() -> None:
    tracker = GapTracker()
    tracker.observe(1, 10)
    assert tracker.observe(1, 10) == Duplicate(expected=11, got=10)
    assert tracker.observe(1, 3) == Duplicate(expected=11, got=3)
    assert tracker.observe(1, 11) == Ok()
    assert tracker.duplicates(1) == 2
    assert tracker.gaps(1) == 0


def test_unsequenced_messages_are_ok_and_leave_the_baseline_alone() -> None:
    tracker = GapTracker()
    assert tracker.observe(3, None) == Ok()
    assert tracker.observe(3, 8) == FirstMessage()
    assert tracker.observe(3, None) == Ok()
    assert tracker.observe(3, 9) == Ok()
    assert tracker.messages(3) == 4


def test_sids_are_tracked_independently() -> None:
    tracker = GapTracker()
    tracker.observe(2, 100)
    tracker.observe(1, 1)
    assert tracker.observe(1, 2) == Ok()
    assert tracker.observe(2, 102) == Gap(expected=101, got=102)
    assert tracker.gaps(1) == 0
    assert list(tracker.sids()) == [1, 2]


def test_reset_restarts_the_numbering_but_keeps_the_counters() -> None:
    tracker = GapTracker()
    tracker.observe(1, 1)
    tracker.observe(1, 3)
    tracker.reset(1)
    assert tracker.observe(1, 1) == FirstMessage()
    assert tracker.observe(1, 2) == Ok()
    assert (tracker.messages(1), tracker.gaps(1)) == (4, 1)
    tracker.reset(99)
    assert list(tracker.sids()) == [1]


def test_forget_drops_the_sid_and_its_counters() -> None:
    tracker = GapTracker()
    tracker.observe(1, 1)
    tracker.observe(1, 1)
    tracker.forget(1)
    tracker.forget(99)
    assert list(tracker.sids()) == []
    assert (tracker.messages(1), tracker.gaps(1), tracker.duplicates(1)) == (0, 0, 0)
    assert tracker.observe(1, 50) == FirstMessage()


@given(
    st.integers(0, 10**9),
    st.lists(st.none() | st.integers(1, 4), max_size=100),
)
@settings(max_examples=300)
def test_a_gap_is_reported_exactly_when_the_sequence_skips(
    start: int, steps: list[int | None]
) -> None:
    tracker = GapTracker()
    assert tracker.observe(5, start) == FirstMessage()
    seq = start
    missing = 0
    for step in steps:
        if step is None:
            assert tracker.observe(5, None) == Ok()
            continue
        previous, seq = seq, seq + step
        verdict = tracker.observe(5, seq)
        if step == 1:
            assert verdict == Ok()
        else:
            assert verdict == Gap(expected=previous + 1, got=seq)
            missing += seq - (previous + 1)
    sequenced = [step for step in steps if step is not None]
    assert missing == seq - start - len(sequenced)
    assert tracker.gaps(5) == sum(1 for step in sequenced if step > 1)
    assert tracker.duplicates(5) == 0
    assert tracker.messages(5) == len(steps) + 1


@given(st.lists(st.none() | st.integers(-5, 40), max_size=100))
@settings(max_examples=300)
def test_the_baseline_never_moves_backwards_whatever_arrives(seqs: list[int | None]) -> None:
    tracker = GapTracker()
    highest: int | None = None
    for seq in seqs:
        verdict = tracker.observe(0, seq)
        if seq is None:
            assert verdict == Ok()
        elif highest is None:
            assert verdict == FirstMessage()
        elif seq <= highest:
            assert verdict == Duplicate(expected=highest + 1, got=seq)
        elif seq == highest + 1:
            assert verdict == Ok()
        else:
            assert verdict == Gap(expected=highest + 1, got=seq)
        if seq is not None:
            highest = seq if highest is None else max(highest, seq)
    assert tracker.gaps(0) + tracker.duplicates(0) <= tracker.messages(0) == len(seqs)
