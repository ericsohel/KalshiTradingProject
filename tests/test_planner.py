"""Connection planning: one group per connection, capacity, stability, and commands."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tape.client.ws import SubscribeCommand, UnsubscribeCommand, UpdateSubscriptionCommand
from tape.recorder import (
    AddGroup,
    AddMarkets,
    Group,
    Plan,
    PlanChange,
    RemoveGroup,
    RemoveMarkets,
    diff,
    group_sort_key,
    plan,
    to_commands,
)

CHANNELS = ("orderbook_delta", "trade")


def group(tickers: Iterable[str], *, conn: int = 0, group_id: str | None = None) -> Group:
    return Group(
        group_id=f"g{conn:04d}" if group_id is None else group_id,
        conn_id=conn,
        tickers=frozenset(tickers),
    )


def make_plan(*groups: Group) -> Plan:
    return Plan(groups=tuple(sorted(groups, key=group_sort_key)))


def with_tickers(old: Group, tickers: frozenset[str]) -> Group:
    return Group(group_id=old.group_id, conn_id=old.conn_id, tickers=tickers)


def apply(changes: Sequence[PlanChange], current: Plan, *, max_per_group: int) -> Plan:
    """Apply changes in order, checking every intermediate state the exchange would see."""
    groups = dict(current.by_id)
    for change in changes:
        if isinstance(change, AddGroup):
            assert change.group.group_id not in groups
            groups[change.group.group_id] = change.group
        elif isinstance(change, RemoveGroup):
            del groups[change.group_id]
        elif isinstance(change, AddMarkets):
            old = groups[change.group_id]
            assert old.tickers.isdisjoint(change.tickers)
            groups[change.group_id] = with_tickers(old, old.tickers | set(change.tickers))
        else:
            old = groups[change.group_id]
            assert set(change.tickers) < old.tickers
            groups[change.group_id] = with_tickers(old, old.tickers - set(change.tickers))
        held = [ticker for kept in groups.values() for ticker in kept.tickers]
        assert len(held) == len(set(held)), "a ticker is in two groups mid-transition"
        assert all(len(kept.tickers) <= max_per_group for kept in groups.values())
        conns = [kept.conn_id for kept in groups.values()]
        assert len(conns) == len(set(conns)), "a connection has two subscriptions mid-transition"
    return make_plan(*groups.values())


# ---------------------------------------------------------------------- shapes


def test_group_and_plan_reject_malformed_shapes() -> None:
    with pytest.raises(ValueError, match="no tickers"):
        group([])
    with pytest.raises(ValueError, match="negative conn_id"):
        group(["a"], conn=-1)
    with pytest.raises(ValueError, match="sorted"):
        Plan(groups=(group(["a"], conn=1), group(["b"], conn=0)))
    with pytest.raises(ValueError, match="two groups on one connection"):
        Plan(groups=(group(["a"], group_id="x"), group(["b"], group_id="y")))
    with pytest.raises(ValueError, match="duplicate group ids"):
        Plan(groups=(group(["a"], group_id="x"), group(["b"], conn=1, group_id="x")))
    with pytest.raises(ValueError, match="x is in both g0000 and g0001"):
        Plan(groups=(group(["x"]), group(["x"], conn=1)))


def test_plan_exposes_its_tickers_and_its_groups_by_id() -> None:
    first, second = group(["a", "b"]), group(["c"], conn=1)
    subject = make_plan(first, second)
    assert subject.tickers == frozenset({"a", "b", "c"})
    assert subject.by_id == {"g0000": first, "g0001": second}


# -------------------------------------------------------------------- planning


def test_plan_validates_its_arguments() -> None:
    with pytest.raises(ValueError, match="max_per_group"):
        plan([], max_per_group=0, max_connections=1)
    with pytest.raises(ValueError, match="max_connections"):
        plan([], max_per_group=1, max_connections=0)


def test_an_empty_universe_is_an_empty_plan() -> None:
    empty = plan([], max_per_group=5, max_connections=2)
    assert empty == Plan(groups=())
    assert empty.tickers == frozenset()
    assert diff(empty, empty) == ()


def test_a_fresh_plan_deals_tickers_in_order_to_the_least_loaded_connection() -> None:
    result = plan(["e", "a", "d", "b", "c"], max_per_group=3, max_connections=2)
    assert result == make_plan(group(["a", "c", "e"], conn=0), group(["b", "d"], conn=1))


def test_markets_on_different_exchange_shards_share_a_connection() -> None:
    """The planner has no notion of shards: the exchange merges them into one subscription."""
    result = plan(["KXA-1", "KXB-1"], max_per_group=500, max_connections=1)
    assert result == make_plan(group(["KXA-1", "KXB-1"]))


def test_a_replan_keeps_members_in_place_and_deals_new_tickers_to_the_emptiest() -> None:
    previous = make_plan(group(["a", "b", "c"], conn=0), group(["d"], conn=1))
    wanted = ["a", "c", "d", "e", "f", "g"]
    result = plan(wanted, max_per_group=3, max_connections=3, previous=previous)
    assert result == make_plan(
        group(["a", "c"], conn=0), group(["d", "f"], conn=1), group(["e", "g"], conn=2)
    )


def test_a_replan_evicts_the_surplus_of_a_shrunken_connection_in_ticker_order() -> None:
    previous = make_plan(group(["a", "b", "c"], conn=0))
    result = plan(["a", "b", "c"], max_per_group=2, max_connections=2, previous=previous)
    assert result == make_plan(group(["a", "b"], conn=0), group(["c"], conn=1))


def test_a_replan_moves_only_markets_of_connections_that_no_longer_exist() -> None:
    previous = make_plan(group(["a"], conn=0), group(["b"], conn=1), group(["c"], conn=3))
    result = plan(["a", "b", "c"], max_per_group=2, max_connections=2, previous=previous)
    assert result == make_plan(group(["a", "c"], conn=0), group(["b"], conn=1))


def test_tickers_beyond_capacity_are_left_out_and_kept_members_never_yield() -> None:
    previous = make_plan(group(["z"], conn=0))
    result = plan(["a", "b", "c", "z"], max_per_group=1, max_connections=2, previous=previous)
    assert result == make_plan(group(["z"], conn=0), group(["a"], conn=1))
    assert frozenset({"a", "b", "c", "z"}) - result.tickers == {"b", "c"}


# ----------------------------------------------------------------------- diffs


def test_diff_orders_removals_before_additions() -> None:
    current = make_plan(group(["a", "b"], conn=0), group(["c"], conn=1))
    desired = make_plan(group(["a", "c"], conn=0), group(["b"], conn=2))
    changes = diff(current, desired)
    assert changes == (
        RemoveMarkets(group_id="g0000", tickers=("b",)),
        RemoveGroup(group_id="g0001"),
        AddGroup(group=group(["b"], conn=2)),
        AddMarkets(group_id="g0000", tickers=("c",)),
    )
    assert apply(changes, current, max_per_group=2) == desired
    assert diff(desired, desired) == ()


def test_diff_rebuilds_groups_that_cannot_be_updated_in_place() -> None:
    current = make_plan(group(["a"], group_id="moved"), group(["b"], conn=1, group_id="members"))
    desired = make_plan(
        group(["a"], conn=2, group_id="moved"), group(["d"], conn=1, group_id="members")
    )
    changes = diff(current, desired)
    assert changes == (
        RemoveGroup(group_id="members"),
        RemoveGroup(group_id="moved"),
        AddGroup(group=desired.by_id["members"]),
        AddGroup(group=desired.by_id["moved"]),
    )
    assert apply(changes, current, max_per_group=1) == desired


# -------------------------------------------------------------------- commands


def test_to_commands_maps_each_change_onto_its_websocket_command() -> None:
    changes = (
        RemoveMarkets(group_id="kept", tickers=("b",)),
        RemoveGroup(group_id="gone"),
        AddGroup(group=group(["z", "y"], group_id="new")),
        AddMarkets(group_id="kept", tickers=("c",)),
    )
    # One sid per channel: "kept" owns 7 and 8, "gone" owns 9 and 10.
    commands = to_commands(
        changes,
        channels=list(CHANNELS),
        use_yes_price=True,
        sid_of={"kept": (8, 7), "gone": (9, 10)},
    )
    assert commands == (
        UpdateSubscriptionCommand(sid=7, action="delete_markets", market_tickers=("b",)),
        UpdateSubscriptionCommand(sid=8, action="delete_markets", market_tickers=("b",)),
        UnsubscribeCommand(sids=(9, 10)),
        SubscribeCommand(channels=CHANNELS, market_tickers=("y", "z"), use_yes_price=True),
        UpdateSubscriptionCommand(sid=7, action="add_markets", market_tickers=("c",)),
        UpdateSubscriptionCommand(sid=8, action="add_markets", market_tickers=("c",)),
    )


def test_to_commands_updates_every_channel_sid_so_no_channel_drifts() -> None:
    """A membership change that reached only one channel's sid would leave the other
    channel recording a different market set, silently."""
    commands = to_commands(
        [AddMarkets(group_id="g", tickers=("m",))],
        channels=CHANNELS,
        use_yes_price=True,
        sid_of={"g": (3, 4)},
    )
    assert [c.sid for c in commands if isinstance(c, UpdateSubscriptionCommand)] == [3, 4]


@pytest.mark.parametrize(
    "change",
    [
        RemoveGroup(group_id="g"),
        AddMarkets(group_id="g", tickers=("a",)),
        RemoveMarkets(group_id="g", tickers=("a",)),
    ],
)
def test_to_commands_refuses_to_touch_a_group_without_a_sid(change: PlanChange) -> None:
    with pytest.raises(KeyError, match="group g has no sid yet"):
        to_commands([change], channels=CHANNELS, use_yes_price=True, sid_of={})
    with pytest.raises(KeyError, match="group g has no sid yet"):
        to_commands([change], channels=CHANNELS, use_yes_price=True, sid_of={"g": ()})


def test_to_commands_needs_a_channel() -> None:
    with pytest.raises(ValueError, match="at least one channel"):
        to_commands([], channels=(), use_yes_price=True, sid_of={})


# ------------------------------------------------------------------ properties

universes = st.frozensets(st.text(alphabet="abcdefghij", min_size=1, max_size=2), max_size=40)
caps = st.integers(1, 6)
connection_counts = st.integers(1, 4)


def conn_of(subject: Plan) -> dict[str, int]:
    return {ticker: kept.conn_id for kept in subject.groups for ticker in kept.tickers}


def assert_well_formed(
    subject: Plan, wanted: frozenset[str], *, max_per_group: int, max_connections: int
) -> None:
    """One group per connection (Plan enforces it), capacity, and coverage up to capacity."""
    assert subject.tickers <= wanted
    assert len(subject.tickers) == min(len(wanted), max_per_group * max_connections)
    for kept in subject.groups:
        assert 1 <= len(kept.tickers) <= max_per_group
        assert 0 <= kept.conn_id < max_connections


@given(
    first=universes,
    second=universes,
    cap_a=caps,
    cap_b=caps,
    conns_a=connection_counts,
    conns_b=connection_counts,
)
@settings(max_examples=300)
def test_every_connection_has_at_most_one_group_within_capacity(
    *,
    first: frozenset[str],
    second: frozenset[str],
    cap_a: int,
    cap_b: int,
    conns_a: int,
    conns_b: int,
) -> None:
    fresh = plan(first, max_per_group=cap_a, max_connections=conns_a)
    assert_well_formed(fresh, first, max_per_group=cap_a, max_connections=conns_a)
    load = conn_of(fresh)
    per_connection = [sum(1 for c in load.values() if c == conn) for conn in range(conns_a)]
    assert max(per_connection) - min(per_connection) <= 1
    replanned = plan(second, max_per_group=cap_b, max_connections=conns_b, previous=fresh)
    assert_well_formed(replanned, second, max_per_group=cap_b, max_connections=conns_b)


@given(universes, universes, caps, connection_counts, st.data())
@settings(max_examples=200)
def test_plan_does_not_depend_on_input_order(
    first: frozenset[str],
    second: frozenset[str],
    cap: int,
    conns: int,
    data: st.DataObject,
) -> None:
    previous = plan(first, max_per_group=cap, max_connections=conns)
    order = data.draw(st.permutations(sorted(second)))
    for base in (None, previous):
        assert plan(order, max_per_group=cap, max_connections=conns, previous=base) == plan(
            sorted(second), max_per_group=cap, max_connections=conns, previous=base
        )


@given(
    first=universes,
    second=universes,
    cap=caps,
    conns_a=connection_counts,
    conns_b=connection_counts,
    replan=st.booleans(),
)
@settings(max_examples=300)
def test_applying_the_diff_yields_exactly_the_desired_plan(
    *,
    first: frozenset[str],
    second: frozenset[str],
    cap: int,
    conns_a: int,
    conns_b: int,
    replan: bool,
) -> None:
    current = plan(first, max_per_group=cap, max_connections=conns_a)
    desired = plan(
        second,
        max_per_group=cap,
        max_connections=conns_b,
        previous=current if replan else None,
    )
    for source, target in ((current, desired), (desired, current)):
        changes = diff(source, target)
        assert apply(changes, source, max_per_group=cap) == target
        width = len(CHANNELS)
        sid_of = {
            kept.group_id: tuple(range(index * width, index * width + width))
            for index, kept in enumerate(source.groups)
        }
        commands = to_commands(changes, channels=CHANNELS, use_yes_price=True, sid_of=sid_of)
        membership = sum(isinstance(c, AddMarkets | RemoveMarkets) for c in changes)
        assert len(commands) == len(changes) + membership * (width - 1)


@given(
    first=universes,
    second=universes,
    cap_a=caps,
    cap_b=caps,
    conns_a=connection_counts,
    conns_b=connection_counts,
)
@settings(max_examples=300)
def test_replanning_moves_a_market_only_off_a_full_or_vanished_connection(
    *,
    first: frozenset[str],
    second: frozenset[str],
    cap_a: int,
    cap_b: int,
    conns_a: int,
    conns_b: int,
) -> None:
    current = plan(first, max_per_group=cap_a, max_connections=conns_a)
    desired = plan(second, max_per_group=cap_b, max_connections=conns_b, previous=current)
    before, after = conn_of(current), conn_of(desired)
    must_move: set[str] = set()
    for kept in current.groups:
        survivors = sorted(ticker for ticker in kept.tickers if ticker in second)
        must_move.update(survivors if kept.conn_id >= conns_b else survivors[cap_b:])
    for ticker in before.keys() & second:
        if ticker in must_move:
            continue
        # Everything else stays put, and is never dropped to make room for a newcomer.
        assert after.get(ticker) == before[ticker]
    dropped = second - desired.tickers
    assert not dropped or len(desired.tickers) == cap_b * conns_b
