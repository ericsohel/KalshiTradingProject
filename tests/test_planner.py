"""Subscription planning: capacity, shard purity, stability across replans, and commands."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence

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


def group(group_id: str, tickers: Iterable[str], *, shard: int = 0, conn: int = 0) -> Group:
    return Group(group_id=group_id, exchange_index=shard, conn_id=conn, tickers=frozenset(tickers))


def make_plan(*groups: Group) -> Plan:
    return Plan(groups=tuple(sorted(groups, key=group_sort_key)))


def one_shard(tickers: Iterable[str], shard: int = 0) -> dict[str, int]:
    return dict.fromkeys(tickers, shard)


def with_tickers(old: Group, tickers: frozenset[str]) -> Group:
    return Group(
        group_id=old.group_id,
        exchange_index=old.exchange_index,
        conn_id=old.conn_id,
        tickers=tickers,
    )


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
    return make_plan(*groups.values())


# ---------------------------------------------------------------------- shapes


def test_group_and_plan_reject_malformed_shapes() -> None:
    with pytest.raises(ValueError, match="no tickers"):
        group("g", [])
    with pytest.raises(ValueError, match="negative conn_id"):
        group("g", ["a"], conn=-1)
    with pytest.raises(ValueError, match="sorted"):
        Plan(groups=(group("b", ["a"]), group("a", ["b"])))
    with pytest.raises(ValueError, match="duplicate group ids"):
        Plan(groups=(group("a", ["a"]), group("a", ["b"])))
    with pytest.raises(ValueError, match="x is in both a and b"):
        Plan(groups=(group("a", ["x"]), group("b", ["x"])))


def test_plan_exposes_its_tickers_and_its_groups_by_id() -> None:
    first, second = group("s0-g0000", ["a", "b"]), group("s1-g0000", ["c"], shard=1)
    subject = make_plan(first, second)
    assert subject.tickers == frozenset({"a", "b", "c"})
    assert subject.by_id == {"s0-g0000": first, "s1-g0000": second}


# -------------------------------------------------------------------- planning


def test_plan_validates_its_arguments() -> None:
    with pytest.raises(ValueError, match="max_per_group"):
        plan([], shard_of={}, max_per_group=0, max_connections=1)
    with pytest.raises(ValueError, match="max_connections"):
        plan([], shard_of={}, max_per_group=1, max_connections=0)
    with pytest.raises(KeyError, match="no exchange_index known for ticker X"):
        plan(["X"], shard_of={}, max_per_group=1, max_connections=1)


def test_an_empty_universe_is_an_empty_plan() -> None:
    empty = plan([], shard_of={}, max_per_group=5, max_connections=2)
    assert empty == Plan(groups=())
    assert empty.tickers == frozenset()
    assert diff(empty, empty) == ()


def test_a_fresh_plan_chunks_each_shard_in_ticker_order_and_alternates_connections() -> None:
    shard_of = {"e": 0, "a": 0, "d": 0, "b": 0, "c": 0, "z": 1}
    result = plan(shard_of.keys(), shard_of=shard_of, max_per_group=2, max_connections=2)
    assert result == make_plan(
        group("s0-g0000", ["a", "b"], conn=0),
        group("s0-g0001", ["c", "d"], conn=1),
        group("s0-g0002", ["e"], conn=0),
        group("s1-g0000", ["z"], shard=1, conn=1),
    )


def test_a_replan_keeps_members_in_place_and_fills_holes_before_opening_groups() -> None:
    previous = make_plan(
        group("s0-g0000", ["a", "b", "c"], conn=0), group("s0-g0001", ["d", "e"], conn=1)
    )
    wanted = ["a", "c", "d", "e", "f", "g", "h"]
    result = plan(
        wanted, shard_of=one_shard(wanted), max_per_group=3, max_connections=2, previous=previous
    )
    assert result == make_plan(
        group("s0-g0000", ["a", "c", "f"], conn=0),
        group("s0-g0001", ["d", "e", "g"], conn=1),
        group("s0-g0002", ["h"], conn=0),
    )


def test_a_replan_moves_a_ticker_whose_shard_changed() -> None:
    previous = make_plan(group("s0-g0000", ["a", "b"]))
    result = plan(
        ["a", "b"],
        shard_of={"a": 0, "b": 1},
        max_per_group=2,
        max_connections=1,
        previous=previous,
    )
    assert result == make_plan(group("s0-g0000", ["a"]), group("s1-g0000", ["b"], shard=1))


def test_a_replan_evicts_the_surplus_of_a_shrunken_group_in_ticker_order() -> None:
    previous = make_plan(group("s0-g0000", ["a", "b", "c"]))
    result = plan(
        ["a", "b", "c"],
        shard_of=one_shard("abc"),
        max_per_group=2,
        max_connections=1,
        previous=previous,
    )
    assert result == make_plan(group("s0-g0000", ["a", "b"]), group("s0-g0001", ["c"]))


def test_a_replan_drops_emptied_groups_and_reuses_their_ids() -> None:
    previous = make_plan(group("s0-g0000", ["a"], conn=0), group("s0-g0001", ["b"], conn=1))
    result = plan(
        ["b", "c"],
        shard_of=one_shard("bc"),
        max_per_group=1,
        max_connections=2,
        previous=previous,
    )
    assert result == make_plan(group("s0-g0000", ["c"], conn=0), group("s0-g0001", ["b"], conn=1))


def test_a_replan_reassigns_only_groups_on_connections_that_no_longer_exist() -> None:
    previous = make_plan(
        group("s0-g0000", ["a"], conn=0),
        group("s0-g0001", ["b"], conn=3),
        group("s0-g0002", ["c"], conn=1),
    )
    result = plan(
        ["a", "b", "c"],
        shard_of=one_shard("abc"),
        max_per_group=1,
        max_connections=2,
        previous=previous,
    )
    assert [kept.conn_id for kept in result.groups] == [0, 0, 1]


# ----------------------------------------------------------------------- diffs


def test_diff_orders_removals_before_additions() -> None:
    current = make_plan(group("s0-g0000", ["a", "b"], conn=0), group("s0-g0001", ["c"], conn=1))
    desired = make_plan(group("s0-g0000", ["a", "c"], conn=0), group("s0-g0002", ["b"], conn=1))
    changes = diff(current, desired)
    assert changes == (
        RemoveMarkets(group_id="s0-g0000", tickers=("b",)),
        RemoveGroup(group_id="s0-g0001"),
        AddGroup(group=group("s0-g0002", ["b"], conn=1)),
        AddMarkets(group_id="s0-g0000", tickers=("c",)),
    )
    assert apply(changes, current, max_per_group=2) == desired
    assert diff(desired, desired) == ()


def test_diff_rebuilds_groups_that_cannot_be_updated_in_place() -> None:
    current = make_plan(group("conn", ["a"]), group("members", ["b"]), group("shard", ["c"]))
    desired = make_plan(
        group("conn", ["a"], conn=1), group("members", ["d"]), group("shard", ["c"], shard=1)
    )
    changes = diff(current, desired)
    assert changes == (
        RemoveGroup(group_id="conn"),
        RemoveGroup(group_id="members"),
        RemoveGroup(group_id="shard"),
        AddGroup(group=desired.by_id["conn"]),
        AddGroup(group=desired.by_id["members"]),
        AddGroup(group=desired.by_id["shard"]),
    )
    assert apply(changes, current, max_per_group=1) == desired


# -------------------------------------------------------------------- commands


def test_to_commands_maps_each_change_onto_its_websocket_command() -> None:
    changes = (
        RemoveMarkets(group_id="kept", tickers=("b",)),
        RemoveGroup(group_id="gone"),
        AddGroup(group=group("new", ["z", "y"])),
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

universes = st.dictionaries(
    st.text(alphabet="abcdefghij", min_size=1, max_size=2), st.integers(0, 2), max_size=40
)
caps = st.integers(1, 6)
connection_counts = st.integers(1, 4)


def home_of(subject: Plan) -> dict[str, str]:
    return {ticker: kept.group_id for kept in subject.groups for ticker in kept.tickers}


def assert_well_formed(
    subject: Plan, shard_of: Mapping[str, int], *, max_per_group: int, max_connections: int
) -> None:
    assert subject.tickers == frozenset(shard_of)
    for kept in subject.groups:
        assert 1 <= len(kept.tickers) <= max_per_group
        assert {shard_of[ticker] for ticker in kept.tickers} == {kept.exchange_index}
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
def test_no_group_exceeds_its_capacity_or_mixes_shards(
    *,
    first: dict[str, int],
    second: dict[str, int],
    cap_a: int,
    cap_b: int,
    conns_a: int,
    conns_b: int,
) -> None:
    fresh = plan(first.keys(), shard_of=first, max_per_group=cap_a, max_connections=conns_a)
    assert_well_formed(fresh, first, max_per_group=cap_a, max_connections=conns_a)
    load = Counter(kept.conn_id for kept in fresh.groups)
    per_connection = [load[conn_id] for conn_id in range(conns_a)]
    assert max(per_connection) - min(per_connection) <= 1
    replanned = plan(
        second.keys(),
        shard_of=second,
        max_per_group=cap_b,
        max_connections=conns_b,
        previous=fresh,
    )
    assert_well_formed(replanned, second, max_per_group=cap_b, max_connections=conns_b)


@given(universes, universes, caps, connection_counts, st.data())
@settings(max_examples=200)
def test_plan_does_not_depend_on_input_order(
    first: dict[str, int],
    second: dict[str, int],
    cap: int,
    conns: int,
    data: st.DataObject,
) -> None:
    previous = plan(first.keys(), shard_of=first, max_per_group=cap, max_connections=conns)
    order = data.draw(st.permutations(sorted(second)))
    reordered = {ticker: second[ticker] for ticker in reversed(order)}
    for base in (None, previous):
        assert plan(
            order, shard_of=reordered, max_per_group=cap, max_connections=conns, previous=base
        ) == plan(
            sorted(second), shard_of=second, max_per_group=cap, max_connections=conns, previous=base
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
    first: dict[str, int],
    second: dict[str, int],
    cap: int,
    conns_a: int,
    conns_b: int,
    replan: bool,
) -> None:
    current = plan(first.keys(), shard_of=first, max_per_group=cap, max_connections=conns_a)
    desired = plan(
        second.keys(),
        shard_of=second,
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


@given(universes, universes, caps, caps, connection_counts)
@settings(max_examples=300)
def test_replanning_moves_only_the_tickers_that_must_move(
    first: dict[str, int], second: dict[str, int], cap_a: int, cap_b: int, conns: int
) -> None:
    current = plan(first.keys(), shard_of=first, max_per_group=cap_a, max_connections=conns)
    desired = plan(
        second.keys(),
        shard_of=second,
        max_per_group=cap_b,
        max_connections=conns,
        previous=current,
    )
    home_a, home_b = home_of(current), home_of(desired)
    staying = home_a.keys() & home_b.keys()
    moved = {ticker for ticker in staying if home_a[ticker] != home_b[ticker]}
    shard_changed = {ticker for ticker in staying if first[ticker] != second[ticker]}
    over_capacity = sum(
        max(0, sum(1 for t in kept.tickers if second.get(t) == kept.exchange_index) - cap_b)
        for kept in current.groups
    )
    assert shard_changed <= moved
    assert len(moved) == len(shard_changed) + over_capacity
    for group_id, kept in desired.by_id.items():
        before = current.by_id.get(group_id)
        if before is not None and not before.tickers.isdisjoint(kept.tickers):
            assert kept.conn_id == before.conn_id
