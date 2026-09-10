"""Assign the universe to order-book connections and move between assignments cheaply.

Responsibility: decide which markets each order-book connection carries, and produce the
smallest ordered set of commands that turns the assignment the recorder is running into the
one it wants (ADR 0020, docs/ARCHITECTURE.md 7.1). Kalshi keeps one subscription per channel
per connection and merges every further ``subscribe`` into it, so a connection's markets are
one group: one ``subscribe``, then ``update_subscription`` for every membership change. The
module is pure: no clock, no socket, no state between calls.

Invariants: a connection carries at most one group; a group holds at most ``max_per_group``
tickers and is never empty; a ticker belongs to exactly one group; a plan is canonically
ordered so two plans are equal when they describe the same subscriptions; and replanning
moves a ticker only when its connection is over capacity or no longer exists, because every
move costs a resnapshot and a resnapshot is a hole in the tape. Exchange shards play no
part: they matter for collateral and order routing, not for market data.
"""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Mapping, Sequence
from typing import Final, assert_never

import msgspec

from tape.client.ws import (
    Command,
    SubscribeCommand,
    UnsubscribeCommand,
    UpdateSubscriptionCommand,
)

__all__ = [
    "AddGroup",
    "AddMarkets",
    "Group",
    "Plan",
    "PlanChange",
    "RemoveGroup",
    "RemoveMarkets",
    "diff",
    "group_sort_key",
    "plan",
    "to_commands",
]

_GROUP_ID_DIGITS: Final = 4


class Group(msgspec.Struct, frozen=True, kw_only=True):
    """The whole market set of one order-book connection.

    Attributes:
        group_id: Stable identity of the group across replans, written into segment
            headers. Not the server's ``sid``, which is connection-scoped and reassigned
            on every reconnect.
        conn_id: WebSocket connection carrying this group.
        tickers: Members. Never empty.

    Raises:
        ValueError: If the group has no members or a negative connection id.
    """

    group_id: str
    conn_id: int
    tickers: frozenset[str]

    def __post_init__(self) -> None:
        if not self.tickers:
            raise ValueError(f"group {self.group_id} has no tickers; drop it instead")
        if self.conn_id < 0:
            raise ValueError(f"group {self.group_id} has a negative conn_id {self.conn_id}")


def group_sort_key(group: Group) -> int:
    """Return the key that puts a plan's groups in canonical order.

    Args:
        group: Any group.

    Returns:
        The group's ``conn_id``, which is unique within a plan.
    """
    return group.conn_id


class Plan(msgspec.Struct, frozen=True, kw_only=True):
    """A complete assignment of markets to order-book connections.

    Groups are held in canonical order so that two plans describing the same
    subscriptions compare equal; build the tuple with ``sorted(groups,
    key=group_sort_key)``.

    Attributes:
        groups: Every group, ordered by :func:`group_sort_key`.

    Raises:
        ValueError: On groups out of canonical order, two groups on one connection, a
            duplicate group id, or a ticker in two groups.
    """

    groups: tuple[Group, ...]

    def __post_init__(self) -> None:
        conn_ids = [group_sort_key(group) for group in self.groups]
        if conn_ids != sorted(conn_ids):
            raise ValueError("plan groups must be sorted by conn_id")
        if len(set(conn_ids)) != len(conn_ids):
            raise ValueError("plan has two groups on one connection")
        if len({group.group_id for group in self.groups}) != len(self.groups):
            raise ValueError("plan has duplicate group ids")
        owner: dict[str, str] = {}
        for group in self.groups:
            for ticker in group.tickers:
                previous_owner = owner.setdefault(ticker, group.group_id)
                if previous_owner != group.group_id:
                    raise ValueError(
                        f"ticker {ticker} is in both {previous_owner} and {group.group_id}"
                    )

    @property
    def tickers(self) -> frozenset[str]:
        """Every ticker the plan subscribes to."""
        return frozenset(ticker for group in self.groups for ticker in group.tickers)

    @property
    def by_id(self) -> Mapping[str, Group]:
        """The groups keyed by ``group_id``."""
        return {group.group_id: group for group in self.groups}


class AddGroup(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """Subscribe a group that the running plan does not have."""

    group: Group


class RemoveGroup(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """Unsubscribe a group the desired plan no longer has."""

    group_id: str


class AddMarkets(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """Add markets to a group that stays subscribed."""

    group_id: str
    tickers: tuple[str, ...]


class RemoveMarkets(msgspec.Struct, frozen=True, kw_only=True, tag=True):
    """Drop markets from a group that stays subscribed."""

    group_id: str
    tickers: tuple[str, ...]


PlanChange = AddGroup | RemoveGroup | AddMarkets | RemoveMarkets
"""One step from the running plan towards the desired one."""


def _group_id(conn_id: int) -> str:
    """Name the group of a connection, for example ``g0002``; a connection has one group."""
    return f"g{conn_id:0{_GROUP_ID_DIGITS}d}"


def _carry_over(
    previous: Plan, desired: frozenset[str], cap: int, max_connections: int
) -> dict[int, list[str]]:
    """Keep every member of the previous plan that may stay on its connection.

    A member stays when it is still wanted and its connection still exists. If a connection
    is over a shrunken ``cap`` the surplus is evicted in ticker order, so the eviction is
    deterministic rather than dependent on set iteration.

    Args:
        previous: The plan currently running.
        desired: The tickers the new plan should cover.
        cap: Maximum members per connection.
        max_connections: Number of book connections available.

    Returns:
        The members kept, by connection id, for every connection that keeps any.
    """
    members: dict[int, list[str]] = {}
    for group in previous.groups:
        if group.conn_id >= max_connections:
            continue
        kept = sorted(ticker for ticker in group.tickers if ticker in desired)
        del kept[cap:]
        if kept:
            members[group.conn_id] = kept
    return members


def _fill(
    members: dict[int, list[str]], unplaced: Sequence[str], cap: int, max_connections: int
) -> None:
    """Give each unplaced ticker, in order, to the connection with the fewest members.

    Ties go to the lowest connection id, so the same inputs always produce the same plan.
    Once every connection holds ``cap`` members the remaining tickers are left out.

    Args:
        members: Members by connection id, mutated in place.
        unplaced: Tickers needing a connection, in ticker order.
        cap: Maximum members per connection.
        max_connections: Number of book connections available.
    """
    room = [
        (len(members.get(conn_id, ())), conn_id)
        for conn_id in range(max_connections)
        if len(members.get(conn_id, ())) < cap
    ]
    heapq.heapify(room)
    for ticker in unplaced:
        if not room:
            return
        load, conn_id = heapq.heappop(room)
        members.setdefault(conn_id, []).append(ticker)
        if load + 1 < cap:
            heapq.heappush(room, (load + 1, conn_id))


def plan(
    tickers: Iterable[str],
    *,
    max_per_group: int,
    max_connections: int,
    previous: Plan | None = None,
) -> Plan:
    """Assign tickers to at most ``max_connections`` connections, one group each.

    Without ``previous`` the tickers are dealt in ticker order to the least loaded
    connection, so connections differ in size by at most one. With ``previous``, every
    ticker that is still wanted stays on its connection unless that connection no longer
    exists or is over ``max_per_group``, and only the tickers without a connection are
    dealt out. That makes replanning cost the fewest resnapshots, at the price of
    connections that need not be as evenly loaded as a fresh plan.

    The plan holds at most ``max_per_group * max_connections`` tickers. When more are
    wanted, the tickers kept from ``previous`` take precedence and the rest are left out
    from the end of ticker order; the caller detects this as ``tickers - plan.tickers``.

    Args:
        tickers: The markets to subscribe to, in any order.
        max_per_group: Maximum tickers on one connection.
        max_connections: Number of book connections, identified ``0 .. max_connections - 1``.
        previous: The plan currently running, if any.

    Returns:
        The desired plan, canonically ordered.

    Raises:
        ValueError: If ``max_per_group`` or ``max_connections`` is below one.
    """
    if max_per_group < 1:
        raise ValueError(f"max_per_group must be at least 1, got {max_per_group}")
    if max_connections < 1:
        raise ValueError(f"max_connections must be at least 1, got {max_connections}")
    desired = frozenset(tickers)
    members: dict[int, list[str]] = {}
    if previous is not None:
        members = _carry_over(previous, desired, max_per_group, max_connections)
    placed = {ticker for kept in members.values() for ticker in kept}
    _fill(members, sorted(desired - placed), max_per_group, max_connections)
    return Plan(
        groups=tuple(
            Group(group_id=_group_id(conn_id), conn_id=conn_id, tickers=frozenset(kept))
            for conn_id, kept in sorted(members.items())
        )
    )


def diff(current: Plan, desired: Plan) -> tuple[PlanChange, ...]:
    """Return the changes that turn ``current`` into ``desired``.

    Removals come before additions, so that applying the changes in order never puts a
    group over ``max_per_group`` and never subscribes a ticker twice while it is still
    resting in its old group. Some groups kept by id are torn down and rebuilt rather than
    updated; see :func:`_must_rebuild`.

    Args:
        current: The plan the recorder is running.
        desired: The plan it should be running.

    Returns:
        The changes in the order they must be applied. Applying all of them to
        ``current`` yields exactly ``desired``.
    """
    running, wanted = current.by_id, desired.by_id
    drop_markets: list[PlanChange] = []
    drop_groups: list[PlanChange] = []
    add_groups: list[PlanChange] = []
    add_markets: list[PlanChange] = []
    rebuilt: set[str] = set()
    for group_id in sorted(running):
        old, new = running[group_id], wanted.get(group_id)
        if new is None or _must_rebuild(old, new):
            drop_groups.append(RemoveGroup(group_id=group_id))
            if new is not None:
                rebuilt.add(group_id)
            continue
        gone = old.tickers - new.tickers
        if gone:
            drop_markets.append(RemoveMarkets(group_id=group_id, tickers=tuple(sorted(gone))))
        added = new.tickers - old.tickers
        if added:
            add_markets.append(AddMarkets(group_id=group_id, tickers=tuple(sorted(added))))
    for group_id in sorted(wanted):
        if group_id not in running or group_id in rebuilt:
            add_groups.append(AddGroup(group=wanted[group_id]))
    return tuple(drop_markets + drop_groups + add_groups + add_markets)


def _must_rebuild(old: Group, new: Group) -> bool:
    """Decide whether a group kept by id must be unsubscribed and subscribed afresh.

    ``update_subscription`` cannot move a subscription to another connection. A group whose
    members are all replaced is rebuilt too: updating it in place would pass through a
    subscription with no markets, which Kalshi does not document, and the rebuild costs the
    same snapshots because every member is new.

    Args:
        old: The group as it is running.
        new: The group as it is wanted, with the same ``group_id``.

    Returns:
        ``True`` when ``diff`` must emit :class:`RemoveGroup` then :class:`AddGroup`.
    """
    return new.conn_id != old.conn_id or old.tickers.isdisjoint(new.tickers)


def to_commands(
    changes: Iterable[PlanChange],
    *,
    channels: Sequence[str],
    use_yes_price: bool,
    sid_of: Mapping[str, tuple[int, ...]],
) -> tuple[Command, ...]:
    """Turn plan changes into the WebSocket commands that realize them.

    Kalshi assigns one subscription id per channel, not per subscribe command: a group
    subscribed to ``orderbook_delta`` and ``trade`` owns two ``sid``s (observed on the
    demo exchange, docs/DATA_FORMATS.md 3.3). ``update_subscription`` accepts exactly one
    ``sid`` (server error 12 otherwise), so a membership change fans out to one command
    per ``sid``; ``unsubscribe`` accepts many, so removing a group is one command.

    Args:
        changes: Changes in the order :func:`diff` produced them.
        channels: Channels each new group subscribes, e.g. ``("orderbook_delta",
            "trade")``.
        use_yes_price: Put both book sides on the YES price scale (ADR 0006).
        sid_of: Every subscription id the server assigned to each group id, one per
            channel.

    Returns:
        The commands, grouped in the order of the changes; within a change, update
        commands are ordered by ascending ``sid`` so the output is deterministic.

    Raises:
        ValueError: If ``channels`` is empty; the server would answer with an error.
        KeyError: If a change other than :class:`AddGroup` names a group with no
            ``sid`` yet. A group that has never been subscribed can only be subscribed.
    """
    if not channels:
        raise ValueError("a subscription needs at least one channel")
    commands: list[Command] = []
    for change in changes:
        if isinstance(change, AddGroup):
            commands.append(
                SubscribeCommand(
                    channels=tuple(channels),
                    market_tickers=tuple(sorted(change.group.tickers)),
                    use_yes_price=use_yes_price,
                )
            )
        elif isinstance(change, RemoveGroup):
            commands.append(UnsubscribeCommand(sids=_sids(sid_of, change.group_id, "unsubscribe")))
        elif isinstance(change, AddMarkets):
            commands.extend(
                UpdateSubscriptionCommand(
                    sid=sid, action="add_markets", market_tickers=change.tickers
                )
                for sid in _sids(sid_of, change.group_id, "add_markets")
            )
        elif isinstance(change, RemoveMarkets):
            commands.extend(
                UpdateSubscriptionCommand(
                    sid=sid, action="delete_markets", market_tickers=change.tickers
                )
                for sid in _sids(sid_of, change.group_id, "delete_markets")
            )
        else:
            assert_never(change)
    return tuple(commands)


def _sids(sid_of: Mapping[str, tuple[int, ...]], group_id: str, action: str) -> tuple[int, ...]:
    """Look up every subscription id a group owns, in ascending order.

    Args:
        sid_of: Group id to the ``sid``s the server assigned, one per channel.
        group_id: Group being acted on.
        action: What was being attempted, for the error message.

    Returns:
        The group's subscription ids, sorted and de-duplicated.

    Raises:
        KeyError: If the group has no ``sid``; it has never been subscribed.
    """
    sids = tuple(sorted(set(sid_of.get(group_id, ()))))
    if not sids:
        raise KeyError(f"cannot {action}: group {group_id} has no sid yet")
    return sids
