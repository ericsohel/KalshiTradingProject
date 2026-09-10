"""Partition the universe into subscription groups and move between plans cheaply.

Responsibility: decide which markets share a ``subscribe`` command, on which
connection, and produce the smallest ordered set of commands that turns the plan the
recorder is running into the plan it wants (ADR 0010, docs/ARCHITECTURE.md 7.1). The
module is pure: no clock, no socket, no state between calls.

Invariants: a group holds markets of exactly one exchange shard, because collateral and
routing are per shard; a group holds at most ``max_per_group`` tickers and is never
empty; a ticker belongs to exactly one group; a plan is canonically ordered so two
plans are equal when they describe the same subscriptions; and replanning moves a
ticker only when it must, because every move costs a resnapshot and a resnapshot is a
hole in the tape.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Container, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
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
    """One ``subscribe`` command's worth of markets.

    Attributes:
        group_id: Stable identity of the group across replans; the recorder's
            subscription table is keyed by it. Not the server's ``sid``, which is
            connection-scoped and reassigned on every reconnect.
        exchange_index: The single exchange shard every member belongs to.
        conn_id: WebSocket connection carrying this group.
        tickers: Members. Never empty.

    Raises:
        ValueError: If the group has no members or a negative connection id.
    """

    group_id: str
    exchange_index: int
    conn_id: int
    tickers: frozenset[str]

    def __post_init__(self) -> None:
        if not self.tickers:
            raise ValueError(f"group {self.group_id} has no tickers; drop it instead")
        if self.conn_id < 0:
            raise ValueError(f"group {self.group_id} has a negative conn_id {self.conn_id}")


def group_sort_key(group: Group) -> tuple[int, str]:
    """Return the key that puts a plan's groups in canonical order.

    Args:
        group: Any group.

    Returns:
        ``(exchange_index, group_id)``, so groups of one shard sit together.
    """
    return (group.exchange_index, group.group_id)


class Plan(msgspec.Struct, frozen=True, kw_only=True):
    """A complete set of subscription groups.

    Groups are held in canonical order so that two plans describing the same
    subscriptions compare equal; build the tuple with ``sorted(groups,
    key=group_sort_key)``.

    Attributes:
        groups: Every group, ordered by :func:`group_sort_key`.

    Raises:
        ValueError: On a duplicate group id, a ticker in two groups, or groups out of
            canonical order.
    """

    groups: tuple[Group, ...]

    def __post_init__(self) -> None:
        keys = [group_sort_key(group) for group in self.groups]
        if keys != sorted(keys):
            raise ValueError("plan groups must be sorted by (exchange_index, group_id)")
        ids = {group.group_id for group in self.groups}
        if len(ids) != len(self.groups):
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


@dataclass(slots=True)
class _Draft:
    """A group under construction: shard, its connection once chosen, and its members."""

    exchange_index: int
    conn_id: int | None
    members: list[str] = field(default_factory=list)


def _shard_of_ticker(shard_of: Mapping[str, int], ticker: str) -> int:
    """Look up a ticker's exchange shard.

    Args:
        shard_of: Ticker to ``exchange_index``.
        ticker: The ticker to place.

    Returns:
        The exchange shard.

    Raises:
        KeyError: If the shard is unknown; guessing one would route a subscription to
            the wrong exchange.
    """
    try:
        return shard_of[ticker]
    except KeyError:
        raise KeyError(f"no exchange_index known for ticker {ticker}") from None


def _fresh_group_ids(shard: int, used: Container[str]) -> Iterator[str]:
    """Yield the shard's unused group ids in ascending order, for example ``s0-g0007``.

    The shard is part of the id, so an id can never be reused for a different shard and
    :func:`diff` never has to update a group across shards. ``used`` is consulted lazily,
    so ids claimed after the generator was created are skipped too.

    Args:
        shard: The exchange shard the ids are for.
        used: Group ids already taken.

    Yields:
        Group ids not in ``used`` at the moment each one is produced.
    """
    ordinal = 0
    while True:
        group_id = f"s{shard}-g{ordinal:0{_GROUP_ID_DIGITS}d}"
        ordinal += 1
        if group_id not in used:
            yield group_id


def _carry_over(
    previous: Plan, desired: frozenset[str], shards: Mapping[str, int], cap: int
) -> dict[str, _Draft]:
    """Keep every member of the previous plan that may stay where it is.

    A member is kept when it is still wanted and its shard has not changed. If the group
    is over a shrunken ``cap`` the surplus is evicted in ticker order, so the eviction is
    deterministic rather than dependent on set iteration.

    Args:
        previous: The plan currently running.
        desired: The tickers the new plan must cover.
        shards: Ticker to ``exchange_index`` for every desired ticker.
        cap: Maximum members per group.

    Returns:
        Drafts keyed by group id, empty groups already dropped.
    """
    drafts: dict[str, _Draft] = {}
    for group in previous.groups:
        members = sorted(
            ticker
            for ticker in group.tickers
            if ticker in desired and shards[ticker] == group.exchange_index
        )
        del members[cap:]
        if members:
            drafts[group.group_id] = _Draft(group.exchange_index, group.conn_id, members)
    return drafts


def _place(drafts: dict[str, _Draft], unplaced: Sequence[str], shard: int, cap: int) -> None:
    """Fill the shard's existing groups, then open new ones for what is left.

    Existing groups are filled in group-id order and new groups take the lowest free ids,
    so the same inputs always produce the same groups.

    Args:
        drafts: Drafts so far, mutated in place.
        unplaced: Tickers of this shard needing a group, in ticker order.
        shard: The exchange shard being filled.
        cap: Maximum members per group.
    """
    index = 0
    for group_id in sorted(gid for gid, draft in drafts.items() if draft.exchange_index == shard):
        if index >= len(unplaced):
            return
        draft = drafts[group_id]
        room = cap - len(draft.members)
        if room > 0:
            chunk = unplaced[index : index + room]
            draft.members.extend(chunk)
            index += len(chunk)
    fresh_ids = _fresh_group_ids(shard, drafts)
    while index < len(unplaced):
        drafts[next(fresh_ids)] = _Draft(shard, None, list(unplaced[index : index + cap]))
        index += cap


def _assign_connections(drafts: Mapping[str, _Draft], max_connections: int) -> None:
    """Give every draft without a usable connection the least loaded one.

    A group that already has a connection keeps it, because moving a group between
    connections resubscribes it and costs the same resnapshot as moving its tickers.

    Args:
        drafts: Drafts to assign, mutated in place.
        max_connections: Number of book connections available.
    """
    load: Counter[int] = Counter()
    for draft in drafts.values():
        if draft.conn_id is not None and 0 <= draft.conn_id < max_connections:
            load[draft.conn_id] += 1
        else:
            draft.conn_id = None
    for group_id in sorted(drafts):
        draft = drafts[group_id]
        if draft.conn_id is not None:
            continue
        chosen = min(range(max_connections), key=lambda conn_id: (load[conn_id], conn_id))
        draft.conn_id = chosen
        load[chosen] += 1


def plan(
    tickers: Iterable[str],
    *,
    shard_of: Mapping[str, int],
    max_per_group: int,
    max_connections: int,
    previous: Plan | None = None,
) -> Plan:
    """Partition tickers into subscription groups spread over the connections.

    Without ``previous`` the tickers of each shard are chunked in ticker order and the
    groups are spread evenly over the connections. With ``previous``, every ticker that
    is still wanted and still on the same shard stays in the group it is already in, new
    tickers fill the gaps left in existing groups before any group is opened, and
    surviving groups keep their connection. That makes replanning cost the fewest
    resnapshots, at the price of groups that need not be as evenly sized as a fresh plan.

    Args:
        tickers: The markets to subscribe to, in any order.
        shard_of: Exchange shard of every ticker.
        max_per_group: Maximum tickers per ``subscribe`` command.
        max_connections: Number of book connections to spread the groups over.
        previous: The plan currently running, if any.

    Returns:
        The desired plan, canonically ordered.

    Raises:
        ValueError: If ``max_per_group`` or ``max_connections`` is below one.
        KeyError: If a ticker's exchange shard is unknown.
    """
    if max_per_group < 1:
        raise ValueError(f"max_per_group must be at least 1, got {max_per_group}")
    if max_connections < 1:
        raise ValueError(f"max_connections must be at least 1, got {max_connections}")
    desired = frozenset(tickers)
    shards = {ticker: _shard_of_ticker(shard_of, ticker) for ticker in desired}

    drafts: dict[str, _Draft] = {}
    if previous is not None:
        drafts = _carry_over(previous, desired, shards, max_per_group)
    placed = {ticker for draft in drafts.values() for ticker in draft.members}
    unplaced: dict[int, list[str]] = {}
    for ticker in sorted(desired - placed):
        unplaced.setdefault(shards[ticker], []).append(ticker)
    for shard in sorted(unplaced):
        _place(drafts, unplaced[shard], shard, max_per_group)

    _assign_connections(drafts, max_connections)
    groups = [
        Group(
            group_id=group_id,
            exchange_index=draft.exchange_index,
            conn_id=draft.conn_id if draft.conn_id is not None else 0,
            tickers=frozenset(draft.members),
        )
        for group_id, draft in drafts.items()
    ]
    return Plan(groups=tuple(sorted(groups, key=group_sort_key)))


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

    ``update_subscription`` can move a subscription to neither another shard nor another
    connection. A group whose members are all replaced is rebuilt too: updating it in
    place would pass through a subscription with no markets, which Kalshi does not
    document, and the rebuild costs the same snapshots because every member is new.

    Args:
        old: The group as it is running.
        new: The group as it is wanted, with the same ``group_id``.

    Returns:
        ``True`` when ``diff`` must emit :class:`RemoveGroup` then :class:`AddGroup`.
    """
    return (
        new.exchange_index != old.exchange_index
        or new.conn_id != old.conn_id
        or old.tickers.isdisjoint(new.tickers)
    )


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
