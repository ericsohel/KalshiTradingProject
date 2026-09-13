"""The recorder's pure decisions: what to capture, how to subscribe, and where data has holes.

``universe`` chooses the markets that earn full order-book capture, ``planner`` assigns
them to order-book connections and turns plan changes into WebSocket commands (ADR 0020),
and ``gaps`` detects sequence discontinuities per subscription (docs/INTERFACES.md 8).
"""

from tape.recorder.gaps import Duplicate, FirstMessage, Gap, GapTracker, GapVerdict, Ok
from tape.recorder.planner import (
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
    split_change,
    to_commands,
)
from tape.recorder.universe import (
    ACTIVE_STATUS,
    DEFAULT_EXCHANGE_INDEX,
    REASON_BELOW_VOLUME,
    REASON_BEYOND_HORIZON,
    REASON_CLOSED,
    REASON_DUPLICATE,
    REASON_MVE,
    REASON_NOT_ACTIVE,
    REASON_OVER_CAP,
    REASONS,
    MarketSummary,
    UniverseDecision,
    UniversePolicy,
    select,
)

__all__ = [
    "ACTIVE_STATUS",
    "DEFAULT_EXCHANGE_INDEX",
    "REASONS",
    "REASON_BELOW_VOLUME",
    "REASON_BEYOND_HORIZON",
    "REASON_CLOSED",
    "REASON_DUPLICATE",
    "REASON_MVE",
    "REASON_NOT_ACTIVE",
    "REASON_OVER_CAP",
    "AddGroup",
    "AddMarkets",
    "Duplicate",
    "FirstMessage",
    "Gap",
    "GapTracker",
    "GapVerdict",
    "Group",
    "MarketSummary",
    "Ok",
    "Plan",
    "PlanChange",
    "RemoveGroup",
    "RemoveMarkets",
    "UniverseDecision",
    "UniversePolicy",
    "diff",
    "group_sort_key",
    "plan",
    "select",
    "split_change",
    "to_commands",
]
