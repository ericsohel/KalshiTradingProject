"""Contract tests: every example payload in the pinned AsyncAPI spec decodes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import msgspec
import pytest
import yaml

from tape.wire import (
    ErrorMsg,
    EventFeeUpdateMsg,
    EventLifecycleMsg,
    FillMsg,
    MarketLifecycleV2Msg,
    OrderbookDeltaMsg,
    OrderbookSnapshotMsg,
    SubscribedMsg,
    TickerMsg,
    TradeMsg,
    UserOrderMsg,
    decode_envelope,
    decode_msg,
)
from tape.wire.ws import OkMsg

SPEC = Path(__file__).resolve().parents[1] / "specs" / "asyncapi.yaml"

STRUCT_FOR_MESSAGE: dict[str, type[msgspec.Struct] | None] = {
    "orderbookSnapshot": OrderbookSnapshotMsg,
    "orderbookDelta": OrderbookDeltaMsg,
    "trade": TradeMsg,
    "ticker": TickerMsg,
    "marketLifecycleV2": MarketLifecycleV2Msg,
    "eventLifecycle": EventLifecycleMsg,
    "eventFeeUpdate": EventFeeUpdateMsg,
    "fill": FillMsg,
    "userOrder": UserOrderMsg,
    "subscribedResponse": SubscribedMsg,
    "errorResponse": ErrorMsg,
    "okResponse": OkMsg,
    "unsubscribedResponse": None,
}


def _examples() -> list[tuple[str, dict[str, Any]]]:
    spec = yaml.safe_load(SPEC.read_text())
    messages = spec["components"]["messages"]
    out: list[tuple[str, dict[str, Any]]] = []
    for name in STRUCT_FOR_MESSAGE:
        for example in messages[name].get("examples", []):
            out.append((name, example["payload"]))
    return out


@pytest.mark.parametrize(
    ("name", "payload"), _examples(), ids=lambda v: v if isinstance(v, str) else ""
)
def test_spec_example_decodes(name: str, payload: dict[str, Any]) -> None:
    env = decode_envelope(json.dumps(payload))
    assert env.type == payload["type"]
    struct_type = STRUCT_FOR_MESSAGE[name]
    if struct_type is None:
        assert "msg" not in payload
        return
    decoded = decode_msg(env, struct_type)
    assert isinstance(decoded, struct_type)


def test_spec_versions_are_the_pinned_ones() -> None:
    spec = yaml.safe_load(SPEC.read_text())
    assert spec["info"]["version"] == "2.0.0"
