"""The committed JSON Schema of the live API matches the msgspec structs it is generated from."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "gen_api_schema.py"


@pytest.fixture
def generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location("gen_api_schema", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen_api_schema"] = module
    spec.loader.exec_module(module)
    return module


def test_the_committed_schema_is_current(generator: ModuleType) -> None:
    committed = generator.SCHEMA_PATH.read_text() if generator.SCHEMA_PATH.exists() else ""
    assert committed == generator.render(), "stale: run uv run python scripts/gen_api_schema.py"


def test_check_mode_fails_on_a_stale_file_and_changes_nothing(
    generator: ModuleType,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    stale = tmp_path / "web" / "src" / "api" / "schema.json"
    monkeypatch.setattr(generator, "ROOT", tmp_path)
    monkeypatch.setattr(generator, "SCHEMA_PATH", stale)

    assert generator.main(["--check"]) == 1
    assert not stale.exists()
    assert "is stale" in capsys.readouterr().err
    assert generator.main([]) == 0
    assert generator.main(["--check"]) == 0
    assert stale.read_text() == generator.render()


def test_the_schema_names_every_rest_body_and_every_live_message(generator: ModuleType) -> None:
    document = json.loads(generator.render())
    definitions = document["$defs"]

    assert {ref["$ref"].rsplit("/", 1)[1] for ref in document["anyOf"]} == {
        "MarketsResponse",
        "MarketDetail",
        "ServiceStatus",
        "ErrorResponse",
        "ServerMessage",
        "ClientMessage",
    }
    assert sorted(definitions["ServerMessage"]["discriminator"]["mapping"]) == [
        "book",
        "delta",
        "error",
        "hello",
        "resync",
        "snapshot",
        "subscribed",
        "ticker",
        "trade",
    ]
    assert definitions["ClientMessage"] == {"$ref": "#/$defs/SubscribeRequest"}
    row = definitions["MarketRow"]["properties"]
    assert row["book"] == {"enum": ["fresh", "stale", "unknown"]}
    assert set(definitions["MarketDetail"]["required"]) == set(
        definitions["MarketRow"]["required"]
    ) | {"price_ranges", "depth"}
