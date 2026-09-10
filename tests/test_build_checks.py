"""The layer and no-float checks must pass on the package itself."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _run(name: str) -> int:
    result: int = _load(name).main()
    return result


def test_layer_rule_holds() -> None:
    assert _run("check_layers") == 0


def test_no_float_rule_holds() -> None:
    assert _run("check_no_float") == 0


def test_a_leaf_adapter_importing_another_adapter_is_a_violation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layers = _load("check_layers")
    monkeypatch.setattr(layers, "SRC", tmp_path)
    for package, source in (
        ("bus", "from tape.book import Book\nfrom tape.recorder.recorder import Recorder\n"),
        ("recorder", "from tape.bus import LiveBooks\nfrom tape.client.ws import WsSession\n"),
    ):
        (tmp_path / package).mkdir()
        (tmp_path / package / "module.py").write_text(source)

    assert layers.check_file(tmp_path / "bus" / "module.py") == [
        "bus/module.py: leaf adapter imports adapter module 'recorder'"
    ]
    assert layers.check_file(tmp_path / "recorder" / "module.py") == []
