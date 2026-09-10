"""The layer and no-float checks must pass on the package itself."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _run(name: str) -> int:
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    result: int = module.main()
    return result


def test_layer_rule_holds() -> None:
    assert _run("check_layers") == 0


def test_no_float_rule_holds() -> None:
    assert _run("check_no_float") == 0
