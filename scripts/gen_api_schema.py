"""Write the JSON Schema of every live API type to web/src/api/schema.json (ADR 0023).

The front end's TypeScript types are generated from that file, so it is committed and must match
the msgspec structs in ``tape.api.contract``. ``--check`` writes nothing and exits 1 when the
committed file differs. The output is deterministic: keys sorted, two-space indentation, and one
trailing newline.
"""

from __future__ import annotations

import argparse
import inspect
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Final

import msgspec

from tape.api import contract

ROOT: Final = Path(__file__).resolve().parents[1]
SCHEMA_PATH: Final = ROOT / "web" / "src" / "api" / "schema.json"
SCHEMA_DIALECT: Final = "https://json-schema.org/draft/2020-12/schema"
REF_TEMPLATE: Final = "#/$defs/{name}"

ROOT_TYPES: Final[dict[str, Any]] = {  # Any: structs and unions of structs
    "MarketsResponse": contract.MarketsResponse,
    "MarketDetail": contract.MarketDetail,
    "ServiceStatus": contract.ServiceStatus,
    "ErrorResponse": contract.ErrorResponse,
    "ServerMessage": contract.ServerMessage,
    "ClientMessage": contract.ClientMessage,
}
"""Every REST response body, and every message in each direction of the live feed, by name."""


def render() -> str:
    """Render the schema document.

    Returns:
        The JSON text: each root type and every struct it uses under ``$defs``, and the roots
        listed in ``anyOf``.
    """
    schemas, components = msgspec.json.schema_components(
        ROOT_TYPES.values(), ref_template=REF_TEMPLATE
    )
    definitions: dict[str, Any] = {  # Any: JSON Schema values
        name: _dedented(component) for name, component in components.items()
    }
    for name, schema in zip(ROOT_TYPES, schemas, strict=True):
        if schema != {"$ref": REF_TEMPLATE.format(name=name)}:
            # A union, or an alias of another struct, has no component of its own name.
            definitions[name] = schema
    document = {
        "$schema": SCHEMA_DIALECT,
        "title": "tape live API",
        "description": (
            "Every REST response body and live feed message of tape serve (docs/FRONTEND.md 4). "
            "Generated from tape.api.contract by scripts/gen_api_schema.py; do not edit."
        ),
        "anyOf": [{"$ref": REF_TEMPLATE.format(name=name)} for name in ROOT_TYPES],
        "$defs": definitions,
    }
    encoded = msgspec.json.encode(document, order="sorted")
    return msgspec.json.format(encoded, indent=2).decode() + "\n"


def _dedented(component: dict[str, Any]) -> dict[str, Any]:  # Any: JSON Schema values
    """A component with its docstring description dedented, as generated comments show it."""
    description = component.get("description")
    if description is None:
        return component
    return {**component, "description": inspect.cleandoc(description)}


def main(argv: Sequence[str] | None = None) -> int:
    """Write or check the schema file.

    Args:
        argv: Arguments after the program name; ``None`` reads ``sys.argv``.

    Returns:
        0 when the file was written or is current, 1 when ``--check`` found it stale.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--check", action="store_true", help="write nothing; exit 1 if the file is stale"
    )
    args = parser.parse_args(argv)
    expected = render()
    shown = SCHEMA_PATH.relative_to(ROOT)
    if args.check:
        current = SCHEMA_PATH.read_text() if SCHEMA_PATH.exists() else ""
        if current != expected:
            print(
                f"{shown} is stale; run: uv run python scripts/gen_api_schema.py", file=sys.stderr
            )
            return 1
        print(f"{shown} is current")
        return 0
    SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
    SCHEMA_PATH.write_text(expected)
    print(f"wrote {shown}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
