"""Enforce the import layer rule from docs/ARCHITECTURE.md section 6.

Core modules may import only the standard library, ``msgspec``, ``numpy``, and other
core modules. Adapters may import core modules and any third-party library; leaf adapters,
each wrapping one I/O mechanism, import no other adapter, so the bus can never reach into
the recorder or the exchange client. The API is a process of its own: only shell modules may
import it, and it never imports the recorder, whose state reaches it only over the bus
(ADR 0023). Shell modules may import anything. Exit status 1 on any violation.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

PACKAGE = "tape"
SRC = Path(__file__).resolve().parents[1] / "src" / PACKAGE

CORE = {
    "fixedpoint",
    "timeutil",
    "errors",
    "events",
    "wire",
    "book",
    "fees",
    "sim",
    "engine",
    "strategies",
}
ADAPTER = {"client", "segment", "recorder", "bus", "bake", "store", "api", "gateway", "probe"}
LEAF_ADAPTER = {"client", "segment", "bus"}
SHELL_ONLY_ADAPTER = {"api"}
"""Adapters that only the composition root may import."""
FORBIDDEN_ADAPTER_IMPORTS = {"api": {"recorder"}}
"""Adapters that a given adapter must never import, whatever else it may."""
SHELL = {"cli", "config", "__init__", "__main__"}
CORE_THIRD_PARTY = {"msgspec", "numpy"}


def layer_of(module: str) -> str:
    """Return the layer name for a top-level submodule of the package."""
    if module in CORE:
        return "core"
    if module in ADAPTER:
        return "adapter"
    if module in SHELL:
        return "shell"
    msg = f"unclassified module {PACKAGE}.{module}; add it to scripts/check_layers.py"
    raise SystemExit(msg)


def imported_roots(tree: ast.AST) -> set[str]:
    """Collect the root name of every import in a module."""
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            roots.add(node.module.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.level > 0:
            # Relative imports stay inside the package; resolved by the caller.
            roots.add(PACKAGE)
    return roots


def package_imports(tree: ast.AST, own_module: str) -> set[str]:
    """Collect the top-level package submodules imported by a module."""
    subs: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            parts = node.module.split(".")
            if node.level == 0 and parts[0] == PACKAGE and len(parts) > 1:
                subs.add(parts[1])
            elif node.level == 1 and own_module == PACKAGE:
                subs.add(parts[0])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                if parts[0] == PACKAGE and len(parts) > 1:
                    subs.add(parts[1])
    return subs


def check_file(path: Path) -> list[str]:
    """Return violation messages for one source file."""
    rel = path.relative_to(SRC)
    top = rel.parts[0].removesuffix(".py")
    layer = layer_of(top)
    tree = ast.parse(path.read_text(), filename=str(path))
    roots = imported_roots(tree)
    problems: list[str] = []
    if layer == "core":
        for root in sorted(roots):
            if root in sys.stdlib_module_names or root in CORE_THIRD_PARTY or root == PACKAGE:
                continue
            problems.append(f"{rel}: core module imports third-party '{root}'")
        for sub in sorted(package_imports(tree, top)):
            if sub != top and layer_of(sub) != "core":
                problems.append(f"{rel}: core module imports {layer_of(sub)} module '{sub}'")
    elif layer == "adapter":
        for sub in sorted(package_imports(tree, top)):
            if sub == top:
                continue
            if layer_of(sub) == "shell":
                problems.append(f"{rel}: adapter imports shell module '{sub}'")
            elif sub in SHELL_ONLY_ADAPTER:
                problems.append(f"{rel}: adapter imports '{sub}', which only shell modules may")
            elif sub in FORBIDDEN_ADAPTER_IMPORTS.get(top, set()):
                problems.append(f"{rel}: adapter '{top}' must not import adapter '{sub}'")
            elif top in LEAF_ADAPTER and layer_of(sub) == "adapter":
                problems.append(f"{rel}: leaf adapter imports adapter module '{sub}'")
    return problems


def main() -> int:
    """Run the check over every module in the package."""
    problems: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        problems.extend(check_file(path))
    for problem in problems:
        print(problem)
    print(f"layer check: {len(problems)} violation(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
