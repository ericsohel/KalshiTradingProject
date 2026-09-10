"""Fail if a float touches a money path (docs/ENGINEERING_STANDARDS.md section 3.1).

In the protected modules a float literal, a call or annotation naming ``float``, or a
true-division operator is a build error. Integer division (``//``) is allowed.
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = "tape"
SRC = Path(__file__).resolve().parents[1] / "src" / PACKAGE
PROTECTED = {
    "fixedpoint",
    "book",
    "fees",
    "sim",
    "engine",
    "strategies",
    "gateway",
    "probe",
    "events",
}


class FloatFinder(ast.NodeVisitor):
    """Collect float usages with line numbers."""

    def __init__(self) -> None:
        self.hits: list[tuple[int, str]] = []

    def visit_Constant(self, node: ast.Constant) -> None:
        """Flag float literals."""
        if isinstance(node.value, float):
            self.hits.append((node.lineno, f"float literal {node.value!r}"))
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        """Flag any reference to the float type."""
        if node.id == "float":
            self.hits.append((node.lineno, "reference to float"))
        self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        """Flag true division."""
        if isinstance(node.op, ast.Div):
            self.hits.append((node.lineno, "true division '/'; use '//' with explicit rounding"))
        self.generic_visit(node)


def main() -> int:
    """Run the check over the protected modules."""
    problems: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        top = path.relative_to(SRC).parts[0].removesuffix(".py")
        if top not in PROTECTED:
            continue
        finder = FloatFinder()
        finder.visit(ast.parse(path.read_text(), filename=str(path)))
        problems.extend(f"{path.relative_to(SRC)}:{line}: {what}" for line, what in finder.hits)
    for problem in problems:
        print(problem)
    print(f"no-float check: {len(problems)} violation(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
