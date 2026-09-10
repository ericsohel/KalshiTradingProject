# Engineering standards

Code quality is the primary deliverable of this project. These rules are enforced by
tooling wherever a tool exists and by review otherwise. A change that violates a rule
is not merged, regardless of whether it works.

## 1. Toolchain

| Concern | Tool | Setting |
|---|---|---|
| Python | 3.12, managed by `uv`; `uv.lock` committed | `requires-python = ">=3.12,<3.13"` |
| Layout | `src/` layout, single package `tape`, distribution `kalshi-tape` | |
| Lint and format | `ruff` | rule sets `E,F,W,I,N,UP,B,A,C4,SIM,PT,RET,ARG,PL,RUF,ANN,S,T20` with a documented ignore list; line length 100 |
| Types | `mypy --strict` on `src/` and `tests/` | zero errors; `# type: ignore` requires a reason code |
| Tests | `pytest`, `hypothesis`, `pytest-asyncio`, `pytest-cov` | see [TESTING.md](TESTING.md) |
| Serialization | `msgspec` for wire structs and bus payloads | no `pydantic` on hot paths |
| Async | `asyncio` with `uvloop` in production | one loop per process |
| Logging | standard library `logging`, structured fields passed through `extra`, a JSON formatter in production | no extra dependency; revisit if log processing outgrows it |
| Pre-commit | `ruff`, `ruff format`, `mypy`, a secrets scanner, and the import-layer check | runs on every commit |
| CI | GitHub Actions on every push and pull request | all gates must pass before merge |

## 2. Architecture rules

1. **Layering.** `core` modules import only the standard library, `msgspec`, and
   `numpy`. `adapter` modules may import `core` and I/O libraries; leaf adapters (`client`,
   `segment`, `bus`) import no other adapter. `shell` modules (`cli`, `config`) may import
   anything. A script in CI (`scripts/check_layers.py`)
   parses imports and fails on violations. The layer of each module is listed in
   [ARCHITECTURE.md](ARCHITECTURE.md) section 6.
2. **No global mutable state.** No module-level singletons, caches, or clients.
   Dependencies are constructed once in a composition root (`cli.py`) and passed
   explicitly. Tests construct their own.
3. **Ports and adapters.** Every I/O boundary is a `Protocol`. Core code depends on the
   protocol; adapters implement it; tests substitute fakes. A fake must satisfy the
   same contract tests as the real adapter.
4. **Time and randomness are injected.** Only `SystemClock` calls `time.*`. Only the
   composition root constructs it. Strategies and simulators never see it.
5. **Single responsibility per module.** A module's docstring states its
   responsibility and its invariants in one paragraph. If you cannot write that
   paragraph, the module is wrong.
6. **Small public surfaces.** `__all__` is explicit. Anything not in `__all__` is
   private and may change without notice.

## 3. Correctness rules

1. **Money and quantities are integers.** `PriceE4`, `CountE2`, `DollarsE6`. `float` is
   forbidden in `fixedpoint`, `book`, `fees`, `sim`, `engine`, `strategies`, `gateway`,
   and `probe`; a ruff custom rule (`scripts/check_no_float.py`) fails the build if a
   float literal, `float(` call, or `/` operator appears there. Division uses `//`
   with explicit rounding helpers.
2. **Parse, don't validate.** Raw strings become typed values exactly once, at the
   boundary. Downstream code never re-parses.
3. **Immutability by default.** Structs and dataclasses are frozen. The only
   deliberately mutable types are `Book`, queues, and adapters' connection state.
4. **Total functions.** Every function handles every input it can be called with,
   or raises a specific `TapeError` subclass. No `return None` to signal failure.
   `Optional` returns are reserved for genuinely absent values (an empty book has no
   best bid).
5. **Invariants are asserted.** Book invariants, fixed-point ranges, and sequence
   monotonicity are checked in code, not only in tests. Assertion failures in
   production are logged with full context and convert to a stale-and-resync path,
   never to silent continuation.
6. **Timeouts and bounds everywhere.** Every await on I/O has a timeout. Every queue
   is bounded. Every retry loop has a maximum. Every pagination loop has a page cap.
7. **Idempotence.** Bake, manifest writing, and keyframe writing are idempotent per
   input; rerunning them produces byte-identical output.
8. **Determinism.** Given the same tape and configuration, replay produces the same
   intents (proved by hash) and bake produces the same files (proved by sha256).

## 4. Style rules

- Names say what a thing is, in the domain's vocabulary ([GLOSSARY.md](GLOSSARY.md)):
  `best_bid`, `taker_side`, `recv_mono_ns`, not `bb`, `ts`, `t`.
- Functions are short enough to read without scrolling; a function that needs a
  comment explaining a block should be two functions.
- Docstrings (Google style) on every public class and function state purpose,
  arguments, return, raised errors, and invariants. No docstrings that restate the
  signature.
- Comments explain *why*, never *what*. A comment that references a Kalshi behavior
  links to the doc page or changelog entry.
- `print` is forbidden outside `cli.py`. Logging uses structured key-value pairs,
  never f-strings with embedded data.
- Type annotations everywhere, including tests. `Any` requires a comment.
- No commented-out code, no TODOs without an issue link, no dead code.

## 5. Change process

1. **Branches.** `main` is protected; work happens on short-lived branches named
   `<area>/<short-description>` and lands by pull request with CI green.
2. **Commits.** Conventional Commits (`feat:`, `fix:`, `docs:`, `test:`, `refactor:`,
   `chore:`, `perf:`, `build:`, `ci:`) with a scope when useful (`feat(recorder): ...`).
   The subject is imperative and under 72 characters; the body explains why. One
   logical change per commit.
3. **Pull request checklist** (copied into the PR template):
   - [ ] The change matches [INTERFACES.md](INTERFACES.md), or that document is updated in the same PR.
   - [ ] New behavior has a test that fails without the change.
   - [ ] No float touches money. No global state. No new dependency without an ADR note.
   - [ ] Errors are specific and documented. Timeouts and bounds are present.
   - [ ] Logs have context and no secrets.
   - [ ] Docs, CHANGELOG, and configuration reference are updated.
4. **ADRs.** Any decision that would be expensive to reverse (format, dependency,
   protocol, hosting) gets an ADR in `docs/adr/` before the code lands. Every ADR
   lists the alternatives considered, the cost of the chosen option, and the
   conditions that would reverse it. A milestone does not start until its
   decisions are recorded this way.
5. **Versioning.** Semantic versioning for the package; the tape and Parquet format
   versions are independent integers recorded in headers and manifests.
6. **Dependencies.** Pinned by `uv.lock`; reviewed monthly; each direct dependency
   is justified in `pyproject.toml` comments.

## 6. Web (`web/`) standards

- TypeScript `strict`, `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`.
- ESLint with `typescript-eslint` recommended-type-checked, `eslint-plugin-react-hooks`;
  Prettier for formatting. Both run in pre-commit and CI.
- Named exports only; no `any`; no non-null assertions without a comment.
- The renderer module has no React imports and no DOM access beyond its canvas.
- Every API type, REST and WebSocket alike, is generated: `scripts/gen_api_schema.py`
  emits JSON Schema from the msgspec structs, `json-schema-to-typescript` turns it into
  TypeScript, and CI fails when either generated file drifts (ADR 0023).
- Vitest for pure modules, Playwright for one smoke test per view, golden-image tests
  for the renderer with a tolerance threshold.

## 7. Definition of done for a module

A module is done when: its docstring states responsibility and invariants; every
public function is typed and documented; unit and property tests cover the invariants;
the contract tests for its protocol pass against the real adapter and the fake; ruff
and mypy are clean; coverage meets the threshold in [TESTING.md](TESTING.md); and the
relevant design document matches the implementation.
