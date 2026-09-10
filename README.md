# Kalshi Tape

An order-book flight recorder, deterministic replay engine, and self-calibrating
market maker for [Kalshi](https://kalshi.com), with a live WebGL viewer.

> Status: recording. The exchange client (RSA-PSS signing, rate-limit
> mirror, bounded-buffer WebSocket session), the book, the tape writer, and
> the recorder orchestrator are built and tested; the viewer, simulator, and
> market maker are still ahead. The design documents in `docs/` remain the
> contract the code is built against.

## What this is

Kalshi keeps about three months of live data and exposes no historical order-book
depth. This project records every order-book change on the exchange into a
replayable *tape*, rebuilds any market's book at any second, and uses that data
for three things:

1. **A live viewer.** A Bookmap-style liquidity heatmap of any recorded market,
   streamed live or scrubbed like a video, served as a public web page.
2. **An honest simulator.** A Kalshi-exact exchange simulator whose fill model is
   calibrated against real one-cent orders, so backtests report error bars
   instead of a single flattering number.
3. **A market maker.** A log-odds quoting strategy that runs on the same
   event-sourced engine in replay, shadow, and live modes, and whose P&L is
   reconciled to the cent against the exchange's own records.

The recorded tape stays private (Kalshi's data terms). The code, methodology,
integrity metrics, and aggregated results are public.

## Documents

| Document | Purpose |
|---|---|
| [docs/PROPOSAL.md](docs/PROPOSAL.md) | Why this project, the research behind it, and the decisions taken so far |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System context, processes, data flow, deployment, failure modes |
| [docs/DATA_FORMATS.md](docs/DATA_FORMATS.md) | Kalshi wire formats, the tape segment format, Parquet tables, manifests |
| [docs/INTERFACES.md](docs/INTERFACES.md) | Module boundaries, protocols, types, error hierarchy, configuration |
| [docs/FRONTEND.md](docs/FRONTEND.md) | The live viewer: views, API contract, rendering, hosting |
| [docs/ENGINEERING_STANDARDS.md](docs/ENGINEERING_STANDARDS.md) | Code quality rules that every change must satisfy |
| [docs/TESTING.md](docs/TESTING.md) | Test strategy from property tests to production integrity metrics |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Deployment, secrets, monitoring, runbook |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Milestones with definitions of done |
| [docs/GLOSSARY.md](docs/GLOSSARY.md) | Kalshi and project vocabulary |
| [docs/adr/](docs/adr/) | Architecture decision records |

## Principles

1. Record first, interpret later. Raw bytes hit disk before any parser runs.
2. Exact arithmetic. Prices, counts, and dollars are integers in fixed units; floats never touch money.
3. Determinism. Replaying a day reproduces the same decisions, proven by hash.
4. Every claim is measured. Uptime, gap share, and book mismatch rate are published daily.
5. The recorder is sacred. Nothing else in the system may endanger it.
6. Exchange semantics are modeled, not approximated.

## Layout (planned)

```
docs/            design documents and ADRs
src/tape/        Python package (recorder, book, tape store, simulator, engine, API)
tests/           unit, property, contract, and integration tests
web/             TypeScript + WebGL2 viewer
deploy/          systemd units, Caddy config, Oracle Cloud notes
```

## License

MIT. See [LICENSE](LICENSE).
