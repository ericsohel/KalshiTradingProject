# Roadmap

Milestones are ordered by dependency, not by calendar. Each has a definition of done
that is checked, not asserted. The recorder comes first because its data compounds
and cannot be recreated.

| # | Milestone | Deliverable | Definition of done |
|---|---|---|---|
| M0 | Design | This document set | Reviewed; ADRs accepted; open questions listed |
| M1 | Foundations | `fixedpoint`, `timeutil`, `wire`, `book`, `segment`, tooling, CI | Property tests green; layer and no-float checks in CI; coverage thresholds met |
| M2 | Client | `client.auth`, `client.ratelimit`, `client.rest`, `client.ws` | Contract tests pass against fakes and against the demo environment; public production endpoints verified |
| M3 | Recorder | `tape record` with universe, subscription planning, capture, gap handling, keyframes, audits, metrics | 7 consecutive days recorded on the Mac with uptime >= 99%, audit exact ratio >= 99.5%, message rate and GB/day measured |
| M4 | Bake and catalog | `tape bake`, manifests, `Catalog.book_at` | Bake is idempotent (sha256 stable); `book_at` equals the recorder's live book at keyframe instants; first manifest published |
| M5 | API and status view | `tape serve` (status, markets, book, tape, live), `web/` status page on Cloudflare Pages, Caddy on the production host | Public URL shows daily integrity numbers; production recorder running on Oracle |
| M6 | Live viewer | Heatmap, trade bubbles, depth ladder, market picker | 60 fps with 20,000 bubbles; golden-image tests; showcase markets live |
| M7 | Replay viewer | Scrubber, playback speeds, annotations, MP4 renderer (Python) | A payrolls-close and an NFL replay rendered from own tape |
| M8 | Simulator and fees | `fees`, `sim` with three fill models, `engine` core, replay CLI | Determinism test green; fee model matches published ranges; simulator passes matching-rule properties |
| M9 | Probes | `tape probe`, `QueueTracker`, calibration fit | About 300 production probes; `fee_cost` matches to the micro-dollar; realized fills within band on >= 95% |
| M10 | Study | Pre-registered fill-time and markout study | `STUDY.md` frozen before results; figures reproducible from CLI |
| M11 | Quoter | Log-odds market maker in replay, then shadow, then micro-live | Walk-forward backtest with bootstrap CIs; shadow agrees with backtest; micro-live fill ratio 0.8 to 1.2 |
| M12 | Scale (open-ended) | Size grows only under the validation ladder | Weekly exchange-reconciled report |

Prerequisites outside the code: a demo account and key (M2), a production account and
read key (M3), a $10 deposit and `write::trade` key on a dedicated subaccount (M9).
