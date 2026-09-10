# Architecture decision records

One file per decision that would be expensive to reverse. Format: context, decision,
consequences. Status is one of proposed, accepted, superseded (by number), deprecated.
Numbers are never reused.

| # | Title | Status |
|---|---|---|
| [0001](0001-record-raw-frames-first.md) | Record raw frames before decoding | accepted |
| [0002](0002-integer-fixed-point.md) | Integer fixed-point for prices, counts, and dollars | accepted |
| [0003](0003-python-312-hand-rolled-client.md) | Python 3.12 with a spec-pinned, hand-rolled client | accepted |
| [0004](0004-functional-core-imperative-shell.md) | Functional core, imperative shell | accepted |
| [0005](0005-event-sourced-deterministic-engine.md) | Event-sourced engine with decision hashing | accepted |
| [0006](0006-yes-space-book-use-yes-price.md) | Consolidated YES-space book with `use_yes_price=true` | accepted |
| [0007](0007-private-tape-public-code.md) | Raw tape private; code, method, and results public | accepted |
| [0008](0008-zeromq-ipc-bus.md) | ZeroMQ PUB/SUB over ipc between recorder and consumers | accepted |
| [0009](0009-typescript-webgl-viewer-hosting.md) | TypeScript + WebGL2 viewer on Cloudflare Pages; API behind Caddy | accepted |
| [0010](0010-small-subscription-groups.md) | Subscription groups of at most 500 markets | accepted |
| [0011](0011-uv-monorepo-mit.md) | uv-managed monorepo with src layout, MIT license | accepted |
