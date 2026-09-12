# Architecture decision records

One file per decision that would be expensive to reverse. Every record has five parts:
context, alternatives considered, decision, consequences, and what would reverse it.
Status is one of proposed, accepted, superseded (by number), deprecated. Numbers are
never reused.

The alternatives section is not decoration. If you cannot name what you did not
choose and why, you have not made a decision; you have made a default.

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
| [0010](0010-small-subscription-groups.md) | Subscription groups of at most 500 markets | superseded by 0020 |
| [0011](0011-uv-monorepo-mit.md) | uv-managed monorepo with src layout, MIT license | accepted |
| [0012](0012-python-recorder-with-escape-hatch.md) | Python and asyncio for the recorder, with a measured escape hatch | accepted |
| [0013](0013-parquet-duckdb-over-database.md) | Parquet files and DuckDB instead of a database server | accepted |
| [0014](0014-fill-model-band.md) | Three fill models reported as a band, never one number | accepted |
| [0015](0015-market-making-first-strategy.md) | Passive market making as the first strategy | accepted |
| [0016](0016-client-side-rate-limit-mirror.md) | Mirror Kalshi's token buckets client-side | accepted |
| [0017](0017-tolerant-inbound-taxonomies.md) | Inbound taxonomies decode as strings; directional bits stay closed | accepted |
| [0018](0018-live-only-ticker-firehose.md) | The unfiltered ticker channel is live-only, not taped | accepted |
| [0019](0019-transport-liveness.md) | Liveness is measured at the transport, not by data traffic | accepted |
| [0020](0020-one-subscription-per-connection.md) | One market set per channel per connection | accepted |
| [0021](0021-audit-window-consistency.md) | An audit passes when the snapshot matches any book state in its request window | accepted |
| [0022](0022-sequenced-bus-with-book-refresh.md) | Live consumers recover from a sequenced bus and periodic book refreshes | accepted |
| [0023](0023-live-api-starlette-and-metadata.md) | The live API: Starlette on msgspec, recorder-published catalog, lazy public metadata | accepted |
| [0024](0024-azure-for-students-host.md) | Host on Azure for Students, and serve the viewer from the same host | accepted |
| [0025](0025-prune-raw-after-verified-bake.md) | Raw segments are pruned only after a verified bake and a retention window | accepted |
