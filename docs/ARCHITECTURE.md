# Architecture

This document describes the system as it will be built. Wire-level details live in
[DATA_FORMATS.md](DATA_FORMATS.md); module contracts live in [INTERFACES.md](INTERFACES.md);
the reasoning behind non-obvious choices lives in [adr/](adr/).

## 1. Purpose

Record every order-book change on Kalshi into a replayable tape; rebuild any market's
book at any instant; serve a live and historical viewer to the public; simulate the
exchange faithfully enough to calibrate a fill model against real fills; and run a
market-making strategy on one event-sourced engine in replay, shadow, and live modes.

## 2. System context

```
                     Kalshi (CFTC-regulated exchange)
        REST https://external-api.kalshi.com/trade-api/v2
        WS   wss://external-api-ws.kalshi.com/trade-api/ws/v2
                 |  public market data (signed session)      ^
                 |  private fills/orders (write::trade key)  |  orders (V2)
                 v                                           |
  +-------------------------------------------------------------------------+
  |  Recorder host (Mac during development, Oracle Cloud ARM in production) |
  |                                                                         |
  |   tape record  --raw frames-->  data/raw/    --bake-->  data/baked/     |
  |        |                        data/keyframes/                         |
  |        +--ZeroMQ ipc bus--> tape serve (public read-only API)           |
  |        +--ZeroMQ ipc bus--> tape engine (shadow / live strategy)        |
  |   tape replay / tape probe / tape audit (batch and calibration jobs)    |
  +-------------------------------------------------------------------------+
                 |  HTTPS (Caddy, auto TLS)          ^
                 v                                   |
        Public API  <-------------------  web/ (TypeScript + WebGL2 viewer,
                                           served from Cloudflare Pages)
```

External dependencies: Kalshi REST and WebSocket APIs (the only data source), a
Kalshi account with a read-scoped API key (required even for public WebSocket
channels), and, for the trading engine only, a second key scoped to `write::trade`
and restricted to a dedicated subaccount.

## 3. Design principles

Each principle is referenced by number from ADRs and code comments.

1. **Record first, interpret later.** Bytes are persisted before any parser runs, with
   local receive timestamps. A parser bug can never lose data. (ADR 0001)
2. **Exact arithmetic.** Prices are `int` ten-thousandths of a dollar, counts are `int`
   hundredths of a contract, money is `int` micro-dollars. Floats never touch these
   paths. (ADR 0002)
3. **Functional core, imperative shell.** Pure modules (book, fees, simulator, strategy)
   perform no I/O and depend on nothing that does. Adapters (client, recorder, store,
   gateway, API) perform I/O and depend inward. (ADR 0004)
4. **Determinism.** The engine consumes a totally ordered event stream and reads no
   clock, socket, or random source. Replaying a recorded day reproduces the same
   intents; a rolling hash proves it. (ADR 0005)
5. **Every claim is measured.** Recorder uptime, sequence-gap share, and the mismatch
   rate between the reconstructed book and independent REST snapshots are computed
   daily and published. Reports never show a single fill-model number; they show a band.
6. **The recorder is sacred.** It runs as its own process with its own credentials,
   its own memory budget, and no inbound network exposure. Nothing downstream may
   apply backpressure to it; slow consumers are dropped.
7. **Exchange semantics are modeled, not approximated.** The simulator implements
   Kalshi's actual order types, post-only cross cancellation, queue rules on amend,
   rejection after close, fee types with scheduled changes, and token-bucket rate limits.
8. **Small, typed, explicit interfaces.** Every module boundary is a `Protocol` or a
   frozen struct. No module reaches into another's internals.

## 4. Processes

All processes are entry points of one Python package (`tape`) and share one
configuration schema. They are independent OS processes so that failure domains do
not overlap.

| Process | Command | Reads | Writes | State | May fail without affecting |
|---|---|---|---|---|---|
| Recorder | `tape record` | Kalshi WS + REST | `data/raw/`, `data/keyframes/`, ZeroMQ PUB, metrics | In-memory books for subscribed markets | everything else |
| Baker | `tape bake`, then `tape prune --apply`, hourly | `data/raw/`, `data/manifests/` | `data/baked/`, `data/manifests/`; prune deletes raw segments of verified hours (ADR 0025) | none: each hour's bake is idempotent, and one lock admits one bake or prune | recorder, API |
| API | `tape serve` | ZeroMQ SUB (events, catalog, status), Kalshi's public event and series endpoints; `data/keyframes/` and `data/baked/` after M4 | HTTP/WS responses | live books, per-client subscriptions, metadata cache | recorder, baker |
| Replayer | `tape replay` | `data/` | reports | none | all |
| Prober | `tape probe` | Kalshi WS + REST (write::trade key) | `data/probes/` | resting penny orders | recorder, API |
| Engine | `tape engine` | ZeroMQ SUB or tape, private WS | orders, `data/engine/` | strategy state, positions | recorder, API |

Only the recorder and the API run continuously in v1. The engine is designed now and
implemented after the simulator is calibrated (see [ROADMAP.md](ROADMAP.md)).

The baker is a short-lived batch process started by a timer (OPERATIONS 3). It bakes one closed
hour at a time: it streams records from each segment, spills each table's rows to disk in hashed
buckets, and sorts one part file at a time, so its memory is bounded by the largest part rather than
by the hour (DATA_FORMATS 6), on a host whose memory it shares with the recorder. It writes the
hour's part files, then the day's manifest; `tape prune` then deletes raw segments only of hours
whose bake it has verified against the manifest, after the retention window. Neither touches the
current hour, keyframes, or anything the recorder is writing.

## 5. Data flow

1. **Universe.** The recorder paginates `GET /markets?status=open&limit=1000` at start and
   every five minutes, reads `GET /series?category=...` at most hourly when a group selects by
   category, and subscribes to `market_lifecycle_v2` (no ticker filter) for immediate
   created/activated/settled notifications. The L2 universe is chosen by ordered rule groups
   (ADR 0028), re-applied at every refresh: each admits the nearest events of named series, or
   the busiest events of a series category, a few markets per event, until the
   `max_l2_markets` budget is reached. `tape universe preview` shows the current choice
   without recording. The `ticker` channel
   covers the recorded markets on a live-only connection whose frames are held in memory
   and published but never taped (ADR 0018); its subscription follows every replan
   (ADR 0027).
2. **Subscriptions** (ADR 0020). Kalshi keeps one subscription per channel per
   connection: a second `subscribe` for a channel the connection already carries is
   merged into the existing `sid` and answered with `ok`. So each book connection carries
   exactly one market set, at most `group_size` markets, subscribed once for
   `orderbook_delta` and `trade` and changed afterwards with `update_subscription`. The
   planner assigns markets to connections and keeps each market on its connection across
   replans, because moving one costs a resnapshot. Markets on different exchange shards
   may share a connection. Sequence numbers are tracked per `sid`, so a gap invalidates
   at most one connection's markets.
3. **Capture.** Every inbound frame is appended, unparsed, to the current raw segment
   with `recv_mono_ns`, `recv_wall_ns`, and the connection id. Only `type`, `sid`, and
   `seq` are read on the hot path for gap detection.
4. **Book maintenance.** Frames are decoded off the hot path into typed structs and
   applied to in-memory YES-space books. Every five minutes, and at the top of every
   hour, each active book is written as a keyframe so that replay can seek without
   reading from midnight.
5. **Publication.** Decoded events are published on a ZeroMQ PUB socket
   (`ipc://`) with the market ticker as topic. Consumers (API, engine) subscribe with
   their own high-water marks; the recorder never blocks on them. (ADR 0008) Every message
   is numbered, and every `bus_refresh_s` the recorder republishes each book it holds, so a
   consumer that lost messages or started late recovers its books. (ADR 0022)
6. **Audit.** Every five minutes the recorder samples active markets and fetches
   `GET /markets/orderbooks?tickers=...` (100 per call), diffing the response against
   the local book at response time. Mismatches are written as audit records.
7. **Bake.** Hourly, the baker converts closed raw segments into Parquet tables with
   integer fixed-point columns, sorted by `(ticker, ts_ms, seq)`, and writes a manifest
   with row counts, hashes, gap epochs, and integrity metrics.
8. **Serve.** The API answers three kinds of request: live fan-out of selected
   markets over WebSocket, historical book reconstruction at an instant (keyframe plus
   deltas), and tape slices as Arrow IPC for the viewer's scrubber.
9. **Replay and simulate.** The replayer merges deltas, trades, lifecycle events, and
   timers into one ordered stream and drives the engine with a simulated exchange.

## 6. Package layout and dependency rule

```
src/tape/
  fixedpoint.py      exact codecs: PriceE4, CountE2, DollarsE6  (core)
  timeutil.py        typed timestamps and conversions           (core)
  errors.py          the TapeError hierarchy                    (core)
  events.py          market-data event structs                  (core)
  wire/              msgspec structs for every REST/WS payload   (core)
  book/              YES-space order book                       (core)
  fees/              fee model with scheduled changes           (core)
  sim/               exchange simulator and fill models         (core)
  engine/            events, intents, strategy protocol, loop   (core)
  strategies/        concrete strategies (logit market maker)   (core)
  client/            auth, rate limiter, REST, WS session        (adapter)
  segment/           raw segment writer/reader, keyframes       (adapter)
  recorder/          universe, subscriptions, capture, audit    (adapter)
  bus/               ZeroMQ publisher/subscriber                (adapter)
  bake/              raw -> Parquet, manifests                  (adapter)
  store/             catalog and queries over baked data        (adapter)
  api/               Starlette app: REST, WS fan-out, Arrow     (adapter)
  gateway/           live execution and reconciliation          (adapter)
  probe/             penny-order calibration harness            (adapter)
  cli.py             argparse entry points (stdlib)                         (shell)
  config.py          settings schema                            (shell)
```

Dependency rule: `core` modules import only the standard library, `msgspec`, and
`numpy`. `adapter` modules may import `core` and third-party I/O libraries. Leaf adapters
(`client`, `segment`, `bus`) each wrap one I/O mechanism and import no other adapter, so,
for example, the bus can never depend on the recorder or the exchange client. `shell`
modules may import anything. The API is imported only by the composition root and never imports
the recorder, whose catalog, status, and books reach it over the bus (ADR 0023). A lint check
enforces this (see
[ENGINEERING_STANDARDS.md](ENGINEERING_STANDARDS.md)).

## 7. Key runtime behaviors

### 7.1 Recorder

- **Connections.** Every connection is either *taped* or *live-only* (ADR 0018). One
  live-only connection carries the `ticker` channel for the markets of the current plan
  (ADR 0027), as one subscription reached in commands of at most `group_size` markets; its
  frames are decoded and published but never stored, and a market's latest value is kept
  only while the market is recorded. One taped control connection
  carries `market_lifecycle_v2`, which is small and essential for replay. N "book" connections carry `orderbook_delta` and
  `trade` groups. N starts at 2 (`book_connections`) and grows when a connection's message rate or the
  server's buffer-overflow error (code 25) indicates saturation. The default account
  limit is 200 connections; the recorder never exceeds a configured ceiling (default 16).
- **Authentication.** Every connection signs `timestamp + "GET" + "/trade-api/ws/v2"`
  with the read-scoped key. Keys never leave the process that loaded them.
- **Subscription options.** Orderbook subscriptions set `use_yes_price=true` so both
  sides arrive on the YES price scale. The flag value is recorded in every segment
  header. (ADR 0006)
- **Liveness** (ADR 0019). The server pings `heartbeat` every 10 seconds and the client
  library answers. The client also pings every 10 seconds and closes the connection if
  no pong arrives within 20, so a dead peer is detected regardless of market traffic,
  within about 32 seconds including the 2-second close timeout.
  Data silence is not a liveness signal on any connection: an idle book connection, a quiet
  lifecycle channel, and recorded markets whose tickers do not change overnight are all
  healthy (ADR 0027).
- **Sequence gaps.** Each sequenced channel carries `seq` per `sid`. Every gap writes a
  GAP record and emits a `GapEvent`. On an `orderbook_delta` subscription it also marks every book on that connection stale and sends `update_subscription` with `action=get_snapshot`,
  naming all of the connection's markets; stale books ignore deltas until the snapshot arrives. A gap
  on `trade` or `market_lifecycle_v2` cannot be repaired and does not corrupt a book, so
  it is recorded and nothing more. A repeated or decreasing `seq` is counted, logged, and
  answered with a resnapshot, because applying a replayed delta would silently corrupt
  the book. When a requested snapshot arrives, the sequence baseline for that `sid`
  resets, since whether a resnapshot continues or restarts `seq` is still unobserved.
- **Reconnect.** Exponential backoff with jitter, capped at 30 seconds. A reconnect
  re-subscribes from the recorder's own subscription table (not from memory of `sid`s,
  which are connection-scoped) and treats the resulting snapshots as authoritative.
- **Backpressure.** Frames enter a bounded in-memory queue drained by one writer
  thread. If the queue is full, the new record is refused and counted rather than
  blocking the socket reader, which would make the server overflow its own buffer and
  lose far more. When the queue has room again, the writer records a `writer_overflow`
  connection event with the number dropped, so the hole is visible in the tape itself
  and not only in a metric.
- **Universe churn.** Each universe refresh replans with the previous plan, so new
  markets join the connection with room and existing markets stay put; the supervisor
  applies the difference with `update_subscription` `add_markets` and `delete_markets`.
  The ticker connection receives the same difference for the whole plan, and markets that
  leave the universe are dropped from the latest-ticker table at that refresh (ADR 0027).
  An `ok` reply to a `subscribe` is handled as a merge, and a subscribe that gets no
  reply within its deadline fails the connection rather than staying pending.

### 7.2 Book

A consolidated YES-space book per market: bids (YES bids) and asks (NO bids reported
in YES-leg pricing). Levels are `price_e4 -> count_e2`. Invariants: all counts
non-negative; best bid strictly below best ask; a snapshot replaces all levels; a
delta adds to one level and removes it at zero. The structure is a pure value type
with `apply_snapshot`, `apply_delta`, `best_bid`, `best_ask`, `depth(n)`, and
`to_keyframe`. See [INTERFACES.md](INTERFACES.md).

### 7.3 API fan-out

The API holds one ZeroMQ SUB socket and a map from ticker to connected clients. Each
client may subscribe to at most 10 tickers. Outbound queues are bounded; a client
that falls behind by more than a configured number of messages receives a
`resync` message and a fresh snapshot rather than a backlog. The API's own books
recover from bus loss, and from starting after the recorder, through the sequenced
envelope and periodic refresh images (ADR 0022). The API is read-only,
unauthenticated, rate-limited per IP, and exposes no account data.

### 7.4 Engine (design only in v1)

Single-threaded event loop over a totally ordered stream of `Event`s. The strategy
implements `on_event(event, ctx) -> Sequence[Intent]`. An `ExecutionGateway` turns
intents into acknowledgements and fills, which return as events. Live, shadow, and
replay differ only in the gateway and the event source. A blake2b hash over the
sequence of emitted intents is logged hourly and must match on replay.

## 8. Deployment topology

| Environment | Where | Purpose |
|---|---|---|
| Development | Owner's Mac | Everything; also records a wider universe while it is on |
| Production | Azure for Students VM, North Central US (ADR 0024) | Recorder and API as separate systemd services; later baker and engine |
| Web | The production host, served by Caddy from the API's origin | Static viewer on the same origin as the API, so no CORS |
| TLS and ingress | Caddy on the production host, with the VM's Azure DNS name | HTTPS termination, static files, and reverse proxy to `tape serve` |
| Backups | Planned: Cloudflare R2 free tier and the Mac | Keyframes, manifests, and baked tables; raw segments only if space allows |

The production host accepts SSH on port 22 (keys only), port 80 for certificate issuance
and redirects, and port 443 for Caddy. The recorder and API bind to localhost and
`ipc://` sockets only. See [OPERATIONS.md](OPERATIONS.md) and `deploy/`.

## 9. Failure modes and responses

| Failure | Detection | Response |
|---|---|---|
| WebSocket disconnect | Missed pong, socket close | Reconnect with backoff; re-subscribe; snapshots overwrite books; gap epoch recorded |
| Sequence gap on one `sid` | `seq` discontinuity | Gap record; `get_snapshot`; books in that group stale until snapshot |
| Server buffer overflow (error 25) | Error frame | Move markets to a less loaded connection; log; never drop the subscription silently |
| REST 429 (no `Retry-After`) | Status code | Client-side token bucket sized from `GET /account/limits`; exponential backoff on the rare 429 |
| Disk full | Writer exception, free-space metric | Alert; recorder keeps running on a ring of the last N segments; bake stops |
| Recorder process death | Dead-man ping missed | systemd restart; the gap is visible in the manifest |
| Bad frame (parser exception) | Decode error off the hot path | Frame is already on disk; skipped for books; counted; sample kept for a regression test |
| Book mismatch vs REST audit | Audit diff | Audit record; if persistent for a group, force `get_snapshot` |
| API client too slow | Outbound queue depth | Drop backlog, send `resync` |
| Host sleep (development on a laptop) | Wall clock advances more than the monotonic clock between status ticks | `clock_jump` connection record written to every taped segment so the gap is attributable; connections reconnect on wake |
| Kalshi API change | Weekly spec diff CI job, changelog RSS | Issue opened; wire structs regenerated; recorder unaffected because it stores raw bytes |

## 10. Security model

- Two API keys: a read-scoped key for the recorder and, later, a `write::trade` key
  restricted to a dedicated subaccount for the engine. No `write::transfer` key exists
  for this project.
- Private keys are loaded from files outside the repository, never from environment
  variables that could leak into logs, and never logged.
- The public API is read-only and exposes only market data derived from public
  channels. Private channels (`fill`, `user_orders`) are never published on the bus
  topic space the API subscribes to.
- The production host runs no other services and accepts SSH by key only.
- Location attestation: `GET /api_keys` reports `api_key_region_expiration_ts`; a
  lapsed attestation blocks API trading in Sports, Elections, and Entertainment. The
  engine checks this before quoting those categories. The recorder is unaffected.

## 11. Non-goals for v1

- Cross-venue data (Polymarket) or execution.
- Combos, multivariate events, and perpetuals.
- Publishing the raw tape.
- Any LLM in the trading loop.
- Speed-based strategies; the design assumes home or cloud latency and avoids edges
  that require co-location.

## 12. Open questions

- Message volume is only sampled so far. A 65-second production capture on
  2026-09-10 at 02:15 ET (a quiet hour) with the unfiltered `ticker` channel plus
  order books and trades for the 50 highest-volume markets delivered 510 frames per
  second with zero gaps and zero drops. The unfiltered `ticker` channel was 99% of
  frames and 99% of bytes; order-book traffic for those 50 markets was 3.3 deltas per
  second. Raw JSON compressed 11.8x with zstd level 3, extrapolating to about 1.7 GB
  per day, almost all of it `ticker`. Peak sports hours will be several times busier,
  so the first week of recording still decides connection count and storage policy. The `ticker` channel is live-only and not written to
  the tape (ADR 0018), and covers only the recorded markets (ADR 0027).
- Resolved 2026-09-10 against the demo exchange: `seq` starts at 1 per `sid` and the
  initial `orderbook_snapshot` messages are part of the same sequence as the deltas
  that follow (five snapshots and 1,240 deltas arrived as `seq` 1 to 1,245 with no
  gaps). Whether a `get_snapshot` resync continues or restarts the sequence is still
  unobserved; the recorder treats any non-monotonic value as suspicious.
- Whether 0.01-contract orders are enabled for every account; the probe stage has a
  1-contract fallback.
