# Kalshi Order-Book Flight Recorder, Replay Engine, and Self-Calibrating Market Maker

> Historical document. This is the proposal and research summary that started the
> project, kept for context. Where it conflicts with ARCHITECTURE.md, INTERFACES.md,
> FRONTEND.md, or an ADR, those documents win.

Design proposal, 2026-09-09. Execution deferred until the open questions at the end are answered.

## The idea in one paragraph

Build the instrument that does not exist for Kalshi: a 24/7 recorder that captures every order-book
change on the exchange into a replayable "tape", a deterministic replay engine and Kalshi-exact
exchange simulator on top of it, a fill model that calibrates itself against real one-cent orders,
a Bookmap-style liquidity heatmap that scrubs any market like a video, and, as the first client of
all that infrastructure, a fee- and inventory-aware market maker whose P&L is reconciled to the cent
against the exchange's own records. The strategy is gated by a validation ladder and only scales
while there is statistical evidence of edge. The infrastructure is valuable whether or not the
strategy makes money, and the data it records cannot be recreated later by anyone.

## Why this, and why now

- Kalshi clears more than $1B on weekdays ($11.86B in the week ending Sept 6, 2026) but its API
  keeps roughly three months of live data (historical cutoff 2026-07-11 on 2026-09-09) and exposes
  no historical order-book depth at all: only 1m/1h/1d candles and public trades back to 2021-06-30.
  Depth history is paid-only (DepthFeed, Lychee, Kingsets, KalshiBackTest). Open collectors are
  toy-sized (17, 3 and 1 GitHub stars). Every day of tape you record is a day nobody else can recreate.
- Every open backtester is dead (PredictionMarketBench, four episodes, last push Jan 2026),
  Polymarket-centric (homerun, AGPL) or paper-only (oracle3). No Kalshi project of any kind has an
  independently checkable track record; the most-starred bots admit live losses or ship simulated
  "99.6% win rate" numbers.
- The one strategy family with academic evidence of edge on Kalshi is passive liquidity provision:
  makers on >=50c contracts earn about +2.6% after fees (Burgi, Deng and Whelan, 2026, 46,282
  contracts); makers earn twice as much per contract in single-name markets because retail
  systematically overbets YES (Bartlett and O'Hara, Stanford, 2026, 41.6M trades). Takers pay
  ceil(0.07 x C x P x (1-P)) per contract (up to 1.75c at 50c); makers pay zero on 13,786 of
  13,949 series. Forecasting strategies, by contrast, have three public negative postmortems in
  weather and six frontier LLM agents that lost 16-31% each on Kalshi.
- Two Kalshi-specific mechanics make a genuinely calibrated fill simulator possible, which nobody
  has built: order-book deltas caused by your own orders are tagged with your client_order_id, and
  fill messages carry post_position_fp and is_taker. Combined with the 0.01-contract minimum order
  size, about 300 resting penny orders (roughly $3 at risk) give hundreds of labeled
  (queue-ahead estimate, actual fill) samples to fit the simulator against.

## What the finished thing looks like

1. A README with three honesty numbers published per day: recorder uptime, sequence-gap epochs as a
   share of market-seconds, and the mismatch rate between the reconstructed book and independent
   REST snapshots.
2. A heatmap video: the payrolls market's book hollowing out in the sixty seconds before its
   8:29 AM ET close, or an NFL fourth quarter with every trade drawn as a bubble.
3. A pre-registered study: time-to-fill for a resting order at the touch by category and price
   bucket, post-fill markouts (adverse selection), and whether the documented maker premium survives
   real queues and real fees.
4. A market maker whose weekly report ties spread capture, adverse selection, inventory, fees and
   settlement to the cent against /portfolio/fills, /portfolio/settlements and /portfolio/balance.

## Architecture

Three processes on one machine (Python 3.12, asyncio + uvloop), sharing one event-sourced core.

### Recorder
- Authenticated WebSocket to wss://external-api-ws.kalshi.com/trade-api/ws/v2 (even public channels
  require the signed handshake: sign "timestamp GET /trade-api/ws/v2" with RSA-PSS SHA-256). Uses a
  read-scoped API key; no funds needed.
- Subscribes to orderbook_delta + trade for the L2 universe in subscriptions of <=500 markets each
  (seq is tracked per subscription id, so a gap only invalidates one group), ticker with no filter
  (BBO/volume/OI for every market), market_lifecycle_v2 (no filter; drives universe membership via
  update_subscription add_markets/delete_markets), and cfbenchmarks_value_5hz + pyth_value for
  crypto reference prices.
- Every frame is written raw first: [len][recv_mono_ns][recv_wall_ns][conn_id][bytes] into hourly
  zstd segments, so a parser bug never loses data. Only sid/seq/type are parsed hot.
- Seq gap -> update_subscription get_snapshot for the affected group plus a GAP record. Answers the
  10-second "heartbeat" pings. Error codes 25/26/27 (buffer overflow, market cap, command rate)
  trigger resharding across more connections (default 200 per user).
- Every 5 minutes: GET /markets/orderbooks?tickers=... (100 per call) diffed against the local book,
  producing an AUDIT record. This substitutes for exchange checksums, which Kalshi does not send.
- Keyframes (full book) every 5 minutes per active market so replay can seek to any second in O(1),
  the way a video codec uses I-frames.

### Book and tape store
- Consolidated YES-space book per market as integer ticks: price "0.5600" -> 5600, count "12.50"
  -> 1250. Kalshi returns bids only (a YES bid at X is a NO ask at 1-X), so asks are derived.
  Fixed numpy arrays sized to the market's price_level_structure (sub-cent grids exist).
- Batch "bake" job converts raw tapes to Hive-partitioned Parquet (deltas, snapshots, keyframes,
  trades, tickers, lifecycle, gaps, meta) queryable with DuckDB and Polars. Daily manifest with
  sha256, row counts, gap epochs and integrity metrics.

### Replay engine
- Single-threaded deterministic event loop. The strategy never touches a clock or a socket; it
  consumes Events (MarketData, Lifecycle, Private, OrderAck, Timer) and emits Intents
  (place/cancel/decrease/cancel_all). Live, shadow and replay modes feed the same event stream, so
  replaying yesterday reproduces yesterday's decisions; an hourly blake2b hash over Intents proves it.

### Exchange simulator and fee engine
- Kalshi-exact order semantics: limit orders only (market orders were removed Feb 2026),
  good_till_canceled with expiration_time, immediate_or_cancel, fill_or_kill, post_only orders that
  would cross are cancelled (PostOnlyCrossCancel), amend keeps queue position only when reducing
  size, all operations rejected after close_time (MARKET_INACTIVE, including cancels), settlement at
  market_result.
- Latencies sampled from the recorder's measured send->ack distribution.
- Maker fills use a price-time queue with three explicit models: pessimistic (all cancels ahead of
  you were behind you), optimistic (all ahead), and calibrated (cancel-ahead fraction fitted from
  the penny probes). Every report prints all three; a single P&L number is never shown.
- Fees: fee_type and fee_multiplier per series from GET /series, event-level overrides from
  GET /events, scheduled changes from GET /series/fee_changes?show_historical=true applied by
  timestamp. Taker = M x 0.07 x C x P x (1-P); maker = 0 for standard series,
  M x 0.0175 x C x P x (1-P) on the 160 quadratic_with_maker_fees series, 50% of taker on combos.
  Rounded per the fee_rounding rules ($0.000001 ceiling plus a balance-precision accumulator).
  The constants come from secondary sources quoting the 7/7/2026 fee PDF, so every live fill's
  fee_cost is asserted against the model and drift raises an alert.

### Penny-probe calibration
- Rests post_only good_till_canceled orders of count "0.01" at the best bid across ~50 markets with
  a 30-minute expiration_time and self_trade_prevention_type=maker. Queue-ahead is estimated from
  the delta tagged with our client_order_id and subsequent trades at that level; the fill message's
  post_position_fp is ground truth. Cancel is free. Roughly $3 at risk for 300 probes; fallback is
  1-contract probes (~$300) if fractional counts are unavailable on the account.

### Viewer
- Panel + Bokeh + datashader heatmap: time on x, price on y, colour = log resting depth, trade
  bubbles sized by count and coloured by taker side, mid line, gap epochs hatched, annotation layer
  (BLS 8:30 release, FOMC 14:00, kickoff), scrub slider and play at 1x/10x/60x, strategy overlay.
  Headless renderer to PNG/MP4 via imageio-ffmpeg for the write-up.

### Quoter (the first strategy)
- Fair value: logit-space microprice from the consolidated book adjusted by order-flow imbalance;
  for crypto binaries, a digital-option price from the 5 Hz reference feed with realized vol.
  Pluggable FairValueSource interface (a later module could plug in an external model).
- Avellaneda-Stoikov rewritten in logit space with terminal variance q^2 p(1-p), the correct risk
  for a Bernoulli payoff, instead of sigma^2 (T-t).
- Fee floor: half-spread in ticks >= maker fee (if any) + measured 1-minute adverse-selection
  markout + 1 tick, otherwise that side is not quoted.
- Toxicity: VPIN over volume buckets classified directly by taker_outcome_side (no Lee-Ready
  needed), large-print flag (>100 contracts), one-sided-flow score -> spread multiplier, skew, or
  quote pull with cooldown. Asymmetric inventory prior in single-name markets (tolerate long NO).
- Time-to-close guard: flatten and stop quoting well before close_time.
- Orders: post_only=true, good_till_canceled with expiration_time = now + TTL (self-expiring, so a
  dead process leaves nothing resting), cancel_order_on_pause=true, client_order_id=uuid7,
  exchange_index omitted (auto-route). Size-down via decrease (keeps queue position). Batched
  create/cancel across markets. Kill switch: DELETE /portfolio/events/orders (cancel-all, 2 tokens)
  on WebSocket silence, audit mismatch, reconciliation mismatch, drawdown, or fee-model drift.
- Quote scheduler mirrors Kalshi's token buckets client-side (Basic 200 read / 100 write tokens per
  second, create 10, cancel 2, 429 returns no Retry-After), seeded from GET /account/limits and
  GET /account/endpoint_costs. After the first API order, POST /account/api_usage_level/upgrade
  moves the account to Advanced (300/300).
- Reconciler: on start and after every reconnect, diff engine state against /portfolio/orders,
  /positions, /balance; mismatch -> cancel-all and halt. REST fills no longer carry client_order_id
  (removed Mar 30, 2026) but the WebSocket fill channel does, so an order_id <-> client_order_id map
  is persisted.

## Repo layout (proposed)

```
kalshi-tape/
  pyproject.toml            # uv, Python 3.12, MIT
  src/tape/
    client/                 # RSA-PSS signing, httpx REST, websockets WS, token-bucket mirror, fixed-point codecs
    recorder/               # multi-connection WS capture, raw segments, gap/resync, REST audit, keyframes
    book/                   # integer-tick consolidated book, delta apply, snapshot, invariants
    store/                  # tape segment format, bake to Parquet, manifests, DuckDB views
    engine/                 # deterministic event loop, Event/Intent types, live|shadow|replay, decision hash
    sim/                    # SimExchange (order semantics, latency, queue models), FeeEngine
    probe/                  # penny-order calibration harness, QueueTracker, fitting
    strategies/logit_mm/    # fair value, inventory model, toxicity, fee floor, quote scheduler
    gateway/                # LiveGateway (V2 orders), reconciler, kill switch
    viewer/                 # Panel/Bokeh/datashader heatmap, renderer
    study/                  # pre-registered analyses (STUDY.md), notebooks
    ops/                    # prometheus metrics, alerts, systemd/docker-compose
  tests/                    # hypothesis property tests (book, fees, codecs), replay determinism, spec contract tests
  STUDY.md                  # pre-registered hypotheses and metrics, committed before results
```

## Build phases (solo, part-time; ~12 weeks of engineering, but the tape must start early)

| Phase | Weeks | Deliverable | Definition of done |
|---|---|---|---|
| 0 Client | 1 | Signed REST + WS client, fixed-point codecs, token-bucket mirror | Demo and production /exchange/status, /account/limits, WS handshake work; contract tests against pinned OpenAPI 3.30.0 |
| 1 Recorder | 1-2 | 24/7 recorder on a targeted universe, raw segments, audits, keyframes | 7 days of tape; audit mismatch <0.5% of sampled books; message rate and GB/day measured |
| 2 Book + bake | 3-4 | Integer book, Parquet bake, DuckDB views | Property tests pass; replaying a day twice yields identical state hashes; >=500k events/s on one core |
| 3 Simulator + fees | 5-6 | SimExchange, FeeEngine with fee_changes, three queue models | Fee engine reproduces published ranges; order-semantics tests pass on demo |
| 4 Penny probes | 6-7 | ~300 production probes, QueueTracker, fitted cancel-ahead and latency | fee_cost matches to the cent on 100% of fills; realized fills between bounds in >95% of probes |
| 5 Viewer | 8-9 | Heatmap app, MP4 renderer | Payrolls-close and NFL videos rendered from own tape |
| 6 Study | 9-10 | Pre-registered fill/markout/maker-premium study | STUDY.md frozen before results; figures reproducible from `tape study` |
| 7 Quoter | 10-12 | LogitMM in replay, then shadow, then micro-live | Walk-forward backtest with bootstrap CIs; shadow agrees with backtest on same days; micro-live fill-rate ratio 0.8-1.2 vs simulated |
| 8 Scale (open-ended) | after | Size grows only while lower bootstrap CI of per-contract P&L after fees > 0 | Weekly exchange-reconciled report; hard stops on drawdown, markout deterioration, scheduled fee changes |

Start the recorder in week 1 even if everything else is rough. It is the only asset that compounds.

## Validation ladder (no step skipped)

0. Protocol conformance on demo and production public channels; property tests; 429 never produced under the bucket mirror.
1. Two to four weeks of tape with a data-quality report.
2. Walk-forward backtest (fit week N, evaluate N+1); sanity anchor: taker markouts must reproduce the favorite-longshot pattern (sub-10c contracts losing >60% for takers). If they do not, the simulator is wrong, not the market.
3. Shadow mode on live books, zero orders sent.
4. Micro-live at 0.01-1 contracts per side; KS test on time-to-fill; fee_cost to the cent; daily reconciliation with zero unexplained discrepancy.
5. Scale under stop rules. Whelan's 33% per-contract standard deviation means thousands of contracts before a CI is tight; the dashboard shows the required-sample-size math.

## Risks and how the design answers them

| Risk | Answer |
|---|---|
| Kalshi's Data Terms of Use prohibit providing archived data sets to others without written consent | Keep the raw tape private; publish code, methodology, aggregated results and rendered figures. "Run the recorder for a week and reproduce my numbers" is the public claim. Ask Kalshi for consent only if you want a public archive. |
| Message volume is unmeasured; tens of thousands of open markets | Targeted L2 universe (a few thousand markets by 24h volume) plus unfiltered ticker for everything else; measure in week 1; shard connections by exchange_index (0 default, 1 combos, 2 crypto/commodities, 3 sports). |
| seq semantics are underspecified in the docs | Small subscription groups, defensive gap handling, gap epochs recorded and masked in analysis. |
| Fee constants come from secondary sources (the fee PDF was unreachable) | Assert every live fill's fee_cost against the model; any discrepancy becomes a regression test. |
| Maker premium may have decayed or adverse selection may dominate | The platform's deliverable is the measurement; the strategy cannot scale without evidence. A null result is publishable. |
| Rate limits with no Retry-After; Basic write is 100 tokens/s | Client-side bucket mirror; self-expiring quotes; decrease-in-place; batched requotes; upgrade to Advanced after the first API order. |
| API churn (306 changelog entries in 17 months; fields removed with about a week's notice) | Pin the OpenAPI/AsyncAPI specs; weekly CI diff and changelog RSS watch; keep the client thin (~300 lines). |
| Region attestation on API keys (api_key_region_expiration_ts, Aug 2026) and jurisdiction | Run from a permitted location; monitor key expiry; do not run the trading process from an unapproved region. |
| Process crash leaves resting orders | Self-expiring quotes, cancel_order_on_pause, cancel-all on any invariant breach, reconciler on restart. |
| Solo maintainer of a 24/7 service | Containerized deploy, dead-man ping, runbook; recorder is the only component that must not stop. |

## Kalshi facts this design relies on (verified 2026-09-09 unless noted)

- Production REST https://external-api.kalshi.com/trade-api/v2, WS wss://external-api-ws.kalshi.com/trade-api/ws/v2; demo at external-api.demo.kalshi.co. https://docs.kalshi.com/getting_started/api_environments
- Signing: RSA-PSS SHA-256 over timestamp+METHOD+path; WS handshake signs "timestamp GET /trade-api/ws/v2"; public WS channels still need the authenticated session. https://docs.kalshi.com/getting_started/quick_start_websockets
- Fixed-point strings: prices 4 dp, counts 2 dp with 0.01 minimum; integer-cent fields removed Mar/Apr 2026. https://docs.kalshi.com/getting_started/fixed_point_migration
- Orderbook is bids-only (yes_dollars/no_dollars), best bid last; batch endpoint takes 100 tickers. https://docs.kalshi.com/getting_started/orderbook_responses
- WS channels: orderbook_delta (snapshot then deltas, seq per sid, get_snapshot), trade, ticker, fill (client_order_id, post_position_fp, is_taker, fee_cost), user_orders, market_lifecycle_v2, cfbenchmarks_value_5hz, pyth_value; 10 s heartbeat pings; 200 connections/user default, 500k markets/session, 10k commands/s. https://docs.kalshi.com/asyncapi.yaml
- V2 orders at /portfolio/events/orders: limit only; side bid|ask; GTC(+expiration_time)/IOC/FOK; post_only (PostOnlyCrossCancel); self_trade_prevention_type required; amend keeps queue only when reducing; decrease endpoint; batched create/cancel; cancel-all costs 2 tokens. https://docs.kalshi.com/openapi.yaml
- Token buckets: Basic 200 read / 100 write per second, default cost 10, 2 s burst, 429 without Retry-After; Advanced 300/300 self-serve after one API order. https://docs.kalshi.com/getting_started/rate_limits
- Fees: taker ceil(0.07 x C x P x (1-P)) on standard series; 13,786 of 13,949 series charge takers only; 160 series charge makers 25% of taker; combos 50%. Formula constants from secondary sources quoting the 7/7/2026 PDF (likely, not verified). https://kalshi.com/fee-schedule and live GET /series
- Historical split: ~3-month live window, cutoff 2026-07-11; /historical/trades back to 2021-06-30; no historical depth endpoint. https://docs.kalshi.com/getting_started/historical_data
- Sharding: exchange_index 0/1/2/3; auto-routing default since Aug 27, 2026; collateral pre-allocated per shard. https://docs.kalshi.com/getting_started/exchange_sharding
- Market lifecycle: all order operations including cancels rejected after close_time. https://docs.kalshi.com/getting_started/market_lifecycle
- Official SDK kalshi_python_sync/async is Python 3.13+ and proprietary; Kalshi recommends generating your own client from the specs for production. https://docs.kalshi.com/sdks/overview
- Academic evidence: Burgi, Deng and Whelan (2026) https://www.karlwhelan.com/Papers/Kalshi.pdf ; Bartlett and O'Hara (2026) https://law.stanford.edu/2026/04/21/adverse-selection-in-prediction-markets-evidence-from-kalshi/ ; Le (2026) https://arxiv.org/html/2602.19520v2

## Runners-up considered (and why they lost)

- A de Finetti Dutch-book engine: compile mutually_exclusive events and strike ladders into
  possible-world constraints, solve a fee-exact linear program against live depth for guaranteed-
  payoff bundles, fire them as one batched fill-or-kill request, and record every lock in a
  hash-chained ledger an outsider can re-derive with a read-only key. Intellectually the most
  distinctive idea, but capacity is tiny (a live scan found ~14 barely profitable candidates; the
  realistic prize is tens of dollars a week) and it needs taker execution, which the fee dome
  punishes. Worth building later as a second strategy module on the same engine.
- A Claude-based forecasting desk for long-horizon markets that pre-registers every forecast in a
  hash-chained ledger and earns the right to trade one category at a time via an anytime-valid
  e-value test. Clever evaluation design, but all public evidence says LLM forecasters lose on
  Kalshi, the calendar (a 3-month prospective ledger) is the bottleneck, and the working edge would
  be the maker baseline anyway. Its FairValueSource could plug into the quoter later.
- A fully open L2 data lake published to R2 with a paper-replication registry. Blocked by the data
  terms and too broad (eight packages) for one person in one season.
- Weather, economic-release and cross-venue arbitrage bots: three independent negative weather
  postmortems, near-perfect calibration in Fed markets, and cross-venue gaps that are not risk-free
  because resolution criteria diverge.

## Decisions locked in on 2026-09-09 (owner's answers)

| Question | Answer | What it changes |
|---|---|---|
| Goal | Portfolio, learning, income, and research, all four | Keep the viewer and pre-registered study (portfolio/research), keep the quoter (income), and order the phases as a curriculum. Income is the least certain of the four: the documented maker premium is small and high-variance, so the validation ladder decides whether the quoter ever scales. |
| Kalshi account | None yet | Step 0 below. The recorder cannot start until a production account exists, because even public WebSocket channels require a signed session. |
| Capital | ~$50 for probes, $500 to $2,000 later is fine | Penny-probe stage and micro-live stage proceed as designed; drawdown stop set at the start of micro-live. |
| Hosting | Free if possible | Start on the owner's Mac; move the recorder to Oracle Cloud Always Free (US region) once stable; back tapes up to Cloudflare R2. See hosting table. |
| Background | Learning as we go | Each phase introduces one new concept; see learning path. |
| Time | Unlimited | Phases can overlap; the only hard ordering is recorder first. |
| Open source | Yes | Public repository from day one, MIT license (assumed; say if you prefer Apache-2.0). Tape data stays private; only code, method, figures and aggregated results are published. |
| First universe | "Aesthetic" | Interpreted as: pick what looks best. Recorder captures everything (ticker for all markets, L2 for the top few thousand by 24h volume across all shards). Viewer showcases the densest books: 15-minute BTC, live NFL games, the payrolls and CPI closes. The quoter's first live universe is chosen from the tape by evidence, biased to categories that need no location attestation (economics, finance, crypto, climate) and charge no maker fees. Correct this if "aesthetic" meant something else. |
| Viewer | The impressive option | TypeScript + WebGL2 front end (heatmap rendered on the GPU, scrub and play, annotations, strategy overlay) served by a small FastAPI backend that streams tape slices as Arrow IPC; DuckDB-WASM optional for browsing Parquet directly in the browser. A quick Python datashader PNG renderer ships in phase 2 so there are images early. |
| Later modules | Not answered | Plug-in interfaces (Strategy protocol, FairValueSource) are reserved; v1 is recorder plus quoter. The Dutch-book solver and the Claude forecasting desk are candidate v2 modules. |

## Step 0: things only the owner can do

1. Create a demo account at https://demo.kalshi.co/sign-up (mock personal details are allowed; only a real email is needed), fund it with the documented test card, and create a demo API key from account settings. Demo lets us build and test signing, WebSocket handling and the order path immediately. Demo prices are explicitly not representative, so no tape is recorded from demo.
2. Create the production account at https://kalshi.com. KYC requires legal name, date of birth, a US residential address (no PO box), SSN, and a government photo ID; 18+ in most states, 21+ in a few. This is done by the owner directly, never through a tool or assistant.
3. Create a production API key with read scope only for the recorder. Save the private key immediately; Kalshi does not store it. Later, create a second key scoped write::trade and restricted to a dedicated subaccount for the quoter. Never create a write::transfer key for this project.
4. Location attestation: GET /api_keys returns api_key_region_expiration_ts, "the unix timestamp when the account's location attestation for API key requests expires; a past value means the attestation has lapsed." Once lapsed, API keys are not valid for trading Sports, Elections and Entertainment markets. This does not affect the read-only recorder. The quoter monitors this field and refuses to quote those categories when it is lapsed.

## Hosting options (verified September 2026)

| Option | Cost | Fit |
|---|---|---|
| Owner's Mac at home, 24/7 | $0 | Best place to start. Use `caffeinate` or Energy Saver settings to prevent sleep, an external SSD for tapes, and a dead-man ping so outages are noticed. Home location is a permitted region. Risk: reboots, updates and ISP drops become tape gaps. |
| Oracle Cloud Always Free, Ampere ARM | $0 | Since June 15, 2026 the allowance is 2 OCPU / 12 GB RAM plus 200 GB block storage (halved from 4/24 without announcement). Still far more than the recorder needs. Pick a US home region at signup (it cannot be changed), expect "out of capacity" retries, and note Oracle reclaims idle instances (a recorder is never idle). A credit card is required for identity. All required Python wheels exist for aarch64 Linux. |
| Google Cloud e2-micro | $0 | 0.25 shared vCPU, 1 GB RAM, 30 GB disk. Too small for the recorder; fine for a status page. |
| DigitalOcean / Vultr / Linode small US droplet | ~$6 to $12 per month | 1 to 2 GB RAM in a US region. Simple and reliable if Oracle capacity is a problem. |
| Hetzner US (Ashburn, Hillsboro) | ~$23 per month for CPX22 | Only CPX/CCX plans in the US; the famous cheap CX/CAX plans are EU-only. Not the cheap option in the US. |
| Cloudflare R2 for tape backups | $0 for 10 GB, then ~$0.015 per GB-month, zero egress | At an estimated 0.5 to 3 GB per day compressed, the first months cost cents to a couple of dollars. Backblaze B2 is the equivalent alternative. |

Recommended path: Mac today; Oracle ARM instance in a US region once the recorder has run a week cleanly; R2 nightly sync. The trading process, when it exists, runs from a permitted US location (home or a US-region VPS) with the attestation check above.

## Learning path (one new concept per phase)

| Phase | Concept learned | Where to read first |
|---|---|---|
| Client | Request signing (RSA-PSS), HTTP clients, token buckets | Kalshi quick-start pages for authenticated requests and rate limits |
| Recorder | asyncio and WebSockets, backpressure, append-only logs, why raw-first | Kalshi WebSocket quick start and orderbook-updates pages |
| Book and bake | Fixed-point arithmetic, columnar storage (Parquet), DuckDB queries, property-based testing with hypothesis | Kalshi fixed-point migration and orderbook-responses pages |
| Simulator and fees | Event sourcing, deterministic replay, exchange matching semantics, fee math | Kalshi order-direction, market-lifecycle and fee-rounding pages |
| Penny probes | Queue position, survival analysis (Kaplan-Meier), calibration | Any introduction to limit-order-book microstructure |
| Viewer | TypeScript, WebGL2, Arrow IPC over HTTP | WebGL2 fundamentals; Apache Arrow JS |
| Study | Pre-registration, bootstrap confidence intervals, markouts and adverse selection | Whelan et al. and Bartlett and O'Hara papers cited above |
| Quoter | Avellaneda-Stoikov in log-odds space, inventory risk for binary payoffs, flow toxicity (VPIN) | Avellaneda and Stoikov (2008); Easley, Lopez de Prado and O'Hara on VPIN |

## Open questions still outstanding

- Does "aesthetic" for the first universe match the interpretation above?
- MIT or Apache-2.0?
- Should v1 reserve a place for the Dutch-book or forecasting modules, or stay strictly recorder plus quoter?

## Zero-cost path (added 2026-09-09)

Every component runs at $0 except optional trading capital.

| Item | Free version | What you give up |
|---|---|---|
| Kalshi account and API | Free to create; API access is free for verified users; no fees until a trade executes | Nothing |
| Compute | Owner's Mac 24/7, then Oracle Cloud Always Free ARM (2 OCPU / 12 GB / 200 GB) in a US region | Uptime depends on home hardware; Oracle capacity is a lottery and needs a card on file |
| Tape storage | Local SSD and Oracle's 200 GB block storage; Cloudflare R2 or Backblaze B2 10 GB free for derived data and keyframes | Full raw retention forever; use a retention policy: full L2 for showcase and quoted markets, 1-second aggregates elsewhere, zstd level 19 for cold days |
| CI, hosting for write-ups | GitHub Actions and GitHub Pages on a public repo | Nothing |
| Monitoring | healthchecks.io free tier for the dead-man ping, ntfy.sh for alerts, self-hosted Prometheus | Grafana Cloud free tier is optional |
| Historical data | Kalshi /historical endpoints and the MIT-licensed Jon-Becker parquet dataset | Nothing |
| Software | Python, uv, DuckDB, Polars, TypeScript, WebGL, ffmpeg, all free | Nothing |
| Penny probes | Skip them, or exercise the code path on demo only | The simulator reports the pessimistic and optimistic fill bands without a calibrated middle; still honest, less precise. Doing them costs the $10 minimum deposit and about $3 of risk |
| Quoter | Backtest and shadow mode are free; micro-live needs a $10 minimum ACH deposit (no fee) | Live P&L evidence. Note Kalshi pays variable interest on balances of $250 or more, so idle bankroll is not dead money |

The only irreducible non-zero costs are electricity for a machine left on, and any trading capital you choose to risk.
