# Front end: the live viewer

A public web page where anyone can watch Kalshi order books move in real time, scrub
any recorded market like a video, and see the recorder's integrity numbers. It is the
project's showcase and the only outward-facing surface, so it is held to the same
standards as the recorder.

## 1. Goals and non-goals

Goals: legible to someone who has never seen an order book; smooth at 60 fps on a
laptop; honest (gaps and stale periods are drawn, not hidden); zero secrets in the
browser; cheap to host (static site plus one small API).

Non-goals for v1: accounts, trading from the page, mobile-first layout, more than a
handful of concurrent live markets per viewer.

## 2. Views

| View | What the viewer sees | Data |
|---|---|---|
| **Live** | A heatmap for one market (time on x, YES price 0 to 1 on y, color = resting depth in log scale), best bid/ask lines, last-trade bubbles sized by count and colored by taker side, a depth ladder beside it, and a market picker ranked by 24h volume | WebSocket live feed |
| **Replay** | The same heatmap over a chosen window, with a scrubber, play/pause at 1x, 10x, 60x, and event annotations (payrolls release, kickoff) | Arrow IPC slices |
| **Status** | Uptime, gap share, audit exact-match ratio per day; message rates per channel; storage used; the current showcase list | REST |

## 3. Architecture

```
Cloudflare Pages (static: index.html, JS, WASM)  --HTTPS-->  Caddy  --> tape serve (FastAPI)
                                                                     |--> ZeroMQ SUB (live events from the recorder)
                                                                     |--> Catalog (keyframes + baked Parquet)
```

- **web/** is a Vite + TypeScript (strict) project. UI chrome is React 18; the renderer
  is a framework-free module (`web/src/render/`) that owns a WebGL2 context and knows
  nothing about React.
- **Data path.** Live: one WebSocket per viewer carrying msgspec-JSON events for up to
  10 tickers. Replay: `fetch` of Arrow IPC streams decoded with `apache-arrow` and
  uploaded to GPU textures in chunks. Status: plain JSON.
- **State.** A `TapeStore` per market holds a ring buffer of `(t, price, count)` columns
  and a current book. The renderer reads from the store; React reads derived
  summaries. No global mutable state outside the store.

## 4. API contract (`tape serve`)

All routes are under `/api/v1`, read-only, JSON unless stated, and versioned by path.
Integers follow [DATA_FORMATS.md](DATA_FORMATS.md) (`price_e4`, `count_e2`, `ts_ms`).

| Route | Response |
|---|---|
| `GET /markets?limit=50` | `[{ticker, title, event_ticker, category, exchange_index, bid_e4, ask_e4, last_e4, volume_24h_e2, close_ts, showcase: bool}]` sorted by 24h volume |
| `GET /markets/{ticker}` | metadata plus current book depth (top 20 each side) |
| `GET /markets/{ticker}/book?at=<wall_ns>` | `{as_of_ns, stale, bids: [[price_e4,count_e2]], asks: [...]}` reconstructed from the nearest prior keyframe plus deltas |
| `GET /markets/{ticker}/tape?from=<ns>&to=<ns>&kind=deltas|trades` | `application/vnd.apache.arrow.stream`; capped at 5,000,000 rows per request, with `X-Tape-Truncated: true` when capped |
| `GET /status` | latest manifest summary plus live counters `{recording: bool, connections, msgs_per_s: {...}, subscribed_markets, last_frame_age_ms}` |
| `GET /status/days?from=&to=` | per-day integrity numbers |
| `WS /live` | client sends `{"subscribe": ["TICKER", ...]}` (max 10); server sends `{"t":"snapshot",...}` then `{"t":"delta"|"trade"|"ticker",...}`; on lag `{"t":"resync"}` followed by a fresh snapshot |

A `resync` is followed by the market's snapshot as soon as the API's own book for it
is known again, which after a bus loss or an API restart takes at most one bus refresh
interval (ADR 0022).

Errors are `{error: {code, message}}` with appropriate status codes. Every response
sets `Cache-Control` (`no-store` for live, `public, max-age=3600` for closed windows).

## 5. Rendering design

- **Heatmap texture.** A 2D texture of `W` time columns by `H` price rows (`H` = 1,001
  for deci-cent grids, 101 for cent grids, chosen per market). Each column is the
  book's depth profile at that time bin. Live mode appends columns to a ring texture
  and shifts the viewport; replay mode fills columns from Arrow chunks.
- **Color.** Perceptually uniform ramp (viridis-like) over `log1p(count_e2 / 100)`,
  with a fixed scale per market session so brightness is comparable across time.
  Stale or gap intervals are drawn with a hatch overlay, never blank.
- **Overlays.** Best bid and ask as line strips; trades as instanced circles with area
  proportional to count and hue by taker side; the current time cursor; annotations.
- **Interaction.** Hover shows the exact level and count; drag on the time axis scrubs;
  keyboard: space play/pause, arrows step, 1/2/3 speed.
- **Budget.** First paint under 1.5 s on a cold cache; 60 fps with 20,000 trade
  bubbles on screen; under 50 MB of GPU memory per market.

## 6. Hosting

| Piece | Where | Cost |
|---|---|---|
| Static site | Cloudflare Pages, `*.pages.dev` hostname, built by CI on push to `main` | $0 |
| API | `tape serve` on the recorder host, bound to localhost | $0 |
| TLS and ingress | Caddy with automatic certificates; hostname from a free dynamic-DNS provider (DuckDNS) or a purchased domain (about $10 per year) | $0 to $10/yr |
| CORS | API allows only the Pages origin and localhost for development | |

The API is rate-limited per client IP (token bucket in Caddy or in the app) and
serves at most 200 concurrent live clients; beyond that it answers 503 with a retry
hint rather than degrading the recorder host.

## 7. Security and privacy

No cookies, no analytics beyond request logs, no user data. The API never exposes
account endpoints or private channels; it has no Kalshi credentials at all (it reads
the bus and the on-disk tape). Content Security Policy forbids inline scripts and
third-party origins except the API.

## 8. Standards for `web/`

TypeScript `strict` with `noUncheckedIndexedAccess`; ESLint (typescript-eslint
recommended-type-checked) and Prettier; Vitest for units; Playwright for one smoke
test per view; the renderer has golden-image tests against fixture tapes. No `any`,
no default exports, no implicit globals. See
[ENGINEERING_STANDARDS.md](ENGINEERING_STANDARDS.md) section 6.

## 9. Delivery order

1. Live view with heatmap and trade bubbles for showcase markets, served locally
   first (proves the bus, the API's live path, and the renderer).
2. Status view: live counters first, daily integrity numbers once bake exists.
3. Replay view with scrubber and annotations.
4. Depth ladder, hover inspection, keyboard controls, polish.
