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
Cloudflare Pages (static: index.html, JS, WASM)  --HTTPS-->  Caddy  --> tape serve (Starlette)
                                                                     |--> ZeroMQ SUB (live events from the recorder)
                                                                     |--> Catalog (keyframes + baked Parquet)
```

- **web/** is a Vite + TypeScript (strict) project. UI chrome is React 18; the renderer
  is a framework-free module (`web/src/render/`) that owns a WebGL2 context and knows
  nothing about React.
- **Data path.** Live: one WebSocket per viewer carrying the JSON messages of 4.2 for up
  to 10 tickers. Replay: `fetch` of Arrow IPC streams decoded with `apache-arrow` and
  uploaded to GPU textures in chunks. Status: plain JSON.
- **State.** A `TapeStore` per market holds a ring buffer of `(t, price, count)` columns
  and a current book. The renderer reads from the store; React reads derived
  summaries. No global mutable state outside the store.

## 4. API contract (`tape serve`)

All routes are under `/api/v1`, read-only, JSON unless stated, and versioned by path.
Integers follow [DATA_FORMATS.md](DATA_FORMATS.md) (`price_e4`, `count_e2`, `ts_ms`); every
price is a YES price, and `null` means none is known. Every type below is a msgspec struct
in `tape.api`; `web/src/api/schema.json` is generated from those structs and the
front end's TypeScript types from that file, so the two cannot drift (ADR 0023).

### 4.1 Live routes

| Route | Response |
|---|---|
| `GET /markets?limit=50` | `{"markets": [MarketRow]}`; `limit` 1 to 200; sorted by `volume_24h_e2` descending, then by ticker |
| `GET /markets/{ticker}` | `MarketDetail`; 404 with code `unknown_ticker` when the market is not recorded |
| `GET /status` | `ServiceStatus` |
| `WS /live` | the feed in 4.2 |

- **`MarketRow`**: `{ticker, event_ticker, series_ticker, title, subtitle, category,
  showcase, volume_24h_e2, close_ts, bid_e4, ask_e4, last_e4, book}`. `title` is the event
  title, `subtitle` the market's YES subtitle, and `category` the series category, each
  `null` until resolved; `close_ts` is Unix seconds or `null`; the prices come from the
  latest ticker update; `book` is `"unknown"`, `"fresh"`, or `"stale"`.
- **`MarketDetail`**: the `MarketRow` fields plus `price_ranges`, a list of
  `{start_e4, end_e4, step_e4}` or `null` until resolved, and `depth`,
  `{ts_ms, bids: [[price_e4, count_e2]], asks: [...]}` with the best 20 levels per side,
  best first, or `null` unless the book is known.
- **`ServiceStatus`**: `{recording, recorder_status_age_ms, recorder, bus, clients}`.
  `recording` is true when a recorder status arrived within two status intervals;
  `recorder` is the latest recorder status (`universe_size`, `subscribed_markets`, and
  per connection `conn_id, taped, frames, gaps, reconnects, stale_books, sink_dropped`) or
  `null`; `bus` is `{epoch, last_seq, messages, resets, missed, books_known}`; `clients`
  counts open live connections.

Errors are `{"error": {"code", "message"}}` with an appropriate status code. Live routes
set `Cache-Control: no-store`. CORS allows only the configured origins.

### 4.2 Live feed (`WS /live`)

One JSON object per text frame. The handshake is refused with 403 when the `Origin`
header is not an allowed origin, and closed with 1013 when `max_clients` connections are
open.

Client to server. A client message is at most 4 KB, and a client sends at most 10 per
second; either violation closes the connection with 1008.

| Message | Meaning |
|---|---|
| `{"op": "subscribe", "tickers": ["KX...", ...]}` | Replace the subscription set. An empty list unsubscribes from everything |

Server to client; `t` names the type.

| Message | When |
|---|---|
| `{"t": "hello", "protocol": 1, "max_tickers": 10, "bus_refresh_s": 10}` | First message on every connection |
| `{"t": "subscribed", "tickers": [...], "rejected": [{"ticker", "code"}]}` | Reply to each subscribe; `code` is `unknown_ticker` or `too_many_tickers` |
| `{"t": "snapshot", "ticker", "book": "fresh"\|"stale", "ts_ms", "bids": [[price_e4, count_e2]], "asks": [...]}` | The whole book, best first: on subscribing to a known book, when a book becomes known, and after a resync |
| `{"t": "delta", "ticker", "ts_ms", "side": "bid"\|"ask", "price_e4", "delta_e2"}` | A signed change at one level, applied to the latest snapshot |
| `{"t": "book", "ticker", "book": "fresh"\|"stale"}` | The book's freshness changed without a snapshot; a stale book receives no deltas until its next snapshot |
| `{"t": "resync", "ticker", "reason": "client_lag"\|"bus_loss"}` | Discard this market's book. A snapshot follows as soon as the server's book is known: at once after `client_lag`, within `bus_refresh_s` after `bus_loss` |
| `{"t": "trade", "ticker", "ts_ms", "price_e4", "count_e2", "taker_side": "bid"\|"ask"}` | A public trade; `bid` means the taker bought YES |
| `{"t": "ticker", "ticker", "ts_ms", "bid_e4", "ask_e4", "last_e4", "volume_e2"}` | Top of book and cumulative volume |
| `{"t": "error", "code", "message"}` | A client message the server could not accept, such as malformed JSON or an unknown `op` |

Messages about one market arrive in bus order. Trades and ticker updates are forwarded
whatever the book's state.

**Backpressure.** Each connection has a bounded queue of `client_queue_max` messages.
When it fills, the server discards what is queued and sends, for each subscribed market,
`resync` with reason `client_lag`, followed by a snapshot when the book is known. A
connection that lags three times within 60 seconds is closed with 4000 (too slow).

### 4.3 Historical routes (after M4)

| Route | Response |
|---|---|
| `GET /markets/{ticker}/book?at=<wall_ns>` | `{as_of_ns, stale, bids: [[price_e4,count_e2]], asks: [...]}` reconstructed from the nearest prior keyframe plus deltas |
| `GET /markets/{ticker}/tape?from=<ns>&to=<ns>&kind=deltas\|trades` | `application/vnd.apache.arrow.stream`; capped at 5,000,000 rows per request, with `X-Tape-Truncated: true` when capped |
| `GET /status/days?from=&to=` | per-day integrity numbers |

Closed historical windows set `Cache-Control: public, max-age=3600`.

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
