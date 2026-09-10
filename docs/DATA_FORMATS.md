# Data formats

All formats are versioned. Reading code must accept every prior version; writing
code emits only the current version. Changing a format requires a new version number
and an ADR.

## 1. Conventions

### 1.1 Fixed-point encodings

Kalshi transmits prices and counts as decimal strings. They are converted exactly at
the boundary and stored as integers everywhere else.

| Type | Unit | Range | Python | Parquet |
|---|---|---|---|---|
| `PriceE4` | 1/10,000 dollar | 0 to 10,000 | `int` (NewType) | `int32` |
| `CountE2` | 1/100 contract | 0 to 2^62 | `int` (NewType) | `int64` |
| `DollarsE6` | 1/1,000,000 dollar (fees, balances, fill costs) | signed | `int` (NewType) | `int64` |

Rules: parsing uses `decimal.Decimal`, rejects more decimal places than the target
unit allows, and rejects out-of-range values. Formatting emits the canonical string
Kalshi accepts (`"0.5600"`, `"10.00"`). Floats are forbidden in any module that touches
these types; a lint rule enforces it.

### 1.2 Timestamps

| Field | Meaning | Source | Unit |
|---|---|---|---|
| `ts_ms` | Exchange event time | Kalshi payload | ms since Unix epoch |
| `recv_mono_ns` | Local monotonic receive time (latency and ordering) | `time.monotonic_ns()` | ns |
| `recv_wall_ns` | Local wall-clock receive time (file placement, display) | `time.time_ns()` | ns |

The production host runs `chrony`; measured offset is recorded in the daily manifest.

### 1.3 Sides

Internally the book is in **YES space**: `bid` is a YES bid, `ask` is a YES ask. A NO
bid at no-price `q` is a YES ask at `1 - q`. Orderbook subscriptions request
`use_yes_price=true`, so the feed already reports the NO side on the YES price scale;
the recorded flag value tells replay which convention a segment used. Trade direction
uses `taker_book_side` (`bid` = taker bought YES, `ask` = taker bought NO).

### 1.4 Identifiers

Market ticker strings match `^[A-Z0-9-]+$` and are the primary key everywhere.
`market_id` (UUID) is stored but not used as a key. `sid` (subscription id) and
`seq` are connection-scoped and meaningful only within one raw segment stream.

## 2. Kalshi REST formats relied upon

Base URLs: production `https://external-api.kalshi.com/trade-api/v2`, demo
`https://external-api.demo.kalshi.co/trade-api/v2`. Spec: OpenAPI 3.30.0
(`https://docs.kalshi.com/openapi.yaml`), pinned in `specs/` and diffed weekly.

### 2.1 Authentication

Headers on every authenticated request: `KALSHI-ACCESS-KEY` (key id),
`KALSHI-ACCESS-TIMESTAMP` (ms), `KALSHI-ACCESS-SIGNATURE` (base64). Signature: RSA-PSS,
SHA-256, MGF1(SHA-256), salt length equal to the digest length, over the UTF-8 bytes
of `timestamp + METHOD + path`, where `path` starts at `/trade-api/v2/...` and excludes
the query string. Public market-data endpoints answer without headers.

### 2.2 Endpoints

| Endpoint | Used for | Parameters used | Fields relied upon |
|---|---|---|---|
| `GET /exchange/status` | health gate | none | `exchange_active`, `trading_active`, `exchange_index_statuses[]` |
| `GET /markets` | universe, metadata | `status=open`, `limit=1000`, `cursor`, `min_updated_ts`, `mve_filter=exclude` | `ticker`, `event_ticker`, `status`, `exchange_index`, `open_time`, `close_time`, `volume_24h_fp`, `price_level_structure`, `price_ranges[]{start,end,step}`, `strike_type`, `floor_strike`, `cap_strike`, `can_close_early`, `rules_primary`, `rules_secondary` |
| `GET /markets/{ticker}` | settlement result | path | `status`, `result`, `settlement_value_dollars`, `settlement_ts` |
| `GET /markets/{ticker}/orderbook` | single audit | `depth=0` | `orderbook_fp.yes_dollars[]`, `orderbook_fp.no_dollars[]` as `[price, count_fp]` |
| `GET /markets/orderbooks` | batch audit | `tickers` (up to 100) | `orderbooks[]{ticker, orderbook_fp}` |
| `GET /markets/trades` | trade reconciliation | `ticker`, `min_ts`, `max_ts`, `limit=1000`, `cursor` | `trade_id`, `ticker`, `count_fp`, `yes_price_dollars`, `taker_book_side`, `created_time`, `is_block_trade` |
| `GET /series` | fee regime, category | `include_volume`, `min_updated_ts` | `ticker`, `category`, `fee_type`, `fee_multiplier`, `settlement_sources[]`, `exchange_index` |
| `GET /series/fee_changes` | scheduled fee changes | `show_historical=true` | `series_fee_change_arr[]{series_ticker, fee_type, fee_multiplier, scheduled_ts}` |
| `GET /events` | event grouping | `status=open`, `with_nested_markets`, `limit=200`, `cursor` | `event_ticker`, `series_ticker`, `mutually_exclusive`, `fee_type_override`, `fee_multiplier_override` |
| `GET /markets/candlesticks` | context before the tape starts | `market_tickers` (100), `start_ts`, `end_ts`, `period_interval` in {1,60,1440} | `end_period_ts`, `yes_bid{open,high,low,close}_dollars`, `yes_ask{...}`, `price{...}`, `volume_fp`, `open_interest_fp` |
| `GET /historical/cutoff` | live/historical split | none | `market_settled_ts`, `trades_created_ts` |
| `GET /historical/trades` | long-run priors | `min_ts`, `max_ts`, `limit`, `cursor` | as `/markets/trades` |
| `GET /account/limits` | rate-limiter sizing | none | `usage_tier`, `read{refill_rate,bucket_capacity}`, `write{...}` |
| `GET /api_keys` | attestation check | none | `api_key_region_expiration_ts`, `api_keys[]{scopes,subaccount}` |
| `POST /portfolio/events/orders` | probe and engine orders | body | see 2.3 |
| `DELETE /portfolio/events/orders/{order_id}` | cancel | path, `market_ticker` for routing | `reduced_by` |
| `DELETE /portfolio/events/orders` | cancel-all kill switch | none | count |
| `GET /portfolio/fills` | fee verification | `min_ts`, `limit` | `fill_id`, `order_id`, `ticker`, `book_side`, `count_fp`, `yes_price_dollars`, `is_taker`, `fee_cost`, `created_time` |
| `GET /portfolio/settlements` | P&L reconciliation | `min_ts` | `ticker`, `market_result`, `yes_count_fp`, `no_count_fp`, `revenue`, `fee_cost`, `settled_time` |
| `GET /portfolio/balance` | reconciliation | `exchange_index` | `balance_dollars`, `portfolio_value` |

Pagination: request without `cursor`; pass the returned `cursor` until it is empty.
Rate limits: token buckets per account, separate read and write, default cost 10 per
request, no `Retry-After` on 429. The client mirrors the buckets locally.

### 2.3 Order request (V2)

```
POST /portfolio/events/orders
{
  "ticker": "KXBTC15M-26SEP092130-00",
  "client_order_id": "0192...uuid7",
  "side": "bid",                       # bid = buy YES, ask = sell YES (= buy NO)
  "count": "0.01",                     # CountE2 string, 0.01 minimum
  "price": "0.5600",                   # PriceE4 string on the market's grid
  "time_in_force": "good_till_canceled",
  "expiration_time": 1757460000,       # seconds; only with good_till_canceled
  "post_only": true,
  "self_trade_prevention_type": "maker",
  "cancel_order_on_pause": true,
  "subaccount": 1
}
-> 201 { "order_id", "client_order_id", "fill_count", "remaining_count",
         "average_fill_price"?, "average_fee_paid"?, "ts_ms" }
```

`immediate_or_cancel` may not carry `expiration_time`. There is no market order type.
A post-only order that would cross is cancelled with reason `PostOnlyCrossCancel`.

## 3. Kalshi WebSocket protocol

URL: `wss://external-api-ws.kalshi.com/trade-api/ws/v2` (demo:
`wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2`). Spec: AsyncAPI 2.0.0
(`https://docs.kalshi.com/asyncapi.yaml`), pinned in `specs/`.

### 3.1 Handshake and keep-alive

The three signature headers are sent on the upgrade request, signing
`timestamp + "GET" + "/trade-api/ws/v2"`. Public channels still require this. The server
sends a ping frame with body `heartbeat` every 10 seconds; the client must answer with a
pong. Limits: 200 connections per user by default, 500,000 markets per session,
10,000 commands per second.

### 3.2 Commands (client to server)

```
{"id": 1, "cmd": "subscribe",
 "params": {"channels": ["orderbook_delta", "trade"],
            "market_tickers": ["...", "..."],      # omit to receive all markets where allowed
            "use_yes_price": true}}                # orderbook channel only
{"id": 2, "cmd": "update_subscription",
 "params": {"sid": 7, "action": "add_markets", "market_tickers": ["..."]}}
                                    # actions: add_markets | delete_markets | get_snapshot
{"id": 9, "cmd": "update_subscription",
 "params": {"sid": 7, "action": "get_snapshot", "market_tickers": ["..."]}}
                                    # get_snapshot names its markets too; the subscription is unchanged
{"id": 3, "cmd": "unsubscribe", "params": {"sids": [7]}}
{"id": 4, "cmd": "list_subscriptions"}
```

Responses: `{"id", "type": "subscribed", "msg": {"channel", "sid"}}` (one per channel),
`{"id", "sid", "seq", "type": "ok", "msg": {"market_tickers": [...]}}` after an update,
`{"id", "type": "unsubscribed", "sid", "seq"}`, and
`{"id"?, "sid"?, "seq"?, "type": "error", "msg": {"code": int, "msg": str}}`.

Error codes the recorder handles specially: 9 (authentication required), 25
(subscription buffer overflow), 26 (per-subscription market limit), 27 (command rate
limit). All others are logged and surfaced.

### 3.3 Message envelope

Every data message is `{"type": <channel message type>, "sid": int, "seq"?: int, "msg": {...}}`.
`seq` is present on `orderbook_snapshot`, `orderbook_delta`, `trade`,
`market_lifecycle_v2`, and `event_lifecycle`; absent on `ticker`, `fill`, and `user_order`.
Observed on the demo exchange: `seq` starts at 1 for each `sid`, snapshots and deltas
share one sequence, and one `subscribe` naming two channels yields two `subscribed`
responses carrying the same command `id` and distinct `sid`s.

### 3.4 Channel payloads used

`orderbook_snapshot` (sent once per market after subscribe or `get_snapshot`):
```
{"market_ticker", "market_id",
 "yes_dollars_fp": [["0.0800","300.00"], ...],     # absent when the side is empty
 "no_dollars_fp":  [["0.5600","146.00"], ...]}     # YES-leg prices when use_yes_price=true
```

`orderbook_delta`:
```
{"market_ticker", "market_id", "price_dollars": "0.9600", "delta_fp": "-54.00",
 "side": "yes" | "no", "client_order_id"?: str, "subaccount"?: int, "ts_ms": int}
```
A delta adds `delta_fp` (signed) to the level at `price_dollars` on `side`.
`client_order_id` is present only when the caller's own order caused the change.

`trade`:
```
{"trade_id", "market_ticker", "yes_price_dollars", "no_price_dollars", "count_fp",
 "taker_outcome_side": "yes"|"no", "taker_book_side": "bid"|"ask", "is_block_trade", "ts_ms"}
```

`ticker` (all markets when subscribed without tickers):
```
{"market_ticker", "market_id", "price_dollars", "yes_bid_dollars", "yes_ask_dollars",
 "yes_bid_size_fp", "yes_ask_size_fp", "last_trade_size_fp", "volume_fp",
 "open_interest_fp", "dollar_volume", "dollar_open_interest", "ts_ms"}
```

`market_lifecycle_v2` (`event_type` in created, activated, deactivated,
close_date_updated, determined, settled, price_level_structure_updated,
metadata_updated): `created` carries `exchange_index`, `open_ts`, `close_ts`,
`price_level_structure`, `additional_metadata{...}`; `determined` carries `result`,
`determination_ts`, `settlement_value`; `settled` carries `settled_ts`;
`price_level_structure_updated` carries `price_level_structure`, `price_ranges[]`.

`fill` (private; engine and probe only):
```
{"trade_id", "order_id", "client_order_id"?, "market_ticker", "exchange_index",
 "is_taker", "yes_price_dollars", "count_fp", "fee_cost", "outcome_side", "book_side",
 "post_position_fp", "ts_ms", "subaccount"?}
```

`user_order` (private): `order_id`, `ticker`, `status` in resting|canceled|executed,
`book_side`, `yes_price_dollars`, `fill_count_fp`, `remaining_count_fp`,
`initial_count_fp`, `taker_fees_dollars`, `maker_fees_dollars`, `client_order_id`,
`created_ts_ms`, `expiration_ts_ms`?.

## 4. Raw segment format (version 1)

Purpose: preserve every inbound frame byte-for-byte with local timestamps, cheaply,
append-only, in files small enough to lose gracefully.

Path: `data/raw/YYYY-MM-DD/HH/conn-<NN>-<UUUU>.tape.zst` where `NN` is the connection
id and `UUUU` a per-hour segment counter (a new file is started on rotation or
reconnect). Files are zstd frames (streaming compression, level 3, flushed every
second so a crash loses at most one second).

Decompressed content is a sequence of records, little-endian:

```
Header record (first record of every file)
  magic      4 bytes  "TAPE"
  version    u16      1
  hdr_len    u32      length of the JSON header that follows
  header     JSON     {"created_wall_ns", "host", "env": "prod"|"demo",
                       "conn_id", "ws_url", "use_yes_price": true,
                       "subscriptions": [{"sid", "channel", "group_id"}],
                       "software_version", "spec_versions": {"openapi", "asyncapi"}}

Data record
  kind       u8       1 = inbound text frame, 2 = outbound command, 3 = gap marker,
                      4 = connection event (open/close/error), 5 = audit result
  conn_id    u16
  recv_mono_ns u64
  recv_wall_ns u64
  len        u32
  payload    bytes    kind 1: the frame text as received (UTF-8 JSON)
                      kind 2: the command JSON as sent
                      kind 3: JSON {"sid", "expected_seq", "got_seq"}
                      kind 4: JSON {"event": "open"|"close"|"error", "detail"}
                              or {"event": "writer_overflow", "dropped": N}
                              or {"event": "clock_jump", "wall_ns_delta": N,
                                  "mono_ns_delta": N}   # host slept; gap is attributable
                      kind 5: JSON {"ticker", "levels_rest", "levels_local", "mismatched_levels",
                                    "max_abs_diff_e2"} plus "rest_levels" and
                                    "local_levels" only when mismatched, so a mismatch is
                                    diagnosable from the tape alone
```

Readers validate the magic and version, tolerate a truncated final record (crash), and
expose records as an iterator. Records are never rewritten.

The header's `subscriptions` list reflects the connection at the moment the file was
opened. A segment opens when a connection begins, before it subscribes, so that list is
usually empty; the authoritative record of what a segment carries is its `COMMAND`
records and the `subscribed` responses among its frames, which replay reads in order.

## 5. Keyframes

Path: `data/keyframes/YYYY-MM-DD/HH/MM.parquet` (every 5 minutes: `MM` in 00, 05, ...,
55). One row per level of every subscribed market's book at that instant:

| column | type | note |
|---|---|---|
| `ticker` | string (dictionary) | |
| `side` | int8 | 0 = bid, 1 = ask (YES space) |
| `price_e4` | int32 | |
| `count_e2` | int64 | |
| `as_of_recv_ns` | int64 | wall ns when the keyframe was taken |
| `last_ts_ms` | int64 | exchange time of the last applied message |
| `stale` | bool | true if the book was awaiting a snapshot |

A market with an empty book contributes one row with `side = -1` so that emptiness is
distinguishable from absence.

On shutdown the recorder writes one final keyframe, filed under the current minute
rather than the interval slot, so it never overwrites that slot's periodic keyframe.

## 6. Baked Parquet tables (version 1)

Path: `data/baked/<table>/dt=YYYY-MM-DD/hour=HH/part-<n>.parquet`, zstd, sorted by
`(ticker, ts_ms, seq)` within a file. All strings dictionary-encoded.

| table | columns |
|---|---|
| `deltas` | `ticker`, `ts_ms` i64, `recv_mono_ns` i64, `recv_wall_ns` i64, `conn_id` i16, `sid` i32, `seq` i64, `side` i8, `price_e4` i32, `delta_e2` i64, `own_client_order_id` string? |
| `snapshots` | `ticker`, `recv_wall_ns`, `sid`, `seq`, `side`, `price_e4`, `count_e2`, `reason` i8 (0 initial, 1 gap resync, 2 reconnect) |
| `trades` | `ticker`, `trade_id` string, `ts_ms`, `recv_wall_ns`, `sid`, `seq`, `price_e4`, `count_e2`, `taker_side` i8 (0 bid, 1 ask), `is_block` bool |
| `tickers` | Not produced. The unfiltered `ticker` channel is live-only (ADR 0018); top-of-book for recorded markets is derived from `deltas` and `snapshots` |
| `lifecycle` | `ticker`, `ts_s` i64, `recv_wall_ns`, `event_type` string, `payload_json` string |
| `gaps` | `conn_id`, `sid`, `recv_wall_ns`, `expected_seq`, `got_seq`, `resolved_recv_wall_ns` i64? |
| `audits` | `ticker`, `recv_wall_ns`, `levels_rest` i32, `levels_local` i32, `mismatched_levels` i32, `max_abs_diff_e2` i64 |
| `markets` | slowly changing dimension: `ticker`, `valid_from_ns`, `valid_to_ns`?, `event_ticker`, `series_ticker`, `exchange_index`, `status`, `open_ts`, `close_ts`, `price_level_structure`, `price_ranges_json`, `strike_type`, `floor_strike` f64?, `cap_strike` f64?, `rules_primary` |
| `series` | `ticker`, `valid_from_ns`, `valid_to_ns`?, `category`, `fee_type`, `fee_multiplier` (stored as `fee_multiplier_e4` i32), `settlement_sources_json` |
| `fee_changes` | `series_ticker`, `fee_type`, `fee_multiplier_e4`, `scheduled_ts` |

Strike values are the only floating-point columns; they are metadata, never money.

## 7. Manifests

One JSON document per day at `data/manifests/YYYY-MM-DD.json`, rewritten by each bake:

```
{
  "version": 1,
  "date": "2026-09-10",
  "software_version": "...",
  "segments": [{"path", "bytes", "sha256", "records", "first_recv_wall_ns", "last_recv_wall_ns"}],
  "tables": {"deltas": {"rows", "files"}, ...},
  "coverage": {"subscribed_markets_max", "rest_open_markets_max", "ratio"},
  "uptime": {"seconds_recording", "seconds_in_day", "ratio"},
  "gaps": {"count", "market_seconds_affected", "share"},
  "audits": {"books_sampled", "books_exact", "exact_ratio", "levels_mismatched"},
  "clock": {"chrony_offset_ms"}
}
```

The three integrity numbers published on the status page are `uptime.ratio`,
`gaps.share`, and `audits.exact_ratio`.

## 8. Public API formats

Defined in [FRONTEND.md](FRONTEND.md) section 4. They are derived views over the tables
above and carry the same integer encodings.

## 9. Evolution rules

- New optional fields on Kalshi payloads are ignored by decoders (`forbid_unknown_fields=False`)
  and remain available in the raw tape.
- New *values* in an inbound enumerated field decode as plain strings rather than
  failing (ADR 0017). Kalshi has already shipped a `fee_type` that its own published
  enum does not contain. Directional fields (`book_side`, `outcome_side`) are the
  exception and stay closed, because an unknown direction must not be guessed at.
- A removed or renamed Kalshi field is a breaking wire change: the weekly spec diff
  opens an issue; decoders are updated; old raw segments remain readable because the
  decoder version is chosen from the segment header's `spec_versions`.
- Tape and Parquet versions are bumped only with an ADR and a migration note.
