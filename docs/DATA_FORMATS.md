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

Market ticker strings match `^[A-Z0-9][A-Z0-9.-]*$` and are the primary key everywhere: upper-case
letters, digits, and hyphens, plus the dots of fractional strike values such as
`KXAAAGASD-26SEP11-4.2700` or `KX10YRDIRHM-26SEP30H-T4.85`. Every recorded ticker observed so far
starts with a letter.
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
| `GET /series` | fee regime, category, universe category groups (hourly) | `category`, `min_updated_ts`; never `include_volume` | `ticker`, `category`, `fee_type`, `fee_multiplier`, `settlement_sources[]`, `exchange_index` |
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
`{"id", "sid", "seq", "type": "ok", "msg": {"market_tickers": [...]}}` after an update, or after a `subscribe` the server merged into an existing
subscription (ADR 0020),
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
responses carrying the same command `id` and distinct `sid`s. Observed on production:
a second `subscribe` naming a channel the connection already carries creates no new
subscription; the server merges its markets into the existing `sid` and replies `ok`
with the complete `market_tickers` list for each channel (ADR 0020). This is not in the
published specification.

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
                      kind 5: JSON {"ticker", "levels_rest", "outcome", "send_mono_ns",
                                    "send_wall_ns", "window_open_mono_ns",
                                    "window_close_mono_ns", "window_events"} (ADR 0021)
                                    plus "levels_local", "mismatched_levels", and
                                    "max_abs_diff_e2" unless undecidable; "match_index"
                                    when exact or consistent; "fault" when undecidable
                                    with a reason from the tap; "rest_levels" and
                                    "local_levels" only when inconsistent, so a real
                                    finding is diagnosable from the tape alone
```

Readers validate the magic and version, tolerate a truncated final record (crash), and
expose records as an iterator. A decompression error, or bytes after the end of the zstd
frame, is damage rather than truncation: a writer never produces it, so readers stop there
and report it. Records are never rewritten.

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

Path: `data/baked/<table>/dt=YYYY-MM-DD/hour=HH/part-<n>.parquet`. One bake of one closed hour of
raw segments (`raw/YYYY-MM-DD/HH/`) writes every table's part files for that hour and replaces the
hour's earlier files whole (ADR 0025). A table with no rows in an hour has no file for it. The hour
of a partition is the hour directory of the segments it was baked from; a record received in the
last moments of an hour can sit in the next hour's segment, because the writer files records by
the time it writes them (docs/INTERFACES.md 8.4).

Every column is an integer, a boolean, or a string, in the encodings of section 1.1; no column is
floating point. Strings are plain Arrow strings, so row-group statistics on `ticker` let a reader
skip every other market's rows. Files are zstd level 6, in row groups of 65,536 rows, with
statistics on every column and the sort order in the file metadata. Low-cardinality columns are
dictionary-encoded and rising ones delta-encoded, as the second table says; every other column is
plain. These settings were chosen by measuring the recorded archive (docs/OPERATIONS.md 5) and are
fixed, so baking the same segments again writes byte-identical files.

| table | columns (`?` nullable) |
|---|---|
| `deltas` | `ticker` string, `ts_ms` i64?, `recv_mono_ns` i64, `recv_wall_ns` i64, `conn_id` i16, `sid` i32, `seq` i64?, `side` i8, `price_e4` i32, `delta_e2` i64, `own_client_order_id` string? |
| `snapshots` | `ticker` string, `recv_wall_ns` i64, `recv_mono_ns` i64, `conn_id` i16, `sid` i32, `seq` i64?, `side` i8, `price_e4` i32, `count_e2` i64, `reason` i8 |
| `trades` | `ticker` string, `trade_id` string, `ts_ms` i64, `recv_wall_ns` i64, `sid` i32, `seq` i64?, `price_e4` i32, `count_e2` i64, `taker_side` i8 (0 bid, 1 ask), `is_block` bool |
| `lifecycle` | `ticker` string, `msg_type` string, `event_type` string?, `ts_s` i64?, `recv_wall_ns` i64, `sid` i32, `seq` i64?, `payload_json` string |
| `gaps` | `conn_id` i16, `sid` i32, `recv_wall_ns` i64, `recv_mono_ns` i64, `expected_seq` i64, `got_seq` i64, `resolved_recv_wall_ns` i64? |
| `audits` | `ticker` string, `recv_wall_ns` i64, `conn_id` i16, `outcome` string, `send_wall_ns` i64, `send_mono_ns` i64, `recv_mono_ns` i64, `window_open_mono_ns` i64, `window_close_mono_ns` i64, `window_events` i32, `match_index` i32?, `levels_rest` i32, `levels_local` i32?, `mismatched_levels` i32?, `max_abs_diff_e2` i64?, `fault` string?, `rest_levels_json` string?, `local_levels_json` string? |

| table | sorted by, within a file | dictionary-encoded | delta-encoded |
|---|---|---|---|
| `deltas` | `ticker, ts_ms, seq, recv_wall_ns` | `ticker, conn_id, sid, side, price_e4, delta_e2, own_client_order_id` | `ts_ms, recv_mono_ns, recv_wall_ns, seq` |
| `snapshots` | `ticker, recv_wall_ns, seq, side, price_e4` | `ticker, recv_wall_ns, recv_mono_ns, conn_id, sid, side, reason` | |
| `trades` | `ticker, ts_ms, seq, trade_id` | `ticker, sid, price_e4, taker_side` | `recv_wall_ns, seq` |
| `lifecycle` | `ticker, recv_wall_ns, seq` | `ticker, msg_type, event_type` | |
| `gaps` | `conn_id, recv_wall_ns, sid` | | |
| `audits` | `ticker, recv_wall_ns` | `ticker, outcome, fault` | |

Nulls sort last, and rows equal on every sort key keep the order of their records. Every table
that changes a book carries `recv_mono_ns`, because the order the recorder applied changes in is
their monotonic receive order: the wall clock can step backwards, and a reconnected connection's
subscription gets the same `sid` again and restarts its sequence. A market's rows
(a connection's, for `gaps`) share one part file of the hour, unless its spill bucket holds more
rows than one part may; then they are spread over consecutive part files, each sorted on its own.

- **`deltas`**: one row per `orderbook_delta` frame, in YES space (section 1.3), converted by the
  same functions the recorder applies to its books.
- **`snapshots`**: one row per level of an `orderbook_snapshot` frame, bids with `side` 0 and asks
  with `side` 1; an empty book is one row with `side = -1`, `price_e4 = 0`, `count_e2 = 0`, as in
  keyframes. `reason` is read from the segment alone: 1 (resync) when the snapshot answers a
  `get_snapshot` command earlier in the segment or repeats a market already snapshotted on that
  `sid` in the segment; otherwise 2 (reconnect) when the segment begins a new connection and the
  previous segment of the same connection in the hour ended with a lost connection, a `close`
  whose detail is not `stopped`; otherwise 0 (initial). A reconnect across an hour boundary shows
  as 0.
- **`trades`**: one row per `trade` frame; `price_e4` is the YES price.
- **`lifecycle`**: one row per message on the lifecycle channel. `market_lifecycle_v2` rows carry the
  market ticker and `event_type`; `event_lifecycle` and `event_fee_update` rows carry the event
  ticker in `ticker`, their message type in `msg_type`, and no `event_type`. `ts_s` is `settled_ts`
  for `settled` and `determination_ts` for `determined`, and null otherwise. `payload_json` is the
  `msg` object exactly as received, so fee multipliers and metadata never pass through a float.
- **`gaps`**: one row per `GAP` record. A gap on an orderbook subscription stales every book received
  on that `sid` in the segment; `resolved_recv_wall_ns` is when the last of them was resnapshotted,
  and null when the gap staled no book (a gap on `trade`, for example) or one still awaited its
  snapshot when the segment ended.
- **`audits`**: one row per `AUDIT` record (ADR 0021), `recv_*` being the REST reply;
  `rest_levels_json` and `local_levels_json` are the level lists an inconsistent audit recorded, so
  a finding stays diagnosable after its raw hour is pruned.

Tables not produced:

| table | why |
|---|---|
| `tickers` | the `ticker` channel is live-only (ADR 0018, ADR 0027); top-of-book for recorded markets derives from `deltas` and `snapshots` |
| `markets`, `series`, `fee_changes` | raw segments hold only WebSocket traffic; these tables wait for a recorder change that tapes REST metadata (ADR 0025) |

**Record accounting.** A bake counts every data record of the hour exactly once, and a record's rows
are written only after all of it decoded (ADR 0025, condition 3):

| outcome | key | records |
|---|---|---|
| baked | the frame's type | `orderbook_delta`, `orderbook_snapshot`, `trade`, `market_lifecycle_v2`, `event_lifecycle`, `event_fee_update` |
| baked | `gap`, `audit` | `GAP` and `AUDIT` records |
| not baked | `command` | outbound commands |
| not baked | `connection` | connection events; their spans, sleeps, and overflow counts go to the manifest |
| not baked | the reply's type | `subscribed`, `ok`, `unsubscribed`, and `error` frames |
| not baked | `ticker` | `ticker` frames, should one reach a segment (ADR 0018) |
| decode failure | `frame_envelope` | a frame that is not a JSON object with a string `type` |
| decode failure | `frame_payload` | a frame of a known type whose payload does not match its struct, lacks `sid`, or holds a malformed number |
| decode failure | `frame_unknown_type` | a frame of any other type, so a message Kalshi adds blocks pruning until the baker knows it |
| decode failure | `gap_payload`, `audit_payload`, `connection_payload`, `command_payload` | a malformed annotation record |

A truncated final record is tolerated, as section 4 says, and its segment is marked `truncated`. A
segment with a malformed header, an unknown record kind, or damage (section 4) counts as a corrupt
segment: the records read before the fault are baked, and like a decode failure it keeps its hour
from being pruned.

**Memory.** A bake streams each segment's records and never holds an hour in memory. A table's rows
are buffered 50,000 at a time and spilled to an Arrow IPC file, one record batch per spill bucket,
the bucket being the CRC-32 of the row's ticker (connection for `gaps`) modulo 64. Once the hour is
read, buckets are laid out in order into parts of at most `bake.max_part_rows` rows (500,000 by
default): a bucket that fits goes whole into a part, and a larger one, which a single busy market can
fill, is cut in the order its rows were added. Each part alone is read back, sorted by index, and
written one row group at a time, so a bake holds at most one part's rows, their order, and one
sorted row group. How rows fall into buckets and parts depends only on the rows. Parts are written
under `baked/.staging/` and moved into place once every part of the hour exists.

## 7. Manifests

One JSON document per UTC day at `data/manifests/YYYY-MM-DD.json`. Each bake of an hour rewrites it,
and `tape prune --apply` rewrites it before deleting a file; a rewrite goes to a synced temporary
file renamed into place. Decoding is strict: an unknown or missing field, another version, or
inconsistent contents is an error. Paths are POSIX paths relative to `raw/` and `baked/`.

```
{
  "version": 1,
  "date": "2026-09-10",
  "software_version": "0.1.0",
  "segments": [{"path", "hour", "bytes", "sha256", "records",
                "first_recv_wall_ns", "last_recv_wall_ns", "truncated"}],
  "tables": {"deltas": {"rows", "files": [{"path", "hour", "rows", "bytes", "sha256"}]}, ...},
  "bake": {"hours": [{"hour", "bake_version", "software_version",
                      "accounting": {"records": {kind: n}, "baked": {source: n},
                                     "not_baked": {reason: n}, "decode_failures": {class: n},
                                     "corrupt_segments"},
                      "integrity": {"spans": [{"conn_id", "start_wall_ns", "end_wall_ns",
                                               "opened", "closed"}],
                                    "sleeps": [{"start_wall_ns", "end_wall_ns"}],
                                    "gaps": {"count", "stale_market_ns", "observed_market_ns"},
                                    "audits": {"exact", "consistent", "inconsistent",
                                               "undecidable", "levels_mismatched"},
                                    "writer_overflow_dropped"}}]},
  "uptime": {"seconds_recording", "seconds_in_day", "ratio": [n, d]},
  "gaps": {"count", "market_seconds_affected", "market_seconds_observed", "share": [n, d]},
  "audits": {"books_sampled", "books_exact", "books_consistent", "books_inconsistent",
             "books_undecidable", "levels_mismatched", "exact_ratio": [n, d],
             "consistency_ratio": [n, d]},
  "clock": {"chrony_offset_ms"},
  "pruned": [{"path", "bytes", "sha256", "pruned_wall_ns"}]
}
```

- **Ratios** are exact `[numerator, denominator]` integer pairs; `[0, 0]` means no data.
- **Segments** lists every segment a bake read, pruned or not, and `pruned` records when each was
  deleted, so the record of what was captured outlives the files (ADR 0025). An hour's `bake` entry,
  segments, and part files are replaced together by the next bake of that hour; an hour with a pruned
  segment is never baked again.
- **`uptime`**: seconds of the day with at least one taped connection open, less host sleep. A
  segment's span runs from its `open` record, or from the start of its hour when the segment continues
  a connection, to its `close` record or its last record; a span left open is joined to the span of
  the same connection that continues it without an `open`. Sleeps come from `clock_jump` records.
- **`gaps`**: `market_seconds_affected` sums, over markets, the time from a gap on a market's book
  subscription to the market's next snapshot or the end of its segment; `market_seconds_observed` sums
  the time from a market's first book message in a segment to the end of the segment or the market's
  removal from the subscription. Connection outages count against uptime, not here.
- **`audits`** counts outcomes as ADR 0021 defines them; undecidable audits are in neither ratio.
- **`clock.chrony_offset_ms`** is the system clock's offset as `chronyc -c tracking` reported it at the
  latest bake, rounded to milliseconds, or null where chrony is unavailable.
- **Not in version 1:** coverage of the REST-listed open markets, which raw segments cannot give.

The integrity numbers are recomputed from `bake.hours` on every write. The three published on the
status page are `uptime.ratio`, `gaps.share`, and `audits.consistency_ratio`.

## 8. Public API formats

Defined in [FRONTEND.md](FRONTEND.md) section 4. They are derived views over the tables
above and carry the same integer encodings. `scripts/gen_api_schema.py` writes the JSON Schema of
every REST body and live message to `web/src/api/schema.json` from the structs in
`tape.api.contract`, and a test fails when the committed file is stale (ADR 0023).

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
- Baked tables are the long-term record once raw hours are pruned (ADR 0025). A change to what a
  bake writes for the same segments, a decoder fix included, raises `BAKE_VERSION` in
  `tape.bake.bake`. Every hour baked by an older version is then stale: `tape bake` bakes it again
  and `tape prune` keeps its raw segments until it has been. Hours already pruned keep their older
  tables, so readers must accept every bake version of a table version. Byte-identical re-bakes hold
  for one bake version, part size, and pyarrow version.

## 10. Bus messages (version 1)

The recorder publishes live events on a ZeroMQ PUB socket (ADR 0008, ADR 0022). Every
message has two frames:

| frame | content |
|---|---|
| topic | ASCII: `md.<ticker>` for `BookSnapshot`, `BookDelta`, `BookRefresh`, `Trade`, and `Ticker`; `ctl.lifecycle` for `Lifecycle`; `ctl.gap` for `GapEvent`; `ctl.catalog` for `MarketCatalog`; `ctl.status` for `StatusReport` |
| payload | MessagePack map `{bus_epoch, bus_seq, event}` |

| field | type | note |
|---|---|---|
| `bus_epoch` | int | publisher start, wall ns; a new value means the publisher restarted |
| `bus_seq` | int | 1 for an epoch's first attempted message, then one more for every attempted message on any topic, delivered or not |
| `event` | map | one struct from [INTERFACES.md](INTERFACES.md) section 4, named by its `type` key; integers follow section 1.1, `Side` is 0 or 1, and a `Level` is the array `[price_e4, count_e2]` |

`BookRefresh` is the recorder's own image of a book it holds: `ticker`; `ts_ms`, the exchange
time of the last change applied, or null; `receipt`, the connection holding the book and the
local time the image was taken; `stale`; and `bids` and `asks`, best first. It is taken
between frames, so it equals the book after every message with a lower `bus_seq`.

`MarketCatalog` opens every refresh cycle: `markets`, one map per recorded market of the latest
universe decision, in ticker order, `{ticker, series_ticker, event_ticker, volume_24h, close_ts,
showcase}`, where `volume_24h` is a `count_e2` from the recorder's latest listing, `close_ts`
is Unix seconds or null, and `showcase` is true for a market a series group admitted (ADR 0028).
Each catalog replaces the previous one. None is sent before the first
decision (ADR 0023).

`StatusReport` follows every status log line: `interval_s`, the seconds between reports;
`universe_size`; `subscribed_markets`; and `connections`, one map per connection by ascending id,
`{conn_id, taped, frames, gaps, reconnects, stale_books, sink_dropped}`, each counter cumulative
since the recorder started.

Delivery is lossy per subscriber: one whose queue is full misses messages that others
receive, and learns it from a gap in `bus_seq`, which it can see only if it subscribes to
every topic. Messages are MessagePack rather than JSON because the bus is local and read only
by Python consumers built on the same structs. The payload carries no version field: the
recorder and its consumers are deployed from one revision, and a change here ships to both.
