# Glossary

| Term | Meaning |
|---|---|
| **Market** | One binary contract that settles YES ($1) or NO ($0). Identified by a ticker such as `KXBTC15M-26SEP092130-00`. |
| **Event** | A group of related markets (for example every strike of one day's temperature). `mutually_exclusive=true` means at most one market in the event settles YES. |
| **Series** | A recurring template of events (`KXHIGHNY`, `KXPAYROLLS`). Fee type and category live here. |
| **YES space** | Quoting everything in terms of the YES contract. A NO bid at price `q` is a YES ask at `1 - q`. The book, tape, and viewer use YES space only. |
| **Bid / ask** | `bid` buys YES (equivalently sells NO); `ask` sells YES (buys NO). Kalshi calls these `book_side`. |
| **Taker / maker** | The taker's order crosses the spread and pays the taker fee; the maker's order rested and pays the maker fee (zero on standard series). |
| **Tick** | The smallest price step for a market, from `price_ranges` (whole cent, half, quint, deci, or centi). |
| **`PriceE4`, `CountE2`, `DollarsE6`** | Integer encodings in 1/10,000 dollar, 1/100 contract, and 1/1,000,000 dollar. |
| **`ts_ms`** | Exchange-side event time in milliseconds. |
| **`recv_mono_ns` / `recv_wall_ns`** | Local monotonic and wall-clock receive times in nanoseconds. |
| **`sid`** | Subscription id assigned by the WebSocket server per channel per subscribe command. Connection-scoped. |
| **`seq`** | Per-`sid` sequence number on sequenced channels. A skip is a gap. |
| **Snapshot / delta** | A full book image versus a signed change to one price level on one side. |
| **Keyframe** | A periodic full-book image written by the recorder so replay can seek. Named after video I-frames. |
| **Bus** | The ZeroMQ PUB/SUB channel on which the recorder publishes live events to `tape serve` and the engine. Lossy per subscriber by design. |
| **`bus_epoch` / `bus_seq`** | The publisher's start time, and the number of each message it attempts from 1. A new epoch or a skipped number means a subscriber lost messages. |
| **Market catalog** | The recorder's list of the markets it records, with series, event, 24-hour volume, close time, and showcase flag, republished on `ctl.catalog` every `bus_refresh_s`. |
| **Universe group** | One ordered rule of the recorded universe (ADR 0028): named series or a series category, how many events to take, how many markets per event, and optionally how soon an event must close (`max_hours_to_close`). Groups apply in order at every universe refresh until `max_l2_markets` is reached. |
| **Showcase market** | A recorded market admitted by a series group; the catalog flags it. |
| **Status report** | The recorder's health counters, published on `ctl.status` every `status_interval_s`; `tape serve` shows the latest in `/api/v1/status`. |
| **Refresh image** | A `BookRefresh`: the recorder's own copy of one book, republished every `bus_refresh_s` so a bus consumer can recover that book after loss. |
| **Segment** | One raw tape file: every frame received on one connection during one hour (or until rotation). |
| **Tape** | The whole recorded archive: segments, keyframes, baked tables, manifests. |
| **Bake** | Converting one closed hour of raw segments into typed Parquet tables, and recording it in the day's manifest. |
| **Part file** | One Parquet file of a baked table for one hour, `part-<n>.parquet`, sorted by the table's keys. |
| **Bake version** | The baker's output version, recorded with each hour's bake. A bake by an older version is stale: the hour is baked again before its raw segments may be pruned. |
| **Record accounting** | A bake's count of every raw record as baked, intentionally not baked, or a decode failure. Pruning an hour requires every record accounted for and no failure. |
| **Manifest** | The daily JSON document of what was recorded, baked, and pruned, and how well: segments with hashes, part files with hashes, per-hour record accounting, and the integrity numbers. |
| **Prune** | Deleting an hour's raw segments once a verified bake of it exists and its retention window has passed (ADR 0025). |
| **Retention window** | `bake.raw_retention_hours`: how long after an hour ends its raw segments are kept. |
| **Catalog** | The read side of the archive: books at an instant, deltas and trades over a range, and a day's manifest. |
| **Gap epoch** | An interval during which a group's books were stale because of a sequence gap or disconnect. |
| **Audit** | A comparison of the recorder's book against an independent REST orderbook fetch. |
| **Shard (`exchange_index`)** | One of Kalshi's matching-engine instances (0 default, 1 combos, 2 crypto and commodities, 3 sports). Collateral is per shard. |
| **Token bucket** | Kalshi's rate-limit model: tokens refill per second, requests cost tokens (default 10), separate read and write buckets. |
| **Post-only** | An order that is cancelled instead of crossing. Guarantees maker status. |
| **GTC / IOC / FOK** | Good till canceled (optionally with expiration), immediate or cancel, fill or kill. |
| **STP** | Self-trade prevention: `taker_at_cross` cancels the incoming order, `maker` cancels the resting one. |
| **Queue ahead** | Contracts resting at the same price level that will fill before yours. Estimated by the tracker, observed via `post_position_fp`. |
| **Markout** | Mid price some time after a fill minus the fill price; a measure of adverse selection. |
| **VPIN** | Volume-synchronized probability of informed trading; a flow-toxicity estimate from taker sides. |
| **Fee dome** | Kalshi's taker fee `0.07 x P x (1-P)` per contract, largest at 50 cents. |
| **Location attestation** | A periodic confirmation of the account's location required to trade Sports, Elections, and Entertainment through the API. |
