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
| **Segment** | One raw tape file: every frame received on one connection during one hour (or until rotation). |
| **Tape** | The whole recorded archive: segments, keyframes, baked tables, manifests. |
| **Bake** | Converting raw segments into typed Parquet tables. |
| **Manifest** | The daily JSON summary of what was recorded and how well. |
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
