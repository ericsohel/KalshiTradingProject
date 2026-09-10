# Interfaces

Module contracts for the `tape` package. Signatures are the design; implementations
must match them or change this document first. Type names follow
[DATA_FORMATS.md](DATA_FORMATS.md). All public types are immutable unless stated.

## 0. Cross-cutting rules

- Every boundary is a `typing.Protocol` or a frozen `msgspec.Struct`/`dataclass`.
- Core modules (`fixedpoint`, `timeutil`, `wire`, `book`, `fees`, `sim`, `engine`,
  `strategies`) perform no I/O, read no clock, and import no I/O library.
- Every I/O call has an explicit timeout. Every retry is bounded and only for
  idempotent operations.
- Functions either return a value or raise a `TapeError` subclass. No sentinel returns
  for failure, no bare `except`.

## 1. `tape.fixedpoint`

```python
PriceE4  = NewType("PriceE4", int)    # 0..10_000
CountE2  = NewType("CountE2", int)    # >= 0
DollarsE6 = NewType("DollarsE6", int) # signed

def parse_price(s: str) -> PriceE4          # "0.5600" -> 5600; raises FixedPointError
def parse_count(s: str) -> CountE2          # "12.50" -> 1250
def parse_dollars(s: str) -> DollarsE6      # "0.010000" -> 10_000
def parse_signed_count(s: str) -> int       # "-54.00" -> -5400; deltas only
def format_price(p: PriceE4) -> str         # 5600 -> "0.5600"
def format_count(c: CountE2) -> str         # 1250 -> "12.50"
def complement(p: PriceE4) -> PriceE4       # 10_000 - p
def notional_e6(p: PriceE4, c: CountE2) -> DollarsE6   # exact: e4 * e2 = e6
def div_ceil(n: int, d: int) -> int; def div_floor(n: int, d: int) -> int
```

Invariants: parse/format round-trip exactly; parsing rejects excess precision,
negative prices, and prices above 1.0000. Property-tested.

## 2. `tape.timeutil`

```python
Ms = NewType("Ms", int)
Ns = NewType("Ns", int)


class Clock(Protocol):
    def mono_ns(self) -> Ns: ...
    def wall_ns(self) -> Ns: ...


class SystemClock(Clock): ...  # adapter; the only place time.* is called


class FrozenClock(Clock): ...  # tests and replay; advanced explicitly
```

## 3. `tape.wire`

`msgspec.Struct` definitions (frozen, `kw_only=True`, unknown fields ignored) for every
REST response and WebSocket message used, named after the spec (`MarketV1`,
`OrderbookSnapshotMsg`, `OrderbookDeltaMsg`, `TradeMsg`, `TickerMsg`,
`MarketLifecycleV2Msg`, `FillMsg`, `UserOrderMsg`, `SubscribedResponse`,
`OkResponse`, `ErrorResponse`, ...). Fields keep Kalshi's names and string types; the
conversion to fixed-point happens in `tape.wire.convert`:

```python
def decode_envelope(raw: bytes | str) -> Envelope          # type, sid?, seq?, id?; msg left raw
def decode_msg[T: msgspec.Struct](env: Envelope, struct_type: type[T]) -> T
def to_book_snapshot(m: OrderbookSnapshotMsg, env: Envelope, recv: Receipt, *, use_yes_price: bool) -> BookSnapshot
def to_book_delta(m: OrderbookDeltaMsg, env: Envelope, recv: Receipt, *, use_yes_price: bool) -> BookDelta
def to_trade(m: TradeMsg, env: Envelope, recv: Receipt) -> Trade
def to_ticker(m: TickerMsg, env: Envelope, recv: Receipt) -> Ticker
def to_lifecycle(m: MarketLifecycleV2Msg, env: Envelope, recv: Receipt) -> Lifecycle
```

`decode_envelope` is the only decoder allowed on the recorder's hot path; it reads
`type`, `sid`, `seq`, and `id` without materializing `msg`. Conversions take the
envelope for `sid` and `seq`, raise `WireError` when `sid` is absent, and raise
`FixedPointError` on malformed numbers. Zero-count snapshot levels are dropped.

## 4. `tape.events`

Frozen, tagged `msgspec.Struct`s shared by recorder, bus, API, and engine:

```python
class Side(IntEnum): BID = 0; ASK = 1
Receipt(conn_id, recv_mono_ns, recv_wall_ns)
Level(price: PriceE4, count: CountE2)                     # array-encoded on the bus
BookSnapshot(ticker, ts_ms, receipt, sid, seq, bids: tuple[Level, ...], asks: tuple[Level, ...])
BookDelta(ticker, ts_ms, receipt, sid, seq, side, price, delta: int, own_client_order_id)
Trade(ticker, ts_ms, receipt, sid, seq, trade_id, price, count, taker_side, is_block)
Ticker(ticker, ts_ms, receipt, sid, last, bid, ask, bid_size, ask_size, volume, open_interest)
Lifecycle(ticker, receipt, sid, seq, event_type, payload_json)
GapEvent(receipt, sid, expected_seq, got_seq)
MarketEvent = BookSnapshot | BookDelta | Trade | Ticker | Lifecycle | GapEvent
```

Private events (fills, order updates, acknowledgements, timers) are defined in
`tape.engine` because only the engine consumes them.

## 5. `tape.book`

```python
class KeyframeRow(Struct): ticker; side: int (-1 = empty book); price_e4; count_e2; as_of_recv_ns; last_ts_ms | None; stale
class LevelDiff(Struct): side; price; count_a; count_b
class BookDiff(Struct): ticker; differences: tuple[LevelDiff, ...]; is_empty

class Book:                       # mutable, single-owner, not thread-safe; stale until first snapshot
    ticker: str; last_ts_ms: Ms | None
    def apply_snapshot(self, bids: Iterable[Level], asks: Iterable[Level], *, ts_ms: Ms | None) -> None
    def apply_delta(self, side: Side, price: PriceE4, delta: int, *, ts_ms: Ms | None) -> bool  # False if stale
    def mark_stale(self) -> None; def is_stale(self) -> bool
    def best_bid(self) -> Level | None; def best_ask(self) -> Level | None
    def size_at(self, side: Side, price: PriceE4) -> CountE2
    def depth(self, side: Side, n: int) -> list[Level]          # best first
    def levels(self, side: Side) -> list[Level]; def level_count(self, side: Side) -> int
    def checksum(self) -> int                                   # order-independent 64-bit; audits and tests
    def to_keyframe(self, *, as_of_recv_ns: Ns) -> list[KeyframeRow]

def diff(a: Book, b: Book) -> BookDiff                          # ValueError on different tickers
def books_from_keyframe_rows(rows: Iterable[KeyframeRow]) -> dict[str, Book]
```

Invariants (asserted in code, property-tested): counts are positive (a delta that
would go negative raises `BookInvariantError` and marks the book stale); `best_bid <
best_ask` whenever both exist (a crossing snapshot or delta raises and marks stale);
a snapshot fully replaces prior levels; deltas on a stale book are ignored and
reported by the `False` return, because their base is unknown.

## 6. `tape.client`

### 6.1 `tape.client.auth`

```python
HEADER_KEY = "KALSHI-ACCESS-KEY"; HEADER_TIMESTAMP = ...; HEADER_SIGNATURE = ...

class Signer(Protocol):                       # runtime_checkable
    @property
    def key_id(self) -> str: ...
    def sign(self, timestamp_ms: int, method: str, path: str) -> str      # base64 RSA-PSS
    def headers(self, method: str, path: str, *, now_ms: int) -> dict[str, str]

class RsaPssSigner:            # loads a PEM once; the key is never exposed or logged
    def __init__(self, key_id: str, private_key_path: Path, *, password: bytes | None = None)
```

Signs `timestamp + METHOD + path` with RSA-PSS/SHA-256, MGF1(SHA-256), salt length equal
to the digest length. `path` must start with `/` and must already have the query string
removed; `sign` raises `ValueError` otherwise rather than producing a signature the
exchange would silently reject. A bad key id, unreadable file, malformed PEM, or
non-RSA key raises `ConfigError`. `__repr__` redacts the key.

### 6.2 `tape.client.ratelimit`

```python
Bucket = Literal["read", "write"]
DEFAULT_TOKEN_COST = 10
class BucketLimits(Struct): refill_per_s: int; capacity: int
BASIC_READ = BucketLimits(200, 400); BASIC_WRITE = BucketLimits(100, 100)

class TokenBucket:              # pure; every method takes the current time
    def __init__(self, limits: BucketLimits, *, now_ns: Ns) -> None
    def tokens(self, now_ns: Ns) -> int
    def try_take(self, tokens: int, now_ns: Ns) -> bool
    def wait_ns(self, tokens: int, now_ns: Ns) -> Ns          # rounds up; 0 when available

class RateLimiter(Protocol):
    async def acquire(self, cost: int, *, bucket: Bucket) -> None
    def resize(self, *, read: BucketLimits, write: BucketLimits) -> None
class BucketRateLimiter(RateLimiter)   # one bucket and one lock per side; FIFO under contention
class NullRateLimiter(RateLimiter)     # never waits; tests and replay
```

Tokens are tracked in billionths so refill is exact integer arithmetic. A request for
more tokens than a bucket can ever hold raises `ValueError` instead of waiting forever.
Buckets default to the documented Basic tier and are replaced by `resize()` once
`GET /account/limits` has been read (ADR 0016).

### 6.3 `tape.client.rest`

`tape.wire.rest` holds frozen structs mirroring the pinned OpenAPI document field for
field, with Kalshi's own names and string encodings; conversion to fixed-point is the
caller's job. Inbound enumerated fields decode as `str` (ADR 0017). `Page[T]` carries
`items: tuple[T, ...]` and `cursor: str | None`.

```python
def build_client(base_url: str, *, timeout_s: float = ...) -> httpx.AsyncClient
DEFAULT_MAX_PAGES = 1000

class KalshiRest:
    def __init__(self, base_url: str, client: httpx.AsyncClient,
                 limiter: RateLimiter, clock: Clock, signer: Signer | None = None)

    # public market data (no signer required)
    async def exchange_status() -> ExchangeStatus
    async def markets(*, status=None, limit=100, cursor=None, **filters) -> Page[Market]
    async def market(ticker: str) -> Market
    async def orderbook(ticker: str, *, depth: int = 0) -> OrderbookCountFp
    async def orderbooks(tickers: Sequence[str]) -> list[MarketOrderbookFp]   # <= 100
    async def trades(*, ticker=None, min_ts=None, max_ts=None, cursor=None) -> Page[Trade]
    async def series(*, min_updated_ts=None) -> list[Series]
    async def fee_changes(*, show_historical: bool = False) -> list[SeriesFeeChange]
    async def events(*, status=None, with_nested_markets=False, cursor=None) -> Page[EventData]
    async def candlesticks(tickers, *, start_ts, end_ts, period_min) -> list[MarketCandlesticksResponse]

    # cursor-following generators; every one is bounded by max_pages
    def iter_markets(...) -> AsyncIterator[Market]
    def iter_trades(...) -> AsyncIterator[Trade]
    def iter_events(...) -> AsyncIterator[EventData]
    def iter_fills(...) -> AsyncIterator[Fill]
    def iter_settlements(...) -> AsyncIterator[Settlement]

    # authenticated
    async def account_limits() -> GetAccountApiLimitsResponse
    async def api_keys() -> GetApiKeysResponse
    async def fills(*, min_ts=None, cursor=None) -> Page[Fill]
    async def settlements(*, min_ts=None, cursor=None) -> Page[Settlement]
    async def balance(*, exchange_index: int | None = None) -> GetBalanceResponse

    # trading (write::trade key only)
    async def create_order(req: CreateOrderV2Request) -> CreateOrderV2Response   # 10 tokens
    async def cancel_order(order_id: str, *, market_ticker: str) -> CancelOrderV2Response  # 2
    async def decrease_order(order_id, *, reduce_by: CountE2, market_ticker) -> DecreaseOrderV2Response
    async def cancel_all() -> None                                              # kill switch, 2 tokens
```

The client never builds its own transport; `build_client` is called once by the
composition root, and tests pass an `httpx.AsyncClient` over `httpx.MockTransport`.
Requests are signed only when a signer is present, over the path from root including
`/trade-api/v2` and excluding the query string. Every call takes tokens from the
limiter first, using the `write` bucket for order mutations and `read` otherwise.

Errors: `KalshiHttpError(status, code, message, details)` for non-2xx, with the body's
`ErrorResponse` parsed when present; `RateLimitedError` for 429; `KalshiTransportError`
for network and timeout failures; `WireError` for a body that does not match its
struct. No `httpx` exception escapes.

Batch endpoints raise `ValueError` above 100 tickers rather than truncating, because a
silent truncation would look like an empty book. `cancel_all` returns nothing: Kalshi
answers `204` and reports no count, and inventing one would imply information the
exchange never gave.

### 6.4 `tape.client.ws`

```python
class RawFrame(Struct): payload: bytes; recv_mono_ns: Ns; recv_wall_ns: Ns

# Frozen command structs; each validates in __post_init__ what the server would reject.
SubscribeCommand(channels, market_tickers=None, use_yes_price=None, send_initial_snapshot=None)
UpdateSubscriptionCommand(sid, action: "add_markets"|"delete_markets"|"get_snapshot", market_tickers=None)
UnsubscribeCommand(sids); ListSubscriptionsCommand()
Command = SubscribeCommand | UpdateSubscriptionCommand | UnsubscribeCommand | ListSubscriptionsCommand
def encode_command(command: Command, command_id: int) -> bytes   # {"id","cmd","params"?}

class WsSession:
    def __init__(self, url, signer: Signer, clock: Clock, *, conn_id=0,
                 silence_timeout_ns=30e9, connect_timeout_ns=10e9, send_timeout_ns=10e9,
                 close_timeout_ns=5e9, max_buffered_frames=4096)
    conn_id: int; frames_dropped: int; is_open: bool
    async def connect(self) -> None                  # KalshiTransportError on failure
    async def send(self, command: Command) -> int    # returns the assigned command id
    def frames(self) -> AsyncIterator[RawFrame]      # receive order; one consumer only
    async def close(self) -> None                    # idempotent
    async def __aenter__/__aexit__
```

`WsSession` decodes nothing, tracks no sequence numbers, and never reconnects; gap
handling and reconnect policy belong to the recorder. It signs `timestamp + "GET" +
"/trade-api/ws/v2"` on the upgrade. The server drives the 10-second heartbeat and the
library answers its pings, so no client keepalive is configured; inbound silence past
`silence_timeout_ns`, a peer close, or a reader bug all raise `WsClosedError`, while a
deliberate `close()` ends `frames()` cleanly. A session is single-use.

**Backpressure.** A reader task drains the socket into a bounded buffer. When the
consumer falls behind, the *oldest* frame is dropped and `frames_dropped` counts it,
because blocking the reader would stall the socket and make the server overflow its own
subscription buffer (error 25), losing far more.

## 7. `tape.segment` (segment and keyframe I/O)

```python
class RecordKind(IntEnum): FRAME = 1; COMMAND = 2; GAP = 3; CONNECTION = 4; AUDIT = 5
SubscriptionInfo(sid, channel, group_id)
SegmentHeader(created_wall_ns, host, env, conn_id, ws_url, use_yes_price, subscriptions, software_version, spec_versions)
Record(kind, conn_id, recv_mono_ns, recv_wall_ns, payload: bytes)

class SegmentWriter:            # one file; rotation policy belongs to the recorder
    def __init__(self, path: Path, header: SegmentHeader, *, level: int = 3) -> None
    def append(self, record: Record) -> None      # ValueError after close or on payload > 2**32 - 1
    def flush(self) -> None                       # complete zstd block + OS flush
    def close(self) -> None                       # idempotent; context manager supported
    records_written: int; bytes_written: int
class SegmentReader:
    def __init__(self, path: Path) -> None        # parses the header; TapeCorruptionError if malformed
    header: SegmentHeader; truncated: bool        # truncated is final after records() is exhausted
    def records(self) -> Iterator[Record]         # stops cleanly at a truncated tail or unfinished frame
def write_keyframe(path: Path, rows: Iterable[KeyframeRow]) -> int    # atomic (temp file + rename)
def read_keyframe(path: Path) -> list[KeyframeRow]                    # TapeCorruptionError on schema mismatch
```

`SegmentWriter.append` is called from a single writer thread fed by a bounded
`queue.Queue`; the asyncio side never blocks on disk.

## 8. `tape.recorder`

Pure planning and detection logic, tested without a network, plus the adapters that
drive a live connection.

### 8.1 `tape.recorder.universe`

```python
class UniversePolicy(Struct): min_volume_24h: CountE2; max_l2_markets: int;
                              showcase_series: frozenset[str]; exclude_mve: bool = True;
                              max_seconds_to_close: int | None = None
class MarketSummary(Struct):  ticker; series_ticker; event_ticker; exchange_index; status;
                              volume_24h: CountE2; close_ts: int | None; is_mve: bool
    @classmethod
    def from_wire(cls, market: Market, *, is_mve: bool = False) -> MarketSummary
class UniverseDecision(Struct): l2_tickers: frozenset[str]; showcase: frozenset[str];
                                dropped_for_cap: int; reason_counts: Mapping[str, int]
def select(markets: Sequence[MarketSummary], policy: UniversePolicy, *, now_ts: int) -> UniverseDecision
```

Eligibility first (status `"active"`, multivariate legs, already closed, close horizon),
then every showcase-series market unconditionally, then the remaining budget by
descending 24-hour volume with ties broken by ticker. Showcase markets are never dropped
for the cap; overflow is reported in `dropped_for_cap`. A ticker repeated in one listing
is counted once, and the copy kept is chosen by content rather than arrival order. The
pinned spec carries no series field on a market, so `series_ticker` is the ticker's first
dash-separated segment.

### 8.2 `tape.recorder.planner`

```python
class Group(Struct): group_id: str; exchange_index: int; conn_id: int; tickers: frozenset[str]
class Plan(Struct): groups: tuple[Group, ...]; tickers: frozenset[str]; by_id: Mapping[str, Group]
AddGroup(group) | RemoveGroup(group_id) | AddMarkets(group_id, tickers) | RemoveMarkets(group_id, tickers)

def plan(tickers, *, shard_of: Mapping[str, int], max_per_group: int,
         max_connections: int, previous: Plan | None = None) -> Plan
def diff(current: Plan, desired: Plan) -> tuple[PlanChange, ...]
def to_commands(changes, *, channels: Sequence[str], use_yes_price: bool,
                sid_of: Mapping[str, tuple[int, ...]]) -> tuple[Command, ...]
```

Groups never mix exchange shards and never exceed `max_per_group`. Replanning with
`previous` keeps every ticker in its group unless that group is over capacity or the
ticker's shard changed, because moving a ticker costs a resnapshot. `diff` orders
removals before additions, and a kept group whose entire membership is replaced is
unsubscribed and resubscribed rather than emptied mid-flight. Applying `diff(a, b)` to
`a` yields exactly `b` (property-tested).

`sid_of` maps a group to **every** subscription id it owns, because Kalshi assigns one
`sid` per channel, not per subscribe command. `update_subscription` accepts exactly one
`sid`, so a membership change emits one command per `sid`; `unsubscribe` accepts many,
so removing a group is one command. A group with no `sid` yet can only be subscribed.

### 8.3 `tape.recorder.gaps`

```python
Ok | FirstMessage | Gap(expected, got) | Duplicate(expected, got)
class GapTracker:
    def observe(self, sid: int, seq: int | None) -> GapVerdict
    def reset(self, sid: int) -> None; def forget(self, sid: int) -> None
    def sids(self) -> Iterator[int]
    def messages(self, sid: int) -> int; def gaps(self, sid: int) -> int; def duplicates(self, sid: int) -> int
```

The first `seq` on a `sid` sets the baseline. `last + 1` is `Ok`, larger is a `Gap`,
equal or smaller is a `Duplicate` (suspicious, never fatal). `seq is None`, on unsequenced
channels such as `ticker` and `fill`, is `Ok` and leaves the baseline alone. After a gap
the baseline moves to the observed value, so one hole is reported once.

### 8.4 Adapters (next)

```python
class ConnectionSupervisor:   # one per WS connection: WsSession, segment writer feed, GapTracker, books
class Auditor:                # samples markets, fetches REST books, diffs against local books
class Recorder:               # composition root for `tape record`
```

The recorder's hot path per frame: read, enqueue raw bytes for the writer,
`decode_envelope`, `GapTracker.observe`, then off the hot path full decode, book apply,
and bus publish. The writer queue is enqueued first so nothing downstream can lose a
frame.

## 9. `tape.bus`

```python
class Publisher(Protocol):
    def publish(self, topic: bytes, payload: bytes) -> None     # never blocks; drops on HWM
class Subscriber(Protocol):
    async def messages(self) -> AsyncIterator[tuple[bytes, bytes]]
    def subscribe(self, topic_prefix: bytes) -> None
```

Topics: `md.<ticker>` for market data events (msgspec-encoded `Event` union),
`ctl.lifecycle`, `ctl.gap`, `ctl.audit`, `ctl.heartbeat`. Private events never use the
`md.` prefix. Transport: ZeroMQ PUB/SUB over `ipc://` (ADR 0008).

## 10. `tape.bake` and `tape.store`

```python
def bake_hour(raw_dir: Path, out_dir: Path, *, date: date, hour: int, spec: DecoderSpec) -> BakeReport   # idempotent
def write_manifest(day_dir: Path, reports: Sequence[BakeReport], *, clock_offset_ms: int) -> Manifest
class Catalog:                      # DuckDB views over data/baked and data/keyframes
    def book_at(self, ticker: str, at_wall_ns: Ns) -> Book         # nearest prior keyframe + deltas
    def deltas(self, ticker: str, t0: Ns, t1: Ns) -> pa.Table
    def trades(self, ticker: str, t0: Ns, t1: Ns) -> pa.Table
    def integrity(self, day: date) -> Manifest
```

## 11. `tape.engine`

```python
# Events (frozen structs, tagged union)
BookSnapshot(ticker, ts_ms, recv_ns, bids, asks)
BookDelta(ticker, ts_ms, recv_ns, side, price, delta, own_client_order_id)
TradeEvent(ticker, ts_ms, recv_ns, trade_id, price, count, taker_side, is_block)
TickerEvent(ticker, ts_ms, recv_ns, bid, ask, last, bid_size, ask_size)
LifecycleEvent(ticker, ts_s, recv_ns, kind, payload)
GapEvent(sid, recv_ns, expected_seq, got_seq)
FillEvent(ticker, ts_ms, recv_ns, order_id, client_order_id, price, count, is_taker, fee, post_position)
OrderUpdateEvent(order_id, client_order_id, status, remaining, ...)
OrderAckEvent(intent_id, order_id | None, error | None, send_ns, recv_ns)
TimerEvent(fire_ns, key)

# Intents
PlaceOrder(intent_id, client_order_id, ticker, side, price, count, tif, post_only, expiration_ts, stp)
CancelOrder(intent_id, order_id, ticker)
DecreaseOrder(intent_id, order_id, ticker, reduce_by)
CancelAll(intent_id)
SetTimer(intent_id, fire_ns, key)

class StrategyContext(Protocol):    # what a strategy may read
    def now_ns(self) -> Ns                       # derived from the last event, never the clock
    def book(self, ticker: str) -> Book
    def position(self, ticker: str) -> CountE2
    def open_orders(self, ticker: str) -> Sequence[OpenOrder]
    def fee_model(self) -> FeeModel
class Strategy(Protocol):
    def on_event(self, event: Event, ctx: StrategyContext) -> Sequence[Intent]
class ExecutionGateway(Protocol):
    async def submit(self, intent: Intent) -> None      # results come back as events
class EventSource(Protocol):
    async def events(self) -> AsyncIterator[Event]
class Engine:                       # single loop: source -> strategy -> gateway; logs blake2b(intents) hourly
```

Determinism rule: `Strategy` implementations import nothing from `tape.client`, `time`,
`random`, or `os`. A test replays a recorded day twice and asserts identical hashes.

## 12. `tape.fees`

```python
class FeeType(StrEnum): QUADRATIC, QUADRATIC_WITH_MAKER_FEES, QUADRATIC_WITH_COMBO_MAKER_FEES, FLAT
@dataclass(frozen=True) class FeeRegime: fee_type: FeeType; multiplier_e4: int; effective_from_ts: int
class FeeSchedule:                  # pure; built from series, event overrides, fee_changes
    def regime(self, ticker: str, at_ts: int) -> FeeRegime
def taker_fee(price: PriceE4, count: CountE2, regime: FeeRegime) -> DollarsE6
def maker_fee(price: PriceE4, count: CountE2, regime: FeeRegime) -> DollarsE6
```

Formulae: taker `ceil(M * 0.07 * C * P * (1-P))`, maker `ceil(M_maker * 0.0175 * C * P * (1-P))`
with combo makers at 50% of taker, computed in exact integer arithmetic to
micro-dollars and rounded per Kalshi's fee-rounding rules. The constants are verified
against `fee_cost` on real fills (see [TESTING.md](TESTING.md)).

## 13. `tape.sim`

```python
class FillModel(Protocol):
    def queue_ahead_after_cancel(self, ahead: CountE2, cancelled: CountE2, level_before: CountE2) -> CountE2
class Pessimistic(FillModel); class Optimistic(FillModel); class Calibrated(FillModel)  # phi fitted from probes
class SimExchange:                  # pure; consumes recorded events + intents, emits acks/fills
    def __init__(self, fee_schedule: FeeSchedule, fill_model: FillModel, latency: LatencyModel, limits: TokenLimits) -> None
    def on_market_event(self, e: Event) -> list[Event]
    def on_intent(self, i: Intent, now_ns: Ns) -> list[Event]
```

Semantics implemented: limit orders only; `good_till_canceled` with expiration;
`immediate_or_cancel`; `fill_or_kill`; post-only cross cancel; amend loses queue
unless reducing; `decrease` keeps queue; every operation rejected after `close_ts`;
settlement at `determined`; token buckets for reads and writes; latency sampled from
recorded ack distributions.

## 14. `tape.probe`

```python
class ProbePlan: markets: Sequence[str]; count: CountE2; ttl: timedelta; side_rule: Literal["join_best_bid"]
class ProbeRunner:                  # adapter; places post-only GTC orders, records queue estimates and outcomes
class QueueTracker:                 # pure; estimates queue-ahead from own-tagged deltas and trades
def fit_fill_model(samples: Sequence[ProbeSample]) -> Calibrated
```

## 15. `tape.api`

FastAPI application. Routes in [FRONTEND.md](FRONTEND.md). Dependencies injected at
startup: `Catalog`, `Subscriber`, settings. No route touches Kalshi directly.

## 16. Errors

```
TapeError
  FixedPointError          # also a ValueError
  WireError                # also a ValueError
  BookInvariantError(ticker, detail)
  TapeCorruptionError
  ConfigError
  KalshiError
    KalshiHttpError(status, code, message, details)
    RateLimitedError
    KalshiTransportError
    WsProtocolError(code, message)
    WsClosedError
  SequenceGapError        # internal signal, converted to GapEvent
```

## 17. Configuration (`tape.config`)

Loaded from a TOML file plus environment overrides, validated into a frozen struct:

```
[kalshi]   env = "prod" | "demo"; key_id; private_key_path; rest_timeout_s = 10; ws_silence_timeout_s = 30
[recorder] data_dir; max_connections = 16; group_size = 500; universe.min_volume_24h = "1.00";
           universe.max_l2_markets = 4000; universe.showcase_series = ["KXBTC15M", "KXPAYROLLS", ...];
           keyframe_interval_s = 300; audit_interval_s = 300; audit_sample = 200; writer_queue_max = 200_000
[bus]      endpoint = "ipc:///tmp/tape.pub"; hwm = 100_000
[api]      bind = "127.0.0.1:8080"; max_tickers_per_client = 10; max_clients = 200; cors_origins = [...]
[engine]   mode = "replay" | "shadow" | "live"; subaccount; write_key_id; write_key_path; bankroll_e6; kill_switch = true
```

Secrets are file paths, never values. `tape config check` validates and prints the
effective configuration with secrets redacted.
