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
BookRefresh(ticker, ts_ms, receipt, stale, bids, asks)    # the recorder's image of a live book (ADR 0022)
CatalogEntry(ticker, series_ticker, event_ticker, volume_24h: CountE2, close_ts: int | None, showcase: bool)
MarketCatalog(markets: tuple[CatalogEntry, ...])          # ctl.catalog: every recorded market (ADR 0023)
ConnectionReport(conn_id, taped, frames, gaps, reconnects, stale_books, sink_dropped)
StatusReport(interval_s, universe_size, subscribed_markets, connections: tuple[ConnectionReport, ...])
MarketEvent = BookSnapshot | BookDelta | Trade | Ticker | Lifecycle | GapEvent | BookRefresh
BusEvent = MarketEvent | MarketCatalog | StatusReport      # every event on the bus
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
class BucketRateLimiter(RateLimiter)   # one bucket and one lock per side; FIFO; sleep= injectable
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
    async def event(event_ticker: str) -> GetEventResponse       # {event, markets}
    async def series_by_ticker(series_ticker: str) -> Series
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
UpdateSubscriptionCommand(sid, action: "add_markets"|"delete_markets"|"get_snapshot", market_tickers)  # tickers required for every action
UnsubscribeCommand(sids); ListSubscriptionsCommand()
Command = SubscribeCommand | UpdateSubscriptionCommand | UnsubscribeCommand | ListSubscriptionsCommand
def encode_command(command: Command, command_id: int) -> bytes   # {"id","cmd","params"?}

class WsSession:
    def __init__(self, url, signer: Signer, clock: Clock, *, conn_id=0,
                 silence_timeout_ns: int | None = None,      # data silence; off by default (ADR 0019)
                 ping_interval_ns=10e9, ping_timeout_ns=20e9, # transport keepalive
                 connect_timeout_ns=10e9, send_timeout_ns=10e9, close_timeout_ns=2e9,
                 max_buffered_frames=4096)
    conn_id: int; frames_dropped: int; is_open: bool
    async def connect(self) -> None                  # KalshiTransportError on failure
    async def send(self, command: Command) -> int    # returns the assigned command id
    def frames(self) -> AsyncIterator[RawFrame]      # receive order; one consumer only
    async def close(self) -> None                    # idempotent
    async def __aenter__/__aexit__
```

`WsSession` decodes nothing, tracks no sequence numbers, and never reconnects; gap
handling and reconnect policy belong to the recorder. It signs `timestamp + "GET" +
"/trade-api/ws/v2"` on the upgrade. Liveness is measured at the transport (ADR 0019). The library answers the server's
heartbeat pings and also sends its own every `ping_interval_ns`; a pong missing for
`ping_timeout_ns` fails the connection with close code 1011, and after the library's
close timeout the pending read raises, so a dead peer surfaces as `WsClosedError` within about 32 seconds by default. Data silence is checked only when `silence_timeout_ns` is
set, which the recorder does solely for the unfiltered ticker connection. A peer close,
a missed pong, a set silence timeout, or a reader bug all raise `WsClosedError`, while a
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
                                markets: tuple[MarketSummary, ...];   # of l2_tickers, ticker order
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
class Group(Struct): group_id: str; conn_id: int; tickers: frozenset[str]
class Plan(Struct): groups: tuple[Group, ...]; tickers: frozenset[str]; by_id: Mapping[str, Group]
AddGroup(group) | RemoveGroup(group_id) | AddMarkets(group_id, tickers) | RemoveMarkets(group_id, tickers)

def plan(tickers: Iterable[str], *, max_per_group: int, max_connections: int,
         previous: Plan | None = None) -> Plan
def diff(current: Plan, desired: Plan) -> tuple[PlanChange, ...]
def to_commands(changes, *, channels: Sequence[str], use_yes_price: bool,
                sid_of: Mapping[str, tuple[int, ...]]) -> tuple[Command, ...]
```

Invariants (ADR 0020): at most one group per connection, and that group is the
connection's whole market set; at most `max_per_group` markets per group; markets on
different exchange shards may share a group, because shards matter for collateral and
order routing, not market data. With `previous`, a market stays on its connection unless
that connection is over capacity or no longer exists, because moving a market costs a
resnapshot. Tickers beyond `max_per_group * max_connections` are left out rather than
overfilling a connection; configuration validation keeps a valid setup from reaching that
case. `diff` orders removals before additions, and applying `diff(a, b)` to `a` yields
exactly `b` (property-tested). `to_commands` maps a group to every `sid` it owns: a
membership change emits one `update_subscription` per `sid`, and removing a group is one
`unsubscribe`.

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

### 8.4 `tape.recorder.writer` and `tape.recorder.supervisor`

```python
class SegmentSink:                 # owns SegmentWriters on a dedicated thread; asyncio never touches disk
    def __init__(self, root: Path, conn_id: int, clock: Clock, header_factory: HeaderFactory,
                 *, queue_max: int, ...)
    def start(self) -> None
    def put(self, record: Record) -> bool        # non-blocking, thread-safe; False when the queue is full
    def rotate(self) -> None                     # new file; called on every reconnect
    def close(self) -> None                      # drains, flushes, joins; idempotent
    stats: SinkStats; failure: BaseException | None
def segment_path(root, conn_id, wall_ns, counter) -> Path   # raw/YYYY-MM-DD/HH/conn-NN-UUUU.tape.zst

class SupervisorConfig(Struct):
    conn_id: int
    book_channels: tuple[str, ...] = ("orderbook_delta", "trade")   # one sid per channel
    firehose_channels: tuple[str, ...] = ()      # unfiltered, e.g. ("ticker",); never also a book channel
    use_yes_price: bool = True
    persist: bool = True                         # False = live-only (ADR 0018)
    backoff_initial_ns: int; backoff_max_ns: int; max_consecutive_failures: int | None = None
    subscribe_timeout_ns: int = 10e9             # a subscribe left unanswered fails the connection

class ConnectionSupervisor:
    def __init__(self, config: SupervisorConfig, *, session_factory: Callable[[], WsSession],
                 clock: Clock, sleep: Callable[[float], Awaitable[None]], jitter: Callable[[], float],
                 sink: SegmentSink | None = None, on_event: Callable[[MarketEvent], None] | None = None,
                 deadline_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep)
    async def run(self) -> None                  # until stop(); raises past max_consecutive_failures
    async def stop(self) -> None                 # idempotent
    async def set_group(self, group: Group | None) -> None   # the connection's whole market set
    def open_tap(self, tickers: Iterable[str], *, max_events: int) -> LiveBookTap   # ADR 0021, 8.7
    tapped_tickers: frozenset[str]              # markets some open tap observes
def backoff_delay_s(failures, initial_ns, max_ns, jitter) -> float
```

The supervisor also exposes read-only views of its books, its group, its live subscriptions,
recent server errors, and counters.

Sessions are single-use, so every connection attempt builds a new one from the factory.
Sleep and jitter are injected so reconnect behavior is deterministic under test. On
disconnect the supervisor writes a close record, marks every book stale, rotates the
segment, waits `min(max, initial * 2**failures) * (0.5 + jitter/2)`, then reconnects and
resubscribes from its own group, never from old `sid`s. Error codes 25, 26, and 27 are
counted and exposed for the recorder to act on.

A book connection subscribes each book channel once; later membership changes use
`update_subscription`. Because Kalshi merges a repeated `subscribe` into the existing
subscription (ADR 0020), an `ok` reply to a pending subscribe is handled as a merge: the
supervisor keeps a per-connection map from `sid` to channel, adopts the reply's
`market_tickers` as the membership, resolves the pending command, and requests snapshots
for any wanted market that lacks a fresh book, since which markets Kalshi snapshots on a
merge is undocumented. A subscribe that is not answered for every channel within
`subscribe_timeout_ns` fails the connection instead of staying pending; that deadline runs
on the injected `deadline_sleep`, separate from the backoff `sleep`. Book frames are routed
by membership in the connection's market set. A gap on a book `sid` stales every book on the
connection and requests snapshots for all of its markets. Known limitation: if the server
refuses a subscribe for only one of the book channels, that channel stays unsubscribed.
Duplicate and backpressure behavior is described in docs/ARCHITECTURE.md 7.1.

`on_event` receives every trade, ticker, lifecycle event, and gap, and every snapshot
and delta that was actually applied to a book.

### 8.5 `tape.recorder.auditor`

```python
class RecordSink(Protocol):          # tape.recorder.writer; SegmentSink satisfies it
    @property
    def conn_id(self) -> int: ...
    def put(self, record: Record) -> bool: ...

type AuditOutcome = Literal["exact", "consistent", "inconsistent", "undecidable"]

class AuditResult(Struct): ticker; outcome: AuditOutcome; window_open_mono_ns; send_mono_ns;
                           send_wall_ns; recv_mono_ns; recv_wall_ns; window_close_mono_ns;
                           window_events; match_index: int | None; levels_rest;
                           levels_local: int | None; mismatched_levels: int | None;
                           max_abs_diff_e2: int | None; fault: TapFault | None = None
class AuditStats(Struct):  rounds; books_sampled; books_exact; books_consistent;
                           books_inconsistent; books_mismatched; books_undecidable;
                           books_skipped_stale; books_missing_local; books_invalid_rest;
                           levels_mismatched
    exact_ratio -> tuple[int, int]         # (books_exact, books_sampled)
    consistency_ratio -> tuple[int, int]   # (exact + consistent, exact + consistent + inconsistent)
class WindowVerdict(Struct): outcome: AuditOutcome; match_index: int | None; reply_state: BookImage | None

def classify_window(window: TapWindow, rest_book: Book, *, reply_mono_ns: int,
                    checksum: Callable[[Book], int] = Book.checksum) -> WindowVerdict
def round_robin_choice(tickers: Sequence[str], count: int, cursor: int) -> tuple[tuple[str, ...], int]

class Auditor:
    def __init__(self, rest: KalshiRest, books: Callable[[], Mapping[str, Book]], clock: Clock, *,
                 sink_for: Callable[[str], RecordSink | None], open_tap: BookTapOpener,
                 sample_size: int, lead_ns: int, settle_ns: int, tap_max_events: int,
                 window_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep)
    async def audit_once(self) -> tuple[AuditResult, ...]
    async def run(self, *, interval_s: float, stop: asyncio.Event, sleep=asyncio.sleep) -> None
    stats: AuditStats
```

Sampling is round-robin over sorted tickers whose local book is fresh, so every book is
audited over successive rounds without randomness. For each batch of at most 100 markets
the auditor opens a book tap, waits `lead_ns`, fetches the REST books, waits `settle_ns`,
and closes the tap (ADR 0021). REST books are converted to YES space with
`rest_orderbook_levels`, complementing the NO side, because REST has no `use_yes_price`
flag.

`classify_window` rebuilds the local states from the tap's starting copy and its events.
The outcome is **exact** when the REST book equals the state as of the reply, **consistent**
when it equals the starting copy or the state after any event in the window,
**inconsistent** when it equals none of them, and **undecidable** when the tap cannot vouch
for every state it spans. Candidates are compared by level count and checksum, and a match
is always confirmed with `diff`, so a checksum collision can never pass. An undecidable
result carries the tap's fault reason when there is one.

`books_sampled` counts decidable audits; undecidable ones are counted separately and are
excluded from both ratios, which stay exact integer pairs. A malformed REST price is counted
in `books_invalid_rest` and a failing batch is logged and skipped; neither stops the round.
`run` races every wait against `stop`, the interval between rounds and a round in progress
alike, so a shutdown never waits out an interval; a batch cut off mid-window writes no
records, and its tap is still closed.

### 8.6 `tape.recorder.recorder`

```python
class PeriodicTask(Protocol):
    async def run(self, *, stop: asyncio.Event) -> None
class RecorderConfig(Struct): ...           # built from Settings by tape.cli.recorder_config
PINNED_SPEC_VERSIONS: Mapping[str, str]     # copied into every segment header
def keyframe_path(root: Path, wall_ns: int, interval_s: int) -> Path
def refresh_slices(tickers: Sequence[str], *, interval_s: int) -> tuple[tuple[str, ...], ...]
class BusStatus(Struct): bus_epoch; bus_seq; sent; dropped; errors; refreshes
class RecorderStatus(Struct): connections; universe_size; subscribed_markets; live_tickers;
                              bus: BusStatus | None      # None without a publisher

class Recorder:
    def __init__(self, config: RecorderConfig, *, clock: Clock, rest: KalshiRest,
                 limiter: RateLimiter, session_builder: SessionBuilder, sink_builder: SinkBuilder,
                 sleep, jitter, periodic_tasks: Sequence[PeriodicTask] = (),
                 publisher: Publisher | None = None)
    async def run(self) -> None
    async def stop(self) -> None; def request_stop(self) -> None     # idempotent
    def books(self) -> Mapping[str, Book]                            # merged across book connections
    def sink_for(self, ticker: str) -> SegmentSink | None
    def open_book_tap(self, tickers: Iterable[str], *, max_events: int) -> CompositeBookTap
    def latest_tickers(self) -> Mapping[str, Ticker]                 # live state, never taped
    def status(self) -> RecorderStatus
    periodic_tasks: tuple[PeriodicTask, ...]
```

`tape.recorder.recorder` is an adapter and cannot import `tape.config`, so it takes a
`RecorderConfig`; `tape.cli.build_recorder(settings, *, http, clock, host)` is the
composition root that wires real dependencies and the auditor, including its tap
opener and the audit lead, settle, and tap-bound settings.

Connection layout: connection 0 is live-only and carries the unfiltered `ticker` channel
(ADR 0018); connection 1 is taped and carries `market_lifecycle_v2`; connections
`2 .. 2 + book_connections - 1` are taped, and each carries exactly one planner group, its
whole market set, on `orderbook_delta` and `trade` with `use_yes_price` (ADR 0020). The layout must fit `max_connections`.

The session builder receives `conn_id` and `silence_timeout_ns`: the live-only ticker
connection gets `RecorderConfig.ticker_silence_timeout_s`, every other connection gets
none. A failed universe refresh keeps the current plan and retries after
`universe_retry_delay_s(failures, interval_s, jitter)`, which is
`min(interval, 15 s * 2**failures) * (0.5 + jitter/2)`, rather than waiting a full
interval. On every status tick `is_clock_jump(wall_delta_ns, mono_delta_ns)` compares
the two clocks; a wall-clock advance more than 5 seconds beyond the monotonic one means
the host slept, so the recorder logs a warning and writes a `clock_jump` connection
record into every taped sink. The record lands after the gap, and its deltas give the
gap's length.

Startup reads `GET /exchange/status` and resizes the rate limiter from
`GET /account/limits`. Every `universe_refresh_s` the recorder pages the open,
non-multivariate markets (logging when a listing is cut short by the page cap), selects the universe, replans with the previous plan, and gives each book connection its
group through `set_group`.
Every `keyframe_interval_s` it writes merged books to
`data_dir/keyframes/YYYY-MM-DD/HH/MM.parquet`. Every `status_interval_s` it logs one
structured status line. A failing periodic task is logged and does not stop capture; a
failing supervisor surfaces. Shutdown stops periodic tasks and the bus refresh cycle, writes
a final keyframe, stops every supervisor, closes the bus publisher, and closes every sink so
everything is flushed.

With a publisher (section 9), every supervisor's `on_event` publishes each event through a
`SequencedPublisher` whose epoch is the wall clock when the recorder was built; on the ticker
connection that follows updating the latest-ticker table. The tape path is untouched. The
refresh cycle publishes a `BookRefresh` of every book `books()` holds, receipt naming the
connection holding it, once per `bus_refresh_s`: each cycle fixes its markets, splits them
with `refresh_slices` (one slice per market, at most 10 slices per second), and publishes
one slice, without awaiting, before each pause of `bus_refresh_s / len(slices)`. The cycle
races the stop request like every loop; if it fails, it is logged and capture continues.
The status line carries `BusStatus`. Each refresh cycle opens with a `MarketCatalog` of the latest
universe decision's markets (none before the first decision), and every status tick publishes a
`StatusReport` of the status it logs (ADR 0023); both go through the same never-raising publisher,
so neither can affect capture.

### 8.7 `tape.recorder.tap`

```python
type BookChange = BookSnapshot | BookDelta
type TapFault = Literal["no_book", "stale", "overflow"]   # FAULT_NO_BOOK, FAULT_STALE, FAULT_OVERFLOW

class BookImage(Struct): bids: tuple[Level, ...]; asks: tuple[Level, ...]
    @classmethod
    def of(cls, book: Book) -> BookImage
    def to_book(self, ticker: str) -> Book
class TapWindow(Struct): ticker: str; start: BookImage | None; events: tuple[BookChange, ...];
                         fault: TapFault | None
    @classmethod
    def absent(cls, ticker: str) -> TapWindow

class BookTap(Protocol):
    def close(self) -> Mapping[str, TapWindow]
class BookTapOpener(Protocol):
    def __call__(self, tickers: Collection[str], *, max_events: int) -> BookTap
class LiveBookTap:        # one supervisor's tap: tickers, closed, record(change, *, was_stale), close()
class CompositeBookTap:   # several taps closed as one, plus absent windows for uncovered markets
```

A tap copies each market's book when it opens and then receives, in order, every snapshot
and delta the supervisor applies, until it closes. Opening and closing never await, so no
frame can be applied between copying a book and watching it. With no tap open, an applied
change costs one membership check on an empty dictionary. A window's fault is `no_book`
when no fresh book existed at opening, `stale` when the book went stale or disappeared
before closing, and `overflow` when more than `max_events` changes arrived; any fault makes
the audit undecidable rather than risking a false verdict. Closing is idempotent.

The recorder's hot path per frame: read, enqueue raw bytes for the writer,
`decode_envelope`, `GapTracker.observe`, then off the hot path full decode, book apply,
and bus publish. The writer queue is enqueued first so nothing downstream can lose a
frame.

## 9. `tape.bus`

A leaf adapter: it imports core modules only, never the recorder or the client.

```python
# ports
class PublisherStats(Struct): sent; dropped; errors         # sent + dropped + errors = publish calls
class Publisher(Protocol):
    stats: PublisherStats
    def publish(self, topic: bytes, payload: bytes) -> None   # never blocks, never raises
    def close(self) -> None                                    # idempotent; unsent messages discarded
class Subscriber(Protocol):
    def subscribe(self, topic_prefix: bytes) -> None           # b"" = every topic; BusError once closed
    def messages(self) -> AsyncIterator[tuple[bytes, bytes]]   # (topic, payload) until close()
    def close(self) -> None                                    # idempotent

# envelope
MARKET_DATA_PREFIX = b"md."; CONTROL_PREFIX = b"ctl."; LIFECYCLE_TOPIC = b"ctl.lifecycle"; GAP_TOPIC = b"ctl.gap"
CATALOG_TOPIC = b"ctl.catalog"; STATUS_TOPIC = b"ctl.status"
FIRST_BUS_SEQ = 1
class BusEnvelope(Struct): bus_epoch: int; bus_seq: int; event: BusEvent
def topic_for(event: BusEvent) -> bytes
def encode_bus_envelope(envelope: BusEnvelope) -> bytes        # MessagePack
def decode_bus_envelope(payload: bytes) -> BusEnvelope         # WireError if malformed
class SequencedPublisher:
    def __init__(self, publisher: Publisher, *, epoch: int)    # ValueError on a negative epoch
    epoch: int; last_seq: int; stats: PublisherStats           # stats.errors adds failures before the transport
    def publish(self, event: BusEvent) -> None              # next bus_seq; never raises
    def close(self) -> None

# sockets
DEFAULT_SEND_HWM = 10_000
def check_endpoint(endpoint: str) -> None                      # ipc:///absolute/path (<= zmq.IPC_PATH_MAX_LEN) or tcp://host:port
class ZmqPublisher(Publisher):
    def __init__(self, endpoint: str, *, send_hwm: int, context: zmq.Context | None = None)
    endpoint: str                                              # BusError if the path is taken or bind fails
class SubscriberStats(Struct): received; malformed
class ZmqSubscriber(Subscriber):
    def __init__(self, endpoint: str, *, receive_hwm: int, context: zmq.asyncio.Context | None = None)
    stats: SubscriberStats

# livebooks (pure)
type BookStatus = Literal["unknown", "fresh", "stale"]         # BOOK_UNKNOWN, BOOK_FRESH, BOOK_STALE
type ResetReason = Literal["start", "epoch", "gap"]            # RESET_START, RESET_EPOCH, RESET_GAP
class StatusChange(Struct): ticker; before: BookStatus; after: BookStatus
class Observation(Struct): reset: ResetReason | None; missed: int; applied: bool;
                           changes: tuple[StatusChange, ...]
class LiveBooksStats(Struct): messages; resets; missed; refreshes; ignored; book_errors
class LiveBooks:
    def observe(self, envelope: BusEnvelope) -> Observation    # one message, in arrival order
    def status(self, ticker: str) -> BookStatus
    def books(self) -> Mapping[str, Book]                      # held copies, fresh or stale; read only
    epoch: int | None; last_seq: int | None; stats: LiveBooksStats
```

Topics: `md.<ticker>` for snapshots, deltas, refresh images, trades, and ticker updates;
`ctl.lifecycle` and `ctl.gap`; `ctl.catalog`, a `MarketCatalog` of the recorded markets with
series, event, 24-hour volume, close time, and showcase flag, at the start of every bus refresh
cycle, and `ctl.status`, a `StatusReport`, every status interval (ADR 0023); `ctl.audit` and
`ctl.heartbeat` are reserved. Private events never use the `md.` prefix. Transport: ZeroMQ
PUB/SUB over `ipc://` (ADR 0008); message layout in [DATA_FORMATS.md](DATA_FORMATS.md) section
10.

Every payload is an envelope `{bus_epoch, bus_seq, event}`: `bus_epoch` is the publisher's
start time in wall ns and `bus_seq` counts every attempted send from 1, including sends that
fail to encode or that the transport drops, so a subscriber detects loss as a gap. Numbers
are shared by every topic, so only a subscriber to every topic can tell loss from filtering.
Every `bus_refresh_s` (default 10) the recorder also publishes, paced across the interval, a
refresh image of each book it holds (`BookRefresh`), consistent with every message of lower
`bus_seq` (section 8.6). Publishing never blocks or raises; counts appear in the recorder's
status.

Loss is per subscriber. A PUB socket never refuses a send: when one subscriber's queue is full
(`send_hwm` at the publisher, the kernel buffer, `receive_hwm` at the subscriber), ZeroMQ drops
that subscriber's copies alone and tells no one, so the other subscribers are unaffected and
the publisher's `dropped` counts only sends refused outright. The slow subscriber sees a gap.

`ZmqPublisher` binds with `LINGER 0` and sends with `NOBLOCK`, counting `zmq.Again` as
dropped and any other `ZMQError`, or a send after `close()`, as an error. Because libzmq
unlinks whatever is at an ipc path before binding, the publisher first removes a leftover
socket that refuses connections and raises `BusError` for a socket a live process listens on
or for anything that is not a socket, leaving it in place. `ZmqSubscriber.close()` ends
`messages()` cleanly; a message without two frames is skipped and counted.

`LiveBooks` implements the consumer rule of ADR 0022. At its first message, a new epoch, or a
`bus_seq` that is not the next one, every book becomes unknown and is dropped; a book becomes
known at its next `BookRefresh`, fresh or stale as the image says, and later snapshots and
deltas apply to it (a stale copy ignores deltas until a snapshot, as the recorder's book does).
Book events for an unknown book are ignored, and a change that would break a book invariant
drops the copy. `Observation.changes` lists every status change once, so a server sends
`resync` for books that became unknown and snapshots for books that became known; `applied`
says whether a book event reached a held copy. Property-tested: for any interleaving of book
changes, silent stale transitions, restarts, and lost messages, a copy reported stale is stale
at the publisher, and a fresh copy equals the publisher's book whenever that book is fresh.

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

A Starlette application served by uvicorn on localhost (ADR 0023). Routes and the live protocol
are specified in [FRONTEND.md](FRONTEND.md) 4 and are binding. Only the composition root imports
the package, and it never imports the recorder: the recorder's catalog, status, and books reach it
over the bus (`scripts/check_layers.py`).

```python
# contract: every REST body and live message, as msgspec structs; schema in web/src/api/schema.json
MarketsResponse; MarketRow; MarketDetail(MarketRow); PriceRange; Depth
ServiceStatus; RecorderHealth; ConnectionHealth; BusHealth; ErrorResponse; ErrorBody
ServerMessage = HelloMessage | SubscribedMessage | SnapshotMessage | DeltaMessage | BookMessage
              | ResyncMessage | TradeMessage | TickerMessage | ErrorMessage      # tagged by "t"
ClientMessage = SubscribeRequest                                                 # tagged by "op"
CLOSE_GOING_AWAY = 1001; CLOSE_POLICY_VIOLATION = 1008; CLOSE_TRY_AGAIN_LATER = 1013; CLOSE_TOO_SLOW = 4000

# directory (pure)
class MarketMetadata(Struct): title; subtitle; category; price_ranges        # each None until resolved
UNRESOLVED: MarketMetadata
class MarketDirectory:
    def apply_catalog(self, catalog: MarketCatalog) -> None                 # replaces the catalog whole
    def apply_ticker(self, update: Ticker) -> None
    def apply_status(self, report: StatusReport, *, received_mono_ns: int) -> None
    def entry(self, ticker: str) -> CatalogEntry | None
    def top(self, limit: int) -> tuple[CatalogEntry, ...]                   # volume desc, then ticker
    def row(self, entry, *, metadata: MarketMetadata, book: Book | None) -> MarketRow
    def detail(self, entry, *, metadata: MarketMetadata, book: Book | None) -> MarketDetail
    def service_status(self, *, now_mono_ns: int, bus: BusHealth, clients: int) -> ServiceStatus

# metadata (adapter)
def metadata_limits(requests_per_s: int) -> BucketLimits
class ResolverStats(Struct): requests; failures; refused; unparsable; pending; events; series
class MetadataResolver:
    def __init__(self, rest: KalshiRest, *, clock: Clock, ttl_s: int, retry_initial_s: int = 30,
                 retry_max_s: int = 900, max_pending: int = 10_000, max_cached: int = 20_000)
    def request(self, entries: Iterable[CatalogEntry]) -> None               # never waits
    def lookup(self, entry: CatalogEntry) -> MarketMetadata                  # from memory, at once
    async def run(self) -> None                                              # until cancelled
    stats: ResolverStats

# session
class LiveSocket(Protocol):
    async def receive(self) -> str | bytes | None; async def send(self, text: str) -> bool
    async def close(self, code: int) -> None
class SessionFeed(Protocol):
    def subscribe(self, session: ClientSession, tickers: Sequence[str]) -> None
    def snapshot(self, ticker: str) -> SnapshotMessage | None
class ClientSession:
    def __init__(self, socket: LiveSocket, feed: SessionFeed, *, clock: Clock, queue_max: int)
    def offer(self, messages: Sequence[ServerMessage]) -> None               # never waits
    def replace_subscriptions(self, tickers: Sequence[str]) -> None
    def close(self, code: int) -> None                                       # idempotent
    async def run(self) -> None
    subscriptions; close_code; lags; queued

# hub
class ServeConfig(Struct): allowed_origins: frozenset[str]; max_clients; max_tickers;
                          client_queue_max; bus_refresh_s          # built by tape.cli.serve_config
class LiveHub(SessionFeed):
    def __init__(self, subscriber: Subscriber, *, directory: MarketDirectory, config: ServeConfig,
                 clock: Clock, request_metadata: Callable[[Iterable[CatalogEntry]], None])
    async def run(self) -> None; def close(self) -> None
    def receive(self, payload: bytes) -> None                                # one bus message
    def admit(self, socket: LiveSocket) -> ClientSession | None              # None at max_clients
    def detach(self, session: ClientSession) -> None
    def close_sessions(self, code: int) -> None
    def book(self, ticker: str) -> Book | None; def bus_health(self) -> BusHealth
    config; directory; clients; malformed

# app
API_PREFIX = "/api/v1"
def create_app(*, hub: LiveHub, resolver: MetadataResolver, clock: Clock) -> Starlette
```

`tape.cli` composes it: `serve_config(settings)`, `build_api(settings, *, http, clock, subscriber)
-> LiveApi(app, hub, resolver)`, `listen_socket(host, port)`, and `serve_api(settings, *, http,
clock, stop, sock)`, which runs uvicorn, the hub, and the resolver until `stop` and then closes
every live connection with 1001, lets uvicorn finish, closes the bus subscriber, and cancels the
resolver. The adapter cannot import `tape.config`, so the hub takes a `ServeConfig`, and the app
reads the hub's configuration and directory rather than taking them twice.

- **Directory.** Rows exist for the markets of the latest `MarketCatalog`. Ticker updates are kept
  for every market until the first catalog and for catalog markets after it. `recording` is true
  while the latest `StatusReport` arrived within two of its own `interval_s`.
- **Metadata.** One `GET /events/{event_ticker}` per event gives the event title and each market's
  `yes_sub_title` and price grid (converted with `parse_price`), and one `GET /series/{series_ticker}`
  per series gives the category. The REST client has no signer and a `BucketRateLimiter` of its
  own at `metadata_limits(serve.metadata_requests_per_s)`. Requests come only from market lists,
  market details, and subscriptions. An entry is queued once while pending or in flight; a value is
  served until it is replaced and refetched after `metadata_ttl_s`; a `KalshiError` or `WireError`
  is logged, counted, and retried no sooner than 30 s, doubling to 900 s; a grid that does not
  parse leaves only that market's `price_ranges` null.
- **Hub.** It subscribes to every topic and applies each message to `LiveBooks` before fanning it
  out without an await, so each session receives one market's messages in bus order. A book that
  became unknown sends `resync` with `bus_loss`; one that became known or turned fresh sends
  `snapshot`; one that turned stale sends `book`; an applied delta sends `delta`; an exchange
  snapshot applied to a fresh book sends `snapshot`; trades and ticker updates always go to
  followers. A subscription dedupes tickers, rejects `unknown_ticker` (not in the catalog) and
  `too_many_tickers`, replies with `subscribed`, and sends snapshots for known books new to the set.
- **Session.** `hello` is queued before the connection is accepted. An offer that does not fit
  `client_queue_max` discards the queue and the offer except the offer's own `subscribed` or
  `error` reply, then queues `resync` (`client_lag`) and a snapshot for each followed market; the
  third lag within 60 s closes with 4000. A message over 4096 bytes or an eleventh within one
  second closes with 1008. Malformed JSON, an unknown `op`, or a message that fails its schema
  gets `error` with `malformed_json`, `unknown_op`, or `invalid_message`. A send that takes over
  10 s abandons the connection.
- **App.** Every HTTP response carries `Cache-Control: no-store`; errors use `bad_request`,
  `unknown_ticker`, `not_found`, `method_not_allowed`, and `internal_error`. CORS allows the
  configured origins for `GET`. A handshake without an allowed `Origin`, including one with none,
  is closed before acceptance, which the server answers with 403; beyond `max_clients` it is accepted and closed with
  1013. uvicorn closes frames over 64 KB with 1009 before the app sees them.

## 16. Errors

```
TapeError
  FixedPointError          # also a ValueError
  WireError                # also a ValueError
  BookInvariantError(ticker, detail)
  TapeCorruptionError
  ConfigError
  BusError                 # endpoint taken, bind or receive failure; never raised by publish
  KalshiError
    KalshiHttpError(status, code, message, details)
    RateLimitedError
    KalshiTransportError
    WsProtocolError(code, message)
    WsClosedError
  SequenceGapError        # internal signal, converted to GapEvent
```

## 17. Configuration (`tape.config`)

A TOML file plus environment overrides, validated into frozen structs with
`forbid_unknown_fields`, so a misspelled key is an error rather than a silent default.
Loading uses the standard library's `tomllib`; there is no configuration dependency.

```toml
[kalshi]
env = "prod"                    # "prod" | "demo"; REST and WS URLs derive from it
key_id = "..."                  # the API key id, not a secret; only tape record needs it
private_key_path = "~/.config/tape/keys/prod-read.pem"   # must be mode 600; only tape record needs it
rest_timeout_s = 10
ws_ping_interval_s = 10          # 1 to 60; transport keepalive for every connection
ws_ping_timeout_s = 20           # 1 to 60
ws_silence_timeout_s = 60        # applies only to the live-only ticker connection

[recorder]
data_dir = "data"
max_connections = 16            # ticker + control + book_connections must fit
book_connections = 4
group_size = 500                # at most 500 (ADR 0010)
keyframe_interval_s = 300       # whole minutes dividing an hour
audit_interval_s = 300
audit_sample = 200
audit_lead_ms = 250             # 1 to 5000; audit window opens this long before the request
audit_settle_ms = 750           # 1 to 5000; and stays open this long after the reply
audit_tap_max_events = 5000     # 1 to 50000 book changes per market per window
writer_queue_max = 200000
universe_refresh_s = 300
status_interval_s = 60
bus_endpoint = "ipc:///run/tape/bus.sock"   # absent by default: no bus; ipc:///absolute/path or tcp://host:port
bus_refresh_s = 10              # 1 to 60; seconds between refresh images of each book
bus_send_hwm = 10000            # 1000 to 100000 messages queued per bus subscriber

[serve]
listen_host = "127.0.0.1"
listen_port = 8080               # 1 to 65535
allowed_origins = ["http://localhost:5173", "http://127.0.0.1:5173"]   # exact scheme://host[:port] for CORS and the WebSocket Origin check
max_clients = 200                # 1 to 1000 live connections
max_tickers_per_client = 10      # 1 to 50
client_queue_max = 5000          # 100 to 100000 messages queued per live client before a client_lag resync
bus_receive_hwm = 10000          # 1000 to 100000 bus messages queued in the API before ZeroMQ drops its copies
metadata_requests_per_s = 2      # 1 to 10 public Kalshi requests for titles, categories, and price grids
metadata_ttl_s = 3600            # 60 to 86400 seconds resolved metadata is served
# The API follows the bus at recorder.bus_endpoint; there is no second setting for it, and
# `tape serve` refuses to start without it.

[recorder.universe]
min_volume_24h = "1000.00"      # fixed-point string, never a float
max_l2_markets = 2000
showcase_series = ["KXBTC15M", "KXPAYROLLS", "KXHIGHNY", "KXFEDDECISION"]
exclude_mve = true
```

```python
class Settings(Struct): kalshi: KalshiSettings; recorder: RecorderSettings; serve: ServeSettings
def load_settings(path: Path, *, environ: Mapping[str, str]) -> Settings   # ConfigError on any problem
def signing_credentials(settings: Settings) -> SigningCredentials          # ConfigError unless key_id and a mode-600 key file are set
def redacted(settings: Settings) -> dict[str, object]                      # for `tape config check`
ENDPOINTS: Mapping[Env, KalshiEndpoints]                                    # prod and demo, fixed
```

Rules:

- **Endpoints are never free text.** `env` selects them, so a demo key cannot be pointed at
  production by a typo in a URL. Demo and production credentials are separate.
- **Environment overrides** are `TAPE_<SECTION>__<KEY>`, nesting with double
  underscores, for example `TAPE_RECORDER__UNIVERSE__MAX_L2_MARKETS=500`. A list takes
  comma-separated items. `environ` is injected; the process environment is never read
  globally.
- **Paths**: a leading `~` expands from the injected `HOME`; relative paths resolve
  against the directory holding the configuration file, not the working directory.
- **Validation**: positive intervals, ping interval and pong timeout each between 1 and
  60 seconds (a dead connection goes unnoticed for their sum), `group_size` at most 500,
  `book_connections * group_size` at least `max_l2_markets`, audit lead and settle
  between 1 and 5000 milliseconds, the audit tap bound between 1 and 50000 changes,
  `bus_refresh_s` between 1 and 60 seconds (a consumer waits up to that long to recover a
  book), `bus_send_hwm` between 1000 and 100000 messages (a reconnect's snapshot burst must
  fit; the queue lives in the recorder's memory), a `bus_endpoint`, when set, that is an
  absolute ipc path within ZeroMQ's length limit (103 bytes on macOS) or a tcp address with a
  port, and `2 + book_connections` at most `max_connections` (ADR 0020), and `data_dir` must be
  writable.
- **Credentials** are checked only for a command that signs. `key_id` and `private_key_path`
  may be left out, because `tape serve` holds no credentials (ADR 0023) and never opens the
  key; `tape record` calls `signing_credentials`, which requires both and a key file that
  exists and is readable by its owner alone, before it connects. `tape config check` checks
  what `tape record` needs by default, and with `--for serve` what `tape serve` needs instead:
  no credentials, and `recorder.bus_endpoint` set.
- **Serve**: every `[serve]` key has a default, so the section may be left out. `listen_port` is
  1 to 65535; `allowed_origins` names at least one exact `scheme://host[:port]`; `max_clients` is
  1 to 1000 and `max_tickers_per_client` 1 to 50; `client_queue_max` is 100 to 100000, room for a
  resync and a snapshot of each followed market; `bus_receive_hwm` has the bounds of
  `bus_send_hwm`; `metadata_requests_per_s` is 1 to 10 and `metadata_ttl_s` 60 to 86400.
- **Secrets are paths, never values.** `tape config check` prints the effective settings
  with nothing to redact beyond what is already only a path.

The real configuration lives in `tape.toml`, ignored by git; `config/tape.example.toml`
is the committed template.
