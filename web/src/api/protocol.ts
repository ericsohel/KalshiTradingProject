/**
 * Wire types of the `tape serve` live API, protocol 1 (docs/FRONTEND.md section 4).
 *
 * Hand-written for this first slice. ADR 0023 makes `schema.json`, generated from the
 * msgspec structs in `tape.api`, the single source of these types; when that generator
 * lands, this file is replaced by the generated one. Keep every API type here, and only
 * here, so the swap is one file.
 *
 * Units follow docs/DATA_FORMATS.md: `price_e4` is a YES price in 1/10,000 dollar
 * (0..10,000), `count_e2` is contracts x 100, `ts_ms` is exchange time in milliseconds.
 * `null` means the value is not known. Taxonomies the server may extend (rejection,
 * resync, and error codes) are open strings, as in ADR 0017; directional values
 * (sides, book status) are closed.
 */

/** The protocol number this client speaks; `hello.protocol` must match. */
export const PROTOCOL_VERSION = 1;

/** Largest valid `price_e4`: one dollar. */
export const MAX_PRICE_E4 = 10_000;

/** A YES price in 1/10,000 dollar, an integer in 0..10,000. */
export type PriceE4 = number;
/** A contract count x 100, a non-negative integer. */
export type CountE2 = number;
/** Exchange event time, milliseconds since the Unix epoch. */
export type TsMs = number;

/** A string union that still accepts values a newer server may add (ADR 0017). */
export type OpenString<Known extends string> = Known | (string & Record<never, never>);

/** Freshness of a market's book as the API reports it. */
export type BookStatus = "unknown" | "fresh" | "stale";
/** Freshness carried by messages about a known book. */
export type KnownBookStatus = Exclude<BookStatus, "unknown">;
/** YES-space side: `bid` buys YES, `ask` sells YES. */
export type BookSide = "bid" | "ask";
/** One price level, `[price_e4, count_e2]`. */
export type PriceLevel = readonly [price_e4: PriceE4, count_e2: CountE2];

/* ---------------------------------------------------------------- REST (4.1) */

export interface PriceRange {
  readonly start_e4: PriceE4;
  readonly end_e4: PriceE4;
  readonly step_e4: PriceE4;
}

export interface MarketRow {
  readonly ticker: string;
  readonly event_ticker: string;
  readonly series_ticker: string;
  /** Event title, `null` until resolved. */
  readonly title: string | null;
  /** The market's YES subtitle, `null` until resolved. */
  readonly subtitle: string | null;
  /** Series category, `null` until resolved. */
  readonly category: string | null;
  readonly showcase: boolean;
  readonly volume_24h_e2: CountE2;
  /** Unix seconds. */
  readonly close_ts: number | null;
  readonly bid_e4: PriceE4 | null;
  readonly ask_e4: PriceE4 | null;
  readonly last_e4: PriceE4 | null;
  readonly book: BookStatus;
}

export interface DepthImage {
  readonly ts_ms: TsMs;
  /** Best 20 levels, best (highest) first. */
  readonly bids: readonly PriceLevel[];
  /** Best 20 levels, best (lowest) first. */
  readonly asks: readonly PriceLevel[];
}

export interface MarketDetail extends MarketRow {
  readonly price_ranges: readonly PriceRange[] | null;
  readonly depth: DepthImage | null;
}

export interface MarketsResponse {
  readonly markets: readonly MarketRow[];
}

export interface RecorderConnectionStatus {
  readonly conn_id: number;
  readonly taped: boolean;
  readonly frames: number;
  readonly gaps: number;
  readonly reconnects: number;
  readonly stale_books: number;
  readonly sink_dropped: number;
}

export interface RecorderStatus {
  readonly universe_size: number;
  readonly subscribed_markets: number;
  readonly connections: readonly RecorderConnectionStatus[];
}

export interface BusCounters {
  /** The publisher's start time in wall nanoseconds. Exceeds 2^53: an identifier, not a count. */
  readonly epoch: number | null;
  readonly last_seq: number;
  readonly messages: number;
  readonly resets: number;
  readonly missed: number;
  readonly books_known: number;
}

export interface ServiceStatus {
  readonly recording: boolean;
  readonly recorder_status_age_ms: number | null;
  readonly recorder: RecorderStatus | null;
  readonly bus: BusCounters;
  readonly clients: number;
}

export interface ApiErrorBody {
  readonly error: { readonly code: string; readonly message: string };
}

/* ------------------------------------------------------ WebSocket feed (4.2) */

/** The only client message: replace the subscription set. */
export interface SubscribeRequest {
  readonly op: "subscribe";
  readonly tickers: readonly string[];
}

/** A client message is at most this many bytes; larger ones close the socket with 1008. */
export const MAX_CLIENT_MESSAGE_BYTES = 4096;
/** A client sends at most this many messages per second; more closes the socket with 1008. */
export const MAX_CLIENT_MESSAGES_PER_SECOND = 10;

export type RejectionCode = OpenString<"unknown_ticker" | "too_many_tickers">;
export type ResyncReason = OpenString<"client_lag" | "bus_loss">;

export interface HelloMessage {
  readonly t: "hello";
  readonly protocol: number;
  readonly max_tickers: number;
  readonly bus_refresh_s: number;
}

export interface RejectedTicker {
  readonly ticker: string;
  readonly code: RejectionCode;
}

export interface SubscribedMessage {
  readonly t: "subscribed";
  readonly tickers: readonly string[];
  readonly rejected: readonly RejectedTicker[];
}

export interface SnapshotMessage {
  readonly t: "snapshot";
  readonly ticker: string;
  readonly book: KnownBookStatus;
  readonly ts_ms: TsMs;
  readonly bids: readonly PriceLevel[];
  readonly asks: readonly PriceLevel[];
}

export interface DeltaMessage {
  readonly t: "delta";
  readonly ticker: string;
  readonly ts_ms: TsMs;
  readonly side: BookSide;
  readonly price_e4: PriceE4;
  /** Signed change in contracts x 100. */
  readonly delta_e2: number;
}

export interface BookFreshnessMessage {
  readonly t: "book";
  readonly ticker: string;
  readonly book: KnownBookStatus;
}

export interface ResyncMessage {
  readonly t: "resync";
  readonly ticker: string;
  readonly reason: ResyncReason;
}

export interface TradeMessage {
  readonly t: "trade";
  readonly ticker: string;
  readonly ts_ms: TsMs;
  readonly price_e4: PriceE4;
  readonly count_e2: CountE2;
  /** `bid`: the taker bought YES. `ask`: the taker sold YES (bought NO). */
  readonly taker_side: BookSide;
}

export interface TickerMessage {
  readonly t: "ticker";
  readonly ticker: string;
  readonly ts_ms: TsMs;
  readonly bid_e4: PriceE4 | null;
  readonly ask_e4: PriceE4 | null;
  readonly last_e4: PriceE4 | null;
  readonly volume_e2: CountE2;
}

export interface ErrorMessage {
  readonly t: "error";
  readonly code: string;
  readonly message: string;
}

/** A server message about one market. */
export type MarketMessage =
  | SnapshotMessage
  | DeltaMessage
  | BookFreshnessMessage
  | ResyncMessage
  | TradeMessage
  | TickerMessage;

export type ServerMessage = HelloMessage | SubscribedMessage | ErrorMessage | MarketMessage;

/** Close codes with a meaning in protocol 1. */
export const CloseCode = {
  normal: 1000,
  goingAway: 1001,
  abnormal: 1006,
  policyViolation: 1008,
  tryAgainLater: 1013,
  tooSlow: 4000,
} as const;
