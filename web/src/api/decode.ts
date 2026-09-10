/**
 * Runtime decoding of everything the API sends (docs/FRONTEND.md section 4).
 *
 * Parse, don't validate: raw JSON becomes a typed value exactly once, here, and nothing
 * downstream re-checks it. Every exported function is total: it returns a result and
 * never throws, so a malformed message is counted and dropped instead of crashing the
 * page. Invariants checked: prices are integers in 0..10,000; counts are non-negative
 * safe integers; levels are best first with no repeated price; closed enums (sides and
 * book status) hold only known values, while open codes accept any string (ADR 0017).
 * Unknown fields are ignored and unknown message types are reported as `ignored`, so a
 * newer server stays compatible.
 */

import {
  MAX_PRICE_E4,
  type ApiErrorBody,
  type BookSide,
  type BookStatus,
  type BusCounters,
  type DepthImage,
  type KnownBookStatus,
  type MarketDetail,
  type MarketRow,
  type PriceLevel,
  type PriceRange,
  type RecorderConnectionStatus,
  type RecorderStatus,
  type RejectedTicker,
  type ServerMessage,
  type ServiceStatus,
} from "./protocol";

export type DecodeResult<T> =
  { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: string };

/** The outcome of decoding one WebSocket text frame. */
export type FrameDecoding =
  | { readonly kind: "message"; readonly message: ServerMessage }
  | { readonly kind: "ignored"; readonly type: string }
  | { readonly kind: "malformed"; readonly error: string };

/** Raised inside this module only; every export converts it into a result. */
class DecodeFailure extends Error {
  override readonly name = "DecodeFailure";
}

type Fields = Readonly<Record<string, unknown>>;

function fail(path: string, expectation: string): never {
  throw new DecodeFailure(`${path}: expected ${expectation}`);
}

function record(value: unknown, path: string): Fields {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    fail(path, "an object");
  }
  return value as Fields;
}

function array(value: unknown, path: string): readonly unknown[] {
  if (!Array.isArray(value)) fail(path, "an array");
  return value;
}

function text(fields: Fields, key: string, path: string): string {
  const value = fields[key];
  if (typeof value !== "string") fail(`${path}.${key}`, "a string");
  return value;
}

function nonEmptyText(fields: Fields, key: string, path: string): string {
  const value = text(fields, key, path);
  if (value.length === 0) fail(`${path}.${key}`, "a non-empty string");
  return value;
}

function nullableText(fields: Fields, key: string, path: string): string | null {
  return fields[key] === null ? null : text(fields, key, path);
}

function flag(fields: Fields, key: string, path: string): boolean {
  const value = fields[key];
  if (typeof value !== "boolean") fail(`${path}.${key}`, "a boolean");
  return value;
}

function integerValue(value: unknown, path: string, min: number, max: number): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) {
    fail(path, `an integer in ${min}..${max}`);
  }
  return value;
}

function integer(
  fields: Fields,
  key: string,
  path: string,
  min = 0,
  max = Number.MAX_SAFE_INTEGER,
): number {
  return integerValue(fields[key], `${path}.${key}`, min, max);
}

function nullableInteger(fields: Fields, key: string, path: string): number | null {
  return fields[key] === null ? null : integer(fields, key, path);
}

function price(fields: Fields, key: string, path: string): number {
  return integer(fields, key, path, 0, MAX_PRICE_E4);
}

function nullablePrice(fields: Fields, key: string, path: string): number | null {
  return fields[key] === null ? null : price(fields, key, path);
}

function oneOf<T extends string>(
  fields: Fields,
  key: string,
  path: string,
  allowed: readonly T[],
): T {
  const value = fields[key];
  if (typeof value !== "string" || !(allowed as readonly string[]).includes(value)) {
    fail(`${path}.${key}`, `one of ${allowed.join(", ")}`);
  }
  return value as T;
}

const BOOK_STATUSES: readonly BookStatus[] = ["unknown", "fresh", "stale"];
const KNOWN_BOOK_STATUSES: readonly KnownBookStatus[] = ["fresh", "stale"];
const SIDES: readonly BookSide[] = ["bid", "ask"];

/**
 * Decodes `[[price_e4, count_e2], ...]`, best first: strictly descending prices for
 * bids and strictly ascending for asks, which also rules out a repeated price.
 */
function levels(fields: Fields, key: string, path: string, side: BookSide): PriceLevel[] {
  const items = array(fields[key], `${path}.${key}`);
  const decoded: PriceLevel[] = [];
  let previous: number | null = null;
  items.forEach((item, index) => {
    const itemPath = `${path}.${key}[${index}]`;
    const pair = array(item, itemPath);
    if (pair.length !== 2) fail(itemPath, "a [price_e4, count_e2] pair");
    const levelPrice = integerValue(pair[0], `${itemPath}[0]`, 0, MAX_PRICE_E4);
    const levelCount = integerValue(pair[1], `${itemPath}[1]`, 0, Number.MAX_SAFE_INTEGER);
    if (previous !== null) {
      const ordered = side === "bid" ? levelPrice < previous : levelPrice > previous;
      if (!ordered) fail(itemPath, `${side === "bid" ? "descending" : "ascending"} prices`);
    }
    previous = levelPrice;
    decoded.push([levelPrice, levelCount]);
  });
  return decoded;
}

function tickerList(fields: Fields, key: string, path: string): string[] {
  return array(fields[key], `${path}.${key}`).map((item, index) => {
    if (typeof item !== "string" || item.length === 0) {
      fail(`${path}.${key}[${index}]`, "a ticker");
    }
    return item;
  });
}

function marketRowFields(fields: Fields, path: string): MarketRow {
  return {
    ticker: nonEmptyText(fields, "ticker", path),
    event_ticker: text(fields, "event_ticker", path),
    series_ticker: text(fields, "series_ticker", path),
    title: nullableText(fields, "title", path),
    subtitle: nullableText(fields, "subtitle", path),
    category: nullableText(fields, "category", path),
    showcase: flag(fields, "showcase", path),
    volume_24h_e2: integer(fields, "volume_24h_e2", path),
    close_ts: nullableInteger(fields, "close_ts", path),
    bid_e4: nullablePrice(fields, "bid_e4", path),
    ask_e4: nullablePrice(fields, "ask_e4", path),
    last_e4: nullablePrice(fields, "last_e4", path),
    book: oneOf(fields, "book", path, BOOK_STATUSES),
  };
}

function priceRange(value: unknown, path: string): PriceRange {
  const fields = record(value, path);
  const range: PriceRange = {
    start_e4: price(fields, "start_e4", path),
    end_e4: price(fields, "end_e4", path),
    step_e4: integer(fields, "step_e4", path, 1, MAX_PRICE_E4),
  };
  if (range.end_e4 <= range.start_e4) fail(path, "end_e4 greater than start_e4");
  return range;
}

function depthImage(value: unknown, path: string): DepthImage {
  const fields = record(value, path);
  return {
    ts_ms: integer(fields, "ts_ms", path),
    bids: levels(fields, "bids", path, "bid"),
    asks: levels(fields, "asks", path, "ask"),
  };
}

function connectionStatus(value: unknown, path: string): RecorderConnectionStatus {
  const fields = record(value, path);
  return {
    conn_id: integer(fields, "conn_id", path),
    taped: flag(fields, "taped", path),
    frames: integer(fields, "frames", path),
    gaps: integer(fields, "gaps", path),
    reconnects: integer(fields, "reconnects", path),
    stale_books: integer(fields, "stale_books", path),
    sink_dropped: integer(fields, "sink_dropped", path),
  };
}

function recorderStatus(value: unknown, path: string): RecorderStatus {
  const fields = record(value, path);
  return {
    universe_size: integer(fields, "universe_size", path),
    subscribed_markets: integer(fields, "subscribed_markets", path),
    connections: array(fields["connections"], `${path}.connections`).map((item, index) =>
      connectionStatus(item, `${path}.connections[${index}]`),
    ),
  };
}

function busCounters(value: unknown, path: string): BusCounters {
  const fields = record(value, path);
  const epoch = fields["epoch"];
  if (epoch !== null && (typeof epoch !== "number" || !Number.isInteger(epoch) || epoch < 0)) {
    fail(`${path}.epoch`, "a non-negative integer or null");
  }
  return {
    epoch,
    last_seq: integer(fields, "last_seq", path),
    messages: integer(fields, "messages", path),
    resets: integer(fields, "resets", path),
    missed: integer(fields, "missed", path),
    books_known: integer(fields, "books_known", path),
  };
}

function serverMessage(fields: Fields, type: string): ServerMessage | null {
  const path = type;
  switch (type) {
    case "hello":
      return {
        t: "hello",
        protocol: integer(fields, "protocol", path, 1),
        max_tickers: integer(fields, "max_tickers", path, 1),
        bus_refresh_s: integer(fields, "bus_refresh_s", path, 1),
      };
    case "subscribed":
      return {
        t: "subscribed",
        tickers: tickerList(fields, "tickers", path),
        rejected: array(fields["rejected"], `${path}.rejected`).map((item, index) => {
          const itemPath = `${path}.rejected[${index}]`;
          const rejected = record(item, itemPath);
          const entry: RejectedTicker = {
            ticker: nonEmptyText(rejected, "ticker", itemPath),
            code: text(rejected, "code", itemPath),
          };
          return entry;
        }),
      };
    case "snapshot":
      return {
        t: "snapshot",
        ticker: nonEmptyText(fields, "ticker", path),
        book: oneOf(fields, "book", path, KNOWN_BOOK_STATUSES),
        ts_ms: integer(fields, "ts_ms", path),
        bids: levels(fields, "bids", path, "bid"),
        asks: levels(fields, "asks", path, "ask"),
      };
    case "delta":
      return {
        t: "delta",
        ticker: nonEmptyText(fields, "ticker", path),
        ts_ms: integer(fields, "ts_ms", path),
        side: oneOf(fields, "side", path, SIDES),
        price_e4: price(fields, "price_e4", path),
        delta_e2: integer(fields, "delta_e2", path, Number.MIN_SAFE_INTEGER),
      };
    case "book":
      return {
        t: "book",
        ticker: nonEmptyText(fields, "ticker", path),
        book: oneOf(fields, "book", path, KNOWN_BOOK_STATUSES),
      };
    case "resync":
      return {
        t: "resync",
        ticker: nonEmptyText(fields, "ticker", path),
        reason: text(fields, "reason", path),
      };
    case "trade":
      return {
        t: "trade",
        ticker: nonEmptyText(fields, "ticker", path),
        ts_ms: integer(fields, "ts_ms", path),
        price_e4: price(fields, "price_e4", path),
        count_e2: integer(fields, "count_e2", path, 1),
        taker_side: oneOf(fields, "taker_side", path, SIDES),
      };
    case "ticker":
      return {
        t: "ticker",
        ticker: nonEmptyText(fields, "ticker", path),
        ts_ms: integer(fields, "ts_ms", path),
        bid_e4: nullablePrice(fields, "bid_e4", path),
        ask_e4: nullablePrice(fields, "ask_e4", path),
        last_e4: nullablePrice(fields, "last_e4", path),
        volume_e2: integer(fields, "volume_e2", path),
      };
    case "error":
      return {
        t: "error",
        code: text(fields, "code", path),
        message: text(fields, "message", path),
      };
    default:
      return null;
  }
}

function attempt<T>(decode: () => T): DecodeResult<T> {
  try {
    return { ok: true, value: decode() };
  } catch (error: unknown) {
    if (error instanceof DecodeFailure) return { ok: false, error: error.message };
    throw error;
  }
}

/**
 * Decodes one WebSocket text frame.
 *
 * @param data The frame payload as delivered by the socket (anything but a string is
 *   malformed: protocol 1 sends text frames only).
 * @returns The typed message; `ignored` for a well-formed message of a type this client
 *   does not know; `malformed` with the first violation otherwise.
 */
export function decodeFrame(data: unknown): FrameDecoding {
  if (typeof data !== "string") return { kind: "malformed", error: "frame: expected text" };
  let parsed: unknown;
  try {
    parsed = JSON.parse(data);
  } catch {
    return { kind: "malformed", error: "frame: invalid JSON" };
  }
  const result = attempt(() => {
    const fields = record(parsed, "message");
    const type = text(fields, "t", "message");
    return { type, message: serverMessage(fields, type) };
  });
  if (!result.ok) return { kind: "malformed", error: result.error };
  const { type, message } = result.value;
  return message === null ? { kind: "ignored", type } : { kind: "message", message };
}

/** The markets that decoded, and how many rows were dropped as malformed. */
export interface MarketList {
  readonly markets: readonly MarketRow[];
  readonly invalidRows: number;
}

/**
 * Decodes `GET /markets`. One malformed row is dropped and counted rather than failing
 * the whole list; a malformed envelope fails.
 */
export function decodeMarketList(json: unknown): DecodeResult<MarketList> {
  return attempt(() => {
    const rows = array(record(json, "response")["markets"], "response.markets");
    const markets: MarketRow[] = [];
    for (const [index, row] of rows.entries()) {
      const decoded = attempt(() => marketRowFields(record(row, `markets[${index}]`), "row"));
      if (decoded.ok) markets.push(decoded.value);
    }
    return { markets, invalidRows: rows.length - markets.length };
  });
}

/** Decodes `GET /markets/{ticker}`. */
export function decodeMarketDetail(json: unknown): DecodeResult<MarketDetail> {
  return attempt(() => {
    const fields = record(json, "market");
    const ranges = fields["price_ranges"];
    const depth = fields["depth"];
    return {
      ...marketRowFields(fields, "market"),
      price_ranges:
        ranges === null
          ? null
          : array(ranges, "market.price_ranges").map((item, index) =>
              priceRange(item, `market.price_ranges[${index}]`),
            ),
      depth: depth === null ? null : depthImage(depth, "market.depth"),
    };
  });
}

/** Decodes `GET /status`. */
export function decodeServiceStatus(json: unknown): DecodeResult<ServiceStatus> {
  return attempt(() => {
    const fields = record(json, "status");
    return {
      recording: flag(fields, "recording", "status"),
      recorder_status_age_ms: nullableInteger(fields, "recorder_status_age_ms", "status"),
      recorder:
        fields["recorder"] === null ? null : recorderStatus(fields["recorder"], "status.recorder"),
      bus: busCounters(fields["bus"], "status.bus"),
      clients: integer(fields, "clients", "status"),
    };
  });
}

/** Decodes the `{"error": {"code", "message"}}` body of a failed request. */
export function decodeApiError(json: unknown): DecodeResult<ApiErrorBody> {
  return attempt(() => {
    const error = record(record(json, "body")["error"], "body.error");
    return {
      error: {
        code: text(error, "code", "body.error"),
        message: text(error, "message", "body.error"),
      },
    };
  });
}
