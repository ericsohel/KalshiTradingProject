/**
 * The `tape serve` live API as the page uses it, protocol 1 (docs/FRONTEND.md section 4).
 *
 * Every API type is derived from `schema.gen.ts`, which `npm run api-types` generates from
 * `schema.json`, which `scripts/gen_api_schema.py` generates from the msgspec structs in
 * `tape.api.contract`; those structs are the source of truth (ADR 0023). This file only
 * adapts the generated types: every one is deeply read-only, and the taxonomies a newer
 * server may extend (rejection and resync codes) are open strings, as in ADR 0017, while
 * directional values (sides, book states) stay closed. The decoders in `decode.ts` produce
 * exactly these types, so a contract change the page does not handle fails to compile.
 * Nothing else defines an API type; the constants below are the contract's numbers that a
 * JSON Schema cannot carry.
 *
 * Units follow docs/DATA_FORMATS.md: `price_e4` is a YES price in 1/10,000 dollar
 * (0..10,000), `count_e2` is contracts x 100, `ts_ms` is exchange time in milliseconds.
 * `null` means the value is not known.
 */

import type * as Generated from "./schema.gen.ts";

/** The protocol number this client speaks; `hello.protocol` must match. */
export const PROTOCOL_VERSION = 1;

/** Largest valid `price_e4`: one dollar. */
export const MAX_PRICE_E4 = 10_000;

/**
 * A market ticker (docs/DATA_FORMATS.md 1.4): upper-case letters, digits, hyphens, and the
 * dots of strike values such as `KXAAAGASD-26SEP11-4.2700`, starting with a letter or digit.
 */
export const TICKER_PATTERN = /^[A-Z0-9][A-Z0-9.-]*$/;

/** A client message is at most this many bytes; larger ones close the socket with 1008. */
export const MAX_CLIENT_MESSAGE_BYTES = 4096;
/** A client sends at most this many messages per second; more closes the socket with 1008. */
export const MAX_CLIENT_MESSAGES_PER_SECOND = 10;

/** Close codes with a meaning in protocol 1. */
export const CloseCode = {
  normal: 1000,
  goingAway: 1001,
  abnormal: 1006,
  policyViolation: 1008,
  tryAgainLater: 1013,
  tooSlow: 4000,
} as const;

/** A YES price in 1/10,000 dollar, an integer in 0..10,000. */
export type PriceE4 = number;
/** A contract count x 100, a non-negative integer. */
export type CountE2 = number;
/** Exchange event time, milliseconds since the Unix epoch. */
export type TsMs = number;

/** A generated type with every property, array, and tuple read-only, however deep. */
type Immutable<T> = T extends string | number | boolean | null
  ? T
  : { readonly [K in keyof T]: Immutable<T[K]> };

/** A string union that still accepts values a newer server may add (ADR 0017). */
export type OpenString<Known extends string> = Known | (string & Record<never, never>);

/* ---------------------------------------------------------------- REST (4.1) */

export type PriceRange = Immutable<Generated.PriceRange>;
export type MarketRow = Immutable<Generated.MarketRow>;
/** The best 20 levels per side, best first; `ts_ms` is `null` when the book has no exchange time. */
export type Depth = Immutable<Generated.Depth>;
export type MarketDetail = Immutable<Generated.MarketDetail>;
export type MarketsResponse = Immutable<Generated.MarketsResponse>;
export type ConnectionHealth = Immutable<Generated.ConnectionHealth>;
export type RecorderHealth = Immutable<Generated.RecorderHealth>;
/** `epoch` is a decimal string: it identifies a recorder run and exceeds 2^53, so compare it only. */
export type BusHealth = Immutable<Generated.BusHealth>;
export type ServiceStatus = Immutable<Generated.ServiceStatus>;
export type ErrorResponse = Immutable<Generated.ErrorResponse>;

/** Freshness of a market's book as the API reports it. */
export type BookState = MarketRow["book"];
/** YES-space side: `bid` buys YES, `ask` sells YES. */
export type BookSide = Generated.DeltaMessage["side"];
/** One price level, `[price_e4, count_e2]`. */
export type PriceLevel = Depth["bids"][number];

/* ------------------------------------------------------ WebSocket feed (4.2) */

/** The only client message: replace the subscription set. */
export type SubscribeRequest = Immutable<Generated.SubscribeRequest>;

export type RejectionCode = OpenString<Generated.Rejection["code"]>;
export type ResyncReason = OpenString<Generated.ResyncMessage["reason"]>;

export type HelloMessage = Immutable<Generated.HelloMessage>;
export type Rejection = Immutable<Omit<Generated.Rejection, "code"> & { code: RejectionCode }>;
export type SubscribedMessage = Immutable<
  Omit<Generated.SubscribedMessage, "rejected"> & { rejected: Rejection[] }
>;
export type SnapshotMessage = Immutable<Generated.SnapshotMessage>;
export type DeltaMessage = Immutable<Generated.DeltaMessage>;
export type BookMessage = Immutable<Generated.BookMessage>;
export type ResyncMessage = Immutable<
  Omit<Generated.ResyncMessage, "reason"> & { reason: ResyncReason }
>;
export type TradeMessage = Immutable<Generated.TradeMessage>;
export type TickerMessage = Immutable<Generated.TickerMessage>;
export type ErrorMessage = Immutable<Generated.ErrorMessage>;

/** Freshness carried by messages about a known book. */
export type KnownBookState = SnapshotMessage["book"];

/** One generated server message as the page sees it. */
type PageMessage<Message> = Message extends { t: "subscribed" }
  ? SubscribedMessage
  : Message extends { t: "resync" }
    ? ResyncMessage
    : Immutable<Message>;

/** Every server message; a message type added to the contract joins this union by itself. */
export type ServerMessage = PageMessage<Generated.ServerMessage>;

/** A server message about one market. */
export type MarketMessage = Exclude<ServerMessage, { t: "hello" | "subscribed" | "error" }>;
