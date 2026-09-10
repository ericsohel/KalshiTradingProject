/**
 * The page's URL state: `?market=TICKER&range=full`. Parsing is strict, so a hand-edited
 * URL can never inject anything into an API path: a ticker must match the documented
 * pattern, which also rules out `.` and `..` path segments.
 */

import { TICKER_PATTERN } from "../api/protocol";

export type PriceRangeMode = "follow" | "full";

const MARKET_PARAM = "market";
const RANGE_PARAM = "range";
/** Longest ticker accepted from a URL; recorded tickers are under 50 characters. */
const MAX_TICKER_LENGTH = 96;

export function tickerFromSearch(search: string): string | null {
  const ticker = new URLSearchParams(search).get(MARKET_PARAM);
  if (ticker === null || ticker.length > MAX_TICKER_LENGTH) return null;
  return TICKER_PATTERN.test(ticker) ? ticker : null;
}

export function rangeFromSearch(search: string): PriceRangeMode {
  return new URLSearchParams(search).get(RANGE_PARAM) === "full" ? "full" : "follow";
}

/** `search` with the market replaced, other parameters kept; `?`-prefixed or empty. */
export function searchWithTicker(search: string, ticker: string): string {
  const params = new URLSearchParams(search);
  params.set(MARKET_PARAM, ticker);
  return `?${params.toString()}`;
}

/** `search` with the range mode replaced; the default mode is omitted. */
export function searchWithRange(search: string, mode: PriceRangeMode): string {
  const params = new URLSearchParams(search);
  if (mode === "full") params.set(RANGE_PARAM, "full");
  else params.delete(RANGE_PARAM);
  const text = params.toString();
  return text === "" ? "" : `?${text}`;
}
