/**
 * Whether the selected market can be shown, in words, from what the REST poll and the
 * live feed report. Pure, so every case is tested without React.
 *
 * Before the recorder's first catalog reaches the API, `/markets` is empty and every
 * market is unknown to it; the live client keeps asking (see `LiveClient`), so that state
 * reads as waiting, not as a market that is not recorded.
 */

import type { Rejection } from "../api/protocol";

export interface Notice {
  readonly tone: "idle" | "error";
  readonly text: string;
}

export interface MarketAvailability {
  /** Rows in the latest market list, or `null` before it loaded. */
  readonly listedMarkets: number | null;
  /** The market's detail request was answered with `unknown_ticker`. */
  readonly detailUnknown: boolean;
  /** The live server accepted this market in its latest `subscribed` reply. */
  readonly subscribed: boolean;
  /** The live server's latest rejection of this market, if any. */
  readonly rejection: Rejection | null;
  /** When the live client asks again for markets rejected as unknown, or `null`. */
  readonly retryAtMs: number | null;
}

/** Shown while the API has no market list, in the picker and in place of a market. */
export const WAITING_FOR_MARKET_LIST =
  "Waiting for the recorder's market list. It reaches the live server within one refresh cycle of the recorder starting, and this page updates on its own.";

/**
 * The notice for the market list itself.
 *
 * @returns `null` when rows are listed or still loading.
 */
export function marketListNotice(listedMarkets: number | null, failed: boolean): Notice | null {
  if (failed || listedMarkets === null || listedMarkets > 0) return null;
  return { tone: "idle", text: WAITING_FOR_MARKET_LIST };
}

/**
 * The notice for the selected market.
 *
 * @returns `null` when nothing stands in the way of showing it.
 */
export function marketNotice(state: MarketAvailability): Notice | null {
  if (state.subscribed) return null;
  const { rejection } = state;
  if (rejection?.code === "too_many_tickers") {
    return {
      tone: "error",
      text: "The live server refused this market: too many markets are open on this page.",
    };
  }
  if (rejection !== null && rejection.code !== "unknown_ticker") {
    return { tone: "error", text: `The live server refused this market (${rejection.code}).` };
  }
  if (rejection === null && !state.detailUnknown) return null;
  if (state.listedMarkets === 0) return { tone: "idle", text: WAITING_FOR_MARKET_LIST };
  if (state.retryAtMs !== null) {
    return {
      tone: "idle",
      text: "The live server does not list this market yet. Asking again shortly.",
    };
  }
  return { tone: "error", text: "This market is not recorded, so there is no live book to show." };
}
