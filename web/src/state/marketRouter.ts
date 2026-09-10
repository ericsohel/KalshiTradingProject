/**
 * Routes live market messages to the `TapeStore` of each subscribed market.
 *
 * The router owns the stores' lifetimes: a store exists from `track` until `release`.
 * Messages for untracked tickers are dropped (they arrive briefly after an unsubscribe).
 * A store that finds its book inconsistent triggers `onBookInvalid`, which the page wires
 * to `LiveClient.requestSnapshot`.
 */

import type { MarketMessage } from "../api/protocol";
import type { PriceGrid } from "./priceGrid";
import { TapeStore } from "./tapeStore";

export interface MarketRouterOptions {
  /** Monotonic milliseconds. */
  readonly now: () => number;
  readonly onBookInvalid: (ticker: string) => void;
  readonly binMs?: number;
  readonly historyColumns?: number;
  readonly tradeCapacity?: number;
}

export class MarketRouter {
  readonly #options: MarketRouterOptions;
  readonly #stores = new Map<string, TapeStore>();

  constructor(options: MarketRouterOptions) {
    this.#options = options;
  }

  /** The store for `ticker`, created with `grid` on first use and returned unchanged after. */
  track(ticker: string, grid: PriceGrid): TapeStore {
    const existing = this.#stores.get(ticker);
    if (existing !== undefined) return existing;
    const { binMs, historyColumns, tradeCapacity } = this.#options;
    const store = new TapeStore({
      ticker,
      grid,
      originMs: this.#options.now(),
      ...(binMs === undefined ? {} : { binMs }),
      ...(historyColumns === undefined ? {} : { historyColumns }),
      ...(tradeCapacity === undefined ? {} : { tradeCapacity }),
    });
    this.#stores.set(ticker, store);
    return store;
  }

  /** Forgets `ticker` and its history. */
  release(ticker: string): void {
    this.#stores.delete(ticker);
  }

  get(ticker: string): TapeStore | undefined {
    return this.#stores.get(ticker);
  }

  /** Moves `ticker`'s store to `grid` (see `TapeStore.setGrid`); no-op when untracked. */
  setGrid(ticker: string, grid: PriceGrid): void {
    this.#stores.get(ticker)?.setGrid(grid, this.#options.now());
  }

  get tickers(): string[] {
    return [...this.#stores.keys()];
  }

  dispatch(message: MarketMessage, receivedAtMs: number): void {
    const store = this.#stores.get(message.ticker);
    if (store === undefined) return;
    if (store.apply(message, receivedAtMs) === "book_invalid") {
      this.#options.onBookInvalid(message.ticker);
    }
  }

  connectionLost(atMs: number): void {
    for (const store of this.#stores.values()) store.markConnectionLost(atMs);
  }
}
