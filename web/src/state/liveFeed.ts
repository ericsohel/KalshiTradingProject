/**
 * `LiveFeed`: the page's live data, wired together without React.
 *
 * It owns one `LiveClient` and one `MarketRouter`: market messages go to the stores, a
 * lost connection marks every book lost, and a store's inconsistent book asks the client
 * for a new snapshot. Watching a ticker creates its store and subscribes; unwatching
 * releases both. Listeners hear about connection changes and store creation, which lets
 * React read `connection` and `store()` through `useSyncExternalStore`.
 */

import { LiveClient, type ConnectionState, type SocketFactory } from "../api/live";
import { MarketRouter } from "./marketRouter";
import { ONE_CENT_GRID, type PriceGrid } from "./priceGrid";
import type { TapeStore } from "./tapeStore";

export interface LiveFeedOptions {
  readonly url: string;
  readonly createSocket: SocketFactory;
  /** Monotonic milliseconds. */
  readonly now: () => number;
  readonly random: () => number;
}

export class LiveFeed {
  readonly router: MarketRouter;
  readonly client: LiveClient;
  readonly #listeners = new Set<() => void>();

  constructor(options: LiveFeedOptions) {
    this.router = new MarketRouter({
      now: options.now,
      onBookInvalid: (ticker) => this.client.requestSnapshot(ticker),
    });
    this.client = new LiveClient({
      url: options.url,
      createSocket: options.createSocket,
      now: options.now,
      random: options.random,
      listener: {
        onMarketMessage: (message, receivedAtMs) => this.router.dispatch(message, receivedAtMs),
        onConnectionLost: (atMs) => this.router.connectionLost(atMs),
        onStateChange: () => this.#notify(),
      },
    });
  }

  get connection(): ConnectionState {
    return this.client.state;
  }

  /** Registers `listener`; returns its removal. Stable, so React may pass it directly. */
  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  start(): void {
    this.client.start();
  }

  stop(): void {
    this.client.stop();
  }

  /** Starts following `ticker`; the store keeps its grid if it already exists. */
  watch(ticker: string, grid: PriceGrid = ONE_CENT_GRID): TapeStore {
    const store = this.router.track(ticker, grid);
    this.client.setSubscription(this.router.tickers);
    this.#notify();
    return store;
  }

  unwatch(ticker: string): void {
    this.router.release(ticker);
    this.client.setSubscription(this.router.tickers);
    this.#notify();
  }

  setGrid(ticker: string, grid: PriceGrid): void {
    this.router.setGrid(ticker, grid);
  }

  store(ticker: string): TapeStore | null {
    return this.router.get(ticker) ?? null;
  }

  #notify(): void {
    for (const listener of this.#listeners) listener();
  }
}
