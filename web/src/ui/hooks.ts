/**
 * React bindings for the framework-free stores: polling REST, reading store summaries,
 * the URL, and the live feed. Every external value is read with `useSyncExternalStore`
 * or set from asynchronous callbacks, never synchronously inside an effect.
 */

import { useCallback, useEffect, useState, useSyncExternalStore } from "react";
import { browserSocketFactory, type ConnectionState } from "../api/live";
import { ApiRequestError } from "../api/rest";
import { LiveFeed } from "../state/liveFeed";
import type { PriceGrid } from "../state/priceGrid";
import type { MarketSummary, TapeStore } from "../state/tapeStore";
import { monotonicNow } from "./clock";

export interface Polled<T> {
  readonly data: T | null;
  readonly error: ApiRequestError | null;
}

interface PollState<T> extends Polled<T> {
  readonly load: (signal: AbortSignal) => Promise<T>;
}

/**
 * Calls `load` now and every `intervalMs` after the previous call settles. A new `load`
 * (keep it stable with `useCallback`) restarts polling and hides the previous data.
 * Errors keep the last good data alongside the error.
 */
export function usePolling<T>(
  load: (signal: AbortSignal) => Promise<T>,
  intervalMs: number,
): Polled<T> {
  const [state, setState] = useState<PollState<T>>({ load, data: null, error: null });
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const run = async (): Promise<void> => {
      try {
        const data = await load(controller.signal);
        if (!controller.signal.aborted) setState({ load, data, error: null });
      } catch (error: unknown) {
        if (controller.signal.aborted) return;
        const failure =
          error instanceof ApiRequestError
            ? error
            : new ApiRequestError(
                "network",
                null,
                error instanceof Error ? error.message : "failed",
              );
        setState((previous) => ({
          load,
          data: previous.load === load ? previous.data : null,
          error: failure,
        }));
      }
      if (!controller.signal.aborted) timer = setTimeout(() => void run(), intervalMs);
    };
    void run();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [load, intervalMs]);
  return state.load === load ? state : { data: null, error: null };
}

function intervalSubscription(intervalMs: number): (onChange: () => void) => () => void {
  return (onChange) => {
    const timer = setInterval(onChange, intervalMs);
    return () => clearInterval(timer);
  };
}

/** The store's summary, re-read at most every `intervalMs`. */
export function useStoreSummary(store: TapeStore | null, intervalMs: number): MarketSummary | null {
  const subscribe = useCallback(
    (onChange: () => void) => intervalSubscription(intervalMs)(onChange),
    [intervalMs],
  );
  const snapshot = useCallback(() => store?.summary() ?? null, [store]);
  return useSyncExternalStore(subscribe, snapshot);
}

/** The monotonic clock, quantized to `intervalMs` so renders are not wasted. */
export function useNow(intervalMs: number): number {
  const subscribe = useCallback(
    (onChange: () => void) => intervalSubscription(intervalMs)(onChange),
    [intervalMs],
  );
  const snapshot = useCallback(
    () => Math.floor(monotonicNow() / intervalMs) * intervalMs,
    [intervalMs],
  );
  return useSyncExternalStore(subscribe, snapshot);
}

const NAVIGATE_EVENT = "tape:navigate";

function subscribeToLocation(onChange: () => void): () => void {
  window.addEventListener("popstate", onChange);
  window.addEventListener(NAVIGATE_EVENT, onChange);
  return () => {
    window.removeEventListener("popstate", onChange);
    window.removeEventListener(NAVIGATE_EVENT, onChange);
  };
}

function currentSearch(): string {
  return window.location.search;
}

/** The URL's query string and a function that pushes a new one onto history. */
export function useUrlSearch(): readonly [string, (search: string) => void] {
  const search = useSyncExternalStore(subscribeToLocation, currentSearch);
  const navigate = useCallback((next: string) => {
    if (next === window.location.search) return;
    const { pathname, hash } = window.location;
    window.history.pushState(null, "", `${pathname}${next}${hash}`);
    window.dispatchEvent(new Event(NAVIGATE_EVENT));
  }, []);
  return [search, navigate];
}

/** One live feed for the page's lifetime, connected while mounted. */
export function useLiveFeed(liveUrl: string): { feed: LiveFeed; connection: ConnectionState } {
  const [feed] = useState(
    () =>
      new LiveFeed({
        url: liveUrl,
        createSocket: browserSocketFactory,
        now: monotonicNow,
        random: Math.random,
      }),
  );
  useEffect(() => {
    feed.start();
    return () => feed.stop();
  }, [feed]);
  const connection = useSyncExternalStore(feed.subscribe, () => feed.connection);
  return { feed, connection };
}

/** Watches `ticker` while mounted and keeps its store on `grid`. */
export function useMarketStore(
  feed: LiveFeed,
  ticker: string | null,
  grid: PriceGrid,
): TapeStore | null {
  useEffect(() => {
    if (ticker === null) return undefined;
    feed.watch(ticker, grid);
    return () => feed.unwatch(ticker);
    // The grid only seeds a new store; later grid changes go through setGrid below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [feed, ticker]);
  useEffect(() => {
    if (ticker !== null) feed.setGrid(ticker, grid);
  }, [feed, ticker, grid]);
  const snapshot = useCallback(() => (ticker === null ? null : feed.store(ticker)), [feed, ticker]);
  return useSyncExternalStore(feed.subscribe, snapshot);
}
