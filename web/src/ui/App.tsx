/**
 * The live view's composition root: API endpoints, the live feed, REST polling, URL
 * state, and the layout. Every dependency is constructed here and passed down.
 */

import { useCallback, useMemo, type CSSProperties } from "react";
import type { MarketRow } from "../api/protocol";
import { resolveEndpoints, RestClient } from "../api/rest";
import { PALETTE } from "../render/heatmapRenderer";
import { gridFromPriceRanges } from "../state/priceGrid";
import { DepthLadder } from "./DepthLadder";
import { HeatmapPanel } from "./HeatmapPanel";
import { useLiveFeed, useMarketStore, usePolling, useStoreSummary, useUrlSearch } from "./hooks";
import { Legend } from "./Legend";
import { MarketHeader } from "./MarketHeader";
import { MarketPicker } from "./MarketPicker";
import { RecentTrades } from "./RecentTrades";
import { StatusBar } from "./StatusBar";
import {
  rangeFromSearch,
  searchWithRange,
  searchWithTicker,
  tickerFromSearch,
  type PriceRangeMode,
} from "./url";

const MARKETS_LIMIT = 50;

function rgbCss(rgb: readonly number[]): string {
  const channel = (index: number): number => Math.round((rgb[index] ?? 0) * 255);
  return `rgb(${channel(0)} ${channel(1)} ${channel(2)})`;
}

const paletteVariables = {
  "--bid": rgbCss(PALETTE.bid),
  "--ask": rgbCss(PALETTE.ask),
} as CSSProperties;

function defaultTicker(rows: readonly MarketRow[]): string | null {
  return rows.find((row) => row.showcase)?.ticker ?? rows[0]?.ticker ?? null;
}

export function App() {
  const endpoints = useMemo(
    () => resolveEndpoints(window.location.href, import.meta.env.VITE_TAPE_API_ORIGIN),
    [],
  );
  const rest = useMemo(
    () =>
      new RestClient({ restBase: endpoints.restBase, fetch: (input, init) => fetch(input, init) }),
    [endpoints],
  );
  const { feed, connection } = useLiveFeed(endpoints.liveUrl);
  const [search, navigate] = useUrlSearch();

  const loadMarkets = useCallback(
    (signal: AbortSignal) => rest.listMarkets(MARKETS_LIMIT, signal),
    [rest],
  );
  const markets = usePolling(loadMarkets, 15_000);
  const loadStatus = useCallback((signal: AbortSignal) => rest.getStatus(signal), [rest]);
  const service = usePolling(loadStatus, 10_000);

  const rows = markets.data?.markets ?? [];
  const selectedTicker = tickerFromSearch(search) ?? defaultTicker(rows);
  const loadDetail = useCallback(
    (signal: AbortSignal) =>
      selectedTicker === null ? Promise.resolve(null) : rest.getMarket(selectedTicker, signal),
    [rest, selectedTicker],
  );
  const detail = usePolling(loadDetail, 30_000);
  const priceRanges = detail.data?.price_ranges ?? null;
  const grid = useMemo(() => gridFromPriceRanges(priceRanges), [priceRanges]);

  const store = useMarketStore(feed, selectedTicker, grid);
  const summary = useStoreSummary(store, 250);
  const row = rows.find((candidate) => candidate.ticker === selectedTicker) ?? detail.data ?? null;
  const mode = rangeFromSearch(search);
  const rejected = connection.rejected.find((entry) => entry.ticker === selectedTicker) ?? null;

  const selectMarket = useCallback(
    (ticker: string) => navigate(searchWithTicker(search, ticker)),
    [navigate, search],
  );
  const selectMode = useCallback(
    (next: PriceRangeMode) => navigate(searchWithRange(search, next)),
    [navigate, search],
  );

  return (
    <div className="app" style={paletteVariables}>
      <a className="skip-link" href="#live-view">
        Skip to the live view
      </a>
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true" />
          <span className="brand-name">tape</span>
          <span className="brand-tagline">Kalshi order books, live</span>
        </div>
        <StatusBar connection={connection} summary={summary} service={service} />
      </header>
      <div className="layout">
        <MarketPicker
          rows={rows}
          loading={markets.data === null && markets.error === null}
          error={markets.error}
          selectedTicker={selectedTicker}
          selectedSummary={summary}
          onSelect={selectMarket}
        />
        <main className="stage" id="live-view" tabIndex={-1}>
          {selectedTicker === null ? (
            <p className="empty-stage">
              {markets.error === null ? "Loading markets…" : "No market to show yet."}
            </p>
          ) : (
            <>
              <MarketHeader ticker={selectedTicker} row={row} summary={summary} />
              {detail.error?.code === "unknown_ticker" || rejected !== null ? (
                <p className="notice notice-error" role="alert">
                  {rejected?.code === "too_many_tickers"
                    ? "The live server refused this market: too many markets are open on this page."
                    : "This market is not recorded, so there is no live book to show."}
                </p>
              ) : null}
              <div className="stage-grid">
                <HeatmapPanel
                  store={store}
                  summary={summary}
                  connection={connection}
                  mode={mode}
                  onModeChange={selectMode}
                  label={row?.title ?? selectedTicker}
                />
                <aside className="side" aria-label="Order book and trades">
                  <DepthLadder summary={summary} />
                  <RecentTrades summary={summary} />
                </aside>
              </div>
              <Legend summary={summary} />
            </>
          )}
        </main>
      </div>
    </div>
  );
}
