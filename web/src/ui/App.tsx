/**
 * The live view's composition root: API endpoints, the live feed, REST polling, URL
 * state, and the layout. Every dependency is constructed here and passed down.
 */

import { useCallback, useMemo, type CSSProperties } from "react";
import type { MarketRow } from "../api/protocol";
import { resolveEndpoints, RestClient } from "../api/rest";
import { PALETTE } from "../render/heatmapRenderer";
import { gridFromPriceRanges } from "../state/priceGrid";
import { marketNotice, WAITING_FOR_MARKET_LIST } from "./availability";
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
/** The market list refreshes at this pace, so one published after the page loaded appears. */
const MARKETS_REFRESH_MS = 15_000;
const STATUS_REFRESH_MS = 10_000;
const DETAIL_REFRESH_MS = 30_000;

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
  const markets = usePolling(loadMarkets, MARKETS_REFRESH_MS);
  const loadStatus = useCallback((signal: AbortSignal) => rest.getStatus(signal), [rest]);
  const service = usePolling(loadStatus, STATUS_REFRESH_MS);

  const rows = markets.data?.markets ?? [];
  const selectedTicker = tickerFromSearch(search) ?? defaultTicker(rows);
  const loadDetail = useCallback(
    (signal: AbortSignal) =>
      selectedTicker === null ? Promise.resolve(null) : rest.getMarket(selectedTicker, signal),
    [rest, selectedTicker],
  );
  const detail = usePolling(loadDetail, DETAIL_REFRESH_MS);
  const priceRanges = detail.data?.price_ranges ?? null;
  const grid = useMemo(() => gridFromPriceRanges(priceRanges), [priceRanges]);

  const store = useMarketStore(feed, selectedTicker, grid);
  const summary = useStoreSummary(store, 250);
  const row = rows.find((candidate) => candidate.ticker === selectedTicker) ?? detail.data ?? null;
  const mode = rangeFromSearch(search);
  const listedMarkets = markets.data === null ? null : rows.length;
  const notice =
    selectedTicker === null
      ? null
      : marketNotice({
          listedMarkets,
          detailUnknown: detail.error?.code === "unknown_ticker",
          subscribed: connection.subscribed.includes(selectedTicker),
          rejection: connection.rejected.find((entry) => entry.ticker === selectedTicker) ?? null,
          retryAtMs: connection.rejectionRetryAtMs,
        });

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
          listed={listedMarkets}
          loading={markets.data === null && markets.error === null}
          error={markets.error}
          selectedTicker={selectedTicker}
          selectedSummary={summary}
          onSelect={selectMarket}
        />
        <main className="stage" id="live-view" tabIndex={-1}>
          {selectedTicker === null ? (
            <p className="empty-stage">
              {markets.error !== null
                ? "No market to show yet."
                : listedMarkets === 0
                  ? WAITING_FOR_MARKET_LIST
                  : "Loading markets…"}
            </p>
          ) : (
            <>
              <MarketHeader ticker={selectedTicker} row={row} summary={summary} />
              {notice !== null ? (
                <p
                  className={notice.tone === "error" ? "notice notice-error" : "notice"}
                  role={notice.tone === "error" ? "alert" : "status"}
                >
                  {notice.text}
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
