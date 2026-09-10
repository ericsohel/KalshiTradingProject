/**
 * `TapeStore`: everything the page knows about one live market.
 *
 * It applies the feed's market messages in bus order to an integer book, records the
 * book's depth into a time-binned ring for the heatmap, keeps best bid and ask per bin,
 * and keeps recent trades. React reads `summary()`, an immutable object rebuilt only
 * when something changed; the renderer reads the arrays through `HeatmapSource`.
 *
 * Book rules (FRONTEND 4.2): a snapshot replaces the book and restores it; a delta
 * applies only to a fresh book; `book` changes freshness of a known book; `resync`, a
 * lost connection, or a delta that would drive a level negative discard the book, and the
 * heatmap marks the interval as a gap until the next snapshot. Time on the heatmap is the
 * local receive time on a monotonic clock, binned from `originMs`.
 */

import type { BookState, MarketMessage, PriceLevel, TickerMessage } from "../api/protocol";
import { ColumnStatus, type ColumnStatusCode, type HeatmapSource } from "../render/source";
import { Book } from "./book";
import { DepthHistory } from "./depthHistory";
import { rowForPrice, sameRows, type PriceGrid } from "./priceGrid";
import { TradeTape, type TradeRecord } from "./tradeTape";

/** Milliseconds per heatmap column. */
export const DEFAULT_BIN_MS = 250;
/** Columns kept: 2,048 x 250 ms is about 8.5 minutes, within WebGL2's minimum texture size. */
export const DEFAULT_HISTORY_COLUMNS = 2048;
/** Trade bubbles kept: above the 20,000-on-screen budget of FRONTEND 5. */
export const DEFAULT_TRADE_CAPACITY = 32_768;
/** Levels per side in the summary's ladder. */
export const LADDER_LEVELS = 10;

export interface TapeStoreOptions {
  readonly ticker: string;
  readonly grid: PriceGrid;
  /** Monotonic time of bin 0. */
  readonly originMs: number;
  readonly binMs?: number;
  readonly historyColumns?: number;
  readonly tradeCapacity?: number;
}

export type ApplyOutcome =
  | "applied"
  /** Not applicable to the current book (a delta to a stale or unknown book). */
  | "ignored"
  /** The message showed the local book to be wrong; it was discarded. */
  | "book_invalid";

export interface ResyncState {
  readonly reason: string;
  readonly sinceMs: number;
}

export interface TickerState {
  readonly receivedAtMs: number;
  readonly tsMs: number;
  readonly bidE4: number | null;
  readonly askE4: number | null;
  readonly lastE4: number | null;
  readonly volumeE2: number;
}

export interface StoreCounters {
  readonly snapshots: number;
  readonly deltas: number;
  readonly trades: number;
  readonly resyncs: number;
  readonly ignoredDeltas: number;
  readonly invalidBooks: number;
}

/** An immutable view of a store for display. */
export interface MarketSummary {
  readonly ticker: string;
  readonly version: number;
  readonly grid: PriceGrid;
  /** The local book: `unknown` until a snapshot, and again after it is discarded. */
  readonly book: BookState;
  /** A known book was lost and no snapshot has restored it yet. */
  readonly gap: boolean;
  readonly resync: ResyncState | null;
  readonly bestBidE4: number | null;
  readonly bestAskE4: number | null;
  /** Best first. */
  readonly bids: readonly PriceLevel[];
  /** Best first. */
  readonly asks: readonly PriceLevel[];
  readonly tickerUpdate: TickerState | null;
  /** Newest first. */
  readonly recentTrades: readonly TradeRecord[];
  readonly lastBookChangeAtMs: number | null;
  readonly maxRowContracts: number;
  /** Largest single trade this session, contracts; sizes the bubble legend. */
  readonly maxTradeContracts: number;
  readonly counters: StoreCounters;
}

export class TapeStore implements HeatmapSource {
  readonly ticker: string;
  readonly binMs: number;
  readonly originMs: number;
  readonly #historyColumns: number;
  readonly #book = new Book();
  readonly #trades: TradeTape;

  #grid: PriceGrid;
  #history: DepthHistory;
  /** Resting contracts x 100 per row, bids and asks together. */
  #rowTotals: Float64Array;
  #maxRowE2 = 0;

  #status: BookState = "unknown";
  #gap = false;
  #resync: ResyncState | null = null;
  #ticker: TickerState | null = null;
  #lastBookChangeAtMs: number | null = null;
  #counters: StoreCounters = {
    snapshots: 0,
    deltas: 0,
    trades: 0,
    resyncs: 0,
    ignoredDeltas: 0,
    invalidBooks: 0,
  };

  #version = 0;
  #summary: MarketSummary | null = null;

  constructor(options: TapeStoreOptions) {
    this.ticker = options.ticker;
    this.binMs = options.binMs ?? DEFAULT_BIN_MS;
    this.originMs = options.originMs;
    this.#historyColumns = options.historyColumns ?? DEFAULT_HISTORY_COLUMNS;
    this.#trades = new TradeTape(options.tradeCapacity ?? DEFAULT_TRADE_CAPACITY);
    this.#grid = options.grid;
    this.#history = new DepthHistory(
      this.#historyColumns,
      options.grid.rows,
      options.grid.rowStepE4,
    );
    this.#rowTotals = new Float64Array(options.grid.rows);
    this.#history.advanceTo(0, ColumnStatus.unknown);
  }

  get columns(): DepthHistory {
    return this.#history;
  }

  get trades(): TradeTape {
    return this.#trades;
  }

  get maxRowContracts(): number {
    return this.#maxRowE2 / 100;
  }

  get grid(): PriceGrid {
    return this.#grid;
  }

  get version(): number {
    return this.#version;
  }

  get bestBidE4(): number | null {
    return this.#status === "unknown" ? null : this.#book.bestBid;
  }

  get bestAskE4(): number | null {
    return this.#status === "unknown" ? null : this.#book.bestAsk;
  }

  /** The fractional bin of monotonic time `atMs`; never below 0. */
  binAt(atMs: number): number {
    return Math.max(0, (atMs - this.originMs) / this.binMs);
  }

  /**
   * Applies one market message received at monotonic time `receivedAtMs`.
   * Messages for another ticker are ignored.
   */
  apply(message: MarketMessage, receivedAtMs: number): ApplyOutcome {
    if (message.ticker !== this.ticker) return "ignored";
    const outcome = this.#dispatch(message, receivedAtMs);
    this.#changed();
    return outcome;
  }

  /** The connection carrying this market closed: its book is no longer maintained. */
  markConnectionLost(atMs: number): void {
    if (this.#status === "unknown") return;
    this.#advance(atMs);
    this.#discardBook();
    this.#changed();
  }

  /**
   * Switches to another price grid (the market's ranges resolved). Heatmap history is
   * restarted, since old columns were binned on other rows; the book and trades are kept.
   */
  setGrid(grid: PriceGrid, atMs: number): void {
    if (sameRows(grid, this.#grid)) {
      this.#grid = grid;
      this.#changed();
      return;
    }
    this.#grid = grid;
    this.#history = new DepthHistory(this.#historyColumns, grid.rows, grid.rowStepE4);
    this.#rowTotals = new Float64Array(grid.rows);
    this.#history.advanceTo(this.#binIndex(atMs), this.#statusCode());
    if (this.#status !== "unknown") this.#rasterizeBook();
    this.#changed();
  }

  /** A summary for display; the same object until the next change. */
  summary(): MarketSummary {
    if (this.#summary?.version === this.#version) return this.#summary;
    const known = this.#status !== "unknown";
    this.#summary = {
      ticker: this.ticker,
      version: this.#version,
      grid: this.#grid,
      book: this.#status,
      gap: this.#gap,
      resync: this.#resync,
      bestBidE4: this.bestBidE4,
      bestAskE4: this.bestAskE4,
      bids: known ? this.#book.topLevels("bid", LADDER_LEVELS) : [],
      asks: known ? this.#book.topLevels("ask", LADDER_LEVELS) : [],
      tickerUpdate: this.#ticker,
      recentTrades: this.#trades.recent(12),
      lastBookChangeAtMs: this.#lastBookChangeAtMs,
      maxRowContracts: this.maxRowContracts,
      maxTradeContracts: this.#trades.maxContracts,
      counters: this.#counters,
    };
    return this.#summary;
  }

  #dispatch(message: MarketMessage, receivedAtMs: number): ApplyOutcome {
    switch (message.t) {
      case "snapshot":
        this.#advance(receivedAtMs);
        this.#book.replace(message.bids, message.asks);
        this.#status = message.book;
        this.#gap = false;
        this.#resync = null;
        this.#lastBookChangeAtMs = receivedAtMs;
        this.#count("snapshots");
        this.#rasterizeBook();
        return "applied";
      case "delta":
        return this.#applyDelta(message.side, message.price_e4, message.delta_e2, receivedAtMs);
      case "book":
        this.#advance(receivedAtMs);
        if (this.#status === "unknown") return "ignored";
        this.#status = message.book;
        this.#history.raiseHeadStatus(this.#statusCode());
        return "applied";
      case "resync":
        this.#advance(receivedAtMs);
        this.#resync = { reason: message.reason, sinceMs: receivedAtMs };
        this.#count("resyncs");
        this.#discardBook();
        return "applied";
      case "trade":
        this.#trades.append(this.binAt(receivedAtMs), {
          receivedAtMs,
          tsMs: message.ts_ms,
          priceE4: message.price_e4,
          countE2: message.count_e2,
          takerSide: message.taker_side,
        });
        this.#count("trades");
        return "applied";
      case "ticker":
        this.#ticker = tickerState(message, receivedAtMs);
        return "applied";
    }
  }

  #applyDelta(side: "bid" | "ask", priceE4: number, deltaE2: number, atMs: number): ApplyOutcome {
    this.#advance(atMs);
    if (this.#status !== "fresh") {
      this.#count("ignoredDeltas");
      return "ignored";
    }
    if (this.#book.applyDelta(side, priceE4, deltaE2) === "negative") {
      this.#count("invalidBooks");
      this.#discardBook();
      return "book_invalid";
    }
    this.#count("deltas");
    this.#lastBookChangeAtMs = atMs;
    const row = rowForPrice(this.#grid, priceE4);
    const total = Math.max(0, (this.#rowTotals[row] ?? 0) + deltaE2);
    this.#rowTotals[row] = total;
    this.#maxRowE2 = Math.max(this.#maxRowE2, total);
    this.#history.setHeadRow(row, Math.log1p(total / 100));
    this.#history.setHeadQuotes(this.#book.bestBid, this.#book.bestAsk);
    return "applied";
  }

  /** Clears the book and marks the head column a gap. */
  #discardBook(): void {
    this.#book.clear();
    this.#status = "unknown";
    this.#gap = true;
    this.#rowTotals.fill(0);
    this.#history.setHeadProfile(new Float32Array(this.#grid.rows));
    this.#history.setHeadQuotes(null, null);
    this.#history.raiseHeadStatus(ColumnStatus.gap);
  }

  /** Rebuilds row totals and the head column from the whole book. */
  #rasterizeBook(): void {
    this.#rowTotals.fill(0);
    this.#book.forEachLevel((_side, priceE4, countE2) => {
      const row = rowForPrice(this.#grid, priceE4);
      this.#rowTotals[row] = (this.#rowTotals[row] ?? 0) + countE2;
    });
    const profile = new Float32Array(this.#grid.rows);
    this.#rowTotals.forEach((total, row) => {
      this.#maxRowE2 = Math.max(this.#maxRowE2, total);
      profile[row] = Math.log1p(total / 100);
    });
    this.#history.setHeadProfile(profile);
    this.#history.setHeadQuotes(this.#book.bestBid, this.#book.bestAsk);
    this.#history.raiseHeadStatus(this.#statusCode());
  }

  #advance(atMs: number): void {
    this.#history.advanceTo(this.#binIndex(atMs), this.#statusCode());
  }

  #binIndex(atMs: number): number {
    return Math.floor(this.binAt(atMs));
  }

  #statusCode(): ColumnStatusCode {
    switch (this.#status) {
      case "fresh":
        return ColumnStatus.fresh;
      case "stale":
        return ColumnStatus.stale;
      case "unknown":
        return this.#gap ? ColumnStatus.gap : ColumnStatus.unknown;
    }
  }

  #count(counter: keyof StoreCounters): void {
    this.#counters = { ...this.#counters, [counter]: this.#counters[counter] + 1 };
  }

  #changed(): void {
    this.#history.setEdgeStatus(this.#statusCode());
    this.#version += 1;
  }
}

function tickerState(message: TickerMessage, receivedAtMs: number): TickerState {
  return {
    receivedAtMs,
    tsMs: message.ts_ms,
    bidE4: message.bid_e4,
    askE4: message.ask_e4,
    lastE4: message.last_e4,
    volumeE2: message.volume_e2,
  };
}
