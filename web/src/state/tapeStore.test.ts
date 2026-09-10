import { describe, expect, it } from "vitest";
import type { MarketMessage, PriceLevel } from "../api/protocol";
import {
  ColumnStatus,
  META_BEST_ASK,
  META_BEST_BID,
  META_STRIDE,
  NO_PRICE,
  TRADE_STRIDE,
} from "../render/source";
import { gridFromPriceRanges, ONE_CENT_GRID } from "./priceGrid";
import { TapeStore } from "./tapeStore";

const TICKER = "KXA";
const DECI_GRID = gridFromPriceRanges([{ start_e4: 0, end_e4: 10_000, step_e4: 10 }]);

function store(historyColumns = 64): TapeStore {
  return new TapeStore({
    ticker: TICKER,
    grid: ONE_CENT_GRID,
    originMs: 0,
    binMs: 100,
    historyColumns,
    tradeCapacity: 8,
  });
}

function snapshot(
  bids: PriceLevel[],
  asks: PriceLevel[],
  book: "fresh" | "stale" = "fresh",
): MarketMessage {
  return { t: "snapshot", ticker: TICKER, book, ts_ms: 1, bids, asks };
}

function delta(side: "bid" | "ask", price_e4: number, delta_e2: number): MarketMessage {
  return { t: "delta", ticker: TICKER, ts_ms: 1, side, price_e4, delta_e2 };
}

function headMeta(target: TapeStore, field: number): number | undefined {
  const { columns } = target;
  return columns.meta[(columns.headBin % columns.capacity) * META_STRIDE + field];
}

const BIDS: PriceLevel[] = [
  [5600, 10_000],
  [5500, 2_000],
];
const ASKS: PriceLevel[] = [[5700, 300]];

describe("TapeStore book", () => {
  it("starts unknown, with an unknown column at bin 0", () => {
    const tape = store();
    expect(tape.summary().book).toBe("unknown");
    expect(tape.columns.statusAt(0)).toBe(ColumnStatus.unknown);
  });

  it("applies a snapshot: book, depth rows in log1p contracts, quotes, status", () => {
    const tape = store();
    expect(tape.apply(snapshot(BIDS, ASKS), 250)).toBe("applied");
    const summary = tape.summary();
    expect(summary).toMatchObject({ book: "fresh", gap: false, bestBidE4: 5600, bestAskE4: 5700 });
    expect(summary.bids).toEqual(BIDS);
    expect(tape.columns.headBin).toBe(2);
    expect(tape.columns.depthAt(2, 56)).toBeCloseTo(Math.log1p(100));
    expect(tape.columns.depthAt(2, 57)).toBeCloseTo(Math.log1p(3));
    expect(tape.columns.depthAt(2, 10)).toBe(0);
    expect(headMeta(tape, META_BEST_BID)).toBe(5600);
    expect(tape.columns.statusAt(1)).toBe(ColumnStatus.unknown);
    expect(tape.columns.statusAt(2)).toBe(ColumnStatus.unknown);
    tape.apply(delta("bid", 5600, 1), 300);
    expect(tape.columns.statusAt(3)).toBe(ColumnStatus.fresh);
    expect(tape.maxRowContracts).toBe(100.01);
  });

  it("applies deltas to one row and the best prices", () => {
    const tape = store();
    tape.apply(snapshot(BIDS, ASKS), 0);
    expect(tape.apply(delta("ask", 5650, 500), 10)).toBe("applied");
    expect(tape.apply(delta("bid", 5600, -10_000), 20)).toBe("applied");
    expect(tape.summary()).toMatchObject({ bestBidE4: 5500, bestAskE4: 5650 });
    expect(tape.columns.depthAt(0, 56)).toBe(0);
    expect(tape.columns.depthAt(0, 57)).toBeCloseTo(Math.log1p(8), 5);
    expect(headMeta(tape, META_BEST_ASK)).toBe(5650);
    expect(tape.summary().counters.deltas).toBe(2);
  });

  it("sums levels that share a row on an aggregated grid", () => {
    const tape = new TapeStore({ ticker: TICKER, grid: DECI_GRID, originMs: 0 });
    tape.apply(
      snapshot(
        [
          [5601, 100],
          [5600, 100],
        ],
        [],
      ),
      0,
    );
    expect(tape.columns.depthAt(0, 560)).toBeCloseTo(Math.log1p(2));
  });

  it("ignores deltas until a fresh book is known", () => {
    const tape = store();
    expect(tape.apply(delta("bid", 5600, 1), 0)).toBe("ignored");
    tape.apply(snapshot(BIDS, ASKS, "stale"), 0);
    expect(tape.apply(delta("bid", 5600, 1), 0)).toBe("ignored");
    expect(tape.summary().counters.ignoredDeltas).toBe(2);
  });

  it("discards the book when a delta would drive a level negative", () => {
    const tape = store();
    tape.apply(snapshot(BIDS, ASKS), 0);
    expect(tape.apply(delta("ask", 5700, -301), 150)).toBe("book_invalid");
    expect(tape.summary()).toMatchObject({ book: "unknown", gap: true, bids: [], bestBidE4: null });
    expect(tape.columns.statusAt(1)).toBe(ColumnStatus.gap);
    expect(tape.columns.depthAt(1, 56)).toBe(0);
  });

  it("ignores messages for other tickers", () => {
    const tape = store();
    expect(tape.apply({ ...snapshot(BIDS, ASKS), ticker: "KXB" }, 0)).toBe("ignored");
  });

  it("returns the same summary object until something changes", () => {
    const tape = store();
    const first = tape.summary();
    expect(tape.summary()).toBe(first);
    tape.apply(snapshot(BIDS, ASKS), 0);
    expect(tape.summary()).not.toBe(first);
  });
});

describe("TapeStore freshness, resync, and gaps", () => {
  it("marks stale bins from a book message and restores with a snapshot", () => {
    const tape = store();
    tape.apply(snapshot(BIDS, ASKS), 0);
    tape.apply({ t: "book", ticker: TICKER, book: "stale" }, 100);
    expect(tape.summary().book).toBe("stale");
    expect(tape.columns.statusAt(1)).toBe(ColumnStatus.stale);
    expect(tape.columns.depthAt(1, 56)).toBeCloseTo(Math.log1p(100));
    tape.apply(snapshot(BIDS, ASKS), 500);
    expect(tape.columns.statusAt(3)).toBe(ColumnStatus.stale);
    tape.apply(delta("bid", 5600, 1), 600);
    expect(tape.columns.statusAt(6)).toBe(ColumnStatus.fresh);
  });

  it("ignores a book message while no book is known", () => {
    const tape = store();
    expect(tape.apply({ t: "book", ticker: TICKER, book: "fresh" }, 0)).toBe("ignored");
    expect(tape.summary().book).toBe("unknown");
  });

  it.each(["client_lag", "bus_loss"])(
    "on resync (%s) clears the book and marks a gap until the next snapshot",
    (reason) => {
      const tape = store();
      tape.apply(snapshot(BIDS, ASKS), 0);
      tape.apply({ t: "resync", ticker: TICKER, reason }, 200);
      expect(tape.summary()).toMatchObject({
        book: "unknown",
        gap: true,
        resync: { reason, sinceMs: 200 },
        asks: [],
      });
      expect(tape.columns.statusAt(2)).toBe(ColumnStatus.gap);
      expect(headMeta(tape, META_BEST_BID)).toBe(NO_PRICE);
      tape.apply(
        { t: "trade", ticker: TICKER, ts_ms: 1, price_e4: 5700, count_e2: 100, taker_side: "bid" },
        700,
      );
      tape.apply(delta("bid", 5600, 1), 700);
      expect(tape.columns.statusAt(7)).toBe(ColumnStatus.gap);
      tape.apply(snapshot(BIDS, ASKS), 900);
      expect(tape.summary()).toMatchObject({ book: "fresh", gap: false, resync: null });
      expect(tape.columns.statusAt(9)).toBe(ColumnStatus.gap);
      tape.apply(delta("bid", 5600, 1), 1000);
      expect(tape.columns.statusAt(10)).toBe(ColumnStatus.fresh);
      expect(tape.summary().counters.resyncs).toBe(1);
    },
  );

  it("marks a gap when the connection is lost, but not before any book", () => {
    const tape = store();
    tape.markConnectionLost(50);
    expect(tape.summary().gap).toBe(false);
    tape.apply(snapshot(BIDS, ASKS), 100);
    tape.markConnectionLost(300);
    expect(tape.summary()).toMatchObject({ book: "unknown", gap: true, resync: null });
    expect(tape.columns.statusAt(3)).toBe(ColumnStatus.gap);
  });
});

describe("TapeStore history, trades, and grids", () => {
  it("wraps its column ring", () => {
    const tape = store(8);
    tape.apply(snapshot(BIDS, ASKS), 0);
    for (let time = 100; time <= 2000; time += 100) tape.apply(delta("bid", 5600, 1), time);
    expect([tape.columns.headBin, tape.columns.oldestBin]).toEqual([20, 13]);
    expect(tape.columns.depthAt(13, 56)).toBeGreaterThan(0);
  });

  it("packs trades for drawing and keeps recent ones newest first", () => {
    const tape = store();
    for (let index = 0; index < 10; index += 1) {
      tape.apply(
        {
          t: "trade",
          ticker: TICKER,
          ts_ms: index,
          price_e4: 5000 + index,
          count_e2: (index + 1) * 100,
          taker_side: index % 2 === 0 ? "bid" : "ask",
        },
        index * 50,
      );
    }
    const { trades } = tape;
    expect(trades.writeCount).toBe(10);
    expect(Array.from(trades.instances.subarray(TRADE_STRIDE, 2 * TRADE_STRIDE))).toEqual([
      4.5, 5009, 10, 1,
    ]);
    const summary = tape.summary();
    expect(summary.recentTrades[0]).toMatchObject({ priceE4: 5009, takerSide: "ask" });
    expect(summary.maxTradeContracts).toBe(10);
  });

  it("keeps the latest ticker update", () => {
    const tape = store();
    tape.apply(
      { t: "ticker", ticker: TICKER, ts_ms: 9, bid_e4: 1, ask_e4: null, last_e4: 2, volume_e2: 3 },
      40,
    );
    expect(tape.summary().tickerUpdate).toEqual({
      receivedAtMs: 40,
      tsMs: 9,
      bidE4: 1,
      askE4: null,
      lastE4: 2,
      volumeE2: 3,
    });
  });

  it("restarts history on a new row layout but keeps the book", () => {
    const tape = store();
    tape.apply(snapshot(BIDS, ASKS), 0);
    const before = tape.columns;
    tape.setGrid(gridFromPriceRanges([{ start_e4: 0, end_e4: 10_000, step_e4: 100 }]), 50);
    expect(tape.columns).toBe(before);
    tape.setGrid(DECI_GRID, 1000);
    expect(tape.columns).not.toBe(before);
    expect(tape.columns.rows).toBe(1001);
    expect([tape.columns.oldestBin, tape.columns.headBin]).toEqual([10, 10]);
    expect(tape.columns.depthAt(10, 560)).toBeCloseTo(Math.log1p(100));
    expect(tape.summary().bestBidE4).toBe(5600);
  });
});
