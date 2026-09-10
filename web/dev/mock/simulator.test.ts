import { describe, expect, it } from "vitest";
import type { PriceLevel } from "../../src/api/protocol.ts";
import { MARKETS } from "./markets.ts";
import { seededRandom } from "./random.ts";
import { gridPrices, MarketSimulator } from "./simulator.ts";

function toMap(levels: readonly PriceLevel[]): Map<number, number> {
  return new Map(levels.map(([price, count]) => [price, count]));
}

describe("MarketSimulator", () => {
  it.each(MARKETS.map((definition) => [definition.ticker, definition] as const))(
    "%s: snapshot plus emitted deltas reproduces the book and never crosses",
    (_ticker, definition) => {
      const simulator = new MarketSimulator(definition, seededRandom(7));
      const grid = new Set(gridPrices(definition.tradingRanges));
      const start = simulator.levels();
      const bids = toMap(start.bids);
      const asks = toMap(start.asks);
      for (let step = 0; step < 3000; step += 1) {
        for (const event of simulator.step(100)) {
          expect(grid.has(event.priceE4)).toBe(true);
          if (event.kind === "trade") {
            expect(event.countE2).toBeGreaterThan(0);
            continue;
          }
          const side = event.side === "bid" ? bids : asks;
          const next = (side.get(event.priceE4) ?? 0) + event.deltaE2;
          expect(next).toBeGreaterThanOrEqual(0);
          if (next === 0) side.delete(event.priceE4);
          else side.set(event.priceE4, next);
        }
        const bestBid = simulator.bestBidE4;
        const bestAsk = simulator.bestAskE4;
        if (bestBid !== null && bestAsk !== null) expect(bestBid).toBeLessThan(bestAsk);
      }
      const end = simulator.levels();
      expect(bids).toEqual(toMap(end.bids));
      expect(asks).toEqual(toMap(end.asks));
    },
  );

  it("lists grid prices strictly inside 0 and 1 dollar", () => {
    const prices = gridPrices([
      { start_e4: 0, end_e4: 1000, step_e4: 10 },
      { start_e4: 1000, end_e4: 9000, step_e4: 100 },
      { start_e4: 9000, end_e4: 10000, step_e4: 10 },
    ]);
    expect(prices[0]).toBe(10);
    expect(prices.at(-1)).toBe(9990);
    expect(prices).toContain(1000);
    expect(prices).not.toContain(1010);
  });
});
