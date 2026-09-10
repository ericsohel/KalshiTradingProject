/**
 * A random-walk order book for one synthetic market.
 *
 * A hidden fair value drifts; takers lean toward it, consume the touch, and move the
 * price; makers add and cancel near the touch and refill depth. Every change is emitted
 * as the event a subscriber needs, so snapshot plus deltas always reproduces the book.
 * Invariants: counts are positive integers (contracts x 100), the book never crosses,
 * and every price lies on the market's grid strictly between 0 and 1 dollar.
 */

import type { BookSide, PriceLevel, PriceRange } from "../../src/api/protocol.ts";
import type { MarketDefinition } from "./markets.ts";
import { logNormalInt, normal, poisson, type Random } from "./random.ts";

export type SimEvent =
  | {
      readonly kind: "delta";
      readonly side: BookSide;
      readonly priceE4: number;
      readonly deltaE2: number;
    }
  | {
      readonly kind: "trade";
      readonly priceE4: number;
      readonly countE2: number;
      readonly takerSide: BookSide;
    };

/** Every tradable price on a grid, ascending, excluding 0 and 1 dollar. */
export function gridPrices(ranges: readonly PriceRange[]): number[] {
  const prices = new Set<number>();
  for (const range of ranges) {
    for (let price = range.start_e4; price <= range.end_e4; price += range.step_e4) {
      if (price > 0 && price < 10_000) prices.add(price);
    }
  }
  return [...prices].sort((left, right) => left - right);
}

export class MarketSimulator {
  readonly definition: MarketDefinition;
  readonly prices: readonly number[];
  /** Count x 100 by grid index. */
  readonly #bids = new Map<number, number>();
  readonly #asks = new Map<number, number>();
  readonly #random: Random;
  #fair: number;
  volumeE2: number;
  lastE4: number | null = null;

  constructor(definition: MarketDefinition, random: Random) {
    this.definition = definition;
    this.prices = gridPrices(definition.tradingRanges);
    this.#random = random;
    this.volumeE2 = definition.volume24hContracts * 100;
    const mid = this.#indexNear(definition.startMidE4);
    this.#fair = mid;
    const lowAsk = mid + Math.ceil(definition.spreadTicks / 2);
    const highBid = lowAsk - definition.spreadTicks;
    for (let level = 0; level < definition.levelsPerSide; level += 1) {
      if (highBid - level >= 0) this.#bids.set(highBid - level, this.#restingSize(level));
      if (lowAsk + level < this.prices.length)
        this.#asks.set(lowAsk + level, this.#restingSize(level));
    }
  }

  get bestBidE4(): number | null {
    const index = this.#bestBidIndex();
    return index === null ? null : (this.prices[index] ?? null);
  }

  get bestAskE4(): number | null {
    const index = this.#bestAskIndex();
    return index === null ? null : (this.prices[index] ?? null);
  }

  /** The whole book, best first, as `[price_e4, count_e2]`. */
  levels(): { bids: PriceLevel[]; asks: PriceLevel[] } {
    const toLevels = (side: Map<number, number>, descending: boolean): PriceLevel[] =>
      [...side.entries()]
        .sort(([left], [right]) => (descending ? right - left : left - right))
        .map(([index, count]): PriceLevel => [this.prices[index] ?? 0, count]);
    return { bids: toLevels(this.#bids, true), asks: toLevels(this.#asks, false) };
  }

  /** Advances the market by `dtMs` and returns what changed, in order. */
  step(dtMs: number): SimEvent[] {
    const seconds = dtMs / 1000;
    const events: SimEvent[] = [];
    const maxFair = this.prices.length - 4;
    this.#fair = Math.min(
      maxFair,
      Math.max(
        3,
        this.#fair + normal(this.#random) * this.definition.volatilityTicks * Math.sqrt(seconds),
      ),
    );
    for (
      let trade = poisson(this.#random, this.definition.tradesPerSecond * seconds);
      trade > 0;
      trade -= 1
    ) {
      this.#take(events);
    }
    for (
      let change = poisson(this.#random, this.definition.churnPerSecond * seconds);
      change > 0;
      change -= 1
    ) {
      this.#churn(events);
    }
    this.#refill(events);
    return events;
  }

  #take(events: SimEvent[]): void {
    const bid = this.#bestBidIndex();
    const ask = this.#bestAskIndex();
    if (bid === null || ask === null) return;
    const lean = Math.max(-0.4, Math.min(0.4, (this.#fair - (bid + ask) / 2) / 6));
    const takerSide: BookSide = this.#random() < 0.5 + lean ? "bid" : "ask";
    let remaining =
      logNormalInt(this.#random, this.definition.medianLevelContracts * 0.5, 1.1, 1, 50_000) * 100;
    // A sweep occasionally takes more than the touch and moves the price.
    if (this.#random() < 0.12) remaining *= 4;
    const book = takerSide === "bid" ? this.#asks : this.#bids;
    while (remaining > 0) {
      const index = takerSide === "bid" ? this.#bestAskIndex() : this.#bestBidIndex();
      if (index === null) break;
      const resting = book.get(index) ?? 0;
      const filled = Math.min(resting, remaining);
      const priceE4 = this.prices[index] ?? 0;
      events.push({ kind: "trade", priceE4, countE2: filled, takerSide });
      this.#change(events, takerSide === "bid" ? "ask" : "bid", index, -filled);
      this.volumeE2 += filled;
      this.lastE4 = priceE4;
      remaining -= filled;
      if (this.#random() < 0.6) break;
    }
  }

  #churn(events: SimEvent[]): void {
    const side: BookSide = this.#random() < 0.5 ? "bid" : "ask";
    const bid = this.#bestBidIndex();
    const ask = this.#bestAskIndex();
    if (bid === null || ask === null) return;
    const distance =
      Math.floor(Math.abs(normal(this.#random)) * 5) - (this.#random() < 0.15 ? 1 : 0);
    const index = side === "bid" ? bid - distance : ask + distance;
    if (index < 0 || index >= this.prices.length) return;
    if (side === "bid" ? index >= ask : index <= bid) return;
    const resting = (side === "bid" ? this.#bids : this.#asks).get(index) ?? 0;
    const add = resting === 0 || this.#random() < 0.52;
    const size =
      logNormalInt(this.#random, this.definition.medianLevelContracts * 0.4, 1, 1, 20_000) * 100;
    this.#change(events, side, index, add ? size : -Math.min(resting, size));
  }

  /** Keeps a two-sided book with depth: re-quotes a wide spread and restocks far levels. */
  #refill(events: SimEvent[]): void {
    const bid = this.#bestBidIndex();
    const ask = this.#bestAskIndex();
    const target = Math.round(this.#fair);
    if (bid === null)
      this.#change(
        events,
        "bid",
        Math.max(0, Math.min(target - 1, (ask ?? target) - 1)),
        this.#restingSize(0),
      );
    if (ask === null)
      this.#change(
        events,
        "ask",
        Math.min(
          this.prices.length - 1,
          Math.max(target + 1, (this.#bestBidIndex() ?? target) + 1),
        ),
        this.#restingSize(0),
      );
    const newBid = this.#bestBidIndex();
    const newAsk = this.#bestAskIndex();
    if (newBid === null || newAsk === null) return;
    if (newAsk - newBid > this.definition.spreadTicks + 2 && this.#random() < 0.5) {
      const side: BookSide = target - newBid > newAsk - target ? "bid" : "ask";
      const index = side === "bid" ? newBid + 1 : newAsk - 1;
      this.#change(events, side, index, this.#restingSize(0));
    }
    for (const side of ["bid", "ask"] as const) {
      const book = side === "bid" ? this.#bids : this.#asks;
      if (book.size >= this.definition.levelsPerSide) continue;
      const touch = side === "bid" ? newBid : newAsk;
      const index = side === "bid" ? touch - book.size : touch + book.size;
      if (index >= 0 && index < this.prices.length && !book.has(index)) {
        this.#change(events, side, index, this.#restingSize(book.size));
      }
    }
  }

  #change(events: SimEvent[], side: BookSide, index: number, deltaE2: number): void {
    if (deltaE2 === 0) return;
    const book = side === "bid" ? this.#bids : this.#asks;
    const next = (book.get(index) ?? 0) + deltaE2;
    if (next < 0) throw new Error(`simulator bug: negative level at ${index}`);
    if (next === 0) book.delete(index);
    else book.set(index, next);
    events.push({ kind: "delta", side, priceE4: this.prices[index] ?? 0, deltaE2 });
  }

  #restingSize(level: number): number {
    const wall = this.#random() < 0.04 ? 12 : 1;
    const median = this.definition.medianLevelContracts * (1 + level * 0.12) * wall;
    return logNormalInt(this.#random, median, 0.8, 1, 250_000) * 100;
  }

  #bestBidIndex(): number | null {
    return this.#bids.size === 0 ? null : Math.max(...this.#bids.keys());
  }

  #bestAskIndex(): number | null {
    return this.#asks.size === 0 ? null : Math.min(...this.#asks.keys());
  }

  #indexNear(priceE4: number): number {
    let best = 0;
    this.prices.forEach((price, index) => {
      if (Math.abs(price - priceE4) < Math.abs((this.prices[best] ?? 0) - priceE4)) best = index;
    });
    return best;
  }
}
