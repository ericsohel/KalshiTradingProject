/**
 * One market's order book in YES space, in integer units.
 *
 * Levels are held densely, indexed by `price_e4` (0..10,000), so applying a delta and
 * finding the best price are O(1) amortized, with a scan only when the best level empties.
 * Invariants: every count is a non-negative integer below 2^53; `bestBid` is the highest
 * price with a positive bid count and `bestAsk` the lowest with a positive ask count, or
 * `null` when a side is empty. A delta that would drive a level negative is reported,
 * not applied: the book no longer matches the server and the caller must resynchronize.
 */

import { MAX_PRICE_E4, type BookSide, type PriceLevel } from "../api/protocol";

export type DeltaOutcome = "applied" | "negative";

export class Book {
  readonly #bids = new Float64Array(MAX_PRICE_E4 + 1);
  readonly #asks = new Float64Array(MAX_PRICE_E4 + 1);
  #bestBid: number | null = null;
  #bestAsk: number | null = null;

  get bestBid(): number | null {
    return this.#bestBid;
  }

  get bestAsk(): number | null {
    return this.#bestAsk;
  }

  /** Empties both sides. */
  clear(): void {
    this.#bids.fill(0);
    this.#asks.fill(0);
    this.#bestBid = null;
    this.#bestAsk = null;
  }

  /** Replaces the whole book with a snapshot's levels. Zero-count levels are ignored. */
  replace(bids: readonly PriceLevel[], asks: readonly PriceLevel[]): void {
    this.clear();
    for (const [price, count] of bids) this.#bids[price] = count;
    for (const [price, count] of asks) this.#asks[price] = count;
    this.#bestBid = this.#scanBid(MAX_PRICE_E4);
    this.#bestAsk = this.#scanAsk(0);
  }

  /** Resting count at one level, contracts x 100. */
  count(side: BookSide, priceE4: number): number {
    return (side === "bid" ? this.#bids : this.#asks)[priceE4] ?? 0;
  }

  /**
   * Adds a signed change to one level.
   *
   * @returns `negative` (and leaves the book unchanged) when the level would go below
   *   zero; `applied` otherwise.
   */
  applyDelta(side: BookSide, priceE4: number, deltaE2: number): DeltaOutcome {
    const levels = side === "bid" ? this.#bids : this.#asks;
    const next = (levels[priceE4] ?? 0) + deltaE2;
    if (next < 0) return "negative";
    levels[priceE4] = next;
    if (side === "bid") this.#afterBidChange(priceE4, next);
    else this.#afterAskChange(priceE4, next);
    return "applied";
  }

  /** Up to `limit` levels of one side, best first. */
  topLevels(side: BookSide, limit: number): PriceLevel[] {
    const levels: PriceLevel[] = [];
    const best = side === "bid" ? this.#bestBid : this.#bestAsk;
    if (best === null) return levels;
    const source = side === "bid" ? this.#bids : this.#asks;
    const step = side === "bid" ? -1 : 1;
    for (let price = best; price >= 0 && price <= MAX_PRICE_E4; price += step) {
      const count = source[price] ?? 0;
      if (count > 0) levels.push([price, count]);
      if (levels.length >= limit) break;
    }
    return levels;
  }

  /** Calls `visit` for every positive level of both sides, in ascending price. */
  forEachLevel(visit: (side: BookSide, priceE4: number, countE2: number) => void): void {
    for (let price = 0; price <= MAX_PRICE_E4; price += 1) {
      const bid = this.#bids[price] ?? 0;
      const ask = this.#asks[price] ?? 0;
      if (bid > 0) visit("bid", price, bid);
      if (ask > 0) visit("ask", price, ask);
    }
  }

  #afterBidChange(price: number, count: number): void {
    if (count > 0 && (this.#bestBid === null || price > this.#bestBid)) this.#bestBid = price;
    else if (count === 0 && price === this.#bestBid) this.#bestBid = this.#scanBid(price - 1);
  }

  #afterAskChange(price: number, count: number): void {
    if (count > 0 && (this.#bestAsk === null || price < this.#bestAsk)) this.#bestAsk = price;
    else if (count === 0 && price === this.#bestAsk) this.#bestAsk = this.#scanAsk(price + 1);
  }

  #scanBid(from: number): number | null {
    for (let price = from; price >= 0; price -= 1) if ((this.#bids[price] ?? 0) > 0) return price;
    return null;
  }

  #scanAsk(from: number): number | null {
    for (let price = from; price <= MAX_PRICE_E4; price += 1) {
      if ((this.#asks[price] ?? 0) > 0) return price;
    }
    return null;
  }
}
