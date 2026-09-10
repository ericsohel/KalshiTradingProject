/**
 * Recent trades of one market: a ring packed for instanced drawing, plus the last few
 * trades in full precision for the page's text.
 *
 * Invariants: `writeCount` only increases; the instance slot of trade `n` is
 * `n mod capacity`; `maxContracts` is the largest trade appended this session.
 */

import type { BookSide, CountE2, PriceE4, TsMs } from "../api/protocol";
import { packTradeInstance } from "../render/bubbles";
import { TRADE_STRIDE, type TradeInstances } from "../render/source";

export interface TradeRecord {
  readonly receivedAtMs: number;
  readonly tsMs: TsMs;
  readonly priceE4: PriceE4;
  readonly countE2: CountE2;
  readonly takerSide: BookSide;
}

/** Trades kept in full precision for lists. */
export const RECENT_TRADES = 32;

export class TradeTape implements TradeInstances {
  readonly capacity: number;
  readonly instances: Float32Array;
  #writeCount = 0;
  #maxContracts = 0;
  readonly #recent: TradeRecord[] = [];

  constructor(capacity: number) {
    this.capacity = capacity;
    this.instances = new Float32Array(capacity * TRADE_STRIDE);
  }

  get writeCount(): number {
    return this.#writeCount;
  }

  get maxContracts(): number {
    return this.#maxContracts;
  }

  /** Appends a trade drawn at fractional time bin `bin`. */
  append(bin: number, trade: TradeRecord): void {
    const slot = this.#writeCount % this.capacity;
    packTradeInstance(this.instances, slot, bin, trade.priceE4, trade.countE2, trade.takerSide);
    this.#writeCount += 1;
    this.#maxContracts = Math.max(this.#maxContracts, trade.countE2 / 100);
    this.#recent.push(trade);
    if (this.#recent.length > RECENT_TRADES) this.#recent.shift();
  }

  /** Up to `limit` most recent trades, newest first. */
  recent(limit: number): TradeRecord[] {
    return this.#recent.slice(-limit).reverse();
  }
}
