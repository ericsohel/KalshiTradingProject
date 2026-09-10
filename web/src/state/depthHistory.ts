/**
 * The time-binned depth history of one market: a ring of columns the renderer uploads.
 *
 * Each column is the book's depth profile during one bin plus its best bid, best ask,
 * and the worst book status seen in that bin. Only the head (newest) column changes;
 * moving the head forward copies the head column into every skipped bin, because a book
 * with no messages did not change. Until then, the time after the head bin is drawn from
 * the head column with `edgeStatus`, the book's state now, so how long a market stays
 * quiet, or when a frame happens to be drawn, never changes what that time shows.
 * Invariants: `oldestBin <= headBin`, the ring never holds more than `capacity` bins, a
 * column's status only rises within its bin, and each write increments that column's
 * revision.
 */

import { ringColumn } from "../render/columns";
import {
  ColumnStatus,
  META_BEST_ASK,
  META_BEST_BID,
  META_STATUS,
  META_STRIDE,
  NO_PRICE,
  type ColumnStatusCode,
  type DepthColumns,
} from "../render/source";

export class DepthHistory implements DepthColumns {
  readonly capacity: number;
  readonly rows: number;
  readonly rowStepE4: number;
  readonly depth: Float32Array;
  readonly meta: Float32Array;
  readonly revisions: Uint32Array;
  #headBin = -1;
  #oldestBin = 0;
  #edgeStatus: ColumnStatusCode = ColumnStatus.unknown;

  /**
   * @param capacity Columns in the ring (the texture's height; at most 2,048 for WebGL2).
   * @param rows Price rows per column.
   * @param rowStepE4 Price distance between rows.
   */
  constructor(capacity: number, rows: number, rowStepE4: number) {
    this.capacity = capacity;
    this.rows = rows;
    this.rowStepE4 = rowStepE4;
    this.depth = new Float32Array(capacity * rows);
    this.meta = new Float32Array(capacity * META_STRIDE);
    this.revisions = new Uint32Array(capacity);
  }

  get headBin(): number {
    return this.#headBin;
  }

  get oldestBin(): number {
    return this.#oldestBin;
  }

  get edgeStatus(): ColumnStatusCode {
    return this.#edgeStatus;
  }

  /** Sets the book's state now, which the bins after the head are drawn with. */
  setEdgeStatus(status: ColumnStatusCode): void {
    this.#edgeStatus = status;
  }

  /**
   * Makes `bin` the head. A bin at or before the head leaves the head where it is (time
   * never runs backwards in the picture). Skipped bins inherit the head column with
   * `status`, the book's status throughout the silence, which is also the edge status.
   */
  advanceTo(bin: number, status: ColumnStatusCode): void {
    this.#edgeStatus = status;
    if (this.#headBin < 0) {
      this.#headBin = bin;
      this.#oldestBin = bin;
      this.#writeEmpty(ringColumn(bin, this.capacity), status);
      return;
    }
    if (bin <= this.#headBin) return;
    const source = ringColumn(this.#headBin, this.capacity);
    const firstBin = Math.max(this.#headBin + 1, bin - this.capacity + 1);
    const depth = this.depth.slice(source * this.rows, (source + 1) * this.rows);
    const bestBid = this.meta[source * META_STRIDE + META_BEST_BID] ?? NO_PRICE;
    const bestAsk = this.meta[source * META_STRIDE + META_BEST_ASK] ?? NO_PRICE;
    for (let target = firstBin; target <= bin; target += 1) {
      const column = ringColumn(target, this.capacity);
      this.depth.set(depth, column * this.rows);
      this.#writeMeta(column, bestBid, bestAsk, status);
    }
    this.#headBin = bin;
    this.#oldestBin = Math.max(this.#oldestBin, bin - this.capacity + 1);
  }

  /** Sets one row of the head column. */
  setHeadRow(row: number, value: number): void {
    const column = this.#head();
    this.depth[column * this.rows + row] = value;
    this.#touch(column);
  }

  /** Replaces the head column's whole profile; `values` has `rows` entries. */
  setHeadProfile(values: Float32Array): void {
    const column = this.#head();
    this.depth.set(values, column * this.rows);
    this.#touch(column);
  }

  /** Sets the head column's best prices (`null` for none). */
  setHeadQuotes(bestBid: number | null, bestAsk: number | null): void {
    const offset = this.#head() * META_STRIDE;
    const bid = bestBid ?? NO_PRICE;
    const ask = bestAsk ?? NO_PRICE;
    if (this.meta[offset + META_BEST_BID] === bid && this.meta[offset + META_BEST_ASK] === ask) {
      return;
    }
    this.meta[offset + META_BEST_BID] = bid;
    this.meta[offset + META_BEST_ASK] = ask;
    this.#touch(this.#head());
  }

  /** Raises the head column's status to `status` if that is worse than what it holds. */
  raiseHeadStatus(status: ColumnStatusCode): void {
    const offset = this.#head() * META_STRIDE + META_STATUS;
    if ((this.meta[offset] ?? 0) >= status) return;
    this.meta[offset] = status;
    this.#touch(this.#head());
  }

  /** The status stored for `bin`, or `unknown` outside the ring. */
  statusAt(bin: number): ColumnStatusCode {
    if (this.#headBin < 0 || bin < this.#oldestBin || bin > this.#headBin) {
      return ColumnStatus.unknown;
    }
    const value = this.meta[ringColumn(bin, this.capacity) * META_STRIDE + META_STATUS] ?? 0;
    return value as ColumnStatusCode;
  }

  /** The depth value stored for `row` in `bin`; 0 outside the ring. */
  depthAt(bin: number, row: number): number {
    if (this.#headBin < 0 || bin < this.#oldestBin || bin > this.#headBin) return 0;
    return this.depth[ringColumn(bin, this.capacity) * this.rows + row] ?? 0;
  }

  #head(): number {
    if (this.#headBin < 0) throw new Error("DepthHistory: no head column before advanceTo");
    return ringColumn(this.#headBin, this.capacity);
  }

  #writeEmpty(column: number, status: ColumnStatusCode): void {
    this.depth.fill(0, column * this.rows, (column + 1) * this.rows);
    this.#writeMeta(column, NO_PRICE, NO_PRICE, status);
  }

  #writeMeta(column: number, bestBid: number, bestAsk: number, status: ColumnStatusCode): void {
    const offset = column * META_STRIDE;
    this.meta[offset + META_BEST_BID] = bestBid;
    this.meta[offset + META_BEST_ASK] = bestAsk;
    this.meta[offset + META_STATUS] = status;
    this.#touch(column);
  }

  #touch(column: number): void {
    this.revisions[column] = ((this.revisions[column] ?? 0) + 1) >>> 0;
  }
}
