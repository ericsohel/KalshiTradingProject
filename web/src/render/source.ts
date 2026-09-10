/**
 * What the renderer reads: the contract between a data store and `HeatmapRenderer`.
 *
 * The renderer depends on this interface and nothing else from the app, so it can draw a
 * live `TapeStore` today and replay chunks later. Arrays are shared, not copied: the
 * store writes, the renderer uploads what changed, detected by per-column revisions and
 * a monotonically increasing trade write count.
 */

/** Per-column book condition, ordered by severity so a bin keeps its worst state. */
export const ColumnStatus = {
  /** A fresh book was maintained through the bin. */
  fresh: 0,
  /** The book was stale: depth is the last known image, not current. */
  stale: 1,
  /** No book was known yet. */
  unknown: 2,
  /** A known book was lost (resync or disconnect) and not yet restored. */
  gap: 3,
} as const;

export type ColumnStatusCode = (typeof ColumnStatus)[keyof typeof ColumnStatus];

/** Floats per column in `DepthColumns.meta`. */
export const META_STRIDE = 4;
export const META_BEST_BID = 0;
export const META_BEST_ASK = 1;
export const META_STATUS = 2;
/** Stored in place of a best price when there is none. */
export const NO_PRICE = -1;

/**
 * A ring of time columns. Column `c` holds bin `b` when `c = b mod capacity` and `b` is
 * in `[oldestBin, headBin]`. Bins after `headBin`, up to now, have no column yet: they
 * continue the head column's depth and quotes, which are the latest book, with
 * `edgeStatus`, the book's state now (see `sampleBin` in columns.ts).
 */
export interface DepthColumns {
  readonly capacity: number;
  readonly rows: number;
  readonly rowStepE4: number;
  /** `capacity * rows` values; `[column * rows + row]` is log1p(resting contracts). */
  readonly depth: Float32Array;
  /** `capacity * META_STRIDE` values: best bid e4, best ask e4, status, reserved. */
  readonly meta: Float32Array;
  /** Incremented whenever a column's depth or meta changes. */
  readonly revisions: Uint32Array;
  /** Newest bin written, or -1 before the first. */
  readonly headBin: number;
  readonly oldestBin: number;
  /**
   * The book's state now, drawn for the bins after `headBin`. The head column's own status
   * is the worst its bin saw, so a snapshot that restored the book within that bin leaves
   * it `unknown`; the time after it must show the restored book.
   */
  readonly edgeStatus: ColumnStatusCode;
}

/** Floats per trade in `TradeInstances.instances`. */
export const TRADE_STRIDE = 4;
export const TRADE_BIN = 0;
export const TRADE_PRICE = 1;
export const TRADE_CONTRACTS = 2;
export const TRADE_SIDE = 3;

/** A ring of trades packed for instanced drawing. */
export interface TradeInstances {
  readonly capacity: number;
  /** `capacity * TRADE_STRIDE` values: bin (fractional), price e4, contracts, side (0 bid, 1 ask). */
  readonly instances: Float32Array;
  /** Trades ever appended; slot of the next one is `writeCount mod capacity`. */
  readonly writeCount: number;
  /** Largest single trade seen this session, in contracts. */
  readonly maxContracts: number;
}

export interface HeatmapSource {
  readonly columns: DepthColumns;
  readonly trades: TradeInstances;
  /** Largest resting depth in one row seen this session, in contracts. */
  readonly maxRowContracts: number;
}
