/**
 * Pure helpers for the ring of time columns: which column and status each time bin is
 * drawn with, and the upload plan for the textures.
 *
 * The depth texture is transposed relative to the picture: texture x is the price row
 * and texture y is the ring column, so one time column is one texture row and uploads
 * as a single contiguous `texSubImage2D`. Invariants: every range returned lies inside
 * `[0, capacity)`, ranges are disjoint and ascending, and together they cover every
 * slot that changed.
 */

import {
  ColumnStatus,
  META_STATUS,
  META_STRIDE,
  type ColumnStatusCode,
  type DepthColumns,
} from "./source";

export interface UploadRange {
  readonly start: number;
  readonly count: number;
}

/** The ring slot of `bin`; `bin` must be a non-negative integer. */
export function ringColumn(bin: number, capacity: number): number {
  return ((bin % capacity) + capacity) % capacity;
}

/** What the heatmap draws for one time bin. */
export interface BinSample {
  /** Ring slot whose depth and quotes are drawn, or `null` when the bin has none. */
  readonly column: number | null;
  readonly status: ColumnStatusCode;
}

/**
 * The column and status drawn at integer `bin`, exactly as the heatmap shader samples them.
 *
 * Before the first column, or before the ring, a bin has no depth and is unknown. A bin in
 * the ring is its own column. A bin after the head has no column yet: it continues the head
 * column, which holds the latest book, with `edgeStatus`, the book's state now, rather than
 * the worst state the head bin saw. The result depends only on the columns, never on the
 * frame's time, so the live edge looks the same at 1 frame per second as at 60.
 */
export function sampleBin(columns: DepthColumns, bin: number): BinSample {
  if (columns.headBin < 0 || bin < columns.oldestBin) {
    return { column: null, status: ColumnStatus.unknown };
  }
  if (bin > columns.headBin) {
    return { column: ringColumn(columns.headBin, columns.capacity), status: columns.edgeStatus };
  }
  const column = ringColumn(bin, columns.capacity);
  const stored = columns.meta[column * META_STRIDE + META_STATUS] ?? ColumnStatus.unknown;
  return { column, status: Math.round(stored) as ColumnStatusCode };
}

/** How many bins the ring currently holds. */
export function heldBins(headBin: number, oldestBin: number): number {
  return headBin < 0 ? 0 : headBin - oldestBin + 1;
}

/**
 * Columns whose revision differs from what was uploaded, packed into contiguous ranges.
 *
 * @param revisions Current per-column revisions.
 * @param uploaded Revisions at the last upload, same length.
 * @param maxRanges Past this many ranges one full upload is cheaper than many small ones.
 */
export function dirtyColumnRanges(
  revisions: Uint32Array,
  uploaded: Uint32Array,
  maxRanges: number,
): UploadRange[] {
  const ranges: UploadRange[] = [];
  let start = -1;
  for (let column = 0; column <= revisions.length; column += 1) {
    const dirty = column < revisions.length && revisions[column] !== uploaded[column];
    if (dirty && start < 0) start = column;
    if (!dirty && start >= 0) {
      ranges.push({ start, count: column - start });
      start = -1;
    }
  }
  return ranges.length > maxRanges ? [{ start: 0, count: revisions.length }] : ranges;
}

/**
 * Ring slots written while a write counter went from `fromCount` to `toCount`.
 *
 * @returns Up to two ranges (the write may wrap), or the whole ring when at least
 *   `capacity` writes happened.
 */
export function ringWriteRanges(
  fromCount: number,
  toCount: number,
  capacity: number,
): UploadRange[] {
  const written = toCount - fromCount;
  if (written <= 0) return [];
  if (written >= capacity) return [{ start: 0, count: capacity }];
  const start = fromCount % capacity;
  const end = start + written;
  if (end <= capacity) return [{ start, count: written }];
  return [
    { start: 0, count: end - capacity },
    { start, count: capacity - start },
  ];
}
