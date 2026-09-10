/**
 * The heatmap's price rows for one market, derived from its `price_ranges`.
 *
 * One row per price step from 0 to 1 dollar inclusive: 101 rows on a one-cent grid and
 * 1,001 on a tenth-of-a-cent grid (FRONTEND 5). A market mixing steps (tapered grids,
 * finer near 0 and 1) uses its finest step. Row spacing is never finer than a tenth of a
 * cent, so a hundredth-of-a-cent market shares rows rather than allocating 10,001; the
 * spacing always divides 10,000, so 0 and 1 dollar are rows. Unknown ranges fall back to
 * one cent, flagged `assumed`. Arithmetic is integer throughout.
 */

import { MAX_PRICE_E4, type PriceRange } from "../api/protocol";

export interface PriceGrid {
  /** The market's finest tick, e4. */
  readonly tickE4: number;
  /** Distance between rows, e4; divides 10,000. */
  readonly rowStepE4: number;
  /** `10,000 / rowStepE4 + 1`. */
  readonly rows: number;
  /** True when the market's ranges were not known and one cent was assumed. */
  readonly assumed: boolean;
}

/** Row spacings the heatmap may use: divisors of 10,000 no finer than a tenth of a cent. */
const ROW_STEPS_E4 = [
  10, 16, 20, 25, 40, 50, 80, 100, 125, 200, 250, 400, 500, 625, 1000, 1250, 2000, 2500, 5000,
  10000,
];

export const ONE_CENT_GRID: PriceGrid = { tickE4: 100, rowStepE4: 100, rows: 101, assumed: true };

/**
 * The grid for a market.
 *
 * @param ranges The market's `price_ranges`, or `null` while unresolved.
 */
export function gridFromPriceRanges(ranges: readonly PriceRange[] | null): PriceGrid {
  if (ranges === null || ranges.length === 0) return ONE_CENT_GRID;
  const tickE4 = Math.min(...ranges.map((range) => range.step_e4));
  const rowStepE4 = ROW_STEPS_E4.find((step) => step >= tickE4) ?? MAX_PRICE_E4;
  return { tickE4, rowStepE4, rows: MAX_PRICE_E4 / rowStepE4 + 1, assumed: false };
}

/** True when two grids lay out rows identically. */
export function sameRows(left: PriceGrid, right: PriceGrid): boolean {
  return left.rowStepE4 === right.rowStepE4 && left.rows === right.rows;
}

/** The row nearest `priceE4` (half rounds up), clamped to the grid. */
export function rowForPrice(grid: PriceGrid, priceE4: number): number {
  const row = Math.floor((2 * priceE4 + grid.rowStepE4) / (2 * grid.rowStepE4));
  return Math.min(grid.rows - 1, Math.max(0, row));
}

/** The price a row stands for, e4. */
export function priceForRow(grid: PriceGrid, row: number): number {
  return row * grid.rowStepE4;
}

/** Digits after the cent needed to write any price on this grid: 0, 1, or 2. */
export function centDecimals(grid: PriceGrid): 0 | 1 | 2 {
  if (grid.tickE4 % 100 === 0) return 0;
  return grid.tickE4 % 10 === 0 ? 1 : 2;
}
