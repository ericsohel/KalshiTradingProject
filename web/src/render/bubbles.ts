/**
 * Trade bubble geometry, shared by the store (packing), the shader (drawing), and the
 * legend (sample sizes).
 *
 * A bubble's area is proportional to the trade's size: radius grows with the square
 * root of contracts relative to a session reference. The reference starts at
 * `MIN_REFERENCE_CONTRACTS` and rises by powers of four, so a size change is always a
 * doubling of radius range and happens rarely. Radii are clamped: a floor keeps one-lot
 * trades visible and a cap keeps block trades from covering the book; the legend says so.
 */

import { TRADE_BIN, TRADE_CONTRACTS, TRADE_PRICE, TRADE_SIDE, TRADE_STRIDE } from "./source";

/** Smallest drawn radius, CSS pixels. */
export const BUBBLE_MIN_RADIUS_PX = 2;
/** Largest drawn radius, CSS pixels; reached by a trade of the reference size. */
export const BUBBLE_MAX_RADIUS_PX = 14;
export const MIN_REFERENCE_CONTRACTS = 1024;

/** The size that draws at the maximum radius, given the largest trade this session. */
export function bubbleReferenceContracts(maxContracts: number): number {
  let reference = MIN_REFERENCE_CONTRACTS;
  while (reference < maxContracts && reference < Number.MAX_SAFE_INTEGER / 4) reference *= 4;
  return reference;
}

/**
 * Radius of a bubble for `contracts`: area proportional to size, clamped to
 * `[minRadiusPx, maxRadiusPx]`. Mirrored exactly by the bubble vertex shader.
 */
export function bubbleRadiusPx(
  contracts: number,
  referenceContracts: number,
  minRadiusPx = BUBBLE_MIN_RADIUS_PX,
  maxRadiusPx = BUBBLE_MAX_RADIUS_PX,
): number {
  const scaled = maxRadiusPx * Math.sqrt(Math.max(0, contracts) / referenceContracts);
  return Math.min(maxRadiusPx, Math.max(minRadiusPx, scaled));
}

/**
 * The two triangles of a bubble's quad in unit coordinates, counter-clockwise, indexed by
 * `gl_VertexID` in the shader.
 */
export const BUBBLE_CORNERS: readonly (readonly [number, number])[] = [
  [-1, -1],
  [1, -1],
  [-1, 1],
  [-1, 1],
  [1, -1],
  [1, 1],
];

/**
 * Writes one trade into the instance ring at `slot`.
 *
 * @param bin Fractional time bin of the trade.
 * @param priceE4 YES price.
 * @param countE2 Contracts x 100; stored as contracts, a rendering value only.
 * @param side `bid` when the taker bought YES.
 */
export function packTradeInstance(
  instances: Float32Array,
  slot: number,
  bin: number,
  priceE4: number,
  countE2: number,
  side: "bid" | "ask",
): void {
  const offset = slot * TRADE_STRIDE;
  instances[offset + TRADE_BIN] = bin;
  instances[offset + TRADE_PRICE] = priceE4;
  instances[offset + TRADE_CONTRACTS] = countE2 / 100;
  instances[offset + TRADE_SIDE] = side === "bid" ? 0 : 1;
}
