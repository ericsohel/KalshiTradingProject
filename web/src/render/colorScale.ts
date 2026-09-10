/**
 * Mapping resting depth to a position on the color ramp.
 *
 * Color is `log1p(contracts) / log1p(ceiling)`: logarithmic so a 50-contract level and a
 * 50,000-contract wall are both visible. The ceiling is stable for a market session: it
 * starts at `MIN_CEILING_CONTRACTS` and only ever rises, to the next power of two above
 * the deepest row seen, so brightness means the same thing across the visible history
 * and changes at most a handful of times per session.
 */

/** A thin book still gets a meaningful ramp. */
export const MIN_CEILING_CONTRACTS = 128;

/**
 * The ceiling for a session whose deepest row so far held `maxRowContracts`.
 *
 * @returns A power of two, at least `MIN_CEILING_CONTRACTS`.
 */
export function depthCeilingContracts(maxRowContracts: number): number {
  if (!Number.isFinite(maxRowContracts) || maxRowContracts <= MIN_CEILING_CONTRACTS) {
    return MIN_CEILING_CONTRACTS;
  }
  return 2 ** Math.ceil(Math.log2(maxRowContracts));
}

/** The value stored per texel: log1p of contracts, with negatives read as empty. */
export function depthValue(contracts: number): number {
  return Math.log1p(Math.max(0, contracts));
}

/** Ramp position of `contracts` under `ceilingContracts`, in [0, 1]. */
export function depthRampPosition(contracts: number, ceilingContracts: number): number {
  return Math.min(1, depthValue(contracts) / depthValue(ceilingContracts));
}

export interface LegendTick {
  readonly contracts: number;
  /** Position along the ramp, 0..1. */
  readonly position: number;
}

/** Powers of ten from 1 contract up to the ceiling, for labeling the legend's ramp. */
export function depthLegendTicks(ceilingContracts: number): LegendTick[] {
  const ticks: LegendTick[] = [];
  for (let contracts = 1; contracts <= ceilingContracts; contracts *= 10) {
    ticks.push({ contracts, position: depthRampPosition(contracts, ceilingContracts) });
  }
  return ticks;
}
