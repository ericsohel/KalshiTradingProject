/** Seeded randomness for the mock server, so a run can be reproduced with `--seed`. */

export type Random = () => number;

/** Mulberry32: a small, fast, well-distributed 32-bit generator. */
export function seededRandom(seed: number): Random {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

/** A standard normal sample (Box-Muller). */
export function normal(random: Random): number {
  const u = Math.max(random(), Number.EPSILON);
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * random());
}

/** A log-normal integer around `median`, clamped to `[min, max]`. */
export function logNormalInt(
  random: Random,
  median: number,
  sigma: number,
  min: number,
  max: number,
): number {
  const value = Math.round(median * Math.exp(sigma * normal(random)));
  return Math.min(max, Math.max(min, value));
}

/** A Poisson count with mean `mean` (Knuth; fine for the small means used here). */
export function poisson(random: Random, mean: number): number {
  const limit = Math.exp(-mean);
  let count = 0;
  let product = random();
  while (product > limit) {
    count += 1;
    product *= random();
  }
  return count;
}
