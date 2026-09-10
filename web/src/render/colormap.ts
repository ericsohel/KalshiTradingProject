/**
 * The heatmap's color ramp: viridis, perceptually uniform and legible to most color
 * vision deficiencies.
 *
 * Ten anchors of matplotlib's viridis interpolated linearly in sRGB, which stays within a
 * few units of the full 256-entry table. The shader samples a LUT texture built from
 * this function and the legend draws a CSS gradient from the same anchors, so the two
 * can never disagree.
 */

export type Rgb = readonly [red: number, green: number, blue: number];

const VIRIDIS_ANCHORS: readonly Rgb[] = [
  [0x44, 0x01, 0x54],
  [0x48, 0x28, 0x78],
  [0x3e, 0x4a, 0x89],
  [0x31, 0x68, 0x8e],
  [0x26, 0x82, 0x8e],
  [0x1f, 0x9e, 0x89],
  [0x35, 0xb7, 0x79],
  [0x6d, 0xcd, 0x59],
  [0xb4, 0xde, 0x2c],
  [0xfd, 0xe7, 0x25],
];

/**
 * The ramp color at `t`.
 *
 * @param t Position in [0, 1]; values outside are clamped, NaN reads as 0.
 * @returns Integer sRGB channels in 0..255.
 */
export function sampleViridis(t: number): Rgb {
  const clamped = Number.isNaN(t) ? 0 : Math.min(1, Math.max(0, t));
  const position = clamped * (VIRIDIS_ANCHORS.length - 1);
  const index = Math.min(Math.floor(position), VIRIDIS_ANCHORS.length - 2);
  const fraction = position - index;
  const low = VIRIDIS_ANCHORS[index] ?? VIRIDIS_ANCHORS[0];
  const high = VIRIDIS_ANCHORS[index + 1] ?? low;
  const channel = (channelIndex: 0 | 1 | 2): number => {
    const a = low?.[channelIndex] ?? 0;
    const b = high?.[channelIndex] ?? 0;
    return Math.round(a + (b - a) * fraction);
  };
  return [channel(0), channel(1), channel(2)];
}

/** An RGBA8 lookup table of `size` entries for a `size x 1` texture. */
export function buildViridisLut(size: number): Uint8Array {
  const lut = new Uint8Array(size * 4);
  for (let entry = 0; entry < size; entry += 1) {
    const [red, green, blue] = sampleViridis(size === 1 ? 0 : entry / (size - 1));
    lut.set([red, green, blue, 255], entry * 4);
  }
  return lut;
}

/** A left-to-right CSS gradient of the ramp with `stops` evenly spaced stops. */
export function viridisCssGradient(stops: number): string {
  const count = Math.max(2, Math.floor(stops));
  const parts = Array.from({ length: count }, (_, index) => {
    const t = index / (count - 1);
    const [red, green, blue] = sampleViridis(t);
    return `rgb(${red} ${green} ${blue}) ${Math.round(t * 100)}%`;
  });
  return `linear-gradient(to right, ${parts.join(", ")})`;
}
