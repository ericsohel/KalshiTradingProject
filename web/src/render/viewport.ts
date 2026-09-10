/**
 * The plot's coordinate system, shared by the shaders (as uniforms) and the DOM axes.
 *
 * x is time in bins, increasing to the right, with "now" a little left of the right edge
 * so the newest bubbles are not clipped. y is YES price in e4, increasing upward. All
 * functions are pure; pixel values are CSS or device pixels as the caller supplies.
 */

import { MAX_PRICE_E4 } from "./constants";

export interface PriceWindow {
  /** Price at the bottom edge, e4. May sit slightly below 0 to show the 0 row whole. */
  readonly loE4: number;
  /** Price at the top edge, e4. */
  readonly hiE4: number;
}

export interface PlotView {
  /** Bin at the right edge (fractional). */
  readonly rightBin: number;
  readonly visibleBins: number;
  /** The current moment, fractional bin. */
  readonly nowBin: number;
  readonly window: PriceWindow;
}

/** Margin above 100 cents and below 0 in the full view, e4. */
export const FULL_RANGE_MARGIN_E4 = 100;

export const FULL_WINDOW: PriceWindow = {
  loE4: -FULL_RANGE_MARGIN_E4,
  hiE4: MAX_PRICE_E4 + FULL_RANGE_MARGIN_E4,
};

/**
 * A live view with `now` at `1 - rightPadFraction` of the width.
 *
 * @param nowBin The current fractional bin.
 * @param visibleBins Bins across the plot.
 * @param rightPadFraction Share of the width reserved right of now, in [0, 0.5].
 */
export function liveView(
  nowBin: number,
  visibleBins: number,
  rightPadFraction: number,
  window: PriceWindow,
): PlotView {
  const pad = Math.min(0.5, Math.max(0, rightPadFraction));
  return { rightBin: nowBin + visibleBins * pad, visibleBins, nowBin, window };
}

/** The bin at the left edge. */
export function leftBin(view: PlotView): number {
  return view.rightBin - view.visibleBins;
}

/** Horizontal position of `bin` as a fraction of the width (0 left, 1 right). */
export function binToFraction(bin: number, view: PlotView): number {
  return (bin - leftBin(view)) / view.visibleBins;
}

/** Vertical position of `priceE4` as a fraction of the height from the top (0 top, 1 bottom). */
export function priceToFractionFromTop(priceE4: number, window: PriceWindow): number {
  return (window.hiE4 - priceE4) / (window.hiE4 - window.loE4);
}

/**
 * A window of height `spanE4` that follows the mid price.
 *
 * It re-centers only when the mid leaves the middle half of the previous window, so the
 * axis holds still while the price wiggles. The window never extends past the full
 * view's margins.
 *
 * @param previous The window drawn last frame, or `null` on the first frame.
 * @param midE4 The current mid (or best known price); `null` keeps the previous window.
 */
export function followPrice(
  previous: PriceWindow | null,
  midE4: number | null,
  spanE4: number,
): PriceWindow {
  const span = Math.min(spanE4, FULL_WINDOW.hiE4 - FULL_WINDOW.loE4);
  if (midE4 === null) return previous ?? FULL_WINDOW;
  if (previous !== null && previous.hiE4 - previous.loE4 === span) {
    const quarter = span / 4;
    if (midE4 >= previous.loE4 + quarter && midE4 <= previous.hiE4 - quarter) return previous;
  }
  const lo = Math.min(Math.max(midE4 - span / 2, FULL_WINDOW.loE4), FULL_WINDOW.hiE4 - span);
  return { loE4: lo, hiE4: lo + span };
}

const PRICE_TICK_STEPS_E4 = [10, 20, 50, 100, 200, 500, 1000, 2000, 2500, 5000];

/**
 * Round price labels inside `window` and inside 0..1.
 *
 * @param maxTicks Upper bound on the number of labels (the plot's height decides it).
 */
export function priceTicks(window: PriceWindow, maxTicks: number): number[] {
  const span = window.hiE4 - window.loE4;
  const step =
    PRICE_TICK_STEPS_E4.find((candidate) => span / candidate <= Math.max(1, maxTicks)) ?? 5000;
  const first = Math.max(0, Math.ceil(window.loE4 / step) * step);
  const ticks: number[] = [];
  for (let price = first; price <= Math.min(window.hiE4, MAX_PRICE_E4); price += step) {
    ticks.push(price);
  }
  return ticks;
}

export interface TimeTick {
  /** Milliseconds before now; 0 is now. */
  readonly agoMs: number;
  /** Horizontal fraction of the plot width. */
  readonly fraction: number;
}

/**
 * Evenly spaced "time ago" labels for a live view.
 *
 * @param binMs Milliseconds per bin.
 * @param everyMs Spacing of labels.
 */
export function timeTicks(view: PlotView, binMs: number, everyMs: number): TimeTick[] {
  const ticks: TimeTick[] = [];
  for (let agoMs = 0; ; agoMs += everyMs) {
    const fraction = binToFraction(view.nowBin - agoMs / binMs, view);
    if (fraction < 0) break;
    ticks.push({ agoMs, fraction });
  }
  return ticks;
}
