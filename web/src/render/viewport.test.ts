import { describe, expect, it } from "vitest";
import {
  binToFraction,
  followPrice,
  FULL_WINDOW,
  leftBin,
  liveView,
  priceTicks,
  priceToFractionFromTop,
  timeTicks,
} from "./viewport";

describe("live view geometry", () => {
  it("places now at 1 - pad of the width", () => {
    const view = liveView(1000, 1200, 0.05, FULL_WINDOW);
    expect(leftBin(view)).toBe(-140);
    expect(binToFraction(1000, view)).toBeCloseTo(0.95);
    expect(liveView(0, 100, 3, FULL_WINDOW).rightBin).toBe(50);
  });

  it("maps prices top-down within a window", () => {
    const window = { loE4: 0, hiE4: 10_000 };
    expect(priceToFractionFromTop(10_000, window)).toBe(0);
    expect(priceToFractionFromTop(2500, window)).toBe(0.75);
  });
});

describe("followPrice", () => {
  it("centers on the mid initially", () => {
    expect(followPrice(null, 5000, 2000)).toEqual({ loE4: 4000, hiE4: 6000 });
  });

  it("holds still while the mid stays in the middle half", () => {
    const window = { loE4: 4000, hiE4: 6000 };
    expect(followPrice(window, 5400, 2000)).toBe(window);
    expect(followPrice(window, 4600, 2000)).toBe(window);
  });

  it("re-centers once the mid leaves the middle half", () => {
    expect(followPrice({ loE4: 4000, hiE4: 6000 }, 5600, 2000)).toEqual({ loE4: 4600, hiE4: 6600 });
  });

  it("stays inside the full range near the edges", () => {
    expect(followPrice(null, 150, 2000)).toEqual({
      loE4: FULL_WINDOW.loE4,
      hiE4: FULL_WINDOW.loE4 + 2000,
    });
    expect(followPrice(null, 9990, 2000)).toEqual({
      loE4: FULL_WINDOW.hiE4 - 2000,
      hiE4: FULL_WINDOW.hiE4,
    });
  });

  it("keeps the previous window when no price is known", () => {
    const window = { loE4: 1, hiE4: 2001 };
    expect(followPrice(window, null, 2000)).toBe(window);
    expect(followPrice(null, null, 2000)).toBe(FULL_WINDOW);
  });
});

describe("axis ticks", () => {
  it("chooses a round step that fits the height", () => {
    expect(priceTicks({ loE4: 4000, hiE4: 6000 }, 10)).toEqual([
      4000, 4200, 4400, 4600, 4800, 5000, 5200, 5400, 5600, 5800, 6000,
    ]);
    expect(priceTicks(FULL_WINDOW, 12)).toEqual([
      0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10_000,
    ]);
  });

  it("never labels prices outside 0 to 1 dollar", () => {
    const ticks = priceTicks({ loE4: -100, hiE4: 300 }, 5);
    expect(ticks[0]).toBe(0);
    expect(ticks.every((tick) => tick >= 0 && tick <= 10_000)).toBe(true);
  });

  it("spaces time labels back from now until the left edge", () => {
    const view = liveView(1200, 1200, 0, FULL_WINDOW);
    const ticks = timeTicks(view, 250, 60_000);
    expect(ticks.map((tick) => tick.agoMs)).toEqual([
      0, 60_000, 120_000, 180_000, 240_000, 300_000,
    ]);
    expect(ticks[1]?.fraction).toBeCloseTo(0.8);
  });
});
