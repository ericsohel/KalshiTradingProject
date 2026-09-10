import { describe, expect, it } from "vitest";
import {
  formatAgo,
  formatCents,
  formatCloses,
  formatCompactContracts,
  formatContracts,
  marketLabel,
} from "./format";

describe("formatCents", () => {
  it("writes cents with the grid's precision, or more when the price needs it", () => {
    expect(formatCents(5600, 0)).toBe("56¢");
    expect(formatCents(5630, 1)).toBe("56.3¢");
    expect(formatCents(5600, 1)).toBe("56.0¢");
    expect(formatCents(5625, 0)).toBe("56.25¢");
    expect(formatCents(5, 2)).toBe("0.05¢");
    expect(formatCents(10_000, 0)).toBe("100¢");
    expect(formatCents(null, 0)).toBe("—");
  });
});

describe("contract counts", () => {
  it("splits count_e2 exactly", () => {
    expect(formatContracts(123_400)).toBe("1,234");
    expect(formatContracts(123_450)).toBe("1,234.5");
    expect(formatContracts(123_405)).toBe("1,234.05");
    expect(formatContracts(9_007_199_254_740_900)).toBe("90,071,992,547,409");
  });

  it("abbreviates large counts", () => {
    expect(formatCompactContracts(83_000)).toBe("830");
    expect(formatCompactContracts(4_120_000)).toBe("41K");
    expect(formatCompactContracts(150_000)).toBe("1.5K");
    expect(formatCompactContracts(184_000_000)).toBe("1.8M");
  });
});

describe("times", () => {
  it("formats elapsed time", () => {
    expect([0, 999, 12_000, 240_000, 7_200_000, -5].map(formatAgo)).toEqual([
      "now",
      "now",
      "12s",
      "4m",
      "2h",
      "now",
    ]);
  });

  it("formats closing time", () => {
    const nowMs = 1_000_000_000;
    expect(formatCloses(null, nowMs)).toBeNull();
    expect(formatCloses(999_000, nowMs)).toBe("closed");
    expect(formatCloses(1_000_000 + 600, nowMs)).toBe("closes in 10m");
    expect(formatCloses(1_000_000 + 6 * 3600, nowMs)).toBe("closes in 6h");
    expect(formatCloses(1_000_000 + 5 * 86_400, nowMs)).toBe("closes in 5d");
  });
});

describe("marketLabel", () => {
  it("prefers the title and subtitle and falls back to the ticker", () => {
    expect(marketLabel({ ticker: "KXA", title: "Rain?", subtitle: "Above 1 inch" })).toEqual({
      primary: "Rain?",
      secondary: "Above 1 inch",
    });
    expect(marketLabel({ ticker: "KXA", title: null, subtitle: null })).toEqual({
      primary: "KXA",
      secondary: null,
    });
  });
});
