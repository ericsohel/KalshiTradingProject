import { describe, expect, it } from "vitest";
import { rangeFromSearch, searchWithRange, searchWithTicker, tickerFromSearch } from "./url";

describe("URL state", () => {
  it("reads a well-formed ticker, including strike dots", () => {
    for (const ticker of [
      "KXBTCD-26SEP1017-T64999.99",
      "KXAAAGASD-26SEP11-4.2700",
      "KX10YRDIRHM-26SEP30H-T4.85",
      "KXHIGHNY-26SEP10-B84.5",
    ]) {
      expect(tickerFromSearch(`?market=${ticker}`)).toBe(ticker);
    }
    expect(tickerFromSearch("")).toBeNull();
  });

  it("rejects anything that is not a ticker (docs/DATA_FORMATS.md 1.4)", () => {
    for (const text of [
      "../status",
      "..",
      ".",
      "-KXA",
      "KX%20A",
      "kxbtcd-26sep1017",
      "KX_A",
      "KX/A",
      "A".repeat(97),
    ]) {
      expect(tickerFromSearch(`?market=${text}`)).toBeNull();
    }
    expect(tickerFromSearch(`?market=${"A".repeat(96)}`)).toBe("A".repeat(96));
  });

  it("writes the ticker and keeps other parameters", () => {
    expect(searchWithTicker("?range=full", "KXA")).toBe("?range=full&market=KXA");
    expect(searchWithTicker("?market=KXA", "KXB")).toBe("?market=KXB");
  });

  it("reads and writes the range mode, omitting the default", () => {
    expect(rangeFromSearch("?range=full")).toBe("full");
    expect(rangeFromSearch("?range=bogus")).toBe("follow");
    expect(searchWithRange("?market=KXA", "full")).toBe("?market=KXA&range=full");
    expect(searchWithRange("?market=KXA&range=full", "follow")).toBe("?market=KXA");
    expect(searchWithRange("?range=full", "follow")).toBe("");
  });
});
