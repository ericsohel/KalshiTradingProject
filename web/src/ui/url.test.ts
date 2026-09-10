import { describe, expect, it } from "vitest";
import { rangeFromSearch, searchWithRange, searchWithTicker, tickerFromSearch } from "./url";

describe("URL state", () => {
  it("reads a well-formed ticker, including strike dots", () => {
    expect(tickerFromSearch("?market=KXBTCD-26SEP1017-T64999.99")).toBe(
      "KXBTCD-26SEP1017-T64999.99",
    );
    expect(tickerFromSearch("")).toBeNull();
  });

  it("rejects anything that is not a ticker", () => {
    expect(tickerFromSearch("?market=../status")).toBeNull();
    expect(tickerFromSearch("?market=KX%20A")).toBeNull();
    expect(tickerFromSearch(`?market=${"A".repeat(200)}`)).toBeNull();
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
