import { describe, expect, it } from "vitest";
import {
  marketListNotice,
  marketNotice,
  WAITING_FOR_MARKET_LIST,
  type MarketAvailability,
} from "./availability";

const SHOWN: MarketAvailability = {
  listedMarkets: 40,
  detailUnknown: false,
  subscribed: true,
  rejection: null,
  retryAtMs: null,
};
const UNKNOWN = { ticker: "KXA", code: "unknown_ticker" };

describe("marketListNotice", () => {
  it("waits for the recorder's market list only when the API answered with none", () => {
    expect(marketListNotice(0, false)).toEqual({ tone: "idle", text: WAITING_FOR_MARKET_LIST });
    expect(marketListNotice(null, false)).toBeNull();
    expect(marketListNotice(3, false)).toBeNull();
    expect(marketListNotice(0, true)).toBeNull();
  });
});

describe("marketNotice", () => {
  it("says nothing about a market the live server accepted, whatever an older detail said", () => {
    expect(marketNotice(SHOWN)).toBeNull();
    expect(marketNotice({ ...SHOWN, detailUnknown: true })).toBeNull();
    expect(marketNotice({ ...SHOWN, subscribed: false })).toBeNull();
  });

  it("waits while the API lists no markets, instead of calling the market unrecorded", () => {
    const waiting = { tone: "idle", text: WAITING_FOR_MARKET_LIST };
    const beforeCatalog = { ...SHOWN, listedMarkets: 0, subscribed: false };
    expect(marketNotice({ ...beforeCatalog, rejection: UNKNOWN, retryAtMs: 5 })).toEqual(waiting);
    expect(marketNotice({ ...beforeCatalog, rejection: UNKNOWN })).toEqual(waiting);
    expect(marketNotice({ ...beforeCatalog, detailUnknown: true })).toEqual(waiting);
  });

  it("keeps asking while a retry is pending, then says the market is not recorded", () => {
    const rejected = { ...SHOWN, subscribed: false, rejection: UNKNOWN };
    expect(marketNotice({ ...rejected, retryAtMs: 10_000 })?.tone).toBe("idle");
    expect(marketNotice(rejected)).toEqual({
      tone: "error",
      text: "This market is not recorded, so there is no live book to show.",
    });
    expect(marketNotice({ ...SHOWN, subscribed: false, detailUnknown: true })?.tone).toBe("error");
  });

  it("explains refusals that retrying cannot fix", () => {
    const refused = { ...SHOWN, subscribed: false, retryAtMs: 1 };
    expect(
      marketNotice({ ...refused, rejection: { ticker: "KXA", code: "too_many_tickers" } })?.text,
    ).toMatch(/too many markets/);
    expect(
      marketNotice({ ...refused, rejection: { ticker: "KXA", code: "future_code" } })?.text,
    ).toMatch(/future_code/);
  });
});
