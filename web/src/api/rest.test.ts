import { describe, expect, it } from "vitest";
import { ApiRequestError, resolveEndpoints, RestClient, type FetchLike } from "./rest";

describe("resolveEndpoints", () => {
  it("uses the page's origin by default", () => {
    expect(resolveEndpoints("http://127.0.0.1:5173/?market=KXA", undefined)).toEqual({
      restBase: "http://127.0.0.1:5173/api/v1",
      liveUrl: "ws://127.0.0.1:5173/api/v1/live",
    });
  });

  it("uses wss for https and honors a configured origin", () => {
    expect(resolveEndpoints("https://tape.pages.dev/", "https://api.example.org")).toEqual({
      restBase: "https://api.example.org/api/v1",
      liveUrl: "wss://api.example.org/api/v1/live",
    });
    expect(resolveEndpoints("https://tape.pages.dev/", "").liveUrl).toBe(
      "wss://tape.pages.dev/api/v1/live",
    );
  });
});

function fakeFetch(status: number, body: unknown, seen: string[] = []): FetchLike {
  return (input) => {
    seen.push(input);
    return Promise.resolve(new Response(JSON.stringify(body), { status }));
  };
}

const ROW = {
  ticker: "KXA",
  event_ticker: "E",
  series_ticker: "S",
  title: null,
  subtitle: null,
  category: null,
  showcase: false,
  volume_24h_e2: 0,
  close_ts: null,
  bid_e4: null,
  ask_e4: null,
  last_e4: null,
  book: "unknown",
};

describe("RestClient", () => {
  it("requests a clamped page of markets and decodes it", async () => {
    const seen: string[] = [];
    const client = new RestClient({
      restBase: "http://h/api/v1/",
      fetch: fakeFetch(200, { markets: [ROW] }, seen),
    });
    const page = await client.listMarkets(5000);
    expect(seen).toEqual(["http://h/api/v1/markets?limit=200"]);
    expect(page.markets).toHaveLength(1);
  });

  it("encodes the ticker into the path", async () => {
    const seen: string[] = [];
    const client = new RestClient({
      restBase: "http://h/api/v1",
      fetch: fakeFetch(200, { ...ROW, price_ranges: null, depth: null }, seen),
    });
    await client.getMarket("A/B C");
    expect(seen).toEqual(["http://h/api/v1/markets/A%2FB%20C"]);
  });

  it("surfaces the server's error code", async () => {
    const client = new RestClient({
      restBase: "http://h/api/v1",
      fetch: fakeFetch(404, { error: { code: "unknown_ticker", message: "not recorded" } }),
    });
    const failure = await client.getMarket("KXZ").catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(ApiRequestError);
    expect(failure).toMatchObject({ code: "unknown_ticker", status: 404, message: "not recorded" });
  });

  it("reports an HTTP error without a body and a malformed response", async () => {
    const plain = new RestClient({ restBase: "http://h/api/v1", fetch: fakeFetch(503, "busy") });
    await expect(plain.getStatus()).rejects.toMatchObject({ code: "http_error", status: 503 });
    const malformed = new RestClient({
      restBase: "http://h/api/v1",
      fetch: fakeFetch(200, { recording: 1 }),
    });
    await expect(malformed.getStatus()).rejects.toMatchObject({ code: "malformed_response" });
  });

  it("reports network failures and caller aborts", async () => {
    const offline = new RestClient({
      restBase: "http://h/api/v1",
      fetch: () => Promise.reject(new TypeError("offline")),
    });
    await expect(offline.getStatus()).rejects.toMatchObject({ code: "network", status: null });
    const controller = new AbortController();
    controller.abort();
    const aborted = new RestClient({
      restBase: "http://h/api/v1",
      fetch: () => Promise.reject(new DOMException("aborted", "AbortError")),
    });
    await expect(aborted.getStatus(controller.signal)).rejects.toMatchObject({ code: "aborted" });
  });
});
