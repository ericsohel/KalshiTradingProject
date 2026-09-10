import { describe, expect, it } from "vitest";
import {
  decodeApiError,
  decodeFrame,
  decodeMarketDetail,
  decodeMarketList,
  decodeServiceStatus,
} from "./decode";

const frame = (value: unknown) => decodeFrame(JSON.stringify(value));

const VALID_MESSAGES = {
  hello: { t: "hello", protocol: 1, max_tickers: 10, bus_refresh_s: 10 },
  subscribed: {
    t: "subscribed",
    tickers: ["KXA"],
    rejected: [{ ticker: "KXB", code: "unknown_ticker" }],
  },
  snapshot: {
    t: "snapshot",
    ticker: "KXA",
    book: "fresh",
    ts_ms: 1_757_500_000_000,
    bids: [
      [5600, 1000],
      [5500, 250],
    ],
    asks: [[5700, 300]],
  },
  delta: { t: "delta", ticker: "KXA", ts_ms: 1, side: "ask", price_e4: 5700, delta_e2: -300 },
  book: { t: "book", ticker: "KXA", book: "stale" },
  resync_client_lag: { t: "resync", ticker: "KXA", reason: "client_lag" },
  resync_bus_loss: { t: "resync", ticker: "KXA", reason: "bus_loss" },
  trade: { t: "trade", ticker: "KXA", ts_ms: 1, price_e4: 5700, count_e2: 500, taker_side: "bid" },
  ticker: {
    t: "ticker",
    ticker: "KXA",
    ts_ms: 1,
    bid_e4: 5600,
    ask_e4: null,
    last_e4: 5650,
    volume_e2: 123_400,
  },
  error: { t: "error", code: "malformed_json", message: "Not JSON" },
} as const;

describe("decodeFrame: valid messages", () => {
  it.each(Object.entries(VALID_MESSAGES))("decodes %s exactly", (_name, message) => {
    expect(frame(message)).toEqual({ kind: "message", message });
  });

  it("ignores unknown fields", () => {
    const result = frame({ ...VALID_MESSAGES.book, extra: [1, 2, 3] });
    expect(result).toEqual({ kind: "message", message: VALID_MESSAGES.book });
  });

  it("accepts codes a newer server may add (open taxonomies)", () => {
    expect(frame({ t: "resync", ticker: "KXA", reason: "future_reason" }).kind).toBe("message");
    expect(
      frame({ t: "subscribed", tickers: [], rejected: [{ ticker: "KXA", code: "new_code" }] }).kind,
    ).toBe("message");
    expect(frame({ t: "error", code: "new_code", message: "" }).kind).toBe("message");
  });

  it("accepts a snapshot and a delta without exchange time, as the schema allows", () => {
    const snapshot = { ...VALID_MESSAGES.snapshot, ts_ms: null };
    const delta = { ...VALID_MESSAGES.delta, ts_ms: null };
    expect(frame(snapshot)).toEqual({ kind: "message", message: snapshot });
    expect(frame(delta)).toEqual({ kind: "message", message: delta });
  });

  it("accepts empty books and tickers with dots", () => {
    const snapshot = {
      ...VALID_MESSAGES.snapshot,
      ticker: "KXBTCD-26SEP1017-T64999.99",
      bids: [],
      asks: [],
    };
    expect(frame(snapshot)).toEqual({ kind: "message", message: snapshot });
  });

  it("reports well-formed messages of an unknown type as ignored", () => {
    expect(frame({ t: "heartbeat", n: 1 })).toEqual({ kind: "ignored", type: "heartbeat" });
  });
});

describe("decodeFrame: malformed input never throws", () => {
  const snapshot = VALID_MESSAGES.snapshot;
  const cases: readonly (readonly [string, unknown])[] = [
    ["hello with a string protocol", { ...VALID_MESSAGES.hello, protocol: "1" }],
    ["hello with zero max_tickers", { ...VALID_MESSAGES.hello, max_tickers: 0 }],
    ["subscribed without a ticker list", { t: "subscribed", tickers: "KXA", rejected: [] }],
    [
      "subscribed with a rejection lacking code",
      { t: "subscribed", tickers: [], rejected: [{ ticker: "KXA" }] },
    ],
    ["snapshot with book unknown", { ...snapshot, book: "unknown" }],
    [
      "snapshot bids not best first",
      {
        ...snapshot,
        bids: [
          [5500, 1],
          [5600, 1],
        ],
      },
    ],
    [
      "snapshot asks not best first",
      {
        ...snapshot,
        asks: [
          [5800, 1],
          [5700, 1],
        ],
      },
    ],
    [
      "snapshot with a repeated price",
      {
        ...snapshot,
        asks: [
          [5700, 1],
          [5700, 2],
        ],
      },
    ],
    ["snapshot price above one dollar", { ...snapshot, asks: [[10_001, 1]] }],
    ["snapshot negative count", { ...snapshot, asks: [[5700, -1]] }],
    ["snapshot fractional count", { ...snapshot, asks: [[5700, 1.5]] }],
    ["snapshot level with three numbers", { ...snapshot, asks: [[5700, 1, 2]] }],
    ["snapshot without ticker", { ...snapshot, ticker: undefined }],
    ["snapshot with empty ticker", { ...snapshot, ticker: "" }],
    ["snapshot without ts_ms", { ...snapshot, ts_ms: undefined }],
    ["delta without ts_ms", { ...VALID_MESSAGES.delta, ts_ms: undefined }],
    ["trade with a null ts_ms", { ...VALID_MESSAGES.trade, ts_ms: null }],
    ["delta with a Kalshi side", { ...VALID_MESSAGES.delta, side: "yes" }],
    ["delta with a fractional change", { ...VALID_MESSAGES.delta, delta_e2: 1.5 }],
    ["delta with a negative price", { ...VALID_MESSAGES.delta, price_e4: -1 }],
    ["delta with an unsafe integer", { ...VALID_MESSAGES.delta, delta_e2: 2 ** 60 }],
    ["book with status unknown", { ...VALID_MESSAGES.book, book: "unknown" }],
    ["resync without reason", { t: "resync", ticker: "KXA" }],
    ["trade with an unknown taker side", { ...VALID_MESSAGES.trade, taker_side: "buy" }],
    ["trade with zero count", { ...VALID_MESSAGES.trade, count_e2: 0 }],
    ["ticker missing bid_e4", { ...VALID_MESSAGES.ticker, bid_e4: undefined }],
    ["ticker with null volume", { ...VALID_MESSAGES.ticker, volume_e2: null }],
    ["error without message", { t: "error", code: "x" }],
    ["a message without t", { ticker: "KXA" }],
    ["a message whose t is a number", { t: 7 }],
    ["an array", [VALID_MESSAGES.hello]],
    ["null", null],
  ];

  it.each(cases)("rejects %s", (_name, value) => {
    const result = frame(value);
    expect(result.kind).toBe("malformed");
  });

  it("names the offending field", () => {
    const result = frame({ ...VALID_MESSAGES.delta, side: "yes" });
    expect(result).toEqual({
      kind: "malformed",
      error: expect.stringContaining("delta.side") as string,
    });
  });

  it("rejects invalid JSON and binary frames", () => {
    expect(decodeFrame("{not json").kind).toBe("malformed");
    expect(decodeFrame(new ArrayBuffer(4)).kind).toBe("malformed");
  });
});

const ROW = {
  ticker: "KXA",
  event_ticker: "KXA-EV",
  series_ticker: "KX",
  title: "Highest temperature in NYC today?",
  subtitle: "84° to 85°",
  category: "Climate and Weather",
  showcase: true,
  volume_24h_e2: 41_200_000,
  close_ts: 1_757_540_000,
  bid_e4: 3100,
  ask_e4: 3200,
  last_e4: null,
  book: "fresh",
};

describe("REST decoders", () => {
  it("decodes a market list and drops malformed rows individually", () => {
    const result = decodeMarketList({
      markets: [ROW, { ...ROW, ticker: 5 }, { ...ROW, ticker: "KXC", title: null }],
    });
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.value.markets.map((row) => row.ticker)).toEqual(["KXA", "KXC"]);
    expect(result.value.invalidRows).toBe(1);
  });

  it("fails a list without a markets array", () => {
    expect(decodeMarketList({ rows: [] }).ok).toBe(false);
  });

  it("decodes market detail with unresolved grid and unknown book", () => {
    const result = decodeMarketDetail({ ...ROW, book: "unknown", price_ranges: null, depth: null });
    expect(result).toEqual({
      ok: true,
      value: { ...ROW, book: "unknown", price_ranges: null, depth: null },
    });
  });

  it("decodes market detail with ranges and depth", () => {
    const detail = {
      ...ROW,
      price_ranges: [{ start_e4: 0, end_e4: 10_000, step_e4: 100 }],
      depth: { ts_ms: 5, bids: [[3100, 100]], asks: [[3200, 200]] },
    };
    expect(decodeMarketDetail(detail)).toEqual({ ok: true, value: detail });
    const untimed = { ...detail, depth: { ...detail.depth, ts_ms: null } };
    expect(decodeMarketDetail(untimed)).toEqual({ ok: true, value: untimed });
  });

  it("rejects an empty price range and unordered depth", () => {
    expect(
      decodeMarketDetail({
        ...ROW,
        price_ranges: [{ start_e4: 100, end_e4: 100, step_e4: 1 }],
        depth: null,
      }).ok,
    ).toBe(false);
    expect(
      decodeMarketDetail({
        ...ROW,
        price_ranges: null,
        depth: {
          ts_ms: 1,
          bids: [
            [1, 1],
            [2, 1],
          ],
          asks: [],
        },
      }).ok,
    ).toBe(false);
  });

  it("decodes service status, keeping an epoch beyond 2^53 exact as a string", () => {
    const status = {
      recording: true,
      recorder_status_age_ms: 1200,
      recorder: {
        universe_size: 2143,
        subscribed_markets: 1800,
        connections: [
          {
            conn_id: 0,
            taped: false,
            frames: 10,
            gaps: 0,
            reconnects: 1,
            stale_books: 0,
            sink_dropped: 0,
          },
        ],
      },
      bus: {
        epoch: "1757500000000000123",
        last_seq: 9,
        messages: 9,
        resets: 0,
        missed: 0,
        books_known: 3,
      },
      clients: 2,
    };
    expect(decodeServiceStatus(status)).toEqual({ ok: true, value: status });
    const beforeTheBus = { ...status.bus, epoch: null, last_seq: null, messages: 0 };
    expect(decodeServiceStatus({ ...status, bus: beforeTheBus })).toEqual({
      ok: true,
      value: { ...status, bus: beforeTheBus },
    });
    for (const epoch of [1_757_500_000_000_000_000, "", "-1", "1e18", " 7"]) {
      expect(decodeServiceStatus({ ...status, bus: { ...status.bus, epoch } }).ok).toBe(false);
    }
    expect(
      decodeServiceStatus({ ...status, recorder: null, recorder_status_age_ms: null }).ok,
    ).toBe(true);
    expect(decodeServiceStatus({ ...status, recording: "yes" }).ok).toBe(false);
    expect(
      decodeServiceStatus({
        ...status,
        recorder: { ...status.recorder, connections: [{ conn_id: 0 }] },
      }).ok,
    ).toBe(false);
  });

  it("decodes an error body", () => {
    expect(decodeApiError({ error: { code: "unknown_ticker", message: "nope" } })).toEqual({
      ok: true,
      value: { error: { code: "unknown_ticker", message: "nope" } },
    });
    expect(decodeApiError({ detail: "x" }).ok).toBe(false);
  });
});
