import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  closePolicy,
  LiveClient,
  type ConnectionState,
  type LiveClientOptions,
  type LiveSocket,
  type SocketEvents,
} from "./live";
import type { MarketMessage } from "./protocol";

class FakeSocket implements LiveSocket {
  readonly url: string;
  readonly events: SocketEvents;
  readonly sent: string[] = [];
  closed: { code: number; reason: string } | null = null;

  constructor(url: string, events: SocketEvents) {
    this.url = url;
    this.events = events;
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(code: number, reason: string): void {
    this.closed = { code, reason };
  }

  open(): void {
    this.events.onOpen();
  }

  receive(message: unknown): void {
    this.events.onMessage(JSON.stringify(message));
  }

  serverClose(code: number, reason = ""): void {
    this.events.onClose(code, reason);
  }

  subscriptions(): string[][] {
    return this.sent.map((payload) => (JSON.parse(payload) as { tickers: string[] }).tickers);
  }
}

const HELLO = { t: "hello", protocol: 1, max_tickers: 10, bus_refresh_s: 10 };

function setup(overrides: Partial<LiveClientOptions> = {}) {
  const sockets: FakeSocket[] = [];
  const messages: MarketMessage[] = [];
  const lost: number[] = [];
  const states: ConnectionState[] = [];
  const client = new LiveClient({
    url: "ws://test/api/v1/live",
    createSocket: (url, events) => {
      const socket = new FakeSocket(url, events);
      sockets.push(socket);
      return socket;
    },
    listener: {
      onMarketMessage: (message) => messages.push(message),
      onConnectionLost: (atMs) => lost.push(atMs),
      onStateChange: (state) => states.push(state),
    },
    now: () => Date.now(),
    random: () => 0.5,
    ...overrides,
  });
  const socket = (index = -1): FakeSocket => {
    const found = sockets.at(index);
    if (found === undefined) throw new Error("no socket");
    return found;
  };
  const goLive = (): FakeSocket => {
    const current = socket();
    current.open();
    current.receive(HELLO);
    return current;
  };
  return { client, sockets, messages, lost, states, socket, goLive };
}

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000_000);
});

afterEach(() => {
  vi.useRealTimers();
});

describe("LiveClient handshake", () => {
  it("subscribes only after hello", () => {
    const { client, socket } = setup();
    client.setSubscription(["KXA", "KXB", "KXA"]);
    client.start();
    socket().open();
    expect(client.state.phase).toBe("handshaking");
    expect(socket().sent).toEqual([]);
    socket().receive(HELLO);
    expect(client.state.phase).toBe("live");
    expect(client.state.hello?.bus_refresh_s).toBe(10);
    expect(socket().subscriptions()).toEqual([["KXA", "KXB"]]);
  });

  it("refuses a server speaking another protocol and does not retry", () => {
    const { client, socket, sockets } = setup();
    client.start();
    socket().open();
    socket().receive({ ...HELLO, protocol: 2 });
    expect(client.state.phase).toBe("incompatible");
    expect(socket().closed).not.toBeNull();
    vi.advanceTimersByTime(120_000);
    expect(sockets).toHaveLength(1);
  });

  it("gives up on a socket that never says hello", () => {
    const { client, socket, sockets } = setup({ helloTimeoutMs: 10_000 });
    client.start();
    socket().open();
    vi.advanceTimersByTime(10_000);
    expect(socket(0).closed).not.toBeNull();
    expect(client.state.phase).toBe("waiting");
    vi.advanceTimersByTime(1000);
    expect(sockets).toHaveLength(2);
  });

  it("retries when the socket cannot even be created", () => {
    let calls = 0;
    const { client } = setup({
      createSocket: () => {
        calls += 1;
        throw new SyntaxError("bad url");
      },
    });
    client.start();
    expect(client.state.phase).toBe("waiting");
    vi.advanceTimersByTime(249);
    expect(calls).toBe(1);
    vi.advanceTimersByTime(1);
    expect(calls).toBe(2);
  });
});

describe("LiveClient messages", () => {
  it("forwards every market message type, including both resync reasons", () => {
    const { client, goLive, messages } = setup();
    client.start();
    const socket = goLive();
    const feed = [
      { t: "snapshot", ticker: "KXA", book: "fresh", ts_ms: 1, bids: [[100, 1]], asks: [] },
      { t: "delta", ticker: "KXA", ts_ms: 2, side: "bid", price_e4: 100, delta_e2: 5 },
      { t: "book", ticker: "KXA", book: "stale" },
      { t: "resync", ticker: "KXA", reason: "client_lag" },
      { t: "resync", ticker: "KXA", reason: "bus_loss" },
      { t: "trade", ticker: "KXA", ts_ms: 3, price_e4: 100, count_e2: 100, taker_side: "ask" },
      {
        t: "ticker",
        ticker: "KXA",
        ts_ms: 4,
        bid_e4: 100,
        ask_e4: null,
        last_e4: 100,
        volume_e2: 7,
      },
    ];
    for (const message of feed) socket.receive(message);
    expect(messages).toEqual(feed);
  });

  it("does not forward market messages before hello", () => {
    const { client, socket, messages } = setup();
    client.start();
    socket().open();
    socket().receive({ t: "book", ticker: "KXA", book: "fresh" });
    expect(messages).toEqual([]);
  });

  it("counts malformed and unknown frames without crashing", () => {
    const { client, goLive, messages } = setup();
    client.start();
    const socket = goLive();
    socket.events.onMessage("{oops");
    socket.receive({ t: "delta", ticker: "KXA", side: "yes" });
    socket.receive({ t: "announcement", text: "hi" });
    socket.receive({ t: "book", ticker: "KXA", book: "fresh" });
    expect(client.state.malformedFrames).toBe(2);
    expect(client.state.ignoredFrames).toBe(1);
    expect(messages).toHaveLength(1);
  });

  it("records the subscription reply and server errors", () => {
    const { client, goLive } = setup();
    client.start();
    const socket = goLive();
    socket.receive({
      t: "subscribed",
      tickers: ["KXA"],
      rejected: [{ ticker: "KXZ", code: "unknown_ticker" }],
    });
    socket.receive({ t: "error", code: "unknown_op", message: "Only subscribe" });
    expect(client.state.subscribed).toEqual(["KXA"]);
    expect(client.state.rejected).toEqual([{ ticker: "KXZ", code: "unknown_ticker" }]);
    expect(client.state.lastServerError).toEqual({ code: "unknown_op", message: "Only subscribe" });
  });
});

describe("LiveClient subscription pacing", () => {
  it("coalesces rapid changes and stays under the server's rate limit", () => {
    const { client, goLive } = setup();
    client.start();
    const socket = goLive();
    for (let step = 0; step < 50; step += 1) {
      client.setSubscription([`KX${step}`]);
      vi.advanceTimersByTime(20);
    }
    vi.advanceTimersByTime(1000);
    const sent = socket.subscriptions();
    expect(sent.length).toBeLessThanOrEqual(6);
    expect(sent.at(-1)).toEqual(["KX49"]);
  });

  it("does not resend an unchanged set", () => {
    const { client, goLive } = setup();
    client.setSubscription(["KXA"]);
    client.start();
    const socket = goLive();
    vi.advanceTimersByTime(500);
    client.setSubscription(["KXA"]);
    vi.advanceTimersByTime(500);
    expect(socket.subscriptions()).toEqual([["KXA"]]);
  });

  it("requests a fresh snapshot by dropping and re-adding one ticker", () => {
    const { client, goLive } = setup();
    client.setSubscription(["KXA", "KXB"]);
    client.start();
    const socket = goLive();
    vi.advanceTimersByTime(300);
    client.requestSnapshot("KXA");
    client.requestSnapshot("KXNOTWATCHED");
    vi.advanceTimersByTime(1000);
    expect(socket.subscriptions()).toEqual([["KXA", "KXB"], ["KXB"], ["KXA", "KXB"]]);
  });

  it("refuses to send a subscription over 4 KB", () => {
    const { client, goLive } = setup();
    client.start();
    const socket = goLive();
    client.setSubscription(
      Array.from({ length: 200 }, (_, index) => `KXVERYLONGTICKERNAME-${index}`),
    );
    expect(socket.sent).toEqual([]);
    expect(client.state.lastServerError?.code).toBe("subscription_too_large");
  });
});

describe("LiveClient before the recorder's market list reaches the server", () => {
  const unknown = (...tickers: string[]) => ({
    t: "subscribed",
    tickers: [],
    rejected: tickers.map((ticker) => ({ ticker, code: "unknown_ticker" })),
  });

  it("asks again for markets rejected as unknown after bus_refresh_s, until accepted", () => {
    const { client, goLive } = setup();
    client.setSubscription(["KXA"]);
    client.start();
    const socket = goLive();
    socket.receive(unknown("KXA"));
    expect(client.state.rejectionRetryAtMs).toBe(Date.now() + 10_000);
    vi.advanceTimersByTime(9_999);
    expect(socket.subscriptions()).toEqual([["KXA"]]);
    vi.advanceTimersByTime(1);
    expect(socket.subscriptions()).toEqual([["KXA"], ["KXA"]]);
    socket.receive(unknown("KXA"));
    vi.advanceTimersByTime(10_000);
    expect(socket.subscriptions()).toHaveLength(3);
    socket.receive({ t: "subscribed", tickers: ["KXA"], rejected: [] });
    expect(client.state).toMatchObject({
      subscribed: ["KXA"],
      rejected: [],
      rejectionRetryAtMs: null,
    });
    vi.advanceTimersByTime(120_000);
    expect(socket.subscriptions()).toHaveLength(3);
  });

  it("gives up after the retry budget, and a new subscription set renews it", () => {
    const { client, goLive } = setup({ unknownTickerRetries: 2 });
    client.setSubscription(["KXA"]);
    client.start();
    const socket = goLive();
    for (let reply = 0; reply < 3; reply += 1) {
      socket.receive(unknown("KXA"));
      vi.advanceTimersByTime(10_000);
    }
    expect(socket.subscriptions()).toEqual([["KXA"], ["KXA"], ["KXA"]]);
    expect(client.state.rejectionRetryAtMs).toBeNull();
    expect(client.state.rejected).toEqual([{ ticker: "KXA", code: "unknown_ticker" }]);
    vi.advanceTimersByTime(120_000);
    expect(socket.subscriptions()).toHaveLength(3);

    client.setSubscription(["KXB"]);
    vi.advanceTimersByTime(300);
    socket.receive(unknown("KXB"));
    vi.advanceTimersByTime(10_000);
    expect(socket.subscriptions().slice(3)).toEqual([["KXB"], ["KXB"]]);
  });

  it("does not retry markets refused for being too many", () => {
    const { client, goLive } = setup();
    client.setSubscription(["KXA", "KXB"]);
    client.start();
    const socket = goLive();
    socket.receive({
      t: "subscribed",
      tickers: ["KXA"],
      rejected: [{ ticker: "KXB", code: "too_many_tickers" }],
    });
    expect(client.state.rejectionRetryAtMs).toBeNull();
    vi.advanceTimersByTime(120_000);
    expect(socket.subscriptions()).toEqual([["KXA", "KXB"]]);
  });

  it("uses the server's refresh interval and starts over on a new connection", () => {
    const { client, socket } = setup({ unknownTickerRetries: 1 });
    client.setSubscription(["KXA"]);
    client.start();
    const first = socket();
    first.open();
    first.receive({ ...HELLO, bus_refresh_s: 3 });
    first.receive(unknown("KXA"));
    vi.advanceTimersByTime(3_000);
    first.receive(unknown("KXA"));
    expect(first.subscriptions()).toEqual([["KXA"], ["KXA"]]);
    expect(client.state.rejectionRetryAtMs).toBeNull();
    first.serverClose(1006);
    vi.advanceTimersByTime(1_000);
    const second = socket();
    second.open();
    second.receive(HELLO);
    second.receive(unknown("KXA"));
    expect(client.state.rejectionRetryAtMs).toBe(Date.now() + 10_000);
  });

  it("cancels a pending retry on stop", () => {
    const { client, goLive } = setup();
    client.setSubscription(["KXA"]);
    client.start();
    const socket = goLive();
    socket.receive(unknown("KXA"));
    client.stop();
    expect(client.state.rejectionRetryAtMs).toBeNull();
    vi.advanceTimersByTime(120_000);
    expect(socket.subscriptions()).toEqual([["KXA"]]);
  });
});

describe("LiveClient reconnects", () => {
  it("reports the loss once, backs off, reconnects, and resubscribes", () => {
    const { client, goLive, sockets, lost } = setup();
    client.setSubscription(["KXA"]);
    client.start();
    goLive().serverClose(1006);
    expect(lost).toEqual([1_000_000]);
    expect(client.state.phase).toBe("waiting");
    expect(client.state.retryAtMs).toBe(1_000_250);
    expect(client.state.subscribed).toEqual([]);
    vi.advanceTimersByTime(249);
    expect(sockets).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(sockets).toHaveLength(2);
    const second = goLive();
    expect(second.subscriptions()).toEqual([["KXA"]]);
    expect(lost).toHaveLength(1);
  });

  it("grows the delay across consecutive failures and resets after a stable connection", () => {
    const { client, socket, goLive } = setup({ stableAfterMs: 30_000 });
    client.start();
    const delays: number[] = [];
    for (let failure = 0; failure < 4; failure += 1) {
      socket().open();
      socket().serverClose(1006);
      delays.push((client.state.retryAtMs ?? 0) - Date.now());
      vi.advanceTimersByTime(delays.at(-1) ?? 0);
    }
    expect(delays).toEqual([250, 500, 1000, 2000]);
    goLive();
    vi.advanceTimersByTime(30_000);
    socket().serverClose(1006);
    expect((client.state.retryAtMs ?? 0) - Date.now()).toBe(250);
  });

  it.each([
    [1013, 5000],
    [1008, 15_000],
    [4000, 2000],
  ])("pauses at least the floor after close code %i", (code, floorMs) => {
    const { client, goLive } = setup();
    client.start();
    goLive().serverClose(code);
    expect((client.state.retryAtMs ?? 0) - Date.now()).toBeGreaterThanOrEqual(floorMs);
    expect(client.state.lastClose?.code).toBe(code);
    expect(client.state.lastClose?.explanation).toBe(closePolicy(code).explanation);
  });

  it("ignores events from a socket that was replaced", () => {
    const { client, goLive, socket, messages, lost } = setup();
    client.start();
    const first = goLive();
    first.serverClose(1006);
    vi.advanceTimersByTime(1000);
    goLive();
    first.receive({ t: "book", ticker: "KXA", book: "fresh" });
    first.serverClose(1006);
    expect(messages).toEqual([]);
    expect(lost).toHaveLength(1);
    expect(socket().closed).toBeNull();
  });

  it("stops cleanly: closes, cancels timers, and reports the loss once", () => {
    const { client, goLive, sockets, lost } = setup();
    client.start();
    const socket = goLive();
    client.stop();
    expect(socket.closed?.code).toBe(1000);
    expect(client.state.phase).toBe("stopped");
    expect(lost).toHaveLength(1);
    vi.advanceTimersByTime(120_000);
    expect(sockets).toHaveLength(1);
    client.start();
    expect(sockets).toHaveLength(2);
  });

  it("cancels a pending reconnect on stop", () => {
    const { client, goLive, sockets } = setup();
    client.start();
    goLive().serverClose(1006);
    client.stop();
    vi.advanceTimersByTime(120_000);
    expect(sockets).toHaveLength(1);
  });
});
