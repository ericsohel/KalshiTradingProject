/**
 * A stand-in for `tape serve` on 127.0.0.1:8787, implementing docs/FRONTEND.md 4.1 and 4.2
 * with synthetic markets, so the viewer can be developed without the recorder.
 *
 *   npm run mock                      # steady markets
 *   npm run mock -- --chaos           # plus a random disruption every 20 to 40 s
 *   TAPE_API=http://127.0.0.1:8787 npm run dev
 *
 * Disruptions on demand (POST, e.g. `curl -X POST 'http://127.0.0.1:8787/mock/resync?reason=bus_loss'`):
 *   /mock/resync?reason=client_lag|bus_loss   resync every subscribed market
 *   /mock/stale?seconds=15[&ticker=T]         mark books stale, then restore with a snapshot
 *   /mock/slow                                lag each client three times: close 4000
 *   /mock/full?seconds=30                     close clients with 1013 and refuse new ones
 *   /mock/policy                              close clients with 1008
 */

import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { parseArgs } from "node:util";
import { WebSocketServer } from "ws";
import type { RawData, WebSocket } from "ws";
import type {
  BookState,
  ErrorResponse,
  MarketDetail,
  MarketRow,
  ServerMessage,
  ServiceStatus,
  SnapshotMessage,
  TickerMessage,
} from "../src/api/protocol.ts";
import { MARKETS, type MarketDefinition } from "./mock/markets.ts";
import { seededRandom } from "./mock/random.ts";
import { MarketSimulator } from "./mock/simulator.ts";

const { values: args } = parseArgs({
  options: {
    port: { type: "string", default: "8787" },
    seed: { type: "string", default: "20260910" },
    chaos: { type: "boolean", default: false },
  },
});

const HOST = "127.0.0.1";
const PORT = Number(args.port);
const STEP_MS = 100;
const BUS_REFRESH_S = 10;
const MAX_TICKERS = 10;
const MAX_CLIENTS = 20;
const MAX_MESSAGE_BYTES = 4096;
const MAX_MESSAGES_PER_SECOND = 10;
const LAGS_BEFORE_CLOSE = 3;
const LAG_WINDOW_MS = 60_000;

const random = seededRandom(Number(args.seed));
const startedAtMs = Date.now();

interface MarketState {
  readonly definition: MarketDefinition;
  readonly simulator: MarketSimulator;
  book: BookState;
  /** When a stale or lost book comes back with a snapshot. */
  recoverAtMs: number | null;
  lastTicker: TickerMessage | null;
}

interface Client {
  readonly id: number;
  readonly socket: WebSocket;
  tickers: string[];
  readonly receivedAt: number[];
  readonly lagAt: number[];
}

const markets = new Map<string, MarketState>(
  MARKETS.map((definition) => [
    definition.ticker,
    {
      definition,
      simulator: new MarketSimulator(definition, random),
      book: "fresh",
      recoverAtMs: null,
      lastTicker: null,
    },
  ]),
);
const clients = new Set<Client>();
let nextClientId = 1;
let fullUntilMs = 0;
let messagesSent = 0;
let busResets = 0;

/* ------------------------------------------------------------------ feed */

function send(client: Client, message: ServerMessage): void {
  if (client.socket.readyState !== client.socket.OPEN) return;
  client.socket.send(JSON.stringify(message));
  messagesSent += 1;
}

function broadcast(ticker: string, message: ServerMessage): void {
  for (const client of clients) if (client.tickers.includes(ticker)) send(client, message);
}

function snapshotOf(state: MarketState): SnapshotMessage | null {
  if (state.book === "unknown") return null;
  const { bids, asks } = state.simulator.levels();
  return {
    t: "snapshot",
    ticker: state.definition.ticker,
    book: state.book,
    ts_ms: Date.now(),
    bids,
    asks,
  };
}

function stepMarket(state: MarketState, nowMs: number): void {
  const ticker = state.definition.ticker;
  if (state.recoverAtMs !== null && nowMs >= state.recoverAtMs) {
    state.recoverAtMs = null;
    state.book = "fresh";
    const snapshot = snapshotOf(state);
    if (snapshot !== null) broadcast(ticker, snapshot);
  }
  for (const event of state.simulator.step(STEP_MS)) {
    if (event.kind === "delta") {
      if (state.book !== "fresh") continue;
      broadcast(ticker, {
        t: "delta",
        ticker,
        ts_ms: nowMs,
        side: event.side,
        price_e4: event.priceE4,
        delta_e2: event.deltaE2,
      });
    } else {
      broadcast(ticker, {
        t: "trade",
        ticker,
        ts_ms: nowMs,
        price_e4: event.priceE4,
        count_e2: event.countE2,
        taker_side: event.takerSide,
      });
    }
  }
  const previous = state.lastTicker;
  const bid = state.simulator.bestBidE4;
  const ask = state.simulator.bestAskE4;
  const topChanged = previous === null || previous.bid_e4 !== bid || previous.ask_e4 !== ask;
  if (topChanged || nowMs - previous.ts_ms >= 1000) {
    state.lastTicker = {
      t: "ticker",
      ticker,
      ts_ms: nowMs,
      bid_e4: bid,
      ask_e4: ask,
      last_e4: state.simulator.lastE4,
      volume_e2: state.simulator.volumeE2,
    };
    broadcast(ticker, state.lastTicker);
  }
}

const loop = setInterval(() => {
  const nowMs = Date.now();
  for (const state of markets.values()) stepMarket(state, nowMs);
}, STEP_MS);

/* ----------------------------------------------------------- disruptions */

function lag(client: Client): void {
  const nowMs = Date.now();
  for (const ticker of client.tickers) {
    send(client, { t: "resync", ticker, reason: "client_lag" });
    const state = markets.get(ticker);
    const snapshot = state === undefined ? null : snapshotOf(state);
    if (snapshot !== null) send(client, snapshot);
  }
  client.lagAt.push(nowMs);
  while ((client.lagAt[0] ?? nowMs) < nowMs - LAG_WINDOW_MS) client.lagAt.shift();
  if (client.lagAt.length >= LAGS_BEFORE_CLOSE) client.socket.close(4000, "too slow");
}

function busLoss(): void {
  busResets += 1;
  const nowMs = Date.now();
  for (const state of markets.values()) {
    state.book = "unknown";
    state.recoverAtMs = nowMs + 1500 + Math.floor(random() * (BUS_REFRESH_S * 1000 - 1500));
    broadcast(state.definition.ticker, {
      t: "resync",
      ticker: state.definition.ticker,
      reason: "bus_loss",
    });
  }
}

function stale(state: MarketState, seconds: number): void {
  if (state.book === "unknown") return;
  state.book = "stale";
  state.recoverAtMs = Date.now() + seconds * 1000;
  broadcast(state.definition.ticker, { t: "book", ticker: state.definition.ticker, book: "stale" });
}

function closeAll(code: number, reason: string): void {
  for (const client of clients) client.socket.close(code, reason);
}

/* ------------------------------------------------------------------ REST */

function rowOf(state: MarketState): MarketRow {
  const { definition, simulator } = state;
  return {
    ticker: definition.ticker,
    event_ticker: definition.eventTicker,
    series_ticker: definition.seriesTicker,
    title: definition.title,
    subtitle: definition.subtitle,
    category: definition.category,
    showcase: definition.showcase,
    volume_24h_e2: simulator.volumeE2,
    close_ts: Math.floor(startedAtMs / 1000) + definition.closesInHours * 3600,
    bid_e4: state.lastTicker?.bid_e4 ?? null,
    ask_e4: state.lastTicker?.ask_e4 ?? null,
    last_e4: state.lastTicker?.last_e4 ?? null,
    book: state.book,
  };
}

function detailOf(state: MarketState): MarketDetail {
  const known = state.book !== "unknown";
  const { bids, asks } = state.simulator.levels();
  return {
    ...rowOf(state),
    price_ranges: state.definition.priceRanges,
    depth: known ? { ts_ms: Date.now(), bids: bids.slice(0, 20), asks: asks.slice(0, 20) } : null,
  };
}

function statusOf(): ServiceStatus {
  const uptimeS = Math.floor((Date.now() - startedAtMs) / 1000);
  const staleBooks = [...markets.values()].filter((state) => state.book === "stale").length;
  const connection = (conn_id: number, taped: boolean, rate: number) => ({
    conn_id,
    taped,
    frames: uptimeS * rate,
    gaps: 0,
    reconnects: 0,
    stale_books: conn_id === 2 ? staleBooks : 0,
    sink_dropped: 0,
  });
  return {
    recording: true,
    recorder_status_age_ms: (Date.now() - startedAtMs) % 5000,
    recorder: {
      universe_size: 2143,
      subscribed_markets: markets.size,
      connections: [connection(0, false, 180), connection(1, true, 3), connection(2, true, 95)],
    },
    bus: {
      epoch: (BigInt(startedAtMs) * 1_000_000n).toString(),
      last_seq: messagesSent,
      messages: messagesSent,
      resets: busResets,
      missed: 0,
      books_known: [...markets.values()].filter((state) => state.book !== "unknown").length,
    },
    clients: clients.size,
  };
}

function json(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "Content-Type": "application/json", "Cache-Control": "no-store" });
  response.end(JSON.stringify(body));
}

function apiError(code: string, message: string): ErrorResponse {
  return { error: { code, message } };
}

function handleApi(request: IncomingMessage, url: URL, response: ServerResponse): void {
  if (request.method !== "GET")
    return json(response, 405, apiError("method_not_allowed", "Only GET"));
  if (url.pathname === "/api/v1/markets") {
    const limit = Number(url.searchParams.get("limit") ?? "50");
    if (!Number.isInteger(limit) || limit < 1 || limit > 200) {
      return json(response, 400, apiError("invalid_limit", "limit must be 1 to 200"));
    }
    const rows = [...markets.values()]
      .map(rowOf)
      .sort(
        (left, right) =>
          right.volume_24h_e2 - left.volume_24h_e2 || left.ticker.localeCompare(right.ticker),
      )
      .slice(0, limit);
    return json(response, 200, { markets: rows });
  }
  const detail = /^\/api\/v1\/markets\/([^/]+)$/.exec(url.pathname);
  if (detail?.[1] !== undefined) {
    const state = markets.get(decodeURIComponent(detail[1]));
    return state === undefined
      ? json(response, 404, apiError("unknown_ticker", "This market is not recorded"))
      : json(response, 200, detailOf(state));
  }
  if (url.pathname === "/api/v1/status") return json(response, 200, statusOf());
  return json(response, 404, apiError("not_found", "No such route"));
}

const HELP = `mock tape serve: POST one of
  /mock/resync?reason=client_lag|bus_loss
  /mock/stale?seconds=15[&ticker=T]
  /mock/slow
  /mock/full?seconds=30
  /mock/policy
`;

function handleControl(request: IncomingMessage, url: URL, response: ServerResponse): void {
  if (url.pathname === "/mock" && request.method === "GET") {
    response.writeHead(200, { "Content-Type": "text/plain" });
    response.end(HELP);
    return;
  }
  if (request.method !== "POST") return json(response, 405, apiError("method_not_allowed", HELP));
  const seconds = Number(url.searchParams.get("seconds") ?? "0");
  switch (url.pathname) {
    case "/mock/resync": {
      const reason = url.searchParams.get("reason") ?? "client_lag";
      if (reason === "bus_loss") busLoss();
      else if (reason === "client_lag") for (const client of clients) lag(client);
      else return json(response, 400, apiError("invalid_reason", "client_lag or bus_loss"));
      break;
    }
    case "/mock/stale": {
      const ticker = url.searchParams.get("ticker");
      for (const state of markets.values()) {
        if (ticker === null || ticker === state.definition.ticker)
          stale(state, seconds > 0 ? seconds : 15);
      }
      break;
    }
    case "/mock/slow":
      for (const client of clients)
        for (let count = 0; count < LAGS_BEFORE_CLOSE; count += 1) lag(client);
      break;
    case "/mock/full":
      fullUntilMs = Date.now() + (seconds > 0 ? seconds : 30) * 1000;
      closeAll(1013, "server full");
      break;
    case "/mock/policy":
      closeAll(1008, "policy violation");
      break;
    default:
      return json(response, 404, apiError("not_found", HELP));
  }
  console.log(`mock: ${request.method} ${url.pathname}${url.search}`);
  json(response, 200, { ok: true, action: url.pathname });
}

/* ------------------------------------------------------------- WebSocket */

function rawLength(data: RawData): number {
  if (Array.isArray(data)) return data.reduce((total, part) => total + part.length, 0);
  return data instanceof ArrayBuffer ? data.byteLength : data.length;
}

function rawText(data: RawData): string {
  if (Array.isArray(data)) return Buffer.concat(data).toString("utf8");
  return (data instanceof ArrayBuffer ? Buffer.from(data) : data).toString("utf8");
}

function onClientMessage(client: Client, data: RawData, isBinary: boolean): void {
  const nowMs = Date.now();
  client.receivedAt.push(nowMs);
  while ((client.receivedAt[0] ?? nowMs) <= nowMs - 1000) client.receivedAt.shift();
  if (client.receivedAt.length > MAX_MESSAGES_PER_SECOND)
    return client.socket.close(1008, "too many messages");
  if (rawLength(data) > MAX_MESSAGE_BYTES) return client.socket.close(1008, "message too large");
  if (isBinary)
    return send(client, { t: "error", code: "binary_frame", message: "Send text frames" });
  let request: unknown;
  try {
    request = JSON.parse(rawText(data));
  } catch {
    return send(client, { t: "error", code: "malformed_json", message: "Not JSON" });
  }
  const fields =
    typeof request === "object" && request !== null ? (request as Record<string, unknown>) : {};
  if (fields["op"] !== "subscribe")
    return send(client, { t: "error", code: "unknown_op", message: "Only subscribe" });
  const tickers = fields["tickers"];
  if (
    !Array.isArray(tickers) ||
    !tickers.every((ticker): ticker is string => typeof ticker === "string")
  ) {
    return send(client, {
      t: "error",
      code: "invalid_tickers",
      message: "tickers must be strings",
    });
  }
  const accepted: string[] = [];
  const rejected: { ticker: string; code: string }[] = [];
  for (const ticker of new Set(tickers)) {
    if (!markets.has(ticker)) rejected.push({ ticker, code: "unknown_ticker" });
    else if (accepted.length >= MAX_TICKERS) rejected.push({ ticker, code: "too_many_tickers" });
    else accepted.push(ticker);
  }
  const added = accepted.filter((ticker) => !client.tickers.includes(ticker));
  client.tickers = accepted;
  send(client, { t: "subscribed", tickers: accepted, rejected });
  for (const ticker of added) {
    const state = markets.get(ticker);
    const snapshot = state === undefined ? null : snapshotOf(state);
    if (snapshot !== null) send(client, snapshot);
    if (state !== undefined && state.lastTicker !== null) send(client, state.lastTicker);
  }
}

function originAllowed(origin: string | undefined): boolean {
  if (origin === undefined) return true;
  try {
    return ["localhost", "127.0.0.1", "[::1]"].includes(new URL(origin).hostname);
  } catch {
    return false;
  }
}

const sockets = new WebSocketServer({ noServer: true, maxPayload: 64 * 1024 });

function accept(socket: WebSocket): void {
  if (clients.size >= MAX_CLIENTS || Date.now() < fullUntilMs) {
    socket.close(1013, "server full");
    return;
  }
  const client: Client = { id: nextClientId, socket, tickers: [], receivedAt: [], lagAt: [] };
  nextClientId += 1;
  clients.add(client);
  console.log(`mock: client ${client.id} connected (${clients.size} open)`);
  send(client, { t: "hello", protocol: 1, max_tickers: MAX_TICKERS, bus_refresh_s: BUS_REFRESH_S });
  socket.on("message", (data, isBinary) => onClientMessage(client, data, isBinary));
  socket.on("close", (code) => {
    clients.delete(client);
    console.log(`mock: client ${client.id} closed ${code} (${clients.size} open)`);
  });
}

const server = createServer((request, response) => {
  const url = new URL(request.url ?? "/", `http://${HOST}:${PORT}`);
  if (url.pathname.startsWith("/mock")) handleControl(request, url, response);
  else handleApi(request, url, response);
});

server.on("upgrade", (request: IncomingMessage, socket: Duplex, head: Buffer) => {
  const url = new URL(request.url ?? "/", `http://${HOST}:${PORT}`);
  if (url.pathname !== "/api/v1/live") {
    socket.end("HTTP/1.1 404 Not Found\r\n\r\n");
    return;
  }
  if (!originAllowed(request.headers.origin)) {
    socket.end("HTTP/1.1 403 Forbidden\r\n\r\n");
    return;
  }
  sockets.handleUpgrade(request, socket, head, accept);
});

let chaosTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleChaos(): void {
  chaosTimer = setTimeout(
    () => {
      const roll = random();
      const all = [...markets.values()];
      if (roll < 0.4) for (const client of clients) lag(client);
      else if (roll < 0.7) busLoss();
      else {
        const state = all[Math.floor(random() * all.length)];
        if (state !== undefined) stale(state, 12);
      }
      console.log(`mock: chaos ${roll < 0.4 ? "client_lag" : roll < 0.7 ? "bus_loss" : "stale"}`);
      scheduleChaos();
    },
    20_000 + Math.floor(random() * 20_000),
  );
}

if (args.chaos) scheduleChaos();

server.listen(PORT, HOST, () => {
  console.log(
    `mock tape serve on http://${HOST}:${PORT} (seed ${args.seed}, chaos ${args.chaos ? "on" : "off"})`,
  );
  console.log(HELP);
});

function shutdown(): void {
  clearInterval(loop);
  if (chaosTimer !== null) clearTimeout(chaosTimer);
  closeAll(1001, "server shutting down");
  sockets.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 500).unref();
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
