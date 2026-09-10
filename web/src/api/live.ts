/**
 * The live feed client: one WebSocket to `/api/v1/live`, kept alive (FRONTEND 4.2).
 *
 * Responsibilities: connect, wait for `hello` and check the protocol, (re)send the
 * desired subscription, forward decoded market messages, and reconnect with capped,
 * jittered exponential backoff. Close codes pick the pause: 1013 (server full) and 1008
 * (policy violation) wait longer than an ordinary drop, 4000 (too slow) a little.
 *
 * Invariants: at most one socket is current, and events from a replaced socket are
 * ignored; subscribe messages are coalesced and spaced by `minSubscribeIntervalMs`, which
 * keeps the client under the server's 10-per-second limit; a subscribe message never
 * exceeds 4 KB; every timer is cleared by `stop()`. Market state is not kept here: the
 * listener receives messages and an explicit "connection lost" signal and owns recovery.
 */

import { backoffDelayMs, DEFAULT_BACKOFF, type BackoffPolicy } from "./backoff";
import { decodeFrame } from "./decode";
import {
  CloseCode,
  MAX_CLIENT_MESSAGE_BYTES,
  PROTOCOL_VERSION,
  type HelloMessage,
  type MarketMessage,
  type RejectedTicker,
  type SubscribeRequest,
} from "./protocol";

/** Callbacks a socket implementation reports into. */
export interface SocketEvents {
  onOpen(): void;
  onMessage(data: unknown): void;
  onClose(code: number, reason: string): void;
  onError(): void;
}

/** The two operations the client needs from a WebSocket. */
export interface LiveSocket {
  send(data: string): void;
  close(code: number, reason: string): void;
}

/** Opens a socket to `url` reporting into `events`. May throw for an invalid URL. */
export type SocketFactory = (url: string, events: SocketEvents) => LiveSocket;

/** The browser's WebSocket behind the `SocketFactory` seam. */
export const browserSocketFactory: SocketFactory = (url, events) => {
  const socket = new WebSocket(url);
  socket.onopen = () => events.onOpen();
  socket.onmessage = (event: MessageEvent<unknown>) => events.onMessage(event.data);
  socket.onclose = (event: CloseEvent) => events.onClose(event.code, event.reason);
  socket.onerror = () => events.onError();
  return {
    send: (data) => socket.send(data),
    close: (code, reason) => socket.close(code, reason),
  };
};

export type ConnectionPhase =
  /** Not started. */
  | "idle"
  /** Socket opening. */
  | "connecting"
  /** Open, waiting for `hello`. */
  | "handshaking"
  /** `hello` received and accepted. */
  | "live"
  /** Waiting to reconnect (see `retryAtMs`). */
  | "waiting"
  /** Stopped by the page. */
  | "stopped"
  /** The server speaks another protocol; the client will not retry. */
  | "incompatible";

export interface CloseInfo {
  readonly code: number;
  readonly reason: string;
  readonly atMs: number;
  /** A sentence for a person, not a log line. */
  readonly explanation: string;
}

/** A snapshot of the connection for display. Replaced, never mutated. */
export interface ConnectionState {
  readonly phase: ConnectionPhase;
  /** Consecutive reconnects without a stable connection. */
  readonly attempt: number;
  readonly retryAtMs: number | null;
  readonly lastClose: CloseInfo | null;
  readonly hello: HelloMessage | null;
  /** Tickers the server confirmed in its latest `subscribed` reply. */
  readonly subscribed: readonly string[];
  readonly rejected: readonly RejectedTicker[];
  readonly lastServerError: { readonly code: string; readonly message: string } | null;
  /** Frames that failed to decode, over the client's lifetime. */
  readonly malformedFrames: number;
  /** Well-formed frames of a type this client does not know. */
  readonly ignoredFrames: number;
}

export interface LiveClientListener {
  /** A decoded message about one market, with the local receive time. */
  onMarketMessage(message: MarketMessage, receivedAtMs: number): void;
  /**
   * The connection that carried market messages is gone; every book it maintained is
   * now unknown. Called once per lost connection, before any reconnect.
   */
  onConnectionLost(atMs: number): void;
  onStateChange(state: ConnectionState): void;
}

export interface LiveClientOptions {
  readonly url: string;
  readonly createSocket: SocketFactory;
  readonly listener: LiveClientListener;
  /** Milliseconds on a clock that never goes backwards. */
  readonly now: () => number;
  readonly random: () => number;
  readonly backoff?: BackoffPolicy;
  /** Give up on a socket that opened but sent no `hello`; default 10 s. */
  readonly helloTimeoutMs?: number;
  /** A connection live this long resets the backoff; default 30 s. */
  readonly stableAfterMs?: number;
  /** Minimum spacing of subscribe messages; default 250 ms (at most 4 per second). */
  readonly minSubscribeIntervalMs?: number;
}

interface ClosePolicy {
  readonly floorMs: number;
  readonly explanation: string;
}

/**
 * How long to pause, at least, after a close code, and how to explain it.
 *
 * @param code The WebSocket close code.
 */
export function closePolicy(code: number): ClosePolicy {
  switch (code) {
    case CloseCode.policyViolation:
      return {
        floorMs: 15_000,
        explanation: "The server refused a message from this page (policy violation).",
      };
    case CloseCode.tryAgainLater:
      return { floorMs: 5_000, explanation: "The live server is at capacity." };
    case CloseCode.tooSlow:
      return {
        floorMs: 2_000,
        explanation: "This connection fell behind the live feed too often.",
      };
    case CloseCode.normal:
    case CloseCode.goingAway:
      return { floorMs: 0, explanation: "The server closed the connection." };
    default:
      return { floorMs: 0, explanation: "The connection to the live server was lost." };
  }
}

const encoder = new TextEncoder();

type Timer = ReturnType<typeof setTimeout>;

/** Clears `timer` if set; returns `null` for assignment back to the field. */
function cancel(timer: Timer | null): null {
  if (timer !== null) clearTimeout(timer);
  return null;
}

/** Order-preserving de-duplication of a ticker list. */
function uniqueTickers(tickers: readonly string[]): string[] {
  return [...new Set(tickers.filter((ticker) => ticker.length > 0))];
}

function sameList(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((item, index) => item === right[index]);
}

export class LiveClient {
  readonly #options: LiveClientOptions;
  readonly #backoff: BackoffPolicy;
  readonly #helloTimeoutMs: number;
  readonly #stableAfterMs: number;
  readonly #minSubscribeIntervalMs: number;

  #state: ConnectionState = {
    phase: "idle",
    attempt: 0,
    retryAtMs: null,
    lastClose: null,
    hello: null,
    subscribed: [],
    rejected: [],
    lastServerError: null,
    malformedFrames: 0,
    ignoredFrames: 0,
  };

  #socket: LiveSocket | null = null;
  /** Identifies the current socket; events carrying another value are stale. */
  #generation = 0;
  #liveSinceMs: number | null = null;
  #reconnectTimer: Timer | null = null;
  #helloTimer: Timer | null = null;
  #subscribeTimer: Timer | null = null;

  #desired: string[] = [];
  /** What the current connection was last told; `null` before the first subscribe. */
  #sent: string[] | null = null;
  #lastSubscribeAtMs = Number.NEGATIVE_INFINITY;
  /** Tickers to drop and re-add so the server sends a fresh snapshot. */
  #refresh = new Set<string>();

  constructor(options: LiveClientOptions) {
    this.#options = options;
    this.#backoff = options.backoff ?? DEFAULT_BACKOFF;
    this.#helloTimeoutMs = options.helloTimeoutMs ?? 10_000;
    this.#stableAfterMs = options.stableAfterMs ?? 30_000;
    this.#minSubscribeIntervalMs = options.minSubscribeIntervalMs ?? 250;
  }

  get state(): ConnectionState {
    return this.#state;
  }

  /** Opens the connection. Idempotent while running; restarts after `stop()`. */
  start(): void {
    if (this.#state.phase !== "idle" && this.#state.phase !== "stopped") return;
    this.#update({ attempt: 0, retryAtMs: null });
    this.#connect();
  }

  /** Closes the connection and cancels every timer. */
  stop(): void {
    if (this.#state.phase === "idle" || this.#state.phase === "stopped") return;
    const wasLive = this.#state.phase === "live";
    this.#dropSocket(CloseCode.normal, "client stopped");
    this.#reconnectTimer = cancel(this.#reconnectTimer);
    if (wasLive) this.#options.listener.onConnectionLost(this.#options.now());
    this.#update({ phase: "stopped", retryAtMs: null, subscribed: [], rejected: [] });
  }

  /**
   * Sets the tickers this page wants. Sent now if connected and allowed by the spacing
   * rule, later otherwise, and again after every reconnect.
   */
  setSubscription(tickers: readonly string[]): void {
    this.#desired = uniqueTickers(tickers);
    for (const ticker of this.#refresh) {
      if (!this.#desired.includes(ticker)) this.#refresh.delete(ticker);
    }
    this.#scheduleSubscribe();
  }

  /**
   * Asks for a new snapshot of `ticker` after the local book was found inconsistent,
   * by removing it from the subscription and adding it back. Protocol 1 has no explicit
   * snapshot request; a newly subscribed known book is always sent a snapshot.
   */
  requestSnapshot(ticker: string): void {
    if (!this.#desired.includes(ticker)) return;
    this.#refresh.add(ticker);
    this.#scheduleSubscribe();
  }

  #connect(): void {
    this.#generation += 1;
    const generation = this.#generation;
    this.#update({ phase: "connecting", retryAtMs: null, hello: null });
    const events: SocketEvents = {
      onOpen: () => {
        if (generation === this.#generation) this.#handleOpen();
      },
      onMessage: (data) => {
        if (generation === this.#generation) this.#handleFrame(data);
      },
      onClose: (code, reason) => {
        if (generation === this.#generation) this.#handleClose(code, reason);
      },
      // A close event always follows an error event; the close decides what happens.
      onError: () => undefined,
    };
    try {
      this.#socket = this.#options.createSocket(this.#options.url, events);
    } catch (error: unknown) {
      const detail = error instanceof Error ? error.message : "could not open socket";
      this.#socket = null;
      this.#handleClose(CloseCode.abnormal, detail);
    }
  }

  #handleOpen(): void {
    this.#update({ phase: "handshaking" });
    this.#helloTimer = setTimeout(() => {
      this.#helloTimer = null;
      this.#closeAndRetry(CloseCode.normal, "no hello from server");
    }, this.#helloTimeoutMs);
  }

  #handleFrame(data: unknown): void {
    const receivedAtMs = this.#options.now();
    const decoded = decodeFrame(data);
    if (decoded.kind === "malformed") {
      this.#update({ malformedFrames: this.#state.malformedFrames + 1 });
      return;
    }
    if (decoded.kind === "ignored") {
      this.#update({ ignoredFrames: this.#state.ignoredFrames + 1 });
      return;
    }
    const message = decoded.message;
    switch (message.t) {
      case "hello":
        this.#handleHello(message);
        return;
      case "subscribed":
        this.#update({ subscribed: message.tickers, rejected: message.rejected });
        return;
      case "error":
        this.#update({ lastServerError: { code: message.code, message: message.message } });
        return;
      case "snapshot":
      case "delta":
      case "book":
      case "resync":
      case "trade":
      case "ticker":
        if (this.#state.phase === "live") {
          this.#options.listener.onMarketMessage(message, receivedAtMs);
        }
        return;
    }
  }

  #handleHello(hello: HelloMessage): void {
    this.#helloTimer = cancel(this.#helloTimer);
    if (this.#state.phase !== "handshaking") return;
    if (hello.protocol !== PROTOCOL_VERSION) {
      this.#dropSocket(CloseCode.normal, "unsupported protocol");
      this.#update({
        phase: "incompatible",
        hello,
        lastClose: {
          code: CloseCode.normal,
          reason: "unsupported protocol",
          atMs: this.#options.now(),
          explanation: `The server speaks protocol ${hello.protocol}; this page speaks ${PROTOCOL_VERSION}. Reload to update.`,
        },
      });
      return;
    }
    this.#liveSinceMs = this.#options.now();
    this.#sent = null;
    this.#lastSubscribeAtMs = Number.NEGATIVE_INFINITY;
    this.#refresh.clear();
    this.#update({ phase: "live", hello, subscribed: [], rejected: [] });
    this.#scheduleSubscribe();
  }

  #handleClose(code: number, reason: string): void {
    const atMs = this.#options.now();
    const wasLive = this.#state.phase === "live";
    const stable = this.#liveSinceMs !== null && atMs - this.#liveSinceMs >= this.#stableAfterMs;
    this.#socket = null;
    this.#liveSinceMs = null;
    this.#helloTimer = cancel(this.#helloTimer);
    this.#subscribeTimer = cancel(this.#subscribeTimer);
    if (wasLive) this.#options.listener.onConnectionLost(atMs);
    const attempt = stable ? 0 : this.#state.attempt;
    const policy = closePolicy(code);
    const delay = backoffDelayMs(attempt, this.#backoff, this.#options.random, policy.floorMs);
    this.#update({
      phase: "waiting",
      attempt: attempt + 1,
      retryAtMs: atMs + delay,
      lastClose: { code, reason, atMs, explanation: policy.explanation },
      subscribed: [],
      rejected: [],
    });
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      this.#connect();
    }, delay);
  }

  #closeAndRetry(code: number, reason: string): void {
    this.#dropSocket(code, reason);
    this.#handleClose(CloseCode.abnormal, reason);
  }

  /** Detaches and closes the current socket so its later events are ignored. */
  #dropSocket(code: number, reason: string): void {
    const socket = this.#socket;
    this.#socket = null;
    this.#generation += 1;
    this.#liveSinceMs = null;
    this.#helloTimer = cancel(this.#helloTimer);
    this.#subscribeTimer = cancel(this.#subscribeTimer);
    if (socket === null) return;
    try {
      socket.close(code, reason);
    } catch {
      // Closing a socket that never opened can throw; it is discarded either way.
    }
  }

  #scheduleSubscribe(): void {
    if (this.#state.phase !== "live" || this.#subscribeTimer !== null) return;
    const waitMs = this.#lastSubscribeAtMs + this.#minSubscribeIntervalMs - this.#options.now();
    if (waitMs <= 0) {
      this.#flushSubscription();
      return;
    }
    this.#subscribeTimer = setTimeout(() => {
      this.#subscribeTimer = null;
      this.#flushSubscription();
    }, waitMs);
  }

  #flushSubscription(): void {
    const socket = this.#socket;
    if (this.#state.phase !== "live" || socket === null) return;
    const refreshing = this.#refresh.size > 0;
    const tickers = refreshing
      ? this.#desired.filter((ticker) => !this.#refresh.has(ticker))
      : this.#desired;
    this.#refresh.clear();
    if (this.#sent !== null && sameList(this.#sent, tickers)) return;
    if (this.#sent === null && tickers.length === 0) return;
    const request: SubscribeRequest = { op: "subscribe", tickers };
    const payload = JSON.stringify(request);
    if (encoder.encode(payload).length > MAX_CLIENT_MESSAGE_BYTES) {
      this.#update({
        lastServerError: { code: "subscription_too_large", message: "Too many tickers to send." },
      });
      return;
    }
    socket.send(payload);
    this.#sent = [...tickers];
    this.#lastSubscribeAtMs = this.#options.now();
    // A refresh sent the reduced set; send the full set once the spacing allows.
    if (refreshing) this.#scheduleSubscribe();
  }

  #update(patch: Partial<ConnectionState>): void {
    this.#state = { ...this.#state, ...patch };
    this.#options.listener.onStateChange(this.#state);
  }
}
