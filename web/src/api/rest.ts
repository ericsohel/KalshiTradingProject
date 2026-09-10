/**
 * The REST half of the live API: `/markets`, `/markets/{ticker}`, and `/status`.
 *
 * Every request has a timeout and every response is decoded before it is returned, so a
 * caller receives either a typed value or an `ApiRequestError` with a stable `code`.
 * The fetch function and base URL are injected; nothing here reads globals.
 */

import {
  decodeApiError,
  decodeMarketDetail,
  decodeMarketList,
  decodeServiceStatus,
  type DecodeResult,
  type MarketList,
} from "./decode";
import type { MarketDetail, ServiceStatus } from "./protocol";

/** Where the page finds the API. */
export interface ApiEndpoints {
  /** Absolute base of REST routes, ending in `/api/v1`. */
  readonly restBase: string;
  /** Absolute `ws:` or `wss:` URL of the live feed. */
  readonly liveUrl: string;
}

/**
 * Resolves the API location.
 *
 * @param pageUrl The page's own URL; the API is same-origin unless `apiOrigin` is set
 *   (development goes through the Vite proxy, so it is always same-origin).
 * @param apiOrigin An absolute origin such as `https://api.example.org`, from the build's
 *   `VITE_TAPE_API_ORIGIN`, for a static site hosted apart from the API.
 */
export function resolveEndpoints(pageUrl: string, apiOrigin: string | undefined): ApiEndpoints {
  const origin = new URL(apiOrigin !== undefined && apiOrigin !== "" ? apiOrigin : pageUrl);
  const restBase = new URL("/api/v1", origin.origin).href;
  const live = new URL("/api/v1/live", origin.origin);
  live.protocol = live.protocol === "https:" ? "wss:" : "ws:";
  return { restBase, liveUrl: live.href };
}

export type FetchLike = (input: string, init: RequestInit) => Promise<Response>;

export type ApiErrorCode =
  "network" | "timeout" | "aborted" | "malformed_response" | (string & Record<never, never>);

/** A failed API request. `code` is the server's error code when it sent one. */
export class ApiRequestError extends Error {
  override readonly name = "ApiRequestError";
  readonly code: ApiErrorCode;
  /** HTTP status, or `null` when no response arrived. */
  readonly status: number | null;

  constructor(code: ApiErrorCode, status: number | null, message: string) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

export interface RestClientOptions {
  readonly restBase: string;
  readonly fetch: FetchLike;
  /** Per-request timeout; default 10 s. */
  readonly timeoutMs?: number;
}

const DEFAULT_TIMEOUT_MS = 10_000;

/** Largest page `GET /markets` accepts. */
export const MAX_MARKETS_LIMIT = 200;

export class RestClient {
  readonly #restBase: string;
  readonly #fetch: FetchLike;
  readonly #timeoutMs: number;

  constructor(options: RestClientOptions) {
    this.#restBase = options.restBase.replace(/\/+$/, "");
    this.#fetch = options.fetch;
    this.#timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  }

  /** Markets ranked by 24-hour volume. `limit` is clamped to 1..200. */
  listMarkets(limit: number, signal?: AbortSignal): Promise<MarketList> {
    const clamped = Math.min(MAX_MARKETS_LIMIT, Math.max(1, Math.floor(limit)));
    return this.#get(`/markets?limit=${clamped}`, decodeMarketList, signal);
  }

  /** One market with its price grid and top-20 depth. Rejects with `unknown_ticker` on 404. */
  getMarket(ticker: string, signal?: AbortSignal): Promise<MarketDetail> {
    return this.#get(`/markets/${encodeURIComponent(ticker)}`, decodeMarketDetail, signal);
  }

  /** Recorder and bus health. */
  getStatus(signal?: AbortSignal): Promise<ServiceStatus> {
    return this.#get("/status", decodeServiceStatus, signal);
  }

  async #get<T>(
    route: string,
    decode: (json: unknown) => DecodeResult<T>,
    signal: AbortSignal | undefined,
  ): Promise<T> {
    const timeout = AbortSignal.timeout(this.#timeoutMs);
    const combined = signal === undefined ? timeout : AbortSignal.any([signal, timeout]);
    const url = `${this.#restBase}${route}`;
    let response: Response;
    let body: unknown;
    try {
      response = await this.#fetch(url, {
        method: "GET",
        headers: { Accept: "application/json" },
        signal: combined,
      });
      body = await response.json().catch(() => undefined);
    } catch (error: unknown) {
      if (signal?.aborted === true) throw new ApiRequestError("aborted", null, "Request aborted");
      if (timeout.aborted) throw new ApiRequestError("timeout", null, `No response from ${route}`);
      const detail = error instanceof Error ? error.message : "request failed";
      throw new ApiRequestError("network", null, detail);
    }
    if (!response.ok) {
      const apiError = decodeApiError(body);
      throw apiError.ok
        ? new ApiRequestError(
            apiError.value.error.code,
            response.status,
            apiError.value.error.message,
          )
        : new ApiRequestError(
            "http_error",
            response.status,
            `HTTP ${response.status} from ${route}`,
          );
    }
    const decoded = decode(body);
    if (!decoded.ok)
      throw new ApiRequestError("malformed_response", response.status, decoded.error);
    return decoded.value;
  }
}
