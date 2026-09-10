/**
 * Reconnect delays: capped exponential backoff with full jitter.
 *
 * Full jitter spreads a crowd of viewers that lost the server at the same instant, so a
 * restarted API is not hit by all of them in one burst. Invariant: the delay is always
 * in `[floorMs, capMs]` and never negative or NaN, whatever `attempt` and `random` are.
 */

export interface BackoffPolicy {
  /** Ceiling of the first retry's jitter window. */
  readonly baseMs: number;
  /** No delay exceeds this. */
  readonly capMs: number;
}

export const DEFAULT_BACKOFF: BackoffPolicy = { baseMs: 500, capMs: 30_000 };

/** Exponents beyond this would only ever hit the cap; clamping avoids `Infinity`. */
const MAX_EXPONENT = 30;

/**
 * The delay before reconnect attempt `attempt` (0 for the first retry).
 *
 * @param attempt Consecutive failed attempts so far; negative values count as 0.
 * @param policy Base and cap.
 * @param random A uniform source in [0, 1), injected so tests are deterministic.
 * @param floorMs A minimum the close reason demands (a full server wants a pause).
 * @returns Milliseconds, an integer in `[min(floorMs, capMs), capMs]`.
 */
export function backoffDelayMs(
  attempt: number,
  policy: BackoffPolicy,
  random: () => number,
  floorMs = 0,
): number {
  const exponent = Math.min(Math.max(0, Math.floor(attempt)), MAX_EXPONENT);
  const window = Math.min(policy.capMs, policy.baseMs * 2 ** exponent);
  const unit = Math.min(Math.max(random(), 0), 1);
  const jittered = Math.floor(unit * window);
  const floor = Math.min(Math.max(0, floorMs), policy.capMs);
  return Math.min(policy.capMs, floor + (Number.isFinite(jittered) ? jittered : 0));
}
