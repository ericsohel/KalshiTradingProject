/**
 * Which listed markets have closed by the page's clock, and when the next one closes. The market
 * list is polled, so it can be older than a close: the page labels a market closed itself once
 * its close time passes (ADR 0029). Pure, apart from the timer `watchCloses` sets.
 */

/** The longest delay `setTimeout` honours; a later close is waited for in steps of it. */
const MAX_TIMER_MS = 2_147_483_647;

/** Whether a market has closed: its close time, in Unix seconds, is at or before `nowMs`. */
export function isClosed(closeTs: number | null, nowMs: number): boolean {
  return closeTs !== null && closeTs * 1000 <= nowMs;
}

/**
 * The latest of `closeTimes` at or before `nowMs`, in milliseconds.
 *
 * It changes only when a close passes, so it is a clock to render closes by: for each of
 * `closeTimes`, `isClosed(closeTs, lastCloseMs(closeTimes, nowMs))` equals
 * `isClosed(closeTs, nowMs)`.
 *
 * @returns `null` when none has passed.
 */
export function lastCloseMs(closeTimes: readonly (number | null)[], nowMs: number): number | null {
  let last: number | null = null;
  for (const closeTs of closeTimes) {
    if (closeTs === null) continue;
    const closeMs = closeTs * 1000;
    if (closeMs <= nowMs && (last === null || closeMs > last)) last = closeMs;
  }
  return last;
}

/**
 * Milliseconds from `nowMs` until the earliest of `closeTimes` still ahead.
 *
 * @returns `null` when none is ahead.
 */
export function msUntilNextClose(
  closeTimes: readonly (number | null)[],
  nowMs: number,
): number | null {
  let next: number | null = null;
  for (const closeTs of closeTimes) {
    if (closeTs === null) continue;
    const delayMs = closeTs * 1000 - nowMs;
    if (delayMs > 0 && (next === null || delayMs < next)) next = delayMs;
  }
  return next;
}

/**
 * Calls `onClose` as each of `closeTimes` passes on the clock `now`, until the returned function
 * stops it. Closes at the same moment call it once.
 */
export function watchCloses(
  closeTimes: readonly (number | null)[],
  now: () => number,
  onClose: () => void,
): () => void {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const schedule = (): void => {
    const delayMs = msUntilNextClose(closeTimes, now());
    if (delayMs === null) {
      timer = undefined;
      return;
    }
    timer = setTimeout(
      () => {
        if (delayMs <= MAX_TIMER_MS) onClose();
        schedule();
      },
      Math.min(delayMs, MAX_TIMER_MS),
    );
  };
  schedule();
  return () => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
}
