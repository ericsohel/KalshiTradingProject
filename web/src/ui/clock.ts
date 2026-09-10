/** The page's clock: wall-anchored milliseconds that never run backwards. */
export function monotonicNow(): number {
  return performance.timeOrigin + performance.now();
}
