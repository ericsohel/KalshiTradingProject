/**
 * Repeats a REST request on an interval, for the resources the page keeps current: the
 * market list, the selected market's detail, and the service status.
 *
 * Invariants: at most one request is in flight; the next starts `intervalMs` after the
 * previous one settled, whether it succeeded or failed, so an API that is down, or one
 * that has no market list yet, is asked again at the same pace without a reload; after
 * the returned stop function runs, the request in flight is aborted, no timer is pending,
 * and no callback is called.
 */

export interface PollOptions<T> {
  /** Performs one request; `signal` aborts it. */
  readonly load: (signal: AbortSignal) => Promise<T>;
  /** Pause between one request settling and the next starting. */
  readonly intervalMs: number;
  readonly onData: (data: T) => void;
  readonly onError: (error: unknown) => void;
}

/**
 * Requests at once, then again every `intervalMs` after each request settles.
 *
 * @returns A function that stops polling.
 */
export function startPolling<T>(options: PollOptions<T>): () => void {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const run = async (): Promise<void> => {
    try {
      const data = await options.load(controller.signal);
      if (!controller.signal.aborted) options.onData(data);
    } catch (error: unknown) {
      if (!controller.signal.aborted) options.onError(error);
    }
    if (!controller.signal.aborted) timer = setTimeout(() => void run(), options.intervalMs);
  };
  void run();
  return () => {
    controller.abort();
    clearTimeout(timer);
  };
}
