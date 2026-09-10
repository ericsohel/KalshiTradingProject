import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { startPolling } from "./poll";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("startPolling", () => {
  it("requests at once and again after each interval, so an empty list refreshes by itself", async () => {
    const lists = [[], [], ["KXA"]];
    let calls = 0;
    const seen: string[][] = [];
    const stop = startPolling({
      load: () => Promise.resolve(lists[Math.min(calls++, lists.length - 1)] ?? []),
      intervalMs: 15_000,
      onData: (data) => seen.push(data),
      onError: () => undefined,
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(seen).toEqual([[]]);
    await vi.advanceTimersByTimeAsync(14_999);
    expect(calls).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(seen).toEqual([[], []]);
    await vi.advanceTimersByTimeAsync(15_000);
    expect(seen).toEqual([[], [], ["KXA"]]);
    stop();
  });

  it("keeps polling after a failure, at the same pace", async () => {
    let calls = 0;
    const errors: unknown[] = [];
    const data: number[] = [];
    const stop = startPolling({
      load: () => {
        calls += 1;
        return calls === 1 ? Promise.reject(new Error("down")) : Promise.resolve(calls);
      },
      intervalMs: 1000,
      onData: (value) => data.push(value),
      onError: (error) => errors.push(error),
    });
    await vi.advanceTimersByTimeAsync(0);
    expect(errors).toHaveLength(1);
    await vi.advanceTimersByTimeAsync(1000);
    expect(data).toEqual([2]);
    stop();
  });

  it("waits for a slow request to settle before timing the next one", async () => {
    let calls = 0;
    const stop = startPolling({
      load: () => {
        calls += 1;
        return new Promise<number>((resolve) => setTimeout(() => resolve(calls), 5000));
      },
      intervalMs: 1000,
      onData: () => undefined,
      onError: () => undefined,
    });
    await vi.advanceTimersByTimeAsync(5999);
    expect(calls).toBe(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toBe(2);
    stop();
  });

  it("stops: aborts the request in flight and calls nothing afterwards", async () => {
    let signal: AbortSignal | null = null;
    const onData = vi.fn();
    const onError = vi.fn();
    const stop = startPolling({
      load: (abort) => {
        signal = abort;
        return new Promise<string>((resolve) => setTimeout(() => resolve("late"), 100));
      },
      intervalMs: 1000,
      onData,
      onError,
    });
    stop();
    expect(signal).not.toBeNull();
    expect((signal as AbortSignal | null)?.aborted).toBe(true);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(onData).not.toHaveBeenCalled();
    expect(onError).not.toHaveBeenCalled();
  });
});
