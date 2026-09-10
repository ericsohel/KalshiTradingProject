import { describe, expect, it } from "vitest";
import { backoffDelayMs } from "./backoff";

const policy = { baseMs: 500, capMs: 30_000 };

describe("backoffDelayMs", () => {
  it("draws from a window that doubles per attempt", () => {
    expect([0, 1, 2, 3].map((attempt) => backoffDelayMs(attempt, policy, () => 0.5))).toEqual([
      250, 500, 1000, 2000,
    ]);
  });

  it("never exceeds the cap, even for huge attempts", () => {
    expect(backoffDelayMs(10, policy, () => 0.999)).toBeLessThanOrEqual(30_000);
    expect(backoffDelayMs(10_000, policy, () => 0.999)).toBeLessThanOrEqual(30_000);
  });

  it("spans the whole window with full jitter", () => {
    expect(backoffDelayMs(3, policy, () => 0)).toBe(0);
    expect(backoffDelayMs(3, policy, () => 0.9999)).toBe(3999);
  });

  it("adds a floor demanded by the close code, still capped", () => {
    expect(backoffDelayMs(0, policy, () => 0, 5000)).toBe(5000);
    expect(backoffDelayMs(20, policy, () => 0.99, 15_000)).toBe(30_000);
  });

  it("tolerates nonsense inputs", () => {
    expect(backoffDelayMs(-3, policy, () => 0.5)).toBe(250);
    expect(backoffDelayMs(2, policy, () => Number.NaN)).toBe(0);
    expect(backoffDelayMs(2, policy, () => 7)).toBe(2000);
  });
});
