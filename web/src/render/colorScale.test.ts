import { describe, expect, it } from "vitest";
import {
  depthCeilingContracts,
  depthLegendTicks,
  depthRampPosition,
  depthValue,
  MIN_CEILING_CONTRACTS,
} from "./colorScale";

describe("depth color scale", () => {
  it("starts at a minimum ceiling and rises only to powers of two", () => {
    expect(depthCeilingContracts(0)).toBe(MIN_CEILING_CONTRACTS);
    expect(depthCeilingContracts(Number.NaN)).toBe(MIN_CEILING_CONTRACTS);
    expect(depthCeilingContracts(129)).toBe(256);
    expect(depthCeilingContracts(256)).toBe(256);
    expect(depthCeilingContracts(70_000)).toBe(131_072);
  });

  it("is stable: every depth within a power of two maps to the same ceiling", () => {
    const ceilings = new Set([300, 400, 511.99, 512].map(depthCeilingContracts));
    expect([...ceilings]).toEqual([512]);
  });

  it("uses log1p of contracts and treats negatives as empty", () => {
    expect(depthValue(0)).toBe(0);
    expect(depthValue(-5)).toBe(0);
    expect(depthValue(Math.E - 1)).toBeCloseTo(1);
  });

  it("maps empty to 0, the ceiling to 1, and clamps above", () => {
    expect(depthRampPosition(0, 1024)).toBe(0);
    expect(depthRampPosition(1024, 1024)).toBe(1);
    expect(depthRampPosition(1e9, 1024)).toBe(1);
    expect(depthRampPosition(10, 1024)).toBeLessThan(depthRampPosition(100, 1024));
  });

  it("labels powers of ten up to the ceiling", () => {
    const ticks = depthLegendTicks(4096);
    expect(ticks.map((tick) => tick.contracts)).toEqual([1, 10, 100, 1000]);
    expect(ticks.every((tick) => tick.position > 0 && tick.position <= 1)).toBe(true);
  });
});
