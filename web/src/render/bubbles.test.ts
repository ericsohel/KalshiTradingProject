import { describe, expect, it } from "vitest";
import {
  BUBBLE_CORNERS,
  BUBBLE_MAX_RADIUS_PX,
  BUBBLE_MIN_RADIUS_PX,
  bubbleRadiusPx,
  bubbleReferenceContracts,
  MIN_REFERENCE_CONTRACTS,
  packTradeInstance,
} from "./bubbles";
import { TRADE_STRIDE } from "./source";

describe("bubble geometry", () => {
  it("makes area proportional to contracts between the clamps", () => {
    const reference = 1024;
    const small = bubbleRadiusPx(64, reference);
    const large = bubbleRadiusPx(256, reference);
    expect((large * large) / (small * small)).toBeCloseTo(4);
  });

  it("reaches the maximum radius at the reference size and clamps beyond it", () => {
    expect(bubbleRadiusPx(1024, 1024)).toBe(BUBBLE_MAX_RADIUS_PX);
    expect(bubbleRadiusPx(1e9, 1024)).toBe(BUBBLE_MAX_RADIUS_PX);
  });

  it("keeps tiny trades visible", () => {
    expect(bubbleRadiusPx(0.01, 1024)).toBe(BUBBLE_MIN_RADIUS_PX);
    expect(bubbleRadiusPx(-3, 1024)).toBe(BUBBLE_MIN_RADIUS_PX);
  });

  it("raises the reference by powers of four", () => {
    expect(bubbleReferenceContracts(0)).toBe(MIN_REFERENCE_CONTRACTS);
    expect(bubbleReferenceContracts(257)).toBe(1024);
    expect(bubbleReferenceContracts(5000)).toBe(16_384);
  });

  it("draws a quad as two counter-clockwise triangles covering the unit square", () => {
    expect(BUBBLE_CORNERS).toHaveLength(6);
    let area = 0;
    for (let triangle = 0; triangle < 2; triangle += 1) {
      const [a, b, c] = [0, 1, 2].map((vertex) => BUBBLE_CORNERS[triangle * 3 + vertex] ?? [0, 0]);
      const signed =
        ((b?.[0] ?? 0) - (a?.[0] ?? 0)) * ((c?.[1] ?? 0) - (a?.[1] ?? 0)) -
        ((c?.[0] ?? 0) - (a?.[0] ?? 0)) * ((b?.[1] ?? 0) - (a?.[1] ?? 0));
      expect(signed).toBeGreaterThan(0);
      area += signed / 2;
    }
    expect(area).toBe(4);
  });

  it("packs a trade as bin, price, contracts, side", () => {
    const instances = new Float32Array(3 * TRADE_STRIDE);
    packTradeInstance(instances, 2, 12.5, 5700, 1250, "ask");
    expect(Array.from(instances.subarray(2 * TRADE_STRIDE))).toEqual([12.5, 5700, 12.5, 1]);
    packTradeInstance(instances, 0, 1, 100, 100, "bid");
    expect(instances[3]).toBe(0);
  });
});
