import { describe, expect, it } from "vitest";
import { buildViridisLut, sampleViridis, viridisCssGradient } from "./colormap";

const luminance = ([red, green, blue]: readonly number[]): number =>
  0.2126 * (red ?? 0) + 0.7152 * (green ?? 0) + 0.0722 * (blue ?? 0);

describe("viridis colormap", () => {
  it("matches viridis at both ends", () => {
    expect(sampleViridis(0)).toEqual([0x44, 0x01, 0x54]);
    expect(sampleViridis(1)).toEqual([0xfd, 0xe7, 0x25]);
  });

  it("clamps out-of-range and NaN positions", () => {
    expect(sampleViridis(-1)).toEqual(sampleViridis(0));
    expect(sampleViridis(9)).toEqual(sampleViridis(1));
    expect(sampleViridis(Number.NaN)).toEqual(sampleViridis(0));
  });

  it("brightens monotonically, so more depth always reads brighter", () => {
    const samples = Array.from({ length: 101 }, (_, index) =>
      luminance(sampleViridis(index / 100)),
    );
    for (let index = 1; index < samples.length; index += 1) {
      expect(samples[index]).toBeGreaterThanOrEqual((samples[index - 1] ?? 0) - 0.5);
    }
  });

  it("builds an opaque RGBA lookup table", () => {
    const lut = buildViridisLut(256);
    expect(lut).toHaveLength(1024);
    expect(Array.from(lut.subarray(0, 4))).toEqual([0x44, 0x01, 0x54, 255]);
    expect(Array.from(lut.subarray(1020))).toEqual([0xfd, 0xe7, 0x25, 255]);
    expect(lut.filter((_, index) => index % 4 === 3).every((alpha) => alpha === 255)).toBe(true);
  });

  it("writes a CSS gradient from the same anchors", () => {
    const css = viridisCssGradient(3);
    expect(css).toBe(
      `linear-gradient(to right, rgb(68 1 84) 0%, ${`rgb(${sampleViridis(0.5).join(" ")})`} 50%, rgb(253 231 37) 100%)`,
    );
  });
});
