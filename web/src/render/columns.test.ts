import { describe, expect, it } from "vitest";
import { dirtyColumnRanges, heldBins, ringColumn, ringWriteRanges } from "./columns";

describe("ring columns", () => {
  it("maps bins to slots", () => {
    expect([0, 3, 4, 9].map((bin) => ringColumn(bin, 4))).toEqual([0, 3, 0, 1]);
    expect(ringColumn(-1, 4)).toBe(3);
  });

  it("counts held bins", () => {
    expect(heldBins(-1, 0)).toBe(0);
    expect(heldBins(9, 6)).toBe(4);
  });
});

describe("dirtyColumnRanges", () => {
  it("packs changed columns into contiguous ranges", () => {
    const revisions = Uint32Array.from([1, 2, 2, 0, 5, 1]);
    const uploaded = Uint32Array.from([1, 1, 1, 0, 4, 0]);
    expect(dirtyColumnRanges(revisions, uploaded, 8)).toEqual([
      { start: 1, count: 2 },
      { start: 4, count: 2 },
    ]);
  });

  it("returns nothing when all is uploaded", () => {
    const revisions = Uint32Array.from([3, 3]);
    expect(dirtyColumnRanges(revisions, revisions.slice(), 8)).toEqual([]);
  });

  it("falls back to one full upload past the range budget", () => {
    const revisions = Uint32Array.from([1, 0, 1, 0, 1]);
    expect(dirtyColumnRanges(revisions, new Uint32Array(5), 2)).toEqual([{ start: 0, count: 5 }]);
  });
});

describe("ringWriteRanges", () => {
  it("covers new writes without wrapping", () => {
    expect(ringWriteRanges(2, 5, 8)).toEqual([{ start: 2, count: 3 }]);
    expect(ringWriteRanges(5, 5, 8)).toEqual([]);
  });

  it("splits writes that wrap the ring", () => {
    expect(ringWriteRanges(6, 11, 8)).toEqual([
      { start: 0, count: 3 },
      { start: 6, count: 2 },
    ]);
    expect(ringWriteRanges(14, 16, 8)).toEqual([{ start: 6, count: 2 }]);
  });

  it("uploads the whole ring after a full lap", () => {
    expect(ringWriteRanges(3, 11, 8)).toEqual([{ start: 0, count: 8 }]);
    expect(ringWriteRanges(0, 100, 8)).toEqual([{ start: 0, count: 8 }]);
  });
});
