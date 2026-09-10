import { describe, expect, it } from "vitest";
import { ColumnStatus, META_BEST_BID, META_STRIDE, NO_PRICE } from "../render/source";
import { DepthHistory } from "./depthHistory";

function profile(rows: number, value: number): Float32Array {
  return new Float32Array(rows).fill(value);
}

describe("DepthHistory", () => {
  it("starts empty and opens its first bin as given", () => {
    const history = new DepthHistory(4, 3, 100);
    expect(history.headBin).toBe(-1);
    history.advanceTo(10, ColumnStatus.unknown);
    expect([history.headBin, history.oldestBin]).toEqual([10, 10]);
    expect(history.statusAt(10)).toBe(ColumnStatus.unknown);
    expect(history.meta[(10 % 4) * META_STRIDE + META_BEST_BID]).toBe(NO_PRICE);
  });

  it("carries the head column into skipped bins", () => {
    const history = new DepthHistory(8, 3, 100);
    history.advanceTo(0, ColumnStatus.fresh);
    history.setHeadProfile(profile(3, 2));
    history.setHeadQuotes(4000, 4100);
    history.advanceTo(3, ColumnStatus.stale);
    for (const bin of [1, 2, 3]) {
      expect(history.depthAt(bin, 1)).toBe(2);
      expect(history.statusAt(bin)).toBe(ColumnStatus.stale);
    }
    expect(history.meta[3 * META_STRIDE + META_BEST_BID]).toBe(4000);
  });

  it("wraps around, keeping only the newest capacity bins", () => {
    const history = new DepthHistory(4, 2, 100);
    for (let bin = 0; bin < 10; bin += 1) {
      history.advanceTo(bin, ColumnStatus.fresh);
      history.setHeadRow(0, bin);
    }
    expect([history.headBin, history.oldestBin]).toEqual([9, 6]);
    expect([6, 7, 8, 9].map((bin) => history.depthAt(bin, 0))).toEqual([6, 7, 8, 9]);
    expect(history.depthAt(5, 0)).toBe(0);
    expect(history.statusAt(5)).toBe(ColumnStatus.unknown);
    expect(history.depth[(9 % 4) * 2]).toBe(9);
  });

  it("survives a silence longer than the whole ring", () => {
    const history = new DepthHistory(4, 2, 100);
    history.advanceTo(0, ColumnStatus.fresh);
    history.setHeadProfile(profile(2, 5));
    history.advanceTo(100, ColumnStatus.fresh);
    expect([history.headBin, history.oldestBin]).toEqual([100, 97]);
    expect([97, 98, 99, 100].map((bin) => history.depthAt(bin, 1))).toEqual([5, 5, 5, 5]);
  });

  it("never moves the head backwards", () => {
    const history = new DepthHistory(4, 2, 100);
    history.advanceTo(5, ColumnStatus.fresh);
    history.advanceTo(3, ColumnStatus.gap);
    expect(history.headBin).toBe(5);
    expect(history.statusAt(5)).toBe(ColumnStatus.fresh);
  });

  it("only raises a bin's status, and bumps revisions on every write", () => {
    const history = new DepthHistory(4, 2, 100);
    history.advanceTo(0, ColumnStatus.fresh);
    const before = history.revisions[0] ?? 0;
    history.raiseHeadStatus(ColumnStatus.gap);
    history.raiseHeadStatus(ColumnStatus.fresh);
    expect(history.statusAt(0)).toBe(ColumnStatus.gap);
    expect(history.revisions[0]).toBe(before + 1);
    history.setHeadRow(1, 3);
    expect(history.revisions[0]).toBe(before + 2);
    history.setHeadQuotes(null, null);
    expect(history.revisions[0]).toBe(before + 2);
  });
});
