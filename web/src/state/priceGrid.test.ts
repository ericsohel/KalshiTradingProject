import { describe, expect, it } from "vitest";
import {
  centDecimals,
  gridFromPriceRanges,
  ONE_CENT_GRID,
  priceForRow,
  rowForPrice,
  sameRows,
} from "./priceGrid";

const range = (start_e4: number, end_e4: number, step_e4: number) => ({
  start_e4,
  end_e4,
  step_e4,
});

describe("gridFromPriceRanges", () => {
  it("falls back to a one-cent grid when ranges are unknown", () => {
    expect(gridFromPriceRanges(null)).toEqual(ONE_CENT_GRID);
    expect(gridFromPriceRanges([])).toEqual(ONE_CENT_GRID);
    expect(ONE_CENT_GRID.assumed).toBe(true);
  });

  it("uses 101 rows for a cent grid and 1,001 for a tenth-of-a-cent grid", () => {
    expect(gridFromPriceRanges([range(0, 10_000, 100)])).toEqual({
      tickE4: 100,
      rowStepE4: 100,
      rows: 101,
      assumed: false,
    });
    expect(gridFromPriceRanges([range(0, 10_000, 10)]).rows).toBe(1001);
  });

  it("uses the finest step of a tapered grid", () => {
    const grid = gridFromPriceRanges([
      range(0, 1000, 10),
      range(1000, 9000, 100),
      range(9000, 10_000, 10),
    ]);
    expect(grid).toMatchObject({ tickE4: 10, rowStepE4: 10, rows: 1001 });
  });

  it("never draws rows finer than a tenth of a cent", () => {
    expect(gridFromPriceRanges([range(0, 10_000, 1)])).toMatchObject({
      tickE4: 1,
      rowStepE4: 10,
      rows: 1001,
    });
  });

  it("picks a row step that divides one dollar", () => {
    expect(gridFromPriceRanges([range(0, 10_000, 50)])).toMatchObject({ rowStepE4: 50, rows: 201 });
    expect(gridFromPriceRanges([range(0, 10_000, 30)])).toMatchObject({ rowStepE4: 40, rows: 251 });
  });
});

describe("row mapping", () => {
  const deci = gridFromPriceRanges([range(0, 10_000, 10)]);

  it("maps prices to the nearest row and back", () => {
    expect(rowForPrice(ONE_CENT_GRID, 5600)).toBe(56);
    expect(priceForRow(ONE_CENT_GRID, 56)).toBe(5600);
    expect(rowForPrice(deci, 5634)).toBe(563);
    expect(rowForPrice(deci, 5635)).toBe(564);
    expect(rowForPrice(ONE_CENT_GRID, 10_000)).toBe(100);
  });

  it("clamps to the grid", () => {
    expect(rowForPrice(ONE_CENT_GRID, -5)).toBe(0);
    expect(rowForPrice(ONE_CENT_GRID, 12_000)).toBe(100);
  });

  it("knows how many digits after the cent a grid needs", () => {
    expect(centDecimals(ONE_CENT_GRID)).toBe(0);
    expect(centDecimals(deci)).toBe(1);
    expect(centDecimals(gridFromPriceRanges([range(0, 10_000, 1)]))).toBe(2);
  });

  it("compares row layouts, not provenance", () => {
    expect(sameRows(ONE_CENT_GRID, gridFromPriceRanges([range(0, 10_000, 100)]))).toBe(true);
    expect(sameRows(ONE_CENT_GRID, deci)).toBe(false);
  });
});
