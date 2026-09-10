import { describe, expect, it } from "vitest";
import { Book } from "./book";

function sampleBook(): Book {
  const book = new Book();
  book.replace(
    [
      [5600, 1000],
      [5500, 200],
      [5000, 0],
    ],
    [
      [5700, 300],
      [5900, 50],
    ],
  );
  return book;
}

describe("Book", () => {
  it("replaces from a snapshot and finds the best prices", () => {
    const book = sampleBook();
    expect(book.bestBid).toBe(5600);
    expect(book.bestAsk).toBe(5700);
    expect(book.count("bid", 5000)).toBe(0);
    expect(book.topLevels("ask", 10)).toEqual([
      [5700, 300],
      [5900, 50],
    ]);
  });

  it("moves the best bid up on an improving delta and down when the best empties", () => {
    const book = sampleBook();
    expect(book.applyDelta("bid", 5650, 100)).toBe("applied");
    expect(book.bestBid).toBe(5650);
    book.applyDelta("bid", 5650, -100);
    expect(book.bestBid).toBe(5600);
    book.applyDelta("bid", 5600, -1000);
    expect(book.bestBid).toBe(5500);
    book.applyDelta("bid", 5500, -200);
    expect(book.bestBid).toBeNull();
  });

  it("moves the best ask symmetrically", () => {
    const book = sampleBook();
    book.applyDelta("ask", 5700, -300);
    expect(book.bestAsk).toBe(5900);
    book.applyDelta("ask", 5650, 10);
    expect(book.bestAsk).toBe(5650);
  });

  it("rejects a delta that would go negative and leaves the book unchanged", () => {
    const book = sampleBook();
    expect(book.applyDelta("ask", 5700, -301)).toBe("negative");
    expect(book.count("ask", 5700)).toBe(300);
    expect(book.applyDelta("bid", 100, -1)).toBe("negative");
  });

  it("lists top levels best first, up to a limit", () => {
    const book = sampleBook();
    expect(book.topLevels("bid", 1)).toEqual([[5600, 1000]]);
    expect(book.topLevels("bid", 10)).toEqual([
      [5600, 1000],
      [5500, 200],
    ]);
  });

  it("visits every positive level in ascending price and clears", () => {
    const book = sampleBook();
    const seen: string[] = [];
    book.forEachLevel((side, price, count) => seen.push(`${side}:${price}:${count}`));
    expect(seen).toEqual(["bid:5500:200", "bid:5600:1000", "ask:5700:300", "ask:5900:50"]);
    book.clear();
    expect([book.bestBid, book.bestAsk]).toEqual([null, null]);
    expect(book.topLevels("bid", 5)).toEqual([]);
  });
});
