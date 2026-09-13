import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MarketRow } from "../api/protocol";
import { isClosed, lastCloseMs, msUntilNextClose, watchCloses } from "./closes";
import { monotonicNow } from "./clock";
import { MarketPicker } from "./MarketPicker";

const NOW_MS = 1_800_000_000_000;
const NOW_S = NOW_MS / 1000;

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(NOW_MS);
});

afterEach(() => {
  vi.useRealTimers();
});

describe("closes", () => {
  it("counts a close as passed from its second on", () => {
    expect(isClosed(NOW_S, NOW_MS)).toBe(true);
    expect(isClosed(NOW_S + 1, NOW_MS + 999)).toBe(false);
    expect(isClosed(null, NOW_MS)).toBe(false);
    const closeTimes = [NOW_S + 30, null, NOW_S - 5, NOW_S - 60];
    expect(lastCloseMs(closeTimes, NOW_MS)).toBe(NOW_MS - 5_000);
    expect(lastCloseMs([NOW_S + 1, null], NOW_MS)).toBeNull();
    expect(msUntilNextClose(closeTimes, NOW_MS)).toBe(30_000);
    expect(msUntilNextClose([NOW_S, null], NOW_MS)).toBeNull();
  });

  it("tells the list at each close, once per moment, and not before", () => {
    const closeTimes = [NOW_S + 30, null, NOW_S + 10, NOW_S + 10, NOW_S - 5];
    let lastClose = lastCloseMs(closeTimes, Date.now());
    const labels = (): boolean[] =>
      closeTimes.map((closeTs) => lastClose !== null && isClosed(closeTs, lastClose));
    const onClose = vi.fn(() => {
      lastClose = lastCloseMs(closeTimes, Date.now());
    });
    const stop = watchCloses(closeTimes, () => Date.now(), onClose);
    expect(labels()).toEqual([false, false, false, false, true]);

    vi.advanceTimersByTime(9_999);
    expect(onClose).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(labels()).toEqual([false, false, true, true, true]);

    vi.advanceTimersByTime(20_000);
    expect(onClose).toHaveBeenCalledTimes(2);
    expect(labels()).toEqual([true, false, true, true, true]);
    vi.advanceTimersByTime(3_600_000);
    expect(onClose).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
    stop();
  });

  it("waits for a close beyond the longest timer in steps, and stops when asked", () => {
    const farS = NOW_S + 30 * 86_400;
    const onClose = vi.fn();
    const stop = watchCloses([farS], () => Date.now(), onClose);
    vi.advanceTimersByTime(2_147_483_647);
    expect(onClose).not.toHaveBeenCalled();
    expect(vi.getTimerCount()).toBe(1);
    vi.advanceTimersByTime(farS * 1000 - Date.now());
    expect(onClose).toHaveBeenCalledTimes(1);

    const later = vi.fn();
    const stopLater = watchCloses([NOW_S + 60 * 86_400], () => Date.now(), later);
    stopLater();
    stop();
    vi.advanceTimersByTime(60 * 86_400_000);
    expect(later).not.toHaveBeenCalled();
  });
});

describe("the market list", () => {
  function row(ticker: string, closeTs: number | null): MarketRow {
    return {
      ticker,
      event_ticker: ticker,
      series_ticker: "KXA",
      title: null,
      subtitle: null,
      category: null,
      showcase: false,
      volume_24h_e2: 100_000,
      close_ts: closeTs,
      bid_e4: null,
      ask_e4: null,
      last_e4: null,
      book: "fresh",
    };
  }

  it("labels a market closed once its close time passes on the page's clock", () => {
    const nowS = Math.floor(monotonicNow() / 1000);
    const markup = renderToStaticMarkup(
      createElement(MarketPicker, {
        rows: [row("KXA-OPEN", nowS + 60), row("KXA-SHUT", nowS - 1), row("KXA-NONE", null)],
        listed: 3,
        loading: false,
        error: null,
        selectedTicker: null,
        selectedSummary: null,
        onSelect: () => undefined,
      }),
    );

    const items = markup.split("<li>").slice(1);
    expect(items).toHaveLength(3);
    const closed = items.map((item) => item.includes(">Closed<"));
    expect(closed).toEqual([false, true, false]);
    expect(items[1]).toContain("picker-item-closed");
    expect(items[0]).not.toContain("picker-item-closed");
  });
});
