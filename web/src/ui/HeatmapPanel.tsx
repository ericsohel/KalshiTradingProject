/** The live heatmap card: canvas, axes, range toggle, and a plain-language data notice. */

import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import type { ConnectionState } from "../api/live";
import { priceTicks, priceToFractionFromTop, timeTicks } from "../render/viewport";
import { centDecimals } from "../state/priceGrid";
import { DEFAULT_BIN_MS, type MarketSummary, type TapeStore } from "../state/tapeStore";
import { monotonicNow } from "./clock";
import { formatCents } from "./format";
import { HeatmapHost, referenceView, VISIBLE_MS, type HeatmapHostState } from "./heatmapHost";
import type { PriceRangeMode } from "./url";

export interface HeatmapPanelProps {
  readonly store: TapeStore | null;
  readonly summary: MarketSummary | null;
  readonly connection: ConnectionState;
  readonly mode: PriceRangeMode;
  readonly onModeChange: (mode: PriceRangeMode) => void;
  /** The market's name for the canvas's accessible description. */
  readonly label: string;
}

interface Notice {
  readonly tone: "idle" | "warn" | "bad";
  readonly text: string;
}

const LABEL_EVERY_MS = 60_000;
const PRICE_LABEL_SPACING_PX = 34;

function chartNotice(
  summary: MarketSummary | null,
  connection: ConnectionState,
  host: HeatmapHostState,
): Notice | null {
  if (host.phase === "unsupported") {
    return {
      tone: "bad",
      text: `${host.message ?? "The heatmap could not start."} The ladder and trades still update.`,
    };
  }
  if (host.phase === "context_lost")
    return { tone: "warn", text: host.message ?? "Restoring graphics." };
  if (connection.phase === "incompatible") {
    return {
      tone: "bad",
      text: connection.lastClose?.explanation ?? "The server speaks another protocol.",
    };
  }
  if (summary === null) return null;
  if (summary.resync !== null) {
    const refreshS = connection.hello?.bus_refresh_s ?? 10;
    return summary.resync.reason === "bus_loss"
      ? {
          tone: "warn",
          text: `Resynchronizing. The server lost part of the recorder's feed, so this book was discarded. It returns at the next refresh, within about ${refreshS} seconds; the hatched band marks the gap.`,
        }
      : {
          tone: "warn",
          text: "Resynchronizing. This page fell behind the live feed, so the book was discarded and a fresh snapshot is on its way.",
        };
  }
  if (summary.book === "unknown") {
    if (connection.phase !== "live") {
      return summary.gap
        ? {
            tone: "warn",
            text: "Connection lost. The book returns when the page reconnects; the hatched band marks the gap.",
          }
        : { tone: "idle", text: "Connecting to the live feed…" };
    }
    return summary.gap
      ? { tone: "warn", text: "Book lost. Waiting for a fresh snapshot." }
      : { tone: "idle", text: "Waiting for the first order book snapshot…" };
  }
  if (summary.book === "stale") {
    return {
      tone: "warn",
      text: "Stale book. The recorder cannot confirm this book is current; amber hatching marks depth shown as last known.",
    };
  }
  return null;
}

export function HeatmapPanel({
  store,
  summary,
  connection,
  mode,
  onModeChange,
  label,
}: HeatmapPanelProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [host] = useState(() => new HeatmapHost(monotonicNow));

  useEffect(() => {
    const canvas = canvasRef.current;
    if (canvas === null) return undefined;
    host.attach(canvas);
    return () => host.detach();
  }, [host]);
  useEffect(() => host.setStore(store), [host, store]);
  useEffect(() => host.setMode(mode), [host, mode]);

  const state = useSyncExternalStore(host.subscribe, host.getState);
  const binMs = store?.binMs ?? DEFAULT_BIN_MS;
  const grid = summary?.grid ?? null;
  const ticks = priceTicks(
    state.window,
    Math.max(2, Math.floor(state.cssHeight / PRICE_LABEL_SPACING_PX)),
  );
  const tickStep = (ticks[1] ?? 100) - (ticks[0] ?? 0);
  const labelDecimals = tickStep % 100 === 0 ? 0 : 1;
  const times = timeTicks(referenceView(binMs, state.window), binMs, LABEL_EVERY_MS);
  const notice = chartNotice(summary, connection, state);
  const decimals = grid === null ? 0 : centDecimals(grid);
  const description = `Live order book heatmap for ${label}, the last ${VISIBLE_MS / 60_000} minutes. Best bid ${formatCents(
    summary?.bestBidE4 ?? null,
    decimals,
  )}, best ask ${formatCents(summary?.bestAskE4 ?? null, decimals)}.`;

  return (
    <section className="card chart-card" aria-labelledby="heatmap-title">
      <div className="chart-toolbar">
        <h2 className="card-title" id="heatmap-title">
          Liquidity heatmap
        </h2>
        <div className="chart-controls">
          {state.phase === "running" && state.fps > 0 ? (
            <span
              className="chart-perf"
              title="Frames drawn per second and trade bubbles in the buffer"
            >
              {state.fps} fps · {state.bubbles.toLocaleString("en-US")} trades
            </span>
          ) : null}
          <div className="segmented" role="group" aria-label="Price range">
            <button
              type="button"
              aria-pressed={mode === "follow"}
              onClick={() => onModeChange("follow")}
            >
              Around price
            </button>
            <button
              type="button"
              aria-pressed={mode === "full"}
              onClick={() => onModeChange("full")}
            >
              Full 0–100¢
            </button>
          </div>
        </div>
      </div>
      <div className="chart-frame">
        <div className="price-axis" aria-hidden="true">
          {ticks.map((price) => (
            <span
              key={price}
              style={{ top: `${priceToFractionFromTop(price, state.window) * 100}%` }}
            >
              {formatCents(price, labelDecimals)}
            </span>
          ))}
        </div>
        <div className="plot">
          <canvas ref={canvasRef} role="img" aria-label={description} />
          {notice !== null ? (
            <p className={`chart-notice tone-${notice.tone}`} role="status">
              {notice.text}
            </p>
          ) : null}
        </div>
        <div className="time-axis" aria-hidden="true">
          {times.map((tick) => (
            <span key={tick.agoMs} style={{ left: `${tick.fraction * 100}%` }}>
              {tick.agoMs === 0 ? "now" : `${tick.agoMs / 60_000}m ago`}
            </span>
          ))}
        </div>
      </div>
    </section>
  );
}
