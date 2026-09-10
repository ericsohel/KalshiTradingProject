/**
 * `HeatmapHost`: drives a `HeatmapRenderer` for a React component without React state in
 * the hot path.
 *
 * It owns the frame loop, canvas sizing (CSS size from a ResizeObserver, device pixel
 * ratio checked each frame), the live view (five minutes, now near the right edge), and
 * the price window (following the mid price or the full 0 to 1 range). React reads its
 * low-frequency state (phase, window, size, fps) through `subscribe` and `getState`.
 */

import { RendererError } from "../render/gl";
import { HeatmapRenderer } from "../render/heatmapRenderer";
import {
  FULL_WINDOW,
  followPrice,
  liveView,
  type PlotView,
  type PriceWindow,
} from "../render/viewport";
import { DEFAULT_BIN_MS, type TapeStore } from "../state/tapeStore";
import type { PriceRangeMode } from "./url";

/** Time across the plot. */
export const VISIBLE_MS = 5 * 60_000;
/** Share of the width right of now, so the newest bubbles are not clipped. */
export const RIGHT_PAD_FRACTION = 0.04;
/** Price span of the follow view, e4 (24 cents). */
export const FOLLOW_SPAN_E4 = 2400;

export interface HeatmapHostState {
  readonly phase: "detached" | "running" | "unsupported" | "context_lost";
  readonly message: string | null;
  readonly window: PriceWindow;
  readonly cssWidth: number;
  readonly cssHeight: number;
  readonly fps: number;
  readonly bubbles: number;
}

/** A view with the same geometry as the live one, for placing static time labels. */
export function referenceView(binMs: number, window: PriceWindow): PlotView {
  const visibleBins = VISIBLE_MS / binMs;
  return liveView(visibleBins, visibleBins, RIGHT_PAD_FRACTION, window);
}

function midPrice(store: TapeStore | null): number | null {
  if (store === null) return null;
  const { bestBidE4: bid, bestAskE4: ask } = store;
  if (bid !== null && ask !== null) return (bid + ask) / 2;
  return bid ?? ask;
}

export class HeatmapHost {
  readonly #now: () => number;
  readonly #listeners = new Set<() => void>();
  #state: HeatmapHostState = {
    phase: "detached",
    message: null,
    window: FULL_WINDOW,
    cssWidth: 0,
    cssHeight: 0,
    fps: 0,
    bubbles: 0,
  };
  #renderer: HeatmapRenderer | null = null;
  #observer: ResizeObserver | null = null;
  #frame: number | null = null;
  #store: TapeStore | null = null;
  #mode: PriceRangeMode = "follow";
  #window: PriceWindow = FULL_WINDOW;
  #devicePixelRatio = 0;
  #framesInSample = 0;
  #sampleStartMs = 0;

  constructor(now: () => number) {
    this.#now = now;
  }

  readonly subscribe = (listener: () => void): (() => void) => {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  };

  readonly getState = (): HeatmapHostState => this.#state;

  attach(canvas: HTMLCanvasElement): void {
    this.detach();
    try {
      this.#renderer = new HeatmapRenderer(canvas);
    } catch (error: unknown) {
      const message =
        error instanceof RendererError ? error.message : "The heatmap could not start.";
      this.#update({ phase: "unsupported", message });
      return;
    }
    this.#renderer.setSource(this.#store);
    this.#observer = new ResizeObserver((entries) => {
      const box = entries[0]?.contentRect;
      if (box === undefined) return;
      this.#resize(box.width, box.height);
    });
    this.#observer.observe(canvas);
    const bounds = canvas.getBoundingClientRect();
    this.#resize(bounds.width, bounds.height);
    this.#update({ phase: "running", message: null });
    this.#frame = requestAnimationFrame(this.#tick);
  }

  detach(): void {
    if (this.#frame !== null) cancelAnimationFrame(this.#frame);
    this.#frame = null;
    this.#observer?.disconnect();
    this.#observer = null;
    this.#renderer?.dispose();
    this.#renderer = null;
    if (this.#state.phase !== "detached") this.#update({ phase: "detached" });
  }

  setStore(store: TapeStore | null): void {
    this.#store = store;
    this.#renderer?.setSource(store);
  }

  setMode(mode: PriceRangeMode): void {
    this.#mode = mode;
  }

  #resize(cssWidth: number, cssHeight: number): void {
    this.#devicePixelRatio = window.devicePixelRatio || 1;
    this.#renderer?.resize(cssWidth, cssHeight, this.#devicePixelRatio);
    if (cssWidth !== this.#state.cssWidth || cssHeight !== this.#state.cssHeight) {
      this.#update({ cssWidth, cssHeight });
    }
  }

  readonly #tick = (frameTimeMs: number): void => {
    this.#frame = requestAnimationFrame(this.#tick);
    const renderer = this.#renderer;
    if (renderer === null) return;
    if ((window.devicePixelRatio || 1) !== this.#devicePixelRatio) {
      this.#resize(this.#state.cssWidth, this.#state.cssHeight);
    }
    if (renderer.contextLost) {
      if (this.#state.phase !== "context_lost") {
        this.#update({
          phase: "context_lost",
          message: "The graphics context was lost; restoring.",
        });
      }
      return;
    }
    if (this.#state.phase === "context_lost") this.#update({ phase: "running", message: null });

    const store = this.#store;
    const binMs = store?.binMs ?? DEFAULT_BIN_MS;
    const nowBin = store === null ? 0 : store.binAt(this.#now());
    this.#window =
      this.#mode === "full"
        ? FULL_WINDOW
        : followPrice(this.#window, midPrice(store), FOLLOW_SPAN_E4);
    const stats = renderer.render(
      liveView(nowBin, VISIBLE_MS / binMs, RIGHT_PAD_FRACTION, this.#window),
    );

    this.#framesInSample += 1;
    const elapsed = frameTimeMs - this.#sampleStartMs;
    if (elapsed >= 1000) {
      const fps = Math.round((this.#framesInSample * 1000) / elapsed);
      this.#framesInSample = 0;
      this.#sampleStartMs = frameTimeMs;
      this.#update({ fps, bubbles: stats.bubbles });
    }
    if (this.#window !== this.#state.window) this.#update({ window: this.#window });
  };

  #update(patch: Partial<HeatmapHostState>): void {
    this.#state = { ...this.#state, ...patch };
    for (const listener of this.#listeners) listener();
  }
}
