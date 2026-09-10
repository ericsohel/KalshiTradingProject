/** How to read the heatmap, for someone who has never seen an order book. */

import { bubbleRadiusPx, bubbleReferenceContracts } from "../render/bubbles";
import { depthCeilingContracts, depthLegendTicks } from "../render/colorScale";
import { viridisCssGradient } from "../render/colormap";
import type { MarketSummary } from "../state/tapeStore";

export interface LegendProps {
  readonly summary: MarketSummary | null;
}

const RAMP = viridisCssGradient(16);
const SAMPLE_SIZES = [10, 100, 1000];

function contractsLabel(contracts: number): string {
  if (contracts >= 1_000_000) return `${contracts / 1_000_000}M`;
  return contracts >= 1000 ? `${contracts / 1000}K` : String(contracts);
}

export function Legend({ summary }: LegendProps) {
  const ceiling = depthCeilingContracts(summary?.maxRowContracts ?? 0);
  const ticks = depthLegendTicks(ceiling);
  const reference = bubbleReferenceContracts(summary?.maxTradeContracts ?? 0);
  const bubbles: { contracts: number; radius: number; centerX: number }[] = [];
  let bubbleWidth = 0;
  for (const contracts of SAMPLE_SIZES) {
    const radius = bubbleRadiusPx(contracts, reference);
    bubbles.push({ contracts, radius, centerX: bubbleWidth + radius + 5 });
    bubbleWidth += radius * 2 + 10;
  }
  const bubbleHeight = Math.max(...bubbles.map((bubble) => bubble.radius)) * 2 + 4;
  return (
    <section className="card legend" aria-labelledby="legend-title">
      <h2 className="card-title" id="legend-title">
        How to read the heatmap
      </h2>
      <p className="legend-lead">
        Time runs left to right over the last five minutes; the thin vertical line is now. Height is
        the price of YES, from 0 to 100 cents; a price of 60¢ means traders collectively put the
        chance of YES near 60%. Each colored cell is how many contracts were waiting to trade at
        that price at that moment.
      </p>
      <div className="legend-grid">
        <div className="legend-item">
          <h3>Resting orders</h3>
          <div
            className="ramp"
            style={{ background: RAMP }}
            role="img"
            aria-label="Color scale from dark purple (few contracts) to yellow (many)"
          />
          <div className="ramp-ticks" aria-hidden="true">
            {ticks.map((tick) => (
              <span key={tick.contracts} style={{ left: `${tick.position * 100}%` }}>
                {contractsLabel(tick.contracts)}
              </span>
            ))}
          </div>
          <p>
            Contracts waiting at a price, on a log scale so a small order and a wall both show. Dark
            means empty. The scale is fixed for the session and only widens when a bigger level
            appears (now {contractsLabel(ceiling)}).
          </p>
        </div>
        <div className="legend-item">
          <h3>Best prices</h3>
          <p className="legend-row">
            <span className="line-swatch bid" aria-hidden="true" />
            Best bid: the most anyone will pay for YES.
          </p>
          <p className="legend-row">
            <span className="line-swatch ask" aria-hidden="true" />
            Best ask: the least anyone will sell YES for.
          </p>
          <p>The gap between the lines is the spread. Bids rest below it and asks above it.</p>
        </div>
        <div className="legend-item">
          <h3>Trades</h3>
          <svg
            className="legend-bubbles"
            width={bubbleWidth}
            height={bubbleHeight}
            aria-hidden="true"
          >
            {bubbles.map((bubble, index) => (
              <circle
                key={bubble.contracts}
                className={`legend-bubble ${index === 1 ? "ask" : "bid"}`}
                cx={bubble.centerX}
                cy={bubbleHeight / 2}
                r={bubble.radius}
              />
            ))}
          </svg>
          <p>
            <span className="text-bid">Blue</span>: someone bought YES from the ask.{" "}
            <span className="text-ask">Pink</span>: someone sold YES into the bid. Area grows with
            contracts traded (shown: {SAMPLE_SIZES.join(", ")}); very small trades keep a minimum
            size and very large ones a maximum.
          </p>
        </div>
        <div className="legend-item">
          <h3>Missing or doubtful data</h3>
          <p className="legend-row">
            <span className="hatch hatch-stale" aria-hidden="true" />
            Stale: the book could not be confirmed current; depth is the last known.
          </p>
          <p className="legend-row">
            <span className="hatch hatch-gap" aria-hidden="true" />
            No data: not received yet, or lost and resynchronizing.
          </p>
          <p>Gaps are drawn, never hidden or filled in.</p>
        </div>
      </div>
    </section>
  );
}
