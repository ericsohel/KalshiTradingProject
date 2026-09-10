/** The top of the book as a price ladder: asks above, bids below, the spread between. */

import type { PriceLevel } from "../api/protocol";
import { centDecimals } from "../state/priceGrid";
import type { MarketSummary } from "../state/tapeStore";
import { formatCents, formatContracts } from "./format";

export interface DepthLadderProps {
  readonly summary: MarketSummary | null;
}

function LadderRow({
  side,
  level,
  decimals,
  maxCount,
}: {
  side: "bid" | "ask";
  level: PriceLevel;
  decimals: 0 | 1 | 2;
  maxCount: number;
}) {
  const [price, count] = level;
  return (
    <tr className={`ladder-${side}`}>
      <td className={`ladder-price-${side}`}>{formatCents(price, decimals)}</td>
      <td className="ladder-size">
        <span
          className="ladder-bar"
          style={{ width: `${Math.max(2, (count / maxCount) * 100)}%` }}
        />
        <span className="ladder-size-value">{formatContracts(count)}</span>
      </td>
    </tr>
  );
}

function emptyText(summary: MarketSummary | null): string {
  if (summary === null) return "Pick a market to see its book.";
  if (summary.resync !== null || summary.gap) return "Book discarded; resynchronizing.";
  return "Waiting for the order book…";
}

export function DepthLadder({ summary }: DepthLadderProps) {
  const known = summary !== null && summary.book !== "unknown";
  const decimals = summary === null ? 0 : centDecimals(summary.grid);
  const asks = known ? [...summary.asks].reverse() : [];
  const bids = known ? summary.bids : [];
  const maxCount = Math.max(
    1,
    ...asks.map(([, count]) => count),
    ...bids.map(([, count]) => count),
  );
  const bid = summary?.bestBidE4 ?? null;
  const ask = summary?.bestAskE4 ?? null;
  return (
    <section className="card ladder-card" aria-labelledby="ladder-title">
      <h2 className="card-title" id="ladder-title">
        Order book
      </h2>
      {known ? (
        <table className="ladder">
          <caption className="visually-hidden">
            Top {asks.length} asks above the spread and top {bids.length} bids below it. Prices are
            YES prices; sizes are contracts.
          </caption>
          <thead>
            <tr>
              <th scope="col">Price</th>
              <th scope="col">Contracts</th>
            </tr>
          </thead>
          <tbody>
            {asks.map((level) => (
              <LadderRow
                key={`ask-${level[0]}`}
                side="ask"
                level={level}
                decimals={decimals}
                maxCount={maxCount}
              />
            ))}
            <tr className="ladder-spread">
              <td colSpan={2}>
                {bid !== null && ask !== null
                  ? `Spread ${formatCents(ask - bid, decimals)}`
                  : "One side empty"}
                {summary.book === "stale" ? " · stale" : ""}
              </td>
            </tr>
            {bids.map((level) => (
              <LadderRow
                key={`bid-${level[0]}`}
                side="bid"
                level={level}
                decimals={decimals}
                maxCount={maxCount}
              />
            ))}
          </tbody>
        </table>
      ) : (
        <p className="ladder-empty">{emptyText(summary)}</p>
      )}
    </section>
  );
}
