/** The latest trades in words: who took liquidity, at what price, and how many contracts. */

import { centDecimals } from "../state/priceGrid";
import type { MarketSummary } from "../state/tapeStore";
import { monotonicNow } from "./clock";
import { formatAgo, formatCents, formatContracts } from "./format";
import { useNow } from "./hooks";

export interface RecentTradesProps {
  readonly summary: MarketSummary | null;
}

export function RecentTrades({ summary }: RecentTradesProps) {
  useNow(1000);
  const nowMs = monotonicNow();
  const trades = summary?.recentTrades ?? [];
  const decimals = summary === null ? 0 : centDecimals(summary.grid);
  return (
    <section className="card trades-card" aria-labelledby="trades-title">
      <h2 className="card-title" id="trades-title">
        Recent trades
      </h2>
      {trades.length === 0 ? (
        <p className="ladder-empty">No trades since this page opened.</p>
      ) : (
        <ul className="trades">
          {trades.map((trade) => (
            <li
              key={`${trade.receivedAtMs}-${trade.priceE4}-${trade.countE2}`}
              className={`trade trade-${trade.takerSide}`}
            >
              <span className="trade-ago">{formatAgo(nowMs - trade.receivedAtMs)}</span>
              <span className="trade-side">
                {trade.takerSide === "bid" ? "Bought YES" : "Sold YES"}
              </span>
              <span className="trade-price">{formatCents(trade.priceE4, decimals)}</span>
              <span className="trade-count">{formatContracts(trade.countE2)}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
