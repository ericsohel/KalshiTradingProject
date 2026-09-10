/** The selected market's name and top-of-book numbers. */

import type { MarketRow } from "../api/protocol";
import { centDecimals } from "../state/priceGrid";
import type { MarketSummary } from "../state/tapeStore";
import { formatCents, formatCloses, formatCompactContracts, marketLabel } from "./format";
import { useNow } from "./hooks";

export interface MarketHeaderProps {
  readonly ticker: string;
  readonly row: MarketRow | null;
  readonly summary: MarketSummary | null;
}

function Stat({ label, value, hint }: { label: string; value: string; hint: string }) {
  return (
    <div className="stat" title={hint}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

export function MarketHeader({ ticker, row, summary }: MarketHeaderProps) {
  const wallNowMs = useNow(60_000);
  const label = marketLabel(row ?? { ticker, title: null, subtitle: null });
  const decimals = summary === null ? 0 : centDecimals(summary.grid);
  const bid = summary?.bestBidE4 ?? summary?.tickerUpdate?.bidE4 ?? row?.bid_e4 ?? null;
  const ask = summary?.bestAskE4 ?? summary?.tickerUpdate?.askE4 ?? row?.ask_e4 ?? null;
  const last = summary?.tickerUpdate?.lastE4 ?? row?.last_e4 ?? null;
  const closes = formatCloses(row?.close_ts ?? null, wallNowMs);
  return (
    <header className="market-header">
      <div className="market-names">
        <h1 className="market-title">{label.primary}</h1>
        <p className="market-subline">
          {label.secondary !== null ? (
            <span className="market-subtitle">{label.secondary}</span>
          ) : null}
          <code className="market-ticker">{ticker}</code>
          {row !== null && row.category !== null ? <span>{row.category}</span> : null}
          {row?.showcase === true ? <span className="badge">Showcase</span> : null}
          {closes !== null ? <span>{closes}</span> : null}
        </p>
      </div>
      <dl className="market-stats">
        <Stat
          label="Bid"
          value={formatCents(bid, decimals)}
          hint="Best bid: the highest price someone is paying for YES."
        />
        <Stat
          label="Ask"
          value={formatCents(ask, decimals)}
          hint="Best ask: the lowest price someone is selling YES for."
        />
        <Stat
          label="Last"
          value={formatCents(last, decimals)}
          hint="Price of the most recent trade."
        />
        <Stat
          label="24h vol"
          value={row === null ? "—" : formatCompactContracts(row.volume_24h_e2)}
          hint="Contracts traded in the last 24 hours."
        />
      </dl>
    </header>
  );
}
