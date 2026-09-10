/**
 * Text formatting for prices, sizes, and times. Integer inputs are split with integer
 * arithmetic before display so a count or price is never rounded by floating point.
 */

import type { MarketRow } from "../api/protocol";

const grouped = new Intl.NumberFormat("en-US");
const EM_DASH = "—";

/**
 * A YES price in cents: `56¢`, `56.3¢`, `56.25¢`.
 *
 * @param decimals Digits after the cent the market's grid needs; more are shown when the
 *   price itself needs them.
 */
export function formatCents(priceE4: number | null, decimals: 0 | 1 | 2): string {
  if (priceE4 === null) return EM_DASH;
  const fraction = priceE4 % 100;
  const whole = (priceE4 - fraction) / 100;
  const needed = fraction === 0 ? 0 : fraction % 10 === 0 ? 1 : 2;
  const digits = Math.max(decimals, needed);
  if (digits === 0) return `${whole}¢`;
  const text = String(fraction).padStart(2, "0").slice(0, digits);
  return `${whole}.${text}¢`;
}

/** Contracts from `count_e2`: `1,234` or `1,234.5`. */
export function formatContracts(countE2: number): string {
  const fraction = countE2 % 100;
  const whole = grouped.format((countE2 - fraction) / 100);
  if (fraction === 0) return whole;
  const text = String(fraction).padStart(2, "0");
  return `${whole}.${text.endsWith("0") ? text.slice(0, 1) : text}`;
}

/** Contracts from `count_e2` in a few characters: `830`, `41K`, `1.8M`. */
export function formatCompactContracts(countE2: number): string {
  const contracts = Math.floor(countE2 / 100);
  if (contracts < 1000) return String(contracts);
  const [divisor, suffix] = contracts < 1_000_000 ? [1000, "K"] : [1_000_000, "M"];
  const scaled = contracts / divisor;
  return `${scaled < 10 ? scaled.toFixed(1).replace(/\.0$/, "") : Math.floor(scaled)}${suffix}`;
}

/** A short elapsed time: `now`, `12s`, `4m`, `3h`. */
export function formatAgo(elapsedMs: number): string {
  const seconds = Math.floor(Math.max(0, elapsedMs) / 1000);
  if (seconds < 1) return "now";
  if (seconds < 60) return `${seconds}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h`;
}

/** When a market closes relative to now: `closes in 6h`, `closes in 3d`, `closed`. */
export function formatCloses(closeTs: number | null, nowMs: number): string | null {
  if (closeTs === null) return null;
  const remainingS = closeTs - Math.floor(nowMs / 1000);
  if (remainingS <= 0) return "closed";
  if (remainingS < 3600) return `closes in ${Math.max(1, Math.floor(remainingS / 60))}m`;
  if (remainingS < 172_800) return `closes in ${Math.floor(remainingS / 3600)}h`;
  return `closes in ${Math.floor(remainingS / 86_400)}d`;
}

export interface MarketLabel {
  readonly primary: string;
  readonly secondary: string | null;
}

/** A market's name for people: the event title and YES subtitle, or the ticker until resolved. */
export function marketLabel(row: Pick<MarketRow, "ticker" | "title" | "subtitle">): MarketLabel {
  if (row.title === null) return { primary: row.ticker, secondary: row.subtitle };
  return { primary: row.title, secondary: row.subtitle };
}
