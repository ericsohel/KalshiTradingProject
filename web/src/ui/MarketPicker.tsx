/** Markets ranked by 24-hour volume, filterable, navigable with arrow keys. */

import { useId, useState, type KeyboardEvent } from "react";
import type { BookState, MarketRow } from "../api/protocol";
import type { ApiRequestError } from "../api/rest";
import type { MarketSummary } from "../state/tapeStore";
import { marketListNotice } from "./availability";
import { formatCompactContracts, marketLabel } from "./format";

export interface MarketPickerProps {
  readonly rows: readonly MarketRow[];
  /** Rows in the latest list the API answered with, or `null` before one arrived. */
  readonly listed: number | null;
  readonly loading: boolean;
  readonly error: ApiRequestError | null;
  readonly selectedTicker: string | null;
  /** Live book status of the selected market, fresher than the polled list. */
  readonly selectedSummary: MarketSummary | null;
  readonly onSelect: (ticker: string) => void;
}

const BOOK_TEXT: Record<BookState, string> = {
  fresh: "Fresh",
  stale: "Stale",
  unknown: "Unknown",
};

function matches(row: MarketRow, query: string): boolean {
  if (query === "") return true;
  const needle = query.toLowerCase();
  return [row.ticker, row.title, row.subtitle, row.category].some(
    (field) => field?.toLowerCase().includes(needle) === true,
  );
}

/** Moves focus between market buttons with the arrow, Home, and End keys. */
function moveFocus(event: KeyboardEvent<HTMLUListElement>): void {
  const keys = ["ArrowDown", "ArrowUp", "Home", "End"];
  if (!keys.includes(event.key)) return;
  const buttons = [
    ...event.currentTarget.querySelectorAll<HTMLButtonElement>("button[data-ticker]"),
  ];
  if (buttons.length === 0) return;
  event.preventDefault();
  const current = buttons.findIndex((button) => button === document.activeElement);
  const next =
    event.key === "Home"
      ? 0
      : event.key === "End"
        ? buttons.length - 1
        : Math.min(buttons.length - 1, Math.max(0, current + (event.key === "ArrowDown" ? 1 : -1)));
  buttons[next]?.focus();
}

export function MarketPicker({
  rows,
  listed,
  loading,
  error,
  selectedTicker,
  selectedSummary,
  onSelect,
}: MarketPickerProps) {
  const [query, setQuery] = useState("");
  const filterId = useId();
  const visible = rows.filter((row) => matches(row, query));
  const listNotice = marketListNotice(listed, error !== null);
  return (
    <nav className="picker" aria-label="Markets">
      <div className="picker-head">
        <h2 className="picker-title">Markets</h2>
        <span className="picker-hint">by 24h volume</span>
      </div>
      <label className="visually-hidden" htmlFor={filterId}>
        Filter markets
      </label>
      <input
        id={filterId}
        className="picker-filter"
        type="search"
        placeholder="Filter by title, ticker, category"
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        autoComplete="off"
        spellCheck={false}
      />
      {error !== null ? (
        <p className="picker-message tone-bad" role="alert">
          Could not load markets: {error.message}. Retrying.
        </p>
      ) : null}
      {loading ? <p className="picker-message">Loading markets…</p> : null}
      {listNotice !== null ? (
        <p className="picker-message" role="status">
          {listNotice.text}
        </p>
      ) : null}
      <ul className="picker-list" onKeyDown={moveFocus}>
        {visible.map((row, index) => {
          const selected = row.ticker === selectedTicker;
          const label = marketLabel(row);
          const book = selected && selectedSummary !== null ? selectedSummary.book : row.book;
          return (
            <li key={row.ticker}>
              <button
                type="button"
                className="picker-item"
                data-ticker={row.ticker}
                aria-current={selected ? "true" : undefined}
                onClick={() => onSelect(row.ticker)}
              >
                <span className="picker-rank" aria-hidden="true">
                  {index + 1}
                </span>
                <span className="picker-text">
                  <span className="picker-primary">{label.primary}</span>
                  {label.secondary !== null ? (
                    <span className="picker-secondary">{label.secondary}</span>
                  ) : null}
                  <span className="picker-meta">
                    {row.showcase ? <span className="badge">Showcase</span> : null}
                    {row.category !== null ? <span>{row.category}</span> : null}
                    <span title="Contracts traded in the last 24 hours">
                      {formatCompactContracts(row.volume_24h_e2)} vol
                    </span>
                    <span
                      className={`book-pill book-${book}`}
                      title={`Order book: ${BOOK_TEXT[book].toLowerCase()}`}
                    >
                      {BOOK_TEXT[book]}
                    </span>
                  </span>
                </span>
              </button>
            </li>
          );
        })}
      </ul>
      {rows.length > 0 && visible.length === 0 ? (
        <p className="picker-message">No market matches “{query}”.</p>
      ) : null}
    </nav>
  );
}
