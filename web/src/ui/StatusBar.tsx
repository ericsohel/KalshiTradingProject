/** Connection, book, and recorder health in one line, each with a plain-language tooltip. */

import type { ConnectionState } from "../api/live";
import type { ServiceStatus } from "../api/protocol";
import type { MarketSummary } from "../state/tapeStore";
import { monotonicNow } from "./clock";
import { useNow, type Polled } from "./hooks";

type Tone = "good" | "warn" | "bad" | "idle";

interface Indicator {
  readonly label: string;
  readonly tone: Tone;
  readonly detail: string;
}

function connectionIndicator(connection: ConnectionState, nowMs: number): Indicator {
  switch (connection.phase) {
    case "live":
      return { label: "Live", tone: "good", detail: "Connected to the live feed." };
    case "idle":
    case "connecting":
    case "handshaking":
      return { label: "Connecting", tone: "idle", detail: "Opening the live feed." };
    case "waiting": {
      const seconds = Math.max(0, Math.ceil(((connection.retryAtMs ?? nowMs) - nowMs) / 1000));
      const why = connection.lastClose?.explanation ?? "The connection closed.";
      return { label: `Reconnecting in ${seconds}s`, tone: "warn", detail: why };
    }
    case "stopped":
      return { label: "Paused", tone: "idle", detail: "The live feed is stopped." };
    case "incompatible":
      return {
        label: "Update needed",
        tone: "bad",
        detail: connection.lastClose?.explanation ?? "The server speaks a newer protocol.",
      };
  }
}

function bookIndicator(summary: MarketSummary | null): Indicator {
  if (summary === null)
    return { label: "No market", tone: "idle", detail: "Pick a market to follow." };
  if (summary.resync !== null) {
    const detail =
      summary.resync.reason === "bus_loss"
        ? "The server lost part of the recorder's feed; the book returns at its next refresh."
        : summary.resync.reason === "client_lag"
          ? "This page fell behind the feed; a fresh snapshot is on its way."
          : `Resynchronizing (${summary.resync.reason}).`;
    return { label: "Resyncing", tone: "warn", detail };
  }
  switch (summary.book) {
    case "fresh":
      return {
        label: "Book fresh",
        tone: "good",
        detail: "The book is current, change by change.",
      };
    case "stale":
      return {
        label: "Book stale",
        tone: "warn",
        detail: "The recorder cannot confirm this book is current.",
      };
    case "unknown":
      return summary.gap
        ? { label: "Book lost", tone: "warn", detail: "The book will return after reconnecting." }
        : { label: "Book unknown", tone: "idle", detail: "Waiting for the first snapshot." };
  }
}

function recorderIndicator(service: Polled<ServiceStatus>): Indicator {
  if (service.data === null) {
    return service.error === null
      ? { label: "Recorder…", tone: "idle", detail: "Asking the API for recorder status." }
      : { label: "Status unavailable", tone: "bad", detail: service.error.message };
  }
  const { recording, recorder_status_age_ms: ageMs, recorder } = service.data;
  const markets = recorder === null ? "" : ` ${recorder.subscribed_markets} markets.`;
  return recording
    ? {
        label: "Recording",
        tone: "good",
        detail: `The recorder reported ${Math.round((ageMs ?? 0) / 1000)}s ago.${markets}`,
      }
    : { label: "Recorder silent", tone: "bad", detail: "No recorder status within two intervals." };
}

function Item({ indicator }: { indicator: Indicator }) {
  return (
    <li className={`status-item tone-${indicator.tone}`} title={indicator.detail}>
      <span className="status-dot" aria-hidden="true" />
      <span>{indicator.label}</span>
      <span className="visually-hidden">: {indicator.detail}</span>
    </li>
  );
}

export interface StatusBarProps {
  readonly connection: ConnectionState;
  readonly summary: MarketSummary | null;
  readonly service: Polled<ServiceStatus>;
}

export function StatusBar({ connection, summary, service }: StatusBarProps) {
  useNow(1000);
  const nowMs = monotonicNow();
  const connectionState = connectionIndicator(connection, nowMs);
  return (
    <div className="status-bar">
      <ul className="status-list" aria-label="Data status">
        <Item indicator={connectionState} />
        <Item indicator={bookIndicator(summary)} />
        <Item indicator={recorderIndicator(service)} />
      </ul>
      {connection.malformedFrames > 0 ? (
        <span
          className="status-note"
          title="Messages from the server that failed validation and were dropped."
        >
          {connection.malformedFrames} malformed
        </span>
      ) : null}
      <span className="visually-hidden" role="status" aria-live="polite">
        {connectionState.label === "Live"
          ? "Live feed connected"
          : connectionState.label.replace(/ in \d+s$/, "")}
      </span>
    </div>
  );
}
