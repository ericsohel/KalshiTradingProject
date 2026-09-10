# 0014. Three fill models reported as a band, never one number

Status: accepted. Date: 2026-09-09.

## Context

Backtesting a passive (maker) strategy requires knowing when a resting order would
have filled. Kalshi's order book is aggregated by price level with no order ids, so
queue position is never observed directly. Whether the contracts cancelled ahead of
you were ahead or behind is unknowable from public data alone.

## Alternatives considered

1. **A single "best" fill model.** Every public backtester does this and prints one
   P&L number. Rejected: the number is a guess presented as a measurement; the
   research found no Kalshi project whose backtest survived contact with live trading.
2. **Skip maker fill modeling; backtest takers only.** Honest but useless: the only
   documented edge on Kalshi is on the maker side.
3. **Calibrated model only.** Better than a guess, still one number that hides how
   much was assumed.

## Decision

Every replay computes fills under three models: pessimistic (all cancellations at the
level were behind you), optimistic (all were ahead), and calibrated (a cancel-ahead
fraction fitted from real penny-order probes, using the queue position Kalshi reports
in fill messages). Every report shows all three; the P&L is a band.

## Consequences

Reports are less flattering and more true. The probe stage becomes a first-class
milestone because it is what narrows the band. Anyone reading a result sees how much
of it is assumption.

## What would reverse it

Nothing; even a well-calibrated model stays inside a band. If Kalshi ever exposed
order-level book data, the band would collapse toward the calibrated line.
