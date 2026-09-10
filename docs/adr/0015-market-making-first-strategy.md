# 0015. Passive market making as the first strategy

Status: accepted. Date: 2026-09-09.

## Context

The project needs one strategy to give the infrastructure a purpose and a client.
Public evidence on Kalshi, gathered before design: three independent weather-trading
postmortems were losses; six frontier LLM agents each lost 16 to 31 percent in eight
weeks; cross-venue "arbitrage" is not risk-free because resolution criteria diverge;
structural multi-outcome arbitrage capacity is a handful of marginal opportunities.
Two academic studies on tens of millions of trades find makers on contracts above
50 cents earn a small positive return after fees and makers earn twice as much per
contract in single-name markets, because retail systematically overbets YES.

## Alternatives considered

1. **Weather forecasting** (numerical models versus temperature markets). Rejected:
   the market is frequently right, fat tails, a fee "death zone" below 15 cents, and
   faster bots reprice within seconds of model updates.
2. **LLM forecasting.** Rejected: no public evidence of profit; every rigorous test
   is negative; the working edge in such systems turns out to be the maker baseline.
3. **Cross-venue arbitrage with Polymarket.** Rejected: legal access constraints,
   resolution-criteria divergence, fee drag around 5 percent, and windows of seconds.
4. **Structural (Dutch-book) arbitrage.** Intellectually attractive and near
   risk-free. Deferred: capacity of tens of dollars a week and taker fees; a candidate
   second module on the same engine.

## Decision

The first strategy is a log-odds market maker with a binary-settlement inventory
model, a fee floor, a flow-toxicity monitor, and post-only quotes. It is developed on
the same engine in replay, shadow, and micro-live, and scales only under the
validation ladder.

## Consequences

The strategy's edge is selection, skew, and cost discipline, not speed or forecasting,
which suits home or cloud latency. Its profitability is uncertain and high-variance;
the design treats that as a measurement problem, not a marketing one.

## What would reverse it

The project's own tape. If markouts show adverse selection eating the spread across
categories, the strategy is retired and the infrastructure remains the deliverable.
