# 0028. The recorded universe is chosen by ordered rule groups, not volume alone

Status: accepted. Date: 2026-09-13. Amends the universe selection of ADR 0020 and the
`showcase_series` policy.

## Context

The production host (ADR 0024) exhausted its CPU credits and now runs at its 20% baseline,
so capture degrades whenever traffic spikes. Its universe had two rules: admit every open
market of the showcase series, then fill a budget of 200 by 24-hour volume.

Measured over two Saturday-evening hours (UTC 20:00 to 22:00), those rules chose poorly:

- **Traffic concentration.** Sports carried 76% of order-book traffic and crypto 23%, the
  Bitcoin 15-minute series alone 20.5%. Weather and economics together were about 1%.
- **Showcase markets that cost slots, not CPU.** The showcase series took 112 of the 200
  slots with nearly idle books: 60 Fed-decision markets across every future meeting, and
  39 jobs-report brackets across three months.
- **Volume fill.** It admitted several variants of the same game (winner, spread, total)
  and both sides of each binary outcome.
- **Cutting the budget alone does not help.** The 100 busiest markets carried 98% of
  traffic, so lowering the budget to 100 would have cut almost nothing.

Markets rotate constantly: a Bitcoin 15-minute market lasts a quarter of an hour, and a
game ends in hours. Any choice must therefore be a rule re-applied at each universe
refresh, not a list.

## Alternatives considered

1. **A fixed list of tickers.** Stale within hours. Rejected.
2. **A smaller budget with the same rules.** Keeps the costliest markets. Rejected.
3. **Traffic-aware selection** from per-market rates measured on the tape. Targets cost
   directly, but feeds the tape back into selection and needs a cold-start rule. Deferred.
4. **Ordered rule groups**, each naming what it admits and how much. Chosen.

## Decision

`recorder.universe` holds an ordered list of groups. Each universe refresh applies them in
order until `max_l2_markets` is reached, and a market admitted by an earlier group is not
counted again. A group selects markets by one of:

- **`series`:** a list of series tickers. `events` counts per series: the nearest `events`
  open events of each series, by the earliest close time among their markets.
- **`category`:** a Kalshi series category, such as `Sports` or `Politics`. `events` counts
  across the whole group: the `events` open events with the highest 24-hour volume, summed
  over their markets, above `min_volume_24h`.

Within each chosen event, at most `markets_per_event` markets are admitted, highest 24-hour
volume first. A group may also cap its total with `max_markets`, and may set
`max_hours_to_close`: an event is then eligible for that group only if the earliest close among
its markets is within that many hours, checked before events are chosen, so a category group
ranks games closing soon rather than long-lived futures. Markets admitted by series
groups carry the showcase flag in the catalog. Multivariate legs, closed markets, and the
close horizon are still excluded before any group applies. Series categories come from
Kalshi's series listing, fetched no more often than hourly and cached, and only when a
category group needs them. `showcase_series` is removed.

The production host's groups, in order:

| Group | Selector | Events | Markets per event | Closes within |
|---|---|---|---|---|
| crypto 15-minute | `KXBTC15M`, `KXETH15M` | 1 each | 1 | any |
| Bitcoin hourly | `KXBTCD` | 1 | 6 | any |
| stock indexes hourly | `KXINXU`, `KXNASDAQ100U` | 1 each | 6 | any |
| economy | `KXFEDDECISION`, `KXCPIYOY`, `KXPAYROLLS`, `KXAAAGASW` | 1 each | 6 | any |
| weather | `KXHIGHNY`, `KXHIGHLAX`, `KXHIGHCHI`, `KXHIGHMIA` | 1 each | 6 | any |
| politics | category `Politics` | 5 | 1 | any |
| sports | category `Sports` | 10 | 1 | 48 hours |

## Consequences

- **Load.** From the Saturday sample, recorded traffic should fall to roughly half.
  Confirming that below the CPU baseline takes a day of the credit metric, after which the
  sports group's `events` is the first dial.
- **Maintenance.** A popular new series outside the groups is recorded only through a
  category group, and seasonal series need configuration changes.
- **One side of each game.** With one market per event, research that needs both books
  of a binary event must raise `markets_per_event` for that group.
- **Categories.** Selection depends on Kalshi's categories, and category groups cost the
  recorder one cached listing request an hour.
- **Configuration.** It is longer, but every recorded market is explained by the group
  that admitted it, which the universe log and `tape universe preview` report. The decision
  is not written to the tape, so the daily manifest cannot report it.

## What would reverse it

- **Rules that misjudge cost** often enough that per-market traffic measured on the tape
  is the better selector.
- **More capacity,** which would justify larger groups again.
