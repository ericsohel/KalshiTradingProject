# 0029. React to market closes, and choose strikes near the price

Status: accepted. Date: 2026-09-13. Amends ADR 0028 (refresh timing and market order within
an event) and FRONTEND 4.1 (which markets the list shows).

## Context

A market leaves the recorded universe only at the next universe refresh. That refresh
runs every `universe_refresh_s` (300 seconds) after the previous one finished, unrelated
to when markets close. The API never checks close times, and nobody reads the lifecycle
events the recorder already tapes (`determined`, `settled`, `close_date_updated`,
`created`, `activated`, `deactivated`).

Watched live at the 05:00 UTC close on 2026-09-13, 20 recorded markets (Bitcoin and
Ethereum 15-minute, Bitcoin hourly, and two cities' temperatures) stayed listed 46
seconds after closing. Their replacements appeared at 05:01:06, only because a refresh
happened to run at 05:00:55. The worst case is about five minutes, or longer when a
refresh fails, which is a third of a 15-minute market's life.

The same watch showed the six strikes chosen for the new Bitcoin hourly event near
$67,600 to $68,100, while the previous hour's were near $77,000 to $77,500. Ordering an
event's markets by 24-hour volume picks arbitrary strikes when the event is new and has
barely traded.

## Alternatives considered

1. **A shorter `universe_refresh_s`.** Every refresh lists about 118,000 markets over
   more than 100 requests, a real CPU and rate-budget cost on the throttled host. It
   still leaves a gap after each close. Rejected.
2. **Full refresh on every close.** Closes happen many times an hour across series.
   Rejected for the same cost.
3. **Hide closed markets only in the viewer.** Fixes the page, but not the recording of
   dead markets or the late start of new ones. Rejected on its own.
4. **React at the moment of close:** drop closed markets at once, re-list only the
   affected series, and hide closed markets in the API and viewer. Chosen.

## Decision

- **API.** `GET /markets` omits every market whose `close_ts` has passed on the API's
  clock, or whose `determined` or `settled` lifecycle event the API has seen.
  `close_date_updated` events update the close time the API uses. `GET /markets/{ticker}`
  still answers for a market in the catalog, so a viewer on it can show that it closed.
- **Recorder, at close time.** The recorder schedules a tick at the earliest `close_ts`
  among planned markets, plus a small delay, and on any `determined`, `settled`, or
  `close_date_updated` event for a planned market. At that tick it removes closed markets
  from the plan, and their subscriptions follow ADR 0027 and ADR 0020, without listing all
  markets.
- **Recorder, replacements.** In the same tick, it re-lists only the open markets of the
  series groups that lost a market (`GET /markets` filtered by series), re-applies the
  groups, and replans. A `created` or `activated` event for a series named by a series
  group triggers the same targeted re-listing, debounced by a few seconds.
- **Bounds.** Targeted re-listings are spaced by a minimum interval, so a burst of events
  costs one re-listing.
- **Full refresh.** A full universe refresh still runs every `universe_refresh_s`.
  Category groups are replenished only there; a closed category-group market is removed
  at close and not replaced until then.
- **Viewer.** The market list labels a market as closed once its close time passes,
  in case the list is older than the close.
- **Market order within an event.** A group chooses which of an event's markets to admit
  with `market_order`:
  - `volume`, the default: highest 24-hour volume first, as before.
  - `near_price`: markets nearest the current price first. For a range bucket
    (`strike_type` of `between`), the highest YES mid price comes first, because those
    are the likeliest outcomes. For a threshold (`greater`, `less`, and similar), the YES
    mid closest to 50 cents comes first. The mid is the average of the listing's YES bid
    and ask, or the last price when either is missing. Markets with no price at all come
    after priced ones, ordered by volume. When a listing leaves a `near_price` series group
    with an admitted event whose markets are not all priced, as when a new event is listed
    before its first quotes, the recorder lists that group again after the minimum interval,
    at most four times per event, until they are.
  
  The production groups for Bitcoin hourly, stock indexes, economy, and weather use
  `near_price`.

## Consequences

- **Closed markets leave the site within seconds**, and replacements in series groups
  start recording within seconds of opening, instead of up to five minutes later.
- **The recorder now depends on lifecycle events and scheduled ticks for timeliness,**
  but not for correctness: the full refresh still converges on the same universe.
- **Targeted re-listings add a handful of small requests per close,** bounded by the
  minimum interval.
- **Near-price order reflects the price at the moment of listing.** Strikes can drift
  from the price within an event's life until the next refresh re-ranks them, and a
  re-rank changes subscriptions.

## What would reverse it

- **Closes clustering densely enough** that targeted re-listing costs more than a periodic
  full refresh.
- **Research that needs the full strike ladder** of an event, which would use
  `markets_per_event` equal to the ladder size.
