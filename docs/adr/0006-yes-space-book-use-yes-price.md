# 0006. Consolidated YES-space book with `use_yes_price=true`

Status: accepted. Date: 2026-09-09.

## Context

Kalshi's order book is transmitted as two bid lists (YES bids and NO bids) because a
NO bid at price `q` is economically a YES ask at `1 - q`. By default, NO-side deltas
arrive in NO-leg prices. The `use_yes_price` subscription flag reports both sides on
the YES price scale, and Kalshi has announced that this will become the default and
then the only behavior.

## Decision

Subscribe with `use_yes_price=true`. Maintain one consolidated book per market in
YES space: bids and asks keyed by `PriceE4`. Record the flag value in every segment
header so replay applies the correct convention to old segments. All derived data,
the API, and the viewer use YES space only.

## Consequences

One price scale everywhere; `best_bid < best_ask` is a checkable invariant; trade
direction maps directly to `taker_book_side`. Code that reads legacy segments must
honor the header flag.
