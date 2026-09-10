# 0006. Consolidated YES-space book with `use_yes_price=true`

Status: accepted. Date: 2026-09-09.

## Context

Kalshi's order book is transmitted as two bid lists (YES bids and NO bids) because a
NO bid at price `q` is economically a YES ask at `1 - q`. By default, NO-side deltas
arrive in NO-leg prices. The `use_yes_price` subscription flag reports both sides on
the YES price scale, and Kalshi has announced that this will become the default and
then the only behavior.

## Alternatives considered

1. **Mirror the wire: keep YES-bid and NO-bid books.** Zero conversion at capture.
   Rejected: every consumer converts, the `best_bid < best_ask` invariant is not
   directly checkable, and the default convention is scheduled to change underneath.
2. **Convert client-side without the flag.** Works today. Rejected: when Kalshi flips
   the default, a recorder that does not request the flag explicitly would silently
   start double-converting.

## Decision

Subscribe with `use_yes_price=true`. Maintain one consolidated book per market in YES
space: bids and asks keyed by `PriceE4`. Record the flag value in every segment header
so replay applies the correct convention to any segment. All derived data, the API,
and the viewer use YES space only.

## Consequences

One price scale everywhere; the crossed-book invariant is checkable; trade direction
maps directly to `taker_book_side`. Readers of legacy segments must honor the header
flag; the decoder does this, not the consumer.

## What would reverse it

Nothing; Kalshi is moving in this direction. If the flag is removed, the header
simply records `true` forever.
