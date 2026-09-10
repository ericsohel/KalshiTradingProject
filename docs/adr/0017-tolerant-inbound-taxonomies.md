# 0017. Inbound taxonomies decode as strings; directional bits stay closed

Status: accepted. Date: 2026-09-10.

## Context

Kalshi publishes an OpenAPI document with enumerated values for fields such as
`fee_type`, `market_type`, `status`, and `result`. Mirroring those enums as
`Literal` types is the obvious way to make illegal states unrepresentable, and it was
the first implementation.

On 2026-09-10, the first live call to `GET /series/fee_changes?show_historical=true`
returned `fee_type: "margin_market_maker_program_fees"` on 24 of 147 scheduled changes.
That value is absent from the `FeeType` enum in the pinned specification. The strict
type turned a routine, additive change on Kalshi's side into a decode failure of the
whole response, and this was found only because the client was pointed at production.

The exchange shipped 306 changelog entries in 17 months, so this will happen again.
The project's own rule already says additive changes must never break capture
(docs/DATA_FORMATS.md 9), and the recorder exists to preserve data that cannot be
re-fetched.

## Alternatives considered

1. **Add the missing value and keep the enum.** Fixes today's break. Rejected: it
   treats a class of failure as an instance, and the next unknown value crashes again,
   possibly at three in the morning on a market the recorder will never see twice.
2. **Keep the enums but catch decode errors and skip the record.** Rejected: silently
   dropping the only fee-schedule change for a series is worse than not knowing about
   it, and the failure would be invisible in aggregate.
3. **Decode every field as `str`, including sides.** Rejected: a `book_side` outside
   `{bid, ask}` is not something to shrug at. Guessing a direction puts an order or a
   fill on the wrong side of the book, which costs money.
4. **Generate the structs from the spec on every run.** Rejected: it makes the client
   change without review, which is precisely what the weekly drift job exists to stop.

## Decision

Split the two cases by consequence.

**Open taxonomies decode as `str`**: `fee_type`, `market_type`, `status`, `result`,
and the private order `status`. Kalshi may extend these, and an unknown value is
information, not corruption. Recognizing them is the domain layer's job: the fee
engine maps the values it knows and refuses to quote a series whose fee type it does
not, which is the safe response rather than a guess.

**Closed sets stay `Literal`**: `book_side` and `outcome_side`, where an unrecognized
value must fail loudly, and the request-only enums (`time_in_force`,
`self_trade_prevention_type`), which constrain what this client may send.

A live smoke test against the public production endpoints runs in CI, marked
`integration`, so the next divergence between the spec and reality is found by the
project rather than by an outage.

## Consequences

Decoding survives additive changes on Kalshi's side. Type checking no longer proves a
`fee_type` is one of four values, so the fee engine must handle the unknown case
explicitly, which it must do anyway to be correct. The tests pin an unknown value as
decodable so the tolerance cannot be regressed by someone tightening the type.

## What would reverse it

Nothing for the open taxonomies. If Kalshi ever published a versioned, additive-only
contract with a documented deprecation window, closed enums would become safe again.
