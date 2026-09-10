# 0002. Integer fixed-point for prices, counts, and dollars

Status: accepted. Date: 2026-09-09.

## Context

Kalshi transmits prices and quantities as decimal strings with fixed precision (up to
four decimals for book prices, two for contract counts, up to six for money). The
project's honesty claims depend on reconciling fees to the micro-dollar and P&L to the
cent against the exchange's own records.

## Alternatives considered

1. **`float`.** Fast, native to numpy. Rejected: `0.07 * 0.56 * 0.44` is not
   representable; accumulated error makes cent-level reconciliation fail, and a
   reconciliation that "almost" matches proves nothing.
2. **`decimal.Decimal` everywhere.** Exact. Rejected for the hot path: 10 to 50 times
   slower than `int`, heavy objects, awkward in numpy and Parquet.
3. **`fractions.Fraction`.** Exact and general. Rejected: slower still, and the
   generality is unnecessary because every unit is a fixed power of ten.

## Decision

Prices are `int` in 1/10,000 dollar (`PriceE4`), counts are `int` in 1/100 contract
(`CountE2`), and money is `int` in 1/1,000,000 dollar (`DollarsE6`). Strings become
integers exactly once, at the boundary, through `Decimal`. Floats are forbidden in
every module that touches these types, enforced by a build check. Parquet stores the
same integers.

## Consequences

Arithmetic is exact and fast. Units live in type names and must be respected at every
multiplication (a price times a count yields `DollarsE6` only after rescaling).
Division needs explicit rounding helpers. Strike values, which are metadata rather
than money, stay floats.

## What would reverse it

Nothing realistic. If Kalshi increased money precision beyond six decimals the unit
would widen, not the approach.
