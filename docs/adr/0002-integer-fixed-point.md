# 0002. Integer fixed-point for prices, counts, and dollars

Status: accepted. Date: 2026-09-09.

## Context

Kalshi transmits prices and quantities as decimal strings with fixed precision (up to
four decimals for prices on the book, two for contract counts, up to six for money
amounts). Binary floating point cannot represent these exactly; accumulated rounding
would make fee verification to the micro-dollar and P&L reconciliation to the cent
impossible.

## Decision

Prices are `int` in 1/10,000 dollar (`PriceE4`), counts are `int` in 1/100 contract
(`CountE2`), and money is `int` in 1/1,000,000 dollar (`DollarsE6`). Conversion from
and to strings uses `decimal.Decimal` exactly once at the boundary. Floats are
forbidden in every module that handles these types, enforced by a build check.
Parquet stores the same integers.

## Consequences

Arithmetic is exact and fast. Code must use integer division with explicit rounding
helpers. Strike values, which are metadata rather than money, remain floats.
