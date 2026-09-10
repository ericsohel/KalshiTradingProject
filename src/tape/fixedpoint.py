"""Exact fixed-point codecs for prices, contract counts, and dollar amounts.

Kalshi transmits these as decimal strings. They are converted exactly once, here, into
integers in fixed units and never touch floating point again (ADR 0002).

Units:
    ``PriceE4``   1/10,000 dollar, range 0 to 10,000 inclusive (a $0 to $1 contract).
    ``CountE2``   1/100 contract, non-negative.
    ``DollarsE6`` 1/1,000,000 dollar, signed.

Invariant: ``parse_x(format_x(v)) == v`` and ``format_x(parse_x(s))`` is the canonical
form of ``s`` for every valid input.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Final, NewType

from tape.errors import FixedPointError

__all__ = [
    "COUNT_SCALE",
    "DOLLARS_SCALE",
    "PRICE_MAX",
    "PRICE_SCALE",
    "CountE2",
    "DollarsE6",
    "PriceE4",
    "complement",
    "div_ceil",
    "div_floor",
    "format_count",
    "format_dollars",
    "format_price",
    "notional_e6",
    "parse_count",
    "parse_dollars",
    "parse_price",
]

PriceE4 = NewType("PriceE4", int)
CountE2 = NewType("CountE2", int)
DollarsE6 = NewType("DollarsE6", int)

PRICE_SCALE: Final = 10_000
COUNT_SCALE: Final = 100
DOLLARS_SCALE: Final = 1_000_000
PRICE_MAX: Final = PriceE4(PRICE_SCALE)

_PRICE_DECIMALS: Final = 4
_COUNT_DECIMALS: Final = 2
_DOLLARS_DECIMALS: Final = 6

_DECIMAL_STRING: Final = re.compile(r"^-?\d+(\.\d+)?$")


def _parse_scaled(text: str, decimals: int, *, what: str) -> int:
    """Convert a plain decimal string to an integer scaled by ``10**decimals``.

    Args:
        text: A string such as ``"0.5600"``. Exponent notation, whitespace, and
            signs other than a leading minus are rejected.
        decimals: Number of decimal places in the target unit.
        what: Name of the quantity, used in error messages.

    Returns:
        The exact integer value in the target unit.

    Raises:
        FixedPointError: If the string is not a plain decimal or has more decimal
            places than the unit can represent.
    """
    if not isinstance(text, str) or not _DECIMAL_STRING.match(text):
        raise FixedPointError(f"{what}: not a plain decimal string: {text!r}")
    try:
        value = Decimal(text).scaleb(decimals)
    except InvalidOperation as exc:  # pragma: no cover - guarded by the regex
        raise FixedPointError(f"{what}: invalid decimal: {text!r}") from exc
    if value != value.to_integral_value():
        raise FixedPointError(f"{what}: more than {decimals} decimal places: {text!r}")
    return int(value)


def parse_price(text: str) -> PriceE4:
    """Parse a dollar price string such as ``"0.5600"`` into ``PriceE4``.

    Raises:
        FixedPointError: If the string is malformed, negative, above ``1.0000``, or
            has more than four decimal places.
    """
    value = _parse_scaled(text, _PRICE_DECIMALS, what="price")
    if value < 0 or value > PRICE_SCALE:
        raise FixedPointError(f"price out of range [0, 1.0000]: {text!r}")
    return PriceE4(value)


def parse_count(text: str) -> CountE2:
    """Parse a contract count string such as ``"12.50"`` into ``CountE2``.

    Raises:
        FixedPointError: If the string is malformed, negative, or has more than two
            decimal places.
    """
    value = _parse_scaled(text, _COUNT_DECIMALS, what="count")
    if value < 0:
        raise FixedPointError(f"count must be non-negative: {text!r}")
    return CountE2(value)


def parse_signed_count(text: str) -> int:
    """Parse a signed contract delta such as ``"-54.00"`` into hundredths.

    Deltas are the one place a negative count is meaningful, so this returns a plain
    ``int`` rather than ``CountE2``.

    Raises:
        FixedPointError: If the string is malformed or has more than two decimals.
    """
    return _parse_scaled(text, _COUNT_DECIMALS, what="count delta")


def parse_dollars(text: str) -> DollarsE6:
    """Parse a signed dollar amount such as ``"0.010000"`` into ``DollarsE6``.

    Raises:
        FixedPointError: If the string is malformed or has more than six decimals.
    """
    return DollarsE6(_parse_scaled(text, _DOLLARS_DECIMALS, what="dollars"))


def _format_scaled(value: int, decimals: int) -> str:
    """Format a scaled integer as a canonical decimal string with fixed decimals."""
    sign = "-" if value < 0 else ""
    magnitude = abs(value)
    scale = 10**decimals
    return f"{sign}{magnitude // scale}.{magnitude % scale:0{decimals}d}"


def format_price(price: PriceE4) -> str:
    """Format ``PriceE4`` as Kalshi's canonical four-decimal string, e.g. ``"0.5600"``."""
    return _format_scaled(price, _PRICE_DECIMALS)


def format_count(count: CountE2) -> str:
    """Format ``CountE2`` as Kalshi's canonical two-decimal string, e.g. ``"12.50"``."""
    return _format_scaled(count, _COUNT_DECIMALS)


def format_dollars(dollars: DollarsE6) -> str:
    """Format ``DollarsE6`` as a six-decimal string, e.g. ``"0.010000"``."""
    return _format_scaled(dollars, _DOLLARS_DECIMALS)


def complement(price: PriceE4) -> PriceE4:
    """Return the NO-leg price for a YES-leg price, or vice versa: ``1.0000 - price``."""
    return PriceE4(PRICE_SCALE - price)


def notional_e6(price: PriceE4, count: CountE2) -> DollarsE6:
    """Return ``price * count`` in micro-dollars.

    The units multiply exactly: ``1e-4 dollars * 1e-2 contracts = 1e-6 dollars``, so
    no rescaling or rounding is needed.
    """
    return DollarsE6(price * count)


def div_ceil(numerator: int, denominator: int) -> int:
    """Integer division rounding toward positive infinity.

    Raises:
        ZeroDivisionError: If ``denominator`` is zero.
    """
    if denominator == 0:
        raise ZeroDivisionError("division by zero")
    return -((-numerator) // denominator)


def div_floor(numerator: int, denominator: int) -> int:
    """Integer division rounding toward negative infinity (Python's ``//``)."""
    if denominator == 0:
        raise ZeroDivisionError("division by zero")
    return numerator // denominator
