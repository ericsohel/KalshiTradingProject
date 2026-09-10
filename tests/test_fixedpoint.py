"""Fixed-point codecs: exactness, canonical formatting, and rejection of bad input."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from tape.errors import FixedPointError
from tape.fixedpoint import (
    PRICE_SCALE,
    CountE2,
    DollarsE6,
    PriceE4,
    complement,
    div_ceil,
    div_floor,
    format_count,
    format_dollars,
    format_price,
    notional_e6,
    parse_count,
    parse_dollars,
    parse_price,
    parse_signed_count,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [("0.5600", 5600), ("0.56", 5600), ("1", 10_000), ("0", 0), ("1.0000", 10_000), ("0.0001", 1)],
)
def test_parse_price_examples(text: str, expected: int) -> None:
    assert parse_price(text) == expected


@pytest.mark.parametrize(
    "text",
    ["1.0001", "-0.01", "0.56001", "1e-4", " 0.5", "0.5 ", "abc", "", ".5", "5.", "+0.5", "NaN"],
)
def test_parse_price_rejects(text: str) -> None:
    with pytest.raises(FixedPointError):
        parse_price(text)


@pytest.mark.parametrize(("text", "expected"), [("12.50", 1250), ("10", 1000), ("0.01", 1)])
def test_parse_count_examples(text: str, expected: int) -> None:
    assert parse_count(text) == expected


@pytest.mark.parametrize("text", ["-1.00", "0.001", "1e2"])
def test_parse_count_rejects(text: str) -> None:
    with pytest.raises(FixedPointError):
        parse_count(text)


def test_parse_signed_count_allows_negative() -> None:
    assert parse_signed_count("-54.00") == -5400
    with pytest.raises(FixedPointError):
        parse_signed_count("-54.001")


@pytest.mark.parametrize(
    ("text", "expected"), [("0.010000", 10_000), ("-0.5", -500_000), ("1.75", 1_750_000)]
)
def test_parse_dollars_examples(text: str, expected: int) -> None:
    assert parse_dollars(text) == expected


def test_parse_dollars_rejects_seven_decimals() -> None:
    with pytest.raises(FixedPointError):
        parse_dollars("0.0000001")


def test_format_examples() -> None:
    assert format_price(PriceE4(5600)) == "0.5600"
    assert format_price(PriceE4(10_000)) == "1.0000"
    assert format_price(PriceE4(0)) == "0.0000"
    assert format_count(CountE2(1250)) == "12.50"
    assert format_count(CountE2(1)) == "0.01"
    assert format_dollars(DollarsE6(10_000)) == "0.010000"
    assert format_dollars(DollarsE6(-500_000)) == "-0.500000"


@given(st.integers(min_value=0, max_value=PRICE_SCALE))
def test_price_round_trip(value: int) -> None:
    assert parse_price(format_price(PriceE4(value))) == value


@given(st.integers(min_value=0, max_value=10**15))
def test_count_round_trip(value: int) -> None:
    assert parse_count(format_count(CountE2(value))) == value


@given(st.integers(min_value=-(10**15), max_value=10**15))
def test_dollars_round_trip(value: int) -> None:
    assert parse_dollars(format_dollars(DollarsE6(value))) == value


@given(
    st.integers(min_value=0, max_value=1),
    st.integers(min_value=0, max_value=4),
)
def test_price_string_with_fewer_decimals_is_canonicalized(whole: int, decimals: int) -> None:
    text = str(whole) if decimals == 0 else f"{whole}.{'5' * decimals}"
    try:
        value = parse_price(text)
    except FixedPointError:
        assert whole == 1
        assert decimals > 0
        return
    assert parse_price(format_price(value)) == value


@given(st.integers(min_value=0, max_value=PRICE_SCALE))
def test_complement_is_an_involution(value: int) -> None:
    price = PriceE4(value)
    assert complement(complement(price)) == price
    assert complement(price) + price == PRICE_SCALE


def test_notional_units_multiply_exactly() -> None:
    # 0.5600 dollars * 12.50 contracts = 7.000000 dollars
    assert notional_e6(PriceE4(5600), CountE2(1250)) == 7_000_000


@given(st.integers(min_value=-(10**9), max_value=10**9), st.integers(min_value=1, max_value=10**6))
def test_div_ceil_and_floor_bracket_the_quotient(numerator: int, denominator: int) -> None:
    floor = div_floor(numerator, denominator)
    ceil = div_ceil(numerator, denominator)
    assert floor * denominator <= numerator <= ceil * denominator
    assert 0 <= ceil - floor <= 1
    assert (ceil == floor) == (numerator % denominator == 0)


def test_division_by_zero_raises() -> None:
    with pytest.raises(ZeroDivisionError):
        div_ceil(1, 0)
    with pytest.raises(ZeroDivisionError):
        div_floor(1, 0)
