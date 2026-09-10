"""Error hierarchy: structured attributes and inheritance."""

from __future__ import annotations

import pytest

from tape.errors import (
    BookInvariantError,
    FixedPointError,
    KalshiError,
    KalshiHttpError,
    RateLimitedError,
    SequenceGapError,
    TapeError,
    WireError,
    WsProtocolError,
)


def test_http_error_carries_structured_context() -> None:
    err = KalshiHttpError(400, code="bad_request", message="nope", details="field x")
    assert err.status == 400
    assert err.code == "bad_request"
    assert err.details == "field x"
    assert "400" in str(err)
    assert isinstance(err, KalshiError)
    assert isinstance(err, TapeError)


def test_rate_limited_is_a_429() -> None:
    err = RateLimitedError()
    assert err.status == 429
    assert isinstance(err, KalshiHttpError)


def test_ws_protocol_error_keeps_code_and_sid() -> None:
    err = WsProtocolError(25, "Subscription buffer overflow", sid=7)
    assert (err.code, err.sid) == (25, 7)
    assert "25" in str(err)


def test_sequence_gap_error_reports_expected_and_got() -> None:
    err = SequenceGapError(3, expected=10, got=12)
    assert (err.sid, err.expected, err.got) == (3, 10, 12)


def test_book_invariant_error_names_the_market() -> None:
    err = BookInvariantError("KXTEST", "crossed")
    assert err.ticker == "KXTEST"
    assert str(err) == "KXTEST: crossed"


@pytest.mark.parametrize("cls", [FixedPointError, WireError])
def test_value_errors_are_also_value_errors(cls: type[TapeError]) -> None:
    assert issubclass(cls, ValueError)
