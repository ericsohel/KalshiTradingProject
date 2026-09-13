"""Interpreting raw records: rows for every table, accounting, snapshot reasons, and integrity."""

from __future__ import annotations

import typing
from collections.abc import Iterator

import pytest

from tape.bake.interpret import (
    FAILURE_AUDIT_PAYLOAD,
    FAILURE_COMMAND_PAYLOAD,
    FAILURE_CONNECTION_PAYLOAD,
    FAILURE_FRAME_ENVELOPE,
    FAILURE_FRAME_PAYLOAD,
    FAILURE_FRAME_UNKNOWN_TYPE,
    FAILURE_GAP_PAYLOAD,
    FAILURE_SEGMENT_CORRUPT,
    MAX_FAILURE_SAMPLES,
    STOPPED_DETAIL,
    AuditOutcome,
    HourInterpreter,
    HourResult,
)
from tape.bake.manifest import ConnectionSpan, GapFacts, Sleep
from tape.bake.tables import (
    EMPTY_BOOK_SIDE,
    SNAPSHOT_INITIAL,
    SNAPSHOT_RECONNECT,
    SNAPSHOT_RESYNC,
    TABLES,
    Row,
    TableName,
)
from tape.errors import TapeCorruptionError
from tape.recorder import auditor, supervisor, writer
from tape.segment import Record, RecordKind
from tests.fakes.synthetic_tape import (
    HOUR,
    MS,
    SECOND,
    START,
    SegmentScript,
    header,
    mono_of,
)

A = "KXA-26SEP10-T1"
B = "KXB-26SEP10-T2"


class Rows:
    """A row sink that keeps every row by table."""

    def __init__(self) -> None:
        self.tables: dict[str, list[Row]] = {}

    def add(self, table: TableName, row: Row) -> None:
        assert len(row) == len(TABLES[table].schema)
        self.tables.setdefault(table, []).append(row)

    def of(self, table: str) -> list[Row]:
        return self.tables.get(table, [])

    def column(self, table: TableName, name: str) -> list[object]:
        index = TABLES[table].schema.get_field_index(name)
        return [row[index] for row in self.of(table)]


def interpret(*scripts: SegmentScript) -> tuple[Rows, HourResult]:
    rows = Rows()
    interpreter = HourInterpreter(HOUR, rows)
    for index, script in enumerate(scripts):
        interpreter.read_segment(
            f"segment-{index}",
            header(script.conn_id, use_yes_price=script.use_yes_price),
            script.records,
        )
    return rows, interpreter.finish()


def at(seconds: int, ms: int = 0) -> int:
    return START + seconds * SECOND + ms * MS


def every_kind() -> SegmentScript:
    """One segment holding every record kind and every frame type the recorder tapes."""
    script = SegmentScript(conn_id=2)
    script.opened(at(1))
    script.command(
        at(1, 1), "subscribe", 1, channels=["orderbook_delta", "trade"], market_tickers=[A, B]
    )
    script.subscribed(at(1, 2), channel="orderbook_delta", sid=1)
    script.subscribed(at(1, 3), channel="trade", sid=2)
    script.snapshot(
        at(2), sid=1, seq=1, ticker=A, yes=[("0.4000", "10.00")], no=[("0.6000", "5.00")]
    )
    script.snapshot(at(2, 1), sid=1, seq=2, ticker=B, yes=(), no=())
    script.delta(
        at(3), sid=1, seq=3, ticker=A, price="0.4100", delta="2.00", client_order_id="mine"
    )
    script.delta(at(4), sid=1, seq=4, ticker=A, price="0.6000", delta="-5.00", side="no")
    script.trade(at(5), sid=2, seq=1, ticker=A, trade_id="t-1", taker_book_side="ask")
    script.frame(at(6), {"type": "ok", "id": 2, "sid": 1, "seq": 5, "msg": {"market_tickers": [A]}})
    script.frame(at(6, 1), {"type": "error", "id": 3, "msg": {"code": 6, "msg": "already"}})
    script.frame(at(6, 2), {"type": "unsubscribed", "id": 4, "sid": 2, "seq": 2})
    script.frame(at(6, 3), {"type": "ticker", "sid": 9, "msg": {"market_ticker": A, "ts_ms": 1}})
    script.lifecycle(at(7), sid=3, seq=1, ticker=A, event_type="settled", settled_ts=1_789_000_000)
    script.lifecycle(at(7, 1), sid=3, seq=2, ticker=B, event_type="activated")
    script.frame(
        at(7, 2),
        {"type": "event_lifecycle", "sid": 3, "seq": 3, "msg": {"event_ticker": "KXA-26SEP10"}},
    )
    script.frame(
        at(7, 3),
        {
            "type": "event_fee_update",
            "sid": 3,
            "seq": 4,
            "msg": {"event_ticker": "KXB-26SEP10", "fee_type_override": None},
        },
    )
    rest = [{"side": 0, "price_e4": 4000, "count_e2": 1000}]
    for index, outcome in enumerate(("exact", "consistent", "inconsistent", "undecidable")):
        extra: dict[str, object] = {}
        if outcome in ("exact", "consistent"):
            extra["match_index"] = index
        if outcome == "inconsistent":
            extra |= {"mismatched_levels": 3, "rest_levels": rest, "local_levels": []}
        if outcome == "undecidable":
            extra["fault"] = "stale"
        script.audit(at(8, index), ticker=A, outcome=outcome, **extra)
    script.gap(at(9), sid=2, expected_seq=2, got_seq=4)
    script.connection(at(10), "writer_overflow", dropped=3)
    script.closed(at(11))
    return script


def test_every_record_becomes_rows_or_is_counted_not_baked() -> None:
    script = every_kind()
    _, result = interpret(script)
    accounting = result.accounting

    assert accounting.records == {"audit": 4, "command": 1, "connection": 3, "frame": 15, "gap": 1}
    assert accounting.total == len(script.records)
    assert accounting.baked == {
        "audit": 4,
        "event_fee_update": 1,
        "event_lifecycle": 1,
        "gap": 1,
        "market_lifecycle_v2": 2,
        "orderbook_delta": 2,
        "orderbook_snapshot": 2,
        "trade": 1,
    }
    assert accounting.not_baked == {
        "command": 1,
        "connection": 3,
        "error": 1,
        "ok": 1,
        "subscribed": 2,
        "ticker": 1,
        "unsubscribed": 1,
    }
    assert accounting.decode_failures == {}
    assert accounting.failures == 0
    assert result.failures == ()


def test_rows_carry_the_integers_the_recorder_applied() -> None:
    rows, _ = interpret(every_kind())

    assert rows.of("deltas") == [
        (A, at(3) // MS, mono_of(at(3)), at(3), 2, 1, 3, 0, 4100, 200, "mine"),
        # A NO delta on the YES price scale touches the ask side at its price (ADR 0006).
        (A, at(4) // MS, mono_of(at(4)), at(4), 2, 1, 4, 1, 6000, -500, None),
    ]
    assert rows.of("snapshots") == [
        (A, at(2), mono_of(at(2)), 2, 1, 1, 0, 4000, 1000, SNAPSHOT_INITIAL),
        (A, at(2), mono_of(at(2)), 2, 1, 1, 1, 6000, 500, SNAPSHOT_INITIAL),
        # An empty book is one row, so emptiness is distinguishable from absence.
        (B, at(2, 1), mono_of(at(2, 1)), 2, 1, 2, EMPTY_BOOK_SIDE, 0, 0, SNAPSHOT_INITIAL),
    ]
    assert rows.of("trades") == [(A, "t-1", at(5) // MS, at(5), 2, 1, 5500, 300, 1, False)]
    lifecycle = rows.of("lifecycle")
    assert [row[:7] for row in lifecycle] == [
        (A, "market_lifecycle_v2", "settled", 1_789_000_000, at(7), 3, 1),
        (B, "market_lifecycle_v2", "activated", None, at(7, 1), 3, 2),
        ("KXA-26SEP10", "event_lifecycle", None, None, at(7, 2), 3, 3),
        ("KXB-26SEP10", "event_fee_update", None, None, at(7, 3), 3, 4),
    ]
    assert lifecycle[3][7] == '{"event_ticker":"KXB-26SEP10","fee_type_override":null}'
    assert rows.column("audits", "outcome") == [
        "exact",
        "consistent",
        "inconsistent",
        "undecidable",
    ]
    assert rows.column("audits", "match_index") == [0, 1, None, None]
    assert rows.column("audits", "fault") == [None, None, None, "stale"]
    assert rows.column("audits", "rest_levels_json") == [
        None,
        None,
        '[{"side":0,"price_e4":4000,"count_e2":1000}]',
        None,
    ]
    assert rows.column("audits", "levels_local") == [2, 2, 2, None]
    # A gap on a trade subscription stales no book and so resolves nothing.
    assert rows.of("gaps") == [(2, 2, at(9), mono_of(at(9)), 2, 4, None)]


def test_integrity_facts_count_audits_by_outcome_and_the_writer_overflow() -> None:
    _, result = interpret(every_kind())
    audits = result.integrity.audits

    assert (audits.exact, audits.consistent, audits.inconsistent, audits.undecidable) == (
        1,
        1,
        1,
        1,
    )
    assert audits.levels_mismatched == 3
    assert result.integrity.writer_overflow_dropped == 3
    assert result.integrity.spans == (
        ConnectionSpan(
            conn_id=2, start_wall_ns=at(1), end_wall_ns=at(11), opened=True, closed=True
        ),
    )


def test_a_no_side_without_yes_pricing_is_complemented_onto_the_yes_scale() -> None:
    script = SegmentScript(conn_id=2, use_yes_price=False)
    script.delta(at(1), sid=1, seq=1, ticker=A, price="0.4000", delta="1.00", side="no")
    rows, _ = interpret(script)
    assert rows.column("deltas", "price_e4") == [6000]
    assert rows.column("deltas", "side") == [1]


def test_every_decode_failure_is_counted_by_class_and_bakes_nothing() -> None:
    script = SegmentScript(conn_id=2)
    script.frame(at(1), b"not json")
    script.frame(at(2), {"type": "market_position", "sid": 1, "msg": {}})
    script.delta(at(3), sid=1, seq=1, ticker=A, price="0.12345")
    script.frame(at(4), {"type": "orderbook_delta", "msg": {"market_ticker": A}})
    script.snapshot(at(5), sid=1, seq=2, ticker=A, yes=[("0.4000", "1.00"), ("bad", "1.00")])
    script.frame(at(6), {"type": "subscribed", "id": 1, "msg": {"channel": 5}})
    script.record(RecordKind.GAP, at(7), b"{}")
    script.audit(at(8), ticker=A, outcome="maybe")
    script.record(RecordKind.CONNECTION, at(9), b"[]")
    script.record(RecordKind.COMMAND, at(10), b"{}")
    script.trade(at(11), sid=2, seq=1, ticker=A, trade_id="ok")
    rows, result = interpret(script)
    accounting = result.accounting

    assert accounting.decode_failures == {
        FAILURE_AUDIT_PAYLOAD: 1,
        FAILURE_COMMAND_PAYLOAD: 1,
        FAILURE_CONNECTION_PAYLOAD: 1,
        FAILURE_FRAME_ENVELOPE: 1,
        FAILURE_FRAME_PAYLOAD: 4,
        FAILURE_FRAME_UNKNOWN_TYPE: 1,
        FAILURE_GAP_PAYLOAD: 1,
    }
    assert accounting.baked == {"trade": 1}
    assert accounting.not_baked == {}
    assert accounting.total == len(script.records) == accounting.failures + 1
    assert set(rows.tables) == {"trades"}
    assert [(f.failure, f.record_index) for f in result.failures][:3] == [
        (FAILURE_FRAME_ENVELOPE, 0),
        (FAILURE_FRAME_UNKNOWN_TYPE, 1),
        (FAILURE_FRAME_PAYLOAD, 2),
    ]
    assert {f.segment for f in result.failures} == {"segment-0"}


def test_failure_samples_are_bounded_per_class() -> None:
    script = SegmentScript(conn_id=2)
    for second in range(MAX_FAILURE_SAMPLES + 3):
        script.frame(at(second), b"{")
    _, result = interpret(script)
    assert result.accounting.decode_failures == {FAILURE_FRAME_ENVELOPE: MAX_FAILURE_SAMPLES + 3}
    assert len(result.failures) == MAX_FAILURE_SAMPLES


def test_snapshot_reasons_follow_requests_repeats_and_lost_connections() -> None:
    first = SegmentScript(conn_id=2)
    first.opened(at(1))
    first.subscribed(at(1, 1), channel="orderbook_delta", sid=1)
    first.snapshot(at(2), sid=1, seq=1, ticker=A)
    first.command(at(3), "update_subscription", 2, sid=1, action="get_snapshot", market_tickers=[A])
    first.snapshot(at(4), sid=1, seq=2, ticker=A)
    first.snapshot(at(5), sid=1, seq=3, ticker=A)
    first.closed(at(6), "connection 2 closed by peer: no close frame received or sent")
    reconnected = SegmentScript(conn_id=2)
    reconnected.connection(at(7), "error", detail="websocket connect failed")
    reconnected.opened(at(8))
    reconnected.snapshot(at(9), sid=1, seq=1, ticker=A)
    reconnected.closed(at(10), STOPPED_DETAIL)
    restarted = SegmentScript(conn_id=2)
    restarted.opened(at(11))
    restarted.snapshot(at(12), sid=1, seq=1, ticker=A)
    other_connection = SegmentScript(conn_id=3)
    other_connection.opened(at(13))
    other_connection.snapshot(at(14), sid=1, seq=1, ticker=B)

    rows, _ = interpret(first, reconnected, restarted, other_connection)

    assert rows.column("snapshots", "reason") == [
        SNAPSHOT_INITIAL,
        SNAPSHOT_INITIAL,
        SNAPSHOT_RESYNC,
        SNAPSHOT_RESYNC,
        SNAPSHOT_RESYNC,
        SNAPSHOT_RESYNC,
        SNAPSHOT_RECONNECT,
        SNAPSHOT_RECONNECT,
        # A stop on purpose is not a lost connection, so the next process starts afresh.
        SNAPSHOT_INITIAL,
        SNAPSHOT_INITIAL,
        SNAPSHOT_INITIAL,
        SNAPSHOT_INITIAL,
    ]


def test_a_gap_on_a_book_subscription_stales_its_books_until_each_is_resnapshotted() -> None:
    script = SegmentScript(conn_id=2)
    script.opened(at(0))
    script.subscribed(at(0, 1), channel="orderbook_delta", sid=1)
    script.subscribed(at(0, 2), channel="trade", sid=2)
    script.snapshot(at(1), sid=1, seq=1, ticker=A)
    script.snapshot(at(1), sid=1, seq=2, ticker=B)
    script.gap(at(10), sid=1, expected_seq=3, got_seq=9)
    script.snapshot(at(12), sid=1, seq=10, ticker=A)
    script.gap(at(13), sid=2, expected_seq=1, got_seq=5)
    script.snapshot(at(15), sid=1, seq=11, ticker=B)
    script.gap(at(16), sid=1, expected_seq=12, got_seq=14)
    script.command(
        at(18), "update_subscription", 3, sid=1, action="delete_markets", market_tickers=[B]
    )
    script.delta(at(20), sid=1, seq=15, ticker=A)

    rows, result = interpret(script)

    # A gap's row waits for its resolution, so rows arrive out of order; parts sort them.
    assert sorted(rows.of("gaps"), key=lambda row: str(row[2])) == [
        (2, 1, at(10), mono_of(at(10)), 3, 9, at(15)),
        (2, 2, at(13), mono_of(at(13)), 1, 5, None),
        # Still waiting for snapshots when the segment ended.
        (2, 1, at(16), mono_of(at(16)), 12, 14, None),
    ]
    assert result.integrity.gaps == GapFacts(
        count=3,
        # A: 10 s to 12 s and 16 s to 20 s. B: 10 s to 15 s and 16 s to its removal at 18 s.
        stale_market_ns=(2 + 4 + 5 + 2) * SECOND,
        observed_market_ns=((20 - 1) + (18 - 1)) * SECOND,
    )


def test_spans_start_at_the_hour_for_a_continuing_connection_and_sleeps_collapse() -> None:
    continuing = SegmentScript(conn_id=1)
    continuing.lifecycle(at(300), sid=1, seq=7, ticker=A, event_type="activated")
    continuing.connection(
        at(1800), "clock_jump", wall_ns_delta=70 * SECOND, mono_ns_delta=10 * SECOND
    )
    continuing.closed(at(1900))
    book = SegmentScript(conn_id=2)
    book.delta(at(400), sid=1, seq=3, ticker=A)
    book.connection(at(1800), "clock_jump", wall_ns_delta=70 * SECOND, mono_ns_delta=10 * SECOND)
    reopened = SegmentScript(conn_id=1)
    reopened.connection(at(1950), "error", detail="handshake failed")
    reopened.opened(at(2000))
    reopened.lifecycle(at(2100), sid=1, seq=1, ticker=A, event_type="activated")

    _, result = interpret(continuing, book, reopened)

    assert result.integrity.spans == (
        ConnectionSpan(
            conn_id=1, start_wall_ns=START, end_wall_ns=at(1900), opened=False, closed=True
        ),
        ConnectionSpan(
            conn_id=2, start_wall_ns=START, end_wall_ns=at(1800), opened=False, closed=False
        ),
        ConnectionSpan(
            conn_id=1, start_wall_ns=at(2000), end_wall_ns=at(2100), opened=True, closed=False
        ),
    )
    assert result.integrity.sleeps == (Sleep(start_wall_ns=at(1740), end_wall_ns=at(1800)),)


def test_a_corrupt_segment_counts_what_was_read_and_ends_its_state() -> None:
    script = SegmentScript(conn_id=2)
    script.opened(at(1))
    script.subscribed(at(1, 1), channel="orderbook_delta", sid=1)
    script.snapshot(at(2), sid=1, seq=1, ticker=A)
    script.gap(at(3), sid=1, expected_seq=2, got_seq=5)

    def damaged() -> Iterator[Record]:
        yield from script.records
        raise TapeCorruptionError("unknown record kind 200")

    rows = Rows()
    interpreter = HourInterpreter(HOUR, rows)
    facts = interpreter.read_segment("damaged", header(2), damaged())
    result = interpreter.finish()

    assert facts.records == len(script.records)
    assert result.accounting.corrupt_segments == 1
    assert result.accounting.failures == 1
    assert result.accounting.total == len(script.records)
    assert [f.failure for f in result.failures] == [FAILURE_SEGMENT_CORRUPT]
    assert rows.of("gaps") == [(2, 1, at(3), mono_of(at(3)), 2, 5, None)]
    assert result.integrity.spans[0].closed is False


def test_an_empty_hour_accounts_for_nothing_and_is_consistent() -> None:
    rows, result = interpret(SegmentScript(conn_id=2))
    assert rows.tables == {}
    assert result.accounting.total == 0
    assert result.integrity.spans == ()


def test_the_constants_the_baker_shares_with_the_recorder_agree() -> None:
    assert STOPPED_DETAIL == supervisor._STOPPED
    assert writer.OVERFLOW_EVENT == "writer_overflow"
    assert typing.get_args(AuditOutcome.__value__) == typing.get_args(
        auditor.AuditOutcome.__value__
    )


@pytest.mark.parametrize("outcome", ["exact", "consistent", "inconsistent", "undecidable"])
def test_each_audit_outcome_is_baked(outcome: str) -> None:
    script = SegmentScript(conn_id=2)
    script.audit(at(1), ticker=A, outcome=outcome)
    rows, result = interpret(script)
    assert rows.column("audits", "outcome") == [outcome]
    assert result.accounting.baked == {"audit": 1}
