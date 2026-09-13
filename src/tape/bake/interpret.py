"""Turn one hour of raw records into baked table rows, and account for every record.

Responsibility: interpret the records of an hour's segments, in file order, as the recorder
produced them (docs/DATA_FORMATS.md 4, docs/INTERFACES.md 8.4). Frames, gaps, and audits become
rows of the tables in docs/DATA_FORMATS.md 6; commands, connection events, subscription replies,
and ticker frames are counted as intentionally not baked; anything that does not decode is counted
as a decode failure. Along the way it gathers the hour's integrity facts. It reads no file and no
clock: the caller feeds it records and gives it a sink for rows.

Invariants: every record fed is counted exactly once, as baked, not baked, or a decode failure,
so the counts by outcome always sum to the records read (ADR 0025, condition 3); a record's rows
are emitted only after all of it decoded, so a failure never leaves part of a record baked;
frames are converted with the same functions the recorder applies to its books, so a row carries
exactly the integers the live book received; a frame type not listed here is a decode failure,
never skipped, so a message Kalshi adds blocks pruning until the baker learns it; and the rows
and facts depend only on the records and their order.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Final, Literal, Protocol

import msgspec

from tape.bake.layout import HourKey
from tape.bake.manifest import (
    AuditFacts,
    ConnectionSpan,
    GapFacts,
    HourIntegrity,
    RecordAccounting,
    Sleep,
)
from tape.bake.tables import (
    EMPTY_BOOK_SIDE,
    SNAPSHOT_INITIAL,
    SNAPSHOT_RECONNECT,
    SNAPSHOT_RESYNC,
    Row,
    TableName,
)
from tape.errors import FixedPointError, TapeCorruptionError, WireError
from tape.events import Receipt
from tape.segment import Record, RecordKind, SegmentHeader
from tape.timeutil import Ns
from tape.wire.convert import to_book_delta, to_book_snapshot, to_lifecycle, to_trade
from tape.wire.ws import (
    Envelope,
    EventFeeUpdateMsg,
    EventLifecycleMsg,
    MarketLifecycleV2Msg,
    OrderbookDeltaMsg,
    OrderbookSnapshotMsg,
    SubscribedMsg,
    TradeMsg,
    decode_envelope,
    decode_msg,
)

__all__ = [
    "BAKED_FRAME_TYPES",
    "FAILURE_AUDIT_PAYLOAD",
    "FAILURE_COMMAND_PAYLOAD",
    "FAILURE_CONNECTION_PAYLOAD",
    "FAILURE_FRAME_ENVELOPE",
    "FAILURE_FRAME_PAYLOAD",
    "FAILURE_FRAME_UNKNOWN_TYPE",
    "FAILURE_GAP_PAYLOAD",
    "FAILURE_SEGMENT_CORRUPT",
    "MAX_FAILURE_SAMPLES",
    "NOT_BAKED_FRAME_TYPES",
    "STOPPED_DETAIL",
    "DecodeFailure",
    "HourInterpreter",
    "HourResult",
    "RowSink",
    "SegmentFacts",
]

BAKED_FRAME_TYPES: Final = frozenset(
    {
        "orderbook_delta",
        "orderbook_snapshot",
        "trade",
        "market_lifecycle_v2",
        "event_lifecycle",
        "event_fee_update",
    }
)
"""Frame types that become rows."""

NOT_BAKED_FRAME_TYPES: Final = frozenset({"subscribed", "ok", "unsubscribed", "error", "ticker"})
"""Frame types intentionally not baked: subscription replies, and the live-only ``ticker``
channel (ADR 0018), should one ever reach a segment."""

FAILURE_FRAME_ENVELOPE: Final = "frame_envelope"
"""A frame that is not a JSON object with a string ``type``."""

FAILURE_FRAME_PAYLOAD: Final = "frame_payload"
"""A frame of a known type whose payload does not match its struct or holds a malformed number."""

FAILURE_FRAME_UNKNOWN_TYPE: Final = "frame_unknown_type"
"""A frame of a type the baker does not know."""

FAILURE_GAP_PAYLOAD: Final = "gap_payload"
FAILURE_AUDIT_PAYLOAD: Final = "audit_payload"
FAILURE_CONNECTION_PAYLOAD: Final = "connection_payload"
FAILURE_COMMAND_PAYLOAD: Final = "command_payload"

FAILURE_SEGMENT_CORRUPT: Final = "segment_corrupt"
"""Kept as a sample for a corrupt segment; counted in ``corrupt_segments``, not per record."""

MAX_FAILURE_SAMPLES: Final = 5
"""Decode failures kept per class, with where they happened, for investigation."""

STOPPED_DETAIL: Final = "stopped"
"""``detail`` of the ``close`` record a supervisor writes when it is stopped on purpose, as opposed
to losing its connection (``tape.recorder.supervisor``)."""

_EVENT_OPEN: Final = "open"
_EVENT_CLOSE: Final = "close"
_EVENT_ERROR: Final = "error"
_EVENT_CLOCK_JUMP: Final = "clock_jump"
_EVENT_WRITER_OVERFLOW: Final = "writer_overflow"
_ORDERBOOK_CHANNEL: Final = "orderbook_delta"
_LIFECYCLE_TYPE: Final = "market_lifecycle_v2"
_TS_S_BY_EVENT_TYPE: Final = {"settled": "settled_ts", "determined": "determination_ts"}

type AuditOutcome = Literal["exact", "consistent", "inconsistent", "undecidable"]
"""The outcomes of ``tape.recorder.auditor`` (ADR 0021)."""


class RowSink(Protocol):
    """Where interpreted rows go."""

    def add(self, table: TableName, row: Row) -> None:
        """Take one row of a table, in the table's column order."""
        ...


class DecodeFailure(msgspec.Struct, frozen=True, kw_only=True):
    """Where one decode failure happened, kept for investigation.

    Attributes:
        failure: The failure class.
        segment: The segment's name.
        record_index: Position of the record in the segment, from 0.
        detail: The decoder's message.
    """

    failure: str
    segment: str
    record_index: int
    detail: str


class SegmentFacts(msgspec.Struct, frozen=True, kw_only=True):
    """What reading one segment found.

    Attributes:
        records: Data records read.
        first_recv_wall_ns: Wall time of the first record, or ``None`` when there were none.
        last_recv_wall_ns: Wall time of the last record, or ``None`` when there were none.
    """

    records: int
    first_recv_wall_ns: int | None
    last_recv_wall_ns: int | None


class HourResult(msgspec.Struct, frozen=True, kw_only=True):
    """Everything interpreting an hour found besides its rows.

    Attributes:
        accounting: How every record was accounted for.
        integrity: The hour's integrity facts.
        failures: Up to :data:`MAX_FAILURE_SAMPLES` decode failures per class.
    """

    accounting: RecordAccounting
    integrity: HourIntegrity
    failures: tuple[DecodeFailure, ...]


class _GapPayload(msgspec.Struct, frozen=True, kw_only=True):
    sid: int
    expected_seq: int
    got_seq: int


class _AuditPayload(msgspec.Struct, frozen=True, kw_only=True):
    ticker: str
    outcome: AuditOutcome
    levels_rest: int
    send_mono_ns: int
    send_wall_ns: int
    window_open_mono_ns: int
    window_close_mono_ns: int
    window_events: int
    levels_local: int | None = None
    mismatched_levels: int | None = None
    max_abs_diff_e2: int | None = None
    match_index: int | None = None
    fault: str | None = None
    # Empty when absent: a union of Raw and None does not decode a present value.
    rest_levels: msgspec.Raw = msgspec.Raw()
    local_levels: msgspec.Raw = msgspec.Raw()


class _ConnectionPayload(msgspec.Struct, frozen=True, kw_only=True):
    event: str
    detail: str | None = None
    dropped: int | None = None
    wall_ns_delta: int | None = None
    mono_ns_delta: int | None = None


class _CommandParams(msgspec.Struct, frozen=True, kw_only=True):
    sid: int | None = None
    sids: list[int] | None = None
    action: str | None = None
    market_tickers: list[str] | None = None


class _CommandPayload(msgspec.Struct, frozen=True, kw_only=True):
    cmd: str
    id: int | None = None
    params: _CommandParams | None = None


_gap_decoder: Final = msgspec.json.Decoder(_GapPayload)
_audit_decoder: Final = msgspec.json.Decoder(_AuditPayload)
_connection_decoder: Final = msgspec.json.Decoder(_ConnectionPayload)
_command_decoder: Final = msgspec.json.Decoder(_CommandPayload)


@dataclass(slots=True)
class _Market:
    """A market's book time within one segment."""

    sid: int
    since_wall_ns: int
    stale_since_wall_ns: int | None = None


@dataclass(slots=True)
class _PendingGap:
    """A gap whose row waits for the snapshots of the books it staled."""

    row: list[object]
    sid: int
    waiting: set[str]


@dataclass(slots=True)
class _Counters:
    records: dict[str, int] = field(default_factory=dict)
    baked: dict[str, int] = field(default_factory=dict)
    not_baked: dict[str, int] = field(default_factory=dict)
    decode_failures: dict[str, int] = field(default_factory=dict)


def _bump(counts: dict[str, int], key: str, by: int = 1) -> None:
    counts[key] = counts.get(key, 0) + by


def _sorted(counts: dict[str, int]) -> dict[str, int]:
    return dict(sorted(counts.items()))


class HourInterpreter:
    """Interprets the segments of one hour, one after another, in name order.

    Args:
        hour: The hour being baked; a segment that continues a connection is taken to have been
            open since its start.
        sink: Receives every row.
    """

    def __init__(self, hour: HourKey, sink: RowSink) -> None:
        self._hour = hour
        self._sink = sink
        self._counters = _Counters()
        self._corrupt_segments = 0
        self._failures: list[DecodeFailure] = []
        self._failure_samples: dict[str, int] = {}
        self._spans: list[ConnectionSpan] = []
        self._sleeps: set[tuple[int, int]] = set()
        self._gap_count = 0
        self._stale_market_ns = 0
        self._observed_market_ns = 0
        self._audits: dict[str, int] = {}
        self._levels_mismatched = 0
        self._overflow_dropped = 0
        self._lost_connection: dict[int, bool] = {}

    def read_segment(
        self, name: str, header: SegmentHeader, records: Iterable[Record]
    ) -> SegmentFacts:
        """Interpret every record of one segment.

        A record source that raises :class:`TapeCorruptionError` ends the segment there: the
        records before the damage count as usual, and the segment counts as corrupt.

        Args:
            name: The segment's name, for failure samples.
            header: Its header; ``use_yes_price`` and ``conn_id`` come from it.
            records: Its records, in file order.

        Returns:
            How many records were read and when the first and last were received.
        """
        segment = _Segment(
            self,
            name=name,
            conn_id=header.conn_id,
            use_yes_price=header.use_yes_price,
            after_lost_connection=self._lost_connection.get(header.conn_id, False),
        )
        try:
            for record in records:
                segment.feed(record)
        except TapeCorruptionError as exc:
            self.corrupt_segment(name, detail=str(exc), record_index=segment.records)
        facts = segment.finish()
        self._lost_connection[header.conn_id] = segment.lost_connection
        return facts

    def corrupt_segment(self, name: str, *, detail: str, record_index: int = 0) -> None:
        """Count a segment that could not be read to its end for a reason other than truncation.

        Args:
            name: The segment's name.
            detail: What the reader reported.
            record_index: Records read before the damage.
        """
        self._corrupt_segments += 1
        self._failures.append(
            DecodeFailure(
                failure=FAILURE_SEGMENT_CORRUPT,
                segment=name,
                record_index=record_index,
                detail=detail,
            )
        )

    def finish(self) -> HourResult:
        """The hour's accounting and integrity facts.

        Returns:
            Everything found besides the rows, which the sink already holds.
        """
        counters = self._counters
        audits = self._audits
        return HourResult(
            accounting=RecordAccounting(
                records=_sorted(counters.records),
                baked=_sorted(counters.baked),
                not_baked=_sorted(counters.not_baked),
                decode_failures=_sorted(counters.decode_failures),
                corrupt_segments=self._corrupt_segments,
            ),
            integrity=HourIntegrity(
                spans=tuple(self._spans),
                sleeps=tuple(
                    Sleep(start_wall_ns=s, end_wall_ns=e) for s, e in sorted(self._sleeps)
                ),
                gaps=GapFacts(
                    count=self._gap_count,
                    stale_market_ns=self._stale_market_ns,
                    observed_market_ns=self._observed_market_ns,
                ),
                audits=AuditFacts(
                    exact=audits.get("exact", 0),
                    consistent=audits.get("consistent", 0),
                    inconsistent=audits.get("inconsistent", 0),
                    undecidable=audits.get("undecidable", 0),
                    levels_mismatched=self._levels_mismatched,
                ),
                writer_overflow_dropped=self._overflow_dropped,
            ),
            failures=tuple(self._failures),
        )

    # ------------------------------------------------------------ used by _Segment

    @property
    def hour_start_wall_ns(self) -> int:
        """The first instant of the hour being baked."""
        return self._hour.start_wall_ns

    @property
    def counters(self) -> _Counters:
        """The hour's record counts, which every segment adds to."""
        return self._counters

    def emit(self, table: TableName, row: Row) -> None:
        """Pass one row to the sink."""
        self._sink.add(table, row)

    def fail(self, failure: str, *, segment: str, record_index: int, detail: str) -> None:
        """Count a decode failure and keep it as a sample while its class has room."""
        _bump(self._counters.decode_failures, failure)
        kept = self._failure_samples.get(failure, 0)
        if kept < MAX_FAILURE_SAMPLES:
            self._failure_samples[failure] = kept + 1
            self._failures.append(
                DecodeFailure(
                    failure=failure, segment=segment, record_index=record_index, detail=detail
                )
            )

    def add_span(self, span: ConnectionSpan) -> None:
        """Keep a connection span."""
        self._spans.append(span)

    def add_sleep(self, start_wall_ns: int, end_wall_ns: int) -> None:
        """Keep a host sleep; every taped sink records the same one, so duplicates collapse."""
        self._sleeps.add((start_wall_ns, end_wall_ns))

    def add_gap(self) -> None:
        """Count a gap record."""
        self._gap_count += 1

    def add_book_time(self, *, observed_ns: int, stale_ns: int) -> None:
        """Add market time observed and market time spent stale."""
        self._observed_market_ns += observed_ns
        self._stale_market_ns += stale_ns

    def add_audit(self, outcome: str, mismatched_levels: int | None) -> None:
        """Count an audit by outcome and add its mismatched levels when it was decidable."""
        _bump(self._audits, outcome)
        if outcome != "undecidable" and mismatched_levels is not None:
            self._levels_mismatched += mismatched_levels

    def add_overflow(self, dropped: int) -> None:
        """Add records the recorder's writer refused."""
        self._overflow_dropped += dropped


class _Segment:
    """The state of one segment while its records are interpreted."""

    def __init__(
        self,
        hour: HourInterpreter,
        *,
        name: str,
        conn_id: int,
        use_yes_price: bool,
        after_lost_connection: bool,
    ) -> None:
        self._hour = hour
        self._name = name
        self._conn_id = conn_id
        self._use_yes_price = use_yes_price
        self._after_lost_connection = after_lost_connection
        self._index = 0
        self._first_wall_ns: int | None = None
        self._last_wall_ns: int | None = None
        # Whether the segment began with a connection being opened rather than continuing one.
        self._opens_connection = False
        self._span_start: int | None = None
        self._span_opened = False
        self._last_event: str | None = None
        self._last_detail: str | None = None
        self._book_sids: set[int] = set()
        self._requested: dict[int, set[str]] = {}
        self._snapshotted: set[tuple[int, str]] = set()
        self._markets: dict[str, _Market] = {}
        self._pending_gaps: list[_PendingGap] = []

    @property
    def records(self) -> int:
        """Records fed so far."""
        return self._index

    @property
    def lost_connection(self) -> bool:
        """Whether the segment ended with its connection lost rather than stopped on purpose."""
        return self._last_event == _EVENT_CLOSE and self._last_detail != STOPPED_DETAIL

    def feed(self, record: Record) -> None:
        wall_ns = record.recv_wall_ns
        if self._first_wall_ns is None:
            self._first_wall_ns = wall_ns
            self._begin(record)
        self._last_wall_ns = wall_ns
        _bump(self._hour.counters.records, record.kind.name.lower())
        match record.kind:
            case RecordKind.FRAME:
                self._frame(record)
            case RecordKind.COMMAND:
                self._command(record)
            case RecordKind.GAP:
                self._gap(record)
            case RecordKind.CONNECTION:
                self._connection(record)
            case RecordKind.AUDIT:
                self._audit(record)
        self._index += 1

    def finish(self) -> SegmentFacts:
        end = self._last_wall_ns
        if end is not None:
            for market in self._markets.values():
                self._end_market(market, end)
            if self._span_start is not None:
                self._close_span(end, closed=False)
        for pending in self._pending_gaps:
            self._hour.emit("gaps", tuple(pending.row))
        self._pending_gaps.clear()
        return SegmentFacts(
            records=self._index,
            first_recv_wall_ns=self._first_wall_ns,
            last_recv_wall_ns=self._last_wall_ns,
        )

    # -------------------------------------------------------------------- records

    def _begin(self, record: Record) -> None:
        """Decide from the first record whether the segment continues an open connection.

        A supervisor writes ``open`` or ``error`` first in the file a new connection attempt
        opens, because a lost connection rotates the segment (docs/INTERFACES.md 8.4). Any other
        first record means the file was opened by the hour changing under a live connection.
        """
        if record.kind is RecordKind.CONNECTION:
            try:
                event = _connection_decoder.decode(record.payload).event
            except (msgspec.DecodeError, msgspec.ValidationError):
                event = None
            if event in (_EVENT_OPEN, _EVENT_ERROR):
                self._opens_connection = True
                return
        self._span_start = self._hour.hour_start_wall_ns
        self._span_opened = False

    def _fail(self, failure: str, exc: Exception) -> None:
        self._hour.fail(failure, segment=self._name, record_index=self._index, detail=str(exc))

    def _frame(self, record: Record) -> None:
        try:
            envelope = decode_envelope(record.payload)
        except WireError as exc:
            self._fail(FAILURE_FRAME_ENVELOPE, exc)
            return
        message_type = envelope.type
        if message_type in NOT_BAKED_FRAME_TYPES:
            self._reply(envelope)
            return
        if message_type not in BAKED_FRAME_TYPES:
            self._fail(FAILURE_FRAME_UNKNOWN_TYPE, WireError(f"unknown type {message_type!r}"))
            return
        receipt = Receipt(
            conn_id=record.conn_id,
            recv_mono_ns=Ns(record.recv_mono_ns),
            recv_wall_ns=Ns(record.recv_wall_ns),
        )
        try:
            self._data_frame(envelope, receipt)
        except (WireError, FixedPointError) as exc:
            self._fail(FAILURE_FRAME_PAYLOAD, exc)
            return
        _bump(self._hour.counters.baked, message_type)

    def _reply(self, envelope: Envelope) -> None:
        if envelope.type == "subscribed":
            try:
                subscribed = decode_msg(envelope, SubscribedMsg)
            except WireError as exc:
                self._fail(FAILURE_FRAME_PAYLOAD, exc)
                return
            if subscribed.channel == _ORDERBOOK_CHANNEL:
                self._book_sids.add(subscribed.sid)
        _bump(self._hour.counters.not_baked, envelope.type)

    def _data_frame(self, envelope: Envelope, receipt: Receipt) -> None:
        """Convert one data frame completely, then emit its rows.

        Raises:
            WireError: If the payload does not match its struct or lacks a ``sid``.
            FixedPointError: If a price or count is malformed.
        """
        match envelope.type:
            case "orderbook_delta":
                self._delta(envelope, receipt)
            case "orderbook_snapshot":
                self._snapshot(envelope, receipt)
            case "trade":
                self._trade(envelope, receipt)
            case "market_lifecycle_v2":
                self._market_lifecycle(envelope, receipt)
            case _:
                self._event_message(envelope, receipt)

    def _delta(self, envelope: Envelope, receipt: Receipt) -> None:
        delta = to_book_delta(
            decode_msg(envelope, OrderbookDeltaMsg),
            envelope,
            receipt,
            use_yes_price=self._use_yes_price,
        )
        self._book_sids.add(delta.sid)
        self._observe(delta.ticker, delta.sid, receipt.recv_wall_ns)
        self._hour.emit(
            "deltas",
            (
                delta.ticker,
                delta.ts_ms,
                receipt.recv_mono_ns,
                receipt.recv_wall_ns,
                receipt.conn_id,
                delta.sid,
                delta.seq,
                int(delta.side),
                delta.price,
                delta.delta,
                delta.own_client_order_id,
            ),
        )

    def _snapshot(self, envelope: Envelope, receipt: Receipt) -> None:
        snapshot = to_book_snapshot(
            decode_msg(envelope, OrderbookSnapshotMsg),
            envelope,
            receipt,
            use_yes_price=self._use_yes_price,
        )
        ticker, sid, wall_ns = snapshot.ticker, snapshot.sid, receipt.recv_wall_ns
        reason = self._snapshot_reason(sid, ticker)
        base = (ticker, wall_ns, receipt.recv_mono_ns, receipt.conn_id, sid, snapshot.seq)
        rows: list[Row] = [(*base, 0, level.price, level.count, reason) for level in snapshot.bids]
        rows.extend((*base, 1, level.price, level.count, reason) for level in snapshot.asks)
        if not rows:
            rows.append((*base, EMPTY_BOOK_SIDE, 0, 0, reason))
        self._book_sids.add(sid)
        self._observe(ticker, sid, wall_ns)
        market = self._markets[ticker]
        if market.stale_since_wall_ns is not None:
            self._hour.add_book_time(observed_ns=0, stale_ns=wall_ns - market.stale_since_wall_ns)
            market.stale_since_wall_ns = None
        for row in rows:
            self._hour.emit("snapshots", row)
        self._resolve_gaps(sid, ticker, wall_ns)

    def _snapshot_reason(self, sid: int, ticker: str) -> int:
        """Why a snapshot arrived, from what this segment shows (docs/DATA_FORMATS.md 6)."""
        key = (sid, ticker)
        requested = self._requested.get(sid)
        repeated = key in self._snapshotted
        self._snapshotted.add(key)
        if requested is not None and ticker in requested:
            requested.discard(ticker)
            return SNAPSHOT_RESYNC
        if repeated:
            return SNAPSHOT_RESYNC
        if self._opens_connection and self._after_lost_connection:
            return SNAPSHOT_RECONNECT
        return SNAPSHOT_INITIAL

    def _trade(self, envelope: Envelope, receipt: Receipt) -> None:
        trade = to_trade(decode_msg(envelope, TradeMsg), envelope, receipt)
        self._hour.emit(
            "trades",
            (
                trade.ticker,
                trade.trade_id,
                trade.ts_ms,
                receipt.recv_wall_ns,
                trade.sid,
                trade.seq,
                trade.price,
                trade.count,
                int(trade.taker_side),
                trade.is_block,
            ),
        )

    def _market_lifecycle(self, envelope: Envelope, receipt: Receipt) -> None:
        msg = decode_msg(envelope, MarketLifecycleV2Msg)
        lifecycle = to_lifecycle(msg, envelope, receipt)
        ts_field = _TS_S_BY_EVENT_TYPE.get(msg.event_type)
        ts_s = None if ts_field is None else getattr(msg, ts_field)
        self._hour.emit(
            "lifecycle",
            (
                lifecycle.ticker,
                _LIFECYCLE_TYPE,
                lifecycle.event_type,
                ts_s,
                receipt.recv_wall_ns,
                lifecycle.sid,
                lifecycle.seq,
                lifecycle.payload_json,
            ),
        )

    def _event_message(self, envelope: Envelope, receipt: Receipt) -> None:
        """An event-level message on the lifecycle channel, keyed by its event ticker."""
        if envelope.type == "event_lifecycle":
            event_ticker = decode_msg(envelope, EventLifecycleMsg).event_ticker
        else:
            event_ticker = decode_msg(envelope, EventFeeUpdateMsg).event_ticker
        if envelope.sid is None:
            raise WireError(f"{envelope.type} without sid")
        self._hour.emit(
            "lifecycle",
            (
                event_ticker,
                envelope.type,
                None,
                None,
                receipt.recv_wall_ns,
                envelope.sid,
                envelope.seq,
                bytes(envelope.msg).decode("utf-8"),
            ),
        )

    def _command(self, record: Record) -> None:
        try:
            command = _command_decoder.decode(record.payload)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            self._fail(FAILURE_COMMAND_PAYLOAD, exc)
            return
        _bump(self._hour.counters.not_baked, "command")
        params = command.params
        if params is None:
            return
        wall_ns = record.recv_wall_ns
        tickers = params.market_tickers or []
        if command.cmd == "update_subscription" and params.sid is not None:
            if params.action == "get_snapshot":
                self._requested.setdefault(params.sid, set()).update(tickers)
            elif params.action == "delete_markets":
                for ticker in tickers:
                    self._leave(ticker, wall_ns)
        elif command.cmd == "unsubscribe" and params.sids:
            retired = set(params.sids)
            for ticker in [t for t, m in self._markets.items() if m.sid in retired]:
                self._leave(ticker, wall_ns)

    def _gap(self, record: Record) -> None:
        try:
            gap = _gap_decoder.decode(record.payload)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            self._fail(FAILURE_GAP_PAYLOAD, exc)
            return
        _bump(self._hour.counters.baked, "gap")
        self._hour.add_gap()
        wall_ns = record.recv_wall_ns
        row: list[object] = [
            record.conn_id,
            gap.sid,
            wall_ns,
            record.recv_mono_ns,
            gap.expected_seq,
            gap.got_seq,
            None,
        ]
        waiting: set[str] = set()
        if gap.sid in self._book_sids:
            # A gap on a book subscription stales every book it carries (docs/INTERFACES.md 8.4).
            for ticker, market in self._markets.items():
                if market.sid == gap.sid:
                    waiting.add(ticker)
                    if market.stale_since_wall_ns is None:
                        market.stale_since_wall_ns = wall_ns
        if waiting:
            self._pending_gaps.append(_PendingGap(row=row, sid=gap.sid, waiting=waiting))
        else:
            self._hour.emit("gaps", tuple(row))

    def _audit(self, record: Record) -> None:
        try:
            audit = _audit_decoder.decode(record.payload)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            self._fail(FAILURE_AUDIT_PAYLOAD, exc)
            return
        _bump(self._hour.counters.baked, "audit")
        self._hour.add_audit(audit.outcome, audit.mismatched_levels)
        self._hour.emit(
            "audits",
            (
                audit.ticker,
                record.recv_wall_ns,
                record.conn_id,
                audit.outcome,
                audit.send_wall_ns,
                audit.send_mono_ns,
                record.recv_mono_ns,
                audit.window_open_mono_ns,
                audit.window_close_mono_ns,
                audit.window_events,
                audit.match_index,
                audit.levels_rest,
                audit.levels_local,
                audit.mismatched_levels,
                audit.max_abs_diff_e2,
                audit.fault,
                _raw_text(audit.rest_levels),
                _raw_text(audit.local_levels),
            ),
        )

    def _connection(self, record: Record) -> None:
        try:
            connection = _connection_decoder.decode(record.payload)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            self._fail(FAILURE_CONNECTION_PAYLOAD, exc)
            return
        _bump(self._hour.counters.not_baked, "connection")
        wall_ns = record.recv_wall_ns
        event = connection.event
        if event == _EVENT_OPEN:
            if self._span_start is not None:
                self._close_span(wall_ns, closed=False)
            self._span_start, self._span_opened = wall_ns, True
        elif event == _EVENT_CLOSE:
            if self._span_start is not None:
                self._close_span(wall_ns, closed=True)
        elif event == _EVENT_CLOCK_JUMP:
            wall_delta, mono_delta = connection.wall_ns_delta, connection.mono_ns_delta
            if wall_delta is not None and mono_delta is not None and wall_delta > mono_delta:
                self._hour.add_sleep(wall_ns - (wall_delta - mono_delta), wall_ns)
        elif event == _EVENT_WRITER_OVERFLOW and connection.dropped is not None:
            self._hour.add_overflow(connection.dropped)
        if event in (_EVENT_OPEN, _EVENT_CLOSE, _EVENT_ERROR):
            self._last_event, self._last_detail = event, connection.detail

    # ------------------------------------------------------------------ book time

    def _observe(self, ticker: str, sid: int, wall_ns: int) -> None:
        if ticker not in self._markets:
            self._markets[ticker] = _Market(sid=sid, since_wall_ns=wall_ns)

    def _leave(self, ticker: str, wall_ns: int) -> None:
        market = self._markets.pop(ticker, None)
        if market is not None:
            self._end_market(market, wall_ns)

    def _end_market(self, market: _Market, end_wall_ns: int) -> None:
        stale_ns = 0
        if market.stale_since_wall_ns is not None:
            stale_ns = max(0, end_wall_ns - market.stale_since_wall_ns)
        self._hour.add_book_time(
            observed_ns=max(0, end_wall_ns - market.since_wall_ns), stale_ns=stale_ns
        )

    def _resolve_gaps(self, sid: int, ticker: str, wall_ns: int) -> None:
        still_pending: list[_PendingGap] = []
        for pending in self._pending_gaps:
            if pending.sid == sid:
                pending.waiting.discard(ticker)
                if not pending.waiting:
                    pending.row[-1] = wall_ns
                    self._hour.emit("gaps", tuple(pending.row))
                    continue
            still_pending.append(pending)
        self._pending_gaps = still_pending

    def _close_span(self, end_wall_ns: int, *, closed: bool) -> None:
        start = self._span_start
        if start is None:
            return
        self._hour.add_span(
            ConnectionSpan(
                conn_id=self._conn_id,
                start_wall_ns=start,
                end_wall_ns=max(start, end_wall_ns),
                opened=self._span_opened,
                closed=closed,
            )
        )
        self._span_start = None


def _raw_text(raw: msgspec.Raw) -> str | None:
    return bytes(raw).decode("utf-8") if len(raw) else None
