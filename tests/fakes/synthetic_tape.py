"""Synthetic raw segments for bake, prune, and catalog tests, written with the real segment writer.

A :class:`SegmentScript` collects the records one connection's segment would hold, each stamped
with an explicit wall time, in the exact payload shapes the recorder writes
(docs/DATA_FORMATS.md 4): frames as Kalshi sends them, commands as ``encode_command`` emits them,
and gap, connection, and audit records as the supervisor, the sink, and the auditor encode them.
"""

from __future__ import annotations

import datetime as dt
import struct
from collections.abc import Sequence
from pathlib import Path
from typing import Final, Self

import msgspec
import zstandard

from tape.bake.layout import DataLayout, HourKey
from tape.segment import FORMAT_VERSION, MAGIC, Record, RecordKind, SegmentHeader, SegmentWriter

HOUR: Final = HourKey(dt.date(2026, 9, 10), 12)
"""The hour most tests bake."""

START: Final = HOUR.start_wall_ns
"""Wall-clock nanoseconds at the start of :data:`HOUR`."""

MS: Final = 1_000_000
SECOND: Final = 1_000_000_000

MONO_ORIGIN: Final = HourKey(dt.date(2026, 9, 1), 0).start_wall_ns
"""The wall time at which the synthetic monotonic clock reads zero, before every hour tests use."""


def mono_of(wall_ns: int) -> int:
    """The synthetic monotonic reading taken with a wall-clock reading."""
    return wall_ns - MONO_ORIGIN


type Levels = Sequence[tuple[str, str]]

_encoder: Final = msgspec.json.Encoder()


def header(
    conn_id: int, *, created_wall_ns: int = START, use_yes_price: bool = True
) -> SegmentHeader:
    return SegmentHeader(
        created_wall_ns=created_wall_ns,
        host="test",
        env="demo",
        conn_id=conn_id,
        ws_url="wss://example/trade-api/ws/v2",
        use_yes_price=use_yes_price,
        subscriptions=[],
        software_version="0.1.0",
        spec_versions={"asyncapi": "2.0.0", "openapi": "3.30.0"},
    )


class SegmentScript:
    """The records of one segment of one connection, in file order."""

    def __init__(self, conn_id: int, *, use_yes_price: bool = True) -> None:
        self.conn_id = conn_id
        self.use_yes_price = use_yes_price
        self.records: list[Record] = []
        self.wall_step_ns = 0

    def step_wall_clock(self, delta_ns: int) -> Self:
        """Offset every later wall reading from the monotonic clock, as a stepped clock does."""
        self.wall_step_ns += delta_ns
        return self

    def record(self, kind: RecordKind, wall_ns: int, payload: bytes) -> Self:
        self.records.append(
            Record(
                kind=kind,
                conn_id=self.conn_id,
                recv_mono_ns=mono_of(wall_ns - self.wall_step_ns),
                recv_wall_ns=wall_ns,
                payload=payload,
            )
        )
        return self

    def frame(self, wall_ns: int, message: dict[str, object] | bytes) -> Self:
        payload = message if isinstance(message, bytes) else _encoder.encode(message)
        return self.record(RecordKind.FRAME, wall_ns, payload)

    def delta(
        self,
        wall_ns: int,
        *,
        sid: int,
        seq: int,
        ticker: str,
        price: str = "0.5000",
        delta: str = "1.00",
        side: str = "yes",
        ts_ms: int | None = None,
        client_order_id: str | None = None,
    ) -> Self:
        msg: dict[str, object] = {
            "market_ticker": ticker,
            "market_id": "00000000-0000-0000-0000-000000000000",
            "price_dollars": price,
            "delta_fp": delta,
            "side": side,
            "ts_ms": wall_ns // MS if ts_ms is None else ts_ms,
        }
        if client_order_id is not None:
            msg["client_order_id"] = client_order_id
        return self.frame(wall_ns, {"type": "orderbook_delta", "sid": sid, "seq": seq, "msg": msg})

    def snapshot(
        self,
        wall_ns: int,
        *,
        sid: int,
        seq: int,
        ticker: str,
        yes: Levels = (("0.4000", "10.00"),),
        no: Levels = (("0.6000", "5.00"),),
    ) -> Self:
        msg: dict[str, object] = {"market_ticker": ticker, "market_id": "m"}
        if yes:
            msg["yes_dollars_fp"] = [list(level) for level in yes]
        if no:
            msg["no_dollars_fp"] = [list(level) for level in no]
        return self.frame(
            wall_ns, {"type": "orderbook_snapshot", "sid": sid, "seq": seq, "msg": msg}
        )

    def trade(
        self,
        wall_ns: int,
        *,
        sid: int,
        seq: int,
        ticker: str,
        trade_id: str,
        price: str = "0.5500",
        count: str = "3.00",
        taker_book_side: str = "bid",
    ) -> Self:
        msg = {
            "trade_id": trade_id,
            "market_ticker": ticker,
            "yes_price_dollars": price,
            "no_price_dollars": "0.4500",
            "count_fp": count,
            "taker_outcome_side": "yes",
            "taker_book_side": taker_book_side,
            "is_block_trade": False,
            "ts_ms": wall_ns // MS,
        }
        return self.frame(wall_ns, {"type": "trade", "sid": sid, "seq": seq, "msg": msg})

    def lifecycle(
        self, wall_ns: int, *, sid: int, seq: int, ticker: str, event_type: str, **fields: object
    ) -> Self:
        msg = {"market_ticker": ticker, "event_type": event_type, **fields}
        return self.frame(
            wall_ns, {"type": "market_lifecycle_v2", "sid": sid, "seq": seq, "msg": msg}
        )

    def subscribed(self, wall_ns: int, *, channel: str, sid: int, command_id: int = 1) -> Self:
        return self.frame(
            wall_ns,
            {"type": "subscribed", "id": command_id, "msg": {"channel": channel, "sid": sid}},
        )

    def command(self, wall_ns: int, cmd: str, command_id: int, **params: object) -> Self:
        body: dict[str, object] = {"id": command_id, "cmd": cmd}
        if params:
            body["params"] = params
        return self.record(RecordKind.COMMAND, wall_ns, _encoder.encode(body))

    def gap(self, wall_ns: int, *, sid: int, expected_seq: int, got_seq: int) -> Self:
        payload = {"sid": sid, "expected_seq": expected_seq, "got_seq": got_seq}
        return self.record(RecordKind.GAP, wall_ns, _encoder.encode(payload))

    def connection(self, wall_ns: int, event: str, **fields: object) -> Self:
        return self.record(
            RecordKind.CONNECTION, wall_ns, _encoder.encode({"event": event, **fields})
        )

    def opened(self, wall_ns: int) -> Self:
        return self.connection(wall_ns, "open", detail="connected")

    def closed(self, wall_ns: int, detail: str = "connection closed by peer") -> Self:
        return self.connection(wall_ns, "close", detail=detail)

    def audit(self, wall_ns: int, *, ticker: str, outcome: str, **fields: object) -> Self:
        payload: dict[str, object] = {"ticker": ticker, "levels_rest": 2}
        if outcome != "undecidable":
            payload |= {"levels_local": 2, "mismatched_levels": 0, "max_abs_diff_e2": 0}
        payload |= {
            "outcome": outcome,
            "send_mono_ns": mono_of(wall_ns) - 100,
            "send_wall_ns": wall_ns - 100,
            "window_open_mono_ns": mono_of(wall_ns) - 200,
            "window_close_mono_ns": mono_of(wall_ns) + 300,
            "window_events": 0,
        }
        payload |= fields
        return self.record(RecordKind.AUDIT, wall_ns, _encoder.encode(payload))

    def write(self, layout: DataLayout, hour: HourKey, counter: int) -> Path:
        """Write the segment as ``conn-NN-UUUU.tape.zst`` in the hour's raw directory."""
        path = segment_file(layout, hour, self.conn_id, counter)
        with SegmentWriter(path, header(self.conn_id, use_yes_price=self.use_yes_price)) as writer:
            for record in self.records:
                writer.append(record)
        return path

    def write_truncated(self, layout: DataLayout, hour: HourKey, counter: int, *, cut: int) -> Path:
        """Write the segment without its last ``cut`` bytes, as a crash mid-record leaves it."""
        path = segment_file(layout, hour, self.conn_id, counter)
        header_json = msgspec.json.encode(header(self.conn_id, use_yes_price=self.use_yes_price))
        body = bytearray(
            struct.pack("<4sHI", MAGIC, FORMAT_VERSION, len(header_json)) + header_json
        )
        for record in self.records:
            body += struct.pack(
                "<BHQQI",
                int(record.kind),
                record.conn_id,
                record.recv_mono_ns,
                record.recv_wall_ns,
                len(record.payload),
            )
            body += record.payload
        path.write_bytes(zstandard.ZstdCompressor().compress(bytes(body[:-cut])))
        return path


def segment_file(layout: DataLayout, hour: HourKey, conn_id: int, counter: int) -> Path:
    directory = layout.hour_dir(hour)
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"conn-{conn_id:02d}-{counter:04d}.tape.zst"
