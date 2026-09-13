"""Historical queries over keyframes and baked tables: books, deltas, trades, and integrity.

Responsibility: answer the historical questions of docs/ARCHITECTURE.md 5 from the files the
recorder and the baker wrote, reading only the part files and row groups that can hold the answer.

Engine: pyarrow datasets rather than DuckDB (docs/INTERFACES.md 10). Every query here names markets
and a receive-time range; part files are sorted by market in small row groups, so the statistics of
the plain string ``ticker`` column and of ``recv_wall_ns`` let pyarrow skip the rest without a query
engine held in memory on the 1 GB host (ADR 0024).

Invariants: a book is rebuilt from the latest keyframe taken at or before the instant, then every
baked snapshot and delta of its market received after that keyframe and at or before the instant,
in the order the recorder applied them (docs/INTERFACES.md 5, 8.4): one subscription's changes by
sequence number, subscriptions by receive time. A snapshot replaces the book, a delta on a stale
book is ignored, a change that breaks an invariant leaves the book stale, and a gap on the
subscription that last changed the book marks it stale; a market with no keyframe row starts stale,
so it is fresh only after a snapshot; and nothing here writes.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from tape.bake.layout import NS_PER_HOUR, DataLayout, HourKey
from tape.bake.manifest import Manifest, read_manifest
from tape.bake.tables import TABLES, TableName
from tape.book import Book, books_from_keyframe_rows
from tape.errors import ArchiveError, BookInvariantError
from tape.events import Level, Side
from tape.fixedpoint import CountE2, PriceE4
from tape.segment import read_keyframe
from tape.timeutil import NS_PER_S, Ms

__all__ = ["KEYFRAME_LOOKBACK_HOURS", "Catalog", "replay"]

KEYFRAME_LOOKBACK_HOURS: Final = 24
"""Hours searched back for a keyframe; keyframes are minutes apart while the recorder runs."""

_WRITE_LAG_HOURS: Final = 1
"""A record received just before an hour ends may be written into the next hour's segment,
because the writer places records by the time it writes them (docs/INTERFACES.md 8.4)."""

_MONO_RESTART_NS: Final = 600 * NS_PER_S
"""A monotonic receipt this far below one received earlier means the recording host restarted: a
monotonic clock restarts with its host, and a wall clock step is far smaller than ten minutes."""

_GAP_ORDER: Final = 0
_CHANGE_ORDER: Final = 1
_DELTA_COLUMNS: Final = (
    "ticker",
    "recv_wall_ns",
    "recv_mono_ns",
    "conn_id",
    "sid",
    "seq",
    "side",
    "price_e4",
    "delta_e2",
    "ts_ms",
)
_SNAPSHOT_COLUMNS: Final = (
    "ticker",
    "recv_wall_ns",
    "recv_mono_ns",
    "conn_id",
    "sid",
    "seq",
    "side",
    "price_e4",
    "count_e2",
)
_GAP_COLUMNS: Final = ("conn_id", "sid", "recv_wall_ns", "recv_mono_ns")
_REPLAY_COLUMNS: Final[Mapping[TableName, tuple[str, ...]]] = MappingProxyType(
    {"deltas": _DELTA_COLUMNS, "snapshots": _SNAPSHOT_COLUMNS, "gaps": _GAP_COLUMNS}
)


class Catalog:
    """Reads one archive's keyframes, baked tables, and manifests.

    Args:
        layout: The archive.
        keyframe_lookback_hours: Hours searched back for a keyframe before an instant.

    Raises:
        ValueError: If ``keyframe_lookback_hours`` is negative.
    """

    def __init__(
        self, layout: DataLayout, *, keyframe_lookback_hours: int = KEYFRAME_LOOKBACK_HOURS
    ) -> None:
        if keyframe_lookback_hours < 0:
            raise ValueError("keyframe_lookback_hours must be non-negative")
        self._layout = layout
        self._lookback_hours = keyframe_lookback_hours

    def book_at(self, ticker: str, at_wall_ns: int) -> Book:
        """One market's book as the recorder held it at an instant.

        Args:
            ticker: The market.
            at_wall_ns: The instant, as receive wall time.

        Returns:
            The rebuilt book; stale when the recorder's book would have been, or when nothing
            recorded before the instant makes it known.

        Raises:
            TapeCorruptionError: If a keyframe has an unexpected schema.
        """
        return self.books_at(at_wall_ns, tickers=(ticker,))[ticker]

    def books_at(
        self,
        at_wall_ns: int,
        *,
        tickers: Collection[str] | None = None,
        start_wall_ns: int | None = None,
    ) -> dict[str, Book]:
        """Several markets' books at one instant, from one keyframe and one read of the tables.

        Args:
            at_wall_ns: The instant, as receive wall time.
            tickers: The markets; ``None`` means every market in the keyframe or changed after it.
            start_wall_ns: Start from the latest keyframe taken at or before this instant rather
                than before ``at_wall_ns``; replaying from an earlier keyframe checks a later one.

        Returns:
            Books by ticker; every requested market is present.

        Raises:
            ValueError: If ``start_wall_ns`` is after ``at_wall_ns``.
            TapeCorruptionError: If a keyframe has an unexpected schema.
        """
        if start_wall_ns is not None and start_wall_ns > at_wall_ns:
            raise ValueError(f"start_wall_ns {start_wall_ns} is after at_wall_ns {at_wall_ns}")
        keyframe_by = at_wall_ns if start_wall_ns is None else start_wall_ns
        start_ns, books = self._keyframe_books(keyframe_by, tickers)
        wanted = None if tickers is None else sorted(set(tickers))
        window = (pc.field("recv_wall_ns") > start_ns) & (pc.field("recv_wall_ns") <= at_wall_ns)
        tables = {
            name: self._scan(
                name,
                start_ns=start_ns,
                end_ns=at_wall_ns,
                time_filter=window,
                tickers=None if name == "gaps" else wanted,
                columns=columns,
            )
            for name, columns in _REPLAY_COLUMNS.items()
        }
        for ticker in wanted or ():
            books.setdefault(ticker, Book(ticker))
        replay(books, deltas=tables["deltas"], snapshots=tables["snapshots"], gaps=tables["gaps"])
        return books

    def deltas(self, ticker: str, t0: int, t1: int) -> pa.Table:
        """One market's deltas received in ``[t0, t1)``, in receive order.

        Raises:
            ValueError: If ``t1`` is before ``t0``.
        """
        return self._range("deltas", ticker, t0, t1)

    def trades(self, ticker: str, t0: int, t1: int) -> pa.Table:
        """One market's trades received in ``[t0, t1)``, in receive order.

        Raises:
            ValueError: If ``t1`` is before ``t0``.
        """
        return self._range("trades", ticker, t0, t1)

    def integrity(self, day: dt.date) -> Manifest:
        """The manifest of one UTC day, with its integrity numbers.

        Raises:
            ArchiveError: If the day has no manifest.
            TapeCorruptionError: If the manifest does not decode.
        """
        manifest = read_manifest(self._layout.manifest_path(day))
        if manifest is None:
            raise ArchiveError(f"no manifest for {day.isoformat()}; has the day been baked?")
        return manifest

    # ----------------------------------------------------------------- internals

    def _range(self, table: TableName, ticker: str, t0: int, t1: int) -> pa.Table:
        if t1 < t0:
            raise ValueError(f"t1 {t1} is before t0 {t0}")
        received = pc.field("recv_wall_ns")
        rows = self._scan(
            table,
            start_ns=t0,
            end_ns=t1,
            time_filter=(received >= t0) & (received < t1),
            tickers=[ticker],
            columns=None,
        )
        return rows.sort_by([("recv_wall_ns", "ascending"), ("seq", "ascending")])

    def _scan(
        self,
        table: TableName,
        *,
        start_ns: int,
        end_ns: int,
        time_filter: pc.Expression,
        tickers: list[str] | None,
        columns: Iterable[str] | None,
    ) -> pa.Table:
        """Rows of a table from the partitions that can hold receive times in a range."""
        spec = TABLES[table]
        condition = time_filter
        if tickers is not None and len(tickers) == 1:
            condition = condition & (pc.field("ticker") == tickers[0])
        elif tickers is not None:
            condition = condition & pc.field("ticker").isin(tickers)
        dataset = ds.dataset(
            self._part_files(table, start_ns, end_ns), schema=spec.schema, format="parquet"
        )
        return dataset.to_table(
            filter=condition, columns=None if columns is None else list(columns)
        )

    def _part_files(self, table: TableName, start_ns: int, end_ns: int) -> list[str]:
        files: list[str] = []
        first = HourKey.of_wall_ns(start_ns).start_wall_ns
        last = HourKey.of_wall_ns(end_ns).start_wall_ns + _WRITE_LAG_HOURS * NS_PER_HOUR
        for start in range(first, last + 1, NS_PER_HOUR):
            directory = self._layout.partition_dir(table, HourKey.of_wall_ns(start))
            if directory.is_dir():
                files.extend(str(path) for path in sorted(directory.glob("part-*.parquet")))
        return files

    def _keyframe_books(
        self, at_wall_ns: int, tickers: Collection[str] | None
    ) -> tuple[int, dict[str, Book]]:
        """The latest keyframe taken at or before an instant, as books, and when it was taken.

        With no keyframe in the lookback window, replay starts at the window's start with no books.
        """
        hour = HourKey.of_wall_ns(at_wall_ns)
        for back in range(self._lookback_hours + 1):
            searched = HourKey.of_wall_ns(hour.start_wall_ns - back * NS_PER_HOUR)
            directory = self._layout.keyframe_dir(searched)
            if not directory.is_dir():
                continue
            for path in sorted(directory.glob("*.parquet"), reverse=True):
                taken_ns = _keyframe_instant(path)
                if taken_ns is not None and taken_ns <= at_wall_ns:
                    rows = read_keyframe(path, tickers=tickers)
                    return taken_ns, books_from_keyframe_rows(rows)
        return hour.start_wall_ns - self._lookback_hours * NS_PER_HOUR, {}


def _keyframe_instant(path: Path) -> int | None:
    """When a keyframe was taken, or ``None`` when the file holds no rows."""
    taken = pq.read_table(path, columns=["as_of_recv_ns"])["as_of_recv_ns"]
    if len(taken) == 0:
        return None
    latest = pc.max(taken).as_py()
    return None if latest is None else int(latest)


@dataclass(frozen=True, slots=True)
class _Delta:
    ticker: str
    conn_id: int
    sid: int
    side: Side
    price: PriceE4
    delta: int
    ts_ms: Ms | None


@dataclass(frozen=True, slots=True)
class _Snapshot:
    ticker: str
    conn_id: int
    sid: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]


@dataclass(frozen=True, slots=True)
class _Gap:
    conn_id: int
    sid: int


type _Change = _Delta | _Snapshot | _Gap


@dataclass(frozen=True, slots=True)
class _Stamped:
    """A change with the receipt times that order it."""

    recv_wall_ns: int
    recv_mono_ns: int
    order: int
    change: _Change


def replay(
    books: dict[str, Book], *, deltas: pa.Table, snapshots: pa.Table, gaps: pa.Table
) -> None:
    """Apply baked changes to books in the order the recorder applied them live.

    Changes are ordered by monotonic receive time, a gap just before the frame that revealed it,
    because the wall clock that stamps receipts can step backwards; it did on the recorded archive
    (docs/OPERATIONS.md 5), and sequence numbers restart with every connection. A monotonic clock
    restarts with its host, so changes are first taken in wall-clock order and split wherever a
    monotonic reading falls more than :data:`_MONO_RESTART_NS` below one received before it; each
    stretch is applied whole before the next. A book missing from ``books`` is created stale when
    its market first changes.

    Args:
        books: Books by ticker, updated in place.
        deltas: ``deltas`` rows with at least the columns the catalog reads.
        snapshots: ``snapshots`` rows likewise; one snapshot's rows share its receipt and sequence.
        gaps: ``gaps`` rows likewise.
    """
    events = [*_delta_events(deltas), *_snapshot_events(snapshots), *_gap_events(gaps)]
    keys: list[tuple[int, int, int, int]] = [(0, 0, 0, 0)] * len(events)
    restarts = 0
    highest: int | None = None
    for index in sorted(range(len(events)), key=lambda i: (events[i].recv_wall_ns, i)):
        mono = events[index].recv_mono_ns
        if highest is not None and mono < highest - _MONO_RESTART_NS:
            restarts += 1
            highest = mono
        else:
            highest = mono if highest is None else max(highest, mono)
        keys[index] = (restarts, mono, events[index].order, index)
    last_subscription: dict[str, tuple[int, int]] = {}
    for index in sorted(range(len(events)), key=keys.__getitem__):
        _apply(books, last_subscription, events[index].change)


def _apply(
    books: dict[str, Book], last_subscription: dict[str, tuple[int, int]], change: _Change
) -> None:
    if isinstance(change, _Gap):
        for ticker, subscription in last_subscription.items():
            if subscription == (change.conn_id, change.sid):
                books[ticker].mark_stale()
        return
    book = books.get(change.ticker)
    if book is None:
        book = books[change.ticker] = Book(change.ticker)
    last_subscription[change.ticker] = (change.conn_id, change.sid)
    try:
        if isinstance(change, _Delta):
            book.apply_delta(change.side, change.price, change.delta, ts_ms=change.ts_ms)
        else:
            book.apply_snapshot(change.bids, change.asks, ts_ms=None)
    except BookInvariantError:
        # The book marked itself stale, as the recorder's does, and waits for a snapshot.
        return


def _delta_events(table: pa.Table) -> list[_Stamped]:
    if table.num_rows == 0:
        return []
    column: dict[str, list[Any]] = {name: table[name].to_pylist() for name in _DELTA_COLUMNS}
    events: list[_Stamped] = []
    for index in range(table.num_rows):
        ts_ms = column["ts_ms"][index]
        change = _Delta(
            ticker=column["ticker"][index],
            conn_id=column["conn_id"][index],
            sid=column["sid"][index],
            side=Side(column["side"][index]),
            price=PriceE4(column["price_e4"][index]),
            delta=column["delta_e2"][index],
            ts_ms=None if ts_ms is None else Ms(ts_ms),
        )
        events.append(
            _Stamped(
                recv_wall_ns=column["recv_wall_ns"][index],
                recv_mono_ns=column["recv_mono_ns"][index],
                order=_CHANGE_ORDER,
                change=change,
            )
        )
    return events


def _snapshot_events(table: pa.Table) -> list[_Stamped]:
    """One event per snapshot, from the rows that share its receipt and sequence."""
    if table.num_rows == 0:
        return []
    keys = ("ticker", "recv_wall_ns", "conn_id", "sid", "seq")
    ordered = table.sort_by([(name, "ascending") for name in keys])
    column: dict[str, list[Any]] = {name: ordered[name].to_pylist() for name in _SNAPSHOT_COLUMNS}
    events: list[_Stamped] = []
    start = 0
    for index in range(1, ordered.num_rows + 1):
        if index < ordered.num_rows and all(
            column[name][index] == column[name][start] for name in keys
        ):
            continue
        levels = range(start, index)
        change = _Snapshot(
            ticker=column["ticker"][start],
            conn_id=column["conn_id"][start],
            sid=column["sid"][start],
            bids=_levels(column, levels, Side.BID),
            asks=_levels(column, levels, Side.ASK),
        )
        events.append(
            _Stamped(
                recv_wall_ns=column["recv_wall_ns"][start],
                recv_mono_ns=column["recv_mono_ns"][start],
                order=_CHANGE_ORDER,
                change=change,
            )
        )
        start = index
    return events


def _levels(column: dict[str, list[Any]], rows: range, side: Side) -> tuple[Level, ...]:
    return tuple(
        Level(PriceE4(column["price_e4"][row]), CountE2(column["count_e2"][row]))
        for row in rows
        if column["side"][row] == int(side)
    )


def _gap_events(table: pa.Table) -> list[_Stamped]:
    """One event per gap, stamped with the receipt of the frame that revealed it."""
    if table.num_rows == 0:
        return []
    column: dict[str, list[Any]] = {name: table[name].to_pylist() for name in _GAP_COLUMNS}
    return [
        _Stamped(
            recv_wall_ns=column["recv_wall_ns"][index],
            recv_mono_ns=column["recv_mono_ns"][index],
            order=_GAP_ORDER,
            change=_Gap(conn_id=column["conn_id"][index], sid=column["sid"][index]),
        )
        for index in range(table.num_rows)
    ]
