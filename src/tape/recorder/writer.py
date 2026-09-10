"""The writer thread that owns one connection's segment files.

Responsibility: accept records from the asyncio side without ever blocking it, and write
them into raw segments laid out as docs/DATA_FORMATS.md 4 describes, so that the event
loop never touches disk (docs/INTERFACES.md 7, docs/ARCHITECTURE.md 7.1). Records go to
``<root>/raw/YYYY-MM-DD/HH/conn-<NN>-<UUUU>.tape.zst``, placed by the injected clock's UTC
wall time; a file is closed when the hour changes or when :meth:`SegmentSink.rotate` is
called, and the next record opens the next one.

Invariants: :meth:`SegmentSink.put` never blocks and never raises; records reach disk in
the order ``put`` accepted them; every refused record is counted, and the count reaches
the tape as a ``CONNECTION`` record the next time the queue has room, so a hole is visible
where it happened; a rotation falls between exactly the records it was requested between;
an existing file is never opened for writing; a written file is flushed at least once per
``flush_interval_ns`` even when nothing else arrives; and only the writer thread touches a
``SegmentWriter``.
"""

from __future__ import annotations

import contextlib
import logging
import queue
import threading
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Final, Protocol, Self

import msgspec

from tape.segment import Record, RecordKind, SegmentHeader, SegmentWriter
from tape.timeutil import NS_PER_MS, NS_PER_S, Clock, wall_ns_to_datetime

__all__ = [
    "DEFAULT_CLOSE_TIMEOUT_NS",
    "DEFAULT_FLUSH_INTERVAL_NS",
    "DEFAULT_MAX_QUEUED_RECORDS",
    "DEFAULT_POLL_INTERVAL_NS",
    "MAX_SEGMENTS_PER_HOUR",
    "OVERFLOW_EVENT",
    "HeaderFactory",
    "RecordSink",
    "SegmentSink",
    "SinkStats",
    "segment_path",
]

DEFAULT_MAX_QUEUED_RECORDS: Final = 200_000
"""Records held for the writer thread; ``writer_queue_max`` in docs/INTERFACES.md 17."""

DEFAULT_FLUSH_INTERVAL_NS: Final = NS_PER_S
"""A crash loses at most this much tape (docs/DATA_FORMATS.md 4)."""

DEFAULT_POLL_INTERVAL_NS: Final = 100 * NS_PER_MS
"""Longest the thread waits on an empty queue before checking the flush and hour clocks."""

DEFAULT_CLOSE_TIMEOUT_NS: Final = 30 * NS_PER_S
"""Deadline for draining the queue and joining the thread on :meth:`SegmentSink.close`."""

MAX_SEGMENTS_PER_HOUR: Final = 10_000
"""The ``UUUU`` counter has four digits; one connection cannot open more files in an hour."""

OVERFLOW_EVENT: Final = "writer_overflow"
"""``event`` of the ``CONNECTION`` record that reports records refused by a full queue."""

_NS_PER_HOUR: Final = 3_600 * NS_PER_S

_CONTROL_SLOTS: Final = 4
"""Queue capacity reserved beyond the record bound for markers.

Records are accepted only while the queue holds fewer than ``max_queued_records`` items,
and one accepted ``put`` adds at most three (a pending overflow notice, a pending
rotation, the record), so the queue never exceeds the bound by more than two; ``close``
adds at most a notice and a wake-up on top. Four slots therefore make every marker
``put_nowait`` on the producer side infallible.
"""

HeaderFactory = Callable[[], SegmentHeader]
"""Builds the header for a new segment. Called on the writer thread; must be thread-safe."""


class SinkStats(msgspec.Struct, frozen=True, kw_only=True):
    """Counters of one sink. Exact after :meth:`SegmentSink.close`; a moment's view before.

    Attributes:
        records_written: Records appended to a segment, overflow notices included.
        records_dropped: Records that never reached a segment: refused by a full queue,
            put after close, or lost to a write error.
        files_opened: Segment files created.
        bytes_written: Uncompressed bytes appended across every file, headers included.
        flushes: Periodic flushes performed (closing a file flushes it too, uncounted).
        write_errors: Disk errors met while opening, writing, flushing, or closing a file.
    """

    records_written: int
    records_dropped: int
    files_opened: int
    bytes_written: int
    flushes: int
    write_errors: int


class _Rotate:
    """Marker: close the current file; the next record opens a new one."""


class _Wake:
    """Marker: end a blocking ``get`` so the thread notices it is stopping."""


_ROTATE: Final = _Rotate()
_WAKE: Final = _Wake()


def segment_path(root: Path, conn_id: int, wall_ns: int, counter: int) -> Path:
    """Return where a segment belongs (docs/DATA_FORMATS.md 4).

    Args:
        root: Data directory; segments live under ``root / "raw"``.
        conn_id: Connection the segment records, rendered as at least two digits.
        wall_ns: Wall-clock nanoseconds since the Unix epoch; its UTC date and hour
            choose the directory.
        counter: Per-hour segment counter, rendered as four digits.

    Returns:
        ``root/raw/YYYY-MM-DD/HH/conn-<NN>-<UUUU>.tape.zst``.

    Raises:
        ValueError: If ``conn_id`` or ``wall_ns`` is negative, or ``counter`` is outside
            ``[0, MAX_SEGMENTS_PER_HOUR)``.
    """
    if conn_id < 0:
        raise ValueError(f"conn_id must be non-negative, got {conn_id}")
    if wall_ns < 0:
        raise ValueError(f"wall_ns must be non-negative, got {wall_ns}")
    if not 0 <= counter < MAX_SEGMENTS_PER_HOUR:
        raise ValueError(f"segment counter must be in [0, {MAX_SEGMENTS_PER_HOUR}), got {counter}")
    moment = wall_ns_to_datetime(wall_ns)
    name = f"conn-{conn_id:02d}-{counter:04d}.tape.zst"
    return root / "raw" / f"{moment:%Y-%m-%d}" / f"{moment:%H}" / name


class RecordSink(Protocol):
    """Where a component writes tape records; :class:`SegmentSink` is the real one.

    Components that only emit records, such as the auditor, depend on this port rather
    than on the threaded writer, so tests can pass a plain in-memory fake.
    """

    @property
    def conn_id(self) -> int:
        """Connection whose segment the records land in; stamped on each record."""
        ...

    def put(self, record: Record) -> bool:
        """Queue one record without blocking; ``False`` means it was refused."""
        ...


class SegmentSink:
    """Bounded, non-blocking hand-off from the event loop to one writer thread.

    The asyncio side calls :meth:`put` and :meth:`rotate`; the thread started by
    :meth:`start` owns every ``SegmentWriter``. Files are opened lazily by the first
    record after a rotation or an hour change, so an idle connection leaves no empty
    files, and a file is closed as soon as its hour ends, even while idle, so the baker
    can take it.

    A disk error does not stop the thread: the record is counted as dropped, the file is
    abandoned, and the next record tries a new file. Any other exception is a bug; it is
    logged, kept in :attr:`failure`, and ends the thread, after which ``put`` refuses
    every record.

    Args:
        root: Data directory; segments go under ``root / "raw"``.
        conn_id: Connection id for file names and for the overflow notices this sink
            writes itself.
        header_factory: Builds the header of each new file. It runs on the writer
            thread, so it must read only immutable snapshots of the caller's state.
        clock: Places files by UTC wall time and paces flushes by monotonic time.
        max_queued_records: Records held for the thread before ``put`` refuses more.
        flush_interval_ns: Longest a written record may wait before a flush.
        poll_interval_ns: Longest the thread blocks on an empty queue.
        close_timeout_ns: Deadline for :meth:`close` to drain and join the thread.
        level: zstd compression level.
        logger: Destination for error logs; defaults to this module's logger.

    Raises:
        ValueError: If a bound or interval is not positive, or ``conn_id`` is negative.
    """

    def __init__(
        self,
        root: Path,
        *,
        conn_id: int,
        header_factory: HeaderFactory,
        clock: Clock,
        max_queued_records: int = DEFAULT_MAX_QUEUED_RECORDS,
        flush_interval_ns: int = DEFAULT_FLUSH_INTERVAL_NS,
        poll_interval_ns: int = DEFAULT_POLL_INTERVAL_NS,
        close_timeout_ns: int = DEFAULT_CLOSE_TIMEOUT_NS,
        level: int = 3,
        logger: logging.Logger | None = None,
    ) -> None:
        for name, value in (
            ("max_queued_records", max_queued_records),
            ("flush_interval_ns", flush_interval_ns),
            ("poll_interval_ns", poll_interval_ns),
            ("close_timeout_ns", close_timeout_ns),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive, got {value}")
        if conn_id < 0:
            raise ValueError(f"conn_id must be non-negative, got {conn_id}")
        self._root = root
        self._conn_id = conn_id
        self._header_factory = header_factory
        self._clock = clock
        self._max_records = max_queued_records
        self._flush_interval_ns = flush_interval_ns
        self._poll_s = poll_interval_ns / NS_PER_S
        self._close_timeout_s = close_timeout_ns / NS_PER_S
        self._level = level
        self._log = logger if logger is not None else logging.getLogger(__name__)
        self._queue: queue.Queue[Record | _Rotate | _Wake] = queue.Queue(
            maxsize=max_queued_records + _CONTROL_SLOTS
        )
        self._thread = threading.Thread(
            target=self._run, name=f"segment-sink-{conn_id}", daemon=False
        )
        self._stopping = threading.Event()
        # Producer-side state, guarded by the lock so ``put`` and ``rotate`` may be called
        # from any thread.
        self._lock = threading.Lock()
        self._started = False
        self._closed = False
        self._refused = 0
        self._unreported_refusals = 0
        self._rotate_pending = False
        self._last_queued_rotate = False
        # Writer-thread state. Other threads only read the counters.
        self._writer: SegmentWriter | None = None
        self._writer_hour = -1
        self._counter_hour = -1
        self._next_counter = 0
        self._dirty = False
        self._last_flush_ns = int(clock.mono_ns())
        self._records_written = 0
        self._records_lost = 0
        self._files_opened = 0
        self._closed_file_bytes = 0
        self._flushes = 0
        self._write_errors = 0
        self._finished = False
        self._failure: Exception | None = None

    # ------------------------------------------------------------------ producer side

    @property
    def stats(self) -> SinkStats:
        """Current counters; see :class:`SinkStats`."""
        writer = self._writer
        open_bytes = 0 if writer is None else writer.bytes_written
        return SinkStats(
            records_written=self._records_written,
            records_dropped=self._refused + self._records_lost,
            files_opened=self._files_opened,
            bytes_written=self._closed_file_bytes + open_bytes,
            flushes=self._flushes,
            write_errors=self._write_errors,
        )

    @property
    def conn_id(self) -> int:
        """Connection this sink records; its segment files are named after it."""
        return self._conn_id

    @property
    def failure(self) -> Exception | None:
        """The unexpected exception that ended the writer thread, if one did."""
        return self._failure

    def start(self) -> None:
        """Start the writer thread. Records put before this are kept in order.

        Raises:
            RuntimeError: If the sink was already started or closed.
        """
        with self._lock:
            if self._started or self._closed:
                raise RuntimeError(f"segment sink {self._conn_id} cannot be started twice")
            self._started = True
        self._thread.start()

    def put(self, record: Record) -> bool:
        """Queue one record for the writer thread without blocking.

        Thread-safe. When the queue is full the record is refused and counted, and the
        next accepted record is preceded by a ``CONNECTION`` record carrying
        ``{"event": "writer_overflow", "dropped": N}``.

        Args:
            record: The record to write.

        Returns:
            ``True`` if the record was queued; ``False`` if it was refused because the
            queue is full, the sink is closed, or the writer thread has failed.
        """
        with self._lock:
            if self._closed or self._failure is not None:
                self._refused += 1
                return False
            if self._queue.qsize() >= self._max_records:
                self._refused += 1
                self._unreported_refusals += 1
                return False
            self._queue_overflow_notice()
            if self._rotate_pending:
                self._rotate_pending = False
                self._queue.put_nowait(_ROTATE)
            self._queue.put_nowait(record)
            self._last_queued_rotate = False
            return True

    def rotate(self) -> None:
        """Close the current file after every record already put; later records open a new one.

        Thread-safe and non-blocking. Consecutive rotations with no record between them
        are one rotation. If the queue is full, the rotation is queued ahead of the next
        accepted record instead, so refusals from both sides of it are reported before it.
        Ignored after :meth:`close`.
        """
        with self._lock:
            if self._closed:
                return
            if self._queue.qsize() >= self._max_records:
                self._rotate_pending = True
                return
            self._queue_overflow_notice()
            if not self._last_queued_rotate:
                self._queue.put_nowait(_ROTATE)
                self._last_queued_rotate = True

    def close(self) -> None:
        """Drain the queue, flush and close the current file, and join the thread.

        Idempotent. A sink that was never started is drained on the calling thread.

        Raises:
            TimeoutError: If the thread has not finished within ``close_timeout_ns``,
                for example because the disk has stopped responding.
        """
        with self._lock:
            if not self._closed:
                self._closed = True
                if self._unreported_refusals and not self._queue.full():
                    self._queue_overflow_notice()
            started = self._started
        self._stopping.set()
        # A full queue needs no wake-up: the thread is busy draining and sees the flag.
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(_WAKE)
        if not started:
            if not self._finished:
                self._run()
            return
        self._thread.join(self._close_timeout_s)
        if self._thread.is_alive():
            raise TimeoutError(
                f"segment sink {self._conn_id} did not finish within {self._close_timeout_s} s"
            )

    def __enter__(self) -> Self:
        """Start the writer thread and return the sink."""
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the sink, whatever happened inside the block."""
        self.close()

    def _queue_overflow_notice(self) -> None:
        """Queue the report of refused records, if any. The lock must be held."""
        if not self._unreported_refusals:
            return
        payload = msgspec.json.encode(
            {"event": OVERFLOW_EVENT, "dropped": self._unreported_refusals}
        )
        self._unreported_refusals = 0
        self._queue.put_nowait(
            Record(
                kind=RecordKind.CONNECTION,
                conn_id=self._conn_id,
                recv_mono_ns=int(self._clock.mono_ns()),
                recv_wall_ns=int(self._clock.wall_ns()),
                payload=payload,
            )
        )
        self._last_queued_rotate = False

    # -------------------------------------------------------------------- writer side

    def _run(self) -> None:
        """Write queued items until stopped and drained, then close the current file."""
        try:
            while True:
                stopping = self._stopping.is_set()
                try:
                    if stopping:
                        item = self._queue.get_nowait()
                    else:
                        item = self._queue.get(timeout=self._poll_s)
                except queue.Empty:
                    if stopping:
                        break
                    self._tick()
                    continue
                if isinstance(item, Record):
                    self._write(item)
                elif isinstance(item, _Rotate):
                    self._close_writer()
                self._tick()
        except Exception as exc:
            self._failure = exc
            self._log.exception(
                "segment sink thread failed", extra={"conn_id": self._conn_id, "error": repr(exc)}
            )
        finally:
            self._close_writer()
            self._finished = True

    def _tick(self) -> None:
        """Close a file whose hour has ended and flush one that has waited long enough."""
        if self._writer is not None and self._hour(self._clock.wall_ns()) != self._writer_hour:
            self._close_writer()
        if self._dirty and self._clock.mono_ns() - self._last_flush_ns >= self._flush_interval_ns:
            self._flush()

    def _write(self, record: Record) -> None:
        """Append one record, opening a file first if none is open for the current hour."""
        try:
            writer = self._writer_for(int(self._clock.wall_ns()))
            writer.append(record)
        except OSError as exc:
            self._records_lost += 1
            self._disk_error("segment write failed", exc)
            self._close_writer()
            return
        except ValueError as exc:
            # Only an oversized payload reaches here; the file itself is still sound.
            self._records_lost += 1
            self._log.error(
                "segment record refused",
                extra={"conn_id": self._conn_id, "error": repr(exc), "bytes": len(record.payload)},
            )
            return
        self._records_written += 1
        self._dirty = True

    def _writer_for(self, wall_ns: int) -> SegmentWriter:
        """Return the open writer for the hour of ``wall_ns``, rotating or opening as needed.

        Raises:
            OSError: If the directory or file cannot be created.
        """
        hour = self._hour(wall_ns)
        if self._writer is not None and self._writer_hour != hour:
            self._close_writer()
        if self._writer is None:
            self._writer = self._open(wall_ns, hour)
            self._writer_hour = hour
        return self._writer

    def _open(self, wall_ns: int, hour: int) -> SegmentWriter:
        """Claim the next unused file name for this hour and open a writer on it.

        The name is claimed with an exclusive create, so an existing file, whether from
        an earlier run or another process, is skipped rather than truncated.

        Raises:
            OSError: If the directory cannot be created, or every counter for the hour
                is taken.
        """
        if hour != self._counter_hour:
            self._counter_hour = hour
            self._next_counter = 0
        for counter in range(self._next_counter, MAX_SEGMENTS_PER_HOUR):
            path = segment_path(self._root, self._conn_id, wall_ns, counter)
            path.parent.mkdir(parents=True, exist_ok=True)
            try:
                path.touch(exist_ok=False)
            except FileExistsError:
                continue
            self._next_counter = counter + 1
            writer = SegmentWriter(path, self._header_factory(), level=self._level)
            self._files_opened += 1
            self._last_flush_ns = int(self._clock.mono_ns())
            return writer
        self._next_counter = MAX_SEGMENTS_PER_HOUR
        raise OSError(f"every segment counter is taken for conn {self._conn_id} this hour")

    def _flush(self) -> None:
        """Flush the open writer; a failing flush abandons the file."""
        writer = self._writer
        self._dirty = False
        self._last_flush_ns = int(self._clock.mono_ns())
        if writer is None:
            return
        try:
            writer.flush()
        except OSError as exc:
            self._disk_error("segment flush failed", exc)
            self._close_writer()
            return
        self._flushes += 1

    def _close_writer(self) -> None:
        """Finish and close the open file, if any. Never raises for a disk error."""
        writer, self._writer = self._writer, None
        self._dirty = False
        if writer is None:
            return
        try:
            writer.close()
        except OSError as exc:
            self._disk_error("segment close failed", exc)
        finally:
            self._closed_file_bytes += writer.bytes_written

    def _disk_error(self, event: str, exc: OSError) -> None:
        self._write_errors += 1
        self._log.error(event, extra={"conn_id": self._conn_id, "error": repr(exc)})

    @staticmethod
    def _hour(wall_ns: int) -> int:
        return wall_ns // _NS_PER_HOUR
