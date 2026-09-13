"""Raw segment format, version 1 (docs/DATA_FORMATS.md 4).

A segment is a zstd stream whose decompressed content is a header record followed by
data records, all little-endian::

    header:  magic "TAPE" | u16 version | u32 hdr_len | JSON header
    record:  u8 kind | u16 conn_id | u64 recv_mono_ns | u64 recv_wall_ns | u32 len | payload

Writers append only and flush per block so a crash loses at most the unflushed tail.
Readers tolerate a truncated tail (the last partial record or an unfinished zstd
frame) and expose it through ``SegmentReader.truncated``; any other malformation is a
``TapeCorruptionError``.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from enum import IntEnum
from pathlib import Path
from types import TracebackType
from typing import IO, Final, Self

import msgspec
import zstandard

from tape.errors import TapeCorruptionError

__all__ = [
    "FORMAT_VERSION",
    "MAGIC",
    "Record",
    "RecordKind",
    "SegmentHeader",
    "SegmentReader",
    "SegmentWriter",
    "SubscriptionInfo",
]

MAGIC: Final = b"TAPE"
FORMAT_VERSION: Final = 1
_FILE_HEADER: Final = struct.Struct("<4sHI")
_RECORD_HEADER: Final = struct.Struct("<BHQQI")
_MAX_PAYLOAD: Final = (1 << 32) - 1
_READ_CHUNK: Final = 1 << 16
"""Compressed bytes decompressed at once. Raw JSON compresses about twelve times, so a larger chunk
means a transient buffer of megabytes per read, which the allocator keeps: on a busy hour 1 MiB
chunks held about 190 MB, and 64 KiB chunks 38 MB, at the same speed."""
_DEFAULT_LEVEL: Final = 3


class RecordKind(IntEnum):
    """What a data record's payload contains."""

    FRAME = 1
    COMMAND = 2
    GAP = 3
    CONNECTION = 4
    AUDIT = 5


class SubscriptionInfo(msgspec.Struct, frozen=True, kw_only=True):
    """One subscription active on the connection when the segment was opened."""

    sid: int
    channel: str
    group_id: str


class SegmentHeader(msgspec.Struct, frozen=True, kw_only=True):
    """JSON header written once at the start of every segment."""

    created_wall_ns: int
    host: str
    env: str
    conn_id: int
    ws_url: str
    use_yes_price: bool
    subscriptions: list[SubscriptionInfo]
    software_version: str
    spec_versions: dict[str, str]


class Record(msgspec.Struct, frozen=True, kw_only=True):
    """One data record: a raw frame, an outbound command, or an annotation."""

    kind: RecordKind
    conn_id: int
    recv_mono_ns: int
    recv_wall_ns: int
    payload: bytes


_header_encoder = msgspec.json.Encoder()
_header_decoder = msgspec.json.Decoder(SegmentHeader)


class SegmentWriter:
    """Append-only writer for one segment file.

    Args:
        path: File to create. Parent directories must exist. An existing file is
            truncated, because segments are never appended to across processes.
        header: Metadata written first.
        level: zstd compression level (3 is fast; cold data may be recompressed later).
    """

    def __init__(self, path: Path, header: SegmentHeader, *, level: int = _DEFAULT_LEVEL) -> None:
        self._path = path
        self._fh: IO[bytes] = path.open("wb")
        self._writer = zstandard.ZstdCompressor(level=level).stream_writer(self._fh, closefd=False)
        self._closed = False
        self.records_written = 0
        self.bytes_written = 0
        header_json = _header_encoder.encode(header)
        self._write(_FILE_HEADER.pack(MAGIC, FORMAT_VERSION, len(header_json)))
        self._write(header_json)

    @property
    def path(self) -> Path:
        """Location of the segment file."""
        return self._path

    def append(self, record: Record) -> None:
        """Append one record.

        Raises:
            ValueError: If the payload exceeds 2**32 - 1 bytes or the writer is closed.
        """
        if self._closed:
            raise ValueError("segment writer is closed")
        if len(record.payload) > _MAX_PAYLOAD:
            raise ValueError("payload too large for segment record")
        self._write(
            _RECORD_HEADER.pack(
                int(record.kind),
                record.conn_id,
                record.recv_mono_ns,
                record.recv_wall_ns,
                len(record.payload),
            )
        )
        self._write(record.payload)
        self.records_written += 1

    def flush(self) -> None:
        """Flush a complete zstd block and the OS buffer so a crash keeps what is written."""
        if self._closed:
            return
        self._writer.flush(zstandard.FLUSH_BLOCK)
        self._fh.flush()

    def close(self) -> None:
        """Finish the zstd frame and close the file. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._writer.close()  # type: ignore[no-untyped-call]  # zstandard stub gap
        self._fh.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _write(self, data: bytes) -> None:
        self._writer.write(data)
        self.bytes_written += len(data)


class SegmentReader:
    """Streaming reader for one segment file.

    The header is parsed eagerly. Records are yielded lazily by ``records()``. After
    iteration, ``truncated`` tells whether the file ended mid-record or mid-frame, as a
    crash leaves it, and ``damaged`` whether decompression failed or bytes followed the
    end of the frame, which a writer never produces. Reading stops at either; the records
    yielded before it remain valid.

    Raises:
        TapeCorruptionError: On a bad magic, unsupported version, or malformed header.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh: IO[bytes] = path.open("rb")
        self._dobj = zstandard.ZstdDecompressor().decompressobj()
        self._buffer = bytearray()
        self._eof = False
        self.truncated = False
        self.damaged = False
        self.header = self._read_header()

    @property
    def path(self) -> Path:
        """Location of the segment file."""
        return self._path

    def records(self) -> Iterator[Record]:
        """Yield records in file order, stopping cleanly at a truncated tail."""
        while True:
            if not self._ensure(_RECORD_HEADER.size):
                self.truncated = self.truncated or len(self._buffer) > 0
                return
            kind, conn_id, mono, wall, length = _RECORD_HEADER.unpack_from(self._buffer, 0)
            if not self._ensure(_RECORD_HEADER.size + length):
                self.truncated = True
                return
            start = _RECORD_HEADER.size
            payload = bytes(self._buffer[start : start + length])
            del self._buffer[: start + length]
            try:
                record_kind = RecordKind(kind)
            except ValueError as exc:
                raise TapeCorruptionError(f"{self._path}: unknown record kind {kind}") from exc
            yield Record(
                kind=record_kind,
                conn_id=conn_id,
                recv_mono_ns=mono,
                recv_wall_ns=wall,
                payload=payload,
            )

    def close(self) -> None:
        """Close the underlying file. Idempotent."""
        self._fh.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _fill(self) -> bool:
        """Decompress one chunk of input into the buffer. Returns False at end of input.

        An unfinished zstd frame at end of file (the writer never closed) marks the
        reader as truncated. A decompression error, or input left over after the frame
        ended, marks it as damaged and ends the input. Either way the records decoded
        before that point remain valid.
        """
        if self._eof:
            return False
        raw = self._fh.read(_READ_CHUNK)
        if not raw:
            self._eof = True
            if not self._dobj.eof:
                self.truncated = True
            return False
        try:
            chunk = self._dobj.decompress(raw)
        except zstandard.ZstdError:
            self._eof = True
            self.damaged = True
            return False
        if self._dobj.eof and self._dobj.unused_data:
            self._eof = True
            self.damaged = True
        if chunk:
            self._buffer.extend(chunk)
            return True
        return self._fill()

    def _ensure(self, needed: int) -> bool:
        """Fill until the buffer holds ``needed`` bytes; False if input ends first."""
        while len(self._buffer) < needed:
            if not self._fill():
                return False
        return True

    def _read_header(self) -> SegmentHeader:
        if not self._ensure(_FILE_HEADER.size):
            raise TapeCorruptionError(f"{self._path}: file shorter than the header")
        magic, version, hdr_len = _FILE_HEADER.unpack_from(self._buffer, 0)
        if magic != MAGIC:
            raise TapeCorruptionError(f"{self._path}: bad magic {magic!r}")
        if version != FORMAT_VERSION:
            raise TapeCorruptionError(f"{self._path}: unsupported segment version {version}")
        if not self._ensure(_FILE_HEADER.size + hdr_len):
            raise TapeCorruptionError(f"{self._path}: truncated header")
        raw = bytes(self._buffer[_FILE_HEADER.size : _FILE_HEADER.size + hdr_len])
        del self._buffer[: _FILE_HEADER.size + hdr_len]
        try:
            return _header_decoder.decode(raw)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            raise TapeCorruptionError(f"{self._path}: malformed header: {exc}") from exc
