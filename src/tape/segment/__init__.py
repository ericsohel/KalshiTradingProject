"""Raw tape segments and keyframe files (docs/DATA_FORMATS.md 4 and 5, ADR 0001)."""

from tape.segment.keyframe import KEYFRAME_SCHEMA, read_keyframe, write_keyframe
from tape.segment.segment import (
    FORMAT_VERSION,
    MAGIC,
    Record,
    RecordKind,
    SegmentHeader,
    SegmentReader,
    SegmentWriter,
    SubscriptionInfo,
)

__all__ = [
    "FORMAT_VERSION",
    "KEYFRAME_SCHEMA",
    "MAGIC",
    "Record",
    "RecordKind",
    "SegmentHeader",
    "SegmentReader",
    "SegmentWriter",
    "SubscriptionInfo",
    "read_keyframe",
    "write_keyframe",
]
