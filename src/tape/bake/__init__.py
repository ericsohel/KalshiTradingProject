"""Bake raw segments into Parquet tables, write daily manifests, and prune raw data safely.

``tape.bake.bake`` bakes one closed hour and records it in the day's manifest; the rows come from
``tape.bake.interpret`` and are written in bounded memory by ``tape.bake.spill`` with the tables of
``tape.bake.tables``. ``tape.bake.manifest`` defines the manifest, and ``tape.bake.prune`` deletes
raw segments only from hours ADR 0025 allows (docs/DATA_FORMATS.md 6 and 7, docs/INTERFACES.md 10).
"""

from tape.bake.bake import (
    BAKE_VERSION,
    DEFAULT_MAX_PART_ROWS,
    BakeReport,
    archive_lock,
    bake_hour,
    bake_needed,
    hours_to_bake,
    record_bake,
)
from tape.bake.layout import DataLayout, HourKey, closed_hours
from tape.bake.manifest import Manifest, read_manifest
from tape.bake.prune import PruneDecision, PruneReason, apply, decide, survey
from tape.bake.tables import TABLE_NAMES, TABLES, TableName

__all__ = [
    "BAKE_VERSION",
    "DEFAULT_MAX_PART_ROWS",
    "TABLES",
    "TABLE_NAMES",
    "BakeReport",
    "DataLayout",
    "HourKey",
    "Manifest",
    "PruneDecision",
    "PruneReason",
    "TableName",
    "apply",
    "archive_lock",
    "bake_hour",
    "bake_needed",
    "closed_hours",
    "decide",
    "hours_to_bake",
    "read_manifest",
    "record_bake",
    "survey",
]
