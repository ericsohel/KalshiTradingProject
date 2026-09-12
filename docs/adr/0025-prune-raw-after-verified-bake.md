# 0025. Raw segments are pruned only after a verified bake and a retention window

Status: accepted. Date: 2026-09-12. Amends ADR 0001 (raw retention) and the storage
policy in OPERATIONS 5.

## Context

ADR 0001 keeps raw frames as the source of truth, so a decoder change never requires
re-recording. The storage policy in OPERATIONS 5 kept raw segments of showcase markets
forever and others for 90 days, and assumed a 200 GB disk.

The production host (ADR 0024) has a 64 GB data disk. Over its first hours it wrote about
5.2 GB of raw data a day at 200 markets, on a Saturday with live sports, which fills the
disk in about 11 days. No free storage holds months of raw data, and the owner's Mac is
not always on. Baked columnar tables should be far smaller than raw JSON frames, because
of dictionary-encoded strings and typed integers; that is to be measured, not assumed.

## Alternatives considered

1. **Keep raw forever and grow the disk.** Rejected: premium storage beyond the free
   allowance draws on the student credit every month.
2. **Record fewer markets, or compress harder.** Rejected as the fix: fewer markets defeats
   the purpose, and a much higher zstd level competes with capture for two vCPUs. The
   compression level may still be tuned later.
3. **Ship raw off the host.** Object-storage free tiers of 5 to 10 GB are far too small,
   and the Mac is intermittent. Kept as a complement, not a dependency.
4. **Delete raw by age alone.** Simple, but a failed or buggy bake would destroy data that
   has no typed copy. Rejected.
5. **Prune an hour only after a verified bake, and only after a retention window.** Chosen.

## Decision

- **Bake** works hour by hour on closed hours: every segment in `raw/YYYY-MM-DD/HH/`, once
  the hour ended more than a grace period ago, so no file in it is still being written.
- **A raw hour is prunable only when all of these hold:**
  1. It ended more than `raw_retention_hours` ago; the production default is 72.
  2. A bake of that hour completed. Its manifest lists every segment file of the hour with
     byte size and sha256, and the files on disk still match.
  3. The bake accounted for every record: each one either became table rows or was counted
     as intentionally not baked (commands, subscription replies, errors), with zero
     decode failures.
  4. Every baked file the manifest lists exists and matches its recorded sha256.
- **`tape prune` deletes nothing without `--apply`.** Without it, it reports what it would
  delete and why each remaining hour is not yet prunable.
- **The manifest records each pruned segment** (path, bytes, sha256, time pruned), so the
  record of what was captured outlives the files.
- **Keyframes, baked tables, and manifests are kept.** The showcase exception does not apply
  to the production host, whose disk cannot hold it; a longer raw archive belongs on a
  larger disk, such as the owner's Mac.

## Consequences

- **Bugs found late.** A decoder bug found more than the retention window after capture
  can no longer be repaired by re-baking pruned hours. The window and the record-level
  reconciliation are what make that risk acceptable.
- **Baked tables are the long-term record,** so the schema evolution rules in
  DATA_FORMATS 9 now carry more weight.
- **Disk use** becomes about the retention window of raw data, plus the baked tables and
  keyframes.
- **Metadata tables.** `markets`, `series`, and `fee_changes` cannot be baked from raw
  segments, which hold only WebSocket traffic. They wait for a recorder change that tapes
  REST metadata.

## What would reverse it

- **Cheap storage for raw data:** written consent for a funded archive, or an always-on
  host with a large disk.
- **Re-baking pruned history:** a decoder change that must be applied to hours already
  pruned.
- **Baked tables not much smaller than raw:** pruning would then buy only the window, and
  the recorded universe would have to shrink.
