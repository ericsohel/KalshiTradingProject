# Operations

## 1. Environments

| Name | Host | Kalshi env | Purpose |
|---|---|---|---|
| dev | Owner's Mac | demo for order paths, prod public data for capture | development, first recordings |
| prod | Oracle Cloud Always Free ARM (2 OCPU, 12 GB, 200 GB), US region | prod | continuous recording, API |

The first production tape starts on the Mac the day a production read key exists;
the process moves to the cloud host once it has run cleanly for a week.

## 2. Secrets

- API private keys live in `~/.config/tape/keys/` (mode 600) on the host that uses
  them, referenced by path in `tape.toml`. They are never in the repository,
  environment variables, logs, or the API.
- The recorder host holds only the read key. The `write::trade` key exists only on the
  host that runs the engine or probe, and only while those run.
- Rotating a key: create the new key in Kalshi's settings, place the file, update the
  path, restart the process, delete the old key in Kalshi.

## 3. Provisioning the production host

1. Create the Always Free ARM instance (Ubuntu 24.04 aarch64) in a US home region;
   open only port 443 in the security list; SSH by key.
2. Install `chrony`, `zstd`, `caddy`, and `uv`; create user `tape` with no sudo.
3. `git clone`, `uv sync --frozen`, place the key, write `tape.toml`.
4. Install the systemd units from `deploy/systemd/` (`tape-record`, `tape-serve`,
   `tape-bake.timer` hourly, `tape-backup.timer` nightly) with `Restart=always`,
   `MemoryMax` set per process, and journald logging.
5. Point a DuckDNS (or owned) hostname at the instance; install `deploy/Caddyfile`;
   confirm the certificate is issued.
6. Register the dead-man ping (healthchecks.io) and the alert channel (ntfy topic).

## 4. Monitoring

- `tape record` exposes Prometheus metrics on localhost: messages per second per
  channel and connection, gap count, resync count, reconnects, writer queue depth,
  frames dropped, disk free, audit mismatches, last-frame age.
- A dead-man ping fires every minute while the control connection is open; a missed
  ping alerts within three minutes.
- Alerts: recorder down, disk under 10% free, gap share over 1% in the last hour,
  audit exact ratio under 99% in the last hour, certificate expiry within 14 days.

## 5. Storage and retention

Measured in week one and revisited monthly. Policy from day one:

| Data | Retention on host | Backup |
|---|---|---|
| Raw segments for showcase and quoted markets | forever (compressed) | R2 nightly while under the free tier |
| Raw segments for other markets | 90 days, then keep baked tables only | none |
| Keyframes | forever | R2 nightly |
| Baked tables and manifests | forever | R2 nightly |

`tape bake --prune` applies the policy and records what it removed in the manifest.

## 6. Runbook

| Symptom | Check | Action |
|---|---|---|
| Dead-man alert | `systemctl status tape-record`, journal tail | Restart if crashed; if a Kalshi outage, wait; the gap is visible in the manifest |
| Repeated reconnects | `GET /exchange/status`, changelog RSS | If Kalshi maintenance (Thursdays 3 to 5 AM ET) do nothing; else inspect error frames in the raw segment |
| Gap share rising | per-connection message rate | Add a connection; shrink group size |
| Audit mismatches on one group | affected `sid` | Force `get_snapshot`; if persistent, open an issue with the raw frames |
| Disk filling | `df`, manifest sizes | Run `tape bake --prune`; tighten the L2 universe |
| API slow | client count, outbound queue depths | Lower `max_clients`; the recorder is unaffected by design |
| Spec-drift CI failure | the diff | Update `wire` structs and decoders; raw tape is unaffected |

## 7. Backups and restore

Nightly `rclone sync` of keyframes, baked tables, manifests, and (while free) raw
segments to R2. Restore is `rclone copy` into `data/`; the catalog rebuilds its views
on start. A restore is exercised quarterly.

## 8. Cost ledger

| Item | Budget | Notes |
|---|---|---|
| Kalshi deposit for probes | $10 | minimum ACH deposit; about $3 at risk |
| Hostname | $0 to $10/yr | DuckDNS free or a purchased domain |
| Compute, storage, TLS, CI, hosting | $0 | Oracle Always Free, R2 free tier, Cloudflare Pages, GitHub Actions |
