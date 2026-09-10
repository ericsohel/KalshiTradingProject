# Operations

## 1. Environments

| Name | Host | Kalshi env | Purpose |
|---|---|---|---|
| dev | Owner's Mac | demo for order paths, prod public data for capture | development, first recordings |
| prod | Oracle Cloud Always Free ARM (2 OCPU, 12 GB, 200 GB), US region | prod | continuous recording, API |

The first production tape starts on the Mac the day a production read key exists; the
process moves to the cloud host once it has run cleanly for a week.

**Running on a Mac.** A sleeping laptop records nothing: the first production smoke
test lost two five-minute windows to sleep on battery. The Mac must be on AC power,
with automatic sleep prevented while the display is off (System Settings, Battery,
Options), and the recorder runs under `caffeinate -i` so the process holds a sleep
assertion for as long as it runs. Sleep that happens anyway is written into the tape as
a `clock_jump` record, so the resulting gap is attributable rather than mysterious.

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
- The recorder's status line carries the bus counters: `bus_seq`, `sent`, `dropped`,
  `errors`, and `refreshes`. A subscriber's own losses are not among them, because ZeroMQ
  drops a slow subscriber's copies without telling the publisher; the subscriber counts them
  from gaps in `bus_seq`.
- A dead-man ping fires every minute while the control connection is open; a missed
  ping alerts within three minutes.
- Alerts: recorder down, disk under 10% free, gap share over 1% in the last hour,
  audit exact ratio under 99% in the last hour, certificate expiry within 14 days.

### 4.1 The live bus

`tape record` publishes on the bus only when `recorder.bus_endpoint` is set; enabling or
changing it takes a recorder restart, which the tape records as a gap. Use an absolute
`ipc://` path, at most 103 bytes on macOS, in a directory that only the `tape` user can
write; the recorder replaces a stale socket left by a crash and refuses to start if another
process is listening on the path or a file other than a socket is there. `tape serve` connects
to the same endpoint and can start, stop, or restart at any time: it knows each book again
within `bus_refresh_s` (ADR 0022). Nothing a consumer does can block or stop the recorder.

### 4.2 The live API

`tape serve` serves the viewer's API (docs/FRONTEND.md 4) from the same `tape.toml`:

```
uv run tape serve --config tape.toml
curl -s http://127.0.0.1:8080/api/v1/status
curl -s 'http://127.0.0.1:8080/api/v1/markets?limit=5'
```

It listens on `serve.listen_host:serve.listen_port`, `127.0.0.1:8080` by default, and follows
the bus at `recorder.bus_endpoint`; without that setting it refuses to start. The recorder must
run with the same endpoint, and a recorder started before the catalog and status topics existed
must be restarted once to publish them: until then the market list is empty and `recorder` is
null, although the bus counters move. The API holds no credentials: `[kalshi] env` only selects
where public titles, categories, and price grids come from, and the key file is never opened.
SIGINT or SIGTERM closes live connections with 1001 and stops the process; a second signal
forces it.

`GET /api/v1/status` shows `recording`, true while a recorder status arrived within two of its
intervals; `recorder_status_age_ms`; `recorder`, the latest status (universe size, live
subscriptions, and per connection frames, gaps, reconnects, stale books, and dropped records);
`bus`, the recorder run the API follows (`epoch`, `last_seq`), `messages` received, `resets` and
`missed` from lost messages, and `books_known`; and `clients`. After a start, books are known
within `bus_refresh_s`, markets are listed at the next refresh cycle, and metadata fills in as
Kalshi answers, at most `serve.metadata_requests_per_s` requests a second.

## 5. Storage and retention

Measured in week one and revisited monthly. A first 65-second production sample
extrapolated to about 1.7 GB per day compressed, 99% of it from the unfiltered
`ticker` channel, which would fill the 200 GB free-tier disk in roughly four months
and the 10 GB R2 free tier in under a week. Order books and trades for the 50
busiest markets were a small fraction of that. The `ticker` channel is therefore
live-only and never taped (ADR 0018), so the taped volume is the order-book, trade, and
lifecycle traffic. Policy from day one:

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
| API slow | `clients` in `/api/v1/status`, clients closed for lagging in the API log | Lower `serve.max_clients`; the recorder is unaffected by design |
| Live view stuck in resync | recorder status `bus.refreshes` and `bus.errors`; both processes name the same `bus_endpoint`; `bus.resets` and `bus.missed` in the API's `/api/v1/status` | Refreshes not rising: check the recorder log for `bus refresh failed`. Resets rising: raise `serve.bus_receive_hwm` or `bus_send_hwm`. The tape is unaffected either way |
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
