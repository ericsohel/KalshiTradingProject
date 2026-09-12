# Operations

## 1. Environments

| Name | Host | Kalshi env | Purpose |
|---|---|---|---|
| dev | Owner's Mac | demo for order paths, prod public data for capture | development; records a wider universe while it is on |
| prod | Azure for Students VM `tape-1` (`Standard_B2ats_v2`: 2 vCPUs, 1 GB, 64 GB data disk), North Central US (ADR 0024) | prod | continuous recording of the 200 busiest markets, live API, viewer |

The first production tape ran on the Mac from 2026-09-10, and the production host has
recorded since 2026-09-12. The Mac's tape is a separate, wider recording; it does not
replace the host's.

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

The host of ADR 0024, as provisioned on 2026-09-12. Files named below are in `deploy/`.

1. In Azure Cloud Shell, under the Azure for Students subscription, read the allowed
   regions (`az policy assignment list`), then create the VM in an allowed US region:
   Ubuntu 24.04, `Standard_B2ats_v2`, a 64 GB Premium SSD OS disk and a 64 GB data disk,
   a Standard public IP with a DNS label, and SSH key authentication. Open ports 80 and
   443 besides SSH (`az vm open-port`).
2. Do not name the admin user `tape`. Ubuntu already has a system group `tape`, so
   provisioning fails to create the user and installs no key. The host uses `tapeops`,
   added afterwards with `az vm user update`.
3. Format the data disk as ext4 with label `tapedata` and mount it at `/srv/tape` with
   `nofail`; add a 2 GB swap file with `vm.swappiness=10`.
4. Install `git`, `zstd`, `uv`, and Caddy from its official package repository.
5. Clone the repository and run `uv sync --frozen`. Create a read-only Kalshi key for
   this host alone, copy its PEM to `~/.config/tape/keys/server-read.pem` (mode 600)
   with `scp`, and install `tape.server.example.toml` as `~/tape.toml` with its `key_id`.
   Check it with `tape config check` and `tape config check --for serve`.
6. Create `/srv/tape/run` (mode 700) for the bus socket, install
   `systemd/tape-record.service` and `systemd/tape-serve.service`, and enable both.
7. Build the viewer on a development machine (`npm --prefix web run build`), copy
   `web/dist/` to `/srv/tape/www` with `rsync --delete`, install `caddy/Caddyfile`, and
   confirm that the certificate is issued.

To deploy new code: `git -C ~/tape pull --ff-only` and `uv sync --frozen`, then restart
`tape-serve`, and `tape-record` only when recorder code changed, because every recorder
restart is a gap in the tape.

Not yet in place: a check that time sync is healthy, bake and backup timers, a dead-man
ping and alerts, and `MemoryMax` per service.

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
where public titles, categories, and price grids come from, `key_id` and `private_key_path` may
be left out of its configuration, and the key file is never opened;
`uv run tape config check --config tape.toml --for serve` checks what it needs. SIGINT or
SIGTERM closes live connections with 1001 and stops the process; a second signal forces it.

`GET /api/v1/status` shows `recording`, true while a recorder status arrived within two of its
intervals; `recorder_status_age_ms`; `recorder`, the latest status (universe size, live
subscriptions, and per connection frames, gaps, reconnects, stale books, and dropped records);
`bus`, the recorder run the API follows (`epoch`, `last_seq`), `messages` received, `resets` and
`missed` from lost messages, and `books_known`; and `clients`. After a start, books are known
within `bus_refresh_s`, markets are listed at the next refresh cycle, and metadata fills in as
Kalshi answers, at most `serve.metadata_requests_per_s` requests a second.

### 4.3 Watching live data locally

The viewer's development server proxies `/api` to `tape serve`, so real books show on a laptop
with three processes, each in its own terminal, from the repository root:

1. The recorder with a bus. Set `bus_endpoint` under `[recorder]` in `tape.toml` to an absolute
   `ipc://` path in a directory only you can write, for example
   `bus_endpoint = "ipc:///Users/you/kalshiProject/run/bus.sock"` after
   `mkdir -m 700 run`, then (re)start it: `uv run tape record --config tape.toml`.
2. The API: `uv run tape serve --config tape.toml`. `curl -s http://127.0.0.1:8080/api/v1/status`
   shows `recording: true` once the recorder has reported, and
   `curl -s 'http://127.0.0.1:8080/api/v1/markets?limit=5'` lists markets after its next
   refresh cycle.
3. The viewer (run `npm --prefix web ci` once first):
   `TAPE_API=http://127.0.0.1:8080 npm --prefix web run dev`, then open
   <http://localhost:5173>. `serve.allowed_origins` admits `http://localhost:5173` and
   `http://127.0.0.1:5173` by default; a page served from any other origin needs that origin
   added, or the live feed refuses it.

Until the recorder's first catalog reaches the API the page says it is waiting for the market
list, and it recovers without a reload.

Without the recorder, a synthetic stand-in for `tape serve` serves the same routes and feed on
port 8787: `npm --prefix web run mock` (add `-- --chaos` for random resyncs, stale books, and
closes), then `TAPE_API=http://127.0.0.1:8787 npm --prefix web run dev`; any local origin is
accepted. `web/dev/mock-server.ts` lists the disruptions it can trigger on request.

## 5. Storage and retention

Measured in production and revisited monthly. With the `ticker` channel live-only
(ADR 0018), 2,000 markets produce about 9.4 GB of raw data a day and 200 markets about
2.4 GB. The production host's 64 GB data disk therefore holds about 25 days at 200
markets, so retention, or moving data off the host, is required within weeks; the Mac,
with far more disk, keeps the longer archive. Policy from day one:

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
| Hostname | $0 | Azure DNS label on the VM's public IP (`*.cloudapp.azure.com`) |
| Compute, storage, TLS, CI, hosting | $0 | Azure for Students (USD 100 of credit a year and 12 months of free services; to be confirmed on the first bill), Let's Encrypt through Caddy, GitHub Actions |
