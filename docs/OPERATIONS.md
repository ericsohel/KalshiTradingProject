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
   `systemd/tape-record.service` and `systemd/tape-serve.service`, and enable both. Install
   `needrestart/tape.conf` in `/etc/needrestart/conf.d/`, so unattended upgrades install but never
   restart either service; every deploy restarts them deliberately.
7. Build the viewer on a development machine (`npm --prefix web run build`), copy
   `web/dist/` to `/srv/tape/www` with `rsync --delete`, install `caddy/Caddyfile`, and
   confirm that the certificate is issued.
8. Bake and prune every hour. The maintainer installs `systemd/tape-bake.timer` and `systemd/tape-bake.service`, a timer that
   starts a oneshot service as `tapeops` at a quarter past every hour (`OnCalendar=*:15`), with
   `Nice=10` and `IOSchedulingClass=idle` so capture keeps the CPUs and the disk. The service runs
   `tape bake --config ~/tape.toml`, then, only if that exits 0,
   `tape prune --config ~/tape.toml --apply`. `tape bake` bakes every hour that ended more than
   `bake.grace_s` ago and has no current bake, and records each in its day's manifest.
   `tape prune --apply` deletes the raw segments of hours that meet every condition of ADR 0025, after
   `bake.raw_retention_hours`, and writes each deletion into the manifest before deleting the file.
   Both commands take a lock beside the manifests, so an overlapping run exits 1 and changes nothing,
   and both print a JSON summary to the journal. `tape prune` without `--apply` shows, for every raw
   hour, whether it is prunable and every reason it is not.

To deploy new code: `git -C ~/tape pull --ff-only` and `uv sync --frozen`, then restart
`tape-serve`, and `tape-record` only when recorder code changed, because every recorder
restart is a gap in the tape.

Not yet in place: a check that time sync is healthy, backup timers, a
dead-man ping and alerts, and `MemoryMax` per service.

## 4. Monitoring

- `tape record` exposes Prometheus metrics on localhost: messages per second per
  channel and connection, gap count, resync count, reconnects, writer queue depth,
  frames dropped, disk free, audit mismatches, last-frame age.
- The recorder's status line carries the bus counters: `bus_seq`, `sent`, `dropped`,
  `errors`, and `refreshes`. A subscriber's own losses are not among them, because ZeroMQ
  drops a slow subscriber's copies without telling the publisher; the subscriber counts them
  from gaps in `bus_seq`.
- The status line's `universe_size` counts the markets selected, `subscribed_markets` those
  of book connections whose subscriptions are live, and `live_tickers` the recorded markets
  with a latest `ticker` value in memory. `live_tickers` never exceeds the plan: a market is
  dropped at the refresh that removes it from the universe (ADR 0027). It is lower while some
  recorded markets have had no ticker change since they were subscribed.
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

### 4.4 Choosing the recorded universe

`[[recorder.universe.groups]]` decides which markets are recorded (ADR 0028). Changing the groups
takes a recorder restart, which the tape records as a gap, so check first what they would choose
now:

```
uv run tape universe preview --config tape.toml
```

The preview lists the open markets and, when a group selects by category, that category's series,
from Kalshi's public endpoints without credentials. It opens no WebSocket and writes nothing.
Holding no credentials, it cannot read its rate tier and paces itself at 2 requests a second, below
Kalshi's unsigned allowance, so a listing of about 120 pages takes a minute. For
each group it prints its settings, `market_order` included, the chosen events and their markets
with series, 24-hour volume, YES mid (`no price` when the listing has neither quotes nor a trade),
and close time, and what the group admitted, in how many events, and how many markets it chose that
did not fit the budget; then the total against `max_l2_markets` and why every other listed market is not
recorded. It also counts the series in each category, where zero usually means a misspelled
category, and names series of series groups with no open market, such as a monthly series between
releases.

- **A group takes too much:** lower its `events` or `markets_per_event`, or set `max_markets`. On
  a host short of CPU, a category group's `events` is the first dial.
- **Season futures in a category group:** summed volume favours long-lived events with many
  markets. Set `max_hours_to_close` so the group ranks only events whose earliest close is within
  that many hours; the production sports group uses 48 (ADR 0028).
- **Strikes far from the price:** by default an event's busiest markets come first, which on an
  event that opened minutes ago are arbitrary strikes. Set `market_order = "near_price"` on a
  ladder group: thresholds whose YES mid is closest to 50 cents come first, and in an event of
  buckets or outcomes, the highest mid; the preview's mid column shows the result (ADR 0029). The
  production Bitcoin hourly, stock index, economy, and weather groups use it.
- **Both books of a binary event:** raise `markets_per_event` for that group.
- **Priority:** groups later in the list lose first when the budget runs out, and `skipped for
  budget` shows by how much. A market is admitted by the first group that chooses it and does not
  count against later groups.
- **Volume floor:** series groups ignore `min_volume_24h`; a category group chooses only events whose
  24-hour volume, summed over their markets, reaches it.

Markets admitted by series groups carry the showcase flag in the catalog. A configuration without
groups records nothing: every refresh warns `no universe groups are configured; no market will be
recorded`, `tape config check` prints a warning on standard error, and the preview says so.

Every universe refresh logs `universe refreshed` with `listed`, `truncated`, `selected` (never more
than `max_l2_markets`), `showcase`, `dropped_for_cap`, `reason_counts`, `groups`, which gives per
group `admitted`, `events`, and `skipped_for_budget`, and `book_groups`, the book connections in use.
`reason_counts` accounts for every listed market: `duplicate`, `not_active`, `mve`, `closed`, and
`beyond_horizon` before any group, then `no_group`, `event_beyond_horizon`, `below_volume`, `event_not_chosen`,
`over_markets_per_event`, `over_max_markets`, and `over_cap`, which is skipped for budget. With a
category group, series categories are fetched at the first refresh and then at most hourly:
`series categories fetched` gives the series per category, a failure logs `series categories not
fetched; keeping the last known` and is retried at the next refresh, and until the first success
every refresh warns `series categories unknown; category groups admit nothing`.

Between refreshes the recorder reacts to closes (ADR 0029). Three seconds after a planned market's
close time, or at once when the control connection reports it `determined` or `settled`, it
removes the market from the plan without a listing and logs `closed markets removed from the
universe` with `closed` (the tickers), `selected`, `groups`, `relisting` (the series groups that
lost a market), and `book_groups`. Each such series group, and five seconds after a `created` or
`activated` event any series group naming that series, is listed again with
`GET /markets?series_ticker=<series>&status=open` for each of its series, at most 10 pages each,
and re-applied alone; every other group keeps what it admitted. That logs `universe groups
re-listed` with `relisted`, `series`, `listed`, `truncated`, `unreadable`, `first_error`, `added`,
`removed`, `selected`, `groups`, and `book_groups`. Two re-listings start at least 30 seconds
apart, so a burst of closes and new markets costs one. A failed re-listing logs `targeted
re-listing failed; keeping the current plan and retrying` with `error`, `relisting`, and
`retry_in_s`, and is retried after that interval; capture is unaffected, and the full refresh
still converges on the same universe.

A new event is often listed before its first quotes, so a `near_price` group can only admit its
markets by volume and ticker. When a full refresh or a re-listing leaves such a series group with
an admitted event whose admitted markets are not all priced (a market without a YES mid), the
recorder lists the group again 30 seconds after that listing started, at most 4 times per event,
and logs `near-price follow-up re-listing requested` with `group`, `event`, `attempt`,
`max_attempts`, `unpriced` (the tickers), and `due_in_s`. Follow-ups for an event end with `near-price
follow-ups ended`, with `group`, `event`, `attempts`, and `outcome`: `priced` once its admitted
markets all have a mid, `left_plan` once no market of it is admitted, or `gave_up` after the last
attempt, which also carries `unpriced` and `given_up_total`, the events given up on since start.
A group that gave up on an event is not listed again for it; the next full refresh re-ranks it.

- **A category group runs short:** its closed markets are removed at close but replaced only at
  the next full refresh, so `universe_refresh_s` bounds how long it has fewer markets. Name a
  series in a series group to have its replacements start within seconds.
- **No re-listing after a close:** the group that lost the market is a category group, or the
  series has no open market yet; a `created` or `activated` event for the series brings one.

## 5. Storage and retention

Measured in production and revisited monthly. With the `ticker` channel live-only
(ADR 0018), 2,000 markets produce about 9.4 GB of raw data a day and 200 markets about
2.4 GB. The production host's 64 GB data disk therefore holds about 25 days at 200
markets, so retention, or moving data off the host, is required within weeks; the Mac,
with far more disk, keeps the longer archive. Policy (ADR 0025):

| Data | Retention on the production host | Backup |
|---|---|---|
| Raw segments | `raw_retention_hours` (72) after a verified bake of their hour (ADR 0025) | the Mac archive, once set up |
| Keyframes | forever | planned |
| Baked tables and manifests | forever | planned |

`tape bake` bakes closed hours; `tape prune --apply` then deletes the raw segments of hours
that meet every condition in ADR 0025 and records each deletion in the day's manifest.

### 5.1 Baking the recorded archive

Measured on 2026-09-12 by baking the Mac's tape from 2026-09-10T20 to 2026-09-12T20 UTC, read
only, into a separate archive: 49 closed hours, 636 segments, 5.81 GiB of raw data, and
219,979,432 records, one hour per process on an Apple silicon Mac.

| Measure | Result |
|---|---|
| Baked size | 1.99 GiB, 2.91 times smaller than raw; 2.92 times over the 21 hours above 50 MiB; from 1.5 times for nearly idle hours to 3.06 times; two hours of a few kilobytes wrote no rows |
| Largest hours | 2026-09-11T01: 667 MiB to 230 MiB in 87 s; T02: 576 MiB to 197 MiB in 76 s; T00: 527 MiB to 180 MiB in 69 s |
| Share of baked bytes | `deltas` 94%, `trades` 5%, `snapshots` 1%, the rest under 0.2% |
| Speed | 7.6 MiB of raw data a second on one core |
| Peak memory | at most 518 MB resident and 402 MB physical footprint for any hour, with 500,000-row parts |
| Records | 214,720,809 deltas, 198,879 snapshots (9,308,701 rows), 4,879,863 trades, 139,534 lifecycle messages, 32,810 audits baked; 2,840 commands, 1,490 connection events, and 3,207 subscription replies not baked |
| Failures | no decode failure, corrupt segment, truncated segment, gap, or writer overflow |

- **Encodings.** Delta-encoding deltas' receive times and sequence numbers and dictionary-encoding
  their prices, sides, and counts made them 17% smaller than plain encoding; delta encoding made
  snapshots twice as large, so encodings are chosen per table. zstd level 9 saved under 1% over
  level 6 at twice the write time. Of about 73 bits a delta row, `recv_mono_ns` and `recv_wall_ns`
  take 24 and 20.
- **Memory.** On the second largest hour, reading records peaked near 155 MB of physical footprint
  and writing parts at 336 MB with 500,000-row parts, or 277 MB with 250,000, at the same speed;
  the production configuration uses 250,000. Arrow's system allocator lowered the peak by about a
  third against its default pool, and reading segments in 64 KiB chunks rather than 1 MiB cut the
  reader's footprint from 190 MB to 38 MB.
- **Disk.** At 5.2 GB of raw data a day and 2.9 times, baked tables grow about 1.8 GB a day, and 72
  hours of raw data hold about 16 GB, so the production host's 64 GB disk lasts about 26 days
  instead of 11. Baked tables are smaller, not far smaller (ADR 0025): pruning buys weeks, and a
  larger disk, a smaller receive-time encoding, or moving baked tables off the host is still needed
  within a month.
- **Integrity numbers.** The Mac slept for much of 2026-09-11 (56,072 seconds of recorded sleep),
  so that day's uptime is 29,182 of 86,400 seconds. Audits passed 5,567 of 5,606, 18,248 of 18,400,
  and 8,657 of 8,704 on the three days (ADR 0021).
- **Clock.** At 2026-09-11T03:13:42 UTC the Mac's wall clock stepped back by more than 50 ms, so
  frames of one subscription carry receive times out of sequence order. Rebuilding books by wall
  receive time broke three books there. Ordering by sequence number instead was far worse, 95.7%
  agreement, because every reconnect gets `sid` 1 again and restarts the sequence; replay therefore
  orders changes by monotonic receive time.
- **Rebuilding books.** For each of the 187 pairs of consecutive keyframes in the baked hours,
  every book fresh in both keyframes was rebuilt from the earlier keyframe and the baked changes up
  to the later keyframe's instant (`Catalog.books_at` with `start_wall_ns`): 317,217 of 317,389
  equal the later keyframe, 99.95%. Each of the other 172 had a book change received within 100 ms
  of a keyframe instant, 47 of them ending stale where a replayed delta found a level missing. A
  frame is stamped when it is read from the socket but applied when the event loop reaches it, and a
  keyframe is taken between two applied frames, so a frame read just before the instant can be
  applied just after it. No book disagreed away from a keyframe instant.

A dry run of `tape prune` on copies of four hours (2026-09-10T22, 2026-09-11T06 and T07, and
2026-09-12T20), baked with `tape bake`, reported every hour inside the 72-hour window. With
`TAPE_BAKE__RAW_RETENTION_HOURS=24` the three older hours, 173 MiB, were prunable and 2026-09-12T20
was inside the window. After a byte was appended to a segment of T06 and a part file of T07 deleted,
T06 reported `segment_changed` and T07 `baked_file_missing`; `--apply` deleted only T22's 53
segments and recorded all 53 in the manifest, which still lists them. A new `tape bake` restored
T07, which became prunable, and baked T06 again, which then reported `decode_failures`: the
appended byte is damage, not a truncated tail.

## 6. Runbook

| Symptom | Check | Action |
|---|---|---|
| Dead-man alert | `systemctl status tape-record`, journal tail | Restart if crashed; if a Kalshi outage, wait; the gap is visible in the manifest |
| Repeated reconnects | `GET /exchange/status`, changelog RSS | If Kalshi maintenance (Thursdays 3 to 5 AM ET) do nothing; else inspect error frames in the raw segment |
| Recorder or API restarted with no deploy | `Stopping tape-record.service` in the journal with no matching `sudo` command; `/var/log/apt/history.log` at that time | Unattended upgrades restarted it through needrestart: install `deploy/needrestart/tape.conf`; the upgraded libraries take effect at the next deliberate restart |
| Gap share rising | per-connection message rate | Add a connection; shrink group size |
| Audit mismatches on one group | affected `sid` | Force `get_snapshot`; if persistent, open an issue with the raw frames |
| Disk filling | `df`, manifest sizes, `tape prune` (reasons hours are kept) | Fix what keeps hours from pruning (a decode failure needs a baker fix and a `BAKE_VERSION` bump), run `tape bake`, then `tape prune --apply`; tighten the L2 universe |
| Hour not prunable | `tape prune` reasons; the hour's `bake.hours` entry in its manifest | `segment_changed` or `baked_file_changed`: find what wrote to the archive; `baked_file_missing` or `bake_version_stale`: `tape bake` repairs it; `decode_failures`: the bake log names each failure and its segment |
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
