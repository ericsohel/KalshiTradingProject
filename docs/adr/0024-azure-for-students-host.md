# 0024. Host on Azure for Students, and serve the viewer from the same host

Status: accepted. Date: 2026-09-12. Amends ADR 0009 (static site hosting and ingress).

## Context

The recorder must run around the clock, because a missed hour of order-book data cannot
be recovered. The plan was an Oracle Cloud Always Free Arm instance with Caddy and a free
dynamic-DNS name, the viewer on Cloudflare Pages, and the owner's laptop as a stopgap.
Three facts changed that plan in September 2026:

- Oracle signup did not complete for the owner, and Oracle had cut the Always Free Arm
  allowance in half in June 2026 while US regions often reported no Arm capacity.
- The owner did not want a laptop running 24/7, and a laptop that travels stops recording.
- The owner is a student, which makes Azure for Students available: USD 100 of credit per
  year while enrolled, no card, and 12 months of free services. Each student subscription
  may deploy only to a fixed list of regions, set by an Azure policy.

Storage and transfer set the recording scope. Measured on the laptop, 2,000 markets
produce about 9.4 GB of raw data per day and 200 markets about 2.4 GB.

## Alternatives considered

1. **Oracle Cloud Always Free.** The most capable free host. Rejected for now: signup
   failed, capacity is scarce, and the allowance was just reduced.
2. **The laptop, exposed with Tailscale Funnel.** Free, with 830 GB of disk. Rejected: it
   requires the laptop awake, powered, and at home around the clock.
3. **A paid VPS** (DigitalOcean, Vultr, or Linode at USD 4 to 6 a month). Rejected: the
   project budget of about USD 20 lasts a few months, and Hetzner's US prices rose in
   June 2026.
4. **Google Cloud's free e2-micro.** Rejected: a 30 GB disk and 1 GB of free egress a
   month cannot hold or ship the data.
5. **Viewer on Cloudflare Pages** (ADR 0009). Kept possible, not chosen: it needs another
   account, a cross-origin API, and CORS configuration, to protect a host that serves a
   few hundred kilobytes of static files to a handful of viewers.

## Decision

- **Host.** One Azure for Students virtual machine, `Standard_B2ats_v2` (2 vCPUs, 1 GB of
  memory, 2 GB of swap), Ubuntu 24.04, in North Central US, one of the subscription's two
  allowed US regions and the closer one to the exchange. Data lives on a separate 64 GB
  disk mounted at `/srv/tape`.
- **Scope.** The server records the 200 busiest markets, showcase series first. The
  laptop may keep recording the wider universe while it is on, and later pulls the
  server's data as an archive.
- **Serving.** Caddy terminates TLS for the VM's free Azure DNS name and serves the built
  viewer and the API from one origin: static files from `/srv/tape/www`, `/api/*` to
  `tape serve` on localhost. No CORS is needed, and the WebSocket origin check admits
  only that origin.
- **Processes.** `tape record` and `tape serve` run as separate systemd services that
  restart on failure, so a crash in one does not take down the other.
- **Credentials.** The server holds its own read-only Kalshi key, revocable without
  touching the laptop's.

The unit files, Caddyfile, and server configuration are in `deploy/`.

## Consequences

- **Capacity.** The host has little headroom: about 300 MB of memory is free with both
  services running, so recording more markets needs a larger size or less per-market
  state.
- **Disk.** The data disk holds roughly 25 days at 200 markets, so rolling retention or
  pulling data off the server is required within weeks.
- **Region.** The region list is fixed by policy; East US is denied.
- **Cost.** The free hours end after 12 months, and the credit then pays for the VM.
  Whether this size and these disks fall entirely within the free services is assumed
  until the first bill confirms it.
- **Failure domain.** The page, the API, and the recorder share one host, but not one
  process. Reaching the public means maintaining a single Ubuntu host: its updates, its
  firewall rules (ports 22, 80, and 443), and its services.

## What would reverse it

- **The bill shows charges** beyond the free services: change the VM size or disks.
- **Viewer traffic that measurably affects the host:** move the static site to Cloudflare
  Pages using the build's `VITE_TAPE_API_ORIGIN`.
- **Losing student status or credit:** retry Oracle's free tier, or move to a paid VPS.
- **Recording beyond 200 markets:** a larger disk, or baked tables with raw-data
  retention.
