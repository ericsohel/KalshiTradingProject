# Deployment files

The configuration of the production host described in ADR 0024 and
[docs/OPERATIONS.md](../docs/OPERATIONS.md) section 3. Paths and the service user
(`tapeops`) are this deployment's; adjust them for another host.

| File | Installed as |
|---|---|
| `tape.server.example.toml` | `/home/tapeops/tape.toml` (mode 600), with the real `key_id` |
| `systemd/tape-record.service` | `/etc/systemd/system/tape-record.service` |
| `systemd/tape-serve.service` | `/etc/systemd/system/tape-serve.service` |
| `caddy/Caddyfile` | `/etc/caddy/Caddyfile` |
