# Deployment files

The configuration of the production host described in ADR 0024 and
[docs/OPERATIONS.md](../docs/OPERATIONS.md) section 3. Paths and the service user
(`tapeops`) are this deployment's; adjust them for another host.

| File | Installed as |
|---|---|
| `tape.server.example.toml` | `/home/tapeops/tape.toml` (mode 600), with the real `key_id` |
| `systemd/tape-record.service` | `/etc/systemd/system/tape-record.service` |
| `systemd/tape-serve.service` | `/etc/systemd/system/tape-serve.service` |
| `systemd/tape-bake.service` | `/etc/systemd/system/tape-bake.service` |
| `systemd/tape-bake.timer` | `/etc/systemd/system/tape-bake.timer`, enabled with `systemctl enable --now` |
| `caddy/Caddyfile` | `/etc/caddy/Caddyfile` |
| `needrestart/tape.conf` | `/etc/needrestart/conf.d/tape.conf`: automatic library upgrades never restart the recorder or API |
