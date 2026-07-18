# 35 — pe-service binary deploy runbook (VPS)

> This runbook deploys the ordinary **paper-only** `pe-service`. It does not deploy or configure
> the isolated Polymarket V2 canary; see
> [`36-POLYMARKET-V2-CANARY-RUNBOOK.md`](36-POLYMARKET-V2-CANARY-RUNBOOK.md).

**Purpose.** The repeatable procedure for building the `pe-service` release binary and
deploying it to the VPS. Until this file, the procedure lived only in session memory —
fields marked *(verify)* are recorded from operator history and must be confirmed
against the live box on first use, then corrected here.

## Facts

| item | value |
|---|---|
| VPS | `82.22.32.225`, user `sean` (`ssh -i ~/.ssh/id_personal sean@82.22.32.225` — never root) |
| unit | systemd **system** unit `pe-service` — start/stop needs interactive sudo (`ssh -t … 'sudo systemctl <verb> pe-service'`); a coding agent cannot restart it non-interactively |
| binary path | `target/release/pe-service` under the service checkout *(verify via `systemctl cat pe-service` → `ExecStart`/`WorkingDirectory`)* |
| backup convention | before overwrite: `cp target/release/pe-service target/release/pe-service.bak-<old-sha>` (e.g. `pe-service.bak-a166f05`, `pe-service.bak-da17c16`) |
| env | `.env` on the VPS (REST keys `PE_SUPABASE_URL`/`PE_SUPABASE_SECRET_KEY`; **no** `SUPABASE_DB_URL` there). `PE_` booleans must be `true`/`false`, never `1`/`0` (figment rejects ints → restart loop) |
| build box | the VPS has no cargo — build on the dev box and `scp`, or build in a VPS-compatible container. Dev box: `cargo build --release -p pe-service` at the target main SHA (rustc 1.95.0) |
| logs | `journalctl -u pe-service -f` (Tier-1 prod check); JSONL sinks per `jsonl_log_path` |

## Procedure

1. **Build at the exact main SHA** being deployed (record it):
   `git -C /home/sean/git/prediction-markets rev-parse --short HEAD && cargo build --release -p pe-service`
2. **Ship**: `scp -i ~/.ssh/id_personal target/release/pe-service sean@82.22.32.225:/tmp/pe-service.new`
3. **Stop** (operator, interactive sudo): `ssh -t … 'sudo systemctl stop pe-service'`
4. **Backup + swap** (on the VPS, service stopped):
   `cp <workdir>/target/release/pe-service <workdir>/target/release/pe-service.bak-<old-sha> && mv /tmp/pe-service.new <workdir>/target/release/pe-service && chmod +x <workdir>/target/release/pe-service`
5. **Config deltas for this deploy** (see each PR's "Deployment impact"): update `.env` /
   TOML boot knobs (e.g. `PE_WATCHLIST_MEMBERSHIP_MODE=full_rerank`) and PATCH/INSERT the
   live `service_config` rows the new binary reads (seed `on conflict do nothing` never
   updates an existing row — changed defaults need a manual `PATCH`).
6. **Start**: `ssh -t … 'sudo systemctl start pe-service'`
7. **Verify**: `journalctl -u pe-service -n 100` — clean boot (no config-parse error /
   restart loop), watchlist seeded from `latest_ranking`, `service_config poll loop
   started`, no poll failures; then confirm behavior-specific log lines for the deploy
   (e.g. the first `full re-rank membership swap applied` after a ranking push).

## Rollback

Stop the service, `mv target/release/pe-service.bak-<old-sha>` back over the binary,
revert any config deltas the old binary does not understand (unknown `service_config`
keys are ignored by old binaries — safe to leave), start, verify boot per step 7.
