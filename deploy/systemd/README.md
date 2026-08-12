# systemd units

The `pe-bootstrap-*` files are user-mode research/maintenance timers. The separate
`pe-service-live-canary.service` is a system unit for the isolated, initially disabled
Polymarket V2 canary. It uses a dedicated unprivileged account, systemd credentials, and private
state/runtime directories; it never reads the ordinary service `.env`.

Do not enable or start the canary as part of installation. Follow
[`docs/36-POLYMARKET-V2-CANARY-RUNBOOK.md`](../../docs/36-POLYMARKET-V2-CANARY-RUNBOOK.md).

Ordinary `pe-service` live credential custody (#508) follows the same root-controlled source-file
precedent without sharing canary secrets: install the committed drop-in that maps the root-owned
mode-`0600` age identity as `LoadCredential=pe-age-identity:<identity-path>`. The per-account bundles
remain age-sealed in Supabase and are never environment variables. Install, verify, and arm only via
[`docs/35-PE-SERVICE-DEPLOY-RUNBOOK.md`](../../docs/35-PE-SERVICE-DEPLOY-RUNBOOK.md).

## pe-bootstrap timers

User-mode systemd timer + service pairs that run the pile-maintenance
subcommands added in issue #166. All units are `Type=oneshot` and log to the
journal under their own `SyslogIdentifier`. (The Dune `discovery` timer was
removed in #335; incremental wallet discovery now runs via
`scripts/winner_discovery.sh` / `pe-bootstrap winner-discovery`.)

## Install

```bash
# 1. Build and install the binary to ~/.cargo/bin
cd <repo-root>
cargo install --path crates/bootstrap

# 2. Copy unit files
mkdir -p ~/.config/systemd/user
cp deploy/systemd/pe-bootstrap-*.{timer,service} ~/.config/systemd/user/

# 3. Adjust WorkingDirectory / EnvironmentFile paths if the repo is not at
#    ~/prediction-markets, then reload + enable.
systemctl --user daemon-reload
systemctl --user enable --now pe-bootstrap-backfill.timer
systemctl --user enable --now pe-bootstrap-weekly.timer
```

## Schedule

The timers are deliberately staggered so they never write to the
`wallet_cache.db` simultaneously — SQLite WAL mode permits concurrent readers
but only one writer, and two timers writing in lock-step would surface as
`SQLITE_BUSY` errors.

| Subcommand | Schedule | Rationale |
|---|---|---|
| `backfill`  | Daily 06:00            | Polymarket `/activity` for stale-or-NULL `last_polymarket_fetch_at`. |
| `weekly`    | Sunday 12:00           | Etherscan funder refresh for stale-or-NULL `last_funder_fetch_at`. |

## First-run discipline

After the initial pile population, the `last_polymarket_fetch_at`
queue starts with many NULL rows (newly-discovered wallets from
winner-discovery). Run `pe-bootstrap backfill` once manually with the
default `backfill_limit = 0` (no limit) to drain that queue before enabling
the daily timer:

```bash
pe-bootstrap backfill
```

Quality ordering (`dune_win_rate_bps DESC NULLS LAST, dune_closed_markets
DESC NULLS LAST`) processes known-good wallets first within the NULL bucket,
so even a partial run produces the most useful coverage first.

Once steady-state, set a positive `PE_BOOTSTRAP_BACKFILL_LIMIT` in `.env` if
daily run time grows unmanageable.

## Verification

```bash
systemctl --user list-timers | grep pe-bootstrap
journalctl --user -u pe-bootstrap-backfill.service -n 100
```
