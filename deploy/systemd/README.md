# pe-bootstrap systemd units

User-mode systemd timer + service pairs that run the three pile-maintenance
subcommands added in issue #166. All units are `Type=oneshot` and log to the
journal under their own `SyslogIdentifier`.

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
systemctl --user enable --now pe-bootstrap-discovery.timer
systemctl --user enable --now pe-bootstrap-backfill.timer
systemctl --user enable --now pe-bootstrap-weekly.timer
```

## Schedule

The three timers are deliberately staggered so they never write to the
`wallet_cache.db` simultaneously — SQLite WAL mode permits concurrent readers
but only one writer, and two timers writing in lock-step would surface as
`SQLITE_BUSY` errors.

| Subcommand | Schedule | Rationale |
|---|---|---|
| `discovery` | Mon/Wed/Fri/Sun 02:00 | Every ~48h; Dune anti-join keeps the upload incremental. |
| `backfill`  | Daily 06:00            | Polymarket `/activity` for stale-or-NULL `last_polymarket_fetch_at`. |
| `weekly`    | Sunday 12:00           | Etherscan funder refresh for stale-or-NULL `last_funder_fetch_at`. |

## First-run discipline

After the initial `pe-bootstrap migrate` run, the `last_polymarket_fetch_at`
queue starts with hundreds of thousands of NULL rows (newly-active wallets
from the Dune CSVs). Run `pe-bootstrap backfill` once manually with the
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
