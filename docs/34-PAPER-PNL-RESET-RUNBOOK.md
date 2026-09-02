# 34 — Paper-P&L archive-then-reset runbook

**Purpose.** Start the paper copy-trader's P&L from a clean T0 for the 2026-07-03
single-system cutover (`docs/32` §1 operator decision) — archiving, never destroying,
the prior track record. Tooling: `scripts/paper_reset/reset_paper_state.sh` (dry-run by
default) + `scripts/paper_reset/archive_paper_state.sql` (fail-closed, single
transaction, the #474 archive-before-DELETE discipline).

## What is being reset, and where it lives

Paper state is dual-store (`docs/_GLOSSARY.md`, issue #397): **Supabase is
authoritative** (`paper_bankroll` singleton, `paper_positions`, `paper_fills`,
`settled_markets`; mutated only via the `commit_fill` / `apply_resolution` RPCs), local
SQLite (`paper_state.db`) is the write-through cache, and the BLAKE3 **event log**
(`paper.log`, plus the `live_journal.log` stream) is the true fill source that boot
replays. `fill_market_snapshots` (analytics) is archived alongside. Local-only tables
(`seen_trades`, `poll_cursors`, `leader_positions`, `meta`) go with the SQLite file.

The account-tagged journal is named exactly `live_journal.log`; there is no
`paper.live.log` artifact. Schema v2 adds reconciled activity/group revisions, durable
entry-gate history/results/completeness, `decision_pending`, wallet fences, position
validations, and the machine-owned v1→v2 migration/activation record. Those rows are part of
the authority boundary, not disposable analytics state.

> **Schema-v2 stop condition (#544).** Before executing this runbook, inspect
> `PRAGMA user_version` on the stopped service's fixed `paper_state.db`. The archive/reset
> tooling below is the historical schema-v1 reset workflow. Do not execute it when the value is
> `2`: rotating away the installed v2 main would also erase the machine-owned migration record,
> and ordinary boot requires an existing fixed main with matching recorded log boundaries. A v2
> P&L reset needs a separately reviewed generation-reset surface; none shipped in #544.

## The one landmine

**The event log MUST be rotated together with the SQLite file.** A fresh
`paper_state.db` resets both replay watermarks (`last_applied_event_seq`,
`last_supabase_applied_event_seq`); if `paper.log` is still present at the next boot,
`reconcile_paper_state` replays every historical fill into SQLite and
`catch_up_supabase` re-inserts them into the just-emptied Supabase `paper_fills` —
re-debiting the fresh bankroll and silently undoing the reset
(`crates/service/src/paper_recovery.rs`, `supabase_state.rs`).

Bounded, accepted side effect: wiping `seen_trades` + `poll_cursors` re-opens a
boundary-second window (cursors re-seed to each wallet's `last_trade_unix`; the poller
fetches from cursor−1 exclusive), so a handful of already-copied boundary trades may
re-arrive as fresh events. Idempotency keys regenerate identically, but against an
emptied `paper_fills` the RPC treats them as new — the exposure is a few
boundary-second trades at most, and the first-entry gate blocks most.
The captured legacy history file is deliberately **kept** for a schema-v1 migration rehearsal.
It is a one-time, hash-bound migration input selected by `PE_LEGACY_WALLET_HISTORY_PATH`, not a
runtime sidecar; once the migration reaches phase `installed`, edits are inert.

## Sequence

| # | where | action |
|---|---|---|
| 1 | operator (sudo) | `ssh -t -i ~/.ssh/id_personal sean@82.22.32.225 'sudo systemctl stop pe-service'` |
| 2 | local checkout | `bash scripts/paper_reset/reset_paper_state.sh --execute` — archives all 5 tables into `*_archive` (stamped `archived_at`), verifies counts, deletes live rows, keeps `supabase_sink_hwm`, reloads the PostgREST cache. Any failure = full rollback. |
| 3 | VPS (service **stopped**) | rotate local state into an archive dir: `paper_state.db`, `paper_state.db-wal`, `paper_state.db-shm`, `paper.log`, `live_journal.log` (the live journal is derived beside the configured event log in `main.rs`). Resolve the exact working directory via `systemctl cat pe-service` first (paths are CWD-relative, `config.rs`). |
| 4 | VPS (service stopped) | confirm the fresh starting bankroll: `bankroll_usd` boot config (default `10000`) — `init_bankroll` credits it because the fresh SQLite has no row. Then run the binary once with `--backfill-supabase` to re-seed the Supabase `paper_bankroll` singleton (authoritative boot fail-closes without it, by design). Also set the `service_config.bankroll_usd` row to the same value (bookkeeping mirror only — no runtime behavior consumes the parsed value, #516; the BOOT value is the /paper/pnl denominator). |
| 5 | operator (sudo) | `ssh -t ... 'sudo systemctl start pe-service'` |
| 6 | local | `psql "$SUPABASE_DB_URL" -c 'refresh materialized view concurrently wallet_live_stats_mv;'` (optional — pg_cron refreshes ≤2 min) |

## Verification (step 6+)

- `select count(*) from paper_fills;` → 0, then grows only with organic post-T0 fills.
- `select bankroll_str from paper_bankroll where id = 0;` → the fresh starting value.
- Boot log shows a clean authoritative boot (no `Uninitialised`), watchlist seeded from
  `latest_ranking`, `service_config poll loop started`, 0 poll failures.
- Dashboard shows zeroed P&L after the matview refresh.
- The #473 demotion gate is expected to be **inert** immediately post-reset (it feeds on
  settled local fills; nothing fires below `demotion_min_trades` per wallet) — this is
  correct, not a bug.

## Rollback

The archive is the rollback: restore Supabase rows with
`insert into paper_fills select <original columns> from paper_fills_archive` (drop the
`archived_at` column from the select list; same for the other four tables), and move
the archived `paper_state.db*`, `paper.log`, and `live_journal.log` files back before starting the
service.
Archive tables are append-only across resets (`archived_at` distinguishes epochs) —
never dropped by tooling.

## #511: rebuild-state and RPC v1 notes

- `--rebuild-state` is **refused in authoritative mode** (`PE_SUPABASE_AUTHORITATIVE=true`):
  frame-only reconstruction cannot know authority dispositions (a refused or ambiguously
  failed frame would resurrect locally and diverge from Supabase). Restore local state by
  restarting the service — the boot frame-walk converges SQLite on the system of record.
  In legacy mode, rebuild restores `settled_markets` from its own backup before replaying,
  so replay refuses fills into already-settled markets.
- The v1 RPCs (`commit_fill`, `apply_resolution`) are retained through the #511 rollback
  window. Revoke their `service_role` execute grants in a later cycle once the #511 binary
  has soaked (rollback to the pre-#511 binary requires them).

## #544 schema-v1 to schema-v2 roll-forward notes

This is migration, not a P&L reset. One ordinary v2 `pe-service` boot performs it with
`PE_SUPABASE_AUTHORITATIVE=true` and `PE_LEGACY_WALLET_HISTORY_PATH` pointing at the captured
legacy input. The machine-owned phases are `boundary_recorded`,
`version_two_inputs_appending`, `side_state_built`, `activation_tails_recorded`, and
`installed`; restart resumes the exact recorded side main and phase. The source, paper, and
`live_journal.log` path/tail/sequence/hash bindings must still match.

Only before any v2 append or active-state commit, rollback can preserve the failed side and
restore the immutable v1 main:

```bash
PE_PAPER_V1_BACKUP_PATH="$PAPER_V1_BACKUP" \
PE_PAPER_FAILED_SIDE_PATH="$FAILED_PAPER_SIDE" \
pe-service --rollback-paper-v1
```

Once any first v2 append or active-state commit exists, that command refuses. Restart the
v2-compatible binary to resume roll-forward; do not rotate logs, replace the fixed main, or
manually edit the migration record.
