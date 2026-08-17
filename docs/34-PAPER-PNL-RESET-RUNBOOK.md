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
(`paper.log`, plus the `paper.live.log` stream) is the true fill source that boot
replays. `fill_market_snapshots` (analytics) is archived alongside. Local-only tables
(`seen_trades`, `poll_cursors`, `leader_positions`, `meta`) go with the SQLite file.

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
`wallet_market_history.json` (first-entry sidecar) is deliberately **kept**:
first-EVER-entry semantics remain correct across the reset.

## Sequence

| # | where | action |
|---|---|---|
| 1 | operator (sudo) | `ssh -t -i ~/.ssh/id_personal sean@82.22.32.225 'sudo systemctl stop pe-service'` |
| 2 | local checkout | `bash scripts/paper_reset/reset_paper_state.sh --execute` — archives all 5 tables into `*_archive` (stamped `archived_at`), verifies counts, deletes live rows, keeps `supabase_sink_hwm`, reloads the PostgREST cache. Any failure = full rollback. |
| 3 | VPS (service **stopped**) | rotate local state into an archive dir: `paper_state.db`, `paper_state.db-wal`, `paper_state.db-shm`, `paper.log`, `paper.live.log` (the live stream is `paper.live.log` — `with_extension("live.log")`, `main.rs`). Resolve the exact working directory via `systemctl cat pe-service` first (paths are CWD-relative, `config.rs`). |
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
the archived `paper_state.db*` + `paper.log*` files back before starting the service.
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
