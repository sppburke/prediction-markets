#!/usr/bin/env bash
# reset_paper_state.sh — orchestrate the paper-P&L archive-then-reset
# (2026-07-03 single-system cutover; full runbook: docs/34-PAPER-PNL-RESET-RUNBOOK.md).
#
# DEFAULT IS DRY-RUN: prints live row counts and the exact step sequence, mutates
# nothing. Pass --execute to run the Supabase archive+reset (step 2 below). The VPS
# file rotation and service restart are deliberately NOT automated here — the service
# stop/start needs interactive sudo (ops gotcha) and the file moves must happen while
# the service is STOPPED; this script prints the exact commands instead.
#
# Sequence (docs/34):
#   1. STOP pe-service on the VPS                                (manual, sudo)
#   2. Supabase archive+delete (this script, --execute)          (fail-closed, 1 txn)
#   3. VPS: rotate paper_state.db(+-wal/-shm), paper.log,        (manual/ssh, service
#      live_journal.log into an archive dir                       stopped!)
#   4. VPS: set the fresh bankroll (PE_BANKROLL_USD / .env),
#      then `pe-service --backfill-supabase` re-seeds paper_bankroll
#   5. START pe-service                                          (manual, sudo)
#   6. REFRESH MATERIALIZED VIEW CONCURRENTLY wallet_live_stats_mv (this script prints
#      the psql one-liner; pg_cron self-heals within 2 min anyway)
#
# WHY the event log MUST rotate with the DB (step 3): a fresh SQLite resets both
# replay watermarks; an un-rotated paper.log would be fully replayed into SQLite AND
# re-inserted into the emptied Supabase paper_fills via catch_up_supabase, re-debiting
# the fresh bankroll. Archive them together or the reset silently undoes itself.

set -euo pipefail
cd "$(dirname "$0")/../.."

MODE="dry-run"
[[ "${1:-}" == "--execute" ]] && MODE="execute"

# .env carries SUPABASE_DB_URL (the IPv4 session pooler; LOCAL box only, not the VPS).
if [[ -f .env ]]; then set -a; # shellcheck disable=SC1091
  source .env; set +a; fi
: "${SUPABASE_DB_URL:?SUPABASE_DB_URL missing — set it in .env (local checkout only)}"

command -v psql >/dev/null || { echo "FATAL: psql not installed" >&2; exit 1; }

echo "── Live paper-state row counts (Supabase) ──────────────────────────────────"
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -At <<'SQL'
select 'paper_fills            ' || count(*) from paper_fills
union all select 'settled_markets        ' || count(*) from settled_markets
union all select 'paper_positions        ' || count(*) from paper_positions
union all select 'paper_bankroll         ' || count(*) || '  (bankroll=' ||
  coalesce((select bankroll_str from paper_bankroll where id = 0), 'ABSENT') || ')'
union all select 'fill_market_snapshots  ' || count(*) from fill_market_snapshots;
SQL

if [[ "$MODE" == "dry-run" ]]; then
  cat <<'EOT'

── DRY-RUN (nothing mutated). The full sequence: ────────────────────────────
 1. STOP the service (interactive sudo, run yourself):
      ssh -t -i ~/.ssh/id_personal sean@82.22.32.225 'sudo systemctl stop pe-service'
 2. Archive + reset Supabase:
      bash scripts/paper_reset/reset_paper_state.sh --execute
 3. Rotate the VPS local state (service MUST be stopped; adjust paths per
    `systemctl cat pe-service` WorkingDirectory):
      ssh -i ~/.ssh/id_personal sean@82.22.32.225 '
        set -e; cd <service-workdir> &&
        mkdir -p archive-$(date +%Y%m%d) &&
        for f in paper_state.db paper_state.db-wal paper_state.db-shm \
                 paper.log live_journal.log; do
          if [ -e "$f" ]; then mv "$f" archive-$(date +%Y%m%d)/; fi
        done &&
        ls -la archive-$(date +%Y%m%d)/'
    (the -wal/-shm files may legitimately be absent after a clean checkpoint; the
     loop moves what exists and FAILS LOUDLY on a real mv error — verify the ls
     shows at least paper_state.db AND paper.log before proceeding)
 4. Re-seed the fresh bankroll (still on the VPS, service stopped):
      confirm PE_BANKROLL_USD (or bankroll_usd) in the service env = the fresh
      starting value, then run the service binary once with --backfill-supabase.
      Update the service_config `bankroll_usd` row too (bookkeeping mirror only; #516).
 5. START the service:
      ssh -t -i ~/.ssh/id_personal sean@82.22.32.225 'sudo systemctl start pe-service'
 6. Refresh the dashboard matview immediately (optional; pg_cron does it ≤2 min):
      psql "$SUPABASE_DB_URL" -c 'refresh materialized view concurrently wallet_live_stats_mv;'
EOT
  exit 0
fi

echo
echo "── EXECUTE: fail-closed archive + reset (single transaction) ───────────────"
read -r -p "Type RESET to archive-and-clear the Supabase paper state: " confirm
[[ "$confirm" == "RESET" ]] || { echo "aborted (no mutation)"; exit 1; }

psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f scripts/paper_reset/archive_paper_state.sql

echo
echo "── Post-reset verification ──────────────────────────────────────────────────"
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -At <<'SQL'
select 'live paper_fills       ' || count(*) from paper_fills
union all select 'archived paper_fills   ' || count(*) from paper_fills_archive
union all select 'live paper_bankroll    ' || count(*) from paper_bankroll
union all select 'sink hwm rows          ' || count(*) from supabase_sink_hwm;
SQL
echo "Supabase reset complete. Continue with the VPS rotation (steps 3-6 above / docs/34)."
