#!/usr/bin/env bash
# rank_and_push.sh — THE single, cron-ready entry point for the copy-trade ranking
# pipeline. ONE command, ZERO arguments: it refreshes data, ranks, reranks, and
# publishes the result to Supabase, so "done ranking" always means "in Supabase".
#
# THERE IS EXACTLY ONE RANKER: scripts/rank_72hr_buyandhold.py (buy-and-hold,
# decay-aware, memory-safe inline scan over the full trade universe; the "72hr" in
# the filename is historical — the production TTR ceiling is 48h since the run28
# cutover, docs/_GLOSSARY `ranker_ttr_hours`). pass-2
# (latency_shift_rerank.py) reranks and adds hit_rate; push_ranking_to_supabase.py
# publishes the `latest_ranking` table that pe-service reads. No second ranker, no
# curated-universe pre-gate — the ranker's own filters decide the cohort (#370).
#
# What `bash scripts/rank_and_push.sh` does (no args), in order:
#   Step 0  refresh data (always-on; --skip-discovery / --skip-backfill to bypass):
#     discover     pe-bootstrap winner-discovery  leaderboard + datadash + radion → new wallets
#     backfill     pe-bootstrap backfill          trade history for every active wallet (trades only)
#     events       pe-bootstrap events            condition→event + fee maps (eligibility gate)
#     resolutions  pe-bootstrap resolutions       CLOB→Gamma resolutions + schedule end_dates, run
#                                                  ONCE (fetch_resolutions_and_schedules; the latter
#                                                  is the --scheduled-only TTR clock)
#   Step 0a export  export_trades_parquet.py       trades+maps → Parquet for the DuckDB read-layer
#                                                  (#375; auto/duck engines only; --skip-export bypass)
#   Stage 1  rank    rank_72hr_buyandhold.py  --universe-from-trades  (every wallet w/ trade data)
#   Stage 2  rerank  latency_shift_rerank.py  (adds hit_rate)
#            prune   delete the multi-GB qualifying_positions_72hr.csv once pass-2 has consumed it
#                    (kept: the .txt deliverables, ranked_72hr_buyandhold.csv, latency_shift_ranked.csv;
#                    --keep-intermediates to retain it; only prunes when this run generated it)
#   Stage 3  push    push_ranking_to_supabase.py → Supabase latest_ranking
#   Verify           latest_ranking is now populated.
#   Stage 4  purge   pe-bootstrap purge (#385)  delete proven-loser & dead-weight wallets from
#                    the local cache (DELETE is a no-op unless purge_enabled=true; always reports).
#                    Final + non-fatal (the push already published); --skip-purge to bypass,
#                    auto-skipped under --skip-backfill / --skip-rank (needs a fresh backfill+verdict).
#
# Production defaults are baked in (override via flags): --universe-from-trades,
# HALF_LIFE_DAYS, relative 180d window, band 0.15–0.85, TTR 48h, MinTRL 20 (the run28
# winner shape, 2026-07-03 cutover: --min-trl 20 REPLACES the per-month activity gates,
# which production zeroes), --scheduled-only (drops the resolved-at look-ahead fallback),
# floor_tstat 2.0, top_n 200.
#
# Cron (4h production cadence on the box holding wallet_cache.db; NOT the VPS):
#   The cheap 4-hourly tick re-ranks + pushes from the existing cache:
#     0 */4 * * *  cd <repo> && bash scripts/rank_and_push.sh --skip-discovery --skip-backfill >> data/eval-results/cron.log 2>&1
#   At least ONE full run per day stays MANDATORY (the push aborts if the cache's newest
#   trade is >24h old, and purge only runs on full runs), e.g. replace the 06:00 tick:
#     0 6 * * *    cd <repo> && bash scripts/rank_and_push.sh >> data/eval-results/cron.log 2>&1
#   The PID lock makes an overlapping tick abort (exit 3) — a long full run simply eats
#   the next 4h tick; the following tick recovers.
#
# Requirements on the box:
#   - .env with SUPABASE_URL + SUPABASE_SECRET_KEY (sourced below).
#   - data/wallet_cache.db present (the ranker reads it; Step 0 refreshes it).
#   - target/release/pe-bootstrap built, with its env config — incl.
#     PE_BOOTSTRAP_RADION_API_KEY for the Radion source (#373).
#     Build: cargo build --release -p pe-bootstrap
#
# Research / re-push overrides:
#   --universe <file>     rank a curated wallet file instead of all-trade-wallets.
#   --half-life-days N    override the production decay half-life.
#   --out-dir <dir>       override the auto-timestamped output dir.
#   --bootstrap-config T  pass a BootstrapConfig TOML positional to each Step-0 stage.
#   --skip-discovery      skip Step-0 winner-discovery (new-wallet ingest).
#   --skip-backfill       skip Step-0 backfill + market-data refresh (events/resolutions).
#   --skip-rank           reuse existing CSVs in --out-dir; just (re-)push. The push still
#                         filters against the cache and ABORTS if its newest trade is >24h old
#                         (issue #350 WS3); re-backfill first, or pass --max-cache-staleness-hours.
#   --engine E            ranker engine: auto (default) | duck | sqlite. auto uses the DuckDB
#                         read-layer over a fresh Parquet snapshot (faster scan), else SQLite.
#   --skip-export         reuse an existing Parquet snapshot (skip the Step-0a rewrite).
#   --skip-purge          skip the final Stage-4 cache purge (#385).
#   --keep-intermediates  retain qualifying_positions_72hr.csv (the >5 GB pass-1 intermediate) instead
#                         of auto-pruning it after pass-2; useful for debugging the raw position set.
#   Pure re-push:  --skip-discovery --skip-backfill --skip-rank --out-dir <prior run>

set -euo pipefail
cd "$(dirname "$0")/.."

# ── Defaults (override via flags) ────────────────────────────────────────────────────────
DB="data/wallet_cache.db"
UNIVERSE=""                       # empty => --universe-from-trades (the #370 production default)
OUT_DIR=""                        # empty => auto-timestamped data/eval-results/cron-<UTC>
# Empty window => the Python ranker's RELATIVE defaults apply (win_end = today UTC-midnight,
# win_start = win_end - ranker_window_days(180)); override with --win-start/--win-end (issue #366).
WIN_START=""
WIN_END=""
# Recency decay (issue #366). Production default is 30-day half-life (issue #370, the sweep
# adopted it). Override with --half-life-days N; 0 = flat (legacy, bitwise-identical to no decay).
# Staging note (#370): for the first full-universe run, override with --half-life-days 0 so a
# surprising cohort shift is attributable to the wider universe vs decay; drop the override after.
HALF_LIFE_DAYS="30"
# Shared decay age anchor for BOTH passes; empty => resolved below to UTC-midnight today (= the
# relative win_end) so the two passes weight every trade against the identical anchor.
AS_OF=""
TTR_HOURS="48"
TARGET_N="25"
PRICE_MIN="0.15"
PRICE_MAX="0.85"
FLOOR_TSTAT="2.0"
# MinTRL eligibility (run28 winner shape, 2026-07-03 cutover): >=20 qualifying positions
# in-window REPLACES the per-month activity gates (run28's trl20 axis has no per-month
# component), so production zeroes both. docs/_GLOSSARY `ranker_min_trl`.
MIN_TRL="20"
MIN_AVG_PER_MONTH="0"
MIN_ACTIVE_MONTHS="0"
LATENCY_SHIFT_SECS="20"
FILL_WINDOW_SECS="120"
TOP_N="200"
# DuckDB read-layer (issue #375). Empty => resolved after .env (flag > .env > default).
# ENGINE: auto (DuckDB over a fresh Parquet snapshot, else SQLite) | duck (require it) |
# sqlite (force SQLite, skip the export). SKIP_EXPORT bypasses the Step-0a rewrite.
ENGINE=""
PARQUET_DIR=""
PARQUET_MAX_AGE_HOURS=""
SKIP_EXPORT="0"
# Active-only upload filter (issue #350 WS3). Empty => use push_ranking_to_supabase.py's
# canonical defaults (72 / 24, docs/_GLOSSARY), so the thresholds live in exactly one place.
ACTIVE_WINDOW_HOURS=""
MAX_CACHE_STALENESS_HOURS=""
NOTES=""
SKIP_RANK="0"
SKIP_DISCOVERY="0"
SKIP_BACKFILL="0"
SKIP_PURGE="0"
KEEP_INTERMEDIATES="0"            # 1 => retain the multi-GB qualifying_positions_72hr.csv after pass-2
BOOTSTRAP_CONFIG=""               # optional BootstrapConfig TOML positional for Step-0 stages
PE_BOOTSTRAP_BIN="target/release/pe-bootstrap"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db) DB="$2"; shift 2;;
    --universe) UNIVERSE="$2"; shift 2;;
    --out-dir) OUT_DIR="$2"; shift 2;;
    --win-start) WIN_START="$2"; shift 2;;
    --win-end) WIN_END="$2"; shift 2;;
    --half-life-days) HALF_LIFE_DAYS="$2"; shift 2;;
    --as-of) AS_OF="$2"; shift 2;;
    --ttr-hours) TTR_HOURS="$2"; shift 2;;
    --target-n) TARGET_N="$2"; shift 2;;
    --price-min) PRICE_MIN="$2"; shift 2;;
    --price-max) PRICE_MAX="$2"; shift 2;;
    --floor-tstat) FLOOR_TSTAT="$2"; shift 2;;
    --min-trl) MIN_TRL="$2"; shift 2;;
    --min-avg-per-month) MIN_AVG_PER_MONTH="$2"; shift 2;;
    --min-active-months) MIN_ACTIVE_MONTHS="$2"; shift 2;;
    --latency-shift-secs) LATENCY_SHIFT_SECS="$2"; shift 2;;
    --fill-window-secs) FILL_WINDOW_SECS="$2"; shift 2;;
    --top-n) TOP_N="$2"; shift 2;;
    --active-window-hours) ACTIVE_WINDOW_HOURS="$2"; shift 2;;
    --max-cache-staleness-hours) MAX_CACHE_STALENESS_HOURS="$2"; shift 2;;
    --notes) NOTES="$2"; shift 2;;
    --bootstrap-config) BOOTSTRAP_CONFIG="$2"; shift 2;;
    --skip-rank) SKIP_RANK="1"; shift;;
    --skip-discovery) SKIP_DISCOVERY="1"; shift;;
    --skip-backfill) SKIP_BACKFILL="1"; shift;;
    --skip-purge) SKIP_PURGE="1"; shift;;
    --keep-intermediates) KEEP_INTERMEDIATES="1"; shift;;
    --engine) ENGINE="$2"; shift 2;;
    --parquet-dir) PARQUET_DIR="$2"; shift 2;;
    --parquet-max-age-hours) PARQUET_MAX_AGE_HOURS="$2"; shift 2;;
    --skip-export) SKIP_EXPORT="1"; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

[[ -f .env ]] || { echo "FATAL: .env not found (need SUPABASE_URL + SUPABASE_SECRET_KEY)" >&2; exit 2; }

# Zero-arg cron: default OUT_DIR to a timestamped dir so no flag is ever required.
if [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="data/eval-results/cron-$(date -u +%Y%m%dT%H%M%SZ)"
fi

# Load secrets up front so EVERY stage (incl. the push + verify, which read SUPABASE_URL /
# SUPABASE_SECRET_KEY from the environment) sees them. Sourcing only before the verify step
# would leave the push stage without credentials.
set -a; source .env; set +a

# Neutralize PE_BOOTSTRAP_FETCH_RESOLUTIONS for the `backfill` stage (issue #383). `.env` sets
# it =1 (.env:30) and the `set -a; source .env` above exports it GLOBALLY, so without this unset
# the `backfill` stage's gate (backfill.rs:98 `if config.fetch_resolutions`) fires and runs the
# entire CLOB→Gamma resolutions/schedules refresh — which the explicit `resolutions` stage then
# runs AGAIN unconditionally (main.rs:199), doubling a multi-hour pipeline. Unsetting here makes
# `backfill` trades-only; the `resolutions` stage re-exports the var (parity signal) and runs the
# refresh exactly once. The two refresh call sites are identical (same fetch_resolutions_and_schedules
# over the same cache.all_market_ids), so dropping backfill's copy is byte-for-byte behavior-preserving.
unset PE_BOOTSTRAP_FETCH_RESOLUTIONS

# DuckDB read-layer settings (#375): flag > .env (PE_RANKER_*) > default. Exported so
# BOTH ranker passes (via ranker_duck.get_engine) AND Step-0a below see one source of truth.
ENGINE="${ENGINE:-${PE_RANKER_ENGINE:-auto}}"
PARQUET_DIR="${PARQUET_DIR:-${PE_RANKER_PARQUET_DIR:-data/parquet}}"
PARQUET_MAX_AGE_HOURS="${PARQUET_MAX_AGE_HOURS:-${PE_RANKER_PARQUET_MAX_AGE_HOURS:-4}}"
export PE_RANKER_ENGINE="$ENGINE"
export PE_RANKER_PARQUET_DIR="$PARQUET_DIR"
export PE_RANKER_PARQUET_MAX_AGE_HOURS="$PARQUET_MAX_AGE_HOURS"

# ── Single-run lock (PID-based) ──────────────────────────────────────────────────────────
# A full run (≈496K wallets, 30–90 min) must never overlap the next cron tick. A live holder
# aborts the new run; a stale lock from a crashed run (PID not alive) is reclaimed. The trap
# is set only AFTER we own the lock, so a held-lock abort never deletes the other run's file.
LOCK_FILE="data/eval-results/.rank_and_push.lock"
mkdir -p "$(dirname "$LOCK_FILE")"
if [[ -e "$LOCK_FILE" ]]; then
  LOCK_PID="$(cat "$LOCK_FILE" 2>/dev/null || true)"
  if [[ -n "$LOCK_PID" ]] && kill -0 "$LOCK_PID" 2>/dev/null; then
    echo "FATAL: another rank_and_push.sh is already running (PID $LOCK_PID). Aborting." >&2
    exit 3
  fi
  echo "WARN: reclaiming stale lock (PID ${LOCK_PID:-unknown} not alive)." >&2
fi
echo "$$" > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

mkdir -p "$OUT_DIR"

RANKED_CSV="$OUT_DIR/ranked_72hr_buyandhold.csv"
POSITIONS_CSV="$OUT_DIR/qualifying_positions_72hr.csv"
LATENCY_CSV="$OUT_DIR/latency_shift_ranked.csv"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

# Universe source: explicit --universe <file>, else the production default --universe-from-trades
# (every wallet with trade history; #370). The ranker enforces "exactly one of these", so pass
# exactly one (mirror the WIN_ARGS conditional-array pattern below).
UNIVERSE_ARGS=()
if [[ -n "$UNIVERSE" ]]; then
  UNIVERSE_ARGS+=(--universe "$UNIVERSE")
else
  UNIVERSE_ARGS+=(--universe-from-trades)
fi

# Optional BootstrapConfig TOML positional, threaded to every Step-0 pe-bootstrap stage.
BOOTSTRAP_CONFIG_ARGS=()
[[ -n "$BOOTSTRAP_CONFIG" ]] && BOOTSTRAP_CONFIG_ARGS+=("$BOOTSTRAP_CONFIG")

# Window args only when explicitly overridden (mirror FILTER_ARGS below); empty => the Python
# ranker's relative defaults apply. Pass the IDENTICAL conditional set to both passes.
WIN_ARGS=()
[[ -n "$WIN_START" ]] && WIN_ARGS+=(--win-start "$WIN_START")
[[ -n "$WIN_END" ]] && WIN_ARGS+=(--win-end "$WIN_END")

# Resolve a concrete shared decay anchor once and thread it to BOTH passes so they weight every
# trade against the identical as_of: explicit --as-of wins; else an explicit --win-end; else
# UTC-midnight today (= the ranker's relative win_end via today_midnight_unix()).
if [[ -z "$AS_OF" ]]; then
  AS_OF="${WIN_END:-$(date -u +%Y-%m-%d)}"
fi

# ── Step 0: data refresh ─────────────────────────────────────────────────────────────────
# Run one pe-bootstrap stage with the uniform exit-code convention
# (crates/bootstrap/src/main.rs:21-25): 0 = clean, 2 = partial soft-fail (cache durable,
# failures retried next run) → WARN + continue, 1/other = fatal → abort the whole run.
# NEVER pass --strict: it turns a tolerable partial (2) into a fatal (1). backfill and
# resolutions routinely return 2 at full scale, so swallowing 2 is load-bearing for cron.
run_refresh_stage() {
  local label="$1"; shift
  echo "   [$label] running: $*"
  local rc=0
  "$@" || rc=$?
  case "$rc" in
    0) echo "   [$label] ok" ;;
    2) echo "   [$label] WARN exit 2 (partial); cache durable, continuing" >&2 ;;
    *) echo "   [$label] FATAL exit $rc — aborting run" >&2; exit "$rc" ;;
  esac
}

# Always-on data refresh (discover → backfill → events → resolutions). Each half is
# independently bypassable; when both are skipped the whole step is a no-op (no binary needed).
refresh_data() {
  if [[ "$SKIP_DISCOVERY" == "1" && "$SKIP_BACKFILL" == "1" ]]; then
    echo "── Step 0: data refresh fully skipped (--skip-discovery --skip-backfill) ─────────"
    return 0
  fi

  [[ -x "$PE_BOOTSTRAP_BIN" ]] || {
    echo "FATAL: $PE_BOOTSTRAP_BIN not found/executable. Build: cargo build --release -p pe-bootstrap" >&2
    exit 2
  }

  # Step 0 and the ranker MUST read the SAME cache. pe-bootstrap's config default for
  # cache_path is bare "wallet_cache.db" (crates/bootstrap/src/config.rs:419-420), so force it
  # onto $DB (default data/wallet_cache.db) to prevent Step 0 refreshing a different file than
  # the ranker reads.
  export PE_BOOTSTRAP_CACHE_PATH="$DB"

  echo "── Step 0: data refresh (discover → backfill → events → resolutions) ──"

  if [[ "$SKIP_DISCOVERY" == "1" ]]; then
    echo "   discovery skipped (--skip-discovery)"
  else
    # winner-discovery hits all three sources: leaderboard (errors propagate → fatal),
    # datadash + radion (soft-fail internally → still exit 0). #324/#365/#373.
    run_refresh_stage "winner-discovery" "$PE_BOOTSTRAP_BIN" winner-discovery "${BOOTSTRAP_CONFIG_ARGS[@]}"
  fi

  if [[ "$SKIP_BACKFILL" == "1" ]]; then
    echo "   backfill + market-data skipped (--skip-backfill)"
  else
    run_refresh_stage "backfill" "$PE_BOOTSTRAP_BIN" backfill "${BOOTSTRAP_CONFIG_ARGS[@]}"
    run_refresh_stage "events" "$PE_BOOTSTRAP_BIN" events "${BOOTSTRAP_CONFIG_ARGS[@]}"
    # The explicit `resolutions` subcommand runs the CLOB→Gamma resolutions+schedules pipeline
    # UNCONDITIONALLY (main.rs:199) — it does NOT gate on PE_BOOTSTRAP_FETCH_RESOLUTIONS. `backfill`
    # above runs trades-only because the var was unset right after `source .env` (issue #383); THAT
    # top-level unset is what prevents the double refresh, not the export below. The export is kept
    # only as a docs/26 canonical-invocation parity signal (operator "explicit env" preference) and
    # is bounded by the matching unset so it never leaks past this stage. fetch_resolutions_and_schedules
    # already runs run_schedule_backfill internally (lib.rs:131), so there is no separate `schedules`
    # stage: after `resolutions`, resolved−scheduled is empty and a standalone pass would be a no-op.
    export PE_BOOTSTRAP_FETCH_RESOLUTIONS=1
    run_refresh_stage "resolutions" "$PE_BOOTSTRAP_BIN" resolutions "${BOOTSTRAP_CONFIG_ARGS[@]}"
    unset PE_BOOTSTRAP_FETCH_RESOLUTIONS
  fi
}

refresh_data

# ── Step 0a: Parquet snapshot for the DuckDB read-layer (#375) ────────────────────────────
# Full atomic rewrite from the just-refreshed cache (so the snapshot is fresh for this run).
# Skipped when ranking is skipped, export is skipped, or the engine is forced to sqlite. In
# auto mode an export failure (e.g. duckdb not installed) is non-fatal — get_engine() then
# returns None and both passes fall back to SQLite; with --engine duck it is fatal.
if [[ "$SKIP_RANK" == "0" && "$SKIP_EXPORT" == "0" && "$ENGINE" != "sqlite" ]]; then
  echo "── Step 0a: export Parquet snapshot ($PARQUET_DIR) for the DuckDB read-layer ──"
  if python3 scripts/export_trades_parquet.py --db "$DB" --out-dir "$PARQUET_DIR"; then
    echo "   export ok"
  elif [[ "$ENGINE" == "duck" ]]; then
    echo "FATAL: --engine duck but the Parquet export failed" >&2; exit 1
  else
    echo "   WARN: Parquet export failed (engine=auto) — ranker will use SQLite" >&2
  fi
else
  echo "── Step 0a: Parquet export skipped (engine=$ENGINE skip_rank=$SKIP_RANK skip_export=$SKIP_EXPORT) ──"
fi

if [[ "$SKIP_RANK" == "0" ]]; then
  echo "── Stage 1/3: pass-1 edge-floor ranking ──────────────────────────────────────"
  python3 scripts/rank_72hr_buyandhold.py \
    --db "$DB" "${UNIVERSE_ARGS[@]}" --out-dir "$OUT_DIR" \
    "${WIN_ARGS[@]}" --ttr-hours "$TTR_HOURS" \
    --half-life-days "$HALF_LIFE_DAYS" --as-of "$AS_OF" \
    --target-n "$TARGET_N" --price-min "$PRICE_MIN" --price-max "$PRICE_MAX" \
    --min-trl "$MIN_TRL" --min-avg-per-month "$MIN_AVG_PER_MONTH" \
    --min-active-months "$MIN_ACTIVE_MONTHS" \
    --floor-tstat "$FLOOR_TSTAT" --scheduled-only

  echo "── Stage 2/3: pass-2 latency-shift rerank (adds hit_rate) ─────────────────────"
  python3 scripts/latency_shift_rerank.py \
    --db "$DB" --ranked-csv "$RANKED_CSV" --positions-csv "$POSITIONS_CSV" \
    --out-dir "$OUT_DIR" \
    --latency-shift-secs "$LATENCY_SHIFT_SECS" --fill-window-secs "$FILL_WINDOW_SECS" \
    --half-life-days "$HALF_LIFE_DAYS" --as-of "$AS_OF" \
    --min-trl "$MIN_TRL" --min-avg-per-month "$MIN_AVG_PER_MONTH" \
    --min-active-months "$MIN_ACTIVE_MONTHS" \
    --floor-tstat "$FLOOR_TSTAT"
else
  echo "── Stages 1-2 skipped (--skip-rank); reusing $LATENCY_CSV ──"
fi

[[ -s "$LATENCY_CSV" ]] || { echo "FATAL: ranking produced no $LATENCY_CSV" >&2; exit 1; }

# Auto-prune the multi-GB pass-1 intermediate now that pass-2 has consumed it. qualifying_positions_72hr.csv
# (often >5 GB) is the raw per-(wallet,market) first-buy extraction read ONLY by latency_shift_rerank.py
# (`--positions-csv` above); nothing downstream — push, purge, or a later --skip-rank re-push — reads it.
# The small .txt deliverables, ranked_72hr_buyandhold.csv (the purge decision + per-wallet audit), and
# latency_shift_ranked.csv (the push input, needed for re-push) are kept. Only prune when this run actually
# generated it (SKIP_RANK=0); --keep-intermediates retains it for debugging.
if [[ "$SKIP_RANK" == "0" && "$KEEP_INTERMEDIATES" == "0" && -f "$POSITIONS_CSV" ]]; then
  pos_sz="$(du -h "$POSITIONS_CSV" 2>/dev/null | cut -f1)"
  rm -f "$POSITIONS_CSV"
  echo "── Pruned pass-1 intermediate: $POSITIONS_CSV (${pos_sz:-?} freed; --keep-intermediates to retain) ──"
fi

echo "── Stage 3/3: push to Supabase (the previously-missing step) ──────────────────"
# Active-only upload filter args (issue #350 WS3): always pass --db; window/staleness only
# when explicitly overridden, so the canonical defaults stay solely in the push script.
FILTER_ARGS=(--db "$DB")
[[ -n "$ACTIVE_WINDOW_HOURS" ]] && FILTER_ARGS+=(--active-window-hours "$ACTIVE_WINDOW_HOURS")
[[ -n "$MAX_CACHE_STALENESS_HOURS" ]] && FILTER_ARGS+=(--max-cache-staleness-hours "$MAX_CACHE_STALENESS_HOURS")

# TTR provenance: pass the actual ranking TTR ceiling so ranking_batches.ttr_max_secs
# reflects the shape the entries were ranked at (the script default would silently
# record 72h after the 48h cutover). Floor stays pass-1's --min-ttr-hours default (30s).
TTR_MAX_SECS="$(python3 -c "print(int(float('$TTR_HOURS')*3600))")"

python3 scripts/push_ranking_to_supabase.py \
  --ranked-csv "$LATENCY_CSV" --top-n "$TOP_N" \
  --band-lo "$PRICE_MIN" --band-hi "$PRICE_MAX" \
  --ttr-max-secs "$TTR_MAX_SECS" \
  --latency-shift-secs "$LATENCY_SHIFT_SECS" \
  "${FILTER_ARGS[@]}" \
  --git-sha "$GIT_SHA" --notes "${NOTES:-rank_and_push.sh $GIT_SHA}"

echo "── Verify: Supabase latest_ranking is now populated ──────────────────────────"
python3 - <<'PY'
import os, urllib.request
base=os.environ["SUPABASE_URL"]; key=os.environ["SUPABASE_SECRET_KEY"]
req=urllib.request.Request(f"{base}/rest/v1/latest_ranking?select=rank&limit=1",
    headers={"apikey":key,"Authorization":f"Bearer {key}","Prefer":"count=exact","Range":"0-0"})
with urllib.request.urlopen(req, timeout=20) as r:
    cr=r.headers.get("Content-Range","?")
n=cr.split("/")[-1]
print(f"latest_ranking rows: {n}")
raise SystemExit(0 if n.isdigit() and int(n)>0 else 1)
PY
echo "✓ rank_and_push complete — Supabase populated. pe-service picks it up within one refresh interval."

# ── Stage 4/4 (final): purge proven-loser & dead-weight wallets from the local cache (issue #385) ──
# Opt-in: the DELETE is a no-op unless purge_enabled=true in .env; the stage still emits a would-purge
# report every run. Runs AFTER the push/verify so a purge failure can never block the (already-complete)
# Supabase publish — hence non-fatal here. Skipped when --skip-purge, or when this run did not produce a
# fresh backfill + verdict CSV (--skip-backfill / --skip-rank): purge relies on a fresh backfill (it
# refuses an armed run on a stale cache) and the current run's RANKED_CSV verdict.
if [[ "$SKIP_PURGE" == "1" ]]; then
  echo "── Stage 4/4: purge skipped (--skip-purge) ───────────────────────────────────"
elif [[ "$SKIP_BACKFILL" == "1" || "$SKIP_RANK" == "1" ]]; then
  echo "── Stage 4/4: purge skipped (needs a fresh backfill + verdict this run) ───────"
else
  echo "── Stage 4/4: purge proven-loser & dead-weight wallets (issue #385) ───────────"
  prc=0
  PE_BOOTSTRAP_CACHE_PATH="$DB" PE_BOOTSTRAP_PURGE_DECISION_CSV="$RANKED_CSV" \
    "$PE_BOOTSTRAP_BIN" purge "${BOOTSTRAP_CONFIG_ARGS[@]}" || prc=$?
  case "$prc" in
    0) echo "   [purge] ok" ;;
    *) echo "   [purge] WARN exit $prc — purge stage failed; Supabase publish already complete, continuing" >&2 ;;
  esac
fi
