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
#     discover     pe-bootstrap winner-discovery --defer-activation  ingest without bulk activation
#     activate     pe-bootstrap activate-next     at most the next audited 20,000 non-infra wallets
#     backfill     pe-bootstrap backfill --defer-activation  trade history for every active wallet
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
#   Stage 3  publish push_ranking_to_supabase.py records the exact request, then
#                    atomically/idempotently publishes it through the Supabase RPC.
#   Verify           that exact batch and contiguous rank set are latest_ranking.
#
# Production defaults are baked in (override via flags): --universe-from-trades,
# HALF_LIFE_DAYS, relative 180d window, band 0.15–0.85, TTR 48h, MinTRL 20 (the run28
# winner shape, 2026-07-03 cutover: --min-trl 20 REPLACES the per-month activity gates,
# which production zeroes), --scheduled-only (drops the resolved-at look-ahead fallback),
# floor_tstat 2.0, top_n 200.
#
# Continuous production execution is owned by scripts/rank_and_push_loop.sh.
# A zero-argument run persists its logical run directory before activation. A
# pre-publication retry reuses that directory and therefore the same audited
# activation batch; a pending exact publication request takes precedence.
# Do not run a cron/timer copy alongside that supervisor.
#
# Requirements on the box:
#   - .env with SUPABASE_URL + SUPABASE_SECRET_KEY (sourced below).
#   - data/wallet_cache.db present (the ranker reads it; Step 0 refreshes it).
#   - Python dependencies installed in .venv-analysis (preferred) or .venv. Set
#     PE_PYTHON to an executable path to override the repository-local interpreter.
#     Install: <venv>/bin/python3 -m pip install -r scripts/requirements.txt
#   - target/release/pe-bootstrap built, with its env config.
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
#   --cache-stage-record P enable schema-two cutover: rank finalized side --db,
#                         prepare publication, activate onto --fixed-db, then resume it.
#   --fixed-db P          installed Forge cache path for schema-two cutover.
#   --prior-cache-backup P one generic hash-bound prior-main backup path.
#   --skip-purge          accepted as a backward-compatible no-op; automatic purge is retired (#544).
#   --keep-intermediates  retain qualifying_positions_72hr.csv (the >5 GB pass-1 intermediate) instead
#                         of auto-pruning it after pass-2; useful for debugging the raw position set.
#   Pure re-push:  --skip-discovery --skip-backfill --skip-rank --out-dir <prior run>
#
# Schema-two private-candidate lane (#588): a zero-argument production cycle whose
# installed cache is already schema two, or whose .env sets PE_RANK_SCHEMA_TWO_CUTOVER=1
# while it is still schema one, replaces Step 0 with: stage one private
# candidate beside the physical fixed file (cache-stage-v2) → migrate the
# initial schema-one candidate once (cache-migrate-v2) → discover → activate →
# collect complete activity for the union of current acquisition candidates and
# retained histories (cache-populate-activity-v2 --fresh-generation) → payout →
# cache-finalize-v2 → the existing cutover path (rank, targeted prices, re-finalize,
# prepare-only publication, cache-activate, exact resume). Legacy backfill/events/
# resolutions read retired tables and do not run in this lane. The lane is frozen
# in the cycle configuration, so a resumed cycle keeps its lane.

set -euo pipefail
cd "$(dirname "$0")/.."
INVOCATION_ARGC=$#

command -v flock >/dev/null 2>&1 || {
  echo "FATAL: required command is unavailable: flock" >&2
  exit 2
}

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
# component), so production zeroes both. docs/_GLOSSARY `ranker_prod_min_trl`.
MIN_TRL="20"
MIN_AVG_PER_MONTH="0"
MIN_ACTIVE_MONTHS="0"
# Δ = measured websocket-path copy latency, conservatively rounded (#530: feed p95 1.32s
# + book fetch 64ms + commit 163ms ≈ 1.6s → 2s; sweep at Δ=2: 290 statistical / 12 active
# survivors vs 185/6 at the old 20s). Ships with the websocket input path under mandatory
# service-first deploy ordering; RE-CHECK against the +1-week measured span artifact and
# raise only if measured p95 > 2s (issue #530).
LATENCY_SHIFT_SECS="2"
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
RESUME_PENDING="0"                # internal supervisor recovery path; never starts a new cohort
PENDING_FILE="data/eval-results/rank_and_push.pending"
CYCLE_FILE="data/eval-results/rank_and_push.cycle"
PRODUCTION_CYCLE="0"
CACHE_STAGE_RECORD=""
FIXED_DB=""
PRIOR_CACHE_BACKUP=""

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
    --cache-stage-record) CACHE_STAGE_RECORD="$2"; shift 2;;
    --fixed-db) FIXED_DB="$2"; shift 2;;
    --prior-cache-backup) PRIOR_CACHE_BACKUP="$2"; shift 2;;
    --resume-pending) RESUME_PENDING="1"; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

[[ -f .env ]] || { echo "FATAL: .env not found (need SUPABASE_URL + SUPABASE_SECRET_KEY)" >&2; exit 2; }

# A direct zero-argument production invocation must honor the exact publication
# recovery request just like the supervisor. Never rebuild the same cycle while
# an already-materialized request is pending.
if [[ "$INVOCATION_ARGC" -eq 0 && ( -e "$PENDING_FILE" || -L "$PENDING_FILE" ) ]]; then
  RESUME_PENDING="1"
  echo "RANK_AND_PUSH_AUTO_RESUME_PENDING=$PENDING_FILE"
fi

if [[ "$RESUME_PENDING" == "1" && "$INVOCATION_ARGC" -gt 1 ]]; then
  echo "FATAL: --resume-pending cannot be combined with other arguments" >&2; exit 2
fi

# The internal recovery path accepts no other flags and follows only an atomically-written
# production pointer. Resolve + validate it before creating any output or taking the run lock.
if [[ "$RESUME_PENDING" == "1" && ( -e "$PENDING_FILE" || -L "$PENDING_FILE" ) ]]; then
  if [[ "$INVOCATION_ARGC" -ne 0 && "$INVOCATION_ARGC" -ne 1 ]]; then
    echo "FATAL: --resume-pending cannot be combined with other arguments" >&2
    exit 2
  fi
  [[ -f "$PENDING_FILE" && ! -L "$PENDING_FILE" ]] || {
    echo "FATAL: pending publication pointer is missing or not a regular file: $PENDING_FILE" >&2
    exit 2
  }
  mapfile -t PENDING_LINES < "$PENDING_FILE"
  if [[ "${#PENDING_LINES[@]}" -ne 1 || -z "${PENDING_LINES[0]}" ]]; then
    echo "FATAL: $PENDING_FILE must contain exactly one non-empty request path" >&2
    exit 2
  fi
  PENDING_REQUEST="${PENDING_LINES[0]}"
  [[ "$PENDING_REQUEST" != /* ]] || {
    echo "FATAL: pending publication request path must be repository-relative" >&2
    exit 2
  }
  PENDING_REQUEST_REAL="$(readlink -f -- "$PENDING_REQUEST" 2>/dev/null || true)"
  REPO_REAL="$(pwd -P)"
  case "$PENDING_REQUEST_REAL" in
    "$REPO_REAL"/data/eval-results/cron-*/ranking_publish_request.json) ;;
    *)
      echo "FATAL: pending publication request escaped the production cron directory" >&2
      exit 2
      ;;
  esac
  [[ -f "$PENDING_REQUEST_REAL" && ! -L "$PENDING_REQUEST_REAL" ]] || {
    echo "FATAL: pending publication request is missing or not a regular file" >&2
    exit 2
  }
  OUT_DIR="$(dirname "$PENDING_REQUEST_REAL")"
  SKIP_DISCOVERY="1"
  SKIP_BACKFILL="1"
  SKIP_RANK="1"
  SKIP_EXPORT="1"
  echo "RANK_AND_PUSH_RESUME_REQUEST=$PENDING_REQUEST"
elif [[ "$INVOCATION_ARGC" -eq 0 || "$RESUME_PENDING" == "1" ]]; then
  PRODUCTION_CYCLE="1"
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

# ── Repository Python runtime + fail-fast dependency preflight ───────────────────────────
# Cron, SSH, and nohup commonly start with a minimal PATH. Never let their ambient `python3`
# choose the analytics environment: an incomplete system interpreter can otherwise fail only
# after the multi-hour refresh has finished. PE_PYTHON is an executable path (not a shell
# command); absent an override, use a repository-local environment in canonical order.
PYTHON_BIN=""
if [[ -n "${PE_PYTHON:-}" ]]; then
  if [[ "$PE_PYTHON" == /* ]]; then
    PYTHON_BIN="$PE_PYTHON"
  else
    PYTHON_BIN="$PWD/$PE_PYTHON"
  fi
  [[ -x "$PYTHON_BIN" ]] || {
    echo "FATAL: PE_PYTHON is not executable: $PYTHON_BIN" >&2
    exit 2
  }
else
  for candidate in "$PWD/.venv-analysis/bin/python3" "$PWD/.venv/bin/python3"; do
    if [[ -x "$candidate" ]]; then
      PYTHON_BIN="$candidate"
      break
    fi
  done
  [[ -n "$PYTHON_BIN" ]] || {
    echo "FATAL: no repository Python found (.venv-analysis/bin/python3 or .venv/bin/python3)." >&2
    echo "Create one and install: <venv>/bin/python3 -m pip install -r scripts/requirements.txt" >&2
    exit 2
  }
fi

# sqlite3 and urllib.request are required by the push/verify/checkpoint path even for a pure
# re-push. Full ranking additionally imports numpy and pandas unconditionally. Import the real
# modules (rather than only inspecting package metadata) so broken native wheels fail here too.
PYTHON_MODULES=(sqlite3 urllib.request)
if [[ "$SKIP_RANK" == "0" ]]; then
  PYTHON_MODULES+=(numpy pandas)
fi
if ! "$PYTHON_BIN" - "${PYTHON_MODULES[@]}" <<'PY'
import importlib
import sys

failures = []
for module_name in sys.argv[1:]:
    try:
        importlib.import_module(module_name)
    except Exception as error:  # the operator needs the concrete broken import
        failures.append(f"{module_name}: {type(error).__name__}: {error}")

if failures:
    print("Python dependency preflight failed:", file=sys.stderr)
    for failure in failures:
        print(f"  - {failure}", file=sys.stderr)
    raise SystemExit(1)
PY
then
  echo "FATAL: Python dependency preflight failed for $PYTHON_BIN" >&2
  echo "Install: $PYTHON_BIN -m pip install -r scripts/requirements.txt" >&2
  exit 2
fi

# DuckDB stays optional in auto mode (the reviewed SQLite fallback) and irrelevant in sqlite
# mode. Forced duck mode cannot work without it, so fail before refresh rather than hours later.
if [[ "$SKIP_RANK" == "0" && "$ENGINE" != "sqlite" ]]; then
  if "$PYTHON_BIN" -c 'import duckdb' 2>/dev/null; then
    echo "Python runtime: $PYTHON_BIN (dependency preflight ok; DuckDB available)"
  elif [[ "$ENGINE" == "duck" ]]; then
    echo "FATAL: --engine duck requires duckdb in $PYTHON_BIN" >&2
    echo "Install: $PYTHON_BIN -m pip install -r scripts/requirements.txt" >&2
    exit 2
  else
    echo "Python runtime: $PYTHON_BIN (dependency preflight ok)"
    echo "WARN: duckdb is unavailable; engine=auto will fall back to SQLite" >&2
  fi
else
  echo "Python runtime: $PYTHON_BIN (dependency preflight ok)"
fi

# Fail before the run lock, logical-cycle directory, or pointer when this
# invocation will need the Rust cache mutator. A pure research re-push can still
# omit the binary because every bootstrap stage is skipped.
if [[ "$SKIP_DISCOVERY" == "0" || "$SKIP_BACKFILL" == "0" || "$SKIP_RANK" == "0" ]]; then
  [[ -x "$PE_BOOTSTRAP_BIN" ]] || {
    echo "FATAL: $PE_BOOTSTRAP_BIN not found/executable. Build: cargo build --release -p pe-bootstrap" >&2
    exit 2
  }
fi

# ── Single-run kernel lock ────────────────────────────────────────────────────────────────
# Keep one persistent inode so shell flock and Rust fs2 contenders coordinate on the same
# kernel lock. The PID is diagnostic only and is written after acquisition (#544).
LOCK_FILE="data/eval-results/.rank_and_push.lock"
mkdir -p "$(dirname "$LOCK_FILE")"
exec 8<>"$LOCK_FILE"
if ! flock -n 8; then
  LOCK_PID="$(tr -cd '0-9' < "$LOCK_FILE" 2>/dev/null || true)"
  echo "FATAL: another rank_and_push.sh is already running (PID ${LOCK_PID:-unknown}). Aborting." >&2
  exit 3
fi
printf '%s\n' "$$" > "$LOCK_FILE"
CYCLE_TMP=""
PIPELINE_VERSIONS_TMP=""
CYCLE_CONFIG_TMP=""
CURRENT_CYCLE_TMP=""
cleanup_rank_and_push() {
  [[ -z "$CYCLE_TMP" ]] || rm -f "$CYCLE_TMP"
  [[ -z "$PIPELINE_VERSIONS_TMP" ]] || rm -f "$PIPELINE_VERSIONS_TMP"
  [[ -z "$CYCLE_CONFIG_TMP" ]] || rm -f "$CYCLE_CONFIG_TMP"
  [[ -z "$CURRENT_CYCLE_TMP" ]] || rm -f "$CURRENT_CYCLE_TMP"
}
trap cleanup_rank_and_push EXIT

validate_cycle_pointer() {
  [[ -f "$CYCLE_FILE" && ! -L "$CYCLE_FILE" ]] || {
    echo "FATAL: cycle pointer is missing or not a regular file: $CYCLE_FILE" >&2
    return 2
  }
  local -a cycle_lines=()
  mapfile -t cycle_lines < "$CYCLE_FILE"
  if [[ "${#cycle_lines[@]}" -ne 1 || -z "${cycle_lines[0]}" ]]; then
    echo "FATAL: $CYCLE_FILE must contain exactly one non-empty run directory" >&2
    return 2
  fi
  local cycle_dir="${cycle_lines[0]}"
  if [[ ! "$cycle_dir" =~ ^data/eval-results/cron-[0-9]{8}T[0-9]{6}Z$ ]]; then
    echo "FATAL: cycle pointer must name one repository-relative production cron directory" >&2
    return 2
  fi
  local cycle_real
  cycle_real="$(readlink -f -- "$cycle_dir" 2>/dev/null || true)"
  local repo_real
  repo_real="$(pwd -P)"
  case "$cycle_real" in
    "$repo_real"/data/eval-results/cron-*) ;;
    *)
      echo "FATAL: cycle pointer escaped the production cron directory" >&2
      return 2
      ;;
  esac
  [[ -d "$cycle_real" && ! -L "$cycle_dir" ]] || {
    echo "FATAL: cycle run directory is missing or not a real directory: $cycle_dir" >&2
    return 2
  }
  printf '%s' "$cycle_dir"
}

# Resume pointerless, verified retirement before any new cycle or unchanged-day
# exit. The durable accepted watermark plus request/staging evidence owns this
# obligation; a pause defers the pass without admitting another candidate.
if [[ "$PRODUCTION_CYCLE" == "1" && ! -e "$CYCLE_FILE" && ! -L "$CYCLE_FILE" &&
      ! -e "$PENDING_FILE" && ! -L "$PENDING_FILE" ]]; then
  retention_rc=0
  "$PYTHON_BIN" scripts/rank_cycle_manifest.py retire-completed \
    --root data/eval-results || retention_rc=$?
  case "$retention_rc" in
    0)
      if [[ "$RESUME_PENDING" == "1" ]]; then
        echo "✓ completed publication retention synchronized."
        exit 0
      fi
      ;;
    2) exit 0 ;; # A pause holds cleanup and admission until a later loop pass.
    3) ;; # No completed candidate cycle; continue ordinary recovery/admission.
    *) exit "$retention_rc" ;;
  esac
fi

# Recover the crash seam between durable request creation and pointer creation.
# Validate through the publisher before its existing atomic pointer writer runs.
if [[ "$PRODUCTION_CYCLE" == "1" && ( -e "$CYCLE_FILE" || -L "$CYCLE_FILE" ) ]]; then
  recovery_cycle="$(validate_cycle_pointer)" || exit $?
  recovery_request="$recovery_cycle/ranking_publish_request.json"
  if [[ -e "$recovery_request" || -L "$recovery_request" ]]; then
    [[ -f "$recovery_request" && ! -L "$recovery_request" ]] || {
      echo "FATAL: durable cycle request is not a regular file" >&2; exit 2;
    }
    "$PYTHON_BIN" scripts/push_ranking_to_supabase.py --validate-request "$recovery_request" > /dev/null
    "$PYTHON_BIN" - "$PENDING_FILE" "$recovery_request" <<'PY'
import sys
sys.path.insert(0, "scripts")
from push_ranking_to_supabase import save_pending_pointer
save_pending_pointer(sys.argv[1], sys.argv[2])
PY
    OUT_DIR="$recovery_cycle"
    RESUME_PENDING="1"
    PRODUCTION_CYCLE="0"
    SKIP_DISCOVERY="1"; SKIP_BACKFILL="1"; SKIP_RANK="1"; SKIP_EXPORT="1"
    echo "RANK_AND_PUSH_RECOVERED_REQUEST=$recovery_request"
  fi
fi
if [[ "$RESUME_PENDING" == "1" && "$PRODUCTION_CYCLE" == "1" ]]; then
  echo "FATAL: no pending pointer or durable cycle request to recover" >&2; exit 2
fi

# The schema-two candidate lane (#588) is a cycle property: chosen when the cycle
# is created and read back from its frozen configuration on every resume.
FRESH_LANE="0"
if [[ "$PRODUCTION_CYCLE" == "1" ]]; then
  if [[ -e "$CYCLE_FILE" || -L "$CYCLE_FILE" ]]; then
    OUT_DIR="$(validate_cycle_pointer)" || exit $?
    [[ -f "$OUT_DIR/cycle_manifest.json" && ! -L "$OUT_DIR/cycle_manifest.json" ]] || {
      echo "FATAL: resumed production cycle omitted its frozen cycle manifest" >&2
      exit 2
    }
    FRESH_LANE="$("$PYTHON_BIN" -c 'import json, sys
configuration = json.load(open(sys.argv[1], encoding="utf-8"))
print(1 if configuration.get("cache_lane") == "fresh_v2" else 0)' "$OUT_DIR/cycle_configuration.json")"
    echo "RANK_AND_PUSH_CYCLE_RESUME=$OUT_DIR"
  else
    INSTALLED_SCHEMA="$("$PYTHON_BIN" -c 'import sqlite3, sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as connection:
    schema = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if schema == -2:
        raise SystemExit("unfinished bulk root; resume cache-populate-activity-v2 --bulk-root before ranking")
    print(schema)' "$DB")"
    case "${PE_RANK_SCHEMA_TWO_CUTOVER:-0}" in
      0|1|prepare) ;;
      *) echo "FATAL: PE_RANK_SCHEMA_TWO_CUTOVER must be 0, 1, or prepare" >&2; exit 2;;
    esac
    if [[ "$INSTALLED_SCHEMA" == "2" || "${PE_RANK_SCHEMA_TWO_CUTOVER:-0}" != "0" ]]; then
      FRESH_LANE="1"
    fi
    PIPELINE_VERSIONS_TMP="data/eval-results/.pipeline_versions.$$.json"
    CYCLE_CONFIG_TMP="data/eval-results/.cycle_configuration.$$.json"
    CURRENT_CYCLE_TMP="data/eval-results/.cycle_manifest.$$.json"
    "$PE_BOOTSTRAP_BIN" pipeline-versions > "$PIPELINE_VERSIONS_TMP"
    "$PYTHON_BIN" - "$CYCLE_CONFIG_TMP" \
      "$HALF_LIFE_DAYS" "$TTR_HOURS" "$PRICE_MIN" "$PRICE_MAX" "$FLOOR_TSTAT" \
      "$MIN_TRL" "$MIN_AVG_PER_MONTH" "$MIN_ACTIVE_MONTHS" "$LATENCY_SHIFT_SECS" \
      "$FILL_WINDOW_SECS" "$TOP_N" "$FRESH_LANE" <<'PY'
import json
import sys

keys = [
    "half_life_days", "ttr_hours", "price_min", "price_max", "floor_tstat",
    "min_trl", "min_avg_per_month", "min_active_months", "latency_shift_secs",
    "fill_window_secs", "top_n",
]
configuration = dict(zip(keys, sys.argv[2:-1], strict=True))
# The lane is part of the daily watermark identity: switching lanes is a
# configuration change, not an unchanged day. Legacy bytes stay identical.
if sys.argv[-1] == "1":
    configuration["cache_lane"] = "fresh_v2"
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(configuration, handle, sort_keys=True)
    handle.write("\n")
PY
    CYCLE_DAY_UTC="$(date -u +%Y-%m-%d)"
    "$PYTHON_BIN" scripts/rank_cycle_manifest.py capture \
      --db "$DB" --day-utc "$CYCLE_DAY_UTC" \
      --versions-file "$PIPELINE_VERSIONS_TMP" \
      --configuration-file "$CYCLE_CONFIG_TMP" --output "$CURRENT_CYCLE_TMP"
    # Schema-one partial active wallets force a retry even at an unchanged watermark.
    if "$PYTHON_BIN" scripts/rank_cycle_manifest.py unchanged \
      --db "$DB" --current "$CURRENT_CYCLE_TMP" --root data/eval-results; then
      echo "RANK_AND_PUSH_UNCHANGED_DAILY_WATERMARK=1"
      exit 0
    fi
    OUT_DIR="data/eval-results/cron-$(date -u +%Y%m%dT%H%M%SZ)"
    mkdir -p "$OUT_DIR"
    mv -f -- "$CURRENT_CYCLE_TMP" "$OUT_DIR/cycle_manifest.json"
    CURRENT_CYCLE_TMP=""
    cp -- "$PIPELINE_VERSIONS_TMP" "$OUT_DIR/pipeline_versions.json"
    cp -- "$CYCLE_CONFIG_TMP" "$OUT_DIR/cycle_configuration.json"
    CYCLE_TMP="${CYCLE_FILE}.tmp.$$"
    printf '%s\n' "$OUT_DIR" > "$CYCLE_TMP"
    mv -f -- "$CYCLE_TMP" "$CYCLE_FILE"
    CYCLE_TMP=""
    echo "RANK_AND_PUSH_CYCLE_CREATED=$OUT_DIR"
  fi
elif [[ -z "$OUT_DIR" ]]; then
  OUT_DIR="data/eval-results/cron-$(date -u +%Y%m%dT%H%M%SZ)"
fi

mkdir -p "$OUT_DIR"
echo "RANK_AND_PUSH_RUN_DIR=$OUT_DIR"

CUTOVER_MODE="0"
BEFORE_RANKING_JSON=""
[[ -z "$CACHE_STAGE_RECORD" ]] || CUTOVER_MODE="1"

# Validate the finalized candidate and snapshot the current publication. Runs
# after Step 0 because the candidate lane finalizes its candidate there.
require_cutover_inputs() {
  [[ -f "$CACHE_STAGE_RECORD" && ! -L "$CACHE_STAGE_RECORD" ]] || {
    echo "FATAL: --cache-stage-record must be a regular file" >&2; exit 2;
  }
  [[ -n "$FIXED_DB" && -f "$FIXED_DB" ]] || {
    echo "FATAL: schema-two cutover requires an existing --fixed-db" >&2; exit 2;
  }
  [[ -n "$PRIOR_CACHE_BACKUP" ]] || {
    echo "FATAL: schema-two cutover requires --prior-cache-backup" >&2; exit 2;
  }
  [[ "$ENGINE" != "sqlite" ]] || {
    echo "FATAL: schema-two cutover refuses --engine sqlite" >&2; exit 2;
  }
  "$PYTHON_BIN" - "$DB" <<'PY'
import sqlite3
import sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as connection:
    schema = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if schema == -2:
        raise SystemExit("schema-two cutover --db is an unfinished bulk root; resume cache-populate-activity-v2 --bulk-root before cutover")
    if schema != 2:
        raise SystemExit(f"schema-two cutover --db has schema {schema}, expected PRAGMA user_version=2")
PY
  "$PYTHON_BIN" -c 'import duckdb'
  BEFORE_RANKING_JSON="$OUT_DIR/before_ranking.json"
  "$PYTHON_BIN" scripts/push_ranking_to_supabase.py \
    --snapshot-current "$BEFORE_RANKING_JSON"
}

ACTIVATION_RUN_HASH="$("$PYTHON_BIN" -c \
  'import hashlib, os, sys; print(hashlib.sha256(os.path.realpath(os.path.abspath(sys.argv[1])).encode()).hexdigest()[:16])' \
  "$OUT_DIR")"
ACTIVATION_BATCH_ID="run-${ACTIVATION_RUN_HASH}"
if [[ ! "$ACTIVATION_BATCH_ID" =~ ^run-[0-9a-f]{16}$ ]]; then
  echo "FATAL: output directory cannot form a valid activation batch id: $OUT_DIR" >&2
  exit 2
fi
ACTIVATION_AUDIT_CSV="$OUT_DIR/activated_wallets.csv"

RANKED_CSV="$OUT_DIR/ranked_72hr_buyandhold.csv"
POSITIONS_CSV="$OUT_DIR/qualifying_positions_72hr.csv"
LATENCY_CSV="$OUT_DIR/latency_shift_ranked.csv"
PUBLISH_REQUEST_FILE="$OUT_DIR/ranking_publish_request.json"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
PIPELINE_VERSIONS_FILE="$OUT_DIR/pipeline_versions.json"
if [[ "$SKIP_RANK" == "0" && ! -f "$PIPELINE_VERSIONS_FILE" ]]; then
  "$PE_BOOTSTRAP_BIN" pipeline-versions > "$PIPELINE_VERSIONS_FILE"
fi
if [[ "$CUTOVER_MODE" == "1" && ! -f "$OUT_DIR/cycle_manifest.json" ]]; then
  "$PYTHON_BIN" - "$OUT_DIR/cycle_configuration.json" \
    "$HALF_LIFE_DAYS" "$TTR_HOURS" "$PRICE_MIN" "$PRICE_MAX" \
    "$FLOOR_TSTAT" "$MIN_TRL" "$MIN_AVG_PER_MONTH" "$MIN_ACTIVE_MONTHS" \
    "$LATENCY_SHIFT_SECS" "$FILL_WINDOW_SECS" "$TOP_N" <<'PY'
import json
import sys
keys = [
    "half_life_days", "ttr_hours", "price_min", "price_max", "floor_tstat",
    "min_trl", "min_avg_per_month", "min_active_months", "latency_shift_secs",
    "fill_window_secs", "top_n",
]
with open(sys.argv[1], "w", encoding="utf-8") as destination:
    json.dump(dict(zip(keys, sys.argv[2:], strict=True)), destination, sort_keys=True)
    destination.write("\n")
PY
  "$PYTHON_BIN" scripts/rank_cycle_manifest.py capture --db "$DB" \
    --day-utc "$(date -u +%Y-%m-%d)" --versions-file "$PIPELINE_VERSIONS_FILE" \
    --configuration-file "$OUT_DIR/cycle_configuration.json" \
    --output "$OUT_DIR/cycle_manifest.json"
fi

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
# Run one pe-bootstrap stage with the shared exit-code vocabulary
# (crates/bootstrap/src/main.rs dispatch comment): 0 = clean, 2 = partial soft-fail (cache
# durable, failures retried next run) → WARN + continue, anything else → abort the run with
# that code. Propagating the code verbatim is load-bearing for 75 (temporary failure, #534:
# events page exhaustion, resolutions audit-incomplete or exhausted-transient CLOB walk):
# the loop supervisor retries a 75 cycle, while 1 (permanent) stops the loop.
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
    75) echo "   [$label] TEMPFAIL exit 75 (temporary failure); aborting run for supervised retry" >&2; exit "$rc" ;;
    *) echo "   [$label] FATAL exit $rc — aborting run" >&2; exit "$rc" ;;
  esac
}

# 76 is emitted only for schema-one empty ranking/publication or globally stale
# trades, before a publication request exists. All other exits retain their meaning.
# Read CURRENT state: the frozen manifest precedes backfill and survives retries.
run_ranking_stage() {
  local label="$1"; shift
  local rc=0
  "$@" || rc=$?
  if [[ "$rc" -eq 76 ]]; then
    local guard_rc=0
    "$PYTHON_BIN" - "$DB" <<'PYGUARD' || guard_rc=$?
import sqlite3
import sys
sys.path.insert(0, "scripts")
from partial_backfill_wallets import partial_backfill_wallets
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as connection:
    retryable = partial_backfill_wallets(connection, retryable_only=True)
sys.exit(75 if retryable else 1)
PYGUARD
    if [[ "$guard_rc" -eq 75 ]]; then
      echo "   [$label] TEMPFAIL exit 76 → 75; incomplete wallets await backfill" >&2
      return 75
    fi
    echo "   [$label] FATAL exit 76 → 1; no retryable partial wallets" >&2
    return 1
  fi
  return "$rc"
}

# Always-on data refresh (discover → activate → backfill → events → resolutions). Each half is
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

  echo "── Step 0: data refresh (discover → activate 20,000 → backfill → events → resolutions) ──"

  if [[ "$SKIP_DISCOVERY" == "1" ]]; then
    echo "   discovery skipped (--skip-discovery)"
  else
    # winner-discovery hits both sources: leaderboard (errors propagate → fatal),
    # datadash (soft-fails internally → still exit 0). #324/#365.
    run_refresh_stage "winner-discovery" "$PE_BOOTSTRAP_BIN" winner-discovery --defer-activation "${BOOTSTRAP_CONFIG_ARGS[@]}"
    run_refresh_stage "activate-next" "$PE_BOOTSTRAP_BIN" activate-next \
      --batch-id "$ACTIVATION_BATCH_ID" --audit-csv "$ACTIVATION_AUDIT_CSV" \
      "${BOOTSTRAP_CONFIG_ARGS[@]}"
  fi

  if [[ "$SKIP_BACKFILL" == "1" ]]; then
    echo "   backfill + market-data skipped (--skip-backfill)"
  else
    run_refresh_stage "backfill" "$PE_BOOTSTRAP_BIN" backfill --defer-activation "${BOOTSTRAP_CONFIG_ARGS[@]}"
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

# Schema-two private-candidate lane (#588). Every path is a Rust owner that
# resumes from its own durable state; the wrapper only derives the cycle's
# physical names, the recorded initial activity/payout targets, and the
# one linked activity top-up recorded on the candidate.
stage_candidate_cache() {
  "$PE_BOOTSTRAP_BIN" cache-stage-v2 --db "$FIXED_DB" --prior "$1" --side "$2" \
    --manifest "$3" > "$4.tmp" || return $?
  mv -- "$4.tmp" "$4"
}

refresh_candidate_v2() {
  echo "── Step 0: schema-two private candidate (stage → discover → activate → activity → payout → finalize) ──"
  FIXED_DB="$(readlink -f -- "$DB" 2>/dev/null || true)"
  [[ -n "$FIXED_DB" && -f "$FIXED_DB" && ! -L "$FIXED_DB" ]] || {
    echo "FATAL: installed cache does not resolve to a regular file: $DB" >&2
    exit 2
  }
  # The Rust lock owners derive the cache lock and the loop/run lock directory
  # from the physical fixed path; both must be the repository inodes (docs/26).
  local phys_dir
  phys_dir="$(dirname -- "$FIXED_DB")"
  local fixed_name
  fixed_name="$(basename -- "$FIXED_DB")"
  [[ "$(readlink -f -- "$phys_dir/eval-results" 2>/dev/null || true)" == "$(readlink -f -- data/eval-results)" ]] || {
    echo "FATAL: $phys_dir/eval-results must resolve to data/eval-results" >&2
    exit 2
  }
  [[ "$(readlink -f -- "$phys_dir/$fixed_name.lock" 2>/dev/null || true)" == "$(readlink -f -- "$(dirname -- "$DB")")/$(basename -- "$DB").lock" ]] || {
    echo "FATAL: $phys_dir/$fixed_name.lock must resolve to $DB.lock" >&2
    exit 2
  }
  local cycle_name
  cycle_name="$(basename -- "$OUT_DIR")"
  local side="$phys_dir/wallet_cache.$cycle_name.side.db"
  local prior="$phys_dir/wallet_cache.$cycle_name.prior.db"
  echo "RANK_AND_PUSH_CACHE_SIDE=$side"
  echo "RANK_AND_PUSH_CACHE_PRIOR=$prior"
  echo "RANK_AND_PUSH_CACHE_DISPLACED=$phys_dir/wallet_cache.$cycle_name.displaced.db"

  local stage_json="$OUT_DIR/cache_stage.json"
  local build_manifest="$OUT_DIR/cache_build_manifest.json"
  # Raw version inspection is dispatch only: a fenced resume must not enter
  # staging, migration, discovery or activation. Rust revalidates bulk admission.
  local side_schema=""
  if [[ -e "$side" ]]; then
    [[ -f "$side" && ! -L "$side" ]] || { echo "FATAL: candidate must be a regular file" >&2; exit 2; }
    side_schema="$("$PYTHON_BIN" -c 'import sqlite3, sys
with sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True) as connection:
    print(int(connection.execute("PRAGMA user_version").fetchone()[0]))' "$side")"
  fi
  if [[ "$side_schema" == "-2" ]]; then
    [[ -f "$stage_json" && ! -L "$stage_json" ]] || {
      echo "FATAL: unfinished bulk root omitted its cycle staging report; preserve and investigate the cycle" >&2; exit 2;
    }
    echo "   [bulk-root] resuming the frozen candidate; setup already completed"
  else
    run_refresh_stage "cache-stage" stage_candidate_cache "$prior" "$side" "$build_manifest" "$stage_json"
    side_schema="$("$PYTHON_BIN" -c 'import json, sys
print(int(json.load(open(sys.argv[1], encoding="utf-8"))["side_schema"]))' "$stage_json")"
    if [[ "$side_schema" != "2" ]]; then
      # Initial schema-one candidate: seal it once against the hash-bound build
      # manifest that staging wrote from the verified source bytes. A sealed
      # candidate records that manifest's hash itself and is not migrated again.
      run_refresh_stage "cache-migrate" "$PE_BOOTSTRAP_BIN" cache-migrate-v2 --db "$side" \
        --manifest "$build_manifest" "${BOOTSTRAP_CONFIG_ARGS[@]}"
    fi

    export PE_BOOTSTRAP_CACHE_PATH="$side"
    run_refresh_stage "winner-discovery" "$PE_BOOTSTRAP_BIN" winner-discovery --defer-activation "${BOOTSTRAP_CONFIG_ARGS[@]}"
    run_refresh_stage "activate-next" "$PE_BOOTSTRAP_BIN" activate-next \
      --batch-id "$ACTIVATION_BATCH_ID" --audit-csv "$ACTIVATION_AUDIT_CSV" \
      "${BOOTSTRAP_CONFIG_ARGS[@]}"
  fi
  export PE_BOOTSTRAP_CACHE_PATH="$side"

  # Read the recorded candidate head, including an interrupted or manually
  # started top-up. Its persisted base link consumes this cycle's one allowance.
  local -a targets=()
  local target_output
  target_output="$("$PYTHON_BIN" scripts/rank_cycle_manifest.py candidate-targets --prior "$prior" --side "$side" --include-bulk-root)" || exit $?
  mapfile -t targets <<< "$target_output"
  [[ "${#targets[@]}" -eq 4 && "${targets[3]}" =~ ^[01]$ ]] || { echo "FATAL: could not derive candidate targets" >&2; exit 2; }
  local activity_target="${targets[0]}"
  local -a bulk_args=()
  if [[ "${targets[3]}" == "1" ]]; then
    bulk_args=(--bulk-root --fixed-db "$FIXED_DB")
    [[ ! -f "$prior" ]] || bulk_args+=(--prior "$prior")
  fi
  echo "   [targets] activity generation $activity_target; payout generation ${targets[1]} (complete=${targets[2]})"
  run_refresh_stage "activity" "$PE_BOOTSTRAP_BIN" cache-populate-activity-v2 --db "$side" \
    --fresh-generation "$activity_target" "${bulk_args[@]}" "${BOOTSTRAP_CONFIG_ARGS[@]}"
  local -a freshness_args=()
  [[ -z "$MAX_CACHE_STALENESS_HOURS" ]] || freshness_args+=(--max-staleness-hours "$MAX_CACHE_STALENESS_HOURS")
  target_output="$("$PYTHON_BIN" scripts/rank_cycle_manifest.py candidate-targets --prior "$prior" --side "$side" --include-bulk-root --after-collection "${freshness_args[@]}")" || exit $?
  mapfile -t targets <<< "$target_output"
  if [[ "${targets[0]}" != "$activity_target" ]]; then
    activity_target="${targets[0]}"
    run_refresh_stage "activity-top-up" "$PE_BOOTSTRAP_BIN" cache-populate-activity-v2 --db "$side" \
      --fresh-generation "$activity_target" "${BOOTSTRAP_CONFIG_ARGS[@]}"
    # Recheck after the only allowed top-up; a stale successor fails before ranking.
    target_output="$("$PYTHON_BIN" scripts/rank_cycle_manifest.py candidate-targets --prior "$prior" --side "$side" --include-bulk-root --after-collection "${freshness_args[@]}")" || exit $?
    mapfile -t targets <<< "$target_output"
    [[ "${targets[0]}" == "$activity_target" ]] || { echo "FATAL: unexpected second activity top-up" >&2; exit 2; }
  fi
  if [[ "${targets[2]}" == "1" ]]; then
    echo "   [payout] generation ${targets[1]} already complete on the candidate; reused"
  else
    run_refresh_stage "payout" "$PE_BOOTSTRAP_BIN" cache-populate-payout-v2 --db "$side" \
      "${BOOTSTRAP_CONFIG_ARGS[@]}"
  fi
  CACHE_STAGE_RECORD="$OUT_DIR/cache_stage_record.json"
  run_refresh_stage "cache-finalize" "$PE_BOOTSTRAP_BIN" cache-finalize-v2 --db "$side" \
    --stage-record "$CACHE_STAGE_RECORD" "${BOOTSTRAP_CONFIG_ARGS[@]}"
  DB="$side"
  if [[ -f "$prior" ]]; then
    PRIOR_CACHE_BACKUP="$prior"
  else
    PRIOR_CACHE_BACKUP="$phys_dir/wallet_cache.$cycle_name.displaced.db"
  fi
  CUTOVER_MODE="1"
}

if [[ "$FRESH_LANE" == "1" ]]; then
  refresh_candidate_v2
elif [[ "$CUTOVER_MODE" == "1" ]]; then
  echo "── Step 0: finalized schema-two side cache; legacy refresh skipped ──"
else
  refresh_data
fi
if [[ "$CUTOVER_MODE" == "1" ]]; then
  require_cutover_inputs
fi

# ── Step 0a: Parquet snapshot for the DuckDB read-layer (#375) ────────────────────────────
# Full atomic rewrite from the just-refreshed cache (so the snapshot is fresh for this run).
# Skipped when ranking is skipped, export is skipped, or the engine is forced to sqlite. In
# auto mode an export failure (e.g. duckdb not installed) is non-fatal — get_engine() then
# returns None and both passes fall back to SQLite; with --engine duck it is fatal.
if [[ "$SKIP_RANK" == "0" && "$SKIP_EXPORT" == "0" && "$ENGINE" != "sqlite" ]]; then
  echo "── Step 0a: export Parquet snapshot ($PARQUET_DIR) for the DuckDB read-layer ──"
  if "$PYTHON_BIN" scripts/export_trades_parquet.py --db "$DB" --out-dir "$PARQUET_DIR"; then
    echo "   export ok"
  elif [[ "$ENGINE" == "duck" ]]; then
    echo "FATAL: --engine duck but the Parquet export failed" >&2; exit 1
  else
    echo "   WARN: Parquet export failed (engine=auto) — ranker will use SQLite" >&2
  fi
else
  echo "── Step 0a: Parquet export skipped (engine=$ENGINE skip_rank=$SKIP_RANK skip_export=$SKIP_EXPORT) ──"
fi

TTR_MAX_SECS="$("$PYTHON_BIN" -c "print(int(float('$TTR_HOURS')*3600))")"

if [[ "$SKIP_RANK" == "0" ]]; then
  RERANK_CACHE_ARGS=()
  if [[ "$CUTOVER_MODE" == "1" ]]; then
    RERANK_CACHE_ARGS+=(
      --before-ranking-json "$BEFORE_RANKING_JSON"
      --cycle-manifest-file "$OUT_DIR/candidate_cycle_manifest.json"
      --cache-stage-record "$CACHE_STAGE_RECORD"
    )
  fi
  echo "── Stage 1/3: pass-1 edge-floor ranking ──────────────────────────────────────"
  run_ranking_stage "pass-1" "$PYTHON_BIN" scripts/rank_72hr_buyandhold.py \
    --db "$DB" "${UNIVERSE_ARGS[@]}" --out-dir "$OUT_DIR" \
    "${WIN_ARGS[@]}" --ttr-hours "$TTR_HOURS" \
    --half-life-days "$HALF_LIFE_DAYS" --as-of "$AS_OF" \
    --target-n "$TARGET_N" --price-min "$PRICE_MIN" --price-max "$PRICE_MAX" \
    --min-trl "$MIN_TRL" --min-avg-per-month "$MIN_AVG_PER_MONTH" \
    --min-active-months "$MIN_ACTIVE_MONTHS" \
    --floor-tstat "$FLOOR_TSTAT" --scheduled-only

  echo "── Stage 2a/3: emit reference-oracle fetch targets (#536) ─────────────────────"
  TARGETS_CSV="$OUT_DIR/oracle_targets.csv"
  run_ranking_stage "pass-2" "$PYTHON_BIN" scripts/latency_shift_rerank.py \
    --db "$DB" --ranked-csv "$RANKED_CSV" --positions-csv "$POSITIONS_CSV" \
    --out-dir "$OUT_DIR" \
    --latency-shift-secs "$LATENCY_SHIFT_SECS" --fill-window-secs "$FILL_WINDOW_SECS" \
    --min-trl "$MIN_TRL" --min-ttr-secs 60 --ttr-max-secs "$TTR_MAX_SECS" \
    --floor-tstat "$FLOOR_TSTAT" --emit-targets "$TARGETS_CSV"

  echo "── Stage 2b/3: targeted reference fetch into the ranker price store (#536) ────"
  # Write-once + range algebra => resumable; transient page failures return partial
  # (exit 2, WARN + continue) and pass-2's terminal-coverage gate then exits 75 so the
  # supervisor retries the cycle — a partially fetched cycle can never publish.
  export PE_BOOTSTRAP_CACHE_PATH="$DB"
  run_refresh_stage "reference-fetch" "$PE_BOOTSTRAP_BIN" prices-history \
    --targets-csv "$TARGETS_CSV" "${BOOTSTRAP_CONFIG_ARGS[@]}"
  if [[ "$CUTOVER_MODE" == "1" ]]; then
    # The price store lives in the same SQLite file. Re-finalize after its
    # targeted writes so activation is bound to the exact ranked cache bytes,
    # and capture the finalized candidate separately from the cycle's initial
    # installed-cache watermark.
    "$PE_BOOTSTRAP_BIN" cache-finalize-v2 --db "$DB" \
      --stage-record "$CACHE_STAGE_RECORD" "${BOOTSTRAP_CONFIG_ARGS[@]}"
    "$PYTHON_BIN" scripts/rank_cycle_manifest.py capture --db "$DB" \
      --day-utc "$(date -u +%Y-%m-%d)" --versions-file "$PIPELINE_VERSIONS_FILE" \
      --configuration-file "$OUT_DIR/cycle_configuration.json" \
      --output "$OUT_DIR/candidate_cycle_manifest.json"
  fi

  echo "── Stage 2c/3: pass-2 reference-oracle rerank (adds hit_rate) ─────────────────"
  run_ranking_stage "pass-2" "$PYTHON_BIN" scripts/latency_shift_rerank.py \
    --db "$DB" --ranked-csv "$RANKED_CSV" --positions-csv "$POSITIONS_CSV" \
    --out-dir "$OUT_DIR" \
    --latency-shift-secs "$LATENCY_SHIFT_SECS" --fill-window-secs "$FILL_WINDOW_SECS" \
    --half-life-days "$HALF_LIFE_DAYS" --as-of "$AS_OF" \
    --min-trl "$MIN_TRL" --min-avg-per-month "$MIN_AVG_PER_MONTH" \
    --min-active-months "$MIN_ACTIVE_MONTHS" \
    --min-ttr-secs 60 --ttr-max-secs "$TTR_MAX_SECS" \
    --price-min "$PRICE_MIN" --price-max "$PRICE_MAX" \
    --floor-tstat "$FLOOR_TSTAT" --git-sha "$GIT_SHA" \
    --pipeline-versions-file "$PIPELINE_VERSIONS_FILE" \
    "${RERANK_CACHE_ARGS[@]}"
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

echo "── Stage 3/3: atomically publish exact ranking to Supabase ─────────────────────"
PUSH_ARGS=()
if [[ "$RESUME_PENDING" == "1" ]]; then
  # The request contains the original provenance, active-filter result, entry clocks,
  # and content hash. Never rebuild it from a later clock/cache/code revision.
  PUSH_ARGS+=(--resume-request "$PUBLISH_REQUEST_FILE")
else
  # Active-only upload filter args (issue #350 WS3): always pass --db; window/staleness
  # only when explicitly overridden, so the canonical defaults stay in the push script.
  FILTER_ARGS=(--db "$DB")
  [[ -n "$ACTIVE_WINDOW_HOURS" ]] && FILTER_ARGS+=(--active-window-hours "$ACTIVE_WINDOW_HOURS")
  [[ -n "$MAX_CACHE_STALENESS_HOURS" ]] && FILTER_ARGS+=(--max-cache-staleness-hours "$MAX_CACHE_STALENESS_HOURS")

  # TTR provenance: pass the actual ranking TTR ceiling so the durable request reflects
  # the shape these entries were ranked at.
  PUSH_ARGS+=(
    --ranked-csv "$LATENCY_CSV" --top-n "$TOP_N"
    --band-lo "$PRICE_MIN" --band-hi "$PRICE_MAX"
    --ttr-max-secs "$TTR_MAX_SECS"
    --latency-shift-secs "$LATENCY_SHIFT_SECS"
    "${FILTER_ARGS[@]}"
    --git-sha "$GIT_SHA" --notes "${NOTES:-rank_and_push.sh $GIT_SHA}"
    --request-file "$PUBLISH_REQUEST_FILE"
  )
  # #536: bind the oracle manifest into config_hash. A fresh rerank ALWAYS writes it
  # (stage 2c, before the ranked CSV is considered complete), so its absence there is
  # corruption — fail closed, never silently publish provenance-less. Only a
  # pre-cutover directory re-pushed via --skip-rank legitimately has none and
  # publishes config_hash = null (the documented legacy-replay shape).
  MANIFEST_FILE="$OUT_DIR/oracle_manifest.json"
  if [[ -f "$MANIFEST_FILE" ]]; then
    PUSH_ARGS+=(--manifest-file "$MANIFEST_FILE")
  elif [[ "$SKIP_RANK" == "0" ]]; then
    echo "FATAL: fresh rerank left no $MANIFEST_FILE — refusing provenance-less publish" >&2
    exit 1
  fi

  # Parameterized research/re-push invocations never own the singleton production
  # recovery pointer. The complete zero-argument cycle is its sole normal writer.
  if [[ "$INVOCATION_ARGC" -eq 0 || "$CUTOVER_MODE" == "1" ]]; then
    PUSH_ARGS+=(--pending-file "$PENDING_FILE")
  fi
  if [[ "$CUTOVER_MODE" == "1" ]]; then
    PUSH_ARGS+=(
      --cache-stage-record "$CACHE_STAGE_RECORD"
      --cache-side-db "$DB"
      --cache-fixed-db "$FIXED_DB"
      --prior-cache-backup "$PRIOR_CACHE_BACKUP"
    )
  fi
fi

ACCEPTED_DB="$DB"
activate_bound_cache() {
  local request_path="$1"
  local -a binding=()
  local validation_output
  validation_output="$("$PYTHON_BIN" scripts/push_ranking_to_supabase.py \
    --validate-request "$request_path")" || return $?
  if [[ -z "$validation_output" ]]; then
    return 0
  fi
  mapfile -t binding <<< "$validation_output"
  [[ "${#binding[@]}" -eq 4 || "${#binding[@]}" -eq 5 ]] || {
    echo "FATAL: publication request has malformed cache activation evidence" >&2
    return 2
  }
  # The request's fixed path is the installed cache the accepted watermark
  # must be captured from on every entry, including recovery.
  ACCEPTED_DB="${binding[1]}"
  if [[ "${PE_RANK_SCHEMA_TWO_CUTOVER:-0}" == "prepare" ]]; then
    echo "RANK_AND_PUSH_PREPARED_ONLY=$request_path"
    echo "FATAL: PE_RANK_SCHEMA_TWO_CUTOVER=prepare holds the prepared request at the acceptance boundary; set it to 1 to activate and publish" >&2
    return 2
  fi
  local -a lock_handoff=(
    --held-run-lock-fd 8
    --held-run-lock-pid "$$"
  )
  local loop_lock_file="data/eval-results/.rank_and_push_loop.lock"
  if [[ -e /proc/self/fd/9 && /proc/self/fd/9 -ef "$loop_lock_file" ]]; then
    local loop_pid
    loop_pid="$(tr -cd '0-9' < "$loop_lock_file" 2>/dev/null || true)"
    [[ -n "$loop_pid" ]] || {
      echo "FATAL: inherited ranking-loop lock has no holder PID" >&2
      return 2
    }
    lock_handoff+=(--held-loop-lock-fd 9 --held-loop-lock-pid "$loop_pid")
  fi
  local -a stage_binding=()
  [[ "${#binding[@]}" -ne 5 ]] || stage_binding=(--stage-evidence-sha256 "${binding[4]}")
  "$PE_BOOTSTRAP_BIN" cache-activate --db "${binding[0]}" \
    --fixed-db "${binding[1]}" --backup "${binding[2]}" \
    --expected-sha256 "${binding[3]}" "${stage_binding[@]}" "${lock_handoff[@]}"
}

push_rc=0
if [[ "$CUTOVER_MODE" == "1" && "$RESUME_PENDING" != "1" ]]; then
  run_ranking_stage "push" "$PYTHON_BIN" scripts/push_ranking_to_supabase.py \
    "${PUSH_ARGS[@]}" --prepare-only || push_rc=$?
  if [[ "$push_rc" -eq 0 && "$FRESH_LANE" == "1" && "${PE_RANK_SCHEMA_TWO_CUTOVER:-0}" == "prepare" ]]; then
    # Acceptance boundary (#588 §6.3): the full candidate has reached exact
    # request preparation within the publisher's unchanged freshness checks.
    # Stop here with the pending request retained. Exit 2 stops the supervisor
    # (non-75) instead of retrying into activation; recovery refuses to
    # activate until the operator records the measurement and sets the value
    # to 1, after which the ordinary pending-publication recovery activates and
    # publishes exactly this request.
    echo "RANK_AND_PUSH_PREPARED_ONLY=$PUBLISH_REQUEST_FILE"
    exit 2
  fi
  if [[ "$push_rc" -eq 0 ]]; then
    activate_bound_cache "$PUBLISH_REQUEST_FILE"
    run_ranking_stage "push" "$PYTHON_BIN" scripts/push_ranking_to_supabase.py \
      --resume-request "$PUBLISH_REQUEST_FILE" || push_rc=$?
  fi
elif [[ "$RESUME_PENDING" == "1" ]]; then
  activate_bound_cache "$PUBLISH_REQUEST_FILE"
  run_ranking_stage "push" "$PYTHON_BIN" scripts/push_ranking_to_supabase.py "${PUSH_ARGS[@]}" || push_rc=$?
else
  run_ranking_stage "push" "$PYTHON_BIN" scripts/push_ranking_to_supabase.py "${PUSH_ARGS[@]}" || push_rc=$?
fi
if [[ "$push_rc" -ne 0 ]]; then
  echo "   [push] exit $push_rc — pending request retained for recovery" >&2
  exit "$push_rc"
fi
echo "✓ Supabase exact batch published and verified. pe-service picks it up within one refresh interval."

if [[ -f "$OUT_DIR/cycle_configuration.json" ]]; then
  # The accepted watermark is captured from the installed cache only after the
  # exact publication succeeds — on the fresh path and on either recovery entry
  # (automatic or --resume-pending), since activation may have replaced the
  # fixed file. A later same-day zero-argument invocation compares against this
  # post-refresh state before discovery or any other cache mutation.
  "$PE_BOOTSTRAP_BIN" pipeline-versions > "$OUT_DIR/pipeline_versions.json"
  "$PYTHON_BIN" scripts/rank_cycle_manifest.py capture \
    --db "$ACCEPTED_DB" --day-utc "$(date -u +%Y-%m-%d)" \
    --versions-file "$OUT_DIR/pipeline_versions.json" \
    --configuration-file "$OUT_DIR/cycle_configuration.json" \
    --output "$OUT_DIR/accepted_cycle_manifest.json"
fi

# Compare-and-clear: never erase a different/newer recovery request. The request JSON
# remains in the run directory as publication audit evidence; only the singleton pointer
# is consumed after the same run's publish finishes.
if [[ -e "$PENDING_FILE" || -L "$PENDING_FILE" ]]; then
  if [[ -f "$PENDING_FILE" && ! -L "$PENDING_FILE" ]]; then
    mapfile -t COMPLETED_PENDING_LINES < "$PENDING_FILE"
    if [[ "${#COMPLETED_PENDING_LINES[@]}" -eq 1 && -n "${COMPLETED_PENDING_LINES[0]}" ]]; then
      COMPLETED_PENDING_REAL="$(readlink -f -- "${COMPLETED_PENDING_LINES[0]}" 2>/dev/null || true)"
      REQUEST_REAL="$(readlink -f -- "$PUBLISH_REQUEST_FILE" 2>/dev/null || true)"
      if [[ -n "$REQUEST_REAL" && "$COMPLETED_PENDING_REAL" == "$REQUEST_REAL" ]]; then
        rm -f "$PENDING_FILE"
        echo "   [recovery] cleared completed pending publication pointer"
      else
        echo "   [recovery] WARN pending pointer changed; leaving it intact" >&2
      fi
    else
      echo "   [recovery] WARN malformed pending pointer; leaving it intact" >&2
    fi
  else
    echo "   [recovery] WARN unsafe pending pointer; leaving it intact" >&2
  fi
fi

# Compare-and-clear the broader logical-cycle pointer only after publication has completed.
# A changed/malformed pointer is evidence
# for another operator action and must never be erased.
if [[ -e "$CYCLE_FILE" || -L "$CYCLE_FILE" ]]; then
  if [[ -f "$CYCLE_FILE" && ! -L "$CYCLE_FILE" ]]; then
    mapfile -t COMPLETED_CYCLE_LINES < "$CYCLE_FILE"
    if [[ "${#COMPLETED_CYCLE_LINES[@]}" -eq 1 && -n "${COMPLETED_CYCLE_LINES[0]}" ]]; then
      COMPLETED_CYCLE_REAL="$(readlink -f -- "${COMPLETED_CYCLE_LINES[0]}" 2>/dev/null || true)"
      OUT_DIR_REAL="$(readlink -f -- "$OUT_DIR" 2>/dev/null || true)"
      if [[ -n "$OUT_DIR_REAL" && "$COMPLETED_CYCLE_REAL" == "$OUT_DIR_REAL" ]]; then
        rm -f "$CYCLE_FILE"
        echo "   [recovery] cleared completed logical-cycle pointer"
      else
        echo "   [recovery] WARN cycle pointer changed; leaving it intact" >&2
      fi
    else
      echo "   [recovery] WARN malformed cycle pointer; leaving it intact" >&2
    fi
  else
    echo "   [recovery] WARN unsafe cycle pointer; leaving it intact" >&2
  fi
fi
# The accepted watermark and request remain discoverable after pointer clearing.
if [[ -f "$OUT_DIR/cycle_configuration.json" ]]; then
  retention_rc=0
  "$PYTHON_BIN" scripts/rank_cycle_manifest.py retire-completed \
    --root data/eval-results --out-dir "$OUT_DIR" || retention_rc=$?
  case "$retention_rc" in
    0|2|3) ;; # Complete, deferred by a guard, or not a candidate cycle.
    *) exit "$retention_rc" ;;
  esac
fi
echo "✓ rank_and_push complete — Supabase published."
