#!/usr/bin/env bash
# rank_and_push.sh — THE single, cron-ready entry point for the copy-trade ranking
# pipeline. ONE command, ZERO arguments: it refreshes data, ranks, reranks, and
# publishes the result to Supabase, so "done ranking" always means "in Supabase".
#
# THERE IS EXACTLY ONE RANKER: scripts/rank_72hr_buyandhold.py (72h buy-and-hold,
# decay-aware, memory-safe inline scan over the full trade universe). pass-2
# (latency_shift_rerank.py) reranks and adds hit_rate; push_ranking_to_supabase.py
# publishes the `latest_ranking` table that pe-service reads. No second ranker, no
# curated-universe pre-gate — the ranker's own filters decide the cohort (#370).
#
# What `bash scripts/rank_and_push.sh` does (no args), in order:
#   Step 0  refresh data (always-on; --skip-discovery / --skip-backfill to bypass):
#     discover     pe-bootstrap winner-discovery  leaderboard + datadash + radion → new wallets
#     backfill     pe-bootstrap backfill          trade history for every active wallet
#     events       pe-bootstrap events            condition→event + fee maps (eligibility gate)
#     resolutions  pe-bootstrap resolutions       CLOB→Gamma market resolutions (payoff)
#     schedules    pe-bootstrap schedules         scheduled end_date (the --scheduled-only TTR clock)
#   Stage 1  rank    rank_72hr_buyandhold.py  --universe-from-trades  (every wallet w/ trade data)
#   Stage 2  rerank  latency_shift_rerank.py  (adds hit_rate)
#   Stage 3  push    push_ranking_to_supabase.py → Supabase latest_ranking
#   Verify           latest_ranking is now populated.
#
# Production defaults are baked in (override via flags): --universe-from-trades,
# HALF_LIFE_DAYS, relative 180d window, band 0.15–0.85, TTR 72h, --scheduled-only
# (drops the resolved-at look-ahead fallback), floor_tstat 2.0, top_n 200.
#
# Cron (example — daily 06:00 UTC on the box holding wallet_cache.db; NOT the VPS):
#   0 6 * * *  cd /home/sean/git/prediction-markets && bash scripts/rank_and_push.sh >> data/eval-results/cron.log 2>&1
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
#   --skip-backfill       skip Step-0 backfill + market-data refresh (events/resolutions/schedules).
#   --skip-rank           reuse existing CSVs in --out-dir; just (re-)push. The push still
#                         filters against the cache and ABORTS if its newest trade is >24h old
#                         (issue #350 WS3); re-backfill first, or pass --max-cache-staleness-hours.
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
TTR_HOURS="72"
TARGET_N="25"
PRICE_MIN="0.15"
PRICE_MAX="0.85"
FLOOR_TSTAT="2.0"
LATENCY_SHIFT_SECS="20"
FILL_WINDOW_SECS="120"
TOP_N="200"
# Active-only upload filter (issue #350 WS3). Empty => use push_ranking_to_supabase.py's
# canonical defaults (72 / 24, docs/_GLOSSARY), so the thresholds live in exactly one place.
ACTIVE_WINDOW_HOURS=""
MAX_CACHE_STALENESS_HOURS=""
NOTES=""
SKIP_RANK="0"
SKIP_DISCOVERY="0"
SKIP_BACKFILL="0"
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

# Always-on data refresh (discover → backfill → events → resolutions → schedules). Each half is
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

  echo "── Step 0: data refresh (discover → backfill → events → resolutions → schedules) ──"

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
    # The explicit `resolutions` subcommand runs the CLOB→Gamma pipeline unconditionally
    # (main.rs:187-218); PE_BOOTSTRAP_FETCH_RESOLUTIONS=1 matches docs/26's canonical invocation
    # and is scoped to this stage so `backfill` above does not also re-run the pipeline.
    export PE_BOOTSTRAP_FETCH_RESOLUTIONS=1
    run_refresh_stage "resolutions" "$PE_BOOTSTRAP_BIN" resolutions "${BOOTSTRAP_CONFIG_ARGS[@]}"
    unset PE_BOOTSTRAP_FETCH_RESOLUTIONS
    run_refresh_stage "schedules" "$PE_BOOTSTRAP_BIN" schedules "${BOOTSTRAP_CONFIG_ARGS[@]}"
  fi
}

refresh_data

if [[ "$SKIP_RANK" == "0" ]]; then
  echo "── Stage 1/3: pass-1 edge-floor ranking ──────────────────────────────────────"
  python3 scripts/rank_72hr_buyandhold.py \
    --db "$DB" "${UNIVERSE_ARGS[@]}" --out-dir "$OUT_DIR" \
    "${WIN_ARGS[@]}" --ttr-hours "$TTR_HOURS" \
    --half-life-days "$HALF_LIFE_DAYS" --as-of "$AS_OF" \
    --target-n "$TARGET_N" --price-min "$PRICE_MIN" --price-max "$PRICE_MAX" \
    --floor-tstat "$FLOOR_TSTAT" --scheduled-only

  echo "── Stage 2/3: pass-2 latency-shift rerank (adds hit_rate) ─────────────────────"
  python3 scripts/latency_shift_rerank.py \
    --db "$DB" --ranked-csv "$RANKED_CSV" --positions-csv "$POSITIONS_CSV" \
    --out-dir "$OUT_DIR" \
    --latency-shift-secs "$LATENCY_SHIFT_SECS" --fill-window-secs "$FILL_WINDOW_SECS" \
    --half-life-days "$HALF_LIFE_DAYS" --as-of "$AS_OF" \
    --floor-tstat "$FLOOR_TSTAT"
else
  echo "── Stages 1-2 skipped (--skip-rank); reusing $LATENCY_CSV ──"
fi

[[ -s "$LATENCY_CSV" ]] || { echo "FATAL: ranking produced no $LATENCY_CSV" >&2; exit 1; }

echo "── Stage 3/3: push to Supabase (the previously-missing step) ──────────────────"
# Active-only upload filter args (issue #350 WS3): always pass --db; window/staleness only
# when explicitly overridden, so the canonical defaults stay solely in the push script.
FILTER_ARGS=(--db "$DB")
[[ -n "$ACTIVE_WINDOW_HOURS" ]] && FILTER_ARGS+=(--active-window-hours "$ACTIVE_WINDOW_HOURS")
[[ -n "$MAX_CACHE_STALENESS_HOURS" ]] && FILTER_ARGS+=(--max-cache-staleness-hours "$MAX_CACHE_STALENESS_HOURS")

python3 scripts/push_ranking_to_supabase.py \
  --ranked-csv "$LATENCY_CSV" --top-n "$TOP_N" \
  --band-lo "$PRICE_MIN" --band-hi "$PRICE_MAX" \
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
