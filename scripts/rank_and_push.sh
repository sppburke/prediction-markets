#!/usr/bin/env bash
# Rank the copy-trade wallet universe AND publish the result to Supabase — as ONE
# command, so "done ranking" always means "in Supabase".
#
# THE BUG THIS FIXES: the ranking pipeline (pass-1 rank_72hr_buyandhold.py -> pass-2
# latency_shift_rerank.py) ended by writing latency_shift_ranked.csv to disk and
# STOPPED. push_ranking_to_supabase.py had no caller anywhere, so Supabase stayed
# empty and pe-service fell back to its static seed. This wrapper makes the push a
# non-optional final stage: if ranking succeeds, the push runs; if the push fails,
# the whole command fails loudly (set -e), so it can never be silently skipped again.
#
# Usage (from repo root):
#   bash scripts/rank_and_push.sh --universe data/eval-results/<run>/universe_eligible.txt \
#        --out-dir data/eval-results/<run> [--notes "monthly refresh"]
#
# Requirements: .env with SUPABASE_URL + SUPABASE_SECRET_KEY; data/wallet_cache.db present.
# Re-push only (skip ranking, reuse existing CSVs): add --skip-rank.
#
# BACKFILL FIRST (issue #350 WS3): the push drops wallets idle > --active-window-hours (72)
# and ABORTS if the cache's newest trade is > 24h old, so run docs/26 Part 1 (backfill)
# immediately before this. A stale cache would otherwise filter out every wallet.
# A re-push (--skip-rank) still filters against the cache; if it runs >24h after the
# backfill, re-backfill first or pass --max-cache-staleness-hours to relax the guard.

set -euo pipefail
cd "$(dirname "$0")/.."

# ── Defaults match the 2026-06-14 production run (override via flags) ──────────────────────
DB="data/wallet_cache.db"
UNIVERSE=""
OUT_DIR=""
# Empty window => the Python ranker's RELATIVE defaults apply (win_end = today UTC-midnight,
# win_start = win_end - ranker_window_days(180)); override with --win-start/--win-end (issue #366).
WIN_START=""
WIN_END=""
# Recency decay (issue #366). The WRAPPER defaults to FLAT (0) so a merge is a no-op for the
# live Supabase ranking: decay only reaches live when the operator opts in with --half-life-days N
# after sweeping. The Python scripts default to ranker_half_life_days(30) for standalone sweeps.
HALF_LIFE_DAYS="0"
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
    --skip-rank) SKIP_RANK="1"; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

[[ -n "$OUT_DIR" ]] || { echo "FATAL: --out-dir is required" >&2; exit 2; }
[[ -f .env ]] || { echo "FATAL: .env not found (need SUPABASE_URL + SUPABASE_SECRET_KEY)" >&2; exit 2; }

# Load secrets up front so EVERY stage (incl. the push + verify, which read SUPABASE_URL /
# SUPABASE_SECRET_KEY from the environment) sees them. Sourcing only before the verify step
# would leave the push stage without credentials.
set -a; source .env; set +a
mkdir -p "$OUT_DIR"

RANKED_CSV="$OUT_DIR/ranked_72hr_buyandhold.csv"
POSITIONS_CSV="$OUT_DIR/qualifying_positions_72hr.csv"
LATENCY_CSV="$OUT_DIR/latency_shift_ranked.csv"
GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"

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

if [[ "$SKIP_RANK" == "0" ]]; then
  [[ -n "$UNIVERSE" ]] || { echo "FATAL: --universe is required unless --skip-rank" >&2; exit 2; }

  echo "── Stage 1/3: pass-1 edge-floor ranking ──────────────────────────────────────"
  python3 scripts/rank_72hr_buyandhold.py \
    --db "$DB" --universe "$UNIVERSE" --out-dir "$OUT_DIR" \
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
