#!/usr/bin/env bash
# overnight_pipeline.sh — portfolio constructor → sizing sim → filtered DB rebuild
# Steps:
#   1. Full portfolio constructor (all eligible anchors, n_seeds=1, fwd=7d, 500bps)
#      --max-candidates 200 caps market-sets load to top-200 per anchor (~25x speedup)
#      --n-seeds 1        skips PBO matrix re-runs (~5x speedup; PBO verdict = undefined)
#   2. Sizing simulation on the portfolio watchlist output from Step 1
#   3. Filtered DB rebuild (non-destructive; swap is a manual step)
# Logs everything to /tmp/overnight_pipeline.log

set -euo pipefail
LOG=/tmp/overnight_pipeline.log
REPO=/home/sean/git/prediction-markets
PY="$REPO/.venv-analysis/bin/python3"
DB="$REPO/data/wallet_cache.db"
WL="$REPO/data/watchlist-20260528T194034Z-gbm_bhq_intersection_3.txt"
OUT_DIR="$REPO/data/eval-results"

log() { echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG"; }

log "=== overnight pipeline start ==="

# ── Step 1: full portfolio constructor ──────────────────────────────────────
log "STEP 1: portfolio constructor (n_seeds=1, max_candidates=200, fwd=7d, 500bps)"
PORTFOLIO_WL=""
PORTFOLIO_WL=$("$PY" scripts/portfolio_constructor/cli.py \
    --db-path "$DB" \
    --watchlist "$WL" \
    --fwd-days 7 \
    --n-seeds 1 \
    --max-candidates 200 \
    --haircut-bps 500 \
    --label portfolio_greedy_fwd7d \
    2>&1 | tee -a "$LOG" \
    | grep "^Deploy watchlist:" | awk '{print $3}')

log "STEP 1 complete — portfolio watchlist: $PORTFOLIO_WL"

# ── Step 2: sizing simulation on the portfolio output ────────────────────────
log "STEP 2: sizing simulation (April + May holdouts, portfolio cohort)"
if [ -n "$PORTFOLIO_WL" ] && [ -f "$PORTFOLIO_WL" ]; then
    log "  using portfolio watchlist: $PORTFOLIO_WL"
    "$PY" scripts/sizing_april_sim.py \
        --db-path "$DB" \
        --watchlist "$PORTFOLIO_WL" \
        --holdouts April,May \
        --n-seeds 1 \
        2>&1 | tee -a "$LOG"
else
    log "  WARN: no portfolio watchlist found — falling back to throughput cohort"
    "$PY" scripts/sizing_april_sim.py \
        --db-path "$DB" \
        --holdouts April,May \
        --n-seeds 1 \
        2>&1 | tee -a "$LOG"
fi

log "STEP 2 complete"

# ── Step 3: filtered DB rebuild (read internal, write pruned copy to CORSAIR) ─
log "STEP 3: filtered DB rebuild → /media/sean/CORSAIR/db_update/wallet_cache_pruned.db"
log "  (non-destructive; original DB never touched; swap is a manual step)"
"$PY" scripts/filtered_rebuild.py \
    2>&1 | tee -a "$LOG"

log "STEP 3 complete — review verification output above before swapping"
log "=== overnight pipeline done ==="
