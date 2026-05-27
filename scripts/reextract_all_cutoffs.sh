#!/usr/bin/env bash
# Re-extract wallet_features across the 7 most recent cutoffs with CLEAN_PRIOR=true.
# Runs newest-first so useful cutoffs finish early and the run can be killed once
# the recent ones are done. The two oldest cutoffs (2025-09-30, 2025-10-31) are
# omitted: they cover the largest trade windows, are the slowest to extract, and
# contribute little to GBM training since the harness uses the most-recent K anchors.
# Usage: bash scripts/reextract_all_cutoffs.sh

set -euo pipefail

BIN="./target/release/pe-skill-select"
DB="data/wallet_cache.db"

CUTOFFS=(
  1779839999   # 2026-05-26
  1777679999   # 2026-05-01
  1775001599   # 2026-03-31
  1772495999   # 2026-03-02
  1769903999   # 2026-01-31
  1767225599   # 2025-12-31
  1764547199   # 2025-11-30
)

TOTAL=${#CUTOFFS[@]}
START_ALL=$(date +%s)

for i in "${!CUTOFFS[@]}"; do
  CUTOFF="${CUTOFFS[$i]}"
  NUM=$((i + 1))
  DATE=$(date -d "@$CUTOFF" +%Y-%m-%d 2>/dev/null || date -r "$CUTOFF" +%Y-%m-%d)
  echo "=== [$NUM/$TOTAL] cutoff=$CUTOFF ($DATE) ==="
  T0=$(date +%s)
  PE_SKILL_CACHE_PATH="$DB" \
  PE_SKILL_CUTOFF_UNIX="$CUTOFF" \
  PE_SKILL_EXTRACT_CLEAN_PRIOR=true \
    "$BIN" extract
  T1=$(date +%s)
  echo "    done in $((T1 - T0))s"
done

ELAPSED=$(( $(date +%s) - START_ALL ))
echo ""
echo "=== All $TOTAL cutoffs done in ${ELAPSED}s ==="
