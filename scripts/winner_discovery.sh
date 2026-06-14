#!/usr/bin/env bash
# Winner-discovery pipeline (issue #324).
#
# Ingests new candidate wallets from the Polymarket leaderboard (and Radion
# when configured), runs them through the full eval pipeline, and produces
# data/winner_discovery_candidates.json for manual review.
#
# Usage:
#   ./scripts/winner_discovery.sh [config.toml]
#
# The optional positional argument is a BootstrapConfig TOML path.  If omitted,
# the binary falls back to PE_BOOTSTRAP_CONFIG or its own built-in defaults.
#
# Optional env (examples):
#   PE_BOOTSTRAP_LEADERBOARD_BASE_URL    override leaderboard host
#   PE_BOOTSTRAP_LEADERBOARD_TOP_N       default 50 (the API caps limit at 50)
#   PE_BOOTSTRAP_LEADERBOARD_CATEGORIES  TOML array; default = all 10 categories
#   PE_BOOTSTRAP_RADION_API_URL          enables Radion source
#   PE_BOOTSTRAP_RADION_API_KEY

set -euo pipefail

CONFIG_ARG="${1:-}"

BIN_DIR="$(dirname "$0")/../target/release"
PE_BOOTSTRAP="$BIN_DIR/pe-bootstrap"
PE_SKILL_SELECT="$BIN_DIR/pe-skill-select"

OUT_DIR="$(dirname "$0")/../data"
CANDIDATES_JSON="$OUT_DIR/winner_discovery_candidates.json"

# ── Step 1: ingest new wallets from leaderboard (+ Radion if configured) ──────
echo "[winner-discovery] step 1: ingest leaderboard wallets"
"$PE_BOOTSTRAP" winner-discovery ${CONFIG_ARG:+"$CONFIG_ARG"}

# ── Step 2: backfill trade history for newly-activated wallets ─────────────────
echo "[winner-discovery] step 2: backfill"
"$PE_BOOTSTRAP" backfill ${CONFIG_ARG:+"$CONFIG_ARG"}

# ── Step 3: extract features ──────────────────────────────────────────────────
echo "[winner-discovery] step 3: extract"
"$PE_SKILL_SELECT" extract ${CONFIG_ARG:+"$CONFIG_ARG"}

# ── Step 4: composite ranking ─────────────────────────────────────────────────
echo "[winner-discovery] step 4: composite"
"$PE_SKILL_SELECT" composite ${CONFIG_ARG:+"$CONFIG_ARG"}

# ── Step 5: export candidate watchlist ────────────────────────────────────────
echo "[winner-discovery] step 5: export-watchlist → $CANDIDATES_JSON"
"$PE_SKILL_SELECT" export-watchlist --output "$CANDIDATES_JSON" ${CONFIG_ARG:+"$CONFIG_ARG"}

echo "[winner-discovery] done — review $CANDIDATES_JSON before deploying to VPS"
