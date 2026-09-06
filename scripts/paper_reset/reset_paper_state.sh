#!/usr/bin/env bash
# Read-only preview or deliberate manual execution of the activation-stamped Supabase reset.

set -euo pipefail
# shellcheck source=../deploy/generation_common.sh
source "$(cd "$(dirname "$0")/../deploy" && pwd)/generation_common.sh"
cd "$(dirname "$0")/../.."

usage() {
  echo "usage: $0 [--execute] --activation-id ID --bankroll DECIMAL" >&2
  exit 2
}

mode=dry-run
activation_id=
bankroll=
while (($#)); do
  case "$1" in
    --execute) mode=execute; shift ;;
    --activation-id) [[ $# -ge 2 ]] || usage; activation_id=$2; shift 2 ;;
    --bankroll) [[ $# -ge 2 ]] || usage; bankroll=$2; shift 2 ;;
    *) usage ;;
  esac
done
[[ "$activation_id" =~ ^[A-Za-z0-9._-]+$ ]] || usage
[[ "$bankroll" =~ ^[0-9]+([.][0-9]+)?$ ]] || usage

# SUPABASE_DB_URL belongs on the operator host, never in the VPS service environment.
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
: "${SUPABASE_DB_URL:?SUPABASE_DB_URL missing; set it in the operator environment or local .env}"
command -v psql >/dev/null || { echo "FATAL: psql not installed" >&2; exit 1; }

echo "live paper-state rows before activation $activation_id:"
psql_service_db -v ON_ERROR_STOP=1 -At <<'SQL'
select 'paper_fills=' || count(*) from paper_fills
union all select 'settled_markets=' || count(*) from settled_markets
union all select 'paper_positions=' || count(*) from paper_positions
union all select 'paper_bankroll=' || count(*) from paper_bankroll
union all select 'fill_market_snapshots=' || count(*) from fill_market_snapshots;
SQL

if [[ "$mode" == dry-run ]]; then
  printf '%s\n' \
    'DRY-RUN: no database rows changed' \
    "activation_id=$activation_id" \
    "fresh_bankroll=$bankroll" \
    'execute_owner=scripts/paper_reset/archive_paper_state.sql' \
    'generation_cutover_owner=scripts/deploy/activate_generation.sh' \
    'postcondition=five activation-stamped archives; four empty live tables; one fresh bankroll row'
  exit 0
fi

read -r -p "Type the activation id '$activation_id' to archive and reset Supabase: " confirm
[[ "$confirm" == "$activation_id" ]] || { echo "aborted (no mutation)"; exit 1; }

psql_service_db -v ON_ERROR_STOP=1 \
  -v activation_id="$activation_id" -v bankroll="$bankroll" \
  -f scripts/paper_reset/archive_paper_state.sql

echo "post-reset paper-state rows:"
psql_service_db -v ON_ERROR_STOP=1 -At -v activation_id="$activation_id" <<'SQL'
select 'paper_fills=' || count(*) from paper_fills
union all select 'settled_markets=' || count(*) from settled_markets
union all select 'paper_positions=' || count(*) from paper_positions
union all select 'paper_bankroll=' || count(*) from paper_bankroll
union all select 'fill_market_snapshots=' || count(*) from fill_market_snapshots
union all select 'archive_bankroll_stamp=' || count(*) from paper_bankroll_archive
  where activation_id = :'activation_id';
SQL
echo "Supabase activation-stamped reset complete; continue only through docs/35 and the locked driver."
