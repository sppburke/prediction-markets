#!/usr/bin/env bash
# Locked, resumable rollback of one activation-stamped paper-state generation.

set -euo pipefail

# shellcheck source=generation_common.sh
source "$(cd "$(dirname "$0")" && pwd)/generation_common.sh"

usage() {
  echo "usage: $0 --activation-id ID [--simulate-crash-after BOUNDARY]" >&2
  exit 2
}

activation_id=
while (($#)); do
  case "$1" in
    --activation-id) [[ $# -ge 2 ]] || usage; activation_id=$2; shift 2 ;;
    --simulate-crash-after) [[ $# -ge 2 ]] || usage; SIMULATE_CRASH_AFTER=$2; shift 2 ;;
    *) usage ;;
  esac
done
[[ "$activation_id" =~ ^[A-Za-z0-9._-]+$ ]] || usage

for command in python3 sha256sum psql flock systemctl; do
  command -v "$command" >/dev/null || die "$command not installed"
done
: "${SUPABASE_DB_URL:?SUPABASE_DB_URL must be exported for rollback}"

acquire_deploy_lock
[[ -f "$MANIFEST" ]] || die "activation manifest is absent"
[[ "$(manifest_get activation_id)" == "$activation_id" ]] || die "manifest activation mismatch"

state=$(manifest_get state)
case "$state" in
  guarded|archived|reset|switched|started|verified)
    manifest_advance rolling_back
    state=rolling_back
    ;;
  rolling_back) ;;
  rolled_back)
    echo "activation_id=$activation_id state=rolled_back"
    exit 0
    ;;
  *) die "rollback requires a post-guarded or rolling_back state, found $state" ;;
esac

# Every rollback attempt first makes the service inert before consulting archive or database facts.
"${SERVICE_MUTATE[@]}" disable pe-service
"${SERVICE_MUTATE[@]}" stop pe-service
service_active=$(systemctl_active_state pe-service)
service_enabled=$(systemctl_enabled_state pe-service)
[[ "$service_active" == false ]] || die "pe-service is still active"
[[ "$service_enabled" == false ]] || die "pe-service is still enabled"

manifest_has() {
  python3 -c 'import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    if not isinstance(value,dict) or part not in value: raise SystemExit(1)
    value=value[part]' "$MANIFEST" "$1"
}

archive_path() {
  manifest_get "archive_artifacts.$1.path"
}

archive_sha() {
  manifest_get "archive_artifacts.$1.sha256"
}

old_sha() {
  manifest_get "old_installed_artifacts.$1.sha256"
}

verify_archive_sources() {
  local name path expected actual
  for name in service_toml service_env pe_service; do
    path=$(archive_path "$name")
    expected=$(archive_sha "$name")
    [[ -f "$path" ]] || die "archived $name source is absent: $path"
    actual=$(sha256_file "$path")
    [[ "$actual" == "$expected" ]] ||
      die "archived $name source hash mismatch: expected $expected, found $actual"
  done
}

installed_old_artifacts() {
  [[ -f "$SERVICE_CONFIG" && -f "$SERVICE_ENV" && -f "$SERVICE_BINARY" ]] || return 1
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(old_sha service_toml)" ]] || return 1
  [[ "$(sha256_file "$SERVICE_ENV")" == "$(old_sha service_env)" ]] || return 1
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$(old_sha pe_service)" ]] || return 1
}

restore_artifacts=false
if manifest_has archive_artifacts; then
  # Validate every rollback source before any database or installed artifact is changed.
  verify_archive_sources
  restore_artifacts=true
else
  installed_old_artifacts ||
    die "pre-T0 artifacts changed before the activation archive was recorded"
fi

restored_counts_match() {
  local answer
  answer=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
    "select case when not exists (
        (select idempotency_key,leader_wallet,source_trade_id,market_id,outcome_id,side,contracts,fill_price,entry_unix,event_seq,inserted_at from paper_fills)
        except all
        (select idempotency_key,leader_wallet,source_trade_id,market_id,outcome_id,side,contracts,fill_price,entry_unix,event_seq,inserted_at from paper_fills_archive where activation_id='$activation_id'))
      and not exists (
        (select idempotency_key,leader_wallet,source_trade_id,market_id,outcome_id,side,contracts,fill_price,entry_unix,event_seq,inserted_at from paper_fills_archive where activation_id='$activation_id')
        except all
        (select idempotency_key,leader_wallet,source_trade_id,market_id,outcome_id,side,contracts,fill_price,entry_unix,event_seq,inserted_at from paper_fills))
      and not exists ((select market_id,outcome_prices,credit_applied,settled_at_unix,inserted_at from settled_markets) except all (select market_id,outcome_prices,credit_applied,settled_at_unix,inserted_at from settled_markets_archive where activation_id='$activation_id'))
      and not exists ((select market_id,outcome_prices,credit_applied,settled_at_unix,inserted_at from settled_markets_archive where activation_id='$activation_id') except all (select market_id,outcome_prices,credit_applied,settled_at_unix,inserted_at from settled_markets))
      and not exists ((select market_id,outcome_id,long_contracts,short_contracts,updated_at from paper_positions) except all (select market_id,outcome_id,long_contracts,short_contracts,updated_at from paper_positions_archive where activation_id='$activation_id'))
      and not exists ((select market_id,outcome_id,long_contracts,short_contracts,updated_at from paper_positions_archive where activation_id='$activation_id') except all (select market_id,outcome_id,long_contracts,short_contracts,updated_at from paper_positions))
      and not exists ((select id,bankroll_str,updated_at from paper_bankroll) except all (select id,bankroll_str,updated_at from paper_bankroll_archive where activation_id='$activation_id'))
      and not exists ((select id,bankroll_str,updated_at from paper_bankroll_archive where activation_id='$activation_id') except all (select id,bankroll_str,updated_at from paper_bankroll))
      and not exists ((select idempotency_key,liquidity,volume,absorbable_usd_100bps,ask_levels_json,captured_at_unix,inserted_at from fill_market_snapshots) except all (select idempotency_key,liquidity,volume,absorbable_usd_100bps,ask_levels_json,captured_at_unix,inserted_at from fill_market_snapshots_archive where activation_id='$activation_id'))
      and not exists ((select idempotency_key,liquidity,volume,absorbable_usd_100bps,ask_levels_json,captured_at_unix,inserted_at from fill_market_snapshots_archive where activation_id='$activation_id') except all (select idempotency_key,liquidity,volume,absorbable_usd_100bps,ask_levels_json,captured_at_unix,inserted_at from fill_market_snapshots))
      then 'ok' else 'mismatch' end;")
  [[ "$answer" == ok ]]
}

verify_old_running() {
  local service_active service_enabled
  installed_old_artifacts || die "installed artifacts are not the archived generation"
  service_active=$(systemctl_active_state pe-service)
  service_enabled=$(systemctl_enabled_state pe-service)
  [[ "$service_active" == true ]] || die "old generation is not active"
  [[ "$service_enabled" == true ]] || die "old generation is not enabled"
  local pid running
  pid=$(systemctl show pe-service -p MainPID --value)
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || die "pe-service has no MainPID"
  running=$(sha256_file "$PROC_ROOT/$pid/exe")
  [[ "$running" == "$(old_sha pe_service)" ]] || die "running binary is not the archived generation"
}

archive_columns_required=false
manifest_requires_archive_columns && archive_columns_required=true
archive_counts=$(activation_archive_counts "$activation_id" "$archive_columns_required")
if activation_archive_stamp_exists "$archive_counts"; then
  activation_archive_counts_match_pre_reset "$archive_counts" ||
    die "activation archive counts do not match the recorded pre-reset live counts"
  read -r _ _ _ bankroll_archived _ <<< "$archive_counts"
  [[ "$bankroll_archived" -gt 0 ]] || die "activation archive lacks its guaranteed bankroll stamp"
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v activation_id="$activation_id" \
    -f "$REPO_ROOT/scripts/paper_reset/restore_paper_state.sql"
  maybe_crash rollback-db-restored
  restored_counts_match || die "restored live counts do not match the activation archive"
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c \
    'refresh materialized view concurrently wallet_live_stats_mv;'
fi

if [[ "$restore_artifacts" == true ]]; then
  atomic_adopt "$(archive_path service_toml)" "$SERVICE_CONFIG" 0644 rollback-config
  atomic_adopt "$(archive_path service_env)" "$SERVICE_ENV" 0600 rollback-env
  atomic_adopt "$(archive_path pe_service)" "$SERVICE_BINARY" 0755 rollback-binary
fi
installed_old_artifacts || die "pre-T0 artifact restoration or verification failed"

"${SERVICE_MUTATE[@]}" enable pe-service
"${SERVICE_MUTATE[@]}" start pe-service
maybe_crash rollback-started
verify_old_running

manifest_advance rolled_back
echo "activation_id=$activation_id state=rolled_back"
