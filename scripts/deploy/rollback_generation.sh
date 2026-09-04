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
  verified)
    manifest_advance rolling_back
    state=rolling_back
    ;;
  rolling_back) ;;
  rolled_back)
    echo "activation_id=$activation_id state=rolled_back"
    exit 0
    ;;
  *) die "rollback requires verified or rolling_back state, found $state" ;;
esac

archive_path() {
  manifest_get "archive_artifacts.$1.path"
}

archive_sha() {
  manifest_get "archive_artifacts.$1.sha256"
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
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(archive_sha service_toml)" ]] || return 1
  [[ "$(sha256_file "$SERVICE_ENV")" == "$(archive_sha service_env)" ]] || return 1
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$(archive_sha pe_service)" ]] || return 1
}

# Validate every rollback source before any database or installed artifact is changed.
verify_archive_sources

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
  installed_old_artifacts || die "installed artifacts are not the archived generation"
  [[ "$(systemctl_active pe-service)" == true ]] || die "old generation is not active"
  [[ "$(systemctl_enabled pe-service)" == true ]] || die "old generation is not enabled"
  if [[ "${PE_ACTIVATION_TESTING:-0}" != 1 ]]; then
    local pid running
    pid=$(systemctl show pe-service -p MainPID --value)
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || die "pe-service has no MainPID"
    running=$(sha256_file "/proc/$pid/exe")
    [[ "$running" == "$(archive_sha pe_service)" ]] || die "running binary is not the archived generation"
  fi
}

# A crash after the old service started is adopted only after both durable sides
# independently prove rollback completion. This prevents a second service start.
if [[ "$(systemctl_active pe-service)" == true ]] && installed_old_artifacts && restored_counts_match; then
  verify_old_running
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c \
    'refresh materialized view concurrently wallet_live_stats_mv;'
  manifest_advance rolled_back
  echo "activation_id=$activation_id state=rolled_back (adopted running old generation)"
  exit 0
fi

# Any non-adoptable service is disabled and stopped before the database is touched.
"${SERVICE_MUTATE[@]}" disable pe-service
"${SERVICE_MUTATE[@]}" stop pe-service
[[ "$(systemctl_active pe-service)" == false ]] || die "pe-service is still active"
[[ "$(systemctl_enabled pe-service)" == false ]] || die "pe-service is still enabled"

psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v activation_id="$activation_id" \
  -f "$REPO_ROOT/scripts/paper_reset/restore_paper_state.sql"
maybe_crash rollback-db-restored
restored_counts_match || die "restored live counts do not match the activation archive"
psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c \
  'refresh materialized view concurrently wallet_live_stats_mv;'

atomic_adopt "$(archive_path service_toml)" "$SERVICE_CONFIG" 0644 rollback-config
atomic_adopt "$(archive_path service_env)" "$SERVICE_ENV" 0600 rollback-env
atomic_adopt "$(archive_path pe_service)" "$SERVICE_BINARY" 0755 rollback-binary
installed_old_artifacts || die "archived artifact restoration failed"

if [[ "$(systemctl_active pe-service)" == true ]]; then
  verify_old_running
else
  "${SERVICE_MUTATE[@]}" enable pe-service
  "${SERVICE_MUTATE[@]}" start pe-service
  maybe_crash rollback-started
  verify_old_running
fi

manifest_advance rolled_back
echo "activation_id=$activation_id state=rolled_back"
