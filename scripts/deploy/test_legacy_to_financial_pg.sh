#!/usr/bin/env bash
# Real-PostgreSQL Legacy17 -> Financial15 transition proof for issue #545.

set -euo pipefail

: "${PG_ADMIN_URL:?PG_ADMIN_URL is required}"
: "${PG_LEGACY_URL:?PG_LEGACY_URL is required}"
: "${RUNNER_TEMP:?RUNNER_TEMP is required}"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export SUPABASE_DB_URL=$PG_LEGACY_URL
# generation_common.sh is resolved from this script's absolute directory.
# shellcheck disable=SC1091
source "$SCRIPT_DIR/generation_common.sh"

current_step=initialization

report_failure() {
  local status=$?
  echo "CIPG FAIL: $current_step" >&2
  exit "$status"
}

trap report_failure ERR

start_step() {
  current_step=$1
  echo "CIPG: $current_step"
}

pass_step() {
  echo "CIPG PASS: $current_step"
}

psql_admin() {
  psql_url PG_ADMIN_URL "$@"
}

install_roles() {
  psql_admin -v ON_ERROR_STOP=1 <<'SQL'
do $$ begin
  if not exists (select 1 from pg_roles where rolname = 'anon') then
    create role anon nologin nobypassrls;
  end if;
  if not exists (select 1 from pg_roles where rolname = 'authenticated') then
    create role authenticated nologin nobypassrls;
  end if;
  if not exists (select 1 from pg_roles where rolname = 'service_role') then
    create role service_role nologin bypassrls;
  end if;
  -- Reruns on a reused server must still model Supabase's role attributes exactly.
  alter role anon nobypassrls;
  alter role authenticated nobypassrls;
  alter role service_role bypassrls;
end $$;
SQL
}

# Build the service once and run the binary: `cargo run` per call recompiled pe-service on every
# one of this scenario's ten calls in CI, about 27 s each (#664). Select the service binary
# because this package also owns the live canary.
pe_service_binary="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/debug/pe-service"
(cd "$REPO_ROOT" && cargo build -p pe-service --all-features --bin pe-service)
[[ -f "$pe_service_binary" ]] || die "cargo build did not produce $pe_service_binary"

run_pe_service() {
  (
    cd "$REPO_ROOT"
    "$pe_service_binary" "$@"
  )
}

run_financial_era() {
  PE_SUPABASE_AUTHORITATIVE=true \
    PE_SUPABASE_URL=https://ci-financial-era.invalid \
    PE_SUPABASE_SECRET_KEY=ci-financial-era-service-role \
    run_pe_service "$@"
}

create_isolated_database() {
  local database_count
  database_count=$(psql_admin -v ON_ERROR_STOP=1 -Atc \
    "select count(*) from pg_database where datname = 'pe_legacy_545';")
  case "$database_count" in
    0) psql_admin -v ON_ERROR_STOP=1 -c 'create database pe_legacy_545;' ;;
    1) ;;
    *) die "isolated database census is invalid: $database_count" ;;
  esac
}

apply_financial_config_migration() {
  local legacy_key_count financial_key_count
  legacy_key_count=$(psql_service_db -v ON_ERROR_STOP=1 -Atc \
    "select count(*) from service_config where key in ('fill_mode','polymarket_fee_rate');")
  financial_key_count=$(psql_service_db -v ON_ERROR_STOP=1 -Atc \
    "select count(*) from service_config where key not in ('risk_halt_release_hash');")
  if [[ "$legacy_key_count" == 2 && "$financial_key_count" == 17 ]]; then
    psql_service_db -v ON_ERROR_STOP=1 \
      -f "$REPO_ROOT/scripts/migrate_service_config_545.sql"
  elif [[ "$legacy_key_count" != 0 || "$financial_key_count" != 15 ]]; then
    die "service_config is neither the exact Legacy17 nor Financial15 migration boundary"
  fi
}

retry_manifest_patch() {
  local boundary=$1 patch=$2 first_sha256
  manifest_patch_boundary "$boundary" "$patch"
  first_sha256=$(sha256_file "$MANIFEST")
  manifest_patch_boundary "$boundary" "$patch"
  [[ "$first_sha256" == "$(sha256_file "$MANIFEST")" ]] ||
    die "$boundary manifest retry changed the receipt"
}

retry_manifest_advance() {
  local state=$1 patch=${2:-'{}'} first_sha256
  manifest_advance "$state" "$patch"
  first_sha256=$(sha256_file "$MANIFEST")
  manifest_advance "$state" "$patch"
  [[ "$first_sha256" == "$(sha256_file "$MANIFEST")" ]] ||
    die "$state manifest retry changed the state"
}

start_step "create isolated roles and legacy database"
install_roles
install_roles
create_isolated_database
create_isolated_database
pass_step

start_step "install pre-Start schema and seed Legacy17 plus the release row"
legacy_schema="$REPO_ROOT/scripts/fixtures/pre545/supabase_paper_state_schema.sql"
legacy_multi_account_schema="$REPO_ROOT/scripts/fixtures/pre545/supabase_multi_account_live_schema.sql"
financial_multi_account_schema="$REPO_ROOT/scripts/supabase_multi_account_live_schema.sql"
[[ -s "$legacy_schema" ]] || die "pre-Start paper-state schema is empty"
[[ -s "$legacy_multi_account_schema" ]] || die "pre-Start multi-account schema is empty"
[[ -s "$financial_multi_account_schema" ]] || die "financial multi-account schema is empty"
psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_schema.sql"
psql_service_db -v ON_ERROR_STOP=1 -f "$legacy_schema"
psql_service_db -v ON_ERROR_STOP=1 -f "$legacy_multi_account_schema"
psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_wallet_live_stats_mv.sql"
# Retry every installed-schema boundary once against the same PostgreSQL database.
psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_schema.sql"
psql_service_db -v ON_ERROR_STOP=1 -f "$legacy_schema"
psql_service_db -v ON_ERROR_STOP=1 -f "$legacy_multi_account_schema"
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
insert into service_config (key, value, value_type) values
  ('fill_mode', 'clob_best_ask', 'text'),
  ('polymarket_fee_rate', '0.04', 'decimal'),
  ('kelly_fraction_override', '0.01', 'decimal');
insert into service_config
  (key, value, value_type, description, updated_at, updated_by)
values
  ('risk_halt_release_hash', repeat('a', 64), 'text',
   'ci issue 545 release-row preservation proof',
   timestamptz '2000-01-01 00:00:00+00', 'ci-545');

do $$
declare
  legacy_keys constant text[] := array[
    'active_watchlist_size','mode','max_fill_price','min_fill_price',
    'min_resolution_horizon_secs','max_resolution_horizon_secs','fill_mode',
    'price_impact_cap_bps','flip_human_approved',
    'kelly_fraction_above_default_human_approved','polymarket_fee_rate',
    'kelly_fraction_override','per_trade_cap','slippage_rate','sizing_mode',
    'sizing_dollar_usd','sizing_contracts'
  ];
begin
  if exists (
       select key from service_config
       except
       select key from unnest(legacy_keys || array['risk_halt_release_hash']::text[]) key
     ) or exists (
       select key from unnest(legacy_keys || array['risk_halt_release_hash']::text[]) key
       except
       select key from service_config
     ) then
    raise exception 'seeded service_config does not equal Legacy17 plus the release row';
  end if;
end $$;

insert into paper_bankroll (id, bankroll_str) values (0, '9998.625');
insert into paper_positions
  (market_id, outcome_id, long_contracts, short_contracts)
values ('legacy-market', 0, 1, 0);
insert into paper_fills
  (idempotency_key, leader_wallet, source_trade_id, market_id, outcome_id,
   side, contracts, fill_price, entry_unix, event_seq)
values ('legacy-fill', '0x1111111111111111111111111111111111111111',
        'legacy-source', 'legacy-market', 0, 'buy', 1.25, 0.55, 100, 7);
insert into settled_markets
  (market_id, outcome_prices, credit_applied, settled_at_unix)
values ('legacy-settled-market', '[1,0]'::jsonb, 0.625, 101);
insert into fill_market_snapshots
  (idempotency_key, captured_at_unix)
values ('legacy-fill', 100);
insert into accounts (account_id, is_primary)
values ('legacy-primary', true);
insert into live_positions
  (account_id, market_id, outcome_id, long_contracts, short_contracts, cost_basis)
values ('legacy-primary', 'legacy-live-market', 0, 7, 3, 1.5);
SQL
pass_step

start_step "refresh and prove the public projection before the transition"
psql_service_db -v ON_ERROR_STOP=1 -c \
  'refresh materialized view concurrently public.wallet_live_stats_mv;'
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
do $$ begin
  if (select count(*) from public.wallet_live_stats_mv) <> 1
     or (select count(*) from public.wallet_live_stats) <> 1 then
    raise exception 'pre-transition public projection does not contain the legacy wallet';
  end if;
end $$;
SQL
pass_step

start_step "run the driver-owned Legacy17 verifier"
verify_legacy_service_contract
verify_legacy_service_contract
pass_step

start_step "prove the Legacy17 verifier refuses a malformed release row"
malformed_log="$RUNNER_TEMP/legacy-service-contract-malformed.log"
psql_service_db -v ON_ERROR_STOP=1 -c \
  "update service_config set value = 'malformed' where key = 'risk_halt_release_hash';"
if verify_legacy_service_contract >"$malformed_log" 2>&1; then
  die "Legacy17 verifier accepted a malformed risk_halt_release_hash"
fi
if ! grep -Fq 'optional risk_halt_release_hash is malformed' "$malformed_log"; then
  sed -n '1,120p' "$malformed_log" >&2
  die "Legacy17 verifier refused the malformed row for an unexpected reason"
fi
echo "CIPG PASS: Legacy17 verifier refused the malformed release row"
psql_service_db -v ON_ERROR_STOP=1 -c \
  "update service_config set value = repeat('a', 64) where key = 'risk_halt_release_hash';"
verify_legacy_service_contract
pass_step

start_step "prepare a fresh financial-era fixture through the Rust owner"
financial_fixture=$(mktemp -d "$RUNNER_TEMP/cipg-financial-era.XXXXXX")
paper_log="$financial_fixture/paper.log"
source_log="$financial_fixture/source_events.log"
live_journal="$financial_fixture/live_journal.log"
paper_state="$financial_fixture/paper_state.db"
status_path="$financial_fixture/status.json"
fixture_config="$financial_fixture/service.toml"
fixture_environment="$financial_fixture/service.env"
financial_config_rows="$financial_fixture/service_config.financial15.json"
activation_manifest="$financial_fixture/pe-financial-era.json"

# These are fresh version-one event-log files. The Rust prepare/start owners verify all three,
# while --rebuild-state creates the SQLite schema and bankroll through PaperStateDb.
printf 'EDGE\001' > "$paper_log"
printf 'EDGE\001' > "$source_log"
printf 'EDGE\001' > "$live_journal"
chmod 0600 "$paper_log" "$source_log" "$live_journal"
printf '%s\n' \
  '{"live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"stale":false,"accounts":[]}}' \
  > "$status_path"
python3 -c 'import json,sys
paper,source,status,state=sys.argv[1:]
print("event_log_path = "+json.dumps(paper))
print("source_event_log_path = "+json.dumps(source))
print("status_path = "+json.dumps(status))
print("paper_state_db_path = "+json.dumps(state))
print("bankroll_usd = \"10000\"")' \
  "$paper_log" "$source_log" "$status_path" "$paper_state" > "$fixture_config"
printf '%s\n' \
  'PE_SUPABASE_AUTHORITATIVE=true' \
  'PE_SUPABASE_URL=https://ci-financial-era.invalid' \
  'PE_SUPABASE_SECRET_KEY=ci-financial-era-service-role' \
  > "$fixture_environment"
chmod 0600 "$fixture_config" "$fixture_environment" "$status_path"
run_pe_service "$fixture_config" --rebuild-state >/dev/null

psql_service_db -v ON_ERROR_STOP=1 -Atc \
  "select coalesce(json_agg(json_build_object('key',key,'value',value,'value_type',value_type) order by key),'[]'::json)::text
     from service_config
    where key not in ('fill_mode','polymarket_fee_rate','risk_halt_release_hash');" \
  > "$financial_config_rows"
[[ -s "$financial_config_rows" ]] || die "Financial15 configuration export is empty"

staged_identity=$(run_pe_service --verify-staged-identity)
read -r target_revision artifact_blake3 < <(parse_staged_identity <<< "$staged_identity") ||
  die "pe-service staged identity is malformed"
run_pe_service --verify-staged-identity "$target_revision" "$artifact_blake3" >/dev/null
run_pe_service --verify-staged-identity "$target_revision" "$artifact_blake3" >/dev/null
artifact_sha256=$(sha256_file "$pe_service_binary")
config_sha256=$(sha256_file "$fixture_config")
environment_sha256=$(sha256_file "$fixture_environment")
static_config_hash=$(python3 -c 'import hashlib,json,sys
config_hash,environment_hash=sys.argv[1:]
payload=json.dumps({"config_sha256":config_hash,"environment_sha256":environment_hash},sort_keys=True,separators=(",",":")).encode()
print(hashlib.sha256(b"prediction-edge/effective-static-config-v1\0"+payload).hexdigest())' \
  "$config_sha256" "$environment_sha256")

initial_manifest=$(python3 -c 'import json,sys
(path,activation,generation,bankroll,revision,artifact,static,batch,paper,source,live,state,
 artifact_sha,config_sha,environment_sha)=sys.argv[1:]
value={
 "kind":"financial-era-v1","state":"prepared","activation_id":activation,
 "generation":generation,"fresh_bankroll":int(bankroll)*1000000,
 "target_revision":revision,"artifact_blake3":artifact,"static_config_hash":static,
 "ranking_batch_id":int(batch),"membership":[],"schema_version":3,"parser_version":1,
 "financial_semantic_version":1,"start_unix":1700000000,
 "paths":{"paper_log":paper,"source_log":source,"live_journal":live,"paper_state":state},
 "old_artifact_sha256":artifact_sha,"target_artifact_sha256":artifact_sha,
 "old_config_sha256":config_sha,"target_config_sha256":config_sha,
 "old_environment_sha256":environment_sha,"target_environment_sha256":environment_sha,
 "preparation":None
}
print(json.dumps(value,sort_keys=True,separators=(",",":")))' \
  "$activation_manifest" ci-545-transition "$financial_fixture" 10000 "$target_revision" \
  "$artifact_blake3" "$static_config_hash" 545 "$paper_log" "$source_log" "$live_journal" \
  "$paper_state" "$artifact_sha256" "$config_sha256" "$environment_sha256")
# Use the activation driver's fsynced manifest owner for the fixture and every receipt below.
MANIFEST=$activation_manifest
atomic_manifest_json "$initial_manifest" prepared
initial_manifest_sha256=$(sha256_file "$activation_manifest")
atomic_manifest_json "$initial_manifest" prepared
[[ "$initial_manifest_sha256" == "$(sha256_file "$activation_manifest")" ]] ||
  die "prepared-manifest retry changed the initial identity"

durable_before=$(printf '%s:%s:%s:%s' \
  "$(sha256_file "$paper_log")" "$(sha256_file "$source_log")" \
  "$(sha256_file "$live_journal")" "$(sha256_file "$paper_state")")
prepare_first=$(run_financial_era "$fixture_config" --financial-era=prepare \
  --activation-manifest="$activation_manifest" \
  --financial-config-rows="$financial_config_rows")
prepare_retry=$(run_financial_era "$fixture_config" --financial-era=prepare \
  --activation-manifest="$activation_manifest" \
  --financial-config-rows="$financial_config_rows")
[[ "$prepare_first" == "$prepare_retry" ]] || die "financial-era prepare retry changed its result"
durable_after=$(printf '%s:%s:%s:%s' \
  "$(sha256_file "$paper_log")" "$(sha256_file "$source_log")" \
  "$(sha256_file "$live_journal")" "$(sha256_file "$paper_state")")
[[ "$durable_before" == "$durable_after" ]] || die "financial-era prepare mutated a durable fixture"

# Match the driver's durable preparation receipt followed by its prepared -> guarded advance,
# retrying both manifest boundaries with the same identities.
preparation_patch=$(python3 -c 'import json,sys
print(json.dumps({"preparation":json.loads(sys.argv[1])},sort_keys=True,separators=(",",":")))' \
  "$prepare_first")
retry_manifest_patch preparation "$preparation_patch"
retry_manifest_advance guarded
pass_step

start_step "archive and reset the pre-Start legacy state"
retry_manifest_patch remote-archive-intent '{"remote_archive_intent":true}'
psql_service_db -v ON_ERROR_STOP=1 -v activation_id=ci-545-transition \
  -v bankroll=10000 -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
# The archive owner recognizes the completed activation stamp and proves the fresh live book.
psql_service_db -v ON_ERROR_STOP=1 -v activation_id=ci-545-transition \
  -v bankroll=10000 -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
do $$ begin
  if (select count(*) from paper_fills) <> 0
     or (select count(*) from paper_positions) <> 0
     or (select count(*) from settled_markets) <> 0
     or (select count(*) from fill_market_snapshots) <> 0
     or (select count(*) from paper_bankroll) <> 1
     or not exists (
       select 1 from paper_bankroll where id = 0 and bankroll_str::numeric = 10000
     ) then
    raise exception 'legacy archive/reset did not leave the exact fresh live book';
  end if;
  if not exists (
       select 1 from paper_fills_archive
        where activation_id = 'ci-545-transition' and contracts = 1.25
     ) or not exists (
       select 1 from paper_bankroll_archive
        where activation_id = 'ci-545-transition' and bankroll_str::numeric = 9998.625
     ) or not exists (
       select 1 from settled_markets_archive
        where activation_id = 'ci-545-transition' and credit_applied = 0.625
     ) then
    raise exception 'legacy archive did not retain the pre-Start state exactly';
  end if;
end $$;
SQL
retry_manifest_patch remote-archived '{"remote_archive_completed":true}'
pass_step

start_step "execute physical QualificationStarted through the Rust owner"
retry_manifest_patch qualification-start-intent '{"qualification_start_intent":true}'
start_first=$(run_financial_era "$fixture_config" --financial-era=start \
  --activation-manifest="$activation_manifest" \
  --financial-config-rows="$financial_config_rows")
start_retry=$(run_financial_era "$fixture_config" --financial-era=start \
  --activation-manifest="$activation_manifest" \
  --financial-config-rows="$financial_config_rows")
[[ "$start_first" == "$start_retry" ]] || die "financial-era Start retry changed its receipt"
rollback_check_first=$(run_financial_era "$fixture_config" --financial-era=rollback-check \
  --activation-manifest="$activation_manifest")
rollback_check_retry=$(run_financial_era "$fixture_config" --financial-era=rollback-check \
  --activation-manifest="$activation_manifest")
[[ "$rollback_check_first" == "$rollback_check_retry" ]] || \
  die "financial-era rollback-check retry changed its result"
python3 -c 'import json,sys
start=json.loads(sys.argv[1]); preparation=json.loads(sys.argv[2])
first=json.loads(sys.argv[3]); retry=json.loads(sys.argv[4])
if start != preparation.get("expected_receipt"):
 raise SystemExit("physical Start receipt differs from prepared identity")
for result in (first,retry):
 if result.get("complete_start") is not True or result.get("receipt") != start:
  raise SystemExit("rollback-check did not find the physical manifest-bound Start")' \
  "$start_first" "$prepare_first" "$rollback_check_first" "$rollback_check_retry"
mapfile -t start_identity < <(python3 -c 'import json,sys
value=json.loads(sys.argv[1]); print(value["sequence"]); print(value["this_hash"])' "$start_first")
[[ ${#start_identity[@]} -eq 2 && "${start_identity[0]}" =~ ^[0-9]+$ && \
   "${start_identity[1]}" =~ ^[0-9a-f]{64}$ ]] || die "physical Start receipt is invalid"
start_seq=${start_identity[0]}
start_hash=${start_identity[1]}
start_receipt_patch=$(python3 -c 'import json,sys
print(json.dumps({"start_receipt":json.loads(sys.argv[1])},sort_keys=True,separators=(",",":")))' \
  "$start_first")
retry_manifest_patch qualification-started "$start_receipt_patch"
pass_step

start_step "install financial schema, seed the physical Start receipt, and read it back"
retry_manifest_patch authority-schema-intent '{"authority_schema_intent":true}'
psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_paper_state_schema.sql"
# The schema itself is an idempotent boundary.
psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_paper_state_schema.sql"
retry_manifest_patch authority-schema-installed '{"authority_schema_installed":true}'
retry_manifest_patch live-schema-intent '{"live_schema_intent":true}'
psql_service_db -v ON_ERROR_STOP=1 -f "$financial_multi_account_schema"
# A retry from the intent receipt must accept the pre-545 bigint shape and the migrated shape.
psql_service_db -v ON_ERROR_STOP=1 -f "$financial_multi_account_schema"
retry_manifest_patch live-schema-installed '{"live_schema_installed":true}'
live_position_types=$(psql_service_db -v ON_ERROR_STOP=1 -F '|' -Atc \
  "select column_name, data_type
     from information_schema.columns
    where table_schema = 'public'
      and table_name = 'live_positions'
      and column_name in ('long_contracts', 'short_contracts')
    order by column_name;")
[[ "$live_position_types" == $'long_contracts|numeric\nshort_contracts|numeric' ]] ||
  die "live position quantity types did not migrate to numeric: $live_position_types"
legacy_live_position=$(psql_service_db -v ON_ERROR_STOP=1 -Atc \
  "select long_contracts::text || '|' || short_contracts::text
     from live_positions
    where account_id = 'legacy-primary'
      and market_id = 'legacy-live-market'
      and outcome_id = 0;")
[[ "$legacy_live_position" == '7|3' ]] ||
  die "legacy whole-contract live position did not survive: $legacy_live_position"
psql_service_db -v ON_ERROR_STOP=1 -c \
  "update live_positions
      set long_contracts = 1.125, short_contracts = 0.375
    where account_id = 'legacy-primary'
      and market_id = 'legacy-live-market'
      and outcome_id = 0;"
fractional_live_position=$(psql_service_db -v ON_ERROR_STOP=1 -Atc \
  "select long_contracts::text || '|' || short_contracts::text
     from live_positions
    where account_id = 'legacy-primary'
      and market_id = 'legacy-live-market'
      and outcome_id = 0;")
[[ "$fractional_live_position" == '1.125|0.375' ]] ||
  die "fractional live position did not round-trip exactly: $fractional_live_position"
# psql does not interpolate variables inside -c strings; feed the statement on standard input.
seed_start() {
  psql_service_db -v ON_ERROR_STOP=1 -At \
    -v start_seq="$start_seq" -v start_hash="$start_hash" <<'SQL'
select seed_financial_start(:'start_seq'::bigint, :'start_hash');
SQL
}
authority_start_first=$(seed_start)
authority_start_retry=$(seed_start)
authority_start_readback=$(psql_service_db -v ON_ERROR_STOP=1 -Atc \
  "select json_build_object(
     'bankroll',(select bankroll_str from paper_bankroll where id=0),
     'start_seq',(select start_seq from paper_bankroll where id=0),
     'start_hash',(select start_hash from paper_bankroll where id=0),
     'last_prepared_seq',(select last_prepared_seq from paper_bankroll where id=0))::text;")
python3 -c 'import decimal,json,sys
sequence=int(sys.argv[1]); digest=sys.argv[2]
first=json.loads(sys.argv[3]); retry=json.loads(sys.argv[4]); row=json.loads(sys.argv[5])
if first.get("outcome") != "applied": raise SystemExit("first authority Start was not applied")
if retry.get("outcome") != "existing": raise SystemExit("authority Start retry was not existing")
for result in (first,retry):
 if result.get("start_seq") != sequence or result.get("start_hash") != digest:
  raise SystemExit("authority Start result differs from the physical receipt")
if (row.get("start_seq") != sequence or row.get("start_hash") != digest
    or row.get("last_prepared_seq") is not None
    or decimal.Decimal(row.get("bankroll")) != decimal.Decimal("10000")):
 raise SystemExit("authority Start read-back differs from the physical receipt")' \
  "$start_seq" "$start_hash" "$authority_start_first" "$authority_start_retry" \
  "$authority_start_readback"
authority_start_patch=$(python3 -c 'import json,sys
first=json.loads(sys.argv[1]); row=json.loads(sys.argv[2])
print(json.dumps({"authority_start_seeded":True,"authority_start_proof":[first,row]},sort_keys=True,separators=(",",":")))' \
  "$authority_start_first" "$authority_start_readback")
retry_manifest_patch authority-start-seeded "$authority_start_patch"
pass_step

start_step "migrate Legacy17 to Financial15 and preserve the release row"
retry_manifest_patch financial-config-migration-intent \
  '{"financial_config_migration_intent":true}'
apply_financial_config_migration
apply_financial_config_migration
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
do $$
declare
  financial_keys constant text[] := array[
    'active_watchlist_size','mode','max_fill_price','min_fill_price',
    'min_resolution_horizon_secs','max_resolution_horizon_secs',
    'price_impact_cap_bps','flip_human_approved',
    'kelly_fraction_above_default_human_approved','kelly_fraction_override',
    'per_trade_cap','slippage_rate','sizing_mode','sizing_dollar_usd','sizing_contracts'
  ];
begin
  if exists (
       select key from service_config where key <> 'risk_halt_release_hash'
       except select key from unnest(financial_keys) key
     ) or exists (
       select key from unnest(financial_keys) key
       except select key from service_config where key <> 'risk_halt_release_hash'
     ) or (select count(*) from service_config) <> 16 then
    raise exception 'service_config does not equal Financial15 plus the release row';
  end if;
  if not exists (
       select 1 from service_config
        where key = 'risk_halt_release_hash'
          and value = repeat('a', 64)
          and value_type = 'text'
          and description = 'ci issue 545 release-row preservation proof'
          and updated_at = timestamptz '2000-01-01 00:00:00+00'
          and updated_by = 'ci-545'
     ) then
    raise exception 'risk_halt_release_hash did not survive the migration untouched';
  end if;
end $$;
SQL
retry_manifest_patch financial-config-migrated '{"financial_config_migrated":true}'
pass_step

start_step "refresh the public projection at the forward activation boundary"
retry_manifest_patch wallet-live-stats-refresh-intent \
  '{"wallet_live_stats_refresh_intent":true}'
psql_service_db -v ON_ERROR_STOP=1 -c \
  'refresh materialized view concurrently public.wallet_live_stats_mv;'
# Model a crash after REFRESH committed but before its completed manifest receipt.
psql_service_db -v ON_ERROR_STOP=1 -c \
  'refresh materialized view concurrently public.wallet_live_stats_mv;'
retry_manifest_patch wallet-live-stats-refreshed '{"wallet_live_stats_refreshed":true}'
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
do $$ begin
  if (select count(*) from public.wallet_live_stats_mv)
       <> (select count(*) from public.wallet_live_stats)
     or (select count(*) from public.wallet_live_stats_mv) <> 0 then
    raise exception 'forward public projection does not equal the fresh base view';
  end if;
end $$;
SQL
pass_step

start_step "round-trip fractional financial state through archive and restore"
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
update paper_bankroll
   set bankroll_str = '9998.625', last_prepared_seq = 44
 where id = 0;
insert into paper_positions
  (market_id, outcome_id, long_contracts, short_contracts)
values ('financial-market', 0, 1.125, 0.375);
insert into paper_fills
  (idempotency_key, leader_wallet, source_trade_id, market_id, outcome_id,
   side, contracts, fill_price, principal, fee, entry_unix, event_seq, prepared_seq)
values ('financial-fill', '0x2222222222222222222222222222222222222222',
        'financial-source', 'financial-market', 0, 'buy', 1.25, 0.55,
        0.6875, 0.001, 200, 43, 43);
insert into settled_markets
  (market_id, outcome_prices, credit_applied, settled_at_unix, prepared_seq)
values ('financial-settled-market', '[1,0]'::jsonb, 0.625, 201, 44);
insert into fill_market_snapshots
  (idempotency_key, captured_at_unix)
values ('financial-fill', 200);
SQL
psql_service_db -v ON_ERROR_STOP=1 -c \
  'refresh materialized view concurrently public.wallet_live_stats_mv;'
psql_service_db -v ON_ERROR_STOP=1 -v activation_id=ci-545-fractional \
  -v bankroll=10000 -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
psql_service_db -v ON_ERROR_STOP=1 -v activation_id=ci-545-fractional \
  -v bankroll=10000 -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
psql_service_db -v ON_ERROR_STOP=1 -c \
  'refresh materialized view concurrently public.wallet_live_stats_mv;'
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
do $$ begin
  if (select count(*) from paper_fills) <> 0
     or (select count(*) from paper_positions) <> 0
     or (select count(*) from settled_markets) <> 0 then
    raise exception 'fractional archive did not clear the live financial book';
  end if;
  if not exists (
       select 1 from paper_fills_archive
        where activation_id = 'ci-545-fractional'
          and contracts = 1.25 and principal = 0.6875 and fee = 0.001
     ) or not exists (
       select 1 from paper_positions_archive
        where activation_id = 'ci-545-fractional'
          and long_contracts = 1.125 and short_contracts = 0.375
     ) then
    raise exception 'fractional financial values were not archived exactly';
  end if;
end $$;
SQL
psql_service_db -v ON_ERROR_STOP=1 -v activation_id=ci-545-fractional \
  -f "$REPO_ROOT/scripts/paper_reset/restore_paper_state.sql"
psql_service_db -v ON_ERROR_STOP=1 -v activation_id=ci-545-fractional \
  -f "$REPO_ROOT/scripts/paper_reset/restore_paper_state.sql"
retry_manifest_patch rollback-wallet-live-stats-refresh-intent \
  '{"rollback_wallet_live_stats_refresh_intent":true}'
psql_service_db -v ON_ERROR_STOP=1 -c \
  'refresh materialized view concurrently public.wallet_live_stats_mv;'
# Model a crash after the rollback REFRESH committed but before its completed receipt.
psql_service_db -v ON_ERROR_STOP=1 -c \
  'refresh materialized view concurrently public.wallet_live_stats_mv;'
retry_manifest_patch rollback-wallet-live-stats-refreshed \
  '{"rollback_wallet_live_stats_refreshed":true}'
psql_service_db -v ON_ERROR_STOP=1 -v start_seq="$start_seq" -v start_hash="$start_hash" <<'SQL'
begin;
select set_config('pe.start_seq', :'start_seq', true);
select set_config('pe.start_hash', :'start_hash', true);
do $$ begin
  if not exists (
       select 1 from paper_fills
        where idempotency_key = 'financial-fill'
          and contracts = 1.25 and principal = 0.6875 and fee = 0.001
     ) or not exists (
       select 1 from paper_positions
        where market_id = 'financial-market' and outcome_id = 0
          and long_contracts = 1.125 and short_contracts = 0.375
     ) or not exists (
       select 1 from paper_bankroll
        where bankroll_str::numeric = 9998.625
          and start_seq = current_setting('pe.start_seq')::bigint
          and start_hash = current_setting('pe.start_hash')
          and last_prepared_seq = 44
     ) or not exists (
       select 1 from settled_markets
        where market_id = 'financial-settled-market'
          and credit_applied = 0.625 and prepared_seq = 44
     ) or (select count(*) from public.wallet_live_stats_mv)
          <> (select count(*) from public.wallet_live_stats)
       or (select count(*) from public.wallet_live_stats_mv) <> 1
     then
    raise exception 'fractional archive/restore round trip changed the financial book';
  end if;
end $$;
commit;
SQL
pass_step

start_step "assert the financial callable inventory and service-role-only grants"
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
do $$
declare
  role_name text;
  expected_execute boolean;
begin
  if exists (
       select p.proname, pg_get_function_identity_arguments(p.oid)
         from pg_proc p
         join pg_namespace n on n.oid = p.pronamespace
        where n.nspname = 'public'
          and p.proname in (
            'commit_fill','apply_resolution','commit_fill_v2','apply_resolution_v2',
            'seed_financial_start'
          )
       except
       select * from (values
         ('apply_resolution_v2',
          'p_start_seq bigint, p_start_hash text, p_expected_prior_seq bigint, p_prepared_seq bigint, p_condition_id text, p_payout_by_outcome_index jsonb, p_settled_at_unix bigint'),
         ('commit_fill_v2',
          'p_start_seq bigint, p_start_hash text, p_expected_prior_seq bigint, p_prepared_seq bigint, p_idempotency_key text, p_leader_wallet text, p_source_trade_id text, p_market_id text, p_outcome_id integer, p_side text, p_quantity numeric, p_fill_price numeric, p_principal numeric, p_fee numeric, p_entry_unix bigint'),
         ('seed_financial_start', 'p_start_seq bigint, p_start_hash text')
       ) expected(proname, identity_arguments)
     ) or exists (
       select * from (values
         ('apply_resolution_v2',
          'p_start_seq bigint, p_start_hash text, p_expected_prior_seq bigint, p_prepared_seq bigint, p_condition_id text, p_payout_by_outcome_index jsonb, p_settled_at_unix bigint'),
         ('commit_fill_v2',
          'p_start_seq bigint, p_start_hash text, p_expected_prior_seq bigint, p_prepared_seq bigint, p_idempotency_key text, p_leader_wallet text, p_source_trade_id text, p_market_id text, p_outcome_id integer, p_side text, p_quantity numeric, p_fill_price numeric, p_principal numeric, p_fee numeric, p_entry_unix bigint'),
         ('seed_financial_start', 'p_start_seq bigint, p_start_hash text')
       ) expected(proname, identity_arguments)
       except
       select p.proname, pg_get_function_identity_arguments(p.oid)
         from pg_proc p
         join pg_namespace n on n.oid = p.pronamespace
        where n.nspname = 'public'
          and p.proname in (
            'commit_fill','apply_resolution','commit_fill_v2','apply_resolution_v2',
            'seed_financial_start'
          )
     ) then
    raise exception 'financial callable inventory differs from PG-545-ACL';
  end if;

  for role_name, expected_execute in
    select * from (values
      ('anon'::text, false),
      ('authenticated'::text, false),
      ('service_role'::text, true)
    ) roles(role_name, expected_execute)
  loop
    if has_function_privilege(
         role_name,
         'commit_fill_v2(bigint,text,bigint,bigint,text,text,text,text,integer,text,numeric,numeric,numeric,numeric,bigint)',
         'execute'
       ) is distinct from expected_execute
       or has_function_privilege(
         role_name,
         'apply_resolution_v2(bigint,text,bigint,bigint,text,jsonb,bigint)',
         'execute'
       ) is distinct from expected_execute
       or has_function_privilege(
         role_name, 'seed_financial_start(bigint,text)', 'execute'
       ) is distinct from expected_execute then
      raise exception 'financial callable ACL differs for role %', role_name;
    end if;
  end loop;
end $$;
SQL
pass_step

start_step "prove protected account reads and the rehearsal census against the candidate schema"
for table_name in accounts account_credentials; do
  denied_log="$RUNNER_TEMP/anon-${table_name}-select.log"
  if psql_service_db -v ON_ERROR_STOP=1 >"$denied_log" 2>&1 <<SQL
begin;
set local role anon;
select count(*) from public.$table_name;
commit;
SQL
  then
    die "anon unexpectedly selected from public.$table_name"
  fi
  if ! grep -Fq "permission denied for table $table_name" "$denied_log"; then
    sed -n '1,120p' "$denied_log" >&2
    die "anon public.$table_name read failed for an unexpected reason"
  fi
done

service_role_initial_counts=$(psql_service_db -v ON_ERROR_STOP=1 -Atq <<'SQL'
begin;
set local role service_role;
select current_user || '|' ||
       (select count(*)::text from public.accounts) || '|' ||
       (select count(*)::text from public.account_credentials);
commit;
SQL
)
[[ "$service_role_initial_counts" == 'service_role|1|0' ]] ||
  die "service_role protected-table reads returned unexpected counts: $service_role_initial_counts"

# The fixture account owns live rows (restrict foreign keys); the census proof needs an empty
# accounts table, so the scenario clears the fixture as the database owner, not as a service role.
psql_admin -d pe_legacy_545 -v ON_ERROR_STOP=1 <<'SQL'
begin;
delete from public.live_positions;
delete from public.live_fills;
delete from public.live_account_state;
delete from public.account_events;
delete from public.account_credentials;
delete from public.accounts;
commit;
SQL
zero_census=$(PGOPTIONS='-c role=service_role' account_census_observation)
read -r zero_count zero_digest zero_safe <<< "$zero_census"
[[ "$zero_count" == 0 && "$zero_digest" =~ ^[0-9a-f]{64}$ && "$zero_safe" == 1 ]] ||
  die "service_role zero-row account census is invalid: $zero_census"

psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
begin;
set local role service_role;
select public.account_create('ci-auth-chain', true, 'ci-545');
select public.account_rotate_credentials(
  'ci-auth-chain', 1, 'ci-key', 'ci-sealed-bundle', 'ci-fingerprint', 'ci-545'
);
commit;
SQL
before_census=$(PGOPTIONS='-c role=service_role' account_census_observation)
read -r before_count before_digest before_safe <<< "$before_census"
[[ "$before_count" == 1 && "$before_digest" =~ ^[0-9a-f]{64}$ && "$before_safe" == 1 ]] ||
  die "service_role non-zero account census is invalid: $before_census"
service_role_nonzero_counts=$(psql_service_db -v ON_ERROR_STOP=1 -Atq <<'SQL'
begin;
set local role service_role;
select current_user || '|' ||
       (select count(*)::text from public.accounts) || '|' ||
       (select count(*)::text from public.account_credentials);
commit;
SQL
)
[[ "$service_role_nonzero_counts" == 'service_role|1|1' ]] ||
  die "service_role non-zero protected-table reads returned unexpected counts: $service_role_nonzero_counts"

psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
begin;
set local role service_role;
select public.account_request_mode('ci-auth-chain', 'live_tiny', 'ci-545');
select public.account_set_effective_mode(
  'ci-auth-chain', 'live_tiny', 'ci-545', 'census drift proof'
);
commit;
SQL
after_census=$(PGOPTIONS='-c role=service_role' account_census_observation)
read -r after_count after_digest after_safe <<< "$after_census"
[[ "$after_count" == 1 && "$after_digest" =~ ^[0-9a-f]{64}$ && "$after_safe" == 0 ]] ||
  die "live_tiny account census did not fail closed: $after_census"
[[ "$before_digest" != "$after_digest" ]] ||
  die "the shared rehearsal census did not detect the live_tiny mode change"
pass_step

echo "CIPG PASS: real PostgreSQL Legacy17-to-Financial15 scenario completed"
