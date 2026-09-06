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
  psql_url "$PG_ADMIN_URL" "$@"
}

start_step "create isolated roles and legacy database"
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
end $$;
SQL
psql_admin -v ON_ERROR_STOP=1 -c 'create database pe_legacy_545;'
pass_step

start_step "install pre-Start schema and seed Legacy17 plus the release row"
legacy_schema="$RUNNER_TEMP/supabase_paper_state_schema.pre-545.sql"
legacy_multi_account_schema="$RUNNER_TEMP/supabase_multi_account_live_schema.pre-545.sql"
git show origin/main:scripts/supabase_paper_state_schema.sql > "$legacy_schema"
git show origin/main:scripts/supabase_multi_account_live_schema.sql > "$legacy_multi_account_schema"
[[ -s "$legacy_schema" ]] || die "pre-Start paper-state schema is empty"
[[ -s "$legacy_multi_account_schema" ]] || die "pre-Start multi-account schema is empty"
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
SQL
pass_step

start_step "run the driver-owned Legacy17 verifier"
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

start_step "archive and reset the pre-Start legacy state"
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
pass_step

start_step "install financial schema, seed the Start identity, and read it back"
psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_paper_state_schema.sql"
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
do $$
declare
  start_result jsonb;
begin
  start_result := seed_financial_start(42, repeat('b', 64));
  if start_result->>'outcome' not in ('applied', 'existing')
     or (start_result->>'start_seq')::bigint <> 42
     or start_result->>'start_hash' <> repeat('b', 64) then
    raise exception 'financial Start seed returned an unexpected result: %', start_result;
  end if;
  if not exists (
       select 1 from paper_bankroll
        where id = 0
          and bankroll_str::numeric = 10000
          and start_seq = 42
          and start_hash = repeat('b', 64)
          and last_prepared_seq is null
     ) then
    raise exception 'financial Start read-back differs from the seeded identity';
  end if;
end $$;
SQL
pass_step

start_step "migrate Legacy17 to Financial15 and preserve the release row"
psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/migrate_service_config_545.sql"
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
psql_service_db -v ON_ERROR_STOP=1 -v activation_id=ci-545-fractional \
  -v bankroll=10000 -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
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
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
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
          and start_seq = 42 and start_hash = repeat('b', 64)
          and last_prepared_seq = 44
     ) or not exists (
       select 1 from settled_markets
        where market_id = 'financial-settled-market'
          and credit_applied = 0.625 and prepared_seq = 44
     ) then
    raise exception 'fractional archive/restore round trip changed the financial book';
  end if;
end $$;
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

echo "CIPG PASS: real PostgreSQL Legacy17-to-Financial15 scenario completed"
