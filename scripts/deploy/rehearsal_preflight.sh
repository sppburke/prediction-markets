#!/usr/bin/env bash
# Prove that the publishable-key rehearsal can read but cannot reach any service write sink.

set -euo pipefail

[[ $# -eq 1 ]] || { echo "usage: $0 REHEARSAL_ENV" >&2; exit 2; }
env_file=$1
[[ -f "$env_file" ]] || { echo "FATAL: missing rehearsal environment: $env_file" >&2; exit 1; }
command -v psql >/dev/null || { echo "FATAL: psql not installed" >&2; exit 1; }
command -v curl >/dev/null || { echo "FATAL: curl not installed" >&2; exit 1; }

set -a
# shellcheck disable=SC1090
source "$env_file"
set +a
: "${SUPABASE_DB_URL:?SUPABASE_DB_URL is required for the privilege matrix}"
: "${PE_SUPABASE_URL:?PE_SUPABASE_URL is required}"
: "${PE_SUPABASE_ANON_KEY:?PE_SUPABASE_ANON_KEY is required}"
: "${PE_SUPABASE_SECRET_KEY:?publishable key must occupy PE_SUPABASE_SECRET_KEY}"
[[ "$PE_SUPABASE_SECRET_KEY" == "$PE_SUPABASE_ANON_KEY" ]] || {
  echo "FATAL: rehearsal secret slot does not contain the publishable key" >&2
  exit 1
}

psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 <<'SQL'
do $$
declare
  role_name text;
  table_name text;
  function_name text;
  dml text;
  live_tables constant text[] := array['live_fills', 'live_positions', 'live_account_state'];
  rls_tables constant text[] := array[
    'paper_fills', 'paper_positions', 'paper_bankroll', 'settled_markets',
    'fill_market_snapshots', 'supabase_sink_hwm', 'service_watchlist',
    'service_runtime', 'wallet_lifecycle_events'
  ];
  functions constant text[] := array[
    'commit_fill(text,text,text,text,integer,text,bigint,text,bigint,bigint)',
    'commit_fill_v2(text,text,text,text,integer,text,bigint,text,bigint,bigint)',
    'apply_resolution(text,jsonb,text,bigint)',
    'apply_resolution_v2(text,jsonb,bigint)',
    'service_watchlist_replace_v1(timestamp with time zone,jsonb)',
    'account_set_effective_mode(text,text,text,text)'
  ];
begin
  if (select rolbypassrls from pg_roles where rolname = 'anon') is distinct from false then
    raise exception 'anon must exist and must not bypass RLS';
  end if;

  foreach function_name in array functions loop
    if to_regprocedure(function_name) is null then
      raise exception 'required write function is absent: %', function_name;
    end if;
    if has_function_privilege('anon', function_name, 'EXECUTE') then
      raise exception 'anon unexpectedly has EXECUTE on %', function_name;
    end if;
    if not has_function_privilege('service_role', function_name, 'EXECUTE') then
      raise exception 'service_role lacks EXECUTE on %', function_name;
    end if;
  end loop;

  foreach table_name in array live_tables loop
    foreach dml in array array['INSERT', 'UPDATE', 'DELETE'] loop
      if has_table_privilege('anon', 'public.' || table_name, dml) then
        raise exception 'anon unexpectedly has % on %', dml, table_name;
      end if;
    end loop;
  end loop;

  foreach table_name in array rls_tables loop
    foreach dml in array array['INSERT', 'UPDATE', 'DELETE'] loop
      if not has_table_privilege('anon', 'public.' || table_name, dml) then
        raise exception 'expected platform-default anon % grant is absent on %', dml, table_name;
      end if;
    end loop;
    if not (
      select relrowsecurity
        from pg_class
       where oid = ('public.' || table_name)::regclass
    ) then
      raise exception 'RLS is disabled on %', table_name;
    end if;
    if exists (
      select 1
        from pg_class c join pg_roles r on r.oid = c.relowner
       where c.oid = ('public.' || table_name)::regclass and r.rolname = 'anon'
    ) then
      raise exception 'anon owns RLS sink %', table_name;
    end if;
    if exists (
      select 1 from pg_policies
       where schemaname = 'public' and tablename = table_name
         and cmd <> 'SELECT'
         and (roles && array['anon', 'authenticated', 'public']::name[])
    ) then
      raise exception 'write policy exposes RLS sink %', table_name;
    end if;
  end loop;
end $$;
SQL

marker="pe-rehearsal-canary-557"
auth=(-H "apikey: $PE_SUPABASE_SECRET_KEY" -H "Authorization: Bearer $PE_SUPABASE_SECRET_KEY")
json=(-H 'Content-Type: application/json' -H 'Prefer: return=representation')

refused() {
  local path=$1 payload=$2 body status
  body=$(mktemp)
  trap 'rm -f "$body"' RETURN
  status=$(curl --silent --show-error --output "$body" --write-out '%{http_code}' \
    -X POST "${PE_SUPABASE_URL%/}$path" "${auth[@]}" "${json[@]}" --data "$payload")
  [[ "$status" == 401 || "$status" == 403 ]] || {
    echo "FATAL: rehearsal canary $path returned HTTP $status, expected 401/403" >&2
    sed -n '1,5p' "$body" >&2
    exit 1
  }
  rm -f "$body"
  trap - RETURN
}

refused '/rest/v1/paper_fills' \
  "[{\"idempotency_key\":\"$marker\",\"leader_wallet\":\"$marker\",\"market_id\":\"$marker\",\"outcome_id\":0,\"side\":\"buy\",\"contracts\":1,\"fill_price\":\"0.5\",\"event_seq\":-557}]"
refused '/rest/v1/supabase_sink_hwm' '[{"id":2,"last_event_seq":-557}]'
refused '/rest/v1/service_watchlist' \
  '[{"wallet_hex":"0x0000000000000000000000000000000000000557","rank":557,"leader_score_bps":557}]'
refused '/rest/v1/wallet_lifecycle_events' \
  '[{"wallet_hex":"0x0000000000000000000000000000000000000557","event":"demote","reason":"pe-rehearsal-canary-557"}]'
refused '/rest/v1/rpc/service_watchlist_replace_v1' \
  '{"expected_token":"2000-01-01T00:00:00Z","entries":[]}'

landed=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
  "select (select count(*) from paper_fills where idempotency_key = '$marker') +
          (select count(*) from supabase_sink_hwm where id = 2) +
          (select count(*) from wallet_lifecycle_events where reason = '$marker') +
          (select count(*) from service_watchlist where wallet_hex =
            '0x0000000000000000000000000000000000000557');")
[[ "$landed" == 0 ]] || { echo "FATAL: $landed rehearsal canary row(s) landed" >&2; exit 1; }

echo "rehearsal privilege matrix: PASS"
echo "rehearsal non-mutating canaries: PASS (all 401/403; landed=0)"
