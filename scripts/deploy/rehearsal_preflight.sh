#!/usr/bin/env bash
# Prove that the publishable-key rehearsal can read but cannot reach any service write sink.

set -euo pipefail

# shellcheck source=generation_common.sh
source "$(cd "$(dirname "$0")" && pwd)/generation_common.sh"

[[ $# -eq 1 ]] || { echo "usage: $0 SANITIZED_REHEARSAL_ENV" >&2; exit 2; }
env_file=$1
[[ -f "$env_file" ]] || { echo "FATAL: missing sanitized rehearsal environment: $env_file" >&2; exit 1; }
command -v psql >/dev/null || { echo "FATAL: psql not installed" >&2; exit 1; }
command -v curl >/dev/null || { echo "FATAL: curl not installed" >&2; exit 1; }
command -v python3 >/dev/null || { echo "FATAL: python3 not installed" >&2; exit 1; }

# A traced operator shell must not print credential-bearing assignments or HTTP headers.
[[ $- != *x* ]] || set +x
mapfile -d '' -t rehearsal_environment < <(
  env_file_values "$env_file" PE_SUPABASE_URL PE_SUPABASE_ANON_KEY PE_SUPABASE_SECRET_KEY &&
    printf '__PE_ENV_FILE_PARSED__\0'
)
last_environment_index=$((${#rehearsal_environment[@]} - 1))
if ((last_environment_index < 0)) ||
   [[ ${rehearsal_environment[$last_environment_index]} != __PE_ENV_FILE_PARSED__ ]]; then
  die "could not parse the sanitized rehearsal environment"
fi
unset 'rehearsal_environment[last_environment_index]'
for assignment in "${rehearsal_environment[@]}"; do
  export "${assignment?}"
done

python3 -c '
import base64, json, os, re, sys

required = ("SUPABASE_DB_URL", "PE_SUPABASE_URL", "PE_SUPABASE_ANON_KEY", "PE_SUPABASE_SECRET_KEY")
if any(not os.environ.get(name) for name in required):
    raise SystemExit("rehearsal database and Supabase values are required")
if os.environ["PE_SUPABASE_SECRET_KEY"] != os.environ["PE_SUPABASE_ANON_KEY"]:
    raise SystemExit("rehearsal secret slot does not contain the publishable key")
key = os.environ["PE_SUPABASE_SECRET_KEY"]
if key.startswith("sb_publishable_") and len(key) > len("sb_publishable_"):
    raise SystemExit(0)
if key.startswith("sb_secret_"):
    raise SystemExit("rehearsal key is a modern secret key, not a publishable key")
parts = key.split(".")
if len(parts) != 3 or not all(re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in parts):
    raise SystemExit("rehearsal key is neither sb_publishable_* nor a legacy anon JWT")
try:
    payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as error:
    raise SystemExit("rehearsal legacy JWT payload is malformed") from error
if not isinstance(payload, dict) or payload.get("role") != "anon":
    raise SystemExit("rehearsal legacy JWT role is not anon")
'

rehearsal_psql() {
  env -i PATH="$PATH" LANG="${LANG:-C.UTF-8}" \
    bash -c 'eval "$(cat <&3)"; exec 3<&-; exec psql -X "$@"' psql "$@" \
    3< <(pg_url_env SUPABASE_DB_URL)
}
psql_service_db() {
  rehearsal_psql "$@"
}
verify_legacy_service_contract

rehearsal_psql -v ON_ERROR_STOP=1 <<'SQL'
do $$
declare
  table_name text;
  dml text;
  live_tables constant text[] := array['live_fills', 'live_positions', 'live_account_state'];
  rls_tables constant text[] := array[
    'paper_fills', 'paper_positions', 'paper_bankroll', 'settled_markets',
    'fill_market_snapshots', 'supabase_sink_hwm', 'service_watchlist',
    'service_runtime', 'wallet_lifecycle_events'
  ];
begin
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
canary_body=

cleanup_canary_rows() {
  rehearsal_psql -v ON_ERROR_STOP=1 >/dev/null <<'SQL'
delete from public.paper_fills
 where idempotency_key = 'pe-rehearsal-canary-557';
delete from public.supabase_sink_hwm
 where id = 2 and last_event_seq = -557;
delete from public.wallet_lifecycle_events
 where reason = 'pe-rehearsal-canary-557';
delete from public.service_watchlist
 where wallet_hex = '0x0000000000000000000000000000000000000557'
   and rank = 557 and leader_score_bps = 557;
SQL
}

cleanup_on_exit() {
  local original_status=$? cleanup_status=0
  trap - EXIT
  rm -f "${canary_body:-}"
  cleanup_canary_rows || cleanup_status=$?
  if ((cleanup_status != 0)); then
    echo "FATAL: rehearsal canary cleanup failed" >&2
    exit "$cleanup_status"
  fi
  exit "$original_status"
}
trap cleanup_on_exit EXIT
cleanup_canary_rows

refused() {
  local path=$1 payload=$2 status
  canary_body=$(mktemp)
  status=$(env -i PATH="$PATH" LANG="${LANG:-C.UTF-8}" \
    curl --disable --silent --show-error --output "$canary_body" --write-out '%{http_code}' \
    -X POST "${PE_SUPABASE_URL%/}$path" "${auth[@]}" "${json[@]}" --data "$payload")
  [[ "$status" == 401 || "$status" == 403 ]] || {
    echo "FATAL: rehearsal canary $path returned HTTP $status, expected 401/403" >&2
    sed -n '1,5p' "$canary_body" >&2
    exit 1
  }
  rm -f "$canary_body"
  canary_body=
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

landed=$(rehearsal_psql -v ON_ERROR_STOP=1 -Atc \
  "select (select count(*) from paper_fills where idempotency_key = '$marker') +
          (select count(*) from supabase_sink_hwm where id = 2) +
          (select count(*) from wallet_lifecycle_events where reason = '$marker') +
          (select count(*) from service_watchlist where wallet_hex =
            '0x0000000000000000000000000000000000000557');")
[[ "$landed" == 0 ]] || { echo "FATAL: $landed rehearsal canary row(s) landed" >&2; exit 1; }

account_census=$(account_census_receipt) || die "privileged rehearsal account census failed"
read -r account_census_count account_census_sha256 <<< "$account_census"
[[ "$account_census_count" =~ ^[0-9]+$ && "$account_census_sha256" =~ ^[0-9a-f]{64}$ ]] ||
  die "privileged rehearsal account census receipt is malformed"

echo "rehearsal privilege matrix: PASS"
echo "rehearsal publishable-key class: PASS"
echo "rehearsal non-mutating canaries: PASS (all 401/403; landed=0)"
echo "REHEARSAL_ACCOUNT_CENSUS_V1 count=$account_census_count sha256=$account_census_sha256"
