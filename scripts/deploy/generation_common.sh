#!/usr/bin/env bash
# Shared crash-safe primitives for the generation activation and rollback drivers.

set -euo pipefail

DEPLOY_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$DEPLOY_SCRIPT_DIR/../.." && pwd)

if [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]]; then
  : "${PE_ACTIVATION_TEST_ROOT:?PE_ACTIVATION_TEST_ROOT is required in test mode}"
  DEPLOY_HOME=$(realpath -m "$PE_ACTIVATION_TEST_ROOT")
  SERVICE_ROOT="$DEPLOY_HOME/prediction-markets"
  SERVICE_MUTATE=(systemctl)
else
  DEPLOY_HOME=/home/sean
  SERVICE_ROOT=/home/sean/prediction-markets
  SERVICE_MUTATE=(sudo -n systemctl)
fi

MANIFEST="$DEPLOY_HOME/pe-activation.json"
DEPLOY_LOCK="$DEPLOY_HOME/.pe-deploy.lock"
SERVICE_BINARY="$SERVICE_ROOT/target/release/pe-service"
SERVICE_CONFIG="$SERVICE_ROOT/smoke-test/service.toml"
SERVICE_ENV="$SERVICE_ROOT/.env"

SIMULATE_CRASH_AFTER=${SIMULATE_CRASH_AFTER:-}

die() {
  echo "FATAL: $*" >&2
  exit 1
}

if [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]]; then
  PROC_ROOT=${PROC_ROOT:-/proc}
else
  [[ ! -v PROC_ROOT ]] || die "PROC_ROOT is permitted only when PE_ACTIVATION_TESTING=1"
  PROC_ROOT=/proc
fi
readonly PROC_ROOT

maybe_crash() {
  local boundary=$1
  if [[ -n "$SIMULATE_CRASH_AFTER" && "$SIMULATE_CRASH_AFTER" == "$boundary" ]]; then
    echo "SIMULATED CRASH after $boundary" >&2
    exit 86
  fi
}

acquire_deploy_lock() {
  [[ -f "$DEPLOY_LOCK" ]] || die "provisioned deploy lock is absent: $DEPLOY_LOCK"
  exec 9<"$DEPLOY_LOCK"
  flock -n 9 || die "another generation driver holds $DEPLOY_LOCK"
}

hold_deploy_lock_at_test_boundary() {
  if [[ "${PE_ACTIVATION_TESTING:-0}" == 1 && -n "${PE_ACTIVATION_TEST_HOLD_LOCK_FILE:-}" ]]; then
    : > "$PE_ACTIVATION_TEST_HOLD_LOCK_FILE.ready"
    while [[ -e "$PE_ACTIVATION_TEST_HOLD_LOCK_FILE" ]]; do sleep 0.05; done
  fi
}

sha256_file() {
  sha256sum "$1" | awk '{print $1}'
}

# Keep the credential-bearing libpq URL out of argv and /proc/<pid>/cmdline. PGDATABASE accepts
# either a database name or a PostgreSQL connection URI, so the existing SUPABASE_DB_URL contract
# remains unchanged while every caller supplies only non-secret psql options on the command line.
psql_service_db() {
  : "${SUPABASE_DB_URL:?SUPABASE_DB_URL is required}"
  PGDATABASE="$SUPABASE_DB_URL" psql "$@"
}

manifest_get() {
  python3 -c 'import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
for part in sys.argv[2].split("."):
    value=value[part]
if isinstance(value, bool): print(str(value).lower())
elif value is None: print("")
elif isinstance(value, (dict,list)): print(json.dumps(value, sort_keys=True, separators=(",",":")))
else: print(value)' "$MANIFEST" "$1"
}

manifest_requires_archive_columns() {
  python3 -c 'import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
durable=("archive_counts" in value or
         value.get("state") in {"reset","switched","started","verified"})
raise SystemExit(0 if durable else 1)' "$MANIFEST"
}

atomic_manifest_json() {
  local json=$1 boundary=$2
  maybe_crash "before-manifest-$boundary"
  python3 -c 'import json,os,sys
path,payload=sys.argv[1:]
value=json.loads(payload)
parent=os.path.dirname(path) or "."
tmp=os.path.join(parent, ".pe-activation.tmp.%d" % os.getpid())
with open(tmp, "w", encoding="utf-8") as handle:
    json.dump(value, handle, sort_keys=True, separators=(",",":"))
    handle.write("\n")
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(tmp, 0o600)
os.replace(tmp, path)
directory=os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try: os.fsync(directory)
finally: os.close(directory)' "$MANIFEST" "$json"
  maybe_crash "$boundary"
  maybe_crash "after-manifest-$boundary"
}

manifest_advance() {
  local state=$1 patch=${2:-'{}'}
  local json
  json=$(python3 -c 'import json,sys
path,state,patch=sys.argv[1:]
value=json.load(open(path, encoding="utf-8"))
delta=json.loads(patch)
value.update(delta)
value["state"]=state
print(json.dumps(value, sort_keys=True, separators=(",",":")))' \
    "$MANIFEST" "$state" "$patch")
  atomic_manifest_json "$json" "$state"
}

manifest_patch_boundary() {
  local boundary=$1 patch=$2 json
  json=$(python3 -c 'import json,sys
path,patch=sys.argv[1:]
value=json.load(open(path, encoding="utf-8"))
value.update(json.loads(patch))
print(json.dumps(value, sort_keys=True, separators=(",",":")))' "$MANIFEST" "$patch")
  atomic_manifest_json "$json" "$boundary"
}

atomic_adopt() {
  local source=$1 destination=$2 mode=$3 boundary=$4 expected actual
  expected=$(sha256_file "$source")
  if [[ -f "$destination" ]]; then
    actual=$(sha256_file "$destination")
    if [[ "$actual" == "$expected" && "$(stat -c '%a' "$destination")" == "${mode#0}" ]]; then
      return 0
    fi
  fi
  mkdir -p "$(dirname "$destination")"
  python3 -c 'import os,shutil,sys
source,destination,mode=sys.argv[1:]
parent=os.path.dirname(destination) or "."
tmp=os.path.join(parent, ".pe-adopt.tmp.%d" % os.getpid())
shutil.copyfile(source, tmp)
os.chmod(tmp, int(mode, 8))
with open(tmp, "rb") as handle: os.fsync(handle.fileno())
os.replace(tmp, destination)
directory=os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try: os.fsync(directory)
finally: os.close(directory)' "$source" "$destination" "$mode"
  actual=$(sha256_file "$destination")
  [[ "$actual" == "$expected" ]] || die "adopted hash mismatch for $destination"
  maybe_crash "$boundary"
}

process_runs_service() {
  # Ownership of what runs NOW, from the process itself (lossless, no unit-file interpretation):
  #   argv  == exactly `<binary> <config>` (NUL-split; empty elements kept)
  #   environ: every variable the installed environment file defines (evaluated with the unit's own
  #   `set -a; source; exec env -0` semantics) is present with an equal value, and every extra name is
  #   one of the exact systemd-injected names observed for the production unit or one of the unit's
  #   `bash -c` wrapper's own variables. PWD, SHLVL, OLDPWD, and _ are stripped from the expected set
  #   because they do not affect the binary; cwd is proved independently from /proc/<pid>/cwd.
  local expected_binary=$1 expected_config=$2 expected_env=$3 working=$4 pid=$5
  python3 -c 'import os,subprocess,sys
expected_binary,expected_config,expected_env,working,pid,proc_root=sys.argv[1:]
def norm(path):
    return os.path.normpath(path if os.path.isabs(path) else os.path.join(working, path))
with open("%s/%s/cmdline" % (proc_root, pid), "rb") as handle:
    raw=handle.read()
if raw.endswith(b"\0"):
    raw=raw[:-1]
argv=[part.decode() for part in raw.split(b"\0")]
if not (len(argv) == 2 and argv[0] == expected_binary and norm(argv[1]) == os.path.normpath(expected_config)):
    raise SystemExit(1)
def parse_environment(raw):
    entries={}
    for part in raw.rstrip(b"\0").split(b"\0"):
        if not part:
            continue
        if b"=" not in part:
            raise SystemExit(1)
        name,value=part.split(b"=",1)
        if name in entries:
            raise SystemExit(1)
        entries[name]=value
    return entries
with open("%s/%s/environ" % (proc_root, pid), "rb") as handle:
    running=parse_environment(handle.read())
evaluated=subprocess.run(
    ["env","-i","/bin/bash","-c","""set -a
source "$1"
set +a
if [[ ${LD_PRELOAD+x} == x || ${LD_LIBRARY_PATH+x} == x ]]; then
    exit 97
fi
exec env -0""","bash",expected_env],
    check=False,cwd=working,stdout=subprocess.PIPE,
)
if evaluated.returncode == 97:
    raise SystemExit(1)
if evaluated.returncode != 0:
    raise SystemExit(1)
shell_own={b"_",b"PWD",b"SHLVL",b"OLDPWD"}
evaluated_environment=parse_environment(evaluated.stdout)
loader_overrides={b"LD_PRELOAD",b"LD_LIBRARY_PATH"}
if loader_overrides & (running.keys() | evaluated_environment.keys()):
    raise SystemExit(1)
expected={name:value for name,value in evaluated_environment.items() if name not in shell_own}
missing=[name for name,value in expected.items() if running.get(name) != value]
injected={
    b"CREDENTIALS_DIRECTORY",b"HOME",b"INVOCATION_ID",b"JOURNAL_STREAM",b"LANG",b"LOGNAME",
    b"MEMORY_PRESSURE_WATCH",b"MEMORY_PRESSURE_WRITE",b"PATH",b"SHELL",b"SYSTEMD_EXEC_PID",b"USER",
    b"PWD",b"SHLVL",b"OLDPWD",b"_",
}
unknown=[name for name in running if name not in expected and name not in injected]
credential_ok=running.get(b"CREDENTIALS_DIRECTORY") == b"/run/credentials/pe-service.service"
raise SystemExit(0 if not missing and not unknown and credential_ok else 1)' \
    "$expected_binary" "$expected_config" "$expected_env" "$working" "$pid" "$PROC_ROOT"
}

read_service_process_snapshot() {
  local output key value
  local active= pid= invocation= active_enter=
  output=$(systemctl show -p ActiveState -p MainPID -p InvocationID -p ActiveEnterTimestamp pe-service) ||
    return 1
  while IFS='=' read -r key value; do
    case "$key" in
      ActiveState) active=$value ;;
      MainPID) pid=$value ;;
      InvocationID) invocation=$value ;;
      ActiveEnterTimestamp) active_enter=$value ;;
    esac
  done <<< "$output"
  [[ -n "$active" && -n "$pid" && -n "$invocation" && -n "$active_enter" ]] || return 1
  SERVICE_SNAPSHOT_ACTIVE=$active
  SERVICE_SNAPSHOT_PID=$pid
  SERVICE_SNAPSHOT_INVOCATION=$invocation
  SERVICE_SNAPSHOT_ACTIVE_ENTER=$active_enter
}

verify_installed_unit_owner() {
  # Ownership is proved from one stable manager snapshot plus the running process, never from unit-file
  # syntax. The same checks run on the NEW process in `verified`, so what the unit starts after the switch
  # is proved, not parsed.
  local active enabled pid invocation active_enter working running
  [[ -f "$SERVICE_CONFIG" && -f "$SERVICE_ENV" && -f "$SERVICE_BINARY" ]] ||
    die "one or more installed service artifacts are absent"
  active=$(systemctl_active_state pe-service)
  enabled=$(systemctl_enabled_state pe-service)
  [[ "$active" == true ]] || die "installed pe-service is not active"
  [[ "$enabled" == true ]] || die "installed pe-service is not enabled"
  read_service_process_snapshot || die "could not read the pe-service process snapshot"
  active=$SERVICE_SNAPSHOT_ACTIVE
  pid=$SERVICE_SNAPSHOT_PID
  invocation=$SERVICE_SNAPSHOT_INVOCATION
  active_enter=$SERVICE_SNAPSHOT_ACTIVE_ENTER
  [[ "$active" == active ]] || die "installed pe-service is not active"
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || die "pe-service has no MainPID"
  [[ "$(systemctl show pe-service -p NeedDaemonReload --value)" == no ]] ||
    die "pe-service unit files differ from the loaded configuration (daemon-reload pending)"
  working=$(readlink "$PROC_ROOT/$pid/cwd") || die "could not read pe-service process cwd"
  [[ "$(realpath -m "$working")" == "$SERVICE_ROOT" ]] ||
    die "pe-service process cwd is $working, expected $SERVICE_ROOT"
  running=$(sha256_file "$PROC_ROOT/$pid/exe")
  [[ "$running" == "$(sha256_file "$SERVICE_BINARY")" ]] ||
    die "running pe-service is not the installed binary"
  process_runs_service "$SERVICE_BINARY" "$SERVICE_CONFIG" "$SERVICE_ENV" "$working" "$pid" ||
    die "running pe-service does not run the installed binary, service config and environment"
  read_service_process_snapshot || die "could not re-read the pe-service process snapshot"
  [[ "$SERVICE_SNAPSHOT_ACTIVE" == "$active" && "$SERVICE_SNAPSHOT_PID" == "$pid" &&
     "$SERVICE_SNAPSHOT_INVOCATION" == "$invocation" &&
     "$SERVICE_SNAPSHOT_ACTIVE_ENTER" == "$active_enter" ]] ||
    die "pe-service process snapshot changed during ownership proof"
}

# Read-only proof of the complete pre-financial-era database contract (#545). Both the publishable-
# key rehearsal and the financial-era driver call this owner so callable signatures, grants, and the
# Legacy17 hot-config inventory cannot drift between the rehearsal and the Start boundary.
verify_legacy_service_contract() {
  psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
set search_path = public, pg_catalog;
do $$
declare
  function_name text;
  expected_functions constant text[] := array[
    'commit_fill(text,text,text,text,integer,text,bigint,text,bigint,bigint)',
    'commit_fill_v2(text,text,text,text,integer,text,bigint,text,bigint,bigint)',
    'apply_resolution(text,jsonb,text,bigint)',
    'apply_resolution_v2(text,jsonb,bigint)',
    'service_watchlist_replace_v1(timestamp with time zone,jsonb)',
    'account_set_effective_mode(text,text,text,text)'
  ];
  legacy_keys constant text[] := array[
    'active_watchlist_size','mode','max_fill_price','min_fill_price',
    'min_resolution_horizon_secs','max_resolution_horizon_secs','fill_mode',
    'price_impact_cap_bps','flip_human_approved',
    'kelly_fraction_above_default_human_approved','polymarket_fee_rate',
    'kelly_fraction_override','per_trade_cap','slippage_rate','sizing_mode',
    'sizing_dollar_usd','sizing_contracts'
  ];
begin
  if (select rolbypassrls from pg_roles where rolname = 'anon') is distinct from false then
    raise exception 'anon must exist and must not bypass RLS';
  end if;
  if (select rolbypassrls from pg_roles where rolname = 'service_role') is distinct from true then
    raise exception 'service_role must exist and bypass RLS';
  end if;

  if exists (
    select signature from unnest(expected_functions) signature
    except
    select p.oid::regprocedure::text
      from pg_proc p
     where p.pronamespace = 'public'::regnamespace
       and p.proname in (
       'commit_fill','commit_fill_v2','apply_resolution','apply_resolution_v2',
       'service_watchlist_replace_v1','account_set_effective_mode'
     )
  ) or exists (
    select p.oid::regprocedure::text
      from pg_proc p
     where p.pronamespace = 'public'::regnamespace
       and p.proname in (
       'commit_fill','commit_fill_v2','apply_resolution','apply_resolution_v2',
       'service_watchlist_replace_v1','account_set_effective_mode'
     )
    except
    select signature from unnest(expected_functions) signature
  ) then
    raise exception 'pre-Start callable-function inventory differs from Legacy17';
  end if;

  foreach function_name in array expected_functions loop
    if has_function_privilege('anon', function_name, 'EXECUTE') then
      raise exception 'anon unexpectedly has EXECUTE on %', function_name;
    end if;
    if not has_function_privilege('service_role', function_name, 'EXECUTE') then
      raise exception 'service_role lacks EXECUTE on %', function_name;
    end if;
  end loop;

  if exists (select key from service_config except select key from unnest(legacy_keys) key)
     or exists (
       select key from unnest(legacy_keys) key
        where key <> 'kelly_fraction_override'
       except select key from service_config
     )
     or (select count(*) from service_config where key = 'kelly_fraction_override') > 1 then
    raise exception 'service_config does not match the Legacy17 contract';
  end if;
end $$;
SQL
}

activation_archive_counts() {
  # The archive tables gain `activation_id` only when archive_paper_state.sql first runs (`add column if
  # not exists`); the deployed database had no such column before the first activation (EVIDENCE F7
  # addendum). A missing column means no stamped rows, by definition — never a query error.
  local activation_id=$1 durable_reset=$2 present
  present=$(psql_service_db -v ON_ERROR_STOP=1 -Atc \
    "select count(*) from information_schema.columns where table_schema='public' and column_name='activation_id'
       and table_name in ('paper_fills_archive','settled_markets_archive','paper_positions_archive','paper_bankroll_archive','fill_market_snapshots_archive');") ||
    return 1
  case "$present" in
    0)
      [[ "$durable_reset" == false ]] ||
        die "archive stamp column missing after a durable reset"
      echo '0 0 0 0 0'
      return 0
      ;;
    5) ;;
    *)
      if [[ "$durable_reset" == true ]]; then
        die "archive stamp column missing after a durable reset"
      fi
      die "activation_id is present on $present of the five archive tables"
      ;;
  esac
  psql_service_db -v ON_ERROR_STOP=1 -Atc \
    "select (select count(*) from paper_fills_archive where activation_id='$activation_id') || ' ' ||
            (select count(*) from settled_markets_archive where activation_id='$activation_id') || ' ' ||
            (select count(*) from paper_positions_archive where activation_id='$activation_id') || ' ' ||
            (select count(*) from paper_bankroll_archive where activation_id='$activation_id') || ' ' ||
            (select count(*) from fill_market_snapshots_archive where activation_id='$activation_id');"
}

activation_archive_stamp_exists() {
  local counts=$1 fills settled positions bankroll snapshots
  read -r fills settled positions bankroll snapshots <<< "$counts"
  [[ "$fills" =~ ^[0-9]+$ && "$settled" =~ ^[0-9]+$ && "$positions" =~ ^[0-9]+$ &&
     "$bankroll" =~ ^[0-9]+$ && "$snapshots" =~ ^[0-9]+$ ]] ||
    die "invalid activation archive counts: $counts"
  ((fills + settled + positions + bankroll + snapshots > 0))
}

activation_archive_counts_match_pre_reset() {
  local counts=$1 recorded
  recorded=$(manifest_get pre_reset_live_counts)
  python3 -c 'import json,sys
counts=[int(value) for value in sys.argv[1].split()]
if len(counts) != 5: raise SystemExit(1)
recorded=json.loads(sys.argv[2])
keys=["paper_fills","settled_markets","paper_positions","paper_bankroll","fill_market_snapshots"]
raise SystemExit(0 if counts == [int(recorded[key]) for key in keys] else 1)' \
    "$counts" "$recorded"
}

# The empty form of every EDGE-framed log the service opens (paper.log, live_journal.log,
# source_events.log) is exactly the 5-byte header MAGIC "EDGE" + version 0x01.
log_is_header_only() {
  [[ -f "$1" && "$(stat -c %s "$1")" == 5 && "$(head -c 5 "$1" | od -An -tx1 | tr -d ' \n')" == 4544474501 ]]
}

systemctl_enabled_state() {
  local output status=0
  output=$(systemctl is-enabled "$1" 2>/dev/null) || status=$?
  case "$output" in
    enabled) [[ "$status" == 0 ]] || die "systemctl is-enabled failed for $1"; echo true ;;
    disabled) [[ "$status" == 1 ]] || die "systemctl is-enabled failed for $1"; echo false ;;
    *) die "could not prove $1 enablement from systemctl is-enabled: ${output:-<no output>}" ;;
  esac
}

systemctl_active_state() {
  local output status=0
  output=$(systemctl is-active "$1" 2>/dev/null) || status=$?
  case "$output" in
    active) [[ "$status" == 0 ]] || die "systemctl is-active failed for $1"; echo true ;;
    inactive|failed) [[ "$status" == 3 ]] || die "systemctl is-active failed for $1"; echo false ;;
    *) die "could not prove $1 activity from systemctl is-active: ${output:-<no output>}" ;;
  esac
}
