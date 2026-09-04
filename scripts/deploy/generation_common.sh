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
  if [[ "${PE_ACTIVATION_TESTING:-0}" == 1 && -n "${PE_ACTIVATION_TEST_HOLD_LOCK_FILE:-}" ]]; then
    : > "$PE_ACTIVATION_TEST_HOLD_LOCK_FILE.ready"
    while [[ -e "$PE_ACTIVATION_TEST_HOLD_LOCK_FILE" ]]; do sleep 0.05; done
  fi
}

sha256_file() {
  sha256sum "$1" | awk '{print $1}'
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

activation_archive_counts() {
  # The archive tables gain `activation_id` only when archive_paper_state.sql first runs (`add column if
  # not exists`); the deployed database had no such column before the first activation (EVIDENCE F7
  # addendum). A missing column means no stamped rows, by definition — never a query error.
  local activation_id=$1 present
  present=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
    "select count(*) from information_schema.columns where table_schema='public' and column_name='activation_id'
       and table_name in ('paper_fills_archive','settled_markets_archive','paper_positions_archive','paper_bankroll_archive','fill_market_snapshots_archive');") ||
    return 1
  case "$present" in
    0) echo '0 0 0 0 0'; return 0 ;;
    5) ;;
    *) die "activation_id is present on $present of the five archive tables" ;;
  esac
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
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

systemctl_enabled() {
  if systemctl is-enabled "$1" >/dev/null 2>&1; then echo true; else echo false; fi
}

systemctl_active() {
  if systemctl is-active "$1" >/dev/null 2>&1; then echo true; else echo false; fi
}
