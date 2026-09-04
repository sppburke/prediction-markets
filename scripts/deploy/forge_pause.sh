#!/usr/bin/env bash
# Forge-local pause protocol for the continuous ranking loop.

set -euo pipefail

REPO_ROOT="$HOME/prediction-markets"
RUN_FLAG="$REPO_ROOT/data/eval-results/rank_and_push.loop"
PAUSE_RECORD="$REPO_ROOT/data/eval-results/.forge_pause.json"
PAUSE_LOCK="$REPO_ROOT/data/eval-results/.forge_pause.lock"
RANK_LOCKS=(
  "$REPO_ROOT/data/eval-results/.rank_and_push_loop.lock"
  "$REPO_ROOT/data/eval-results/.rank_and_push.lock"
  "$REPO_ROOT/data/wallet_cache.db.lock"
)
STOP_WAIT_SECS=$((45 + 5))

die() {
  echo "FATAL: $*" >&2
  exit 1
}

usage() {
  echo "usage: forge_pause.sh {pause|restore|status}" >&2
  exit 2
}

unit_enabled() {
  local output rc
  if output=$(systemctl --user is-enabled pe-rank-loop 2>/dev/null); then rc=0; else rc=$?; fi
  case "$output:$rc" in
    enabled:0) echo true ;;
    disabled:1) echo false ;;
    *) die "could not prove pe-rank-loop enablement (rc=$rc, output=$output)" ;;
  esac
}

unit_active() {
  local output rc
  if output=$(systemctl --user is-active pe-rank-loop 2>/dev/null); then rc=0; else rc=$?; fi
  case "$output:$rc" in
    active:0) echo true ;;
    inactive:3|failed:3) echo false ;;
    *) die "could not prove pe-rank-loop activity (rc=$rc, output=$output)" ;;
  esac
}

current_flag() {
  local value=missing
  [[ ! -f "$RUN_FLAG" ]] || value=$(<"$RUN_FLAG")
  [[ "$value" == run || "$value" == stop || "$value" == missing ]] ||
    die "invalid Forge flag: $value"
  printf '%s\n' "$value"
}

fsync_directory() {
  python3 -c 'import os,sys
directory=os.open(sys.argv[1], os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try: os.fsync(directory)
finally: os.close(directory)' "$1"
}

write_flag_atomically() {
  local value=$1 parent
  parent=$(dirname "$RUN_FLAG")
  mkdir -p "$parent"
  if [[ "$value" == missing ]]; then
    rm -f "$RUN_FLAG"
    fsync_directory "$parent"
    return
  fi
  [[ "$value" == run || "$value" == stop ]] || die "invalid Forge flag value: $value"
  python3 -c 'import os,sys,tempfile
path,value=sys.argv[1:]
parent=os.path.dirname(path) or "."
fd,tmp=tempfile.mkstemp(prefix=".rank_and_push.loop.", dir=parent)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(value+"\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    directory=os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try: os.fsync(directory)
    finally: os.close(directory)
except BaseException:
    try: os.unlink(tmp)
    except FileNotFoundError: pass
    raise' "$RUN_FLAG" "$value"
}

record_pause_if_absent() {
  local prior_flag=$1 prior_enabled=$2 prior_active=$3 recorded_at=$4
  mkdir -p "$(dirname "$PAUSE_RECORD")"
  python3 -c 'import json,os,sys,tempfile
path,prior_flag,prior_enabled,prior_active,recorded_at=sys.argv[1:]
parent=os.path.dirname(path) or "."
if os.path.exists(path):
    raise SystemExit(0)
value={
    "prior_flag":prior_flag,
    "prior_enabled":prior_enabled == "true",
    "prior_active":prior_active == "true",
    "recorded_at":recorded_at,
}
fd,tmp=tempfile.mkstemp(prefix=".forge_pause.", dir=parent)
try:
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",",":"))
        handle.write("\n")
        handle.flush()
        os.fchmod(handle.fileno(), 0o600)
        os.fsync(handle.fileno())
    try: os.link(tmp, path)
    except FileExistsError: pass
    directory=os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try: os.fsync(directory)
    finally: os.close(directory)
finally:
    try: os.unlink(tmp)
    except FileNotFoundError: pass' \
    "$PAUSE_RECORD" "$prior_flag" "$prior_enabled" "$prior_active" "$recorded_at"
}

load_pause_record() {
  local output
  output=$(python3 -c 'import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
flag=value.get("prior_flag")
enabled=value.get("prior_enabled")
active=value.get("prior_active")
recorded_at=value.get("recorded_at")
if flag not in ("run","stop","missing"): raise SystemExit("invalid prior_flag")
if type(enabled) is not bool: raise SystemExit("invalid prior_enabled")
if type(active) is not bool: raise SystemExit("invalid prior_active")
if not isinstance(recorded_at,str) or not recorded_at: raise SystemExit("invalid recorded_at")
print(flag)
print(str(enabled).lower())
print(str(active).lower())
print(recorded_at)' "$PAUSE_RECORD")
  mapfile -t PAUSE_RECORD_FIELDS <<< "$output"
  [[ "${#PAUSE_RECORD_FIELDS[@]}" == 4 ]] || die "invalid Forge pause record"
}

delete_pause_record() {
  local parent
  parent=$(dirname "$PAUSE_RECORD")
  rm -f "$PAUSE_RECORD"
  fsync_directory "$parent"
}

cycle_descendant_running() {
  pgrep -f 'rank_and_push_loop[.]sh|rank_and_push[.]sh|[p]e-bootstrap|[l]atency_shift_rerank[.]py|[r]ank_72hr_buyandhold[.]py' >/dev/null
}

rank_locks_unheld() {
  local lock
  for lock in "${RANK_LOCKS[@]}"; do
    [[ -e "$lock" ]] || continue
    flock -n "$lock" true || return 1
  done
}

pause() {
  local deadline lock prior_flag prior_enabled prior_active active
  if [[ ! -f "$PAUSE_RECORD" ]]; then
    prior_flag=$(current_flag) || exit $?
    prior_enabled=$(unit_enabled) || exit $?
    prior_active=$(unit_active) || exit $?
    record_pause_if_absent "$prior_flag" "$prior_enabled" "$prior_active" \
      "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  fi
  load_pause_record
  write_flag_atomically stop
  deadline=$((SECONDS + STOP_WAIT_SECS))
  systemctl --user stop pe-rank-loop
  active=$(unit_active) || exit $?
  while [[ "$active" == true ]]; do
    ((SECONDS < deadline)) || die "pe-rank-loop did not become inactive within $STOP_WAIT_SECS seconds"
    sleep 1
    active=$(unit_active) || exit $?
  done
  if cycle_descendant_running; then
    die "a Forge cycle descendant is still running"
  fi
  for lock in "${RANK_LOCKS[@]}"; do
    mkdir -p "$(dirname "$lock")"
    flock -n "$lock" true || die "Forge lock is still held: $lock"
  done
  echo "Forge ranking loop paused"
}

restore() {
  if [[ ! -f "$PAUSE_RECORD" ]]; then
    echo "Forge pause record is absent; nothing to restore"
    return
  fi
  load_pause_record
  local prior_flag=${PAUSE_RECORD_FIELDS[0]}
  local prior_enabled=${PAUSE_RECORD_FIELDS[1]}
  local prior_active=${PAUSE_RECORD_FIELDS[2]}
  local enabled active
  write_flag_atomically "$prior_flag"
  if [[ "$prior_enabled" == true ]]; then
    systemctl --user enable pe-rank-loop
    enabled=$(unit_enabled) || exit $?
    [[ "$enabled" == true ]] || die "failed to restore Forge enablement"
  else
    systemctl --user disable pe-rank-loop
    enabled=$(unit_enabled) || exit $?
    [[ "$enabled" == false ]] || die "failed to restore Forge disablement"
  fi
  if [[ "$prior_active" == true ]]; then
    systemctl --user start pe-rank-loop
    active=$(unit_active) || exit $?
    [[ "$active" == true ]] || die "failed to restore Forge activity"
  else
    systemctl --user stop pe-rank-loop
    active=$(unit_active) || exit $?
    [[ "$active" == false ]] || die "failed to restore Forge inactivity"
  fi
  delete_pause_record
  echo "Forge ranking loop restored"
}

status() {
  local flag enabled active descendants_running=false locks_unheld=true record_present=false
  local paused_complete=false
  flag=$(current_flag) || exit $?
  enabled=$(unit_enabled) || exit $?
  active=$(unit_active) || exit $?
  if cycle_descendant_running; then descendants_running=true; fi
  if ! rank_locks_unheld; then locks_unheld=false; fi
  if [[ -f "$PAUSE_RECORD" ]]; then
    load_pause_record
    record_present=true
  fi
  if [[ "$record_present" == true && "$flag" == stop && "$active" == false &&
        "$descendants_running" == false && "$locks_unheld" == true ]]; then
    paused_complete=true
  fi
  python3 -c 'import json,os,sys
path,flag,enabled,active,descendants_running,locks_unheld,paused_complete=sys.argv[1:]
record=None
if os.path.exists(path):
    with open(path, encoding="utf-8") as handle: record=json.load(handle)
print(json.dumps({
    "live":{
        "active":active=="true",
        "cycle_descendant_running":descendants_running=="true",
        "enabled":enabled=="true",
        "flag":flag,
        "locks_unheld":locks_unheld=="true",
    },
    "paused_complete":paused_complete=="true",
    "record":record,
},sort_keys=True,indent=2))' \
    "$PAUSE_RECORD" "$flag" "$enabled" "$active" "$descendants_running" \
    "$locks_unheld" "$paused_complete"
}

[[ $# == 1 ]] || usage
[[ -d "$REPO_ROOT" ]] || die "Forge checkout is absent: $REPO_ROOT"
cd "$REPO_ROOT"
for command in python3 systemctl flock pgrep; do
  command -v "$command" >/dev/null || die "$command not installed"
done
[[ -d "$(dirname "$PAUSE_LOCK")" ]] || die "Forge pause lock directory is absent: $(dirname "$PAUSE_LOCK")"
exec 9>>"$PAUSE_LOCK"
flock -n 9 || die "another Forge pause/restore/status invocation holds $PAUSE_LOCK"

case "$1" in
  pause) pause ;;
  restore) restore ;;
  status) status ;;
  *) usage ;;
esac
