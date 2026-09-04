#!/usr/bin/env bash
# Locked, resumable warm-prepare activation for one permanent paper-state generation.

set -euo pipefail

# shellcheck source=generation_common.sh
source "$(cd "$(dirname "$0")" && pwd)/generation_common.sh"
umask 077

usage() {
  cat >&2 <<'EOF'
usage: activate_generation.sh [--dry-run] [--simulate-crash-after BOUNDARY]
  --activation-id ID --generation-dir DIR --source-v1-main PATH
  --legacy-history PATH --staged-binary PATH --config-template PATH
  --rehearsal-env PATH --service-env PATH --merge-commit SHA
  --bind ADDR --rehearsal-bind ADDR --bankroll DECIMAL
  [--approve-due-subset COUNT] [--site-confirmed]
EOF
  exit 2
}

dry_run=false
activation_id=
generation_dir=
source_v1_main=
legacy_history=
input_binary=
config_template=
rehearsal_env_template=
service_env_template=
merge_commit=
production_bind=
rehearsal_bind=
bankroll=
approved_due_subset=
site_confirmed=false
while (($#)); do
  case "$1" in
    --dry-run) dry_run=true; shift ;;
    --simulate-crash-after) [[ $# -ge 2 ]] || usage; SIMULATE_CRASH_AFTER=$2; shift 2 ;;
    --activation-id) [[ $# -ge 2 ]] || usage; activation_id=$2; shift 2 ;;
    --generation-dir) [[ $# -ge 2 ]] || usage; generation_dir=$2; shift 2 ;;
    --source-v1-main) [[ $# -ge 2 ]] || usage; source_v1_main=$2; shift 2 ;;
    --legacy-history) [[ $# -ge 2 ]] || usage; legacy_history=$2; shift 2 ;;
    --staged-binary) [[ $# -ge 2 ]] || usage; input_binary=$2; shift 2 ;;
    --config-template) [[ $# -ge 2 ]] || usage; config_template=$2; shift 2 ;;
    --rehearsal-env) [[ $# -ge 2 ]] || usage; rehearsal_env_template=$2; shift 2 ;;
    --service-env) [[ $# -ge 2 ]] || usage; service_env_template=$2; shift 2 ;;
    --merge-commit) [[ $# -ge 2 ]] || usage; merge_commit=$2; shift 2 ;;
    --bind) [[ $# -ge 2 ]] || usage; production_bind=$2; shift 2 ;;
    --rehearsal-bind) [[ $# -ge 2 ]] || usage; rehearsal_bind=$2; shift 2 ;;
    --bankroll) [[ $# -ge 2 ]] || usage; bankroll=$2; shift 2 ;;
    --approve-due-subset) [[ $# -ge 2 ]] || usage; approved_due_subset=$2; shift 2 ;;
    --site-confirmed) site_confirmed=true; shift ;;
    *) usage ;;
  esac
done

for required in activation_id generation_dir source_v1_main legacy_history input_binary \
  config_template rehearsal_env_template service_env_template merge_commit production_bind \
  rehearsal_bind bankroll; do
  [[ -n "${!required}" ]] || usage
done
[[ "$activation_id" =~ ^[A-Za-z0-9._-]+$ ]] || die "activation id has invalid characters"
[[ "$merge_commit" =~ ^[0-9a-f]{40}$ ]] || die "merge commit must be 40 lowercase hex digits"
[[ "$bankroll" =~ ^[0-9]+([.][0-9]+)?$ ]] || die "bankroll must be a non-negative decimal string"
[[ -z "$approved_due_subset" || "$approved_due_subset" =~ ^[0-9]+$ ]] || die "approved due subset must be an integer"

generation_dir=$(realpath -m "$generation_dir")
source_v1_main=$(realpath "$source_v1_main")
legacy_history=$(realpath "$legacy_history")
input_binary=$(realpath "$input_binary")
config_template=$(realpath "$config_template")
rehearsal_env_template=$(realpath "$rehearsal_env_template")
service_env_template=$(realpath "$service_env_template")

[[ "$generation_dir" == "$SERVICE_ROOT"/gen/* ]] || die "generation must be beneath $SERVICE_ROOT/gen"

if [[ "$dry_run" == true ]]; then
  cat <<EOF
DRY-RUN: no files, services, locks, or databases will be changed
activation_id=$activation_id
manifest=$MANIFEST
generation_dir=$generation_dir
state_plan=seed -> prepared -> prechecked -> guarded -> archived -> reset -> switched -> started -> verified
rollback_plan=rolling_back -> rolled_back
service_artifacts=$SERVICE_CONFIG $SERVICE_ENV $SERVICE_BINARY
EOF
  exit 0
fi

for command in python3 realpath readlink sha256sum sqlite3 psql flock systemctl ss; do
  command -v "$command" >/dev/null || die "$command not installed"
done
: "${SUPABASE_DB_URL:?SUPABASE_DB_URL must be exported for activation}"

acquire_deploy_lock

state_rank() {
  case "$1" in
    seed) echo 1 ;; prepared) echo 2 ;; prechecked) echo 3 ;; guarded) echo 4 ;;
    archived) echo 5 ;; reset) echo 6 ;; switched) echo 7 ;; started) echo 8 ;;
    verified) echo 9 ;; rolling_back) echo 10 ;; rolled_back) echo 11 ;;
    *) die "unknown manifest state: $1" ;;
  esac
}

complete_post_start_refusal() {
  local disable_rc=0 stop_rc=0 active_rc=0 enabled_rc=0 active='' enabled=''
  local -a failures=()
  "${SERVICE_MUTATE[@]}" disable pe-service || disable_rc=$?
  maybe_crash refusal-disabled
  "${SERVICE_MUTATE[@]}" stop pe-service || stop_rc=$?
  maybe_crash refusal-stopped
  active=$(systemctl_active_state pe-service) || active_rc=$?
  enabled=$(systemctl_enabled_state pe-service) || enabled_rc=$?
  ((disable_rc == 0)) || failures+=("disable failed ($disable_rc)")
  ((stop_rc == 0)) || failures+=("stop failed ($stop_rc)")
  ((active_rc == 0)) || failures+=("activity probe failed")
  ((enabled_rc == 0)) || failures+=("enablement probe failed")
  [[ "$active" == false ]] || failures+=("pe-service remained active")
  [[ "$enabled" == false ]] || failures+=("pe-service remained enabled")
  ((${#failures[@]} == 0)) || die "post-start refusal did not make pe-service inert: ${failures[*]}"
  maybe_crash refusal-inert-proved

  local json reason
  reason=$(manifest_get refusal.reason)
  json=$(python3 -c 'import json,sys
path=sys.argv[1]
value=json.load(open(path, encoding="utf-8"))
refusal=value.get("refusal")
if not isinstance(refusal,dict) or not refusal.get("at") or not refusal.get("reason"):
    raise SystemExit("invalid durable refusal intent")
for key in ("invocation_id","active_enter_timestamp","active_enter_unix"):
    value.pop(key,None)
value.setdefault("post_start_refusals",[]).append(refusal)
if refusal.get("forward_blocked") is True:
    value["forward_blocked"]={"at":refusal["at"],"reason":refusal["reason"]}
value.pop("refusal",None)
value["state"]="switched"
print(json.dumps(value,sort_keys=True,separators=(",",":")))' "$MANIFEST")
  atomic_manifest_json "$json" post-start-refusal
  die "$reason; pe-service disabled and stopped, activation rewound to switched"
}

post_start_refusal() {
  local reason=$1 forward_blocked=${2:-false} at json
  at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  json=$(python3 -c 'import json,sys
path,at,reason,forward_blocked=sys.argv[1:]
value=json.load(open(path, encoding="utf-8"))
refusal={"at":at,"reason":reason}
if forward_blocked == "true": refusal["forward_blocked"]=True
value["refusal"]=refusal
value["state"]="refusing"
print(json.dumps(value,sort_keys=True,separators=(",",":")))' \
    "$MANIFEST" "$at" "$reason" "$forward_blocked")
  atomic_manifest_json "$json" refusing
  maybe_crash refusal-intent-recorded
  complete_post_start_refusal
}

verify_staged_artifacts() {
  local path expected label
  while (($#)); do
    path=$(manifest_get "artifacts.$1.path")
    expected=$(manifest_get "artifacts.$1.sha256")
    label=$2
    [[ -f "$path" ]] || die "staged $label is absent: $path"
    [[ "$(sha256_file "$path")" == "$expected" ]] || die "staged $label hash drift"
    shift 2
  done
}

env_value() {
  local file=$1 name=$2
  (
    set +u
    set -a
    # shellcheck disable=SC1090
    source "$file"
    set +a
    printf '%s' "${!name-}"
  )
}

toml_value() {
  local file=$1 key=$2
  python3 -c 'import re,sys
text=open(sys.argv[1], encoding="utf-8").read()
match=re.search(r"(?m)^\s*"+re.escape(sys.argv[2])+r"\s*=\s*\"([^\"]*)\"\s*(?:#.*)?$", text)
print(match.group(1) if match else "")' "$file" "$key"
}

absolute_from_root() {
  local value=$1
  if [[ "$value" == /* ]]; then realpath -m "$value"; else realpath -m "$SERVICE_ROOT/$value"; fi
}

effective_path() {
  local env_file=$1 config_file=$2 env_name=$3 toml_name=$4 fallback=$5 value
  value=$(env_value "$env_file" "$env_name")
  [[ -n "$value" ]] || value=$(toml_value "$config_file" "$toml_name")
  [[ -n "$value" ]] || value=$fallback
  absolute_from_root "$value"
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

render_config() {
  local source=$1 destination=$2 bind=$3
  mkdir -p "$(dirname "$destination")"
  python3 -c 'import json,os,re,sys
source,destination,bind,generation=sys.argv[1:]
values={
 "bind":bind,
 "event_log_path":generation+"/paper.log",
 "source_event_log_path":generation+"/source_events.log",
 "jsonl_log_path":generation+"/paper.jsonl",
 "status_path":generation+"/status.json",
 "paper_state_db_path":generation+"/paper_state.db",
 "legacy_wallet_history_path":generation+"/wallet_market_history.json",
}
text=open(source, encoding="utf-8").read()
missing=[]
for key,value in values.items():
    pattern=r"(?m)^\s*"+re.escape(key)+r"\s*=.*$"
    replacement=key.ljust(32)+" = "+json.dumps(value)
    text,count=re.subn(pattern, replacement, text, count=1)
    if count == 0: missing.append(replacement)
if missing: text="\n".join(missing)+"\n\n"+text
parent=os.path.dirname(destination) or "."
tmp=os.path.join(parent, ".config.tmp.%d" % os.getpid())
with open(tmp,"w",encoding="utf-8") as handle:
    handle.write(text)
    handle.flush(); os.fsync(handle.fileno())
os.replace(tmp,destination)' "$source" "$destination" "$bind" "$generation_dir"
  python3 -c 'import os,sys
directory=os.open(sys.argv[1], os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try: os.fsync(directory)
finally: os.close(directory)' "$(dirname "$destination")"
}

render_env() {
  local source=$1 destination=$2 bind=$3 rehearsal=$4 anon=
  if [[ "$rehearsal" == true ]]; then
    anon=$(env_value "$source" PE_SUPABASE_ANON_KEY)
    [[ -n "$anon" ]] || die "rehearsal environment has no PE_SUPABASE_ANON_KEY"
  fi
  python3 -c 'import os,re,shlex,sys
source,destination,bind,generation,rehearsal,anon=sys.argv[1:]
values={
 "PE_BIND":bind,
 "PE_EVENT_LOG_PATH":generation+"/paper.log",
 "PE_SOURCE_EVENT_LOG_PATH":generation+"/source_events.log",
 "PE_JSONL_LOG_PATH":generation+"/paper.jsonl",
 "PE_STATUS_PATH":generation+"/status.json",
 "PE_PAPER_STATE_DB_PATH":generation+"/paper_state.db",
 "PE_LEGACY_WALLET_HISTORY_PATH":generation+"/wallet_market_history.json",
}
if rehearsal == "true": values["PE_SUPABASE_SECRET_KEY"]=anon
lines=open(source, encoding="utf-8").read().splitlines()
keys=set(values)
kept=[line for line in lines if not any(re.match(r"^\s*(?:export\s+)?"+re.escape(key)+r"=",line) for key in keys)]
kept.extend(key+"="+shlex.quote(value) for key,value in values.items())
parent=os.path.dirname(destination) or "."; os.makedirs(parent,mode=0o700,exist_ok=True)
tmp=os.path.join(parent,".env.tmp.%d" % os.getpid())
fd=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
with os.fdopen(fd,"w",encoding="utf-8") as handle:
    handle.write("\n".join(kept)+"\n")
    handle.flush(); os.fchmod(handle.fileno(),0o600); os.fsync(handle.fileno())
os.replace(tmp,destination)
directory=os.open(parent,os.O_RDONLY|getattr(os,"O_DIRECTORY",0))
try: os.fsync(directory)
finally: os.close(directory)' \
    "$source" "$destination" "$bind" "$generation_dir" "$rehearsal" "$anon"
}

validate_environment() {
  local file=$1 bind=$2 rehearsal=$3 key expected actual
  declare -A expected_values=(
    [PE_BIND]="$bind"
    [PE_EVENT_LOG_PATH]="$generation_dir/paper.log"
    [PE_SOURCE_EVENT_LOG_PATH]="$generation_dir/source_events.log"
    [PE_JSONL_LOG_PATH]="$generation_dir/paper.jsonl"
    [PE_STATUS_PATH]="$generation_dir/status.json"
    [PE_PAPER_STATE_DB_PATH]="$generation_dir/paper_state.db"
    [PE_LEGACY_WALLET_HISTORY_PATH]="$generation_dir/wallet_market_history.json"
  )
  for key in "${!expected_values[@]}"; do
    expected=${expected_values[$key]}
    actual=$(env_value "$file" "$key")
    [[ "$actual" == "$expected" ]] || die "$file resolves $key=$actual, expected $expected"
  done
  [[ -n "$(env_value "$file" PE_SUPABASE_URL)" ]] || die "$file has no PE_SUPABASE_URL"
  [[ -n "$(env_value "$file" PE_SUPABASE_SECRET_KEY)" ]] || die "$file has no PE_SUPABASE_SECRET_KEY"
  if [[ "$rehearsal" == true ]]; then
    [[ "$(env_value "$file" PE_SUPABASE_SECRET_KEY)" == "$(env_value "$file" PE_SUPABASE_ANON_KEY)" ]] ||
      die "rehearsal environment does not put the publishable key in the secret slot"
  fi
}

stage_inputs() {
  local stage="$generation_dir/staged"
  mkdir -p "$stage"
  atomic_adopt "$input_binary" "$stage/pe-service" 0755 stage-binary
  render_config "$config_template" "$stage/service.toml" "$production_bind"
  maybe_crash rendered-config
  render_config "$config_template" "$stage/rehearsal.service.toml" "$rehearsal_bind"
  maybe_crash rendered-rehearsal-config
  render_env "$service_env_template" "$stage/service.env" "$production_bind" false
  maybe_crash rendered-env
  render_env "$rehearsal_env_template" "$stage/rehearsal.env" "$rehearsal_bind" true
  maybe_crash rendered-rehearsal-env
  validate_environment "$stage/service.env" "$production_bind" false
  validate_environment "$stage/rehearsal.env" "$rehearsal_bind" true
  "$stage/pe-service" --verify-staged-revision "$merge_commit"
}

if [[ -f "$MANIFEST" ]]; then
  manifest_id=$(manifest_get activation_id)
  manifest_state=$(manifest_get state)
  if [[ "$manifest_id" != "$activation_id" ]]; then
    if [[ "$manifest_state" != verified && "$manifest_state" != rolled_back ]]; then
      die "activation $manifest_id is non-terminal at $manifest_state"
    fi
  elif [[ "$manifest_state" == refusing ]]; then
    complete_post_start_refusal
  elif forward_blocked_reason=$(python3 -c 'import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
blocked=value.get("forward_blocked")
print(blocked.get("reason", "") if isinstance(blocked,dict) else "")' "$MANIFEST") &&
      [[ -n "$forward_blocked_reason" ]]; then
    die "forward blocked: $forward_blocked_reason; run rollback_generation.sh"
  elif [[ "$manifest_state" == verified ]]; then
    echo "activation $activation_id is already verified"
    exit 0
  elif [[ "$manifest_state" == rolling_back || "$manifest_state" == rolled_back ]]; then
    die "activation $activation_id is rolling back or rolled back; forward resume is refused"
  fi
fi

if [[ ! -f "$MANIFEST" || "$(manifest_get activation_id 2>/dev/null || true)" != "$activation_id" ]]; then
  archive_columns_required=false
  if [[ -f "$MANIFEST" ]] && manifest_requires_archive_columns; then
    archive_columns_required=true
  fi
  archive_counts=$(activation_archive_counts "$activation_id" "$archive_columns_required")
  if activation_archive_stamp_exists "$archive_counts"; then
    die "activation id already used: rows stamped $archive_counts; choose a new id"
  fi
  SIMULATE_CRASH_AFTER="$SIMULATE_CRASH_AFTER" \
    "$REPO_ROOT/scripts/paper_reset/seed_v1_empty.sh" --execute \
    --source-main "$source_v1_main" --generation-dir "$generation_dir" \
    --legacy-history "$legacy_history"
  stage_inputs

  verify_installed_unit_owner
  old_event=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_EVENT_LOG_PATH event_log_path ./paper.log)
  old_source=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_SOURCE_EVENT_LOG_PATH source_event_log_path source_events.log)
  old_status=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_STATUS_PATH status_path ./status.json)
  old_db=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_PAPER_STATE_DB_PATH paper_state_db_path ./paper_state.db)
  old_history=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_LEGACY_WALLET_HISTORY_PATH legacy_wallet_history_path ./wallet_market_history.json)
  old_live="$(dirname "$old_event")/live_journal.log"
  initial=$(python3 -c 'import json,sys
(activation,generation,commit,bankroll,source_main,legacy,stage,old_event,old_source,
 old_status,old_db,old_history,old_live,config,env,binary)=sys.argv[1:]
def artifact(path):
 import hashlib
 with open(path,"rb") as handle: digest=hashlib.sha256(handle.read()).hexdigest()
 return {"path":path,"sha256":digest}
value={
 "activation_id":activation,"state":"seed","generation_dir":generation,
 "merge_commit":commit,"bankroll":bankroll,"source_v1_main":artifact(source_main),
 "legacy_history":artifact(legacy),
 "artifacts":{
   "seed_main":artifact(generation+"/paper_state.db"),
   "binary":artifact(stage+"/pe-service"),"config":artifact(stage+"/service.toml"),
   "environment":artifact(stage+"/service.env"),
   "rehearsal_config":artifact(stage+"/rehearsal.service.toml"),
   "rehearsal_environment":artifact(stage+"/rehearsal.env")},
 "old_installed_artifacts":{"service_toml":artifact(config),"service_env":artifact(env),
   "pe_service":artifact(binary)},
 "destinations":{"binary":binary,"config":config,"environment":env},
 "old_paths":{"paper_log":old_event,"source_log":old_source,"live_journal":old_live,
   "status":old_status,"paper_state":old_db,"legacy_history":old_history}}
print(json.dumps(value,sort_keys=True,separators=(",",":")))' \
    "$activation_id" "$generation_dir" "$merge_commit" "$bankroll" "$source_v1_main" \
    "$legacy_history" "$generation_dir/staged" "$old_event" "$old_source" "$old_status" \
    "$old_db" "$old_history" "$old_live" "$SERVICE_CONFIG" "$SERVICE_ENV" "$SERVICE_BINARY")
  atomic_manifest_json "$initial" seed
  hold_deploy_lock_at_test_boundary
fi

[[ "$(manifest_get activation_id)" == "$activation_id" ]] || die "manifest activation mismatch"
[[ "$(manifest_get generation_dir)" == "$generation_dir" ]] || die "manifest generation mismatch"
[[ "$(manifest_get merge_commit)" == "$merge_commit" ]] || die "manifest merge commit mismatch"
[[ "$(manifest_get bankroll)" == "$bankroll" ]] || die "manifest bankroll mismatch"
verify_staged_artifacts environment "production environment" \
  rehearsal_environment "rehearsal environment" rehearsal_config "rehearsal config" \
  binary "rehearsal binary"
[[ "$(env_value "$(manifest_get artifacts.environment.path)" PE_BIND)" == "$production_bind" ]] ||
  die "manifest production bind mismatch"
[[ "$(env_value "$(manifest_get artifacts.rehearsal_environment.path)" PE_BIND)" == "$rehearsal_bind" ]] ||
  die "manifest rehearsal bind mismatch"
state=$(manifest_get state)

verify_pre_t0_artifacts() {
  local event source status db history live
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(manifest_get old_installed_artifacts.service_toml.sha256)" ]] ||
    die "installed pre-T0 service config hash drift"
  [[ "$(sha256_file "$SERVICE_ENV")" == "$(manifest_get old_installed_artifacts.service_env.sha256)" ]] ||
    die "installed pre-T0 service environment hash drift"
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$(manifest_get old_installed_artifacts.pe_service.sha256)" ]] ||
    die "installed pre-T0 service binary hash drift"
  event=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_EVENT_LOG_PATH event_log_path ./paper.log)
  source=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_SOURCE_EVENT_LOG_PATH source_event_log_path source_events.log)
  status=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_STATUS_PATH status_path ./status.json)
  db=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_PAPER_STATE_DB_PATH paper_state_db_path ./paper_state.db)
  history=$(effective_path "$SERVICE_ENV" "$SERVICE_CONFIG" PE_LEGACY_WALLET_HISTORY_PATH legacy_wallet_history_path ./wallet_market_history.json)
  live="$(dirname "$event")/live_journal.log"
  [[ "$event" == "$(manifest_get old_paths.paper_log)" &&
     "$source" == "$(manifest_get old_paths.source_log)" &&
     "$status" == "$(manifest_get old_paths.status)" &&
     "$db" == "$(manifest_get old_paths.paper_state)" &&
     "$history" == "$(manifest_get old_paths.legacy_history)" &&
     "$live" == "$(manifest_get old_paths.live_journal)" ]] ||
    die "installed pre-T0 effective paths drifted from the manifest"
}

verify_pre_t0_owner() {
  verify_installed_unit_owner
  verify_pre_t0_artifacts
}

if (( $(state_rank "$state") < $(state_rank guarded) )); then
  if [[ "$state" == prechecked ]]; then
    service_active=$(systemctl_active_state pe-service)
    service_enabled=$(systemctl_enabled_state pe-service)
    if [[ "$service_active" == false && "$service_enabled" == false ]]; then
      # A crash can land after disable+stop but before the guarded manifest rename. The frozen-batch
      # recheck that precedes T0 must still happen here; the old service is already stopped, so a
      # changed batch leaves a rollback-only `guarded` manifest instead of continuing forward.
      verify_pre_t0_artifacts
      latest_batch=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc 'select max(batch_id) from ranking_batches;')
      if [[ "$latest_batch" != "$(manifest_get ranking_batch_id)" ]]; then
        blocked_patch=$(python3 -c 'import json,sys; print(json.dumps({"forward_blocked":{"at":sys.argv[1],"reason":sys.argv[2]}}))' \
          "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
          "latest ranking batch $latest_batch changed after prechecked while pe-service was already stopped")
        manifest_advance guarded "$blocked_patch"
        die "latest ranking batch $latest_batch changed after prechecked while pe-service was already stopped; forward blocked, run rollback_generation.sh"
      fi
      manifest_advance guarded
      state=guarded
    else
      verify_pre_t0_owner
    fi
  else
    verify_pre_t0_owner
  fi
fi

verify_seed_or_prepared() {
  local expected_version=$1 version total integrity expected_hash
  version=$(sqlite3 -readonly "file:$generation_dir/paper_state.db?immutable=1" 'pragma user_version;')
  [[ "$version" == "$expected_version" ]] || die "generation user_version=$version, expected $expected_version"
  integrity=$(sqlite3 -readonly "file:$generation_dir/paper_state.db?immutable=1" 'pragma integrity_check;')
  [[ "$integrity" == ok ]] || die "generation integrity_check=$integrity"
  if [[ "$expected_version" == 1 ]]; then
    expected_hash=$(manifest_get artifacts.seed_main.sha256)
    [[ "$(sha256_file "$generation_dir/paper_state.db")" == "$expected_hash" ]] ||
      die "version-one seed main hash drift"
    total=$(sqlite3 -readonly "file:$generation_dir/paper_state.db?immutable=1" \
      "select (select count(*) from fills)+(select count(*) from positions)+(select count(*) from bankroll)+(select count(*) from settled_markets)+(select count(*) from fill_market_snapshots)+(select count(*) from seen_trades)+(select count(*) from leader_positions)+(select count(*) from poll_cursors)+(select count(*) from meta)+(select count(*) from no_copy_dispositions)+(select count(*) from dispatch_seeds)+(select count(*) from dispatch_targets);")
    [[ "$total" == 0 ]] || die "version-one seed contains $total row(s)"
    [[ -f "$generation_dir/paper.log" && ! -s "$generation_dir/paper.log" ]] ||
      die "version-one seed paper.log is absent or non-empty"
    [[ -f "$generation_dir/live_journal.log" && ! -s "$generation_dir/live_journal.log" ]] ||
      die "version-one seed live_journal.log is absent or non-empty"
    [[ -f "$generation_dir/source_events.log" && ! -s "$generation_dir/source_events.log" ]] ||
      die "version-one seed source_events.log is absent or non-empty"
    expected_hash=$(manifest_get legacy_history.sha256)
    [[ "$(sha256_file "$generation_dir/wallet_market_history.json")" == "$expected_hash" ]] ||
      die "version-one seed legacy history hash drift"
  else
    read -r fills settled watermark sealed mismatch < <(sqlite3 -separator ' ' -readonly \
      "file:$generation_dir/paper_state.db?immutable=1" \
      "select (select count(*) from fills), (select count(*) from settled_markets), (select count(*) from meta where key='last_supabase_applied_event_seq'), (select count(*) from poll_cursors_v1_sealed), (select count(*) from poll_cursors where last_ts_unix <> activity_cutoff_unix or activity_cutoff_unix is null);")
    [[ "$fills $settled $watermark $sealed $mismatch" == "0 0 0 0 0" ]] ||
      die "prepared generation postconditions failed: fills=$fills settled=$settled watermark=$watermark sealed=$sealed cursor_mismatch=$mismatch"
    [[ ! -s "$generation_dir/paper.log" && ! -s "$generation_dir/live_journal.log" ]] ||
      die "prepared paper/live logs must remain empty"
    [[ -s "$generation_dir/source_events.log" ]] || die "prepared source log must be non-empty"
  fi
}

if (( $(state_rank "$state") < $(state_rank prepared) )); then
  seed_version=$(sqlite3 -readonly "file:$generation_dir/paper_state.db?immutable=1" 'pragma user_version;')
  if [[ "$seed_version" == 1 ]]; then
    verify_seed_or_prepared 1
  elif [[ "$seed_version" != 2 ]]; then
    die "generation user_version=$seed_version, expected 1 or resumable 2"
  fi
  rehearsal_env=$(manifest_get artifacts.rehearsal_environment.path)
  rehearsal_config=$(manifest_get artifacts.rehearsal_config.path)
  staged_binary=$(manifest_get artifacts.binary.path)
  verify_staged_artifacts rehearsal_environment "rehearsal environment" \
    rehearsal_config "rehearsal config" binary "rehearsal binary"
  "$DEPLOY_SCRIPT_DIR/rehearsal_preflight.sh" "$rehearsal_env"
  verify_staged_artifacts rehearsal_environment "rehearsal environment" \
    rehearsal_config "rehearsal config" binary "rehearsal binary"
  (
    set -a
    # shellcheck disable=SC1090
    source "$rehearsal_env"
    set +a
    cd "$SERVICE_ROOT"
    "$staged_binary" "$rehearsal_config" --exit-after-anchors
  )
  # A version-two main is never adopted from table counts alone. Re-entering the
  # binary completes or verifies the machine-owned migration through installed.
  verify_seed_or_prepared 2
  manifest_advance prepared
  state=prepared
fi

verify_fresh_supabase() {
  local result
  result=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
    "select case when (select count(*) from paper_fills)=0
       and (select count(*) from settled_markets)=0
       and (select count(*) from paper_positions)=0
       and (select count(*) from fill_market_snapshots)=0
       and (select count(*) from paper_bankroll where id=0 and bankroll_str::numeric='$bankroll'::numeric)=1
       then 'ok' else 'mismatch' end;")
  [[ "$result" == ok ]] || die "Supabase fresh-book verification failed: $result"
}

if [[ "$state" == archived ]]; then
  archive_columns_required=false
  manifest_requires_archive_columns && archive_columns_required=true
  counts=$(activation_archive_counts "$activation_id" "$archive_columns_required")
  read -r _ _ _ bankroll_archived _ <<<"$counts"
  if activation_archive_stamp_exists "$counts"; then
    activation_archive_counts_match_pre_reset "$counts" ||
      die "activation archive counts do not match the recorded pre-reset live counts"
    [[ "$bankroll_archived" -gt 0 ]] || die "activation archive lacks its guaranteed bankroll stamp"
    verify_fresh_supabase
    patch=$(python3 -c 'import json,sys; print(json.dumps({"archive_counts":sys.argv[1]}))' "$counts")
    manifest_advance reset "$patch"
    state=reset
  fi
fi

if (( $(state_rank "$state") < $(state_rank prechecked) )); then
  "$DEPLOY_SCRIPT_DIR/rehearsal_preflight.sh" "$(manifest_get artifacts.rehearsal_environment.path)"
  batch_id=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc 'select max(batch_id) from ranking_batches;')
  [[ "$batch_id" =~ ^[0-9]+$ ]] || die "latest ranking batch is absent"
  wallets_file=$(mktemp "$generation_dir/.batch-wallets.XXXXXX")
  trap 'rm -f "$wallets_file"' EXIT
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
    "select lower(wallet_hex) from ranking_entries where batch_id=$batch_id and survives is true order by rank;" > "$wallets_file"
  due_wallets=0
  now_unix=$(date +%s)
  while IFS= read -r wallet; do
    [[ "$wallet" =~ ^0x[0-9a-f]{40}$ ]] || die "invalid wallet in ranking batch: $wallet"
    # 3600 is the canonical, non-configurable ANCHOR_REFRESH_SECS in docs/_GLOSSARY.md.
    due=$(sqlite3 -readonly "file:$generation_dir/paper_state.db?immutable=1" \
      "select case when exists(select 1 from wallet_fences where wallet_hex='$wallet')
        or not exists(select 1 from wallet_history_status_v2 where wallet_hex='$wallet' and complete=1)
        or not exists(select 1 from poll_cursors where wallet_hex='$wallet' and activity_cutoff_unix is not null and reanchor_required=0)
        or not exists(select 1 from position_anchors where wallet_hex='$wallet')
        or $now_unix-(select anchored_at_unix from position_anchors where wallet_hex='$wallet' order by anchor_seq desc limit 1)>3600
        then 1 else 0 end;")
    due_wallets=$((due_wallets + due))
  done < "$wallets_file"
  rm -f "$wallets_file"
  trap - EXIT
  if ((due_wallets > 0)); then
    [[ -n "$approved_due_subset" && "$approved_due_subset" -eq "$due_wallets" ]] ||
      die "due_wallets=$due_wallets; rerun prepared or pass the owner's exact --approve-due-subset decision"
  fi
  anchor_count=$(sqlite3 -readonly "file:$generation_dir/paper_state.db?immutable=1" 'select count(*) from position_anchors;')
  precheck_patch=$(python3 -c 'import json,sys
approved=None if sys.argv[4]=="" else int(sys.argv[4])
print(json.dumps({"ranking_batch_id":int(sys.argv[1]),"due_wallets":int(sys.argv[2]),"prepared_anchor_count":int(sys.argv[3]),"owner_approved_due_subset":approved}))' \
    "$batch_id" "$due_wallets" "$anchor_count" "$approved_due_subset")
  manifest_advance prechecked "$precheck_patch"
  state=prechecked
fi

if (( $(state_rank "$state") < $(state_rank guarded) )); then
  verify_pre_t0_owner
  latest_batch=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc 'select max(batch_id) from ranking_batches;')
  [[ "$latest_batch" == "$(manifest_get ranking_batch_id)" ]] ||
    die "latest ranking batch $latest_batch changed between prechecked and guarded; pe-service was not touched"
  "${SERVICE_MUTATE[@]}" disable pe-service
  "${SERVICE_MUTATE[@]}" stop pe-service
  service_active=$(systemctl_active_state pe-service)
  service_enabled=$(systemctl_enabled_state pe-service)
  [[ "$service_active" == false ]] || die "pe-service is still active"
  [[ "$service_enabled" == false ]] || die "pe-service is still enabled"
  manifest_advance guarded
  state=guarded
fi

copy_pre_t0_files() {
  local archive="$generation_dir/pre-t0" name source tmp
  mkdir -p "$archive"
  source=$(manifest_get old_paths.paper_state)
  [[ -f "$source" ]] || die "missing pre-T0 paper main: $source"
  if [[ ! -f "$archive/paper_state.db" ]]; then
    tmp="$archive/.paper_state.db.$$"
    sqlite3 -readonly "$source" ".backup '$tmp'"
    mv "$tmp" "$archive/paper_state.db"
    maybe_crash archive-paper-state
  fi
  for name in paper_log source_log live_journal legacy_history; do
    source=$(manifest_get "old_paths.$name")
    [[ -f "$source" ]] || die "missing pre-T0 $name: $source"
    atomic_adopt "$source" "$archive/$name" 0600 "archive-$name"
  done
  [[ -f "$SERVICE_CONFIG" && -f "$SERVICE_ENV" && -f "$SERVICE_BINARY" ]] ||
    die "one or more installed service artifacts are absent"
  atomic_adopt "$SERVICE_CONFIG" "$archive/service.toml" 0600 archive-config
  atomic_adopt "$SERVICE_ENV" "$archive/service.env" 0600 archive-env
  atomic_adopt "$SERVICE_BINARY" "$archive/pe-service" 0755 archive-binary
  source=$(manifest_get old_paths.status)
  [[ ! -f "$source" ]] || atomic_adopt "$source" "$archive/status.json" 0600 archive-status
  python3 -c 'import hashlib,json,os,sys
root=sys.argv[1]; result={}
for name in sorted(os.listdir(root)):
 path=os.path.join(root,name)
 if os.path.isfile(path):
  key=name.replace(".","_").replace("-","_")
  with open(path,"rb") as handle: result[key]={"path":path,"sha256":hashlib.sha256(handle.read()).hexdigest()}
print(json.dumps({"archive_artifacts":result},sort_keys=True))' "$archive"
}

if (( $(state_rank "$state") < $(state_rank archived) )); then
  service_active=$(systemctl_active_state pe-service)
  service_enabled=$(systemctl_enabled_state pe-service)
  [[ "$service_active" == false && "$service_enabled" == false ]] ||
    die "guarded service state was lost"
  artifacts_patch=$(copy_pre_t0_files)
  live_counts=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
    "select json_build_object('paper_fills',(select count(*) from paper_fills),'settled_markets',(select count(*) from settled_markets),'paper_positions',(select count(*) from paper_positions),'paper_bankroll',(select count(*) from paper_bankroll),'fill_market_snapshots',(select count(*) from fill_market_snapshots));")
  archived_patch=$(python3 -c 'import json,sys
value=json.loads(sys.argv[1]); value["pre_reset_live_counts"]=json.loads(sys.argv[2]); print(json.dumps(value))' \
    "$artifacts_patch" "$live_counts")
  manifest_advance archived "$archived_patch"
  state=archived
fi

if (( $(state_rank "$state") < $(state_rank reset) )); then
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v activation_id="$activation_id" \
    -v bankroll="$bankroll" -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
  maybe_crash db-commit
  verify_fresh_supabase
  counts=$(activation_archive_counts "$activation_id" true)
  read -r _ _ _ bankroll_archived _ <<<"$counts"
  [[ "$bankroll_archived" -gt 0 ]] || die "activation archive lacks its guaranteed bankroll stamp"
  activation_archive_counts_match_pre_reset "$counts" ||
    die "activation archive counts do not match the recorded pre-reset live counts"
  patch=$(python3 -c 'import json,sys; print(json.dumps({"archive_counts":sys.argv[1]}))' "$counts")
  manifest_advance reset "$patch"
  state=reset
fi

print_effective_configuration() {
  local env_file=$1
  printf '%s\n' \
    "effective.PE_BIND=$(env_value "$env_file" PE_BIND)" \
    "effective.PE_EVENT_LOG_PATH=$(env_value "$env_file" PE_EVENT_LOG_PATH)" \
    "effective.PE_SOURCE_EVENT_LOG_PATH=$(env_value "$env_file" PE_SOURCE_EVENT_LOG_PATH)" \
    "effective.PE_JSONL_LOG_PATH=$(env_value "$env_file" PE_JSONL_LOG_PATH)" \
    "effective.PE_STATUS_PATH=$(env_value "$env_file" PE_STATUS_PATH)" \
    "effective.PE_PAPER_STATE_DB_PATH=$(env_value "$env_file" PE_PAPER_STATE_DB_PATH)" \
    "effective.PE_LEGACY_WALLET_HISTORY_PATH=$(env_value "$env_file" PE_LEGACY_WALLET_HISTORY_PATH)"
}

if (( $(state_rank "$state") < $(state_rank switched) )); then
  staged_config=$(manifest_get artifacts.config.path)
  staged_env=$(manifest_get artifacts.environment.path)
  staged_binary=$(manifest_get artifacts.binary.path)
  [[ "$(sha256_file "$staged_config")" == "$(manifest_get artifacts.config.sha256)" ]] || die "staged config hash drift"
  [[ "$(sha256_file "$staged_env")" == "$(manifest_get artifacts.environment.sha256)" ]] || die "staged env hash drift"
  [[ "$(sha256_file "$staged_binary")" == "$(manifest_get artifacts.binary.sha256)" ]] || die "staged binary hash drift"
  # Replace artifacts in place at the proved paths so the same unit starts them identically; unit-file edits are caught by the next pre-T0 or post-start proof, without parsing or reloading the unit.
  atomic_adopt "$staged_config" "$SERVICE_CONFIG" 0644 adopted-config
  atomic_adopt "$staged_env" "$SERVICE_ENV" 0600 adopted-env
  atomic_adopt "$staged_binary" "$SERVICE_BINARY" 0755 adopted-binary
  validate_environment "$SERVICE_ENV" "$production_bind" false
  print_effective_configuration "$SERVICE_ENV"
  manifest_advance switched
  state=switched
fi

verify_running_generation() {
  local expected active pid invocation active_enter working running
  expected=$(manifest_get artifacts.binary.sha256)
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$expected" ]] || die "installed binary is not the generation binary"
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(manifest_get artifacts.config.sha256)" ]] || die "installed config hash drift"
  [[ "$(sha256_file "$SERVICE_ENV")" == "$(manifest_get artifacts.environment.sha256)" ]] || die "installed env hash drift"
  validate_environment "$SERVICE_ENV" "$production_bind" false
  read_service_process_snapshot || die "could not read the pe-service process snapshot"
  active=$SERVICE_SNAPSHOT_ACTIVE
  pid=$SERVICE_SNAPSHOT_PID
  invocation=$SERVICE_SNAPSHOT_INVOCATION
  active_enter=$SERVICE_SNAPSHOT_ACTIVE_ENTER
  [[ "$active" == active ]] || die "generation pe-service is not active"
  [[ "$pid" =~ ^[1-9][0-9]*$ ]] || die "pe-service has no MainPID"
  [[ "$(systemctl show pe-service -p NeedDaemonReload --value)" == no ]] ||
    die "pe-service unit files differ from the loaded configuration (daemon-reload pending)"
  working=$(readlink "$PROC_ROOT/$pid/cwd") || die "could not read pe-service process cwd"
  [[ "$(realpath -m "$working")" == "$SERVICE_ROOT" ]] ||
    die "pe-service process cwd is $working, expected $SERVICE_ROOT"
  running=$(sha256_file "$PROC_ROOT/$pid/exe")
  [[ "$running" == "$expected" ]] || die "running pe-service hash is not the generation hash"
  process_runs_service "$SERVICE_BINARY" "$SERVICE_CONFIG" "$SERVICE_ENV" "$working" "$pid" ||
    die "running pe-service does not run the generation binary, service config and environment"
  read_service_process_snapshot || die "could not re-read the pe-service process snapshot"
  [[ "$SERVICE_SNAPSHOT_ACTIVE" == "$active" && "$SERVICE_SNAPSHOT_PID" == "$pid" &&
     "$SERVICE_SNAPSHOT_INVOCATION" == "$invocation" &&
     "$SERVICE_SNAPSHOT_ACTIVE_ENTER" == "$active_enter" ]] ||
    die "pe-service process snapshot changed during generation proof"
  printf '%s\n%s\n' "$invocation" "$active_enter"
}

verify_listening_bind() {
  local pid port sockets
  pid=$(systemctl show pe-service -p MainPID --value)
  port=$(python3 -c 'import sys
value=sys.argv[1]
try: port=int(value.rsplit(":",1)[1])
except (IndexError,ValueError): raise SystemExit("invalid production bind")
if not 0 < port < 65536: raise SystemExit("invalid production bind port")
print(port)' "$production_bind")
  sockets=$(ss -H -ltnp "sport = :$port")
  [[ -n "$sockets" && "$sockets" == *"pid=$pid,"* ]] ||
    die "pe-service MainPID $pid is not listening on configured port $port"
}

if (( $(state_rank "$state") < $(state_rank started) )); then
  proof=
  service_active=$(systemctl_active_state pe-service)
  if [[ "$service_active" == false ]]; then
    "${SERVICE_MUTATE[@]}" enable pe-service
    "${SERVICE_MUTATE[@]}" start pe-service
    maybe_crash service-started
  fi
  service_active=$(systemctl_active_state pe-service)
  service_enabled=$(systemctl_enabled_state pe-service)
  [[ "$service_active" == true ]] || post_start_refusal "pe-service did not become active while entering started"
  [[ "$service_enabled" == true ]] || post_start_refusal "pe-service was disabled while entering started"
  if ! proof=$(verify_running_generation); then
    post_start_refusal "new pe-service process proof failed while entering started"
  fi
  service_active=$(systemctl_active_state pe-service)
  service_enabled=$(systemctl_enabled_state pe-service)
  [[ "$service_active" == true ]] || post_start_refusal "pe-service became inactive while entering started"
  [[ "$service_enabled" == true ]] || post_start_refusal "pe-service became disabled while entering started"
  mapfile -t proved_snapshot <<< "$proof"
  [[ "${#proved_snapshot[@]}" == 2 ]] || die "generation proof returned an invalid snapshot"
  invocation=${proved_snapshot[0]}
  [[ -n "$invocation" ]] || die "pe-service has no InvocationID"
  active_enter=${proved_snapshot[1]}
  [[ -n "$active_enter" ]] || die "pe-service has no ActiveEnterTimestamp"
  active_enter_unix=$(date --date="$active_enter" +%s)
  [[ "$active_enter_unix" =~ ^[0-9]+$ ]] || die "invalid pe-service ActiveEnterTimestamp: $active_enter"
  patch=$(python3 -c 'import json,sys; print(json.dumps({"invocation_id":sys.argv[1],"active_enter_timestamp":sys.argv[2],"active_enter_unix":int(sys.argv[3])}))' \
    "$invocation" "$active_enter" "$active_enter_unix")
  manifest_advance started "$patch"
  state=started
fi

verify_status() {
  python3 -c 'import json,sys
from datetime import datetime
path,revision,bankroll,active_enter_unix=sys.argv[1:]
value=json.load(open(path, encoding="utf-8"))
assert value.get("revision")==revision, "status revision mismatch"
assert str(value.get("bankroll"))==bankroll, "status bankroll mismatch"
updated=value.get("updated_at")
assert isinstance(updated,str), "status updated_at is absent"
updated_unix=datetime.fromisoformat(updated.replace("Z","+00:00")).timestamp()
assert updated_unix > int(active_enter_unix), "status predates the recorded invocation"
assert value.get("fills_total")==0 and value.get("settled_total")==0, "fresh local book is not empty"
tasks=value.get("tasks",[])
required={"activity_ingest","public_activity_poll","orchestrator","resolution_poller","watchlist_refresh","status_writer","http_server"}
running={row.get("name") for row in tasks if row.get("state")=="running"}
assert required <= running, "producer/critical task set is not running"
assert all(row.get("state")=="running" for row in tasks if row.get("class")=="critical"), "critical owner is not running"
projection=value.get("watchlist_projection") or {}
assert projection.get("applied") is not None and projection.get("last_error") is None, "watchlist projection has not succeeded"' \
    "$generation_dir/status.json" "$merge_commit" "$bankroll" "$1"
}

wait_for_invocation_status() {
  local active_enter_unix=$1
  for _ in {1..120}; do
    if [[ -f "$generation_dir/status.json" ]] && verify_status "$active_enter_unix" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  verify_status "$active_enter_unix" || true
  die "status.json did not become healthy and newer than the recorded invocation within 120 seconds"
}

verify_started_invocation() {
  read_service_process_snapshot || die "could not read the pe-service process snapshot"
  [[ "$SERVICE_SNAPSHOT_INVOCATION" == "$(manifest_get invocation_id)" ]] ||
    die "running pe-service InvocationID differs from the started manifest"
  [[ "$SERVICE_SNAPSHOT_ACTIVE_ENTER" == "$(manifest_get active_enter_timestamp)" ]] ||
    die "running pe-service ActiveEnterTimestamp differs from the started manifest"
}

record_site_confirmation() {
  local confirmed_by confirmed_at patch recorded_rc=0
  python3 -c 'import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
raise SystemExit(0 if value.get("site_confirmed_by") and value.get("site_confirmed_at") else 3)' \
    "$MANIFEST" || recorded_rc=$?
  if [[ "$recorded_rc" == 0 ]]; then
    return
  fi
  [[ "$recorded_rc" == 3 ]] || return "$recorded_rc"
  if [[ -t 0 ]]; then
    read -r -p "After signing in, verify the fresh era and enter your operator name to confirm: " confirmed_by
    if [[ -z "$confirmed_by" ]]; then
      echo "FATAL: signed-in site confirmation requires an operator name" >&2
      return 98
    fi
  else
    if [[ "$site_confirmed" != true ]]; then
      echo "FATAL: non-interactive verification requires --site-confirmed after the signed-in site check" >&2
      return 98
    fi
    confirmed_by=${SUDO_USER:-}
    [[ -n "$confirmed_by" ]] || confirmed_by=$(id -un) || return $?
  fi
  confirmed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ) || return $?
  patch=$(python3 -c 'import json,sys; print(json.dumps({"site_confirmed_by":sys.argv[1],"site_confirmed_at":sys.argv[2]}))' \
    "$confirmed_by" "$confirmed_at") || return $?
  manifest_patch_boundary site-confirmed "$patch" || return $?
  python3 -c 'import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
raise SystemExit(0 if value.get("site_confirmed_by")==sys.argv[2] and value.get("site_confirmed_at")==sys.argv[3] else 1)' \
    "$MANIFEST" "$confirmed_by" "$confirmed_at"
}

if (( $(state_rank "$state") < $(state_rank verified) )); then
  service_active=$(systemctl_active_state pe-service)
  service_enabled=$(systemctl_enabled_state pe-service)
  [[ "$service_active" == true ]] || post_start_refusal "pe-service is inactive while entering verified"
  [[ "$service_enabled" == true ]] || post_start_refusal "pe-service is disabled while entering verified"
  if ! (verify_running_generation >/dev/null); then
    post_start_refusal "new pe-service process proof failed while entering verified"
  fi
  if ! (verify_listening_bind); then
    post_start_refusal "pe-service is not listening on the configured production bind"
  fi
  if ! (verify_started_invocation); then
    post_start_refusal "recorded pe-service invocation changed while entering verified"
  fi
  if ! active_enter_unix=$(manifest_get active_enter_unix); then
    post_start_refusal "recorded pe-service invocation timestamp is unavailable"
  fi
  if ! (wait_for_invocation_status "$active_enter_unix"); then
    post_start_refusal "status.json did not become healthy and newer than the recorded invocation within 120 seconds"
  fi
  if ! (verify_started_invocation); then
    post_start_refusal "recorded pe-service invocation changed while entering verified"
  fi
  if ! latest_batch=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc 'select max(batch_id) from ranking_batches;'); then
    post_start_refusal "could not verify the frozen activation ranking batch"
  fi
  if ! frozen_batch=$(manifest_get ranking_batch_id); then
    post_start_refusal "frozen activation ranking batch is unavailable"
  fi
  if [[ "$latest_batch" != "$frozen_batch" ]]; then
    post_start_refusal \
      "latest ranking batch $latest_batch differs from the frozen activation batch $frozen_batch" true
  fi
  if ! before=$(manifest_get prepared_anchor_count); then
    post_start_refusal "prepared anchor count is unavailable"
  fi
  if ! approved=$(manifest_get owner_approved_due_subset); then
    post_start_refusal "owner-approved due subset is unavailable"
  fi
  [[ -n "$approved" ]] || approved=0
  if ! after=$(sqlite3 -readonly "file:$generation_dir/paper_state.db?immutable=1" \
    'select count(*) from position_anchors;'); then
    post_start_refusal "could not read the generation anchor count"
  fi
  if [[ ! "$before" =~ ^[0-9]+$ || ! "$approved" =~ ^[0-9]+$ || ! "$after" =~ ^[0-9]+$ ]]; then
    post_start_refusal "generation anchor counts are invalid"
  fi
  if [[ "$after" -ne $((before + approved)) ]]; then
    post_start_refusal \
      "boot anchor count changed by $((after - before)); expected approved walk of $approved"
  fi
  if ! (verify_seed_or_prepared 2); then
    post_start_refusal "prepared generation verification failed after service start"
  fi
  if ! psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -c \
    'refresh materialized view concurrently wallet_live_stats_mv;'; then
    post_start_refusal "wallet_live_stats_mv refresh failed after service start"
  fi
  if ! (verify_fresh_supabase); then
    post_start_refusal "Supabase fresh-book verification failed after service start"
  fi
  if ! projection=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
    "select case when (select count(*) from service_watchlist)>0 and
      (select watchlist_size from service_runtime where id=1)=(select count(*) from service_watchlist)
      then 'ok' else 'mismatch' end;"); then
    post_start_refusal "could not verify the Supabase watchlist projection"
  fi
  if [[ "$projection" != ok ]]; then
    post_start_refusal "Supabase projection verification failed: $projection"
  fi
  site_confirmation_rc=0
  (record_site_confirmation) || site_confirmation_rc=$?
  case "$site_confirmation_rc" in
    0) ;;
    86) exit 86 ;;
    98) exit 1 ;;
    *) post_start_refusal "signed-in site confirmation recording failed" ;;
  esac
  service_active=$(systemctl_active_state pe-service)
  service_enabled=$(systemctl_enabled_state pe-service)
  [[ "$service_active" == true ]] || post_start_refusal "pe-service became inactive while entering verified"
  [[ "$service_enabled" == true ]] || post_start_refusal "pe-service became disabled while entering verified"
  manifest_advance verified
  state=verified
fi

echo "activation_id=$activation_id state=$state generation=$generation_dir"
