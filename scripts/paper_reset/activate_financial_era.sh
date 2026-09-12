#!/usr/bin/env bash
# Locked, resumable schema-two financial-era activation (#545).

set -euo pipefail

financial_deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../deploy" && pwd -P)
# shellcheck source=../deploy/generation_common.sh
source "$financial_deploy_dir/generation_common.sh"

IDENTITY_MANIFEST=$MANIFEST
MANIFEST="$DEPLOY_HOME/pe-financial-era.json"

usage() {
  echo "usage: $0 --target-binary PATH --target-config PATH --target-environment PATH \
--paper-log PATH --source-log PATH --live-journal PATH --paper-state PATH \
--fresh-bankroll DECIMAL --rehearsal-evidence PATH \
--ranking-batch-id ID --membership-json PATH \
[--rollback-before-start] [--simulate-crash-after BOUNDARY]" >&2
  exit 2
}

target_binary= target_config= target_environment=
paper_log= source_log= live_journal= paper_state=
fresh_bankroll= artifact_blake3= ranking_batch_id=
target_revision=
membership_json= rehearsal_evidence=
rollback_before_start=false
while (($#)); do
  case "$1" in
    --target-binary) [[ $# -ge 2 ]] || usage; target_binary=$2; shift 2 ;;
    --target-config) [[ $# -ge 2 ]] || usage; target_config=$2; shift 2 ;;
    --target-environment) [[ $# -ge 2 ]] || usage; target_environment=$2; shift 2 ;;
    --paper-log) [[ $# -ge 2 ]] || usage; paper_log=$2; shift 2 ;;
    --source-log) [[ $# -ge 2 ]] || usage; source_log=$2; shift 2 ;;
    --live-journal) [[ $# -ge 2 ]] || usage; live_journal=$2; shift 2 ;;
    --paper-state) [[ $# -ge 2 ]] || usage; paper_state=$2; shift 2 ;;
    --fresh-bankroll) [[ $# -ge 2 ]] || usage; fresh_bankroll=$2; shift 2 ;;
    --rehearsal-evidence) [[ $# -ge 2 ]] || usage; rehearsal_evidence=$2; shift 2 ;;
    --ranking-batch-id) [[ $# -ge 2 ]] || usage; ranking_batch_id=$2; shift 2 ;;
    --membership-json) [[ $# -ge 2 ]] || usage; membership_json=$2; shift 2 ;;
    --rollback-before-start) rollback_before_start=true; shift ;;
    --simulate-crash-after) [[ $# -ge 2 ]] || usage; SIMULATE_CRASH_AFTER=$2; shift 2 ;;
    *) usage ;;
  esac
done

for value in target_binary target_config target_environment paper_log source_log live_journal \
  paper_state fresh_bankroll rehearsal_evidence ranking_batch_id membership_json; do
  [[ -n "${!value}" ]] || usage
done
[[ "$fresh_bankroll" =~ ^[0-9]+([.][0-9]{1,6})?$ ]] || die "fresh bankroll must be an exact non-negative six-decimal value"
[[ "$ranking_batch_id" =~ ^[0-9]+$ ]] || die "invalid ranking batch id"
[[ -f "$target_binary" && -f "$target_config" && -f "$target_environment" ]] ||
  die "one or more reviewed target artifacts are absent"
[[ -f "$membership_json" ]] || die "membership JSON is absent"

for command in python3 sha256sum flock systemctl psql mktemp curl; do
  command -v "$command" >/dev/null || die "$command not installed"
done
export SUPABASE_DB_URL
python3 -c 'import os,sys
raise SystemExit(0 if os.environ.get(sys.argv[1]) else 1)' SUPABASE_DB_URL ||
  die "SUPABASE_DB_URL must be exported for financial-era activation"

acquire_deploy_lock
[[ -f "$IDENTITY_MANIFEST" ]] || die "#557 activation identity is absent: $IDENTITY_MANIFEST"
identity=$(python3 -c 'import json,os,re,sys
path,installed_binary,installed_config,installed_environment=sys.argv[1:]
value=json.load(open(path, encoding="utf-8"))
required={"activation_id","state","generation_dir","merge_commit","bankroll","source_v1_main",
          "legacy_history","artifacts","old_installed_artifacts","destinations","old_paths"}
if not required <= set(value): raise SystemExit("#557 activation manifest lacks its production schema")
if value.get("state") != "verified": raise SystemExit("#557 activation is not verified")
activation=value.get("activation_id"); generation=value.get("generation_dir"); commit=value.get("merge_commit")
if not isinstance(activation,str) or re.fullmatch(r"[A-Za-z0-9._-]+",activation) is None:
    raise SystemExit("invalid #557 activation id")
if not isinstance(generation,str) or not os.path.isabs(generation) or not os.path.isdir(generation) or os.path.realpath(generation) != generation:
    raise SystemExit("invalid #557 generation directory")
if not isinstance(commit,str) or re.fullmatch(r"[0-9a-f]{40}",commit) is None:
    raise SystemExit("invalid #557 merge commit")
bankroll=value.get("bankroll")
if not isinstance(bankroll,str) or re.fullmatch(r"[0-9]+(?:[.][0-9]{1,6})?",bankroll) is None:
    raise SystemExit("invalid #557 bankroll")
def evidence(name):
    row=value.get(name)
    if not isinstance(row,dict) or set(row) != {"path","sha256"}:
        raise SystemExit("invalid #557 "+name+" evidence")
    evidence_path=row.get("path"); digest=row.get("sha256")
    if not isinstance(evidence_path,str) or not os.path.isabs(evidence_path):
        raise SystemExit("invalid #557 "+name+" path")
    if not isinstance(digest,str) or re.fullmatch(r"[0-9a-f]{64}",digest) is None:
        raise SystemExit("invalid #557 "+name+" hash")
    return row
source_v1_main=evidence("source_v1_main")
legacy_history=evidence("legacy_history")
artifacts=value.get("artifacts")
if not isinstance(artifacts,dict) or set(artifacts) != {"seed_main","binary","config","environment","rehearsal_config","rehearsal_environment"}:
    raise SystemExit("invalid #557 artifact inventory")
destinations=value.get("destinations")
if not isinstance(destinations,dict) or set(destinations) != {"binary","config","environment"}:
    raise SystemExit("invalid #557 installed destinations")
for name,installed in (("binary",installed_binary),("config",installed_config),("environment",installed_environment)):
    row=artifacts.get(name)
    if not isinstance(row,dict) or set(row) != {"path","sha256"}:
        raise SystemExit("invalid #557 "+name+" artifact")
    if not isinstance(row["path"],str) or not os.path.isabs(row["path"]):
        raise SystemExit("invalid #557 "+name+" artifact path")
    destination=destinations.get(name)
    if not isinstance(destination,str) or os.path.realpath(destination) != os.path.realpath(installed):
        raise SystemExit("#557 "+name+" destination differs from the installed old artifact")
    digest=row.get("sha256")
    if not isinstance(digest,str) or re.fullmatch(r"[0-9a-f]{64}",digest) is None:
        raise SystemExit("invalid #557 "+name+" hash")
print(activation); print(generation); print(commit); print(bankroll)
print(json.dumps(source_v1_main,sort_keys=True,separators=(",",":")))
print(json.dumps(legacy_history,sort_keys=True,separators=(",",":")))
print(artifacts["binary"]["sha256"]); print(artifacts["config"]["sha256"]); print(artifacts["environment"]["sha256"])' \
  "$IDENTITY_MANIFEST" "$SERVICE_BINARY" "$SERVICE_CONFIG" "$SERVICE_ENV") ||
  die "invalid #557 activation identity"
mapfile -t identity_parts <<< "$identity"
[[ ${#identity_parts[@]} -eq 9 ]] || die "invalid #557 identity result"
activation_id=${identity_parts[0]}
generation=${identity_parts[1]}
generation_merge_commit=${identity_parts[2]}
generation_bankroll=${identity_parts[3]}
generation_source_v1_main=${identity_parts[4]}
generation_legacy_history=${identity_parts[5]}
generation_artifact_sha256=${identity_parts[6]}
generation_config_sha256=${identity_parts[7]}
generation_environment_sha256=${identity_parts[8]}

staged_identity_output=$("$target_binary" --verify-staged-identity) ||
  die "staged binary could not derive its own identity"
read -r target_revision artifact_blake3 < <(parse_staged_identity <<< "$staged_identity_output") ||
  die "staged binary identity output is invalid"
"$target_binary" --verify-staged-identity "$target_revision" "$artifact_blake3" >/dev/null ||
  die "staged binary identity self-verification failed"
static_config_hash=$(python3 -c 'import hashlib,json,sys
config_hash,environment_hash=sys.argv[1:]
payload=json.dumps({"config_sha256":config_hash,"environment_sha256":environment_hash},sort_keys=True,separators=(",",":")).encode()
print(hashlib.sha256(b"prediction-edge/effective-static-config-v1\0"+payload).hexdigest())' \
  "$(sha256_file "$target_config")" "$(sha256_file "$target_environment")") ||
  die "derive effective staged configuration identity"
target_artifact_sha256=$(sha256_file "$target_binary")

validate_target_environment_key_class() {
  env_file_values "$target_environment" PE_SUPABASE_SECRET_KEY | python3 -c '
import base64, json, re, sys

parts = [part for part in sys.stdin.buffer.read().split(b"\0") if part]
if len(parts) != 1 or not parts[0].startswith(b"PE_SUPABASE_SECRET_KEY="):
    raise SystemExit("production secret slot is absent")
key = parts[0].split(b"=", 1)[1].decode("utf-8")
if key.startswith("sb_secret_") and len(key) > len("sb_secret_"):
    raise SystemExit(0)
if key.startswith("sb_publishable_"):
    raise SystemExit("production secret slot contains a publishable key")
parts = key.split(".")
if len(parts) != 3 or not all(re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in parts):
    raise SystemExit("production secret slot is neither sb_secret_* nor a legacy service-role JWT")
try:
    payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as error:
    raise SystemExit("production secret slot has a malformed legacy JWT") from error
if not isinstance(payload, dict) or payload.get("role") != "service_role":
    raise SystemExit("production legacy JWT role is not service_role")
' || die "reviewed production target secret slot is not secret/service-role class"
}

# The reviewed file is later adopted as the production service environment. Prove its authority
# credential class before creating a transition manifest or reaching any Start boundary.
validate_target_environment_key_class

financial_config_rows_dir=
financial_config_rows_file=
financial_readiness_response=
cleanup_financial_config_rows() {
  [[ -z "$financial_config_rows_file" || ! -e "$financial_config_rows_file" ]] ||
    rm -f -- "$financial_config_rows_file"
  [[ -z "$financial_readiness_response" || ! -e "$financial_readiness_response" ]] ||
    rm -f -- "$financial_readiness_response"
  [[ -z "$financial_config_rows_dir" || ! -d "$financial_config_rows_dir" ]] ||
    rmdir -- "$financial_config_rows_dir"
}
trap cleanup_financial_config_rows EXIT

export_financial_config_rows() {
  if [[ -z "$financial_config_rows_dir" ]]; then
    financial_config_rows_dir=$(mktemp -d)
    financial_config_rows_file="$financial_config_rows_dir/service_config.financial15.json"
  fi
  psql_service_db -v ON_ERROR_STOP=1 -Atc \
    "select coalesce(json_agg(json_build_object('key',key,'value',value,'value_type',value_type) order by key),'[]'::json)::text
       from service_config
      where key not in ('fill_mode','polymarket_fee_rate','risk_halt_release_hash');" \
    > "$financial_config_rows_file" || die "export the exact Financial15 configuration rows"
  [[ -s "$financial_config_rows_file" ]] || die "Financial15 configuration export is empty"
}

read_rehearsal_evidence() {
  local expected_harness_bundle
  expected_harness_bundle=$(harness_bundle_digest "$financial_deploy_dir") ||
    die "derive the current rehearsal harness bundle"
  python3 -c 'import hashlib,json,os,re,sys
evidence_path,expected_revision,expected_artifact,expected_artifact_sha,expected_activation,expected_generation,expected_config_sha,expected_environment_sha,expected_harness_bundle=sys.argv[1:]
def refuse(reason):
    print("REHEARSAL_REFUSAL="+reason,file=sys.stderr)
    raise SystemExit(1)
if not os.path.isfile(evidence_path): refuse("missing_evidence_file")
if os.path.islink(evidence_path): refuse("evidence_file_is_symlink")
try:
    with open(evidence_path,encoding="utf-8") as source: evidence=json.load(source)
except (OSError,ValueError):
    refuse("malformed_evidence_file")
expected_keys={"kind","result","evidence_sha256","manifest_path","target_revision","artifact_blake3","artifact_sha256","activation_id","generation_dir","copy_manifest_sha256","readiness_sha256","config_sha256","environment_sha256","rehearsal_environment_sha256"}
if not isinstance(evidence,dict) or set(evidence) != expected_keys or evidence.get("kind") != "rehearsal545-evidence-v1":
    refuse("malformed_evidence_file")
digest=evidence.get("evidence_sha256")
if evidence.get("result") != "PASS": refuse("rehearsal_did_not_pass")
if not isinstance(digest,str) or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
    refuse("malformed_evidence_hash")
manifest_path=evidence.get("manifest_path")
if not isinstance(manifest_path,str) or not os.path.isabs(manifest_path) or not os.path.isfile(manifest_path):
    refuse("missing_result_manifest")
if os.path.islink(manifest_path): refuse("result_manifest_is_symlink")
with open(manifest_path,"rb") as source: actual=hashlib.sha256(source.read()).hexdigest()
if actual != digest: refuse("evidence_hash_mismatch")
rows={}
try:
    with open(manifest_path,encoding="utf-8") as source:
        for raw in source:
            key,separator,value=raw.rstrip("\n").partition("=")
            if not separator or not key: refuse("malformed_result_manifest")
            if key in rows:
                if key == "legacy_continuations": refuse("malformed_legacy_continuations")
                if key == "harness_bundle_sha256": refuse("malformed_harness_bundle_sha256")
                if key in {"unit_kill_signal","unit_timeout_stop_secs"}: refuse("malformed_unit_policy")
                refuse("malformed_result_manifest")
            rows[key]=value
except (OSError,UnicodeError):
    refuse("malformed_result_manifest")
# Issue #584: this row stays in the hash-bound result manifest, not the fixed-shape outer JSON.
legacy_continuations=rows.get("legacy_continuations")
if legacy_continuations is None: refuse("missing_legacy_continuations")
if re.fullmatch(r"[0-9]+:[0-9a-f]{64}",legacy_continuations) is None:
    refuse("malformed_legacy_continuations")
# Issue #586: these bindings remain in the hash-bound result manifest so the fixed-shape outer
# rehearsal-evidence JSON does not change.
harness_bundle=rows.get("harness_bundle_sha256")
if harness_bundle is None: refuse("missing_harness_bundle_sha256")
if re.fullmatch(r"[0-9a-f]{64}",harness_bundle) is None:
    refuse("malformed_harness_bundle_sha256")
if harness_bundle != expected_harness_bundle: refuse("harness_bundle_mismatch")
unit_kill_signal=rows.get("unit_kill_signal")
unit_timeout_stop_secs=rows.get("unit_timeout_stop_secs")
if unit_kill_signal is None or unit_timeout_stop_secs is None: refuse("missing_unit_policy")
if (re.fullmatch(r"[0-9]+",unit_kill_signal) is None
        or re.fullmatch(r"[0-9]+",unit_timeout_stop_secs) is None):
    refuse("malformed_unit_policy")
for key in ("result","target_revision","artifact_blake3","artifact_sha256","activation_id","generation_dir","copy_manifest_sha256","readiness_sha256","config_sha256","environment_sha256","rehearsal_environment_sha256"):
    if rows.get(key) != evidence.get(key): refuse("evidence_identity_mismatch")
if rows.get("sha") != evidence.get("target_revision"): refuse("reviewed_revision_mismatch")
if evidence.get("target_revision") != expected_revision or evidence.get("artifact_blake3") != expected_artifact or evidence.get("artifact_sha256") != expected_artifact_sha:
    refuse("artifact_identity_mismatch")
if evidence.get("activation_id") != expected_activation: refuse("activation_identity_mismatch")
evidence_generation=evidence.get("generation_dir")
if not isinstance(evidence_generation,str) or not os.path.isabs(evidence_generation) or os.path.realpath(evidence_generation) != expected_generation:
    refuse("generation_identity_mismatch")
if evidence.get("config_sha256") != expected_config_sha: refuse("config_identity_mismatch")
if evidence.get("environment_sha256") != expected_environment_sha: refuse("environment_identity_mismatch")
for key in ("copy_manifest_sha256","readiness_sha256","rehearsal_environment_sha256"):
    value=evidence.get(key)
    if not isinstance(value,str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        refuse(key.removesuffix("_sha256")+"_identity_mismatch")
if evidence.get("rehearsal_environment_sha256") == expected_environment_sha:
    refuse("rehearsal_environment_identity_mismatch")
bound={
    "path":os.path.realpath(evidence_path),
    "manifest_path":os.path.realpath(manifest_path),
    "sha256":digest,
    "target_revision":evidence["target_revision"],
    "artifact_blake3":evidence["artifact_blake3"],
    "artifact_sha256":evidence["artifact_sha256"],
    "activation_id":evidence["activation_id"],
    "generation_dir":evidence["generation_dir"],
    "copy_manifest_sha256":evidence["copy_manifest_sha256"],
    "readiness_sha256":evidence["readiness_sha256"],
    "config_sha256":evidence["config_sha256"],
    "environment_sha256":evidence["environment_sha256"],
    "rehearsal_environment_sha256":evidence["rehearsal_environment_sha256"],
    "legacy_continuations":legacy_continuations,
    "harness_bundle_sha256":harness_bundle,
    "unit_kill_signal":int(unit_kill_signal),
    "unit_timeout_stop_secs":int(unit_timeout_stop_secs),
}
print(json.dumps(bound,sort_keys=True,separators=(",",":")))' \
    "$rehearsal_evidence" "$target_revision" "$artifact_blake3" "$target_artifact_sha256" \
    "$activation_id" "$generation" "$(sha256_file "$target_config")" "$(sha256_file "$target_environment")" \
    "$expected_harness_bundle"
}

verify_persisted_financial_guard() {
  local guard=$1 service_activity=${2:-} current persisted observed policy_valid=true
  local observed_kill observed_timeout expected_kill expected_timeout
  case "$guard" in
    harness_bundle)
      current=$(harness_bundle_digest "$financial_deploy_dir") ||
        die "derive the current rehearsal harness bundle"
      persisted=$(manifest_get rehearsal_evidence.harness_bundle_sha256)
      if [[ "$current" != "$persisted" ]]; then
        echo "REHEARSAL_REFUSAL=harness_bundle_mismatch" >&2
        die "harness_bundle_mismatch"
      fi
      ;;
    unit_stop_policy)
      if ! observed=$(service_unit_stop_policy pe-service); then policy_valid=false; fi
      read -r observed_kill observed_timeout <<< "$observed"
      expected_kill=$(manifest_get rehearsal_evidence.unit_kill_signal)
      expected_timeout=$(manifest_get rehearsal_evidence.unit_timeout_stop_secs)
      if [[ "$policy_valid" != true || "$observed_kill" != 2 ||
            "$observed_kill" != "$expected_kill" || "$observed_timeout" != "$expected_timeout" ]]; then
        printf 'REHEARSAL_REFUSAL=unit_stop_policy_drift service_active=%s observed_kill_signal=%s observed_timeout_stop_secs=%s\n' \
          "$service_activity" "$observed_kill" "$observed_timeout" >&2
        die "unit_stop_policy_drift"
      fi
      ;;
    *) die "unknown persisted financial guard: $guard" ;;
  esac
}

file_identity_json() {
  python3 -c 'import json,sys
print(json.dumps({"path":sys.argv[1],"sha256":sys.argv[2]},sort_keys=True,separators=(",",":")))' \
    "$1" "$(sha256_file "$1")"
}

installed_readiness_url() {
  local installed_bind='' assignment
  local -a parsed_bind
  mapfile -d '' -t parsed_bind < <(
    env_file_values "$SERVICE_ENV" PE_BIND && printf '__PE_ENV_FILE_PARSED__\0'
  )
  ((${#parsed_bind[@]} >= 1)) || return 1
  [[ ${parsed_bind[-1]} == __PE_ENV_FILE_PARSED__ ]] || return 1
  unset 'parsed_bind[-1]'
  for assignment in "${parsed_bind[@]}"; do
    [[ $assignment == PE_BIND=* ]] || return 1
    installed_bind=${assignment#*=}
  done
  if [[ -z "$installed_bind" ]]; then
    installed_bind=$(python3 -c 'import sys,tomllib
with open(sys.argv[1],"rb") as source: value=tomllib.load(source).get("bind","127.0.0.1:8080")
if not isinstance(value,str): raise SystemExit("installed bind is not a string")
print(value)' "$SERVICE_CONFIG") || return 1
  fi
  python3 -c 'import ipaddress,sys
value=sys.argv[1]
if value.startswith("["):
    close=value.find("]")
    if close < 0 or value[close+1:close+2] != ":": raise SystemExit("invalid installed bind")
    host,port=value[1:close],value[close+2:]
else:
    host,separator,port=value.rpartition(":")
    if not separator or ":" in host: raise SystemExit("invalid installed bind")
if not port.isascii() or not port.isdecimal() or not 1 <= int(port) <= 65535:
    raise SystemExit("invalid installed bind port")
address=ipaddress.ip_address(host)
if not address.is_loopback: raise SystemExit("installed readiness bind is not loopback")
url_host=f"[{address}]" if address.version == 6 else str(address)
print(f"http://{url_host}:{port}/health/ready")' "$installed_bind"
}

run_target_offline() {
  local -a preserved=("PATH=$PATH")
  [[ -z "${HOME:-}" ]] || preserved+=("HOME=$HOME")
  if [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]]; then
    preserved+=("PE_ACTIVATION_TESTING=1" "PE_ACTIVATION_TEST_ROOT=$PE_ACTIVATION_TEST_ROOT")
  fi
  env -i "${preserved[@]}" /bin/bash -c '
parsed=0
while IFS= read -r -d "" assignment <&3; do
  if [[ $assignment == __PE_ENV_FILE_PARSED__ ]]; then parsed=1; break; fi
  export "$assignment"
done
exec 3<&-
[[ $parsed == 1 ]] || exit 96
exec "$@"
' bash "$target_binary" "$target_config" "$@" \
    3< <(env_file_values "$target_environment" "${SERVICE_ENV_ALLOWLIST[@]}" &&
      printf '__PE_ENV_FILE_PARSED__\0')
}

manifest_complete_start() {
  local output status
  output=$(run_target_offline --financial-era=rollback-check \
    --activation-manifest="$MANIFEST") || {
    printf 'rollback-check output: %s\n' "$output" >&2
    return 2
  }
  COMPLETE_START_OUTPUT=$output
  if python3 -c 'import json,sys
try: value=json.loads(sys.argv[1])
except Exception: raise SystemExit(2)
if value.get("complete_start") is True: raise SystemExit(0)
if value.get("complete_start") is False: raise SystemExit(1)
raise SystemExit(2)' "$output"; then
    return 0
  else
    status=$?
  fi
  if [[ $status -ne 1 ]]; then
    printf 'rollback-check output: %s\n' "$output" >&2
    return 2
  fi
  return 1
}

complete_sqlite_backup() {
  local source=$1 destination=$2
  python3 -c 'import os,sqlite3,sys
source,destination=sys.argv[1:]
tmp=destination+".tmp.%d" % os.getpid()
src=sqlite3.connect("file:"+source+"?mode=ro", uri=True)
dst=sqlite3.connect(tmp)
try:
    src.backup(dst)
    answer=dst.execute("pragma integrity_check").fetchone()
    if answer != ("ok",): raise SystemExit("SQLite backup integrity check failed")
    dst.commit()
finally:
    dst.close(); src.close()
with open(tmp,"rb") as handle: os.fsync(handle.fileno())
os.replace(tmp,destination)
directory=os.open(os.path.dirname(destination) or ".",os.O_RDONLY|getattr(os,"O_DIRECTORY",0))
try: os.fsync(directory)
finally: os.close(directory)' "$source" "$destination"
}

restore_sqlite_backup() {
  local source=$1 destination=$2
  python3 -c 'import os,shutil,sqlite3,sys
source,destination=sys.argv[1:]
tmp=destination+".restore.%d" % os.getpid()
shutil.copyfile(source,tmp)
db=sqlite3.connect("file:"+tmp+"?mode=ro",uri=True)
try:
    if db.execute("pragma integrity_check").fetchone() != ("ok",): raise SystemExit("restored SQLite integrity check failed")
finally: db.close()
with open(tmp,"rb") as handle: os.fsync(handle.fileno())
os.replace(tmp,destination)
for suffix in ("-wal","-shm"):
    try: os.unlink(destination+suffix)
    except FileNotFoundError: pass
with open(destination,"rb") as handle: os.fsync(handle.fileno())
directory=os.open(os.path.dirname(destination) or ".",os.O_RDONLY|getattr(os,"O_DIRECTORY",0))
try: os.fsync(directory)
finally: os.close(directory)' "$source" "$destination"
}

manifest_flag() {
  local key=$1
  python3 -c 'import json,sys
value=json.load(open(sys.argv[1],encoding="utf-8"))
raise SystemExit(0 if value.get(sys.argv[2]) is True else 1)' "$MANIFEST" "$key"
}

remote_paper_census() {
  psql_service_db -v ON_ERROR_STOP=1 -Atc \
    "select json_build_object('paper_fills',(select count(*) from paper_fills),'settled_markets',(select count(*) from settled_markets),'paper_positions',(select count(*) from paper_positions),'paper_bankroll',(select count(*) from paper_bankroll),'fill_market_snapshots',(select count(*) from fill_market_snapshots))::text;"
}

# Read-only, catalog-ordered bidirectional equality for one activation's five remote archives.
# A nonzero result means restoration is still required; success proves a retry need not mutate.
activation_archive_matches_live() {
  local activation_id=$1
  psql_service_db -v ON_ERROR_STOP=1 -v activation_id="$activation_id" <<'SQL'
select set_config('pe.activation_id', :'activation_id', true);
do $$
declare
  activation text := current_setting('pe.activation_id');
  t text;
  arch text;
  collist text;
  differs boolean;
  tables constant text[] := array[
    'paper_fills', 'settled_markets', 'paper_positions',
    'paper_bankroll', 'fill_market_snapshots'
  ];
begin
  foreach t in array tables loop
    arch := t || '_archive';
    select string_agg(format('%I', a.attname), ', ' order by a.attnum)
      into collist
      from pg_attribute a
     where a.attrelid = t::regclass and a.attnum > 0 and not a.attisdropped;
    execute format(
      'select exists ((select %1$s from %2$I) except all '
      '(select %1$s from %3$I where activation_id = $1)) or exists '
      '((select %1$s from %3$I where activation_id = $1) except all '
      '(select %1$s from %2$I))',
      collist, t, arch
    ) into differs using activation;
    if differs then
      raise exception 'live % differs from activation % archive', t, activation;
    end if;
  end loop;
end $$;
SQL
}

verify_guarded_log_identities() {
  python3 -c 'import hashlib,json,os,sys
manifest=json.load(open(sys.argv[1],encoding="utf-8"))
logs=manifest.get("guarded_logs")
if not isinstance(logs,dict) or set(logs) != {"paper","source","live"}: raise SystemExit(1)
for name in ("paper","source","live"):
    row=logs[name]; path=row.get("path")
    if not isinstance(path,str) or not os.path.isfile(path): raise SystemExit(1)
    if os.path.getsize(path) != row.get("bytes"): raise SystemExit(1)
    if hashlib.sha256(open(path,"rb").read()).hexdigest() != row.get("sha256"): raise SystemExit(1)' \
    "$MANIFEST"
}

verify_guarded_log_prefixes() {
  python3 -c 'import hashlib,json,os,sys
manifest=json.load(open(sys.argv[1],encoding="utf-8"))
logs=manifest.get("guarded_logs")
if not isinstance(logs,dict) or set(logs) != {"paper","source","live"}: raise SystemExit(1)
for name in ("paper","source","live"):
    row=logs[name]; path=row.get("path"); size=row.get("bytes")
    if not isinstance(path,str) or not isinstance(size,int) or size < 0: raise SystemExit(1)
    if not os.path.isfile(path) or os.path.getsize(path) < size: raise SystemExit(1)
    with open(path,"rb") as handle: prefix=handle.read(size)
    if len(prefix) != size or hashlib.sha256(prefix).hexdigest() != row.get("sha256"): raise SystemExit(1)' \
    "$MANIFEST"
}

verify_pre_start_restore_authority() {
  manifest_flag remote_archive_completed || die "rollback requires a completed activation archive receipt"
  python3 -c 'import json,sys
value=json.load(open(sys.argv[1],encoding="utf-8"))
if not isinstance(value.get("remote_census"),dict): raise SystemExit(1)
if not isinstance(value.get("backup"),dict): raise SystemExit(1)' "$MANIFEST" ||
    die "rollback requires the recorded remote census and local backup"
  local counts
  counts=$(activation_archive_counts "$activation_id" true) || die "read activation archive stamp"
  activation_archive_stamp_exists "$counts" || die "activation archive stamp is absent"
  python3 -c 'import json,sys
actual=[int(part) for part in sys.argv[1].split()]
value=json.load(open(sys.argv[2],encoding="utf-8"))["remote_census"]
keys=["paper_fills","settled_markets","paper_positions","paper_bankroll","fill_market_snapshots"]
if actual != [int(value[key]) for key in keys]: raise SystemExit(1)
if actual[3] != 1: raise SystemExit(1)' "$counts" "$MANIFEST" ||
    die "activation archive stamp differs from the recorded remote census"
}

reconcile_remote_archive_receipt() {
  manifest_flag remote_archive_completed && return 0
  manifest_flag remote_archive_intent || return 1
  local counts
  counts=$(activation_archive_counts "$activation_id" false) ||
    die "could not reconcile the in-flight activation archive"
  activation_archive_stamp_exists "$counts" || return 1
  python3 -c 'import json,sys
actual=[int(part) for part in sys.argv[1].split()]
value=json.load(open(sys.argv[2],encoding="utf-8")).get("remote_census")
keys=["paper_fills","settled_markets","paper_positions","paper_bankroll","fill_market_snapshots"]
if not isinstance(value,dict) or actual != [int(value[key]) for key in keys]: raise SystemExit(1)
if actual[3] != 1: raise SystemExit(1)' "$counts" "$MANIFEST" ||
    die "in-flight activation archive stamp differs from the recorded census"
  manifest_patch_boundary remote-archived '{"remote_archive_completed":true,"remote_archive_reconciled":true}'
}

finish_unmutated_rollback() {
  manifest_flag qualification_start_intent &&
    die "a local Start intent without an archive receipt is inconsistent"
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$(manifest_get old_artifact_sha256)" ]] ||
    die "pre-Start service binary changed"
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(manifest_get old_config_sha256)" ]] ||
    die "pre-Start service config changed"
  [[ "$(sha256_file "$SERVICE_ENV")" == "$(manifest_get old_environment_sha256)" ]] ||
    die "pre-Start service environment changed"
  if manifest_flag service_stop_intent; then
    local was_active current_active
    was_active=$(manifest_get service_was_active)
    current_active=$(systemctl_active_state pe-service)
    if [[ "$was_active" == true && "$current_active" == false ]]; then
      manifest_patch_boundary rollback-old-service-start-intent '{"old_service_start_intent":true}'
      "${SERVICE_MUTATE[@]}" start pe-service
      [[ "$(systemctl_active_state pe-service)" == true ]] || die "old pe-service did not start"
      [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]] || verify_installed_unit_owner
      manifest_patch_boundary old-service-started '{"old_service_started":true}'
    elif [[ "$was_active" == false && "$current_active" != false ]]; then
      die "originally inactive service unexpectedly became active"
    fi
  fi
  manifest_advance rolled_back \
    '{"archive_restore_skipped":true,"local_restore_skipped":true,"no_financial_mutation":true}'
  echo "activation_id=$activation_id state=rolled_back"
  exit 0
}

if [[ ! -f "$MANIFEST" ]]; then
  [[ "$rollback_before_start" == false ]] || die "financial-era manifest is absent"
  rehearsal_evidence_json=$(read_rehearsal_evidence) ||
    die "financial-era rehearsal evidence was refused"
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$generation_artifact_sha256" ]] ||
    die "installed old binary differs from the verified #557 artifact"
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$generation_config_sha256" ]] ||
    die "installed old config differs from the verified #557 artifact"
  [[ "$(sha256_file "$SERVICE_ENV")" == "$generation_environment_sha256" ]] ||
    die "installed old environment differs from the verified #557 artifact"
  old_binary=$(file_identity_json "$SERVICE_BINARY")
  old_config=$(file_identity_json "$SERVICE_CONFIG")
  old_environment=$(file_identity_json "$SERVICE_ENV")
  target_binary_json=$(file_identity_json "$target_binary")
  target_config_json=$(file_identity_json "$target_config")
  target_environment_json=$(file_identity_json "$target_environment")
  initial=$(python3 -c 'import decimal,json,sys,time
(activation,generation,generation_commit,generation_bankroll,generation_source,generation_history,
 bankroll,revision,artifact,static,batch,members_path,
 paper,source,live,state,old_binary,old_config,old_env,target_binary,target_config,target_env,rehearsal)=sys.argv[1:]
amount=decimal.Decimal(bankroll)
atomic=amount*decimal.Decimal(1000000)
if atomic != atomic.to_integral_value(): raise SystemExit("bankroll is not exact")
members=json.load(open(members_path,encoding="utf-8"))
if not isinstance(members,list) or len(members)!=len(set(members)): raise SystemExit("membership must be a unique JSON array")
if not members: raise SystemExit("membership must not be empty")
value={
 "kind":"financial-era-v1","state":"prepared","activation_id":activation,"generation":generation,
 "generation_merge_commit":generation_commit,"generation_bankroll":generation_bankroll,
 "generation_source_v1_main":json.loads(generation_source),
 "generation_legacy_history":json.loads(generation_history),
 "fresh_bankroll":int(atomic),"target_revision":revision,"artifact_blake3":artifact,"static_config_hash":static,
 "ranking_batch_id":int(batch),"membership":members,
 "schema_version":3,"parser_version":1,"financial_semantic_version":1,
 "start_unix":int(time.time()),"paths":{"paper_log":paper,"source_log":source,"live_journal":live,"paper_state":state},
 "old_artifact_sha256":json.loads(old_binary)["sha256"],"target_artifact_sha256":json.loads(target_binary)["sha256"],
 "old_config_sha256":json.loads(old_config)["sha256"],"target_config_sha256":json.loads(target_config)["sha256"],
 "old_environment_sha256":json.loads(old_env)["sha256"],"target_environment_sha256":json.loads(target_env)["sha256"],
 "rehearsal_evidence":json.loads(rehearsal),"preparation":None}
print(json.dumps(value,sort_keys=True,separators=(",",":")))' \
    "$activation_id" "$generation" "$generation_merge_commit" "$generation_bankroll" \
    "$generation_source_v1_main" "$generation_legacy_history" \
    "$fresh_bankroll" "$target_revision" \
    "$artifact_blake3" "$static_config_hash" "$ranking_batch_id" "$membership_json" \
    "$paper_log" "$source_log" "$live_journal" "$paper_state" \
    "$old_binary" "$old_config" "$old_environment" "$target_binary_json" \
    "$target_config_json" "$target_environment_json" "$rehearsal_evidence_json") || die "construct financial-era manifest"
  atomic_manifest_json "$initial" prepared
fi

[[ "$(manifest_get kind)" == financial-era-v1 ]] || die "wrong financial-era manifest kind"
[[ "$(manifest_get activation_id)" == "$activation_id" ]] || die "#557 activation identity changed"
[[ "$(manifest_get generation)" == "$generation" ]] || die "#557 generation identity changed"
[[ "$(manifest_get generation_merge_commit)" == "$generation_merge_commit" ]] || die "#557 merge-commit identity changed"
[[ "$(manifest_get generation_bankroll)" == "$generation_bankroll" ]] || die "#557 bankroll identity changed"
python3 -c 'import json,sys
manifest=json.load(open(sys.argv[1],encoding="utf-8"))
if manifest.get("generation_source_v1_main") != json.loads(sys.argv[2]): raise SystemExit("#557 source evidence changed")
if manifest.get("generation_legacy_history") != json.loads(sys.argv[3]): raise SystemExit("#557 legacy-history evidence changed")' \
  "$MANIFEST" "$generation_source_v1_main" "$generation_legacy_history" || die "#557 source/history identity changed"
[[ "$(manifest_get old_artifact_sha256)" == "$generation_artifact_sha256" ]] || die "recorded old binary is not the verified #557 artifact"
[[ "$(manifest_get old_config_sha256)" == "$generation_config_sha256" ]] || die "recorded old config is not the verified #557 artifact"
[[ "$(manifest_get old_environment_sha256)" == "$generation_environment_sha256" ]] || die "recorded old environment is not the verified #557 artifact"
[[ "$(manifest_get target_revision)" == "$target_revision" ]] || die "reviewed target revision changed"
[[ "$(sha256_file "$target_binary")" == "$(manifest_get target_artifact_sha256)" ]] || die "reviewed target artifact changed"
[[ "$(sha256_file "$target_config")" == "$(manifest_get target_config_sha256)" ]] || die "reviewed target config changed"
[[ "$(sha256_file "$target_environment")" == "$(manifest_get target_environment_sha256)" ]] || die "reviewed target environment changed"
[[ "$paper_log" == "$(manifest_get paths.paper_log)" &&
   "$source_log" == "$(manifest_get paths.source_log)" &&
   "$live_journal" == "$(manifest_get paths.live_journal)" &&
   "$paper_state" == "$(manifest_get paths.paper_state)" ]] || die "financial-era durable paths changed"
[[ "$artifact_blake3" == "$(manifest_get artifact_blake3)" &&
   "$static_config_hash" == "$(manifest_get static_config_hash)" &&
   "$ranking_batch_id" == "$(manifest_get ranking_batch_id)" ]] ||
  die "financial-era evidence identity changed"
ranking_identity="batch:$ranking_batch_id"
python3 -c 'import decimal,json,sys
manifest,members,bankroll=sys.argv[1:]
value=json.load(open(manifest,encoding="utf-8"))
supplied=json.load(open(members,encoding="utf-8"))
if supplied != value["membership"]: raise SystemExit("membership identity changed")
atomic=decimal.Decimal(bankroll)*decimal.Decimal(1000000)
if atomic != decimal.Decimal(value["fresh_bankroll"]): raise SystemExit("fresh bankroll identity changed")' \
  "$MANIFEST" "$membership_json" "$fresh_bankroll" || die "financial-era membership or bankroll identity changed"

state=$(manifest_get state)
if [[ "$state" == prepared && "$rollback_before_start" == false ]]; then
  rehearsal_evidence_json=$(read_rehearsal_evidence) ||
    die "financial-era rehearsal evidence was refused"
  python3 -c 'import json,sys
manifest=json.load(open(sys.argv[1],encoding="utf-8"))
supplied=json.loads(sys.argv[2])
if manifest.get("rehearsal_evidence") != supplied:
    print("REHEARSAL_REFUSAL=prepared_evidence_unbound_or_changed",file=sys.stderr)
    raise SystemExit(1)' "$MANIFEST" "$rehearsal_evidence_json" ||
    die "financial-era prepared rehearsal binding was refused"
fi
complete_start=false
if manifest_complete_start; then
  complete_start=true
else
  start_state=$?
  [[ $start_state -eq 1 ]] || die "QualificationStarted state is unknown; activation cannot continue"
fi
if [[ "$rollback_before_start" == true ]]; then
  case "$state" in
    started|verified) die "rollback is forbidden at or after QualificationStarted" ;;
    rolled_back) echo "activation_id=$activation_id state=rolled_back"; exit 0 ;;
    prepared|guarded|rolling_back) ;;
    *) die "rollback-before-start is invalid from state $state" ;;
  esac
  if [[ "$complete_start" == true ]]; then
    die "a complete QualificationStarted forces roll-forward"
  fi
  if ! reconcile_remote_archive_receipt; then
    finish_unmutated_rollback
  fi
  verify_pre_start_restore_authority
  backup_path=$(manifest_get backup.path)
  [[ "$(sha256_file "$backup_path")" == "$(manifest_get backup.sha256)" ]] || die "local backup hash mismatch"
  old_start_inflight=false
  if manifest_flag old_service_start_intent && manifest_flag archive_restored &&
     manifest_flag local_restored && [[ "$(systemctl_active_state pe-service)" == true ]]; then
    old_start_inflight=true
    verify_guarded_log_prefixes || die "old service does not extend the guarded log prefixes"
  else
    verify_guarded_log_identities || die "guarded paper/source/live log identity changed"
  fi
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$(manifest_get old_artifact_sha256)" ]] || die "pre-Start service binary changed"
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(manifest_get old_config_sha256)" ]] || die "pre-Start service config changed"
  [[ "$(sha256_file "$SERVICE_ENV")" == "$(manifest_get old_environment_sha256)" ]] || die "pre-Start service environment changed"
  if [[ "$old_start_inflight" == true ]]; then
    [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]] || verify_installed_unit_owner
    manifest_patch_boundary old-service-started '{"old_service_started":true}'
    manifest_advance rolled_back
    echo "activation_id=$activation_id state=rolled_back"
    exit 0
  fi
  if [[ "$(systemctl_active_state pe-service)" == true ]]; then
    "${SERVICE_MUTATE[@]}" stop pe-service
    manifest_patch_boundary rollback-service-stopped '{"rollback_service_stopped":true}'
  fi
  [[ "$(systemctl_active_state pe-service)" == false ]] || die "pe-service is not inert before rollback"
  [[ "$state" == rolling_back ]] || manifest_advance rolling_back
  if ! manifest_flag archive_restored; then
    [[ "$(systemctl_active_state pe-service)" == false ]] || die "pe-service is not inert before archive restore"
    if manifest_flag archive_restore_intent && activation_archive_matches_live "$activation_id"; then
      :
    else
      manifest_patch_boundary rollback-archive-restore-intent '{"archive_restore_intent":true}'
      psql_service_db -v ON_ERROR_STOP=1 -v activation_id="$activation_id" \
        -f "$REPO_ROOT/scripts/paper_reset/restore_paper_state.sql"
    fi
    restored_census=$(remote_paper_census)
    python3 -c 'import json,sys
raise SystemExit(0 if json.loads(sys.argv[1]) == json.loads(sys.argv[2]) else 1)' \
      "$restored_census" "$(manifest_get remote_census)" ||
      die "restored remote census differs from the guarded census"
    manifest_patch_boundary archive-restored '{"archive_restored":true}'
  fi
  if ! manifest_flag rollback_wallet_live_stats_refreshed; then
    manifest_patch_boundary rollback-wallet-live-stats-refresh-intent \
      '{"rollback_wallet_live_stats_refresh_intent":true}'
    psql_service_db -v ON_ERROR_STOP=1 -c \
      'refresh materialized view concurrently public.wallet_live_stats_mv;'
    manifest_patch_boundary rollback-wallet-live-stats-refreshed \
      '{"rollback_wallet_live_stats_refreshed":true}'
  fi
  if ! manifest_flag local_restored || manifest_flag qualification_start_intent; then
    [[ "$(systemctl_active_state pe-service)" == false ]] || die "pe-service is not inert before local restore"
    current_local_sha=$(sha256_file "$paper_state")
    if manifest_flag qualification_start_intent ||
       [[ "$current_local_sha" != "$(manifest_get guarded_paper_state_sha256)" ]]; then
      # An inactive unit with an old-start intent may have run and stopped again. Only the
      # untouched restored image permits a retry; never erase possible post-resumption writes.
      if manifest_flag old_service_start_intent; then
        [[ "$current_local_sha" == "$(manifest_get backup.sha256)" &&
           ! -e "$paper_state-wal" && ! -e "$paper_state-shm" ]] ||
          die "old-service resumption is ambiguous; refusing local restore"
      fi
      if [[ "$current_local_sha" != "$(manifest_get guarded_paper_state_sha256)" ]] &&
         ! manifest_flag local_mutation_observed; then
        manifest_patch_boundary rollback-local-mutation-observed \
          "$(python3 -c 'import json,sys; print(json.dumps({"local_mutation_observed":True,"mutated_local_sha256":sys.argv[1]},sort_keys=True,separators=(",",":")))' "$current_local_sha")"
      fi
      # Start may commit only into WAL, leaving either main-file hash unchanged. A durable
      # restore intent plus backup equality AND absent sidecars certifies helper completion.
      if { manifest_flag qualification_start_intent && ! manifest_flag local_restore_intent; } ||
         [[ "$current_local_sha" != "$(manifest_get backup.sha256)" ]] ||
         { [[ -e "$paper_state-wal" || -e "$paper_state-shm" ]] &&
           { manifest_flag qualification_start_intent || manifest_flag local_restore_intent; }; }; then
        manifest_patch_boundary rollback-local-restore-intent \
          '{"local_restore_intent":true,"local_restore_skipped":false}'
        restore_sqlite_backup "$backup_path" "$paper_state"
      else
        manifest_flag local_restore_intent ||
          die "local state equals the backup without a durable restore intent"
        if manifest_flag local_restore_skipped; then
          manifest_patch_boundary rollback-local-restore-intent \
            '{"local_restore_intent":true,"local_restore_skipped":false}'
        fi
      fi
    else
      manifest_patch_boundary rollback-local-restore-skipped '{"local_restore_skipped":true}'
    fi
    if manifest_flag local_restore_skipped; then
      [[ "$(sha256_file "$paper_state")" == "$(manifest_get guarded_paper_state_sha256)" ]] ||
        die "unmutated local paper-state differs from its guarded identity"
    else
      [[ "$(sha256_file "$paper_state")" == "$(manifest_get backup.sha256)" ]] ||
        die "restored local paper-state bytes differ from the complete backup"
      [[ ! -e "$paper_state-wal" && ! -e "$paper_state-shm" ]] ||
        die "restored local paper-state has surviving SQLite sidecars"
    fi
    verify_guarded_log_identities || die "paper/source/live log identity changed during rollback"
    manifest_patch_boundary local-restored '{"local_restored":true}'
  fi
  if [[ "$(manifest_get service_was_active)" == true ]] && ! manifest_flag old_service_started; then
    [[ "$(systemctl_active_state pe-service)" == false ]] || die "pe-service is not inert before old-service start"
    manifest_patch_boundary rollback-old-service-start-intent '{"old_service_start_intent":true}'
    "${SERVICE_MUTATE[@]}" start pe-service
    [[ "$(systemctl_active_state pe-service)" == true ]] || die "old pe-service did not start"
    [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]] || verify_installed_unit_owner
    manifest_patch_boundary old-service-started '{"old_service_started":true}'
  elif [[ "$(manifest_get service_was_active)" == false ]]; then
    [[ "$(systemctl_active_state pe-service)" == false ]] || die "originally inactive service unexpectedly started"
    manifest_patch_boundary old-service-start-skipped '{"old_service_start_skipped":true}'
  fi
  manifest_advance rolled_back
  echo "activation_id=$activation_id state=rolled_back"
  exit 0
fi

# A manifest recorded with an empty membership (possible before #567) must never go forward:
# the first ordinary boot after Start refuses an empty watchlist, and Start is irreversible.
# Rollback exited above; a complete durable Start must still roll forward.
if [[ ( "$state" == prepared || "$state" == guarded ) && "$complete_start" == false ]]; then
  python3 -c 'import json,sys
if not json.load(open(sys.argv[1],encoding="utf-8"))["membership"]: raise SystemExit("membership must not be empty")' \
    "$MANIFEST" || die "financial-era manifest membership is empty; refusing to go forward"
fi

case "$state" in
  prepared)
    if ! python3 -c 'import json,sys; v=json.load(open(sys.argv[1])); raise SystemExit(0 if v.get("stop_invoked") else 1)' "$MANIFEST"; then
      current_active=$(systemctl_active_state pe-service)
      verify_persisted_financial_guard unit_stop_policy "$current_active"
      if ! manifest_flag service_stop_intent; then
        was_active=$current_active
        manifest_patch_boundary service-stop-intent "{\"service_stop_intent\":true,\"service_was_active\":$was_active}"
      else
        was_active=$(manifest_get service_was_active)
      fi
      if [[ "$current_active" == true ]]; then
        "${SERVICE_MUTATE[@]}" stop pe-service
      fi
      [[ "$(systemctl_active_state pe-service)" == false ]] || die "pe-service did not become inert"
      manifest_patch_boundary service-stopped "{\"stop_invoked\":true,\"service_was_active\":$was_active}"
    fi
    [[ "$(systemctl_active_state pe-service)" == false ]] || die "pe-service is not inert"
    verify_legacy_service_contract
    manifest_patch_boundary legacy-contract-verified '{"legacy_contract_verified":true}'
    backup_path="$SERVICE_ROOT/financial-era-$activation_id-paper-state.db"
    complete_sqlite_backup "$paper_state" "$backup_path"
    backup_sha=$(sha256_file "$backup_path")
    census=$(remote_paper_census)
    guarded_paper_state_sha=$(sha256_file "$paper_state")
    patch=$(python3 -c 'import json,os,sys
backup,sha,guarded_state_sha,census,paper,source,live=sys.argv[1:]
logs={name:{"path":path,"sha256":__import__("hashlib").sha256(open(path,"rb").read()).hexdigest(),"bytes":os.path.getsize(path)} for name,path in (("paper",paper),("source",source),("live",live))}
print(json.dumps({"backup":{"path":backup,"sha256":sha},"guarded_paper_state_sha256":guarded_state_sha,"remote_census":json.loads(census),"guarded_logs":logs},sort_keys=True,separators=(",",":")))' \
      "$backup_path" "$backup_sha" "$guarded_paper_state_sha" "$census" "$paper_log" "$source_log" "$live_journal")
    if [[ "$(manifest_get preparation)" == "" ]]; then
      export_financial_config_rows
      preparation=$(run_target_offline --financial-era=prepare \
        --activation-manifest="$MANIFEST" \
        --financial-config-rows="$financial_config_rows_file") ||
        die "read-only financial-era preparation failed"
      manifest_patch_boundary preparation "$(python3 -c 'import json,sys; print(json.dumps({"preparation":json.loads(sys.argv[1])},sort_keys=True,separators=(",",":")))' "$preparation")"
    fi
    manifest_advance guarded "$patch"
    state=guarded
    ;;
  guarded|started|verified) ;;
  rolled_back) die "rolled-back financial era requires a new activation identity" ;;
  *) die "unsupported financial-era state $state" ;;
esac

if [[ "$state" == guarded ]]; then
  service_active=$(systemctl_active_state pe-service)
  if [[ "$complete_start" == false ]]; then
    # Issue #586: guarded entry reuses only persisted bindings; it never reopens rehearsal files.
    verify_persisted_financial_guard harness_bundle
    verify_persisted_financial_guard unit_stop_policy "$service_active"
    [[ "$service_active" == false ]] || die "pre-Start guarded activation requires an inert service"
    verify_legacy_service_contract
    if ! python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); raise SystemExit(0 if value.get("remote_archive_completed") is True else 1)' "$MANIFEST"; then
      manifest_patch_boundary remote-archive-intent '{"remote_archive_intent":true}'
      psql_service_db -v ON_ERROR_STOP=1 -v activation_id="$activation_id" \
        -v bankroll="$fresh_bankroll" -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
      manifest_patch_boundary remote-archived '{"remote_archive_completed":true}'
    fi
    export_financial_config_rows
    manifest_patch_boundary qualification-start-intent '{"qualification_start_intent":true}'
    start_receipt=$(run_target_offline --financial-era=start \
      --activation-manifest="$MANIFEST" \
      --financial-config-rows="$financial_config_rows_file") || die "offline financial-era Start failed"
    manifest_patch_boundary qualification-started "$(python3 -c 'import json,sys; print(json.dumps({"start_receipt":json.loads(sys.argv[1])},sort_keys=True,separators=(",",":")))' "$start_receipt")"
  else
    start_receipt=$(python3 -c 'import json,sys; print(json.dumps(json.loads(sys.argv[1])["receipt"],sort_keys=True,separators=(",",":")))' "$COMPLETE_START_OUTPUT") || die "complete Start receipt is invalid"
    if [[ "$service_active" == false ]]; then
      # Catch resume-time policy drift before any remaining roll-forward receipt is written.
      verify_persisted_financial_guard unit_stop_policy "$service_active"
    fi
    if [[ "$service_active" == true ]]; then
      [[ "$(sha256_file "$SERVICE_BINARY")" == "$(manifest_get target_artifact_sha256)" ]] || die "running post-Start binary is not the reviewed target"
      [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(manifest_get target_config_sha256)" ]] || die "running post-Start config is not the reviewed target"
      [[ "$(sha256_file "$SERVICE_ENV")" == "$(manifest_get target_environment_sha256)" ]] || die "running post-Start environment is not the reviewed target"
      [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]] || verify_installed_unit_owner
    fi
  fi
  [[ -f "$REPO_ROOT/scripts/migrate_service_config_545.sql" ]] || die "post-Start service-config migration is absent"
  start_seq=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["sequence"])' "$start_receipt") ||
    die "Start sequence is invalid"
  start_hash=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["this_hash"])' "$start_receipt") ||
    die "Start hash is invalid"
  if ! manifest_flag authority_schema_installed; then
    manifest_patch_boundary authority-schema-intent '{"authority_schema_intent":true}'
    psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_paper_state_schema.sql"
    manifest_patch_boundary authority-schema-installed '{"authority_schema_installed":true}'
  fi
  if ! manifest_flag live_schema_installed; then
    manifest_patch_boundary live-schema-intent '{"live_schema_intent":true}'
    psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_multi_account_live_schema.sql"
    manifest_patch_boundary live-schema-installed '{"live_schema_installed":true}'
  fi
  authority_start=$(psql_service_db -v ON_ERROR_STOP=1 -Atc \
    "select seed_financial_start($start_seq,'$start_hash');
     select json_build_object('bankroll',(select bankroll_str from paper_bankroll where id=0),
       'start_seq',(select start_seq from paper_bankroll where id=0),
       'start_hash',(select start_hash from paper_bankroll where id=0),
       'last_prepared_seq',(select last_prepared_seq from paper_bankroll where id=0))::text;") ||
    die "seed and read back the authority Start identity"
  python3 -c 'import decimal,json,sys
lines=[line for line in sys.argv[1].splitlines() if line]
if len(lines) != 2: raise SystemExit("authority Start proof must contain seed and read-back rows")
seed,row=map(json.loads,lines)
if seed.get("outcome") not in {"applied","existing"}: raise SystemExit("authority Start seed conflicted")
sequence=int(sys.argv[2]); digest=sys.argv[3]
if seed.get("start_seq") != sequence or seed.get("start_hash") != digest: raise SystemExit("seed result identity differs")
if row.get("start_seq") != sequence or row.get("start_hash") != digest or row.get("last_prepared_seq") is not None: raise SystemExit("authority Start read-back differs")
if decimal.Decimal(row.get("bankroll")) != decimal.Decimal(sys.argv[4]): raise SystemExit("authority bankroll read-back differs")' \
    "$authority_start" "$start_seq" "$start_hash" "$fresh_bankroll" ||
    die "authority Start seed/read-back proof failed"
  manifest_patch_boundary authority-start-seeded \
    "$(python3 -c 'import json,sys; print(json.dumps({"authority_start_seeded":True,"authority_start_proof":[json.loads(line) for line in sys.argv[1].splitlines() if line]},sort_keys=True,separators=(",",":")))' "$authority_start")"
  if ! manifest_flag financial_config_migrated; then
    manifest_patch_boundary financial-config-migration-intent '{"financial_config_migration_intent":true}'
    psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/migrate_service_config_545.sql"
    manifest_patch_boundary financial-config-migrated '{"financial_config_migrated":true}'
  fi
  if ! manifest_flag wallet_live_stats_refreshed; then
    manifest_patch_boundary wallet-live-stats-refresh-intent \
      '{"wallet_live_stats_refresh_intent":true}'
    psql_service_db -v ON_ERROR_STOP=1 -c \
      'refresh materialized view concurrently public.wallet_live_stats_mv;'
    manifest_patch_boundary wallet-live-stats-refreshed \
      '{"wallet_live_stats_refreshed":true}'
  fi
  if ! manifest_flag target_config_adopted; then
    manifest_patch_boundary target-config-adopt-intent '{"target_config_adopt_intent":true}'
    atomic_adopt "$target_config" "$SERVICE_CONFIG" 0644 financial-config-adopted \
      "$(manifest_get target_config_sha256)"
    manifest_patch_boundary target-config-adopted '{"target_config_adopted":true}'
  fi
  if ! manifest_flag target_environment_adopted; then
    manifest_patch_boundary target-environment-adopt-intent '{"target_environment_adopt_intent":true}'
    atomic_adopt "$target_environment" "$SERVICE_ENV" 0600 financial-environment-adopted \
      "$(manifest_get target_environment_sha256)"
    manifest_patch_boundary target-environment-adopted '{"target_environment_adopted":true}'
  fi
  if ! manifest_flag target_binary_adopted; then
    manifest_patch_boundary target-binary-adopt-intent '{"target_binary_adopt_intent":true}'
    atomic_adopt "$target_binary" "$SERVICE_BINARY" 0755 financial-binary-adopted \
      "$(manifest_get target_artifact_sha256)"
    manifest_patch_boundary target-binary-adopted '{"target_binary_adopted":true}'
  fi
  service_active=$(systemctl_active_state pe-service)
  if [[ "$service_active" == false ]]; then
    verify_persisted_financial_guard unit_stop_policy "$service_active"
    manifest_flag service_start_intent ||
      manifest_patch_boundary service-start-intent '{"service_start_intent":true}'
    "${SERVICE_MUTATE[@]}" start pe-service
  else
    [[ "$service_active" == true ]] || die "pe-service has an invalid active state"
    [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]] || verify_installed_unit_owner
  fi
  manifest_flag service_started ||
    manifest_patch_boundary service-started '{"service_started":true}'
  started_unix=$(date +%s)
  manifest_advance started "$(python3 -c 'import json,sys; print(json.dumps({"start_receipt":json.loads(sys.argv[1]),"started_unix":int(sys.argv[2])},sort_keys=True,separators=(",",":")))' "$start_receipt" "$started_unix")"
  echo "activation_id=$activation_id state=started"
  exit 0
fi

if [[ "$state" == started ]]; then
  [[ "$complete_start" == true ]] || die "verified state requires the manifest-bound QualificationStarted"
  [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]] || verify_installed_unit_owner
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$(manifest_get target_artifact_sha256)" ]] || die "installed target artifact hash mismatch"
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(manifest_get target_config_sha256)" ]] || die "installed target config hash mismatch"
  [[ "$(sha256_file "$SERVICE_ENV")" == "$(manifest_get target_environment_sha256)" ]] || die "installed target environment hash mismatch"
  [[ -f "$generation/status.json" ]] || die "fresh status proof is not available yet"
  hot_config_hash=$(manifest_get preparation.start.hot_config_hash)
  [[ "$hot_config_hash" =~ ^[0-9a-f]{64}$ ]] || die "prepared hot-config identity is invalid"
  membership_proofs_hash=$(manifest_get preparation.start.membership_proofs_hash)
  [[ "$membership_proofs_hash" =~ ^[0-9a-f]{64}$ ]] ||
    die "prepared membership-proofs identity is invalid"
  verify_guarded_log_prefixes || die "paper/source/live prefixes do not extend their guarded identities"
  python3 -c 'import decimal,json,os,sys
path,started,revision,hot,bankroll,membership_count,ranking_identity=sys.argv[1:]
started=int(started); membership_count=int(membership_count)
value=json.load(open(path,encoding="utf-8"))
if os.stat(path).st_mtime < started: raise SystemExit("status predates financial-era start")
if value.get("revision") != revision: raise SystemExit("status revision differs from the reviewed target")
if value.get("status_error") is not None: raise SystemExit("status carries a financial sampling error")
if value.get("mode") != "paper" or value.get("authoritative") is not True: raise SystemExit("status is not authoritative paper mode")
if decimal.Decimal(str(value.get("bankroll"))) != decimal.Decimal(bankroll): raise SystemExit("status bankroll differs from the fresh baseline")
if any(value.get(key) != 0 for key in ("open_positions","fills_total","settled_total")): raise SystemExit("status financial state is not freshly reset")
runtime=value.get("runtime_config") or {}
if runtime.get("applied_hash") != hot or runtime.get("rejected") is not None: raise SystemExit("status hot configuration identity differs")
if value.get("watchlist_size") != membership_count or value.get("watchlist_target_size") != membership_count: raise SystemExit("status membership count differs")
if membership_count and not isinstance(value.get("oldest_anchor_age_secs"),int): raise SystemExit("status does not prove installed membership anchors")
projection=value.get("watchlist_projection") or {}
applied=projection.get("applied") or {}
if applied.get("token") != ranking_identity or applied.get("count") != membership_count or projection.get("pending") is not None or projection.get("last_error") is not None: raise SystemExit("status membership projection is not exact")
tasks=value.get("tasks") or []
required={"activity_ingest","public_activity_poll","orchestrator","resolution_poller","watchlist_refresh","status_writer","http_server"}
running={row.get("name") for row in tasks if row.get("state") == "running"}
if not required <= running or any(row.get("state") != "running" for row in tasks if row.get("class") == "critical"): raise SystemExit("the producer/critical owner set is not running")
source=value.get("source_health")
if not isinstance(source,dict): raise SystemExit("source owner health is absent")
if source.get("poll_error_streak") != 0 or source.get("copy_admission_blocked") is not False or source.get("ws_sink_poisoned") is not False: raise SystemExit("source owner is unhealthy")
age=source.get("poll_last_round_age_secs")
uptime=value.get("uptime_secs")
if age is None or not isinstance(age,int) or age < 0 or not isinstance(uptime,int) or age > uptime: raise SystemExit("source owner has no completed post-boot poll")
live=value.get("live")
if not isinstance(live,dict) or live.get("stale") is not False: raise SystemExit("live account posture is absent or stale")
if live.get("pending_dispatch_seeds") != 0 or live.get("ready_dispatch_seeds") != 0: raise SystemExit("live dispatch work remains")
accounts=live.get("accounts")
if not isinstance(accounts,list): raise SystemExit("live account inventory is absent")
identities=[]
for account in accounts:
    identities.append(account.get("account_id"))
    if account.get("requested_live_mode") != "off" or account.get("effective_live_mode") != "off" or account.get("armed") is not False:
        raise SystemExit("a live account is not off and unarmed")
if None in identities or len(identities) != len(set(identities)): raise SystemExit("live account inventory is not uniquely identified")' \
    "$generation/status.json" "$(manifest_get started_unix)" "$target_revision" \
    "$hot_config_hash" "$fresh_bankroll" "$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["membership"]))' "$MANIFEST")" \
    "$ranking_identity" ||
    die "first fresh health proof is incomplete"
  readiness_url=$(installed_readiness_url) || die "installed readiness endpoint is invalid"
  financial_readiness_response=$(mktemp)
  readiness_env=("PATH=$PATH" "LANG=${LANG:-C.UTF-8}")
  if [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]]; then
    readiness_env+=("PE_ACTIVATION_TEST_ROOT=$PE_ACTIVATION_TEST_ROOT")
  fi
  env -i "${readiness_env[@]}" curl --disable --silent --show-error --fail-with-body --noproxy '*' \
    --output "$financial_readiness_response" "$readiness_url" ||
    die "installed readiness endpoint did not return HTTP success"
  python3 -c 'import json,sys
with open(sys.argv[1],encoding="utf-8") as source: value=json.load(source)
if not isinstance(value,dict) or value.get("ready") is not True or value.get("issues",[]) != []:
    raise SystemExit("installed invocation is not ready")' "$financial_readiness_response" ||
    die "installed readiness proof is incomplete"
  verified_readiness_sha256=$(sha256_file "$financial_readiness_response")
  python3 -c 'import decimal,json,sqlite3,sys
path,start_seq,start_hash,bankroll=sys.argv[1:]
db=sqlite3.connect("file:"+path+"?mode=ro",uri=True)
def meta(key):
    row=db.execute("select value from meta where key=?",(key,)).fetchone()
    if row is None: return None
    value=row[0]
    return value.decode() if isinstance(value,bytes) else str(value)
if meta("financial_start_seq") != start_seq or meta("financial_start_hash") != start_hash: raise SystemExit("local Start identity differs")
if meta("financial_last_prepared_seq") is not None: raise SystemExit("local financial version is not fresh")
for table in ("fills","positions","settled_markets","fill_market_snapshots"):
    if db.execute("select count(*) from "+table).fetchone()[0] != 0: raise SystemExit("local financial table is not empty: "+table)
rows=db.execute("select bankroll_str from bankroll").fetchall()
if len(rows) != 1 or decimal.Decimal(rows[0][0]) != decimal.Decimal(bankroll): raise SystemExit("local bankroll differs")' \
    "$paper_state" "$(manifest_get start_receipt.sequence)" "$(manifest_get start_receipt.this_hash)" "$fresh_bankroll" ||
    die "local financial reset/Start proof is incomplete"
  remote_verified=$(psql_service_db -v ON_ERROR_STOP=1 -Atc \
    "select json_build_object(
       'paper_fills',(select count(*) from paper_fills),
       'settled_markets',(select count(*) from settled_markets),
       'paper_positions',(select count(*) from paper_positions),
       'fill_market_snapshots',(select count(*) from fill_market_snapshots),
       'bankroll_count',(select count(*) from paper_bankroll),
       'bankroll',(select bankroll_str from paper_bankroll where id=0),
       'start_seq',(select start_seq from paper_bankroll where id=0),
       'start_hash',(select start_hash from paper_bankroll where id=0),
       'last_prepared_seq',(select last_prepared_seq from paper_bankroll where id=0),
       'ranking_batch_id',(select max(batch_id) from ranking_batches),
       'membership',coalesce((select json_agg(wallet_hex order by wallet_hex) from service_watchlist),'[]'::json)
     )::text;") || die "read remote verified-state proof"
  python3 -c 'import decimal,json,sys
actual=json.loads(sys.argv[1]); manifest=json.load(open(sys.argv[2],encoding="utf-8"))
if any(actual[key] != 0 for key in ("paper_fills","settled_markets","paper_positions","fill_market_snapshots")): raise SystemExit("remote financial tables are not empty")
if actual["bankroll_count"] != 1 or decimal.Decimal(actual["bankroll"]) != decimal.Decimal(sys.argv[3]): raise SystemExit("remote bankroll differs")
receipt=manifest["start_receipt"]
if actual["start_seq"] != receipt["sequence"] or actual["start_hash"] != receipt["this_hash"] or actual["last_prepared_seq"] is not None: raise SystemExit("remote Start/version differs")
if actual["ranking_batch_id"] != manifest["ranking_batch_id"]: raise SystemExit("remote ranking batch differs")
if sorted(actual["membership"]) != sorted(manifest["membership"]): raise SystemExit("remote membership differs")' \
    "$remote_verified" "$MANIFEST" "$fresh_bankroll" || die "remote financial/ranking/membership proof is incomplete"
  manifest_advance verified \
    "$(python3 -c 'import json,sys
print(json.dumps({"verified_state_assertions":True,"verified_start_reset":True,"verified_source_replay_continuity":True,"verified_ranking_membership":True,"verified_producers_projection":True,"verified_accounts_off_unarmed":True,"verified_readiness_sha256":sys.argv[1]},sort_keys=True,separators=(",",":")))' "$verified_readiness_sha256")"
fi

echo "activation_id=$activation_id state=$(manifest_get state)"
