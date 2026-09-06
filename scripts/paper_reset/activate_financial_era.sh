#!/usr/bin/env bash
# Locked, resumable schema-two financial-era activation (#545).

set -euo pipefail

# shellcheck source=../deploy/generation_common.sh
source "$(cd "$(dirname "$0")/../deploy" && pwd)/generation_common.sh"

IDENTITY_MANIFEST=$MANIFEST
MANIFEST="$DEPLOY_HOME/pe-financial-era.json"

usage() {
  echo "usage: $0 --target-binary PATH --target-config PATH --target-environment PATH \
--paper-log PATH --source-log PATH --live-journal PATH --paper-state PATH \
--fresh-bankroll DECIMAL --hot-config-hash HASH \
--ranking-batch-id ID --policy-hash HASH --membership-json PATH \
--membership-proofs-hash HASH [--rollback-before-start] [--simulate-crash-after BOUNDARY]" >&2
  exit 2
}

target_binary= target_config= target_environment=
paper_log= source_log= live_journal= paper_state=
fresh_bankroll= artifact_blake3= hot_config_hash= ranking_batch_id=
target_revision=
policy_hash= membership_json= membership_proofs_hash=
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
    --hot-config-hash) [[ $# -ge 2 ]] || usage; hot_config_hash=$2; shift 2 ;;
    --ranking-batch-id) [[ $# -ge 2 ]] || usage; ranking_batch_id=$2; shift 2 ;;
    --policy-hash) [[ $# -ge 2 ]] || usage; policy_hash=$2; shift 2 ;;
    --membership-json) [[ $# -ge 2 ]] || usage; membership_json=$2; shift 2 ;;
    --membership-proofs-hash) [[ $# -ge 2 ]] || usage; membership_proofs_hash=$2; shift 2 ;;
    --rollback-before-start) rollback_before_start=true; shift ;;
    --simulate-crash-after) [[ $# -ge 2 ]] || usage; SIMULATE_CRASH_AFTER=$2; shift 2 ;;
    *) usage ;;
  esac
done

for value in target_binary target_config target_environment paper_log source_log live_journal \
  paper_state fresh_bankroll hot_config_hash ranking_batch_id policy_hash \
  membership_json membership_proofs_hash; do
  [[ -n "${!value}" ]] || usage
done
[[ "$fresh_bankroll" =~ ^[0-9]+([.][0-9]{1,6})?$ ]] || die "fresh bankroll must be an exact non-negative six-decimal value"
[[ "$hot_config_hash" =~ ^[0-9a-f]{64}$ ]] || die "invalid hot-config hash"
[[ "$ranking_batch_id" =~ ^[0-9]+$ ]] || die "invalid ranking batch id"
[[ -f "$target_binary" && -f "$target_config" && -f "$target_environment" ]] ||
  die "one or more reviewed target artifacts are absent"
[[ -f "$membership_json" ]] || die "membership JSON is absent"

for command in python3 sha256sum flock systemctl psql; do
  command -v "$command" >/dev/null || die "$command not installed"
done
: "${SUPABASE_DB_URL:?SUPABASE_DB_URL must be exported for financial-era activation}"

acquire_deploy_lock
[[ -f "$IDENTITY_MANIFEST" ]] || die "#557 activation identity is absent: $IDENTITY_MANIFEST"
identity=$(python3 -c 'import json,sys
value=json.load(open(sys.argv[1], encoding="utf-8"))
if value.get("state") != "verified": raise SystemExit("#557 activation is not verified")
print(value["activation_id"], value["generation"])' "$IDENTITY_MANIFEST") || die "invalid #557 activation identity"
read -r activation_id generation <<< "$identity"
[[ "$activation_id" =~ ^[A-Za-z0-9._-]+$ && -n "$generation" ]] || die "invalid #557 identity"

staged_identity_output=$("$target_binary" --verify-staged-identity) ||
  die "staged binary could not derive its own identity"
read -r target_revision artifact_blake3 < <(python3 -c 'import re,sys
match=re.fullmatch(r"prediction-edge revision=([0-9a-f]{40}) artifact_blake3=([0-9a-f]{64})\n?",sys.stdin.read())
if match is None: raise SystemExit(1)
print(*match.groups())' <<< "$staged_identity_output") || die "staged binary identity output is invalid"
"$target_binary" --verify-staged-identity "$target_revision" "$artifact_blake3" >/dev/null ||
  die "staged binary identity self-verification failed"
static_config_hash=$(python3 -c 'import hashlib,json,sys
config_hash,environment_hash=sys.argv[1:]
payload=json.dumps({"config_sha256":config_hash,"environment_sha256":environment_hash},sort_keys=True,separators=(",",":")).encode()
print(hashlib.sha256(b"prediction-edge/effective-static-config-v1\0"+payload).hexdigest())' \
  "$(sha256_file "$target_config")" "$(sha256_file "$target_environment")") ||
  die "derive effective staged configuration identity"

file_identity_json() {
  python3 -c 'import json,sys
print(json.dumps({"path":sys.argv[1],"sha256":sys.argv[2]},sort_keys=True,separators=(",",":")))' \
    "$1" "$(sha256_file "$1")"
}

run_target_offline() {
  local -a preserved=("PATH=$PATH")
  [[ -z "${HOME:-}" ]] || preserved+=("HOME=$HOME")
  if [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]]; then
    preserved+=("PE_ACTIVATION_TESTING=1" "PE_ACTIVATION_TEST_ROOT=$PE_ACTIVATION_TEST_ROOT")
  fi
  env -i "${preserved[@]}" /bin/bash -c '
set -a
# shellcheck disable=SC1090
source "$1"
set +a
shift
exec "$@"
' bash "$target_environment" "$target_binary" "$target_config" "$@"
}

manifest_complete_start() {
  local output
  output=$(run_target_offline --financial-era=rollback-check \
    --activation-manifest="$MANIFEST") || return 2
  COMPLETE_START_OUTPUT=$output
  python3 -c 'import json,sys
try: value=json.loads(sys.argv[1])
except Exception: raise SystemExit(2)
if value.get("complete_start") is True: raise SystemExit(0)
if value.get("complete_start") is False: raise SystemExit(1)
raise SystemExit(2)' "$output"
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
  old_binary=$(file_identity_json "$SERVICE_BINARY")
  old_config=$(file_identity_json "$SERVICE_CONFIG")
  old_environment=$(file_identity_json "$SERVICE_ENV")
  target_binary_json=$(file_identity_json "$target_binary")
  target_config_json=$(file_identity_json "$target_config")
  target_environment_json=$(file_identity_json "$target_environment")
  initial=$(python3 -c 'import decimal,json,sys,time
(manifest,activation,generation,bankroll,revision,artifact,static,hot,batch,policy,members_path,proofs,
 paper,source,live,state,old_binary,old_config,old_env,target_binary,target_config,target_env)=sys.argv[1:]
amount=decimal.Decimal(bankroll)
atomic=amount*decimal.Decimal(1000000)
if atomic != atomic.to_integral_value(): raise SystemExit("bankroll is not exact")
members=json.load(open(members_path,encoding="utf-8"))
if not isinstance(members,list) or len(members)!=len(set(members)): raise SystemExit("membership must be a unique JSON array")
value={
 "kind":"financial-era-v1","state":"prepared","activation_id":activation,"generation":generation,
 "fresh_bankroll":int(atomic),"target_revision":revision,"artifact_blake3":artifact,"static_config_hash":static,
 "hot_config_hash":hot,"ranking_batch_id":int(batch),"policy_hash":policy,"membership":members,
 "membership_proofs_hash":proofs,"schema_version":3,"parser_version":1,"financial_semantic_version":1,
 "start_unix":int(time.time()),"paths":{"paper_log":paper,"source_log":source,"live_journal":live,"paper_state":state},
 "old_artifact_sha256":json.loads(old_binary)["sha256"],"target_artifact_sha256":json.loads(target_binary)["sha256"],
 "old_config_sha256":json.loads(old_config)["sha256"],"target_config_sha256":json.loads(target_config)["sha256"],
 "old_environment_sha256":json.loads(old_env)["sha256"],"target_environment_sha256":json.loads(target_env)["sha256"],
 "expected_hot_config_names_hash":hot,"ranking_identity":"batch:"+batch,
 "fresh_bankroll_identity":format(amount,"f"),"preparation":None}
print(json.dumps(value,sort_keys=True,separators=(",",":")))' \
    "$MANIFEST" "$activation_id" "$generation" "$fresh_bankroll" "$target_revision" \
    "$artifact_blake3" "$static_config_hash" "$hot_config_hash" "$ranking_batch_id" "$policy_hash" "$membership_json" \
    "$membership_proofs_hash" "$paper_log" "$source_log" "$live_journal" "$paper_state" \
    "$old_binary" "$old_config" "$old_environment" "$target_binary_json" \
    "$target_config_json" "$target_environment_json") || die "construct financial-era manifest"
  atomic_manifest_json "$initial" prepared
fi

[[ "$(manifest_get kind)" == financial-era-v1 ]] || die "wrong financial-era manifest kind"
[[ "$(manifest_get activation_id)" == "$activation_id" ]] || die "#557 activation identity changed"
[[ "$(manifest_get generation)" == "$generation" ]] || die "#557 generation identity changed"
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
   "$hot_config_hash" == "$(manifest_get hot_config_hash)" &&
   "$ranking_batch_id" == "$(manifest_get ranking_batch_id)" &&
   "$policy_hash" == "$(manifest_get policy_hash)" &&
   "$membership_proofs_hash" == "$(manifest_get membership_proofs_hash)" ]] ||
  die "financial-era evidence identity changed"
python3 -c 'import decimal,json,sys
manifest,members,bankroll=sys.argv[1:]
value=json.load(open(manifest,encoding="utf-8"))
supplied=json.load(open(members,encoding="utf-8"))
if supplied != value["membership"]: raise SystemExit("membership identity changed")
atomic=decimal.Decimal(bankroll)*decimal.Decimal(1000000)
if atomic != decimal.Decimal(value["fresh_bankroll"]): raise SystemExit("fresh bankroll identity changed")' \
  "$MANIFEST" "$membership_json" "$fresh_bankroll" || die "financial-era membership or bankroll identity changed"

state=$(manifest_get state)
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
  if ! manifest_flag local_restored; then
    [[ "$(systemctl_active_state pe-service)" == false ]] || die "pe-service is not inert before local restore"
    current_local_sha=$(sha256_file "$paper_state")
    if [[ "$current_local_sha" != "$(manifest_get guarded_paper_state_sha256)" ]]; then
      if ! manifest_flag local_mutation_observed; then
        manifest_patch_boundary rollback-local-mutation-observed \
          "$(python3 -c 'import json,sys; print(json.dumps({"local_mutation_observed":True,"mutated_local_sha256":sys.argv[1]},sort_keys=True,separators=(",",":")))' "$current_local_sha")"
      fi
      if [[ "$current_local_sha" != "$(manifest_get backup.sha256)" ]]; then
        manifest_flag local_restore_intent ||
          manifest_patch_boundary rollback-local-restore-intent '{"local_restore_intent":true}'
        restore_sqlite_backup "$backup_path" "$paper_state"
      else
        manifest_flag local_restore_intent ||
          die "local state equals the backup without a durable restore intent"
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

case "$state" in
  prepared)
    if ! python3 -c 'import json,sys; v=json.load(open(sys.argv[1])); raise SystemExit(0 if v.get("stop_invoked") else 1)' "$MANIFEST"; then
      if ! manifest_flag service_stop_intent; then
        was_active=$(systemctl_active_state pe-service)
        manifest_patch_boundary service-stop-intent "{\"service_stop_intent\":true,\"service_was_active\":$was_active}"
      else
        was_active=$(manifest_get service_was_active)
      fi
      if [[ "$(systemctl_active_state pe-service)" == true ]]; then
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
      preparation=$(run_target_offline --financial-era=prepare \
        --activation-manifest="$MANIFEST") || die "read-only financial-era preparation failed"
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
    [[ "$service_active" == false ]] || die "pre-Start guarded activation requires an inert service"
    verify_legacy_service_contract
    if ! python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); raise SystemExit(0 if value.get("remote_archive_completed") is True else 1)' "$MANIFEST"; then
      manifest_patch_boundary remote-archive-intent '{"remote_archive_intent":true}'
      psql_service_db -v ON_ERROR_STOP=1 -v activation_id="$activation_id" \
        -v bankroll="$fresh_bankroll" -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
      manifest_patch_boundary remote-archived '{"remote_archive_completed":true}'
    fi
    manifest_patch_boundary qualification-start-intent '{"qualification_start_intent":true}'
    start_receipt=$(run_target_offline --financial-era=start \
      --activation-manifest="$MANIFEST") || die "offline financial-era Start failed"
    manifest_patch_boundary qualification-started "$(python3 -c 'import json,sys; print(json.dumps({"start_receipt":json.loads(sys.argv[1])},sort_keys=True,separators=(",",":")))' "$start_receipt")"
  else
    start_receipt=$(python3 -c 'import json,sys; print(json.dumps(json.loads(sys.argv[1])["receipt"],sort_keys=True,separators=(",",":")))' "$COMPLETE_START_OUTPUT") || die "complete Start receipt is invalid"
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
  if ! manifest_flag target_config_adopted; then
    manifest_patch_boundary target-config-adopt-intent '{"target_config_adopt_intent":true}'
    atomic_adopt "$target_config" "$SERVICE_CONFIG" 0644 financial-config-adopted
    manifest_patch_boundary target-config-adopted '{"target_config_adopted":true}'
  fi
  if ! manifest_flag target_environment_adopted; then
    manifest_patch_boundary target-environment-adopt-intent '{"target_environment_adopt_intent":true}'
    atomic_adopt "$target_environment" "$SERVICE_ENV" 0600 financial-environment-adopted
    manifest_patch_boundary target-environment-adopted '{"target_environment_adopted":true}'
  fi
  if ! manifest_flag target_binary_adopted; then
    manifest_patch_boundary target-binary-adopt-intent '{"target_binary_adopt_intent":true}'
    atomic_adopt "$target_binary" "$SERVICE_BINARY" 0755 financial-binary-adopted
    manifest_patch_boundary target-binary-adopted '{"target_binary_adopted":true}'
  fi
  service_active=$(systemctl_active_state pe-service)
  if [[ "$service_active" == false ]]; then
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
    "$(manifest_get ranking_identity)" ||
    die "first fresh health proof is incomplete"
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
    '{"verified_state_assertions":true,"verified_start_reset":true,"verified_source_replay_continuity":true,"verified_ranking_membership":true,"verified_producers_projection":true,"verified_accounts_off_unarmed":true}'
fi

echo "activation_id=$activation_id state=$(manifest_get state)"
