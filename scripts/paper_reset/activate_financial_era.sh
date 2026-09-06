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
--fresh-bankroll DECIMAL --artifact-blake3 HASH --hot-config-hash HASH \
--ranking-batch-id ID --policy-hash HASH --membership-json PATH \
--membership-proofs-hash HASH [--rollback-before-start] [--simulate-crash-after BOUNDARY]" >&2
  exit 2
}

target_binary= target_config= target_environment=
paper_log= source_log= live_journal= paper_state=
fresh_bankroll= artifact_blake3= hot_config_hash= ranking_batch_id=
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
    --artifact-blake3) [[ $# -ge 2 ]] || usage; artifact_blake3=$2; shift 2 ;;
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
  paper_state fresh_bankroll artifact_blake3 hot_config_hash ranking_batch_id policy_hash \
  membership_json membership_proofs_hash; do
  [[ -n "${!value}" ]] || usage
done
[[ "$fresh_bankroll" =~ ^[0-9]+([.][0-9]{1,6})?$ ]] || die "fresh bankroll must be an exact non-negative six-decimal value"
[[ "$artifact_blake3" =~ ^[0-9a-f]{64}$ ]] || die "invalid artifact BLAKE3"
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

file_identity_json() {
  python3 -c 'import json,sys
print(json.dumps({"path":sys.argv[1],"sha256":sys.argv[2]},sort_keys=True,separators=(",",":")))' \
    "$1" "$(sha256_file "$1")"
}

manifest_complete_start() {
  local output
  output=$("$target_binary" "$target_config" --financial-era=rollback-check \
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
  python3 -c 'import os,sqlite3,sys
source,destination=sys.argv[1:]
src=sqlite3.connect("file:"+source+"?mode=ro", uri=True)
dst=sqlite3.connect(destination)
try: src.backup(dst); dst.commit()
finally: dst.close(); src.close()
for suffix in ("-wal","-shm"):
    try: os.unlink(destination+suffix)
    except FileNotFoundError: pass
with open(destination,"rb") as handle: os.fsync(handle.fileno())
directory=os.open(os.path.dirname(destination) or ".",os.O_RDONLY|getattr(os,"O_DIRECTORY",0))
try: os.fsync(directory)
finally: os.close(directory)' "$source" "$destination"
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
(manifest,activation,generation,bankroll,artifact,hot,batch,policy,members_path,proofs,
 paper,source,live,state,old_binary,old_config,old_env,target_binary,target_config,target_env)=sys.argv[1:]
amount=decimal.Decimal(bankroll)
atomic=amount*decimal.Decimal(1000000)
if atomic != atomic.to_integral_value(): raise SystemExit("bankroll is not exact")
members=json.load(open(members_path,encoding="utf-8"))
if not isinstance(members,list) or len(members)!=len(set(members)): raise SystemExit("membership must be a unique JSON array")
value={
 "kind":"financial-era-v1","state":"prepared","activation_id":activation,"generation":generation,
 "fresh_bankroll":int(atomic),"artifact_blake3":artifact,"static_config_hash":json.loads(target_config)["sha256"],
 "hot_config_hash":hot,"ranking_batch_id":int(batch),"policy_hash":policy,"membership":members,
 "membership_proofs_hash":proofs,"schema_version":3,"parser_version":1,"financial_semantic_version":1,
 "start_unix":int(time.time()),"paths":{"paper_log":paper,"source_log":source,"live_journal":live,"paper_state":state},
 "old_artifact_sha256":json.loads(old_binary)["sha256"],"target_artifact_sha256":json.loads(target_binary)["sha256"],
 "old_config_sha256":json.loads(old_config)["sha256"],"target_config_sha256":json.loads(target_config)["sha256"],
 "old_environment_sha256":json.loads(old_env)["sha256"],"target_environment_sha256":json.loads(target_env)["sha256"],
 "expected_hot_config_names_hash":hot,"ranking_identity":"batch:"+batch,
 "fresh_bankroll_identity":format(amount,"f"),"preparation":None}
print(json.dumps(value,sort_keys=True,separators=(",",":")))' \
    "$MANIFEST" "$activation_id" "$generation" "$fresh_bankroll" "$artifact_blake3" \
    "$hot_config_hash" "$ranking_batch_id" "$policy_hash" "$membership_json" \
    "$membership_proofs_hash" "$paper_log" "$source_log" "$live_journal" "$paper_state" \
    "$old_binary" "$old_config" "$old_environment" "$target_binary_json" \
    "$target_config_json" "$target_environment_json") || die "construct financial-era manifest"
  atomic_manifest_json "$initial" prepared
fi

[[ "$(manifest_get kind)" == financial-era-v1 ]] || die "wrong financial-era manifest kind"
[[ "$(manifest_get activation_id)" == "$activation_id" ]] || die "#557 activation identity changed"
[[ "$(manifest_get generation)" == "$generation" ]] || die "#557 generation identity changed"
[[ "$(sha256_file "$target_binary")" == "$(manifest_get target_artifact_sha256)" ]] || die "reviewed target artifact changed"
[[ "$(sha256_file "$target_config")" == "$(manifest_get target_config_sha256)" ]] || die "reviewed target config changed"
[[ "$(sha256_file "$target_environment")" == "$(manifest_get target_environment_sha256)" ]] || die "reviewed target environment changed"
[[ "$paper_log" == "$(manifest_get paths.paper_log)" &&
   "$source_log" == "$(manifest_get paths.source_log)" &&
   "$live_journal" == "$(manifest_get paths.live_journal)" &&
   "$paper_state" == "$(manifest_get paths.paper_state)" ]] || die "financial-era durable paths changed"
[[ "$artifact_blake3" == "$(manifest_get artifact_blake3)" &&
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
  [[ "$state" == rolling_back ]] || manifest_advance rolling_back
  if python3 -c 'import json,sys; v=json.load(open(sys.argv[1])); raise SystemExit(0 if "backup" in v else 1)' "$MANIFEST"; then
    backup_path=$(manifest_get backup.path)
    [[ "$(sha256_file "$backup_path")" == "$(manifest_get backup.sha256)" ]] || die "local backup hash mismatch"
    psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v activation_id="$activation_id" \
      -f "$REPO_ROOT/scripts/paper_reset/restore_paper_state.sql"
    restore_sqlite_backup "$backup_path" "$paper_state"
  fi
  if python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); raise SystemExit(0 if value.get("service_was_active") is True else 1)' "$MANIFEST"; then
    "${SERVICE_MUTATE[@]}" start pe-service
  fi
  manifest_advance rolled_back
  echo "activation_id=$activation_id state=rolled_back"
  exit 0
fi

case "$state" in
  prepared)
    if ! python3 -c 'import json,sys; v=json.load(open(sys.argv[1])); raise SystemExit(0 if v.get("stop_invoked") else 1)' "$MANIFEST"; then
      was_active=$(systemctl_active_state pe-service)
      "${SERVICE_MUTATE[@]}" stop pe-service
      manifest_patch_boundary service-stopped "{\"stop_invoked\":true,\"service_was_active\":$was_active}"
    fi
    [[ "$(systemctl_active_state pe-service)" == false ]] || die "pe-service is not inert"
    backup_path="$SERVICE_ROOT/financial-era-$activation_id-paper-state.db"
    complete_sqlite_backup "$paper_state" "$backup_path"
    backup_sha=$(sha256_file "$backup_path")
    census=$(psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -Atc \
      "select json_build_object('paper_fills',(select count(*) from paper_fills),'settled_markets',(select count(*) from settled_markets),'paper_positions',(select count(*) from paper_positions),'paper_bankroll',(select count(*) from paper_bankroll),'fill_market_snapshots',(select count(*) from fill_market_snapshots))::text;")
    patch=$(python3 -c 'import json,os,sys
backup,sha,census,paper,source,live=sys.argv[1:]
logs={name:{"path":path,"sha256":__import__("hashlib").sha256(open(path,"rb").read()).hexdigest(),"bytes":os.path.getsize(path)} for name,path in (("paper",paper),("source",source),("live",live))}
print(json.dumps({"backup":{"path":backup,"sha256":sha},"remote_census":json.loads(census),"guarded_logs":logs},sort_keys=True,separators=(",",":")))' \
      "$backup_path" "$backup_sha" "$census" "$paper_log" "$source_log" "$live_journal")
    if [[ "$(manifest_get preparation)" == "" ]]; then
      preparation=$("$target_binary" "$target_config" --financial-era=prepare \
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
    if ! python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); raise SystemExit(0 if value.get("remote_archive_completed") is True else 1)' "$MANIFEST"; then
      manifest_patch_boundary remote-archive-intent '{"remote_archive_intent":true}'
      psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -v activation_id="$activation_id" \
        -v bankroll="$fresh_bankroll" -f "$REPO_ROOT/scripts/paper_reset/archive_paper_state.sql"
      manifest_patch_boundary remote-archived '{"remote_archive_completed":true}'
    fi
    manifest_patch_boundary qualification-start-intent '{"qualification_start_intent":true}'
    start_receipt=$("$target_binary" "$target_config" --financial-era=start \
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
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/supabase_paper_state_schema.sql"
  psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/migrate_service_config_545.sql"
  atomic_adopt "$target_config" "$SERVICE_CONFIG" 0644 financial-config-adopted
  atomic_adopt "$target_environment" "$SERVICE_ENV" 0600 financial-environment-adopted
  atomic_adopt "$target_binary" "$SERVICE_BINARY" 0755 financial-binary-adopted
  service_active=$(systemctl_active_state pe-service)
  if [[ "$service_active" == false ]]; then
    manifest_patch_boundary service-start-intent '{"service_start_intent":true}'
    "${SERVICE_MUTATE[@]}" start pe-service
  else
    [[ "$service_active" == true ]] || die "pe-service has an invalid active state"
    [[ "${PE_ACTIVATION_TESTING:-0}" == 1 ]] || verify_installed_unit_owner
  fi
  started_unix=$(date +%s)
  manifest_advance started "$(python3 -c 'import json,sys; print(json.dumps({"start_receipt":json.loads(sys.argv[1]),"started_unix":int(sys.argv[2])},sort_keys=True,separators=(",",":")))' "$start_receipt" "$started_unix")"
  echo "activation_id=$activation_id state=started"
  exit 0
fi

if [[ "$state" == started ]]; then
  verify_installed_unit_owner
  [[ "$(sha256_file "$SERVICE_BINARY")" == "$(manifest_get target_artifact_sha256)" ]] || die "installed target artifact hash mismatch"
  [[ "$(sha256_file "$SERVICE_CONFIG")" == "$(manifest_get target_config_sha256)" ]] || die "installed target config hash mismatch"
  [[ "$(sha256_file "$SERVICE_ENV")" == "$(manifest_get target_environment_sha256)" ]] || die "installed target environment hash mismatch"
  [[ -f "$generation/status.json" ]] || die "fresh status proof is not available yet"
  python3 -c 'import json,os,sys
path,started=sys.argv[1],int(sys.argv[2])
value=json.load(open(path,encoding="utf-8"))
if os.stat(path).st_mtime < started: raise SystemExit("status predates financial-era start")
if not value.get("ready",False): raise SystemExit("status is not ready")' \
    "$generation/status.json" "$(manifest_get started_unix)" || die "first fresh health proof is incomplete"
  manifest_advance verified
fi

echo "activation_id=$activation_id state=$(manifest_get state)"
