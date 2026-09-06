#!/usr/bin/env bash
# Network-free contract checks for the schema-two financial-era driver (#545).

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
DRIVER="$SCRIPT_DIR/activate_financial_era.sh"
COMMON="$REPO_ROOT/scripts/deploy/generation_common.sh"
GENERATION="$REPO_ROOT/scripts/deploy/activate_generation.sh"
RESTORE="$SCRIPT_DIR/restore_paper_state.sql"
ROLLBACK="$REPO_ROOT/scripts/deploy/rollback_generation.sh"
RUNBOOK="$REPO_ROOT/docs/35-PE-SERVICE-DEPLOY-RUNBOOK.md"
REHEARSAL="$REPO_ROOT/scripts/deploy/rehearsal545.sh"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

require_text() {
  local path=$1 text=$2
  grep -Fq -- "$text" "$path" || fail "$path is missing: $text"
}

reject_text() {
  local path=$1 text=$2
  if grep -Fq -- "$text" "$path"; then
    fail "$path unexpectedly contains: $text"
  fi
}

bash -n "$DRIVER" "$COMMON" "$GENERATION" "$ROLLBACK" "$REHEARSAL"

for field in activation_id generation_dir copy_manifest_sha256 readiness_sha256 config_sha256 environment_sha256; do
  require_text "$REHEARSAL" "\"$field\""
done
require_text "$REHEARSAL" 'atomic_adopt "$copy_manifest_stage" "$copy_manifest"'
require_text "$REHEARSAL" 'atomic_adopt "$manifest_stage" "$manifest"'
require_text "$REHEARSAL" 'atomic_adopt "$evidence_stage" "$evidence_hash_file"'

# The financial transition has a separate durable identity and never invokes the
# destructive schema-one bootstrap owner.
require_text "$DRIVER" 'MANIFEST="$DEPLOY_HOME/pe-financial-era.json"'
require_text "$DRIVER" '"kind":"financial-era-v1"'
require_text "$DRIVER" 'prepared|guarded|rolling_back'
require_text "$DRIVER" 'manifest_advance guarded'
require_text "$DRIVER" 'manifest_advance started'
require_text "$DRIVER" 'manifest_advance verified'
require_text "$DRIVER" 'archive_paper_state.sql'
require_text "$DRIVER" '--financial-era=prepare'
require_text "$DRIVER" '--financial-era=start'
require_text "$DRIVER" '--financial-era=rollback-check'
require_text "$DRIVER" '--financial-config-rows='
require_text "$DRIVER" '--verify-staged-identity'
require_text "$DRIVER" '--rehearsal-evidence'
require_text "$DRIVER" 'REHEARSAL_REFUSAL='
require_text "$DRIVER" 'verify_legacy_service_contract'
require_text "$DRIVER" 'archive_restored'
require_text "$DRIVER" 'local_restored'
require_text "$DRIVER" 'old_service_started'
reject_text "$DRIVER" 'seed_v1_empty.sh'
reject_text "$DRIVER" 'sleep '
reject_text "$DRIVER" '--hot-config-hash'
reject_text "$DRIVER" '--membership-proofs-hash'
reject_text "$DRIVER" 'expected_hot_config_names_hash'
reject_text "$DRIVER" 'fresh_bankroll_identity'

# The operator command is an executable contract: every required driver flag is documented and no
# unsupported flag can drift into the canonical runbook.
python3 -c 'import re,sys
driver,runbook=sys.argv[1:]
source=open(driver,encoding="utf-8").read()
usage=source.split("usage() {",1)[1].split("}",1)[0]
usage_flags=set(re.findall(r"--[a-z][a-z0-9-]*",usage))
optional={"--rollback-before-start","--simulate-crash-after"}
text=open(runbook,encoding="utf-8").read()
section=text.split("Run the exact reviewed driver command on the production host:",1)[1]
command=section.split("```bash",1)[1].split("```",1)[0]
documented=set(re.findall(r"--[a-z][a-z0-9-]*",command))
expected=usage_flags-optional
if documented != expected:
    raise SystemExit(f"financial driver/runbook flag drift: documented={sorted(documented)} expected={sorted(expected)}")' \
  "$DRIVER" "$RUNBOOK" || fail "documented financial driver command does not match usage"

# Only the three process-proof functions moved, and both drivers source them.
for function_name in process_runs_service read_service_process_snapshot verify_installed_unit_owner; do
  [[ $(grep -Ec "^${function_name}[(][)]" "$COMMON") -eq 1 ]] ||
    fail "$function_name is not defined exactly once in generation_common.sh"
  reject_text "$GENERATION" "${function_name}()"
done

# Restore and the legacy rollback equality proof both derive their compared
# columns from PostgreSQL catalog order, so later fractional columns participate.
for path in "$RESTORE" "$ROLLBACK"; do
  require_text "$path" 'from pg_attribute a'
  require_text "$path" 'order by a.attnum'
  require_text "$path" 'not a.attisdropped'
done
reject_text "$RESTORE" 'paper_fills (idempotency_key'
require_text "$RESTORE" 'except all'
require_text "$COMMON" 'verify_legacy_service_contract()'
require_text "$DRIVER" 'activation_archive_matches_live()'

TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

write_shims() {
  local root=$1 bin=$root/bin
  mkdir -p "$bin"
  cat > "$bin/systemctl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state=${PE_ACTIVATION_TEST_ROOT:?}/test-state
case "$1" in
  is-active)
    if [[ $(<"$state/service.active") == true ]]; then echo active; exit 0; fi
    echo inactive; exit 3
    ;;
  is-enabled) echo enabled ;;
  stop)
    echo false > "$state/service.active"
    count=0; [[ ! -f "$state/stop-count" ]] || count=$(<"$state/stop-count")
    echo $((count + 1)) > "$state/stop-count"
    ;;
  start)
    echo true > "$state/service.active"
    count=0; [[ ! -f "$state/start-count" ]] || count=$(<"$state/start-count")
    echo $((count + 1)) > "$state/start-count"
    python3 -c 'import datetime,json,os,sys
root=sys.argv[1]; path=os.path.join(root,"prediction-markets/gen/g557/status.json")
value={"revision":"1"*40,"applied_config_hash":"static","updated_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
"tasks":[{"name":name,"state":"running","class":"critical"} for name in ("activity_ingest","public_activity_poll","orchestrator","resolution_poller","watchlist_refresh","status_writer","http_server")],"status_error":None,"uptime_secs":1,
"mode":"paper","authoritative":True,"bankroll":"10000","open_positions":0,"fills_total":0,"settled_total":0,"oldest_anchor_age_secs":0,
"last_event_seq":0,"watchlist_size":1,"watchlist_target_size":1,
"source_health":{"poll_error_streak":0,"copy_admission_blocked":False,"ws_sink_poisoned":False,"poll_last_round_age_secs":0},
"runtime_config":{"applied_hash":"b"*64,"rejected":None},
"watchlist_projection":{"applied":{"token":"batch:545","count":1,"time":"now"},"last_error":None},
"supabase_rpc_calls":0,"live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"fetched_at_unix":None,"stale":False,
"accounts":[{"account_id":"live-a","is_primary":True,"enabled":False,"requested_live_mode":"off","effective_live_mode":"off","armed":False}]}}
json.dump(value,open(path,"w"))' "$PE_ACTIVATION_TEST_ROOT"
    ;;
  *) echo "unexpected systemctl command: $*" >&2; exit 97 ;;
esac
SH
cat > "$bin/psql" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state=${PE_ACTIVATION_TEST_ROOT:?}/test-state
# The driver must reach psql through parsed libpq variables (never a URL in an argument).
[[ "${PGHOST:-}" == 127.0.0.1 && "${PGPORT:-}" == 1 && "${PGUSER:-}" == harness && "${PGPASSWORD:-}" == harness && "${PGDATABASE:-}" == harness ]] || exit 91
for argument in "$@"; do [[ "$argument" != *://* ]] || exit 91; done
[[ " $* " != *" fake "* ]] || exit 92
file= sql= stdin=
while (($#)); do
  case "$1" in
    -f) file=$2; shift 2 ;;
    -c|-Atc) sql=$2; shift 2 ;;
    -v) shift 2 ;;
    *) shift ;;
  esac
done
if [[ -z "$file" && -z "$sql" ]]; then stdin=$(dd bs=4096 2>/dev/null || true); fi
if [[ "$file" == *archive_paper_state.sql ]]; then
  [[ $(<"$state/service.active") == false ]] || exit 98
  count=0; [[ ! -f "$state/archive-count" ]] || count=$(<"$state/archive-count")
  [[ ! -f "$state/remote-state" || $(<"$state/remote-state") != fresh ]] &&
    echo $((count + 1)) > "$state/archive-count"
  echo fresh > "$state/remote-state"
elif [[ "$file" == *restore_paper_state.sql ]]; then
  [[ $(<"$state/service.active") == false ]] || exit 99
  count=0; [[ ! -f "$state/restore-count" ]] || count=$(<"$state/restore-count")
  echo $((count + 1)) > "$state/restore-count"
  echo restored > "$state/remote-state"
elif [[ "$stdin" == *"live % differs from activation"* ]]; then
  [[ -f "$state/remote-state" && $(<"$state/remote-state") == restored ]] || exit 1
elif [[ "$stdin" == *"anon must exist and must not bypass RLS"* ]]; then
  [[ "$stdin" == *"legacy_keys || array['risk_halt_release_hash']"* ]] || exit 93
  [[ "$stdin" == *"value_type <> 'text' or value !~ '^[0-9a-f]{64}$'"* ]] || exit 93
  if [[ -f "$state/release-row" ]]; then
    read -r count value_type value < "$state/release-row"
    [[ "$count" == 1 && "$value_type" == text && "$value" =~ ^[0-9a-f]{64}$ ]] || {
      echo 'simulated malformed optional risk_halt_release_hash' >&2
      exit 94
    }
  fi
elif [[ "$sql" == *"json_agg(json_build_object('key',key,'value',value,'value_type',value_type) order by key)"* ]]; then
  cat <<'JSON'
[{"key":"active_watchlist_size","value":"100","value_type":"integer"},{"key":"flip_human_approved","value":"false","value_type":"bool"},{"key":"kelly_fraction_above_default_human_approved","value":"false","value_type":"bool"},{"key":"max_fill_price","value":"0.85","value_type":"decimal"},{"key":"max_resolution_horizon_secs","value":"172800","value_type":"integer"},{"key":"min_fill_price","value":"0.15","value_type":"decimal"},{"key":"min_resolution_horizon_secs","value":"60","value_type":"integer"},{"key":"mode","value":"paper","value_type":"text"},{"key":"per_trade_cap","value":"unlimited","value_type":"text"},{"key":"price_impact_cap_bps","value":"100","value_type":"integer"},{"key":"sizing_contracts","value":"1","value_type":"integer"},{"key":"sizing_dollar_usd","value":"25","value_type":"decimal"},{"key":"sizing_mode","value":"dollar","value_type":"text"},{"key":"slippage_rate","value":"0.01","value_type":"decimal"}]
JSON
elif [[ "$sql" == *information_schema.columns* ]]; then
  echo 5
elif [[ "$sql" == *paper_fills_archive* ]]; then
  echo '0 0 0 1 0'
elif [[ "$sql" == *seed_financial_start* ]]; then
  echo '{"outcome":"applied","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}'
  echo '{"bankroll":"10000","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","last_prepared_seq":null}'
elif [[ "$sql" == *"'start_seq'"* ]]; then
  echo '{"paper_fills":0,"settled_markets":0,"paper_positions":0,"fill_market_snapshots":0,"bankroll_count":1,"bankroll":"10000","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","last_prepared_seq":null,"ranking_batch_id":545,"membership":["0x0000000000000000000000000000000000000545"]}'
elif [[ "$sql" == *json_build_object* ]]; then
  echo '{"paper_fills":0,"settled_markets":0,"paper_positions":0,"paper_bankroll":1,"fill_market_snapshots":0}'
fi
SH
  chmod +x "$bin/systemctl" "$bin/psql"
}

write_rehearsal_evidence() {
  local root=$1
  local artifact_sha256=${2:-$(sha256sum "$root/target/pe-service" | awk '{print $1}')}
  local rehearsal=$root/rehearsal manifest=$root/rehearsal/manifest.txt digest
  mkdir -p "$rehearsal"
  printf '%s\n' \
    'result=PASS' \
    'reason=evidence_complete' \
    'sha=1111111111111111111111111111111111111111' \
    'target_revision=1111111111111111111111111111111111111111' \
    'artifact_blake3=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
    "artifact_sha256=$artifact_sha256" > "$manifest"
  digest=$(sha256sum "$manifest" | awk '{print $1}')
  python3 -c 'import json,os,sys
path,manifest,digest,artifact_sha=sys.argv[1:]
value={"kind":"rehearsal545-evidence-v1","result":"PASS","evidence_sha256":digest,
"manifest_path":os.path.realpath(manifest),"target_revision":"1"*40,
"artifact_blake3":"a"*64,"artifact_sha256":artifact_sha}
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' \
    "$rehearsal/evidence.json" "$manifest" "$digest" "$artifact_sha256"
}

setup_fixture() {
  local root=$1 service=$root/prediction-markets target=$root/target state=$root/test-state
  mkdir -p "$service/target/release" "$service/smoke-test" "$service/gen/g557" "$target" "$state"
  : > "$root/.pe-deploy.lock"
  echo true > "$state/service.active"
  printf '%s\n' old-binary > "$service/target/release/pe-service"
  printf '%s\n' old-config > "$service/smoke-test/service.toml"
  printf '%s\n' old-environment > "$service/.env"
  printf '%s\n' target-config > "$target/service.toml"
  cat > "$target/service.env" <<'ENV'
PE_SUPABASE_AUTHORITATIVE=true
PE_SUPABASE_URL=https://example.invalid
PE_SUPABASE_SECRET_KEY=test-service-role
ENV
  printf '%s\n' '["0x0000000000000000000000000000000000000545"]' > "$target/membership.json"
  printf 'paper-before\n' > "$service/gen/g557/paper.log"
  printf 'source-before\n' > "$service/gen/g557/source_events.log"
  printf 'live-before\n' > "$service/gen/g557/live_journal.log"
  python3 -c 'import sqlite3,sys
db=sqlite3.connect(sys.argv[1]); db.executescript("""
create table durable(value text); insert into durable values ("before");
create table fills(value text); create table positions(value text);
create table settled_markets(value text); create table fill_market_snapshots(value text);
create table bankroll(id integer primary key, bankroll_str text not null);
insert into bankroll values(0,"10000"); create table meta(key text primary key,value blob not null);
"""); db.commit(); db.close()' \
    "$service/gen/g557/paper_state.db"
  cat > "$target/pe-service" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state=${PE_ACTIVATION_TEST_ROOT:?}/test-state
case "$*" in
  *--verify-staged-identity*)
    echo 'prediction-edge revision=1111111111111111111111111111111111111111 artifact_blake3=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    ;;
  *--financial-era=prepare*)
    cat <<'JSON'
{"start":{"starting_bankroll":10000000000,"paper_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"source_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"live_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"artifact_blake3":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","static_config_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","hot_config_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","generation":"g557","activation_id":"act-545","ranking_batch_id":545,"policy_hash":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","membership":["0x0000000000000000000000000000000000000545"],"membership_proofs_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","schema_version":3,"parser_version":1,"financial_semantic_version":1},"expected_receipt":{"sequence":1,"this_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}
JSON
    ;;
  *--financial-era=start*)
    python3 -c 'import sqlite3,sys
db=sqlite3.connect(sys.argv[1]);
for table in ("fills","positions","settled_markets","fill_market_snapshots"): db.execute("delete from "+table)
db.execute("delete from bankroll"); db.execute("insert into bankroll values(0,\"10000\")")
db.execute("delete from meta"); db.execute("insert into meta values(\"financial_start_seq\",\"1\")")
db.execute("insert into meta values(\"financial_start_hash\",?)",("c"*64,)); db.commit(); db.close()' \
      "$PE_ACTIVATION_TEST_ROOT/prediction-markets/gen/g557/paper_state.db"
    : > "$state/complete-start"
    echo '{"sequence":1,"this_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}'
    ;;
  *--financial-era=rollback-check*)
    [[ ! -f "$state/rollback-error" ]] || exit 95
    if [[ -f "$state/complete-start" ]]; then echo '{"complete_start":true,"receipt":{"sequence":1,"this_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}'
    else echo '{"complete_start":false,"repaired":false}'; fi
    ;;
  *) exit 96 ;;
esac
SH
  chmod +x "$target/pe-service"
  python3 -c 'import hashlib,json,sys
path,generation,service,target=sys.argv[1:]
def artifact(value):
    with open(value,"rb") as source: digest=hashlib.sha256(source.read()).hexdigest()
    return {"path":value,"sha256":digest}
value={
 "activation_id":"act-545","state":"verified","generation_dir":generation,
 "merge_commit":"1"*40,"bankroll":"10000","source_v1_main":artifact(generation+"/source_events.log"),
 "legacy_history":artifact(target+"/membership.json"),
 "artifacts":{"seed_main":artifact(generation+"/paper_state.db"),"binary":artifact(target+"/pe-service"),
              "config":artifact(target+"/service.toml"),"environment":artifact(target+"/service.env"),
              "rehearsal_config":artifact(target+"/service.toml"),
              "rehearsal_environment":artifact(target+"/service.env")},
 "old_installed_artifacts":{"service_toml":artifact(service+"/smoke-test/service.toml"),
                            "service_env":artifact(service+"/.env"),
                            "pe_service":artifact(service+"/target/release/pe-service")},
 "destinations":{"binary":service+"/target/release/pe-service","config":service+"/smoke-test/service.toml",
                 "environment":service+"/.env"},
 "old_paths":{"paper_log":generation+"/paper.log","source_log":generation+"/source_events.log",
              "live_journal":generation+"/live_journal.log","status":generation+"/status.json",
              "paper_state":generation+"/paper_state.db","legacy_history":target+"/membership.json"}}
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' \
    "$root/pe-activation.json" "$service/gen/g557" "$service" "$target"
  write_rehearsal_evidence "$root"
  write_shims "$root"
}

driver_args() {
  local root=$1 service=$root/prediction-markets target=$root/target
  DRIVER_ARGS=(
    --target-binary "$target/pe-service"
    --target-config "$target/service.toml"
    --target-environment "$target/service.env"
    --paper-log "$service/gen/g557/paper.log"
    --source-log "$service/gen/g557/source_events.log"
    --live-journal "$service/gen/g557/live_journal.log"
    --paper-state "$service/gen/g557/paper_state.db"
    --fresh-bankroll 10000
    --rehearsal-evidence "$root/rehearsal/evidence.json"
    --ranking-batch-id 545
    --policy-hash dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
    --membership-json "$target/membership.json"
  )
}

run_driver() {
  local root=$1; shift
  PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" SUPABASE_DB_URL=postgres://harness:harness@127.0.0.1:1/harness \
    PATH="$root/bin:$PATH" "$DRIVER" "${DRIVER_ARGS[@]}" "$@"
}

drive_to_verified() {
  local root=$1 state attempt
  for attempt in 1 2 3 4; do
    state=$(python3 -c 'import json,sys
try: print(json.load(open(sys.argv[1]))["state"])
except FileNotFoundError: print("absent")' "$root/pe-financial-era.json")
    [[ "$state" == verified ]] && return 0
    if [[ "$state" == started ]]; then touch "$root/prediction-markets/gen/g557/status.json"; fi
    run_driver "$root" >/dev/null
  done
  return 1
}

# Rehearsal fixtures use a curl shim instead of a loopback listener. The shim withholds readiness
# until every asynchronous observer has published passing state. For a race case it then returns one
# non-ready response to establish that state, appends the controlled late line during the next curl,
# and only then completes the successful readiness response.
setup_rehearsal_fixture() {
  local root=$1 websocket_enabled=$2 injection=$3
  local release=$root/release generation=$root/generation target=$root/target installed=$root/installed
  mkdir -p "$release/target/release" "$release/scripts/deploy" "$generation" "$target" \
    "$installed" "$root/bin" "$root/test-state"
  printf 'EDGE\001' > "$generation/paper.log"
  printf 'EDGE\001' > "$generation/source_events.log"
  printf 'EDGE\001' > "$generation/live_journal.log"
  printf '%s\n' '{}' > "$generation/wallet_market_history.json"
  python3 -c 'import sqlite3,sys
db=sqlite3.connect(sys.argv[1])
db.executescript("""
create table poll_cursors(wallet_hex text primary key,activity_cutoff_unix integer,reanchor_required integer);
create table position_anchors(wallet_hex text,anchor_seq integer);
create table wallet_fences(wallet_hex text,cause text);
insert into poll_cursors values("0x0000000000000000000000000000000000000545",1,0);
insert into position_anchors values("0x0000000000000000000000000000000000000545",1);
""")
db.commit(); db.close()' "$generation/paper_state.db"
  printf '%s\n' 'bind = "127.0.0.1:19000"' > "$installed/service.toml"
  printf '%s\n' 'PE_BIND=127.0.0.1:19000' > "$installed/service.env"
  printf '%s\n' 'bind = "127.0.0.1:19001"' > "$target/service.toml"
  printf '%s\n' \
    'PE_SUPABASE_ANON_KEY=sb_publishable_rehearsal' \
    'PE_SUPABASE_SECRET_KEY=sb_publishable_rehearsal' \
    "PE_POLYMARKET_ACTIVITY_WS_ENABLED=$websocket_enabled" > "$target/service.env"
  printf '%s\n' "$injection" > "$root/test-state/injection"

  cat > "$release/target/release/pe-service" <<'SH'
#!/bin/bash
set -euo pipefail
if [[ "$*" == *--verify-staged-identity* ]]; then
  echo 'prediction-edge revision=1111111111111111111111111111111111111111 artifact_blake3=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
  exit 0
fi
/usr/bin/python3 - "$PE_STATUS_PATH" "$PE_PAPER_STATE_DB_PATH" <<'PY'
import datetime,json,os,sqlite3,sys
status,database=sys.argv[1:]
db=sqlite3.connect(database)
db.execute("insert into position_anchors values(?,?)",("0x0000000000000000000000000000000000000545",2))
db.execute("update poll_cursors set reanchor_required=0 where wallet_hex=?",("0x0000000000000000000000000000000000000545",))
db.commit(); db.close()
value={
 "revision":"1"*40,"updated_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
 "tasks":[{"name":"public_activity_poll","state":"running","class":"critical"}],
 "source_health":{"poll_last_round_age_secs":0},
 "live":{"accounts":[{"armed":False,"requested_live_mode":"off","effective_live_mode":"off"}]},
}
temporary=status+".service.tmp"
with open(temporary,"w",encoding="utf-8") as output: json.dump(value,output)
os.replace(temporary,status)
PY
printf '%s\n' '{"level":"INFO","message":"fake rehearsal service ready"}'
trap 'exit 0' TERM INT
while :; do /bin/sleep 1; done
SH
  chmod +x "$release/target/release/pe-service"
  cat > "$release/scripts/deploy/rehearsal_preflight.sh" <<'SH'
#!/bin/bash
set -euo pipefail
[[ -f "$1" ]]
SH
  chmod +x "$release/scripts/deploy/rehearsal_preflight.sh"
  cat > "$root/bin/curl" <<'SH'
#!/bin/bash
set -euo pipefail
output=
while (($#)); do
  case "$1" in
    --output) output=$2; shift 2 ;;
    *) shift ;;
  esac
done
[[ -n "$output" ]]
rehearsal_root=$(/usr/bin/dirname "$output")
fixture_root=$(/usr/bin/dirname "$rehearsal_root")
state=$fixture_root/test-state
count=0
[[ ! -f "$state/curl-count" ]] || count=$(<"$state/curl-count")
count=$((count + 1))
printf '%s\n' "$count" > "$state/curl-count"
observers_ready=false
if [[ -f "$rehearsal_root/status-1111111.state" &&
      -f "$rehearsal_root/drops-1111111.state" &&
      -f "$rehearsal_root/fences-1111111.state" &&
      -f "$rehearsal_root/writes-1111111.state" &&
      $(<"$rehearsal_root/status-1111111.state") == '1 1 1 1 1' &&
      $(<"$rehearsal_root/drops-1111111.state") == '0 0 0' &&
      $(<"$rehearsal_root/fences-1111111.state") == '1 0' &&
      $(<"$rehearsal_root/writes-1111111.state") == '0 0' ]]; then
  observers_ready=true
fi
injection=$(<"$state/injection")
if [[ "$observers_ready" != true ]]; then
  printf '%s\n' '{"ready":false,"issues":["observers_pending"]}' > "$output"
elif [[ "$injection" != none && ! -f "$state/race-armed" ]]; then
  : > "$state/race-armed"
  printf '%s\n' '{"ready":false,"issues":["race_armed"]}' > "$output"
else
  if [[ "$injection" != none && ! -f "$state/injected" ]]; then
    case "$injection" in
      error) printf '%s\n' '{"level":"ERROR","message":"controlled late error"}' ;;
      write) printf '%s\n' '{"level":"INFO","message":"fill committed"}' ;;
      *) exit 97 ;;
    esac >> "$rehearsal_root/service-1111111.log"
    : > "$state/injected"
  fi
  printf '%s\n' '{"ready":true,"issues":[]}' > "$output"
fi
SH
  chmod +x "$root/bin/curl"
  python3 -c 'import hashlib,json,sys
path,generation,config,environment,installed_config,installed_environment=sys.argv[1:]
def artifact(value):
    with open(value,"rb") as source: digest=hashlib.sha256(source.read()).hexdigest()
    return {"path":value,"sha256":digest}
value={"state":"verified","activation_id":"act-545","generation_dir":generation,
       "artifacts":{"config":artifact(config),"environment":artifact(environment)},
       "destinations":{"config":installed_config,"environment":installed_environment}}
with open(path,"w",encoding="utf-8") as output: json.dump(value,output,sort_keys=True,separators=(",",":"))' \
    "$root/pe-activation.json" "$generation" "$target/service.toml" "$target/service.env" \
    "$installed/service.toml" "$installed/service.env"
}

run_rehearsal_fixture() {
  local root=$1
  PATH="$root/bin:$PATH" \
  PE_ACTIVATION_MANIFEST="$root/pe-activation.json" \
  PE_REHEARSAL_ROOT="$root/rehearsal" \
  PE_REHEARSAL_RELEASE_ROOT="$root/release" \
  PE_REHEARSAL_COPY_DIR="$root/rehearsal/copy" \
  PE_REHEARSAL_BIND=127.0.0.1:19001 \
  PE_REHEARSAL_TIMEOUT_SECS=10 \
  PE_REHEARSAL_POLL_SECS=1 \
  PE_REHEARSAL_EVIDENCE_HASH_FILE="$root/rehearsal/evidence.json" \
    "$REHEARSAL" --target-config "$root/target/service.toml" \
    --target-environment "$root/target/service.env" \
    1111111111111111111111111111111111111111
}

# Scenario REHEARSAL-BINDINGS-01
# Preconditions: authoritative reviewed config/environment and a clean copied generation.
# PASS: outer and result evidence bind all C2/C3 identities plus the exact final scan.
# FAIL: a required identity is absent, mismatched, or the clean rehearsal does not pass.
root=$TEST_TMP/rehearsal-bindings
setup_rehearsal_fixture "$root" true none
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] || fail "bound rehearsal did not pass: $output"
python3 -c 'import hashlib,json,os,sys
evidence_path,activation_path,config,environment,copy_manifest,readiness=sys.argv[1:]
e=json.load(open(evidence_path,encoding="utf-8"))
a=json.load(open(activation_path,encoding="utf-8"))
def digest(path): return hashlib.sha256(open(path,"rb").read()).hexdigest()
assert e["result"]=="PASS"
assert e["activation_id"]==a["activation_id"]
assert e["generation_dir"]==os.path.realpath(a["generation_dir"])
assert e["copy_manifest_sha256"]==digest(copy_manifest)
assert e["readiness_sha256"]==digest(readiness)
assert e["config_sha256"]==digest(config)
assert e["environment_sha256"]==digest(environment)
rows=dict(line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8"))
for key in ("activation_id","generation_dir","copy_manifest_sha256","readiness_sha256","config_sha256","environment_sha256"):
    assert rows[key]==e[key]
assert rows["service_log_prefix_length"].isdigit()
assert len(rows["service_log_prefix_sha256"])==64
assert rows["database_observation"]=="anchor_after:2,reanchor_required:0,unexpected_fences:0"' \
  "$root/rehearsal/evidence.json" "$root/pe-activation.json" "$root/target/service.toml" \
  "$root/target/service.env" "$root/rehearsal/copy/copied.sha256" \
  "$root/rehearsal/readiness-1111111.json" || fail "rehearsal evidence bindings are incomplete"
production_environment_sha256=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["environment_sha256"])' \
  "$root/rehearsal/evidence.json")

# Scenario REHEARSAL-CONFIG-02
# Preconditions: the explicit target environment enables the activity websocket.
# PASS: a different PE_REHEARSAL_ENV path is refused, while making the disabled file authoritative
# changes environment_sha256 in otherwise-valid evidence.
# FAIL: an arbitrary override runs or the production-affecting flag is absent from evidence identity.
root=$TEST_TMP/rehearsal-override
setup_rehearsal_fixture "$root" true none
sed 's/PE_POLYMARKET_ACTIVITY_WS_ENABLED=true/PE_POLYMARKET_ACTIVITY_WS_ENABLED=false/' \
  "$root/target/service.env" > "$root/target/disabled.env"
set +e
output=$(PE_REHEARSAL_ENV="$root/target/disabled.env" run_rehearsal_fixture "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'PE_REHEARSAL_ENV differs from the reviewed target environment'* ]] ||
  fail "arbitrary rehearsal environment override was not refused: $output"
root=$TEST_TMP/rehearsal-disabled-authority
setup_rehearsal_fixture "$root" false none
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] || fail "authoritative disabled-source rehearsal did not pass: $output"
disabled_environment_sha256=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["environment_sha256"])' \
  "$root/rehearsal/evidence.json")
[[ "$production_environment_sha256" != "$disabled_environment_sha256" ]] ||
  fail "production-enabled source change did not alter rehearsal environment identity"

# Scenarios REHEARSAL-LATE-ERROR-03 and REHEARSAL-LATE-WRITE-04
# Preconditions: all observer state is passing before curl begins the successful readiness request.
# Injected boundary: append an ERROR or successful-write line after state reads but before curl returns.
# PASS: the synchronous final prefix scan records unsafe_evidence and refuses PASS.
# FAIL: either controlled late observation produces REHEARSAL545_PASS.
for injection in error write; do
  root=$TEST_TMP/rehearsal-late-$injection
  setup_rehearsal_fixture "$root" true "$injection"
  set +e
  output=$(run_rehearsal_fixture "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *REHEARSAL545_FAIL* ]] ||
    fail "late $injection race was not refused: $output"
  python3 -c 'import json,sys
e=json.load(open(sys.argv[1],encoding="utf-8")); assert e["result"]=="FAIL"
rows=dict(line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8"))
assert rows["reason"]=="unsafe_evidence"
assert rows["service_log_prefix_length"].isdigit()
assert len(rows["service_log_prefix_sha256"])==64' "$root/rehearsal/evidence.json" ||
    fail "late $injection result did not bind the refusing final scan"
done

# Scenario FE-DERIVED-IDENTITIES-00
# Preconditions: a production-schema #557 manifest and otherwise-valid driver arguments.
# PASS: the removed digest assertions are rejected during parsing before any durable mutation.
# FAIL: a caller can still supply either derived Start identity.
root=$TEST_TMP/derived-identities
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --hot-config-hash "$(printf '0%.0s' {1..64})" >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 2 && ! -e "$root/pe-financial-era.json" && $(<"$root/test-state/service.active") == true ]] ||
  fail "removed hot-config assertion was not rejected before mutation"

# Scenario FE-LEGACY-RELEASE-VALID-00A
# Preconditions: Legacy17 plus one optional, text, lowercase-64-hex incident release row.
# Injected boundary: `legacy-contract-verified`.
# PASS: the common guard accepts the row without making it an economic key.
# FAIL: the valid incident row bricks the pre-Start cutover.
root=$TEST_TMP/release-valid
setup_fixture "$root"
driver_args "$root"
printf '1 text %s\n' "$(printf 'a%.0s' {1..64})" > "$root/test-state/release-row"
set +e
run_driver "$root" --simulate-crash-after legacy-contract-verified >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "valid optional risk-halt release row was refused"

# Scenario FE-LEGACY-RELEASE-MALFORMED-00B
# Preconditions: Legacy17 plus a malformed incident release row.
# PASS: the common guard refuses before backup, preparation, archive, or Start.
# FAIL: the malformed row crosses the legacy-contract boundary.
root=$TEST_TMP/release-malformed
setup_fixture "$root"
driver_args "$root"
printf '%s\n' '1 text NOT-A-LOWERCASE-DIGEST' > "$root/test-state/release-row"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'simulated malformed optional risk_halt_release_hash'* ]] ||
  fail "malformed optional risk-halt release row was not refused: $output"
[[ ! -e "$root/test-state/archive-count" ]] || fail "malformed release row reached archive"

# Scenario FE-REHEARSAL-MISSING-01
# Preconditions: a durable prepared manifest lacks the rehearsal binding expected by the driver.
# PASS: the rerun returns the typed unbound-evidence refusal with the service and state untouched.
# FAIL: the service stops, an archive occurs, or the prepared manifest advances.
root=$TEST_TMP/rehearsal-missing
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "rehearsal-missing setup did not stop at prepared"
python3 -c 'import json,sys
value=json.load(open(sys.argv[1])); evidence=value["rehearsal_evidence"]
assert value["state"] == "prepared" and evidence["sha256"] and evidence["artifact_blake3"] == "a"*64' \
  "$root/pe-financial-era.json" || fail "prepared manifest did not bind rehearsal evidence"
python3 -c 'import json,sys
path=sys.argv[1]; value=json.load(open(path)); del value["rehearsal_evidence"]
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' "$root/pe-financial-era.json"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'REHEARSAL_REFUSAL=prepared_evidence_unbound_or_changed'* ]] ||
  fail "prepared manifest without a rehearsal binding was not typed-refused: $output"
[[ $(<"$root/test-state/service.active") == true && ! -e "$root/test-state/stop-count" &&
   ! -e "$root/test-state/archive-count" ]] ||
  fail "unbound rehearsal evidence reached a guarded mutation"
[[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == prepared ]] ||
  fail "unbound rehearsal evidence advanced the prepared manifest"

# Scenario FE-REHEARSAL-ARTIFACT-02
# Preconditions: PASS evidence is internally hash-consistent but names a different binary artifact.
# PASS: the typed artifact refusal occurs before the financial manifest or service mutation exists.
# FAIL: mismatched rehearsal evidence is recorded or the service is stopped.
root=$TEST_TMP/rehearsal-artifact
setup_fixture "$root"
write_rehearsal_evidence "$root" 0000000000000000000000000000000000000000000000000000000000000000
driver_args "$root"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'REHEARSAL_REFUSAL=artifact_identity_mismatch'* ]] ||
  fail "mismatched rehearsal artifact was not typed-refused: $output"
[[ ! -e "$root/pe-financial-era.json" && $(<"$root/test-state/service.active") == true &&
   ! -e "$root/test-state/stop-count" && ! -e "$root/test-state/archive-count" ]] ||
  fail "mismatched rehearsal artifact caused a durable mutation"

# Scenario FE-REHEARSAL-MATCH-03
# Preconditions: PASS evidence hashes its manifest and names the exact staged binary identities.
# Injected boundaries: prepared, then service-stop-intent on the identical rerun.
# PASS: the binding remains byte-identical and the rerun may enter the guarded transition.
# FAIL: the matching rerun rewrites the binding or refuses before stop intent.
root=$TEST_TMP/rehearsal-match
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "matching rehearsal did not reach prepared"
binding_before=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["rehearsal_evidence"],sort_keys=True))' "$root/pe-financial-era.json")
set +e
run_driver "$root" --simulate-crash-after service-stop-intent >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "identical matching rehearsal rerun did not proceed"
binding_after=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["rehearsal_evidence"],sort_keys=True))' "$root/pe-financial-era.json")
[[ "$binding_before" == "$binding_after" ]] || fail "identical rehearsal rerun changed its manifest binding"
[[ $(<"$root/test-state/service.active") == true && ! -e "$root/test-state/stop-count" &&
   ! -e "$root/test-state/archive-count" ]] ||
  fail "matching rehearsal crossed the injected stop-intent boundary"

# Scenario FE-PREP-01
# Preconditions: active old service; clean paper/source/live logs and local state.
# Injected boundary: `preparation`, after offline prepare is durable and before `guarded`.
# PASS: the service is inert, inputs are byte-identical, and no remote archive exists.
# FAIL: any input changes, archive occurs, or stop is repeated.
root=$TEST_TMP/prepare
setup_fixture "$root"
driver_args "$root"
before=$(sha256sum "$root/prediction-markets/gen/g557/"{paper.log,source_events.log,live_journal.log,paper_state.db})
set +e
run_driver "$root" --simulate-crash-after preparation >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "prepare crash boundary returned $status"
after=$(sha256sum "$root/prediction-markets/gen/g557/"{paper.log,source_events.log,live_journal.log,paper_state.db})
[[ "$before" == "$after" && $(<"$root/test-state/service.active") == false ]] ||
  fail "prepare mutated a durable input or ran before the service was inert"
[[ $(<"$root/test-state/stop-count") -eq 1 ]] || fail "prepare did not stop the service exactly once"
[[ ! -e "$root/test-state/archive-count" ]] || fail "prepare reached the remote archive"

# Scenario FE-START-UNKNOWN-02
# Preconditions: stopped service with no Start and a completed stop receipt.
# Injected boundary: `service-stopped`, followed by an unreadable Start scan.
# PASS: rollback refuses before any restore. FAIL: unknown is treated as no Start or restore runs.
root=$TEST_TMP/start-unknown
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after service-stopped >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "service-stopped crash boundary returned $status"
touch "$root/test-state/rollback-error"
set +e
output=$(run_driver "$root" --rollback-before-start 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'QualificationStarted state is unknown'* ]] ||
  fail "unknown Start state did not block rollback"
[[ ! -e "$root/test-state/restore-count" ]] || fail "unknown Start state reached restore"

# Scenario FE-ROLLBACK-NOMUT-03
# Preconditions: guarded service with no remote archive and no local Start attempt.
# Injected boundary: `guarded` immediately after the durable guard receipt.
# PASS: rollback restarts the old service without remote/local restore and records no mutation.
# FAIL: any restore runs, the local database changes, or the old service remains stopped.
root=$TEST_TMP/guarded-no-mutation
setup_fixture "$root"
driver_args "$root"
local_before=$(sha256sum "$root/prediction-markets/gen/g557/paper_state.db")
set +e
run_driver "$root" --simulate-crash-after guarded >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 && ! -e "$root/test-state/archive-count" ]] ||
  fail "guarded no-mutation boundary was not reached before archive"
run_driver "$root" --rollback-before-start >/dev/null
local_after=$(sha256sum "$root/prediction-markets/gen/g557/paper_state.db")
[[ "$local_before" == "$local_after" && ! -e "$root/test-state/restore-count" ]] ||
  fail "guarded no-mutation rollback restored state without a mutation receipt"
python3 -c 'import json,sys
value=json.load(open(sys.argv[1])); assert value["state"]=="rolled_back" and value["no_financial_mutation"] is True' \
  "$root/pe-financial-era.json" || fail "guarded no-mutation rollback receipt is incomplete"
[[ $(<"$root/test-state/start-count") -eq 1 ]] || fail "guarded rollback did not restart once"

# Scenario FE-ROLLBACK-ARCHIVE-04
# Preconditions: activation-stamped remote archive, inert service, unchanged local database.
# Injected boundary: `remote-archived`.
# PASS: remote data restores once, local restore is skipped, and old service starts once.
# FAIL: duplicate restore/start or any unreceipted local restore.
root=$TEST_TMP/pre-start-rollback
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after remote-archived >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 && $(<"$root/test-state/archive-count") -eq 1 ]] ||
  fail "remote archive crash seam was not reached exactly once"
run_driver "$root" --rollback-before-start >/dev/null
[[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == rolled_back ]] ||
  fail "pre-Start rollback did not become durable"
[[ $(<"$root/test-state/restore-count") -eq 1 && $(<"$root/test-state/start-count") -eq 1 ]] ||
  fail "pre-Start rollback did not restore/restart exactly once"
python3 -c 'import json,sys
value=json.load(open(sys.argv[1])); assert value["local_restore_skipped"] is True' \
  "$root/pe-financial-era.json" || fail "unchanged local state was restored without a mutation receipt"
run_driver "$root" --rollback-before-start >/dev/null
[[ $(<"$root/test-state/restore-count") -eq 1 ]] || fail "rolled-back rerun restored twice"

# Scenario FE-START-FORWARD-05
# Preconditions: physical Start exists while the shell manifest still says guarded.
# Injected boundary: `qualification-started`.
# PASS: rollback refuses and preserves the archive. FAIL: any restore occurs after Start.
root=$TEST_TMP/complete-start
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after qualification-started >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 && -f "$root/test-state/complete-start" ]] ||
  fail "complete-Start crash seam was not reached"
set +e
output=$(run_driver "$root" --rollback-before-start 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'complete QualificationStarted forces roll-forward'* ]] ||
  fail "complete Start did not force roll-forward"
[[ ! -e "$root/test-state/restore-count" ]] || fail "complete Start was rolled back"

# Scenario FE-SERVICE-START-06
# Preconditions: complete Start and adopted target artifacts.
# Injected boundary: `before-manifest-started`, after systemctl start and its own receipt.
# PASS: rerun rolls forward without repeating archive/start. FAIL: rollback or duplicate mutation.
root=$TEST_TMP/post-service-start
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after before-manifest-started >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 && $(<"$root/test-state/service.active") == true ]] ||
  fail "post-service-start crash seam was not reached"
[[ $(<"$root/test-state/archive-count") -eq 1 ]] || fail "initial activation did not archive once"
run_driver "$root" >/dev/null
[[ $(<"$root/test-state/archive-count") -eq 1 ]] || fail "Start recovery repeated remote archive"
[[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == started ]] ||
  fail "Start recovery did not durably roll forward to started"

# Scenario FE-VERIFY-07
# Preconditions: first same-invocation status has source poll, anchors, producers, projection, and
# off/unarmed account evidence, while local/remote financial state is fresh and Start-bound.
# Injected boundary: none; verification is a single no-wait read.
# PASS: every named assertion is manifest-bound. FAIL: a generic readiness bit can satisfy it.
python3 -c 'import datetime,json,sys
path=sys.argv[1]
value={
 "revision":"1"*40,"applied_config_hash":"static","updated_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
 "tasks":[{"name":name,"state":"running","class":"critical"} for name in ("activity_ingest","public_activity_poll","orchestrator","resolution_poller","watchlist_refresh","status_writer","http_server")],"status_error":None,
 "uptime_secs":1,"mode":"paper","authoritative":True,"bankroll":"10000","open_positions":0,"oldest_anchor_age_secs":0,
 "fills_total":0,"settled_total":0,"last_event_seq":0,"watchlist_size":1,"watchlist_target_size":1,
 "runtime_config":{"applied_hash":"b"*64,"rejected":None},
 "source_health":{"poll_error_streak":0,"copy_admission_blocked":False,"ws_sink_poisoned":False,"poll_last_round_age_secs":0},
 "watchlist_projection":{"applied":{"token":"batch:545","count":1,"time":"now"},"last_error":None},
 "supabase_rpc_calls":0,
 "live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"fetched_at_unix":None,"stale":False,
         "accounts":[{"account_id":"live-a","is_primary":True,"enabled":False,
                      "requested_live_mode":"off","effective_live_mode":"off","armed":False}]}}
json.dump(value,open(path,"w"))' "$root/prediction-markets/gen/g557/status.json"
run_driver "$root" >/dev/null
python3 -c 'import json,sys
value=json.load(open(sys.argv[1]));
assert value["state"]=="verified" and value["verified_state_assertions"] is True' \
  "$root/pe-financial-era.json" || fail "verified state did not retain all merged assertions"

# Scenario FE-FORWARD-MATRIX-08
# Preconditions: fresh fixture at each case; exact staged/environment and Legacy17 guard pass.
# Injected boundaries: before, at, and after every durable manifest receipt, plus each atomic
# adoption action after rename and before its manifest receipt.
# PASS: each case converges with one stop, archive, and start. FAIL: duplicate or stranded action.
forward_manifest_boundaries=(
  prepared service-stop-intent service-stopped legacy-contract-verified preparation guarded
  remote-archive-intent remote-archived qualification-start-intent qualification-started
  authority-schema-intent authority-schema-installed authority-start-seeded
  financial-config-migration-intent financial-config-migrated
  target-config-adopt-intent target-config-adopted
  target-environment-adopt-intent target-environment-adopted
  target-binary-adopt-intent target-binary-adopted
  service-start-intent service-started started verified
)
forward_boundaries=(financial-config-adopted financial-environment-adopted financial-binary-adopted)
for receipt in "${forward_manifest_boundaries[@]}"; do
  forward_boundaries+=("before-manifest-$receipt" "$receipt" "after-manifest-$receipt")
done
for boundary in "${forward_boundaries[@]}"; do
  root="$TEST_TMP/forward-$boundary"
  [[ ! -e "$root" ]] || fail "duplicate forward fixture path for $boundary"
  setup_fixture "$root"
  driver_args "$root"
  if [[ "$boundary" == before-manifest-verified || "$boundary" == verified ||
        "$boundary" == after-manifest-verified ]]; then
    run_driver "$root" >/dev/null
    touch "$root/prediction-markets/gen/g557/status.json"
  fi
  set +e
  output=$(run_driver "$root" --simulate-crash-after "$boundary" 2>&1)
  status=$?
  set -e
  [[ $status -eq 86 ]] || fail "forward crash boundary $boundary returned $status: $output"
  drive_to_verified "$root" || fail "forward crash boundary $boundary did not converge"
  [[ $(<"$root/test-state/stop-count") -eq 1 ]] || fail "$boundary stopped the service more than once"
  [[ $(<"$root/test-state/archive-count") -eq 1 ]] || fail "$boundary archived the remote book more than once"
  [[ $(<"$root/test-state/start-count") -eq 1 ]] || fail "$boundary started the service more than once"
done

# Scenario FE-ROLLBACK-MATRIX-09
# Preconditions: stamped archive, deliberately changed local database, inert old service.
# Injected boundaries: before, at, and after every rollback receipt.
# PASS: catalog-equal remote restore, byte-equal local restore, and old start each occur once.
# FAIL: retry restores while active, repeats a transition, or loses an equality proof.
rollback_manifest_boundaries=(
  rollback-service-stopped rolling_back rollback-archive-restore-intent archive-restored
  rollback-local-mutation-observed rollback-local-restore-intent local-restored
  rollback-old-service-start-intent old-service-started rolled_back
)
rollback_boundaries=()
for receipt in "${rollback_manifest_boundaries[@]}"; do
  rollback_boundaries+=("before-manifest-$receipt" "$receipt" "after-manifest-$receipt")
done
for boundary in "${rollback_boundaries[@]}"; do
  root="$TEST_TMP/rollback-$boundary"
  [[ ! -e "$root" ]] || fail "duplicate rollback fixture path for $boundary"
  setup_fixture "$root"
  driver_args "$root"
  set +e
  run_driver "$root" --simulate-crash-after remote-archived >/dev/null 2>&1
  status=$?
  set -e
  [[ $status -eq 86 ]] || fail "rollback setup for $boundary did not reach the archive"
  python3 -c 'import sqlite3,sys
db=sqlite3.connect(sys.argv[1]); db.execute("update durable set value=\"mutated\""); db.commit(); db.close()' \
    "$root/prediction-markets/gen/g557/paper_state.db"
  if [[ "$boundary" == *rollback-service-stopped* ]]; then
    echo true > "$root/test-state/service.active"
  fi
  set +e
  output=$(run_driver "$root" --rollback-before-start --simulate-crash-after "$boundary" 2>&1)
  status=$?
  set -e
  [[ $status -eq 86 ]] || fail "rollback crash boundary $boundary returned $status: $output"
  run_driver "$root" --rollback-before-start >/dev/null
  [[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == rolled_back ]] ||
    fail "rollback crash boundary $boundary did not converge"
  [[ $(<"$root/test-state/restore-count") -eq 1 ]] || fail "$boundary restored the archive more than once"
  [[ $(<"$root/test-state/start-count") -eq 1 ]] || fail "$boundary started the old service more than once"
done

echo "PASS: FE-DERIVED-IDENTITIES-00, FE-LEGACY-RELEASE-VALID-00A/00B, FE-REHEARSAL-MISSING-01..FE-REHEARSAL-MATCH-03 and FE-PREP-01..FE-ROLLBACK-MATRIX-09; ${#forward_boundaries[@]} network-free forward hooks and ${#rollback_boundaries[@]} mutation-observed rollback hooks converge; PostgreSQL execution remains shimmed"
