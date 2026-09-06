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

bash -n "$DRIVER" "$COMMON" "$GENERATION" "$ROLLBACK"

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
require_text "$DRIVER" '--verify-staged-identity'
require_text "$DRIVER" 'verify_legacy_service_contract'
require_text "$DRIVER" 'archive_restored'
require_text "$DRIVER" 'local_restored'
require_text "$DRIVER" 'old_service_started'
reject_text "$DRIVER" 'seed_v1_empty.sh'
reject_text "$DRIVER" 'sleep '

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
[[ "${PGDATABASE:-}" == fake ]] || exit 91
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
  python3 -c 'import json,sys
path,generation=sys.argv[1:]
json.dump({"state":"verified","activation_id":"act-545","generation":generation},open(path,"w"))' \
    "$root/pe-activation.json" "$service/gen/g557"
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
{"start":{"starting_bankroll":10000000000,"paper_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"source_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"live_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"artifact_blake3":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","static_config_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","hot_config_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","generation":"g557","activation_id":"act-545","ranking_batch_id":545,"policy_hash":"policy-545","membership":[],"membership_proofs_hash":"proof-545","schema_version":3,"parser_version":1,"financial_semantic_version":1},"expected_receipt":{"sequence":1,"this_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}
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
    --hot-config-hash bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    --ranking-batch-id 545
    --policy-hash policy-545
    --membership-json "$target/membership.json"
    --membership-proofs-hash proof-545
  )
}

run_driver() {
  local root=$1; shift
  PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" SUPABASE_DB_URL=fake \
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

echo "PASS: FE-PREP-01..FE-ROLLBACK-MATRIX-09; ${#forward_boundaries[@]} network-free forward hooks and ${#rollback_boundaries[@]} mutation-observed rollback hooks converge; PostgreSQL execution remains shimmed"
