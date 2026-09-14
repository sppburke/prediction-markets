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
REHEARSAL_PREFLIGHT="$REPO_ROOT/scripts/deploy/rehearsal_preflight.sh"
CI_WORKFLOW="$REPO_ROOT/.github/workflows/ci.yml"

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

# Per file: `bash -n a b` checks only the first and treats the rest as positional arguments.
for script in "$DRIVER" "$COMMON" "$GENERATION" "$ROLLBACK" "$REHEARSAL" "$REHEARSAL_PREFLIGHT"; do
  bash -n "$script" || fail "syntax error in $script"
done
REPOSITORY_HARNESS_BUNDLE_SHA256=$(bash -c \
  'source "$1"; harness_bundle_digest "$2"' bash "$COMMON" "$REPO_ROOT/scripts/deploy") ||
  fail "could not derive the repository rehearsal harness bundle"

for field in activation_id generation_dir copy_manifest_sha256 readiness_sha256 config_sha256 \
  environment_sha256 rehearsal_environment_sha256; do
  require_text "$REHEARSAL" "\"$field\""
done
require_text "$COMMON" 'account_census_observation()'
require_text "$COMMON" 'account_census_receipt()'
require_text "$REHEARSAL_PREFLIGHT" 'REHEARSAL_ACCOUNT_CENSUS_V1'
require_text "$REHEARSAL" 'account_census_before_count='
require_text "$REHEARSAL" 'account_census_before_sha256='
require_text "$REHEARSAL" 'account_census_after_count='
require_text "$REHEARSAL" 'account_census_after_sha256='
require_text "$REHEARSAL" 'account_census_before_after_identical='
require_text "$REHEARSAL" 'atomic_adopt "$copy_manifest_stage" "$copy_manifest"'
require_text "$REHEARSAL" 'atomic_adopt "$manifest_stage" "$manifest"'
require_text "$REHEARSAL" 'atomic_adopt "$evidence_stage" "$evidence_hash_file"'

# Execute the exact logical rehearsal dry-run command committed in CI. Extracting it at test time
# makes target-flag requirements and caller syntax one contract rather than two copied commands.
ci_rehearsal_dry_run=$(python3 - "$CI_WORKFLOW" <<'PY'
import sys

lines = open(sys.argv[1], encoding="utf-8").read().splitlines()
starts = [index for index, line in enumerate(lines)
          if line.strip().startswith("bash scripts/deploy/rehearsal545.sh --dry-run")]
if len(starts) != 1:
    raise SystemExit(f"expected one rehearsal dry-run in CI, found {len(starts)}")
command = []
index = starts[0]
while True:
    line = lines[index].strip()
    command.append(line)
    if not line.endswith("\\"):
        break
    index += 1
    if index >= len(lines):
        raise SystemExit("unterminated rehearsal dry-run command in CI")
print("\n".join(command))
PY
) || fail "could not extract the CI rehearsal dry-run command"
(cd "$REPO_ROOT" && /bin/bash -c "$ci_rehearsal_dry_run") ||
  fail "the exact CI rehearsal dry-run command failed"
set +e
"$REHEARSAL" --dry-run --target-config unused \
  0000000000000000000000000000000000000000 >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 2 ]] || fail "dry-run accepted only one member of the optional target pair"
set +e
"$REHEARSAL" --dry-run --target-config '' --target-environment '' \
  0000000000000000000000000000000000000000 >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 2 ]] || fail "dry-run accepted supplied empty target paths"
dry_run_with_targets=$("$REHEARSAL" --dry-run \
  --target-config /path/need/not/exist/service.toml \
  --target-environment /path/need/not/exist/service.env \
  0000000000000000000000000000000000000000) ||
  fail "dry-run performed filesystem-dependent target validation"
[[ "$dry_run_with_targets" == *'target_config=/path/need/not/exist/service.toml'* &&
   "$dry_run_with_targets" == *'target_environment=/path/need/not/exist/service.env'* ]] ||
  fail "dry-run did not preserve the supplied target pair"

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
require_text "$DRIVER" 'supabase_multi_account_live_schema.sql'
require_text "$DRIVER" 'live-schema-installed'
require_text "$DRIVER" 'wallet-live-stats-refreshed'
require_text "$DRIVER" 'rollback-wallet-live-stats-refreshed'
require_text "$DRIVER" 'refresh materialized view concurrently public.wallet_live_stats_mv;'
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
  # Observe the real restore helper without replacing its SQLite/file operations. The crash
  # seams leave the replacement main in place with the old WAL/shm or only shm beside it.
  cat > "$bin/python3" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state=${PE_ACTIVATION_TEST_ROOT:-}/test-state
if [[ "${1-}" == -c && "${2-}" == *'shutil.copyfile(source,tmp)'* ]]; then
  /usr/bin/python3 - "$state" <<'PY'
import json, pathlib, sys
state=pathlib.Path(sys.argv[1])
manifest=json.loads((state.parent/"pe-financial-era.json").read_text())
assert manifest["local_restore_intent"] is True
assert manifest.get("local_restore_skipped") is not True
assert (state/"service.active").read_text().strip() == "false"
count=state/"local-restore-count"
count.write_text(str(int(count.read_text())+1 if count.exists() else 1)+"\n")
PY
  if [[ -f "$state/crash-after-local-replace" ]]; then
    rm "$state/crash-after-local-replace"
    script=${2/'os.replace(tmp,destination)'/$'os.replace(tmp,destination)\nos._exit(86)'}
    shift 2
    exec /usr/bin/python3 -c "$script" "$@"
  elif [[ -f "$state/crash-after-local-wal-removal" ]]; then
    rm "$state/crash-after-local-wal-removal"
    script=${2/'    except FileNotFoundError: pass'/$'    except FileNotFoundError: pass\n    if suffix == "-wal": os._exit(86)'}
    shift 2
    exec /usr/bin/python3 -c "$script" "$@"
  fi
fi
exec /usr/bin/python3 "$@"
SH
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
  show)
    if [[ -f "$state/unit-stop-policy" ]]; then
      cat "$state/unit-stop-policy"
    else
      printf '%s\n' 'KillSignal=2' 'TimeoutStopUSec=5s'
    fi
    ;;
  stop)
    echo false > "$state/service.active"
    count=0; [[ ! -f "$state/stop-count" ]] || count=$(<"$state/stop-count")
    echo $((count + 1)) > "$state/stop-count"
    # #618: production writes status.json once more on its way down, and that write can record
    # `stale: true`. Model it so the post-stop preflight has something real to catch.
    if [[ -f "$state/stale-status-on-stop" ]]; then
      python3 -c 'import json,sys
path=sys.argv[1]
value=json.load(open(path))
value["live"]["stale"]=True
json.dump(value,open(path,"w"))' \
        "$PE_ACTIVATION_TEST_ROOT/prediction-markets/gen/g557/status.json"
    fi
    if [[ -f "$state/crash-after-stop" ]]; then
      rm -f "$state/crash-after-stop"
      exit 86
    fi
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
"last_event_seq":0,"watchlist_size":1,"watchlist_target_size":100,
"source_health":{"poll_error_streak":0,"copy_admission_blocked":False,"ws_sink_poisoned":False,"poll_last_round_age_secs":0},
"runtime_config":{"applied_hash":"b"*64,"rejected":None},
"watchlist_projection":{"applied":{"token":"2026-09-14T18:36:09.442332+00:00","count":1,"time":"now"},"last_error":None},
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
# #618: count the legacy-contract proof directly, so a test can assert the post-stop remote check
# never ran rather than inferring it from a missing receipt.
if [[ "$stdin" == *rolbypassrls* ]]; then
  count=0; [[ ! -f "$state/legacy-contract-count" ]] || count=$(<"$state/legacy-contract-count")
  echo $((count + 1)) > "$state/legacy-contract-count"
fi
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
elif [[ "$file" == *supabase_multi_account_live_schema.sql ]]; then
  count=0; [[ ! -f "$state/live-schema-count" ]] || count=$(<"$state/live-schema-count")
  echo $((count + 1)) > "$state/live-schema-count"
elif [[ "$sql" == *"refresh materialized view concurrently public.wallet_live_stats_mv"* ]]; then
  [[ ! -e "$state/materialized-view-fail" ]] || exit 96
  if [[ -f "$state/remote-state" && $(<"$state/remote-state") == restored ]]; then
    count=0; [[ ! -f "$state/rollback-refresh-count" ]] || count=$(<"$state/rollback-refresh-count")
    echo $((count + 1)) > "$state/rollback-refresh-count"
  else
    count=0; [[ ! -f "$state/forward-refresh-count" ]] || count=$(<"$state/forward-refresh-count")
    echo $((count + 1)) > "$state/forward-refresh-count"
  fi
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
elif [[ "$sql" == *"from public.accounts"* ]]; then
  count=0
  [[ ! -f "$state/rehearsal-account-census-query-count" ]] ||
    count=$(<"$state/rehearsal-account-census-query-count")
  count=$((count + 1))
  printf '%s\n' "$count" > "$state/rehearsal-account-census-query-count"
  if ((count == 1)); then
    cat "$state/rehearsal-account-census-before"
  else
    cat "$state/rehearsal-account-census-after"
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
  # #628: `authority-has-traded` makes the authority report a legitimately PROGRESSED era -- the
  # exact state a resumed activation meets after the service started and filled. `seed_financial_start`
  # is idempotent for a matching Start, so it still answers `existing`; only the read-back moves.
  if [[ -f "$state/authority-has-traded" ]]; then
    echo '{"outcome":"existing","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}'
    echo '{"bankroll":"9998.99","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","last_prepared_seq":42}'
  elif [[ -f "$state/authority-start-impostor" ]]; then
    # A DIFFERENT Start on a progressed era: identity must still be refused on the resume path.
    echo '{"outcome":"existing","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}'
    echo '{"bankroll":"9998.99","start_seq":9,"start_hash":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","last_prepared_seq":42}'
  else
    echo '{"outcome":"applied","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}'
    echo '{"bankroll":"10000","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","last_prepared_seq":null}'
  fi
elif [[ "$sql" == *"'start_seq'"* ]]; then
  echo '{"paper_fills":0,"settled_markets":0,"paper_positions":0,"fill_market_snapshots":0,"bankroll_count":1,"bankroll":"10000","start_seq":1,"start_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","last_prepared_seq":null,"ranking_batch_id":545,"membership":["0x0000000000000000000000000000000000000545"]}'
elif [[ "$sql" == *json_build_object* ]]; then
  echo '{"paper_fills":0,"settled_markets":0,"paper_positions":0,"paper_bankroll":1,"fill_market_snapshots":0}'
fi
SH
cat > "$bin/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state=${PE_ACTIVATION_TEST_ROOT:?}/test-state
output=
url=
while (($#)); do
  case "$1" in
    --output) output=$2; shift 2 ;;
    --*) shift ;;
    *) url=$1; shift ;;
  esac
done
[[ -n "$output" && "$url" == http://127.0.0.1:18080/health/ready ]] || exit 96
count=0
[[ ! -f "$state/readiness-count" ]] || count=$(<"$state/readiness-count")
echo $((count + 1)) > "$state/readiness-count"
mode=pass
[[ ! -f "$state/readiness-mode" ]] || mode=$(<"$state/readiness-mode")
case "$mode" in
  pass) printf '%s\n' '{"ready":true,"issues":[]}' > "$output" ;;
  http_failure) printf '%s\n' '{"ready":false,"issues":["http_failure"]}' > "$output"; exit 22 ;;
  not_ready) printf '%s\n' '{"ready":false,"issues":[]}' > "$output" ;;
  issues) printf '%s\n' '{"ready":true,"issues":["event_log_not_writable"]}' > "$output" ;;
  *) exit 95 ;;
esac
SH
  # Issue #586 branch review: a sqlite3 wrapper that stalls the fence observer inside its helper when
  # the slow-observer marker exists, recording its pid so the scenario can prove the observer's
  # process group was reaped before the child was signalled.
  cat > "$bin/sqlite3" <<'SH'
#!/usr/bin/env bash
state=${PE_ACTIVATION_TEST_ROOT:?}/test-state
if [[ -f "$state/slow-sqlite" ]]; then
  echo $$ > "$state/slow-sqlite.pid"
  sleep 20
fi
exec /usr/bin/sqlite3 "$@"
SH
  chmod +x "$bin/python3" "$bin/systemctl" "$bin/psql" "$bin/curl" "$bin/sqlite3"
}

write_rehearsal_evidence() {
  local root=$1
  local artifact_sha256=${2:-$(sha256sum "$root/target/pe-service" | awk '{print $1}')}
  local rehearsal=$root/rehearsal manifest=$root/rehearsal/manifest.txt digest config_sha256 environment_sha256
  local empty_sha256 harness_bundle_sha256
  local rehearsal_environment_sha256=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
  config_sha256=$(sha256sum "$root/target/service.toml" | awk '{print $1}')
  environment_sha256=$(sha256sum "$root/target/service.env" | awk '{print $1}')
  empty_sha256=$(printf '' | sha256sum | awk '{print $1}')
  harness_bundle_sha256=$REPOSITORY_HARNESS_BUNDLE_SHA256
  mkdir -p "$rehearsal"
  printf '%s\n' \
    'result=PASS' \
    'reason=evidence_complete' \
    'sha=1111111111111111111111111111111111111111' \
    'target_revision=1111111111111111111111111111111111111111' \
    'artifact_blake3=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
    "artifact_sha256=$artifact_sha256" \
    'activation_id=act-545' \
    "generation_dir=$root/prediction-markets/gen/g557" \
    'copy_manifest_sha256=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc' \
    "legacy_continuations=0:$empty_sha256" \
    "harness_bundle_sha256=$harness_bundle_sha256" \
    'unit_kill_signal=2' \
    'unit_timeout_stop_secs=5' \
    'readiness_sha256=dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' \
    "config_sha256=$config_sha256" \
    "environment_sha256=$environment_sha256" \
    "rehearsal_environment_sha256=$rehearsal_environment_sha256" > "$manifest"
  digest=$(sha256sum "$manifest" | awk '{print $1}')
  python3 -c 'import json,os,sys
path,manifest,digest,artifact_sha,activation,generation,config_sha,environment_sha,rehearsal_environment_sha=sys.argv[1:]
value={"kind":"rehearsal545-evidence-v1","result":"PASS","evidence_sha256":digest,
"manifest_path":os.path.realpath(manifest),"target_revision":"1"*40,
"artifact_blake3":"a"*64,"artifact_sha256":artifact_sha,"activation_id":activation,
"generation_dir":os.path.realpath(generation),"copy_manifest_sha256":"c"*64,
"readiness_sha256":"d"*64,"config_sha256":config_sha,"environment_sha256":environment_sha,
"rehearsal_environment_sha256":rehearsal_environment_sha}
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' \
    "$rehearsal/evidence.json" "$manifest" "$digest" "$artifact_sha256" "act-545" \
    "$root/prediction-markets/gen/g557" "$config_sha256" "$environment_sha256" \
    "$rehearsal_environment_sha256"
}

rewrite_rehearsal_evidence_field() {
  local root=$1 key=$2 value=$3
  python3 - "$root/rehearsal/evidence.json" "$key" "$value" <<'PY'
import hashlib, json, sys

evidence_path, key, replacement = sys.argv[1:]
with open(evidence_path, encoding="utf-8") as source:
    evidence = json.load(source)
manifest_path = evidence["manifest_path"]
rows = []
found = False
with open(manifest_path, encoding="utf-8") as source:
    for raw in source:
        name, separator, value = raw.rstrip("\n").partition("=")
        if name == key:
            value = replacement
            found = True
        rows.append(f"{name}{separator}{value}\n")
if not found:
    raise SystemExit(f"result manifest has no {key}")
with open(manifest_path, "w", encoding="utf-8") as output:
    output.writelines(rows)
with open(manifest_path, "rb") as source:
    evidence["evidence_sha256"] = hashlib.sha256(source.read()).hexdigest()
evidence[key] = replacement
with open(evidence_path, "w", encoding="utf-8") as output:
    json.dump(evidence, output, sort_keys=True, separators=(",", ":"))
PY
}

edit_bound_manifest_row() {
  local root=$1 key=$2 mode=$3 value=${4:-}
  python3 - "$root/rehearsal/evidence.json" "$key" "$mode" "$value" <<'PY'
import hashlib, json, sys

evidence_path, key, mode, replacement = sys.argv[1:]
evidence=json.load(open(evidence_path,encoding="utf-8"))
manifest_path=evidence["manifest_path"]
rows=[]
found=[]
for raw in open(manifest_path,encoding="utf-8"):
    name,separator,value=raw.rstrip("\n").partition("=")
    if name == key:
        found.append(raw)
        if mode == "missing":
            continue
        if mode in {"replace","duplicate"}:
            raw=f"{key}={replacement}\n"
    rows.append(raw)
assert len(found) == 1
if mode == "duplicate":
    rows.append(f"{key}={replacement}\n")
with open(manifest_path,"w",encoding="utf-8") as output:
    output.writelines(rows)
evidence["evidence_sha256"]=hashlib.sha256(open(manifest_path,"rb").read()).hexdigest()
with open(evidence_path,"w",encoding="utf-8") as output:
    json.dump(evidence,output,sort_keys=True,separators=(",",":"))
PY
}

setup_hermetic_financial_driver() {
  local root=$1 copy_root=$root/hermetic-driver
  mkdir -p "$copy_root/scripts/deploy" "$copy_root/scripts/paper_reset"
  cp "$DRIVER" "$SCRIPT_DIR/archive_paper_state.sql" "$SCRIPT_DIR/restore_paper_state.sql" \
    "$copy_root/scripts/paper_reset/"
  cp "$REHEARSAL" "$COMMON" "$REHEARSAL_PREFLIGHT" "$copy_root/scripts/deploy/"
  cp "$REPO_ROOT/scripts/migrate_service_config_545.sql" \
    "$REPO_ROOT/scripts/supabase_paper_state_schema.sql" \
    "$REPO_ROOT/scripts/supabase_multi_account_live_schema.sql" "$copy_root/scripts/"
  HERMETIC_DRIVER="$copy_root/scripts/paper_reset/activate_financial_era.sh"
  HERMETIC_PREFLIGHT="$copy_root/scripts/deploy/rehearsal_preflight.sh"
  HERMETIC_BUNDLE_SHA256=$(bash -c 'source "$1"; harness_bundle_digest "$2"' bash \
    "$copy_root/scripts/deploy/generation_common.sh" "$copy_root/scripts/deploy")
  edit_bound_manifest_row "$root" harness_bundle_sha256 replace "$HERMETIC_BUNDLE_SHA256"
}

# #618: a running production service always has a status file, and the preflight reads it. The fake
# `systemctl start` writes one; fixtures that begin with the service already active must too, or the
# driver would be asked to preflight a service whose status simply does not exist.
write_clean_status() {
  local root=$1
  mkdir -p "$root/prediction-markets/gen/g557"
  python3 -c 'import datetime,json,os,sys
root=sys.argv[1]; path=os.path.join(root,"prediction-markets/gen/g557/status.json")
value={"revision":"1"*40,"applied_config_hash":"static","updated_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
"tasks":[{"name":name,"state":"running","class":"critical"} for name in ("activity_ingest","public_activity_poll","orchestrator","resolution_poller","watchlist_refresh","status_writer","http_server")],"status_error":None,"uptime_secs":1,
"mode":"paper","authoritative":True,"bankroll":"10000","open_positions":0,"fills_total":0,"settled_total":0,"oldest_anchor_age_secs":0,
"last_event_seq":0,"watchlist_size":1,"watchlist_target_size":100,
"source_health":{"poll_error_streak":0,"copy_admission_blocked":False,"ws_sink_poisoned":False,"poll_last_round_age_secs":0},
"runtime_config":{"applied_hash":"b"*64,"rejected":None},
"watchlist_projection":{"applied":{"token":"2026-09-14T18:36:09.442332+00:00","count":1,"time":"now"},"last_error":None},
"supabase_rpc_calls":0,"live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"fetched_at_unix":None,"stale":False,
"accounts":[{"account_id":"live-a","is_primary":True,"enabled":False,"requested_live_mode":"off","effective_live_mode":"off","armed":False}]}}
json.dump(value,open(path,"w"))' "$root"
}

setup_fixture() {
  # `root` MUST be assigned in its own `local` statement: bash expands every word on a `local` line
  # before the assignments take effect, so declaring the derived paths alongside it silently resolves
  # `$root` from the CALLER's global of that name. Invisible while every scenario does
  # `root=X; setup_fixture "$root"`, and wrong the moment a caller uses any other variable.
  local root=$1
  local service=$root/prediction-markets target=$root/target state=$root/test-state staged=$root/staged-557
  local journal_mode=${2:-delete}
  mkdir -p "$service/target/release" "$service/smoke-test" "$service/gen/g557" "$target" "$state" "$staged"
  : > "$root/.pe-deploy.lock"
  echo true > "$state/service.active"
  write_clean_status "$root"
  printf '%s\n' old-binary > "$service/target/release/pe-service"
  printf '%s\n' 'bind = "127.0.0.1:8080"' > "$service/smoke-test/service.toml"
  printf '%s\n' 'PE_BIND=127.0.0.1:8080' > "$service/.env"
  printf '%s\n' 'bind = "127.0.0.1:18080"' > "$target/service.toml"
  cat > "$target/service.env" <<'ENV'
PE_BIND=127.0.0.1:18080
PE_SUPABASE_AUTHORITATIVE=true
PE_SUPABASE_URL=https://example.invalid
PE_SUPABASE_ANON_KEY=sb_publishable_rehearsal
PE_SUPABASE_SECRET_KEY=sb_secret_test_service_role
ENV
  printf '%s\n' '["0x0000000000000000000000000000000000000545"]' > "$target/membership.json"
  printf 'EDGE\001paper-before\n' > "$service/gen/g557/paper.log"
  printf 'EDGE\001source-before\n' > "$service/gen/g557/source_events.log"
  printf 'EDGE\001live-before\n' > "$service/gen/g557/live_journal.log"
  printf '%s\n' '{}' > "$service/gen/g557/wallet_market_history.json"
  python3 -c 'import json,sqlite3,sys
db=sqlite3.connect(sys.argv[1]); db.execute("pragma journal_mode="+sys.argv[2])
db.executescript("""
pragma user_version=2;
create table durable(value text); insert into durable values ("before");
create index durable_value on durable(value);
create table fills(value text); create table positions(value text);
create table settled_markets(value text); create table fill_market_snapshots(value text);
create table bankroll(id integer primary key, bankroll_str text not null);
insert into bankroll values(0,"10000"); create table meta(key text primary key,value blob not null);
create table poll_cursors(wallet_hex text primary key,activity_cutoff_unix integer,reanchor_required integer);
create table position_anchors(wallet_hex text,anchor_seq integer);
create table decision_pending(source_trade_id text, semantic_revision text, wallet_hex text,
source_epoch integer, frozen_inputs_json text, post_commit_inputs_json text, state text,
terminal_disposition text, updated_at_unix integer);
create table wallet_fences(wallet_hex text,cause text);
insert into poll_cursors values("0x0000000000000000000000000000000000000545",1,0);
insert into position_anchors values("0x0000000000000000000000000000000000000545",1);
"""); db.commit()
json.dump({"schema":db.execute("select type,name,sql from sqlite_schema order by type,name").fetchall(),
           "dump":list(db.iterdump()),"version":db.execute("pragma user_version").fetchone()[0]},
          open(sys.argv[3],"w"))
db.close()' \
    "$service/gen/g557/paper_state.db" "$journal_mode" "$state/original-sqlite.json"
  cat > "$target/pe-service" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state=${PE_ACTIVATION_TEST_ROOT:-}/test-state
record_offline_environment() {
  local operation=$1
  /usr/bin/python3 - "$state/offline-$operation.environment" <<'PY'
import os, sys

with open(sys.argv[1], "wb") as output:
    for name, value in sorted(os.environb.items()):
        output.write(name + b"=" + value + b"\0")
PY
}
case "$*" in
  *--verify-staged-identity*)
    echo 'pe-service 0.1.0 revision=1111111111111111111111111111111111111111 config_identity=runtime-applied artifact_blake3=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    ;;
  *--financial-era=prepare*)
    record_offline_environment prepare
    if grep -Fq 'approved-only' \
      "$PE_ACTIVATION_TEST_ROOT/prediction-markets/gen/g557/live_journal.log"; then
      echo 'financial-era prepare found an unmatched Approved admission' >&2
      exit 1
    fi
    # The default below is a miniature, production-UNLIKE preparation: a 64-hex
    # `membership_proofs_hash` and a one-wallet membership. The real command emits a serialized
    # `MembershipProofBinding` carrying every member's anchor and validation proof documents; on the
    # live generation that measured 21,711,795 bytes for 26 members. Fixtures that model the small
    # shape are why the whole `guarded -> started -> verified` path could stay broken under a green
    # suite (#628). PE_SEAM_PRODUCTION_SHAPED_PREPARATION=1 emits the real shape instead, at a size
    # driven by the measured mean validation proof document (341,056 B).
    # A marker file, NOT an env var: run_target_offline runs the target under `env -i` with a
    # four-name allowlist (activate_financial_era.sh:373-377), so an exported toggle never reaches
    # this shim. Marker files under $state are the established mechanism (see crash-after-* above).
    seam_marker=${PE_ACTIVATION_TEST_ROOT:-}/test-state/production-shaped-preparation
    if [[ -f "$seam_marker" ]]; then
      python3 -c 'import json,sys
# Fail loudly rather than emitting nothing: a generator that dies silently would make the driver
# report its GENERIC prepare failure, which a permissive scenario would misread as the transport
# defect. Emitting no payload must never look like emitting an oversized one.
count, target = int(sys.argv[1]), int(sys.argv[2])
assert count > 0 and target > 0, "seam marker must carry positive <members> <bytes>"
members = ["0x%040x" % (0x545 + i) for i in range(count)]
overhead = len(json.dumps({"tag": "proof", "pad": ""}))
doc = json.dumps({"tag": "proof", "pad": "a" * max(target - overhead, 0)})
assert len(doc) == max(target, overhead), "document is not the requested serialized size"
proofs = [{
    "wallet": m,
    "history": {"complete": True, "proof_json": "{\"history\":true}", "updated_at_unix": 1},
    "coverage": {"activity_cutoff_unix": 1, "coverage_generation": 0, "reanchor_required": False,
                 "anchor_seq": 1, "anchored_at_unix": 1},
    "anchor": {"anchor_seq": 1, "anchored_at_unix": 1, "activity_cutoff_unix": 1,
               "balances_json": "[]", "ledger_hash_after": "ledger", "proof_json": doc},
    "validation": {"ledger_hash": "ledger", "positions_proof_hash": "positions",
                   "activity_bounds_json": "[]", "source_log_generation": "g557",
                   "proof_json": doc, "recorded_at_unix": 1},
} for m in members]
binding = json.dumps({"version": 1, "proof_hash": "c" * 64,
                      "manifest": {"membership": members, "proofs": proofs}},
                     separators=(",", ":"))
zero = "0" * 64
prefix = {"physical_tail": 1, "last_sequence": None, "last_hash": zero}
print(json.dumps({"start": {"starting_bankroll": 10000000000,
    "paper_prefix": prefix, "source_prefix": prefix, "live_prefix": prefix,
    "artifact_blake3": "a" * 64, "static_config_hash": "b" * 64, "hot_config_hash": "b" * 64,
    "generation": "g557", "activation_id": "act-545", "ranking_batch_id": 545,
    "membership": members, "membership_proofs_hash": binding,
    "schema_version": 3, "parser_version": 1, "financial_semantic_version": 1},
    "expected_receipt": {"sequence": 1, "this_hash": "c" * 64}}, separators=(",", ":")))' \
        $(cat "$seam_marker")
      exit 0
    fi
    cat <<'JSON'
{"start":{"starting_bankroll":10000000000,"paper_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"source_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"live_prefix":{"physical_tail":1,"last_sequence":null,"last_hash":"0000000000000000000000000000000000000000000000000000000000000000"},"artifact_blake3":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","static_config_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","hot_config_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","generation":"g557","activation_id":"act-545","ranking_batch_id":545,"membership":["0x0000000000000000000000000000000000000545"],"membership_proofs_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","schema_version":3,"parser_version":1,"financial_semantic_version":1},"expected_receipt":{"sequence":1,"this_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}
JSON
    ;;
  *--financial-era=start*)
    record_offline_environment start
    python3 -c 'import os,sqlite3,sys
db=sqlite3.connect(sys.argv[1]);
wal_crash=os.path.exists(sys.argv[2]+"/start-wal-crash")
if wal_crash:
    assert db.execute("pragma journal_mode").fetchone() == ("wal",)
    db.execute("pragma wal_autocheckpoint=0")
for table in ("fills","positions","settled_markets","fill_market_snapshots"): db.execute("delete from "+table)
db.execute("delete from bankroll"); db.execute("insert into bankroll values(0,\"10000\")")
db.execute("delete from meta"); db.execute("insert into meta values(\"financial_start_seq\",\"1\")")
db.execute("insert into meta values(\"financial_start_hash\",?)",("c"*64,))
if wal_crash:
    db.execute("drop index durable_value")
    db.execute("alter table durable add column reset_only text")
    db.execute("update durable set value=\"reset\"")
    db.execute("pragma user_version=3")
db.commit()
if wal_crash: os._exit(86)
db.close()' \
      "$PE_ACTIVATION_TEST_ROOT/prediction-markets/gen/g557/paper_state.db" "$state"
    : > "$state/complete-start"
    if [[ -f "$state/replace-target-binary-after-start" ]]; then
      mv "$PE_ACTIVATION_TEST_ROOT/target/pe-service.next" \
        "$PE_ACTIVATION_TEST_ROOT/target/pe-service"
    fi
    echo '{"sequence":1,"this_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}'
    ;;
  *--financial-era=preflight*)
    record_offline_environment preflight
    # #618: exercise the real gate rather than bypassing it. Record every call so a test can assert
    # which observation refused, then apply the live-status predicate to the actual status file.
    [[ "$*" == *--financial-config-rows=* ]] || {
      echo 'financial-era preflight requires the exported Financial15 rows' >&2
      exit 1
    }
    count=0; [[ ! -f "$state/preflight-count" ]] || count=$(<"$state/preflight-count")
    echo $((count + 1)) > "$state/preflight-count"
    python3 -c 'import json,sys
live=json.load(open(sys.argv[1]))["live"]
bad = (live.get("stale") is not False
       or live.get("pending_dispatch_seeds") != 0
       or live.get("ready_dispatch_seeds") != 0
       or any(a.get("requested_live_mode") != "off" or a.get("effective_live_mode") != "off"
              or a.get("armed") is not False for a in live.get("accounts", [])))
raise SystemExit(1 if bad else 0)' \
      "$PE_ACTIVATION_TEST_ROOT/prediction-markets/gen/g557/status.json" || {
      echo 'financial-era live status is stale or has pending dispatch work' >&2
      exit 1
    }
    echo '{"gates":["financial_manifest","financial_target_config","financial15_config_rows","live_status_posture"],"hot_config_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","live_accounts":1}'
    ;;
  *--financial-era=rollback-check*)
    record_offline_environment rollback-check
    if [[ -f "$state/complete-start" ]]; then
      echo '{"complete_start":true,"receipt":{"sequence":1,"this_hash":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}}'
    else
      if [[ -f "$state/rollback-error" ]]; then
        echo 'rollback-error stdout marker'
        echo 'rollback-error stderr marker' >&2
        exit 95
      fi
      if [[ -f "$state/rollback-output" ]]; then
        cat "$state/rollback-output"
        exit 0
      fi
      echo '{"complete_start":false,"repaired":false}'
    fi
    ;;
  *--update-paper-migration-paths*)
    # Runs under env -i with only the copied-generation overrides (#570): derive the fixture
    # root from the copied database path, never from PE_ACTIVATION_TEST_ROOT.
    [[ ! -v PE_SUPABASE_SECRET_KEY && ! -v SUPABASE_DB_URL ]] || {
      echo "paper migration path update received a privileged credential" >&2
      exit 96
    }
    case "$PE_PAPER_STATE_DB_PATH" in
      */rehearsal/copy/paper_state.db)
        fixture_root=${PE_PAPER_STATE_DB_PATH%/rehearsal/copy/paper_state.db}
        ;;
      *)
        echo "paper migration path update did not target the rehearsal copy" >&2
        exit 96
        ;;
    esac
    [[ "${PE_EVENT_LOG_PATH-}" == "$fixture_root/rehearsal/copy/paper.log" &&
       "${PE_SOURCE_EVENT_LOG_PATH-}" == "$fixture_root/rehearsal/copy/source_events.log" ]] || {
      echo "paper migration path update did not receive the copied log overrides" >&2
      exit 96
    }
    [[ ! -f "$fixture_root/test-state/paths-update-error" ]] || exit 94
    printf '%s\n' "$PE_PAPER_STATE_DB_PATH" > "$fixture_root/test-state/paths-updated"
    echo 'paper migration paths updated'
    ;;
  *)
    [[ "${PE_SUPABASE_ANON_KEY-}" == sb_publishable_rehearsal &&
       "${PE_SUPABASE_SECRET_KEY-}" == "$PE_SUPABASE_ANON_KEY" &&
       ! -v SUPABASE_DB_URL ]] || {
      echo "rehearsal child received a privileged credential" >&2
      exit 97
    }
    case "$PE_STATUS_PATH" in
      */rehearsal/copy/status.json)
        fixture_root=${PE_STATUS_PATH%/rehearsal/copy/status.json}
        /usr/bin/mkdir -p "$fixture_root/proc/$$"
        /usr/bin/ln -s "$0" "$fixture_root/proc/$$/exe"
        ;;
    esac
    [[ -f "$fixture_root/test-state/paths-updated" ]] || {
      echo "rehearsal child started before the copy's migration paths were updated" >&2
      exit 93
    }
    exec /usr/bin/python3 - "$PE_STATUS_PATH" "$PE_PAPER_STATE_DB_PATH" \
      "$fixture_root/test-state/rehearsal-account-status" \
      "$fixture_root/test-state/injection" \
      "$fixture_root/test-state/rehearsal-sigint-count" <<'PY'
import datetime, json, os, signal, sqlite3, sys, time

status, database, scenario_path, injection_path, sigint_marker = sys.argv[1:]
db=sqlite3.connect(database)
db.execute("insert into position_anchors values(?,?)",("0x0000000000000000000000000000000000000545",2))
fences_path=os.path.join(os.path.dirname(scenario_path),"rehearsal-fences")
if os.path.exists(fences_path):
    for line in open(fences_path,encoding="utf-8"):
        line=line.strip()
        if line:
            wallet,cause=line.split("|",1)
            db.execute("insert into wallet_fences values(?,?)",(wallet,cause))
db.commit(); db.close()

def running_status(counter=0, updated_at=None):
    value={
     "revision":"1"*40,"updated_at":(updated_at or datetime.datetime.now(datetime.timezone.utc)).isoformat(),
     "tasks":[{"name":name,"state":"running","class":"critical","failure":None}
              for name in ("public_activity_poll","status_writer")],
     "source_health":{"poll_last_round_age_secs":0,
                      "reconciliation_obligations_dropped_total":counter,
                      "ws_sink_poisoned":False},
     "live":{"stale":True,"accounts":[]},
    }
    return value

def write_status(value):
    temporary=status+".service.tmp"
    with open(temporary,"w",encoding="utf-8") as output:
        json.dump(value,output)
    os.replace(temporary,status)

injection=open(injection_path,encoding="utf-8").read().strip()
value=running_status({"within_run_loss": 1, "within_run_loss_highbit": 9223372036854775808}.get(injection, 0))
scenario=open(scenario_path,encoding="utf-8").read().strip()
if scenario == "absent_live": value.pop("live")
elif scenario == "fresh_empty": value["live"]["stale"] = False
elif scenario == "stale_nonempty":
    value["live"]["accounts"] = [{"account_id":"live-a","armed":False,
                                      "requested_live_mode":"off","effective_live_mode":"off"}]
elif scenario == "fresh_nonempty":
    value["live"] = {"stale":False,"accounts":[{"account_id":"live-a","armed":False,
                       "requested_live_mode":"off","effective_live_mode":"off"}]}
elif scenario != "authorization_denied":
    raise SystemExit("unknown rehearsal account-status scenario")
write_status(value)

def terminate(_signum, _frame):
    os._exit(0)

def interrupt(_signum, _frame):
    count=0
    try:
        count=int(open(sigint_marker,encoding="utf-8").read().strip())
    except (FileNotFoundError,ValueError):
        pass
    with open(sigint_marker,"w",encoding="utf-8") as output:
        output.write(str(count+1)+"\n")
    current=open(injection_path,encoding="utf-8").read().strip()
    if current == "exit_nonzero_on_sigint":
        os._exit(3)
    if current == "missing_file":
        try: os.unlink(status)
        except FileNotFoundError: pass
        os._exit(0)
    if current == "invalid_json":
        with open(status,"w",encoding="utf-8") as output: output.write("{invalid\n")
        os._exit(0)
    if current == "same_second_stale":
        signal_second=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        write_status(running_status(updated_at=signal_second))
        os._exit(0)

    final={
     "revision":"1"*40,"updated_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
     "tasks":[
       {"name":"public_activity_poll","state":"stopped","class":"critical","failure":None},
       {"name":"status_writer","state":"stopping","class":"critical","failure":None},
     ],
     "source_health":{"poll_last_round_age_secs":0,
                      "reconciliation_obligations_dropped_total":0,
                      "ws_sink_poisoned":False},
     "live":{"stale":True,"accounts":[]},
    }
    if current == "stale_updated_at":
        final["updated_at"]=(datetime.datetime.now(datetime.timezone.utc)
                             - datetime.timedelta(seconds=2)).isoformat()
    elif current == "no_stopping_marker":
        final["tasks"][1]["state"]="stopped"
    elif current == "missing_source_health":
        final.pop("source_health")
    elif current == "missing_counter":
        final["source_health"].pop("reconciliation_obligations_dropped_total")
    elif current == "negative_counter":
        final["source_health"]["reconciliation_obligations_dropped_total"]=-1
    elif current == "boolean_counter":
        final["source_health"]["reconciliation_obligations_dropped_total"]=True
    elif current == "string_counter":
        final["source_health"]["reconciliation_obligations_dropped_total"]="0"
    elif current == "poisoned_final":
        final["source_health"]["ws_sink_poisoned"]=True
    elif current == "poison_flag_missing":
        final["source_health"].pop("ws_sink_poisoned")
    elif current == "poison_flag_non_boolean":
        final["source_health"]["ws_sink_poisoned"]="false"
    elif current == "failed_critical_owner":
        final["tasks"][0].update(state="failed",failure={"kind":"fixture"})
    elif current == "critical_owner_still_running":
        final["tasks"][0]["state"]="running"
    elif current == "obligations_dropped":
        final["source_health"]["reconciliation_obligations_dropped_total"]=1
    elif current == "obligations_dropped_highbit":
        final["source_health"]["reconciliation_obligations_dropped_total"]=9223372036854775808
    write_status(final)
    delay = {"delayed_clean_exit": 6, "delayed_clean_exit_just_over": 5.05, "clean_exit_inside_bound": 4.5}.get(current)
    if delay is not None:
        time.sleep(delay)
    os._exit(0)

signal.signal(signal.SIGTERM, terminate)
if injection == "ignore_sigint":
    signal.signal(signal.SIGINT, signal.SIG_IGN)
else:
    signal.signal(signal.SIGINT, interrupt)
print('{"level":"INFO","message":"fake rehearsal service ready"}',flush=True)
while True:
    signal.pause()
PY
    ;;
esac
SH
  chmod +x "$target/pe-service"
  cp "$service/target/release/pe-service" "$staged/pe-service"
  cp "$service/smoke-test/service.toml" "$staged/service.toml"
  cp "$service/.env" "$staged/service.env"
  python3 -c 'import hashlib,json,sys
path,generation,service,target,staged=sys.argv[1:]
def artifact(value):
    with open(value,"rb") as source: digest=hashlib.sha256(source.read()).hexdigest()
    return {"path":value,"sha256":digest}
value={
 "activation_id":"act-545","state":"verified","generation_dir":generation,
 "merge_commit":"5"*40,"bankroll":"10000","source_v1_main":artifact(generation+"/source_events.log"),
 "legacy_history":artifact(generation+"/wallet_market_history.json"),
 "artifacts":{"seed_main":artifact(generation+"/paper_state.db"),"binary":artifact(staged+"/pe-service"),
              "config":artifact(staged+"/service.toml"),"environment":artifact(staged+"/service.env"),
              "rehearsal_config":artifact(staged+"/service.toml"),
              "rehearsal_environment":artifact(staged+"/service.env")},
 "old_installed_artifacts":{"service_toml":artifact(service+"/smoke-test/service.toml"),
                            "service_env":artifact(service+"/.env"),
                            "pe_service":artifact(service+"/target/release/pe-service")},
 "destinations":{"binary":service+"/target/release/pe-service","config":service+"/smoke-test/service.toml",
                 "environment":service+"/.env"},
 "old_paths":{"paper_log":generation+"/paper.log","source_log":generation+"/source_events.log",
              "live_journal":generation+"/live_journal.log","status":generation+"/status.json",
              "paper_state":generation+"/paper_state.db","legacy_history":generation+"/wallet_market_history.json"}}
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' \
    "$root/pe-activation.json" "$service/gen/g557" "$service" "$target" "$staged"
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
    --membership-json "$target/membership.json"
  )
}

run_driver() {
  local root=$1; shift
  PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" SUPABASE_DB_URL=postgres://harness:harness@127.0.0.1:1/harness \
    PATH="$root/bin:$PATH" "$DRIVER" "${DRIVER_ARGS[@]}" "$@"
}

assert_restored_sqlite() {
  local root=$1 database=$root/prediction-markets/gen/g557/paper_state.db backup
  backup=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["backup"]["path"])' \
    "$root/pe-financial-era.json")
  # Opening a WAL-mode database can create sidecars, so inspect physical completion first.
  [[ ! -e "$database-wal" && ! -e "$database-shm" ]] || fail "$root retained SQLite sidecars"
  cmp -s "$database" "$backup" || fail "$root main bytes differ from the complete backup"
  python3 - "$database" "$root/test-state/original-sqlite.json" <<'PY' || fail "$root old-reader SQL reopen failed"
import json, sqlite3, sys
original=json.load(open(sys.argv[2]))
db=sqlite3.connect(sys.argv[1])
assert db.execute("pragma integrity_check").fetchone() == ("ok",)
assert db.execute("pragma user_version").fetchone()[0] == original["version"]
assert [list(row) for row in db.execute("select type,name,sql from sqlite_schema order by type,name")] == original["schema"]
assert list(db.iterdump()) == original["dump"]
assert db.execute("select value from durable indexed by durable_value").fetchall() == [("before",)]
assert db.execute("select bankroll_str from bankroll where id=0").fetchone() == ("10000",)
assert db.execute("select value from meta where key='financial_start_seq'").fetchone() is None
db.close()
PY
}

drive_to_wal_reset() {
  local root=$1 active=${2:-true} status output
  setup_fixture "$root" wal
  driver_args "$root"
  echo "$active" > "$root/test-state/service.active"
  touch "$root/test-state/start-wal-crash"
  set +e
  output=$(run_driver "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *'offline financial-era Start failed'* ]] ||
    fail "WAL-only reset did not interrupt Start: $output"
  python3 - "$root" <<'PY' || fail "$root did not retain the WAL-only reset"
import hashlib, json, os, pathlib, sqlite3, sys
root=pathlib.Path(sys.argv[1]); manifest=json.loads((root/"pe-financial-era.json").read_text())
database=pathlib.Path(manifest["paths"]["paper_state"])
assert manifest["qualification_start_intent"] is True and manifest["state"] == "guarded"
assert not (root/"test-state/complete-start").exists()
assert hashlib.sha256(database.read_bytes()).hexdigest() == manifest["guarded_paper_state_sha256"]
assert pathlib.Path(str(database)+"-wal").stat().st_size > 0
db=sqlite3.connect("file:"+str(database)+"?mode=ro",uri=True)
assert db.execute("select value,reset_only from durable").fetchall() == [("reset",None)]
assert db.execute("pragma user_version").fetchone() == (3,)
assert db.execute("select value from meta where key='financial_start_seq'").fetchone() == ("1",)
os._exit(0)  # Leave the crash's WAL intact; a graceful last close may checkpoint it.
PY
}

rollback_snapshot() {
  python3 - "$1" <<'PY'
import hashlib, json, pathlib, sys
root=pathlib.Path(sys.argv[1]); generation=root/"prediction-markets/gen/g557"
paths=[root/"pe-financial-era.json"]
paths += [generation/name for name in ("paper_state.db","paper_state.db-wal","paper_state.db-shm",
                                     "paper.log","source_events.log","live_journal.log")]
paths += [root/"test-state"/name for name in ("archive-count","restore-count","local-restore-count",
                                           "start-count","stop-count","service.active")]
print(json.dumps({str(path):hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None
                  for path in paths},sort_keys=True))
PY
}

drive_to_verified() {
  local root=$1 state attempt
  for attempt in 1 2 3 4; do
    state=$(python3 -c 'import json,sys
try: print(json.load(open(sys.argv[1]))["state"])
except FileNotFoundError: print("absent")' "$root/pe-financial-era.json")
    [[ "$state" == verified ]] && return 0
    # `touch` deliberately, NOT write_clean_status: on an existing file touch preserves content,
    # so a dirty status written by the fake `systemctl start` still refuses here. Replacing the
    # document would hand these convergence tests passing evidence they never earned.
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
  local account_scenario=${4:-authorization_denied} census_scenario=${5:-nonzero_identical}
  local release=$root/release target=$root/target
  setup_fixture "$root"
  mkdir -p "$release/target/release" "$release/scripts/deploy"
  printf '%s\n' "PE_POLYMARKET_ACTIVITY_WS_ENABLED=$websocket_enabled" >> "$target/service.env"
  printf '%s\n' "$injection" > "$root/test-state/injection"
  printf '%s\n' "$account_scenario" > "$root/test-state/rehearsal-account-status"
  case "$census_scenario" in
    zero_identical)
      : > "$root/test-state/rehearsal-account-census-before"
      : > "$root/test-state/rehearsal-account-census-after"
      ;;
    nonzero_identical)
      printf '%s\n' 'live-a|off|off' > "$root/test-state/rehearsal-account-census-before"
      cp "$root/test-state/rehearsal-account-census-before" \
        "$root/test-state/rehearsal-account-census-after"
      ;;
    live_tiny_after)
      printf '%s\n' 'live-a|off|off' > "$root/test-state/rehearsal-account-census-before"
      printf '%s\n' 'live-a|live_tiny|live_tiny' > \
        "$root/test-state/rehearsal-account-census-after"
      ;;
    changed_after)
      printf '%s\n' 'live-a|off|off' > "$root/test-state/rehearsal-account-census-before"
      printf '%s\n' 'live-a|off|off' 'live-b|off|off' > \
        "$root/test-state/rehearsal-account-census-after"
      ;;
    *) fail "unknown rehearsal census scenario: $census_scenario" ;;
  esac
  cp "$target/pe-service" "$release/target/release/pe-service"
  cp "$COMMON" "$release/scripts/deploy/generation_common.sh"
  cat > "$release/scripts/deploy/rehearsal_preflight.sh" <<'SH'
#!/bin/bash
set -euo pipefail
[[ -f "$1" ]]
source "$(cd "$(dirname "$0")" && pwd)/generation_common.sh"
env_file_values "$1" PE_SUPABASE_URL PE_SUPABASE_ANON_KEY PE_SUPABASE_SECRET_KEY >/dev/null
fixture_root=$(dirname "$(dirname "$1")")
export PE_ACTIVATION_TEST_ROOT="$fixture_root"
export SUPABASE_DB_URL
python3 -c 'import os; raise SystemExit(0 if os.environ.get("SUPABASE_DB_URL") else 1)'
account_census=$(account_census_receipt)
read -r account_census_count account_census_sha256 <<< "$account_census"
printf 'REHEARSAL_ACCOUNT_CENSUS_V1 count=%s sha256=%s\n' \
  "$account_census_count" "$account_census_sha256"
if [[ -f "$fixture_root/test-state/replace-rehearsal-target-binary" ]]; then
  mv "$fixture_root/target/pe-service.next" "$fixture_root/target/pe-service"
fi
if [[ -f "$fixture_root/test-state/replace-rehearsal-private-binary" ]]; then
  mv "$fixture_root/target/pe-service.next" \
    "$fixture_root/rehearsal/artifacts-1111111/pe-service"
fi
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
injection=$(<"$state/injection")
observers_ready=false
expected_drops='0 0'
if [[ "$injection" == recycle && -f "$state/injected" ]]; then expected_drops='1 0'; fi
if [[ -f "$rehearsal_root/status-1111111.state" &&
      -f "$rehearsal_root/drops-1111111.state" &&
      -f "$rehearsal_root/fences-1111111.state" &&
      -f "$rehearsal_root/writes-1111111.state" &&
      $(<"$rehearsal_root/status-1111111.state") == '1 1 1 1 1 0' &&
      $(<"$rehearsal_root/drops-1111111.state") == "$expected_drops" &&
      $(<"$rehearsal_root/fences-1111111.state") == '1 0' &&
      $(<"$rehearsal_root/writes-1111111.state") == '0 0' ]]; then
  observers_ready=true
fi
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
      recycle)
        if [[ -f "$state/recycle-log-line" ]]; then
          cat "$state/recycle-log-line"
        else
          printf '%s\n' '{"level":"WARN","message":"activity ws reader produced no normalized activity row; dropping socket","slot":1,"timeout_secs":30,"last_wire_frame_age_secs":"Some(0)","last_normalized_activity_age_secs":"Some(175)","buffered_frame_processed":true}'
        fi
        ;;
      slow_observer) : > "$state/slow-sqlite" ;;
      *) : ;;
    esac >> "$rehearsal_root/service-1111111.log"
    : > "$state/injected"
    if [[ "$injection" == recycle ]]; then
      printf '%s\n' '{"ready":false,"issues":["recycle_observers_pending"]}' > "$output"
      exit 0
    fi
  fi
  printf '%s\n' '{"ready":true,"issues":[]}' > "$output"
fi
SH
  chmod +x "$root/bin/curl"
}

run_rehearsal_fixture() {
  local root=$1
  PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" PROC_ROOT="$root/proc" \
  PATH="$root/bin:$PATH" \
  PE_ACTIVATION_MANIFEST="$root/pe-activation.json" \
  PE_REHEARSAL_ROOT="$root/rehearsal" \
  PE_REHEARSAL_RELEASE_ROOT="$root/release" \
  PE_REHEARSAL_COPY_DIR="$root/rehearsal/copy" \
  PE_REHEARSAL_BIND=127.0.0.1:19001 \
  PE_REHEARSAL_TIMEOUT_SECS=10 \
  PE_REHEARSAL_POLL_SECS=1 \
  PE_REHEARSAL_EVIDENCE_HASH_FILE="$root/rehearsal/evidence.json" \
  SUPABASE_DB_URL=postgres://harness:harness@127.0.0.1:1/harness \
    "$REHEARSAL" --target-config "$root/target/service.toml" \
    --target-environment "$root/target/service.env" \
    1111111111111111111111111111111111111111
}

run_with_proc_sweep() {
  local output=$1 trace=$2; shift 2
  local command_pid monitor_pid status
  "$@" >> "$output" 2>> "$trace" &
  command_pid=$!
  TRACE_SENTINEL_DATABASE_URL="$SENTINEL_DATABASE_URL" \
    TRACE_SENTINEL_SECRET_KEY="$SENTINEL_SECRET_KEY" \
    TRACE_SWEEP_PID="$command_pid" TRACE_SWEEP_RESULT="$output.proc-sweep" \
    python3 -c 'import glob,os,time
needles=(os.environ["TRACE_SENTINEL_DATABASE_URL"].encode(),
         os.environ["TRACE_SENTINEL_SECRET_KEY"].encode())
watched=int(os.environ["TRACE_SWEEP_PID"]); result=os.environ["TRACE_SWEEP_RESULT"]
found=[]
while os.path.exists(f"/proc/{watched}"):
    for path in glob.glob("/proc/[0-9]*/cmdline"):
        try: value=open(path,"rb").read()
        except OSError: continue
        if any(needle in value for needle in needles): found.append(path)
    time.sleep(0.001)
with open(result,"w",encoding="utf-8") as output:
    output.write("\n".join(sorted(set(found))))' &
  monitor_pid=$!
  if wait "$command_pid"; then status=0; else status=$?; fi
  wait "$monitor_pid"
  [[ ! -s "$output.proc-sweep" ]] || fail "credential appeared in a process command line"
  return "$status"
}

assert_trace_has_no_sentinels() {
  local trace=$1
  TRACE_SENTINEL_DATABASE_URL="$SENTINEL_DATABASE_URL" \
    TRACE_SENTINEL_SECRET_KEY="$SENTINEL_SECRET_KEY" TRACE_PATH="$trace" \
    python3 -c 'import os
value=open(os.environ["TRACE_PATH"],"rb").read()
needles=(os.environ["TRACE_SENTINEL_DATABASE_URL"].encode(),
         os.environ["TRACE_SENTINEL_SECRET_KEY"].encode())
offending=[]
for line in value.splitlines():
    if any(needle in line for needle in needles):
        for needle in needles: line=line.replace(needle,b"<credential>")
        offending.append(line.decode("utf-8",errors="replace"))
if offending:
    print("\n".join(offending),file=__import__("sys").stderr)
    raise SystemExit(1)' ||
    fail "credential appeared in bash xtrace output"
}

run_rehearsal_traced() {
  local root=$1
  PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" PROC_ROOT="$root/proc" \
  PATH="$root/bin:$PATH" \
  PE_ACTIVATION_MANIFEST="$root/pe-activation.json" \
  PE_REHEARSAL_ROOT="$root/rehearsal" \
  PE_REHEARSAL_RELEASE_ROOT="$root/release" \
  PE_REHEARSAL_COPY_DIR="$root/rehearsal/copy" \
  PE_REHEARSAL_BIND=127.0.0.1:19001 \
  PE_REHEARSAL_TIMEOUT_SECS=10 PE_REHEARSAL_POLL_SECS=1 \
  PE_REHEARSAL_EVIDENCE_HASH_FILE="$root/rehearsal/evidence.json" \
  SUPABASE_DB_URL="$SENTINEL_DATABASE_URL" \
    bash -x "$REHEARSAL" --target-config "$root/target/service.toml" \
    --target-environment "$root/target/service.env" \
    1111111111111111111111111111111111111111
}

run_driver_traced() {
  local root=$1
  PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" \
    SUPABASE_DB_URL="$SENTINEL_DATABASE_URL" PATH="$root/bin:$PATH" \
    bash -x "$DRIVER" "${DRIVER_ARGS[@]}"
}

# Scenario REHEARSAL-BINDINGS-01
# Preconditions: authoritative reviewed config/environment and a clean copied generation.
# PASS: outer and result evidence bind all C2/C3 identities plus the exact final scan.
# FAIL: a required identity is absent, mismatched, or the clean rehearsal does not pass.
root=$TEST_TMP/rehearsal-bindings
setup_rehearsal_fixture "$root" true none
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] || fail "bound rehearsal did not pass: $output"
[[ "$(cat "$root/test-state/paths-updated")" == "$root/rehearsal/copy/paper_state.db" ]] ||
  fail "rehearsal did not update the copy's migration paths before starting the child"
[[ "$output" == *"rehearsal copy migration paths: paper migration paths updated"* ]] ||
  fail "rehearsal output did not report the migration path update: $output"
grep -Fq "PROCESS_EXE expected=$root/rehearsal/artifacts-1111111/pe-service resolved=$root/rehearsal/artifacts-1111111/pe-service matches=true" \
  "$root/rehearsal/watch-1111111.log" ||
  fail "rehearsal watch log did not bind the running private executable"
python3 -c 'import datetime,hashlib,json,os,re,stat,sys
(evidence_path,activation_path,config,environment,rehearsal_environment,copy_manifest,
 readiness,target_binary,census_before_path,census_after_path,census_query_count_path,
 final_status_path,expected_bundle)=sys.argv[1:]
e=json.load(open(evidence_path,encoding="utf-8"))
a=json.load(open(activation_path,encoding="utf-8"))
def digest(path): return hashlib.sha256(open(path,"rb").read()).hexdigest()
assert e["result"]=="PASS"
assert e["evidence_sha256"]==digest(e["manifest_path"])
assert e["activation_id"]==a["activation_id"]
assert e["generation_dir"]==os.path.realpath(a["generation_dir"])
assert set(a["artifacts"])=={"seed_main","binary","config","environment","rehearsal_config","rehearsal_environment"}
assert a["artifacts"]["binary"]["path"] != os.path.realpath(target_binary)
assert a["artifacts"]["config"]["path"] != os.path.realpath(config)
assert a["artifacts"]["environment"]["path"] != os.path.realpath(environment)
assert e["copy_manifest_sha256"]==digest(copy_manifest)
assert e["readiness_sha256"]==digest(readiness)
assert e["config_sha256"]==digest(config)
assert e["environment_sha256"]==digest(environment)
assert e["rehearsal_environment_sha256"]==digest(rehearsal_environment)
assert e["rehearsal_environment_sha256"] != e["environment_sha256"]
assert stat.S_IMODE(os.stat(rehearsal_environment).st_mode) == 0o600
credential=re.compile(r"^[ \t]*(?:export[ \t]+)?PE_SUPABASE_(?:ANON|SECRET)_KEY[ \t]*=")
production_lines=open(environment,encoding="utf-8").readlines()
rehearsal_lines=open(rehearsal_environment,encoding="utf-8").readlines()
assert [line for line in production_lines if not credential.match(line)] == [line for line in rehearsal_lines if not credential.match(line)]
rehearsal_credentials=[line.rstrip("\n").split("=",1)[1] for line in rehearsal_lines if credential.match(line)]
assert rehearsal_credentials == ["sb_publishable_rehearsal","sb_publishable_rehearsal"]
raw_rows=[line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8")]
rows=dict(raw_rows)
for key in ("activation_id","generation_dir","copy_manifest_sha256","readiness_sha256","config_sha256","environment_sha256","rehearsal_environment_sha256"):
    assert rows[key]==e[key]
assert rows["legacy_continuations"] == "0:" + hashlib.sha256(b"").hexdigest()
for key in ("harness_bundle_sha256","final_status_sha256","unit_kill_signal",
            "unit_timeout_stop_secs","shutdown_signal_unix","shutdown_elapsed_secs"):
    assert sum(name == key for name,_ in raw_rows) == 1
assert rows["harness_bundle_sha256"] == expected_bundle
assert rows["unit_kill_signal"] == "2" and rows["unit_timeout_stop_secs"] == "5"
assert rows["shutdown_signal_unix"].isdigit() and rows["shutdown_elapsed_secs"].isdigit()
assert rows["final_status_sha256"] == digest(final_status_path)
final_status=json.load(open(final_status_path,encoding="utf-8"))
updated=datetime.datetime.fromisoformat(final_status["updated_at"].replace("Z","+00:00")).timestamp()
assert updated >= int(rows["shutdown_signal_unix"])
assert final_status["source_health"]["ws_sink_poisoned"] is False
before=open(census_before_path,"rb").read()
after=open(census_after_path,"rb").read()
assert rows["account_census_before_count"]==str(len(before.splitlines()))
assert rows["account_census_before_sha256"]==hashlib.sha256(before).hexdigest()
assert rows["account_census_before_safe"]=="true"
assert rows["account_census_after_count"]==str(len(after.splitlines()))
assert rows["account_census_after_sha256"]==hashlib.sha256(after).hexdigest()
assert rows["account_census_after_safe"]=="true"
assert rows["account_census_before_after_identical"]=="true"
assert open(census_query_count_path,encoding="utf-8").read().strip()=="2"
assert rows["service_log_prefix_length"].isdigit()
assert len(rows["service_log_prefix_sha256"])==64
assert rows["database_observation"]=="anchor_rows_before:1,anchor_rows_after:2,unexpected_fences:0"' \
  "$root/rehearsal/evidence.json" "$root/pe-activation.json" "$root/target/service.toml" \
  "$root/target/service.env" "$root/rehearsal/environment-1111111.rehearsal.env" \
  "$root/rehearsal/copy/copied.sha256" \
  "$root/rehearsal/readiness-1111111.json" "$root/target/pe-service" \
  "$root/test-state/rehearsal-account-census-before" \
  "$root/test-state/rehearsal-account-census-after" \
  "$root/test-state/rehearsal-account-census-query-count" \
  "$root/rehearsal/copy/status.json" "$REPOSITORY_HARNESS_BUNDLE_SHA256" ||
  fail "rehearsal evidence bindings are incomplete"
production_environment_sha256=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["environment_sha256"])' \
  "$root/rehearsal/evidence.json")
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "authentic rehearsal evidence did not join the financial driver fixture"
python3 -c 'import json,sys
activation=json.load(open(sys.argv[1])); financial=json.load(open(sys.argv[2])); evidence=json.load(open(sys.argv[3]))
rows=dict(line.rstrip("\n").split("=",1) for line in open(evidence["manifest_path"],encoding="utf-8"))
assert financial["rehearsal_evidence"]["sha256"] == evidence["evidence_sha256"]
assert financial["rehearsal_evidence"]["legacy_continuations"] == rows["legacy_continuations"]
assert financial["rehearsal_evidence"]["harness_bundle_sha256"] == rows["harness_bundle_sha256"]
assert financial["rehearsal_evidence"]["unit_kill_signal"] == 2
assert financial["rehearsal_evidence"]["unit_timeout_stop_secs"] == 5
assert financial["target_config_sha256"] == evidence["config_sha256"]
assert financial["target_environment_sha256"] == evidence["environment_sha256"]
assert financial["old_artifact_sha256"] == activation["artifacts"]["binary"]["sha256"]
assert financial["old_config_sha256"] == activation["artifacts"]["config"]["sha256"]
assert financial["old_environment_sha256"] == activation["artifacts"]["environment"]["sha256"]
assert financial["old_artifact_sha256"] != financial["target_artifact_sha256"]
assert financial["old_config_sha256"] != financial["target_config_sha256"]
assert financial["old_environment_sha256"] != financial["target_environment_sha256"]' \
  "$root/pe-activation.json" "$root/pe-financial-era.json" "$root/rehearsal/evidence.json" ||
  fail "joined rehearsal/driver fixture conflated inherited and target identities"

# Scenarios REHEARSAL-FENCES-08A..08C
# Preconditions: the fake child durably fences wallets after startup and installs one anchor for
# wallet ...0545 above the pre-launch baseline.
# PASS: every allowlisted cause still passes with unexpected_fences:0 and anchored=1; a defined
# but unlisted cause and a non-enum cause each fail with reason=unsafe_evidence.
# FAIL: an allowlisted cause fails, or an unlisted cause records REHEARSAL545_PASS.
root=$TEST_TMP/rehearsal-fences-allowlisted
setup_rehearsal_fixture "$root" true none
printf '%s\n' \
  '0x00000000000000000000000000000000000000a1|order_dependent_equal_second' \
  '0x00000000000000000000000000000000000000a2|position_underflow' \
  '0x00000000000000000000000000000000000000a3|conversion_unknown_conditions' \
  > "$root/test-state/rehearsal-fences"
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] || fail "allowlisted fence causes did not pass: $output"
[[ $(<"$root/rehearsal/fences-1111111.state") == '1 0' ]] ||
  fail "allowlisted fence causes changed the fence/anchor census: $(<"$root/rehearsal/fences-1111111.state")"
grep -Fq 'unexpected_fences:0' \
  "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["manifest_path"])' "$root/rehearsal/evidence.json")" ||
  fail "allowlisted fence causes were counted as unexpected"
for fence_cause in position_overflow unexpected_test_cause; do
  root=$TEST_TMP/rehearsal-fences-$fence_cause
  setup_rehearsal_fixture "$root" true none
  printf '%s\n' "0x00000000000000000000000000000000000000b1|$fence_cause" > "$root/test-state/rehearsal-fences"
  set +e
  output=$(run_rehearsal_fixture "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *"REHEARSAL545_FAIL reason=unsafe_evidence"* ]] ||
    fail "unlisted fence cause $fence_cause did not fail as unsafe evidence: $output"
  [[ "$output" != *REHEARSAL545_PASS* ]] || fail "unlisted fence cause $fence_cause passed"
done

# Scenario REHEARSAL-LEGACY-CONTINUATIONS-08D (#584)
# Preconditions: the checkpoint source has three terminal pre-#545 version-2 continuations (one
# compact, one with JSON whitespace, one whose decision inputs carry an unrelated "era") and three rows that must not count: an era-bearing version-3
# row, a version-20 row, and a version-3 row whose nested decision inputs contain "version":2 and
# "fill_mode". PASS: the copy census binds exactly the three legacy rows and the otherwise-clean
# rehearsal still passes. FAIL: a row is missed, a non-legacy row is counted, or PASS changes.
root=$TEST_TMP/rehearsal-legacy-continuations
setup_rehearsal_fixture "$root" true none
legacy_source_trade_id="g2:$(printf '5%.0s' {1..64})"
current_source_trade_id="g2:$(printf '6%.0s' {1..64})"
spaced_source_trade_id="g2:$(printf '7%.0s' {1..64})"
version20_source_trade_id="g2:$(printf '8%.0s' {1..64})"
nested_source_trade_id="g2:$(printf '9%.0s' {1..64})"
decoy_inputs_source_trade_id="g2:$(printf 'a%.0s' {1..64})"
python3 - "$root/prediction-markets/gen/g557/paper_state.db" \
  "$legacy_source_trade_id" "$current_source_trade_id" "$spaced_source_trade_id" \
  "$version20_source_trade_id" "$nested_source_trade_id" "$decoy_inputs_source_trade_id" <<'PY'
import json, sqlite3, sys

database, legacy_id, current_id, spaced_id, version20_id, nested_id, decoy_inputs_id = sys.argv[1:]
legacy_configuration = {
    "active_watchlist_size": 100,
    "mode": "paper",
    "max_fill_price": "0.85",
    "min_fill_price": "0.15",
    "min_resolution_horizon_secs": 60,
    "max_resolution_horizon_secs": 172800,
    "price_impact_cap_bps": 100,
    "flip_human_approved": False,
    "kelly_fraction_above_default_human_approved": False,
    "kelly_fraction_override": None,
    "per_trade_cap": {"kind": "unlimited"},
    "slippage_rate": "0.01",
    "sizing_mode": {"kind": "dollar"},
    "sizing_dollar_usd": "25",
    "sizing_contracts": 1,
    "fill_mode": "clob_best_ask",
    "polymarket_fee_rate": "0.04",
}
assert len(legacy_configuration) == 17
financial_configuration = {
    "era": "financial15",
    **{key: value for key, value in legacy_configuration.items()
       if key not in {"fill_mode", "polymarket_fee_rate"}},
}
rows = (
    (legacy_id, "2" * 64,
     json.dumps({"version": 2, "source_trade_id": legacy_id,
                 "applied_configuration": legacy_configuration}, separators=(",", ":")), 1),
    (current_id, "3" * 64,
     json.dumps({"version": 3, "source_trade_id": current_id,
                 "applied_configuration": financial_configuration}, separators=(",", ":")), 2),
    # The same legacy shape serialized with JSON whitespace still counts.
    (spaced_id, "7" * 64,
     json.dumps({"version": 2, "source_trade_id": spaced_id,
                 "applied_configuration": legacy_configuration}, separators=(", ", ": ")), 3),
    # A version-20 row shares the substring "version":2 and must not count.
    (version20_id, "8" * 64,
     json.dumps({"version": 20, "source_trade_id": version20_id,
                 "applied_configuration": legacy_configuration}, separators=(",", ":")), 4),
    # A current row whose nested decision inputs carry "version":2 and "fill_mode" must not count.
    (nested_id, "9" * 64,
     json.dumps({"version": 3, "source_trade_id": nested_id,
                 "decision_inputs": {"version": 2, "fill_mode": "clob_best_ask"},
                 "applied_configuration": financial_configuration}, separators=(",", ":")), 5),
    # A legacy row whose unconstrained decision inputs mention "era" still counts: only the
    # applied configuration object decides.
    (decoy_inputs_id, "a" * 64,
     json.dumps({"version": 2, "source_trade_id": decoy_inputs_id,
                 "decision_inputs": {"era": "unrelated-evidence"},
                 "applied_configuration": legacy_configuration}, separators=(",", ":")), 6),
)
connection = sqlite3.connect(database)
for source_trade_id, revision, frozen, updated_at in rows:
    connection.execute(
        "insert into decision_pending values(?,?,?,?,?,?,?,?,?)",
        (source_trade_id, revision, "0x" + "5" * 40, updated_at, frozen, "{}",
         "terminal", "no_fill", updated_at),
    )
connection.commit()
connection.close()
PY
legacy_digest=$(printf '%s\n%s\n%s\n' "$legacy_source_trade_id" "$spaced_source_trade_id" \
  "$decoy_inputs_source_trade_id" | sort | sha256sum | awk '{print $1}')
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] ||
  fail "legacy-continuation census rehearsal did not pass: $output"
[[ "$output" == *"rehearsal copy legacy continuations: count=3 digest=$legacy_digest"* ]] ||
  fail "legacy-continuation census log is missing or incorrect: $output"
manifest_path=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["manifest_path"])' \
  "$root/rehearsal/evidence.json")
[[ $(grep -c '^legacy_continuations=' "$manifest_path") -eq 1 ]] &&
  grep -Fxq "legacy_continuations=3:$legacy_digest" "$manifest_path" ||
  fail "legacy-continuation result-manifest row is missing or incorrect"

# Scenario REHEARSAL-PATHS-UPDATE-01A
# Preconditions: a clean copied generation whose migration path update fails (#570).
# PASS: the harness stops with the fatal line before any child starts and never passes.
# FAIL: the child starts, the rehearsal passes, or the fatal line is missing.
root=$TEST_TMP/rehearsal-paths-update
setup_rehearsal_fixture "$root" true none
: > "$root/test-state/paths-update-error"
set +e
output=$(run_rehearsal_fixture "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *"FATAL: rehearsal copy migration paths were not updated"* ]] ||
  fail "failed migration path update did not stop the rehearsal: $output"
[[ "$output" != *REHEARSAL545_PASS* ]] || fail "rehearsal passed after a failed migration path update"
[[ ! -e "$root/rehearsal/copy/status.json" && ! -e "$root/test-state/paths-updated" ]] ||
  fail "rehearsal child or marker appeared after a failed migration path update"

# Scenarios REHEARSAL-ACCOUNTS-02A..02D
# Preconditions: both privileged censuses contain one identical off account; the publishable-only
# child reports a live block other than the exact authorization-denied stale/empty shape.
# PASS: every non-denied child shape fails even though the privileged censuses remain safe.
# FAIL: any non-denied child shape records REHEARSAL545_PASS.
for account_scenario in absent_live fresh_empty stale_nonempty fresh_nonempty; do
  root=$TEST_TMP/rehearsal-account-$account_scenario
  setup_rehearsal_fixture "$root" true none "$account_scenario"
  set +e
  output=$(run_rehearsal_fixture "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *REHEARSAL545_FAIL* ]] ||
    fail "$account_scenario rehearsal account evidence was not refused: $output"
  python3 -c 'import json,sys
e=json.load(open(sys.argv[1],encoding="utf-8")); assert e["result"]=="FAIL"
rows=dict(line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8"))
assert rows["reason"]=="unsafe_child_account_evidence"
assert rows["account_census_before_count"]=="1"
assert rows["account_census_after_count"]=="1"
assert rows["account_census_before_safe"]=="true"
assert rows["account_census_after_safe"]=="true"
assert rows["account_census_before_after_identical"]=="true"' \
    "$root/rehearsal/evidence.json" ||
    fail "$account_scenario refusal did not bind both privileged account censuses"
done

# Scenarios REHEARSAL-CENSUS-02E..02H
# Preconditions: the publishable-only child emits the authentic stale/empty status shape. The psql
# shim answers an initial and a post-quiescence census with the selected rows.
# PASS: identical safe zero/nonzero censuses pass; a live_tiny transition or any safe-but-different
# final census fails and binds both observations plus `before_after_identical=false`.
# FAIL: an identical safe census is refused or either changed census records REHEARSAL545_PASS.
for census_scenario in zero_identical nonzero_identical; do
  root=$TEST_TMP/rehearsal-census-$census_scenario
  setup_rehearsal_fixture "$root" true none authorization_denied "$census_scenario"
  output=$(run_rehearsal_fixture "$root" 2>&1)
  [[ "$output" == *REHEARSAL545_PASS* ]] ||
    fail "$census_scenario rehearsal census did not pass: $output"
  python3 -c 'import json,sys
e=json.load(open(sys.argv[1],encoding="utf-8")); assert e["result"]=="PASS"
rows=dict(line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8"))
assert rows["account_census_before_count"]==sys.argv[2]
assert rows["account_census_after_count"]==sys.argv[2]
assert rows["account_census_before_safe"]=="true"
assert rows["account_census_after_safe"]=="true"
assert rows["account_census_before_after_identical"]=="true"' \
    "$root/rehearsal/evidence.json" "$([[ "$census_scenario" == zero_identical ]] && echo 0 || echo 1)" ||
    fail "$census_scenario PASS did not bind identical safe censuses"
done

for census_scenario in live_tiny_after changed_after; do
  root=$TEST_TMP/rehearsal-census-$census_scenario
  setup_rehearsal_fixture "$root" true none authorization_denied "$census_scenario"
  set +e
  output=$(run_rehearsal_fixture "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *REHEARSAL545_FAIL* ]] ||
    fail "$census_scenario rehearsal census was not refused: $output"
  python3 -c 'import json,sys
e=json.load(open(sys.argv[1],encoding="utf-8")); assert e["result"]=="FAIL"
rows=dict(line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8"))
assert rows["reason"]=="unsafe_account_census"
assert rows["account_census_before_count"]=="1"
assert rows["account_census_before_safe"]=="true"
assert rows["account_census_after_safe"]==sys.argv[2]
assert rows["account_census_before_after_identical"]=="false"' \
    "$root/rehearsal/evidence.json" \
    "$([[ "$census_scenario" == live_tiny_after ]] && echo false || echo true)" ||
    fail "$census_scenario refusal did not bind the changed privileged census"
done

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
# This is an environment-identity fixture: its fake child emits source_health in both websocket
# modes and deliberately does not model the real websocket-disabled status shape (#586).
manifest_path=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["manifest_path"])' \
  "$root/rehearsal/evidence.json")
grep -Eq '^final=.*credit_loss=0([[:space:]]|$)' "$manifest_path" ||
  fail "websocket-disabled rehearsal did not retain zero observed credit loss"
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

# Scenarios REHEARSAL-SOCKET-RECYCLE-09A (two socket paths),
# REHEARSAL-WITHIN-RUN-LOSS-09E, and REHEARSAL-OBLIGATIONS-DROPPED-09B (#586).
for recycle_path in buffered_frame control_frame; do
  root="$TEST_TMP/rehearsal-recycle-$recycle_path"
  setup_rehearsal_fixture "$root" true recycle
  if [[ "$recycle_path" == control_frame ]]; then
    printf '%s\n' '{"level":"WARN","message":"activity ws reader produced no normalized activity row; dropping socket","slot":1,"timeout_secs":30,"last_wire_frame_age_secs":"Some(1)","last_normalized_activity_age_secs":"Some(31)","buffered_frame_processed":false}' \
      > "$root/test-state/recycle-log-line"
  fi
  output=$(run_rehearsal_fixture "$root" 2>&1)
  [[ "$output" == *REHEARSAL545_PASS* ]] || fail "$recycle_path socket recycle did not pass: $output"
  [[ $(<"$root/rehearsal/drops-1111111.state") == '1 0' &&
     $(<"$root/rehearsal/status-1111111.state") == '1 1 1 1 1 0' ]] ||
    fail "$recycle_path socket recycle did not preserve the two observer contracts"
  [[ $(<"$root/test-state/rehearsal-sigint-count") == 1 ]] ||
    fail "$recycle_path socket recycle did not handle SIGINT exactly once"
  python3 - "$root/rehearsal/evidence.json" "$root/rehearsal/copy/status.json" \
    "$REPOSITORY_HARNESS_BUNDLE_SHA256" <<'PY' || fail "socket recycle evidence bindings are incomplete"
import datetime, hashlib, json, re, sys
evidence_path, status_path, expected_bundle = sys.argv[1:]
evidence=json.load(open(evidence_path,encoding="utf-8"))
raw=[line.rstrip("\n").split("=",1) for line in open(evidence["manifest_path"],encoding="utf-8")]
rows=dict(raw)
assert "drops=1" in rows["final"] and "credit_loss=0" in rows["final"]
assert rows["harness_bundle_sha256"] == expected_bundle
assert rows["unit_kill_signal"] == "2" and rows["unit_timeout_stop_secs"] == "5"
assert rows["shutdown_signal_unix"].isdigit() and rows["shutdown_elapsed_secs"].isdigit()
assert rows["final_status_sha256"] == hashlib.sha256(open(status_path,"rb").read()).hexdigest()
status=json.load(open(status_path,encoding="utf-8"))
updated=datetime.datetime.fromisoformat(status["updated_at"].replace("Z","+00:00")).timestamp()
assert updated >= int(rows["shutdown_signal_unix"])
for key in ("harness_bundle_sha256","final_status_sha256","unit_kill_signal",
            "unit_timeout_stop_secs","shutdown_signal_unix","shutdown_elapsed_secs"):
    assert sum(name == key for name,_ in raw) == 1
PY
done

# The high-bit row is a legal u64 above Bash's signed range (#586 review): the harness compares the
# canonical decimal text, so it must still refuse.
for loss_case in "within_run_loss 1" "within_run_loss_highbit 9223372036854775808"; do
set -- $loss_case; loss_injection=$1; loss_value=$2
root=$TEST_TMP/rehearsal-$loss_injection
setup_rehearsal_fixture "$root" true "$loss_injection"
set +e
output=$(run_rehearsal_fixture "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *"REHEARSAL545_FAIL reason=unsafe_evidence"* ]] ||
  fail "$loss_injection obligation loss was not refused: $output"
[[ $(<"$root/rehearsal/status-1111111.state") == "1 1 1 1 1 $loss_value" &&
   ! -e "$root/test-state/rehearsal-sigint-count" ]] ||
  fail "$loss_injection obligation loss did not abort before SIGINT"
python3 - "$root/rehearsal/evidence.json" "$root/rehearsal/copy/status.json" <<'PY' || fail "within-run loss did not bind the preserved status"
import hashlib,json,sys
e=json.load(open(sys.argv[1],encoding="utf-8"))
raw=[line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8")]
rows=dict(raw)
assert rows["reason"] == "unsafe_evidence"
assert rows["final_status_sha256"] == hashlib.sha256(open(sys.argv[2],"rb").read()).hexdigest()
for key in ("unit_kill_signal","unit_timeout_stop_secs","shutdown_signal_unix","shutdown_elapsed_secs"):
    assert sum(name == key for name,_ in raw) == 1 and rows[key] == "absent"
PY
done

for loss_case in "obligations_dropped 1" "obligations_dropped_highbit 9223372036854775808"; do
set -- $loss_case; loss_injection=$1; loss_value=$2
root=$TEST_TMP/rehearsal-final-$loss_injection
setup_rehearsal_fixture "$root" true "$loss_injection"
set +e
output=$(run_rehearsal_fixture "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *"REHEARSAL545_FAIL reason=unsafe_evidence"* ]] ||
  fail "$loss_injection final obligation loss was not refused: $output"
[[ $(<"$root/rehearsal/status-1111111.state") == '1 1 1 1 1 0' &&
   $(<"$root/test-state/rehearsal-sigint-count") == 1 ]] ||
  fail "final obligation-loss fixture did not preserve its within-run sample and SIGINT proof"
python3 - "$root/rehearsal/evidence.json" "$root/rehearsal/copy/status.json" "$loss_value" <<'PY' || fail "final obligation loss did not bind the authoritative final status"
import hashlib,json,sys
e=json.load(open(sys.argv[1],encoding="utf-8")); rows=dict(
    line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8"))
assert f"credit_loss={sys.argv[3]}" in rows["final"], rows["final"]
assert rows["final_status_sha256"] == hashlib.sha256(open(sys.argv[2],"rb").read()).hexdigest()
PY
done

# Scenario REHEARSAL-FINAL-STATUS-INVALID-09C: every malformed, stale, unsafe, or non-final shape
# refuses PASS independently of the preserved-file digest (#586).
final_status_cases=(
  missing_file invalid_json stale_updated_at same_second_stale no_stopping_marker
  missing_source_health missing_counter negative_counter boolean_counter string_counter
  poisoned_final poison_flag_missing poison_flag_non_boolean failed_critical_owner
  critical_owner_still_running
)
for final_case in "${final_status_cases[@]}"; do
  root="$TEST_TMP/rehearsal-final-status-$final_case"
  setup_rehearsal_fixture "$root" true "$final_case"
  set +e
  output=$(run_rehearsal_fixture "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *"REHEARSAL545_FAIL reason=final_status_observation_failed"* ]] ||
    fail "$final_case final status was not refused: $output"
  python3 - "$root/rehearsal/evidence.json" "$root/rehearsal/copy/status.json" \
    "$final_case" <<'PY' || fail "$final_case final-status digest binding is incorrect"
import hashlib,json,os,sys
evidence_path,status_path,case=sys.argv[1:]
e=json.load(open(evidence_path,encoding="utf-8"))
raw=[line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8")]
rows=dict(raw)
assert rows["reason"] == "final_status_observation_failed"
assert sum(name == "final_status_sha256" for name,_ in raw) == 1
if case == "missing_file":
    assert not os.path.exists(status_path) and rows["final_status_sha256"] == "absent"
else:
    assert rows["final_status_sha256"] == hashlib.sha256(open(status_path,"rb").read()).hexdigest()
PY
done

# Scenario REHEARSAL-SHUTDOWN-INCOMPLETE-09D: timeout, nonzero exit, and post-bound clean exit all
# refuse before final-status validation, while cleanup leaves no child behind (#586).
for shutdown_case in ignore_sigint exit_nonzero_on_sigint delayed_clean_exit delayed_clean_exit_just_over; do
  root="$TEST_TMP/rehearsal-shutdown-$shutdown_case"
  setup_rehearsal_fixture "$root" true "$shutdown_case"
  set +e
  output=$(run_rehearsal_fixture "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *"REHEARSAL545_FAIL reason=service_shutdown_incomplete"* &&
     "$output" != *REHEARSAL545_PASS* ]] ||
    fail "$shutdown_case incomplete shutdown was not refused: $output"
  child_pid=$(python3 -c 'import json,sys
e=json.load(open(sys.argv[1],encoding="utf-8")); rows=dict(
 line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8")); print(rows["service_invocation_pid"])' \
    "$root/rehearsal/evidence.json")
  [[ ! -e "$root/proc/$child_pid" || ! -d "/proc/$child_pid" ]] ||
    fail "$shutdown_case rehearsal child survived cleanup"
  python3 - "$root/rehearsal/evidence.json" "$root/rehearsal/copy/status.json" <<'PY' || fail "incomplete shutdown evidence bindings are incomplete"
import hashlib,json,sys
e=json.load(open(sys.argv[1],encoding="utf-8"))
raw=[line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8")]
rows=dict(raw)
assert rows["unit_kill_signal"] == "2" and rows["unit_timeout_stop_secs"] == "5"
assert rows["shutdown_elapsed_secs"].isdigit()
assert sum(name == "final_status_sha256" for name,_ in raw) == 1
assert rows["final_status_sha256"] == hashlib.sha256(open(sys.argv[2],"rb").read()).hexdigest()
PY
done

# Scenario REHEARSAL-SHUTDOWN-INSIDE-BOUND-09H: a clean exit just inside the unit timeout still passes,
# and the recorded elapsed seconds are the floor of the measured shutdown (#586 branch review).
root="$TEST_TMP/rehearsal-shutdown-inside-bound"
setup_rehearsal_fixture "$root" true clean_exit_inside_bound
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] || fail "clean exit inside the bound did not pass: $output"
python3 - "$root/rehearsal/evidence.json" <<'PY' || fail "inside-bound shutdown evidence is wrong"
import json,sys
e=json.load(open(sys.argv[1],encoding="utf-8"))
rows=dict(line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8"))
assert rows["shutdown_elapsed_secs"] == "4", rows["shutdown_elapsed_secs"]
assert rows["unit_timeout_stop_secs"] == "5"
PY

# Scenario REHEARSAL-OBSERVER-JOIN-09I: an observer stalled inside its sqlite3 helper across quiescence
# is reaped with its process group before SIGINT, and its state file is not written after the signal
# instant (#586 branch review).
root="$TEST_TMP/rehearsal-observer-join"
setup_rehearsal_fixture "$root" true slow_observer
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] || fail "slow observer run did not pass: $output"
[[ -f "$root/test-state/slow-sqlite.pid" ]] || fail "slow sqlite3 helper never ran"
helper_pid=$(<"$root/test-state/slow-sqlite.pid")
[[ ! -d "/proc/$helper_pid" ]] || fail "observer helper $helper_pid survived quiescence"
python3 - "$root/rehearsal/evidence.json" "$root/rehearsal" <<'PY' || fail "observer state was written after the signal instant"
import glob,json,os,sys
e=json.load(open(sys.argv[1],encoding="utf-8"))
rows=dict(line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8"))
signal_unix=int(rows["shutdown_signal_unix"])
states=glob.glob(os.path.join(sys.argv[2],"fences-*.state"))
assert states, "no fence state file"
for path in states:
    assert int(os.stat(path).st_mtime) <= signal_unix, (path, os.stat(path).st_mtime, signal_unix)
PY

# Scenario REHEARSAL-STALE-QUIESCE-FLAG-09J: a quiesce flag left by an earlier run in the same root is
# cleared at startup, so a same-revision rerun still reaches PASS (#586 branch review).
root="$TEST_TMP/rehearsal-stale-quiesce-flag"
setup_rehearsal_fixture "$root" true none
mkdir -p "$root/rehearsal"; : > "$root/rehearsal/quiesce-1111111.flag"
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] || fail "stale quiesce flag blocked a rerun: $output"

# Scenario REHEARSAL-UNIT-STOP-POLICY-09F: invalid production stop policies refuse before SIGINT.
for policy_case in wrong_signal missing_timeout malformed_timeout zero_timeout; do
  root="$TEST_TMP/rehearsal-unit-policy-$policy_case"
  setup_rehearsal_fixture "$root" true none
  case "$policy_case" in
    wrong_signal) printf '%s\n' 'KillSignal=15' 'TimeoutStopUSec=5s' > "$root/test-state/unit-stop-policy" ;;
    missing_timeout) printf '%s\n' 'KillSignal=2' > "$root/test-state/unit-stop-policy" ;;
    malformed_timeout) printf '%s\n' 'KillSignal=2' 'TimeoutStopUSec=garbage' > "$root/test-state/unit-stop-policy" ;;
    zero_timeout) printf '%s\n' 'KillSignal=2' 'TimeoutStopUSec=0' > "$root/test-state/unit-stop-policy" ;;
  esac
  set +e
  output=$(run_rehearsal_fixture "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *"REHEARSAL545_FAIL reason=unit_stop_policy_mismatch"* &&
     ! -e "$root/test-state/rehearsal-sigint-count" ]] ||
    fail "$policy_case unit stop policy was not refused before SIGINT: $output"
  python3 - "$root/rehearsal/evidence.json" "$policy_case" <<'PY' || fail "$policy_case unit-policy evidence values are incorrect"
import json,sys
e=json.load(open(sys.argv[1],encoding="utf-8")); case=sys.argv[2]
raw=[line.rstrip("\n").split("=",1) for line in open(e["manifest_path"],encoding="utf-8")]
rows=dict(raw)
expected={"wrong_signal":("15","5"),"missing_timeout":("2","absent"),
          "malformed_timeout":("2","absent"),"zero_timeout":("2","0")}[case]
assert (rows["unit_kill_signal"],rows["unit_timeout_stop_secs"]) == expected
for key in ("unit_kill_signal","unit_timeout_stop_secs","shutdown_signal_unix","shutdown_elapsed_secs"):
    assert sum(name == key for name,_ in raw) == 1
assert rows["shutdown_signal_unix"] == "absent" and rows["shutdown_elapsed_secs"] == "absent"
PY
done

# Scenario REHEARSAL-RELEASE-ROOT-09G: production evidence is tree-local, while dry-run remains
# deliberately path-independent (#586).
root=$TEST_TMP/rehearsal-release-root
mkdir -p "$root/divergent-release"
set +e
output=$(env -u PE_ACTIVATION_TESTING PE_REHEARSAL_ROOT="$root/rehearsal" \
  PE_REHEARSAL_RELEASE_ROOT="$root/divergent-release" "$REHEARSAL" \
  --target-config "$root/missing.toml" --target-environment "$root/missing.env" \
  1111111111111111111111111111111111111111 2>&1)
status=$?
set -e
[[ $status -eq 1 && "$output" == *"REHEARSAL545_FAIL reason=release_root_mismatch"* &&
   ! -e "$root/rehearsal" ]] || fail "divergent production release root was not refused before copy: $output"
output=$(env -u PE_ACTIVATION_TESTING PE_REHEARSAL_RELEASE_ROOT="$root/divergent-release" \
  "$REHEARSAL" --dry-run 1111111111111111111111111111111111111111 2>&1)
[[ "$output" == *REHEARSAL545_DRY_RUN=1* ]] ||
  fail "dry-run became dependent on the release-root production guard: $output"

# Scenario ENV-DATA-05
# Preconditions: the reviewed target environment contains a shell command that reads the inherited
# database authority. PASS: both rehearsal and activation fail at that physical line and no command
# executes. FAIL: the credential is written or either workflow crosses its first durable boundary.
root=$TEST_TMP/environment-data
setup_rehearsal_fixture "$root" true none
printf 'printf '\''%%s\\n'\'' "$SUPABASE_DB_URL" > %s\n' "$root/exfiltrated-url" \
  >> "$root/target/service.env"
set +e
output=$(run_rehearsal_fixture "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'service.env:7:'* ]] ||
  fail "rehearsal did not fail closed on shell syntax: $output"
[[ ! -e "$root/exfiltrated-url" ]] || fail "rehearsal executed the target environment"
driver_args "$root"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'service.env:7:'* ]] ||
  fail "financial driver did not fail closed on shell syntax: $output"
[[ ! -e "$root/exfiltrated-url" && ! -e "$root/pe-financial-era.json" ]] ||
  fail "target environment shell syntax crossed the financial prepared boundary"

# Scenario ENV-DB-AUTHORITY-06
# Preconditions: a syntactically valid target entry names an attacker database URL.
# PASS: rehearsal and the complete driver still reach every psql call through the inherited URL's
# parsed PG* values. FAIL: sourcing the target redirects either workflow or prevents convergence.
root=$TEST_TMP/environment-db-authority
setup_rehearsal_fixture "$root" true none
printf "%s\n" "SUPABASE_DB_URL='postgresql://attacker/other'" >> "$root/target/service.env"
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* ]] || fail "target database override redirected rehearsal: $output"
write_shims "$root"
driver_args "$root"
drive_to_verified "$root" || fail "target database override redirected the financial driver"

# Scenario FE-OFFLINE-ENV-ALLOWLIST-06A
# Preconditions: the reviewed target carries one service setting plus unrelated and non-forbidden
# LD-prefixed assignments. PASS: rollback-check, prepare, and Start receive only names selected by the
# shared service allowlist (plus the driver's execution controls), and receive no LD-prefixed name.
# FAIL: any target-file unrequested or LD-prefixed assignment reaches an offline target execution.
root=$TEST_TMP/offline-environment-allowlist
setup_fixture "$root"
printf '%s\n' 'PE_MODE=paper' 'UNREQUESTED_OFFLINE=hidden' 'LD_DEBUG=libs' \
  >> "$root/target/service.env"
write_rehearsal_evidence "$root"
driver_args "$root"
run_driver "$root" >/dev/null
mapfile -t shared_service_env_allowlist < <(
  bash -c 'source "$1"; printf "%s\n" "${SERVICE_ENV_ALLOWLIST[@]}"' bash "$COMMON"
)
for operation in rollback-check prepare start; do
  environment_dump="$root/test-state/offline-$operation.environment"
  [[ -s "$environment_dump" ]] || fail "$operation did not dump its offline environment"
  if ! python3 - "$environment_dump" "${shared_service_env_allowlist[@]}" <<'PY'
import sys

path, *allowlisted = sys.argv[1:]
parts = [part for part in open(path, "rb").read().split(b"\0") if part]
environment = dict(part.split(b"=", 1) for part in parts)
names = {name.decode("ascii") for name in environment}
controls = {
    "PATH", "HOME", "PE_ACTIVATION_TESTING", "PE_ACTIVATION_TEST_ROOT",
    "LC_CTYPE", "PWD", "SHLVL", "OLDPWD", "_",
}
assert names - controls <= set(allowlisted), sorted(names - controls - set(allowlisted))
assert environment[b"PE_MODE"] == b"paper"
assert environment[b"PE_SUPABASE_SECRET_KEY"] == b"sb_secret_test_service_role"
assert b"UNREQUESTED_OFFLINE" not in environment
assert not any(name.startswith("LD_") for name in names), sorted(names)
PY
  then
    fail "$operation offline environment escaped the shared service allowlist"
  fi
done

# Scenario FE-LOADER-ENV-REFUSAL-06B
# Preconditions: an otherwise reviewed production environment assigns LD_AUDIT.
# PASS: the physical line is rejected before a financial manifest or service stop exists.
# FAIL: a loader-audit control reaches prepared or any mutation boundary.
root=$TEST_TMP/loader-environment-refusal
setup_fixture "$root"
printf '%s\n' 'LD_AUDIT=/tmp/review-audit.so' >> "$root/target/service.env"
driver_args "$root"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'service.env:6: forbidden environment assignment: LD_AUDIT'* ]] ||
  fail "LD_AUDIT target assignment was not explicitly refused: $output"
[[ ! -e "$root/pe-financial-era.json" && ! -e "$root/test-state/stop-count" ]] ||
  fail "LD_AUDIT target assignment crossed the pre-manifest mutation boundary"

# Scenario FE-PREFLIGHT-65 — a status that is already dirty costs no downtime (#618).
# Preconditions: `live.stale` is true before the driver runs.
# PASS: the pre-stop preflight refuses; the service was never stopped and no stop intent was
#       recorded, so production is exactly where it started.
# FAIL: the driver stops the service to discover what it could have read first.
# Scope: the harness rollback-check at driver:724 does not read status.json, so this isolates the
# new pre-stop preflight. Protection against a status that goes bad AFTER that earlier check is
# what FE-PREFLIGHT-66 proves.
root=$TEST_TMP/preflight-dirty-before-stop
setup_fixture "$root"
python3 -c 'import json,sys
path=sys.argv[1]; value=json.load(open(path)); value["live"]["stale"]=True
json.dump(value,open(path,"w"))' "$root/prediction-markets/gen/g557/status.json"
driver_args "$root"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'preflight refused before the stop'* ]] ||
  fail "a dirty status did not refuse at the pre-stop preflight: $output"
[[ ! -e "$root/test-state/stop-count" ]] ||
  fail "the driver stopped production to learn what the preflight already knew"
[[ $(<"$root/test-state/preflight-count") == 1 ]] ||
  fail "expected exactly one preflight observation"
python3 -c 'import json,sys
v=json.load(open(sys.argv[1]))
raise SystemExit(0 if not v.get("service_stop_intent") and not v.get("stop_invoked") else 1)' \
  "$root/pe-financial-era.json" ||
  fail "a pre-stop preflight refusal recorded a stop boundary"
echo "PASS: FE-PREFLIGHT-65"

# Scenario FE-SEAM-68 — a production-shaped preparation now crosses the driver plumbing (#626).
# Preconditions: the prepare shim emits the PRODUCTION shape — a serialized MembershipProofBinding
#       carrying each member proof document — instead of the miniature fixture the other scenarios
#       use. Sizes come from the measured live generation (mean validation proof 341,056 B); the
#       live 26-member binding was 21,711,795 B against a 131,072-byte MAX_ARG_STRLEN.
# PASS: the driver RECORDS the preparation. Before the stdin transport it could not: the whole
#       payload went as one argv element and failed E2BIG at activate_financial_era.sh:945 —
#       immediately after the ~95-minute post-stop prepare had already succeeded, with production
#       already stopped and rollback forbidden.
# FAIL: the preparation is unrecorded, or the run dies with "Argument list too long" — the
#       transport regressed to argv.
# Scope: transport only. This scenario says nothing about proof or projection content.
root=$TEST_TMP/seam-production-shaped-preparation
setup_fixture "$root"
driver_args "$root"
set +e
printf '3 341056\n' > "$root/test-state/production-shaped-preparation"
output=$(drive_to_verified "$root" 2>&1)
set -e
[[ "$output" != *'Argument list too long'* ]] ||
  fail "a production-shaped preparation still hit the argv limit; the transport regressed: $output"
recorded=$(python3 -c 'import json,sys
try: print("yes" if json.load(open(sys.argv[1])).get("preparation") is not None else "no")
except FileNotFoundError: print("absent")' "$root/pe-financial-era.json")
[[ "$recorded" == "yes" ]] ||
  fail "a production-shaped preparation was not recorded ($recorded); the transport does not carry \
the real payload"
# Prove it carried the WHOLE payload, not a truncated one: the recorded binding must still be at
# least the generated size. A transport that silently truncated would satisfy "recorded".
carried=$(python3 -c 'import json,sys
v=json.load(open(sys.argv[1]))["preparation"]
print(len(json.dumps(v,separators=(",",":"))))' "$root/pe-financial-era.json")
[[ "$carried" -ge 1000000 ]] ||
  fail "the recorded preparation is only $carried bytes; the payload was truncated in transit"

# Control: the SAME generator at a size UNDER the old limit must also record, so the oversized case
# is evidence about SIZE rather than about the generator working at all.
control=$TEST_TMP/seam-undersized-control
setup_fixture "$control"
driver_args "$control"
printf '1 1024\n' > "$control/test-state/production-shaped-preparation"
drive_to_verified "$control" >/dev/null 2>&1 || true
control_recorded=$(python3 -c 'import json,sys
try: print("yes" if json.load(open(sys.argv[1])).get("preparation") is not None else "no")
except FileNotFoundError: print("absent")' "$control/pe-financial-era.json")
[[ "$control_recorded" == "yes" ]] ||
  fail "the seam generator cannot produce a recordable preparation even when small (got \
$control_recorded), so the oversized case proves nothing about SIZE"
echo "PASS: FE-SEAM-68 (production-shaped preparation recorded, $carried bytes carried via stdin)"

# Scenario FE-PREFLIGHT-66 — the decisive one: clean before the stop, dirty because of it (#618).
# Preconditions: the status is clean when the earlier rollback-check and the pre-stop preflight read
#   it; the shutdown write then records `stale: true` — the #545 attempt-15 shape.
# PASS: the post-stop preflight refuses immediately; the stop receipts stand, but no legacy contract
#       check, no backup and no integrity check ever ran.
# FAIL: the refusal waits until preparation, an hour of downtime later.
root=$TEST_TMP/preflight-dirtied-by-shutdown
setup_fixture "$root"
: > "$root/test-state/stale-status-on-stop"
driver_args "$root"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'preflight refused immediately after the stop'* ]] ||
  fail "a shutdown-dirtied status was not caught by the post-stop preflight: $output"
[[ "$output" == *'preflight pre-stop:'* ]] ||
  fail "the pre-stop observation should have passed on a clean status"
[[ $(<"$root/test-state/preflight-count") == 2 ]] ||
  fail "expected a pre-stop and a post-stop observation"
[[ $(<"$root/test-state/stop-count") == 1 ]] ||
  fail "the service should have been stopped exactly once"
compgen -G "$root/prediction-markets/financial-era-*-paper-state.db" > /dev/null &&
  fail "a backup was started after the post-stop preflight refused"
compgen -G "$root/prediction-markets/financial-era-*-paper-state.db.tmp.*" > /dev/null &&
  fail "a partial backup was started after the post-stop preflight refused"
[[ $(<"$root/test-state/legacy-contract-count") == 1 ]] ||
  fail "the post-stop remote contract check ran despite the preflight refusal"
python3 -c 'import json,sys
v=json.load(open(sys.argv[1]))
missing=[k for k in ("service_stop_intent","stop_invoked") if not v.get(k)]
if missing: raise SystemExit("stop receipts were not preserved: %s" % missing)
if v.get("legacy_contract_verified"): raise SystemExit("the remote contract check ran anyway")
if v.get("backup"): raise SystemExit("a backup receipt was recorded")' \
  "$root/pe-financial-era.json" ||
  fail "the post-stop refusal left the wrong manifest state"
echo "PASS: FE-PREFLIGHT-66"

# Scenario FE-PREFLIGHT-67 — an already-stopped entry is gated exactly like a forward one (#628).
# Preconditions: the service is already inactive at entry, so the pre-stop arm never runs. Under
#   #618 this invocation performed NO preflight at all and walked into the backup ungated; the rows
#   export was the only reason, and it is idempotent, so it is recreated instead.
# PASS: the run converges having performed exactly ONE preflight — the post-stop one — before the
#   backup, and without stopping a service that was already inactive.
# FAIL: no preflight (the #618 hole), or a stop of an already-inert service.
root=$TEST_TMP/preflight-inactive-entry
setup_fixture "$root"
echo false > "$root/test-state/service.active"
driver_args "$root"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -eq 0 ]] ||
  fail "an initially inactive entry did not converge: $output"
[[ $(<"$root/test-state/preflight-count") == 1 ]] ||
  fail "an already-stopped entry must carry exactly one current preflight before the backup"
[[ ! -e "$root/test-state/stop-count" ]] ||
  fail "the driver stopped a service that was already inert"
echo "PASS: FE-PREFLIGHT-67"

# Scenario REHEARSAL-REVIEWED-BYTES-07
# Preconditions: the target binary is copied, then preflight atomically replaces its original path.
# PASS: the private reviewed copy runs to PASS and the replacement never executes.
# FAIL: the post-validation target path remains the execution authority.
root=$TEST_TMP/rehearsal-reviewed-bytes
setup_rehearsal_fixture "$root" true none
reviewed_sha=$(sha256sum "$root/target/pe-service" | awk '{print $1}')
printf '%s\n' '#!/usr/bin/env bash' ": > '$root/replacement-executed'" 'exit 97' \
  > "$root/target/pe-service.next"
chmod +x "$root/target/pe-service.next"
touch "$root/test-state/replace-rehearsal-target-binary"
output=$(run_rehearsal_fixture "$root" 2>&1)
[[ "$output" == *REHEARSAL545_PASS* && ! -e "$root/replacement-executed" ]] ||
  fail "rehearsal did not execute its private reviewed binary: $output"
python3 -c 'import hashlib,json,sys
e=json.load(open(sys.argv[1],encoding="utf-8"))
assert e["artifact_sha256"] == sys.argv[2]
assert hashlib.sha256(open(sys.argv[3],"rb").read()).hexdigest() != sys.argv[2]' \
  "$root/rehearsal/evidence.json" "$reviewed_sha" "$root/target/pe-service" ||
  fail "rehearsal evidence did not bind the copied binary"

# Scenario REHEARSAL-PRIVATE-BINARY-07A
# Preconditions: privileged preflight replaces the private copied binary after identity validation.
# PASS: the final binary rehash refuses before service execution and the replacement marker is absent.
# FAIL: the replacement executes or the rehearsal records PASS for different bytes.
root=$TEST_TMP/rehearsal-private-binary
setup_rehearsal_fixture "$root" true none
printf '%s\n' '#!/usr/bin/env bash' ": > '$root/private-replacement-executed'" 'exit 97' \
  > "$root/target/pe-service.next"
chmod +x "$root/target/pe-service.next"
touch "$root/test-state/replace-rehearsal-private-binary"
set +e
output=$(run_rehearsal_fixture "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'rehearsal binary changed while constructing the service environment'* ]] ||
  fail "rehearsal did not refuse private binary drift: $output"
[[ ! -e "$root/private-replacement-executed" ]] ||
  fail "rehearsal executed the drifted private binary"

# Scenario FE-ADOPT-REVIEWED-BYTES-08
# Preconditions: the target process atomically replaces its source path after Start but before adopt.
# PASS: digest-bound adoption refuses without replacing the installed service binary.
# FAIL: the replacement becomes the installed executable before the later verification catches it.
root=$TEST_TMP/financial-reviewed-bytes
setup_fixture "$root"
driver_args "$root"
installed_before=$(sha256sum "$root/prediction-markets/target/release/pe-service" | awk '{print $1}')
printf '%s\n' '#!/usr/bin/env bash' 'exit 97' > "$root/target/pe-service.next"
chmod +x "$root/target/pe-service.next"
touch "$root/test-state/replace-target-binary-after-start"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'source hash changed before adoption'* ]] ||
  fail "financial driver did not refuse replaced target bytes: $output"
[[ "$installed_before" == "$(sha256sum "$root/prediction-markets/target/release/pe-service" | awk '{print $1}')" ]] ||
  fail "financial driver replaced the service binary before detecting target drift"
python3 -c 'import json,sys
value=json.load(open(sys.argv[1],encoding="utf-8"))
assert value.get("target_binary_adopted") is not True' "$root/pe-financial-era.json" ||
  fail "financial driver receipted a refused binary adoption"

# Scenario CREDENTIAL-PRIVATE-09
# Preconditions: complete rehearsal and activation paths use sentinel database and service-role
# credentials under bash -x. PASS: both converge while repeated /proc cmdline sweeps and the complete
# trace contain neither sentinel. FAIL: any validator, fd bridge, or child environment uses argv.
SENTINEL_DATABASE_URL='postgresql://harness:harness@127.0.0.1:1/harness?application_name=SENTINEL_URL_545'
SENTINEL_SECRET_KEY='sb_secret_SENTINEL_545'
root=$TEST_TMP/credential-private
setup_rehearsal_fixture "$root" true none
sed -i "s/sb_secret_test_service_role/$SENTINEL_SECRET_KEY/" "$root/target/service.env"
trace="$root/complete.xtrace"
output="$root/complete.output"
: > "$trace"
: > "$output"
run_with_proc_sweep "$output" "$trace" run_rehearsal_traced "$root" ||
  fail "sentinel rehearsal did not complete"
grep -q REHEARSAL545_PASS "$output" || fail "sentinel rehearsal did not pass"
write_shims "$root"
driver_args "$root"
run_with_proc_sweep "$output" "$trace" run_driver_traced "$root" ||
  fail "sentinel financial activation did not reach started"
touch "$root/prediction-markets/gen/g557/status.json"
run_with_proc_sweep "$output" "$trace" run_driver_traced "$root" ||
  fail "sentinel financial activation did not reach verified"
[[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' \
  "$root/pe-financial-era.json") == verified ]] || fail "sentinel financial activation did not verify"
assert_trace_has_no_sentinels "$trace"

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

# Scenario FE-SEPARATE-GENERATION-TARGET-00C
# Preconditions: the authentic #557 staged paths/commit differ from the reviewed #545 target.
# PASS: prepared binds the old installed bytes to #557 and records the target independently.
# FAIL: the target must equal #557, or the installed old files are not proven from #557.
root=$TEST_TMP/separate-identities
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "distinct #557 and #545 identities did not reach prepared"
python3 -c 'import json,sys
activation=json.load(open(sys.argv[1])); financial=json.load(open(sys.argv[2]))
assert activation["merge_commit"] == "5"*40
assert financial["generation_merge_commit"] == activation["merge_commit"]
assert financial["target_revision"] == "1"*40
assert activation["artifacts"]["binary"]["path"] != sys.argv[3]
assert financial["old_artifact_sha256"] == activation["artifacts"]["binary"]["sha256"]
assert financial["old_artifact_sha256"] != financial["target_artifact_sha256"]
assert financial["generation_source_v1_main"] == activation["source_v1_main"]
assert financial["generation_legacy_history"] == activation["legacy_history"]' \
  "$root/pe-activation.json" "$root/pe-financial-era.json" "$root/target/pe-service" ||
  fail "financial manifest conflated the inherited generation and reviewed target"

# Scenario FE-TARGET-SERVICE-ROLE-00E
# Preconditions: the reviewed production target puts its publishable key in the secret slot.
# PASS: the driver rejects the key class before creating a manifest or stopping the service.
# FAIL: a publishable production authority reaches Prepared or a Start-capable transition.
root=$TEST_TMP/target-publishable-secret
setup_fixture "$root"
sed -i 's/PE_SUPABASE_SECRET_KEY=sb_secret_test_service_role/PE_SUPABASE_SECRET_KEY=sb_publishable_rehearsal/' \
  "$root/target/service.env"
driver_args "$root"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'reviewed production target secret slot is not secret/service-role class'* ]] ||
  fail "publishable production secret slot was not refused: $output"
[[ ! -e "$root/pe-financial-era.json" && ! -e "$root/test-state/stop-count" ]] ||
  fail "publishable production secret slot crossed the pre-Start boundary"

# Scenario FE-OLD-GENERATION-DRIFT-00D
# Preconditions: the installed old binary differs from the verified #557 artifact inventory.
# PASS: refusal occurs before the financial manifest or a stop intent.
# FAIL: unverified old bytes become rollback authority.
root=$TEST_TMP/old-generation-drift
setup_fixture "$root"
driver_args "$root"
printf '%s\n' drift >> "$root/prediction-markets/target/release/pe-service"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'installed old binary differs from the verified #557 artifact'* ]] ||
  fail "unverified installed old binary was not refused: $output"
[[ ! -e "$root/pe-financial-era.json" && ! -e "$root/test-state/stop-count" ]] ||
  fail "old-generation drift crossed the prepared boundary"

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

# Scenario FE-EMPTY-MEMBERSHIP-FRESH-00F
# Preconditions: a fresh invocation receives an empty membership file.
# PASS: manifest construction refuses before creating durable state or stopping the service.
# FAIL: the shared resume check handles the refusal, a manifest exists, or the service stops.
root=$TEST_TMP/empty-membership-fresh
setup_fixture "$root"
printf '%s\n' '[]' > "$root/target/membership.json"
driver_args "$root"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'membership must not be empty'* &&
   "$output" != *'financial-era manifest membership is empty; refusing to go forward'* ]] ||
  fail "fresh empty membership did not fail during manifest construction: $output"
[[ ! -e "$root/pe-financial-era.json" && $(<"$root/test-state/service.active") == true &&
   ! -e "$root/test-state/stop-count" ]] ||
  fail "fresh empty membership created a manifest or stopped the service"

# Scenario FE-EMPTY-MEMBERSHIP-PREPARED-00G
# Preconditions: an older prepared manifest and the supplied membership file both record `[]`.
# PASS: the shared pre-Start check refuses before service stop and leaves the manifest byte-identical.
# FAIL: identity validation wins, the service stops, or the prepared manifest changes.
root=$TEST_TMP/empty-membership-prepared
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "empty prepared membership setup did not reach prepared"
printf '%s\n' '[]' > "$root/target/membership.json"
python3 -c 'import json,sys
path=sys.argv[1]; value=json.load(open(path)); value["membership"]=[]
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' \
  "$root/pe-financial-era.json"
manifest_before=$(sha256sum "$root/pe-financial-era.json")
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'financial-era manifest membership is empty; refusing to go forward'* &&
   "$output" != *'membership identity changed'* ]] ||
  fail "prepared empty membership did not reach the shared refusal: $output"
manifest_after=$(sha256sum "$root/pe-financial-era.json")
[[ "$manifest_before" == "$manifest_after" && $(<"$root/test-state/service.active") == true &&
   ! -e "$root/test-state/stop-count" ]] ||
  fail "prepared empty membership changed the manifest or stopped the service"

# Scenario FE-EMPTY-MEMBERSHIP-GUARDED-00H
# Preconditions: an older guarded manifest and the supplied membership file both record `[]`.
# PASS: the shared pre-Start check refuses before archive or Start and leaves the manifest unchanged.
# FAIL: identity validation wins, archive/Start occurs, or the guarded manifest changes.
root=$TEST_TMP/empty-membership-guarded
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after guarded >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 && $(<"$root/test-state/stop-count") -eq 1 ]] ||
  fail "empty guarded membership setup did not reach guarded"
printf '%s\n' '[]' > "$root/target/membership.json"
python3 -c 'import json,sys
path=sys.argv[1]; value=json.load(open(path)); value["membership"]=[]
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' \
  "$root/pe-financial-era.json"
manifest_before=$(sha256sum "$root/pe-financial-era.json")
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'financial-era manifest membership is empty; refusing to go forward'* &&
   "$output" != *'membership identity changed'* ]] ||
  fail "guarded empty membership did not reach the shared refusal: $output"
manifest_after=$(sha256sum "$root/pe-financial-era.json")
[[ "$manifest_before" == "$manifest_after" && $(<"$root/test-state/stop-count") -eq 1 &&
   ! -e "$root/test-state/archive-count" && ! -e "$root/test-state/complete-start" ]] ||
  fail "guarded empty membership changed the manifest or crossed archive/Start"

# Scenario FE-EMPTY-MEMBERSHIP-ROLLBACK-00I
# Preconditions: an older prepared manifest and the supplied membership file both record `[]`.
# PASS: no-Start rollback bypasses the forward refusal and durably reaches `rolled_back`.
# FAIL: empty membership blocks rollback or any stop, archive, or Start mutation occurs.
root=$TEST_TMP/empty-membership-rollback
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "empty membership rollback setup did not reach prepared"
printf '%s\n' '[]' > "$root/target/membership.json"
python3 -c 'import json,sys
path=sys.argv[1]; value=json.load(open(path)); value["membership"]=[]
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' \
  "$root/pe-financial-era.json"
run_driver "$root" --rollback-before-start >/dev/null
python3 -c 'import json,sys
value=json.load(open(sys.argv[1])); assert value["state"]=="rolled_back"
assert value["membership"] == [] and value["no_financial_mutation"] is True' \
  "$root/pe-financial-era.json" || fail "empty membership no-Start rollback did not complete"
[[ $(<"$root/test-state/service.active") == true && ! -e "$root/test-state/stop-count" &&
   ! -e "$root/test-state/archive-count" && ! -e "$root/test-state/complete-start" ]] ||
  fail "empty membership no-Start rollback crossed a mutation boundary"

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

# Scenarios FE-REHEARSAL-LEGACY-MISSING-01A and FE-REHEARSAL-LEGACY-MALFORMED-01B (#584)
# Preconditions: PASS evidence remains hash-consistent after its legacy-continuation row is removed
# or malformed. PASS: each form receives its exact typed refusal before any durable mutation.
# FAIL: the driver reports another refusal or crosses the prepared boundary.
for evidence_case in missing malformed; do
  root="$TEST_TMP/rehearsal-legacy-$evidence_case"
  setup_fixture "$root"
  python3 - "$root/rehearsal/evidence.json" "$evidence_case" <<'PY'
import hashlib, json, sys

evidence_path, mode = sys.argv[1:]
with open(evidence_path, encoding="utf-8") as source:
    evidence = json.load(source)
manifest_path = evidence["manifest_path"]
rows = []
found = 0
with open(manifest_path, encoding="utf-8") as source:
    for raw in source:
        if raw.startswith("legacy_continuations="):
            found += 1
            if mode == "missing":
                continue
            raw = "legacy_continuations=1:not-a-digest\n"
        rows.append(raw)
assert found == 1
with open(manifest_path, "w", encoding="utf-8") as output:
    output.writelines(rows)
with open(manifest_path, "rb") as source:
    evidence["evidence_sha256"] = hashlib.sha256(source.read()).hexdigest()
with open(evidence_path, "w", encoding="utf-8") as output:
    json.dump(evidence, output, sort_keys=True, separators=(",", ":"))
PY
  driver_args "$root"
  expected="${evidence_case}_legacy_continuations"
  set +e
  output=$(run_driver "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *"REHEARSAL_REFUSAL=$expected"* ]] ||
    fail "$evidence_case legacy-continuation evidence was not typed-refused: $output"
  [[ ! -e "$root/pe-financial-era.json" && $(<"$root/test-state/service.active") == true &&
     ! -e "$root/test-state/stop-count" ]] ||
    fail "$evidence_case legacy-continuation evidence crossed the prepared boundary"
done

# Scenarios FE-REHEARSAL-HARNESS-MISSING-01C through FE-REHEARSAL-HARNESS-DUPLICATE-01F
# and FE-REHEARSAL-UNIT-POLICY-ROWS-01I (#586).
for harness_case in missing malformed mismatch duplicate; do
  root="$TEST_TMP/rehearsal-harness-$harness_case"
  setup_fixture "$root"
  case "$harness_case" in
    missing)
      edit_bound_manifest_row "$root" harness_bundle_sha256 missing
      expected=missing_harness_bundle_sha256
      ;;
    malformed)
      edit_bound_manifest_row "$root" harness_bundle_sha256 replace not-a-digest
      expected=malformed_harness_bundle_sha256
      ;;
    mismatch)
      edit_bound_manifest_row "$root" harness_bundle_sha256 replace "$(printf '0%.0s' {1..64})"
      expected=harness_bundle_mismatch
      ;;
    duplicate)
      edit_bound_manifest_row "$root" harness_bundle_sha256 duplicate "$REPOSITORY_HARNESS_BUNDLE_SHA256"
      expected=malformed_harness_bundle_sha256
      ;;
  esac
  driver_args "$root"
  set +e
  output=$(run_driver "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *"REHEARSAL_REFUSAL=$expected"* ]] ||
    fail "$harness_case harness-bundle evidence was not typed-refused: $output"
  [[ ! -e "$root/pe-financial-era.json" && $(<"$root/test-state/service.active") == true &&
     ! -e "$root/test-state/stop-count" && ! -e "$root/test-state/archive-count" ]] ||
    fail "$harness_case harness-bundle evidence crossed the prepared boundary"
done

for unit_field in unit_kill_signal unit_timeout_stop_secs; do
  for unit_case in missing malformed duplicate; do
    root="$TEST_TMP/rehearsal-unit-row-$unit_field-$unit_case"
    setup_fixture "$root"
    case "$unit_case" in
      missing) edit_bound_manifest_row "$root" "$unit_field" missing ;;
      malformed) edit_bound_manifest_row "$root" "$unit_field" replace not-an-integer ;;
      duplicate)
        value=2; [[ "$unit_field" != unit_timeout_stop_secs ]] || value=5
        edit_bound_manifest_row "$root" "$unit_field" duplicate "$value"
        ;;
    esac
    driver_args "$root"
    set +e
    output=$(run_driver "$root" 2>&1)
    status=$?
    set -e
    expected=malformed_unit_policy
    [[ "$unit_case" != missing ]] || expected=missing_unit_policy
    [[ $status -ne 0 && "$output" == *"REHEARSAL_REFUSAL=$expected"* ]] ||
      fail "$unit_field $unit_case evidence was not typed-refused: $output"
    [[ ! -e "$root/pe-financial-era.json" && $(<"$root/test-state/service.active") == true &&
       ! -e "$root/test-state/stop-count" ]] ||
      fail "$unit_field $unit_case evidence crossed the prepared boundary"
  done
done

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

# Scenario FE-REHEARSAL-CONTEXT-04
# Preconditions: internally hash-consistent PASS evidence substitutes one current-generation or
# reviewed-target identity before prepared.
# PASS: each substitution is typed-refused before service mutation.
# FAIL: cross-activation, cross-generation, config, or environment evidence reaches prepared.
for case_name in activation generation config environment; do
  root="$TEST_TMP/rehearsal-context-$case_name"
  setup_fixture "$root"
  driver_args "$root"
  case "$case_name" in
    activation)
      rewrite_rehearsal_evidence_field "$root" activation_id act-other
      expected=activation_identity_mismatch
      ;;
    generation)
      mkdir -p "$root/other-generation"
      rewrite_rehearsal_evidence_field "$root" generation_dir "$root/other-generation"
      expected=generation_identity_mismatch
      ;;
    config)
      rewrite_rehearsal_evidence_field "$root" config_sha256 "$(printf '0%.0s' {1..64})"
      expected=config_identity_mismatch
      ;;
    environment)
      rewrite_rehearsal_evidence_field "$root" environment_sha256 "$(printf '0%.0s' {1..64})"
      expected=environment_identity_mismatch
      ;;
  esac
  set +e
  output=$(run_driver "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *"REHEARSAL_REFUSAL=$expected"* ]] ||
    fail "$case_name rehearsal substitution was not typed-refused: $output"
  [[ ! -e "$root/pe-financial-era.json" && ! -e "$root/test-state/stop-count" ]] ||
    fail "$case_name rehearsal substitution crossed the prepared boundary"
done

# Scenario FE-REHEARSAL-DURABLE-BINDINGS-05
# Preconditions: copied-state/readiness/sanitized-environment identities were bound in prepared,
# then coherently replaced in both evidence layers with a new result-manifest digest.
# PASS: guarded revalidation refuses the substituted evidence before stop intent.
# FAIL: a different copied checkpoint, readiness response, or sanitized environment authorizes guarded.
for field in copy_manifest_sha256 readiness_sha256 rehearsal_environment_sha256; do
  root="$TEST_TMP/rehearsal-binding-$field"
  setup_fixture "$root"
  driver_args "$root"
  set +e
  run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
  status=$?
  set -e
  [[ $status -eq 86 ]] || fail "$field substitution setup did not reach prepared"
  rewrite_rehearsal_evidence_field "$root" "$field" "$(printf '0%.0s' {1..64})"
  set +e
  output=$(run_driver "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *'REHEARSAL_REFUSAL=prepared_evidence_unbound_or_changed'* ]] ||
    fail "$field rehearsal substitution was not refused at guarded: $output"
  [[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == prepared &&
     ! -e "$root/test-state/stop-count" ]] ||
    fail "$field rehearsal substitution crossed the guarded mutation boundary"
done

root=$TEST_TMP/rehearsal-binding-harness-bundle
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "harness-bundle substitution setup did not reach prepared"
python3 - "$root/rehearsal/evidence.json" <<'PY'
import json,sys
e=json.load(open(sys.argv[1],encoding="utf-8")); path=e["manifest_path"]
rows=[]
for raw in open(path,encoding="utf-8"):
    if raw.startswith("harness_bundle_sha256="):
        raw="harness_bundle_sha256="+"0"*64+"\n"
    rows.append(raw)
with open(path,"w",encoding="utf-8") as output: output.writelines(rows)
PY
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'REHEARSAL_REFUSAL=evidence_hash_mismatch'* ]] ||
  fail "hash-unbound harness-bundle substitution was not refused digest-first: $output"
[[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' \
     "$root/pe-financial-era.json") == prepared &&
   $(<"$root/test-state/service.active") == true && ! -e "$root/test-state/stop-count" &&
   ! -e "$root/test-state/archive-count" ]] ||
  fail "hash-unbound harness-bundle substitution crossed the prepared boundary"

# Scenario FE-REHEARSAL-UNIT-POLICY-DRIFT-01G: every pre-stop, guarded-entry, and pre-start
# observation refuses drift from the stop policy bound at Prepared (#586).
for drift_case in prepared_signal prepared_timeout stop_intent stopped_unreceipted \
  guarded_entry qualification_started service_start_intent; do
  root="$TEST_TMP/unit-policy-drift-$drift_case"
  setup_fixture "$root"
  driver_args "$root"
  set +e
  case "$drift_case" in
    prepared_signal|prepared_timeout)
      run_driver "$root" --simulate-crash-after prepared >/dev/null 2>&1
      ;;
    stop_intent)
      run_driver "$root" --simulate-crash-after service-stop-intent >/dev/null 2>&1
      ;;
    stopped_unreceipted)
      : > "$root/test-state/crash-after-stop"
      run_driver "$root" >/dev/null 2>&1
      ;;
    guarded_entry)
      run_driver "$root" --simulate-crash-after guarded >/dev/null 2>&1
      ;;
    qualification_started)
      run_driver "$root" --simulate-crash-after qualification-started >/dev/null 2>&1
      ;;
    service_start_intent)
      run_driver "$root" --simulate-crash-after service-start-intent >/dev/null 2>&1
      ;;
  esac
  setup_status=$?
  set -e
  [[ $setup_status -eq 86 ]] || fail "$drift_case policy-drift setup returned $setup_status"
  expected_activity=true
  case "$drift_case" in
    stopped_unreceipted|guarded_entry|qualification_started|service_start_intent)
      expected_activity=false
      ;;
  esac
  if [[ "$drift_case" == prepared_timeout ]]; then
    printf '%s\n' 'KillSignal=2' 'TimeoutStopUSec=7s' > "$root/test-state/unit-stop-policy"
  else
    printf '%s\n' 'KillSignal=15' 'TimeoutStopUSec=5s' > "$root/test-state/unit-stop-policy"
  fi
  state_before=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' \
    "$root/pe-financial-era.json")
  manifest_before=$(sha256sum "$root/pe-financial-era.json" | awk '{print $1}')
  set +e
  output=$(run_driver "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *'REHEARSAL_REFUSAL=unit_stop_policy_drift'* &&
     "$output" == *"service_active=$expected_activity"* ]] ||
    fail "$drift_case live unit policy was not refused with observed activity: $output"
  state_after=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' \
    "$root/pe-financial-era.json")
  manifest_after=$(sha256sum "$root/pe-financial-era.json" | awk '{print $1}')
  [[ "$state_after" == "$state_before" && "$manifest_after" == "$manifest_before" &&
     $(<"$root/test-state/service.active") == "$expected_activity" ]] ||
    fail "$drift_case policy refusal changed state or service activity"
  case "$drift_case" in
    prepared_signal|prepared_timeout|stop_intent)
      [[ ! -e "$root/test-state/stop-count" && ! -e "$root/test-state/archive-count" ]] ||
        fail "$drift_case policy refusal crossed stop or archive"
      ;;
    stopped_unreceipted|guarded_entry)
      [[ $(<"$root/test-state/stop-count") == 1 && ! -e "$root/test-state/archive-count" ]] ||
        fail "$drift_case policy refusal crossed archive or repeated stop"
      if [[ "$drift_case" == guarded_entry ]]; then
        [[ ! -e "$root/test-state/live-schema-count" &&
           ! -e "$root/test-state/forward-refresh-count" &&
           ! -e "$root/test-state/start-count" ]] ||
          fail "guarded-entry policy refusal crossed a post-Start mutation"
      fi
      ;;
    qualification_started|service_start_intent)
      [[ ! -e "$root/test-state/start-count" ]] ||
        fail "$drift_case policy refusal started the service"
      ;;
  esac
done

# Scenario FE-REHEARSAL-BUNDLE-RESUME-01H: pre-Start guarded resumes rebind only to the persisted
# hermetic closure, while a completed Start preserves forced roll-forward (#586).
root=$TEST_TMP/rehearsal-bundle-resume-pre-start
setup_fixture "$root"
setup_hermetic_financial_driver "$root"
driver_args "$root"
repository_driver=$DRIVER
DRIVER=$HERMETIC_DRIVER
set +e
run_driver "$root" --simulate-crash-after guarded >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "hermetic bundle resume did not reach guarded"
cp "$HERMETIC_PREFLIGHT" "$HERMETIC_PREFLIGHT.original"
printf '%s\n' '# controlled bundle drift' >> "$HERMETIC_PREFLIGHT"
manifest_before=$(sha256sum "$root/pe-financial-era.json")
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'REHEARSAL_REFUSAL=harness_bundle_mismatch'* ]] ||
  fail "guarded hermetic bundle drift was not refused: $output"
manifest_after=$(sha256sum "$root/pe-financial-era.json")
[[ "$manifest_before" == "$manifest_after" && ! -e "$root/test-state/archive-count" &&
   $(<"$root/test-state/service.active") == false ]] ||
  fail "guarded hermetic bundle drift changed state or reached archive"
cp "$HERMETIC_PREFLIGHT.original" "$HERMETIC_PREFLIGHT"
drive_to_verified "$root" || fail "restored hermetic bundle did not converge"
DRIVER=$repository_driver

root=$TEST_TMP/rehearsal-bundle-resume-post-start
setup_fixture "$root"
setup_hermetic_financial_driver "$root"
driver_args "$root"
repository_driver=$DRIVER
DRIVER=$HERMETIC_DRIVER
set +e
run_driver "$root" --simulate-crash-after qualification-started >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 && -e "$root/test-state/complete-start" ]] ||
  fail "post-Start hermetic bundle resume did not reach QualificationStarted"
printf '%s\n' '# post-Start forced-roll-forward drift' >> "$HERMETIC_PREFLIGHT"
output=$(run_driver "$root" 2>&1) || fail "post-Start bundle drift blocked roll-forward: $output"
[[ "$output" != *harness_bundle_mismatch* && $(<"$root/test-state/start-count") == 1 &&
   $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' \
     "$root/pe-financial-era.json") == started ]] ||
  fail "post-Start bundle drift did not roll forward exactly once"
DRIVER=$repository_driver

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

# Scenario FE-PREP-APPROVED-01A
# Preconditions: each fresh fixture contains an Approved-only live admission marker.
# Injected boundaries: preparation and representative later forward crash seams.
# PASS: every invocation refuses at the real offline preparation call before archive or Start.
# FAIL: a requested later crash seam bypasses preparation, mutates a durable input, or starts.
for boundary in preparation guarded qualification-started; do
  root="$TEST_TMP/prepare-approved-$boundary"
  setup_fixture "$root"
  driver_args "$root"
  printf '%s\n' approved-only >> \
    "$root/prediction-markets/gen/g557/live_journal.log"
  before=$(sha256sum "$root/prediction-markets/gen/g557/"{paper.log,source_events.log,live_journal.log,paper_state.db})
  set +e
  output=$(run_driver "$root" --simulate-crash-after "$boundary" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *'unmatched Approved admission'* ]] ||
    fail "Approved-only preparation was not refused for $boundary: $output"
  after=$(sha256sum "$root/prediction-markets/gen/g557/"{paper.log,source_events.log,live_journal.log,paper_state.db})
  [[ "$before" == "$after" && $(<"$root/test-state/service.active") == false ]] ||
    fail "Approved-only preparation changed a durable input or left the service active for $boundary"
  [[ ! -e "$root/test-state/archive-count" && ! -e "$root/test-state/complete-start" ]] ||
    fail "Approved-only preparation crossed archive or Start for $boundary"
done

# Scenario FE-START-UNKNOWN-02
# Preconditions: stopped service with no Start and a completed stop receipt.
# Injected boundary: `service-stopped`, followed by an unreadable Start scan.
# PASS: rollback preserves stdout/stderr diagnostics and refuses before any restore.
# FAIL: unknown is treated as no Start, diagnostics are lost, or restore runs.
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
[[ "$output" == *'rollback-check output: rollback-error stdout marker'* ]] ||
  fail "unknown Start state discarded rollback-check stdout: $output"
grep -Fxq 'rollback-error stderr marker' <<< "$output" ||
  fail "unknown Start state changed or discarded rollback-check stderr: $output"
[[ ! -e "$root/test-state/restore-count" ]] || fail "unknown Start state reached restore"
rm "$root/test-state/rollback-error"
for rollback_output in 'invalid-json' '{}' '{"complete_start":"false"}' 'null' '[]' '0' '"x"'; do
  printf '%s\n' "$rollback_output" > "$root/test-state/rollback-output"
  set +e
  output=$(run_driver "$root" --rollback-before-start 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 && "$output" == *'QualificationStarted state is unknown'* ]] ||
    fail "invalid Start output did not block rollback: $output"
  [[ "$output" == *"rollback-check output: $rollback_output"* ]] ||
    fail "invalid Start output was discarded: $output"
  [[ ! -e "$root/test-state/restore-count" ]] || fail "invalid Start output reached restore"
done

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
[[ $(<"$root/test-state/rollback-refresh-count") -eq 1 ]] ||
  fail "pre-Start rollback did not refresh the public projection exactly once"
python3 -c 'import json,sys
value=json.load(open(sys.argv[1])); assert value["local_restore_skipped"] is True' \
  "$root/pe-financial-era.json" || fail "unchanged local state was restored without a mutation receipt"
run_driver "$root" --rollback-before-start >/dev/null
[[ $(<"$root/test-state/restore-count") -eq 1 ]] || fail "rolled-back rerun restored twice"
[[ ! -e "$root/test-state/local-restore-count" ]] || fail "no-Start shortcut invoked local restore"
# Terminal rollback is immutable even if later writes leave SQLite sidecars.
printf 'terminal-wal\n' > "$root/prediction-markets/gen/g557/paper_state.db-wal"
printf 'terminal-shm\n' > "$root/prediction-markets/gen/g557/paper_state.db-shm"
terminal_before=$(rollback_snapshot "$root")
run_driver "$root" --rollback-before-start >/dev/null
[[ "$(rollback_snapshot "$root")" == "$terminal_before" ]] || fail "terminal rollback changed state"

# Scenario FE-ROLLBACK-NO-START-SIDECARS-14
# Preconditions: no Start/restore intent, unrestored local main equals backup but not guarded,
# WAL present, archive already restored, and inert old service.
# PASS: missing-intent refusal preserves local bytes/sidecars and records no restore intent.
# FAIL: local restoration or old start occurs, or a restore intent is recorded.
root=$TEST_TMP/rollback-no-start-sidecars
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after remote-archived >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "no-Start sidecar setup did not reach the archive"
set +e
run_driver "$root" --rollback-before-start \
  --simulate-crash-after rollback-wallet-live-stats-refreshed >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 ]] || fail "no-Start sidecar setup did not restore the archive"
python3 - "$root" <<'PY' || fail "no-Start sidecar fixture does not match the refusal state"
import hashlib, json, pathlib, shutil, sys
root=pathlib.Path(sys.argv[1]); value=json.loads((root/"pe-financial-era.json").read_text())
assert value["state"] == "rolling_back" and value["archive_restored"] is True
assert not value.get("qualification_start_intent",False)
assert not value.get("local_restored",False)
assert "local_restore_intent" not in value
assert not value.get("old_service_start_intent",False)
assert (root/"test-state/service.active").read_text().strip() == "false"
database=pathlib.Path(value["paths"]["paper_state"])
shutil.copyfile(value["backup"]["path"],database)
assert hashlib.sha256(database.read_bytes()).hexdigest() == value["backup"]["sha256"]
assert value["backup"]["sha256"] != value["guarded_paper_state_sha256"]
pathlib.Path(str(database)+"-wal").write_bytes(b"unexplained-wal\n")
PY
refusal_before=$(rollback_snapshot "$root")
set +e
output=$(run_driver "$root" --rollback-before-start 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'local state equals the backup without a durable restore intent'* ]] ||
  fail "no-Start sidecars did not retain the missing-intent refusal: $output"
[[ ! -e "$root/test-state/local-restore-count" && ! -e "$root/test-state/start-count" ]] ||
  fail "no-Start sidecars authorized local restoration or old start"
python3 - "$root/pe-financial-era.json" "$refusal_before" "$(rollback_snapshot "$root")" <<'PY' || fail "no-Start refusal changed local state or recorded a restore intent"
import json, sys
value=json.load(open(sys.argv[1]))
assert "local_restore_intent" not in value and not value.get("local_restored",False)
before,after=map(json.loads,sys.argv[2:])
for snapshot in (before,after): del snapshot[sys.argv[1]]
assert before == after
PY
echo 'PASS: FE-ROLLBACK-NO-START-SIDECARS-14'

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
touch "$root/test-state/rollback-error"
set +e
output=$(run_driver "$root" --rollback-before-start 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'complete QualificationStarted forces roll-forward'* ]] ||
  fail "complete Start did not force roll-forward"
[[ ! -e "$root/test-state/restore-count" ]] || fail "complete Start was rolled back"

# Scenario FE-SERVICE-START-06
# Preconditions: complete Start and adopted target artifacts; the second fixture then records `[]`.
# Injected boundary: `before-manifest-started`, after systemctl start and its own receipt, in each fixture.
# PASS: both reruns roll forward without repeating archive/start. FAIL: refusal or duplicate mutation.
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
[[ $(<"$root/test-state/live-schema-count") -eq 1 ]] ||
  fail "initial activation did not install the live financial schema once"
[[ $(<"$root/test-state/forward-refresh-count") -eq 1 ]] ||
  fail "initial activation did not refresh the public projection once"
run_driver "$root" >/dev/null
[[ $(<"$root/test-state/archive-count") -eq 1 ]] || fail "Start recovery repeated remote archive"
[[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == started ]] ||
  fail "Start recovery did not durably roll forward to started"

# Scenario FE-SERVICE-START-69 — a resumed activation accepts an era that has already traded (#628).
# Preconditions: complete Start, service started, then an interruption before `started` is recorded.
#   Before the retry the AUTHORITY reports a progressed era: `last_prepared_seq` set and cash moved.
# PASS: the retry rolls forward to `started`, proving Start identity without re-proving pristineness,
#       and without repeating the archive or starting the service a second time.
# FAIL: the driver refuses ("authority Start read-back differs"), stranding an activation whose
#       service is running normally, with rollback already forbidden by the complete Start.
root=$TEST_TMP/post-service-start-traded
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after before-manifest-started >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 && $(<"$root/test-state/service.active") == true ]] ||
  fail "traded-resume fixture did not reach the post-service-start seam"
archive_before=$(<"$root/test-state/archive-count")
start_before=$(<"$root/test-state/start-count")

# The service is up and has traded: the authority has advanced past the pristine baseline.
touch "$root/test-state/authority-has-traded"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -eq 0 ]] ||
  fail "a resumed activation refused an era that legitimately traded: $output"
[[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == started ]] ||
  fail "traded resume did not durably roll forward to started"
[[ $(<"$root/test-state/archive-count") -eq "$archive_before" ]] ||
  fail "traded resume repeated the remote archive"
[[ $(<"$root/test-state/start-count") -eq "$start_before" ]] ||
  fail "traded resume started the service a second time"

# Control: identity is still proven on the resume path. A progressed era carrying a DIFFERENT Start
# must still refuse, otherwise accepting progress would have become a hole.
root=$TEST_TMP/post-service-start-traded-impostor
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after before-manifest-started >/dev/null 2>&1
set -e
touch "$root/test-state/authority-start-impostor"
set +e
output=$(run_driver "$root" 2>&1)
status=$?
set -e
[[ $status -ne 0 && "$output" == *'authority Start read-back differs'* ]] ||
  fail "a resumed activation accepted a different authority Start: $output"
echo "PASS: FE-SERVICE-START-69"

root=$TEST_TMP/post-service-start-empty-membership
setup_fixture "$root"
driver_args "$root"
set +e
run_driver "$root" --simulate-crash-after before-manifest-started >/dev/null 2>&1
status=$?
set -e
[[ $status -eq 86 && $(<"$root/test-state/service.active") == true ]] ||
  fail "empty-membership post-service-start crash seam was not reached"
printf '%s\n' '[]' > "$root/target/membership.json"
python3 -c 'import json,sys
path=sys.argv[1]; value=json.load(open(path)); assert value["state"]=="guarded"
value["membership"]=[]
json.dump(value,open(path,"w"),sort_keys=True,separators=(",",":"))' \
  "$root/pe-financial-era.json"
run_driver "$root" >/dev/null
[[ $(<"$root/test-state/archive-count") -eq 1 ]] || fail "Start recovery repeated remote archive"
[[ $(<"$root/test-state/start-count") -eq 1 && $(<"$root/test-state/live-schema-count") -eq 1 &&
   $(<"$root/test-state/forward-refresh-count") -eq 1 ]] ||
  fail "Start recovery with empty membership repeated a completed mutation"
python3 -c 'import json,sys
value=json.load(open(sys.argv[1])); assert value["state"]=="started" and value["membership"] == []' \
  "$root/pe-financial-era.json" ||
  fail "Start recovery with empty membership did not durably roll forward to started"
root=$TEST_TMP/post-service-start
driver_args "$root"

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
assert value["state"]=="verified" and value["verified_state_assertions"] is True
assert value["verified_readiness_sha256"]' \
  "$root/pe-financial-era.json" || fail "verified state did not retain all merged assertions"
[[ $(<"$root/test-state/readiness-count") -eq 1 ]] ||
  fail "verified transition did not query readiness exactly once"

# Scenario FE-VERIFY-READINESS-07A
# Preconditions: every status/local/remote assertion passes, but one readiness-only contract fails.
# PASS: HTTP failure, ready:false, and a nonempty issue list each leave the manifest at started.
# FAIL: status evidence alone can advance verified or readiness is queried more than once.
for readiness_mode in http_failure not_ready issues; do
  root="$TEST_TMP/verify-readiness-$readiness_mode"
  setup_fixture "$root"
  driver_args "$root"
  run_driver "$root" >/dev/null
  [[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == started ]] ||
    fail "$readiness_mode setup did not reach started"
  touch "$root/prediction-markets/gen/g557/status.json"
  echo "$readiness_mode" > "$root/test-state/readiness-mode"
  set +e
  output=$(run_driver "$root" 2>&1)
  status=$?
  set -e
  [[ $status -ne 0 ]] || fail "$readiness_mode readiness failure advanced verified"
  if [[ "$readiness_mode" == http_failure ]]; then
    [[ "$output" == *'installed readiness endpoint did not return HTTP success'* ]] ||
      fail "$readiness_mode failed before its readiness assertion: $output"
  else
    [[ "$output" == *'installed readiness proof is incomplete'* ]] ||
      fail "$readiness_mode failed before its readiness assertion: $output"
  fi
  [[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$root/pe-financial-era.json") == started ]] ||
    fail "$readiness_mode readiness failure changed durable state"
  [[ -f "$root/test-state/readiness-count" && $(<"$root/test-state/readiness-count") -eq 1 ]] ||
    fail "$readiness_mode readiness failure was not queried exactly once"
done

# Scenario FE-FORWARD-MATRIX-08
# Preconditions: fresh fixture at each case; exact staged/environment and Legacy17 guard pass.
# Injected boundaries: before, at, and after every durable manifest receipt, plus each atomic
# adoption action after rename and before its manifest receipt.
# PASS: each case converges with one stop, archive, and start. FAIL: duplicate or stranded action.
forward_manifest_boundaries=(
  prepared service-stop-intent service-stopped legacy-contract-verified preparation guarded
  remote-archive-intent remote-archived qualification-start-intent qualification-started
  authority-schema-intent authority-schema-installed live-schema-intent live-schema-installed
  authority-start-seeded
  financial-config-migration-intent financial-config-migrated
  wallet-live-stats-refresh-intent wallet-live-stats-refreshed
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
  [[ -f "$root/test-state/live-schema-count" && $(<"$root/test-state/live-schema-count") -ge 1 ]] ||
    fail "$boundary did not install the live financial schema"
  [[ -f "$root/test-state/forward-refresh-count" && $(<"$root/test-state/forward-refresh-count") -ge 1 ]] ||
    fail "$boundary did not refresh the public projection"
done

# Scenario FE-ROLLBACK-MATRIX-09
# Preconditions: stamped archive, deliberately changed local database, inert old service.
# Injected boundaries: before, at, and after every rollback receipt.
# PASS: catalog-equal remote restore, byte-equal local restore, and old start each occur once.
# FAIL: retry restores while active, repeats a transition, or loses an equality proof.
rollback_manifest_boundaries=(
  rollback-service-stopped rolling_back rollback-archive-restore-intent archive-restored
  rollback-wallet-live-stats-refresh-intent rollback-wallet-live-stats-refreshed
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
  [[ -f "$root/test-state/rollback-refresh-count" && $(<"$root/test-state/rollback-refresh-count") -ge 1 ]] ||
    fail "$boundary did not refresh the restored public projection"
  [[ $(<"$root/test-state/local-restore-count") -eq 1 ]] || fail "$boundary repeated local restoration"
  assert_restored_sqlite "$root"
done

# Scenario FE-ROLLBACK-WAL-ONLY-10
# PASS: unchanged guarded main plus committed reset WAL restores exact backup/schema/content,
# removes sidecars before reopening, and starts the old service once. FAIL: hash-only certification.
root=$TEST_TMP/rollback-wal-only
drive_to_wal_reset "$root"
run_driver "$root" --rollback-before-start >/dev/null
assert_restored_sqlite "$root"
[[ $(<"$root/test-state/local-restore-count") -eq 1 &&
   $(<"$root/test-state/restore-count") -eq 1 && $(<"$root/test-state/start-count") -eq 1 ]] ||
  fail "WAL-only rollback did not restore/start exactly once"
echo 'PASS: FE-ROLLBACK-WAL-ONLY-10'

# Scenario FE-ROLLBACK-STALE-RECEIPTS-11
# PASS: skipped/restored receipts never bypass physical restore, including a backup-equal main
# with stale WAL and a main with no sidecars but no restore intent; a clean main with restore intent
# certifies without re-restoring and clears skipped. FAIL: old start precedes physical certification.
for stale in skipped skipped-restored restored main-equal-with-stale-wal main-equal-clean \
  main-equal-clean-restore-intent; do
  root=$TEST_TMP/rollback-stale-$stale
  drive_to_wal_reset "$root"
  expected_restores=1
  if [[ "$stale" == main-equal-clean-restore-intent ]]; then
    expected_restores=0
    echo 0 > "$root/test-state/local-restore-count"
  fi
  python3 - "$root" "$stale" <<'PY'
import json, pathlib, shutil, sys
root=pathlib.Path(sys.argv[1]); path=root/"pe-financial-era.json"; value=json.loads(path.read_text())
value["state"]="rolling_back"
value["local_restore_skipped"]=sys.argv[2] != "restored"
value["local_restored"]=sys.argv[2] != "skipped"
if sys.argv[2].startswith("main-equal"):
    shutil.copyfile(value["backup"]["path"],value["paths"]["paper_state"])
if sys.argv[2].startswith("main-equal-clean"):
    for suffix in ("-wal","-shm"): pathlib.Path(value["paths"]["paper_state"]+suffix).unlink()
if sys.argv[2] == "main-equal-clean-restore-intent": value["local_restore_intent"]=True
path.write_text(json.dumps(value,sort_keys=True,separators=(",",":")))
PY
  set +e
  run_driver "$root" --rollback-before-start --simulate-crash-after local-restored >/dev/null 2>&1
  status=$?
  set -e
  [[ $status -eq 86 ]] || fail "$stale missed local-restored crash: $status"
  [[ ! -e "$root/test-state/start-count" && $(<"$root/test-state/local-restore-count") -eq $expected_restores ]] ||
    fail "$stale started before certification or repeated physical restoration"
  assert_restored_sqlite "$root"
  python3 -c 'import json,sys; v=json.load(open(sys.argv[1])); assert v["local_restore_intent"] and v["local_restored"] and not v["local_restore_skipped"]' \
    "$root/pe-financial-era.json" || fail "$stale retained obsolete skipped receipt"
  run_driver "$root" --rollback-before-start >/dev/null
  [[ $(<"$root/test-state/local-restore-count") -eq $expected_restores &&
     $(<"$root/test-state/restore-count") -eq 1 && $(<"$root/test-state/start-count") -eq 1 ]] ||
    fail "$stale repeated restoration or old start"
  echo "PASS: FE-ROLLBACK-STALE-RECEIPTS-11/$stale"
done
echo 'PASS: FE-ROLLBACK-STALE-RECEIPTS-11'

# Scenario FE-ROLLBACK-RESTORE-RETRY-12
# PASS: every restore receipt boundary, replacement-before-sidecar-cleanup and SHM-only interruption
# converges without duplicate archive restoration or old start. FAIL: stale WAL certifies completion.
wal_restore_boundaries=(replacement-before-sidecar-cleanup wal-removed-before-shm-cleanup)
for receipt in rollback-local-restore-intent local-restored; do
  wal_restore_boundaries+=("before-manifest-$receipt" "$receipt" "after-manifest-$receipt")
done
for boundary in "${wal_restore_boundaries[@]}"; do
  root=$TEST_TMP/rollback-wal-$boundary
  drive_to_wal_reset "$root"
  crash_args=(--simulate-crash-after "$boundary")
  expected_restores=1
  if [[ "$boundary" == replacement-before-sidecar-cleanup ]]; then
    touch "$root/test-state/crash-after-local-replace"
    crash_args=()
    expected_restores=2
  elif [[ "$boundary" == wal-removed-before-shm-cleanup ]]; then
    touch "$root/test-state/crash-after-local-wal-removal"
    crash_args=()
    expected_restores=2
  fi
  set +e
  output=$(run_driver "$root" --rollback-before-start "${crash_args[@]}" 2>&1)
  status=$?
  set -e
  [[ $status -eq 86 ]] || fail "$boundary missed restore crash: $status: $output"
  if [[ "$boundary" == replacement-before-sidecar-cleanup || "$boundary" == wal-removed-before-shm-cleanup ]]; then
    database=$root/prediction-markets/gen/g557/paper_state.db
    cmp -s "$database" "$root/prediction-markets/financial-era-act-545-paper-state.db" ||
      fail "interrupted replacement main differs from backup"
    [[ -e "$database-shm" ]] || fail "interrupted helper lost its SHM"
    if [[ "$boundary" == wal-removed-before-shm-cleanup ]]; then
      [[ ! -e "$database-wal" ]] || fail "interrupted helper retained its WAL"
    else
      [[ -s "$database-wal" ]] || fail "interrupted helper lost its WAL"
    fi
  fi
  run_driver "$root" --rollback-before-start >/dev/null
  assert_restored_sqlite "$root"
  terminal_before=$(rollback_snapshot "$root")
  run_driver "$root" --rollback-before-start >/dev/null
  [[ "$(rollback_snapshot "$root")" == "$terminal_before" ]] || fail "$boundary changed terminal rollback"
  [[ $(<"$root/test-state/local-restore-count") -eq $expected_restores &&
     $(<"$root/test-state/restore-count") -eq 1 && $(<"$root/test-state/start-count") -eq 1 ]] ||
    fail "$boundary repeated restoration or old start"
  echo "PASS: FE-ROLLBACK-RESTORE-RETRY-12/$boundary"
done
echo 'PASS: FE-ROLLBACK-RESTORE-RETRY-12'

# Scenario FE-ROLLBACK-RESUMED-WRITES-13
# PASS: active old service retains legitimate WAL/log suffix writes with or without a completed
# start receipt; pre-systemctl start-intent retry accepts only an untouched restored image;
# inactive ambiguous resumption fails closed, and originally inactive stays inactive.
# FAIL: restore overwrites later writes, ambiguity succeeds, or a duplicate start occurs.
for resumption in start-inflight started ambiguous-main ambiguous-wal ambiguous-log inactive \
  start-intent-untouched start-intent-changed; do
  root=$TEST_TMP/rollback-resumed-$resumption
  if [[ "$resumption" == inactive ]]; then
    drive_to_wal_reset "$root" false
    run_driver "$root" --rollback-before-start >/dev/null
    assert_restored_sqlite "$root"
    [[ ! -e "$root/test-state/start-count" && $(<"$root/test-state/service.active") == false ]] ||
      fail "originally inactive service was started"
  else
    drive_to_wal_reset "$root"
    boundary=before-manifest-old-service-started
    [[ "$resumption" != started ]] || boundary=old-service-started
    if [[ "$resumption" == start-intent-* ]]; then boundary=rollback-old-service-start-intent; fi
    set +e
    run_driver "$root" --rollback-before-start --simulate-crash-after "$boundary" >/dev/null 2>&1
    status=$?
    set -e
    [[ $status -eq 86 ]] || fail "$resumption did not reach old-service start"
    if [[ "$resumption" == start-intent-* ]]; then
      [[ ! -e "$root/test-state/start-count" && $(<"$root/test-state/service.active") == false ]] ||
        fail "$resumption crossed systemctl start before the interruption"
      python3 -c 'import json,sys
v=json.load(open(sys.argv[1]))
assert v["qualification_start_intent"] and v["local_restore_intent"] and v["local_restored"]
assert v["old_service_start_intent"] and not v.get("old_service_started",False)' \
        "$root/pe-financial-era.json" || fail "$resumption lacks the pre-systemctl start receipts"
    fi
    python3 - "$root/prediction-markets/gen/g557" "$resumption" <<'PY'
import os, pathlib, sqlite3, sys
generation=pathlib.Path(sys.argv[1]); mode=sys.argv[2]
if mode == "start-intent-changed": original=(generation/"paper_state.db").read_bytes()
if mode not in ("ambiguous-log","start-intent-untouched"):
    db=sqlite3.connect(str(generation/"paper_state.db"))
    db.execute("pragma wal_autocheckpoint=0")
    db.execute("insert into durable values('legitimate-resumed-write')")
    db.commit()
    if mode in ("ambiguous-main","start-intent-changed"): db.close()
if mode == "start-intent-changed":
    assert (generation/"paper_state.db").read_bytes() != original
    assert not (generation/"paper_state.db-wal").exists()
    assert not (generation/"paper_state.db-shm").exists()
if mode not in ("ambiguous-main","ambiguous-wal","start-intent-untouched","start-intent-changed"):
    for name in ("paper.log","source_events.log","live_journal.log"):
        with (generation/name).open("ab") as output: output.write(b"legitimate-resumed-suffix\n")
os._exit(0)
PY
    if [[ "$resumption" == ambiguous-* ]]; then echo false > "$root/test-state/service.active"; fi
  fi
  resumed_before=$(rollback_snapshot "$root")
  set +e
  output=$(run_driver "$root" --rollback-before-start 2>&1)
  status=$?
  set -e
  if [[ "$resumption" == ambiguous-* || "$resumption" == start-intent-changed ]]; then
    [[ $status -ne 0 && ( "$output" == *'old-service resumption is ambiguous'* ||
                         "$output" == *'guarded paper/source/live log identity changed'* ) ]] ||
      fail "$resumption did not fail closed: $output"
  else
    [[ $status -eq 0 && "$output" == *state=rolled_back* ]] || fail "$resumption failed: $output"
  fi
  resumed_after=$(rollback_snapshot "$root")
  python3 - "$resumed_before" "$resumed_after" "$resumption" <<'PY' || fail "$resumption overwrote resumed state"
import json, sys
before,after=map(json.loads,sys.argv[1:3])
if sys.argv[3] in ("start-inflight","started","start-intent-untouched"):
    for values in (before,after):
        for key in list(values):
            if key.endswith("/pe-financial-era.json") or (sys.argv[3] == "start-intent-untouched" and
                    key.endswith(("/start-count","/service.active"))): del values[key]
assert before == after
PY
  if [[ "$resumption" == start-intent-untouched ]]; then
    [[ $(<"$root/test-state/start-count") -eq 1 && $(<"$root/test-state/service.active") == true &&
       $(<"$root/test-state/local-restore-count") -eq 1 && $(<"$root/test-state/restore-count") -eq 1 ]] ||
      fail "untouched start-intent retry repeated restoration or did not start exactly once"
    assert_restored_sqlite "$root"
  fi
  echo "PASS: FE-ROLLBACK-RESUMED-WRITES-13/$resumption"
done
echo 'PASS: FE-ROLLBACK-RESUMED-WRITES-13'


# ── FE-STREAMHASH-63: the three embedded Python hashes must not allocate per file size ──────────
# #545 attempt 14 aborted AFTER stopping production and taking the backup, with MemoryError, because
# each of these blocks read a whole file (the source log is 13.76 GB on a 1,967 MB host). Both the
# file length AND the recorded prefix length exceed the address-space limit below, so the prefix
# verifier's allocation — which is driven by the recorded `bytes` — is genuinely exercised.
sh63_root=$TEST_TMP/streamhash
mkdir -p "$sh63_root"
sh63_limit_kib=262144                                    # 256 MiB address space
sh63_big=$sh63_root/big.log                              # 320 MiB payload > the limit
python3 - "$sh63_big" <<'FIXTURE' || fail "FE-STREAMHASH-63 could not build the fixture"
import sys
block = b"pe-financial-era streaming hash fixture\n" * 26215
block = block[: 1 << 20] if len(block) >= (1 << 20) else block + b"\0" * ((1 << 20) - len(block))
with open(sys.argv[1], "wb") as handle:
    for _ in range(320):
        handle.write(block)
FIXTURE
printf 'trailing suffix beyond the recorded prefix\n' >> "$sh63_big"
sh63_prefix_bytes=$(( 288 * 1024 * 1024 ))               # recorded prefix also > the limit
sh63_prefix_sha=$(head -c "$sh63_prefix_bytes" "$sh63_big" | sha256sum | cut -d' ' -f1)
sh63_full_sha=$(sha256sum "$sh63_big" | cut -d' ' -f1)
sh63_full_bytes=$(stat -c %s "$sh63_big")

sh63_manifest() {
  python3 - "$1" "$sh63_big" "$2" "$3" <<'WRITE'
import json, sys
manifest, path, sha, size = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
logs = {name: {"path": path, "sha256": sha, "bytes": size} for name in ("paper", "source", "live")}
json.dump({"guarded_logs": logs}, open(manifest, "w"), sort_keys=True)
WRITE
}
sh63_manifest "$sh63_root/whole.json" "$sh63_full_sha" "$sh63_full_bytes"
sh63_manifest "$sh63_root/prefix.json" "$sh63_prefix_sha" "$sh63_prefix_bytes"

# Extract each verifier verbatim from the shipped driver and run it in a constrained subprocess.
sed -n '/^verify_guarded_log_identities() {/,/^}/p' "$DRIVER" > "$sh63_root/identities.sh"
sed -n '/^verify_guarded_log_prefixes() {/,/^}/p' "$DRIVER" > "$sh63_root/prefixes.sh"
[[ -s "$sh63_root/identities.sh" && -s "$sh63_root/prefixes.sh" ]] ||
  fail "FE-STREAMHASH-63 could not extract the verifier bodies"

MANIFEST="$sh63_root/whole.json" bash -c "ulimit -v $sh63_limit_kib
source '$sh63_root/identities.sh'; verify_guarded_log_identities" ||
  fail "FE-STREAMHASH-63 identity verifier failed under a ${sh63_limit_kib} KiB limit"

MANIFEST="$sh63_root/prefix.json" bash -c "ulimit -v $sh63_limit_kib
source '$sh63_root/prefixes.sh'; verify_guarded_log_prefixes" ||
  fail "FE-STREAMHASH-63 prefix verifier failed under a ${sh63_limit_kib} KiB limit"

# The recording block, taken verbatim from the driver, in a fresh constrained subprocess.
sed -n '/patch=\$(python3 -c/,/"\$backup_path" "\$backup_sha"/p' "$DRIVER" > "$sh63_root/record.sh"
[[ -s "$sh63_root/record.sh" ]] || fail "FE-STREAMHASH-63 could not extract the recording block"
sh63_patch=$(bash -c "ulimit -v $sh63_limit_kib
backup_path=/tmp/backup.db; backup_sha=deadbeef; guarded_paper_state_sha=cafebabe; census='{}'
paper_log='$sh63_big'; source_log='$sh63_big'; live_journal='$sh63_big'
source '$sh63_root/record.sh'
printf '%s' \"\$patch\"") ||
  fail "FE-STREAMHASH-63 recording block failed under a ${sh63_limit_kib} KiB limit"
python3 - "$sh63_patch" "$sh63_full_sha" "$sh63_full_bytes" <<'CHECK' ||
import json, sys
patch, sha, size = json.loads(sys.argv[1]), sys.argv[2], int(sys.argv[3])
logs = patch["guarded_logs"]
assert set(logs) == {"paper", "source", "live"}, logs
for row in logs.values():
    assert row["sha256"] == sha, (row["sha256"], sha)
    assert row["bytes"] == size, (row["bytes"], size)
CHECK
  fail "FE-STREAMHASH-63 recorded digests disagree with sha256sum"

# A file shorter than the recorded prefix, and a wrong digest, are still rejected.
head -c 1024 "$sh63_big" > "$sh63_root/short.log"
python3 - "$sh63_root/short.json" "$sh63_root/short.log" "$sh63_prefix_sha" "$sh63_prefix_bytes" <<'WRITE'
import json, sys
manifest, path, sha, size = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
logs = {name: {"path": path, "sha256": sha, "bytes": size} for name in ("paper", "source", "live")}
json.dump({"guarded_logs": logs}, open(manifest, "w"), sort_keys=True)
WRITE
if MANIFEST="$sh63_root/short.json" bash -c "source '$sh63_root/prefixes.sh'
verify_guarded_log_prefixes" 2>/dev/null; then
  fail "FE-STREAMHASH-63 prefix verifier accepted a file shorter than the recorded bytes"
fi
# The full-file digest is guaranteed to differ from the prefix digest (the file has a suffix);
# a character substitution is not, because a digest may contain no instance of that character.
[[ "$sh63_full_sha" != "$sh63_prefix_sha" ]] || fail "FE-STREAMHASH-63 fixture digests collide"
sh63_manifest "$sh63_root/wrong.json" "$sh63_full_sha" "$sh63_prefix_bytes"
if MANIFEST="$sh63_root/wrong.json" bash -c "source '$sh63_root/prefixes.sh'
verify_guarded_log_prefixes" 2>/dev/null; then
  fail "FE-STREAMHASH-63 prefix verifier accepted a wrong digest"
fi
rm -f "$sh63_big" "$sh63_root/short.log"
echo "PASS: FE-STREAMHASH-63"

# ── FE-BACKUPCACHE-64: every integrity check must run with the raised cache budget in effect ─────
# Observed with SQLite's own statement-level hook (`set_trace_callback`), which fires for EVERY
# statement the connection executes — including statements inside `executescript` and those issued
# through any cursor — so the assertion does not depend on enumerating Python execution APIs. At
# each `integrity_check` the effective `cache_size` is read back from that same connection, so a
# later reset, a reset inside a script, or a second untuned check cannot pass. Whether the raised
# cache actually shortens the check is an operational measurement against the real 4.28 GB copy,
# recorded in the PR — never asserted here.
sh64_root=$TEST_TMP/backupcache
mkdir -p "$sh64_root/tracer"
cat > "$sh64_root/tracer/sitecustomize.py" <<'TRACER'
import os, sqlite3

_trace = os.environ.get("PE_SQLITE_TRACE")
if _trace:
    _log = open(_trace, "a", buffering=1)
    _connect = sqlite3.connect
    _counter = [0]
    _busy = [False]

    def _watch(connection, token):
        def callback(statement):
            if _busy[0] or "integrity_check" not in " ".join(str(statement).split()).lower():
                return
            _busy[0] = True                      # the read-back is itself a statement
            try:
                effective = sqlite3.Connection.execute(
                    connection, "pragma cache_size").fetchone()[0]
                _log.write("%s\t%s\n" % (token, effective))
            finally:
                _busy[0] = False
        connection.set_trace_callback(callback)

    def connect(*args, **kwargs):
        con = _connect(*args, **kwargs)
        _counter[0] += 1
        _watch(con, "c%d" % _counter[0])         # token is never reused, unlike id()
        return con

    sqlite3.connect = connect
TRACER
python3 - "$sh64_root/source.db" <<'SEED' || fail "FE-BACKUPCACHE-64 could not seed the fixture database"
import sqlite3, sys
con = sqlite3.connect(sys.argv[1])
con.execute("create table t(a integer primary key, b text)")
con.executemany("insert into t(b) values(?)", [("row %d" % i,) for i in range(256)])
con.commit(); con.close()
SEED

# ONE evaluator, shared by the positive path and every negative control: a trace is "tuned" only
# when it contains at least one observation and every observation reports -262144.
cat > "$sh64_root/evaluate.py" <<'EVAL'
import sys
rows = [l.rstrip("\n").split("\t", 1) for l in open(sys.argv[1]) if "\t" in l]
if not rows:
    print("none"); raise SystemExit(0)
print("tuned" if all(v.strip() == "-262144" for _, v in rows) else "untuned")
EVAL
sh64_eval() { python3 "$sh64_root/evaluate.py" "$1"; }

sed -n '/^complete_sqlite_backup() {/,/^}/p' "$DRIVER" > "$sh64_root/backup.sh"
sed -n '/^restore_sqlite_backup() {/,/^}/p' "$DRIVER" > "$sh64_root/restore.sh"
[[ -s "$sh64_root/backup.sh" && -s "$sh64_root/restore.sh" ]] ||
  fail "FE-BACKUPCACHE-64 could not extract the SQLite maintenance owners"
# Bound what the tracer can see: these owners must open connections only through sqlite3.connect.
for sh64_owner in "$sh64_root/backup.sh" "$sh64_root/restore.sh"; do
  grep -qE 'sqlite3\.(Connection\(|dbapi2\.)' "$sh64_owner" &&
    fail "FE-BACKUPCACHE-64 $(basename "$sh64_owner") constructs a connection the tracer cannot see"
done

PYTHONPATH="$sh64_root/tracer" PE_SQLITE_TRACE="$sh64_root/backup.trace" \
  bash -c "source '$sh64_root/backup.sh'; complete_sqlite_backup '$sh64_root/source.db' '$sh64_root/backup.db'" ||
  fail "FE-BACKUPCACHE-64 complete_sqlite_backup failed"
[[ "$(sh64_eval "$sh64_root/backup.trace")" == tuned ]] ||
  fail "FE-BACKUPCACHE-64 complete_sqlite_backup checked with an untuned or unobserved connection"
[[ -s "$sh64_root/backup.db" ]] || fail "FE-BACKUPCACHE-64 complete_sqlite_backup produced no destination"

PYTHONPATH="$sh64_root/tracer" PE_SQLITE_TRACE="$sh64_root/restore.trace" \
  bash -c "source '$sh64_root/restore.sh'; restore_sqlite_backup '$sh64_root/backup.db' '$sh64_root/restored.db'" ||
  fail "FE-BACKUPCACHE-64 restore_sqlite_backup failed"
[[ "$(sh64_eval "$sh64_root/restore.trace")" == tuned ]] ||
  fail "FE-BACKUPCACHE-64 restore_sqlite_backup checked with an untuned or unobserved connection"
[[ -s "$sh64_root/restored.db" ]] || fail "FE-BACKUPCACHE-64 restore_sqlite_backup produced no destination"

# Negative controls. Each must be OBSERVED and judged untuned: a control that merely produces an
# empty trace would pass vacuously, so "none" is a failure of the control itself.
sh64_reject() {                      # <label> <python body>
  local label=$1 body=$2 trace=$sh64_root/neg-$1.trace verdict
  rm -f "$trace"
  PYTHONPATH="$sh64_root/tracer" PE_SQLITE_TRACE="$trace" python3 -c "$body" "$sh64_root/source.db" ||
    fail "FE-BACKUPCACHE-64 negative control $label did not run"
  verdict=$(sh64_eval "$trace")
  [[ "$verdict" == none ]] &&
    fail "FE-BACKUPCACHE-64 negative control $label observed nothing; the tracer is not instrumenting it"
  [[ "$verdict" == untuned ]] ||
    fail "FE-BACKUPCACHE-64 accepted the '$label' negative control"
}
sh64_reject no-pragma 'import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute("pragma integrity_check"); c.close()'
sh64_reject cursor-override 'import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute("pragma cache_size=-262144")
c.cursor().execute("pragma cache_size=-2000"); c.execute("pragma integrity_check"); c.close()'
sh64_reject reset-before-check 'import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute("pragma cache_size=-262144")
c.execute("pragma cache_size=-2000"); c.execute("pragma integrity_check"); c.close()'
sh64_reject reopened-connection 'import gc,sqlite3,sys
a=sqlite3.connect(sys.argv[1]); a.execute("pragma cache_size=-262144"); a.close(); del a; gc.collect()
b=sqlite3.connect(sys.argv[1]); b.execute("pragma integrity_check"); b.close()'
sh64_reject second-untuned-check 'import sqlite3,sys
a=sqlite3.connect(sys.argv[1]); a.execute("pragma cache_size=-262144"); a.execute("pragma integrity_check")
b=sqlite3.connect(sys.argv[1]); b.execute("pragma integrity_check"); a.close(); b.close()'
sh64_reject cursor-issued-check 'import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.cursor().execute("pragma integrity_check"); c.close()'
sh64_reject script-reset-then-check 'import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute("pragma cache_size=-262144")
c.executescript("pragma cache_size=-2000; pragma integrity_check;"); c.close()'
sh64_reject script-two-checks 'import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute("pragma cache_size=-262144")
c.executescript("pragma integrity_check; pragma cache_size=-2000; pragma integrity_check;"); c.close()'
sh64_reject shortcut-cursor-check 'import sqlite3,sys
c=sqlite3.connect(sys.argv[1]); c.execute("pragma cache_size=-262144"); c.execute("pragma integrity_check")
cur=c.execute("select 1"); c.execute("pragma cache_size=-2000")
cur.execute("pragma integrity_check").fetchone(); c.close()'
echo "PASS: FE-BACKUPCACHE-64"

echo "PASS: 69 scenario contracts, including the pre-stop and immediate post-stop financial-era preflight, WAL-only rollback and resumed-write preservation; ${#forward_boundaries[@]} network-free forward hooks and ${#rollback_boundaries[@]} mutation-observed rollback hooks converge; ${#wal_restore_boundaries[@]} WAL restore hooks converge; PostgreSQL execution remains shimmed"
