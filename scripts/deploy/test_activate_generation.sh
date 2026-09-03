#!/usr/bin/env bash
# Deterministic, network-free crash matrix for the generation activation drivers.

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

write_shims() {
  local bin=$1
  mkdir -p "$bin"

  cat > "$bin/b3sum" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
sha256sum "$@"
SH

  cat > "$bin/date" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1-}" == +%s ]]; then echo 2000000000; else /usr/bin/date "$@"; fi
SH

  cat > "$bin/pgrep" <<'SH'
#!/usr/bin/env bash
exit 1
SH

  cat > "$bin/ssh" <<'SH'
#!/usr/bin/env bash
echo "network access is forbidden in this test" >&2
exit 90
SH

  cat > "$bin/curl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
output=
while (($#)); do
  case "$1" in
    --output) output=$2; shift 2 ;;
    *) shift ;;
  esac
done
[[ -z "$output" ]] || printf '%s\n' '{"code":"42501"}' > "$output"
printf '401'
SH

  cat > "$bin/sqlite3" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
separator=
while (($#)); do
  case "$1" in
    -readonly) shift ;;
    -separator) separator=$2; shift 2 ;;
    *) break ;;
  esac
done
db=${1:?missing database}; shift
sql=${1-}
[[ -n "$sql" ]] || sql=$(cat)
if [[ "$sql" == ".backup '"*"'" ]]; then
  target=${sql#".backup '"}
  target=${target%"'"}
  cp "${db#file:}" "$target"
elif [[ "$sql" == *"from sqlite_schema"* ]]; then
  printf '%s\n' bankroll dispatch_seeds dispatch_targets fill_market_snapshots fills \
    leader_positions meta no_copy_dispositions poll_cursors positions seen_trades settled_markets
elif [[ "$sql" == *"pragma user_version"* ]]; then
  path=${db#file:}; path=${path%%\?*}
  if [[ -e "$(dirname "$path")/prepared.marker" ]]; then echo 2; else echo 1; fi
elif [[ "$sql" == *"pragma integrity_check"* ]]; then
  echo ok
elif [[ "$sql" == *"last_ts_unix <> activity_cutoff_unix"* ]]; then
  echo '0 0 0 0 0'
elif [[ "$sql" == *"select count(*) from position_anchors"* ]]; then
  echo 2
elif [[ "$sql" == *"select case when exists(select 1 from wallet_fences"* ]]; then
  echo 0
elif [[ "$sql" == *"select count(*) from"* || "$sql" == *"select (select count(*)"* ]]; then
  echo 0
fi
SH

  cat > "$bin/psql" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
state=${PE_ACTIVATION_TEST_ROOT:?}/test-state
mkdir -p "$state"
sql=
file=
while (($#)); do
  case "$1" in
    -c|-Atc) sql=$2; shift 2 ;;
    -f) file=$2; shift 2 ;;
    -v) shift 2 ;;
    *) shift ;;
  esac
done
if [[ -z "$sql" && -z "$file" && ! -t 0 ]]; then sql=$(cat); fi
if [[ "$file" == *archive_paper_state.sql ]]; then
  if [[ ! -e "$state/archive-stamp" ]]; then
    echo 1 > "$state/archive-count"
    : > "$state/archive-stamp"
  fi
  : > "$state/db-reset"
  rm -f "$state/db-restored"
elif [[ "$file" == *restore_paper_state.sql ]]; then
  : > "$state/db-restored"
  count=0; [[ ! -f "$state/restore-count" ]] || count=$(<"$state/restore-count")
  echo $((count + 1)) > "$state/restore-count"
elif [[ "$sql" == *"select max(batch_id)"* ]]; then
  echo 7
elif [[ "$sql" == *"select lower(wallet_hex)"* ]]; then
  echo 0x0000000000000000000000000000000000000557
elif [[ "$sql" == *"json_build_object('paper_fills'"* ]]; then
  echo '{"paper_fills":3,"settled_markets":2,"paper_positions":2,"paper_bankroll":1,"fill_market_snapshots":1}'
elif [[ "$sql" == *"paper_fills_archive where activation_id"* && "$sql" == *" || ' ' ||"* ]]; then
  if [[ -e "$state/archive-stamp" ]]; then echo '3 2 2 1 1'; else echo '0 0 0 0 0'; fi
elif [[ "$sql" == *"Supabase fresh"* ]]; then
  echo ok
elif [[ "$sql" == *"paper_bankroll where id=0"* ]]; then
  [[ -e "$state/db-reset" ]] && echo ok || echo mismatch
elif [[ "$sql" == *"then 'ok' else 'mismatch'"* && "$sql" == *"paper_fills_archive"* ]]; then
  [[ -e "$state/db-restored" ]] && echo ok || echo mismatch
elif [[ "$sql" == *"pe-rehearsal-canary-557"* ]]; then
  echo 0
elif [[ "$sql" == *"select count(*) from service_watchlist"* ]]; then
  echo ok
fi
SH

  cat > "$bin/systemctl" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
root=${PE_ACTIVATION_TEST_ROOT:?}
state="$root/test-state"
mkdir -p "$state"
user=false
if [[ "${1-}" == --user ]]; then user=true; shift; fi
action=${1:?}; shift
unit=${*: -1}
if [[ "$unit" == pe-rank-loop ]]; then prefix=forge; else prefix=service; fi
read_bit() { [[ -e "$state/$1" ]] && cat "$state/$1" || echo false; }
write_bit() { printf '%s\n' "$2" > "$state/$1"; }
start_service() {
  if [[ "$(read_bit service.active)" != true ]]; then
    count=0; [[ ! -f "$state/service-start-count" ]] || count=$(<"$state/service-start-count")
    echo $((count + 1)) > "$state/service-start-count"
  fi
  write_bit service.active true
  env_file="$root/prediction-markets/.env"
  if [[ -f "$env_file" ]]; then
    set -a; source "$env_file"; set +a
    mkdir -p "$(dirname "$PE_STATUS_PATH")"
    cat > "$PE_STATUS_PATH" <<JSON
{"revision":"0123456789abcdef0123456789abcdef01234567","bankroll":"1000.00","fills_total":0,"settled_total":0,"tasks":[{"name":"activity_ingest","state":"running","class":"critical"},{"name":"public_activity_poll","state":"running","class":"critical"},{"name":"orchestrator","state":"running","class":"critical"},{"name":"resolution_poller","state":"running","class":"critical"},{"name":"watchlist_refresh","state":"running","class":"critical"},{"name":"status_writer","state":"running","class":"critical"},{"name":"http_server","state":"running","class":"critical"}],"watchlist_projection":{"applied":"2000-01-01T00:00:00Z","last_error":null}}
JSON
  fi
}
case "$action" in
  is-active) [[ "$(read_bit "$prefix.active")" == true ]]
    ;;
  is-enabled) [[ "$(read_bit "$prefix.enabled")" == true ]]
    ;;
  stop) write_bit "$prefix.active" false ;;
  start)
    if [[ "$prefix" == service ]]; then start_service; else write_bit forge.active true; fi
    ;;
  disable) write_bit "$prefix.enabled" false ;;
  enable)
    write_bit "$prefix.enabled" true
    if [[ "${1-}" == --now || "${2-}" == --now ]]; then
      if [[ "$prefix" == service ]]; then start_service; else write_bit forge.active true; fi
    fi
    ;;
  show)
    if [[ "$*" == *InvocationID* ]]; then echo invocation-test-1; else echo 1234; fi
    ;;
  *) echo "unsupported fake systemctl action: $action" >&2; exit 2 ;;
esac
SH

  chmod +x "$bin"/*
}

make_case() {
  local name=$1 root service
  root="$TEST_TMP/$name"
  service="$root/prediction-markets"
  mkdir -p "$service/target/release" "$service/smoke-test" \
    "$service/data/eval-results" "$service/data" "$root/input" "$root/gen/557" \
    "$root/test-state"
  write_shims "$root/bin"
  printf '%s\n' old-binary > "$service/target/release/pe-service"
  chmod +x "$service/target/release/pe-service"
  cat > "$service/smoke-test/service.toml" <<'TOML'
bind = "127.0.0.1:9000"
event_log_path = "old/paper.log"
source_event_log_path = "old/source_events.log"
jsonl_log_path = "old/paper.jsonl"
status_path = "old/status.json"
paper_state_db_path = "old/paper_state.db"
legacy_wallet_history_path = "old/wallet_market_history.json"
TOML
  cat > "$service/.env" <<EOF
SUPABASE_DB_URL=postgres://test
PE_SUPABASE_URL=https://invalid.example
PE_SUPABASE_ANON_KEY=anon-test
PE_SUPABASE_SECRET_KEY=service-test
PE_BIND=127.0.0.1:9000
PE_EVENT_LOG_PATH=$service/old/paper.log
PE_SOURCE_EVENT_LOG_PATH=$service/old/source_events.log
PE_JSONL_LOG_PATH=$service/old/paper.jsonl
PE_STATUS_PATH=$service/old/status.json
PE_PAPER_STATE_DB_PATH=$service/old/paper_state.db
PE_LEGACY_WALLET_HISTORY_PATH=$service/old/wallet_market_history.json
EOF
  mkdir -p "$service/old"
  printf '%s\n' old-paper > "$service/old/paper.log"
  printf '%s\n' old-source > "$service/old/source_events.log"
  printf '%s\n' old-live > "$service/old/live_journal.log"
  printf '%s\n' old-db > "$service/old/paper_state.db"
  printf '%s\n' old-history > "$service/old/wallet_market_history.json"
  printf '%s\n' '{"old":true}' > "$service/old/status.json"
  printf '%s\n' source-v1 > "$root/input/source-v1.db"
  printf '%s\n' '[{"legacy":true}]' > "$root/input/history.json"
  cat > "$root/input/new-pe-service" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1-}" == --verify-staged-revision ]]; then exit 0; fi
: "${PE_PAPER_STATE_DB_PATH:?}"
generation=$(dirname "$PE_PAPER_STATE_DB_PATH")
: > "$generation/prepared.marker"
printf '%s\n' prepared-source-frame >> "$PE_SOURCE_EVENT_LOG_PATH"
SH
  chmod +x "$root/input/new-pe-service"
  cp "$service/smoke-test/service.toml" "$root/input/service.toml"
  cp "$service/.env" "$root/input/service.env"
  cp "$service/.env" "$root/input/rehearsal.env"
  printf '%s\n' run > "$service/data/eval-results/rank_and_push.loop"
  printf '%s\n' true > "$root/test-state/service.active"
  printf '%s\n' true > "$root/test-state/service.enabled"
  printf '%s\n' true > "$root/test-state/forge.active"
  printf '%s\n' false > "$root/test-state/forge.enabled"
  echo "$root"
}

activate() {
  local root=$1 id=${2:-activation-557}; shift 2 || true
  env PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" \
    SUPABASE_DB_URL=postgres://test PATH="$root/bin:$PATH" \
    "$SCRIPT_DIR/activate_generation.sh" \
      --activation-id "$id" --generation-dir "$root/gen/557" \
      --source-v1-main "$root/input/source-v1.db" \
      --legacy-history "$root/input/history.json" \
      --staged-binary "$root/input/new-pe-service" \
      --config-template "$root/input/service.toml" \
      --rehearsal-env "$root/input/rehearsal.env" \
      --service-env "$root/input/service.env" \
      --merge-commit 0123456789abcdef0123456789abcdef01234567 \
      --bind 127.0.0.1:9100 --rehearsal-bind 127.0.0.1:9200 \
      --bankroll 1000.00 "$@"
}

rollback() {
  local root=$1; shift
  env PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" \
    SUPABASE_DB_URL=postgres://test PATH="$root/bin:$PATH" \
    "$SCRIPT_DIR/rollback_generation.sh" --activation-id activation-557 "$@"
}

assert_verified() {
  local root=$1 state starts stamps
  state=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' \
    "$root/pe-activation.json")
  [[ "$state" == verified ]] || fail "$root ended in state $state"
  starts=$(<"$root/test-state/service-start-count")
  [[ "$starts" == 1 ]] || fail "$root started the fresh service $starts times"
  stamps=$(<"$root/test-state/archive-count")
  [[ "$stamps" == 1 ]] || fail "$root created $stamps archive stamps"
  [[ "$(<"$root/test-state/forge.enabled")" == false ]] || fail "Forge enablement changed"
  [[ "$(<"$root/test-state/forge.active")" == true ]] || fail "Forge activity changed"
  [[ "$(<"$root/prediction-markets/data/eval-results/rank_and_push.loop")" == run ]] ||
    fail "Forge flag changed"
}

run_crash_case() {
  local boundary=$1 safe root rc
  safe=${boundary//[^A-Za-z0-9]/_}
  if [[ "${PE_ACTIVATION_TEST_VERBOSE:-0}" == 1 ]]; then
    echo "test activation boundary: $boundary"
  fi
  root=$(make_case "crash-$safe")
  set +e
  activate "$root" activation-557 --simulate-crash-after "$boundary" \
    >"$root/first.out" 2>"$root/first.err"
  rc=$?
  set -e
  if [[ "$rc" != 86 ]]; then
    sed -n '1,80p' "$root/first.err" >&2
    fail "$boundary returned $rc, expected simulated crash 86"
  fi
  if ! activate "$root" activation-557 >"$root/resume.out" 2>"$root/resume.err"; then
    sed -n '1,120p' "$root/resume.err" >&2
    sed -n '1,120p' "$root/resume.out" >&2
    fail "$boundary did not resume"
  fi
  assert_verified "$root"
}

manifest_boundaries=(
  seed prepared prechecked-recorded prechecked guarded archived reset switched started verified
)
for state in "${manifest_boundaries[@]}"; do
  run_crash_case "before-manifest-$state"
  run_crash_case "$state"
  run_crash_case "after-manifest-$state"
done

artifact_boundaries=(
  stage-binary archive-paper-state archive-paper_log archive-source_log archive-live_journal
  archive-legacy_history archive-config archive-env archive-binary archive-status
  adopted-config adopted-env adopted-binary db-commit service-started
)
for boundary in "${artifact_boundaries[@]}"; do run_crash_case "$boundary"; done

# A lock contender cannot enter while the first process holds the whole-run lock.
root=$(make_case lock)
hold="$root/hold"
: > "$hold"
env PE_ACTIVATION_TEST_HOLD_LOCK_FILE="$hold" \
  PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$root" \
  SUPABASE_DB_URL=postgres://test PATH="$root/bin:$PATH" \
  "$SCRIPT_DIR/activate_generation.sh" --activation-id activation-557 \
    --generation-dir "$root/gen/557" --source-v1-main "$root/input/source-v1.db" \
    --legacy-history "$root/input/history.json" --staged-binary "$root/input/new-pe-service" \
    --config-template "$root/input/service.toml" --rehearsal-env "$root/input/rehearsal.env" \
    --service-env "$root/input/service.env" --merge-commit 0123456789abcdef0123456789abcdef01234567 \
    --bind 127.0.0.1:9100 --rehearsal-bind 127.0.0.1:9200 --bankroll 1000.00 \
    >"$root/holder.out" 2>"$root/holder.err" &
holder=$!
for _ in {1..100}; do [[ -e "$hold.ready" ]] && break; sleep 0.02; done
[[ -e "$hold.ready" ]] || fail "lock holder did not reach its test seam"
set +e
activate "$root" activation-557 >"$root/contender.out" 2>"$root/contender.err"
rc=$?
set -e
[[ "$rc" == 1 ]] || fail "lock contender returned $rc"
grep -q 'another generation driver holds' "$root/contender.err" || fail "lock refusal was not explicit"
rm "$hold"
wait "$holder"
assert_verified "$root"

# A different id is rejected while durable state is non-terminal.
root=$(make_case different-id)
set +e
activate "$root" activation-557 --simulate-crash-after guarded >/dev/null 2>"$root/crash.err"
rc=$?
set -e
[[ "$rc" == 86 ]] || fail "guarded reboot seam did not crash"
set +e
activate "$root" other-activation >"$root/other.out" 2>"$root/other.err"
rc=$?
set -e
[[ "$rc" == 1 ]] || fail "different activation id was not refused"
grep -q 'non-terminal' "$root/other.err" || fail "different-id refusal was not explicit"
activate "$root" activation-557 >/dev/null
assert_verified "$root"

# Explicit host-reboot resumes from the durable guarded/reset/switched states.
for state in guarded reset switched; do
  root=$(make_case "reboot-$state")
  set +e
  activate "$root" activation-557 --simulate-crash-after "$state" >/dev/null 2>"$root/reboot.err"
  rc=$?
  set -e
  [[ "$rc" == 86 ]] || fail "reboot after $state did not reach its seam"
  activate "$root" activation-557 >/dev/null
  assert_verified "$root"
done

# Mid-rollback repeats the restore transaction safely; a crash after start adopts it.
for boundary in rollback-db-restored rollback-started before-manifest-rolling_back \
  rolling_back after-manifest-rolling_back before-manifest-rolled_back rolled_back \
  after-manifest-rolled_back; do
  safe=${boundary//[^A-Za-z0-9]/_}
  root=$(make_case "rollback-$safe")
  activate "$root" activation-557 >/dev/null
  set +e
  rollback "$root" --simulate-crash-after "$boundary" >"$root/rollback.out" 2>"$root/rollback.err"
  rc=$?
  set -e
  [[ "$rc" == 86 ]] || fail "rollback $boundary returned $rc"
  rollback "$root" >/dev/null
  state=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' \
    "$root/pe-activation.json")
  [[ "$state" == rolled_back ]] || fail "rollback $boundary ended at $state"
  [[ -e "$root/test-state/db-restored" ]] || fail "rollback did not restore database"
  [[ "$(<"$root/test-state/archive-count")" == 1 ]] || fail "rollback duplicated archive stamp"
  [[ "$(sha256sum "$root/prediction-markets/target/release/pe-service" | awk '{print $1}')" == \
     "$(printf '%s\n' old-binary | sha256sum | awk '{print $1}')" ]] || fail "old binary not restored"
  starts=$(<"$root/test-state/service-start-count")
  [[ "$starts" == 2 ]] || fail "rollback $boundary caused $starts total service starts"
  set +e
  activate "$root" activation-557 >/dev/null 2>"$root/forward.err"
  rc=$?
  set -e
  [[ "$rc" == 1 ]] || fail "forward rerun of rolled-back id was accepted"
done

echo "activation crash matrix: PASS"
echo "archive stamp exactly once and fresh service starts at most once: PASS"
echo "rollback restore/adoption and forward-refusal matrix: PASS"
echo "lock, different-id, reboot, and independent Forge restoration: PASS"
