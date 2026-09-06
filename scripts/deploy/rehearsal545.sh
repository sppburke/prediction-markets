#!/usr/bin/env bash
# Bounded final-head production rehearsal for issue #545.

set -euo pipefail

usage() {
  echo "usage: $0 [--dry-run] 40_HEX_GIT_SHA" >&2
  exit 2
}

dry_run=0
if [[ "${1:-}" == "--dry-run" ]]; then
  dry_run=1
  shift
fi
[[ $# -eq 1 ]] || usage
sha=$1
[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || usage
short=${sha:0:7}

root=${PE_REHEARSAL_ROOT:-"$HOME/rehearsal545"}
activation_manifest=${PE_ACTIVATION_MANIFEST:-"$HOME/pe-activation.json"}
release_root=${PE_REHEARSAL_RELEASE_ROOT:-"$HOME/releases/pe-545-$short"}
binary=${PE_REHEARSAL_BINARY:-"$release_root/target/release/pe-service"}
config=${PE_REHEARSAL_CONFIG:-"$root/service.toml"}
env_file=${PE_REHEARSAL_ENV:-"$root/.env"}
copy_dir=${PE_REHEARSAL_COPY_DIR:-"$root/gen-$short"}
rehearsal_bind=${PE_REHEARSAL_BIND:-}
timeout_secs=${PE_REHEARSAL_TIMEOUT_SECS:-10800}
poll_secs=${PE_REHEARSAL_POLL_SECS:-10}
evidence_hash_file=${PE_REHEARSAL_EVIDENCE_HASH_FILE:-"$root/evidence-$short.json"}

if [[ "$dry_run" == 1 ]]; then
  printf '%s\n' \
    "REHEARSAL545_DRY_RUN=1" \
    "sha=$sha" \
    "activation_manifest=$activation_manifest" \
    "active_generation=read-from-activation-manifest" \
    "copy_dir=$copy_dir" \
    "binary=$binary" \
    "config=$config" \
    "env=$env_file" \
    "rehearsal_bind=${rehearsal_bind:-required}" \
    "credential_slots=PE_SUPABASE_ANON_KEY,PE_SUPABASE_SECRET_KEY(equal)" \
    "logs=paper.log,source_events.log,live_journal.log" \
    "observers=status_file_poller,health_ready_query,reader_drop_classifier,fence_anchor_census,write_refusal_counter" \
    "proof=exact-revision,poll-after-start,reanchor,real-readiness,critical-health,accounts-off-unarmed,no-unsafe-evidence" \
    "stop=first-complete-evidence-or-first-failure-or-timeout" \
    "evidence_contract=rehearsal545-evidence-v1" \
    "evidence_hash_file=$evidence_hash_file"
  exit 0
fi

for required in python3 sqlite3 sha256sum od cp grep awk sed flock curl env; do
  command -v "$required" >/dev/null 2>&1 || {
    echo "FATAL: required command is unavailable: $required" >&2
    exit 1
  }
done
[[ -x "$binary" ]] || { echo "FATAL: staged binary is not executable: $binary" >&2; exit 1; }
[[ -f "$config" ]] || { echo "FATAL: rehearsal config is missing: $config" >&2; exit 1; }
[[ -f "$env_file" ]] || { echo "FATAL: rehearsal environment is missing: $env_file" >&2; exit 1; }
[[ -n "$rehearsal_bind" ]] || {
  echo "FATAL: PE_REHEARSAL_BIND is required" >&2
  exit 1
}
[[ "$timeout_secs" =~ ^[1-9][0-9]*$ && "$poll_secs" =~ ^[1-9][0-9]*$ ]] || {
  echo "FATAL: rehearsal timeout and poll cadence must be positive integers" >&2
  exit 1
}

staged_identity_output=$("$binary" --verify-staged-identity) || {
  echo "FATAL: rehearsal binary could not derive its own identity" >&2
  exit 1
}
read -r target_revision artifact_blake3 < <(python3 -c 'import re,sys
match=re.fullmatch(r"prediction-edge revision=([0-9a-f]{40}) artifact_blake3=([0-9a-f]{64})\n?",sys.stdin.read())
if match is None: raise SystemExit(1)
print(*match.groups())' <<< "$staged_identity_output") || {
  echo "FATAL: rehearsal binary identity output is invalid" >&2
  exit 1
}
[[ "$target_revision" == "$sha" ]] || {
  echo "FATAL: rehearsal binary revision does not match the reviewed Git identity" >&2
  exit 1
}
"$binary" --verify-staged-identity "$target_revision" "$artifact_blake3" >/dev/null || {
  echo "FATAL: rehearsal binary identity self-verification failed" >&2
  exit 1
}
artifact_sha256=$(sha256sum "$binary" | awk '{print $1}')

readarray -t activation < <(python3 - "$activation_manifest" <<'PY'
import json, os, sys
with open(sys.argv[1], encoding="utf-8") as source:
    value = json.load(source)
if value.get("state") != "verified":
    raise SystemExit("activation manifest state is not verified")
generation = value.get("generation_dir")
activation_id = value.get("activation_id")
destinations = value.get("destinations")
if not isinstance(generation, str) or not os.path.isabs(generation):
    raise SystemExit("activation manifest has no absolute generation_dir")
if not isinstance(activation_id, str) or not activation_id:
    raise SystemExit("activation manifest has no activation_id")
if not isinstance(destinations, dict):
    raise SystemExit("activation manifest has no installed artifact authority")
installed_config = destinations.get("config")
installed_environment = destinations.get("environment")
if not all(isinstance(path, str) and os.path.isabs(path)
           for path in (installed_config, installed_environment)):
    raise SystemExit("activation manifest has non-absolute installed artifact paths")
print(generation)
print(activation_id)
print(installed_config)
print(installed_environment)
PY
)
[[ ${#activation[@]} -eq 4 ]] || { echo "FATAL: malformed activation manifest" >&2; exit 1; }
active_generation=${activation[0]}
activation_id=${activation[1]}
installed_config=${activation[2]}
installed_environment=${activation[3]}

for installed in "$installed_config" "$installed_environment"; do
  [[ -f "$installed" && ! -L "$installed" ]] || {
    echo "FATAL: activation authority names a missing or linked installed artifact: $installed" >&2
    exit 1
  }
done
installed_bind=$(env -i HOME="$HOME" PATH="$PATH" /bin/bash -c '
set -eo pipefail
set +u
set -a
# shellcheck disable=SC1090
source "$1"
set +a
printf "%s" "${PE_BIND-}"
' bash "$installed_environment")
if [[ -z "$installed_bind" ]]; then
  installed_bind=$(python3 - "$installed_config" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as source:
    value = tomllib.load(source).get("bind", "127.0.0.1:8080")
if not isinstance(value, str):
    raise SystemExit("installed bind is not a string")
print(value)
PY
  )
fi
readarray -t bind_proof < <(python3 - "$rehearsal_bind" "$installed_bind" <<'PY'
import ipaddress, sys

def split(value):
    if value.startswith("["):
        close = value.find("]")
        if close < 0 or value[close + 1:close + 2] != ":":
            raise ValueError("expected [address]:port")
        host, port = value[1:close], value[close + 2:]
    else:
        host, separator, port = value.rpartition(":")
        if not separator or ":" in host:
            raise ValueError("expected address:port")
    if not port.isascii() or not port.isdecimal() or not 1 <= int(port) <= 65535:
        raise ValueError("port must be in 1..=65535")
    return host, int(port)

try:
    rehearsal_host, rehearsal_port = split(sys.argv[1])
    _, installed_port = split(sys.argv[2])
    address = ipaddress.ip_address(rehearsal_host)
except ValueError as error:
    raise SystemExit(f"invalid rehearsal/installed bind: {error}")
if not address.is_loopback:
    raise SystemExit("rehearsal bind must use a numeric loopback address")
if rehearsal_port == installed_port:
    raise SystemExit("rehearsal port must differ from the installed service port")
url_host = f"[{address}]" if address.version == 6 else str(address)
print(rehearsal_port)
print(f"http://{url_host}:{rehearsal_port}")
PY
)
[[ ${#bind_proof[@]} -eq 2 ]] || { echo "FATAL: invalid isolated rehearsal bind" >&2; exit 1; }
rehearsal_port=${bind_proof[0]}
readiness_base_url=${bind_proof[1]}

for name in paper_state.db paper.log source_events.log live_journal.log wallet_market_history.json; do
  [[ -f "$active_generation/$name" && ! -L "$active_generation/$name" ]] || {
    echo "FATAL: active generation is missing regular $name" >&2
    exit 1
  }
done
for name in paper.log source_events.log live_journal.log; do
  magic=$(od -An -tx1 -N5 "$active_generation/$name" | tr -d ' \n')
  [[ "$magic" == "4544474501" ]] || {
    echo "FATAL: active $name is not an EDGE-v1 log" >&2
    exit 1
  }
done

mkdir -p "$root" "$copy_dir"
exec 7<>"$root/.rehearsal545.lock"
flock -n 7 || { echo "FATAL: another #545 rehearsal is running" >&2; exit 1; }
copy_manifest="$copy_dir/copied.sha256"
source_identity="$copy_dir/source.identity"
[[ ! -L "$copy_manifest" && ! -L "$source_identity" ]] || {
  echo "FATAL: rehearsal copy authority files must not be symlinks" >&2
  exit 1
}
validate_copy_manifest() {
  python3 - "$copy_manifest" <<'PY'
import re, sys

expected = {
    "paper_state.db", "paper.log", "source_events.log", "live_journal.log",
    "wallet_market_history.json", "source.identity",
}
seen = set()
with open(sys.argv[1], encoding="utf-8") as source:
    for line in source:
        match = re.fullmatch(r"[0-9a-f]{64}  ([^\n]+)\n?", line)
        if match is None or match.group(1) in seen:
            raise SystemExit("malformed rehearsal copy hash manifest")
        seen.add(match.group(1))
if seen != expected:
    raise SystemExit("rehearsal copy hash manifest has the wrong file inventory")
PY
  (
    cd "$copy_dir"
    sha256sum --strict -c copied.sha256 >/dev/null
  )
}
if [[ -f "$copy_manifest" ]]; then
  [[ -f "$source_identity" ]] || { echo "FATAL: rehearsal source identity is missing" >&2; exit 1; }
  grep -Fxq "activation_id=$activation_id" "$source_identity" \
    && grep -Fxq "generation_dir=$active_generation" "$source_identity" || {
      echo "FATAL: existing rehearsal copy belongs to another active generation" >&2
      exit 1
    }
  for name in paper_state.db paper.log source_events.log live_journal.log wallet_market_history.json; do
    [[ -f "$copy_dir/$name" && ! -L "$copy_dir/$name" ]] || {
      echo "FATAL: reusable rehearsal copy is missing regular $name" >&2
      exit 1
    }
  done
  validate_copy_manifest || {
    echo "FATAL: reusable rehearsal copy failed recorded hash validation" >&2
    exit 1
  }
  [[ "$(sqlite3 -readonly "$copy_dir/paper_state.db" 'pragma quick_check;')" == ok ]] || {
    echo "FATAL: reusable rehearsal database failed SQLite integrity" >&2
    exit 1
  }
  for name in paper.log source_events.log live_journal.log; do
    magic=$(od -An -tx1 -N5 "$copy_dir/$name" | tr -d ' \n')
    [[ "$magic" == "4544474501" ]] || {
      echo "FATAL: reusable $name lost its EDGE-v1 framing" >&2
      exit 1
    }
  done
else
  if find "$copy_dir" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
    echo "FATAL: uncheckpointed rehearsal copy exists: $copy_dir" >&2
    exit 1
  fi
  sqlite3 -readonly "$active_generation/paper_state.db" ".backup '$copy_dir/paper_state.db'"
  for name in paper.log source_events.log live_journal.log wallet_market_history.json; do
    cp -p "$active_generation/$name" "$copy_dir/$name"
  done
  printf 'activation_id=%s\ngeneration_dir=%s\n' "$activation_id" "$active_generation" \
    > "$source_identity"
  (
    cd "$copy_dir"
    sha256sum paper_state.db paper.log source_events.log live_journal.log \
      wallet_market_history.json source.identity > copied.sha256.tmp
    mv copied.sha256.tmp copied.sha256
  )
  validate_copy_manifest || {
    echo "FATAL: new rehearsal copy failed recorded hash validation" >&2
    exit 1
  }
fi
copy_manifest_sha256=$(sha256sum "$copy_manifest" | awk '{print $1}')

env_file_sha256=$(sha256sum "$env_file" | awk '{print $1}')
env -i PATH="$PATH" HOME="$HOME" LANG="${LANG:-C.UTF-8}" \
  /bin/bash "$release_root/scripts/deploy/rehearsal_preflight.sh" "$env_file"
[[ "$(sha256sum "$env_file" | awk '{print $1}')" == "$env_file_sha256" ]] || {
  echo "FATAL: rehearsal environment changed during privileged preflight" >&2
  exit 1
}

# Tier-1 allowlist: these are the environment names accepted by
# `crates/service/src/config.rs::load`, plus the two Rust diagnostics. Path,
# endpoint, and bind owners are appended below as fixed rehearsal overrides.
service_env_allowlist=(
  RUST_LOG RUST_BACKTRACE
  PE_POLYMARKET_CHANNEL_CAPACITY PE_TRADE_POLL_INTERVAL_SECS
  PE_POLYMARKET_ACTIVITY_WS_ENABLED PE_COPY_LATENCY_BUDGET_SECS
  PE_STATUS_INTERVAL_SECS PE_LOG_RETENTION_DAYS
  PE_GAMMA_RESOLUTION_POLL_INTERVAL_SECS PE_MAX_RESOLUTION_HORIZON_SECS
  PE_MIN_RESOLUTION_HORIZON_SECS PE_MAX_FILL_PRICE PE_MIN_FILL_PRICE
  PE_WATCHLIST_MEMBERSHIP_MODE PE_SUPABASE_URL PE_SUPABASE_ANON_KEY
  PE_SUPABASE_SECRET_KEY PE_SUPABASE_REFRESH_INTERVAL_SECS
  PE_SUPABASE_SINK_ENABLED PE_SUPABASE_SINK_CHANNEL_CAPACITY
  PE_SUPABASE_SINK_RECONCILE_INTERVAL_SECS PE_SUPABASE_AUTHORITATIVE
  PE_BANKROLL_USD PE_MODE PE_STRATEGY PE_POLYGON_RECEIPT_RPC_URL
)
mapfile -d '' -t service_child_env < <(
  env -i HOME="$HOME" PATH="$PATH" LANG="${LANG:-C.UTF-8}" /bin/bash -c '
set -euo pipefail
set -a
# shellcheck disable=SC1090
source "$1"
set +a
shift
for name in "$@"; do
  if [[ -v $name ]]; then printf "%s=%s\0" "$name" "${!name}"; fi
done
printf "__REHEARSAL_ENV_LOADED__\0"
' bash "$env_file" "${service_env_allowlist[@]}"
)
last_env_index=$((${#service_child_env[@]} - 1))
if ((last_env_index < 0)) || [[ "${service_child_env[$last_env_index]}" != __REHEARSAL_ENV_LOADED__ ]]; then
  echo "FATAL: could not load the allowlisted rehearsal environment" >&2
  exit 1
fi
unset 'service_child_env[last_env_index]'
[[ "$(sha256sum "$env_file" | awk '{print $1}')" == "$env_file_sha256" ]] || {
  echo "FATAL: rehearsal environment changed while constructing the service environment" >&2
  exit 1
}
anon_key=
secret_key=
for assignment in "${service_child_env[@]}"; do
  case "$assignment" in
    PE_SUPABASE_ANON_KEY=*) anon_key=${assignment#*=} ;;
    PE_SUPABASE_SECRET_KEY=*) secret_key=${assignment#*=} ;;
  esac
done
[[ -n "$anon_key" && -n "$secret_key" ]] || {
  echo "FATAL: both rehearsal Supabase credential slots are required" >&2
  exit 1
}
[[ "$anon_key" == "$secret_key" ]] || {
  echo "FATAL: the publishable key must occupy both Supabase credential slots" >&2
  exit 1
}

# Force the copied process onto the real first-party venue endpoints even when
# the staged static config was previously used with a local fixture.
service_child_env+=(
  "PE_POLYMARKET_BASE_URL=https://data-api.polymarket.com"
  "PE_POLYMARKET_CLOB_BASE_URL=https://clob.polymarket.com"
  "PE_GAMMA_BASE_URL=https://gamma-api.polymarket.com"
  "PE_BIND=$rehearsal_bind"
  "PE_EVENT_LOG_PATH=$copy_dir/paper.log"
  "PE_SOURCE_EVENT_LOG_PATH=$copy_dir/source_events.log"
  "PE_JSONL_LOG_PATH=$copy_dir/paper.jsonl"
  "PE_STATUS_PATH=$copy_dir/status.json"
  "PE_PAPER_STATE_DB_PATH=$copy_dir/paper_state.db"
  "PE_LEGACY_WALLET_HISTORY_PATH=$copy_dir/wallet_market_history.json"
)

wallet=$(sqlite3 "$copy_dir/paper_state.db" \
  "select c.wallet_hex from poll_cursors c join position_anchors a on a.wallet_hex=c.wallet_hex where c.activity_cutoff_unix is not null and c.reanchor_required=0 and c.wallet_hex not in (select wallet_hex from wallet_fences) order by c.wallet_hex limit 1;")
[[ "$wallet" =~ ^0x[0-9a-f]{40}$ ]] || {
  echo "FATAL: copied generation has no canonical eligible reanchor wallet" >&2
  exit 1
}
anchor_before=$(sqlite3 "$copy_dir/paper_state.db" \
  "select coalesce(max(anchor_seq),-1) from position_anchors where wallet_hex='$wallet';")
[[ "$anchor_before" =~ ^-?[0-9]+$ ]] || { echo "FATAL: invalid initial anchor sequence" >&2; exit 1; }
changed=$(sqlite3 "$copy_dir/paper_state.db" \
  "update poll_cursors set reanchor_required=1 where wallet_hex='$wallet'; select changes();")
[[ "$changed" == 1 ]] || { echo "FATAL: reanchor probe did not change exactly one row" >&2; exit 1; }

service_log="$root/service-$short.log"
watch_log="$root/watch-$short.log"
manifest="$root/manifest-$short.txt"
status_state="$root/status-$short.state"
drop_state="$root/drops-$short.state"
fence_state="$root/fences-$short.state"
write_state="$root/writes-$short.state"
readiness_response="$root/readiness-$short.json"
: > "$service_log"
: > "$watch_log"
rm -f "$status_state" "$drop_state" "$fence_state" "$write_state" \
  "$readiness_response" "$readiness_response.tmp"

service_pid=""
observer_pids=()
stop_all() {
  local pid
  for pid in "${observer_pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  if [[ -n "$service_pid" ]]; then
    kill -TERM "$service_pid" 2>/dev/null || true
    for _ in $(seq 1 15); do kill -0 "$service_pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$service_pid" 2>/dev/null || true
    wait "$service_pid" 2>/dev/null || true
  fi
  for pid in "${observer_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap stop_all EXIT TERM INT

env -i "${service_child_env[@]}" "$binary" "$config" > "$service_log" 2>&1 &
service_pid=$!
service_invocation_pid=$service_pid
started_at=$(date +%s)
printf 'REHEARSAL_START sha=%s activation=%s wallet=%s anchor_before=%s unix=%s\n' \
  "$sha" "$activation_id" "$wallet" "$anchor_before" "$started_at" | tee -a "$watch_log"

status_file_poller() {
  while kill -0 "$service_pid" 2>/dev/null; do
    if [[ -f "$copy_dir/status.json" ]]; then
      python3 - "$copy_dir/status.json" "$started_at" "$status_state" "$sha" <<'PY' || true
import datetime, json, os, sys
source, started, target, expected_revision = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
with open(source, encoding="utf-8") as handle:
    value = json.load(handle)
updated = datetime.datetime.fromisoformat(value["updated_at"].replace("Z", "+00:00")).timestamp()
fresh = updated >= started
revision_ok = value.get("revision") == expected_revision
health = value.get("source_health") or {}
polled = fresh and health.get("poll_last_round_age_secs") is not None
critical = [task for task in (value.get("tasks") or []) if task.get("class") == "critical"]
healthy = fresh and bool(critical) and all(task.get("state") == "running" for task in critical)
live = value.get("live") or {}
accounts = live.get("accounts") or []
accounts_safe = all(
    not account.get("armed", False)
    and account.get("requested_live_mode") == "off"
    and account.get("effective_live_mode") == "off"
    for account in accounts
)
temp = target + ".tmp"
with open(temp, "w", encoding="utf-8") as output:
    output.write(
        f"{int(fresh)} {int(revision_ok)} {int(polled)} {int(healthy)} "
        f"{int(accounts_safe)}\n"
    )
os.replace(temp, target)
PY
    fi
    sleep "$poll_secs"
  done
}

reader_drop_classifier() {
  while kill -0 "$service_pid" 2>/dev/null; do
    python3 - "$service_log" "$drop_state" <<'PY' || true
import json, os, sys
drops = credit_loss = unexpected_errors = 0
with open(sys.argv[1], encoding="utf-8", errors="replace") as source:
    for line in source:
        if "dropping socket" in line:
            drops += 1
            try: value = json.loads(line)
            except json.JSONDecodeError:
                credit_loss += 1
                continue
            fields = value.get("fields", value)
            raw = fields.get("last_wire_frame_age_secs")
            buffered = fields.get("buffered_frame_processed")
            age = raw if isinstance(raw, int) else None
            if isinstance(raw, str) and raw.startswith("Some(") and raw.endswith(")"):
                try: age = int(raw[5:-1])
                except ValueError: age = None
            if not (age is not None and age >= 29 and buffered is False):
                credit_loss += 1
        if '"level":"ERROR"' in line and not any(
            marker in line for marker in ("permission denied", "HTTP 401", "HTTP 403")
        ):
            unexpected_errors += 1
temp = sys.argv[2] + ".tmp"
with open(temp, "w", encoding="utf-8") as output:
    output.write(f"{drops} {credit_loss} {unexpected_errors}\n")
os.replace(temp, sys.argv[2])
PY
    sleep "$poll_secs"
  done
}

fence_anchor_census() {
  while kill -0 "$service_pid" 2>/dev/null; do
    anchor_after=$(sqlite3 "file:$copy_dir/paper_state.db?mode=ro" \
      "select coalesce(max(anchor_seq),-1) from position_anchors where wallet_hex='$wallet';" 2>/dev/null || echo "$anchor_before")
    reanchor=$(sqlite3 "file:$copy_dir/paper_state.db?mode=ro" \
      "select reanchor_required from poll_cursors where wallet_hex='$wallet';" 2>/dev/null || echo 1)
    unexpected=$(sqlite3 "file:$copy_dir/paper_state.db?mode=ro" \
      "select count(*) from wallet_fences where cause not in ('order_dependent_equal_second','position_underflow');" 2>/dev/null || echo 1)
    anchored=0
    [[ "$anchor_after" =~ ^-?[0-9]+$ && "$anchor_after" -gt "$anchor_before" && "$reanchor" == 0 ]] && anchored=1
    printf '%s %s\n' "$anchored" "$unexpected" > "$fence_state.tmp"
    mv "$fence_state.tmp" "$fence_state"
    sleep "$poll_secs"
  done
}

write_refusal_counter() {
  while kill -0 "$service_pid" 2>/dev/null; do
    refused=$(grep -a -c -E 'HTTP 40[13]|permission denied|projection failed|apply_resolution failed|commit_fill.*failed' "$service_log" || true)
    writes=$(grep -a -c -E '"message":"(fill committed|resolution applied|watchlist projection applied|paper_fill)"' "$service_log" || true)
    printf '%s %s\n' "$refused" "$writes" > "$write_state.tmp"
    mv "$write_state.tmp" "$write_state"
    sleep "$poll_secs"
  done
}

status_file_poller & observer_pids+=("$!")
reader_drop_classifier & observer_pids+=("$!")
fence_anchor_census & observer_pids+=("$!")
write_refusal_counter & observer_pids+=("$!")

result=FAIL
reason=timeout
deadline=$((started_at + timeout_secs))
last="status_fresh=0 endpoint_ready=0 revision_ok=0 polled=0 healthy=0 accounts_safe=0 anchored=0 drops=0 credit_loss=0 errors=0 fences=0 refused=0 writes=0"
while (( $(date +%s) <= deadline )); do
  if ! kill -0 "$service_pid" 2>/dev/null; then reason=process_exited; break; fi
  status_fresh=0; endpoint_ready=0; revision_ok=0; polled=0; healthy=0; accounts_safe=0; anchored=0
  drops=0; credit_loss=0; errors=0; fences=0; refused=0; writes=0
  [[ ! -f "$status_state" ]] || read -r status_fresh revision_ok polled healthy accounts_safe < "$status_state"
  [[ ! -f "$drop_state" ]] || read -r drops credit_loss errors < "$drop_state"
  [[ ! -f "$fence_state" ]] || read -r anchored fences < "$fence_state"
  [[ ! -f "$write_state" ]] || read -r refused writes < "$write_state"
  if env -i PATH="$PATH" LANG="${LANG:-C.UTF-8}" \
      curl --disable --silent --show-error --fail-with-body --noproxy '*' \
      --max-time "$poll_secs" \
      --output "$readiness_response.tmp" "$readiness_base_url/health/ready" \
      2>>"$watch_log"; then
    if python3 - "$readiness_response.tmp" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    value = json.load(source)
raise SystemExit(0 if isinstance(value, dict) and value.get("ready") is True
                 and value.get("issues", []) == [] else 1)
PY
    then
      endpoint_ready=1
    fi
  fi
  [[ ! -f "$readiness_response.tmp" ]] || mv "$readiness_response.tmp" "$readiness_response"
  last="status_fresh=$status_fresh endpoint_ready=$endpoint_ready revision_ok=$revision_ok polled=$polled healthy=$healthy accounts_safe=$accounts_safe anchored=$anchored drops=$drops credit_loss=$credit_loss errors=$errors fences=$fences refused=$refused writes=$writes"
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$last" >> "$watch_log"
  if (( credit_loss > 0 || errors > 0 || fences > 0 || writes > 0 )); then
    reason=unsafe_evidence
    break
  fi
  if (( status_fresh == 1 && endpoint_ready == 1 && revision_ok == 1 && polled == 1 && healthy == 1 && accounts_safe == 1 && anchored == 1 )); then
    result=PASS
    reason=evidence_complete
    break
  fi
  sleep "$poll_secs"
done

stop_all
service_pid=""
observer_pids=()
trap - EXIT TERM INT
readiness_response_sha256=absent
if [[ -f "$readiness_response" ]]; then
  readiness_response_sha256=$(sha256sum "$readiness_response" | awk '{print $1}')
fi
{
  printf 'result=%s\nreason=%s\nsha=%s\ntarget_revision=%s\nartifact_blake3=%s\nartifact_sha256=%s\nactivation_id=%s\nactive_generation=%s\nservice_invocation_pid=%s\nrehearsal_bind=%s\nrehearsal_port=%s\ninstalled_bind=%s\nreadiness_base_url=%s\nwallet=%s\nanchor_before=%s\n' \
    "$result" "$reason" "$sha" "$target_revision" "$artifact_blake3" "$artifact_sha256" \
    "$activation_id" "$active_generation" "$service_invocation_pid" "$rehearsal_bind" \
    "$rehearsal_port" "$installed_bind" "$readiness_base_url" "$wallet" "$anchor_before"
  printf 'final=%s\n' "$last"
  printf 'copy_manifest_sha256=%s\nreadiness_response_sha256=%s\nwatch_log_sha256=%s\nservice_log_sha256=%s\n' \
    "$copy_manifest_sha256" \
    "$readiness_response_sha256" \
    "$(sha256sum "$watch_log" | awk '{print $1}')" \
    "$(sha256sum "$service_log" | awk '{print $1}')"
} > "$manifest.tmp"
mv "$manifest.tmp" "$manifest"
hash=$(sha256sum "$manifest" | awk '{print $1}')
python3 -c 'import json,os,sys
path,result,digest,manifest,revision,artifact,artifact_sha=sys.argv[1:]
value={
    "kind":"rehearsal545-evidence-v1",
    "result":result,
    "evidence_sha256":digest,
    "manifest_path":os.path.realpath(manifest),
    "target_revision":revision,
    "artifact_blake3":artifact,
    "artifact_sha256":artifact_sha,
}
with open(path,"w",encoding="utf-8") as output:
    json.dump(value,output,sort_keys=True,separators=(",",":"))
    output.write("\n")' \
  "$evidence_hash_file.tmp" "$result" "$hash" "$manifest" "$target_revision" \
  "$artifact_blake3" "$artifact_sha256"
mv "$evidence_hash_file.tmp" "$evidence_hash_file"
printf 'REHEARSAL545_%s reason=%s evidence_sha256=%s evidence_file=%s\n' \
  "$result" "$reason" "$hash" "$evidence_hash_file"
[[ "$result" == PASS ]]
