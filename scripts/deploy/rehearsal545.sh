#!/usr/bin/env bash
# Bounded final-head production rehearsal for issue #545.

set -euo pipefail

# shellcheck source=generation_common.sh
source "$(cd "$(dirname "$0")" && pwd)/generation_common.sh"

usage() {
  echo "usage: $0 --dry-run [--target-config PATH --target-environment PATH] 40_HEX_GIT_SHA" >&2
  echo "       $0 --target-config PATH --target-environment PATH 40_HEX_GIT_SHA" >&2
  exit 2
}

dry_run=0
target_config=
target_environment=
target_config_supplied=0
target_environment_supplied=0
while (($#)); do
  case "$1" in
    --dry-run) dry_run=1; shift ;;
    --target-config)
      [[ $# -ge 2 ]] || usage
      target_config=$2
      target_config_supplied=1
      shift 2
      ;;
    --target-environment)
      [[ $# -ge 2 ]] || usage
      target_environment=$2
      target_environment_supplied=1
      shift 2
      ;;
    --) shift; break ;;
    -*) usage ;;
    *) break ;;
  esac
done
[[ $# -eq 1 ]] || usage
if ((target_config_supplied || target_environment_supplied)); then
  ((target_config_supplied && target_environment_supplied)) || usage
  [[ -n "$target_config" && -n "$target_environment" ]] || usage
elif [[ "$dry_run" != 1 ]]; then
  usage
fi
sha=$1
[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || usage
short=${sha:0:7}

root=${PE_REHEARSAL_ROOT:-"$HOME/rehearsal545"}
activation_manifest=${PE_ACTIVATION_MANIFEST:-"$HOME/pe-activation.json"}
release_root=${PE_REHEARSAL_RELEASE_ROOT:-"$HOME/releases/pe-545-$short"}
binary=${PE_REHEARSAL_BINARY:-"$release_root/target/release/pe-service"}
config=${target_config:-not-supplied}
env_file=${target_environment:-not-supplied}
copy_dir=${PE_REHEARSAL_COPY_DIR:-"$root/gen-$short"}
rehearsal_bind=${PE_REHEARSAL_BIND:-}
timeout_secs=${PE_REHEARSAL_TIMEOUT_SECS:-10800}
# Explicit #545 rehearsal allowlist; strings are WalletFenceCause::as_str values (crates/position-ledger/src/lib.rs).
# A wallet_fences row with any other cause is unexpected evidence and fails the run.
expected_fence_causes="'order_dependent_equal_second','position_underflow','conversion_unknown_conditions'"
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
    "target_config=$config" \
    "target_environment=$env_file" \
    "config_override=${PE_REHEARSAL_CONFIG:-bound-to-target-config}" \
    "environment_override=${PE_REHEARSAL_ENV:-bound-to-target-environment}" \
    "rehearsal_bind=${rehearsal_bind:-required}" \
    "credential_slots=production-secret/service-role,sanitized-rehearsal-publishable" \
    "logs=paper.log,source_events.log,live_journal.log" \
    "observers=status_file_poller,health_ready_query,reader_drop_classifier,fence_anchor_census,write_refusal_counter,privileged_account_census" \
    "proof=exact-revision,poll-after-start,reanchor,real-readiness,critical-health,publishable-child-denied,accounts-before-after-off,no-unsafe-evidence" \
    "stop=first-complete-evidence-or-first-failure-or-timeout" \
    "evidence_contract=rehearsal545-evidence-v1" \
    "evidence_hash_file=$evidence_hash_file"
  exit 0
fi

for required in python3 sqlite3 sha256sum od cp grep awk sed flock curl env realpath mktemp; do
  command -v "$required" >/dev/null 2>&1 || {
    echo "FATAL: required command is unavailable: $required" >&2
    exit 1
  }
done
[[ -x "$binary" ]] || { echo "FATAL: staged binary is not executable: $binary" >&2; exit 1; }
[[ -f "$config" ]] || { echo "FATAL: rehearsal config is missing: $config" >&2; exit 1; }
[[ -f "$env_file" ]] || { echo "FATAL: rehearsal environment is missing: $env_file" >&2; exit 1; }
export SUPABASE_DB_URL
python3 -c 'import os,sys
raise SystemExit(0 if os.environ.get(sys.argv[1]) else 1)' SUPABASE_DB_URL ||
  die "SUPABASE_DB_URL must be exported for rehearsal preflight"
input_binary=$(realpath "$binary")
input_config=$(realpath "$config")
env_file=$(realpath "$env_file")
environment_sha256=$(sha256sum "$env_file" | awk '{print $1}')
env_file_values "$env_file" >/dev/null
if [[ -v PE_REHEARSAL_CONFIG && "$(realpath "$PE_REHEARSAL_CONFIG")" != "$input_config" ]]; then
  echo "FATAL: PE_REHEARSAL_CONFIG differs from the reviewed target config" >&2
  exit 1
fi
if [[ -v PE_REHEARSAL_ENV && "$(realpath "$PE_REHEARSAL_ENV")" != "$env_file" ]]; then
  echo "FATAL: PE_REHEARSAL_ENV differs from the reviewed target environment" >&2
  exit 1
fi
[[ -n "$rehearsal_bind" ]] || {
  echo "FATAL: PE_REHEARSAL_BIND is required" >&2
  exit 1
}
[[ "$timeout_secs" =~ ^[1-9][0-9]*$ && "$poll_secs" =~ ^[1-9][0-9]*$ ]] || {
  echo "FATAL: rehearsal timeout and poll cadence must be positive integers" >&2
  exit 1
}

mkdir -p "$root"
exec 7<>"$root/.rehearsal545.lock"
flock -n 7 || { echo "FATAL: another #545 rehearsal is running" >&2; exit 1; }
artifact_dir="$root/artifacts-$short"
mkdir -p "$artifact_dir"
chmod 0700 "$artifact_dir"
input_artifact_sha256=$(sha256_file "$input_binary")
input_config_sha256=$(sha256_file "$input_config")
atomic_adopt "$input_binary" "$artifact_dir/pe-service" 0500 rehearsal-binary-copied \
  "$input_artifact_sha256"
atomic_adopt "$input_config" "$artifact_dir/service.toml" 0400 rehearsal-config-copied \
  "$input_config_sha256"
binary="$artifact_dir/pe-service"
config="$artifact_dir/service.toml"
artifact_sha256=$(sha256_file "$binary")
config_sha256=$(sha256_file "$config")

staged_identity_output=$("$binary" --verify-staged-identity) || {
  echo "FATAL: rehearsal binary could not derive its own identity" >&2
  exit 1
}
read -r target_revision artifact_blake3 < <(parse_staged_identity <<< "$staged_identity_output") || {
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
readarray -t activation < <(python3 - "$activation_manifest" <<'PY'
import hashlib, json, os, re, sys
with open(sys.argv[1], encoding="utf-8") as source:
    value = json.load(source)
required = {
    "activation_id", "state", "generation_dir", "merge_commit", "bankroll",
    "source_v1_main", "legacy_history", "artifacts", "old_installed_artifacts",
    "destinations", "old_paths",
}
if not required <= set(value):
    raise SystemExit("#557 activation manifest lacks its production schema")
if value.get("state") != "verified":
    raise SystemExit("activation manifest state is not verified")
generation = value.get("generation_dir")
activation_id = value.get("activation_id")
destinations = value.get("destinations")
artifacts = value.get("artifacts")
if (not isinstance(generation, str) or not os.path.isabs(generation)
        or not os.path.isdir(generation) or os.path.realpath(generation) != generation):
    raise SystemExit("activation manifest has no absolute generation_dir")
if (not isinstance(activation_id, str)
        or re.fullmatch(r"[A-Za-z0-9._-]+", activation_id) is None):
    raise SystemExit("activation manifest has no activation_id")
if not isinstance(destinations, dict) or set(destinations) != {"binary", "config", "environment"}:
    raise SystemExit("activation manifest has no installed artifact authority")
if (not isinstance(artifacts, dict)
        or set(artifacts) != {"seed_main", "binary", "config", "environment",
                             "rehearsal_config", "rehearsal_environment"}):
    raise SystemExit("activation manifest has no reviewed artifact authority")

def verify_installed_artifact(name):
    row = artifacts.get(name)
    if not isinstance(row, dict) or set(row) != {"path", "sha256"}:
        raise SystemExit(f"activation manifest has invalid inherited {name} artifact")
    path, digest = row["path"], row["sha256"]
    if not isinstance(path, str) or not os.path.isabs(path):
        raise SystemExit(f"activation manifest has invalid inherited {name} path")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise SystemExit(f"activation manifest has invalid inherited {name} digest")
    installed = destinations.get(name)
    if (not isinstance(installed, str) or not os.path.isabs(installed)
            or not os.path.isfile(installed) or os.path.islink(installed)):
        raise SystemExit(f"activation manifest has invalid installed {name} destination")
    with open(installed, "rb") as source:
        actual = hashlib.sha256(source.read()).hexdigest()
    if actual != digest:
        raise SystemExit(f"installed old {name} differs from #557 artifact authority")
    return os.path.realpath(installed)

verify_installed_artifact("binary")
installed_config = verify_installed_artifact("config")
installed_environment = verify_installed_artifact("environment")
print(os.path.realpath(generation))
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
mapfile -d '' -t installed_bind_parts < <(
  env_file_values "$installed_environment" PE_BIND && printf '__PE_ENV_FILE_PARSED__\0'
)
installed_bind=
last_bind_index=$((${#installed_bind_parts[@]} - 1))
if ((last_bind_index < 0)) ||
   [[ ${installed_bind_parts[$last_bind_index]} != __PE_ENV_FILE_PARSED__ ]]; then
  die "could not parse the installed service environment"
fi
unset 'installed_bind_parts[last_bind_index]'
for assignment in "${installed_bind_parts[@]}"; do
  [[ $assignment == PE_BIND=* ]] || die "installed environment returned an unexpected name"
  installed_bind=${assignment#*=}
done
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

mkdir -p "$copy_dir"
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
  source_identity_stage=$(mktemp "$root/.source.identity.XXXXXX")
  printf 'activation_id=%s\ngeneration_dir=%s\n' "$activation_id" "$active_generation" \
    > "$source_identity_stage"
  atomic_adopt "$source_identity_stage" "$source_identity" 0600 rehearsal-source-identity
  rm -f "$source_identity_stage"
  copy_manifest_stage=$(mktemp "$root/.copied.sha256.XXXXXX")
  (
    cd "$copy_dir"
    sha256sum paper_state.db paper.log source_events.log live_journal.log \
      wallet_market_history.json source.identity > "$copy_manifest_stage"
  )
  atomic_adopt "$copy_manifest_stage" "$copy_manifest" 0600 rehearsal-copy-manifest
  rm -f "$copy_manifest_stage"
  validate_copy_manifest || {
    echo "FATAL: new rehearsal copy failed recorded hash validation" >&2
    exit 1
  }
fi
copy_manifest_sha256=$(sha256sum "$copy_manifest" | awk '{print $1}')

# Issue #584: bind the checkpointed copy's terminal pre-#545 continuation inventory before any
# reviewed-binary verb mutates that private copy.
legacy_continuations_count=$(sqlite3 -readonly "$copy_dir/paper_state.db" \
  "select count(*) from decision_pending
    where state = 'terminal'
      and instr(frozen_inputs_json, '\"version\":2') > 0
      and instr(frozen_inputs_json, '\"era\"') = 0
      and instr(frozen_inputs_json, '\"fill_mode\"') > 0;")
legacy_continuations_digest=$(sqlite3 -readonly "$copy_dir/paper_state.db" \
  "select source_trade_id from decision_pending
    where state = 'terminal'
      and instr(frozen_inputs_json, '\"version\":2') > 0
      and instr(frozen_inputs_json, '\"era\"') = 0
      and instr(frozen_inputs_json, '\"fill_mode\"') > 0
    order by source_trade_id;" | sha256sum | awk '{print $1}')
printf 'rehearsal copy legacy continuations: count=%s digest=%s\n' \
  "$legacy_continuations_count" "$legacy_continuations_digest"

rehearsal_env_file="$root/environment-$short.rehearsal.env"
rehearsal_env_stage=$(mktemp "$root/.environment-$short.rehearsal.XXXXXX")
python3 - "$env_file" "$rehearsal_env_stage" "$environment_sha256" 3< <(
  env_file_values "$env_file" PE_SUPABASE_ANON_KEY
) <<'PY'
import base64, hashlib, json, os, re, shlex, sys

source_path, destination_path, expected_digest = sys.argv[1:]
parsed = b"".join(iter(lambda: os.read(3, 65536), b""))
parts = [part for part in parsed.split(b"\0") if part]
if len(parts) != 1 or not parts[0].startswith(b"PE_SUPABASE_ANON_KEY="):
    raise SystemExit("reviewed production environment has no Supabase publishable key")
publishable = parts[0].split(b"=", 1)[1].decode("utf-8")
if publishable.startswith("sb_publishable_") and len(publishable) > len("sb_publishable_"):
    pass
else:
    parts = publishable.split(".")
    if len(parts) != 3 or not all(re.fullmatch(r"[A-Za-z0-9_-]+", part) for part in parts):
        raise SystemExit("production anon slot is neither sb_publishable_* nor a legacy anon JWT")
    try:
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as error:
        raise SystemExit("production anon slot has a malformed legacy JWT") from error
    if not isinstance(payload, dict) or payload.get("role") != "anon":
        raise SystemExit("production anon slot is not publishable/anon class")

assignment = re.compile(
    r"^([ \t]*(?:export[ \t]+)?(PE_SUPABASE_(?:ANON|SECRET)_KEY)[ \t]*=).*(\r?\n)?$"
)
counts = {"PE_SUPABASE_ANON_KEY": 0, "PE_SUPABASE_SECRET_KEY": 0}
with open(source_path, "rb") as source:
    raw = source.read()
if hashlib.sha256(raw).hexdigest() != expected_digest:
    raise SystemExit("reviewed production environment changed before sanitization")
lines = raw.decode("utf-8").splitlines(keepends=True)
output = []
for line in lines:
    match = assignment.fullmatch(line)
    if match is None:
        output.append(line)
        continue
    name = match.group(2)
    counts[name] += 1
    newline = match.group(3) or ""
    output.append(match.group(1) + shlex.quote(publishable) + newline)
if counts != {"PE_SUPABASE_ANON_KEY": 1, "PE_SUPABASE_SECRET_KEY": 1}:
    raise SystemExit("production environment must assign each Supabase credential slot exactly once")
with open(destination_path, "w", encoding="utf-8", newline="") as destination:
    destination.writelines(output)
PY
env_file_values "$rehearsal_env_stage" >/dev/null
rehearsal_environment_sha256=$(sha256_file "$rehearsal_env_stage")
atomic_adopt "$rehearsal_env_stage" "$rehearsal_env_file" 0600 sanitized-rehearsal-environment \
  "$rehearsal_environment_sha256"
rm -f "$rehearsal_env_stage"

preflight_output=$(env -i PATH="$PATH" HOME="$HOME" LANG="${LANG:-C.UTF-8}" /bin/bash -c '
eval "$(cat <&3)"
exec 3<&-
exec /bin/bash "$@"
' bash "$release_root/scripts/deploy/rehearsal_preflight.sh" "$rehearsal_env_file" \
  3< <(python3 -c 'import os,shlex,sys
name=sys.argv[1]
print(f"export {name}={shlex.quote(os.environ[name])}")' SUPABASE_DB_URL))
printf '%s\n' "$preflight_output"
readarray -t account_census < <(python3 -c 'import re,sys
matches=re.findall(r"^REHEARSAL_ACCOUNT_CENSUS_V1 count=([0-9]+) sha256=([0-9a-f]{64})$",sys.stdin.read(),re.MULTILINE)
if len(matches) != 1: raise SystemExit("privileged preflight did not emit exactly one account census receipt")
print(matches[0][0]); print(matches[0][1])' <<< "$preflight_output")
[[ ${#account_census[@]} -eq 2 ]] || die "privileged preflight account census receipt is absent"
account_census_before_count=${account_census[0]}
account_census_before_sha256=${account_census[1]}
[[ "$(sha256sum "$rehearsal_env_file" | awk '{print $1}')" == "$rehearsal_environment_sha256" ]] || {
  echo "FATAL: sanitized rehearsal environment changed during privileged preflight" >&2
  exit 1
}
[[ "$(sha256sum "$config" | awk '{print $1}')" == "$config_sha256" ]] || {
  echo "FATAL: rehearsal config changed during privileged preflight" >&2
  exit 1
}

python3 -c 'import os,sys
raw=b"".join(iter(lambda: os.read(3,65536),b""))
parts=raw.split(b"\0")
if len(parts) < 2 or parts[-2:] != [b"__PE_ENV_FILE_PARSED__",b""]:
    raise SystemExit("could not parse the allowlisted rehearsal environment")
values={}
for part in parts[:-2]:
    name,separator,value=part.partition(b"=")
    if not separator or name in values: raise SystemExit("invalid parsed rehearsal environment")
    values[name]=value
anon=values.get(b"PE_SUPABASE_ANON_KEY")
secret=values.get(b"PE_SUPABASE_SECRET_KEY")
if not anon or not secret: raise SystemExit("both rehearsal Supabase credential slots are required")
if anon != secret: raise SystemExit("the publishable key must occupy both Supabase credential slots")' \
  3< <(env_file_values "$rehearsal_env_file" PE_SUPABASE_ANON_KEY PE_SUPABASE_SECRET_KEY &&
    printf '__PE_ENV_FILE_PARSED__\0')
[[ "$(sha256sum "$rehearsal_env_file" | awk '{print $1}')" == "$rehearsal_environment_sha256" ]] || {
  echo "FATAL: sanitized rehearsal environment changed while constructing the service environment" >&2
  exit 1
}
[[ "$(sha256sum "$config" | awk '{print $1}')" == "$config_sha256" ]] || {
  echo "FATAL: rehearsal config changed while constructing the service environment" >&2
  exit 1
}
[[ "$(sha256sum "$binary" | awk '{print $1}')" == "$artifact_sha256" ]] || {
  echo "FATAL: rehearsal binary changed while constructing the service environment" >&2
  exit 1
}

# Force the copied process onto the real first-party venue endpoints even when
# the staged static config was previously used with a local fixture.
service_child_env=(
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

# The copy's installed migration record still binds the production log paths. Rebind it to the
# copy with the reviewed binary before anything else touches the private copy (#570): the verb
# verifies the recorded activation prefixes against the copied logs and rewrites only the copy's
# record. The production generation is never named here.
update_output=$(env -i "${service_child_env[@]}" "$binary" "$config" --update-paper-migration-paths) || {
  echo "FATAL: rehearsal copy migration paths were not updated" >&2
  exit 1
}
echo "rehearsal copy migration paths: $update_output"

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

scan_service_log_prefix() {
  python3 - "$service_log" <<'PY'
import hashlib, json, sys

with open(sys.argv[1], "rb") as source:
    prefix = source.read()
drops = credit_loss = unexpected_errors = refused = writes = 0
for line in prefix.decode("utf-8", errors="replace").splitlines():
    if "dropping socket" in line:
        drops += 1
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            credit_loss += 1
            continue
        fields = value.get("fields", value)
        raw = fields.get("last_wire_frame_age_secs")
        buffered = fields.get("buffered_frame_processed")
        age = raw if isinstance(raw, int) else None
        if isinstance(raw, str) and raw.startswith("Some(") and raw.endswith(")"):
            try:
                age = int(raw[5:-1])
            except ValueError:
                age = None
        if not (age is not None and age >= 29 and buffered is False):
            credit_loss += 1
    if '"level":"ERROR"' in line and not any(
        marker in line for marker in ("permission denied", "HTTP 401", "HTTP 403")
    ):
        unexpected_errors += 1
    if any(marker in line for marker in (
        "HTTP 401", "HTTP 403", "permission denied", "projection failed",
        "apply_resolution failed", "commit_fill",
    )) and ("commit_fill" not in line or "failed" in line):
        refused += 1
    if any(marker in line for marker in (
        '"message":"fill committed"',
        '"message":"resolution applied"',
        '"message":"watchlist projection applied"',
        '"message":"paper_fill"',
    )):
        writes += 1
print(
    drops, credit_loss, unexpected_errors, refused, writes, len(prefix),
    hashlib.sha256(prefix).hexdigest(),
)
PY
}

observe_database_final() {
  sqlite3 "file:$copy_dir/paper_state.db?mode=ro" \
    "select coalesce((select max(anchor_seq) from position_anchors where wallet_hex='$wallet'),-1),
            coalesce((select reanchor_required from poll_cursors where wallet_hex='$wallet'),1),
            (select count(*) from wallet_fences where cause not in ($expected_fence_causes));"
}

env -i "${service_child_env[@]}" python3 -c '
import os,sys
overrides=dict(os.environ)
environment={}
raw=b"".join(iter(lambda: os.read(3,65536),b""))
parts=raw.split(b"\0")
if len(parts) < 2 or parts[-2:] != [b"__PE_ENV_FILE_PARSED__",b""]:
    raise SystemExit("could not parse the allowlisted rehearsal environment")
for part in parts[:-2]:
    name,separator,value=part.partition(b"=")
    if not separator: raise SystemExit("invalid parsed rehearsal environment")
    environment[name.decode("ascii")]=value.decode("utf-8")
environment.update(overrides)
os.execve(sys.argv[1],sys.argv[1:],environment)' "$binary" "$config" \
  3< <(env_file_values "$rehearsal_env_file" "${SERVICE_ENV_ALLOWLIST[@]}" &&
    printf '__PE_ENV_FILE_PARSED__\0') > "$service_log" 2>&1 &
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
source, started, target, expected_revision = sys.argv[1:]
started = int(started)
with open(source, encoding="utf-8") as handle:
    value = json.load(handle)
updated = datetime.datetime.fromisoformat(value["updated_at"].replace("Z", "+00:00")).timestamp()
fresh = updated >= started
revision_ok = value.get("revision") == expected_revision
health = value.get("source_health") or {}
polled = fresh and health.get("poll_last_round_age_secs") is not None
critical = [task for task in (value.get("tasks") or []) if task.get("class") == "critical"]
healthy = fresh and bool(critical) and all(task.get("state") == "running" for task in critical)
live = value.get("live")
accounts = live.get("accounts") if isinstance(live, dict) else None
child_authorization_denied = (
    isinstance(live, dict)
    and live.get("stale") is True
    and isinstance(accounts, list)
    and not accounts
)
temp = target + ".tmp"
with open(temp, "w", encoding="utf-8") as output:
    output.write(
        f"{int(fresh)} {int(revision_ok)} {int(polled)} {int(healthy)} "
        f"{int(child_authorization_denied)}\n"
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
      "select count(*) from wallet_fences where cause not in ($expected_fence_causes);" 2>/dev/null || echo 1)
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
completion_candidate=0
deadline=$((started_at + timeout_secs))
readiness_sha256=absent
service_log_prefix_length=absent
service_log_prefix_sha256=absent
database_observation=absent
account_census_after_count=absent
account_census_after_sha256=absent
account_census_after_safe=false
account_census_before_after_identical=false
last="status_fresh=0 endpoint_ready=0 revision_ok=0 polled=0 healthy=0 child_authorization_denied=0 anchored=0 drops=0 credit_loss=0 errors=0 fences=0 refused=0 writes=0"
while (( $(date +%s) <= deadline )); do
  if ! kill -0 "$service_pid" 2>/dev/null; then reason=process_exited; break; fi
  status_fresh=0; endpoint_ready=0; revision_ok=0; polled=0; healthy=0; child_authorization_denied=0; anchored=0
  drops=0; credit_loss=0; errors=0; fences=0; refused=0; writes=0
  readiness_candidate_sha256=absent
  [[ ! -f "$status_state" ]] || read -r status_fresh revision_ok polled healthy child_authorization_denied < "$status_state"
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
      readiness_candidate_sha256=$(sha256sum "$readiness_response.tmp" | awk '{print $1}')
    fi
  fi
  [[ ! -f "$readiness_response.tmp" ]] || mv "$readiness_response.tmp" "$readiness_response"
  last="status_fresh=$status_fresh endpoint_ready=$endpoint_ready revision_ok=$revision_ok polled=$polled healthy=$healthy child_authorization_denied=$child_authorization_denied anchored=$anchored drops=$drops credit_loss=$credit_loss errors=$errors fences=$fences refused=$refused writes=$writes"
  printf '%s %s\n' "$(date -u +%FT%TZ)" "$last" >> "$watch_log"
  if (( credit_loss > 0 || errors > 0 || fences > 0 || writes > 0 )); then
    reason=unsafe_evidence
    break
  fi
  if (( status_fresh == 1 && child_authorization_denied == 0 )); then
    reason=unsafe_child_account_evidence
    break
  fi
  if (( status_fresh == 1 && endpoint_ready == 1 && revision_ok == 1 && polled == 1 && healthy == 1 && child_authorization_denied == 1 && anchored == 1 )); then
    private_binary_path=$(realpath "$binary")
    if service_executable_path=$(realpath "$PROC_ROOT/$service_pid/exe" 2>/dev/null); then
      executable_matches=false
      [[ "$service_executable_path" != "$private_binary_path" ]] || executable_matches=true
      printf '%s PROCESS_EXE expected=%s resolved=%s matches=%s\n' \
        "$(date -u +%FT%TZ)" "$private_binary_path" "$service_executable_path" \
        "$executable_matches" >> "$watch_log"
      if [[ "$executable_matches" != true ]]; then
        reason=process_executable_mismatch
        break
      fi
    else
      printf '%s PROCESS_EXE expected=%s resolved=unreadable matches=false\n' \
        "$(date -u +%FT%TZ)" "$private_binary_path" >> "$watch_log"
      reason=process_executable_unreadable
      break
    fi
    # Quiesce the exact invocation before the final observation. Its shutdown path is part of the
    # scanned evidence, and no later service append or database write can race the PASS decision.
    stop_all
    service_pid=""
    observer_pids=()
    final_log_scan=$(scan_service_log_prefix) || {
      reason=final_service_log_scan_failed
      break
    }
    read -r drops credit_loss errors refused writes service_log_prefix_length service_log_prefix_sha256 \
      <<< "$final_log_scan"
    if [[ ! "$drops $credit_loss $errors $refused $writes $service_log_prefix_length" =~ ^[0-9]+\ [0-9]+\ [0-9]+\ [0-9]+\ [0-9]+\ [0-9]+$ ||
          ! "$service_log_prefix_sha256" =~ ^[0-9a-f]{64}$ ]]; then
      reason=final_service_log_scan_failed
      break
    fi
    final_database=$(observe_database_final) || {
      reason=final_database_observation_failed
      break
    }
    IFS='|' read -r anchor_after reanchor fences <<< "$final_database"
    if [[ ! "$anchor_after" =~ ^-?[0-9]+$ || ! "$reanchor" =~ ^[01]$ || ! "$fences" =~ ^[0-9]+$ ]]; then
      reason=final_database_observation_failed
      break
    fi
    anchored=0
    [[ "$anchor_after" -gt "$anchor_before" && "$reanchor" == 0 ]] && anchored=1
    database_observation="anchor_after:$anchor_after,reanchor_required:$reanchor,unexpected_fences:$fences"
    last="status_fresh=$status_fresh endpoint_ready=$endpoint_ready revision_ok=$revision_ok polled=$polled healthy=$healthy child_authorization_denied=$child_authorization_denied anchored=$anchored drops=$drops credit_loss=$credit_loss errors=$errors fences=$fences refused=$refused writes=$writes"
    printf '%s FINAL %s service_log_prefix_length=%s service_log_prefix_sha256=%s database_observation=%s\n' \
      "$(date -u +%FT%TZ)" "$last" "$service_log_prefix_length" \
      "$service_log_prefix_sha256" "$database_observation" >> "$watch_log"
    if (( credit_loss > 0 || errors > 0 || fences > 0 || writes > 0 || anchored != 1 )); then
      reason=unsafe_evidence
      break
    fi
    completion_candidate=1
    readiness_sha256=$readiness_candidate_sha256
    break
  fi
  sleep "$poll_secs"
done

stop_all
service_pid=""
observer_pids=()
trap - EXIT TERM INT

# The child is now quiescent, so this descriptor-fed privileged observation cannot race a later
# child write. It, not the publishable-only child's intentionally stale/empty snapshot, proves that
# every account remained off throughout the bounded run.
if account_census_after=$(account_census_observation); then
  read -r account_census_after_count account_census_after_sha256 account_census_after_safe_flag \
    <<< "$account_census_after"
  if [[ "$account_census_after_count" =~ ^[0-9]+$ &&
        "$account_census_after_sha256" =~ ^[0-9a-f]{64}$ &&
        "$account_census_after_safe_flag" =~ ^[01]$ ]]; then
    [[ "$account_census_after_safe_flag" == 1 ]] && account_census_after_safe=true
    if [[ "$account_census_after_count" == "$account_census_before_count" &&
          "$account_census_after_sha256" == "$account_census_before_sha256" ]]; then
      account_census_before_after_identical=true
    fi
  elif ((completion_candidate == 1)); then
    reason=final_account_census_failed
  fi
elif ((completion_candidate == 1)); then
  reason=final_account_census_failed
fi
if ((completion_candidate == 1)); then
  if [[ "$account_census_after_safe" == true &&
        "$account_census_before_after_identical" == true ]]; then
    result=PASS
    reason=evidence_complete
  else
    reason=unsafe_account_census
  fi
fi
manifest_stage=$(mktemp "$root/.manifest-$short.XXXXXX")
{
  printf 'result=%s\nreason=%s\nsha=%s\ntarget_revision=%s\nartifact_blake3=%s\nartifact_sha256=%s\nactivation_id=%s\ngeneration_dir=%s\nconfig_sha256=%s\nenvironment_sha256=%s\nrehearsal_environment_sha256=%s\nservice_invocation_pid=%s\nrehearsal_bind=%s\nrehearsal_port=%s\ninstalled_bind=%s\nreadiness_base_url=%s\nwallet=%s\nanchor_before=%s\n' \
    "$result" "$reason" "$sha" "$target_revision" "$artifact_blake3" "$artifact_sha256" \
    "$activation_id" "$active_generation" "$config_sha256" "$environment_sha256" \
    "$rehearsal_environment_sha256" \
    "$service_invocation_pid" "$rehearsal_bind" "$rehearsal_port" "$installed_bind" \
    "$readiness_base_url" "$wallet" "$anchor_before"
  printf 'final=%s\n' "$last"
  printf 'legacy_continuations=%s:%s\n' \
    "$legacy_continuations_count" "$legacy_continuations_digest"
  printf 'copy_manifest_sha256=%s\naccount_census_before_count=%s\naccount_census_before_sha256=%s\naccount_census_before_safe=true\naccount_census_after_count=%s\naccount_census_after_sha256=%s\naccount_census_after_safe=%s\naccount_census_before_after_identical=%s\nreadiness_sha256=%s\nservice_log_prefix_length=%s\nservice_log_prefix_sha256=%s\ndatabase_observation=%s\nwatch_log_sha256=%s\nservice_log_sha256=%s\n' \
    "$copy_manifest_sha256" \
    "$account_census_before_count" \
    "$account_census_before_sha256" \
    "$account_census_after_count" \
    "$account_census_after_sha256" \
    "$account_census_after_safe" \
    "$account_census_before_after_identical" \
    "$readiness_sha256" \
    "$service_log_prefix_length" \
    "$service_log_prefix_sha256" \
    "$database_observation" \
    "$(sha256sum "$watch_log" | awk '{print $1}')" \
    "$(sha256sum "$service_log" | awk '{print $1}')"
} > "$manifest_stage"
atomic_adopt "$manifest_stage" "$manifest" 0600 rehearsal-result-manifest
rm -f "$manifest_stage"
hash=$(sha256sum "$manifest" | awk '{print $1}')
mkdir -p "$(dirname "$evidence_hash_file")"
evidence_stage=$(mktemp "$(dirname "$evidence_hash_file")/.rehearsal-evidence.XXXXXX")
python3 -c 'import json,os,sys
(path,result,digest,manifest,revision,artifact,artifact_sha,activation,generation,copy_digest,
 readiness_digest,config_digest,environment_digest,rehearsal_environment_digest)=sys.argv[1:]
value={
    "kind":"rehearsal545-evidence-v1",
    "result":result,
    "evidence_sha256":digest,
    "manifest_path":os.path.realpath(manifest),
    "target_revision":revision,
    "artifact_blake3":artifact,
    "artifact_sha256":artifact_sha,
    "activation_id":activation,
    "generation_dir":os.path.realpath(generation),
    "copy_manifest_sha256":copy_digest,
    "readiness_sha256":readiness_digest,
    "config_sha256":config_digest,
    "environment_sha256":environment_digest,
    "rehearsal_environment_sha256":rehearsal_environment_digest,
}
with open(path,"w",encoding="utf-8") as output:
    json.dump(value,output,sort_keys=True,separators=(",",":"))
    output.write("\n")' \
  "$evidence_stage" "$result" "$hash" "$manifest" "$target_revision" \
  "$artifact_blake3" "$artifact_sha256" "$activation_id" "$active_generation" \
  "$copy_manifest_sha256" "$readiness_sha256" "$config_sha256" "$environment_sha256" \
  "$rehearsal_environment_sha256"
atomic_adopt "$evidence_stage" "$evidence_hash_file" 0600 rehearsal-evidence-json
rm -f "$evidence_stage"
printf 'REHEARSAL545_%s reason=%s evidence_sha256=%s evidence_file=%s\n' \
  "$result" "$reason" "$hash" "$evidence_hash_file"
[[ "$result" == PASS ]]
