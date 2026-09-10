#!/usr/bin/env bash
# Bounded final-head production rehearsal for issue #545.

set -euo pipefail

harness_deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=generation_common.sh
source "$harness_deploy_dir/generation_common.sh"

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
    "proof=exact-revision,poll-after-start,post-start-anchoring,real-readiness,critical-health,publishable-child-denied,accounts-before-after-off,no-unsafe-evidence" \
    "stop=first-complete-evidence-or-first-failure-or-timeout" \
    "evidence_contract=rehearsal545-evidence-v1" \
    "evidence_hash_file=$evidence_hash_file"
  exit 0
fi

# Issue #586: production evidence may only be emitted by the harness in the release tree whose
# bytes it is rehearsing. The test fixture is the sole intentional cross-tree caller.
if [[ "${PE_ACTIVATION_TESTING:-0}" != 1 ]]; then
  harness_tree_root=$(cd "$harness_deploy_dir/../.." && pwd -P)
  resolved_release_root=$(realpath "$release_root" 2>/dev/null || true)
  if [[ -z "$resolved_release_root" || "$resolved_release_root" != "$harness_tree_root" ]]; then
    echo "REHEARSAL545_FAIL reason=release_root_mismatch"
    exit 1
  fi
fi
# Issue #586: the bundle is captured before any copy or preflight work and re-checked at manifest time.
harness_bundle_sha256=$(harness_bundle_digest "$harness_deploy_dir")

for required in python3 sqlite3 sha256sum od cp grep awk sed flock curl env realpath mktemp systemctl; do
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
# reviewed-binary verb mutates that private copy. One structural predicate (SQLite's built-in JSON
# functions) owns the definition for both queries: the root version is the integer 2, the applied
# configuration object has no `era` key at all (a JSON null is not absence), and its `fill_mode` is
# text. Substring matching would misclassify `"version":20`, a nested `"version":2`, or JSON with
# whitespace.
read -r -d '' legacy_continuation_predicate <<'SQL' || true
state = 'terminal'
  and json_valid(frozen_inputs_json)
  and json_type(frozen_inputs_json, '$.version') = 'integer'
  and json_extract(frozen_inputs_json, '$.version') = 2
  and json_type(frozen_inputs_json, '$.applied_configuration') = 'object'
  and json_type(frozen_inputs_json, '$.applied_configuration.era') is null
  and json_type(frozen_inputs_json, '$.applied_configuration.fill_mode') = 'text'
SQL
legacy_continuations_count=$(sqlite3 -readonly "$copy_dir/paper_state.db" \
  "select count(*) from decision_pending where $legacy_continuation_predicate;")
legacy_continuations_digest=$(sqlite3 -readonly "$copy_dir/paper_state.db" \
  "select source_trade_id from decision_pending where $legacy_continuation_predicate
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

# Issue #590: pre-launch baseline for the post-start anchoring proof (rows are never deleted).
anchor_rows_before=$(sqlite3 -readonly "$copy_dir/paper_state.db" "select count(*) from position_anchors;")
[[ "$anchor_rows_before" =~ ^[0-9]+$ ]] || { echo "FATAL: invalid initial anchor row count" >&2; exit 1; }

service_log="$root/service-$short.log"
watch_log="$root/watch-$short.log"
manifest="$root/manifest-$short.txt"
status_state="$root/status-$short.state"
quiesce_flag="$root/quiesce-$short.flag"
drop_state="$root/drops-$short.state"
fence_state="$root/fences-$short.state"
write_state="$root/writes-$short.state"
readiness_response="$root/readiness-$short.json"
: > "$service_log"
: > "$watch_log"
rm -f "$status_state" "$drop_state" "$fence_state" "$write_state" \
  "$readiness_response" "$readiness_response.tmp" "$quiesce_flag"

service_pid=""
observer_pids=()
stop_all() {
  local pid
  for pid in "${observer_pids[@]}"; do kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true; done
  if [[ -n "$service_pid" ]]; then
    kill -TERM "$service_pid" 2>/dev/null || true
    for _ in $(seq 1 15); do kill -0 "$service_pid" 2>/dev/null || break; sleep 1; done
    kill -KILL "$service_pid" 2>/dev/null || true
    wait "$service_pid" 2>/dev/null || true
  fi
  for pid in "${observer_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}

epoch_ms() {
  local t=${EPOCHREALTIME/./}
  printf '%s\n' "${t:0:${#t}-3}"
}

quiesce_service() {
  local pid service_status start_ms now_ms elapsed_ms bound_ms
  # Issue #586: raise the quiesce flag, then stop each observer's whole process group so no
  # descendant (python3/sqlite3/sleep) can publish state after the signal instant.
  : > "$quiesce_flag"
  for pid in "${observer_pids[@]}"; do kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${observer_pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  observer_pids=()
  quiesce_sent_at=$(date +%s)
  shutdown_signal_unix=$quiesce_sent_at
  start_ms=$(epoch_ms)
  bound_ms=$((unit_timeout_stop_secs * 1000))
  if ! kill -INT "$service_pid" 2>/dev/null; then
    shutdown_elapsed_secs=0
    return 1
  fi
  # The whole-second signal instant serves only final-status causality; the bound is enforced on a
  # subsecond clock and cannot accept an exit observed after it.
  while kill -0 "$service_pid" 2>/dev/null; do
    now_ms=$(epoch_ms); elapsed_ms=$((now_ms - start_ms))
    if ((elapsed_ms > bound_ms)); then
      shutdown_elapsed_secs=$unit_timeout_stop_secs
      return 1
    fi
    sleep 0.1
  done
  now_ms=$(epoch_ms); elapsed_ms=$((now_ms - start_ms))
  shutdown_elapsed_secs=$((elapsed_ms / 1000))
  if wait "$service_pid"; then
    service_status=0
  else
    service_status=$?
  fi
  service_pid=""
  ((elapsed_ms <= bound_ms && service_status == 0))
}
trap stop_all EXIT TERM INT

scan_service_log_prefix() {
  python3 - "$service_log" <<'PY'
import hashlib, sys

with open(sys.argv[1], "rb") as source:
    prefix = source.read()
drops = unexpected_errors = refused = writes = 0
for line in prefix.decode("utf-8", errors="replace").splitlines():
    if "dropping socket" in line:
        drops += 1
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
    drops, unexpected_errors, refused, writes, len(prefix),
    hashlib.sha256(prefix).hexdigest(),
)
PY
}

observe_final_status() {
  python3 - "$copy_dir/status.json" "$1" <<'PY'
import datetime, json, sys

path, since = sys.argv[1], int(sys.argv[2])
try:
    with open(path, encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ValueError("status is not an object")
    updated = datetime.datetime.fromisoformat(
        value["updated_at"].replace("Z", "+00:00")
    ).timestamp()
    if updated < since:
        raise ValueError("status predates shutdown signal")
    tasks = value.get("tasks")
    if not isinstance(tasks, list) or not all(isinstance(task, dict) for task in tasks):
        raise ValueError("tasks are malformed")
    status_writers = [task for task in tasks if task.get("name") == "status_writer"]
    if len(status_writers) != 1:
        raise ValueError("status_writer marker is not unique")
    status_writer = status_writers[0]
    if (status_writer.get("class") != "critical"
            or status_writer.get("state") != "stopping"
            or status_writer.get("failure") is not None):
        raise ValueError("status_writer final marker is invalid")
    for task in tasks:
        if task is status_writer or task.get("class") != "critical":
            continue
        if task.get("state") != "stopped" or task.get("failure") is not None:
            raise ValueError("another critical owner did not stop cleanly")
    health = value.get("source_health")
    if not isinstance(health, dict):
        raise ValueError("source health is absent")
    count = health.get("reconciliation_obligations_dropped_total")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError("reconciliation obligation count is invalid")
    if health.get("ws_sink_poisoned") is not False:
        raise ValueError("websocket sink poison state is unsafe")
except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
    raise SystemExit(1)
print(count)
PY
}

# Issue #590: `position_anchors` rows are never deleted, so a count above the pre-launch baseline
# proves the reviewed binary installed an anchor in this run (boot anchor walk or periodic refresh).
observe_database_final() {
  sqlite3 "file:$copy_dir/paper_state.db?mode=ro" \
    "select (select count(*) from position_anchors),
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
printf 'REHEARSAL_START sha=%s activation=%s anchor_rows_before=%s unix=%s\n' \
  "$sha" "$activation_id" "$anchor_rows_before" "$started_at" | tee -a "$watch_log"

# Issue #586: `crates/service/src/status_writer.rs` owns
# `reconciliation_obligations_dropped_total`, fed by `crates/service/src/activity_ingest.rs`.
# Socket recycles remain recorded as `drops`; they are not inferred to have lost an obligation.
status_file_poller() {
  while kill -0 "$service_pid" 2>/dev/null; do
    if [[ -f "$copy_dir/status.json" ]]; then
      python3 - "$copy_dir/status.json" "$started_at" "$status_state" "$sha" "$quiesce_flag" <<'PY' || true
import datetime, json, os, sys
source, started, target, expected_revision, quiesce_flag = sys.argv[1:]
started = int(started)
fresh = revision_ok = polled = healthy = child_authorization_denied = False
credit_loss = 0
try:
    with open(source, encoding="utf-8") as handle:
        value = json.load(handle)
    updated = datetime.datetime.fromisoformat(value["updated_at"].replace("Z", "+00:00")).timestamp()
    fresh = updated >= started
    revision_ok = value.get("revision") == expected_revision
    health = value.get("source_health")
    if not isinstance(health, dict):
        health = {}
    polled = fresh and health.get("poll_last_round_age_secs") is not None
    credit_loss = health.get("reconciliation_obligations_dropped_total")
    # Issue #586: only a fresh observed status can report loss; anything else publishes zero.
    if not fresh or isinstance(credit_loss, bool) or not isinstance(credit_loss, int) or credit_loss < 0:
        credit_loss = 0
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
except Exception:
    fresh = revision_ok = polled = healthy = child_authorization_denied = False
    credit_loss = 0
if os.path.exists(quiesce_flag):
    raise SystemExit(0)
temp = target + ".tmp"
with open(temp, "w", encoding="utf-8") as output:
    output.write(
        f"{int(fresh)} {int(revision_ok)} {int(polled)} {int(healthy)} "
        f"{int(child_authorization_denied)} {credit_loss}\n"
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
import os, sys
drops = unexpected_errors = 0
with open(sys.argv[1], encoding="utf-8", errors="replace") as source:
    for line in source:
        if "dropping socket" in line:
            drops += 1
        if '"level":"ERROR"' in line and not any(
            marker in line for marker in ("permission denied", "HTTP 401", "HTTP 403")
        ):
            unexpected_errors += 1
temp = sys.argv[2] + ".tmp"
with open(temp, "w", encoding="utf-8") as output:
    output.write(f"{drops} {unexpected_errors}\n")
os.replace(temp, sys.argv[2])
PY
    sleep "$poll_secs"
  done
}

fence_anchor_census() {
  while kill -0 "$service_pid" 2>/dev/null; do
    anchor_rows=$(sqlite3 "file:$copy_dir/paper_state.db?mode=ro" \
      "select count(*) from position_anchors;" 2>/dev/null || echo "$anchor_rows_before")
    unexpected=$(sqlite3 "file:$copy_dir/paper_state.db?mode=ro" \
      "select count(*) from wallet_fences where cause not in ($expected_fence_causes);" 2>/dev/null || echo 1)
    anchored=0
    [[ "$anchor_rows" =~ ^[0-9]+$ && "$anchor_rows" -gt "$anchor_rows_before" ]] && anchored=1
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

# Issue #586: job control gives each observer its own process group, so quiescence can reap it whole.
set -m
status_file_poller & observer_pids+=("$!")
reader_drop_classifier & observer_pids+=("$!")
fence_anchor_census & observer_pids+=("$!")
write_refusal_counter & observer_pids+=("$!")
set +m

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
final_status_sha256=absent
unit_kill_signal=absent
unit_timeout_stop_secs=absent
shutdown_signal_unix=absent
shutdown_elapsed_secs=absent
last="status_fresh=0 endpoint_ready=0 revision_ok=0 polled=0 healthy=0 child_authorization_denied=0 anchored=0 drops=0 credit_loss=0 errors=0 fences=0 refused=0 writes=0"
while (( $(date +%s) <= deadline )); do
  if ! kill -0 "$service_pid" 2>/dev/null; then reason=process_exited; break; fi
  status_fresh=0; endpoint_ready=0; revision_ok=0; polled=0; healthy=0; child_authorization_denied=0; anchored=0
  drops=0; credit_loss=0; errors=0; fences=0; refused=0; writes=0
  readiness_candidate_sha256=absent
  [[ ! -f "$status_state" ]] || read -r status_fresh revision_ok polled healthy child_authorization_denied credit_loss < "$status_state"
  [[ ! -f "$drop_state" ]] || read -r drops errors < "$drop_state"
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
  # Issue #586 review: the counter is canonical decimal text (a u64 can exceed Bash's signed range); compare as text.
  if [[ "$credit_loss" != 0 ]] || (( errors > 0 || fences > 0 || writes > 0 )); then
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
    # Issue #586: the service owns SIGINT shutdown. Prove the production unit gives that handled
    # path its configured stop timeout; the harness owns no independent quiescence number.
    unit_policy_valid=true
    if ! unit_policy=$(service_unit_stop_policy pe-service); then unit_policy_valid=false; fi
    read -r unit_kill_signal unit_timeout_stop_secs <<< "$unit_policy"
    if [[ "$unit_policy_valid" != true || "$unit_kill_signal" != 2 ||
          ! "$unit_timeout_stop_secs" =~ ^[1-9][0-9]*$ ]]; then
      reason=unit_stop_policy_mismatch
      break
    fi
    # Stop continuous observers before SIGINT so their preserved files remain the within-run sample.
    if ! quiesce_service; then
      reason=service_shutdown_incomplete
      break
    fi
    final_log_scan=$(scan_service_log_prefix) || {
      reason=final_service_log_scan_failed
      break
    }
    read -r drops errors refused writes service_log_prefix_length service_log_prefix_sha256 \
      <<< "$final_log_scan"
    if [[ ! "$drops $errors $refused $writes $service_log_prefix_length" =~ ^[0-9]+\ [0-9]+\ [0-9]+\ [0-9]+\ [0-9]+$ ||
          ! "$service_log_prefix_sha256" =~ ^[0-9a-f]{64}$ ]]; then
      reason=final_service_log_scan_failed
      break
    fi
    if ! credit_loss=$(observe_final_status "$quiesce_sent_at"); then
      reason=final_status_observation_failed
      break
    fi
    final_database=$(observe_database_final) || {
      reason=final_database_observation_failed
      break
    }
    IFS='|' read -r anchor_rows fences <<< "$final_database"
    if [[ ! "$anchor_rows" =~ ^[0-9]+$ || ! "$fences" =~ ^[0-9]+$ ]]; then
      reason=final_database_observation_failed
      break
    fi
    anchored=0
    (( anchor_rows > anchor_rows_before )) && anchored=1
    database_observation="anchor_rows_before:$anchor_rows_before,anchor_rows_after:$anchor_rows,unexpected_fences:$fences"
    last="status_fresh=$status_fresh endpoint_ready=$endpoint_ready revision_ok=$revision_ok polled=$polled healthy=$healthy child_authorization_denied=$child_authorization_denied anchored=$anchored drops=$drops credit_loss=$credit_loss errors=$errors fences=$fences refused=$refused writes=$writes"
    printf '%s FINAL %s service_log_prefix_length=%s service_log_prefix_sha256=%s database_observation=%s\n' \
      "$(date -u +%FT%TZ)" "$last" "$service_log_prefix_length" \
      "$service_log_prefix_sha256" "$database_observation" >> "$watch_log"
    if [[ "$credit_loss" != 0 ]] || (( errors > 0 || fences > 0 || writes > 0 || anchored != 1 )); then
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
# Issue #586: on a refusal this digest identifies the latest preserved status, not a proven final
# status. Final-status validity remains the independent PASS predicate above.
if [[ -f "$copy_dir/status.json" ]]; then
  final_status_sha256=$(sha256_file "$copy_dir/status.json")
fi
manifest_stage=$(mktemp "$root/.manifest-$short.XXXXXX")
# Issue #586: the evidence names the closure that ran; refuse emission if the bundled bytes changed.
if [[ "$(harness_bundle_digest "$harness_deploy_dir")" != "$harness_bundle_sha256" ]]; then
  result=FAIL
  reason=harness_bundle_changed
fi
{
  printf 'result=%s\nreason=%s\nsha=%s\ntarget_revision=%s\nartifact_blake3=%s\nartifact_sha256=%s\nactivation_id=%s\ngeneration_dir=%s\nconfig_sha256=%s\nenvironment_sha256=%s\nrehearsal_environment_sha256=%s\nservice_invocation_pid=%s\nrehearsal_bind=%s\nrehearsal_port=%s\ninstalled_bind=%s\nreadiness_base_url=%s\n' \
    "$result" "$reason" "$sha" "$target_revision" "$artifact_blake3" "$artifact_sha256" \
    "$activation_id" "$active_generation" "$config_sha256" "$environment_sha256" \
    "$rehearsal_environment_sha256" \
    "$service_invocation_pid" "$rehearsal_bind" "$rehearsal_port" "$installed_bind" \
    "$readiness_base_url"
  printf 'final=%s\n' "$last"
  printf 'legacy_continuations=%s:%s\n' \
    "$legacy_continuations_count" "$legacy_continuations_digest"
  printf 'harness_bundle_sha256=%s\nfinal_status_sha256=%s\nunit_kill_signal=%s\nunit_timeout_stop_secs=%s\nshutdown_signal_unix=%s\nshutdown_elapsed_secs=%s\n' \
    "$harness_bundle_sha256" "$final_status_sha256" "$unit_kill_signal" \
    "$unit_timeout_stop_secs" "$shutdown_signal_unix" "$shutdown_elapsed_secs"
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
