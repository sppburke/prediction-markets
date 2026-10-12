#!/usr/bin/env bash
# Rehearse the real successor driver against a supplied quiesced capture; never capture production.
set -euo pipefail

harness_deploy_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
source "$harness_deploy_dir/generation_common.sh"
usage() {
  echo "usage: $0 [--dry-run] --capture PATH --run-root PATH --target-binary PATH --target-config PATH --target-environment PATH --old-binary PATH --old-config PATH --old-environment PATH --old-unit-policy PATH [--pg-port PORT --http-port PORT --service-port PORT --postgrest-port PORT --fresh-bankroll DECIMAL]" >&2
  exit 2
}
dry_run=false
capture= run_root= target_binary= target_config= target_environment=
old_binary= old_config= old_environment= old_unit_policy=
pg_port=56432 http_port=56321 service_port=58080 postgrest_port=56322 fresh_bankroll=
while (($#)); do
  case "$1" in
    --dry-run) dry_run=true; shift ;;
    --capture|--run-root|--target-binary|--target-config|--target-environment|--old-binary|--old-config|--old-environment|--old-unit-policy|--pg-port|--http-port|--service-port|--postgrest-port|--fresh-bankroll)
      [[ $# -ge 2 && -n "$2" ]] || usage
      name=${1#--}; name=${name//-/_}; printf -v "$name" '%s' "$2"; shift 2 ;;
    *) usage ;;
  esac
done
for name in capture run_root target_binary target_config target_environment old_binary old_config old_environment old_unit_policy; do
  [[ -n "${!name}" ]] || usage
done
for port in "$pg_port" "$http_port" "$service_port" "$postgrest_port"; do
  [[ "$port" =~ ^[0-9]+$ ]] && ((port > 0 && port <= 65535)) || usage
done
[[ "$pg_port" != "$http_port" && "$pg_port" != "$service_port" && "$http_port" != "$service_port" &&
   "$postgrest_port" != "$pg_port" && "$postgrest_port" != "$http_port" && "$postgrest_port" != "$service_port" ]] || usage
[[ -z "$fresh_bankroll" || "$fresh_bankroll" =~ ^[0-9]+([.][0-9]{1,6})?$ ]] || usage
if [[ "$dry_run" == true ]]; then
  printf '%s\n' 'SUCCESSOR_REHEARSAL_DRY_RUN=1' "capture=$capture" "run_root=$run_root" \
    "target_binary=$target_binary" "target_config=$target_config" "target_environment=$target_environment" \
    "old_binary=$old_binary" "old_config=$old_config" "old_environment=$old_environment" "old_unit_policy=$old_unit_policy" \
    'capture_contract=capture.json,copied.sha256,state/,authority/*.csv,pe-financial-era.json,pe-activation.json' \
    'authority=private-loopback-PostgreSQL-and-PostgREST,local-service-role-JWT' \
    'branches=forward,writer-lock-rollback,fresh-attempt' 'ownership_proof=skipped: testing mode' \
    'evidence_contract=successor-rehearsal-evidence-v1'
  exit 0
fi
for command in python3 psql docker systemctl curl flock sha256sum; do
  command -v "$command" >/dev/null || die "$command is required for successor rehearsal"
done
capture=$(realpath "$capture"); run_root=$(realpath -m "$run_root")
[[ "$run_root" =~ ^/[A-Za-z0-9/._-]+$ ]] || die "run root cannot contain unit/config metacharacters"
[[ ! -e "$run_root" && "$run_root" != /home/sean && "$run_root" != / && "$run_root" != "$capture" && "$run_root" != "$capture/"* ]] || die "run root must be a fresh isolated directory"
for name in target_binary target_config target_environment old_binary old_config old_environment old_unit_policy; do
  [[ -f "${!name}" && ! -L "${!name}" ]] || die "missing or linked $name"
  printf -v "$name" '%s' "$(realpath "${!name}")"
done
bundle=$(successor_harness_bundle_digest "$harness_deploy_dir")
# The inventory is exhaustive, including retention companions and both manifest authorities.
python3 - "$capture" "$old_binary" "$old_config" "$old_environment" "$old_unit_policy" <<'PY'
import hashlib,json,os,pathlib,re,sys
root=pathlib.Path(sys.argv[1]); inventory={}
for line in (root/"copied.sha256").read_text().splitlines():
    match=re.fullmatch(r"([0-9a-f]{64})  (.+)",line)
    if not match: raise SystemExit("malformed capture inventory")
    digest,name=match.groups(); path=root/name
    if name in inventory or path.is_symlink() or not path.is_file() or path.resolve().is_relative_to(root) is False:
        raise SystemExit("invalid capture inventory path")
    if any(part.is_symlink() for part in path.parents if part!=root): raise SystemExit("linked capture directory")
    h=hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda:source.read(1<<20),b""): h.update(chunk)
    if h.hexdigest()!=digest: raise SystemExit("capture hash mismatch: "+name)
    inventory[name]=digest
paths=list(root.rglob("*"))
if any(path.is_symlink() for path in paths): raise SystemExit("capture contains a linked file or directory")
actual={str(path.relative_to(root)) for path in paths if path.is_file() and path.name!="copied.sha256"}
if actual!=inventory.keys(): raise SystemExit("capture inventory is not exhaustive")
required={"capture.json","pe-financial-era.json","pe-activation.json","state/paper_state.db","state/paper.log","state/source_events.log","state/live_journal.log"}
if not required<=inventory.keys(): raise SystemExit("capture lacks a required authority")
if (root/"state/paper_state.db-wal").exists() or (root/"state/paper_state.db-shm").exists():
    raise SystemExit("capture must be quiesced and checkpointed without SQLite sidecars")
metadata=json.loads((root/"capture.json").read_text())
if metadata.get("kind")!="successor-capture-v1" or metadata.get("quiesced") is not True:
    raise SystemExit("capture lacks quiescence receipt")
for key,path in zip(("binary","config","environment_template","unit_policy"),sys.argv[2:]):
    if hashlib.sha256(open(path,"rb").read()).hexdigest()!=metadata["old_tuple"][key+"_sha256"]:
        raise SystemExit("installed-old tuple differs from capture: "+key)
if not metadata.get("environment_transformations") or not re.fullmatch("[0-9a-f]{64}",metadata["old_tuple"]["environment_sha256"]):
    raise SystemExit("capture lacks original environment identity/normalization provenance")
PY
# Cross-check the two stores against the predecessor Start before launching either tuple. The Rust owners
# validate the paper chain and membership: the old binary's boot and the successor preflight.
python3 - "$capture" <<'PY'
import csv,decimal,json,pathlib,sqlite3,sys
root=pathlib.Path(sys.argv[1]); receipt=json.loads((root/"pe-financial-era.json").read_text())["start_receipt"]
def rows(name): return list(csv.DictReader((root/"authority"/(name+".csv")).open()))
remote=rows("paper_bankroll")
if len(remote)!=1 or int(remote[0]["start_seq"])!=receipt["sequence"] or remote[0]["start_hash"]!=receipt["this_hash"]: raise SystemExit("authority predecessor differs")
# immutable: the quiesced capture has no sidecars and nobody has it open; never create them here.
db=sqlite3.connect("file:"+str(root/"state/paper_state.db")+"?mode=ro&immutable=1",uri=True); db.execute("BEGIN")
def meta(key):
    row=db.execute("select value from meta where key=?",(key,)).fetchone()
    if row is None: return None
    return row[0].decode() if isinstance(row[0],bytes) else str(row[0])
if meta("financial_start_seq")!=str(receipt["sequence"]) or meta("financial_start_hash")!=receipt["this_hash"]: raise SystemExit("SQLite predecessor differs")
local_cursor=None if meta("financial_last_prepared_seq") is None else int(meta("financial_last_prepared_seq"))
remote_cursor=None if not remote[0]["last_prepared_seq"] else int(remote[0]["last_prepared_seq"])
local_rows=sorted(r[0] for r in db.execute("select prepared_seq from fills union all select prepared_seq from settled_markets"))
remote_rows=sorted(int(r["prepared_seq"]) for name in ("paper_fills","settled_markets") for r in rows(name))
if local_cursor!=remote_cursor or local_rows!=remote_rows or local_cursor!=(local_rows[-1] if local_rows else None): raise SystemExit("captured completed Prepared frontier differs")
cash=db.execute("select bankroll_str from bankroll").fetchall()
if len(cash)!=1 or decimal.Decimal(cash[0][0])!=decimal.Decimal(remote[0]["bankroll_str"]): raise SystemExit("captured cash differs")
local={(r[0],r[1]):(decimal.Decimal(r[2]),decimal.Decimal(r[3])) for r in db.execute("select market_id,outcome_id,long_str,short_str from positions")}
authority={(r["market_id"],int(r["outcome_id"])):(decimal.Decimal(r["long_contracts"]),decimal.Decimal(r["short_contracts"])) for r in rows("paper_positions")}
if local!=authority: raise SystemExit("captured holdings differ")
PY

mkdir -m 700 -p "$run_root"
cp "$capture/capture.json" "$run_root/capture.json"
cp "$capture/copied.sha256" "$run_root/copied.sha256"
original_predecessor_sha=$(sha256_file "$capture/pe-financial-era.json")
original_predecessor_start=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["start_receipt"],separators=(",",":")))' "$capture/pe-financial-era.json")
original_generation=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["generation_dir"])' "$capture/pe-activation.json")
generation_id=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["activation_id"])' "$capture/pe-activation.json")
read -r kill_signal timeout_secs < <(python3 -c 'import json,sys
v=json.load(open(sys.argv[1])); k=v["KillSignal"]; t=v["TimeoutStopSec"]
if type(k)!=int or k!=2 or type(t)!=int or t<=0: raise SystemExit("old unit stop policy is unsafe")
print(k,t)' "$old_unit_policy") || die "invalid old unit policy"
if [[ -z "$fresh_bankroll" ]]; then
  fresh_bankroll=$(env_file_values "$target_environment" PE_BANKROLL_USD | python3 -c 'import sys,tomllib
rows=dict(part.decode().split("=",1) for part in sys.stdin.buffer.read().split(b"\0") if part)
v=tomllib.load(open(sys.argv[1],"rb")); print(rows.get("PE_BANKROLL_USD",v.get("bankroll_usd",10000)))' "$target_config")
fi
[[ "$fresh_bankroll" =~ ^[0-9]+([.][0-9]{1,6})?$ ]] || die "invalid target bankroll"
# Fresh local credentials are never inherited from either input environment.
python3 - "$run_root" "$pg_port" <<'PY'
import base64,hashlib,hmac,json,pathlib,secrets,sys
root=pathlib.Path(sys.argv[1]); secret=secrets.token_hex(32); password=secrets.token_hex(24)
def token(role):
    def enc(value): return base64.urlsafe_b64encode(json.dumps(value,separators=(",",":")).encode()).rstrip(b"=")
    body=enc({"alg":"HS256","typ":"JWT"})+b"."+enc({"role":role})
    return (body+b"."+base64.urlsafe_b64encode(hmac.new(secret.encode(),body,hashlib.sha256).digest()).rstrip(b"=")).decode()
(root/"jwt-secret").write_text(secret)
(root/"credentials").write_text("PE_SUPABASE_SECRET_KEY="+token("service_role")+"\nPE_SUPABASE_ANON_KEY="+token("anon")+"\n")
(root/"pg-password").write_text(password)
for path in root.iterdir(): path.chmod(0o600)
PY
# The authority runs in pinned local images on the host network, bound to loopback only. Secrets reach
# the containers through mode-0600 env files, never argv.
containers="pe-successor-$(printf '%s' "$run_root" | sha256sum | cut -c1-12)" unit=
remove_containers() {
  docker ps -aq --filter "name=^$containers-$1" | xargs -r docker rm -f >/dev/null 2>&1 || true
}
cleanup() {
  if [[ -n "$unit" ]]; then systemctl --user disable --now "$unit" >/dev/null 2>&1 || true; fi
  remove_containers ''
}
trap cleanup EXIT
private_env() {
  (umask 077; printf '%s\n' "${@:2}" > "$run_root/$1")
}
private_env postgres.env POSTGRES_USER=rehearsal "POSTGRES_PASSWORD=$(cat "$run_root/pg-password")"
docker run -d --name "$containers-pg" --network host --env-file "$run_root/postgres.env" postgres:16 \
  -c listen_addresses=127.0.0.1 -c port="$pg_port" >/dev/null
for attempt in {1..60}; do
  docker exec "$containers-pg" pg_isready -q -h 127.0.0.1 -p "$pg_port" -U rehearsal && break
  sleep 1
done
docker exec "$containers-pg" pg_isready -q -h 127.0.0.1 -p "$pg_port" -U rehearsal || die "private PostgreSQL did not start"
SUPABASE_DB_URL="postgres://rehearsal:$(cat "$run_root/pg-password")@127.0.0.1:$pg_port/postgres"
export SUPABASE_DB_URL
psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
create role anon nologin;
create role authenticated nologin;
create role service_role nologin bypassrls;
grant anon,authenticated,service_role to rehearsal;
SQL

start_authority() {
  local branch=$1
  psql_service_db -v ON_ERROR_STOP=1 -c "create database successor_$branch;" >/dev/null
  SUPABASE_DB_URL="postgres://rehearsal:$(cat "$run_root/pg-password")@127.0.0.1:$pg_port/successor_$branch"
  export SUPABASE_DB_URL
  psql_service_db -v ON_ERROR_STOP=1 <<'SQL'
grant usage on schema public to anon,authenticated,service_role;
alter default privileges in schema public grant all on tables to service_role;
alter default privileges in schema public grant all on sequences to service_role;
create schema auth;
create function auth.jwt() returns jsonb language sql stable as $$select coalesce(nullif(current_setting('request.jwt.claims',true),''),'{}')::jsonb$$;
create function auth.uid() returns uuid language sql stable as $$select (auth.jwt()->>'sub')::uuid$$;
SQL
  for schema in supabase_schema.sql supabase_paper_state_schema.sql supabase_multi_account_live_schema.sql supabase_wallet_live_stats_mv.sql; do
    psql_service_db -v ON_ERROR_STOP=1 -f "$REPO_ROOT/scripts/$schema" >> "$run_root/schema-$branch.log"
  done
  # Preserve every predecessor archive, using catalog-native copies of the installed table types.
  psql_service_db -v ON_ERROR_STOP=1 <<'ARCHIVE_SQL'
do $$ declare t text; begin
  foreach t in array array['paper_fills','settled_markets','paper_positions','paper_bankroll','fill_market_snapshots'] loop
    execute format('create table if not exists %I (like %I including defaults)',t||'_archive',t);
    execute format('alter table %I add column if not exists archived_at timestamptz default now()',t||'_archive');
    execute format('alter table %I add column if not exists activation_id text',t||'_archive');
  end loop;
end $$;
ARCHIVE_SQL
  # CSV headers are the export's exact columns. Load all row sets in one transaction.
  python3 - "$capture/authority" <<'PY' | psql_service_db -v ON_ERROR_STOP=1 > "$run_root/load-$branch.log"
import csv,pathlib,re,sys
root=pathlib.Path(sys.argv[1]); files=sorted(path for path in root.glob("*.csv") if not path.stem.endswith("_mv"))
required={"paper_fills","paper_positions","paper_bankroll","settled_markets","fill_market_snapshots","service_config","accounts","service_watchlist","service_runtime","ranking_batches","ranking_entries"}
if not required<={path.stem for path in files}: raise SystemExit("incomplete authority export")
print("begin;")
for path in files:
    columns=next(csv.reader(path.open()))
    if not re.fullmatch("[a-z_][a-z0-9_]*",path.stem) or not all(re.fullmatch("[a-z_][a-z0-9_]*",name) for name in columns):
        raise SystemExit("invalid authority CSV identifiers")
    print("truncate public."+path.stem+" cascade;")
for path in files:
    columns=next(csv.reader(path.open()))
    print("\\copy public."+path.stem+" ("+",".join(columns)+") from '"+str(path).replace("'","''")+"' with (format csv,header true)")
print("commit;")
PY
  verify_financial_service_contract
  account_census_receipt >/dev/null
  private_env "postgrest-$branch.env" "PGRST_DB_URI=$SUPABASE_DB_URL" PGRST_DB_SCHEMAS=public \
    PGRST_DB_ANON_ROLE=anon "PGRST_JWT_SECRET=$(cat "$run_root/jwt-secret")" \
    PGRST_SERVER_HOST=127.0.0.1 "PGRST_SERVER_PORT=$postgrest_port"
  docker run -d --name "$containers-rest-$branch" --network host \
    --env-file "$run_root/postgrest-$branch.env" postgrest/postgrest:v12.2.3 >/dev/null
  printf 'events {}\nhttp { access_log off; server { listen 127.0.0.1:%s; location /rest/v1/ { proxy_pass http://127.0.0.1:%s/; } } }\n' \
    "$http_port" "$postgrest_port" > "$run_root/nginx-$branch.conf"
  docker run -d --name "$containers-gw-$branch" --network host \
    -v "$run_root/nginx-$branch.conf:/etc/nginx/nginx.conf:ro" nginx:alpine >/dev/null
  for attempt in {1..60}; do
    curl -fsS "http://127.0.0.1:$http_port/rest/v1/" >/dev/null && return
    [[ $(docker inspect -f '{{.State.Running}}' "$containers-rest-$branch") == true ]] || die "private PostgREST exited"
    sleep 1
  done
  die "private PostgREST did not listen"
}

prepare_branch() {
  local branch=$1
  branch_root="$run_root/$branch"
  service="$branch_root/prediction-markets"
  generation="$service/gen/capture"
  mkdir -p "$generation" "$service/target/release" "$service/smoke-test" "$branch_root/target" "$branch_root/bin"
  cp -a --sparse=always "$capture/state/." "$generation/"
  cp "$capture/pe-activation.json" "$branch_root/pe-activation.json"
  cp "$capture/pe-financial-era.json" "$branch_root/pe-financial-era.json"
  cp "$old_binary" "$service/target/release/pe-service"
  cp "$target_binary" "$branch_root/target/pe-service"
  cp "$old_config" "$service/smoke-test/service.toml"
  cp "$target_config" "$branch_root/target/service.toml"
  cp "$old_environment" "$branch_root/old.template"
  cp "$target_environment" "$branch_root/target.template"
  cp "$run_root/credentials" "$branch_root/credentials"
  printf 'SUPABASE_DB_URL=%s\n' "$SUPABASE_DB_URL" >> "$branch_root/credentials"
  chmod 600 "$branch_root/credentials"
  # Every normalized assignment is explicit and hash-bound; unknown secret-bearing assignments are dropped.
  for tuple in old target; do
    env_file_values "$branch_root/$tuple.template" "${SERVICE_ENV_ALLOWLIST[@]}" | python3 -c 'import json,pathlib,sys
root,tuple,port,http=sys.argv[1:]; root=pathlib.Path(root); generation=root/"prediction-markets/gen/capture"
values=dict(part.decode().split("=",1) for part in sys.stdin.buffer.read().split(b"\0") if part)
credentials=dict(line.rstrip("\n").split("=",1) for line in (root/"credentials").read_text().splitlines())
values.update({key:credentials[key] for key in ("PE_SUPABASE_SECRET_KEY","PE_SUPABASE_ANON_KEY")})
values.update(PE_SUPABASE_URL="http://127.0.0.1:"+http,PE_BIND="127.0.0.1:"+port,
 PE_EVENT_LOG_PATH=str(generation/"paper.log"),
 PE_SOURCE_EVENT_LOG_PATH=str(generation/"source_events.log"),PE_PAPER_STATE_DB_PATH=str(generation/"paper_state.db"),
 PE_STATUS_PATH=str(generation/"status.json"),PE_JSONL_LOG_PATH=str(generation/"paper.jsonl"))
for key in ("PE_LEGACY_WALLET_HISTORY_PATH",):
    if key in values: values[key]=str(generation/"legacy_wallet_history.jsonl")
if any("\n" in v or "\x27" in v or "\\" in v for v in values.values()): raise SystemExit("unrepresentable normalized environment")
path=root/"prediction-markets/.env" if tuple=="old" else root/"target/service.env"
path.write_text("".join(k+"=\x27"+v+"\x27\n" for k,v in sorted(values.items()))); path.chmod(0o600)
(root/(tuple+".transformations.json")).write_text(json.dumps({"normalized_environment_sha256":"computed-in-result","paths":str(generation),"http_authority":values["PE_SUPABASE_URL"],"credential_provenance":"fresh local JWT"},sort_keys=True))
' "$branch_root" "$tuple" "$service_port" "$http_port"
  done
  mkdir "$branch_root/normalized-old"
  cp "$service/target/release/pe-service" "$branch_root/normalized-old/binary"
  cp "$service/smoke-test/service.toml" "$branch_root/normalized-old/config"
  cp "$service/.env" "$branch_root/normalized-old/environment"
  : > "$branch_root/.pe-deploy.lock"
  python3 - "$branch_root" <<'PY'
import hashlib,json,pathlib,sys
root=pathlib.Path(sys.argv[1]); service=root/"prediction-markets"; generation=service/"gen/capture"
identity=json.loads((root/"pe-activation.json").read_text()); predecessor=json.loads((root/"pe-financial-era.json").read_text())
for key,path in (("binary",service/"target/release/pe-service"),("config",service/"smoke-test/service.toml"),("environment",service/".env")):
    identity["destinations"][key]=str(path); identity["artifacts"][key]={"path":str(path),"sha256":hashlib.sha256(path.read_bytes()).hexdigest()}
predecessor["paths"]={"paper_log":str(generation/"paper.log"),"source_log":str(generation/"source_events.log"),"live_journal":str(generation/"live_journal.log"),"paper_state":str(generation/"paper_state.db")}
for name,value in (("pe-activation.json",identity),("pe-financial-era.json",predecessor)):
    (root/name).write_text(json.dumps(value,sort_keys=True,separators=(",",":"))+"\n")
PY
  # Relocate only the private migration paths through their existing owner.
  env -i PATH="$PATH" PE_PAPER_STATE_DB_PATH="$generation/paper_state.db" PE_EVENT_LOG_PATH="$generation/paper.log" \
    PE_SOURCE_EVENT_LOG_PATH="$generation/source_events.log" "$target_binary" --update-paper-migration-paths > "$branch_root/relocation.log"
  unit="pe-successor-$(printf '%s' "$branch_root" | sha256sum | cut -c1-16).service"
  python3 - "$branch_root" "$unit" "$timeout_secs" <<'PY'
import pathlib,shlex,sys
root=pathlib.Path(sys.argv[1]); service=root/"prediction-markets"
launcher=root/"launch-service"
launcher.write_text("#!/usr/bin/env bash\nset -euo pipefail\nexec env -i PATH=/usr/bin:/bin /bin/bash -c "+shlex.quote('set -euo pipefail; set -a; source "$1"; set +a; exec "$2" "$3"')+" bash "+shlex.quote(str(service/".env"))+" "+shlex.quote(str(service/"target/release/pe-service"))+" "+shlex.quote(str(service/"smoke-test/service.toml"))+"\n"); launcher.chmod(0o700)
(root/sys.argv[2]).write_text("[Unit]\nDescription=Private successor rehearsal\n[Service]\nWorkingDirectory="+str(service)+"\nExecStart="+str(launcher)+"\nKillSignal=SIGINT\nTimeoutStopSec="+sys.argv[3]+"\nRestart=no\n[Install]\nWantedBy=default.target\n")
shim=root/"bin/systemctl"
shim.write_text("#!/usr/bin/env bash\nset -euo pipefail\nif [[ $1 == is-enabled ]]; then\n  status=0; state=$(/usr/bin/systemctl --user is-enabled "+shlex.quote(sys.argv[2])+") || status=$?\n  if [[ $state == linked ]]; then echo disabled; exit 1; fi\n  printf '%s\\n' \"$state\"; exit \"$status\"\nfi\nargs=()\nfor arg in \"$@\"; do\n  [[ $arg != pe-service ]] || arg="+shlex.quote(sys.argv[2])+"\n  args+=(\"$arg\")\ndone\nexec /usr/bin/systemctl --user \"${args[@]}\"\n"); shim.chmod(0o700)
PY
  systemctl --user link "$branch_root/$unit" >/dev/null
  systemctl --user enable --now "$unit" >/dev/null
  wait_ready
  # Old boot validates membership using the Rust owner. Quiesce again before comparing stores.
  systemctl --user disable "$unit" >/dev/null
  systemctl --user stop "$unit"
  [[ $(systemctl --user show "$unit" -p MainPID --value) == 0 ]] || die "old capture service remains"
  "$harness_deploy_dir/pe_service_census.sh" --config "$service/smoke-test/service.toml" --cwd-root "$service" \
    --binary "$service/target/release/pe-service" --binary "$branch_root/target/pe-service" > "$branch_root/base-census.jsonl"
}
wait_ready() {
  local attempt
  # A relocated copy's first boot rebuilds its checkpoint by walking the retained window.
  for attempt in {1..3600}; do
    if curl -fsS "http://127.0.0.1:$service_port/health/ready" > "$branch_root/readiness.json"; then return; fi
    [[ $(systemctl --user is-active "$unit" || true) == active ]] || die "private service requires recovery"
    sleep 1
  done
  die "private service did not become ready"
}

archive_digest() {
  local transition=${1:-} table
  for table in paper_fills settled_markets paper_positions paper_bankroll fill_market_snapshots; do
    psql_service_db -v ON_ERROR_STOP=1 -Atc "select coalesce(jsonb_agg(to_jsonb(a) order by to_jsonb(a)::text),'[]'::jsonb)::text from ${table}_archive a where activation_id is distinct from '$transition';"
  done | sha256sum | awk '{print $1}'
}

assert_pristine_start() {
  local receipt observation remote transition census
  receipt=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["start_receipt"]))' "$branch_root/pe-financial-era.json")
  transition=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["activation_id"])' "$branch_root/pe-financial-era.json")
  observation=$(local_financial_observation "$generation/paper_state.db")
  printf '%s' "$observation" | verify_financial_observation "$receipt" "$fresh_bankroll"
  remote=$(psql_service_db -v ON_ERROR_STOP=1 -Atc "select json_build_object(
    'bankroll',(select json_agg(to_jsonb(b)) from paper_bankroll b),
    'counts',array[(select count(*) from paper_fills),(select count(*) from settled_markets),
      (select count(*) from paper_positions),(select count(*) from fill_market_snapshots)])::text;")
  printf '%s' "$remote" | python3 -c 'import decimal,json,sys
value=json.load(sys.stdin); receipt=json.loads(sys.argv[1]); rows=value["bankroll"]
if value["counts"]!=[0,0,0,0] or len(rows)!=1: raise SystemExit("pre-producer authority is not pristine")
row=rows[0]
if row["start_seq"]!=receipt["sequence"] or row["start_hash"]!=receipt["this_hash"] or row["last_prepared_seq"] is not None or decimal.Decimal(row["bankroll_str"])!=decimal.Decimal(sys.argv[2]):
    raise SystemExit("pre-producer authority Start differs")
' "$receipt" "$fresh_bankroll"
  [[ "$(archive_digest "$transition")" == "$predecessor_archive_digest" ]] || die "predecessor archives changed"
  census=$(activation_archive_counts "$transition" true)
  python3 -c 'import json,sys
m=json.load(open(sys.argv[1])); census=m["remote_census"]
if list(map(int,sys.argv[2].split()))!=[census[key] for key in ("paper_fills","settled_markets","paper_positions","paper_bankroll","fill_market_snapshots")]:
    raise SystemExit("successor archive differs from guarded census")
' "$branch_root/pe-financial-era.json" "$census"
}
stop_origin() {
  stop_origin_us=$(systemctl --user show "$unit" -p InactiveEnterTimestampMonotonic --value)
  [[ "$stop_origin_us" =~ ^[1-9][0-9]*$ ]] || die "private unit lacks a stop timestamp"
  stop_previous_ns=$((stop_origin_us * 1000))
}

record_stop_phase() {
  local phase=$1 now
  now=$(python3 -c 'import time; print(time.monotonic_ns())')
  (( now >= stop_previous_ns )) || die "private stop clock moved backwards"
  printf '%s|%s|%s\n' "$phase" "$((now-stop_previous_ns))" "$((now-stop_origin_us*1000))" >> "$branch_root/stop-durations.tsv"
  stop_previous_ns=$now
}

run_driver() {
  local begin end status
  begin=$(date +%s%N)
  PE_ACTIVATION_TESTING=1 PE_ACTIVATION_TEST_ROOT="$branch_root" PE_SUCCESSOR_REHEARSAL_BOOTSTRAP=1 \
    PE_SUCCESSOR_BOOTSTRAP_CREDENTIALS="$branch_root/credentials" PATH="$branch_root/bin:$PATH" \
    "$REPO_ROOT/scripts/paper_reset/activate_financial_era.sh" \
    --target-binary "$branch_root/target/pe-service" --target-config "$branch_root/target/service.toml" \
    --target-environment "$branch_root/target/service.env" --paper-log "$generation/paper.log" \
    --source-log "$generation/source_events.log" --live-journal "$generation/live_journal.log" \
    --paper-state "$generation/paper_state.db" --fresh-bankroll "$fresh_bankroll" \
    --rehearsal-evidence "$branch_root/bootstrap.json" --successor-of "$private_predecessor_sha" "$@" \
    >> "$branch_root/driver.log" 2>&1 && status=0 || status=$?
  end=$(date +%s%N)
  printf '%s|%s|%s\n' "$*" "$((end-begin))" "$status" >> "$branch_root/timings.tsv"
  return "$status"
}

for branch in forward rollback; do
  start_authority "$branch"
  prepare_branch "$branch"
  predecessor_archive_digest=$(archive_digest)
  private_predecessor_sha=$(sha256_file "$branch_root/pe-financial-era.json")
  # Restart the old unit so the driver's pre-stop census and account gates execute.
  systemctl --user enable --now "$unit" >/dev/null
  wait_ready
  if [[ "$branch" == forward ]]; then
    run_driver --simulate-crash-after guarded && die "guarded interruption did not occur" || [[ $? == 86 ]] || die "forward guard failed"
    stop_origin; record_stop_phase guarded
    run_driver --simulate-crash-after service-start-intent && die "pre-producer interruption did not occur" || [[ $? == 86 ]] || die "forward Start failed"
    assert_pristine_start
    record_stop_phase archive_start_seed_adopt
    run_driver || die "forward activation failed"
    wait_ready
    record_stop_phase start_to_ready
    run_driver || die "forward verification failed"
  else
    run_driver --simulate-crash-after remote-archived && die "archive interruption did not occur" || [[ $? == 86 ]] || die "rollback guard failed"
    stop_origin; record_stop_phase archived
    exec 8<>"$generation/paper.log"
    flock -x 8
    run_driver && die "writer lock did not refuse Start" || true
    python3 - "$branch_root/pe-financial-era.json" "$generation/paper_state.db" <<'PY'
import json,sqlite3,sys
manifest=json.load(open(sys.argv[1])); receipt=manifest["preparation"]["expected_receipt"]
db=sqlite3.connect("file:"+sys.argv[2]+"?mode=ro",uri=True)
def meta(key):
    value=db.execute("select value from meta where key=?",(key,)).fetchone()[0]
    return value.decode() if isinstance(value,bytes) else str(value)
if meta("financial_start_seq")!=str(receipt["sequence"]) or meta("financial_start_hash")!=receipt["this_hash"]:
    raise SystemExit("writer-lock interruption did not commit the expected local reset")
PY
    record_stop_phase reset_before_append
    grep -Eq 'Locked|locked' "$branch_root/driver.log" || die "Start did not report writer lock"
    flock -u 8; exec 8>&-
    # Stale cached status must not prevent durable rollback.
    python3 - "$generation/status.json" <<'PY'
import json,sys
v=json.load(open(sys.argv[1])); v["live"]["stale"]=True
json.dump(v,open(sys.argv[1],"w"))
PY
    run_driver --rollback-before-start --simulate-crash-after restoration-certified && die "restore interruption did not occur" || [[ $? == 86 ]] || die "rollback restoration failed"
    backup=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["backup"]["path"])' "$branch_root/pe-financial-era.json")
    cmp "$backup" "$generation/paper_state.db" || die "rollback SQLite bytes differ"
    [[ ! -e "$generation/paper_state.db-wal" && ! -e "$generation/paper_state.db-shm" ]] || die "rollback SQLite sidecars remain"
    record_stop_phase exact_restore
    run_driver --rollback-before-start || die "rollback reinstatement failed"
    wait_ready
    record_stop_phase old_service_to_ready
    sleep 1
    run_driver || die "fresh attempt failed"
    stop_origin
    wait_ready
    record_stop_phase fresh_attempt_to_ready
    run_driver || die "fresh attempt verification failed"
  fi
  [[ $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["state"])' "$branch_root/pe-financial-era.json") == verified ]] || die "branch was not verified"
  systemctl --user disable --now "$unit" >/dev/null
  unit=
  remove_containers "gw-$branch"; remove_containers "rest-$branch"
  SUPABASE_DB_URL="postgres://rehearsal:$(cat "$run_root/pg-password")@127.0.0.1:$pg_port/postgres"
  export SUPABASE_DB_URL
done
[[ $(successor_harness_bundle_digest "$harness_deploy_dir") == "$bundle" ]] || die "execution closure changed during rehearsal"
read -r revision artifact < <("$target_binary" --verify-staged-identity | parse_staged_identity) || die "invalid target identity"
python3 - "$run_root" "$bundle" "$original_predecessor_sha" "$original_predecessor_start" "$generation_id" "$original_generation" \
  "$revision" "$artifact" "$target_binary" "$target_config" "$target_environment" "$kill_signal" "$timeout_secs" <<'PY'
import hashlib,json,pathlib,platform,sys
root=pathlib.Path(sys.argv[1]); bundle,predecessor,start,generation,directory,revision,artifact=sys.argv[2:9]
def digest(path): return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
metadata=json.loads((root/"capture.json").read_text())
result={"kind":"successor-rehearsal-result-v1","result":"PASS","harness_bundle_sha256":bundle,
"predecessor_manifest_sha256":predecessor,"predecessor_start":json.loads(start),"generation_activation_id":generation,
"generation_dir":directory,"target_revision":revision,"artifact_blake3":artifact,
"target_tuple":dict(zip(("binary_sha256","config_sha256","environment_sha256"),map(digest,sys.argv[9:12]))),
"old_tuple":{key:metadata["old_tuple"][key] for key in ("binary_sha256","config_sha256","environment_sha256")},
"copy_manifest_sha256":digest(root/"copied.sha256"),"normalized_copy_sha256":digest(root/"forward/pe-activation.json"),
"normalized_tuples":{tuple:{key+"_sha256":digest(root/"forward"/relative) for key,relative in paths.items()} for tuple,paths in {"old":{"binary":"normalized-old/binary","config":"normalized-old/config","environment":"normalized-old/environment"},"target":{"binary":"target/pe-service","config":"target/service.toml","environment":"target/service.env"}}.items()},
"transformations":{"old":metadata["environment_transformations"],"private":[json.loads(path.read_text()) for path in root.glob("*/*.transformations.json")]},
"ownership_proof":"skipped: testing mode","unit_kill_signal":int(sys.argv[12]),"unit_timeout_stop_secs":int(sys.argv[13]),
"pass_rows":{key:"PASS" for key in ("capture_consistent","forward_verified","rollback_reset_before_append","rollback_exact_restore","fresh_attempt_verified")},
"phase_timings":{branch:(root/branch/"timings.tsv").read_text() for branch in ("forward","rollback")},
"stop_durations_ns":{branch:(root/branch/"stop-durations.tsv").read_text() for branch in ("forward","rollback")},
"component_timings":{branch:[line for line in (root/branch/"driver.log").read_text().splitlines() if line.startswith("financial-era timing ")] for branch in ("forward","rollback")}}
path=root/"result.json"; path.write_text(json.dumps(result,sort_keys=True,separators=(",",":"))+"\n"); path.chmod(0o600)
(root/"evidence.json").write_text(json.dumps({"kind":"successor-rehearsal-evidence-v1","result":"PASS","manifest_path":str(path),"evidence_sha256":digest(path)},sort_keys=True)+"\n")
(root/"diagnostics.json").write_text(json.dumps({"host":platform.node(),"platform":platform.platform(),"stop_timings_are_not_a_production_bound":True})+"\n")
PY
printf 'SUCCESSOR_REHEARSAL_PASS evidence=%s\n' "$run_root/evidence.json"
