#!/usr/bin/env bash
# Build the only supported fresh schema-v2 input: an emptied production schema-v1 copy.

set -euo pipefail
umask 077

maybe_crash() {
  if [[ -n "${SIMULATE_CRASH_AFTER:-}" && "$SIMULATE_CRASH_AFTER" == "$1" ]]; then
    echo "SIMULATED CRASH after $1" >&2
    exit 86
  fi
}

usage() {
  echo "usage: $0 [--execute] --source-main PATH --generation-dir DIR --legacy-history PATH" >&2
  exit 2
}

mode=dry-run
source_main=
generation_dir=
legacy_history=
while (($#)); do
  case "$1" in
    --execute) mode=execute; shift ;;
    --source-main) [[ $# -ge 2 ]] || usage; source_main=$2; shift 2 ;;
    --generation-dir) [[ $# -ge 2 ]] || usage; generation_dir=$2; shift 2 ;;
    --legacy-history) [[ $# -ge 2 ]] || usage; legacy_history=$2; shift 2 ;;
    *) usage ;;
  esac
done
[[ -n "$source_main" && -n "$generation_dir" && -n "$legacy_history" ]] || usage

source_main=$(realpath "$source_main")
legacy_history=$(realpath "$legacy_history")
generation_dir=$(realpath -m "$generation_dir")
destination="$generation_dir/paper_state.db"
history_destination="$generation_dir/wallet_market_history.json"
hash_record="$generation_dir/legacy-history.hashes"
expected_tables="bankroll dispatch_seeds dispatch_targets fill_market_snapshots fills leader_positions meta no_copy_dispositions poll_cursors positions seen_trades settled_markets"

for command in sqlite3 sha256sum b3sum realpath python3; do
  command -v "$command" >/dev/null || { echo "FATAL: $command not installed" >&2; exit 1; }
done
[[ -f "$source_main" ]] || { echo "FATAL: source main is not a file: $source_main" >&2; exit 1; }
[[ -f "$legacy_history" ]] || { echo "FATAL: legacy history is not a file: $legacy_history" >&2; exit 1; }

source_version=$(sqlite3 -readonly "file:$source_main?immutable=1" 'pragma user_version;')
source_tables=$(sqlite3 -readonly "file:$source_main?immutable=1" \
  "select name from sqlite_schema where type='table' and name not like 'sqlite_%' order by name;" | paste -sd' ' -)
[[ "$source_version" == 1 ]] || { echo "FATAL: source PRAGMA user_version=$source_version, expected 1" >&2; exit 1; }
[[ "$source_tables" == "$expected_tables" ]] || {
  echo "FATAL: unexpected schema-v1 user tables: $source_tables" >&2
  echo "expected: $expected_tables" >&2
  exit 1
}

legacy_blake3=$(b3sum "$legacy_history" | awk '{print $1}')
legacy_sha256=$(sha256sum "$legacy_history" | awk '{print $1}')

if [[ "$mode" == dry-run ]]; then
  printf '%s\n' \
    'DRY-RUN: no files written' \
    "source_main=$source_main" \
    "generation_dir=$generation_dir" \
    'postcondition.user_tables=12 exact a5f3a8f allowlist' \
    'postcondition.rows_each=0' \
    'postcondition.user_version=1' \
    'postcondition.integrity_check=ok' \
    'postcondition.paper.log=empty' \
    'postcondition.live_journal.log=empty' \
    'postcondition.source_events.log=empty' \
    "legacy_history.copy=$history_destination" \
    "legacy_history.blake3=$legacy_blake3" \
    "legacy_history.sha256=$legacy_sha256"
  exit 0
fi

mkdir -p "$generation_dir"
tmp="$generation_dir/.paper_state.db.seed.$$"
trap 'rm -f "$tmp" "$tmp-wal" "$tmp-shm"' EXIT

if [[ -e "$destination" ]]; then
  existing_version=$(sqlite3 -readonly "file:$destination?immutable=1" 'pragma user_version;')
  existing_total=$(sqlite3 -readonly "file:$destination?immutable=1" \
    "select (select count(*) from fills)+(select count(*) from positions)+(select count(*) from bankroll)+(select count(*) from settled_markets)+(select count(*) from fill_market_snapshots)+(select count(*) from seen_trades)+(select count(*) from leader_positions)+(select count(*) from poll_cursors)+(select count(*) from meta)+(select count(*) from no_copy_dispositions)+(select count(*) from dispatch_seeds)+(select count(*) from dispatch_targets);")
  [[ "$existing_version" == 1 && "$existing_total" == 0 ]] || {
    echo "FATAL: existing generation main is not the empty version-one seed" >&2
    exit 1
  }
else
  sqlite3 -readonly "$source_main" ".backup '$tmp'"
  sqlite3 "$tmp" >/dev/null <<'SQL'
begin immediate;
delete from fills;
delete from positions;
delete from bankroll;
delete from settled_markets;
delete from fill_market_snapshots;
delete from seen_trades;
delete from leader_positions;
delete from poll_cursors;
delete from meta;
delete from no_copy_dispositions;
delete from dispatch_targets;
delete from dispatch_seeds;
commit;
pragma wal_checkpoint(truncate);
SQL
  mv "$tmp" "$destination"
  python3 -c 'import os,sys
directory=os.open(sys.argv[1], os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try: os.fsync(directory)
finally: os.close(directory)' "$generation_dir"
  maybe_crash seed-main
fi

actual_tables=$(sqlite3 -readonly "file:$destination?immutable=1" \
  "select name from sqlite_schema where type='table' and name not like 'sqlite_%' order by name;" | paste -sd' ' -)
actual_version=$(sqlite3 -readonly "file:$destination?immutable=1" 'pragma user_version;')
integrity=$(sqlite3 -readonly "file:$destination?immutable=1" 'pragma integrity_check;')
[[ "$actual_tables" == "$expected_tables" ]] || { echo "FATAL: seeded table allowlist drift" >&2; exit 1; }
for table in $expected_tables; do
  table_rows=$(sqlite3 -readonly "file:$destination?immutable=1" "select count(*) from $table;")
  [[ "$table_rows" == 0 ]] || { echo "FATAL: seeded table $table contains $table_rows row(s)" >&2; exit 1; }
done
[[ "$actual_version" == 1 ]] || { echo "FATAL: seeded user_version=$actual_version" >&2; exit 1; }
[[ "$integrity" == ok ]] || { echo "FATAL: seeded integrity_check=$integrity" >&2; exit 1; }

for log_name in paper.log live_journal.log source_events.log; do
  python3 -c 'import os,sys
path=os.path.join(sys.argv[1],sys.argv[2])
fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
with os.fdopen(fd,"wb") as handle: handle.flush(); os.fsync(handle.fileno())' \
    "$generation_dir" "$log_name"
  case "$log_name" in
    paper.log) maybe_crash seed-paper-log ;;
    live_journal.log) maybe_crash seed-live-journal ;;
    source_events.log) maybe_crash seed-source-log ;;
  esac
done
if [[ -e "$history_destination" ]]; then
  [[ "$(sha256sum "$history_destination" | awk '{print $1}')" == "$legacy_sha256" ]] || {
    echo "FATAL: existing generation legacy history differs from the requested input" >&2
    exit 1
  }
else
  history_tmp="$generation_dir/.wallet_market_history.json.$$"
  cp "$legacy_history" "$history_tmp"
  python3 -c 'import os,sys
path=sys.argv[1]
with open(path, "rb") as handle: os.fsync(handle.fileno())' "$history_tmp"
  mv "$history_tmp" "$history_destination"
fi
maybe_crash seed-history
hash_tmp="$generation_dir/.legacy-history.hashes.$$"
printf 'blake3 %s  %s\nsha256 %s  %s\n' \
  "$legacy_blake3" "$legacy_history" "$legacy_sha256" "$legacy_history" > "$hash_tmp"
python3 -c 'import os,sys
path=sys.argv[1]
with open(path, "rb") as handle: os.fsync(handle.fileno())' "$hash_tmp"
mv "$hash_tmp" "$hash_record"
python3 -c 'import os,sys
directory=os.open(sys.argv[1], os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try: os.fsync(directory)
finally: os.close(directory)' "$generation_dir"
maybe_crash seed-history-hashes

printf '%s\n' \
  "seed.main=$destination" \
  'postcondition.user_tables=12 exact a5f3a8f allowlist' \
  'postcondition.rows_each=0' \
  'postcondition.user_version=1' \
  'postcondition.integrity_check=ok' \
  'postcondition.paper.log=empty' \
  'postcondition.live_journal.log=empty' \
  'postcondition.source_events.log=empty' \
  "legacy_history.copy=$history_destination" \
  "legacy_history.blake3=$legacy_blake3" \
  "legacy_history.sha256=$legacy_sha256" \
  "legacy_history.record=$hash_record"
