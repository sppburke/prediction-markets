#!/usr/bin/env bash
# Continuously run the complete zero-argument rank-and-push pipeline while the
# operator flag contains exactly `run`. Linux/Forge only: requires flock,
# setsid, and negative-process-group signaling.

set -euo pipefail
cd "$(dirname "$0")/.."

if [[ $# -ne 0 ]]; then
  echo "FATAL: rank_and_push_loop.sh accepts no arguments" >&2
  exit 2
fi

for required in flock setsid; do
  command -v "$required" >/dev/null 2>&1 || {
    echo "FATAL: required command is unavailable: $required" >&2
    exit 2
  }
done
KILL_BIN="$(type -P kill || true)"
if [[ -z "$KILL_BIN" || ! -x "$KILL_BIN" ]]; then
  echo "FATAL: an external kill command is required for process-group signaling" >&2
  exit 2
fi
if ! setsid sh -c '"$1" -0 -- "-$$"' sh "$KILL_BIN"; then
  echo "FATAL: negative process-group signaling is unavailable" >&2
  exit 2
fi

FLAG_FILE="data/eval-results/rank_and_push.loop"
PENDING_FILE="data/eval-results/rank_and_push.pending"
LOOP_LOCK_FILE="data/eval-results/.rank_and_push_loop.lock"
TRANSIENT_RETRY_DELAY_SECS=60
mkdir -p "$(dirname "$FLAG_FILE")"

# Kernel-held singleton lock: stale process exits release it automatically. Open
# read/write without O_TRUNC so a rejected second launch cannot erase the live
# holder's PID metadata before it discovers that the inode is locked.
exec 9<>"$LOOP_LOCK_FILE"
if ! flock -n 9; then
  holder="$(tr -cd '0-9' < "$LOOP_LOCK_FILE" 2>/dev/null || true)"
  echo "FATAL: rank-and-push loop already running (PID ${holder:-unknown})" >&2
  exit 3
fi
: > "$LOOP_LOCK_FILE"
printf '%s\n' "$$" >&9

ACTIVE_CHILD_PID=""
TERM_GRACE_SECS=30

cleanup() {
  rm -f "$LOOP_LOCK_FILE"
}

terminate_active_group() {
  local exit_code="$1"
  trap - TERM INT
  if [[ -n "$ACTIVE_CHILD_PID" ]] && "$KILL_BIN" -0 -- "-$ACTIVE_CHILD_PID" 2>/dev/null; then
    echo "LOOP_SIGNAL child_pid=$ACTIVE_CHILD_PID action=TERM"
    "$KILL_BIN" -TERM -- "-$ACTIVE_CHILD_PID" 2>/dev/null || true
    for _ in $(seq 1 "$TERM_GRACE_SECS"); do
      if ! "$KILL_BIN" -0 -- "-$ACTIVE_CHILD_PID" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if "$KILL_BIN" -0 -- "-$ACTIVE_CHILD_PID" 2>/dev/null; then
      echo "LOOP_SIGNAL child_pid=$ACTIVE_CHILD_PID action=KILL" >&2
      "$KILL_BIN" -KILL -- "-$ACTIVE_CHILD_PID" 2>/dev/null || true
    fi
    wait "$ACTIVE_CHILD_PID" 2>/dev/null || true
    ACTIVE_CHILD_PID=""
  fi
  exit "$exit_code"
}

trap cleanup EXIT
trap 'terminate_active_group 143' TERM
trap 'terminate_active_group 130' INT

read_flag() {
  if [[ ! -e "$FLAG_FILE" ]]; then
    printf '%s' "missing"
    return 0
  fi
  local value
  local -a lines=()
  mapfile -t lines < "$FLAG_FILE"
  if [[ "${#lines[@]}" -ne 1 ]]; then
    echo "FATAL: $FLAG_FILE must contain exactly one line: 'run' or 'stop'" >&2
    return 2
  fi
  value="${lines[0]}"
  case "$value" in
    run|stop) printf '%s' "$value" ;;
    *)
      echo "FATAL: $FLAG_FILE must contain exactly 'run' or 'stop'" >&2
      return 2
      ;;
  esac
}

cycle=0
while true; do
  flag_value="$(read_flag)" || exit $?
  case "$flag_value" in
    missing)
      echo "LOOP_STOP reason=flag_missing"
      exit 0
      ;;
    stop)
      echo "LOOP_STOP reason=flag_stop"
      exit 0
      ;;
    run) ;;
  esac

  child_args=()
  child_kind="cycle"
  if [[ -e "$PENDING_FILE" ]]; then
    child_kind="resume"
    child_args+=(--resume-pending)
    echo "LOOP_RESUME_START utc=$(date -u +%Y-%m-%dT%H:%M:%SZ) pending=$PENDING_FILE"
  else
    cycle=$((cycle + 1))
    started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "LOOP_CYCLE_START cycle=$cycle utc=$started_at flag=$flag_value"
  fi

  # setsid makes the one-shot shell the leader of a dedicated process group.
  # Its machine-readable RANK_AND_PUSH_RUN_DIR line is inherited into this log.
  setsid bash scripts/rank_and_push.sh "${child_args[@]}" &
  ACTIVE_CHILD_PID=$!
  echo "LOOP_CHILD kind=$child_kind cycle=$cycle pid=$ACTIVE_CHILD_PID pgid=$ACTIVE_CHILD_PID"

  child_status=0
  wait "$ACTIVE_CHILD_PID" || child_status=$?
  ACTIVE_CHILD_PID=""
  ended_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if [[ "$child_kind" == "resume" ]]; then
    echo "LOOP_RESUME_END utc=$ended_at status=$child_status"
  else
    echo "LOOP_CYCLE_END cycle=$cycle utc=$ended_at status=$child_status"
  fi

  if [[ "$child_status" -eq 0 ]]; then
    continue
  fi
  if [[ "$child_status" -ne 75 ]]; then
    echo "FATAL: rank-and-push $child_kind exited $child_status; loop stopped" >&2
    exit "$child_status"
  fi

  [[ -e "$PENDING_FILE" ]] || {
    echo "FATAL: transient exit 75 did not retain $PENDING_FILE; loop stopped" >&2
    exit 1
  }
  echo "LOOP_TEMPFAIL kind=$child_kind status=75 retry_in=${TRANSIENT_RETRY_DELAY_SECS}s"
  for ((waited = 0; waited < TRANSIENT_RETRY_DELAY_SECS; waited++)); do
    flag_value="$(read_flag)" || exit $?
    case "$flag_value" in
      missing)
        echo "LOOP_STOP reason=flag_missing_during_retry"
        exit 0
        ;;
      stop)
        echo "LOOP_STOP reason=flag_stop_during_retry"
        exit 0
        ;;
      run) ;;
    esac
    sleep 1
  done
done
