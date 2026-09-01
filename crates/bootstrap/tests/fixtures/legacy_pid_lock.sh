#!/usr/bin/env bash
# Test-only preservation of the 47b95eee PID-file acquisition paths. The
# optional barriers make their check/remove races deterministic; production
# code must never source or invoke this fixture (#544).
set -euo pipefail

mode="$1"
lock_file="$2"

case "$mode" in
  wrapper-race)
    ready_file="$3"
    go_file="$4"
    result_file="$5"
    token="$6"
    if [[ -e "$lock_file" ]]; then
      lock_pid="$(cat "$lock_file" 2>/dev/null || true)"
      if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
        exit 3
      fi
      rm -f "$lock_file"
    fi
    printf '%s\n' "$token" >> "$ready_file"
    while [[ ! -e "$go_file" ]]; do :; done
    printf '%s\n' "$$" > "$lock_file"
    printf '%s\n' "$token" >> "$result_file"
    ;;
  wrapper-check)
    if [[ -e "$lock_file" ]]; then
      lock_pid="$(cat "$lock_file" 2>/dev/null || true)"
      if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
        exit 3
      fi
      rm -f "$lock_file"
    fi
    printf '%s\n' "$$" > "$lock_file"
    ;;
  cache-reclaim)
    if (set -o noclobber; : > "$lock_file") 2>/dev/null; then
      printf '%s\n' "$$" > "$lock_file"
      exit 0
    fi
    lock_pid="$(cat "$lock_file" 2>/dev/null || true)"
    if [[ -n "$lock_pid" ]] && kill -0 "$lock_pid" 2>/dev/null; then
      exit 3
    fi
    rm -f "$lock_file"
    (set -o noclobber; : > "$lock_file")
    printf '%s\n' "$$" > "$lock_file"
    ;;
  *)
    exit 64
    ;;
esac
