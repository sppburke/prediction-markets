#!/usr/bin/env bash
# wait-for-issue-merge.sh — block until a GitHub issue is closed by a merged PR on main.
#
# Watches two conditions and exits 0 only when BOTH hold:
#   1. The issue's GitHub state has flipped to "closed".
#   2. A commit on origin/main mentions the issue (`#<num>` in subject or body).
#
# The combined check guards against transient closes (e.g. someone clicking "close"
# manually) — only a real merge produces a referencing commit on origin/main.
#
# Usage:
#   scripts/wait-for-issue-merge.sh <issue-number> [poll-interval-sec]
#
# Defaults: poll every 180 s. Exit codes: 0 = merged, 2 = usage error.

set -euo pipefail

ISSUE="${1:?usage: $0 <issue-number> [poll-interval-sec]}"
INTERVAL="${2:-180}"
REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"

log() { printf '[wait-for-issue-merge] %s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }

log "watching issue #${ISSUE} in ${REPO}; poll every ${INTERVAL}s"

while true; do
    if ! state=$(gh api "repos/${REPO}/issues/${ISSUE}" --jq '.state' 2>/dev/null); then
        log "warn: failed to fetch issue state; retrying"
        sleep "${INTERVAL}"
        continue
    fi

    if [[ "$state" == "closed" ]]; then
        git fetch origin main --quiet 2>/dev/null || true
        if commit=$(git log origin/main --grep="#${ISSUE}\\b" -n1 --pretty=format:'%h %s' 2>/dev/null) \
           && [[ -n "$commit" ]]; then
            log "merged: ${commit}"
            exit 0
        else
            log "issue closed but no main commit references #${ISSUE} yet; waiting"
        fi
    else
        log "state=${state}; sleeping ${INTERVAL}s"
    fi
    sleep "${INTERVAL}"
done
