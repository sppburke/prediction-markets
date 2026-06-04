#!/usr/bin/env bash
# Runs a 30-minute paper-mode smoke test against live Polygon + Polymarket data.
#
# Usage (from repo root):
#   bash smoke-test/run.sh
#
# Requirements:
#   - .env file in repo root with PE_POLYGON_HTTP_URL and PE_POLYGON_WS_URL
#   - Binary already built: cargo build --release -p pe-service
#
# The process runs for 30 minutes, then SIGINT is sent automatically.
# Logs land in smoke-test/paper.jsonl (JSONL) and smoke-test/paper.log (binary).

set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
    echo "ERROR: .env not found. Create it with PE_POLYGON_HTTP_URL and PE_POLYGON_WS_URL."
    exit 1
fi

if [[ ! -f target/release/pe-service ]]; then
    echo "ERROR: binary not found. Run: cargo build --release -p pe-service"
    exit 1
fi

# Load secrets into environment.
set -a
source .env
set +a

echo "Starting pe-service smoke test (30 min)..."
echo "Logs: smoke-test/paper.jsonl"
echo "Press Ctrl-C to stop early."

# Run for 30 minutes then send SIGINT for graceful shutdown.
timeout --signal=INT 1800 ./target/release/pe-service smoke-test/service.toml || {
    code=$?
    # exit code 124 = timeout (expected); 130 = SIGINT (user Ctrl-C); 0 = clean exit
    if [[ $code -eq 124 || $code -eq 130 || $code -eq 0 ]]; then
        echo "pe-service stopped (exit $code — expected)."
    else
        echo "pe-service exited with unexpected code $code."
        exit $code
    fi
}

echo ""
echo "=== Smoke-test summary ==="
echo "JSONL log: smoke-test/paper.jsonl"
echo "Binary log: smoke-test/paper.log"

if [[ -f smoke-test/paper.jsonl ]]; then
    echo ""
    echo "--- Last 20 log lines ---"
    tail -20 smoke-test/paper.jsonl
    echo ""
    echo "--- Event counts ---"
    jq -r '.msg // .message // "unknown"' smoke-test/paper.jsonl 2>/dev/null | sort | uniq -c | sort -rn | head -20 || \
        grep -o '"msg":"[^"]*"' smoke-test/paper.jsonl | sort | uniq -c | sort -rn | head -20 || true
fi
