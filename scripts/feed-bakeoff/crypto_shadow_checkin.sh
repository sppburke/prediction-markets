#!/bin/bash
# Check-in for a pe-crypto-shadow measurement run. TWO MODES:
#
#   (default)  LIVE-SAFE — process / disk / watchdog / run.log greps ONLY.
#              Touches NEITHER the SQLite DB NOR the network. Safe to run at
#              any cadence while the capture is live.
#   --post     Full #311 + #317 AC queries + resolve + report. REFUSES to run while
#              the capture process exists (override with --force, which WILL
#              invalidate the tape — see below).
#
# WHY THE SPLIT (incident 2026-06-10 04:48Z): the old single-mode script ran
# `resolve` (a SQLite WRITER) and full-scan reads against the LIVE DB. SQLite
# allows one writer at a time, so the drive loop's batched flush blocked behind
# the resolve transaction, the bounded frame channel filled, and 3,284 frames
# dropped — non-zero frames_dropped_* is run-invalidating per the #311
# contract. Mid-run telemetry must come from run.log and the filesystem only.
set -u
cd ~/crypto-shadow || exit 1
DB=crypto_shadow.db
LOG=run.log

MODE=live
FORCE=0
for arg in "$@"; do
  case "$arg" in
    --post) MODE=post ;;
    --force) FORCE=1 ;;
    *) echo "usage: $0 [--post [--force]]" >&2; exit 2 ;;
  esac
done

RUNPID=$(pgrep -x pe-crypto-shado | head -1)
if [ -n "$RUNPID" ]; then
  EL=$(( $(ps -o etimes= -p "$RUNPID" | tr -d ' ') / 60 ))
  echo "RUN=alive"
else
  EL=0
  echo "RUN=STOPPED"
fi
REM=$(( 240 - EL ))
echo "NOW=$(date -u +%H:%M:%SZ) ELAPSED_MIN=$EL REMAIN_MIN=$REM"
if pgrep -f 'while pgrep -f "pe-crypto-shadow run"' >/dev/null; then WD=alive; else WD=gone; fi
echo "WATCHDOG=$WD $( [ -s watchdog.log ] && cat watchdog.log || echo clean )"
AVAIL=$(df --output=avail -m / | tail -1 | tr -d ' ')
DBMB=$(du -m "$DB" 2>/dev/null | cut -f1)
echo "DISK_AVAIL_MB=$AVAIL DB_MB=${DBMB:-0}"
if [ "$EL" -gt 0 ] && [ -n "${DBMB:-}" ]; then
  RATE=$(( DBMB / EL ))
  echo "BURN_MB_PER_MIN=$RATE"
  if [ "$RATE" -gt 0 ]; then echo "MIN_TO_2600MB_FLOOR=$(( (AVAIL - 2600) / RATE ))"; fi
fi

# ---- log-derived telemetry (live-safe: file reads only) ----
# Drop warnings are the mid-run saturation tell; the runner logs a bounded
# cumulative summary line whenever any tally is non-zero.
DROP_LINES=$(grep -c "frames dropped" "$LOG" 2>/dev/null || true)
echo "LOG_DROP_WARN_LINES=${DROP_LINES:-0}"
if [ "${DROP_LINES:-0}" -gt 0 ]; then
  echo "LOG_LAST_DROP=$(grep "frames dropped" "$LOG" | tail -1)"
fi
echo "LOG_LAST_DECODE_ERRORS=$(grep "decode errors" "$LOG" | tail -1 | grep -oE '"(chainlink|clob|exchange)":[0-9]+' | tr '\n' ' ')"
echo "LOG_SUBSCRIBED_EVENTS=$(grep -c "subscribed new CLOB" "$LOG" 2>/dev/null || true)"
echo "LOG_PRUNED_EVENTS=$(grep -c "pruned expired markets" "$LOG" 2>/dev/null || true)"
echo "LOG_RECONNECTS=$(grep -c "stream ended; reconnecting" "$LOG" 2>/dev/null || true)"

if [ "$MODE" = live ]; then
  echo "MODE=live-safe (no DB/network access; run --post after run end for AC queries + resolve + report)"
  exit 0
fi

# ---- post-run mode: DB queries + resolve + report ----
if [ -n "$RUNPID" ] && [ "$FORCE" -ne 1 ]; then
  echo "REFUSED: capture run (pid $RUNPID) is live — post-mode DB access stalls the" >&2
  echo "flush and DROPS FRAMES (run-invalidating; 2026-06-10 04:48Z incident)." >&2
  echo "Wait for run end, or pass --force to invalidate the tape knowingly." >&2
  exit 3
fi
sqlite3 "$DB" "
SELECT 'OBS_TOTAL='||count(*) FROM observations;
SELECT 'OBS_'||move_direction||'='||count(*) FROM observations GROUP BY move_direction;
SELECT 'OBS_WITH_NO_ASK='||count(*) FROM observations WHERE no_best_ask_str IS NOT NULL;
SELECT 'OBS_DOWN_WITH_NO_ASK='||count(*) FROM observations WHERE move_direction='down' AND no_best_ask_str IS NOT NULL;
SELECT 'RAW_TICKS='||MAX(id) FROM raw_ticks;
SELECT 'MARKETS_SEEN='||count(*) FROM markets;
SELECT 'RESOLUTIONS='||count(*) FROM resolutions;" 2>/dev/null
# Issue #311 validation AC. frames_dropped_* is stamped into meta AFTER drive
# returns (run end); MISSING keys mean a crashed capture — not-clean, never 0
# (the #310 sweep stamps that case 'unknown'). Any non-zero value invalidates
# the tape. CLOB_5M_STALENESS_S > 300 at run end = the 5m capture died.
AC_KEYS=$(sqlite3 "$DB" "SELECT key||'='||value FROM meta WHERE key LIKE 'frames_dropped_%';" 2>/dev/null)
if [ -z "$AC_KEYS" ]; then
  echo "FRAMES_DROPPED=MISSING (crashed capture? not-clean per #310 tape-validity)"
else
  echo "$AC_KEYS"
fi
# Issue #317 AC2: max CLOB inter-frame gap (secs), stamped to meta at clean run
# end like frames_dropped_*. MISSING = crashed capture (not-clean); >= 300 = a
# CLOB feed died mid-run (fails crypto_shadow_tape_validity_max_gap_secs=300).
MAX_GAP=$(sqlite3 "$DB" "SELECT value FROM meta WHERE key='max_clob_gap_secs';" 2>/dev/null)
if [ -z "$MAX_GAP" ]; then
  echo "MAX_CLOB_GAP_SECS=MISSING (crashed capture? not-clean per #317 AC)"
else
  echo "MAX_CLOB_GAP_SECS=$MAX_GAP"
fi
sqlite3 "$DB" "SELECT 'CLOB_5M_STALENESS_S='||CAST((strftime('%s','now')*1000 - MAX(received_at_ms))/1000 AS INTEGER) FROM clob_trades WHERE series='5m';" 2>/dev/null
./pe-crypto-shadow resolve >/tmp/resolve.log 2>&1
echo "RESOLVE: $(grep -oE '"(observed_markets|resolved)":[0-9]+' /tmp/resolve.log | tr '\n' ' ')"
./pe-crypto-shadow report 2>/dev/null > /tmp/rep.json
python3 - <<'PY'
import json
try:
    d = json.load(open("/tmp/rep.json"))
except Exception as e:
    print("REPORT_ERR", e); raise SystemExit
print("REPORT_TOTAL_OBS=" + str(d["total_observations"]))
rz = d.get("realized", [])
print("REALIZED_GROUPS=" + str(len(rz)))
for g in rz:
    print("  R %s %-4s n=%s mean=%s frac+=%s" % (
        g["series"], g["move_direction"], g["count_scored"],
        g["mean_realized_net_vs_ask"], g["frac_realized_positive"]))
PY
