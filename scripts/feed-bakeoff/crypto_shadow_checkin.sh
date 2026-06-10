#!/bin/bash
# 15-min check-in for the pe-crypto-shadow 4hr measurement run. Read-only against
# the live WAL DB (plus a tiny resolve upsert). Emits compact key=value lines.
cd ~/crypto-shadow || exit 1
DB=crypto_shadow.db
# Elapsed from the run PROCESS age (not the log mtime, which updates every write).
RUNPID=$(pgrep -f "pe-crypto-shadow run" | head -1)
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
if [ "$EL" -gt 0 ] && [ -n "$DBMB" ]; then
  RATE=$(( DBMB / EL ))
  echo "BURN_MB_PER_MIN=$RATE"
  if [ "$RATE" -gt 0 ]; then echo "MIN_TO_2600MB_FLOOR=$(( (AVAIL - 2600) / RATE ))"; fi
fi
sqlite3 "$DB" "
SELECT 'OBS_TOTAL='||count(*) FROM observations;
SELECT 'OBS_'||move_direction||'='||count(*) FROM observations GROUP BY move_direction;
SELECT 'OBS_WITH_NO_ASK='||count(*) FROM observations WHERE no_best_ask_str IS NOT NULL;
SELECT 'OBS_DOWN_WITH_NO_ASK='||count(*) FROM observations WHERE move_direction='down' AND no_best_ask_str IS NOT NULL;
SELECT 'RAW_TICKS='||count(*) FROM raw_ticks;
SELECT 'MARKETS_SEEN='||count(*) FROM markets;
SELECT 'RESOLUTIONS='||count(*) FROM resolutions;" 2>/dev/null
# Issue #311 validation AC. frames_dropped_* is stamped into meta AFTER drive
# returns, so it is meaningful at run end only (mid-run check-ins read the
# bounded periodic log summary instead). Any non-zero value invalidates the
# tape for the #310 sweep. CLOB_5M_STALENESS_S > 300 at run end = capture died.
sqlite3 "$DB" "SELECT key||'='||value FROM meta WHERE key LIKE 'frames_dropped_%';" 2>/dev/null
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
