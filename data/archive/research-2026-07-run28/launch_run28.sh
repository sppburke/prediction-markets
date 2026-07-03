#!/bin/bash
cd ~/prediction-markets
pkill -9 -f ranker.bakeoff 2>/dev/null
sleep 2
mkdir -p runs
PYTHONPATH=scripts setsid nohup .venv-analysis/bin/python -m ranker.bakeoff \
  --forward-criteria-filter --workers 48 --min-periods 6 --steps 24 --step-days 7 \
  --horizon-days 7 --train-days 180 --universe-pos-min 20 --engine duck \
  --parquet-dir data/parquet --pe-backtest target/release/pe-backtest \
  --cache data/wallet_cache.db --start-unix 1767398400 \
  --out-dir runs/run28-2026-weekly-lambda --executor process > runs/run28.log 2>&1 &
sleep 12
RP=$(pgrep -f "ranker.bakeoff.*run28-2026-weekly-lambda" | sort -n | head -1)
echo "$RP" > runs/run28.pid
echo "REAL_PID=$RP"
kill -0 "$RP" 2>/dev/null && echo "STATUS=RUNNING" || echo "STATUS=FAILED"
echo "--- log ---"; tail -5 runs/run28.log
