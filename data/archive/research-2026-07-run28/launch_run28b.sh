#!/bin/bash
cd /home/ubuntu/prediction-markets
mkdir -p runs/run28b-thread-nogil
PYTHONPATH=scripts .venv-analysis/bin/python -m ranker.bakeoff \
  --forward-criteria-filter --workers 48 --min-periods 6 --steps 24 \
  --step-days 7 --horizon-days 7 --train-days 180 --universe-pos-min 20 \
  --engine duck --parquet-dir data/parquet \
  --pe-backtest target/release/pe-backtest --cache data/wallet_cache.db \
  --start-unix 1767398400 --out-dir runs/run28b-thread-nogil \
  --executor thread > runs/run28b.log 2>&1
