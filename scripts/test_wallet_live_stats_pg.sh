#!/bin/bash
# wallet_live_stats economics suite: the site's realized P&L deducts recorded fees.
#
# Runs against a disposable Postgres cloned from the CI template, where scripts/supabase_schema.sql,
# scripts/supabase_paper_state_schema.sql and scripts/supabase_wallet_live_stats_mv.sql are loaded in
# that order. Verifies that the view's realized P&L, wins, win rate and edge follow
# `pe_paper_pnl::realized_edge` (BUY payout − principal − fee, SELL principal − payout − fee), that a
# row without principal/fee keeps the notional formula, that the column contract and security_invoker
# option are unchanged, and that re-applying either schema file alone keeps the same results in the
# view and in the refreshed materialized view.
#
# Usage: PE_WALLET_STATS_TEST_DISPOSABLE=1 bash scripts/test_wallet_live_stats_pg.sh "$PG_URL"
# The target must have empty paper_fills and settled_markets; use a new disposable database per run.
set -u
URL="${1:?pass a disposable Postgres URL}"
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
fails=0

# This suite writes fills and re-applies schema files. Require an explicit local disposable-db
# attestation outside CI, a loopback URL, and empty target tables. Never delete rows to make a
# target look disposable.
if [ "${CI:-}" != "true" ] && [ "${PE_WALLET_STATS_TEST_DISPOSABLE:-}" != "1" ]; then
  echo "REFUSE: set PE_WALLET_STATS_TEST_DISPOSABLE=1 only for a disposable database" >&2
  exit 2
fi
case "$URL" in
  postgres://*@localhost:*/*|postgresql://*@localhost:*/*|postgres://*@127.0.0.1:*/*|postgresql://*@127.0.0.1:*/*) ;;
  *) echo "REFUSE: target URL must use a loopback host" >&2; exit 2 ;;
esac
q() { psql "$URL" -X -v ON_ERROR_STOP=1 -Atq -c "$1"; }
guard=$(q "select case when not exists (select 1 from paper_fills) and not exists (select 1 from settled_markets) then 'disposable' end")
if [ "$guard" != "disposable" ]; then
  echo "REFUSE: target must have empty paper_fills and settled_markets" >&2
  exit 2
fi
check() { # name, query, expected
  local got
  got=$(q "$2" 2>&1)
  if [ "$got" = "$3" ]; then echo "PASS: $1"; else echo "FAIL: $1: got [$got], want [$3]"; fails=$((fails+1)); fi
}

# One wallet (mixed case: the view groups on lower()), five fills:
#   k1 BUY  100 @ 0.40, recorded principal 40.5 (not 40.0), fee 1.68; outcome 0 pays 1 → 100 − 40.5 − 1.68 = 57.82 (won)
#   k2 BUY   10 @ 0.99, principal 9.9, fee 0.2; pays 1 → 10 − 9.9 − 0.2 = −0.10 (lost; +0.10 before fees)
#   k3 SELL  50 @ 0.30, principal 15, fee 0.5; outcome 1 pays 0 → 15 − 0 − 0.5 = 14.50 (won)
#   k4 BUY   20 @ 0.25, no principal/fee (legacy); outcome 0 pays 0 → (0 − 0.25) × 20 = −5.00 (lost)
#   k5 BUY   30 @ 0.50, principal 15, fee 0.53; market still open
# Settled 4, open 1, won 2: realized 67.22, win rate 0.5, edge 16.805.
WALLET=0xAbCdEf0123456789aBcDeF0123456789AbCdEf01
psql "$URL" -X -v ON_ERROR_STOP=1 -q <<SQL || { echo "FAIL: seed"; exit 1; }
insert into paper_fills (idempotency_key, leader_wallet, source_trade_id, market_id, outcome_id, side,
                         contracts, fill_price, entry_unix, event_seq, principal, fee)
values ('k1', '$WALLET', 't1', 'm1', 0, 'buy', 100, 0.40, 1800000000, 1, 40.5, 1.68),
       ('k2', '$WALLET', 't2', 'm2', 0, 'buy', 10, 0.99, 1800000001, 2, 9.9, 0.2),
       ('k3', '$WALLET', 't3', 'm3', 1, 'sell', 50, 0.30, 1800000002, 3, 15, 0.5),
       ('k4', '$WALLET', 't4', 'm4', 0, 'buy', 20, 0.25, 1800000003, 4, null, null),
       ('k5', '$WALLET', 't5', 'm5', 0, 'buy', 30, 0.50, 1800000004, 5, 15, 0.53);
insert into settled_markets (market_id, outcome_prices, credit_applied, settled_at_unix)
values ('m1', '["1","0"]', 100, 1800000100),
       ('m2', '["1","0"]', 10, 1800000101),
       ('m3', '["1","0"]', 0, 1800000102),
       ('m4', '["0","1"]', 0, 1800000103);
SQL

STATS="live_total_fills, live_settled_count, live_open_fills, live_wins, round(live_win_rate, 4),
       round(live_realized_pnl, 4), round(live_edge, 4)"
EXPECTED="5|4|1|2|0.5000|67.2200|16.8050"
# The production column contract (read from Supabase on 2026-10-20) must not change.
COLUMNS="wallet text, live_total_fills bigint, live_settled_count bigint, live_open_fills bigint, live_wins bigint, live_win_rate numeric, live_realized_pnl numeric, live_edge numeric, rank integer, ls_edge numeric, ls_tstat numeric, fill_rate numeric, n_trades integer, hit_rate numeric, avg_price numeric, last_trade_unix bigint"

verify() { # stage label
  check "$1: view economics" "select $STATS from wallet_live_stats where wallet = lower('$WALLET')" "$EXPECTED"
  check "$1: column contract" "select string_agg(column_name || ' ' || data_type, ', ' order by ordinal_position)
    from information_schema.columns where table_schema = 'public' and table_name = 'wallet_live_stats'" "$COLUMNS"
  check "$1: security_invoker kept" "select reloptions::text from pg_class
    where oid = 'public.wallet_live_stats'::regclass" "{security_invoker=true}"
  check "$1: one definition" "select count(*) from pg_views where viewname = 'wallet_live_stats'" "1"
  if q "refresh materialized view wallet_live_stats_mv" >/dev/null; then
    check "$1: refreshed matview economics" "select $STATS from wallet_live_stats_mv where wallet = lower('$WALLET')" "$EXPECTED"
  else
    echo "FAIL: $1: matview refresh"; fails=$((fails+1))
  fi
}

verify "template (CI order)"
if psql "$URL" -X -v ON_ERROR_STOP=1 -q -f "$SCRIPT_DIR/supabase_schema.sql" >/dev/null 2>&1; then
  verify "base file re-applied alone"
else
  echo "FAIL: re-applying scripts/supabase_schema.sql"; fails=$((fails+1))
fi
if psql "$URL" -X -v ON_ERROR_STOP=1 -q -f "$SCRIPT_DIR/supabase_paper_state_schema.sql" >/dev/null 2>&1; then
  verify "paper-state file re-applied alone"
else
  echo "FAIL: re-applying scripts/supabase_paper_state_schema.sql"; fails=$((fails+1))
fi

if [ "$fails" -gt 0 ]; then echo "wallet_live_stats suite: $fails failure(s)"; exit 1; fi
echo "wallet_live_stats suite: all checks passed"
