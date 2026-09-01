-- #544 operator migration: retire only the reviewed service_config rows.
--
-- Idempotent. Apply with psql. The preflight and DELETE share one
-- transaction, so any unknown key raises before mutation and exits nonzero while listing the
-- unresolved keys.
\set ON_ERROR_STOP on

begin;

do $$
declare
  unknown_keys text[];
begin
  select array_agg(key order by key)
    into unknown_keys
    from service_config
   where key <> all (array[
     'active_watchlist_size',
     'mode',
     'max_fill_price',
     'min_fill_price',
     'min_resolution_horizon_secs',
     'max_resolution_horizon_secs',
     'fill_mode',
     'price_impact_cap_bps',
     'flip_human_approved',
     'kelly_fraction_above_default_human_approved',
     'polymarket_fee_rate',
     'kelly_fraction_override',
     'per_trade_cap',
     'slippage_rate',
     'sizing_mode',
     'sizing_dollar_usd',
     'sizing_contracts',
     'bankroll_usd',
     'bench_overfetch',
     'demotion_cb_alpha',
     'demotion_min_trades',
     'demotion_pnl_window_secs',
     'gamma_resolution_poll_interval_secs',
     'inactivity_hard_cap_secs',
     'inactivity_threshold_secs',
     'log_retention_days',
     'maintenance_interval_secs',
     'paper_fill_haircut_bps',
     'paper_fill_slippage_bps',
     'status_interval_secs',
     'supabase_refresh_interval_secs',
     'supabase_sink_reconcile_interval_secs',
     'entry_gate_fail_closed',
     'position_page_limit',
     'position_reseed_interval_secs',
     'position_size_threshold',
     'wallet_market_history_path',
     'clob_best_ask_fallback_haircut_bps',
     'trade_poll_interval_secs'
   ]::text[]);

  if unknown_keys is not null then
    raise exception 'unknown service_config keys: %', array_to_string(unknown_keys, ', ');
  end if;
end;
$$;

delete from service_config
 where key = any (array[
   'bankroll_usd',
   'bench_overfetch',
   'demotion_cb_alpha',
   'demotion_min_trades',
   'demotion_pnl_window_secs',
   'gamma_resolution_poll_interval_secs',
   'inactivity_hard_cap_secs',
   'inactivity_threshold_secs',
   'log_retention_days',
   'maintenance_interval_secs',
   'paper_fill_haircut_bps',
   'paper_fill_slippage_bps',
   'status_interval_secs',
   'supabase_refresh_interval_secs',
   'supabase_sink_reconcile_interval_secs',
   'entry_gate_fail_closed',
   'position_page_limit',
   'position_reseed_interval_secs',
   'position_size_threshold',
   'wallet_market_history_path',
   'clob_best_ask_fallback_haircut_bps',
   'trade_poll_interval_secs'
 ]::text[]);

commit;
