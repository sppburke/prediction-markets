-- #545 financial-era service_config cutover.
--
-- The preflight accepts exactly the pre-Start 17-key economic contract plus the optional
-- incident-release row. The transaction then retires only the two superseded economics keys.
\set ON_ERROR_STOP on

begin;

do $$
declare
  unknown_keys text[];
  missing_keys text[];
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
     'risk_halt_release_hash'
   ]::text[]);

  if unknown_keys is not null then
    raise exception 'unknown service_config keys: %', array_to_string(unknown_keys, ', ');
  end if;

  select array_agg(required.key order by required.key)
    into missing_keys
    from unnest(array[
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
      'per_trade_cap',
      'slippage_rate',
      'sizing_mode',
      'sizing_dollar_usd',
      'sizing_contracts'
    ]::text[]) as required(key)
   where not exists (select 1 from service_config where service_config.key = required.key);

  if missing_keys is not null then
    raise exception 'missing service_config keys: %', array_to_string(missing_keys, ', ');
  end if;
end;
$$;

delete from service_config
 where key = any (array[
   'fill_mode',
   'polymarket_fee_rate'
 ]::text[]);

commit;
