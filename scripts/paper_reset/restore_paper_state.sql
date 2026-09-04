-- restore_paper_state.sql — restore exactly one activation-stamped paper era
-- Deliberate unconditional deletes run only after all five live tables are locked.

\if :{?activation_id}
\else
\warn 'activation_id psql variable is required'
\quit 2
\endif

begin;

lock table paper_fills in access exclusive mode;
lock table settled_markets in access exclusive mode;
lock table paper_positions in access exclusive mode;
lock table paper_bankroll in access exclusive mode;
lock table fill_market_snapshots in access exclusive mode;

delete from paper_fills;
delete from settled_markets;
delete from paper_positions;
delete from paper_bankroll;
delete from fill_market_snapshots;

insert into paper_fills (
  idempotency_key, leader_wallet, source_trade_id, market_id, outcome_id,
  side, contracts, fill_price, entry_unix, event_seq, inserted_at
)
select idempotency_key, leader_wallet, source_trade_id, market_id, outcome_id,
       side, contracts, fill_price, entry_unix, event_seq, inserted_at
  from paper_fills_archive where activation_id = :'activation_id';

insert into settled_markets (
  market_id, outcome_prices, credit_applied, settled_at_unix, inserted_at
)
select market_id, outcome_prices, credit_applied, settled_at_unix, inserted_at
  from settled_markets_archive where activation_id = :'activation_id';

insert into paper_positions (
  market_id, outcome_id, long_contracts, short_contracts, updated_at
)
select market_id, outcome_id, long_contracts, short_contracts, updated_at
  from paper_positions_archive where activation_id = :'activation_id';

insert into paper_bankroll (id, bankroll_str, updated_at)
select id, bankroll_str, updated_at
  from paper_bankroll_archive where activation_id = :'activation_id';

insert into fill_market_snapshots (
  idempotency_key, liquidity, volume, absorbable_usd_100bps,
  ask_levels_json, captured_at_unix, inserted_at
)
select idempotency_key, liquidity, volume, absorbable_usd_100bps,
       ask_levels_json, captured_at_unix, inserted_at
  from fill_market_snapshots_archive where activation_id = :'activation_id';

select set_config('pe.activation_id', :'activation_id', true);
do $$
declare activation text := current_setting('pe.activation_id');
begin
  if (select count(*) from paper_fills) <>
       (select count(*) from paper_fills_archive where activation_id = activation)
     or (select count(*) from settled_markets) <>
       (select count(*) from settled_markets_archive where activation_id = activation)
     or (select count(*) from paper_positions) <>
       (select count(*) from paper_positions_archive where activation_id = activation)
     or (select count(*) from paper_bankroll) <>
       (select count(*) from paper_bankroll_archive where activation_id = activation)
     or (select count(*) from fill_market_snapshots) <>
       (select count(*) from fill_market_snapshots_archive where activation_id = activation) then
    raise exception 'restored live counts do not match activation % archive counts', activation;
  end if;
  if (select count(*) from paper_bankroll) <> 1 then
    raise exception 'restored activation % must contain exactly one bankroll row', activation;
  end if;
end $$;

commit;
notify pgrst, 'reload schema';
