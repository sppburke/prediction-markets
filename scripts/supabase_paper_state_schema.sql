-- Supabase schema for the authoritative paper financial protocol (#545).
--
-- Prerequisite: scripts/supabase_schema.sql creates paper_fills and settled_markets.
-- This guarded, post-QualificationStarted migration deliberately removes every older
-- money-moving RPC signature. A Prepared receipt is the sole mutation identity and
-- paper_bankroll is the single serialization lock.

create table if not exists paper_bankroll (
  id                integer     primary key default 0 check (id = 0),
  bankroll_str      text        not null,
  start_seq         bigint,
  start_hash        text,
  last_prepared_seq bigint,
  updated_at        timestamptz not null default now(),
  constraint paper_bankroll_financial_start_pair
    check ((start_seq is null) = (start_hash is null))
);
alter table paper_bankroll add column if not exists start_seq bigint;
alter table paper_bankroll add column if not exists start_hash text;
alter table paper_bankroll add column if not exists last_prepared_seq bigint;
do $constraints$
begin
  if not exists (
    select 1 from pg_constraint
     where conrelid = 'paper_bankroll'::regclass
       and conname = 'paper_bankroll_financial_start_pair'
  ) then
    alter table paper_bankroll
      add constraint paper_bankroll_financial_start_pair
      check ((start_seq is null) = (start_hash is null));
  end if;
end;
$constraints$;

create table if not exists paper_positions (
  market_id       text        not null,
  outcome_id      integer     not null,
  long_contracts  numeric     not null,
  short_contracts numeric     not null,
  updated_at      timestamptz not null default now(),
  primary key (market_id, outcome_id)
);
alter table paper_positions
  alter column long_contracts type numeric using long_contracts::numeric,
  alter column short_contracts type numeric using short_contracts::numeric;

alter table paper_fills add column if not exists principal numeric;
alter table paper_fills add column if not exists fee numeric;
alter table paper_fills add column if not exists prepared_seq bigint;
alter table settled_markets add column if not exists prepared_seq bigint;

-- Remove all bypasses before the new functions become callable. REVOKE precedes DROP
-- so a failed migration cannot leave an old signature exposed.
do $migration$
begin
  if to_regprocedure(
    'commit_fill(text,text,text,text,integer,text,bigint,text,bigint,bigint)'
  ) is not null then
    revoke all on function commit_fill(
      text, text, text, text, integer, text, bigint, text, bigint, bigint
    ) from public, anon, authenticated, service_role;
    drop function commit_fill(
      text, text, text, text, integer, text, bigint, text, bigint, bigint
    );
  end if;
  if to_regprocedure('apply_resolution(text,jsonb,text,bigint)') is not null then
    revoke all on function apply_resolution(text, jsonb, text, bigint)
      from public, anon, authenticated, service_role;
    drop function apply_resolution(text, jsonb, text, bigint);
  end if;
  if to_regprocedure(
    'commit_fill_v2(text,text,text,text,integer,text,bigint,text,bigint,bigint)'
  ) is not null then
    revoke all on function commit_fill_v2(
      text, text, text, text, integer, text, bigint, text, bigint, bigint
    ) from public, anon, authenticated, service_role;
    drop function commit_fill_v2(
      text, text, text, text, integer, text, bigint, text, bigint, bigint
    );
  end if;
  if to_regprocedure('apply_resolution_v2(text,jsonb,bigint)') is not null then
    revoke all on function apply_resolution_v2(text, jsonb, bigint)
      from public, anon, authenticated, service_role;
    drop function apply_resolution_v2(text, jsonb, bigint);
  end if;
end;
$migration$;

-- Bind the already-created bankroll singleton to the synchronized Start receipt.
-- Equal retries are idempotent; a distinct Start is a typed conflict.
create or replace function seed_financial_start(
  p_start_seq bigint,
  p_start_hash text
) returns jsonb
language plpgsql
as $$
declare
  v_bankroll paper_bankroll%rowtype;
begin
  select * into v_bankroll from paper_bankroll where id = 0 for update;
  if not found then
    raise exception 'paper_bankroll singleton missing before financial Start';
  end if;
  if p_start_seq is null or p_start_seq < 0 or p_start_hash is null
     or p_start_hash !~ '^[0-9a-f]{64}$' then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'invalid_financial_start',
      'start_seq', v_bankroll.start_seq, 'start_hash', v_bankroll.start_hash);
  end if;

  if v_bankroll.start_seq is null and v_bankroll.start_hash is null then
    update paper_bankroll
       set start_seq = p_start_seq,
           start_hash = p_start_hash,
           last_prepared_seq = null,
           updated_at = now()
     where id = 0;
    return jsonb_build_object(
      'outcome', 'applied', 'start_seq', p_start_seq, 'start_hash', p_start_hash);
  end if;

  if v_bankroll.start_seq = p_start_seq and v_bankroll.start_hash = p_start_hash then
    return jsonb_build_object(
      'outcome', 'existing', 'start_seq', v_bankroll.start_seq,
      'start_hash', v_bankroll.start_hash);
  end if;

  return jsonb_build_object(
    'outcome', 'conflict', 'conflict_reason', 'financial_start_mismatch',
    'start_seq', v_bankroll.start_seq, 'start_hash', v_bankroll.start_hash);
end;
$$;

-- Exactly one Start-bound paper fill transition. The Prepared frame sequence is both
-- the paper_fills.event_seq audit pointer and the monotonic financial version.
create or replace function commit_fill_v2(
  p_start_seq bigint,
  p_start_hash text,
  p_expected_prior_seq bigint,
  p_prepared_seq bigint,
  p_idempotency_key text,
  p_leader_wallet text,
  p_source_trade_id text,
  p_market_id text,
  p_outcome_id integer,
  p_side text,
  p_quantity numeric,
  p_fill_price numeric,
  p_principal numeric,
  p_fee numeric,
  p_entry_unix bigint
) returns jsonb
language plpgsql
as $$
declare
  v_bankroll       paper_bankroll%rowtype;
  v_fill           paper_fills%rowtype;
  v_long           numeric := 0;
  v_short          numeric := 0;
  v_covered        numeric;
  v_trimmed        numeric;
  v_new_bankroll   numeric;
begin
  select * into v_bankroll from paper_bankroll where id = 0 for update;
  if not found then
    raise exception 'paper_bankroll singleton missing before financial fill';
  end if;

  if v_bankroll.start_seq is distinct from p_start_seq
     or v_bankroll.start_hash is distinct from p_start_hash then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'financial_start_mismatch',
      'bankroll', v_bankroll.bankroll_str, 'applied_prepared_seq', null, 'row', null);
  end if;
  if p_prepared_seq is null or p_prepared_seq <= p_start_seq
     or (p_expected_prior_seq is not null and p_prepared_seq <= p_expected_prior_seq) then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'invalid_prepared_sequence',
      'bankroll', v_bankroll.bankroll_str,
      'applied_prepared_seq', v_bankroll.last_prepared_seq, 'row', null);
  end if;

  select * into v_fill from paper_fills where idempotency_key = p_idempotency_key;
  if found then
    if v_fill.leader_wallet is distinct from p_leader_wallet
       or v_fill.source_trade_id is distinct from p_source_trade_id
       or v_fill.market_id is distinct from p_market_id
       or v_fill.outcome_id is distinct from p_outcome_id
       or v_fill.side is distinct from p_side
       or v_fill.contracts is distinct from p_quantity
       or v_fill.fill_price is distinct from p_fill_price
       or v_fill.principal is distinct from p_principal
       or v_fill.fee is distinct from p_fee
       or v_fill.entry_unix is distinct from p_entry_unix
       or v_fill.event_seq is distinct from p_prepared_seq
       or v_fill.prepared_seq is distinct from p_prepared_seq
       or v_bankroll.last_prepared_seq is distinct from p_prepared_seq then
      return jsonb_build_object(
        'outcome', 'conflict', 'conflict_reason', 'fill_retry_mismatch',
        'bankroll', v_bankroll.bankroll_str,
        'applied_prepared_seq', v_fill.prepared_seq, 'row', null);
    end if;

    return jsonb_build_object(
      'outcome', 'existing', 'conflict_reason', null,
      'bankroll', v_bankroll.bankroll_str,
      'applied_prepared_seq', v_fill.prepared_seq,
      'row', jsonb_build_object(
        'idempotency_key', v_fill.idempotency_key,
        'leader_wallet', v_fill.leader_wallet,
        'source_trade_id', v_fill.source_trade_id,
        'market_id', v_fill.market_id,
        'outcome_id', v_fill.outcome_id,
        'side', v_fill.side,
        'quantity', v_fill.contracts::text,
        'fill_price', v_fill.fill_price::text,
        'principal', v_fill.principal::text,
        'fee', v_fill.fee::text,
        'entry_unix', v_fill.entry_unix,
        'prepared_seq', v_fill.prepared_seq));
  end if;

  if v_bankroll.last_prepared_seq is distinct from p_expected_prior_seq then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'prepared_predecessor_mismatch',
      'bankroll', v_bankroll.bankroll_str,
      'applied_prepared_seq', v_bankroll.last_prepared_seq, 'row', null);
  end if;
  if exists (select 1 from settled_markets where market_id = p_market_id) then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'market_already_settled',
      'bankroll', v_bankroll.bankroll_str,
      'applied_prepared_seq', v_bankroll.last_prepared_seq, 'row', null);
  end if;
  if p_side not in ('buy', 'sell') or p_quantity <= 0 or p_principal < 0 or p_fee < 0
     or p_fill_price <= 0 or p_fill_price >= 1 then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'invalid_fill_economics',
      'bankroll', v_bankroll.bankroll_str,
      'applied_prepared_seq', v_bankroll.last_prepared_seq, 'row', null);
  end if;
  if v_bankroll.bankroll_str::numeric < p_principal + p_fee then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'insufficient_bankroll',
      'bankroll', v_bankroll.bankroll_str,
      'applied_prepared_seq', v_bankroll.last_prepared_seq, 'row', null);
  end if;

  insert into paper_fills
    (idempotency_key, leader_wallet, source_trade_id, market_id, outcome_id, side,
     contracts, fill_price, principal, fee, entry_unix, event_seq, prepared_seq)
  values
    (p_idempotency_key, p_leader_wallet, p_source_trade_id, p_market_id, p_outcome_id,
     p_side, p_quantity, p_fill_price, p_principal, p_fee, p_entry_unix,
     p_prepared_seq, p_prepared_seq);

  select long_contracts, short_contracts into v_long, v_short
    from paper_positions
   where market_id = p_market_id and outcome_id = p_outcome_id;
  if not found then
    v_long := 0;
    v_short := 0;
  end if;
  if p_side = 'buy' then
    v_covered := least(v_short, p_quantity);
    v_long := v_long + (p_quantity - v_covered);
    v_short := v_short - v_covered;
  else
    v_trimmed := least(v_long, p_quantity);
    v_long := v_long - v_trimmed;
    v_short := v_short + (p_quantity - v_trimmed);
  end if;
  insert into paper_positions (market_id, outcome_id, long_contracts, short_contracts)
  values (p_market_id, p_outcome_id, v_long, v_short)
  on conflict (market_id, outcome_id) do update
    set long_contracts = excluded.long_contracts,
        short_contracts = excluded.short_contracts,
        updated_at = now();

  v_new_bankroll := v_bankroll.bankroll_str::numeric - p_principal - p_fee;
  update paper_bankroll
     set bankroll_str = v_new_bankroll::text,
         last_prepared_seq = p_prepared_seq,
         updated_at = now()
   where id = 0;

  return jsonb_build_object(
    'outcome', 'applied', 'conflict_reason', null,
    'bankroll', v_new_bankroll::text,
    'applied_prepared_seq', p_prepared_seq,
    'row', jsonb_build_object(
      'idempotency_key', p_idempotency_key,
      'leader_wallet', p_leader_wallet,
      'source_trade_id', p_source_trade_id,
      'market_id', p_market_id,
      'outcome_id', p_outcome_id,
      'side', p_side,
      'quantity', p_quantity::text,
      'fill_price', p_fill_price::text,
      'principal', p_principal::text,
      'fee', p_fee::text,
      'entry_unix', p_entry_unix,
      'prepared_seq', p_prepared_seq));
end;
$$;

-- Exactly one Start-bound resolution transition. Credit is computed inside the same
-- lock as fills. Shares are first aggregated per outcome, then each outcome payout is
-- floored once to the six-decimal collateral quantum, then the outcomes are summed.
create or replace function apply_resolution_v2(
  p_start_seq bigint,
  p_start_hash text,
  p_expected_prior_seq bigint,
  p_prepared_seq bigint,
  p_condition_id text,
  p_payout_by_outcome_index jsonb,
  p_settled_at_unix bigint
) returns jsonb
language plpgsql
as $$
declare
  v_bankroll     paper_bankroll%rowtype;
  v_settled      settled_markets%rowtype;
  v_credit       numeric := 0;
  v_new_bankroll numeric;
begin
  select * into v_bankroll from paper_bankroll where id = 0 for update;
  if not found then
    raise exception 'paper_bankroll singleton missing before financial resolution';
  end if;
  if v_bankroll.start_seq is distinct from p_start_seq
     or v_bankroll.start_hash is distinct from p_start_hash then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'financial_start_mismatch',
      'credit', null, 'applied_prepared_seq', null,
      'bankroll', v_bankroll.bankroll_str);
  end if;
  if p_prepared_seq is null or p_prepared_seq <= p_start_seq
     or (p_expected_prior_seq is not null and p_prepared_seq <= p_expected_prior_seq) then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'invalid_prepared_sequence',
      'credit', null, 'applied_prepared_seq', v_bankroll.last_prepared_seq,
      'bankroll', v_bankroll.bankroll_str);
  end if;

  select * into v_settled from settled_markets where market_id = p_condition_id;
  if found then
    if v_settled.outcome_prices is distinct from p_payout_by_outcome_index
       or v_settled.settled_at_unix is distinct from p_settled_at_unix
       or v_settled.prepared_seq is distinct from p_prepared_seq
       or v_bankroll.last_prepared_seq is distinct from p_prepared_seq then
      return jsonb_build_object(
        'outcome', 'conflict', 'conflict_reason', 'resolution_retry_mismatch',
        'credit', v_settled.credit_applied::text,
        'applied_prepared_seq', v_settled.prepared_seq,
        'bankroll', v_bankroll.bankroll_str);
    end if;
    return jsonb_build_object(
      'outcome', 'existing', 'conflict_reason', null,
      'credit', v_settled.credit_applied::text,
      'applied_prepared_seq', v_settled.prepared_seq,
      'bankroll', v_bankroll.bankroll_str);
  end if;

  if v_bankroll.last_prepared_seq is distinct from p_expected_prior_seq then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'prepared_predecessor_mismatch',
      'credit', null, 'applied_prepared_seq', v_bankroll.last_prepared_seq,
      'bankroll', v_bankroll.bankroll_str);
  end if;
  if jsonb_typeof(p_payout_by_outcome_index) is distinct from 'array' then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'invalid_payout_vector',
      'credit', null, 'applied_prepared_seq', v_bankroll.last_prepared_seq,
      'bankroll', v_bankroll.bankroll_str);
  end if;
  if jsonb_array_length(p_payout_by_outcome_index) <> 2
     or not (
       p_payout_by_outcome_index = '["1","0"]'::jsonb
       or p_payout_by_outcome_index = '["0","1"]'::jsonb
       or p_payout_by_outcome_index = '["0.5","0.5"]'::jsonb
     ) then
    return jsonb_build_object(
      'outcome', 'conflict', 'conflict_reason', 'invalid_payout_vector',
      'credit', null, 'applied_prepared_seq', v_bankroll.last_prepared_seq,
      'bankroll', v_bankroll.bankroll_str);
  end if;

  select coalesce(sum(outcome_credit), 0) into v_credit
    from (
      select floor(
               sum(greatest(p.long_contracts - p.short_contracts, 0))
               * (p_payout_by_outcome_index ->> p.outcome_id)::numeric
               * 1000000
             ) / 1000000 as outcome_credit
        from paper_positions p
       where p.market_id = p_condition_id
       group by p.outcome_id
    ) credits;

  insert into settled_markets
    (market_id, outcome_prices, credit_applied, settled_at_unix, prepared_seq)
  values
    (p_condition_id, p_payout_by_outcome_index, v_credit, p_settled_at_unix,
     p_prepared_seq);
  v_new_bankroll := v_bankroll.bankroll_str::numeric + v_credit;
  update paper_bankroll
     set bankroll_str = v_new_bankroll::text,
         last_prepared_seq = p_prepared_seq,
         updated_at = now()
   where id = 0;

  return jsonb_build_object(
    'outcome', 'applied', 'conflict_reason', null,
    'credit', v_credit::text,
    'applied_prepared_seq', p_prepared_seq,
    'bankroll', v_new_bankroll::text);
end;
$$;

-- The public stats view derives realized P&L from exact economics, not fill-price
-- notional. Existing pre-v3 rows remain readable through the legacy expression.
create or replace view wallet_live_stats
  with (security_invoker = true) as
  with fill_stats as (
    select
      lower(f.leader_wallet) as wallet,
      count(*) as live_total_fills,
      count(*) filter (where s.market_id is not null) as live_settled_count,
      count(*) filter (where s.market_id is null) as live_open_fills,
      count(*) filter (
        where s.market_id is not null
          and case
                when f.principal is not null and f.fee is not null then
                  coalesce((s.outcome_prices ->> f.outcome_id)::numeric, 0)
                    * f.contracts - f.principal - f.fee
                else
                  (case f.side when 'buy' then 1 else -1 end)
                    * (coalesce((s.outcome_prices ->> f.outcome_id)::numeric, 0)
                       - f.fill_price) * f.contracts
              end > 0
      ) as live_wins,
      coalesce(sum(
        case when s.market_id is not null then
          case
            when f.principal is not null and f.fee is not null then
              coalesce((s.outcome_prices ->> f.outcome_id)::numeric, 0)
                * f.contracts - f.principal - f.fee
            else
              (case f.side when 'buy' then 1 else -1 end)
                * (coalesce((s.outcome_prices ->> f.outcome_id)::numeric, 0)
                   - f.fill_price) * f.contracts
          end
        else 0 end
      ), 0) as live_realized_pnl
    from paper_fills f
    left join settled_markets s on s.market_id = f.market_id
    group by lower(f.leader_wallet)
  )
  select
    coalesce(fs.wallet, lower(r.wallet_hex)) as wallet,
    coalesce(fs.live_total_fills, 0) as live_total_fills,
    coalesce(fs.live_settled_count, 0) as live_settled_count,
    coalesce(fs.live_open_fills, 0) as live_open_fills,
    coalesce(fs.live_wins, 0) as live_wins,
    case when coalesce(fs.live_settled_count, 0) > 0
         then fs.live_wins::numeric / fs.live_settled_count end as live_win_rate,
    coalesce(fs.live_realized_pnl, 0) as live_realized_pnl,
    case when coalesce(fs.live_settled_count, 0) > 0
         then fs.live_realized_pnl / fs.live_settled_count end as live_edge,
    r.rank, r.ls_edge, r.ls_tstat, r.fill_rate, r.n_trades, r.hit_rate, r.avg_price,
    r.last_trade_unix
  from fill_stats fs
  full outer join latest_ranking r on fs.wallet = lower(r.wallet_hex);

alter table paper_bankroll enable row level security;
drop policy if exists "paper_bankroll_anon_read" on paper_bankroll;
create policy "paper_bankroll_anon_read" on paper_bankroll for select to anon using (true);
grant select on paper_bankroll to anon;

alter table paper_positions enable row level security;
drop policy if exists "paper_positions_anon_read" on paper_positions;
create policy "paper_positions_anon_read" on paper_positions for select to anon using (true);
grant select on paper_positions to anon;

revoke all on function seed_financial_start(bigint, text)
  from public, anon, authenticated;
grant execute on function seed_financial_start(bigint, text) to service_role;

revoke all on function commit_fill_v2(
  bigint, text, bigint, bigint, text, text, text, text, integer, text,
  numeric, numeric, numeric, numeric, bigint
) from public, anon, authenticated;
grant execute on function commit_fill_v2(
  bigint, text, bigint, bigint, text, text, text, text, integer, text,
  numeric, numeric, numeric, numeric, bigint
) to service_role;

revoke all on function apply_resolution_v2(
  bigint, text, bigint, bigint, text, jsonb, bigint
) from public, anon, authenticated;
grant execute on function apply_resolution_v2(
  bigint, text, bigint, bigint, text, jsonb, bigint
) to service_role;

notify pgrst, 'reload schema';
