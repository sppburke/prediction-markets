-- Supabase schema for the AUTHORITATIVE paper-state system of record (issue #397).
--
-- Makes Supabase the source of truth for "the money and the book": the new
-- `paper_bankroll` + `paper_positions` tables plus the already-present `paper_fills`
-- + `settled_markets` (created in scripts/supabase_schema.sql). Local SQLite is demoted
-- to a write-through read cache; `seen_trades`/`leader_positions`/`poll_cursors`/`meta`
-- stay LOCAL-only (no cross-host consumer). pe-service writes through two RPCs when
-- PE_SUPABASE_AUTHORITATIVE=1; the site reads the two anon-readable tables.
--
-- Prerequisite: scripts/supabase_schema.sql (defines paper_fills + settled_markets, which
-- the RPCs below write). Idempotent: safe to re-run.

-- ════════════════════════════════════════════════════════════════════════════
-- Tables
-- ════════════════════════════════════════════════════════════════════════════

-- Single-row authoritative bankroll. Decimal stored as TEXT for exactness (mirrors the
-- local SQLite `bankroll` table). NOT seeded here on purpose: an authoritative deploy
-- that skipped `--backfill-supabase` then has no row, so `commit_fill`/`apply_resolution`
-- RAISE (fail-closed) and the boot pull is a no-op — far safer than seeding a 0 that the
-- boot pull would mirror over the real local balance. `--backfill-supabase` inserts it.
create table if not exists paper_bankroll (
  id           integer     primary key default 0 check (id = 0),
  bankroll_str text        not null,
  updated_at   timestamptz not null default now()
);

-- Our own net paper positions per (market, outcome). PK matches the local `positions`
-- natural key. CUMULATIVE by design (#516): settlement inserts `settled_markets` and
-- credits the bankroll but never zeroes/deletes rows here — `apply_resolution_v2`
-- computes its credit FROM these rows at settlement time. "Open" is always derived as
-- net-nonzero AND market not settled. `bigint` contracts (the local column is INTEGER; values are small).
create table if not exists paper_positions (
  market_id       text        not null,
  outcome_id      integer     not null,
  long_contracts  bigint      not null,
  short_contracts bigint      not null,
  updated_at      timestamptz not null default now(),
  primary key (market_id, outcome_id)
);

-- ════════════════════════════════════════════════════════════════════════════
-- RPCs — the authoritative write paths (pe-service calls these with the service-role
-- key, which bypasses RLS). Each is one transaction. Both mutate `paper_bankroll` via a
-- single SELF-REFERENCING UPDATE (never read-into-variable-then-write) so a concurrent
-- commit_fill (main task) and apply_resolution (resolution task) cannot lose each other's
-- update under READ COMMITTED — only a row lock prevents the lost-update anomaly (#397 B-E).
-- ════════════════════════════════════════════════════════════════════════════

-- Atomic fill commit: dedup-insert into paper_fills, then — ONLY when that row is new —
-- net the position and debit/credit the bankroll. Mirrors paper-state `commit_fill`
-- (apply_fill_to_net + tx_apply_bankroll's max(0) clamp + the `if inserted` gate).
-- Returns the resulting bankroll (text). RAISEs if the bankroll singleton is absent.
create or replace function commit_fill(
  p_idempotency_key text,
  p_leader_wallet   text,
  p_source_trade_id text,
  p_market_id       text,
  p_outcome_id      integer,
  p_side            text,
  p_contracts       bigint,
  p_fill_price      text,
  p_entry_unix      bigint,
  p_event_seq       bigint
) returns text
language plpgsql
as $$
declare
  v_inserted bigint := 0;
  v_long     bigint := 0;
  v_short    bigint := 0;
  v_covered  bigint;
  v_trimmed  bigint;
  v_notional numeric;
  v_bankroll text;
begin
  -- Output dedup + gate: a duplicate idempotency_key no-ops here and skips the apply.
  insert into paper_fills
    (idempotency_key, leader_wallet, source_trade_id, market_id, outcome_id,
     side, contracts, fill_price, entry_unix, event_seq)
  values
    (p_idempotency_key, p_leader_wallet, p_source_trade_id, p_market_id, p_outcome_id,
     p_side, p_contracts, p_fill_price::numeric, p_entry_unix, p_event_seq)
  on conflict (idempotency_key) do nothing;
  get diagnostics v_inserted = row_count;

  if v_inserted > 0 then
    -- Net-position update (apply_fill_to_net): BUY covers shorts first then adds to long;
    -- SELL trims longs first then adds to short. Single-writer (commit_fill only), so a
    -- read-then-write is safe here — unlike the cross-task bankroll below.
    select long_contracts, short_contracts into v_long, v_short
      from paper_positions
     where market_id = p_market_id and outcome_id = p_outcome_id;
    if not found then
      v_long := 0; v_short := 0;
    end if;

    if p_side = 'buy' then
      v_covered := least(v_short, p_contracts);
      v_long := v_long + (p_contracts - v_covered);
      v_short := v_short - v_covered;
    else
      v_trimmed := least(v_long, p_contracts);
      v_long := v_long - v_trimmed;
      v_short := v_short + (p_contracts - v_trimmed);
    end if;

    insert into paper_positions (market_id, outcome_id, long_contracts, short_contracts)
    values (p_market_id, p_outcome_id, v_long, v_short)
    on conflict (market_id, outcome_id) do update
      set long_contracts  = excluded.long_contracts,
          short_contracts = excluded.short_contracts,
          updated_at      = now();

    -- Bankroll: BUY debits price×contracts clamped at 0; SELL credits it. Self-referencing
    -- UPDATE → row lock → no lost update vs a concurrent apply_resolution credit (#397 B-E).
    v_notional := p_fill_price::numeric * p_contracts;
    if p_side = 'buy' then
      update paper_bankroll
         set bankroll_str = round(greatest(bankroll_str::numeric - v_notional, 0), 18)::text,
             updated_at   = now()
       where id = 0;
    else
      update paper_bankroll
         set bankroll_str = round(bankroll_str::numeric + v_notional, 18)::text,
             updated_at   = now()
       where id = 0;
    end if;
  end if;

  select bankroll_str into v_bankroll from paper_bankroll where id = 0;
  if v_bankroll is null then
    raise exception
      'paper_bankroll singleton missing; run --backfill-supabase before enabling PE_SUPABASE_AUTHORITATIVE';
  end if;
  return v_bankroll;
end;
$$;

-- Atomic resolution: guard-insert into settled_markets, then — ONLY when that row is new —
-- credit the bankroll. The insert-gate makes a reload-then-retry credit ZERO (#397 B-D),
-- and the credit is a self-referencing UPDATE (#397 B-E). Always returns the current
-- bankroll, never NULL, on the already-settled branch (#397 C5).
create or replace function apply_resolution(
  p_market_id       text,
  p_outcome_prices  jsonb,
  p_credit          text,
  p_settled_at_unix bigint
) returns text
language plpgsql
as $$
declare
  v_inserted bigint := 0;
  v_bankroll text;
begin
  insert into settled_markets (market_id, outcome_prices, credit_applied, settled_at_unix)
  values (p_market_id, p_outcome_prices, p_credit::numeric, p_settled_at_unix)
  on conflict (market_id) do nothing;
  get diagnostics v_inserted = row_count;

  if v_inserted > 0 then
    update paper_bankroll
       set bankroll_str = round(bankroll_str::numeric + p_credit::numeric, 18)::text,
           updated_at   = now()
     where id = 0;
  end if;

  select bankroll_str into v_bankroll from paper_bankroll where id = 0;
  if v_bankroll is null then
    raise exception
      'paper_bankroll singleton missing; run --backfill-supabase before enabling PE_SUPABASE_AUTHORITATIVE';
  end if;
  return v_bankroll;
end;
$$;

-- ════════════════════════════════════════════════════════════════════════════
-- #511 v2 RPCs — the authority owns fill admission and resolution credit.
--
-- Serialization: BOTH functions lock the paper_bankroll singleton FIRST (the global
-- money mutex; ~100 fills/wk makes this free). Locking last only serializes the
-- arithmetic, not admission: under READ COMMITTED a fill's settled-check and a
-- resolution's position-read could each run before the other's commit. Lock-first
-- makes the two strictly serial: fill-first ⇒ its position is visible to the
-- resolution's in-function read (credited); resolution-first ⇒ the settled row is
-- visible to the fill's check (refused).
--
-- v1 (`commit_fill`, `apply_resolution`) is RETAINED for the binary-rollout window and
-- as the rollback path; revoke it in a later cycle once the #511 binary has soaked.
-- ════════════════════════════════════════════════════════════════════════════

-- Atomic fill commit v2. Order matters (#511 R3): an existing idempotency_key returns
-- its CANONICAL row even if the market has since settled (the earlier attempt landed;
-- the caller must converge on it); only an absent key can be refused as settled.
-- Returns jsonb: {outcome: 'applied'|'existing'|'settled', bankroll: text, row: {...}|null}
-- (money as decimal strings; `row` carries the canonical paper_fills fields).
create or replace function commit_fill_v2(
  p_idempotency_key text,
  p_leader_wallet   text,
  p_source_trade_id text,
  p_market_id       text,
  p_outcome_id      integer,
  p_side            text,
  p_contracts       bigint,
  p_fill_price      text,
  p_entry_unix      bigint,
  p_event_seq       bigint
) returns jsonb
language plpgsql
as $$
declare
  v_bankroll text;
  v_row      paper_fills%rowtype;
begin
  -- Global money mutex (see header). RAISEs if the singleton is absent.
  select bankroll_str into v_bankroll from paper_bankroll where id = 0 for update;
  if v_bankroll is null then
    raise exception
      'paper_bankroll singleton missing; run --backfill-supabase before enabling PE_SUPABASE_AUTHORITATIVE';
  end if;

  select * into v_row from paper_fills where idempotency_key = p_idempotency_key;
  if found then
    return jsonb_build_object(
      'outcome', 'existing',
      'bankroll', v_bankroll,
      'row', jsonb_build_object(
        'idempotency_key', v_row.idempotency_key,
        'leader_wallet',   v_row.leader_wallet,
        'source_trade_id', v_row.source_trade_id,
        'market_id',       v_row.market_id,
        'outcome_id',      v_row.outcome_id,
        'side',            v_row.side,
        'contracts',       v_row.contracts,
        'fill_price',      v_row.fill_price::text,
        'entry_unix',      v_row.entry_unix,
        'event_seq',       v_row.event_seq));
  end if;

  if exists (select 1 from settled_markets where market_id = p_market_id) then
    return jsonb_build_object('outcome', 'settled', 'bankroll', v_bankroll, 'row', null);
  end if;

  -- v1 arithmetic verbatim (insert cannot conflict: the key was absent under the lock).
  perform commit_fill(
    p_idempotency_key, p_leader_wallet, p_source_trade_id, p_market_id, p_outcome_id,
    p_side, p_contracts, p_fill_price, p_entry_unix, p_event_seq);

  select bankroll_str into v_bankroll from paper_bankroll where id = 0;
  select * into v_row from paper_fills where idempotency_key = p_idempotency_key;
  return jsonb_build_object(
    'outcome', 'applied',
    'bankroll', v_bankroll,
    'row', jsonb_build_object(
      'idempotency_key', v_row.idempotency_key,
      'leader_wallet',   v_row.leader_wallet,
      'source_trade_id', v_row.source_trade_id,
      'market_id',       v_row.market_id,
      'outcome_id',      v_row.outcome_id,
      'side',            v_row.side,
      'contracts',       v_row.contracts,
      'fill_price',      v_row.fill_price::text,
      'entry_unix',      v_row.entry_unix,
      'event_seq',       v_row.event_seq));
end;
$$;

-- Atomic resolution v2 (#511): credit computed INSIDE the function from paper_positions
-- under the bankroll lock — the client no longer computes it from a pre-RPC snapshot
-- (the read-then-resolve TOCTOU). Credit = greatest(Σ (long−short)×price[outcome], 0),
-- the exact PnlLedger::resolution_credit semantics (missing index ⇒ 0). Returns jsonb
-- {outcome: 'applied'|'existing', credit: text, outcome_prices: jsonb,
--  settled_at_unix: bigint, bankroll: text} — on 'existing', the CANONICAL recorded
-- values, so a crash-then-retry client mirrors idempotently with no double credit.
create or replace function apply_resolution_v2(
  p_market_id       text,
  p_outcome_prices  jsonb,
  p_settled_at_unix bigint
) returns jsonb
language plpgsql
as $$
declare
  v_bankroll text;
  v_credit   numeric;
  v_settled  settled_markets%rowtype;
begin
  select bankroll_str into v_bankroll from paper_bankroll where id = 0 for update;
  if v_bankroll is null then
    raise exception
      'paper_bankroll singleton missing; run --backfill-supabase before enabling PE_SUPABASE_AUTHORITATIVE';
  end if;

  select * into v_settled from settled_markets where market_id = p_market_id;
  if found then
    return jsonb_build_object(
      'outcome', 'existing',
      'credit', v_settled.credit_applied::text,
      'outcome_prices', v_settled.outcome_prices,
      'settled_at_unix', v_settled.settled_at_unix,
      'bankroll', v_bankroll);
  end if;

  select coalesce(greatest(sum(
           (p.long_contracts - p.short_contracts)
           * coalesce((p_outcome_prices ->> p.outcome_id)::numeric, 0)
         ), 0), 0)
    into v_credit
    from paper_positions p
   where p.market_id = p_market_id;

  insert into settled_markets (market_id, outcome_prices, credit_applied, settled_at_unix)
  values (p_market_id, p_outcome_prices, v_credit, p_settled_at_unix);

  update paper_bankroll
     set bankroll_str = (bankroll_str::numeric + v_credit)::text,
         updated_at   = now()
   where id = 0;
  select bankroll_str into v_bankroll from paper_bankroll where id = 0;

  return jsonb_build_object(
    'outcome', 'applied',
    'credit', v_credit::text,
    'outcome_prices', p_outcome_prices,
    'settled_at_unix', p_settled_at_unix,
    'bankroll', v_bankroll);
end;
$$;

-- ════════════════════════════════════════════════════════════════════════════
-- Row-level security & grants
-- The site reads paper_bankroll + paper_positions with the anon key (RLS below). The
-- two RPCs move money, so they are writer-only: revoke execute from anon/authenticated.
-- pe-service uses the service-role secret (bypasses RLS, retains execute).
-- `drop policy if exists` keeps create idempotent (Postgres has no create-if-not-exists).
-- ════════════════════════════════════════════════════════════════════════════

alter table paper_bankroll enable row level security;
drop policy if exists "paper_bankroll_anon_read" on paper_bankroll;
create policy "paper_bankroll_anon_read" on paper_bankroll for select to anon using (true);
grant select on paper_bankroll to anon;

alter table paper_positions enable row level security;
drop policy if exists "paper_positions_anon_read" on paper_positions;
create policy "paper_positions_anon_read" on paper_positions for select to anon using (true);
grant select on paper_positions to anon;

revoke all on function commit_fill(
  text, text, text, text, integer, text, bigint, text, bigint, bigint
) from public, anon, authenticated;
grant execute on function commit_fill(
  text, text, text, text, integer, text, bigint, text, bigint, bigint
) to service_role;

revoke all on function apply_resolution(text, jsonb, text, bigint) from public, anon, authenticated;
grant execute on function apply_resolution(text, jsonb, text, bigint) to service_role;

revoke all on function commit_fill_v2(
  text, text, text, text, integer, text, bigint, text, bigint, bigint
) from public, anon, authenticated;
grant execute on function commit_fill_v2(
  text, text, text, text, integer, text, bigint, text, bigint, bigint
) to service_role;

revoke all on function apply_resolution_v2(text, jsonb, bigint) from public, anon, authenticated;
grant execute on function apply_resolution_v2(text, jsonb, bigint) to service_role;

-- PostgREST discovers new RPCs only after a schema reload.
notify pgrst, 'reload schema';
