-- Supabase schema for the copy-trade wallet-ranking handoff.
-- LOCAL ranker writes append-only batches (heavy compute, has the 359 GB wallet DB);
-- VPS reads the latest batch (light) and runs the absolute-loss demotion test.
-- `batch_id` is the EPOCH — append-only history future-proofs the "is a wholesale
-- top-25 swap at frequency X worthwhile" replay (project_copytrade_knockout_policy).
-- Idempotent: safe to re-run.

create table if not exists ranking_batches (
  batch_id           bigserial primary key,   -- the epoch
  created_at         timestamptz not null default now(),
  git_sha            text,
  config_hash        text,
  band_lo            numeric,                 -- entry-price band [lo, hi]
  band_hi            numeric,
  ttr_floor_secs     integer,                 -- min time-to-resolution (copyability floor)
  ttr_max_secs       integer,                 -- max TTR (72h)
  latency_shift_secs integer,                 -- Δ used for the latency-shifted fill
  universe_size      integer,
  notes              text
);

create table if not exists ranking_entries (
  batch_id    bigint  not null references ranking_batches(batch_id) on delete cascade,
  rank        integer not null,               -- 1..N (top-200 bench; top-25 = live)
  wallet_hex  text    not null,
  ls_edge     numeric,                         -- latency-shifted mean net return
  ls_tstat    numeric,                         -- latency-shifted net t-stat
  fill_rate   numeric,                         -- fraction of positions fillable at entry+Δ
  n_trades    integer,                         -- filled positions in the eval window
  hit_rate    numeric,
  avg_price   numeric,
  primary key (batch_id, rank)
);
create index if not exists idx_ranking_entries_wallet on ranking_entries (wallet_hex);

create table if not exists wallet_lifecycle_events (
  id              bigserial primary key,       -- VPS writes the demotion/promotion audit trail
  ts              timestamptz not null default now(),
  wallet_hex      text not null,
  event           text not null check (event in ('promote','demote')),
  reason          text,                         -- e.g. 'upper_cb_edge<0 & realized_pnl<0'
  live_pnl        numeric,
  trades_observed integer,
  from_batch_id   bigint references ranking_batches(batch_id)
);
create index if not exists idx_lifecycle_wallet on wallet_lifecycle_events (wallet_hex, ts);

-- Convenience read for the VPS: the current bench = entries of the most recent batch.
create or replace view latest_ranking as
  select e.* from ranking_entries e
  where e.batch_id = (select max(batch_id) from ranking_batches)
  order by e.rank;

-- ════════════════════════════════════════════════════════════════════════════
-- Paper-trade analytics sink (issue #343 PR2).
-- pe-service dual-writes paper fills + settlements here (best-effort; local
-- paper_state.db stays authoritative). The historical-vs-live site (PR3) reads
-- the `wallet_live_stats` view with the anon key, gated by the RLS below.
-- Idempotent: safe to re-run.
-- ════════════════════════════════════════════════════════════════════════════

create table if not exists paper_fills (
  idempotency_key text        primary key,    -- wf|{leader}|{src}|{mkt}|{outcome}|{side}|{bucket}
  leader_wallet   text        not null,        -- parsed from the idempotency key
  source_trade_id text,
  market_id       text        not null,
  outcome_id      integer     not null,
  side            text        not null,        -- 'buy' | 'sell'
  contracts       numeric     not null,
  fill_price      numeric     not null,
  entry_unix      bigint,                       -- leader trade observed_at (from the key)
  event_seq       bigint      not null,         -- event-log frame (sparse; sink HWM cursor)
  inserted_at     timestamptz not null default now()
);

create table if not exists settled_markets (
  market_id       text        primary key,
  outcome_prices  jsonb       not null,         -- array of resolved per-outcome prices
  credit_applied  numeric     not null,
  settled_at_unix bigint      not null,
  inserted_at     timestamptz not null default now()
);

-- Writer-only fill catch-up cursor: the contiguous-prefix `event_seq` confirmed in
-- Supabase. Single row (id = 1). No anon access (see RLS below).
create table if not exists supabase_sink_hwm (
  id             integer     primary key default 1 check (id = 1),
  last_event_seq bigint      not null default 0
);
insert into supabase_sink_hwm (id, last_event_seq) values (1, 0)
  on conflict (id) do nothing;

-- Service runtime telemetry: the size of pe-service's current live watchlist (the wallets
-- it actually copies = the maintained working set of `MAINTAINED_SET_SIZE` wallets, held by
-- a score-update-only refresh and the maintenance tick — issue #350 WS1). The count lives
-- only in service memory, so the analytics site cannot derive it from latest_ranking (it
-- does not know the limit) — pe-service publishes it here every refresh. Single row
-- (id = 1). Anon-readable (see RLS below).
create table if not exists service_runtime (
  id             integer     primary key default 1 check (id = 1),
  watchlist_size integer     not null default 0,
  updated_at     timestamptz not null default now()
);
insert into service_runtime (id, watchlist_size) values (1, 0)
  on conflict (id) do nothing;

-- Per-wallet historical (ranker) vs live (paper) stats. `security_invoker = true` so the
-- view executes with the *querying* role's privileges and the anon RLS below applies.
-- Live realized P&L mirrors `crates/paper-pnl` `value_fill`: for a settled fill,
-- realized = side_sign × (resolved_outcome_price − fill_price) × contracts, won iff > 0.
-- FULL OUTER join on lower(wallet) so admitted-but-not-yet-traded wallets (ranker only)
-- and aged-out live wallets (fills only, no longer in latest_ranking) both appear.
create or replace view wallet_live_stats
  with (security_invoker = true) as
  with fill_stats as (
    select
      lower(f.leader_wallet)                                          as wallet,
      count(*)                                                        as live_total_fills,
      count(*) filter (where s.market_id is not null)                 as live_settled_count,
      count(*) filter (where s.market_id is null)                     as live_open_fills,
      count(*) filter (
        where s.market_id is not null
          and (case f.side when 'buy' then 1 else -1 end)
            * (coalesce((s.outcome_prices ->> f.outcome_id)::numeric, 0) - f.fill_price)
            * f.contracts > 0
      )                                                               as live_wins,
      coalesce(sum(
        case when s.market_id is not null then
          (case f.side when 'buy' then 1 else -1 end)
            * (coalesce((s.outcome_prices ->> f.outcome_id)::numeric, 0) - f.fill_price)
            * f.contracts
        else 0 end
      ), 0)                                                           as live_realized_pnl
    from paper_fills f
    left join settled_markets s on s.market_id = f.market_id
    group by lower(f.leader_wallet)
  )
  select
    coalesce(fs.wallet, lower(r.wallet_hex))                          as wallet,
    -- live (paper) stats
    coalesce(fs.live_total_fills, 0)                                  as live_total_fills,
    coalesce(fs.live_settled_count, 0)                                as live_settled_count,
    coalesce(fs.live_open_fills, 0)                                   as live_open_fills,
    coalesce(fs.live_wins, 0)                                         as live_wins,
    case when coalesce(fs.live_settled_count, 0) > 0
         then fs.live_wins::numeric / fs.live_settled_count end       as live_win_rate,
    coalesce(fs.live_realized_pnl, 0)                                 as live_realized_pnl,
    case when coalesce(fs.live_settled_count, 0) > 0
         then fs.live_realized_pnl / fs.live_settled_count end        as live_edge,
    -- historical (ranker) stats from the current batch
    r.rank, r.ls_edge, r.ls_tstat, r.fill_rate, r.n_trades, r.hit_rate, r.avg_price
  from fill_stats fs
  full outer join latest_ranking r on fs.wallet = lower(r.wallet_hex);

-- ── Row-level security ───────────────────────────────────────────────────────
-- The site reads with the anon/publishable key (subject to RLS); pe-service writes
-- with the service-role secret key (bypasses RLS). Net-new: the schema had no RLS.
-- `drop policy if exists` keeps the create idempotent (Postgres has no
-- `create policy if not exists`).
alter table paper_fills enable row level security;
drop policy if exists "paper_fills_anon_read" on paper_fills;
create policy "paper_fills_anon_read" on paper_fills for select to anon using (true);
grant select on paper_fills to anon;

alter table settled_markets enable row level security;
drop policy if exists "settled_markets_anon_read" on settled_markets;
create policy "settled_markets_anon_read" on settled_markets for select to anon using (true);
grant select on settled_markets to anon;

-- The site is the first anon reader of ranking_entries. The anon read policy preserves
-- the existing service-role readers (secret key bypasses RLS) and the latest_ranking
-- view (owner-rights, RLS-bypassing); no anon write policy is created.
alter table ranking_entries enable row level security;
drop policy if exists "ranking_entries_anon_read" on ranking_entries;
create policy "ranking_entries_anon_read" on ranking_entries for select to anon using (true);
grant select on ranking_entries to anon;

-- Writer-only cursor: RLS enabled with NO anon policy and NO grant — anon cannot touch it.
alter table supabase_sink_hwm enable row level security;

-- The site reads the live watchlist size for the "N watched" KPI; pe-service writes it with
-- the service-role key (bypasses RLS). Anon read-only; no anon write policy.
alter table service_runtime enable row level security;
drop policy if exists "service_runtime_anon_read" on service_runtime;
create policy "service_runtime_anon_read" on service_runtime for select to anon using (true);
grant select on service_runtime to anon;

-- Views are not RLS-bearing; the anon role still needs an explicit grant to read them.
grant select on latest_ranking to anon;
grant select on wallet_live_stats to anon;
