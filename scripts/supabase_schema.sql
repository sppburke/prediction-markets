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
-- Real last-trade clock (#357): push-time snapshot of MAX(timestamp_unix) per wallet.
-- Nullable + idempotent so an existing project gains the column with no rebuild; the VPS
-- seeds each admitted wallet's poll cursor (the inactivity clock) from this value.
alter table ranking_entries add column if not exists last_trade_unix bigint;

create table if not exists wallet_lifecycle_events (
  id              bigserial primary key,       -- VPS writes the demotion/promotion audit trail
  ts              timestamptz not null default now(),
  wallet_hex      text not null,
  event           text not null check (event in ('promote','demote')),
  reason          text,                         -- e.g. 'upper_cb_edge<0 & realized_pnl<0'
  live_pnl        numeric,
  trades_observed integer,
  from_batch_id   bigint references ranking_batches(batch_id) on delete set null
);
create index if not exists idx_lifecycle_wallet on wallet_lifecycle_events (wallet_hex, ts);
-- Demote audit records the wallet's real last trade at eviction time (#357); nullable + idempotent.
alter table wallet_lifecycle_events add column if not exists last_trade_unix bigint;
-- #411: keep `from_batch_id` ON DELETE SET NULL so the ranker push can prune old
-- `ranking_batches` (keep newest N) without RESTRICT-blocking on a referencing audit row.
-- It is provenance only (the wallet's admission batch); nulling it on prune preserves the
-- demote/promote audit row while letting the batch be reclaimed. The inline FK above covers
-- fresh DBs; this drop+add (idempotent, mirrors the `drop policy if exists` idiom below)
-- migrates an existing DB. Today every `from_batch_id` is null (the demote writer emits null),
-- so this is a no-op on current data and forward-proofs a future promote writer.
alter table wallet_lifecycle_events
  drop constraint if exists wallet_lifecycle_events_from_batch_id_fkey;
alter table wallet_lifecycle_events
  add constraint wallet_lifecycle_events_from_batch_id_fkey
  foreign key (from_batch_id) references ranking_batches(batch_id) on delete set null;

-- Convenience read for the VPS: the current bench = entries of the most recent batch.
-- `security_invoker = true` (Supabase lint 0010_security_definer_view): the view executes
-- with the *querying* role's privileges, not the owner's, so it cannot bypass RLS. Readers
-- therefore need read access to BOTH base tables it touches (`ranking_entries` and the
-- `ranking_batches` subquery) — see the RLS block below. Service-role readers (secret key)
-- bypass RLS regardless, so pe-service's refresh and the ranker push are unaffected.
create or replace view latest_ranking
  with (security_invoker = true) as
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

-- Liquidity-at-fill capture (issue #350 WS2 PR-H): one row per BUY fill carrying the Gamma
-- depth scalars plus the filled outcome's CLOB ask-side depth. `idempotency_key` mirrors
-- `paper_fills(idempotency_key)` but declares NO foreign key — the relationship is documentary
-- (the canonical SQLite sidecar likewise has no FK; `paper_fills` is append-only). A `/book`
-- failure yields a partial row: `absorbable_usd_100bps`/`ask_levels_json` are null.
create table if not exists fill_market_snapshots (
  idempotency_key       text        primary key,
  liquidity             numeric,                  -- Gamma order-book depth (USD), nullable
  volume                numeric,                  -- Gamma cumulative volume (USD), nullable
  absorbable_usd_100bps numeric,                  -- Σ price·size within 100 bps of best ask
  ask_levels_json       jsonb,                    -- raw ask levels [{price,size}], nullable
  captured_at_unix      bigint      not null,
  inserted_at           timestamptz not null default now()
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

-- ── Operator runtime config (issue #398 WS1) ─────────────────────────────────
-- Supabase is authoritative for all non-secret runtime knobs. pe-service polls this table
-- (WS1, 60 s) into an ArcSwap snapshot and rebuilds the strategy config per event; the admin
-- panel (WS3) edits rows with the service-role key. Secrets, paths, bind, channel caps, and
-- supabase_authoritative stay env/boot-frozen and are NOT here. KV layout (one row per knob)
-- so old binaries ignore unknown keys and new keys land additively. value_type drives the
-- admin panel's typed input (bool | integer | decimal | text).
create table if not exists service_config (
  key        text        primary key,
  value      text        not null,
  value_type text        not null check (value_type in ('bool', 'integer', 'decimal', 'text')),
  description text,
  updated_at timestamptz not null default now(),
  updated_by text                                    -- admin email (WS3 PATCH); null for the seed
);

-- Seed every non-secret knob with its compiled boot default. The sizing keys carry the live
-- smoke-test/service.toml override (sizing_mode=dollar, sizing_dollar_usd=25), so the pre-first-poll
-- window and any Supabase outage size at $25 flat, never Kelly (#398 WS2 cutover safety). `do
-- nothing` never clobbers a live admin edit. Rust tests assert these match the boot defaults: the
-- flat-scalar keys in crates/service config.rs `service_config_seed_matches_boot_defaults`, and the
-- three sizing keys (reassembled into SizingMode) in runtime_config `seed_reconstructs_boot_strategy`.
insert into service_config (key, value, value_type, description) values
  ('mode',                                  'paper',  'text',    'Trading mode: paper | shadow | live_tiny | promoted'),
  ('bankroll_usd',                          '10000',  'decimal', 'Starting-capital baseline (dashboard denominator); never re-credits the running bankroll'),
  ('max_fill_price',                        '0.85',   'decimal', 'Skip BUYs at or above this current price'),
  ('min_fill_price',                        '0.15',   'decimal', 'Skip BUYs below this current price (run28 band lower bound; 0 disables)'),
  ('min_resolution_horizon_secs',           '60',     'integer', 'Min time-to-resolution copy floor (seconds)'),
  ('max_resolution_horizon_secs',           '172800', 'integer', 'Max time-to-resolution, 48 h (run28 cutover; seconds)'),
  ('entry_gate_fail_closed',                'false',  'bool',    'Block copies for wallets whose market history could not be fetched'),
  ('trade_poll_interval_secs',              '30',     'integer', 'Leader trade poll interval (seconds)'),
  ('position_reseed_interval_secs',         '300',    'integer', 'Held-position reseed interval (seconds)'),
  ('position_page_limit',                   '500',    'integer', 'Position fetch page size'),
  ('position_size_threshold',               '1',      'integer', 'Min contracts to treat a position as held'),
  ('paper_fill_haircut_bps',                '500',    'integer', 'Paper-fill conservative haircut (bps)'),
  ('paper_fill_slippage_bps',               '100',    'integer', 'Paper-fill slippage (bps)'),
  ('fill_mode',                             'clob_best_ask', 'text', 'Paper fill-price mode: clob_best_ask | leader_haircut (#486)'),
  ('clob_best_ask_fallback_haircut_bps',    '100',    'integer', 'Fallback BUY haircut (bps) when a clob_best_ask fill has no usable best-ask (#486)'),
  ('status_interval_secs',                   '30',     'integer', 'status.json snapshot interval (seconds); 0 disables'),
  ('log_retention_days',                     '7',      'integer', 'Daily-rotated JSONL files kept per sink'),
  ('gamma_resolution_poll_interval_secs',   '120',    'integer', 'Settled-market resolution poll interval (seconds)'),
  ('supabase_refresh_interval_secs',        '300',    'integer', 'Watchlist refresh interval (seconds)'),
  ('supabase_sink_reconcile_interval_secs', '300',    'integer', 'Analytics sink reconcile interval (seconds)'),
  ('maintenance_interval_secs',             '600',    'integer', 'Watchlist maintenance tick interval (seconds)'),
  ('inactivity_threshold_secs',             '259200', 'integer', 'Soft inactivity threshold, 72 h (seconds)'),
  ('inactivity_hard_cap_secs',              '604800', 'integer', 'Hard inactivity cap, 7 d (seconds)'),
  ('bench_overfetch',                       '10',     'integer', 'Bench overfetch count'),
  ('demotion_min_trades',                   '10',     'integer', 'Min settled trades before the demotion test'),
  ('demotion_cb_alpha',                     '0.10',   'decimal', 'Empirical-Bernstein demotion confidence alpha'),
  ('demotion_pnl_window_secs',              '2592000', 'integer', 'Trailing window for the demotion dollar-P&L conjunct, 30 d (#473; seconds)'),
  ('flip_human_approved',                   'false',  'bool',    'Approval flag: allow Flip trades (admin-editable, audit-logged)'),
  ('kelly_fraction_above_default_human_approved', 'false', 'bool', 'Approval flag: allow a Kelly fraction above the mode default'),
  ('polymarket_fee_rate',                   '0.04',   'decimal', 'Polymarket BUY taker fee rate for net cost c'),
  ('slippage_rate',                         '0.01',   'decimal', 'Expected fill slippage rate added to c'),
  ('sizing_mode',                           'dollar', 'text',    'Sizing mode: kelly | dollar | contract (#398 WS2); live boot default = dollar'),
  ('sizing_dollar_usd',                     '25',     'decimal', 'USD per trade when sizing_mode=dollar (the $25-flat live default)'),
  ('sizing_contracts',                      '1',      'integer', 'Contracts per trade when sizing_mode=contract (parked default)'),
  ('price_impact_cap_bps',                  '0',      'integer', 'Price-impact gate cap (bps of best ask); 0 disables (fail-open). #398 WS2')
  on conflict (key) do nothing;

-- Operator watchlist (issue #398): the wallets pe-service copies, written by the service-role
-- key. Anon-readable so the dashboard's Live tab can compute watched-set membership. Distinct
-- from latest_ranking (the full bench); this is the live maintained working set.
create table if not exists service_watchlist (
  wallet_hex text        primary key,
  rank       integer,
  updated_at timestamptz not null default now()
);

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
    r.rank, r.ls_edge, r.ls_tstat, r.fill_rate, r.n_trades, r.hit_rate, r.avg_price,
    -- the wallet's real last on-chain trade time (epoch seconds), captured at the
    -- rank push (#357); null for aged-out wallets (fills-only, not in latest_ranking)
    -- or pre-#357 batches that predate the column.
    r.last_trade_unix
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

-- Liquidity-at-fill snapshots (#350 WS2 PR-H): anon read-only, writer (service-role) inserts
-- bypass RLS. No anon write policy → anon insert is rejected (42501), matching paper_fills.
alter table fill_market_snapshots enable row level security;
drop policy if exists "fill_market_snapshots_anon_read" on fill_market_snapshots;
create policy "fill_market_snapshots_anon_read" on fill_market_snapshots for select to anon using (true);
grant select on fill_market_snapshots to anon;

-- The site is the first anon reader of ranking_entries. Because `latest_ranking` and
-- `wallet_live_stats` are both `security_invoker` views (Supabase lint 0010), an anon read of
-- either runs the underlying scans as `anon`, so anon needs an explicit read policy on every
-- base table those views touch. Service-role readers/writers (secret key) bypass RLS, so the
-- ranker push and pe-service refresh are unaffected. No anon write policy is created.
alter table ranking_entries enable row level security;
drop policy if exists "ranking_entries_anon_read" on ranking_entries;
create policy "ranking_entries_anon_read" on ranking_entries for select to anon using (true);
grant select on ranking_entries to anon;

-- `latest_ranking` evaluates `(select max(batch_id) from ranking_batches)`; under
-- security_invoker the anon caller runs that subquery, so anon needs read access here too.
-- RLS on + an anon read policy mirrors the ranking_entries block above (and clears the
-- rls_disabled_in_public lint on this table). The ranker push writes with the service-role
-- key, which bypasses RLS — identical to the ranking_entries precedent.
alter table ranking_batches enable row level security;
drop policy if exists "ranking_batches_anon_read" on ranking_batches;
create policy "ranking_batches_anon_read" on ranking_batches for select to anon using (true);
grant select on ranking_batches to anon;

-- Writer-only cursor: RLS enabled with NO anon policy and NO grant — anon cannot touch it.
alter table supabase_sink_hwm enable row level security;

-- Writer-only audit trail (#350 WS1 PR-D): pe-service appends demote/promote rows with the
-- service-role key (bypasses RLS). RLS enabled with NO anon policy and NO grant — anon cannot
-- touch it, matching the supabase_sink_hwm precedent above.
alter table wallet_lifecycle_events enable row level security;

-- The site reads the live watchlist size for the "N watched" KPI; pe-service writes it with
-- the service-role key (bypasses RLS). Anon read-only; no anon write policy.
alter table service_runtime enable row level security;
drop policy if exists "service_runtime_anon_read" on service_runtime;
create policy "service_runtime_anon_read" on service_runtime for select to anon using (true);
grant select on service_runtime to anon;

-- Operator runtime config (#398): the dashboard reads current values (anon); the admin panel
-- writes with the service-role key (bypasses RLS). Anon read-only, no anon write policy.
alter table service_config enable row level security;
drop policy if exists "service_config_anon_read" on service_config;
create policy "service_config_anon_read" on service_config for select to anon using (true);
grant select on service_config to anon;

-- Operator watchlist (#398): the dashboard reads (anon) for Live-tab membership; pe-service
-- writes with the service-role key. Anon read-only, no anon write policy.
alter table service_watchlist enable row level security;
drop policy if exists "service_watchlist_anon_read" on service_watchlist;
create policy "service_watchlist_anon_read" on service_watchlist for select to anon using (true);
grant select on service_watchlist to anon;

-- Views are not RLS-bearing; the anon role still needs an explicit grant to read them.
grant select on latest_ranking to anon;
grant select on wallet_live_stats to anon;
