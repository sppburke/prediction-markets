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
