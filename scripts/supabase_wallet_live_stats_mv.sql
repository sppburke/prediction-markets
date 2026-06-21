-- Materialized view for the analytics site — read-side optimization.
--
-- `wallet_live_stats` (scripts/supabase_schema.sql) is a FULL OUTER JOIN + aggregation over
-- ALL `paper_fills` recomputed on EVERY query; the site hit it on every page view, so cost
-- grew with the fills table. This materializes the view's output and refreshes it on a
-- schedule, so the expensive query runs once per refresh instead of once per page view.
--
-- Prerequisite: scripts/supabase_schema.sql (defines `wallet_live_stats`). Idempotent.
-- Apply via: psql "$SUPABASE_DB_URL" -f scripts/supabase_wallet_live_stats_mv.sql

-- The matview caches the existing view's output (no query duplication). It is refreshed by the
-- owner, so the `security_invoker` view sees all rows; anon reads the materialized result
-- directly via the grant below (materialized views do not carry RLS — the data is the same
-- public analytics the view already exposes to anon).
create materialized view if not exists wallet_live_stats_mv as
  select * from wallet_live_stats;

-- Unique index on `wallet` (the view yields one row per wallet) — required for
-- REFRESH ... CONCURRENTLY, which keeps the matview readable during a refresh.
create unique index if not exists wallet_live_stats_mv_wallet
  on wallet_live_stats_mv (wallet);

-- The site reads this with the anon key (same exposure as the `wallet_live_stats` view).
grant select on wallet_live_stats_mv to anon;

-- Schedule a refresh every 2 minutes (analytics — 2-min staleness is fine). Guarded by a
-- pg_cron availability check so this file still loads on a vanilla Postgres (CI / docker),
-- which has no pg_cron; on Supabase (pg_cron available) it enables the extension and schedules.
-- `cron.schedule(jobname, ...)` is idempotent by name. If pg_cron is absent, schedule the
-- refresh externally (`refresh materialized view concurrently wallet_live_stats_mv`).
do $$
begin
  if exists (select 1 from pg_available_extensions where name = 'pg_cron') then
    create extension if not exists pg_cron;
    perform cron.schedule(
      'refresh_wallet_live_stats_mv',
      '*/2 * * * *',
      'refresh materialized view concurrently wallet_live_stats_mv'
    );
    raise notice 'pg_cron: scheduled refresh_wallet_live_stats_mv every 2 minutes';
  else
    raise notice 'pg_cron not available; refresh wallet_live_stats_mv externally';
  end if;
end $$;
