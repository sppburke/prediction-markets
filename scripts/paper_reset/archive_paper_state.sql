-- archive_paper_state.sql — fail-closed Supabase paper-state archive + reset
-- (2026-07-03 single-system cutover, docs/32 §1 operator decision; runbook docs/34).
--
-- Archives EVERY row of the five paper-state tables into *_archive siblings, verifies
-- the copy counts, and only then deletes the live rows — all in ONE transaction, so any
-- failure rolls the whole thing back (the #474 archive-before-DELETE discipline: never
-- delete what was not archived). Run via:
--
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f scripts/paper_reset/archive_paper_state.sql
--
-- (single-statement-per-txn is NOT enough here — the file opens its own BEGIN/COMMIT.)
--
-- What it deliberately does NOT touch:
--   * supabase_sink_hwm  — single id=1 row must survive (read by --backfill-supabase).
--   * service_config     — runtime knobs; `bankroll_usd` is a bookkeeping mirror
--                          (no runtime behavior consumes the parsed value, #516),
--                          updated by the operator if the new starting bankroll differs.
--   * latest_ranking / ranking_* — the wallet source is not paper state.
--   * wallet_live_stats  — a plain view over paper_fills; empties by construction.
--     The dashboard reads wallet_live_stats_mv (pg_cron refresh ≤2 min); the runbook
--     issues an immediate manual REFRESH after this script.
--
-- Re-run safety: archive tables are constraint-free append logs stamped with
-- `archived_at`; a re-run against already-emptied live tables inserts 0 rows and
-- deletes 0 rows (harmless no-op). Do not run two copies concurrently.

begin;

-- ── 1. Archive tables (constraint-free, append-only, stamped) ────────────────
create table if not exists paper_fills_archive
  as select *, now() as archived_at from paper_fills where false;
create table if not exists settled_markets_archive
  as select *, now() as archived_at from settled_markets where false;
create table if not exists paper_positions_archive
  as select *, now() as archived_at from paper_positions where false;
create table if not exists paper_bankroll_archive
  as select *, now() as archived_at from paper_bankroll where false;
create table if not exists fill_market_snapshots_archive
  as select *, now() as archived_at from fill_market_snapshots where false;

-- ── 2. Copy + verify + delete, fail-closed ───────────────────────────────────
-- Columns are reconciled BY NAME (the #474 lesson, PR-review finding on this file's
-- first draft): a future additive `alter table ... add column` on a live table must
-- not break the next reset, so any missing column is added to the archive with the
-- live column's exact type, and the insert lists columns explicitly — never a
-- positional `select *`. Each table is locked ACCESS EXCLUSIVE for the transaction
-- so no concurrent RPC write can slip between the archive insert and the delete
-- (defense-in-depth; the runbook stops the service first).
do $$
declare
  t text;
  arch text;
  col record;
  collist text;
  live_n bigint;
  copied_n bigint;
  tables constant text[] := array[
    'paper_fills', 'settled_markets', 'paper_positions',
    'paper_bankroll', 'fill_market_snapshots'
  ];
begin
  foreach t in array tables loop
    arch := t || '_archive';
    execute format('lock table %I in access exclusive mode', t);
    for col in
      select a.attname, format_type(a.atttypid, a.atttypmod) as typ
      from pg_attribute a
      where a.attrelid = t::regclass and a.attnum > 0 and not a.attisdropped
    loop
      execute format('alter table %I add column if not exists %I %s',
                     arch, col.attname, col.typ);
    end loop;
    select string_agg(format('%I', a.attname), ', ' order by a.attnum)
      into collist
      from pg_attribute a
      where a.attrelid = t::regclass and a.attnum > 0 and not a.attisdropped;
    execute format('select count(*) from %I', t) into live_n;
    execute format('insert into %I (%s, archived_at) select %s, now() from %I',
                   arch, collist, collist, t);
    get diagnostics copied_n = row_count;
    if copied_n <> live_n then
      raise exception 'archive count mismatch for %: live=% copied=% — ROLLING BACK',
        t, live_n, copied_n;
    end if;
    execute format('delete from %I', t);
    raise notice 'archived + cleared %: % row(s)', t, live_n;
  end loop;
end $$;

-- ── 3. Sanity: the sink HWM row must still exist (never touched above) ───────
do $$
declare hwm_n bigint;
begin
  select count(*) into hwm_n from supabase_sink_hwm;
  if hwm_n <> 1 then
    raise exception 'supabase_sink_hwm must keep exactly its id=1 row (found %)', hwm_n;
  end if;
end $$;

commit;

-- New tables → PostgREST schema cache reload, or REST 404s them (PGRST205).
notify pgrst, 'reload schema';

-- Post-reset state: paper_bankroll is now EMPTY — authoritative boot will fail-closed
-- (Uninitialised) by design until `pe-service --backfill-supabase` re-seeds it from the
-- fresh local state. That is the sanctioned re-seed step; see docs/34.
