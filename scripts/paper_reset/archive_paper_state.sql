-- archive_paper_state.sql — activation-stamped, fail-closed paper-state reset
--
-- psql -v ON_ERROR_STOP=1 -v activation_id=... -v bankroll=... \
--   -f scripts/paper_reset/archive_paper_state.sql
--
-- The five live tables are locked, copied, verified, and cleared in one transaction.
-- This preserves the #474 archive-before-DELETE discipline. Unconditional deletes in
-- this reviewed reset owner are intentionally allowlisted by check_sql_unfiltered_dml.py.

\if :{?activation_id}
\else
\warn 'activation_id psql variable is required'
\quit 2
\endif
\if :{?bankroll}
\else
\warn 'bankroll psql variable is required'
\quit 2
\endif

begin;

create table if not exists paper_fills_archive
  as select *, now() as archived_at, null::text as activation_id
  from paper_fills where false;
create table if not exists settled_markets_archive
  as select *, now() as archived_at, null::text as activation_id
  from settled_markets where false;
create table if not exists paper_positions_archive
  as select *, now() as archived_at, null::text as activation_id
  from paper_positions where false;
create table if not exists paper_bankroll_archive
  as select *, now() as archived_at, null::text as activation_id
  from paper_bankroll where false;
create table if not exists fill_market_snapshots_archive
  as select *, now() as archived_at, null::text as activation_id
  from fill_market_snapshots where false;

alter table paper_fills_archive add column if not exists activation_id text;
alter table settled_markets_archive add column if not exists activation_id text;
alter table paper_positions_archive add column if not exists activation_id text;
alter table paper_bankroll_archive add column if not exists activation_id text;
alter table fill_market_snapshots_archive add column if not exists activation_id text;

select set_config('pe.activation_id', :'activation_id', true);
select set_config('pe.bankroll', :'bankroll', true);

do $$
declare
  t text;
  arch text;
  col record;
  collist text;
  live_n bigint;
  copied_n bigint;
  archived_n bigint;
  already_archived boolean;
  activation text := current_setting('pe.activation_id');
  fresh_bankroll text := current_setting('pe.bankroll');
  tables constant text[] := array[
    'paper_fills', 'settled_markets', 'paper_positions',
    'paper_bankroll', 'fill_market_snapshots'
  ];
begin
  if activation = '' then
    raise exception 'activation_id must not be empty';
  end if;
  if fresh_bankroll = '' or fresh_bankroll !~ '^[0-9]+([.][0-9]+)?$' then
    raise exception 'bankroll must be a non-negative decimal string';
  end if;

  foreach t in array tables loop
    execute format('lock table %I in access exclusive mode', t);
  end loop;

  select exists (
    select 1 from paper_fills_archive where activation_id = activation
    union all select 1 from settled_markets_archive where activation_id = activation
    union all select 1 from paper_positions_archive where activation_id = activation
    union all select 1 from paper_bankroll_archive where activation_id = activation
    union all select 1 from fill_market_snapshots_archive where activation_id = activation
  ) into already_archived;

  if already_archived then
    if not exists (
      select 1 from paper_bankroll_archive where activation_id = activation
    ) then
      raise exception 'partial activation archive % has no bankroll stamp', activation;
    end if;
    if (select count(*) from paper_fills) <> 0
       or (select count(*) from settled_markets) <> 0
       or (select count(*) from paper_positions) <> 0
       or (select count(*) from fill_market_snapshots) <> 0
       or (select count(*) from paper_bankroll) <> 1
       or not exists (
         select 1 from paper_bankroll
          where id = 0 and bankroll_str::numeric = fresh_bankroll::numeric
       ) then
      raise exception 'activation % is stamped but the fresh live book does not match', activation;
    end if;
  else
    if (select count(*) from paper_bankroll) <> 1 then
      raise exception 'pre-reset paper_bankroll must contain exactly one row';
    end if;
    foreach t in array tables loop
      arch := t || '_archive';
      for col in
        select a.attname, format_type(a.atttypid, a.atttypmod) as typ
          from pg_attribute a
         where a.attrelid = t::regclass and a.attnum > 0 and not a.attisdropped
      loop
        execute format(
          'alter table %I add column if not exists %I %s', arch, col.attname, col.typ
        );
      end loop;
      select string_agg(format('%I', a.attname), ', ' order by a.attnum)
        into collist
        from pg_attribute a
       where a.attrelid = t::regclass and a.attnum > 0 and not a.attisdropped;
      execute format('select count(*) from %I', t) into live_n;
      execute format(
        'insert into %I (%s, archived_at, activation_id) '
        'select %s, now(), $1 from %I',
        arch, collist, collist, t
      ) using activation;
      get diagnostics copied_n = row_count;
      if copied_n <> live_n then
        raise exception 'archive count mismatch for %: live=% copied=%', t, live_n, copied_n;
      end if;
    end loop;

    delete from paper_fills;
    delete from settled_markets;
    delete from paper_positions;
    delete from paper_bankroll;
    delete from fill_market_snapshots;

    foreach t in array tables loop
      execute format('select count(*) from %I', t) into live_n;
      if live_n <> 0 then
        raise exception 'post-delete live count for % is %, expected zero', t, live_n;
      end if;
    end loop;

    insert into paper_bankroll (id, bankroll_str)
    values (0, fresh_bankroll);
  end if;

  foreach t in array tables loop
    arch := t || '_archive';
    execute format('select count(*) from %I where activation_id = $1', arch)
      into archived_n using activation;
    raise notice 'activation % archived %: % row(s)', activation, t, archived_n;
  end loop;
end $$;

do $$
declare hwm_n bigint;
begin
  select count(*) into hwm_n from supabase_sink_hwm;
  if hwm_n <> 1 then
    raise exception 'supabase_sink_hwm must retain exactly its id=1 row (found %)', hwm_n;
  end if;
end $$;

commit;
notify pgrst, 'reload schema';
