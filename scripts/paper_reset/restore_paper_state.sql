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

select set_config('pe.activation_id', :'activation_id', true);
do $$
declare
  activation text := current_setting('pe.activation_id');
  t text;
  arch text;
  collist text;
  missing_column text;
  live_n bigint;
  archived_n bigint;
  tables constant text[] := array[
    'paper_fills', 'settled_markets', 'paper_positions',
    'paper_bankroll', 'fill_market_snapshots'
  ];
begin
  foreach t in array tables loop
    arch := t || '_archive';
    select a.attname
      into missing_column
      from pg_attribute a
     where a.attrelid = t::regclass and a.attnum > 0 and not a.attisdropped
       and not exists (
         select 1
           from pg_attribute archived
          where archived.attrelid = arch::regclass
            and archived.attnum > 0 and not archived.attisdropped
            and archived.attname = a.attname
       )
     order by a.attnum
     limit 1;
    if missing_column is not null then
      raise exception 'archive % lacks live column %', arch, missing_column;
    end if;
    select string_agg(format('%I', a.attname), ', ' order by a.attnum)
      into collist
      from pg_attribute a
     where a.attrelid = t::regclass and a.attnum > 0 and not a.attisdropped;
    execute format(
      'insert into %I (%s) select %s from %I where activation_id = $1',
      t, collist, collist, arch
    ) using activation;
    execute format('select count(*) from %I', t) into live_n;
    execute format('select count(*) from %I where activation_id = $1', arch)
      into archived_n using activation;
    if live_n <> archived_n then
      raise exception 'restored % count % does not match activation % archive count %',
        t, live_n, activation, archived_n;
    end if;
  end loop;
  if (select count(*) from paper_bankroll) <> 1 then
    raise exception 'restored activation % must contain exactly one bankroll row', activation;
  end if;
end $$;

commit;
notify pgrst, 'reload schema';
