-- Online expansion for #714 account-cap retirement.
-- Install before the cap-free site and service. This keeps the eight-argument RPC
-- and the cap column available to old callers until the canonical contraction.
-- Reapplying this script is safe while the old schema is present.

begin;

create or replace function public.account_update_live_settings(
  p_account_id                text,
  p_enabled                   boolean,
  p_execution_order           integer,
  p_live_sizing_mode          text,
  p_live_sizing_dollar_usd    numeric,
  p_live_sizing_contracts     bigint,
  p_actor                     text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_execution_order           integer;
  v_live_sizing_mode          text;
  v_live_sizing_dollar_usd    numeric;
  v_live_sizing_contracts     bigint;
  v_from_value                text;
  v_to_value                  text;
begin
  if p_enabled is not null then
    raise exception 'enabled is historical; use Request mode';
  end if;

  select
    execution_order,
    live_sizing_mode,
    live_sizing_dollar_usd,
    live_sizing_contracts
  into
    v_execution_order,
    v_live_sizing_mode,
    v_live_sizing_dollar_usd,
    v_live_sizing_contracts
  from public.accounts
  where account_id = p_account_id
  for update;

  if not found then
    raise exception 'unknown account_id: %', p_account_id;
  end if;

  v_from_value := pg_catalog.jsonb_build_object(
    'execution_order', v_execution_order,
    'live_sizing_mode', v_live_sizing_mode,
    'live_sizing_dollar_usd', v_live_sizing_dollar_usd,
    'live_sizing_contracts', v_live_sizing_contracts
  )::text;

  update public.accounts
     set execution_order           = p_execution_order,
         live_sizing_mode          = p_live_sizing_mode,
         live_sizing_dollar_usd    = p_live_sizing_dollar_usd,
         live_sizing_contracts     = p_live_sizing_contracts,
         updated_at                = pg_catalog.now()
   where account_id = p_account_id;

  v_to_value := pg_catalog.jsonb_build_object(
    'execution_order', p_execution_order,
    'live_sizing_mode', p_live_sizing_mode,
    'live_sizing_dollar_usd', p_live_sizing_dollar_usd,
    'live_sizing_contracts', p_live_sizing_contracts
  )::text;

  insert into public.account_events (
    account_id,
    event_kind,
    actor,
    from_value,
    to_value
  )
  values (
    p_account_id,
    'live_settings_changed',
    p_actor,
    v_from_value,
    v_to_value
  );
end;
$$;

revoke all on function public.account_update_live_settings(
  text, boolean, integer, text, numeric, bigint, text
) from public, anon, authenticated;
grant execute on function public.account_update_live_settings(
  text, boolean, integer, text, numeric, bigint, text
) to service_role;

notify pgrst, 'reload schema';

commit;
