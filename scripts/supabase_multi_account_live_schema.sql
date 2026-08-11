-- Supabase schema for Phase-B multi-account live execution (issue #508).
--
-- This additive schema introduces typed per-account control state, sealed credential
-- storage, an immutable control-plane event ledger, and account-scoped live execution
-- state. It creates no rows and is safe to load while the current production binary is
-- running. All control mutations use SECURITY DEFINER RPCs so each state change and its
-- single sanitized audit event commit atomically.
--
-- Idempotent: safe to re-run. Every object created here is new to issue #508.

begin;

-- ════════════════════════════════════════════════════════════════════════════
-- Tables
-- ════════════════════════════════════════════════════════════════════════════

-- Typed account inventory and control state (#508). `account_id` and `is_primary`
-- are immutable after insertion through the trigger below. A service-role DELETE
-- remains available as a break-glass path for a mis-created, never-armed account.
create table if not exists public.accounts (
  account_id                    text        primary key
    check (account_id ~ '^[a-z0-9_-]{1,32}$'),
  is_primary                    boolean     not null default false,
  login_email                   text
    check (
      login_email is null
      or (
        login_email <> ''
        and login_email = lower(btrim(login_email))
      )
    ),
  enabled                       boolean     not null default false,
  execution_order               integer     not null default 0,
  requested_live_mode           text        not null default 'off'
    check (requested_live_mode in ('off', 'live_tiny')),
  effective_live_mode           text        not null default 'off'
    check (effective_live_mode in ('off', 'live_tiny')),
  live_sizing_mode              text
    check (
      live_sizing_mode is null
      or live_sizing_mode in ('kelly', 'dollar', 'contract')
    ),
  live_sizing_dollar_usd        numeric
    check (
      live_sizing_dollar_usd is null
      or live_sizing_dollar_usd > 0
    ),
  live_sizing_contracts         bigint
    check (
      live_sizing_contracts is null
      or live_sizing_contracts > 0
    ),
  live_price_impact_cap_bps     integer     not null default 100
    check (live_price_impact_cap_bps between 1 and 10000),
  custody_wallet_address        text,
  custody_wallet_kind           text
    check (
      custody_wallet_kind is null
      or custody_wallet_kind in ('eoa', 'deposit_wallet', 'proxy', 'safe')
    ),
  created_at                    timestamptz not null default now(),
  updated_at                    timestamptz not null default now()
);

-- At most one account may be marked primary (#508). Non-primary execution order
-- intentionally permits ties; Rust orders those accounts by (execution_order, account_id).
create unique index if not exists accounts_one_primary_idx
  on public.accounts (is_primary)
  where is_primary;

-- Login authorization is independent of the execution `enabled` flag. NULL means
-- login disabled; normalized non-NULL addresses must be globally unique (#508).
create unique index if not exists accounts_login_email_idx
  on public.accounts (login_email)
  where login_email is not null;

-- Sealed age/X25519 credential bundles (#508). The site writes through
-- account_rotate_credentials; the execution service may read the sealed bundle with
-- its service-role credential. No credential material is copied into account_events.
create table if not exists public.account_credentials (
  account_id     text        primary key
    references public.accounts(account_id) on delete cascade,
  bundle_version integer     not null,
  key_id         text        not null,
  sealed_bundle  text        not null,
  fingerprint    text        not null,
  updated_at     timestamptz not null default now()
);

-- Immutable control-plane history (#508). `account_id` deliberately has no foreign
-- key: account history must survive a break-glass DELETE from public.accounts.
create table if not exists public.account_events (
  event_id     bigint generated always as identity primary key,
  account_id   text        not null,
  event_kind   text        not null
    check (
      event_kind in (
        'account_created',
        'login_email_granted',
        'login_email_revoked',
        'live_settings_changed',
        'credential_rotated',
        'mode_requested',
        'mode_effective',
        'promotion_reviewed',
        'promotion_review_revoked'
      )
    ),
  actor        text        not null,
  from_value   text,
  to_value     text,
  reason       text,
  evidence_ref text,
  created_at   timestamptz not null default now()
);

create index if not exists account_events_account_created_idx
  on public.account_events (account_id, created_at, event_id);

-- Account-scoped live fills (#508). The shape mirrors paper_fills while extending
-- its idempotency boundary with account_id. ON DELETE RESTRICT prevents deletion
-- of an account once a ledgered live fill exists.
create table if not exists public.live_fills (
  account_id       text        not null
    references public.accounts(account_id) on delete restrict,
  idempotency_key  text        not null,
  leader_wallet    text        not null,
  source_trade_id  text,
  market_id        text        not null,
  outcome_id       integer     not null,
  side             text        not null,
  contracts        numeric     not null,
  fill_price       numeric     not null,
  entry_unix       bigint,
  event_seq        bigint      not null,
  inserted_at      timestamptz not null default now(),
  primary key (account_id, idempotency_key)
);

-- Current account-scoped live positions (#508). These rows are derived mutable
-- state; live_fills remains the idempotent fill ledger.
create table if not exists public.live_positions (
  account_id       text    not null
    references public.accounts(account_id) on delete restrict,
  market_id        text    not null,
  outcome_id       integer not null,
  long_contracts   bigint  not null default 0,
  short_contracts  bigint  not null default 0,
  cost_basis       numeric not null default 0,
  primary key (account_id, market_id, outcome_id)
);

-- One current collateral/admission row per account when live state has been
-- initialized (#508). No rows are seeded by this migration.
create table if not exists public.live_account_state (
  account_id              text    primary key
    references public.accounts(account_id) on delete restrict,
  free_collateral         numeric not null default 0,
  reserved                numeric not null default 0,
  unredeemed_value        numeric not null default 0,
  last_reconciled_at      timestamptz,
  admission_closed_reason text
);

-- ════════════════════════════════════════════════════════════════════════════
-- Guard triggers
-- ════════════════════════════════════════════════════════════════════════════

-- The account identity and primary designation are creation-time facts (#508).
-- Ordinary account settings remain mutable only through the RPCs below.
create or replace function public.account_reject_identity_change()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  if new.account_id is distinct from old.account_id
     or new.is_primary is distinct from old.is_primary then
    raise exception
      'accounts.account_id and accounts.is_primary are immutable';
  end if;

  return new;
end;
$$;

drop trigger if exists accounts_identity_immutable on public.accounts;
create trigger accounts_identity_immutable
before update on public.accounts
for each row
execute function public.account_reject_identity_change();

-- Belt-and-braces append-only enforcement for the #508 control-plane ledger.
-- The ACL block below denies runtime UPDATE/DELETE; this trigger also rejects
-- ordinary table-owner and superuser UPDATE/DELETE paths.
create or replace function public.account_events_reject_mutation()
returns trigger
language plpgsql
set search_path = ''
as $$
begin
  raise exception 'account_events is append-only: % is forbidden', tg_op;
  return null;
end;
$$;

drop trigger if exists account_events_append_only on public.account_events;
create trigger account_events_append_only
before update or delete on public.account_events
for each row
execute function public.account_events_reject_mutation();

revoke all on function public.account_reject_identity_change()
  from public, anon, authenticated, service_role;
revoke all on function public.account_events_reject_mutation()
  from public, anon, authenticated, service_role;

-- ════════════════════════════════════════════════════════════════════════════
-- Control-mutation RPCs
--
-- Each SECURITY DEFINER RPC is one atomic state transition and inserts exactly
-- one typed, sanitized account_events row (#508). A raised exception rolls back
-- both halves. The empty search_path and schema-qualified relations prevent
-- object-shadowing in these privileged functions.
-- ════════════════════════════════════════════════════════════════════════════

create or replace function public.account_create(
  p_account_id text,
  p_is_primary boolean,
  p_actor      text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
  insert into public.accounts (account_id, is_primary)
  values (p_account_id, p_is_primary);

  insert into public.account_events (
    account_id,
    event_kind,
    actor,
    to_value
  )
  values (
    p_account_id,
    'account_created',
    p_actor,
    pg_catalog.jsonb_build_object(
      'is_primary', p_is_primary
    )::text
  );
end;
$$;

create or replace function public.account_set_login_email(
  p_account_id  text,
  p_login_email text,
  p_actor       text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_from_email text;
  v_to_email   text;
  v_event_kind text;
begin
  select login_email
    into v_from_email
    from public.accounts
   where account_id = p_account_id
   for update;

  if not found then
    raise exception 'unknown account_id: %', p_account_id;
  end if;

  -- NULL and whitespace-only inputs revoke login. All other values are stored
  -- in the same normalized form enforced by the table CHECK (#508).
  -- NULLIF is a SQL conditional expression (parser-resolved, immune to search_path),
  -- not a pg_catalog function.
  v_to_email := nullif(
    pg_catalog.lower(pg_catalog.btrim(p_login_email)),
    ''
  );

  if v_to_email is null then
    v_event_kind := 'login_email_revoked';
  else
    v_event_kind := 'login_email_granted';
  end if;

  update public.accounts
     set login_email = v_to_email,
         updated_at  = pg_catalog.now()
   where account_id = p_account_id;

  insert into public.account_events (
    account_id,
    event_kind,
    actor,
    from_value,
    to_value
  )
  values (
    p_account_id,
    v_event_kind,
    p_actor,
    v_from_email,
    v_to_email
  );
end;
$$;

create or replace function public.account_update_live_settings(
  p_account_id                text,
  p_enabled                   boolean,
  p_execution_order           integer,
  p_live_sizing_mode          text,
  p_live_sizing_dollar_usd    numeric,
  p_live_sizing_contracts     bigint,
  p_live_price_impact_cap_bps integer,
  p_actor                     text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_enabled                   boolean;
  v_execution_order           integer;
  v_live_sizing_mode          text;
  v_live_sizing_dollar_usd    numeric;
  v_live_sizing_contracts     bigint;
  v_live_price_impact_cap_bps integer;
  v_from_value                text;
  v_to_value                  text;
begin
  select
    enabled,
    execution_order,
    live_sizing_mode,
    live_sizing_dollar_usd,
    live_sizing_contracts,
    live_price_impact_cap_bps
  into
    v_enabled,
    v_execution_order,
    v_live_sizing_mode,
    v_live_sizing_dollar_usd,
    v_live_sizing_contracts,
    v_live_price_impact_cap_bps
  from public.accounts
  where account_id = p_account_id
  for update;

  if not found then
    raise exception 'unknown account_id: %', p_account_id;
  end if;

  v_from_value := pg_catalog.jsonb_build_object(
    'enabled', v_enabled,
    'execution_order', v_execution_order,
    'live_sizing_mode', v_live_sizing_mode,
    'live_sizing_dollar_usd', v_live_sizing_dollar_usd,
    'live_sizing_contracts', v_live_sizing_contracts,
    'live_price_impact_cap_bps', v_live_price_impact_cap_bps
  )::text;

  update public.accounts
     set enabled                   = p_enabled,
         execution_order           = p_execution_order,
         live_sizing_mode          = p_live_sizing_mode,
         live_sizing_dollar_usd    = p_live_sizing_dollar_usd,
         live_sizing_contracts     = p_live_sizing_contracts,
         live_price_impact_cap_bps = p_live_price_impact_cap_bps,
         updated_at                = pg_catalog.now()
   where account_id = p_account_id;

  v_to_value := pg_catalog.jsonb_build_object(
    'enabled', p_enabled,
    'execution_order', p_execution_order,
    'live_sizing_mode', p_live_sizing_mode,
    'live_sizing_dollar_usd', p_live_sizing_dollar_usd,
    'live_sizing_contracts', p_live_sizing_contracts,
    'live_price_impact_cap_bps', p_live_price_impact_cap_bps
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

create or replace function public.account_rotate_credentials(
  p_account_id     text,
  p_bundle_version integer,
  p_key_id         text,
  p_sealed_bundle  text,
  p_fingerprint    text,
  p_actor          text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_bundle_version integer;
  v_key_id         text;
  v_fingerprint    text;
  v_from_value     text;
  v_to_value       text;
begin
  -- Lock the owning account first so credential rotations serialize with all
  -- other account control mutations and cannot race a break-glass DELETE.
  perform 1
    from public.accounts
   where account_id = p_account_id
   for update;

  if not found then
    raise exception 'unknown account_id: %', p_account_id;
  end if;

  select bundle_version, key_id, fingerprint
    into v_bundle_version, v_key_id, v_fingerprint
    from public.account_credentials
   where account_id = p_account_id;

  v_from_value := pg_catalog.jsonb_build_object(
    'bundle_version', v_bundle_version,
    'key_id', v_key_id,
    'fingerprint', v_fingerprint
  )::text;

  insert into public.account_credentials (
    account_id,
    bundle_version,
    key_id,
    sealed_bundle,
    fingerprint
  )
  values (
    p_account_id,
    p_bundle_version,
    p_key_id,
    p_sealed_bundle,
    p_fingerprint
  )
  on conflict (account_id) do update
    set bundle_version = excluded.bundle_version,
        key_id         = excluded.key_id,
        sealed_bundle  = excluded.sealed_bundle,
        fingerprint    = excluded.fingerprint,
        updated_at     = pg_catalog.now();

  -- Deliberately excludes p_sealed_bundle. Only display-safe credential
  -- metadata enters the immutable event ledger (#508).
  v_to_value := pg_catalog.jsonb_build_object(
    'bundle_version', p_bundle_version,
    'key_id', p_key_id,
    'fingerprint', p_fingerprint
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
    'credential_rotated',
    p_actor,
    v_from_value,
    v_to_value
  );
end;
$$;

create or replace function public.account_request_mode(
  p_account_id     text,
  p_requested_mode text,
  p_actor          text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_from_mode text;
begin
  select requested_live_mode
    into v_from_mode
    from public.accounts
   where account_id = p_account_id
   for update;

  if not found then
    raise exception 'unknown account_id: %', p_account_id;
  end if;

  update public.accounts
     set requested_live_mode = p_requested_mode,
         updated_at          = pg_catalog.now()
   where account_id = p_account_id;

  insert into public.account_events (
    account_id,
    event_kind,
    actor,
    from_value,
    to_value
  )
  values (
    p_account_id,
    'mode_requested',
    p_actor,
    v_from_mode,
    p_requested_mode
  );
end;
$$;

create or replace function public.account_set_effective_mode(
  p_account_id     text,
  p_effective_mode text,
  p_actor          text,
  p_reason         text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
declare
  v_from_mode text;
begin
  select effective_live_mode
    into v_from_mode
    from public.accounts
   where account_id = p_account_id
   for update;

  if not found then
    raise exception 'unknown account_id: %', p_account_id;
  end if;

  update public.accounts
     set effective_live_mode = p_effective_mode,
         updated_at          = pg_catalog.now()
   where account_id = p_account_id;

  insert into public.account_events (
    account_id,
    event_kind,
    actor,
    from_value,
    to_value,
    reason
  )
  values (
    p_account_id,
    'mode_effective',
    p_actor,
    v_from_mode,
    p_effective_mode,
    p_reason
  );
end;
$$;

create or replace function public.account_record_promotion_review(
  p_account_id  text,
  p_actor       text,
  p_reason      text,
  p_evidence_ref text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
  perform 1
    from public.accounts
   where account_id = p_account_id
   for update;

  if not found then
    raise exception 'unknown account_id: %', p_account_id;
  end if;

  -- The immutable event itself is the promotion review record (#508).
  insert into public.account_events (
    account_id,
    event_kind,
    actor,
    reason,
    evidence_ref
  )
  values (
    p_account_id,
    'promotion_reviewed',
    p_actor,
    p_reason,
    p_evidence_ref
  );
end;
$$;

create or replace function public.account_revoke_promotion_review(
  p_account_id text,
  p_actor      text,
  p_reason     text
) returns void
language plpgsql
security definer
set search_path = ''
as $$
begin
  perform 1
    from public.accounts
   where account_id = p_account_id
   for update;

  if not found then
    raise exception 'unknown account_id: %', p_account_id;
  end if;

  -- Revocation supersedes prior review events without rewriting history (#508).
  insert into public.account_events (
    account_id,
    event_kind,
    actor,
    reason
  )
  values (
    p_account_id,
    'promotion_review_revoked',
    p_actor,
    p_reason
  );
end;
$$;

-- ════════════════════════════════════════════════════════════════════════════
-- Row-level security and table privileges
--
-- These are service-role-only objects (#508): RLS is enabled with no anon or
-- authenticated policies. Supabase's service_role bypasses RLS. Control tables
-- expose reads plus the explicit account DELETE break-glass path; their writes
-- otherwise go through SECURITY DEFINER RPCs. Live derived state is maintained
-- directly by the execution service, while live_fills is insert-only.
-- ════════════════════════════════════════════════════════════════════════════

alter table public.accounts enable row level security;
alter table public.account_credentials enable row level security;
alter table public.account_events enable row level security;
alter table public.live_fills enable row level security;
alter table public.live_positions enable row level security;
alter table public.live_account_state enable row level security;

revoke all privileges on table
  public.accounts,
  public.account_credentials,
  public.account_events,
  public.live_fills,
  public.live_positions,
  public.live_account_state
from public, anon, authenticated;

revoke all privileges on sequence public.account_events_event_id_seq
  from public, anon, authenticated;

-- Control state is read by the service, but all ordinary writes use the typed
-- RPCs. DELETE on accounts is the explicit #508 break-glass exception.
grant select, delete on table public.accounts to service_role;
grant select on table public.account_credentials to service_role;
grant select on table public.account_events to service_role;

-- A fill is immutable after its idempotent insert. Positions and account state
-- are current-state projections and may be upserted or removed by the service.
grant select, insert on table public.live_fills to service_role;
grant select, insert, update, delete on table public.live_positions to service_role;
grant select, insert, update, delete on table public.live_account_state to service_role;

-- Explicitly preserve append-only history even if a prior deployment granted
-- broader service-role table privileges.
revoke update, delete on table public.account_events
  from public, anon, authenticated, service_role;

-- ════════════════════════════════════════════════════════════════════════════
-- RPC privileges
--
-- PostgreSQL grants function EXECUTE to PUBLIC by default. Remove that grant and
-- expose every #508 control mutation only to service_role.
-- ════════════════════════════════════════════════════════════════════════════

revoke all on function public.account_create(text, boolean, text)
  from public, anon, authenticated;
grant execute on function public.account_create(text, boolean, text)
  to service_role;

revoke all on function public.account_set_login_email(text, text, text)
  from public, anon, authenticated;
grant execute on function public.account_set_login_email(text, text, text)
  to service_role;

revoke all on function public.account_update_live_settings(
  text, boolean, integer, text, numeric, bigint, integer, text
) from public, anon, authenticated;
grant execute on function public.account_update_live_settings(
  text, boolean, integer, text, numeric, bigint, integer, text
) to service_role;

revoke all on function public.account_rotate_credentials(
  text, integer, text, text, text, text
) from public, anon, authenticated;
grant execute on function public.account_rotate_credentials(
  text, integer, text, text, text, text
) to service_role;

revoke all on function public.account_request_mode(text, text, text)
  from public, anon, authenticated;
grant execute on function public.account_request_mode(text, text, text)
  to service_role;

revoke all on function public.account_set_effective_mode(text, text, text, text)
  from public, anon, authenticated;
grant execute on function public.account_set_effective_mode(text, text, text, text)
  to service_role;

revoke all on function public.account_record_promotion_review(text, text, text, text)
  from public, anon, authenticated;
grant execute on function public.account_record_promotion_review(text, text, text, text)
  to service_role;

revoke all on function public.account_revoke_promotion_review(text, text, text)
  from public, anon, authenticated;
grant execute on function public.account_revoke_promotion_review(text, text, text)
  to service_role;

-- ════════════════════════════════════════════════════════════════════════════
-- VERIFICATION — manual psql snippets for issue #508
--
-- These commands are intentionally commented out. Run after loading the schema
-- with ON_ERROR_STOP disabled (or one expected-failure statement at a time).
-- The CI schema-load step may reference this suite when adding the Phase-B gate.
-- ════════════════════════════════════════════════════════════════════════════

-- Role-denial suite: both queries must return zero rows.
--
-- select grantee, table_name, privilege_type
--   from information_schema.table_privileges
--  where table_schema = 'public'
--    and table_name in (
--      'accounts',
--      'account_credentials',
--      'account_events',
--      'live_fills',
--      'live_positions',
--      'live_account_state'
--    )
--    and grantee in ('PUBLIC', 'anon', 'authenticated');
--
-- select grantee, routine_name, privilege_type
--   from information_schema.routine_privileges
--  where specific_schema = 'public'
--    and routine_name in (
--      'account_create',
--      'account_set_login_email',
--      'account_update_live_settings',
--      'account_rotate_credentials',
--      'account_request_mode',
--      'account_set_effective_mode',
--      'account_record_promotion_review',
--      'account_revoke_promotion_review'
--    )
--    and grantee in ('PUBLIC', 'anon', 'authenticated');

-- Direct role checks: every statement must fail with permission denied.
--
-- set role anon;
-- select * from public.accounts;
-- select * from public.account_credentials;
-- select * from public.account_events;
-- select * from public.live_fills;
-- select * from public.live_positions;
-- select * from public.live_account_state;
-- select public.account_create('denied', false, 'verification');
-- reset role;
--
-- set role authenticated;
-- select * from public.accounts;
-- select public.account_request_mode('missing', 'live_tiny', 'verification');
-- reset role;

-- Constraint, atomicity, sanitization, and append-only checks.
-- Use \set ON_ERROR_STOP off and \set ON_ERROR_ROLLBACK on for expected errors.
--
-- begin;
-- select public.account_create('primary', true, 'verification');
-- select public.account_create('secondary', false, 'verification');
--
-- -- Expected error: slug CHECK.
-- select public.account_create('INVALID ACCOUNT', false, 'verification');
--
-- -- Expected error: partial unique index permits only one primary.
-- select public.account_create('second_primary', true, 'verification');
--
-- -- Expected errors: creation-time identity is immutable.
-- update public.accounts
--    set account_id = 'renamed'
--  where account_id = 'secondary';
-- update public.accounts
--    set is_primary = true
--  where account_id = 'secondary';
--
-- -- Expected stored value: operator@example.com.
-- select public.account_set_login_email(
--   'secondary',
--   '  Operator@Example.COM  ',
--   'verification'
-- );
-- select login_email
--   from public.accounts
--  where account_id = 'secondary';
--
-- -- Expected error: normalized non-NULL login emails are unique.
-- select public.account_set_login_email(
--   'primary',
--   'operator@example.com',
--   'verification'
-- );
--
-- -- Expected stored value: NULL, with login_email_revoked event.
-- select public.account_set_login_email(
--   'secondary',
--   '   ',
--   'verification'
-- );
--
-- -- Expected errors: positive sizing and 1..10000 bps constraints.
-- select public.account_update_live_settings(
--   'secondary', true, 10, 'dollar', -1, null, 100, 'verification'
-- );
-- select public.account_update_live_settings(
--   'secondary', true, 10, 'dollar', 25, null, 0, 'verification'
-- );
--
-- -- Rotation succeeds, and the event must contain metadata but not ciphertext.
-- select public.account_rotate_credentials(
--   'secondary',
--   1,
--   'phase-b-key',
--   'AGE-TEST-CIPHERTEXT-MUST-NOT-ENTER-EVENTS',
--   'fp-test',
--   'verification'
-- );
-- select
--   event_kind,
--   position(
--     'AGE-TEST-CIPHERTEXT-MUST-NOT-ENTER-EVENTS'
--     in coalesce(from_value, '') || coalesce(to_value, '')
--   ) = 0 as ciphertext_absent
-- from public.account_events
-- where account_id = 'secondary'
--   and event_kind = 'credential_rotated'
-- order by event_id desc
-- limit 1;
--
-- -- Expected error, followed by zero rows: unknown accounts leave no event half.
-- select public.account_request_mode(
--   'missing',
--   'live_tiny',
--   'verification'
-- );
-- select count(*)
--   from public.account_events
--  where account_id = 'missing';
--
-- -- Expected errors even as the table owner: the ledger guard rejects mutation.
-- update public.account_events
--    set reason = 'forbidden rewrite'
--  where account_id = 'secondary';
-- delete from public.account_events
--  where account_id = 'secondary';
--
-- -- Expected error: a ledgered live account cannot be break-glass deleted.
-- insert into public.live_fills (
--   account_id,
--   idempotency_key,
--   leader_wallet,
--   market_id,
--   outcome_id,
--   side,
--   contracts,
--   fill_price,
--   event_seq
-- )
-- values (
--   'secondary',
--   'verification-fill',
--   '0xverification',
--   'verification-market',
--   0,
--   'buy',
--   1,
--   0.5,
--   1
-- );
-- delete from public.accounts where account_id = 'secondary';
--
-- rollback;

-- PostgREST publishes the new tables and RPC signatures after this transaction commits.
notify pgrst, 'reload schema';

commit;