#!/bin/bash
# Multi-account live schema acceptance suite (#508 Phase B).
#
# Runs against a Postgres that has loaded scripts/supabase_multi_account_live_schema.sql
# (plus the Supabase roles). Verifies the SQL-layer contracts: slug grammar, at-most-one
# primary, identity immutability, login_email normalization/uniqueness, the per-account
# impact-cap CHECK, RPC atomicity (state + exactly one sanitized event per transaction),
# retired review controls, credential sanitization, append-only account_events against
# every runtime role including service_role, full anon/authenticated denial, the break-glass
# accounts DELETE (events survive; a ledgered account is RESTRICTed), and the live_fills
# idempotent insert-only ledger.
#
# Usage: bash scripts/test_multi_account_schema.sh "$PG_URL"
# CI runs it after loading the schema twice (idempotency); safe to re-run.
set -u
URL="$1"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
fails=0
expect_ok()  { if psql "$URL" -v ON_ERROR_STOP=1 -c "$2" >/dev/null 2>&1; then echo "PASS: $1"; else echo "FAIL(expected ok): $1"; fails=$((fails+1)); fi; }
expect_err() { if psql "$URL" -v ON_ERROR_STOP=1 -c "$2" >/dev/null 2>&1; then echo "FAIL(expected err): $1"; fails=$((fails+1)); else echo "PASS: $1"; fi; }

# Clean slate for re-runs.
psql "$URL" -c "set session_replication_role = replica; delete from account_events; delete from account_credentials; delete from live_fills; delete from live_positions; delete from live_account_state; delete from accounts;" >/dev/null 2>&1

# 1. Slug grammar
expect_err "slug grammar rejects uppercase"      "select account_create('BadSlug', false, 't')"
expect_err "slug grammar rejects 33 chars"       "select account_create('$(printf 'a%.0s' {1..33})', false, 't')"
expect_ok  "slug grammar accepts valid slug"     "select account_create('sppburke', true, 't')"
expect_ok  "second non-primary account creates"  "select account_create('partner-2', false, 't')"

# 2. At-most-one-primary
expect_err "second primary rejected"             "select account_create('another', true, 't')"

# 3. Immutability triggers
expect_err "account_id immutable"                "update accounts set account_id='renamed' where account_id='partner-2'"
expect_err "is_primary immutable"                "update accounts set is_primary=true where account_id='partner-2'"

# 4. login_email normalization/uniqueness (via RPC + direct CHECK)
expect_ok  "grant login email"                   "select account_set_login_email('partner-2', '  Foo@Bar.COM ', 't')"
expect_ok  "normalized stored"                   "do \$\$ begin if (select login_email from accounts where account_id='partner-2') is distinct from 'foo@bar.com' then raise exception 'not normalized'; end if; end \$\$"
expect_err "duplicate login email rejected"      "select account_set_login_email('sppburke', 'foo@bar.com', 't')"
expect_err "direct un-normalized insert rejected" "update accounts set login_email='X@Y.COM' where account_id='sppburke'"
expect_ok  "clear login email (revoke)"          "select account_set_login_email('partner-2', null, 't')"

# 5. Per-account impact-cap CHECK
expect_err "impact cap 0 rejected"               "select account_update_live_settings('partner-2', null, 1, null, null, null, 0, 't')"
expect_err "impact cap 10001 rejected"           "select account_update_live_settings('partner-2', null, 1, null, null, null, 10001, 't')"
expect_ok  "impact cap 100 accepted"             "select account_update_live_settings('partner-2', null, 1, null, null, null, 100, 't')"
before=$(psql "$URL" -Atc "select count(*) from account_events")
expect_err "old settings shape rejects enabled" "select account_update_live_settings('partner-2', false, 2, null, null, null, 100, 't')"
expect_err "old settings shape rejects enabled true" "select account_update_live_settings('partner-2', true, 2, null, null, null, 100, 't')"
after=$(psql "$URL" -Atc "select count(*) from account_events")
if [ "$before" = "$after" ]; then echo "PASS: rejected enabled left no event"; else echo "FAIL: rejected enabled wrote event"; fails=$((fails+1)); fi
expect_ok "historical enabled unchanged" "do \$\$ begin if (select enabled from accounts where account_id='partner-2') is distinct from false then raise exception 'enabled changed'; end if; end \$\$"
expect_ok "rejected settings left other columns unchanged" "do \$\$ begin if (select execution_order from accounts where account_id='partner-2') is distinct from 1 then raise exception 'settings changed'; end if; end \$\$"
expect_ok "new settings event omits enabled" "do \$\$ begin if exists (select 1 from account_events where event_kind='live_settings_changed' and (from_value::jsonb ? 'enabled' or to_value::jsonb ? 'enabled')) then raise exception 'enabled in event'; end if; end \$\$"

# 6. RPC atomicity: unknown account leaves neither half
before=$(psql "$URL" -Atc "select count(*) from account_events")
expect_err "unknown account RPC raises"          "select account_set_login_email('ghost', 'g@g.com', 't')"
after=$(psql "$URL" -Atc "select count(*) from account_events")
if [ "$before" = "$after" ]; then echo "PASS: failed RPC left no event row"; else echo "FAIL: failed RPC leaked event rows"; fails=$((fails+1)); fi

# 7. Exactly one event per successful mutation
n=$(psql "$URL" -Atc "select count(*) from account_events where account_id='partner-2' and event_kind='live_settings_changed'")
if [ "$n" = "1" ]; then echo "PASS: exactly one live_settings_changed event"; else echo "FAIL: expected 1 settings event, got $n"; fails=$((fails+1)); fi

# Owner request alone drives effective mode. Historical enabled remains false.
expect_ok "third account creates without live-count gate" "select account_create('partner-3', false, 't')"
expect_ok "owner requests live" "select account_request_mode('partner-2', 'live_tiny', 'owner')"
expect_err "stale effective proposal rejected" "select account_set_effective_mode('partner-2', 'off', 'service', 'stale')"
expect_ok "effective mode converges to live" "select account_set_effective_mode('partner-2', 'live_tiny', 'service', 'owner_request')"
expect_ok "repeat effective proposal is a no-op" "select account_set_effective_mode('partner-2', 'live_tiny', 'service', 'owner_request')"
expect_ok "owner requests off" "select account_request_mode('partner-2', 'off', 'owner')"
expect_err "old live proposal rejected after off" "select account_set_effective_mode('partner-2', 'live_tiny', 'service', 'stale')"
expect_ok "effective mode converges to off" "select account_set_effective_mode('partner-2', 'off', 'service', 'owner_request')"
expect_ok "only real effective transitions wrote events" "do \$\$ begin if (select count(*) from account_events where account_id='partner-2' and event_kind='mode_effective') != 2 then raise exception 'unexpected mode event count'; end if; end \$\$"

# Race: the owner holds the account row lock through a request change. A stale
# effective proposal waits, then must compare against the newly committed request.
race_log=$(mktemp)
PGAPPNAME=owner_mode_race psql "$URL" -v ON_ERROR_STOP=1 -c "begin; select account_request_mode('partner-3', 'live_tiny', 'owner'); select pg_sleep(5); commit;" >"$race_log" 2>&1 &
race_pid=$!
race_waiting=0
for _ in {1..100}; do
  waiting=$(psql "$URL" -Atc "select count(*) from pg_stat_activity where application_name='owner_mode_race' and wait_event='PgSleep'")
  if [ "$waiting" = "1" ]; then race_waiting=1; break; fi
  sleep 0.05
done
if [ "$race_waiting" = "1" ]; then
  expect_err "locked RPC rejects proposal stale after owner commit" "select account_set_effective_mode('partner-3', 'off', 'service', 'stale')"
else
  echo "FAIL: owner lock race did not reach held state"
  fails=$((fails+1))
fi
if wait "$race_pid"; then echo "PASS: owner request committed in race"; else echo "FAIL: owner request race failed"; fails=$((fails+1)); fi
rm -f "$race_log"
expect_ok "race kept owner's requested mode and old effective mode" "do \$\$ begin if (select requested_live_mode from accounts where account_id='partner-3') is distinct from 'live_tiny' or (select effective_live_mode from accounts where account_id='partner-3') is distinct from 'off' then raise exception 'mode race lost'; end if; end \$\$"

# 8. Retired review signatures are absent; historical event kinds remain valid.
expect_ok "retired review signatures absent" "do \$\$ begin if to_regprocedure('public.account_record_promotion_review(text,text,text,text)') is not null or to_regprocedure('public.account_revoke_promotion_review(text,text,text)') is not null then raise exception 'review RPC remains'; end if; end \$\$"
expect_err "old review route receives function-missing error" "select account_record_promotion_review('partner-2', 'admin', 'old route', 'evidence-1')"
expect_err "old revoke route receives function-missing error" "select account_revoke_promotion_review('partner-2', 'admin', 'old route')"
expect_ok "historical review event remains readable" "insert into account_events(account_id,event_kind,actor) values('partner-2','promotion_reviewed','history')"

# 9. Credential rotation: sealed bundle never in events; version chain is CAS-enforced
expect_err "rotation with a version gap rejected" "select account_rotate_credentials('partner-2', 3, 'key-1', 'X', 'fp', 't')"
expect_ok  "credential rotation"                 "select account_rotate_credentials('partner-2', 1, 'key-1', 'AGE-SEALED-SECRET-BYTES', 'fp:ab12', 't')"
expect_err "replayed same-version rotation rejected" "select account_rotate_credentials('partner-2', 1, 'key-1', 'Y', 'fp2', 't')"
expect_ok  "next-version rotation accepted"      "select account_rotate_credentials('partner-2', 2, 'key-2', 'Z', 'fp3', 't')"
leak=$(psql "$URL" -Atc "select count(*) from account_events where from_value like '%SEALED-SECRET%' or to_value like '%SEALED-SECRET%' or reason like '%SEALED-SECRET%'")
if [ "$leak" = "0" ]; then echo "PASS: sealed bundle absent from event ledger"; else echo "FAIL: sealed bundle leaked to events"; fails=$((fails+1)); fi

# 10. Append-only vs service_role (and anon/authenticated denial)
expect_err "service_role UPDATE on events denied"  "set role service_role; update account_events set actor='x' where true"
expect_err "service_role DELETE on events denied"  "set role service_role; delete from account_events where true"
for role in anon authenticated; do
  expect_err "$role SELECT accounts denied"        "set role $role; select * from accounts"
  expect_err "$role SELECT credentials denied"     "set role $role; select * from account_credentials"
  expect_err "$role SELECT events denied"          "set role $role; select * from account_events"
  expect_err "$role SELECT live_fills denied"      "set role $role; select * from live_fills"
  expect_err "$role INSERT live_fills denied"      "set role $role; insert into live_fills(account_id,idempotency_key,leader_wallet,market_id,outcome_id,side,contracts,fill_price,event_seq) values('a','k','w','m',0,'buy',1,0.5,1)"
  expect_err "$role EXECUTE account_create denied" "set role $role; select account_create('zzz', false, 't')"
done

# 11. Owner-path append-only (trigger belt-and-braces, superuser)
expect_err "owner UPDATE on events denied by trigger" "update account_events set actor='x' where true"
expect_err "owner DELETE on events denied by trigger" "delete from account_events where true"

# 12. Break-glass DELETE: allowed for a never-armed account; events survive; ledgered account restricted
ev_before=$(psql "$URL" -Atc "select count(*) from account_events where account_id='partner-2'")
expect_ok  "service_role break-glass DELETE"     "set role service_role; delete from accounts where account_id='partner-2'"
ev_after=$(psql "$URL" -Atc "select count(*) from account_events where account_id='partner-2'")
if [ "$ev_before" = "$ev_after" ] && [ "$ev_after" != "0" ]; then echo "PASS: events survive break-glass DELETE"; else echo "FAIL: events lost on DELETE ($ev_before -> $ev_after)"; fails=$((fails+1)); fi
expect_ok  "recreate account for fill test"      "select account_create('ledgered', false, 't')"
expect_ok  "insert a live fill"                  "insert into live_fills(account_id,idempotency_key,leader_wallet,market_id,outcome_id,side,contracts,fill_price,event_seq) values('ledgered','k1','0xw','0xm',0,'buy',10,0.5,1)"
expect_err "ledgered account DELETE restricted"  "delete from accounts where account_id='ledgered'"

# 13. live_fills idempotent insert (PK) + service_role UPDATE denied (insert-only ledger)
expect_err "duplicate live fill rejected by PK"  "insert into live_fills(account_id,idempotency_key,leader_wallet,market_id,outcome_id,side,contracts,fill_price,event_seq) values('ledgered','k1','0xw','0xm',0,'buy',10,0.5,1)"
expect_err "service_role UPDATE live_fills denied" "set role service_role; update live_fills set contracts=99 where true"

# 14. An old bigint position survives the idempotent numeric migration, then fractional values
# round trip through the migrated columns.
expect_ok  "restore legacy bigint position shape" "alter table live_positions alter column long_contracts type bigint using long_contracts::bigint, alter column short_contracts type bigint using short_contracts::bigint"
expect_ok  "insert legacy bigint position" "insert into live_positions(account_id,market_id,outcome_id,long_contracts,short_contracts,cost_basis) values('ledgered','0xlegacy',0,7,2,3.5)"
if psql "$URL" -v ON_ERROR_STOP=1 -f "$SCRIPT_DIR/supabase_multi_account_live_schema.sql" >/dev/null 2>&1; then
  echo "PASS: legacy bigint live-position migration reapplies"
else
  echo "FAIL(expected ok): legacy bigint live-position migration reapplies"
  fails=$((fails+1))
fi
legacy=$(psql "$URL" -Atc "select long_contracts::text || ',' || short_contracts::text || ',' || pg_typeof(long_contracts)::text from live_positions where account_id='ledgered' and market_id='0xlegacy' and outcome_id=0")
if [ "$legacy" = "7,2,numeric" ]; then echo "PASS: legacy bigint row preserved as numeric"; else echo "FAIL: legacy bigint row migration changed ($legacy)"; fails=$((fails+1)); fi
expect_ok  "fractional live position inserts" "insert into live_positions(account_id,market_id,outcome_id,long_contracts,short_contracts,cost_basis) values('ledgered','0xfractional',1,3.125001,0.000001,2.5)"
fractional=$(psql "$URL" -Atc "select long_contracts::text || ',' || short_contracts::text from live_positions where account_id='ledgered' and market_id='0xfractional' and outcome_id=1")
if [ "$fractional" = "3.125001,0.000001" ]; then echo "PASS: fractional live position round trip"; else echo "FAIL: fractional live position changed ($fractional)"; fails=$((fails+1)); fi

# 15. Pinned ea1a1a6 old-schema upgrade in a private CI database. The production
# schema and its rows above are never downgraded during this rehearsal.
upgrade_db="live_upgrade_$$"
case "$URL" in
  postgres://*/*|postgresql://*/*) upgrade_url="${URL%/*}/$upgrade_db" ;;
  *) echo "FAIL: upgrade fixture requires a database URL"; fails=$((fails+1)); upgrade_url="" ;;
esac
if [ -n "$upgrade_url" ]; then
  if psql "$URL" -v ON_ERROR_STOP=1 -c "create database $upgrade_db" >/dev/null 2>&1; then
    if psql "$upgrade_url" -v ON_ERROR_STOP=1 -f "$SCRIPT_DIR/fixtures/multi_account_live_schema_ea1a1a6.sql" >/dev/null 2>&1 \
      && psql "$upgrade_url" -v ON_ERROR_STOP=1 -c "select account_create('upgrade-acct', true, 'fixture'); select account_update_live_settings('upgrade-acct', true, 7, null, null, null, 100, 'fixture'); select account_record_promotion_review('upgrade-acct', 'fixture', 'legacy review', 'legacy-evidence');" >/dev/null 2>&1 \
      && psql "$upgrade_url" -v ON_ERROR_STOP=1 -f "$SCRIPT_DIR/supabase_multi_account_live_schema.sql" >/dev/null 2>&1 \
      && psql "$upgrade_url" -v ON_ERROR_STOP=1 -c "do \$\$ begin if (select enabled from accounts where account_id='upgrade-acct') is distinct from true or (select execution_order from accounts where account_id='upgrade-acct') is distinct from 7 then raise exception 'historical account changed'; end if; if not exists (select 1 from account_events where account_id='upgrade-acct' and event_kind='promotion_reviewed') then raise exception 'historical review missing'; end if; if to_regprocedure('public.account_record_promotion_review(text,text,text,text)') is not null then raise exception 'review function retained'; end if; end \$\$" >/dev/null 2>&1; then
      echo "PASS: ea1a1a6 schema and rows upgrade without losing history"
    else
      echo "FAIL: ea1a1a6 schema upgrade fixture"
      fails=$((fails+1))
    fi
    psql "$URL" -v ON_ERROR_STOP=1 -c "drop database $upgrade_db with (force)" >/dev/null 2>&1 || { echo "FAIL: upgrade fixture database cleanup"; fails=$((fails+1)); }
  else
    echo "FAIL: create upgrade fixture database"
    fails=$((fails+1))
  fi
fi

echo "---"
if [ "$fails" = "0" ]; then echo "ALL PHASE-B SQL ACCEPTANCE CHECKS PASSED"; else echo "$fails CHECK(S) FAILED"; exit 1; fi
