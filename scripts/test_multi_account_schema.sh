#!/bin/bash
# Multi-account live schema acceptance suite (#508 Phase B).
#
# Runs against a Postgres that has loaded scripts/supabase_multi_account_live_schema.sql
# (plus the Supabase roles). Verifies the SQL-layer contracts: slug grammar, at-most-one
# primary, identity immutability, login_email normalization/uniqueness, the per-account
# impact-cap CHECK, RPC atomicity (state + exactly one sanitized event per transaction),
# promotion-review round-trip, credential sanitization, append-only account_events against
# every runtime role including service_role, full anon/authenticated denial, the break-glass
# accounts DELETE (events survive; a ledgered account is RESTRICTed), and the live_fills
# idempotent insert-only ledger.
#
# Usage: bash scripts/test_multi_account_schema.sh "$PG_URL"
# CI runs it after loading the schema twice (idempotency); safe to re-run.
set -u
URL="$1"
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
expect_err "impact cap 0 rejected"               "select account_update_live_settings('partner-2', false, 1, null, null, null, 0, 't')"
expect_err "impact cap 10001 rejected"           "select account_update_live_settings('partner-2', false, 1, null, null, null, 10001, 't')"
expect_ok  "impact cap 100 accepted"             "select account_update_live_settings('partner-2', false, 1, null, null, null, 100, 't')"

# 6. RPC atomicity: unknown account leaves neither half
before=$(psql "$URL" -Atc "select count(*) from account_events")
expect_err "unknown account RPC raises"          "select account_set_login_email('ghost', 'g@g.com', 't')"
after=$(psql "$URL" -Atc "select count(*) from account_events")
if [ "$before" = "$after" ]; then echo "PASS: failed RPC left no event row"; else echo "FAIL: failed RPC leaked event rows"; fails=$((fails+1)); fi

# 7. Exactly one event per successful mutation
n=$(psql "$URL" -Atc "select count(*) from account_events where account_id='partner-2' and event_kind='live_settings_changed'")
if [ "$n" = "1" ]; then echo "PASS: exactly one live_settings_changed event"; else echo "FAIL: expected 1 settings event, got $n"; fails=$((fails+1)); fi

# 8. Promotion review record + revoke round-trip
expect_ok  "promotion review recorded"           "select account_record_promotion_review('partner-2', 'admin', 'docs/19 review', 'evidence-1')"
expect_ok  "promotion review revoked"            "select account_revoke_promotion_review('partner-2', 'admin', 'changed mind')"
n=$(psql "$URL" -Atc "select count(*) from account_events where account_id='partner-2' and event_kind in ('promotion_reviewed','promotion_review_revoked')")
if [ "$n" = "2" ]; then echo "PASS: review+revoke = two typed events"; else echo "FAIL: got $n review events"; fails=$((fails+1)); fi

# 9. Credential rotation: sealed bundle never in events
expect_ok  "credential rotation"                 "select account_rotate_credentials('partner-2', 1, 'key-1', 'AGE-SEALED-SECRET-BYTES', 'fp:ab12', 't')"
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

echo "---"
if [ "$fails" = "0" ]; then echo "ALL PHASE-B SQL ACCEPTANCE CHECKS PASSED"; else echo "$fails CHECK(S) FAILED"; exit 1; fi
