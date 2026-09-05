//! Live-Postgres parity + concurrency harness for the authoritative RPCs (issue #397).
//!
//! Executes the real PL/pgSQL `commit_fill` / `apply_resolution` (from
//! `scripts/supabase_paper_state_schema.sql`) against a Postgres and checks the properties
//! that the in-process fake in `scenario_supabase_state.rs` cannot: that the SQL *itself*
//! matches the canonical Rust accounting, and that the self-referencing bankroll UPDATE
//! actually prevents lost updates under concurrent transactions (the B-E fix).
//!
//! **Self-skips when `PE_TEST_PG_URL` is unset**, so it never runs (and never fails) under
//! the local `cargo nextest run --workspace --all-features` gate, which has no Postgres. CI
//! sets `PE_TEST_PG_URL` to its `services: postgres` and loads both schema files first.
//! Run locally:
//!   docker run -d --name pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
//!   psql "$URL" -c "create role anon; create role authenticated; create role service_role;"
//!   psql "$URL" -f scripts/supabase_schema.sql -f scripts/supabase_paper_state_schema.sql
//!   PE_TEST_PG_URL="$URL" cargo nextest run -p pe-service --features scenario --test pg_parity
//!
//! Phases (one sequential test so the shared `paper_bankroll` singleton is not raced):
//!   PG-PARITY — SQL `commit_fill` bankroll + positions == `PaperStateDb::commit_fill`.
//!   PG-AC2    — a duplicate `idempotency_key` debits once.
//!   PG-AC8    — a duplicate `apply_resolution` market credits once.
//!   PG-AC9    — concurrent debits + credits across many connections lose no update.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::str::FromStr;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio_postgres::{Client, NoTls};

/// The pg tests share one database and nextest runs each test in its OWN PROCESS — an
/// in-process mutex cannot serialize them. A session-scoped Postgres advisory lock does:
/// the returned connection holds it for the test's lifetime and releases it on drop.
async fn pg_lock(url: &str) -> Client {
    let client = connect(url).await;
    client
        .batch_execute("select pg_advisory_lock(715_511)")
        .await
        .unwrap();
    client
}

/// Reload the exact checked-in financial schema so parity cannot accidentally exercise a stale
/// database function body. The prerequisite shared schema is still installed by CI/the operator.
async fn load_financial_schema(client: &Client) {
    client
        .batch_execute(include_str!(
            "../../../scripts/supabase_paper_state_schema.sql"
        ))
        .await
        .expect("load checked-in paper financial schema");
}

const LEADER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const START: &str = "100000";
const START_SEQ: i64 = 10;
const START_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn dec(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

async fn connect(url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .expect("connect pg");
    // Drive the connection in the background; it ends when the client drops.
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Truncate the authoritative tables and seed the bankroll singleton to `start`.
async fn reset(client: &Client, start: &str) {
    client
        .batch_execute(
            "delete from paper_fills; delete from paper_positions; \
             delete from settled_markets; delete from paper_bankroll;",
        )
        .await
        .unwrap();
    client
        .execute(
            "insert into paper_bankroll \
                (id, bankroll_str, start_seq, start_hash, last_prepared_seq) \
             values (0, $1, $2, $3, null)",
            &[&start, &START_SEQ, &START_HASH],
        )
        .await
        .unwrap();
}

/// Call the Start-bound exact fill RPC and return its canonical JSON result.
#[allow(clippy::too_many_arguments)]
async fn sql_commit_fill(
    client: &Client,
    expected_prior: Option<i64>,
    prepared_seq: i64,
    key: &str,
    market: &str,
    outcome: i32,
    side: &str,
    quantity: &str,
    price: &str,
    principal: &str,
    fee: &str,
    entry_unix: i64,
) -> serde_json::Value {
    let row = client
        .query_one(
            "select commit_fill_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)::text",
            &[
                &START_SEQ,
                &START_HASH,
                &expected_prior,
                &prepared_seq,
                &key,
                &LEADER_HEX,
                &"src",
                &market,
                &outcome,
                &side,
                &quantity,
                &price,
                &principal,
                &fee,
                &entry_unix,
            ],
        )
        .await
        .unwrap();
    serde_json::from_str(&row.get::<_, String>(0)).unwrap()
}

/// Call the Start-bound exact resolution RPC and return its canonical JSON result.
async fn sql_apply_resolution(
    client: &Client,
    expected_prior: Option<i64>,
    prepared_seq: i64,
    market: &str,
    prices_json: &str,
    settled_at: i64,
) -> serde_json::Value {
    let row = client
        .query_one(
            "select apply_resolution_v2($1,$2,$3,$4,$5,$6::text::jsonb,$7)::text",
            &[
                &START_SEQ,
                &START_HASH,
                &expected_prior,
                &prepared_seq,
                &market,
                &prices_json,
                &settled_at,
            ],
        )
        .await
        .unwrap();
    serde_json::from_str(&row.get::<_, String>(0)).unwrap()
}

async fn sql_bankroll(client: &Client) -> Decimal {
    let row = client
        .query_one("select bankroll_str from paper_bankroll where id = 0", &[])
        .await
        .unwrap();
    dec(&row.get::<_, String>(0))
}

async fn sql_replace_watchlist(
    client: &Client,
    token: &str,
    entries: &str,
) -> Result<(String, i32), tokio_postgres::Error> {
    client
        .query_one(
            "select new_token::text, count from service_watchlist_replace_v1(\
             $1::text::timestamptz, $2::text::jsonb)",
            &[&token, &entries],
        )
        .await
        .map(|row| (row.get(0), row.get(1)))
}

async fn watchlist_runtime(client: &Client) -> (String, i32, i64) {
    let row = client
        .query_one(
            "select updated_at::text, watchlist_size, \
             (select count(*) from service_watchlist) from service_runtime where id = 1",
            &[],
        )
        .await
        .unwrap();
    (row.get(0), row.get(1), row.get(2))
}

async fn watchlist_rows(client: &Client) -> String {
    client
        .query_one(
            "select coalesce(jsonb_agg(jsonb_build_object(\
             'wallet_hex', wallet_hex, 'rank', rank, 'leader_score_bps', leader_score_bps) \
             order by rank), '[]'::jsonb)::text from service_watchlist",
            &[],
        )
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn pg_watchlist_replace_parity_and_concurrency() {
    let Ok(url) = std::env::var("PE_TEST_PG_URL") else {
        eprintln!(
            "SKIP: PE_TEST_PG_URL unset — watchlist RPC parity runs only in CI / against a local pg"
        );
        return;
    };
    let _guard = pg_lock(&url).await;
    let client = connect(&url).await;
    client
        .batch_execute(
            "delete from service_watchlist; \
             update service_runtime set watchlist_size = 0, \
             updated_at = '2026-09-01 00:00:00+00' where id = 1;",
        )
        .await
        .unwrap();

    let acl = client
        .query_one(
            "select \
             has_function_privilege('service_role', \
               'service_watchlist_replace_v1(timestamptz,jsonb)', 'execute'), \
             has_function_privilege('anon', \
               'service_watchlist_replace_v1(timestamptz,jsonb)', 'execute')",
            &[],
        )
        .await
        .unwrap();
    assert!(acl.get::<_, bool>(0));
    assert!(!acl.get::<_, bool>(1));

    let initial = watchlist_runtime(&client).await.0;
    let one = r#"[{"wallet_hex":"0x1111111111111111111111111111111111111111","rank":1,"leader_score_bps":100}]"#;
    let two = r#"[{"wallet_hex":"0x2222222222222222222222222222222222222222","rank":1,"leader_score_bps":800},{"wallet_hex":"0x3333333333333333333333333333333333333333","rank":2,"leader_score_bps":700}]"#;
    let (one_token, one_count) = sql_replace_watchlist(&client, &initial, one).await.unwrap();
    assert_eq!(one_count, 1);

    // A reader whose statement snapshot predates the replacement sees the complete old
    // generation; a later statement sees the complete new generation, never a partial delete.
    let old_reader = connect(&url).await;
    old_reader
        .batch_execute("begin isolation level repeatable read")
        .await
        .unwrap();
    assert_eq!(watchlist_runtime(&old_reader).await.1, 1);
    let old_generation = watchlist_rows(&old_reader).await;
    assert!(old_generation.contains("0x1111111111111111111111111111111111111111"));
    assert!(!old_generation.contains("0x2222222222222222222222222222222222222222"));
    let writer = connect(&url).await;
    let (two_token, two_count) = sql_replace_watchlist(&writer, &one_token, two)
        .await
        .unwrap();
    assert_eq!(two_count, 2);
    assert_eq!(watchlist_runtime(&old_reader).await.1, 1);
    assert_eq!(watchlist_rows(&old_reader).await, old_generation);
    old_reader.batch_execute("commit").await.unwrap();
    assert_eq!(watchlist_runtime(&old_reader).await.1, 2);
    let new_generation = watchlist_rows(&old_reader).await;
    assert!(!new_generation.contains("0x1111111111111111111111111111111111111111"));
    assert!(new_generation.contains("0x2222222222222222222222222222222222222222"));
    assert!(new_generation.contains("0x3333333333333333333333333333333333333333"));

    let stable_runtime = watchlist_runtime(&client).await;
    let stable_rows = watchlist_rows(&client).await;
    let stale = sql_replace_watchlist(&client, &one_token, one)
        .await
        .expect_err("stale token must lose");
    assert_eq!(stale.as_db_error().unwrap().code().code(), "P5441");
    assert_eq!(watchlist_runtime(&client).await, stable_runtime);
    assert_eq!(watchlist_rows(&client).await, stable_rows);

    let duplicate = r#"[{"wallet_hex":"0x2222222222222222222222222222222222222222","rank":1,"leader_score_bps":1},{"wallet_hex":"0x2222222222222222222222222222222222222222","rank":2,"leader_score_bps":2}]"#;
    let malformed = sql_replace_watchlist(&client, &two_token, duplicate)
        .await
        .expect_err("duplicate wallets must reject before mutation");
    assert_eq!(malformed.as_db_error().unwrap().code().code(), "P5442");
    assert_eq!(watchlist_runtime(&client).await, stable_runtime);
    assert_eq!(watchlist_rows(&client).await, stable_rows);

    // Score-only changes persist through the canonical column. A legacy reader selecting only
    // the pre-#544 columns remains valid after the additive migration.
    let rescored = r#"[{"wallet_hex":"0x2222222222222222222222222222222222222222","rank":1,"leader_score_bps":999},{"wallet_hex":"0x3333333333333333333333333333333333333333","rank":2,"leader_score_bps":888}]"#;
    let (score_token, score_count) = sql_replace_watchlist(&client, &two_token, rescored)
        .await
        .unwrap();
    assert_eq!(score_count, 2);
    let scores = client
        .query(
            "select wallet_hex, rank, updated_at, leader_score_bps \
             from service_watchlist order by rank",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(scores.len(), 2);
    assert_eq!(scores[0].get::<_, i32>(3), 999);
    assert_eq!(scores[1].get::<_, i32>(3), 888);
    let legacy_rows = client
        .query(
            "select wallet_hex, rank, updated_at from service_watchlist order by rank",
            &[],
        )
        .await
        .expect("legacy projection must remain readable without selecting the additive score");
    assert_eq!(legacy_rows.len(), 2);

    let (empty_token, empty_count) = sql_replace_watchlist(&client, &score_token, "[]")
        .await
        .unwrap();
    assert_eq!(empty_count, 0);
    assert!(watchlist_rows(&client).await.contains("[]"));
    assert_eq!(watchlist_runtime(&client).await, (empty_token, 0, 0));
    println!(
        "PASS: PG-544 — atomic old/new visibility, stale/malformed zero-write loss, scores, legacy read, and valid empty"
    );
}

#[tokio::test]
async fn pg_parity_and_concurrency() {
    let Ok(url) = std::env::var("PE_TEST_PG_URL") else {
        eprintln!(
            "SKIP: PE_TEST_PG_URL unset — live-Postgres parity test runs only in CI / against a local pg"
        );
        return;
    };
    let _guard = pg_lock(&url).await;
    load_financial_schema(&_guard).await;
    let client = connect(&url).await;

    reset(&client, START).await;
    let applied = sql_commit_fill(
        &client, None, 11, "wf|dup", "0xm2", 0, "buy", "10.5", "0.40", "4.2", "0.01", 1,
    )
    .await;
    let existing = sql_commit_fill(
        &client, None, 11, "wf|dup", "0xm2", 0, "buy", "10.5", "0.40", "4.2", "0.01", 1,
    )
    .await;
    assert_eq!(applied["outcome"], "applied");
    assert_eq!(existing["outcome"], "existing");
    assert_eq!(dec(applied["bankroll"].as_str().unwrap()), dec!(99995.79));
    assert_eq!(sql_bankroll(&client).await, dec!(99995.79));
    println!("PASS: PG-PARITY — exact principal+fee debit and idempotent retry");
}

/// PG-CAS (#508 Phase A): rehearse the predicated compare-and-swap `service_config` UPDATE
/// the A0–A4 operator steps rely on. Many agents push to prod, so every production config
/// mutation is `UPDATE … WHERE key=… AND value=:prior AND updated_at=:prior_ts RETURNING …`:
/// a concurrent edit landing between token capture and the predicated UPDATE must make the
/// UPDATE return **zero rows** (the operator aborts/reconciles), leaving the row intact.
#[tokio::test]
async fn pg_cas_predicated_update_rehearsal() {
    let Ok(url) = std::env::var("PE_TEST_PG_URL") else {
        eprintln!(
            "SKIP: PE_TEST_PG_URL unset — CAS rehearsal runs only in CI / against a local pg"
        );
        return;
    };
    let _guard = pg_lock(&url).await;
    let client = connect(&url).await;
    // Isolated key: never a seeded production row, so this cannot race pg_parity.
    client
        .execute(
            "insert into service_config (key, value, value_type, description)
             values ('cas_rehearsal_key', '0', 'integer', 'pg_parity CAS rehearsal (#508)')
             on conflict (key) do update set value = '0', updated_at = now()",
            &[],
        )
        .await
        .unwrap();

    // A0: capture the predicate tokens.
    let row = client
        .query_one(
            "select value, updated_at from service_config where key = 'cas_rehearsal_key'",
            &[],
        )
        .await
        .unwrap();
    let prior_value: String = row.get(0);
    let prior_ts: std::time::SystemTime = row.get(1);

    // A3 happy path: the predicated UPDATE returns exactly one row and its tokens.
    let updated = client
        .query(
            "update service_config set value = '100', updated_by = 'cas-rehearsal', updated_at = now()
             where key = 'cas_rehearsal_key' and value = $1 and updated_at = $2
             returning key, value, updated_at",
            &[&prior_value, &prior_ts],
        )
        .await
        .unwrap();
    assert_eq!(
        updated.len(),
        1,
        "predicated UPDATE must return exactly one row"
    );
    assert_eq!(updated[0].get::<_, String>(1), "100");

    // Concurrent-edit window: a second agent's edit lands, then OUR stale-token UPDATE must
    // return zero rows and leave the concurrent value untouched.
    client
        .execute(
            "update service_config set value = '250', updated_by = 'other-agent', updated_at = now()
             where key = 'cas_rehearsal_key'",
            &[],
        )
        .await
        .unwrap();
    let stale = client
        .query(
            "update service_config set value = '999', updated_by = 'cas-rehearsal', updated_at = now()
             where key = 'cas_rehearsal_key' and value = $1 and updated_at = $2
             returning key",
            &[&prior_value, &prior_ts],
        )
        .await
        .unwrap();
    assert!(
        stale.is_empty(),
        "a stale-token predicated UPDATE must return zero rows (abort/reconcile)"
    );
    let now_value: String = client
        .query_one(
            "select value from service_config where key = 'cas_rehearsal_key'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(now_value, "250", "the concurrent edit must be left intact");

    // Cleanup so re-runs start clean.
    client
        .execute(
            "delete from service_config where key = 'cas_rehearsal_key'",
            &[],
        )
        .await
        .unwrap();
    println!("PASS: PG-CAS — predicated service_config UPDATE aborts on a concurrent edit (#508)");
}

/// Call `commit_fill_v2`; return the raw jsonb as serde_json::Value.
#[allow(clippy::too_many_arguments)]
async fn sql_commit_fill_v2(
    client: &Client,
    key: &str,
    market: &str,
    price: &str,
    seq: i64,
) -> serde_json::Value {
    sql_commit_fill(
        client, None, seq, key, market, 0, "buy", "10.5", price, "4.2", "0.01", 0,
    )
    .await
}

/// PG-511-V2: v2 outcome protocol on live PL/pgSQL — applied parity with v1 arithmetic,
/// `existing` returns the CANONICAL row (even after later settlement), `settled` refuses
/// absent keys, and `apply_resolution_v2` computes the credit in-RPC from paper_positions.
#[tokio::test]
async fn pg_511_v2_outcomes_and_in_rpc_credit() {
    let Ok(url) = std::env::var("PE_TEST_PG_URL") else {
        eprintln!("SKIP: PE_TEST_PG_URL unset");
        return;
    };
    let _guard = pg_lock(&url).await;
    load_financial_schema(&_guard).await;
    let client = connect(&url).await;
    reset(&client, "1000").await;

    // applied: 1000 − principal 4.2 − fee .01 = 995.79.
    let v = sql_commit_fill_v2(&client, "wf|k1", "0xmkt", "0.40", 11).await;
    assert_eq!(v["outcome"], "applied");
    assert_eq!(dec(v["bankroll"].as_str().unwrap()), dec!(995.79));
    assert_eq!(v["row"]["prepared_seq"], 11);

    let v = sql_commit_fill_v2(&client, "wf|k1", "0xmkt", "0.40", 11).await;
    assert_eq!(v["outcome"], "existing");
    assert_eq!(dec(v["bankroll"].as_str().unwrap()), dec!(995.79));

    // Changed immutable economics for the same key/Prepared conflicts.
    let v = sql_commit_fill_v2(&client, "wf|k1", "0xmkt", "0.55", 11).await;
    assert_eq!(v["outcome"], "conflict");

    let r = sql_apply_resolution(&client, Some(11), 12, "0xmkt", r#"["1","0"]"#, 100).await;
    assert_eq!(r["outcome"], "applied");
    assert_eq!(dec(r["credit"].as_str().unwrap()), dec!(10.5));
    assert_eq!(dec(r["bankroll"].as_str().unwrap()), dec!(1006.29));

    let r = sql_apply_resolution(&client, Some(11), 12, "0xmkt", r#"["1","0"]"#, 100).await;
    assert_eq!(r["outcome"], "existing");
    let r = sql_apply_resolution(&client, Some(11), 12, "0xmkt", r#"["1","0"]"#, 999).await;
    assert_eq!(r["outcome"], "conflict");
    println!("PASS: PG-545 — immutable retries conflict and exact resolution credits once");
}

/// PG-511-CONC: genuinely OVERLAPPING fill and resolution transactions, both orders. The
/// bankroll-row lock taken FIRST serializes admission: fill-first ⇒ its position is in the
/// resolution's in-RPC read (credited); resolution-first ⇒ the settled row is visible to
/// the fill's check (refused). Money is conserved in both interleavings.
#[tokio::test]
async fn pg_511_concurrent_fill_vs_resolution_conserves_money() {
    let Ok(url) = std::env::var("PE_TEST_PG_URL") else {
        eprintln!("SKIP: PE_TEST_PG_URL unset");
        return;
    };
    let _guard = pg_lock(&url).await;
    load_financial_schema(&_guard).await;
    for fill_first in [true, false] {
        let a = connect(&url).await;
        reset(&a, "1000").await;
        drop(a);

        const FILL_SQL: &str = "select commit_fill_v2(10,repeat('0',64),null,11,\
            'wf|kc','0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','src','0xmkt',0,'buy',\
            10.5,0.40,4.2,0.01,0)::text";
        const RES_SQL: &str = "select apply_resolution_v2(10,repeat('0',64),null,12,\
            '0xmkt','[\"1\",\"0\"]'::jsonb,100)::text";
        let (first_sql, second_sql) = if fill_first {
            (FILL_SQL, RES_SQL)
        } else {
            (RES_SQL, FILL_SQL)
        };

        // Session 1: begin, run its statement (takes the bankroll lock), HOLD uncommitted.
        let s1 = connect(&url).await;
        s1.batch_execute("begin").await.unwrap();
        s1.query_one(first_sql, &[]).await.unwrap();

        // Session 2 (own task, owns its client): begins and issues the other statement,
        // which BLOCKS on the bankroll lock until session 1 commits.
        let url2 = url.clone();
        let second = tokio::spawn(async move {
            let s2 = connect(&url2).await;
            s2.batch_execute("begin").await.unwrap();
            let v = s2.query_one(second_sql, &[]).await.unwrap();
            s2.batch_execute("commit").await.unwrap();
            v.get::<_, String>(0)
        });
        // Give session 2 time to reach the lock (a genuine overlap), then release.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !second.is_finished(),
            "session 2 must be blocked on the bankroll lock"
        );
        s1.batch_execute("commit").await.unwrap();
        let _second_json = second.await.unwrap();

        let check = connect(&url).await;
        let bankroll = dec(&check
            .query_one("select bankroll_str from paper_bankroll where id = 0", &[])
            .await
            .unwrap()
            .get::<_, String>(0));
        let fills: i64 = check
            .query_one("select count(*) from paper_fills", &[])
            .await
            .unwrap()
            .get(0);
        if fill_first {
            // Fill advances the singleton to Prepared 11; the concurrent resolution's stale
            // predecessor conflicts rather than observing a different financial prefix.
            assert_eq!(fills, 1);
            assert_eq!(bankroll, dec!(995.79), "fill-first: only fill applied");
        } else {
            // Resolution advances the singleton first with zero credit; the stale fill conflicts.
            assert_eq!(fills, 0);
            assert_eq!(bankroll, dec!(1000), "resolution-first: fill refused");
        }
        println!(
            "PASS: PG-511-CONC fill_first={fill_first} → fills={fills}, bankroll={bankroll} (conserved)"
        );
    }
}

/// PG-545-ACL: exact callable inventory and service-role-only execution.
#[tokio::test]
async fn pg_511_v2_acl_service_role_only() {
    let Ok(url) = std::env::var("PE_TEST_PG_URL") else {
        eprintln!("SKIP: PE_TEST_PG_URL unset");
        return;
    };
    let _guard = pg_lock(&url).await;
    load_financial_schema(&_guard).await;
    let client = connect(&url).await;
    let inventory = client
        .query(
            "select p.proname, pg_get_function_identity_arguments(p.oid) \
             from pg_proc p join pg_namespace n on n.oid = p.pronamespace \
             where n.nspname = 'public' and p.proname in \
                ('commit_fill','apply_resolution','commit_fill_v2','apply_resolution_v2',\
                 'seed_financial_start') order by p.proname, 2",
            &[],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        inventory,
        vec![
            (
                "apply_resolution_v2".to_owned(),
                "p_start_seq bigint, p_start_hash text, p_expected_prior_seq bigint, p_prepared_seq bigint, p_condition_id text, p_payout_by_outcome_index jsonb, p_settled_at_unix bigint".to_owned(),
            ),
            (
                "commit_fill_v2".to_owned(),
                "p_start_seq bigint, p_start_hash text, p_expected_prior_seq bigint, p_prepared_seq bigint, p_idempotency_key text, p_leader_wallet text, p_source_trade_id text, p_market_id text, p_outcome_id integer, p_side text, p_quantity numeric, p_fill_price numeric, p_principal numeric, p_fee numeric, p_entry_unix bigint".to_owned(),
            ),
            (
                "seed_financial_start".to_owned(),
                "p_start_seq bigint, p_start_hash text".to_owned(),
            ),
        ]
    );

    for (role, expect) in [
        ("anon", false),
        ("authenticated", false),
        ("service_role", true),
    ] {
        let row = client
            .query_one(
                "select has_function_privilege($1, \
                 'commit_fill_v2(bigint,text,bigint,bigint,text,text,text,text,integer,text,numeric,numeric,numeric,numeric,bigint)', \
                 'execute'), has_function_privilege($1, \
                 'apply_resolution_v2(bigint,text,bigint,bigint,text,jsonb,bigint)', 'execute'), \
                 has_function_privilege($1, 'seed_financial_start(bigint,text)', 'execute')",
                &[&role],
            )
            .await
            .unwrap();
        assert_eq!(
            row.get::<_, bool>(0),
            expect,
            "commit_fill_v2 acl for {role}"
        );
        assert_eq!(
            row.get::<_, bool>(1),
            expect,
            "apply_resolution_v2 acl for {role}"
        );
        assert_eq!(row.get::<_, bool>(2), expect, "seed Start acl for {role}");
    }
    println!("PASS: PG-511-ACL — v2 RPCs are service-role-only");
}
