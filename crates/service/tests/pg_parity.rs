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
//!   PE_TEST_PG_URL="$URL" cargo nextest run -p pe-service --features scenario pg_parity
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

use pe_core_types::{
    EventSeq, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_paper_state::{FillRecord, LeaderPositionRow, PaperStateDb};
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

const LEADER_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const START: &str = "100000";

const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

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
            "insert into paper_bankroll (id, bankroll_str) values (0, $1)",
            &[&start],
        )
        .await
        .unwrap();
}

/// Call the SQL `commit_fill` RPC; return the new bankroll.
#[allow(clippy::too_many_arguments)]
async fn sql_commit_fill(
    client: &Client,
    key: &str,
    market: &str,
    outcome: i32,
    side: &str,
    contracts: i64,
    price: &str,
    seq: i64,
) -> Decimal {
    let row = client
        .query_one(
            "select commit_fill($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
            &[
                &key,
                &LEADER_HEX,
                &"src",
                &market,
                &outcome,
                &side,
                &contracts,
                &price,
                &0i64,
                &seq,
            ],
        )
        .await
        .unwrap();
    dec(&row.get::<_, String>(0))
}

/// Call the SQL `apply_resolution` RPC (jsonb passed as text + `::jsonb` cast); new bankroll.
async fn sql_apply_resolution(
    client: &Client,
    market: &str,
    prices_json: &str,
    credit: &str,
    settled_at: i64,
) -> Decimal {
    let row = client
        .query_one(
            // `$2::text::jsonb`: infer $2 as text (we send the JSON as a string), then cast to
            // jsonb. A bare `$2::jsonb` makes Postgres infer $2 itself as jsonb → a type error.
            "select apply_resolution($1, $2::text::jsonb, $3, $4)",
            &[&market, &prices_json, &credit, &settled_at],
        )
        .await
        .unwrap();
    dec(&row.get::<_, String>(0))
}

async fn sql_bankroll(client: &Client) -> Decimal {
    let row = client
        .query_one("select bankroll_str from paper_bankroll where id = 0", &[])
        .await
        .unwrap();
    dec(&row.get::<_, String>(0))
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
    let client = connect(&url).await;

    // ── PG-PARITY: SQL commit_fill == PaperStateDb::commit_fill over a fill mix ──────────
    reset(&client, START).await;
    let dir = tempfile::tempdir().unwrap();
    let db = PaperStateDb::open(&dir.path().join("p.db")).unwrap();
    db.init_bankroll(dec(START)).unwrap();
    let market = MarketId(VenueMarketId("0xmkt".to_string()));
    let leader = LeaderPositionRow {
        wallet: WalletAddress::from_hex(LEADER_HEX).unwrap(),
        market_id: market.clone(),
        outcome_id: OutcomeId(0),
        long_contracts: 0,
        short_contracts: 0,
    };
    // buy, sell (trims long), buy — exercises both bankroll directions and net positioning.
    let fills = [
        ("k1", Side::Buy, 10u64, dec!(0.40), 1i64),
        ("k2", Side::Sell, 4, dec!(0.60), 2),
        ("k3", Side::Buy, 7, dec!(0.55), 3),
    ];
    for (key, side, contracts, price, seq) in fills {
        let record = FillRecord {
            idempotency_key: key.to_string(),
            market_id: market.clone(),
            outcome_id: OutcomeId(0),
            side,
            contracts,
            fill_price: Price(price),
        };
        let rust_bankroll = db
            .commit_fill(
                &SourceTradeId(format!("s{seq}")),
                &leader,
                &record,
                EventSeq(seq as u64),
            )
            .unwrap();
        let pe_paper_state::FillCommitOutcome::Applied(rust_bankroll) = rust_bankroll else {
            panic!("parity fixture never settles markets mid-mix");
        };
        let pg_bankroll = sql_commit_fill(
            &client,
            key,
            "0xmkt",
            0,
            side_str(side),
            contracts as i64,
            &price.to_string(),
            seq,
        )
        .await;
        assert_eq!(
            pg_bankroll, rust_bankroll,
            "PG-PARITY bankroll mismatch at seq {seq}"
        );
    }
    // Position parity too: the SQL net position must equal PaperStateDb's.
    let pos_row = client
        .query_one(
            "select long_contracts, short_contracts from paper_positions \
             where market_id = '0xmkt' and outcome_id = 0",
            &[],
        )
        .await
        .unwrap();
    let (pg_long, pg_short): (i64, i64) = (pos_row.get(0), pos_row.get(1));
    let rust_pos = db.paper_positions().unwrap();
    assert_eq!(rust_pos.len(), 1);
    assert_eq!(
        (pg_long as u64, pg_short as u64),
        (rust_pos[0].long_contracts, rust_pos[0].short_contracts),
        "PG-PARITY position mismatch"
    );
    println!("PASS: PG-PARITY — SQL commit_fill bankroll + position match PaperStateDb");

    // ── PG-AC2: a duplicate idempotency_key debits once ─────────────────────────────────
    reset(&client, START).await;
    let b1 = sql_commit_fill(&client, "dup", "0xm2", 0, "buy", 10, "0.40", 1).await;
    let b2 = sql_commit_fill(&client, "dup", "0xm2", 0, "buy", 10, "0.40", 1).await;
    assert_eq!(b1, dec!(99996.0));
    assert_eq!(b2, b1, "PG-AC2: duplicate idempotency_key must debit once");
    println!("PASS: PG-AC2 — duplicate idempotency_key debited once ({b1})");

    // ── PG-AC8: a duplicate apply_resolution market credits once ────────────────────────
    reset(&client, START).await;
    let r1 = sql_apply_resolution(&client, "0xres", "[\"1\",\"0\"]", "25", 1_700_000_000).await;
    let r2 = sql_apply_resolution(&client, "0xres", "[\"1\",\"0\"]", "25", 1_700_000_999).await;
    assert_eq!(r1, dec!(100025));
    assert_eq!(r2, r1, "PG-AC8: duplicate market must credit once");
    println!("PASS: PG-AC8 — duplicate apply_resolution credited once ({r1})");

    // ── PG-AC9: concurrent debits + credits lose no update (the B-E self-ref UPDATE) ────
    // K connections each run M (debit 1.00, credit 3.00) pairs, all contending the single
    // paper_bankroll row. Asymmetric amounts (1 vs 3) make any lost update visible in the
    // total. Correct (row-locked self-ref UPDATE) → final == START − K·M·1 + K·M·3.
    reset(&client, START).await;
    const K: i64 = 8;
    const M: i64 = 25;
    let mut tasks = Vec::new();
    for t in 0..K {
        let url = url.clone();
        tasks.push(tokio::spawn(async move {
            let c = connect(&url).await;
            for i in 0..M {
                sql_commit_fill(
                    &c,
                    &format!("d{t}_{i}"),
                    &format!("0xd{t}_{i}"),
                    0,
                    "buy",
                    1,
                    "1.00",
                    i,
                )
                .await;
                sql_apply_resolution(&c, &format!("0xc{t}_{i}"), "[\"1\"]", "3.00", i).await;
            }
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let expected = dec(START) + Decimal::from(K * M * 2); // −1 +3 per pair = +2
    let final_bankroll = sql_bankroll(&client).await;
    assert_eq!(
        final_bankroll,
        expected,
        "PG-AC9: lost update — {} concurrent debit/credit pairs did not all apply (got {final_bankroll}, want {expected})",
        K * M
    );
    println!(
        "PASS: PG-AC9 — {} concurrent debit/credit pairs across {K} connections, no lost update (final {final_bankroll})",
        K * M
    );
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
    let row = client
        .query_one(
            "select commit_fill_v2($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)::text",
            &[
                &key,
                &LEADER_HEX,
                &"src",
                &market,
                &0i32,
                &"buy",
                &10i64,
                &price,
                &0i64,
                &seq,
            ],
        )
        .await
        .unwrap();
    serde_json::from_str(&row.get::<_, String>(0)).unwrap()
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
    let client = connect(&url).await;
    reset(&client, "1000").await;

    // applied: 1000 − 10×0.40 = 996; row carries the canonical fields.
    let v = sql_commit_fill_v2(&client, "wf|k1", "0xmkt", "0.40", 0).await;
    assert_eq!(v["outcome"], "applied");
    assert_eq!(dec(v["bankroll"].as_str().unwrap()), dec!(996.0));
    assert_eq!(v["row"]["event_seq"], 0);

    // existing: same key, re-priced retry at a NEW seq → canonical (original) row wins.
    let v = sql_commit_fill_v2(&client, "wf|k1", "0xmkt", "0.55", 7).await;
    assert_eq!(v["outcome"], "existing");
    assert_eq!(v["row"]["fill_price"].as_str().unwrap(), "0.40");
    assert_eq!(v["row"]["event_seq"], 0);
    assert_eq!(dec(v["bankroll"].as_str().unwrap()), dec!(996.0));

    // resolution v2: credit computed IN-RPC from paper_positions (10 long × 1 = 10).
    let row = client
        .query_one(
            "select apply_resolution_v2($1,$2::text::jsonb,$3)::text",
            &[&"0xmkt", &r#"["1","0"]"#, &100i64],
        )
        .await
        .unwrap();
    let r: serde_json::Value = serde_json::from_str(&row.get::<_, String>(0)).unwrap();
    assert_eq!(r["outcome"], "applied");
    assert_eq!(r["credit"].as_str().unwrap(), "10");
    assert_eq!(dec(r["bankroll"].as_str().unwrap()), dec!(1006.0));

    // idempotent retry: existing + canonical recorded values, no double credit.
    let row = client
        .query_one(
            "select apply_resolution_v2($1,$2::text::jsonb,$3)::text",
            &[&"0xmkt", &r#"["1","0"]"#, &999i64],
        )
        .await
        .unwrap();
    let r: serde_json::Value = serde_json::from_str(&row.get::<_, String>(0)).unwrap();
    assert_eq!(r["outcome"], "existing");
    assert_eq!(r["credit"].as_str().unwrap(), "10");
    assert_eq!(r["settled_at_unix"], 100);
    assert_eq!(dec(r["bankroll"].as_str().unwrap()), dec!(1006.0));

    // settled refusal: a NEW key into the settled market is refused, nothing inserted.
    let v = sql_commit_fill_v2(&client, "wf|k2", "0xmkt", "0.30", 8).await;
    assert_eq!(v["outcome"], "settled");
    assert!(v["row"].is_null());
    assert_eq!(dec(v["bankroll"].as_str().unwrap()), dec!(1006.0));
    // existing STILL wins over settlement (ordering).
    let v = sql_commit_fill_v2(&client, "wf|k1", "0xmkt", "0.99", 9).await;
    assert_eq!(v["outcome"], "existing");
    println!("PASS: PG-511-V2 — applied/existing/settled + in-RPC credit on live PL/pgSQL");
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
    for fill_first in [true, false] {
        let a = connect(&url).await;
        reset(&a, "1000").await;
        drop(a);

        const FILL_SQL: &str =
            "select commit_fill_v2('wf|kc','0xled','src','0xmkt',0,'buy',10,'0.40',0,0)::text";
        const RES_SQL: &str =
            "select apply_resolution_v2('0xmkt','[\"1\",\"0\"]'::jsonb,100)::text";
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
            // Fill held the lock first ⇒ applied (996), and the blocked resolution then
            // saw its position ⇒ credited 10 ⇒ 1006. The fill can NEVER be settled-over.
            assert_eq!(fills, 1);
            assert_eq!(bankroll, dec!(1006.0), "fill-first: applied AND credited");
        } else {
            // Resolution held the lock first ⇒ settled with zero positions; the blocked
            // fill then saw the settled row ⇒ refused. No debit, no credit, no orphan.
            assert_eq!(fills, 0);
            assert_eq!(bankroll, dec!(1000), "resolution-first: fill refused");
        }
        println!(
            "PASS: PG-511-CONC fill_first={fill_first} → fills={fills}, bankroll={bankroll} (conserved)"
        );
    }
}

/// PG-511-ACL: the v2 RPCs are service-role-only (public/anon/authenticated revoked).
#[tokio::test]
async fn pg_511_v2_acl_service_role_only() {
    let Ok(url) = std::env::var("PE_TEST_PG_URL") else {
        eprintln!("SKIP: PE_TEST_PG_URL unset");
        return;
    };
    let _guard = pg_lock(&url).await;
    let client = connect(&url).await;
    for (role, expect) in [
        ("anon", false),
        ("authenticated", false),
        ("service_role", true),
    ] {
        let row = client
            .query_one(
                "select has_function_privilege($1, \
                 'commit_fill_v2(text,text,text,text,integer,text,bigint,text,bigint,bigint)', \
                 'execute'), has_function_privilege($1, \
                 'apply_resolution_v2(text,jsonb,bigint)', 'execute')",
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
    }
    println!("PASS: PG-511-ACL — v2 RPCs are service-role-only");
}
