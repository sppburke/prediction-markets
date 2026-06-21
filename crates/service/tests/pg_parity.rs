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
