//! Scenario tests for the issue #176 delta-backfill orchestration.
//!
//! These tests use the in-memory `InMemoryChainLogFetcher` stub so no real
//! Polygon RPC is required. They exercise the trait-generic entry point
//! `run_delta_scan_with_fetcher` to validate the scanner's contract, and
//! the cache integration to validate cursor + audit persistence.
//!
//! Determinism: fixture data is generated in-process; tests do not call
//! `OffsetDateTime::now_utc()` for assertion-critical values; `tempfile`
//! gives each test an isolated SQLite file under `target/`.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;

use alloy::primitives::{Address, B256, Bytes, LogData};
use alloy::rpc::types::Log;
use pe_bootstrap::backfill::run_delta_scan_with_fetcher;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::polygon_ctf_delta::POLYGON_CTF_BACKFILL_CURSOR_KEY;
use pe_bootstrap::polygon_ctf_delta::test_support::InMemoryChainLogFetcher;
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::contracts::TOPIC_ORDER_FILLED;
use rusqlite::params;
use tempfile::TempDir;

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

fn wallet_topic(byte: u8) -> B256 {
    let mut b = [0u8; 32];
    b[31] = byte;
    B256::from(b)
}

fn order_filled_log(maker: u8, taker: u8) -> Log {
    let order_hash = B256::repeat_byte(0xaa);
    let inner = alloy::primitives::Log {
        address: Address::ZERO,
        data: LogData::new_unchecked(
            vec![
                TOPIC_ORDER_FILLED,
                order_hash,
                wallet_topic(maker),
                wallet_topic(taker),
            ],
            Bytes::from(vec![0u8; 32]),
        ),
    };
    Log {
        inner,
        ..Default::default()
    }
}

fn make_wallet(byte: u8) -> WalletAddress {
    let mut b = [0u8; 20];
    b[19] = byte;
    WalletAddress(b)
}

// ── Scenario 1 ───────────────────────────────────────────────────────────────
// PASS criterion: delta scan success populates `active_wallets`, returns a
// `Some(new_cursor)`, and no error is logged.

#[tokio::test]
async fn scenario_scan_success_populates_active_wallets() {
    let (_dir, cache) = open_cache();
    let logs = vec![order_filled_log(0x01, 0x02), order_filled_log(0x03, 0x04)];
    let fetcher = InMemoryChainLogFetcher::ok(1_000_000, logs);
    let (active, cursor) = run_delta_scan_with_fetcher(&fetcher, 256, &cache).await;
    // Expected `to_block` = 1_000_000 - 256 = 999_744; we don't assert that
    // here (it's a unit-test concern); we assert "set has 4 wallets, cursor
    // is Some".
    assert_eq!(active.len(), 4);
    assert!(cursor.is_some());
    let target = cursor.unwrap();
    assert_eq!(target, 999_744);
}

// ── Scenario 2 ───────────────────────────────────────────────────────────────
// PASS criterion: scan failure (RPC down) returns empty set + `None` cursor.
// Caller falls back to legacy full-fetch.

#[tokio::test]
async fn scenario_rpc_failure_returns_none_cursor() {
    let (_dir, cache) = open_cache();
    let fetcher = InMemoryChainLogFetcher::block_number_err("rpc unreachable");
    let (active, cursor) = run_delta_scan_with_fetcher(&fetcher, 256, &cache).await;
    assert!(active.is_empty());
    assert!(cursor.is_none());
}

// ── Scenario 3 ───────────────────────────────────────────────────────────────
// PASS criterion: cursor persistence — after a successful scan, writing the
// new_cursor to source_cursor + re-reading produces the same value.

#[tokio::test]
async fn scenario_cursor_persistence() {
    let (_dir, mut cache) = open_cache();
    let fetcher = InMemoryChainLogFetcher::ok(2_000_000, Vec::new());
    let (_, cursor) = run_delta_scan_with_fetcher(&fetcher, 256, &cache).await;
    let target = cursor.unwrap();
    cache
        .set_source_cursor(POLYGON_CTF_BACKFILL_CURSOR_KEY, &target.to_string())
        .unwrap();
    // Round-trip
    let read_back: u64 = cache
        .get_source_cursor(POLYGON_CTF_BACKFILL_CURSOR_KEY)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(read_back, target);
    assert_eq!(read_back, 2_000_000 - 256);
}

// ── Scenario 4 ───────────────────────────────────────────────────────────────
// PASS criterion: delta scanner correctly extracts ALL distinct maker/taker
// addresses from a multi-log batch (no losses, no duplicates).

#[tokio::test]
async fn scenario_dedup_across_logs() {
    let (_dir, cache) = open_cache();
    let logs = vec![
        order_filled_log(0x10, 0x11),
        order_filled_log(0x11, 0x10), // same pair, swapped
        order_filled_log(0x10, 0x12), // 0x10 again as maker, 0x12 new
    ];
    let fetcher = InMemoryChainLogFetcher::ok(1_000_000, logs);
    let (active, _) = run_delta_scan_with_fetcher(&fetcher, 256, &cache).await;
    // Distinct addresses: {0x10, 0x11, 0x12}
    let expected: HashSet<WalletAddress> =
        [make_wallet(0x10), make_wallet(0x11), make_wallet(0x12)]
            .into_iter()
            .collect();
    assert_eq!(active, expected);
}

// ── Scenario 5 ───────────────────────────────────────────────────────────────
// PASS criterion: `delta_audit` table is reachable AND idempotent on
// `(run_at_unix, wallet_hex)` PK. Insert a fixed row twice; only one persists.

#[tokio::test]
async fn scenario_audit_table_idempotent_pk() {
    let (_dir, mut cache) = open_cache();
    let run_at_unix: i64 = 1_700_000_000;
    let hex = make_wallet(0x42).to_string();
    let rows = vec![
        (run_at_unix, hex.clone(), "DELTA_HIT", 5_i64),
        (run_at_unix, hex.clone(), "DELTA_HIT", 99_i64), // duplicate PK
    ];
    cache.insert_delta_audit_rows(&rows).unwrap();
    let count: i64 = cache
        .raw_conn_for_test()
        .query_row(
            "SELECT COUNT(*) FROM delta_audit WHERE wallet_hex = ?1",
            params![hex],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(count, 1, "PK conflict must be a silent no-op");
    let stored_count: i64 = cache
        .raw_conn_for_test()
        .query_row(
            "SELECT new_trades_fetched FROM delta_audit WHERE wallet_hex = ?1",
            params![hex],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(stored_count, 5, "first insert wins");
}

// ── Scenario 6 ───────────────────────────────────────────────────────────────
// PASS criterion: `wallets_due_for_full_fetch` selects wallets with NULL
// `last_polymarket_full_at` or stale beyond the staleness window — exact
// shape the issue #176 paranoia backstop relies on.

#[tokio::test]
async fn scenario_paranoia_backstop_selects_stale_wallets() {
    use pe_bootstrap::pile;

    let (_dir, mut cache) = open_cache();
    let now: i64 = 1_700_000_000;
    let staleness: i64 = 604_800; // 7 days

    let fresh = format!("0x{:040x}", 0x11);
    let stale = format!("0x{:040x}", 0x22);
    let never = format!("0x{:040x}", 0x33);

    // All three wallets are active.
    cache
        .upsert_wallet(&fresh, pile::SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    cache
        .upsert_wallet(&stale, pile::SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    cache
        .upsert_wallet(&never, pile::SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    pile::apply_activation_rules(&mut cache).unwrap();

    // fresh: stamped within window. stale: stamped > 7d ago. never: NULL.
    cache
        .update_last_polymarket_full_at(&fresh, now - 3 * 86_400)
        .unwrap();
    cache
        .update_last_polymarket_full_at(&stale, now - 8 * 86_400)
        .unwrap();

    let due = pile::select_full_fetch_due(&cache, now, staleness).unwrap();
    let due_set: HashSet<String> = due.into_iter().collect();
    assert!(!due_set.contains(&fresh), "fresh wallet must NOT be due");
    assert!(due_set.contains(&stale), "stale wallet must be due");
    assert!(due_set.contains(&never), "NULL-stamp wallet must be due");
}

// ── Scenario 7 ───────────────────────────────────────────────────────────────
// PASS criterion: `insert_new` returns the EXACT count of newly inserted
// rows under `INSERT OR IGNORE` — duplicates do not inflate the count. This
// is the foundation for `FetchOutcome::new_trades` accuracy in the
// delta-audit pipeline.

#[tokio::test]
async fn scenario_insert_new_returns_exact_inserted_count() {
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId,
        VenueMarketId,
    };
    use pe_trader_index::snapshot::RawTrade;
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    let (_dir, mut cache) = open_cache();
    let wallet = make_wallet(0xab);
    let make_trade = |id: &str, ts: i64| RawTrade {
        wallet,
        source_trade_id: SourceTradeId(id.to_owned()),
        market_id: MarketId(VenueMarketId(format!("0x{:064x}", 1))),
        outcome_id: OutcomeId(0u16),
        side: Side::Buy,
        price: Price(dec!(0.5)),
        contracts: ContractQty(1_000),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
    };

    // First call: 3 brand-new trades → returns 3.
    let trades = vec![
        make_trade("t1", 1_000_000),
        make_trade("t2", 1_000_001),
        make_trade("t3", 1_000_002),
    ];
    let inserted = cache.insert_new(&wallet.to_string(), trades).unwrap();
    assert_eq!(inserted, 3);

    // Second call with same trades + 1 new → returns 1 (only the new one).
    let trades2 = vec![
        make_trade("t1", 1_000_000), // dup
        make_trade("t2", 1_000_001), // dup
        make_trade("t3", 1_000_002), // dup
        make_trade("t4", 1_000_003), // NEW
    ];
    let inserted2 = cache.insert_new(&wallet.to_string(), trades2).unwrap();
    assert_eq!(inserted2, 1);

    // Third call with all dups → returns 0.
    let trades3 = vec![make_trade("t1", 1_000_000)];
    let inserted3 = cache.insert_new(&wallet.to_string(), trades3).unwrap();
    assert_eq!(inserted3, 0);
}
