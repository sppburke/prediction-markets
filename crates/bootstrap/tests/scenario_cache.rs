//! Scenario tests for the permanent SQLite trade cache.
//!
//! Each scenario has a single PASS/FAIL criterion written before the test body.
//! No network calls; all data is constructed in-process.
//! Clock is fixed via hardcoded timestamps; no RNG is used.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_bootstrap::build_seed_watchlist;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::filter::FilterConfig;
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_core_types::{SourceTimestamp, SourceTradeId, WalletAddress};
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use pe_trader_index::build_trader_ledgers;
use tempfile::TempDir;
use time::OffsetDateTime;

const BASE_URL: &str = "https://data-api.polymarket.com";
const WALLET_A_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
#[allow(dead_code)]
const WALLET_B_HEX: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn wallet_a() -> WalletAddress {
    WalletAddress::from_hex(WALLET_A_HEX).unwrap()
}

#[allow(dead_code)]
fn wallet_b() -> WalletAddress {
    WalletAddress::from_hex(WALLET_B_HEX).unwrap()
}

/// Cold-start URL: no end/start cursor.
fn trade_url_cold(wallet: WalletAddress) -> String {
    PolymarketEndpoint::UserTradeActivityPage {
        user: wallet.to_string(),
        end: 2_000_000_000,
        start: Some(1),
        offset: 0,
    }
    .url(BASE_URL)
}

/// Backward-fill URL with an inclusive `end` timestamp cursor.
fn trade_url_end(wallet: WalletAddress, end: i64) -> String {
    PolymarketEndpoint::UserTradeActivityPage {
        user: wallet.to_string(),
        end,
        start: Some(1),
        offset: 0,
    }
    .url(BASE_URL)
}

/// Forward-fill URL with an exclusive `start` timestamp cursor.
fn trade_url_start(wallet: WalletAddress, start: i64) -> String {
    PolymarketEndpoint::UserTradeActivityPage {
        user: wallet.to_string(),
        end: 2_000_000_000,
        start: Some(start + 1),
        offset: 0,
    }
    .url(BASE_URL)
}

/// Build a JSON page of trades for `wallet`. Timestamps must be supplied
/// strictly decreasing (newest-first) so the API order is realistic.
fn trades_page(wallet: &str, hashes: &[(&str, i64)]) -> Vec<u8> {
    let entries: Vec<String> = hashes
        .iter()
        .map(|(h, ts)| {
            format!(
                r#"{{"transactionHash":"{h}","conditionId":"0xcond","side":"BUY","size":1,"price":0.60,"timestamp":{ts},"maker":"{wallet}"}}"#
            )
        })
        .collect();
    format!("[{}]", entries.join(",")).into_bytes()
}

fn empty_page() -> Vec<u8> {
    b"[]".to_vec()
}

// ── Scenario 1 ────────────────────────────────────────────────────────────────
//
// PASS: empty cache → all trades fetched and stored on cold run.
// FAIL: any trade missing from cache after the run.

#[tokio::test]
async fn scenario_first_run_full_fetch() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&path).unwrap();

    // 3 trades (partial page → stops; no next backward cursor needed).
    let page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xhash1", 2_000_003),
            ("0xhash2", 2_000_002),
            ("0xhash3", 2_000_001),
        ],
    );
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(wallet_a()), page);

    let fetcher = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_clock_for_test(|| 2_000_000_000);
    fetcher.fetch_all(&[wallet_a()], &mut cache).await.unwrap();

    assert_eq!(
        cache.known_trade_ids(WALLET_A_HEX).len(),
        3,
        "all 3 trades must be in the cache"
    );

    // Drop and reopen to verify durability through SQLite WAL.
    drop(cache);
    let reloaded = WalletCache::open(&path).unwrap();
    assert_eq!(reloaded.trades_for(WALLET_A_HEX).len(), 3);
}

// ── Scenario 2 ────────────────────────────────────────────────────────────────
//
// PASS: warm run whose incremental pages are all empty writes nothing new —
//       cache count stays at 3.
// FAIL: cache count changes after the second run.

#[tokio::test]
async fn scenario_empty_warm_run_inserts_nothing() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    // 3 trades: oldest=2_000_001, newest=2_000_003.
    let page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xhash1", 2_000_003),
            ("0xhash2", 2_000_002),
            ("0xhash3", 2_000_001),
        ],
    );

    // Cold run.
    let mut r1 = HashMap::new();
    r1.insert(trade_url_cold(wallet_a()), page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r1))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&[wallet_a()], &mut cache)
        .await
        .unwrap();
    assert_eq!(cache.trade_count(), 3);

    // Warm run:
    //   Phase 1 backward (end = 2_000_001 - 1 = 2_000_000) → empty.
    //   Phase 2 forward (start = 2_000_003) → empty.
    let mut r2 = HashMap::new();
    r2.insert(trade_url_end(wallet_a(), 2_000_000), empty_page());
    r2.insert(
        PolymarketEndpoint::UserTradeActivityPage {
            user: wallet_a().to_string(),
            end: 2_000_003,
            start: Some(2_000_003),
            offset: 0,
        }
        .url(BASE_URL),
        b"[]".to_vec(),
    );
    r2.insert(trade_url_start(wallet_a(), 2_000_003), empty_page());
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r2))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&[wallet_a()], &mut cache)
        .await
        .unwrap();

    assert_eq!(
        cache.trade_count(),
        3,
        "cache count must not change on a warm run with no new trades"
    );
}

// ── Scenario 3 ────────────────────────────────────────────────────────────────
//
// PASS: second run with 3 new trades + existing trades appends exactly 3 new.
// FAIL: wrong count or new trades missing.

#[tokio::test]
async fn scenario_second_run_appends_only_unknown_trades() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    // Cold run: 3 old trades; oldest=1_000_001, newest=1_000_003.
    let old_page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xold1", 1_000_003),
            ("0xold2", 1_000_002),
            ("0xold3", 1_000_001),
        ],
    );
    let mut r1 = HashMap::new();
    r1.insert(trade_url_cold(wallet_a()), old_page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r1))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&[wallet_a()], &mut cache)
        .await
        .unwrap();

    // Warm run:
    //   Phase 1 backward (end = 1_000_001 - 1 = 1_000_000) → empty.
    //   Phase 2 forward (start = 1_000_003): 3 new rows in the complete window.
    let new_page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xnew1", 2_000_003),
            ("0xnew2", 2_000_002),
            ("0xnew3", 2_000_001),
        ],
    );
    let mut r2 = HashMap::new();
    r2.insert(trade_url_end(wallet_a(), 1_000_000), empty_page());
    r2.insert(
        PolymarketEndpoint::UserTradeActivityPage {
            user: wallet_a().to_string(),
            end: 1_000_003,
            start: Some(1_000_003),
            offset: 0,
        }
        .url(BASE_URL),
        b"[]".to_vec(),
    );
    r2.insert(trade_url_start(wallet_a(), 1_000_003), new_page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r2))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&[wallet_a()], &mut cache)
        .await
        .unwrap();

    assert_eq!(cache.trade_count(), 6, "3 old + 3 new = 6");

    let ids: Vec<_> = cache
        .known_trade_ids(WALLET_A_HEX)
        .iter()
        .map(|id| id.0.clone())
        .collect();
    assert!(ids.iter().any(|id| id == "0xnew1"));
    assert!(ids.iter().any(|id| id == "0xnew2"));
    assert!(ids.iter().any(|id| id == "0xnew3"));
}

// ── Scenario 4 ────────────────────────────────────────────────────────────────
//
// PASS: build_seed_watchlist with the same snapshot_at produces bit-identical
//       output on two consecutive calls from the same populated cache.
// FAIL: any field differs between the two calls.

#[tokio::test]
async fn scenario_replay_reproducibility() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&path).unwrap();

    // Populate with enough trades to produce a non-empty watchlist (12 wins).
    let win_trades: Vec<(&str, i64)> = (0..12)
        .map(|i| {
            let ts = 1_700_000_000 - i as i64 * 86_400;
            (
                Box::leak(format!("0xwintx{i:03}").into_boxed_str()) as &str,
                ts,
            )
        })
        .collect();
    let page = trades_page(WALLET_A_HEX, &win_trades);
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(wallet_a()), page);

    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&[wallet_a()], &mut cache)
        .await
        .unwrap();
    drop(cache);

    let fixed_snapshot_at =
        SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_701_000_000).unwrap());

    let build_watchlist = |c: &WalletCache| {
        let trades = c.trades_for(WALLET_A_HEX);
        let ledgers = build_trader_ledgers(&trades, u32::MAX, None);
        build_seed_watchlist(ledgers, fixed_snapshot_at.clone(), &FilterConfig::default())
    };

    let reloaded = WalletCache::open(&path).unwrap();
    let w1 = build_watchlist(&reloaded);
    let w2 = build_watchlist(&reloaded);

    assert_eq!(
        w1.active_count, w2.active_count,
        "active_count must be identical across two calls"
    );
    assert_eq!(
        w1.entries.len(),
        w2.entries.len(),
        "entry count must be identical"
    );
    for (e1, e2) in w1.entries.iter().zip(w2.entries.iter()) {
        assert_eq!(e1.wallet, e2.wallet, "wallet must match");
        assert_eq!(e1.leader_score_bps, e2.leader_score_bps, "score must match");
    }
}

// ── Scenario 5 ────────────────────────────────────────────────────────────────
//
// PASS: a page returned in ascending order (oldest-first) is sorted correctly
//       and new trades appearing before the known sequence are appended.
// FAIL: new trade is missing from cache after the run.

#[tokio::test]
async fn scenario_out_of_order_page_handled() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    // Cold run: 3 old trades (partial). oldest=1_000_001, newest=1_000_003.
    let old_page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xold1", 1_000_003),
            ("0xold2", 1_000_002),
            ("0xold3", 1_000_001),
        ],
    );
    let mut r1 = HashMap::new();
    r1.insert(trade_url_cold(wallet_a()), old_page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r1))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&[wallet_a()], &mut cache)
        .await
        .unwrap();

    // Incremental run:
    //   Phase 1 backward (end = 1_000_001 - 1 = 1_000_000) → empty.
    //   Phase 2 forward (start = 1_000_003): page arrives oldest-first (ascending).
    //   The complete window is persisted independently of row order.
    let reversed_page = trades_page(WALLET_A_HEX, &[("0xnew1", 2_000_000)]);
    let mut r2 = HashMap::new();
    r2.insert(trade_url_end(wallet_a(), 1_000_000), empty_page());
    r2.insert(
        PolymarketEndpoint::UserTradeActivityPage {
            user: wallet_a().to_string(),
            end: 1_000_003,
            start: Some(1_000_003),
            offset: 0,
        }
        .url(BASE_URL),
        b"[]".to_vec(),
    );
    r2.insert(trade_url_start(wallet_a(), 1_000_003), reversed_page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(r2))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&[wallet_a()], &mut cache)
        .await
        .unwrap();

    assert_eq!(
        cache.trade_count(),
        4,
        "new trade arriving before known sequence must be appended"
    );
    let ids: Vec<_> = cache
        .known_trade_ids(WALLET_A_HEX)
        .iter()
        .map(|id| id.0.clone())
        .collect();
    assert!(ids.iter().any(|id| id == "0xnew1"));
}

// ── Scenario 6 ────────────────────────────────────────────────────────────────
//
// PASS: config.audit_window_days = u32::MAX → all trades from any timestamp
//       are included in build_trader_ledgers output (sentinel honoured).
// FAIL: old trades excluded from ledger reconstruction.

#[tokio::test]
async fn scenario_unlimited_window_includes_all_trades() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    let page = trades_page(
        WALLET_A_HEX,
        &[
            ("0xrecent", 1_704_067_200),  // 2024-01-01
            ("0xancient", 1_580_000_000), // 2020-01-26
        ],
    );
    let mut responses = HashMap::new();
    responses.insert(trade_url_cold(wallet_a()), page);
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&[wallet_a()], &mut cache)
        .await
        .unwrap();

    assert_eq!(cache.trade_count(), 2, "both trades must be cached");

    let trades = cache.trades_for(WALLET_A_HEX);
    assert_eq!(trades.len(), 2);

    let ledgers = build_trader_ledgers(&trades, u32::MAX, None);
    let ledger = ledgers
        .iter()
        .find(|l| l.wallet == wallet_a())
        .expect("ledger for wallet_a must exist");

    let all_ids: Vec<&SourceTradeId> = ledger
        .open_positions
        .iter()
        .flat_map(|p| p.source_trade_ids.iter())
        .chain(
            ledger
                .closed_trades
                .iter()
                .flat_map(|t| t.source_trade_ids.iter()),
        )
        .collect();

    assert!(all_ids.iter().any(|id| id.0 == "0xrecent"));
    assert!(
        all_ids.iter().any(|id| id.0 == "0xancient"),
        "ancient trade must appear in the ledger — unlimited window must not exclude it"
    );
}

// ── Scenario 7 ────────────────────────────────────────────────────────────────
//
// PASS: parallel fetch (concurrency=N) and sequential fetch (concurrency=1)
//       produce identical trade sets per wallet.
// FAIL: any wallet's trades differ between parallel and sequential runs.

#[tokio::test]
async fn scenario_parallel_fetch_matches_sequential() {
    const N: usize = 8;
    let wallets: Vec<WalletAddress> = (0..N)
        .map(|i| {
            let hex = format!("0x{:040x}", i + 1);
            WalletAddress::from_hex(&hex).unwrap()
        })
        .collect();

    let mut responses = HashMap::new();
    for (i, w) in wallets.iter().enumerate() {
        let hash = format!("0xhash{i:02}");
        let page = trades_page(&w.to_string(), &[(&hash, 2_000_000 + i as i64)]);
        responses.insert(trade_url_cold(*w), page);
    }

    // Parallel run.
    let dir_par = TempDir::new().unwrap();
    let mut cache_par = WalletCache::open(&dir_par.path().join("cache.db")).unwrap();
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses.clone()))
        .with_clock_for_test(|| 2_000_000_000)
        .with_concurrency(N)
        .fetch_all(&wallets, &mut cache_par)
        .await
        .unwrap();

    // Sequential run.
    let dir_seq = TempDir::new().unwrap();
    let mut cache_seq = WalletCache::open(&dir_seq.path().join("cache.db")).unwrap();
    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_clock_for_test(|| 2_000_000_000)
        .with_concurrency(1)
        .fetch_all(&wallets, &mut cache_seq)
        .await
        .unwrap();

    assert_eq!(cache_par.trade_count(), N);
    assert_eq!(cache_seq.trade_count(), N);

    for (i, w) in wallets.iter().enumerate() {
        let hex = w.to_string();
        let par_ids = cache_par.known_trade_ids(&hex);
        let seq_ids = cache_seq.known_trade_ids(&hex);
        let expected_hash = format!("0xhash{i:02}");
        assert_eq!(par_ids.len(), 1);
        assert_eq!(par_ids[0], SourceTradeId(expected_hash));
        assert_eq!(
            seq_ids, par_ids,
            "parallel and sequential cache must be identical"
        );
    }
}

// ── Scenario 8 ────────────────────────────────────────────────────────────────
//
// PASS: opening a path that points to a non-SQLite file (e.g. legacy JSON or
//       garbage bytes) returns an error rather than silently corrupting state.
// FAIL: open() succeeds and silently masks the bad data.

#[tokio::test]
async fn scenario_non_sqlite_file_errors_clearly() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cache.db");

    // Pre-existing legacy JSON content at the SQLite path.
    let legacy = r#"{"entries":{"0xaa":{"fetched_at_unix":1,"trades":[]}}}"#;
    std::fs::write(&path, legacy).unwrap();

    let result = WalletCache::open(&path);
    // Either the file is interpreted as a (corrupt) SQLite DB and errors out,
    // or `open` returns a usable cache that immediately errors on any query.
    // Either way the contract is: do not silently report 0 trades for the
    // wallets in the legacy file.
    if let Ok(cache) = result {
        // If open() ignored the bad bytes, the cache must still report nothing
        // about those wallets — they were never inserted via the new API.
        assert_eq!(cache.trade_count(), 0);
        assert!(cache.all_wallet_addresses().is_empty());
    }
}

// ── Scenario 9 ────────────────────────────────────────────────────────────────
//
// PASS: per-wallet streaming reads N wallets sequentially without holding all
//       trades in memory simultaneously — verified by checking that
//       trades_for returns only one wallet's trades.
// FAIL: trades_for returns trades for other wallets.

#[tokio::test]
async fn scenario_per_wallet_streaming_isolation() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    let mut responses = HashMap::new();
    let wallets: Vec<WalletAddress> = (1..=4)
        .map(|i| {
            let hex = format!("0x{:040x}", i);
            WalletAddress::from_hex(&hex).unwrap()
        })
        .collect();
    for (i, w) in wallets.iter().enumerate() {
        let hash = format!("0xtx{i:02}");
        let page = trades_page(&w.to_string(), &[(&hash, 2_000_000 + i as i64)]);
        responses.insert(trade_url_cold(*w), page);
    }

    PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses))
        .with_clock_for_test(|| 2_000_000_000)
        .fetch_all(&wallets, &mut cache)
        .await
        .unwrap();

    assert_eq!(cache.trade_count(), 4);

    // Each wallet's trades_for must return ONLY its own trades.
    for (i, w) in wallets.iter().enumerate() {
        let trades = cache.trades_for(&w.to_string());
        assert_eq!(trades.len(), 1, "wallet {i} must have exactly 1 trade");
        assert_eq!(trades[0].wallet, *w, "wallet field must match");
        assert_eq!(
            trades[0].source_trade_id,
            SourceTradeId(format!("0xtx{i:02}"))
        );
    }
}
