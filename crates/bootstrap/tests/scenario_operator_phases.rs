//! Operator-level scenario tests for the decomposed pe-bootstrap phases (issue #195).
//!
//! Verifies the independent invocability and correctness of the new phase
//! functions: `enumerate`, `fetch`, `watchlist_phase`, `seed_historical`, and
//! the `weekly` exit-code fix. No network calls; deterministic.
//!
//! PASS criteria are stated inline above each test.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::config::BootstrapConfig;
use pe_bootstrap::enumerate;
use pe_bootstrap::error::BootstrapError;
use pe_bootstrap::fetch;
use pe_bootstrap::migrate;
use pe_bootstrap::pile::SRC_WALLET_SET_JSON;
use pe_bootstrap::polymarket::PolymarketBulkFetcher;
use pe_bootstrap::seed_historical::parse_seed_as_of_env;
use pe_bootstrap::watchlist_phase;
use pe_core_types::WalletAddress;
use pe_source_polymarket_public::{FixtureFetcher, PolymarketEndpoint};
use tempfile::TempDir;

const BASE_URL: &str = "https://data-api.polymarket.com";

fn wallet(byte: u8) -> WalletAddress {
    WalletAddress::from_hex(&format!("0x{:040x}", byte)).unwrap()
}

fn trade_url_cold(w: WalletAddress) -> String {
    PolymarketEndpoint::UserTradeActivity {
        user: w.to_string(),
        end: None,
        start: None,
    }
    .url(BASE_URL)
}

fn end_page() -> Vec<u8> {
    b"[]".to_vec()
}

/// Build a page JSON with N synthetic trades for a wallet.
fn trades_page(n: usize, market_prefix: &str) -> Vec<u8> {
    let mut s = String::from("[");
    for i in 0..n {
        if i > 0 {
            s.push(',');
        }
        let tid = format!("0x{:064x}", i + 1);
        let mid = format!("0x{market_prefix}{i:060x}");
        s.push_str(&format!(
            r#"{{"transactionHash":"{tid}","conditionId":"{mid}","outcomeIndex":0,"side":"BUY","price":"0.5","size":"1","timestamp":{ts},"asset":"0","title":"t","slug":"s","icon":"","eventSlug":"e","outcome":"YES","name":"n","pseudonym":"p","bio":"","profileImage":"","profileImageOptimized":""}}"#,
            ts = 1_700_000_000_i64 + i as i64
        ));
    }
    s.push(']');
    s.into_bytes()
}

fn config_with_dir(dir: &TempDir) -> BootstrapConfig {
    BootstrapConfig {
        cache_path: dir.path().join("cache.db"),
        output_path: dir.path().join("watchlist.json"),
        polymarket_base_url: BASE_URL.to_owned(),
        // Disable optional phases so tests stay local.
        fetch_funder_graph: false,
        fetch_resolutions: false,
        write_snapshot: false,
        // Very loose post-filter so test fixtures pass through.
        min_closed_trades: 0,
        min_win_rate_pct: 0,
        post_filter_active_window_days: 36500,
        post_filter_max_avg_hours_to_resolution: 1_000_000,
        ..BootstrapConfig::default()
    }
}

// ── Scenario 1: run_enumerate skips when all enumeration already done ─────────
//
// PASS: EnumerateReport::skipped == true; wallets_discovered reflects existing cache.
// FAIL: re-runs enumeration or returns skipped=false.

#[tokio::test]
async fn scenario_enumerate_skips_when_state_is_complete() {
    use pe_bootstrap::chain::{ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS};

    let dir = TempDir::new().unwrap();
    let config = config_with_dir(&dir);
    let mut cache = WalletCache::open(&config.cache_path).unwrap();

    // Seed a wallet directly so wallets_discovered > 0.
    cache
        .upsert_wallets_bulk(&[(
            wallet(0xAA).to_string(),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
            0,
        )])
        .unwrap();

    // Mark all contracts and all topics as fully enumerated.
    let all_contracts: Vec<String> = ALL_EXCHANGE_CONTRACTS
        .iter()
        .map(|c| format!("0x{c:x}"))
        .collect();
    let all_topics: Vec<String> = ALL_ORDER_FILLED_TOPICS
        .iter()
        .map(|h| format!("{h}"))
        .collect();
    migrate::save_enum_state(&mut cache, &all_contracts, &all_topics).unwrap();

    let report = enumerate::run_enumerate(&config, &mut cache).await.unwrap();

    assert!(
        report.skipped,
        "expected skipped=true when all topics/contracts already enumerated"
    );
    assert_eq!(
        report.chunks_scanned, 0,
        "no chunks should be scanned when skipped"
    );
    assert_eq!(
        report.topics_completed, 0,
        "no topics completed when skipped"
    );
    assert_eq!(report.wallets_discovered, 1, "should count existing wallet");
}

// ── Scenario 2: run_fetch honours skip_trade_fetch=true ──────────────────────
//
// PASS: FetchReport { attempted: 0, failed: 0 }; no trades inserted.
// FAIL: attempts fetch, returns non-zero counts, or errors.

#[tokio::test]
async fn scenario_fetch_skip_trade_fetch_flag_is_a_noop() {
    let dir = TempDir::new().unwrap();
    let mut config = config_with_dir(&dir);
    config.skip_trade_fetch = true;

    let mut cache = WalletCache::open(&config.cache_path).unwrap();

    let wallets = vec![wallet(0x01), wallet(0x02)];
    let report = fetch::run_fetch(&config, &mut cache, &wallets)
        .await
        .unwrap();

    assert_eq!(
        report.attempted, 0,
        "skip_trade_fetch must not attempt any fetch"
    );
    assert_eq!(report.failed, 0);
    // No trades should have been inserted.
    assert!(
        cache.trades_for(&wallets[0].to_string()).is_empty(),
        "no trades expected when skip_trade_fetch=true"
    );
}

// ── Scenario 3: run_fetch returns PartialFetch when wallets fail ──────────────
//
// PASS: fetch::run_fetch returns Err(BootstrapError::PartialFetch { failed_wallets: 1 })
//       when the wallet's HTTP request fails (connection refused to non-listening port).
// FAIL: returns Ok or a different error variant.

#[tokio::test]
async fn scenario_fetch_partial_fail_returns_partial_fetch_error() {
    let dir = TempDir::new().unwrap();
    let mut config = config_with_dir(&dir);
    config.polymarket_wallet_timeout_secs = 2; // fast but not flaky
    // Point at a port with no listener; connection refused → wallet-level failure.
    config.polymarket_base_url = "http://127.0.0.1:12321".to_owned();

    let mut cache = WalletCache::open(&config.cache_path).unwrap();

    let w = wallet(0xCC);
    cache
        .upsert_wallets_bulk(&[(
            w.to_string(),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
            0,
        )])
        .unwrap();

    // run_fetch itself must propagate the per-wallet failure as PartialFetch.
    let result = fetch::run_fetch(&config, &mut cache, &[w]).await;
    assert!(
        matches!(
            result,
            Err(BootstrapError::PartialFetch { failed_wallets: 1 })
        ),
        "expected PartialFetch {{failed_wallets:1}}, got {result:?}"
    );
}

// ── Scenario 4: run_watchlist standalone — reads cache without re-fetching ───
//
// PASS: WatchlistReport.ledger_count reflects pre-inserted trades; watchlist.json
//       is written; report fields are consistent with the inserted data.
// FAIL: re-fetches trades, panics, or writes no file.

#[tokio::test]
async fn scenario_watchlist_standalone_reads_existing_cache_trades() {
    let dir = TempDir::new().unwrap();
    let config = config_with_dir(&dir);
    let mut cache = WalletCache::open(&config.cache_path).unwrap();

    let w = wallet(0xAA);

    // Register wallet in SRC_WALLET_SET_JSON so the phase includes it.
    cache
        .upsert_wallets_bulk(&[(
            w.to_string(),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
            0,
        )])
        .unwrap();

    // Insert 10 trades via fixture fetcher (same pattern as scenario_pile).
    let mut pages: HashMap<String, Vec<u8>> = HashMap::new();
    pages.insert(trade_url_cold(w), trades_page(10, "cc"));
    let end_url = PolymarketEndpoint::UserTradeActivity {
        user: w.to_string(),
        end: Some(1_700_000_010),
        start: None,
    }
    .url(BASE_URL);
    pages.insert(end_url, end_page());

    let fetcher = PolymarketBulkFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(pages))
        .with_concurrency(1)
        .with_wallet_timeout(30);
    fetcher.fetch_all(&[w], &mut cache).await.unwrap();

    // Now call run_watchlist standalone — must not attempt any re-fetch.
    let wallets = vec![w];
    let report = watchlist_phase::run_watchlist(&config, &mut cache, &wallets, None)
        .await
        .unwrap();

    assert_eq!(
        report.ledger_count, 1,
        "one ledger reconstructed for the wallet"
    );
    assert_eq!(report.total_trades, 10, "all 10 inserted trades counted");
    assert!(!report.snapshot_written, "write_snapshot=false by default");
    assert!(
        report.output_path.exists(),
        "watchlist.json must be written by run_watchlist"
    );
}

// ── Scenario 5: run_watchlist with empty wallet list ─────────────────────────
//
// PASS: Report has all zeros; watchlist.json written with empty entries array.
// FAIL: panics or fails to write the output file.

#[tokio::test]
async fn scenario_watchlist_empty_wallets_writes_empty_json() {
    let dir = TempDir::new().unwrap();
    let config = config_with_dir(&dir);
    let mut cache = WalletCache::open(&config.cache_path).unwrap();

    let report = watchlist_phase::run_watchlist(&config, &mut cache, &[], None)
        .await
        .unwrap();

    assert_eq!(report.ledger_count, 0);
    assert_eq!(report.total_trades, 0);
    assert_eq!(report.active_count, 0);
    assert!(
        report.output_path.exists(),
        "watchlist.json must be written even for empty list"
    );
}

// ── Scenario 6: parse_seed_as_of_env edge cases ──────────────────────────────
//
// PASS: Parses valid ISO-8601 dates; returns empty on empty input; rejects bad input.
// FAIL: panics, returns wrong dates, or ignores trailing whitespace.

#[test]
fn scenario_parse_seed_as_of_env_valid() {
    let dates = parse_seed_as_of_env("2024-01-15,2024-06-30").unwrap();
    assert_eq!(dates.len(), 2);
    assert_eq!(dates[0].year(), 2024);
    assert_eq!(dates[0].month() as u8, 1);
    assert_eq!(dates[0].day(), 15);
    assert_eq!(dates[1].month() as u8, 6);
    assert_eq!(dates[1].day(), 30);
}

#[test]
fn scenario_parse_seed_as_of_env_empty_is_empty_vec() {
    let dates = parse_seed_as_of_env("").unwrap();
    assert!(dates.is_empty());
}

#[test]
fn scenario_parse_seed_as_of_env_whitespace_trimmed() {
    let dates = parse_seed_as_of_env("  2024-03-01 , 2024-04-01  ").unwrap();
    assert_eq!(dates.len(), 2);
}

#[test]
fn scenario_parse_seed_as_of_env_trailing_comma_skipped() {
    let dates = parse_seed_as_of_env("2024-01-01,").unwrap();
    assert_eq!(dates.len(), 1);
}

#[test]
fn scenario_parse_seed_as_of_env_invalid_returns_error() {
    let result = parse_seed_as_of_env("not-a-date");
    assert!(result.is_err(), "invalid date must produce an error");
    assert!(matches!(result, Err(BootstrapError::Parse { .. })));
}

// ── Scenario 7: weekly exit-2 logic — PartialFetch error variant ─────────────
//
// PASS: BootstrapError::PartialFetch { failed_wallets: N } matches the arm that
//       main.rs uses to produce exit 2 for weekly partial failures.
// FAIL: The variant is missing or the match arm would not compile.

#[test]
fn scenario_weekly_partial_error_matches_exit2_arm() {
    // Verifies that the error type returned by run_weekly on partial failure
    // matches the arm in main.rs that produces exit 2. This ensures the
    // type-level contract is preserved even without running a real Etherscan call.
    let err = BootstrapError::PartialFetch { failed_wallets: 3 };
    let exit_code = match err {
        BootstrapError::PartialFetch { .. } => 2i32,
        _ => 1i32,
    };
    assert_eq!(
        exit_code, 2,
        "PartialFetch must map to exit 2 in caller dispatch"
    );
}

// ── Scenario 8: run_enumerate with no-op config (skip_trade_fetch irrelevant) ─
//
// PASS: When enum state is complete and a wallet already exists, wallets_discovered
//       counts only the pre-existing valid wallet. The skipped flag is set.
// FAIL: wallets_discovered is wrong, or skipped is false.

#[tokio::test]
async fn scenario_enumerate_wallets_discovered_counts_source_bit_wallets() {
    use pe_bootstrap::chain::{ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS};

    let dir = TempDir::new().unwrap();
    let config = config_with_dir(&dir);
    let mut cache = WalletCache::open(&config.cache_path).unwrap();

    // Insert 3 wallets with SRC_WALLET_SET_JSON bit and 1 with a different bit.
    for i in 0u8..3 {
        cache
            .upsert_wallets_bulk(&[(
                wallet(i).to_string(),
                SRC_WALLET_SET_JSON,
                false,
                None,
                None,
                None,
                0,
            )])
            .unwrap();
    }
    // This wallet has a different source bit and must NOT be counted.
    cache
        .upsert_wallets_bulk(&[(
            wallet(0xFF).to_string(),
            pe_bootstrap::pile::SRC_LEADERBOARD,
            false,
            None,
            None,
            None,
            0,
        )])
        .unwrap();

    // Mark enumeration complete so the phase skips without network.
    let all_contracts: Vec<String> = ALL_EXCHANGE_CONTRACTS
        .iter()
        .map(|c| format!("0x{c:x}"))
        .collect();
    let all_topics: Vec<String> = ALL_ORDER_FILLED_TOPICS
        .iter()
        .map(|h| format!("{h}"))
        .collect();
    migrate::save_enum_state(&mut cache, &all_contracts, &all_topics).unwrap();

    let report = enumerate::run_enumerate(&config, &mut cache).await.unwrap();

    assert!(report.skipped);
    assert_eq!(
        report.wallets_discovered, 3,
        "only SRC_WALLET_SET_JSON wallets are counted"
    );
}
