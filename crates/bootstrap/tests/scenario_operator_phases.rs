//! Operator-level scenario tests for the decomposed pe-bootstrap phases (issue #195).
//!
//! Verifies the independent invocability and correctness of the phase functions
//! `watchlist_phase`. No network calls; deterministic. (The Dune
//! `enumerate` / `seed-historical` phases were removed in #335; their scenarios
//! went with them.)
//!
//! PASS criteria are stated inline above each test.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::config::BootstrapConfig;
use pe_bootstrap::pile::SRC_WALLET_SET_JSON;
#[path = "support/cache_seed.rs"]
mod cache_seed;
use cache_seed::insert_page;
use pe_bootstrap::watchlist_phase;
use pe_core_types::WalletAddress;
use tempfile::TempDir;

const BASE_URL: &str = "https://data-api.polymarket.com";

fn wallet(byte: u8) -> WalletAddress {
    WalletAddress::from_hex(&format!("0x{:040x}", byte)).unwrap()
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

    insert_page(&mut cache, w, &trades_page(10, "cc"));

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
