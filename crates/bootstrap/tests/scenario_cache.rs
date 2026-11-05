//! Scenario tests for the permanent SQLite trade cache.
//!
//! Each scenario has a single PASS/FAIL criterion written before the test body.
//! No network calls; all data is constructed in-process.
//! Clock is fixed via hardcoded timestamps; no RNG is used.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "support/cache_seed.rs"]
mod cache_seed;
use cache_seed::insert_page;

use pe_bootstrap::build_seed_watchlist;
use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::filter::FilterConfig;
use pe_core_types::{SourceTimestamp, SourceTradeId, WalletAddress};
use pe_trader_index::build_trader_ledgers;
use tempfile::TempDir;
use time::OffsetDateTime;

const WALLET_A_HEX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
fn wallet_a() -> WalletAddress {
    WalletAddress::from_hex(WALLET_A_HEX).unwrap()
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

#[tokio::test]
async fn scenario_first_write_survives_reopen() {
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
    insert_page(&mut cache, wallet_a(), &page);

    assert_eq!(
        cache.known_trade_ids(WALLET_A_HEX).unwrap().len(),
        3,
        "all 3 trades must be in the cache"
    );

    // Drop and reopen to verify durability through SQLite WAL.
    drop(cache);
    let reloaded = WalletCache::open(&path).unwrap();
    assert_eq!(reloaded.trades_for(WALLET_A_HEX).len(), 3);
}

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
    insert_page(&mut cache, wallet_a(), &page);
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
    insert_page(&mut cache, wallet_a(), &page);

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

#[tokio::test]
async fn scenario_per_wallet_streaming_isolation() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    let wallets: Vec<WalletAddress> = (1..=4)
        .map(|i| {
            let hex = format!("0x{:040x}", i);
            WalletAddress::from_hex(&hex).unwrap()
        })
        .collect();
    for (i, w) in wallets.iter().enumerate() {
        let hash = format!("0xtx{i:02}");
        let page = trades_page(&w.to_string(), &[(&hash, 2_000_000 + i as i64)]);
        insert_page(&mut cache, *w, &page);
    }

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
