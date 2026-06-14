#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario tests for the one-shot legacy migration `migrate::auto_migrate_legacy`.
//!
//! Covers the behaviours that survive #335 (which removed the Dune-CSV ingest
//! stage and the on-chain re-enumeration reader): `wallet_set.json` ingest
//! (bare-array and checkpoint formats) + deletion, the no-legacy-files no-op,
//! the post-ingest-sequence skip (regression for the unconditional-UPDATE bug),
//! and idempotency across reruns.

use std::path::PathBuf;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::chain::{ALL_EXCHANGE_CONTRACTS, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2};
use pe_bootstrap::config::BootstrapConfig;
use pe_bootstrap::migrate::auto_migrate_legacy;
use pe_bootstrap::pile::SRC_WALLET_SET_JSON;
use tempfile::TempDir;

fn open_cache_in(dir: &TempDir) -> (PathBuf, WalletCache) {
    let path = dir.path().join("cache.db");
    let cache = WalletCache::open(&path).unwrap();
    (path, cache)
}

fn config_for(dir: &TempDir, cache_path: PathBuf, wallet_set_path: PathBuf) -> BootstrapConfig {
    BootstrapConfig {
        cache_path,
        wallet_set_path,
        output_path: dir.path().join("watchlist.json"),
        ..BootstrapConfig::default()
    }
}

/// Post-#179 checkpoint `wallet_set.json` with 3 wallets.
fn legacy_wallet_set_checkpoint(dir: &TempDir, both_topics: bool) -> PathBuf {
    let path = dir.path().join("wallet_set.json");
    let topics_str = if both_topics {
        format!(
            r#"["{v1}","{v2}"]"#,
            v1 = TOPIC_ORDER_FILLED_V1,
            v2 = TOPIC_ORDER_FILLED_V2,
        )
    } else {
        "[]".to_owned()
    };
    let json = format!(
        r#"{{
            "completed_contracts": ["0x{c0:x}","0x{c1:x}","0x{c2:x}","0x{c3:x}"],
            "wallets": [
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "0xcccccccccccccccccccccccccccccccccccccccc"
            ],
            "enumerated_topic_hashes": {topics_str}
        }}"#,
        c0 = ALL_EXCHANGE_CONTRACTS[0],
        c1 = ALL_EXCHANGE_CONTRACTS[1],
        c2 = ALL_EXCHANGE_CONTRACTS[2],
        c3 = ALL_EXCHANGE_CONTRACTS[3],
    );
    std::fs::write(&path, json).unwrap();
    path
}

// PASS: a checkpoint-format wallet_set.json ingests all 3 wallets and is deleted.
// FAIL: wallets missing, or the file survives the migration.
#[tokio::test]
async fn checkpoint_wallet_set_ingests_and_deletes() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = legacy_wallet_set_checkpoint(&dir, true);
    let config = config_for(&dir, cache_path, wallet_set_path.clone());

    auto_migrate_legacy(&config, &mut cache).unwrap();

    assert!(
        !wallet_set_path.exists(),
        "wallet_set.json must be deleted after successful migration"
    );
    let hexes = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON).unwrap();
    assert_eq!(
        hexes.len(),
        3,
        "all 3 wallets from the file must be in cache"
    );
}

// PASS: a bare-array wallet_set.json ingests both wallets and is deleted.
// FAIL: wallets missing, or the file survives the migration.
#[tokio::test]
async fn bare_array_wallet_set_ingests_and_deletes() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = dir.path().join("wallet_set.json");
    let bare = r#"["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]"#;
    std::fs::write(&wallet_set_path, bare).unwrap();
    let config = config_for(&dir, cache_path, wallet_set_path.clone());

    auto_migrate_legacy(&config, &mut cache).unwrap();

    assert!(!wallet_set_path.exists());
    let hexes = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON).unwrap();
    assert_eq!(hexes.len(), 2);
}

// PASS: with no legacy files the migration is a clean no-op (no wallets added).
// FAIL: errors, or invents wallets from nothing.
#[tokio::test]
async fn no_legacy_files_is_a_clean_noop() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = dir.path().join("wallet_set.json");
    let config = config_for(&dir, cache_path, wallet_set_path);

    auto_migrate_legacy(&config, &mut cache).unwrap();

    let hexes = cache.wallets_with_source_bit(SRC_WALLET_SET_JSON).unwrap();
    assert!(hexes.is_empty(), "fresh install: no wallets ingested");
}

// PASS: with no legacy files the expensive post-ingest sequence is SKIPPED — a
// pre-populated (wrong) trade_count is left untouched (refresh_trade_counts did
// not fire). Regression guard for the unconditional-post-ingest bug.
// FAIL: trade_count is reset to 0 (post-ingest sequence ran on a no-op).
#[tokio::test]
async fn no_legacy_files_skips_post_ingest_sequence() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = dir.path().join("wallet_set.json");
    let config = config_for(&dir, cache_path, wallet_set_path);

    let hex = "0xf000000000000000000000000000000000000000";
    cache
        .upsert_wallets_bulk(&[(
            hex.to_owned(),
            SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
            0,
        )])
        .unwrap();
    cache.conn_for_test_set_trade_count(hex, 999);

    auto_migrate_legacy(&config, &mut cache).unwrap();

    let count: i64 = cache
        .raw_conn_for_test()
        .query_row(
            "SELECT trade_count FROM wallets WHERE wallet_hex = ?1",
            rusqlite::params![hex],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        count, 999,
        "post-ingest sequence MUST be skipped when no legacy files exist; \
         trade_count would have been reset to 0 if refresh_trade_counts ran"
    );
}

// PASS: migration is idempotent — a second run (file already consumed) is a
// no-op and the wallet count does not grow.
// FAIL: second run errors, or wallets are duplicated.
#[tokio::test]
async fn auto_migrate_legacy_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = legacy_wallet_set_checkpoint(&dir, true);
    let config = config_for(&dir, cache_path, wallet_set_path);

    auto_migrate_legacy(&config, &mut cache).unwrap();
    let n1 = cache
        .wallets_with_source_bit(SRC_WALLET_SET_JSON)
        .unwrap()
        .len();

    auto_migrate_legacy(&config, &mut cache).unwrap();
    let n2 = cache
        .wallets_with_source_bit(SRC_WALLET_SET_JSON)
        .unwrap()
        .len();

    assert_eq!(n1, n2, "wallet count must not grow on idempotent re-run");
}
