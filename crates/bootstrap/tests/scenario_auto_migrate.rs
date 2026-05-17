//! Operator-level scenarios for the issue #181 one-shot legacy migration.
//!
//! Exercises `migrate::auto_migrate_legacy` against three on-disk states:
//!   1. Pre-#179 checkpoint format (post-#68 structured JSON, empty
//!      `enumerated_topic_hashes` via `#[serde(default)]`).
//!   2. Post-#179 checkpoint format (both topics in `enumerated_topic_hashes`).
//!   3. No legacy files (post-cleanup state — no-op).
//!   4. Bare-array legacy format (PR #68 pre-checkpoint) — must synthesize
//!      V1-done sentinel.
//!   5. Dune-CSV archive — files renamed to `.csv.imported` after ingest.
//!
//! Determinism: pure in-process. Each test gets a fresh `TempDir` under
//! `target/`, so no shared state across runs.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::config::BootstrapConfig;
use pe_bootstrap::migrate::{
    self, CURSOR_WALLET_ENUM_COMPLETED_CONTRACTS, CURSOR_WALLET_ENUM_TOPIC_HASHES,
    auto_migrate_legacy,
};
use pe_source_onchain_polygon::contracts::{
    ALL_EXCHANGE_CONTRACTS, ALL_ORDER_FILLED_TOPICS, TOPIC_ORDER_FILLED_V1, TOPIC_ORDER_FILLED_V2,
};
use tempfile::TempDir;

fn open_cache_in(dir: &TempDir) -> (PathBuf, WalletCache) {
    let path = dir.path().join("cache.db");
    let cache = WalletCache::open(&path).unwrap();
    (path, cache)
}

fn config_for(dir: &TempDir, cache_path: PathBuf, wallet_set_path: PathBuf) -> BootstrapConfig {
    // output_path doesn't matter for migrate-only tests but must be set.
    BootstrapConfig {
        cache_path,
        wallet_set_path,
        output_path: dir.path().join("watchlist.json"),
        ..BootstrapConfig::default()
    }
}

fn legacy_wallet_set_post_179(dir: &TempDir, both_topics: bool) -> PathBuf {
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

// ── Scenario A ───────────────────────────────────────────────────────────────
// PASS: a pre-#179 wallet_set.json on disk gets fully consolidated. After
//       auto_migrate_legacy: wallets in SQLite with SRC_WALLET_SET_JSON bit;
//       both `source_cursor` keys populated; JSON file deleted.
// FAIL: any of: wallets missing, cursor keys missing, file not deleted, OR
//       wallets present in `wallets` table without SRC_WALLET_SET_JSON bit.

#[tokio::test]
async fn pre_179_checkpoint_migrates_and_deletes() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = legacy_wallet_set_post_179(&dir, false);
    let config = config_for(&dir, cache_path.clone(), wallet_set_path.clone());

    auto_migrate_legacy(&config, &mut cache).unwrap();

    // 1. File deleted.
    assert!(
        !wallet_set_path.exists(),
        "wallet_set.json must be deleted after successful migration"
    );

    // 2. Cursor keys populated. Contracts = all 4. Topics = empty
    //    (pre-#179 shape was empty; auto_migrate copies verbatim).
    let (contracts, topics) = migrate::load_enum_state(&cache).unwrap();
    assert_eq!(contracts.len(), 4, "all 4 contracts must be in cursor");
    assert!(
        topics.is_empty(),
        "pre-#179 shape had empty topics; cursor should mirror that"
    );

    // 3. Wallets in SQLite with SRC_WALLET_SET_JSON bit.
    let hexes = cache
        .wallets_with_source_bit(pe_bootstrap::pile::SRC_WALLET_SET_JSON)
        .unwrap();
    assert_eq!(hexes.len(), 3, "all 3 wallets from file must be in cache");
}

// ── Scenario B ───────────────────────────────────────────────────────────────
// PASS: post-#179 wallet_set.json (both topics in field) migrates with the
//       enumerated_topic_hashes field copied verbatim to source_cursor.
// FAIL: topics field lost during migration.

#[tokio::test]
async fn post_179_checkpoint_preserves_both_topic_hashes() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = legacy_wallet_set_post_179(&dir, true);
    let config = config_for(&dir, cache_path.clone(), wallet_set_path.clone());

    auto_migrate_legacy(&config, &mut cache).unwrap();

    let (_, topics) = migrate::load_enum_state(&cache).unwrap();
    assert_eq!(topics.len(), 2, "both V1+V2 topics must be in cursor");
    for topic in &ALL_ORDER_FILLED_TOPICS {
        let expected = format!("{topic}");
        assert!(
            topics.contains(&expected),
            "topic {expected} missing from migrated cursor"
        );
    }
}

// ── Scenario C ───────────────────────────────────────────────────────────────
// PASS: bare-array legacy format (pre-#68 binary) migrates with synthesized
//       V1-done sentinel (matches the prior `lib.rs:140-148` upgrade behavior).
// FAIL: bare-array load fails, OR no V1 topic synthesized, OR contracts list
//       wrong.

#[tokio::test]
async fn bare_array_legacy_synthesizes_v1_done_sentinel() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = dir.path().join("wallet_set.json");
    // Bare-array format (PR #68): just a JSON array of wallet hex strings.
    let bare = r#"["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]"#;
    std::fs::write(&wallet_set_path, bare).unwrap();

    let config = config_for(&dir, cache_path.clone(), wallet_set_path.clone());
    auto_migrate_legacy(&config, &mut cache).unwrap();

    // File deleted.
    assert!(!wallet_set_path.exists());

    // Cursor: contracts = all 4, topics = [V1] (the synthesized sentinel).
    let (contracts, topics) = migrate::load_enum_state(&cache).unwrap();
    assert_eq!(
        contracts.len(),
        4,
        "bare-array migration must synthesize all 4 contracts as complete"
    );
    assert_eq!(
        topics,
        vec![format!("{TOPIC_ORDER_FILLED_V1}")],
        "bare-array migration must synthesize V1-done sentinel"
    );

    // Wallets ingested.
    let hexes = cache
        .wallets_with_source_bit(pe_bootstrap::pile::SRC_WALLET_SET_JSON)
        .unwrap();
    assert_eq!(hexes.len(), 2);
}

// ── Scenario D ───────────────────────────────────────────────────────────────
// PASS: no legacy files on disk → auto_migrate_legacy is a no-op (no errors,
//       no spurious cursor writes, no spurious file deletions).
// FAIL: function errors on missing files, OR writes a cursor key with empty
//       data when it shouldn't (could mask a real "fresh install" state).

#[tokio::test]
async fn no_legacy_files_is_a_clean_noop() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    // No wallet_set.json, no data/dune_csvs/.
    let wallet_set_path = dir.path().join("wallet_set.json");
    let config = config_for(&dir, cache_path.clone(), wallet_set_path.clone());

    auto_migrate_legacy(&config, &mut cache).unwrap();

    // Cursors should be absent (no migration occurred).
    assert!(
        cache
            .get_source_cursor(CURSOR_WALLET_ENUM_COMPLETED_CONTRACTS)
            .is_none(),
        "fresh install must leave cursor unset"
    );
    assert!(
        cache
            .get_source_cursor(CURSOR_WALLET_ENUM_TOPIC_HASHES)
            .is_none(),
        "fresh install must leave cursor unset"
    );
    let (contracts, topics) = migrate::load_enum_state(&cache).unwrap();
    assert!(contracts.is_empty(), "fresh install: contracts empty");
    assert!(topics.is_empty(), "fresh install: topics empty");
}

// ── Scenario D' (regression for code-review finding) ─────────────────────────
// PASS: when no legacy files are on disk, the post-ingest sequence is SKIPPED
//       (no full-table UPDATEs against the ~2.7M-row `wallets` table). Sketch:
//       pre-populate the cache with a wallet whose `trade_count` is wrong,
//       run auto_migrate_legacy with no legacy files, assert trade_count is
//       UNCHANGED — proves `refresh_trade_counts` did not fire.
// FAIL: trade_count changes on a no-op run (means the expensive post-ingest
//       sequence is running unconditionally — the bug code-review caught).

#[tokio::test]
async fn no_legacy_files_skips_post_ingest_sequence() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = dir.path().join("wallet_set.json");
    let config = config_for(&dir, cache_path.clone(), wallet_set_path.clone());

    // Insert a wallet with a deliberately-wrong trade_count via the
    // scenario-only escape hatch. If the post-ingest sequence fires,
    // `refresh_trade_counts` would reset it to 0 (no trades in this fixture).
    let hex = "0xf000000000000000000000000000000000000000";
    cache
        .upsert_wallets_bulk(&[(
            hex.to_owned(),
            pe_bootstrap::pile::SRC_WALLET_SET_JSON,
            false,
            None,
            None,
            None,
            0,
        )])
        .unwrap();
    cache.conn_for_test_set_trade_count(hex, 999);

    auto_migrate_legacy(&config, &mut cache).unwrap();

    // refresh_trade_counts would have reset this to 0 (no trades exist).
    // The fact that it's still 999 proves the post-ingest sequence was skipped.
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

// ── Scenario E ───────────────────────────────────────────────────────────────
// PASS: data/dune_csvs/*.csv files are renamed to *.csv.imported after
//       successful ingest. On a re-run, the .imported files are skipped
//       (no double-ingest of the same row).
// FAIL: .csv files still present after migration (would re-ingest), OR
//       .imported files re-ingested (.csv extension filter broken).

#[tokio::test]
async fn dune_csvs_archived_after_ingest() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = dir.path().join("wallet_set.json");

    // Set up data/dune_csvs/ next to the cache (matches the helper's
    // `parent.join("dune_csvs")` derivation).
    let csv_dir = dir.path().join("dune_csvs");
    std::fs::create_dir_all(&csv_dir).unwrap();
    let csv_path = csv_dir.join("dune-test-wallets.csv");
    // Minimal valid CSV: header row + 2 wallets.
    let csv = "wallet,first_seen_unix,closed_markets,win_rate_bps\n\
               0x1111111111111111111111111111111111111111,1700000000,5,8500\n\
               0x2222222222222222222222222222222222222222,1700000001,7,9000\n";
    std::fs::write(&csv_path, csv).unwrap();

    let config = config_for(&dir, cache_path.clone(), wallet_set_path);
    auto_migrate_legacy(&config, &mut cache).unwrap();

    // Original .csv gone; .csv.imported present.
    assert!(
        !csv_path.exists(),
        "original .csv file must be renamed after ingest"
    );
    let archived = csv_dir.join("dune-test-wallets.csv.imported");
    assert!(archived.exists(), "archived .csv.imported file must exist");

    // Verify wallets ingested.
    let hexes = cache
        .wallets_with_source_bit(pe_bootstrap::pile::SRC_DUNE_CSV)
        .unwrap();
    assert_eq!(hexes.len(), 2, "both CSV wallets must be in cache");

    // Re-run migration — .imported files are skipped; no error.
    auto_migrate_legacy(&config, &mut cache).unwrap();
}

// ── Scenario F ───────────────────────────────────────────────────────────────
// PASS: migration is idempotent — running it twice produces the same state
//       (same cursor values, same wallet count, no errors).
// FAIL: second run errors, OR cursor values drift, OR wallets duplicated.

#[tokio::test]
async fn auto_migrate_legacy_is_idempotent() {
    let dir = TempDir::new().unwrap();
    let (cache_path, mut cache) = open_cache_in(&dir);
    let wallet_set_path = legacy_wallet_set_post_179(&dir, true);
    let config = config_for(&dir, cache_path.clone(), wallet_set_path.clone());

    auto_migrate_legacy(&config, &mut cache).unwrap();
    let (c1, t1) = migrate::load_enum_state(&cache).unwrap();
    let n1 = cache
        .wallets_with_source_bit(pe_bootstrap::pile::SRC_WALLET_SET_JSON)
        .unwrap()
        .len();

    // Run again — file is gone, so this should be a no-op.
    auto_migrate_legacy(&config, &mut cache).unwrap();
    let (c2, t2) = migrate::load_enum_state(&cache).unwrap();
    let n2 = cache
        .wallets_with_source_bit(pe_bootstrap::pile::SRC_WALLET_SET_JSON)
        .unwrap()
        .len();

    assert_eq!(c1, c2, "contracts cursor must be stable across re-runs");
    assert_eq!(t1, t2, "topics cursor must be stable across re-runs");
    assert_eq!(n1, n2, "wallet count must not grow on idempotent re-run");
}
