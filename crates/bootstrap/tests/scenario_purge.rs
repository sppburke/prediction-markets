//! Scenario tests for `pe-bootstrap purge` (issue #385).
//!
//! Deterministic, no network. Cache-level invariants (override-by-source-bit,
//! atomic tombstone, rule-B freshness/recency, the activation guard) use a fixed
//! `now_unix`; the two `run_purge` end-to-end scenarios use the wall clock with a
//! wide (24 h / 14 d) slack so the PASS/FAIL outcome is clock-independent.
//!
//! Each scenario states a single PASS criterion before the test body.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;

use pe_bootstrap::BootstrapConfig;
use pe_bootstrap::cache::{PurgeReason, PurgeRow, WalletCache};
use pe_bootstrap::pile::{
    BACKFILL_STALENESS_SECS, PILE_ACTIVATION_MIN_TRADES, SRC_DATADASH, SRC_LEADERBOARD, SRC_TRADES,
};
use pe_bootstrap::purge::{run_infra_purge, run_purge};
use tempfile::TempDir;

const DAY: i64 = 86_400;

fn wallet_hex(byte: u8) -> String {
    format!("0x{byte:040x}")
}

fn open_cache(dir: &TempDir) -> WalletCache {
    WalletCache::open(&dir.path().join("cache.db")).unwrap()
}

/// Names of every index on the `trades` table (issue #401), via the same PRAGMA
/// family the `crypto-shadow` index-assertion precedent uses. Includes SQLite's
/// auto-index for the `source_trade_id` PK, so assert presence/absence of named
/// indexes rather than exact set equality.
fn trades_index_names(cache: &WalletCache) -> HashSet<String> {
    let conn = cache.raw_conn_for_test();
    let mut stmt = conn.prepare("PRAGMA index_list(trades)").unwrap();
    // index_list columns: (seq, name, unique, origin, partial) — name is col 1.
    stmt.query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// Key columns of `index`, in index order (issue #401). `PRAGMA index_info`
/// returns one row per key column in `seqno` order; `name` is col 2.
fn index_columns(cache: &WalletCache, index: &str) -> Vec<String> {
    let conn = cache.raw_conn_for_test();
    let mut stmt = conn
        .prepare(&format!("PRAGMA index_info({index})"))
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(2))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// `PRAGMA freelist_count` — free (unused) pages in the main DB file (issue #401).
/// VACUUM drains this to 0; a plain DELETE (no `auto_vacuum`) only grows it.
fn freelist_count(cache: &WalletCache) -> i64 {
    cache
        .raw_conn_for_test()
        .query_row("PRAGMA freelist_count", [], |r| r.get(0))
        .unwrap()
}

/// `PRAGMA page_count` — total pages in the main DB file (issue #401). Only
/// free-page reclamation (conversion VACUUM / incremental_vacuum, #538) shrinks
/// it; a plain DELETE leaves it unchanged (freed pages go to the freelist).
fn page_count(cache: &WalletCache) -> i64 {
    cache
        .raw_conn_for_test()
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap()
}

/// Upsert a fresh row (bulk path, so the tombstone gate is exercised on re-admit).
fn upsert(cache: &mut WalletCache, w: &str, bits: i64) {
    cache
        .upsert_wallets_bulk(&[(w.to_owned(), bits, false, None, None, None, 0)])
        .unwrap();
}

fn proven(w: &str) -> PurgeRow {
    PurgeRow {
        wallet_hex: w.to_owned(),
        reason: PurgeReason::ProvenLoser,
    }
}

fn dead(w: &str) -> PurgeRow {
    PurgeRow {
        wallet_hex: w.to_owned(),
        reason: PurgeReason::DeadWeight,
    }
}

/// Tombstone `w` (create a row + trade, then armed-purge it as a proven loser),
/// re-discover it via `bit`, and report `(still_purged, row_exists)`.
fn lift_outcome(bit: i64) -> (bool, bool) {
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let w = wallet_hex(0x11);
    upsert(&mut cache, &w, SRC_LEADERBOARD);
    cache.conn_for_test_insert_trade(&w, "t1", 1_000);
    cache.purge_wallets(&[proven(&w)], 2_000, false).unwrap();
    assert!(cache.is_purged(&w).unwrap(), "setup: wallet not tombstoned");
    assert!(
        !cache.conn_for_test_wallet_exists(&w),
        "setup: wallet row not deleted"
    );
    // Re-discovery via the source under test.
    upsert(&mut cache, &w, bit);
    (
        cache.is_purged(&w).unwrap(),
        cache.conn_for_test_wallet_exists(&w),
    )
}

// ── Override: leaderboard lifts; datadash / trades / retired-radion do not ────

#[test]
fn leaderboard_rediscovery_lifts_tombstone() {
    // PASS: re-discovery via SRC_LEADERBOARD lifts the tombstone and re-admits.
    let (still_purged, exists) = lift_outcome(SRC_LEADERBOARD);
    assert!(!still_purged && exists, "leaderboard must lift + re-admit");
    println!("PASS: leaderboard re-discovery lifts the tombstone and re-admits the wallet");
}

#[test]
fn retired_radion_bit_no_longer_lifts() {
    // Regression guard for the Radion retirement: bit 32 was SRC_RADION and used to
    // lift the tombstone. After retirement it is dropped from TOMBSTONE_OVERRIDE_SOURCES
    // (48 → 16), so a legacy row carrying only bit 32 must NOT lift (32 & 16 == 0).
    let (still_purged, exists) = lift_outcome(0b0100000);
    assert!(
        still_purged && !exists,
        "retired radion bit 32 must NOT lift (32 & 16 == 0)"
    );
    println!("PASS: retired radion bit (32) no longer lifts the tombstone");
}

#[test]
fn datadash_rediscovery_does_not_lift() {
    // PASS: re-discovery via SRC_DATADASH leaves the tombstone intact + no row.
    let (still_purged, exists) = lift_outcome(SRC_DATADASH);
    assert!(
        still_purged && !exists,
        "datadash must NOT lift (128 & 16 == 0)"
    );
    println!(
        "PASS: datadash re-discovery does NOT lift — tombstone intact, wallet not re-inserted"
    );
}

#[test]
fn migrate_trades_rediscovery_does_not_lift() {
    // PASS: re-discovery via SRC_TRADES (the migrate path) does not lift.
    let (still_purged, exists) = lift_outcome(SRC_TRADES);
    assert!(
        still_purged && !exists,
        "trades/migrate must NOT lift (2 & 16 == 0)"
    );
    println!("PASS: trades/migrate re-discovery does NOT lift — tombstone intact by construction");
}

// ── Armed purge: delete both rules, tombstone only rule A (atomic) ────────────

#[test]
fn armed_purge_deletes_both_tombstones_proven_loser_only() {
    // PASS: an armed purge removes both wallets from wallets+trades+snapshots and
    // writes a tombstone for the proven loser ONLY — and the proven loser is never
    // observed deleted-without-its-tombstone (atomicity postcondition).
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let wa = wallet_hex(0xa);
    let wb = wallet_hex(0xb);
    for w in [&wa, &wb] {
        upsert(&mut cache, w, SRC_LEADERBOARD);
        cache.conn_for_test_insert_trade(w, &format!("{w}-t"), 1_000);
        cache.conn_for_test_insert_snapshot(w, 1_000);
    }

    let report = cache
        .purge_wallets(&[proven(&wa), dead(&wb)], 5_000, false)
        .unwrap();

    // Atomicity: proven loser deleted AND tombstoned (never one without the other).
    assert!(!cache.conn_for_test_wallet_exists(&wa) && cache.is_purged(&wa).unwrap());
    // Dead weight deleted, NOT tombstoned.
    assert!(!cache.conn_for_test_wallet_exists(&wb) && !cache.is_purged(&wb).unwrap());
    assert!(cache.trades_for(&wa).is_empty() && cache.trades_for(&wb).is_empty());
    assert_eq!(report.proven_losers_deleted, 1);
    assert_eq!(report.dead_weight_deleted, 1);
    assert_eq!(report.tombstones_written, 1);
    assert_eq!(report.trades_deleted, 2);
    assert_eq!(report.snapshots_deleted, 2);
    assert!(!report.dry_run);
    println!(
        "PASS: armed purge deletes both, tombstones only the proven loser (delete+tombstone atomic)"
    );
}

// ── Dry run: estimate only, write nothing ────────────────────────────────────

#[test]
fn dry_run_estimates_and_writes_nothing() {
    // PASS: a dry-run leaves every row intact and writes no tombstone; the report
    // reflects would-delete counts (trades estimated from wallets.trade_count).
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let wa = wallet_hex(0xa);
    let wb = wallet_hex(0xb);
    upsert(&mut cache, &wa, SRC_LEADERBOARD);
    upsert(&mut cache, &wb, SRC_LEADERBOARD);
    cache.conn_for_test_set_trade_count(&wa, 5);
    cache.conn_for_test_set_trade_count(&wb, 7);
    cache.conn_for_test_insert_snapshot(&wa, 1_000);
    cache.conn_for_test_insert_snapshot(&wb, 1_000);

    let report = cache
        .purge_wallets(&[proven(&wa), dead(&wb)], 5_000, true)
        .unwrap();

    assert!(report.dry_run);
    assert!(cache.conn_for_test_wallet_exists(&wa) && cache.conn_for_test_wallet_exists(&wb));
    assert!(!cache.is_purged(&wa).unwrap() && !cache.is_purged(&wb).unwrap());
    assert_eq!(report.proven_losers_deleted, 1);
    assert_eq!(report.dead_weight_deleted, 1);
    assert_eq!(report.tombstones_written, 1);
    assert_eq!(
        report.trades_deleted, 12,
        "trade estimate = SUM(trade_count) 5+7"
    );
    assert_eq!(report.snapshots_deleted, 2);
    println!("PASS: dry-run deletes nothing, writes no tombstone, reports would-delete counts");
}

// ── Defense-in-depth: activation never reactivates a tombstoned wallet ────────

#[test]
fn activation_skips_tombstoned_wallet() {
    // PASS: apply_activation_rules never sets is_active=1 for a tombstoned wallet
    // even if a stray row exists; a non-tombstoned leaderboard wallet activates.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let w = wallet_hex(0x1);
    let c = wallet_hex(0x2);
    // Tombstone w (create + armed-purge).
    upsert(&mut cache, &w, SRC_LEADERBOARD);
    cache.conn_for_test_insert_trade(&w, "t", 1_000);
    cache.purge_wallets(&[proven(&w)], 2_000, false).unwrap();
    // Stray row for w via the single-wallet path (ungated) — the defense case.
    cache
        .upsert_wallet(&w, SRC_LEADERBOARD, false, None, None, None)
        .unwrap();
    // Control: a normal, non-tombstoned leaderboard wallet.
    cache
        .upsert_wallet(&c, SRC_LEADERBOARD, false, None, None, None)
        .unwrap();

    cache
        .apply_activation_rules(PILE_ACTIVATION_MIN_TRADES)
        .unwrap();

    let active: HashSet<String> = cache
        .active_tradeable_wallet_hexes()
        .unwrap()
        .into_iter()
        .collect();
    assert!(active.contains(&c), "control wallet should activate");
    assert!(!active.contains(&w), "tombstoned wallet must NOT activate");
    println!("PASS: activation guard blocks a tombstoned wallet; non-tombstoned control activates");
}

// ── Rule-B candidate selection: freshness gate + recency + NULL handling ──────

#[test]
fn dead_weight_candidates_freshness_and_recency() {
    // PASS: only an active, refreshed-this-run, dormant>14d wallet is a candidate;
    // a stale-fetch (soft-failed backfill), recently-active, inactive, or
    // zero-trade wallet is excluded.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let t = 1_700_000_000_i64;

    let w1 = wallet_hex(0x1); // active, fresh fetch, dormant → candidate
    let w2 = wallet_hex(0x2); // active, STALE fetch, dormant → excluded
    let w3 = wallet_hex(0x3); // active, fresh fetch, recent trade → excluded
    let w4 = wallet_hex(0x4); // INACTIVE, fresh fetch, dormant → excluded
    let w5 = wallet_hex(0x5); // active, fresh fetch, NO trades → excluded

    for w in [&w1, &w2, &w3, &w4, &w5] {
        upsert(&mut cache, w, SRC_LEADERBOARD);
    }
    for w in [&w1, &w2, &w3, &w5] {
        cache.conn_for_test_set_active(w, 1);
    }
    cache.update_last_polymarket_fetch(&w1, t).unwrap();
    cache
        .update_last_polymarket_fetch(&w2, t - 2 * DAY)
        .unwrap(); // stale
    cache.update_last_polymarket_fetch(&w3, t).unwrap();
    cache.update_last_polymarket_fetch(&w4, t).unwrap();
    cache.update_last_polymarket_fetch(&w5, t).unwrap();
    cache.conn_for_test_insert_trade(&w1, "1", t - 30 * DAY);
    cache.conn_for_test_insert_trade(&w2, "2", t - 30 * DAY);
    cache.conn_for_test_insert_trade(&w3, "3", t - DAY); // recent
    cache.conn_for_test_insert_trade(&w4, "4", t - 30 * DAY);

    let got: HashSet<String> = cache
        .select_dead_weight_candidates(t, 14 * DAY, BACKFILL_STALENESS_SECS)
        .unwrap()
        .into_iter()
        .collect();
    let want: HashSet<String> = [w1].into_iter().collect();
    assert_eq!(got, want, "only the fresh+dormant active wallet qualifies");
    println!(
        "PASS: rule-B candidates = fresh+dormant only (stale/recent/inactive/zero-trade excluded)"
    );
}

// ── run_purge end-to-end: armed + dry-run ────────────────────────────────────

fn write_csv(dir: &TempDir, body: &str) -> String {
    let path = dir.path().join("ranked_72hr_buyandhold.csv");
    std::fs::write(&path, body).unwrap();
    path.to_string_lossy().into_owned()
}

/// `cache_path` is derived from the CSV's directory (== the test tempdir, matching
/// [`open_cache`]) so the archive-before-DELETE artifact lands inside the tempdir —
/// never at the default `data/` path relative to the test cwd.
fn cfg(csv: String, enabled: bool) -> BootstrapConfig {
    let cache_path = std::path::Path::new(&csv).with_file_name("cache.db");
    BootstrapConfig {
        purge_decision_csv: Some(csv),
        purge_enabled: enabled,
        cache_path,
        ..BootstrapConfig::default()
    }
}

/// Like [`cfg`] but pins `purge_bulk_min_wallets` (issue #401) so a scenario can
/// force the bulk path (low threshold) or the incremental path (huge threshold)
/// regardless of the small delete-set sizes these deterministic fixtures use.
fn cfg_with_bulk_min(csv: String, enabled: bool, bulk_min: u64) -> BootstrapConfig {
    BootstrapConfig {
        purge_bulk_min_wallets: bulk_min,
        ..cfg(csv, enabled)
    }
}

/// Set up an active wallet with a fresh fetch + one trade at `trade_ts`.
fn active_with_trade(cache: &mut WalletCache, w: &str, fetch_ts: i64, trade_ts: i64) {
    upsert(cache, w, SRC_LEADERBOARD);
    cache.conn_for_test_set_active(w, 1);
    cache.update_last_polymarket_fetch(w, fetch_ts).unwrap();
    cache.conn_for_test_insert_trade(w, &format!("{w}-t"), trade_ts);
}

/// An active, freshly-fetched, dormant (rule-B) wallet carrying `n` trades (issue
/// #401). Sized so its delete frees ≥1 full page (page_size 4096, `auto_vacuum`
/// OFF) — otherwise the freelist/page-count assertions would be vacuous.
fn dead_weight_with_n_trades(
    cache: &mut WalletCache,
    w: &str,
    fetch_ts: i64,
    trade_ts: i64,
    n: usize,
) {
    upsert(cache, w, SRC_LEADERBOARD);
    cache.conn_for_test_set_active(w, 1);
    cache.update_last_polymarket_fetch(w, fetch_ts).unwrap();
    for i in 0..n {
        cache.conn_for_test_insert_trade(w, &format!("{w}-{i}"), trade_ts);
    }
}

#[test]
fn run_purge_armed_end_to_end() {
    // PASS: an armed run deletes+tombstones the rule-A loser, deletes (no tombstone)
    // the rule-B dead weight, and spares the eligible winner.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();

    let wa = wallet_hex(0xa); // eligible loser (rule A)
    let wb = wallet_hex(0xb); // eligible winner (spared) + keeps the cache fresh
    let wc = wallet_hex(0xc); // dead weight (rule B): not eligible, dormant
    active_with_trade(&mut cache, &wa, now, now - 2 * DAY);
    active_with_trade(&mut cache, &wb, now, now - 3_600); // recent → cache fresh
    active_with_trade(&mut cache, &wc, now, now - 30 * DAY);

    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000a,-3.0,-0.5,50,True\n\
         0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n",
    );
    let report = run_purge(&cfg(csv, true), &mut cache, false).unwrap();

    assert!(!cache.conn_for_test_wallet_exists(&wa) && cache.is_purged(&wa).unwrap());
    assert!(!cache.conn_for_test_wallet_exists(&wc) && !cache.is_purged(&wc).unwrap());
    assert!(
        cache.conn_for_test_wallet_exists(&wb),
        "eligible winner spared"
    );
    assert_eq!(report.proven_losers_deleted, 1);
    assert_eq!(report.dead_weight_deleted, 1);
    assert!(!report.dry_run);
    println!("PASS: armed run_purge — rule-A tombstoned, rule-B deleted, eligible winner spared");
}

#[test]
fn run_purge_dry_run_reports_without_deleting() {
    // PASS: with purge_enabled=false the run deletes nothing and writes no
    // tombstone, but the report still names the rule-A loser as would-delete.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();

    let wa = wallet_hex(0xa);
    let wb = wallet_hex(0xb);
    active_with_trade(&mut cache, &wa, now, now - 2 * DAY);
    active_with_trade(&mut cache, &wb, now, now - 3_600); // keeps cache fresh

    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000a,-3.0,-0.5,50,True\n\
         0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n",
    );
    let report = run_purge(&cfg(csv, false), &mut cache, false).unwrap();

    assert!(report.dry_run);
    assert!(
        cache.conn_for_test_wallet_exists(&wa),
        "nothing deleted in dry-run"
    );
    assert!(
        !cache.is_purged(&wa).unwrap(),
        "no tombstone written in dry-run"
    );
    assert_eq!(
        report.proven_losers_deleted, 1,
        "rule-A loser reported as would-delete"
    );
    println!("PASS: dry-run run_purge reports the rule-A loser but deletes nothing");
}

#[test]
fn infra_purge_is_armed_durable_and_invalidates_wallet_derived_data() {
    // PASS: purge-infra ignores ordinary purge_enabled=false, archives and
    // deletes only infra wallet-keyed rows, writes a non-liftable exclusion,
    // clears the first-mover cache, and preserves activation-batch audit.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let infra = wallet_hex(0x31);
    let normal = wallet_hex(0x32);
    upsert(&mut cache, &infra, SRC_LEADERBOARD);
    upsert(&mut cache, &normal, SRC_LEADERBOARD);
    cache.mark_infra(&infra).unwrap();
    cache.conn_for_test_insert_trade(&infra, "infra-trade", 1_000);
    cache.conn_for_test_insert_trade(&normal, "normal-trade", 1_001);
    cache
        .raw_conn_for_test()
        .execute_batch(
            "CREATE TABLE wallet_features (wallet_hex TEXT PRIMARY KEY, score INTEGER); \
             INSERT INTO wallet_features VALUES \
             ('0x0000000000000000000000000000000000000031', 1), \
             ('0x0000000000000000000000000000000000000032', 2); \
             INSERT INTO first_mover_rank_cache \
             (cutoff_unix, market_id, outcome_id, ts_json) VALUES (1, 'm', 0, '[]');",
        )
        .unwrap();
    let batch = cache
        .activate_next_batch("historical", 1, PILE_ACTIVATION_MIN_TRADES, 999)
        .unwrap();
    assert!(!batch.wallet_hexes.is_empty());

    let config = BootstrapConfig {
        cache_path: dir.path().join("cache.db"),
        purge_enabled: false,
        purge_bulk_min_wallets: u64::MAX,
        ..BootstrapConfig::default()
    };
    let preview = run_infra_purge(&config, &mut cache, true).unwrap();
    assert!(preview.dry_run);
    assert_eq!(preview.infrastructure_deleted, 1);
    assert!(cache.conn_for_test_wallet_exists(&infra));

    let report = run_infra_purge(&config, &mut cache, false).unwrap();
    assert!(!report.dry_run);
    assert_eq!(report.infrastructure_deleted, 1);
    assert_eq!(report.wallet_features_deleted, 1);
    assert_eq!(report.tombstones_written, 1);
    assert!(!cache.conn_for_test_wallet_exists(&infra));
    assert!(cache.conn_for_test_wallet_exists(&normal));
    assert!(cache.is_purged(&infra).unwrap());
    let conn = cache.raw_conn_for_test();
    let feature_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM wallet_features", [], |row| row.get(0))
        .unwrap();
    assert_eq!(feature_rows, 1, "non-infra feature row preserved");
    let rank_cache_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM first_mover_rank_cache", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(rank_cache_rows, 0, "derived cache invalidated");
    let audit_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM wallet_activation_batch_wallets WHERE batch_id='historical'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(audit_rows, 1, "activation audit survives live-wallet purge");

    upsert(&mut cache, &infra, SRC_LEADERBOARD);
    assert!(
        !cache.conn_for_test_wallet_exists(&infra),
        "leaderboard cannot lift an infra exclusion"
    );
    assert!(cache.clear_infra_exclusion(&infra).unwrap());
    upsert(&mut cache, &infra, SRC_LEADERBOARD);
    assert!(cache.conn_for_test_wallet_exists(&infra));

    let archive = rusqlite::Connection::open(dir.path().join("cache.purge-archive.db")).unwrap();
    let reason: String = archive
        .query_row(
            "SELECT reason FROM purge_manifest WHERE wallet_hex=?1",
            [&infra],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(reason, "infra");
}

// ── #401: bulk-delete index drop/rebuild + SCHEMA-on-open backstop ────────────

#[test]
fn drop_then_create_trades_indexes_roundtrips() {
    // PASS: drop removes exactly the two non-lookup `trades` indexes (the
    // `wallet_hex` lookup index is kept), and create rebuilds them — the covering
    // index with its exact 5-column order.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);

    let base = trades_index_names(&cache);
    assert!(
        base.contains("idx_trades_wallet_ts"),
        "lookup index present at open"
    );
    assert!(base.contains("idx_trades_market_id"));
    assert!(base.contains("idx_trades_buy_market_outcome_wallet_ts"));

    cache.drop_trades_bulk_delete_indexes().unwrap();
    let dropped = trades_index_names(&cache);
    assert!(
        dropped.contains("idx_trades_wallet_ts"),
        "lookup index must survive the drop"
    );
    assert!(
        !dropped.contains("idx_trades_market_id"),
        "market_id index must be dropped"
    );
    assert!(
        !dropped.contains("idx_trades_buy_market_outcome_wallet_ts"),
        "covering index must be dropped"
    );

    cache.create_trades_bulk_delete_indexes().unwrap();
    let rebuilt = trades_index_names(&cache);
    assert!(
        rebuilt.contains("idx_trades_market_id"),
        "market_id index rebuilt"
    );
    assert!(
        rebuilt.contains("idx_trades_buy_market_outcome_wallet_ts"),
        "covering index rebuilt"
    );
    assert_eq!(
        index_columns(&cache, "idx_trades_buy_market_outcome_wallet_ts"),
        [
            "side",
            "market_id",
            "outcome_id",
            "wallet_hex",
            "timestamp_unix"
        ],
        "covering index rebuilt with its exact 5-column order"
    );
    println!(
        "PASS: drop keeps the lookup index + removes the two non-lookup indexes; create rebuilds them with the exact 5-column order"
    );
}

#[test]
fn armed_run_purge_keeps_trades_indexes_intact() {
    // PASS: after a full armed run_purge in BULK mode (drop → delete → reclaim →
    // recreate) all three trades indexes are present and the covering index keeps
    // its 5-column order — the drop/rebuild dance leaves a correct schema. Forced
    // bulk via threshold=1 (issue #401), since the 2-wallet delete-set would
    // otherwise take the incremental path that never drops an index.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();

    let wa = wallet_hex(0xa); // eligible loser (rule A)
    let wb = wallet_hex(0xb); // eligible winner (spared) + keeps cache fresh
    let wc = wallet_hex(0xc); // dead weight (rule B)
    active_with_trade(&mut cache, &wa, now, now - 2 * DAY);
    active_with_trade(&mut cache, &wb, now, now - 3_600);
    active_with_trade(&mut cache, &wc, now, now - 30 * DAY);

    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000a,-3.0,-0.5,50,True\n\
         0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n",
    );
    run_purge(&cfg_with_bulk_min(csv, true, 1), &mut cache, false).unwrap();

    let idx = trades_index_names(&cache);
    assert!(idx.contains("idx_trades_wallet_ts"), "lookup index intact");
    assert!(
        idx.contains("idx_trades_market_id"),
        "market_id index rebuilt"
    );
    assert!(
        idx.contains("idx_trades_buy_market_outcome_wallet_ts"),
        "covering index rebuilt"
    );
    assert_eq!(
        index_columns(&cache, "idx_trades_buy_market_outcome_wallet_ts"),
        [
            "side",
            "market_id",
            "outcome_id",
            "wallet_hex",
            "timestamp_unix"
        ],
    );
    println!(
        "PASS: armed run_purge leaves all three trades indexes present with the covering index's 5-column order intact"
    );
}

#[test]
fn schema_on_open_heals_dropped_trades_indexes() {
    // PASS: if a crash leaves the two non-lookup indexes dropped, the next
    // WalletCache::open recreates them from SCHEMA (the absent-index backstop).
    let dir = TempDir::new().unwrap();
    {
        let mut cache = open_cache(&dir);
        cache.drop_trades_bulk_delete_indexes().unwrap();
        let dropped = trades_index_names(&cache);
        assert!(
            !dropped.contains("idx_trades_market_id")
                && !dropped.contains("idx_trades_buy_market_outcome_wallet_ts"),
            "setup: both indexes dropped before reopen"
        );
    }
    // Reopen the same DB file → execute_batch(SCHEMA) runs again on open.
    let cache = open_cache(&dir);
    let healed = trades_index_names(&cache);
    assert!(
        healed.contains("idx_trades_market_id"),
        "SCHEMA-on-open recreated market_id"
    );
    assert!(
        healed.contains("idx_trades_buy_market_outcome_wallet_ts"),
        "SCHEMA-on-open recreated covering index"
    );
    assert_eq!(
        index_columns(&cache, "idx_trades_buy_market_outcome_wallet_ts"),
        [
            "side",
            "market_id",
            "outcome_id",
            "wallet_hex",
            "timestamp_unix"
        ],
        "healed covering index keeps its 5-column order"
    );
    println!(
        "PASS: SCHEMA-on-open recreates the two dropped trades indexes with the covering index's exact column order"
    );
}

// ── #401: size-gated bulk vs incremental purge ───────────────────────────────

#[test]
fn run_purge_bulk_mode_vacuums_and_reclaims() {
    // PASS: an armed purge whose delete-set >= purge_bulk_min_wallets runs in BULK
    // mode — the two non-lookup indexes are dropped+rebuilt (present afterward) AND
    // free-page reclamation (#538: incremental_vacuum here — a fresh test db is
    // born auto_vacuum=INCREMENTAL) drains the freelist and shrinks the file.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();

    let wfresh = wallet_hex(0xb); // eligible winner: recent trade keeps cache fresh, spared
    let wd = wallet_hex(0xd); // dead weight (rule B): many dormant trades → the delete set
    active_with_trade(&mut cache, &wfresh, now, now - 3_600);
    dead_weight_with_n_trades(&mut cache, &wd, now, now - 30 * DAY, 1_000);

    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n",
    );

    let pages_before = page_count(&cache);
    // threshold 1 ⇒ the 1-wallet delete-set forces bulk mode.
    let report = run_purge(&cfg_with_bulk_min(csv, true, 1), &mut cache, false).unwrap();

    assert_eq!(report.dead_weight_deleted, 1, "rule-B dead weight deleted");
    assert!(
        !cache.conn_for_test_wallet_exists(&wd),
        "dead weight removed"
    );
    assert!(
        cache.conn_for_test_wallet_exists(&wfresh),
        "fresh winner spared"
    );
    // Dropped-then-rebuilt: the two secondary indexes are present again.
    let idx = trades_index_names(&cache);
    assert!(idx.contains("idx_trades_market_id"), "market_id rebuilt");
    assert!(
        idx.contains("idx_trades_buy_market_outcome_wallet_ts"),
        "covering index rebuilt"
    );
    // Reclamation ran: freelist drained to zero and the file shrank.
    assert_eq!(
        freelist_count(&cache),
        0,
        "bulk reclamation drains the freelist"
    );
    let pages_after = page_count(&cache);
    assert!(
        pages_after < pages_before,
        "bulk reclamation shrinks the file (after {pages_after} !< before {pages_before})"
    );
    println!(
        "PASS: bulk-mode run_purge rebuilds both indexes and reclamation drains the freelist (file shrank {pages_before}→{pages_after} pages)"
    );
}

#[test]
fn run_purge_subthreshold_mode_keeps_indexes_no_reclamation() {
    // PASS: an armed purge whose delete-set < purge_bulk_min_wallets runs in
    // SUBTHRESHOLD mode — the two secondary indexes are never dropped (present) and
    // no reclamation runs (freelist_count > 0 and page_count unchanged; #538
    // recovery reclamation fires here only when reclamation_pending is set).
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();

    let wfresh = wallet_hex(0xb);
    let wd = wallet_hex(0xd);
    active_with_trade(&mut cache, &wfresh, now, now - 3_600);
    dead_weight_with_n_trades(&mut cache, &wd, now, now - 30 * DAY, 1_000);

    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n",
    );

    let pages_before = page_count(&cache);
    // threshold 1_000_000 ⇒ the 1-wallet delete-set stays under it ⇒ incremental.
    let report = run_purge(&cfg_with_bulk_min(csv, true, 1_000_000), &mut cache, false).unwrap();

    assert_eq!(
        report.dead_weight_deleted, 1,
        "rule-B dead weight still deleted"
    );
    assert!(
        !cache.conn_for_test_wallet_exists(&wd),
        "dead weight removed"
    );
    // Indexes never dropped: incremental keeps them live.
    let idx = trades_index_names(&cache);
    assert!(idx.contains("idx_trades_market_id"), "market_id index live");
    assert!(
        idx.contains("idx_trades_buy_market_outcome_wallet_ts"),
        "covering index live"
    );
    // No reclamation: freed pages sit on the freelist and the file does not shrink.
    assert!(
        freelist_count(&cache) > 0,
        "subthreshold delete frees pages to the freelist (no reclamation)"
    );
    assert_eq!(
        page_count(&cache),
        pages_before,
        "subthreshold mode does not reclaim ⇒ page_count unchanged"
    );
    println!(
        "PASS: subthreshold run_purge keeps both indexes live and skips reclamation (freelist > 0, file unchanged at {pages_before} pages)"
    );
}

// ── #538: incremental reclamation + pending-marker recovery ──────────────────

#[test]
fn legacy_mode0_db_converts_via_full_vacuum() {
    // PASS: a pre-existing auto_vacuum=NONE db (created WITHOUT the open pragma,
    // like the production cache) stays mode 0 through open, and the first bulk
    // purge's reclamation takes the full-VACUUM path — the one-time conversion —
    // after which the db reads back auto_vacuum=2 (incremental).
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cache.db");
    {
        // Raw pre-creation with any DDL fixes the header at mode 0.
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE legacy_seed (x INTEGER);")
            .unwrap();
    }
    let mut cache = WalletCache::open(&db_path).unwrap();
    let av: i64 = cache
        .raw_conn_for_test()
        .query_row("PRAGMA auto_vacuum", [], |r| r.get(0))
        .unwrap();
    assert_eq!(av, 0, "existing non-empty db stays mode 0 through open");

    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let wfresh = wallet_hex(0xb); // fresh eligible winner: passes the staleness guard, spared
    let wd = wallet_hex(0xd);
    active_with_trade(&mut cache, &wfresh, now, now - 3_600);
    dead_weight_with_n_trades(&mut cache, &wd, now, now - 30 * DAY, 1_000);
    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n",
    );

    run_purge(&cfg_with_bulk_min(csv, true, 1), &mut cache, false).unwrap();

    let av_after: i64 = cache
        .raw_conn_for_test()
        .query_row("PRAGMA auto_vacuum", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        av_after, 2,
        "conversion VACUUM flipped the db to incremental"
    );
    assert_eq!(
        freelist_count(&cache),
        0,
        "conversion VACUUM drained the freelist"
    );
    let idx = trades_index_names(&cache);
    assert!(idx.contains("idx_trades_market_id"));
    assert!(idx.contains("idx_trades_buy_market_outcome_wallet_ts"));
    println!("PASS: legacy mode-0 db converted to incremental via the bulk purge's full VACUUM");
}

#[test]
fn bulk_error_ordering_recreates_then_propagates() {
    // PASS: an injected mid-delete failure (tombstone table dropped) makes the
    // bulk purge return Err, but the recreate-then-propagate contract still
    // rebuilds both indexes, reclamation is skipped, and reclamation_pending
    // stays SET so the next purge recovers (#538 ordering contract).
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();

    // A rule-A proven loser: its delete writes a tombstone -> the injection point.
    let wl = wallet_hex(0xa);
    active_with_trade(&mut cache, &wl, now, now - 3_600);
    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000a,-9.0,-0.5,40,True\n",
    );
    cache
        .raw_conn_for_test()
        .execute_batch("DROP TABLE purged_wallets;")
        .unwrap();

    let config = BootstrapConfig {
        purge_archive_enabled: false,
        ..cfg_with_bulk_min(csv, true, 1)
    };
    let err = run_purge(&config, &mut cache, false).unwrap_err();
    assert!(
        format!("{err}").contains("purged_wallets"),
        "the DELETE stage's error wins the precedence (got: {err})"
    );

    let idx = trades_index_names(&cache);
    assert!(
        idx.contains("idx_trades_market_id")
            && idx.contains("idx_trades_buy_market_outcome_wallet_ts"),
        "recreate-then-propagate rebuilt both indexes despite the delete error"
    );
    assert!(
        cache.reclamation_pending().unwrap(),
        "marker stays set after a failed bulk run — recovery owed"
    );
    println!("PASS: failed bulk delete still recreates indexes and leaves reclamation_pending set");
}

#[test]
fn infra_purge_never_services_pending_recovery() {
    // #538 H-finding regression: purge-infra runs PRE-ranking and is fatal to
    // the cycle — a pending marker must NOT trigger reclamation there.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    cache.set_reclamation_pending().unwrap();

    let config = BootstrapConfig {
        purge_archive_enabled: false,
        cache_path: dir.path().join("cache.db"),
        ..BootstrapConfig::default()
    };
    run_infra_purge(&config, &mut cache, false).unwrap();
    assert!(
        cache.reclamation_pending().unwrap(),
        "infra purge must leave the marker for the ordinary post-publication purge"
    );
    println!("PASS: purge-infra ignores reclamation_pending (recovery deferred to Stage 4)");
}

#[test]
fn reclamation_pending_recovery_via_subthreshold_purge() {
    // PASS (end-to-end shape of the #538 recovery; the no-DROP-INDEX proof is the
    // in-crate trace test): with the marker set and an empty delete set, an armed
    // subthreshold purge reclaims, ensures indexes, and clears the marker.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let wfresh = wallet_hex(0xb); // fresh eligible wallet satisfies the staleness guard
    active_with_trade(&mut cache, &wfresh, now, now - 3_600);
    cache.set_reclamation_pending().unwrap();
    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n",
    );

    let report = run_purge(&cfg(csv, true), &mut cache, false).unwrap();
    assert_eq!(report.proven_losers_deleted + report.dead_weight_deleted, 0);
    assert!(
        !cache.reclamation_pending().unwrap(),
        "recovery cleared the marker"
    );
    assert_eq!(
        freelist_count(&cache),
        0,
        "recovery reclamation drained the freelist"
    );
    println!("PASS: subthreshold purge with pending marker recovers reclamation and clears it");
}

// ── archive-before-DELETE (item 3.7, 2026-07-01 decision record / #417) ────────

/// The derived archive path for a test tempdir cache (`cache.db` →
/// `cache.purge-archive.db`).
fn archive_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("cache.purge-archive.db")
}

#[test]
fn armed_purge_archives_before_delete() {
    // PASS: after an armed purge, the sibling archive DB holds every doomed
    // wallet's trades + wallet row + a both-rules manifest, the spared wallet is
    // absent from the archive, and the live tables no longer hold the doomed rows.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();

    let wa = wallet_hex(0xa); // rule A: eligible proven loser (1 trade)
    let wb = wallet_hex(0xb); // spared eligible winner
    let wc = wallet_hex(0xc); // rule B: dead weight, 5 trades
    active_with_trade(&mut cache, &wa, now, now - 2 * DAY);
    active_with_trade(&mut cache, &wb, now, now - 3_600);
    dead_weight_with_n_trades(&mut cache, &wc, now, now - 30 * DAY, 5);

    let csv = write_csv(
        &dir,
        "wallet,tstat_net,mean_net,n_eff,eligible\n\
         0x000000000000000000000000000000000000000a,-3.0,-0.5,50,True\n\
         0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n",
    );
    let report = run_purge(&cfg(csv, true), &mut cache, false).unwrap();
    assert_eq!(report.proven_losers_deleted + report.dead_weight_deleted, 2);

    let apath = archive_path(&dir);
    assert!(apath.exists(), "archive DB must exist after an armed purge");
    let arch = rusqlite::Connection::open(&apath).unwrap();
    let count = |sql: &str| -> i64 { arch.query_row(sql, [], |r| r.get(0)).unwrap() };

    // Doomed wallets fully archived: wa 1 trade + wc 5 trades; both wallet rows;
    // manifest covers BOTH rules (the live tombstone table records rule A only).
    assert_eq!(count("SELECT COUNT(*) FROM trades"), 6);
    assert_eq!(count("SELECT COUNT(*) FROM wallets"), 2);
    assert_eq!(count("SELECT COUNT(*) FROM purge_manifest"), 2);
    let reason_a: String = arch
        .query_row(
            "SELECT reason FROM purge_manifest WHERE wallet_hex = ?1",
            [&wa],
            |r| r.get(0),
        )
        .unwrap();
    let reason_c: String = arch
        .query_row(
            "SELECT reason FROM purge_manifest WHERE wallet_hex = ?1",
            [&wc],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(reason_a, "proven_loser");
    assert_eq!(reason_c, "dead_weight");
    // Spared winner is NOT archived.
    let wb_rows: i64 = arch
        .query_row(
            "SELECT COUNT(*) FROM wallets WHERE wallet_hex = ?1",
            [&wb],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(wb_rows, 0);
    // And the live side is actually purged (archive happened BEFORE delete).
    assert!(!cache.conn_for_test_wallet_exists(&wa));
    assert!(!cache.conn_for_test_wallet_exists(&wc));
    assert!(cache.conn_for_test_wallet_exists(&wb));
    println!("PASS: armed purge archives all doomed rows + both-rules manifest before deleting");
}

#[test]
fn dry_run_and_disabled_archive_write_nothing() {
    // PASS: a dry-run writes no archive; an armed run with the archive knob off
    // still purges but writes no archive.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let wa = wallet_hex(0xa);
    let wb = wallet_hex(0xb);
    active_with_trade(&mut cache, &wa, now, now - 2 * DAY);
    active_with_trade(&mut cache, &wb, now, now - 3_600);
    let body = "wallet,tstat_net,mean_net,n_eff,eligible\n\
                0x000000000000000000000000000000000000000a,-3.0,-0.5,50,True\n\
                0x000000000000000000000000000000000000000b,5.0,0.2,40,True\n";

    // Dry run: nothing deleted, nothing archived.
    let csv = write_csv(&dir, body);
    run_purge(&cfg(csv.clone(), true), &mut cache, true).unwrap();
    assert!(
        !archive_path(&dir).exists(),
        "dry-run must not write an archive"
    );

    // Armed with archive disabled: purge proceeds, no archive artifact.
    let cfg_off = BootstrapConfig {
        purge_archive_enabled: false,
        ..cfg(csv, true)
    };
    run_purge(&cfg_off, &mut cache, false).unwrap();
    assert!(
        !cache.conn_for_test_wallet_exists(&wa),
        "purge still deletes"
    );
    assert!(
        !archive_path(&dir).exists(),
        "disabled archive must write nothing"
    );
    println!("PASS: dry-run and archive-disabled runs write no archive artifact");
}

#[test]
fn archive_rerun_is_idempotent_and_repurge_unions() {
    // PASS: a crash-rerun over the same delete-set inserts nothing new (OR IGNORE
    // under the unique row identities), and a wallet re-discovered with NEW trades
    // and purged again ADDS those rows without touching the originally archived
    // ones — the archive is a union across purges.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let wa = wallet_hex(0xa);
    dead_weight_with_n_trades(&mut cache, &wa, now, now - 30 * DAY, 5);

    let rows = vec![PurgeRow {
        wallet_hex: wa.clone(),
        reason: PurgeReason::DeadWeight,
    }];
    let apath = archive_path(&dir);
    let first = cache.archive_wallets(&rows, &apath, now).unwrap();
    assert_eq!(first.trades_archived, 5);

    // Crash-rerun: same rows, nothing new inserted, totals unchanged.
    let second = cache.archive_wallets(&rows, &apath, now).unwrap();
    assert_eq!(
        second.trades_archived, 0,
        "rerun inserts nothing (OR IGNORE)"
    );

    // Re-discovery: the wallet re-accumulates 2 NEW trades and is purged again.
    cache.conn_for_test_insert_trade(&wa, &format!("{wa}-new-0"), now - DAY);
    cache.conn_for_test_insert_trade(&wa, &format!("{wa}-new-1"), now - DAY);
    let third = cache.archive_wallets(&rows, &apath, now + 60).unwrap();
    assert_eq!(third.trades_archived, 2, "only the new rows are added");

    let arch = rusqlite::Connection::open(&apath).unwrap();
    let trades: i64 = arch
        .query_row("SELECT COUNT(*) FROM trades", [], |r| r.get(0))
        .unwrap();
    assert_eq!(trades, 7, "union across purges: 5 original + 2 new");
    let manifest_total: i64 = arch
        .query_row(
            "SELECT trades_archived FROM purge_manifest WHERE wallet_hex = ?1",
            [&wa],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        manifest_total, 7,
        "manifest carries the TOTAL archived count"
    );
    println!("PASS: archive rerun idempotent; re-purge unions, never overwrites");
}

#[test]
fn archive_survives_additive_main_schema_migration() {
    // PASS: after `main.trades` gains a column (an additive migration), archiving
    // into a PRE-EXISTING archive still succeeds — the mirror is reconciled by
    // name (`ALTER TABLE … ADD COLUMN`) and explicit-name inserts keep working.
    let dir = TempDir::new().unwrap();
    let mut cache = open_cache(&dir);
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let wa = wallet_hex(0xa);
    let wb = wallet_hex(0xb);
    dead_weight_with_n_trades(&mut cache, &wa, now, now - 30 * DAY, 3);
    dead_weight_with_n_trades(&mut cache, &wb, now, now - 30 * DAY, 2);

    // First archive creates the mirror at today's schema.
    let apath = archive_path(&dir);
    let rows_a = vec![PurgeRow {
        wallet_hex: wa.clone(),
        reason: PurgeReason::DeadWeight,
    }];
    cache.archive_wallets(&rows_a, &apath, now).unwrap();

    // Simulate a future additive migration on main (SQLite appends the column).
    {
        let main = rusqlite::Connection::open(dir.path().join("cache.db")).unwrap();
        main.execute_batch("ALTER TABLE trades ADD COLUMN future_col TEXT")
            .unwrap();
    }

    // Archiving another wallet against the OLD archive must still succeed.
    let rows_b = vec![PurgeRow {
        wallet_hex: wb.clone(),
        reason: PurgeReason::DeadWeight,
    }];
    let rep = cache.archive_wallets(&rows_b, &apath, now + 60).unwrap();
    assert_eq!(rep.trades_archived, 2);

    let arch = rusqlite::Connection::open(&apath).unwrap();
    let trades: i64 = arch
        .query_row("SELECT COUNT(*) FROM trades", [], |r| r.get(0))
        .unwrap();
    assert_eq!(trades, 5, "3 pre-migration + 2 post-migration rows");
    // The reconciled column exists in the archive; pre-migration rows read NULL.
    let nulls: i64 = arch
        .query_row(
            "SELECT COUNT(*) FROM trades WHERE future_col IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(nulls, 5, "old archive rows read NULL in the new column");
    println!("PASS: additive main migration never bricks an existing archive");
}
