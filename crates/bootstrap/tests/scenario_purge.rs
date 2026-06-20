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
    BACKFILL_STALENESS_SECS, PILE_ACTIVATION_MIN_TRADES, SRC_DATADASH, SRC_LEADERBOARD, SRC_RADION,
    SRC_TRADES,
};
use pe_bootstrap::purge::run_purge;
use tempfile::TempDir;

const DAY: i64 = 86_400;

fn wallet_hex(byte: u8) -> String {
    format!("0x{byte:040x}")
}

fn open_cache(dir: &TempDir) -> WalletCache {
    WalletCache::open(&dir.path().join("cache.db")).unwrap()
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

// ── Override: leaderboard / radion lift; datadash / trades do not ─────────────

#[test]
fn leaderboard_rediscovery_lifts_tombstone() {
    // PASS: re-discovery via SRC_LEADERBOARD lifts the tombstone and re-admits.
    let (still_purged, exists) = lift_outcome(SRC_LEADERBOARD);
    assert!(!still_purged && exists, "leaderboard must lift + re-admit");
    println!("PASS: leaderboard re-discovery lifts the tombstone and re-admits the wallet");
}

#[test]
fn radion_rediscovery_lifts_tombstone() {
    // PASS: re-discovery via SRC_RADION lifts the tombstone and re-admits.
    let (still_purged, exists) = lift_outcome(SRC_RADION);
    assert!(!still_purged && exists, "radion must lift + re-admit");
    println!("PASS: radion re-discovery lifts the tombstone and re-admits the wallet");
}

#[test]
fn datadash_rediscovery_does_not_lift() {
    // PASS: re-discovery via SRC_DATADASH leaves the tombstone intact + no row.
    let (still_purged, exists) = lift_outcome(SRC_DATADASH);
    assert!(
        still_purged && !exists,
        "datadash must NOT lift (128 & 48 == 0)"
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
        "trades/migrate must NOT lift (2 & 48 == 0)"
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

fn cfg(csv: String, enabled: bool) -> BootstrapConfig {
    BootstrapConfig {
        purge_decision_csv: Some(csv),
        purge_enabled: enabled,
        ..BootstrapConfig::default()
    }
}

/// Set up an active wallet with a fresh fetch + one trade at `trade_ts`.
fn active_with_trade(cache: &mut WalletCache, w: &str, fetch_ts: i64, trade_ts: i64) {
    upsert(cache, w, SRC_LEADERBOARD);
    cache.conn_for_test_set_active(w, 1);
    cache.update_last_polymarket_fetch(w, fetch_ts).unwrap();
    cache.conn_for_test_insert_trade(w, &format!("{w}-t"), trade_ts);
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
