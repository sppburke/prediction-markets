//! Scenario tests for the read-only `coverage` backtest-readiness probe (issue #208).
//!
//! Each scenario has a single PASS/FAIL criterion written before the test body.
//! No network calls; all data is constructed in-process. Clock is fixed via
//! hardcoded timestamps; no RNG is used. Each scenario seeds a cache via the
//! read-write handle, drops it, then runs `run_coverage` — exercising
//! `WalletCache::open_read_only` + `coverage_counts` end-to-end.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::{CoverageReport, WalletCache};
use pe_bootstrap::coverage::run_coverage;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const WALLET_1_HEX: &str = "0x1111111111111111111111111111111111111111";
const WALLET_2_HEX: &str = "0x2222222222222222222222222222222222222222";
const MARKET_A: &str = "0xaaaa";
const MARKET_B: &str = "0xbbbb";
// Polymarket source-bit (any non-zero tag; coverage does not read it).
const SOURCE_BITS: i64 = 1;

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

/// A single trade by `wallet` on `market_hex`. Only `market_id` matters for the
/// `missing_*` counts; the other fields are fixed, valid placeholders.
fn trade(wallet: WalletAddress, market_hex: &str, id: &str, ts: i64) -> RawTrade {
    RawTrade {
        wallet,
        market_id: MarketId(VenueMarketId(market_hex.to_owned())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price::new(dec!(0.60)).unwrap(),
        contracts: ContractQty(1),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        source_trade_id: SourceTradeId(id.to_owned()),
    }
}

/// Insert an active, non-infra wallet (so it appears in `active_tradeable_wallets`).
fn seed_active_wallet(cache: &mut WalletCache, hex: &str) {
    cache
        .upsert_wallet(hex, SOURCE_BITS, false, None, None, None)
        .unwrap();
    cache.conn_for_test_set_active(hex, 1);
}

/// Scenario: a partially-prepared cache reports one gap in every category.
///
/// PASS: `run_coverage` returns exactly
///   `{ fetch_incomplete: 1, missing_resolution: 1, missing_schedule: 1 }`
///   (a non-clean report → exit category `2`).
/// FAIL: any count differs, or the probe errors.
#[test]
fn scenario_coverage_reports_one_gap_per_category() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wallet_cache.db");

    {
        let mut cache = WalletCache::open(&db_path).unwrap();

        // Two traded markets (A and B); resolve + schedule only A → B is the gap.
        let w1 = wallet(WALLET_1_HEX);
        cache
            .insert_new(
                &w1.to_string(),
                vec![
                    trade(w1, MARKET_A, "0xt1", 1_700_000_000),
                    trade(w1, MARKET_B, "0xt2", 1_700_000_001),
                ],
            )
            .unwrap();
        cache
            .insert_resolution(MARKET_A, Some(0), 1_700_100_000, 1_700_100_001)
            .unwrap();
        cache
            .insert_schedule(MARKET_A, Some(1_700_200_000), 1_700_200_001)
            .unwrap();

        // Two active wallets. W1: fetched. W2: not fetched → the fetch_incomplete gap.
        seed_active_wallet(&mut cache, WALLET_1_HEX);
        seed_active_wallet(&mut cache, WALLET_2_HEX);
        cache
            .update_last_polymarket_fetch(WALLET_1_HEX, 1_700_300_001)
            .unwrap();
    } // drop the read-write handle before the read-only probe opens the file.

    let report = run_coverage(&db_path).expect("coverage probe must not error");

    let expected = CoverageReport {
        fetch_incomplete: 1,
        missing_resolution: 1,
        missing_schedule: 1,
    };
    assert_eq!(
        report, expected,
        "coverage gap counts must match the seeded cache"
    );
    println!("PASS: scenario_coverage_reports_one_gap_per_category — {report:?}");
}

/// Scenario: a fully-prepared cache reports clean (exit category `0`).
///
/// PASS: `run_coverage` returns a report where `is_clean()` is true (all
///   counts zero).
/// FAIL: any count is non-zero, or the probe errors.
#[test]
fn scenario_coverage_clean_when_fully_covered() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("wallet_cache.db");

    {
        let mut cache = WalletCache::open(&db_path).unwrap();

        let w1 = wallet(WALLET_1_HEX);
        cache
            .insert_new(
                &w1.to_string(),
                vec![trade(w1, MARKET_A, "0xt1", 1_700_000_000)],
            )
            .unwrap();
        cache
            .insert_resolution(MARKET_A, Some(0), 1_700_100_000, 1_700_100_001)
            .unwrap();
        cache
            .insert_schedule(MARKET_A, Some(1_700_200_000), 1_700_200_001)
            .unwrap();

        seed_active_wallet(&mut cache, WALLET_1_HEX);
        cache
            .update_last_polymarket_fetch(WALLET_1_HEX, 1_700_300_001)
            .unwrap();
    }

    let report = run_coverage(&db_path).expect("coverage probe must not error");

    assert!(
        report.is_clean(),
        "fully-covered cache must report clean: {report:?}"
    );
    println!("PASS: scenario_coverage_clean_when_fully_covered — {report:?}");
}

#[test]
fn scenario_coverage_partial_prior_stamp_is_incomplete_and_unmigrated_reader_works() {
    for migrated in [true, false] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cache.db");
        let mut cache = WalletCache::open(&path).unwrap();
        seed_active_wallet(&mut cache, WALLET_1_HEX);
        cache
            .update_last_polymarket_fetch(WALLET_1_HEX, 1_700_300_000)
            .unwrap();
        cache
            .insert_new(
                WALLET_1_HEX,
                vec![trade(wallet(WALLET_1_HEX), MARKET_A, "t", 1_700_000_000)],
            )
            .unwrap();
        cache
            .insert_resolution(MARKET_A, Some(0), 1_700_100_000, 1_700_200_000)
            .unwrap();
        cache
            .insert_schedule(MARKET_A, Some(1_700_100_000), 1_700_200_000)
            .unwrap();
        if migrated {
            cache
                .raw_conn_for_test()
                .execute_batch("UPDATE wallets SET backfill_partial = 1")
                .unwrap();
        } else {
            cache
                .raw_conn_for_test()
                .execute_batch("ALTER TABLE wallets DROP COLUMN backfill_partial")
                .unwrap();
        }
        drop(cache);
        let report = run_coverage(&path).unwrap();
        assert_eq!(report.fetch_incomplete, usize::from(migrated));
        assert_eq!(report.is_clean(), !migrated);
    }
}

// ---------------------------------------------------------------------------
// #608: every coverage count must observe ONE committed state.
//
// The probe reads the partial-marker count and the three market sets in separate
// statements. Without a read transaction a backfill committing mid-probe leaves
// them describing different states, and the report can say CLEAN when no single
// state was. A SQL trace commits a real change from a second connection *after*
// the first read, which is the interleaving a static-state test cannot reach.
// `trace` takes a bare fn pointer, hence the statics.
static INTERLEAVE_PATH: std::sync::Mutex<Option<std::path::PathBuf>> = std::sync::Mutex::new(None);
static INTERLEAVE_STATE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn interleave_after_first_read(sql: &str) {
    use std::sync::atomic::Ordering::SeqCst;
    match INTERLEAVE_STATE.load(SeqCst) {
        // Arm once the wallet count is about to run...
        0 if sql.contains("active_tradeable_wallets") => INTERLEAVE_STATE.store(1, SeqCst),
        // ...then fire on the NEXT statement, so the deferred snapshot is already
        // established and this commit must be invisible to the remaining reads.
        1 => {
            INTERLEAVE_STATE.store(2, SeqCst);
            let guard = INTERLEAVE_PATH.lock().unwrap();
            if let Some(path) = guard.as_ref() {
                if let Ok(writer) = rusqlite::Connection::open(path) {
                    let _ = writer.execute_batch("DELETE FROM market_resolutions;");
                }
            }
        }
        _ => {}
    }
}

#[test]
fn coverage_counts_ignore_a_commit_landing_mid_probe() {
    use std::sync::atomic::Ordering::SeqCst;
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");
    {
        let mut cache = WalletCache::open(&path).unwrap();
        seed_active_wallet(&mut cache, WALLET_1_HEX);
        cache
            .update_last_polymarket_fetch(WALLET_1_HEX, 1_700_300_000)
            .unwrap();
        cache
            .insert_new(
                WALLET_1_HEX,
                vec![trade(wallet(WALLET_1_HEX), MARKET_A, "t", 1_700_000_000)],
            )
            .unwrap();
        cache
            .insert_resolution(MARKET_A, Some(0), 1_700_100_000, 1_700_200_000)
            .unwrap();
        cache
            .insert_schedule(MARKET_A, Some(1_700_100_000), 1_700_200_000)
            .unwrap();
    }
    let before = WalletCache::open_read_only(&path)
        .unwrap()
        .coverage_counts()
        .unwrap();
    assert_eq!(before.missing_resolution, 0, "fixture must start clean");

    *INTERLEAVE_PATH.lock().unwrap() = Some(path.clone());
    INTERLEAVE_STATE.store(0, SeqCst);
    let during = {
        let mut cache = WalletCache::open_read_only(&path).unwrap();
        cache
            .raw_conn_mut_for_test()
            .trace(Some(interleave_after_first_read));
        cache.coverage_counts().unwrap()
    };
    assert_eq!(
        INTERLEAVE_STATE.load(SeqCst),
        2,
        "the interleaving never fired; the test would be vacuous"
    );
    assert_eq!(
        during.missing_resolution, 0,
        "a commit landing mid-probe leaked into a later count: the reads did not share one snapshot"
    );

    let after = WalletCache::open_read_only(&path)
        .unwrap()
        .coverage_counts()
        .unwrap();
    assert_eq!(
        after.missing_resolution, 1,
        "the injected delete must really have been durable"
    );
}
