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
///   `{ pending_funder: 1, fetch_incomplete: 1, missing_resolution: 1, missing_schedule: 1 }`
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

        // Two active wallets. W1: funder-done + fetched. W2: neither → the gap
        // in both pending_funder and fetch_incomplete.
        seed_active_wallet(&mut cache, WALLET_1_HEX);
        seed_active_wallet(&mut cache, WALLET_2_HEX);
        cache.insert_funder_edges(w1, &[], 1_700_300_000).unwrap();
        cache
            .update_last_polymarket_fetch(WALLET_1_HEX, 1_700_300_001)
            .unwrap();
    } // drop the read-write handle before the read-only probe opens the file.

    let report = run_coverage(&db_path).expect("coverage probe must not error");

    let expected = CoverageReport {
        pending_funder: 1,
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
/// PASS: `run_coverage` returns a report where `is_clean()` is true (all four
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
        cache.insert_funder_edges(w1, &[], 1_700_300_000).unwrap();
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
