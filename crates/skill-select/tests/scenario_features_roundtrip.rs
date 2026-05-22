//! Scenario: `wallet_features` survives a write-pass close and read-only reopen.
//!
//! Single PASS/FAIL criterion written before the body. No network; all data is
//! constructed in-process; no RNG; timestamps are fixed literals. Writes go to a
//! `TempDir` cleaned up at end of test.
//!
//! PASS: rows written via `SkillCache::open` (read-write) and committed are read
//!       back identically through a fresh `SkillCache::open_read_only`, scoped to
//!       the requested cutoff.
//! FAIL: any field differs, the wrong cutoff's rows appear, or either open errors.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_skill_select::{SkillCache, WalletFeatures};
use rust_decimal_macros::dec;
use tempfile::TempDir;

fn features(hex: &str, cutoff: i64, pnl: rust_decimal::Decimal) -> WalletFeatures {
    WalletFeatures {
        wallet_hex: hex.to_owned(),
        cutoff_unix: cutoff,
        extracted_at_unix: 1_700_000_000,
        reconstruction_quality: 88,
        closed_trades: 21,
        distinct_markets: 15,
        distinct_events: 9,
        total_pnl_usd: pnl,
        roi_bps: 420,
        win_rate_bps: 5_500,
        lcb_5pct_bps: -100,
        sharpe_bps: 12_000,
        skewness_bps: 0,
        excess_kurtosis_bps: 3_000,
        compound_return_bps: 1_000,
        buy_hold_return_bps: 250,
        calibration_bps: 175,
        avg_hold_secs: 43_200,
        skill_pnl_usd: pnl,
        skill_pvalue_bps: 320,
        skill_permutations: 999,
        deflated_sharpe_bps: 1_500,
    }
}

#[test]
fn scenario_wallet_features_persist_and_reopen_read_only() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");
    let cutoff = 1_743_465_599; // train/forward split

    let want = vec![
        features(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            cutoff,
            dec!(1000.25),
        ),
        features(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            cutoff,
            dec!(-42.10),
        ),
    ];

    {
        let mut cache = SkillCache::open(&path).unwrap();
        // A row at a different cutoff must not leak into the queried population.
        cache
            .upsert_features_batch(&[features(
                "0xcccccccccccccccccccccccccccccccccccccccc",
                1_000,
                dec!(7.0),
            )])
            .unwrap();
        cache.upsert_features_batch(&want).unwrap();
    } // drop the read-write handle before the read-only reopen.

    let ro = SkillCache::open_read_only(&path).unwrap();
    let got = ro.load_features_for_cutoff(cutoff).unwrap();

    assert_eq!(
        got, want,
        "PASS criterion: read-only reopen returns the committed rows for the cutoff, byte-identical"
    );
    println!(
        "PASS: scenario_wallet_features_persist_and_reopen_read_only — {} rows",
        got.len()
    );
}
