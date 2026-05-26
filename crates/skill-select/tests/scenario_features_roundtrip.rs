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

use pe_skill_select::{DeterministicFeatures, SkillCache, WalletFeatures};
use rust_decimal_macros::dec;
use tempfile::TempDir;

fn features(hex: &str, cutoff: i64, pnl: rust_decimal::Decimal) -> WalletFeatures {
    WalletFeatures {
        features: DeterministicFeatures {
            wallet_hex: hex.to_owned(),
            cutoff_unix: cutoff,
            reconstruction_quality: 88,
            closed_trades: 21,
            distinct_markets: 15,
            distinct_events: 9,
            total_pnl_usd: pnl,
            roi_bps: 420,
            win_rate_bps: 5_500,
            avg_hold_secs: 43_200,
            trading_days: 9,
            mean_daily_return_bps: 300,
            std_daily_return_bps: 700,
            sharpe_bps: 12_000,
            skewness_bps: 0,
            excess_kurtosis_bps: 3_000,
            lcb_5pct_bps: -100,
            ev_mean_bps: 1_100,
            ev_tstat_bps: 7_500,
            bb_shrunk_edge_bps: 600,
            kelly_log_growth_bps: 90,
            brier_score_bps: 3_800,
            brier_resolution_bps: 1_400,
            concentration_hhi_bps: 2_500,
            concentration_n_eff_bps: 40_000,
            concentration_rpc_bps: 19_500,
            first_entries_per_active_day_bps: 16_667,
            median_first_entry_to_resolution_secs: 432_000,
            longshot_bias_ratio_bps: -2_500,
            hold_to_resolution_rate_bps: 6_000,
        },
        extracted_at_unix: 1_700_000_000,
        skill_pnl_usd: pnl,
        skill_pvalue_bps: 320,
        skill_permutations: 999,
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
