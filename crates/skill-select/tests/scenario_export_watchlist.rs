//! Scenario tests for `export-watchlist` subcommand.
//!
//! PASS criteria written before each test body. No network; all data
//! constructed in-process; timestamps are fixed literals. DB and output files
//! go to `TempDir` cleaned up on drop.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;

use pe_skill_select::{DeterministicFeatures, ExportWatchlistConfig, SkillCache, WalletFeatures};
use pe_trader_index::{Watchlist, WatchlistTier};
use rust_decimal_macros::dec;
use tempfile::TempDir;

const CUTOFF: i64 = 1_775_001_599; // 2026-03-31T23:59:59Z

fn make_wf(hex: &str, win_rate_bps: i32, lcb_5pct_bps: i32, closed_trades: u32) -> WalletFeatures {
    WalletFeatures {
        features: DeterministicFeatures {
            wallet_hex: hex.to_owned(),
            cutoff_unix: CUTOFF,
            reconstruction_quality: 80,
            closed_trades,
            distinct_markets: 10,
            distinct_events: 10,
            total_pnl_usd: dec!(100.0),
            roi_bps: 500,
            win_rate_bps,
            avg_hold_secs: 86_400,
            trading_days: 30,
            mean_daily_return_bps: 100,
            std_daily_return_bps: 300,
            sharpe_bps: 5_000,
            skewness_bps: 0,
            excess_kurtosis_bps: 0,
            lcb_5pct_bps,
            ev_mean_bps: 200,
            ev_tstat_bps: 2_000,
            bb_shrunk_edge_bps: 150,
            kelly_log_growth_bps: 50,
            brier_score_bps: 4_500,
            brier_resolution_bps: 1_000,
            concentration_hhi_bps: 2_000,
            concentration_n_eff_bps: 30_000,
            concentration_rpc_bps: 15_000,
            first_entries_per_active_day_bps: 10_000,
            median_first_entry_to_resolution_secs: 259_200,
            longshot_bias_ratio_bps: 0,
            hold_to_resolution_rate_bps: 7_000,
            position_sizing_cv_bps: 3_000,
            first_mover_percentile_bps: 5_000,
        },
        extracted_at_unix: 1_700_000_000,
        skill_pnl_usd: dec!(100.0),
        skill_pvalue_bps: 250,
        skill_permutations: 999,
    }
}

fn seed_cache(dir: &TempDir, wallets: &[WalletFeatures]) -> std::path::PathBuf {
    let path = dir.path().join("wallet_cache.db");
    let mut cache = SkillCache::open(&path).unwrap();
    cache.upsert_features_batch(wallets).unwrap();
    path
}

fn write_txt(dir: &TempDir, lines: &[&str]) -> std::path::PathBuf {
    let path = dir.path().join("input.txt");
    fs::write(&path, lines.join("\n")).unwrap();
    path
}

/// PASS: 2 wallets seeded, both in .txt → wallets_written == 2, valid JSON
///       round-trips back to a `Watchlist` with matching entry count and field values.
/// FAIL: wallets_written != 2, serde round-trip fails, or tier is wrong.
#[test]
fn scenario_export_watchlist_writes_json_round_trip() {
    let dir = TempDir::new().unwrap();
    let hex_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let hex_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    let wf_a = make_wf(hex_a, 6_000, 1_200, 50);
    let wf_b = make_wf(hex_b, 5_500, 900, 40);
    let cache_path = seed_cache(&dir, &[wf_a.clone(), wf_b.clone()]);

    let txt_path = write_txt(&dir, &[hex_a, hex_b]);
    let out_path = dir.path().join("watchlist.json");

    let cfg = ExportWatchlistConfig {
        cache_path,
        watchlist_txt_path: txt_path,
        cutoff_unix: CUTOFF,
        output_path: out_path.clone(),
    };

    let stats = pe_skill_select::run_export_watchlist(&cfg).unwrap();
    assert_eq!(stats.wallets_written, 2, "PASS: wallets_written == 2");
    assert_eq!(stats.wallets_missing, 0);
    assert_eq!(stats.active_count, 2);
    assert_eq!(stats.incubator_count, 0);

    let json = fs::read_to_string(&out_path).unwrap();
    let parsed: Watchlist = serde_json::from_str(&json).expect("valid Watchlist JSON");
    assert_eq!(parsed.entries.len(), 2);
    assert_eq!(parsed.active_count, 2);
    assert_eq!(parsed.incubator_count, 0);
    assert_eq!(parsed.entries[0].tier, WatchlistTier::Active);
    // Entries sorted descending by leader_score_bps: A (1200) before B (900).
    assert_eq!(parsed.entries[0].win_rate_bps.0, wf_a.features.win_rate_bps);
    assert_eq!(parsed.entries[1].win_rate_bps.0, wf_b.features.win_rate_bps);

    println!("PASS: scenario_export_watchlist_writes_json_round_trip — 2 entries, round-trip OK");
}

/// PASS: 1 wallet seeded, .txt has 2 hexes (one present, one absent) →
///       wallets_written == 1, wallets_missing == 1, no error.
/// FAIL: returns Err, or counts differ.
#[test]
fn scenario_export_watchlist_skips_and_counts_missing_wallets() {
    let dir = TempDir::new().unwrap();
    let hex_present = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let hex_absent = "0xcccccccccccccccccccccccccccccccccccccccc";

    let cache_path = seed_cache(&dir, &[make_wf(hex_present, 5_000, 500, 25)]);
    let txt_path = write_txt(&dir, &[hex_present, hex_absent]);
    let out_path = dir.path().join("watchlist.json");

    let cfg = ExportWatchlistConfig {
        cache_path,
        watchlist_txt_path: txt_path,
        cutoff_unix: CUTOFF,
        output_path: out_path,
    };

    let stats = pe_skill_select::run_export_watchlist(&cfg).unwrap();
    assert_eq!(stats.wallets_written, 1);
    assert_eq!(stats.wallets_missing, 1);
    assert_eq!(stats.wallets_requested, 2);

    println!(
        "PASS: scenario_export_watchlist_skips_and_counts_missing_wallets — 1 written, 1 missing"
    );
}

/// PASS: empty .txt → no error, entries == [], active_count == 0.
/// FAIL: returns Err, or output contains entries.
#[test]
fn scenario_export_watchlist_handles_empty_input() {
    let dir = TempDir::new().unwrap();
    let cache_path = seed_cache(
        &dir,
        &[make_wf(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            5_000,
            500,
            25,
        )],
    );
    let txt_path = write_txt(&dir, &[]);
    let out_path = dir.path().join("watchlist.json");

    let cfg = ExportWatchlistConfig {
        cache_path,
        watchlist_txt_path: txt_path,
        cutoff_unix: CUTOFF,
        output_path: out_path.clone(),
    };

    let stats = pe_skill_select::run_export_watchlist(&cfg).unwrap();
    assert_eq!(stats.wallets_written, 0);
    assert_eq!(stats.active_count, 0);

    let json = fs::read_to_string(&out_path).unwrap();
    let parsed: Watchlist = serde_json::from_str(&json).unwrap();
    assert!(parsed.entries.is_empty());

    println!("PASS: scenario_export_watchlist_handles_empty_input — 0 entries, no error");
}

/// PASS: .txt contains `not-a-hex-address` → Err(SkillSelectError::Decode(_)).
/// FAIL: returns Ok or a different error variant.
#[test]
fn scenario_export_watchlist_errs_on_malformed_hex() {
    use pe_skill_select::SkillSelectError;

    let dir = TempDir::new().unwrap();
    let cache_path = seed_cache(&dir, &[]);
    let txt_path = write_txt(&dir, &["not-a-hex-address"]);
    let out_path = dir.path().join("watchlist.json");

    let cfg = ExportWatchlistConfig {
        cache_path,
        watchlist_txt_path: txt_path,
        cutoff_unix: CUTOFF,
        output_path: out_path,
    };

    let err = pe_skill_select::run_export_watchlist(&cfg).unwrap_err();
    assert!(
        matches!(err, SkillSelectError::Decode(_)),
        "PASS: malformed hex returns Decode variant; got {err:?}"
    );
    println!("PASS: scenario_export_watchlist_errs_on_malformed_hex — Decode returned");
}

/// PASS: .txt has 3 '#'-comment lines + 2 blank lines + 1 valid hex →
///       wallets_requested == 1, no parse error on comment lines.
/// FAIL: returns Err, or wallets_requested != 1.
#[test]
fn scenario_export_watchlist_skips_comment_header_lines() {
    let dir = TempDir::new().unwrap();
    let hex = "0xdddddddddddddddddddddddddddddddddddddddd";

    let cache_path = seed_cache(&dir, &[make_wf(hex, 5_000, 400, 22)]);
    let txt_path = write_txt(
        &dir,
        &[
            "# generated by monthly_rerank_gbm.py",
            "# strategy: gbm_throughput_single",
            "# cutoff: 2026-03-31",
            "",
            "",
            hex,
        ],
    );
    let out_path = dir.path().join("watchlist.json");

    let cfg = ExportWatchlistConfig {
        cache_path,
        watchlist_txt_path: txt_path,
        cutoff_unix: CUTOFF,
        output_path: out_path,
    };

    let stats = pe_skill_select::run_export_watchlist(&cfg).unwrap();
    assert_eq!(
        stats.wallets_requested, 1,
        "comment and blank lines must be skipped"
    );
    assert_eq!(stats.wallets_written, 1);

    println!(
        "PASS: scenario_export_watchlist_skips_comment_header_lines — 1 wallet, comments ignored"
    );
}
