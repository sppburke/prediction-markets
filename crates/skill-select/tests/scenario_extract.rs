//! Scenario: Phase-A extraction over a seeded cache writes one `wallet_features`
//! row per eligible active-tradeable wallet, end-to-end.
//!
//! Single PASS/FAIL criterion. No network; in-process seeding; fixed timestamps;
//! deterministic skill-test seed. Writes to a `TempDir`.
//!
//! PASS: after `run_extract`, the active wallet with ≥1 reconstructable closed
//!       trade has a `wallet_features` row (carrying its features + skill p-value
//!       + every PR-1 candidate-features column, populated from a seeded
//!       resolution); the active wallet with no closed trade and the inactive
//!       wallet do not.
//! FAIL: the eligible wallet is missing, an ineligible/inactive wallet appears,
//!       any PR-1 column is silently unpopulated, or the report counts disagree.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_skill_select::{SkillCache, run_extract};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const W_ELIGIBLE: &str = "0x1111111111111111111111111111111111111111";
const W_NO_CLOSE: &str = "0x2222222222222222222222222222222222222222";
const W_INACTIVE: &str = "0x3333333333333333333333333333333333333333";
const CUTOFF: i64 = 1_743_465_599;

fn raw(
    wallet: &str,
    market: &str,
    side: Side,
    price: rust_decimal::Decimal,
    ts: i64,
    id: &str,
) -> RawTrade {
    RawTrade {
        wallet: WalletAddress::from_hex(wallet).unwrap(),
        market_id: MarketId(VenueMarketId(market.to_owned())),
        outcome_id: OutcomeId(0),
        side,
        price: Price::new(price).unwrap(),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        source_trade_id: SourceTradeId(id.to_owned()),
    }
}

fn activate(cache: &mut WalletCache, hex: &str) {
    cache
        .upsert_wallet(hex, 1, false, None, None, None)
        .unwrap();
    cache.conn_for_test_set_active(hex, 1);
}

#[test]
fn scenario_extract_writes_rows_for_eligible_active_wallets() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");

    {
        let mut cache = WalletCache::open(&path).unwrap();
        cache
            .upsert_market_events("0xm1", "evtA", Some("slug"), 100)
            .unwrap();
        // Seed a resolution for 0xm1 so the eligible wallet's per-bet quality
        // (EV / Brier / Kelly) actually populates rather than defaulting to 0.
        // The bought outcome is 0 (see raw()); winning_outcome_id=0 → o=1, win.
        cache
            .insert_resolution(
                "0xm1",
                Some(0), // winning outcome id
                3_000,   // resolved after the sell at 2000
                3_000,   // fetched_at
            )
            .unwrap();

        // Eligible: active, with a matched buy→sell → one closed trade (≤ cutoff).
        activate(&mut cache, W_ELIGIBLE);
        cache
            .insert_new(
                W_ELIGIBLE,
                vec![
                    raw(W_ELIGIBLE, "0xm1", Side::Buy, dec!(0.50), 1_000, "0xb1"),
                    raw(W_ELIGIBLE, "0xm1", Side::Sell, dec!(0.70), 2_000, "0xs1"),
                ],
            )
            .unwrap();

        // Active but only an open buy → no closed trade → skipped.
        activate(&mut cache, W_NO_CLOSE);
        cache
            .insert_new(
                W_NO_CLOSE,
                vec![raw(
                    W_NO_CLOSE,
                    "0xm1",
                    Side::Buy,
                    dec!(0.40),
                    1_500,
                    "0xb2",
                )],
            )
            .unwrap();

        // Inactive (is_active stays 0): not in the active_tradeable view.
        cache
            .upsert_wallet(W_INACTIVE, 1, false, None, None, None)
            .unwrap();
        cache
            .insert_new(
                W_INACTIVE,
                vec![
                    raw(W_INACTIVE, "0xm1", Side::Buy, dec!(0.50), 1_000, "0xb3"),
                    raw(W_INACTIVE, "0xm1", Side::Sell, dec!(0.90), 2_000, "0xs3"),
                ],
            )
            .unwrap();
    } // drop the read-write cache before the read-only extract pass.

    // min_distinct_events=0 keeps the existing tiny-fixture scenario in scope;
    // the production default 10 is exercised by the unit tests in features.rs.
    let report = run_extract(&path, CUTOFF, 1, 0, 1, 1, 99, 42, 1_700_000_000).unwrap();

    let rows = SkillCache::open_read_only(&path)
        .unwrap()
        .load_features_for_cutoff(CUTOFF)
        .unwrap();
    let hexes: Vec<&str> = rows
        .iter()
        .map(|r| r.features.wallet_hex.as_str())
        .collect();

    // Inactive wallet is not even scanned; the two active wallets are.
    assert_eq!(
        report.wallets_scanned, 2,
        "only active_tradeable wallets scanned"
    );
    assert_eq!(report.wallets_written, 1);
    assert_eq!(report.wallets_skipped, 1);
    assert_eq!(
        hexes,
        vec![W_ELIGIBLE],
        "PASS: exactly the eligible active wallet gets a wallet_features row"
    );

    // The written row carries real features (one winning closed trade).
    let r = &rows[0];
    assert_eq!(r.features.closed_trades, 1);
    assert_eq!(r.features.win_rate_bps, 10_000); // the single closed trade won
    assert_eq!(r.skill_permutations, 99);
    // PR-1 candidate features populate from the resolved trade:
    //   bought outcome 0 = winner → o=1, c=0.50 → EV = 0.50 → 5000 bps.
    //   Brier = (0.50 − 1)² = 0.25 → 2500 bps.
    //   Single market with positive PnL → HHI = 1.0 → 10000 bps, N_eff = 1 → 10000 bps,
    //   RPC = 1·1.0 = 1.0 → 10000 bps.
    //   1 distinct market / 1 trading day → 1.0 → 10000 bps.
    //   Resolution at 3000 minus first-entry at 1000 → median delta = 2000 secs.
    assert_eq!(r.features.ev_mean_bps, 5_000);
    assert_eq!(r.features.brier_score_bps, 2_500);
    assert_eq!(r.features.concentration_hhi_bps, 10_000);
    assert_eq!(r.features.concentration_n_eff_bps, 10_000);
    assert_eq!(r.features.concentration_rpc_bps, 10_000);
    assert_eq!(r.features.first_entries_per_active_day_bps, 10_000);
    assert_eq!(r.features.median_first_entry_to_resolution_secs, 2_000);
    println!(
        "PASS: scenario_extract_writes_rows_for_eligible_active_wallets — scanned={} written={} skipped={}",
        report.wallets_scanned, report.wallets_written, report.wallets_skipped
    );
}
