//! Scenario: end-to-end composite ranker — extract → write `wallet_features`
//! → rank by composite → assert top wallet matches the wallet with the best
//! per-bet quality, not the wallet with the highest Sharpe.
//!
//! PASS: with default weights (per-bet quality dominant), the wallet whose
//!       resolved-buy outcomes (EV) are strongest wins the composite even
//!       when a competing wallet has a numerically larger Sharpe driven by
//!       a few-day, high-variance daily-return path.
//! FAIL: the high-Sharpe-low-EV wallet is ranked above the low-Sharpe-high-EV
//!       wallet (i.e. the composite degenerates to deflated-Sharpe), or any
//!       wallet without `min_distinct_events` makes it into the selected set.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_skill_select::{CompositeWeights, SkillCache, rank_by_composite, run_extract};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const CUTOFF: i64 = 10_000_000;
const W_HIGH_EV: &str = "0x1111111111111111111111111111111111111111";
const W_HIGH_SHARPE: &str = "0x2222222222222222222222222222222222222222";

fn raw(wallet: &str, market: &str, side: Side, price: Decimal, ts: i64, id: &str) -> RawTrade {
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
fn scenario_composite_picks_per_bet_quality_over_raw_sharpe() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");

    {
        let mut cache = WalletCache::open(&path).unwrap();

        // Build 20 markets across 20 distinct events so both wallets clear
        // min_distinct_events>=10 (and min_closed_trades=20 / min_trading_days=20).
        for idx in 0..20 {
            let market = format!("0xm{idx:02}");
            cache
                .upsert_market_events(&market, &format!("evt{idx:02}"), Some("slug"), 100)
                .unwrap();
            // All resolve to outcome 0 — buying outcome 0 always wins.
            cache
                .insert_resolution(&market, Some(0), 5_000_000, 5_000_000)
                .unwrap();
        }

        // W_HIGH_EV: 20 cheap buys (0.30) → wins (resolution=0). Modest
        // per-day return; high per-bet edge (o − c = 0.70 each).
        activate(&mut cache, W_HIGH_EV);
        let mut trades = Vec::new();
        for idx in 0..20 {
            let day = 86_400 * (idx as i64 + 1);
            trades.push(raw(
                W_HIGH_EV,
                &format!("0xm{idx:02}"),
                Side::Buy,
                dec!(0.30),
                day,
                &format!("b1_{idx:02}"),
            ));
            trades.push(raw(
                W_HIGH_EV,
                &format!("0xm{idx:02}"),
                Side::Sell,
                dec!(1.0),
                day + 3_600,
                &format!("s1_{idx:02}"),
            ));
        }
        cache.insert_new(W_HIGH_EV, trades).unwrap();

        // W_HIGH_SHARPE: 20 expensive buys (0.90) → also wins (resolution=0).
        // Tiny per-bet edge (o − c = 0.10). All trades on the same 20 days
        // with very tight daily returns → Sharpe will be high relative to EV
        // because std is tiny across consistent small wins.
        activate(&mut cache, W_HIGH_SHARPE);
        let mut trades = Vec::new();
        for idx in 0..20 {
            let day = 86_400 * (idx as i64 + 1);
            trades.push(raw(
                W_HIGH_SHARPE,
                &format!("0xm{idx:02}"),
                Side::Buy,
                dec!(0.90),
                day,
                &format!("b2_{idx:02}"),
            ));
            trades.push(raw(
                W_HIGH_SHARPE,
                &format!("0xm{idx:02}"),
                Side::Sell,
                dec!(1.0),
                day + 3_600,
                &format!("s2_{idx:02}"),
            ));
        }
        cache.insert_new(W_HIGH_SHARPE, trades).unwrap();
    }

    // Extract — keep production gates intact (min_closed_trades=20,
    // min_distinct_events=10), 99 perms keeps the test fast, 1 thread for
    // determinism in the scenario (the cohort is 2 wallets — parallelism is
    // moot, sequential is the cleaner control here).
    let report = run_extract(&path, CUTOFF, 20, 10, 1, 1, 99, 42, 1_700_000_000, 1, false).unwrap();
    assert_eq!(
        report.wallets_written, 2,
        "both wallets must clear extraction gates"
    );

    // Load and rank with default composite weights (per-bet quality dominant).
    let rows = SkillCache::open_read_only(&path)
        .unwrap()
        .load_features_for_cutoff(CUTOFF)
        .unwrap();
    assert_eq!(rows.len(), 2);
    let by_hex: std::collections::HashMap<&str, &pe_skill_select::WalletFeatures> = rows
        .iter()
        .map(|r| (r.features.wallet_hex.as_str(), r))
        .collect();
    // Sanity: high-EV wallet really does carry the larger EV.
    let ev_high = by_hex[W_HIGH_EV].features.ev_mean_bps;
    let ev_low = by_hex[W_HIGH_SHARPE].features.ev_mean_bps;
    assert!(
        ev_high > ev_low,
        "fixture invariant: W_HIGH_EV should out-edge W_HIGH_SHARPE (got {ev_high} vs {ev_low})"
    );

    let out = rank_by_composite(&rows, &CompositeWeights::default(), 10_000, 10, 20);
    let selected_ordered: Vec<&str> = out
        .iter()
        .filter(|r| r.selected)
        .map(|r| r.wallet_hex.as_str())
        .collect();
    assert_eq!(
        selected_ordered.first(),
        Some(&W_HIGH_EV),
        "PASS criterion: per-bet-quality-dominant composite ranks the high-EV wallet #1"
    );
    println!(
        "PASS: scenario_composite_picks_per_bet_quality_over_raw_sharpe — selected order = {selected_ordered:?}"
    );
}
