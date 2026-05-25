//! Scenario: the forward test holds a selected wallet's post-cutoff buys to
//! resolution and reports flat-$1 + Kelly PnL, end-to-end over a seeded cache.
//!
//! Single PASS/FAIL criterion. No network; in-process seeding; fixed timestamps;
//! deterministic. Writes to a `TempDir`.
//!
//! PASS: a wallet with a strong ≤cutoff calibration history (high win-rate in
//!       the 0.5 price band) and a post-cutoff winning buy at 0.50 books
//!       flat PnL = +1.0 and a positive Kelly PnL using the ≤cutoff prior;
//!       an unresolved post-cutoff buy is excluded.
//! FAIL: flat PnL ≠ +1.0, Kelly PnL not positive, or the unresolved buy scored.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_skill_select::{
    CompositeWeights, SkillCache, rank_by_composite, run_extract, run_forward_test,
};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const WALLET: &str = "0x1111111111111111111111111111111111111111";
const CUTOFF: i64 = 1_743_465_599;

fn buy(market: &str, outcome: u16, price: rust_decimal::Decimal, ts: i64, id: &str) -> RawTrade {
    RawTrade {
        wallet: WalletAddress::from_hex(WALLET).unwrap(),
        market_id: MarketId(VenueMarketId(market.to_owned())),
        outcome_id: OutcomeId(outcome),
        side: Side::Buy,
        price: Price::new(price).unwrap(),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        source_trade_id: SourceTradeId(id.to_owned()),
    }
}

#[test]
fn scenario_forward_holds_post_cutoff_buys_to_resolution() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");
    let pre = CUTOFF - 100_000; // ≤ cutoff
    let post = CUTOFF + 100_000; // > cutoff

    {
        let mut cache = WalletCache::open(&path).unwrap();
        cache
            .upsert_wallet(WALLET, 1, false, None, None, None)
            .unwrap();

        let mut trades = Vec::new();
        // ≤cutoff calibration: 6 buys at 0.50 (bucket 5), 5 win → p = 5/6 ≈ 0.833.
        for i in 0..6 {
            trades.push(buy(
                &format!("0xcal{i}"),
                0,
                dec!(0.50),
                pre,
                &format!("c{i}"),
            ));
        }
        // post-cutoff: one winning buy at 0.50, one unresolved buy.
        trades.push(buy("0xfwd_win", 0, dec!(0.50), post, "fw"));
        trades.push(buy("0xfwd_none", 0, dec!(0.50), post, "fn"));
        cache.insert_new(WALLET, trades).unwrap();

        // Resolutions: calibration markets (5 win / 1 lose), the forward winner; none for 0xfwd_none.
        for i in 0..6 {
            let winner = if i < 5 { 0 } else { 1 };
            cache
                .insert_resolution(&format!("0xcal{i}"), Some(winner), pre + 1, pre + 1)
                .unwrap();
        }
        cache
            .insert_resolution("0xfwd_win", Some(0), post + 1, post + 1)
            .unwrap();
    } // drop RW handle before the read-only forward pass.

    // f = 0.10, bucket width 0.10, min 5 ≤cutoff buys per bucket.
    let report = run_forward_test(
        &path,
        &[WALLET.to_owned()],
        CUTOFF,
        dec!(0.10),
        dec!(0.10),
        5,
    )
    .unwrap();

    assert_eq!(report.wallets, 1);
    assert_eq!(
        report.resolved_positions, 1,
        "only the resolved forward buy scores"
    );
    assert_eq!(
        report.excluded_positions, 1,
        "the unresolved buy is excluded"
    );
    assert_eq!(report.kelly_fallback_positions, 0, "bucket has 6 ≥ min 5");
    assert!(report.gross_of_fees);
    // flat: winning buy at 0.50 → (1-0.5)/0.5 = +1.0
    assert_eq!(report.flat_pnl_usd, dec!(1.0));
    // kelly: 0.10 * ((5/6 - 0.5)/0.5) * 1.0 > 0
    let expected_kelly = dec!(0.10) * ((dec!(5) / dec!(6) - dec!(0.5)) / dec!(0.5));
    assert_eq!(report.kelly_pnl_usd, expected_kelly);
    assert!(report.kelly_pnl_usd > dec!(0));

    println!(
        "PASS: scenario_forward_holds_post_cutoff_buys_to_resolution — resolved={} excluded={} flat={} kelly={}",
        report.resolved_positions,
        report.excluded_positions,
        report.flat_pnl_usd,
        report.kelly_pnl_usd
    );
}

/// Scenario: `run_forward_test` consumes the composite ranker's selected-hex
/// output cleanly (the data-shape boundary the new `ForwardSource::Composite`
/// path in `main.rs::selected_wallets_for_source` exercises). Mirrors that
/// helper's logic at the library level: extract → rank_by_composite → filter
/// to selected hexes → run_forward_test.
///
/// PASS: a composite-selected eligible wallet drives the forward harness end
///       to end and produces a well-formed `ForwardReport` (correct wallets
///       count, the seeded post-cutoff winner books the expected flat-$1
///       return).
/// FAIL: the hex-list conversion drops the wallet, the report shape is
///       malformed, or the seeded winning post-cutoff buy doesn't book +$1.
#[test]
fn scenario_forward_consumes_composite_ranker_output() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wallet_cache.db");
    let pre = CUTOFF - 100_000;
    let post = CUTOFF + 100_000;

    {
        let mut cache = WalletCache::open(&path).unwrap();
        cache
            .upsert_wallet(WALLET, 1, false, None, None, None)
            .unwrap();
        cache.conn_for_test_set_active(WALLET, 1);

        // Seed enough pre-cutoff trades / events to clear min_closed_trades=1
        // and min_distinct_events=0 (we pass them to run_extract directly).
        // Each pre-cutoff buy is paired with a post-cutoff sell, building 6
        // closed trades on 6 distinct events. One additional post-cutoff buy
        // (0xfwd_win) is the held-to-resolution forward position.
        let mut trades = Vec::new();
        for i in 0..6 {
            trades.push(buy(
                &format!("0xcal{i}"),
                0,
                dec!(0.50),
                pre,
                &format!("c{i}b"),
            ));
            // Closing sells happen ≤ cutoff so they count as closed trades.
            trades.push(RawTrade {
                wallet: WalletAddress::from_hex(WALLET).unwrap(),
                market_id: MarketId(VenueMarketId(format!("0xcal{i}"))),
                outcome_id: OutcomeId(0),
                side: Side::Sell,
                price: Price::new(dec!(0.70)).unwrap(),
                contracts: ContractQty(100),
                timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(pre + 1).unwrap()),
                source_trade_id: SourceTradeId(format!("c{i}s")),
            });
            cache
                .upsert_market_events(&format!("0xcal{i}"), &format!("evt{i}"), Some("slug"), 100)
                .unwrap();
            cache
                .insert_resolution(&format!("0xcal{i}"), Some(0), pre + 2, pre + 2)
                .unwrap();
        }
        // Post-cutoff winning buy + its resolution.
        trades.push(buy("0xfwd_win", 0, dec!(0.50), post, "fw"));
        cache.insert_new(WALLET, trades).unwrap();
        cache
            .insert_resolution("0xfwd_win", Some(0), post + 1, post + 1)
            .unwrap();
    } // drop RW handle before the read-only extract pass.

    // Extract over the seeded cache (1 worker for determinism, clean_prior=false).
    let report = run_extract(&path, CUTOFF, 1, 0, 1, 1, 99, 42, 1_700_000_000, 1, false).unwrap();
    assert_eq!(report.wallets_written, 1, "the seeded wallet must extract");

    // Mirror what `selected_wallets_for_source` does for the Composite source:
    // load features → rank_by_composite → filter to selected hexes.
    let rows = SkillCache::open_read_only(&path)
        .unwrap()
        .load_features_for_cutoff(CUTOFF)
        .unwrap();
    // bhq_q_bps=10000 (q=1.0) so the single seeded wallet passes BHq trivially;
    // min_trading_days=0 because our seed lands on one calendar day.
    let composite_selected: Vec<String> =
        rank_by_composite(&rows, &CompositeWeights::default(), 10_000, 50, 0)
            .iter()
            .filter(|r| r.selected)
            .map(|r| r.wallet_hex.clone())
            .collect();
    assert_eq!(
        composite_selected,
        vec![WALLET.to_owned()],
        "composite must select the one BHq-eligible wallet"
    );

    // Hand the composite-derived hex list to the unchanged forward harness.
    let fwd_report = run_forward_test(
        &path,
        &composite_selected,
        CUTOFF,
        dec!(0.10),
        dec!(0.10),
        5,
    )
    .unwrap();
    assert_eq!(fwd_report.wallets, 1);
    assert_eq!(
        fwd_report.resolved_positions, 1,
        "PASS criterion: the post-cutoff resolved buy scores"
    );
    assert_eq!(fwd_report.flat_pnl_usd, dec!(1.0));
    println!(
        "PASS: scenario_forward_consumes_composite_ranker_output — composite_selected={} resolved={} flat={}",
        composite_selected.len(),
        fwd_report.resolved_positions,
        fwd_report.flat_pnl_usd
    );
}
