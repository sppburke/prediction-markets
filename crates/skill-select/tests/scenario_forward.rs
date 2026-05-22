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
use pe_skill_select::run_forward_test;
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
