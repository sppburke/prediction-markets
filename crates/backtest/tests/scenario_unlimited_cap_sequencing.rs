//! Scenario: unlimited-cap sequential bankroll accounting.
//!
//! With `PerTradeCap::Unlimited` and a $10k bankroll, run N signals serially.
//! After each fill the bankroll is debited. The sum of all fill costs must be ≤ initial bankroll
//! and each individual fill must be ≤ remaining bankroll at the time of the trade.
//!
//! PASS: sum(fill_cost_i) ≤ bankroll_initial AND each fill_cost_i ≤ bankroll_before_fill_i.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, ContractQty, LeaderAction, MarketId, OutcomeId, Price, Probability,
    ProbabilityPpm, Quantity, ReconstructionQuality, Side, SourceTradeId, TraderId, VenueId,
    VenueMarketId, WalletAddress,
};
use pe_risk_engine::{ConcentrationCaps, RiskSnapshot, snapshot::TradingMode};
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::{
    ExecutionMode, PerTradeCap, WinnerFollowConfig, WinnerFollowStrategy,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use time::macros::datetime;

const NOW: time::OffsetDateTime = datetime!(2024-07-01 00:00:00 UTC);

fn wallet(b: u8) -> WalletAddress {
    let mut bytes = [0u8; 20];
    bytes[19] = b;
    WalletAddress(bytes)
}

fn price(d: Decimal) -> Price {
    Price::new(d).expect("valid price")
}

fn quality(q: u8) -> ReconstructionQuality {
    ReconstructionQuality::new(q).expect("quality in 0..=100")
}

fn make_signal(market_idx: u8) -> LeaderSignal {
    let market_id = format!("mkt-{market_idx:03}");
    LeaderSignal {
        leader: TraderId(wallet(0x10)),
        venue: VenueId::polymarket(),
        market_id: MarketId(VenueMarketId(market_id.clone())),
        outcome_id: OutcomeId(0),
        action: LeaderAction::Entry,
        leader_side: Side::Buy,
        leader_price: price(dec!(0.40)),
        leader_size: Quantity(ContractQty(100)),
        observed_at: NOW,
        received_at: NOW,
        reconstruction_quality: quality(100),
        source_trade_id: SourceTradeId(format!("tid-seq-{market_idx}")),
        action_confidence_ppm: ProbabilityPpm(1_000_000),
    }
}

fn clean_snapshot() -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        onchain_source_status: SourceStatus::Healthy,
        copy_latency_p95_ms: 500,
        trading_mode: TradingMode::LiveTiny,
        proposed_trade_bps: BasisPoints(0),
        per_trade_cap_bps: 0, // overridden by evaluate()
        concentration_caps: Some(ConcentrationCaps::CANONICAL),
    }
}

/// PASS: with Unlimited cap and p=0.44, each fill cost ≤ remaining bankroll,
///       and sum(fill_costs) ≤ initial bankroll = $10_000.
///
/// p=0.44 yields ~98 bps per trade (Kelly stake ≈ $99 on $10k at 0.25× Kelly),
/// staying under all concentration caps (market=200, operator/leader=300 bps) so
/// the risk gate passes and the test focuses on bankroll accounting, not exposure limits.
#[test]
fn unlimited_cap_sequential_fills_stay_within_bankroll() {
    let strategy = WinnerFollowStrategy::new(WinnerFollowConfig {
        per_trade_cap: PerTradeCap::Unlimited,
        ..WinnerFollowConfig::default()
    });

    let p = Probability::new(dec!(0.44)).expect("valid");
    let bankroll_initial = dec!(10_000);
    let mut bankroll = bankroll_initial;
    let mut total_cost = Decimal::ZERO;

    for i in 0u8..5 {
        if bankroll < dec!(0.40) {
            // Not enough to buy even one contract — stop.
            break;
        }
        let signal = make_signal(i);
        let intent = strategy
            .evaluate(
                &signal,
                p,
                clean_snapshot(),
                bankroll,
                ExecutionMode::LiveTiny,
            )
            .expect("unlimited cap with p=0.44 must produce an order");

        let fill_cost = Decimal::from(intent.contracts.0) * intent.limit_price.0;
        assert!(
            fill_cost <= bankroll,
            "fill cost {fill_cost} exceeds remaining bankroll {bankroll} on signal {i}"
        );

        bankroll -= fill_cost;
        total_cost += fill_cost;
    }

    assert!(
        total_cost <= bankroll_initial,
        "total fill costs {total_cost} exceed initial bankroll {bankroll_initial}"
    );
}
