#![cfg(feature = "scenario")]

use std::collections::HashSet;

use pe_core_types::{BasisPoints, FundingHopCount};
use pe_operator_graph::AntiGamingFlag;
use pe_risk_engine::{
    RiskBlock, RiskSnapshot,
    engine::{RiskDecision, evaluate_risk},
    snapshot::TradingMode,
};
use pe_source_core::SourceStatus;

fn base_snapshot() -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(0),
        operator_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        funder_inherited_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        anti_gaming_flags: HashSet::new(),
        onchain_source_status: SourceStatus::Healthy,
        proxy_funder_mapping_proven: true,
        funder_seeding_rate_suspicious: false,
        cluster_membership_stable: true,
        funding_hop_count: Some(FundingHopCount(1)),
        copy_latency_p95_ms: 100,
        trading_mode: TradingMode::LiveTiny,
        proposed_trade_bps: BasisPoints(10),
    }
}

/// Scenario 1: All checks pass → Approved
#[test]
fn scenario_1_all_checks_pass() {
    let s = base_snapshot();
    assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
}

/// Scenario 2: Intraday halt fires at -200 bps (not kill switch)
#[test]
fn scenario_2_intraday_halt_at_minus_200() {
    let mut s = base_snapshot();
    s.intraday_pnl_bps = BasisPoints(-200);
    assert_eq!(
        evaluate_risk(&s),
        RiskDecision::Blocked(RiskBlock::IntradayDrawdownStop)
    );
}

/// Scenario 3: Kill switch fires at -1000 bps
#[test]
fn scenario_3_kill_switch_at_minus_1000() {
    let mut s = base_snapshot();
    s.intraday_pnl_bps = BasisPoints(-1_000);
    assert_eq!(
        evaluate_risk(&s),
        RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown)
    );
}

/// Scenario 4: Concentration cap fires (operator at 280, proposed 25 → 305 > 300)
#[test]
fn scenario_4_operator_concentration_exceeded() {
    let mut s = base_snapshot();
    s.operator_exposure_bps = BasisPoints(280);
    s.proposed_trade_bps = BasisPoints(25);
    assert_eq!(
        evaluate_risk(&s),
        RiskDecision::Blocked(RiskBlock::OperatorConcentrationExceeded)
    );
}

/// Scenario 5: Per-trade size cap fires in live-tiny (proposed 26 bps > 25)
#[test]
fn scenario_5_per_trade_size_cap_live_tiny() {
    let mut s = base_snapshot();
    s.trading_mode = TradingMode::LiveTiny;
    s.proposed_trade_bps = BasisPoints(26);
    assert_eq!(
        evaluate_risk(&s),
        RiskDecision::Blocked(RiskBlock::PerTradeSizeExceeded)
    );
}

// Suppress unused import warning for AntiGamingFlag (available for future scenarios)
const _: fn() = || {
    let _ = AntiGamingFlag::BaitWalletSuspect;
};
