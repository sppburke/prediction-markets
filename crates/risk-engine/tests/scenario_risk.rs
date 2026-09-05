#![cfg(feature = "scenario")]

use pe_core_types::BasisPoints;
use pe_risk_engine::{
    ConcentrationCaps, RiskBlock, RiskSnapshot,
    engine::{RiskDecision, evaluate_risk},
};

fn base_snapshot() -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        absolute_pnl_bps: BasisPoints(0),
        copy_latency_kill_switch_active: false,
        proposed_trade_bps: BasisPoints(10),
        per_trade_cap_bps: 25,
        concentration_caps: Some(ConcentrationCaps::CANONICAL),
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

/// Scenario 3: Kill switch fires at -1000 bps of absolute PnL.
#[test]
fn scenario_3_kill_switch_at_minus_1000() {
    let mut s = base_snapshot();
    s.absolute_pnl_bps = BasisPoints(-1_000);
    assert_eq!(
        evaluate_risk(&s),
        RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown)
    );
}

/// Scenario 5 (defense-in-depth): Per-trade size cap fires when proposed exceeds snapshot cap.
///
/// Under normal flow `clamp_contracts_to_cap` prevents this. This scenario tests that the
/// gate still fires if the clamp is bypassed (bug detection).
///
/// PASS: `Blocked(PerTradeSizeExceeded)` when `proposed_trade_bps = 999 > per_trade_cap_bps = 25`.
#[test]
fn scenario_5_per_trade_cap_defense_in_depth() {
    let mut s = base_snapshot();
    s.per_trade_cap_bps = 25;
    s.proposed_trade_bps = BasisPoints(999);
    assert_eq!(
        evaluate_risk(&s),
        RiskDecision::Blocked(RiskBlock::PerTradeSizeExceeded)
    );
}

/// Scenario 6: Cap is read from snapshot — cap=100, proposed=99 → Approved.
///
/// PASS: `Approved` when `proposed_trade_bps = 99 ≤ per_trade_cap_bps = 100`.
#[test]
fn scenario_6_snapshot_cap_field_controls_gate() {
    let mut s = base_snapshot();
    s.per_trade_cap_bps = 100;
    s.proposed_trade_bps = BasisPoints(99);
    assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
}

/// Scenario 7: Unlimited cap (10_000 bps) — trade above the 25-bps default is approved.
///
/// 50 bps is above the 25-bps mode default (would have been blocked by the old mode-keyed
/// match), but below all pure-wallet concentration caps (market 200, leader 300, family
/// 800 bps), so the risk gate approves when `per_trade_cap_bps = 10_000`.
///
/// PASS: `Approved` when `proposed_trade_bps = 50 ≤ per_trade_cap_bps = 10_000`.
#[test]
fn scenario_7_unlimited_cap_approves_large_trade() {
    let mut s = base_snapshot();
    s.per_trade_cap_bps = 10_000;
    s.proposed_trade_bps = BasisPoints(50);
    assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
}
