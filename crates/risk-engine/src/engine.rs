use pe_core_types::{BasisPoints, CollateralAmount};

use crate::{
    CANARY_MAX_ALLOWANCE, CANARY_MAX_ORDER_DEBIT, CANARY_PER_TRADE_CAP_BPS, INTRADAY_STOP_BPS,
    KILL_SWITCH_DRAWDOWN_BPS, ROLLING_7D_STOP_BPS,
    block::RiskBlock,
    snapshot::{CanaryRiskSnapshot, RiskSnapshot},
};
use pe_core_types::CanaryOrigin;

/// Result of evaluating a risk snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskDecision {
    Approved,
    Blocked(RiskBlock),
}

/// Evaluate whether a proposed trade passes all risk gates.
///
/// Checks are applied in priority order: kill switches first, drawdown stops,
/// latency, then the per-trade size cap and pure-wallet
/// concentration caps last. (Operator/funder/cluster/anti-gaming gates were
/// removed in the wallet-isolation purge, #326.)
pub fn evaluate_risk(s: &RiskSnapshot) -> RiskDecision {
    // 1. Absolute kill switch (strategy-wide; manual review required to resume)
    if s.absolute_pnl_bps.0 <= KILL_SWITCH_DRAWDOWN_BPS {
        return RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown);
    }

    // 2. Intraday drawdown stop (-200 bps)
    if s.intraday_pnl_bps.0 <= INTRADAY_STOP_BPS {
        return RiskDecision::Blocked(RiskBlock::IntradayDrawdownStop);
    }

    // 3. Rolling 7-day drawdown stop (-600 bps)
    if s.rolling_7d_pnl_bps.0 <= ROLLING_7D_STOP_BPS {
        return RiskDecision::Blocked(RiskBlock::Rolling7dDrawdownStop);
    }

    // 4. Copy latency kill switch state is derived by its service-owned state machine.
    if s.copy_latency_kill_switch_active {
        return RiskDecision::Blocked(RiskBlock::CopyLatencyKillSwitch);
    }

    // 5. Per-trade size cap (defense-in-depth; clamp_contracts_to_cap normally prevents this)
    if s.proposed_trade_bps.0 > s.per_trade_cap_bps {
        return RiskDecision::Blocked(RiskBlock::PerTradeSizeExceeded);
    }

    // 6. Concentration caps (add proposed trade to existing exposure). Enforced only when the
    //    snapshot carries caps: `None` = un-enforced by owner decision (#508 Phase A; the
    //    production copy path). Backtest and tests keep `ConcentrationCaps::CANONICAL`.
    if let Some(caps) = s.concentration_caps {
        let proposed = s.proposed_trade_bps.0;

        if exceeds_concentration_cap(s.leader_exposure_bps.0, proposed, caps.max_leader_bps) {
            return RiskDecision::Blocked(RiskBlock::LeaderConcentrationExceeded);
        }
        if exceeds_concentration_cap(s.market_exposure_bps.0, proposed, caps.max_market_bps) {
            return RiskDecision::Blocked(RiskBlock::MarketConcentrationExceeded);
        }
        if exceeds_concentration_cap(s.family_exposure_bps.0, proposed, caps.max_family_bps) {
            return RiskDecision::Blocked(RiskBlock::FamilyConcentrationExceeded);
        }
        if exceeds_concentration_cap(
            s.total_copy_exposure_bps.0,
            proposed,
            caps.max_total_copy_bps,
        ) {
            return RiskDecision::Blocked(RiskBlock::TotalCopyExposureExceeded);
        }
    }

    RiskDecision::Approved
}

const fn exceeds_concentration_cap(existing: i32, proposed: i32, cap: i32) -> bool {
    match existing.checked_add(proposed) {
        Some(total) => total > cap,
        None => true,
    }
}

/// Convert an exact positive amount to bankroll basis points, rounding outward.
pub fn exposure_bps_ceil(
    amount: CollateralAmount,
    bankroll: CollateralAmount,
) -> Option<BasisPoints> {
    if bankroll == CollateralAmount::ZERO {
        return None;
    }
    let numerator = u128::from(amount.atomic()).checked_mul(10_000)?;
    let denominator = u128::from(bankroll.atomic());
    let bps = numerator.checked_add(denominator.checked_sub(1)?)? / denominator;
    i32::try_from(bps).ok().map(BasisPoints)
}

/// Pure fail-closed gate for the isolated canary path.
pub fn evaluate_canary_risk(s: &CanaryRiskSnapshot) -> RiskDecision {
    let Some(proposed) = exposure_bps_ceil(s.proposed_worst_case_debit, s.canary_bankroll) else {
        return RiskDecision::Blocked(RiskBlock::InvalidCanaryBankroll);
    };

    if !s.resolver_tradable {
        return RiskDecision::Blocked(RiskBlock::ResolverNotTradable);
    }
    if !s.account_state_fresh {
        return RiskDecision::Blocked(RiskBlock::AccountStateStale);
    }
    if !s.venue_reconciliation_fresh {
        return RiskDecision::Blocked(RiskBlock::VenueReconciliationStale);
    }
    if !s.geoblock_fresh || s.geoblocked || !s.jurisdiction_attestation_valid {
        return RiskDecision::Blocked(RiskBlock::JurisdictionBlocked);
    }
    if !s.closed_only_fresh || s.closed_only {
        return RiskDecision::Blocked(RiskBlock::ClosedOnly);
    }
    if s.pending_reservation {
        return RiskDecision::Blocked(RiskBlock::PendingReservation);
    }
    if !s.standard_spender_only
        || s.allowance > CANARY_MAX_ALLOWANCE
        || s.allowance < s.proposed_worst_case_debit
    {
        return RiskDecision::Blocked(RiskBlock::AllowanceExceeded);
    }
    if proposed > CANARY_PER_TRADE_CAP_BPS || s.proposed_worst_case_debit > CANARY_MAX_ORDER_DEBIT {
        return RiskDecision::Blocked(RiskBlock::PerTradeSizeExceeded);
    }

    let leader = match (s.origin, s.leader_exposure_bps) {
        (CanaryOrigin::Organic, Some(value)) => value,
        (CanaryOrigin::OperatorProbe, None) => BasisPoints(0),
        _ => return RiskDecision::Blocked(RiskBlock::OriginInputsInvalid),
    };
    if leader.0 + proposed.0 > 300 {
        return RiskDecision::Blocked(RiskBlock::LeaderConcentrationExceeded);
    }
    if s.market_exposure_bps.0 + proposed.0 > 200 {
        return RiskDecision::Blocked(RiskBlock::MarketConcentrationExceeded);
    }
    if s.family_exposure_bps.0 + proposed.0 > 800 {
        return RiskDecision::Blocked(RiskBlock::FamilyConcentrationExceeded);
    }
    if s.total_copy_exposure_bps.0 + proposed.0 > 2_500
        || s.open_exposure_bps.0 + proposed.0 > 2_500
    {
        return RiskDecision::Blocked(RiskBlock::TotalCopyExposureExceeded);
    }
    RiskDecision::Approved
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use pe_core_types::BasisPoints;

    use super::*;

    pub(super) fn clean_snapshot() -> RiskSnapshot {
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
            concentration_caps: Some(crate::ConcentrationCaps::CANONICAL),
        }
    }

    #[test]
    fn clean_snapshot_is_approved() {
        let s = clean_snapshot();
        assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
    }

    #[test]
    fn concentration_enforced_only_when_caps_are_carried() {
        // #508 Phase A regression: an impact-sized $230 order at a $10,000 bankroll is
        // 230 bps. With the canonical caps it breaches the 200 bps market cap; with
        // `None` (un-enforced by owner decision — the production posture) it is approved.
        let mut s = clean_snapshot();
        s.per_trade_cap_bps = 10_000; // per_trade_cap=unlimited (#508 Phase A)
        s.proposed_trade_bps = BasisPoints(230);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::MarketConcentrationExceeded)
        );
        s.concentration_caps = None;
        assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
        // Drawdown/latency kill switches still fire with non-zero inputs regardless.
        s.absolute_pnl_bps = BasisPoints(-1_000);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown)
        );
    }

    #[test]
    fn leader_concentration_blocks_at_one_over_and_on_overflow() {
        let mut s = clean_snapshot();
        s.proposed_trade_bps = BasisPoints(1);
        s.leader_exposure_bps = BasisPoints(300);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::LeaderConcentrationExceeded)
        );
        s.leader_exposure_bps = BasisPoints(i32::MAX);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::LeaderConcentrationExceeded)
        );
    }

    #[test]
    fn market_concentration_blocks_at_one_over_and_on_overflow() {
        let mut s = clean_snapshot();
        s.proposed_trade_bps = BasisPoints(1);
        s.market_exposure_bps = BasisPoints(200);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::MarketConcentrationExceeded)
        );
        s.market_exposure_bps = BasisPoints(i32::MAX);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::MarketConcentrationExceeded)
        );
    }

    #[test]
    fn family_concentration_blocks_at_one_over_and_on_overflow() {
        let mut s = clean_snapshot();
        s.proposed_trade_bps = BasisPoints(1);
        s.family_exposure_bps = BasisPoints(800);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::FamilyConcentrationExceeded)
        );
        s.family_exposure_bps = BasisPoints(i32::MAX);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::FamilyConcentrationExceeded)
        );
    }

    #[test]
    fn total_concentration_blocks_at_one_over_and_on_overflow() {
        let mut s = clean_snapshot();
        s.proposed_trade_bps = BasisPoints(1);
        s.total_copy_exposure_bps = BasisPoints(2_500);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::TotalCopyExposureExceeded)
        );
        s.total_copy_exposure_bps = BasisPoints(i32::MAX);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::TotalCopyExposureExceeded)
        );
    }

    #[test]
    fn kill_switch_at_minus_1000() {
        let mut s = clean_snapshot();
        s.absolute_pnl_bps = BasisPoints(-1_000);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown)
        );
    }

    #[test]
    fn kill_switch_at_minus_999_is_not_kill_switch() {
        let mut s = clean_snapshot();
        s.absolute_pnl_bps = BasisPoints(-999);
        assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
    }

    #[test]
    fn intraday_drawdown_stop_at_minus_200() {
        let mut s = clean_snapshot();
        s.intraday_pnl_bps = BasisPoints(-200);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::IntradayDrawdownStop)
        );
    }

    #[test]
    fn intraday_drawdown_stop_at_minus_199_is_approved() {
        let mut s = clean_snapshot();
        s.intraday_pnl_bps = BasisPoints(-199);
        assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
    }

    #[test]
    fn rolling_7d_drawdown_stop_at_minus_600() {
        let mut s = clean_snapshot();
        s.rolling_7d_pnl_bps = BasisPoints(-600);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::Rolling7dDrawdownStop)
        );
    }

    #[test]
    fn rolling_7d_drawdown_stop_at_minus_599_is_approved() {
        let mut s = clean_snapshot();
        s.rolling_7d_pnl_bps = BasisPoints(-599);
        assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
    }

    #[test]
    fn active_copy_latency_kill_switch_blocks() {
        let mut s = clean_snapshot();
        s.copy_latency_kill_switch_active = true;
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::CopyLatencyKillSwitch)
        );
    }

    #[test]
    fn inactive_copy_latency_kill_switch_is_approved() {
        let s = clean_snapshot();
        assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
    }
}

#[cfg(test)]
mod proptests {
    use pe_core_types::BasisPoints;
    use proptest::prelude::*;

    use super::{
        tests::clean_snapshot,
        {RiskDecision, evaluate_risk},
    };
    use crate::block::RiskBlock;

    proptest! {
        #[test]
        fn kill_switch_always_fires_at_minus_1000_bps(
            absolute_pnl in -10_000_i32..=-1_000_i32
        ) {
            let mut s = clean_snapshot();
            s.absolute_pnl_bps = BasisPoints(absolute_pnl);
            let decision = evaluate_risk(&s);
            prop_assert_eq!(decision, RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown));
        }
    }
}
