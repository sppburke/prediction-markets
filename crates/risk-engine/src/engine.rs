use pe_source_core::SourceStatus;

use pe_core_types::{BasisPoints, CollateralAmount};

use crate::{
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
/// latency, source health, then the per-trade size cap and pure-wallet
/// concentration caps last. (Operator/funder/cluster/anti-gaming gates were
/// removed in the wallet-isolation purge, #326.)
pub fn evaluate_risk(s: &RiskSnapshot) -> RiskDecision {
    // 1. Absolute kill switch (strategy-wide; manual review required to resume)
    if s.intraday_pnl_bps.0 <= -1_000 {
        return RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown);
    }

    // 2. Intraday drawdown stop (-200 bps)
    if s.intraday_pnl_bps.0 <= -200 {
        return RiskDecision::Blocked(RiskBlock::IntradayDrawdownStop);
    }

    // 3. Rolling 7-day drawdown stop (-600 bps)
    if s.rolling_7d_pnl_bps.0 <= -600 {
        return RiskDecision::Blocked(RiskBlock::Rolling7dDrawdownStop);
    }

    // 4. Copy latency kill switch: fire when p95 > 1.5× the 2000 ms budget = 3000 ms
    if s.copy_latency_p95_ms > 3_000 {
        return RiskDecision::Blocked(RiskBlock::CopyLatencyKillSwitch);
    }

    // 5. On-chain source health
    if s.onchain_source_status != SourceStatus::Healthy {
        return RiskDecision::Blocked(RiskBlock::OnchainSourceUnhealthy);
    }

    // 6. Per-trade size cap (defense-in-depth; clamp_contracts_to_cap normally prevents this)
    if s.proposed_trade_bps.0 > s.per_trade_cap_bps {
        return RiskDecision::Blocked(RiskBlock::PerTradeSizeExceeded);
    }

    // 7. Concentration caps (add proposed trade to existing exposure)
    let proposed = s.proposed_trade_bps.0;

    if s.leader_exposure_bps.0 + proposed > 300 {
        return RiskDecision::Blocked(RiskBlock::LeaderConcentrationExceeded);
    }
    if s.market_exposure_bps.0 + proposed > 200 {
        return RiskDecision::Blocked(RiskBlock::MarketConcentrationExceeded);
    }
    if s.family_exposure_bps.0 + proposed > 800 {
        return RiskDecision::Blocked(RiskBlock::FamilyConcentrationExceeded);
    }
    if s.total_copy_exposure_bps.0 + proposed > 2_500 {
        return RiskDecision::Blocked(RiskBlock::TotalCopyExposureExceeded);
    }

    RiskDecision::Approved
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
        || s.allowance.atomic() > 8_000_000
        || s.allowance < s.proposed_worst_case_debit
    {
        return RiskDecision::Blocked(RiskBlock::AllowanceExceeded);
    }
    if proposed.0 > 25 || s.proposed_worst_case_debit.atomic() > 1_000_000 {
        return RiskDecision::Blocked(RiskBlock::PerTradeSizeExceeded);
    }
    if s.drawdown_bps.0 <= -200 {
        return RiskDecision::Blocked(RiskBlock::CanaryDrawdownStop);
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
    use pe_source_core::SourceStatus;

    use super::*;
    use crate::snapshot::TradingMode;

    pub(super) fn clean_snapshot() -> RiskSnapshot {
        RiskSnapshot {
            leader_exposure_bps: BasisPoints(0),
            market_exposure_bps: BasisPoints(0),
            family_exposure_bps: BasisPoints(0),
            total_copy_exposure_bps: BasisPoints(0),
            intraday_pnl_bps: BasisPoints(0),
            rolling_7d_pnl_bps: BasisPoints(0),
            onchain_source_status: SourceStatus::Healthy,
            copy_latency_p95_ms: 100,
            trading_mode: TradingMode::LiveTiny,
            proposed_trade_bps: BasisPoints(10),
            per_trade_cap_bps: 25,
        }
    }

    #[test]
    fn clean_snapshot_is_approved() {
        let s = clean_snapshot();
        assert_eq!(evaluate_risk(&s), RiskDecision::Approved);
    }

    #[test]
    fn kill_switch_at_minus_1000() {
        let mut s = clean_snapshot();
        s.intraday_pnl_bps = BasisPoints(-1_000);
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown)
        );
    }

    #[test]
    fn kill_switch_at_minus_999_is_not_kill_switch() {
        let mut s = clean_snapshot();
        s.intraday_pnl_bps = BasisPoints(-999);
        // Should hit IntradayDrawdownStop instead
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::IntradayDrawdownStop)
        );
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
    fn copy_latency_kill_switch_at_3001_ms() {
        let mut s = clean_snapshot();
        s.copy_latency_p95_ms = 3_001;
        assert_eq!(
            evaluate_risk(&s),
            RiskDecision::Blocked(RiskBlock::CopyLatencyKillSwitch)
        );
    }

    #[test]
    fn copy_latency_at_3000_ms_is_approved() {
        let mut s = clean_snapshot();
        s.copy_latency_p95_ms = 3_000;
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
            intraday_pnl in -10_000_i32..=-1_000_i32
        ) {
            let mut s = clean_snapshot();
            s.intraday_pnl_bps = BasisPoints(intraday_pnl);
            let decision = evaluate_risk(&s);
            prop_assert_eq!(decision, RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown));
        }
    }
}
