use pe_source_core::SourceStatus;

use crate::{block::RiskBlock, snapshot::RiskSnapshot};

/// Result of evaluating a risk snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskDecision {
    Approved,
    Blocked(RiskBlock),
}

/// Evaluate whether a proposed trade passes all risk gates.
///
/// Checks are applied in priority order: kill switches first, drawdown stops,
/// latency, source health, identity flags, concentration caps last.
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

    // 6. Proxy-funder mapping
    if !s.proxy_funder_mapping_proven {
        return RiskDecision::Blocked(RiskBlock::ProxyFunderMappingUnproven);
    }

    // 7. Anti-gaming flags
    if !s.anti_gaming_flags.is_empty() {
        return RiskDecision::Blocked(RiskBlock::AntiGamingFlagActive);
    }

    // 8. Funder seeding rate
    if s.funder_seeding_rate_suspicious {
        return RiskDecision::Blocked(RiskBlock::FunderSeedingRateSuspicious);
    }

    // 9. Cluster membership stability
    if !s.cluster_membership_stable {
        return RiskDecision::Blocked(RiskBlock::ClusterMembershipUnstable);
    }

    // 10. Funding hop count (max 3, from _GLOSSARY.md)
    if let Some(hops) = s.funding_hop_count
        && hops.0 > 3
    {
        return RiskDecision::Blocked(RiskBlock::FunderHopCountExcessive);
    }

    // 11. Per-trade size cap (defense-in-depth; clamp_contracts_to_cap normally prevents this)
    if s.proposed_trade_bps.0 > s.per_trade_cap_bps {
        return RiskDecision::Blocked(RiskBlock::PerTradeSizeExceeded);
    }

    // 12. Concentration caps (add proposed trade to existing exposure)
    let proposed = s.proposed_trade_bps.0;

    if s.operator_exposure_bps.0 + proposed > 300 {
        return RiskDecision::Blocked(RiskBlock::OperatorConcentrationExceeded);
    }
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
    if s.funder_inherited_exposure_bps.0 + proposed > 100 {
        return RiskDecision::Blocked(RiskBlock::FunderInheritedExposureExceeded);
    }

    RiskDecision::Approved
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashSet;

    use pe_core_types::{BasisPoints, FundingHopCount};
    use pe_source_core::SourceStatus;

    use super::*;
    use crate::snapshot::TradingMode;

    pub(super) fn clean_snapshot() -> RiskSnapshot {
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
