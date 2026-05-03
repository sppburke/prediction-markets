use serde::{Deserialize, Serialize};

/// Reasons a trade can be blocked by the risk engine.
///
/// The 16 canonical block reasons from `docs/19-WINNER-FOLLOW-STRATEGY.md`,
/// plus `PerTradeSizeExceeded` for per-mode trade size caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskBlock {
    /// Proposed trade + existing operator exposure would exceed 300 bps.
    OperatorConcentrationExceeded,
    /// Proposed trade + existing leader exposure would exceed 300 bps.
    LeaderConcentrationExceeded,
    /// Proposed trade + existing market exposure would exceed 200 bps.
    MarketConcentrationExceeded,
    /// Proposed trade + existing family exposure would exceed 800 bps.
    FamilyConcentrationExceeded,
    /// Proposed trade + total copy exposure would exceed 2500 bps.
    TotalCopyExposureExceeded,
    /// Proposed trade + funder inherited exposure would exceed 100 bps.
    FunderInheritedExposureExceeded,
    /// Funder seeding rate is suspicious.
    FunderSeedingRateSuspicious,
    /// Cluster membership is unstable (not stable).
    ClusterMembershipUnstable,
    /// Funding hop count exceeds the maximum of 3.
    FunderHopCountExcessive,
    /// On-chain source is not healthy (Degraded or Dead).
    OnchainSourceUnhealthy,
    /// Proxy-funder mapping has not been proven for this wallet class.
    ProxyFunderMappingUnproven,
    /// One or more anti-gaming flags are active.
    AntiGamingFlagActive,
    /// Intraday PnL has hit or breached the -200 bps drawdown stop.
    IntradayDrawdownStop,
    /// Rolling 7-day PnL has hit or breached the -600 bps drawdown stop.
    Rolling7dDrawdownStop,
    /// Intraday PnL has hit or breached the -1000 bps absolute kill switch.
    KillSwitchDrawdown,
    /// Copy latency p95 has exceeded the kill switch threshold (>3000 ms).
    CopyLatencyKillSwitch,
    /// Proposed trade size exceeds the per-mode cap (live-tiny: 25 bps, promoted: 100 bps).
    PerTradeSizeExceeded,
}
