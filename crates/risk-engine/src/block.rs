use serde::{Deserialize, Serialize};

/// Reasons a trade can be blocked by the risk engine.
///
/// The pure-wallet block reasons (the operator/funder/cluster/anti-gaming
/// reasons were removed in the wallet-isolation purge, #326), plus
/// `PerTradeSizeExceeded` for per-mode trade size caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskBlock {
    /// Proposed trade + existing leader exposure would exceed 300 bps.
    LeaderConcentrationExceeded,
    /// Proposed trade + existing market exposure would exceed 200 bps.
    MarketConcentrationExceeded,
    /// Proposed trade + existing family exposure would exceed 800 bps.
    FamilyConcentrationExceeded,
    /// Proposed trade + total copy exposure would exceed 2500 bps.
    TotalCopyExposureExceeded,
    /// On-chain source is not healthy (Degraded or Dead).
    OnchainSourceUnhealthy,
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
    /// Resolver evidence is absent, stale, ambiguous, deferred, or mismatched.
    ResolverNotTradable,
    /// Deposit-wallet balances, orders, trades, positions, or allowance are stale.
    AccountStateStale,
    /// Venue reconciliation has not completed successfully within its freshness budget.
    VenueReconciliationStale,
    /// Same-egress jurisdiction evidence is stale, blocked, or disagrees with the authority.
    JurisdictionBlocked,
    /// Authenticated CLOB account is in close-only mode or the observation is stale.
    ClosedOnly,
    /// A nonterminal reservation already owns the one allowed in-flight slot.
    PendingReservation,
    /// Allowance exceeds the campaign cap or names another spender.
    AllowanceExceeded,
    /// Canary bankroll is zero or the exact debit cannot be expressed safely.
    InvalidCanaryBankroll,
    /// Organic requests require leader exposure; probes must not fabricate it.
    OriginInputsInvalid,
    /// Conservative cash-ledger drawdown has reached the canonical intraday stop.
    CanaryDrawdownStop,
}
