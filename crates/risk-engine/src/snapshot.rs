use std::collections::HashSet;

use pe_core_types::{BasisPoints, FundingHopCount};
use pe_operator_graph::AntiGamingFlag;
use pe_source_core::SourceStatus;
use serde::{Deserialize, Serialize};

/// Whether the trade is in live-tiny or promoted mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingMode {
    /// First live stage: max_trade = 25 bps, kelly = 0.25
    LiveTiny,
    /// After passing promotion gates: max_trade = 100 bps, kelly = 0.25
    Promoted,
}

/// Point-in-time snapshot of all risk inputs for a single proposed trade.
///
/// All exposure values are in basis points of current bankroll.
/// Positive values = open exposure. Negative PnL values = losses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskSnapshot {
    // ── Current exposure (bps of bankroll) ──────────────────────────────────
    /// Existing open exposure to this specific leader.
    pub leader_exposure_bps: BasisPoints,
    /// Existing open exposure to this operator (sum of all leaders in the cluster).
    pub operator_exposure_bps: BasisPoints,
    /// Existing open exposure to this market (sum across all leaders).
    pub market_exposure_bps: BasisPoints,
    /// Existing open exposure to this MarketFamily.
    pub family_exposure_bps: BasisPoints,
    /// Total open copy-trade exposure across all leaders and markets.
    pub total_copy_exposure_bps: BasisPoints,
    /// Existing open exposure from inherited-prior first-trade signals for this funder.
    pub funder_inherited_exposure_bps: BasisPoints,

    // ── PnL (bps of bankroll) — negative = loss ──────────────────────────────
    /// Intraday realized + unrealized PnL. Resets at calendar-day boundary.
    pub intraday_pnl_bps: BasisPoints,
    /// Rolling 7-day realized PnL.
    pub rolling_7d_pnl_bps: BasisPoints,

    // ── Flags and status ────────────────────────────────────────────────────
    /// Anti-gaming flags currently active for this operator.
    pub anti_gaming_flags: HashSet<AntiGamingFlag>,
    /// Health of the on-chain Polygon data source.
    pub onchain_source_status: SourceStatus,
    /// Whether the proxy-funder mapping has been proven for this wallet class.
    pub proxy_funder_mapping_proven: bool,
    /// Whether the funder's seeding rate is suspicious.
    pub funder_seeding_rate_suspicious: bool,
    /// Whether cluster membership is stable (true = stable, false = unstable).
    pub cluster_membership_stable: bool,
    /// Hop count from funder root to the trading wallet (None if unknown).
    pub funding_hop_count: Option<FundingHopCount>,

    // ── Latency ─────────────────────────────────────────────────────────────
    /// Observed p95 copy latency in milliseconds (trailing measurement).
    pub copy_latency_p95_ms: u64,

    // ── Proposed trade ──────────────────────────────────────────────────────
    /// Whether this trade is in live-tiny or promoted mode.
    pub trading_mode: TradingMode,
    /// Size of the proposed trade as basis points of bankroll.
    pub proposed_trade_bps: BasisPoints,
}
