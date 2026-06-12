use pe_core_types::BasisPoints;
use pe_source_core::SourceStatus;
use serde::{Deserialize, Serialize};

/// Whether the trade is in live-tiny or promoted mode.
///
/// After the configurable-cap change (`PerTradeCap`), `trading_mode` is no longer used by any
/// gate check in `evaluate_risk` — the cap is carried in `per_trade_cap_bps`. The field is
/// retained in `RiskSnapshot` for logging and tracing only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingMode {
    /// First live stage; default cap = 25 bps when `PerTradeCap::ModeDefault`.
    LiveTiny,
    /// After passing promotion gates; default cap = 100 bps when `PerTradeCap::ModeDefault`.
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
    /// Existing open exposure to this market (sum across all leaders).
    pub market_exposure_bps: BasisPoints,
    /// Existing open exposure to this MarketFamily.
    pub family_exposure_bps: BasisPoints,
    /// Total open copy-trade exposure across all leaders and markets.
    pub total_copy_exposure_bps: BasisPoints,

    // ── PnL (bps of bankroll) — negative = loss ──────────────────────────────
    /// Intraday realized + unrealized PnL. Resets at calendar-day boundary.
    pub intraday_pnl_bps: BasisPoints,
    /// Rolling 7-day realized PnL.
    pub rolling_7d_pnl_bps: BasisPoints,

    // ── Flags and status ────────────────────────────────────────────────────
    /// Health of the on-chain Polygon data source.
    pub onchain_source_status: SourceStatus,

    // ── Latency ─────────────────────────────────────────────────────────────
    /// Observed p95 copy latency in milliseconds (trailing measurement).
    pub copy_latency_p95_ms: u64,

    // ── Proposed trade ──────────────────────────────────────────────────────
    /// Whether this trade is in live-tiny or promoted mode. Retained for logging only;
    /// not read by any gate check in `evaluate_risk` after the configurable-cap change.
    pub trading_mode: TradingMode,
    /// Size of the proposed trade as basis points of bankroll.
    /// Populated by `WinnerFollowStrategy::evaluate` after clamping to `per_trade_cap_bps`.
    pub proposed_trade_bps: BasisPoints,
    /// Per-trade size cap in basis points of bankroll.
    ///
    /// Populated by `WinnerFollowStrategy::evaluate` from the resolved `PerTradeCap` config.
    /// The risk gate fires `PerTradeSizeExceeded` if `proposed_trade_bps > per_trade_cap_bps`.
    /// Under normal flow this never triggers because the strategy already clamped the contracts.
    ///
    /// Default 25 for serde backwards compatibility with snapshots written before this field existed.
    #[serde(default = "default_per_trade_cap_bps")]
    pub per_trade_cap_bps: i32,
}

fn default_per_trade_cap_bps() -> i32 {
    25
}
