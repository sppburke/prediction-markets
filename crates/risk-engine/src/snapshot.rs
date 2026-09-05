use pe_core_types::{BasisPoints, CanaryOrigin, CollateralAmount};
use serde::{Deserialize, Serialize};

/// Whether the trade is in live-tiny or promoted mode.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradingMode {
    /// First live stage; `PerTradeCap::ModeDefault` resolves 25 bps (a boot/backtest
    /// posture — production retired the per-trade bps caps in #508).
    LiveTiny,
    /// After passing promotion gates; `ModeDefault` resolves 100 bps (same retirement).
    Promoted,
}

/// Concentration caps in basis points of bankroll, applied to `existing exposure +
/// proposed trade` per dimension (#508 Phase A).
///
/// Carried on [`RiskSnapshot`] as an `Option`: `Some(caps)` enforces the ladder (backtest
/// and tests keep the canonical `docs/19-` values via [`ConcentrationCaps::CANONICAL`]);
/// `None` = concentration is **not enforced by owner decision** — the production copy path
/// passes `None`, recorded in `docs/19-WINNER-FOLLOW-STRATEGY.md`. This is the
/// `PerTradeCap` → `per_trade_cap_bps` precedent: the policy value moves onto the snapshot
/// so the engine stays pure and the enforcement posture is explicit in the type system.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConcentrationCaps {
    /// Max per-leader exposure (docs/19 canonical: 300 bps).
    pub max_leader_bps: i32,
    /// Max per-market exposure (docs/19 canonical: 200 bps).
    pub max_market_bps: i32,
    /// Max per-family exposure (docs/19 canonical: 800 bps).
    pub max_family_bps: i32,
    /// Max total open copy exposure (docs/19 canonical: 2500 bps).
    pub max_total_copy_bps: i32,
}

impl ConcentrationCaps {
    /// The canonical `docs/19-WINNER-FOLLOW-STRATEGY.md` ladder (300/200/800/2500 bps),
    /// used wherever enforcement is kept (backtest, risk tests).
    pub const CANONICAL: Self = Self {
        max_leader_bps: 300,
        max_market_bps: 200,
        max_family_bps: 800,
        max_total_copy_bps: 2_500,
    };
}

/// Point-in-time snapshot of all risk inputs for a single proposed trade.
///
/// All exposure values are in basis points of current bankroll.
/// Positive values = open exposure. Negative PnL values = losses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    /// Absolute realized + unrealized PnL from the owning qualification baseline.
    pub absolute_pnl_bps: BasisPoints,

    // ── Kill-switch state ───────────────────────────────────────────────────
    /// Whether the service-owned copy-latency state machine has activated its kill switch.
    pub copy_latency_kill_switch_active: bool,

    // ── Proposed trade ──────────────────────────────────────────────────────
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
    /// Concentration-cap enforcement (#508 Phase A): `Some` enforces, `None` = un-enforced
    /// by owner decision (the production copy path). No serde default — every snapshot is
    /// code-constructed (none is persisted anywhere), so the posture is always explicit.
    pub concentration_caps: Option<ConcentrationCaps>,
}

fn default_per_trade_cap_bps() -> i32 {
    25
}

/// Typed, fail-closed risk inputs for the isolated canary path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanaryRiskSnapshot {
    pub origin: CanaryOrigin,
    pub proposed_worst_case_debit: CollateralAmount,
    pub canary_bankroll: CollateralAmount,
    pub leader_exposure_bps: Option<BasisPoints>,
    pub market_exposure_bps: BasisPoints,
    pub family_exposure_bps: BasisPoints,
    pub total_copy_exposure_bps: BasisPoints,
    pub open_exposure_bps: BasisPoints,
    pub resolver_tradable: bool,
    pub account_state_fresh: bool,
    pub venue_reconciliation_fresh: bool,
    pub geoblock_fresh: bool,
    pub geoblocked: bool,
    pub closed_only_fresh: bool,
    pub closed_only: bool,
    pub jurisdiction_attestation_valid: bool,
    pub pending_reservation: bool,
    pub allowance: CollateralAmount,
    pub standard_spender_only: bool,
}
