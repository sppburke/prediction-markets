//! Strategy-level configuration for Winner-Follow.
//!
//! Numeric thresholds live in `_GLOSSARY.md` and `19-WINNER-FOLLOW-STRATEGY.md`;
//! they are stored in the downstream risk/sizing crates. This config holds the
//! approval flags that require a signed config change to flip, plus the fee-rate
//! and slippage-rate constants used to compute net cost `c` in Kelly sizing.

use pe_core_types::KellyFraction;
use pe_risk_engine::snapshot::TradingMode;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Per-trade size cap configuration.
///
/// Canonical defaults in `docs/_GLOSSARY.md` (`per_trade_cap_default`, `per_trade_cap_unlimited_resolved_bps`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum PerTradeCap {
    /// Use the mode-keyed defaults: 25 bps for LiveTiny, 100 bps for Promoted.
    #[default]
    ModeDefault,
    /// Explicit cap in basis points of bankroll.
    Bps(i32),
    /// No per-trade cap — effective cap is 10_000 bps (= full bankroll).
    Unlimited,
}

impl PerTradeCap {
    /// Resolve to a concrete cap in basis points.
    ///
    /// `ModeDefault` maps 25 / 100 by mode; `Unlimited` resolves to 10_000 bps (full bankroll).
    pub fn resolve_bps(self, mode: TradingMode) -> i32 {
        match self {
            PerTradeCap::ModeDefault => match mode {
                TradingMode::LiveTiny => 25,
                TradingMode::Promoted => 100,
            },
            PerTradeCap::Bps(n) => n,
            PerTradeCap::Unlimited => 10_000,
        }
    }
}

/// Configuration for the Winner-Follow strategy.
///
/// Approval flags default to `false` (deny) and require an audit-logged signed
/// config change to flip; they cannot be changed at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WinnerFollowConfig {
    /// Allow `LeaderAction::Flip` trades. Default: false.
    #[serde(default)]
    pub flip_human_approved: bool,
    /// Allow Kelly fractions above the mode default (> 0.25). Default: false.
    #[serde(default)]
    pub kelly_fraction_above_default_human_approved: bool,
    /// Polymarket BUY taker fee rate used to compute net cost `c` in Kelly sizing.
    /// See `_GLOSSARY.md` `polymarket_fee_rate`. Default: 0.04 (March 2026 model).
    #[serde(default = "default_polymarket_fee_rate")]
    pub polymarket_fee_rate: Decimal,
    /// When set, overrides all mode-based Kelly fractions in all contexts (including
    /// production). Defaults to `None` (mode-based selection applies).
    #[serde(default)]
    pub kelly_fraction_override: Option<KellyFraction>,
    /// Per-trade size cap. Default: `ModeDefault` (25 bps LiveTiny / 100 bps Promoted).
    ///
    /// In backtest, override with `PE_BACKTEST_PER_TRADE_CAP=unlimited` to remove the cap
    /// and observe true Kelly-fraction effects. See `docs/_GLOSSARY.md`.
    #[serde(default)]
    pub per_trade_cap: PerTradeCap,
    /// Expected fill slippage rate added to `c` for BUY orders, as a fraction.
    /// Canonical default: `slippage_rate = 0.01` (100 bps). See `docs/_GLOSSARY.md`.
    #[serde(default = "default_slippage_rate")]
    pub slippage_rate: Decimal,
}

fn default_polymarket_fee_rate() -> Decimal {
    Decimal::new(4, 2)
}

fn default_slippage_rate() -> Decimal {
    Decimal::new(1, 2)
}

impl Default for WinnerFollowConfig {
    fn default() -> Self {
        Self {
            flip_human_approved: false,
            kelly_fraction_above_default_human_approved: false,
            polymarket_fee_rate: default_polymarket_fee_rate(),
            kelly_fraction_override: None,
            per_trade_cap: PerTradeCap::default(),
            slippage_rate: default_slippage_rate(),
        }
    }
}
