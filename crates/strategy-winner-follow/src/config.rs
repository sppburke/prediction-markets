//! Strategy-level configuration for Winner-Follow.
//!
//! Numeric thresholds live in `_GLOSSARY.md` and `19-WINNER-FOLLOW-STRATEGY.md`;
//! they are stored in the downstream risk/sizing crates. This config holds the
//! approval flags (admin-mutable at runtime via Supabase `service_config` since #398), plus the
//! fee-rate and slippage-rate constants used to compute net cost `c` in Kelly sizing.

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

/// How each BUY copy is sized (#398 WS2; replaces the former `flat_usd_per_trade` toggle).
///
/// Canonical default: `Kelly`. See `docs/_GLOSSARY.md` `sizing_mode_default`. The KV layer stores
/// this as three flat `service_config` keys (`sizing_mode` / `sizing_dollar_usd` /
/// `sizing_contracts`) reassembled in `runtime_config::parse_config`; the `[strategy]` TOML boot
/// default uses this enum's `kind`/`value` serde form.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum SizingMode {
    /// Fractional-Kelly sizing (the full `c` / `p` / bankroll math). Default.
    #[default]
    Kelly,
    /// Fixed USD notional: `max(1, floor(usd / current_price))` contracts. Bypasses only the Kelly
    /// fraction + price-derived math; the per-trade cap, price-impact book cap, and risk gate still
    /// apply. The migration target for the legacy `flat_usd_per_trade`.
    Dollar { usd: Decimal },
    /// Fixed contract count, then clamped by the per-trade cap, price-impact book cap, and risk
    /// gate.
    Contract { contracts: u64 },
}

/// Configuration for the Winner-Follow strategy.
///
/// Approval flags default to `false` (deny). Since #398 (Decision #2) they are admin-mutable at
/// runtime via the Supabase `service_config` table (audit-logged via `updated_by`/`updated_at`),
/// applied on the next ≤60s config poll — reversing the prior "signed config change only" rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// How each BUY copy is sized (Kelly / Dollar / Contract). Default: `Kelly`. Replaces the
    /// former `flat_usd_per_trade` toggle (#398 WS2); `Dollar { usd }` is the equivalent of the
    /// old flat path. The orchestrator's per-event config rebuild (WS1) keeps this live.
    /// Canonical default: `docs/_GLOSSARY.md` `sizing_mode_default`.
    #[serde(default)]
    pub sizing_mode: SizingMode,
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
            sizing_mode: SizingMode::default(),
        }
    }
}
