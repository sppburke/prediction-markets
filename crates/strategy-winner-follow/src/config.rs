//! Strategy-level configuration for Winner-Follow.
//!
//! Numeric thresholds live in `_GLOSSARY.md` and `19-WINNER-FOLLOW-STRATEGY.md`;
//! they are stored in the downstream risk/sizing crates. This config holds the
//! approval flags that require a signed config change to flip, plus the fee-rate
//! constant used to compute net cost `c` in Kelly sizing.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Configuration for the Winner-Follow strategy.
///
/// Approval flags default to `false` (deny) and require an audit-logged signed
/// config change to flip; they cannot be changed at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WinnerFollowConfig {
    /// Allow `LeaderAction::Flip` trades. Default: false.
    pub flip_human_approved: bool,
    /// Allow Kelly fractions above the mode default (> 0.25). Default: false.
    pub kelly_fraction_above_default_human_approved: bool,
    /// Polymarket BUY taker fee rate used to compute net cost `c` in Kelly sizing.
    /// See `_GLOSSARY.md` `polymarket_fee_rate`. Default: 0.04 (March 2026 model).
    #[serde(default = "default_polymarket_fee_rate")]
    pub polymarket_fee_rate: Decimal,
}

fn default_polymarket_fee_rate() -> Decimal {
    Decimal::new(4, 2)
}

impl Default for WinnerFollowConfig {
    fn default() -> Self {
        Self {
            flip_human_approved: false,
            kelly_fraction_above_default_human_approved: false,
            polymarket_fee_rate: default_polymarket_fee_rate(),
        }
    }
}
