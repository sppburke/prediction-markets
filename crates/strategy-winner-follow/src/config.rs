//! Strategy-level configuration for Winner-Follow.
//!
//! Numeric thresholds live in `_GLOSSARY.md` and `19-WINNER-FOLLOW-STRATEGY.md`;
//! they are stored in the downstream risk/sizing crates. This config holds the
//! approval flags that require a signed config change to flip, plus the edge
//! parameter used until model-calibrated probabilities land.

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
    /// Edge assumption added to `leader_price` to form the win probability `p`.
    /// Breaks the `p=c` deadlock until model-calibrated `p` lands in Phase 3.
    /// See `_GLOSSARY.md` `winner_follow_leader_alpha`. Default: 0.05.
    #[serde(default = "default_leader_alpha")]
    pub leader_alpha: Decimal,
}

fn default_leader_alpha() -> Decimal {
    Decimal::new(5, 2)
}

impl Default for WinnerFollowConfig {
    fn default() -> Self {
        Self {
            flip_human_approved: false,
            kelly_fraction_above_default_human_approved: false,
            leader_alpha: default_leader_alpha(),
        }
    }
}
