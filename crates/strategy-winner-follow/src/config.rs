//! Strategy-level configuration flags for Winner-Follow.
//!
//! Numeric thresholds live in `_GLOSSARY.md` and `19-WINNER-FOLLOW-STRATEGY.md`;
//! they are stored in the downstream risk/sizing crates. This config holds the
//! approval flags that require a signed config change to flip.

use serde::{Deserialize, Serialize};

/// First-class approval flags for Winner-Follow.
///
/// Both flags default to `false` (deny). Changing either requires an audit-logged
/// signed config change; they cannot be flipped at runtime.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WinnerFollowConfig {
    /// Allow `LeaderAction::Flip` trades. Default: false.
    pub flip_human_approved: bool,
    /// Allow Kelly fractions above the mode default (> 0.25). Default: false.
    pub kelly_fraction_above_default_human_approved: bool,
}
