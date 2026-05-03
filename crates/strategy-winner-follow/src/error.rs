//! Error type for Winner-Follow strategy evaluation.

use pe_kelly_sizer::KellyError;
use pe_risk_engine::RiskBlock;

/// Reasons `WinnerFollowStrategy::evaluate` returns `Err`.
#[derive(Debug, thiserror::Error)]
pub enum WinnerFollowError {
    /// Signal kind (or requested mode) resolved to Shadow — no `OrderIntent` emitted.
    #[error("shadow mode: signal recorded but not executed")]
    ShadowMode,

    /// `LeaderAction::Flip` is blocked until `flip_human_approved = true` in config.
    #[error("flip action requires human approval (flip_human_approved = false)")]
    FlipNotApproved,

    /// Kelly sizing returned zero contracts — no edge at current price.
    #[error("no edge: Kelly sizing produced zero contracts")]
    NoEdge,

    /// Risk gate blocked the trade.
    #[error("risk blocked: {0:?}")]
    Blocked(RiskBlock),

    /// Underlying Kelly sizing computation failed (invalid inputs).
    #[error("Kelly sizing error: {0}")]
    KellySizing(#[from] KellyError),
}
