//! Error types for Winner-Follow strategy evaluation and paper execution.

use pe_event_log::LogError;
use pe_kelly_sizer::KellyError;
use pe_risk_engine::RiskBlock;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

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

/// Durable, replay-safe projection of an entry refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum WinnerFollowDeclineAudit {
    ShadowMode,
    FlipNotApproved,
    NoEdge,
    Blocked(RiskBlock),
    KellySizing(KellyErrorAudit),
}

/// Serializable projection of [`KellyError`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum KellyErrorAudit {
    InvalidProbability { value: Decimal },
    InvalidNetPrice { value: Decimal },
    InvalidBankroll { value: Decimal },
    ContractOverflow { value: Decimal },
}

impl From<&WinnerFollowError> for WinnerFollowDeclineAudit {
    fn from(value: &WinnerFollowError) -> Self {
        match value {
            WinnerFollowError::ShadowMode => Self::ShadowMode,
            WinnerFollowError::FlipNotApproved => Self::FlipNotApproved,
            WinnerFollowError::NoEdge => Self::NoEdge,
            WinnerFollowError::Blocked(reason) => Self::Blocked(*reason),
            WinnerFollowError::KellySizing(error) => Self::KellySizing(match error {
                KellyError::InvalidProbability { value } => {
                    KellyErrorAudit::InvalidProbability { value: *value }
                }
                KellyError::InvalidNetPrice { value } => {
                    KellyErrorAudit::InvalidNetPrice { value: *value }
                }
                KellyError::InvalidBankroll { value } => {
                    KellyErrorAudit::InvalidBankroll { value: *value }
                }
                KellyError::ContractOverflow { value } => {
                    KellyErrorAudit::ContractOverflow { value: *value }
                }
            }),
        }
    }
}

/// Reasons `PaperExecutor::execute` returns `Err`.
#[derive(Debug, thiserror::Error)]
pub enum PaperExecutionError {
    /// Failed to serialise `PaperFill` to JSON before writing to the log.
    #[error("serialisation error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Event-log write or sync failed.
    #[error("log write error: {0}")]
    LogWrite(#[from] LogError),

    /// The haircut-adjusted fill price fell outside the valid `(0, 1)` range.
    /// Unreachable in practice — the haircut clamps into `[0.001, 0.999]` before
    /// constructing the `Price` — but kept so the conversion is total.
    #[error("internal: clamped fill price out of range")]
    PriceOutOfRange,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_risk_engine::RiskBlock;
    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn decline_audit_conversion_is_exhaustive_and_round_trips() {
        let errors = [
            WinnerFollowError::ShadowMode,
            WinnerFollowError::FlipNotApproved,
            WinnerFollowError::NoEdge,
            WinnerFollowError::Blocked(RiskBlock::CopyLatencyKillSwitch),
            WinnerFollowError::KellySizing(KellyError::InvalidProbability { value: dec!(1.1) }),
            WinnerFollowError::KellySizing(KellyError::InvalidNetPrice { value: dec!(0) }),
            WinnerFollowError::KellySizing(KellyError::InvalidBankroll { value: dec!(-1) }),
            WinnerFollowError::KellySizing(KellyError::ContractOverflow {
                value: dec!(18446744073709551616),
            }),
        ];

        for error in errors {
            let audit = WinnerFollowDeclineAudit::from(&error);
            let bytes = serde_json::to_vec(&audit).unwrap();
            assert_eq!(
                serde_json::from_slice::<WinnerFollowDeclineAudit>(&bytes).unwrap(),
                audit
            );
        }
    }
}
