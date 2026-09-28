//! Owner-requested live-mode reconciliation. The requested mode is the only mode command.

/// An order-path check result. These checks never change the account's effective mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckOutcome {
    Pass,
    PersistentFail(&'static str),
    Transient(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModeDecision {
    Keep,
    SetEffective {
        mode: &'static str,
        reason: &'static str,
    },
}

/// Compare only the owner's request and the effective mode. The SQL RPC rechecks the
/// request under the account lock before it commits this proposal.
#[must_use]
pub fn evaluate_mode(requested: &str, effective: &str) -> ModeDecision {
    if requested == effective {
        ModeDecision::Keep
    } else if requested == "live_tiny" {
        ModeDecision::SetEffective {
            mode: "live_tiny",
            reason: "owner requested live_tiny",
        }
    } else {
        ModeDecision::SetEffective {
            mode: "off",
            reason: "owner requested off",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_request_alone_controls_mode() {
        assert_eq!(evaluate_mode("off", "off"), ModeDecision::Keep);
        assert_eq!(evaluate_mode("live_tiny", "live_tiny"), ModeDecision::Keep);
        assert_eq!(
            evaluate_mode("off", "live_tiny"),
            ModeDecision::SetEffective {
                mode: "off",
                reason: "owner requested off"
            }
        );
        assert_eq!(
            evaluate_mode("live_tiny", "off"),
            ModeDecision::SetEffective {
                mode: "live_tiny",
                reason: "owner requested live_tiny"
            }
        );
    }
}
