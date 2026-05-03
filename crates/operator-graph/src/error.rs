//! Error types for the operator-graph crate.

/// Errors produced by the operator clustering engine.
#[derive(Debug, thiserror::Error)]
pub enum OperatorGraphError {
    #[error("cycle detected in funding graph")]
    CycleDetected,
    #[error("hop count exceeds maximum of {max}")]
    HopCountExceeded { max: u8 },
}
