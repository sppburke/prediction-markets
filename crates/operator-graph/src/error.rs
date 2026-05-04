//! Error types for the operator-graph crate.

/// Errors produced by the operator clustering engine.
///
/// Both variants are currently unconstructed — clustering recovers from cycles
/// by treating the offending wallet as self-funded, and hop-budget exhaustion
/// is handled inline by promoting the boundary wallet to root. Variants are
/// retained as named placeholders for future graph algorithms that may
/// genuinely fail.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum OperatorGraphError {
    #[error("cycle detected in funding graph")]
    CycleDetected,
    #[error("hop count exceeds maximum of {max}")]
    HopCountExceeded { max: u8 },
}
