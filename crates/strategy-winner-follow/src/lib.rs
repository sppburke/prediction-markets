//! `pe-strategy-winner-follow` — Winner-Follow strategy evaluation.
//!
//! # Modules
//!
//! - **evaluate** — pure signal → Kelly → risk → `OrderIntent` pipeline. No I/O.
//!
//! `float_arithmetic = "deny"` — all numeric operations use integers or `rust_decimal`.
//! No `unwrap`/`expect`/`panic!` in production code.

pub mod config;
pub mod error;
pub mod evaluate;
pub mod mode;

pub use config::{PerTradeCap, SizingMode, WinnerFollowConfig};
pub use error::{
    KellyErrorAudit, RiskInputsUnavailable, WinnerFollowDeclineAudit, WinnerFollowError,
};
pub use evaluate::{
    OrganicCanaryOrder, OrganicCanaryPolicy, OrganicDecisionProof, WinnerFollowStrategy,
    build_idempotency_key, organic_decision_proof_hash,
};
pub use mode::ExecutionMode;
