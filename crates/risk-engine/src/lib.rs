//! Pure, deterministic risk gate. Takes a typed `RiskSnapshot` (no live queries) and returns
//! `RiskApproved` or a `RiskBlock` variant with the reason. No I/O, no network, deterministic
//! from the snapshot.

pub mod block;
pub mod engine;
pub mod snapshot;

pub use block::RiskBlock;
pub use engine::evaluate_risk;
pub use snapshot::{RiskSnapshot, TradingMode};
