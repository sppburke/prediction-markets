//! Signal classification enums shared across strategy and execution crates.

use serde::{Deserialize, Serialize};

/// Observed action of a leader trader in a single `(market, outcome)` bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LeaderAction {
    /// Opening a new position (no prior exposure on this side).
    Entry,
    /// Adding to an existing same-side position.
    Add,
    /// Partially closing an opposite-side position; more than `near_close_remaining_pct`% remains.
    Trim,
    /// Closing or nearly-closing an opposite-side position; ≤ `near_close_remaining_pct`% remains.
    Exit,
    /// Closing one side and immediately opening the reverse.
    Flip,
    /// Action could not be reliably classified; always blocked from copy.
    Unknown,
}
