use serde::{Deserialize, Serialize};

/// Execution direction. Buy and sell are the only possible directions — not #[non_exhaustive].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}
