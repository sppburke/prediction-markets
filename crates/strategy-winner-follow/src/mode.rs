//! Execution mode for Winner-Follow copy trades.

use pe_risk_engine::snapshot::TradingMode;
use serde::{Deserialize, Serialize};

/// Execution mode for a Winner-Follow copy trade.
///
/// The requested mode is used as-is; copy signals are ordinary leader-follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// Record only; no `OrderIntent` emitted.
    Shadow,
    /// Paper-copy; Kelly fraction = 0.10. Execution handled by `execution-core` (#18).
    Paper,
    /// First live stage; Kelly fraction = 0.25. `ModeDefault` resolves to 25 bps here; the
    /// production per-trade cap policy (reviewed `unlimited`, valid post-activation rows) is
    /// canonical in `docs/19-WINNER-FOLLOW-STRATEGY.md`.
    LiveTiny,
    /// Post-promotion live stage; Kelly fraction = 0.25. `ModeDefault` resolves to 100 bps
    /// here; the production cap policy is canonical in `docs/19-WINNER-FOLLOW-STRATEGY.md`.
    Promoted,
}

/// Map an `ExecutionMode` to the risk-engine's `TradingMode`.
///
/// `Paper` uses `LiveTiny` caps (most conservative live setting).
/// Never called for `Shadow` — callers must handle that before reaching risk.
pub fn to_risk_trading_mode(mode: ExecutionMode) -> TradingMode {
    match mode {
        ExecutionMode::Paper | ExecutionMode::LiveTiny => TradingMode::LiveTiny,
        ExecutionMode::Promoted => TradingMode::Promoted,
        // Shadow is filtered before reaching the risk gate; treat as unreachable.
        ExecutionMode::Shadow => TradingMode::LiveTiny,
    }
}
