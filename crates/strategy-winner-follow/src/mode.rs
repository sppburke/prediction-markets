//! Execution mode and signal-kind clamping rules.

use pe_core_types::WinnerFollowSignalKind;
use pe_risk_engine::snapshot::TradingMode;
use serde::{Deserialize, Serialize};

/// Execution mode for a Winner-Follow copy trade.
///
/// Signal-kind clamping (enforced by [`clamp_mode`]):
/// - `ClusterCoordination` → always `Shadow`
/// - `FreshWalletFirstTrade` → at most `Paper`
/// - `NormalLeaderFollow` → uses the requested mode unchanged
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// Record only; no `OrderIntent` emitted.
    Shadow,
    /// Paper-copy; Kelly fraction = 0.10. Execution handled by `execution-core` (#18).
    Paper,
    /// First live stage; Kelly fraction = 0.25, max_trade = 25 bps.
    LiveTiny,
    /// Post-promotion live stage; Kelly fraction = 0.25, max_trade = 100 bps.
    Promoted,
}

/// Clamp the requested mode to the ceiling imposed by the signal kind.
pub fn clamp_mode(requested: ExecutionMode, signal_kind: WinnerFollowSignalKind) -> ExecutionMode {
    match signal_kind {
        WinnerFollowSignalKind::ClusterCoordination => ExecutionMode::Shadow,
        WinnerFollowSignalKind::FreshWalletFirstTrade => match requested {
            ExecutionMode::Shadow | ExecutionMode::Paper => requested,
            ExecutionMode::LiveTiny | ExecutionMode::Promoted => ExecutionMode::Paper,
        },
        WinnerFollowSignalKind::NormalLeaderFollow => requested,
    }
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
