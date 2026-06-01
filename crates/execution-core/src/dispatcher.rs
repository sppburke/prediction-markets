//! `ExecutionDispatcher`: routes `OrderIntent` to paper or live executor by mode.
//!
//! Mode dispatch:
//! - `Shadow` / `Paper` → `PaperExecutor` (no live funds involved)
//! - `LiveTiny` / `Promoted` → `LiveExecutor` (real CLOB submission)
//!
//! `PaperExecutor` is imported from `pe-strategy-winner-follow` — not moved.

use pe_core_types::{EventSeq, SourceTimestamp};
use pe_strategy_winner_follow::{ExecutionMode, PaperExecutor, PaperFill};
use pe_venue_core::OrderIntent;
use pe_venue_polymarket::CLOBClient;

use crate::error::ExecutionError;
use crate::live::{LiveExecuteResult, LiveExecutor};

/// Routes `OrderIntent` to paper or live execution based on `ExecutionMode`.
///
/// The mode is consulted on every call, so it can change between calls without
/// re-constructing the dispatcher.
pub struct ExecutionDispatcher<C: CLOBClient> {
    paper: PaperExecutor,
    live: LiveExecutor<C>,
}

impl<C: CLOBClient> ExecutionDispatcher<C> {
    pub fn new(paper: PaperExecutor, live: LiveExecutor<C>) -> Self {
        Self { paper, live }
    }

    /// Route `intent` to paper or live executor based on `mode`.
    pub async fn execute(
        &mut self,
        intent: &OrderIntent,
        mode: ExecutionMode,
        now: SourceTimestamp,
    ) -> Result<DispatchResult, ExecutionError> {
        match mode {
            ExecutionMode::Shadow | ExecutionMode::Paper => {
                let (fill, seq) = self.paper.execute(intent, now)?;
                Ok(DispatchResult::Paper { fill, seq })
            }
            ExecutionMode::LiveTiny | ExecutionMode::Promoted => {
                let result = self.live.execute(intent, now).await?;
                Ok(DispatchResult::Live(result))
            }
        }
    }
}

/// Outcome of a dispatch call: either a paper fill or a live result.
///
/// `Paper` carries the event-log [`EventSeq`] of the written fill frame so the
/// service tier can advance the `paper-state` reconciliation cursor.
#[derive(Debug, Clone)]
pub enum DispatchResult {
    Paper { fill: PaperFill, seq: EventSeq },
    Live(LiveExecuteResult),
}
