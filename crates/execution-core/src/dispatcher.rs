//! Ordinary paper-only `ExecutionDispatcher`.
//!
//! Mode dispatch:
//! - `Shadow` / `Paper` → `PaperExecutor` (no live funds involved)
//! - `LiveTiny` / `Promoted` → fail closed (the isolated canary owns the only live POST seam)
//!
//! `PaperExecutor` is imported from `pe-strategy-winner-follow` — not moved.

use pe_core_types::{EventSeq, Price, SourceTimestamp};
use pe_event_log::{AppendReceipt, PoisonReason};
use pe_strategy_winner_follow::{ExecutionMode, FillSource, PaperExecutor, PaperFill};
use pe_venue_core::OrderIntent;

use crate::error::ExecutionError;

/// Routes `OrderIntent` to paper or live execution based on `ExecutionMode`.
///
/// The mode is consulted on every call, so it can change between calls without
/// re-constructing the dispatcher.
pub struct ExecutionDispatcher {
    paper: PaperExecutor,
}

impl ExecutionDispatcher {
    /// Construct the ordinary service dispatcher. Credentialed execution is deliberately absent;
    /// the isolated canary actor is the only production owner of a POST seam.
    pub fn paper_only(paper: PaperExecutor) -> Self {
        Self { paper }
    }

    /// Typed paper-log durability state for readiness and bounded producer shutdown (#544).
    pub fn paper_poisoned(&self) -> Option<&PoisonReason> {
        self.paper.poisoned()
    }

    /// Append one orchestrator-serialized paper record through the sole paper writer.
    pub fn append_paper_payload_synced(
        &mut self,
        schema_version: u32,
        parser_version: u32,
        observed_at: SourceTimestamp,
        payload: Vec<u8>,
    ) -> Result<AppendReceipt, ExecutionError> {
        self.paper
            .append_payload_synced(schema_version, parser_version, observed_at, payload)
            .map_err(ExecutionError::from)
    }

    /// Route `intent` to paper or live executor based on `mode`.
    ///
    /// `observed_fill_price` carries the orchestrator-resolved paper fill basis (#486): `Some`
    /// on the paper path so `PaperExecutor` records it verbatim, `None` elsewhere (the executor
    /// recomputes the local haircut). The live executor ignores it — a live order fills at the
    /// venue's real price regardless.
    pub async fn execute(
        &mut self,
        intent: &OrderIntent,
        mode: ExecutionMode,
        now: SourceTimestamp,
        observed_fill_price: Option<(Price, FillSource)>,
    ) -> Result<DispatchResult, ExecutionError> {
        match mode {
            ExecutionMode::Shadow | ExecutionMode::Paper => {
                match self.paper.execute(intent, now, observed_fill_price) {
                    Ok((fill, seq)) => Ok(DispatchResult::Paper { fill, seq }),
                    Err(error) => match self.paper.poisoned().copied() {
                        Some(reason) => Err(ExecutionError::PaperDurabilityUncertain { reason }),
                        None => Err(error.into()),
                    },
                }
            }
            ExecutionMode::LiveTiny | ExecutionMode::Promoted => Err(ExecutionError::Live(
                "ordinary credentialed dispatch is retired; use the isolated inactive canary role"
                    .to_owned(),
            )),
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
}
