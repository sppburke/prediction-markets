//! `LiveExecutor`: submits `OrderIntent` to the Polymarket CLOB and writes
//! `LiveFill` or `LiveOrderTerminal` records to the event log.
//!
//! Analogous to `PaperExecutor` in `pe-strategy-winner-follow` but for live orders.
//! `schema_version = 2` distinguishes live fills from paper fills (`schema_version = 1`).

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn, Writer};
use pe_venue_core::{OrderIntent, OrderOutcome};
use pe_venue_polymarket::{CLOBClient, PolymarketVenueAdapter};
use serde::{Deserialize, Serialize};

use crate::error::ExecutionError;

const SCHEMA_VERSION: u32 = 2;
const PARSER_VERSION: u32 = 1;

// ── Event types ───────────────────────────────────────────────────────────────

/// A live fill recorded when an `OrderIntent` is executed via the Polymarket CLOB.
///
/// Replayable from the event log: any frame with `schema_version = 2` and
/// `parser_version = 1` written by a `LiveExecutor`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveFill {
    pub intent: OrderIntent,
    pub outcome: OrderOutcome,
    pub submitted_at: SourceTimestamp,
}

/// A non-fill terminal event (rejected, expired, cancelled) recorded for replay completeness.
///
/// Replay must account for terminal events to reconstruct positions correctly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveOrderTerminal {
    pub intent: OrderIntent,
    pub outcome: OrderOutcome,
    pub submitted_at: SourceTimestamp,
}

// ── Executor ──────────────────────────────────────────────────────────────────

/// Executes `OrderIntent`s via the Polymarket CLOB and records results to the event log.
///
/// Every call to `execute` either writes a `LiveFill` or `LiveOrderTerminal` envelope
/// and fsyncs, ensuring the record is durable before returning.
///
/// Generic over `C: CLOBClient` so production code uses `ReqwestCLOBClient` and
/// tests inject `FixtureCLOBClient`.
pub struct LiveExecutor<C: CLOBClient> {
    adapter: PolymarketVenueAdapter<C>,
    writer: Writer,
    source_id: SourceId,
}

impl<C: CLOBClient> LiveExecutor<C> {
    pub fn new(adapter: PolymarketVenueAdapter<C>, writer: Writer, source_id: SourceId) -> Self {
        Self {
            adapter,
            writer,
            source_id,
        }
    }

    /// Submit `intent` to the CLOB and record the outcome to the event log.
    ///
    /// Returns the `LiveFill` on a fill/partial-fill, or the terminal record for
    /// rejected/expired/cancelled outcomes. The event log write fsyncs before returning.
    pub async fn execute(
        &mut self,
        intent: &OrderIntent,
        now: SourceTimestamp,
    ) -> Result<LiveExecuteResult, ExecutionError> {
        let outcome = self
            .adapter
            .submit(intent)
            .await
            .map_err(ExecutionError::Venue)?;

        match &outcome {
            OrderOutcome::Filled { .. } | OrderOutcome::PartialFill { .. } => {
                let fill = LiveFill {
                    intent: intent.clone(),
                    outcome: outcome.clone(),
                    submitted_at: now.clone(),
                };
                let payload = serde_json::to_vec(&fill)?;
                self.writer.append(EnvelopeIn {
                    source_id: self.source_id.clone(),
                    schema_version: SCHEMA_VERSION,
                    parser_version: PARSER_VERSION,
                    observed_at: now.clone(),
                    received_at: ReceivedAt(now.0),
                    content_type: ContentType::Json,
                    payload,
                })?;
                self.writer.sync()?;
                Ok(LiveExecuteResult::Fill(fill))
            }
            _ => {
                let terminal = LiveOrderTerminal {
                    intent: intent.clone(),
                    outcome: outcome.clone(),
                    submitted_at: now.clone(),
                };
                let payload = serde_json::to_vec(&terminal)?;
                self.writer.append(EnvelopeIn {
                    source_id: self.source_id.clone(),
                    schema_version: SCHEMA_VERSION,
                    parser_version: PARSER_VERSION,
                    observed_at: now.clone(),
                    received_at: ReceivedAt(now.0),
                    content_type: ContentType::Json,
                    payload,
                })?;
                self.writer.sync()?;
                Ok(LiveExecuteResult::Terminal(terminal))
            }
        }
    }
}

/// Result of a `LiveExecutor::execute` call.
#[derive(Debug, Clone)]
pub enum LiveExecuteResult {
    Fill(LiveFill),
    Terminal(LiveOrderTerminal),
}

impl LiveExecuteResult {
    pub fn outcome(&self) -> &OrderOutcome {
        match self {
            Self::Fill(f) => &f.outcome,
            Self::Terminal(t) => &t.outcome,
        }
    }
}
