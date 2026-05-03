//! Paper-mode execution: simulate fills and record them to the event-log.
//!
//! `PaperExecutor` accepts an `OrderIntent`, simulates a fill at the limit price
//! (no slippage), and writes a `PaperFill` record to the event-log. Fills are
//! deterministic and replayable: given the same log, the same fills can be
//! reconstructed in the same order.

use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn, Writer};
use pe_venue_core::OrderIntent;
use serde::{Deserialize, Serialize};

use crate::PaperExecutionError;

const SCHEMA_VERSION: u32 = 1;
const PARSER_VERSION: u32 = 1;

/// A simulated fill recorded when an `OrderIntent` is executed in paper mode.
///
/// `simulated_fill_price = intent.limit_price` — no slippage modelled in paper mode.
/// Replayable from the event-log: deserialise the JSON payload of any frame whose
/// `schema_version = 1` and `parser_version = 1` written by a `PaperExecutor`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperFill {
    pub intent: OrderIntent,
    pub simulated_fill_price: pe_core_types::Price,
    pub simulated_at: SourceTimestamp,
}

/// Executes `OrderIntent`s in paper mode by simulating fills and writing them to
/// the event-log. Each call to `execute` writes one frame and fsyncs.
///
/// # Replayability
///
/// Every fill is written as a JSON `PaperFill` payload inside a BLAKE3-chained
/// `EventEnvelope`. Replaying the log in sequence reproduces the paper-fill
/// history deterministically.
pub struct PaperExecutor {
    writer: Writer,
    source_id: SourceId,
}

impl PaperExecutor {
    pub fn new(writer: Writer, source_id: SourceId) -> Self {
        Self { writer, source_id }
    }

    /// Simulate a fill at `intent.limit_price` and record it to the event-log.
    ///
    /// `now` is used as both `observed_at` and `received_at` on the envelope, and
    /// as `simulated_at` on the fill record. Calls `sync()` after the append so
    /// each fill is durable before returning.
    pub fn execute(
        &mut self,
        intent: &OrderIntent,
        now: SourceTimestamp,
    ) -> Result<PaperFill, PaperExecutionError> {
        let fill = PaperFill {
            intent: intent.clone(),
            simulated_fill_price: intent.limit_price,
            simulated_at: now.clone(),
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

        Ok(fill)
    }
}
