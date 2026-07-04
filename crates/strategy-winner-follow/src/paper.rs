//! Paper-mode execution: simulate fills and record them to the event-log.
//!
//! `PaperExecutor` accepts an `OrderIntent`, simulates a fill at a haircut-adjusted
//! price, and writes a `PaperFill` record to the event-log. Fills are deterministic
//! and replayable: given the same log, the same fills can be reconstructed in order.

use rust_decimal::Decimal;

use pe_core_types::{EventSeq, Price, ReceivedAt, Side, SourceId, SourceTimestamp};
use pe_event_log::{ContentType, EnvelopeIn, Writer};
use pe_venue_core::OrderIntent;
use serde::{Deserialize, Serialize};

use crate::PaperExecutionError;

const SCHEMA_VERSION: u32 = 1;
const PARSER_VERSION: u32 = 1;

/// A simulated fill recorded when an `OrderIntent` is executed in paper mode.
///
/// `simulated_fill_price` is the haircut-adjusted fill price (see [`PaperExecutor`]).
/// The JSON shape is unchanged from `schema_version = 1`: only the recorded value
/// reflects the haircut, so existing logs replay unchanged.
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
/// # Fill realism (side-split haircut)
///
/// The recorded fill price applies a basis-point haircut keyed on side, clamped to
/// the open interval `(0, 1)`:
///
/// - **BUY** pays fee + slippage: `min(limit × (1 + haircut_bps/10_000), 0.999)`.
/// - **SELL** pays slippage only (no taker fee): `max(limit × (1 − slippage_bps/10_000), 0.001)`.
///
/// `haircut_bps = 0` reproduces the un-haircut fill price for any `limit ≤ 0.999`.
/// The BUY haircut mirrors the sizing cost `c` in `evaluate`; the SELL slippage is a
/// fill-realism choice that diverges from sizing (which charges SELL nothing) and has
/// no research analog — see issue #282 Open risk #5.
///
/// # Replayability
///
/// Every fill is written as a JSON `PaperFill` payload inside a BLAKE3-chained
/// `EventEnvelope`. Replaying the log in sequence reproduces the paper-fill history
/// deterministically.
pub struct PaperExecutor {
    writer: Writer,
    source_id: SourceId,
    /// BUY-side fill haircut in basis points (fee + slippage). Default `paper_fill_haircut_bps`.
    haircut_bps: u32,
    /// SELL-side fill slippage in basis points (no taker fee). Default `paper_fill_slippage_bps`.
    slippage_bps: u32,
}

impl PaperExecutor {
    pub fn new(writer: Writer, source_id: SourceId, haircut_bps: u32, slippage_bps: u32) -> Self {
        Self {
            writer,
            source_id,
            haircut_bps,
            slippage_bps,
        }
    }

    /// Simulate a haircut-adjusted fill and record it to the event-log.
    ///
    /// `now` is used as both `observed_at` and `received_at` on the envelope, and as
    /// `simulated_at` on the fill record. Calls `sync()` after the append so each fill
    /// is durable before returning. Returns the fill together with the event-log
    /// [`EventSeq`] of its frame, so the caller can advance the reconciliation cursor.
    pub fn execute(
        &mut self,
        intent: &OrderIntent,
        now: SourceTimestamp,
    ) -> Result<(PaperFill, EventSeq), PaperExecutionError> {
        let fill = PaperFill {
            intent: intent.clone(),
            simulated_fill_price: self.simulated_fill_price(intent)?,
            simulated_at: now.clone(),
        };

        let payload = serde_json::to_vec(&fill)?;

        let seq = self.writer.append(EnvelopeIn {
            source_id: self.source_id.clone(),
            schema_version: SCHEMA_VERSION,
            parser_version: PARSER_VERSION,
            observed_at: now.clone(),
            received_at: ReceivedAt(now.0),
            content_type: ContentType::Json,
            payload,
        })?;
        self.writer.sync()?;

        Ok((fill, seq))
    }

    /// Apply the side-split haircut to `intent.limit_price` and clamp into `(0, 1)`.
    fn simulated_fill_price(&self, intent: &OrderIntent) -> Result<Price, PaperExecutionError> {
        Self::fill_price(
            intent.side,
            intent.limit_price,
            self.haircut_bps,
            self.slippage_bps,
        )
    }

    /// The haircut-adjusted fill price for `(side, limit_price)`, clamped into `(0, 1)`.
    ///
    /// Pure and side-effect free: this is the single source of truth for the paper-fill
    /// price formula (see the type-level doc). It is `pub` so the live copy path can size
    /// and gate a copy against the exact price it will fill at, rather than a separate
    /// (stale) market-mid estimate — keeping sizing/gating and the recorded fill on one
    /// price. BUY: `min(limit × (1 + haircut_bps/10_000), 0.999)`;
    /// SELL: `max(limit × (1 − slippage_bps/10_000), 0.001)`.
    pub fn fill_price(
        side: Side,
        limit_price: Price,
        haircut_bps: u32,
        slippage_bps: u32,
    ) -> Result<Price, PaperExecutionError> {
        let bps = Decimal::from(10_000u32);
        let limit = limit_price.0;
        // Upper/lower bounds keep the result a constructible `Price` (and a BUY can
        // never fill at ≥ 1, nor a SELL at ≤ 0).
        let max_fill = Decimal::new(999, 3); // 0.999
        let min_fill = Decimal::new(1, 3); // 0.001
        let clamped = match side {
            Side::Buy => {
                let raw = limit * (Decimal::ONE + Decimal::from(haircut_bps) / bps);
                raw.min(max_fill)
            }
            Side::Sell => {
                let raw = limit * (Decimal::ONE - Decimal::from(slippage_bps) / bps);
                raw.max(min_fill)
            }
        };
        Price::new(clamped).map_err(|_| PaperExecutionError::PriceOutOfRange)
    }
}
