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

/// Provenance of a paper fill's recorded price (#486).
///
/// Serialised as a bare string on [`PaperFill`]. `LeaderHaircut` is the `#[default]`, so a
/// pre-#486 frame — written before the field existed — deserialises to `LeaderHaircut` via the
/// field's `#[serde(default)]`: old frames legitimately *are* haircut fills, so crash-recovery
/// replay (`pe_service::paper_recovery`) keeps working across the additive schema evolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum FillSource {
    /// The fresh CLOB best-ask observed at copy time (paper `clob_best_ask` BUY).
    ClobBestAsk,
    /// The `clob_best_ask_fallback_haircut_bps` fallback for a BUY with no usable ask (empty /
    /// errored / timed-out book, missing CLOB token, or a degenerate best-ask). Production
    /// Winner-Follow rejects SELLs before fill-price resolution.
    Fallback,
    /// The boot-frozen leader-price haircut: paper `leader_haircut` mode, every non-paper mode,
    /// or the recompute a `None` override takes — and every pre-#486 frame.
    #[default]
    LeaderHaircut,
}

/// A simulated fill recorded when an `OrderIntent` is executed in paper mode.
///
/// `simulated_fill_price` is the recorded fill price and `fill_source` its provenance
/// ([`FillSource`]). Since #486 the JSON shape is *additive with a defaulted field* (it was
/// "unchanged from `schema_version = 1`"): a pre-#486 frame carries no `fill_source` key and
/// deserialises it to [`FillSource::LeaderHaircut`] via `#[serde(default)]`, so existing logs
/// replay unchanged. Replayable from the event-log: deserialise the JSON payload of any frame
/// whose `schema_version = 1` and `parser_version = 1` written by a `PaperExecutor`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperFill {
    pub intent: OrderIntent,
    pub simulated_fill_price: pe_core_types::Price,
    pub simulated_at: SourceTimestamp,
    /// Provenance of `simulated_fill_price` (#486). `#[serde(default)]` → a pre-#486 frame
    /// (no key) resolves to [`FillSource::LeaderHaircut`].
    #[serde(default)]
    pub fill_source: FillSource,
}

/// Executes `OrderIntent`s in paper mode by simulating fills and writing them to
/// the event-log. Each call to `execute` writes one frame and fsyncs.
///
/// # Fill realism (side-split haircut)
///
/// The recorded fill price applies a basis-point haircut keyed on side, clamped to
/// the open interval `(0, 1)`:
///
/// - **BUY** pays fee + slippage: `clamp(limit × (1 + haircut_bps/10_000), 0.001, 0.999)`.
/// - **SELL** pays slippage only (no taker fee): `clamp(limit × (1 − slippage_bps/10_000), 0.001, 0.999)`.
///
/// (Both bounds apply on each side: the lower `0.001` guards a degenerate `limit == 0` from
/// yielding `Price(0)`, which would divide-by-zero the dollar-sizing path that consumes this.)
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

    /// Record a paper fill to the event-log, using `observed_fill_price` when the caller
    /// resolved one, else the local side-split haircut.
    ///
    /// `observed_fill_price`: `Some((price, source))` records `price`/`source` verbatim — the
    /// orchestrator's authoritative paper basis (best-ask or its fallback, #486), so the executor
    /// is a pure recorder and the fill mode stays runtime-mutable with no executor setter. `None`
    /// recomputes the haircut fill from `intent.limit_price` and tags it
    /// [`FillSource::LeaderHaircut`], byte-identical to the pre-#486 behaviour.
    ///
    /// `now` is used as both `observed_at` and `received_at` on the envelope, and as
    /// `simulated_at` on the fill record. Calls `sync()` after the append so each fill is durable
    /// before returning. Returns the fill together with the event-log [`EventSeq`] of its frame,
    /// so the caller can advance the reconciliation cursor.
    pub fn execute(
        &mut self,
        intent: &OrderIntent,
        now: SourceTimestamp,
        observed_fill_price: Option<(Price, FillSource)>,
    ) -> Result<(PaperFill, EventSeq), PaperExecutionError> {
        let (simulated_fill_price, fill_source) = match observed_fill_price {
            Some((price, source)) => (price, source),
            None => (
                self.simulated_fill_price(intent)?,
                FillSource::LeaderHaircut,
            ),
        };
        let fill = PaperFill {
            intent: intent.clone(),
            simulated_fill_price,
            simulated_at: now.clone(),
            fill_source,
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
                // Clamp into `[min_fill, max_fill]`: the upper bound keeps a BUY constructible
                // (< 1); the lower bound guards a degenerate `limit == 0` from yielding
                // `Price(0)` (valid per `Price::new`), which would divide-by-zero in the
                // dollar-sizing path that consumes this price.
                raw.min(max_fill).max(min_fill)
            }
            Side::Sell => {
                let raw = limit * (Decimal::ONE - Decimal::from(slippage_bps) / bps);
                raw.max(min_fill).min(max_fill)
            }
        };
        Price::new(clamped).map_err(|_| PaperExecutionError::PriceOutOfRange)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// AC7(ii): a keyless pre-#486 `PaperFill` payload — captured from the pre-feature struct
    /// (`tests/fixtures/pre_486_paper_fill.json`, a real payload byte-shape) — must deserialise,
    /// exactly as `pe_service::paper_recovery` does via `serde_json::from_slice`, and resolve
    /// `fill_source` to `LeaderHaircut` through `#[serde(default)]`. Without that default this
    /// `from_slice` errors and crash-recovery replay aborts on restart — invisible to any
    /// hash-chain test, which verifies stored bytes with no field-presence check.
    #[test]
    fn pre_486_keyless_frame_defaults_to_leader_haircut() {
        const PRE_486: &[u8] = include_bytes!("../tests/fixtures/pre_486_paper_fill.json");
        // Guard the fixture's provenance: a genuine pre-#486 payload carries no `fill_source` key.
        assert!(
            PRE_486
                .windows(b"fill_source".len())
                .all(|w| w != b"fill_source"),
            "fixture must be a genuine pre-#486 payload with no fill_source key"
        );
        let fill: PaperFill =
            serde_json::from_slice(PRE_486).expect("keyless pre-#486 frame must deserialise");
        assert_eq!(fill.fill_source, FillSource::LeaderHaircut);
        assert_eq!(
            fill.simulated_fill_price,
            Price::new(Decimal::new(42, 2)).unwrap()
        );
    }
}
