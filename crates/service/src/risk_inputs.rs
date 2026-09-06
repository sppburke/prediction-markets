//! Replayable paper/live risk-input composition and daily-mark validation (#545).
//!
//! These reducers combine the active financial snapshot with causal price, mark, PnL, exposure,
//! and latency evidence. Missing, stale, conflicting, or incoherent evidence fails closed with a
//! service-private diagnostic before strategy evaluation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use pe_core_types::{EventSeq, MarketId, OutcomeId, Price};
use pe_event_log::{AppendReceipt, Reader};
use pe_paper_state::FinancialSnapshot;
use pe_risk_engine::{
    EquityInputs, PnlWindow, RiskHaltCause, RiskMathError, RiskSnapshot, current_equity,
    latency_switch, nearest_rank_p95, pnl_bps,
};
use pe_source_polymarket_public::ClassifiedPricesHistory;
use rust_decimal::Decimal;

#[cfg(test)]
use crate::paper_recovery::active_risk_halts;
use crate::paper_recovery::{
    FinancialPayload, FinancialResult, HaltState, PaperEra, PaperLogFrame, PaperLogRecord,
    RiskHaltOwner, oldest_unmatched_prepared,
};

const SECONDS_PER_HOUR: i64 = 3_600;
const SECONDS_PER_DAY: i64 = 86_400;
pub(crate) const MAX_HISTORICAL_MARK_AGE_SECS: i64 = 120;

/// Service-owned reason that replayable evidence could not produce a risk snapshot; the strategy
/// crate sees only the unit `RiskInputsUnavailable` decline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RiskInputsUnavailable {
    #[error("financial snapshot sequence does not match the completed paper-log prefix")]
    SnapshotSequenceMismatch,
    #[error("the paper log has an unmatched FinancialPrepared record")]
    UnmatchedPrepared,
    #[error("a required position price is missing")]
    PriceMissing,
    #[error("a required position price is stale")]
    PriceStale,
    #[error("a required position price is from the future")]
    PriceFuture,
    #[error("position price evidence conflicts")]
    PriceConflict,
    #[error("the immediately preceding midnight mark is missing")]
    MarkMissing,
    #[error("the immediately preceding midnight mark is duplicated")]
    MarkDuplicate,
    #[error("the immediately preceding midnight mark is invalid")]
    MarkInvalid,
    #[error("the fixed qualification baseline is not positive")]
    BaselineNonPositive,
    #[error("exact risk arithmetic overflowed")]
    Overflow,
}

#[derive(Debug, thiserror::Error)]
pub enum BoundaryMarkError {
    #[error("historical mark transport is retryable: {0}")]
    Retryable(String),
    #[error("source-log coordinator closed")]
    SourceLogClosed,
    #[error("historical mark classification failed: {0}")]
    Classification(String),
    #[error("historical mark evidence is invalid: {0}")]
    Invalid(RiskInputsUnavailable),
}

/// Strict historical price selected for a causal paper/live daily mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoricalMarkPrice {
    pub price: Price,
    pub sample_unix: i64,
    pub receipt: AppendReceipt,
}

/// Select the latest unique sample at-or-before the cutoff from an already-recorded response.
/// The caller owns transport retries and must append the response before calling this function.
pub(crate) fn historical_mark_price(
    classified: &ClassifiedPricesHistory,
    cutoff_unix: i64,
    receipt: AppendReceipt,
) -> Result<HistoricalMarkPrice, RiskInputsUnavailable> {
    let points = match classified {
        ClassifiedPricesHistory::Points(points) => points,
        ClassifiedPricesHistory::Empty => return Err(RiskInputsUnavailable::PriceMissing),
        ClassifiedPricesHistory::Rejected { .. } => {
            return Err(RiskInputsUnavailable::MarkInvalid);
        }
    };
    let Some(latest_unix) = points
        .iter()
        .filter(|point| point.t <= cutoff_unix)
        .map(|point| point.t)
        .max()
    else {
        return if points.iter().any(|point| point.t > cutoff_unix) {
            Err(RiskInputsUnavailable::PriceFuture)
        } else {
            Err(RiskInputsUnavailable::PriceMissing)
        };
    };
    let mut matching = points.iter().filter(|point| point.t == latest_unix);
    let first = matching.next().ok_or(RiskInputsUnavailable::PriceMissing)?;
    if matching.any(|point| point.price != first.price) {
        return Err(RiskInputsUnavailable::PriceConflict);
    }
    let age = cutoff_unix
        .checked_sub(latest_unix)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    if age > MAX_HISTORICAL_MARK_AGE_SECS {
        return Err(RiskInputsUnavailable::PriceStale);
    }
    let price = Price::new(first.price).map_err(|_| RiskInputsUnavailable::MarkInvalid)?;
    Ok(HistoricalMarkPrice {
        price,
        sample_unix: latest_unix,
        receipt,
    })
}

/// A manual release target proven to be the currently active event named by the incident row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditedHaltRelease {
    pub owner: RiskHaltOwner,
    pub cause: RiskHaltCause,
    pub engaged_receipt: AppendReceipt,
}

/// Match an incident-row hash only to its own currently active absolute-loss cause, or to a
/// latency cause whose latest completed hour is unavailable. Stale, already-consumed,
/// mismatched-cause, non-starved latency, and unknown hashes produce `None`.
#[must_use]
pub fn audited_halt_release(
    era: &PaperEra,
    active: &HashSet<(RiskHaltOwner, RiskHaltCause)>,
    release_hash: &str,
    latest_latency_p95_ms: Option<u64>,
) -> Option<AuditedHaltRelease> {
    era.frames.iter().enumerate().find_map(|(index, frame)| {
        if frame.receipt.this_hash.to_hex().as_str() != release_hash {
            return None;
        }
        let PaperLogFrame::Record(PaperLogRecord::RiskHaltChanged {
            owner,
            cause,
            state: HaltState::Engaged,
            ..
        }) = &frame.frame
        else {
            return None;
        };
        if !matches!(
            cause,
            RiskHaltCause::AbsoluteLoss | RiskHaltCause::CopyLatency
        ) || !active.contains(&(owner.clone(), *cause))
            || (*cause == RiskHaltCause::CopyLatency && latest_latency_p95_ms.is_some())
            || era.frames[index.saturating_add(1)..]
                .iter()
                .any(|later| match &later.frame {
                    PaperLogFrame::Record(PaperLogRecord::RiskHaltChanged {
                        owner: later_owner,
                        cause: later_cause,
                        ..
                    }) => later_owner == owner && later_cause == cause,
                    _ => false,
                })
        {
            return None;
        }
        Some(AuditedHaltRelease {
            owner: owner.clone(),
            cause: *cause,
            engaged_receipt: frame.receipt,
        })
    })
}

/// Latest completed Prepared from the scanner-validated financial protocol. The shared scanner is
/// the state-machine owner; risk builds no second reducer.
#[must_use]
pub fn latest_completed_prepared(era: &PaperEra) -> Option<EventSeq> {
    era.frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt, ..
            }) => Some(prepared_receipt.sequence),
            _ => None,
        })
}

/// Completed financial facts whose source observation is inside the boundary prefix and was
/// received strictly before the cutoff. This is the shared causal filter for historical marks.
pub(crate) fn completed_prepared_before_boundary(
    era: &PaperEra,
    source_log_path: &Path,
    cutoff_unix: i64,
    boundary_receipt: AppendReceipt,
) -> Result<HashSet<EventSeq>, RiskInputsUnavailable> {
    let cutoff_ms = cutoff_unix
        .checked_mul(1_000)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    let prepared = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { payload, .. }) => {
                Some((frame.receipt.sequence, payload))
            }
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    let mut completed = HashSet::new();
    for frame in &era.frames {
        let PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
            prepared_receipt, ..
        }) = &frame.frame
        else {
            continue;
        };
        if frame.envelope.received_at.0.unix_timestamp() >= cutoff_unix {
            continue;
        }
        let payload = prepared
            .get(&prepared_receipt.sequence)
            .ok_or(RiskInputsUnavailable::UnmatchedPrepared)?;
        let source_receipt = match payload {
            FinancialPayload::Fill { economic, .. } => economic
                .observation
                .as_ref()
                .map(|observation| observation.source_receipt)
                .ok_or(RiskInputsUnavailable::PriceMissing)?,
            FinancialPayload::Resolution {
                resolution_source_receipt,
                ..
            } => *resolution_source_receipt,
        };
        if source_receipt.sequence <= boundary_receipt.sequence
            && source_receipt_received_millis(source_log_path, source_receipt)? < cutoff_ms
        {
            completed.insert(prepared_receipt.sequence);
        }
    }
    Ok(completed)
}

/// Compose exact paper PnL/latency into the existing proposal snapshot. Exposure, proposal, and cap
/// fields are preserved from `base`; zeroed proposal facts are never manufactured here.
pub fn build_paper_risk_snapshot(
    base: &RiskSnapshot,
    snapshot: &FinancialSnapshot,
    era: &PaperEra,
    current_prices: &HashMap<(MarketId, OutcomeId), Price>,
    source_log_path: &Path,
    now_unix: i64,
    latency_was_active: bool,
) -> Result<RiskSnapshot, RiskInputsUnavailable> {
    let Some((start_receipt, start)) = &era.start else {
        return Err(RiskInputsUnavailable::SnapshotSequenceMismatch);
    };
    if oldest_unmatched_prepared(era).is_some() {
        return Err(RiskInputsUnavailable::UnmatchedPrepared);
    }
    if snapshot.start != Some((start_receipt.sequence, start_receipt.this_hash))
        || snapshot.last_prepared_seq != latest_completed_prepared(era)
    {
        return Err(RiskInputsUnavailable::SnapshotSequenceMismatch);
    }

    let mut valued_positions = Vec::with_capacity(snapshot.positions.len());
    for position in &snapshot.positions {
        let quantity = position
            .long
            .checked_sub(position.short)
            .map_err(|_| RiskInputsUnavailable::Overflow)?;
        if quantity == pe_core_types::ShareAmount::ZERO {
            continue;
        }
        let price = current_prices
            .get(&(position.market_id.clone(), position.outcome_id))
            .copied()
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        valued_positions.push((quantity, price));
    }
    let equity = current_equity(&EquityInputs {
        cash: snapshot.cash,
        positions: &valued_positions,
    })
    .map_err(risk_math_error)?;
    let realized_closes_7d =
        snapshot
            .settlements_7d
            .iter()
            .try_fold(Decimal::ZERO, |total, settlement| {
                let costs = snapshot
                    .fills_for_open_and_7d
                    .iter()
                    .filter(|fill| fill.market_id == settlement.market_id)
                    .try_fold(Decimal::ZERO, |cost, fill| {
                        fill.principal
                            .checked_add(fill.fee)
                            .ok()
                            .and_then(|debit| cost.checked_add(debit.to_decimal()))
                            .ok_or(RiskInputsUnavailable::Overflow)
                    })?;
                settlement
                    .credit_applied
                    .checked_sub(costs)
                    .and_then(|close| total.checked_add(close))
                    .ok_or(RiskInputsUnavailable::Overflow)
            })?;
    let pnl = pnl_bps(
        equity,
        &PnlWindow {
            starting_bankroll: start.starting_bankroll.to_decimal(),
            preceding_mark_equity: preceding_midnight_equity(era, now_unix)?,
            realized_closes_7d,
        },
    )
    .map_err(risk_math_error)?;
    let latency = paper_latency_samples(era, source_log_path, now_unix)?;

    let mut composed = base.clone();
    composed.intraday_pnl_bps = pnl.intraday;
    composed.rolling_7d_pnl_bps = pnl.rolling_7d;
    composed.absolute_pnl_bps = pnl.absolute;
    composed.copy_latency_kill_switch_active = latency.switch_active(latency_was_active);
    Ok(composed)
}

fn risk_math_error(error: RiskMathError) -> RiskInputsUnavailable {
    match error {
        RiskMathError::NonPositive | RiskMathError::Empty => {
            RiskInputsUnavailable::BaselineNonPositive
        }
        RiskMathError::Overflow => RiskInputsUnavailable::Overflow,
    }
}

/// Resolve the immediately preceding required UTC-midnight equity. Before the first completed
/// midnight following Start, `None` means the fixed starting bankroll is the baseline.
pub(crate) fn preceding_midnight_equity(
    era: &PaperEra,
    now_unix: i64,
) -> Result<Option<Decimal>, RiskInputsUnavailable> {
    let Some((_, _)) = &era.start else {
        return Err(RiskInputsUnavailable::MarkMissing);
    };
    let start_unix = era
        .frames
        .iter()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::QualificationStarted(_)) => {
                Some(frame.envelope.received_at.0.unix_timestamp())
            }
            _ => None,
        })
        .ok_or(RiskInputsUnavailable::MarkMissing)?;
    let first_cutoff = start_unix
        .div_euclid(SECONDS_PER_DAY)
        .checked_add(1)
        .and_then(|day| day.checked_mul(SECONDS_PER_DAY))
        .ok_or(RiskInputsUnavailable::Overflow)?;
    if now_unix < start_unix {
        return Err(RiskInputsUnavailable::MarkInvalid);
    }
    if now_unix < first_cutoff {
        return Ok(None);
    }
    let required_cutoff = now_unix
        .div_euclid(SECONDS_PER_DAY)
        .checked_mul(SECONDS_PER_DAY)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    let mut candidates = era.frames.iter().filter_map(|frame| {
        let PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) = &frame.frame else {
            return None;
        };
        (mark.cutoff_unix == required_cutoff).then_some((frame, mark.as_ref()))
    });
    let Some((frame, mark)) = candidates.next() else {
        return Err(RiskInputsUnavailable::MarkMissing);
    };
    if candidates.next().is_some() {
        return Err(RiskInputsUnavailable::MarkDuplicate);
    }
    let recorded_unix = frame.envelope.received_at.0.unix_timestamp();
    if recorded_unix < required_cutoff
        || recorded_unix > now_unix
        || mark.invalid.is_some()
        || mark.equity < Decimal::ZERO
    {
        return Err(RiskInputsUnavailable::MarkInvalid);
    }
    Ok(Some(mark.equity))
}

/// One completed prior-hour latency derivation retained in Prepared/mark/halt evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencyHour {
    pub start_unix: i64,
    pub end_unix: i64,
    pub sample_count: usize,
    pub p95_ms: Option<u64>,
}

/// Last two completed prior-hour values and the resulting owner-local switch state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatencySamples {
    pub previous: LatencyHour,
    pub latest: LatencyHour,
}

impl LatencySamples {
    #[must_use]
    pub fn switch_active(self, currently_active: bool) -> bool {
        latency_switch(currently_active, self.previous.p95_ms, self.latest.p95_ms)
    }
}

/// Derive paper latency from each Fill Final envelope's `received_at` minus its Prepared
/// observation timestamp. Completion belongs to the hour containing the Final endpoint.
pub(crate) fn paper_latency_samples(
    era: &PaperEra,
    source_log_path: &Path,
    now_unix: i64,
) -> Result<LatencySamples, RiskInputsUnavailable> {
    let latest_end = now_unix
        .div_euclid(SECONDS_PER_HOUR)
        .checked_mul(SECONDS_PER_HOUR)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    let latest_start = latest_end
        .checked_sub(SECONDS_PER_HOUR)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    let previous_start = latest_start
        .checked_sub(SECONDS_PER_HOUR)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    let all_samples = paper_latency_endpoint_samples(era, source_log_path)?;
    Ok(LatencySamples {
        previous: latency_hour(&all_samples, previous_start)?,
        latest: latency_hour(&all_samples, latest_start)?,
    })
}

fn latency_hour(
    samples: &[(i64, u64)],
    start_unix: i64,
) -> Result<LatencyHour, RiskInputsUnavailable> {
    let end_unix = start_unix
        .checked_add(SECONDS_PER_HOUR)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    let values = samples
        .iter()
        .filter(|(endpoint, _)| (start_unix..end_unix).contains(endpoint))
        .map(|(_, value)| *value)
        .collect::<Vec<_>>();
    Ok(LatencyHour {
        start_unix,
        end_unix,
        sample_count: values.len(),
        p95_ms: nearest_rank_p95(&values),
    })
}

fn paper_latency_endpoint_samples(
    era: &PaperEra,
    source_log_path: &Path,
) -> Result<Vec<(i64, u64)>, RiskInputsUnavailable> {
    let prepared = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                payload: FinancialPayload::Fill { economic, .. },
                ..
            }) => economic
                .observation
                .as_ref()
                .map(|observation| (frame.receipt.sequence, (frame.receipt, observation))),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut samples = Vec::new();
    for frame in &era.frames {
        let PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
            prepared_receipt,
            result: FinancialResult::Fill { .. },
        }) = &frame.frame
        else {
            continue;
        };
        let final_unix = frame.envelope.received_at.0.unix_timestamp();
        let Some((known_receipt, observation)) = prepared.get(&prepared_receipt.sequence) else {
            return Err(RiskInputsUnavailable::UnmatchedPrepared);
        };
        if *known_receipt != *prepared_receipt {
            return Err(RiskInputsUnavailable::UnmatchedPrepared);
        }
        let final_ms = frame
            .envelope
            .received_at
            .0
            .unix_timestamp_nanos()
            .checked_div(1_000_000)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(RiskInputsUnavailable::Overflow)?;
        let observed_ms =
            source_receipt_received_millis(source_log_path, observation.source_receipt)?;
        let latency_ms = final_ms
            .checked_sub(observed_ms)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(RiskInputsUnavailable::Overflow)?;
        samples.push((final_unix, latency_ms));
    }
    Ok(samples)
}

fn source_receipt_received_millis(
    source_log_path: &Path,
    receipt: AppendReceipt,
) -> Result<i64, RiskInputsUnavailable> {
    for item in Reader::replay(source_log_path).map_err(|_| RiskInputsUnavailable::PriceMissing)? {
        let (_sequence, envelope) = item.map_err(|_| RiskInputsUnavailable::PriceMissing)?;
        if envelope.seq != receipt.sequence {
            continue;
        }
        if envelope.this_hash != receipt.this_hash {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        return envelope
            .received_at
            .0
            .unix_timestamp_nanos()
            .checked_div(1_000_000)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(RiskInputsUnavailable::Overflow);
    }
    Err(RiskInputsUnavailable::PriceMissing)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{
        AccountId, CollateralAmount, ReceivedAt, SourceId, SourceTimestamp, WalletAddress,
    };
    use pe_event_log::{ContentType, EventEnvelope};
    use pe_source_polymarket_public::PricePoint;
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    use super::*;
    use crate::paper_recovery::ScannedPaperFrame;

    fn receipt(sequence: u64, byte: u8) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::Hash::from_bytes([byte; 32]),
        }
    }

    fn frame(sequence: u64, unix: i64, record: PaperLogRecord) -> ScannedPaperFrame {
        let receipt = receipt(sequence, u8::try_from(sequence).unwrap_or(u8::MAX));
        let at = OffsetDateTime::from_unix_timestamp(unix).unwrap();
        ScannedPaperFrame {
            envelope: EventEnvelope {
                seq: receipt.sequence,
                source_id: SourceId("paper".to_owned()),
                schema_version: 2,
                parser_version: 1,
                observed_at: SourceTimestamp(at),
                received_at: ReceivedAt(at),
                content_type: ContentType::Json,
                raw_payload_hash: blake3::hash(b"payload"),
                prev_hash: blake3::Hash::from_bytes([0; 32]),
                this_hash: receipt.this_hash,
                payload: Vec::new(),
            },
            receipt,
            frame: PaperLogFrame::Record(record),
            legacy_fill: None,
        }
    }

    fn halt(
        sequence: u64,
        owner: RiskHaltOwner,
        cause: RiskHaltCause,
        state: HaltState,
    ) -> ScannedPaperFrame {
        frame(
            sequence,
            i64::try_from(sequence).unwrap(),
            PaperLogRecord::RiskHaltChanged {
                owner,
                cause,
                state,
                evidence: serde_json::json!({}),
            },
        )
    }

    fn account() -> RiskHaltOwner {
        RiskHaltOwner::LiveAccount(AccountId::new("live-a").unwrap())
    }

    fn start() -> crate::paper_recovery::QualificationStarted {
        let tail = crate::paper_recovery::TailBinding {
            physical_tail: 0,
            last_sequence: None,
            last_hash: "00".repeat(32),
        };
        crate::paper_recovery::QualificationStarted {
            starting_bankroll: CollateralAmount::from_atomic(100_000_000),
            paper_prefix: tail.clone(),
            source_prefix: tail.clone(),
            live_prefix: tail,
            artifact_blake3: "artifact".to_owned(),
            static_config_hash: "static".to_owned(),
            hot_config_hash: "hot".to_owned(),
            generation: "generation".to_owned(),
            activation_id: "activation".to_owned(),
            ranking_batch_id: 1,
            policy_hash: "policy".to_owned(),
            membership: vec![
                WalletAddress::from_hex("0x1111111111111111111111111111111111111111").unwrap(),
            ],
            membership_proofs_hash: "proof".to_owned(),
            schema_version: 2,
            parser_version: 1,
            financial_semantic_version: 1,
        }
    }

    fn mark(
        sequence: u64,
        cutoff_unix: i64,
        equity: Decimal,
        invalid: Option<&str>,
    ) -> ScannedPaperFrame {
        frame(
            sequence,
            cutoff_unix,
            PaperLogRecord::PortfolioMark(Box::new(crate::paper_recovery::PortfolioMark {
                boundary_receipt: receipt(sequence.saturating_sub(1), 9),
                cutoff_unix,
                source_tail: crate::paper_recovery::TailBinding {
                    physical_tail: 0,
                    last_sequence: None,
                    last_hash: "00".repeat(32),
                },
                financial_prefix_seq: None,
                prices: Vec::new(),
                cash: equity,
                equity,
                invalid: invalid.map(str::to_owned),
            })),
        )
    }

    fn era_with_marks(marks: Vec<ScannedPaperFrame>) -> PaperEra {
        let start = start();
        let start_frame = frame(
            1,
            100,
            PaperLogRecord::QualificationStarted(Box::new(start.clone())),
        );
        let mut frames = vec![start_frame];
        frames.extend(marks);
        PaperEra {
            start: Some((receipt(1, 1), start)),
            frames,
        }
    }

    /// PASS: moving risk-input diagnostics behind the service boundary preserves every display
    /// string previously carried by the strategy error and embedded in durable decision evidence.
    #[test]
    fn risk_input_diagnostic_display_is_stable() {
        let cases = [
            (
                RiskInputsUnavailable::SnapshotSequenceMismatch,
                "financial snapshot sequence does not match the completed paper-log prefix",
            ),
            (
                RiskInputsUnavailable::UnmatchedPrepared,
                "the paper log has an unmatched FinancialPrepared record",
            ),
            (
                RiskInputsUnavailable::PriceMissing,
                "a required position price is missing",
            ),
            (
                RiskInputsUnavailable::PriceStale,
                "a required position price is stale",
            ),
            (
                RiskInputsUnavailable::PriceFuture,
                "a required position price is from the future",
            ),
            (
                RiskInputsUnavailable::PriceConflict,
                "position price evidence conflicts",
            ),
            (
                RiskInputsUnavailable::MarkMissing,
                "the immediately preceding midnight mark is missing",
            ),
            (
                RiskInputsUnavailable::MarkDuplicate,
                "the immediately preceding midnight mark is duplicated",
            ),
            (
                RiskInputsUnavailable::MarkInvalid,
                "the immediately preceding midnight mark is invalid",
            ),
            (
                RiskInputsUnavailable::BaselineNonPositive,
                "the fixed qualification baseline is not positive",
            ),
            (
                RiskInputsUnavailable::Overflow,
                "exact risk arithmetic overflowed",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
        }
    }

    /// PASS: the fixed starting bankroll applies before the first post-Start midnight, after which
    /// the exact immediately preceding mark is mandatory and older marks cannot substitute.
    #[test]
    fn preceding_midnight_mark_is_causal_and_mandatory() {
        let no_marks = era_with_marks(Vec::new());
        assert_eq!(
            preceding_midnight_equity(&no_marks, 99),
            Err(RiskInputsUnavailable::MarkInvalid)
        );
        assert_eq!(preceding_midnight_equity(&no_marks, 86_399).unwrap(), None);
        assert_eq!(
            preceding_midnight_equity(&no_marks, 86_400),
            Err(RiskInputsUnavailable::MarkMissing)
        );
        let marked = era_with_marks(vec![mark(2, 86_400, dec!(98), None)]);
        assert_eq!(
            preceding_midnight_equity(&marked, 86_499).unwrap(),
            Some(dec!(98))
        );
        assert_eq!(
            preceding_midnight_equity(&marked, 172_800),
            Err(RiskInputsUnavailable::MarkMissing)
        );
    }

    /// PASS: duplicate and explicitly invalid marks fail with distinct typed evidence errors.
    #[test]
    fn preceding_midnight_rejects_duplicate_and_invalid_marks() {
        let duplicate = era_with_marks(vec![
            mark(2, 86_400, dec!(98), None),
            mark(3, 86_400, dec!(98), None),
        ]);
        assert_eq!(
            preceding_midnight_equity(&duplicate, 86_500),
            Err(RiskInputsUnavailable::MarkDuplicate)
        );
        let invalid = era_with_marks(vec![mark(2, 86_400, dec!(98), Some("price_stale"))]);
        assert_eq!(
            preceding_midnight_equity(&invalid, 86_500),
            Err(RiskInputsUnavailable::MarkInvalid)
        );

        let total_loss = era_with_marks(vec![mark(2, 86_400, Decimal::ZERO, None)]);
        assert_eq!(
            preceding_midnight_equity(&total_loss, 86_500).unwrap(),
            Some(Decimal::ZERO),
            "a valid 100% loss remains an exact -10,000 bps baseline"
        );
    }

    /// PASS: mark selection uses the latest unique at-or-before sample, including the exact
    /// 120-second age endpoint, and binds the supplied append receipt.
    #[test]
    fn historical_mark_selects_exact_latest_sample() {
        let classified = ClassifiedPricesHistory::Points(vec![
            PricePoint {
                t: 879,
                price: dec!(0.4),
            },
            PricePoint {
                t: 880,
                price: dec!(0.6),
            },
            PricePoint {
                t: 1_001,
                price: dec!(0.9),
            },
        ]);
        assert_eq!(
            historical_mark_price(&classified, 1_000, receipt(7, 7)).unwrap(),
            HistoricalMarkPrice {
                price: Price::new(dec!(0.6)).unwrap(),
                sample_unix: 880,
                receipt: receipt(7, 7),
            }
        );
    }

    /// PASS: stale, future-only, empty, rejected, and same-timestamp disagreement are distinct
    /// typed invalid evidence and never become zero or a carried-forward price.
    #[test]
    fn historical_mark_fails_closed_by_evidence_class() {
        let selected = |points| ClassifiedPricesHistory::Points(points);
        assert_eq!(
            historical_mark_price(
                &selected(vec![PricePoint {
                    t: 879,
                    price: dec!(0.5),
                }]),
                1_000,
                receipt(1, 1),
            ),
            Err(RiskInputsUnavailable::PriceStale)
        );
        assert_eq!(
            historical_mark_price(
                &selected(vec![PricePoint {
                    t: 1_001,
                    price: dec!(0.5),
                }]),
                1_000,
                receipt(1, 1),
            ),
            Err(RiskInputsUnavailable::PriceFuture)
        );
        assert_eq!(
            historical_mark_price(&ClassifiedPricesHistory::Empty, 1_000, receipt(1, 1),),
            Err(RiskInputsUnavailable::PriceMissing)
        );
        assert_eq!(
            historical_mark_price(
                &ClassifiedPricesHistory::Rejected {
                    message: "bad request".to_owned(),
                },
                1_000,
                receipt(1, 1),
            ),
            Err(RiskInputsUnavailable::MarkInvalid)
        );
        assert_eq!(
            historical_mark_price(
                &selected(vec![
                    PricePoint {
                        t: 1_000,
                        price: dec!(0.4),
                    },
                    PricePoint {
                        t: 1_000,
                        price: dec!(0.6),
                    },
                ]),
                1_000,
                receipt(1, 1),
            ),
            Err(RiskInputsUnavailable::PriceConflict)
        );
    }

    /// PASS: completion endpoints are half-open by hour; current-hour samples do not enter either
    /// completed-prior-hour p95 and empty completed hours stay unavailable.
    #[test]
    fn latency_hours_use_final_endpoints_and_completed_prior_hours() {
        let samples = vec![
            (3_600, 100),
            (7_199, 200),
            (7_200, 3_100),
            (10_799, 3_200),
            (10_800, 9_999),
        ];
        let previous = latency_hour(&samples, 3_600).unwrap();
        let latest = latency_hour(&samples, 7_200).unwrap();
        let empty = latency_hour(&samples, 0).unwrap();
        assert_eq!((previous.sample_count, previous.p95_ms), (2, Some(200)));
        assert_eq!((latest.sample_count, latest.p95_ms), (2, Some(3_200)));
        assert_eq!((empty.sample_count, empty.p95_ms), (0, None));
    }

    /// PASS: replay folds each owner/cause independently and any active cause blocks entries.
    #[test]
    fn halt_replay_keeps_independent_owner_causes() {
        let era = PaperEra {
            start: None,
            frames: vec![
                halt(
                    1,
                    RiskHaltOwner::Paper,
                    RiskHaltCause::AbsoluteLoss,
                    HaltState::Engaged,
                ),
                halt(2, account(), RiskHaltCause::CopyLatency, HaltState::Engaged),
                halt(
                    3,
                    account(),
                    RiskHaltCause::CopyLatency,
                    HaltState::Released,
                ),
            ],
        };
        let active = active_risk_halts(&era);
        assert!(!active.is_empty());
        assert!(active.contains(&(RiskHaltOwner::Paper, RiskHaltCause::AbsoluteLoss)));
        assert!(!active.contains(&(account(), RiskHaltCause::CopyLatency)));
    }

    /// PASS: only the latest still-active absolute/latency engagement can be manually released.
    #[test]
    fn audited_release_rejects_stale_consumed_and_other_cause_hashes() {
        let stale = halt(
            1,
            RiskHaltOwner::Paper,
            RiskHaltCause::CopyLatency,
            HaltState::Engaged,
        );
        let released = halt(
            2,
            RiskHaltOwner::Paper,
            RiskHaltCause::CopyLatency,
            HaltState::Released,
        );
        let absolute = halt(
            3,
            RiskHaltOwner::Paper,
            RiskHaltCause::AbsoluteLoss,
            HaltState::Engaged,
        );
        let intraday = halt(
            4,
            account(),
            RiskHaltCause::IntradayDrawdown,
            HaltState::Engaged,
        );
        let newer_absolute = halt(
            5,
            RiskHaltOwner::Paper,
            RiskHaltCause::AbsoluteLoss,
            HaltState::Engaged,
        );
        let starved_latency = halt(6, account(), RiskHaltCause::CopyLatency, HaltState::Engaged);
        let era = PaperEra {
            start: None,
            frames: vec![
                stale.clone(),
                released,
                absolute.clone(),
                intraday.clone(),
                newer_absolute.clone(),
                starved_latency.clone(),
            ],
        };
        let active = active_risk_halts(&era);
        assert!(
            audited_halt_release(
                &era,
                &active,
                stale.receipt.this_hash.to_hex().as_str(),
                None,
            )
            .is_none()
        );
        assert!(
            audited_halt_release(
                &era,
                &active,
                intraday.receipt.this_hash.to_hex().as_str(),
                None,
            )
            .is_none()
        );
        assert!(
            audited_halt_release(
                &era,
                &active,
                absolute.receipt.this_hash.to_hex().as_str(),
                None,
            )
            .is_none()
        );
        assert_eq!(
            audited_halt_release(
                &era,
                &active,
                newer_absolute.receipt.this_hash.to_hex().as_str(),
                Some(2_500),
            )
            .unwrap()
            .cause,
            RiskHaltCause::AbsoluteLoss
        );
        assert!(
            audited_halt_release(
                &era,
                &active,
                starved_latency.receipt.this_hash.to_hex().as_str(),
                Some(2_500),
            )
            .is_none()
        );
        assert_eq!(
            audited_halt_release(
                &era,
                &active,
                starved_latency.receipt.this_hash.to_hex().as_str(),
                None,
            )
            .unwrap()
            .cause,
            RiskHaltCause::CopyLatency
        );
    }
}
