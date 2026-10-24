//! Replayable paper/live risk-input composition and daily-mark validation (#545).
//!
//! These reducers combine the active financial snapshot with causal price, mark, PnL, exposure,
//! and latency evidence. Missing, stale, conflicting, or incoherent evidence fails closed with a
//! service-private diagnostic before strategy evaluation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use pe_core_types::{EventSeq, MarketId, OutcomeId, Price, ReceivedAt};
use pe_event_log::{AppendReceipt, EventEnvelope, LogTailBinding, Reader, RetentionAuthority};
use pe_paper_state::FinancialSnapshot;
use pe_risk_engine::{
    EquityInputs, PnlWindow, RiskHaltCause, RiskMathError, RiskSnapshot, current_equity,
    latency_switch, nearest_rank_p95, pnl_bps,
};
use pe_source_polymarket_public::{
    ClassifiedPricesHistory, ClobPricesHistoryClient, FixtureFetcher, GAMMA_BATCH_LIMIT_PARAM,
    GammaConditionRequest, GammaMarket, GammaMarketsClient,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[cfg(test)]
use crate::paper_recovery::active_risk_halts;
use crate::paper_recovery::{
    FinancialPayload, FinancialResult, HaltState, PaperEra, PaperLogFrame, PaperLogRecord,
    RiskHaltOwner, ScannedPaperFrame, oldest_unmatched_prepared,
};

const SECONDS_PER_HOUR: i64 = 3_600;
const SECONDS_PER_DAY: i64 = 86_400;
pub(crate) const MAX_HISTORICAL_MARK_AGE_SECS: i64 = 120;
/// Canonical `closed_mark_lookback_secs` in `docs/_GLOSSARY.md`.
pub(crate) const CLOSED_MARK_LOOKBACK_SECS: i64 = 1_123_200;

/// Apply the strategy-wide halt overlay reconstructed from the durable paper prefix. Halt owners
/// are deliberately ignored here: every active cause gates every new strategy entry.
pub(crate) fn apply_global_risk_halts(
    active: &HashSet<(RiskHaltOwner, RiskHaltCause)>,
    snapshot: &mut RiskSnapshot,
    copy_latency_enabled: bool,
) {
    use pe_risk_engine::{DrawdownPolicy, INTRADAY_STOP_BPS, ROLLING_7D_STOP_BPS};

    for (_, cause) in active {
        match cause {
            RiskHaltCause::AbsoluteLoss => {
                snapshot.absolute_pnl_bps.0 = snapshot
                    .absolute_pnl_bps
                    .0
                    .min(snapshot.drawdown_policy.absolute_loss_bps());
            }
            RiskHaltCause::IntradayDrawdown
                if snapshot.drawdown_policy == DrawdownPolicy::Stops =>
            {
                snapshot.intraday_pnl_bps.0 = snapshot.intraday_pnl_bps.0.min(INTRADAY_STOP_BPS);
            }
            RiskHaltCause::Rolling7dDrawdown
                if snapshot.drawdown_policy == DrawdownPolicy::Stops =>
            {
                snapshot.rolling_7d_pnl_bps.0 =
                    snapshot.rolling_7d_pnl_bps.0.min(ROLLING_7D_STOP_BPS);
            }
            RiskHaltCause::CopyLatency if copy_latency_enabled => {
                snapshot.copy_latency_kill_switch_active = true;
            }
            RiskHaltCause::CopyLatency
            | RiskHaltCause::IntradayDrawdown
            | RiskHaltCause::Rolling7dDrawdown => {}
        }
    }
}

/// Service-owned reason that replayable evidence could not produce a risk snapshot; the strategy
/// crate sees only the unit `RiskInputsUnavailable` decline.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, thiserror::Error,
)]
#[serde(rename_all = "snake_case")]
pub enum RiskInputsUnavailable {
    #[error("source frame has been erased by retention")]
    Erased,
    #[error("source evidence verification cancelled")]
    Cancelled,
    #[error("source retention epoch changed during verification")]
    RetentionChanged,
    #[error("financial snapshot sequence does not match the completed paper-log prefix")]
    SnapshotSequenceMismatch,
    #[error("the paper log has an unmatched FinancialPrepared record")]
    UnmatchedPrepared,
    #[error(
        "a post-QualificationStarted live latency release is missing its journal-tail checkpoint"
    )]
    LiveLatencyReleaseCheckpointMissing,
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
    select_historical_mark_price(
        classified,
        cutoff_unix,
        receipt,
        MAX_HISTORICAL_MARK_AGE_SECS,
    )
}

/// Paper-only rule: recorded closure at or before the cutoff permits the longer history window.
pub(crate) fn closed_historical_mark_price(
    classified: &ClassifiedPricesHistory,
    condition_id: &str,
    closure: &GammaMarket,
    cutoff_unix: i64,
    receipt: AppendReceipt,
) -> Result<HistoricalMarkPrice, RiskInputsUnavailable> {
    if closure.condition_id != condition_id
        || !closure.closed
        || closure
            .closed_time_unix
            .is_none_or(|closed| closed > cutoff_unix)
    {
        return Err(RiskInputsUnavailable::MarkInvalid);
    }
    select_historical_mark_price(classified, cutoff_unix, receipt, CLOSED_MARK_LOOKBACK_SECS)
}

/// Replay the paper rule selected by the durable closure receipt, rebuilding both request bounds
/// from the held identity and cutoff. Receipt/source authentication belongs to the caller.
pub(crate) async fn replay_paper_mark_price(
    price: &crate::paper_recovery::PaperMarkPrice,
    token_id: &str,
    cutoff_unix: i64,
    history_payload: &[u8],
    closure_payload: Option<&[u8]>,
) -> Result<HistoricalMarkPrice, BoundaryMarkError> {
    let receipt = price.receipt.ok_or(BoundaryMarkError::Invalid(
        RiskInputsUnavailable::PriceMissing,
    ))?;
    let lookback_secs = if price.closure_receipt.is_some() {
        CLOSED_MARK_LOOKBACK_SECS
    } else {
        MAX_HISTORICAL_MARK_AGE_SECS
    };
    let start_unix = cutoff_unix
        .checked_sub(lookback_secs)
        .ok_or(BoundaryMarkError::Invalid(RiskInputsUnavailable::Overflow))?;
    let base_url = "https://offline.invalid";
    let url = format!(
        "{base_url}/prices-history?market={token_id}&startTs={start_unix}&endTs={cutoff_unix}&fidelity=1"
    );
    let replayed = ClobPricesHistoryClient::new(
        base_url.to_owned(),
        FixtureFetcher::new(HashMap::from([(url, history_payload.to_vec())])),
    )
    .with_fidelity_minutes(1)
    .fetch_prices_history_classified(token_id, start_unix, cutoff_unix)
    .await
    .map_err(|error| BoundaryMarkError::Classification(error.to_string()))?;
    if replayed.body != history_payload {
        return Err(BoundaryMarkError::Invalid(
            RiskInputsUnavailable::MarkInvalid,
        ));
    }
    // Replay applies acquisition's selector unchanged: a repeated timestamp matters only at the
    // selected sample, where any disagreement is a conflict (as live mark replay does).
    if price.closure_receipt.is_none() {
        return historical_mark_price(&replayed.outcome, cutoff_unix, receipt)
            .map_err(BoundaryMarkError::Invalid);
    }
    let payload = closure_payload.ok_or(BoundaryMarkError::Invalid(
        RiskInputsUnavailable::MarkInvalid,
    ))?;
    let url = format!(
        "{base_url}/markets?condition_ids={}&closed=true&limit={GAMMA_BATCH_LIMIT_PARAM}",
        price.market_id,
    );
    let request = GammaConditionRequest::parse(&url).ok_or(BoundaryMarkError::Invalid(
        RiskInputsUnavailable::MarkInvalid,
    ))?;
    let fetched = GammaMarketsClient::new(
        request.base_url,
        FixtureFetcher::new(HashMap::from([(url, payload.to_vec())])),
    )
    .fetch_markets_with_pages(&request.condition_ids, request.filter)
    .await
    .map_err(|error| BoundaryMarkError::Classification(error.to_string()))?;
    if fetched
        .conflicting_closure_condition_ids
        .contains(&price.market_id)
    {
        return Err(BoundaryMarkError::Invalid(
            RiskInputsUnavailable::MarkInvalid,
        ));
    }
    let closure =
        fetched
            .markets
            .markets
            .get(&price.market_id)
            .ok_or(BoundaryMarkError::Invalid(
                RiskInputsUnavailable::MarkInvalid,
            ))?;
    closed_historical_mark_price(
        &replayed.outcome,
        &price.market_id,
        closure,
        cutoff_unix,
        receipt,
    )
    .map_err(BoundaryMarkError::Invalid)
}

fn select_historical_mark_price(
    classified: &ClassifiedPricesHistory,
    cutoff_unix: i64,
    receipt: AppendReceipt,
    max_age_secs: i64,
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
    if age > max_age_secs {
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

/// Match an incident-row hash only to its own currently active absolute-loss or latency cause.
/// The caller resolves owner-local latency evidence only after this match identifies the owner.
/// Stale, already-consumed, mismatched-cause, and unknown hashes produce `None`.
#[must_use]
pub fn audited_halt_release(
    era: &PaperEra,
    active: &HashSet<(RiskHaltOwner, RiskHaltCause)>,
    release_hash: &str,
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
pub fn completed_prepared_before_boundary(
    era: &PaperEra,
    source_log_path: &Path,
    cutoff_unix: i64,
    boundary_receipt: AppendReceipt,
) -> Result<HashSet<EventSeq>, RiskInputsUnavailable> {
    completed_prepared_before_boundary_with(era, cutoff_unix, boundary_receipt, |receipts| {
        source_receipt_index(source_log_path, receipts.into_iter())
    })
}

/// Runtime variant using the installed source receipt index for the same causal cutoff.
pub fn completed_prepared_before_boundary_indexed(
    era: &PaperEra,
    source_receipts: &SourceReceiptIndex,
    cutoff_unix: i64,
    boundary_receipt: AppendReceipt,
) -> Result<HashSet<EventSeq>, RiskInputsUnavailable> {
    completed_prepared_before_boundary_with(era, cutoff_unix, boundary_receipt, |receipts| {
        let mut received = BTreeMap::new();
        for receipt in receipts {
            let Some((indexed, millis)) = source_receipts.receipt_at(receipt.sequence)? else {
                return Err(RiskInputsUnavailable::PriceMissing);
            };
            if indexed != receipt {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
            received.insert(receipt.sequence, (indexed, millis));
        }
        Ok(received)
    })
}

fn completed_prepared_before_boundary_with(
    era: &PaperEra,
    cutoff_unix: i64,
    boundary_receipt: AppendReceipt,
    resolve: impl FnOnce(
        Vec<AppendReceipt>,
    ) -> Result<BTreeMap<EventSeq, (AppendReceipt, i64)>, RiskInputsUnavailable>,
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
    let mut causal = Vec::new();
    for frame in &era.frames {
        let PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
            prepared_receipt, ..
        }) = &frame.frame
        else {
            continue;
        };
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
        if source_receipt.sequence <= boundary_receipt.sequence {
            causal.push((prepared_receipt.sequence, source_receipt));
        }
    }
    let received_millis = resolve(causal.iter().map(|(_, receipt)| *receipt).collect())?;
    causal
        .into_iter()
        .filter_map(|(prepared_sequence, receipt)| {
            match source_receipt_received_millis(&received_millis, receipt) {
                Ok(received) if received < cutoff_ms => Some(Ok(prepared_sequence)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect()
}

/// Exposure-only intermediate for one paper proposal.
///
/// This deliberately cannot be passed to `evaluate_risk`; only this module can turn it into the
/// complete [`RiskSnapshot`] that includes causal PnL and latency state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PaperExposureBase {
    pub(crate) leader_exposure_bps: pe_core_types::BasisPoints,
    pub(crate) market_exposure_bps: pe_core_types::BasisPoints,
    pub(crate) family_exposure_bps: pe_core_types::BasisPoints,
    pub(crate) total_copy_exposure_bps: pe_core_types::BasisPoints,
    pub(crate) proposed_trade_bps: pe_core_types::BasisPoints,
    pub(crate) per_trade_cap_bps: i32,
}

/// The positions risk values and prices: net quantity per row, excluding zero-net rows (a
/// market closed by a resolution stays projected with zero shares). Runtime pricing and replay
/// select the same set through this one rule.
pub(crate) fn open_positions(
    positions: &[pe_paper_state::PaperPositionRow],
) -> Result<
    Vec<(
        &pe_paper_state::PaperPositionRow,
        pe_core_types::ShareAmount,
    )>,
    RiskInputsUnavailable,
> {
    let mut open = Vec::with_capacity(positions.len());
    for position in positions {
        let quantity = position
            .long
            .checked_sub(position.short)
            .map_err(|_| RiskInputsUnavailable::Overflow)?;
        if quantity != pe_core_types::ShareAmount::ZERO {
            open.push((position, quantity));
        }
    }
    Ok(open)
}

/// Compose paper risk from a source prefix that the caller has already replayed and verified.
/// Runtime supplies the maintained source receipt index; offline qualification keeps its own
/// sealed-prefix view so every Prepared reuses the correct historical boundary.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_paper_risk_snapshot_from_source_receipts<F>(
    base: &PaperExposureBase,
    snapshot: &FinancialSnapshot,
    era: &PaperEra,
    current_prices: &HashMap<(MarketId, OutcomeId), Price>,
    source_receipt_received_millis: F,
    now_unix: i64,
    latency_was_active: bool,
    financial_semantic_version: u32,
) -> Result<RiskSnapshot, RiskInputsUnavailable>
where
    F: Fn(AppendReceipt) -> Result<i64, RiskInputsUnavailable>,
{
    let copy_latency_kill_switch_active = if financial_semantic_version < 2 {
        let latency = paper_latency_samples_from_source_receipts(
            era,
            now_unix,
            &source_receipt_received_millis,
        )?;
        latency.switch_active(latency_was_active)
    } else {
        false
    };
    compose_paper_risk_snapshot(
        base,
        snapshot,
        era,
        current_prices,
        now_unix,
        copy_latency_kill_switch_active,
    )
}

fn compose_paper_risk_snapshot(
    base: &PaperExposureBase,
    snapshot: &FinancialSnapshot,
    era: &PaperEra,
    current_prices: &HashMap<(MarketId, OutcomeId), Price>,
    now_unix: i64,
    copy_latency_kill_switch_active: bool,
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
    for (position, quantity) in open_positions(&snapshot.positions)? {
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
    Ok(RiskSnapshot {
        leader_exposure_bps: base.leader_exposure_bps,
        market_exposure_bps: base.market_exposure_bps,
        family_exposure_bps: base.family_exposure_bps,
        total_copy_exposure_bps: base.total_copy_exposure_bps,
        intraday_pnl_bps: pnl.intraday,
        rolling_7d_pnl_bps: pnl.rolling_7d,
        absolute_pnl_bps: pnl.absolute,
        copy_latency_kill_switch_active,
        proposed_trade_bps: base.proposed_trade_bps,
        per_trade_cap_bps: base.per_trade_cap_bps,
        drawdown_policy: pe_risk_engine::DrawdownPolicy::Stops,
        concentration_caps: None,
    })
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

/// Audited account-local tail and complete journal-prefix identity used by a manual release scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LiveLatencyJournalTailEvidence {
    pub last_sequence: Option<EventSeq>,
    pub last_hash: String,
    pub scanned_prefix_last_sequence: Option<EventSeq>,
    pub scanned_prefix_last_hash: String,
}

impl From<pe_execution_core::live_journal::LiveJournalTail> for LiveLatencyJournalTailEvidence {
    fn from(tail: pe_execution_core::live_journal::LiveJournalTail) -> Self {
        Self {
            last_sequence: tail.last_sequence,
            last_hash: tail.last_hash.to_hex().to_string(),
            scanned_prefix_last_sequence: tail.scanned_prefix_last_sequence,
            scanned_prefix_last_hash: tail.scanned_prefix_last_hash.to_hex().to_string(),
        }
    }
}

/// Derive paper latency from each Fill Final envelope's `received_at` minus its Prepared
/// observation timestamp. Completion belongs to the hour containing the Final endpoint.
pub(crate) fn paper_latency_samples(
    era: &PaperEra,
    source_receipts: &SourceReceiptIndex,
    now_unix: i64,
) -> Result<LatencySamples, RiskInputsUnavailable> {
    paper_latency_samples_from_source_receipts(era, now_unix, &|receipt| {
        source_receipts.received_millis(receipt)
    })
}

fn paper_latency_samples_from_source_receipts<F>(
    era: &PaperEra,
    now_unix: i64,
    source_receipt_received_millis: &F,
) -> Result<LatencySamples, RiskInputsUnavailable>
where
    F: Fn(AppendReceipt) -> Result<i64, RiskInputsUnavailable>,
{
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
    let all_samples = paper_latency_endpoint_samples(era, source_receipt_received_millis)?;
    Ok(LatencySamples {
        previous: latency_hour(&all_samples, previous_start)?,
        latest: latency_hour(&all_samples, latest_start)?,
    })
}

/// Derive the last two completed owner-local live latency hours from successful transports.
pub(crate) fn live_latency_samples(
    events: &[pe_execution_core::LiveJournalEvent],
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
    let all_samples = live_latency_endpoint_samples(events)?;
    Ok(LatencySamples {
        previous: latency_hour(&all_samples, previous_start)?,
        latest: latency_hour(&all_samples, latest_start)?,
    })
}

fn live_latency_endpoint_samples(
    events: &[pe_execution_core::LiveJournalEvent],
) -> Result<Vec<(i64, u64)>, RiskInputsUnavailable> {
    let mut samples = Vec::new();
    for event in events {
        let pe_execution_core::LiveJournalPayload::OrderPosted(posted) = &event.payload else {
            continue;
        };
        let pe_core_types::RawHttpAttempt::Response(response) = &posted.evidence else {
            continue;
        };
        let latency_ms = (response.received_at - response.observed_at)
            .whole_milliseconds()
            .try_into()
            .map_err(|_| RiskInputsUnavailable::Overflow)?;
        samples.push((response.received_at.unix_timestamp(), latency_ms));
    }
    Ok(samples)
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
    source_receipt_received_millis: &impl Fn(AppendReceipt) -> Result<i64, RiskInputsUnavailable>,
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
        let observed_ms = source_receipt_received_millis(observation.source_receipt)?;
        let latency_ms = final_ms
            .checked_sub(observed_ms)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(RiskInputsUnavailable::Overflow)?;
        samples.push((final_unix, latency_ms));
    }
    Ok(samples)
}

#[cfg(test)]
fn paper_fill_source_receipts(era: &PaperEra) -> Result<Vec<AppendReceipt>, RiskInputsUnavailable> {
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
    era.frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt,
                result: FinancialResult::Fill { .. },
            }) => Some(prepared_receipt),
            _ => None,
        })
        .map(|prepared_receipt| {
            let Some((known_receipt, observation)) = prepared.get(&prepared_receipt.sequence)
            else {
                return Err(RiskInputsUnavailable::UnmatchedPrepared);
            };
            if *known_receipt != *prepared_receipt {
                return Err(RiskInputsUnavailable::UnmatchedPrepared);
            }
            Ok(observation.source_receipt)
        })
        .collect()
}

/// Constant-size commitment to the canonical receipt records of sequences `[0, count)` (#745).
///
/// The retention pre-commit audit rebuilds it from the receipt sidecar and compares, so damage to
/// any retired record blocks the next commit even when the record's own checksum was recomputed.
#[derive(Clone, Default)]
pub(crate) struct ReceiptCommitment {
    count: u64,
    hasher: blake3::Hasher,
}

impl ReceiptCommitment {
    /// Absorb the next record; its sequence must equal the current count.
    pub(crate) fn push(
        &mut self,
        frame: &SourceFrameMetadata,
    ) -> Result<(), RiskInputsUnavailable> {
        if frame.receipt.sequence.0 != self.count {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let record = pe_event_log::ReceiptRecord {
            receipt: frame.receipt,
            received_millis: frame.received_millis,
            byte_offset: frame.byte_offset,
        };
        self.hasher.update(&record.encode());
        self.count = self
            .count
            .checked_add(1)
            .ok_or(RiskInputsUnavailable::Overflow)?;
        Ok(())
    }

    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    /// Whether both commitments cover the same number of records with identical bytes.
    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.count == other.count && self.hasher.finalize() == other.hasher.finalize()
    }
}

/// Receipts below the in-memory window: every sequence below the committed retention boundary
/// (#745).
///
/// Memory keeps only this constant-size summary. The records stay in the dense receipt sidecar,
/// which is never rewritten below the boundary once retention exists, and are read from it on
/// demand through `reader`.
#[derive(Clone, Default)]
struct RetiredReceipts {
    commitment: ReceiptCommitment,
    /// Newest receive time among the retired receipts.
    max_received: Option<i64>,
    /// The two highest retired receipts, so tail and predecessor reads stay in memory.
    last: Option<SourceFrameMetadata>,
    before_last: Option<SourceFrameMetadata>,
    reader: Option<Arc<std::fs::File>>,
}

impl RetiredReceipts {
    fn count(&self) -> u64 {
        self.commitment.count()
    }

    fn retire(&mut self, frame: SourceFrameMetadata) -> Result<(), RiskInputsUnavailable> {
        self.commitment.push(&frame)?;
        self.max_received = self.max_received.max(Some(frame.received_millis));
        self.before_last = self.last.replace(frame);
        Ok(())
    }

    fn cached(&self, sequence: u64) -> Option<&SourceFrameMetadata> {
        [self.last.as_ref(), self.before_last.as_ref()]
            .into_iter()
            .flatten()
            .find(|frame| frame.receipt.sequence.0 == sequence)
    }
}

/// What a retention preparation's scan of every receipt selects as the next boundary (#745).
pub(crate) enum BoundaryCandidate {
    /// There are no receipts.
    Empty,
    /// The selected receipt is below `base`, among the retired receipts.
    Retired {
        base: u64,
    },
    Frame(SourceFrameMetadata),
}

/// Inputs for the retention pre-commit audit (#745).
pub(crate) struct RetentionAudit {
    /// Commitment to the retired receipts `[0, commitment.count())`.
    pub(crate) commitment: ReceiptCommitment,
    /// In-memory receipts from the window base up to the audited boundary.
    pub(crate) window: Vec<SourceFrameMetadata>,
}

/// Retired receipts a retention authority depends on, read from the sidecar before the write
/// guard, with the window base and epoch they were read against (#745).
struct RetentionDependencies {
    base: u64,
    epoch: u64,
    reader: Option<Arc<std::fs::File>>,
    resolved: BTreeMap<u64, SourceFrameMetadata>,
}

/// Where one receipt is: in memory, in the sidecar below the window, or not yet appended.
enum ReceiptSlot {
    Memory(SourceFrameMetadata),
    Sidecar(Arc<std::fs::File>),
    Absent,
}

impl ReceiptSlot {
    /// Resolve outside the index lock. A sidecar read failure is a missing receipt (#745).
    fn resolve(self, sequence: u64) -> Result<Option<SourceFrameMetadata>, RiskInputsUnavailable> {
        match self {
            Self::Memory(frame) => Ok(Some(frame)),
            Self::Sidecar(file) => read_retired_receipt(&file, sequence).map(Some),
            Self::Absent => Ok(None),
        }
    }
}

/// Positional, checksum-verified read of retired receipt `sequence` from the dense sidecar.
fn read_retired_receipt(
    file: &std::fs::File,
    sequence: u64,
) -> Result<SourceFrameMetadata, RiskInputsUnavailable> {
    use std::os::unix::fs::FileExt as _;
    let position = sequence
        .checked_mul(pe_event_log::RECEIPT_RECORD_LEN)
        .ok_or(RiskInputsUnavailable::Overflow)?;
    let mut bytes = [0; 80];
    file.read_exact_at(&mut bytes, position)
        .map_err(|_| RiskInputsUnavailable::PriceMissing)?;
    let record = pe_event_log::ReceiptRecord::read(&mut bytes.as_slice(), EventSeq(sequence))
        .map_err(|_| RiskInputsUnavailable::PriceMissing)?;
    Ok(SourceFrameMetadata {
        receipt: record.receipt,
        received_millis: record.received_millis,
        byte_offset: record.byte_offset,
    })
}

/// Process-wide verified source receipt projection.
///
/// Boot replays the source log once to retain each receipt, receive millisecond, and frame byte
/// offset. The sole synchronized source-log coordinator records each later append before
/// acknowledging its receipt, so runtime paper risk and exact envelope retrieval
/// never need to replay the growing source prefix.
///
/// Memory holds the receipts from the committed retention boundary to the tail, plus a
/// constant-size summary of the retired ones below it. Retired receipts are read from the dense
/// receipt sidecar on demand, never while the index lock is held (#745).
#[derive(Default)]
struct SourceReceiptIndexState {
    /// Sequences from `retired.count()` to the tail: `frames[i]` is `retired.count() + i`.
    frames: Vec<SourceFrameMetadata>,
    retired: RetiredReceipts,
    retention: Option<Arc<RetentionAuthority>>,
    next_byte_offset: Option<u64>,
    verified_feed_frontiers:
        HashMap<pe_core_types::WalletAddress, (u64, crate::frame_admission::FeedHistoryFrontier)>,
    #[cfg(feature = "scenario")]
    read_verifications: HashMap<(EventSeq, blake3::Hash), usize>,
    #[cfg(feature = "scenario")]
    frame_verifications: HashMap<(EventSeq, blake3::Hash), usize>,
    #[cfg(feature = "scenario")]
    binding_verification_categories: HashMap<(EventSeq, blake3::Hash), [usize; 3]>,
}

impl SourceReceiptIndexState {
    /// Number of receipts from sequence zero to the tail.
    fn len(&self) -> Result<u64, RiskInputsUnavailable> {
        u64::try_from(self.frames.len())
            .ok()
            .and_then(|len| len.checked_add(self.retired.count()))
            .ok_or(RiskInputsUnavailable::Overflow)
    }

    fn last(&self) -> Option<&SourceFrameMetadata> {
        self.frames.last().or(self.retired.last.as_ref())
    }

    /// A receipt held in memory: in the window or among the two highest retired receipts.
    fn memory(&self, sequence: u64) -> Option<&SourceFrameMetadata> {
        match sequence.checked_sub(self.retired.count()) {
            Some(index) => usize::try_from(index)
                .ok()
                .and_then(|index| self.frames.get(index)),
            None => self.retired.cached(sequence),
        }
    }

    /// Locate one receipt under the lock; resolve a sidecar slot after releasing it.
    fn slot(&self, sequence: u64) -> Result<ReceiptSlot, RiskInputsUnavailable> {
        if let Some(frame) = self.memory(sequence) {
            return Ok(ReceiptSlot::Memory(frame.clone()));
        }
        if sequence >= self.retired.count() {
            return Ok(ReceiptSlot::Absent);
        }
        self.retired
            .reader
            .clone()
            .map(ReceiptSlot::Sidecar)
            .ok_or(RiskInputsUnavailable::PriceMissing)
    }
}

pub(crate) struct RetentionIndexInstall<'a> {
    state: std::sync::RwLockWriteGuard<'a, SourceReceiptIndexState>,
    authority: Arc<RetentionAuthority>,
    /// The window from the new boundary and the extended retired summary, built while validating
    /// so the switch itself cannot fail once the authority is visible.
    window: Option<(Vec<SourceFrameMetadata>, RetiredReceipts)>,
}

impl RetentionIndexInstall<'_> {
    pub(crate) fn install_retention(mut self) {
        self.state.verified_feed_frontiers.clear();
        if let Some((frames, retired)) = self.window.take() {
            self.state.frames = frames;
            self.state.retired = retired;
        }
        self.state.retention = Some(self.authority);
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SourceFrameMetadata {
    pub(crate) receipt: AppendReceipt,
    pub(crate) received_millis: i64,
    pub(crate) byte_offset: Option<u64>,
}

#[derive(Clone, Default)]
pub struct SourceReceiptIndex {
    state: Arc<RwLock<SourceReceiptIndexState>>,
    source_log_path: Option<Arc<PathBuf>>,
}

/// Incomplete externally driven source-receipt projection (#572).
///
/// The staging value exposes no reads and becomes usable only after [`Self::complete`] binds its
/// observed frames to a scanner-verified physical log tail.
pub(crate) struct SourceReceiptIndexStaging {
    canonical_source_log_path: PathBuf,
    /// Sequences from the committed retention boundary (or zero) onward.
    frames: Vec<SourceFrameMetadata>,
    /// Receipts below the boundary, folded into the constant-size summary as they stream in.
    retired: RetiredReceipts,
    /// Pins below the boundary and their neighbors, kept only to verify the pins at boot.
    pinned: BTreeMap<u64, SourceFrameMetadata>,
    /// Ascending sequences still to be kept in `pinned`.
    pinned_wanted: std::collections::VecDeque<u64>,
    retention: Option<Arc<RetentionAuthority>>,
}

impl SourceReceiptIndexStaging {
    fn new(canonical_source_log_path: PathBuf, retention: Option<Arc<RetentionAuthority>>) -> Self {
        let mut wanted = std::collections::BTreeSet::new();
        if let Some(authority) = &retention {
            for pin in &authority.pins {
                let sequence = pin.sequence.0;
                wanted.extend(sequence.checked_sub(1));
                wanted.insert(sequence);
                wanted.extend(
                    sequence
                        .checked_add(1)
                        .filter(|next| *next < authority.boundary.sequence.0),
                );
            }
        }
        Self {
            canonical_source_log_path,
            frames: Vec::new(),
            retired: RetiredReceipts::default(),
            pinned: BTreeMap::new(),
            pinned_wanted: wanted.into_iter().collect(),
            retention,
        }
    }

    pub(crate) fn retention(&self) -> Option<&RetentionAuthority> {
        self.retention.as_deref()
    }

    fn len(&self) -> Result<u64, RiskInputsUnavailable> {
        u64::try_from(self.frames.len())
            .ok()
            .and_then(|len| len.checked_add(self.retired.count()))
            .ok_or(RiskInputsUnavailable::Overflow)
    }

    fn last(&self) -> Option<&SourceFrameMetadata> {
        self.frames.last().or(self.retired.last.as_ref())
    }

    /// Append the next receipt, already checked to carry sequence `self.len()`.
    fn push(&mut self, frame: SourceFrameMetadata) -> Result<(), RiskInputsUnavailable> {
        let sequence = frame.receipt.sequence.0;
        let boundary = self
            .retention
            .as_ref()
            .map_or(0, |authority| authority.boundary.sequence.0);
        if sequence >= boundary {
            self.frames.push(frame);
            return Ok(());
        }
        while self
            .pinned_wanted
            .front()
            .is_some_and(|wanted| *wanted < sequence)
        {
            self.pinned_wanted.pop_front();
        }
        if self.pinned_wanted.front() == Some(&sequence) {
            self.pinned_wanted.pop_front();
            self.pinned.insert(sequence, frame.clone());
        }
        self.retired.retire(frame)
    }

    pub(crate) fn frame_metadata(&self, sequence: EventSeq) -> Option<&SourceFrameMetadata> {
        match sequence.0.checked_sub(self.retired.count()) {
            Some(index) => usize::try_from(index)
                .ok()
                .and_then(|index| self.frames.get(index)),
            None => self
                .retired
                .cached(sequence.0)
                .or_else(|| self.pinned.get(&sequence.0)),
        }
    }

    /// Exact metadata for boot verification: absent past the tail, refused for a retired receipt
    /// that boot did not keep.
    fn known(&self, sequence: u64) -> Result<Option<SourceFrameMetadata>, RiskInputsUnavailable> {
        if let Some(frame) = self.frame_metadata(EventSeq(sequence)) {
            return Ok(Some(frame.clone()));
        }
        if sequence >= self.len()? {
            return Ok(None);
        }
        Err(RiskInputsUnavailable::PriceConflict)
    }

    pub(crate) fn validate_retention(&self) -> Result<(), RiskInputsUnavailable> {
        SourceReceiptIndex::validate_retention(
            &|sequence| self.known(sequence),
            self.retention.as_deref(),
        )
    }

    pub(crate) fn restore_record(
        &mut self,
        frame: SourceFrameMetadata,
        binding: &LogTailBinding,
    ) -> Result<(), RiskInputsUnavailable> {
        let sequence = self.len()?;
        if frame.receipt.sequence.0 != sequence
            || frame.byte_offset.is_none_or(|offset| {
                offset < pe_event_log::HEADER_LEN
                    || offset >= binding.physical_tail
                    || self.last().is_some_and(|previous| {
                        previous
                            .byte_offset
                            .is_none_or(|previous| offset <= previous)
                    })
            })
            || (sequence == 0 && frame.byte_offset != Some(pe_event_log::HEADER_LEN))
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        self.push(frame)
    }

    /// Bind restored receipts to `binding`. Every receipt arrived through
    /// [`Self::restore_record`] against this same binding, which already checked its sequence and
    /// offset, so only the tail binding remains.
    pub(crate) fn validate_checkpoint(
        &self,
        binding: &LogTailBinding,
    ) -> Result<(), RiskInputsUnavailable> {
        SourceReceiptIndex::checkpoint_tail_valid(
            &self.canonical_source_log_path,
            self.last(),
            binding,
        )?;
        Ok(())
    }

    /// Observe one verified source frame while constructing an external projection (#572).
    pub(crate) fn observe(
        &mut self,
        byte_offset: u64,
        envelope: &EventEnvelope,
    ) -> Result<(), RiskInputsUnavailable> {
        let receipt = AppendReceipt {
            sequence: envelope.seq,
            this_hash: envelope.this_hash,
        };
        let received_millis = received_at_millis(&envelope.received_at)?;
        if envelope.seq != EventSeq(self.len()?) {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        self.push(SourceFrameMetadata {
            receipt,
            received_millis,
            byte_offset: Some(byte_offset),
        })
    }

    /// Complete an external projection only when its path and logical tail match exactly (#572).
    pub(crate) fn complete(
        self,
        binding: &LogTailBinding,
    ) -> Result<SourceReceiptIndex, RiskInputsUnavailable> {
        let indexed_last_sequence = self.last().map(|frame| frame.receipt.sequence);
        let indexed_last_hash = self
            .last()
            .map_or(blake3::Hash::from_bytes([0; 32]), |frame| {
                frame.receipt.this_hash
            });
        if binding.path != self.canonical_source_log_path
            || binding.last_sequence != indexed_last_sequence
            || binding.last_hash != indexed_last_hash
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        self.validate_retention()?;
        Ok(self.complete_at(binding.physical_tail))
    }

    fn complete_at(self, physical_tail: u64) -> SourceReceiptIndex {
        let mut retired = self.retired;
        if retired.count() > 0 {
            // Boot just read these records from the sidecar. Without it, retired reads fail closed.
            retired.reader = std::fs::File::open(crate::source_checkpoint::receipts_path(
                &self.canonical_source_log_path,
            ))
            .ok()
            .map(Arc::new);
        }
        SourceReceiptIndex {
            state: Arc::new(RwLock::new(SourceReceiptIndexState {
                frames: self.frames.into_boxed_slice().into_vec(),
                retired,
                retention: self.retention,
                next_byte_offset: Some(physical_tail),
                verified_feed_frontiers: HashMap::new(),
                #[cfg(feature = "scenario")]
                read_verifications: HashMap::new(),
                #[cfg(feature = "scenario")]
                frame_verifications: HashMap::new(),
                #[cfg(feature = "scenario")]
                binding_verification_categories: HashMap::new(),
            })),
            source_log_path: Some(Arc::new(self.canonical_source_log_path)),
        }
    }
}

impl SourceReceiptIndex {
    /// Retirement, surviving-obligation and open-continuation authentication counts.
    #[cfg(feature = "scenario")]
    pub fn binding_verification_counts(&self, receipt: AppendReceipt) -> [usize; 3] {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .binding_verification_categories
            .get(&(receipt.sequence, receipt.this_hash))
            .copied()
            .unwrap_or_default()
    }

    #[cfg(feature = "scenario")]
    pub(crate) fn record_binding_verification_category(
        &self,
        receipt: AppendReceipt,
        category: usize,
    ) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = state
            .binding_verification_categories
            .entry((receipt.sequence, receipt.this_hash))
            .or_default()
            .get_mut(category)
        {
            *count += 1;
        }
    }

    #[cfg(feature = "scenario")]
    pub fn read_verification_count(&self, receipt: AppendReceipt) -> usize {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .read_verifications
            .get(&(receipt.sequence, receipt.this_hash))
            .copied()
            .unwrap_or(0)
    }

    #[cfg(feature = "scenario")]
    pub(crate) fn record_read_verification(&self, receipt: AppendReceipt) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state
            .read_verifications
            .entry((receipt.sequence, receipt.this_hash))
            .or_default() += 1;
    }

    #[cfg(feature = "scenario")]
    pub fn frame_verification_count(&self, receipt: AppendReceipt) -> usize {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frame_verifications
            .get(&(receipt.sequence, receipt.this_hash))
            .copied()
            .unwrap_or(0)
    }

    #[cfg(feature = "scenario")]
    pub(crate) fn record_frame_verification(&self, receipt: AppendReceipt) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state
            .frame_verifications
            .entry((receipt.sequence, receipt.this_hash))
            .or_default() += 1;
    }

    pub(crate) fn remember_verified_frontier(
        &self,
        frontier: &crate::frame_admission::FeedHistoryFrontier,
        verified_epoch: u64,
    ) -> Result<(), crate::frame_admission::FrameAdmissionError> {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .retention
            .as_ref()
            .map_or(0, |authority| authority.epoch)
            != verified_epoch
        {
            return Err(crate::frame_admission::FrameAdmissionError::InvalidPrefix(
                "retention epoch changed during frontier verification",
            ));
        }
        state
            .verified_feed_frontiers
            .insert(frontier.wallet, (verified_epoch, frontier.clone()));
        Ok(())
    }

    pub(crate) fn forget_verified_frontier(&self, wallet: pe_core_types::WalletAddress) {
        self.state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .verified_feed_frontiers
            .remove(&wallet);
    }

    pub(crate) fn verify_frame_frontier(
        &self,
        frontier: &crate::frame_admission::FeedHistoryFrontier,
    ) -> Result<(), crate::frame_admission::FrameAdmissionError> {
        let epoch = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let epoch = state
                .retention
                .as_ref()
                .map_or(0, |authority| authority.epoch);
            if state
                .verified_feed_frontiers
                .get(&frontier.wallet)
                .is_some_and(|(verified_epoch, cached)| {
                    *verified_epoch == epoch && cached == frontier
                })
            {
                return Ok(());
            }
            epoch
        };
        #[cfg(feature = "scenario")]
        self.record_read_verification(frontier.commitment);
        frontier.verify(&mut |receipt| {
            self.source_envelope(receipt)
                .map(crate::bucket_commit::CompleteActivityPage::from)
        })?;
        self.remember_verified_frontier(frontier, epoch)
    }

    pub(crate) fn canonical_path(&self) -> Option<&Path> {
        self.source_log_path.as_deref().map(PathBuf::as_path)
    }

    /// Receipts `[start, count)` after binding receipt `count - 1` to `binding`. Retired receipts
    /// in the range are read from the sidecar after the lock is released.
    pub(crate) fn checkpoint_suffix(
        &self,
        start: usize,
        count: usize,
        binding: &LogTailBinding,
    ) -> Result<Vec<SourceFrameMetadata>, RiskInputsUnavailable> {
        self.checkpoint_suffix_inner(
            start,
            count,
            binding,
            #[cfg(feature = "scenario")]
            None,
        )
    }

    /// Pause after the lock is released and before retired receipts are read, to exercise a
    /// concurrent retention switch or sidecar rewrite (#745).
    #[cfg(all(test, feature = "scenario"))]
    pub(crate) fn checkpoint_suffix_with_pause(
        &self,
        start: usize,
        count: usize,
        binding: &LogTailBinding,
        pause: &mut dyn FnMut(),
    ) -> Result<Vec<SourceFrameMetadata>, RiskInputsUnavailable> {
        self.checkpoint_suffix_inner(start, count, binding, Some(pause))
    }

    fn checkpoint_suffix_inner(
        &self,
        start: usize,
        count: usize,
        binding: &LogTailBinding,
        #[cfg(feature = "scenario")] pause: Option<&mut dyn FnMut()>,
    ) -> Result<Vec<SourceFrameMetadata>, RiskInputsUnavailable> {
        let start = u64::try_from(start).map_err(|_| RiskInputsUnavailable::Overflow)?;
        let count = u64::try_from(count).map_err(|_| RiskInputsUnavailable::Overflow)?;
        let (base, last, window, sidecar) = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if count > state.len()? || start > count {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
            let base = state.retired.count();
            let last = count
                .checked_sub(1)
                .map(|last| state.slot(last))
                .transpose()?;
            let window_index = |sequence: u64| {
                usize::try_from(sequence.max(base).saturating_sub(base))
                    .map_err(|_| RiskInputsUnavailable::Overflow)
            };
            let window = state
                .frames
                .get(window_index(start)?..window_index(count)?)
                .ok_or(RiskInputsUnavailable::PriceConflict)?
                .to_vec();
            let sidecar = (start < base.min(count))
                .then(|| {
                    state
                        .retired
                        .reader
                        .clone()
                        .ok_or(RiskInputsUnavailable::PriceMissing)
                })
                .transpose()?;
            (base, last, window, sidecar)
        };
        #[cfg(feature = "scenario")]
        if let Some(pause) = pause {
            pause();
        }
        let last = last
            .map(|slot| slot.resolve(count.saturating_sub(1)))
            .transpose()?
            .flatten();
        if last.as_ref().map(|frame| frame.receipt.sequence) != binding.last_sequence
            || last
                .as_ref()
                .map_or(blake3::Hash::from_bytes([0; 32]), |frame| {
                    frame.receipt.this_hash
                })
                != binding.last_hash
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let mut frames = Vec::new();
        if let Some(file) = sidecar {
            for sequence in start..base.min(count) {
                frames.push(read_retired_receipt(&file, sequence)?);
            }
        }
        frames.extend(window);
        Ok(frames)
    }

    pub(crate) fn checkpoint_metadata_valid(
        path: &Path,
        frames: &[SourceFrameMetadata],
        binding: &LogTailBinding,
    ) -> Result<PathBuf, RiskInputsUnavailable> {
        // A missing path is reported before any receipt conflict, as it always has been.
        std::fs::canonicalize(path).map_err(|_| RiskInputsUnavailable::PriceMissing)?;
        let mut previous_offset = None;
        for (index, frame) in frames.iter().enumerate() {
            if frame.receipt.sequence.0
                != u64::try_from(index).map_err(|_| RiskInputsUnavailable::Overflow)?
                || frame.byte_offset.is_none_or(|offset| {
                    offset < pe_event_log::HEADER_LEN
                        || offset >= binding.physical_tail
                        || previous_offset.is_some_and(|previous| offset <= previous)
                })
                || (index == 0 && frame.byte_offset != Some(pe_event_log::HEADER_LEN))
            {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
            previous_offset = frame.byte_offset;
        }
        Self::checkpoint_tail_valid(path, frames.last(), binding)
    }

    /// Bind the final restored receipt, or its absence, and the canonical path to `binding`.
    fn checkpoint_tail_valid(
        path: &Path,
        last: Option<&SourceFrameMetadata>,
        binding: &LogTailBinding,
    ) -> Result<PathBuf, RiskInputsUnavailable> {
        let canonical_source_log_path =
            std::fs::canonicalize(path).map_err(|_| RiskInputsUnavailable::PriceMissing)?;
        if (last.is_none() && binding.physical_tail != pe_event_log::HEADER_LEN)
            || last.map(|frame| frame.receipt.sequence) != binding.last_sequence
            || last.map_or(blake3::Hash::from_bytes([0; 32]), |frame| {
                frame.receipt.this_hash
            }) != binding.last_hash
            || canonical_source_log_path != binding.path
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        Ok(canonical_source_log_path)
    }

    pub(crate) fn restore_staging(
        path: &Path,
        frames: Vec<SourceFrameMetadata>,
        binding: &LogTailBinding,
    ) -> Result<SourceReceiptIndexStaging, RiskInputsUnavailable> {
        let canonical_source_log_path = Self::checkpoint_metadata_valid(path, &frames, binding)?;
        let mut staging = SourceReceiptIndexStaging::new(
            canonical_source_log_path,
            RetentionAuthority::load(path)
                .map_err(|_| RiskInputsUnavailable::PriceConflict)?
                .map(Arc::new),
        );
        for frame in frames {
            staging.push(frame)?;
        }
        Ok(staging)
    }

    /// Start an externally driven source-receipt projection bound to the canonical log path (#572).
    pub(crate) fn staging(
        source_log_path: &Path,
    ) -> Result<SourceReceiptIndexStaging, RiskInputsUnavailable> {
        let canonical_source_log_path = std::fs::canonicalize(source_log_path)
            .map_err(|_| RiskInputsUnavailable::PriceMissing)?;
        Ok(SourceReceiptIndexStaging::new(
            canonical_source_log_path,
            RetentionAuthority::load(source_log_path)
                .map_err(|_| RiskInputsUnavailable::PriceConflict)?
                .map(Arc::new),
        ))
    }

    /// Rebuild the complete verified source-log projection at boot.
    pub fn replay(source_log_path: &Path) -> Result<Self, RiskInputsUnavailable> {
        crate::source_log_boot::rebuild_source_log(source_log_path, false, None)
            .map(|rebuilt| rebuilt.index)
            .map_err(|_| RiskInputsUnavailable::PriceConflict)
    }

    /// Check the boundary chain head and every pin against `lookup`, which returns the exact
    /// metadata of a receipt, `None` past the tail, or an error when it cannot be read.
    fn validate_retention(
        lookup: &dyn Fn(u64) -> Result<Option<SourceFrameMetadata>, RiskInputsUnavailable>,
        retention: Option<&RetentionAuthority>,
    ) -> Result<(), RiskInputsUnavailable> {
        let Some(authority) = retention else {
            return Ok(());
        };
        let boundary = authority.boundary.sequence.0;
        if lookup(boundary.saturating_sub(1))?
            .is_none_or(|frame| frame.receipt.this_hash != authority.chain_head)
            || lookup(boundary)?
                .is_some_and(|frame| frame.byte_offset != Some(authority.boundary.offset))
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        for pin in &authority.pins {
            let sequence = pin.sequence.0;
            if lookup(sequence)?.is_none_or(|frame| {
                frame.receipt.this_hash != pin.hash || frame.byte_offset != Some(pin.offset)
            }) || sequence
                .checked_sub(1)
                .map(lookup)
                .transpose()?
                .flatten()
                .map_or(blake3::Hash::from_bytes([0; 32]), |frame| {
                    frame.receipt.this_hash
                })
                != pin.predecessor_hash
            {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
        }
        Ok(())
    }

    /// Receipt count bound by `tail`, whose final receipt must match it. The tail is a current
    /// binding, so its final receipt is in memory.
    fn bound_tail_count(
        state: &SourceReceiptIndexState,
        tail: &LogTailBinding,
    ) -> Result<u64, RiskInputsUnavailable> {
        let count = tail.last_sequence.map_or(Ok(0), |last| {
            last.0.checked_add(1).ok_or(RiskInputsUnavailable::Overflow)
        })?;
        let last = count.checked_sub(1).map(|last| state.memory(last));
        if count > state.len()?
            || last.is_some_and(|frame| {
                frame.is_none_or(|frame| frame.receipt.this_hash != tail.last_hash)
            })
            || (last.is_none() && tail.last_hash != blake3::Hash::from_bytes([0; 32]))
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        Ok(count)
    }

    /// The receipt a retention preparation's scan of every receipt bound by `tail` selects: the
    /// first received at or after `cutoff_millis`, else the last (#745). The retired prefix is
    /// summarized by its newest receive time, so only the window is searched.
    pub(crate) fn retention_boundary_candidate(
        &self,
        tail: &LogTailBinding,
        cutoff_millis: i64,
    ) -> Result<BoundaryCandidate, RiskInputsUnavailable> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = Self::bound_tail_count(&state, tail)?;
        let base = state.retired.count();
        if count == 0 {
            return Ok(BoundaryCandidate::Empty);
        }
        if count <= base
            || state
                .retired
                .max_received
                .is_some_and(|newest| newest >= cutoff_millis)
        {
            return Ok(BoundaryCandidate::Retired { base });
        }
        let window = state
            .frames
            .get(
                ..usize::try_from(count.saturating_sub(base))
                    .map_err(|_| RiskInputsUnavailable::Overflow)?,
            )
            .ok_or(RiskInputsUnavailable::PriceConflict)?;
        window
            .iter()
            .find(|frame| frame.received_millis >= cutoff_millis)
            .or_else(|| window.last())
            .cloned()
            .map(BoundaryCandidate::Frame)
            .ok_or(RiskInputsUnavailable::PriceConflict)
    }

    /// Inputs for the retention pre-commit audit of receipts `[0, boundary)` (#745): the retired
    /// commitment and the in-memory receipts from the window base to `boundary`.
    pub(crate) fn retention_audit(
        &self,
        boundary: EventSeq,
        tail: &LogTailBinding,
    ) -> Result<RetentionAudit, RiskInputsUnavailable> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = Self::bound_tail_count(&state, tail)?;
        let base = state.retired.count();
        if boundary.0 < base || boundary.0 > count {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let window = state
            .frames
            .get(
                ..usize::try_from(boundary.0.saturating_sub(base))
                    .map_err(|_| RiskInputsUnavailable::Overflow)?,
            )
            .ok_or(RiskInputsUnavailable::PriceConflict)?
            .to_vec();
        Ok(RetentionAudit {
            commitment: state.retired.commitment.clone(),
            window,
        })
    }

    /// Number of receipts below the in-memory window: the committed retention boundary, or zero.
    pub(crate) fn retired_count(&self) -> u64 {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retired
            .count()
    }

    /// The receipt sidecar handle that serves retired receipts: the current one, or a new one
    /// opened for the first retention switch. A retention commit opens it under the checkpoint
    /// lock before its authority can become visible (#745).
    pub(crate) fn retired_reader(&self) -> Result<Arc<std::fs::File>, RiskInputsUnavailable> {
        if let Some(reader) = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retired
            .reader
            .clone()
        {
            return Ok(reader);
        }
        let path = self
            .source_log_path
            .as_ref()
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        std::fs::File::open(crate::source_checkpoint::receipts_path(path))
            .map(Arc::new)
            .map_err(|_| RiskInputsUnavailable::PriceMissing)
    }

    /// Validate while holding the index lock, before making the authority visible.
    ///
    /// Receipts below the current window that the authority depends on are read from the sidecar
    /// before the write guard is taken; the guard then requires the window and epoch unchanged.
    /// The new window and retired summary are built here, so the switch cannot fail later.
    /// `reader` serves retired receipts after the first switch; without one they fail closed.
    pub(crate) fn prepare_retention_install(
        &self,
        authority: RetentionAuthority,
        reader: Option<Arc<std::fs::File>>,
    ) -> Result<RetentionIndexInstall<'_>, RiskInputsUnavailable> {
        authority
            .validate()
            .map_err(|_| RiskInputsUnavailable::PriceConflict)?;
        let dependencies = self.retention_dependencies(&authority, reader)?;
        self.prepare_resolved_retention_install(authority, dependencies)
    }

    /// Read, without the index lock, every retired receipt below the cache that `authority`'s
    /// boundary and pins depend on, recording the window base and epoch they were read against.
    fn retention_dependencies(
        &self,
        authority: &RetentionAuthority,
        reader: Option<Arc<std::fs::File>>,
    ) -> Result<RetentionDependencies, RiskInputsUnavailable> {
        let (base, epoch, current_reader) = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                state.retired.count(),
                state
                    .retention
                    .as_ref()
                    .map_or(0, |authority| authority.epoch),
                state.retired.reader.clone(),
            )
        };
        let reader = current_reader.or(reader);
        // Below the retired cache (the two highest retired receipts) only the sidecar has them.
        let uncached = base.saturating_sub(2);
        let mut resolved = BTreeMap::new();
        let boundary = authority.boundary.sequence.0;
        let dependencies = authority
            .pins
            .iter()
            .flat_map(|pin| [pin.sequence.0.checked_sub(1), Some(pin.sequence.0)])
            .chain([boundary.checked_sub(1), Some(boundary)])
            .flatten()
            .filter(|sequence| *sequence < uncached);
        for sequence in dependencies {
            if let std::collections::btree_map::Entry::Vacant(entry) = resolved.entry(sequence) {
                let file = reader.as_ref().ok_or(RiskInputsUnavailable::PriceMissing)?;
                entry.insert(read_retired_receipt(file, sequence)?);
            }
        }
        Ok(RetentionDependencies {
            base,
            epoch,
            reader,
            resolved,
        })
    }

    /// Take the write guard and validate `authority` against `dependencies`, refusing when the
    /// window base or epoch they were read against has moved since.
    fn prepare_resolved_retention_install(
        &self,
        authority: RetentionAuthority,
        dependencies: RetentionDependencies,
    ) -> Result<RetentionIndexInstall<'_>, RiskInputsUnavailable> {
        let RetentionDependencies {
            base,
            epoch,
            reader,
            resolved,
        } = dependencies;
        let boundary = authority.boundary.sequence.0;
        let state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retired.count() != base
            || state
                .retention
                .as_ref()
                .map_or(0, |authority| authority.epoch)
                != epoch
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        if state.retention.as_ref().is_some_and(|old| {
            old.epoch > authority.epoch || (old.epoch == authority.epoch && **old != authority)
        }) {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let len = state.len()?;
        Self::validate_retention(
            &|sequence| {
                if let Some(frame) = state.memory(sequence) {
                    return Ok(Some(frame.clone()));
                }
                if sequence >= len {
                    return Ok(None);
                }
                resolved
                    .get(&sequence)
                    .cloned()
                    .map(Some)
                    .ok_or(RiskInputsUnavailable::PriceConflict)
            },
            Some(&authority),
        )?;
        let captured = authority
            .retained_tail
            .last_sequence
            .ok_or(RiskInputsUnavailable::PriceConflict)?
            .0;
        let captured_end = state
            .memory(
                captured
                    .checked_add(1)
                    .ok_or(RiskInputsUnavailable::Overflow)?,
            )
            .and_then(|frame| frame.byte_offset)
            .or(state.next_byte_offset);
        if state
            .memory(captured)
            .is_none_or(|frame| frame.receipt.this_hash != authority.retained_tail.last_hash)
            || captured_end != Some(authority.retained_tail.physical_tail)
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        // Boundaries only advance; the retired prefix can never move back into memory.
        if boundary < base {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let window = if boundary > base {
            let split = usize::try_from(boundary.saturating_sub(base))
                .map_err(|_| RiskInputsUnavailable::Overflow)?;
            let retiring = state
                .frames
                .get(..split)
                .ok_or(RiskInputsUnavailable::PriceConflict)?;
            let mut retired = state.retired.clone();
            for frame in retiring {
                retired.retire(frame.clone())?;
            }
            retired.reader = reader;
            let frames = state
                .frames
                .get(split..)
                .ok_or(RiskInputsUnavailable::PriceConflict)?
                .to_vec();
            Some((frames, retired))
        } else {
            None
        };
        Ok(RetentionIndexInstall {
            state,
            authority: Arc::new(authority),
            window,
        })
    }

    /// Switch exact reads as soon as the committed authority becomes visible.
    pub fn install_retention(
        &self,
        authority: RetentionAuthority,
    ) -> Result<(), RiskInputsUnavailable> {
        let reader = self.retired_reader().ok();
        self.prepare_retention_install(authority, reader)?
            .install_retention();
        Ok(())
    }

    /// Epoch captured by checkpoint verification and publication.
    pub fn retention_epoch(&self) -> u64 {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retention
            .as_ref()
            .map_or(0, |authority| authority.epoch)
    }

    /// Check every committed pin, including dependencies with no reducer owner.
    pub fn verify_retention_pins(&self) -> Result<(), RiskInputsUnavailable> {
        self.verify_retention_pins_cancellable(None)
    }

    pub(crate) fn verify_retention_pins_cancellable(
        &self,
        cancel: Option<&std::sync::atomic::AtomicBool>,
    ) -> Result<(), RiskInputsUnavailable> {
        let retention = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retention
            .clone();
        let epoch = retention.as_ref().map_or(0, |authority| authority.epoch);
        if let Some(authority) = retention {
            for pin in &authority.pins {
                if cancel.is_some_and(|cancel| cancel.load(std::sync::atomic::Ordering::Acquire)) {
                    return Err(RiskInputsUnavailable::Cancelled);
                }
                self.source_envelope(AppendReceipt {
                    sequence: pin.sequence,
                    this_hash: pin.hash,
                })?;
            }
        }
        if self.retention_epoch() != epoch {
            return Err(RiskInputsUnavailable::RetentionChanged);
        }
        Ok(())
    }

    /// Snapshot the synchronized indexed source tail without reading the log (GitHub issue #574).
    pub(crate) fn current_tail_binding(&self) -> Result<LogTailBinding, RiskInputsUnavailable> {
        let (path, physical_tail, last_sequence, last_hash) = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let path = self
                .source_log_path
                .as_ref()
                .map(|path| path.as_ref().clone())
                .ok_or(RiskInputsUnavailable::PriceMissing)?;
            let physical_tail = state
                .next_byte_offset
                .ok_or(RiskInputsUnavailable::PriceMissing)?;
            let last_sequence = state.last().map(|frame| frame.receipt.sequence);
            let last_hash = state
                .last()
                .map_or(blake3::Hash::from_bytes([0; 32]), |frame| {
                    frame.receipt.this_hash
                });
            (path, physical_tail, last_sequence, last_hash)
        };
        Ok(LogTailBinding {
            path,
            physical_tail,
            last_sequence,
            last_hash,
        })
    }

    /// Capture the indexed tail and its final frame from the same projection state.
    pub(crate) fn tail_with_last_frame(
        &self,
    ) -> Result<(LogTailBinding, u64, AppendReceipt, blake3::Hash), RiskInputsUnavailable> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = self
            .source_log_path
            .as_ref()
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        let last = state.last().ok_or(RiskInputsUnavailable::PriceMissing)?;
        let byte_offset = last
            .byte_offset
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        let physical_tail = state
            .next_byte_offset
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        // The predecessor is in the window or the retired cache, never only in the sidecar.
        let previous_hash = match last.receipt.sequence.0.checked_sub(1) {
            Some(previous) => {
                state
                    .memory(previous)
                    .ok_or(RiskInputsUnavailable::PriceMissing)?
                    .receipt
                    .this_hash
            }
            None => blake3::Hash::from_bytes([0; 32]),
        };
        Ok((
            LogTailBinding {
                path: path.as_ref().clone(),
                physical_tail,
                last_sequence: Some(last.receipt.sequence),
                last_hash: last.receipt.this_hash,
            },
            byte_offset,
            last.receipt,
            previous_hash,
        ))
    }

    /// Extend the projection with an append that the source-log owner has already synchronized.
    pub(crate) fn record_synced_append(
        &self,
        receipt: AppendReceipt,
        envelope: &pe_event_log::EnvelopeIn,
    ) -> Result<(), RiskInputsUnavailable> {
        let received_millis = received_at_millis(&envelope.received_at)?;
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let expected_sequence = EventSeq(state.len()?);
        if receipt.sequence < expected_sequence {
            let slot = state.slot(receipt.sequence.0)?;
            // A retired duplicate is read after releasing the guard; retired receipts never change.
            drop(state);
            let known = slot
                .resolve(receipt.sequence.0)?
                .ok_or(RiskInputsUnavailable::PriceConflict)?;
            return if known.receipt == receipt && known.received_millis == received_millis {
                Ok(())
            } else {
                Err(RiskInputsUnavailable::PriceConflict)
            };
        }
        if receipt.sequence != expected_sequence {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let prev_hash = state
            .last()
            .map_or(blake3::Hash::from_bytes([0; 32]), |known| {
                known.receipt.this_hash
            });
        let (_raw_payload_hash, _, this_hash) =
            pe_event_log::envelope::compute_hashes(pe_event_log::envelope::HashInput {
                seq: receipt.sequence,
                source_id: &envelope.source_id,
                schema_version: envelope.schema_version,
                parser_version: envelope.parser_version,
                observed_at: &envelope.observed_at,
                received_at: &envelope.received_at,
                content_type: &envelope.content_type,
                prev_hash: &prev_hash,
                payload: &envelope.payload,
            })
            .map_err(|_| RiskInputsUnavailable::PriceConflict)?;
        if this_hash != receipt.this_hash {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let byte_offset = state.next_byte_offset;
        if let (Some(path), Some(frame_start)) = (&self.source_log_path, byte_offset) {
            let new_tail = std::fs::metadata(path.as_ref())
                .map_err(|_| RiskInputsUnavailable::PriceMissing)?
                .len();
            if new_tail <= frame_start {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
            state.next_byte_offset = Some(new_tail);
        }
        state.frames.push(SourceFrameMetadata {
            receipt,
            received_millis,
            byte_offset,
        });
        Ok(())
    }

    /// Extend the projection through complete frames that survived a synchronization uncertainty.
    ///
    /// The sole source-log coordinator calls this after `Writer::open` has verified and
    /// synchronized the physical tail, but before it retries the held envelope. The stored next
    /// byte offset is the boundary between already indexed frames and the newly verified suffix.
    pub(crate) fn catch_up_verified_tail(&self) -> Result<(), RiskInputsUnavailable> {
        self.catch_up(
            None,
            &mut |_, _| {},
            #[cfg(feature = "scenario")]
            None,
        )
    }

    /// Pause after recovery and before extending the projection, to exercise a concurrent
    /// retention switch (#745).
    #[cfg(feature = "scenario")]
    pub fn catch_up_verified_tail_with_pause(
        &self,
        pause: &mut dyn FnMut(),
    ) -> Result<(), RiskInputsUnavailable> {
        self.catch_up(None, &mut |_, _| {}, Some(pause))
    }

    /// Recover exactly the source-log suffix ending at `expected_tail`, reporting each frame (#572).
    pub(crate) fn catch_up_to(
        &self,
        expected_tail: u64,
        observer: &mut dyn FnMut(u64, &EventEnvelope),
    ) -> Result<(), RiskInputsUnavailable> {
        self.catch_up(
            Some(expected_tail),
            observer,
            #[cfg(feature = "scenario")]
            None,
        )
    }

    fn catch_up(
        &self,
        expected_tail: Option<u64>,
        observer: &mut dyn FnMut(u64, &EventEnvelope),
        #[cfg(feature = "scenario")] pause: Option<&mut dyn FnMut()>,
    ) -> Result<(), RiskInputsUnavailable> {
        let Some(path) = self.source_log_path.as_ref() else {
            return if expected_tail.is_some() {
                Err(RiskInputsUnavailable::PriceMissing)
            } else {
                Ok(())
            };
        };
        // Absolute counts survive a retention switch that moves the window during recovery.
        let (indexed_len, next_byte_offset, indexed_last_hash) = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(next_byte_offset) = state.next_byte_offset else {
                return Err(RiskInputsUnavailable::PriceConflict);
            };
            (
                state.len()?,
                next_byte_offset,
                state
                    .last()
                    .map_or(blake3::Hash::from_bytes([0; 32]), |frame| {
                        frame.receipt.this_hash
                    }),
            )
        };
        let current_tail = std::fs::metadata(path.as_ref())
            .map_err(|_| RiskInputsUnavailable::PriceMissing)?
            .len();
        if expected_tail.is_some_and(|expected| current_tail != expected) {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let verified_tail = expected_tail.unwrap_or(current_tail);
        if verified_tail < next_byte_offset {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        let mut previous_hash = indexed_last_hash;
        let mut recovered = Vec::new();
        let mut byte_offset = next_byte_offset;
        while byte_offset < verified_tail {
            let expected_sequence = u64::try_from(recovered.len())
                .ok()
                .and_then(|recovered| indexed_len.checked_add(recovered))
                .map(EventSeq)
                .ok_or(RiskInputsUnavailable::Overflow)?;
            let (envelope, frame_end) =
                Reader::read_at(path.as_ref(), byte_offset, expected_sequence, previous_hash)
                    .map_err(|_| RiskInputsUnavailable::PriceConflict)?;
            if frame_end <= byte_offset || frame_end > verified_tail {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
            if envelope.seq != expected_sequence || envelope.prev_hash != previous_hash {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
            observer(byte_offset, &envelope);
            let receipt = AppendReceipt {
                sequence: envelope.seq,
                this_hash: envelope.this_hash,
            };
            let received_millis = received_at_millis(&envelope.received_at)?;
            previous_hash = receipt.this_hash;
            recovered.push(SourceFrameMetadata {
                receipt,
                received_millis,
                byte_offset: Some(byte_offset),
            });
            byte_offset = frame_end;
        }
        if byte_offset != verified_tail {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        if expected_tail.is_some()
            && std::fs::metadata(path.as_ref())
                .map_err(|_| RiskInputsUnavailable::PriceMissing)?
                .len()
                != verified_tail
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        #[cfg(feature = "scenario")]
        if let Some(pause) = pause {
            pause();
        }

        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.len()? != indexed_len
            || state.next_byte_offset != Some(next_byte_offset)
            || state
                .last()
                .map_or(blake3::Hash::from_bytes([0; 32]), |frame| {
                    frame.receipt.this_hash
                })
                != indexed_last_hash
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        state.frames.extend(recovered);
        state.next_byte_offset = Some(verified_tail);
        Ok(())
    }

    /// Return the receipt and receive millisecond stored for `sequence`, when present (#572).
    ///
    /// A retired receipt is read from the sidecar; a failed read is `PriceMissing` (#745).
    pub fn receipt_at(
        &self,
        sequence: EventSeq,
    ) -> Result<Option<(AppendReceipt, i64)>, RiskInputsUnavailable> {
        let slot = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .slot(sequence.0)?;
        Ok(slot
            .resolve(sequence.0)?
            .map(|metadata| (metadata.receipt, metadata.received_millis)))
    }

    pub(crate) fn received_millis(
        &self,
        receipt: AppendReceipt,
    ) -> Result<i64, RiskInputsUnavailable> {
        self.receipt_at(receipt.sequence)?
            .filter(|(known_receipt, _)| *known_receipt == receipt)
            .map(|(_, received_millis)| received_millis)
            .ok_or(RiskInputsUnavailable::PriceMissing)
    }

    /// Read one exact receipt-bearing source envelope without retaining neighboring payloads.
    pub(crate) fn source_envelope(
        &self,
        receipt: AppendReceipt,
    ) -> Result<pe_event_log::EventEnvelope, RiskInputsUnavailable> {
        self.source_envelope_inner(
            receipt,
            #[cfg(feature = "scenario")]
            None,
        )
    }

    /// Pause after lookup and before physical I/O to exercise a concurrent retention commit.
    #[cfg(feature = "scenario")]
    pub fn source_envelope_with_pause(
        &self,
        receipt: AppendReceipt,
        pause: &mut dyn FnMut(),
    ) -> Result<EventEnvelope, RiskInputsUnavailable> {
        self.source_envelope_inner(receipt, Some(pause))
    }

    fn source_envelope_inner(
        &self,
        receipt: AppendReceipt,
        #[cfg(feature = "scenario")] pause: Option<&mut dyn FnMut()>,
    ) -> Result<EventEnvelope, RiskInputsUnavailable> {
        let sequence = receipt.sequence.0;
        let (metadata, previous, successor) = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            Self::require_retained(&state, receipt.sequence)?;
            (
                state.slot(sequence)?,
                sequence
                    .checked_sub(1)
                    .map(|previous| state.slot(previous))
                    .transpose()?,
                sequence
                    .checked_add(1)
                    .map(|next| state.slot(next))
                    .transpose()?,
            )
        };
        // Retired receipts (a pin and its neighbors) are read only after the lock is released.
        let metadata = metadata
            .resolve(sequence)?
            .filter(|metadata| metadata.receipt == receipt)
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        let previous_hash = match previous {
            Some(slot) => slot
                .resolve(sequence.saturating_sub(1))?
                .map_or(blake3::Hash::from_bytes([0; 32]), |previous| {
                    previous.receipt.this_hash
                }),
            None => blake3::Hash::from_bytes([0; 32]),
        };
        let successor = successor
            .map(|slot| slot.resolve(sequence.saturating_add(1)))
            .transpose()?
            .flatten();
        let path = self
            .source_log_path
            .as_ref()
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        let byte_offset = metadata
            .byte_offset
            .ok_or(RiskInputsUnavailable::PriceMissing)?;
        #[cfg(feature = "scenario")]
        if let Some(pause) = pause {
            pause();
        }
        let read = Reader::read_at(path.as_ref(), byte_offset, receipt.sequence, previous_hash);
        // The commit may have advanced while the index lock was released, even if no
        // bytes have been punched yet. A physical failure follows the same rule.
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self::require_retained(&state, receipt.sequence)?;
        let (envelope, end) = read.map_err(|_| RiskInputsUnavailable::PriceConflict)?;
        if state
            .retention
            .as_ref()
            .is_some_and(|authority| authority.pin(receipt.sequence).is_some())
        {
            let next = sequence
                .checked_add(1)
                .map(|next| state.slot(next))
                .transpose()?;
            let next_byte_offset = state.next_byte_offset;
            drop(state);
            // A successor seen before the read never changes; one appended since may already
            // be retired, so it is read after the guard is released.
            let successor = match (successor, next) {
                (Some(frame), _) => Some(frame),
                (None, Some(slot)) => slot.resolve(sequence.saturating_add(1))?,
                (None, None) => None,
            };
            let expected_end = successor
                .and_then(|frame| frame.byte_offset)
                .or(next_byte_offset);
            if expected_end != Some(end) {
                return Err(RiskInputsUnavailable::PriceConflict);
            }
        }
        if envelope.this_hash != receipt.this_hash
            || received_at_millis(&envelope.received_at)? != metadata.received_millis
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
        Ok(envelope)
    }

    fn require_retained(
        state: &SourceReceiptIndexState,
        sequence: EventSeq,
    ) -> Result<(), RiskInputsUnavailable> {
        if state.retention.as_ref().is_some_and(|authority| {
            sequence < authority.boundary.sequence && authority.pin(sequence).is_none()
        }) {
            return Err(RiskInputsUnavailable::Erased);
        }
        Ok(())
    }

    /// Every receipt from sequence zero, reading retired ones from the sidecar.
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> BTreeMap<EventSeq, (AppendReceipt, i64)> {
        let len = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            .unwrap_or(0);
        (0..len)
            .filter_map(|sequence| self.receipt_at(EventSeq(sequence)).ok().flatten())
            .map(|(receipt, received_millis)| (receipt.sequence, (receipt, received_millis)))
            .collect()
    }

    /// Write the dense receipt sidecar that production keeps beside a retained log, from an index
    /// that still holds every receipt in memory (#745).
    #[cfg(test)]
    pub(crate) fn write_receipt_sidecar_for_test(&self) -> std::io::Result<()> {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retired.count() != 0 {
            return Err(std::io::Error::other("receipts already retired"));
        }
        let path = self
            .source_log_path
            .as_ref()
            .ok_or_else(|| std::io::Error::other("index has no source log"))?;
        let bytes = state
            .frames
            .iter()
            .flat_map(|frame| {
                pe_event_log::ReceiptRecord {
                    receipt: frame.receipt,
                    received_millis: frame.received_millis,
                    byte_offset: frame.byte_offset,
                }
                .encode()
            })
            .collect::<Vec<_>>();
        std::fs::write(crate::source_checkpoint::receipts_path(path), bytes)
    }

    /// Bytes reserved for in-memory receipt metadata: the window's capacity (#745).
    #[cfg(test)]
    pub(crate) fn retained_frame_metadata_bytes(&self) -> usize {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::mem::size_of::<SourceFrameMetadata>().saturating_mul(state.frames.capacity())
    }

    /// Window base, window length and window capacity, for lifecycle scenarios (#745).
    #[cfg(any(test, feature = "scenario"))]
    pub fn receipt_window_for_scenario(&self) -> (u64, usize, usize) {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state.retired.count(),
            state.frames.len(),
            state.frames.capacity(),
        )
    }
}

fn received_at_millis(received_at: &ReceivedAt) -> Result<i64, RiskInputsUnavailable> {
    received_at
        .0
        .unix_timestamp_nanos()
        .checked_div(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or(RiskInputsUnavailable::Overflow)
}

/// Select the exact paper prefix named by a recorded risk audit and prove it is causal to the
/// evaluation clock. Later durable frames are deliberately excluded even when their captured
/// receive timestamps predate the evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PaperPrefixError {
    #[error("financial prefix is absent from the causal paper log")]
    Absent,
    #[error("risk evaluation precedes its financial prefix")]
    Future,
    #[error("paper-prefix timestamp milliseconds overflow")]
    TimestampOverflow,
}

pub(crate) fn paper_prefix_at_financial_prefix(
    frames: &[Arc<ScannedPaperFrame>],
    financial_prefix: AppendReceipt,
    evaluated_at_unix_ms: i64,
) -> Result<&[Arc<ScannedPaperFrame>], PaperPrefixError> {
    let prefix_index = frames
        .iter()
        .position(|frame| frame.receipt == financial_prefix)
        .ok_or(PaperPrefixError::Absent)?;
    let prefix = &frames[..=prefix_index];
    let prefix_millis = prefix
        .last()
        .map(|frame| received_at_millis(&frame.envelope.received_at))
        .transpose()
        .map_err(|_| PaperPrefixError::TimestampOverflow)?
        .ok_or(PaperPrefixError::Absent)?;
    if prefix_millis > evaluated_at_unix_ms {
        return Err(PaperPrefixError::Future);
    }
    Ok(prefix)
}

fn source_receipt_index(
    source_log_path: &Path,
    receipts: impl Iterator<Item = AppendReceipt>,
) -> Result<BTreeMap<EventSeq, (AppendReceipt, i64)>, RiskInputsUnavailable> {
    let mut wanted = BTreeMap::<EventSeq, AppendReceipt>::new();
    for receipt in receipts {
        if wanted
            .insert(receipt.sequence, receipt)
            .is_some_and(|known| known != receipt)
        {
            return Err(RiskInputsUnavailable::PriceConflict);
        }
    }
    if wanted.is_empty() {
        return Ok(BTreeMap::new());
    }
    let index = SourceReceiptIndex::replay(source_log_path)?;
    wanted
        .into_iter()
        .map(|(sequence, receipt)| Ok((sequence, (receipt, index.received_millis(receipt)?))))
        .collect()
}

fn source_receipt_received_millis(
    received_millis: &BTreeMap<EventSeq, (AppendReceipt, i64)>,
    receipt: AppendReceipt,
) -> Result<i64, RiskInputsUnavailable> {
    let Some((known_receipt, received)) = received_millis.get(&receipt.sequence) else {
        return Err(RiskInputsUnavailable::PriceMissing);
    };
    if *known_receipt != receipt {
        return Err(RiskInputsUnavailable::PriceConflict);
    }
    Ok(*received)
}

/// Reconstruct the paper exposure base for one proposal from the completed financial prefix.
///
/// Completed resolutions remove every earlier fill in that market. The caller supplies only the
/// proposed trade facts that are not owned by the prefix; PnL, marks, and latency are composed by
/// [`build_paper_risk_snapshot_from_source_receipts`]. Runtime and offline qualification use this
/// same pure reducer.
pub(crate) fn build_paper_risk_base(
    era: &PaperEra,
    leader_wallet: pe_core_types::WalletAddress,
    market_id: &str,
    proposed_debit: pe_core_types::CollateralAmount,
    per_trade_cap_bps: i32,
) -> Result<PaperExposureBase, RiskInputsUnavailable> {
    let completed = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt, ..
            }) => Some(prepared_receipt.sequence),
            _ => None,
        })
        .collect::<HashSet<_>>();
    let resolved = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                payload: FinancialPayload::Resolution { condition_id, .. },
                ..
            }) if completed.contains(&frame.receipt.sequence) => Some(condition_id.0.clone()),
            _ => None,
        })
        .collect::<HashSet<_>>();

    let mut leader_exposure = pe_core_types::CollateralAmount::ZERO;
    let mut market_exposure = pe_core_types::CollateralAmount::ZERO;
    let mut total_exposure = pe_core_types::CollateralAmount::ZERO;
    for frame in &era.frames {
        let PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
            payload:
                FinancialPayload::Fill {
                    operation,
                    economic,
                },
            ..
        }) = &frame.frame
        else {
            continue;
        };
        if !completed.contains(&frame.receipt.sequence)
            || resolved.contains(&economic.market.market_id)
        {
            continue;
        }
        let debit = economic
            .sizing
            .principal
            .checked_add(economic.fee.expected_fee)
            .map_err(|_| RiskInputsUnavailable::Overflow)?;
        total_exposure = total_exposure
            .checked_add(debit)
            .map_err(|_| RiskInputsUnavailable::Overflow)?;
        if economic.market.market_id == market_id {
            market_exposure = market_exposure
                .checked_add(debit)
                .map_err(|_| RiskInputsUnavailable::Overflow)?;
        }
        if operation.leader_wallet == leader_wallet {
            leader_exposure = leader_exposure
                .checked_add(debit)
                .map_err(|_| RiskInputsUnavailable::Overflow)?;
        }
    }

    let bankroll = era
        .start
        .as_ref()
        .map(|(_, start)| start.starting_bankroll)
        .ok_or(RiskInputsUnavailable::SnapshotSequenceMismatch)?;
    let exposure = |amount| {
        pe_risk_engine::exposure_bps_ceil(amount, bankroll).ok_or(RiskInputsUnavailable::Overflow)
    };
    let leader_exposure_bps = exposure(leader_exposure)?;
    let market_exposure_bps = exposure(market_exposure)?;
    let total_copy_exposure_bps = exposure(total_exposure)?;
    let proposed_trade_bps = exposure(proposed_debit)?;
    Ok(PaperExposureBase {
        leader_exposure_bps,
        market_exposure_bps,
        // No durable market-family identity exists in the paper protocol. The market value is
        // the conservative available family exposure rather than a fabricated zero.
        family_exposure_bps: market_exposure_bps,
        total_copy_exposure_bps,
        proposed_trade_bps,
        per_trade_cap_bps,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::io::{Read as _, Seek as _, Write as _};

    use pe_core_types::{
        AccountId, BasisPoints, CollateralAmount, KellyFraction, PolymarketConditionId,
        PolymarketTokenId, Probability, ReceivedAt, ShareAmount, Side, SourceId, SourceTimestamp,
        WalletAddress,
    };
    use pe_event_log::{ContentType, EnvelopeIn, Scanner, Writer};
    use pe_execution_core::{
        AdmissionReceipts, BalanceAudit, ECONOMIC_PREPARED_VERSION, EconomicPrepared, FeeAudit,
        LadderAskAudit, LadderPlanAudit, LiveAdmissionArtifactAudit, LiveMarketEvidenceAudit,
        MarketSelection, ObservationEvidence, RiskAudit, RiskDecisionAudit, SizingAudit,
        SizingModeAudit,
    };
    use pe_resolver_card::{
        VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
    };
    use pe_source_polymarket_public::PricePoint;
    use pe_venue_polymarket::CompactFeeSchedule;
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

    fn source_input(unix: i64, payload: &[u8]) -> EnvelopeIn {
        let at = OffsetDateTime::from_unix_timestamp(unix).unwrap();
        EnvelopeIn {
            source_id: SourceId("source-receipt-index".to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        }
    }

    fn observe_source_log(path: &Path) -> SourceReceiptIndexStaging {
        let mut staging = SourceReceiptIndex::staging(path).unwrap();
        for item in Reader::replay_with_offsets(path).unwrap() {
            let (byte_offset, _, envelope) = item.unwrap();
            staging.observe(byte_offset, &envelope).unwrap();
        }
        staging
    }

    type SourceIndexSnapshot = (Vec<(AppendReceipt, i64, Option<u64>)>, Option<u64>);

    fn source_index_snapshot(index: &SourceReceiptIndex) -> SourceIndexSnapshot {
        let state = index
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            state
                .frames
                .iter()
                .map(|frame| (frame.receipt, frame.received_millis, frame.byte_offset))
                .collect(),
            state.next_byte_offset,
        )
    }

    fn frame(sequence: u64, unix: i64, record: PaperLogRecord) -> Arc<ScannedPaperFrame> {
        let receipt = receipt(sequence, u8::try_from(sequence).unwrap_or(u8::MAX));
        let at = OffsetDateTime::from_unix_timestamp(unix).unwrap();
        Arc::new(ScannedPaperFrame {
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
        })
    }

    fn halt(
        sequence: u64,
        owner: RiskHaltOwner,
        cause: RiskHaltCause,
        state: HaltState,
    ) -> Arc<ScannedPaperFrame> {
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
            membership: vec![
                WalletAddress::from_hex("0x1111111111111111111111111111111111111111").unwrap(),
            ],
            membership_proofs_hash: "proof".to_owned(),
            schema_version: 2,
            parser_version: 1,
            financial_semantic_version: 1,
        }
    }

    fn latency_economic(
        source_receipt: AppendReceipt,
        financial_prefix: AppendReceipt,
    ) -> EconomicPrepared {
        let condition = PolymarketConditionId("condition-latency".to_owned());
        let price = Price::new(dec!(0.5)).unwrap();
        let shares = ShareAmount::from_whole(2).unwrap();
        let principal = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();
        EconomicPrepared {
            version: ECONOMIC_PREPARED_VERSION,
            market: MarketSelection {
                condition_id: condition.clone(),
                outcome_index: 0,
                token_id: PolymarketTokenId("token-yes".to_owned()),
                side: Side::Buy,
                market_id: condition.0.clone(),
            },
            admission: LiveAdmissionArtifactAudit {
                market: LiveMarketEvidenceAudit {
                    condition_id: condition.clone(),
                    ordered_outcome_token_ids: [
                        PolymarketTokenId("token-yes".to_owned()),
                        PolymarketTokenId("token-no".to_owned()),
                    ],
                    neg_risk: false,
                    minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                    minimum_order_size: shares,
                    observed_at_unix: 9_998,
                    schema_version: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                settlement: VenueSettlementRecord {
                    schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                    condition_id: condition,
                    status: VenueResolutionStatus::Unresolved,
                    raw_evidence_hash: "settlement".to_owned(),
                    source_timestamp_unix: Some(9_998),
                    observed_at_unix: 9_998,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                fee_schedule: CompactFeeSchedule::Zero,
                scheduled_end_unix: Some(20_000),
                receipts: AdmissionReceipts {
                    gamma: source_receipt,
                    clob_long: source_receipt,
                    clob_compact: source_receipt,
                },
            },
            ladder: LadderPlanAudit {
                used_asks: vec![LadderAskAudit { price, shares }],
                best_ask: price,
                limit_price: price,
                minimum_shares: shares,
                principal,
            },
            book_receipt: source_receipt,
            observation: Some(ObservationEvidence {
                source_receipt,
                complete_bound_receipt: source_receipt,
                observed_unix_ms: 9_998_000,
                provenance: "activity_ws".to_owned(),
            }),
            sizing: SizingAudit {
                mode: SizingModeAudit::Kelly {
                    fraction: KellyFraction::new(dec!(0.25)).unwrap(),
                    probability: Probability::new(dec!(0.6)).unwrap(),
                },
                budget: principal,
                principal,
                minimum_shares: shares,
                expected_shares: shares,
                expected_vwap: price,
                all_in_price: price,
                slippage_rate: Decimal::ZERO,
            },
            fee: FeeAudit {
                schedule: CompactFeeSchedule::Zero,
                expected_fee: CollateralAmount::ZERO,
                reserve: CollateralAmount::ZERO,
            },
            risk: RiskAudit {
                financial_prefix,
                snapshot: RiskSnapshot {
                    leader_exposure_bps: BasisPoints::ZERO,
                    market_exposure_bps: BasisPoints::ZERO,
                    family_exposure_bps: BasisPoints::ZERO,
                    total_copy_exposure_bps: BasisPoints::ZERO,
                    intraday_pnl_bps: BasisPoints::ZERO,
                    rolling_7d_pnl_bps: BasisPoints::ZERO,
                    absolute_pnl_bps: BasisPoints::ZERO,
                    copy_latency_kill_switch_active: false,
                    proposed_trade_bps: BasisPoints(10),
                    per_trade_cap_bps: 25,
                    drawdown_policy: pe_risk_engine::DrawdownPolicy::Stops,
                    concentration_caps: None,
                },
                decision: RiskDecisionAudit::Approved,
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: 9_999_000,
            },
            balance: BalanceAudit {
                cash_before: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
                worst_case_debit: principal,
                price_impact_cap_bps: 100,
                chase_ceiling: price,
                band_floor: Price::ZERO,
                band_ceiling_exclusive: Price::ONE,
            },
            applied_configuration_hash: "config".to_owned(),
        }
    }

    fn mark(
        sequence: u64,
        cutoff_unix: i64,
        equity: Decimal,
        invalid: Option<&str>,
    ) -> Arc<ScannedPaperFrame> {
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

    fn era_with_marks(marks: Vec<Arc<ScannedPaperFrame>>) -> PaperEra {
        let start = start();
        let start_frame = frame(
            1,
            100,
            PaperLogRecord::QualificationStarted(Arc::new(start.clone())),
        );
        let mut frames = vec![start_frame];
        frames.extend(marks);
        PaperEra {
            start: Some((receipt(1, 1), Arc::new(start))),
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
                RiskInputsUnavailable::LiveLatencyReleaseCheckpointMissing,
                "a post-QualificationStarted live latency release is missing its journal-tail checkpoint",
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

    #[tokio::test]
    async fn paper_mark_replay_selects_closure_rule_only_when_the_receipt_is_present() {
        let cutoff = 1_790_985_600;
        let condition = "0x50365ef0731cfa26e35d89b66a91bca5ef1ee8090b02d1ced099acc738868e87";
        let token = "27556300168112004153063282106116891181228462668915873615116883805553394180154";
        let history =
            include_bytes!("../tests/fixtures/london_closed_mark/prices_history_13d.json");
        let closure = include_bytes!("../tests/fixtures/london_closed_mark/gamma_closed.json");
        let mut price = crate::paper_recovery::PaperMarkPrice {
            market_id: condition.to_owned(),
            outcome_id: 0,
            price: Some(Price::new(dec!(0.0005)).unwrap()),
            sample_unix: Some(1_790_985_077),
            receipt: Some(receipt(4, 4)),
            closure_receipt: Some(receipt(3, 3)),
            invalid: None,
        };
        let selected = replay_paper_mark_price(&price, token, cutoff, history, Some(closure))
            .await
            .unwrap();
        assert_eq!(selected.price, price.price.unwrap());
        assert_eq!(selected.sample_unix, price.sample_unix.unwrap());
        // Closure after the cutoff (a fractional second rounds up) or conflicting repeated rows,
        // in either order, stay invalid.
        let rows: serde_json::Value = serde_json::from_slice(closure).unwrap();
        let mut late = rows[0].clone();
        late["closedTime"] = serde_json::json!("2026-10-03 00:00:01+00");
        let mut fraction = rows[0].clone();
        fraction["closedTime"] = serde_json::json!("2026-10-03T00:00:00.500Z");
        for invalid in [
            serde_json::json!([fraction]),
            serde_json::json!([late.clone(), rows[0].clone()]),
            serde_json::json!([rows[0].clone(), late]),
        ] {
            let payload = serde_json::to_vec(&invalid).unwrap();
            assert!(matches!(
                replay_paper_mark_price(&price, token, cutoff, history, Some(&payload)).await,
                Err(BoundaryMarkError::Invalid(
                    RiskInputsUnavailable::MarkInvalid
                ))
            ));
        }
        let mut marked = mark(2, cutoff, dec!(99.001), None);
        let PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) =
            &mut Arc::make_mut(&mut marked).frame
        else {
            unreachable!()
        };
        mark.prices.push(price.clone());
        let era = era_with_marks(vec![marked]);
        assert_eq!(
            preceding_midnight_equity(&era, cutoff + 1).unwrap(),
            Some(dec!(99.001))
        );
        price.closure_receipt = None;
        assert!(matches!(
            replay_paper_mark_price(&price, token, cutoff, history, Some(closure)).await,
            Err(BoundaryMarkError::Invalid(
                RiskInputsUnavailable::PriceStale
            ))
        ));
        let recent = format!(
            r#"{{"history":[{{"t":{},"p":0.4}}]}}"#,
            cutoff - MAX_HISTORICAL_MARK_AGE_SECS
        );
        assert_eq!(
            replay_paper_mark_price(&price, token, cutoff, recent.as_bytes(), None)
                .await
                .unwrap()
                .price,
            Price::new(dec!(0.4)).unwrap()
        );
        let encoded = serde_json::to_value(&price).unwrap();
        assert!(encoded.get("closure_receipt").is_none());
        assert_eq!(
            serde_json::from_value::<crate::paper_recovery::PaperMarkPrice>(encoded).unwrap(),
            price
        );
    }

    /// Acquisition and replay share one duplicate-sample rule under both mark rules: repeated
    /// samples are accepted when they agree or sit before the selected sample, and a disagreement
    /// at the selected sample is a conflict on both sides.
    #[tokio::test]
    async fn paper_mark_acquisition_and_replay_share_the_duplicate_sample_rule() {
        let cutoff = 1_790_985_600;
        let condition = "0x50365ef0731cfa26e35d89b66a91bca5ef1ee8090b02d1ced099acc738868e87";
        let token = "27556300168112004153063282106116891181228462668915873615116883805553394180154";
        let closure_payload =
            include_bytes!("../tests/fixtures/london_closed_mark/gamma_closed.json");
        let url = format!(
            "https://offline.invalid/markets?condition_ids={condition}&closed=true&limit={GAMMA_BATCH_LIMIT_PARAM}"
        );
        let fetched = GammaMarketsClient::new(
            "https://offline.invalid".to_owned(),
            FixtureFetcher::new(HashMap::from([(url, closure_payload.to_vec())])),
        )
        .fetch_markets_with_pages(
            &[condition.to_owned()],
            pe_source_polymarket_public::MarketFilter::ClosedOnly,
        )
        .await
        .unwrap();
        let closure = fetched.markets.markets[condition].clone();
        for closed in [false, true] {
            let latest = if closed { cutoff - 523 } else { cutoff - 60 };
            for (samples, expected) in [
                (vec![(latest, "0.4"), (latest, "0.4")], Ok(dec!(0.4))),
                (
                    vec![(latest - 60, "0.3"), (latest - 60, "0.3"), (latest, "0.4")],
                    Ok(dec!(0.4)),
                ),
                (
                    vec![(latest, "0.4"), (latest, "0.5")],
                    Err(RiskInputsUnavailable::PriceConflict),
                ),
            ] {
                let points = samples
                    .iter()
                    .map(|(t, p)| PricePoint {
                        t: *t,
                        price: p.parse().unwrap(),
                    })
                    .collect();
                let classified = ClassifiedPricesHistory::Points(points);
                let acquired = if closed {
                    closed_historical_mark_price(
                        &classified,
                        condition,
                        &closure,
                        cutoff,
                        receipt(4, 4),
                    )
                } else {
                    historical_mark_price(&classified, cutoff, receipt(4, 4))
                };
                let body = samples
                    .iter()
                    .map(|(t, p)| format!(r#"{{"t":{t},"p":{p}}}"#))
                    .collect::<Vec<_>>()
                    .join(",");
                let history = format!(r#"{{"history":[{body}]}}"#);
                let price = crate::paper_recovery::PaperMarkPrice {
                    market_id: condition.to_owned(),
                    outcome_id: 0,
                    price: None,
                    sample_unix: None,
                    receipt: Some(receipt(4, 4)),
                    closure_receipt: closed.then(|| receipt(3, 3)),
                    invalid: None,
                };
                let replayed = replay_paper_mark_price(
                    &price,
                    token,
                    cutoff,
                    history.as_bytes(),
                    closed.then_some(closure_payload.as_slice()),
                )
                .await;
                match expected {
                    Ok(value) => {
                        let acquired = acquired.unwrap();
                        let replayed = replayed.unwrap();
                        assert_eq!(acquired.price.0, value);
                        assert_eq!(replayed.price, acquired.price);
                        assert_eq!(replayed.sample_unix, acquired.sample_unix);
                    }
                    Err(reason) => {
                        assert_eq!(acquired, Err(reason));
                        assert!(matches!(
                            replayed,
                            Err(BoundaryMarkError::Invalid(replayed)) if replayed == reason
                        ));
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn closed_mark_selection_enforces_the_lookback_and_closure_cutoff() {
        let cutoff = 1_790_985_600;
        let condition = "0x50365ef0731cfa26e35d89b66a91bca5ef1ee8090b02d1ced099acc738868e87";
        let url = format!(
            "https://offline.invalid/markets?condition_ids={condition}&closed=true&limit={GAMMA_BATCH_LIMIT_PARAM}"
        );
        let fetched = GammaMarketsClient::new(
            "https://offline.invalid".to_owned(),
            FixtureFetcher::new(HashMap::from([(
                url,
                include_bytes!("../tests/fixtures/london_closed_mark/gamma_closed.json").to_vec(),
            )])),
        )
        .fetch_markets_with_pages(
            &[condition.to_owned()],
            pe_source_polymarket_public::MarketFilter::ClosedOnly,
        )
        .await
        .unwrap();
        let mut closure = fetched.markets.markets[condition].clone();
        closure.closed_time_unix = Some(cutoff);
        let points = |age| {
            ClassifiedPricesHistory::Points(vec![PricePoint {
                t: cutoff - age,
                price: dec!(0.4),
            }])
        };
        assert!(
            closed_historical_mark_price(
                &points(CLOSED_MARK_LOOKBACK_SECS),
                condition,
                &closure,
                cutoff,
                receipt(4, 4)
            )
            .is_ok()
        );
        assert_eq!(
            closed_historical_mark_price(
                &points(CLOSED_MARK_LOOKBACK_SECS + 1),
                condition,
                &closure,
                cutoff,
                receipt(4, 4)
            ),
            Err(RiskInputsUnavailable::PriceStale)
        );
        closure.closed_time_unix = Some(cutoff + 1);
        assert_eq!(
            closed_historical_mark_price(&points(60), condition, &closure, cutoff, receipt(4, 4)),
            Err(RiskInputsUnavailable::MarkInvalid)
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

    /// PASS: replacing the per-evaluation source replay with the maintained source receipt index
    /// leaves a non-empty paper latency sample and its completed-hour p95 byte-for-byte unchanged.
    #[test]
    fn maintained_receipt_index_preserves_paper_latency_samples() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let source_at = OffsetDateTime::from_unix_timestamp(9_998).unwrap();
        let mut source_writer = Writer::open(&source_path).unwrap();
        let source_receipt = source_writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("latency-source".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(source_at),
                received_at: ReceivedAt(source_at),
                content_type: ContentType::Json,
                payload: br#"{"latency":true}"#.to_vec(),
            })
            .unwrap();
        drop(source_writer);

        let prepared = frame(
            2,
            9_999,
            PaperLogRecord::FinancialPrepared {
                expected_authority: crate::paper_recovery::ExpectedAuthority {
                    qualification_start_receipt: receipt(1, 1),
                    prior_completed_prepared_sequence: None,
                },
                payload: FinancialPayload::Fill {
                    operation: crate::paper_recovery::PaperFillOperationIdentity {
                        leader_wallet: WalletAddress::from_hex(
                            "0x1111111111111111111111111111111111111111",
                        )
                        .unwrap(),
                        source_trade_id: pe_core_types::SourceTradeId("trade".to_owned()),
                        observed_at_bucket: 9_998,
                    },
                    economic: latency_economic(source_receipt, receipt(1, 1)),
                },
            },
        );
        let final_frame = frame(
            3,
            10_000,
            PaperLogRecord::FinancialFinal {
                prepared_receipt: prepared.receipt,
                result: FinancialResult::Fill {
                    canonical: crate::paper_recovery::CanonicalFillResult {
                        outcome: "applied".to_owned(),
                        bankroll: dec!(99),
                        applied_prepared_seq: prepared.receipt.sequence,
                        quantity: ShareAmount::from_whole(2).unwrap(),
                        principal: CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                        fee: CollateralAmount::ZERO,
                        fill_price: Price::new(dec!(0.5)).unwrap(),
                    },
                },
            },
        );
        let era = PaperEra {
            start: None,
            frames: vec![prepared, final_frame],
        };

        let source_receipts = paper_fill_source_receipts(&era).unwrap();
        let from_scratch_index =
            source_receipt_index(&source_path, source_receipts.into_iter()).unwrap();
        let from_scratch = paper_latency_samples_from_source_receipts(&era, 10_800, &|receipt| {
            source_receipt_received_millis(&from_scratch_index, receipt)
        })
        .unwrap();
        let maintained = paper_latency_samples(
            &era,
            &SourceReceiptIndex::replay(&source_path).unwrap(),
            10_800,
        )
        .unwrap();

        assert_eq!(maintained, from_scratch);
        assert_eq!(maintained.latest.sample_count, 1);
        assert_eq!(maintained.latest.p95_ms, Some(2_000));

        // #745: the same samples once the sample's source receipt is retired below the two cached
        // receipts and read from the sidecar; damaging its record makes it a missing receipt.
        let mut source_writer = Writer::open(&source_path).unwrap();
        for unix in 10_000..10_003 {
            source_writer
                .append_synced(source_input(unix, b"retained"))
                .unwrap();
        }
        drop(source_writer);
        let windowed = SourceReceiptIndex::replay(&source_path).unwrap();
        windowed.write_receipt_sidecar_for_test().unwrap();
        install_boundary(&windowed, 3);
        assert_eq!(windowed.receipt_window_for_scenario(), (3, 1, 1));
        assert_eq!(
            paper_latency_samples(&era, &windowed, 10_800).unwrap(),
            from_scratch
        );
        damage_receipt_record(&source_path, source_receipt.sequence.0);
        assert_eq!(
            paper_latency_samples(&era, &windowed, 10_800).err(),
            Some(RiskInputsUnavailable::PriceMissing)
        );
    }

    /// Flip one byte of a receipt record's checksum in the sidecar beside `source_log`.
    fn damage_receipt_record(source_log: &Path, sequence: u64) {
        let sidecar = crate::source_checkpoint::receipts_path(source_log);
        let mut bytes = std::fs::read(&sidecar).unwrap();
        bytes[usize::try_from(sequence * pe_event_log::RECEIPT_RECORD_LEN + 50).unwrap()] ^= 1;
        std::fs::write(&sidecar, bytes).unwrap();
    }

    /// Retire every receipt below `boundary` with an unpinned authority over the whole log.
    fn install_boundary(index: &SourceReceiptIndex, boundary: u64) {
        let tail = index.current_tail_binding().unwrap();
        let count = usize::try_from(tail.last_sequence.unwrap().0 + 1).unwrap();
        let frames = index.checkpoint_suffix(0, count, &tail).unwrap();
        let at = |sequence: u64| &frames[usize::try_from(sequence).unwrap()];
        index
            .install_retention(RetentionAuthority {
                format_version: 1,
                epoch: 1,
                advanced_at: 1,
                boundary: pe_event_log::RetentionBoundary {
                    sequence: EventSeq(boundary),
                    offset: at(boundary).byte_offset.unwrap(),
                },
                chain_head: at(boundary - 1).receipt.this_hash,
                pins: Vec::new(),
                retained_tail: pe_event_log::TailBinding::from(&tail),
                feed: Vec::new(),
            })
            .unwrap();
    }

    #[test]
    fn indexed_completed_prepared_matches_log_cutoff_and_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        let before = writer
            .append_synced(source_input(9_999, b"before"))
            .unwrap();
        let at_cutoff = writer
            .append_synced(source_input(10_000, b"at cutoff"))
            .unwrap();
        let boundary = writer
            .append_synced(source_input(10_000, b"boundary"))
            .unwrap();
        let after_boundary = writer
            .append_synced(source_input(9_998, b"late frame"))
            .unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        let make_pair = |sequence: u64, source_receipt: AppendReceipt| {
            let prepared = frame(
                sequence,
                10_001,
                PaperLogRecord::FinancialPrepared {
                    expected_authority: crate::paper_recovery::ExpectedAuthority {
                        qualification_start_receipt: receipt(0, 0),
                        prior_completed_prepared_sequence: None,
                    },
                    payload: FinancialPayload::Fill {
                        operation: crate::paper_recovery::PaperFillOperationIdentity {
                            leader_wallet: WalletAddress::from_hex(
                                "0x1111111111111111111111111111111111111111",
                            )
                            .unwrap(),
                            source_trade_id: pe_core_types::SourceTradeId(format!(
                                "trade-{sequence}"
                            )),
                            observed_at_bucket: 9_999,
                        },
                        economic: latency_economic(source_receipt, receipt(0, 0)),
                    },
                },
            );
            let final_frame = frame(
                sequence + 1,
                10_002,
                PaperLogRecord::FinancialFinal {
                    prepared_receipt: prepared.receipt,
                    result: FinancialResult::Fill {
                        canonical: crate::paper_recovery::CanonicalFillResult {
                            outcome: "applied".to_owned(),
                            bankroll: dec!(99),
                            applied_prepared_seq: prepared.receipt.sequence,
                            quantity: ShareAmount::from_whole(2).unwrap(),
                            principal: CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                            fee: CollateralAmount::ZERO,
                            fill_price: Price::new(dec!(0.5)).unwrap(),
                        },
                    },
                },
            );
            [prepared, final_frame]
        };
        let era = PaperEra {
            start: None,
            frames: [
                make_pair(1, before),
                make_pair(3, at_cutoff),
                make_pair(5, after_boundary),
            ]
            .into_iter()
            .flatten()
            .collect(),
        };
        let from_log =
            completed_prepared_before_boundary(&era, &source_path, 10_000, boundary).unwrap();
        let from_index =
            completed_prepared_before_boundary_indexed(&era, &index, 10_000, boundary).unwrap();
        assert_eq!(from_index, from_log);
        assert_eq!(from_index, HashSet::from([EventSeq(1)]));

        let missing_index = SourceReceiptIndex::replay(&source_path).unwrap();
        missing_index
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frames
            .clear();
        assert_eq!(
            completed_prepared_before_boundary_indexed(&era, &missing_index, 10_000, boundary),
            Err(RiskInputsUnavailable::PriceMissing)
        );
        let mut wrong_hash_era = PaperEra {
            start: None,
            frames: era.frames.clone(),
        };
        if let PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
            payload: FinancialPayload::Fill { economic, .. },
            ..
        }) = &mut Arc::make_mut(&mut wrong_hash_era.frames[0]).frame
        {
            economic
                .observation
                .as_mut()
                .unwrap()
                .source_receipt
                .this_hash = blake3::Hash::from_bytes([99; 32]);
        }
        assert_eq!(
            completed_prepared_before_boundary_indexed(&wrong_hash_era, &index, 10_000, boundary),
            Err(RiskInputsUnavailable::PriceConflict)
        );

        // #745: unchanged once the fills' source receipts are retired below the two cached
        // receipts and read from the sidecar; damaging one makes it a missing receipt.
        let mut writer = Writer::open(&source_path).unwrap();
        for unix in [10_001, 10_002] {
            writer.append_synced(source_input(unix, b"later")).unwrap();
        }
        drop(writer);
        let windowed = SourceReceiptIndex::replay(&source_path).unwrap();
        windowed.write_receipt_sidecar_for_test().unwrap();
        install_boundary(&windowed, 4);
        assert_eq!(windowed.receipt_window_for_scenario(), (4, 2, 2));
        assert_eq!(
            completed_prepared_before_boundary_indexed(&era, &windowed, 10_000, boundary).unwrap(),
            from_log
        );
        damage_receipt_record(&source_path, before.sequence.0);
        assert_eq!(
            completed_prepared_before_boundary_indexed(&era, &windowed, 10_000, boundary),
            Err(RiskInputsUnavailable::PriceMissing)
        );
    }

    #[test]
    fn empty_index_has_no_confirmable_mark_tail() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        drop(Writer::open(&source_path).unwrap());
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        assert!(matches!(
            index.tail_with_last_frame(),
            Err(RiskInputsUnavailable::PriceMissing)
        ));
    }

    #[test]
    fn retention_commit_does_not_recapture_an_erased_verified_frontier() {
        let old = receipt(0, 1);
        let retained = receipt(1, 2);
        for erased_commitment in [true, false] {
            let index = SourceReceiptIndex {
                state: Arc::new(RwLock::new(SourceReceiptIndexState {
                    frames: vec![
                        SourceFrameMetadata {
                            receipt: old,
                            received_millis: 0,
                            byte_offset: Some(pe_event_log::HEADER_LEN),
                        },
                        SourceFrameMetadata {
                            receipt: retained,
                            received_millis: 1,
                            byte_offset: Some(128),
                        },
                    ],
                    next_byte_offset: Some(256),
                    ..SourceReceiptIndexState::default()
                })),
                source_log_path: None,
            };
            let frontier = crate::frame_admission::FeedHistoryFrontier {
                version: 1,
                wallet: WalletAddress([1; 20]),
                fixed_end: 0,
                commitment: if erased_commitment { old } else { retained },
                page_occurrences: vec![crate::bucket_commit::PageOccurrence {
                    request_url: "recorded-page".to_owned(),
                    raw_hash: "recorded-hash".to_owned(),
                    receipt: if erased_commitment { retained } else { old },
                }],
                pages: Vec::new(),
            };
            let _ = index.remember_verified_frontier(&frontier, 0);
            assert!(index.verify_frame_frontier(&frontier).is_ok());
            index
                .install_retention(RetentionAuthority {
                    format_version: 1,
                    epoch: 1,
                    advanced_at: 1,
                    boundary: pe_event_log::RetentionBoundary {
                        sequence: retained.sequence,
                        offset: 128,
                    },
                    chain_head: old.this_hash,
                    pins: Vec::new(),
                    retained_tail: pe_event_log::TailBinding {
                        physical_tail: 256,
                        last_sequence: Some(retained.sequence),
                        last_hash: retained.this_hash,
                    },
                    feed: Vec::new(),
                })
                .unwrap();
            // A verification that started before the commit now attempts to cache its result.
            let _ = index.remember_verified_frontier(&frontier, 0);
            assert!(
                index
                    .state
                    .read()
                    .unwrap()
                    .verified_feed_frontiers
                    .is_empty()
            );
            assert!(index.verify_frame_frontier(&frontier).is_err());
        }
    }

    /// PASS: an empty externally observed projection becomes the same path-bound index as replay,
    /// including its header-only next offset.
    #[test]
    fn source_receipt_staging_matches_empty_replay() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        drop(Writer::open(&source_path).unwrap());

        let replayed = SourceReceiptIndex::replay(&source_path).unwrap();
        let binding = Scanner::verify(&source_path).unwrap();
        let staged = observe_source_log(&source_path).complete(&binding).unwrap();

        assert_eq!(
            source_index_snapshot(&staged),
            source_index_snapshot(&replayed)
        );
        assert_eq!(
            source_index_snapshot(&staged).1,
            Some(binding.physical_tail)
        );
    }

    /// PASS: a completed header-only index exposes the canonical genesis binding without I/O.
    #[test]
    fn current_tail_binding_is_genesis_for_completed_header_only_index() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        drop(Writer::open(&source_path).unwrap());
        let verified = Scanner::verify(&source_path).unwrap();
        let index = observe_source_log(&source_path)
            .complete(&verified)
            .unwrap();

        let current = index.current_tail_binding().unwrap();

        assert_eq!(current, verified);
    }

    /// PASS: replay snapshots the scanner-verified populated tail, then recording one synchronized
    /// append advances the indexed binding to that frame's tail.
    #[test]
    fn current_tail_binding_advances_after_recorded_synced_append() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(10, b"first")).unwrap();
        writer.append_synced(source_input(11, b"second")).unwrap();
        drop(writer);
        let verified = Scanner::verify(&source_path).unwrap();
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        assert_eq!(index.current_tail_binding().unwrap(), verified);

        let before = index.current_tail_binding().unwrap();
        let appended = source_input(12, b"third");
        let mut writer = Writer::open(&source_path).unwrap();
        let receipt = writer
            .append_synced(EnvelopeIn {
                source_id: appended.source_id.clone(),
                schema_version: appended.schema_version,
                parser_version: appended.parser_version,
                observed_at: appended.observed_at.clone(),
                received_at: appended.received_at.clone(),
                content_type: appended.content_type.clone(),
                payload: appended.payload.clone(),
            })
            .unwrap();
        drop(writer);

        index.record_synced_append(receipt, &appended).unwrap();
        let current = index.current_tail_binding().unwrap();

        assert!(current.physical_tail > before.physical_tail);
        assert_eq!(current, Scanner::verify(&source_path).unwrap());
    }

    /// PASS: external observation retains exactly replay's receipts, receive times, frame offsets,
    /// and verified next offset for a non-empty log.
    #[test]
    fn source_receipt_staging_matches_nonempty_replay() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(10, b"first")).unwrap();
        writer.append_synced(source_input(11, b"second")).unwrap();
        drop(writer);

        let replayed = SourceReceiptIndex::replay(&source_path).unwrap();
        let binding = Scanner::verify(&source_path).unwrap();
        let staged = observe_source_log(&source_path).complete(&binding).unwrap();

        assert_eq!(
            source_index_snapshot(&staged),
            source_index_snapshot(&replayed)
        );
        assert_eq!(
            source_index_snapshot(&staged).1,
            Some(binding.physical_tail)
        );
    }

    /// PASS: staging completion rejects a different canonical path and either logical-tail drift.
    #[test]
    fn source_receipt_staging_rejects_mismatched_binding() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let other_path = dir.path().join("other.log");
        for path in [&source_path, &other_path] {
            let mut writer = Writer::open(path).unwrap();
            writer.append_synced(source_input(10, b"same")).unwrap();
        }

        let other_binding = Scanner::verify(&other_path).unwrap();
        assert!(matches!(
            observe_source_log(&source_path).complete(&other_binding),
            Err(RiskInputsUnavailable::PriceConflict)
        ));

        let binding = Scanner::verify(&source_path).unwrap();
        let mut wrong_sequence = binding.clone();
        wrong_sequence.last_sequence = Some(EventSeq(99));
        assert!(matches!(
            observe_source_log(&source_path).complete(&wrong_sequence),
            Err(RiskInputsUnavailable::PriceConflict)
        ));

        let mut wrong_hash = binding;
        wrong_hash.last_hash = blake3::Hash::from_bytes([99; 32]);
        assert!(matches!(
            observe_source_log(&source_path).complete(&wrong_hash),
            Err(RiskInputsUnavailable::PriceConflict)
        ));
    }

    /// PASS: bounded catch-up accepts an empty suffix without notifying the observer or changing
    /// the projection.
    #[test]
    fn source_receipt_catch_up_to_accepts_empty_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(10, b"first")).unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        let before = source_index_snapshot(&index);
        let expected_tail = std::fs::metadata(&source_path).unwrap().len();
        let mut observed = Vec::new();

        index
            .catch_up_to(expected_tail, &mut |offset, envelope| {
                observed.push((offset, envelope.seq));
            })
            .unwrap();

        assert!(observed.is_empty());
        assert_eq!(source_index_snapshot(&index), before);
    }

    /// PASS: bounded catch-up reports exactly the newly recovered frames at their physical starts
    /// and commits their receipts through the requested tail.
    #[test]
    fn source_receipt_catch_up_to_observes_valid_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(10, b"first")).unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();

        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(11, b"second")).unwrap();
        writer.append_synced(source_input(12, b"third")).unwrap();
        drop(writer);
        let expected_tail = std::fs::metadata(&source_path).unwrap().len();
        let expected = Reader::replay_with_offsets(&source_path)
            .unwrap()
            .skip(1)
            .map(|item| {
                let (offset, _, envelope) = item.unwrap();
                (offset, envelope.seq, envelope.payload)
            })
            .collect::<Vec<_>>();
        let mut observed = Vec::new();

        index
            .catch_up_to(expected_tail, &mut |offset, envelope| {
                observed.push((offset, envelope.seq, envelope.payload.clone()));
            })
            .unwrap();

        assert_eq!(observed, expected);
        assert_eq!(source_index_snapshot(&index).0.len(), 3);
        assert_eq!(source_index_snapshot(&index).1, Some(expected_tail));
    }

    /// PASS: bounded catch-up refuses a suffix whose first frame does not continue the indexed
    /// hash, without committing any recovered metadata.
    #[test]
    fn source_receipt_catch_up_to_rejects_wrong_preceding_hash() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(10, b"first")).unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        index
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .frames[0]
            .receipt
            .this_hash = blake3::Hash::from_bytes([99; 32]);
        let before = source_index_snapshot(&index);

        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(11, b"second")).unwrap();
        drop(writer);
        let expected_tail = std::fs::metadata(&source_path).unwrap().len();

        assert_eq!(
            index.catch_up_to(expected_tail, &mut |_, _| {}),
            Err(RiskInputsUnavailable::PriceConflict)
        );
        assert_eq!(source_index_snapshot(&index), before);
    }

    /// PASS: bounded catch-up refuses an incomplete final frame and leaves the projection at its
    /// prior verified boundary.
    #[test]
    fn source_receipt_catch_up_to_rejects_incomplete_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(10, b"first")).unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        let before = source_index_snapshot(&index);

        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(11, b"second")).unwrap();
        drop(writer);
        let full_tail = std::fs::metadata(&source_path).unwrap().len();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&source_path)
            .unwrap();
        file.set_len(full_tail - 1).unwrap();
        let expected_tail = full_tail - 1;

        assert_eq!(
            index.catch_up_to(expected_tail, &mut |_, _| {}),
            Err(RiskInputsUnavailable::PriceConflict)
        );
        assert_eq!(source_index_snapshot(&index), before);
    }

    /// PASS: bounded catch-up refuses a file longer than the caller's exact expected tail before
    /// observing or committing any suffix frame.
    #[test]
    fn source_receipt_catch_up_to_rejects_longer_file() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(10, b"first")).unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        let before = source_index_snapshot(&index);
        let expected_tail = std::fs::metadata(&source_path).unwrap().len();

        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(source_input(11, b"second")).unwrap();
        drop(writer);
        let mut observed = 0;

        assert_eq!(
            index.catch_up_to(expected_tail, &mut |_, _| observed += 1),
            Err(RiskInputsUnavailable::PriceConflict)
        );
        assert_eq!(observed, 0);
        assert_eq!(source_index_snapshot(&index), before);
    }

    /// PASS: sequence lookup distinguishes absence from the exact stored receipt and receive time.
    #[test]
    fn source_receipt_lookup_has_absent_and_present_outcomes() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let mut writer = Writer::open(&source_path).unwrap();
        let stored = writer.append_synced(source_input(10, b"first")).unwrap();
        drop(writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();

        assert_eq!(index.receipt_at(EventSeq(1)).unwrap(), None);
        assert_eq!(
            index.receipt_at(EventSeq(0)).unwrap(),
            Some((stored, 10_000))
        );
    }

    /// PASS: the maintained index retains zero payload bytes, and appending metadata while an
    /// independently loaded payload is held neither clones nor invalidates that payload.
    #[test]
    fn source_receipt_index_is_payload_independent_during_append() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let first_at = OffsetDateTime::from_unix_timestamp(10).unwrap();
        let first_payload = vec![7; 2 * 1024 * 1024];
        let mut writer = Writer::open(&source_path).unwrap();
        let first_receipt = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("large-source-page".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(first_at),
                received_at: ReceivedAt(first_at),
                content_type: ContentType::Json,
                payload: first_payload.clone(),
            })
            .unwrap();
        drop(writer);

        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let metadata_bytes_before = source_receipts.retained_frame_metadata_bytes();
        assert!(metadata_bytes_before < first_payload.len());
        let held = source_receipts.source_envelope(first_receipt).unwrap();
        let held_pointer = held.payload.as_ptr();

        let second_at = OffsetDateTime::from_unix_timestamp(11).unwrap();
        let second = EnvelopeIn {
            source_id: SourceId("second-source-page".to_owned()),
            schema_version: 2,
            parser_version: 3,
            observed_at: SourceTimestamp(second_at),
            received_at: ReceivedAt(second_at),
            content_type: ContentType::Json,
            payload: vec![9; 1024],
        };
        let mut writer = Writer::open(&source_path).unwrap();
        let second_receipt = writer
            .append_synced(EnvelopeIn {
                source_id: second.source_id.clone(),
                schema_version: second.schema_version,
                parser_version: second.parser_version,
                observed_at: second.observed_at.clone(),
                received_at: second.received_at.clone(),
                content_type: second.content_type.clone(),
                payload: second.payload.clone(),
            })
            .unwrap();
        source_receipts
            .record_synced_append(second_receipt, &second)
            .unwrap();

        assert!(source_receipts.retained_frame_metadata_bytes() < first_payload.len());
        assert_eq!(held.payload.as_ptr(), held_pointer);
        assert_eq!(held.payload, first_payload);
        assert_eq!(
            source_receipts
                .source_envelope(second_receipt)
                .unwrap()
                .payload,
            second.payload
        );
        assert_eq!(source_receipts.snapshot().len(), 2);
    }

    /// PASS: synchronization catch-up reads only from the retained next-frame offset, so damage
    /// deliberately injected into an already indexed frame makes full replay fail but does not
    /// prevent the independently hash-bound new tail frame from being indexed.
    /// FAIL: catch-up rereads the corrupted indexed prefix, skips the hash-bound suffix, or
    /// leaves the suffix unaddressable.
    #[test]
    fn source_receipt_index_catch_up_never_rereads_indexed_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let source_path = dir.path().join("source.log");
        let at = OffsetDateTime::from_unix_timestamp(10).unwrap();
        let make_envelope = |payload: &[u8]| EnvelopeIn {
            source_id: SourceId("catch-up-source".to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        };
        let mut writer = Writer::open(&source_path).unwrap();
        writer.append_synced(make_envelope(b"first")).unwrap();
        writer.append_synced(make_envelope(b"second")).unwrap();
        drop(writer);

        let first_frame_offset = pe_event_log::Reader::replay_with_offsets(&source_path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let mut writer = Writer::open(&source_path).unwrap();
        let tail_receipt = writer.append_synced(make_envelope(b"tail")).unwrap();
        drop(writer);

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&source_path)
            .unwrap();
        file.seek(std::io::SeekFrom::Start(first_frame_offset))
            .unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xff;
        file.seek(std::io::SeekFrom::Start(first_frame_offset))
            .unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
        drop(file);

        assert!(pe_event_log::Reader::replay(&source_path).is_err());
        source_receipts.catch_up_verified_tail().unwrap();
        assert_eq!(source_receipts.snapshot().len(), 3);
        assert_eq!(
            source_receipts
                .source_envelope(tail_receipt)
                .unwrap()
                .payload,
            b"tail"
        );
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

    /// PASS: each active cause owned by a different live account applies its strategy-wide field
    /// clamp to a healthy paper snapshot.
    #[test]
    fn global_halt_overlay_applies_every_foreign_owner_cause() {
        let cases = [
            (
                RiskHaltCause::AbsoluteLoss,
                pe_risk_engine::RiskBlock::KillSwitchDrawdown,
            ),
            (
                RiskHaltCause::IntradayDrawdown,
                pe_risk_engine::RiskBlock::IntradayDrawdownStop,
            ),
            (
                RiskHaltCause::Rolling7dDrawdown,
                pe_risk_engine::RiskBlock::Rolling7dDrawdownStop,
            ),
            (
                RiskHaltCause::CopyLatency,
                pe_risk_engine::RiskBlock::CopyLatencyKillSwitch,
            ),
        ];

        for (index, (cause, expected)) in cases.into_iter().enumerate() {
            let account_id = format!("live-{index}");
            let owner = RiskHaltOwner::LiveAccount(AccountId::new(account_id.as_str()).unwrap());
            let active = HashSet::from([(owner, cause)]);
            let mut snapshot = RiskSnapshot {
                leader_exposure_bps: BasisPoints::ZERO,
                market_exposure_bps: BasisPoints::ZERO,
                family_exposure_bps: BasisPoints::ZERO,
                total_copy_exposure_bps: BasisPoints::ZERO,
                intraday_pnl_bps: BasisPoints::ZERO,
                rolling_7d_pnl_bps: BasisPoints::ZERO,
                absolute_pnl_bps: BasisPoints::ZERO,
                copy_latency_kill_switch_active: false,
                proposed_trade_bps: BasisPoints(1),
                per_trade_cap_bps: 25,
                drawdown_policy: pe_risk_engine::DrawdownPolicy::Stops,
                concentration_caps: None,
            };

            apply_global_risk_halts(&active, &mut snapshot, true);

            assert_eq!(
                pe_risk_engine::evaluate_risk(&snapshot),
                pe_risk_engine::RiskDecision::Blocked(expected)
            );
        }
    }

    #[test]
    fn kill_only_overlay_ignores_recorded_day_week_halts_and_uses_its_loss_threshold() {
        let mut snapshot = RiskSnapshot {
            leader_exposure_bps: pe_core_types::BasisPoints::ZERO,
            market_exposure_bps: pe_core_types::BasisPoints::ZERO,
            family_exposure_bps: pe_core_types::BasisPoints::ZERO,
            total_copy_exposure_bps: pe_core_types::BasisPoints::ZERO,
            intraday_pnl_bps: pe_core_types::BasisPoints::ZERO,
            rolling_7d_pnl_bps: pe_core_types::BasisPoints::ZERO,
            absolute_pnl_bps: pe_core_types::BasisPoints::ZERO,
            copy_latency_kill_switch_active: false,
            proposed_trade_bps: pe_core_types::BasisPoints(1),
            per_trade_cap_bps: 10000,
            drawdown_policy: pe_risk_engine::DrawdownPolicy::KillOnly,
            concentration_caps: None,
        };
        let stops = HashSet::from([
            (RiskHaltOwner::Paper, RiskHaltCause::IntradayDrawdown),
            (RiskHaltOwner::Paper, RiskHaltCause::Rolling7dDrawdown),
        ]);
        let before = snapshot.clone();
        apply_global_risk_halts(&stops, &mut snapshot, false);
        assert_eq!(snapshot, before);
        assert_eq!(
            pe_risk_engine::evaluate_risk(&snapshot),
            pe_risk_engine::RiskDecision::Approved
        );
        apply_global_risk_halts(
            &HashSet::from([(RiskHaltOwner::Paper, RiskHaltCause::AbsoluteLoss)]),
            &mut snapshot,
            false,
        );
        assert_eq!(snapshot.absolute_pnl_bps.0, -3000);
        assert_eq!(
            pe_risk_engine::evaluate_risk(&snapshot),
            pe_risk_engine::RiskDecision::Blocked(pe_risk_engine::RiskBlock::KillSwitchDrawdown)
        );
    }

    /// Historical Stops snapshots ignore retired latency causes and retain the other halts.
    #[test]
    fn retired_latency_overlay_keeps_other_single_cause_halts() {
        let cases = [
            (RiskHaltCause::CopyLatency, None),
            (
                RiskHaltCause::AbsoluteLoss,
                Some(pe_risk_engine::RiskBlock::KillSwitchDrawdown),
            ),
            (
                RiskHaltCause::IntradayDrawdown,
                Some(pe_risk_engine::RiskBlock::IntradayDrawdownStop),
            ),
            (
                RiskHaltCause::Rolling7dDrawdown,
                Some(pe_risk_engine::RiskBlock::Rolling7dDrawdownStop),
            ),
        ];
        for (cause, blocked) in cases {
            let mut snapshot = RiskSnapshot {
                leader_exposure_bps: BasisPoints::ZERO,
                market_exposure_bps: BasisPoints::ZERO,
                family_exposure_bps: BasisPoints::ZERO,
                total_copy_exposure_bps: BasisPoints::ZERO,
                intraday_pnl_bps: BasisPoints::ZERO,
                rolling_7d_pnl_bps: BasisPoints::ZERO,
                absolute_pnl_bps: BasisPoints::ZERO,
                copy_latency_kill_switch_active: false,
                proposed_trade_bps: BasisPoints(1),
                per_trade_cap_bps: 25,
                drawdown_policy: pe_risk_engine::DrawdownPolicy::Stops,
                concentration_caps: None,
            };
            apply_global_risk_halts(
                &HashSet::from([(RiskHaltOwner::Paper, cause)]),
                &mut snapshot,
                false,
            );
            assert!(!snapshot.copy_latency_kill_switch_active);
            assert_eq!(
                pe_risk_engine::evaluate_risk(&snapshot),
                blocked.map_or(
                    pe_risk_engine::RiskDecision::Approved,
                    pe_risk_engine::RiskDecision::Blocked,
                )
            );
        }
    }

    #[test]
    fn paper_snapshot_selects_latency_by_recorded_semantic() {
        let start_frame = frame(
            1,
            0,
            PaperLogRecord::QualificationStarted(Arc::new(start())),
        );
        let era = crate::paper_recovery::paper_era(vec![start_frame.clone()]);
        let financial = FinancialSnapshot {
            cash: dec!(100),
            positions: Vec::new(),
            settlements_7d: Vec::new(),
            fills_for_open_and_7d: Vec::new(),
            last_prepared_seq: None,
            start: Some((start_frame.receipt.sequence, start_frame.receipt.this_hash)),
        };
        let base = PaperExposureBase {
            leader_exposure_bps: BasisPoints::ZERO,
            market_exposure_bps: BasisPoints::ZERO,
            family_exposure_bps: BasisPoints::ZERO,
            total_copy_exposure_bps: BasisPoints::ZERO,
            proposed_trade_bps: BasisPoints(1),
            per_trade_cap_bps: 25,
        };
        let build = |semantic| {
            build_paper_risk_snapshot_from_source_receipts(
                &base,
                &financial,
                &era,
                &HashMap::new(),
                |_| Err(RiskInputsUnavailable::PriceMissing),
                10_800,
                true,
                semantic,
            )
            .unwrap()
        };
        assert!(build(1).copy_latency_kill_switch_active);
        assert!(!build(2).copy_latency_kill_switch_active);
    }

    /// PASS: an absolute-loss engagement remains effective after raw finances recover because the
    /// durable active set, not the recovered raw PnL, owns the manual latch.
    #[test]
    fn global_halt_overlay_preserves_manually_latched_recovered_cause() {
        let era = PaperEra {
            start: None,
            frames: vec![halt(
                1,
                RiskHaltOwner::Paper,
                RiskHaltCause::AbsoluteLoss,
                HaltState::Engaged,
            )],
        };
        let mut recovered = RiskSnapshot {
            leader_exposure_bps: BasisPoints::ZERO,
            market_exposure_bps: BasisPoints::ZERO,
            family_exposure_bps: BasisPoints::ZERO,
            total_copy_exposure_bps: BasisPoints::ZERO,
            intraday_pnl_bps: BasisPoints::ZERO,
            rolling_7d_pnl_bps: BasisPoints::ZERO,
            absolute_pnl_bps: BasisPoints::ZERO,
            copy_latency_kill_switch_active: false,
            proposed_trade_bps: BasisPoints(1),
            per_trade_cap_bps: 25,
            drawdown_policy: pe_risk_engine::DrawdownPolicy::Stops,
            concentration_caps: None,
        };

        apply_global_risk_halts(&active_risk_halts(&era), &mut recovered, true);

        assert_eq!(
            pe_risk_engine::evaluate_risk(&recovered),
            pe_risk_engine::RiskDecision::Blocked(pe_risk_engine::RiskBlock::KillSwitchDrawdown)
        );
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
            audited_halt_release(&era, &active, stale.receipt.this_hash.to_hex().as_str())
                .is_none()
        );
        assert!(
            audited_halt_release(&era, &active, intraday.receipt.this_hash.to_hex().as_str())
                .is_none()
        );
        assert!(
            audited_halt_release(&era, &active, absolute.receipt.this_hash.to_hex().as_str())
                .is_none()
        );
        assert_eq!(
            audited_halt_release(
                &era,
                &active,
                newer_absolute.receipt.this_hash.to_hex().as_str(),
            )
            .unwrap()
            .cause,
            RiskHaltCause::AbsoluteLoss
        );
        let latency_release = audited_halt_release(
            &era,
            &active,
            starved_latency.receipt.this_hash.to_hex().as_str(),
        )
        .unwrap();
        assert_eq!(latency_release.cause, RiskHaltCause::CopyLatency);
        assert_eq!(latency_release.owner, account());
    }

    /// #745: the retained receipt window answers every index method exactly as the dense
    /// projection it replaces, reading retired receipts from the sidecar.
    mod receipt_window {
        use super::*;

        type Outcome<T> = Result<T, RiskInputsUnavailable>;
        type Rows = Vec<(AppendReceipt, i64, Option<u64>)>;

        fn unix_at(sequence: u64) -> i64 {
            // Receive times wander backwards and forwards, as ingestion order allows.
            let wander = i64::try_from((sequence * 7) % 5).unwrap();
            10_000 + i64::try_from(sequence).unwrap() + 3 * wander
        }

        fn payload_at(sequence: u64) -> Vec<u8> {
            format!("{{\"frame\":{sequence}}}").into_bytes()
        }

        fn append_frames(path: &Path, sequences: std::ops::Range<u64>) -> Vec<AppendReceipt> {
            let mut writer = Writer::open(path).unwrap();
            sequences
                .map(|sequence| {
                    writer
                        .append_synced(source_input(unix_at(sequence), &payload_at(sequence)))
                        .unwrap()
                })
                .collect()
        }

        fn tail_of(path: &Path, frames: &[SourceFrameMetadata]) -> LogTailBinding {
            LogTailBinding {
                path: std::fs::canonicalize(path).unwrap(),
                physical_tail: std::fs::metadata(path).unwrap().len(),
                last_sequence: frames.last().map(|frame| frame.receipt.sequence),
                last_hash: frames
                    .last()
                    .map_or(blake3::Hash::from_bytes([0; 32]), |frame| {
                        frame.receipt.this_hash
                    }),
            }
        }

        /// An authority retiring `[0, boundary)` with `pins`, over `frames` ending at `tail`.
        fn authority_over(
            frames: &[SourceFrameMetadata],
            tail: &LogTailBinding,
            boundary: u64,
            pins: &[u64],
            epoch: u64,
        ) -> RetentionAuthority {
            let at = |sequence: u64| &frames[usize::try_from(sequence).unwrap()];
            RetentionAuthority {
                format_version: 1,
                epoch,
                advanced_at: 1,
                boundary: pe_event_log::RetentionBoundary {
                    sequence: EventSeq(boundary),
                    offset: at(boundary).byte_offset.unwrap(),
                },
                chain_head: at(boundary - 1).receipt.this_hash,
                pins: pins
                    .iter()
                    .map(|&sequence| pe_event_log::RetentionPin {
                        sequence: EventSeq(sequence),
                        offset: at(sequence).byte_offset.unwrap(),
                        hash: at(sequence).receipt.this_hash,
                        predecessor_hash: sequence
                            .checked_sub(1)
                            .map_or(blake3::Hash::from_bytes([0; 32]), |previous| {
                                at(previous).receipt.this_hash
                            }),
                        reducer: false,
                        wallet: None,
                    })
                    .collect(),
                retained_tail: pe_event_log::TailBinding::from(tail),
                feed: Vec::new(),
            }
        }

        /// Every receipt of the log at `path`, after writing its dense sidecar.
        fn dense_frames_with_sidecar(path: &Path) -> Vec<SourceFrameMetadata> {
            let dense = SourceReceiptIndex::replay(path).unwrap();
            dense.write_receipt_sidecar_for_test().unwrap();
            let tail = dense.current_tail_binding().unwrap();
            let count = tail.last_sequence.map_or(0, |last| last.0 + 1);
            dense
                .checkpoint_suffix(0, usize::try_from(count).unwrap(), &tail)
                .unwrap()
        }

        struct WindowFixture {
            _dir: tempfile::TempDir,
            path: PathBuf,
            frames: Vec<SourceFrameMetadata>,
        }

        impl WindowFixture {
            fn new(count: u64) -> Self {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("source.log");
                append_frames(&path, 0..count);
                let frames = dense_frames_with_sidecar(&path);
                Self {
                    _dir: dir,
                    path,
                    frames,
                }
            }

            fn count(&self) -> u64 {
                u64::try_from(self.frames.len()).unwrap()
            }

            fn at(&self, sequence: u64) -> &SourceFrameMetadata {
                &self.frames[usize::try_from(sequence).unwrap()]
            }

            fn tail(&self) -> LogTailBinding {
                tail_of(&self.path, &self.frames)
            }

            fn binding(&self, count: u64) -> LogTailBinding {
                LogTailBinding {
                    last_sequence: count.checked_sub(1).map(EventSeq),
                    last_hash: count
                        .checked_sub(1)
                        .map_or(blake3::Hash::from_bytes([0; 32]), |last| {
                            self.at(last).receipt.this_hash
                        }),
                    ..self.tail()
                }
            }

            fn authority(&self, boundary: u64, pins: &[u64], epoch: u64) -> RetentionAuthority {
                authority_over(&self.frames, &self.tail(), boundary, pins, epoch)
            }

            /// The pre-#745 layout: every receipt in memory, with the authority installed.
            fn dense(&self, authority: &RetentionAuthority) -> SourceReceiptIndex {
                SourceReceiptIndex {
                    state: Arc::new(RwLock::new(SourceReceiptIndexState {
                        frames: self.frames.clone(),
                        retention: Some(Arc::new(authority.clone())),
                        next_byte_offset: Some(std::fs::metadata(&self.path).unwrap().len()),
                        ..SourceReceiptIndexState::default()
                    })),
                    source_log_path: Some(Arc::new(std::fs::canonicalize(&self.path).unwrap())),
                }
            }

            /// The window reached by a live retention switch.
            fn switched(&self, authority: &RetentionAuthority) -> SourceReceiptIndex {
                let index = SourceReceiptIndex::replay(&self.path).unwrap();
                index.install_retention(authority.clone()).unwrap();
                index
            }

            /// The window reached by booting a log whose authority is committed.
            fn booted(&self, authority: &RetentionAuthority) -> SourceReceiptIndex {
                authority.write(&self.path).unwrap();
                let index = SourceReceiptIndex::replay(&self.path).unwrap();
                std::fs::remove_file(pe_event_log::retention_path(&self.path)).unwrap();
                index
            }

            fn rewrite_record(&self, sequence: u64, frame: &SourceFrameMetadata) {
                use std::os::unix::fs::FileExt as _;
                let record = pe_event_log::ReceiptRecord {
                    receipt: frame.receipt,
                    received_millis: frame.received_millis,
                    byte_offset: frame.byte_offset,
                };
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(crate::source_checkpoint::receipts_path(&self.path))
                    .unwrap()
                    .write_all_at(
                        &record.encode(),
                        sequence * pe_event_log::RECEIPT_RECORD_LEN,
                    )
                    .unwrap();
            }
        }

        fn rows(frames: Outcome<Vec<SourceFrameMetadata>>) -> Outcome<Rows> {
            frames.map(|frames| frames.iter().map(row).collect())
        }

        fn row(frame: &SourceFrameMetadata) -> (AppendReceipt, i64, Option<u64>) {
            (frame.receipt, frame.received_millis, frame.byte_offset)
        }

        fn envelope(outcome: Outcome<EventEnvelope>) -> Outcome<(blake3::Hash, Vec<u8>)> {
            outcome.map(|envelope| (envelope.this_hash, envelope.payload))
        }

        /// Every index method, over every sequence and range, on one projection.
        fn observe(index: &SourceReceiptIndex, fixture: &WindowFixture) -> Vec<String> {
            let count = fixture.count();
            let mut observed = Vec::new();
            for sequence in 0..count + 2 {
                observed.push(format!(
                    "receipt_at {sequence} {:?}",
                    index.receipt_at(EventSeq(sequence))
                ));
            }
            for frame in &fixture.frames {
                let receipt = frame.receipt;
                observed.push(format!(
                    "received {:?} {:?}",
                    receipt.sequence,
                    index.received_millis(receipt)
                ));
                observed.push(format!(
                    "envelope {:?} {:?}",
                    receipt.sequence,
                    envelope(index.source_envelope(receipt))
                ));
                let wrong = AppendReceipt {
                    sequence: receipt.sequence,
                    this_hash: blake3::Hash::from_bytes([9; 32]),
                };
                observed.push(format!(
                    "wrong {:?} {:?}",
                    receipt.sequence,
                    index.received_millis(wrong)
                ));
                let sequence = receipt.sequence.0;
                let input = source_input(unix_at(sequence), &payload_at(sequence));
                observed.push(format!(
                    "append {sequence} {:?}",
                    index.record_synced_append(receipt, &input)
                ));
                let moved = source_input(unix_at(sequence) + 1, &payload_at(sequence));
                observed.push(format!(
                    "append moved {sequence} {:?}",
                    index.record_synced_append(receipt, &moved)
                ));
            }
            for count in 0..=count {
                for start in 0..=count + 1 {
                    let suffix = index.checkpoint_suffix(
                        usize::try_from(start).unwrap(),
                        usize::try_from(count).unwrap(),
                        &fixture.binding(count),
                    );
                    observed.push(format!("suffix {start}..{count} {:?}", rows(suffix)));
                }
                let stale = LogTailBinding {
                    last_hash: blake3::Hash::from_bytes([8; 32]),
                    ..fixture.binding(count)
                };
                observed.push(format!(
                    "stale suffix {count} {:?}",
                    rows(index.checkpoint_suffix(0, usize::try_from(count).unwrap(), &stale))
                ));
            }
            observed.push(format!(
                "past the tail {:?}",
                rows(index.checkpoint_suffix(
                    0,
                    usize::try_from(count + 1).unwrap(),
                    &fixture.binding(count)
                ))
            ));
            observed.push(format!("tail {:?}", index.current_tail_binding()));
            observed.push(format!("tail frame {:?}", index.tail_with_last_frame()));
            observed.push(format!("pins {:?}", index.verify_retention_pins()));
            observed.push(format!("epoch {:?}", index.retention_epoch()));
            observed.push(format!("catch up {:?}", index.catch_up_verified_tail()));
            observed.push(format!("snapshot {:?}", index.snapshot()));
            observed
        }

        /// The same observation derived from the fixture's metadata alone, independent of any index.
        fn expected(fixture: &WindowFixture, authority: &RetentionAuthority) -> Vec<String> {
            let count = fixture.count();
            let boundary = authority.boundary.sequence.0;
            let mut expected = Vec::new();
            for sequence in 0..count + 2 {
                let stored: Outcome<Option<(AppendReceipt, i64)>> = Ok(fixture
                    .frames
                    .get(usize::try_from(sequence).unwrap())
                    .map(|frame| (frame.receipt, frame.received_millis)));
                expected.push(format!("receipt_at {sequence} {stored:?}"));
            }
            for frame in &fixture.frames {
                let receipt = frame.receipt;
                let sequence = receipt.sequence.0;
                let millis: Outcome<i64> = Ok(frame.received_millis);
                expected.push(format!("received {:?} {millis:?}", receipt.sequence));
                let read: Outcome<(blake3::Hash, Vec<u8>)> =
                    if sequence >= boundary || authority.pin(receipt.sequence).is_some() {
                        Ok((receipt.this_hash, payload_at(sequence)))
                    } else {
                        Err(RiskInputsUnavailable::Erased)
                    };
                expected.push(format!("envelope {:?} {read:?}", receipt.sequence));
                let missing: Outcome<i64> = Err(RiskInputsUnavailable::PriceMissing);
                expected.push(format!("wrong {:?} {missing:?}", receipt.sequence));
                let same: Outcome<()> = Ok(());
                expected.push(format!("append {sequence} {same:?}"));
                let conflict: Outcome<()> = Err(RiskInputsUnavailable::PriceConflict);
                expected.push(format!("append moved {sequence} {conflict:?}"));
            }
            let refused: Outcome<Rows> = Err(RiskInputsUnavailable::PriceConflict);
            for count in 0..=count {
                for start in 0..=count + 1 {
                    let suffix: Outcome<Rows> = if start <= count {
                        Ok(fixture.frames
                            [usize::try_from(start).unwrap()..usize::try_from(count).unwrap()]
                            .iter()
                            .map(row)
                            .collect())
                    } else {
                        Err(RiskInputsUnavailable::PriceConflict)
                    };
                    expected.push(format!("suffix {start}..{count} {suffix:?}"));
                }
                expected.push(format!("stale suffix {count} {refused:?}"));
            }
            expected.push(format!("past the tail {refused:?}"));
            let tail: Outcome<LogTailBinding> = Ok(fixture.tail());
            expected.push(format!("tail {tail:?}"));
            let last = fixture.frames.last().unwrap();
            let previous = fixture.frames[fixture.frames.len() - 2].receipt.this_hash;
            let tail_frame: Outcome<(LogTailBinding, u64, AppendReceipt, blake3::Hash)> = Ok((
                fixture.tail(),
                last.byte_offset.unwrap(),
                last.receipt,
                previous,
            ));
            expected.push(format!("tail frame {tail_frame:?}"));
            let fine: Outcome<()> = Ok(());
            expected.push(format!("pins {fine:?}"));
            expected.push(format!("epoch {:?}", authority.epoch));
            expected.push(format!("catch up {fine:?}"));
            let snapshot = fixture
                .frames
                .iter()
                .map(|frame| {
                    (
                        frame.receipt.sequence,
                        (frame.receipt, frame.received_millis),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            expected.push(format!("snapshot {snapshot:?}"));
            expected
        }

        fn assert_lines(expected: &[String], observed: &[String], context: &str) {
            for (expected, observed) in expected.iter().zip(observed) {
                assert_eq!(expected, observed, "{context}");
            }
            assert_eq!(expected.len(), observed.len(), "{context}");
        }

        /// PASS: boot and live switching both yield an exactly sized window whose every method
        /// answers what the fixture's own metadata dictates (and what the dense layout answers),
        /// including pins just below the boundary, a pin at sequence zero, a one-frame window and
        /// no pins. FAIL: any differing answer, or a window holding retired receipts or spare
        /// capacity.
        #[test]
        fn window_methods_match_the_dense_layout() {
            let fixture = WindowFixture::new(14);
            for (boundary, pins) in [
                (9, vec![2, 6, 7, 8]),
                (13, vec![0, 12]),
                (1, vec![0]),
                (5, Vec::new()),
            ] {
                let authority = fixture.authority(boundary, &pins, 1);
                let expected = expected(&fixture, &authority);
                assert_lines(
                    &expected,
                    &observe(&fixture.dense(&authority), &fixture),
                    "dense",
                );
                for (path, index) in [
                    ("switched", fixture.switched(&authority)),
                    ("booted", fixture.booted(&authority)),
                ] {
                    let window = usize::try_from(fixture.count() - boundary).unwrap();
                    assert_eq!(
                        index.receipt_window_for_scenario(),
                        (boundary, window, window),
                        "{path} {boundary}"
                    );
                    let context = format!("{path} boundary {boundary}");
                    assert_lines(&expected, &observe(&index, &fixture), &context);
                    for audited in boundary..=fixture.count() {
                        let audit = index
                            .retention_audit(EventSeq(audited), &fixture.tail())
                            .unwrap();
                        let mut commitment = ReceiptCommitment::default();
                        for frame in &fixture.frames[..usize::try_from(boundary).unwrap()] {
                            commitment.push(frame).unwrap();
                        }
                        assert!(audit.commitment.matches(&commitment));
                        assert_eq!(
                            rows(Ok(audit.window)).unwrap(),
                            fixture.frames[usize::try_from(boundary).unwrap()
                                ..usize::try_from(audited).unwrap()]
                                .iter()
                                .map(row)
                                .collect::<Rows>()
                        );
                    }
                    assert!(
                        index
                            .retention_audit(EventSeq(boundary - 1), &fixture.tail())
                            .is_err()
                    );
                }
            }
        }

        /// PASS: after retirement a newly synchronized append extends the window at the next
        /// absolute sequence and reads back exactly; a gap is still refused.
        /// FAIL: a refused append, a misnumbered receipt or an accepted gap.
        #[test]
        fn fresh_appends_extend_the_window_after_retirement() {
            for path in ["switched", "booted"] {
                let fixture = WindowFixture::new(10);
                let authority = fixture.authority(7, &[3], 1);
                let index = if path == "switched" {
                    fixture.switched(&authority)
                } else {
                    fixture.booted(&authority)
                };
                let receipt = append_frames(&fixture.path, 10..11)[0];
                let input = source_input(unix_at(10), &payload_at(10));
                index.record_synced_append(receipt, &input).unwrap();
                assert_eq!(receipt.sequence, EventSeq(10), "{path}");
                let (base, len, _) = index.receipt_window_for_scenario();
                assert_eq!((base, len), (7, 4), "{path}");
                assert_eq!(index.receipt_at(EventSeq(10)).unwrap().unwrap().0, receipt);
                assert_eq!(
                    index.source_envelope(receipt).unwrap().payload,
                    payload_at(10)
                );
                assert_eq!(
                    index.current_tail_binding().unwrap().last_sequence,
                    Some(EventSeq(10))
                );
                let gap = AppendReceipt {
                    sequence: EventSeq(12),
                    this_hash: receipt.this_hash,
                };
                assert_eq!(
                    index.record_synced_append(gap, &input).unwrap_err(),
                    RiskInputsUnavailable::PriceConflict
                );
            }
        }

        fn synthetic(received: &[i64]) -> Vec<SourceFrameMetadata> {
            received
                .iter()
                .enumerate()
                .map(|(sequence, &received_millis)| SourceFrameMetadata {
                    receipt: receipt(
                        u64::try_from(sequence).unwrap(),
                        u8::try_from(sequence).unwrap(),
                    ),
                    received_millis,
                    byte_offset: Some(
                        pe_event_log::HEADER_LEN + 100 * u64::try_from(sequence).unwrap(),
                    ),
                })
                .collect()
        }

        fn windowed(frames: &[SourceFrameMetadata], base: usize) -> SourceReceiptIndex {
            let mut retired = RetiredReceipts::default();
            for frame in &frames[..base] {
                retired.retire(frame.clone()).unwrap();
            }
            SourceReceiptIndex {
                state: Arc::new(RwLock::new(SourceReceiptIndexState {
                    frames: frames[base..].to_vec(),
                    retired,
                    next_byte_offset: Some(
                        pe_event_log::HEADER_LEN + 100 * u64::try_from(frames.len()).unwrap(),
                    ),
                    ..SourceReceiptIndexState::default()
                })),
                source_log_path: None,
            }
        }

        /// PASS: over seeded receive-time sequences with ties and reversals, every window base
        /// and cutoffs on, between and beyond the receive times, the window search selects what
        /// the full scan selected, reporting a selection below the base as retired.
        /// FAIL: any other selection.
        #[test]
        fn boundary_search_matches_a_full_scan() {
            let mut state = 0x7465_6e74_u64;
            let mut next = |bound: i64| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                i64::try_from((state >> 33) % u64::try_from(bound).unwrap()).unwrap()
            };
            let mut cases = 0;
            for _ in 0..400 {
                let len = usize::try_from(next(12) + 1).unwrap();
                let received = (0..len).map(|_| 1_000 + 10 * next(6)).collect::<Vec<_>>();
                let frames = synthetic(&received);
                let tail = LogTailBinding {
                    path: PathBuf::new(),
                    physical_tail: 0,
                    last_sequence: frames.last().map(|frame| frame.receipt.sequence),
                    last_hash: frames.last().unwrap().receipt.this_hash,
                };
                for base in 0..len {
                    let index = windowed(&frames, base);
                    for cutoff in (990..=1_070).step_by(5) {
                        let scanned = frames
                            .iter()
                            .find(|frame| frame.received_millis >= cutoff)
                            .or_else(|| frames.last())
                            .unwrap();
                        let selected = index.retention_boundary_candidate(&tail, cutoff).unwrap();
                        match selected {
                            BoundaryCandidate::Retired { base: reported } => {
                                assert_eq!(reported, u64::try_from(base).unwrap());
                                assert!(scanned.receipt.sequence.0 < reported);
                            }
                            BoundaryCandidate::Frame(frame) => {
                                assert_eq!(frame.receipt, scanned.receipt);
                                assert!(frame.receipt.sequence.0 >= u64::try_from(base).unwrap());
                            }
                            BoundaryCandidate::Empty => unreachable!("frames exist"),
                        }
                        cases += 1;
                    }
                }
            }
            assert!(cases > 10_000);
            let empty = windowed(&[], 0);
            let genesis = LogTailBinding {
                path: PathBuf::new(),
                physical_tail: pe_event_log::HEADER_LEN,
                last_sequence: None,
                last_hash: blake3::Hash::from_bytes([0; 32]),
            };
            assert!(matches!(
                empty.retention_boundary_candidate(&genesis, 0).unwrap(),
                BoundaryCandidate::Empty
            ));
        }

        /// PASS: after boot and after a live switch alike, once the sidecar is lost or damaged
        /// each method needing a retired receipt returns its missing-receipt error, while
        /// receipts in memory still answer. FAIL: a silent answer, a different error, or a
        /// failure for an in-memory receipt.
        #[test]
        fn retired_read_failures_fail_closed() {
            for path in ["switched", "booted"] {
                let fixture = WindowFixture::new(14);
                let authority = fixture.authority(9, &[2, 4, 8], 1);
                let index = if path == "switched" {
                    fixture.switched(&authority)
                } else {
                    fixture.booted(&authority)
                };
                let pin = |sequence: u64| fixture.at(sequence).receipt;
                let sidecar = crate::source_checkpoint::receipts_path(&fixture.path);
                let intact = std::fs::read(&sidecar).unwrap();

                // A damaged predecessor or successor of a pin is a missing receipt for that pin.
                for (damaged, pinned) in [(1, 2), (3, 2), (3, 4), (5, 4)] {
                    let mut bytes = intact.clone();
                    bytes[usize::try_from(damaged * pe_event_log::RECEIPT_RECORD_LEN + 50)
                        .unwrap()] ^= 1;
                    std::fs::write(&sidecar, &bytes).unwrap();
                    assert_eq!(
                        index.source_envelope(pin(pinned)).unwrap_err(),
                        RiskInputsUnavailable::PriceMissing,
                        "{path} damaged {damaged}"
                    );
                }
                std::fs::write(&sidecar, &intact).unwrap();
                assert!(index.verify_retention_pins().is_ok(), "{path}");

                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&sidecar)
                    .unwrap()
                    .set_len(0)
                    .unwrap();
                let missing = Some(RiskInputsUnavailable::PriceMissing);
                assert_eq!(index.receipt_at(EventSeq(4)).err(), missing, "{path}");
                assert_eq!(index.received_millis(pin(4)).err(), missing, "{path}");
                assert_eq!(index.source_envelope(pin(2)).err(), missing, "{path}");
                assert_eq!(index.verify_retention_pins().err(), missing, "{path}");
                assert_eq!(
                    index.checkpoint_suffix(0, 9, &fixture.binding(9)).err(),
                    missing,
                    "{path}"
                );
                assert_eq!(
                    index.checkpoint_suffix(4, 5, &fixture.binding(5)).err(),
                    missing,
                    "{path}"
                );
                let input = source_input(unix_at(4), &payload_at(4));
                assert_eq!(
                    index.record_synced_append(pin(4), &input).err(),
                    missing,
                    "{path}"
                );
                // The two highest retired receipts and the window stay answerable from memory.
                for sequence in [7, 8, 9, 13] {
                    assert_eq!(
                        index.receipt_at(EventSeq(sequence)).unwrap(),
                        Some((pin(sequence), fixture.at(sequence).received_millis)),
                        "{path}"
                    );
                }
                assert_eq!(
                    envelope(index.source_envelope(pin(8))).unwrap().1,
                    payload_at(8)
                );
                assert_eq!(
                    index.source_envelope(pin(5)).unwrap_err(),
                    RiskInputsUnavailable::Erased
                );
                assert!(index.checkpoint_suffix(9, 14, &fixture.binding(14)).is_ok());
                assert!(index.current_tail_binding().is_ok());
                assert!(index.tail_with_last_frame().is_ok());
            }
        }

        /// PASS: a second switch landing between an envelope read's lookup and its physical read
        /// leaves a retired pin readable from the sidecar, erases a window frame the new authority
        /// does not pin, and keeps a newly pinned frame readable with its successor's end checked.
        /// FAIL: any other outcome.
        #[cfg(feature = "scenario")]
        #[test]
        fn envelope_reads_stay_exact_when_a_switch_lands_mid_read() {
            for sequence in [2, 8, 9, 10, 12] {
                let fixture = WindowFixture::new(14);
                let index = fixture.switched(&fixture.authority(6, &[2, 4, 5], 1));
                let second = fixture.authority(11, &[2, 4, 5, 9, 10], 2);
                let read =
                    index.source_envelope_with_pause(fixture.at(sequence).receipt, &mut || {
                        index.install_retention(second.clone()).unwrap();
                    });
                let expected = if sequence == 8 {
                    Err(RiskInputsUnavailable::Erased)
                } else {
                    Ok(payload_at(sequence))
                };
                assert_eq!(
                    read.map(|envelope| envelope.payload),
                    expected,
                    "{sequence}"
                );
                assert_eq!(index.receipt_window_for_scenario(), (11, 3, 3));
            }
        }

        /// PASS: when the read frame was the tail at lookup, and frames are appended and retired
        /// past it before the physical read, the pin's end is checked against its successor read
        /// from the sidecar only then: an intact successor reads, a successor whose recorded offset
        /// differs is a conflict, and a damaged one is a missing receipt.
        /// FAIL: any other outcome, which would mean the late successor was not read or not checked.
        #[cfg(feature = "scenario")]
        #[test]
        fn a_successor_appended_and_retired_mid_read_is_read_from_the_sidecar() {
            for case in ["intact", "wrong end", "damaged"] {
                let fixture = WindowFixture::new(10);
                let index = SourceReceiptIndex::replay(&fixture.path).unwrap();
                let read = index.source_envelope_with_pause(fixture.at(9).receipt, &mut || {
                    append_frames(&fixture.path, 10..14);
                    index.catch_up_verified_tail().unwrap();
                    // Publication brings the sidecar to the new tail before retention retires it.
                    let grown = dense_frames_with_sidecar(&fixture.path);
                    let tail = tail_of(&fixture.path, &grown);
                    index
                        .install_retention(authority_over(&grown, &tail, 13, &[9], 1))
                        .unwrap();
                    match case {
                        "wrong end" => {
                            let mut moved = grown[10].clone();
                            moved.byte_offset = moved.byte_offset.map(|offset| offset + 1);
                            fixture.rewrite_record(10, &moved);
                        }
                        "damaged" => damage_receipt_record(&fixture.path, 10),
                        _ => {}
                    }
                });
                let expected = match case {
                    "intact" => Ok(payload_at(9)),
                    "wrong end" => Err(RiskInputsUnavailable::PriceConflict),
                    _ => Err(RiskInputsUnavailable::PriceMissing),
                };
                assert_eq!(read.map(|envelope| envelope.payload), expected, "{case}");
                assert_eq!(index.receipt_window_for_scenario(), (13, 1, 1), "{case}");
            }
        }

        /// PASS: a range read stays exact when a retention switch, or an invalidation trim plus a
        /// suffix rewrite of the sidecar, lands between its locked capture and its sidecar reads.
        /// FAIL: any differing or refused range.
        #[cfg(feature = "scenario")]
        #[test]
        fn range_reads_stay_exact_when_a_switch_or_sidecar_rewrite_lands_mid_read() {
            use std::os::unix::fs::FileExt as _;
            let fixture = WindowFixture::new(14);
            let first = fixture.authority(6, &[2, 4, 5], 1);
            let second = fixture.authority(11, &[2, 4, 5, 9, 10], 2);
            let all = Ok(fixture.frames.iter().map(row).collect::<Rows>());
            let index = fixture.switched(&first);
            let read = index.checkpoint_suffix_with_pause(0, 14, &fixture.binding(14), &mut || {
                index.install_retention(second.clone()).unwrap();
            });
            assert_eq!(rows(read), all);
            let retired = fixture.frames[..6].iter().map(row).collect::<Rows>();
            assert_eq!(
                rows(index.checkpoint_suffix(0, 6, &fixture.binding(6))),
                Ok(retired)
            );

            let index = fixture.switched(&first);
            let sidecar = crate::source_checkpoint::receipts_path(&fixture.path);
            let bytes = std::fs::read(&sidecar).unwrap();
            let split = usize::try_from(6 * pe_event_log::RECEIPT_RECORD_LEN).unwrap();
            let read = index.checkpoint_suffix_with_pause(0, 14, &fixture.binding(14), &mut || {
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&sidecar)
                    .unwrap();
                file.set_len(6 * pe_event_log::RECEIPT_RECORD_LEN).unwrap();
                file.write_all_at(&bytes[split..], 6 * pe_event_log::RECEIPT_RECORD_LEN)
                    .unwrap();
            });
            assert_eq!(rows(read), all);

            // The read resumes while the sidecar is still trimmed to the boundary, as between an
            // invalidation and the next publication: only the retired prefix comes from the file.
            let index = fixture.switched(&first);
            let read = index.checkpoint_suffix_with_pause(0, 14, &fixture.binding(14), &mut || {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&sidecar)
                    .unwrap()
                    .set_len(6 * pe_event_log::RECEIPT_RECORD_LEN)
                    .unwrap();
            });
            assert_eq!(rows(read), all);
            std::fs::write(&sidecar, &bytes).unwrap();
        }

        /// PASS: an installer whose dependencies were read against an older window base or epoch
        /// is refused once it holds the guard, while a fresh read installs. FAIL: an install
        /// validated against stale reads.
        #[test]
        fn a_stale_installer_is_refused() {
            let fixture = WindowFixture::new(14);
            let first = fixture.authority(6, &[2, 4, 5], 1);
            let later = fixture.authority(11, &[2, 4, 5, 9, 10], 3);

            // The window moves between the dependency reads and the guard. This authority needs no
            // receipt below the moved window's cache, so only the recheck can refuse it.
            let in_window = fixture.authority(11, &[9, 10], 3);
            let index = SourceReceiptIndex::replay(&fixture.path).unwrap();
            let stale = index
                .retention_dependencies(&in_window, index.retired_reader().ok())
                .unwrap();
            index.install_retention(first.clone()).unwrap();
            assert_eq!(
                index
                    .prepare_resolved_retention_install(in_window.clone(), stale)
                    .err(),
                Some(RiskInputsUnavailable::PriceConflict)
            );

            // Only the epoch moves: the same boundary under a later epoch.
            let stale = index
                .retention_dependencies(&later, index.retired_reader().ok())
                .unwrap();
            let mut renewed = first.clone();
            renewed.epoch = 2;
            index.install_retention(renewed).unwrap();
            assert_eq!(index.receipt_window_for_scenario().0, 6);
            assert_eq!(
                index
                    .prepare_resolved_retention_install(later.clone(), stale)
                    .err(),
                Some(RiskInputsUnavailable::PriceConflict)
            );

            index.install_retention(later).unwrap();
            assert_eq!(index.receipt_window_for_scenario(), (11, 3, 3));
        }

        /// PASS: a retention switch between catch-up's recovery and its extension moves the
        /// window, and catch-up still extends the projection at the right absolute sequences.
        /// FAIL: a spurious conflict or misnumbered receipts.
        #[cfg(feature = "scenario")]
        #[test]
        fn catch_up_extends_across_a_retention_switch() {
            let fixture = WindowFixture::new(10);
            let index = SourceReceiptIndex::replay(&fixture.path).unwrap();
            // Captured before the appends, as a preparation captures its retained tail.
            let authority = fixture.authority(7, &[3], 1);
            let appended = append_frames(&fixture.path, 10..13);
            index
                .catch_up_verified_tail_with_pause(&mut || {
                    index.install_retention(authority.clone()).unwrap();
                })
                .unwrap();
            assert_eq!(index.receipt_window_for_scenario().0, 7);
            assert_eq!(index.receipt_window_for_scenario().1, 6);
            for receipt in appended {
                assert_eq!(
                    index.receipt_at(receipt.sequence).unwrap().unwrap().0,
                    receipt
                );
                assert_eq!(
                    index.source_envelope(receipt).unwrap().payload,
                    payload_at(receipt.sequence.0)
                );
            }
            assert_eq!(
                index.current_tail_binding().unwrap().last_sequence,
                Some(EventSeq(12))
            );
        }

        /// PASS: without retention no sidecar is needed; the first switch reads the sidecar as it
        /// is then, even after an epoch-zero publication recreated it. FAIL: a retired read served
        /// from the replaced file, or a boot that needs a sidecar.
        #[test]
        fn first_switch_reads_the_current_sidecar() {
            let fixture = WindowFixture::new(10);
            let sidecar = crate::source_checkpoint::receipts_path(&fixture.path);
            let intact = std::fs::read(&sidecar).unwrap();
            std::fs::remove_file(&sidecar).unwrap();
            let index = SourceReceiptIndex::replay(&fixture.path).unwrap();
            assert_eq!(index.receipt_window_for_scenario(), (0, 10, 10));
            assert_eq!(
                index.receipt_at(EventSeq(3)).unwrap(),
                Some((fixture.at(3).receipt, fixture.at(3).received_millis))
            );
            // An earlier sidecar with different contents is replaced before retention exists.
            let mut stale = fixture.at(3).clone();
            stale.received_millis += 1;
            std::fs::write(&sidecar, &intact).unwrap();
            fixture.rewrite_record(3, &stale);
            let _held = std::fs::File::open(&sidecar).unwrap();
            std::fs::remove_file(&sidecar).unwrap();
            std::fs::write(&sidecar, &intact).unwrap();
            index
                .install_retention(fixture.authority(7, &[3], 1))
                .unwrap();
            assert_eq!(
                index.receipt_at(EventSeq(3)).unwrap(),
                Some((fixture.at(3).receipt, fixture.at(3).received_millis))
            );
            assert_eq!(
                index
                    .source_envelope(fixture.at(3).receipt)
                    .unwrap()
                    .payload,
                payload_at(3)
            );
        }
    }
}
