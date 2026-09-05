//! Durable paper risk evidence reducers and daily-mark validation (#545).
//!
//! The financial snapshot-to-`RiskSnapshot` composition is intentionally kept out until the
//! exact `pe-paper-state::FinancialSnapshot` interface lands. These reducers are the independent
//! log side of that composition and are usable at boot before producers start.

use std::collections::BTreeMap;

use pe_core_types::{EventSeq, Price};
use pe_event_log::AppendReceipt;
use pe_risk_engine::{RiskHaltCause, latency_switch, nearest_rank_p95};
use pe_source_polymarket_public::ClassifiedPricesHistory;
use pe_strategy_winner_follow::RiskInputsUnavailable;
use rust_decimal::Decimal;

use crate::paper_recovery::{
    FinancialPayload, FinancialResult, HaltState, PaperEra, PaperLogFrame, PaperLogRecord,
    RiskHaltOwner,
};

const SECONDS_PER_HOUR: i64 = 3_600;
const SECONDS_PER_DAY: i64 = 86_400;
const MAX_HISTORICAL_MARK_AGE_SECS: i64 = 120;

/// Strict historical price selected for a causal paper/live daily mark.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoricalMarkPrice {
    pub price: Price,
    pub sample_unix: i64,
    pub receipt: AppendReceipt,
}

/// Select the latest unique sample at-or-before the cutoff from an already-recorded response.
/// The caller owns transport retries and must append the response before calling this function.
pub fn historical_mark_price(
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

/// Replay-derived active risk causes. The log remains the sole durable owner.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActiveRiskHalts {
    active: Vec<(RiskHaltOwner, RiskHaltCause)>,
}

impl ActiveRiskHalts {
    #[must_use]
    pub fn is_active(&self, owner: &RiskHaltOwner, cause: RiskHaltCause) -> bool {
        self.active
            .iter()
            .any(|(candidate_owner, candidate_cause)| {
                candidate_owner == owner && *candidate_cause == cause
            })
    }

    #[must_use]
    pub fn blocks_new_entries(&self) -> bool {
        !self.active.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &(RiskHaltOwner, RiskHaltCause)> {
        self.active.iter()
    }

    fn apply(&mut self, owner: &RiskHaltOwner, cause: RiskHaltCause, state: HaltState) {
        let existing = self
            .active
            .iter()
            .position(|(candidate_owner, candidate_cause)| {
                candidate_owner == owner && *candidate_cause == cause
            });
        match (existing, state) {
            (None, HaltState::Engaged) => self.active.push((owner.clone(), cause)),
            (Some(index), HaltState::Released) => {
                self.active.remove(index);
            }
            (None, HaltState::Released) | (Some(_), HaltState::Engaged) => {}
        }
    }
}

/// Rebuild the sole in-memory halt set from the active qualification era.
#[must_use]
pub fn rebuild_active_risk_halts(era: &PaperEra) -> ActiveRiskHalts {
    let mut active = ActiveRiskHalts::default();
    for frame in &era.frames {
        if let PaperLogFrame::Record(PaperLogRecord::RiskHaltChanged {
            owner,
            cause,
            state,
            ..
        }) = &frame.frame
        {
            active.apply(owner, *cause, *state);
        }
    }
    active
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
    active: &ActiveRiskHalts,
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
        ) || !active.is_active(owner, *cause)
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

/// Prove there is exactly one Final for every Prepared and that the transactional projection's
/// last Prepared matches the log's latest completed Prepared.
pub fn validate_financial_snapshot_sequence(
    era: &PaperEra,
    snapshot_last_prepared: Option<EventSeq>,
) -> Result<Option<EventSeq>, RiskInputsUnavailable> {
    let mut prepared = BTreeMap::new();
    let mut completed = BTreeMap::<EventSeq, usize>::new();
    for frame in &era.frames {
        match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { .. }) => {
                prepared.insert(frame.receipt.sequence, frame.receipt.this_hash);
            }
            PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt, ..
            }) => {
                if prepared.get(&prepared_receipt.sequence) != Some(&prepared_receipt.this_hash) {
                    return Err(RiskInputsUnavailable::UnmatchedPrepared);
                }
                let count = completed.entry(prepared_receipt.sequence).or_default();
                *count = count
                    .checked_add(1)
                    .ok_or(RiskInputsUnavailable::Overflow)?;
            }
            _ => {}
        }
    }
    if prepared
        .keys()
        .any(|sequence| completed.get(sequence).copied() != Some(1))
    {
        return Err(RiskInputsUnavailable::UnmatchedPrepared);
    }
    let latest = completed.last_key_value().map(|(sequence, _)| *sequence);
    if latest != snapshot_last_prepared {
        return Err(RiskInputsUnavailable::SnapshotSequenceMismatch);
    }
    Ok(latest)
}

/// Resolve the immediately preceding required UTC-midnight equity. Before the first completed
/// midnight following Start, `None` means the fixed starting bankroll is the baseline.
pub fn preceding_midnight_equity(
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
        || mark.equity <= Decimal::ZERO
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
pub fn paper_latency_samples(
    era: &PaperEra,
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
    let all_samples = paper_latency_endpoint_samples(era)?;
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
        let latency_ms = final_ms
            .checked_sub(observation.observed_unix_ms)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or(RiskInputsUnavailable::Overflow)?;
        samples.push((final_unix, latency_ms));
    }
    Ok(samples)
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
        let active = rebuild_active_risk_halts(&era);
        assert!(active.blocks_new_entries());
        assert!(active.is_active(&RiskHaltOwner::Paper, RiskHaltCause::AbsoluteLoss));
        assert!(!active.is_active(&account(), RiskHaltCause::CopyLatency));
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
        let active = rebuild_active_risk_halts(&era);
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
