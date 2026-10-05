//! Shared authentication and semantic conclusions for admitted frame audits.
use pe_core_types::{MarketId, MarketOutcomeId, ShareAmount, Side, SourceTradeId, VenueMarketId};
use pe_event_log::AppendReceipt;
use pe_source_polymarket_public::{ActivityAggregate, ActivityTradeObservation, ActivityType};

use crate::bucket_commit::{CompleteActivityPage, DecisionContinuationV3, VerifiedCommitment};
use crate::paper_recovery::{
    FeedIncident, FeedIncidentCause, HaltState, PaperEra, PaperLogFrame, PaperLogRecord,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditDisposition {
    Matched(SourceTradeId),
    Contradicted(SourceTradeId),
    Absent,
    Unresolved,
}

#[derive(Debug, thiserror::Error)]
pub enum FeedAuditError {
    #[error("frame audit source proof: {0}")]
    Proof(#[from] crate::bucket_commit::CompleteActivityReadError),
    #[error("frame audit continuation: {0}")]
    Continuation(#[from] crate::bucket_commit::DecisionContinuationError),
    #[error("frame audit encoding: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame audit semantic refusal: {0}")]
    Semantic(&'static str),
}

/// Transaction discovery precedes side comparison. An asset identifies the leg when there
/// are multiple legs; an exact group never hides a disagreeing leg of the same asset.
pub(crate) fn counterparts<'a>(
    observation: &ActivityTradeObservation,
    aggregates: impl IntoIterator<Item = &'a ActivityAggregate>,
) -> Vec<&'a ActivityAggregate> {
    let original = observation.group_id.components();
    let mut candidates = aggregates
        .into_iter()
        .filter(|aggregate| {
            let candidate = aggregate.group_id.components();
            candidate.wallet == original.wallet
                && candidate.transaction_hash == original.transaction_hash
                && candidate.activity_type == ActivityType::Trade
        })
        .collect::<Vec<_>>();
    if candidates.len() > 1 {
        candidates.retain(|aggregate| aggregate.group_id.components().asset == original.asset);
    }
    candidates
}

pub(crate) fn disposition(
    frame: &DecisionContinuationV3,
    read: &VerifiedCommitment,
) -> Result<AuditDisposition, FeedAuditError> {
    if !frame.is_activity_frame() || read.wallet != frame.facts.wallet {
        return Err(FeedAuditError::Semantic("audit wallet/authority differs"));
    }
    let receipt = frame
        .observed_source_receipt
        .ok_or(FeedAuditError::Semantic("frame receipt missing"))?;
    let binding = read
        .bindings
        .iter()
        .find(|binding| binding.stream_receipt == receipt);
    if let Some(binding) = binding {
        let target = read
            .aggregates
            .iter()
            .find(|aggregate| aggregate.group_id.key() == &binding.history_group_id)
            .ok_or(FeedAuditError::Semantic(
                "counterpart absent from authenticated read",
            ))?;
        let components = target.group_id.components();
        let raw_identity = components
            .condition_id
            .as_ref()
            .zip(components.outcome)
            .map(|(condition, outcome)| {
                MarketOutcomeId::new(MarketId(VenueMarketId(condition.0.clone())), outcome)
            });
        let identity = read
            .identities
            .get(target.group_id.key())
            .cloned()
            .or(raw_identity);
        let agrees = components.activity_type == ActivityType::Trade
            && !target.is_combo
            && target.share_sum != ShareAmount::ZERO
            && components.side == Some(Side::Buy)
            && identity
                == Some(MarketOutcomeId::new(
                    frame.facts.market_id.clone(),
                    frame.facts.outcome_id,
                ));
        return Ok(if agrees {
            AuditDisposition::Matched(binding.history_group_id.clone())
        } else {
            AuditDisposition::Contradicted(binding.history_group_id.clone())
        });
    }
    let proof: crate::frame_admission::FrameDecisionProof =
        serde_json::from_value(frame.facts.decision_inputs.clone())?;
    let mature_end = proof
        .inputs
        .source_time
        .unix_timestamp()
        .checked_add(
            i64::try_from(
                frame
                    .facts
                    .paper_freshness_policy
                    .ok_or(FeedAuditError::Semantic("frozen copy budget missing"))?
                    .copy_latency_budget_secs,
            )
            .map_err(|_| FeedAuditError::Semantic("copy budget overflow"))?,
        )
        .ok_or(FeedAuditError::Semantic("maturity overflow"))?;
    // An unbound transaction, including an ambiguous pair, cannot prove absence.
    let observation_tx = &frame.facts.transaction_hash;
    if read.full_history
        && read.fixed_end >= mature_end
        && !read.aggregates.iter().any(|aggregate| {
            let components = aggregate.group_id.components();
            components.wallet == frame.facts.wallet
                && &components.transaction_hash == observation_tx
                && components.activity_type == ActivityType::Trade
        })
    {
        Ok(AuditDisposition::Absent)
    } else {
        Ok(AuditDisposition::Unresolved)
    }
}

pub(crate) fn verify_incident<L, E>(
    frame: &DecisionContinuationV3,
    incident: &FeedIncident,
    lookup: &mut L,
) -> Result<(), FeedAuditError>
where
    L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
    E: std::fmt::Display,
{
    frame.verify_activity_frame(lookup)?;
    if frame.observed_source_receipt != Some(incident.frame_receipt)
        || incident.engagement_receipt.is_some()
    {
        return Err(FeedAuditError::Semantic("incident frame receipt differs"));
    }
    let read = crate::bucket_commit::verified_commitment_bindings_with_lookup(
        incident.deciding_commitment_receipt,
        lookup,
    )?;
    verify_incident_conclusion(frame, incident, &read)
}

fn verify_incident_conclusion(
    frame: &DecisionContinuationV3,
    incident: &FeedIncident,
    read: &VerifiedCommitment,
) -> Result<(), FeedAuditError> {
    match (
        incident.cause.clone(),
        disposition(frame, read)?,
        &incident.counterpart_identity,
    ) {
        (FeedIncidentCause::Contradiction, AuditDisposition::Contradicted(id), Some(expected))
            if &id == expected =>
        {
            Ok(())
        }
        (FeedIncidentCause::Absence, AuditDisposition::Absent, None) => Ok(()),
        _ => Err(FeedAuditError::Semantic(
            "incident cause/counterpart differs from authenticated conclusion",
        )),
    }
}

pub(crate) fn latest_incident(era: &PaperEra) -> Option<FeedIncident> {
    era.frames
        .iter()
        .rev()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FeedIncidentChanged {
                incident,
                state: HaltState::Engaged,
            }) => Some(incident.clone()),
            _ => None,
        })
}

/// Engagements remain audit evidence after release. Release never revives an obligation.
pub(crate) fn audited_receipts(era: &PaperEra) -> Vec<AppendReceipt> {
    era.frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FeedIncidentChanged {
                incident,
                state: HaltState::Engaged,
            }) => Some(incident.frame_receipt),
            _ => None,
        })
        .collect()
}

/// Verify every journaled incident and expose admissions still lacking a disposed REST audit.
pub(crate) fn verify_recorded_audits<L, E>(
    state: &pe_paper_state::PaperStateDb,
    frames: &[DecisionContinuationV3],
    era: &PaperEra,
    commitments: &[AppendReceipt],
    lookup: &mut L,
) -> Result<Vec<SourceTradeId>, FeedAuditError>
where
    L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
    E: std::fmt::Display,
{
    let mut receipts = Vec::new();
    for receipt in commitments {
        if !receipts.contains(receipt) {
            receipts.push(*receipt);
        }
    }
    for record in &era.frames {
        if let PaperLogFrame::Record(PaperLogRecord::FeedIncidentChanged {
            incident,
            state: HaltState::Engaged,
        }) = &record.frame
            && !receipts.contains(&incident.deciding_commitment_receipt)
        {
            receipts.push(incident.deciding_commitment_receipt);
        }
    }
    let reads = receipts
        .iter()
        .map(|receipt| {
            crate::bucket_commit::verified_commitment_bindings_with_lookup(*receipt, lookup)
                .map(|read| (*receipt, read))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for record in &era.frames {
        if let PaperLogFrame::Record(PaperLogRecord::FeedIncidentChanged {
            incident,
            state: HaltState::Engaged,
        }) = &record.frame
        {
            let frame = frames
                .iter()
                .find(|frame| frame.observed_source_receipt == Some(incident.frame_receipt))
                .ok_or(FeedAuditError::Semantic(
                    "incident has no durable admitted frame",
                ))?;
            frame.verify_activity_frame(lookup)?;
            if frame.observed_source_receipt != Some(incident.frame_receipt)
                || incident.engagement_receipt.is_some()
            {
                return Err(FeedAuditError::Semantic("incident frame receipt differs"));
            }
            let read = reads
                .iter()
                .find(|(receipt, _)| *receipt == incident.deciding_commitment_receipt)
                .ok_or(FeedAuditError::Semantic("incident commitment missing"))?;
            verify_incident_conclusion(frame, incident, &read.1)?;
        }
    }
    let retired = audited_receipts(era);
    let mut unresolved = Vec::new();
    for frame in frames {
        if frame
            .observed_source_receipt
            .is_some_and(|receipt| retired.contains(&receipt))
        {
            continue;
        }
        let mut matched = false;
        for (receipt, read) in &reads {
            if !commitments.contains(receipt) {
                continue;
            }
            if read.wallet != frame.facts.wallet {
                continue;
            }
            if let AuditDisposition::Matched(id) = disposition(frame, read)? {
                let binding = read
                    .bindings
                    .iter()
                    .find(|binding| {
                        binding.history_group_id == id
                            && Some(binding.stream_receipt) == frame.observed_source_receipt
                    })
                    .ok_or(FeedAuditError::Semantic("matched audit binding missing"))?;
                matched |= state
                    .activity_revision_disposed(&id, &binding.semantic_revision)
                    .map_err(|_| FeedAuditError::Semantic("audit disposition unavailable"))?;
            }
        }
        if !matched {
            unresolved.push(frame.facts.source_trade_id.clone());
        }
    }
    Ok(unresolved)
}
