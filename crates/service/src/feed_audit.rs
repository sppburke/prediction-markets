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

pub(crate) struct FrameAuditFacts<'a> {
    wallet: pe_core_types::WalletAddress,
    source_epoch: i64,
    transaction_hash: &'a str,
    market_id: &'a MarketId,
    outcome_id: pe_core_types::OutcomeId,
    receipt: Option<AppendReceipt>,
    copy_budget: Option<u64>,
    frame_authority: bool,
}

pub(crate) trait FrameAuditIdentity {
    fn audit_facts(&self) -> FrameAuditFacts<'_>;
}

impl FrameAuditIdentity for DecisionContinuationV3 {
    fn audit_facts(&self) -> FrameAuditFacts<'_> {
        FrameAuditFacts {
            wallet: self.facts.wallet,
            source_epoch: self.facts.source_epoch,
            transaction_hash: &self.facts.transaction_hash,
            market_id: &self.facts.market_id,
            outcome_id: self.facts.outcome_id,
            receipt: self.observed_source_receipt,
            copy_budget: self
                .facts
                .paper_freshness_policy
                .map(|policy| policy.copy_latency_budget_secs),
            frame_authority: self.is_activity_frame(),
        }
    }
}

impl FrameAuditIdentity for pe_paper_state::ActivityFrameDecisionIndex {
    fn audit_facts(&self) -> FrameAuditFacts<'_> {
        FrameAuditFacts {
            wallet: self.wallet,
            source_epoch: self.source_epoch,
            transaction_hash: &self.transaction_hash,
            market_id: &self.market_id,
            outcome_id: self.outcome_id,
            receipt: self.observed_source_receipt,
            copy_budget: Some(self.copy_latency_budget_secs),
            frame_authority: true,
        }
    }
}

pub(crate) fn disposition(
    frame: &impl FrameAuditIdentity,
    read: &VerifiedCommitment,
) -> Result<AuditDisposition, FeedAuditError> {
    let frame = frame.audit_facts();
    if !frame.frame_authority || read.wallet != frame.wallet {
        return Err(FeedAuditError::Semantic("audit wallet/authority differs"));
    }
    let receipt = frame
        .receipt
        .ok_or(FeedAuditError::Semantic("frame receipt missing"))?;
    let binding = read
        .binding_indices
        .get(&(receipt.sequence, receipt.this_hash))
        .and_then(|index| read.bindings.get(*index));
    if let Some(binding) = binding {
        let target = read
            .aggregate_indices
            .get(&binding.history_group_id)
            .and_then(|index| read.aggregates.get(*index))
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
                    frame.market_id.clone(),
                    frame.outcome_id,
                ));
        return Ok(if agrees {
            AuditDisposition::Matched(binding.history_group_id.clone())
        } else {
            AuditDisposition::Contradicted(binding.history_group_id.clone())
        });
    }
    let mature_end = frame
        .source_epoch
        .checked_add(
            i64::try_from(
                frame
                    .copy_budget
                    .ok_or(FeedAuditError::Semantic("frozen copy budget missing"))?,
            )
            .map_err(|_| FeedAuditError::Semantic("copy budget overflow"))?,
        )
        .ok_or(FeedAuditError::Semantic("maturity overflow"))?;
    // An unbound transaction, including an ambiguous pair, cannot prove absence.
    let observation_tx = frame.transaction_hash;
    if read.full_history
        && read.fixed_end >= mature_end
        && !read
            .transaction_aggregates
            .get(observation_tx)
            .into_iter()
            .flatten()
            .filter_map(|index| read.aggregates.get(*index))
            .any(|aggregate| aggregate.group_id.components().activity_type == ActivityType::Trade)
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

pub(crate) fn verify_incident_conclusion(
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
    frames: &[&DecisionContinuationV3],
    era: &PaperEra,
    commitments: &[AppendReceipt],
    lookup: &mut L,
) -> Result<Vec<SourceTradeId>, FeedAuditError>
where
    L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
    E: std::fmt::Display,
{
    use std::collections::{HashMap, HashSet};
    let key = |receipt: AppendReceipt| (receipt.sequence, receipt.this_hash);
    let commitment_keys = commitments
        .iter()
        .map(|receipt| key(*receipt))
        .collect::<HashSet<_>>();
    let mut receipt_keys = HashSet::new();
    let mut receipts = commitments
        .iter()
        .copied()
        .filter(|receipt| receipt_keys.insert(key(*receipt)))
        .collect::<Vec<_>>();
    let mut incidents = HashMap::<_, Vec<_>>::new();
    let mut frames_by_receipt = HashMap::new();
    for frame in frames {
        if let Some(receipt) = frame.observed_source_receipt {
            frames_by_receipt.insert(key(receipt), *frame);
        }
    }
    for record in &era.frames {
        if let PaperLogFrame::Record(PaperLogRecord::FeedIncidentChanged {
            incident,
            state: HaltState::Engaged,
        }) = &record.frame
        {
            let frame = frames_by_receipt.get(&key(incident.frame_receipt)).ok_or(
                FeedAuditError::Semantic("incident has no durable admitted frame"),
            )?;
            frame.verify_activity_frame(lookup)?;
            if incident.engagement_receipt.is_some() {
                return Err(FeedAuditError::Semantic("incident frame receipt differs"));
            }
            if receipt_keys.insert(key(incident.deciding_commitment_receipt)) {
                receipts.push(incident.deciding_commitment_receipt);
            }
            incidents
                .entry(key(incident.deciding_commitment_receipt))
                .or_default()
                .push(incident);
        }
    }
    let retired = audited_receipts(era)
        .into_iter()
        .map(key)
        .collect::<HashSet<_>>();
    let mut matched = HashSet::new();
    // Release each reconstructed full-history read before authenticating the next one.
    receipts.sort_by_key(|receipt| receipt.sequence);
    for receipt in receipts {
        let read = crate::bucket_commit::verified_commitment_bindings_with_lookup(receipt, lookup)?;
        for incident in incidents.get(&key(receipt)).into_iter().flatten() {
            let frame = frames_by_receipt.get(&key(incident.frame_receipt)).ok_or(
                FeedAuditError::Semantic("incident has no durable admitted frame"),
            )?;
            verify_incident_conclusion(frame, incident, &read)?;
        }
        if !commitment_keys.contains(&key(receipt)) {
            continue;
        }
        for frame in frames {
            if frame.facts.wallet != read.wallet {
                continue;
            }
            let frame_receipt = frame
                .observed_source_receipt
                .ok_or(FeedAuditError::Semantic("frame receipt missing"))?;
            if retired.contains(&key(frame_receipt)) || matched.contains(&key(frame_receipt)) {
                continue;
            }
            if let AuditDisposition::Matched(id) = disposition(*frame, &read)? {
                let binding = read
                    .binding_indices
                    .get(&(frame_receipt.sequence, frame_receipt.this_hash))
                    .and_then(|index| read.bindings.get(*index))
                    .filter(|binding| binding.history_group_id == id)
                    .ok_or(FeedAuditError::Semantic("matched audit binding missing"))?;
                if state
                    .activity_revision_disposed(&id, &binding.semantic_revision)
                    .map_err(|_| FeedAuditError::Semantic("audit disposition unavailable"))?
                {
                    matched.insert(key(frame_receipt));
                }
            }
        }
    }
    Ok(frames
        .iter()
        .filter(|frame| {
            frame.observed_source_receipt.is_none_or(|receipt| {
                !retired.contains(&key(receipt)) && !matched.contains(&key(receipt))
            })
        })
        .map(|frame| frame.facts.source_trade_id.clone())
        .collect())
}
