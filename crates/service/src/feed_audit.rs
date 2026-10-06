//! Historical frame-audit verification for version-one admissions.
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
    #[error("frame audit disposition: {0}")]
    State(#[from] pe_paper_state::PaperStateError),
}

/// Resolve a frame against the whole authenticated read. A binding or contradiction fixes
/// its target; only read-proven restamps are equivalent. After absence, discovery keeps
/// asset disambiguation and ambiguity until a unique later counterpart can be fixed.
pub(crate) fn resolve_frame_counterpart<'a>(
    observation: &ActivityTradeObservation,
    fixed: Option<Option<&SourceTradeId>>,
    aggregates: impl IntoIterator<Item = &'a ActivityAggregate>,
    pairs: &std::collections::HashMap<SourceTradeId, SourceTradeId>,
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
    if let Some(Some(target)) = fixed {
        candidates.retain(|aggregate| {
            let id = aggregate.group_id.key();
            id == target || pairs.get(id) == Some(target) || pairs.get(target) == Some(id)
        });
    } else if candidates.len() > 1 {
        candidates.retain(|aggregate| aggregate.group_id.components().asset == original.asset);
    }
    crate::bucket_commit::collapse_restamp_pairs(&mut candidates, pairs);
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

/// A concluded frame audit retires once its fixed counterpart group is disposed.
/// Absence has no counterpart and retires on incident acknowledgement.
pub(crate) fn counterpart_disposed(
    state: &pe_paper_state::PaperStateDb,
    counterpart: Option<&SourceTradeId>,
) -> Result<bool, pe_paper_state::PaperStateError> {
    match counterpart {
        Some(id) => Ok(state.activity_group_state(id)?.is_some()),
        None => Ok(true),
    }
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
    let mut retired = HashSet::new();
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
            if counterpart_disposed(state, incident.counterpart_identity.as_ref())? {
                retired.insert(key(incident.frame_receipt));
            }
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
            if let AuditDisposition::Matched(id) = disposition(*frame, &read)?
                && counterpart_disposed(state, Some(&id))?
            {
                matched.insert(key(frame_receipt));
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
    use pe_source_polymarket_public::{
        ActivityParseContext, ActivityTransport, parse_activity_response,
        parse_activity_trade_observation,
    };
    use serde_json::json;
    use time::OffsetDateTime;

    #[test]
    fn post_absence_counterpart_keeps_asset_disambiguation_and_ambiguity() {
        let now = OffsetDateTime::from_unix_timestamp(100).unwrap();
        let wallet = WalletAddress([1; 20]);
        let original = json!({"proxyWallet": wallet.to_string(), "timestamp": 100,
            "type": "TRADE", "conditionId": "market-a", "asset": "asset-a",
            "transactionHash": "tx", "side": "BUY", "outcomeIndex": 0,
            "outcome": "Yes", "size": "5", "usdcSize": "2.5", "price": "0.5", "isCombo": false});
        let observation =
            parse_activity_trade_observation(&serde_json::to_vec(&original).unwrap()).unwrap();
        let mut late = original.clone();
        late["timestamp"] = json!(221);
        let mut other = original.clone();
        other["timestamp"] = json!(220);
        other["conditionId"] = json!("market-b");
        for ambiguous in [false, true] {
            other["asset"] = json!(if ambiguous { "asset-a" } else { "asset-b" });
            let read = parse_activity_response(
                &serde_json::to_vec(&json!([other, late])).unwrap(),
                wallet,
                &ActivityParseContext {
                    source_id: SourceId("fixture".to_owned()),
                    observed_at: SourceTimestamp(now),
                    received_at: ReceivedAt(now),
                    transport: ActivityTransport::Replay,
                },
            )
            .unwrap();
            let aggregates = read.aggregates().unwrap();
            for fixed in [None, Some(None)] {
                let candidates = resolve_frame_counterpart(
                    &observation,
                    fixed,
                    &aggregates,
                    &std::collections::HashMap::new(),
                );
                assert_eq!(candidates.len(), if ambiguous { 2 } else { 1 });
                if !ambiguous {
                    assert_eq!(candidates[0].group_id.key(), observation.group_id.key());
                }
            }
        }
    }
}
