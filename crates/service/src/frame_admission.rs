//! Frozen frame admission and the wallet's authenticated complete-history frontier.
use std::collections::{BTreeMap, HashMap, HashSet};

use pe_core_types::{MarketId, SourceTradeId, WalletAddress};
use pe_event_log::AppendReceipt;
use pe_paper_state::{PaperStateDb, WalletCoverage};
use pe_source_polymarket_public::ReconciliationPageEvidence;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::bucket_commit::{CompleteActivityPage, PageOccurrence};
use crate::orchestrator_control::AdmissionLedgerCapture;

pub const FRAME_ADMISSION_SOURCE_ID: &str = "pe-service.activity-frame-admission";
pub const FRAME_FALLBACK_SOURCE_ID: &str = "pe-service.activity-frame-fallback";

#[derive(Debug, thiserror::Error)]
pub(crate) enum FrameAdmissionError {
    #[error("unsupported {surface} version {version}")]
    UnsupportedVersion { surface: &'static str, version: u16 },
    #[error("invalid frame admission prefix: {0}")]
    InvalidPrefix(&'static str),
    #[error("feed frontier read: {0}")]
    Read(#[from] crate::bucket_commit::CompleteActivityReadError),
    #[error("frame paper state: {0}")]
    State(#[from] pe_paper_state::PaperStateError),
    #[error("frame prefix encoding: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame ledger capture: {0}")]
    Capture(Box<crate::position_seeder::CausalPositionError>),
    #[error("frame ledger history: {0}")]
    History(Box<crate::paper_recovery::WalletLedgerReplayError>),
    #[error("frame ledger effect: {0}")]
    Effect(#[from] pe_position_ledger::LedgerEffectDocumentError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedHistoryFrontier {
    pub version: u16,
    pub wallet: WalletAddress,
    pub fixed_end: i64,
    pub commitment: AppendReceipt,
    pub page_occurrences: Vec<PageOccurrence>,
    pub pages: Vec<ReconciliationPageEvidence>,
}

impl FeedHistoryFrontier {
    pub(crate) fn verify<L, E>(&self, lookup: &mut L) -> Result<(), FrameAdmissionError>
    where
        L: FnMut(AppendReceipt) -> Result<CompleteActivityPage, E>,
        E: std::fmt::Display,
    {
        if self.version != 1 {
            return Err(FrameAdmissionError::UnsupportedVersion {
                surface: "feed frontier",
                version: self.version,
            });
        }
        crate::bucket_commit::verify_feed_frontier(self, lookup).map_err(Into::into)
    }

    /// Equality passes; precise receive/admission instants are never rounded to seconds.
    #[must_use]
    pub fn current(
        &self,
        received: OffsetDateTime,
        admitted: OffsetDateTime,
        stale_secs: i64,
        earlier_wallet_received: Option<OffsetDateTime>,
    ) -> bool {
        stale_secs > 0
            && i128::from(self.fixed_end) * 1_000_000_000 <= received.unix_timestamp_nanos()
            && admitted.unix_timestamp_nanos() >= i128::from(self.fixed_end) * 1_000_000_000
            && admitted.unix_timestamp_nanos() - i128::from(self.fixed_end) * 1_000_000_000
                <= i128::from(stale_secs) * 1_000_000_000
            && earlier_wallet_received.is_none_or(|earlier| {
                (admitted - earlier).whole_nanoseconds() <= i128::from(stale_secs) * 1_000_000_000
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontierCollection {
    pub version: u16,
    pub frontiers: Vec<FeedHistoryFrontier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameFallbackReason {
    Latched,
    HistoryBehind,
    EarlierUnresolvedBuy,
    WalletNotReady,
    IdentityUnverified,
    CopyExpired,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedLatchBasis {
    pub latest_incident: Option<AppendReceipt>,
    pub release: Option<AppendReceipt>,
}
impl FeedLatchBasis {
    #[must_use]
    pub fn engaged(&self) -> bool {
        self.latest_incident.is_some() && self.release.is_none()
    }
}

/// Same-identity priority: admitted receipt, earliest qualifying BUY, earliest receipt.
/// Runtime obligations, the serialized barrier and boot recovery use this exact rule.
pub(crate) fn prefer_observation(
    existing: AppendReceipt,
    existing_admitted: bool,
    existing_qualifying: bool,
    incoming: AppendReceipt,
    incoming_admitted: bool,
    incoming_qualifying: bool,
) -> bool {
    (
        incoming_admitted,
        incoming_qualifying,
        std::cmp::Reverse(incoming.sequence),
    ) > (
        existing_admitted,
        existing_qualifying,
        std::cmp::Reverse(existing.sequence),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EarlierFrame {
    pub receipt: AppendReceipt,
    pub wallet: WalletAddress,
    pub source_trade_id: SourceTradeId,
    pub market: MarketId,
    pub received_at: OffsetDateTime,
    pub unresolved_buy: bool,
}

/// An immutable admission prefix. The source artifact authenticates this capture's digest;
/// classification is reconstructed from its confirmed positions and consumed history.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameAdmissionInputs {
    pub version: u16,
    pub frame_receipt: AppendReceipt,
    pub admitted_at: OffsetDateTime,
    pub received_at: OffsetDateTime,
    pub source_time: OffsetDateTime,
    pub ledger_capture: AdmissionLedgerCapture,
    pub ledger_group_boundary: Option<i64>,
    pub anchor_balances: Vec<(u16, pe_core_types::ShareAmount)>,
    pub ledger_groups: Vec<pe_paper_state::ActivityGroupRow>,
    pub market_consumed: bool,
    pub earlier_frames: Vec<EarlierFrame>,
    pub copy_eligible: bool,
    pub history_complete: bool,
    pub fenced: bool,
    pub coverage: WalletCoverage,
    pub frontier: FeedHistoryFrontier,
    pub poll_round_stale_secs: i64,
    pub latch: FeedLatchBasis,
    pub paper_prefix: Option<AppendReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<FrameIdentityProof>,
}

impl FrameAdmissionInputs {
    pub(crate) fn positions(
        &self,
        wallet: WalletAddress,
        market: &MarketId,
    ) -> Result<pe_copy_signal_engine::PositionSnapshot, FrameAdmissionError> {
        let balances = self
            .anchor_balances
            .iter()
            .map(|(outcome, amount)| (market.to_string(), *outcome, *amount))
            .collect::<Vec<_>>();
        let mut outcomes = HashSet::new();
        if !self
            .anchor_balances
            .iter()
            .all(|(outcome, _)| outcomes.insert(*outcome))
            || (self.coverage.anchor_seq.is_none() && !balances.is_empty())
            || self.ledger_groups.windows(2).any(|rows| {
                (rows[0].source_epoch, &rows[0].source_trade_id.0)
                    >= (rows[1].source_epoch, &rows[1].source_trade_id.0)
            })
        {
            return Err(FrameAdmissionError::InvalidPrefix(
                "invalid market ledger prefix",
            ));
        }
        for group in &self.ledger_groups {
            if self
                .coverage
                .activity_cutoff_unix
                .is_some_and(|cutoff| group.source_epoch <= cutoff)
                || !group_affects_market(group, market)?
            {
                return Err(FrameAdmissionError::InvalidPrefix(
                    "ledger group outside frozen market prefix",
                ));
            }
        }
        let ledger =
            crate::paper_recovery::replay_frozen_records(wallet, &balances, &self.ledger_groups)
                .map_err(|error| FrameAdmissionError::History(Box::new(error)))?;
        let positions = ledger
            .position(&wallet)
            .cloned()
            .ok_or(FrameAdmissionError::InvalidPrefix("market ledger absent"))?;
        Ok(positions)
    }

    /// Authenticate a scoped capture against the append-only anchor and group owners.
    pub(crate) fn verify_durable(
        &self,
        state: &PaperStateDb,
        facts: &crate::bucket_commit::DecisionContinuationFacts,
    ) -> Result<(), FrameAdmissionError> {
        let anchor = self
            .coverage
            .anchor_seq
            .map(|sequence| state.position_anchor(&facts.wallet, sequence))
            .transpose()?
            .flatten();
        let balances: Vec<(String, u16, pe_core_types::ShareAmount)> = match anchor {
            Some(anchor) => {
                if Some(anchor.activity_cutoff_unix) != self.coverage.activity_cutoff_unix
                    || Some(anchor.anchored_at_unix) != self.coverage.anchored_at_unix
                {
                    return Err(FrameAdmissionError::InvalidPrefix(
                        "anchor differs from frozen coverage",
                    ));
                }
                serde_json::from_str(&anchor.balances_json)?
            }
            None if self.coverage.anchor_seq.is_none() => Vec::new(),
            None => {
                return Err(FrameAdmissionError::InvalidPrefix(
                    "frozen anchor owner absent",
                ));
            }
        };
        let scoped = balances
            .iter()
            .filter(|(market, _, _)| market == &facts.market_id.to_string())
            .map(|(_, outcome, amount)| (*outcome, *amount))
            .collect::<Vec<_>>();
        if scoped != self.anchor_balances {
            return Err(FrameAdmissionError::InvalidPrefix(
                "frozen anchor market balances differ",
            ));
        }
        let groups = state.activity_groups_at_boundary(
            &facts.wallet,
            self.coverage.activity_cutoff_unix.unwrap_or(i64::MIN),
            self.ledger_group_boundary,
        )?;
        let last_applied_epoch = groups.iter().map(|group| group.source_epoch).max();
        let current_cursor = state.cursor(&facts.wallet)?;
        if self.ledger_capture.cursor.is_some_and(|cursor| {
            last_applied_epoch.is_some_and(|epoch| cursor < epoch)
                || current_cursor.is_none_or(|current| cursor > current)
        }) || self
            .ledger_group_boundary
            .is_some_and(|boundary| boundary <= 0)
            || self.ledger_group_boundary > state.activity_group_boundary(&facts.wallet)?
        {
            return Err(FrameAdmissionError::InvalidPrefix(
                "frozen ledger boundary differs",
            ));
        }
        let mut scoped_groups = Vec::new();
        for group in &groups {
            if group_affects_market(group, &facts.market_id)? {
                scoped_groups.push(group.clone());
            }
        }
        if scoped_groups != self.ledger_groups {
            return Err(FrameAdmissionError::InvalidPrefix(
                "frozen market groups differ from durable prefix",
            ));
        }
        let ledger = crate::paper_recovery::replay_frozen_records(facts.wallet, &balances, &groups)
            .map_err(|error| FrameAdmissionError::History(Box::new(error)))?;
        if crate::position_seeder::wallet_ledger_hash(&ledger, facts.wallet)
            .map_err(|error| FrameAdmissionError::Capture(Box::new(error)))?
            != self.ledger_capture.hash
        {
            return Err(FrameAdmissionError::InvalidPrefix(
                "frozen ledger capture hash differs",
            ));
        }
        let history = state.market_history_record(&facts.wallet, &facts.market_id)?;
        if history.is_none_or(|history| {
            history.source_trade_id != facts.source_trade_id
                || history.first_epoch != facts.source_epoch
        }) {
            return Err(FrameAdmissionError::InvalidPrefix(
                "frame does not own first market consumption",
            ));
        }
        Ok(())
    }
}

pub(crate) fn group_affects_market(
    group: &pe_paper_state::ActivityGroupRow,
    market: &MarketId,
) -> Result<bool, FrameAdmissionError> {
    use pe_position_ledger::{AppliedEffect, LedgerEffect};
    Ok(
        match AppliedEffect::from_document(&group.proof_json)?
            .effect
            .effective()
        {
            LedgerEffect::Trade { market_id, .. }
            | LedgerEffect::Split { market_id, .. }
            | LedgerEffect::Merge { market_id, .. }
            | LedgerEffect::Redeem { market_id, .. } => market_id == market,
            _ => false,
        },
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameFallbackArtifact {
    pub version: u16,
    pub frame_receipt: AppendReceipt,
    pub routing_clock: OffsetDateTime,
    pub reason: FrameFallbackReason,
    pub frontier: Option<FeedHistoryFrontier>,
    pub latest_incident_basis: FeedLatchBasis,
}

pub(crate) fn persist_frontiers(
    state: &PaperStateDb,
    frontiers: &HashMap<WalletAddress, FeedHistoryFrontier>,
) -> Result<(), FrameAdmissionError> {
    let sorted = frontiers
        .iter()
        .map(|(wallet, frontier)| (wallet.to_string(), frontier.clone()))
        .collect::<BTreeMap<_, _>>();
    let collection = FrontierCollection {
        version: 1,
        frontiers: sorted.into_values().collect(),
    };
    state.publish_feed_history_frontiers(&serde_json::to_value(collection)?)?;
    Ok(())
}

pub(crate) fn frame_prefix_blocks(
    earlier: &[EarlierFrame],
    wallet: WalletAddress,
    market: &MarketId,
) -> bool {
    earlier
        .iter()
        .any(|frame| frame.wallet == wallet && frame.market == *market && frame.unresolved_buy)
}

pub(crate) fn unique_earlier(earlier: &[EarlierFrame], frame: AppendReceipt) -> bool {
    let mut sequences = HashSet::new();
    earlier.iter().all(|earlier| {
        earlier.receipt.sequence < frame.sequence && sequences.insert(earlier.receipt.sequence)
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameIdentityProof {
    pub provenance: crate::asset_identity::IdentityProvenance,
    pub receipt: AppendReceipt,
}

/// Compact source-log authentication of the continuation-owned admission body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameAdmissionArtifact {
    pub version: u16,
    pub frame_receipt: AppendReceipt,
    pub capture_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<FrameIdentityProof>,
}

impl FrameAdmissionArtifact {
    pub fn from_inputs(inputs: &FrameAdmissionInputs) -> Result<Self, serde_json::Error> {
        Ok(Self {
            version: inputs.version,
            frame_receipt: inputs.frame_receipt,
            capture_digest: frame_revision(inputs)?,
            identity: inputs.identity.clone(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameDecisionProof {
    pub admission_receipt: AppendReceipt,
    pub inputs: FrameAdmissionInputs,
}

pub(crate) fn frame_revision(inputs: &FrameAdmissionInputs) -> Result<String, serde_json::Error> {
    let mut hash = blake3::Hasher::new();
    hash.update(if inputs.version == 1 {
        b"prediction-edge/activity-frame-decision/v1\0"
    } else {
        b"prediction-edge/activity-frame-decision/v2\0"
    });
    hash.update(&crate::bucket_commit::canonical_json(inputs)?);
    Ok(hash.finalize().to_hex().to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use pe_core_types::{EventSeq, VenueMarketId};
    fn receipt(sequence: u64) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::hash(&sequence.to_be_bytes()),
        }
    }
    fn at(epoch: i64) -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(epoch).unwrap()
    }

    #[test]
    fn frontier_equality_and_precise_past_bound() {
        let frontier = FeedHistoryFrontier {
            version: 1,
            wallet: WalletAddress([1; 20]),
            fixed_end: 100,
            commitment: receipt(2),
            page_occurrences: Vec::new(),
            pages: Vec::new(),
        };
        assert!(frontier.current(at(101), at(190), 90, None));
        assert!(!frontier.current(at(101), at(190) + time::Duration::nanoseconds(1), 90, None));
        assert!(!frontier.current(at(99), at(101), 90, None));
        assert!(frontier.current(at(101), at(190), 90, Some(at(100))));
        assert!(!frontier.current(
            at(101),
            at(190),
            90,
            Some(at(100) - time::Duration::nanoseconds(1))
        ));
        let stale = 5 * 86400;
        assert!(!frontier.current(at(100 + stale), at(100 + stale), 90, None));
    }

    #[test]
    fn unresolved_buy_barrier_is_wallet_and_market_scoped() {
        let wallet = WalletAddress([1; 20]);
        let market = MarketId(VenueMarketId("market".to_owned()));
        let mut frames = vec![EarlierFrame {
            receipt: receipt(1),
            wallet,
            source_trade_id: SourceTradeId("frame".to_owned()),
            market: market.clone(),
            received_at: at(100),
            unresolved_buy: true,
        }];
        assert!(frame_prefix_blocks(&frames, wallet, &market));
        assert!(!frame_prefix_blocks(
            &frames,
            WalletAddress([2; 20]),
            &market
        ));
        assert!(!frame_prefix_blocks(
            &frames,
            wallet,
            &MarketId(VenueMarketId("other".to_owned()))
        ));
        assert!(unique_earlier(&frames, receipt(2)));
        assert!(!unique_earlier(&frames, receipt(1)));
        frames[0].unresolved_buy = false;
        assert!(!frame_prefix_blocks(&frames, wallet, &market));
    }
}
