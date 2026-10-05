//! Frozen frame admission and the wallet's authenticated complete-history frontier.
use std::collections::{BTreeMap, HashMap, HashSet};

use pe_core_types::{MarketId, MarketOutcomeId, SourceTradeId, WalletAddress};
use pe_event_log::AppendReceipt;
use pe_paper_state::{LeaderPositionRow, PaperStateDb, WalletCoverage};
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
    #[error("frame ledger mutation: {0}")]
    Ledger(#[from] pe_position_ledger::LedgerError),
    #[error("frame ledger timestamp: {0}")]
    Timestamp(#[from] time::error::ComponentRange),
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

pub(crate) fn restore_frontiers(
    state: &PaperStateDb,
    index: &crate::risk_inputs::SourceReceiptIndex,
) -> Result<HashMap<WalletAddress, FeedHistoryFrontier>, FrameAdmissionError> {
    let collection: FrontierCollection = serde_json::from_value(state.feed_history_frontiers()?)?;
    if collection.version != 1 {
        return Err(FrameAdmissionError::UnsupportedVersion {
            surface: "frontier collection",
            version: collection.version,
        });
    }
    let mut result = HashMap::new();
    for frontier in collection.frontiers {
        frontier.verify(&mut |receipt| {
            index
                .source_envelope(receipt)
                .map(CompleteActivityPage::from)
        })?;
        if result.insert(frontier.wallet, frontier).is_some() {
            return Err(FrameAdmissionError::InvalidPrefix(
                "duplicate wallet frontier",
            ));
        }
    }
    Ok(result)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrameFallbackReason {
    Latched,
    HistoryBehind,
    EarlierUnresolvedBuy,
    WalletNotReady,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EarlierFrame {
    pub receipt: AppendReceipt,
    pub wallet: WalletAddress,
    pub source_trade_id: SourceTradeId,
    pub market: MarketId,
    pub received_at: OffsetDateTime,
    pub unresolved_buy: bool,
    pub unresolved_obligation: bool,
}

/// An immutable admission prefix. Canonical source-log bytes authenticate this capture;
/// classification is reconstructed from its confirmed positions and consumed history.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameAdmissionInputs {
    pub version: u16,
    pub frame_receipt: AppendReceipt,
    pub payload_hash: String,
    pub parser_version: u32,
    pub schema_version: u32,
    pub admitted_at: OffsetDateTime,
    pub received_at: OffsetDateTime,
    pub source_time: OffsetDateTime,
    pub ledger_capture: AdmissionLedgerCapture,
    pub ledger_rows: Vec<LeaderPositionRow>,
    pub ledger_anchor: Option<pe_paper_state::PositionAnchorRow>,
    pub ledger_groups: Vec<pe_paper_state::ActivityGroupRow>,
    pub consumed_history: Vec<MarketId>,
    pub earlier_frames: Vec<EarlierFrame>,
    pub copy_eligible: bool,
    pub history_complete: bool,
    pub fenced: bool,
    pub coverage: WalletCoverage,
    pub frontier: FeedHistoryFrontier,
    pub poll_round_stale_secs: i64,
    pub latch: FeedLatchBasis,
    pub paper_prefix: Option<AppendReceipt>,
    pub signal_config: pe_copy_signal_engine::SignalConfig,
    pub applied_configuration: crate::runtime_config::RuntimeConfig,
    pub frozen_basis: crate::bucket_commit::FrozenDecisionBasis,
    pub reconstruction_quality: pe_core_types::ReconstructionQuality,
}

impl FrameAdmissionInputs {
    /// Rebuild the confirmed prefix, using the same effects and disposition policy as recovery.
    pub(crate) fn rebuild_ledger(
        &self,
        wallet: WalletAddress,
    ) -> Result<pe_position_ledger::PositionLedger, FrameAdmissionError> {
        use pe_copy_signal_engine::{PositionSnapshot, PositionState};
        use pe_core_types::{OutcomeId, SourceTimestamp, VenueMarketId};
        use pe_position_ledger::{AppliedEffect, LedgerMutation, PositionLedger};
        let mut positions = HashMap::new();
        if let Some(anchor) = &self.ledger_anchor {
            if anchor.wallet != wallet
                || Some(anchor.anchor_seq) != self.coverage.anchor_seq
                || Some(anchor.activity_cutoff_unix) != self.coverage.activity_cutoff_unix
                || Some(anchor.anchored_at_unix) != self.coverage.anchored_at_unix
            {
                return Err(FrameAdmissionError::InvalidPrefix(
                    "anchor differs from frozen coverage",
                ));
            }
            let balances: Vec<(String, u16, pe_core_types::ShareAmount)> =
                serde_json::from_str(&anchor.balances_json)?;
            for (market, outcome, amount) in balances {
                if positions
                    .insert(
                        MarketOutcomeId::new(MarketId(VenueMarketId(market)), OutcomeId(outcome)),
                        PositionState {
                            long_contracts: amount,
                            short_contracts: pe_core_types::ShareAmount::ZERO,
                        },
                    )
                    .is_some()
                {
                    return Err(FrameAdmissionError::InvalidPrefix(
                        "anchor repeats a balance",
                    ));
                }
            }
        } else if self.coverage.anchor_seq.is_some() {
            return Err(FrameAdmissionError::InvalidPrefix(
                "coverage has no frozen anchor",
            ));
        }
        let mut ledger = PositionLedger::from_snapshots(HashMap::from([(
            wallet,
            PositionSnapshot { wallet, positions },
        )]));
        if let Some(anchor) = &self.ledger_anchor
            && crate::position_seeder::wallet_ledger_hash(&ledger, wallet)
                .map_err(|error| FrameAdmissionError::Capture(Box::new(error)))?
                != anchor.ledger_hash_after
        {
            return Err(FrameAdmissionError::InvalidPrefix(
                "anchor balance hash differs",
            ));
        }
        let mut previous = None;
        let mut start = 0;
        while let Some(first) = self.ledger_groups.get(start) {
            let mut end = start + 1;
            while self
                .ledger_groups
                .get(end)
                .is_some_and(|group| group.source_epoch == first.source_epoch)
            {
                end += 1;
            }
            let mut mutations = Vec::new();
            let mut expected = Vec::new();
            for group in &self.ledger_groups[start..end] {
                let key = (group.source_epoch, &group.source_trade_id.0);
                if previous.is_some_and(|previous| key <= previous)
                    || self
                        .coverage
                        .activity_cutoff_unix
                        .is_some_and(|cutoff| group.source_epoch <= cutoff)
                {
                    return Err(FrameAdmissionError::InvalidPrefix(
                        "ledger groups are outside their ordered prefix",
                    ));
                }
                previous = Some(key);
                if !crate::paper_recovery::applied_disposition(
                    &group.source_trade_id,
                    &group.disposition,
                )
                .map_err(|error| FrameAdmissionError::History(Box::new(error)))?
                {
                    continue;
                }
                let applied = AppliedEffect::from_document(&group.proof_json)?;
                let source_time = OffsetDateTime::from_unix_timestamp(group.source_epoch)?;
                mutations.push(LedgerMutation {
                    source_trade_id: group.source_trade_id.clone(),
                    transaction_hash: group.source_trade_id.0.clone(),
                    wallet,
                    source_time: SourceTimestamp(source_time),
                    effect: applied.effect.clone(),
                });
                expected.push(applied);
            }
            if ledger.apply_all_or_none(&mutations)? != expected {
                return Err(FrameAdmissionError::InvalidPrefix(
                    "ledger effects differ from frozen history",
                ));
            }
            start = end;
        }
        Ok(ledger)
    }

    pub(crate) fn positions(
        &self,
        wallet: WalletAddress,
    ) -> Result<pe_copy_signal_engine::PositionSnapshot, FrameAdmissionError> {
        let mut positions = HashMap::new();
        for row in &self.ledger_rows {
            if row.wallet != wallet
                || positions
                    .insert(
                        MarketOutcomeId::new(row.market_id.clone(), row.outcome_id),
                        pe_copy_signal_engine::PositionState {
                            long_contracts: row.long_contracts,
                            short_contracts: row.short_contracts,
                        },
                    )
                    .is_some()
            {
                return Err(FrameAdmissionError::InvalidPrefix("invalid ledger capture"));
            }
        }
        Ok(pe_copy_signal_engine::PositionSnapshot { wallet, positions })
    }
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

pub(crate) fn canonical_bytes(value: &impl Serialize) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&serde_json::to_value(value)?)
}

pub(crate) fn consumed_history(
    state: &PaperStateDb,
    wallet: WalletAddress,
) -> Result<Vec<MarketId>, pe_paper_state::PaperStateError> {
    let mut markets = state
        .gate_history()?
        .remove(&wallet)
        .unwrap_or_default()
        .into_iter()
        .collect::<Vec<_>>();
    markets.sort_by_key(ToString::to_string);
    Ok(markets)
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameDecisionProof {
    pub admission_receipt: AppendReceipt,
    pub inputs: FrameAdmissionInputs,
}

pub(crate) fn frame_revision(inputs: &FrameAdmissionInputs) -> Result<String, serde_json::Error> {
    let mut hash = blake3::Hasher::new();
    hash.update(b"prediction-edge/activity-frame-decision/v1\0");
    hash.update(&canonical_bytes(inputs)?);
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
            unresolved_obligation: true,
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
