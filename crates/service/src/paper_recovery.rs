//! Startup recovery for the paper trader: reconcile the SQLite mirror against the
//! event log, and rehydrate the in-memory leader `PositionLedger` from the mirror.
//!
//! Lives in the service tier (not in `paper-state`) because both steps need the private legacy
//! fill decoder and `PositionSnapshot`; `paper-state` deliberately depends on neither.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use pe_copy_signal_engine::{PositionSnapshot, PositionState};
use pe_core_types::{
    AccountId, CollateralAmount, EventSeq, MarketId, MarketOutcomeId, OutcomeId,
    PolymarketConditionId, Price, ShareAmount, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_event_log::{AppendReceipt, EventEnvelope, LogTailBinding, Reader};
use pe_execution_core::EconomicPrepared;
use pe_paper_state::{FillRecord, FillRow, PaperStateDb};
use pe_position_ledger::{
    AppliedEffect, LedgerEffectDocumentError, LedgerError, LedgerMutation, PositionLedger,
};
use pe_risk_engine::RiskHaltCause;
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_core::OrderIntent;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::bucket_commit::DecisionContinuationV3;
use crate::decision_replay::{
    AuthorityEvidence, DecisionEvidenceAccumulator, TerminalDispositionEvidence,
};
use crate::orchestrator::{pending_terminal, recorded_fill_terminal, render_pending_evidence};
use crate::position_seeder::ledger_capture;
use crate::supabase_sink::supabase_fill_from;

pub const PAPER_LOG_SCHEMA_VERSION: u32 = 2;
/// Current paper financial meaning. A changed value seals the active qualification before use.
pub const FINANCIAL_SEMANTIC_VERSION: u32 = 1;

/// Schema-one price provenance retained only by the service's legacy decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) enum LegacyFillSource {
    ClobBestAsk,
    Fallback,
    #[default]
    LeaderHaircut,
}

/// Schema-one payload retained only for compatibility reads before the financial era.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LegacyPaperFill {
    pub intent: OrderIntent,
    pub simulated_fill_price: Price,
    pub simulated_at: SourceTimestamp,
    #[serde(default)]
    pub fill_source: LegacyFillSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
pub enum PaperLogRecord {
    FinancialPrepared {
        expected_authority: ExpectedAuthority,
        payload: FinancialPayload,
    },
    FinancialFinal {
        prepared_receipt: AppendReceipt,
        result: FinancialResult,
    },
    MembershipChanged {
        reason: MembershipReason,
        removed: Vec<WalletAddress>,
        added: Vec<WalletAddress>,
        capacity: usize,
        ranking_batch_id: Option<i64>,
        evidence: serde_json::Value,
    },
    RiskHaltChanged {
        owner: RiskHaltOwner,
        cause: RiskHaltCause,
        state: HaltState,
        evidence: serde_json::Value,
    },
    QualificationStarted(Box<QualificationStarted>),
    PortfolioMark(Box<PortfolioMark>),
    QualificationSealed(Box<QualificationSealed>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectedAuthority {
    pub qualification_start_receipt: AppendReceipt,
    pub prior_completed_prepared_sequence: Option<EventSeq>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(clippy::large_enum_variant)]
pub enum FinancialPayload {
    Fill {
        operation: PaperFillOperationIdentity,
        economic: EconomicPrepared,
    },
    Resolution {
        condition_id: PolymarketConditionId,
        payout_by_outcome_index_json: String,
        resolution_source_receipt: AppendReceipt,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperFillOperationIdentity {
    pub leader_wallet: WalletAddress,
    pub source_trade_id: SourceTradeId,
    pub observed_at_bucket: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FinancialResult {
    Fill {
        canonical: CanonicalFillResult,
    },
    Resolution {
        canonical: CanonicalResolutionResult,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalFillResult {
    pub outcome: String,
    pub bankroll: Decimal,
    pub applied_prepared_seq: EventSeq,
    pub quantity: ShareAmount,
    pub principal: CollateralAmount,
    pub fee: CollateralAmount,
    pub fill_price: Price,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalResolutionResult {
    pub outcome: String,
    pub bankroll: Decimal,
    pub applied_prepared_seq: EventSeq,
    pub credit: CollateralAmount,
    pub settled_at_unix: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MembershipReason {
    FullRerank,
    KnockoutInactivity,
    KnockoutInactivityHardCap,
    KnockoutUnderperformance,
    RankerRotation,
    CapacityChange,
    Initial,
}

/// Structural membership publication passed through the orchestrator writer lock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipChange {
    pub reason: MembershipReason,
    pub removed: Vec<WalletAddress>,
    pub added: Vec<WalletAddress>,
    pub capacity: usize,
    pub ranking_batch_id: Option<i64>,
    pub evidence: serde_json::Value,
}

impl MembershipChange {
    #[must_use]
    pub fn into_record(self) -> PaperLogRecord {
        PaperLogRecord::MembershipChanged {
            reason: self.reason,
            removed: self.removed,
            added: self.added,
            capacity: self.capacity,
            ranking_batch_id: self.ranking_batch_id,
            evidence: self.evidence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "account")]
pub enum RiskHaltOwner {
    Paper,
    LiveAccount(AccountId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HaltState {
    Engaged,
    Released,
}

/// Rebuild the active risk-cause set from the current financial era. SQLite metadata is
/// deliberately not involved; the synchronized paper prefix remains the sole owner.
#[must_use]
pub fn active_risk_halts(era: &PaperEra) -> HashSet<(RiskHaltOwner, RiskHaltCause)> {
    let mut active = HashSet::new();
    for frame in &era.frames {
        let PaperLogFrame::Record(PaperLogRecord::RiskHaltChanged {
            owner,
            cause,
            state,
            ..
        }) = &frame.frame
        else {
            continue;
        };
        let key = (owner.clone(), *cause);
        match state {
            HaltState::Engaged => {
                active.insert(key);
            }
            HaltState::Released => {
                active.remove(&key);
            }
        }
    }
    active
}

#[derive(Debug, thiserror::Error)]
pub enum MembershipReplayError {
    #[error("QualificationStarted membership repeats wallet {0}")]
    DuplicateInitial(WalletAddress),
    #[error("QualificationStarted wallet {0} is absent from its pinned ranking batch")]
    MissingInitial(WalletAddress),
    #[error("MembershipChanged removes absent wallet {0}")]
    MissingRemoval(WalletAddress),
    #[error("MembershipChanged adds existing wallet {0}")]
    DuplicateAddition(WalletAddress),
    #[error("MembershipChanged result {actual} exceeds capacity {capacity}")]
    Capacity { actual: usize, capacity: usize },
    #[error("MembershipChanged at sequence {sequence} has no replacement_entries evidence")]
    MissingReplacementEntries { sequence: u64 },
    #[error("MembershipChanged replacement_entries decode failed at sequence {sequence}: {source}")]
    ReplacementEntries {
        sequence: u64,
        source: serde_json::Error,
    },
    #[error(
        "MembershipChanged replacement evidence disagrees with its wallet mutation at sequence {sequence}"
    )]
    ReplacementMismatch { sequence: u64 },
}

/// The structurally applied watchlist generation rebuilt from the durable paper prefix.
#[derive(Debug)]
pub struct ReplayedMembership {
    pub watchlist: Watchlist,
    pub last_ranking_batch_id: i64,
}

fn watchlist_with_entries(mut watchlist: Watchlist, entries: Vec<WatchlistEntry>) -> Watchlist {
    let active_count = entries
        .iter()
        .filter(|entry| entry.tier == WatchlistTier::Active)
        .count();
    let total = entries.len();
    watchlist.entries = entries;
    watchlist.active_count = active_count;
    watchlist.incubator_count = total.saturating_sub(active_count);
    watchlist
}

fn replacement_entries(
    frame: &ScannedPaperFrame,
    evidence: &serde_json::Value,
) -> Result<Vec<WatchlistEntry>, MembershipReplayError> {
    let value = evidence.get("replacement_entries").cloned().ok_or(
        MembershipReplayError::MissingReplacementEntries {
            sequence: frame.receipt.sequence.0,
        },
    )?;
    serde_json::from_value(value).map_err(|source| MembershipReplayError::ReplacementEntries {
        sequence: frame.receipt.sequence.0,
        source,
    })
}

/// Reconstruct the sole structural watchlist generation from Start's pinned ranking rows and
/// subsequent synchronized membership records. Score refreshes and durable fence removals remain
/// owned by their existing projections and therefore do not appear here.
///
/// `start_batch` must be the result of `supabase_reader::fetch_batch` for
/// `QualificationStarted.ranking_batch_id`. Later additions are recovered only from each durable
/// record's sealed `replacement_entries`, never from the moving ranking view.
pub fn replay_membership(
    era: &PaperEra,
    start_batch: Watchlist,
) -> Result<Option<ReplayedMembership>, MembershipReplayError> {
    let Some((_, start)) = &era.start else {
        return Ok(None);
    };
    let mut present = HashSet::with_capacity(start.membership.len());
    for wallet in &start.membership {
        if !present.insert(*wallet) {
            return Err(MembershipReplayError::DuplicateInitial(*wallet));
        }
    }
    let mut entries = start_batch
        .entries
        .iter()
        .filter(|entry| present.contains(&entry.wallet))
        .cloned()
        .collect::<Vec<_>>();
    let available = entries
        .iter()
        .map(|entry| entry.wallet)
        .collect::<HashSet<_>>();
    if let Some(missing) = start
        .membership
        .iter()
        .find(|wallet| !available.contains(wallet))
    {
        return Err(MembershipReplayError::MissingInitial(*missing));
    }

    let mut last_ranking_batch_id = start.ranking_batch_id;
    for frame in &era.frames {
        let PaperLogFrame::Record(PaperLogRecord::MembershipChanged {
            removed,
            added,
            capacity,
            ranking_batch_id,
            evidence,
            ..
        }) = &frame.frame
        else {
            continue;
        };
        for wallet in removed {
            if !present.remove(wallet) {
                return Err(MembershipReplayError::MissingRemoval(*wallet));
            }
        }
        for wallet in added {
            if !present.insert(*wallet) {
                return Err(MembershipReplayError::DuplicateAddition(*wallet));
            }
        }
        if present.len() > *capacity {
            return Err(MembershipReplayError::Capacity {
                actual: present.len(),
                capacity: *capacity,
            });
        }

        let replacements = replacement_entries(frame, evidence)?;
        let removed = removed.iter().copied().collect::<HashSet<_>>();
        entries.retain(|entry| !removed.contains(&entry.wallet));
        let mut entry_wallets = entries
            .iter()
            .map(|entry| entry.wallet)
            .collect::<HashSet<_>>();
        for replacement in replacements {
            if entries.len() >= *capacity {
                break;
            }
            if !removed.contains(&replacement.wallet) && entry_wallets.insert(replacement.wallet) {
                entries.push(replacement);
            }
        }
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.leader_score_bps.0));
        entries.truncate(*capacity);
        entry_wallets = entries.iter().map(|entry| entry.wallet).collect();
        if entry_wallets != present {
            return Err(MembershipReplayError::ReplacementMismatch {
                sequence: frame.receipt.sequence.0,
            });
        }
        if let Some(batch_id) = ranking_batch_id {
            last_ranking_batch_id = *batch_id;
        }
    }
    Ok(Some(ReplayedMembership {
        watchlist: watchlist_with_entries(start_batch, entries),
        last_ranking_batch_id,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationStarted {
    pub starting_bankroll: CollateralAmount,
    pub paper_prefix: TailBinding,
    pub source_prefix: TailBinding,
    pub live_prefix: TailBinding,
    pub artifact_blake3: String,
    pub static_config_hash: String,
    pub hot_config_hash: String,
    pub generation: String,
    pub activation_id: String,
    pub ranking_batch_id: i64,
    pub membership: Vec<WalletAddress>,
    pub membership_proofs_hash: String,
    pub schema_version: u32,
    pub parser_version: u32,
    pub financial_semantic_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TailBinding {
    pub physical_tail: u64,
    pub last_sequence: Option<EventSeq>,
    pub last_hash: String,
}

impl From<&LogTailBinding> for TailBinding {
    fn from(value: &LogTailBinding) -> Self {
        Self {
            physical_tail: value.physical_tail,
            last_sequence: value.last_sequence,
            last_hash: value.last_hash.to_hex().to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortfolioMark {
    pub boundary_receipt: AppendReceipt,
    pub cutoff_unix: i64,
    pub source_tail: TailBinding,
    pub financial_prefix_seq: Option<EventSeq>,
    pub prices: Vec<PaperMarkPrice>,
    pub cash: Decimal,
    pub equity: Decimal,
    pub invalid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperMarkPrice {
    pub market_id: String,
    pub outcome_id: u16,
    pub price: Option<Price>,
    pub sample_unix: Option<i64>,
    pub receipt: Option<AppendReceipt>,
    pub invalid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationSealed {
    pub start_receipt: AppendReceipt,
    pub source_prefix: TailBinding,
    pub financial_prefix: TailBinding,
    pub live_prefix: TailBinding,
    pub decision_evidence_digest: String,
    pub sealed_cutoff_unix: i64,
    pub reason: SealReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum SealReason {
    Complete,
    InsufficientEvidence(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum PaperLogFrame {
    LegacyFill,
    Record(PaperLogRecord),
}

#[derive(Debug, Clone)]
pub struct ScannedPaperFrame {
    pub envelope: EventEnvelope,
    pub receipt: AppendReceipt,
    pub frame: PaperLogFrame,
    pub(crate) legacy_fill: Option<LegacyPaperFill>,
}

impl ScannedPaperFrame {
    pub(crate) fn legacy_fill(&self) -> Option<&LegacyPaperFill> {
        self.legacy_fill.as_ref()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PaperLogScanError {
    #[error("paper log read failed: {0}")]
    EventLog(#[from] pe_event_log::LogError),
    #[error("paper log schema {schema_version} is unsupported at sequence {sequence}")]
    UnsupportedSchema { sequence: u64, schema_version: u32 },
    #[error("paper log payload decode failed at sequence {sequence}: {source}")]
    Decode {
        sequence: u64,
        source: serde_json::Error,
    },
    #[error("paper log contains distinct qualification starts")]
    ConflictingStart,
    #[error("paper financial protocol is invalid at sequence {sequence}: {reason}")]
    FinancialProtocol { sequence: u64, reason: String },
}

pub fn scan_paper_log(path: &Path) -> Result<Vec<ScannedPaperFrame>, PaperLogScanError> {
    let mut frames = Vec::new();
    for item in Reader::replay(path)? {
        let (sequence, envelope) = item?;
        let (frame, legacy_fill) = match envelope.schema_version {
            1 => serde_json::from_slice(&envelope.payload)
                .map(|fill| (PaperLogFrame::LegacyFill, Some(fill)))
                .map_err(|source| PaperLogScanError::Decode {
                    sequence: sequence.0,
                    source,
                })?,
            PAPER_LOG_SCHEMA_VERSION => serde_json::from_slice(&envelope.payload)
                .map(|record| (PaperLogFrame::Record(record), None))
                .map_err(|source| PaperLogScanError::Decode {
                    sequence: sequence.0,
                    source,
                })?,
            schema_version => {
                return Err(PaperLogScanError::UnsupportedSchema {
                    sequence: sequence.0,
                    schema_version,
                });
            }
        };
        let receipt = AppendReceipt {
            sequence,
            this_hash: envelope.this_hash,
        };
        frames.push(ScannedPaperFrame {
            envelope,
            receipt,
            frame,
            legacy_fill,
        });
    }
    validate_qualification_starts(&frames)?;
    validate_financial_pairs(&frames)?;
    Ok(frames)
}

#[derive(Debug)]
pub struct PaperEra {
    pub start: Option<(AppendReceipt, QualificationStarted)>,
    pub frames: Vec<ScannedPaperFrame>,
}

#[must_use]
pub fn paper_era(frames: Vec<ScannedPaperFrame>) -> PaperEra {
    let mut start = None::<(AppendReceipt, QualificationStarted)>;
    let mut era_frames = Vec::new();
    for frame in frames {
        let candidate = match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::QualificationStarted(value)) => {
                Some((frame.receipt, value.as_ref().clone()))
            }
            _ => None,
        };
        if let Some((receipt, candidate)) = candidate {
            // `scan_paper_log` rejects every second physical Start. Preserve the first Start
            // defensively for callers that construct an era view from already-validated frames.
            if start.is_none() {
                start = Some((receipt, candidate));
                era_frames.clear();
            }
        }
        era_frames.push(frame);
    }
    PaperEra {
        start,
        frames: era_frames,
    }
}

fn validate_qualification_starts(frames: &[ScannedPaperFrame]) -> Result<(), PaperLogScanError> {
    let mut start_seen = false;
    for frame in frames {
        let PaperLogFrame::Record(PaperLogRecord::QualificationStarted(_)) = &frame.frame else {
            continue;
        };
        if start_seen {
            return Err(PaperLogScanError::ConflictingStart);
        }
        start_seen = true;
    }
    Ok(())
}

fn validate_financial_pairs(frames: &[ScannedPaperFrame]) -> Result<(), PaperLogScanError> {
    let mut prepared = Vec::<(AppendReceipt, &FinancialPayload)>::new();
    let mut finalized = Vec::<AppendReceipt>::new();
    let mut unmatched = None::<AppendReceipt>;
    let mut completed = None::<EventSeq>;
    let mut active_start = None::<AppendReceipt>;
    for frame in frames {
        match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::QualificationStarted(_))
                if active_start.is_none() =>
            {
                // An equal Start payload can be observed again after an interrupted seeding
                // attempt, but it does not establish a new era or reset financial ordering.
                // `validate_qualification_starts` has already rejected a distinct payload.
                active_start = Some(frame.receipt);
                unmatched = None;
                completed = None;
            }
            PaperLogFrame::Record(PaperLogRecord::QualificationStarted(_)) => {}
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                expected_authority,
                payload,
            }) => {
                let Some(start) = active_start else {
                    return Err(PaperLogScanError::FinancialProtocol {
                        sequence: frame.receipt.sequence.0,
                        reason: "Prepared precedes QualificationStarted".to_owned(),
                    });
                };
                if unmatched.is_some() {
                    return Err(PaperLogScanError::FinancialProtocol {
                        sequence: frame.receipt.sequence.0,
                        reason: "later Prepared overtakes an unmatched Prepared".to_owned(),
                    });
                }
                if expected_authority.qualification_start_receipt != start
                    || expected_authority.prior_completed_prepared_sequence != completed
                {
                    return Err(PaperLogScanError::FinancialProtocol {
                        sequence: frame.receipt.sequence.0,
                        reason: "Prepared Start or predecessor differs from the verified prefix"
                            .to_owned(),
                    });
                }
                prepared.push((frame.receipt, payload));
                unmatched = Some(frame.receipt);
            }
            PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt,
                result,
            }) => {
                let Some((_, payload)) = prepared
                    .iter()
                    .find(|(receipt, _)| receipt == prepared_receipt)
                else {
                    return Err(PaperLogScanError::FinancialProtocol {
                        sequence: frame.receipt.sequence.0,
                        reason: "Final references no earlier Prepared receipt".to_owned(),
                    });
                };
                if finalized.contains(prepared_receipt) {
                    return Err(PaperLogScanError::FinancialProtocol {
                        sequence: frame.receipt.sequence.0,
                        reason: "Prepared receipt has more than one Final".to_owned(),
                    });
                }
                finalized.push(*prepared_receipt);
                let kind_matches = matches!(
                    (payload, result),
                    (FinancialPayload::Fill { .. }, FinancialResult::Fill { .. })
                        | (
                            FinancialPayload::Resolution { .. },
                            FinancialResult::Resolution { .. }
                        )
                );
                let applied_sequence = match result {
                    FinancialResult::Fill { canonical } => canonical.applied_prepared_seq,
                    FinancialResult::Resolution { canonical } => canonical.applied_prepared_seq,
                };
                if !kind_matches || applied_sequence != prepared_receipt.sequence {
                    return Err(PaperLogScanError::FinancialProtocol {
                        sequence: frame.receipt.sequence.0,
                        reason: "Final kind or applied Prepared sequence differs".to_owned(),
                    });
                }
                if unmatched != Some(*prepared_receipt) {
                    return Err(PaperLogScanError::FinancialProtocol {
                        sequence: frame.receipt.sequence.0,
                        reason: "Final does not complete the oldest unmatched Prepared".to_owned(),
                    });
                }
                unmatched = None;
                completed = Some(prepared_receipt.sequence);
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn oldest_unmatched_prepared(era: &PaperEra) -> Option<&ScannedPaperFrame> {
    let completed = era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt, ..
            }) => Some(*prepared_receipt),
            _ => None,
        })
        .collect::<Vec<_>>();
    era.frames.iter().find(|frame| {
        matches!(
            &frame.frame,
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { .. })
        ) && !completed.contains(&frame.receipt)
    })
}

#[cfg(test)]
mod paper_log_tests {
    #![allow(clippy::unwrap_used)]

    use std::collections::HashMap;
    use std::sync::Arc;

    use pe_copy_signal_engine::{SignalConfig, TradeProvenance};
    use pe_core_types::{
        BasisPoints, ContractQty, KellyFraction, LeaderAction, MarketId, OutcomeId,
        PolymarketTokenId, Probability, ProbabilityPpm, ReceivedAt, ReconstructionQuality, Side,
        SourceId, SourceTimestamp, StrategyId, VenueMarketId,
    };
    use pe_event_log::{ContentType, EnvelopeIn, Writer};
    use pe_execution_core::{
        AdmissionReceipts, BalanceAudit, ECONOMIC_PREPARED_VERSION, FeeAudit, LadderAskAudit,
        LadderPlanAudit, LiveAdmissionArtifactAudit, LiveMarketEvidenceAudit, MarketSelection,
        RiskAudit, RiskDecisionAudit, SizingAudit, SizingModeAudit,
    };
    use pe_resolver_card::{
        VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
    };
    use pe_risk_engine::RiskSnapshot;
    use pe_source_polymarket_public::FixtureFetcher;
    use pe_strategy_winner_follow::{ExecutionMode, WinnerFollowConfig, WinnerFollowStrategy};
    use pe_trader_index::Watchlist;
    use pe_venue_core::OrderIntent;
    use pe_venue_polymarket::CompactFeeSchedule;
    use rust_decimal_macros::dec;
    use tempfile::tempdir;
    use time::OffsetDateTime;
    use tokio::sync::mpsc;

    use super::*;
    use crate::bucket_commit::{DecisionContinuationFacts, FrozenDecisionBasis};
    use crate::clob_book::FixtureClobBookFetcher;
    use crate::entry_gate::CopyEntryGateConfig;
    use crate::health::new_shared_health;
    use crate::live_watchlist::LiveWatchlist;
    use crate::mid_price_cache::MidPriceCache;
    use crate::orchestrator::{Orchestrator, OrchestratorConfig};
    use crate::runtime_config::RuntimeConfig;

    fn receipt(sequence: u64) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap_or(u8::MAX); 32]),
        }
    }

    fn tail() -> TailBinding {
        TailBinding {
            physical_tail: 5,
            last_sequence: Some(EventSeq(0)),
            last_hash: "00".repeat(32),
        }
    }

    fn wallet() -> WalletAddress {
        WalletAddress::from_hex("0x1111111111111111111111111111111111111111").unwrap()
    }

    fn origin_main_wallet() -> WalletAddress {
        WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    /// PASS: after the financial Start resets only financial projections, the production
    /// orchestrator constructor validates and boots over a retained body-hashed v2 terminal row.
    #[test]
    fn post_start_boot_accepts_retained_legacy_terminal_decision() {
        const LEGACY_TERMINAL: &str =
            include_str!("../tests/fixtures/decision_replay_origin_main_v2_terminal.json");

        let dir = tempdir().unwrap();
        let state_path = dir.path().join("paper-state.sqlite");
        let paper_state = Arc::new(PaperStateDb::open(&state_path).unwrap());
        let applied_configuration: RuntimeConfig = serde_json::from_value(serde_json::json!({
            "era": "legacy17",
            "active_watchlist_size": 100,
            "mode": "paper",
            "max_fill_price": "0.85",
            "min_fill_price": "0.15",
            "min_resolution_horizon_secs": 60,
            "max_resolution_horizon_secs": 172_800,
            "price_impact_cap_bps": 100,
            "flip_human_approved": false,
            "kelly_fraction_above_default_human_approved": false,
            "kelly_fraction_override": null,
            "per_trade_cap": {"kind": "mode_default"},
            "slippage_rate": "0.01",
            "sizing_mode": {"kind": "kelly"},
            "sizing_dollar_usd": "0",
            "sizing_contracts": 0,
            "legacy_compatibility": {
                "fill_mode": "clob_best_ask",
                "polymarket_fee_rate": "0.04"
            }
        }))
        .unwrap();
        let applied_configuration_hash = applied_configuration.canonical_hash();
        assert_eq!(
            applied_configuration_hash,
            "f602cee694f90f8e48cdd43e70d6d9398879a9991662492af82ec4f7df31b222"
        );
        let facts = DecisionContinuationFacts {
            source_trade_id: SourceTradeId("g2:fill".to_owned()),
            semantic_revision: "semantic-v2".to_owned(),
            transaction_hash: "0xtransaction".to_owned(),
            wallet: origin_main_wallet(),
            source_epoch: 1_700_000_000,
            market_id: MarketId(VenueMarketId(format!("0x{}", "2".repeat(40)))),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price(dec!(0.40)),
            share_amount: ShareAmount::from_whole(10).unwrap(),
            provenance: TradeProvenance::RestPoll,
            pre_bucket_action: LeaderAction::Entry,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
            gate_result: "admitted".to_owned(),
            applied_configuration_hash: applied_configuration_hash.clone(),
            applied_configuration,
            frozen_basis: FrozenDecisionBasis {
                win_rate_p: Probability::ZERO,
                bankroll: Decimal::ZERO,
            },
            decision_inputs: serde_json::json!({"fixed_end": 1_700_000_010_i64, "pages": 1}),
        };
        let facts_json = serde_json::to_string(&facts).unwrap();
        let frozen_inputs_json =
            format!(r#"{{"version":2,{}"#, facts_json.strip_prefix('{').unwrap());

        let connection = rusqlite::Connection::open(&state_path).unwrap();
        connection
            .execute(
                "INSERT INTO decision_pending
                    (source_trade_id, semantic_revision, wallet_hex, source_epoch,
                     frozen_inputs_json, post_commit_inputs_json, state, terminal_disposition,
                     updated_at_unix)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'terminal', 'fill', ?7)",
                rusqlite::params![
                    facts.source_trade_id.0,
                    facts.semantic_revision,
                    facts.wallet.to_string(),
                    facts.source_epoch,
                    frozen_inputs_json,
                    LEGACY_TERMINAL,
                    1_700_000_001_i64,
                ],
            )
            .unwrap();
        drop(connection);

        paper_state
            .reset_financial_era(
                receipt(20),
                CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
            )
            .unwrap();
        assert_eq!(paper_state.decision_pending_history().unwrap().len(), 1);

        let (_control_tx, control_rx) = mpsc::channel(1);
        let result = Orchestrator::new(
            LiveWatchlist::new(Watchlist {
                entries: Vec::new(),
                snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
                active_count: 0,
                incubator_count: 0,
            }),
            OrchestratorConfig {
                bankroll: dec!(100),
                mode: ExecutionMode::Paper,
                signal_config: SignalConfig::default(),
                max_resolution_horizon_secs: 0,
                min_resolution_horizon_secs: 0,
                max_fill_price: Decimal::ZERO,
                min_fill_price: Decimal::ZERO,
                price_impact_cap_bps: 100,
                activity_ws_enabled: false,
                copy_latency_budget_secs: 2,
                watchlist_writer_lock: None,
                entry_gate_config: CopyEntryGateConfig,
                runtime_config: None,
                live_accounts: None,
            },
            WinnerFollowStrategy::new(WinnerFollowConfig::default()),
            Writer::open(dir.path().join("paper.log")).unwrap(),
            paper_state,
            PositionLedger::new(),
            new_shared_health(false),
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), String::new()),
            control_rx,
            None,
            None,
            None,
            Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
        );
        assert!(result.is_ok());
    }

    fn watchlist_entry(wallet: WalletAddress, score: i32) -> WatchlistEntry {
        WatchlistEntry {
            wallet,
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(score),
            lcb_5pct_bps: BasisPoints(score),
            win_rate_bps: BasisPoints(6_000),
            closed_trades_in_window: 20,
            reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
        }
    }

    fn watchlist(entries: Vec<WatchlistEntry>) -> Watchlist {
        let active_count = entries.len();
        Watchlist {
            entries,
            snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            active_count,
            incubator_count: 0,
        }
    }

    fn economic() -> EconomicPrepared {
        let price = Price::new(dec!(0.5)).unwrap();
        let shares = ShareAmount::from_whole(2).unwrap();
        let principal = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();
        let source_receipts = AdmissionReceipts {
            gamma: receipt(1),
            clob_long: receipt(2),
            clob_compact: receipt(3),
        };
        EconomicPrepared {
            version: ECONOMIC_PREPARED_VERSION,
            market: MarketSelection {
                condition_id: PolymarketConditionId("condition".to_owned()),
                outcome_index: 0,
                token_id: PolymarketTokenId("token".to_owned()),
                side: Side::Buy,
                market_id: "market".to_owned(),
            },
            admission: LiveAdmissionArtifactAudit {
                market: LiveMarketEvidenceAudit {
                    condition_id: PolymarketConditionId("condition".to_owned()),
                    ordered_outcome_token_ids: [
                        PolymarketTokenId("token".to_owned()),
                        PolymarketTokenId("other".to_owned()),
                    ],
                    neg_risk: false,
                    minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                    minimum_order_size: shares,
                    observed_at_unix: 1,
                    schema_version: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                settlement: VenueSettlementRecord {
                    schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                    condition_id: PolymarketConditionId("condition".to_owned()),
                    status: VenueResolutionStatus::Unresolved,
                    raw_evidence_hash: "settlement".to_owned(),
                    source_timestamp_unix: Some(1),
                    observed_at_unix: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                fee_schedule: CompactFeeSchedule::Zero,
                scheduled_end_unix: Some(100),
                receipts: source_receipts,
            },
            ladder: LadderPlanAudit {
                used_asks: vec![LadderAskAudit { price, shares }],
                best_ask: price,
                limit_price: price,
                minimum_shares: shares,
                principal,
            },
            book_receipt: receipt(4),
            observation: None,
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
                    concentration_caps: None,
                },
                decision: RiskDecisionAudit::Approved,
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: 1_800_000_000_000,
            },
            balance: BalanceAudit {
                cash_before: CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
                worst_case_debit: principal,
                price_impact_cap_bps: 100,
                chase_ceiling: price,
                band_floor: Price::ZERO,
                band_ceiling_exclusive: Price::ONE,
            },
            applied_configuration_hash: "config".to_owned(),
        }
    }

    fn start(activation_id: &str) -> QualificationStarted {
        QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
            paper_prefix: tail(),
            source_prefix: tail(),
            live_prefix: tail(),
            artifact_blake3: "artifact".to_owned(),
            static_config_hash: "static".to_owned(),
            hot_config_hash: "hot".to_owned(),
            generation: "generation".to_owned(),
            activation_id: activation_id.to_owned(),
            ranking_batch_id: 7,
            membership: vec![wallet()],
            membership_proofs_hash: "proofs".to_owned(),
            schema_version: 1,
            parser_version: 1,
            financial_semantic_version: 1,
        }
    }

    fn prepared(source_trade_id: &str) -> PaperLogRecord {
        prepared_after(receipt(0), None, source_trade_id)
    }

    fn prepared_after(
        start_receipt: AppendReceipt,
        prior: Option<EventSeq>,
        source_trade_id: &str,
    ) -> PaperLogRecord {
        PaperLogRecord::FinancialPrepared {
            expected_authority: ExpectedAuthority {
                qualification_start_receipt: start_receipt,
                prior_completed_prepared_sequence: prior,
            },
            payload: FinancialPayload::Fill {
                operation: PaperFillOperationIdentity {
                    leader_wallet: wallet(),
                    source_trade_id: SourceTradeId(source_trade_id.to_owned()),
                    observed_at_bucket: 1,
                },
                economic: economic(),
            },
        }
    }

    fn final_fill(prepared_receipt: AppendReceipt) -> PaperLogRecord {
        PaperLogRecord::FinancialFinal {
            prepared_receipt,
            result: FinancialResult::Fill {
                canonical: CanonicalFillResult {
                    outcome: "applied".to_owned(),
                    bankroll: dec!(99),
                    applied_prepared_seq: prepared_receipt.sequence,
                    quantity: ShareAmount::from_whole(2).unwrap(),
                    principal: CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                    fee: CollateralAmount::ZERO,
                    fill_price: Price::new(dec!(0.5)).unwrap(),
                },
            },
        }
    }

    fn legacy_fill() -> LegacyPaperFill {
        LegacyPaperFill {
            intent: OrderIntent {
                strategy_id: StrategyId("winner-follow".to_owned()),
                market_id: MarketId(VenueMarketId("market".to_owned())),
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                contracts: ContractQty(1),
                limit_price: Price::new(dec!(0.5)).unwrap(),
                validity_seconds: 30,
                idempotency_key: "legacy".to_owned(),
            },
            simulated_fill_price: Price::new(dec!(0.5)).unwrap(),
            simulated_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            fill_source: LegacyFillSource::LeaderHaircut,
        }
    }

    fn append<T: Serialize>(writer: &mut Writer, schema_version: u32, value: &T) -> AppendReceipt {
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("paper-test".to_owned()),
                schema_version,
                parser_version: 1,
                observed_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
                received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(value).unwrap(),
            })
            .unwrap()
    }

    #[test]
    fn every_paper_record_variant_round_trips() {
        let records = vec![
            prepared("fill"),
            PaperLogRecord::FinancialPrepared {
                expected_authority: ExpectedAuthority {
                    qualification_start_receipt: receipt(0),
                    prior_completed_prepared_sequence: Some(EventSeq(9)),
                },
                payload: FinancialPayload::Resolution {
                    condition_id: PolymarketConditionId("condition".to_owned()),
                    payout_by_outcome_index_json: "[1,0]".to_owned(),
                    resolution_source_receipt: receipt(8),
                },
            },
            final_fill(receipt(5)),
            PaperLogRecord::FinancialFinal {
                prepared_receipt: receipt(6),
                result: FinancialResult::Resolution {
                    canonical: CanonicalResolutionResult {
                        outcome: "applied".to_owned(),
                        bankroll: dec!(101),
                        applied_prepared_seq: EventSeq(6),
                        credit: CollateralAmount::from_decimal_exact(dec!(2)).unwrap(),
                        settled_at_unix: 10,
                    },
                },
            },
            PaperLogRecord::MembershipChanged {
                reason: MembershipReason::FullRerank,
                removed: Vec::new(),
                added: vec![wallet()],
                capacity: 1,
                ranking_batch_id: Some(7),
                evidence: serde_json::json!({"batch": 7}),
            },
            PaperLogRecord::RiskHaltChanged {
                owner: RiskHaltOwner::Paper,
                cause: RiskHaltCause::AbsoluteLoss,
                state: HaltState::Engaged,
                evidence: serde_json::json!({"equity": "90"}),
            },
            PaperLogRecord::QualificationStarted(Box::new(start("activation"))),
            PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                boundary_receipt: receipt(7),
                cutoff_unix: 20,
                source_tail: tail(),
                financial_prefix_seq: Some(EventSeq(6)),
                prices: vec![PaperMarkPrice {
                    market_id: "market".to_owned(),
                    outcome_id: 0,
                    price: Some(Price::new(dec!(0.6)).unwrap()),
                    sample_unix: Some(19),
                    receipt: Some(receipt(8)),
                    invalid: None,
                }],
                cash: dec!(99),
                equity: dec!(100.2),
                invalid: None,
            })),
            PaperLogRecord::QualificationSealed(Box::new(QualificationSealed {
                start_receipt: receipt(1),
                source_prefix: tail(),
                financial_prefix: tail(),
                live_prefix: tail(),
                decision_evidence_digest: "decisions".to_owned(),
                sealed_cutoff_unix: 30,
                reason: SealReason::Complete,
            })),
        ];
        for record in records {
            let bytes = serde_json::to_vec(&record).unwrap();
            assert_eq!(
                serde_json::from_slice::<PaperLogRecord>(&bytes).unwrap(),
                record
            );
        }
    }

    #[test]
    fn every_paper_record_enum_value_round_trips() {
        for reason in [
            MembershipReason::FullRerank,
            MembershipReason::KnockoutInactivity,
            MembershipReason::KnockoutInactivityHardCap,
            MembershipReason::KnockoutUnderperformance,
            MembershipReason::RankerRotation,
            MembershipReason::CapacityChange,
            MembershipReason::Initial,
        ] {
            let bytes = serde_json::to_vec(&reason).unwrap();
            assert_eq!(
                serde_json::from_slice::<MembershipReason>(&bytes).unwrap(),
                reason
            );
        }
        for owner in [
            RiskHaltOwner::Paper,
            RiskHaltOwner::LiveAccount(AccountId::new("live").unwrap()),
        ] {
            let bytes = serde_json::to_vec(&owner).unwrap();
            assert_eq!(
                serde_json::from_slice::<RiskHaltOwner>(&bytes).unwrap(),
                owner
            );
        }
        for state in [HaltState::Engaged, HaltState::Released] {
            let bytes = serde_json::to_vec(&state).unwrap();
            assert_eq!(serde_json::from_slice::<HaltState>(&bytes).unwrap(), state);
        }
        for reason in [
            SealReason::Complete,
            SealReason::InsufficientEvidence("missing mark".to_owned()),
        ] {
            let bytes = serde_json::to_vec(&reason).unwrap();
            assert_eq!(
                serde_json::from_slice::<SealReason>(&bytes).unwrap(),
                reason
            );
        }
    }

    #[test]
    fn economic_record_hash_and_debits_are_exact() {
        let economic = economic();
        assert_eq!(economic.core_hash().unwrap(), economic.core_hash().unwrap());
        assert_eq!(
            economic.all_in_debit().unwrap(),
            CollateralAmount::from_decimal_exact(dec!(1)).unwrap()
        );
        assert_eq!(
            economic.worst_case_all_in_debit().unwrap(),
            CollateralAmount::from_decimal_exact(dec!(1)).unwrap()
        );
    }

    #[test]
    fn mixed_schema_log_scans_legacy_and_versioned_frames() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("paper.log");
        let mut writer = Writer::open(&path).unwrap();
        append(&mut writer, 1, &legacy_fill());
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::MembershipChanged {
                reason: MembershipReason::Initial,
                removed: Vec::new(),
                added: vec![wallet()],
                capacity: 1,
                ranking_batch_id: None,
                evidence: serde_json::json!({}),
            },
        );
        drop(writer);

        let frames = scan_paper_log(&path).unwrap();
        assert_eq!(frames.len(), 2);
        assert!(matches!(frames[0].frame, PaperLogFrame::LegacyFill));
        assert!(frames[0].legacy_fill().is_some());
        assert!(matches!(frames[1].frame, PaperLogFrame::Record(_)));
        assert_eq!(frames[0].receipt.sequence, EventSeq(0));
        assert_eq!(frames[1].receipt.sequence, EventSeq(1));
    }

    /// PASS: later membership uses its durable replacement entry and advances the applied batch;
    /// FAIL: replay consults a moving ranking or reconstructs only wallet identifiers.
    #[test]
    fn structural_membership_replays_start_and_later_replacement_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("membership.log");
        let mut writer = Writer::open(&path).unwrap();
        let initial = wallet();
        let added = WalletAddress([2; 20]);
        let replacement = watchlist_entry(added, 800);
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("activation"))),
        );
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::MembershipChanged {
                reason: MembershipReason::FullRerank,
                removed: vec![initial],
                added: vec![added],
                capacity: 1,
                ranking_batch_id: Some(8),
                evidence: serde_json::json!({
                    "kind": "full_rerank",
                    "replacement_entries": [replacement]
                }),
            },
        );
        drop(writer);

        let era = paper_era(scan_paper_log(&path).unwrap());
        let replayed = replay_membership(&era, watchlist(vec![watchlist_entry(initial, 500)]))
            .unwrap()
            .unwrap();
        assert_eq!(replayed.last_ranking_batch_id, 8);
        assert_eq!(replayed.watchlist.entries.len(), 1);
        assert_eq!(replayed.watchlist.entries[0].wallet, added);
        assert_eq!(
            replayed.watchlist.entries[0].leader_score_bps,
            BasisPoints(800)
        );
    }

    /// PASS: a crash before the new MembershipChanged record leaves boot on Start's A/N;
    /// FAIL: an unrecorded newer publication changes the replayed entries or marker.
    #[test]
    fn structural_membership_crash_before_record_keeps_start_generation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("membership-crash.log");
        let mut writer = Writer::open(&path).unwrap();
        let initial = wallet();
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("activation"))),
        );
        drop(writer);

        let era = paper_era(scan_paper_log(&path).unwrap());
        let replayed = replay_membership(&era, watchlist(vec![watchlist_entry(initial, 500)]))
            .unwrap()
            .unwrap();
        assert_eq!(replayed.last_ranking_batch_id, 7);
        assert_eq!(replayed.watchlist.entries.len(), 1);
        assert_eq!(replayed.watchlist.entries[0].wallet, initial);
    }

    /// PASS: Start cannot be rebuilt when its durable wallet is absent from the pinned batch;
    /// FAIL: replay silently drops the wallet or substitutes a row from another generation.
    #[test]
    fn structural_membership_requires_every_start_wallet_in_pinned_batch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("membership-missing-start.log");
        let mut writer = Writer::open(&path).unwrap();
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("activation"))),
        );
        drop(writer);

        let era = paper_era(scan_paper_log(&path).unwrap());
        let other = WalletAddress([2; 20]);
        assert!(matches!(
            replay_membership(&era, watchlist(vec![watchlist_entry(other, 500)])),
            Err(MembershipReplayError::MissingInitial(missing)) if missing == wallet()
        ));
    }

    #[test]
    fn scanner_accepts_at_most_one_physical_start() {
        let dir = tempdir().unwrap();
        let zero_path = dir.path().join("zero.log");
        let mut zero = Writer::open(&zero_path).unwrap();
        append(&mut zero, 1, &legacy_fill());
        drop(zero);
        let zero = paper_era(scan_paper_log(&zero_path).unwrap());
        assert!(zero.start.is_none());
        assert_eq!(zero.frames.len(), 1);

        let one_path = dir.path().join("one.log");
        let mut one = Writer::open(&one_path).unwrap();
        append(&mut one, 1, &legacy_fill());
        append(
            &mut one,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("same"))),
        );
        append(
            &mut one,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("same"))),
        );
        drop(one);
        assert!(matches!(
            scan_paper_log(&one_path),
            Err(PaperLogScanError::ConflictingStart)
        ));

        let single_path = dir.path().join("single.log");
        let mut single = Writer::open(&single_path).unwrap();
        let first = append(
            &mut single,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("same"))),
        );
        drop(single);
        let single = paper_era(scan_paper_log(&single_path).unwrap());
        assert_eq!(single.start.as_ref().map(|value| value.0), Some(first));
        assert_eq!(single.frames.len(), 1);

        let conflict_path = dir.path().join("conflict.log");
        let mut conflict = Writer::open(&conflict_path).unwrap();
        for activation in ["first", "second"] {
            append(
                &mut conflict,
                PAPER_LOG_SCHEMA_VERSION,
                &PaperLogRecord::QualificationStarted(Box::new(start(activation))),
            );
        }
        drop(conflict);
        assert!(matches!(
            scan_paper_log(&conflict_path),
            Err(PaperLogScanError::ConflictingStart)
        ));
    }

    #[test]
    fn oldest_unmatched_prepared_handles_none_one_and_two() {
        let dir = tempdir().unwrap();
        let empty_path = dir.path().join("empty.log");
        drop(Writer::open(&empty_path).unwrap());
        let empty = paper_era(scan_paper_log(&empty_path).unwrap());
        assert!(oldest_unmatched_prepared(&empty).is_none());

        let one_path = dir.path().join("one-prepared.log");
        let mut writer = Writer::open(&one_path).unwrap();
        let start_receipt = append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("era"))),
        );
        let only = append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &prepared_after(start_receipt, None, "only"),
        );
        drop(writer);
        let one = paper_era(scan_paper_log(&one_path).unwrap());
        assert_eq!(
            oldest_unmatched_prepared(&one).map(|frame| frame.receipt),
            Some(only)
        );

        let two_path = dir.path().join("two-prepared.log");
        let mut writer = Writer::open(&two_path).unwrap();
        let start_receipt = append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("era"))),
        );
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &prepared_after(start_receipt, None, "first"),
        );
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &prepared_after(start_receipt, None, "second"),
        );
        drop(writer);
        assert!(matches!(
            scan_paper_log(&two_path),
            Err(PaperLogScanError::FinancialProtocol { .. })
        ));

        let completed_path = dir.path().join("completed.log");
        let mut writer = Writer::open(&completed_path).unwrap();
        let start_receipt = append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("completed"))),
        );
        let completed = append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &prepared_after(start_receipt, None, "done"),
        );
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &final_fill(completed),
        );
        drop(writer);
        let completed = paper_era(scan_paper_log(&completed_path).unwrap());
        assert!(oldest_unmatched_prepared(&completed).is_none());
    }

    #[test]
    fn active_risk_causes_rebuild_from_edges_after_start() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("risk.log");
        let mut writer = Writer::open(&path).unwrap();
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(start("risk"))),
        );
        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::RiskHaltChanged {
                owner: RiskHaltOwner::Paper,
                cause: RiskHaltCause::AbsoluteLoss,
                state: HaltState::Engaged,
                evidence: serde_json::json!({"bound":"start"}),
            },
        );
        let era = paper_era(scan_paper_log(&path).unwrap());
        assert!(
            active_risk_halts(&era).contains(&(RiskHaltOwner::Paper, RiskHaltCause::AbsoluteLoss))
        );

        append(
            &mut writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::RiskHaltChanged {
                owner: RiskHaltOwner::Paper,
                cause: RiskHaltCause::AbsoluteLoss,
                state: HaltState::Released,
                evidence: serde_json::json!({"bound":"release"}),
            },
        );
        drop(writer);
        let era = paper_era(scan_paper_log(&path).unwrap());
        assert!(active_risk_halts(&era).is_empty());
    }
}

/// Replay event-log fills whose SQLite commit was lost to a crash (those with
/// `seq > last_applied_event_seq`) back into `paper-state`. Returns the count newly
/// applied. No-op when the log does not exist yet (first run). See issue #282 AC5.
pub fn reconcile_paper_state(event_log_path: &Path, paper_state: &PaperStateDb) -> Result<usize> {
    if !event_log_path.exists() {
        return Ok(0);
    }
    let mut applied = 0usize;
    let era = paper_era(
        scan_paper_log(event_log_path)
            .with_context(|| format!("scan paper log {}", event_log_path.display()))?,
    );
    anyhow::ensure!(
        era.start.is_none(),
        "blind local paper replay is forbidden after QualificationStarted"
    );
    // `reconcile_fill` is itself idempotent (guards on `last_applied_event_seq` and the
    // `fills` PK), so every frame is offered to it; already-mirrored fills are skipped.
    for frame in era.frames {
        let seq = frame.receipt.sequence;
        let Some(fill) = frame.legacy_fill() else {
            continue;
        };
        let quantity = ShareAmount::from_whole(fill.intent.contracts.0)
            .map_err(|error| anyhow::anyhow!("legacy paper quantity: {error}"))?;
        let principal = CollateralAmount::from_decimal_exact(
            fill.simulated_fill_price
                .0
                .checked_mul(quantity.to_decimal())
                .ok_or_else(|| anyhow::anyhow!("legacy paper principal overflow"))?,
        )
        .map_err(|error| anyhow::anyhow!("legacy paper principal: {error}"))?;
        let record = FillRecord {
            idempotency_key: fill.intent.idempotency_key.clone(),
            market_id: fill.intent.market_id.clone(),
            outcome_id: fill.intent.outcome_id,
            side: fill.intent.side,
            quantity,
            fill_price: fill.simulated_fill_price,
            principal,
            fee: CollateralAmount::ZERO,
        };
        let source_trade_id = i64::try_from(seq.0)
            .ok()
            .and_then(|_| {
                supabase_fill_from(&FillRow {
                    idempotency_key: record.idempotency_key.clone(),
                    market_id: record.market_id.clone(),
                    outcome_id: record.outcome_id,
                    side: record.side,
                    quantity: record.quantity,
                    fill_price: record.fill_price,
                    principal: record.principal,
                    fee: record.fee,
                    event_seq: seq,
                    prepared_seq: seq,
                    source_receipt_seq: None,
                })
            })
            .and_then(|row| row.source_trade_id)
            .map(SourceTradeId);
        let pending_row = match source_trade_id.as_ref() {
            Some(id) => paper_state
                .open_decision_pending()
                .context("load decision_pending for paper-log recovery")?
                .into_iter()
                .find(|row| &row.source_trade_id == id),
            None => None,
        };
        if let (Some(source_trade_id), Some(pending_row)) =
            (source_trade_id.as_ref(), pending_row.as_ref())
        {
            let continuation = DecisionContinuationV3::from_durable(pending_row)
                .context("decode pending paper-log continuation")?;
            let evidence = DecisionEvidenceAccumulator::from_pending_checkpoint(pending_row)
                .context("decode pending paper-log evidence checkpoint")?;
            let leader = paper_state
                .leader_positions()
                .context("load pending paper-log leader mirror")?
                .into_iter()
                .find(|leader| {
                    leader.wallet == continuation.facts.wallet
                        && leader.market_id == continuation.facts.market_id
                        && leader.outcome_id == continuation.facts.outcome_id
                })
                .context("pending paper-log continuation has no leader mirror")?;
            let fill_pending = render_pending_evidence(
                Some(&evidence),
                AuthorityEvidence::local("recovered_from_paper_log"),
                recorded_fill_terminal(&record, seq),
            )
            .context("render recovered paper-log fill evidence")?;
            let settled_pending = render_pending_evidence(
                Some(&evidence),
                AuthorityEvidence::local("recovered_settled_refusal"),
                TerminalDispositionEvidence::settled_refusal(),
            )
            .context("render recovered paper-log refusal evidence")?;
            let outcome = paper_state
                .commit_fill_with_flip_pending(
                    source_trade_id,
                    &leader,
                    &record,
                    seq,
                    None,
                    fill_pending.as_ref().map(pending_terminal),
                    settled_pending.as_ref().map(pending_terminal),
                )
                .context("commit recovered pending paper-log fill")?;
            if matches!(outcome, pe_paper_state::FillCommitOutcome::Applied(_)) {
                applied += 1;
            }
            continue;
        }
        if paper_state.reconcile_fill(&record, seq)? {
            applied += 1;
        }
    }
    Ok(applied)
}

/// Rebuild the leader `PositionLedger` from the `leader_positions` mirror so the
/// first post-restart trade by a leader with an existing position classifies as
/// Add/Trim/Flip, not a fresh Entry (issue #282 AC4).
pub fn build_leader_ledger(paper_state: &PaperStateDb) -> Result<PositionLedger> {
    let mut snapshots: HashMap<_, PositionSnapshot> = HashMap::new();
    for row in paper_state
        .leader_positions()
        .context("read leader positions")?
    {
        let snap = snapshots
            .entry(row.wallet)
            .or_insert_with(|| PositionSnapshot {
                wallet: row.wallet,
                positions: HashMap::new(),
            });
        let key = MarketOutcomeId::new(row.market_id.clone(), row.outcome_id);
        snap.positions.insert(
            key,
            PositionState {
                long_contracts: row.long_contracts,
                short_contracts: row.short_contracts,
            },
        );
    }
    Ok(PositionLedger::from_snapshots(snapshots))
}

#[derive(Debug, thiserror::Error)]
pub enum WalletLedgerReplayError {
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("position anchor {anchor_seq} balances are malformed: {source}")]
    AnchorBalances {
        anchor_seq: i64,
        source: serde_json::Error,
    },
    #[error("position anchor sequence expected {expected}, found {actual}")]
    AnchorSequence { expected: i64, actual: i64 },
    #[error("position anchor {anchor_seq} contains duplicate {market_id} outcome {outcome}")]
    DuplicateAnchorBalance {
        anchor_seq: i64,
        market_id: String,
        outcome: u16,
    },
    #[error("position anchor {anchor_seq} ledger hash does not match its balances")]
    AnchorHashMismatch { anchor_seq: i64 },
    #[error("activity group {source_trade_id} effect document: {source}")]
    EffectDocument {
        source_trade_id: SourceTradeId,
        source: LedgerEffectDocumentError,
    },
    #[error("activity group {source_trade_id} revision changed during replay")]
    RevisionMismatch { source_trade_id: SourceTradeId },
    #[error("activity group {source_trade_id} has unknown disposition {disposition}")]
    UnknownDisposition {
        source_trade_id: SourceTradeId,
        disposition: String,
    },
    #[error("activity group {source_trade_id} has invalid source epoch {source_epoch}")]
    InvalidSourceEpoch {
        source_trade_id: SourceTradeId,
        source_epoch: i64,
    },
    #[error("activity group {source_trade_id} ledger effect: {message}")]
    Ledger {
        source_trade_id: SourceTradeId,
        message: String,
    },
    #[error(
        "activity bucket for {wallet} at {source_epoch} recomputed clamped residuals {actual:?}, expected {expected:?}"
    )]
    ClampedResidualMismatch {
        wallet: WalletAddress,
        source_epoch: i64,
        expected: Vec<Option<u64>>,
        actual: Vec<Option<u64>>,
    },
    #[error("wallet ledger capture failed: {0}")]
    LedgerCapture(String),
}

/// Rebuild one wallet from its append-only anchors and versioned post-cutoff effects.
pub fn replay_wallet_ledger(
    paper_state: &PaperStateDb,
    wallet: WalletAddress,
) -> Result<PositionLedger, WalletLedgerReplayError> {
    let anchors = paper_state.position_anchors(&wallet)?;
    let Some(first_anchor) = anchors.first() else {
        return Ok(PositionLedger::new());
    };
    for (expected, anchor) in anchors.iter().enumerate() {
        let expected = i64::try_from(expected).unwrap_or(i64::MAX);
        if anchor.anchor_seq != expected {
            return Err(WalletLedgerReplayError::AnchorSequence {
                expected,
                actual: anchor.anchor_seq,
            });
        }
    }

    let mut ledger = PositionLedger::new();
    install_replayed_anchor(&mut ledger, paper_state, first_anchor)?;
    let groups = paper_state.activity_groups_after(&wallet, first_anchor.activity_cutoff_unix)?;
    let mut next_group = 0usize;
    for anchor in anchors.iter().skip(1) {
        let bucket_start = next_group;
        while let Some(group) = groups.get(next_group) {
            if group.source_epoch > anchor.activity_cutoff_unix {
                break;
            }
            next_group = next_group.saturating_add(1);
        }
        apply_replayed_groups(
            &mut ledger,
            paper_state,
            wallet,
            &groups[bucket_start..next_group],
        )?;
        install_replayed_anchor(&mut ledger, paper_state, anchor)?;
    }
    apply_replayed_groups(&mut ledger, paper_state, wallet, &groups[next_group..])?;
    Ok(ledger)
}

fn install_replayed_anchor(
    ledger: &mut PositionLedger,
    paper_state: &PaperStateDb,
    anchor: &pe_paper_state::PositionAnchorRow,
) -> Result<(), WalletLedgerReplayError> {
    let balances: Vec<(String, u16, ShareAmount)> = serde_json::from_str(&anchor.balances_json)
        .map_err(|source| WalletLedgerReplayError::AnchorBalances {
            anchor_seq: anchor.anchor_seq,
            source,
        })?;
    let mut positions = HashMap::new();
    for (market_id, outcome, amount) in balances {
        let key = MarketOutcomeId::new(
            MarketId(VenueMarketId(market_id.clone())),
            OutcomeId(outcome),
        );
        if positions
            .insert(
                key,
                PositionState {
                    long_contracts: amount,
                    short_contracts: ShareAmount::ZERO,
                },
            )
            .is_some()
        {
            return Err(WalletLedgerReplayError::DuplicateAnchorBalance {
                anchor_seq: anchor.anchor_seq,
                market_id,
                outcome,
            });
        }
    }
    ledger.replace_wallet_snapshot(anchor.wallet, positions);
    let capture = ledger_capture(ledger, paper_state, anchor.wallet)
        .map_err(|error| WalletLedgerReplayError::LedgerCapture(error.to_string()))?;
    if capture.hash != anchor.ledger_hash_after {
        return Err(WalletLedgerReplayError::AnchorHashMismatch {
            anchor_seq: anchor.anchor_seq,
        });
    }
    Ok(())
}

fn apply_replayed_groups(
    ledger: &mut PositionLedger,
    paper_state: &PaperStateDb,
    wallet: WalletAddress,
    groups: &[pe_paper_state::ActivityGroupRow],
) -> Result<(), WalletLedgerReplayError> {
    let mut bucket_start = 0usize;
    while let Some(first) = groups.get(bucket_start) {
        let mut bucket_end = bucket_start.saturating_add(1);
        while groups
            .get(bucket_end)
            .is_some_and(|group| group.source_epoch == first.source_epoch)
        {
            bucket_end = bucket_end.saturating_add(1);
        }
        apply_replayed_bucket(
            ledger,
            paper_state,
            wallet,
            &groups[bucket_start..bucket_end],
        )?;
        bucket_start = bucket_end;
    }
    Ok(())
}

fn apply_replayed_bucket(
    ledger: &mut PositionLedger,
    paper_state: &PaperStateDb,
    wallet: WalletAddress,
    groups: &[pe_paper_state::ActivityGroupRow],
) -> Result<(), WalletLedgerReplayError> {
    let Some(first) = groups.first() else {
        return Ok(());
    };
    let source_time =
        time::OffsetDateTime::from_unix_timestamp(first.source_epoch).map_err(|_| {
            WalletLedgerReplayError::InvalidSourceEpoch {
                source_trade_id: first.source_trade_id.clone(),
                source_epoch: first.source_epoch,
            }
        })?;
    let mut mutations = Vec::new();
    let mut expected = Vec::new();
    for group in groups {
        let durable = paper_state.activity_group_state(&group.source_trade_id)?;
        verify_replayed_group_revision(durable.as_ref(), group)?;
        let applied_effect = AppliedEffect::from_document(&group.proof_json).map_err(|source| {
            WalletLedgerReplayError::EffectDocument {
                source_trade_id: group.source_trade_id.clone(),
                source,
            }
        })?;
        if !applied_disposition(&group.source_trade_id, &group.disposition)? {
            continue;
        }
        expected.push(applied_effect.clamped_residual);
        mutations.push(LedgerMutation {
            source_trade_id: group.source_trade_id.clone(),
            transaction_hash: group.source_trade_id.0.clone(),
            wallet,
            source_time: SourceTimestamp(source_time),
            effect: applied_effect.effect,
        });
    }
    let applied =
        ledger
            .apply_all_or_none(&mutations)
            .map_err(|error| WalletLedgerReplayError::Ledger {
                source_trade_id: ledger_error_source_id(&error),
                message: error.to_string(),
            })?;
    let actual = applied
        .into_iter()
        .map(|effect| effect.clamped_residual)
        .collect::<Vec<_>>();
    if actual != expected {
        return Err(WalletLedgerReplayError::ClampedResidualMismatch {
            wallet,
            source_epoch: first.source_epoch,
            expected,
            actual,
        });
    }
    Ok(())
}

fn ledger_error_source_id(error: &LedgerError) -> SourceTradeId {
    match error {
        LedgerError::InvalidMapping { source_trade_id }
        | LedgerError::Underflow { source_trade_id }
        | LedgerError::Overflow { source_trade_id }
        | LedgerError::Conversion { source_trade_id }
        | LedgerError::UnknownEffect { source_trade_id } => source_trade_id.clone(),
    }
}

fn verify_replayed_group_revision(
    durable: Option<&pe_paper_state::ActivityGroupState>,
    group: &pe_paper_state::ActivityGroupRow,
) -> Result<(), WalletLedgerReplayError> {
    if durable.is_none_or(|state| {
        state.semantic_revision != group.semantic_revision
            || state.source_epoch != group.source_epoch
            || state.disposition != group.disposition
    }) {
        return Err(WalletLedgerReplayError::RevisionMismatch {
            source_trade_id: group.source_trade_id.clone(),
        });
    }
    Ok(())
}

fn applied_disposition(
    source_trade_id: &SourceTradeId,
    disposition: &str,
) -> Result<bool, WalletLedgerReplayError> {
    let applied = matches!(
        disposition,
        "applied"
            | "wallet_fenced_applied"
            | "decision_pending"
            | "not_copy_eligible"
            | "not_an_entry"
            | "not_first_entry"
            | "not_buy"
            | "wallet_history_incomplete"
            | "ambiguous_first_entry_same_second"
            | "order_dependent_equal_second_action"
            | "stale_fallback_past_copy_budget"
            | "stale_activity_ws_past_copy_budget"
    );
    let not_applied = matches!(
        disposition,
        "raw_only"
            | "reanchor_required_redemption"
            | "reanchor_required_late_group"
            | "anchor_covered"
            | "anchor_covered_late"
            | "wallet_fenced"
            | "revised_applied_aggregate"
            | "late_group_after_bucket_commit"
            | "invalid_mapping"
            | "position_underflow"
            | "position_overflow"
            | "conversion_unknown_conditions"
            | "unknown_activity_effect"
            | "order_dependent_equal_second"
    );
    if applied || not_applied {
        Ok(applied)
    } else {
        Err(WalletLedgerReplayError::UnknownDisposition {
            source_trade_id: source_trade_id.clone(),
            disposition: disposition.to_owned(),
        })
    }
}

#[cfg(test)]
mod anchor_replay_tests {
    use super::*;

    #[test]
    fn changed_revision_is_a_typed_replay_failure() {
        let source_trade_id = SourceTradeId("g2:revision".to_owned());
        let group = pe_paper_state::ActivityGroupRow {
            source_trade_id: source_trade_id.clone(),
            source_epoch: 100,
            semantic_revision: "revision-a".to_owned(),
            disposition: "applied".to_owned(),
            proof_json: "{}".to_owned(),
        };
        let durable = pe_paper_state::ActivityGroupState {
            transaction_hash: "0xtx".to_owned(),
            semantic_revision: "revision-b".to_owned(),
            source_epoch: 100,
            disposition: "applied".to_owned(),
        };
        assert!(matches!(
            verify_replayed_group_revision(Some(&durable), &group),
            Err(WalletLedgerReplayError::RevisionMismatch {
                source_trade_id: actual
            }) if actual == source_trade_id
        ));
    }
}
