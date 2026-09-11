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
    PolymarketConditionId, Price, ShareAmount, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_event_log::{AppendReceipt, EventEnvelope, LogTailBinding, Reader};
use pe_execution_core::EconomicPrepared;
use pe_paper_pnl::SettlementInfo;
use pe_paper_state::{FillRecord, FillRow, PaperStateDb, SettledMarketRow};
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
use crate::live_watchlist::replace_entries;
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
    CapacityChange,
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

/// Versioned binding stored in the legacy-named `membership_proofs_hash` Start field.
///
/// Keeping the durable field preserves existing Start constructors while the value now carries
/// both the canonical immutable preimages and their digest. Qualification never consults mutable
/// paper-state membership projections after Start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipProofBinding {
    version: u8,
    proof_hash: String,
    manifest: MembershipProofManifest,
}

/// Canonical ordered proof preimages for the membership installed by Start or one admission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipProofManifest {
    pub(crate) membership: Vec<WalletAddress>,
    pub(crate) proofs: Vec<MembershipWalletProof>,
}

/// Exact history, coverage, anchor, and position-validation rows accepted for one wallet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipWalletProof {
    pub(crate) wallet: WalletAddress,
    pub(crate) history: MembershipHistoryProof,
    pub(crate) coverage: MembershipCoverageProof,
    pub(crate) anchor: MembershipAnchorProof,
    pub(crate) validation: MembershipPositionValidationProof,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipHistoryProof {
    complete: bool,
    proof_json: String,
    updated_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipCoverageProof {
    activity_cutoff_unix: i64,
    coverage_generation: i64,
    reanchor_required: bool,
    anchor_seq: i64,
    anchored_at_unix: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipAnchorProof {
    anchor_seq: i64,
    anchored_at_unix: i64,
    activity_cutoff_unix: i64,
    balances_json: String,
    ledger_hash_after: String,
    proof_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipPositionValidationProof {
    ledger_hash: String,
    positions_proof_hash: String,
    activity_bounds_json: String,
    source_log_generation: String,
    proof_json: String,
    recorded_at_unix: i64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum MembershipProofError {
    #[error("paper state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("membership proof JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("membership repeats wallet {0}")]
    DuplicateWallet(WalletAddress),
    #[error("membership proof lacks complete history for {0}")]
    MissingHistory(WalletAddress),
    #[error("membership proof lacks installed anchor coverage for {0}")]
    MissingCoverage(WalletAddress),
    #[error("membership proof requires a new anchor for {0}")]
    ReanchorRequired(WalletAddress),
    #[error("membership proof lacks an anchor record for {0}")]
    MissingAnchor(WalletAddress),
    #[error("membership anchor and coverage disagree for {0}")]
    AnchorCoverageMismatch(WalletAddress),
    #[error("membership proof lacks a current position validation for {0}")]
    MissingValidation(WalletAddress),
    #[error("membership validation and anchor disagree for {0}")]
    ValidationAnchorMismatch(WalletAddress),
    #[error("membership proof manifest does not name the recorded membership")]
    MembershipMismatch,
    #[error("membership proof manifest and wallet preimages differ")]
    ProofWalletMismatch,
    #[error("membership proof binding is not canonical")]
    NonCanonicalBinding,
    #[error("membership proof binding version {0} is unsupported")]
    UnsupportedVersion(u8),
    #[error("membership proof digest differs from its immutable preimages")]
    DigestMismatch,
}

impl MembershipProofManifest {
    pub(crate) fn capture(
        state: &PaperStateDb,
        membership: &[WalletAddress],
    ) -> Result<Self, MembershipProofError> {
        let mut unique = HashSet::new();
        let mut proofs = Vec::with_capacity(membership.len());
        for wallet in membership {
            if !unique.insert(*wallet) {
                return Err(MembershipProofError::DuplicateWallet(*wallet));
            }
            let history = state
                .wallet_history_status(wallet)?
                .filter(|status| status.complete)
                .ok_or(MembershipProofError::MissingHistory(*wallet))?;
            let coverage = state.wallet_coverage(wallet)?;
            let (Some(activity_cutoff_unix), Some(anchor_seq), Some(anchored_at_unix)) = (
                coverage.activity_cutoff_unix,
                coverage.anchor_seq,
                coverage.anchored_at_unix,
            ) else {
                return Err(MembershipProofError::MissingCoverage(*wallet));
            };
            if coverage.reanchor_required {
                return Err(MembershipProofError::ReanchorRequired(*wallet));
            }
            let anchor = state
                .position_anchors(wallet)?
                .into_iter()
                .last()
                .ok_or(MembershipProofError::MissingAnchor(*wallet))?;
            if anchor.anchor_seq != anchor_seq
                || anchor.activity_cutoff_unix != activity_cutoff_unix
                || anchor.anchored_at_unix != anchored_at_unix
            {
                return Err(MembershipProofError::AnchorCoverageMismatch(*wallet));
            }
            let validation = state
                .position_validation(wallet)?
                .ok_or(MembershipProofError::MissingValidation(*wallet))?;
            if validation.ledger_hash != anchor.ledger_hash_after
                || validation.proof_json != anchor.proof_json
            {
                return Err(MembershipProofError::ValidationAnchorMismatch(*wallet));
            }
            proofs.push(MembershipWalletProof {
                wallet: *wallet,
                history: MembershipHistoryProof {
                    complete: history.complete,
                    proof_json: history.proof_json,
                    updated_at_unix: history.updated_at_unix,
                },
                coverage: MembershipCoverageProof {
                    activity_cutoff_unix,
                    coverage_generation: coverage.coverage_generation,
                    reanchor_required: coverage.reanchor_required,
                    anchor_seq,
                    anchored_at_unix,
                },
                anchor: MembershipAnchorProof {
                    anchor_seq: anchor.anchor_seq,
                    anchored_at_unix: anchor.anchored_at_unix,
                    activity_cutoff_unix: anchor.activity_cutoff_unix,
                    balances_json: anchor.balances_json,
                    ledger_hash_after: anchor.ledger_hash_after,
                    proof_json: anchor.proof_json,
                },
                validation: MembershipPositionValidationProof {
                    ledger_hash: validation.ledger_hash,
                    positions_proof_hash: validation.positions_proof_hash,
                    activity_bounds_json: validation.activity_bounds_json,
                    source_log_generation: validation.source_log_generation,
                    proof_json: validation.proof_json,
                    recorded_at_unix: validation.recorded_at_unix,
                },
            });
        }
        let manifest = Self {
            membership: membership.to_vec(),
            proofs,
        };
        manifest.verify(membership)?;
        Ok(manifest)
    }

    pub(crate) fn verify(&self, membership: &[WalletAddress]) -> Result<(), MembershipProofError> {
        if self.membership != membership {
            return Err(MembershipProofError::MembershipMismatch);
        }
        let mut unique = HashSet::new();
        for wallet in membership {
            if !unique.insert(*wallet) {
                return Err(MembershipProofError::DuplicateWallet(*wallet));
            }
        }
        if self.proofs.len() != membership.len()
            || self
                .proofs
                .iter()
                .zip(membership)
                .any(|(proof, wallet)| proof.wallet != *wallet)
        {
            return Err(MembershipProofError::ProofWalletMismatch);
        }
        for proof in &self.proofs {
            if !proof.history.complete {
                return Err(MembershipProofError::MissingHistory(proof.wallet));
            }
            for document in [
                &proof.history.proof_json,
                &proof.anchor.balances_json,
                &proof.anchor.proof_json,
                &proof.validation.activity_bounds_json,
                &proof.validation.proof_json,
            ] {
                serde_json::from_str::<serde_json::Value>(document)?;
            }
            if proof.coverage.reanchor_required {
                return Err(MembershipProofError::ReanchorRequired(proof.wallet));
            }
            if proof.coverage.anchor_seq != proof.anchor.anchor_seq
                || proof.coverage.activity_cutoff_unix != proof.anchor.activity_cutoff_unix
                || proof.coverage.anchored_at_unix != proof.anchor.anchored_at_unix
            {
                return Err(MembershipProofError::AnchorCoverageMismatch(proof.wallet));
            }
            if proof.validation.ledger_hash != proof.anchor.ledger_hash_after
                || proof.validation.proof_json != proof.anchor.proof_json
            {
                return Err(MembershipProofError::ValidationAnchorMismatch(proof.wallet));
            }
        }
        Ok(())
    }
}

impl MembershipProofBinding {
    const VERSION: u8 = 1;

    pub(crate) fn encode(
        manifest: MembershipProofManifest,
    ) -> Result<String, MembershipProofError> {
        let proof_hash = blake3::hash(&serde_json::to_vec(&manifest)?)
            .to_hex()
            .to_string();
        Ok(serde_json::to_string(&Self {
            version: Self::VERSION,
            proof_hash,
            manifest,
        })?)
    }

    pub(crate) fn decode_and_verify(
        encoded: &str,
        membership: &[WalletAddress],
    ) -> Result<MembershipProofManifest, MembershipProofError> {
        let binding: Self = serde_json::from_str(encoded)?;
        if binding.version != Self::VERSION {
            return Err(MembershipProofError::UnsupportedVersion(binding.version));
        }
        if serde_json::to_string(&binding)? != encoded {
            return Err(MembershipProofError::NonCanonicalBinding);
        }
        binding.manifest.verify(membership)?;
        let rederived = blake3::hash(&serde_json::to_vec(&binding.manifest)?)
            .to_hex()
            .to_string();
        if binding.proof_hash != rederived {
            return Err(MembershipProofError::DigestMismatch);
        }
        Ok(binding.manifest)
    }
}

/// Source-log artifact for the exact ranking rows used by one structural publication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RankingMembershipArtifact {
    pub(crate) batch_id: Option<i64>,
    pub(crate) entries: Vec<WatchlistEntry>,
}

/// Source-log artifact for a capacity generation and its exact published ranked set.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CapacityMembershipArtifact {
    pub(crate) generation: u64,
    pub(crate) target: u64,
    pub(crate) published_entries: Vec<WatchlistEntry>,
}

/// Source-log artifact containing the exact immutable admission proof preimages for one wallet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipAdmissionArtifact {
    pub(crate) wallet: WalletAddress,
    pub(crate) proof: MembershipProofManifest,
}

/// Policy and cursor inputs retained before the paper membership record is published.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KnockoutCausalArtifact {
    pub(crate) wallet: WalletAddress,
    pub(crate) evaluated_at_unix: i64,
    pub(crate) last_trade_unix: Option<i64>,
    pub(crate) inactivity_threshold_secs: u64,
    pub(crate) inactivity_hard_cap_secs: u64,
    pub(crate) demotion_min_trades: usize,
    pub(crate) demotion_cb_alpha: Decimal,
    pub(crate) demotion_pnl_window_secs: u64,
    pub(crate) fills: Vec<KnockoutFillArtifact>,
    pub(crate) settlements: Vec<KnockoutSettlementArtifact>,
}

/// Exact persisted fill fields consumed by the demotion-statistic owner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KnockoutFillArtifact {
    pub(crate) idempotency_key: String,
    pub(crate) market_id: MarketId,
    pub(crate) outcome_id: OutcomeId,
    pub(crate) side: Side,
    pub(crate) quantity: ShareAmount,
    pub(crate) fill_price: Price,
    pub(crate) principal: CollateralAmount,
    pub(crate) fee: CollateralAmount,
    pub(crate) event_seq: EventSeq,
    pub(crate) prepared_seq: EventSeq,
    pub(crate) source_receipt_seq: Option<EventSeq>,
}

impl KnockoutFillArtifact {
    pub(crate) fn from_row(row: &FillRow) -> Self {
        Self {
            idempotency_key: row.idempotency_key.clone(),
            market_id: row.market_id.clone(),
            outcome_id: row.outcome_id,
            side: row.side,
            quantity: row.quantity,
            fill_price: row.fill_price,
            principal: row.principal,
            fee: row.fee,
            event_seq: row.event_seq,
            prepared_seq: row.prepared_seq,
            source_receipt_seq: row.source_receipt_seq,
        }
    }

    pub(crate) fn to_row(&self) -> FillRow {
        FillRow {
            idempotency_key: self.idempotency_key.clone(),
            market_id: self.market_id.clone(),
            outcome_id: self.outcome_id,
            side: self.side,
            quantity: self.quantity,
            fill_price: self.fill_price,
            principal: self.principal,
            fee: self.fee,
            event_seq: self.event_seq,
            prepared_seq: self.prepared_seq,
            source_receipt_seq: self.source_receipt_seq,
        }
    }
}

/// Exact settlement values read by the demotion-statistic owner for retained fills.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KnockoutSettlementArtifact {
    pub(crate) market_id: MarketId,
    pub(crate) outcome_prices: Vec<Decimal>,
    pub(crate) credit_applied: Decimal,
    pub(crate) settled_at_unix: i64,
}

impl KnockoutSettlementArtifact {
    pub(crate) fn from_info(market_id: MarketId, info: SettlementInfo) -> Self {
        Self {
            market_id,
            outcome_prices: info.outcome_prices,
            credit_applied: info.credit_applied,
            settled_at_unix: info.settled_at_unix,
        }
    }

    pub(crate) fn to_row(&self) -> Result<SettledMarketRow, serde_json::Error> {
        Ok(SettledMarketRow {
            market_id: self.market_id.clone(),
            outcome_prices_json: serde_json::to_string(
                &self
                    .outcome_prices
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
            )?,
            credit_applied: self.credit_applied,
            settled_at_unix: self.settled_at_unix,
            prepared_seq: None,
            source_receipt_seq: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipAdmissionReceipt {
    pub(crate) wallet: WalletAddress,
    pub(crate) receipt: AppendReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SealedKnockoutEvidence {
    pub(crate) wallet: WalletAddress,
    pub(crate) reason: MembershipReason,
    pub(crate) causal_receipt: AppendReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum SealedMembershipEvidence {
    FullRerank {
        ranking_receipt: AppendReceipt,
        admission_receipts: Vec<MembershipAdmissionReceipt>,
    },
    KnockoutBackfill {
        evictions: Vec<SealedKnockoutEvidence>,
        ranking_receipt: Option<AppendReceipt>,
        admission_receipts: Vec<MembershipAdmissionReceipt>,
    },
    CapacityChange {
        generation: u64,
        config_receipt: AppendReceipt,
        admission_receipts: Vec<MembershipAdmissionReceipt>,
    },
}

impl SealedMembershipEvidence {
    /// Check the receipt identities that must exactly match the structural mutation derived under
    /// the publication lock. Artifact payload semantics are re-run later by qualification.
    pub(crate) fn matches_wallet_mutation(
        &self,
        removed: &[WalletAddress],
        added: &[WalletAddress],
    ) -> bool {
        let receipt_wallets = |receipts: &[MembershipAdmissionReceipt]| {
            receipts
                .iter()
                .map(|proof| proof.wallet)
                .collect::<HashSet<_>>()
        };
        let added_set = added.iter().copied().collect::<HashSet<_>>();
        match self {
            Self::FullRerank {
                admission_receipts, ..
            }
            | Self::CapacityChange {
                admission_receipts, ..
            } => {
                admission_receipts.len() == added.len()
                    && receipt_wallets(admission_receipts) == added_set
            }
            Self::KnockoutBackfill {
                evictions,
                admission_receipts,
                ..
            } => {
                let eviction_wallets = evictions
                    .iter()
                    .map(|eviction| eviction.wallet)
                    .collect::<HashSet<_>>();
                admission_receipts.len() == added.len()
                    && receipt_wallets(admission_receipts) == added_set
                    && evictions.len() == removed.len()
                    && eviction_wallets == removed.iter().copied().collect()
            }
        }
    }

    pub(crate) fn full_rerank(
        ranking_receipt: AppendReceipt,
        admission_receipts: Vec<MembershipAdmissionReceipt>,
    ) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(Self::FullRerank {
            ranking_receipt,
            admission_receipts,
        })
    }

    pub(crate) fn knockout_backfill(
        evictions: Vec<SealedKnockoutEvidence>,
        ranking_receipt: Option<AppendReceipt>,
        admission_receipts: Vec<MembershipAdmissionReceipt>,
    ) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(Self::KnockoutBackfill {
            evictions,
            ranking_receipt,
            admission_receipts,
        })
    }

    pub(crate) fn capacity_change(
        generation: u64,
        config_receipt: AppendReceipt,
        admission_receipts: Vec<MembershipAdmissionReceipt>,
    ) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(Self::CapacityChange {
            generation,
            config_receipt,
            admission_receipts,
        })
    }
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
    #[error("MembershipChanged evidence verification failed at sequence {sequence}: {source}")]
    Evidence {
        sequence: u64,
        source: crate::qualification::QualificationError,
    },
    #[error(
        "MembershipChanged source artifact disagrees with its reconstructed membership at sequence {sequence}"
    )]
    ReconstructionMismatch { sequence: u64 },
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

/// Reconstruct the sole structural watchlist generation from Start's pinned ranking rows and
/// subsequent synchronized membership records. Score refreshes and durable fence removals remain
/// owned by their existing projections and therefore do not appear here.
///
/// `start_batch` must be the result of `supabase_reader::fetch_batch` for
/// `QualificationStarted.ranking_batch_id`. Later replacement vectors are reconstructed and
/// verified from the immutable source-log artifacts named by each durable record, never from the
/// moving ranking view.
pub fn replay_membership(
    era: &PaperEra,
    start_batch: Watchlist,
    source_log: &Path,
) -> Result<Option<ReplayedMembership>, MembershipReplayError> {
    replay_membership_from(
        era,
        start_batch,
        MembershipReplaySource::SourceLog(source_log),
    )
}

/// Reconstruct membership from a process-wide source receipt index that boot already verified.
pub(crate) fn replay_membership_with_source(
    era: &PaperEra,
    start_batch: Watchlist,
    source: &crate::qualification::PublishedMembershipSource,
) -> Result<Option<ReplayedMembership>, MembershipReplayError> {
    replay_membership_from(era, start_batch, MembershipReplaySource::Published(source))
}

#[derive(Clone, Copy)]
enum MembershipReplaySource<'a> {
    SourceLog(&'a Path),
    Published(&'a crate::qualification::PublishedMembershipSource),
}

fn replay_membership_from(
    era: &PaperEra,
    start_batch: Watchlist,
    source: MembershipReplaySource<'_>,
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
    let Some(first_membership_sequence) = era.frames.iter().find_map(|frame| {
        matches!(
            &frame.frame,
            PaperLogFrame::Record(PaperLogRecord::MembershipChanged { .. })
        )
        .then_some(frame.receipt.sequence.0)
    }) else {
        return Ok(Some(ReplayedMembership {
            watchlist: watchlist_with_entries(start_batch, entries),
            last_ranking_batch_id,
        }));
    };
    let scanned_source;
    let membership_source = match source {
        MembershipReplaySource::SourceLog(source_log) => {
            scanned_source = crate::qualification::PublishedMembershipSource::scan(source_log)
                .map_err(|source| MembershipReplayError::Evidence {
                    sequence: first_membership_sequence,
                    source,
                })?;
            &scanned_source
        }
        MembershipReplaySource::Published(source) => source,
    };

    for frame in &era.frames {
        let PaperLogFrame::Record(
            record @ PaperLogRecord::MembershipChanged {
                removed,
                added,
                capacity,
                ranking_batch_id,
                ..
            },
        ) = &frame.frame
        else {
            continue;
        };
        let mut next_present = present.clone();
        for wallet in removed {
            if !next_present.remove(wallet) {
                return Err(MembershipReplayError::MissingRemoval(*wallet));
            }
        }
        for wallet in added {
            if !next_present.insert(*wallet) {
                return Err(MembershipReplayError::DuplicateAddition(*wallet));
            }
        }
        if next_present.len() > *capacity {
            return Err(MembershipReplayError::Capacity {
                actual: next_present.len(),
                capacity: *capacity,
            });
        }

        let replacements = crate::qualification::replay_published_membership_change(
            record,
            membership_source,
            &present,
        )
        .map_err(|source| MembershipReplayError::Evidence {
            sequence: frame.receipt.sequence.0,
            source,
        })?;
        present = next_present;
        let removed = removed.iter().copied().collect::<HashSet<_>>();
        entries = replace_entries(&entries, &removed, &replacements, *capacity);
        let entry_wallets = entries
            .iter()
            .map(|entry| entry.wallet)
            .collect::<HashSet<_>>();
        if entry_wallets != present {
            return Err(MembershipReplayError::ReconstructionMismatch {
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

    /// Bytes read through system calls by this process (Linux `/proc/self/io`, page-cache hits
    /// included), so a whole-log pass shows as about the file's length.
    #[cfg(target_os = "linux")]
    fn read_chars() -> u64 {
        std::fs::read_to_string("/proc/self/io")
            .unwrap()
            .lines()
            .find_map(|line| line.strip_prefix("rchar: "))
            .unwrap()
            .trim()
            .parse()
            .unwrap()
    }
    use crate::bucket_commit::{DecisionContinuationFacts, FrozenDecisionBasis};
    use crate::clob_book::FixtureClobBookFetcher;
    use crate::entry_gate::CopyEntryGateConfig;
    use crate::health::new_shared_health;
    use crate::live_watchlist::LiveWatchlist;
    use crate::mid_price_cache::MidPriceCache;
    use crate::orchestrator::{Orchestrator, OrchestratorConfig};
    use crate::watchlist_admission::{
        AdmissionPreparer, CAPACITY_CONFIG_SOURCE_ID, KNOCKOUT_CAUSAL_SOURCE_ID,
        MEMBERSHIP_ARTIFACT_PARSER_VERSION, MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
        RANKING_MEMBERSHIP_SOURCE_ID,
    };

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
        let applied_configuration = crate::bucket_commit::synthetic_legacy17_runtime_config();
        let applied_configuration_hash = applied_configuration.canonical_hash();
        assert_eq!(
            applied_configuration_hash,
            "f602cee694f90f8e48cdd43e70d6d9398879a9991662492af82ec4f7df31b222"
        );
        let facts = DecisionContinuationFacts {
            paper_freshness_policy: None,
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
        let frozen_inputs_json = crate::bucket_commit::pre_545_frozen_inputs(&facts);

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

    fn economic(financial_prefix: AppendReceipt) -> EconomicPrepared {
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
                economic: economic(start_receipt),
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

    fn append_membership_artifact<T: Serialize>(
        writer: &mut Writer,
        source_id: &str,
        value: &T,
    ) -> AppendReceipt {
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId(source_id.to_owned()),
                schema_version: MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
                parser_version: MEMBERSHIP_ARTIFACT_PARSER_VERSION,
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
            MembershipReason::CapacityChange,
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
        let economic = economic(receipt(0));
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
                reason: MembershipReason::FullRerank,
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

    /// PASS: restart replays records built by all three production evidence constructors from
    /// their source-log artifacts and restores the exact final structural membership;
    /// FAIL: any production evidence variant is unreadable or replay consults moving ranking.
    #[test]
    fn structural_membership_replays_every_production_evidence_variant() {
        let dir = tempdir().unwrap();
        let paper_path = dir.path().join("membership.log");
        let source_path = dir.path().join("source.log");
        let mut paper_writer = Writer::open(&paper_path).unwrap();
        let mut source_writer = Writer::open(&source_path).unwrap();
        let first = wallet();
        let second = WalletAddress([2; 20]);
        let third = WalletAddress([3; 20]);
        let fourth = WalletAddress([4; 20]);
        let mut started = start("activation");
        started.membership = vec![first, second, third, fourth];
        append(
            &mut paper_writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(started)),
        );

        let reranked = vec![
            watchlist_entry(first, 900),
            watchlist_entry(second, 800),
            watchlist_entry(third, 700),
        ];
        let ranking_receipt = append_membership_artifact(
            &mut source_writer,
            RANKING_MEMBERSHIP_SOURCE_ID,
            &RankingMembershipArtifact {
                batch_id: Some(8),
                entries: reranked,
            },
        );
        append(
            &mut paper_writer,
            PAPER_LOG_SCHEMA_VERSION,
            &MembershipChange {
                reason: MembershipReason::FullRerank,
                removed: vec![fourth],
                added: Vec::new(),
                capacity: 3,
                ranking_batch_id: Some(8),
                evidence: SealedMembershipEvidence::full_rerank(ranking_receipt, Vec::new())
                    .unwrap(),
            }
            .into_record(),
        );

        let knockout_receipt = append_membership_artifact(
            &mut source_writer,
            KNOCKOUT_CAUSAL_SOURCE_ID,
            &KnockoutCausalArtifact {
                wallet: third,
                evaluated_at_unix: 259_200,
                last_trade_unix: Some(0),
                inactivity_threshold_secs: 259_200,
                inactivity_hard_cap_secs: 604_800,
                demotion_min_trades: 10,
                demotion_cb_alpha: dec!(0.10),
                demotion_pnl_window_secs: 2_592_000,
                fills: Vec::new(),
                settlements: Vec::new(),
            },
        );
        append(
            &mut paper_writer,
            PAPER_LOG_SCHEMA_VERSION,
            &MembershipChange {
                reason: MembershipReason::KnockoutInactivity,
                removed: vec![third],
                added: Vec::new(),
                capacity: 3,
                ranking_batch_id: Some(8),
                evidence: SealedMembershipEvidence::knockout_backfill(
                    vec![SealedKnockoutEvidence {
                        wallet: third,
                        reason: MembershipReason::KnockoutInactivity,
                        causal_receipt: knockout_receipt,
                    }],
                    None,
                    Vec::new(),
                )
                .unwrap(),
            }
            .into_record(),
        );

        let capacity_receipt = append_membership_artifact(
            &mut source_writer,
            CAPACITY_CONFIG_SOURCE_ID,
            &CapacityMembershipArtifact {
                generation: 2,
                target: 1,
                published_entries: vec![watchlist_entry(first, 900)],
            },
        );
        append(
            &mut paper_writer,
            PAPER_LOG_SCHEMA_VERSION,
            &MembershipChange {
                reason: MembershipReason::CapacityChange,
                removed: vec![second],
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: None,
                evidence: SealedMembershipEvidence::capacity_change(
                    2,
                    capacity_receipt,
                    Vec::new(),
                )
                .unwrap(),
            }
            .into_record(),
        );
        drop(paper_writer);
        // Filler after the artifacts makes a hidden whole-log pass measurable (#572): exact
        // indexed reads touch only the artifact frames.
        for index in 0..20_000_i64 {
            let at = OffsetDateTime::from_unix_timestamp(1_700_000_000 + index).unwrap();
            source_writer
                .append(pe_event_log::EnvelopeIn {
                    source_id: SourceId("membership-filler".to_owned()),
                    schema_version: 1,
                    parser_version: 1,
                    observed_at: SourceTimestamp(at),
                    received_at: ReceivedAt(at),
                    content_type: pe_event_log::ContentType::Json,
                    payload: format!(r#"{{"filler":{index},"pad":"{:0>96}"}}"#, index).into_bytes(),
                })
                .unwrap();
        }
        source_writer.sync().unwrap();
        drop(source_writer);
        let source_length = std::fs::metadata(&source_path).unwrap().len();
        assert!(source_length > 1_000_000, "{source_length}");

        let era = paper_era(scan_paper_log(&paper_path).unwrap());
        let initial_entries = vec![
            watchlist_entry(first, 600),
            watchlist_entry(second, 500),
            watchlist_entry(third, 400),
            watchlist_entry(fourth, 300),
        ];
        let replayed = replay_membership(&era, watchlist(initial_entries.clone()), &source_path)
            .unwrap()
            .unwrap();
        let indexed_source = crate::qualification::PublishedMembershipSource::from_index(
            crate::risk_inputs::SourceReceiptIndex::replay(&source_path).unwrap(),
        );
        #[cfg(target_os = "linux")]
        let before = read_chars();
        let indexed =
            replay_membership_with_source(&era, watchlist(initial_entries), &indexed_source)
                .unwrap()
                .unwrap();
        #[cfg(target_os = "linux")]
        {
            let read = read_chars() - before;
            assert!(
                read < source_length / 4,
                "index-backed membership replay must read only its artifact frames: read {read} of {source_length} bytes"
            );
        }
        assert_eq!(
            indexed.last_ranking_batch_id,
            replayed.last_ranking_batch_id
        );
        assert_eq!(
            serde_json::to_vec(&indexed.watchlist.entries).unwrap(),
            serde_json::to_vec(&replayed.watchlist.entries).unwrap()
        );
        assert_eq!(replayed.last_ranking_batch_id, 8);
        assert_eq!(replayed.watchlist.entries.len(), 1);
        assert_eq!(replayed.watchlist.entries[0].wallet, first);
    }

    fn spawn_membership_orchestrator(
        live: LiveWatchlist,
        paper_writer: Writer,
        paper_state: Arc<PaperStateDb>,
        control_rx: mpsc::Receiver<crate::orchestrator_control::OrchestratorControl>,
    ) -> tokio::task::JoinHandle<()> {
        let orchestrator = Orchestrator::new(
            live.clone(),
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
            paper_writer,
            paper_state.clone(),
            PositionLedger::new(),
            new_shared_health(false),
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), String::new()),
            control_rx,
            None,
            None,
            None,
            Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
        )
        .unwrap();
        tokio::spawn(orchestrator.run(std::future::pending::<()>()))
    }

    /// PASS: a structural change published by the runtime orchestrator and replayed from that
    /// same paper log produces byte-for-byte-equivalent watchlist entries;
    /// FAIL: runtime and boot differ in survivor fields, ordering, deduplication, or capping.
    #[tokio::test]
    async fn structural_membership_runtime_and_boot_replay_exact_entries() {
        let dir = tempdir().unwrap();
        let paper_path = dir.path().join("membership-runtime.log");
        let source_path = dir.path().join("source.log");
        let first = wallet();
        let second = WalletAddress([2; 20]);
        let third = WalletAddress([3; 20]);
        let removed_wallet = WalletAddress([4; 20]);
        let initial_entries = vec![
            watchlist_entry(first, 600),
            watchlist_entry(second, 500),
            watchlist_entry(third, 400),
            watchlist_entry(removed_wallet, 300),
        ];
        let live = LiveWatchlist::new(watchlist(initial_entries.clone()));

        let mut started = start("activation");
        started.membership = vec![first, second, third, removed_wallet];
        let mut paper_writer = Writer::open(&paper_path).unwrap();
        append(
            &mut paper_writer,
            PAPER_LOG_SCHEMA_VERSION,
            &PaperLogRecord::QualificationStarted(Box::new(started)),
        );

        let replacements = vec![
            watchlist_entry(first, 900),
            watchlist_entry(second, 800),
            watchlist_entry(third, 700),
        ];
        let mut source_writer = Writer::open(&source_path).unwrap();
        let ranking_receipt = append_membership_artifact(
            &mut source_writer,
            RANKING_MEMBERSHIP_SOURCE_ID,
            &RankingMembershipArtifact {
                batch_id: Some(8),
                entries: replacements.clone(),
            },
        );
        drop(source_writer);
        let change = MembershipChange {
            reason: MembershipReason::FullRerank,
            removed: vec![removed_wallet],
            added: Vec::new(),
            capacity: 3,
            ranking_batch_id: Some(8),
            evidence: SealedMembershipEvidence::full_rerank(ranking_receipt, Vec::new()).unwrap(),
        };

        let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let (control_tx, control_rx) = mpsc::channel(1);
        let runtime = spawn_membership_orchestrator(
            live.clone(),
            paper_writer,
            paper_state.clone(),
            control_rx,
        );
        let preparer = AdmissionPreparer::new(control_tx, paper_state);
        preparer
            .publish_membership(change, replacements)
            .await
            .unwrap();
        drop(preparer);
        runtime.await.unwrap();

        let runtime_entries = live.snapshot().entries.clone();
        let era = paper_era(scan_paper_log(&paper_path).unwrap());
        let replayed = replay_membership(&era, watchlist(initial_entries), &source_path)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&replayed.watchlist.entries).unwrap(),
            serde_json::to_vec(&runtime_entries).unwrap()
        );
    }

    /// PASS: a retained overfetched legacy artifact and a newly selected, proof-bearing knockout
    /// publication replay their distinct exact memberships; batch, vector, and receipt edits fail.
    #[tokio::test]
    async fn knockout_legacy_and_repaired_publications_replay_exactly() {
        use crate::activity_ingest::{ActivityIngest, SourceLogHandle};
        use crate::runtime_config::AppliedWatchlistCapacity;
        use crate::source_event_sink::SourceEventSink;
        use crate::watchlist_maintenance::{MembershipPublication, apply_evictions_and_backfill};
        for legacy in [true, false] {
            let dir = tempdir().unwrap();
            let paper_path = dir.path().join("paper.log");
            let source_path = dir.path().join("source.log");
            let initial = wallet();
            let excluded = WalletAddress([2; 20]);
            let survivor = WalletAddress([3; 20]);
            let selected = if legacy { excluded } else { survivor };
            let artifact: RankingMembershipArtifact = serde_json::from_slice(include_bytes!(
                "../tests/fixtures/legacy_knockout_candidates.json"
            ))
            .unwrap();
            assert_eq!(artifact.entries.len(), 2);
            let candidates = if legacy {
                artifact.entries.clone()
            } else {
                crate::supabase_reader::select_membership(
                    watchlist(artifact.entries.clone()),
                    HashMap::new(),
                    &HashSet::from([excluded]),
                    1,
                )
                .0
                .entries
            };
            assert_eq!(candidates.len(), if legacy { 2 } else { 1 });
            assert_eq!(candidates[0].wallet, selected);
            let initial_entries = vec![watchlist_entry(initial, 500)];
            let live = LiveWatchlist::new(watchlist(initial_entries.clone()));
            let mut paper_writer = Writer::open(&paper_path).unwrap();
            append(
                &mut paper_writer,
                PAPER_LOG_SCHEMA_VERSION,
                &PaperLogRecord::QualificationStarted(Box::new(start("knockout"))),
            );
            let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            state
                .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                    wallet: selected,
                    complete: true,
                    proof_json: "{\"complete\":true}".to_owned(),
                    updated_at_unix: 10,
                })
                .unwrap();
            state.set_cursor(&selected, 10).unwrap();
            state
                .install_anchors(&[pe_paper_state::AnchorInstallRecord {
                    history_status: None,
                    wallet: selected,
                    balances: Vec::new(),
                    activity_cutoff_unix: 10,
                    anchored_at_unix: 10,
                    ledger_hash_after: "empty".to_owned(),
                    positions_proof_hash: "empty".to_owned(),
                    activity_bounds_json: "[]".to_owned(),
                    source_log_generation: "knockout".to_owned(),
                    proof_json: "{}".to_owned(),
                    recorded_at_unix: 10,
                }])
                .unwrap();
            if !legacy {
                rusqlite::Connection::open(dir.path().join("paper.db")).unwrap().execute(
                    "INSERT INTO wallet_fences VALUES (?1, 'excluded', 'invalid_mapping', '{}', 1)", [excluded.to_string()]
                ).unwrap();
            }
            let (source, rx) = SourceLogHandle::channel(4);
            let (trigger, _triggers) = mpsc::channel(1);
            let ingest = tokio::spawn(
                ActivityIngest::poll_only(
                    SourceEventSink::open(&source_path).unwrap(),
                    rx,
                    trigger,
                    new_shared_health(false),
                )
                .run(),
            );
            let (tx, rx) = mpsc::channel(1);
            let runtime =
                spawn_membership_orchestrator(live.clone(), paper_writer, state.clone(), rx);
            let preparer =
                AdmissionPreparer::new(tx, state.clone()).with_source_log(source.clone());
            let ranking = if legacy {
                let at = OffsetDateTime::from_unix_timestamp(10).unwrap();
                source
                    .append(EnvelopeIn {
                        source_id: SourceId(RANKING_MEMBERSHIP_SOURCE_ID.to_owned()),
                        schema_version: MEMBERSHIP_ARTIFACT_SCHEMA_VERSION,
                        parser_version: MEMBERSHIP_ARTIFACT_PARSER_VERSION,
                        observed_at: SourceTimestamp(at),
                        received_at: ReceivedAt(at),
                        content_type: ContentType::Json,
                        payload: include_bytes!(
                            "../tests/fixtures/legacy_knockout_candidates.json"
                        )
                        .to_vec(),
                    })
                    .await
                    .unwrap()
            } else {
                preparer
                    .record_ranking_membership(Some(8), candidates.clone())
                    .await
                    .unwrap()
            };
            let admissions = preparer.record_admission_proofs(&[selected]).await.unwrap();
            assert_eq!(admissions.len(), 1);
            let evictions = preparer
                .record_knockout_inputs(vec![(
                    MembershipReason::KnockoutInactivity,
                    KnockoutCausalArtifact {
                        wallet: initial,
                        evaluated_at_unix: 259_200,
                        last_trade_unix: Some(0),
                        inactivity_threshold_secs: 259_200,
                        inactivity_hard_cap_secs: 604_800,
                        demotion_min_trades: 10,
                        demotion_cb_alpha: dec!(0.10),
                        demotion_pnl_window_secs: 2_592_000,
                        fills: Vec::new(),
                        settlements: Vec::new(),
                    },
                )])
                .await
                .unwrap();
            let evidence = SealedMembershipEvidence::knockout_backfill(
                evictions,
                Some(ranking),
                admissions.clone(),
            )
            .unwrap();
            if legacy {
                // Historical artifact retains the full overfetch even though only one slot opens.
                preparer
                    .publish_membership(
                        MembershipChange {
                            reason: MembershipReason::KnockoutInactivity,
                            removed: vec![initial],
                            added: vec![selected],
                            capacity: 1,
                            ranking_batch_id: Some(8),
                            evidence,
                        },
                        candidates.clone(),
                    )
                    .await
                    .unwrap();
            } else {
                let capacity = AppliedWatchlistCapacity::new(1);
                apply_evictions_and_backfill(
                    &live,
                    &state,
                    &tokio::sync::Mutex::new(()),
                    &preparer,
                    MembershipPublication {
                        reason: MembershipReason::KnockoutInactivity,
                        ranking_batch_id: Some(8),
                        evidence,
                    },
                    &capacity,
                    capacity.load(),
                    &HashSet::from([initial]),
                    &candidates,
                    &HashMap::from([(selected, 10)]),
                )
                .await
                .unwrap();
            }
            let era = paper_era(scan_paper_log(&paper_path).unwrap());
            let replayed =
                replay_membership(&era, watchlist(initial_entries.clone()), &source_path)
                    .unwrap()
                    .unwrap();
            assert_eq!(replayed.last_ranking_batch_id, 8);
            assert_eq!(replayed.watchlist.entries[0].wallet, selected);
            assert_eq!(
                serde_json::to_vec(&replayed.watchlist.entries).unwrap(),
                serde_json::to_vec(&live.snapshot().entries).unwrap()
            );
            let indexed = crate::qualification::PublishedMembershipSource::from_index(
                crate::risk_inputs::SourceReceiptIndex::replay(&source_path).unwrap(),
            );
            assert_eq!(
                serde_json::to_vec(
                    &replay_membership_with_source(
                        &era,
                        watchlist(initial_entries.clone()),
                        &indexed
                    )
                    .unwrap()
                    .unwrap()
                    .watchlist
                    .entries
                )
                .unwrap(),
                serde_json::to_vec(&replayed.watchlist.entries).unwrap()
            );
            for (change, expected) in [
                ("batch", "different batch"),
                ("candidates", "disagrees with its additions"),
                ("admission", "wrong envelope identity"),
            ] {
                let replacement = match change {
                    "batch" => preparer
                        .record_ranking_membership(Some(9), candidates.clone())
                        .await
                        .unwrap(),
                    "candidates" => preparer
                        .record_ranking_membership(
                            Some(8),
                            vec![watchlist_entry(WalletAddress([4; 20]), 1000)],
                        )
                        .await
                        .unwrap(),
                    _ => ranking,
                };
                let mut changed = paper_era(scan_paper_log(&paper_path).unwrap());
                let frame = changed
                    .frames
                    .iter_mut()
                    .find(|frame| {
                        matches!(
                            frame.frame,
                            PaperLogFrame::Record(PaperLogRecord::MembershipChanged { .. })
                        )
                    })
                    .unwrap();
                let PaperLogFrame::Record(PaperLogRecord::MembershipChanged { evidence, .. }) =
                    &mut frame.frame
                else {
                    unreachable!()
                };
                let mut typed: SealedMembershipEvidence =
                    serde_json::from_value(evidence.clone()).unwrap();
                let SealedMembershipEvidence::KnockoutBackfill {
                    ranking_receipt,
                    admission_receipts,
                    ..
                } = &mut typed
                else {
                    unreachable!()
                };
                if change == "admission" {
                    admission_receipts[0].receipt = replacement;
                } else {
                    *ranking_receipt = Some(replacement);
                }
                *evidence = serde_json::to_value(typed).unwrap();
                let error =
                    replay_membership(&changed, watchlist(initial_entries.clone()), &source_path)
                        .unwrap_err()
                        .to_string();
                assert!(
                    error.contains(expected),
                    "legacy={legacy} {change}: {error}"
                );
            }
            drop(preparer);
            runtime.await.unwrap();
            drop(source);
            ingest.await.unwrap();
        }
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
        let replayed = replay_membership(
            &era,
            watchlist(vec![watchlist_entry(initial, 500)]),
            &dir.path().join("source.log"),
        )
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
            replay_membership(
                &era,
                watchlist(vec![watchlist_entry(other, 500)]),
                &dir.path().join("source.log"),
            ),
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
            | crate::bucket_commit::HISTORY_ONLY_BRACKET
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
            proof_json: String::new(),
        };
        assert!(matches!(
            verify_replayed_group_revision(Some(&durable), &group),
            Err(WalletLedgerReplayError::RevisionMismatch {
                source_trade_id: actual
            }) if actual == source_trade_id
        ));
    }
}
