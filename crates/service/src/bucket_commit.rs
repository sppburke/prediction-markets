//! Deterministic wallet epoch-second activity commits (#544).
//!
//! A complete reconciled second is classified from one immutable pre-state and
//! reaches paper-state in one transaction. Lexical `g2:` order is used only to
//! make storage/replay output canonical; it never selects a causal winner.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, PositionState, SignalConfig, TradeProvenance};
use pe_core_types::{
    LeaderAction, MarketId, MarketOutcomeId, OutcomeId, Price, Probability, ProbabilityPpm,
    ReconstructionQuality, ShareAmount, Side, SourceTradeId, WalletAddress,
};
use pe_event_log::AppendReceipt;
use pe_execution_core::ObservationEvidence;
use pe_paper_state::{
    ActivityBucketCommit, ActivityDispositionRecord, ActivityGroupState, AnchorInstallRecord,
    DecisionPendingRecord, DecisionPendingRow, EntryGateResultRecord, LeaderPositionRow,
    MarketHistoryRecord, NoCopyDisposition, PaperStateDb, ReanchorRecord, WalletFenceRecord,
    WalletHistoryStatusRecord,
};
use pe_position_ledger::{
    AppliedEffect, LedgerEffect, LedgerEffectDocumentError, LedgerError, LedgerMutation,
    PositionLedger, SecondVerdict, TradeDecision, WalletFenceCause, classify_complete_second,
};
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAggregate,
    parse_activity_trade_observation,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::entry_gate::{CopyEntryGate, CopyEntryGateConfig};
use crate::position_seeder::{AnchorInstall, ledger_capture};
use crate::risk_inputs::SourceReceiptIndex;
use crate::runtime_config::RuntimeConfig;

/// Decision inputs already read before the atomic bucket commit.
#[derive(Debug, Clone)]
pub struct BucketDecisionContext {
    pub applied_configuration: RuntimeConfig,
    pub decision_inputs_json: String,
    /// Receipt-bearing page occurrences are frozen only in continuation V3. They stay out of
    /// `decision_inputs`, which owns the logical read proof rather than source-log identities.
    pub page_occurrences: Vec<PageOccurrence>,
    /// Lowest websocket receipt per admitted group; frozen only in continuation V3.
    pub observed_source_receipts: HashMap<SourceTradeId, AppendReceipt>,
    pub reconstruction_quality: ReconstructionQuality,
    pub signal_config: SignalConfig,
    pub copy_eligible: bool,
    /// Bracket catch-up installs an anchor immediately after this read.
    pub bracket_commit: bool,
    pub recorded_at_unix: i64,
    /// Transport retained from the first durable observation of each group.
    pub observation_provenance: HashMap<SourceTradeId, TradeProvenance>,
    /// Typed early-gate dispositions supplied by reconciliation (#544).
    pub no_copy_dispositions: HashMap<SourceTradeId, NoCopyDisposition>,
    /// Venue-metadata corrections keyed by the immutable raw activity group.
    pub identity_overrides: HashMap<SourceTradeId, IdentityOverride>,
    /// Groups whose token identity could not be established by venue metadata.
    pub identity_unresolved: HashSet<SourceTradeId>,
    /// Lane E supplies this only after a complete fixed-end history walk.
    pub history_status: Option<WalletHistoryStatusRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityOverride {
    pub verified: MarketOutcomeId,
    pub evidence_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketCommitResult {
    pub wallet: WalletAddress,
    pub source_epoch: i64,
    pub dispositions: BTreeMap<String, String>,
    pub pending: Vec<SourceTradeId>,
    pub newly_fenced: Option<WalletFenceCause>,
    pub already_committed: bool,
}

/// Mutable decision inputs the orchestrator freezes atomically with the bucket
/// transaction (#544 review round 3): the leader's win-rate probability and the
/// pre-sizing bankroll. A resumed continuation evaluates under these, never a
/// refreshed live watchlist or bankroll, so the same durable checkpoint always
/// reproduces the same decision.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FrozenDecisionBasis {
    pub win_rate_p: Probability,
    pub bankroll: Decimal,
}

/// Receipt-free continuation facts frozen by the bucket transaction.
/// Inputs read after this boundary are appended to paper/source logs and the
/// terminal `decision_pending` transition; offline replay never executes it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionContinuationFacts {
    pub source_trade_id: SourceTradeId,
    pub semantic_revision: String,
    pub transaction_hash: String,
    pub wallet: WalletAddress,
    pub source_epoch: i64,
    pub market_id: MarketId,
    pub outcome_id: OutcomeId,
    pub side: Side,
    pub price: Price,
    pub share_amount: ShareAmount,
    pub provenance: TradeProvenance,
    pub pre_bucket_action: LeaderAction,
    pub reconstruction_quality: ReconstructionQuality,
    pub action_confidence_ppm: ProbabilityPpm,
    pub gate_result: String,
    pub applied_configuration_hash: String,
    pub applied_configuration: RuntimeConfig,
    pub frozen_basis: FrozenDecisionBasis,
    pub decision_inputs: Value,
}

/// Private compatibility decoder for durable version-two continuation rows.
#[derive(Debug, Deserialize)]
struct DecisionContinuationV2Wire {
    version: u16,
    #[serde(flatten)]
    facts: DecisionContinuationFacts,
}

/// One occurrence of a fixed-end activity page and the receipt assigned by the source log.
/// Repeated request URLs and payload hashes remain distinct entries (#545).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PageOccurrence {
    pub request_url: String,
    pub raw_hash: String,
    pub receipt: AppendReceipt,
}

/// Durable receipt-bearing successor and runtime owner of a frozen continuation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionContinuationV3 {
    version: u16,
    #[serde(flatten)]
    pub facts: DecisionContinuationFacts,
    pub observed_source_receipt: Option<AppendReceipt>,
    pub page_occurrences: Vec<PageOccurrence>,
}

impl DecisionContinuationV3 {
    pub(crate) fn new(
        facts: DecisionContinuationFacts,
        observed_source_receipt: Option<AppendReceipt>,
        page_occurrences: Vec<PageOccurrence>,
    ) -> Self {
        Self {
            version: 3,
            facts,
            observed_source_receipt,
            page_occurrences,
        }
    }

    /// Greatest synchronized activity-page receipt in this complete read.
    #[must_use]
    pub fn complete_bound(&self) -> Option<AppendReceipt> {
        self.page_occurrences
            .iter()
            .map(|page| page.receipt)
            .max_by_key(|receipt| receipt.sequence)
    }

    /// Earliest complete observation and its envelope receive time. Legacy V2 rows have no
    /// receipt evidence and return `None`.
    #[must_use]
    fn observation_at(&self, observed_unix_ms: i64) -> Option<ObservationEvidence> {
        let complete_bound_receipt = self.complete_bound()?;
        let (source_receipt, provenance) = match self.observed_source_receipt {
            Some(websocket) if websocket.sequence < complete_bound_receipt.sequence => {
                (websocket, "activity_ws")
            }
            Some(websocket) if websocket.sequence == complete_bound_receipt.sequence => {
                if websocket.this_hash != complete_bound_receipt.this_hash {
                    return None;
                }
                (websocket, "activity_ws")
            }
            _ => (complete_bound_receipt, "rest_poll"),
        };
        Some(ObservationEvidence {
            source_receipt,
            complete_bound_receipt,
            observed_unix_ms,
            provenance: provenance.to_owned(),
        })
    }

    #[must_use]
    pub fn observation_receipt(&self) -> Option<AppendReceipt> {
        let complete_bound = self.complete_bound()?;
        match self.observed_source_receipt {
            Some(websocket) if websocket.sequence < complete_bound.sequence => Some(websocket),
            Some(websocket) if websocket.sequence == complete_bound.sequence => {
                (websocket.this_hash == complete_bound.this_hash).then_some(websocket)
            }
            _ => Some(complete_bound),
        }
    }

    #[must_use]
    pub fn page_occurrences(&self) -> &[PageOccurrence] {
        &self.page_occurrences
    }

    /// Resolve every V3 receipt through the boot-owned verified source-receipt index and derive
    /// observation time from the selected receipt. No timestamp copied into continuation JSON is
    /// trusted, and the growing source log is never replayed on this hot path (#545).
    pub fn observation_from_receipt_index(
        &self,
        source_receipts: &SourceReceiptIndex,
    ) -> Result<Option<ObservationEvidence>, DecisionContinuationError> {
        let Some(selected) = self.observation_receipt() else {
            return Ok(None);
        };
        for page in &self.page_occurrences {
            let envelope = source_receipts.source_envelope(page.receipt)?;
            if envelope.source_id.0 != crate::trade_poller::ACTIVITY_POLL_SOURCE_ID
                || envelope.schema_version != ACTIVITY_SCHEMA_VERSION
                || envelope.parser_version != ACTIVITY_PARSER_VERSION
                || envelope.raw_payload_hash.to_hex().as_str() != page.raw_hash
            {
                return Err(DecisionContinuationError::SourceReceiptMismatch {
                    sequence: page.receipt.sequence.0,
                });
            }
        }
        if let Some(websocket) = self.observed_source_receipt {
            let envelope = source_receipts.source_envelope(websocket)?;
            let activity = parse_activity_trade_observation(&envelope.payload).map_err(|_| {
                DecisionContinuationError::SourceReceiptMismatch {
                    sequence: websocket.sequence.0,
                }
            })?;
            if envelope.source_id.0 != crate::activity_ingest::ACTIVITY_WS_SOURCE_ID
                || envelope.schema_version != ACTIVITY_SCHEMA_VERSION
                || envelope.parser_version != ACTIVITY_PARSER_VERSION
                || activity.wallet != self.facts.wallet
                || activity.group_id.key() != &self.facts.source_trade_id
            {
                return Err(DecisionContinuationError::SourceReceiptMismatch {
                    sequence: websocket.sequence.0,
                });
            }
        }
        let observed_unix_ms = source_receipts.received_millis(selected)?;
        self.observation_at(observed_unix_ms)
            .ok_or(DecisionContinuationError::SourceReceiptMismatch {
                sequence: selected.sequence.0,
            })
            .map(Some)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DecisionContinuationError {
    #[error("invalid frozen continuation json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported frozen continuation version {0}")]
    Version(u16),
    #[error("frozen continuation does not match durable row")]
    DurableMismatch,
    #[error("frozen continuation has invalid source epoch {0}")]
    SourceEpoch(i64),
    #[error("source receipt lookup failed: {0}")]
    SourceReceiptLookup(#[from] crate::risk_inputs::RiskInputsUnavailable),
    #[error("source receipt sequence {sequence} does not match its frozen evidence")]
    SourceReceiptMismatch { sequence: u64 },
}

impl DecisionContinuationV3 {
    /// Decode and bind a frozen continuation to its durable outer row.
    pub fn from_durable(row: &DecisionPendingRow) -> Result<Self, DecisionContinuationError> {
        let value: Value = serde_json::from_str(&row.frozen_inputs_json)?;
        let version = value
            .get("version")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .ok_or(DecisionContinuationError::Version(0))?;
        let continuation = match version {
            2 => {
                let legacy: DecisionContinuationV2Wire = serde_json::from_value(value)?;
                Self {
                    version: legacy.version,
                    facts: legacy.facts,
                    observed_source_receipt: None,
                    page_occurrences: Vec::new(),
                }
            }
            3 => serde_json::from_value(value)?,
            version => return Err(DecisionContinuationError::Version(version)),
        };
        let frozen = &continuation.facts;
        if frozen.source_trade_id != row.source_trade_id
            || frozen.semantic_revision != row.semantic_revision
            || frozen.wallet != row.wallet
            || frozen.source_epoch != row.source_epoch
            || frozen.gate_result != "admitted"
            || frozen.pre_bucket_action != LeaderAction::Entry
            || frozen.applied_configuration.canonical_hash() != frozen.applied_configuration_hash
        {
            return Err(DecisionContinuationError::DurableMismatch);
        }
        if version == 3 {
            let page_occurrences = continuation.page_occurrences();
            if page_occurrences.is_empty() {
                return Err(DecisionContinuationError::DurableMismatch);
            }
            let mut previous = None;
            for page in page_occurrences {
                if previous.is_some_and(|sequence| page.receipt.sequence <= sequence) {
                    return Err(DecisionContinuationError::DurableMismatch);
                }
                previous = Some(page.receipt.sequence);
            }
        }
        Ok(continuation)
    }

    /// Reconstruct only the transport-neutral trade facts needed by the existing
    /// idempotent decision continuation. Ledger/classification/gate are not rerun.
    pub fn incoming_trade(&self) -> Result<IncomingTrade, DecisionContinuationError> {
        let observed_at = time::OffsetDateTime::from_unix_timestamp(self.facts.source_epoch)
            .map_err(|_| DecisionContinuationError::SourceEpoch(self.facts.source_epoch))?;
        Ok(IncomingTrade {
            wallet: self.facts.wallet,
            market_id: self.facts.market_id.clone(),
            outcome_id: self.facts.outcome_id,
            side: self.facts.side,
            price: self.facts.price,
            contracts: self.facts.share_amount,
            observed_at,
            received_at: observed_at,
            source_trade_id: self.facts.source_trade_id.clone(),
            transaction_hash: Some(self.facts.transaction_hash.clone()),
            provenance: self.facts.provenance,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BucketCommitError {
    #[error("cannot commit an empty activity bucket")]
    Empty,
    #[error("activity bucket mixes wallets or epoch seconds")]
    MixedBucket,
    #[error("activity bucket is partially durable")]
    PartialDurableBucket,
    #[error("invalid decision input json: {0}")]
    DecisionInputs(#[from] serde_json::Error),
    #[error("activity ledger: {0}")]
    Ledger(#[from] LedgerError),
    #[error("paper-state: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("ledger effect document: {0}")]
    EffectDocument(#[from] LedgerEffectDocumentError),
    #[error("{0}")]
    Invariant(String),
}

#[derive(Debug, thiserror::Error)]
pub enum AnchorInstallError {
    #[error("wallet {wallet} is durably fenced")]
    Fenced { wallet: WalletAddress },
    #[error("wallet {wallet} ledger changed before anchor install")]
    LedgerHashChanged { wallet: WalletAddress },
    #[error("wallet {wallet} cursor changed before anchor install")]
    CursorChanged { wallet: WalletAddress },
    #[error("wallet {wallet} anchor sequence changed before anchor install")]
    AnchorSeqChanged { wallet: WalletAddress },
    #[error("wallet {wallet} coverage generation changed before anchor install")]
    CoverageGenerationChanged { wallet: WalletAddress },
    #[error("wallet {wallet} anchor cutoff regressed from {stored} to {candidate}")]
    CutoffRegression {
        wallet: WalletAddress,
        stored: i64,
        candidate: i64,
    },
    #[error("anchor install durability failure: {0}")]
    Durability(String),
}

impl From<pe_paper_state::PaperStateError> for AnchorInstallError {
    fn from(error: pe_paper_state::PaperStateError) -> Self {
        Self::Durability(format!("paper-state: {error}"))
    }
}

/// Single runtime owner for the exact leader ledger, durable gate projection,
/// wallet fences, and decision admission.
pub struct BucketCommitEngine {
    paper_state: Arc<PaperStateDb>,
    ledger: PositionLedger,
    entry_gate: CopyEntryGate,
    complete_history: HashSet<WalletAddress>,
    fences: HashSet<WalletAddress>,
}

impl BucketCommitEngine {
    /// Load every durable decision boundary before producers start.
    pub fn load(
        paper_state: Arc<PaperStateDb>,
        ledger: PositionLedger,
    ) -> Result<Self, BucketCommitError> {
        let entry_gate = CopyEntryGate::new(CopyEntryGateConfig, paper_state.gate_history()?);
        let complete_history = paper_state.complete_history_wallets()?;
        let fences = paper_state
            .wallet_fences()?
            .into_iter()
            .map(|fence| fence.wallet)
            .collect();
        Ok(Self {
            paper_state,
            ledger,
            entry_gate,
            complete_history,
            fences,
        })
    }

    #[must_use]
    pub fn ledger(&self) -> &PositionLedger {
        &self.ledger
    }

    /// Move the validated boot ledger into the runtime orchestrator owner.
    #[must_use]
    pub fn into_ledger(self) -> PositionLedger {
        self.ledger
    }

    pub(crate) fn ledger_mut(&mut self) -> &mut PositionLedger {
        &mut self.ledger
    }

    pub(crate) fn entry_gate(&self) -> &CopyEntryGate {
        &self.entry_gate
    }

    pub(crate) fn entry_gate_mut(&mut self) -> &mut CopyEntryGate {
        &mut self.entry_gate
    }

    #[must_use]
    pub fn is_fenced(&self, wallet: &WalletAddress) -> bool {
        self.fences.contains(wallet)
    }

    #[must_use]
    pub fn history_complete(&self, wallet: &WalletAddress) -> bool {
        self.complete_history.contains(wallet)
    }

    /// Commit several activity buckets as one durable paper-state batch.
    ///
    /// Connection ownership makes this safe: the only production caller is the boot bracket's
    /// `commit_direct`, under the engine lock before producers start; `begin_batch` therefore has
    /// that single production caller and batches never nest. The sole boot-path paper-state write
    /// outside that lock, `mark_seeded_history_validated`, runs after every bracket completes. A
    /// failed `ROLLBACK` surfaces as the bracket error, and the next `BEGIN IMMEDIATE` then fails,
    /// so boot fails closed instead of committing partial state.
    pub fn commit_batch<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, BucketCommitError>,
    ) -> Result<T, BucketCommitError> {
        let ledger = self.ledger.clone();
        let entry_gate = self.entry_gate.clone();
        let complete_history = self.complete_history.clone();
        let fences = self.fences.clone();
        self.paper_state.begin_batch()?;
        let result = f(self).and_then(|value| {
            self.paper_state.commit_batch()?;
            Ok(value)
        });
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                let rollback = self.paper_state.rollback_batch();
                self.ledger = ledger;
                self.entry_gate = entry_gate;
                self.complete_history = complete_history;
                self.fences = fences;
                match rollback {
                    Ok(()) => Err(error),
                    Err(rollback_error) => Err(rollback_error.into()),
                }
            }
        }
    }

    /// Compare-and-swap one complete anchor batch, then publish the prebuilt
    /// in-memory ledger only after the durable transaction commits.
    pub fn install_anchors(
        &mut self,
        installs: &[AnchorInstall],
    ) -> Result<(), AnchorInstallError> {
        let mut wallets = HashSet::new();
        for install in installs {
            if !wallets.insert(install.wallet) {
                return Err(AnchorInstallError::Durability(format!(
                    "anchor ledger proof for {}: duplicate wallet in anchor batch",
                    install.wallet
                )));
            }
            if self.is_fenced(&install.wallet) {
                return Err(AnchorInstallError::Fenced {
                    wallet: install.wallet,
                });
            }
            let capture = ledger_capture(&self.ledger, &self.paper_state, install.wallet).map_err(
                |error| {
                    AnchorInstallError::Durability(format!(
                        "anchor ledger proof for {}: {error}",
                        install.wallet
                    ))
                },
            )?;
            if capture.hash != install.expected.ledger_hash {
                return Err(AnchorInstallError::LedgerHashChanged {
                    wallet: install.wallet,
                });
            }
            if capture.cursor != install.expected.cursor {
                return Err(AnchorInstallError::CursorChanged {
                    wallet: install.wallet,
                });
            }
            if capture.anchor_seq != install.expected.anchor_seq {
                return Err(AnchorInstallError::AnchorSeqChanged {
                    wallet: install.wallet,
                });
            }
            if capture.coverage_generation != install.expected.coverage_generation {
                return Err(AnchorInstallError::CoverageGenerationChanged {
                    wallet: install.wallet,
                });
            }
            let coverage = self.paper_state.wallet_coverage(&install.wallet)?;
            if let Some(stored) = coverage.activity_cutoff_unix
                && stored > install.cutoff
            {
                return Err(AnchorInstallError::CutoffRegression {
                    wallet: install.wallet,
                    stored,
                    candidate: install.cutoff,
                });
            }
        }

        let mut candidate = self.ledger.clone();
        let mut records = Vec::with_capacity(installs.len());
        for install in installs {
            let mut positions = HashMap::new();
            for (market_id, outcome_id, amount) in &install.balances {
                let key = MarketOutcomeId::new(market_id.clone(), *outcome_id);
                if positions
                    .insert(
                        key,
                        PositionState {
                            long_contracts: *amount,
                            short_contracts: ShareAmount::ZERO,
                        },
                    )
                    .is_some()
                {
                    return Err(AnchorInstallError::Durability(format!(
                        "anchor ledger proof for {}: duplicate anchored balance for {market_id} outcome {}",
                        install.wallet, outcome_id.0
                    )));
                }
            }
            candidate.replace_wallet_snapshot(install.wallet, positions);
            let post =
                ledger_capture(&candidate, &self.paper_state, install.wallet).map_err(|error| {
                    AnchorInstallError::Durability(format!(
                        "anchor ledger proof for {}: {error}",
                        install.wallet
                    ))
                })?;
            records.push(AnchorInstallRecord {
                wallet: install.wallet,
                balances: install.balances.clone(),
                activity_cutoff_unix: install.cutoff,
                anchored_at_unix: install.proof.recorded_at_unix,
                ledger_hash_after: post.hash,
                positions_proof_hash: install.proof.positions_proof_hash.clone(),
                activity_bounds_json: install.proof.activity_bounds_json.clone(),
                source_log_generation: install.proof.source_log_generation.clone(),
                proof_json: install.proof.document.clone(),
                recorded_at_unix: install.proof.recorded_at_unix,
            });
        }
        self.paper_state.install_anchors(&records)?;
        self.ledger = candidate;
        Ok(())
    }

    /// Commit a complete reconciled wallet-second. Input order is deliberately
    /// discarded before any classification, gate, or financial work.
    pub fn commit(
        &mut self,
        mut aggregates: Vec<ActivityAggregate>,
        context: &BucketDecisionContext,
        frozen_basis: FrozenDecisionBasis,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let first = aggregates.first().ok_or(BucketCommitError::Empty)?;
        let wallet = first.group_id.components().wallet;
        let source_epoch = first.source_time.0.unix_timestamp();
        if aggregates.iter().any(|aggregate| {
            aggregate.group_id.components().wallet != wallet
                || aggregate.source_time.0.unix_timestamp() != source_epoch
        }) {
            return Err(BucketCommitError::MixedBucket);
        }
        aggregates.sort_by(|left, right| left.group_id.key().0.cmp(&right.group_id.key().0));
        // Resolve identity before the coverage branch so covered effects and
        // first-entry history consume the same venue-authoritative mutation as
        // the ordinary apply path.
        let recordable_mutations = aggregates
            .iter()
            .map(|aggregate| recordable_mutation(aggregate, context))
            .collect::<Vec<_>>();

        let durable = aggregates
            .iter()
            .map(|aggregate| {
                self.paper_state
                    .activity_group_state(aggregate.group_id.key())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let changed: Vec<_> = aggregates
            .iter()
            .zip(&durable)
            .filter(|(aggregate, state)| {
                state.as_ref().is_some_and(|state| {
                    state.semantic_revision != aggregate.semantic_revision.as_str()
                        || state.transaction_hash
                            != aggregate.group_id.components().transaction_hash
                })
            })
            .map(|(aggregate, _)| aggregate.group_id.key().clone())
            .collect();
        if let Some(trigger) = changed
            .iter()
            .find(|trigger| !context.identity_unresolved.contains(*trigger))
            .or_else(|| changed.first())
        {
            return self.commit_changed_bucket_fence(
                &aggregates,
                &durable,
                wallet,
                source_epoch,
                (WalletFenceCause::RevisedAggregate, trigger.clone()),
                context,
            );
        }
        let coverage = self.paper_state.wallet_coverage(&wallet)?;
        let seen = durable.iter().filter(|state| state.is_some()).count();
        if seen == aggregates.len() {
            self.paper_state.set_cursor(&wallet, source_epoch)?;
            return Ok(BucketCommitResult {
                wallet,
                source_epoch,
                dispositions: BTreeMap::new(),
                pending: Vec::new(),
                newly_fenced: None,
                already_committed: true,
            });
        }
        if seen == 0
            && (coverage.reanchor_required
                || (coverage
                    .activity_cutoff_unix
                    .is_some_and(|cutoff| source_epoch > cutoff)
                    && self
                        .paper_state
                        .last_activity_group_epoch(&wallet)?
                        .is_some_and(|last_epoch| last_epoch >= source_epoch)))
        {
            let trigger = aggregates
                .iter()
                .find(|aggregate| {
                    !context
                        .identity_unresolved
                        .contains(aggregate.group_id.key())
                })
                .map(|aggregate| aggregate.group_id.key().clone())
                .or_else(|| {
                    aggregates
                        .first()
                        .map(|aggregate| aggregate.group_id.key().clone())
                })
                .ok_or(BucketCommitError::Empty)?;
            return self.commit_late_group_reanchor(
                &aggregates,
                wallet,
                source_epoch,
                trigger,
                context,
            );
        }
        if coverage
            .activity_cutoff_unix
            .is_none_or(|cutoff| source_epoch <= cutoff)
        {
            return self.commit_covered_bucket(
                &aggregates,
                &durable,
                &recordable_mutations,
                wallet,
                source_epoch,
                coverage.anchor_seq.is_some(),
                context,
            );
        }
        let unseen = aggregates
            .iter()
            .zip(&durable)
            .filter(|(_, state)| state.is_none())
            .map(|(aggregate, _)| aggregate.group_id.key())
            .collect::<Vec<_>>();
        if seen != 0 {
            let trigger = aggregates
                .iter()
                .zip(&durable)
                .find(|(_, state)| state.is_none())
                .map(|(aggregate, _)| aggregate.group_id.key().clone())
                .ok_or(BucketCommitError::PartialDurableBucket)?;
            return self.commit_changed_bucket_fence(
                &aggregates,
                &durable,
                wallet,
                source_epoch,
                (WalletFenceCause::LateEqualSecondGroup, trigger),
                context,
            );
        }
        if !unseen.is_empty()
            && unseen
                .iter()
                .all(|source_trade_id| context.identity_unresolved.contains(*source_trade_id))
        {
            return self.commit_covered_bucket(
                &aggregates,
                &durable,
                &recordable_mutations,
                wallet,
                source_epoch,
                true,
                context,
            );
        }

        let decision_inputs: Value = serde_json::from_str(&context.decision_inputs_json)?;
        let mut mutations = Vec::with_capacity(aggregates.len());
        for (aggregate, recordable) in aggregates.iter().zip(&recordable_mutations) {
            if context
                .identity_unresolved
                .contains(aggregate.group_id.key())
            {
                mutations.push(recordable.clone());
                continue;
            }
            match LedgerMutation::from_activity(aggregate) {
                Ok(mutation) => mutations.push(resolve_identity(mutation, context)),
                Err(error) => {
                    return self.commit_fence(
                        &aggregates,
                        wallet,
                        source_epoch,
                        error.fence_cause(),
                        aggregate.group_id.key().clone(),
                        context,
                    );
                }
            }
        }

        if self.fences.contains(&wallet) {
            return self.commit_fenced_bucket(
                &aggregates,
                &mutations,
                wallet,
                source_epoch,
                context,
            );
        }
        if let Some((trigger, cause)) =
            mutations
                .iter()
                .find_map(|mutation| match mutation.effect.effective() {
                    LedgerEffect::Conversion => Some((
                        mutation.source_trade_id.clone(),
                        WalletFenceCause::Conversion,
                    )),
                    LedgerEffect::UnknownEffect => Some((
                        mutation.source_trade_id.clone(),
                        WalletFenceCause::UnknownEffect,
                    )),
                    _ => None,
                })
        {
            return self.commit_fence(&aggregates, wallet, source_epoch, cause, trigger, context);
        }
        let reanchor_trigger = mutations.iter().find_map(|mutation| {
            if !context.bracket_commit
                && context
                    .identity_unresolved
                    .contains(&mutation.source_trade_id)
            {
                Some((
                    mutation.source_trade_id.clone(),
                    "identity_unresolved".to_owned(),
                ))
            } else if matches!(mutation.effect.effective(), LedgerEffect::RequiresAnchor) {
                Some((
                    mutation.source_trade_id.clone(),
                    "reanchor_required_redemption".to_owned(),
                ))
            } else {
                None
            }
        });
        let history_complete = context
            .history_status
            .as_ref()
            .filter(|status| status.wallet == wallet)
            .map_or_else(
                || self.complete_history.contains(&wallet),
                |status| status.complete,
            );
        let (applied, trade_decisions, first_entries) = match classify_complete_second(
            &self.ledger,
            wallet,
            &mutations,
            context.reconstruction_quality,
            &context.signal_config,
            history_complete,
            &|market_id| self.entry_gate.has_market(&wallet, market_id),
        ) {
            Ok(SecondVerdict::OrderIndependent {
                applied,
                decisions,
                first_entries,
            }) => (applied, decisions, first_entries),
            Ok(SecondVerdict::OrderDependent { .. }) => {
                let trigger = mutations
                    .iter()
                    .find(|mutation| {
                        !context
                            .identity_unresolved
                            .contains(&mutation.source_trade_id)
                    })
                    .map(|mutation| mutation.source_trade_id.clone())
                    .ok_or(BucketCommitError::Empty)?;
                return self.commit_fence(
                    &aggregates,
                    wallet,
                    source_epoch,
                    WalletFenceCause::OrderDependentEqualSecond,
                    trigger,
                    context,
                );
            }
            Err(error) => {
                let cause = error.fence_cause();
                let trigger = mutation_error_id(&error);
                return self.commit_fence(
                    &aggregates,
                    wallet,
                    source_epoch,
                    cause,
                    trigger,
                    context,
                );
            }
        };

        let mut candidate = self.ledger.clone();
        if let Err(error) = candidate.apply_all_or_none(&mutations) {
            return self.commit_fence(
                &aggregates,
                wallet,
                source_epoch,
                error.fence_cause(),
                mutation_error_id(&error),
                context,
            );
        }
        let (gate_results, history_effects, gate_outcomes) =
            Self::derive_gate_results(wallet, source_epoch, &trade_decisions, &first_entries);

        let mut pending = Vec::new();
        let mut dispositions = BTreeMap::new();
        let mut disposition_records = Vec::with_capacity(aggregates.len());
        for ((aggregate, mutation), applied_effect) in
            aggregates.iter().zip(&mutations).zip(&applied)
        {
            let source_trade_id = aggregate.group_id.key().clone();
            let disposition = match mutation.effect.effective() {
                LedgerEffect::RawOnly => "raw_only".to_owned(),
                LedgerEffect::RequiresAnchor => "reanchor_required_redemption".to_owned(),
                LedgerEffect::Trade { .. } => {
                    let outcome = gate_outcomes
                        .get(&source_trade_id.0)
                        .cloned()
                        .unwrap_or_else(|| "not_an_entry".to_owned());
                    if let Some(no_copy) = context.no_copy_dispositions.get(&source_trade_id) {
                        no_copy.reason.clone()
                    } else if outcome == "admitted"
                        && context.copy_eligible
                        && !coverage.reanchor_required
                        && reanchor_trigger.is_none()
                        && !trade_decisions.iter().any(|decision| {
                            decision.source_trade_id == source_trade_id
                                && decision.action_order_dependent
                        })
                    {
                        let decision = trade_decisions
                            .iter()
                            .find(|decision| decision.source_trade_id == source_trade_id)
                            .ok_or(BucketCommitError::Empty)?;
                        let LedgerEffect::Trade {
                            market_id,
                            outcome_id,
                            side,
                            amount,
                            price,
                        } = mutation.effect.effective()
                        else {
                            return Err(BucketCommitError::Empty);
                        };
                        let facts = DecisionContinuationFacts {
                            source_trade_id: source_trade_id.clone(),
                            semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
                            transaction_hash: aggregate
                                .group_id
                                .components()
                                .transaction_hash
                                .clone(),
                            wallet,
                            source_epoch,
                            market_id: market_id.clone(),
                            outcome_id: *outcome_id,
                            side: *side,
                            price: *price,
                            share_amount: *amount,
                            provenance: context
                                .observation_provenance
                                .get(&source_trade_id)
                                .copied()
                                .unwrap_or(TradeProvenance::RestPoll),
                            pre_bucket_action: decision.action,
                            reconstruction_quality: context.reconstruction_quality,
                            action_confidence_ppm: ProbabilityPpm(
                                u32::from(context.reconstruction_quality.get()) * 10_000,
                            ),
                            gate_result: "admitted".to_owned(),
                            frozen_basis,
                            applied_configuration_hash: context
                                .applied_configuration
                                .canonical_hash(),
                            applied_configuration: context.applied_configuration.clone(),
                            decision_inputs: decision_inputs.clone(),
                        };
                        if context.page_occurrences.is_empty() {
                            return Err(BucketCommitError::Invariant(
                                "admitted continuation is missing source page receipts".to_owned(),
                            ));
                        }
                        let frozen_inputs_json =
                            serde_json::to_string(&DecisionContinuationV3::new(
                                facts,
                                context
                                    .observed_source_receipts
                                    .get(&source_trade_id)
                                    .copied(),
                                context.page_occurrences.clone(),
                            ))?;
                        pending.push(DecisionPendingRecord {
                            source_trade_id: source_trade_id.clone(),
                            semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
                            wallet,
                            source_epoch,
                            frozen_inputs_json,
                            updated_at_unix: context.recorded_at_unix,
                        });
                        "decision_pending".to_owned()
                    } else if outcome == "admitted" {
                        if trade_decisions.iter().any(|decision| {
                            decision.source_trade_id == source_trade_id
                                && decision.action_order_dependent
                        }) {
                            "order_dependent_equal_second_action".to_owned()
                        } else {
                            "not_copy_eligible".to_owned()
                        }
                    } else {
                        outcome
                    }
                }
                _ => "applied".to_owned(),
            };
            dispositions.insert(source_trade_id.0.clone(), disposition.clone());
            disposition_records.push(activity_record(
                aggregate,
                disposition,
                &applied_effect.effect,
                applied_effect.clamped_residual,
                context.no_copy_dispositions.get(&source_trade_id).cloned(),
            )?);
        }

        let bucket = ActivityBucketCommit {
            wallet,
            source_epoch,
            dispositions: disposition_records,
            leader_positions: touched_leader_rows(&candidate, wallet, &mutations),
            gate_results,
            history_effects: history_effects.clone(),
            history_status: context.history_status.clone(),
            pending: pending.clone(),
            fence: None,
            reanchor: reanchor_trigger
                .clone()
                .map(|(source_trade_id, reason)| ReanchorRecord {
                    source_trade_id,
                    reason,
                }),
            advance_cursor: true,
        };
        self.paper_state.commit_activity_bucket(&bucket)?;
        self.ledger = candidate;
        self.apply_history_projection(wallet, &history_effects, context.history_status.as_ref());
        Ok(BucketCommitResult {
            wallet,
            source_epoch,
            dispositions,
            pending: pending
                .into_iter()
                .map(|record| record.source_trade_id)
                .collect(),
            newly_fenced: None,
            already_committed: false,
        })
    }

    fn commit_late_group_reanchor(
        &mut self,
        aggregates: &[ActivityAggregate],
        wallet: WalletAddress,
        source_epoch: i64,
        trigger: SourceTradeId,
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        const DISPOSITION: &str = "reanchor_required_late_group";
        let mut dispositions = BTreeMap::new();
        let records = aggregates
            .iter()
            .map(|aggregate| {
                dispositions.insert(aggregate.group_id.key().0.clone(), DISPOSITION.to_owned());
                activity_record(
                    aggregate,
                    DISPOSITION.to_owned(),
                    &LedgerEffect::RawOnly,
                    None,
                    context
                        .no_copy_dispositions
                        .get(aggregate.group_id.key())
                        .cloned(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: None,
                reanchor: Some(ReanchorRecord {
                    source_trade_id: trigger,
                    reason: DISPOSITION.to_owned(),
                }),
                advance_cursor: false,
            })?;
        Ok(BucketCommitResult {
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: None,
            already_committed: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn commit_covered_bucket(
        &mut self,
        aggregates: &[ActivityAggregate],
        durable: &[Option<ActivityGroupState>],
        resolved_mutations: &[LedgerMutation],
        wallet: WalletAddress,
        source_epoch: i64,
        late: bool,
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let disposition = if late {
            "anchor_covered_late"
        } else {
            "anchor_covered"
        };
        let mut dispositions = BTreeMap::new();
        let mut records = Vec::new();
        let mut mutations = Vec::new();
        for ((aggregate, state), mutation) in aggregates.iter().zip(durable).zip(resolved_mutations)
        {
            if state.is_some() {
                dispositions.insert(
                    aggregate.group_id.key().0.clone(),
                    "already_committed".to_owned(),
                );
                continue;
            }
            let unresolved = context
                .identity_unresolved
                .contains(aggregate.group_id.key());
            let group_disposition = if unresolved { "raw_only" } else { disposition };
            dispositions.insert(
                aggregate.group_id.key().0.clone(),
                group_disposition.to_owned(),
            );
            records.push(activity_record(
                aggregate,
                group_disposition.to_owned(),
                &mutation.effect,
                None,
                context
                    .no_copy_dispositions
                    .get(aggregate.group_id.key())
                    .cloned(),
            )?);
            mutations.push(mutation.clone());
        }
        let history_effects = self.covered_history_effects(wallet, source_epoch, &mutations);
        let unresolved_trigger = (!context.bracket_commit)
            .then(|| {
                mutations.iter().find_map(|mutation| {
                    context
                        .identity_unresolved
                        .contains(&mutation.source_trade_id)
                        .then(|| mutation.source_trade_id.clone())
                })
            })
            .flatten();
        let late_trigger = late
            .then(|| {
                mutations.iter().find_map(|mutation| {
                    (!context
                        .identity_unresolved
                        .contains(&mutation.source_trade_id))
                    .then(|| mutation.source_trade_id.clone())
                })
            })
            .flatten();
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: history_effects.clone(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: None,
                reanchor: unresolved_trigger
                    .map(|source_trade_id| ReanchorRecord {
                        source_trade_id,
                        reason: "identity_unresolved".to_owned(),
                    })
                    .or_else(|| {
                        late_trigger.map(|source_trade_id| ReanchorRecord {
                            source_trade_id,
                            reason: disposition.to_owned(),
                        })
                    }),
                advance_cursor: true,
            })?;
        self.apply_history_projection(wallet, &history_effects, context.history_status.as_ref());
        Ok(BucketCommitResult {
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: None,
            already_committed: false,
        })
    }

    fn covered_history_effects(
        &self,
        wallet: WalletAddress,
        source_epoch: i64,
        mutations: &[LedgerMutation],
    ) -> Vec<MarketHistoryRecord> {
        let mut first_buys: BTreeMap<String, (MarketId, SourceTradeId)> = BTreeMap::new();
        for mutation in mutations {
            let LedgerEffect::Trade {
                market_id,
                side: Side::Buy,
                ..
            } = mutation.effect.effective()
            else {
                continue;
            };
            if self.entry_gate.has_market(&wallet, market_id) {
                continue;
            }
            first_buys
                .entry(market_id.to_string())
                .and_modify(|(_, current)| {
                    if mutation.source_trade_id.0 < current.0 {
                        current.clone_from(&mutation.source_trade_id);
                    }
                })
                .or_insert_with(|| (market_id.clone(), mutation.source_trade_id.clone()));
        }
        first_buys
            .into_values()
            .map(|(market_id, source_trade_id)| MarketHistoryRecord {
                wallet,
                market_id,
                first_epoch: source_epoch,
                source_trade_id,
            })
            .collect()
    }

    fn apply_history_projection(
        &mut self,
        wallet: WalletAddress,
        history_effects: &[MarketHistoryRecord],
        history_status: Option<&WalletHistoryStatusRecord>,
    ) {
        for history in history_effects {
            self.entry_gate
                .record_entry(history.wallet, &history.market_id);
        }
        if let Some(status) = history_status.filter(|status| status.wallet == wallet) {
            if status.complete {
                self.complete_history.insert(wallet);
                if !self.entry_gate.has_wallet(&wallet) {
                    self.entry_gate
                        .merge_history(HashMap::from([(wallet, HashSet::new())]));
                }
            } else {
                self.complete_history.remove(&wallet);
            }
        }
    }

    fn derive_gate_results(
        wallet: WalletAddress,
        source_epoch: i64,
        decisions: &[TradeDecision],
        first_entries: &[(MarketId, SourceTradeId)],
    ) -> (
        Vec<EntryGateResultRecord>,
        Vec<MarketHistoryRecord>,
        BTreeMap<String, String>,
    ) {
        let outcomes = decisions
            .iter()
            .map(|decision| {
                (
                    decision.source_trade_id.0.clone(),
                    decision.entry.as_str().to_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let history = first_entries
            .iter()
            .map(|(market_id, source_trade_id)| MarketHistoryRecord {
                wallet,
                market_id: market_id.clone(),
                first_epoch: source_epoch,
                source_trade_id: source_trade_id.clone(),
            })
            .collect::<Vec<_>>();
        let gate_results = decisions
            .iter()
            .map(|decision| EntryGateResultRecord {
                source_trade_id: decision.source_trade_id.clone(),
                wallet,
                market_id: decision.market_id.clone(),
                source_epoch,
                result: decision.entry.as_str().to_owned(),
                history_consumed: history
                    .iter()
                    .any(|effect| effect.source_trade_id == decision.source_trade_id),
            })
            .collect();
        (gate_results, history, outcomes)
    }

    fn commit_fence(
        &mut self,
        aggregates: &[ActivityAggregate],
        wallet: WalletAddress,
        source_epoch: i64,
        cause: WalletFenceCause,
        trigger: SourceTradeId,
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let proof = json!({"bucket_epoch": source_epoch, "cause": cause.as_str()});
        let mut dispositions = BTreeMap::new();
        let mut records = Vec::with_capacity(aggregates.len());
        let mut unresolved_trigger = None;
        for aggregate in aggregates {
            let unresolved = context
                .identity_unresolved
                .contains(aggregate.group_id.key());
            let disposition = if unresolved {
                if !context.bracket_commit {
                    unresolved_trigger.get_or_insert_with(|| aggregate.group_id.key().clone());
                }
                "raw_only".to_owned()
            } else if aggregate.group_id.key() == &trigger {
                cause.as_str().to_owned()
            } else {
                "wallet_fenced".to_owned()
            };
            dispositions.insert(aggregate.group_id.key().0.clone(), disposition.clone());
            let mutation = recordable_mutation(aggregate, context);
            records.push(activity_record(
                aggregate,
                disposition,
                &mutation.effect,
                None,
                unresolved
                    .then(|| {
                        context
                            .no_copy_dispositions
                            .get(aggregate.group_id.key())
                            .cloned()
                    })
                    .flatten(),
            )?);
        }
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: Some(WalletFenceRecord {
                    wallet,
                    source_trade_id: trigger,
                    cause: cause.as_str().to_owned(),
                    proof_json: serde_json::to_string(&proof)?,
                    fenced_at_unix: context.recorded_at_unix,
                }),
                reanchor: unresolved_trigger.map(|source_trade_id| ReanchorRecord {
                    source_trade_id,
                    reason: "identity_unresolved".to_owned(),
                }),
                advance_cursor: true,
            })?;
        self.fences.insert(wallet);
        Ok(BucketCommitResult {
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: Some(cause),
            already_committed: false,
        })
    }

    fn commit_changed_bucket_fence(
        &mut self,
        aggregates: &[ActivityAggregate],
        durable: &[Option<ActivityGroupState>],
        wallet: WalletAddress,
        source_epoch: i64,
        fence: (WalletFenceCause, SourceTradeId),
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let (cause, trigger) = fence;
        let already_fenced = self.fences.contains(&wallet);
        let proof = json!({"bucket_epoch": source_epoch, "cause": cause.as_str()});
        let proof_json = serde_json::to_string(&proof)?;
        let mut dispositions = BTreeMap::new();
        let mut records = Vec::new();
        let mut unresolved_trigger = None;
        for (aggregate, state) in aggregates.iter().zip(durable) {
            let differs = state.as_ref().is_none_or(|state| {
                state.semantic_revision != aggregate.semantic_revision.as_str()
                    || state.transaction_hash != aggregate.group_id.components().transaction_hash
            });
            let unresolved = cause != WalletFenceCause::LateEqualSecondGroup
                && context
                    .identity_unresolved
                    .contains(aggregate.group_id.key());
            let disposition = if differs {
                if unresolved {
                    if !context.bracket_commit {
                        unresolved_trigger.get_or_insert_with(|| aggregate.group_id.key().clone());
                    }
                    "raw_only".to_owned()
                } else if aggregate.group_id.key() == &trigger && !already_fenced {
                    cause.as_str().to_owned()
                } else {
                    "wallet_fenced".to_owned()
                }
            } else {
                "already_committed".to_owned()
            };
            dispositions.insert(aggregate.group_id.key().0.clone(), disposition.clone());
            if differs {
                let mutation = recordable_mutation(aggregate, context);
                records.push(activity_record(
                    aggregate,
                    disposition,
                    &mutation.effect,
                    None,
                    unresolved
                        .then(|| {
                            context
                                .no_copy_dispositions
                                .get(aggregate.group_id.key())
                                .cloned()
                        })
                        .flatten(),
                )?);
            }
        }
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: (!already_fenced).then(|| WalletFenceRecord {
                    wallet,
                    source_trade_id: trigger,
                    cause: cause.as_str().to_owned(),
                    proof_json,
                    fenced_at_unix: context.recorded_at_unix,
                }),
                reanchor: unresolved_trigger.map(|source_trade_id| ReanchorRecord {
                    source_trade_id,
                    reason: "identity_unresolved".to_owned(),
                }),
                advance_cursor: true,
            })?;
        if !already_fenced {
            self.fences.insert(wallet);
        }
        Ok(BucketCommitResult {
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: (!already_fenced).then_some(cause),
            already_committed: false,
        })
    }

    fn commit_fenced_bucket(
        &mut self,
        aggregates: &[ActivityAggregate],
        mutations: &[LedgerMutation],
        wallet: WalletAddress,
        source_epoch: i64,
        context: &BucketDecisionContext,
    ) -> Result<BucketCommitResult, BucketCommitError> {
        let known: Vec<_> = mutations
            .iter()
            .filter(|mutation| {
                !matches!(
                    mutation.effect.effective(),
                    LedgerEffect::Conversion | LedgerEffect::UnknownEffect
                )
            })
            .cloned()
            .collect();
        let mut candidate = self.ledger.clone();
        let history_complete = context
            .history_status
            .as_ref()
            .filter(|status| status.wallet == wallet)
            .map_or_else(
                || self.complete_history.contains(&wallet),
                |status| status.complete,
            );
        let applied_outcomes = match classify_complete_second(
            &self.ledger,
            wallet,
            &known,
            context.reconstruction_quality,
            &context.signal_config,
            history_complete,
            &|market_id| self.entry_gate.has_market(&wallet, market_id),
        ) {
            Ok(SecondVerdict::OrderIndependent { applied, .. }) => {
                candidate.apply_all_or_none(&known).ok().map(|_| applied)
            }
            Ok(SecondVerdict::OrderDependent { .. }) | Err(_) => None,
        };
        let applied = applied_outcomes.is_some();
        let applied_by_id = known
            .iter()
            .zip(applied_outcomes.iter().flatten())
            .map(|(mutation, outcome)| (mutation.source_trade_id.clone(), outcome))
            .collect::<HashMap<_, _>>();
        let mut dispositions = BTreeMap::new();
        let unresolved_trigger = (!context.bracket_commit)
            .then(|| {
                mutations.iter().find_map(|mutation| {
                    context
                        .identity_unresolved
                        .contains(&mutation.source_trade_id)
                        .then(|| mutation.source_trade_id.clone())
                })
            })
            .flatten();
        let records = aggregates
            .iter()
            .zip(mutations)
            .map(|(aggregate, mutation)| {
                let disposition = if context
                    .identity_unresolved
                    .contains(&mutation.source_trade_id)
                {
                    "raw_only"
                } else if applied
                    && !matches!(
                        mutation.effect.effective(),
                        LedgerEffect::Conversion | LedgerEffect::UnknownEffect
                    )
                {
                    "wallet_fenced_applied"
                } else {
                    "wallet_fenced"
                };
                dispositions.insert(aggregate.group_id.key().0.clone(), disposition.to_owned());
                let applied_effect = applied_by_id.get(&mutation.source_trade_id);
                activity_record(
                    aggregate,
                    disposition.to_owned(),
                    applied_effect.map_or(&mutation.effect, |outcome| &outcome.effect),
                    applied_effect.and_then(|outcome| outcome.clamped_residual),
                    context
                        .no_copy_dispositions
                        .get(&mutation.source_trade_id)
                        .cloned(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.paper_state
            .commit_activity_bucket(&ActivityBucketCommit {
                wallet,
                source_epoch,
                dispositions: records,
                leader_positions: if applied {
                    touched_leader_rows(&candidate, wallet, &known)
                } else {
                    Vec::new()
                },
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: context.history_status.clone(),
                pending: Vec::new(),
                fence: None,
                reanchor: unresolved_trigger.map(|source_trade_id| ReanchorRecord {
                    source_trade_id,
                    reason: "identity_unresolved".to_owned(),
                }),
                advance_cursor: true,
            })?;
        if applied {
            self.ledger = candidate;
        }
        Ok(BucketCommitResult {
            wallet,
            source_epoch,
            dispositions,
            pending: Vec::new(),
            newly_fenced: None,
            already_committed: false,
        })
    }
}

fn activity_record(
    aggregate: &ActivityAggregate,
    disposition: String,
    effect: &LedgerEffect,
    clamped_residual: Option<u64>,
    no_copy: Option<NoCopyDisposition>,
) -> Result<ActivityDispositionRecord, LedgerEffectDocumentError> {
    let components = aggregate.group_id.components();
    Ok(ActivityDispositionRecord {
        source_trade_id: aggregate.group_id.key().clone(),
        transaction_hash: components.transaction_hash.clone(),
        wallet: components.wallet,
        source_epoch: aggregate.source_time.0.unix_timestamp(),
        semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
        activity_type: components.activity_type.as_str().to_owned(),
        disposition,
        proof_json: AppliedEffect {
            effect: effect.clone(),
            clamped_residual,
        }
        .to_document()?,
        no_copy,
    })
}

fn recordable_mutation(
    aggregate: &ActivityAggregate,
    context: &BucketDecisionContext,
) -> LedgerMutation {
    let mutation = LedgerMutation::from_activity(aggregate).unwrap_or_else(|_| LedgerMutation {
        source_trade_id: aggregate.group_id.key().clone(),
        transaction_hash: aggregate.group_id.components().transaction_hash.clone(),
        wallet: aggregate.group_id.components().wallet,
        source_time: aggregate.source_time.clone(),
        effect: LedgerEffect::UnknownEffect,
    });
    resolve_identity(mutation, context)
}

fn resolve_identity(
    mut mutation: LedgerMutation,
    context: &BucketDecisionContext,
) -> LedgerMutation {
    if context
        .identity_unresolved
        .contains(&mutation.source_trade_id)
    {
        mutation.effect = LedgerEffect::RawOnly;
        return mutation;
    }
    if let Some(identity) = context.identity_overrides.get(&mutation.source_trade_id) {
        return mutation
            .with_verified_identity(identity.verified.clone(), identity.evidence_hash.clone());
    }
    mutation
}

fn mutation_error_id(error: &LedgerError) -> SourceTradeId {
    match error {
        LedgerError::InvalidMapping { source_trade_id }
        | LedgerError::Underflow { source_trade_id }
        | LedgerError::Overflow { source_trade_id }
        | LedgerError::Conversion { source_trade_id }
        | LedgerError::UnknownEffect { source_trade_id } => source_trade_id.clone(),
    }
}

fn encode_key(key: &MarketOutcomeId) -> String {
    format!("{}|{}", key.market(), key.outcome().0)
}

fn touched_leader_rows(
    ledger: &PositionLedger,
    wallet: WalletAddress,
    mutations: &[LedgerMutation],
) -> Vec<LeaderPositionRow> {
    let mut keys: BTreeMap<String, MarketOutcomeId> = BTreeMap::new();
    for mutation in mutations {
        for key in mutation.touched_keys() {
            keys.insert(encode_key(&key), key);
        }
    }
    keys.into_values()
        .map(|key| {
            let state = ledger
                .position(&wallet)
                .and_then(|snapshot| snapshot.positions.get(&key))
                .copied()
                .unwrap_or_default();
            LeaderPositionRow {
                wallet,
                market_id: key.market().clone(),
                outcome_id: key.outcome(),
                long_contracts: state.long_contracts,
                short_contracts: state.short_contracts,
            }
        })
        .collect()
}

#[cfg(test)]
mod continuation_v3_tests {
    #![allow(clippy::unwrap_used)]

    use std::fs::OpenOptions;
    use std::io::{Read as _, Seek as _, SeekFrom, Write as _};

    use pe_core_types::{EventSeq, Probability, ReceivedAt, SourceId, SourceTimestamp};
    use pe_event_log::{ContentType, EnvelopeIn, Reader, Writer};
    use pe_paper_state::DecisionPendingState;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;

    fn receipt(sequence: u64) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap_or(u8::MAX); 32]),
        }
    }

    fn facts(decision_inputs: Value) -> DecisionContinuationFacts {
        let configuration =
            RuntimeConfig::from_service_config(&crate::config::ServiceConfig::default());
        DecisionContinuationFacts {
            source_trade_id: SourceTradeId("g2:continuation".to_owned()),
            semantic_revision: "semantic".to_owned(),
            transaction_hash: "0xtransaction".to_owned(),
            wallet: WalletAddress::from_hex("0x1111111111111111111111111111111111111111").unwrap(),
            source_epoch: 1_700_000_000,
            market_id: MarketId(pe_core_types::VenueMarketId("market".to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.5)).unwrap(),
            share_amount: ShareAmount::from_whole(1).unwrap(),
            provenance: TradeProvenance::ActivityWs,
            pre_bucket_action: LeaderAction::Entry,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
            gate_result: "admitted".to_owned(),
            applied_configuration_hash: configuration.canonical_hash(),
            applied_configuration: configuration,
            frozen_basis: FrozenDecisionBasis {
                win_rate_p: Probability::new(dec!(0.6)).unwrap(),
                bankroll: dec!(100),
            },
            decision_inputs,
        }
    }

    fn legacy_v2_json(facts: &DecisionContinuationFacts) -> String {
        let facts = serde_json::to_string(facts).unwrap();
        format!(r#"{{"version":2,{}"#, facts.strip_prefix('{').unwrap())
    }

    fn durable(value: &impl Serialize) -> DecisionPendingRow {
        let frozen_inputs_json = serde_json::to_string(value).unwrap();
        let continuation: DecisionContinuationV3 =
            serde_json::from_str(&frozen_inputs_json).unwrap();
        DecisionPendingRow {
            source_trade_id: continuation.facts.source_trade_id.clone(),
            semantic_revision: continuation.facts.semantic_revision.clone(),
            wallet: continuation.facts.wallet,
            source_epoch: continuation.facts.source_epoch,
            frozen_inputs_json,
            post_commit_inputs_json: String::new(),
            state: DecisionPendingState::Open,
            terminal_disposition: None,
            updated_at_unix: 1_700_000_001,
        }
    }

    /// PASS: V3 keeps repeated payload occurrences, derives the greatest complete bound, and picks
    /// the lower websocket receipt; the caller-supplied time is not persisted in continuation JSON.
    #[test]
    fn v3_observation_uses_lower_receipt_and_preserves_multiplicity() {
        let pages = vec![
            PageOccurrence {
                request_url: "https://source/page".to_owned(),
                raw_hash: "same".to_owned(),
                receipt: receipt(9),
            },
            PageOccurrence {
                request_url: "https://source/page".to_owned(),
                raw_hash: "same".to_owned(),
                receipt: receipt(11),
            },
        ];
        let value = DecisionContinuationV3::new(
            facts(json!({"fixed_end": 1_700_000_010_i64})),
            Some(receipt(7)),
            pages,
        );
        let decoded = DecisionContinuationV3::from_durable(&durable(&value)).unwrap();
        assert_eq!(decoded.version, 3);
        assert_eq!(decoded.page_occurrences().len(), 2);
        assert_eq!(decoded.complete_bound(), Some(receipt(11)));
        assert_eq!(
            decoded.observation_at(1_700_000_000_007),
            Some(ObservationEvidence {
                source_receipt: receipt(7),
                complete_bound_receipt: receipt(11),
                observed_unix_ms: 1_700_000_000_007,
                provenance: "activity_ws".to_owned(),
            })
        );
    }

    /// PASS: polling-only V3 uses the complete page bound, while a legacy V2 row has no fabricated
    /// observation evidence.
    #[test]
    fn poll_only_v3_and_legacy_v2_have_distinct_observation_semantics() {
        let value = DecisionContinuationV3::new(
            facts(json!({"fixed_end": 1_700_000_010_i64})),
            None,
            vec![PageOccurrence {
                request_url: "https://source/page".to_owned(),
                raw_hash: "hash".to_owned(),
                receipt: receipt(4),
            }],
        );
        let decoded = DecisionContinuationV3::from_durable(&durable(&value)).unwrap();
        assert_eq!(
            decoded.observation_at(1_700_000_000_004),
            Some(ObservationEvidence {
                source_receipt: receipt(4),
                complete_bound_receipt: receipt(4),
                observed_unix_ms: 1_700_000_000_004,
                provenance: "rest_poll".to_owned(),
            })
        );

        let legacy = facts(json!({"fixed_end": 1_700_000_010_i64}));
        let mut row = durable(&value);
        row.frozen_inputs_json = legacy_v2_json(&legacy);
        let decoded = DecisionContinuationV3::from_durable(&row).unwrap();
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.observation_at(1_700_000_000_004), None);
    }

    /// PASS: polling observation time and hashes are recovered through exact receipt-index reads
    /// even when an unrelated interior frame is unreadable; tampering a frozen raw-page hash
    /// still fails closed.
    #[test]
    fn receipt_index_is_the_scoped_observation_time_and_hash_owner() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("source.log");
        let mut writer = Writer::open(&path).unwrap();
        let append = |writer: &mut Writer, payload: &[u8], received_unix: i64| {
            let time = time::OffsetDateTime::from_unix_timestamp(received_unix).unwrap();
            writer
                .append_synced(EnvelopeIn {
                    source_id: SourceId(crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
                    schema_version: ACTIVITY_SCHEMA_VERSION,
                    parser_version: ACTIVITY_PARSER_VERSION,
                    observed_at: SourceTimestamp(time),
                    received_at: ReceivedAt(time),
                    content_type: ContentType::Json,
                    payload: payload.to_vec(),
                })
                .unwrap()
        };
        let first_payload = br#"[{"page":1}]"#;
        let unrelated_payload = br#"[{"unrelated":true}]"#;
        let second_payload = br#"[{"page":2}]"#;
        let first = append(&mut writer, first_payload, 1_700_000_001);
        let _unrelated = append(&mut writer, unrelated_payload, 1_700_000_001);
        let second = append(&mut writer, second_payload, 1_700_000_002);
        drop(writer);
        let offsets = Reader::replay_with_offsets(&path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let source_receipts = SourceReceiptIndex::replay(&path).unwrap();

        // Corrupt only the unrelated frame after boot built the verified index. A complete-log
        // replay now fails, while direct reads of the two receipt-bound page frames remain valid.
        let corrupt_at = offsets[1].0.checked_add(4).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.seek(SeekFrom::Start(corrupt_at)).unwrap();
        let mut byte = [0_u8; 1];
        file.read_exact(&mut byte).unwrap();
        file.seek(SeekFrom::Start(corrupt_at)).unwrap();
        file.write_all(&[byte[0] ^ 0xff]).unwrap();
        file.sync_all().unwrap();
        assert!(
            Reader::replay(&path).is_err(),
            "complete replay sees the corrupt neighbor"
        );

        let value = DecisionContinuationV3::new(
            facts(json!({"fixed_end": 1_700_000_010_i64})),
            None,
            vec![
                PageOccurrence {
                    request_url: "https://source/page/1".to_owned(),
                    raw_hash: blake3::hash(first_payload).to_hex().to_string(),
                    receipt: first,
                },
                PageOccurrence {
                    request_url: "https://source/page/2".to_owned(),
                    raw_hash: blake3::hash(second_payload).to_hex().to_string(),
                    receipt: second,
                },
            ],
        );
        let decoded = DecisionContinuationV3::from_durable(&durable(&value)).unwrap();
        assert_eq!(
            decoded
                .observation_from_receipt_index(&source_receipts)
                .unwrap(),
            Some(ObservationEvidence {
                source_receipt: second,
                complete_bound_receipt: second,
                observed_unix_ms: 1_700_000_002_000,
                provenance: "rest_poll".to_owned(),
            })
        );

        let mut tampered = decoded;
        tampered.page_occurrences[1].raw_hash = "00".repeat(32);
        assert!(matches!(
            tampered.observation_from_receipt_index(&source_receipts),
            Err(DecisionContinuationError::SourceReceiptMismatch { .. })
        ));
    }
}
