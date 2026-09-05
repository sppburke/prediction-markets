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
use pe_source_polymarket_public::ActivityAggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::entry_gate::{CopyEntryGate, CopyEntryGateConfig};
use crate::position_seeder::{AnchorInstall, ledger_capture};
use crate::runtime_config::RuntimeConfig;

/// Decision inputs already read before the atomic bucket commit.
#[derive(Debug, Clone)]
pub struct BucketDecisionContext {
    pub applied_configuration: RuntimeConfig,
    pub decision_inputs_json: String,
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

/// Versioned, self-contained continuation frozen by the bucket transaction.
/// Inputs read after this boundary are appended to paper/source logs and the
/// terminal `decision_pending` transition; offline replay never executes it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionContinuationV2 {
    pub version: u16,
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
}

impl DecisionContinuationV2 {
    /// Decode and bind a frozen continuation to its durable outer row.
    pub fn from_durable(row: &DecisionPendingRow) -> Result<Self, DecisionContinuationError> {
        let frozen: Self = serde_json::from_str(&row.frozen_inputs_json)?;
        if frozen.version != 2 {
            return Err(DecisionContinuationError::Version(frozen.version));
        }
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
        Ok(frozen)
    }

    /// Reconstruct only the transport-neutral trade facts needed by the existing
    /// idempotent decision continuation. Ledger/classification/gate are not rerun.
    pub fn incoming_trade(&self) -> Result<IncomingTrade, DecisionContinuationError> {
        let observed_at = time::OffsetDateTime::from_unix_timestamp(self.source_epoch)
            .map_err(|_| DecisionContinuationError::SourceEpoch(self.source_epoch))?;
        Ok(IncomingTrade {
            wallet: self.wallet,
            market_id: self.market_id.clone(),
            outcome_id: self.outcome_id,
            side: self.side,
            price: self.price,
            contracts: self.share_amount,
            observed_at,
            received_at: observed_at,
            source_trade_id: self.source_trade_id.clone(),
            transaction_hash: Some(self.transaction_hash.clone()),
            provenance: self.provenance,
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
                        let frozen = DecisionContinuationV2 {
                            version: 2,
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
                        pending.push(DecisionPendingRecord {
                            source_trade_id: source_trade_id.clone(),
                            semantic_revision: aggregate.semantic_revision.as_str().to_owned(),
                            wallet,
                            source_epoch,
                            frozen_inputs_json: serde_json::to_string(&frozen)?,
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
