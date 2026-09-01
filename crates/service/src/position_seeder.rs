//! Causal current-position validation for watchlist admission (#544).
//!
//! The ordered activity ledger remains the sole position owner. This module
//! performs the five-step activity/positions bracket and installs only an
//! accepted proof; it never overlays or reseeds the ledger.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use pe_copy_signal_engine::{PositionSnapshot, PositionState, SignalConfig};
use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, ReconstructionQuality, ShareAmount, VenueMarketId,
    WalletAddress,
};
use pe_paper_state::{PaperStateDb, PositionValidationRecord};
use pe_position_ledger::PositionLedger;
use pe_source_polymarket_public::{
    CompleteActivityRead, CompletePositionsRead, PositionClassification, ReconciliationFetcher,
    fetch_complete_activity, fetch_complete_positions,
};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{mpsc, oneshot};

use crate::bucket_commit::{BucketCommitEngine, BucketDecisionContext};
use crate::orchestrator_control::{AdmissionLedgerCapture, OrchestratorControl};

#[cfg(feature = "scenario")]
type BracketStepHook = Arc<dyn Fn(usize, &mut BucketCommitEngine) + Send + Sync>;

/// An accepted proof waiting for the orchestrator's final ledger-hash recheck.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionAcceptance {
    pub validation: PositionValidationRecord,
}

#[derive(Debug, thiserror::Error)]
pub enum CausalPositionError {
    #[error("activity catch-up for {wallet}: {source}")]
    Activity {
        wallet: WalletAddress,
        source: pe_source_polymarket_public::ActivityReadError,
    },
    #[error("positions read for {wallet}: {source}")]
    Positions {
        wallet: WalletAddress,
        source: pe_source_polymarket_public::PositionReadError,
    },
    #[error("activity bucket proof encoding failed: {0}")]
    ProofEncoding(#[from] serde_json::Error),
    #[error("activity bucket commit failed for {wallet}: {message}")]
    BucketCommit {
        wallet: WalletAddress,
        message: String,
    },
    #[error("wallet {wallet} became durably fenced during validation")]
    Fenced { wallet: WalletAddress },
    #[error("activity changed between bracket steps for {wallet}")]
    InterveningActivity { wallet: WalletAddress },
    #[error("current-position semantic proofs changed for {wallet}")]
    PositionRevision { wallet: WalletAddress },
    #[error("activity ledger changed between bracket steps for {wallet}")]
    LedgerRevision { wallet: WalletAddress },
    #[error("activity fixed end moved backwards for {wallet}: previous {previous}, next {next}")]
    NonMonotonicActivityBounds {
        wallet: WalletAddress,
        previous: i64,
        next: i64,
    },
    #[error("stable current positions do not match the activity ledger for {wallet}")]
    StableMismatch { wallet: WalletAddress },
    #[error("orchestrator control channel closed during position validation")]
    ControlClosed,
    #[error("orchestrator dropped a position-validation acknowledgement")]
    AcknowledgementClosed,
    #[error("ledger proof contains multiple assets for {condition_id} outcome {outcome}")]
    DuplicateOutcome { condition_id: String, outcome: u16 },
    #[error("ledger proof component is too long")]
    ProofComponentTooLong,
    #[error("paper-state position proof install failed: {0}")]
    PaperState(#[from] pe_paper_state::PaperStateError),
    #[error("reconstruction quality invariant failed")]
    ReconstructionQuality,
}

/// Shared real/fake-source bracket implementation.
#[derive(Clone)]
pub struct CausalPositionValidator {
    fetcher: Arc<dyn ReconciliationFetcher>,
    base_url: Arc<str>,
    source_log_generation: Arc<str>,
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    #[cfg(feature = "scenario")]
    step_hook: Option<BracketStepHook>,
}

impl CausalPositionValidator {
    pub fn new(
        fetcher: Arc<dyn ReconciliationFetcher>,
        base_url: impl Into<Arc<str>>,
        source_log_generation: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            fetcher,
            base_url: base_url.into(),
            source_log_generation: source_log_generation.into(),
            now: Arc::new(|| time::OffsetDateTime::now_utc().unix_timestamp()),
            #[cfg(feature = "scenario")]
            step_hook: None,
        }
    }

    /// Deterministic bracket clock for hermetic scenario tests.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_clock(mut self, now: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now = now;
        self
    }

    /// Inject a deterministic mutation between bracket steps in scenario tests.
    #[cfg(feature = "scenario")]
    #[must_use]
    pub fn with_step_hook(mut self, hook: BracketStepHook) -> Self {
        self.step_hook = Some(hook);
        self
    }

    /// Validate every wallet through the live orchestrator owner. The caller
    /// sends all returned acceptances in one final installation command.
    pub async fn validate_via_control(
        &self,
        wallets: &[WalletAddress],
        control_tx: &mpsc::Sender<OrchestratorControl>,
    ) -> Result<Vec<AdmissionAcceptance>, CausalPositionError> {
        let mut accepted = Vec::with_capacity(wallets.len());
        for wallet in wallets {
            accepted.push(self.validate_one_control(*wallet, control_tx).await?);
        }
        Ok(accepted)
    }

    /// Boot-time form used before producers start. The same bucket engine is
    /// moved into the orchestrator after the atomic proof installation.
    pub async fn validate_direct(
        &self,
        wallets: &[WalletAddress],
        engine: &mut BucketCommitEngine,
        paper_state: &PaperStateDb,
    ) -> Result<Vec<AdmissionAcceptance>, CausalPositionError> {
        let mut accepted = Vec::with_capacity(wallets.len());
        for wallet in wallets {
            accepted.push(self.validate_one_direct(*wallet, engine).await?);
        }
        let records = accepted
            .iter()
            .map(|acceptance| acceptance.validation.clone())
            .collect::<Vec<_>>();
        for record in &records {
            let current = ledger_capture(engine.ledger(), record.wallet)?;
            if current.hash != record.ledger_hash || engine.is_fenced(&record.wallet) {
                return Err(CausalPositionError::LedgerRevision {
                    wallet: record.wallet,
                });
            }
        }
        paper_state.record_position_validations(&records)?;
        Ok(accepted)
    }

    async fn validate_one_control(
        &self,
        wallet: WalletAddress,
        control_tx: &mpsc::Sender<OrchestratorControl>,
    ) -> Result<AdmissionAcceptance, CausalPositionError> {
        let first_activity = self.activity(wallet).await?;
        self.commit_control(wallet, &first_activity, control_tx, false)
            .await?;
        let first_ledger = capture_control(wallet, control_tx).await?;
        let first_mapping = first_activity
            .asset_mapping()
            .map_err(|source| CausalPositionError::Positions { wallet, source })?;
        let first_positions = self.positions(wallet, &first_mapping).await?;

        let second_activity = self.activity(wallet).await?;
        if self
            .commit_control(wallet, &second_activity, control_tx, true)
            .await?
        {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let second_ledger = capture_control(wallet, control_tx).await?;
        let second_mapping = second_activity
            .asset_mapping()
            .map_err(|source| CausalPositionError::Positions { wallet, source })?;
        let second_positions = self.positions(wallet, &second_mapping).await?;

        let final_activity = self.activity(wallet).await?;
        if self
            .commit_control(wallet, &final_activity, control_tx, true)
            .await?
        {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let final_ledger = capture_control(wallet, control_tx).await?;
        self.finish(
            wallet,
            [&first_activity, &second_activity, &final_activity],
            [&first_ledger, &second_ledger, &final_ledger],
            &first_positions,
            &second_positions,
        )
    }

    async fn validate_one_direct(
        &self,
        wallet: WalletAddress,
        engine: &mut BucketCommitEngine,
    ) -> Result<AdmissionAcceptance, CausalPositionError> {
        let first_activity = self.activity(wallet).await?;
        commit_direct(
            wallet,
            &first_activity,
            engine,
            false,
            &self.source_log_generation,
        )?;
        let first_ledger = ledger_capture(engine.ledger(), wallet)?;
        self.run_step_hook(1, engine);
        let first_mapping = first_activity
            .asset_mapping()
            .map_err(|source| CausalPositionError::Positions { wallet, source })?;
        let first_positions = self.positions(wallet, &first_mapping).await?;
        self.run_step_hook(2, engine);

        let second_activity = self.activity(wallet).await?;
        if commit_direct(
            wallet,
            &second_activity,
            engine,
            true,
            &self.source_log_generation,
        )? {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let second_ledger = ledger_capture(engine.ledger(), wallet)?;
        self.run_step_hook(3, engine);
        let second_mapping = second_activity
            .asset_mapping()
            .map_err(|source| CausalPositionError::Positions { wallet, source })?;
        let second_positions = self.positions(wallet, &second_mapping).await?;
        self.run_step_hook(4, engine);

        let final_activity = self.activity(wallet).await?;
        if commit_direct(
            wallet,
            &final_activity,
            engine,
            true,
            &self.source_log_generation,
        )? {
            return Err(CausalPositionError::InterveningActivity { wallet });
        }
        let final_ledger = ledger_capture(engine.ledger(), wallet)?;
        self.finish(
            wallet,
            [&first_activity, &second_activity, &final_activity],
            [&first_ledger, &second_ledger, &final_ledger],
            &first_positions,
            &second_positions,
        )
    }

    #[cfg(feature = "scenario")]
    fn run_step_hook(&self, step: usize, engine: &mut BucketCommitEngine) {
        if let Some(hook) = &self.step_hook {
            hook(step, engine);
        }
    }

    #[cfg(not(feature = "scenario"))]
    fn run_step_hook(&self, _step: usize, _engine: &mut BucketCommitEngine) {}

    async fn activity(
        &self,
        wallet: WalletAddress,
    ) -> Result<CompleteActivityRead, CausalPositionError> {
        fetch_complete_activity(
            self.fetcher.as_ref(),
            &self.base_url,
            wallet,
            None,
            (self.now)(),
        )
        .await
        .map_err(|source| CausalPositionError::Activity { wallet, source })
    }

    async fn positions(
        &self,
        wallet: WalletAddress,
        mapping: &pe_source_polymarket_public::ActivityAssetMapping,
    ) -> Result<CompletePositionsRead, CausalPositionError> {
        fetch_complete_positions(self.fetcher.as_ref(), &self.base_url, wallet, mapping)
            .await
            .map_err(|source| CausalPositionError::Positions { wallet, source })
    }

    async fn commit_control(
        &self,
        wallet: WalletAddress,
        activity: &CompleteActivityRead,
        control_tx: &mpsc::Sender<OrchestratorControl>,
        count_change: bool,
    ) -> Result<bool, CausalPositionError> {
        let mut changed = false;
        for bucket in activity
            .buckets()
            .map_err(|source| CausalPositionError::Activity { wallet, source })?
        {
            let (committed, acknowledgement) = oneshot::channel();
            control_tx
                .send(OrchestratorControl::CommitActivityBucket {
                    aggregates: bucket,
                    context: Box::new(bracket_context(activity, &self.source_log_generation)?),
                    committed,
                })
                .await
                .map_err(|_| CausalPositionError::ControlClosed)?;
            let result = acknowledgement
                .await
                .map_err(|_| CausalPositionError::AcknowledgementClosed)?
                .map_err(|message| CausalPositionError::BucketCommit { wallet, message })?;
            if result.newly_fenced.is_some() {
                return Err(CausalPositionError::Fenced { wallet });
            }
            changed |= count_change && !result.already_committed;
        }
        Ok(changed)
    }

    fn finish(
        &self,
        wallet: WalletAddress,
        activities: [&CompleteActivityRead; 3],
        ledgers: [&AdmissionLedgerCapture; 3],
        first_positions: &CompletePositionsRead,
        second_positions: &CompletePositionsRead,
    ) -> Result<AdmissionAcceptance, CausalPositionError> {
        for pair in activities.windows(2) {
            if pair[1].fixed_end < pair[0].fixed_end {
                return Err(CausalPositionError::NonMonotonicActivityBounds {
                    wallet,
                    previous: pair[0].fixed_end,
                    next: pair[1].fixed_end,
                });
            }
        }
        if !first_positions.semantically_equal(second_positions) {
            return Err(CausalPositionError::PositionRevision { wallet });
        }
        if ledgers[0].hash != ledgers[1].hash || ledgers[1].hash != ledgers[2].hash {
            return Err(CausalPositionError::LedgerRevision { wallet });
        }
        let position_balances = ordinary_position_balances(first_positions)?;
        if position_balances != ledgers[2].positive_ordinary_balances
            || ledgers[2].has_positive_short
        {
            return Err(CausalPositionError::StableMismatch { wallet });
        }

        let activity_bounds = activities
            .iter()
            .map(|activity| {
                activity
                    .pages
                    .iter()
                    .map(|page| {
                        json!({
                            "request_url": page.request_url,
                            "bounds": page.bounds,
                            "offset": page.offset,
                            "row_count": page.row_count,
                            "canonical_page_hash": page.canonical_page_hash,
                            "raw_page_hash": page.raw_page_hash,
                            "received_at": page.received_at,
                            "schema_version": page.schema_version,
                            "parser_version": page.parser_version,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let proof = json!({
            "version": 1,
            "wallet": wallet,
            "positions_semantic_hash": first_positions.semantic_hash(),
            "ledger_hash": ledgers[2].hash,
            "source_log_generation": self.source_log_generation,
            "activity_walks": activity_bounds,
            "positions_pages": [
                first_positions.pages,
                second_positions.pages,
            ],
        });
        Ok(AdmissionAcceptance {
            validation: PositionValidationRecord {
                wallet,
                ledger_hash: ledgers[2].hash.clone(),
                positions_proof_hash: first_positions.semantic_hash().to_owned(),
                activity_bounds_json: serde_json::to_string(&activity_bounds)?,
                source_log_generation: self.source_log_generation.to_string(),
                proof_json: serde_json::to_string(&proof)?,
                recorded_at_unix: (self.now)(),
            },
        })
    }
}

fn bracket_context(
    activity: &CompleteActivityRead,
    source_log_generation: &str,
) -> Result<BucketDecisionContext, CausalPositionError> {
    let reconstruction_quality =
        ReconstructionQuality::new(100).map_err(|_| CausalPositionError::ReconstructionQuality)?;
    Ok(BucketDecisionContext {
        applied_configuration_hash: "causal-position-bracket-v1".to_owned(),
        decision_inputs_json: serde_json::to_string(&json!({
            "fixed_end": activity.fixed_end,
            "pages": activity.pages,
            "source_log_generation": source_log_generation,
        }))?,
        reconstruction_quality,
        signal_config: SignalConfig::default(),
        copy_eligible: false,
        recorded_at_unix: time::OffsetDateTime::now_utc().unix_timestamp(),
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
        history_status: None,
    })
}

fn commit_direct(
    wallet: WalletAddress,
    activity: &CompleteActivityRead,
    engine: &mut BucketCommitEngine,
    count_change: bool,
    source_log_generation: &str,
) -> Result<bool, CausalPositionError> {
    let mut changed = false;
    for bucket in activity
        .buckets()
        .map_err(|source| CausalPositionError::Activity { wallet, source })?
    {
        let result = engine
            .commit(bucket, &bracket_context(activity, source_log_generation)?)
            .map_err(|error| CausalPositionError::BucketCommit {
                wallet,
                message: error.to_string(),
            })?;
        if result.newly_fenced.is_some() {
            return Err(CausalPositionError::Fenced { wallet });
        }
        changed |= count_change && !result.already_committed;
    }
    Ok(changed)
}

/// Canonical exact ledger proof for one wallet.
pub fn ledger_capture(
    ledger: &PositionLedger,
    wallet: WalletAddress,
) -> Result<AdmissionLedgerCapture, CausalPositionError> {
    let mut all = BTreeMap::<(String, u16), (ShareAmount, ShareAmount)>::new();
    if let Some(snapshot) = ledger.position(&wallet) {
        for (key, state) in &snapshot.positions {
            all.insert(
                (key.market().to_string(), key.outcome().0),
                (state.long_contracts, state.short_contracts),
            );
        }
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"prediction-edge/admission-ledger/v1\0");
    hash_part(&mut hasher, wallet.to_string().as_bytes())?;
    let mut positive_ordinary_balances = BTreeMap::new();
    let mut has_positive_short = false;
    for ((condition_id, outcome), (long, short)) in all {
        hash_part(&mut hasher, condition_id.as_bytes())?;
        hash_part(&mut hasher, &outcome.to_be_bytes())?;
        hash_part(&mut hasher, &long.atomic().to_be_bytes())?;
        hash_part(&mut hasher, &short.atomic().to_be_bytes())?;
        if long > ShareAmount::ZERO {
            positive_ordinary_balances.insert((condition_id, outcome), long);
        }
        has_positive_short |= short > ShareAmount::ZERO;
    }
    Ok(AdmissionLedgerCapture {
        wallet,
        hash: hasher.finalize().to_hex().to_string(),
        positive_ordinary_balances,
        has_positive_short,
    })
}

fn ordinary_position_balances(
    read: &CompletePositionsRead,
) -> Result<BTreeMap<(String, u16), ShareAmount>, CausalPositionError> {
    let mut balances = BTreeMap::new();
    for position in &read.positions {
        if position.classification == PositionClassification::Combo
            || position.size == ShareAmount::ZERO
        {
            continue;
        }
        let key = (position.condition_id.0.clone(), position.outcome.0);
        if balances.insert(key.clone(), position.size).is_some() {
            return Err(CausalPositionError::DuplicateOutcome {
                condition_id: key.0,
                outcome: key.1,
            });
        }
    }
    Ok(balances)
}

fn hash_part(hasher: &mut blake3::Hasher, value: &[u8]) -> Result<(), CausalPositionError> {
    let len = u64::try_from(value.len()).map_err(|_| CausalPositionError::ProofComponentTooLong)?;
    hasher.update(&len.to_be_bytes());
    hasher.update(value);
    Ok(())
}

async fn capture_control(
    wallet: WalletAddress,
    control_tx: &mpsc::Sender<OrchestratorControl>,
) -> Result<AdmissionLedgerCapture, CausalPositionError> {
    let (captured, acknowledgement) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::CaptureAdmissionLedger { wallet, captured })
        .await
        .map_err(|_| CausalPositionError::ControlClosed)?;
    acknowledgement
        .await
        .map_err(|_| CausalPositionError::AcknowledgementClosed)?
        .map_err(|message| CausalPositionError::BucketCommit { wallet, message })
}

// Retained only for the isolated stopped canary, whose authenticated posture
// needs strict `outcomeIndex` parsing but does not mutate the ordinary ledger.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CanaryRawPosition {
    condition_id: String,
    outcome_index: Option<u16>,
    size: Decimal,
}

#[derive(Debug, thiserror::Error)]
pub enum PositionParseError {
    #[error("failed to deserialize positions response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("position omitted outcomeIndex")]
    MissingOutcomeIndex,
    #[error("position has invalid exact size {value}: {reason}")]
    InvalidAmount { value: Decimal, reason: String },
}

pub fn parse_positions_strict(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<PositionSnapshot, PositionParseError> {
    let raw: Vec<CanaryRawPosition> = serde_json::from_slice(bytes)?;
    let mut positions = HashMap::new();
    for item in raw {
        let outcome = item
            .outcome_index
            .ok_or(PositionParseError::MissingOutcomeIndex)?;
        let amount = ShareAmount::from_decimal_exact(item.size).map_err(|error| {
            PositionParseError::InvalidAmount {
                value: item.size,
                reason: error.to_string(),
            }
        })?;
        if amount == ShareAmount::ZERO {
            continue;
        }
        positions.insert(
            MarketOutcomeId::new(
                MarketId(VenueMarketId(item.condition_id)),
                OutcomeId(outcome),
            ),
            PositionState {
                long_contracts: amount,
                short_contracts: ShareAmount::ZERO,
            },
        );
    }
    Ok(PositionSnapshot { wallet, positions })
}
