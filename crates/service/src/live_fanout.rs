//! Strictly sequential ordinary-live dispatch consumer (#508 Decision 10).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use age::x25519::Identity;
use pe_copy_signal_engine::LeaderSignal;
#[cfg(test)]
use pe_core_types::ReconstructionQuality;
use pe_core_types::{
    AccountId, CollateralAmount, MarketId, MarketOutcomeId, OutcomeId, PolymarketConditionId,
    Price, Probability, RawHttpAttempt, ReceivedAt, ShareAmount, Side, SourceId, SourceTimestamp,
    SourceTradeId, TraderId, VenueId, VenueMarketId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, EventEnvelope};
use pe_execution_core::live_executor::{
    LiveAccountResponseClassification, LiveAdmissionAccountEvidence,
    LiveAdmissionClassificationInput, LiveAdmissionEvaluationError,
    classify_live_account_responses, classify_live_admission, verify_live_admission_evaluation,
};
use pe_execution_core::live_journal::{
    LiveAccountBindingAudit, LivePositionEvidenceAudit, LivePositionPageAudit,
    PolygonFinalityClassification, SanitizedHttpRequestDescriptor, classify_polygon_finality,
    verify_polygon_finalized, verify_polygon_reconciliation,
};
use pe_execution_core::{
    AdmissionReceipts, CanonicalPositionAudit, CredentialBindingIdentity, EconomicInputs,
    EconomicPrepared, FrozenLiveTarget, LiveAdmissionArtifact, LiveAdmissionRefusal,
    LiveControlMode, LiveExecutor, LiveFillProjectionIdentity, LiveJournal, LiveJournalEvent,
    LiveJournalOrderOutcome, LiveJournalPayload, LiveModeSnapshot, LiveModeTransitionAudit,
    LiveModeTransitionReason, LiveOrderAmbiguityKind, LiveOrderIdentity, LiveOrderOutcome,
    LiveOrderReconciliationAudit, LiveOrderVenue, LivePrepareResult, LiveReconciliationSource,
    LiveRecoveryInventory, LiveVenueReconciledOutcome, MarkKind, ObservationEvidence,
    OrderFillFinalizedAudit, PreparedOrderFact, RedemptionAttempt, RedemptionAttemptIdentity,
    RedemptionAttemptState, RedemptionPassInput, RiskAudit, RiskDecisionAudit, SizingModeAudit,
    TerminalAdmissionRecoveryOutcome, http_attempt_hashes, prepared_order_fact_matches,
    reconstruct_redemption_attempts, recovery_inventory, redemption_posture, replay_account,
    run_redemption_pass,
};
use pe_kelly_sizer::{KellyInput, size_contracts};
use pe_paper_state::{DispatchSeedRow, DispatchTargetRow, PaperStateDb};
use pe_risk_engine::snapshot::{RiskSnapshot, TradingMode};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ActivityAssetMapping, BinaryPayoutVector, ClobPayoutResolution, ClobPricesHistoryClient,
    FixtureFetcher, GAMMA_MARKETS_PARSER_VERSION, GAMMA_MARKETS_SCHEMA_VERSION,
    GAMMA_MARKETS_SOURCE_ID, LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION,
    PositionClassification, ReconciliationFetcher, fetch_complete_positions,
    parse_activity_trade_observation, parse_clob_market, validate_live_market,
};
use pe_strategy_winner_follow::{
    ExecutionMode, SizingMode, WinnerFollowError, WinnerFollowStrategy,
};
use pe_venue_polymarket::{
    BuySizing, CustodyKind, LadderError, MatchedReceipt, ReceiptError, RedemptionTransport,
    RelayerApiKeyCredentials, RelayerCredentials, RelayerPollPolicy, build_redemption_call,
    canonical_block_matches, parse_chain_id_response, parse_compact_market,
    parse_finalized_block_response, parse_receipt_response, plan_sized_buy,
    sign_deposit_wallet_redemption,
};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{error, info, warn};

use crate::activity_ingest::SourceLogHandle;
#[cfg(test)]
use crate::bucket_commit::PageOccurrence;
use crate::bucket_commit::{CompleteActivityPage, DecisionContinuationV3};
use crate::clob_book::{
    CLOB_BOOK_PARSER_VERSION, CLOB_BOOK_SCHEMA_VERSION, CLOB_BOOK_SOURCE_ID, ClobBookFetcher,
    ReqwestClobBookFetcher,
};
use crate::live_accounts::{AccountContext, LiveAccounts};
use crate::live_credentials::{
    CredentialBinding, CredentialError, LiveAccountCredentials, decrypt_bundle,
};
use crate::live_mode::{
    ArmingProbe, CheckOutcome, ModeDecision, ModeInputs, PromotionEventRow, PromotionFacts,
    QualificationFacts, evaluate_mode, promotion_facts_from_rows,
};
use crate::live_projections::{
    LiveAccountStateRow, LiveFillRow, LivePositionRow, LiveProjectionWriter,
};
use crate::live_venue_adapter::{
    CLOB_COMPACT_MARKET_SOURCE_ID, CLOB_LONG_MARKET_SOURCE_ID, LIVE_MARKET_FRESHNESS_SECS,
    LiveAdmissionBuilder, LiveRedemptionAdapter, LiveVenueAdapterError, PolygonReceiptReader,
    PolygonReceiptRpc, PolymarketLiveVenue,
};
use crate::live_watchlist::LiveWatchlist;
use crate::mark_prices::HistoricalMarkAdapter;
#[cfg(test)]
use crate::mid_price_cache::{
    GAMMA_PRICE_ATTEMPT_PARSER_VERSION, GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION, GammaPriceAttemptRecord,
};
use crate::mid_price_cache::{
    MidPriceCache, MidPriceObservation, RecordedPriceAttempt, RiskPriceReplayError,
    replay_strict_risk_prices,
};
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{
    FINANCIAL_SEMANTIC_VERSION, HaltState, PaperEra, PaperLogFrame, PaperLogRecord, RiskHaltOwner,
    ScannedPaperFrame, active_risk_halts, paper_era, scan_paper_log,
};
use crate::risk_inputs::{
    RiskInputsUnavailable, SourceReceiptIndex, apply_global_risk_halts,
    paper_prefix_at_financial_prefix,
};
use crate::runtime_config::LiveRuntimeConfig;
use crate::supabase_reader::auth_token;
use crate::supervisor::{ShutdownController, ShutdownPhase};

const FANOUT_INTERVAL_SECS: u64 = 2;
const MODE_INTERVAL_SECS: i64 = 30;
const PRUNE_INTERVAL_SECS: i64 = 3_600;
const DISPATCH_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;
const REDEMPTION_RETRY_SECS: i64 = 30;
/// Ordinary redemption inventory/status reconcile cadence (Decision 12).
const REDEMPTION_RECONCILE_CADENCE_SECS: i64 = 300;
const COMPLETE_POSITIONS_SOURCE_ID: &str = "polymarket.data.complete-positions";
const COMPLETE_POSITIONS_SCHEMA_VERSION: u32 = 2;
// verified 2026-08-11 from the official four-minute Deposit Wallet batch example:
// https://github.com/Polymarket/builder-relayer-client#execute-deposit-wallet-batch
const DEPOSIT_WALLET_REDEMPTION_DEADLINE_SECS: u64 = 4 * 60;

/// Inputs owned by the one strictly sequential live task.
pub struct LiveFanoutConfig {
    pub paper_state: Arc<PaperStateDb>,
    pub live_accounts: LiveAccounts,
    pub live_watchlist: LiveWatchlist,
    pub runtime_config: LiveRuntimeConfig,
    /// Canonical report emitted by `--qualify`, loaded once through the boot path.
    pub qualification: Option<QualificationFacts>,
    pub identity: Option<Identity>,
    pub journal: Arc<LiveJournal>,
    pub journal_path: PathBuf,
    /// QualificationStart-bound live prefix, verified once before this fanout is constructed.
    pub era_live_prefix: Option<pe_event_log::LogTailBinding>,
    pub projection: LiveProjectionWriter,
    pub book_fetcher: Arc<ReqwestClobBookFetcher>,
    pub mid_price_cache: MidPriceCache,
    pub source_log: SourceLogHandle,
    /// Boot-verified, append-extended source receipt and evidence projection.
    pub source_receipts: SourceReceiptIndex,
    pub paper_log_path: PathBuf,
    pub orchestrator_control: tokio::sync::mpsc::Sender<OrchestratorControl>,
    pub http: reqwest::Client,
    pub polygon_receipt_rpc_url: String,
    pub supabase_url: String,
    pub supabase_anon_key: String,
    pub supabase_secret_key: String,
    pub gamma_base_url: String,
    pub clob_base_url: String,
    pub data_base_url: String,
    pub projection_reconcile_interval_secs: u64,
    /// Cheap ordered-shutdown probe used to revoke Approved resumption at StopProducers.
    pub shutdown: ShutdownController,
}

#[derive(Default)]
struct AdmissionClosures {
    mode: HashMap<String, String>,
    redemption: HashMap<String, String>,
}

impl AdmissionClosures {
    fn reason(&self, account_id: &str) -> Option<&str> {
        self.redemption
            .get(account_id)
            .or_else(|| self.mode.get(account_id))
            .map(String::as_str)
    }
}

struct FanoutState {
    config: LiveFanoutConfig,
    admission: LiveAdmissionBuilder,
    polygon_receipt_rpc: PolygonReceiptRpc,
    history_fetcher: HistoricalMarkAdapter,
    closures: AdmissionClosures,
    last_mode_unix: Option<i64>,
    last_redemption_unix: Option<i64>,
    last_projection_unix: Option<i64>,
    last_prune_unix: Option<i64>,
}

fn replay_live_account(
    state: &FanoutState,
    account_id: &AccountId,
) -> Result<Vec<LiveJournalEvent>, pe_execution_core::LiveJournalError> {
    let mut events = replay_account(&state.config.journal_path, account_id)?;
    if let Some(prefix) = &state.config.era_live_prefix {
        events.retain(|event| prefix.last_sequence.is_none_or(|last| event.seq > last.0));
    }
    let baseline_index =
        pe_execution_core::live_journal::first_account_baseline_index(&events, account_id)
            .unwrap_or(events.len());
    Ok(events.split_off(baseline_index))
}

fn derive_projection_rows_for_state(
    state: &FanoutState,
    account_id: &AccountId,
    events: &[LiveJournalEvent],
) -> Result<ProjectionDerivation, ProjectionReducerError> {
    let source_envelopes = source_envelopes_for_live_events(&state.config.source_receipts, events)?;
    let paper_frames = scan_paper_log(&state.config.paper_log_path)
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let observation_pages = live_observation_page_index(
        &state.config.paper_state,
        &referenced_source_trade_ids(events),
    )?;
    verify_replayed_live_risks(
        account_id,
        &state.config.journal_path,
        events,
        &source_envelopes,
        Some(&state.config.source_receipts),
        &paper_frames,
        &observation_pages,
    )?;
    derive_projection_rows_with_sources(account_id, events, &source_envelopes)
}

fn source_envelopes_for_live_events(
    source_receipts: &SourceReceiptIndex,
    events: &[LiveJournalEvent],
) -> Result<Vec<EventEnvelope>, ProjectionReducerError> {
    let mut receipts = Vec::new();
    for event in events {
        match &event.payload {
            LiveJournalPayload::AdmissionEvaluated(admission) => {
                receipts.extend([
                    admission.economic.admission.receipts.gamma,
                    admission.economic.admission.receipts.clob_long,
                    admission.economic.admission.receipts.clob_compact,
                    admission.economic.book_receipt,
                ]);
                receipts.extend(admission.economic.risk.price_receipts.iter().copied());
                if let Some(observation) = &admission.economic.observation {
                    receipts.extend([
                        observation.source_receipt,
                        observation.complete_bound_receipt,
                    ]);
                }
            }
            LiveJournalPayload::ResolutionFinalized(resolution) => {
                receipts.push(resolution.source_append_receipt);
            }
            LiveJournalPayload::RedemptionCustodyReconciled(custody) => {
                receipts.extend(custody.venue_position_receipts.iter().copied());
            }
            LiveJournalPayload::AccountPortfolioMarked(mark) => {
                receipts.extend(
                    mark.venue_position_evidence
                        .pages
                        .iter()
                        .map(|page| page.receipt),
                );
                receipts.extend(mark.prices.iter().map(|price| price.receipt));
            }
            _ => {}
        }
    }
    receipts.sort_by_key(|receipt| receipt.sequence);
    if receipts
        .windows(2)
        .any(|pair| pair[0].sequence == pair[1].sequence && pair[0] != pair[1])
    {
        return Err(ProjectionReducerError::InvalidRiskEvidence);
    }
    receipts.dedup_by_key(|receipt| receipt.sequence);
    receipts
        .into_iter()
        .map(|receipt| {
            source_receipts
                .source_envelope(receipt)
                .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)
        })
        .collect()
}

/// Start the first-boot arming fence, then drive mode, redemption, retention, and ordered fan-out.
pub async fn run_live_fanout(config: LiveFanoutConfig) {
    let _ = run_live_fanout_until(config, std::future::pending()).await;
}

/// Live fan-out owner failure surfaced to the named supervisor (#544).
#[derive(Debug, thiserror::Error)]
pub enum LiveFanoutOwnerError {
    #[error("record first-boot fence: {0}")]
    FirstBootFence(#[source] pe_paper_state::PaperStateError),
    #[error("read recovery state during shutdown: {0}")]
    ShutdownRecovery(String),
    #[error("drain live recovery during shutdown: {0}")]
    ShutdownDrain(String),
    #[error("inspect live journal poison state: {0}")]
    JournalState(#[source] pe_execution_core::LiveJournalError),
    #[error("live journal is poisoned")]
    JournalPoisoned,
    #[error("verify live journal tail: {0}")]
    VerifyJournal(#[source] pe_execution_core::LiveJournalError),
}

/// Run until the supervisor closes this sink, then finish only known in-flight recovery work and
/// verify the journal tail before returning (#544).
pub async fn run_live_fanout_until(
    config: LiveFanoutConfig,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), LiveFanoutOwnerError> {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    match config.paper_state.record_live_executor_first_boot(now) {
        Ok(fence) => info!(
            fence_unix = fence,
            "ordinary live executor first-boot fence ready"
        ),
        Err(error) => {
            return Err(LiveFanoutOwnerError::FirstBootFence(error));
        }
    }
    let admission = LiveAdmissionBuilder::new(
        config.http.clone(),
        config.gamma_base_url.clone(),
        config.clob_base_url.clone(),
        config.source_log.clone(),
    );
    let polygon_receipt_rpc =
        PolygonReceiptRpc::new(config.http.clone(), config.polygon_receipt_rpc_url.clone());
    let history_fetcher = HistoricalMarkAdapter::new(
        config.http.clone(),
        config.clob_base_url.clone(),
        config.source_log.clone(),
    );
    let mut state = FanoutState {
        config,
        admission,
        polygon_receipt_rpc,
        history_fetcher,
        closures: AdmissionClosures::default(),
        last_mode_unix: None,
        last_redemption_unix: None,
        last_projection_unix: None,
        last_prune_unix: None,
    };
    reconcile_projections(&mut state, OffsetDateTime::now_utc()).await;
    state.last_projection_unix = Some(OffsetDateTime::now_utc().unix_timestamp());
    let mut ticker = tokio::time::interval(Duration::from_secs(FANOUT_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => break,
            _ = ticker.tick() => {}
        }
        if state
            .config
            .journal
            .poisoned()
            .map_err(LiveFanoutOwnerError::JournalState)?
            .is_some()
        {
            return Err(LiveFanoutOwnerError::JournalPoisoned);
        }
        let now = OffsetDateTime::now_utc();
        let projection_interval =
            i64::try_from(state.config.projection_reconcile_interval_secs.max(1))
                .unwrap_or(i64::MAX);
        if due(
            state.last_projection_unix,
            now.unix_timestamp(),
            projection_interval,
        ) {
            reconcile_projections(&mut state, now).await;
            state.last_projection_unix = Some(now.unix_timestamp());
        }
        if due(
            state.last_mode_unix,
            now.unix_timestamp(),
            MODE_INTERVAL_SECS,
        ) {
            drive_modes(&mut state, now).await;
            state.last_mode_unix = Some(now.unix_timestamp());
        }
        match recovery_is_pending(&state) {
            Ok(true) => {
                if let Err(error) =
                    run_recovery_pass(&mut state, OffsetDateTime::now_utc, true).await
                {
                    error!(error = %error, "live recovery-first pass failed; fan-out remains frozen");
                }
                // Approved/prepared recovery work is the only work allowed on this tick. If it
                // converges to a durable preparation result, ordinary work resumes next tick.
                continue;
            }
            Ok(false) => {}
            Err(error) => {
                error!(error = %error, "live recovery state unreadable; fan-out remains frozen");
                continue;
            }
        }
        if due(
            state.last_redemption_unix,
            now.unix_timestamp(),
            REDEMPTION_RECONCILE_CADENCE_SECS,
        ) {
            drive_redemptions(&mut state, now).await;
            state.last_redemption_unix = Some(now.unix_timestamp());
        }
        if let Err(error) = run_dispatch_pass(&mut state).await {
            error!(error = %error, "live fan-out pass failed; retrying from durable state");
        }
        if due(
            state.last_prune_unix,
            now.unix_timestamp(),
            PRUNE_INTERVAL_SECS,
        ) {
            match state
                .config
                .paper_state
                .prune_terminal_dispatch(now.unix_timestamp(), DISPATCH_RETENTION_SECS)
            {
                Ok(pruned) if pruned > 0 => info!(pruned, "pruned terminal live dispatch seeds"),
                Ok(_) => {}
                Err(error) => warn!(error = %error, "live dispatch retention prune failed"),
            }
            state.last_prune_unix = Some(now.unix_timestamp());
        }
    }

    if recovery_is_pending(&state)
        .map_err(|error| LiveFanoutOwnerError::ShutdownRecovery(error.to_string()))?
    {
        run_recovery_pass(&mut state, OffsetDateTime::now_utc, false)
            .await
            .map_err(|error| LiveFanoutOwnerError::ShutdownDrain(error.to_string()))?;
    }
    if state
        .config
        .journal
        .poisoned()
        .map_err(LiveFanoutOwnerError::JournalState)?
        .is_some()
    {
        return Err(LiveFanoutOwnerError::JournalPoisoned);
    }
    LiveJournal::verified_tail(&state.config.journal_path)
        .map_err(LiveFanoutOwnerError::VerifyJournal)?;
    Ok(())
}

fn verified_recovery_inventory(state: &FanoutState) -> Result<LiveRecoveryInventory, FanoutError> {
    let paper_frames = scan_paper_log(&state.config.paper_log_path)
        .map_err(|error| FanoutError::Signal(format!("paper risk prefix: {error}")))?;
    let inventory = pe_execution_core::live_journal::recovery_inventory_with_admission_verifier(
        &state.config.journal_path,
        state.config.era_live_prefix.as_ref(),
        |event, preceding_events| {
            let LiveJournalPayload::AdmissionEvaluated(admission) = &event.payload else {
                return Ok(());
            };
            let account_prefix = preceding_events
                .iter()
                .filter(|prior| prior.account_id == event.account_id)
                .cloned()
                .collect::<Vec<_>>();
            let mut scoped_events = account_prefix.clone();
            scoped_events.push(event.clone());
            let source_envelopes =
                source_envelopes_for_live_events(&state.config.source_receipts, &scoped_events)
                    .map_err(|_| pe_execution_core::LiveJournalError::OrderFactConflict)?;
            let observation_pages = live_observation_page_index(
                &state.config.paper_state,
                &referenced_source_trade_ids(std::slice::from_ref(event)),
            )
            .map_err(|_| pe_execution_core::LiveJournalError::OrderFactConflict)?;
            verify_replayed_live_risk_with_index(
                &event.account_id,
                &state.config.journal_path,
                &account_prefix,
                admission.as_ref(),
                LiveReplayEvidence {
                    source_envelopes: &source_envelopes,
                    source_receipts: Some(&state.config.source_receipts),
                    paper_frames: &paper_frames,
                    observation_pages: &observation_pages,
                },
            )
            .map_err(|_| pe_execution_core::LiveJournalError::OrderFactConflict)
        },
    )?;
    let mut recoverable_accounts = inventory
        .approved_admissions
        .iter()
        .map(|entry| entry.account_id.clone())
        .chain(
            inventory
                .open_orders
                .iter()
                .map(|entry| entry.inventory.account_id.clone()),
        )
        .chain(
            inventory
                .terminal_admissions
                .iter()
                .map(|entry| entry.account_id.clone()),
        )
        .collect::<BTreeSet<_>>();
    for account_id in std::mem::take(&mut recoverable_accounts) {
        let events = replay_live_account(state, &account_id)?;
        let source_envelopes =
            source_envelopes_for_live_events(&state.config.source_receipts, &events).map_err(
                |error| {
                    FanoutError::Signal(format!(
                        "live recovery source evidence for {account_id}: {error}"
                    ))
                },
            )?;
        let derived = derive_projection_rows_with_sources(&account_id, &events, &source_envelopes)
            .map_err(|error| {
                FanoutError::Signal(format!(
                    "live recovery strict reduction failed for {account_id}: {error}"
                ))
            })?;
        if derived.baseline_sequence.is_none() {
            return Err(FanoutError::Signal(format!(
                "live recovery account {account_id} has no verified financial-era Baseline"
            )));
        }
    }
    Ok(inventory)
}

fn terminal_recovery_reason(outcome: &TerminalAdmissionRecoveryOutcome) -> &'static str {
    match outcome {
        TerminalAdmissionRecoveryOutcome::Refused(
            LiveAdmissionRefusal::CredentialVersionChanged,
        ) => "credential_version_changed",
        TerminalAdmissionRecoveryOutcome::Refused(_) => "admission_refused",
        TerminalAdmissionRecoveryOutcome::PreparationFailed(
            pe_execution_core::LiveOrderPreparationFailure::RecoveryCredentialChanged,
        ) => "recovery_credential_changed",
        TerminalAdmissionRecoveryOutcome::PreparationFailed(
            pe_execution_core::LiveOrderPreparationFailure::RecoveryAdmissionExpired,
        ) => "recovery_admission_expired",
        TerminalAdmissionRecoveryOutcome::PreparationFailed(_) => "rejected",
    }
}

fn terminal_projection_is_pending(
    state: &FanoutState,
    inventory: &LiveRecoveryInventory,
) -> Result<bool, FanoutError> {
    let terminal_targets = inventory
        .terminal_admissions
        .iter()
        .map(|terminal| {
            (
                terminal.identity.dispatch_id.as_str(),
                terminal.account_id.as_str(),
            )
        })
        .collect::<BTreeSet<_>>();
    if terminal_targets.is_empty() {
        return Ok(false);
    }
    let seeds = state
        .config
        .paper_state
        .unfinalized_ready_dispatch_seeds()?;
    Ok(nonterminal_dispatch_targets_for_seeds(state, &seeds)?
        .iter()
        .any(|target| {
            terminal_targets.contains(&(target.dispatch_id.as_str(), target.account_id.as_str()))
        }))
}

fn nonterminal_dispatch_targets_for_seeds(
    state: &FanoutState,
    seeds: &[DispatchSeedRow],
) -> Result<Vec<DispatchTargetRow>, FanoutError> {
    let mut targets = Vec::new();
    for seed in seeds {
        targets.extend(
            state
                .config
                .paper_state
                .dispatch_targets(&seed.dispatch_id)?
                .into_iter()
                .filter(|target| target.state != "terminal"),
        );
    }
    Ok(targets)
}

fn recovery_is_pending(state: &FanoutState) -> Result<bool, FanoutError> {
    // This is only an idle-work hint. The consuming pass verifies the journal and every admission
    // before using them, while even an unverifiable nonterminal admission must keep recovery pending.
    let inventory = pe_execution_core::live_journal::structural_recovery_inventory(
        &state.config.journal_path,
        state.config.era_live_prefix.as_ref(),
    )?;
    Ok(!inventory.approved_admissions.is_empty()
        || !inventory.open_orders.is_empty()
        || terminal_projection_is_pending(state, &inventory)?)
}

fn reconcile_terminal_recovery_work(
    state: &FanoutState,
    inventory: &LiveRecoveryInventory,
    now: OffsetDateTime,
) -> Result<(), FanoutError> {
    for terminal in &inventory.terminal_admissions {
        let targets = state
            .config
            .paper_state
            .dispatch_targets(&terminal.identity.dispatch_id)?;
        for target in targets.into_iter().filter(|target| {
            target.account_id == terminal.account_id.as_str() && target.state != "terminal"
        }) {
            terminalize(
                state,
                &target,
                terminal_recovery_reason(&terminal.outcome),
                now,
            )?;
        }
        state
            .config
            .paper_state
            .finalize_dispatch_if_terminal(&terminal.identity.dispatch_id, now.unix_timestamp())?;
    }
    Ok(())
}

fn due(last: Option<i64>, now: i64, interval: i64) -> bool {
    last.is_none_or(|last| now.saturating_sub(last) >= interval)
}

#[derive(Debug, thiserror::Error)]
enum FanoutError {
    #[error("paper-state: {0}")]
    Paper(#[from] pe_paper_state::PaperStateError),
    #[error("frozen signal is invalid: {0}")]
    Signal(String),
    #[error("live journal: {0}")]
    Journal(#[from] pe_execution_core::LiveJournalError),
    #[error("live executor: {0}")]
    Executor(#[from] pe_execution_core::LiveExecutorError),
}

/// One authenticated matched order awaiting Polygon finality.
#[derive(Debug, Clone)]
pub struct PendingOrderFinality {
    pub prepared_journal_seq: u64,
    pub prepared: Box<pe_execution_core::LiveOrderPreparedAudit>,
    pub transaction_hashes: Vec<String>,
    pub immutable_receipts: BTreeMap<String, MatchedReceipt>,
}

/// Exact result of one no-retry finality batch. Pending/conflict evidence is journaled through the
/// existing `OrderReconciled` record; a finalized value is appended before terminalization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderFinalityDisposition {
    Finalized(Box<OrderFillFinalizedAudit>),
    Pending {
        reason: String,
        evidence: Vec<RawHttpAttempt>,
    },
    Conflict {
        reason: String,
        evidence: Vec<RawHttpAttempt>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderFinalityResult {
    pub identity: LiveOrderIdentity,
    pub order_hash: String,
    pub disposition: OrderFinalityDisposition,
}

/// Post-append target action. Conflict keeps the reservation and freezes this target; finalized
/// is the only variant that permits terminalization, release, and projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalityJournalEffect {
    Pending,
    Conflict,
    Finalized,
}

struct CollectedPolygonFinality {
    chain_id: Result<u64, ReceiptError>,
    chain_attempt: RawHttpAttempt,
    head: Result<pe_venue_polymarket::FinalizedBlock, ReceiptError>,
    head_attempt: RawHttpAttempt,
    receipts: BTreeMap<String, Result<Option<MatchedReceipt>, ReceiptError>>,
    receipt_attempts: BTreeMap<String, RawHttpAttempt>,
    blocks: BTreeMap<u64, Result<pe_venue_polymarket::FinalizedBlock, ReceiptError>>,
    block_attempts: BTreeMap<u64, RawHttpAttempt>,
}

/// Execute one bounded recovery pass: one chain read, one read per distinct transaction, one
/// finalized-head read, and one canonical block read per distinct lower receipt height.
pub async fn collect_order_finality(
    rpc: &dyn PolygonReceiptReader,
    orders: Vec<PendingOrderFinality>,
) -> Vec<OrderFinalityResult> {
    if orders.is_empty() {
        return Vec::new();
    }

    let chain_attempt = rpc.chain_id().await;
    let chain_id = response_body(&chain_attempt)
        .map_err(|()| ReceiptError::MalformedRpc)
        .and_then(parse_chain_id_response);

    let hashes = orders
        .iter()
        .flat_map(|order| order.transaction_hashes.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut receipts = BTreeMap::new();
    let mut receipt_attempts = BTreeMap::new();
    for hash in hashes {
        let attempt = rpc.transaction_receipt(&hash).await;
        let parsed = response_body(&attempt)
            .map_err(|_| ReceiptError::MalformedRpc)
            .and_then(|body| parse_receipt_response(body, &hash));
        receipt_attempts.insert(hash.clone(), attempt);
        receipts.insert(hash, parsed);
    }

    let head_attempt = rpc.finalized_block().await;
    let head = response_body(&head_attempt)
        .map_err(|_| ReceiptError::MalformedRpc)
        .and_then(parse_finalized_block_response);
    let mut lower_heights = BTreeSet::new();
    if let Ok(head) = &head {
        for observation in receipts.values() {
            if let Ok(Some(receipt)) = observation
                && receipt.block_number < head.number
            {
                lower_heights.insert(receipt.block_number);
            }
        }
    }
    let mut blocks = BTreeMap::new();
    let mut block_attempts = BTreeMap::new();
    for height in lower_heights {
        let attempt = rpc.block_by_number(height).await;
        let expected_hash = receipts.values().find_map(|observation| match observation {
            Ok(Some(receipt)) if receipt.block_number == height => {
                Some(receipt.block_hash.as_str())
            }
            Ok(Some(_)) | Ok(None) | Err(_) => None,
        });
        let block = match (response_body(&attempt), expected_hash) {
            (Ok(body), Some(hash)) => canonical_block_matches(body, height, hash),
            (Ok(_), None) | (Err(_), _) => Err(ReceiptError::MalformedRpc),
        };
        block_attempts.insert(height, attempt);
        blocks.insert(height, block);
    }
    let collected = CollectedPolygonFinality {
        chain_id,
        chain_attempt,
        head,
        head_attempt,
        receipts,
        receipt_attempts,
        blocks,
        block_attempts,
    };

    orders
        .into_iter()
        .map(|order| order_finality_result(order, &collected))
        .collect()
}

fn order_finality_result(
    order: PendingOrderFinality,
    collected: &CollectedPolygonFinality,
) -> OrderFinalityResult {
    let identity = order.prepared.identity.clone();
    let order_hash = order.prepared.prepared.order_hash.clone();
    let relevant_heights = order
        .transaction_hashes
        .iter()
        .filter_map(|hash| collected.receipts.get(hash))
        .filter_map(|observation| match observation {
            Ok(Some(receipt))
                if receipt.block_number < collected.head.as_ref().map_or(0, |h| h.number) =>
            {
                Some(receipt.block_number)
            }
            Ok(Some(_)) | Ok(None) | Err(_) => None,
        })
        .collect::<BTreeSet<_>>();
    let receipt_evidence = std::iter::once(collected.chain_attempt.clone())
        .chain(
            order
                .transaction_hashes
                .iter()
                .filter_map(|hash| collected.receipt_attempts.get(hash).cloned()),
        )
        .collect::<Vec<_>>();
    let block_evidence = std::iter::once(collected.head_attempt.clone())
        .chain(
            relevant_heights
                .iter()
                .filter_map(|height| collected.block_attempts.get(height).cloned()),
        )
        .collect::<Vec<_>>();
    let reconciliation_evidence = || {
        receipt_evidence
            .iter()
            .chain(&block_evidence)
            .cloned()
            .collect()
    };
    let disposition = match classify_polygon_finality(
        &order.prepared,
        &order.transaction_hashes,
        &collected.chain_id,
        &collected.head,
        &collected.receipts,
        &collected.blocks,
        &order.immutable_receipts,
    ) {
        PolygonFinalityClassification::Pending { reason } => OrderFinalityDisposition::Pending {
            reason,
            evidence: reconciliation_evidence(),
        },
        PolygonFinalityClassification::Conflict { reason } => OrderFinalityDisposition::Conflict {
            reason,
            evidence: reconciliation_evidence(),
        },
        PolygonFinalityClassification::Finalized {
            principal,
            quantity,
            fee,
            matched_logs,
            chain_id,
            finalized_head,
        } => OrderFinalityDisposition::Finalized(Box::new(OrderFillFinalizedAudit {
            identity: identity.clone(),
            prepared_journal_seq: order.prepared_journal_seq,
            principal,
            quantity,
            fee,
            matched_logs,
            chain_id,
            finalized_head,
            receipts: receipt_evidence,
            blocks: block_evidence,
        })),
    };
    OrderFinalityResult {
        identity,
        order_hash,
        disposition,
    }
}

fn response_body(attempt: &RawHttpAttempt) -> Result<&[u8], ()> {
    match attempt {
        RawHttpAttempt::Response(response) if (200..300).contains(&response.status) => {
            Ok(&response.body)
        }
        RawHttpAttempt::Response(_) | RawHttpAttempt::TransportFailure(_) => Err(()),
    }
}

/// Synchronize one finality result without changing the SQLite target. The caller may terminalize
/// and project only after this returns [`FinalityJournalEffect::Finalized`].
pub fn append_order_finality_result(
    journal: &LiveJournal,
    account_id: AccountId,
    now: OffsetDateTime,
    result: OrderFinalityResult,
) -> Result<FinalityJournalEffect, pe_execution_core::LiveJournalError> {
    let (payload, effect) = match result.disposition {
        OrderFinalityDisposition::Finalized(finalized) => (
            LiveJournalPayload::OrderFillFinalized(finalized),
            FinalityJournalEffect::Finalized,
        ),
        OrderFinalityDisposition::Pending { reason, evidence } => {
            let evidence_hashes = http_attempt_hashes(&evidence)?;
            (
                LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                    identity: result.identity,
                    order_hash: result.order_hash,
                    source: LiveReconciliationSource::PolygonFinality,
                    outcome: LiveJournalOrderOutcome::FinalityPending { reason },
                    evidence,
                    evidence_hashes,
                })),
                FinalityJournalEffect::Pending,
            )
        }
        OrderFinalityDisposition::Conflict { reason, evidence } => {
            let evidence_hashes = http_attempt_hashes(&evidence)?;
            (
                LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                    identity: result.identity,
                    order_hash: result.order_hash,
                    source: LiveReconciliationSource::PolygonFinality,
                    outcome: LiveJournalOrderOutcome::FinalityConflict { reason },
                    evidence,
                    evidence_hashes,
                })),
                FinalityJournalEffect::Conflict,
            )
        }
    };
    journal.append(account_id, now, payload)?;
    Ok(effect)
}

#[derive(Debug, PartialEq, Eq)]
enum PassControl {
    Continue,
    StopSeed,
    FreezePass,
}

/// Pre-network classification of one dispatch target (#514): decided from the target's
/// durable state and the accounts snapshot BEFORE any credential, book, or order work, so
/// a paused target provably performs none of it.
#[derive(Debug, PartialEq, Eq)]
enum TargetClass {
    /// `submitted`/`ambiguous`: reconcile the in-flight order. Never terminalized by the
    /// snapshot, freshness, arming, closure, or credential-CAS gates — the order may
    /// exist on the venue, so only venue reconciliation may decide its outcome.
    RecoverInFlight,
    /// `pending` under a stale/never-successful accounts snapshot: pause (no submission,
    /// no terminalization) until the account evidence is fresh again.
    PauseStale,
    /// `pending` under a FRESH snapshot whose account is missing or unarmed: terminal.
    NotArmed,
    /// `pending`, fresh and armed, but the account's admission is closed: pause.
    PauseClosed,
    /// `pending`, fresh, armed, open: proceed to ordinary dispatch.
    Dispatch,
}

fn classify_target(
    target_state: &str,
    snapshot_is_fresh: bool,
    armed_account_present: bool,
    admission_closed: bool,
) -> TargetClass {
    if target_state == "submitted" || target_state == "ambiguous" {
        return TargetClass::RecoverInFlight;
    }
    if !snapshot_is_fresh {
        return TargetClass::PauseStale;
    }
    if !armed_account_present {
        return TargetClass::NotArmed;
    }
    if admission_closed {
        return TargetClass::PauseClosed;
    }
    TargetClass::Dispatch
}

async fn run_dispatch_pass(state: &mut FanoutState) -> Result<(), FanoutError> {
    let seeds = state
        .config
        .paper_state
        .unfinalized_ready_dispatch_seeds()?;
    // Ships dark: with no staged seed this tick performs no account, credential, venue, or
    // projection work. The account snapshot may be empty without affecting the paper path.
    if seeds.is_empty() {
        return Ok(());
    }
    for seed in seeds {
        let frozen = parse_frozen_signal(&seed)?;
        let targets = state
            .config
            .paper_state
            .dispatch_targets(&seed.dispatch_id)?;
        for target in targets {
            if target.state == "terminal" {
                continue;
            }
            let control = process_target(
                state,
                &seed,
                &target,
                &frozen.signal,
                &frozen.observation,
                OffsetDateTime::now_utc(),
            )
            .await?;
            match control {
                PassControl::Continue => {}
                // A transient refusal cannot be overtaken by a younger seed. Resume this exact
                // oldest target on the next tick.
                PassControl::StopSeed => return Ok(()),
                PassControl::FreezePass => return Ok(()),
            }
        }
        state.config.paper_state.finalize_dispatch_if_terminal(
            &seed.dispatch_id,
            OffsetDateTime::now_utc().unix_timestamp(),
        )?;
    }
    Ok(())
}

/// Recover ordinary-live work. Runtime passes may resume a currently authorized Approved
/// admission; shutdown passes reconcile only already-Prepared work and never create a POST.
enum RecoveryAuthorization {
    Ready {
        account: AccountContext,
        mode: LiveModeSnapshot,
        binding: CredentialBindingIdentity,
    },
    Pause,
    Terminal {
        failure: pe_execution_core::LiveOrderPreparationFailure,
        target_reason: &'static str,
    },
}

enum RecoveryVenueGate {
    Ready(RecoveryExecutionState),
    Control(RecoveryAuthorization),
    MarkUnavailable,
}

struct RecoveryExecutionState {
    mode: LiveModeSnapshot,
    binding: CredentialBindingIdentity,
    account_state: pe_execution_core::LiveVenueAccountState,
}

struct RecoveryVenueContext<'a, V> {
    venue: &'a V,
    account_binding: &'a LiveAccountBindingAudit,
    custody_wallet: &'a str,
}

fn recovery_authorization(
    state: &FanoutState,
    account_id: &AccountId,
    frozen_binding: &CredentialBindingIdentity,
    now: OffsetDateTime,
) -> Result<RecoveryAuthorization, FanoutError> {
    if state.config.shutdown.phase() >= ShutdownPhase::StopProducers {
        return Ok(RecoveryAuthorization::Pause);
    }
    let snapshot = state.config.live_accounts.snapshot();
    if !snapshot.is_fresh(now.unix_timestamp()) {
        return Ok(RecoveryAuthorization::Pause);
    }
    let Some(account) = snapshot
        .accounts
        .iter()
        .find(|account| account.account_id == *account_id)
        .cloned()
    else {
        return Ok(RecoveryAuthorization::Terminal {
            failure: pe_execution_core::LiveOrderPreparationFailure::RecoveryAdmissionExpired,
            target_reason: "recovery_account_not_armed",
        });
    };
    if account.requested_live_mode != "live_tiny"
        || account.effective_live_mode != "live_tiny"
        || !account.is_armed()
    {
        return Ok(RecoveryAuthorization::Terminal {
            failure: pe_execution_core::LiveOrderPreparationFailure::RecoveryAdmissionExpired,
            target_reason: "recovery_account_not_armed",
        });
    }
    let Some((binding_version, binding_key_id)) = account.credential_binding.as_ref() else {
        return Ok(RecoveryAuthorization::Terminal {
            failure: pe_execution_core::LiveOrderPreparationFailure::RecoveryAdmissionExpired,
            target_reason: "recovery_account_not_armed",
        });
    };
    let binding = CredentialBindingIdentity {
        version: *binding_version,
        key_id: binding_key_id.clone(),
    };
    if binding != *frozen_binding {
        return Ok(RecoveryAuthorization::Terminal {
            failure: pe_execution_core::LiveOrderPreparationFailure::RecoveryCredentialChanged,
            target_reason: "recovery_credential_changed",
        });
    }
    let redemption_closed =
        reconstruct_redemption_attempts(&replay_live_account(state, account_id)?)
            .values()
            .any(|attempt| redemption_posture(&attempt.state).closes_new_buy_admission);
    if state.closures.reason(account_id.as_str()).is_some()
        || redemption_closed
        || global_risk_halt_active(state)?
    {
        return Ok(RecoveryAuthorization::Pause);
    }
    Ok(RecoveryAuthorization::Ready {
        mode: LiveModeSnapshot {
            requested: mode_value(&account.requested_live_mode),
            effective: mode_value(&account.effective_live_mode),
        },
        account,
        binding,
    })
}

async fn fresh_recovery_venue_gate<V, C>(
    state: &mut FanoutState,
    account_id: &AccountId,
    frozen_binding: &CredentialBindingIdentity,
    neg_risk: bool,
    venue_context: &RecoveryVenueContext<'_, V>,
    clock: C,
) -> Result<RecoveryVenueGate, FanoutError>
where
    V: LiveOrderVenue,
    C: Fn() -> OffsetDateTime + Copy + Send + Sync,
{
    let account_state = match venue_context
        .venue
        .read_balance_and_allowance(neg_risk)
        .await
    {
        Ok(account_state) => account_state,
        Err(_) => {
            return match recovery_authorization(state, account_id, frozen_binding, clock())? {
                RecoveryAuthorization::Ready { .. } => Ok(RecoveryVenueGate::MarkUnavailable),
                control => Ok(RecoveryVenueGate::Control(control)),
            };
        }
    };
    let reconciliation_at = clock();
    let authorization =
        recovery_authorization(state, account_id, frozen_binding, reconciliation_at)?;
    let RecoveryAuthorization::Ready { account, .. } = &authorization else {
        return Ok(RecoveryVenueGate::Control(authorization));
    };
    if ensure_live_portfolio_marks_at(
        state,
        account,
        &account_state,
        venue_context.account_binding,
        venue_context.custody_wallet,
        reconciliation_at,
    )
    .await
        != CheckOutcome::Pass
    {
        return match recovery_authorization(state, account_id, frozen_binding, clock())? {
            RecoveryAuthorization::Ready { .. } => Ok(RecoveryVenueGate::MarkUnavailable),
            control => Ok(RecoveryVenueGate::Control(control)),
        };
    }
    match recovery_authorization(state, account_id, frozen_binding, clock())? {
        RecoveryAuthorization::Ready { mode, binding, .. } => {
            Ok(RecoveryVenueGate::Ready(RecoveryExecutionState {
                mode,
                binding,
                account_state,
            }))
        }
        control => Ok(RecoveryVenueGate::Control(control)),
    }
}

fn terminalize_unprepared_recovery(
    state: &FanoutState,
    target: &DispatchTargetRow,
    identity: LiveOrderIdentity,
    failure: pe_execution_core::LiveOrderPreparationFailure,
    target_reason: &'static str,
    now: OffsetDateTime,
) -> Result<(), FanoutError> {
    state.config.journal.append(
        AccountId::new(&target.account_id)
            .map_err(|error| FanoutError::Signal(error.to_string()))?,
        now,
        LiveJournalPayload::OrderPreparationFailed(Box::new(
            pe_execution_core::LiveOrderPreparationFailedAudit { identity, failure },
        )),
    )?;
    terminalize(state, target, target_reason, now)
}

async fn run_recovery_pass<C>(
    state: &mut FanoutState,
    clock: C,
    resume_approved_admissions: bool,
) -> Result<(), FanoutError>
where
    C: Fn() -> OffsetDateTime + Copy + Send + Sync,
{
    let pass_started_at = clock();
    let mut freeze = false;
    let inventory = verified_recovery_inventory(state)?;
    reconcile_terminal_recovery_work(state, &inventory, clock())?;
    let LiveRecoveryInventory {
        approved_admissions,
        open_orders,
        ..
    } = inventory;
    let (finality_orders, recoverable_orders): (Vec<_>, Vec<_>) = open_orders
        .into_iter()
        .partition(|order| !order.transaction_hashes.is_empty());
    for pending in approved_admissions
        .into_iter()
        .filter(|_| resume_approved_admissions)
    {
        let recovery_check_at = clock();
        let target = DispatchTargetRow {
            dispatch_id: pending.admission.identity.dispatch_id.clone(),
            account_id: pending.account_id.as_str().to_owned(),
            exec_rank: 0,
            credential_bundle_version: pending.admission.frozen_binding.version,
            credential_key_id: pending.admission.frozen_binding.key_id.clone(),
            state: "pending".to_owned(),
            terminal_reason: None,
            updated_at_unix: pass_started_at.unix_timestamp(),
        };
        match recovery_authorization(
            state,
            &pending.account_id,
            &pending.admission.frozen_binding,
            recovery_check_at,
        )? {
            RecoveryAuthorization::Ready { .. } => {}
            RecoveryAuthorization::Pause => {
                freeze = true;
                continue;
            }
            RecoveryAuthorization::Terminal {
                failure,
                target_reason,
            } => {
                terminalize_unprepared_recovery(
                    state,
                    &target,
                    pending.admission.identity.clone(),
                    failure,
                    target_reason,
                    recovery_check_at,
                )?;
                continue;
            }
        }
        let preparation_check_at = clock();
        let recovery_price_expired = risk_price_expired(
            state,
            &pending.admission.economic.risk,
            preparation_check_at,
        )?;
        let recovery_risk_failure =
            if risk_day_changed(&pending.admission.economic.risk, preparation_check_at) {
                Some(pe_execution_core::LiveOrderPreparationFailure::PrePostRiskDayChanged)
            } else if recovery_price_expired {
                Some(pe_execution_core::LiveOrderPreparationFailure::PrePostRiskPriceExpired)
            } else {
                None
            };
        if let Some(failure) = recovery_risk_failure {
            state.config.journal.append(
                pending.account_id.clone(),
                preparation_check_at,
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    pe_execution_core::LiveOrderPreparationFailedAudit {
                        identity: pending.admission.identity.clone(),
                        failure,
                    },
                )),
            )?;
            state.config.paper_state.set_dispatch_target_state(
                &target.dispatch_id,
                &target.account_id,
                "terminal",
                Some("recovery_risk_evidence_expired"),
                preparation_check_at.unix_timestamp(),
            )?;
            continue;
        }
        let credentials = match credentials_for_target(state, &target).await {
            CredentialLoad::Ready(credentials) => credentials,
            CredentialLoad::Changed => {
                let failed_at = clock();
                state.config.journal.append(
                    pending.account_id.clone(),
                    failed_at,
                    LiveJournalPayload::OrderPreparationFailed(Box::new(
                        pe_execution_core::LiveOrderPreparationFailedAudit {
                            identity: pending.admission.identity.clone(),
                            failure:
                                pe_execution_core::LiveOrderPreparationFailure::RecoveryCredentialChanged,
                        },
                    )),
                )?;
                state.config.paper_state.set_dispatch_target_state(
                    &target.dispatch_id,
                    &target.account_id,
                    "terminal",
                    Some("recovery_credential_changed"),
                    failed_at.unix_timestamp(),
                )?;
                continue;
            }
            CredentialLoad::Transient(_) => {
                freeze = true;
                continue;
            }
        };
        match recovery_authorization(
            state,
            &pending.account_id,
            &pending.admission.frozen_binding,
            clock(),
        )? {
            RecoveryAuthorization::Ready { .. } => {}
            RecoveryAuthorization::Pause => {
                freeze = true;
                continue;
            }
            RecoveryAuthorization::Terminal {
                failure,
                target_reason,
            } => {
                terminalize_unprepared_recovery(
                    state,
                    &target,
                    pending.admission.identity.clone(),
                    failure,
                    target_reason,
                    clock(),
                )?;
                continue;
            }
        }
        let venue = match PolymarketLiveVenue::from_credentials(&credentials).await {
            Ok(venue) => venue,
            Err(_) => {
                freeze = true;
                continue;
            }
        };
        match recovery_authorization(
            state,
            &pending.account_id,
            &pending.admission.frozen_binding,
            clock(),
        )? {
            RecoveryAuthorization::Ready { .. } => {}
            RecoveryAuthorization::Pause => {
                freeze = true;
                continue;
            }
            RecoveryAuthorization::Terminal {
                failure,
                target_reason,
            } => {
                terminalize_unprepared_recovery(
                    state,
                    &target,
                    pending.admission.identity.clone(),
                    failure,
                    target_reason,
                    clock(),
                )?;
                continue;
            }
        }
        let custody_wallet = venue.deposit_wallet();
        let venue_context = RecoveryVenueContext {
            venue: &venue,
            account_binding: venue.account_binding(),
            custody_wallet: &custody_wallet,
        };
        let execution = match fresh_recovery_venue_gate(
            state,
            &pending.account_id,
            &pending.admission.frozen_binding,
            pending.admission.economic.admission.market.neg_risk,
            &venue_context,
            clock,
        )
        .await?
        {
            RecoveryVenueGate::Ready(execution) => execution,
            RecoveryVenueGate::Control(RecoveryAuthorization::Terminal {
                failure,
                target_reason,
            }) => {
                terminalize_unprepared_recovery(
                    state,
                    &target,
                    pending.admission.identity.clone(),
                    failure,
                    target_reason,
                    clock(),
                )?;
                continue;
            }
            RecoveryVenueGate::Control(_) | RecoveryVenueGate::MarkUnavailable => {
                freeze = true;
                continue;
            }
        };
        if resume_approved_with_venue(state, &target, pending, execution, &venue_context, clock)
            .await?
        {
            freeze = true;
        }
    }
    let snapshot = state.config.live_accounts.snapshot();
    for order in recoverable_orders.into_iter() {
        let Some(prepared) = order.prepared else {
            freeze = true;
            continue;
        };
        let target = DispatchTargetRow {
            dispatch_id: order.inventory.identity.dispatch_id,
            account_id: order.inventory.account_id.as_str().to_owned(),
            exec_rank: 0,
            credential_bundle_version: prepared.frozen_binding.version,
            credential_key_id: prepared.frozen_binding.key_id.clone(),
            state: "submitted".to_owned(),
            terminal_reason: None,
            updated_at_unix: pass_started_at.unix_timestamp(),
        };
        let context = snapshot
            .accounts
            .iter()
            .find(|account| account.account_id == order.inventory.account_id);
        if recover_in_flight_target(state, &target, context, clock()).await?
            == PassControl::FreezePass
        {
            freeze = true;
        }
    }
    let seeds = state
        .config
        .paper_state
        .unfinalized_ready_dispatch_seeds()?;

    // One Polygon batch shares chain identity, finalized head, receipts, and canonical blocks
    // across every authenticated target relevant to this recovery pass.
    let mut finality_targets = Vec::new();
    let mut pending = Vec::new();
    let nonterminal_targets = nonterminal_dispatch_targets_for_seeds(state, &seeds)?;
    for order in finality_orders {
        let Some(prepared) = order.prepared else {
            freeze = true;
            continue;
        };
        if order.transaction_hashes.is_empty() {
            continue;
        }
        let target = nonterminal_targets
            .iter()
            .find(|target| {
                target.account_id == order.inventory.account_id.as_str()
                    && target.dispatch_id == order.inventory.identity.dispatch_id
            })
            .cloned();
        pending.push(PendingOrderFinality {
            prepared_journal_seq: order.inventory.prepared_journal_seq,
            prepared,
            transaction_hashes: order.transaction_hashes,
            immutable_receipts: order.immutable_receipts,
        });
        finality_targets.push((order.inventory.account_id, target));
    }
    let results = collect_order_finality(&state.polygon_receipt_rpc, pending).await;
    for ((account_id, target), result) in finality_targets.into_iter().zip(results) {
        let result_at = clock();
        match append_order_finality_result(
            state.config.journal.as_ref(),
            account_id.clone(),
            result_at,
            result,
        )? {
            FinalityJournalEffect::Pending => {
                if let Some(target) = &target {
                    state.config.paper_state.set_dispatch_target_state(
                        &target.dispatch_id,
                        &target.account_id,
                        "submitted",
                        None,
                        result_at.unix_timestamp(),
                    )?;
                }
            }
            FinalityJournalEffect::Conflict => {
                if let Some(target) = &target {
                    state.config.paper_state.set_dispatch_target_state(
                        &target.dispatch_id,
                        &target.account_id,
                        "ambiguous",
                        Some("polygon_finality_conflict"),
                        result_at.unix_timestamp(),
                    )?;
                }
                freeze = true;
            }
            FinalityJournalEffect::Finalized => {
                if let Some(target) = &target {
                    terminalize(state, target, "filled", result_at)?;
                }
                let snapshot = state.config.live_accounts.snapshot();
                if let Some(account) = snapshot
                    .accounts
                    .iter()
                    .find(|account| account.account_id == account_id)
                {
                    reconcile_account_projection(state, &account.account_id, result_at).await;
                }
            }
        }
    }
    for seed in &seeds {
        state
            .config
            .paper_state
            .finalize_dispatch_if_terminal(&seed.dispatch_id, clock().unix_timestamp())?;
    }
    if freeze {
        warn!("one or more live targets remain frozen after recovery");
    }
    Ok(())
}

async fn resume_approved_with_venue<V, C>(
    state: &mut FanoutState,
    target: &DispatchTargetRow,
    pending: pe_execution_core::live_journal::ApprovedAdmissionRecoveryEntry,
    execution: RecoveryExecutionState,
    venue_context: &RecoveryVenueContext<'_, V>,
    clock: C,
) -> Result<bool, FanoutError>
where
    V: LiveOrderVenue,
    C: Fn() -> OffsetDateTime + Copy + Send + Sync,
{
    let journal = state.config.journal.clone();
    let executor = LiveExecutor::new(venue_context.venue, journal.as_ref());
    let preparation_check_at = clock();
    if economic_book_receipt_is_stale(state, &pending.admission.economic, preparation_check_at)? {
        terminalize_unprepared_recovery(
            state,
            target,
            pending.admission.identity.clone(),
            pe_execution_core::LiveOrderPreparationFailure::PrePostLadderExpired,
            "recovery_ladder_expired",
            preparation_check_at,
        )?;
        return Ok(false);
    }
    match executor
        .resume_approved_admission_with_clock(
            pending.account_id,
            pending.admission,
            execution.mode,
            execution.binding,
            execution.account_state,
            clock,
        )
        .await?
    {
        LivePrepareResult::Terminal(outcome) => {
            persist_outcome(state, target, &outcome, clock())?;
            Ok(false)
        }
        LivePrepareResult::Prepared(prepared) => {
            let authorization_check_at = clock();
            let initial_authorization = recovery_authorization(
                state,
                prepared.account_id(),
                &prepared.audit().frozen_binding,
                authorization_check_at,
            )?;
            if !matches!(initial_authorization, RecoveryAuthorization::Ready { .. }) {
                let failure = match initial_authorization {
                    RecoveryAuthorization::Terminal { failure, .. } => failure,
                    RecoveryAuthorization::Pause | RecoveryAuthorization::Ready { .. } => {
                        pe_execution_core::LiveOrderPreparationFailure::PrePostAdmissionExpired
                    }
                };
                let outcome =
                    executor.terminalize_prepared(prepared, authorization_check_at, failure)?;
                persist_outcome(state, target, &outcome, authorization_check_at)?;
                return Ok(false);
            }
            let execution = match fresh_recovery_venue_gate(
                state,
                prepared.account_id(),
                &prepared.audit().frozen_binding,
                prepared.audit().economic.admission.market.neg_risk,
                venue_context,
                clock,
            )
            .await?
            {
                RecoveryVenueGate::Ready(execution) => execution,
                RecoveryVenueGate::Control(control) => {
                    let failure = match control {
                        RecoveryAuthorization::Terminal { failure, .. } => failure,
                        RecoveryAuthorization::Pause | RecoveryAuthorization::Ready { .. } => {
                            pe_execution_core::LiveOrderPreparationFailure::PrePostAdmissionExpired
                        }
                    };
                    let failed_at = clock();
                    let outcome = executor.terminalize_prepared(prepared, failed_at, failure)?;
                    persist_outcome(state, target, &outcome, failed_at)?;
                    return Ok(false);
                }
                RecoveryVenueGate::MarkUnavailable => {
                    let failed_at = clock();
                    let outcome = executor.terminalize_prepared(
                        prepared,
                        failed_at,
                        pe_execution_core::LiveOrderPreparationFailure::PrePostMarkUnavailable,
                    )?;
                    persist_outcome(state, target, &outcome, failed_at)?;
                    return Ok(false);
                }
            };
            let post_check_at = clock();
            let risk_price_expired =
                prepared_risk_price_expired(state, prepared.audit(), post_check_at)?;
            let failure = if prepared_risk_day_changed(prepared.audit(), post_check_at) {
                Some(pe_execution_core::LiveOrderPreparationFailure::PrePostRiskDayChanged)
            } else if risk_price_expired {
                Some(pe_execution_core::LiveOrderPreparationFailure::PrePostRiskPriceExpired)
            } else if !prepared_admission_is_approved_at(prepared.audit(), post_check_at)
                || !prepared_admission_is_approved_with_fresh_account_at(
                    prepared.audit(),
                    &execution,
                    post_check_at,
                )?
            {
                Some(pe_execution_core::LiveOrderPreparationFailure::PrePostAdmissionExpired)
            } else {
                None
            };
            if let Some(failure) = failure {
                let outcome = executor.terminalize_prepared(prepared, post_check_at, failure)?;
                persist_outcome(state, target, &outcome, post_check_at)?;
                return Ok(false);
            }
            let final_authorization = recovery_authorization(
                state,
                prepared.account_id(),
                &prepared.audit().frozen_binding,
                clock(),
            )?;
            if !matches!(final_authorization, RecoveryAuthorization::Ready { .. }) {
                let failure = match final_authorization {
                    RecoveryAuthorization::Terminal { failure, .. } => failure,
                    RecoveryAuthorization::Pause | RecoveryAuthorization::Ready { .. } => {
                        pe_execution_core::LiveOrderPreparationFailure::PrePostAdmissionExpired
                    }
                };
                let failed_at = clock();
                let outcome = executor.terminalize_prepared(prepared, failed_at, failure)?;
                persist_outcome(state, target, &outcome, failed_at)?;
                return Ok(false);
            }
            let final_post_check_at = clock();
            if prepared_ladder_is_stale(state, prepared.audit(), final_post_check_at)? {
                let outcome = executor.terminalize_prepared(
                    prepared,
                    final_post_check_at,
                    pe_execution_core::LiveOrderPreparationFailure::PrePostLadderExpired,
                )?;
                persist_outcome(state, target, &outcome, final_post_check_at)?;
                return Ok(false);
            }
            state.config.paper_state.set_dispatch_target_state(
                &target.dispatch_id,
                &target.account_id,
                "submitted",
                None,
                post_check_at.unix_timestamp(),
            )?;
            let outcome = executor.submit_with_clock(prepared, clock).await?;
            let transition = outcome_transition(&outcome);
            persist_outcome(state, target, &outcome, clock())?;
            Ok(transition.freeze)
        }
    }
}

#[derive(Deserialize)]
struct FrozenSignal {
    schema_version: u16,
    signal: LeaderSignal,
    observation: ObservationEvidence,
}

fn parse_frozen_signal(seed: &DispatchSeedRow) -> Result<FrozenSignal, FanoutError> {
    let value: serde_json::Value = serde_json::from_str(&seed.signal_json)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    if value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        == Some(1)
        && value
            .get("observation")
            .is_none_or(serde_json::Value::is_null)
    {
        return Err(FanoutError::Signal(
            "schema-one live dispatch seed lacks observation evidence; activation requires no open live work"
                .to_owned(),
        ));
    }
    let frozen: FrozenSignal =
        serde_json::from_value(value).map_err(|error| FanoutError::Signal(error.to_string()))?;
    if frozen.schema_version != 1 {
        return Err(FanoutError::Signal("unsupported schema version".to_owned()));
    }
    Ok(frozen)
}

async fn process_target(
    state: &mut FanoutState,
    seed: &DispatchSeedRow,
    target: &DispatchTargetRow,
    signal: &LeaderSignal,
    observation: &ObservationEvidence,
    now: OffsetDateTime,
) -> Result<PassControl, FanoutError> {
    let accounts = state.config.live_accounts.snapshot();
    let account = accounts
        .accounts
        .iter()
        .find(|account| account.account_id.as_str() == target.account_id);
    let fresh = accounts.is_fresh(now.unix_timestamp());
    let closure_reason = state.closures.reason(&target.account_id);
    let class = classify_target(
        &target.state,
        fresh,
        account.is_some_and(AccountContext::is_armed),
        closure_reason.is_some(),
    );
    match class {
        TargetClass::RecoverInFlight => {
            // With no fresh account context the pass reconciles the order state only.
            let context = if fresh { account } else { None };
            return recover_in_flight_target(state, target, context, now).await;
        }
        TargetClass::PauseStale => {
            warn!(account_id = %target.account_id, "live accounts snapshot is stale; pending target paused");
            return Ok(PassControl::StopSeed);
        }
        TargetClass::NotArmed => {
            terminalize(state, target, "not_armed", OffsetDateTime::now_utc())?;
            return Ok(PassControl::Continue);
        }
        TargetClass::PauseClosed => {
            let reason = closure_reason.unwrap_or_default();
            info!(account_id = %target.account_id, reason, "live target remains pending while account admission is closed");
            return Ok(PassControl::StopSeed);
        }
        TargetClass::Dispatch => {}
    }
    let Some(account) = account else {
        // Unreachable by classification (Dispatch requires an armed account); fail closed
        // without submitting.
        return Ok(PassControl::StopSeed);
    };

    let credentials = match credentials_for_target(state, target).await {
        CredentialLoad::Ready(credentials) => credentials,
        CredentialLoad::Changed => {
            terminalize(
                state,
                target,
                "credential_version_changed",
                OffsetDateTime::now_utc(),
            )?;
            return Ok(PassControl::Continue);
        }
        CredentialLoad::Transient(reason) => {
            warn!(account_id = %target.account_id, reason, "live credential decrypt refused transiently; seed paused");
            return Ok(PassControl::StopSeed);
        }
    };
    let venue = match PolymarketLiveVenue::from_credentials(&credentials).await {
        Ok(venue) => venue,
        Err(error) => {
            warn!(account_id = %target.account_id, error = %error, "live V2 client construction failed; seed paused");
            return Ok(PassControl::StopSeed);
        }
    };

    let condition_id = PolymarketConditionId(signal.market_id.0.0.clone());
    let admission_started_at = OffsetDateTime::now_utc();
    let admission = match state
        .admission
        .build(&condition_id, admission_started_at)
        .await
    {
        Ok(admission) => admission,
        Err(error) if admission_error_is_transient(&error) => {
            warn!(account_id = %target.account_id, error = %error, "live market evidence unavailable; seed paused");
            return Ok(PassControl::StopSeed);
        }
        Err(error) => {
            terminalize(
                state,
                target,
                admission_terminal_reason(&error),
                OffsetDateTime::now_utc(),
            )?;
            return Ok(PassControl::Continue);
        }
    };
    let token_id = match admission
        .market
        .ordered_outcome_token_ids
        .get(usize::from(signal.outcome_id.0))
        .cloned()
    {
        Some(token) => token,
        None => {
            terminalize(
                state,
                target,
                "outcome_not_binary",
                OffsetDateTime::now_utc(),
            )?;
            return Ok(PassControl::Continue);
        }
    };

    let account_state = match venue
        .read_balance_and_allowance(admission.market.neg_risk)
        .await
    {
        Ok(account_state) => account_state,
        Err(_) => return Ok(PassControl::StopSeed),
    };
    if live_financial_posture(state, account, account_state.collateral_balance, false)
        != CheckOutcome::Pass
    {
        return Ok(PassControl::StopSeed);
    }
    let book = match state
        .config
        .book_fetcher
        .fetch_book(&condition_id.0, &token_id.0)
        .await
    {
        Ok(book) => book,
        Err(error) => {
            warn!(account_id = %target.account_id, error = %error, "fresh per-account ladder unavailable; seed paused");
            return Ok(PassControl::StopSeed);
        }
    };
    let Some(book_receipt) = book.source_receipt else {
        warn!(account_id = %target.account_id, "recorded /book receipt missing; seed paused");
        return Ok(PassControl::StopSeed);
    };
    let now_ms = unix_ms(OffsetDateTime::now_utc());
    if pe_venue_polymarket::ladder_is_stale(now_ms, book.fetched_at_ms) {
        return Ok(PassControl::StopSeed);
    }
    let Some(ladder) = book.ladder() else {
        terminalize(state, target, "ladder_invalid", OffsetDateTime::now_utc())?;
        return Ok(PassControl::Continue);
    };
    let Some(best_ask) = ladder.first().map(|ask| ask.price) else {
        terminalize(state, target, "ladder_empty", OffsetDateTime::now_utc())?;
        return Ok(PassControl::Continue);
    };
    let settings = match fetch_account_settings(state, &target.account_id).await {
        Ok(settings) => settings,
        Err(reason) => {
            warn!(account_id = %target.account_id, reason, "live sizing settings unavailable; seed paused");
            return Ok(PassControl::StopSeed);
        }
    };
    let runtime = state.config.runtime_config.snapshot();
    let sizing = match settings.sizing_mode(runtime.sizing_mode) {
        Ok(sizing) => sizing,
        Err(reason) => {
            terminalize(state, target, reason, OffsetDateTime::now_utc())?;
            return Ok(PassControl::Continue);
        }
    };
    let mut strategy_config = runtime.winner_follow_config();
    strategy_config.sizing_mode = sizing;
    let strategy = WinnerFollowStrategy::new(strategy_config.clone());
    let probability = probability_for(&state.config.live_watchlist, signal);
    let per_trade_cap_bps = strategy_config
        .per_trade_cap
        .resolve_bps(TradingMode::LiveTiny);
    if ensure_live_portfolio_marks(
        state,
        account,
        &account_state,
        venue.account_binding(),
        &venue.deposit_wallet(),
    )
    .await
        != CheckOutcome::Pass
    {
        warn!(account_id = %target.account_id, "pre-risk inventory reconciliation failed; seed paused");
        return Ok(PassControl::StopSeed);
    }
    let initial_risk = match live_risk_audit(
        state,
        account,
        Some(signal),
        CollateralAmount::ZERO,
        per_trade_cap_bps,
    )
    .await
    {
        Ok(risk) => risk,
        Err(reason) => {
            let decline = WinnerFollowError::RiskInputsUnavailable;
            let report = format!("{decline}: {reason}");
            warn!(account_id = %target.account_id, decline = %report, "live risk inputs unavailable; seed paused");
            return Ok(PassControl::StopSeed);
        }
    };
    if sync_live_risk_halts(state, account, &initial_risk).await? {
        return Ok(PassControl::StopSeed);
    }
    if let Err(error) = evaluate_live_candidate_at_price(
        &strategy,
        signal,
        best_ask,
        probability,
        initial_risk.snapshot.clone(),
        account_state.reconciled_free_collateral.to_decimal(),
    ) {
        terminalize(
            state,
            target,
            winner_follow_terminal_reason(&error),
            OffsetDateTime::now_utc(),
        )?;
        return Ok(PassControl::Continue);
    }
    let minimum_price = parse_band_price(runtime.min_fill_price, Price::ZERO);
    let maximum_price = parse_band_price(runtime.max_fill_price, Price(Decimal::ONE));
    let (minimum_price, maximum_price) = match (minimum_price, maximum_price) {
        (Ok(minimum), Ok(maximum)) => (minimum, maximum),
        _ => {
            terminalize(
                state,
                target,
                "live_price_band_invalid",
                OffsetDateTime::now_utc(),
            )?;
            return Ok(PassControl::Continue);
        }
    };
    let cap_bps = u32::try_from(account.live_price_impact_cap_bps)
        .ok()
        .filter(|cap| (1..=10_000).contains(cap));
    let Some(cap_bps) = cap_bps else {
        terminalize(
            state,
            target,
            "live_price_impact_cap_invalid",
            OffsetDateTime::now_utc(),
        )?;
        return Ok(PassControl::Continue);
    };
    let price_impact_cap_bps = i32::try_from(cap_bps)
        .map_err(|_| FanoutError::Signal("live price-impact cap exceeds i32".to_owned()))?;
    let ceiling_raw = (best_ask.0
        * (Decimal::from(10_000u32 + cap_bps) / Decimal::from(10_000u32)))
    .min(Decimal::ONE);
    let ceiling = match Price::new(ceiling_raw) {
        Ok(price) => price,
        Err(_) => {
            terminalize(
                state,
                target,
                "live_price_impact_cap_invalid",
                OffsetDateTime::now_utc(),
            )?;
            return Ok(PassControl::Continue);
        }
    };
    let policy_cap = collateral_cap(account_state.reconciled_free_collateral, per_trade_cap_bps)
        .ok_or_else(|| FanoutError::Signal("live per-trade cap arithmetic failed".to_owned()))?;
    let kelly_allocate = live_kelly_share_allocator(
        &strategy,
        signal,
        probability,
        &initial_risk.snapshot,
        account_state.reconciled_free_collateral.to_decimal(),
    );
    let requested_sizing = match strategy_config.sizing_mode {
        SizingMode::Dollar { usd } => {
            let budget = CollateralAmount::from_decimal_exact(
                usd.round_dp_with_strategy(6, RoundingStrategy::ToNegativeInfinity),
            )
            .map_err(|error| FanoutError::Signal(error.to_string()))?;
            BuySizing::Dollar { budget }
        }
        SizingMode::Contract { contracts } => BuySizing::Contract { contracts },
        SizingMode::Kelly => BuySizing::Kelly {
            allocate: &kelly_allocate,
            slippage_rate: strategy_config.slippage_rate,
        },
    };
    let sized = match plan_sized_buy(
        &ladder,
        admission.fee_schedule,
        requested_sizing,
        &[account_state.reconciled_free_collateral, policy_cap],
        admission.market.minimum_order_size,
        admission.market.minimum_tick_size,
        minimum_price,
        maximum_price,
        signal.leader_price,
        ceiling,
    ) {
        Ok(plan) => plan,
        Err(error) => {
            terminalize(
                state,
                target,
                ladder_terminal_reason(&error),
                OffsetDateTime::now_utc(),
            )?;
            return Ok(PassControl::Continue);
        }
    };
    let plan = sized.ladder;
    let proposed_debit = plan
        .worst_case_debit
        .checked_add(sized.reserve)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let account_state = match venue
        .read_balance_and_allowance(admission.market.neg_risk)
        .await
    {
        Ok(account_state) => account_state,
        Err(_) => return Ok(PassControl::StopSeed),
    };
    if live_financial_posture(state, account, account_state.collateral_balance, false)
        != CheckOutcome::Pass
    {
        return Ok(PassControl::StopSeed);
    }
    if ensure_live_portfolio_marks(
        state,
        account,
        &account_state,
        venue.account_binding(),
        &venue.deposit_wallet(),
    )
    .await
        != CheckOutcome::Pass
    {
        warn!(account_id = %target.account_id, "pre-prepare inventory reconciliation failed; seed paused");
        return Ok(PassControl::StopSeed);
    }
    let risk = match live_risk_audit(
        state,
        account,
        Some(signal),
        proposed_debit,
        per_trade_cap_bps,
    )
    .await
    {
        Ok(risk) => risk,
        Err(reason) => {
            let decline = WinnerFollowError::RiskInputsUnavailable;
            let report = format!("{decline}: {reason}");
            warn!(account_id = %target.account_id, decline = %report, "live risk inputs unavailable; seed paused");
            return Ok(PassControl::StopSeed);
        }
    };
    if sync_live_risk_halts(state, account, &risk).await? {
        return Ok(PassControl::StopSeed);
    }
    let identity = build_order_identity(
        seed,
        target,
        signal,
        &admission,
        &plan,
        book_receipt,
        &strategy_config,
    )?;
    let outcome_index = u8::try_from(signal.outcome_id.0)
        .map_err(|_| FanoutError::Signal("outcome index exceeds u8".to_owned()))?;
    let sizing_mode = match strategy_config.sizing_mode {
        SizingMode::Kelly => SizingModeAudit::Kelly {
            fraction: strategy.effective_kelly_fraction(ExecutionMode::LiveTiny),
            probability,
        },
        SizingMode::Dollar { usd } => SizingModeAudit::Dollar { usd },
        SizingMode::Contract { contracts } => SizingModeAudit::Contract { contracts },
    };
    let economic = EconomicPrepared::compose(EconomicInputs {
        market: pe_execution_core::MarketSelection {
            condition_id: admission.market.condition_id.clone(),
            outcome_index,
            token_id: token_id.clone(),
            side: Side::Buy,
            market_id: signal.market_id.to_string(),
        },
        admission: &admission,
        plan: &plan,
        book_receipt,
        observation: Some(observation.clone()),
        sizing_mode,
        budget: sized.budget,
        slippage_rate: strategy_config.slippage_rate,
        risk,
        cash_before: account_state.reconciled_free_collateral,
        price_impact_cap_bps,
        chase_ceiling: signal.leader_price,
        band_floor: minimum_price,
        band_ceiling_exclusive: maximum_price,
        applied_configuration_hash: identity.config_hash.clone(),
    })
    .map_err(|error| FanoutError::Signal(error.to_string()))?;
    if let Err(decline) = evaluate_live_candidate_at_price(
        &strategy,
        signal,
        economic.sizing.all_in_price,
        probability,
        economic.risk.snapshot.clone(),
        account_state.reconciled_free_collateral.to_decimal(),
    ) {
        terminalize(
            state,
            target,
            winner_follow_terminal_reason(&decline),
            OffsetDateTime::now_utc(),
        )?;
        return Ok(PassControl::Continue);
    }
    if global_risk_halt_active(state)? {
        return Ok(PassControl::StopSeed);
    }
    let binding = CredentialBindingIdentity {
        version: target.credential_bundle_version,
        key_id: target.credential_key_id.clone(),
    };
    let mut request = pe_execution_core::LiveOrderRequest {
        target: FrozenLiveTarget {
            account_id: account.account_id.clone(),
            credential_binding: binding.clone(),
        },
        current_credential_binding: binding,
        mode: LiveModeSnapshot {
            requested: mode_value(&account.requested_live_mode),
            effective: mode_value(&account.effective_live_mode),
        },
        identity,
        condition_id,
        outcome_id: signal.outcome_id,
        token_id,
        admission,
        ladder: plan.clone(),
        economic,
    };
    let prepare_check_at = OffsetDateTime::now_utc();
    let prepare_price_expired =
        risk_price_expired(state, &request.economic.risk, prepare_check_at)?;
    if risk_day_changed(&request.economic.risk, prepare_check_at) || prepare_price_expired {
        if ensure_live_portfolio_marks(
            state,
            account,
            &account_state,
            venue.account_binding(),
            &venue.deposit_wallet(),
        )
        .await
            != CheckOutcome::Pass
        {
            warn!(account_id = %target.account_id, "boundary refresh inventory reconciliation failed; seed paused");
            return Ok(PassControl::StopSeed);
        }
        let refreshed = match live_risk_audit(
            state,
            account,
            Some(signal),
            proposed_debit,
            per_trade_cap_bps,
        )
        .await
        {
            Ok(risk) => risk,
            Err(reason) => {
                warn!(account_id = %target.account_id, decline = %reason, "boundary refresh risk inputs unavailable; seed paused");
                return Ok(PassControl::StopSeed);
            }
        };
        if sync_live_risk_halts(state, account, &refreshed).await? {
            return Ok(PassControl::StopSeed);
        }
        if let Err(decline) = evaluate_live_candidate_at_price(
            &strategy,
            signal,
            request.economic.sizing.all_in_price,
            probability,
            refreshed.snapshot.clone(),
            account_state.reconciled_free_collateral.to_decimal(),
        ) {
            terminalize(
                state,
                target,
                winner_follow_terminal_reason(&decline),
                OffsetDateTime::now_utc(),
            )?;
            return Ok(PassControl::Continue);
        }
        request.economic.risk = refreshed;
    }
    let final_prepare_check_at = OffsetDateTime::now_utc();
    let final_prepare_price_expired =
        risk_price_expired(state, &request.economic.risk, final_prepare_check_at)?;
    if risk_day_changed(&request.economic.risk, final_prepare_check_at)
        || final_prepare_price_expired
    {
        warn!(account_id = %target.account_id, "risk evidence expired during the boundary refresh; seed paused");
        return Ok(PassControl::StopSeed);
    }
    if economic_book_receipt_is_stale(state, &request.economic, final_prepare_check_at)? {
        warn!(account_id = %target.account_id, "ladder evidence expired before preparation; seed paused");
        return Ok(PassControl::StopSeed);
    }
    let journal = state.config.journal.clone();
    let executor = LiveExecutor::new(&venue, journal.as_ref());
    match executor
        .prepare_with_observed_account_and_clock(
            request,
            account_state.clone(),
            OffsetDateTime::now_utc,
        )
        .await?
    {
        LivePrepareResult::Terminal(outcome) => {
            if refusal_is_transient(&outcome) {
                return Ok(PassControl::StopSeed);
            }
            let transition = outcome_transition(&outcome);
            persist_outcome(state, target, &outcome, OffsetDateTime::now_utc())?;
            Ok(if transition.freeze {
                PassControl::FreezePass
            } else {
                PassControl::Continue
            })
        }
        LivePrepareResult::Prepared(prepared) => {
            if ensure_live_portfolio_marks(
                state,
                account,
                &account_state,
                venue.account_binding(),
                &venue.deposit_wallet(),
            )
            .await
                != CheckOutcome::Pass
            {
                let outcome = executor.terminalize_prepared(
                    prepared,
                    OffsetDateTime::now_utc(),
                    pe_execution_core::LiveOrderPreparationFailure::PrePostMarkUnavailable,
                )?;
                persist_outcome(state, target, &outcome, OffsetDateTime::now_utc())?;
                return Ok(PassControl::StopSeed);
            }
            let post_check_at = OffsetDateTime::now_utc();
            if prepared_ladder_is_stale(state, prepared.audit(), post_check_at)? {
                let outcome = executor.terminalize_prepared(
                    prepared,
                    post_check_at,
                    pe_execution_core::LiveOrderPreparationFailure::PrePostLadderExpired,
                )?;
                persist_outcome(state, target, &outcome, post_check_at)?;
                return Ok(PassControl::StopSeed);
            }
            let risk_price_expired =
                prepared_risk_price_expired(state, prepared.audit(), post_check_at)?;
            let risk_day_changed = prepared_risk_day_changed(prepared.audit(), post_check_at);
            let admission_expired =
                !prepared_admission_is_approved_at(prepared.audit(), post_check_at);
            if risk_day_changed || risk_price_expired || admission_expired {
                if risk_day_changed
                    && ensure_live_portfolio_marks(
                        state,
                        account,
                        &account_state,
                        venue.account_binding(),
                        &venue.deposit_wallet(),
                    )
                    .await
                        != CheckOutcome::Pass
                {
                    let outcome = executor.terminalize_prepared(
                        prepared,
                        OffsetDateTime::now_utc(),
                        pe_execution_core::LiveOrderPreparationFailure::PrePostMarkUnavailable,
                    )?;
                    persist_outcome(state, target, &outcome, OffsetDateTime::now_utc())?;
                    return Ok(PassControl::StopSeed);
                }
                let refreshed = live_risk_audit(
                    state,
                    account,
                    Some(signal),
                    prepared.audit().economic.balance.worst_case_debit,
                    prepared.audit().economic.risk.snapshot.per_trade_cap_bps,
                )
                .await;
                if let Ok(risk) = refreshed {
                    let _ = sync_live_risk_halts(state, account, &risk).await?;
                }
                let failure = if admission_expired {
                    pe_execution_core::LiveOrderPreparationFailure::PrePostAdmissionExpired
                } else if risk_price_expired {
                    pe_execution_core::LiveOrderPreparationFailure::PrePostRiskPriceExpired
                } else {
                    pe_execution_core::LiveOrderPreparationFailure::PrePostRiskDayChanged
                };
                let outcome =
                    executor.terminalize_prepared(prepared, OffsetDateTime::now_utc(), failure)?;
                persist_outcome(state, target, &outcome, OffsetDateTime::now_utc())?;
                return Ok(PassControl::StopSeed);
            }
            // The dispatch reservation is durable before the exactly-one POST capability is
            // consumed. Crash recovery reconciles this order hash; it never blindly resubmits.
            state.config.paper_state.set_dispatch_target_state(
                &target.dispatch_id,
                &target.account_id,
                "submitted",
                None,
                OffsetDateTime::now_utc().unix_timestamp(),
            )?;
            let outcome = executor
                .submit_with_clock(prepared, OffsetDateTime::now_utc)
                .await?;
            let transition = outcome_transition(&outcome);
            persist_outcome(state, target, &outcome, OffsetDateTime::now_utc())?;
            if matches!(outcome, LiveOrderOutcome::Matched { .. }) {
                reconcile_account_projection(state, &account.account_id, OffsetDateTime::now_utc())
                    .await;
            }
            Ok(if transition.freeze {
                PassControl::FreezePass
            } else {
                PassControl::Continue
            })
        }
    }
}

fn mode_value(value: &str) -> LiveControlMode {
    if value == "live_tiny" {
        LiveControlMode::LiveTiny
    } else {
        LiveControlMode::Off
    }
}

fn prepared_risk_day_changed(
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    now: OffsetDateTime,
) -> bool {
    risk_day_changed(&prepared.economic.risk, now)
}

fn prepared_ladder_is_stale(
    state: &FanoutState,
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    now: OffsetDateTime,
) -> Result<bool, FanoutError> {
    economic_book_receipt_is_stale(state, &prepared.economic, now)
}

fn economic_book_receipt_is_stale(
    state: &FanoutState,
    economic: &EconomicPrepared,
    now: OffsetDateTime,
) -> Result<bool, FanoutError> {
    Ok(state
        .config
        .source_receipts
        .received_millis(economic.book_receipt)
        .map_or(true, |observed_unix_ms| {
            ladder_observation_is_stale(unix_millis_i64(now), observed_unix_ms)
        }))
}

fn ladder_observation_is_stale(now_unix_ms: i64, observed_unix_ms: i64) -> bool {
    let (Ok(now_ms), Ok(observed_ms)) =
        (u64::try_from(now_unix_ms), u64::try_from(observed_unix_ms))
    else {
        return true;
    };
    pe_venue_polymarket::ladder_is_stale(now_ms, observed_ms)
}

fn unix_millis_i64(now: OffsetDateTime) -> i64 {
    i64::try_from(now.unix_timestamp_nanos().div_euclid(1_000_000)).unwrap_or(i64::MIN)
}

fn risk_day_changed(risk: &RiskAudit, now: OffsetDateTime) -> bool {
    risk.evaluated_at_unix_ms.div_euclid(86_400_000) != now.unix_timestamp().div_euclid(86_400)
}

fn prepared_risk_price_expired(
    state: &FanoutState,
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    now: OffsetDateTime,
) -> Result<bool, FanoutError> {
    risk_price_expired(state, &prepared.economic.risk, now)
}

fn risk_price_expired(
    state: &FanoutState,
    risk: &RiskAudit,
    now: OffsetDateTime,
) -> Result<bool, FanoutError> {
    if risk.price_receipts.is_empty() {
        return Ok(false);
    }
    let evaluated_at_unix_ms = now.unix_timestamp_nanos().div_euclid(1_000_000);
    Ok(risk.price_receipts.iter().any(|receipt| {
        state
            .config
            .source_receipts
            .received_millis(*receipt)
            .map_or(true, |observed_at_unix_ms| {
                crate::mid_price_cache::classify_strict_price_time(
                    i128::from(observed_at_unix_ms),
                    evaluated_at_unix_ms,
                )
                .is_err()
            })
    }))
}

fn prepared_admission_is_approved_at(
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    evaluated_at: OffsetDateTime,
) -> bool {
    matches!(
        classify_live_admission(LiveAdmissionClassificationInput {
            evaluated_at,
            requested_mode: LiveControlMode::LiveTiny,
            effective_mode: LiveControlMode::LiveTiny,
            frozen_binding: &prepared.frozen_binding,
            current_binding: &prepared.frozen_binding,
            identity: &prepared.identity,
            condition_id: &prepared.economic.market.condition_id,
            outcome_id: OutcomeId(u16::from(prepared.economic.market.outcome_index)),
            token_id: &prepared.economic.market.token_id,
            admission: &prepared.economic.admission,
            ladder: &prepared.economic.ladder,
            economic: &prepared.economic,
            account: LiveAdmissionAccountEvidence::State(&prepared.account_state),
        }),
        Ok(pe_execution_core::LiveAdmissionVerdict::Approved)
    )
}

fn prepared_admission_is_approved_with_fresh_account_at(
    prepared: &pe_execution_core::LiveOrderPreparedAudit,
    execution: &RecoveryExecutionState,
    evaluated_at: OffsetDateTime,
) -> Result<bool, FanoutError> {
    let account_audit = execution.account_state.audit()?;
    Ok(matches!(
        classify_live_admission(LiveAdmissionClassificationInput {
            evaluated_at,
            requested_mode: execution.mode.requested,
            effective_mode: execution.mode.effective,
            frozen_binding: &prepared.frozen_binding,
            current_binding: &execution.binding,
            identity: &prepared.identity,
            condition_id: &prepared.economic.market.condition_id,
            outcome_id: OutcomeId(u16::from(prepared.economic.market.outcome_index)),
            token_id: &prepared.economic.market.token_id,
            admission: &prepared.economic.admission,
            ladder: &prepared.economic.ladder,
            economic: &prepared.economic,
            account: LiveAdmissionAccountEvidence::State(&account_audit),
        }),
        Ok(pe_execution_core::LiveAdmissionVerdict::Approved)
    ))
}

fn unix_ms(now: OffsetDateTime) -> u64 {
    u64::try_from(now.unix_timestamp_nanos() / 1_000_000).unwrap_or(0)
}

fn parse_band_price(value: Decimal, disabled: Price) -> Result<Price, ()> {
    if value == Decimal::ZERO {
        return Ok(disabled);
    }
    Price::new(value).map_err(|_| ())
}

fn collateral_cap(bankroll: CollateralAmount, cap_bps: i32) -> Option<CollateralAmount> {
    if !(0..=10_000).contains(&cap_bps) {
        return None;
    }
    let value = bankroll
        .to_decimal()
        .checked_mul(Decimal::from(cap_bps))?
        .checked_div(Decimal::from(10_000i32))?
        .round_dp_with_strategy(6, RoundingStrategy::ToNegativeInfinity);
    CollateralAmount::from_decimal_exact(value).ok()
}

fn probability_for(live: &LiveWatchlist, signal: &LeaderSignal) -> Probability {
    let bps = live
        .snapshot()
        .entries
        .iter()
        .find(|entry| entry.wallet == signal.leader.0)
        .map(|entry| entry.win_rate_bps.0)
        .unwrap_or(0)
        .clamp(0, 10_000);
    Probability::new(Decimal::from(bps) / Decimal::from(10_000i32)).unwrap_or(Probability::ZERO)
}

/// Sole live adapter around the strategy sizing API. The LEAF integration changes only this
/// helper when removing the lane-B cap arguments from `evaluate_at_price`.
fn evaluate_live_candidate_at_price(
    strategy: &WinnerFollowStrategy,
    signal: &LeaderSignal,
    all_in_price: Price,
    probability: Probability,
    risk: RiskSnapshot,
    bankroll: Decimal,
) -> Result<pe_venue_core::OrderIntent, WinnerFollowError> {
    strategy.evaluate_at_price(
        signal,
        all_in_price,
        probability,
        risk,
        bankroll,
        ExecutionMode::LiveTiny,
    )
}

/// Sole live Kelly allocation adapter for the venue-owned convergence chain.
fn live_kelly_share_allocator<'a>(
    strategy: &'a WinnerFollowStrategy,
    signal: &'a LeaderSignal,
    probability: Probability,
    risk: &'a RiskSnapshot,
    bankroll: Decimal,
) -> impl Fn(Price) -> Result<ShareAmount, LadderError> + 'a {
    move |all_in_price| {
        let intent = evaluate_live_candidate_at_price(
            strategy,
            signal,
            all_in_price,
            probability,
            risk.clone(),
            bankroll,
        )
        .map_err(|error| match error {
            WinnerFollowError::NoEdge => LadderError::NoEdge,
            WinnerFollowError::ShadowMode
            | WinnerFollowError::FlipNotApproved
            | WinnerFollowError::Blocked(_)
            | WinnerFollowError::RiskInputsUnavailable
            | WinnerFollowError::KellySizing(_) => LadderError::KellySizing,
        })?;
        ShareAmount::from_whole(intent.contracts.0).map_err(|_| LadderError::KellySizing)
    }
}

async fn live_risk_audit(
    state: &FanoutState,
    account: &AccountContext,
    signal: Option<&LeaderSignal>,
    proposed_debit: CollateralAmount,
    per_trade_cap_bps: i32,
) -> Result<RiskAudit, RiskInputsUnavailable> {
    let events = replay_live_account(state, &account.account_id)
        .map_err(|_| RiskInputsUnavailable::SnapshotSequenceMismatch)?;
    let derived = derive_projection_rows_for_state(state, &account.account_id, &events)
        .map_err(|_| RiskInputsUnavailable::SnapshotSequenceMismatch)?;
    let ids = derived
        .positions
        .iter()
        .map(|position| {
            let outcome =
                u16::try_from(position.outcome_id).map_err(|_| RiskInputsUnavailable::Overflow)?;
            Ok(MarketOutcomeId::new(
                MarketId(VenueMarketId(position.market_id.clone())),
                OutcomeId(outcome),
            ))
        })
        .collect::<Result<Vec<_>, RiskInputsUnavailable>>()?;
    let price_attempt = state
        .config
        .mid_price_cache
        .fetch_mids_strict_attempt(&ids)
        .await;
    let now = price_attempt.evaluated_at;
    let mids = price_attempt.result?;
    let paper_frames = scan_paper_log(&state.config.paper_log_path)
        .map_err(|_| RiskInputsUnavailable::SnapshotSequenceMismatch)?;
    let era = paper_era_at_evaluation(&paper_frames, evaluated_at_millis(now)?)?;
    compose_live_risk_audit(
        &account.account_id,
        &state.config.journal_path,
        &events,
        &derived,
        &era,
        &mids,
        &price_attempt.price_receipts,
        now,
        LiveRiskProposal {
            leader_wallet: signal.map(|signal| signal.leader.to_string()),
            market_id: signal.map(|signal| signal.market_id.to_string()),
            proposed_debit,
            per_trade_cap_bps,
        },
    )
}

#[derive(Debug, Clone)]
struct LiveRiskProposal {
    leader_wallet: Option<String>,
    market_id: Option<String>,
    proposed_debit: CollateralAmount,
    per_trade_cap_bps: i32,
}

fn evaluated_at_millis(now: OffsetDateTime) -> Result<i64, RiskInputsUnavailable> {
    i64::try_from(now.unix_timestamp_nanos().div_euclid(1_000_000))
        .map_err(|_| RiskInputsUnavailable::Overflow)
}

/// Select the exact paper prefix available at the risk clock. A later frame with an earlier
/// timestamp would make cross-log causality ambiguous and therefore fails closed.
fn paper_era_at_evaluation(
    frames: &[ScannedPaperFrame],
    evaluated_at_unix_ms: i64,
) -> Result<PaperEra, RiskInputsUnavailable> {
    let mut prefix = Vec::new();
    let mut passed_clock = false;
    for frame in frames {
        let received_at_unix_ms = i64::try_from(
            frame
                .envelope
                .received_at
                .0
                .unix_timestamp_nanos()
                .div_euclid(1_000_000),
        )
        .map_err(|_| RiskInputsUnavailable::Overflow)?;
        if received_at_unix_ms <= evaluated_at_unix_ms {
            if passed_clock {
                return Err(RiskInputsUnavailable::SnapshotSequenceMismatch);
            }
            prefix.push(frame.clone());
        } else {
            passed_clock = true;
        }
    }
    if prefix.is_empty() {
        return Err(RiskInputsUnavailable::SnapshotSequenceMismatch);
    }
    Ok(paper_era(prefix))
}

/// The one deterministic owner of an ordinary-live risk audit. Production passes freshly
/// acquired mids; strict replay reconstructs those same observations from the recorded receipts.
/// Every financial dimension is derived from the preceding live prefix, and the paper prefix owns
/// the strategy-wide halt overlay plus the live latency hysteresis seed.
#[allow(clippy::too_many_arguments)]
fn compose_live_risk_audit(
    account_id: &AccountId,
    live_journal_path: &Path,
    events: &[LiveJournalEvent],
    derived: &ProjectionDerivation,
    paper_era: &PaperEra,
    mids: &BTreeMap<(String, u16), MidPriceObservation>,
    acquired_price_receipts: &[AppendReceipt],
    now: OffsetDateTime,
    proposal: LiveRiskProposal,
) -> Result<RiskAudit, RiskInputsUnavailable> {
    let baseline = derived
        .baseline_equity
        .filter(|value| *value != CollateralAmount::ZERO)
        .ok_or(RiskInputsUnavailable::BaselineNonPositive)?;
    let cash = derived
        .economic_cash
        .ok_or(RiskInputsUnavailable::BaselineNonPositive)?;
    let price_receipts = sorted_price_receipts(mids)?;
    if acquired_price_receipts != price_receipts {
        return Err(RiskInputsUnavailable::PriceConflict);
    }
    let marked_positions = derived
        .positions
        .iter()
        .map(|position| {
            if position.short_contracts != Decimal::ZERO {
                return Err(RiskInputsUnavailable::Overflow);
            }
            let quantity = ShareAmount::from_decimal_exact(position.long_contracts)
                .map_err(|_| RiskInputsUnavailable::Overflow)?;
            let outcome =
                u16::try_from(position.outcome_id).map_err(|_| RiskInputsUnavailable::Overflow)?;
            let price = mids
                .get(&(position.market_id.clone(), outcome))
                .ok_or(RiskInputsUnavailable::PriceMissing)?
                .price;
            Ok((quantity, price))
        })
        .collect::<Result<Vec<_>, RiskInputsUnavailable>>()?;
    let equity = pe_risk_engine::current_equity(&pe_risk_engine::EquityInputs {
        cash,
        positions: &marked_positions,
    })
    .map_err(|_| RiskInputsUnavailable::Overflow)?;
    let midnight = now.unix_timestamp().div_euclid(86_400) * 86_400;
    let baseline_cutoff = derived
        .baseline_cutoff_unix
        .ok_or(RiskInputsUnavailable::MarkMissing)?;
    let preceding_mark_equity = if baseline_cutoff < midnight {
        Some(
            derived
                .daily_marks
                .get(&midnight)
                .ok_or(RiskInputsUnavailable::MarkMissing)?
                .to_decimal(),
        )
    } else {
        None
    };
    let realized_closes_7d =
        pe_risk_engine::realized_closes_7d(&derived.realized_closes, now.unix_timestamp())
            .map_err(|_| RiskInputsUnavailable::Overflow)?;
    let pnl = pe_risk_engine::pnl_bps(
        equity,
        &pe_risk_engine::PnlWindow {
            starting_bankroll: baseline.to_decimal(),
            preceding_mark_equity,
            realized_closes_7d,
        },
    )
    .map_err(|_| RiskInputsUnavailable::Overflow)?;

    let exposure = |leader: Option<&str>, market: Option<&str>| {
        derived
            .open_exposures
            .iter()
            .filter(|item| leader.is_none_or(|value| item.leader_wallet == value))
            .filter(|item| market.is_none_or(|value| item.market_id == value))
            .try_fold(CollateralAmount::ZERO, |total, item| {
                total
                    .checked_add(item.debit)
                    .map_err(|_| RiskInputsUnavailable::Overflow)
            })
    };
    let total = exposure(None, None)?;
    let leader = exposure(proposal.leader_wallet.as_deref(), None)?;
    let market = exposure(None, proposal.market_id.as_deref())?;
    let to_bps = |amount| {
        pe_risk_engine::exposure_bps_ceil(amount, baseline).ok_or(RiskInputsUnavailable::Overflow)
    };
    let latency_owner = RiskHaltOwner::LiveAccount(account_id.clone());
    let financial_prefix = paper_era
        .frames
        .last()
        .map(|frame| frame.receipt)
        .ok_or(RiskInputsUnavailable::SnapshotSequenceMismatch)?;
    let latency_seed =
        crate::risk_inputs::latency_hysteresis_seed(paper_era, &latency_owner, live_journal_path)?;
    let copy_latency_kill_switch_active = live_latency_switch(events, now, latency_seed)?;
    let mut snapshot = RiskSnapshot {
        leader_exposure_bps: to_bps(leader)?,
        market_exposure_bps: to_bps(market)?,
        // The live protocol has no independent family identity. Treating every open position as
        // one family is the conservative receipt-backed view, never a fabricated zero.
        family_exposure_bps: to_bps(total)?,
        total_copy_exposure_bps: to_bps(total)?,
        intraday_pnl_bps: pnl.intraday,
        rolling_7d_pnl_bps: pnl.rolling_7d,
        absolute_pnl_bps: pnl.absolute,
        copy_latency_kill_switch_active,
        proposed_trade_bps: to_bps(proposal.proposed_debit)?,
        per_trade_cap_bps: proposal.per_trade_cap_bps,
        concentration_caps: None,
    };
    apply_global_risk_halts(&active_risk_halts(paper_era), &mut snapshot);
    let decision = match pe_risk_engine::evaluate_risk(&snapshot) {
        pe_risk_engine::RiskDecision::Approved => RiskDecisionAudit::Approved,
        pe_risk_engine::RiskDecision::Blocked(reason) => RiskDecisionAudit::Blocked { reason },
    };
    let evaluated_at_unix_ms = evaluated_at_millis(now)?;
    Ok(RiskAudit {
        financial_prefix,
        snapshot,
        decision,
        price_receipts,
        evaluated_at_unix_ms,
    })
}

async fn sync_live_risk_halts(
    state: &FanoutState,
    account: &AccountContext,
    risk: &RiskAudit,
) -> Result<bool, FanoutError> {
    let owner = RiskHaltOwner::LiveAccount(account.account_id.clone());
    let current = active_risk_halts(&paper_era(
        scan_paper_log(&state.config.paper_log_path)
            .map_err(|error| FanoutError::Signal(error.to_string()))?,
    ));
    let desired = [
        (
            pe_risk_engine::RiskHaltCause::IntradayDrawdown,
            risk.snapshot.intraday_pnl_bps.0 <= pe_risk_engine::INTRADAY_STOP_BPS,
        ),
        (
            pe_risk_engine::RiskHaltCause::Rolling7dDrawdown,
            risk.snapshot.rolling_7d_pnl_bps.0 <= pe_risk_engine::ROLLING_7D_STOP_BPS,
        ),
        (
            pe_risk_engine::RiskHaltCause::AbsoluteLoss,
            risk.snapshot.absolute_pnl_bps.0 <= pe_risk_engine::KILL_SWITCH_DRAWDOWN_BPS
                || current.contains(&(owner.clone(), pe_risk_engine::RiskHaltCause::AbsoluteLoss)),
        ),
        (
            pe_risk_engine::RiskHaltCause::CopyLatency,
            risk.snapshot.copy_latency_kill_switch_active,
        ),
    ];
    for (cause, active) in desired {
        let was_active = current.contains(&(owner.clone(), cause));
        if active == was_active {
            continue;
        }
        let (acknowledged, receipt) = tokio::sync::oneshot::channel();
        state
            .config
            .orchestrator_control
            .send(OrchestratorControl::RiskHaltChange {
                owner: owner.clone(),
                cause,
                state: if active {
                    HaltState::Engaged
                } else {
                    HaltState::Released
                },
                evidence: serde_json::json!({
                    "risk": risk,
                    "evaluated_at_unix_ms": risk.evaluated_at_unix_ms,
                }),
                acknowledged,
            })
            .await
            .map_err(|_| FanoutError::Signal("risk halt control closed".to_owned()))?;
        receipt
            .await
            .map_err(|_| FanoutError::Signal("risk halt acknowledgement dropped".to_owned()))?
            .map_err(FanoutError::Signal)?;
    }
    global_risk_halt_active(state)
}

fn global_risk_halt_active(state: &FanoutState) -> Result<bool, FanoutError> {
    Ok(!active_risk_halts(&paper_era(
        scan_paper_log(&state.config.paper_log_path)
            .map_err(|error| FanoutError::Signal(error.to_string()))?,
    ))
    .is_empty())
}

fn sorted_price_receipts(
    mids: &BTreeMap<(String, u16), MidPriceObservation>,
) -> Result<Vec<AppendReceipt>, RiskInputsUnavailable> {
    let mut receipts = mids
        .values()
        .map(|observation| observation.receipt)
        .collect::<Vec<_>>();
    receipts.sort_by_key(|receipt| receipt.sequence);
    if receipts
        .windows(2)
        .any(|pair| pair[0].sequence == pair[1].sequence && pair[0] != pair[1])
    {
        return Err(RiskInputsUnavailable::PriceConflict);
    }
    receipts.dedup_by_key(|receipt| receipt.sequence);
    Ok(receipts)
}

/// Receipt payload retained by the shared admission/book economic replay owner.
#[derive(Debug, Clone)]
pub(crate) struct RecordedEconomicSource {
    pub payload: Vec<u8>,
    pub received_unix_ms: i64,
    pub source_id: String,
    pub schema_version: u32,
    pub parser_version: u32,
    pub content_type: ContentType,
}

/// Fail-closed source-backed economic reconstruction error shared by qualification and live replay.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct EconomicReplayError(pub(crate) String);

fn economic_replay_error(message: impl Into<String>) -> EconomicReplayError {
    EconomicReplayError(message.into())
}

/// Admission and sized ladder reconstructed from the exact four source receipts.
pub(crate) struct SourceBackedEconomicReplay {
    admission: LiveAdmissionArtifact,
    sized: pe_venue_polymarket::SizedBuyPlan,
    cash_before: CollateralAmount,
}

impl SourceBackedEconomicReplay {
    pub(crate) fn proposed_debit(&self) -> Result<CollateralAmount, EconomicReplayError> {
        self.sized.worst_case_all_in_debit().map_err(|error| {
            economic_replay_error(format!(
                "proposed risk debit reconstruction failed: {error}"
            ))
        })
    }

    /// Recompose the complete canonical record after the wrapper reconstructs its risk audit.
    pub(crate) fn recompose(
        &self,
        economic: &EconomicPrepared,
        risk: RiskAudit,
        applied_configuration_hash: String,
    ) -> Result<EconomicPrepared, EconomicReplayError> {
        let recomposed = EconomicPrepared::compose(EconomicInputs {
            market: economic.market.clone(),
            admission: &self.admission,
            plan: &self.sized.ladder,
            book_receipt: economic.book_receipt,
            observation: economic.observation.clone(),
            sizing_mode: economic.sizing.mode,
            budget: self.sized.budget,
            slippage_rate: economic.sizing.slippage_rate,
            risk,
            cash_before: self.cash_before,
            price_impact_cap_bps: economic.balance.price_impact_cap_bps,
            chase_ceiling: economic.balance.chase_ceiling,
            band_floor: economic.balance.band_floor,
            band_ceiling_exclusive: economic.balance.band_ceiling_exclusive,
            applied_configuration_hash,
        })
        .map_err(|error| {
            economic_replay_error(format!(
                "EconomicPrepared canonical composition failed: {error}"
            ))
        })?;
        let recomposed_hash = recomposed.core_hash().map_err(|error| {
            economic_replay_error(format!("recomposed economic core hash failed: {error}"))
        })?;
        let recorded_hash = economic.core_hash().map_err(|error| {
            economic_replay_error(format!("recorded economic core hash failed: {error}"))
        })?;
        if &recomposed != economic || recomposed_hash != recorded_hash {
            return Err(economic_replay_error(
                "EconomicPrepared differs from raw-evidence canonical replay",
            ));
        }
        Ok(recomposed)
    }
}

fn exact_economic_source<L>(
    receipt: AppendReceipt,
    source_id: &str,
    schema_version: u32,
    parser_version: u32,
    evidence_cutoff_unix_ms: i64,
    lookup: &mut L,
) -> Result<RecordedEconomicSource, EconomicReplayError>
where
    L: FnMut(AppendReceipt) -> Result<RecordedEconomicSource, EconomicReplayError>,
{
    let observation = lookup(receipt).map_err(|_| {
        economic_replay_error(format!(
            "{source_id} receipt is absent from the sealed source prefix"
        ))
    })?;
    let contract_matches = if source_id == crate::trade_poller::ACTIVITY_POLL_SOURCE_ID {
        crate::bucket_commit::activity_page_generation(
            &observation.source_id,
            observation.schema_version,
            observation.parser_version,
            &observation.content_type,
        )
        .is_ok()
    } else {
        observation.source_id == source_id
            && observation.schema_version == schema_version
            && observation.parser_version == parser_version
            && observation.content_type == ContentType::Json
    };
    if !contract_matches || observation.received_unix_ms > evidence_cutoff_unix_ms {
        return Err(economic_replay_error(format!(
            "{source_id} receipt has the wrong source contract or is noncausal"
        )));
    }
    Ok(observation)
}

#[derive(Clone, Copy)]
struct LiveObservationBinding<'a> {
    account_id: &'a AccountId,
    identity: &'a LiveOrderIdentity,
    continuation: &'a DecisionContinuationV3,
    correction: Option<&'a pe_position_ledger::IdentityCorrection>,
}

type LiveObservationPageIndex = HashMap<
    SourceTradeId,
    (
        DecisionContinuationV3,
        Option<pe_position_ledger::IdentityCorrection>,
    ),
>;

/// Source trade ids named by the admissions in `events` (the only continuations replay needs).
fn referenced_source_trade_ids(events: &[LiveJournalEvent]) -> HashSet<SourceTradeId> {
    events
        .iter()
        .filter_map(|event| match &event.payload {
            LiveJournalPayload::AdmissionEvaluated(admission) => admission
                .identity
                .fill_projection
                .as_ref()
                .and_then(|projection| projection.source_trade_id.clone())
                .map(SourceTradeId),
            _ => None,
        })
        .collect()
}

/// The durable V3 continuations for exactly the referenced source trades, read one row each.
fn live_observation_page_index(
    paper_state: &PaperStateDb,
    source_trade_ids: &HashSet<SourceTradeId>,
) -> Result<LiveObservationPageIndex, ProjectionReducerError> {
    let mut pages = HashMap::with_capacity(source_trade_ids.len());
    for source_trade_id in source_trade_ids {
        let Some(row) = paper_state
            .decision_pending_for(source_trade_id)
            .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?
        else {
            continue;
        };
        let continuation = DecisionContinuationV3::from_durable(&row)
            .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
        let applied = continuation
            .facts
            .durable_group_effect(paper_state)
            .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
        pages.insert(
            continuation.facts.source_trade_id.clone(),
            (continuation, applied.effect.correction().cloned()),
        );
    }
    Ok(pages)
}

fn source_time_from_millis(received_unix_ms: i64) -> Result<OffsetDateTime, EconomicReplayError> {
    OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(received_unix_ms)
            .checked_mul(1_000_000)
            .ok_or_else(|| economic_replay_error("economic observation clock overflow"))?,
    )
    .map_err(|_| economic_replay_error("economic observation clock is outside the supported range"))
}

fn validate_live_observation_trade(
    economic: &EconomicPrepared,
    aggregates: &[pe_source_polymarket_public::ActivityAggregate],
    selected: &RecordedEconomicSource,
    binding: LiveObservationBinding<'_>,
) -> Result<(), EconomicReplayError> {
    let projection =
        binding.identity.fill_projection.as_deref().ok_or_else(|| {
            economic_replay_error("current live observation has no fill projection")
        })?;
    let source_trade_id = projection.source_trade_id.as_deref().ok_or_else(|| {
        economic_replay_error("current live fill projection has no source trade identity")
    })?;
    let leader = WalletAddress::from_hex(&projection.leader_wallet)
        .map_err(|_| economic_replay_error("current live fill projection has an invalid leader"))?;
    let mut matching = aggregates
        .iter()
        .filter(|aggregate| aggregate.group_id.key().0 == source_trade_id);
    let aggregate = matching.next().ok_or_else(|| {
        economic_replay_error(
            "economic observation REST evidence does not contain the recorded trade",
        )
    })?;
    if matching.next().is_some() {
        return Err(economic_replay_error(
            "economic observation REST evidence contains duplicate recorded trades",
        ));
    }
    let frozen = &binding.continuation.facts;
    let outcome_id = u16::try_from(projection.outcome_id).map_err(|_| {
        economic_replay_error("current live fill projection has an invalid outcome")
    })?;
    let projection_matches_economic = projection.market_id == economic.market.market_id
        && outcome_id == u16::from(economic.market.outcome_index)
        && projection.side == "buy"
        && economic.market.side == Side::Buy;
    let frozen_matches_projection = frozen.wallet == leader
        && frozen.market_id.to_string() == projection.market_id
        && frozen.outcome_id == OutcomeId(outcome_id)
        && frozen.side == Side::Buy;
    if !projection_matches_economic || !frozen_matches_projection {
        return Err(economic_replay_error(
            "economic observation trade identity differs from the live admission",
        ));
    }

    if selected.source_id == crate::activity_ingest::ACTIVITY_WS_SOURCE_ID {
        let websocket = parse_activity_trade_observation(&selected.payload).map_err(|error| {
            economic_replay_error(format!(
                "economic observation websocket parse failed: {error}"
            ))
        })?;
        if websocket.wallet != leader
            || websocket.group_id.key().0 != source_trade_id
            || websocket.source_time != aggregate.source_time
        {
            return Err(economic_replay_error(
                "economic observation websocket differs from the reconciled trade",
            ));
        }
    }

    crate::qualification::verify_decision_continuation_facts(
        aggregate,
        &binding.continuation.facts,
        binding.correction,
    )
    .map_err(|error| {
        economic_replay_error(format!(
            "economic observation continuation fact replay failed: {error}"
        ))
    })?;

    let frozen = &binding.continuation.facts;
    let observed_at = OffsetDateTime::from_unix_timestamp(frozen.source_epoch)
        .map_err(|_| economic_replay_error("frozen decision source clock is invalid"))?;
    let signal = LeaderSignal {
        leader: TraderId(frozen.wallet),
        venue: VenueId::polymarket(),
        market_id: frozen.market_id.clone(),
        outcome_id: frozen.outcome_id,
        action: frozen.pre_bucket_action,
        leader_side: frozen.side,
        leader_price: frozen.price,
        leader_size: frozen.share_amount,
        observed_at,
        received_at: observed_at,
        reconstruction_quality: frozen.reconstruction_quality,
        source_trade_id: frozen.source_trade_id.clone(),
        action_confidence_ppm: frozen.action_confidence_ppm,
    };
    let decision_hash = hash_json(&(
        "prediction-edge/live-decision/v1",
        &signal,
        binding.account_id.as_str(),
        &binding.identity.config_hash,
        &binding.identity.quote_id,
        &binding.identity.evidence_hashes,
    ))
    .map_err(|error| economic_replay_error(format!("live decision hash replay failed: {error}")))?;
    if decision_hash != binding.identity.decision_hash {
        return Err(economic_replay_error(
            "economic observation frozen decision facts differ from the recorded live signal",
        ));
    }
    Ok(())
}

fn validate_economic_observation<L>(
    economic: &EconomicPrepared,
    observation: &ObservationEvidence,
    evidence_cutoff_unix_ms: i64,
    live_binding: Option<LiveObservationBinding<'_>>,
    lookup: &mut L,
) -> Result<(), EconomicReplayError>
where
    L: FnMut(AppendReceipt) -> Result<RecordedEconomicSource, EconomicReplayError>,
{
    if observation.source_receipt.sequence > observation.complete_bound_receipt.sequence {
        return Err(economic_replay_error(
            "economic observation receipt is after its complete-read bound",
        ));
    }
    let complete_bound = exact_economic_source(
        observation.complete_bound_receipt,
        crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
        pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
        pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
        evidence_cutoff_unix_ms,
        lookup,
    )?;
    let selected = match observation.provenance.as_str() {
        "rest_poll" => {
            if observation.source_receipt != observation.complete_bound_receipt {
                return Err(economic_replay_error(
                    "rest-poll observation does not equal its complete-read bound",
                ));
            }
            complete_bound.clone()
        }
        "activity_ws" => {
            if observation.source_receipt.sequence >= observation.complete_bound_receipt.sequence {
                return Err(economic_replay_error(
                    "activity-websocket observation is not before its complete-read bound",
                ));
            }
            exact_economic_source(
                observation.source_receipt,
                crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
                pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                evidence_cutoff_unix_ms,
                lookup,
            )?
        }
        _ => {
            return Err(economic_replay_error(
                "economic observation provenance is unsupported",
            ));
        }
    };
    if selected.received_unix_ms != observation.observed_unix_ms {
        return Err(economic_replay_error(
            "economic observation receive clock is inconsistent with its receipts",
        ));
    }
    if selected.received_unix_ms > complete_bound.received_unix_ms {
        return Err(economic_replay_error(
            "economic observation receive clock is after its complete-read bound",
        ));
    }
    if let Some(binding) = live_binding {
        if binding.continuation.observation_receipt() != Some(observation.source_receipt)
            || binding.continuation.complete_bound() != Some(observation.complete_bound_receipt)
        {
            return Err(economic_replay_error(
                "economic observation receipts differ from the durable continuation",
            ));
        }
        let mut activity_page_lookup = |receipt| -> Result<_, EconomicReplayError> {
            let (source_id, schema, parser) =
                if Some(receipt) == binding.continuation.read_commitment {
                    (
                        crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID,
                        crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
                        crate::bucket_commit::ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
                    )
                } else {
                    (
                        crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
                        pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                        pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    )
                };
            let source = exact_economic_source(
                receipt,
                source_id,
                schema,
                parser,
                evidence_cutoff_unix_ms,
                lookup,
            )?;
            let received_at = source_time_from_millis(source.received_unix_ms)?;
            Ok(CompleteActivityPage {
                payload: source.payload,
                observed_at: SourceTimestamp(received_at),
                received_at: ReceivedAt(received_at),
                source_id: source.source_id,
                schema_version: source.schema_version,
                parser_version: source.parser_version,
                content_type: source.content_type,
            })
        };
        let aggregates = binding
            .continuation
            .reconstruct_complete_activity_read(&mut activity_page_lookup)
            .map_err(|error| {
                economic_replay_error(format!(
                    "economic observation complete activity reconstruction failed: {error}"
                ))
            })?;
        validate_live_observation_trade(economic, &aggregates, &selected, binding)?;
    }
    Ok(())
}

/// Rebuild admission, compact fee authority, and the signed `/book` ladder from four exact
/// receipts. Qualification supplies its sealed-prefix lookup; live replay supplies its scoped
/// source-index lookup. No copied fee, reserve, quantity, or ladder field is trusted here.
fn replay_source_backed_economic_with_live_binding<L>(
    economic: &EconomicPrepared,
    evidence_cutoff_unix_ms: i64,
    cash_before: CollateralAmount,
    live_binding: Option<LiveObservationBinding<'_>>,
    mut lookup: L,
) -> Result<SourceBackedEconomicReplay, EconomicReplayError>
where
    L: FnMut(AppendReceipt) -> Result<RecordedEconomicSource, EconomicReplayError>,
{
    if live_binding.is_some() && economic.observation.is_none() {
        return Err(economic_replay_error(
            "current live economic admission has no observation evidence",
        ));
    }
    if let Some(observation) = &economic.observation {
        validate_economic_observation(
            economic,
            observation,
            evidence_cutoff_unix_ms,
            live_binding,
            &mut lookup,
        )?;
    }
    let receipts = economic.admission.receipts;
    if [
        receipts.gamma.sequence,
        receipts.clob_long.sequence,
        receipts.clob_compact.sequence,
        economic.book_receipt.sequence,
    ]
    .into_iter()
    .collect::<BTreeSet<_>>()
    .len()
        != 4
    {
        return Err(economic_replay_error(
            "economic admission and book receipts are not unique",
        ));
    }
    let gamma = exact_economic_source(
        receipts.gamma,
        GAMMA_MARKETS_SOURCE_ID,
        GAMMA_MARKETS_SCHEMA_VERSION,
        GAMMA_MARKETS_PARSER_VERSION,
        evidence_cutoff_unix_ms,
        &mut lookup,
    )?;
    let clob_long = exact_economic_source(
        receipts.clob_long,
        CLOB_LONG_MARKET_SOURCE_ID,
        LIVE_MARKET_SCHEMA_VERSION,
        LIVE_MARKET_PARSER_VERSION,
        evidence_cutoff_unix_ms,
        &mut lookup,
    )?;
    let clob_compact = exact_economic_source(
        receipts.clob_compact,
        CLOB_COMPACT_MARKET_SOURCE_ID,
        LIVE_MARKET_SCHEMA_VERSION,
        LIVE_MARKET_PARSER_VERSION,
        evidence_cutoff_unix_ms,
        &mut lookup,
    )?;
    let book_observation = exact_economic_source(
        economic.book_receipt,
        CLOB_BOOK_SOURCE_ID,
        CLOB_BOOK_SCHEMA_VERSION,
        CLOB_BOOK_PARSER_VERSION,
        evidence_cutoff_unix_ms,
        &mut lookup,
    )?;
    let market_observed_at_unix = gamma
        .received_unix_ms
        .max(clob_long.received_unix_ms)
        .div_euclid(1_000);
    let settlement_observed_at_unix = clob_long.received_unix_ms.div_euclid(1_000);
    if economic.admission.market.freshness_window_secs != LIVE_MARKET_FRESHNESS_SECS
        || market_observed_at_unix < 0
        || settlement_observed_at_unix < 0
        || economic.admission.market.observed_at_unix != market_observed_at_unix
        || economic.admission.settlement.observed_at_unix != settlement_observed_at_unix
    {
        return Err(economic_replay_error(
            "economic admission clock or freshness contract is invalid",
        ));
    }
    let market = validate_live_market(
        &gamma.payload,
        &clob_long.payload,
        &economic.market.condition_id,
        market_observed_at_unix,
        LIVE_MARKET_FRESHNESS_SECS,
    )
    .map_err(|error| {
        economic_replay_error(format!("economic long-market replay failed: {error}"))
    })?;
    let compact = parse_compact_market(
        &clob_compact.payload,
        &economic.market.condition_id,
        &market.ordered_outcome_token_ids,
    )
    .map_err(|error| {
        economic_replay_error(format!("economic compact-market replay failed: {error}"))
    })?;
    if compact.minimum_order_size != market.minimum_order_size
        || compact.minimum_tick_size != market.minimum_tick_size
        || compact.neg_risk != market.neg_risk
    {
        return Err(economic_replay_error(
            "economic compact and long market rules disagree",
        ));
    }
    let admission = LiveAdmissionArtifact {
        settlement: pe_resolver_card::VenueSettlementRecord {
            schema_version: pe_resolver_card::VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: economic.market.condition_id.clone(),
            status: pe_resolver_card::VenueResolutionStatus::Unresolved,
            raw_evidence_hash: blake3::hash(&clob_long.payload).to_hex().to_string(),
            source_timestamp_unix: None,
            observed_at_unix: settlement_observed_at_unix,
            parser_version: 1,
            freshness_window_secs: LIVE_MARKET_FRESHNESS_SECS,
        },
        market,
        fee_schedule: compact.fee_schedule,
        receipts: AdmissionReceipts {
            gamma: receipts.gamma,
            clob_long: receipts.clob_long,
            clob_compact: receipts.clob_compact,
        },
    };
    let book = crate::clob_book::OrderBook::from_book_json_with_identity(&book_observation.payload)
        .map_err(|error| economic_replay_error(format!("economic book replay failed: {error}")))?;
    book.validate_identity(
        &economic.market.token_id.0,
        Some(&economic.market.condition_id.0),
    )
    .map_err(|error| economic_replay_error(format!("economic book replay failed: {error}")))?;
    let book = book.into_order_book();
    let asks = book
        .ladder()
        .ok_or_else(|| economic_replay_error("economic book cannot form the production ladder"))?;
    let best_ask = asks
        .first()
        .map(|ask| ask.price)
        .ok_or_else(|| economic_replay_error("economic book has no eligible asks"))?;
    let impact_ceiling = recorded_impact_ceiling(best_ask, economic.balance.price_impact_cap_bps)?;
    let allocate = |price: Price| {
        let SizingModeAudit::Kelly {
            fraction,
            probability,
        } = economic.sizing.mode
        else {
            return Err(LadderError::KellySizing);
        };
        let quantity = size_contracts(&KellyInput {
            p: probability,
            c: price,
            kelly_fraction: fraction,
            bankroll: cash_before.to_decimal(),
        })
        .map_err(|_| LadderError::KellySizing)?;
        ShareAmount::from_whole(quantity.0).map_err(|_| LadderError::Amount)
    };
    let sizing = match economic.sizing.mode {
        SizingModeAudit::Dollar { usd } => {
            let budget = CollateralAmount::from_decimal_exact(
                usd.max(Decimal::ZERO)
                    .round_dp_with_strategy(6, RoundingStrategy::ToZero),
            )
            .map_err(|error| {
                economic_replay_error(format!("economic Dollar budget is invalid: {error}"))
            })?;
            BuySizing::Dollar { budget }
        }
        SizingModeAudit::Contract { contracts } => BuySizing::Contract { contracts },
        SizingModeAudit::Kelly { .. } => BuySizing::Kelly {
            allocate: &allocate,
            slippage_rate: economic.sizing.slippage_rate,
        },
    };
    let proportional_cap = CollateralAmount::from_decimal_exact(
        cash_before
            .to_decimal()
            .checked_mul(Decimal::from(economic.risk.snapshot.per_trade_cap_bps))
            .and_then(|value| value.checked_div(Decimal::from(10_000u32)))
            .ok_or_else(|| economic_replay_error("economic per-trade cap arithmetic overflow"))?
            .max(Decimal::ZERO)
            .round_dp_with_strategy(6, RoundingStrategy::ToZero),
    )
    .map_err(|error| {
        economic_replay_error(format!("economic per-trade cap is invalid: {error}"))
    })?;
    let sized = plan_sized_buy(
        &asks,
        admission.fee_schedule,
        sizing,
        &[cash_before, proportional_cap],
        admission.market.minimum_order_size,
        admission.market.minimum_tick_size,
        economic.balance.band_floor,
        economic.balance.band_ceiling_exclusive,
        economic.balance.chase_ceiling,
        impact_ceiling,
    )
    .map_err(|error| {
        economic_replay_error(format!("economic sized-plan replay failed: {error}"))
    })?;

    Ok(SourceBackedEconomicReplay {
        admission,
        sized,
        cash_before,
    })
}

pub(crate) fn replay_source_backed_economic<L>(
    economic: &EconomicPrepared,
    evidence_cutoff_unix_ms: i64,
    cash_before: CollateralAmount,
    lookup: L,
) -> Result<SourceBackedEconomicReplay, EconomicReplayError>
where
    L: FnMut(AppendReceipt) -> Result<RecordedEconomicSource, EconomicReplayError>,
{
    replay_source_backed_economic_with_live_binding(
        economic,
        evidence_cutoff_unix_ms,
        cash_before,
        None,
        lookup,
    )
}

fn recorded_impact_ceiling(best_ask: Price, cap_bps: i32) -> Result<Price, EconomicReplayError> {
    let cap_bps = u32::try_from(cap_bps)
        .ok()
        .filter(|cap| (1..=10_000).contains(cap))
        .ok_or_else(|| {
            economic_replay_error("EconomicPrepared price-impact cap is outside 1..=10000")
        })?;
    let numerator = 10_000u32
        .checked_add(cap_bps)
        .ok_or_else(|| economic_replay_error("EconomicPrepared price-impact cap overflow"))?;
    let ceiling = best_ask
        .0
        .checked_mul(Decimal::from(numerator))
        .and_then(|value| value.checked_div(Decimal::from(10_000u32)))
        .map(|value| value.min(Decimal::ONE))
        .ok_or_else(|| {
            economic_replay_error("EconomicPrepared price-impact arithmetic overflow")
        })?;
    Price::new(ceiling).map_err(|error| {
        economic_replay_error(format!(
            "EconomicPrepared price-impact ceiling is invalid: {error}"
        ))
    })
}

/// Rebuild the current-position mids from only the receipt inventory recorded on the admission.
/// Each receipt must be a valid strict Gamma acquisition that actually covered an open position;
/// the shared strict classifier remains the owner of outcome selection and freshness.
fn replay_live_risk_prices(
    positions: &[LivePositionRow],
    price_receipts: &[AppendReceipt],
    evaluated_at_unix_ms: i64,
    source_envelopes: &[EventEnvelope],
    source_receipts: Option<&SourceReceiptIndex>,
) -> Result<BTreeMap<(String, u16), MidPriceObservation>, RiskInputsUnavailable> {
    let ids = positions
        .iter()
        .map(|position| {
            let outcome =
                u16::try_from(position.outcome_id).map_err(|_| RiskInputsUnavailable::Overflow)?;
            Ok(MarketOutcomeId::new(
                MarketId(VenueMarketId(position.market_id.clone())),
                OutcomeId(outcome),
            ))
        })
        .collect::<Result<Vec<_>, RiskInputsUnavailable>>()?;
    futures::executor::block_on(replay_strict_risk_prices(
        &ids,
        price_receipts,
        evaluated_at_unix_ms,
        |receipt| {
            let envelope = match source_receipts {
                Some(index) => index.source_envelope(receipt).map_err(|error| {
                    RiskPriceReplayError::Insufficient(format!(
                        "risk price receipt lookup failed: {error}"
                    ))
                })?,
                None => source_envelopes
                    .iter()
                    .find(|envelope| {
                        envelope.seq == receipt.sequence && envelope.this_hash == receipt.this_hash
                    })
                    .cloned()
                    .ok_or_else(|| {
                        RiskPriceReplayError::Insufficient(
                            "risk price receipt is absent from the source prefix".to_owned(),
                        )
                    })?,
            };
            let received_unix_ms = i64::try_from(
                envelope
                    .received_at
                    .0
                    .unix_timestamp_nanos()
                    .div_euclid(1_000_000),
            )
            .map_err(|_| RiskPriceReplayError::Unavailable(RiskInputsUnavailable::Overflow))?;
            Ok(RecordedPriceAttempt {
                payload: envelope.payload,
                received_unix_ms,
                source_id: envelope.source_id.0,
                schema_version: envelope.schema_version,
                parser_version: envelope.parser_version,
                content_type: envelope.content_type,
            })
        },
    ))
    .map_err(|error| match error {
        RiskPriceReplayError::Unavailable(cause) => cause,
        RiskPriceReplayError::Insufficient(_) => RiskInputsUnavailable::PriceConflict,
    })
}

#[derive(Clone, Copy)]
struct LiveReplayEvidence<'a> {
    source_envelopes: &'a [EventEnvelope],
    source_receipts: Option<&'a SourceReceiptIndex>,
    paper_frames: &'a [ScannedPaperFrame],
    observation_pages: &'a LiveObservationPageIndex,
}

fn verify_replayed_live_risk_with_index(
    account_id: &AccountId,
    live_journal_path: &Path,
    preceding_events: &[LiveJournalEvent],
    admission: &pe_execution_core::LiveAdmissionEvaluationAudit,
    evidence: LiveReplayEvidence<'_>,
) -> Result<(), ProjectionReducerError> {
    let derived = derive_projection_rows_with_sources(
        account_id,
        preceding_events,
        evidence.source_envelopes,
    )?;
    // Pre-Baseline history remains audit-only and owns no financial state.
    if derived.baseline_sequence.is_none() {
        return Ok(());
    }
    if admission.economic.observation.is_none() {
        return Err(ProjectionReducerError::InvalidRiskEvidence);
    }
    let projection = admission
        .identity
        .fill_projection
        .as_deref()
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    let source_trade_id = SourceTradeId(
        projection
            .source_trade_id
            .clone()
            .ok_or(ProjectionReducerError::InvalidRiskEvidence)?,
    );
    let (continuation, correction) = evidence
        .observation_pages
        .get(&source_trade_id)
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    if continuation.facts.wallet.to_string() != projection.leader_wallet
        || continuation.facts.market_id.to_string() != projection.market_id
        || i64::from(continuation.facts.outcome_id.0) != projection.outcome_id
        || continuation.facts.side != Side::Buy
        || projection.side != "buy"
    {
        return Err(ProjectionReducerError::InvalidRiskEvidence);
    }
    let evaluated_at_unix_ms = admission.economic.risk.evaluated_at_unix_ms;
    let cash_before = CollateralAmount::from_decimal_exact(
        derived
            .economic_cash
            .ok_or(ProjectionReducerError::InvalidRiskEvidence)?,
    )
    .map_err(|_| ProjectionReducerError::Arithmetic)?;
    let source_backed_economic = replay_source_backed_economic_with_live_binding(
        &admission.economic,
        evaluated_at_unix_ms,
        cash_before,
        Some(LiveObservationBinding {
            account_id,
            identity: &admission.identity,
            continuation,
            correction: correction.as_ref(),
        }),
        |receipt| {
            let envelope = match evidence.source_receipts {
                Some(index) => index
                    .source_envelope(receipt)
                    .map_err(|error| EconomicReplayError(error.to_string()))?,
                None => evidence
                    .source_envelopes
                    .iter()
                    .find(|envelope| {
                        envelope.seq == receipt.sequence && envelope.this_hash == receipt.this_hash
                    })
                    .cloned()
                    .ok_or_else(|| {
                        EconomicReplayError(
                            "economic source receipt is absent from the source prefix".to_owned(),
                        )
                    })?,
            };
            let received_unix_ms = i64::try_from(
                envelope
                    .received_at
                    .0
                    .unix_timestamp_nanos()
                    .div_euclid(1_000_000),
            )
            .map_err(|_| EconomicReplayError("economic source clock overflow".to_owned()))?;
            Ok(RecordedEconomicSource {
                payload: envelope.payload,
                received_unix_ms,
                source_id: envelope.source_id.0,
                schema_version: envelope.schema_version,
                parser_version: envelope.parser_version,
                content_type: envelope.content_type,
            })
        },
    )
    .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let proposed_debit = source_backed_economic
        .proposed_debit()
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let now = OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(evaluated_at_unix_ms)
            .checked_mul(1_000_000)
            .ok_or(ProjectionReducerError::Arithmetic)?,
    )
    .map_err(|_| ProjectionReducerError::Arithmetic)?;
    let mids = replay_live_risk_prices(
        &derived.positions,
        &admission.economic.risk.price_receipts,
        evaluated_at_unix_ms,
        evidence.source_envelopes,
        evidence.source_receipts,
    )
    .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let paper_prefix = paper_prefix_at_financial_prefix(
        evidence.paper_frames,
        admission.economic.risk.financial_prefix,
        evaluated_at_unix_ms,
    )
    .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let paper_era = paper_era(paper_prefix.to_vec());
    let per_trade_cap_bps = admission.economic.risk.snapshot.per_trade_cap_bps;
    if !(1..=10_000).contains(&per_trade_cap_bps) {
        return Err(ProjectionReducerError::InvalidRiskEvidence);
    }
    let reconstructed = compose_live_risk_audit(
        account_id,
        live_journal_path,
        preceding_events,
        &derived,
        &paper_era,
        &mids,
        &admission.economic.risk.price_receipts,
        now,
        LiveRiskProposal {
            leader_wallet: Some(projection.leader_wallet.clone()),
            market_id: Some(projection.market_id.clone()),
            proposed_debit,
            per_trade_cap_bps,
        },
    )
    .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    if reconstructed != admission.economic.risk {
        return Err(ProjectionReducerError::InvalidRiskEvidence);
    }
    source_backed_economic
        .recompose(
            &admission.economic,
            reconstructed,
            admission.identity.config_hash.clone(),
        )
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    Ok(())
}

#[cfg(test)]
fn produced_decision_continuation(
    identity: &LiveOrderIdentity,
    observation: &ObservationEvidence,
    aggregate: pe_source_polymarket_public::ActivityAggregate,
    pages: Vec<(
        PageOccurrence,
        pe_source_polymarket_public::ReconciliationPageEvidence,
    )>,
) -> Result<DecisionContinuationV3, ProjectionReducerError> {
    let projection = identity
        .fill_projection
        .as_deref()
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    let source_trade_id = aggregate.group_id.key().clone();
    if projection.source_trade_id.as_deref() != Some(source_trade_id.0.as_str()) {
        return Err(ProjectionReducerError::InvalidRiskEvidence);
    }
    let wallet = aggregate.group_id.components().wallet;
    let source_epoch = aggregate.source_time.0.unix_timestamp();
    let provenance = match observation.provenance.as_str() {
        "activity_ws" => pe_copy_signal_engine::TradeProvenance::ActivityWs,
        "rest_poll" => pe_copy_signal_engine::TradeProvenance::RestPoll,
        _ => return Err(ProjectionReducerError::InvalidRiskEvidence),
    };
    let page_occurrences = pages
        .iter()
        .map(|(occurrence, _)| occurrence.clone())
        .collect::<Vec<_>>();
    let mut page_evidence = pages
        .into_iter()
        .map(|(_, evidence)| evidence)
        .collect::<Vec<_>>();
    page_evidence.sort_by_key(|page| {
        let bounds = page
            .bounds
            .unwrap_or(pe_source_polymarket_public::ActivityRequestBounds {
                start: None,
                end: i64::MIN,
            });
        (bounds.end, bounds.start, page.offset)
    });
    let fixed_end = page_evidence
        .iter()
        .filter_map(|page| page.bounds.map(|bounds| bounds.end))
        .max()
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    let commitment_payload = crate::bucket_commit::activity_read_commitment_payload(
        wallet,
        fixed_end,
        &page_occurrences,
        &page_evidence,
    )
    .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let read_commitment = AppendReceipt {
        sequence: pe_core_types::EventSeq(
            observation
                .complete_bound_receipt
                .sequence
                .0
                .checked_add(1)
                .ok_or(ProjectionReducerError::InvalidRiskEvidence)?,
        ),
        this_hash: blake3::hash(&commitment_payload),
    };
    let configuration = crate::runtime_config::RuntimeConfig::from_service_config(
        &crate::config::ServiceConfig::default(),
    );
    let decision_inputs_json = serde_json::json!({
        "fixed_end": fixed_end,
        "pages": page_evidence,
    })
    .to_string();
    let temp = tempfile::tempdir().map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let paper_state = Arc::new(
        PaperStateDb::open(&temp.path().join("paper.db"))
            .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?,
    );
    let mut engine = crate::bucket_commit::BucketCommitEngine::load(
        Arc::clone(&paper_state),
        pe_position_ledger::PositionLedger::new(),
    )
    .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let anchor_cutoff = source_epoch.saturating_sub(1);
    paper_state
        .set_cursor(&wallet, anchor_cutoff)
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let captured = crate::position_seeder::ledger_capture(engine.ledger(), &paper_state, wallet)
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    engine
        .install_anchors(&[crate::position_seeder::AnchorInstall {
            wallet,
            balances: Vec::new(),
            cutoff: anchor_cutoff,
            proof: crate::position_seeder::AnchorProof {
                positions_proof_hash: "fixture-empty".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "fixture".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix: anchor_cutoff,
            },
            expected: crate::position_seeder::AnchorExpectation {
                ledger_hash: captured.hash,
                cursor: captured.cursor,
                anchor_seq: captured.anchor_seq,
                coverage_generation: captured.coverage_generation,
            },
        }])
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let context = crate::bucket_commit::BucketDecisionContext {
        applied_configuration: configuration,
        decision_inputs_json,
        page_occurrences,
        observed_source_receipts: if provenance
            == pe_copy_signal_engine::TradeProvenance::ActivityWs
        {
            HashMap::from([(source_trade_id.clone(), observation.source_receipt)])
        } else {
            HashMap::new()
        },
        reconstruction_quality: ReconstructionQuality::new(100)
            .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?,

        read_commitment: Some(read_commitment),
        signal_config: pe_copy_signal_engine::SignalConfig::default(),
        copy_eligible: true,
        bracket_commit: false,
        recorded_at_unix: source_epoch,
        observation_provenance: HashMap::from([(source_trade_id.clone(), provenance)]),
        no_copy_dispositions: HashMap::new(),
        identity_overrides: HashMap::new(),
        identity_unresolved: HashSet::new(),
        history_status: Some(pe_paper_state::WalletHistoryStatusRecord {
            wallet,
            complete: true,
            proof_json: serde_json::json!({"fixture": "complete"}).to_string(),
            updated_at_unix: source_epoch,
        }),
    };
    let result = engine
        .commit(
            vec![aggregate],
            &context,
            crate::bucket_commit::FrozenDecisionBasis {
                win_rate_p: Probability::new(Decimal::ONE)
                    .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?,
                bankroll: Decimal::ONE,
            },
        )
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    if result.pending.as_slice() != [source_trade_id.clone()] {
        return Err(ProjectionReducerError::InvalidRiskEvidence);
    }
    let row = paper_state
        .decision_pending_for(&source_trade_id)
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    DecisionContinuationV3::from_durable(&row)
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)
}

#[cfg(test)]
fn produced_observation_index(
    admission: &pe_execution_core::LiveAdmissionEvaluationAudit,
    source_envelopes: &[EventEnvelope],
) -> Result<LiveObservationPageIndex, ProjectionReducerError> {
    let observation = admission
        .economic
        .observation
        .as_ref()
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    let projection = admission
        .identity
        .fill_projection
        .as_deref()
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    let source_trade_id = SourceTradeId(
        projection
            .source_trade_id
            .clone()
            .ok_or(ProjectionReducerError::InvalidRiskEvidence)?,
    );
    let bound = source_envelopes
        .iter()
        .find(|source| {
            source.seq == observation.complete_bound_receipt.sequence
                && source.this_hash == observation.complete_bound_receipt.this_hash
        })
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    let wallet = WalletAddress::from_hex(&projection.leader_wallet)
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let raw_rows = serde_json::from_slice::<Vec<serde_json::Value>>(&bound.payload)
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let source_epoch = raw_rows
        .iter()
        .filter_map(|row| {
            row.get("timestamp").and_then(|timestamp| {
                timestamp
                    .as_i64()
                    .or_else(|| timestamp.as_str().and_then(|value| value.parse().ok()))
            })
        })
        .max()
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    let request_url = pe_source_polymarket_public::PolymarketEndpoint::UserPositionActivityPage {
        user: wallet.to_string(),
        end: source_epoch,
        start: Some(source_epoch),
        offset: 0,
    }
    .url("https://data-api.polymarket.com");
    let raw_hash = bound.raw_payload_hash.to_hex().to_string();
    let page = pe_source_polymarket_public::ReconciliationPageEvidence {
        request_url: request_url.clone(),
        bounds: Some(pe_source_polymarket_public::ActivityRequestBounds {
            start: Some(source_epoch.saturating_sub(1)),
            end: source_epoch,
        }),
        partition: None,
        offset: 0,
        row_count: u32::try_from(raw_rows.len())
            .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?,
        canonical_page_hash: pe_source_polymarket_public::canonical_page_hash(&bound.payload)
            .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?,
        raw_page_hash: raw_hash.clone(),
        received_at: bound.received_at.clone(),
        schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
        parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
    };
    let context = pe_source_polymarket_public::ActivityParseContext {
        source_id: bound.source_id.clone(),
        observed_at: bound.observed_at.clone(),
        received_at: bound.received_at.clone(),
        transport: pe_source_polymarket_public::ActivityTransport::Replay,
    };
    let window =
        pe_source_polymarket_public::parse_activity_response(&bound.payload, wallet, &context)
            .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?;
    let aggregate = window
        .aggregates()
        .map_err(|_| ProjectionReducerError::InvalidRiskEvidence)?
        .into_iter()
        .find(|aggregate| aggregate.group_id.key() == &source_trade_id)
        .ok_or(ProjectionReducerError::InvalidRiskEvidence)?;
    let continuation = produced_decision_continuation(
        &admission.identity,
        observation,
        aggregate,
        vec![(
            PageOccurrence {
                request_url,
                raw_hash,
                receipt: observation.complete_bound_receipt,
            },
            page,
        )],
    );
    let continuation = continuation?;
    // This legacy log harness retains historical schema-2 page envelopes.
    let continuation =
        if bound.schema_version == pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION {
            DecisionContinuationV3::new(
                continuation.facts,
                continuation.observed_source_receipt,
                continuation.page_occurrences,
                None,
            )
        } else {
            continuation
        };
    Ok(HashMap::from([(source_trade_id, (continuation, None))]))
}

#[cfg(test)]
fn verify_replayed_live_risk(
    account_id: &AccountId,
    live_journal_path: &Path,
    preceding_events: &[LiveJournalEvent],
    admission: &pe_execution_core::LiveAdmissionEvaluationAudit,
    source_envelopes: &[EventEnvelope],
    paper_frames: &[ScannedPaperFrame],
) -> Result<(), ProjectionReducerError> {
    let observation_pages = produced_observation_index(admission, source_envelopes)?;
    verify_replayed_live_risk_with_index(
        account_id,
        live_journal_path,
        preceding_events,
        admission,
        LiveReplayEvidence {
            source_envelopes,
            source_receipts: None,
            paper_frames,
            observation_pages: &observation_pages,
        },
    )
}

fn verify_replayed_live_risks(
    account_id: &AccountId,
    live_journal_path: &Path,
    events: &[LiveJournalEvent],
    source_envelopes: &[EventEnvelope],
    source_receipts: Option<&SourceReceiptIndex>,
    paper_frames: &[ScannedPaperFrame],
    observation_pages: &LiveObservationPageIndex,
) -> Result<(), ProjectionReducerError> {
    for (event_index, event) in events.iter().enumerate() {
        let LiveJournalPayload::AdmissionEvaluated(admission) = &event.payload else {
            continue;
        };
        verify_replayed_live_risk_with_index(
            account_id,
            live_journal_path,
            &events[..event_index],
            admission,
            LiveReplayEvidence {
                source_envelopes,
                source_receipts,
                paper_frames,
                observation_pages,
            },
        )?;
    }
    Ok(())
}

fn live_latency_switch(
    events: &[LiveJournalEvent],
    now: OffsetDateTime,
    seed: crate::risk_inputs::LatencyHysteresisSeed,
) -> Result<bool, RiskInputsUnavailable> {
    crate::risk_inputs::replayed_live_latency_switch(events, now.unix_timestamp(), seed)
}

fn build_order_identity(
    seed: &DispatchSeedRow,
    target: &DispatchTargetRow,
    signal: &LeaderSignal,
    admission: &pe_execution_core::LiveAdmissionArtifact,
    plan: &pe_venue_polymarket::LadderPlan,
    book_receipt: AppendReceipt,
    strategy: &pe_strategy_winner_follow::WinnerFollowConfig,
) -> Result<LiveOrderIdentity, FanoutError> {
    validate_live_market_identity(
        &signal.market_id.to_string(),
        &admission.market.condition_id,
    )?;
    let asks = plan
        .used_asks
        .iter()
        .map(|ask| (ask.price, ask.shares))
        .collect::<Vec<_>>();
    let quote_id = hash_json(&(
        "prediction-edge/live-quote/v1",
        &asks,
        plan.best_ask,
        plan.limit_price,
        plan.shares,
        plan.expected_shares()
            .map_err(|error| FanoutError::Signal(error.to_string()))?,
        plan.expected_spend()
            .map_err(|error| FanoutError::Signal(error.to_string()))?,
        plan.vwap(),
        plan.worst_case_debit,
        book_receipt,
    ))?;
    let config_hash = hash_json(&("prediction-edge/live-config/v1", strategy))?;
    let evidence_hashes = vec![
        admission.receipts.gamma.this_hash.to_hex().to_string(),
        admission.receipts.clob_long.this_hash.to_hex().to_string(),
        admission
            .receipts
            .clob_compact
            .this_hash
            .to_hex()
            .to_string(),
        book_receipt.this_hash.to_hex().to_string(),
        quote_id.clone(),
    ];
    let decision_hash = hash_json(&(
        "prediction-edge/live-decision/v1",
        signal,
        &target.account_id,
        &config_hash,
        &quote_id,
        &evidence_hashes,
    ))?;
    Ok(LiveOrderIdentity {
        dispatch_id: seed.dispatch_id.clone(),
        idempotency_key: LiveOrderIdentity::idempotency_key_for(
            &seed.dispatch_id,
            &AccountId::new(&target.account_id)
                .map_err(|error| FanoutError::Signal(error.to_string()))?,
        ),
        quote_id,
        config_hash,
        decision_hash,
        evidence_hashes,
        fill_projection: Some(Box::new(LiveFillProjectionIdentity {
            leader_wallet: signal.leader.to_string(),
            source_trade_id: Some(seed.source_trade_id.clone()),
            market_id: signal.market_id.to_string(),
            outcome_id: i64::from(signal.outcome_id.0),
            side: "buy".to_owned(),
        })),
        schema_version: 1,
        parser_version: 1,
    })
}

fn validate_live_market_identity(
    market_id: &str,
    condition_id: &PolymarketConditionId,
) -> Result<(), FanoutError> {
    if market_id == condition_id.0 {
        Ok(())
    } else {
        Err(FanoutError::Signal(
            "live market ID disagrees with admitted condition ID".to_owned(),
        ))
    }
}

fn hash_json<T: serde::Serialize + ?Sized>(value: &T) -> Result<String, FanoutError> {
    serde_json::to_vec(value)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .map_err(|error| FanoutError::Signal(error.to_string()))
}

struct TargetTransition {
    state: &'static str,
    reason: Option<&'static str>,
    freeze: bool,
}

fn outcome_transition(outcome: &LiveOrderOutcome) -> TargetTransition {
    TargetTransition {
        state: outcome.dispatch_state(),
        reason: outcome.terminal_reason(),
        freeze: matches!(outcome, LiveOrderOutcome::Ambiguous { .. }),
    }
}

fn persist_outcome(
    state: &FanoutState,
    target: &DispatchTargetRow,
    outcome: &LiveOrderOutcome,
    now: OffsetDateTime,
) -> Result<(), FanoutError> {
    let transition = outcome_transition(outcome);
    state.config.paper_state.set_dispatch_target_state(
        &target.dispatch_id,
        &target.account_id,
        transition.state,
        transition.reason,
        now.unix_timestamp(),
    )?;
    Ok(())
}

fn terminalize(
    state: &FanoutState,
    target: &DispatchTargetRow,
    reason: &str,
    now: OffsetDateTime,
) -> Result<(), FanoutError> {
    state.config.paper_state.set_dispatch_target_state(
        &target.dispatch_id,
        &target.account_id,
        "terminal",
        Some(reason),
        now.unix_timestamp(),
    )?;
    Ok(())
}

fn refusal_is_transient(outcome: &LiveOrderOutcome) -> bool {
    matches!(
        outcome,
        LiveOrderOutcome::Refused {
            reason: LiveAdmissionRefusal::AccountStateUnavailable(_)
        }
    )
}

fn admission_error_is_transient(error: &LiveVenueAdapterError) -> bool {
    matches!(
        error,
        LiveVenueAdapterError::MarketTransport(_)
            | LiveVenueAdapterError::MarketStatus(429 | 500..=599)
            | LiveVenueAdapterError::SourceLogClosed
    )
}

fn admission_terminal_reason(error: &LiveVenueAdapterError) -> &'static str {
    match error {
        LiveVenueAdapterError::Outcome => "outcome_not_binary",
        LiveVenueAdapterError::MarketStatus(_) => "market_evidence_http_rejected",
        LiveVenueAdapterError::MarketValidation(_) => "market_evidence_invalid",
        LiveVenueAdapterError::Client(_)
        | LiveVenueAdapterError::MarketTransport(_)
        | LiveVenueAdapterError::Redemption(_)
        | LiveVenueAdapterError::SourceLogClosed => "market_evidence_invalid",
    }
}

fn ladder_terminal_reason(error: &LadderError) -> &'static str {
    match error {
        LadderError::NothingAffordable => "nothing_affordable",
        LadderError::BelowBandAsk => "below_live_price_band",
        LadderError::InsufficientDepth => "insufficient_live_depth",
        LadderError::BelowMinimum => "below_minimum_order",
        LadderError::CapExceeded => "cap_exceeded",
        LadderError::NoEdge => "no_edge",
        LadderError::KellySizing => "kelly_sizing_invalid",
        LadderError::Fee(_) => "fee_schedule_invalid",
        LadderError::Amount => "ladder_amount_invalid",
    }
}

fn winner_follow_terminal_reason(error: &WinnerFollowError) -> &'static str {
    match error {
        WinnerFollowError::ShadowMode => "shadow_mode",
        WinnerFollowError::FlipNotApproved => "flip_not_approved",
        WinnerFollowError::NoEdge => "no_edge",
        WinnerFollowError::Blocked(_) => "risk_blocked",
        WinnerFollowError::RiskInputsUnavailable => "risk_inputs_unavailable",
        WinnerFollowError::KellySizing(_) => "kelly_sizing_invalid",
    }
}

#[derive(Default)]
pub(crate) struct ProjectionDerivation {
    pub(crate) fills: Vec<LiveFillRow>,
    pub(crate) positions: Vec<LivePositionRow>,
    pub(crate) custody_positions: Vec<CanonicalPositionAudit>,
    pub(crate) reserved: Decimal,
    pub(crate) economic_cash: Option<Decimal>,
    pub(crate) receivable: Decimal,
    pub(crate) latest_free_collateral: Option<Decimal>,
    pub(crate) latest_reconciled_at: Option<String>,
    pub(crate) baseline_equity: Option<CollateralAmount>,
    pub(crate) baseline_cutoff_unix: Option<i64>,
    pub(crate) baseline_sequence: Option<u64>,
    pub(crate) account_binding: Option<LiveAccountBindingAudit>,
    pub(crate) daily_marks: BTreeMap<i64, CollateralAmount>,
    pub(crate) realized_closes: Vec<(i64, Decimal)>,
    pub(crate) validated_prepared_sequences: BTreeSet<u64>,
    pub(crate) pending_approved_admission_keys: BTreeSet<String>,
    open_exposures: Vec<LiveOpenExposure>,
    receivable_by_condition: BTreeMap<String, CollateralAmount>,
}

#[derive(Debug, Clone)]
struct LiveOpenExposure {
    leader_wallet: String,
    market_id: String,
    debit: CollateralAmount,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProjectionReducerError {
    #[error("a second Baseline mark exists in the same account era")]
    DuplicateBaseline,
    #[error("Baseline must bind empty venue inventory and exact zero-position equity")]
    InvalidBaseline,
    #[error("retained authenticated-account evidence is missing, malformed, or inconsistent")]
    InvalidAccountEvidence,
    #[error(
        "recorded risk decision disagrees with deterministic risk evaluation or is not approved"
    )]
    InvalidRiskDecision,
    #[error(
        "recorded risk snapshot, financial prefix, price receipts, or decision disagrees with reconstructed evidence"
    )]
    InvalidRiskEvidence,
    #[error("OrderPrepared has no single matching replay-approved admission")]
    MissingApprovedAdmission,
    #[error("retained complete-position evidence is missing, malformed, or inconsistent")]
    InvalidPositionEvidence,
    #[error("retained historical-price evidence is missing, malformed, or inconsistent")]
    InvalidPriceEvidence,
    #[error("duplicate prepared/finalized identity conflicts with prior journal content")]
    IdentityConflict,
    #[error("OrderFillFinalized has no matching prepared record")]
    MissingPrepared,
    #[error("OrderFillFinalized prepared sequence is wrong")]
    PreparedSequenceMismatch,
    #[error("OrderFillFinalized violates its prepared order economics or identity")]
    InvalidFinalizedFill,
    #[error("OrderFillFinalized retained Polygon evidence is invalid")]
    InvalidFinalityEvidence,
    #[error("Polygon finality evidence conflicts with an earlier immutable observation")]
    FinalityObservationConflict,
    #[error("OrderFillFinalized appeared after a terminal Polygon finality conflict")]
    FinalityConflictFrozen,
    #[error("OrderFillFinalized appeared after its condition was finalized")]
    FillAfterResolution,
    #[error("a ResolutionFinalized condition still has a nonterminal prepared order")]
    ResolutionWithPendingOrder,
    #[error("canonical payout vector is invalid")]
    InvalidPayout,
    #[error("ResolutionFinalized source evidence or admission mapping is invalid")]
    InvalidResolutionEvidence,
    #[error("finalized financial arithmetic overflowed")]
    Arithmetic,
    #[error("finalized fill is missing its journal-owned projection identity")]
    MissingProjectionIdentity,
    #[error("finalized principal/quantity does not form a valid fill price")]
    InvalidFillPrice,
    #[error("custody venue inventory disagrees with journal-derived inventory")]
    CustodyInventoryMismatch,
    #[error("authenticated collateral disagrees with economic cash and receivable")]
    CashMismatch,
    #[error("custody reconciliation has no matching confirmed redemption attempt")]
    CustodyWithoutConfirmedRedemption,
    #[error("portfolio mark prices are missing, duplicated, or inconsistent")]
    InvalidMarkPrices,
    #[error("portfolio mark equity disagrees with finalized economics")]
    InvalidMarkEquity,
    #[error("a Daily portfolio mark repeats a cutoff in the same account era")]
    DuplicateDailyMark,
    #[error("a Daily portfolio mark is not a causal UTC-midnight observation")]
    InvalidDailyMark,
}

#[cfg(test)]
pub(crate) fn derive_projection_rows(
    account_id: &AccountId,
    events: &[LiveJournalEvent],
) -> Result<ProjectionDerivation, ProjectionReducerError> {
    derive_projection_rows_with_sources(account_id, events, &[])
}

pub(crate) fn derive_projection_rows_with_sources(
    account_id: &AccountId,
    events: &[LiveJournalEvent],
    source_envelopes: &[EventEnvelope],
) -> Result<ProjectionDerivation, ProjectionReducerError> {
    if events.iter().any(|event| event.account_id != *account_id) {
        return Err(ProjectionReducerError::IdentityConflict);
    }
    let first_baseline_index =
        pe_execution_core::live_journal::first_account_baseline_index(events, account_id)
            .unwrap_or(events.len());
    let mut baseline_seen = false;
    let mut economic_cash = None;
    let mut fills = BTreeMap::<String, (OrderFillFinalizedAudit, LiveFillRow)>::new();
    let mut prepared_orders =
        BTreeMap::<String, (u64, Box<pe_execution_core::LiveOrderPreparedAudit>, bool)>::new();
    let mut evaluated_admissions = BTreeSet::<String>::new();
    let mut approved_admissions =
        BTreeMap::<String, Box<pe_execution_core::LiveAdmissionEvaluationAudit>>::new();
    let mut reservations = BTreeMap::<String, Decimal>::new();
    let mut resolutions = BTreeMap::<String, pe_execution_core::ResolutionFinalizedAudit>::new();
    let mut receivable_by_condition = BTreeMap::<String, CollateralAmount>::new();
    let mut latest_free_collateral = None;
    let mut latest_reconciled_at = None;
    let mut daily_marks = BTreeMap::new();
    let mut baseline_equity = None;
    let mut baseline_cutoff_unix = None;
    let mut baseline_sequence = None;
    let mut realized_closes = Vec::new();
    let mut custody_facts = HashMap::<
        RedemptionAttemptIdentity,
        pe_execution_core::RedemptionCustodyReconciledAudit,
    >::new();
    let mut polygon_finality = BTreeMap::<String, PolygonFinalityAuditState>::new();
    let mut terminal_reconciliations = BTreeMap::<String, Box<LiveOrderReconciliationAudit>>::new();
    let mut matched_transaction_hashes = BTreeMap::<String, BTreeSet<String>>::new();
    let mut current_account_binding = None::<LiveAccountBindingAudit>;

    for (event_index, event) in events.iter().enumerate().skip(first_baseline_index) {
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &event.payload
            && mark.kind == MarkKind::Baseline
        {
            if baseline_seen {
                return Err(ProjectionReducerError::DuplicateBaseline);
            }
            verify_account_binding(&mark.account_binding, account_id)?;
            verify_account_state(&mark.account_state, &mark.account_binding)?;
            let raw_positions = replay_position_evidence(
                &mark.venue_position_evidence,
                source_envelopes,
                &mark.account_binding,
                events[..event_index]
                    .iter()
                    .filter_map(|prior| match &prior.payload {
                        LiveJournalPayload::OrderPrepared(prepared) => Some(prepared.as_ref()),
                        _ => None,
                    }),
            )?;
            if mark.venue_position_evidence.requested_wallet != mark.account_binding.custody_wallet
            {
                return Err(ProjectionReducerError::InvalidPositionEvidence);
            }
            if canonical_positions(&mark.venue_positions)? != canonical_positions(&raw_positions)?
                || !mark.venue_positions.is_empty()
                || !raw_positions.is_empty()
                || !mark.marked_positions.is_empty()
                || !mark.prices.is_empty()
                || mark.equity == CollateralAmount::ZERO
                || mark.equity != mark.account_state.collateral_balance
            {
                return Err(ProjectionReducerError::InvalidBaseline);
            }
            baseline_seen = true;
            baseline_equity = Some(mark.equity);
            baseline_cutoff_unix = Some(mark.cutoff_unix);
            baseline_sequence = Some(event.seq);
            economic_cash = Some(mark.equity.to_decimal());
            current_account_binding = Some(mark.account_binding.clone());
            latest_free_collateral = Some(mark.account_state.collateral_balance.to_decimal());
            latest_reconciled_at = format_observed_at(mark.account_state.observed_at);
            continue;
        }
        if !baseline_seen {
            // Everything before the first synchronized Baseline is retained audit only.
            continue;
        }
        match &event.payload {
            LiveJournalPayload::OrderPrepared(order) => {
                let key = order.identity.dispatch_id.clone();
                let admission = approved_admissions
                    .remove(&key)
                    .ok_or(ProjectionReducerError::MissingApprovedAdmission)?;
                if admission.identity != order.identity
                    || admission.frozen_binding != order.frozen_binding
                    || admission.economic != order.economic
                    || admission.account_state.as_ref() != Some(&order.account_state)
                {
                    return Err(ProjectionReducerError::IdentityConflict);
                }
                if let Some((existing_seq, existing, _)) = prepared_orders.get(&key) {
                    if *existing_seq != event.seq || existing.as_ref() != order.as_ref() {
                        return Err(ProjectionReducerError::IdentityConflict);
                    }
                } else {
                    reservations.insert(
                        key.clone(),
                        order.economic.balance.worst_case_debit.to_decimal(),
                    );
                    prepared_orders.insert(key, (event.seq, order.clone(), false));
                }
            }
            LiveJournalPayload::AdmissionEvaluated(admission) => {
                let key = admission.identity.dispatch_id.clone();
                if !evaluated_admissions.insert(key.clone()) {
                    return Err(ProjectionReducerError::IdentityConflict);
                }
                let binding = current_account_binding
                    .as_ref()
                    .ok_or(ProjectionReducerError::InvalidAccountEvidence)?;
                verify_live_admission_evaluation(admission, event.timestamp, Some(binding))
                    .map_err(|error| match error {
                        LiveAdmissionEvaluationError::InvalidRiskDecision => {
                            ProjectionReducerError::InvalidRiskDecision
                        }
                        LiveAdmissionEvaluationError::InvalidAccountEvidence
                        | LiveAdmissionEvaluationError::Journal(_) => {
                            ProjectionReducerError::InvalidAccountEvidence
                        }
                    })?;
                if admission.verdict == pe_execution_core::LiveAdmissionVerdict::Approved {
                    approved_admissions.insert(key, admission.clone());
                }
            }
            LiveJournalPayload::OrderPreparationFailed(failed) => {
                let key = &failed.identity.dispatch_id;
                if let Some(admission) = approved_admissions.remove(key) {
                    if admission.identity != failed.identity {
                        return Err(ProjectionReducerError::IdentityConflict);
                    }
                } else {
                    let (_, prepared, terminal) = prepared_orders
                        .get_mut(key)
                        .ok_or(ProjectionReducerError::MissingApprovedAdmission)?;
                    if prepared.identity != failed.identity || *terminal {
                        return Err(ProjectionReducerError::IdentityConflict);
                    }
                    *terminal = true;
                    reservations.remove(key);
                }
            }
            LiveJournalPayload::OrderPosted(posted) => {
                let Some((prepared_seq, prepared, terminal)) =
                    prepared_orders.get(&posted.identity.dispatch_id)
                else {
                    return Err(ProjectionReducerError::MissingPrepared);
                };
                if *terminal
                    || !prepared_order_fact_matches(
                        *prepared_seq,
                        &prepared.identity,
                        &prepared.prepared.order_hash,
                        PreparedOrderFact::Posted(posted),
                    )
                {
                    return Err(ProjectionReducerError::IdentityConflict);
                }
            }
            LiveJournalPayload::OrderReconciled(reconciled) => {
                let key = reconciled.identity.dispatch_id.clone();
                let Some((prepared_seq, prepared, terminal)) = prepared_orders.get_mut(&key) else {
                    return Err(ProjectionReducerError::MissingPrepared);
                };
                if *terminal {
                    if terminal_reconciliations
                        .get(&key)
                        .is_some_and(|prior| prior.as_ref() == reconciled.as_ref())
                    {
                        continue;
                    }
                    return Err(ProjectionReducerError::IdentityConflict);
                }
                if !prepared_order_fact_matches(
                    *prepared_seq,
                    &prepared.identity,
                    &prepared.prepared.order_hash,
                    PreparedOrderFact::Reconciled(reconciled),
                ) {
                    return Err(ProjectionReducerError::IdentityConflict);
                }
                if let LiveJournalOrderOutcome::Matched {
                    transaction_hashes, ..
                } = &reconciled.outcome
                {
                    matched_transaction_hashes
                        .entry(key.clone())
                        .or_default()
                        .extend(transaction_hashes.iter().cloned());
                }
                if reconciled.source == LiveReconciliationSource::PolygonFinality {
                    let transaction_hashes = matched_transaction_hashes
                        .get(&key)
                        .map(|hashes| hashes.iter().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();
                    let state = polygon_finality.entry(key.clone()).or_default();
                    let immutable = verify_polygon_reconciliation(
                        prepared,
                        &transaction_hashes,
                        reconciled,
                        &state.immutable_receipts,
                    )
                    .map_err(|_| ProjectionReducerError::InvalidFinalityEvidence)?;
                    match &reconciled.outcome {
                        LiveJournalOrderOutcome::FinalityPending { .. } => {
                            if matches!(
                                state.phase,
                                PolygonFinalityPhase::Conflict | PolygonFinalityPhase::Finalized
                            ) {
                                return Err(ProjectionReducerError::FinalityObservationConflict);
                            }
                            state.phase = PolygonFinalityPhase::Pending;
                            state.immutable_receipts.extend(immutable);
                        }
                        LiveJournalOrderOutcome::FinalityConflict { .. } => {
                            if matches!(
                                state.phase,
                                PolygonFinalityPhase::Conflict | PolygonFinalityPhase::Finalized
                            ) {
                                return Err(ProjectionReducerError::FinalityObservationConflict);
                            }
                            state.phase = PolygonFinalityPhase::Conflict;
                            *terminal = true;
                            terminal_reconciliations.insert(key.clone(), reconciled.clone());
                        }
                        _ => return Err(ProjectionReducerError::InvalidFinalityEvidence),
                    }
                }
                if matches!(
                    reconciled.outcome,
                    LiveJournalOrderOutcome::Killed { .. }
                        | LiveJournalOrderOutcome::Rejected { .. }
                ) {
                    reservations.remove(&reconciled.identity.dispatch_id);
                    *terminal = true;
                    terminal_reconciliations.insert(key, reconciled.clone());
                }
            }
            LiveJournalPayload::OrderFillFinalized(finalized) => {
                let key = finalized.identity.dispatch_id.clone();
                let Some((prepared_seq, prepared_audit, terminal)) = prepared_orders.get_mut(&key)
                else {
                    return Err(ProjectionReducerError::MissingPrepared);
                };
                if !prepared_order_fact_matches(
                    *prepared_seq,
                    &prepared_audit.identity,
                    &prepared_audit.prepared.order_hash,
                    PreparedOrderFact::Finalized(finalized),
                ) {
                    return Err(ProjectionReducerError::PreparedSequenceMismatch);
                }
                if polygon_finality
                    .get(&key)
                    .is_some_and(|state| state.phase == PolygonFinalityPhase::Conflict)
                {
                    return Err(ProjectionReducerError::FinalityConflictFrozen);
                }
                if *terminal && !fills.contains_key(&key) {
                    return Err(ProjectionReducerError::InvalidFinalizedFill);
                }
                let finality_state = polygon_finality.entry(key.clone()).or_default();
                let authenticated_transaction_hashes = matched_transaction_hashes
                    .get(&key)
                    .cloned()
                    .unwrap_or_default();
                verify_polygon_finalized(
                    prepared_audit,
                    &authenticated_transaction_hashes,
                    finalized,
                    &finality_state.immutable_receipts,
                )
                .map_err(|_| ProjectionReducerError::InvalidFinalityEvidence)?;
                finality_state.phase = PolygonFinalityPhase::Finalized;
                if resolutions.contains_key(&prepared_audit.prepared.condition_id.0) {
                    return Err(ProjectionReducerError::FillAfterResolution);
                }
                let projection = finalized
                    .identity
                    .fill_projection
                    .as_deref()
                    .ok_or(ProjectionReducerError::MissingProjectionIdentity)?;
                let fill_price = finalized
                    .principal
                    .to_decimal()
                    .checked_div(finalized.quantity.to_decimal())
                    .and_then(|price| Price::new(price).ok())
                    .ok_or(ProjectionReducerError::InvalidFillPrice)?
                    .0;
                let event_seq =
                    i64::try_from(event.seq).map_err(|_| ProjectionReducerError::Arithmetic)?;
                let row = LiveFillRow {
                    account_id: account_id.as_str().to_owned(),
                    idempotency_key: finalized.identity.idempotency_key.clone(),
                    leader_wallet: projection.leader_wallet.clone(),
                    source_trade_id: projection.source_trade_id.clone(),
                    market_id: projection.market_id.clone(),
                    outcome_id: projection.outcome_id,
                    side: projection.side.clone(),
                    contracts: finalized.quantity.to_decimal(),
                    fill_price,
                    entry_unix: Some(event.timestamp.unix_timestamp()),
                    event_seq,
                };
                if let Some((existing, existing_row)) = fills.get(&key) {
                    if !same_finalized_fill(existing, finalized)
                        || existing_row.idempotency_key != row.idempotency_key
                    {
                        return Err(ProjectionReducerError::IdentityConflict);
                    }
                    continue;
                }
                let debit = finalized
                    .principal
                    .to_decimal()
                    .checked_add(finalized.fee.to_decimal())
                    .ok_or(ProjectionReducerError::Arithmetic)?;
                economic_cash = Some(
                    economic_cash
                        .ok_or(ProjectionReducerError::InvalidBaseline)?
                        .checked_sub(debit)
                        .ok_or(ProjectionReducerError::Arithmetic)?,
                );
                reservations.remove(&key);
                *terminal = true;
                fills.insert(key, (finalized.as_ref().clone(), row));
            }
            LiveJournalPayload::ResolutionFinalized(resolution) => {
                let payout = BinaryPayoutVector::from_canonical_json(
                    &resolution.payout_by_outcome_index_json,
                )
                .map_err(|_| ProjectionReducerError::InvalidPayout)?;
                let condition = resolution.condition_id.0.clone();
                verify_resolution_evidence(
                    resolution,
                    source_envelopes,
                    prepared_orders.values().map(|(_, order, _)| order.as_ref()),
                )?;
                if let Some(existing) = resolutions.get(&condition) {
                    if existing != resolution.as_ref() {
                        return Err(ProjectionReducerError::IdentityConflict);
                    }
                    continue;
                }
                if prepared_orders.values().any(|(_, order, terminal)| {
                    order.prepared.condition_id.0 == condition && !*terminal
                }) {
                    return Err(ProjectionReducerError::ResolutionWithPendingOrder);
                }
                let credit = resolution_credit(&condition, &payout, &prepared_orders, &fills)?;
                let closed_cost =
                    fills
                        .iter()
                        .try_fold(CollateralAmount::ZERO, |total, (key, (fill, _))| {
                            let Some((_, order, _)) = prepared_orders.get(key) else {
                                return Err(ProjectionReducerError::MissingPrepared);
                            };
                            if order.prepared.condition_id.0 != condition {
                                return Ok(total);
                            }
                            let debit = fill
                                .principal
                                .checked_add(fill.fee)
                                .map_err(|_| ProjectionReducerError::Arithmetic)?;
                            total
                                .checked_add(debit)
                                .map_err(|_| ProjectionReducerError::Arithmetic)
                        })?;
                let realized = credit
                    .to_decimal()
                    .checked_sub(closed_cost.to_decimal())
                    .ok_or(ProjectionReducerError::Arithmetic)?;
                realized_closes.push((event.timestamp.unix_timestamp(), realized));
                economic_cash = Some(
                    economic_cash
                        .ok_or(ProjectionReducerError::InvalidBaseline)?
                        .checked_add(credit.to_decimal())
                        .ok_or(ProjectionReducerError::Arithmetic)?,
                );
                if credit != CollateralAmount::ZERO {
                    receivable_by_condition.insert(condition.clone(), credit);
                }
                resolutions.insert(condition, resolution.as_ref().clone());
            }
            LiveJournalPayload::RedemptionCustodyReconciled(custody) => {
                if let Some(existing) = custody_facts.get(&custody.identity) {
                    if existing != custody.as_ref() {
                        return Err(ProjectionReducerError::IdentityConflict);
                    }
                    continue;
                }
                let attempts = reconstruct_redemption_attempts(&events[..event_index]);
                if !matches!(
                    attempts
                        .get(&custody.identity)
                        .map(|attempt| &attempt.state),
                    Some(RedemptionAttemptState::ConfirmedAwaitingBalance { .. })
                ) {
                    return Err(ProjectionReducerError::CustodyWithoutConfirmedRedemption);
                }
                let condition = custody.identity.condition_id.0.clone();
                if !resolutions.contains_key(&condition) {
                    return Err(ProjectionReducerError::IdentityConflict);
                }
                let binding = current_account_binding
                    .as_ref()
                    .ok_or(ProjectionReducerError::InvalidAccountEvidence)?;
                verify_account_state(&custody.account_state, binding)?;
                let raw_positions = replay_position_receipts_legacy(
                    &custody.venue_position_receipts,
                    source_envelopes,
                    &custody.identity.custody_wallet,
                    binding,
                    prepared_orders
                        .values()
                        .map(|(_, prepared, _)| prepared.as_ref()),
                )?;
                if canonical_positions(&custody.venue_positions)?
                    != canonical_positions(&raw_positions)?
                {
                    return Err(ProjectionReducerError::CustodyInventoryMismatch);
                }
                receivable_by_condition.remove(&condition);
                let expected = derive_custody_positions(
                    &prepared_orders,
                    &fills,
                    &resolutions,
                    &receivable_by_condition,
                )?;
                if canonical_positions(&custody.venue_positions)? != expected {
                    return Err(ProjectionReducerError::CustodyInventoryMismatch);
                }
                let receivable = sum_receivable(&receivable_by_condition)?;
                require_cash_reconciliation(
                    custody.account_state.collateral_balance,
                    economic_cash.ok_or(ProjectionReducerError::InvalidBaseline)?,
                    receivable,
                )?;
                latest_free_collateral =
                    Some(custody.account_state.collateral_balance.to_decimal());
                latest_reconciled_at = format_observed_at(custody.account_state.observed_at);
                custody_facts.insert(custody.identity.clone(), custody.as_ref().clone());
            }
            LiveJournalPayload::AccountPortfolioMarked(mark) if mark.kind == MarkKind::Daily => {
                if daily_marks.contains_key(&mark.cutoff_unix) {
                    return Err(ProjectionReducerError::DuplicateDailyMark);
                }
                if mark.cutoff_unix.rem_euclid(86_400) != 0
                    || daily_marks
                        .last_key_value()
                        .is_some_and(|(prior, _)| *prior >= mark.cutoff_unix)
                {
                    return Err(ProjectionReducerError::InvalidDailyMark);
                }
                verify_account_binding(&mark.account_binding, account_id)?;
                if current_account_binding.as_ref() != Some(&mark.account_binding) {
                    return Err(ProjectionReducerError::InvalidAccountEvidence);
                }
                verify_account_state(&mark.account_state, &mark.account_binding)?;
                if mark.venue_position_evidence.requested_wallet
                    != mark.account_binding.custody_wallet
                {
                    return Err(ProjectionReducerError::InvalidPositionEvidence);
                }
                let raw_positions = replay_position_evidence(
                    &mark.venue_position_evidence,
                    source_envelopes,
                    &mark.account_binding,
                    events[..event_index]
                        .iter()
                        .filter_map(|prior| match &prior.payload {
                            LiveJournalPayload::OrderPrepared(prepared) => Some(prepared.as_ref()),
                            _ => None,
                        }),
                )?;
                if canonical_positions(&mark.venue_positions)?
                    != canonical_positions(&raw_positions)?
                {
                    return Err(ProjectionReducerError::CustodyInventoryMismatch);
                }
                let current_expected = derive_custody_positions(
                    &prepared_orders,
                    &fills,
                    &resolutions,
                    &receivable_by_condition,
                )?;
                if canonical_positions(&mark.venue_positions)? != current_expected {
                    return Err(ProjectionReducerError::CustodyInventoryMismatch);
                }
                require_cash_reconciliation(
                    mark.account_state.collateral_balance,
                    economic_cash.ok_or(ProjectionReducerError::InvalidBaseline)?,
                    sum_receivable(&receivable_by_condition)?,
                )?;
                let bounded_events = events[..event_index]
                    .iter()
                    .filter(|prior| prior.timestamp.unix_timestamp() < mark.cutoff_unix)
                    .cloned()
                    .collect::<Vec<_>>();
                let bounded_sources = source_envelopes
                    .iter()
                    .filter(|source| source.received_at.0.unix_timestamp() < mark.cutoff_unix)
                    .cloned()
                    .collect::<Vec<_>>();
                let bounded = derive_projection_rows_with_sources(
                    account_id,
                    &bounded_events,
                    &bounded_sources,
                )?;
                let mut price_receipts = BTreeSet::new();
                for price in &mark.prices {
                    if !price_receipts.insert((
                        price.receipt.sequence,
                        price.receipt.this_hash.to_hex().to_string(),
                    )) {
                        return Err(ProjectionReducerError::InvalidPriceEvidence);
                    }
                    let mut matching = bounded.custody_positions.iter().filter(|position| {
                        !position.redeemable
                            && position.condition_id == price.condition_id
                            && position.outcome_index == price.outcome_index
                    });
                    let position = matching
                        .next()
                        .ok_or(ProjectionReducerError::InvalidPriceEvidence)?;
                    if matching.next().is_some() {
                        return Err(ProjectionReducerError::InvalidPriceEvidence);
                    }
                    verify_mark_price(
                        price,
                        &position.token_id.0,
                        mark.cutoff_unix,
                        source_envelopes,
                    )?;
                }
                daily_marks.insert(mark.cutoff_unix, mark.equity);
                let expected = canonical_positions(&bounded.custody_positions)?;
                if canonical_positions(&mark.marked_positions)? != expected {
                    return Err(ProjectionReducerError::CustodyInventoryMismatch);
                }
                require_mark_equity_for_positions(
                    mark,
                    bounded
                        .economic_cash
                        .ok_or(ProjectionReducerError::InvalidBaseline)?,
                    &bounded.custody_positions,
                )?;
            }
            LiveJournalPayload::AccountPortfolioMarked(_)
            | LiveJournalPayload::RedemptionRequested(_)
            | LiveJournalPayload::RedemptionTransactionIdentified(_)
            | LiveJournalPayload::RedemptionReceiptTransition(_)
            | LiveJournalPayload::CredentialBindingMismatch { .. }
            | LiveJournalPayload::ModeTransitionApplied(_)
            | LiveJournalPayload::LegacyV1(_) => {}
        }
    }

    let mut position_totals = BTreeMap::<(String, i64), (Decimal, Decimal)>::new();
    for (key, (finalized, row)) in &fills {
        let Some((_, order, _)) = prepared_orders.get(key) else {
            return Err(ProjectionReducerError::MissingPrepared);
        };
        if resolutions.contains_key(&order.prepared.condition_id.0) {
            continue;
        }
        let total = position_totals
            .entry((row.market_id.clone(), row.outcome_id))
            .or_insert((Decimal::ZERO, Decimal::ZERO));
        total.0 = total
            .0
            .checked_add(row.contracts)
            .ok_or(ProjectionReducerError::Arithmetic)?;
        total.1 = total
            .1
            .checked_add(finalized.principal.to_decimal())
            .and_then(|value| value.checked_add(finalized.fee.to_decimal()))
            .ok_or(ProjectionReducerError::Arithmetic)?;
    }

    let mut fill_rows = fills
        .values()
        .map(|(_, row)| row.clone())
        .collect::<Vec<_>>();
    fill_rows.sort_by(|left, right| left.idempotency_key.cmp(&right.idempotency_key));
    let mut positions = position_totals
        .into_iter()
        .map(
            |((market_id, outcome_id), (long_contracts, cost_basis))| LivePositionRow {
                account_id: account_id.as_str().to_owned(),
                market_id,
                outcome_id,
                long_contracts,
                short_contracts: Decimal::ZERO,
                cost_basis,
            },
        )
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| {
        (&left.market_id, left.outcome_id).cmp(&(&right.market_id, right.outcome_id))
    });
    let mut open_exposures = Vec::new();
    for (key, (finalized, _)) in &fills {
        let Some((_, order, _)) = prepared_orders.get(key) else {
            return Err(ProjectionReducerError::MissingPrepared);
        };
        if resolutions.contains_key(&order.prepared.condition_id.0) {
            continue;
        }
        let projection = order
            .identity
            .fill_projection
            .as_deref()
            .ok_or(ProjectionReducerError::MissingProjectionIdentity)?;
        open_exposures.push(LiveOpenExposure {
            leader_wallet: projection.leader_wallet.clone(),
            market_id: projection.market_id.clone(),
            debit: finalized
                .principal
                .checked_add(finalized.fee)
                .map_err(|_| ProjectionReducerError::Arithmetic)?,
        });
    }

    let custody_positions = derive_custody_positions(
        &prepared_orders,
        &fills,
        &resolutions,
        &receivable_by_condition,
    )?;
    let receivable = sum_receivable(&receivable_by_condition)?.to_decimal();
    let reserved = reservations
        .values()
        .try_fold(Decimal::ZERO, |total, value| {
            total
                .checked_add(*value)
                .ok_or(ProjectionReducerError::Arithmetic)
        })?;
    Ok(ProjectionDerivation {
        fills: fill_rows,
        positions,
        custody_positions,
        reserved,
        economic_cash,
        receivable,
        latest_free_collateral,
        latest_reconciled_at,
        baseline_equity,
        baseline_cutoff_unix,
        baseline_sequence,
        account_binding: current_account_binding,
        daily_marks,
        realized_closes,
        validated_prepared_sequences: prepared_orders
            .values()
            .map(|(sequence, _, _)| *sequence)
            .collect(),
        pending_approved_admission_keys: approved_admissions.keys().cloned().collect(),
        open_exposures,
        receivable_by_condition,
    })
}

type PreparedOrders = BTreeMap<String, (u64, Box<pe_execution_core::LiveOrderPreparedAudit>, bool)>;
type FinalizedFills = BTreeMap<String, (OrderFillFinalizedAudit, LiveFillRow)>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
enum PolygonFinalityPhase {
    #[default]
    Unseen,
    Pending,
    Conflict,
    Finalized,
}

#[derive(Debug, Default)]
struct PolygonFinalityAuditState {
    phase: PolygonFinalityPhase,
    immutable_receipts: BTreeMap<String, MatchedReceipt>,
}

fn same_finalized_fill(left: &OrderFillFinalizedAudit, right: &OrderFillFinalizedAudit) -> bool {
    left == right
}

pub(crate) fn verify_account_state(
    account: &pe_execution_core::LiveAccountStateAudit,
    binding: &LiveAccountBindingAudit,
) -> Result<(), ProjectionReducerError> {
    let classified = classify_live_account_responses(
        &account.evidence,
        &account.selected_spender,
        &account.request_descriptor_hashes,
        Some(&account.evidence_hashes),
        binding,
    )
    .map_err(|_| ProjectionReducerError::InvalidAccountEvidence)?;
    let LiveAccountResponseClassification::State(state) = classified else {
        return Err(ProjectionReducerError::InvalidAccountEvidence);
    };
    let expected = state
        .audit()
        .map_err(|_| ProjectionReducerError::InvalidAccountEvidence)?;
    if *account != expected {
        return Err(ProjectionReducerError::InvalidAccountEvidence);
    }
    Ok(())
}

fn verify_account_binding(
    binding: &LiveAccountBindingAudit,
    account_id: &AccountId,
) -> Result<(), ProjectionReducerError> {
    if binding.is_valid_for(account_id) {
        Ok(())
    } else {
        Err(ProjectionReducerError::InvalidAccountEvidence)
    }
}

struct RetainedPositionPage {
    request_identity: String,
    body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedPositionResponse {
    request: SanitizedHttpRequestDescriptor,
    body: Vec<u8>,
}

struct RetainedPositionFetcher {
    pages: Mutex<VecDeque<RetainedPositionPage>>,
}

impl ReconciliationFetcher for RetainedPositionFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let page = self
                .pages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .ok_or_else(|| SourceError::Fatal {
                    message: "retained complete-position receipt is missing".to_owned(),
                })?;
            if position_request_identity(url).as_deref() != Some(&page.request_identity) {
                return Err(SourceError::Fatal {
                    message: "retained complete-position request identity disagrees".to_owned(),
                });
            }
            Ok(page.body)
        })
    }
}

fn position_request_identity(url: &str) -> Option<String> {
    let parsed = reqwest::Url::parse(url).ok().or_else(|| {
        reqwest::Url::parse("https://offline.invalid")
            .ok()?
            .join(url)
            .ok()
    })?;
    let mut identity = parsed.path().to_owned();
    if let Some(query) = parsed.query() {
        identity.push('?');
        identity.push_str(query);
    }
    Some(identity)
}

fn position_request_descriptor(
    binding: &LiveAccountBindingAudit,
    url: &str,
) -> Option<SanitizedHttpRequestDescriptor> {
    let parsed = reqwest::Url::parse(url).ok().or_else(|| {
        reqwest::Url::parse("https://offline.invalid")
            .ok()?
            .join(url)
            .ok()
    })?;
    let ordered_query = parsed
        .query_pairs()
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let exactly_one = |name: &str| {
        let mut values = ordered_query
            .iter()
            .filter(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str());
        let value = values.next()?;
        values.next().is_none().then_some(value)
    };
    let redeemable = exactly_one("redeemable")?;
    if !matches!(redeemable, "true" | "false") {
        return None;
    }
    let offset = exactly_one("offset")?.parse::<u64>().ok()?;
    Some(binding.request_descriptor(
        "GET",
        parsed.path(),
        format!("redeemable={redeemable}"),
        offset,
        ordered_query,
    ))
}

fn position_descriptor_identity(descriptor: &SanitizedHttpRequestDescriptor) -> Option<String> {
    let mut url = reqwest::Url::parse("https://offline.invalid").ok()?;
    url.set_path(&descriptor.path);
    {
        let mut query = url.query_pairs_mut();
        for (name, value) in &descriptor.ordered_query {
            query.append_pair(name, value);
        }
    }
    position_request_identity(url.as_str())
}

fn retained_position_response(
    receipt: &AppendReceipt,
    source_envelopes: &[EventEnvelope],
) -> Result<RetainedPositionResponse, ProjectionReducerError> {
    let payload = retained_source_payload(
        receipt,
        source_envelopes,
        COMPLETE_POSITIONS_SOURCE_ID,
        COMPLETE_POSITIONS_SCHEMA_VERSION,
        1,
        RetainedSourceKind::Position,
    )?;
    serde_json::from_slice(payload).map_err(|_| ProjectionReducerError::InvalidPositionEvidence)
}

fn replay_position_evidence<'a>(
    evidence: &LivePositionEvidenceAudit,
    source_envelopes: &[EventEnvelope],
    binding: &LiveAccountBindingAudit,
    prepared_orders: impl Iterator<Item = &'a pe_execution_core::LiveOrderPreparedAudit>,
) -> Result<Vec<CanonicalPositionAudit>, ProjectionReducerError> {
    if evidence.pages.is_empty()
        || evidence
            .pages
            .windows(2)
            .any(|pair| pair[0].receipt.sequence >= pair[1].receipt.sequence)
    {
        return Err(ProjectionReducerError::InvalidPositionEvidence);
    }
    let pages = evidence
        .pages
        .iter()
        .map(|page| {
            let retained = retained_position_response(&page.receipt, source_envelopes)?;
            let expected = position_request_descriptor(binding, &page.request_identity)
                .ok_or(ProjectionReducerError::InvalidPositionEvidence)?;
            if retained.request != expected
                || retained.request.custody_wallet != evidence.requested_wallet
            {
                return Err(ProjectionReducerError::InvalidPositionEvidence);
            }
            Ok(RetainedPositionPage {
                request_identity: page.request_identity.clone(),
                body: retained.body,
            })
        })
        .collect::<Result<VecDeque<_>, ProjectionReducerError>>()?;
    let requested_wallet = WalletAddress::from_hex(&evidence.requested_wallet)
        .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?;
    if requested_wallet.to_string() != evidence.requested_wallet {
        return Err(ProjectionReducerError::InvalidPositionEvidence);
    }
    let mut mapping = ActivityAssetMapping::from_rows(&[]);
    for prepared in prepared_orders {
        for (outcome_index, token_id) in prepared
            .economic
            .admission
            .market
            .ordered_outcome_token_ids
            .iter()
            .enumerate()
        {
            mapping
                .insert_verified_ordinary(
                    token_id.clone(),
                    prepared.economic.market.condition_id.clone(),
                    OutcomeId(
                        u16::try_from(outcome_index)
                            .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?,
                    ),
                )
                .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?;
        }
    }
    let fetcher = RetainedPositionFetcher {
        pages: Mutex::new(pages),
    };
    let complete = futures::executor::block_on(fetch_complete_positions(
        &fetcher,
        "https://offline.invalid",
        requested_wallet,
        &mapping,
    ))
    .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?;
    if complete.pages.len() != evidence.pages.len()
        || !fetcher
            .pages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    {
        return Err(ProjectionReducerError::InvalidPositionEvidence);
    }
    complete
        .positions
        .into_iter()
        .map(|position| {
            if position.classification != PositionClassification::Ordinary {
                return Err(ProjectionReducerError::InvalidPositionEvidence);
            }
            Ok(CanonicalPositionAudit {
                condition_id: position.condition_id,
                outcome_index: u8::try_from(position.outcome.0)
                    .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?,
                token_id: position.asset,
                size: position.size,
                redeemable: position.redeemable,
            })
        })
        .collect()
}

fn replay_position_receipts_legacy<'a>(
    receipts: &[AppendReceipt],
    source_envelopes: &[EventEnvelope],
    requested_wallet: &str,
    binding: &LiveAccountBindingAudit,
    prepared_orders: impl Iterator<Item = &'a pe_execution_core::LiveOrderPreparedAudit>,
) -> Result<Vec<CanonicalPositionAudit>, ProjectionReducerError> {
    if receipts.is_empty()
        || receipts
            .windows(2)
            .any(|pair| pair[0].sequence >= pair[1].sequence)
    {
        return Err(ProjectionReducerError::InvalidPositionEvidence);
    }
    let pages = receipts
        .iter()
        .map(|receipt| {
            let retained = retained_position_response(receipt, source_envelopes)?;
            let request_identity = position_descriptor_identity(&retained.request)
                .ok_or(ProjectionReducerError::InvalidPositionEvidence)?;
            let expected = position_request_descriptor(binding, &request_identity)
                .ok_or(ProjectionReducerError::InvalidPositionEvidence)?;
            if retained.request != expected || retained.request.custody_wallet != requested_wallet {
                return Err(ProjectionReducerError::InvalidPositionEvidence);
            }
            Ok(RetainedPositionPage {
                request_identity,
                body: retained.body,
            })
        })
        .collect::<Result<VecDeque<_>, _>>()?;
    let requested_wallet = WalletAddress::from_hex(requested_wallet)
        .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?;
    let mut mapping = ActivityAssetMapping::from_rows(&[]);
    for prepared in prepared_orders {
        for (outcome_index, token_id) in prepared
            .economic
            .admission
            .market
            .ordered_outcome_token_ids
            .iter()
            .enumerate()
        {
            mapping
                .insert_verified_ordinary(
                    token_id.clone(),
                    prepared.economic.market.condition_id.clone(),
                    OutcomeId(
                        u16::try_from(outcome_index)
                            .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?,
                    ),
                )
                .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?;
        }
    }
    let fetcher = RetainedPositionFetcher {
        pages: Mutex::new(pages),
    };
    let complete = futures::executor::block_on(fetch_complete_positions(
        &fetcher,
        "https://offline.invalid",
        requested_wallet,
        &mapping,
    ))
    .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?;
    if complete.pages.len() != receipts.len()
        || !fetcher
            .pages
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    {
        return Err(ProjectionReducerError::InvalidPositionEvidence);
    }
    complete
        .positions
        .into_iter()
        .map(canonical_position_from_replay)
        .collect()
}

fn canonical_position_from_replay(
    position: pe_source_polymarket_public::CanonicalPosition,
) -> Result<CanonicalPositionAudit, ProjectionReducerError> {
    if position.classification != PositionClassification::Ordinary {
        return Err(ProjectionReducerError::InvalidPositionEvidence);
    }
    Ok(CanonicalPositionAudit {
        condition_id: position.condition_id,
        outcome_index: u8::try_from(position.outcome.0)
            .map_err(|_| ProjectionReducerError::InvalidPositionEvidence)?,
        token_id: position.asset,
        size: position.size,
        redeemable: position.redeemable,
    })
}

fn verify_mark_price(
    price: &pe_execution_core::MarkPrice,
    token_id: &str,
    cutoff_unix: i64,
    source_envelopes: &[EventEnvelope],
) -> Result<(), ProjectionReducerError> {
    let payload = retained_source_payload(
        &price.receipt,
        source_envelopes,
        "pe-service.clob-prices-history",
        1,
        1,
        RetainedSourceKind::Price,
    )?;
    let start_unix = cutoff_unix
        .checked_sub(crate::risk_inputs::MAX_HISTORICAL_MARK_AGE_SECS)
        .ok_or(ProjectionReducerError::InvalidPriceEvidence)?;
    let base_url = "https://offline.invalid";
    let url = format!(
        "{base_url}/prices-history?market={token_id}&startTs={start_unix}&endTs={cutoff_unix}&fidelity=1"
    );
    let fetcher = FixtureFetcher::new(HashMap::from([(url, payload.to_vec())]));
    let client =
        ClobPricesHistoryClient::new(base_url.to_owned(), fetcher).with_fidelity_minutes(1);
    let classified = futures::executor::block_on(client.fetch_prices_history_classified(
        token_id,
        start_unix,
        cutoff_unix,
    ))
    .map_err(|_| ProjectionReducerError::InvalidPriceEvidence)?;
    if classified.body != payload {
        return Err(ProjectionReducerError::InvalidPriceEvidence);
    }
    let validated =
        crate::risk_inputs::historical_mark_price(&classified.outcome, cutoff_unix, price.receipt)
            .map_err(|_| ProjectionReducerError::InvalidPriceEvidence)?;
    if validated.price != price.price
        || validated.sample_unix != price.observed_unix
        || validated.receipt != price.receipt
    {
        return Err(ProjectionReducerError::InvalidPriceEvidence);
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum RetainedSourceKind {
    Position,
    Price,
    Resolution,
}

impl RetainedSourceKind {
    fn error(self) -> ProjectionReducerError {
        match self {
            Self::Position => ProjectionReducerError::InvalidPositionEvidence,
            Self::Price => ProjectionReducerError::InvalidPriceEvidence,
            Self::Resolution => ProjectionReducerError::InvalidResolutionEvidence,
        }
    }
}

fn retained_source_payload<'a>(
    receipt: &AppendReceipt,
    source_envelopes: &'a [EventEnvelope],
    source_id: &str,
    schema_version: u32,
    parser_version: u32,
    kind: RetainedSourceKind,
) -> Result<&'a [u8], ProjectionReducerError> {
    let envelope = source_envelopes
        .iter()
        .find(|envelope| {
            envelope.seq == receipt.sequence && envelope.this_hash == receipt.this_hash
        })
        .ok_or_else(|| kind.error())?;
    if envelope.source_id.0 != source_id
        || envelope.schema_version != schema_version
        || envelope.parser_version != parser_version
        || envelope.content_type != ContentType::Json
        || envelope.raw_payload_hash != blake3::hash(&envelope.payload)
    {
        return Err(kind.error());
    }
    Ok(&envelope.payload)
}

fn verify_resolution_evidence<'a>(
    resolution: &pe_execution_core::ResolutionFinalizedAudit,
    source_envelopes: &[EventEnvelope],
    prepared_orders: impl Iterator<Item = &'a pe_execution_core::LiveOrderPreparedAudit>,
) -> Result<(), ProjectionReducerError> {
    let payload = retained_source_payload(
        &resolution.source_append_receipt,
        source_envelopes,
        "polymarket.clob.resolution",
        pe_source_polymarket_public::CLOB_RESOLUTION_SCHEMA_VERSION,
        pe_source_polymarket_public::CLOB_RESOLUTION_PARSER_VERSION,
        RetainedSourceKind::Resolution,
    )?;
    let market = parse_clob_market(payload)
        .map_err(|_| ProjectionReducerError::InvalidResolutionEvidence)?;
    if market.condition_id.as_deref() != Some(resolution.condition_id.0.as_str()) {
        return Err(ProjectionReducerError::InvalidResolutionEvidence);
    }
    let token_ids = market
        .tokens
        .iter()
        .map(|token| token.token_id.as_deref())
        .collect::<Vec<_>>();
    if token_ids.len() != 2 || token_ids.iter().any(|token| token.is_none()) {
        return Err(ProjectionReducerError::InvalidResolutionEvidence);
    }
    let mut mapped = false;
    for prepared in prepared_orders.filter(|prepared| {
        prepared.economic.admission.market.condition_id == resolution.condition_id
    }) {
        mapped = true;
        let expected = &prepared.economic.admission.market.ordered_outcome_token_ids;
        if token_ids[0] != Some(expected[0].0.as_str())
            || token_ids[1] != Some(expected[1].0.as_str())
        {
            return Err(ProjectionReducerError::InvalidResolutionEvidence);
        }
    }
    let payout = match market.resolution_evidence().payout {
        ClobPayoutResolution::Resolved(payout) => payout,
        ClobPayoutResolution::Unresolved(_) => {
            return Err(ProjectionReducerError::InvalidResolutionEvidence);
        }
    };
    if !mapped || payout.canonical_json() != resolution.payout_by_outcome_index_json {
        return Err(ProjectionReducerError::InvalidResolutionEvidence);
    }
    Ok(())
}

fn resolution_credit(
    condition: &str,
    payout: &BinaryPayoutVector,
    prepared: &PreparedOrders,
    fills: &FinalizedFills,
) -> Result<CollateralAmount, ProjectionReducerError> {
    let mut positions = Vec::new();
    for (key, (fill, _)) in fills {
        let Some((_, order, _)) = prepared.get(key) else {
            return Err(ProjectionReducerError::MissingPrepared);
        };
        if order.prepared.condition_id.0 != condition {
            continue;
        }
        positions.push((order.prepared.outcome_id.0, fill.quantity));
    }
    let decimals = payout.decimals();
    let payout = pe_risk_engine::BinaryPayout::new(decimals[0], decimals[1])
        .map_err(|_| ProjectionReducerError::InvalidPayout)?;
    pe_risk_engine::aggregate_resolution_credit(&positions, &payout)
        .map_err(|_| ProjectionReducerError::Arithmetic)
}

fn derive_custody_positions(
    prepared: &PreparedOrders,
    fills: &FinalizedFills,
    resolutions: &BTreeMap<String, pe_execution_core::ResolutionFinalizedAudit>,
    receivable: &BTreeMap<String, CollateralAmount>,
) -> Result<Vec<CanonicalPositionAudit>, ProjectionReducerError> {
    let mut positions = BTreeMap::<String, CanonicalPositionAudit>::new();
    for (key, (fill, _)) in fills {
        let Some((_, order, _)) = prepared.get(key) else {
            return Err(ProjectionReducerError::MissingPrepared);
        };
        let condition = &order.prepared.condition_id.0;
        let resolved = resolutions.contains_key(condition);
        if resolved && !receivable.contains_key(condition) {
            continue;
        }
        let token = order.prepared.token_id.0.clone();
        let position = positions.entry(token).or_insert(CanonicalPositionAudit {
            condition_id: order.prepared.condition_id.clone(),
            outcome_index: u8::try_from(order.prepared.outcome_id.0)
                .map_err(|_| ProjectionReducerError::IdentityConflict)?,
            token_id: order.prepared.token_id.clone(),
            size: ShareAmount::ZERO,
            redeemable: resolved,
        });
        if position.condition_id != order.prepared.condition_id
            || position.outcome_index
                != u8::try_from(order.prepared.outcome_id.0)
                    .map_err(|_| ProjectionReducerError::IdentityConflict)?
            || position.redeemable != resolved
        {
            return Err(ProjectionReducerError::IdentityConflict);
        }
        position.size = position
            .size
            .checked_add(fill.quantity)
            .map_err(|_| ProjectionReducerError::Arithmetic)?;
    }
    Ok(positions.into_values().collect())
}

fn canonical_positions(
    positions: &[CanonicalPositionAudit],
) -> Result<Vec<CanonicalPositionAudit>, ProjectionReducerError> {
    let mut canonical = positions.to_vec();
    if canonical
        .iter()
        .any(|position| position.size == ShareAmount::ZERO)
    {
        return Err(ProjectionReducerError::CustodyInventoryMismatch);
    }
    canonical.sort_by(|left, right| left.token_id.0.cmp(&right.token_id.0));
    if canonical
        .windows(2)
        .any(|pair| pair[0].token_id == pair[1].token_id)
    {
        return Err(ProjectionReducerError::CustodyInventoryMismatch);
    }
    Ok(canonical)
}

fn sum_receivable(
    receivable: &BTreeMap<String, CollateralAmount>,
) -> Result<CollateralAmount, ProjectionReducerError> {
    receivable
        .values()
        .try_fold(CollateralAmount::ZERO, |total, value| {
            total
                .checked_add(*value)
                .map_err(|_| ProjectionReducerError::Arithmetic)
        })
}

fn require_cash_reconciliation(
    authenticated: CollateralAmount,
    economic_cash: Decimal,
    receivable: CollateralAmount,
) -> Result<(), ProjectionReducerError> {
    let accounted = authenticated
        .to_decimal()
        .checked_add(receivable.to_decimal())
        .ok_or(ProjectionReducerError::Arithmetic)?;
    if accounted != economic_cash {
        return Err(ProjectionReducerError::CashMismatch);
    }
    Ok(())
}

fn require_mark_equity_for_positions(
    mark: &pe_execution_core::AccountPortfolioMarkedAudit,
    economic_cash: Decimal,
    positions: &[CanonicalPositionAudit],
) -> Result<(), ProjectionReducerError> {
    let mut prices = BTreeMap::new();
    for price in &mark.prices {
        if prices
            .insert(
                (price.condition_id.0.clone(), price.outcome_index),
                price.price,
            )
            .is_some()
        {
            return Err(ProjectionReducerError::InvalidMarkPrices);
        }
    }
    let open = positions
        .iter()
        .filter(|position| !position.redeemable)
        .collect::<Vec<_>>();
    if open.len() != prices.len()
        || open.iter().any(|position| {
            !prices.contains_key(&(position.condition_id.0.clone(), position.outcome_index))
        })
    {
        return Err(ProjectionReducerError::InvalidMarkPrices);
    }
    let equity = open
        .iter()
        .try_fold(economic_cash, |total, position| {
            let price = prices
                .get(&(position.condition_id.0.clone(), position.outcome_index))
                .ok_or(ProjectionReducerError::InvalidMarkPrices)?;
            position
                .size
                .to_decimal()
                .checked_mul(price.0)
                .and_then(|value| total.checked_add(value))
                .ok_or(ProjectionReducerError::Arithmetic)
        })?
        .round_dp_with_strategy(6, RoundingStrategy::ToNegativeInfinity);
    if CollateralAmount::from_decimal_exact(equity)
        .map_err(|_| ProjectionReducerError::Arithmetic)?
        != mark.equity
    {
        return Err(ProjectionReducerError::InvalidMarkEquity);
    }
    Ok(())
}

fn format_observed_at(observed_at: OffsetDateTime) -> Option<String> {
    observed_at
        .format(&time::format_description::well_known::Rfc3339)
        .ok()
}

async fn reconcile_account_projection(
    state: &mut FanoutState,
    account_id: &AccountId,
    _now: OffsetDateTime,
) {
    let events = match replay_live_account(state, account_id) {
        Ok(events) => events,
        Err(error) => {
            error!(account_id = %account_id, error = %error, "live projection journal replay failed");
            return;
        }
    };
    let derived = match derive_projection_rows_for_state(state, account_id, &events) {
        Ok(derived) => derived,
        Err(error) => {
            error!(account_id = %account_id, error = %error, "live projection derivation failed");
            return;
        }
    };
    if let Err(error) = state.config.projection.upsert_fills(&derived.fills).await {
        warn!(account_id = %account_id, error = %error, "live fill projection reconcile failed");
    }
    if let Err(error) = state
        .config
        .projection
        .upsert_positions(&derived.positions)
        .await
    {
        warn!(account_id = %account_id, error = %error, "live position projection reconcile failed");
    }

    let closed_reason = state
        .closures
        .reason(account_id.as_str())
        .map(str::to_owned);
    let Some(row) = compose_account_state_row(account_id.as_str(), &derived, closed_reason) else {
        return;
    };
    if let Err(error) = state.config.projection.upsert_account_state(&row).await {
        warn!(account_id = %account_id, error = %error, "live account-state projection reconcile failed");
    }
}

fn compose_account_state_row(
    account_id: &str,
    derived: &ProjectionDerivation,
    admission_closed_reason: Option<String>,
) -> Option<LiveAccountStateRow> {
    let free_collateral = derived.latest_free_collateral;
    let last_reconciled_at = derived.latest_reconciled_at.clone();
    let unredeemed_value = derived.receivable;
    if free_collateral.is_none()
        && unredeemed_value == Decimal::ZERO
        && derived.reserved == Decimal::ZERO
        && admission_closed_reason.is_none()
    {
        return None;
    }
    Some(LiveAccountStateRow {
        account_id: account_id.to_owned(),
        free_collateral: free_collateral.unwrap_or(Decimal::ZERO),
        reserved: derived.reserved,
        unredeemed_value,
        last_reconciled_at,
        admission_closed_reason,
    })
}

async fn reconcile_projections(state: &mut FanoutState, now: OffsetDateTime) {
    let snapshot = state.config.live_accounts.snapshot();
    let mut accounts = recovery_inventory(
        &state.config.journal_path,
        state.config.era_live_prefix.as_ref(),
    )
    .map(|inventory| inventory.account_ids.into_iter().collect::<BTreeSet<_>>())
    .unwrap_or_default();
    accounts.extend(
        snapshot
            .accounts
            .iter()
            .map(|account| account.account_id.clone()),
    );
    for account_id in accounts {
        reconcile_account_projection(state, &account_id, now).await;
    }
}

struct RecoveredPrepared {
    journal_seq: u64,
    audit: Box<pe_execution_core::LiveOrderPreparedAudit>,
    transaction_hashes: BTreeSet<String>,
    finalized: bool,
}

fn recovered_prepared(
    state: &FanoutState,
    target: &DispatchTargetRow,
) -> Result<Option<RecoveredPrepared>, FanoutError> {
    let account_id = AccountId::new(&target.account_id)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let events = replay_live_account(state, &account_id)?;
    let mut recovered = None;
    for event in events {
        if event.account_id != account_id {
            return Err(FanoutError::Signal(
                "live order fact belongs to the wrong account envelope".to_owned(),
            ));
        }
        match event.payload {
            LiveJournalPayload::OrderPrepared(prepared)
                if prepared.identity.dispatch_id == target.dispatch_id && recovered.is_some() =>
            {
                return Err(FanoutError::Signal(
                    "duplicate prepared record for one dispatch target".to_owned(),
                ));
            }
            LiveJournalPayload::OrderPrepared(prepared)
                if prepared.identity.dispatch_id == target.dispatch_id =>
            {
                if prepared.frozen_binding.version != target.credential_bundle_version
                    || prepared.frozen_binding.key_id != target.credential_key_id
                {
                    return Err(FanoutError::Signal(
                        "prepared order does not match the target account binding".to_owned(),
                    ));
                }
                recovered = Some(RecoveredPrepared {
                    journal_seq: event.seq,
                    audit: prepared,
                    transaction_hashes: BTreeSet::new(),
                    finalized: false,
                });
            }
            LiveJournalPayload::OrderPosted(posted)
                if posted.identity.dispatch_id == target.dispatch_id =>
            {
                let Some(prepared) = recovered.as_ref() else {
                    return Err(FanoutError::Signal(
                        "posted order precedes its prepared record".to_owned(),
                    ));
                };
                if !prepared_order_fact_matches(
                    prepared.journal_seq,
                    &prepared.audit.identity,
                    &prepared.audit.prepared.order_hash,
                    PreparedOrderFact::Posted(&posted),
                ) {
                    return Err(FanoutError::Signal(
                        "posted order does not match its prepared record".to_owned(),
                    ));
                }
            }
            LiveJournalPayload::OrderReconciled(reconciled)
                if reconciled.identity.dispatch_id == target.dispatch_id =>
            {
                let Some(prepared) = recovered.as_mut() else {
                    return Err(FanoutError::Signal(
                        "reconciled order precedes its prepared record".to_owned(),
                    ));
                };
                if !prepared_order_fact_matches(
                    prepared.journal_seq,
                    &prepared.audit.identity,
                    &prepared.audit.prepared.order_hash,
                    PreparedOrderFact::Reconciled(&reconciled),
                ) {
                    return Err(FanoutError::Signal(
                        "reconciled order does not match its prepared record".to_owned(),
                    ));
                }
                if let LiveJournalOrderOutcome::Matched {
                    transaction_hashes, ..
                } = reconciled.outcome
                {
                    prepared.transaction_hashes.extend(transaction_hashes);
                }
            }
            LiveJournalPayload::OrderFillFinalized(finalized)
                if finalized.identity.dispatch_id == target.dispatch_id =>
            {
                let Some(prepared) = recovered.as_mut() else {
                    return Err(FanoutError::Signal(
                        "finalized fill precedes its prepared record".to_owned(),
                    ));
                };
                if !prepared_order_fact_matches(
                    prepared.journal_seq,
                    &prepared.audit.identity,
                    &prepared.audit.prepared.order_hash,
                    PreparedOrderFact::Finalized(&finalized),
                ) {
                    return Err(FanoutError::Signal(
                        "finalized fill does not match its prepared record".to_owned(),
                    ));
                }
                prepared.finalized = true;
            }
            _ => {}
        }
    }
    Ok(recovered)
}

/// Reconcile one `submitted`/`ambiguous` target (#514 classify-first). Runs before — and
/// is never blocked or terminalized by — the freshness, arming, closure, and
/// credential-CAS gates. Credential rotation genuinely occurs mid-flight (the single-row
/// `account_credentials` upsert), so an unavailable/rotated frozen binding retains the
/// target's non-terminal state, keeps dispatch frozen, and surfaces loudly for operator
/// recovery. `context` is `Some` only when a fresh snapshot still carries the account;
/// `None` reconciles the order state only (no account-state capture or projection).
async fn recover_in_flight_target(
    state: &mut FanoutState,
    target: &DispatchTargetRow,
    context: Option<&AccountContext>,
    now: OffsetDateTime,
) -> Result<PassControl, FanoutError> {
    if recovered_prepared(state, target)?.is_some_and(|prepared| prepared.finalized) {
        terminalize(state, target, "filled", now)?;
        if let Some(account) = context {
            reconcile_account_projection(state, &account.account_id, now).await;
        }
        return Ok(PassControl::Continue);
    }
    let credentials = match credentials_for_target(state, target).await {
        CredentialLoad::Ready(credentials) => credentials,
        CredentialLoad::Changed => {
            error!(
                account_id = %target.account_id,
                dispatch_id = %target.dispatch_id,
                "in-flight live order's frozen credential binding was rotated away; the \
                 target is retained non-terminal and dispatch stays frozen until operator \
                 recovery"
            );
            return Ok(PassControl::FreezePass);
        }
        CredentialLoad::Transient(reason) => {
            warn!(account_id = %target.account_id, reason, "in-flight live credential load refused transiently; recovery retries");
            return Ok(PassControl::StopSeed);
        }
    };
    let venue = match PolymarketLiveVenue::from_credentials(&credentials).await {
        Ok(venue) => venue,
        Err(error) => {
            warn!(account_id = %target.account_id, error = %error, "live V2 client construction failed; recovery retries");
            return Ok(PassControl::StopSeed);
        }
    };
    let outcome = recover_target(state, target, &venue).await?;
    let transition = outcome_transition(&outcome);
    persist_outcome(state, target, &outcome, OffsetDateTime::now_utc())?;
    if let Some(account) = context
        && matches!(outcome, LiveOrderOutcome::Matched { .. })
    {
        reconcile_account_projection(state, &account.account_id, OffsetDateTime::now_utc()).await;
    }
    Ok(if transition.freeze {
        PassControl::FreezePass
    } else {
        PassControl::Continue
    })
}

async fn recover_target(
    state: &FanoutState,
    target: &DispatchTargetRow,
    venue: &PolymarketLiveVenue,
) -> Result<LiveOrderOutcome, FanoutError> {
    let Some(prepared) = recovered_prepared(state, target)? else {
        return Ok(LiveOrderOutcome::Ambiguous {
            order_hash: "missing-prepared-order-hash".to_owned(),
            kind: LiveOrderAmbiguityKind::ReconciliationUnavailable,
            reconcile_first: true,
        });
    };
    let identity = prepared.audit.identity.clone();
    let order_hash = prepared.audit.prepared.order_hash.clone();
    let account_id = AccountId::new(&target.account_id)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let reconciliation = venue.reconcile_and_cancel_by_order_hash(&order_hash).await;
    let (journal_outcome, outcome, evidence) = match reconciliation {
        Ok(reconciliation) => match reconciliation.outcome {
            LiveVenueReconciledOutcome::Matched {
                venue_order_id,
                transaction_hashes,
            } => {
                let transaction_hashes = prepared
                    .transaction_hashes
                    .iter()
                    .cloned()
                    .chain(transaction_hashes)
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                (
                    LiveJournalOrderOutcome::Matched {
                        venue_order_id: venue_order_id.clone(),
                        transaction_hashes: transaction_hashes.clone(),
                        executed: None,
                    },
                    LiveOrderOutcome::Matched {
                        order_hash: order_hash.clone(),
                        venue_order_id,
                        transaction_hashes,
                        executed: None,
                    },
                    reconciliation.evidence,
                )
            }
            LiveVenueReconciledOutcome::Killed { venue_order_id } => (
                LiveJournalOrderOutcome::Killed {
                    venue_order_id: venue_order_id.clone(),
                },
                LiveOrderOutcome::Killed {
                    order_hash: order_hash.clone(),
                    venue_order_id,
                },
                reconciliation.evidence,
            ),
            LiveVenueReconciledOutcome::Rejected { venue_order_id } => (
                LiveJournalOrderOutcome::Rejected {
                    venue_order_id: venue_order_id.clone(),
                    kind: pe_execution_core::LiveOrderRejectKind::VenueRejected,
                },
                LiveOrderOutcome::Rejected {
                    order_hash: Some(order_hash.clone()),
                    venue_order_id,
                    kind: pe_execution_core::LiveOrderRejectKind::VenueRejected,
                },
                reconciliation.evidence,
            ),
            LiveVenueReconciledOutcome::Ambiguous { kind } => (
                LiveJournalOrderOutcome::Ambiguous { kind },
                LiveOrderOutcome::Ambiguous {
                    order_hash: order_hash.clone(),
                    kind,
                    reconcile_first: true,
                },
                reconciliation.evidence,
            ),
        },
        Err(error) => (
            LiveJournalOrderOutcome::Ambiguous {
                kind: LiveOrderAmbiguityKind::ReconciliationUnavailable,
            },
            LiveOrderOutcome::Ambiguous {
                order_hash: order_hash.clone(),
                kind: LiveOrderAmbiguityKind::ReconciliationUnavailable,
                reconcile_first: true,
            },
            error.evidence,
        ),
    };
    let evidence_hashes = http_attempt_hashes(&evidence)?;
    let reconciled_at = OffsetDateTime::now_utc();
    state.config.journal.append(
        account_id,
        reconciled_at,
        LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
            identity,
            order_hash,
            source: LiveReconciliationSource::OrderHashLookupAndCancel,
            outcome: journal_outcome,
            evidence,
            evidence_hashes,
        })),
    )?;
    Ok(outcome)
}

enum CredentialLoad {
    Ready(Box<LiveAccountCredentials>),
    Changed,
    Transient(&'static str),
}

#[derive(Deserialize)]
struct SealedCredentialRow {
    account_id: String,
    bundle_version: i64,
    key_id: String,
    sealed_bundle: String,
}

async fn credentials_for_target(state: &FanoutState, target: &DispatchTargetRow) -> CredentialLoad {
    let Some(identity) = state.config.identity.as_ref() else {
        return CredentialLoad::Transient("age identity unavailable");
    };
    let row = match fetch_sealed_credential(state, &target.account_id).await {
        Ok(row) => row,
        Err(_) => return CredentialLoad::Transient("credential row unavailable"),
    };
    if row.account_id != target.account_id
        || row.bundle_version != target.credential_bundle_version
        || row.key_id != target.credential_key_id
    {
        return CredentialLoad::Changed;
    }
    let expected = CredentialBinding {
        account_id: target.account_id.clone(),
        bundle_version: target.credential_bundle_version,
        key_id: target.credential_key_id.clone(),
    };
    match decrypt_bundle(&row.sealed_bundle, identity, &expected) {
        Ok(credentials) => CredentialLoad::Ready(Box::new(credentials)),
        Err(CredentialError::BindingMismatch(_)) => CredentialLoad::Changed,
        Err(_) => CredentialLoad::Transient("credential decrypt failed"),
    }
}

async fn fetch_sealed_credential(
    state: &FanoutState,
    account_id: &str,
) -> Result<SealedCredentialRow, &'static str> {
    let token = auth_token(
        &state.config.supabase_anon_key,
        &state.config.supabase_secret_key,
    );
    let url = format!(
        "{}/rest/v1/account_credentials?select=account_id,bundle_version,key_id,sealed_bundle&account_id=eq.{account_id}",
        state.config.supabase_url.trim_end_matches('/')
    );
    let response = state
        .config
        .http
        .get(url)
        .header("apikey", token)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| "transport")?;
    if !response.status().is_success() {
        return Err("status");
    }
    let mut rows: Vec<SealedCredentialRow> = response.json().await.map_err(|_| "decode")?;
    if rows.len() != 1 {
        return Err("cardinality");
    }
    rows.pop().ok_or("cardinality")
}

#[derive(Clone, Deserialize)]
struct AccountSettingsRow {
    account_id: String,
    live_sizing_mode: Option<String>,
    live_sizing_dollar_usd: Option<String>,
    live_sizing_contracts: Option<i64>,
}

impl AccountSettingsRow {
    fn sizing_mode(&self, fallback: SizingMode) -> Result<SizingMode, &'static str> {
        match self.live_sizing_mode.as_deref() {
            None => Ok(fallback),
            Some("kelly") => Ok(SizingMode::Kelly),
            Some("dollar") => self
                .live_sizing_dollar_usd
                .as_deref()
                .and_then(|raw| Decimal::from_str(raw).ok())
                .filter(|value| *value > Decimal::ZERO)
                .map(|usd| SizingMode::Dollar { usd })
                .ok_or("live_sizing_dollar_invalid"),
            Some("contract") => self
                .live_sizing_contracts
                .and_then(|value| u64::try_from(value).ok())
                .filter(|value| *value > 0)
                .map(|contracts| SizingMode::Contract { contracts })
                .ok_or("live_sizing_contracts_invalid"),
            Some(_) => Err("live_sizing_mode_invalid"),
        }
    }
}

/// PostgREST read for one account's sizing posture. `live_sizing_dollar_usd` is `numeric`,
/// so PostgREST serializes it as a JSON number; the query-level `::text` cast makes it a
/// JSON string so the money value decodes exactly via [`Decimal::from_str`]. An uncast
/// number would traverse f64 (`serde_json` without `arbitrary_precision`), violating the
/// no-f64 money rule — and broke decoding entirely against the `Option<String>` DTO,
/// leaving arming permanently refused (#514).
fn account_settings_url(base_url: &str, account_id: &str) -> String {
    format!(
        "{}/rest/v1/accounts?select=account_id,live_sizing_mode,live_sizing_dollar_usd::text,live_sizing_contracts&account_id=eq.{account_id}",
        base_url.trim_end_matches('/')
    )
}

async fn fetch_account_settings(
    state: &FanoutState,
    account_id: &str,
) -> Result<AccountSettingsRow, &'static str> {
    let token = auth_token(
        &state.config.supabase_anon_key,
        &state.config.supabase_secret_key,
    );
    fetch_account_settings_from(
        &state.config.http,
        &state.config.supabase_url,
        token,
        account_id,
    )
    .await
}

async fn fetch_account_settings_from(
    client: &reqwest::Client,
    base_url: &str,
    token: &str,
    account_id: &str,
) -> Result<AccountSettingsRow, &'static str> {
    let response = client
        .get(account_settings_url(base_url, account_id))
        .header("apikey", token)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .map_err(|_| "transport")?;
    if !response.status().is_success() {
        return Err("status");
    }
    let mut rows: Vec<AccountSettingsRow> = response.json().await.map_err(|_| "decode")?;
    if rows.len() != 1 {
        return Err("cardinality");
    }
    let row = rows.pop().ok_or("cardinality")?;
    if row.account_id != account_id {
        return Err("identity");
    }
    Ok(row)
}

#[derive(Deserialize)]
struct AccountPromotionRow {
    account_id: String,
    event_kind: String,
    created_at: String,
    evidence_ref: Option<String>,
}

async fn fetch_promotions(
    state: &FanoutState,
    account_ids: &[AccountId],
) -> Result<HashMap<String, PromotionFacts>, &'static str> {
    let token = auth_token(
        &state.config.supabase_anon_key,
        &state.config.supabase_secret_key,
    );
    fetch_promotions_from(
        &state.config.http,
        &state.config.supabase_url,
        token,
        account_ids,
    )
    .await
}

async fn fetch_promotions_from(
    client: &reqwest::Client,
    supabase_url: &str,
    token: &str,
    account_ids: &[AccountId],
) -> Result<HashMap<String, PromotionFacts>, &'static str> {
    let mut promotions = HashMap::with_capacity(account_ids.len());
    for account_id in account_ids {
        let mut rows = Vec::with_capacity(2);
        for event_kind in ["promotion_reviewed", "promotion_review_revoked"] {
            let response = client
                .get(promotion_event_url(supabase_url, account_id, event_kind))
                .header("apikey", token)
                .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
                .send()
                .await
                .map_err(|_| "transport")?;
            if !response.status().is_success() {
                return Err("status");
            }
            let event_rows: Vec<AccountPromotionRow> =
                response.json().await.map_err(|_| "decode")?;
            if event_rows.len() > 1 {
                return Err("cardinality");
            }
            for row in event_rows {
                if row.account_id != account_id.as_str() || row.event_kind != event_kind {
                    return Err("identity");
                }
                rows.push(PromotionEventRow {
                    event_kind: row.event_kind,
                    created_at: row.created_at,
                    evidence_ref: row.evidence_ref,
                });
            }
        }
        promotions.insert(
            account_id.as_str().to_owned(),
            promotion_facts_from_rows(&rows),
        );
    }
    Ok(promotions)
}

fn promotion_event_url(supabase_url: &str, account_id: &AccountId, event_kind: &str) -> String {
    format!(
        "{}/rest/v1/account_events?select=event_id,account_id,event_kind,created_at,evidence_ref&account_id=eq.{account_id}&event_kind=eq.{event_kind}&order=created_at.desc,event_id.desc&limit=1",
        supabase_url.trim_end_matches('/'),
    )
}

fn current_qualification_seal_hash(
    paper_log_path: &Path,
) -> Result<Option<String>, crate::paper_recovery::PaperLogScanError> {
    let era = paper_era(scan_paper_log(paper_log_path)?);
    Ok(era.frames.iter().rev().find_map(|frame| {
        matches!(
            frame.frame,
            PaperLogFrame::Record(PaperLogRecord::QualificationSealed(_))
        )
        .then(|| frame.receipt.this_hash.to_hex().to_string())
    }))
}

struct StaticProbe {
    account: CheckOutcome,
    geoblock: CheckOutcome,
    balance: CheckOutcome,
    polygon: CheckOutcome,
}

impl ArmingProbe for StaticProbe {
    fn account_state(&self) -> CheckOutcome {
        self.account.clone()
    }

    fn geoblock(&self) -> CheckOutcome {
        self.geoblock.clone()
    }

    fn balance_and_both_spender_allowances(&self) -> CheckOutcome {
        self.balance.clone()
    }

    fn polygon_finality(&self) -> CheckOutcome {
        self.polygon.clone()
    }
}

async fn drive_modes(state: &mut FanoutState, now: OffsetDateTime) {
    let snapshot = state.config.live_accounts.snapshot();
    if snapshot.accounts.is_empty() {
        return;
    }
    // #514: no promotion/demotion is written from stale account evidence. New-order
    // admission is separately paused by staging and target classification.
    if !snapshot.is_fresh(now.unix_timestamp()) {
        warn!("live accounts snapshot is stale; mode pass skipped");
        return;
    }
    let promotion_accounts = snapshot
        .accounts
        .iter()
        .filter(|account| account.enabled && account.requested_live_mode == "live_tiny")
        .map(|account| account.account_id.clone())
        .collect::<Vec<_>>();
    let promotions = match fetch_promotions(state, &promotion_accounts).await {
        Ok(promotions) => promotions,
        Err(reason) => {
            warn!(
                reason,
                "promotion facts unavailable; mode pass refuses arming"
            );
            for account in &snapshot.accounts {
                if account.enabled
                    && (account.requested_live_mode == "live_tiny"
                        || account.effective_live_mode == "live_tiny")
                {
                    state.closures.mode.insert(
                        account.account_id.as_str().to_owned(),
                        "promotion query unavailable".to_owned(),
                    );
                }
            }
            return;
        }
    };
    let fence = match state.config.paper_state.live_executor_first_boot() {
        Ok(fence) => fence,
        Err(error) => {
            warn!(error = %error, "live executor fence read failed; mode pass refused");
            return;
        }
    };
    let current_seal_hash = match current_qualification_seal_hash(&state.config.paper_log_path) {
        Ok(seal_hash) => seal_hash,
        Err(error) => {
            warn!(error = %error, "qualification seal read failed; qualification evidence invalid");
            None
        }
    };
    let economic_configuration_hash = state.config.runtime_config.snapshot().canonical_hash();
    let mut armed_count = snapshot
        .accounts
        .iter()
        .filter(|account| account.effective_live_mode == "live_tiny")
        .count();
    let polygon = polygon_arming_health(state).await;
    for account in &snapshot.accounts {
        let (credential_outcome, probe) = mode_probe(state, account, now, polygon.clone()).await;
        let facts = promotions
            .get(account.account_id.as_str())
            .cloned()
            .unwrap_or_default();
        let already_armed_count =
            armed_count.saturating_sub(usize::from(account.effective_live_mode == "live_tiny"));
        let decision = evaluate_mode(&ModeInputs {
            requested_live_mode: &account.requested_live_mode,
            effective_live_mode: &account.effective_live_mode,
            enabled: account.enabled,
            credentials: credential_outcome,
            qualification: state.config.qualification.clone(),
            current_seal_hash: current_seal_hash.as_deref(),
            economic_configuration_hash: &economic_configuration_hash,
            financial_semantic_version: FINANCIAL_SEMANTIC_VERSION,
            promotion: &facts,
            fence_unix: fence,
            already_armed_count,
            probe: &probe,
        });
        match decision {
            ModeDecision::Keep => {
                state.closures.mode.remove(account.account_id.as_str());
            }
            ModeDecision::RefuseOrders { reason } => {
                state
                    .closures
                    .mode
                    .insert(account.account_id.as_str().to_owned(), reason);
            }
            ModeDecision::SetEffective { mode, reason } => {
                match state
                    .config
                    .projection
                    .set_effective_mode(account.account_id.as_str(), mode, &reason)
                    .await
                {
                    Ok(()) => {
                        let transition = LiveModeTransitionAudit {
                            requested: mode_value(&account.requested_live_mode),
                            previous_effective: mode_value(&account.effective_live_mode),
                            new_effective: mode_value(mode),
                            reason: mode_transition_reason(&reason),
                        };
                        if let Err(error) = state.config.journal.append(
                            account.account_id.clone(),
                            now,
                            LiveJournalPayload::ModeTransitionApplied(transition),
                        ) {
                            state.closures.mode.insert(
                                account.account_id.as_str().to_owned(),
                                "effective-mode journal pending".to_owned(),
                            );
                            error!(account_id = %account.account_id, error = %error, "effective live-mode transition committed but journal append failed");
                            continue;
                        }
                        if account.effective_live_mode == "live_tiny" && mode == "off" {
                            armed_count = armed_count.saturating_sub(1);
                        } else if account.effective_live_mode != "live_tiny" && mode == "live_tiny"
                        {
                            armed_count = armed_count.saturating_add(1);
                        }
                        if mode == "live_tiny" {
                            state.closures.mode.remove(account.account_id.as_str());
                        } else {
                            state
                                .closures
                                .mode
                                .insert(account.account_id.as_str().to_owned(), reason);
                        }
                    }
                    Err(error) => {
                        state.closures.mode.insert(
                            account.account_id.as_str().to_owned(),
                            "effective-mode write pending".to_owned(),
                        );
                        warn!(account_id = %account.account_id, error = %error, "effective live-mode transition failed");
                    }
                }
            }
        }
    }
}

fn mode_transition_reason(reason: &str) -> LiveModeTransitionReason {
    if reason.starts_with("armed:") {
        LiveModeTransitionReason::Armed
    } else if reason.contains("operator requested") {
        LiveModeTransitionReason::OperatorKill
    } else if reason.contains("credential") {
        LiveModeTransitionReason::CredentialInvalidated
    } else if reason.contains("promotion_record") {
        LiveModeTransitionReason::PromotionInvalidated
    } else {
        LiveModeTransitionReason::AccountClosedOnly
    }
}

async fn mode_probe(
    state: &mut FanoutState,
    account: &AccountContext,
    _now: OffsetDateTime,
    polygon: CheckOutcome,
) -> (CheckOutcome, StaticProbe) {
    let unavailable = StaticProbe {
        account: CheckOutcome::Transient("account query unavailable"),
        geoblock: CheckOutcome::Transient("geoblock query unavailable"),
        balance: CheckOutcome::Transient("balance query unavailable"),
        polygon: polygon.clone(),
    };
    // Operator kill/disabled and already-off accounts need no credentialed network probe. The
    // pure machine handles those control paths before inspecting probe results.
    if account.requested_live_mode != "live_tiny" || !account.enabled {
        return (
            CheckOutcome::Pass,
            StaticProbe {
                account: CheckOutcome::Pass,
                geoblock: CheckOutcome::Pass,
                balance: CheckOutcome::Pass,
                polygon,
            },
        );
    }
    let target = DispatchTargetRow {
        dispatch_id: "mode-probe".to_owned(),
        account_id: account.account_id.as_str().to_owned(),
        exec_rank: 0,
        credential_bundle_version: account
            .credential_binding
            .as_ref()
            .map(|binding| binding.0)
            .unwrap_or_default(),
        credential_key_id: account
            .credential_binding
            .as_ref()
            .map(|binding| binding.1.clone())
            .unwrap_or_default(),
        state: "pending".to_owned(),
        terminal_reason: None,
        updated_at_unix: 0,
    };
    let credentials = match credentials_for_target(state, &target).await {
        CredentialLoad::Ready(credentials) => credentials,
        CredentialLoad::Changed => {
            return (
                CheckOutcome::PersistentFail("credential binding changed"),
                unavailable,
            );
        }
        CredentialLoad::Transient(reason) => {
            let outcome = if reason == "credential row unavailable" {
                CheckOutcome::Transient("credential row unavailable")
            } else {
                CheckOutcome::PersistentFail("cannot decrypt")
            };
            return (outcome, unavailable);
        }
    };
    let venue = match PolymarketLiveVenue::from_credentials(&credentials).await {
        Ok(venue) => venue,
        Err(_) => {
            return (
                CheckOutcome::PersistentFail("cannot construct venue client"),
                unavailable,
            );
        }
    };
    let (standard, neg_risk) = match venue.account_states().await {
        Ok(states) => states,
        Err(_) => return (CheckOutcome::Pass, unavailable),
    };
    if standard.binding != neg_risk.binding {
        return (
            CheckOutcome::PersistentFail("credentialed account reads disagree on binding"),
            unavailable,
        );
    }
    let account_outcome = if standard.state.closed_only || neg_risk.state.closed_only {
        CheckOutcome::PersistentFail("closed_only")
    } else {
        CheckOutcome::Pass
    };
    let geoblock = if standard.state.geoblocked || neg_risk.state.geoblocked {
        CheckOutcome::PersistentFail("orders geoblocked")
    } else {
        CheckOutcome::Pass
    };
    let portfolio = if standard.state.collateral_balance != neg_risk.state.collateral_balance {
        CheckOutcome::PersistentFail("spender account reads disagree on collateral")
    } else {
        ensure_live_portfolio_marks(
            state,
            account,
            &standard.state,
            &standard.binding,
            &venue.deposit_wallet(),
        )
        .await
    };
    if portfolio != CheckOutcome::Pass {
        return (
            CheckOutcome::Pass,
            StaticProbe {
                account: account_outcome,
                geoblock,
                balance: portfolio,
                polygon,
            },
        );
    }
    let runtime = state.config.runtime_config.snapshot();
    let risk = match live_risk_audit(
        state,
        account,
        None,
        CollateralAmount::ZERO,
        runtime.per_trade_cap.resolve_bps(TradingMode::LiveTiny),
    )
    .await
    {
        Ok(risk) => risk,
        Err(_) => {
            return (
                CheckOutcome::Pass,
                StaticProbe {
                    account: account_outcome,
                    geoblock,
                    balance: CheckOutcome::Transient("live risk inputs unavailable"),
                    polygon,
                },
            );
        }
    };
    match sync_live_risk_halts(state, account, &risk).await {
        Ok(false) => {}
        Ok(true) => {
            return (
                CheckOutcome::Pass,
                StaticProbe {
                    account: account_outcome,
                    geoblock,
                    balance: CheckOutcome::PersistentFail("global risk halt active"),
                    polygon,
                },
            );
        }
        Err(_) => {
            return (
                CheckOutcome::Pass,
                StaticProbe {
                    account: account_outcome,
                    geoblock,
                    balance: CheckOutcome::Transient("risk halt synchronization unavailable"),
                    polygon,
                },
            );
        }
    }
    let settings = match fetch_account_settings(state, account.account_id.as_str()).await {
        Ok(settings) => settings,
        Err(_) => {
            return (
                CheckOutcome::Pass,
                StaticProbe {
                    account: account_outcome,
                    geoblock,
                    balance: CheckOutcome::Transient("sizing posture unavailable"),
                    polygon,
                },
            );
        }
    };
    let sizing = match settings.sizing_mode(runtime.sizing_mode) {
        Ok(sizing) => sizing,
        Err(_) => {
            return (
                CheckOutcome::Pass,
                StaticProbe {
                    account: account_outcome,
                    geoblock,
                    balance: CheckOutcome::PersistentFail("sizing posture invalid"),
                    polygon,
                },
            );
        }
    };
    let balance = standard
        .state
        .collateral_balance
        .min(neg_risk.state.collateral_balance);
    let posture = arming_posture(
        sizing,
        balance,
        runtime.per_trade_cap.resolve_bps(TradingMode::LiveTiny),
    );
    let mut balance_outcome = if posture == CollateralAmount::ZERO
        || standard.state.collateral_balance < posture
        || neg_risk.state.collateral_balance < posture
        || standard.state.allowance < posture
        || neg_risk.state.allowance < posture
    {
        CheckOutcome::PersistentFail("balance or both-spender allowance below posture")
    } else {
        CheckOutcome::Pass
    };
    if balance_outcome == CheckOutcome::Pass {
        balance_outcome = live_financial_posture(
            state,
            account,
            balance,
            account.effective_live_mode != "live_tiny",
        );
    }
    (
        CheckOutcome::Pass,
        StaticProbe {
            account: account_outcome,
            geoblock,
            balance: balance_outcome,
            polygon,
        },
    )
}

async fn polygon_arming_health(state: &FanoutState) -> CheckOutcome {
    let chain_attempt = state.polygon_receipt_rpc.chain_id().await;
    match response_body(&chain_attempt)
        .ok()
        .and_then(|body| parse_chain_id_response(body).ok())
    {
        Some(pe_venue_polymarket::FINALIZED_CHAIN_ID) => {}
        Some(_) => return CheckOutcome::PersistentFail("wrong chain identity"),
        None => return CheckOutcome::Transient("receipt RPC unavailable"),
    }
    let finalized_attempt = state.polygon_receipt_rpc.finalized_block().await;
    match response_body(&finalized_attempt)
        .ok()
        .and_then(|body| parse_finalized_block_response(body).ok())
    {
        Some(_) => CheckOutcome::Pass,
        None => CheckOutcome::Transient("finalized receipt tag unavailable"),
    }
}

fn baseline_owns_current_binding(
    derived: &ProjectionDerivation,
    current: &LiveAccountBindingAudit,
) -> bool {
    derived.baseline_equity.is_none() || derived.account_binding.as_ref() == Some(current)
}

async fn ensure_live_portfolio_marks(
    state: &mut FanoutState,
    account: &AccountContext,
    account_state: &pe_execution_core::LiveVenueAccountState,
    account_binding: &LiveAccountBindingAudit,
    custody_wallet: &str,
) -> CheckOutcome {
    ensure_live_portfolio_marks_at(
        state,
        account,
        account_state,
        account_binding,
        custody_wallet,
        OffsetDateTime::now_utc(),
    )
    .await
}

async fn ensure_live_portfolio_marks_at(
    state: &mut FanoutState,
    account: &AccountContext,
    account_state: &pe_execution_core::LiveVenueAccountState,
    account_binding: &LiveAccountBindingAudit,
    custody_wallet: &str,
    now: OffsetDateTime,
) -> CheckOutcome {
    let mut events = match replay_live_account(state, &account.account_id) {
        Ok(events) => events,
        Err(_) => return CheckOutcome::Transient("live journal unavailable"),
    };
    let expected_credential = account
        .credential_binding
        .as_ref()
        .map(|(version, key_id)| CredentialBindingIdentity {
            version: *version,
            key_id: key_id.clone(),
        });
    if !account_binding.is_valid_for(&account.account_id)
        || expected_credential.as_ref() != Some(&account_binding.credential)
        || !WalletAddress::from_hex(custody_wallet)
            .is_ok_and(|wallet| wallet.to_string() == account_binding.custody_wallet)
    {
        return CheckOutcome::PersistentFail("live account binding invalid");
    }
    let mut derived = match derive_projection_rows_for_state(state, &account.account_id, &events) {
        Ok(derived) => derived,
        Err(_) => return CheckOutcome::PersistentFail("live financial journal conflicts"),
    };
    if !baseline_owns_current_binding(&derived, account_binding) {
        return CheckOutcome::PersistentFail(
            "current live account binding differs from the financial-era Baseline",
        );
    }
    let inventory =
        match fetch_complete_venue_inventory(state, custody_wallet, account_binding, &events).await
        {
            Ok(inventory) => inventory,
            Err(_) => return CheckOutcome::Transient("complete venue inventory unavailable"),
        };
    let inventory_observed_at = now;
    if derived.baseline_equity.is_none() {
        // The first Baseline is the account's era boundary. Older records remain readable audit
        // history and deliberately create no managed state; only current complete venue inventory
        // can disprove the required empty start.
        if !inventory.positions.is_empty() {
            return CheckOutcome::PersistentFail("nonempty inventory before live Baseline");
        }
        if account_state.collateral_balance == CollateralAmount::ZERO {
            return CheckOutcome::PersistentFail("live Baseline equity is nonpositive");
        }
        let audit = match account_state.audit() {
            Ok(audit) => audit,
            Err(_) => return CheckOutcome::PersistentFail("account audit invalid"),
        };
        if state
            .config
            .journal
            .append(
                account.account_id.clone(),
                inventory_observed_at,
                LiveJournalPayload::AccountPortfolioMarked(Box::new(
                    pe_execution_core::AccountPortfolioMarkedAudit {
                        kind: MarkKind::Baseline,
                        cutoff_unix: inventory_observed_at.unix_timestamp(),
                        account_binding: account_binding.clone(),
                        account_state: audit,
                        venue_positions: inventory.positions.clone(),
                        venue_position_evidence: inventory.evidence.clone(),
                        marked_positions: Vec::new(),
                        prices: Vec::new(),
                        equity: account_state.collateral_balance,
                    },
                )),
            )
            .is_err()
        {
            return CheckOutcome::Transient("live Baseline journal append failed");
        }
        events = match replay_live_account(state, &account.account_id) {
            Ok(events) => events,
            Err(_) => return CheckOutcome::Transient("live Baseline replay failed"),
        };
        derived = match derive_projection_rows_for_state(state, &account.account_id, &events) {
            Ok(derived) => derived,
            Err(_) => return CheckOutcome::PersistentFail("live Baseline conflicts"),
        };
    }

    let expected = match canonical_positions(&derived.custody_positions) {
        Ok(expected) => expected,
        Err(_) => return CheckOutcome::PersistentFail("managed inventory conflicts"),
    };
    let observed = match canonical_positions(&inventory.positions) {
        Ok(observed) => observed,
        Err(_) => return CheckOutcome::PersistentFail("venue inventory conflicts"),
    };
    if expected != observed {
        return CheckOutcome::PersistentFail("venue inventory drift");
    }
    let Some(economic_cash) = derived.economic_cash else {
        return CheckOutcome::Transient("live Baseline unavailable");
    };
    if account_state
        .collateral_balance
        .to_decimal()
        .checked_add(derived.receivable)
        != Some(economic_cash)
    {
        return CheckOutcome::PersistentFail("authenticated cash drift");
    }

    let Some(mut cutoff) = derived
        .baseline_cutoff_unix
        .and_then(|value| value.div_euclid(86_400).checked_add(1))
        .and_then(|day| day.checked_mul(86_400))
    else {
        return CheckOutcome::PersistentFail("live boundary arithmetic overflow");
    };
    let completed_cutoff = now.unix_timestamp().div_euclid(86_400) * 86_400;
    while cutoff <= completed_cutoff {
        let cutoff_is_missing = !derived.daily_marks.contains_key(&cutoff);
        if cutoff_is_missing {
            let cutoff_events = events
                .iter()
                .filter(|event| event.timestamp.unix_timestamp() < cutoff)
                .cloned()
                .collect::<Vec<_>>();
            let Ok(cutoff_sources) =
                source_envelopes_for_live_events(&state.config.source_receipts, &cutoff_events)
            else {
                return CheckOutcome::PersistentFail("daily mark source evidence");
            };
            let cutoff_derived = match derive_projection_rows_with_sources(
                &account.account_id,
                &cutoff_events,
                &cutoff_sources,
            ) {
                Ok(derived) => derived,
                Err(_) => {
                    return CheckOutcome::PersistentFail("cutoff live journal conflicts");
                }
            };
            let mut prices = Vec::new();
            for position in cutoff_derived
                .custody_positions
                .iter()
                .filter(|position| !position.redeemable)
            {
                let price = match fetch_historical_live_mark(state, position, cutoff).await {
                    Ok(price) => price,
                    Err(_) => return CheckOutcome::Transient("daily mark price unavailable"),
                };
                prices.push(price);
            }
            let marked = prices
                .iter()
                .map(|price| {
                    let quantity = cutoff_derived
                        .custody_positions
                        .iter()
                        .find(|position| {
                            position.condition_id == price.condition_id
                                && position.outcome_index == price.outcome_index
                        })
                        .ok_or(())?;
                    Ok((quantity.size, price.price))
                })
                .collect::<Result<Vec<_>, ()>>();
            let Ok(marked) = marked else {
                return CheckOutcome::PersistentFail("daily mark identity conflict");
            };
            let equity = match pe_risk_engine::current_equity(&pe_risk_engine::EquityInputs {
                cash: match cutoff_derived.economic_cash {
                    Some(cash) => cash,
                    None => return CheckOutcome::Transient("cutoff Baseline unavailable"),
                },
                positions: &marked,
            })
            .ok()
            .map(|value| value.round_dp_with_strategy(6, RoundingStrategy::ToNegativeInfinity))
            .and_then(|value| CollateralAmount::from_decimal_exact(value).ok())
            {
                Some(equity) => equity,
                None => return CheckOutcome::PersistentFail("daily mark equity overflow"),
            };
            let audit = match account_state.audit() {
                Ok(audit) => audit,
                Err(_) => return CheckOutcome::PersistentFail("account audit invalid"),
            };
            if state
                .config
                .journal
                .append(
                    account.account_id.clone(),
                    now,
                    LiveJournalPayload::AccountPortfolioMarked(Box::new(
                        pe_execution_core::AccountPortfolioMarkedAudit {
                            kind: MarkKind::Daily,
                            cutoff_unix: cutoff,
                            account_binding: account_binding.clone(),
                            account_state: audit,
                            venue_positions: inventory.positions.clone(),
                            venue_position_evidence: inventory.evidence.clone(),
                            marked_positions: cutoff_derived.custody_positions.clone(),
                            prices,
                            equity,
                        },
                    )),
                )
                .is_err()
            {
                return CheckOutcome::Transient("daily mark journal append failed");
            }
            derived.daily_marks.insert(cutoff, equity);
        }
        cutoff = match cutoff.checked_add(86_400) {
            Some(next) => next,
            None => return CheckOutcome::PersistentFail("live boundary arithmetic overflow"),
        };
    }
    CheckOutcome::Pass
}

async fn fetch_historical_live_mark(
    state: &FanoutState,
    position: &CanonicalPositionAudit,
    cutoff_unix: i64,
) -> Result<pe_execution_core::MarkPrice, RiskInputsUnavailable> {
    let mark = state
        .history_fetcher
        .fetch(&position.token_id.0, cutoff_unix)
        .await
        .map_err(|error| match error {
            crate::risk_inputs::BoundaryMarkError::Invalid(reason) => reason,
            crate::risk_inputs::BoundaryMarkError::Retryable(_) => {
                RiskInputsUnavailable::PriceMissing
            }
            crate::risk_inputs::BoundaryMarkError::SourceLogClosed
            | crate::risk_inputs::BoundaryMarkError::Classification(_) => {
                RiskInputsUnavailable::MarkInvalid
            }
        })?;
    Ok(pe_execution_core::MarkPrice {
        condition_id: position.condition_id.clone(),
        outcome_index: position.outcome_index,
        price: mark.price,
        receipt: mark.receipt,
        observed_unix: mark.sample_unix,
    })
}

fn live_financial_posture(
    state: &FanoutState,
    account: &AccountContext,
    authenticated_cash: CollateralAmount,
    require_empty_inventory: bool,
) -> CheckOutcome {
    let events = match replay_live_account(state, &account.account_id) {
        Ok(events) => events,
        Err(_) => return CheckOutcome::Transient("live journal unavailable"),
    };
    let derived = match derive_projection_rows_for_state(state, &account.account_id, &events) {
        Ok(derived) => derived,
        Err(_) => return CheckOutcome::PersistentFail("live financial journal conflicts"),
    };
    let Some(economic_cash) = derived.economic_cash else {
        return CheckOutcome::Transient("portfolio Baseline unavailable");
    };
    if derived.reserved != Decimal::ZERO {
        return CheckOutcome::Transient("prior live order awaits terminal finality");
    }
    if require_empty_inventory && !derived.custody_positions.is_empty() {
        return CheckOutcome::PersistentFail(
            "managed inventory is nonempty before Baseline arming",
        );
    }
    let Some(accounted_cash) = authenticated_cash
        .to_decimal()
        .checked_add(derived.receivable)
    else {
        return CheckOutcome::PersistentFail("live cash accounting overflow");
    };
    if accounted_cash != economic_cash {
        return CheckOutcome::PersistentFail("authenticated cash drift");
    }
    CheckOutcome::Pass
}

fn arming_posture(sizing: SizingMode, balance: CollateralAmount, cap_bps: i32) -> CollateralAmount {
    let amount = match sizing {
        SizingMode::Dollar { usd } => usd,
        SizingMode::Contract { contracts } => Decimal::from(contracts),
        SizingMode::Kelly => {
            balance.to_decimal() * Decimal::from(cap_bps.clamp(0, 10_000))
                / Decimal::from(10_000i32)
        }
    }
    .max(Decimal::new(1, 6))
    .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero);
    CollateralAmount::from_decimal_exact(amount).unwrap_or(CollateralAmount::ZERO)
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RedeemablePositionRow {
    condition_id: String,
    size: Decimal,
    #[serde(rename = "negativeRisk")]
    neg_risk: bool,
}

#[derive(Clone)]
struct ResolutionDiscovery {
    token_ids: [pe_core_types::PolymarketTokenId; 2],
    payout: Option<BinaryPayoutVector>,
    receipt: AppendReceipt,
}

async fn discover_live_resolutions(
    state: &FanoutState,
    account_id: &AccountId,
    events: &[LiveJournalEvent],
    discoveries: &mut BTreeMap<String, Result<ResolutionDiscovery, &'static str>>,
    now: OffsetDateTime,
) -> Result<(), &'static str> {
    let mut prepared = BTreeMap::<String, Box<pe_execution_core::LiveOrderPreparedAudit>>::new();
    let mut terminal = BTreeSet::new();
    let mut filled_conditions = BTreeSet::new();
    let mut resolved_conditions = BTreeSet::new();
    for event in events {
        match &event.payload {
            LiveJournalPayload::OrderPrepared(order) => {
                prepared.insert(order.identity.idempotency_key.clone(), order.clone());
            }
            LiveJournalPayload::OrderReconciled(reconciled)
                if matches!(
                    reconciled.outcome,
                    LiveJournalOrderOutcome::Killed { .. }
                        | LiveJournalOrderOutcome::Rejected { .. }
                ) =>
            {
                terminal.insert(reconciled.identity.idempotency_key.clone());
            }
            LiveJournalPayload::OrderFillFinalized(fill) => {
                terminal.insert(fill.identity.idempotency_key.clone());
                let order = prepared
                    .get(&fill.identity.idempotency_key)
                    .ok_or("finalized fill has no prepared admission")?;
                filled_conditions.insert(order.economic.market.condition_id.0.clone());
            }
            LiveJournalPayload::ResolutionFinalized(resolution) => {
                resolved_conditions.insert(resolution.condition_id.0.clone());
            }
            _ => {}
        }
    }
    for condition in filled_conditions {
        if resolved_conditions.contains(&condition)
            || prepared.values().any(|order| {
                order.economic.market.condition_id.0 == condition
                    && !terminal.contains(&order.identity.idempotency_key)
            })
        {
            continue;
        }
        let admissions = prepared
            .values()
            .filter(|order| order.economic.market.condition_id.0 == condition)
            .collect::<Vec<_>>();
        let first = admissions
            .first()
            .copied()
            .ok_or("condition admission missing")?;
        if admissions.iter().any(|order| {
            order.economic.admission.market.ordered_outcome_token_ids
                != first.economic.admission.market.ordered_outcome_token_ids
        }) {
            return Err("condition admission mapping conflicts");
        }
        let expected_tokens = &first.economic.admission.market.ordered_outcome_token_ids;
        let discovery = if let Some(discovery) = discoveries.get(&condition) {
            discovery.clone()?
        } else {
            let fetched = async {
                let url = format!(
                    "{}/markets/{condition}",
                    state.config.clob_base_url.trim_end_matches('/')
                );
                let observed_at = OffsetDateTime::now_utc();
                let response = state
                    .config
                    .http
                    .get(&url)
                    .send()
                    .await
                    .map_err(|_| "CLOB resolution transport")?;
                let status = response.status();
                let body = response
                    .bytes()
                    .await
                    .map_err(|_| "CLOB resolution body")?
                    .to_vec();
                let received_at = OffsetDateTime::now_utc();
                let receipt = state
                    .config
                    .source_log
                    .append(EnvelopeIn {
                        source_id: SourceId("polymarket.clob.resolution".to_owned()),
                        schema_version: pe_source_polymarket_public::CLOB_RESOLUTION_SCHEMA_VERSION,
                        parser_version: pe_source_polymarket_public::CLOB_RESOLUTION_PARSER_VERSION,
                        observed_at: SourceTimestamp(observed_at),
                        received_at: ReceivedAt(received_at),
                        content_type: ContentType::Json,
                        payload: body.clone(),
                    })
                    .await
                    .map_err(|_| "resolution source append")?;
                if !status.is_success() {
                    return Err("CLOB resolution status");
                }
                let market = parse_clob_market(&body).map_err(|_| "CLOB resolution parse")?;
                if market
                    .condition_id
                    .as_deref()
                    .is_none_or(|value| !value.eq_ignore_ascii_case(&condition))
                {
                    return Err("CLOB resolution condition mismatch");
                }
                let token_ids: [pe_core_types::PolymarketTokenId; 2] = market
                    .tokens
                    .iter()
                    .map(|token| {
                        token
                            .token_id
                            .as_ref()
                            .map(|token| pe_core_types::PolymarketTokenId(token.clone()))
                            .ok_or("CLOB resolution token mapping missing")
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .try_into()
                    .map_err(|_| "CLOB resolution token mapping cardinality")?;
                let payout = match market.resolution_evidence().payout {
                    ClobPayoutResolution::Resolved(payout) => Some(payout),
                    ClobPayoutResolution::Unresolved(_) => None,
                };
                Ok(ResolutionDiscovery {
                    token_ids,
                    payout,
                    receipt,
                })
            }
            .await;
            discoveries.insert(condition.clone(), fetched.clone());
            fetched?
        };
        if &discovery.token_ids != expected_tokens {
            return Err("CLOB resolution token mapping mismatch");
        }
        let Some(payout) = discovery.payout else {
            continue;
        };
        state
            .config
            .journal
            .append(
                account_id.clone(),
                now,
                LiveJournalPayload::ResolutionFinalized(Box::new(
                    pe_execution_core::ResolutionFinalizedAudit {
                        condition_id: PolymarketConditionId(condition),
                        payout_by_outcome_index_json: payout.canonical_json(),
                        source_append_receipt: discovery.receipt,
                    },
                )),
            )
            .map_err(|_| "resolution journal append")?;
    }
    Ok(())
}

/// Redemption discovery and submission remain fail-closed for every custody kind except the
/// officially documented Deposit Wallet EIP-712 batch path.
async fn drive_redemptions(state: &mut FanoutState, now: OffsetDateTime) {
    let snapshot = state.config.live_accounts.snapshot();
    let mut account_ids = snapshot
        .accounts
        .iter()
        .map(|account| account.account_id.clone())
        .collect::<BTreeSet<_>>();
    if let Ok(inventory) = recovery_inventory(
        &state.config.journal_path,
        state.config.era_live_prefix.as_ref(),
    ) {
        account_ids.extend(inventory.account_ids);
    }
    state
        .closures
        .redemption
        .retain(|account, _| account_ids.iter().any(|id| id.as_str() == account));
    let mut resolution_discoveries = BTreeMap::new();
    for account_id in account_ids {
        let events = match replay_live_account(state, &account_id) {
            Ok(events) => events,
            Err(error) => {
                error!(account_id = %account_id, error = %error, "redemption journal replay failed; attempt frozen");
                state.closures.redemption.insert(
                    account_id.as_str().to_owned(),
                    "redemption journal unavailable; attempt frozen".to_owned(),
                );
                continue;
            }
        };
        if let Err(reason) = discover_live_resolutions(
            state,
            &account_id,
            &events,
            &mut resolution_discoveries,
            now,
        )
        .await
        {
            state.closures.redemption.insert(
                account_id.as_str().to_owned(),
                format!("resolution discovery unavailable: {reason}"),
            );
            continue;
        }
        let Some(account) = snapshot
            .accounts
            .iter()
            .find(|account| account.account_id == account_id)
            .cloned()
            .or_else(|| journal_recovery_account(&account_id, &events))
        else {
            state.closures.redemption.insert(
                account_id.as_str().to_owned(),
                "journal account has no recoverable credential binding".to_owned(),
            );
            continue;
        };
        let events = match replay_live_account(state, &account.account_id) {
            Ok(events) => events,
            Err(_) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    "resolution replay unavailable".to_owned(),
                );
                continue;
            }
        };
        let mut attempts = reconstruct_redemption_attempts(&events);
        let incomplete_attempt = attempts
            .values()
            .any(|attempt| !matches!(attempt.state, RedemptionAttemptState::Complete { .. }));
        let has_receivable = derive_projection_rows_for_state(state, &account.account_id, &events)
            .is_ok_and(|derived| derived.receivable != Decimal::ZERO);
        if !account.is_armed() && !incomplete_attempt && !has_receivable {
            state
                .closures
                .redemption
                .remove(account.account_id.as_str());
            continue;
        }
        let target = DispatchTargetRow {
            dispatch_id: "redemption-probe".to_owned(),
            account_id: account.account_id.as_str().to_owned(),
            exec_rank: 0,
            credential_bundle_version: account
                .credential_binding
                .as_ref()
                .map(|binding| binding.0)
                .unwrap_or_default(),
            credential_key_id: account
                .credential_binding
                .as_ref()
                .map(|binding| binding.1.clone())
                .unwrap_or_default(),
            state: "pending".to_owned(),
            terminal_reason: None,
            updated_at_unix: now.unix_timestamp(),
        };
        let CredentialLoad::Ready(credentials) = credentials_for_target(state, &target).await
        else {
            state.closures.redemption.insert(
                account.account_id.as_str().to_owned(),
                "redemption credentials unavailable".to_owned(),
            );
            continue;
        };
        let venue = match PolymarketLiveVenue::from_credentials(&credentials).await {
            Ok(venue) => venue,
            Err(error) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption venue unavailable: {error}"),
                );
                continue;
            }
        };
        let inventory = match fetch_complete_venue_inventory(
            state,
            &venue.deposit_wallet(),
            venue.account_binding(),
            &events,
        )
        .await
        {
            Ok(inventory) => inventory,
            Err(reason) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption inventory unavailable: {reason}"),
                );
                continue;
            }
        };
        let positions = inventory.redeemable.clone();
        match append_ready_redemption_custody(
            state,
            &account,
            &venue,
            &inventory,
            attempts.values(),
            now,
        )
        .await
        {
            Ok(true) => {
                state
                    .closures
                    .redemption
                    .remove(account.account_id.as_str());
                continue;
            }
            Ok(false) => {}
            Err(reason) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption custody unavailable: {reason}"),
                );
                continue;
            }
        }
        if positions.is_empty() && attempts.is_empty() {
            state
                .closures
                .redemption
                .remove(account.account_id.as_str());
            continue;
        }

        let Some(custody) = custody_kind(account.custody_wallet_kind.as_deref()) else {
            state.closures.redemption.insert(
                account.account_id.as_str().to_owned(),
                "redemption custody kind is missing or unknown".to_owned(),
            );
            continue;
        };
        if custody != CustodyKind::DepositWallet {
            state.closures.redemption.insert(
                account.account_id.as_str().to_owned(),
                format!("redemption submission unsupported for {custody:?} custody"),
            );
            continue;
        }
        let Some(api_key) = credentials.relayer_api_key.as_ref() else {
            state.closures.redemption.insert(
                account.account_id.as_str().to_owned(),
                "redemption Relayer API key is unavailable".to_owned(),
            );
            continue;
        };
        let owner_signer = venue.owner_signer();
        let relayer_credentials = RelayerCredentials::RelayerApiKey(RelayerApiKeyCredentials {
            api_key: api_key.clone(),
            address: owner_signer.clone(),
        });
        let policy = RelayerPollPolicy {
            request_timeout: Duration::from_secs(10),
            poll_interval: Duration::from_secs(2),
            maximum_polls: 5,
        };
        let adapter = match LiveRedemptionAdapter::new(relayer_credentials, custody, policy) {
            Ok(adapter) => adapter,
            Err(error) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption adapter unavailable: {error}"),
                );
                continue;
            }
        };
        let custody_wallet = account
            .custody_wallet_address
            .clone()
            .unwrap_or_else(|| venue.deposit_wallet());
        if !custody_wallet.eq_ignore_ascii_case(&venue.deposit_wallet()) {
            state.closures.redemption.insert(
                account.account_id.as_str().to_owned(),
                "redemption custody wallet does not match decrypted credentials".to_owned(),
            );
            continue;
        }

        let reconcile_first = attempts
            .values()
            .filter(|attempt| !redemption_needs_fresh_submission(&attempt.state, now))
            .min_by(|left, right| {
                left.identity
                    .condition_id
                    .0
                    .cmp(&right.identity.condition_id.0)
            })
            .cloned();
        if let Some(attempt) = reconcile_first {
            let redeemable = redeemable_for_attempt(&positions, &attempt.identity);
            let result = run_redemption_pass(
                &adapter,
                &adapter,
                state.config.journal.as_ref(),
                RedemptionPassInput {
                    attempt,
                    now,
                    resolved_winner_redeemable: redeemable,
                    signed_request: None,
                    request_hash: None,
                    retry_not_before_on_failure: now
                        + time::Duration::seconds(REDEMPTION_RETRY_SECS),
                },
            )
            .await;
            let closure_reason = match result {
                Ok(result) => redemption_closure_reason(result.attempt),
                Err(error) => Some(format!("redemption driver failed: {error}")),
            };
            set_redemption_closure(state, &account, closure_reason);
            continue;
        }

        let Some(position) = positions.first() else {
            state
                .closures
                .redemption
                .remove(account.account_id.as_str());
            continue;
        };
        let call = match build_redemption_call(
            PolymarketConditionId(position.condition_id.clone()),
            position.neg_risk,
        ) {
            Ok(call) => call,
            Err(error) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption call construction failed: {error}"),
                );
                continue;
            }
        };
        let identity = RedemptionAttemptIdentity {
            account_id: account.account_id.clone(),
            condition_id: call.condition_id.clone(),
            adapter: call.to.clone(),
            custody_wallet: custody_wallet.clone(),
        };
        let attempt = attempts.remove(&identity).unwrap_or(RedemptionAttempt {
            identity,
            state: RedemptionAttemptState::default(),
        });
        let nonce = match adapter.fetch_nonce(&owner_signer, custody).await {
            Ok(nonce) => nonce,
            Err(error) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption nonce unavailable: {error}"),
                );
                continue;
            }
        };
        let deadline_unix = match u64::try_from(now.unix_timestamp())
            .ok()
            .and_then(|timestamp| timestamp.checked_add(DEPOSIT_WALLET_REDEMPTION_DEADLINE_SECS))
        {
            Some(deadline) => deadline,
            None => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    "redemption deadline overflow".to_owned(),
                );
                continue;
            }
        };
        let signed_request = match sign_deposit_wallet_redemption(
            &credentials.private_key,
            &call,
            &custody_wallet,
            &nonce.nonce,
            deadline_unix,
        ) {
            Ok(request) => request,
            Err(error) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption signing failed: {error}"),
                );
                continue;
            }
        };
        let request_hash = match adapter.submission_body_hash(&signed_request, now.unix_timestamp())
        {
            Ok(hash) => hash,
            Err(error) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption request validation failed: {error}"),
                );
                continue;
            }
        };
        let redeemable = redeemable_amount(position.size);
        let closure_reason = match run_redemption_pass(
            &adapter,
            &adapter,
            state.config.journal.as_ref(),
            RedemptionPassInput {
                attempt,
                now,
                resolved_winner_redeemable: redeemable,
                signed_request: Some(&signed_request),
                request_hash: Some(&request_hash),
                retry_not_before_on_failure: now + time::Duration::seconds(REDEMPTION_RETRY_SECS),
            },
        )
        .await
        {
            Ok(result) => redemption_closure_reason(result.attempt),
            Err(error) => Some(format!("redemption driver failed: {error}")),
        };
        set_redemption_closure(state, &account, closure_reason);
    }
}

fn journal_recovery_account(
    account_id: &AccountId,
    events: &[LiveJournalEvent],
) -> Option<AccountContext> {
    let prepared = events.iter().rev().find_map(|event| match &event.payload {
        LiveJournalPayload::OrderPrepared(prepared) => Some(prepared.as_ref()),
        _ => None,
    })?;
    Some(AccountContext {
        account_id: account_id.clone(),
        is_primary: false,
        enabled: false,
        execution_order: i64::MAX,
        requested_live_mode: "off".to_owned(),
        effective_live_mode: "off".to_owned(),
        live_price_impact_cap_bps: 0,
        custody_wallet_address: Some(prepared.prepared.funder.clone()),
        custody_wallet_kind: Some("deposit_wallet".to_owned()),
        credential_binding: Some((
            prepared.frozen_binding.version,
            prepared.frozen_binding.key_id.clone(),
        )),
    })
}

fn redemption_needs_fresh_submission(state: &RedemptionAttemptState, now: OffsetDateTime) -> bool {
    match state {
        RedemptionAttemptState::Idle { .. } | RedemptionAttemptState::Complete { .. } => true,
        RedemptionAttemptState::Failed {
            retry_not_before, ..
        } => now >= *retry_not_before,
        RedemptionAttemptState::SubmissionReserved { .. }
        | RedemptionAttemptState::InFlight { .. }
        | RedemptionAttemptState::Ambiguous { .. }
        | RedemptionAttemptState::ConfirmedAwaitingBalance { .. } => false,
    }
}

async fn append_ready_redemption_custody<'a>(
    state: &FanoutState,
    account: &AccountContext,
    venue: &PolymarketLiveVenue,
    inventory: &CompleteVenueInventory,
    attempts: impl Iterator<Item = &'a RedemptionAttempt>,
    now: OffsetDateTime,
) -> Result<bool, &'static str> {
    let Some(attempt) = attempts
        .filter(|attempt| {
            matches!(
                attempt.state,
                RedemptionAttemptState::ConfirmedAwaitingBalance { .. }
            )
        })
        .min_by(|left, right| {
            left.identity
                .condition_id
                .0
                .cmp(&right.identity.condition_id.0)
        })
    else {
        return Ok(false);
    };
    let events = replay_live_account(state, &account.account_id).map_err(|_| "journal replay")?;
    let derived = derive_projection_rows_for_state(state, &account.account_id, &events)
        .map_err(|_| "journal reducer")?;
    let credit = derived
        .receivable_by_condition
        .get(&attempt.identity.condition_id.0)
        .copied()
        .ok_or("condition receivable missing")?;
    let expected = derived
        .custody_positions
        .iter()
        .filter(|position| position.condition_id != attempt.identity.condition_id)
        .cloned()
        .collect::<Vec<_>>();
    if canonical_positions(&expected).map_err(|_| "expected inventory")?
        != canonical_positions(&inventory.positions).map_err(|_| "venue inventory")?
    {
        return Err("remaining inventory mismatch");
    }
    let account_state = venue
        .read_balance_and_allowance(false)
        .await
        .map_err(|_| "account state")?;
    let remaining_receivable = derived
        .receivable
        .checked_sub(credit.to_decimal())
        .ok_or("receivable arithmetic")?;
    if account_state
        .collateral_balance
        .to_decimal()
        .checked_add(remaining_receivable)
        != derived.economic_cash
    {
        return Err("cash reconciliation mismatch");
    }
    state
        .config
        .journal
        .append(
            account.account_id.clone(),
            now,
            LiveJournalPayload::RedemptionCustodyReconciled(Box::new(
                pe_execution_core::RedemptionCustodyReconciledAudit {
                    identity: attempt.identity.clone(),
                    account_state: account_state.audit().map_err(|_| "account audit")?,
                    venue_positions: inventory.positions.clone(),
                    venue_position_receipts: inventory
                        .evidence
                        .pages
                        .iter()
                        .map(|page| page.receipt)
                        .collect(),
                },
            )),
        )
        .map_err(|_| "journal append")?;
    Ok(true)
}

fn redeemable_amount(size: Decimal) -> CollateralAmount {
    CollateralAmount::from_decimal_exact(
        size.max(Decimal::ZERO)
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero),
    )
    .unwrap_or(CollateralAmount::ZERO)
}

fn redeemable_for_attempt(
    positions: &[RedeemablePositionRow],
    identity: &RedemptionAttemptIdentity,
) -> CollateralAmount {
    positions
        .iter()
        .find(|position| position.condition_id == identity.condition_id.0)
        .map_or(CollateralAmount::ZERO, |position| {
            redeemable_amount(position.size)
        })
}

fn redemption_closure_reason(attempt: RedemptionAttempt) -> Option<String> {
    if matches!(
        attempt.state,
        RedemptionAttemptState::SubmissionReserved { .. }
            | RedemptionAttemptState::Ambiguous {
                transaction_id: None,
                ..
            }
    ) {
        error!(
            account_id = %attempt.identity.account_id,
            condition_id = %attempt.identity.condition_id.0,
            "redemption submission identity cannot be reconstructed; attempt remains frozen"
        );
        return Some("redemption submission identity unavailable; attempt frozen".to_owned());
    }
    let posture = redemption_posture(&attempt.state);
    if posture.surface_prominently {
        Some("redemption unresolved after repeated attempts".to_owned())
    } else if posture.closes_new_buy_admission {
        Some("redemption pending".to_owned())
    } else {
        None
    }
}

fn set_redemption_closure(
    state: &mut FanoutState,
    account: &AccountContext,
    reason: Option<String>,
) {
    if let Some(reason) = reason {
        state
            .closures
            .redemption
            .insert(account.account_id.as_str().to_owned(), reason);
    } else {
        state
            .closures
            .redemption
            .remove(account.account_id.as_str());
    }
}

fn custody_kind(value: Option<&str>) -> Option<CustodyKind> {
    match value {
        Some("deposit_wallet") => Some(CustodyKind::DepositWallet),
        Some("proxy") => Some(CustodyKind::Proxy),
        Some("safe") => Some(CustodyKind::Safe),
        Some("eoa") => Some(CustodyKind::Eoa),
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct RecordedPositionPage {
    request_url: String,
    raw_hash: String,
    receipt: AppendReceipt,
}

struct LivePositionFetcher {
    http: reqwest::Client,
    source_log: SourceLogHandle,
    account_binding: LiveAccountBindingAudit,
    pages: Mutex<Vec<RecordedPositionPage>>,
}

impl ReconciliationFetcher for LivePositionFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let request =
                position_request_descriptor(&self.account_binding, url).ok_or_else(|| {
                    SourceError::Fatal {
                        message: "complete-position request descriptor is invalid".to_owned(),
                    }
                })?;
            let observed_at = OffsetDateTime::now_utc();
            let response =
                self.http
                    .get(url)
                    .send()
                    .await
                    .map_err(|error| SourceError::Transient {
                        message: error.to_string(),
                    })?;
            let status = response.status();
            let body = response
                .bytes()
                .await
                .map_err(|error| SourceError::Transient {
                    message: error.to_string(),
                })?
                .to_vec();
            let received_at = OffsetDateTime::now_utc();
            let payload = serde_json::to_vec(&RetainedPositionResponse {
                request,
                body: body.clone(),
            })
            .map_err(|error| SourceError::Fatal {
                message: error.to_string(),
            })?;
            let receipt = self
                .source_log
                .append(EnvelopeIn {
                    source_id: SourceId(COMPLETE_POSITIONS_SOURCE_ID.to_owned()),
                    schema_version: COMPLETE_POSITIONS_SCHEMA_VERSION,
                    parser_version: 1,
                    observed_at: SourceTimestamp(observed_at),
                    received_at: ReceivedAt(received_at),
                    content_type: ContentType::Json,
                    payload,
                })
                .await
                .map_err(|error| SourceError::Fatal {
                    message: error.to_string(),
                })?;
            self.pages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(RecordedPositionPage {
                    request_url: url.to_owned(),
                    raw_hash: blake3::hash(&body).to_hex().to_string(),
                    receipt,
                });
            if !status.is_success() {
                return Err(if status.as_u16() == 429 {
                    SourceError::RateLimited {
                        retry_after_secs: 0,
                    }
                } else {
                    SourceError::Transient {
                        message: format!("positions status {}", status.as_u16()),
                    }
                });
            }
            Ok(body)
        })
    }
}

struct CompleteVenueInventory {
    positions: Vec<CanonicalPositionAudit>,
    evidence: LivePositionEvidenceAudit,
    redeemable: Vec<RedeemablePositionRow>,
}

async fn fetch_complete_venue_inventory(
    state: &FanoutState,
    wallet: &str,
    account_binding: &LiveAccountBindingAudit,
    events: &[LiveJournalEvent],
) -> Result<CompleteVenueInventory, &'static str> {
    let wallet = WalletAddress::from_hex(wallet).map_err(|_| "invalid custody wallet")?;
    let requested_wallet = wallet.to_string();
    if requested_wallet != account_binding.custody_wallet {
        return Err("custody wallet does not match account request binding");
    }
    let mut mapping = ActivityAssetMapping::from_rows(&[]);
    for event in events {
        if let LiveJournalPayload::OrderPrepared(prepared) = &event.payload {
            for (outcome, token) in prepared
                .economic
                .admission
                .market
                .ordered_outcome_token_ids
                .iter()
                .enumerate()
            {
                mapping
                    .insert_verified_ordinary(
                        token.clone(),
                        prepared.economic.market.condition_id.clone(),
                        OutcomeId(u16::try_from(outcome).map_err(|_| "outcome overflow")?),
                    )
                    .map_err(|_| "journal admission mapping conflict")?;
            }
        }
    }
    let fetcher = LivePositionFetcher {
        http: state.config.http.clone(),
        source_log: state.config.source_log.clone(),
        account_binding: account_binding.clone(),
        pages: Mutex::new(Vec::new()),
    };
    let complete =
        fetch_complete_positions(&fetcher, &state.config.data_base_url, wallet, &mapping)
            .await
            .map_err(|_| "complete position read failed")?;
    let mut recorded = fetcher
        .pages
        .into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut pages = Vec::with_capacity(complete.pages.len());
    for page in &complete.pages {
        let index = recorded
            .iter()
            .position(|candidate| {
                candidate.request_url == page.request_url
                    && candidate.raw_hash == page.raw_page_hash
            })
            .ok_or("position page receipt mismatch")?;
        let recorded_page = recorded.remove(index);
        pages.push(LivePositionPageAudit {
            request_identity: position_request_identity(&recorded_page.request_url)
                .ok_or("position page request identity invalid")?,
            receipt: recorded_page.receipt,
        });
    }
    if !recorded.is_empty() {
        return Err("unmatched position page receipt");
    }
    let positions = complete
        .positions
        .iter()
        .map(|position| {
            Ok::<CanonicalPositionAudit, &'static str>(CanonicalPositionAudit {
                condition_id: position.condition_id.clone(),
                outcome_index: u8::try_from(position.outcome.0).map_err(|_| "outcome overflow")?,
                token_id: position.asset.clone(),
                size: position.size,
                redeemable: position.redeemable,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut by_condition = BTreeMap::<String, (Decimal, bool)>::new();
    for position in complete
        .positions
        .iter()
        .filter(|position| position.redeemable)
    {
        let entry = by_condition
            .entry(position.condition_id.0.clone())
            .or_insert((Decimal::ZERO, position.neg_risk));
        if entry.1 != position.neg_risk {
            return Err("condition negativeRisk conflict");
        }
        entry.0 = entry
            .0
            .checked_add(position.size.to_decimal())
            .ok_or("redeemable size overflow")?;
    }
    let redeemable = by_condition
        .into_iter()
        .map(|(condition_id, (size, neg_risk))| RedeemablePositionRow {
            condition_id,
            size,
            neg_risk,
        })
        .collect();
    Ok(CompleteVenueInventory {
        positions,
        evidence: LivePositionEvidenceAudit {
            requested_wallet,
            pages,
        },
        redeemable,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::panic, clippy::unwrap_used)]

    use std::collections::HashSet;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};

    use pe_core_types::SourceTimestamp;
    use pe_core_types::{
        BasisPoints, EventSeq, LeaderAction, MarketId, OutcomeId, ProbabilityPpm,
        ReconstructionQuality, Side, SourceTradeId, TraderId, VenueId, VenueMarketId,
        WalletAddress,
    };
    use pe_execution_core::{
        BalanceAudit, ECONOMIC_PREPARED_VERSION, FeeAudit, LadderPlanAudit, LiveAccountReadFailure,
        LiveAdmissionArtifactAudit, LiveOrderRejectKind, LivePostClassification,
        LivePostParseError, LiveVenueAccountReadError, LiveVenueAccountState,
        LiveVenuePreparationError, LiveVenuePrepareRequest, LiveVenuePrepared,
        LiveVenueReconciliation, LiveVenueReconciliationError, MarketSelection, MatchedLogIdentity,
        SizingAudit,
    };
    use pe_paper_state::{DispatchSeedRecord, DispatchTargetSeed};
    use pe_trader_index::Watchlist;
    use pe_venue_polymarket::NEGRISK_COLLATERAL_ADAPTER;
    use rust_decimal_macros::dec;
    use tempfile::tempdir;

    use super::*;
    use crate::config::ServiceConfig;
    use crate::live_accounts::{AccountRow, CredentialMetaRow, LiveAccountsSnapshot};
    use crate::runtime_config::RuntimeConfig;

    #[test]
    fn economic_live_order_rejects_split_market_identity() {
        let condition = PolymarketConditionId("condition".to_owned());
        assert!(validate_live_market_identity("condition", &condition).is_ok());
        assert!(matches!(
            validate_live_market_identity("other-condition", &condition),
            Err(FanoutError::Signal(_))
        ));
    }

    #[test]
    fn outcome_to_target_state_mapping_and_freeze_rule() {
        let matched = LiveOrderOutcome::Matched {
            order_hash: "hash".to_owned(),
            venue_order_id: "id".to_owned(),
            transaction_hashes: Vec::new(),
            executed: None,
        };
        let killed = LiveOrderOutcome::Killed {
            order_hash: "hash".to_owned(),
            venue_order_id: None,
        };
        let rejected = LiveOrderOutcome::Rejected {
            order_hash: Some("hash".to_owned()),
            venue_order_id: None,
            kind: LiveOrderRejectKind::VenueRejected,
        };
        let ambiguous = LiveOrderOutcome::Ambiguous {
            order_hash: "hash".to_owned(),
            kind: LiveOrderAmbiguityKind::Timeout,
            reconcile_first: true,
        };
        let transition = outcome_transition(&matched);
        assert_eq!(transition.state, "submitted");
        assert_eq!(transition.reason, None);
        assert!(!transition.freeze);
        for (outcome, reason) in [(killed, "killed"), (rejected, "rejected")] {
            let transition = outcome_transition(&outcome);
            assert_eq!(transition.state, "terminal");
            assert_eq!(transition.reason, Some(reason));
            assert!(!transition.freeze);
        }
        let transition = outcome_transition(&ambiguous);
        assert_eq!(transition.state, "ambiguous");
        assert_eq!(transition.reason, None);
        assert!(transition.freeze);
    }

    #[test]
    fn due_is_deterministic_and_immediate_on_first_pass() {
        assert!(due(None, 1_000, 30));
        assert!(!due(Some(990), 1_000, 30));
        assert!(due(Some(970), 1_000, 30));
    }

    /// PASS: the pre-POST guard detects both a crossed UTC risk day and admission evidence whose
    /// sixty-second freshness boundary elapsed after preparation.
    #[test]
    fn pre_post_guard_detects_day_and_admission_freshness_boundaries() {
        let prepared = finality_prepared();
        let fresh = OffsetDateTime::from_unix_timestamp(30).unwrap();
        assert!(!prepared_risk_day_changed(&prepared, fresh));
        assert!(prepared_admission_is_approved_at(&prepared, fresh));

        let next_day = OffsetDateTime::from_unix_timestamp(86_400).unwrap();
        assert!(prepared_risk_day_changed(&prepared, next_day));
        let stale = OffsetDateTime::from_unix_timestamp(61).unwrap();
        assert!(!prepared_admission_is_approved_at(&prepared, stale));
    }

    /// PASS: the shared ladder-age classifier accepts ages 1,999 and 2,000 ms and rejects
    /// 2,001 ms, preserving the venue owner's strict-greater-than expiry contract.
    /// FAIL: either inclusive boundary is stale or the first over-age millisecond is fresh.
    #[test]
    fn ladder_age_classifier_preserves_millisecond_boundary() {
        let observed_unix_ms = 1_700_000_000_000;
        assert!(!ladder_observation_is_stale(
            observed_unix_ms + 1_999,
            observed_unix_ms
        ));
        assert!(!ladder_observation_is_stale(
            observed_unix_ms + 2_000,
            observed_unix_ms
        ));
        assert!(ladder_observation_is_stale(
            observed_unix_ms + 2_001,
            observed_unix_ms
        ));
    }

    #[test]
    fn applied_mode_transition_reason_is_typed_for_the_journal() {
        assert_eq!(
            mode_transition_reason("armed: all checks passed"),
            LiveModeTransitionReason::Armed
        );
        assert_eq!(
            mode_transition_reason("demoted: credentials — invalid"),
            LiveModeTransitionReason::CredentialInvalidated
        );
        assert_eq!(
            mode_transition_reason("operator requested off/disabled"),
            LiveModeTransitionReason::OperatorKill
        );
    }

    #[test]
    fn promotion_event_read_is_account_and_kind_scoped_with_total_order() {
        let account_id = AccountId::new("victim").unwrap();
        let url = promotion_event_url("https://example.test/", &account_id, "promotion_reviewed");
        assert!(url.contains("select=event_id,account_id,event_kind,created_at,evidence_ref"));
        assert!(url.contains("account_id=eq.victim"));
        assert!(url.contains("event_kind=eq.promotion_reviewed"));
        assert!(url.contains("order=created_at.desc,event_id.desc"));
        assert!(url.ends_with("limit=1"));
    }

    async fn promotion_events_page(
        State(events): State<Arc<Vec<serde_json::Value>>>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<Vec<serde_json::Value>> {
        assert_eq!(
            query.get("select").map(String::as_str),
            Some("event_id,account_id,event_kind,created_at,evidence_ref")
        );
        assert_eq!(
            query.get("order").map(String::as_str),
            Some("created_at.desc,event_id.desc")
        );
        assert_eq!(query.get("limit").map(String::as_str), Some("1"));
        let account_id = query
            .get("account_id")
            .and_then(|value| value.strip_prefix("eq."))
            .unwrap();
        let event_kind = query
            .get("event_kind")
            .and_then(|value| value.strip_prefix("eq."))
            .unwrap();
        let mut selected = events
            .iter()
            .filter(|event| {
                event.get("account_id").and_then(serde_json::Value::as_str) == Some(account_id)
                    && event.get("event_kind").and_then(serde_json::Value::as_str)
                        == Some(event_kind)
            })
            .cloned()
            .collect::<Vec<_>>();
        selected.sort_by(|left, right| {
            right["created_at"]
                .as_str()
                .cmp(&left["created_at"].as_str())
                .then_with(|| right["event_id"].as_i64().cmp(&left["event_id"].as_i64()))
        });
        selected.truncate(1);
        Json(selected)
    }

    /// PASS: an account's latest review and revocation remain available when 50 newer events from
    /// distinct other accounts would put all of its evidence beyond a global 50-row window.
    #[tokio::test]
    async fn promotion_fetch_is_not_truncated_by_cross_account_events() {
        let mut events = (0..50)
            .map(|index| {
                serde_json::json!({
                    "event_id": 100 + index,
                    "account_id": format!("other-{index}"),
                    "event_kind": "promotion_reviewed",
                    "created_at": "2026-09-02T00:00:00Z",
                    "evidence_ref": null
                })
            })
            .collect::<Vec<_>>();
        let old_digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let selected_digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        events.extend([
            serde_json::json!({
                "event_id": 1,
                "account_id": "victim",
                "event_kind": "promotion_reviewed",
                "created_at": "2026-09-01T00:00:00Z",
                "evidence_ref": format!("{old_digest}:{old_digest}")
            }),
            serde_json::json!({
                "event_id": 2,
                "account_id": "victim",
                "event_kind": "promotion_reviewed",
                "created_at": "2026-09-01T00:00:00Z",
                "evidence_ref": format!("{selected_digest}:{selected_digest}")
            }),
            serde_json::json!({
                "event_id": 3,
                "account_id": "victim",
                "event_kind": "promotion_review_revoked",
                "created_at": "2026-08-31T00:00:00Z",
                "evidence_ref": null
            }),
        ]);
        let app = Router::new()
            .route("/rest/v1/account_events", get(promotion_events_page))
            .with_state(Arc::new(events));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let victim = AccountId::new("victim").unwrap();

        let promotions = fetch_promotions_from(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            "token",
            &[victim],
        )
        .await
        .unwrap();
        let facts = promotions.get("victim").unwrap();

        assert_eq!(
            facts.latest_review_seal_hash.as_deref(),
            Some(selected_digest),
            "event_id breaks equal-created_at ties"
        );
        assert_eq!(
            facts.latest_review_report_blake3.as_deref(),
            Some(selected_digest)
        );
        assert!(facts.latest_revocation_unix < facts.latest_review_unix);
    }

    #[test]
    fn redemption_reconcile_cadence_is_named_five_minutes() {
        assert_eq!(REDEMPTION_RECONCILE_CADENCE_SECS, 300);
    }

    fn latency_posted_event(seq: u64, received_unix_ms: i64, latency_ms: i64) -> LiveJournalEvent {
        latency_posted_event_appended_at(seq, received_unix_ms, latency_ms, received_unix_ms)
    }

    fn latency_posted_event_appended_at(
        seq: u64,
        received_unix_ms: i64,
        latency_ms: i64,
        appended_unix_ms: i64,
    ) -> LiveJournalEvent {
        let received_at =
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(received_unix_ms) * 1_000_000)
                .unwrap();
        let appended_at =
            OffsetDateTime::from_unix_timestamp_nanos(i128::from(appended_unix_ms) * 1_000_000)
                .unwrap();
        let observed_at = received_at - time::Duration::milliseconds(latency_ms);
        LiveJournalEvent {
            account_id: AccountId::new("latency-account").unwrap(),
            seq,
            timestamp: appended_at,
            payload: LiveJournalPayload::OrderPosted(Box::new(
                pe_execution_core::LiveOrderPostAudit {
                    identity: projection_identity(&format!("latency-{seq}")),
                    order_hash: format!("order-{seq}"),
                    evidence: RawHttpAttempt::Response(pe_core_types::RawHttpResponse {
                        source_id: "polymarket-clob-v2".to_owned(),
                        endpoint_kind: "order-post".to_owned(),
                        method: "POST".to_owned(),
                        path: "/order".to_owned(),
                        ordered_query: Vec::new(),
                        status: 200,
                        headers: Vec::new(),
                        body: b"{}".to_vec(),
                        attempt_ordinal: 1,
                        source_at: None,
                        observed_at,
                        received_at,
                        schema_version: 1,
                        parser_version: 1,
                        adapter_version: "fixture".to_owned(),
                    }),
                    evidence_hash: format!("evidence-{seq}"),
                },
            )),
        }
    }

    /// PASS: the live owner engages on two consecutive completed high-latency hours and releases
    /// on the first completed hour at the release threshold.
    #[test]
    fn live_post_latency_window_engages_and_releases_hysteretically() {
        let mut events = vec![
            latency_posted_event(0, 3_601_000, 3_001),
            latency_posted_event(1, 7_201_000, 3_001),
        ];
        let unseeded = crate::risk_inputs::LatencyHysteresisSeed {
            active: false,
            checkpoint: None,
        };
        let after_two_hours = OffsetDateTime::from_unix_timestamp(3 * 3_600 + 1).unwrap();
        assert!(live_latency_switch(&events, after_two_hours, unseeded).unwrap());

        events.push(latency_posted_event(2, 10_801_000, 2_000));
        let after_release_hour = OffsetDateTime::from_unix_timestamp(4 * 3_600 + 1).unwrap();
        assert!(!live_latency_switch(&events, after_release_hour, unseeded).unwrap());
    }

    /// PASS: a synchronized manual release checkpoints the old high pair, so the first pass and
    /// first new high hour stay released; only a second adjacent post-release high hour re-engages.
    #[test]
    fn live_post_latency_release_survives_restart_without_old_pair_reengagement() {
        let mut events = vec![
            latency_posted_event(0, 3_601_000, 3_001),
            latency_posted_event(1, 7_201_000, 3_001),
        ];
        let owner = RiskHaltOwner::LiveAccount(AccountId::new("latency-account").unwrap());
        let released_at = OffsetDateTime::from_unix_timestamp(7_202).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let paper_log = dir.path().join("paper.log");
        let mut writer = pe_event_log::Writer::open(&paper_log).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: crate::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(released_at),
                received_at: ReceivedAt(released_at),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(
                    &crate::paper_recovery::PaperLogRecord::RiskHaltChanged {
                        owner: owner.clone(),
                        cause: pe_risk_engine::RiskHaltCause::CopyLatency,
                        state: HaltState::Released,
                        evidence: serde_json::json!({}),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        drop(writer);
        let restarted_era = paper_era(scan_paper_log(&paper_log).unwrap());
        let released = crate::risk_inputs::latency_hysteresis_seed(
            &restarted_era,
            &owner,
            &dir.path().join("unused-live.log"),
        )
        .unwrap();
        let first_post_release_pass = OffsetDateTime::from_unix_timestamp(3 * 3_600 + 1).unwrap();
        assert!(!live_latency_switch(&events, first_post_release_pass, released).unwrap());

        events.push(latency_posted_event(2, 10_801_000, 3_001));
        let after_one_new_hour = OffsetDateTime::from_unix_timestamp(4 * 3_600 + 1).unwrap();
        assert!(!live_latency_switch(&events, after_one_new_hour, released).unwrap());

        events.push(latency_posted_event(3, 14_401_000, 3_001));
        let after_two_new_hours = OffsetDateTime::from_unix_timestamp(5 * 3_600 + 1).unwrap();
        assert!(live_latency_switch(&events, after_two_new_hours, released).unwrap());
    }

    /// PASS: responses appended after a manual release's journal tail are replayed by sequence,
    /// even when their embedded receive times precede the release; the delayed high hour and its
    /// adjacent post-tail high hour therefore re-engage the live latency halt.
    #[test]
    fn live_post_latency_release_counts_response_appended_after_tail() {
        let dir = tempfile::tempdir().unwrap();
        let paper_log = dir.path().join("paper.log");
        let live_journal_path = dir.path().join("live.log");
        let journal = pe_execution_core::LiveJournal::open(&live_journal_path).unwrap();
        let mut events = Vec::new();
        for event in [
            latency_posted_event(0, 3_601_000, 3_001),
            latency_posted_event(1, 7_201_000, 3_001),
        ] {
            events.push(
                journal
                    .append(event.account_id, event.timestamp, event.payload)
                    .unwrap(),
            );
        }
        let account_id = AccountId::new("latency-account").unwrap();
        let owner = RiskHaltOwner::LiveAccount(account_id.clone());
        let (_, tail) = pe_execution_core::live_journal::replay_account_with_tail(
            &live_journal_path,
            &account_id,
        )
        .unwrap();
        let released_at = OffsetDateTime::from_unix_timestamp(18_002).unwrap();
        let mut writer = pe_event_log::Writer::open(&paper_log).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: crate::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(released_at),
                received_at: ReceivedAt(released_at),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(
                    &crate::paper_recovery::PaperLogRecord::RiskHaltChanged {
                        owner: owner.clone(),
                        cause: pe_risk_engine::RiskHaltCause::CopyLatency,
                        state: HaltState::Released,
                        evidence: serde_json::json!({
                            "live_journal_tail": crate::risk_inputs::LiveLatencyJournalTailEvidence::from(tail),
                        }),
                    },
                )
                .unwrap(),
            })
            .unwrap();
        drop(writer);

        for event in [
            latency_posted_event_appended_at(2, 10_801_000, 3_001, 18_003_000),
            latency_posted_event_appended_at(3, 14_401_000, 3_001, 18_004_000),
        ] {
            events.push(
                journal
                    .append(event.account_id, event.timestamp, event.payload)
                    .unwrap(),
            );
        }
        let restarted_era = paper_era(scan_paper_log(&paper_log).unwrap());
        let released =
            crate::risk_inputs::latency_hysteresis_seed(&restarted_era, &owner, &live_journal_path)
                .unwrap();
        assert_eq!(
            released.checkpoint,
            Some(crate::risk_inputs::LatencyReplayCheckpoint::LiveJournalTail(tail))
        );
        let after_both_delayed_hours = OffsetDateTime::from_unix_timestamp(6 * 3_600 + 1).unwrap();
        assert!(live_latency_switch(&events, after_both_delayed_hours, released).unwrap());
    }

    /// PASS: live risk evidence is sequence-sorted, deduplicates one receipt shared by outcomes,
    /// and fails closed when one sequence claims two hashes.
    #[test]
    fn live_risk_price_receipts_are_canonical() {
        let receipt = |sequence, byte| AppendReceipt {
            sequence: pe_core_types::EventSeq(sequence),
            this_hash: blake3::Hash::from_bytes([byte; 32]),
        };
        let mut mids = BTreeMap::from([
            (
                ("market-b".to_owned(), 0),
                MidPriceObservation {
                    price: Price(Decimal::new(6, 1)),
                    receipt: receipt(9, 9),
                    observed_unix: 1,
                },
            ),
            (
                ("market-a".to_owned(), 0),
                MidPriceObservation {
                    price: Price(Decimal::new(4, 1)),
                    receipt: receipt(3, 3),
                    observed_unix: 1,
                },
            ),
            (
                ("market-a".to_owned(), 1),
                MidPriceObservation {
                    price: Price(Decimal::new(6, 1)),
                    receipt: receipt(3, 3),
                    observed_unix: 1,
                },
            ),
        ]);
        assert_eq!(
            sorted_price_receipts(&mids).unwrap(),
            vec![receipt(3, 3), receipt(9, 9)]
        );

        mids.insert(
            ("market-b".to_owned(), 1),
            MidPriceObservation {
                price: Price(Decimal::new(4, 1)),
                receipt: receipt(9, 8),
                observed_unix: 1,
            },
        );
        assert_eq!(
            sorted_price_receipts(&mids),
            Err(RiskInputsUnavailable::PriceConflict)
        );
    }

    /// PASS: receipt-scoped live source collection retains the three admission responses, exact
    /// `/book`, risk-price evidence, and both observation receipts referenced by an
    /// `AdmissionEvaluated` record.
    /// FAIL: any referenced economic receipt is absent from the scoped collection.
    #[test]
    fn live_source_scope_includes_all_economic_receipts() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("source.log");
        let mut writer = pe_event_log::Writer::open(&path).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(20).unwrap();
        let mut receipts = Vec::new();
        for ordinal in 0..7 {
            receipts.push(
                writer
                    .append_synced(EnvelopeIn {
                        source_id: SourceId(format!("scope-{ordinal}")),
                        schema_version: 1,
                        parser_version: 1,
                        observed_at: SourceTimestamp(now),
                        received_at: ReceivedAt(now),
                        content_type: ContentType::Json,
                        payload: vec![u8::try_from(ordinal).unwrap()],
                    })
                    .unwrap(),
            );
        }
        drop(writer);
        let index = SourceReceiptIndex::replay(&path).unwrap();
        let mut prepared = finality_prepared();
        prepared.economic.admission.receipts = AdmissionReceipts {
            gamma: receipts[0],
            clob_long: receipts[1],
            clob_compact: receipts[2],
        };
        prepared.economic.book_receipt = receipts[3];
        prepared.economic.risk.price_receipts = vec![receipts[4]];
        prepared.economic.observation = Some(ObservationEvidence {
            source_receipt: receipts[5],
            complete_bound_receipt: receipts[6],
            observed_unix_ms: 20_000,
            provenance: "activity_ws".to_owned(),
        });
        let account_id = AccountId::new("source-scope").unwrap();
        let events = [approved_admission_event(&account_id, &prepared, 0, now)];

        let scoped = source_envelopes_for_live_events(&index, &events).unwrap();
        assert_eq!(
            scoped.iter().map(|source| source.seq).collect::<Vec<_>>(),
            receipts
                .iter()
                .map(|receipt| receipt.sequence)
                .collect::<Vec<_>>()
        );
    }

    fn gamma_risk_price_source(
        receipt: AppendReceipt,
        condition_id: &str,
        prices: [&str; 2],
        now: OffsetDateTime,
    ) -> EventEnvelope {
        let request_url = format!(
            "https://gamma-api.polymarket.com/markets?condition_ids={condition_id}&limit={}",
            pe_source_polymarket_public::GAMMA_BATCH_LIMIT_PARAM
        );
        let encoded_prices = serde_json::to_string(&prices).unwrap();
        let raw = serde_json::to_vec(&serde_json::json!([{
            "conditionId": condition_id,
            "outcomePrices": encoded_prices,
        }]))
        .unwrap();
        let canonical = serde_json::from_slice::<serde_json::Value>(&raw)
            .and_then(|value| serde_json::to_vec(&value))
            .unwrap();
        let payload = serde_json::to_vec(&GammaPriceAttemptRecord::Page {
            evidence: pe_source_polymarket_public::MetadataPageEvidence {
                request_url,
                raw_page_hash: blake3::hash(&raw).to_hex().to_string(),
                canonical_page_hash: blake3::hash(&canonical).to_hex().to_string(),
                received_at: ReceivedAt(now),
                source_id: SourceId(GAMMA_MARKETS_SOURCE_ID.to_owned()),
                schema_version: GAMMA_MARKETS_SCHEMA_VERSION,
                parser_version: GAMMA_MARKETS_PARSER_VERSION,
            },
            payload: raw,
            usable: true,
        })
        .unwrap();
        let mut envelope = source_envelope(receipt, GAMMA_MARKETS_SOURCE_ID, payload, now);
        envelope.schema_version = GAMMA_PRICE_ATTEMPT_SCHEMA_VERSION;
        envelope.parser_version = GAMMA_PRICE_ATTEMPT_PARSER_VERSION;
        envelope
    }

    async fn append_recovery_risk_price(
        state: &FanoutState,
        now: OffsetDateTime,
    ) -> (AppendReceipt, String) {
        let prepared = finality_prepared();
        let condition_id = prepared.economic.market.market_id.clone();
        let source =
            gamma_risk_price_source(fixture_receipt(0), &condition_id, ["0.9", "0.1"], now);
        let input = EnvelopeIn {
            source_id: source.source_id,
            schema_version: source.schema_version,
            parser_version: source.parser_version,
            observed_at: source.observed_at,
            received_at: source.received_at,
            content_type: source.content_type,
            payload: source.payload,
        };
        let receipt = state.config.source_log.append(input).await.unwrap();
        state
            .config
            .source_receipts
            .source_envelope(receipt)
            .unwrap();
        (receipt, condition_id)
    }

    fn reconstructed_live_risk_fixture() -> (
        tempfile::TempDir,
        AccountId,
        Vec<LiveJournalEvent>,
        Vec<EventEnvelope>,
        Vec<ScannedPaperFrame>,
        Box<pe_execution_core::LiveAdmissionEvaluationAudit>,
    ) {
        let dir = tempdir().unwrap();
        let journal_path = dir.path().join("live.journal");
        drop(LiveJournal::open(&journal_path).unwrap());
        let paper_path = dir.path().join("paper.log");
        let now = OffsetDateTime::from_unix_timestamp(20).unwrap();
        let mut paper = pe_event_log::Writer::open(&paper_path).unwrap();
        paper
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: crate::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&PaperLogRecord::RiskHaltChanged {
                    owner: RiskHaltOwner::Paper,
                    cause: pe_risk_engine::RiskHaltCause::CopyLatency,
                    state: HaltState::Released,
                    evidence: serde_json::json!({"fixture": "risk-reconstruction"}),
                })
                .unwrap(),
            })
            .unwrap();
        drop(paper);
        let paper_frames = scan_paper_log(&paper_path).unwrap();

        let account_id = AccountId::new("account").unwrap();
        let mut prior = finality_prepared();
        prior.economic.admission.market.observed_at_unix = now.unix_timestamp();
        prior.economic.admission.settlement.observed_at_unix = now.unix_timestamp();
        let _ = bind_activity_observation(
            &mut prior,
            &account_id,
            fixture_receipt(5),
            fixture_receipt(6),
            now,
            now,
        );
        let events = vec![
            baseline_event(&account_id, 0),
            approved_admission_event(&account_id, &prior, 1, now),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 2,
                timestamp: OffsetDateTime::UNIX_EPOCH,
                payload: LiveJournalPayload::OrderPrepared(prior.clone()),
            },
            finality_matched_event(&account_id, &prior, 3, OffsetDateTime::UNIX_EPOCH),
            finalized_fill_event(&account_id, &prior, 4, 2),
        ];
        let price_receipt = fixture_receipt(200);
        let mut sources = baseline_sources(&account_id);
        sources.extend(economic_source_envelopes(&prior, now));
        let derived = derive_projection_rows_with_sources(&account_id, &events, &sources).unwrap();
        let condition_id = derived.positions[0].market_id.clone();
        sources.push(gamma_risk_price_source(
            price_receipt,
            &condition_id,
            ["0.9", "0.1"],
            now,
        ));
        let mids = replay_live_risk_prices(
            &derived.positions,
            &[price_receipt],
            now.unix_timestamp().saturating_mul(1_000),
            &sources,
            None,
        )
        .unwrap();

        let mut candidate = finality_prepared();
        candidate.economic.admission.market.observed_at_unix = now.unix_timestamp();
        candidate.economic.admission.settlement.observed_at_unix = now.unix_timestamp();
        candidate.identity = projection_identity("risk-candidate");
        let _ = bind_activity_observation(
            &mut candidate,
            &account_id,
            fixture_receipt(5),
            fixture_receipt(6),
            now,
            now,
        );
        candidate.economic.balance.cash_before =
            CollateralAmount::from_decimal_exact(derived.economic_cash.unwrap()).unwrap();
        let projection = candidate.identity.fill_projection.as_ref().unwrap();
        candidate.economic.risk = compose_live_risk_audit(
            &account_id,
            &journal_path,
            &events,
            &derived,
            &paper_era_at_evaluation(&paper_frames, now.unix_timestamp().saturating_mul(1_000))
                .unwrap(),
            &mids,
            &[price_receipt],
            now,
            LiveRiskProposal {
                leader_wallet: Some(projection.leader_wallet.clone()),
                market_id: Some(projection.market_id.clone()),
                proposed_debit: candidate.economic.balance.worst_case_debit,
                per_trade_cap_bps: 10_000,
            },
        )
        .unwrap();
        let admission = approved_admission(&candidate);
        (dir, account_id, events, sources, paper_frames, admission)
    }

    /// PASS: strict live replay independently rebuilds cash, position value, every exposure/PnL
    /// dimension, latency, proposal/cap state, paper prefix, receipt inventory, and decision; a
    /// producer-mutated self-consistent snapshot is rejected for every field.
    #[test]
    fn strict_live_risk_reconstructs_every_snapshot_dimension() {
        let (dir, account_id, events, sources, paper_frames, admission) =
            reconstructed_live_risk_fixture();
        let journal_path = dir.path().join("live.journal");
        verify_replayed_live_risk(
            &account_id,
            &journal_path,
            &events,
            &admission,
            &sources,
            &paper_frames,
        )
        .unwrap();
        assert_eq!(
            admission.economic.risk.snapshot.leader_exposure_bps,
            BasisPoints(2_501)
        );
        assert_eq!(
            admission.economic.risk.snapshot.market_exposure_bps,
            BasisPoints(2_501)
        );
        assert_eq!(
            admission.economic.risk.snapshot.family_exposure_bps,
            BasisPoints(2_501)
        );
        assert_eq!(
            admission.economic.risk.snapshot.total_copy_exposure_bps,
            BasisPoints(2_501)
        );
        assert_eq!(
            admission.economic.risk.snapshot.intraday_pnl_bps,
            BasisPoints(312)
        );
        assert_eq!(
            admission.economic.risk.snapshot.rolling_7d_pnl_bps,
            BasisPoints(0)
        );
        assert_eq!(
            admission.economic.risk.snapshot.absolute_pnl_bps,
            BasisPoints(312)
        );
        assert!(
            !admission
                .economic
                .risk
                .snapshot
                .copy_latency_kill_switch_active
        );
        assert_eq!(
            admission.economic.risk.snapshot.proposed_trade_bps,
            BasisPoints(2_501)
        );
        assert_eq!(admission.economic.risk.snapshot.per_trade_cap_bps, 10_000);
        assert_eq!(admission.economic.risk.snapshot.concentration_caps, None);
        assert_eq!(
            admission.economic.risk.decision,
            RiskDecisionAudit::Approved
        );

        for (index, dimension) in [
            "leader",
            "market",
            "family",
            "total",
            "intraday",
            "rolling",
            "absolute",
            "latency",
            "proposal",
            "cap",
            "concentration",
        ]
        .into_iter()
        .enumerate()
        {
            let mut changed = admission.clone();
            let snapshot = &mut changed.economic.risk.snapshot;
            match index {
                0 => snapshot.leader_exposure_bps.0 += 1,
                1 => snapshot.market_exposure_bps.0 += 1,
                2 => snapshot.family_exposure_bps.0 += 1,
                3 => snapshot.total_copy_exposure_bps.0 += 1,
                4 => snapshot.intraday_pnl_bps.0 += 1,
                5 => snapshot.rolling_7d_pnl_bps.0 += 1,
                6 => snapshot.absolute_pnl_bps.0 += 1,
                7 => snapshot.copy_latency_kill_switch_active = true,
                8 => snapshot.proposed_trade_bps.0 += 1,
                9 => snapshot.per_trade_cap_bps = 0,
                _ => {
                    snapshot.concentration_caps = Some(pe_risk_engine::ConcentrationCaps::CANONICAL)
                }
            }
            changed.economic.risk.decision = match pe_risk_engine::evaluate_risk(snapshot) {
                pe_risk_engine::RiskDecision::Approved => RiskDecisionAudit::Approved,
                pe_risk_engine::RiskDecision::Blocked(reason) => {
                    RiskDecisionAudit::Blocked { reason }
                }
            };
            assert!(
                matches!(
                    verify_replayed_live_risk(
                        &account_id,
                        &journal_path,
                        &events,
                        &changed,
                        &sources,
                        &paper_frames,
                    ),
                    Err(ProjectionReducerError::InvalidRiskEvidence)
                ),
                "{dimension}"
            );
        }

        let mut wrong_prefix = admission.clone();
        wrong_prefix.economic.risk.financial_prefix = fixture_receipt(199);
        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &wrong_prefix,
                &sources,
                &paper_frames,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));
    }

    /// PASS: strict live replay requires each exact Gamma, CLOB-long, CLOB-compact, and `/book`
    /// receipt; substituting an admission or book receipt with conflicting payload evidence fails
    /// before the admission can enter projection or recovery.
    /// FAIL: a missing or substituted admission/book receipt is accepted, or the admission
    /// reaches projection or recovery without all four exact receipts.
    #[test]
    fn strict_live_economic_requires_exact_admission_and_book_receipts() {
        let (dir, account_id, events, sources, paper_frames, admission) =
            reconstructed_live_risk_fixture();
        let journal_path = dir.path().join("live.journal");
        let economic_receipts = [
            admission.economic.admission.receipts.gamma,
            admission.economic.admission.receipts.clob_long,
            admission.economic.admission.receipts.clob_compact,
            admission.economic.book_receipt,
        ];

        for missing_receipt in economic_receipts {
            let missing = sources
                .iter()
                .filter(|source| {
                    source.seq != missing_receipt.sequence
                        || source.this_hash != missing_receipt.this_hash
                })
                .cloned()
                .collect::<Vec<_>>();
            assert!(matches!(
                verify_replayed_live_risk(
                    &account_id,
                    &journal_path,
                    &events,
                    &admission,
                    &missing,
                    &paper_frames,
                ),
                Err(ProjectionReducerError::InvalidRiskEvidence)
            ));
        }

        let substituted_gamma_receipt = fixture_receipt(210);
        let other_condition = format!("0x{}", "66".repeat(32));
        let mut substituted_gamma_sources = sources.clone();
        substituted_gamma_sources.push(source_envelope(
            substituted_gamma_receipt,
            GAMMA_MARKETS_SOURCE_ID,
            economic_source_payloads(&other_condition)[0].clone(),
            OffsetDateTime::from_unix_timestamp(20).unwrap(),
        ));
        let mut substituted_gamma = admission.clone();
        substituted_gamma.economic.admission.receipts.gamma = substituted_gamma_receipt;
        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &substituted_gamma,
                &substituted_gamma_sources,
                &paper_frames,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));

        let substituted_book_receipt = fixture_receipt(211);
        let mut substituted_book_sources = sources.clone();
        substituted_book_sources.push(source_envelope(
            substituted_book_receipt,
            CLOB_BOOK_SOURCE_ID,
            format!(
                r#"{{"market":"{}","asset_id":"123","asks":[{{"price":"0.81","size":"3.125"}}]}}"#,
                admission.economic.market.condition_id.0
            )
            .into_bytes(),
            OffsetDateTime::from_unix_timestamp(20).unwrap(),
        ));
        let mut substituted_book = admission.clone();
        substituted_book.economic.book_receipt = substituted_book_receipt;
        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &substituted_book,
                &substituted_book_sources,
                &paper_frames,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));
    }

    /// PASS: an otherwise byte-equivalent economic ladder carrying a different token identity is
    /// rejected before strict live replay can accept or project it.
    /// FAIL: changing only `asset_id` leaves the recorded admission replayable.
    #[test]
    fn strict_live_economic_rejects_book_asset_identity_substitution() {
        let (dir, account_id, events, mut sources, paper_frames, admission) =
            reconstructed_live_risk_fixture();
        let journal_path = dir.path().join("live.journal");
        let book = sources
            .iter_mut()
            .find(|source| {
                source.seq == admission.economic.book_receipt.sequence
                    && source.this_hash == admission.economic.book_receipt.this_hash
            })
            .unwrap();
        book.payload = format!(
            r#"{{"market":"{}","asset_id":"substituted-token","asks":[{{"price":"0.80","size":"3.125"}}]}}"#,
            admission.economic.market.condition_id.0
        )
        .into_bytes();

        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &admission,
                &sources,
                &paper_frames,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));
    }

    fn replay_economic_with_clocks(
        gamma_received_unix_ms: i64,
        clob_long_received_unix_ms: i64,
        recorded_market_unix: i64,
        recorded_settlement_unix: i64,
    ) -> Result<SourceBackedEconomicReplay, EconomicReplayError> {
        let mut economic = finality_prepared().economic.clone();
        economic.admission.market.observed_at_unix = recorded_market_unix;
        economic.admission.settlement.observed_at_unix = recorded_settlement_unix;
        economic.risk.snapshot.per_trade_cap_bps = 10_000;
        let payloads = economic_source_payloads(&economic.market.condition_id.0);
        let receipts = [
            economic.admission.receipts.gamma,
            economic.admission.receipts.clob_long,
            economic.admission.receipts.clob_compact,
            economic.book_receipt,
        ];
        let source_ids = [
            GAMMA_MARKETS_SOURCE_ID,
            CLOB_LONG_MARKET_SOURCE_ID,
            CLOB_COMPACT_MARKET_SOURCE_ID,
            CLOB_BOOK_SOURCE_ID,
        ];
        let received_unix_ms = [
            gamma_received_unix_ms,
            clob_long_received_unix_ms,
            clob_long_received_unix_ms,
            clob_long_received_unix_ms,
        ];
        let observations = receipts
            .into_iter()
            .zip(source_ids)
            .zip(payloads)
            .zip(received_unix_ms)
            .map(|(((receipt, source_id), payload), received_unix_ms)| {
                (
                    receipt,
                    RecordedEconomicSource {
                        payload,
                        received_unix_ms,
                        source_id: source_id.to_owned(),
                        schema_version: 1,
                        parser_version: 1,
                        content_type: ContentType::Json,
                    },
                )
            })
            .collect::<Vec<_>>();
        replay_source_backed_economic(
            &economic,
            clob_long_received_unix_ms.saturating_add(10_000),
            CollateralAmount::from_atomic(10_000_000),
            |receipt| {
                observations
                    .iter()
                    .find(|(candidate, _)| *candidate == receipt)
                    .map(|(_, observation)| observation.clone())
                    .ok_or_else(|| EconomicReplayError("fixture receipt missing".to_owned()))
            },
        )
    }

    /// PASS: the source-backed owner used by live and qualification replay accepts receipt clocks
    /// within one UTC second and derives that second for both market and settlement evidence.
    #[test]
    fn strict_live_economic_clock_accepts_same_second_receipts() {
        replay_economic_with_clocks(20_100, 20_999, 20, 20).unwrap();
    }

    /// PASS: the source-backed owner used by live and qualification replay derives the market
    /// second from the later CLOB-long receipt when Gamma and CLOB straddle a second boundary.
    #[test]
    fn strict_live_economic_clock_accepts_second_straddling_receipts() {
        replay_economic_with_clocks(20_999, 21_001, 21, 21).unwrap();
    }

    /// PASS: strict replay rejects either recorded clock when it copies another receipt's second
    /// instead of equaling the independently derived market and settlement clocks.
    #[test]
    fn strict_live_economic_clock_rejects_copied_clock_tamper() {
        assert!(replay_economic_with_clocks(20_999, 21_001, 20, 21).is_err());
        assert!(replay_economic_with_clocks(20_999, 21_001, 21, 20).is_err());
    }

    /// PASS: the live composer preserves both `None` and receipt-bearing observations, and the
    /// observation changes the canonical economic core hash.
    /// FAIL: live composition drops the observation or produces the same hash for both inputs.
    #[test]
    fn live_composer_preserves_optional_observation() {
        let replayed = replay_economic_with_clocks(20_100, 20_999, 20, 20).unwrap();
        let template = finality_prepared().economic;
        let observation = ObservationEvidence {
            source_receipt: fixture_receipt(5),
            complete_bound_receipt: fixture_receipt(6),
            observed_unix_ms: 20_000,
            provenance: "activity_ws".to_owned(),
        };
        let compose = |observation| {
            EconomicPrepared::compose(EconomicInputs {
                market: template.market.clone(),
                admission: &replayed.admission,
                plan: &replayed.sized.ladder,
                book_receipt: template.book_receipt,
                observation,
                sizing_mode: template.sizing.mode,
                budget: replayed.sized.budget,
                slippage_rate: template.sizing.slippage_rate,
                risk: template.risk.clone(),
                cash_before: replayed.cash_before,
                price_impact_cap_bps: template.balance.price_impact_cap_bps,
                chase_ceiling: template.balance.chase_ceiling,
                band_floor: template.balance.band_floor,
                band_ceiling_exclusive: template.balance.band_ceiling_exclusive,
                applied_configuration_hash: template.applied_configuration_hash.clone(),
            })
            .unwrap()
        };

        let without = compose(None);
        let with = compose(Some(observation.clone()));
        assert_eq!(without.observation, None);
        assert_eq!(with.observation, Some(observation));
        assert_ne!(without.core_hash().unwrap(), with.core_hash().unwrap());
    }

    fn observation_replay_fixture() -> (
        Box<pe_execution_core::LiveOrderPreparedAudit>,
        AccountId,
        Vec<(AppendReceipt, RecordedEconomicSource)>,
        DecisionContinuationV3,
    ) {
        observation_replay_fixture_for(true)
    }

    fn observation_replay_fixture_for(
        websocket_assisted: bool,
    ) -> (
        Box<pe_execution_core::LiveOrderPreparedAudit>,
        AccountId,
        Vec<(AppendReceipt, RecordedEconomicSource)>,
        DecisionContinuationV3,
    ) {
        let mut prepared = finality_prepared();
        prepared.economic.admission.market.observed_at_unix = 20;
        prepared.economic.admission.settlement.observed_at_unix = 20;
        prepared.economic.risk.snapshot.per_trade_cap_bps = 10_000;
        prepared.economic.risk.evaluated_at_unix_ms = 30_000;
        let source_receipt = fixture_receipt(5);
        let complete_bound_receipt = fixture_receipt(6);
        let account_id = AccountId::new("account").unwrap();
        let source_time = OffsetDateTime::from_unix_timestamp(18).unwrap();
        let observed_at = OffsetDateTime::from_unix_timestamp(19).unwrap();
        let (websocket, rest) = bind_activity_observation(
            &mut prepared,
            &account_id,
            source_receipt,
            complete_bound_receipt,
            observed_at,
            source_time,
        );
        if !websocket_assisted {
            prepared.economic.observation = Some(ObservationEvidence {
                source_receipt: complete_bound_receipt,
                complete_bound_receipt,
                observed_unix_ms: 20_000,
                provenance: "rest_poll".to_owned(),
            });
        }
        let economic = &prepared.economic;
        let payloads = economic_source_payloads(&economic.market.condition_id.0);
        let receipts = [
            economic.admission.receipts.gamma,
            economic.admission.receipts.clob_long,
            economic.admission.receipts.clob_compact,
            economic.book_receipt,
        ];
        let source_ids = [
            GAMMA_MARKETS_SOURCE_ID,
            CLOB_LONG_MARKET_SOURCE_ID,
            CLOB_COMPACT_MARKET_SOURCE_ID,
            CLOB_BOOK_SOURCE_ID,
        ];
        let mut sources = receipts
            .into_iter()
            .zip(source_ids)
            .zip(payloads)
            .map(|((receipt, source_id), payload)| {
                (
                    receipt,
                    RecordedEconomicSource {
                        payload,
                        received_unix_ms: 20_000,
                        source_id: source_id.to_owned(),
                        schema_version: 1,
                        parser_version: 1,
                        content_type: ContentType::Json,
                    },
                )
            })
            .collect::<Vec<_>>();
        sources.extend([
            (
                source_receipt,
                RecordedEconomicSource {
                    payload: websocket,
                    received_unix_ms: 19_000,
                    source_id: crate::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned(),
                    schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    content_type: ContentType::Json,
                },
            ),
            (
                complete_bound_receipt,
                RecordedEconomicSource {
                    payload: rest.clone(),
                    received_unix_ms: 20_000,
                    source_id: crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned(),
                    schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    content_type: ContentType::Json,
                },
            ),
        ]);
        let continuation = observation_continuation(
            &prepared,
            &[rest.as_slice()],
            vec![activity_page_fixture(
                complete_bound_receipt,
                &rest,
                Some(17),
                18,
                0,
                observed_at,
            )],
        );
        record_read_commitment(&mut sources, &continuation);
        (prepared, account_id, sources, continuation)
    }

    fn observation_continuation(
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        aggregate_payloads: &[&[u8]],
        pages: Vec<(
            PageOccurrence,
            pe_source_polymarket_public::ReconciliationPageEvidence,
        )>,
    ) -> DecisionContinuationV3 {
        observation_continuation_for(
            &prepared.identity,
            &prepared.economic,
            aggregate_payloads,
            pages,
        )
    }

    fn observation_continuation_for(
        identity: &LiveOrderIdentity,
        economic: &EconomicPrepared,
        aggregate_payloads: &[&[u8]],
        pages: Vec<(
            PageOccurrence,
            pe_source_polymarket_public::ReconciliationPageEvidence,
        )>,
    ) -> DecisionContinuationV3 {
        let projection = identity.fill_projection.as_deref().unwrap();
        let observation = economic.observation.as_ref().unwrap();
        let wallet = WalletAddress::from_hex(&projection.leader_wallet).unwrap();
        let at = OffsetDateTime::from_unix_timestamp(20).unwrap();
        let context = pe_source_polymarket_public::ActivityParseContext {
            source_id: SourceId(crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            transport: pe_source_polymarket_public::ActivityTransport::Replay,
        };
        let mut rows = Vec::new();
        for payload in aggregate_payloads {
            rows.extend(
                pe_source_polymarket_public::parse_activity_response(payload, wallet, &context)
                    .unwrap()
                    .rows,
            );
        }
        let source_trade_id = SourceTradeId(projection.source_trade_id.clone().unwrap());
        let aggregate = pe_source_polymarket_public::aggregate_activity_rows(&rows)
            .unwrap()
            .into_iter()
            .find(|aggregate| aggregate.group_id.key() == &source_trade_id)
            .unwrap();
        produced_decision_continuation(identity, observation, aggregate, pages).unwrap()
    }

    fn read_commitment_source(
        continuation: &DecisionContinuationV3,
    ) -> (AppendReceipt, RecordedEconomicSource) {
        let pages = serde_json::from_value::<
            Vec<pe_source_polymarket_public::ReconciliationPageEvidence>,
        >(continuation.facts.decision_inputs["pages"].clone())
        .unwrap();
        let payload = crate::bucket_commit::activity_read_commitment_payload(
            continuation.facts.wallet,
            continuation.facts.decision_inputs["fixed_end"]
                .as_i64()
                .unwrap(),
            continuation.page_occurrences(),
            &pages,
        )
        .unwrap();
        (
            continuation.read_commitment.unwrap(),
            RecordedEconomicSource {
                payload,
                received_unix_ms: pages
                    .iter()
                    .map(|page| {
                        i64::try_from(page.received_at.0.unix_timestamp_nanos() / 1_000_000)
                            .unwrap()
                    })
                    .max()
                    .unwrap(),
                source_id: crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned(),
                schema_version: crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
                parser_version: crate::bucket_commit::ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
                content_type: ContentType::Json,
            },
        )
    }

    fn record_read_commitment(
        sources: &mut Vec<(AppendReceipt, RecordedEconomicSource)>,
        continuation: &DecisionContinuationV3,
    ) {
        sources.retain(|(_, source)| {
            source.source_id != crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        });
        for (receipt, source) in sources.iter_mut() {
            if continuation
                .page_occurrences()
                .iter()
                .any(|page| page.receipt == *receipt)
            {
                source.schema_version = crate::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION;
            }
        }
        sources.push(read_commitment_source(continuation));
    }

    fn record_read_commitment_envelope(
        sources: &mut Vec<EventEnvelope>,
        continuation: &DecisionContinuationV3,
    ) {
        sources.retain(|source| {
            source.source_id.0 != crate::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        });
        for source in sources.iter_mut() {
            if continuation.page_occurrences().iter().any(|page| {
                page.receipt.sequence == source.seq && page.receipt.this_hash == source.this_hash
            }) {
                source.schema_version = crate::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION;
            }
        }
        let (receipt, source) = read_commitment_source(continuation);
        let at = source_time_from_millis(source.received_unix_ms).unwrap();
        let mut envelope = source_envelope(receipt, &source.source_id, source.payload, at);
        envelope.schema_version = source.schema_version;
        envelope.parser_version = source.parser_version;
        sources.push(envelope);
    }

    fn replay_observation_fixture(
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        account_id: &AccountId,
        sources: &[(AppendReceipt, RecordedEconomicSource)],
        continuation: &DecisionContinuationV3,
    ) -> Result<SourceBackedEconomicReplay, EconomicReplayError> {
        let economic = &prepared.economic;
        replay_source_backed_economic_with_live_binding(
            economic,
            economic.risk.evaluated_at_unix_ms,
            CollateralAmount::from_atomic(10_000_000),
            Some(LiveObservationBinding {
                account_id,
                identity: &prepared.identity,
                continuation,
                correction: None,
            }),
            |receipt| {
                sources
                    .iter()
                    .find(|(candidate, _)| *candidate == receipt)
                    .map(|(_, source)| source.clone())
                    .ok_or_else(|| EconomicReplayError("fixture receipt missing".to_owned()))
            },
        )
    }

    fn activity_page_fragment(
        template: &serde_json::Value,
        size: &str,
        price: &str,
        usdc_size: &str,
    ) -> Vec<u8> {
        let mut row = template.clone();
        let object = row.as_object_mut().unwrap();
        object.insert("size".to_owned(), serde_json::json!(size));
        object.insert("price".to_owned(), serde_json::json!(price));
        object.insert("usdcSize".to_owned(), serde_json::json!(usdc_size));
        serde_json::to_vec(&vec![row]).unwrap()
    }

    fn full_activity_page_with_fragment(
        template: &serde_json::Value,
        size: &str,
        price: &str,
        usdc_size: &str,
    ) -> Vec<u8> {
        let mut rows = serde_json::from_slice::<Vec<serde_json::Value>>(&activity_page_fragment(
            template, size, price, usdc_size,
        ))
        .unwrap();
        for index in 0..(pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT - 1) {
            let mut filler = template.clone();
            let object = filler.as_object_mut().unwrap();
            object.insert(
                "transactionHash".to_owned(),
                serde_json::json!(format!("0x{index:064x}")),
            );
            object.insert(
                "asset".to_owned(),
                serde_json::json!(format!("filler-{index}")),
            );
            object.insert("size".to_owned(), serde_json::json!("1.000000"));
            object.insert("price".to_owned(), serde_json::json!("0.50"));
            object.insert("usdcSize".to_owned(), serde_json::json!("0.500000"));
            rows.push(filler);
        }
        serde_json::to_vec(&rows).unwrap()
    }

    fn activity_page_fixture(
        receipt: AppendReceipt,
        payload: &[u8],
        start: Option<i64>,
        end: i64,
        offset: u32,
        received_at: OffsetDateTime,
    ) -> (
        PageOccurrence,
        pe_source_polymarket_public::ReconciliationPageEvidence,
    ) {
        let request_url =
            pe_source_polymarket_public::PolymarketEndpoint::UserPositionActivityPage {
                user: WalletAddress([0xaa; 20]).to_string(),
                end,
                start: start.map(|start| start.saturating_add(1)),
                offset,
            }
            .url("https://data-api.polymarket.com");
        let raw_hash = blake3::hash(payload).to_hex().to_string();
        let row_count = serde_json::from_slice::<Vec<serde_json::Value>>(payload)
            .unwrap()
            .len();
        (
            PageOccurrence {
                request_url: request_url.clone(),
                raw_hash: raw_hash.clone(),
                receipt,
            },
            pe_source_polymarket_public::ReconciliationPageEvidence {
                request_url,
                bounds: Some(pe_source_polymarket_public::ActivityRequestBounds { start, end }),
                partition: None,
                offset,
                row_count: u32::try_from(row_count).unwrap(),
                canonical_page_hash: pe_source_polymarket_public::canonical_page_hash(payload)
                    .unwrap(),
                raw_page_hash: raw_hash,
                received_at: ReceivedAt(received_at),
                schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            },
        )
    }

    struct SaturatedActivityGraphFixture {
        pages: Vec<(
            PageOccurrence,
            pe_source_polymarket_public::ReconciliationPageEvidence,
        )>,
        payloads: Vec<(AppendReceipt, Vec<u8>)>,
        websocket_payload: Vec<u8>,
        selected_payload: Vec<u8>,
        complete_bound_receipt: AppendReceipt,
        selected_source_time: OffsetDateTime,
    }

    fn saturated_activity_graph_fixture(
        template: &serde_json::Value,
        first_sequence: u64,
        received_at: OffsetDateTime,
        selected_on_old_leaf: bool,
    ) -> SaturatedActivityGraphFixture {
        let row = |timestamp: i64,
                   transaction_hash: String,
                   asset: String,
                   size: &str,
                   price: &str,
                   usdc_size: &str| {
            let mut row = template.clone();
            let object = row.as_object_mut().unwrap();
            object.insert("timestamp".to_owned(), serde_json::json!(timestamp));
            object.insert(
                "transactionHash".to_owned(),
                serde_json::json!(transaction_hash),
            );
            object.insert("asset".to_owned(), serde_json::json!(asset));
            object.insert("size".to_owned(), serde_json::json!(size));
            object.insert("price".to_owned(), serde_json::json!(price));
            object.insert("usdcSize".to_owned(), serde_json::json!(usdc_size));
            row
        };
        let selected_transaction = template["transactionHash"].as_str().unwrap().to_owned();
        let selected_asset = template["asset"].as_str().unwrap().to_owned();
        let selected_source_time = if selected_on_old_leaf { 10 } else { 51 };
        let selected_rows = if selected_on_old_leaf {
            vec![row(
                10,
                selected_transaction.clone(),
                selected_asset.clone(),
                "10.000000",
                "0.52",
                "5.200000",
            )]
        } else {
            vec![
                row(
                    51,
                    selected_transaction.clone(),
                    selected_asset.clone(),
                    "4.000000",
                    "0.40",
                    "1.600000",
                ),
                row(
                    51,
                    selected_transaction.clone(),
                    selected_asset.clone(),
                    "6.000000",
                    "0.60",
                    "3.600000",
                ),
            ]
        };
        let selected_payload = serde_json::to_vec(&selected_rows).unwrap();
        let upper_rows = if selected_on_old_leaf {
            (0..2_u32)
                .map(|index| {
                    row(
                        51,
                        format!("0xupper{index:058}"),
                        format!("upper-{index}"),
                        "1.000000",
                        "0.50",
                        "0.500000",
                    )
                })
                .collect::<Vec<_>>()
        } else {
            selected_rows.clone()
        };
        let boundary_rows = (0..5_499_u32)
            .map(|index| {
                row(
                    50,
                    format!("0x{index:064x}"),
                    format!("boundary-{index}"),
                    "1.000000",
                    "0.50",
                    "0.500000",
                )
            })
            .collect::<Vec<_>>();
        let old_rows = if selected_on_old_leaf {
            selected_rows
        } else {
            vec![row(
                10,
                format!("0x{}", "77".repeat(32)),
                "old".to_owned(),
                "1.000000",
                "0.50",
                "0.500000",
            )]
        };
        let parent_rows = upper_rows
            .iter()
            .chain(boundary_rows.iter().take(5_498))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(parent_rows.len(), 5_500);

        let mut pages = Vec::new();
        let mut payloads = Vec::new();
        let mut sequence = first_sequence;
        let mut append_segment = |start: Option<i64>, end: i64, rows: &[serde_json::Value]| {
            for (page_index, chunk) in rows
                .chunks(
                    usize::try_from(pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT)
                        .unwrap(),
                )
                .enumerate()
            {
                let offset = u32::try_from(page_index).unwrap()
                    * pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT;
                let payload = serde_json::to_vec(chunk).unwrap();
                let receipt = fixture_receipt(sequence);
                sequence += 1;
                pages.push(activity_page_fixture(
                    receipt,
                    &payload,
                    start,
                    end,
                    offset,
                    received_at,
                ));
                payloads.push((receipt, payload));
            }
        };
        append_segment(None, 100, &parent_rows);
        append_segment(None, 49, &old_rows);
        append_segment(Some(49), 50, &boundary_rows);
        append_segment(Some(50), 100, &upper_rows);
        let complete_bound_receipt = pages.last().unwrap().0.receipt;

        let mut websocket = if selected_on_old_leaf {
            old_rows[0].clone()
        } else {
            upper_rows[0].clone()
        };
        let websocket_object = websocket.as_object_mut().unwrap();
        websocket_object.remove("type");
        websocket_object.remove("usdcSize");
        websocket_object.remove("isCombo");
        SaturatedActivityGraphFixture {
            pages,
            payloads,
            websocket_payload: serde_json::to_vec(&websocket).unwrap(),
            selected_payload,
            complete_bound_receipt,
            selected_source_time: OffsetDateTime::from_unix_timestamp(selected_source_time)
                .unwrap(),
        }
    }

    fn saturated_observation_replay_fixture(
        websocket_assisted: bool,
        selected_on_old_leaf: bool,
    ) -> (
        Box<pe_execution_core::LiveOrderPreparedAudit>,
        AccountId,
        Vec<(AppendReceipt, RecordedEconomicSource)>,
        DecisionContinuationV3,
    ) {
        let (mut prepared, account_id, mut sources, _) = observation_replay_fixture();
        let old_observation = prepared.economic.observation.as_ref().unwrap().clone();
        let original_rest = sources
            .iter()
            .find(|(receipt, _)| *receipt == old_observation.complete_bound_receipt)
            .map(|(_, source)| source.payload.clone())
            .unwrap();
        let template = serde_json::from_slice::<Vec<serde_json::Value>>(&original_rest)
            .unwrap()
            .remove(0);
        sources.retain(|(receipt, _)| {
            *receipt != old_observation.source_receipt
                && *receipt != old_observation.complete_bound_receipt
        });
        let graph = saturated_activity_graph_fixture(
            &template,
            220,
            OffsetDateTime::from_unix_timestamp(20).unwrap(),
            selected_on_old_leaf,
        );
        let websocket_receipt = fixture_receipt(219);
        if websocket_assisted {
            sources.push((
                websocket_receipt,
                RecordedEconomicSource {
                    payload: graph.websocket_payload.clone(),
                    received_unix_ms: 19_000,
                    source_id: crate::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned(),
                    schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    content_type: ContentType::Json,
                },
            ));
        }
        sources.extend(graph.payloads.iter().map(|(receipt, payload)| {
            (
                *receipt,
                RecordedEconomicSource {
                    payload: payload.clone(),
                    received_unix_ms: 20_000,
                    source_id: crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned(),
                    schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    content_type: ContentType::Json,
                },
            )
        }));
        prepared.economic.observation = Some(ObservationEvidence {
            source_receipt: if websocket_assisted {
                websocket_receipt
            } else {
                graph.complete_bound_receipt
            },
            complete_bound_receipt: graph.complete_bound_receipt,
            observed_unix_ms: if websocket_assisted { 19_000 } else { 20_000 },
            provenance: if websocket_assisted {
                "activity_ws".to_owned()
            } else {
                "rest_poll".to_owned()
            },
        });
        bind_reconciled_activity_decision(
            &mut prepared,
            &account_id,
            graph.selected_source_time,
            Price(dec!(0.52)),
            ShareAmount::from_whole(10).unwrap(),
        );
        let continuation =
            observation_continuation(&prepared, &[graph.selected_payload.as_slice()], graph.pages);
        record_read_commitment(&mut sources, &continuation);
        (prepared, account_id, sources, continuation)
    }

    fn bind_reconciled_activity_decision(
        prepared: &mut pe_execution_core::LiveOrderPreparedAudit,
        account_id: &AccountId,
        source_time: OffsetDateTime,
        price: Price,
        shares: ShareAmount,
    ) {
        prepared.identity.decision_hash = reconciled_activity_decision_hash(
            &prepared.identity,
            account_id,
            source_time,
            price,
            shares,
        );
    }

    fn reconciled_activity_decision_hash(
        identity: &LiveOrderIdentity,
        account_id: &AccountId,
        source_time: OffsetDateTime,
        price: Price,
        shares: ShareAmount,
    ) -> String {
        let projection = identity.fill_projection.as_deref().unwrap();
        let source_trade_id = SourceTradeId(projection.source_trade_id.clone().unwrap());
        let signal = LeaderSignal {
            leader: TraderId(WalletAddress::from_hex(&projection.leader_wallet).unwrap()),
            venue: VenueId::polymarket(),
            market_id: MarketId(VenueMarketId(projection.market_id.clone())),
            outcome_id: OutcomeId(u16::try_from(projection.outcome_id).unwrap()),
            action: LeaderAction::Entry,
            leader_side: Side::Buy,
            leader_price: price,
            leader_size: shares,
            observed_at: source_time,
            received_at: source_time,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id,
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        };
        hash_json(&(
            "prediction-edge/live-decision/v1",
            &signal,
            account_id.as_str(),
            &identity.config_hash,
            &identity.quote_id,
            &identity.evidence_hashes,
        ))
        .unwrap()
    }

    fn multipage_observation_replay_fixture(
        websocket_assisted: bool,
        split_group: bool,
    ) -> (
        Box<pe_execution_core::LiveOrderPreparedAudit>,
        AccountId,
        Vec<(AppendReceipt, RecordedEconomicSource)>,
        DecisionContinuationV3,
    ) {
        let (mut prepared, account_id, mut sources, _) = observation_replay_fixture();
        let original_bound = prepared
            .economic
            .observation
            .as_ref()
            .unwrap()
            .complete_bound_receipt;
        let original_page = sources
            .iter()
            .find(|(receipt, _)| *receipt == original_bound)
            .map(|(_, source)| source.payload.clone())
            .unwrap();
        let template = serde_json::from_slice::<Vec<serde_json::Value>>(&original_page)
            .unwrap()
            .remove(0);
        sources.retain(|(receipt, _)| *receipt != original_bound);

        let (first_payload, final_payload) = if split_group {
            (
                full_activity_page_with_fragment(&template, "4.000000", "0.40", "1.600000"),
                activity_page_fragment(&template, "6.000000", "0.60", "3.600000"),
            )
        } else {
            (
                full_activity_page_with_fragment(&template, "10.000000", "0.52", "5.200000"),
                serde_json::to_vec(&Vec::<serde_json::Value>::new()).unwrap(),
            )
        };
        let first_receipt = original_bound;
        let final_receipt = fixture_receipt(original_bound.sequence.0 + 1);
        let page_source = |payload, received_unix_ms| RecordedEconomicSource {
            payload,
            received_unix_ms,
            source_id: crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned(),
            schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            content_type: ContentType::Json,
        };
        sources.extend([
            (first_receipt, page_source(first_payload.clone(), 20_000)),
            (final_receipt, page_source(final_payload.clone(), 21_000)),
        ]);

        let source_time = OffsetDateTime::from_unix_timestamp(18).unwrap();
        bind_reconciled_activity_decision(
            &mut prepared,
            &account_id,
            source_time,
            Price(dec!(0.52)),
            ShareAmount::from_whole(10).unwrap(),
        );
        let selected_receipt = if websocket_assisted {
            prepared
                .economic
                .observation
                .as_ref()
                .unwrap()
                .source_receipt
        } else {
            final_receipt
        };
        prepared.economic.observation = Some(ObservationEvidence {
            source_receipt: selected_receipt,
            complete_bound_receipt: final_receipt,
            observed_unix_ms: if websocket_assisted { 19_000 } else { 21_000 },
            provenance: if websocket_assisted {
                "activity_ws".to_owned()
            } else {
                "rest_poll".to_owned()
            },
        });
        let continuation = observation_continuation(
            &prepared,
            &[first_payload.as_slice(), final_payload.as_slice()],
            vec![
                activity_page_fixture(
                    first_receipt,
                    &first_payload,
                    Some(17),
                    18,
                    0,
                    OffsetDateTime::from_unix_timestamp(20).unwrap(),
                ),
                activity_page_fixture(
                    final_receipt,
                    &final_payload,
                    Some(17),
                    18,
                    pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT,
                    OffsetDateTime::from_unix_timestamp(21).unwrap(),
                ),
            ],
        );
        record_read_commitment(&mut sources, &continuation);
        (prepared, account_id, sources, continuation)
    }

    /// PASS: authentic terminal-child collapse and coherent origin substitution fail against the unchanged V4 commitment with a live binding; the original read passes.
    /// FAIL: strict live replay accepts a mutable continuation that redefines its committed read.
    #[test]
    fn strict_live_economic_rejects_read_commitment_substitution() {
        let (prepared, account_id, sources, continuation) =
            saturated_observation_replay_fixture(false, false);
        replay_observation_fixture(&prepared, &account_id, &sources, &continuation).unwrap();
        let mut collapsed = continuation.clone();
        let leaf = collapsed.page_occurrences.last().unwrap().clone();
        let evidence = collapsed.facts.decision_inputs["pages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|page| page["request_url"] == leaf.request_url)
            .unwrap()
            .clone();
        collapsed.page_occurrences = vec![leaf];
        collapsed.facts.decision_inputs["pages"] = serde_json::json!([evidence]);
        let mut substituted = continuation.clone();
        for occurrence in &mut substituted.page_occurrences {
            occurrence.request_url = occurrence
                .request_url
                .replace("https://data-api.polymarket.com", "https://other.example");
        }
        for evidence in substituted.facts.decision_inputs["pages"]
            .as_array_mut()
            .unwrap()
        {
            evidence["request_url"] = serde_json::json!(
                evidence["request_url"]
                    .as_str()
                    .unwrap()
                    .replace("https://data-api.polymarket.com", "https://other.example")
            );
        }
        for changed in [collapsed, substituted] {
            let error = replay_observation_fixture(&prepared, &account_id, &sources, &changed)
                .err()
                .unwrap();
            assert!(error.to_string().contains("commitment differs"), "{error}");
        }
    }

    /// PASS: strict economic replay resolves both observation receipts, their ordered receive
    /// clocks, source contracts, and websocket provenance before accepting the economic record.
    /// FAIL: either the websocket or REST observation provenance cannot replay from valid inputs.
    #[test]
    fn strict_live_economic_validates_observation_receipts() {
        let (prepared, account_id, sources, continuation) = observation_replay_fixture();
        replay_observation_fixture(&prepared, &account_id, &sources, &continuation).unwrap();

        let (rest_poll, rest_account_id, rest_sources, rest_continuation) =
            observation_replay_fixture_for(false);
        assert_eq!(rest_account_id, account_id);
        replay_observation_fixture(&rest_poll, &account_id, &rest_sources, &rest_continuation)
            .unwrap();
    }

    /// PASS: post-Baseline strict live replay reconstructs both REST-only and websocket-assisted
    /// decisions from all offset-0/offset-500 pages, whether the group is split across them or is
    /// present only on the non-final page; each reconstruction matches the recorded decision hash.
    /// FAIL: replay uses only the complete-bound page or requires that page to contain the trade.
    #[test]
    fn strict_live_economic_reconstructs_complete_multipage_activity_reads() {
        for websocket_assisted in [false, true] {
            for split_group in [false, true] {
                let (prepared, account_id, sources, continuation) =
                    multipage_observation_replay_fixture(websocket_assisted, split_group);
                replay_observation_fixture(&prepared, &account_id, &sources, &continuation)
                    .unwrap();
            }
        }
    }

    /// PASS: REST-only and websocket-assisted strict replay parse the retained saturated parent
    /// pages as evidence but aggregate only its complete child leaves, reconstructing the selected
    /// second-51 legs as exactly 10 shares at 0.52 and matching the recorded decision hash.
    /// FAIL: parent and child rows are concatenated into 20 shares or either provenance is rejected.
    #[test]
    fn strict_live_economic_excludes_saturated_parent_rows() {
        for websocket_assisted in [false, true] {
            let (prepared, account_id, sources, continuation) =
                saturated_observation_replay_fixture(websocket_assisted, false);
            replay_observation_fixture(&prepared, &account_id, &sources, &continuation).unwrap();
        }
    }

    /// PASS: strict replay finds and hashes a selected group that exists only in the old, non-final
    /// leaf of a saturated split graph rather than requiring the greatest-receipt leaf to contain it.
    /// FAIL: replay validates only the final leaf or includes saturated-parent production rows.
    #[test]
    fn strict_live_economic_reconstructs_selected_group_from_non_final_leaf() {
        let (prepared, account_id, sources, continuation) =
            saturated_observation_replay_fixture(false, true);
        replay_observation_fixture(&prepared, &account_id, &sources, &continuation).unwrap();
    }

    /// PASS: the post-Baseline live-risk verifier accepts both REST-only and websocket-assisted
    /// admissions after rebuilding a split offset-0/offset-500 group from its durable V3 page
    /// occurrences and matching the resulting decision hash.
    /// FAIL: the production strict-recovery path still validates only the final activity page.
    #[test]
    fn strict_live_economic_reconstructs_post_baseline_multipage_admissions() {
        for websocket_assisted in [false, true] {
            for split_group in [false, true] {
                let (dir, account_id, events, mut sources, paper_frames, mut admission) =
                    reconstructed_live_risk_fixture();
                let observation = admission.economic.observation.as_ref().unwrap().clone();
                let first_receipt = observation.complete_bound_receipt;
                let original_page = sources
                    .iter()
                    .find(|source| {
                        source.seq == first_receipt.sequence
                            && source.this_hash == first_receipt.this_hash
                    })
                    .map(|source| source.payload.clone())
                    .unwrap();
                let template = serde_json::from_slice::<Vec<serde_json::Value>>(&original_page)
                    .unwrap()
                    .remove(0);
                let (first_payload, final_payload) = if split_group {
                    (
                        full_activity_page_with_fragment(&template, "4.000000", "0.40", "1.600000"),
                        activity_page_fragment(&template, "6.000000", "0.60", "3.600000"),
                    )
                } else {
                    (
                        full_activity_page_with_fragment(
                            &template,
                            "10.000000",
                            "0.52",
                            "5.200000",
                        ),
                        serde_json::to_vec(&Vec::<serde_json::Value>::new()).unwrap(),
                    )
                };
                let first = sources
                    .iter_mut()
                    .find(|source| {
                        source.seq == first_receipt.sequence
                            && source.this_hash == first_receipt.this_hash
                    })
                    .unwrap();
                first.payload = first_payload.clone();
                first.raw_payload_hash = blake3::hash(&first_payload);

                let final_receipt = fixture_receipt(first_receipt.sequence.0 + 1);
                let received_at = OffsetDateTime::from_unix_timestamp(20).unwrap();
                let mut final_page = source_envelope(
                    final_receipt,
                    crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
                    final_payload.clone(),
                    received_at,
                );
                final_page.schema_version = pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION;
                final_page.parser_version = pe_source_polymarket_public::ACTIVITY_PARSER_VERSION;
                sources.push(final_page);
                if !websocket_assisted {
                    sources.retain(|source| {
                        source.source_id.0 != crate::activity_ingest::ACTIVITY_WS_SOURCE_ID
                    });
                }

                let selected_receipt = if websocket_assisted {
                    observation.source_receipt
                } else {
                    final_receipt
                };
                admission.economic.observation = Some(ObservationEvidence {
                    source_receipt: selected_receipt,
                    complete_bound_receipt: final_receipt,
                    observed_unix_ms: 20_000,
                    provenance: if websocket_assisted {
                        "activity_ws".to_owned()
                    } else {
                        "rest_poll".to_owned()
                    },
                });
                admission.identity.decision_hash = reconciled_activity_decision_hash(
                    &admission.identity,
                    &account_id,
                    received_at,
                    Price(dec!(0.52)),
                    ShareAmount::from_whole(10).unwrap(),
                );
                let continuation = observation_continuation_for(
                    &admission.identity,
                    &admission.economic,
                    &[first_payload.as_slice(), final_payload.as_slice()],
                    vec![
                        activity_page_fixture(
                            first_receipt,
                            &first_payload,
                            Some(19),
                            20,
                            0,
                            received_at,
                        ),
                        activity_page_fixture(
                            final_receipt,
                            &final_payload,
                            Some(19),
                            20,
                            pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT,
                            received_at,
                        ),
                    ],
                );
                record_read_commitment_envelope(&mut sources, &continuation);
                let source_trade_id = continuation.facts.source_trade_id.clone();
                let observation_pages = HashMap::from([(source_trade_id, (continuation, None))]);

                verify_replayed_live_risk_with_index(
                    &account_id,
                    &dir.path().join("live.journal"),
                    &events,
                    &admission,
                    LiveReplayEvidence {
                        source_envelopes: &sources,
                        source_receipts: None,
                        paper_frames: &paper_frames,
                        observation_pages: &observation_pages,
                    },
                )
                .unwrap();
            }
        }
    }

    /// PASS: the post-Baseline strict recovery verifier accepts REST-only and websocket-assisted
    /// admissions whose V3 evidence is an inclusive-API saturated parent plus complete children;
    /// the selected 4@0.40 and 6@0.60 legs reconstruct as 10@0.52 and match the recorded hash.
    /// FAIL: strict recovery concatenates the saturated parent or cannot replay either provenance.
    #[test]
    fn strict_live_economic_reconstructs_post_baseline_saturated_admissions() {
        for websocket_assisted in [false, true] {
            let (dir, account_id, events, mut sources, paper_frames, mut admission) =
                reconstructed_live_risk_fixture();
            let old_observation = admission.economic.observation.as_ref().unwrap().clone();
            let original_page = sources
                .iter()
                .find(|source| {
                    source.seq == old_observation.complete_bound_receipt.sequence
                        && source.this_hash == old_observation.complete_bound_receipt.this_hash
                })
                .map(|source| source.payload.clone())
                .unwrap();
            let template = serde_json::from_slice::<Vec<serde_json::Value>>(&original_page)
                .unwrap()
                .remove(0);
            sources.retain(|source| {
                source.seq != old_observation.source_receipt.sequence
                    && source.seq != old_observation.complete_bound_receipt.sequence
            });
            let received_at = OffsetDateTime::from_unix_timestamp(20).unwrap();
            let graph = saturated_activity_graph_fixture(&template, 220, received_at, false);
            let websocket_receipt = fixture_receipt(219);
            if websocket_assisted {
                let mut source = source_envelope(
                    websocket_receipt,
                    crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
                    graph.websocket_payload.clone(),
                    received_at,
                );
                source.schema_version = pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION;
                source.parser_version = pe_source_polymarket_public::ACTIVITY_PARSER_VERSION;
                sources.push(source);
            }
            for (receipt, payload) in &graph.payloads {
                let mut source = source_envelope(
                    *receipt,
                    crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
                    payload.clone(),
                    received_at,
                );
                source.schema_version = pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION;
                source.parser_version = pe_source_polymarket_public::ACTIVITY_PARSER_VERSION;
                sources.push(source);
            }
            admission.economic.observation = Some(ObservationEvidence {
                source_receipt: if websocket_assisted {
                    websocket_receipt
                } else {
                    graph.complete_bound_receipt
                },
                complete_bound_receipt: graph.complete_bound_receipt,
                observed_unix_ms: 20_000,
                provenance: if websocket_assisted {
                    "activity_ws".to_owned()
                } else {
                    "rest_poll".to_owned()
                },
            });
            admission.identity.decision_hash = reconciled_activity_decision_hash(
                &admission.identity,
                &account_id,
                graph.selected_source_time,
                Price(dec!(0.52)),
                ShareAmount::from_whole(10).unwrap(),
            );
            let continuation = observation_continuation_for(
                &admission.identity,
                &admission.economic,
                &[graph.selected_payload.as_slice()],
                graph.pages,
            );
            record_read_commitment_envelope(&mut sources, &continuation);
            let source_trade_id = continuation.facts.source_trade_id.clone();
            let observation_pages = HashMap::from([(source_trade_id, (continuation, None))]);

            verify_replayed_live_risk_with_index(
                &account_id,
                &dir.path().join("live.journal"),
                &events,
                &admission,
                LiveReplayEvidence {
                    source_envelopes: &sources,
                    source_receipts: None,
                    paper_frames: &paper_frames,
                    observation_pages: &observation_pages,
                },
            )
            .unwrap();
        }
    }

    /// PASS: two byte-identical retained page occurrences both contribute to strict replay, so the
    /// reconstructed quantity and decision hash preserve occurrence multiplicity.
    /// FAIL: replay deduplicates identical page bytes and reconstructs only half the trade.
    #[test]
    fn strict_live_economic_preserves_repeated_page_multiplicity() {
        let (mut prepared, account_id, mut sources, continuation) =
            multipage_observation_replay_fixture(true, false);
        let first_receipt = continuation.page_occurrences()[0].receipt;
        let second_receipt = continuation.page_occurrences()[1].receipt;
        let template = sources
            .iter()
            .find(|(receipt, _)| *receipt == first_receipt)
            .map(|(_, source)| {
                serde_json::from_slice::<Vec<serde_json::Value>>(&source.payload)
                    .unwrap()
                    .remove(0)
            })
            .unwrap();
        let repeated = full_activity_page_with_fragment(&template, "5.000000", "0.52", "2.600000");
        for receipt in [first_receipt, second_receipt] {
            sources
                .iter_mut()
                .find(|(candidate, _)| *candidate == receipt)
                .unwrap()
                .1
                .payload = repeated.clone();
        }
        let terminal_receipt = fixture_receipt(second_receipt.sequence.0 + 1);
        let terminal_payload = serde_json::to_vec(&Vec::<serde_json::Value>::new()).unwrap();
        sources.push((
            terminal_receipt,
            RecordedEconomicSource {
                payload: terminal_payload.clone(),
                received_unix_ms: 22_000,
                source_id: crate::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned(),
                schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                content_type: ContentType::Json,
            },
        ));
        prepared
            .economic
            .observation
            .as_mut()
            .unwrap()
            .complete_bound_receipt = terminal_receipt;
        bind_reconciled_activity_decision(
            &mut prepared,
            &account_id,
            OffsetDateTime::from_unix_timestamp(18).unwrap(),
            Price(dec!(0.52)),
            ShareAmount::from_whole(10).unwrap(),
        );
        let continuation = observation_continuation(
            &prepared,
            &[
                repeated.as_slice(),
                repeated.as_slice(),
                terminal_payload.as_slice(),
            ],
            vec![
                activity_page_fixture(
                    first_receipt,
                    &repeated,
                    Some(17),
                    18,
                    0,
                    OffsetDateTime::from_unix_timestamp(20).unwrap(),
                ),
                activity_page_fixture(
                    second_receipt,
                    &repeated,
                    Some(17),
                    18,
                    pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT,
                    OffsetDateTime::from_unix_timestamp(21).unwrap(),
                ),
                activity_page_fixture(
                    terminal_receipt,
                    &terminal_payload,
                    Some(17),
                    18,
                    pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT * 2,
                    OffsetDateTime::from_unix_timestamp(22).unwrap(),
                ),
            ],
        );

        record_read_commitment(&mut sources, &continuation);
        replay_observation_fixture(&prepared, &account_id, &sources, &continuation).unwrap();
    }

    /// PASS: strict replay fails closed when any retained page is absent or substituted, when only
    /// a page-local partial group is offered, and when the declared completeness bound is not the
    /// greatest retained page receipt.
    /// FAIL: incomplete or differently bound page evidence reproduces an authentic admission.
    #[test]
    fn strict_live_economic_rejects_incomplete_or_substituted_multipage_reads() {
        let (prepared, account_id, sources, continuation) =
            multipage_observation_replay_fixture(true, true);
        let first_receipt = continuation.page_occurrences()[0].receipt;

        let missing = sources
            .iter()
            .filter(|(receipt, _)| *receipt != first_receipt)
            .cloned()
            .collect::<Vec<_>>();
        assert!(
            replay_observation_fixture(&prepared, &account_id, &missing, &continuation).is_err()
        );

        let mut substituted = sources.clone();
        substituted
            .iter_mut()
            .find(|(receipt, _)| *receipt == first_receipt)
            .unwrap()
            .1
            .payload = serde_json::to_vec(&Vec::<serde_json::Value>::new()).unwrap();
        assert!(
            replay_observation_fixture(&prepared, &account_id, &substituted, &continuation,)
                .is_err()
        );

        let mut page_local = continuation.clone();
        page_local.page_occurrences.remove(0);
        assert!(replay_observation_fixture(&prepared, &account_id, &sources, &page_local).is_err());

        let mut wrong_bound_prepared = prepared.clone();
        wrong_bound_prepared
            .economic
            .observation
            .as_mut()
            .unwrap()
            .complete_bound_receipt = first_receipt;
        let wrong_bound = continuation.clone();
        assert!(
            replay_observation_fixture(&wrong_bound_prepared, &account_id, &sources, &wrong_bound,)
                .is_err()
        );
    }

    /// PASS: removing either observation receipt, changing its source contract or provenance,
    /// or changing the copied observation clock causes strict replay to fail closed.
    /// FAIL: any missing, future, or tampered observation evidence remains replayable.
    #[test]
    fn strict_live_economic_rejects_absent_or_tampered_observation_receipts() {
        let (prepared, account_id, sources, continuation) = observation_replay_fixture();
        let observation = prepared.economic.observation.as_ref().unwrap();
        for missing_receipt in [
            observation.source_receipt,
            observation.complete_bound_receipt,
        ] {
            let missing = sources
                .iter()
                .filter(|(receipt, _)| *receipt != missing_receipt)
                .cloned()
                .collect::<Vec<_>>();
            assert!(
                replay_observation_fixture(&prepared, &account_id, &missing, &continuation)
                    .is_err()
            );
        }

        let mut wrong_contract = sources.clone();
        wrong_contract
            .iter_mut()
            .find(|(receipt, _)| *receipt == observation.complete_bound_receipt)
            .unwrap()
            .1
            .source_id = crate::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned();
        assert!(
            replay_observation_fixture(&prepared, &account_id, &wrong_contract, &continuation)
                .is_err()
        );

        let mut wrong_provenance = prepared.clone();
        wrong_provenance
            .economic
            .observation
            .as_mut()
            .unwrap()
            .provenance = "rest_poll".to_owned();
        assert!(
            replay_observation_fixture(&wrong_provenance, &account_id, &sources, &continuation)
                .is_err()
        );

        let mut wrong_clock = prepared.clone();
        wrong_clock
            .economic
            .observation
            .as_mut()
            .unwrap()
            .observed_unix_ms += 1;
        assert!(
            replay_observation_fixture(&wrong_clock, &account_id, &sources, &continuation).is_err()
        );

        for future_receipt in [
            observation.source_receipt,
            observation.complete_bound_receipt,
        ] {
            let mut future_source = sources.clone();
            future_source
                .iter_mut()
                .find(|(receipt, _)| *receipt == future_receipt)
                .unwrap()
                .1
                .received_unix_ms = prepared.economic.risk.evaluated_at_unix_ms + 1;
            assert!(
                replay_observation_fixture(&prepared, &account_id, &future_source, &continuation,)
                    .is_err()
            );
        }

        let mut missing = prepared.clone();
        missing.economic.observation = None;
        assert!(
            replay_observation_fixture(&missing, &account_id, &sources, &continuation).is_err()
        );
    }

    /// PASS: current live replay rejects malformed websocket evidence and websocket payloads that
    /// identify a different wallet or trade, even when the referenced receipt metadata is valid.
    /// FAIL: any selected payload that cannot reproduce the recorded trade remains replayable.
    #[test]
    fn strict_live_economic_rejects_malformed_or_different_selected_trade() {
        let (prepared, account_id, sources, continuation) = observation_replay_fixture();
        let selected = prepared
            .economic
            .observation
            .as_ref()
            .unwrap()
            .source_receipt;
        for payload in [
            br#"{"malformed":true}"#.to_vec(),
            serde_json::to_vec(&serde_json::json!({
                "proxyWallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "timestamp": 18,
                "conditionId": prepared.economic.market.condition_id.0,
                "asset": prepared.economic.market.token_id.0,
                "side": "BUY",
                "size": "10.000000",
                "price": "0.80",
                "transactionHash": format!("0x{}", "55".repeat(32)),
                "outcomeIndex": 0,
                "outcome": "Yes"
            }))
            .unwrap(),
            activity_payload_pair(
                &prepared.economic,
                OffsetDateTime::from_unix_timestamp(18).unwrap(),
                WalletAddress([0xaa; 20]),
                &format!("0x{}", "66".repeat(32)),
            )
            .0,
        ] {
            let mut changed = sources.clone();
            changed
                .iter_mut()
                .find(|(receipt, _)| *receipt == selected)
                .unwrap()
                .1
                .payload = payload;
            assert!(
                replay_observation_fixture(&prepared, &account_id, &changed, &continuation)
                    .is_err()
            );
        }
    }

    /// PASS: current live replay rejects a syntactically valid complete REST page that is
    /// unrelated to the selected websocket trade.
    /// FAIL: receipt metadata alone allows an unrelated REST page to satisfy the binding.
    #[test]
    fn strict_live_economic_rejects_unrelated_rest_page() {
        let (prepared, account_id, mut sources, continuation) = observation_replay_fixture();
        let bound = prepared
            .economic
            .observation
            .as_ref()
            .unwrap()
            .complete_bound_receipt;
        let unrelated = activity_payload_pair(
            &prepared.economic,
            OffsetDateTime::from_unix_timestamp(18).unwrap(),
            WalletAddress([0xaa; 20]),
            &format!("0x{}", "66".repeat(32)),
        )
        .1;
        sources
            .iter_mut()
            .find(|(receipt, _)| *receipt == bound)
            .unwrap()
            .1
            .payload = unrelated;
        assert!(
            replay_observation_fixture(&prepared, &account_id, &sources, &continuation).is_err()
        );
    }

    /// PASS: changing only the REST trade's exact quantity, price, or source clock leaves its
    /// group identity intact but breaks the recorded live decision binding.
    /// FAIL: strict replay accepts different economic signal inputs under the same trade id.
    #[test]
    fn strict_live_economic_binds_trade_quantity_price_and_source_clock() {
        let (prepared, account_id, sources, continuation) = observation_replay_fixture();
        let bound = prepared
            .economic
            .observation
            .as_ref()
            .unwrap()
            .complete_bound_receipt;
        for (field, value) in [
            ("size", serde_json::json!("9.000000")),
            ("price", serde_json::json!("0.70")),
            ("timestamp", serde_json::json!(17)),
        ] {
            let mut changed = sources.clone();
            let source = &mut changed
                .iter_mut()
                .find(|(receipt, _)| *receipt == bound)
                .unwrap()
                .1;
            let mut payload: serde_json::Value = serde_json::from_slice(&source.payload).unwrap();
            payload.as_array_mut().unwrap()[0]
                .as_object_mut()
                .unwrap()
                .insert(field.to_owned(), value);
            source.payload = serde_json::to_vec(&payload).unwrap();
            assert!(
                replay_observation_fixture(&prepared, &account_id, &changed, &continuation)
                    .is_err(),
                "{field}"
            );
        }
    }

    /// PASS: an untampered producer-owned continuation verifies, while changing its frozen
    /// quality/confidence, price, quantity, epoch, transaction hash, semantic revision fails closed. The quality case retains a decision hash produced at quality 100,
    /// proving that a different hash-compatible quality cannot authorize the tampered facts.
    /// FAIL: strict replay searches alternative qualities or trusts raw aggregate facts in place
    /// of the immutable continuation.
    #[test]
    fn strict_live_economic_binds_every_frozen_decision_fact() {
        let (prepared, account_id, sources, continuation) = observation_replay_fixture();
        replay_observation_fixture(&prepared, &account_id, &sources, &continuation).unwrap();

        let rejects = |label: &str, changed: &DecisionContinuationV3| {
            assert!(
                replay_observation_fixture(&prepared, &account_id, &sources, changed).is_err(),
                "{label}"
            );
        };

        let mut changed = continuation.clone();
        changed.facts.reconstruction_quality = ReconstructionQuality::new(99).unwrap();
        changed.facts.action_confidence_ppm = ProbabilityPpm(990_000);
        rejects("quality and confidence", &changed);

        let mut changed = continuation.clone();
        changed.facts.action_confidence_ppm = ProbabilityPpm(999_999);
        rejects("confidence", &changed);

        let mut changed = continuation.clone();
        changed.facts.price = Price(dec!(0.79));
        rejects("price", &changed);

        let mut changed = continuation.clone();
        changed.facts.share_amount = ShareAmount::from_whole(9).unwrap();
        rejects("quantity", &changed);

        let mut changed = continuation.clone();
        changed.facts.source_epoch -= 1;
        rejects("source epoch", &changed);

        let mut changed = continuation.clone();
        changed.facts.transaction_hash = format!("0x{}", "66".repeat(32));
        rejects("transaction hash", &changed);

        let mut changed = continuation.clone();
        changed.facts.semantic_revision = "0".repeat(64);
        rejects("semantic revision", &changed);
    }

    /// PASS: payloads that conflict with the copied admission or ladder fail even when presented
    /// under the journal's referenced receipt identity.
    /// FAIL: a conflicting payload under the referenced receipt identity is accepted.
    #[test]
    fn strict_live_economic_rejects_payload_conflicts() {
        let (dir, account_id, events, sources, paper_frames, admission) =
            reconstructed_live_risk_fixture();
        let journal_path = dir.path().join("live.journal");
        for receipt in [
            admission.economic.admission.receipts.gamma,
            admission.economic.admission.receipts.clob_long,
            admission.economic.admission.receipts.clob_compact,
            admission.economic.book_receipt,
        ] {
            let mut conflicting = sources.clone();
            let envelope = conflicting
                .iter_mut()
                .find(|source| {
                    source.seq == receipt.sequence && source.this_hash == receipt.this_hash
                })
                .unwrap();
            envelope.payload = br#"{"conflicting":true}"#.to_vec();
            assert!(matches!(
                verify_replayed_live_risk(
                    &account_id,
                    &journal_path,
                    &events,
                    &admission,
                    &conflicting,
                    &paper_frames,
                ),
                Err(ProjectionReducerError::InvalidRiskEvidence)
            ));
        }
    }

    /// PASS: a journal that copies internally consistent zero-fee fields from a nonzero compact
    /// fee source is rejected by source-derived reserve, debit, risk, and canonical recomposition.
    /// FAIL: the zero-fee copy is accepted, or the source-derived reserve/debit/risk agrees with
    /// it.
    #[test]
    fn strict_live_economic_rejects_zero_copied_fee_for_nonzero_source_fee() {
        let (dir, account_id, events, sources, paper_frames, mut admission) =
            reconstructed_live_risk_fixture();
        let journal_path = dir.path().join("live.journal");
        admission.economic.admission.fee_schedule = pe_venue_polymarket::CompactFeeSchedule::Zero;
        admission.economic.fee = FeeAudit {
            schedule: pe_venue_polymarket::CompactFeeSchedule::Zero,
            expected_fee: CollateralAmount::ZERO,
            reserve: CollateralAmount::ZERO,
        };
        admission.economic.sizing.all_in_price = Price(dec!(0.8));
        admission.economic.balance.worst_case_debit = admission.economic.sizing.principal;
        admission.economic.risk.snapshot.proposed_trade_bps = BasisPoints(2_500);
        admission.economic.risk.decision =
            match pe_risk_engine::evaluate_risk(&admission.economic.risk.snapshot) {
                pe_risk_engine::RiskDecision::Approved => RiskDecisionAudit::Approved,
                pe_risk_engine::RiskDecision::Blocked(reason) => {
                    RiskDecisionAudit::Blocked { reason }
                }
            };

        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &admission,
                &sources,
                &paper_frames,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));
    }

    /// PASS: strict replay treats each recorded in-domain per-trade cap as an input, reconstructs
    /// its derived decision, and fails closed before reconstruction for an out-of-domain cap.
    #[test]
    fn strict_live_risk_replays_recorded_cap_domain() {
        let (dir, account_id, events, sources, paper_frames, admission) =
            reconstructed_live_risk_fixture();
        let journal_path = dir.path().join("live.journal");

        for cap in [
            pe_strategy_winner_follow::PerTradeCap::Bps(5_000).resolve_bps(TradingMode::LiveTiny),
            pe_strategy_winner_follow::PerTradeCap::Unlimited.resolve_bps(TradingMode::LiveTiny),
        ] {
            let mut recorded = admission.clone();
            recorded.economic.risk.snapshot.per_trade_cap_bps = cap;
            recorded.economic.risk.decision =
                match pe_risk_engine::evaluate_risk(&recorded.economic.risk.snapshot) {
                    pe_risk_engine::RiskDecision::Approved => RiskDecisionAudit::Approved,
                    pe_risk_engine::RiskDecision::Blocked(reason) => {
                        RiskDecisionAudit::Blocked { reason }
                    }
                };
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &recorded,
                &sources,
                &paper_frames,
            )
            .unwrap();
        }

        for cap in [0, 10_001] {
            let mut invalid = admission.clone();
            invalid.economic.risk.snapshot.per_trade_cap_bps = cap;
            invalid.economic.risk.decision =
                match pe_risk_engine::evaluate_risk(&invalid.economic.risk.snapshot) {
                    pe_risk_engine::RiskDecision::Approved => RiskDecisionAudit::Approved,
                    pe_risk_engine::RiskDecision::Blocked(reason) => {
                        RiskDecisionAudit::Blocked { reason }
                    }
                };
            assert!(matches!(
                verify_replayed_live_risk(
                    &account_id,
                    &journal_path,
                    &events,
                    &invalid,
                    &sources,
                    &paper_frames,
                ),
                Err(ProjectionReducerError::InvalidRiskEvidence)
            ));
        }
    }

    /// PASS: a frame made durable after runtime captured its paper prefix cannot enter replay only
    /// because its earlier receive timestamp predates evaluation; absent or tampered prefixes fail.
    #[test]
    fn strict_live_risk_replays_the_recorded_paper_prefix() {
        let (dir, account_id, events, sources, _, admission) = reconstructed_live_risk_fixture();
        let journal_path = dir.path().join("live.journal");
        let paper_path = dir.path().join("paper.log");
        let late_received = OffsetDateTime::from_unix_timestamp(19).unwrap();
        let mut writer = pe_event_log::Writer::open(&paper_path).unwrap();
        let later_durable_receipt = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: crate::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(late_received),
                received_at: ReceivedAt(late_received),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&PaperLogRecord::RiskHaltChanged {
                    owner: RiskHaltOwner::Paper,
                    cause: pe_risk_engine::RiskHaltCause::CopyLatency,
                    state: HaltState::Engaged,
                    evidence: serde_json::json!({"fixture": "late-durable-earlier-received"}),
                })
                .unwrap(),
            })
            .unwrap();
        drop(writer);
        let complete_later_log = scan_paper_log(&paper_path).unwrap();

        verify_replayed_live_risk(
            &account_id,
            &journal_path,
            &events,
            &admission,
            &sources,
            &complete_later_log,
        )
        .unwrap();

        let mut tampered = admission.clone();
        tampered.economic.risk.financial_prefix = AppendReceipt {
            sequence: later_durable_receipt.sequence,
            this_hash: blake3::hash(b"tampered-financial-prefix"),
        };
        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &tampered,
                &sources,
                &complete_later_log,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));
    }

    /// PASS: strict live reconstruction applies the shared strategy-wide halt overlay from the
    /// paper prefix at the evaluation clock and binds the resulting decision to that prefix.
    #[test]
    fn strict_live_risk_applies_the_paper_global_halt_prefix() {
        let (dir, account_id, events, sources, _, mut admission) =
            reconstructed_live_risk_fixture();
        let now = OffsetDateTime::from_unix_timestamp(20).unwrap();
        let paper_path = dir.path().join("paper.log");
        let mut writer = pe_event_log::Writer::open(&paper_path).unwrap();
        let financial_prefix = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: crate::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&PaperLogRecord::RiskHaltChanged {
                    owner: RiskHaltOwner::Paper,
                    cause: pe_risk_engine::RiskHaltCause::CopyLatency,
                    state: HaltState::Engaged,
                    evidence: serde_json::json!({"fixture": "global-halt"}),
                })
                .unwrap(),
            })
            .unwrap();
        drop(writer);
        let paper_frames = scan_paper_log(&paper_path).unwrap();
        admission.economic.risk.financial_prefix = financial_prefix;
        admission
            .economic
            .risk
            .snapshot
            .copy_latency_kill_switch_active = true;
        admission.economic.risk.decision =
            match pe_risk_engine::evaluate_risk(&admission.economic.risk.snapshot) {
                pe_risk_engine::RiskDecision::Approved => RiskDecisionAudit::Approved,
                pe_risk_engine::RiskDecision::Blocked(reason) => {
                    RiskDecisionAudit::Blocked { reason }
                }
            };

        verify_replayed_live_risk(
            &account_id,
            &dir.path().join("live.journal"),
            &events,
            &admission,
            &sources,
            &paper_frames,
        )
        .unwrap();
        assert!(matches!(
            admission.economic.risk.decision,
            RiskDecisionAudit::Blocked { .. }
        ));
    }

    /// PASS: an open live position requires its exact source-backed price inventory; a missing,
    /// unrelated-market, or substituted-price receipt cannot validate a copied snapshot.
    #[test]
    fn strict_live_risk_rejects_missing_unrelated_and_substituted_price_receipts() {
        let (dir, account_id, events, mut sources, paper_frames, admission) =
            reconstructed_live_risk_fixture();
        let journal_path = dir.path().join("live.journal");

        let mut missing = admission.clone();
        missing.economic.risk.price_receipts.clear();
        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &missing,
                &sources,
                &paper_frames,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));

        let now = OffsetDateTime::from_unix_timestamp(20).unwrap();
        let unrelated_receipt = fixture_receipt(201);
        sources.push(gamma_risk_price_source(
            unrelated_receipt,
            &format!("0x{}", "88".repeat(32)),
            ["0.9", "0.1"],
            now,
        ));
        let mut unrelated = admission.clone();
        unrelated.economic.risk.price_receipts = vec![unrelated_receipt];
        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &unrelated,
                &sources,
                &paper_frames,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));

        let substituted_receipt = fixture_receipt(202);
        let condition_id = admission
            .identity
            .fill_projection
            .as_ref()
            .unwrap()
            .market_id
            .clone();
        sources.push(gamma_risk_price_source(
            substituted_receipt,
            &condition_id,
            ["0.8", "0.2"],
            now,
        ));
        let mut substituted = admission;
        substituted.economic.risk.price_receipts = vec![substituted_receipt];
        assert!(matches!(
            verify_replayed_live_risk(
                &account_id,
                &journal_path,
                &events,
                &substituted,
                &sources,
                &paper_frames,
            ),
            Err(ProjectionReducerError::InvalidRiskEvidence)
        ));
    }

    /// PASS: live risk price expiry uses millisecond units through the receipt index: observations
    /// aged 1 ms and 59,999 ms remain fresh, while the strict 60,000 ms boundary is expired.
    #[tokio::test]
    async fn risk_price_expiry_uses_millisecond_ttl_boundaries() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let state = fanout_state(
            &dir,
            db,
            LiveAccountsSnapshot::default(),
            "http://127.0.0.1:9",
            None,
        );
        let observed_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
        let receipt = state
            .config
            .source_log
            .append(EnvelopeIn {
                source_id: SourceId("risk-price-boundary".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(observed_at),
                received_at: ReceivedAt(observed_at),
                content_type: ContentType::Json,
                payload: br#"{"price":"0.50"}"#.to_vec(),
            })
            .await
            .unwrap();
        let mut risk = finality_prepared().economic.risk;
        risk.price_receipts = vec![receipt];

        assert!(
            !risk_price_expired(&state, &risk, observed_at + time::Duration::milliseconds(1))
                .unwrap()
        );
        assert!(
            !risk_price_expired(
                &state,
                &risk,
                observed_at + time::Duration::milliseconds(59_999),
            )
            .unwrap()
        );
        assert!(
            risk_price_expired(
                &state,
                &risk,
                observed_at + time::Duration::milliseconds(60_000),
            )
            .unwrap()
        );
    }

    /// PASS: a dispatch boundary that resumes after an awaited operation re-resolves the book
    /// receipt and rejects the ladder once the sampled action time is 2,001 ms after receipt.
    /// FAIL: the pre-await freshness result is reused after the modeled awaited delay.
    #[tokio::test]
    async fn dispatch_rechecks_ladder_after_awaited_boundary() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let state = fanout_state(
            &dir,
            db,
            LiveAccountsSnapshot::default(),
            "http://127.0.0.1:9",
            None,
        );
        let observed_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
        let book_receipt = state
            .config
            .source_log
            .append(EnvelopeIn {
                source_id: SourceId(CLOB_BOOK_SOURCE_ID.to_owned()),
                schema_version: CLOB_BOOK_SCHEMA_VERSION,
                parser_version: CLOB_BOOK_PARSER_VERSION,
                observed_at: SourceTimestamp(observed_at),
                received_at: ReceivedAt(observed_at),
                content_type: ContentType::Json,
                payload: br#"{"asks":[]}"#.to_vec(),
            })
            .await
            .unwrap();
        let mut economic = finality_prepared().economic;
        economic.book_receipt = book_receipt;
        let observed_unix_ms = unix_millis_i64(observed_at);
        let action_clock = AtomicI64::new(observed_unix_ms + 1_999);

        assert!(
            !economic_book_receipt_is_stale(
                &state,
                &economic,
                OffsetDateTime::from_unix_timestamp_nanos(
                    i128::from(action_clock.load(Ordering::SeqCst)) * 1_000_000,
                )
                .unwrap(),
            )
            .unwrap()
        );
        tokio::task::yield_now().await;
        action_clock.store(observed_unix_ms + 2_001, Ordering::SeqCst);
        assert!(
            economic_book_receipt_is_stale(
                &state,
                &economic,
                OffsetDateTime::from_unix_timestamp_nanos(
                    i128::from(action_clock.load(Ordering::SeqCst)) * 1_000_000,
                )
                .unwrap(),
            )
            .unwrap()
        );
    }

    fn settings(
        mode: Option<&str>,
        dollar: Option<&str>,
        contracts: Option<i64>,
    ) -> AccountSettingsRow {
        AccountSettingsRow {
            account_id: "account".to_owned(),
            live_sizing_mode: mode.map(str::to_owned),
            live_sizing_dollar_usd: dollar.map(str::to_owned),
            live_sizing_contracts: contracts,
        }
    }

    #[test]
    fn sizing_mode_parses_exact_dollars_and_fails_closed() {
        let fallback = SizingMode::Kelly;
        // Cast text decodes to the exact Decimal, whole or fractional.
        assert_eq!(
            settings(Some("dollar"), Some("1"), None).sizing_mode(fallback),
            Ok(SizingMode::Dollar { usd: dec!(1) })
        );
        assert_eq!(
            settings(Some("dollar"), Some("1.25"), None).sizing_mode(fallback),
            Ok(SizingMode::Dollar { usd: dec!(1.25) })
        );
        // `dollar` with an absent/null/non-positive/malformed cell fails closed.
        for dollar in [None, Some("0"), Some("-1"), Some("not-a-number")] {
            assert_eq!(
                settings(Some("dollar"), dollar, None).sizing_mode(fallback),
                Err("live_sizing_dollar_invalid"),
                "dollar cell {dollar:?}"
            );
        }
        // A NULL mode is the runtime fallback; kelly/contract ignore the dollar cell.
        assert_eq!(
            settings(None, None, None).sizing_mode(fallback),
            Ok(fallback)
        );
        assert_eq!(
            settings(Some("kelly"), Some("not-a-number"), None).sizing_mode(fallback),
            Ok(SizingMode::Kelly)
        );
        assert_eq!(
            settings(Some("contract"), Some("not-a-number"), Some(3)).sizing_mode(fallback),
            Ok(SizingMode::Contract { contracts: 3 })
        );
        assert_eq!(
            settings(Some("bogus"), Some("1"), None).sizing_mode(fallback),
            Err("live_sizing_mode_invalid")
        );
    }

    async fn account_settings_page(
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<Vec<serde_json::Value>> {
        // The wire contract under test (#514): the money column is requested with the
        // exact-text cast, and the response carries it as a JSON string.
        assert_eq!(
            query.get("select").map(String::as_str),
            Some("account_id,live_sizing_mode,live_sizing_dollar_usd::text,live_sizing_contracts")
        );
        assert_eq!(query.get("account_id").map(String::as_str), Some("eq.acct"));
        Json(vec![serde_json::json!({
            "account_id": "acct",
            "live_sizing_mode": "dollar",
            "live_sizing_dollar_usd": "1.25",
            "live_sizing_contracts": null
        })])
    }

    #[tokio::test]
    async fn account_settings_fetch_casts_the_money_column_and_sizes() {
        let app = Router::new().route("/rest/v1/accounts", get(account_settings_page));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        let row =
            fetch_account_settings_from(&client, &format!("http://{address}"), "token", "acct")
                .await
                .unwrap();
        assert_eq!(
            row.sizing_mode(SizingMode::Kelly),
            Ok(SizingMode::Dollar { usd: dec!(1.25) })
        );
    }

    struct FakeVenue {
        calls: Mutex<Vec<String>>,
        outcomes: Mutex<Vec<LiveVenueReconciledOutcome>>,
    }

    impl FakeVenue {
        fn new(outcomes: Vec<LiveVenueReconciledOutcome>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                outcomes: Mutex::new(outcomes.into_iter().rev().collect()),
            }
        }

        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl LiveOrderVenue for FakeVenue {
        type Submission = ();

        fn prepare<'a>(
            &'a self,
            _request: LiveVenuePrepareRequest,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            LiveVenuePrepared<Self::Submission>,
                            LiveVenuePreparationError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async { Err(LiveVenuePreparationError::Venue) })
        }

        fn post_once<'a>(
            &'a self,
            _submission: Self::Submission,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            pe_core_types::RawHttpResponse,
                            pe_core_types::RawTransportFailure,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async {
                Err(pe_core_types::RawTransportFailure {
                    source_id: "fake".to_owned(),
                    endpoint_kind: "post".to_owned(),
                    method: "POST".to_owned(),
                    path: "/order".to_owned(),
                    ordered_query: Vec::new(),
                    attempt_ordinal: 1,
                    observed_at: OffsetDateTime::UNIX_EPOCH,
                    received_at: OffsetDateTime::UNIX_EPOCH,
                    error_class: pe_core_types::TransportErrorClass::Other,
                    schema_version: 1,
                    parser_version: 1,
                    adapter_version: "fake".to_owned(),
                })
            })
        }

        fn classify_post_response(
            &self,
            _response: &pe_core_types::RawHttpResponse,
        ) -> Result<LivePostClassification, LivePostParseError> {
            Err(LivePostParseError::InvalidResponse)
        }

        fn reconcile_and_cancel_by_order_hash<'a>(
            &'a self,
            order_hash: &'a str,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<LiveVenueReconciliation, LiveVenueReconciliationError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.calls.lock().unwrap().push(order_hash.to_owned());
                let outcome = self.outcomes.lock().unwrap().pop().unwrap_or(
                    LiveVenueReconciledOutcome::Ambiguous {
                        kind: LiveOrderAmbiguityKind::ReconciliationUnavailable,
                    },
                );
                Ok(LiveVenueReconciliation {
                    outcome,
                    evidence: Vec::new(),
                })
            })
        }

        fn read_balance_and_allowance<'a>(
            &'a self,
            _neg_risk: bool,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<LiveVenueAccountState, LiveVenueAccountReadError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async {
                Err(LiveVenueAccountReadError {
                    kind: LiveAccountReadFailure::Protocol,
                    evidence: Vec::new(),
                    request_descriptor_hashes: Vec::new(),
                })
            })
        }
    }

    struct RecoveryPostVenue {
        client: reqwest::Client,
        order_url: String,
        prepared: pe_venue_polymarket::PreparedPolymarketBuy,
        observed_at: OffsetDateTime,
        prepare_clock_millis: Option<Arc<AtomicI64>>,
        prepare_advance_millis: i64,
        post_attempts: Option<Arc<AtomicUsize>>,
        account_state: Option<pe_execution_core::LiveVenueAccountState>,
        prepare_barrier: Option<Arc<tokio::sync::Barrier>>,
        prepare_release: Option<Arc<tokio::sync::Notify>>,
    }

    impl LiveOrderVenue for RecoveryPostVenue {
        type Submission = ();

        fn prepare<'a>(
            &'a self,
            _request: LiveVenuePrepareRequest,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            LiveVenuePrepared<Self::Submission>,
                            LiveVenuePreparationError,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                if let Some(barrier) = &self.prepare_barrier {
                    barrier.wait().await;
                }
                if let Some(release) = &self.prepare_release {
                    release.notified().await;
                }
                if let Some(clock_millis) = &self.prepare_clock_millis {
                    clock_millis.fetch_add(self.prepare_advance_millis, Ordering::SeqCst);
                }
                Ok(LiveVenuePrepared::new(self.prepared.clone(), ()))
            })
        }

        fn post_once<'a>(
            &'a self,
            _submission: Self::Submission,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            pe_core_types::RawHttpResponse,
                            pe_core_types::RawTransportFailure,
                        >,
                    > + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                if let Some(post_attempts) = &self.post_attempts {
                    post_attempts.fetch_add(1, Ordering::SeqCst);
                }
                self.client.post(&self.order_url).send().await.unwrap();
                Ok(pe_core_types::RawHttpResponse {
                    source_id: "recovery-loopback".to_owned(),
                    endpoint_kind: "order-post".to_owned(),
                    method: "POST".to_owned(),
                    path: "/order".to_owned(),
                    ordered_query: Vec::new(),
                    status: 400,
                    headers: Vec::new(),
                    body: Vec::new(),
                    attempt_ordinal: 1,
                    source_at: None,
                    observed_at: self.observed_at,
                    received_at: self.observed_at,
                    schema_version: 1,
                    parser_version: 1,
                    adapter_version: "recovery-loopback-v1".to_owned(),
                })
            })
        }

        fn classify_post_response(
            &self,
            _response: &pe_core_types::RawHttpResponse,
        ) -> Result<LivePostClassification, LivePostParseError> {
            Ok(LivePostClassification::Rejected {
                venue_order_id: None,
            })
        }

        fn reconcile_and_cancel_by_order_hash<'a>(
            &'a self,
            _order_hash: &'a str,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<LiveVenueReconciliation, LiveVenueReconciliationError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async {
                Err(LiveVenueReconciliationError {
                    evidence: Vec::new(),
                })
            })
        }

        fn read_balance_and_allowance<'a>(
            &'a self,
            _neg_risk: bool,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<LiveVenueAccountState, LiveVenueAccountReadError>>
                    + Send
                    + 'a,
            >,
        > {
            Box::pin(async move {
                self.account_state
                    .clone()
                    .ok_or_else(|| LiveVenueAccountReadError {
                        kind: LiveAccountReadFailure::Protocol,
                        evidence: Vec::new(),
                        request_descriptor_hashes: Vec::new(),
                    })
            })
        }
    }

    fn venue_account_state_from_audit(
        audit: &pe_execution_core::LiveAccountStateAudit,
    ) -> pe_execution_core::LiveVenueAccountState {
        pe_execution_core::LiveVenueAccountState {
            observed_at: audit.observed_at,
            closed_only: audit.closed_only,
            geoblocked: audit.geoblocked,
            selected_spender: audit.selected_spender.clone(),
            collateral_balance: audit.collateral_balance,
            allowance: audit.allowance,
            reconciled_free_collateral: audit.reconciled_free_collateral,
            schema_version: audit.schema_version,
            parser_version: audit.parser_version,
            evidence: audit.evidence.clone(),
            request_descriptor_hashes: audit.request_descriptor_hashes.clone(),
        }
    }

    fn db() -> (tempfile::TempDir, PaperStateDb) {
        let dir = tempdir().unwrap();
        let db = PaperStateDb::open(&dir.path().join("paper.db")).unwrap();
        (dir, db)
    }

    fn projection_signal() -> LeaderSignal {
        LeaderSignal {
            leader: TraderId(WalletAddress([0xaa; 20])),
            venue: VenueId::polymarket(),
            market_id: MarketId(VenueMarketId(
                "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f".to_owned(),
            )),
            outcome_id: OutcomeId(0),
            action: LeaderAction::Entry,
            leader_side: Side::Buy,
            leader_price: Price(dec!(0.50)),
            leader_size: pe_core_types::ShareAmount::from_whole(10).unwrap(),
            observed_at: OffsetDateTime::UNIX_EPOCH,
            received_at: OffsetDateTime::UNIX_EPOCH,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: SourceTradeId("source-projection".to_owned()),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        }
    }

    fn activity_payload_pair(
        economic: &EconomicPrepared,
        source_time: OffsetDateTime,
        wallet: WalletAddress,
        transaction_hash: &str,
    ) -> (Vec<u8>, Vec<u8>, SourceTradeId) {
        let common = serde_json::json!({
            "proxyWallet": wallet.to_string(),
            "timestamp": source_time.unix_timestamp(),
            "conditionId": economic.market.condition_id.0,
            "asset": economic.market.token_id.0,
            "side": "BUY",
            "size": "10.000000",
            "price": economic.balance.chase_ceiling.0.to_string(),
            "transactionHash": transaction_hash,
            "outcomeIndex": economic.market.outcome_index,
            "outcome": "Yes",
        });
        let websocket = serde_json::to_vec(&common).unwrap();
        let mut rest = common;
        let object = rest.as_object_mut().unwrap();
        object.insert("type".to_owned(), serde_json::json!("TRADE"));
        object.insert("usdcSize".to_owned(), serde_json::json!("8.000000"));
        object.insert("isCombo".to_owned(), serde_json::json!(false));
        let rest = serde_json::to_vec(&vec![rest]).unwrap();
        let source_trade_id = parse_activity_trade_observation(&websocket)
            .unwrap()
            .group_id
            .key()
            .clone();
        (websocket, rest, source_trade_id)
    }

    fn bind_activity_observation(
        prepared: &mut pe_execution_core::LiveOrderPreparedAudit,
        account_id: &AccountId,
        source_receipt: AppendReceipt,
        complete_bound_receipt: AppendReceipt,
        observed_at: OffsetDateTime,
        source_time: OffsetDateTime,
    ) -> (Vec<u8>, Vec<u8>) {
        bind_activity_observation_with_transaction(
            prepared,
            account_id,
            source_receipt,
            complete_bound_receipt,
            observed_at,
            source_time,
            &format!("0x{}", "55".repeat(32)),
        )
    }

    fn bind_activity_observation_with_transaction(
        prepared: &mut pe_execution_core::LiveOrderPreparedAudit,
        account_id: &AccountId,
        source_receipt: AppendReceipt,
        complete_bound_receipt: AppendReceipt,
        observed_at: OffsetDateTime,
        source_time: OffsetDateTime,
        transaction_hash: &str,
    ) -> (Vec<u8>, Vec<u8>) {
        let leader = WalletAddress([0xaa; 20]);
        // The activity payload carries the trade time in whole seconds; the recorded signal must
        // use the same instant so strict replay's reconstruction hashes to the recorded decision.
        let source_time =
            OffsetDateTime::from_unix_timestamp(source_time.unix_timestamp()).unwrap();
        let (websocket, rest, source_trade_id) =
            activity_payload_pair(&prepared.economic, source_time, leader, transaction_hash);
        let signal = LeaderSignal {
            leader: TraderId(leader),
            venue: VenueId::polymarket(),
            market_id: MarketId(VenueMarketId(prepared.economic.market.market_id.clone())),
            outcome_id: OutcomeId(u16::from(prepared.economic.market.outcome_index)),
            action: LeaderAction::Entry,
            leader_side: Side::Buy,
            leader_price: prepared.economic.balance.chase_ceiling,
            leader_size: ShareAmount::from_whole(10).unwrap(),
            observed_at: source_time,
            received_at: source_time,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: source_trade_id.clone(),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        };
        prepared.identity.fill_projection = Some(Box::new(LiveFillProjectionIdentity {
            leader_wallet: leader.to_string(),
            source_trade_id: Some(source_trade_id.0),
            market_id: prepared.economic.market.market_id.clone(),
            outcome_id: i64::from(prepared.economic.market.outcome_index),
            side: "buy".to_owned(),
        }));
        prepared.identity.decision_hash = hash_json(&(
            "prediction-edge/live-decision/v1",
            &signal,
            account_id.as_str(),
            &prepared.identity.config_hash,
            &prepared.identity.quote_id,
            &prepared.identity.evidence_hashes,
        ))
        .unwrap();
        prepared.economic.observation = Some(ObservationEvidence {
            source_receipt,
            complete_bound_receipt,
            observed_unix_ms: unix_millis_i64(observed_at),
            provenance: "activity_ws".to_owned(),
        });
        (websocket, rest)
    }

    fn activity_transaction_hash(identity: &LiveOrderIdentity) -> String {
        format!(
            "0x{}",
            blake3::hash(identity.dispatch_id.as_bytes()).to_hex()
        )
    }

    fn fixture_observation_evidence() -> ObservationEvidence {
        ObservationEvidence {
            source_receipt: fixture_receipt(5),
            complete_bound_receipt: fixture_receipt(6),
            observed_unix_ms: 0,
            provenance: "activity_ws".to_owned(),
        }
    }

    fn projection_identity(dispatch_id: &str) -> LiveOrderIdentity {
        let signal = projection_signal();
        let account_id = AccountId::new("account").unwrap();
        LiveOrderIdentity {
            dispatch_id: dispatch_id.to_owned(),
            idempotency_key: LiveOrderIdentity::idempotency_key_for(dispatch_id, &account_id),
            quote_id: "quote".to_owned(),
            config_hash: "config".to_owned(),
            decision_hash: "decision".to_owned(),
            evidence_hashes: vec!["evidence".to_owned()],
            fill_projection: Some(Box::new(LiveFillProjectionIdentity {
                leader_wallet: signal.leader.to_string(),
                source_trade_id: Some(signal.source_trade_id.0),
                market_id: signal.market_id.to_string(),
                outcome_id: i64::from(signal.outcome_id.0),
                side: "buy".to_owned(),
            })),
            schema_version: 1,
            parser_version: 1,
        }
    }

    fn matched_projection_event(
        account_id: &AccountId,
        dispatch_id: &str,
        seq: u64,
    ) -> LiveJournalEvent {
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap(),
            payload: LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                identity: projection_identity(dispatch_id),
                order_hash: "order-hash".to_owned(),
                source: LiveReconciliationSource::PostResponse,
                outcome: LiveJournalOrderOutcome::Matched {
                    venue_order_id: "venue-order".to_owned(),
                    transaction_hashes: Vec::new(),
                    executed: None,
                },
                evidence: Vec::new(),
                evidence_hashes: Vec::new(),
            })),
        }
    }

    #[test]
    fn real_shape_negative_risk_position_selects_negrisk_adapter() {
        let fixture = serde_json::json!({
            "proxyWallet": "0x1111111111111111111111111111111111111111",
            "asset": "123",
            "conditionId": "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "size": 10,
            "avgPrice": 0.42,
            "redeemable": true,
            "negativeRisk": true
        });
        let position: RedeemablePositionRow = serde_json::from_value(fixture).unwrap();
        let call = build_redemption_call(
            PolymarketConditionId(position.condition_id),
            position.neg_risk,
        )
        .unwrap();
        assert!(call.neg_risk);
        assert_eq!(call.to, NEGRISK_COLLATERAL_ADAPTER);

        let missing = serde_json::json!({
            "conditionId": "0x000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
            "size": 10
        });
        assert!(serde_json::from_value::<RedeemablePositionRow>(missing).is_err());
    }

    /// PASS: records preceding Baseline remain audit-only and create no managed state.
    #[test]
    fn pre_baseline_records_are_audit_only() {
        let account_id = AccountId::new("account").unwrap();
        let dispatch_id = "dispatch-projection";
        let events = vec![
            matched_projection_event(&account_id, dispatch_id, 7),
            matched_projection_event(&account_id, dispatch_id, 8),
        ];
        let rows = derive_projection_rows(&account_id, &events).unwrap();
        assert!(rows.fills.is_empty());
        assert!(rows.positions.is_empty());
        assert!(rows.economic_cash.is_none());
    }

    /// PASS: admission, preparation, POST, and reconciliation facts before the first Baseline are
    /// audit-only in both strict projection reduction and execution-core recovery inventory.
    /// FAIL: either consumer reports recovery work or managed state from a pre-Baseline fact, or
    /// the two consumers disagree.
    #[test]
    fn recovery_and_reducer_share_the_pre_baseline_slice() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let at = OffsetDateTime::UNIX_EPOCH;
        let events = vec![
            approved_admission_event(&account_id, &prepared, 1, at),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 2,
                timestamp: at,
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 3,
                timestamp: at,
                payload: LiveJournalPayload::OrderPosted(Box::new(
                    pe_execution_core::LiveOrderPostAudit {
                        identity: prepared.identity.clone(),
                        order_hash: prepared.prepared.order_hash.clone(),
                        evidence: RawHttpAttempt::TransportFailure(
                            pe_core_types::RawTransportFailure {
                                source_id: "fixture".to_owned(),
                                endpoint_kind: "post".to_owned(),
                                method: "POST".to_owned(),
                                path: "/order".to_owned(),
                                ordered_query: Vec::new(),
                                attempt_ordinal: 1,
                                observed_at: at,
                                received_at: at,
                                error_class: pe_core_types::TransportErrorClass::Other,
                                schema_version: 1,
                                parser_version: 1,
                                adapter_version: "fixture".to_owned(),
                            },
                        ),
                        evidence_hash: "ignored-before-baseline".to_owned(),
                    },
                )),
            },
            matched_projection_event(&account_id, "dispatch-finality", 4),
            baseline_event(&account_id, 5),
        ];
        let derived = derive_with_baseline_evidence(&account_id, &events, &[]).unwrap();
        assert_eq!(derived.baseline_sequence, Some(5));
        assert!(derived.fills.is_empty());
        assert!(derived.positions.is_empty());
        assert_eq!(derived.reserved, Decimal::ZERO);

        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        for event in events {
            journal
                .append(event.account_id, event.timestamp, event.payload)
                .unwrap();
        }
        drop(journal);
        let recovery = recovery_inventory(&path, None).unwrap();
        assert!(recovery.approved_admissions.is_empty());
        assert!(recovery.open_orders.is_empty());
        assert!(recovery.terminal_admissions.is_empty());
    }

    fn fixture_receipt(sequence: u64) -> AppendReceipt {
        AppendReceipt {
            sequence: EventSeq(sequence),
            this_hash: blake3::Hash::from_bytes([u8::try_from(sequence).unwrap(); 32]),
        }
    }

    fn account_state_fixture(
        cash: CollateralAmount,
        observed_at: OffsetDateTime,
    ) -> pe_execution_core::LiveAccountStateAudit {
        let account_id = AccountId::new("account").unwrap();
        let binding =
            account_binding_fixture(&account_id, "0x1111111111111111111111111111111111111111");
        account_state_fixture_for_binding(cash, observed_at, &binding)
    }

    fn account_state_fixture_for_binding(
        cash: CollateralAmount,
        observed_at: OffsetDateTime,
        binding: &LiveAccountBindingAudit,
    ) -> pe_execution_core::LiveAccountStateAudit {
        let spender = pe_venue_polymarket::CanaryV2Client::standard_spender().unwrap();
        let negrisk_spender = pe_venue_polymarket::CanaryV2Client::negrisk_spender().unwrap();
        let response = |endpoint_kind: &str,
                        path: &str,
                        ordered_query: Vec<(String, String)>,
                        body: Vec<u8>| {
            RawHttpAttempt::Response(pe_core_types::RawHttpResponse {
                source_id: "polymarket-clob-v2".to_owned(),
                endpoint_kind: endpoint_kind.to_owned(),
                method: "GET".to_owned(),
                path: path.to_owned(),
                ordered_query,
                status: 200,
                headers: Vec::new(),
                body,
                attempt_ordinal: 1,
                source_at: None,
                observed_at,
                received_at: observed_at,
                schema_version: 1,
                parser_version: 1,
                adapter_version: pe_venue_polymarket::SDK_VERSION.to_owned(),
            })
        };
        let evidence = vec![
            response(
                "geoblock",
                "/api/geoblock",
                Vec::new(),
                br#"{"blocked":false,"country":"US"}"#.to_vec(),
            ),
            response(
                "closed-only",
                "/auth/ban-status/closed-only",
                Vec::new(),
                br#"{"closed_only":false}"#.to_vec(),
            ),
            response(
                "balance-allowance",
                "/balance-allowance",
                vec![
                    ("asset_type".to_owned(), "COLLATERAL".to_owned()),
                    ("signature_type".to_owned(), "3".to_owned()),
                ],
                serde_json::to_vec(&serde_json::json!({
                    "balance": cash.atomic().to_string(),
                    "allowances": {
                        spender.clone(): cash.atomic().to_string(),
                        negrisk_spender: cash.atomic().to_string(),
                    },
                }))
                .unwrap(),
            ),
        ];
        let request_descriptor_hashes = account_request_descriptor_hashes(&evidence, binding);
        pe_execution_core::LiveAccountStateAudit {
            observed_at,
            closed_only: false,
            geoblocked: false,
            selected_spender: spender,
            collateral_balance: cash,
            allowance: cash,
            reconciled_free_collateral: cash,
            schema_version: 1,
            parser_version: 1,
            request_descriptor_hashes,
            evidence_hashes: http_attempt_hashes(&evidence).unwrap(),
            evidence,
        }
    }

    fn account_request_descriptor_hashes(
        evidence: &[RawHttpAttempt],
        binding: &LiveAccountBindingAudit,
    ) -> Vec<String> {
        evidence
            .iter()
            .map(|attempt| {
                pe_execution_core::live_journal::request_descriptor_hash(
                    &binding.request_descriptor_for_attempt(attempt),
                )
                .unwrap()
            })
            .collect()
    }

    /// PASS: every runtime-success classification produces an audit accepted by the reducer for
    /// every response ordering; an absent selected allowance is canonical zero, while a malformed
    /// selected allowance or absent allowance map fails before an audit can be appended.
    #[test]
    fn runtime_account_success_always_round_trips_through_reducer() {
        #[derive(Clone, Copy)]
        enum AllowanceShape {
            Valid,
            SelectedMissing,
            SelectedMalformed,
            MapMissing,
        }

        let observed_at = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
        let spender = pe_venue_polymarket::CanaryV2Client::standard_spender().unwrap();
        let response_orders = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let cases = [
            (
                AllowanceShape::Valid,
                Some(CollateralAmount::from_atomic(500_000)),
            ),
            (
                AllowanceShape::SelectedMissing,
                Some(CollateralAmount::ZERO),
            ),
            (AllowanceShape::SelectedMalformed, None),
            (AllowanceShape::MapMissing, None),
        ];

        for (shape, expected_allowance) in cases {
            for order in response_orders {
                let mut evidence =
                    account_state_fixture(CollateralAmount::from_atomic(1_000_000), observed_at)
                        .evidence;
                let balance = evidence
                    .iter_mut()
                    .find_map(|attempt| match attempt {
                        RawHttpAttempt::Response(response)
                            if response.endpoint_kind == "balance-allowance" =>
                        {
                            Some(response)
                        }
                        RawHttpAttempt::Response(_) | RawHttpAttempt::TransportFailure(_) => None,
                    })
                    .unwrap();
                let body = match shape {
                    AllowanceShape::Valid => serde_json::json!({
                        "balance": "1000000",
                        "allowances": { spender.clone(): "500000" },
                    }),
                    AllowanceShape::SelectedMissing => serde_json::json!({
                        "balance": "1000000",
                        "allowances": {},
                    }),
                    AllowanceShape::SelectedMalformed => serde_json::json!({
                        "balance": "1000000",
                        "allowances": { spender.clone(): "not-an-integer" },
                    }),
                    AllowanceShape::MapMissing => serde_json::json!({
                        "balance": "1000000",
                    }),
                };
                balance.body = serde_json::to_vec(&body).unwrap();
                let ordered_evidence = order
                    .into_iter()
                    .map(|index| evidence[index].clone())
                    .collect::<Vec<_>>();
                let account_id = AccountId::new("account").unwrap();
                let binding = account_binding_fixture(
                    &account_id,
                    "0x1111111111111111111111111111111111111111",
                );
                let request_descriptor_hashes =
                    account_request_descriptor_hashes(&ordered_evidence, &binding);
                let classified = classify_live_account_responses(
                    &ordered_evidence,
                    &spender,
                    &request_descriptor_hashes,
                    None,
                    &binding,
                )
                .unwrap();

                if let Some(expected_allowance) = expected_allowance {
                    assert!(matches!(
                        classified,
                        LiveAccountResponseClassification::State(_)
                    ));
                    let LiveAccountResponseClassification::State(state) = classified else {
                        return;
                    };
                    assert_eq!(state.allowance, expected_allowance);
                    let audit = state.audit().unwrap();
                    verify_account_state(&audit, &binding).unwrap();
                } else {
                    assert!(matches!(
                        classified,
                        LiveAccountResponseClassification::ReadFailure(
                            pe_execution_core::LiveVenueAccountReadError {
                                kind: pe_execution_core::LiveAccountReadFailure::Protocol,
                                ..
                            }
                        )
                    ));
                }
            }
        }
    }

    fn source_envelope(
        receipt: AppendReceipt,
        source_id: &str,
        payload: Vec<u8>,
        received_at: OffsetDateTime,
    ) -> EventEnvelope {
        EventEnvelope {
            seq: receipt.sequence,
            source_id: SourceId(source_id.to_owned()),
            schema_version: 1,
            parser_version: 1,
            observed_at: SourceTimestamp(received_at),
            received_at: ReceivedAt(received_at),
            content_type: ContentType::Json,
            raw_payload_hash: blake3::hash(&payload),
            prev_hash: blake3::Hash::from_bytes([0; 32]),
            this_hash: receipt.this_hash,
            payload,
        }
    }

    fn account_binding_fixture(
        account_id: &AccountId,
        custody_wallet: &str,
    ) -> LiveAccountBindingAudit {
        LiveAccountBindingAudit::new(
            account_id.clone(),
            CredentialBindingIdentity {
                version: 1,
                key_id: "key".to_owned(),
            },
            WalletAddress::from_hex(custody_wallet).unwrap(),
            blake3::hash(account_id.as_str().as_bytes())
                .to_hex()
                .to_string(),
        )
    }

    fn empty_position_evidence(
        first_sequence: u64,
        received_at: OffsetDateTime,
        binding: &LiveAccountBindingAudit,
    ) -> (LivePositionEvidenceAudit, Vec<EventEnvelope>) {
        position_evidence(
            first_sequence,
            received_at,
            binding,
            [b"[]".to_vec(), b"[]".to_vec()],
        )
    }

    fn position_evidence(
        first_sequence: u64,
        received_at: OffsetDateTime,
        binding: &LiveAccountBindingAudit,
        partition_bodies: [Vec<u8>; 2],
    ) -> (LivePositionEvidenceAudit, Vec<EventEnvelope>) {
        let requested_wallet = binding.custody_wallet.clone();
        let receipts = vec![
            fixture_receipt(first_sequence),
            fixture_receipt(first_sequence.checked_add(1).unwrap()),
        ];
        let pages = [false, true]
            .into_iter()
            .zip(receipts)
            .map(|(redeemable, receipt)| LivePositionPageAudit {
                request_identity: format!(
                    "/positions?user={requested_wallet}&sizeThreshold=0&includeArchived=true&limit=500&sortBy=TOKENS&sortDirection=ASC&redeemable={redeemable}&offset=0"
                ),
                receipt,
            })
            .collect::<Vec<_>>();
        let sources = pages
            .iter()
            .zip(partition_bodies)
            .map(|(page, body)| {
                let request = position_request_descriptor(binding, &page.request_identity).unwrap();
                let payload =
                    serde_json::to_vec(&RetainedPositionResponse { request, body }).unwrap();
                let mut envelope = source_envelope(
                    page.receipt,
                    COMPLETE_POSITIONS_SOURCE_ID,
                    payload,
                    received_at,
                );
                envelope.schema_version = COMPLETE_POSITIONS_SCHEMA_VERSION;
                envelope
            })
            .collect();
        (
            LivePositionEvidenceAudit {
                requested_wallet,
                pages,
            },
            sources,
        )
    }

    fn baseline_sources(account_id: &AccountId) -> Vec<EventEnvelope> {
        let binding =
            account_binding_fixture(account_id, "0x1111111111111111111111111111111111111111");
        empty_position_evidence(100, OffsetDateTime::UNIX_EPOCH, &binding).1
    }

    fn derive_with_baseline_evidence(
        account_id: &AccountId,
        events: &[LiveJournalEvent],
        additional_sources: &[EventEnvelope],
    ) -> Result<ProjectionDerivation, ProjectionReducerError> {
        let mut sources = baseline_sources(account_id);
        sources.extend_from_slice(additional_sources);
        derive_projection_rows_with_sources(account_id, events, &sources)
    }

    fn baseline_event(account_id: &AccountId, seq: u64) -> LiveJournalEvent {
        let cash = CollateralAmount::from_atomic(10_000_000);
        let account_binding =
            account_binding_fixture(account_id, "0x1111111111111111111111111111111111111111");
        let (venue_position_evidence, _) =
            empty_position_evidence(100, OffsetDateTime::UNIX_EPOCH, &account_binding);
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: OffsetDateTime::UNIX_EPOCH,
            payload: LiveJournalPayload::AccountPortfolioMarked(Box::new(
                pe_execution_core::AccountPortfolioMarkedAudit {
                    kind: MarkKind::Baseline,
                    cutoff_unix: 0,
                    account_binding: account_binding.clone(),
                    account_state: account_state_fixture_for_binding(
                        cash,
                        OffsetDateTime::UNIX_EPOCH,
                        &account_binding,
                    ),
                    venue_positions: Vec::new(),
                    venue_position_evidence,
                    marked_positions: Vec::new(),
                    prices: Vec::new(),
                    equity: cash,
                },
            )),
        }
    }

    fn rotated_account_binding(account_id: &AccountId) -> LiveAccountBindingAudit {
        LiveAccountBindingAudit::new(
            account_id.clone(),
            CredentialBindingIdentity {
                version: 2,
                key_id: "key-2".to_owned(),
            },
            WalletAddress::from_hex("0x1111111111111111111111111111111111111111").unwrap(),
            blake3::hash(b"rotated-secret-material")
                .to_hex()
                .to_string(),
        )
    }

    /// PASS: a target frozen to a newly rotated credential cannot pass the existing financial-era
    /// Baseline; arming must wait for the next canonical Baseline transition.
    #[test]
    fn new_version_target_requires_a_new_baseline() {
        let account_id = AccountId::new("credential-rotation").unwrap();
        let derived =
            derive_with_baseline_evidence(&account_id, &[baseline_event(&account_id, 0)], &[])
                .unwrap();
        assert!(!baseline_owns_current_binding(
            &derived,
            &rotated_account_binding(&account_id)
        ));
    }

    /// PASS: a same-day credential rotation cannot replace the Baseline's full binding, including
    /// its secret-derived fingerprint, without a new financial era.
    #[test]
    fn same_day_credential_rotation_keeps_the_baseline_binding() {
        let account_id = AccountId::new("same-day-rotation").unwrap();
        let derived =
            derive_with_baseline_evidence(&account_id, &[baseline_event(&account_id, 0)], &[])
                .unwrap();
        let mut same_version_new_secret =
            account_binding_fixture(&account_id, "0x1111111111111111111111111111111111111111");
        same_version_new_secret.credential_fingerprint =
            blake3::hash(b"new-secret").to_hex().to_string();
        assert!(!baseline_owns_current_binding(
            &derived,
            &same_version_new_secret
        ));
    }

    /// PASS: a Daily mark preserves rather than transitions the Baseline binding, so a rotated
    /// target remains blocked across the UTC-day boundary.
    #[test]
    fn daily_boundary_does_not_transition_the_baseline_binding() {
        let account_id = AccountId::new("daily-rotation").unwrap();
        let mut daily = baseline_event(&account_id, 1);
        daily.timestamp = OffsetDateTime::from_unix_timestamp(86_401).unwrap();
        let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut daily.payload else {
            return;
        };
        mark.kind = MarkKind::Daily;
        mark.cutoff_unix = 86_400;
        let derived = derive_with_baseline_evidence(
            &account_id,
            &[baseline_event(&account_id, 0), daily],
            &[],
        )
        .unwrap();
        assert!(!baseline_owns_current_binding(
            &derived,
            &rotated_account_binding(&account_id)
        ));
    }

    fn economic_source_payloads_with_end(
        condition_id: &str,
        scheduled_end_unix: Option<i64>,
    ) -> [Vec<u8>; 4] {
        let gamma = serde_json::to_vec(&serde_json::json!([{
            "conditionId": condition_id,
            "active": true,
            "closed": false,
            "acceptingOrders": true,
            "enableOrderBook": true,
            "negRisk": false,
            "outcomes": "[\"Yes\",\"No\"]",
            "clobTokenIds": "[\"123\",\"124\"]",
            "orderPriceMinTickSize": "0.01",
            "orderMinSize": "1",
            "secondsDelay": 0
        }]))
        .unwrap();
        let mut clob_long = serde_json::json!({
            "condition_id": condition_id,
            "active": true,
            "closed": false,
            "accepting_orders": true,
            "enable_order_book": true,
            "minimum_order_size": "1",
            "minimum_tick_size": "0.01",
            "neg_risk": false,
            "seconds_delay": 0,
            "tokens": [
                {"token_id": "123", "outcome": "Yes"},
                {"token_id": "124", "outcome": "No"}
            ]
        });
        if let Some(scheduled_end_unix) = scheduled_end_unix {
            clob_long["end_date_iso"] = serde_json::Value::String(
                OffsetDateTime::from_unix_timestamp(scheduled_end_unix)
                    .unwrap()
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap(),
            );
        }
        let clob_long = serde_json::to_vec(&clob_long).unwrap();
        let clob_compact = format!(
            r#"{{"c":"{condition_id}","t":[{{"t":"123","o":"Yes"}},{{"t":"124","o":"No"}}],"mts":0.01,"mos":1,"nr":false,"fd":{{"r":0.00024,"e":1,"to":true}}}}"#
        )
        .into_bytes();
        let book = format!(
            r#"{{"market":"{condition_id}","asset_id":"123","asks":[{{"price":"0.80","size":"3.125"}}]}}"#
        )
        .into_bytes();
        [gamma, clob_long, clob_compact, book]
    }

    fn economic_source_payloads(condition_id: &str) -> [Vec<u8>; 4] {
        economic_source_payloads_with_end(condition_id, None)
    }

    fn economic_source_envelopes(
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        received_at: OffsetDateTime,
    ) -> Vec<EventEnvelope> {
        let economic = &prepared.economic;
        let payloads = economic_source_payloads_with_end(
            &economic.market.condition_id.0,
            economic.admission.scheduled_end_unix,
        );
        let receipts = [
            economic.admission.receipts.gamma,
            economic.admission.receipts.clob_long,
            economic.admission.receipts.clob_compact,
            economic.book_receipt,
        ];
        let source_ids = [
            GAMMA_MARKETS_SOURCE_ID,
            CLOB_LONG_MARKET_SOURCE_ID,
            CLOB_COMPACT_MARKET_SOURCE_ID,
            CLOB_BOOK_SOURCE_ID,
        ];
        let mut sources = receipts
            .into_iter()
            .zip(source_ids)
            .zip(payloads)
            .map(|((receipt, source_id), payload)| {
                source_envelope(receipt, source_id, payload, received_at)
            })
            .collect::<Vec<_>>();
        let observation = economic.observation.as_ref().unwrap();
        let (websocket, rest, _) = activity_payload_pair(
            economic,
            received_at,
            WalletAddress([0xaa; 20]),
            &format!("0x{}", "55".repeat(32)),
        );
        for (receipt, source_id, payload) in [
            (
                observation.source_receipt,
                crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
                websocket,
            ),
            (
                observation.complete_bound_receipt,
                crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
                rest,
            ),
        ] {
            let mut envelope = source_envelope(receipt, source_id, payload, received_at);
            envelope.schema_version = pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION;
            envelope.parser_version = pe_source_polymarket_public::ACTIVITY_PARSER_VERSION;
            sources.push(envelope);
        }
        sources
    }

    async fn append_economic_sources(
        state: &FanoutState,
        prepared: &mut pe_execution_core::LiveOrderPreparedAudit,
        account_id: &AccountId,
        received_at: OffsetDateTime,
    ) {
        let transaction_hash = activity_transaction_hash(&prepared.identity);
        let economic = &mut prepared.economic;
        let payloads = economic_source_payloads_with_end(
            &economic.market.condition_id.0,
            economic.admission.scheduled_end_unix,
        );
        let source_ids = [
            GAMMA_MARKETS_SOURCE_ID,
            CLOB_LONG_MARKET_SOURCE_ID,
            CLOB_COMPACT_MARKET_SOURCE_ID,
            CLOB_BOOK_SOURCE_ID,
        ];
        let mut receipts = Vec::new();
        for (source_id, payload) in source_ids.into_iter().zip(payloads.iter()) {
            receipts.push(
                state
                    .config
                    .source_log
                    .append(EnvelopeIn {
                        source_id: SourceId(source_id.to_owned()),
                        schema_version: 1,
                        parser_version: 1,
                        observed_at: SourceTimestamp(received_at),
                        received_at: ReceivedAt(received_at),
                        content_type: ContentType::Json,
                        payload: payload.clone(),
                    })
                    .await
                    .unwrap(),
            );
        }
        economic.admission.receipts = AdmissionReceipts {
            gamma: receipts[0],
            clob_long: receipts[1],
            clob_compact: receipts[2],
        };
        economic.book_receipt = receipts[3];
        economic.admission.market.observed_at_unix = received_at.unix_timestamp();
        economic.admission.settlement.observed_at_unix = received_at.unix_timestamp();
        economic.admission.settlement.raw_evidence_hash =
            blake3::hash(&payloads[1]).to_hex().to_string();
        let (websocket, rest, _) = activity_payload_pair(
            economic,
            received_at,
            WalletAddress([0xaa; 20]),
            &transaction_hash,
        );
        let mut observation_receipts = Vec::new();
        for (source_id, payload) in [
            (crate::activity_ingest::ACTIVITY_WS_SOURCE_ID, websocket),
            (crate::trade_poller::ACTIVITY_POLL_SOURCE_ID, rest),
        ] {
            observation_receipts.push(
                state
                    .config
                    .source_log
                    .append(EnvelopeIn {
                        source_id: SourceId(source_id.to_owned()),
                        schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                        parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                        observed_at: SourceTimestamp(received_at),
                        received_at: ReceivedAt(received_at),
                        content_type: ContentType::Json,
                        payload,
                    })
                    .await
                    .unwrap(),
            );
        }
        let _ = bind_activity_observation_with_transaction(
            prepared,
            account_id,
            observation_receipts[0],
            observation_receipts[1],
            received_at,
            received_at,
            &transaction_hash,
        );
        record_observation_continuation(state, prepared, received_at);
    }

    fn record_observation_continuation(
        state: &FanoutState,
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        source_time: OffsetDateTime,
    ) {
        let observation = prepared.economic.observation.as_ref().unwrap();
        let complete_bound = state
            .config
            .source_receipts
            .source_envelope(observation.complete_bound_receipt)
            .unwrap();
        let continuation = observation_continuation(
            prepared,
            &[complete_bound.payload.as_slice()],
            vec![activity_page_fixture(
                observation.complete_bound_receipt,
                &complete_bound.payload,
                Some(source_time.unix_timestamp().saturating_sub(1)),
                source_time.unix_timestamp(),
                0,
                complete_bound.received_at.0,
            )],
        );
        let continuation = if complete_bound.schema_version
            == pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION
        {
            DecisionContinuationV3::new(
                continuation.facts,
                continuation.observed_source_receipt,
                continuation.page_occurrences,
                None,
            )
        } else {
            continuation
        };
        let frozen_inputs_json = serde_json::to_string(&continuation).unwrap();
        state
            .config
            .paper_state
            .commit_activity_bucket(&pe_paper_state::ActivityBucketCommit {
                wallet: continuation.facts.wallet,
                source_epoch: continuation.facts.source_epoch,
                dispositions: vec![pe_paper_state::ActivityDispositionRecord {
                    source_trade_id: continuation.facts.source_trade_id.clone(),
                    transaction_hash: continuation.facts.transaction_hash.clone(),
                    wallet: continuation.facts.wallet,
                    source_epoch: continuation.facts.source_epoch,
                    semantic_revision: continuation.facts.semantic_revision.clone(),
                    activity_type: "trade".to_owned(),
                    disposition: "decision_pending".to_owned(),
                    proof_json: pe_position_ledger::AppliedEffect {
                        effect: pe_position_ledger::LedgerEffect::Trade {
                            market_id: continuation.facts.market_id.clone(),
                            outcome_id: continuation.facts.outcome_id,
                            side: continuation.facts.side,
                            amount: continuation.facts.share_amount,
                            price: continuation.facts.price,
                        },
                        clamped_residual: None,
                    }
                    .to_document()
                    .unwrap(),
                    no_copy: None,
                }],
                leader_positions: Vec::new(),
                gate_results: Vec::new(),
                history_effects: Vec::new(),
                history_status: None,
                pending: vec![pe_paper_state::DecisionPendingRecord {
                    source_trade_id: continuation.facts.source_trade_id.clone(),
                    semantic_revision: continuation.facts.semantic_revision.clone(),
                    wallet: continuation.facts.wallet,
                    source_epoch: continuation.facts.source_epoch,
                    frozen_inputs_json,
                    updated_at_unix: source_time.unix_timestamp(),
                }],
                fence: None,
                reanchor: None,
                advance_cursor: false,
            })
            .unwrap();
    }

    fn finality_prepared() -> Box<pe_execution_core::LiveOrderPreparedAudit> {
        let identity = projection_identity("dispatch-finality");
        let condition_id = PolymarketConditionId(format!("0x{}", "77".repeat(32)));
        let token_id = pe_core_types::PolymarketTokenId("123".to_owned());
        let gamma_receipt = fixture_receipt(1);
        let clob_long_receipt = fixture_receipt(2);
        let clob_compact_receipt = fixture_receipt(3);
        let book_receipt = fixture_receipt(4);
        let clob_long_payload = economic_source_payloads(&condition_id.0)[1].clone();
        let market = pe_execution_core::LiveMarketEvidenceAudit {
            condition_id: condition_id.clone(),
            ordered_outcome_token_ids: [
                token_id.clone(),
                pe_core_types::PolymarketTokenId("124".to_owned()),
            ],
            neg_risk: false,
            minimum_tick_size: Price(dec!(0.01)),
            minimum_order_size: ShareAmount::from_atomic(1_000_000),
            observed_at_unix: 0,
            schema_version: 1,
            parser_version: 1,
            freshness_window_secs: 60,
        };
        let admission = LiveAdmissionArtifactAudit {
            market,
            settlement: pe_resolver_card::VenueSettlementRecord {
                schema_version: pe_resolver_card::VENUE_SETTLEMENT_SCHEMA_VERSION,
                condition_id: condition_id.clone(),
                status: pe_resolver_card::VenueResolutionStatus::Unresolved,
                raw_evidence_hash: blake3::hash(&clob_long_payload).to_hex().to_string(),
                source_timestamp_unix: None,
                observed_at_unix: 0,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            fee_schedule: pe_venue_polymarket::CompactFeeSchedule::Taker {
                rate: dec!(0.00024),
            },
            scheduled_end_unix: None,
            receipts: pe_execution_core::AdmissionReceipts {
                gamma: gamma_receipt,
                clob_long: clob_long_receipt,
                clob_compact: clob_compact_receipt,
            },
        };
        let ladder = LadderPlanAudit {
            used_asks: vec![pe_execution_core::LadderAskAudit {
                price: Price(dec!(0.80)),
                shares: ShareAmount::from_atomic(3_125_000),
            }],
            best_ask: Price(dec!(0.80)),
            limit_price: Price(dec!(0.80)),
            minimum_shares: ShareAmount::from_atomic(3_125_000),
            principal: CollateralAmount::from_atomic(2_500_000),
        };
        let cash = CollateralAmount::from_atomic(10_000_000);
        let economic = EconomicPrepared {
            version: ECONOMIC_PREPARED_VERSION,
            market: MarketSelection {
                condition_id: condition_id.clone(),
                outcome_index: 0,
                token_id: token_id.clone(),
                side: Side::Buy,
                market_id: condition_id.0.clone(),
            },
            admission,
            ladder,
            book_receipt,
            observation: None,
            sizing: SizingAudit {
                mode: SizingModeAudit::Dollar {
                    usd: dec!(2.500121),
                },
                budget: CollateralAmount::from_atomic(2_500_121),
                principal: CollateralAmount::from_atomic(2_500_000),
                minimum_shares: ShareAmount::from_atomic(3_125_000),
                expected_shares: ShareAmount::from_atomic(3_125_000),
                expected_vwap: Price(dec!(0.8)),
                all_in_price: Price(dec!(0.8000384)),
                slippage_rate: Decimal::ZERO,
            },
            fee: FeeAudit {
                schedule: pe_venue_polymarket::CompactFeeSchedule::Taker {
                    rate: dec!(0.00024),
                },
                expected_fee: CollateralAmount::from_atomic(120),
                reserve: CollateralAmount::from_atomic(120),
            },
            risk: RiskAudit {
                financial_prefix: gamma_receipt,
                snapshot: RiskSnapshot {
                    leader_exposure_bps: BasisPoints(0),
                    market_exposure_bps: BasisPoints(0),
                    family_exposure_bps: BasisPoints(0),
                    total_copy_exposure_bps: BasisPoints(0),
                    intraday_pnl_bps: BasisPoints(0),
                    rolling_7d_pnl_bps: BasisPoints(0),
                    absolute_pnl_bps: BasisPoints(0),
                    copy_latency_kill_switch_active: false,
                    proposed_trade_bps: BasisPoints(0),
                    per_trade_cap_bps: 25,
                    concentration_caps: None,
                },
                decision: RiskDecisionAudit::Approved,
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: 0,
            },
            balance: BalanceAudit {
                cash_before: cash,
                worst_case_debit: CollateralAmount::from_atomic(2_500_120),
                price_impact_cap_bps: 100,
                chase_ceiling: Price(dec!(0.80)),
                band_floor: Price(dec!(0.01)),
                band_ceiling_exclusive: Price(dec!(0.99)),
            },
            applied_configuration_hash: identity.config_hash.clone(),
        };
        Box::new(pe_execution_core::LiveOrderPreparedAudit {
            identity,
            frozen_binding: CredentialBindingIdentity {
                version: 1,
                key_id: "key".to_owned(),
            },
            economic,
            account_state: account_state_fixture(cash, OffsetDateTime::UNIX_EPOCH),
            prepared: pe_venue_polymarket::PreparedPolymarketBuy {
                condition_id,
                outcome_id: OutcomeId(0),
                token_id,
                maker: "0x1111111111111111111111111111111111111111".to_owned(),
                signer: "0x1111111111111111111111111111111111111111".to_owned(),
                funder: "0x1111111111111111111111111111111111111111".to_owned(),
                verifying_contract: pe_venue_polymarket::CTF_EXCHANGE_V2.to_string(),
                spender: pe_venue_polymarket::CTF_EXCHANGE_V2.to_string(),
                exchange_domain_version: 2,
                neg_risk: false,
                side: "BUY".to_owned(),
                salt: "1".to_owned(),
                timestamp_ms: 1,
                expiration: "0".to_owned(),
                maker_collateral: CollateralAmount::from_atomic(2_500_000),
                taker_shares: ShareAmount::from_atomic(3_125_000),
                limit_price: Price(dec!(0.80)),
                minimum_tick_size: Price(dec!(0.01)),
                signature_type: 3,
                order_type: "FOK".to_owned(),
                post_only: false,
                defer_exec: false,
                metadata: format!("0x{}", "00".repeat(32)),
                builder: format!("0x{}", "00".repeat(32)),
                order_hash: format!("0x{}", "22".repeat(32)),
                post_body_hash: "post".to_owned(),
                sdk_version: "test".to_owned(),
                sdk_archive_sha256: "test".to_owned(),
                metadata_hashes: Vec::new(),
                worst_case_debit: CollateralAmount::from_atomic(2_500_120),
            },
        })
    }

    fn approved_admission(
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
    ) -> Box<pe_execution_core::LiveAdmissionEvaluationAudit> {
        Box::new(pe_execution_core::LiveAdmissionEvaluationAudit {
            identity: prepared.identity.clone(),
            frozen_binding: prepared.frozen_binding.clone(),
            current_binding: prepared.frozen_binding.clone(),
            requested_mode: LiveControlMode::LiveTiny,
            effective_mode: LiveControlMode::LiveTiny,
            economic: prepared.economic.clone(),
            account_state: Some(prepared.account_state.clone()),
            account_read_failure_evidence: Vec::new(),
            account_read_failure_request_descriptor_hashes: Vec::new(),
            account_read_failure_evidence_hashes: Vec::new(),
            verdict: pe_execution_core::LiveAdmissionVerdict::Approved,
        })
    }

    fn approved_admission_event(
        account_id: &AccountId,
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        seq: u64,
        timestamp: OffsetDateTime,
    ) -> LiveJournalEvent {
        let mut admission = approved_admission(prepared);
        admission.identity.idempotency_key =
            LiveOrderIdentity::idempotency_key_for(&admission.identity.dispatch_id, account_id);
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp,
            payload: LiveJournalPayload::AdmissionEvaluated(admission),
        }
    }

    fn append_approved_admission(
        journal: &LiveJournal,
        account_id: &AccountId,
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        timestamp: OffsetDateTime,
    ) {
        let mut admission = approved_admission(prepared);
        admission.identity.idempotency_key =
            LiveOrderIdentity::idempotency_key_for(&admission.identity.dispatch_id, account_id);
        journal
            .append(
                account_id.clone(),
                timestamp,
                LiveJournalPayload::AdmissionEvaluated(admission),
            )
            .unwrap();
    }

    async fn append_recovery_baseline(
        state: &FanoutState,
        account_id: &AccountId,
        now: OffsetDateTime,
        equity: CollateralAmount,
    ) -> pe_execution_core::LiveAccountStateAudit {
        let cash = CollateralAmount::from_atomic(10_000_000);
        let binding =
            account_binding_fixture(account_id, "0x1111111111111111111111111111111111111111");
        let (mut position_evidence, source_envelopes) = empty_position_evidence(0, now, &binding);
        for (page, envelope) in position_evidence.pages.iter_mut().zip(source_envelopes) {
            page.receipt = state
                .config
                .source_log
                .append(EnvelopeIn {
                    source_id: envelope.source_id,
                    schema_version: envelope.schema_version,
                    parser_version: envelope.parser_version,
                    observed_at: envelope.observed_at,
                    received_at: envelope.received_at,
                    content_type: envelope.content_type,
                    payload: envelope.payload,
                })
                .await
                .unwrap();
        }
        let account_state = account_state_fixture_for_binding(cash, now, &binding);
        state
            .config
            .journal
            .append(
                account_id.clone(),
                now,
                LiveJournalPayload::AccountPortfolioMarked(Box::new(
                    pe_execution_core::AccountPortfolioMarkedAudit {
                        kind: MarkKind::Baseline,
                        cutoff_unix: now.unix_timestamp(),
                        account_binding: binding,
                        account_state: account_state.clone(),
                        venue_positions: Vec::new(),
                        venue_position_evidence: position_evidence,
                        marked_positions: Vec::new(),
                        prices: Vec::new(),
                        equity,
                    },
                )),
            )
            .unwrap();
        account_state
    }

    async fn append_recovery_filled_position(
        state: &FanoutState,
        account_id: &AccountId,
        account_state: &pe_execution_core::LiveAccountStateAudit,
        now: OffsetDateTime,
    ) -> (Vec<LiveJournalEvent>, ProjectionDerivation) {
        let mut prior = finality_prepared();
        prior.identity = projection_identity("recovery-risk-prior");
        prior.identity.idempotency_key =
            LiveOrderIdentity::idempotency_key_for(&prior.identity.dispatch_id, account_id);
        prior.account_state = account_state.clone();
        prior.economic.admission.market.observed_at_unix = now.unix_timestamp();
        prior.economic.admission.settlement.observed_at_unix = now.unix_timestamp();
        prior.economic.admission.scheduled_end_unix = now.unix_timestamp().checked_add(300);
        bind_empty_live_risk_to_paper_prefix(state, &mut prior, now);
        append_economic_sources(state, &mut prior, account_id, now).await;
        append_approved_admission(state.config.journal.as_ref(), account_id, &prior, now);
        let prepared = state
            .config
            .journal
            .append(
                account_id.clone(),
                now,
                LiveJournalPayload::OrderPrepared(prior.clone()),
            )
            .unwrap();
        let matched = finality_matched_event(account_id, &prior, 0, now);
        state
            .config
            .journal
            .append(account_id.clone(), now, matched.payload)
            .unwrap();
        let fill = finalized_fill_event(account_id, &prior, 0, prepared.seq);
        state
            .config
            .journal
            .append(account_id.clone(), now, fill.payload)
            .unwrap();

        let events = replay_live_account(state, account_id).unwrap();
        let sources =
            source_envelopes_for_live_events(&state.config.source_receipts, &events).unwrap();
        let derived = derive_projection_rows_with_sources(account_id, &events, &sources).unwrap();
        (events, derived)
    }

    async fn stage_recoverable_approved(
        state: &mut FanoutState,
        now: OffsetDateTime,
        risk_price_receipt: Option<AppendReceipt>,
    ) -> (
        pe_execution_core::live_journal::ApprovedAdmissionRecoveryEntry,
        pe_venue_polymarket::PreparedPolymarketBuy,
    ) {
        let account_id = AccountId::new("acct").unwrap();
        stage(
            state.config.paper_state.as_ref(),
            "dispatch-finality",
            now.unix_timestamp(),
            &["acct"],
        );
        let mut prepared = finality_prepared();
        prepared.identity.idempotency_key =
            LiveOrderIdentity::idempotency_key_for("dispatch-finality", &account_id);
        prepared.account_state = append_recovery_baseline(
            state,
            &account_id,
            now,
            CollateralAmount::from_atomic(10_000_000),
        )
        .await;
        prepared.economic.admission.market.observed_at_unix = now.unix_timestamp();
        prepared.economic.admission.settlement.observed_at_unix = now.unix_timestamp();
        prepared.economic.admission.scheduled_end_unix = now.unix_timestamp().checked_add(300);
        prepared.economic.risk.evaluated_at_unix_ms = unix_millis_i64(now);
        prepared.economic.risk.price_receipts.clear();
        let _ = bind_activity_observation(
            &mut prepared,
            &account_id,
            fixture_receipt(5),
            fixture_receipt(6),
            now,
            now,
        );
        if let Some(price_receipt) = risk_price_receipt {
            let (events, derived) =
                append_recovery_filled_position(state, &account_id, &prepared.account_state, now)
                    .await;
            let cash =
                CollateralAmount::from_decimal_exact(derived.economic_cash.unwrap()).unwrap();
            prepared.economic.balance.cash_before = cash;
            let binding = derived.account_binding.as_ref().unwrap();
            prepared.account_state = account_state_fixture_for_binding(cash, now, binding);
            let mut sources =
                source_envelopes_for_live_events(&state.config.source_receipts, &events).unwrap();
            sources.push(
                state
                    .config
                    .source_receipts
                    .source_envelope(price_receipt)
                    .unwrap(),
            );
            sources.sort_by_key(|source| source.seq);
            let mids = replay_live_risk_prices(
                &derived.positions,
                &[price_receipt],
                unix_millis_i64(now),
                &sources,
                Some(&state.config.source_receipts),
            )
            .unwrap();
            let paper_frames = scan_paper_log(&state.config.paper_log_path).unwrap();
            let projection = prepared.identity.fill_projection.as_ref().unwrap();
            prepared.economic.risk = compose_live_risk_audit(
                &account_id,
                &state.config.journal_path,
                &events,
                &derived,
                &paper_era_at_evaluation(&paper_frames, unix_millis_i64(now)).unwrap(),
                &mids,
                &[price_receipt],
                now,
                LiveRiskProposal {
                    leader_wallet: Some(projection.leader_wallet.clone()),
                    market_id: Some(projection.market_id.clone()),
                    proposed_debit: prepared.economic.balance.worst_case_debit,
                    per_trade_cap_bps: 10_000,
                },
            )
            .unwrap();
        } else {
            bind_empty_live_risk_to_paper_prefix(state, &mut prepared, now);
        }
        append_economic_sources(state, &mut prepared, &account_id, now).await;
        // The fake venue returns this prepared order verbatim; the resume path requires it to
        // match the request derived from the admission (ladder debit and identity hashes).
        prepared.prepared.worst_case_debit = prepared.prepared.maker_collateral;
        prepared.prepared.metadata_hashes = prepared.identity.evidence_hashes.clone();
        append_approved_admission(state.config.journal.as_ref(), &account_id, &prepared, now);
        let mut inventory = verified_recovery_inventory(state).unwrap();
        let pending = inventory.approved_admissions.remove(0);
        (pending, prepared.prepared)
    }

    /// PASS: the receipt-bearing recovery fixture has one strictly verified Approved candidate
    /// and one open position on the market covered by its Gamma price receipt.
    /// FAIL: service-tier recovery verification or strict projection derivation rejects the
    /// staged journal, or the staged position and price market differ.
    #[tokio::test]
    async fn stage_recoverable_approved_with_price_is_strictly_replayable() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let mut snapshot = armed_snapshot("acct");
        snapshot.fetched_at_unix = Some(now.unix_timestamp());
        let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
        let (price_receipt, condition_id) = append_recovery_risk_price(&state, now).await;

        let _ = stage_recoverable_approved(&mut state, now, Some(price_receipt)).await;

        let inventory = verified_recovery_inventory(&state).unwrap();
        assert_eq!(inventory.approved_admissions.len(), 1);
        assert_eq!(
            inventory.approved_admissions[0]
                .admission
                .economic
                .risk
                .price_receipts,
            vec![price_receipt]
        );
        let account_id = AccountId::new("acct").unwrap();
        let events = replay_live_account(&state, &account_id).unwrap();
        let derived = derive_projection_rows_for_state(&state, &account_id, &events).unwrap();
        assert_eq!(derived.positions.len(), 1);
        assert_eq!(derived.positions[0].market_id, condition_id);
    }

    /// PASS: a post-Baseline Approved admission with legacy `None` observation fails strict
    /// recovery inventory before preparation and leaves the journal with zero order POST facts.
    /// FAIL: current-era recovery treats the readable legacy shape as executable work.
    #[tokio::test]
    async fn current_recovery_rejects_missing_observation_before_post() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let mut state = fanout_state(
            &dir,
            db.clone(),
            armed_snapshot("acct"),
            "http://127.0.0.1:9",
            None,
        );
        let account_id = AccountId::new("acct").unwrap();
        stage(
            state.config.paper_state.as_ref(),
            "dispatch-finality",
            now.unix_timestamp(),
            &["acct"],
        );
        let mut prepared = finality_prepared();
        prepared.identity.idempotency_key =
            LiveOrderIdentity::idempotency_key_for(&prepared.identity.dispatch_id, &account_id);
        prepared.account_state = append_recovery_baseline(
            &state,
            &account_id,
            now,
            CollateralAmount::from_atomic(10_000_000),
        )
        .await;
        prepared.economic.admission.scheduled_end_unix = now.unix_timestamp().checked_add(300);
        prepared.economic.risk.evaluated_at_unix_ms = unix_millis_i64(now);
        bind_empty_live_risk_to_paper_prefix(&state, &mut prepared, now);
        append_economic_sources(&state, &mut prepared, &account_id, now).await;
        prepared.economic.observation = None;
        append_approved_admission(state.config.journal.as_ref(), &account_id, &prepared, now);

        assert!(verified_recovery_inventory(&state).is_err());
        assert!(run_recovery_pass(&mut state, || now, true).await.is_err());
        let events = replay_live_account(&state, &account_id).unwrap();
        assert!(!events.iter().any(|event| matches!(
            event.payload,
            LiveJournalPayload::OrderPrepared(_) | LiveJournalPayload::OrderPosted(_)
        )));
        assert_eq!(
            db.dispatch_targets("dispatch-finality").unwrap()[0].state,
            "pending"
        );
    }

    /// PASS: two Approved facts for one account/dispatch conflict before any recovery
    /// preparation, even when the second carries a different nonempty idempotency key.
    /// FAIL: recovery prepares or posts for either duplicate, or accepts the second key as a
    /// distinct dispatch.
    #[tokio::test]
    async fn duplicate_dispatch_admissions_stop_recovery_before_preparation() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let mut snapshot = armed_snapshot("acct");
        snapshot.fetched_at_unix = Some(now.unix_timestamp());
        let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
        let (pending, _) = stage_recoverable_approved(&mut state, now, None).await;
        let mut duplicate = pending.admission;
        duplicate.identity.idempotency_key = "different-nonempty-key".to_owned();
        state
            .config
            .journal
            .append(
                pending.account_id.clone(),
                now,
                LiveJournalPayload::AdmissionEvaluated(duplicate),
            )
            .unwrap();

        assert!(matches!(
            verified_recovery_inventory(&state),
            Err(FanoutError::Journal(
                pe_execution_core::LiveJournalError::OrderFactConflict
            ))
        ));
        assert!(
            replay_live_account(&state, &pending.account_id)
                .unwrap()
                .iter()
                .all(|event| !matches!(event.payload, LiveJournalPayload::OrderPrepared(_)))
        );
    }

    fn finality_matched(
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        transaction_hash: String,
    ) -> LiveOrderReconciliationAudit {
        LiveOrderReconciliationAudit {
            identity: prepared.identity.clone(),
            order_hash: prepared.prepared.order_hash.clone(),
            source: LiveReconciliationSource::PostResponse,
            outcome: LiveJournalOrderOutcome::Matched {
                venue_order_id: "venue-order".to_owned(),
                transaction_hashes: vec![transaction_hash],
                executed: None,
            },
            evidence: Vec::new(),
            evidence_hashes: Vec::new(),
        }
    }

    fn finality_matched_event(
        account_id: &AccountId,
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        seq: u64,
        timestamp: OffsetDateTime,
    ) -> LiveJournalEvent {
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp,
            payload: LiveJournalPayload::OrderReconciled(Box::new(finality_matched(
                prepared,
                format!("0x{}", "11".repeat(32)),
            ))),
        }
    }

    /// PASS: a terminal reconciliation cannot release a reservation unless every Prepared
    /// identity field, order hash, account envelope, and source/outcome pairing match.
    #[test]
    fn terminal_reconciliation_requires_the_exact_prepared_fact() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let mut wrong_identity = prepared.identity.clone();
        wrong_identity.quote_id = "different-quote".to_owned();
        let events = vec![
            baseline_event(&account_id, 1),
            approved_admission_event(&account_id, &prepared, 2, OffsetDateTime::UNIX_EPOCH),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 3,
                timestamp: OffsetDateTime::UNIX_EPOCH,
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 4,
                timestamp: OffsetDateTime::UNIX_EPOCH,
                payload: LiveJournalPayload::OrderReconciled(Box::new(
                    LiveOrderReconciliationAudit {
                        identity: wrong_identity,
                        order_hash: prepared.prepared.order_hash.clone(),
                        source: LiveReconciliationSource::PostResponse,
                        outcome: LiveJournalOrderOutcome::Killed {
                            venue_order_id: None,
                        },
                        evidence: Vec::new(),
                        evidence_hashes: Vec::new(),
                    },
                )),
            },
        ];
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &events, &[]),
            Err(ProjectionReducerError::IdentityConflict)
        ));

        let mut wrong_pair = events;
        if let LiveJournalPayload::OrderReconciled(reconciled) = &mut wrong_pair[3].payload {
            reconciled.identity = prepared.identity.clone();
            reconciled.source = LiveReconciliationSource::PolygonFinality;
        }
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &wrong_pair, &[]),
            Err(ProjectionReducerError::IdentityConflict)
        ));
    }

    /// PASS: an exact terminal reconciliation retry is idempotent, while any later changed fact
    /// (including Matched) makes the strict reducer reject the journal.
    #[test]
    fn terminal_reconciliation_retries_are_complete_fact_exact() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let terminal = LiveOrderReconciliationAudit {
            identity: prepared.identity.clone(),
            order_hash: prepared.prepared.order_hash.clone(),
            source: LiveReconciliationSource::OrderHashLookupAndCancel,
            outcome: LiveJournalOrderOutcome::Killed {
                venue_order_id: Some("venue-order".to_owned()),
            },
            evidence: Vec::new(),
            evidence_hashes: Vec::new(),
        };
        let event = |seq, audit: LiveOrderReconciliationAudit| LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: OffsetDateTime::from_unix_timestamp(i64::try_from(seq).unwrap()).unwrap(),
            payload: LiveJournalPayload::OrderReconciled(Box::new(audit)),
        };
        let prefix = vec![
            baseline_event(&account_id, 1),
            approved_admission_event(
                &account_id,
                &prepared,
                2,
                OffsetDateTime::from_unix_timestamp(2).unwrap(),
            ),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 3,
                timestamp: OffsetDateTime::from_unix_timestamp(3).unwrap(),
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
            event(4, terminal.clone()),
        ];
        let mut duplicate = prefix.clone();
        duplicate.push(event(5, terminal.clone()));
        let replayed = derive_with_baseline_evidence(&account_id, &duplicate, &[]).unwrap();
        assert_eq!(replayed.reserved, Decimal::ZERO);

        let mut changed = terminal;
        changed.source = LiveReconciliationSource::PostResponse;
        changed.outcome = LiveJournalOrderOutcome::Matched {
            venue_order_id: "venue-order".to_owned(),
            transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
            executed: None,
        };
        let mut contradictory = prefix;
        contradictory.push(event(5, changed));
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &contradictory, &[]),
            Err(ProjectionReducerError::IdentityConflict)
        ));
    }

    struct FakePolygonRpc {
        calls: Mutex<Vec<(String, Option<u64>)>>,
        chain_id_body: Vec<u8>,
        finalized_head: u64,
        finalized_body: Option<Vec<u8>>,
        receipt: Vec<u8>,
        receipts_by_hash: BTreeMap<String, Vec<u8>>,
        canonical_hash_byte: &'static str,
        canonical_body: Option<Vec<u8>>,
    }

    impl FakePolygonRpc {
        fn new(finalized_head: u64) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                chain_id_body: br#"{"jsonrpc":"2.0","id":1,"result":"0x89"}"#.to_vec(),
                finalized_head,
                finalized_body: None,
                receipt: include_bytes!(
                    "../../venue-polymarket/tests/fixtures/receipts/standard_v2.json"
                )
                .to_vec(),
                receipts_by_hash: BTreeMap::new(),
                canonical_hash_byte: "aa",
                canonical_body: None,
            }
        }

        fn with_chain_id_body(mut self, body: Vec<u8>) -> Self {
            self.chain_id_body = body;
            self
        }

        fn with_finalized_body(mut self, body: Vec<u8>) -> Self {
            self.finalized_body = Some(body);
            self
        }

        fn with_receipt(mut self, receipt: Vec<u8>) -> Self {
            self.receipt = receipt;
            self
        }

        fn with_canonical_body(mut self, body: Vec<u8>) -> Self {
            self.canonical_body = Some(body);
            self
        }

        fn with_canonical_hash(mut self, byte: &'static str) -> Self {
            self.canonical_hash_byte = byte;
            self
        }

        fn with_transaction_receipts(
            mut self,
            receipts: impl IntoIterator<Item = (String, Vec<u8>)>,
        ) -> Self {
            self.receipts_by_hash.extend(receipts);
            self
        }

        fn response(
            endpoint_kind: &str,
            rpc_method: &str,
            rpc_params: serde_json::Value,
            body: Vec<u8>,
        ) -> RawHttpAttempt {
            RawHttpAttempt::Response(pe_core_types::RawHttpResponse {
                source_id: "polygon-receipt-rpc".to_owned(),
                endpoint_kind: endpoint_kind.to_owned(),
                method: "POST".to_owned(),
                path: "fixture://polygon".to_owned(),
                ordered_query: vec![
                    ("rpc_method".to_owned(), rpc_method.to_owned()),
                    ("rpc_params".to_owned(), rpc_params.to_string()),
                ],
                status: 200,
                headers: Vec::new(),
                body,
                attempt_ordinal: 1,
                source_at: None,
                observed_at: OffsetDateTime::UNIX_EPOCH,
                received_at: OffsetDateTime::UNIX_EPOCH,
                schema_version: 1,
                parser_version: 1,
                adapter_version: "fixture".to_owned(),
            })
        }

        fn call_count(&self, method: &str) -> usize {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(candidate, _)| candidate == method)
                .count()
        }
    }

    impl PolygonReceiptReader for FakePolygonRpc {
        fn chain_id(&self) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>> {
            self.calls
                .lock()
                .unwrap()
                .push(("eth_chainId".to_owned(), None));
            let body = self.chain_id_body.clone();
            Box::pin(async move {
                Self::response(
                    "polygon-chain-id",
                    "eth_chainId",
                    serde_json::json!([]),
                    body,
                )
            })
        }

        fn transaction_receipt<'a>(
            &'a self,
            transaction_hash: &'a str,
        ) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + 'a>> {
            self.calls
                .lock()
                .unwrap()
                .push(("eth_getTransactionReceipt".to_owned(), None));
            let receipt = self
                .receipts_by_hash
                .get(transaction_hash)
                .cloned()
                .unwrap_or_else(|| self.receipt.clone());
            let transaction_hash = transaction_hash.to_owned();
            Box::pin(async move {
                Self::response(
                    "polygon-transaction-receipt",
                    "eth_getTransactionReceipt",
                    serde_json::json!([transaction_hash]),
                    receipt,
                )
            })
        }

        fn finalized_block(&self) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>> {
            self.calls
                .lock()
                .unwrap()
                .push(("eth_getBlockByNumber.finalized".to_owned(), None));
            let number = self.finalized_head;
            let body = self.finalized_body.clone();
            Box::pin(async move {
                let hash = if number == 100 { "aa" } else { "bb" };
                Self::response(
                    "polygon-finalized-block",
                    "eth_getBlockByNumber",
                    serde_json::json!(["finalized", false]),
                    body.unwrap_or_else(|| {
                        serde_json::to_vec(&serde_json::json!({
                            "jsonrpc":"2.0","id":1,"result":{
                                "number":format!("0x{number:x}"),
                                "hash":format!("0x{}", hash.repeat(32))
                            }
                        }))
                        .unwrap()
                    }),
                )
            })
        }

        fn block_by_number(
            &self,
            number: u64,
        ) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>> {
            self.calls
                .lock()
                .unwrap()
                .push(("eth_getBlockByNumber.canonical".to_owned(), Some(number)));
            let hash = self.canonical_hash_byte;
            let body = self.canonical_body.clone();
            Box::pin(async move {
                Self::response(
                    "polygon-canonical-block",
                    "eth_getBlockByNumber",
                    serde_json::json!([format!("0x{number:x}"), false]),
                    body.unwrap_or_else(|| {
                        serde_json::to_vec(&serde_json::json!({
                            "jsonrpc":"2.0","id":1,"result":{
                                "number":format!("0x{number:x}"),
                                "hash":format!("0x{}", hash.repeat(32))
                            }
                        }))
                        .unwrap()
                    }),
                )
            })
        }
    }

    /// PASS: one pass reads each distinct receipt and lower block once, without a range query.
    #[tokio::test]
    async fn finality_batch_deduplicates_receipts_and_block_reads() {
        let rpc = FakePolygonRpc::new(101);
        let reserve = finality_prepared().economic.fee.reserve;
        let order = PendingOrderFinality {
            prepared_journal_seq: 9,
            prepared: finality_prepared(),
            transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
            immutable_receipts: BTreeMap::new(),
        };
        let results = collect_order_finality(&rpc, vec![order.clone(), order]).await;
        assert_eq!(results.len(), 2);
        let finalized = results
            .iter()
            .filter_map(|result| match &result.disposition {
                OrderFinalityDisposition::Finalized(finalized) => Some(finalized),
                OrderFinalityDisposition::Pending { .. }
                | OrderFinalityDisposition::Conflict { .. } => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            finalized.len(),
            2,
            "finality results were not complete: {results:?}"
        );
        for finalized in finalized {
            assert_eq!(
                finalized.principal,
                CollateralAmount::from_atomic(2_500_000)
            );
            assert_eq!(finalized.quantity, ShareAmount::from_atomic(3_125_000));
            assert_eq!(finalized.fee, CollateralAmount::from_atomic(120));
            assert_eq!(
                finalized.fee, reserve,
                "the reserve equality edge finalizes"
            );
        }
        assert_eq!(rpc.call_count("eth_chainId"), 1);
        assert_eq!(rpc.call_count("eth_getTransactionReceipt"), 1);
        assert_eq!(rpc.call_count("eth_getBlockByNumber.finalized"), 1);
        assert_eq!(rpc.call_count("eth_getBlockByNumber.canonical"), 1);
        assert_eq!(rpc.call_count("eth_getLogs"), 0);
    }

    /// PASS: each malformed HTTP 200 Polygon body retains its exact raw attempt, appends the
    /// runtime-produced Pending fact, and is accepted with the same reason by recovery and strict
    /// projection replay.
    #[tokio::test]
    async fn malformed_runtime_finality_results_round_trip_through_both_reducers() {
        let malformed = b"not-json".to_vec();
        let cases = [
            (
                "polygon-chain-id",
                FakePolygonRpc::new(101).with_chain_id_body(malformed.clone()),
                "polygon chain identity unavailable or not 137",
            ),
            (
                "polygon-transaction-receipt",
                FakePolygonRpc::new(101).with_receipt(malformed.clone()),
                "transaction receipt unavailable: malformed JSON-RPC response",
            ),
            (
                "polygon-finalized-block",
                FakePolygonRpc::new(101).with_finalized_body(malformed.clone()),
                "polygon finalized head unavailable",
            ),
            (
                "polygon-canonical-block",
                FakePolygonRpc::new(101).with_canonical_body(malformed.clone()),
                "canonical block unavailable: malformed JSON-RPC response",
            ),
        ];

        for (endpoint_kind, rpc, expected_reason) in cases {
            let dir = tempdir().unwrap();
            let path = dir.path().join("live.log");
            let journal = LiveJournal::open(&path).unwrap();
            let account_id = AccountId::new("account").unwrap();
            let baseline = baseline_event(&account_id, 0);
            journal
                .append(account_id.clone(), baseline.timestamp, baseline.payload)
                .unwrap();
            let prepared = finality_prepared();
            append_approved_admission(&journal, &account_id, &prepared, OffsetDateTime::UNIX_EPOCH);
            let prepared_event = journal
                .append(
                    account_id.clone(),
                    OffsetDateTime::UNIX_EPOCH,
                    LiveJournalPayload::OrderPrepared(prepared.clone()),
                )
                .unwrap();
            let transaction_hash = format!("0x{}", "11".repeat(32));
            journal
                .append(
                    account_id.clone(),
                    OffsetDateTime::UNIX_EPOCH,
                    LiveJournalPayload::OrderReconciled(Box::new(finality_matched(
                        &prepared,
                        transaction_hash.clone(),
                    ))),
                )
                .unwrap();

            let result = collect_order_finality(
                &rpc,
                vec![PendingOrderFinality {
                    prepared_journal_seq: prepared_event.seq,
                    prepared: prepared.clone(),
                    transaction_hashes: vec![transaction_hash.clone()],
                    immutable_receipts: BTreeMap::new(),
                }],
            )
            .await
            .pop()
            .unwrap();
            assert!(
                matches!(
                    &result.disposition,
                    OrderFinalityDisposition::Pending { .. }
                ),
                "{endpoint_kind} should be runtime Pending, got {:?}",
                result.disposition
            );
            let OrderFinalityDisposition::Pending { reason, evidence } = &result.disposition else {
                continue;
            };
            let runtime_reason = reason.clone();
            let runtime_evidence = evidence.clone();
            assert_eq!(runtime_reason, expected_reason, "{endpoint_kind}");
            assert!(runtime_evidence.iter().any(|attempt| matches!(
                attempt,
                RawHttpAttempt::Response(response)
                    if response.endpoint_kind == endpoint_kind && response.body == malformed
            )));
            assert_eq!(
                append_order_finality_result(
                    &journal,
                    account_id.clone(),
                    OffsetDateTime::UNIX_EPOCH,
                    result,
                )
                .unwrap(),
                FinalityJournalEffect::Pending
            );

            let inventory = recovery_inventory(&path, None).unwrap();
            assert_eq!(inventory.open_orders.len(), 1, "{endpoint_kind}");
            assert_eq!(
                inventory.open_orders[0].transaction_hashes,
                vec![transaction_hash]
            );
            let events = replay_account(&path, &account_id).unwrap();
            assert!(matches!(
                &events.last().unwrap().payload,
                LiveJournalPayload::OrderReconciled(_)
            ));
            let LiveJournalPayload::OrderReconciled(replayed) = &events.last().unwrap().payload
            else {
                continue;
            };
            assert_eq!(replayed.evidence, runtime_evidence, "{endpoint_kind}");
            assert_eq!(
                replayed.outcome,
                LiveJournalOrderOutcome::FinalityPending {
                    reason: runtime_reason
                },
                "{endpoint_kind}"
            );
            let projection = derive_with_baseline_evidence(&account_id, &events, &[]).unwrap();
            assert_eq!(projection.reserved, dec!(2.500120), "{endpoint_kind}");
        }
    }

    fn receipt_for_transaction(
        transaction_hash: &str,
        log_index: u64,
        principal: u64,
        quantity: u64,
        fee: u64,
    ) -> Vec<u8> {
        let mut receipt: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../venue-polymarket/tests/fixtures/receipts/standard_v2.json"
        ))
        .unwrap();
        receipt["result"]["transactionHash"] = serde_json::json!(transaction_hash);
        receipt["result"]["logs"][0]["transactionHash"] = serde_json::json!(transaction_hash);
        receipt["result"]["logs"][0]["logIndex"] = serde_json::json!(format!("0x{log_index:x}"));
        let data = receipt["result"]["logs"][0]["data"]
            .as_str()
            .unwrap()
            .strip_prefix("0x")
            .unwrap();
        let mut words = data
            .as_bytes()
            .chunks_exact(64)
            .map(|word| std::str::from_utf8(word).unwrap().to_owned())
            .collect::<Vec<_>>();
        for (index, value) in [(2, principal), (3, quantity), (4, fee)] {
            *words.get_mut(index).unwrap() = format!("{value:064x}");
        }
        receipt["result"]["logs"][0]["data"] = serde_json::json!(format!("0x{}", words.concat()));
        serde_json::to_vec(&receipt).unwrap()
    }

    /// PASS: distinct authenticated transactions aggregate exactly and each receipt is read once.
    #[tokio::test]
    async fn finality_aggregates_multiple_transactions() {
        let first = format!("0x{}", "11".repeat(32));
        let second = format!("0x{}", "22".repeat(32));
        let rpc = FakePolygonRpc::new(101).with_transaction_receipts([
            (
                first.clone(),
                receipt_for_transaction(&first, 7, 1_000_000, 1_250_000, 50),
            ),
            (
                second.clone(),
                receipt_for_transaction(&second, 8, 1_500_000, 1_875_000, 70),
            ),
        ]);
        let mut results = collect_order_finality(
            &rpc,
            vec![PendingOrderFinality {
                prepared_journal_seq: 9,
                prepared: finality_prepared(),
                transaction_hashes: vec![second.clone(), first, second],
                immutable_receipts: BTreeMap::new(),
            }],
        )
        .await;
        let result = results.pop().unwrap();
        let finalized = match result.disposition {
            OrderFinalityDisposition::Finalized(finalized) => Some(finalized),
            OrderFinalityDisposition::Pending { .. }
            | OrderFinalityDisposition::Conflict { .. } => None,
        };
        assert!(finalized.is_some_and(|finalized| {
            finalized.principal == CollateralAmount::from_atomic(2_500_000)
                && finalized.quantity == ShareAmount::from_atomic(3_125_000)
                && finalized.fee == CollateralAmount::from_atomic(120)
                && finalized.matched_logs.len() == 2
        }));
        assert_eq!(rpc.call_count("eth_getTransactionReceipt"), 2);
        assert_eq!(rpc.call_count("eth_getBlockByNumber.canonical"), 1);
    }

    /// PASS: one multi-level collateral plan retains its improved paper quantity, its distinct
    /// signed minimum, and the exact finalized live quantity without a prefix rewalk.
    #[tokio::test]
    async fn multilevel_plan_signed_minimum_and_finalized_quantity_share_one_audit() {
        let ladder = vec![
            pe_venue_polymarket::AskLevel {
                price: Price(dec!(0.70)),
                shares: ShareAmount::from_decimal_exact(dec!(1)).unwrap(),
            },
            pe_venue_polymarket::AskLevel {
                price: Price(dec!(0.80)),
                shares: ShareAmount::from_decimal_exact(dec!(10)).unwrap(),
            },
        ];
        let sized = plan_sized_buy(
            &ladder,
            pe_venue_polymarket::CompactFeeSchedule::Zero,
            BuySizing::Dollar {
                budget: CollateralAmount::from_decimal_exact(dec!(2.5)).unwrap(),
            },
            &[CollateralAmount::from_decimal_exact(dec!(2.5)).unwrap()],
            ShareAmount::from_decimal_exact(dec!(1)).unwrap(),
            Price(dec!(0.01)),
            Price(dec!(0.15)),
            Price(dec!(0.85)),
            Price(dec!(0.80)),
            Price(dec!(0.80)),
        )
        .unwrap();
        assert_eq!(sized.ladder.shares.to_decimal(), dec!(3.125));
        assert_eq!(
            sized.ladder.expected_shares().unwrap().to_decimal(),
            dec!(3.25)
        );

        let mut prepared = finality_prepared();
        prepared.economic.ladder = LadderPlanAudit::new(&sized.ladder);
        prepared.economic.sizing.principal = sized.ladder.worst_case_debit;
        prepared.economic.sizing.minimum_shares = sized.ladder.shares;
        prepared.economic.sizing.expected_shares = sized.ladder.expected_shares().unwrap();
        prepared.economic.sizing.expected_vwap = sized.ladder.vwap().unwrap();
        prepared.prepared.maker_collateral = sized.ladder.worst_case_debit;
        prepared.prepared.taker_shares = sized.ladder.shares;
        prepared.prepared.limit_price = sized.ladder.limit_price;
        assert_eq!(prepared.prepared.taker_shares.to_decimal(), dec!(3.125));
        assert_eq!(
            prepared.economic.sizing.expected_shares.to_decimal(),
            dec!(3.25)
        );
        let mut results = collect_order_finality(
            &FakePolygonRpc::new(101).with_receipt(receipt_with_word(3, 3_250_000)),
            vec![PendingOrderFinality {
                prepared_journal_seq: 9,
                prepared,
                transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
                immutable_receipts: BTreeMap::new(),
            }],
        )
        .await;
        let disposition = results.pop().unwrap().disposition;
        assert!(
            matches!(disposition, OrderFinalityDisposition::Finalized(_)),
            "expected exact multi-level finality, got {disposition:?}"
        );
        let OrderFinalityDisposition::Finalized(exact) = disposition else {
            return;
        };
        assert_eq!(exact.quantity.to_decimal(), dec!(3.25));
        assert_eq!(exact.principal.to_decimal(), dec!(2.5));
    }

    /// PASS: a receipt at the head reuses that head; an above-head receipt stays pending.
    #[tokio::test]
    async fn finality_equal_and_above_head_issue_no_canonical_block_read() {
        let order = PendingOrderFinality {
            prepared_journal_seq: 9,
            prepared: finality_prepared(),
            transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
            immutable_receipts: BTreeMap::new(),
        };
        let equal = FakePolygonRpc::new(100);
        let equal_result = collect_order_finality(&equal, vec![order.clone()]).await;
        assert!(
            matches!(
                equal_result.first().map(|result| &result.disposition),
                Some(OrderFinalityDisposition::Finalized(_))
            ),
            "{equal_result:?}"
        );
        assert_eq!(equal.call_count("eth_getBlockByNumber.canonical"), 0);

        let above = FakePolygonRpc::new(99);
        let above_result = collect_order_finality(&above, vec![order]).await;
        assert!(matches!(
            above_result.first().map(|result| &result.disposition),
            Some(OrderFinalityDisposition::Pending { .. })
        ));
        assert_eq!(above.call_count("eth_getBlockByNumber.canonical"), 0);
    }

    /// PASS: recovery inventory and strict projection both accept a verified conflict and its
    /// exact duplicate, and both reject a later changed PostResponse/Matched reconciliation.
    #[tokio::test]
    async fn finality_conflict_terminal_retry_parity_between_both_reducers() {
        enum Followup {
            None,
            ExactDuplicate,
            LaterMatched,
        }

        for (case, followup, expected_acceptance) in [
            ("baseline", Followup::None, true),
            ("exact_duplicate", Followup::ExactDuplicate, true),
            ("later_matched", Followup::LaterMatched, false),
        ] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("live.log");
            let journal = LiveJournal::open(&path).unwrap();
            let account_id = AccountId::new("account").unwrap();
            let baseline = baseline_event(&account_id, 0);
            journal
                .append(account_id.clone(), baseline.timestamp, baseline.payload)
                .unwrap();
            let prepared = finality_prepared();
            append_approved_admission(&journal, &account_id, &prepared, OffsetDateTime::UNIX_EPOCH);
            let prepared_event = journal
                .append(
                    account_id.clone(),
                    OffsetDateTime::UNIX_EPOCH,
                    LiveJournalPayload::OrderPrepared(prepared.clone()),
                )
                .unwrap();
            let first_hash = format!("0x{}", "11".repeat(32));
            journal
                .append(
                    account_id.clone(),
                    OffsetDateTime::UNIX_EPOCH,
                    LiveJournalPayload::OrderReconciled(Box::new(finality_matched(
                        &prepared,
                        first_hash.clone(),
                    ))),
                )
                .unwrap();
            let conflict = collect_order_finality(
                &FakePolygonRpc::new(101).with_receipt(receipt_with_word(2, 2_500_001)),
                vec![PendingOrderFinality {
                    prepared_journal_seq: prepared_event.seq,
                    prepared: prepared.clone(),
                    transaction_hashes: vec![first_hash],
                    immutable_receipts: BTreeMap::new(),
                }],
            )
            .await
            .pop()
            .unwrap();
            assert!(matches!(
                &conflict.disposition,
                OrderFinalityDisposition::Conflict { .. }
            ));
            assert_eq!(
                append_order_finality_result(
                    &journal,
                    account_id.clone(),
                    OffsetDateTime::UNIX_EPOCH,
                    conflict,
                )
                .unwrap(),
                FinalityJournalEffect::Conflict
            );
            let conflict_payload = replay_account(&path, &account_id)
                .unwrap()
                .last()
                .unwrap()
                .payload
                .clone();
            match followup {
                Followup::None => {}
                Followup::ExactDuplicate => {
                    journal
                        .append(
                            account_id.clone(),
                            OffsetDateTime::UNIX_EPOCH,
                            conflict_payload,
                        )
                        .unwrap();
                }
                Followup::LaterMatched => {
                    journal
                        .append(
                            account_id.clone(),
                            OffsetDateTime::UNIX_EPOCH,
                            LiveJournalPayload::OrderReconciled(Box::new(finality_matched(
                                &prepared,
                                format!("0x{}", "22".repeat(32)),
                            ))),
                        )
                        .unwrap();
                }
            }

            let recovery = recovery_inventory(&path, None);
            let events = replay_account(&path, &account_id).unwrap();
            let projection = derive_with_baseline_evidence(&account_id, &events, &[]);
            assert_eq!(recovery.is_ok(), expected_acceptance, "recovery: {case}");
            assert_eq!(
                projection.is_ok(),
                expected_acceptance,
                "projection: {case}"
            );
            if expected_acceptance {
                assert!(recovery.unwrap().open_orders.is_empty(), "{case}");
                assert_eq!(projection.unwrap().reserved, dec!(2.500120), "{case}");
            }
        }
    }

    /// PASS: Pending may advance to a conflict, but that conflict removes the order from automated
    /// recovery, preserves its reservation in the strict reducer, and rejects a later clean Final.
    #[tokio::test]
    async fn finality_conflict_freezes_recovery_and_rejects_clean_retry() {
        let account_id = AccountId::new("account").unwrap();
        let order = PendingOrderFinality {
            prepared_journal_seq: 2,
            prepared: finality_prepared(),
            transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
            immutable_receipts: BTreeMap::new(),
        };
        let mut pending =
            collect_order_finality(&FakePolygonRpc::new(99), vec![order.clone()]).await;
        let mut conflict = collect_order_finality(
            &FakePolygonRpc::new(101).with_receipt(receipt_with_word(2, 2_500_001)),
            vec![order.clone()],
        )
        .await;
        let mut finalized = collect_order_finality(&FakePolygonRpc::new(101), vec![order]).await;
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let prepared = finality_prepared();
        let baseline = baseline_event(&account_id, 0);
        journal
            .append(account_id.clone(), baseline.timestamp, baseline.payload)
            .unwrap();
        append_approved_admission(&journal, &account_id, &prepared, OffsetDateTime::UNIX_EPOCH);
        journal
            .append(
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                LiveJournalPayload::OrderPrepared(prepared.clone()),
            )
            .unwrap();
        let matched_prepared = prepared.clone();
        let matched = LiveOrderReconciliationAudit {
            identity: matched_prepared.identity.clone(),
            order_hash: matched_prepared.prepared.order_hash.clone(),
            source: LiveReconciliationSource::PostResponse,
            outcome: LiveJournalOrderOutcome::Matched {
                venue_order_id: "venue-order".to_owned(),
                transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
                executed: None,
            },
            evidence: Vec::new(),
            evidence_hashes: Vec::new(),
        };
        journal
            .append(
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                LiveJournalPayload::OrderReconciled(Box::new(matched.clone())),
            )
            .unwrap();
        assert_eq!(
            append_order_finality_result(
                &journal,
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                pending.pop().unwrap(),
            )
            .unwrap(),
            FinalityJournalEffect::Pending
        );
        assert_eq!(
            append_order_finality_result(
                &journal,
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                conflict.pop().unwrap(),
            )
            .unwrap(),
            FinalityJournalEffect::Conflict
        );
        let frozen = recovery_inventory(&path, None).unwrap();
        assert!(frozen.open_orders.is_empty());
        let retained = replay_account(&path, &account_id).unwrap();
        let pending_payload = retained[4].payload.clone();
        let conflict_payload = retained[5].payload.clone();
        assert_eq!(
            append_order_finality_result(
                &journal,
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                finalized.pop().unwrap(),
            )
            .unwrap(),
            FinalityJournalEffect::Finalized
        );
        assert!(matches!(
            recovery_inventory(&path, None),
            Err(pe_execution_core::LiveJournalError::OrderFactConflict)
        ));

        let reducer_events = vec![
            baseline_event(&account_id, 1),
            approved_admission_event(
                &account_id,
                &prepared,
                2,
                OffsetDateTime::from_unix_timestamp(5).unwrap(),
            ),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 3,
                timestamp: OffsetDateTime::from_unix_timestamp(5).unwrap(),
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 4,
                timestamp: OffsetDateTime::from_unix_timestamp(6).unwrap(),
                payload: LiveJournalPayload::OrderReconciled(Box::new(matched)),
            },
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 5,
                timestamp: OffsetDateTime::from_unix_timestamp(8).unwrap(),
                payload: pending_payload,
            },
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 6,
                timestamp: OffsetDateTime::from_unix_timestamp(9).unwrap(),
                payload: conflict_payload,
            },
            finalized_fill_event(&account_id, &prepared, 7, 3),
        ];
        let frozen_projection =
            derive_with_baseline_evidence(&account_id, &reducer_events[..5], &[]).unwrap();
        assert_eq!(frozen_projection.reserved, dec!(2.500120));

        let mut evidence_free = reducer_events[..3].to_vec();
        evidence_free.push(LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 4,
            timestamp: OffsetDateTime::from_unix_timestamp(9).unwrap(),
            payload: LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                identity: prepared.identity.clone(),
                order_hash: prepared.prepared.order_hash.clone(),
                source: LiveReconciliationSource::PolygonFinality,
                outcome: LiveJournalOrderOutcome::FinalityConflict {
                    reason: "finalized principal exceeds the signed principal".to_owned(),
                },
                evidence: Vec::new(),
                evidence_hashes: Vec::new(),
            })),
        });
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &evidence_free, &[]),
            Err(ProjectionReducerError::InvalidFinalityEvidence)
        ));

        for endpoint_kind in ["polygon-transaction-receipt", "polygon-finalized-block"] {
            let mut malformed = reducer_events[..5].to_vec();
            if let LiveJournalPayload::OrderReconciled(conflict) = &mut malformed[4].payload {
                let response = conflict
                    .evidence
                    .iter_mut()
                    .find_map(|attempt| match attempt {
                        RawHttpAttempt::Response(response)
                            if response.endpoint_kind == endpoint_kind =>
                        {
                            Some(response)
                        }
                        RawHttpAttempt::Response(_) | RawHttpAttempt::TransportFailure(_) => None,
                    })
                    .unwrap();
                response.body = b"not-json".to_vec();
                conflict.evidence_hashes = http_attempt_hashes(&conflict.evidence).unwrap();
            }
            assert!(matches!(
                derive_with_baseline_evidence(&account_id, &malformed, &[]),
                Err(ProjectionReducerError::InvalidFinalityEvidence)
            ));
        }
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &reducer_events, &[]),
            Err(ProjectionReducerError::FinalityConflictFrozen)
        ));
    }

    /// PASS: a 2,499,999-principal Pending is recovered into the next real collection pass; the
    /// same hash/log rewritten to 2,500,000 appends a durable Conflict accepted by both consumers.
    #[tokio::test]
    async fn finality_pending_rewrite_appends_conflict_through_both_consumers() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let transaction_hash = format!("0x{}", "11".repeat(32));
        let baseline = baseline_event(&account_id, 0);
        journal
            .append(account_id.clone(), baseline.timestamp, baseline.payload)
            .unwrap();
        append_approved_admission(&journal, &account_id, &prepared, OffsetDateTime::UNIX_EPOCH);
        let prepared_event = journal
            .append(
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                LiveJournalPayload::OrderPrepared(prepared.clone()),
            )
            .unwrap();
        journal
            .append(
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                LiveJournalPayload::OrderReconciled(Box::new(finality_matched(
                    &prepared,
                    transaction_hash.clone(),
                ))),
            )
            .unwrap();

        let pending_result = collect_order_finality(
            &FakePolygonRpc::new(101).with_receipt(receipt_with_word(2, 2_499_999)),
            vec![PendingOrderFinality {
                prepared_journal_seq: prepared_event.seq,
                prepared: prepared.clone(),
                transaction_hashes: vec![transaction_hash.clone()],
                immutable_receipts: BTreeMap::new(),
            }],
        )
        .await
        .pop()
        .unwrap();
        assert_eq!(
            append_order_finality_result(
                &journal,
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                pending_result,
            )
            .unwrap(),
            FinalityJournalEffect::Pending
        );

        let inventory = recovery_inventory(&path, None).unwrap();
        let recovered = inventory.open_orders.first().unwrap();
        assert_eq!(recovered.immutable_receipts.len(), 1);
        assert!(recovered.immutable_receipts.contains_key(&transaction_hash));
        let changed_result = collect_order_finality(
            &FakePolygonRpc::new(101),
            vec![PendingOrderFinality {
                prepared_journal_seq: recovered.inventory.prepared_journal_seq,
                prepared: recovered.prepared.clone().unwrap(),
                transaction_hashes: recovered.transaction_hashes.clone(),
                immutable_receipts: recovered.immutable_receipts.clone(),
            }],
        )
        .await
        .pop()
        .unwrap();
        assert!(matches!(
            &changed_result.disposition,
            OrderFinalityDisposition::Conflict { reason, .. }
                if reason == "transaction receipt changed after becoming an immutable observation"
        ));
        assert_eq!(
            append_order_finality_result(
                &journal,
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                changed_result,
            )
            .unwrap(),
            FinalityJournalEffect::Conflict
        );
        drop(journal);

        assert!(
            recovery_inventory(&path, None)
                .unwrap()
                .open_orders
                .is_empty()
        );
        let events = replay_account(&path, &account_id).unwrap();
        assert!(matches!(
            &events.last().unwrap().payload,
            LiveJournalPayload::OrderReconciled(reconciled)
                if matches!(reconciled.outcome, LiveJournalOrderOutcome::FinalityConflict { .. })
        ));
        let projection = derive_with_baseline_evidence(&account_id, &events, &[]).unwrap();
        assert_eq!(projection.reserved, dec!(2.500120));
    }

    fn receipt_with_word(index: usize, value: u64) -> Vec<u8> {
        let mut receipt: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../venue-polymarket/tests/fixtures/receipts/standard_v2.json"
        ))
        .unwrap();
        let data = receipt["result"]["logs"][0]["data"]
            .as_str()
            .unwrap()
            .strip_prefix("0x")
            .unwrap();
        let mut words = data
            .as_bytes()
            .chunks_exact(64)
            .map(|word| std::str::from_utf8(word).unwrap().to_owned())
            .collect::<Vec<_>>();
        *words.get_mut(index).unwrap() = format!("{value:064x}");
        receipt["result"]["logs"][0]["data"] = serde_json::json!(format!("0x{}", words.concat()));
        serde_json::to_vec(&receipt).unwrap()
    }

    /// PASS: malformed, economically rewritten, inventory-unbound, canonical-query-mismatched,
    /// and prior-Pending-contradicting Finals are rejected as invalid finality evidence by both
    /// recovery inventory and the strict financial reducer.
    #[tokio::test]
    async fn finalized_evidence_parity_matrix_rejects_invalid_finals() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let transaction_hash = format!("0x{}", "11".repeat(32));
        let prefix = || {
            vec![
                baseline_event(&account_id, 0),
                approved_admission_event(&account_id, &prepared, 1, OffsetDateTime::UNIX_EPOCH),
                LiveJournalEvent {
                    account_id: account_id.clone(),
                    seq: 2,
                    timestamp: OffsetDateTime::UNIX_EPOCH,
                    payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
                },
                LiveJournalEvent {
                    account_id: account_id.clone(),
                    seq: 3,
                    timestamp: OffsetDateTime::UNIX_EPOCH,
                    payload: LiveJournalPayload::OrderReconciled(Box::new(finality_matched(
                        &prepared,
                        transaction_hash.clone(),
                    ))),
                },
            ]
        };

        let mut malformed = prefix();
        let mut malformed_final = finalized_fill_event(&account_id, &prepared, 4, 2);
        if let LiveJournalPayload::OrderFillFinalized(finalized) = &mut malformed_final.payload {
            finalized.receipts.clear();
            finalized.blocks.clear();
        }
        malformed.push(malformed_final);

        let mut economic = prefix();
        let mut economic_final = finalized_fill_event(&account_id, &prepared, 4, 2);
        if let LiveJournalPayload::OrderFillFinalized(finalized) = &mut economic_final.payload {
            finalized.principal = CollateralAmount::from_atomic(2_500_001);
            let RawHttpAttempt::Response(response) = finalized.receipts.get_mut(1).unwrap() else {
                unreachable!();
            };
            response.body = receipt_with_word(2, 2_500_001);
        }
        economic.push(economic_final);

        let second_transaction_hash = format!("0x{}", "22".repeat(32));
        let mut missing_authenticated_hash = prefix();
        if let LiveJournalPayload::OrderReconciled(reconciled) =
            &mut missing_authenticated_hash[3].payload
            && let LiveJournalOrderOutcome::Matched {
                transaction_hashes, ..
            } = &mut reconciled.outcome
        {
            transaction_hashes.push(second_transaction_hash.clone());
        }
        missing_authenticated_hash.push(finalized_fill_event(&account_id, &prepared, 4, 2));

        let mut substituted_authenticated_hash = prefix();
        if let LiveJournalPayload::OrderReconciled(reconciled) =
            &mut substituted_authenticated_hash[3].payload
            && let LiveJournalOrderOutcome::Matched {
                transaction_hashes, ..
            } = &mut reconciled.outcome
        {
            *transaction_hashes = vec![second_transaction_hash.clone()];
        }
        substituted_authenticated_hash.push(finalized_fill_event(&account_id, &prepared, 4, 2));

        let mut zero_authenticated_hashes = vec![
            baseline_event(&account_id, 0),
            approved_admission_event(&account_id, &prepared, 1, OffsetDateTime::UNIX_EPOCH),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 2,
                timestamp: OffsetDateTime::UNIX_EPOCH,
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
        ];
        zero_authenticated_hashes.push(finalized_fill_event(&account_id, &prepared, 3, 2));

        let mut extra_receipt = prefix();
        let mut extra_receipt_final = finalized_fill_event(&account_id, &prepared, 4, 2);
        if let LiveJournalPayload::OrderFillFinalized(finalized) = &mut extra_receipt_final.payload
        {
            let mut receipt_body: serde_json::Value = serde_json::from_slice(
                &receipt_for_transaction(&second_transaction_hash, 8, 0, 0, 0),
            )
            .unwrap();
            receipt_body["result"]["logs"] = serde_json::json!([]);
            let RawHttpAttempt::Response(mut response) = finalized.receipts[1].clone() else {
                unreachable!();
            };
            response.ordered_query[1].1 = serde_json::json!([second_transaction_hash]).to_string();
            response.body = serde_json::to_vec(&receipt_body).unwrap();
            finalized.receipts.push(RawHttpAttempt::Response(response));
        }
        extra_receipt.push(extra_receipt_final);

        let mut canonical_key_mismatch = prefix();
        let mut canonical_key_mismatch_final = finalized_fill_event(&account_id, &prepared, 4, 2);
        if let LiveJournalPayload::OrderFillFinalized(finalized) =
            &mut canonical_key_mismatch_final.payload
        {
            let RawHttpAttempt::Response(mut response) = finalized.blocks[0].clone() else {
                unreachable!();
            };
            response.endpoint_kind = "polygon-canonical-block".to_owned();
            response.ordered_query[1].1 = serde_json::json!(["0x64", false]).to_string();
            finalized.blocks.push(RawHttpAttempt::Response(response));
        }
        canonical_key_mismatch.push(canonical_key_mismatch_final);

        let pending_result = collect_order_finality(
            &FakePolygonRpc::new(101).with_receipt(receipt_with_word(2, 2_499_999)),
            vec![PendingOrderFinality {
                prepared_journal_seq: 2,
                prepared: prepared.clone(),
                transaction_hashes: vec![transaction_hash.clone()],
                immutable_receipts: BTreeMap::new(),
            }],
        )
        .await
        .pop()
        .unwrap();
        let OrderFinalityDisposition::Pending { reason, evidence } = pending_result.disposition
        else {
            unreachable!();
        };
        let mut contradicts_pending = prefix();
        contradicts_pending.push(LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 4,
            timestamp: OffsetDateTime::UNIX_EPOCH,
            payload: LiveJournalPayload::OrderReconciled(Box::new(LiveOrderReconciliationAudit {
                identity: prepared.identity.clone(),
                order_hash: prepared.prepared.order_hash.clone(),
                source: LiveReconciliationSource::PolygonFinality,
                outcome: LiveJournalOrderOutcome::FinalityPending { reason },
                evidence_hashes: http_attempt_hashes(&evidence).unwrap(),
                evidence,
            })),
        });
        contradicts_pending.push(finalized_fill_event(&account_id, &prepared, 5, 2));

        let mut failures = Vec::new();
        for (case, events) in [
            ("malformed", malformed),
            ("economic_change", economic),
            ("missing_authenticated_hash", missing_authenticated_hash),
            (
                "substituted_authenticated_hash",
                substituted_authenticated_hash,
            ),
            ("zero_authenticated_hashes", zero_authenticated_hashes),
            ("extra_receipt", extra_receipt),
            ("canonical_key_mismatch", canonical_key_mismatch),
            ("prior_pending_contradiction", contradicts_pending),
        ] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("live.log");
            let journal = LiveJournal::open(&path).unwrap();
            for event in &events {
                journal
                    .append(
                        event.account_id.clone(),
                        event.timestamp,
                        event.payload.clone(),
                    )
                    .unwrap();
            }
            drop(journal);
            let recovery = recovery_inventory(&path, None);
            if !matches!(
                recovery,
                Err(pe_execution_core::LiveJournalError::InvalidFinalityEvidence)
            ) {
                failures.push(format!("recovery accepted {case}: {recovery:?}"));
            }
            let projection = derive_with_baseline_evidence(&account_id, &events, &[]);
            match projection {
                Err(ProjectionReducerError::InvalidFinalityEvidence) => {}
                Err(error) => {
                    failures.push(format!("strict reducer returned {error:?} for {case}"));
                }
                Ok(_) => failures.push(format!("strict reducer accepted {case}")),
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    async fn one_finality_disposition(rpc: &FakePolygonRpc) -> OrderFinalityDisposition {
        collect_order_finality(
            rpc,
            vec![PendingOrderFinality {
                prepared_journal_seq: 9,
                prepared: finality_prepared(),
                transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
                immutable_receipts: BTreeMap::new(),
            }],
        )
        .await
        .pop()
        .unwrap()
        .disposition
    }

    /// PASS: principal/quantity/fee, duplicate-log, zero-match, and canonicality edges classify exactly.
    #[tokio::test]
    async fn finality_economic_and_identity_edges_fail_closed() {
        assert!(matches!(
            one_finality_disposition(&FakePolygonRpc::new(101).with_receipt(b"not-json".to_vec()))
                .await,
            OrderFinalityDisposition::Pending { .. }
        ));
        assert!(matches!(
            one_finality_disposition(
                &FakePolygonRpc::new(101).with_receipt(receipt_with_word(2, 2_499_999))
            )
            .await,
            OrderFinalityDisposition::Pending { .. }
        ));
        for (word, value) in [(2, 2_500_001), (3, 3_124_999), (4, 130)] {
            assert!(matches!(
                one_finality_disposition(
                    &FakePolygonRpc::new(101).with_receipt(receipt_with_word(word, value))
                )
                .await,
                OrderFinalityDisposition::Conflict { .. }
            ));
        }
        let first = format!("0x{}", "11".repeat(32));
        let second = format!("0x{}", "22".repeat(32));
        let overflow = FakePolygonRpc::new(101).with_transaction_receipts([
            (
                first.clone(),
                receipt_for_transaction(&first, 7, u64::MAX, 1, 0),
            ),
            (
                second.clone(),
                receipt_for_transaction(&second, 8, u64::MAX, 1, 0),
            ),
        ]);
        let overflow_result = collect_order_finality(
            &overflow,
            vec![PendingOrderFinality {
                prepared_journal_seq: 9,
                prepared: finality_prepared(),
                transaction_hashes: vec![first, second],
                immutable_receipts: BTreeMap::new(),
            }],
        )
        .await;
        assert!(matches!(
            overflow_result.first().map(|result| &result.disposition),
            Some(OrderFinalityDisposition::Conflict { .. })
        ));

        let mut duplicate: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../venue-polymarket/tests/fixtures/receipts/standard_v2.json"
        ))
        .unwrap();
        let log = duplicate["result"]["logs"][0].clone();
        duplicate["result"]["logs"]
            .as_array_mut()
            .unwrap()
            .push(log);
        assert!(matches!(
            one_finality_disposition(
                &FakePolygonRpc::new(101).with_receipt(serde_json::to_vec(&duplicate).unwrap())
            )
            .await,
            OrderFinalityDisposition::Finalized(_)
        ));

        let mut conflicting = duplicate;
        conflicting["result"]["logs"][1]["topics"][3] =
            serde_json::json!(format!("0x{}", "44".repeat(32)));
        assert!(matches!(
            one_finality_disposition(
                &FakePolygonRpc::new(101).with_receipt(serde_json::to_vec(&conflicting).unwrap())
            )
            .await,
            OrderFinalityDisposition::Conflict { .. }
        ));

        let mut unrelated: serde_json::Value = serde_json::from_slice(include_bytes!(
            "../../venue-polymarket/tests/fixtures/receipts/standard_v2.json"
        ))
        .unwrap();
        unrelated["result"]["logs"][0]["topics"][1] =
            serde_json::json!(format!("0x{}", "ff".repeat(32)));
        assert!(matches!(
            one_finality_disposition(
                &FakePolygonRpc::new(101).with_receipt(serde_json::to_vec(&unrelated).unwrap())
            )
            .await,
            OrderFinalityDisposition::Conflict { .. }
        ));
        assert!(matches!(
            one_finality_disposition(&FakePolygonRpc::new(101).with_canonical_hash("cc")).await,
            OrderFinalityDisposition::Conflict { .. }
        ));
    }

    /// PASS: Baseline fails closed without raw account attempts or complete-position receipts,
    /// while the same copied summary is accepted only with its retained preimages.
    #[test]
    fn baseline_requires_retained_account_and_position_evidence() {
        let account_id = AccountId::new("account").unwrap();
        let mut no_account = baseline_event(&account_id, 1);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut no_account.payload {
            mark.account_state.evidence.clear();
            mark.account_state.evidence_hashes.clear();
            mark.venue_position_evidence.pages.clear();
        }
        assert!(matches!(
            derive_projection_rows_with_sources(
                &account_id,
                &[no_account],
                &baseline_sources(&account_id),
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut no_positions = baseline_event(&account_id, 1);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut no_positions.payload {
            mark.venue_position_evidence.pages.clear();
        }
        assert!(matches!(
            derive_projection_rows_with_sources(
                &account_id,
                &[no_positions],
                &baseline_sources(&account_id),
            ),
            Err(ProjectionReducerError::InvalidPositionEvidence)
        ));

        let mut copied_cash = baseline_event(&account_id, 1);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut copied_cash.payload {
            mark.account_state.collateral_balance = CollateralAmount::from_atomic(9_999_999);
        }
        assert!(matches!(
            derive_projection_rows_with_sources(
                &account_id,
                &[copied_cash],
                &baseline_sources(&account_id),
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut copied_hash = baseline_event(&account_id, 1);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut copied_hash.payload {
            mark.account_state.evidence_hashes[0] = "rewritten".to_owned();
        }
        assert!(matches!(
            derive_projection_rows_with_sources(
                &account_id,
                &[copied_hash],
                &baseline_sources(&account_id),
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut copied_request_hash = baseline_event(&account_id, 1);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut copied_request_hash.payload {
            mark.account_state.request_descriptor_hashes[0] = "rewritten".to_owned();
        }
        assert!(matches!(
            derive_projection_rows_with_sources(
                &account_id,
                &[copied_request_hash],
                &baseline_sources(&account_id),
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut corrupted_position_sources = baseline_sources(&account_id);
        corrupted_position_sources[0].raw_payload_hash = blake3::hash(b"rewritten");
        assert!(matches!(
            derive_projection_rows_with_sources(
                &account_id,
                &[baseline_event(&account_id, 1)],
                &corrupted_position_sources,
            ),
            Err(ProjectionReducerError::InvalidPositionEvidence)
        ));

        let accepted =
            derive_with_baseline_evidence(&account_id, &[baseline_event(&account_id, 1)], &[])
                .unwrap();
        assert_eq!(accepted.economic_cash, Some(dec!(10)));
        assert_eq!(accepted.baseline_sequence, Some(1));

        let mut negrisk = baseline_event(&account_id, 1);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut negrisk.payload {
            mark.account_state.selected_spender =
                pe_venue_polymarket::CanaryV2Client::negrisk_spender().unwrap();
        }
        assert!(derive_with_baseline_evidence(&account_id, &[negrisk], &[]).is_ok());
    }

    /// PASS: approved admission and Prepared account reads are accepted only when their request
    /// descriptors bind the active Baseline account; failed reads retain the same binding check.
    #[test]
    fn admission_and_prepared_account_evidence_bind_the_active_baseline() {
        let account_a = AccountId::new("account").unwrap();
        let account_b = AccountId::new("account-b").unwrap();
        let wallet = "0x1111111111111111111111111111111111111111";
        let binding_b = account_binding_fixture(&account_b, wallet);
        let prepared = finality_prepared();
        let event = |seq, payload| LiveJournalEvent {
            account_id: account_a.clone(),
            seq,
            timestamp: OffsetDateTime::from_unix_timestamp(i64::try_from(seq).unwrap()).unwrap(),
            payload,
        };

        let valid_admission = approved_admission(&prepared);
        let valid = vec![
            baseline_event(&account_a, 1),
            event(2, LiveJournalPayload::AdmissionEvaluated(valid_admission)),
            event(3, LiveJournalPayload::OrderPrepared(prepared.clone())),
        ];
        assert!(derive_with_baseline_evidence(&account_a, &valid, &[]).is_ok());

        let account_b_state = account_state_fixture_for_binding(
            CollateralAmount::from_atomic(10_000_000),
            OffsetDateTime::UNIX_EPOCH,
            &binding_b,
        );
        let mut substituted_prepared = prepared.clone();
        substituted_prepared.account_state = account_b_state.clone();
        let substituted_admission = approved_admission(&substituted_prepared);
        let substituted = vec![
            baseline_event(&account_a, 1),
            event(
                2,
                LiveJournalPayload::AdmissionEvaluated(substituted_admission),
            ),
            event(
                3,
                LiveJournalPayload::OrderPrepared(substituted_prepared.clone()),
            ),
        ];
        assert!(matches!(
            derive_with_baseline_evidence(&account_a, &substituted, &[]),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut prepared_only_substitution = valid.clone();
        if let LiveJournalPayload::OrderPrepared(order) = &mut prepared_only_substitution[2].payload
        {
            order.account_state = account_b_state.clone();
        }
        assert!(matches!(
            derive_with_baseline_evidence(&account_a, &prepared_only_substitution, &[]),
            Err(ProjectionReducerError::IdentityConflict)
        ));

        let mut credential_substitution = valid.clone();
        if let LiveJournalPayload::AdmissionEvaluated(admission) =
            &mut credential_substitution[1].payload
        {
            admission.frozen_binding.version = 2;
            admission.current_binding.version = 2;
        }
        assert!(matches!(
            derive_with_baseline_evidence(&account_a, &credential_substitution, &[]),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut descriptor_tamper = valid.clone();
        if let LiveJournalPayload::AdmissionEvaluated(admission) = &mut descriptor_tamper[1].payload
            && let Some(account_state) = &mut admission.account_state
        {
            account_state.request_descriptor_hashes[0] = "rewritten".to_owned();
        }
        assert!(matches!(
            derive_with_baseline_evidence(&account_a, &descriptor_tamper, &[]),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut refused = approved_admission(&prepared);
        let account_state = refused.account_state.take().unwrap();
        refused.account_read_failure_evidence = vec![account_state.evidence[0].clone()];
        refused.account_read_failure_request_descriptor_hashes =
            vec![account_state.request_descriptor_hashes[0].clone()];
        refused.account_read_failure_evidence_hashes =
            http_attempt_hashes(&refused.account_read_failure_evidence).unwrap();
        refused.verdict = pe_execution_core::LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::AccountStateUnavailable(
                pe_execution_core::LiveAccountReadFailure::Authentication,
            ),
        );
        let refused_events = vec![
            baseline_event(&account_a, 1),
            event(2, LiveJournalPayload::AdmissionEvaluated(refused.clone())),
        ];
        assert!(matches!(
            derive_with_baseline_evidence(&account_a, &refused_events, &[]),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut refused_substitution = refused.clone();
        refused_substitution.account_read_failure_evidence =
            vec![account_b_state.evidence[0].clone()];
        refused_substitution.account_read_failure_request_descriptor_hashes =
            vec![account_b_state.request_descriptor_hashes[0].clone()];
        refused_substitution.account_read_failure_evidence_hashes =
            http_attempt_hashes(&refused_substitution.account_read_failure_evidence).unwrap();
        let refused_substituted_events = vec![
            baseline_event(&account_a, 1),
            event(
                2,
                LiveJournalPayload::AdmissionEvaluated(refused_substitution),
            ),
        ];
        assert!(matches!(
            derive_with_baseline_evidence(&account_a, &refused_substituted_events, &[]),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        refused.account_read_failure_request_descriptor_hashes[0] = "rewritten".to_owned();
        let refused_tamper = vec![
            baseline_event(&account_a, 1),
            event(2, LiveJournalPayload::AdmissionEvaluated(refused)),
        ];
        assert!(matches!(
            derive_with_baseline_evidence(&account_a, &refused_tamper, &[]),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));
    }

    /// PASS: strict reduction recomputes the pure risk decision and rejects a producer-recorded
    /// Approved decision whose snapshot deterministically trips the absolute-loss kill switch.
    #[test]
    fn strict_reducer_rejects_approved_admission_with_blocked_risk_snapshot() {
        let account_id = AccountId::new("risk-recheck").unwrap();
        let prepared = finality_prepared();
        let mut admission = approved_admission(&prepared);
        admission.identity.idempotency_key =
            LiveOrderIdentity::idempotency_key_for(&admission.identity.dispatch_id, &account_id);
        admission.economic.risk.snapshot.absolute_pnl_bps = pe_core_types::BasisPoints(-1_000);
        admission.economic.risk.decision = RiskDecisionAudit::Approved;
        let events = vec![
            baseline_event(&account_id, 1),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 2,
                timestamp: OffsetDateTime::from_unix_timestamp(2).unwrap(),
                payload: LiveJournalPayload::AdmissionEvaluated(admission),
            },
        ];

        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &events, &[]),
            Err(ProjectionReducerError::InvalidRiskDecision)
        ));
    }

    /// PASS: every crash seam after Approved and before Prepared reduces to one pending recovery
    /// key; resuming with Prepared or terminalizing with a typed failure consumes it exactly once.
    #[test]
    fn strict_reducer_converges_unmatched_approved_admission() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let event = |seq, payload| LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: OffsetDateTime::from_unix_timestamp(i64::try_from(seq).unwrap()).unwrap(),
            payload,
        };
        for _seam in [
            "after_admission_append",
            "during_venue_preparation",
            "before_prepared_append",
        ] {
            let prefix = vec![
                baseline_event(&account_id, 1),
                event(
                    2,
                    LiveJournalPayload::AdmissionEvaluated(approved_admission(&prepared)),
                ),
            ];
            let pending = derive_with_baseline_evidence(&account_id, &prefix, &[]).unwrap();
            assert_eq!(
                pending.pending_approved_admission_keys,
                BTreeSet::from([prepared.identity.dispatch_id.clone()])
            );

            let mut resumed = prefix.clone();
            resumed.push(event(
                3,
                LiveJournalPayload::OrderPrepared(prepared.clone()),
            ));
            let resumed = derive_with_baseline_evidence(&account_id, &resumed, &[]).unwrap();
            assert!(resumed.pending_approved_admission_keys.is_empty());
            assert!(resumed.validated_prepared_sequences.contains(&3));

            let mut terminalized = prefix;
            terminalized.push(event(
                3,
                LiveJournalPayload::OrderPreparationFailed(Box::new(
                    pe_execution_core::LiveOrderPreparationFailedAudit {
                        identity: prepared.identity.clone(),
                        failure: pe_execution_core::LiveOrderPreparationFailure::Venue,
                    },
                )),
            ));
            let terminalized =
                derive_with_baseline_evidence(&account_id, &terminalized, &[]).unwrap();
            assert!(terminalized.pending_approved_admission_keys.is_empty());
            assert!(terminalized.validated_prepared_sequences.is_empty());
        }
    }

    /// PASS: the fan-out recovery inventory excludes an Approved admission at or before the
    /// verified QualificationStarted live prefix.
    /// FAIL: recovery lists, prepares, or posts an admission recorded at or before that prefix.
    #[tokio::test]
    async fn approved_recovery_excludes_pre_start_live_prefix() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let mut state = fanout_state(
            &dir,
            db,
            LiveAccountsSnapshot::default(),
            "http://127.0.0.1:9",
            None,
        );
        let account_id = AccountId::new("acct").unwrap();
        let mut prepared = finality_prepared();
        prepared.account_state = append_recovery_baseline(
            &state,
            &account_id,
            OffsetDateTime::UNIX_EPOCH,
            CollateralAmount::from_atomic(10_000_000),
        )
        .await;
        bind_empty_live_risk_to_paper_prefix(&state, &mut prepared, OffsetDateTime::UNIX_EPOCH);
        append_approved_admission(
            state.config.journal.as_ref(),
            &account_id,
            &prepared,
            OffsetDateTime::UNIX_EPOCH,
        );
        assert!(recovery_is_pending(&state).unwrap());

        let prefix = LiveJournal::verified_tail(&state.config.journal_path).unwrap();
        state.config.era_live_prefix = Some(prefix);
        assert!(!recovery_is_pending(&state).unwrap());
    }

    /// PASS: the structural idle hint reports a tampered Approved admission as pending without
    /// evaluating its evidence, while consuming recovery rejects the invalid risk decision.
    /// FAIL: the hint errors on the tamper, misses the pending admission, or consumption accepts it.
    #[tokio::test]
    async fn idle_recovery_hint_does_not_verify_admission_evidence() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let state = fanout_state(
            &dir,
            db,
            LiveAccountsSnapshot::default(),
            "http://127.0.0.1:9",
            None,
        );
        let account_id = AccountId::new("idle-hint").unwrap();
        let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
        let mut prepared = finality_prepared();
        prepared.account_state = append_recovery_baseline(
            &state,
            &account_id,
            now,
            CollateralAmount::from_atomic(10_000_000),
        )
        .await;
        prepared.economic.admission.market.observed_at_unix = now.unix_timestamp();
        prepared.economic.admission.settlement.observed_at_unix = now.unix_timestamp();
        prepared.economic.admission.settlement.source_timestamp_unix = Some(now.unix_timestamp());
        prepared.economic.admission.scheduled_end_unix = now.unix_timestamp().checked_add(300);
        prepared.economic.risk.price_receipts.clear();
        bind_empty_live_risk_to_paper_prefix(&state, &mut prepared, now);
        let mut admission = approved_admission(&prepared);
        admission.economic.risk.snapshot.absolute_pnl_bps = BasisPoints(-1_000);
        admission.economic.risk.decision = RiskDecisionAudit::Approved;
        state
            .config
            .journal
            .append(
                account_id,
                now,
                LiveJournalPayload::AdmissionEvaluated(admission),
            )
            .unwrap();

        assert!(recovery_is_pending(&state).unwrap());
        assert!(matches!(
            verified_recovery_inventory(&state),
            Err(FanoutError::Journal(
                pe_execution_core::LiveJournalError::OrderFactConflict
            ))
        ));
    }

    /// PASS: recovery exposes no admission when the same strict reducer rejects a zero-equity or
    /// duplicate same-era Baseline, even though the journal-only inventory can parse both logs.
    #[tokio::test]
    async fn recovery_requires_the_strict_source_backed_baseline_rule() {
        for duplicate in [false, true] {
            let dir = tempdir().unwrap();
            let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let state = fanout_state(
                &dir,
                db,
                LiveAccountsSnapshot::default(),
                "http://127.0.0.1:9",
                None,
            );
            let account_id = AccountId::new("acct").unwrap();
            let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
            let equity = if duplicate {
                CollateralAmount::from_atomic(10_000_000)
            } else {
                CollateralAmount::ZERO
            };
            let account_state = append_recovery_baseline(&state, &account_id, now, equity).await;
            if duplicate {
                let baseline = replay_live_account(&state, &account_id).unwrap()[0]
                    .payload
                    .clone();
                state
                    .config
                    .journal
                    .append(account_id.clone(), now, baseline)
                    .unwrap();
            }
            let mut prepared = finality_prepared();
            prepared.identity.idempotency_key =
                LiveOrderIdentity::idempotency_key_for(&prepared.identity.dispatch_id, &account_id);
            prepared.account_state = account_state;
            prepared.economic.admission.market.observed_at_unix = now.unix_timestamp();
            prepared.economic.admission.settlement.observed_at_unix = now.unix_timestamp();
            prepared.economic.admission.scheduled_end_unix = now.unix_timestamp().checked_add(300);
            prepared.economic.risk.evaluated_at_unix_ms =
                now.unix_timestamp().saturating_mul(1_000);
            prepared.economic.risk.price_receipts.clear();
            bind_empty_live_risk_to_paper_prefix(&state, &mut prepared, now);
            append_approved_admission(state.config.journal.as_ref(), &account_id, &prepared, now);

            if duplicate {
                assert!(matches!(
                    recovery_inventory(&state.config.journal_path, None),
                    Err(pe_execution_core::LiveJournalError::RequestBinding)
                ));
            } else {
                assert_eq!(
                    recovery_inventory(&state.config.journal_path, None)
                        .unwrap()
                        .approved_admissions
                        .len(),
                    1
                );
            }
            assert!(matches!(
                verified_recovery_inventory(&state),
                Err(FanoutError::Journal(
                    pe_execution_core::LiveJournalError::OrderFactConflict
                        | pe_execution_core::LiveJournalError::RequestBinding
                ))
            ));
        }
    }

    /// PASS: crashes after a durable Refused admission or preparation failure leave a pending
    /// SQLite target that recovery deterministically terminalizes without appending another fact.
    /// FAIL: ordinary target processing owns terminal crash reconciliation or the terminal reason
    /// differs from the durable recovery outcome.
    #[tokio::test]
    async fn terminal_admission_crash_seams_reconcile_the_dispatch_projection() {
        for preparation_failed in [false, true] {
            let dir = tempdir().unwrap();
            let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let mut state = fanout_state(
                &dir,
                db.clone(),
                LiveAccountsSnapshot::default(),
                "http://127.0.0.1:9",
                None,
            );
            let now = OffsetDateTime::from_unix_timestamp(1_800_000_000).unwrap();
            stage(
                db.as_ref(),
                "dispatch-finality",
                now.unix_timestamp(),
                &["acct"],
            );
            let account_id = AccountId::new("acct").unwrap();
            let account_state = append_recovery_baseline(
                &state,
                &account_id,
                now,
                CollateralAmount::from_atomic(10_000_000),
            )
            .await;
            let mut prepared = finality_prepared();
            prepared.identity.idempotency_key =
                LiveOrderIdentity::idempotency_key_for(&prepared.identity.dispatch_id, &account_id);
            prepared.account_state = account_state;
            prepared.economic.admission.market.observed_at_unix = now.unix_timestamp();
            prepared.economic.admission.settlement.observed_at_unix = now.unix_timestamp();
            prepared.economic.admission.scheduled_end_unix = now.unix_timestamp().checked_add(300);
            prepared.economic.risk.evaluated_at_unix_ms =
                now.unix_timestamp().saturating_mul(1_000);
            prepared.economic.risk.price_receipts.clear();
            bind_empty_live_risk_to_paper_prefix(&state, &mut prepared, now);
            append_economic_sources(&state, &mut prepared, &account_id, now).await;
            if preparation_failed {
                append_approved_admission(
                    state.config.journal.as_ref(),
                    &account_id,
                    &prepared,
                    now,
                );
                state
                    .config
                    .journal
                    .append(
                        account_id,
                        now,
                        LiveJournalPayload::OrderPreparationFailed(Box::new(
                            pe_execution_core::LiveOrderPreparationFailedAudit {
                                identity: prepared.identity.clone(),
                                failure: pe_execution_core::LiveOrderPreparationFailure::Venue,
                            },
                        )),
                    )
                    .unwrap();
            } else {
                let mut refused = approved_admission(&prepared);
                refused.requested_mode = LiveControlMode::Off;
                refused.effective_mode = LiveControlMode::Off;
                refused.account_state = None;
                refused.verdict = pe_execution_core::LiveAdmissionVerdict::Refused(
                    LiveAdmissionRefusal::ModeNotArmed,
                );
                state
                    .config
                    .journal
                    .append(
                        account_id,
                        now,
                        LiveJournalPayload::AdmissionEvaluated(refused),
                    )
                    .unwrap();
            }

            assert_eq!(
                db.dispatch_targets("dispatch-finality").unwrap()[0].state,
                "pending"
            );
            let inventory = verified_recovery_inventory(&state).unwrap();
            assert_eq!(inventory.terminal_admissions.len(), 1);
            assert!(recovery_is_pending(&state).unwrap());
            let journal_len = replay_live_account(&state, &AccountId::new("acct").unwrap())
                .unwrap()
                .len();
            run_recovery_pass(&mut state, || now, true).await.unwrap();
            let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
            assert_eq!(target.state, "terminal");
            assert_eq!(
                target.terminal_reason.as_deref(),
                Some(if preparation_failed {
                    "rejected"
                } else {
                    "admission_refused"
                })
            );
            assert!(db.unfinalized_ready_dispatch_seeds().unwrap().is_empty());
            assert_eq!(
                replay_live_account(&state, &AccountId::new("acct").unwrap())
                    .unwrap()
                    .len(),
                journal_len,
                "ordinary dispatch must not append a second admission"
            );
            assert!(!recovery_is_pending(&state).unwrap());
        }
    }

    /// PASS: a failed account request consumes exactly one descriptor bound to the active
    /// Baseline; an otherwise identical account-B descriptor and a tampered descriptor fail.
    #[test]
    fn transport_failure_descriptor_rejects_cross_account_substitution_and_tamper() {
        let account_a = AccountId::new("account").unwrap();
        let account_b = AccountId::new("account-b").unwrap();
        let wallet = "0x1111111111111111111111111111111111111111";
        let binding_a = account_binding_fixture(&account_a, wallet);
        let binding_b = account_binding_fixture(&account_b, wallet);
        let prepared = finality_prepared();
        let failure = pe_core_types::RawTransportFailure {
            source_id: "polymarket-clob-v2".to_owned(),
            endpoint_kind: "closed-only".to_owned(),
            method: "GET".to_owned(),
            path: "/auth/ban-status/closed-only".to_owned(),
            ordered_query: Vec::new(),
            attempt_ordinal: 1,
            observed_at: OffsetDateTime::UNIX_EPOCH,
            received_at: OffsetDateTime::UNIX_EPOCH,
            error_class: pe_core_types::TransportErrorClass::Connect,
            schema_version: 1,
            parser_version: 1,
            adapter_version: pe_venue_polymarket::SDK_VERSION.to_owned(),
        };
        let attempt = RawHttpAttempt::TransportFailure(failure);
        let mut refusal = approved_admission(&prepared);
        refusal.account_state = None;
        refusal.account_read_failure_evidence = vec![attempt.clone()];
        refusal.account_read_failure_evidence_hashes =
            http_attempt_hashes(&refusal.account_read_failure_evidence).unwrap();
        refusal.verdict = pe_execution_core::LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::AccountStateUnavailable(
                pe_execution_core::LiveAccountReadFailure::Transport,
            ),
        );
        let event = |audit| LiveJournalEvent {
            account_id: account_a.clone(),
            seq: 2,
            timestamp: OffsetDateTime::from_unix_timestamp(2).unwrap(),
            payload: LiveJournalPayload::AdmissionEvaluated(audit),
        };

        refusal.account_read_failure_request_descriptor_hashes = vec![
            pe_execution_core::live_journal::request_descriptor_hash(
                &binding_a.request_descriptor_for_attempt(&attempt),
            )
            .unwrap(),
        ];
        assert!(
            derive_with_baseline_evidence(
                &account_a,
                &[baseline_event(&account_a, 1), event(refusal.clone())],
                &[],
            )
            .is_ok()
        );

        let mut substituted = refusal.clone();
        substituted.account_read_failure_request_descriptor_hashes = vec![
            pe_execution_core::live_journal::request_descriptor_hash(
                &binding_b.request_descriptor_for_attempt(&attempt),
            )
            .unwrap(),
        ];
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_a,
                &[baseline_event(&account_a, 1), event(substituted)],
                &[],
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        refusal.account_read_failure_request_descriptor_hashes[0] = "rewritten".to_owned();
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_a,
                &[baseline_event(&account_a, 1), event(refusal)],
                &[],
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));
    }

    /// PASS: strict reduction derives the ordered admission verdict and consumes one matching
    /// approval before Prepared; authentic underfunding, relabeling, reason/bound tampering, and
    /// a missing admission all fail closed while the unchanged fixture passes.
    #[test]
    fn strict_reducer_replays_ordered_admission_before_prepared() {
        let account_id = AccountId::new("account").unwrap();
        let binding =
            account_binding_fixture(&account_id, "0x1111111111111111111111111111111111111111");
        let prepared = finality_prepared();
        let event = |seq, payload| LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: OffsetDateTime::from_unix_timestamp(i64::try_from(seq).unwrap()).unwrap(),
            payload,
        };
        let stream = |admission, prepared| {
            vec![
                baseline_event(&account_id, 1),
                event(2, LiveJournalPayload::AdmissionEvaluated(admission)),
                event(3, LiveJournalPayload::OrderPrepared(prepared)),
            ]
        };
        assert!(
            derive_with_baseline_evidence(
                &account_id,
                &stream(approved_admission(&prepared), prepared.clone()),
                &[],
            )
            .is_ok()
        );

        let underfunded_state = account_state_fixture_for_binding(
            CollateralAmount::from_atomic(1),
            OffsetDateTime::UNIX_EPOCH,
            &binding,
        );
        let mut underfunded_prepared = prepared.clone();
        underfunded_prepared.account_state = underfunded_state;
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &stream(
                    approved_admission(&underfunded_prepared),
                    underfunded_prepared.clone(),
                ),
                &[],
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut relabeled = approved_admission(&prepared);
        let account_state = relabeled.account_state.take().unwrap();
        relabeled.account_read_failure_evidence = account_state.evidence;
        relabeled.account_read_failure_request_descriptor_hashes =
            account_state.request_descriptor_hashes;
        relabeled.account_read_failure_evidence_hashes =
            http_attempt_hashes(&relabeled.account_read_failure_evidence).unwrap();
        relabeled.verdict = pe_execution_core::LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::AccountStateUnavailable(
                pe_execution_core::LiveAccountReadFailure::Authentication,
            ),
        );
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &[
                    baseline_event(&account_id, 1),
                    event(2, LiveJournalPayload::AdmissionEvaluated(relabeled))
                ],
                &[],
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut valid_authentication_failure = approved_admission(&prepared);
        let mut failed_account = valid_authentication_failure.account_state.take().unwrap();
        let response = failed_account
            .evidence
            .iter_mut()
            .find_map(|attempt| match attempt {
                RawHttpAttempt::Response(response) if response.endpoint_kind == "geoblock" => {
                    Some(response)
                }
                RawHttpAttempt::Response(_) | RawHttpAttempt::TransportFailure(_) => None,
            })
            .unwrap();
        response.status = 401;
        valid_authentication_failure.account_read_failure_evidence = failed_account.evidence;
        valid_authentication_failure.account_read_failure_request_descriptor_hashes =
            failed_account.request_descriptor_hashes;
        valid_authentication_failure.account_read_failure_evidence_hashes =
            http_attempt_hashes(&valid_authentication_failure.account_read_failure_evidence)
                .unwrap();
        valid_authentication_failure.verdict = pe_execution_core::LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::AccountStateUnavailable(
                pe_execution_core::LiveAccountReadFailure::Authentication,
            ),
        );
        assert!(
            derive_with_baseline_evidence(
                &account_id,
                &[
                    baseline_event(&account_id, 1),
                    event(
                        2,
                        LiveJournalPayload::AdmissionEvaluated(valid_authentication_failure),
                    ),
                ],
                &[],
            )
            .is_ok()
        );

        let mut wrong_reason = approved_admission(&underfunded_prepared);
        wrong_reason.verdict = pe_execution_core::LiveAdmissionVerdict::Refused(
            LiveAdmissionRefusal::InsufficientBalance {
                required: CollateralAmount::from_atomic(1),
                available: CollateralAmount::from_atomic(1),
            },
        );
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &[
                    baseline_event(&account_id, 1),
                    event(2, LiveJournalPayload::AdmissionEvaluated(wrong_reason))
                ],
                &[],
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut wrong_bound_prepared = prepared.clone();
        wrong_bound_prepared.economic.balance.worst_case_debit = CollateralAmount::from_atomic(1);
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &stream(
                    approved_admission(&wrong_bound_prepared),
                    wrong_bound_prepared,
                ),
                &[],
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &[
                    baseline_event(&account_id, 1),
                    event(2, LiveJournalPayload::OrderPrepared(prepared)),
                ],
                &[],
            ),
            Err(ProjectionReducerError::MissingApprovedAdmission)
        ));
    }

    /// PASS: valid account-B responses and empty position pages cannot establish account A's
    /// Baseline or Daily mark after the copied account/request labels are changed to A.
    #[test]
    fn baseline_rejects_cross_account_evidence_even_when_positions_are_empty() {
        let account_a = AccountId::new("account-a").unwrap();
        let account_b = AccountId::new("account-b").unwrap();
        let wallet = "0x1111111111111111111111111111111111111111";
        let binding_a = account_binding_fixture(&account_a, wallet);
        let binding_b = account_binding_fixture(&account_b, wallet);
        let (account_b_positions, account_b_sources) =
            empty_position_evidence(120, OffsetDateTime::UNIX_EPOCH, &binding_b);
        let mut copied_account = baseline_event(&account_a, 1);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut copied_account.payload {
            mark.account_state = account_state_fixture_for_binding(
                CollateralAmount::from_whole(10).unwrap(),
                OffsetDateTime::UNIX_EPOCH,
                &binding_b,
            );
            mark.account_binding = binding_a.clone();
            mark.venue_position_evidence = account_b_positions.clone();
        }
        assert!(matches!(
            derive_projection_rows_with_sources(&account_a, &[copied_account], &account_b_sources,),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut copied_positions = baseline_event(&account_a, 1);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut copied_positions.payload {
            mark.venue_position_evidence = account_b_positions.clone();
        }
        assert!(matches!(
            derive_projection_rows_with_sources(
                &account_a,
                &[copied_positions],
                &account_b_sources,
            ),
            Err(ProjectionReducerError::InvalidPositionEvidence)
        ));

        let daily_at = OffsetDateTime::from_unix_timestamp(86_401).unwrap();
        let mut copied_daily = baseline_event(&account_a, 2);
        copied_daily.timestamp = daily_at;
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut copied_daily.payload {
            mark.kind = MarkKind::Daily;
            mark.cutoff_unix = 86_400;
            mark.account_state = account_state_fixture_for_binding(
                CollateralAmount::from_whole(10).unwrap(),
                daily_at,
                &binding_b,
            );
            mark.venue_position_evidence = account_b_positions.clone();
        }
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_a,
                &[baseline_event(&account_a, 1), copied_daily],
                &account_b_sources,
            ),
            Err(ProjectionReducerError::InvalidAccountEvidence)
        ));

        let mut copied_daily_positions = baseline_event(&account_a, 2);
        copied_daily_positions.timestamp = OffsetDateTime::from_unix_timestamp(86_401).unwrap();
        if let LiveJournalPayload::AccountPortfolioMarked(mark) =
            &mut copied_daily_positions.payload
        {
            mark.kind = MarkKind::Daily;
            mark.cutoff_unix = 86_400;
            mark.venue_position_evidence = account_b_positions;
        }
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_a,
                &[baseline_event(&account_a, 1), copied_daily_positions],
                &account_b_sources,
            ),
            Err(ProjectionReducerError::InvalidPositionEvidence)
        ));
    }

    /// PASS: a second Baseline in one account era is a typed reducer conflict.
    #[test]
    fn second_baseline_in_the_same_era_conflicts() {
        let account_id = AccountId::new("account").unwrap();
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &[
                    baseline_event(&account_id, 0),
                    baseline_event(&account_id, 1)
                ],
                &[],
            ),
            Err(ProjectionReducerError::DuplicateBaseline)
        ));
    }

    /// PASS: a repeated Daily cutoff in one account era is a typed reducer conflict.
    #[test]
    fn duplicate_daily_cutoff_conflicts() {
        let account_id = AccountId::new("account").unwrap();
        let mut daily = baseline_event(&account_id, 2);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut daily.payload {
            mark.kind = MarkKind::Daily;
            mark.cutoff_unix = 86_400;
        }
        let mut repeated = daily.clone();
        repeated.seq = 3;
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &[baseline_event(&account_id, 1), daily, repeated],
                &[],
            ),
            Err(ProjectionReducerError::DuplicateDailyMark)
        ));
    }

    /// PASS: Daily replay rejects non-monotonic UTC-midnight cutoffs.
    #[test]
    fn daily_marks_require_causal_midnight_evidence() {
        let account_id = AccountId::new("account").unwrap();
        let mut first = baseline_event(&account_id, 2);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut first.payload {
            mark.kind = MarkKind::Daily;
            mark.cutoff_unix = 86_400;
        }
        let mut earlier = first.clone();
        earlier.seq = 3;
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &mut earlier.payload {
            mark.cutoff_unix = 0;
        }
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &[baseline_event(&account_id, 1), first.clone(), earlier],
                &[],
            ),
            Err(ProjectionReducerError::InvalidDailyMark)
        ));
    }

    /// PASS: unexplained authenticated cash movement blocks the strict live posture.
    #[test]
    fn cash_drift_is_rejected_at_one_atomic_unit() {
        assert!(
            require_cash_reconciliation(
                CollateralAmount::from_atomic(7_000_000),
                dec!(10),
                CollateralAmount::from_atomic(3_000_000),
            )
            .is_ok()
        );
        assert!(matches!(
            require_cash_reconciliation(
                CollateralAmount::from_atomic(7_000_001),
                dec!(10),
                CollateralAmount::from_atomic(3_000_000),
            ),
            Err(ProjectionReducerError::CashMismatch)
        ));
    }

    fn finalized_fill_event(
        account_id: &AccountId,
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        seq: u64,
        prepared_seq: u64,
    ) -> LiveJournalEvent {
        let polygon_response = |endpoint_kind: &str,
                                rpc_method: &str,
                                rpc_params: serde_json::Value,
                                body: Vec<u8>| {
            RawHttpAttempt::Response(pe_core_types::RawHttpResponse {
                source_id: "polygon-receipt-rpc".to_owned(),
                endpoint_kind: endpoint_kind.to_owned(),
                method: "POST".to_owned(),
                path: "fixture://polygon".to_owned(),
                ordered_query: vec![
                    ("rpc_method".to_owned(), rpc_method.to_owned()),
                    ("rpc_params".to_owned(), rpc_params.to_string()),
                ],
                status: 200,
                headers: Vec::new(),
                body,
                attempt_ordinal: 1,
                source_at: None,
                observed_at: OffsetDateTime::UNIX_EPOCH,
                received_at: OffsetDateTime::UNIX_EPOCH,
                schema_version: 1,
                parser_version: 1,
                adapter_version: "fixture".to_owned(),
            })
        };
        let transaction_hash = format!("0x{}", "11".repeat(32));
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: OffsetDateTime::from_unix_timestamp(10).unwrap(),
            payload: LiveJournalPayload::OrderFillFinalized(Box::new(OrderFillFinalizedAudit {
                identity: prepared.identity.clone(),
                prepared_journal_seq: prepared_seq,
                principal: CollateralAmount::from_atomic(2_500_000),
                quantity: ShareAmount::from_atomic(3_125_000),
                fee: CollateralAmount::from_atomic(120),
                matched_logs: vec![MatchedLogIdentity {
                    transaction_hash: transaction_hash.clone(),
                    log_index: 7,
                }],
                chain_id: 137,
                finalized_head: 100,
                receipts: vec![
                    polygon_response(
                        "polygon-chain-id",
                        "eth_chainId",
                        serde_json::json!([]),
                        br#"{"jsonrpc":"2.0","id":1,"result":"0x89"}"#.to_vec(),
                    ),
                    polygon_response(
                        "polygon-transaction-receipt",
                        "eth_getTransactionReceipt",
                        serde_json::json!([transaction_hash]),
                        include_bytes!(
                            "../../venue-polymarket/tests/fixtures/receipts/standard_v2.json"
                        )
                        .to_vec(),
                    ),
                ],
                blocks: vec![polygon_response(
                    "polygon-finalized-block",
                    "eth_getBlockByNumber",
                    serde_json::json!(["finalized", false]),
                    serde_json::to_vec(&serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "number": "0x64",
                            "hash": format!("0x{}", "aa".repeat(32)),
                        }
                    }))
                    .unwrap(),
                )],
            })),
        }
    }

    fn resolution_event(account_id: &AccountId, seq: u64) -> LiveJournalEvent {
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: OffsetDateTime::from_unix_timestamp(20).unwrap(),
            payload: LiveJournalPayload::ResolutionFinalized(Box::new(
                pe_execution_core::ResolutionFinalizedAudit {
                    condition_id: PolymarketConditionId(format!("0x{}", "77".repeat(32))),
                    payout_by_outcome_index_json: BinaryPayoutVector::winner(0)
                        .unwrap()
                        .canonical_json(),
                    source_append_receipt: AppendReceipt {
                        sequence: EventSeq(2),
                        this_hash: blake3::Hash::from_bytes([2; 32]),
                    },
                },
            )),
        }
    }

    fn resolution_source_envelope(event: &LiveJournalEvent) -> EventEnvelope {
        let LiveJournalPayload::ResolutionFinalized(resolution) = &event.payload else {
            unreachable!();
        };
        let payout =
            BinaryPayoutVector::from_canonical_json(&resolution.payout_by_outcome_index_json)
                .unwrap();
        let [first, second] = *payout.decimals();
        let is_half = first == dec!(0.5) && second == dec!(0.5);
        let payload = serde_json::to_vec(&serde_json::json!({
            "active": false,
            "closed": true,
            "condition_id": resolution.condition_id.0,
            "end_date_iso": "2024-09-10T00:00:00Z",
            "is_50_50_outcome": is_half,
            "tokens": [
                {"token_id":"123", "outcome":"Yes", "price":first, "winner":first == Decimal::ONE},
                {"token_id":"124", "outcome":"No", "price":second, "winner":second == Decimal::ONE}
            ]
        }))
        .unwrap();
        EventEnvelope {
            seq: resolution.source_append_receipt.sequence,
            source_id: SourceId("polymarket.clob.resolution".to_owned()),
            schema_version: pe_source_polymarket_public::CLOB_RESOLUTION_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::CLOB_RESOLUTION_PARSER_VERSION,
            observed_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            received_at: ReceivedAt(OffsetDateTime::UNIX_EPOCH),
            content_type: ContentType::Json,
            raw_payload_hash: blake3::hash(&payload),
            prev_hash: blake3::Hash::from_bytes([1; 32]),
            this_hash: resolution.source_append_receipt.this_hash,
            payload,
        }
    }

    /// PASS: a finalized fill entirely before the account's first Baseline causes no resolution
    /// GET and appends no current-era `ResolutionFinalized` fact, even when the loopback endpoint
    /// would report the market resolved.
    /// FAIL: resolution discovery observes the pre-Baseline fill, reaches the loopback server, or
    /// grows the journal.
    #[tokio::test]
    async fn redemption_recovery_ignores_pre_baseline_fills() {
        async fn resolved_market(State(hits): State<Arc<AtomicUsize>>) -> Json<serde_json::Value> {
            hits.fetch_add(1, Ordering::SeqCst);
            Json(serde_json::json!({
                "active": false,
                "closed": true,
                "condition_id": format!("0x{}", "77".repeat(32)),
                "tokens": [
                    {"token_id":"123", "outcome":"Yes", "price":"1", "winner":true},
                    {"token_id":"124", "outcome":"No", "price":"0", "winner":false}
                ]
            }))
        }

        let hits = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .fallback(get(resolved_market))
            .with_state(Arc::clone(&hits));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let snapshot = LiveAccountsSnapshot::from_rows(Vec::new(), &[]);
        let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
        state.config.clob_base_url = format!("http://{address}");
        let account_id = AccountId::new("pre-baseline-redemption").unwrap();
        let prepared = finality_prepared();
        let prepared_event = state
            .config
            .journal
            .append(
                account_id.clone(),
                OffsetDateTime::from_unix_timestamp(1).unwrap(),
                LiveJournalPayload::OrderPrepared(prepared.clone()),
            )
            .unwrap();
        let finalized = finalized_fill_event(
            &account_id,
            &prepared,
            prepared_event.seq.saturating_add(1),
            prepared_event.seq,
        );
        state
            .config
            .journal
            .append(
                account_id.clone(),
                OffsetDateTime::from_unix_timestamp(2).unwrap(),
                finalized.payload,
            )
            .unwrap();
        append_recovery_baseline(
            &state,
            &account_id,
            OffsetDateTime::from_unix_timestamp(3).unwrap(),
            CollateralAmount::from_atomic(10_000_000),
        )
        .await;
        let before = replay_account(&state.config.journal_path, &account_id)
            .unwrap()
            .len();
        let events = replay_live_account(&state, &account_id).unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0].payload,
            LiveJournalPayload::AccountPortfolioMarked(_)
        ));

        discover_live_resolutions(
            &state,
            &account_id,
            &events,
            &mut BTreeMap::new(),
            OffsetDateTime::from_unix_timestamp(4).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert_eq!(
            replay_account(&state.config.journal_path, &account_id)
                .unwrap()
                .len(),
            before
        );
        server.abort();
    }

    /// PASS: live resolution floors an aggregate half payout once and credits a losing outcome zero.
    #[test]
    fn live_resolution_half_payout_and_loser_are_exact() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let mut half_fill = finalized_fill_event(&account_id, &prepared, 5, 3);
        if let LiveJournalPayload::OrderFillFinalized(fill) = &mut half_fill.payload {
            fill.quantity = ShareAmount::from_atomic(3_125_001);
            if let Some(RawHttpAttempt::Response(response)) = fill.receipts.get_mut(1) {
                response.body = receipt_with_word(3, 3_125_001);
            }
        }
        let mut half_resolution = resolution_event(&account_id, 5);
        if let LiveJournalPayload::ResolutionFinalized(resolution) = &mut half_resolution.payload {
            resolution.payout_by_outcome_index_json =
                BinaryPayoutVector::fifty_fifty().canonical_json();
        }
        let half_source = resolution_source_envelope(&half_resolution);
        let half = derive_with_baseline_evidence(
            &account_id,
            &[
                baseline_event(&account_id, 1),
                approved_admission_event(
                    &account_id,
                    &prepared,
                    2,
                    OffsetDateTime::from_unix_timestamp(5).unwrap(),
                ),
                LiveJournalEvent {
                    account_id: account_id.clone(),
                    seq: 3,
                    timestamp: OffsetDateTime::from_unix_timestamp(5).unwrap(),
                    payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
                },
                finality_matched_event(
                    &account_id,
                    &prepared,
                    3,
                    OffsetDateTime::from_unix_timestamp(6).unwrap(),
                ),
                half_fill,
                half_resolution,
            ],
            &[half_source],
        )
        .unwrap();
        assert_eq!(half.receivable, dec!(1.562500));
        assert_eq!(half.economic_cash, Some(dec!(9.062380)));

        let mut losing_resolution = resolution_event(&account_id, 5);
        if let LiveJournalPayload::ResolutionFinalized(resolution) = &mut losing_resolution.payload
        {
            resolution.payout_by_outcome_index_json =
                BinaryPayoutVector::winner(1).unwrap().canonical_json();
        }
        let losing_source = resolution_source_envelope(&losing_resolution);
        let losing = derive_with_baseline_evidence(
            &account_id,
            &[
                baseline_event(&account_id, 1),
                approved_admission_event(
                    &account_id,
                    &prepared,
                    2,
                    OffsetDateTime::from_unix_timestamp(5).unwrap(),
                ),
                LiveJournalEvent {
                    account_id: account_id.clone(),
                    seq: 3,
                    timestamp: OffsetDateTime::from_unix_timestamp(5).unwrap(),
                    payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
                },
                finality_matched_event(
                    &account_id,
                    &prepared,
                    4,
                    OffsetDateTime::from_unix_timestamp(6).unwrap(),
                ),
                finalized_fill_event(&account_id, &prepared, 5, 3),
                losing_resolution,
            ],
            &[losing_source],
        )
        .unwrap();
        assert_eq!(losing.receivable, Decimal::ZERO);
        assert_eq!(losing.economic_cash, Some(dec!(7.499880)));
    }

    /// PASS: a post-cutoff fill remains part of the Daily event's current custody state, so an
    /// otherwise valid empty response cannot hide it behind the cutoff-bounded projection.
    #[test]
    fn daily_rejects_empty_current_inventory_that_hides_a_post_cutoff_fill() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let binding =
            account_binding_fixture(&account_id, "0x1111111111111111111111111111111111111111");
        let daily_at = OffsetDateTime::from_unix_timestamp(86_402).unwrap();
        let (position_evidence, position_sources) =
            empty_position_evidence(120, daily_at, &binding);
        let mut fill = finalized_fill_event(&account_id, &prepared, 5, 3);
        fill.timestamp = OffsetDateTime::from_unix_timestamp(86_401).unwrap();
        let daily = LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 5,
            timestamp: daily_at,
            payload: LiveJournalPayload::AccountPortfolioMarked(Box::new(
                pe_execution_core::AccountPortfolioMarkedAudit {
                    kind: MarkKind::Daily,
                    cutoff_unix: 86_400,
                    account_binding: binding.clone(),
                    account_state: account_state_fixture_for_binding(
                        CollateralAmount::from_decimal_exact(dec!(7.499880)).unwrap(),
                        daily_at,
                        &binding,
                    ),
                    venue_positions: Vec::new(),
                    venue_position_evidence: position_evidence,
                    marked_positions: Vec::new(),
                    prices: Vec::new(),
                    equity: CollateralAmount::from_whole(10).unwrap(),
                },
            )),
        };
        let events = vec![
            baseline_event(&account_id, 1),
            approved_admission_event(
                &account_id,
                &prepared,
                2,
                OffsetDateTime::from_unix_timestamp(5).unwrap(),
            ),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 3,
                timestamp: OffsetDateTime::from_unix_timestamp(86_401).unwrap(),
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
            finality_matched_event(
                &account_id,
                &prepared,
                3,
                OffsetDateTime::from_unix_timestamp(86_401).unwrap(),
            ),
            fill,
            daily,
        ];
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &events, &position_sources),
            Err(ProjectionReducerError::CustodyInventoryMismatch)
        ));
    }

    /// PASS: a catch-up Daily mark rejects an absent price preimage, then accepts the real
    /// classified response while ignoring a resolution received after its cutoff.
    #[test]
    fn daily_mark_replays_the_cutoff_bounded_financial_view() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let mut resolution = resolution_event(&account_id, 5);
        resolution.timestamp = OffsetDateTime::from_unix_timestamp(86_401).unwrap();
        let source = resolution_source_envelope(&resolution);
        let account_binding =
            account_binding_fixture(&account_id, "0x1111111111111111111111111111111111111111");
        let current_position = CanonicalPositionAudit {
            condition_id: prepared.prepared.condition_id.clone(),
            outcome_index: 0,
            token_id: prepared.prepared.token_id.clone(),
            size: ShareAmount::from_atomic(3_125_000),
            redeemable: true,
        };
        let redeemable_body = serde_json::to_vec(&serde_json::json!([{
            "proxyWallet": account_binding.custody_wallet,
            "asset": prepared.prepared.token_id.0,
            "conditionId": prepared.prepared.condition_id.0,
            "outcomeIndex": 0,
            "size": "3.125",
            "negativeRisk": false,
        }]))
        .unwrap();
        let (daily_position_evidence, daily_position_sources) = position_evidence(
            120,
            OffsetDateTime::from_unix_timestamp(86_402).unwrap(),
            &account_binding,
            [b"[]".to_vec(), redeemable_body],
        );
        let cash = CollateralAmount::from_decimal_exact(dec!(7.499880)).unwrap();
        let daily = LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 6,
            timestamp: OffsetDateTime::from_unix_timestamp(86_402).unwrap(),
            payload: LiveJournalPayload::AccountPortfolioMarked(Box::new(
                pe_execution_core::AccountPortfolioMarkedAudit {
                    kind: MarkKind::Daily,
                    cutoff_unix: 86_400,
                    account_binding: account_binding.clone(),
                    account_state: account_state_fixture_for_binding(
                        cash,
                        OffsetDateTime::from_unix_timestamp(86_402).unwrap(),
                        &account_binding,
                    ),
                    venue_positions: vec![current_position],
                    venue_position_evidence: daily_position_evidence,
                    marked_positions: vec![CanonicalPositionAudit {
                        condition_id: prepared.prepared.condition_id.clone(),
                        outcome_index: 0,
                        token_id: prepared.prepared.token_id.clone(),
                        size: ShareAmount::from_atomic(3_125_000),
                        redeemable: false,
                    }],
                    prices: vec![pe_execution_core::MarkPrice {
                        condition_id: prepared.prepared.condition_id.clone(),
                        outcome_index: 0,
                        price: Price::new(dec!(0.5)).unwrap(),
                        receipt: fixture_receipt(9),
                        observed_unix: 86_400,
                    }],
                    equity: CollateralAmount::from_decimal_exact(dec!(9.062380)).unwrap(),
                },
            )),
        };
        let events = vec![
            baseline_event(&account_id, 1),
            approved_admission_event(
                &account_id,
                &prepared,
                2,
                OffsetDateTime::from_unix_timestamp(5).unwrap(),
            ),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 3,
                timestamp: OffsetDateTime::from_unix_timestamp(5).unwrap(),
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
            finality_matched_event(
                &account_id,
                &prepared,
                4,
                OffsetDateTime::from_unix_timestamp(6).unwrap(),
            ),
            finalized_fill_event(&account_id, &prepared, 5, 3),
            resolution,
            daily,
        ];
        let mut retained_sources = vec![source.clone()];
        retained_sources.extend(daily_position_sources.clone());
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &events, &retained_sources),
            Err(ProjectionReducerError::InvalidPriceEvidence)
        ));
        let price_source = source_envelope(
            fixture_receipt(9),
            "pe-service.clob-prices-history",
            br#"{"history":[{"t":86400,"p":"0.5"}]}"#.to_vec(),
            OffsetDateTime::from_unix_timestamp(86_402).unwrap(),
        );
        let mut copied_price_events = events.clone();
        if let Some(LiveJournalEvent {
            payload: LiveJournalPayload::AccountPortfolioMarked(mark),
            ..
        }) = copied_price_events.last_mut()
            && let Some(price) = mark.prices.first_mut()
        {
            price.price = Price::new(dec!(0.6)).unwrap();
        }
        let mut copied_sources = vec![source.clone(), price_source.clone()];
        copied_sources.extend(daily_position_sources.clone());
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &copied_price_events, &copied_sources),
            Err(ProjectionReducerError::InvalidPriceEvidence)
        ));
        let mut invalid_hash_source = price_source.clone();
        invalid_hash_source.raw_payload_hash = blake3::hash(b"different");
        let mut invalid_hash_sources = vec![source.clone(), invalid_hash_source];
        invalid_hash_sources.extend(daily_position_sources.clone());
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &events, &invalid_hash_sources),
            Err(ProjectionReducerError::InvalidPriceEvidence)
        ));
        let mut copied_historical_inventory = events.clone();
        if let Some(LiveJournalEvent {
            payload: LiveJournalPayload::AccountPortfolioMarked(mark),
            ..
        }) = copied_historical_inventory.last_mut()
        {
            mark.venue_positions = mark.marked_positions.clone();
        }
        let mut copied_inventory_sources = vec![source.clone(), price_source.clone()];
        copied_inventory_sources.extend(daily_position_sources.clone());
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &copied_historical_inventory,
                &copied_inventory_sources,
            ),
            Err(ProjectionReducerError::CustodyInventoryMismatch)
        ));
        let mut accepted_sources = vec![source, price_source];
        accepted_sources.extend(daily_position_sources);
        let derived =
            derive_with_baseline_evidence(&account_id, &events, &accepted_sources).unwrap();
        assert_eq!(
            derived.daily_marks.get(&86_400),
            Some(&CollateralAmount::from_decimal_exact(dec!(9.062380)).unwrap())
        );
        assert_eq!(derived.economic_cash, Some(dec!(10.624880)));
    }

    /// PASS: removing an account from configuration retains its journal-frozen recovery binding.
    #[test]
    fn journal_account_recovery_does_not_require_the_account_projection() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let recovered = journal_recovery_account(
            &account_id,
            &[LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 1,
                timestamp: OffsetDateTime::UNIX_EPOCH,
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            }],
        )
        .unwrap();
        assert_eq!(recovered.account_id, account_id);
        assert_eq!(
            recovered.credential_binding,
            Some((
                prepared.frozen_binding.version,
                prepared.frozen_binding.key_id.clone()
            ))
        );
        assert_eq!(
            recovered.custody_wallet_address.as_deref(),
            Some(prepared.prepared.funder.as_str())
        );
        assert!(!recovered.is_armed());
    }

    /// PASS: resolution changes economics once; custody reconciliation changes no equity.
    #[test]
    fn finalized_resolution_and_custody_replay_converge_once() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let repeated_fill = finalized_fill_event(&account_id, &prepared, 6, 3);
        let mut events = vec![
            baseline_event(&account_id, 1),
            approved_admission_event(
                &account_id,
                &prepared,
                2,
                OffsetDateTime::from_unix_timestamp(5).unwrap(),
            ),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 3,
                timestamp: OffsetDateTime::from_unix_timestamp(5).unwrap(),
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
            finality_matched_event(
                &account_id,
                &prepared,
                4,
                OffsetDateTime::from_unix_timestamp(6).unwrap(),
            ),
            finalized_fill_event(&account_id, &prepared, 5, 3),
        ];
        let filled = derive_with_baseline_evidence(&account_id, &events, &[]).unwrap();
        assert_eq!(filled.positions.len(), 1);
        assert!(filled.positions.first().is_some_and(|position| {
            position.long_contracts == dec!(3.125000)
                && position.short_contracts == Decimal::ZERO
                && position.cost_basis == dec!(2.500120)
        }));
        let mut conflicting_fill = repeated_fill.clone();
        if let LiveJournalPayload::OrderFillFinalized(finalized) = &mut conflicting_fill.payload {
            finalized.finalized_head = 101;
        }
        let mut conflicting_events = events.clone();
        conflicting_events.push(conflicting_fill);
        assert!(derive_with_baseline_evidence(&account_id, &conflicting_events, &[]).is_err());

        let mut invented_fill = finalized_fill_event(&account_id, &prepared, 6, 3);
        if let LiveJournalPayload::OrderFillFinalized(finalized) = &mut invented_fill.payload {
            finalized.receipts.clear();
            finalized.blocks.clear();
        }
        let mut invented_events = events.clone();
        invented_events.push(invented_fill);
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &invented_events, &[]),
            Err(ProjectionReducerError::InvalidFinalityEvidence)
        ));

        let first_resolution = resolution_event(&account_id, 6);
        let resolution_source = resolution_source_envelope(&first_resolution);
        events.extend([
            repeated_fill,
            first_resolution,
            resolution_event(&account_id, 7),
        ]);
        let resolved = derive_with_baseline_evidence(
            &account_id,
            &events,
            std::slice::from_ref(&resolution_source),
        )
        .unwrap();
        assert_eq!(resolved.economic_cash, Some(dec!(10.624880)));
        assert_eq!(resolved.receivable, dec!(3.125000));
        assert!(resolved.positions.is_empty());
        assert_eq!(resolved.custody_positions.len(), 1);
        assert!(
            resolved
                .custody_positions
                .first()
                .is_some_and(|position| position.redeemable)
        );

        let reconciled_cash = CollateralAmount::from_decimal_exact(dec!(10.624880)).unwrap();
        let redemption_identity = RedemptionAttemptIdentity {
            account_id: account_id.clone(),
            condition_id: PolymarketConditionId(format!("0x{}", "77".repeat(32))),
            adapter: "adapter".to_owned(),
            custody_wallet: prepared.prepared.funder.clone(),
        };
        let custody_binding = account_binding_fixture(&account_id, &prepared.prepared.funder);
        let (custody_position_evidence, custody_sources) = empty_position_evidence(
            110,
            OffsetDateTime::from_unix_timestamp(30).unwrap(),
            &custody_binding,
        );
        let custody_audit = pe_execution_core::RedemptionCustodyReconciledAudit {
            identity: redemption_identity.clone(),
            account_state: account_state_fixture_for_binding(
                reconciled_cash,
                OffsetDateTime::from_unix_timestamp(30).unwrap(),
                &custody_binding,
            ),
            venue_positions: Vec::new(),
            venue_position_receipts: custody_position_evidence
                .pages
                .iter()
                .map(|page| page.receipt)
                .collect(),
        };
        events.push(LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 8,
            timestamp: OffsetDateTime::from_unix_timestamp(30).unwrap(),
            payload: LiveJournalPayload::RedemptionCustodyReconciled(Box::new(
                custody_audit.clone(),
            )),
        });
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &events,
                std::slice::from_ref(&resolution_source)
            ),
            Err(ProjectionReducerError::CustodyWithoutConfirmedRedemption)
        ));
        events.pop();
        events.extend([
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 8,
                timestamp: OffsetDateTime::from_unix_timestamp(28).unwrap(),
                payload: LiveJournalPayload::RedemptionTransactionIdentified(Box::new(
                    pe_execution_core::RedemptionTransactionAudit {
                        identity: redemption_identity.clone(),
                        attempt_count: 1,
                        transaction_id: "tx-1".to_owned(),
                        submit_body_hash: "body".to_owned(),
                        evidence: Vec::new(),
                        evidence_hashes: Vec::new(),
                    },
                )),
            },
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 9,
                timestamp: OffsetDateTime::from_unix_timestamp(29).unwrap(),
                payload: LiveJournalPayload::RedemptionReceiptTransition(Box::new(
                    pe_execution_core::RedemptionReceiptAudit {
                        identity: redemption_identity,
                        attempt_count: 1,
                        transaction_id: "tx-1".to_owned(),
                        transaction_hash: Some(format!("0x{}", "99".repeat(32))),
                        status: pe_execution_core::RedemptionReceiptStatusAudit::Confirmed,
                        evidence: Vec::new(),
                        evidence_hashes: Vec::new(),
                    },
                )),
            },
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 10,
                timestamp: OffsetDateTime::from_unix_timestamp(30).unwrap(),
                payload: LiveJournalPayload::RedemptionCustodyReconciled(Box::new(
                    custody_audit.clone(),
                )),
            },
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 11,
                timestamp: OffsetDateTime::from_unix_timestamp(30).unwrap(),
                payload: LiveJournalPayload::RedemptionCustodyReconciled(Box::new(custody_audit)),
            },
        ]);
        let account_b = AccountId::new("account-b").unwrap();
        let binding_b = account_binding_fixture(&account_b, &prepared.prepared.funder);
        let (_, account_b_custody_sources) = empty_position_evidence(
            110,
            OffsetDateTime::from_unix_timestamp(30).unwrap(),
            &binding_b,
        );
        let mut substituted_custody_sources = vec![resolution_source.clone()];
        substituted_custody_sources.extend(account_b_custody_sources);
        assert!(matches!(
            derive_with_baseline_evidence(&account_id, &events, &substituted_custody_sources),
            Err(ProjectionReducerError::InvalidPositionEvidence)
        ));
        let mut custody_and_resolution_sources = vec![resolution_source];
        custody_and_resolution_sources.extend(custody_sources);
        let mut copied_position_events = events.clone();
        for event in &mut copied_position_events {
            if let LiveJournalPayload::RedemptionCustodyReconciled(custody) = &mut event.payload {
                custody.venue_positions.push(CanonicalPositionAudit {
                    condition_id: prepared.prepared.condition_id.clone(),
                    outcome_index: 0,
                    token_id: prepared.prepared.token_id.clone(),
                    size: ShareAmount::from_atomic(1),
                    redeemable: false,
                });
            }
        }
        assert!(matches!(
            derive_with_baseline_evidence(
                &account_id,
                &copied_position_events,
                &custody_and_resolution_sources
            ),
            Err(ProjectionReducerError::CustodyInventoryMismatch)
        ));
        let custody =
            derive_with_baseline_evidence(&account_id, &events, &custody_and_resolution_sources)
                .unwrap();
        assert_eq!(custody.economic_cash, resolved.economic_cash);
        assert_eq!(custody.receivable, Decimal::ZERO);
        assert!(custody.custody_positions.is_empty());
    }

    /// PASS: account-state projection is composed only from strict reducer outputs.
    #[test]
    fn single_account_state_writer_uses_only_reducer_inputs() {
        let derived = ProjectionDerivation {
            reserved: dec!(2.50),
            receivable: dec!(3),
            latest_free_collateral: Some(dec!(12.50)),
            latest_reconciled_at: Some("2026-08-11T12:00:00Z".to_owned()),
            ..ProjectionDerivation::default()
        };
        let row =
            compose_account_state_row("account", &derived, Some("redemption pending".to_owned()))
                .unwrap();
        assert_eq!(row.free_collateral, dec!(12.50));
        assert_eq!(row.reserved, dec!(2.50));
        assert_eq!(row.unredeemed_value, dec!(3));
        assert_eq!(
            row.admission_closed_reason.as_deref(),
            Some("redemption pending")
        );
    }

    #[test]
    fn reconstructed_ambiguous_attempt_reconciles_before_nonce_path() {
        let state = RedemptionAttemptState::Ambiguous {
            attempt_count: 1,
            transaction_id: Some("tx-existing".to_owned()),
            submit_body_hash: "body".to_owned(),
        };
        assert!(!redemption_needs_fresh_submission(
            &state,
            OffsetDateTime::UNIX_EPOCH
        ));
    }

    #[test]
    fn target_classification_orders_recovery_before_every_gate() {
        // In-flight targets recover regardless of freshness, arming, or closures (#514):
        // the order may exist on the venue, so no gate may terminalize or bypass it.
        for state in ["submitted", "ambiguous"] {
            for fresh in [false, true] {
                for armed in [false, true] {
                    for closed in [false, true] {
                        assert_eq!(
                            classify_target(state, fresh, armed, closed),
                            TargetClass::RecoverInFlight,
                            "{state} fresh={fresh} armed={armed} closed={closed}"
                        );
                    }
                }
            }
        }
        // A pending target is paused while blind — staleness precedes the arming and
        // closure gates, so a dead poller can never terminalize or dispatch it.
        assert_eq!(
            classify_target("pending", false, true, false),
            TargetClass::PauseStale
        );
        assert_eq!(
            classify_target("pending", false, false, true),
            TargetClass::PauseStale
        );
        // Only a FRESH snapshot may terminalize a missing/unarmed pending target.
        assert_eq!(
            classify_target("pending", true, false, false),
            TargetClass::NotArmed
        );
        assert_eq!(
            classify_target("pending", true, true, true),
            TargetClass::PauseClosed
        );
        assert_eq!(
            classify_target("pending", true, true, false),
            TargetClass::Dispatch
        );
    }

    fn armed_account_row(id: &str) -> AccountRow {
        AccountRow {
            account_id: id.to_owned(),
            is_primary: true,
            enabled: true,
            execution_order: 0,
            requested_live_mode: "live_tiny".to_owned(),
            effective_live_mode: "live_tiny".to_owned(),
            live_price_impact_cap_bps: 100,
            custody_wallet_address: None,
            custody_wallet_kind: None,
        }
    }

    fn armed_snapshot(id: &str) -> LiveAccountsSnapshot {
        LiveAccountsSnapshot::from_rows(
            vec![armed_account_row(id)],
            &[CredentialMetaRow {
                account_id: id.to_owned(),
                bundle_version: 1,
                key_id: "key".to_owned(),
            }],
        )
    }

    fn fanout_state(
        dir: &tempfile::TempDir,
        paper_state: Arc<PaperStateDb>,
        snapshot: LiveAccountsSnapshot,
        supabase_url: &str,
        identity: Option<Identity>,
    ) -> FanoutState {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let journal_path = dir.path().join("live.journal");
        let journal = Arc::new(LiveJournal::open(&journal_path).unwrap());
        let (source_log, source_rx) = SourceLogHandle::channel(8);
        let source_log_path = dir.path().join("live-source.log");
        let source_sink =
            crate::source_event_sink::SourceEventSink::open(&source_log_path).unwrap();
        let source_receipts = SourceReceiptIndex::replay(&source_log_path).unwrap();
        let paper_log_path = dir.path().join("paper.log");
        drop(pe_event_log::Writer::open(&paper_log_path).unwrap());
        let (trigger_tx, mut trigger_rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move { while trigger_rx.recv().await.is_some() {} });
        let (orchestrator_control, _orchestrator_control_rx) = tokio::sync::mpsc::channel(1);
        let source_health = crate::health::new_shared_health_with_ws(false, true, 90);
        tokio::spawn(
            crate::activity_ingest::ActivityIngest::poll_only(
                source_sink,
                source_rx,
                trigger_tx,
                source_health,
            )
            .with_source_receipt_index(source_receipts.clone())
            .run(),
        );
        let admission = LiveAdmissionBuilder::new(
            http.clone(),
            "http://127.0.0.1:9".to_owned(),
            "http://127.0.0.1:9".to_owned(),
            source_log.clone(),
        );
        FanoutState {
            config: LiveFanoutConfig {
                paper_state,
                live_accounts: LiveAccounts::new(snapshot),
                live_watchlist: LiveWatchlist::new(Watchlist {
                    entries: Vec::new(),
                    snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
                    active_count: 0,
                    incubator_count: 0,
                }),
                runtime_config: LiveRuntimeConfig::new(RuntimeConfig::from_service_config(
                    &ServiceConfig::default(),
                )),
                qualification: None,
                identity,
                journal,
                journal_path,
                era_live_prefix: None,
                projection: LiveProjectionWriter::new(http.clone(), supabase_url, "anon", ""),
                book_fetcher: Arc::new(ReqwestClobBookFetcher::new(http.clone())),
                mid_price_cache: MidPriceCache::new("http://127.0.0.1:9".to_owned()),
                source_log: source_log.clone(),
                source_receipts,
                paper_log_path,
                orchestrator_control,
                http: http.clone(),
                polygon_receipt_rpc_url: "http://127.0.0.1:9".to_owned(),
                supabase_url: supabase_url.to_owned(),
                supabase_anon_key: "anon".to_owned(),
                supabase_secret_key: String::new(),
                gamma_base_url: "http://127.0.0.1:9".to_owned(),
                clob_base_url: "http://127.0.0.1:9".to_owned(),
                data_base_url: "http://127.0.0.1:9".to_owned(),
                projection_reconcile_interval_secs: 3_600,
                shutdown: ShutdownController::new().0,
            },
            admission,
            polygon_receipt_rpc: PolygonReceiptRpc::new(http, "http://127.0.0.1:9"),
            history_fetcher: HistoricalMarkAdapter::new(
                reqwest::Client::new(),
                "http://127.0.0.1:9",
                source_log,
            ),
            closures: AdmissionClosures::default(),
            last_mode_unix: None,
            last_redemption_unix: None,
            last_projection_unix: None,
            last_prune_unix: None,
        }
    }

    fn bind_empty_live_risk_to_paper_prefix(
        state: &FanoutState,
        prepared: &mut pe_execution_core::LiveOrderPreparedAudit,
        now: OffsetDateTime,
    ) {
        let mut writer = pe_event_log::Writer::open(&state.config.paper_log_path).unwrap();
        let financial_prefix = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: crate::paper_recovery::PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&PaperLogRecord::RiskHaltChanged {
                    owner: RiskHaltOwner::Paper,
                    cause: pe_risk_engine::RiskHaltCause::CopyLatency,
                    state: HaltState::Released,
                    evidence: serde_json::json!({"fixture": "live-risk-prefix"}),
                })
                .unwrap(),
            })
            .unwrap();
        prepared.economic.risk = RiskAudit {
            financial_prefix,
            snapshot: RiskSnapshot {
                leader_exposure_bps: BasisPoints(0),
                market_exposure_bps: BasisPoints(0),
                family_exposure_bps: BasisPoints(0),
                total_copy_exposure_bps: BasisPoints(0),
                intraday_pnl_bps: BasisPoints(0),
                rolling_7d_pnl_bps: BasisPoints(0),
                absolute_pnl_bps: BasisPoints(0),
                copy_latency_kill_switch_active: false,
                proposed_trade_bps: BasisPoints(2_501),
                per_trade_cap_bps: 10_000,
                concentration_caps: None,
            },
            decision: RiskDecisionAudit::Approved,
            price_receipts: Vec::new(),
            evaluated_at_unix_ms: unix_millis_i64(now),
        };
    }

    async fn counting_fallback(
        State(hits): State<Arc<Mutex<Vec<String>>>>,
        uri: axum::http::Uri,
    ) -> Json<Vec<serde_json::Value>> {
        hits.lock().unwrap().push(uri.path().to_owned());
        Json(Vec::new())
    }

    async fn count_order_post(State(posts): State<Arc<AtomicUsize>>) -> StatusCode {
        posts.fetch_add(1, Ordering::SeqCst);
        StatusCode::BAD_REQUEST
    }

    async fn empty_positions() -> Json<Vec<serde_json::Value>> {
        Json(Vec::new())
    }

    async fn recovery_open_position(
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<Vec<serde_json::Value>> {
        if query.get("redeemable").map(String::as_str) == Some("false") {
            return Json(vec![serde_json::json!({
                "proxyWallet": "0x1111111111111111111111111111111111111111",
                "asset": "123",
                "conditionId": finality_prepared().economic.market.condition_id.0,
                "outcomeIndex": 0,
                "size": "3.125",
                "negativeRisk": false,
            })]);
        }
        Json(Vec::new())
    }

    async fn recovery_positions(
        State(include_unknown): State<bool>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<Vec<serde_json::Value>> {
        if include_unknown && query.get("redeemable").map(String::as_str) == Some("false") {
            return Json(vec![serde_json::json!({
                "proxyWallet": "0x1111111111111111111111111111111111111111",
                "asset": "999",
                "conditionId": format!("0x{}", "99".repeat(32)),
                "outcomeIndex": 0,
                "size": "0.000001",
                "negativeRisk": false,
            })]);
        }
        Json(Vec::new())
    }

    /// PASS: unknown venue inventory, one-atomic cash drift, insufficient allowance, and a
    /// transient authenticated read all stop Approved recovery before any POST.
    /// FAIL: any of the four drift/allowance/transient cases reaches a loopback POST, or the
    /// drift-free armed case posts other than exactly once.
    #[tokio::test]
    async fn recovery_fresh_portfolio_and_allowance_gate_zero_posts() {
        for case in ["unknown_position", "cash_drift", "allowance", "transient"] {
            let app = Router::new()
                .route("/positions", get(recovery_positions))
                .with_state(case == "unknown_position");
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let dir = tempdir().unwrap();
            let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
            let mut snapshot = armed_snapshot("acct");
            snapshot.fetched_at_unix = Some(now.unix_timestamp());
            let mut state = fanout_state(&dir, db.clone(), snapshot, "http://127.0.0.1:9", None);
            state.config.data_base_url = format!("http://{address}");
            let (pending, prepared) = stage_recoverable_approved(&mut state, now, None).await;
            let mut current_account_state =
                venue_account_state_from_audit(pending.admission.account_state.as_ref().unwrap());
            if case == "cash_drift" {
                current_account_state.collateral_balance = CollateralAmount::from_atomic(9_999_999);
                current_account_state.reconciled_free_collateral =
                    CollateralAmount::from_atomic(9_999_999);
            } else if case == "allowance" {
                current_account_state.allowance = CollateralAmount::from_atomic(2_500_119);
            }
            let account_binding = account_binding_fixture(
                &pending.account_id,
                "0x1111111111111111111111111111111111111111",
            );
            let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
            let posts = Arc::new(AtomicUsize::new(0));
            let venue = RecoveryPostVenue {
                client: reqwest::Client::new(),
                order_url: "http://127.0.0.1:9/order".to_owned(),
                prepared,
                observed_at: now,
                prepare_clock_millis: None,
                prepare_advance_millis: 0,
                post_attempts: Some(posts.clone()),
                account_state: (case != "transient").then_some(current_account_state.clone()),
                prepare_barrier: None,
                prepare_release: None,
            };
            let venue_context = RecoveryVenueContext {
                venue: &venue,
                account_binding: &account_binding,
                custody_wallet: "0x1111111111111111111111111111111111111111",
            };
            let gate = fresh_recovery_venue_gate(
                &mut state,
                &pending.account_id,
                &pending.admission.frozen_binding,
                pending.admission.economic.admission.market.neg_risk,
                &venue_context,
                || now,
            )
            .await
            .unwrap();
            if case == "allowance" {
                let RecoveryVenueGate::Ready(execution) = gate else {
                    panic!("allowance case must reach the executor admission gate");
                };
                assert!(
                    !resume_approved_with_venue(
                        &mut state,
                        &target,
                        pending,
                        execution,
                        &venue_context,
                        || now,
                    )
                    .await
                    .unwrap()
                );
            } else {
                assert!(matches!(gate, RecoveryVenueGate::MarkUnavailable), "{case}");
            }
            assert_eq!(posts.load(Ordering::SeqCst), 0, "{case}");
        }
    }

    /// PASS: fresh current off/missing evidence terminalizes Approved-only work, stale or closed
    /// evidence pauses it, and only the fresh armed case reaches exactly one loopback POST.
    /// FAIL: any non-armed, stale, closed, or missing account case reaches a loopback POST, or
    /// the armed case posts more than once.
    #[tokio::test]
    async fn recovery_approved_admission_requires_current_armed_account() {
        let posts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/order", post(count_order_post))
            .route("/positions", get(empty_positions))
            .with_state(posts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();

        let armed_dir = tempdir().unwrap();
        let armed_db = Arc::new(PaperStateDb::open(&armed_dir.path().join("paper.db")).unwrap());
        let mut armed = armed_snapshot("acct");
        armed.fetched_at_unix = Some(now.unix_timestamp());
        let mut armed_state = fanout_state(
            &armed_dir,
            armed_db.clone(),
            armed,
            "http://127.0.0.1:9",
            None,
        );
        let (pending, prepared) = stage_recoverable_approved(&mut armed_state, now, None).await;
        let current_account_state =
            venue_account_state_from_audit(pending.admission.account_state.as_ref().unwrap());
        let account_binding = account_binding_fixture(
            &pending.account_id,
            "0x1111111111111111111111111111111111111111",
        );
        armed_state.config.data_base_url = format!("http://{address}");
        let target = armed_db
            .dispatch_targets("dispatch-finality")
            .unwrap()
            .remove(0);
        let venue = RecoveryPostVenue {
            client: reqwest::Client::new(),
            order_url: format!("http://{address}/order"),
            prepared,
            observed_at: now,
            prepare_clock_millis: None,
            prepare_advance_millis: 0,
            post_attempts: None,
            account_state: Some(current_account_state.clone()),
            prepare_barrier: None,
            prepare_release: None,
        };
        let venue_context = RecoveryVenueContext {
            venue: &venue,
            account_binding: &account_binding,
            custody_wallet: "0x1111111111111111111111111111111111111111",
        };
        assert!(
            !resume_approved_with_venue(
                &mut armed_state,
                &target,
                pending,
                RecoveryExecutionState {
                    mode: LiveModeSnapshot {
                        requested: LiveControlMode::LiveTiny,
                        effective: LiveControlMode::LiveTiny,
                    },
                    account_state: current_account_state,
                    binding: CredentialBindingIdentity {
                        version: 1,
                        key_id: "key".to_owned(),
                    },
                },
                &venue_context,
                || now,
            )
            .await
            .unwrap()
        );
        assert_eq!(posts.load(Ordering::SeqCst), 1);

        for case in ["off", "missing", "stale", "closed"] {
            let dir = tempdir().unwrap();
            let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let mut snapshot = if case == "missing" {
                LiveAccountsSnapshot {
                    accounts: Vec::new(),
                    fetched_at_unix: Some(now.unix_timestamp()),
                }
            } else {
                armed_snapshot("acct")
            };
            snapshot.fetched_at_unix = Some(if case == "stale" {
                now.unix_timestamp() - crate::live_accounts::LIVE_ACCOUNTS_STALE_AFTER_SECS
            } else {
                now.unix_timestamp()
            });
            if case == "off" {
                snapshot.accounts[0].enabled = false;
                snapshot.accounts[0].requested_live_mode = "off".to_owned();
                snapshot.accounts[0].effective_live_mode = "off".to_owned();
            }
            let mut state = fanout_state(&dir, db.clone(), snapshot, "http://127.0.0.1:9", None);
            let _ = stage_recoverable_approved(&mut state, now, None).await;
            if case == "closed" {
                state
                    .closures
                    .mode
                    .insert("acct".to_owned(), "current admission closed".to_owned());
            }
            run_recovery_pass(&mut state, || now, true).await.unwrap();
            let recovery = recovery_inventory(&state.config.journal_path, None).unwrap();
            let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
            if matches!(case, "off" | "missing") {
                assert!(recovery.approved_admissions.is_empty(), "{case}");
                assert_eq!(target.state, "terminal", "{case}");
                assert_eq!(
                    target.terminal_reason.as_deref(),
                    Some("recovery_account_not_armed"),
                    "{case}"
                );
            } else {
                assert_eq!(recovery.approved_admissions.len(), 1, "{case}");
                assert_eq!(target.state, "pending", "{case}");
            }
            assert_eq!(posts.load(Ordering::SeqCst), 1, "{case}");
        }
    }

    /// PASS: a price receipt aged 59,999 ms at recovery start expires when preparation advances
    /// the injected clock by 2 ms, terminalizing Prepared without reaching the loopback POST.
    /// FAIL: the Prepared order reaches a loopback POST, or its terminal fact is stamped before
    /// the post-preparation instant.
    #[tokio::test]
    async fn recovery_rechecks_price_expiry_after_preparation() {
        let posts = Arc::new(AtomicUsize::new(0));
        let app = Router::new().route("/positions", get(recovery_open_position));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let observed_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let pass_started_at = observed_at + time::Duration::milliseconds(59_999);
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let mut snapshot = armed_snapshot("acct");
        snapshot.fetched_at_unix = Some(pass_started_at.unix_timestamp());
        let mut state = fanout_state(&dir, db.clone(), snapshot, "http://127.0.0.1:9", None);
        let (price_receipt, _) = append_recovery_risk_price(&state, observed_at).await;
        let (pending, prepared) =
            stage_recoverable_approved(&mut state, pass_started_at, Some(price_receipt)).await;
        let current_account_state =
            venue_account_state_from_audit(pending.admission.account_state.as_ref().unwrap());
        let account_binding = account_binding_fixture(
            &pending.account_id,
            "0x1111111111111111111111111111111111111111",
        );
        state.config.data_base_url = format!("http://{address}");

        let clock_millis = Arc::new(AtomicI64::new(
            i64::try_from(pass_started_at.unix_timestamp_nanos() / 1_000_000).unwrap(),
        ));
        let venue = RecoveryPostVenue {
            client: reqwest::Client::new(),
            order_url: "http://127.0.0.1:9/order".to_owned(),
            prepared,
            observed_at: pass_started_at,
            prepare_clock_millis: Some(clock_millis.clone()),
            prepare_advance_millis: 2,
            post_attempts: Some(posts.clone()),
            account_state: Some(current_account_state.clone()),
            prepare_barrier: None,
            prepare_release: None,
        };
        let sampled_clock = clock_millis.clone();
        let clock = move || {
            OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(sampled_clock.load(Ordering::SeqCst)) * 1_000_000,
            )
            .unwrap()
        };
        let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
        let venue_context = RecoveryVenueContext {
            venue: &venue,
            account_binding: &account_binding,
            custody_wallet: "0x1111111111111111111111111111111111111111",
        };

        assert!(
            !resume_approved_with_venue(
                &mut state,
                &target,
                pending,
                RecoveryExecutionState {
                    mode: LiveModeSnapshot {
                        requested: LiveControlMode::LiveTiny,
                        effective: LiveControlMode::LiveTiny,
                    },
                    account_state: current_account_state,
                    binding: CredentialBindingIdentity {
                        version: 1,
                        key_id: "key".to_owned(),
                    },
                },
                &venue_context,
                &clock,
            )
            .await
            .unwrap()
        );
        assert_eq!(posts.load(Ordering::SeqCst), 0);
        let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
        assert_eq!(target.state, "terminal");
        let after_preparation = pass_started_at + time::Duration::milliseconds(2);
        let events = replay_live_account(&state, &AccountId::new("acct").unwrap()).unwrap();
        assert!(events.iter().any(|event| {
            matches!(event.payload, LiveJournalPayload::OrderPrepared(_))
                && event.timestamp == after_preparation
        }));
        assert!(events.iter().any(|event| {
            matches!(
                &event.payload,
                LiveJournalPayload::OrderPreparationFailed(failed)
                    if failed.failure
                        == pe_execution_core::LiveOrderPreparationFailure::PrePostRiskPriceExpired
            ) && event.timestamp == after_preparation
        }));
    }

    /// PASS: a ladder aged 1,999 ms before venue preparation is rechecked after preparation
    /// advances the action clock by 2 ms, terminalizing Prepared with zero POST attempts.
    /// FAIL: recovery reuses the pre-prepare freshness result or reaches the POST boundary.
    #[tokio::test]
    async fn recovery_rechecks_ladder_after_slow_preparation() {
        let app = Router::new().route("/positions", get(empty_positions));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let observed_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let pass_started_at = observed_at + time::Duration::milliseconds(1_999);
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let mut snapshot = armed_snapshot("acct");
        snapshot.fetched_at_unix = Some(pass_started_at.unix_timestamp());
        let mut state = fanout_state(&dir, db.clone(), snapshot, "http://127.0.0.1:9", None);
        state.config.data_base_url = format!("http://{address}");
        let (pending, prepared) = stage_recoverable_approved(&mut state, observed_at, None).await;
        let current_account_state =
            venue_account_state_from_audit(pending.admission.account_state.as_ref().unwrap());
        let account_binding = account_binding_fixture(
            &pending.account_id,
            "0x1111111111111111111111111111111111111111",
        );
        let clock_millis = Arc::new(AtomicI64::new(
            i64::try_from(pass_started_at.unix_timestamp_nanos() / 1_000_000).unwrap(),
        ));
        let posts = Arc::new(AtomicUsize::new(0));
        let venue = RecoveryPostVenue {
            client: reqwest::Client::new(),
            order_url: "http://127.0.0.1:9/order".to_owned(),
            prepared,
            observed_at: pass_started_at,
            prepare_clock_millis: Some(clock_millis.clone()),
            prepare_advance_millis: 2,
            post_attempts: Some(posts.clone()),
            account_state: Some(current_account_state.clone()),
            prepare_barrier: None,
            prepare_release: None,
        };
        let sampled_clock = clock_millis.clone();
        let clock = move || {
            OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(sampled_clock.load(Ordering::SeqCst)) * 1_000_000,
            )
            .unwrap()
        };
        let venue_context = RecoveryVenueContext {
            venue: &venue,
            account_binding: &account_binding,
            custody_wallet: "0x1111111111111111111111111111111111111111",
        };
        let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);

        assert!(
            !resume_approved_with_venue(
                &mut state,
                &target,
                pending,
                RecoveryExecutionState {
                    mode: LiveModeSnapshot {
                        requested: LiveControlMode::LiveTiny,
                        effective: LiveControlMode::LiveTiny,
                    },
                    account_state: current_account_state,
                    binding: CredentialBindingIdentity {
                        version: 1,
                        key_id: "key".to_owned(),
                    },
                },
                &venue_context,
                &clock,
            )
            .await
            .unwrap()
        );
        assert_eq!(posts.load(Ordering::SeqCst), 0);
        let after_preparation = pass_started_at + time::Duration::milliseconds(2);
        let events = replay_live_account(&state, &AccountId::new("acct").unwrap()).unwrap();
        assert!(events.iter().any(|event| {
            matches!(event.payload, LiveJournalPayload::OrderPrepared(_))
                && event.timestamp == after_preparation
        }));
        assert!(events.iter().any(|event| {
            matches!(
                &event.payload,
                LiveJournalPayload::OrderPreparationFailed(failed)
                    if failed.failure
                        == pe_execution_core::LiveOrderPreparationFailure::PrePostLadderExpired
            ) && event.timestamp == after_preparation
        }));
    }

    /// PASS: crash recovery sampled three seconds after the recorded book receipt terminalizes
    /// the Approved admission before venue preparation and reaches zero POST attempts.
    /// FAIL: recovery prepares or posts from the expired ladder, or leaves the target pending.
    #[tokio::test]
    async fn recovery_rejects_three_second_old_ladder_before_preparation() {
        let observed_at = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let recovery_at = observed_at + time::Duration::seconds(3);
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let mut snapshot = armed_snapshot("acct");
        snapshot.fetched_at_unix = Some(recovery_at.unix_timestamp());
        let mut state = fanout_state(&dir, db.clone(), snapshot, "http://127.0.0.1:9", None);
        let (pending, prepared) = stage_recoverable_approved(&mut state, observed_at, None).await;
        let current_account_state =
            venue_account_state_from_audit(pending.admission.account_state.as_ref().unwrap());
        let account_binding = account_binding_fixture(
            &pending.account_id,
            "0x1111111111111111111111111111111111111111",
        );
        let posts = Arc::new(AtomicUsize::new(0));
        let venue = RecoveryPostVenue {
            client: reqwest::Client::new(),
            order_url: "http://127.0.0.1:9/order".to_owned(),
            prepared,
            observed_at: recovery_at,
            prepare_clock_millis: None,
            prepare_advance_millis: 0,
            post_attempts: Some(posts.clone()),
            account_state: Some(current_account_state.clone()),
            prepare_barrier: None,
            prepare_release: None,
        };
        let venue_context = RecoveryVenueContext {
            venue: &venue,
            account_binding: &account_binding,
            custody_wallet: "0x1111111111111111111111111111111111111111",
        };
        let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);

        assert!(
            !resume_approved_with_venue(
                &mut state,
                &target,
                pending,
                RecoveryExecutionState {
                    mode: LiveModeSnapshot {
                        requested: LiveControlMode::LiveTiny,
                        effective: LiveControlMode::LiveTiny,
                    },
                    account_state: current_account_state,
                    binding: CredentialBindingIdentity {
                        version: 1,
                        key_id: "key".to_owned(),
                    },
                },
                &venue_context,
                || recovery_at,
            )
            .await
            .unwrap()
        );
        assert_eq!(posts.load(Ordering::SeqCst), 0);
        let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
        assert_eq!(target.state, "terminal");
        assert_eq!(
            target.terminal_reason.as_deref(),
            Some("recovery_ladder_expired")
        );
        let events = replay_live_account(&state, &AccountId::new("acct").unwrap()).unwrap();
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            LiveJournalPayload::OrderPreparationFailed(failed)
                if failed.failure
                    == pe_execution_core::LiveOrderPreparationFailure::PrePostLadderExpired
        )));
    }

    /// PASS: StopProducers reached while venue preparation is parked prevents the POST and
    /// terminalizes the now-durable Prepared capability before it can be consumed.
    /// FAIL: any loopback POST after StopProducers, or the durable Prepared capability is left
    /// consumable.
    #[tokio::test]
    async fn recovery_rechecks_shutdown_after_parked_preparation() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let mut snapshot = armed_snapshot("acct");
        snapshot.fetched_at_unix = Some(now.unix_timestamp());
        let mut state = fanout_state(&dir, db.clone(), snapshot, "http://127.0.0.1:9", None);
        let shutdown = state.config.shutdown.clone();
        let journal_path = state.config.journal_path.clone();
        let (pending, prepared) = stage_recoverable_approved(&mut state, now, None).await;
        let current_account_state =
            venue_account_state_from_audit(pending.admission.account_state.as_ref().unwrap());
        let account_binding = account_binding_fixture(
            &pending.account_id,
            "0x1111111111111111111111111111111111111111",
        );
        let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Notify::new());
        let posts = Arc::new(AtomicUsize::new(0));
        let venue = RecoveryPostVenue {
            client: reqwest::Client::new(),
            order_url: "http://127.0.0.1:9/order".to_owned(),
            prepared,
            observed_at: now,
            prepare_clock_millis: None,
            prepare_advance_millis: 0,
            post_attempts: Some(posts.clone()),
            account_state: Some(current_account_state.clone()),
            prepare_barrier: Some(barrier.clone()),
            prepare_release: Some(release.clone()),
        };
        let task = tokio::spawn(async move {
            let venue_context = RecoveryVenueContext {
                venue: &venue,
                account_binding: &account_binding,
                custody_wallet: "0x1111111111111111111111111111111111111111",
            };
            resume_approved_with_venue(
                &mut state,
                &target,
                pending,
                RecoveryExecutionState {
                    mode: LiveModeSnapshot {
                        requested: LiveControlMode::LiveTiny,
                        effective: LiveControlMode::LiveTiny,
                    },
                    account_state: current_account_state,
                    binding: CredentialBindingIdentity {
                        version: 1,
                        key_id: "key".to_owned(),
                    },
                },
                &venue_context,
                || now,
            )
            .await
        });
        barrier.wait().await;
        shutdown.advance(ShutdownPhase::StopProducers);
        release.notify_one();

        assert!(!task.await.unwrap().unwrap());
        assert_eq!(posts.load(Ordering::SeqCst), 0);
        let events = replay_account(&journal_path, &AccountId::new("acct").unwrap()).unwrap();
        assert!(events.iter().any(|event| matches!(
            &event.payload,
            LiveJournalPayload::OrderPreparationFailed(failed)
                if failed.failure
                    == pe_execution_core::LiveOrderPreparationFailure::PrePostAdmissionExpired
        )));
    }

    /// PASS: an off/off transition or rotated binding observed after parked preparation prevents
    /// every POST and records the corresponding terminal preparation failure.
    /// FAIL: any loopback POST after the off transition or binding rotation, or a missing
    /// terminal preparation failure.
    #[tokio::test]
    async fn recovery_rechecks_account_control_after_parked_preparation() {
        for case in ["off", "rotated"] {
            let dir = tempdir().unwrap();
            let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
            let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
            let mut snapshot = armed_snapshot("acct");
            snapshot.fetched_at_unix = Some(now.unix_timestamp());
            let mut state = fanout_state(&dir, db, snapshot, "http://127.0.0.1:9", None);
            let live_accounts = state.config.live_accounts.clone();
            let journal_path = state.config.journal_path.clone();
            let (pending, prepared) = stage_recoverable_approved(&mut state, now, None).await;
            let current_account_state =
                venue_account_state_from_audit(pending.admission.account_state.as_ref().unwrap());
            let account_binding = account_binding_fixture(
                &pending.account_id,
                "0x1111111111111111111111111111111111111111",
            );
            let target = state
                .config
                .paper_state
                .dispatch_targets("dispatch-finality")
                .unwrap()
                .remove(0);
            let barrier = Arc::new(tokio::sync::Barrier::new(2));
            let release = Arc::new(tokio::sync::Notify::new());
            let posts = Arc::new(AtomicUsize::new(0));
            let venue = RecoveryPostVenue {
                client: reqwest::Client::new(),
                order_url: "http://127.0.0.1:9/order".to_owned(),
                prepared,
                observed_at: now,
                prepare_clock_millis: None,
                prepare_advance_millis: 0,
                post_attempts: Some(posts.clone()),
                account_state: Some(current_account_state.clone()),
                prepare_barrier: Some(barrier.clone()),
                prepare_release: Some(release.clone()),
            };
            let task = tokio::spawn(async move {
                let venue_context = RecoveryVenueContext {
                    venue: &venue,
                    account_binding: &account_binding,
                    custody_wallet: "0x1111111111111111111111111111111111111111",
                };
                resume_approved_with_venue(
                    &mut state,
                    &target,
                    pending,
                    RecoveryExecutionState {
                        mode: LiveModeSnapshot {
                            requested: LiveControlMode::LiveTiny,
                            effective: LiveControlMode::LiveTiny,
                        },
                        account_state: current_account_state,
                        binding: CredentialBindingIdentity {
                            version: 1,
                            key_id: "key".to_owned(),
                        },
                    },
                    &venue_context,
                    || now,
                )
                .await
            });
            barrier.wait().await;
            let mut changed = armed_snapshot("acct");
            changed.fetched_at_unix = Some(now.unix_timestamp());
            if case == "off" {
                changed.accounts[0].enabled = false;
                changed.accounts[0].requested_live_mode = "off".to_owned();
                changed.accounts[0].effective_live_mode = "off".to_owned();
            } else {
                changed.accounts[0].credential_binding = Some((2, "key-2".to_owned()));
            }
            live_accounts.store(changed);
            release.notify_one();

            assert!(!task.await.unwrap().unwrap(), "{case}");
            assert_eq!(posts.load(Ordering::SeqCst), 0, "{case}");
            let expected = if case == "rotated" {
                pe_execution_core::LiveOrderPreparationFailure::RecoveryCredentialChanged
            } else {
                pe_execution_core::LiveOrderPreparationFailure::RecoveryAdmissionExpired
            };
            let events = replay_account(&journal_path, &AccountId::new("acct").unwrap()).unwrap();
            assert!(
                events.iter().any(|event| matches!(
                    &event.payload,
                    LiveJournalPayload::OrderPreparationFailed(failed) if failed.failure == expected
                )),
                "{case}"
            );
        }
    }

    /// PASS: resolving the owner shutdown future leaves an Approved-but-unprepared admission
    /// pending and reaches zero loopback order POSTs.
    /// FAIL: shutdown prepares, posts, or terminalizes the unprepared admission.
    #[tokio::test]
    async fn shutdown_does_not_submit_unprepared_admission() {
        let posts = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/order", post(count_order_post))
            .with_state(posts.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let mut snapshot = armed_snapshot("acct");
        snapshot.fetched_at_unix = Some(now.unix_timestamp());
        let mut state = fanout_state(&dir, db, snapshot, &format!("http://{address}"), None);
        let _ = stage_recoverable_approved(&mut state, now, None).await;
        run_live_fanout_until(state.config, std::future::ready(()))
            .await
            .unwrap();
        assert_eq!(posts.load(Ordering::SeqCst), 0);
        assert_eq!(
            recovery_inventory(dir.path().join("live.journal"), None)
                .unwrap()
                .approved_admissions
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn stale_pending_target_pauses_with_zero_network_calls() {
        let hits = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .fallback(counting_fallback)
            .with_state(hits.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        stage(&db, "seed-stale", 1, &["acct"]);
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let mut snapshot = armed_snapshot("acct");
        snapshot.fetched_at_unix =
            Some(now.unix_timestamp() - crate::live_accounts::LIVE_ACCOUNTS_STALE_AFTER_SECS);
        let mut state = fanout_state(
            &dir,
            db.clone(),
            snapshot,
            &format!("http://{address}"),
            None,
        );
        let seed = db.unfinalized_ready_dispatch_seeds().unwrap().remove(0);
        let target = db.dispatch_targets(&seed.dispatch_id).unwrap().remove(0);
        let control = process_target(
            &mut state,
            &seed,
            &target,
            &projection_signal(),
            &fixture_observation_evidence(),
            now,
        )
        .await
        .unwrap();
        assert_eq!(control, PassControl::StopSeed);
        assert_eq!(
            db.dispatch_targets("seed-stale").unwrap()[0].state,
            "pending",
            "a stale pending target is paused, never terminalized"
        );
        assert!(
            hits.lock().unwrap().is_empty(),
            "no credential/book/order call is made while stale"
        );
    }

    async fn rotated_credential_row() -> Json<Vec<serde_json::Value>> {
        Json(vec![serde_json::json!({
            "account_id": "acct",
            "bundle_version": 2,
            "key_id": "key-2",
            "sealed_bundle": "unused"
        })])
    }

    #[tokio::test]
    async fn rotated_credentials_never_terminalize_an_in_flight_target() {
        let app = Router::new().route("/rest/v1/account_credentials", get(rotated_credential_row));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        stage(&db, "seed-inflight", 1, &["acct"]);
        db.set_dispatch_target_state("seed-inflight", "acct", "submitted", None, 5)
            .unwrap();
        // Recovery runs even when the account is MISSING from a never-successful snapshot
        // AND its admission is closed (classify-first), and credential rotation retains
        // the target non-terminally through all of it.
        let mut state = fanout_state(
            &dir,
            db.clone(),
            LiveAccountsSnapshot::default(),
            &format!("http://{address}"),
            Some(Identity::generate()),
        );
        state
            .closures
            .mode
            .insert("acct".to_owned(), "closed".to_owned());
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let seed = db.unfinalized_ready_dispatch_seeds().unwrap().remove(0);
        let target = db.dispatch_targets(&seed.dispatch_id).unwrap().remove(0);
        let control = process_target(
            &mut state,
            &seed,
            &target,
            &projection_signal(),
            &fixture_observation_evidence(),
            now,
        )
        .await
        .unwrap();
        assert_eq!(control, PassControl::FreezePass, "dispatch stays frozen");
        assert_eq!(
            db.dispatch_targets("seed-inflight").unwrap()[0].state,
            "submitted",
            "the in-flight order is retained non-terminally"
        );
    }

    /// PASS: a crash after the synchronized final fill terminalizes locally before credentials.
    #[tokio::test]
    async fn finalized_fill_recovery_terminalizes_without_external_reads() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        stage(&db, "dispatch-finality", 1, &["acct"]);
        db.set_dispatch_target_state("dispatch-finality", "acct", "submitted", None, 5)
            .unwrap();
        let mut state = fanout_state(
            &dir,
            db.clone(),
            LiveAccountsSnapshot::default(),
            "http://127.0.0.1:9",
            None,
        );
        let account_id = AccountId::new("acct").unwrap();
        let mut prepared = finality_prepared();
        let _ = bind_activity_observation(
            &mut prepared,
            &account_id,
            fixture_receipt(5),
            fixture_receipt(6),
            OffsetDateTime::UNIX_EPOCH,
            OffsetDateTime::UNIX_EPOCH,
        );
        let baseline = baseline_event(&account_id, 0);
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &baseline.payload {
            prepared.account_state = mark.account_state.clone();
        }
        state
            .config
            .journal
            .append(account_id.clone(), baseline.timestamp, baseline.payload)
            .unwrap();
        append_approved_admission(
            state.config.journal.as_ref(),
            &account_id,
            &prepared,
            OffsetDateTime::UNIX_EPOCH,
        );
        let prepared_event = state
            .config
            .journal
            .append(
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                LiveJournalPayload::OrderPrepared(prepared.clone()),
            )
            .unwrap();
        let matched_event = state
            .config
            .journal
            .append(
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                LiveJournalPayload::OrderReconciled(Box::new(finality_matched(
                    &prepared,
                    format!("0x{}", "11".repeat(32)),
                ))),
            )
            .unwrap();
        let fill = finalized_fill_event(
            &account_id,
            &prepared,
            matched_event.seq + 1,
            prepared_event.seq,
        );
        state
            .config
            .journal
            .append(account_id, fill.timestamp, fill.payload)
            .unwrap();
        let seed = db.unfinalized_ready_dispatch_seeds().unwrap().remove(0);
        let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
        let control = process_target(
            &mut state,
            &seed,
            &target,
            &projection_signal(),
            &fixture_observation_evidence(),
            OffsetDateTime::from_unix_timestamp(20).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(control, PassControl::Continue);
        let target = db.dispatch_targets("dispatch-finality").unwrap().remove(0);
        assert_eq!(target.state, "terminal");
        assert_eq!(target.terminal_reason.as_deref(), Some("filled"));
    }

    #[tokio::test]
    async fn stale_snapshot_skips_the_mode_pass_and_fresh_does_not() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        // FRESH + unreachable Supabase: the pass runs and records the promotion-query
        // closure — proving the stale skip below is the freshness gate, not inertness.
        let fresh_dir = tempdir().unwrap();
        let fresh_db = Arc::new(PaperStateDb::open(&fresh_dir.path().join("paper.db")).unwrap());
        let mut fresh_snapshot = armed_snapshot("acct");
        fresh_snapshot.fetched_at_unix = Some(now.unix_timestamp());
        let mut fresh_state = fanout_state(
            &fresh_dir,
            fresh_db,
            fresh_snapshot,
            "http://127.0.0.1:9",
            None,
        );
        drive_modes(&mut fresh_state, now).await;
        assert!(fresh_state.closures.mode.contains_key("acct"));
        // STALE: the pass returns before any evidence read or mode write.
        let stale_dir = tempdir().unwrap();
        let stale_db = Arc::new(PaperStateDb::open(&stale_dir.path().join("paper.db")).unwrap());
        let mut stale_state = fanout_state(
            &stale_dir,
            stale_db,
            armed_snapshot("acct"),
            "http://127.0.0.1:9",
            None,
        );
        drive_modes(&mut stale_state, now).await;
        assert!(
            stale_state.closures.mode.is_empty(),
            "no mode decision is taken from stale evidence"
        );
    }

    fn stage(db: &PaperStateDb, id: &str, created: i64, accounts: &[&str]) {
        db.stage_dispatch_seed(&DispatchSeedRecord {
            dispatch_id: id.to_owned(),
            signal_json: "{}".to_owned(),
            source_trade_id: format!("source-{id}"),
            created_at_unix: created,
            targets: accounts
                .iter()
                .map(|account| DispatchTargetSeed {
                    account_id: (*account).to_owned(),
                    credential_bundle_version: 1,
                    credential_key_id: "key".to_owned(),
                })
                .collect(),
        })
        .unwrap();
        db.flip_dispatch_ready(id, "fill").unwrap();
    }

    /// PASS: a schema-one pending seed without observation evidence fails before target dispatch;
    /// the target remains pending and the live journal records no attempted admission or POST.
    /// FAIL: activation silently upgrades an open legacy seed or reaches account/venue work.
    #[tokio::test]
    async fn current_dispatch_seed_requires_observation_before_network_work() {
        let dir = tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        db.stage_dispatch_seed(&DispatchSeedRecord {
            dispatch_id: "missing-observation".to_owned(),
            signal_json: serde_json::to_string(&serde_json::json!({
                "schema_version": 1,
                "signal": projection_signal(),
            }))
            .unwrap(),
            source_trade_id: "source-missing-observation".to_owned(),
            created_at_unix: 1_700_000_000,
            targets: vec![DispatchTargetSeed {
                account_id: "acct".to_owned(),
                credential_bundle_version: 1,
                credential_key_id: "key".to_owned(),
            }],
        })
        .unwrap();
        db.flip_dispatch_ready("missing-observation", "fill")
            .unwrap();
        let mut state = fanout_state(
            &dir,
            db.clone(),
            armed_snapshot("acct"),
            "http://127.0.0.1:9",
            None,
        );

        assert!(matches!(
            run_dispatch_pass(&mut state).await,
            Err(FanoutError::Signal(reason))
                if reason.contains("activation requires no open live work")
        ));
        assert_eq!(
            db.dispatch_targets("missing-observation").unwrap()[0].state,
            "pending"
        );
        assert!(
            replay_account(&state.config.journal_path, &AccountId::new("acct").unwrap())
                .unwrap()
                .is_empty()
        );
    }

    async fn fixture_pass(db: &PaperStateDb, venue: &FakeVenue, armed: &HashSet<&str>) {
        for seed in db.unfinalized_ready_dispatch_seeds().unwrap() {
            for target in db.dispatch_targets(&seed.dispatch_id).unwrap() {
                if target.state == "terminal" {
                    continue;
                }
                if !armed.contains(target.account_id.as_str()) {
                    db.set_dispatch_target_state(
                        &seed.dispatch_id,
                        &target.account_id,
                        "terminal",
                        Some("not_armed"),
                        10,
                    )
                    .unwrap();
                    continue;
                }
                let reconciliation = venue
                    .reconcile_and_cancel_by_order_hash(&target.account_id)
                    .await
                    .unwrap();
                let outcome = match reconciliation.outcome {
                    LiveVenueReconciledOutcome::Matched {
                        venue_order_id,
                        transaction_hashes,
                    } => LiveOrderOutcome::Matched {
                        order_hash: target.account_id.clone(),
                        venue_order_id,
                        transaction_hashes,
                        executed: None,
                    },
                    LiveVenueReconciledOutcome::Killed { venue_order_id } => {
                        LiveOrderOutcome::Killed {
                            order_hash: target.account_id.clone(),
                            venue_order_id,
                        }
                    }
                    LiveVenueReconciledOutcome::Rejected { venue_order_id } => {
                        LiveOrderOutcome::Rejected {
                            order_hash: Some(target.account_id.clone()),
                            venue_order_id,
                            kind: LiveOrderRejectKind::VenueRejected,
                        }
                    }
                    LiveVenueReconciledOutcome::Ambiguous { kind } => LiveOrderOutcome::Ambiguous {
                        order_hash: target.account_id.clone(),
                        kind,
                        reconcile_first: true,
                    },
                };
                let transition = outcome_transition(&outcome);
                db.set_dispatch_target_state(
                    &seed.dispatch_id,
                    &target.account_id,
                    transition.state,
                    transition.reason,
                    10,
                )
                .unwrap();
                if transition.freeze {
                    return;
                }
            }
            db.finalize_dispatch_if_terminal(&seed.dispatch_id, 10)
                .unwrap();
        }
    }

    #[tokio::test]
    async fn ordered_execution_is_primary_first_and_next_waits_for_terminal() {
        let (_dir, db) = db();
        stage(&db, "seed-a", 1, &["primary", "secondary"]);
        let venue = FakeVenue::new(vec![
            LiveVenueReconciledOutcome::Killed {
                venue_order_id: Some("primary-order".to_owned()),
            },
            LiveVenueReconciledOutcome::Killed {
                venue_order_id: Some("secondary-order".to_owned()),
            },
        ]);
        fixture_pass(&db, &venue, &HashSet::from(["primary", "secondary"])).await;
        assert_eq!(venue.calls(), vec!["primary", "secondary"]);
        let targets = db.dispatch_targets("seed-a").unwrap();
        assert!(targets.iter().all(|target| target.state == "terminal"));
        assert!(
            db.dispatch_seed("seed-a")
                .unwrap()
                .unwrap()
                .finalized_at_unix
                .is_some()
        );
    }

    #[tokio::test]
    async fn ambiguous_freezes_later_accounts_and_later_seeds() {
        let (_dir, db) = db();
        stage(&db, "seed-a", 1, &["primary", "secondary"]);
        stage(&db, "seed-b", 2, &["primary"]);
        let venue = FakeVenue::new(vec![LiveVenueReconciledOutcome::Ambiguous {
            kind: LiveOrderAmbiguityKind::ReconciliationPending,
        }]);
        fixture_pass(&db, &venue, &HashSet::from(["primary", "secondary"])).await;
        assert_eq!(venue.calls(), vec!["primary"]);
        let first = db.dispatch_targets("seed-a").unwrap();
        assert_eq!(first[0].state, "ambiguous");
        assert_eq!(first[1].state, "pending");
        assert_eq!(db.dispatch_targets("seed-b").unwrap()[0].state, "pending");
    }

    #[tokio::test]
    async fn not_armed_targets_are_terminal_skipped_before_next_account() {
        let (_dir, db) = db();
        stage(&db, "seed-a", 1, &["retired", "primary"]);
        let venue = FakeVenue::new(vec![LiveVenueReconciledOutcome::Killed {
            venue_order_id: None,
        }]);
        fixture_pass(&db, &venue, &HashSet::from(["primary"])).await;
        assert_eq!(venue.calls(), vec!["primary"]);
        let targets = db.dispatch_targets("seed-a").unwrap();
        assert_eq!(targets[0].terminal_reason.as_deref(), Some("not_armed"));
        assert_eq!(targets[1].terminal_reason.as_deref(), Some("killed"));
    }

    #[tokio::test]
    async fn dark_pass_with_no_staged_targets_touches_nothing() {
        let (_dir, db) = db();
        let venue = FakeVenue::new(Vec::new());
        fixture_pass(&db, &venue, &HashSet::new()).await;
        assert!(venue.calls().is_empty());
        assert!(db.unfinalized_ready_dispatch_seeds().unwrap().is_empty());
    }
}
