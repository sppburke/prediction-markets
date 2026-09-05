//! Strictly sequential ordinary-live dispatch consumer (#508 Decision 10).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use age::x25519::Identity;
use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    AccountId, BasisPoints, CollateralAmount, EventSeq, KellyFraction, PolymarketConditionId,
    Price, Probability, RawHttpAttempt, ShareAmount, Side,
};
use pe_event_log::AppendReceipt;
use pe_execution_core::{
    BalanceAudit, CanonicalPositionAudit, CredentialBindingIdentity, ECONOMIC_PREPARED_VERSION,
    EconomicPrepared, FeeAudit, FrozenLiveTarget, LadderPlanAudit, LiveAdmissionArtifactAudit,
    LiveAdmissionRefusal, LiveControlMode, LiveExecutor, LiveFillProjectionIdentity, LiveJournal,
    LiveJournalEvent, LiveJournalOrderOutcome, LiveJournalPayload, LiveModeSnapshot,
    LiveModeTransitionAudit, LiveModeTransitionReason, LiveOrderAmbiguityKind, LiveOrderIdentity,
    LiveOrderOutcome, LiveOrderReconciliationAudit, LiveOrderVenue, LivePrepareResult,
    LiveReconciliationSource, LiveVenueReconciledOutcome, MarkKind, MarketSelection,
    MatchedLogIdentity, OrderFillFinalizedAudit, RedemptionAttempt, RedemptionAttemptIdentity,
    RedemptionAttemptState, RedemptionPassInput, RiskAudit, RiskDecisionAudit, SizingAudit,
    SizingModeAudit, reconstruct_redemption_attempts, redemption_posture, replay_account,
    run_redemption_pass,
};
use pe_paper_state::{DispatchSeedRow, DispatchTargetRow, PaperStateDb};
use pe_risk_engine::snapshot::{RiskSnapshot, TradingMode};
use pe_source_polymarket_public::BinaryPayoutVector;
use pe_strategy_winner_follow::{ExecutionMode, SizingMode, WinnerFollowStrategy};
use pe_venue_polymarket::{
    CustodyKind, DecodedOrderFill, LadderError, MatchedReceipt, ReceiptError, RedemptionTransport,
    RelayerApiKeyCredentials, RelayerCredentials, RelayerPollPolicy, build_redemption_call,
    canonical_block_matches, decode_order_fills, fee_reserve, fee_within_reserve,
    parse_chain_id_response, parse_finalized_block_response, parse_receipt_response,
    plan_budget_buy, sign_deposit_wallet_redemption, taker_fee,
};
use rust_decimal::prelude::ToPrimitive as _;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Deserialize;
use time::OffsetDateTime;
use tracing::{error, info, warn};

use crate::clob_book::{ClobBookFetcher, ReqwestClobBookFetcher};
use crate::live_accounts::{AccountContext, LiveAccounts};
use crate::live_credentials::{
    CredentialBinding, CredentialError, LiveAccountCredentials, decrypt_bundle,
};
use crate::live_mode::{
    ArmingProbe, CheckOutcome, ModeDecision, ModeInputs, PromotionEventRow, PromotionFacts,
    evaluate_mode, promotion_facts_from_rows,
};
use crate::live_projections::{
    LiveAccountStateRow, LiveFillRow, LivePositionRow, LiveProjectionWriter,
};
use crate::live_venue_adapter::{
    LiveAdmissionBuilder, LiveRedemptionAdapter, LiveVenueAdapterError, PolygonReceiptReader,
    PolymarketLiveVenue,
};
use crate::live_watchlist::LiveWatchlist;
use crate::runtime_config::LiveRuntimeConfig;
use crate::supabase_reader::auth_token;

const FANOUT_INTERVAL_SECS: u64 = 2;
const MODE_INTERVAL_SECS: i64 = 30;
const PRUNE_INTERVAL_SECS: i64 = 3_600;
const DISPATCH_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;
const REDEMPTION_RETRY_SECS: i64 = 30;
/// Ordinary redemption inventory/status reconcile cadence (Decision 12).
const REDEMPTION_RECONCILE_CADENCE_SECS: i64 = 300;
const REDEEMABLE_POSITION_PAGE_LIMIT: usize = 500;
const REDEEMABLE_POSITION_MAX_OFFSET: usize = 10_000;
// verified 2026-08-11 from the official four-minute Deposit Wallet batch example:
// https://github.com/Polymarket/builder-relayer-client#execute-deposit-wallet-batch
const DEPOSIT_WALLET_REDEMPTION_DEADLINE_SECS: u64 = 4 * 60;

/// Inputs owned by the one strictly sequential live task.
pub struct LiveFanoutConfig {
    pub paper_state: Arc<PaperStateDb>,
    pub live_accounts: LiveAccounts,
    pub live_watchlist: LiveWatchlist,
    pub runtime_config: LiveRuntimeConfig,
    pub identity: Option<Identity>,
    pub journal: Arc<LiveJournal>,
    pub journal_path: PathBuf,
    pub projection: LiveProjectionWriter,
    pub book_fetcher: Arc<ReqwestClobBookFetcher>,
    pub http: reqwest::Client,
    pub supabase_url: String,
    pub supabase_anon_key: String,
    pub supabase_secret_key: String,
    pub gamma_base_url: String,
    pub clob_base_url: String,
    pub data_base_url: String,
    pub projection_reconcile_interval_secs: u64,
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
    closures: AdmissionClosures,
    last_mode_unix: Option<i64>,
    last_redemption_unix: Option<i64>,
    last_projection_unix: Option<i64>,
    last_prune_unix: Option<i64>,
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
    ShutdownRecovery(#[source] pe_paper_state::PaperStateError),
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
    );
    let mut state = FanoutState {
        config,
        admission,
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
            _ = ticker.tick() => {}
            () = &mut shutdown => break,
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
        match recovery_is_pending(&state.config.paper_state) {
            Ok(true) => {
                if let Err(error) = run_recovery_pass(&mut state, now).await {
                    error!(error = %error, "live recovery-first pass failed; fan-out remains frozen");
                }
                // A submitted/ambiguous reservation is the only work allowed on this tick.
                // If reconciliation terminalized it, ordinary work resumes next tick.
                continue;
            }
            Ok(false) => {}
            Err(error) => {
                error!(error = %error, "live recovery state unreadable; fan-out remains frozen");
                continue;
            }
        }
        if due(
            state.last_mode_unix,
            now.unix_timestamp(),
            MODE_INTERVAL_SECS,
        ) {
            drive_modes(&mut state, now).await;
            state.last_mode_unix = Some(now.unix_timestamp());
        }
        if due(
            state.last_redemption_unix,
            now.unix_timestamp(),
            REDEMPTION_RECONCILE_CADENCE_SECS,
        ) {
            drive_redemptions(&mut state, now).await;
            state.last_redemption_unix = Some(now.unix_timestamp());
        }
        if let Err(error) = run_dispatch_pass(&mut state, now).await {
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

    if recovery_is_pending(&state.config.paper_state)
        .map_err(LiveFanoutOwnerError::ShutdownRecovery)?
    {
        run_recovery_pass(&mut state, OffsetDateTime::now_utc())
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

fn recovery_is_pending(
    paper_state: &PaperStateDb,
) -> Result<bool, pe_paper_state::PaperStateError> {
    for seed in paper_state.unfinalized_ready_dispatch_seeds()? {
        if paper_state
            .dispatch_targets(&seed.dispatch_id)?
            .iter()
            .any(|target| target.state == "submitted" || target.state == "ambiguous")
        {
            return Ok(true);
        }
    }
    Ok(false)
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

struct ReceiptObservation {
    attempt: RawHttpAttempt,
    parsed: Result<Option<MatchedReceipt>, ReceiptError>,
}

struct BlockObservation {
    attempt: RawHttpAttempt,
    block: Result<pe_venue_polymarket::FinalizedBlock, ReceiptError>,
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
        .and_then(|body| parse_chain_id_response(body).map_err(|_| ()))
        .ok();

    let hashes = orders
        .iter()
        .flat_map(|order| order.transaction_hashes.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut receipts = BTreeMap::new();
    for hash in hashes {
        let attempt = rpc.transaction_receipt(&hash).await;
        let parsed = response_body(&attempt)
            .map_err(|_| ReceiptError::MalformedRpc)
            .and_then(|body| parse_receipt_response(body, &hash));
        receipts.insert(hash, ReceiptObservation { attempt, parsed });
    }

    let head_attempt = rpc.finalized_block().await;
    let head = response_body(&head_attempt)
        .map_err(|_| ReceiptError::MalformedRpc)
        .and_then(parse_finalized_block_response);
    let mut lower_heights = BTreeSet::new();
    if let Ok(head) = &head {
        for observation in receipts.values() {
            if let Ok(Some(receipt)) = &observation.parsed
                && receipt.block_number < head.number
            {
                lower_heights.insert(receipt.block_number);
            }
        }
    }
    let mut blocks = BTreeMap::new();
    for height in lower_heights {
        let attempt = rpc.block_by_number(height).await;
        let expected_hash = receipts
            .values()
            .find_map(|observation| match &observation.parsed {
                Ok(Some(receipt)) if receipt.block_number == height => {
                    Some(receipt.block_hash.as_str())
                }
                Ok(Some(_)) | Ok(None) | Err(_) => None,
            });
        let block = match (response_body(&attempt), expected_hash) {
            (Ok(body), Some(hash)) => canonical_block_matches(body, height, hash),
            (Ok(_), None) | (Err(_), _) => Err(ReceiptError::MalformedRpc),
        };
        blocks.insert(height, BlockObservation { attempt, block });
    }

    orders
        .into_iter()
        .map(|order| {
            classify_order_finality(
                order,
                chain_id,
                &chain_attempt,
                &head_attempt,
                &head,
                &receipts,
                &blocks,
            )
        })
        .collect()
}

fn classify_order_finality(
    order: PendingOrderFinality,
    chain_id: Option<u64>,
    chain_attempt: &RawHttpAttempt,
    head_attempt: &RawHttpAttempt,
    head: &Result<pe_venue_polymarket::FinalizedBlock, ReceiptError>,
    receipts: &BTreeMap<String, ReceiptObservation>,
    blocks: &BTreeMap<u64, BlockObservation>,
) -> OrderFinalityResult {
    let identity = order.prepared.identity.clone();
    let order_hash = order.prepared.prepared.order_hash.clone();
    let all_evidence = || {
        std::iter::once(chain_attempt.clone())
            .chain(receipts.values().map(|item| item.attempt.clone()))
            .chain(std::iter::once(head_attempt.clone()))
            .chain(blocks.values().map(|item| item.attempt.clone()))
            .collect::<Vec<_>>()
    };
    let pending = |reason: String| OrderFinalityResult {
        identity: identity.clone(),
        order_hash: order_hash.clone(),
        disposition: OrderFinalityDisposition::Pending {
            reason,
            evidence: all_evidence(),
        },
    };
    let conflict = |reason: String| OrderFinalityResult {
        identity: identity.clone(),
        order_hash: order_hash.clone(),
        disposition: OrderFinalityDisposition::Conflict {
            reason,
            evidence: all_evidence(),
        },
    };

    if chain_id != Some(pe_venue_polymarket::FINALIZED_CHAIN_ID) {
        return pending("polygon chain identity unavailable or not 137".to_owned());
    }
    let Ok(head) = head else {
        return pending("polygon finalized head unavailable".to_owned());
    };
    if order.transaction_hashes.is_empty() {
        return pending("authenticated match has no nonzero transaction hash yet".to_owned());
    }

    let mut fills = BTreeMap::<(String, u64), DecodedOrderFill>::new();
    let mut receipt_evidence = vec![chain_attempt.clone()];
    let mut block_evidence = vec![head_attempt.clone()];
    let mut has_unfinalized = false;
    for hash in &order.transaction_hashes {
        let Some(observation) = receipts.get(hash) else {
            return conflict(format!(
                "receipt request missing for authenticated transaction {hash}"
            ));
        };
        receipt_evidence.push(observation.attempt.clone());
        let receipt = match &observation.parsed {
            Ok(Some(receipt)) => receipt,
            Ok(None) => {
                has_unfinalized = true;
                continue;
            }
            Err(error) if finality_evidence_unavailable(error) => {
                return pending(format!("transaction receipt unavailable: {error}"));
            }
            Err(error) => return conflict(format!("transaction receipt conflict: {error}")),
        };
        if receipt.block_number > head.number {
            has_unfinalized = true;
            continue;
        }
        if receipt.block_number == head.number {
            if receipt.block_hash != head.hash {
                return conflict("receipt block hash conflicts with finalized head".to_owned());
            }
        } else {
            let Some(block) = blocks.get(&receipt.block_number) else {
                return conflict("canonical lower block evidence is missing".to_owned());
            };
            block_evidence.push(block.attempt.clone());
            match &block.block {
                Ok(canonical) if canonical.hash == receipt.block_hash => {}
                Ok(_) => return conflict("receipt block hash is not canonical".to_owned()),
                Err(error) if finality_evidence_unavailable(error) => {
                    return pending(format!("canonical block unavailable: {error}"));
                }
                Err(error) => return conflict(format!("canonical block conflict: {error}")),
            }
        }
        let decoded = match decode_order_fills(receipt, &order.prepared.prepared) {
            Ok(decoded) => decoded,
            Err(error) => return conflict(format!("OrderFilled conflict: {error}")),
        };
        for fill in decoded {
            let key = (fill.transaction_hash.clone(), fill.log_index);
            match fills.get(&key) {
                Some(existing) if existing == &fill => {}
                Some(_) => {
                    return conflict(
                        "OrderFilled log identity was reused with different content".to_owned(),
                    );
                }
                None => {
                    fills.insert(key, fill);
                }
            }
        }
    }
    if has_unfinalized {
        return pending("one or more authenticated transactions are not finalized".to_owned());
    }
    if fills.is_empty() {
        return conflict("finalized receipts contain zero matching OrderFilled logs".to_owned());
    }

    let aggregates = fills.values().try_fold(
        (
            CollateralAmount::ZERO,
            ShareAmount::ZERO,
            CollateralAmount::ZERO,
        ),
        |(principal, quantity, fee), fill| {
            Some((
                principal.checked_add(fill.principal).ok()?,
                quantity.checked_add(fill.quantity).ok()?,
                fee.checked_add(fill.fee).ok()?,
            ))
        },
    );
    let Some((principal, quantity, fee)) = aggregates else {
        return conflict("finalized fill aggregation overflow".to_owned());
    };
    if order.prepared.economic.sizing.principal != order.prepared.prepared.maker_collateral
        || order.prepared.economic.ladder.minimum_shares != order.prepared.prepared.taker_shares
    {
        return conflict("prepared economics disagree with the signed order".to_owned());
    }
    let expected_principal = order.prepared.prepared.maker_collateral;
    if principal < expected_principal {
        return pending("finalized principal remains below the signed principal".to_owned());
    }
    if principal > expected_principal {
        return conflict("finalized principal exceeds the signed principal".to_owned());
    }
    if quantity < order.prepared.economic.ladder.minimum_shares {
        return conflict(
            "full-principal finalized quantity is below the signed minimum".to_owned(),
        );
    }
    if !fee_within_reserve(fee, order.prepared.economic.fee.reserve) {
        return conflict("finalized fee exceeds the prepared reserve".to_owned());
    }

    let matched_logs = fills
        .into_keys()
        .map(|(transaction_hash, log_index)| MatchedLogIdentity {
            transaction_hash,
            log_index,
        })
        .collect();
    OrderFinalityResult {
        identity: identity.clone(),
        order_hash,
        disposition: OrderFinalityDisposition::Finalized(Box::new(OrderFillFinalizedAudit {
            identity,
            prepared_journal_seq: order.prepared_journal_seq,
            principal,
            quantity,
            fee,
            matched_logs,
            chain_id: pe_venue_polymarket::FINALIZED_CHAIN_ID,
            finalized_head: head.number,
            receipts: receipt_evidence,
            blocks: block_evidence,
        })),
    }
}

fn finality_evidence_unavailable(error: &ReceiptError) -> bool {
    matches!(
        error,
        ReceiptError::MalformedRpc
            | ReceiptError::RpcError
            | ReceiptError::MissingResult
            | ReceiptError::UnsupportedFinalizedTag
    )
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
            let evidence_hashes = evidence
                .iter()
                .map(|attempt| {
                    serde_json::to_vec(&("prediction-edge/live-http-attempt/v1", attempt))
                        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
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
            let evidence_hashes = evidence
                .iter()
                .map(|attempt| {
                    serde_json::to_vec(&("prediction-edge/live-http-attempt/v1", attempt))
                        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
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

async fn run_dispatch_pass(
    state: &mut FanoutState,
    now: OffsetDateTime,
) -> Result<(), FanoutError> {
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
        let signal = parse_frozen_signal(&seed)?;
        let targets = state
            .config
            .paper_state
            .dispatch_targets(&seed.dispatch_id)?;
        for target in targets {
            if target.state == "terminal" {
                continue;
            }
            let control = process_target(state, &seed, &target, &signal, now).await?;
            match control {
                PassControl::Continue => {}
                // A transient refusal cannot be overtaken by a younger seed. Resume this exact
                // oldest target on the next tick.
                PassControl::StopSeed => return Ok(()),
                PassControl::FreezePass => return Ok(()),
            }
        }
        state
            .config
            .paper_state
            .finalize_dispatch_if_terminal(&seed.dispatch_id, now.unix_timestamp())?;
    }
    Ok(())
}

/// Reconcile only already-submitted/ambiguous orders. This is the recovery-first and shutdown
/// path; it can never turn a pending target into a new venue submission (#544).
async fn run_recovery_pass(
    state: &mut FanoutState,
    now: OffsetDateTime,
) -> Result<(), FanoutError> {
    for seed in state
        .config
        .paper_state
        .unfinalized_ready_dispatch_seeds()?
    {
        let signal = parse_frozen_signal(&seed)?;
        for target in state
            .config
            .paper_state
            .dispatch_targets(&seed.dispatch_id)?
            .into_iter()
            .filter(|target| target.state == "submitted" || target.state == "ambiguous")
        {
            if process_target(state, &seed, &target, &signal, now).await? == PassControl::FreezePass
            {
                return Ok(());
            }
        }
        state
            .config
            .paper_state
            .finalize_dispatch_if_terminal(&seed.dispatch_id, now.unix_timestamp())?;
    }
    Ok(())
}

#[derive(Deserialize)]
struct FrozenSignal {
    schema_version: u16,
    signal: LeaderSignal,
}

fn parse_frozen_signal(seed: &DispatchSeedRow) -> Result<LeaderSignal, FanoutError> {
    let frozen: FrozenSignal = serde_json::from_str(&seed.signal_json)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    if frozen.schema_version != 1 {
        return Err(FanoutError::Signal("unsupported schema version".to_owned()));
    }
    Ok(frozen.signal)
}

async fn process_target(
    state: &mut FanoutState,
    seed: &DispatchSeedRow,
    target: &DispatchTargetRow,
    signal: &LeaderSignal,
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
            terminalize(state, target, "not_armed", now)?;
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
            terminalize(state, target, "credential_version_changed", now)?;
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
    let admission = match state.admission.build(&condition_id, now).await {
        Ok(admission) => admission,
        Err(error) if admission_error_is_transient(&error) => {
            warn!(account_id = %target.account_id, error = %error, "live market evidence unavailable; seed paused");
            return Ok(PassControl::StopSeed);
        }
        Err(error) => {
            terminalize(state, target, admission_terminal_reason(&error), now)?;
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
            terminalize(state, target, "outcome_not_binary", now)?;
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
    let book = match state.config.book_fetcher.fetch_book(&token_id.0).await {
        Ok(book) => book,
        Err(error) => {
            warn!(account_id = %target.account_id, error = %error, "fresh per-account ladder unavailable; seed paused");
            return Ok(PassControl::StopSeed);
        }
    };
    let now_ms = unix_ms(now);
    if pe_venue_polymarket::ladder_is_stale(now_ms, book.fetched_at_ms) {
        return Ok(PassControl::StopSeed);
    }
    let Some(ladder) = book.ladder() else {
        terminalize(state, target, "ladder_invalid", now)?;
        return Ok(PassControl::Continue);
    };
    let Some(best_ask) = ladder.first().map(|ask| ask.price) else {
        terminalize(state, target, "ladder_empty", now)?;
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
            terminalize(state, target, reason, now)?;
            return Ok(PassControl::Continue);
        }
    };
    let mut strategy_config = runtime.winner_follow_config();
    strategy_config.sizing_mode = sizing;
    let strategy = WinnerFollowStrategy::new(strategy_config.clone());
    let probability = probability_for(&state.config.live_watchlist, signal);
    let intent = match strategy.evaluate_at_price(
        signal,
        best_ask,
        probability,
        zeroed_risk_snapshot(),
        account_state.reconciled_free_collateral.to_decimal(),
        ExecutionMode::LiveTiny,
        None,
        Some(best_ask),
    ) {
        Ok(intent) => intent,
        Err(_) => {
            terminalize(state, target, "live_sizing_refused", now)?;
            return Ok(PassControl::Continue);
        }
    };
    let minimum_price = parse_band_price(runtime.min_fill_price, Price::ZERO);
    let maximum_price = parse_band_price(runtime.max_fill_price, Price(Decimal::ONE));
    let (minimum_price, maximum_price) = match (minimum_price, maximum_price) {
        (Ok(minimum), Ok(maximum)) => (minimum, maximum),
        _ => {
            terminalize(state, target, "live_price_band_invalid", now)?;
            return Ok(PassControl::Continue);
        }
    };
    let cap_bps = u32::try_from(account.live_price_impact_cap_bps)
        .ok()
        .filter(|cap| (1..=10_000).contains(cap));
    let Some(cap_bps) = cap_bps else {
        terminalize(state, target, "live_price_impact_cap_invalid", now)?;
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
            terminalize(state, target, "live_price_impact_cap_invalid", now)?;
            return Ok(PassControl::Continue);
        }
    };
    let plan = match plan_budget_buy(
        &ladder,
        account_state.reconciled_free_collateral,
        Some(intent.contracts.0),
        minimum_price,
        maximum_price,
        ceiling,
    ) {
        Ok(plan) if plan.shares >= admission.market.minimum_order_size => plan,
        Ok(_) => {
            terminalize(state, target, "below_minimum_order", now)?;
            return Ok(PassControl::Continue);
        }
        Err(error) => {
            terminalize(state, target, ladder_terminal_reason(&error), now)?;
            return Ok(PassControl::Continue);
        }
    };
    let identity = build_order_identity(seed, target, signal, &admission, &plan, &strategy_config)?;
    let economic = build_economic_prepared(
        signal,
        &identity,
        &admission,
        &plan,
        &strategy_config,
        probability,
        account_state.reconciled_free_collateral,
        price_impact_cap_bps,
        ceiling,
        minimum_price,
        maximum_price,
    )?;
    let binding = CredentialBindingIdentity {
        version: target.credential_bundle_version,
        key_id: target.credential_key_id.clone(),
    };
    let request = pe_execution_core::LiveOrderRequest {
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
    let executor = LiveExecutor::new(&venue, state.config.journal.as_ref());
    match executor.prepare(request, now).await? {
        LivePrepareResult::Terminal(outcome) => {
            if refusal_is_transient(&outcome) {
                return Ok(PassControl::StopSeed);
            }
            let transition = outcome_transition(&outcome);
            persist_outcome(state, target, &outcome, now)?;
            Ok(if transition.freeze {
                PassControl::FreezePass
            } else {
                PassControl::Continue
            })
        }
        LivePrepareResult::Prepared(prepared) => {
            // The dispatch reservation is durable before the exactly-one POST capability is
            // consumed. Crash recovery reconciles this order hash; it never blindly resubmits.
            state.config.paper_state.set_dispatch_target_state(
                &target.dispatch_id,
                &target.account_id,
                "submitted",
                None,
                now.unix_timestamp(),
            )?;
            let outcome = executor.submit(prepared, now).await?;
            let transition = outcome_transition(&outcome);
            persist_outcome(state, target, &outcome, now)?;
            if matches!(outcome, LiveOrderOutcome::Matched { .. }) {
                reconcile_account_projection(state, account, now).await;
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

fn unix_ms(now: OffsetDateTime) -> u64 {
    u64::try_from(now.unix_timestamp_nanos() / 1_000_000).unwrap_or(0)
}

fn parse_band_price(value: Decimal, disabled: Price) -> Result<Price, ()> {
    if value == Decimal::ZERO {
        return Ok(disabled);
    }
    Price::new(value).map_err(|_| ())
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

fn zeroed_risk_snapshot() -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        absolute_pnl_bps: BasisPoints(0),
        copy_latency_kill_switch_active: false,
        proposed_trade_bps: BasisPoints(0),
        per_trade_cap_bps: 0,
        concentration_caps: None,
    }
}

fn build_order_identity(
    seed: &DispatchSeedRow,
    target: &DispatchTargetRow,
    signal: &LeaderSignal,
    admission: &pe_execution_core::LiveAdmissionArtifact,
    plan: &pe_venue_polymarket::LadderPlan,
    strategy: &pe_strategy_winner_follow::WinnerFollowConfig,
) -> Result<LiveOrderIdentity, FanoutError> {
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
        plan.estimated_ladder_spend,
        plan.worst_case_debit,
    ))?;
    let config_hash = hash_json(&("prediction-edge/live-config/v1", strategy))?;
    let evidence_hashes = vec![
        admission.market.raw_gamma_market_hash.to_hex().to_string(),
        admission.market.raw_clob_market_hash.to_hex().to_string(),
        admission.settlement.raw_evidence_hash.clone(),
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
        idempotency_key: format!("{}:{}", seed.dispatch_id, target.account_id),
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

#[allow(clippy::too_many_arguments)]
fn build_economic_prepared(
    signal: &LeaderSignal,
    identity: &LiveOrderIdentity,
    admission: &pe_execution_core::LiveAdmissionArtifact,
    plan: &pe_venue_polymarket::LadderPlan,
    strategy: &pe_strategy_winner_follow::WinnerFollowConfig,
    probability: Probability,
    cash_before: CollateralAmount,
    price_impact_cap_bps: i32,
    chase_ceiling: Price,
    band_floor: Price,
    band_ceiling_exclusive: Price,
) -> Result<EconomicPrepared, FanoutError> {
    let outcome_index = u8::try_from(signal.outcome_id.0)
        .map_err(|_| FanoutError::Signal("outcome index exceeds u8".to_owned()))?;
    let token_id = admission
        .market
        .ordered_outcome_token_ids
        .get(usize::from(outcome_index))
        .cloned()
        .ok_or_else(|| FanoutError::Signal("outcome token is missing".to_owned()))?;
    let ladder = LadderPlanAudit::new(plan);
    let expected_shares = ladder
        .expected_shares()
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let expected_vwap = ladder
        .expected_vwap()
        .ok_or_else(|| FanoutError::Signal("ladder VWAP is invalid".to_owned()))?;
    let schedule = admission.fee_schedule;
    let expected_fee = taker_fee(schedule, plan.shares, plan.limit_price)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let reserve = fee_reserve(
        schedule,
        plan.worst_case_debit,
        plan.best_ask,
        plan.limit_price,
    )
    .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let paper_debit = plan
        .worst_case_debit
        .checked_add(expected_fee)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let all_in_price = Price::new(
        paper_debit
            .to_decimal()
            .checked_div(plan.shares.to_decimal())
            .and_then(|price| {
                Decimal::ONE
                    .checked_add(strategy.slippage_rate)
                    .and_then(|slippage| price.checked_mul(slippage))
            })
            .ok_or_else(|| FanoutError::Signal("all-in price arithmetic failed".to_owned()))?,
    )
    .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let sizing_mode = match strategy.sizing_mode {
        SizingMode::Kelly => SizingModeAudit::Kelly {
            fraction: strategy
                .kelly_fraction_override
                .unwrap_or(KellyFraction(Decimal::new(25, 2))),
            probability,
        },
        SizingMode::Dollar { usd } => SizingModeAudit::Dollar { usd },
        SizingMode::Contract { contracts } => SizingModeAudit::Contract { contracts },
    };
    let mut snapshot = zeroed_risk_snapshot();
    let proposed_trade_bps = plan
        .shares
        .to_decimal()
        .checked_mul(plan.best_ask.0)
        .and_then(|notional| notional.checked_div(cash_before.to_decimal()))
        .and_then(|ratio| ratio.checked_mul(Decimal::from(10_000u32)))
        .map(|value| value.ceil())
        .and_then(|value| value.to_i32())
        .ok_or_else(|| FanoutError::Signal("risk exposure arithmetic failed".to_owned()))?;
    snapshot.proposed_trade_bps = BasisPoints(proposed_trade_bps);
    snapshot.per_trade_cap_bps = strategy.per_trade_cap.resolve_bps(TradingMode::LiveTiny);
    let worst_case_debit = plan
        .worst_case_debit
        .checked_add(reserve)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;

    // #545 lane E fills this with the synchronized `/book` source-log receipt. The current book
    // fetcher exposes only its raw response digest, so no append sequence exists at this boundary.
    let book_receipt = AppendReceipt {
        sequence: EventSeq(0),
        this_hash: blake3::Hash::from_bytes([0; 32]),
    };
    Ok(EconomicPrepared {
        version: ECONOMIC_PREPARED_VERSION,
        market: MarketSelection {
            condition_id: admission.market.condition_id.clone(),
            outcome_index,
            token_id,
            side: Side::Buy,
            market_id: signal.market_id.to_string(),
        },
        admission: LiveAdmissionArtifactAudit::new(
            &admission.market,
            &admission.settlement,
            admission.fee_schedule,
            admission.receipts,
        ),
        ladder,
        book_receipt,
        observation: None,
        sizing: SizingAudit {
            mode: sizing_mode,
            budget: cash_before,
            principal: plan.worst_case_debit,
            minimum_shares: plan.shares,
            expected_shares,
            expected_vwap,
            all_in_price,
            slippage_rate: strategy.slippage_rate,
        },
        fee: FeeAudit {
            schedule,
            expected_fee,
            reserve,
        },
        risk: RiskAudit {
            snapshot,
            decision: RiskDecisionAudit::Approved,
        },
        balance: BalanceAudit {
            cash_before,
            worst_case_debit,
            price_impact_cap_bps,
            chase_ceiling,
            band_floor,
            band_ceiling_exclusive,
        },
        applied_configuration_hash: identity.config_hash.clone(),
    })
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
    )
}

fn admission_terminal_reason(error: &LiveVenueAdapterError) -> &'static str {
    match error {
        LiveVenueAdapterError::Outcome => "outcome_not_binary",
        LiveVenueAdapterError::MarketStatus(_) => "market_evidence_http_rejected",
        LiveVenueAdapterError::MarketValidation(_) => "market_evidence_invalid",
        LiveVenueAdapterError::Client(_)
        | LiveVenueAdapterError::MarketTransport(_)
        | LiveVenueAdapterError::Redemption(_) => "market_evidence_invalid",
    }
}

fn ladder_terminal_reason(error: &LadderError) -> &'static str {
    match error {
        LadderError::NothingAffordable => "nothing_affordable",
        LadderError::BelowBandAsk => "below_live_price_band",
        LadderError::InsufficientDepth => "insufficient_live_depth",
        LadderError::Amount => "ladder_amount_invalid",
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
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ProjectionReducerError {
    #[error("a second Baseline mark exists in the same account era")]
    DuplicateBaseline,
    #[error("Baseline must bind empty venue inventory and exact zero-position equity")]
    InvalidBaseline,
    #[error("duplicate prepared/finalized identity conflicts with prior journal content")]
    IdentityConflict,
    #[error("OrderFillFinalized has no matching prepared record")]
    MissingPrepared,
    #[error("OrderFillFinalized prepared sequence is wrong")]
    PreparedSequenceMismatch,
    #[error("OrderFillFinalized violates its prepared order economics or identity")]
    InvalidFinalizedFill,
    #[error("OrderFillFinalized appeared after its condition was finalized")]
    FillAfterResolution,
    #[error("a ResolutionFinalized condition still has a nonterminal prepared order")]
    ResolutionWithPendingOrder,
    #[error("canonical payout vector is invalid")]
    InvalidPayout,
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
    #[error("portfolio mark prices are missing, duplicated, or inconsistent")]
    InvalidMarkPrices,
    #[error("portfolio mark equity disagrees with finalized economics")]
    InvalidMarkEquity,
    #[error("a Daily portfolio mark repeats a cutoff in the same account era")]
    DuplicateDailyMark,
}

pub(crate) fn derive_projection_rows(
    account_id: &AccountId,
    events: &[LiveJournalEvent],
) -> Result<ProjectionDerivation, ProjectionReducerError> {
    let mut baseline_seen = false;
    let mut economic_cash = None;
    let mut fills = BTreeMap::<String, (OrderFillFinalizedAudit, LiveFillRow)>::new();
    let mut prepared_orders =
        BTreeMap::<String, (u64, Box<pe_execution_core::LiveOrderPreparedAudit>, bool)>::new();
    let mut reservations = BTreeMap::<String, Decimal>::new();
    let mut resolutions = BTreeMap::<String, pe_execution_core::ResolutionFinalizedAudit>::new();
    let mut receivable_by_condition = BTreeMap::<String, CollateralAmount>::new();
    let mut latest_free_collateral = None;
    let mut latest_reconciled_at = None;
    let mut daily_marks = BTreeMap::new();

    for event in events {
        if let LiveJournalPayload::AccountPortfolioMarked(mark) = &event.payload
            && mark.kind == MarkKind::Baseline
        {
            if baseline_seen {
                return Err(ProjectionReducerError::DuplicateBaseline);
            }
            if !mark.venue_positions.is_empty()
                || !mark.prices.is_empty()
                || mark.equity == CollateralAmount::ZERO
                || mark.equity != mark.account_state.collateral_balance
            {
                return Err(ProjectionReducerError::InvalidBaseline);
            }
            baseline_seen = true;
            economic_cash = Some(mark.equity.to_decimal());
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
                let key = order.identity.idempotency_key.clone();
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
            LiveJournalPayload::OrderReconciled(reconciled) => {
                if matches!(
                    reconciled.outcome,
                    LiveJournalOrderOutcome::Killed { .. }
                        | LiveJournalOrderOutcome::Rejected { .. }
                ) {
                    reservations.remove(&reconciled.identity.idempotency_key);
                    if let Some((_, _, terminal)) =
                        prepared_orders.get_mut(&reconciled.identity.idempotency_key)
                    {
                        *terminal = true;
                    }
                }
            }
            LiveJournalPayload::OrderFillFinalized(finalized) => {
                let key = finalized.identity.idempotency_key.clone();
                let Some((prepared_seq, prepared_audit, terminal)) = prepared_orders.get_mut(&key)
                else {
                    return Err(ProjectionReducerError::MissingPrepared);
                };
                if *prepared_seq != finalized.prepared_journal_seq {
                    return Err(ProjectionReducerError::PreparedSequenceMismatch);
                }
                let unique_logs = finalized
                    .matched_logs
                    .iter()
                    .map(|log| (&log.transaction_hash, log.log_index))
                    .collect::<BTreeSet<_>>();
                let logs_are_sorted = finalized.matched_logs.windows(2).all(|pair| {
                    (&pair[0].transaction_hash, pair[0].log_index)
                        < (&pair[1].transaction_hash, pair[1].log_index)
                });
                if finalized.identity != prepared_audit.identity
                    || (*terminal && !fills.contains_key(&key))
                    || finalized.chain_id != pe_venue_polymarket::FINALIZED_CHAIN_ID
                    || finalized.principal != prepared_audit.economic.sizing.principal
                    || finalized.principal != prepared_audit.prepared.maker_collateral
                    || prepared_audit.economic.ladder.minimum_shares
                        != prepared_audit.prepared.taker_shares
                    || finalized.quantity < prepared_audit.economic.ladder.minimum_shares
                    || !fee_within_reserve(finalized.fee, prepared_audit.economic.fee.reserve)
                    || finalized.matched_logs.is_empty()
                    || unique_logs.len() != finalized.matched_logs.len()
                    || !logs_are_sorted
                {
                    return Err(ProjectionReducerError::InvalidFinalizedFill);
                }
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
                    idempotency_key: key.clone(),
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
                let condition = custody.identity.condition_id.0.clone();
                if !resolutions.contains_key(&condition) {
                    return Err(ProjectionReducerError::IdentityConflict);
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
            }
            LiveJournalPayload::AccountPortfolioMarked(mark) if mark.kind == MarkKind::Daily => {
                if daily_marks.insert(mark.cutoff_unix, mark.equity).is_some() {
                    return Err(ProjectionReducerError::DuplicateDailyMark);
                }
                let expected = derive_custody_positions(
                    &prepared_orders,
                    &fills,
                    &resolutions,
                    &receivable_by_condition,
                )?;
                if canonical_positions(&mark.venue_positions)? != expected {
                    return Err(ProjectionReducerError::CustodyInventoryMismatch);
                }
                let receivable = sum_receivable(&receivable_by_condition)?;
                let cash = economic_cash.ok_or(ProjectionReducerError::InvalidBaseline)?;
                require_cash_reconciliation(
                    mark.account_state.collateral_balance,
                    cash,
                    receivable,
                )?;
                require_mark_equity(mark, cash, &prepared_orders, &fills, &resolutions)?;
                latest_free_collateral = Some(mark.account_state.collateral_balance.to_decimal());
                latest_reconciled_at = format_observed_at(mark.account_state.observed_at);
            }
            LiveJournalPayload::AdmissionEvaluated(_)
            | LiveJournalPayload::OrderPreparationFailed(_)
            | LiveJournalPayload::OrderPosted(_)
            | LiveJournalPayload::AccountPortfolioMarked(_)
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
    })
}

type PreparedOrders = BTreeMap<String, (u64, Box<pe_execution_core::LiveOrderPreparedAudit>, bool)>;
type FinalizedFills = BTreeMap<String, (OrderFillFinalizedAudit, LiveFillRow)>;

fn same_finalized_fill(left: &OrderFillFinalizedAudit, right: &OrderFillFinalizedAudit) -> bool {
    left.identity == right.identity
        && left.prepared_journal_seq == right.prepared_journal_seq
        && left.principal == right.principal
        && left.quantity == right.quantity
        && left.fee == right.fee
        && left.matched_logs == right.matched_logs
        && left.chain_id == right.chain_id
        && response_bodies(&left.receipts) == response_bodies(&right.receipts)
}

fn response_bodies(attempts: &[RawHttpAttempt]) -> Vec<(String, Vec<u8>)> {
    attempts
        .iter()
        .filter_map(|attempt| match attempt {
            RawHttpAttempt::Response(response) => {
                Some((response.endpoint_kind.clone(), response.body.clone()))
            }
            RawHttpAttempt::TransportFailure(_) => None,
        })
        .collect()
}

fn resolution_credit(
    condition: &str,
    payout: &BinaryPayoutVector,
    prepared: &PreparedOrders,
    fills: &FinalizedFills,
) -> Result<CollateralAmount, ProjectionReducerError> {
    let mut atomic_by_outcome = [0_u64; 2];
    for (key, (fill, _)) in fills {
        let Some((_, order, _)) = prepared.get(key) else {
            return Err(ProjectionReducerError::MissingPrepared);
        };
        if order.prepared.condition_id.0 != condition {
            continue;
        }
        let outcome = usize::from(order.prepared.outcome_id.0);
        let Some(total) = atomic_by_outcome.get_mut(outcome) else {
            return Err(ProjectionReducerError::IdentityConflict);
        };
        *total = total
            .checked_add(fill.quantity.atomic())
            .ok_or(ProjectionReducerError::Arithmetic)?;
    }
    let credit_atomic = atomic_by_outcome
        .into_iter()
        .zip(payout.decimals())
        .try_fold(0_u64, |total, (shares, payout)| {
            let component = Decimal::from(shares)
                .checked_mul(*payout)
                .ok_or(ProjectionReducerError::Arithmetic)?
                .round_dp_with_strategy(0, RoundingStrategy::ToNegativeInfinity)
                .to_u64()
                .ok_or(ProjectionReducerError::Arithmetic)?;
            total
                .checked_add(component)
                .ok_or(ProjectionReducerError::Arithmetic)
        })?;
    Ok(CollateralAmount::from_atomic(credit_atomic))
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

fn require_mark_equity(
    mark: &pe_execution_core::AccountPortfolioMarkedAudit,
    economic_cash: Decimal,
    prepared: &PreparedOrders,
    fills: &FinalizedFills,
    resolutions: &BTreeMap<String, pe_execution_core::ResolutionFinalizedAudit>,
) -> Result<(), ProjectionReducerError> {
    let mut quantities = BTreeMap::<(String, u8), ShareAmount>::new();
    for (key, (fill, _)) in fills {
        let Some((_, order, _)) = prepared.get(key) else {
            return Err(ProjectionReducerError::MissingPrepared);
        };
        if resolutions.contains_key(&order.prepared.condition_id.0) {
            continue;
        }
        let outcome = u8::try_from(order.prepared.outcome_id.0)
            .map_err(|_| ProjectionReducerError::IdentityConflict)?;
        let quantity = quantities
            .entry((order.prepared.condition_id.0.clone(), outcome))
            .or_insert(ShareAmount::ZERO);
        *quantity = quantity
            .checked_add(fill.quantity)
            .map_err(|_| ProjectionReducerError::Arithmetic)?;
    }
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
    if quantities.len() != prices.len() || quantities.keys().any(|key| !prices.contains_key(key)) {
        return Err(ProjectionReducerError::InvalidMarkPrices);
    }
    let equity = quantities
        .iter()
        .try_fold(economic_cash, |total, (key, quantity)| {
            let price = prices
                .get(key)
                .ok_or(ProjectionReducerError::InvalidMarkPrices)?;
            quantity
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
    account: &AccountContext,
    _now: OffsetDateTime,
) {
    let events = match replay_account(&state.config.journal_path, &account.account_id) {
        Ok(events) => events,
        Err(error) => {
            error!(account_id = %account.account_id, error = %error, "live projection journal replay failed");
            return;
        }
    };
    let derived = match derive_projection_rows(&account.account_id, &events) {
        Ok(derived) => derived,
        Err(error) => {
            error!(account_id = %account.account_id, error = %error, "live projection derivation failed");
            return;
        }
    };
    if let Err(error) = state.config.projection.upsert_fills(&derived.fills).await {
        warn!(account_id = %account.account_id, error = %error, "live fill projection reconcile failed");
    }
    if let Err(error) = state
        .config
        .projection
        .upsert_positions(&derived.positions)
        .await
    {
        warn!(account_id = %account.account_id, error = %error, "live position projection reconcile failed");
    }

    let closed_reason = state
        .closures
        .reason(account.account_id.as_str())
        .map(str::to_owned);
    let Some(row) = compose_account_state_row(account.account_id.as_str(), &derived, closed_reason)
    else {
        return;
    };
    if let Err(error) = state.config.projection.upsert_account_state(&row).await {
        warn!(account_id = %account.account_id, error = %error, "live account-state projection reconcile failed");
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
    for account in &snapshot.accounts {
        reconcile_account_projection(state, account, now).await;
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
    let events = replay_account(&state.config.journal_path, &account_id)?;
    let mut recovered = None;
    for event in events {
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
                recovered = Some(RecoveredPrepared {
                    journal_seq: event.seq,
                    audit: prepared,
                    transaction_hashes: BTreeSet::new(),
                    finalized: false,
                });
            }
            LiveJournalPayload::OrderReconciled(reconciled)
                if reconciled.identity.dispatch_id == target.dispatch_id =>
            {
                if let LiveJournalOrderOutcome::Matched {
                    transaction_hashes, ..
                } = reconciled.outcome
                    && let Some(recovered) = recovered.as_mut()
                {
                    recovered.transaction_hashes.extend(transaction_hashes);
                }
            }
            LiveJournalPayload::OrderFillFinalized(finalized)
                if finalized.identity.dispatch_id == target.dispatch_id =>
            {
                if let Some(recovered) = recovered.as_mut() {
                    if finalized.prepared_journal_seq != recovered.journal_seq {
                        return Err(FanoutError::Signal(
                            "finalized fill references the wrong prepared journal sequence"
                                .to_owned(),
                        ));
                    }
                    recovered.finalized = true;
                }
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
            reconcile_account_projection(state, account, now).await;
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
    let outcome = recover_target(state, target, &venue, now).await?;
    let transition = outcome_transition(&outcome);
    persist_outcome(state, target, &outcome, now)?;
    if let Some(account) = context
        && matches!(outcome, LiveOrderOutcome::Matched { .. })
    {
        reconcile_account_projection(state, account, now).await;
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
    now: OffsetDateTime,
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
    let evidence_hashes = evidence
        .iter()
        .map(|attempt| hash_json(&("prediction-edge/live-http-attempt/v1", attempt)))
        .collect::<Result<Vec<_>, _>>()?;
    state.config.journal.append(
        account_id,
        now,
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
}

async fn fetch_promotions(
    state: &FanoutState,
) -> Result<HashMap<String, PromotionFacts>, &'static str> {
    let token = auth_token(
        &state.config.supabase_anon_key,
        &state.config.supabase_secret_key,
    );
    let url = promotion_events_url(&state.config.supabase_url);
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
    let rows: Vec<AccountPromotionRow> = response.json().await.map_err(|_| "decode")?;
    let mut grouped: HashMap<String, Vec<PromotionEventRow>> = HashMap::new();
    for row in rows {
        grouped
            .entry(row.account_id)
            .or_default()
            .push(PromotionEventRow {
                event_kind: row.event_kind,
                created_at: row.created_at,
            });
    }
    Ok(grouped
        .into_iter()
        .map(|(account, rows)| (account, promotion_facts_from_rows(&rows)))
        .collect())
}

fn promotion_events_url(supabase_url: &str) -> String {
    format!(
        "{}/rest/v1/account_events?select=account_id,event_kind,created_at&event_kind=in.(promotion_reviewed,promotion_review_revoked)&order=created_at.desc&limit=50",
        supabase_url.trim_end_matches('/')
    )
}

struct StaticProbe {
    account: CheckOutcome,
    geoblock: CheckOutcome,
    balance: CheckOutcome,
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
    let promotions = match fetch_promotions(state).await {
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
    let mut armed_count = snapshot
        .accounts
        .iter()
        .filter(|account| account.effective_live_mode == "live_tiny")
        .count();
    for account in &snapshot.accounts {
        let (credential_outcome, probe) = mode_probe(state, account, now).await;
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
) -> (CheckOutcome, StaticProbe) {
    let unavailable = StaticProbe {
        account: CheckOutcome::Transient("account query unavailable"),
        geoblock: CheckOutcome::Transient("geoblock query unavailable"),
        balance: CheckOutcome::Transient("balance query unavailable"),
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
    let account_outcome = if standard.closed_only || neg_risk.closed_only {
        CheckOutcome::PersistentFail("closed_only")
    } else {
        CheckOutcome::Pass
    };
    let geoblock = if standard.geoblocked || neg_risk.geoblocked {
        CheckOutcome::PersistentFail("orders geoblocked")
    } else {
        CheckOutcome::Pass
    };
    let settings = match fetch_account_settings(state, account.account_id.as_str()).await {
        Ok(settings) => settings,
        Err(_) => {
            return (
                CheckOutcome::Pass,
                StaticProbe {
                    account: account_outcome,
                    geoblock,
                    balance: CheckOutcome::Transient("sizing posture unavailable"),
                },
            );
        }
    };
    let runtime = state.config.runtime_config.snapshot();
    let sizing = match settings.sizing_mode(runtime.sizing_mode) {
        Ok(sizing) => sizing,
        Err(_) => {
            return (
                CheckOutcome::Pass,
                StaticProbe {
                    account: account_outcome,
                    geoblock,
                    balance: CheckOutcome::PersistentFail("sizing posture invalid"),
                },
            );
        }
    };
    let balance = standard.collateral_balance.min(neg_risk.collateral_balance);
    let posture = arming_posture(
        sizing,
        balance,
        runtime.per_trade_cap.resolve_bps(TradingMode::LiveTiny),
    );
    let mut balance_outcome = if posture == CollateralAmount::ZERO
        || standard.collateral_balance < posture
        || neg_risk.collateral_balance < posture
        || standard.allowance < posture
        || neg_risk.allowance < posture
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
        },
    )
}

fn live_financial_posture(
    state: &FanoutState,
    account: &AccountContext,
    authenticated_cash: CollateralAmount,
    require_empty_inventory: bool,
) -> CheckOutcome {
    let events = match replay_account(&state.config.journal_path, &account.account_id) {
        Ok(events) => events,
        Err(_) => return CheckOutcome::Transient("live journal unavailable"),
    };
    let derived = match derive_projection_rows(&account.account_id, &events) {
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RedeemablePositionRow {
    condition_id: String,
    size: Decimal,
    #[serde(rename = "negativeRisk")]
    neg_risk: bool,
}

/// Redemption discovery and submission remain fail-closed for every custody kind except the
/// officially documented Deposit Wallet EIP-712 batch path.
async fn drive_redemptions(state: &mut FanoutState, now: OffsetDateTime) {
    let snapshot = state.config.live_accounts.snapshot();
    let account_ids = snapshot
        .accounts
        .iter()
        .map(|account| account.account_id.as_str().to_owned())
        .collect::<HashSet<_>>();
    state
        .closures
        .redemption
        .retain(|account, _| account_ids.contains(account));
    for account in &snapshot.accounts {
        let events = match replay_account(&state.config.journal_path, &account.account_id) {
            Ok(events) => events,
            Err(error) => {
                error!(account_id = %account.account_id, error = %error, "redemption journal replay failed; attempt frozen");
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    "redemption journal unavailable; attempt frozen".to_owned(),
                );
                continue;
            }
        };
        let mut attempts = reconstruct_redemption_attempts(&events);
        let incomplete_attempt = attempts
            .values()
            .any(|attempt| !matches!(attempt.state, RedemptionAttemptState::Complete { .. }));
        let has_receivable = derive_projection_rows(&account.account_id, &events)
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
        let positions = match fetch_redeemable_positions(state, &venue.deposit_wallet()).await {
            Ok(positions) => positions,
            Err(reason) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption inventory unavailable: {reason}"),
                );
                continue;
            }
        };
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
            set_redemption_closure(state, account, closure_reason);
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
        set_redemption_closure(state, account, closure_reason);
    }
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

async fn fetch_redeemable_positions(
    state: &FanoutState,
    wallet: &str,
) -> Result<Vec<RedeemablePositionRow>, &'static str> {
    fetch_redeemable_positions_from(&state.config.http, &state.config.data_base_url, wallet).await
}

async fn fetch_redeemable_positions_from(
    client: &reqwest::Client,
    data_base_url: &str,
    wallet: &str,
) -> Result<Vec<RedeemablePositionRow>, &'static str> {
    let mut positions = Vec::new();
    for offset in (0..=REDEEMABLE_POSITION_MAX_OFFSET).step_by(REDEEMABLE_POSITION_PAGE_LIMIT) {
        let url = format!(
            "{}/positions?user={wallet}&redeemable=true&sizeThreshold=0&limit={REDEEMABLE_POSITION_PAGE_LIMIT}&offset={offset}",
            data_base_url.trim_end_matches('/')
        );
        let response = client.get(url).send().await.map_err(|_| "transport")?;
        if !response.status().is_success() {
            return Err("status");
        }
        let page: Vec<serde_json::Value> = response.json().await.map_err(|_| "decode")?;
        let page_len = page.len();
        for (index, value) in page.into_iter().enumerate() {
            match serde_json::from_value(value) {
                Ok(position) => positions.push(position),
                Err(parse_error) => {
                    error!(
                        wallet,
                        offset,
                        index,
                        error = %parse_error,
                        "redeemable position is unparseable; redemption pass fails closed"
                    );
                    return Err("unparseable position");
                }
            }
        }
        if page_len < REDEEMABLE_POSITION_PAGE_LIMIT {
            return Ok(positions);
        }
        if offset == REDEEMABLE_POSITION_MAX_OFFSET {
            error!(
                wallet,
                offset,
                "redeemable positions pagination bound reached; redemption pass fails closed"
            );
            return Err("pagination bound reached");
        }
    }
    Err("pagination bound reached")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use axum::extract::{Query, State};
    use axum::routing::get;
    use axum::{Json, Router};

    use pe_core_types::SourceTimestamp;
    use pe_core_types::{
        LeaderAction, MarketId, OutcomeId, ProbabilityPpm, ReconstructionQuality, Side,
        SourceTradeId, TraderId, VenueId, VenueMarketId, WalletAddress,
    };
    use pe_execution_core::{
        LiveAccountReadFailure, LiveOrderRejectKind, LivePostClassification, LivePostParseError,
        LiveVenueAccountReadError, LiveVenueAccountState, LiveVenuePreparationError,
        LiveVenuePrepareRequest, LiveVenuePrepared, LiveVenueReconciliation,
        LiveVenueReconciliationError,
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
    fn promotion_events_read_is_latest_first_and_bounded() {
        let url = promotion_events_url("https://example.test/");
        assert!(url.contains("order=created_at.desc"));
        assert!(url.ends_with("limit=50"));
    }

    #[test]
    fn redemption_reconcile_cadence_is_named_five_minutes() {
        assert_eq!(REDEMPTION_RECONCILE_CADENCE_SECS, 300);
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
                })
            })
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

    fn projection_identity(dispatch_id: &str) -> LiveOrderIdentity {
        let signal = projection_signal();
        LiveOrderIdentity {
            dispatch_id: dispatch_id.to_owned(),
            idempotency_key: format!("{dispatch_id}:account"),
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

    fn baseline_event(account_id: &AccountId, seq: u64) -> LiveJournalEvent {
        let cash = CollateralAmount::from_atomic(10_000_000);
        LiveJournalEvent {
            account_id: account_id.clone(),
            seq,
            timestamp: OffsetDateTime::UNIX_EPOCH,
            payload: LiveJournalPayload::AccountPortfolioMarked(Box::new(
                pe_execution_core::AccountPortfolioMarkedAudit {
                    kind: MarkKind::Baseline,
                    cutoff_unix: 0,
                    account_state: pe_execution_core::LiveAccountStateAudit {
                        observed_at: OffsetDateTime::UNIX_EPOCH,
                        closed_only: false,
                        geoblocked: false,
                        selected_spender: "spender".to_owned(),
                        collateral_balance: cash,
                        allowance: cash,
                        reconciled_free_collateral: cash,
                        schema_version: 1,
                        parser_version: 1,
                        evidence: Vec::new(),
                        evidence_hashes: Vec::new(),
                    },
                    venue_positions: Vec::new(),
                    venue_position_receipts: Vec::new(),
                    prices: Vec::new(),
                    equity: cash,
                },
            )),
        }
    }

    fn finality_prepared() -> Box<pe_execution_core::LiveOrderPreparedAudit> {
        let identity = projection_identity("dispatch-finality");
        let condition_id = PolymarketConditionId(format!("0x{}", "77".repeat(32)));
        let token_id = pe_core_types::PolymarketTokenId("123".to_owned());
        let receipt = AppendReceipt {
            sequence: EventSeq(1),
            this_hash: blake3::Hash::from_bytes([1; 32]),
        };
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
                raw_evidence_hash: "settlement".to_owned(),
                source_timestamp_unix: None,
                observed_at_unix: 0,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            fee_schedule: pe_venue_polymarket::CompactFeeSchedule::Zero,
            scheduled_end_unix: None,
            receipts: pe_execution_core::AdmissionReceipts {
                gamma: receipt,
                clob_long: receipt,
                clob_compact: receipt,
            },
        };
        let ladder = LadderPlanAudit {
            used_asks: Vec::new(),
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
            book_receipt: receipt,
            observation: None,
            sizing: SizingAudit {
                mode: SizingModeAudit::Contract { contracts: 3 },
                budget: cash,
                principal: CollateralAmount::from_atomic(2_500_000),
                minimum_shares: ShareAmount::from_atomic(3_125_000),
                expected_shares: ShareAmount::from_atomic(3_125_000),
                expected_vwap: Price(dec!(0.80)),
                all_in_price: Price(dec!(0.80)),
                slippage_rate: Decimal::ZERO,
            },
            fee: FeeAudit {
                schedule: pe_venue_polymarket::CompactFeeSchedule::Zero,
                expected_fee: CollateralAmount::ZERO,
                reserve: CollateralAmount::from_atomic(120),
            },
            risk: RiskAudit {
                snapshot: zeroed_risk_snapshot(),
                decision: RiskDecisionAudit::Approved,
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
            account_state: pe_execution_core::LiveAccountStateAudit {
                observed_at: OffsetDateTime::UNIX_EPOCH,
                closed_only: false,
                geoblocked: false,
                selected_spender: "spender".to_owned(),
                collateral_balance: cash,
                allowance: cash,
                reconciled_free_collateral: cash,
                schema_version: 1,
                parser_version: 1,
                evidence: Vec::new(),
                evidence_hashes: Vec::new(),
            },
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

    struct FakePolygonRpc {
        calls: Mutex<Vec<(String, Option<u64>)>>,
        finalized_head: u64,
        receipt: Vec<u8>,
        receipts_by_hash: BTreeMap<String, Vec<u8>>,
        canonical_hash_byte: &'static str,
    }

    impl FakePolygonRpc {
        fn new(finalized_head: u64) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                finalized_head,
                receipt: include_bytes!(
                    "../../venue-polymarket/tests/fixtures/receipts/standard_v2.json"
                )
                .to_vec(),
                receipts_by_hash: BTreeMap::new(),
                canonical_hash_byte: "aa",
            }
        }

        fn with_receipt(mut self, receipt: Vec<u8>) -> Self {
            self.receipt = receipt;
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

        fn response(endpoint_kind: &str, body: Vec<u8>) -> RawHttpAttempt {
            RawHttpAttempt::Response(pe_core_types::RawHttpResponse {
                source_id: "fixture".to_owned(),
                endpoint_kind: endpoint_kind.to_owned(),
                method: "POST".to_owned(),
                path: "fixture://polygon".to_owned(),
                ordered_query: Vec::new(),
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
            Box::pin(async {
                Self::response(
                    "polygon-chain-id",
                    br#"{"jsonrpc":"2.0","id":1,"result":"0x89"}"#.to_vec(),
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
            Box::pin(async move { Self::response("polygon-transaction-receipt", receipt) })
        }

        fn finalized_block(&self) -> Pin<Box<dyn Future<Output = RawHttpAttempt> + Send + '_>> {
            self.calls
                .lock()
                .unwrap()
                .push(("eth_getBlockByNumber.finalized".to_owned(), None));
            let number = self.finalized_head;
            Box::pin(async move {
                let hash = if number == 100 { "aa" } else { "bb" };
                Self::response(
                    "polygon-finalized-block",
                    serde_json::to_vec(&serde_json::json!({
                        "jsonrpc":"2.0","id":1,"result":{
                            "number":format!("0x{number:x}"),
                            "hash":format!("0x{}", hash.repeat(32))
                        }
                    }))
                    .unwrap(),
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
            Box::pin(async move {
                Self::response(
                    "polygon-canonical-block",
                    serde_json::to_vec(&serde_json::json!({
                        "jsonrpc":"2.0","id":1,"result":{
                            "number":format!("0x{number:x}"),
                            "hash":format!("0x{}", hash.repeat(32))
                        }
                    }))
                    .unwrap(),
                )
            })
        }
    }

    /// PASS: one pass reads each distinct receipt and lower block once, without a range query.
    #[tokio::test]
    async fn finality_batch_deduplicates_receipts_and_block_reads() {
        let rpc = FakePolygonRpc::new(101);
        let order = PendingOrderFinality {
            prepared_journal_seq: 9,
            prepared: finality_prepared(),
            transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
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
        }
        assert_eq!(rpc.call_count("eth_chainId"), 1);
        assert_eq!(rpc.call_count("eth_getTransactionReceipt"), 1);
        assert_eq!(rpc.call_count("eth_getBlockByNumber.finalized"), 1);
        assert_eq!(rpc.call_count("eth_getBlockByNumber.canonical"), 1);
        assert_eq!(rpc.call_count("eth_getLogs"), 0);
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

    /// PASS: a receipt at the head reuses that head; an above-head receipt stays pending.
    #[tokio::test]
    async fn finality_equal_and_above_head_issue_no_canonical_block_read() {
        let order = PendingOrderFinality {
            prepared_journal_seq: 9,
            prepared: finality_prepared(),
            transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
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

    /// PASS: pending, conflict, and finalized Polygon decisions synchronize through frozen facts.
    #[tokio::test]
    async fn finality_results_append_before_terminalization() {
        let account_id = AccountId::new("account").unwrap();
        let order = PendingOrderFinality {
            prepared_journal_seq: 9,
            prepared: finality_prepared(),
            transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
        };
        let mut pending =
            collect_order_finality(&FakePolygonRpc::new(99), vec![order.clone()]).await;
        let mut conflict = collect_order_finality(
            &FakePolygonRpc::new(101).with_canonical_hash("cc"),
            vec![order.clone()],
        )
        .await;
        let mut finalized = collect_order_finality(&FakePolygonRpc::new(101), vec![order]).await;
        let dir = tempdir().unwrap();
        let path = dir.path().join("live.log");
        let journal = LiveJournal::open(&path).unwrap();
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
        let events = replay_account(&path, &account_id).unwrap();
        assert!(matches!(
            events.first().map(|event| &event.payload),
            Some(LiveJournalPayload::OrderReconciled(reconciled))
                if reconciled.source == LiveReconciliationSource::PolygonFinality
                    && matches!(
                        reconciled.outcome,
                        LiveJournalOrderOutcome::FinalityPending { .. }
                    )
        ));
        assert!(matches!(
            events.get(1).map(|event| &event.payload),
            Some(LiveJournalPayload::OrderReconciled(reconciled))
                if matches!(
                    reconciled.outcome,
                    LiveJournalOrderOutcome::FinalityConflict { .. }
                )
        ));
        assert!(matches!(
            events.get(2).map(|event| &event.payload),
            Some(LiveJournalPayload::OrderFillFinalized(_))
        ));
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

    async fn one_finality_disposition(rpc: &FakePolygonRpc) -> OrderFinalityDisposition {
        collect_order_finality(
            rpc,
            vec![PendingOrderFinality {
                prepared_journal_seq: 9,
                prepared: finality_prepared(),
                transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
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

    /// PASS: a second Baseline in one account era is a typed reducer conflict.
    #[test]
    fn second_baseline_in_the_same_era_conflicts() {
        let account_id = AccountId::new("account").unwrap();
        assert!(matches!(
            derive_projection_rows(
                &account_id,
                &[
                    baseline_event(&account_id, 0),
                    baseline_event(&account_id, 1)
                ]
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
            derive_projection_rows(
                &account_id,
                &[baseline_event(&account_id, 1), daily, repeated]
            ),
            Err(ProjectionReducerError::DuplicateDailyMark)
        ));
    }

    fn finalized_fill_event(
        account_id: &AccountId,
        prepared: &pe_execution_core::LiveOrderPreparedAudit,
        seq: u64,
        prepared_seq: u64,
    ) -> LiveJournalEvent {
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
                    transaction_hash: format!("0x{}", "11".repeat(32)),
                    log_index: 7,
                }],
                chain_id: 137,
                finalized_head: 100,
                receipts: Vec::new(),
                blocks: Vec::new(),
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

    /// PASS: resolution changes economics once; custody reconciliation changes no equity.
    #[test]
    fn finalized_resolution_and_custody_replay_converge_once() {
        let account_id = AccountId::new("account").unwrap();
        let prepared = finality_prepared();
        let mut repeated_fill = finalized_fill_event(&account_id, &prepared, 4, 2);
        if let LiveJournalPayload::OrderFillFinalized(finalized) = &mut repeated_fill.payload {
            finalized.finalized_head = 101;
        }
        let mut events = vec![
            baseline_event(&account_id, 1),
            LiveJournalEvent {
                account_id: account_id.clone(),
                seq: 2,
                timestamp: OffsetDateTime::from_unix_timestamp(5).unwrap(),
                payload: LiveJournalPayload::OrderPrepared(prepared.clone()),
            },
            finalized_fill_event(&account_id, &prepared, 3, 2),
        ];
        let filled = derive_projection_rows(&account_id, &events).unwrap();
        assert_eq!(filled.positions.len(), 1);
        assert!(filled.positions.first().is_some_and(|position| {
            position.long_contracts == dec!(3.125000)
                && position.short_contracts == Decimal::ZERO
                && position.cost_basis == dec!(2.500120)
        }));
        events.extend([
            repeated_fill,
            resolution_event(&account_id, 5),
            resolution_event(&account_id, 6),
        ]);
        let resolved = derive_projection_rows(&account_id, &events).unwrap();
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
        events.push(LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 7,
            timestamp: OffsetDateTime::from_unix_timestamp(30).unwrap(),
            payload: LiveJournalPayload::RedemptionCustodyReconciled(Box::new(
                pe_execution_core::RedemptionCustodyReconciledAudit {
                    identity: RedemptionAttemptIdentity {
                        account_id: account_id.clone(),
                        condition_id: PolymarketConditionId(format!("0x{}", "77".repeat(32))),
                        adapter: "adapter".to_owned(),
                        custody_wallet: "custody".to_owned(),
                    },
                    account_state: pe_execution_core::LiveAccountStateAudit {
                        observed_at: OffsetDateTime::from_unix_timestamp(30).unwrap(),
                        closed_only: false,
                        geoblocked: false,
                        selected_spender: "spender".to_owned(),
                        collateral_balance: reconciled_cash,
                        allowance: reconciled_cash,
                        reconciled_free_collateral: reconciled_cash,
                        schema_version: 1,
                        parser_version: 1,
                        evidence: Vec::new(),
                        evidence_hashes: Vec::new(),
                    },
                    venue_positions: Vec::new(),
                    venue_position_receipts: Vec::new(),
                },
            )),
        });
        let custody = derive_projection_rows(&account_id, &events).unwrap();
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
        let admission = LiveAdmissionBuilder::new(
            http.clone(),
            "http://127.0.0.1:9".to_owned(),
            "http://127.0.0.1:9".to_owned(),
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
                identity,
                journal,
                journal_path,
                projection: LiveProjectionWriter::new(http.clone(), supabase_url, "anon", ""),
                book_fetcher: Arc::new(ReqwestClobBookFetcher::new(http.clone())),
                http,
                supabase_url: supabase_url.to_owned(),
                supabase_anon_key: "anon".to_owned(),
                supabase_secret_key: String::new(),
                gamma_base_url: "http://127.0.0.1:9".to_owned(),
                clob_base_url: "http://127.0.0.1:9".to_owned(),
                data_base_url: "http://127.0.0.1:9".to_owned(),
                projection_reconcile_interval_secs: 3_600,
            },
            admission,
            closures: AdmissionClosures::default(),
            last_mode_unix: None,
            last_redemption_unix: None,
            last_projection_unix: None,
            last_prune_unix: None,
        }
    }

    async fn counting_fallback(
        State(hits): State<Arc<Mutex<Vec<String>>>>,
        uri: axum::http::Uri,
    ) -> Json<Vec<serde_json::Value>> {
        hits.lock().unwrap().push(uri.path().to_owned());
        Json(Vec::new())
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
        let control = process_target(&mut state, &seed, &target, &projection_signal(), now)
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
        let control = process_target(&mut state, &seed, &target, &projection_signal(), now)
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
        let prepared = finality_prepared();
        let prepared_event = state
            .config
            .journal
            .append(
                account_id.clone(),
                OffsetDateTime::UNIX_EPOCH,
                LiveJournalPayload::OrderPrepared(prepared.clone()),
            )
            .unwrap();
        let fill = finalized_fill_event(
            &account_id,
            &prepared,
            prepared_event.seq + 1,
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

    #[derive(Clone)]
    struct PositionPageState {
        offsets: Arc<Mutex<Vec<usize>>>,
    }

    async fn position_page(
        State(state): State<PositionPageState>,
        Query(query): Query<HashMap<String, String>>,
    ) -> Json<Vec<serde_json::Value>> {
        let offset = query
            .get("offset")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap();
        assert_eq!(query.get("redeemable").map(String::as_str), Some("true"));
        assert_eq!(query.get("limit").map(String::as_str), Some("500"));
        state.offsets.lock().unwrap().push(offset);
        let count = if offset == 0 { 500 } else { 1 };
        Json(
            (0..count)
                .map(|index| {
                    serde_json::json!({
                        "conditionId": format!(
                            "0x{index:064x}"
                        ),
                        "size": 1,
                        "negativeRisk": index % 2 == 0
                    })
                })
                .collect(),
        )
    }

    #[tokio::test]
    async fn redeemable_positions_fetch_walks_offset_pages_to_exhaustion() {
        let offsets = Arc::new(Mutex::new(Vec::new()));
        let app = Router::new()
            .route("/positions", get(position_page))
            .with_state(PositionPageState {
                offsets: offsets.clone(),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap();
        let positions = fetch_redeemable_positions_from(
            &client,
            &format!("http://{address}"),
            "0x1111111111111111111111111111111111111111",
        )
        .await
        .unwrap();
        assert_eq!(positions.len(), 501);
        assert_eq!(*offsets.lock().unwrap(), vec![0, 500]);
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
