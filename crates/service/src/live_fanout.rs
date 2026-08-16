//! Strictly sequential ordinary-live dispatch consumer (#508 Decision 10).

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use age::x25519::Identity;
use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    AccountId, BasisPoints, CollateralAmount, PolymarketConditionId, Price, Probability,
};
use pe_execution_core::{
    CredentialBindingIdentity, FrozenLiveTarget, LiveAdmissionRefusal, LiveControlMode,
    LiveExecutedAmounts, LiveExecutor, LiveFillProjectionIdentity, LiveJournal, LiveJournalEvent,
    LiveJournalOrderOutcome, LiveJournalPayload, LiveModeSnapshot, LiveModeTransitionAudit,
    LiveModeTransitionReason, LiveOrderAmbiguityKind, LiveOrderIdentity, LiveOrderOutcome,
    LiveOrderReconciliationAudit, LiveOrderVenue, LivePrepareResult, LiveReconciliationSource,
    LiveVenueReconciledOutcome, RedemptionAttempt, RedemptionAttemptIdentity,
    RedemptionAttemptState, RedemptionEvent, RedemptionPassInput, advance,
    reconstruct_redemption_attempts, redemption_posture, replay_account, run_redemption_pass,
};
use pe_paper_state::{DispatchSeedRow, DispatchTargetRow, PaperStateDb};
use pe_risk_engine::snapshot::{RiskSnapshot, TradingMode};
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::{ExecutionMode, SizingMode, WinnerFollowStrategy};
use pe_venue_polymarket::{
    CustodyKind, LadderError, RedemptionTransport, RelayerApiKeyCredentials, RelayerCredentials,
    RelayerPollPolicy, build_redemption_call, plan_budget_buy, sign_deposit_wallet_redemption,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
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
    LiveAdmissionBuilder, LiveRedemptionAdapter, LiveVenueAdapterError, PolymarketLiveVenue,
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
    account_state_inputs: HashMap<String, AccountStateInputs>,
}

#[derive(Default)]
struct AccountStateInputs {
    free_collateral: Option<Decimal>,
    unredeemed_value: Option<Decimal>,
    last_reconciled_at: Option<String>,
}

/// Start the first-boot arming fence, then drive mode, redemption, retention, and ordered fan-out.
pub async fn run_live_fanout(config: LiveFanoutConfig) {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    match config.paper_state.record_live_executor_first_boot(now) {
        Ok(fence) => info!(
            fence_unix = fence,
            "ordinary live executor first-boot fence ready"
        ),
        Err(error) => {
            error!(error = %error, "live executor fence could not be recorded; fan-out stopped");
            return;
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
        account_state_inputs: HashMap::new(),
    };
    reconcile_projections(&mut state, OffsetDateTime::now_utc()).await;
    state.last_projection_unix = Some(OffsetDateTime::now_utc().unix_timestamp());
    let mut ticker = tokio::time::interval(Duration::from_secs(FANOUT_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
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
                if let Err(error) = run_dispatch_pass(&mut state, now).await {
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
    let minimum_price = parse_band_price(&runtime.min_fill_price, Price::ZERO);
    let maximum_price = parse_band_price(&runtime.max_fill_price, Price(Decimal::ONE));
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
    };
    let executor = LiveExecutor::new(&venue, state.config.journal.as_ref());
    match executor.prepare(request, now).await? {
        LivePrepareResult::Terminal(outcome) => {
            if refusal_is_transient(&outcome) {
                return Ok(PassControl::StopSeed);
            }
            let transition = outcome_transition(&outcome);
            persist_outcome(state, target, &outcome, now)?;
            capture_account_state_row(state, account, &account_state, now);
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
            capture_account_state_row(state, account, &account_state, now);
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

fn parse_band_price(raw: &str, disabled: Price) -> Result<Price, ()> {
    let value = Decimal::from_str(raw).map_err(|_| ())?;
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
        onchain_source_status: SourceStatus::Healthy,
        copy_latency_p95_ms: 0,
        trading_mode: TradingMode::LiveTiny,
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

fn capture_account_state_row(
    state: &mut FanoutState,
    account: &AccountContext,
    account_state: &pe_execution_core::LiveVenueAccountState,
    now: OffsetDateTime,
) {
    let inputs = state
        .account_state_inputs
        .entry(account.account_id.as_str().to_owned())
        .or_default();
    inputs.free_collateral = Some(account_state.reconciled_free_collateral.to_decimal());
    inputs.last_reconciled_at = now
        .format(&time::format_description::well_known::Rfc3339)
        .ok();
}

async fn capture_account_state(
    state: &mut FanoutState,
    account: &AccountContext,
    venue: &PolymarketLiveVenue,
    neg_risk: bool,
    now: OffsetDateTime,
) {
    if let Ok(account_state) = venue.read_balance_and_allowance(neg_risk).await {
        capture_account_state_row(state, account, &account_state, now);
    }
}

#[derive(Default)]
struct ProjectionDerivation {
    fills: Vec<LiveFillRow>,
    positions: Vec<LivePositionRow>,
    reserved: Decimal,
    latest_free_collateral: Option<Decimal>,
    latest_reconciled_at: Option<String>,
}

fn executed_fill_price(executed: &LiveExecutedAmounts) -> Option<Decimal> {
    if executed.making_amount <= Decimal::ZERO
        || executed.taking_amount <= Decimal::ZERO
        || executed.taking_amount.fract() != Decimal::ZERO
    {
        return None;
    }
    let price = executed.making_amount.checked_div(executed.taking_amount)?;
    Price::new(price).ok().map(|value| value.0)
}

fn derive_projection_rows(
    paper_state: &PaperStateDb,
    account_id: &AccountId,
    events: &[LiveJournalEvent],
) -> Result<ProjectionDerivation, FanoutError> {
    let mut fills = HashMap::<String, (LiveFillRow, Decimal)>::new();
    let mut reservations = HashMap::<String, Decimal>::new();
    let mut redeemed_conditions = HashSet::<String>::new();
    let mut latest_free_collateral = None;
    let mut latest_reconciled_at = None;

    for event in events {
        match &event.payload {
            LiveJournalPayload::AdmissionEvaluated(audit) => {
                if let Some(account_state) = audit.account_state.as_ref() {
                    latest_free_collateral =
                        Some(account_state.reconciled_free_collateral.to_decimal());
                    latest_reconciled_at = account_state
                        .observed_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .ok();
                }
            }
            LiveJournalPayload::OrderPrepared(prepared) => {
                reservations.insert(
                    prepared.identity.idempotency_key.clone(),
                    prepared.ladder.worst_case_debit.to_decimal(),
                );
            }
            LiveJournalPayload::OrderReconciled(reconciled) => {
                if !matches!(
                    reconciled.outcome,
                    LiveJournalOrderOutcome::Ambiguous { .. }
                ) {
                    reservations.remove(&reconciled.identity.idempotency_key);
                }
                let LiveJournalOrderOutcome::Matched {
                    executed: Some(executed),
                    ..
                } = &reconciled.outcome
                else {
                    if matches!(
                        reconciled.outcome,
                        LiveJournalOrderOutcome::Matched { executed: None, .. }
                    ) {
                        error!(
                            account_id = %account_id,
                            dispatch_id = %reconciled.identity.dispatch_id,
                            "matched live order lacks executed amounts; fill projection remains pending"
                        );
                    }
                    continue;
                };
                let Some(fill_price) = executed_fill_price(executed) else {
                    error!(
                        account_id = %account_id,
                        dispatch_id = %reconciled.identity.dispatch_id,
                        "matched live order has invalid executed amounts; fill projection remains pending"
                    );
                    continue;
                };
                let projection = if let Some(projection) =
                    reconciled.identity.fill_projection.as_deref()
                {
                    projection.clone()
                } else {
                    let Some(seed) = paper_state.dispatch_seed(&reconciled.identity.dispatch_id)?
                    else {
                        error!(
                            account_id = %account_id,
                            dispatch_id = %reconciled.identity.dispatch_id,
                            "matched live order has neither journaled nor frozen dispatch metadata; fill projection remains pending"
                        );
                        continue;
                    };
                    let signal = match parse_frozen_signal(&seed) {
                        Ok(signal) => signal,
                        Err(error) => {
                            error!(
                                account_id = %account_id,
                                dispatch_id = %reconciled.identity.dispatch_id,
                                error = %error,
                                "matched live order frozen signal is invalid; fill projection remains pending"
                            );
                            continue;
                        }
                    };
                    LiveFillProjectionIdentity {
                        leader_wallet: signal.leader.to_string(),
                        source_trade_id: Some(seed.source_trade_id),
                        market_id: signal.market_id.to_string(),
                        outcome_id: i64::from(signal.outcome_id.0),
                        side: "buy".to_owned(),
                    }
                };
                let Ok(event_seq) = i64::try_from(event.seq) else {
                    error!(
                        account_id = %account_id,
                        dispatch_id = %reconciled.identity.dispatch_id,
                        "matched live order event sequence overflow; fill projection remains pending"
                    );
                    continue;
                };
                fills
                    .entry(reconciled.identity.idempotency_key.clone())
                    .or_insert_with(|| {
                        (
                            LiveFillRow {
                                account_id: account_id.as_str().to_owned(),
                                idempotency_key: reconciled.identity.idempotency_key.clone(),
                                leader_wallet: projection.leader_wallet,
                                source_trade_id: projection.source_trade_id,
                                market_id: projection.market_id,
                                outcome_id: projection.outcome_id,
                                side: projection.side,
                                contracts: executed.taking_amount,
                                fill_price,
                                entry_unix: Some(event.timestamp.unix_timestamp()),
                                event_seq,
                            },
                            executed.making_amount,
                        )
                    });
            }
            LiveJournalPayload::RedemptionReceiptTransition(receipt)
                if receipt.status == pe_execution_core::RedemptionReceiptStatusAudit::Confirmed =>
            {
                redeemed_conditions.insert(receipt.identity.condition_id.0.clone());
            }
            LiveJournalPayload::OrderPreparationFailed(_)
            | LiveJournalPayload::OrderPosted(_)
            | LiveJournalPayload::RedemptionRequested(_)
            | LiveJournalPayload::RedemptionTransactionIdentified(_)
            | LiveJournalPayload::RedemptionReceiptTransition(_)
            | LiveJournalPayload::CredentialBindingMismatch { .. }
            | LiveJournalPayload::ModeTransitionApplied(_) => {}
        }
    }

    let mut position_totals = HashMap::<(String, i64), (i64, Decimal)>::new();
    for (row, collateral) in fills.values() {
        let Some(contracts) = row.contracts.to_i64() else {
            error!(
                account_id = %account_id,
                idempotency_key = %row.idempotency_key,
                "matched live order contracts overflow position projection"
            );
            continue;
        };
        let total = position_totals
            .entry((row.market_id.clone(), row.outcome_id))
            .or_insert((0, Decimal::ZERO));
        total.0 = total.0.saturating_add(contracts);
        total.1 += *collateral;
    }
    for ((market_id, _), total) in &mut position_totals {
        if redeemed_conditions.contains(market_id) {
            *total = (0, Decimal::ZERO);
        }
    }

    let mut fill_rows = fills.into_values().map(|(row, _)| row).collect::<Vec<_>>();
    fill_rows.sort_by(|left, right| left.idempotency_key.cmp(&right.idempotency_key));
    let mut positions = position_totals
        .into_iter()
        .map(
            |((market_id, outcome_id), (long_contracts, cost_basis))| LivePositionRow {
                account_id: account_id.as_str().to_owned(),
                market_id,
                outcome_id,
                long_contracts,
                short_contracts: 0,
                cost_basis,
            },
        )
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| {
        (&left.market_id, left.outcome_id).cmp(&(&right.market_id, right.outcome_id))
    });

    Ok(ProjectionDerivation {
        fills: fill_rows,
        positions,
        reserved: reservations.into_values().sum(),
        latest_free_collateral,
        latest_reconciled_at,
    })
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
    let derived = match derive_projection_rows(
        state.config.paper_state.as_ref(),
        &account.account_id,
        &events,
    ) {
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
    let Some(row) = compose_account_state_row(
        account.account_id.as_str(),
        state.account_state_inputs.get(account.account_id.as_str()),
        &derived,
        closed_reason,
    ) else {
        return;
    };
    if let Err(error) = state.config.projection.upsert_account_state(&row).await {
        warn!(account_id = %account.account_id, error = %error, "live account-state projection reconcile failed");
    }
}

fn compose_account_state_row(
    account_id: &str,
    inputs: Option<&AccountStateInputs>,
    derived: &ProjectionDerivation,
    admission_closed_reason: Option<String>,
) -> Option<LiveAccountStateRow> {
    let free_collateral = inputs
        .and_then(|inputs| inputs.free_collateral)
        .or(derived.latest_free_collateral);
    let last_reconciled_at = inputs
        .and_then(|inputs| inputs.last_reconciled_at.clone())
        .or_else(|| derived.latest_reconciled_at.clone());
    let unredeemed_value = inputs.and_then(|inputs| inputs.unredeemed_value);
    if free_collateral.is_none()
        && unredeemed_value.is_none()
        && derived.reserved == Decimal::ZERO
        && admission_closed_reason.is_none()
    {
        return None;
    }
    Some(LiveAccountStateRow {
        account_id: account_id.to_owned(),
        free_collateral: free_collateral.unwrap_or(Decimal::ZERO),
        reserved: derived.reserved,
        unredeemed_value: unredeemed_value.unwrap_or(Decimal::ZERO),
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
    identity: LiveOrderIdentity,
    order_hash: String,
}

fn recovered_prepared(
    state: &FanoutState,
    target: &DispatchTargetRow,
) -> Result<Option<RecoveredPrepared>, FanoutError> {
    let account_id = AccountId::new(&target.account_id)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let events = replay_account(&state.config.journal_path, &account_id)?;
    let prepared = events
        .into_iter()
        .rev()
        .find_map(|event| match event.payload {
            LiveJournalPayload::OrderPrepared(prepared)
                if prepared.identity.dispatch_id == target.dispatch_id =>
            {
                Some(RecoveredPrepared {
                    identity: prepared.identity.clone(),
                    order_hash: prepared.prepared.order_hash.clone(),
                })
            }
            _ => None,
        });
    Ok(prepared)
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
    if let Some(account) = context {
        capture_account_state(state, account, &venue, false, now).await;
        if matches!(outcome, LiveOrderOutcome::Matched { .. }) {
            reconcile_account_projection(state, account, now).await;
        }
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
    let account_id = AccountId::new(&target.account_id)
        .map_err(|error| FanoutError::Signal(error.to_string()))?;
    let reconciliation = venue
        .reconcile_and_cancel_by_order_hash(&prepared.order_hash)
        .await;
    let (journal_outcome, outcome, evidence) = match reconciliation {
        Ok(reconciliation) => match reconciliation.outcome {
            LiveVenueReconciledOutcome::Matched { venue_order_id } => (
                LiveJournalOrderOutcome::Matched {
                    venue_order_id: venue_order_id.clone(),
                    executed: None,
                },
                LiveOrderOutcome::Matched {
                    order_hash: prepared.order_hash.clone(),
                    venue_order_id,
                    executed: None,
                },
                reconciliation.evidence,
            ),
            LiveVenueReconciledOutcome::Killed { venue_order_id } => (
                LiveJournalOrderOutcome::Killed {
                    venue_order_id: venue_order_id.clone(),
                },
                LiveOrderOutcome::Killed {
                    order_hash: prepared.order_hash.clone(),
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
                    order_hash: Some(prepared.order_hash.clone()),
                    venue_order_id,
                    kind: pe_execution_core::LiveOrderRejectKind::VenueRejected,
                },
                reconciliation.evidence,
            ),
            LiveVenueReconciledOutcome::Ambiguous { kind } => (
                LiveJournalOrderOutcome::Ambiguous { kind },
                LiveOrderOutcome::Ambiguous {
                    order_hash: prepared.order_hash.clone(),
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
                order_hash: prepared.order_hash.clone(),
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
            identity: prepared.identity,
            order_hash: prepared.order_hash,
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
    now: OffsetDateTime,
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
    let inputs = state
        .account_state_inputs
        .entry(account.account_id.as_str().to_owned())
        .or_default();
    inputs.free_collateral = Some(balance.to_decimal());
    inputs.last_reconciled_at = now
        .format(&time::format_description::well_known::Rfc3339)
        .ok();
    let posture = arming_posture(
        sizing,
        balance,
        runtime.per_trade_cap.resolve_bps(TradingMode::LiveTiny),
    );
    let balance_outcome = if posture == CollateralAmount::ZERO
        || standard.collateral_balance < posture
        || neg_risk.collateral_balance < posture
        || standard.allowance < posture
        || neg_risk.allowance < posture
    {
        CheckOutcome::PersistentFail("balance or both-spender allowance below posture")
    } else {
        CheckOutcome::Pass
    };
    (
        CheckOutcome::Pass,
        StaticProbe {
            account: account_outcome,
            geoblock,
            balance: balance_outcome,
        },
    )
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
    let armed = snapshot.armed_targets();
    let armed_ids = armed
        .iter()
        .map(|account| account.account_id.as_str().to_owned())
        .collect::<HashSet<_>>();
    state
        .closures
        .redemption
        .retain(|account, _| armed_ids.contains(account));
    for account in armed {
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
        let unredeemed = positions
            .iter()
            .map(|position| position.size.max(Decimal::ZERO))
            .sum::<Decimal>();
        capture_redemption_state(state, account, unredeemed, now);
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
                Ok(result) => redemption_closure_reason(result.attempt, redeemable, now),
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
            Ok(result) => redemption_closure_reason(result.attempt, redeemable, now),
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

fn redemption_closure_reason(
    mut attempt: RedemptionAttempt,
    redeemable: CollateralAmount,
    now: OffsetDateTime,
) -> Option<String> {
    if matches!(
        attempt.state,
        RedemptionAttemptState::ConfirmedAwaitingBalance { .. }
    ) && redeemable == CollateralAmount::ZERO
    {
        let (state, _) = advance(
            attempt.state,
            RedemptionEvent::BalanceReconciled {
                reconciled_at: now,
                credited_collateral: CollateralAmount::ZERO,
                remaining_redeemable: CollateralAmount::ZERO,
            },
        );
        attempt.state = state;
    }
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

fn capture_redemption_state(
    state: &mut FanoutState,
    account: &AccountContext,
    unredeemed: Decimal,
    now: OffsetDateTime,
) {
    let inputs = state
        .account_state_inputs
        .entry(account.account_id.as_str().to_owned())
        .or_default();
    inputs.unredeemed_value = Some(unredeemed);
    inputs.last_reconciled_at = now
        .format(&time::format_description::well_known::Rfc3339)
        .ok();
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
        ContractQty, LeaderAction, MarketId, OutcomeId, ProbabilityPpm, Quantity,
        ReconstructionQuality, Side, SourceTradeId, TraderId, VenueId, VenueMarketId,
        WalletAddress,
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
        for (outcome, reason) in [
            (matched, "filled"),
            (killed, "killed"),
            (rejected, "rejected"),
        ] {
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
            leader_size: Quantity(ContractQty(10)),
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
                    executed: Some(LiveExecutedAmounts {
                        making_amount: dec!(4.00),
                        taking_amount: dec!(10),
                    }),
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

    #[test]
    fn journal_projection_reconcile_heals_dropped_write_and_deduplicates_dispatch() {
        let (_dir, db) = db();
        let account_id = AccountId::new("account").unwrap();
        let dispatch_id = "dispatch-projection";
        let events = vec![
            matched_projection_event(&account_id, dispatch_id, 7),
            matched_projection_event(&account_id, dispatch_id, 8),
        ];
        let rows = derive_projection_rows(&db, &account_id, &events).unwrap();
        assert_eq!(rows.fills.len(), 1, "duplicate dispatch converges");
        assert_eq!(rows.fills[0].contracts, dec!(10));
        assert_eq!(
            rows.fills[0].fill_price,
            dec!(0.40),
            "executed price improvement must replace the 0.50 plan quote"
        );
        assert_eq!(rows.fills[0].event_seq, 7);
        assert_eq!(rows.positions.len(), 1);
        assert_eq!(rows.positions[0].long_contracts, 10);
        assert_eq!(rows.positions[0].cost_basis, dec!(4.00));
    }

    #[test]
    fn confirmed_redemption_receipt_zeroes_the_derived_position() {
        let (_dir, db) = db();
        let account_id = AccountId::new("account").unwrap();
        let dispatch_id = "dispatch-redeemed";
        let signal = projection_signal();
        let condition_id = signal.market_id.0.0.clone();
        let receipt = LiveJournalEvent {
            account_id: account_id.clone(),
            seq: 8,
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_001).unwrap(),
            payload: LiveJournalPayload::RedemptionReceiptTransition(Box::new(
                pe_execution_core::RedemptionReceiptAudit {
                    identity: RedemptionAttemptIdentity {
                        account_id: account_id.clone(),
                        condition_id: PolymarketConditionId(condition_id),
                        adapter: NEGRISK_COLLATERAL_ADAPTER.to_owned(),
                        custody_wallet: "0x1111111111111111111111111111111111111111".to_owned(),
                    },
                    attempt_count: 1,
                    transaction_id: "tx".to_owned(),
                    transaction_hash: Some("0xreceipt".to_owned()),
                    status: pe_execution_core::RedemptionReceiptStatusAudit::Confirmed,
                    evidence: Vec::new(),
                    evidence_hashes: Vec::new(),
                },
            )),
        };
        let rows = derive_projection_rows(
            &db,
            &account_id,
            &[
                matched_projection_event(&account_id, dispatch_id, 7),
                receipt,
            ],
        )
        .unwrap();
        assert_eq!(rows.positions[0].long_contracts, 0);
        assert_eq!(rows.positions[0].cost_basis, Decimal::ZERO);
    }

    #[test]
    fn single_account_state_writer_composes_dispatch_and_redemption_inputs() {
        let inputs = AccountStateInputs {
            free_collateral: Some(dec!(12.50)),
            unredeemed_value: Some(dec!(3)),
            last_reconciled_at: Some("2026-08-11T12:00:00Z".to_owned()),
        };
        let derived = ProjectionDerivation {
            reserved: dec!(2.50),
            ..ProjectionDerivation::default()
        };
        let row = compose_account_state_row(
            "account",
            Some(&inputs),
            &derived,
            Some("redemption pending".to_owned()),
        )
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
            account_state_inputs: HashMap::new(),
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
                    LiveVenueReconciledOutcome::Matched { venue_order_id } => {
                        LiveOrderOutcome::Matched {
                            order_hash: target.account_id.clone(),
                            venue_order_id,
                            executed: None,
                        }
                    }
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
