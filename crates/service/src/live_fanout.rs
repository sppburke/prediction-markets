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
    LiveExecutor, LiveJournal, LiveJournalOrderOutcome, LiveJournalPayload, LiveModeSnapshot,
    LiveOrderAmbiguityKind, LiveOrderIdentity, LiveOrderOutcome, LiveOrderReconciliationAudit,
    LiveOrderVenue, LivePrepareResult, LiveReconciliationSource, LiveVenueReconciledOutcome,
    RedemptionAttempt, RedemptionAttemptIdentity, RedemptionAttemptState, RedemptionPassInput,
    redemption_posture, replay_account, run_redemption_pass,
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
use crate::live_projections::{LiveAccountStateRow, LiveFillRow, LiveProjectionWriter};
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
    pub reconcile_interval_secs: u64,
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
    last_prune_unix: Option<i64>,
    touched_state: HashMap<String, LiveAccountStateRow>,
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
        last_prune_unix: None,
        touched_state: HashMap::new(),
    };
    let mut ticker = tokio::time::interval(Duration::from_secs(FANOUT_INTERVAL_SECS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now = OffsetDateTime::now_utc();
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
        let redemption_interval =
            i64::try_from(state.config.reconcile_interval_secs.max(1)).unwrap_or(i64::MAX);
        if due(
            state.last_redemption_unix,
            now.unix_timestamp(),
            redemption_interval,
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

enum PassControl {
    Continue,
    StopSeed,
    FreezePass,
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
        state.touched_state.clear();
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
        if state
            .config
            .paper_state
            .finalize_dispatch_if_terminal(&seed.dispatch_id, now.unix_timestamp())?
        {
            for row in state.touched_state.values() {
                if let Err(error) = state.config.projection.upsert_account_state(row).await {
                    warn!(account_id = %row.account_id, error = %error, "live account-state projection failed; reconcile will heal");
                }
            }
        }
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
    let Some(account) = account else {
        terminalize(state, target, "not_armed", now)?;
        return Ok(PassControl::Continue);
    };
    if !account.is_armed() {
        terminalize(state, target, "not_armed", now)?;
        return Ok(PassControl::Continue);
    }
    if let Some(reason) = state.closures.reason(&target.account_id) {
        info!(account_id = %target.account_id, reason, "live target remains pending while account admission is closed");
        return Ok(PassControl::StopSeed);
    }

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

    if target.state == "ambiguous" || target.state == "submitted" {
        let outcome = recover_target(state, target, &venue, now).await?;
        let transition = outcome_transition(&outcome);
        persist_outcome(state, target, &outcome, now)?;
        if matches!(outcome, LiveOrderOutcome::Matched { .. })
            && let Some(prepared) = recovered_prepared(state, target)?
        {
            project_fill(state, seed, target, signal, &prepared.ladder, now).await;
        }
        capture_account_state(state, account, &venue, false, now).await;
        return Ok(if transition.freeze {
            PassControl::FreezePass
        } else {
            PassControl::Continue
        });
    }

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
            if matches!(outcome, LiveOrderOutcome::Matched { .. }) {
                project_fill(state, seed, target, signal, &plan, now).await;
            }
            capture_account_state_row(state, account, &account_state, now);
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

async fn project_fill(
    state: &FanoutState,
    seed: &DispatchSeedRow,
    target: &DispatchTargetRow,
    signal: &LeaderSignal,
    plan: &pe_venue_polymarket::LadderPlan,
    now: OffsetDateTime,
) {
    let Some(fill_price) = plan.vwap() else {
        warn!(dispatch_id = %seed.dispatch_id, account_id = %target.account_id, "matched live order has no valid VWAP projection");
        return;
    };
    let row = LiveFillRow {
        account_id: target.account_id.clone(),
        idempotency_key: format!("{}:{}", seed.dispatch_id, target.account_id),
        leader_wallet: signal.leader.to_string(),
        source_trade_id: Some(seed.source_trade_id.clone()),
        market_id: signal.market_id.to_string(),
        outcome_id: i64::from(signal.outcome_id.0),
        side: "buy".to_owned(),
        contracts: plan.shares.to_decimal(),
        fill_price: fill_price.0,
        entry_unix: Some(now.unix_timestamp()),
        // Stable across projection retries; the account/idempotency natural key remains the
        // authoritative uniqueness boundary.
        event_seq: seed.created_at_unix,
    };
    if let Err(error) = state.config.projection.upsert_fills(&[row]).await {
        warn!(dispatch_id = %seed.dispatch_id, account_id = %target.account_id, error = %error, "live fill projection failed; reconcile will heal");
    }
}

fn capture_account_state_row(
    state: &mut FanoutState,
    account: &AccountContext,
    account_state: &pe_execution_core::LiveVenueAccountState,
    now: OffsetDateTime,
) {
    state.touched_state.insert(
        account.account_id.as_str().to_owned(),
        LiveAccountStateRow {
            account_id: account.account_id.as_str().to_owned(),
            free_collateral: account_state.reconciled_free_collateral.to_decimal(),
            reserved: Decimal::ZERO,
            unredeemed_value: Decimal::ZERO,
            last_reconciled_at: now
                .format(&time::format_description::well_known::Rfc3339)
                .ok(),
            admission_closed_reason: state
                .closures
                .reason(account.account_id.as_str())
                .map(str::to_owned),
        },
    );
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

struct RecoveredPrepared {
    identity: LiveOrderIdentity,
    order_hash: String,
    ladder: pe_venue_polymarket::LadderPlan,
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
                let used_asks = prepared
                    .ladder
                    .used_asks
                    .iter()
                    .map(|ask| pe_venue_polymarket::AskLevel {
                        price: ask.price,
                        shares: ask.shares,
                    })
                    .collect();
                Some(RecoveredPrepared {
                    identity: prepared.identity.clone(),
                    order_hash: prepared.prepared.order_hash.clone(),
                    ladder: pe_venue_polymarket::LadderPlan {
                        used_asks,
                        best_ask: prepared.ladder.best_ask,
                        limit_price: prepared.ladder.limit_price,
                        shares: prepared.ladder.shares,
                        estimated_ladder_spend: prepared.ladder.estimated_ladder_spend,
                        worst_case_debit: prepared.ladder.worst_case_debit,
                    },
                })
            }
            _ => None,
        });
    Ok(prepared)
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
                },
                LiveOrderOutcome::Matched {
                    order_hash: prepared.order_hash.clone(),
                    venue_order_id,
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

async fn fetch_account_settings(
    state: &FanoutState,
    account_id: &str,
) -> Result<AccountSettingsRow, &'static str> {
    let token = auth_token(
        &state.config.supabase_anon_key,
        &state.config.supabase_secret_key,
    );
    let url = format!(
        "{}/rest/v1/accounts?select=account_id,live_sizing_mode,live_sizing_dollar_usd,live_sizing_contracts&account_id=eq.{account_id}",
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
    let url = format!(
        "{}/rest/v1/account_events?select=account_id,event_kind,created_at&event_kind=in.(promotion_reviewed,promotion_review_revoked)",
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

async fn drive_modes(state: &mut FanoutState, _now: OffsetDateTime) {
    let snapshot = state.config.live_accounts.snapshot();
    if snapshot.accounts.is_empty() {
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
        let (credential_outcome, probe) = mode_probe(state, account).await;
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

async fn mode_probe(state: &FanoutState, account: &AccountContext) -> (CheckOutcome, StaticProbe) {
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
    let standard = venue.read_balance_and_allowance(false).await;
    let neg_risk = venue.read_balance_and_allowance(true).await;
    let (standard, neg_risk) = match (standard, neg_risk) {
        (Ok(standard), Ok(neg_risk)) => (standard, neg_risk),
        _ => return (CheckOutcome::Pass, unavailable),
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
    #[serde(default)]
    neg_risk: Option<bool>,
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
            Err(_) => continue,
        };
        let positions = match fetch_redeemable_positions(state, &venue.deposit_wallet()).await {
            Ok(positions) => positions,
            Err(reason) => {
                state.closures.redemption.insert(
                    account.account_id.as_str().to_owned(),
                    format!("redemption inventory unavailable: {reason}"),
                );
                upsert_redemption_state(state, account, Decimal::ZERO, now).await;
                continue;
            }
        };
        if positions.is_empty() {
            state
                .closures
                .redemption
                .remove(account.account_id.as_str());
            upsert_redemption_state(state, account, Decimal::ZERO, now).await;
            continue;
        }
        let unredeemed = positions
            .iter()
            .map(|position| position.size.max(Decimal::ZERO))
            .sum::<Decimal>();
        let closure_reason = 'redemption: {
            let Some(position) = positions.first() else {
                break 'redemption "redeemable inventory could not be selected".to_owned();
            };
            let Some(custody) = custody_kind(account.custody_wallet_kind.as_deref()) else {
                break 'redemption "redemption custody kind is missing or unknown".to_owned();
            };
            if custody != CustodyKind::DepositWallet {
                break 'redemption format!(
                    "redemption submission unsupported for {custody:?} custody"
                );
            }
            let Some(api_key) = credentials.relayer_api_key.as_ref() else {
                break 'redemption "redemption Relayer API key is unavailable".to_owned();
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
                    break 'redemption format!("redemption adapter unavailable: {error}");
                }
            };
            let custody_wallet = account
                .custody_wallet_address
                .clone()
                .unwrap_or_else(|| venue.deposit_wallet());
            if !custody_wallet.eq_ignore_ascii_case(&venue.deposit_wallet()) {
                break 'redemption "redemption custody wallet does not match decrypted credentials"
                    .to_owned();
            }
            let call = match build_redemption_call(
                PolymarketConditionId(position.condition_id.clone()),
                position.neg_risk.unwrap_or(false),
            ) {
                Ok(call) => call,
                Err(error) => {
                    break 'redemption format!("redemption call construction failed: {error}");
                }
            };
            let nonce = match adapter.fetch_nonce(&owner_signer, custody).await {
                Ok(nonce) => nonce,
                Err(error) => {
                    break 'redemption format!("redemption nonce unavailable: {error}");
                }
            };
            let deadline_unix =
                match u64::try_from(now.unix_timestamp())
                    .ok()
                    .and_then(|timestamp| {
                        timestamp.checked_add(DEPOSIT_WALLET_REDEMPTION_DEADLINE_SECS)
                    }) {
                    Some(deadline) => deadline,
                    None => break 'redemption "redemption deadline overflow".to_owned(),
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
                    break 'redemption format!("redemption signing failed: {error}");
                }
            };
            let request_hash =
                match adapter.submission_body_hash(&signed_request, now.unix_timestamp()) {
                    Ok(hash) => hash,
                    Err(error) => {
                        break 'redemption format!("redemption request validation failed: {error}");
                    }
                };
            let attempt = RedemptionAttempt {
                identity: RedemptionAttemptIdentity {
                    account_id: account.account_id.clone(),
                    condition_id: call.condition_id.clone(),
                    adapter: call.to.clone(),
                    custody_wallet,
                },
                state: RedemptionAttemptState::default(),
            };
            let redeemable = CollateralAmount::from_decimal_exact(
                position
                    .size
                    .max(Decimal::ZERO)
                    .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero),
            )
            .unwrap_or(CollateralAmount::ZERO);
            match run_redemption_pass(
                &adapter,
                &adapter,
                state.config.journal.as_ref(),
                RedemptionPassInput {
                    attempt,
                    now,
                    resolved_winner_redeemable: redeemable,
                    signed_request: Some(&signed_request),
                    request_hash: Some(&request_hash),
                    retry_not_before_on_failure: now
                        + time::Duration::seconds(REDEMPTION_RETRY_SECS),
                },
            )
            .await
            {
                Ok(result) => {
                    let posture = redemption_posture(&result.attempt.state);
                    if posture.surface_prominently {
                        "redemption unresolved after repeated attempts".to_owned()
                    } else if posture.closes_new_buy_admission {
                        "redemption pending".to_owned()
                    } else {
                        "redeemable inventory awaiting balance reconciliation".to_owned()
                    }
                }
                Err(error) => format!("redemption driver failed: {error}"),
            }
        };
        state
            .closures
            .redemption
            .insert(account.account_id.as_str().to_owned(), closure_reason);
        upsert_redemption_state(state, account, unredeemed, now).await;
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
    let url = format!(
        "{}/positions?user={wallet}&redeemable=true&sizeThreshold=0&limit=500&offset=0",
        state.config.data_base_url.trim_end_matches('/')
    );
    let response = state
        .config
        .http
        .get(url)
        .send()
        .await
        .map_err(|_| "transport")?;
    if !response.status().is_success() {
        return Err("status");
    }
    response.json().await.map_err(|_| "decode")
}

async fn upsert_redemption_state(
    state: &FanoutState,
    account: &AccountContext,
    unredeemed: Decimal,
    now: OffsetDateTime,
) {
    let row = LiveAccountStateRow {
        account_id: account.account_id.as_str().to_owned(),
        free_collateral: Decimal::ZERO,
        reserved: Decimal::ZERO,
        unredeemed_value: unredeemed,
        last_reconciled_at: now
            .format(&time::format_description::well_known::Rfc3339)
            .ok(),
        admission_closed_reason: state
            .closures
            .reason(account.account_id.as_str())
            .map(str::to_owned),
    };
    if let Err(error) = state.config.projection.upsert_account_state(&row).await {
        warn!(account_id = %account.account_id, error = %error, "redemption posture projection failed");
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;

    use pe_execution_core::{
        LiveAccountReadFailure, LiveOrderRejectKind, LivePostClassification, LivePostParseError,
        LiveVenueAccountReadError, LiveVenueAccountState, LiveVenuePreparationError,
        LiveVenuePrepareRequest, LiveVenuePrepared, LiveVenueReconciliation,
        LiveVenueReconciliationError,
    };
    use pe_paper_state::{DispatchSeedRecord, DispatchTargetSeed};
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn outcome_to_target_state_mapping_and_freeze_rule() {
        let matched = LiveOrderOutcome::Matched {
            order_hash: "hash".to_owned(),
            venue_order_id: "id".to_owned(),
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
