//! Event dispatch loop: routes decoded trade events to the copy-signal-engine,
//! then gates signals through strategy evaluation and execution dispatch.
//!
//! The orchestrator is the sole paper financial serializer. Its I/O is the paper-log writer,
//! admission/current-price clients, and the one CLOB `/book` fetch used to build exact economics.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use pe_copy_signal_engine::{
    IncomingTrade, LeaderSignal, SignalConfig, TradeProvenance, classify_trade,
};
use pe_core_types::{
    CollateralAmount, EventSeq, MarketId, MarketOutcomeId, OutcomeId, Price, Probability,
    ReceivedAt, ReconstructionQuality, ShareAmount, Side, SourceId, SourceTimestamp, SourceTradeId,
    TraderId, VenueId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Scanner, Writer};
use pe_kelly_sizer::{KellyInput, size_contracts};
use pe_paper_state::{FillRecord, LeaderPositionRow, PaperStateDb, PendingTerminalEvidence};
use pe_position_ledger::PositionLedger;
use pe_risk_engine::{EquityInputs, RiskSnapshot, TradingMode, current_equity};
use pe_source_polymarket_public::PageFetcher;
use pe_strategy_winner_follow::{ExecutionMode, SizingMode, WinnerFollowStrategy};
use pe_trader_index::Watchlist;
use pe_venue_polymarket::{BuySizing, LadderError, LadderPlan, ladder_is_stale, plan_sized_buy};
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info, warn};

use crate::bucket_commit::{BucketCommitEngine, DecisionContinuationV3};
use crate::clob_book::ClobBookFetcher;
use crate::decision_replay::{
    AuthorityEvidence, BookEvidence, DecisionEvidenceAccumulator, MarketEndEvidence,
    MarketPriceEvidence, TerminalDispositionEvidence, WinnerFollowDecisionInputs,
    WinnerFollowRiskInputEvidence, ladder_plan_blake3, replay_decision_pending,
};
use crate::entry_gate::CopyEntryGateConfig;
use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
use crate::mark_prices::HistoricalMarkAdapter;
use crate::mid_price_cache::MidPriceCache;
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{
    PAPER_LOG_SCHEMA_VERSION, PaperLogFrame, PaperLogRecord, PaperMarkPrice, PortfolioMark,
    QualificationSealed, SealReason, TailBinding,
};
use crate::risk_inputs::{
    BoundaryMarkError, RiskInputsUnavailable, SourceReceiptIndex, apply_global_risk_halts,
};
use crate::runtime_config::{self, LiveRuntimeConfig};
use crate::snapshot_worker::{SnapshotHandle, enqueue_if_buy};
use crate::supabase_sink::SinkHandle;
use crate::supabase_state::SupabaseStateClient;
use crate::supabase_state::{
    PreparedFillRequest, PreparedResolutionRequest, SupabaseStateTrait, apply_financial_result,
    reconcile_active_financial_frames, resolution_source_received_at,
    terminalize_final_fill_decision,
};

/// Hot-path `/book` fetch timeout for the mandatory price-impact gate (#398 WS2). A timeout makes
/// the book unusable and closes copy admission without stalling the trade.
/// Canonical default in `docs/_GLOSSARY.md`: `clob_book_hot_path_timeout_secs`.
const CLOB_BOOK_HOT_PATH_TIMEOUT_SECS: u64 = 2;

/// Bounded in-process retry for a failed local paper-outcome commit (#508 round-4: every
/// failed local finalization surfaces and enters an in-process reconcile pass — never a
/// silent log-and-continue). Restart recovery remains the durable backstop.
const LOCAL_COMMIT_RETRIES: u32 = 3;
const LOCAL_COMMIT_RETRY_DELAY_MS: u64 = 100;

/// Outcome of the enabled price-impact gate for one admitted signal (#508).
enum GatePlan {
    /// The budget planner produced a within-band plan (quantity/VWAP/limit).
    Planned(LadderPlan),
    /// The book read SUCCEEDED but the in-band ladder affords no atomic share for the paper
    /// budget — a paper-only skip that must never suppress live targets (Decision 10). The
    /// best ask anchors the shared band gate.
    NothingAffordable { best_ask: Price },
}

struct GatePlanEvidence {
    gate: GatePlan,
    book: BookEvidence,
    book_receipt: Option<pe_event_log::AppendReceipt>,
    budget: CollateralAmount,
    worst_case_all_in_debit: CollateralAmount,
    checked_at_unix_ms: u64,
}

struct GatePlanFailure {
    reason: &'static str,
    book: Box<BookEvidence>,
    checked_at_unix_ms: Option<u64>,
}

struct ActivePaperRiskFailure {
    cause: RiskInputsUnavailable,
    evidence: WinnerFollowRiskInputEvidence,
}

impl ActivePaperRiskFailure {
    fn new(cause: RiskInputsUnavailable, evidence: &WinnerFollowRiskInputEvidence) -> Self {
        Self {
            cause,
            evidence: evidence.clone(),
        }
    }
}

fn book_failure(
    token_id: Option<&str>,
    outcome: &str,
    response_blake3: Option<&str>,
    fetched_at_unix_ms: Option<u64>,
    reason: &str,
) -> BookEvidence {
    BookEvidence {
        request_token_id: token_id.map(str::to_owned),
        outcome: outcome.to_owned(),
        response_blake3: response_blake3.map(str::to_owned),
        fetched_at_unix_ms,
        best_ask: None,
        vwap_basis: None,
        ladder_plan_blake3: None,
        reason: Some(reason.to_owned()),
    }
}

fn unix_millis(instant: OffsetDateTime) -> i64 {
    i64::try_from(instant.unix_timestamp_nanos() / 1_000_000).unwrap_or(i64::MAX)
}

fn record_clock(
    evidence: &mut Option<DecisionEvidenceAccumulator>,
    purpose: &str,
    instant: OffsetDateTime,
) {
    if let Some(evidence) = evidence.as_mut() {
        evidence.record_clock(purpose, unix_millis(instant));
    }
}

pub(crate) fn render_pending_evidence(
    evidence: Option<&DecisionEvidenceAccumulator>,
    authority: AuthorityEvidence,
    terminal: TerminalDispositionEvidence,
) -> Result<Option<(String, i64)>, serde_json::Error> {
    let Some(evidence) = evidence else {
        return Ok(None);
    };
    let terminal_at = OffsetDateTime::now_utc();
    let mut complete = evidence.clone();
    complete.record_clock("terminal_transition", unix_millis(terminal_at));
    let json = complete.render(authority, terminal)?;
    Ok(Some((json, terminal_at.unix_timestamp())))
}

pub(crate) fn pending_terminal(value: &(String, i64)) -> PendingTerminalEvidence<'_> {
    PendingTerminalEvidence {
        post_commit_inputs_json: &value.0,
        updated_at_unix: value.1,
    }
}

pub(crate) fn recorded_fill_terminal(
    record: &FillRecord,
    seq: EventSeq,
) -> TerminalDispositionEvidence {
    let side = match record.side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    };
    TerminalDispositionEvidence::fill(
        record.idempotency_key.clone(),
        record.market_id.to_string(),
        record.outcome_id.0,
        side,
        record.quantity.atomic() / 1_000_000,
        record.fill_price,
        seq,
    )
}

/// Captured pre-admission state for exact rollback (#511): the leader ledger's
/// pre-trade `(long, short)` for the touched key, or `None` when the trade created it.
struct RollbackCtx {
    wallet: WalletAddress,
    key: MarketOutcomeId,
    prev: Option<(ShareAmount, ShareAmount)>,
    enabled: bool,
}

/// Orchestrator configuration.
pub struct OrchestratorConfig {
    pub bankroll: Decimal,
    pub mode: ExecutionMode,
    pub signal_config: SignalConfig,
    /// Drop signals whose market `endDate` is further than this many seconds into
    /// the future. 0 disables the upper bound. Default: 48 h (172_800 s, run28 cutover).
    pub max_resolution_horizon_secs: u64,
    /// Drop signals whose market resolves sooner than this many seconds from now.
    /// 0 disables the lower bound. Default: 60 s (docs/29 copy floor).
    pub min_resolution_horizon_secs: u64,
    /// Maximum current price at which a BUY copy will fill (issue #142 parity).
    /// `Decimal::ZERO` disables the cap.
    pub max_fill_price: Decimal,
    /// Minimum current price at which a BUY copy will fill — the run28 entry-band
    /// lower bound (#468 parity with the backtest `min_signal_price` floor).
    /// `Decimal::ZERO` disables the floor.
    pub min_fill_price: Decimal,
    /// Mandatory price-impact ceiling in basis points (#544). Valid values are `1..=10_000`;
    /// unusable book evidence fails closed before paper fill resolution.
    pub price_impact_cap_bps: i32,
    /// Copy-entry gate config (first-entry/fail-closed posture). The per-wallet
    /// market history is supplied separately to [`Orchestrator::new`].
    pub entry_gate_config: CopyEntryGateConfig,
    /// Supabase-authoritative runtime config (#398 WS1). `Some` in production: `handle_trade`
    /// rebuilds the strategy/mode/gate knobs from its snapshot per event. `None` (tests) keeps the
    /// boot config — the per-event rebuild is skipped.
    pub runtime_config: Option<LiveRuntimeConfig>,
    /// Live account contexts (#508): `Some` in production so admitted signals with armed
    /// live targets stage a dispatch aggregate. `None` (tests / Supabase off) = zero
    /// targets = the Phase-A baseline path (no seeds).
    pub live_accounts: Option<crate::live_accounts::LiveAccounts>,
    /// #530: websocket-primary mode. Gates the stale-fallback no-copy rule and the
    /// dual-unhealthy admission block; `false` keeps poll-only behavior byte-identical.
    pub activity_ws_enabled: bool,
    /// #530/#546: calibrated copy budget (seconds). In websocket-primary mode an observation
    /// from either source older than this is admitted with a typed no-copy disposition — at
    /// the early gate and again immediately before dispatch staging.
    pub copy_latency_budget_secs: u64,
    /// Shared structural membership-writer lock. Production supplies the same
    /// lock used by refresh/maintenance so a newly durable fence is removed
    /// from the published generation before the bucket acknowledgement (#544).
    pub watchlist_writer_lock: Option<Arc<Mutex<()>>>,
}

/// Scenario-only deterministic seams (#546): fixed admission-clock instants and historical mark
/// results consumed in order, plus one-shot faults immediately before the two durable writes whose
/// rollback the fan-in acceptance suite must prove. Compiled only with the `scenario` feature;
/// production has no clock/result injection and no fault path.
#[cfg(feature = "scenario")]
#[derive(Debug, Default)]
pub struct ScenarioHooks {
    pub age_clock: std::sync::Mutex<std::collections::VecDeque<OffsetDateTime>>,
    /// Deterministically advance the next queued age sample after one successful observation
    /// resolution. This models receipt I/O latency without sleeping in scenario tests.
    pub observation_resolution_advance_millis: std::sync::atomic::AtomicI64,
    pub admission_artifacts:
        std::sync::Mutex<std::collections::VecDeque<pe_execution_core::LiveAdmissionArtifact>>,
    pub boundary_mark_prices:
        std::sync::Mutex<std::collections::VecDeque<crate::risk_inputs::HistoricalMarkPrice>>,
    pub financial_clock_unix: std::sync::atomic::AtomicI64,
    pub fail_next_stage_seed: std::sync::atomic::AtomicBool,
    pub fail_next_no_copy_commit: std::sync::atomic::AtomicBool,
}

pub struct Orchestrator<
    F: PageFetcher + Send + Sync,
    B: ClobBookFetcher,
    S: SupabaseStateTrait + Clone = SupabaseStateClient,
> {
    /// Deterministic bucket state shared by acknowledged production commits and scenario checks.
    bucket_engine: BucketCommitEngine,
    live_watchlist: LiveWatchlist,
    signal_config: SignalConfig,
    strategy: WinnerFollowStrategy,
    paper_writer: Writer,
    mode: ExecutionMode,
    bankroll: Decimal,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,
    // Live current-price source (Gamma mids) for the post-latency sizing basis (#339).
    mid_price_cache: MidPriceCache<F>,
    max_resolution_horizon_secs: u64,
    min_resolution_horizon_secs: u64,
    activity_ws_enabled: bool,
    copy_latency_budget_secs: u64,
    #[cfg(feature = "scenario")]
    scenario_hooks: Option<Arc<ScenarioHooks>>,
    // Skip BUYs whose FILL price is >= this (issue #142 parity). ZERO disables.
    max_fill_price: Decimal,
    // Skip BUYs whose FILL price is < this (run28 band lower, #468 parity). ZERO disables.
    min_fill_price: Decimal,
    // Tracks (market, outcome) pairs we already hold a paper position in.
    // Prevents multiple leaders entering the same contract from stacking fills.
    filled_positions: HashSet<MarketOutcomeId>,
    // Copy-entry gate: admits only a leader's first-ever BUY entry into a market
    // (#290; price band removed in #339).
    // Sentinel quality (0) returned for any wallet not found in the watchlist.
    // Zero quality → LeaderAction::Unknown → classify_trade returns None, so no signal.
    min_quality: ReconstructionQuality,
    control_rx: mpsc::Receiver<OrchestratorControl>,
    // Liquidity-at-fill snapshot enqueue handle (issue #350 WS2 PR-H). `None` when capture is
    // disabled. Buy-only; enqueue is non-blocking (drop-on-full), off the trade hot path.
    snapshot_sink: Option<SnapshotHandle>,
    // Authoritative Supabase paper-state client (issue #397). In the financial era, `Some` owns
    // the Prepared-sequenced authority mutation before the local projection and Final append.
    // `None` is the legacy SQLite-authoritative path.
    supabase_state: Option<S>,
    // Supabase-authoritative runtime config (#398 WS1). `Some` in production; the per-event
    // rebuild at the top of `handle_trade` reads one snapshot. `None` in tests (boot config).
    runtime_config: Option<LiveRuntimeConfig>,
    // Live CLOB /book fetcher for the price-impact gate (#398 WS2). Shared `Arc` with the snapshot
    // worker so the 5 rps rate gate is global. Consulted for every admitted signal.
    book_fetcher: Arc<B>,
    // Mandatory price-impact gate cap in bps, rebuilt per event from the runtime-config snapshot.
    price_impact_cap_bps: i32,
    // Live account contexts (#508): armed targets stage dispatch aggregates. `None` in
    // tests / when Supabase is off — zero targets, Phase-A baseline behavior.
    live_accounts: Option<crate::live_accounts::LiveAccounts>,
    /// Set at the first uncertain paper sync boundary. Dropping the receiver
    /// then backpressures/stops every producer; supervisor owns process exit.
    intake_stopped: bool,
    watchlist_writer_lock: Option<Arc<Mutex<()>>>,
    /// Open production continuations loaded before producers. The durable row
    /// remains the owner; this queue only preserves its causal boot order.
    pending_boot: VecDeque<IncomingTrade>,
    pending_continuations: HashMap<SourceTradeId, DecisionContinuationV3>,
    /// True only while replaying continuations that were open at process boot. Newly committed
    /// buckets still pass the current source-health gate before their financial disposition.
    resuming_boot: bool,
    financial_log_paths: Option<(std::path::PathBuf, std::path::PathBuf)>,
    source_receipts: Option<SourceReceiptIndex>,
    qualification_start: Option<pe_event_log::AppendReceipt>,
    admission_builder: Option<crate::live_venue_adapter::LiveAdmissionBuilder>,
    boundary_mark_fetcher: Option<Arc<HistoricalMarkAdapter>>,
    active_risk_halts: HashSet<(
        crate::paper_recovery::RiskHaltOwner,
        pe_risk_engine::RiskHaltCause,
    )>,
}

#[derive(Debug, thiserror::Error)]
pub enum OrchestratorRunError {
    #[error("decision-pending recovery failed: {0}")]
    PendingRecovery(String),
    #[error("{channel} input channel closed before coordinated shutdown")]
    PrematureInputClosure { channel: &'static str },
    #[error("paper durability became uncertain")]
    PaperDurabilityUncertain,
}

impl<F: PageFetcher + Send + Sync, B: ClobBookFetcher, S: SupabaseStateTrait + Clone>
    Orchestrator<F, B, S>
{
    fn financial_now(&self) -> OffsetDateTime {
        #[cfg(feature = "scenario")]
        if let Some(unix) = self
            .scenario_hooks
            .as_ref()
            .map(|hooks| {
                hooks
                    .financial_clock_unix
                    .load(std::sync::atomic::Ordering::SeqCst)
            })
            .filter(|unix| *unix != 0)
            && let Ok(now) = OffsetDateTime::from_unix_timestamp(unix)
        {
            return now;
        }
        OffsetDateTime::now_utc()
    }

    fn append_paper_record(
        &mut self,
        record: &PaperLogRecord,
    ) -> Result<pe_event_log::AppendReceipt, String> {
        let payload = serde_json::to_vec(record).map_err(|error| error.to_string())?;
        let now = self.financial_now();
        self.paper_writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                content_type: ContentType::Json,
                payload,
            })
            .map_err(|error| error.to_string())
    }

    fn apply_risk_halt_transition(
        &mut self,
        owner: crate::paper_recovery::RiskHaltOwner,
        cause: pe_risk_engine::RiskHaltCause,
        state: crate::paper_recovery::HaltState,
        evidence: serde_json::Value,
    ) -> Result<pe_event_log::AppendReceipt, String> {
        let key = (owner.clone(), cause);
        let receipt = self.append_paper_record(&PaperLogRecord::RiskHaltChanged {
            owner,
            cause,
            state,
            evidence,
        })?;
        match state {
            crate::paper_recovery::HaltState::Engaged => {
                self.active_risk_halts.insert(key);
            }
            crate::paper_recovery::HaltState::Released => {
                self.active_risk_halts.remove(&key);
            }
        }
        Ok(receipt)
    }

    fn synchronize_paper_risk_halts(
        &mut self,
        snapshot: &RiskSnapshot,
        evaluated_at_unix_ms: i64,
    ) -> Result<(), String> {
        use crate::paper_recovery::{HaltState, RiskHaltOwner};
        use pe_risk_engine::{
            INTRADAY_STOP_BPS, KILL_SWITCH_DRAWDOWN_BPS, ROLLING_7D_STOP_BPS, RiskHaltCause,
        };

        let owner = RiskHaltOwner::Paper;
        for (cause, desired, manual_release_only) in [
            (
                RiskHaltCause::AbsoluteLoss,
                snapshot.absolute_pnl_bps.0 <= KILL_SWITCH_DRAWDOWN_BPS,
                true,
            ),
            (
                RiskHaltCause::IntradayDrawdown,
                snapshot.intraday_pnl_bps.0 <= INTRADAY_STOP_BPS,
                false,
            ),
            (
                RiskHaltCause::Rolling7dDrawdown,
                snapshot.rolling_7d_pnl_bps.0 <= ROLLING_7D_STOP_BPS,
                false,
            ),
            (
                RiskHaltCause::CopyLatency,
                snapshot.copy_latency_kill_switch_active,
                false,
            ),
        ] {
            let active = self.active_risk_halts.contains(&(owner.clone(), cause));
            let state = match (active, desired, manual_release_only) {
                (false, true, _) => Some(HaltState::Engaged),
                (true, false, false) => Some(HaltState::Released),
                _ => None,
            };
            if let Some(state) = state {
                self.apply_risk_halt_transition(
                    owner.clone(),
                    cause,
                    state,
                    serde_json::json!({
                        "evaluated_at_unix_ms": evaluated_at_unix_ms,
                        "snapshot": snapshot,
                    }),
                )?;
            }
        }
        Ok(())
    }

    fn apply_global_halts_to_snapshot(&self, snapshot: &mut RiskSnapshot) {
        apply_global_risk_halts(&self.active_risk_halts, snapshot);
    }

    fn qualification_seal_reason(
        started: &crate::paper_recovery::QualificationStarted,
        proposed_economic_hash: &str,
        proposed_financial_semantic_version: u32,
    ) -> Option<SealReason> {
        crate::qualification::seal_if_semantic_drift(started, proposed_economic_hash).or_else(
            || {
                (started.financial_semantic_version != proposed_financial_semantic_version).then(
                    || {
                        SealReason::InsufficientEvidence(format!(
                            "financial semantic version changed from {} to {proposed_financial_semantic_version}",
                            started.financial_semantic_version
                        ))
                    },
                )
            },
        )
    }

    fn seal_qualification(
        &mut self,
        reason: SealReason,
        sealed_cutoff_unix: i64,
    ) -> Result<(), String> {
        let (paper_log_path, source_log_path) = self
            .financial_log_paths
            .as_ref()
            .ok_or_else(|| "qualification seal is unavailable before Start".to_owned())?;
        let era = crate::paper_recovery::paper_era(
            crate::paper_recovery::scan_paper_log(paper_log_path)
                .map_err(|error| error.to_string())?,
        );
        let (start_receipt, started) = era
            .start
            .as_ref()
            .ok_or_else(|| "qualification seal has no verified Start".to_owned())?;
        if era.frames.iter().any(|frame| {
            matches!(
                &frame.frame,
                PaperLogFrame::Record(PaperLogRecord::QualificationSealed(_))
            )
        }) {
            return Ok(());
        }
        if crate::paper_recovery::oldest_unmatched_prepared(&era).is_some() {
            return Err("qualification seal waits for the oldest unmatched Prepared".to_owned());
        }
        let source_prefix = Scanner::verify(source_log_path).map_err(|error| error.to_string())?;
        let sealed_source_prefix = TailBinding::from(&source_prefix);
        let decisions = crate::qualification::decision_rows_for_source_prefix(
            &self.paper_state,
            source_log_path,
            &started.source_prefix,
            &sealed_source_prefix,
        )
        .map_err(|error| error.to_string())?;
        let decision_keys = decisions
            .into_iter()
            .map(|row| (row.source_trade_id, row.semantic_revision))
            .collect::<Vec<_>>();
        let decision_evidence = self
            .paper_state
            .seal_decision_evidence_for_source_prefix(
                &decision_keys,
                started.source_prefix.last_sequence,
                sealed_source_prefix.last_sequence,
            )
            .map_err(|error| error.to_string())?;
        let financial_prefix =
            Scanner::verify(paper_log_path).map_err(|error| error.to_string())?;
        let live_prefix = pe_execution_core::LiveJournal::verified_tail(
            paper_log_path.with_file_name("live_journal.log"),
        )
        .map_err(|error| error.to_string())?;
        self.append_paper_record(&PaperLogRecord::QualificationSealed(Box::new(
            QualificationSealed {
                start_receipt: *start_receipt,
                source_prefix: sealed_source_prefix,
                financial_prefix: TailBinding::from(&financial_prefix),
                live_prefix: TailBinding::from(&live_prefix),
                decision_evidence_digest: blake3::hash(&decision_evidence).to_hex().to_string(),
                sealed_cutoff_unix,
                reason,
            },
        )))?;
        Ok(())
    }

    fn apply_seal_check(
        &mut self,
        proposed_economic_hash: &str,
        proposed_financial_semantic_version: u32,
    ) -> Result<(), String> {
        let (paper_log_path, _) = self
            .financial_log_paths
            .as_ref()
            .ok_or_else(|| "qualification seal check is unavailable before Start".to_owned())?;
        let era = crate::paper_recovery::paper_era(
            crate::paper_recovery::scan_paper_log(paper_log_path)
                .map_err(|error| error.to_string())?,
        );
        let (_, started) = era
            .start
            .as_ref()
            .ok_or_else(|| "qualification seal check has no verified Start".to_owned())?;
        let Some(reason) = Self::qualification_seal_reason(
            started,
            proposed_economic_hash,
            proposed_financial_semantic_version,
        ) else {
            return Ok(());
        };
        self.seal_qualification(reason, OffsetDateTime::now_utc().unix_timestamp())
    }

    /// Converge the verified oldest paper Prepared before any successor financial control.
    /// The shared recovery routine owns frozen-request reconstruction and appends at most the
    /// missing Final; no control-specific cursor or retry state is maintained here.
    async fn reconcile_oldest_financial_prepared(&mut self) -> Result<(), String> {
        let (paper_log_path, source_log_path) = self
            .financial_log_paths
            .clone()
            .ok_or_else(|| "active financial log paths are not configured".to_owned())?;
        let authority = self
            .supabase_state
            .clone()
            .ok_or_else(|| "active financial era has no authority client".to_owned())?;
        reconcile_active_financial_frames(
            &authority,
            &self.paper_state,
            &paper_log_path,
            &source_log_path,
            &mut self.paper_writer,
        )
        .await
        .map_err(|error| error.to_string())?;
        let era = crate::paper_recovery::paper_era(
            crate::paper_recovery::scan_paper_log(&paper_log_path)
                .map_err(|error| error.to_string())?,
        );
        if crate::paper_recovery::oldest_unmatched_prepared(&era).is_some() {
            return Err("oldest paper Prepared remains unmatched after redrive".to_owned());
        }
        Ok(())
    }

    async fn apply_resolution_candidate(
        &mut self,
        condition: pe_core_types::PolymarketConditionId,
        payout_by_outcome_index_json: String,
        source_receipt: pe_event_log::AppendReceipt,
    ) -> Result<(), String> {
        self.reconcile_oldest_financial_prepared().await?;
        let (paper_log_path, source_log_path) = self
            .financial_log_paths
            .clone()
            .ok_or_else(|| "active financial log paths are not configured".to_owned())?;
        let start = self
            .paper_state
            .financial_start()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "resolution candidate precedes QualificationStarted".to_owned())?;
        let era = crate::paper_recovery::paper_era(
            crate::paper_recovery::scan_paper_log(&paper_log_path)
                .map_err(|error| error.to_string())?,
        );
        let completed = era
            .frames
            .iter()
            .rev()
            .find_map(|frame| match &frame.frame {
                crate::paper_recovery::PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                    prepared_receipt,
                    ..
                }) => Some(prepared_receipt.sequence),
                _ => None,
            });
        let local_completed = self
            .paper_state
            .financial_last_prepared_seq()
            .map_err(|error| error.to_string())?;
        if local_completed != completed {
            return Err(
                "local financial snapshot sequence differs from the verified paper prefix"
                    .to_owned(),
            );
        }
        let expected = crate::paper_recovery::ExpectedAuthority {
            qualification_start_receipt: start,
            prior_completed_prepared_sequence: completed,
        };
        // Validate and freeze the source-derived settlement time before the irreversible
        // Prepared append. Recovery repeats this check as corruption detection.
        let settled_at_unix = resolution_source_received_at(
            &source_log_path,
            source_receipt,
            &condition,
            &payout_by_outcome_index_json,
        )
        .map_err(|error| error.to_string())?;
        let prepared_record = PaperLogRecord::FinancialPrepared {
            expected_authority: expected.clone(),
            payload: crate::paper_recovery::FinancialPayload::Resolution {
                condition_id: condition.clone(),
                payout_by_outcome_index_json: payout_by_outcome_index_json.clone(),
                resolution_source_receipt: source_receipt,
            },
        };
        let prepared_receipt = self.append_paper_record(&prepared_record)?;
        let authority = self
            .supabase_state
            .as_ref()
            .ok_or_else(|| "active financial era has no authority client".to_owned())?;
        let request = PreparedResolutionRequest {
            expected_authority: expected.clone(),
            prepared_receipt,
            condition,
            payout_by_outcome_index_json,
            settled_at_unix,
        };
        let canonical = authority
            .apply_prepared_resolution(&request)
            .await
            .map_err(|error| error.to_string())?;
        self.bankroll = canonical.bankroll;
        let result = crate::paper_recovery::FinancialResult::Resolution { canonical };
        let PaperLogRecord::FinancialPrepared { payload, .. } = &prepared_record else {
            return Err("internal financial Prepared kind mismatch".to_owned());
        };
        apply_financial_result(
            &self.paper_state,
            start,
            &expected,
            prepared_receipt,
            payload,
            &result,
            &source_log_path,
        )
        .map_err(|error| error.to_string())?;
        self.append_paper_record(&PaperLogRecord::FinancialFinal {
            prepared_receipt,
            result,
        })?;
        Ok(())
    }

    /// Serialize one active-era fill as Prepared → authority → local transaction → Final.
    /// Runtime redrives the verified oldest unmatched Prepared before admitting this successor.
    async fn apply_active_financial_fill(
        &mut self,
        trade: &IncomingTrade,
        economic: pe_execution_core::EconomicPrepared,
        dispatch_id: Option<&str>,
    ) -> Result<pe_event_log::AppendReceipt, String> {
        if economic.market.outcome_index > 1 {
            return Err(format!(
                "active paper fill outcome {} is not binary",
                economic.market.outcome_index
            ));
        }
        self.reconcile_oldest_financial_prepared().await?;

        let (paper_log_path, source_log_path) = self
            .financial_log_paths
            .clone()
            .ok_or_else(|| "active financial log paths are not configured".to_owned())?;
        let authority = self
            .supabase_state
            .clone()
            .ok_or_else(|| "active financial era has no authority client".to_owned())?;

        let era = crate::paper_recovery::paper_era(
            crate::paper_recovery::scan_paper_log(&paper_log_path)
                .map_err(|error| error.to_string())?,
        );
        if crate::paper_recovery::oldest_unmatched_prepared(&era).is_some() {
            return Err("oldest paper Prepared remains unmatched after redrive".to_owned());
        }
        let start = era
            .start
            .as_ref()
            .map(|(receipt, _)| *receipt)
            .ok_or_else(|| "active fill precedes QualificationStarted".to_owned())?;
        let prior = crate::risk_inputs::latest_completed_prepared(&era);
        if self
            .paper_state
            .financial_last_prepared_seq()
            .map_err(|error| error.to_string())?
            != prior
        {
            return Err(
                "local financial snapshot differs from the verified paper prefix".to_owned(),
            );
        }
        let expected = crate::paper_recovery::ExpectedAuthority {
            qualification_start_receipt: start,
            prior_completed_prepared_sequence: prior,
        };
        let payload = crate::paper_recovery::FinancialPayload::Fill {
            operation: crate::paper_recovery::PaperFillOperationIdentity {
                leader_wallet: trade.wallet,
                source_trade_id: trade.source_trade_id.clone(),
                observed_at_bucket: trade.observed_at.unix_timestamp(),
            },
            economic,
        };
        let prepared_receipt = self.append_paper_record(&PaperLogRecord::FinancialPrepared {
            expected_authority: expected.clone(),
            payload: payload.clone(),
        })?;
        let crate::paper_recovery::FinancialPayload::Fill {
            operation,
            economic,
        } = &payload
        else {
            return Err("internal fill Prepared kind mismatch".to_owned());
        };
        let request = PreparedFillRequest::from_prepared(
            expected.clone(),
            prepared_receipt,
            operation,
            economic,
        );
        let canonical = authority
            .commit_prepared_fill(&request)
            .await
            .map_err(|error| error.to_string())?;
        let result = crate::paper_recovery::FinancialResult::Fill { canonical };
        apply_financial_result(
            &self.paper_state,
            start,
            &expected,
            prepared_receipt,
            &payload,
            &result,
            &source_log_path,
        )
        .map_err(|error| error.to_string())?;
        let final_receipt = self.append_paper_record(&PaperLogRecord::FinancialFinal {
            prepared_receipt,
            result: result.clone(),
        })?;
        terminalize_final_fill_decision(&self.paper_state, &payload, &result, final_receipt)
            .map_err(|error| error.to_string())?;
        if let Some(dispatch_id) = dispatch_id {
            self.paper_state
                .flip_dispatch_ready(dispatch_id, "fill")
                .map_err(|error| error.to_string())?;
        }
        let crate::paper_recovery::FinancialResult::Fill { canonical } = result else {
            return Err("internal fill Final kind mismatch".to_owned());
        };
        self.bankroll = canonical.bankroll;
        Ok(final_receipt)
    }

    /// Build the paper owner's coherent risk snapshot from the active financial prefix.
    async fn active_paper_risk_snapshot(
        &mut self,
        signal: &LeaderSignal,
        proposed_debit: CollateralAmount,
        per_trade_cap_bps: i32,
    ) -> Result<(pe_execution_core::RiskAudit, pe_event_log::AppendReceipt), ActivePaperRiskFailure>
    {
        let mut attempt = WinnerFollowRiskInputEvidence {
            financial_prefix: None,
            price_receipts: Vec::new(),
            evaluated_at_unix_ms: i64::MAX,
            proposed_debit,
            per_trade_cap_bps,
        };
        let (paper_log_path, _) = self.financial_log_paths.as_ref().cloned().ok_or_else(|| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;
        let source_receipts = self.source_receipts.as_ref().ok_or_else(|| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;
        let era = crate::paper_recovery::paper_era(
            crate::paper_recovery::scan_paper_log(&paper_log_path).map_err(|_| {
                ActivePaperRiskFailure::new(
                    RiskInputsUnavailable::SnapshotSequenceMismatch,
                    &attempt,
                )
            })?,
        );
        let financial_prefix = era
            .frames
            .last()
            .map(|frame| frame.receipt)
            .ok_or_else(|| {
                ActivePaperRiskFailure::new(
                    RiskInputsUnavailable::SnapshotSequenceMismatch,
                    &attempt,
                )
            })?;
        attempt.financial_prefix = Some(financial_prefix);
        let positions = self.paper_state.open_positions().map_err(|_| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;
        let ids = crate::risk_inputs::open_positions(&positions)
            .map_err(|cause| ActivePaperRiskFailure::new(cause, &attempt))?
            .into_iter()
            .map(|(position, _)| {
                MarketOutcomeId::new(position.market_id.clone(), position.outcome_id)
            })
            .collect::<Vec<_>>();
        let mid_price_cache = self.mid_price_cache.clone();
        let price_attempt = mid_price_cache.fetch_mids_strict_attempt(&ids).await;
        attempt
            .price_receipts
            .clone_from(&price_attempt.price_receipts);
        let evaluated_at = price_attempt.evaluated_at;
        let evaluated_at_unix_ms = i64::try_from(
            evaluated_at.unix_timestamp_nanos().div_euclid(1_000_000),
        )
        .map_err(|_| ActivePaperRiskFailure::new(RiskInputsUnavailable::Overflow, &attempt))?;
        attempt.evaluated_at_unix_ms = evaluated_at_unix_ms;
        let observed = price_attempt
            .result
            .map_err(|cause| ActivePaperRiskFailure::new(cause, &attempt))?;
        let price_receipts = attempt.price_receipts.clone();
        let prices = observed
            .into_iter()
            .map(|((market, outcome), value)| {
                (
                    (
                        MarketId(pe_core_types::VenueMarketId(market)),
                        OutcomeId(outcome),
                    ),
                    value.price,
                )
            })
            .collect::<HashMap<_, _>>();
        let now = evaluated_at.unix_timestamp();
        let snapshot = self.paper_state.financial_snapshot(now).map_err(|_| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;

        let base = crate::risk_inputs::build_paper_risk_base(
            &era,
            signal.leader.0,
            &signal.market_id.to_string(),
            proposed_debit,
            per_trade_cap_bps,
        )
        .map_err(|cause| ActivePaperRiskFailure::new(cause, &attempt))?;
        let latency_was_active = self.active_risk_halts.contains(&(
            crate::paper_recovery::RiskHaltOwner::Paper,
            pe_risk_engine::RiskHaltCause::CopyLatency,
        ));
        let mut snapshot = crate::risk_inputs::build_paper_risk_snapshot_from_source_receipts(
            &base,
            &snapshot,
            &era,
            &prices,
            |receipt| source_receipts.received_millis(receipt),
            now,
            latency_was_active,
        )
        .map_err(|cause| ActivePaperRiskFailure::new(cause, &attempt))?;
        self.synchronize_paper_risk_halts(&snapshot, evaluated_at_unix_ms)
            .map_err(|_| {
                ActivePaperRiskFailure::new(
                    RiskInputsUnavailable::SnapshotSequenceMismatch,
                    &attempt,
                )
            })?;
        let financial_prefix = attempt.financial_prefix.ok_or_else(|| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;
        self.apply_global_halts_to_snapshot(&mut snapshot);
        let decision = match pe_risk_engine::evaluate_risk(&snapshot) {
            pe_risk_engine::RiskDecision::Approved => {
                pe_execution_core::RiskDecisionAudit::Approved
            }
            pe_risk_engine::RiskDecision::Blocked(reason) => {
                pe_execution_core::RiskDecisionAudit::Blocked { reason }
            }
        };
        Ok((
            pe_execution_core::RiskAudit {
                financial_prefix,
                snapshot,
                decision,
                price_receipts,
                evaluated_at_unix_ms,
            },
            financial_prefix,
        ))
    }

    /// Compose the one active-era economic record from the final sized plan. This is the only
    /// paper call to `EconomicPrepared::compose`; strategy evaluation consumes its all-in price.
    #[allow(clippy::too_many_arguments)]
    fn compose_active_paper_economic(
        &self,
        signal: &LeaderSignal,
        admission: &pe_execution_core::LiveAdmissionArtifact,
        plan: &LadderPlan,
        book_receipt: pe_event_log::AppendReceipt,
        observation: &pe_execution_core::ObservationEvidence,
        probability: Probability,
        budget: CollateralAmount,
        risk: pe_execution_core::RiskAudit,
        applied_configuration_hash: String,
    ) -> Result<pe_execution_core::EconomicPrepared, String> {
        let outcome_index = u8::try_from(signal.outcome_id.0)
            .ok()
            .filter(|value| *value <= 1)
            .ok_or_else(|| "active fill outcome is not binary".to_owned())?;
        let token_id = admission
            .market
            .ordered_outcome_token_ids
            .get(usize::from(outcome_index))
            .cloned()
            .ok_or_else(|| "active fill outcome has no admitted token".to_owned())?;
        let band_floor = Price::new(self.min_fill_price.max(Decimal::ZERO))
            .map_err(|error| error.to_string())?;
        let band_ceiling_exclusive = Price::new(if self.max_fill_price > Decimal::ZERO {
            self.max_fill_price
        } else {
            Decimal::ONE
        })
        .map_err(|error| error.to_string())?;
        let cash_before = CollateralAmount::from_decimal_exact(
            self.paper_state
                .financial_snapshot(self.financial_now().unix_timestamp())
                .map_err(|error| error.to_string())?
                .cash,
        )
        .map_err(|error| error.to_string())?;
        let sizing_mode = match self.strategy.config().sizing_mode {
            SizingMode::Dollar { usd } => pe_execution_core::SizingModeAudit::Dollar { usd },
            SizingMode::Contract { contracts } => {
                pe_execution_core::SizingModeAudit::Contract { contracts }
            }
            SizingMode::Kelly => pe_execution_core::SizingModeAudit::Kelly {
                fraction: self.strategy.effective_kelly_fraction(self.mode),
                probability,
            },
        };
        pe_execution_core::EconomicPrepared::compose(pe_execution_core::EconomicInputs {
            market: pe_execution_core::MarketSelection {
                condition_id: pe_core_types::PolymarketConditionId(signal.market_id.to_string()),
                outcome_index,
                token_id,
                side: signal.leader_side,
                market_id: signal.market_id.to_string(),
            },
            admission,
            plan,
            book_receipt,
            observation: Some(observation.clone()),
            sizing_mode,
            budget,
            slippage_rate: self.strategy.config().slippage_rate,
            risk,
            cash_before,
            price_impact_cap_bps: self.price_impact_cap_bps,
            chase_ceiling: signal.leader_price,
            band_floor,
            band_ceiling_exclusive,
            applied_configuration_hash,
        })
        .map_err(|error| error.to_string())
    }

    fn automatic_completion_seal_reason(
        mark_is_valid: bool,
        completion: Option<crate::qualification::QualificationCompletion>,
    ) -> Option<SealReason> {
        (mark_is_valid && completion.is_some_and(|value| value.is_complete()))
            .then_some(SealReason::Complete)
    }

    /// Build and synchronize the one paper mark for an acknowledged source boundary.
    async fn mark_at_boundary(
        &mut self,
        cutoff_unix: i64,
        boundary_receipt: pe_event_log::AppendReceipt,
    ) -> Result<(), String> {
        self.reconcile_oldest_financial_prepared().await?;
        let (paper_log_path, source_log_path) =
            self.financial_log_paths.as_ref().cloned().ok_or_else(|| {
                "daily boundary is unavailable before QualificationStarted".to_owned()
            })?;
        let mark_fetcher =
            Arc::clone(self.boundary_mark_fetcher.as_ref().ok_or_else(|| {
                "daily boundary historical-price reader is unavailable".to_owned()
            })?);
        let era = crate::paper_recovery::paper_era(
            crate::paper_recovery::scan_paper_log(&paper_log_path)
                .map_err(|error| error.to_string())?,
        );
        let completed = crate::risk_inputs::completed_prepared_before_boundary(
            &era,
            &source_log_path,
            cutoff_unix,
            boundary_receipt,
        )
        .map_err(|error| error.to_string())?;
        let mut completed_sequences = completed.iter().copied().collect::<Vec<_>>();
        completed_sequences.sort_by_key(|sequence| sequence.0);
        let snapshot = self
            .paper_state
            .financial_snapshot_before_source_bound(
                cutoff_unix,
                boundary_receipt.sequence,
                &completed_sequences,
            )
            .map_err(|error| error.to_string())?;
        let expected_last = completed.iter().copied().max_by_key(|sequence| sequence.0);
        if snapshot.last_prepared_seq != expected_last {
            return Err("daily boundary financial prefix differs from local projection".to_owned());
        }
        let token_by_position = era
            .frames
            .iter()
            .filter_map(|frame| match &frame.frame {
                crate::paper_recovery::PaperLogFrame::Record(
                    PaperLogRecord::FinancialPrepared {
                        payload: crate::paper_recovery::FinancialPayload::Fill { economic, .. },
                        ..
                    },
                ) if completed.contains(&frame.receipt.sequence) => Some((
                    (
                        economic.market.market_id.clone(),
                        u16::from(economic.market.outcome_index),
                    ),
                    economic.market.token_id.0.clone(),
                )),
                _ => None,
            })
            .collect::<HashMap<_, _>>();

        let mut prices = Vec::with_capacity(snapshot.positions.len());
        let mut valued_positions = Vec::with_capacity(snapshot.positions.len());
        let mut invalid = None;
        for position in &snapshot.positions {
            let key = (position.market_id.to_string(), position.outcome_id.0);
            let Some(token_id) = token_by_position.get(&key) else {
                let reason = "open position has no completed Prepared token mapping".to_owned();
                invalid.get_or_insert_with(|| reason.clone());
                prices.push(PaperMarkPrice {
                    market_id: key.0,
                    outcome_id: key.1,
                    price: None,
                    sample_unix: None,
                    receipt: None,
                    invalid: Some(reason),
                });
                continue;
            };
            #[cfg(feature = "scenario")]
            let scenario_mark = self
                .scenario_hooks
                .as_ref()
                .and_then(|hooks| hooks.boundary_mark_prices.lock().ok()?.pop_front());
            #[cfg(not(feature = "scenario"))]
            let scenario_mark = None;
            let fetched = match scenario_mark {
                Some(mark) => Ok(mark),
                None => mark_fetcher.fetch(token_id, cutoff_unix).await,
            };
            match fetched {
                Ok(mark) => {
                    let net = position.long.checked_sub(position.short).map_err(|_| {
                        "daily boundary encountered a net-short paper position".to_owned()
                    })?;
                    valued_positions.push((net, mark.price));
                    prices.push(PaperMarkPrice {
                        market_id: key.0,
                        outcome_id: key.1,
                        price: Some(mark.price),
                        sample_unix: Some(mark.sample_unix),
                        receipt: Some(mark.receipt),
                        invalid: None,
                    });
                }
                Err(BoundaryMarkError::Retryable(reason)) => return Err(reason),
                Err(error) => {
                    let reason = error.to_string();
                    invalid.get_or_insert_with(|| reason.clone());
                    prices.push(PaperMarkPrice {
                        market_id: key.0,
                        outcome_id: key.1,
                        price: None,
                        sample_unix: None,
                        receipt: None,
                        invalid: Some(reason),
                    });
                }
            }
        }
        prices.sort_by(|left, right| {
            (&left.market_id, left.outcome_id).cmp(&(&right.market_id, right.outcome_id))
        });
        let cash = snapshot.cash;
        let equity = if invalid.is_none() {
            current_equity(&EquityInputs {
                cash,
                positions: &valued_positions,
            })
            .map_err(|error| error.to_string())?
        } else {
            cash
        };
        let source_tail = Scanner::verify(&source_log_path).map_err(|error| error.to_string())?;
        let mark = PortfolioMark {
            boundary_receipt,
            cutoff_unix,
            source_tail: TailBinding::from(&source_tail),
            financial_prefix_seq: snapshot.last_prepared_seq,
            prices,
            cash,
            equity,
            invalid,
        };
        let mark_is_valid = mark.invalid.is_none();
        self.append_paper_record(&PaperLogRecord::PortfolioMark(Box::new(mark)))?;
        let completion = if mark_is_valid {
            let era = crate::paper_recovery::paper_era(
                crate::paper_recovery::scan_paper_log(&paper_log_path)
                    .map_err(|error| error.to_string())?,
            );
            crate::qualification::qualification_completion_for_causal_facts(&era, &completed)
        } else {
            None
        };
        if let Some(reason) = Self::automatic_completion_seal_reason(mark_is_valid, completion) {
            self.seal_qualification(reason, cutoff_unix)?;
        }
        Ok(())
    }

    async fn apply_control_message(&mut self, message: OrchestratorControl) {
        match message {
            OrchestratorControl::PrepareAdmissions {
                wallets,
                acknowledged,
            } => {
                if wallets
                    .iter()
                    .all(|wallet| !self.bucket_engine.is_fenced(wallet))
                {
                    let _ = acknowledged.send(());
                } else {
                    warn!(
                        wallets = wallets.len(),
                        "admission invalidated by durable wallet fence"
                    );
                }
            }
            OrchestratorControl::InstallAnchors {
                installs,
                acknowledged,
            } => {
                let result = self.bucket_engine.install_anchors(&installs);
                let _ = acknowledged.send(result);
            }
            OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                let result = crate::position_seeder::ledger_capture(
                    self.bucket_engine.ledger(),
                    &self.paper_state,
                    wallet,
                )
                .map_err(|error| error.to_string());
                let _ = captured.send(result);
            }
            OrchestratorControl::CommitActivityBucket {
                aggregates,
                context,
                committed,
            } => {
                // Every position-changing commit invalidates an accepted bracket in
                // the same SQLite transaction. Serialize that commit against the
                // final membership recheck/publication so a wallet cannot become
                // visible from a proof invalidated between those two operations.
                let writer_lock = self.watchlist_writer_lock.clone();
                let result = if let Some(writer_lock) = writer_lock {
                    let _writer_guard = writer_lock.lock().await;
                    // Freeze the mutable decision basis UNDER the writer lock
                    // (#544 review round 3): capture is linearized against a
                    // concurrent refresh, and later watchlist/bankroll moves
                    // cannot change what a resumed continuation decides.
                    let frozen_basis = self.freeze_decision_basis(&aggregates);
                    let result =
                        self.bucket_engine
                            .commit(aggregates, context.as_ref(), frozen_basis);
                    if let Ok(result) = &result
                        && result.newly_fenced.is_some()
                    {
                        let mut fenced = HashSet::new();
                        fenced.insert(result.wallet);
                        // The attached bounded latest-only notifier marks the effective
                        // projection dirty after this post-commit removal (#544).
                        self.live_watchlist.remove_fenced(&fenced);
                    }
                    result
                } else {
                    // Scenario/unit construction may omit the production writer lock;
                    // those harnesses have no competing membership writer to
                    // linearize the basis capture against.
                    let frozen_basis = self.freeze_decision_basis(&aggregates);
                    let result =
                        self.bucket_engine
                            .commit(aggregates, context.as_ref(), frozen_basis);
                    if let Ok(result) = &result
                        && result.newly_fenced.is_some()
                    {
                        let mut fenced = HashSet::new();
                        fenced.insert(result.wallet);
                        self.live_watchlist.remove_fenced(&fenced);
                    }
                    result
                };
                if let Ok(result) = &result {
                    for source_trade_id in &result.pending {
                        match self.load_pending_continuation(source_trade_id) {
                            Ok(Some(trade)) => self.handle_trade(trade).await,
                            Ok(None) => {}
                            Err(error) => {
                                error!(%error, trade = %source_trade_id, "load committed decision continuation failed");
                            }
                        }
                    }
                }
                let result = result.map_err(|error| error.to_string());
                let _ = committed.send(result);
            }
            OrchestratorControl::ResolutionCandidate {
                condition,
                payout_by_outcome_index_json,
                receipt,
                acknowledged,
            } => {
                let result = self
                    .apply_resolution_candidate(condition, payout_by_outcome_index_json, receipt)
                    .await;
                if result.is_err() {
                    self.intake_stopped = true;
                }
                let _ = acknowledged.send(result);
            }
            OrchestratorControl::PublishMembership {
                change,
                replacements,
                acknowledged,
            } => {
                let removed = change.removed.iter().copied().collect::<HashSet<_>>();
                let capacity = change.capacity;
                let receipt = self.append_paper_record(&change.into_record());
                if receipt.is_ok() {
                    self.live_watchlist
                        .replace(&removed, &replacements, capacity);
                }
                let result = receipt;
                let _ = acknowledged.send(result);
            }
            OrchestratorControl::RiskHaltChange {
                owner,
                cause,
                state,
                evidence,
                acknowledged,
            } => {
                let result = self.apply_risk_halt_transition(owner, cause, state, evidence);
                let _ = acknowledged.send(result);
            }
            OrchestratorControl::DailyBoundary {
                cutoff_unix,
                boundary_receipt,
                acknowledged,
            } => {
                let result = self.mark_at_boundary(cutoff_unix, boundary_receipt).await;
                let _ = acknowledged.send(result);
            }
            OrchestratorControl::SealCheck {
                proposed_economic_hash,
                proposed_financial_semantic_version,
                acknowledged,
            } => {
                let result = match self.reconcile_oldest_financial_prepared().await {
                    Ok(()) => self.apply_seal_check(
                        &proposed_economic_hash,
                        proposed_financial_semantic_version,
                    ),
                    Err(error) => Err(error),
                };
                let _ = acknowledged.send(result);
            }
        }
    }
}

impl<F: PageFetcher + Send + Sync, B: ClobBookFetcher> Orchestrator<F, B> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        live_watchlist: LiveWatchlist,
        config: OrchestratorConfig,
        strategy: WinnerFollowStrategy,
        paper_writer: Writer,
        paper_state: Arc<PaperStateDb>,
        leader_ledger: PositionLedger,
        health: SharedHealth,
        mid_price_cache: MidPriceCache<F>,
        control_rx: mpsc::Receiver<OrchestratorControl>,
        _sink: Option<SinkHandle>,
        snapshot_sink: Option<SnapshotHandle>,
        supabase_state: Option<SupabaseStateClient>,
        book_fetcher: Arc<B>,
    ) -> Result<Self, anyhow::Error> {
        Self::new_inner(
            live_watchlist,
            config,
            strategy,
            paper_writer,
            paper_state,
            leader_ledger,
            health,
            mid_price_cache,
            control_rx,
            _sink,
            snapshot_sink,
            supabase_state,
            book_fetcher,
        )
    }
}

impl<F: PageFetcher + Send + Sync, B: ClobBookFetcher, S: SupabaseStateTrait + Clone>
    Orchestrator<F, B, S>
{
    /// Scenario constructor for the same orchestrator with an in-memory financial authority.
    #[cfg(feature = "scenario")]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_authority(
        live_watchlist: LiveWatchlist,
        config: OrchestratorConfig,
        strategy: WinnerFollowStrategy,
        paper_writer: Writer,
        paper_state: Arc<PaperStateDb>,
        leader_ledger: PositionLedger,
        health: SharedHealth,
        mid_price_cache: MidPriceCache<F>,
        control_rx: mpsc::Receiver<OrchestratorControl>,
        snapshot_sink: Option<SnapshotHandle>,
        supabase_state: S,
        book_fetcher: Arc<B>,
    ) -> Result<Self, anyhow::Error> {
        Self::new_inner(
            live_watchlist,
            config,
            strategy,
            paper_writer,
            paper_state,
            leader_ledger,
            health,
            mid_price_cache,
            control_rx,
            None,
            snapshot_sink,
            Some(supabase_state),
            book_fetcher,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        live_watchlist: LiveWatchlist,
        config: OrchestratorConfig,
        strategy: WinnerFollowStrategy,
        paper_writer: Writer,
        paper_state: Arc<PaperStateDb>,
        leader_ledger: PositionLedger,
        health: SharedHealth,
        mid_price_cache: MidPriceCache<F>,
        control_rx: mpsc::Receiver<OrchestratorControl>,
        _sink: Option<SinkHandle>,
        snapshot_sink: Option<SnapshotHandle>,
        supabase_state: Option<S>,
        book_fetcher: Arc<B>,
    ) -> Result<Self, anyhow::Error> {
        let min_quality = ReconstructionQuality::new(0)
            .map_err(|_| anyhow::anyhow!("internal: ReconstructionQuality::new(0) failed"))?;
        if !(1..=10_000).contains(&config.price_impact_cap_bps) {
            return Err(anyhow::anyhow!(
                "price_impact_cap_bps must be in 1..=10_000"
            ));
        }

        // Seed the held-position guard from the exact active-era projection. Fractional positions
        // must not disappear across restart merely because the legacy integer view floors them.
        let filled_positions: HashSet<MarketOutcomeId> = if paper_state
            .financial_start()
            .map_err(|e| anyhow::anyhow!("load financial Start: {e}"))?
            .is_some()
        {
            paper_state
                .financial_snapshot(OffsetDateTime::now_utc().unix_timestamp())
                .map_err(|e| anyhow::anyhow!("load exact paper positions: {e}"))?
                .positions
                .into_iter()
                .filter(|position| {
                    position.long != ShareAmount::ZERO || position.short != ShareAmount::ZERO
                })
                .map(|position| MarketOutcomeId::new(position.market_id, position.outcome_id))
                .collect()
        } else {
            paper_state
                .paper_positions()
                .map_err(|e| anyhow::anyhow!("load legacy paper positions: {e}"))?
                .into_iter()
                .filter(|position| {
                    position.long != ShareAmount::ZERO || position.short != ShareAmount::ZERO
                })
                .map(|position| MarketOutcomeId::new(position.market_id, position.outcome_id))
                .collect()
        };

        let bucket_engine = BucketCommitEngine::load(paper_state.clone(), leader_ledger)
            .map_err(|error| anyhow::anyhow!("load bucket commit engine: {error}"))?;
        for row in paper_state
            .decision_pending_history()
            .map_err(|error| anyhow::anyhow!("load decision continuation history: {error}"))?
            .into_iter()
            .filter(|row| row.state == pe_paper_state::DecisionPendingState::Terminal)
        {
            replay_decision_pending(&row).map_err(|error| {
                anyhow::anyhow!("verify terminal pending {}: {error}", row.source_trade_id)
            })?;
        }
        let mut pending_boot = VecDeque::new();
        let mut pending_continuations = HashMap::new();
        for row in paper_state
            .open_decision_pending()
            .map_err(|error| anyhow::anyhow!("load open decision continuations: {error}"))?
        {
            let continuation = DecisionContinuationV3::from_durable(&row).map_err(|error| {
                anyhow::anyhow!("decode pending {}: {error}", row.source_trade_id)
            })?;
            let trade = continuation.incoming_trade().map_err(|error| {
                anyhow::anyhow!("rebuild pending {}: {error}", row.source_trade_id)
            })?;
            pending_continuations.insert(row.source_trade_id, continuation);
            pending_boot.push_back(trade);
        }
        Ok(Self {
            bucket_engine,
            live_watchlist,
            signal_config: config.signal_config,
            strategy,
            paper_writer,
            mode: config.mode,
            bankroll: config.bankroll,
            paper_state,
            health,
            mid_price_cache,
            activity_ws_enabled: config.activity_ws_enabled,
            copy_latency_budget_secs: config.copy_latency_budget_secs,
            #[cfg(feature = "scenario")]
            scenario_hooks: None,
            max_resolution_horizon_secs: config.max_resolution_horizon_secs,
            min_resolution_horizon_secs: config.min_resolution_horizon_secs,
            max_fill_price: config.max_fill_price,
            min_fill_price: config.min_fill_price,
            filled_positions,
            min_quality,
            control_rx,
            snapshot_sink,
            supabase_state,
            runtime_config: config.runtime_config,
            book_fetcher,
            price_impact_cap_bps: config.price_impact_cap_bps,
            live_accounts: config.live_accounts,
            intake_stopped: false,
            watchlist_writer_lock: config.watchlist_writer_lock,
            pending_boot,
            pending_continuations,
            resuming_boot: false,
            financial_log_paths: None,
            source_receipts: None,
            qualification_start: None,
            admission_builder: None,
            boundary_mark_fetcher: None,
            active_risk_halts: HashSet::new(),
        })
    }

    /// Install the verified log pair used by the Start-bound financial protocol.
    pub fn configure_financial_log_paths(
        &mut self,
        paper_log_path: std::path::PathBuf,
        source_log_path: std::path::PathBuf,
        admission_builder: crate::live_venue_adapter::LiveAdmissionBuilder,
        boundary_mark_fetcher: Arc<HistoricalMarkAdapter>,
        source_receipts: SourceReceiptIndex,
    ) -> Result<(), crate::paper_recovery::PaperLogScanError> {
        let era = crate::paper_recovery::paper_era(crate::paper_recovery::scan_paper_log(
            &paper_log_path,
        )?);
        self.active_risk_halts = crate::paper_recovery::active_risk_halts(&era);
        self.qualification_start = era.start.as_ref().map(|(receipt, _)| *receipt);
        self.financial_log_paths = Some((paper_log_path, source_log_path));
        self.source_receipts = Some(source_receipts);
        self.admission_builder = Some(admission_builder);
        self.boundary_mark_fetcher = Some(boundary_mark_fetcher);
        Ok(())
    }

    /// Install the scenario-only clock/fault seams (#546). Scenario builds only.
    #[cfg(feature = "scenario")]
    pub fn set_scenario_hooks(&mut self, hooks: Arc<ScenarioHooks>) {
        self.scenario_hooks = Some(hooks);
    }

    /// Wall clock for the copy-budget checks; scenario builds may pop fixed instants.
    fn admission_now(&self) -> OffsetDateTime {
        #[cfg(feature = "scenario")]
        if let Some(instant) = self
            .scenario_hooks
            .as_ref()
            .and_then(|h| h.age_clock.lock().ok()?.pop_front())
        {
            return instant;
        }
        OffsetDateTime::now_utc()
    }

    /// Apply the scenario-only elapsed time attributed to receipt-scoped observation resolution.
    #[cfg(feature = "scenario")]
    fn apply_observation_resolution_clock_advance(&self) {
        let Some(hooks) = self.scenario_hooks.as_ref() else {
            return;
        };
        let advance_millis = hooks
            .observation_resolution_advance_millis
            .swap(0, std::sync::atomic::Ordering::SeqCst);
        if advance_millis == 0 {
            return;
        }
        let mut clock = hooks
            .age_clock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(next) = clock.front_mut()
            && let Some(advanced) = next.checked_add(time::Duration::milliseconds(advance_millis))
        {
            *next = advanced;
        }
    }

    fn load_pending_continuation(
        &mut self,
        source_trade_id: &SourceTradeId,
    ) -> Result<Option<IncomingTrade>, anyhow::Error> {
        let row = self
            .paper_state
            .open_decision_pending()
            .map_err(|error| anyhow::anyhow!("read open continuations: {error}"))?
            .into_iter()
            .find(|row| &row.source_trade_id == source_trade_id);
        let Some(row) = row else {
            return Ok(None);
        };
        let continuation = DecisionContinuationV3::from_durable(&row)
            .map_err(|error| anyhow::anyhow!("decode pending {source_trade_id}: {error}"))?;
        let trade = continuation
            .incoming_trade()
            .map_err(|error| anyhow::anyhow!("rebuild pending {source_trade_id}: {error}"))?;
        self.pending_continuations
            .insert(source_trade_id.clone(), continuation);
        Ok(Some(trade))
    }

    #[cfg(feature = "scenario")]
    fn take_scenario_fault(
        &self,
        select: impl FnOnce(&ScenarioHooks) -> &std::sync::atomic::AtomicBool,
    ) -> bool {
        self.scenario_hooks
            .as_ref()
            .is_some_and(|h| select(h).swap(false, std::sync::atomic::Ordering::SeqCst))
    }

    /// Resume every open durable continuation before producers or the control loop start.
    /// Continuations run in causal boot order; uncertain paper durability aborts recovery.
    pub async fn resume_pending_before_producers(&mut self) -> Result<(), anyhow::Error> {
        // Production boot recovery runs before either producer receiver is polled.
        // `handle_trade` detects the durable pending owner and skips ledger/gate/history.
        self.resuming_boot = true;
        while let Some(trade) = self.pending_boot.pop_front() {
            self.handle_trade(trade).await;
            if self.intake_stopped {
                self.resuming_boot = false;
                return Err(anyhow::anyhow!(
                    "paper durability became uncertain while resuming decision_pending"
                ));
            }
        }
        self.resuming_boot = false;
        Ok(())
    }

    pub async fn run(mut self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        if let Err(error) = self.resume_pending_before_producers().await {
            error!(%error, "decision_pending boot recovery failed");
            return;
        }
        let mut control_done = false;
        loop {
            if control_done {
                break;
            }

            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    break;
                }
                result = self.control_rx.recv(), if !control_done => {
                    match result {
                        Some(message) => self.apply_control_message(message).await,
                        None => control_done = true,
                    }
                }
            }
        }
    }

    /// Supervised production loop. Once `shutdown` resolves this owner drains accepted control
    /// input before returning. Control-channel closure before coordinated shutdown is a typed
    /// critical failure.
    pub async fn run_coordinated(
        mut self,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), OrchestratorRunError> {
        tokio::pin!(shutdown);
        self.resume_pending_before_producers()
            .await
            .map_err(|error| OrchestratorRunError::PendingRecovery(error.to_string()))?;
        let mut draining = false;
        let mut control_done = false;
        loop {
            if self.intake_stopped {
                return Err(OrchestratorRunError::PaperDurabilityUncertain);
            }
            if draining && control_done {
                return Ok(());
            }

            tokio::select! {
                biased;
                _ = &mut shutdown, if !draining => draining = true,
                result = self.control_rx.recv(), if !control_done => {
                    match result {
                        Some(message) => self.apply_control_message(message).await,
                        None if draining => control_done = true,
                        None => return Err(OrchestratorRunError::PrematureInputClosure {
                            channel: "orchestrator_control",
                        }),
                    }
                }
            }
        }
    }

    // ── Private handlers ──────────────────────────────────────────────────────

    /// Impact-gate ladder plan (#508 Phase A): one `/book` fetch per admitted signal feeds
    /// the band gate, sizing plan, and paper fill basis.
    ///
    /// Called only when the gate is enabled (`price_impact_cap_bps ≥ 1`). Every failure is a
    /// typed skip reason and **fails closed** — unusable book (missing token, fetch
    /// error/timeout, corrupt levels, empty ladder, stale snapshot) and an in-band ladder
    /// affording no atomic share both produce no order. The planner budget follows the sizing
    /// mode: `Dollar` requests its USD notional; `Contract` requests its exact configured count;
    /// `Kelly` requests its allocator result. The venue planner declines requests that depth or a
    /// monetary bound cannot satisfy.
    async fn plan_impact_gate(
        &self,
        signal: &LeaderSignal,
        probability: Probability,
        sizing_bankroll: Decimal,
        admission: &pe_execution_core::LiveAdmissionArtifact,
    ) -> Result<GatePlanEvidence, GatePlanFailure> {
        let cap_bps = u64::try_from(self.price_impact_cap_bps).map_err(|_| GatePlanFailure {
            reason: "impact gate cap invalid",
            book: Box::new(book_failure(
                None,
                "not_read",
                None,
                None,
                "impact gate cap invalid",
            )),
            checked_at_unix_ms: None,
        })?;
        let token_id = admission
            .market
            .ordered_outcome_token_ids
            .get(usize::from(signal.outcome_id.0))
            .map(ToString::to_string);
        let Some(token_id) = token_id else {
            return Err(GatePlanFailure {
                reason: "price-impact book unusable: missing CLOB token (fail closed)",
                book: Box::new(book_failure(
                    None,
                    "missing_token",
                    None,
                    None,
                    "outcome had no CLOB token id",
                )),
                checked_at_unix_ms: None,
            });
        };
        let book = match tokio::time::timeout(
            Duration::from_secs(CLOB_BOOK_HOT_PATH_TIMEOUT_SECS),
            self.book_fetcher
                .fetch_book(&admission.market.condition_id.0, &token_id),
        )
        .await
        {
            Ok(Ok(book)) => book,
            Ok(Err(_)) => {
                return Err(GatePlanFailure {
                    reason: "price-impact book unusable: /book fetch failed (fail closed)",
                    book: Box::new(book_failure(
                        Some(&token_id),
                        "fetch_failed",
                        None,
                        None,
                        "/book fetch failed",
                    )),
                    checked_at_unix_ms: None,
                });
            }
            Err(_) => {
                return Err(GatePlanFailure {
                    reason: "price-impact book unusable: /book fetch timed out (fail closed)",
                    book: Box::new(book_failure(
                        Some(&token_id),
                        "fetch_timed_out",
                        None,
                        None,
                        "/book fetch timed out",
                    )),
                    checked_at_unix_ms: None,
                });
            }
        };
        let Some(ladder) = book.ladder() else {
            return Err(GatePlanFailure {
                reason: "price-impact book unusable: corrupt ask levels (fail closed)",
                book: Box::new(book_failure(
                    Some(&token_id),
                    "corrupt_ladder",
                    Some(&book.response_blake3),
                    Some(book.fetched_at_ms),
                    "ask levels could not form an exact ladder",
                )),
                checked_at_unix_ms: None,
            });
        };
        let Some(best) = ladder.first().map(|l| l.price) else {
            return Err(GatePlanFailure {
                reason: "price-impact book unusable: empty ask book (fail closed)",
                book: Box::new(book_failure(
                    Some(&token_id),
                    "empty_ladder",
                    Some(&book.response_blake3),
                    Some(book.fetched_at_ms),
                    "ask ladder was empty",
                )),
                checked_at_unix_ms: None,
            });
        };
        let checked_at_unix_ms = crate::clob_book::now_unix_ms();
        if ladder_is_stale(checked_at_unix_ms, book.fetched_at_ms) {
            return Err(GatePlanFailure {
                reason: "price-impact book unusable: stale snapshot (fail closed)",
                book: Box::new(BookEvidence {
                    request_token_id: Some(token_id),
                    outcome: "stale".to_owned(),
                    response_blake3: Some(book.response_blake3),
                    fetched_at_unix_ms: Some(book.fetched_at_ms),
                    best_ask: Some(best.0.normalize().to_string()),
                    vwap_basis: None,
                    ladder_plan_blake3: None,
                    reason: Some("snapshot exceeded the ladder age bound".to_owned()),
                }),
                checked_at_unix_ms: Some(checked_at_unix_ms),
            });
        }
        // Inclusive band ceiling: best × (1 + cap/10_000), clamped into the Price domain —
        // the same edge semantic as the analytics `absorbable_usd_100bps` column.
        let ceiling_raw = (best.0
            * (Decimal::from(10_000u32 + u32::try_from(cap_bps).unwrap_or(10_000))
                / Decimal::from(10_000u32)))
        .min(Decimal::ONE);
        let ceiling = Price::new(ceiling_raw).map_err(|_| GatePlanFailure {
            reason: "price-impact band ceiling not constructible",
            book: Box::new(book_failure(
                Some(&token_id),
                "arithmetic_failure",
                Some(&book.response_blake3),
                Some(book.fetched_at_ms),
                "price-impact band ceiling not constructible",
            )),
            checked_at_unix_ms: Some(checked_at_unix_ms),
        })?;
        // Planner budget from the sizing mode; 6-dp truncation never overstates the budget.
        let to_budget = |d: Decimal| {
            CollateralAmount::from_decimal_exact(
                d.max(Decimal::ZERO)
                    .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero),
            )
            .map_err(|_| GatePlanFailure {
                reason: "impact budget not constructible",
                book: Box::new(book_failure(
                    Some(&token_id),
                    "arithmetic_failure",
                    Some(&book.response_blake3),
                    Some(book.fetched_at_ms),
                    "impact budget not constructible",
                )),
                checked_at_unix_ms: Some(checked_at_unix_ms),
            })
        };
        let cash_cap = to_budget(sizing_bankroll)?;
        let per_trade_cap_bps = self
            .strategy
            .config()
            .per_trade_cap
            .resolve_bps(TradingMode::LiveTiny);
        let proportional_cap = to_budget(
            sizing_bankroll
                .checked_mul(Decimal::from(per_trade_cap_bps))
                .and_then(|value| value.checked_div(Decimal::from(10_000)))
                .ok_or_else(|| GatePlanFailure {
                    reason: "impact budget not constructible",
                    book: Box::new(book_failure(
                        Some(&token_id),
                        "arithmetic_failure",
                        Some(&book.response_blake3),
                        Some(book.fetched_at_ms),
                        "per-trade cap not constructible",
                    )),
                    checked_at_unix_ms: Some(checked_at_unix_ms),
                })?,
        )?;
        let kelly_fraction = self.strategy.effective_kelly_fraction(self.mode);
        let allocate = |price: Price| {
            let quantity = size_contracts(&KellyInput {
                p: probability,
                c: price,
                kelly_fraction,
                bankroll: sizing_bankroll,
            })
            .map_err(|_| LadderError::KellySizing)?;
            ShareAmount::from_whole(quantity.0).map_err(|_| LadderError::Amount)
        };
        let sizing = match self.strategy.config().sizing_mode {
            SizingMode::Dollar { usd } => BuySizing::Dollar {
                budget: to_budget(usd)?,
            },
            SizingMode::Contract { contracts } => BuySizing::Contract { contracts },
            SizingMode::Kelly => BuySizing::Kelly {
                allocate: &allocate,
                slippage_rate: self.strategy.config().slippage_rate,
            },
        };
        let maximum_price_exclusive = Price::new(if self.max_fill_price > Decimal::ZERO {
            self.max_fill_price
        } else {
            Decimal::ONE
        })
        .map_err(|_| GatePlanFailure {
            reason: "price domain invariant broken",
            book: Box::new(book_failure(
                Some(&token_id),
                "arithmetic_failure",
                Some(&book.response_blake3),
                Some(book.fetched_at_ms),
                "price domain invariant broken",
            )),
            checked_at_unix_ms: Some(checked_at_unix_ms),
        })?;
        let minimum_price =
            Price::new(self.min_fill_price.max(Decimal::ZERO)).map_err(|_| GatePlanFailure {
                reason: "price domain invariant broken",
                book: Box::new(book_failure(
                    Some(&token_id),
                    "arithmetic_failure",
                    Some(&book.response_blake3),
                    Some(book.fetched_at_ms),
                    "price band floor not constructible",
                )),
                checked_at_unix_ms: Some(checked_at_unix_ms),
            })?;
        let schedule = admission.fee_schedule;
        let minimum_order_size = admission.market.minimum_order_size;
        let minimum_tick_size = admission.market.minimum_tick_size;
        match plan_sized_buy(
            &ladder,
            schedule,
            sizing,
            &[cash_cap, proportional_cap],
            minimum_order_size,
            minimum_tick_size,
            minimum_price,
            maximum_price_exclusive,
            signal.leader_price,
            ceiling,
        ) {
            Ok(sized) => {
                let budget = sized.budget;
                let worst_case_all_in_debit =
                    sized
                        .worst_case_all_in_debit()
                        .map_err(|_| GatePlanFailure {
                            reason: "price-impact ladder arithmetic failed (fail closed)",
                            book: Box::new(book_failure(
                                Some(&token_id),
                                "arithmetic_failure",
                                Some(&book.response_blake3),
                                Some(book.fetched_at_ms),
                                "all-in ladder debit could not be derived",
                            )),
                            checked_at_unix_ms: Some(checked_at_unix_ms),
                        })?;
                let plan = sized.ladder;
                Ok(GatePlanEvidence {
                    book: BookEvidence {
                        request_token_id: Some(token_id),
                        outcome: "planned".to_owned(),
                        response_blake3: Some(book.response_blake3),
                        fetched_at_unix_ms: Some(book.fetched_at_ms),
                        best_ask: Some(plan.best_ask.0.normalize().to_string()),
                        vwap_basis: plan.vwap().map(|value| value.0.normalize().to_string()),
                        ladder_plan_blake3: Some(ladder_plan_blake3(&plan)),
                        reason: None,
                    },
                    gate: GatePlan::Planned(plan),
                    book_receipt: book.source_receipt,
                    budget,
                    worst_case_all_in_debit,
                    checked_at_unix_ms,
                })
            }
            // Decision 10 taxonomy (#508): a SUCCESSFUL read whose in-band depth cannot
            // absorb the paper budget is a PAPER-ONLY decision — it must never suppress
            // otherwise-admissible live targets, so it is not a shared rejection. The
            // best ask from the successful read anchors the shared band gate.
            Err(LadderError::NothingAffordable) => Ok(GatePlanEvidence {
                book: BookEvidence {
                    request_token_id: Some(token_id),
                    outcome: "nothing_affordable".to_owned(),
                    response_blake3: Some(book.response_blake3),
                    fetched_at_unix_ms: Some(book.fetched_at_ms),
                    best_ask: Some(best.0.normalize().to_string()),
                    vwap_basis: None,
                    ladder_plan_blake3: None,
                    reason: Some("budget afforded no atomic share".to_owned()),
                },
                gate: GatePlan::NothingAffordable { best_ask: best },
                book_receipt: book.source_receipt,
                budget: CollateralAmount::ZERO,
                worst_case_all_in_debit: CollateralAmount::ZERO,
                checked_at_unix_ms,
            }),
            Err(LadderError::BelowBandAsk | LadderError::InsufficientDepth) => {
                Err(GatePlanFailure {
                    reason: "price-impact ladder walk failed (fail closed)",
                    book: Box::new(book_failure(
                        Some(&token_id),
                        "ladder_rejected",
                        Some(&book.response_blake3),
                        Some(book.fetched_at_ms),
                        "ladder walk did not satisfy the price band",
                    )),
                    checked_at_unix_ms: Some(checked_at_unix_ms),
                })
            }
            Err(
                LadderError::Amount
                | LadderError::BelowMinimum
                | LadderError::CapExceeded
                | LadderError::NoEdge
                | LadderError::KellySizing
                | LadderError::Fee(_),
            ) => Err(GatePlanFailure {
                reason: "price-impact ladder arithmetic failed (fail closed)",
                book: Box::new(book_failure(
                    Some(&token_id),
                    "arithmetic_failure",
                    Some(&book.response_blake3),
                    Some(book.fetched_at_ms),
                    "ladder arithmetic failed",
                )),
                checked_at_unix_ms: Some(checked_at_unix_ms),
            }),
        }
    }

    /// Stage the #508 dispatch aggregate when the current live-accounts snapshot carries
    /// armed targets (Decision 10: staged at shared signal admission, durably, BEFORE the
    /// paper outcome can become durable). Returns the staged `dispatch_id`, or `None` when
    /// no target exists (no aggregate; the Phase-A baseline path). A staging failure is a
    /// hard skip signalled as `Err` — the caller abandons the trade without marking it
    /// seen, so a redelivery can retry the whole admission.
    fn stage_dispatch_if_targeted(
        &self,
        signal: &LeaderSignal,
        observation: &pe_execution_core::ObservationEvidence,
        evidence: &mut Option<DecisionEvidenceAccumulator>,
    ) -> Result<Option<String>, ()> {
        let Some(live) = self.live_accounts.as_ref() else {
            return Ok(None);
        };
        let snapshot = live.snapshot();
        // #514: no NEW live aggregates while blind — a stale/never-successful accounts
        // snapshot stages nothing. Paper execution proceeds unchanged; in-flight recovery
        // and redemption reconciliation do not gate on freshness.
        let accounts_at = OffsetDateTime::now_utc();
        record_clock(evidence, "live_accounts_freshness", accounts_at);
        if !snapshot.is_fresh(accounts_at.unix_timestamp()) {
            warn!(
                "live accounts snapshot is stale; dispatch staging paused (no new live aggregates)"
            );
            return Ok(None);
        }
        let armed = snapshot.armed_targets();
        if armed.is_empty() {
            return Ok(None);
        }
        let dispatch_id = pe_strategy_winner_follow::build_idempotency_key(signal);
        let targets = armed
            .iter()
            .filter_map(|account| {
                account
                    .credential_binding
                    .as_ref()
                    .map(|(version, key_id)| pe_paper_state::DispatchTargetSeed {
                        account_id: account.account_id.as_str().to_owned(),
                        credential_bundle_version: *version,
                        credential_key_id: key_id.clone(),
                    })
            })
            .collect::<Vec<_>>();
        // The frozen signal + decision identity: redelivery replays THIS, never current config.
        let frozen = serde_json::json!({
            "schema_version": 1,
            "signal": signal,
            "observation": observation,
            "price_impact_cap_bps": self.price_impact_cap_bps,
            "mode": format!("{:?}", self.mode),
        });
        let staged_at = OffsetDateTime::now_utc();
        record_clock(evidence, "dispatch_seed_created", staged_at);
        let record = pe_paper_state::DispatchSeedRecord {
            dispatch_id: dispatch_id.clone(),
            signal_json: frozen.to_string(),
            source_trade_id: signal.source_trade_id.0.clone(),
            created_at_unix: staged_at.unix_timestamp(),
            targets,
        };
        #[cfg(feature = "scenario")]
        if self.take_scenario_fault(|h| &h.fail_next_stage_seed) {
            error!(dispatch_id = %dispatch_id,
                "scenario fault: dispatch seed staging failed; abandoning the trade unseen");
            return Err(());
        }
        let pending = match render_pending_evidence(
            evidence.as_ref(),
            AuthorityEvidence::not_read("dispatch_staged_before_fill_authority"),
            TerminalDispositionEvidence::dispatch_staged(dispatch_id.clone()),
        ) {
            Ok(value) => value,
            Err(error) => {
                error!(%error, dispatch_id = %dispatch_id, "encode dispatch decision evidence failed");
                return Err(());
            }
        };
        match self
            .paper_state
            .stage_dispatch_seed_pending(&record, pending.as_ref().map(pending_terminal))
        {
            Ok(_staged_or_reused) => Ok(Some(dispatch_id)),
            Err(e) => {
                error!(
                    error = %e,
                    dispatch_id = %dispatch_id,
                    "dispatch seed staging failed; abandoning the trade unseen (fail closed for \
                     paper AND live — a redelivery retries the whole admission)"
                );
                Err(())
            }
        }
    }

    fn apply_runtime_snapshot(&mut self, rc: &runtime_config::RuntimeConfig) {
        self.strategy.set_config(rc.winner_follow_config());
        if let Some(mode) = runtime_config::parse_execution_mode(&rc.mode) {
            self.mode = mode;
        }
        self.max_fill_price = rc.max_fill_price;
        self.min_fill_price = rc.min_fill_price;
        self.max_resolution_horizon_secs = rc.max_resolution_horizon_secs;
        self.min_resolution_horizon_secs = rc.min_resolution_horizon_secs;
        self.price_impact_cap_bps = rc.price_impact_cap_bps;
    }

    async fn handle_trade(&mut self, trade: IncomingTrade) {
        let source_trade_id = trade.source_trade_id.clone();
        let pending_owned = self.pending_continuations.contains_key(&source_trade_id);
        self.handle_trade_once(trade).await;
        if pending_owned {
            match self.paper_state.is_decision_pending_open(&source_trade_id) {
                Ok(true) => {
                    error!(trade = %source_trade_id, "decision_pending continuation remained open; stopping producer intake for boot recovery");
                    self.intake_stopped = true;
                }
                Ok(false) => {
                    self.pending_continuations.remove(&source_trade_id);
                }
                Err(error) => {
                    error!(%error, trade = %source_trade_id, "verify decision_pending terminal transition failed");
                    self.intake_stopped = true;
                }
            }
        }
    }

    async fn handle_trade_once(&mut self, mut trade: IncomingTrade) {
        let pending = match self
            .pending_continuations
            .get(&trade.source_trade_id)
            .cloned()
        {
            Some(continuation) => match self
                .paper_state
                .is_decision_pending_open(&trade.source_trade_id)
            {
                Ok(true) => {
                    match continuation.incoming_trade() {
                        Ok(frozen_trade) => trade = frozen_trade,
                        Err(error) => {
                            error!(%error, trade = %trade.source_trade_id, "invalid durable continuation");
                            return;
                        }
                    }
                    Some(continuation)
                }
                Ok(false) => {
                    self.pending_continuations.remove(&trade.source_trade_id);
                    None
                }
                Err(error) => {
                    error!(%error, trade = %trade.source_trade_id, "read pending continuation state failed");
                    return;
                }
            },
            None => None,
        };
        let mut decision_evidence = pending
            .as_ref()
            .map(|continuation| DecisionEvidenceAccumulator::new(&continuation.facts));
        // New decisions use the current hot snapshot. A committed continuation instead
        // reinstalls its complete frozen 17-key snapshot before any post-boundary read.
        let applied_runtime = pending
            .as_ref()
            .map(|continuation| continuation.facts.applied_configuration.clone())
            .or_else(|| {
                self.runtime_config
                    .as_ref()
                    .map(|live| live.snapshot().as_ref().clone())
            });
        if let Some(rc) = applied_runtime.as_ref() {
            self.apply_runtime_snapshot(rc);
        }

        // Mark polymarket freshness; in the same lock, evaluate the #530
        // dual-unhealthy admission block (websocket stale-or-worse AND the poll
        // source unhealthy). A blocked trade is refused BEFORE any state write -
        // it stays unseen, so the held cursor / firehose backstop redelivers it
        // exactly once when a source recovers.
        let admission_blocked = !self.resuming_boot && {
            let h = self
                .health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            h.copy_admission_blocked(OffsetDateTime::now_utc(), tokio::time::Instant::now())
        };
        if admission_blocked {
            warn!(
                trade = %trade.source_trade_id,
                "copy admission blocked: both trade sources unhealthy (#530); holding the trade until a source recovers"
            );
            // #530 review F1: dropping the refused trade would orphan it — a
            // websocket-delivered trade behind the poll cursor is never refetched
            // (the cursor is a bandwidth bound, not a redelivery guarantee). Hold
            // THIS trade instead: the bounded channel backpressures upstream,
            // order is preserved, and nothing stages while both sources are
            // unhealthy. Shutdown still works (the service aborts this task).
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                let unblocked = {
                    let h = self
                        .health
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    !h.copy_admission_blocked(
                        OffsetDateTime::now_utc(),
                        tokio::time::Instant::now(),
                    )
                };
                if unblocked {
                    break;
                }
            }
        }
        // Liveness marks only for trades that passed (or outlasted) the gate.
        {
            let mut h = self
                .health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            h.polymarket_last_event_at = Some(OffsetDateTime::now_utc());
        }

        // INPUT DEDUP: the first action after recording freshness, before any
        // ledger/cluster mutation. A trade already processed (RTDS then poll
        // backstop, or a re-poll) must not advance the leader ledger a second time.
        match self.paper_state.is_seen(&trade.source_trade_id) {
            Ok(true) if pending.is_some() => {}
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                error!(error = %e, "paper-state is_seen failed; skipping trade");
                return;
            }
        }

        // A durable fence is an immediate decision boundary (#544). Legacy v1 deliveries
        // still advance the exact ledger when their effect is known, but must not read the
        // watchlist, classify, mutate the entry gate, fetch market/book evidence, fill, or seed.
        if self.bucket_engine.is_fenced(&trade.wallet) {
            if pending.is_some() {
                let leader = self.leader_position_row(&trade);
                let fenced_at = self.admission_now();
                record_clock(
                    &mut decision_evidence,
                    "wallet_fence_terminal_check",
                    fenced_at,
                );
                let disposition = pe_paper_state::NoCopyDisposition {
                    provenance: "reconciled_rest".to_owned(),
                    age_secs: 0,
                    reason: "wallet_fenced_before_dispatch".to_owned(),
                    recorded_at_unix: fenced_at.unix_timestamp(),
                };
                self.commit_no_copy_or_rollback(
                    &trade,
                    &leader,
                    &disposition,
                    &RollbackCtx {
                        wallet: trade.wallet,
                        key: MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id),
                        prev: None,
                        enabled: false,
                    },
                    None,
                    decision_evidence.as_ref(),
                );
                return;
            }
            let key = MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id);
            let previous = self
                .bucket_engine
                .ledger()
                .position(&trade.wallet)
                .and_then(|snapshot| snapshot.positions.get(&key))
                .map(|state| (state.long_contracts, state.short_contracts));
            let rollback = RollbackCtx {
                wallet: trade.wallet,
                key,
                prev: previous,
                enabled: true,
            };
            if let Err(error) = self.bucket_engine.ledger_mut().ingest(&trade) {
                error!(%error, trade = %trade.source_trade_id, "fenced-wallet ledger advance failed");
                return;
            }
            let leader = self.leader_position_row(&trade);
            let provenance = match trade.provenance {
                TradeProvenance::RestPoll => "rest_poll",
                TradeProvenance::ActivityWs => "activity_ws",
            };
            self.commit_no_copy_or_rollback(
                &trade,
                &leader,
                &pe_paper_state::NoCopyDisposition {
                    provenance: provenance.to_owned(),
                    age_secs: 0,
                    reason: "wallet_fenced".to_owned(),
                    recorded_at_unix: self.admission_now().unix_timestamp(),
                },
                &rollback,
                None,
                decision_evidence.as_ref(),
            );
            return;
        }

        // One consistent watchlist snapshot for this event (ArcSwap hot path): every
        // watchlist lookup below reads the same generation.
        let watchlist = self.live_watchlist.snapshot();

        // Look up wallet in watchlist; skip non-watchlisted wallets.
        let quality = self.quality_for(&watchlist, &trade.wallet);

        let (rb, leader_row, signal) = if let Some(continuation) = pending.as_ref() {
            let key = MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id);
            (
                RollbackCtx {
                    wallet: trade.wallet,
                    key,
                    prev: None,
                    enabled: false,
                },
                self.leader_position_row(&trade),
                LeaderSignal {
                    leader: TraderId(trade.wallet),
                    venue: VenueId::polymarket(),
                    market_id: trade.market_id.clone(),
                    outcome_id: trade.outcome_id,
                    action: continuation.facts.pre_bucket_action,
                    leader_side: trade.side,
                    leader_price: trade.price,
                    leader_size: trade.contracts,
                    observed_at: trade.observed_at,
                    received_at: trade.received_at,
                    reconstruction_quality: continuation.facts.reconstruction_quality,
                    source_trade_id: trade.source_trade_id.clone(),
                    action_confidence_ppm: continuation.facts.action_confidence_ppm,
                },
            )
        } else {
            // Capture pre-trade state, then advance the legacy v1 ledger exactly once.
            let position = self.bucket_engine.ledger().position(&trade.wallet).cloned();
            let key = MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id);
            let rb = RollbackCtx {
                wallet: trade.wallet,
                key: key.clone(),
                prev: position
                    .as_ref()
                    .and_then(|snapshot| snapshot.positions.get(&key))
                    .map(|state| (state.long_contracts, state.short_contracts)),
                enabled: true,
            };
            if let Err(error) = self.bucket_engine.ledger_mut().ingest(&trade) {
                error!(%error, trade = %trade.source_trade_id, "leader ledger rejected trade");
                return;
            }
            let leader_row = self.leader_position_row(&trade);
            // Preserve version-one admission ordering: its stale bookkeeping
            // disposition predates classification/gate and remains non-consuming.
            if let Some(disposition) = self.stale_no_copy(&trade, self.admission_now()) {
                self.commit_no_copy_or_rollback(
                    &trade,
                    &leader_row,
                    &disposition,
                    &rb,
                    None,
                    decision_evidence.as_ref(),
                );
                return;
            }
            let Some(signal) = classify_trade(
                &trade,
                position.as_ref(),
                &watchlist,
                quality,
                VenueId::polymarket(),
                &self.signal_config,
            ) else {
                self.no_fill_or_rollback(
                    &trade,
                    &leader_row,
                    None,
                    "classification_rejected",
                    &rb,
                    None,
                    decision_evidence.as_ref(),
                )
                .await;
                return;
            };
            if let Some(reason) = self.bucket_engine.entry_gate().admit(&signal) {
                info!(
                    reason = %reason,
                    market = %signal.market_id,
                    leader_price = %signal.leader_price.0,
                    "signal did not produce order",
                );
                self.no_fill_or_rollback(
                    &trade,
                    &leader_row,
                    None,
                    "entry_gate_rejected",
                    &rb,
                    None,
                    decision_evidence.as_ref(),
                )
                .await;
                return;
            }
            // Legacy v1 remains an in-memory projection; v2 history was already
            // consumed atomically by the bucket transaction.
            self.bucket_engine
                .entry_gate_mut()
                .record_entry(signal.leader.0, &signal.market_id);
            (rb, leader_row, signal)
        };

        // Staleness is intentionally post-pending: it is a continuation input and
        // cannot cause the ledger/gate/history portion to be reapplied on restart.
        if pending.is_some() {
            let stale_at = self.admission_now();
            record_clock(&mut decision_evidence, "initial_staleness_gate", stale_at);
            if let Some(disposition) = self.stale_no_copy(&trade, stale_at) {
                self.commit_no_copy_or_rollback(
                    &trade,
                    &leader_row,
                    &disposition,
                    &rb,
                    None,
                    decision_evidence.as_ref(),
                );
                return;
            }
        }

        // Classification and durable no-copy evidence remain available during rehearsal, but no
        // financial decision may be composed or mutated before the verified Start owns its exact
        // admission, risk, authority, and configuration evidence.
        if self.qualification_start.is_none() {
            self.no_fill_or_rollback(
                &trade,
                &leader_row,
                None,
                "financial_era_not_started",
                &rb,
                Some(&signal.market_id),
                decision_evidence.as_ref(),
            )
            .await;
            return;
        }

        // One admission read owns market mapping, venue rules, fees, and scheduled end for both
        // paper and live. Its three raw responses are already synchronized in the source log.
        let condition_id = pe_core_types::PolymarketConditionId(signal.market_id.to_string());
        #[cfg(feature = "scenario")]
        let scenario_admission = self
            .scenario_hooks
            .as_ref()
            .and_then(|hooks| hooks.admission_artifacts.lock().ok()?.pop_front());
        #[cfg(not(feature = "scenario"))]
        let scenario_admission = None;
        let admission = if let Some(admission) = scenario_admission {
            admission
        } else {
            match &self.admission_builder {
                Some(builder) => match builder
                    .build(&condition_id, OffsetDateTime::now_utc())
                    .await
                {
                    Ok(admission) => admission,
                    Err(error) => {
                        info!(%error, market = %signal.market_id, "market admission failed closed");
                        self.no_fill_or_rollback(
                            &trade,
                            &leader_row,
                            None,
                            "market_admission_unavailable",
                            &rb,
                            Some(&signal.market_id),
                            decision_evidence.as_ref(),
                        )
                        .await;
                        return;
                    }
                },
                None => {
                    error!(market = %signal.market_id, "active financial era has no admission builder");
                    self.intake_stopped = true;
                    return;
                }
            }
        };

        // Resolution-horizon gate uses the scheduled end already carried by admission; no second
        // Gamma request or dashboard cache participates in economics.
        if self.max_resolution_horizon_secs > 0 || self.min_resolution_horizon_secs > 0 {
            let resolution_unix = admission.market.scheduled_end_unix;
            if let Some(evidence) = decision_evidence.as_mut() {
                evidence.record_market_end(MarketEndEvidence {
                    market_id: signal.market_id.to_string(),
                    resolution_unix,
                    source: "polymarket.clob.market".to_owned(),
                });
            }
            let horizon_at = OffsetDateTime::now_utc();
            record_clock(
                &mut decision_evidence,
                "resolution_horizon_gate",
                horizon_at,
            );
            let now_unix = horizon_at.unix_timestamp();
            if let Some(reason) = check_resolution_horizon(
                resolution_unix,
                now_unix,
                self.max_resolution_horizon_secs,
                self.min_resolution_horizon_secs,
            ) {
                info!(
                    reason,
                    market = %signal.market_id,
                    resolution_unix = ?resolution_unix,
                    "signal did not produce order",
                );
                self.no_fill_or_rollback(
                    &trade,
                    &leader_row,
                    None,
                    reason,
                    &rb,
                    Some(&signal.market_id),
                    decision_evidence.as_ref(),
                )
                .await;
                return;
            }
        }

        // Market-liveness + observability (the Gamma mid). Historically (#339) this mid
        // was ALSO the sizing/gating basis, but it is a 60 s-TTL per-outcome MARK price
        // that can diverge sharply from the leader's just-executed trade price (observed
        // ~2× on thin near-resolution markets), so keying sizing/gating off it produced
        // uncontrolled notional and let fills slip outside the band. We keep fetching it
        // only to (a) confirm the market is open/priced and (b) log the divergence for
        // diagnosis; sizing and gating below use `fill_basis` instead. Fail closed on an
        // absent mid (unchanged liveness behaviour).
        let _market_mid = {
            let mids = self
                .mid_price_cache
                .fetch_mids(std::slice::from_ref(&signal.market_id))
                .await;
            let px = mids
                .get(&signal.market_id)
                .and_then(|prices| prices.get(usize::from(signal.outcome_id.0)).copied());
            match px.and_then(|d| Price::new(d).ok()) {
                Some(p) => {
                    if let Some(evidence) = decision_evidence.as_mut() {
                        evidence.record_market_price(MarketPriceEvidence {
                            market_id: signal.market_id.to_string(),
                            outcome_id: signal.outcome_id.0,
                            mid_price: Some(p.0.normalize().to_string()),
                            source: "gamma.outcome_prices".to_owned(),
                        });
                    }
                    p
                }
                None => {
                    if let Some(evidence) = decision_evidence.as_mut() {
                        evidence.record_market_price(MarketPriceEvidence {
                            market_id: signal.market_id.to_string(),
                            outcome_id: signal.outcome_id.0,
                            mid_price: None,
                            source: "gamma.outcome_prices_unavailable".to_owned(),
                        });
                    }
                    info!(
                        reason = "current market price unavailable",
                        market = %signal.market_id,
                        outcome = signal.outcome_id.0,
                        "signal did not produce order",
                    );
                    self.no_fill_or_rollback(
                        &trade,
                        &leader_row,
                        None,
                        "current_market_price_unavailable",
                        &rb,
                        Some(&signal.market_id),
                        decision_evidence.as_ref(),
                    )
                    .await;
                    return;
                }
            }
        };

        // Mandatory price-impact gate (#508 Phase A, #544): one `/book` fetch produces the
        // executable-ladder plan that feeds the band gate, sizing plan, and VWAP fill basis.
        // An unusable book — missing token, fetch error/timeout, corrupt/empty/stale — fails
        // closed, as does an in-band ladder that affords no atomic share.
        // A resumed continuation evaluates under its frozen basis; only a fresh
        // decision reads the live watchlist and bankroll (#544 review round 3).
        // Selected BEFORE the impact gate so ladder budget, VWAP, and strategy
        // sizing all share one basis.
        let p = pending
            .as_ref()
            .map(|continuation| continuation.facts.frozen_basis.win_rate_p)
            .unwrap_or_else(|| self.win_rate_p_for(&watchlist, &signal.leader));
        let sizing_bankroll = match pending.as_ref() {
            Some(continuation) => continuation.facts.frozen_basis.bankroll,
            None => match self
                .paper_state
                .financial_snapshot(OffsetDateTime::now_utc().unix_timestamp())
            {
                Ok(snapshot) => snapshot.cash,
                Err(error) => {
                    error!(%error, trade = %trade.source_trade_id, "read active sizing bankroll failed");
                    self.intake_stopped = true;
                    return;
                }
            },
        };
        let gate_evidence = match self
            .plan_impact_gate(&signal, p, sizing_bankroll, &admission)
            .await
        {
            Ok(outcome) => outcome,
            Err(failure) => {
                if let Some(evidence) = decision_evidence.as_mut() {
                    evidence.record_book(*failure.book);
                    if let Some(checked_at) = failure.checked_at_unix_ms {
                        evidence.record_clock(
                            "book_staleness_check",
                            i64::try_from(checked_at).unwrap_or(i64::MAX),
                        );
                    }
                }
                // Shared-gate rejection (#508 Decision 10): an UNUSABLE admission quote
                // suppresses every destination, pre-staging — no aggregate.
                info!(
                    reason = failure.reason,
                    market = %signal.market_id,
                    outcome = signal.outcome_id.0,
                    "signal did not produce order",
                );
                self.no_fill_or_rollback(
                    &trade,
                    &leader_row,
                    None,
                    failure.reason,
                    &rb,
                    Some(&signal.market_id),
                    decision_evidence.as_ref(),
                )
                .await;
                return;
            }
        };
        if let Some(evidence) = decision_evidence.as_mut() {
            evidence.record_book(gate_evidence.book.clone());
            evidence.record_clock(
                "book_staleness_check",
                i64::try_from(gate_evidence.checked_at_unix_ms).unwrap_or(i64::MAX),
            );
        }
        let book_receipt = gate_evidence.book_receipt;
        let plan_budget = gate_evidence.budget;
        let planned_worst_case_all_in_debit = gate_evidence.worst_case_all_in_debit;
        let gate = gate_evidence.gate;
        let gate_plan: Option<&LadderPlan> = match &gate {
            GatePlan::Planned(plan) => Some(plan),
            _ => None,
        };

        // Paper fills at the expected VWAP derived by the one collateral ladder. The same plan
        // supplies sizing, band checks, Prepared economics, and the local exact quantity.
        let clob_basis_applies =
            self.mode == ExecutionMode::Paper && signal.leader_side == Side::Buy;
        let planned_vwap_basis =
            gate_plan.and_then(|plan| clob_basis_applies.then(|| plan.vwap()).flatten());
        // Zero-absorb reads still anchor the SHARED band gate on the successful best ask
        // (#508 Decision 10 — the paper-only skip happens after staging, below).
        let zero_absorb_basis = match &gate {
            GatePlan::NothingAffordable { best_ask } if clob_basis_applies => Some(*best_ask),
            _ => None,
        };
        let fill_basis = if let Some(vwap) = planned_vwap_basis {
            vwap
        } else if let Some(best_ask) = zero_absorb_basis {
            best_ask
        } else {
            if clob_basis_applies {
                info!(
                    reason = "price-impact book supplied no usable fill basis (fail closed)",
                    market = %signal.market_id,
                    "signal did not produce order",
                );
                self.no_fill_or_rollback(
                    &trade,
                    &leader_row,
                    None,
                    "",
                    &rb,
                    Some(&signal.market_id),
                    decision_evidence.as_ref(),
                )
                .await;
                return;
            }
            signal.leader_price
        };

        // max_fill_price safety rail (#142 parity): skip BUYs whose FILL price is at or
        // above the cap (catastrophic payoff geometry near $1). ZERO disables. Gated on
        // `fill_basis` (the price paid), matching the backtest's fill-price cap.
        if signal.leader_side == Side::Buy
            && self.max_fill_price > Decimal::ZERO
            && fill_basis.0 >= self.max_fill_price
        {
            info!(
                reason = "fill price at or above max_fill_price",
                market = %signal.market_id,
                fill_price = %fill_basis.0,
                leader_price = %signal.leader_price.0,
                max_fill_price = %self.max_fill_price,
                "signal did not produce order",
            );
            self.no_fill_or_rollback(
                &trade,
                &leader_row,
                None,
                "fill_price_at_or_above_max",
                &rb,
                Some(&signal.market_id),
                decision_evidence.as_ref(),
            )
            .await;
            return;
        }

        // min_fill_price band floor (run28 cutover, #468 selection↔deployment parity):
        // skip BUYs whose FILL price is below the entry-band lower bound. Strictly `<` so
        // the boundary value fills, mirroring the backtest `min_signal_price` floor (also
        // gated on the fill price). ZERO disables.
        if signal.leader_side == Side::Buy
            && self.min_fill_price > Decimal::ZERO
            && fill_basis.0 < self.min_fill_price
        {
            info!(
                reason = "fill price below min_fill_price",
                market = %signal.market_id,
                fill_price = %fill_basis.0,
                leader_price = %signal.leader_price.0,
                min_fill_price = %self.min_fill_price,
                "signal did not produce order",
            );
            self.no_fill_or_rollback(
                &trade,
                &leader_row,
                None,
                "fill_price_below_min",
                &rb,
                Some(&signal.market_id),
                decision_evidence.as_ref(),
            )
            .await;
            return;
        }

        // ── Shared gates end here. Stage the dispatch aggregate (#508 Decision 10) ──
        // Every rejection ABOVE suppressed all destinations pre-staging (no aggregate).
        // Every decision BELOW is paper-only and must never suppress live targets.
        // Resolve and verify the V3 observation ONCE through exact indexed receipt reads before
        // taking the final age sample. Both live staging and paper composition consume this same
        // value; neither may replay the growing source log or introduce unchecked latency after
        // the sample.
        let Some(continuation) = pending.as_ref() else {
            error!(trade = %trade.source_trade_id, "dispatch has no durable decision continuation");
            self.rollback_admission(&rb, Some(&signal.market_id));
            return;
        };
        let Some(source_receipts) = self.source_receipts.as_ref() else {
            error!(trade = %trade.source_trade_id, "dispatch has no verified source-receipt index");
            self.rollback_admission(&rb, Some(&signal.market_id));
            return;
        };
        let observation = match continuation.observation_from_receipt_index(source_receipts) {
            Ok(Some(observation)) => observation,
            Ok(None) => {
                error!(trade = %trade.source_trade_id, "dispatch continuation has no version-three observation");
                self.rollback_admission(&rb, Some(&signal.market_id));
                return;
            }
            Err(error) => {
                error!(%error, trade = %trade.source_trade_id, "dispatch observation verification failed");
                self.rollback_admission(&rb, Some(&signal.market_id));
                return;
            }
        };
        #[cfg(feature = "scenario")]
        self.apply_observation_resolution_clock_advance();
        // #546: re-check the copy budget with a FRESH time sample immediately before
        // staging. Channel and awaited-gate delay (book fetch, resolution lookup, the
        // admission hold) must not turn an observation that was fresh at the early gate
        // into a stale fill or dispatch. The recorded same-session entry is deliberately
        // kept: this leader did enter the market.
        let dispatch_stale_at = self.admission_now();
        record_clock(
            &mut decision_evidence,
            "pre_dispatch_staleness_gate",
            dispatch_stale_at,
        );
        if let Some(disposition) = self.stale_no_copy(&trade, dispatch_stale_at) {
            self.commit_no_copy_or_rollback(
                &trade,
                &leader_row,
                &disposition,
                &rb,
                Some(&signal.market_id),
                decision_evidence.as_ref(),
            );
            return;
        }

        let dispatch_id =
            match self.stage_dispatch_if_targeted(&signal, &observation, &mut decision_evidence) {
                Ok(id) => id,
                Err(()) => {
                    // Staging failed: abandoned unseen. #511: exact in-memory rollback so
                    // the held-cursor redelivery re-admits byte-identically.
                    self.rollback_admission(&rb, Some(&signal.market_id));
                    return;
                }
            };

        // Relocated hold/already-filled gate (#508; historically pre-first-BUY): a paper
        // position we already hold skips the PAPER order only — live targets in the staged
        // aggregate still execute against their own venue state.
        let pos_key = MarketOutcomeId::new(signal.market_id.clone(), signal.outcome_id);
        if self.filled_positions.contains(&pos_key) {
            info!(
                reason = "already hold position in this market outcome (paper-only)",
                market = %signal.market_id,
                outcome = signal.outcome_id.0,
                "signal did not produce order",
            );
            self.no_fill_or_rollback(
                &trade,
                &leader_row,
                dispatch_id.as_deref(),
                "paper_held",
                &rb,
                Some(&signal.market_id),
                decision_evidence.as_ref(),
            )
            .await;
            return;
        }

        // Paper-only zero-absorb skip (#508 Decision 10): the successful admission quote
        // could not absorb an atomic share for the paper budget.
        if matches!(&gate, GatePlan::NothingAffordable { .. }) {
            info!(
                reason = "paper budget afforded no atomic share within the impact band (paper-only)",
                market = %signal.market_id,
                outcome = signal.outcome_id.0,
                "signal did not produce order",
            );
            self.no_fill_or_rollback(
                &trade,
                &leader_row,
                dispatch_id.as_deref(),
                "impact_absorbs_zero",
                &rb,
                Some(&signal.market_id),
                decision_evidence.as_ref(),
            )
            .await;
            return;
        }

        let Some(plan) = gate_plan else {
            self.intake_stopped = true;
            return;
        };
        let Some(book_receipt) = book_receipt else {
            error!(trade = %trade.source_trade_id, "active fill book is not source-log bound");
            self.intake_stopped = true;
            return;
        };
        let per_trade_cap_bps = self
            .strategy
            .config()
            .per_trade_cap
            .resolve_bps(TradingMode::LiveTiny);
        let (risk, _) = match self
            .active_paper_risk_snapshot(&signal, planned_worst_case_all_in_debit, per_trade_cap_bps)
            .await
        {
            Ok(risk) => risk,
            Err(failure) => {
                let decline = pe_strategy_winner_follow::WinnerFollowError::RiskInputsUnavailable;
                let reason = format!("{decline}: {}", failure.cause);
                info!(reason = %reason, "signal did not produce order");
                self.decline_or_rollback(
                    &trade,
                    &leader_row,
                    dispatch_id.as_deref(),
                    &format!("paper_reject:{reason}"),
                    &decline,
                    WinnerFollowDecisionInputs::RiskInputsUnavailable {
                        cause: failure.cause,
                        evidence: failure.evidence,
                    },
                    &rb,
                    Some(&signal.market_id),
                    decision_evidence.as_ref(),
                )
                .await;
                return;
            }
        };
        let Some(applied_runtime) = applied_runtime.as_ref() else {
            error!(trade = %trade.source_trade_id, "active fill has no runtime configuration evidence");
            self.intake_stopped = true;
            return;
        };
        let applied_configuration_hash = applied_runtime.canonical_hash();
        let economic = match self.compose_active_paper_economic(
            &signal,
            &admission,
            plan,
            book_receipt,
            &observation,
            p,
            plan_budget,
            risk,
            applied_configuration_hash,
        ) {
            Ok(economic) => economic,
            Err(error) => {
                error!(%error, trade = %trade.source_trade_id, "compose active paper economics failed");
                self.intake_stopped = true;
                return;
            }
        };
        match self.strategy.evaluate_at_price(
            &signal,
            economic.sizing.all_in_price,
            p,
            economic.risk.snapshot.clone(),
            sizing_bankroll,
            self.mode,
        ) {
            Err(e) => {
                info!(reason = %e, "signal did not produce order");
                let reason = format!("paper_reject:{e}");
                self.decline_or_rollback(
                    &trade,
                    &leader_row,
                    dispatch_id.as_deref(),
                    &reason,
                    &e,
                    WinnerFollowDecisionInputs::Evaluated {
                        economic: Box::new(economic),
                    },
                    &rb,
                    Some(&signal.market_id),
                    decision_evidence.as_ref(),
                )
                .await;
            }
            Ok(intent) => {
                let execution_at = OffsetDateTime::now_utc();
                record_clock(&mut decision_evidence, "paper_dispatch", execution_at);
                if let Some(evidence) = decision_evidence.as_ref() {
                    let checkpoint = match evidence.checkpoint_json() {
                        Ok(checkpoint) => checkpoint,
                        Err(error) => {
                            error!(%error, trade = %trade.source_trade_id,
                                "encode pre-dispatch decision evidence failed; stopping producer intake");
                            self.intake_stopped = true;
                            return;
                        }
                    };
                    if let Err(error) = self.paper_state.checkpoint_decision_pending(
                        &trade.source_trade_id,
                        &checkpoint,
                        execution_at.unix_timestamp(),
                    ) {
                        error!(%error, trade = %trade.source_trade_id,
                            "persist pre-dispatch decision evidence failed; stopping producer intake");
                        self.intake_stopped = true;
                        return;
                    }
                }
                match self
                    .apply_active_financial_fill(&trade, economic, dispatch_id.as_deref())
                    .await
                {
                    Ok(final_receipt) => {
                        self.filled_positions.insert(MarketOutcomeId::new(
                            signal.market_id.clone(),
                            signal.outcome_id,
                        ));
                        enqueue_if_buy(
                            self.snapshot_sink.as_ref(),
                            intent.side,
                            &intent.idempotency_key,
                            &intent.market_id,
                            intent.outcome_id,
                            OffsetDateTime::now_utc().unix_timestamp(),
                        );
                        info!(
                            kind = "paper_financial_final",
                            final_sequence = final_receipt.sequence.0,
                            market = %intent.market_id,
                            "paper fill committed"
                        );
                    }
                    Err(error) => {
                        error!(%error, trade = %trade.source_trade_id, "active paper financial transition is uncertain");
                        self.intake_stopped = true;
                    }
                }
            }
        }
    }

    /// #530/#546 copy-budget rule (websocket-primary mode only): the ranker's latency shift
    /// assumes copies happen at websocket speed, so an observation from EITHER source older
    /// than the calibrated budget is admitted for bookkeeping — seen-state, leader ledger,
    /// and a typed disposition in ONE transaction, so the held cursor (#511) advances — but
    /// stages no copy. Copying it late is the padded-watchlist loss class. Strict
    /// full-`Duration` compare (#530 review F6): 2.5s old with a 2s budget IS stale;
    /// whole-second truncation would admit up to budget+1s. `now` is sampled ONCE per
    /// decision and stamps both `age_secs` and `recorded_at_unix`.
    fn stale_no_copy(
        &self,
        trade: &IncomingTrade,
        now: OffsetDateTime,
    ) -> Option<pe_paper_state::NoCopyDisposition> {
        if !self.activity_ws_enabled {
            return None;
        }
        let age = now - trade.observed_at;
        // The budget is bounds-checked at config load; try_from is belt-and-suspenders.
        let budget = time::Duration::seconds(
            i64::try_from(self.copy_latency_budget_secs).unwrap_or(i64::MAX),
        );
        if age <= budget {
            return None;
        }
        let (provenance, reason) = match trade.provenance {
            TradeProvenance::RestPoll => ("rest_poll", "stale_fallback_past_copy_budget"),
            TradeProvenance::ActivityWs => ("activity_ws", "stale_activity_ws_past_copy_budget"),
        };
        Some(pe_paper_state::NoCopyDisposition {
            provenance: provenance.to_string(),
            age_secs: age.whole_seconds(),
            reason: reason.to_string(),
            recorded_at_unix: now.unix_timestamp(),
        })
    }

    /// Commit a typed no-copy admission (seen + leader + disposition, atomically). On failure
    /// the trade is abandoned unseen and the in-memory admission effects are rolled back, so
    /// the next reader copy or redelivery re-runs a byte-identical admission.
    fn commit_no_copy_or_rollback(
        &mut self,
        trade: &IncomingTrade,
        leader: &LeaderPositionRow,
        disposition: &pe_paper_state::NoCopyDisposition,
        rb: &RollbackCtx,
        unrecord_market: Option<&MarketId>,
        evidence: Option<&DecisionEvidenceAccumulator>,
    ) {
        info!(
            trade = %trade.source_trade_id,
            provenance = %disposition.provenance,
            age_secs = disposition.age_secs,
            budget_secs = self.copy_latency_budget_secs,
            reason = %disposition.reason,
            "stale observation: admitted with no-copy disposition"
        );
        #[cfg(feature = "scenario")]
        if self.take_scenario_fault(|h| &h.fail_next_no_copy_commit) {
            error!(trade = %trade.source_trade_id,
                "scenario fault: no-copy disposition commit failed; rolling back admission");
            self.rollback_admission(rb, unrecord_market);
            return;
        }
        let pending = match render_pending_evidence(
            evidence,
            AuthorityEvidence::not_read("terminal_before_fill_authority"),
            TerminalDispositionEvidence::no_copy(&disposition.reason),
        ) {
            Ok(value) => value,
            Err(error) => {
                error!(%error, trade = %trade.source_trade_id, "encode no-copy decision evidence failed");
                self.rollback_admission(rb, unrecord_market);
                return;
            }
        };
        if let Err(e) = self.paper_state.commit_seen_no_copy_with_pending(
            &trade.source_trade_id,
            leader,
            disposition,
            pending.as_ref().map(pending_terminal),
        ) {
            error!(error = %e, trade = %trade.source_trade_id,
                "no-copy disposition commit failed; rolling back admission");
            self.rollback_admission(rb, unrecord_market);
        }
    }

    /// Roll back the in-memory admission effects of an abandoned-unseen trade (#511
    /// pre-frame failure): restore the leader ledger to the captured pre-trade state and
    /// un-record the tentative same-session entry. Redelivery then re-runs a
    /// byte-identical admission.
    fn rollback_admission(&mut self, rb: &RollbackCtx, unrecord_market: Option<&MarketId>) {
        if !rb.enabled {
            return;
        }
        self.bucket_engine
            .ledger_mut()
            .restore(rb.wallet, &rb.key, rb.prev);
        if unrecord_market.is_some() {
            match self.paper_state.gate_history() {
                Ok(history) => {
                    *self.bucket_engine.entry_gate_mut() =
                        crate::entry_gate::CopyEntryGate::new(CopyEntryGateConfig, history);
                }
                Err(error) => {
                    error!(%error, "rebuild durable entry-gate projection after rollback failed");
                    self.intake_stopped = true;
                }
            }
        }
    }

    /// Terminal no-fill that STAYS terminal only if the seen-commit lands (#511): on
    /// commit failure the trade is abandoned unseen, so the in-memory admission effects
    /// are rolled back — the held cursor redelivers into a fresh identical admission.
    #[allow(clippy::too_many_arguments)]
    async fn no_fill_or_rollback(
        &mut self,
        trade: &IncomingTrade,
        leader: &LeaderPositionRow,
        dispatch_id: Option<&str>,
        no_fill_reason: &str,
        rb: &RollbackCtx,
        unrecord_market: Option<&MarketId>,
        evidence: Option<&DecisionEvidenceAccumulator>,
    ) {
        if self
            .commit_no_fill_flipping(trade, leader, dispatch_id, no_fill_reason, None, evidence)
            .await
        {
            return;
        }
        self.rollback_admission(rb, unrecord_market);
    }

    /// Persist the actual shared-strategy error together with the exact inputs that produced it.
    /// Risk-input acquisition failure is represented explicitly because strategy evaluation cannot
    /// run without a complete snapshot.
    #[allow(clippy::too_many_arguments)]
    async fn decline_or_rollback(
        &mut self,
        trade: &IncomingTrade,
        leader: &LeaderPositionRow,
        dispatch_id: Option<&str>,
        no_fill_reason: &str,
        error: &pe_strategy_winner_follow::WinnerFollowError,
        inputs: WinnerFollowDecisionInputs,
        rb: &RollbackCtx,
        unrecord_market: Option<&MarketId>,
        evidence: Option<&DecisionEvidenceAccumulator>,
    ) {
        let terminal = TerminalDispositionEvidence::declined(error, inputs);
        if self
            .commit_no_fill_flipping(
                trade,
                leader,
                dispatch_id,
                no_fill_reason,
                Some(terminal),
                evidence,
            )
            .await
        {
            return;
        }
        self.rollback_admission(rb, unrecord_market);
    }

    /// Build the leader-position mirror row for the `(market, outcome)` this trade
    /// touched, read from the in-memory ledger *after* the trade was ingested.
    fn leader_position_row(&self, trade: &IncomingTrade) -> LeaderPositionRow {
        let key = MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id);
        let (long, short) = self
            .bucket_engine
            .ledger()
            .position(&trade.wallet)
            .and_then(|snap| snap.positions.get(&key))
            .map(|st| (st.long_contracts, st.short_contracts))
            .unwrap_or((ShareAmount::ZERO, ShareAmount::ZERO));
        LeaderPositionRow {
            wallet: trade.wallet,
            market_id: trade.market_id.clone(),
            outcome_id: trade.outcome_id,
            long_contracts: long,
            short_contracts: short,
        }
    }

    /// [`Self::commit_no_fill`] that also flips a staged dispatch aggregate with a TYPED
    /// no-fill outcome (#508 Decision 10) in the same transaction. Every failed local
    /// finalization surfaces and retries in-process (bounded) — never a silent
    /// log-and-continue (round-4); restart recovery remains the durable backstop.
    async fn commit_no_fill_flipping(
        &self,
        trade: &IncomingTrade,
        leader: &LeaderPositionRow,
        dispatch_id: Option<&str>,
        no_fill_reason: &str,
        terminal: Option<TerminalDispositionEvidence>,
        evidence: Option<&DecisionEvidenceAccumulator>,
    ) -> bool {
        let durable_reason = if no_fill_reason.is_empty() {
            "no_order"
        } else {
            no_fill_reason
        };
        let terminal =
            terminal.unwrap_or_else(|| TerminalDispositionEvidence::no_fill(durable_reason));
        let pending = match render_pending_evidence(
            evidence,
            AuthorityEvidence::not_read("terminal_before_fill_authority"),
            terminal,
        ) {
            Ok(value) => value,
            Err(error) => {
                error!(%error, trade = %trade.source_trade_id, "encode no-fill decision evidence failed");
                return false;
            }
        };
        let outcome = format!("no_fill:{no_fill_reason}");
        let flip = dispatch_id.map(|id| pe_paper_state::DispatchFlip {
            dispatch_id: id,
            paper_outcome: &outcome,
        });
        for attempt in 1..=LOCAL_COMMIT_RETRIES {
            match self.paper_state.commit_seen_no_fill_with_flip_pending(
                &trade.source_trade_id,
                leader,
                flip,
                pending.as_ref().map(pending_terminal),
            ) {
                Ok(()) => return true,
                Err(e) if attempt < LOCAL_COMMIT_RETRIES => {
                    error!(
                        error = %e,
                        attempt,
                        "paper-state no-fill commit failed; retrying in-process"
                    );
                    tokio::time::sleep(Duration::from_millis(LOCAL_COMMIT_RETRY_DELAY_MS)).await;
                }
                Err(e) => {
                    error!(
                        error = %e,
                        dispatch_id = ?dispatch_id,
                        "paper-state no-fill commit failed after in-process retries; the \
                         trade is abandoned UNSEEN (#511: the caller rolls back the \
                         in-memory admission and the held cursor redelivers it)"
                    );
                }
            }
        }
        false
    }

    fn quality_for(&self, watchlist: &Watchlist, wallet: &WalletAddress) -> ReconstructionQuality {
        watchlist
            .entries
            .iter()
            .find(|e| &e.wallet == wallet)
            .map(|e| e.reconstruction_quality)
            .unwrap_or(self.min_quality)
    }

    /// Empirical win-rate probability for a leader, sourced from the watchlist's
    /// `win_rate_bps` (wins / closed_trades × 10 000). Falls back to `Probability::ZERO`
    /// if the leader is not in the watchlist (signal will produce no edge → NoEdge error).
    /// Capture the mutable decision basis for one bucket's leader (#544).
    fn freeze_decision_basis(
        &self,
        aggregates: &[pe_source_polymarket_public::ActivityAggregate],
    ) -> crate::bucket_commit::FrozenDecisionBasis {
        let watchlist = self.live_watchlist.snapshot();
        let leader = aggregates
            .first()
            .map(|aggregate| TraderId(aggregate.group_id.components().wallet));
        crate::bucket_commit::FrozenDecisionBasis {
            win_rate_p: leader
                .map(|leader| self.win_rate_p_for(&watchlist, &leader))
                .unwrap_or(Probability::ZERO),
            bankroll: self.bankroll,
        }
    }

    fn win_rate_p_for(&self, watchlist: &Watchlist, leader: &TraderId) -> Probability {
        let bps = watchlist
            .entries
            .iter()
            .find(|e| e.wallet == leader.0)
            .map(|e| e.win_rate_bps.0)
            .unwrap_or(0)
            .clamp(0, 10_000);
        let p_raw = Decimal::from(bps) / Decimal::from(10_000i32);
        // Infallible after clamping to [0, 10_000]: p_raw is in [0, 1].
        Probability::new(p_raw).unwrap_or(Probability::ZERO)
    }
}

/// Resolution-horizon gate decision (#290, #339). Returns `Some(reason)` to reject the
/// copy, `None` to allow it.
///
/// `max_secs` rejects markets resolving more than that many seconds out (0 disables the
/// upper bound); `min_secs` rejects markets resolving sooner than that (0 disables the
/// lower bound). An unknown resolution time (`None`) always fails closed — the horizon
/// cannot be confirmed, so the copy is skipped.
fn check_resolution_horizon(
    resolution_unix: Option<i64>,
    now_unix: i64,
    max_secs: u64,
    min_secs: u64,
) -> Option<&'static str> {
    let Some(unix) = resolution_unix else {
        return Some("market resolution time unknown");
    };
    let secs_until = unix - now_unix;
    let max = i64::try_from(max_secs).unwrap_or(i64::MAX);
    let min = i64::try_from(min_secs).unwrap_or(i64::MAX);
    if max_secs > 0 && secs_until > max {
        return Some("market resolves too far out");
    }
    if min_secs > 0 && secs_until < min {
        return Some("market resolves too soon");
    }
    None
}

// ── Stub risk snapshot ────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashSet;

    use pe_core_types::{AccountId, BasisPoints};
    use pe_risk_engine::{
        ConcentrationCaps, RiskBlock, RiskDecision, RiskHaltCause, RiskSnapshot, evaluate_risk,
    };

    use crate::paper_recovery::{RiskHaltOwner, SealReason};
    use crate::qualification::QualificationCompletion;

    use super::{Orchestrator, check_resolution_horizon};
    use crate::risk_inputs::apply_global_risk_halts;

    const NOW: i64 = 1_700_000_000;

    fn healthy_risk_snapshot() -> RiskSnapshot {
        RiskSnapshot {
            leader_exposure_bps: BasisPoints(0),
            market_exposure_bps: BasisPoints(0),
            family_exposure_bps: BasisPoints(0),
            total_copy_exposure_bps: BasisPoints(0),
            intraday_pnl_bps: BasisPoints(0),
            rolling_7d_pnl_bps: BasisPoints(0),
            absolute_pnl_bps: BasisPoints(0),
            copy_latency_kill_switch_active: false,
            proposed_trade_bps: BasisPoints(1),
            per_trade_cap_bps: 25,
            concentration_caps: Some(ConcentrationCaps::CANONICAL),
        }
    }

    /// PASS: a halt owned by a live account blocks the paper snapshot through the global set.
    /// FAIL: paper evaluates only its locally derived risk state and approves the entry.
    #[test]
    fn live_owner_halt_gates_paper_risk() {
        let account = AccountId::new("live-a").unwrap();
        let active = HashSet::from([(
            RiskHaltOwner::LiveAccount(account),
            RiskHaltCause::AbsoluteLoss,
        )]);
        let mut snapshot = healthy_risk_snapshot();

        apply_global_risk_halts(&active, &mut snapshot);

        assert_eq!(
            evaluate_risk(&snapshot),
            RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown)
        );
    }

    /// PASS: the first valid boundary that reaches both canonical completion counts requests the
    /// existing Complete seal; an invalid mark or either below-threshold count requests no seal.
    /// FAIL: the boundary can seal early, seal invalid evidence, or misses the first valid mark.
    #[test]
    fn daily_boundary_requests_automatic_complete_seal_at_exact_threshold() {
        type TestOrchestrator = Orchestrator<
            pe_source_polymarket_public::FixtureFetcher,
            crate::clob_book::FixtureClobBookFetcher,
        >;
        let exact = QualificationCompletion {
            complete_days: 30,
            causal_closes: 90,
        };

        assert_eq!(
            TestOrchestrator::automatic_completion_seal_reason(true, Some(exact)),
            Some(SealReason::Complete)
        );
        assert_eq!(
            TestOrchestrator::automatic_completion_seal_reason(false, Some(exact)),
            None
        );
        assert_eq!(
            TestOrchestrator::automatic_completion_seal_reason(
                true,
                Some(QualificationCompletion {
                    complete_days: 29,
                    causal_closes: 90,
                }),
            ),
            None
        );
        assert_eq!(
            TestOrchestrator::automatic_completion_seal_reason(
                true,
                Some(QualificationCompletion {
                    complete_days: 30,
                    causal_closes: 89,
                }),
            ),
            None
        );
    }

    #[test]
    fn unknown_resolution_fails_closed() {
        // Both bounds active: a missing resolution time is always rejected.
        assert_eq!(
            check_resolution_horizon(None, NOW, 259_200, 60),
            Some("market resolution time unknown")
        );
    }

    #[test]
    fn rejects_too_far_out() {
        // Resolves 72h + 1s out, max = 72h → rejected.
        let unix = NOW + 259_200 + 1;
        assert_eq!(
            check_resolution_horizon(Some(unix), NOW, 259_200, 60),
            Some("market resolves too far out")
        );
    }

    #[test]
    fn rejects_too_soon() {
        // Resolves 59s out, min = 60s → rejected.
        let unix = NOW + 59;
        assert_eq!(
            check_resolution_horizon(Some(unix), NOW, 259_200, 60),
            Some("market resolves too soon")
        );
    }

    #[test]
    fn admits_within_both_bounds() {
        // Resolves 1h out — inside [60s, 72h].
        let unix = NOW + 3_600;
        assert_eq!(check_resolution_horizon(Some(unix), NOW, 259_200, 60), None);
    }

    #[test]
    fn bounds_are_inclusive_at_edges() {
        // Exactly max out and exactly min out are both allowed (strict >/< rejects).
        assert_eq!(
            check_resolution_horizon(Some(NOW + 259_200), NOW, 259_200, 60),
            None
        );
        assert_eq!(
            check_resolution_horizon(Some(NOW + 60), NOW, 259_200, 60),
            None
        );
    }

    #[test]
    fn zero_disables_each_bound_independently() {
        // max=0 disables the upper bound; a far-future market is allowed.
        assert_eq!(
            check_resolution_horizon(Some(NOW + 10_000_000), NOW, 0, 60),
            None
        );
        // min=0 disables the lower bound; an imminent market is allowed.
        assert_eq!(
            check_resolution_horizon(Some(NOW + 1), NOW, 259_200, 0),
            None
        );
    }
}
