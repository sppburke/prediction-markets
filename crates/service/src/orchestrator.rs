//! Event dispatch loop: routes decoded trade events to the copy-signal-engine,
//! then gates signals through strategy evaluation and execution dispatch.
//!
//! The orchestrator is the sole paper financial serializer. Its I/O is the paper-log writer,
//! admission/current-price clients, and the one CLOB `/book` fetch used to build exact economics.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pe_copy_signal_engine::{
    IncomingTrade, LeaderSignal, SignalConfig, TradeProvenance, classify_trade,
};
use pe_core_types::{
    CollateralAmount, EventSeq, MarketId, MarketOutcomeId, OutcomeId, PolymarketTokenId, Price,
    Probability, ReceivedAt, ReconstructionQuality, ShareAmount, Side, SourceId, SourceTimestamp,
    SourceTradeId, TraderId, VenueId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn};
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

use crate::bucket_commit::{BucketCommitEngine, DecisionContinuationV3, PaperFreshnessPolicy};
use crate::clob_book::{ClobBookError, ClobBookFetcher, OrderBook};
use crate::decision_replay::{
    AuthorityEvidence, BookEvidence, DecisionEvidenceAccumulator, MarketEndEvidence,
    MarketPriceEvidence, TerminalDispositionEvidence, WinnerFollowDecisionInputs,
    WinnerFollowRiskInputEvidence, ladder_plan_blake3, replay_decision_pending,
};
#[cfg(test)]
use crate::decision_replay::{ReplayDecisionError, replay_decision_pending_with_control};
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
    PreparedFillRequest, PreparedResolutionRequest, SourceEvidence, SupabaseStateTrait,
    apply_financial_result, reconcile_active_financial_frames, resolution_source_received_at,
    terminalize_final_fill_decision,
};

fn runtime_source_evidence(
    source_receipts: Option<&SourceReceiptIndex>,
) -> Result<SourceEvidence<'_>, String> {
    source_receipts
        .map(SourceEvidence::Index)
        .ok_or_else(|| "runtime source receipt index is unavailable".to_owned())
}

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

type BookReadResult = Result<Result<OrderBook, ClobBookError>, tokio::time::error::Elapsed>;

/// The decision owns this read; dropping it cancels unfinished book work.
struct EarlyBookRead<F: Future> {
    token_id: PolymarketTokenId,
    future: Pin<Box<F>>,
    completed: Option<F::Output>,
}

impl<F: Future> EarlyBookRead<F> {
    async fn during<W: Future>(&mut self, work: W) -> W::Output {
        tokio::pin!(work);
        if self.completed.is_none() {
            tokio::select! {
                biased;
                result = &mut self.future => self.completed = Some(result),
                output = &mut work => return output,
            }
        }
        work.await
    }

    async fn finish(self) -> (PolymarketTokenId, F::Output) {
        let output = match self.completed {
            Some(output) => output,
            None => self.future.await,
        };
        (self.token_id, output)
    }
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
    best_ask: Option<Price>,
    reason: &str,
) -> BookEvidence {
    BookEvidence {
        request_token_id: token_id.map(str::to_owned),
        outcome: outcome.to_owned(),
        response_blake3: response_blake3.map(str::to_owned),
        fetched_at_unix_ms,
        best_ask: best_ask.map(|price| price.0.normalize().to_string()),
        vwap_basis: None,
        ladder_plan_blake3: None,
        reason: Some(reason.to_owned()),
    }
}

fn unix_millis(instant: OffsetDateTime) -> i64 {
    i64::try_from(instant.unix_timestamp_nanos() / 1_000_000).unwrap_or(i64::MAX)
}

enum ActiveFinancialFill {
    Committed(pe_event_log::AppendReceipt),
    Expired,
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
    let terminal_at = terminal_now();
    let mut complete = evidence.clone();
    complete.record_clock("terminal_transition", unix_millis(terminal_at));
    let json = complete.render(authority, terminal)?;
    Ok(Some((json, terminal_at.unix_timestamp())))
}

pub(crate) fn terminal_now() -> OffsetDateTime {
    #[cfg(feature = "scenario")]
    if let Ok(at) = SCENARIO_TERMINAL_CLOCK.try_with(|at| *at) {
        return at;
    }
    OffsetDateTime::now_utc()
}

#[cfg(feature = "scenario")]
tokio::task_local! {
    /// Fixed terminal clock shared by orchestrator and recovery byte-compatibility scenarios.
    pub static SCENARIO_TERMINAL_CLOCK: OffsetDateTime;
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
    pub live_journal: Option<LiveJournalAccess>,
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

/// Boot verification can read an existing journal without granting a paper-only
/// service the writer used for live dispatch staging.
pub enum LiveJournalAccess {
    Writable(Arc<pe_execution_core::LiveJournal>),
    ReadOnly(PathBuf),
}

impl LiveJournalAccess {
    fn path(&self) -> &Path {
        match self {
            Self::Writable(journal) => journal.path(),
            Self::ReadOnly(path) => path,
        }
    }
}

impl From<Arc<pe_execution_core::LiveJournal>> for LiveJournalAccess {
    fn from(journal: Arc<pe_execution_core::LiveJournal>) -> Self {
        Self::Writable(journal)
    }
}

#[cfg(feature = "scenario")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameCrashBoundary {
    FrameCommit,
    Staging,
    Prepared,
    Authority,
    Final,
}

/// Scenario-only deterministic seams (#546): fixed admission-clock instants and historical mark
/// results consumed in order, plus one-shot faults immediately before the durable writes whose
/// rollback the fan-in acceptance suite must prove. Compiled only with the `scenario` feature;
/// production has no clock/result injection and no fault path.
#[cfg(feature = "scenario")]
#[derive(Debug, Default)]
pub struct ScenarioHooks {
    pub frame_barriers: std::sync::Mutex<
        std::collections::HashMap<WalletAddress, Vec<pe_event_log::AppendReceipt>>,
    >,
    pub frame_crash_boundary: std::sync::Mutex<Option<FrameCrashBoundary>>,
    pub age_clock: std::sync::Mutex<std::collections::VecDeque<OffsetDateTime>>,
    /// Deterministically advance the next queued age sample after one successful observation
    /// resolution. This models receipt I/O latency without sleeping in scenario tests.
    pub observation_resolution_advance_millis: std::sync::atomic::AtomicI64,
    pub admission_artifacts:
        std::sync::Mutex<std::collections::VecDeque<pe_execution_core::LiveAdmissionArtifact>>,
    #[cfg(test)]
    admission_wait: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    #[cfg(test)]
    admission_failure: std::sync::atomic::AtomicBool,
    pub boundary_mark_prices:
        std::sync::Mutex<std::collections::VecDeque<crate::risk_inputs::HistoricalMarkPrice>>,
    pub financial_clock_unix: std::sync::atomic::AtomicI64,
    pub fail_next_stage_seed: std::sync::atomic::AtomicBool,
    pub fail_next_no_copy_commit: std::sync::atomic::AtomicBool,
    /// One-shot fault standing in for a failed `RiskHaltChanged` append.
    pub fail_next_halt_append: std::sync::atomic::AtomicBool,
    /// Stop after the accepted checkpoint, before any new FinancialPrepared is durable.
    pub fail_next_prepared_append: std::sync::atomic::AtomicBool,
    /// Pause after a synchronized membership append, before either in-memory set is changed.
    pub pause_after_membership_append: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
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
    paper_writer: crate::paper_recovery::PaperLog,
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
    live_journal: Option<Arc<pe_execution_core::LiveJournal>>,
    live_dispatch_ready: Option<Arc<tokio::sync::Notify>>,
    /// Set at the first uncertain paper sync boundary. Dropping the receiver
    /// then backpressures/stops every producer; supervisor owns process exit.
    intake_stopped: bool,
    watchlist_writer_lock: Option<Arc<Mutex<()>>>,
    /// Open production continuations loaded before producers. The durable row
    /// remains the owner; this queue only preserves its causal boot order.
    pending_boot: VecDeque<IncomingTrade>,
    pending_continuations: HashMap<SourceTradeId, DecisionContinuationV3>,
    /// A committed continuation failed to load; stop the critical owner before more control work.
    pending_load_failure: Option<String>,
    /// True only while replaying continuations that were open at process boot. Newly committed
    /// buckets still pass the current source-health gate before their financial disposition.
    resuming_boot: bool,
    financial_log_paths: Option<(std::path::PathBuf, std::path::PathBuf)>,
    source_receipts: Option<SourceReceiptIndex>,
    frame_source_log: Option<crate::activity_ingest::SourceLogHandle>,
    frame_stale_secs: i64,
    qualification_start: Option<pe_event_log::AppendReceipt>,
    admission_builder: Option<crate::live_venue_adapter::LiveAdmissionBuilder>,
    boundary_mark_fetcher: Option<Arc<HistoricalMarkAdapter>>,
    active_risk_halts: HashSet<(
        crate::paper_recovery::RiskHaltOwner,
        pe_risk_engine::RiskHaltCause,
    )>,
    feed_latch: crate::frame_admission::FeedLatchBasis,
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
            .map_err(|error| {
                self.intake_stopped = true;
                error.to_string()
            })
    }

    fn apply_risk_halt_transition(
        &mut self,
        owner: crate::paper_recovery::RiskHaltOwner,
        cause: pe_risk_engine::RiskHaltCause,
        state: crate::paper_recovery::HaltState,
        evidence: serde_json::Value,
    ) -> Result<pe_event_log::AppendReceipt, String> {
        let key = (owner.clone(), cause);
        let record = PaperLogRecord::RiskHaltChanged {
            owner,
            cause,
            state,
            evidence,
        };
        #[cfg(feature = "scenario")]
        let injected = self.take_scenario_fault(|h| &h.fail_next_halt_append);
        #[cfg(not(feature = "scenario"))]
        let injected = false;
        let appended = if injected {
            Err("injected risk halt append failure".to_owned())
        } else {
            self.append_paper_record(&record)
        };
        let receipt = match appended {
            Ok(receipt) => receipt,
            Err(error) => {
                // The frame's durability is unknown, so the in-memory halt set can no longer stand
                // in for the paper prefix; stop intake and let the restart rebuild it from the log.
                error!(%error, owner = ?key.0, ?cause, ?state, "risk halt append failed; stopping intake");
                self.intake_stopped = true;
                return Err(error);
            }
        };
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
        financial_semantic_version: u32,
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
            if financial_semantic_version >= 2 && cause == RiskHaltCause::CopyLatency {
                continue;
            }
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

    fn apply_global_halts_to_snapshot(
        &self,
        snapshot: &mut RiskSnapshot,
        financial_semantic_version: u32,
    ) {
        apply_global_risk_halts(
            &self.active_risk_halts,
            snapshot,
            financial_semantic_version < 2,
        );
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

    /// Seal qualification against the caller's exact source candidate (GitHub issue #574).
    fn seal_qualification(
        &mut self,
        reason: SealReason,
        sealed_cutoff_unix: i64,
        candidate: pe_event_log::LogTailBinding,
    ) -> Result<(), String> {
        let (paper_log_path, _) = self
            .financial_log_paths
            .as_ref()
            .ok_or_else(|| "qualification seal is unavailable before Start".to_owned())?;
        let era = crate::paper_recovery::paper_era(
            self.paper_writer
                .snapshot()
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
        let source_receipts = self.source_receipts.as_ref().ok_or_else(|| {
            "qualification seal is unavailable before the receipt index is installed".to_owned()
        })?;
        let seal_started = std::time::Instant::now();
        info!(
            reason = ?reason,
            candidate_sequence = ?candidate.last_sequence,
            "qualification seal started"
        );
        let qualification_error = |error| match error {
            crate::qualification::QualificationError::EventLog(error) => error.to_string(),
            other => other.to_string(),
        };
        let (sealed_source_prefix, selected) =
            crate::qualification::indexed_source_prefix_selection(
                &self.paper_state,
                source_receipts,
                &candidate,
                &started.source_prefix,
            )
            .map_err(qualification_error)?;
        let frames_walked = sealed_source_prefix
            .last_sequence
            .map_or(0, |sequence| sequence.0.saturating_add(1));
        let (decision_evidence, reason) = match (selected, reason) {
            (Ok(decisions), reason) => {
                let decision_keys = decisions
                    .rows
                    .into_iter()
                    .map(|row| (row.source_trade_id, row.semantic_revision))
                    .collect::<Vec<_>>();
                let evidence = self
                    .paper_state
                    .seal_decision_evidence_for_source_prefix(
                        &decision_keys,
                        &decisions.in_prefix,
                        sealed_source_prefix.last_sequence,
                    )
                    .map_err(|error| error.to_string())?;
                (evidence, reason)
            }
            // An insufficient-evidence seal still closes the verified prefix when its decision
            // evidence cannot be assembled (a bracket's recorded pages whose install never
            // committed leave source trades without durable groups): its digest covers no
            // decisions and its reason says why. A Complete seal still requires the evidence.
            (
                Err(crate::qualification::QualificationError::InsufficientEvidence(detail)),
                SealReason::InsufficientEvidence(why),
            ) => (
                Vec::new(),
                SealReason::InsufficientEvidence(format!(
                    "{why}; decision evidence unavailable: {detail}"
                )),
            ),
            (Err(error), _) => return Err(qualification_error(error)),
        };
        let financial_prefix = self.paper_writer.verified_tail().map_err(|error| {
            self.intake_stopped = true;
            error.to_string()
        })?;
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
        info!(
            elapsed_ms = u64::try_from(seal_started.elapsed().as_millis()).unwrap_or(u64::MAX),
            frames_walked, "qualification seal completed"
        );
        Ok(())
    }

    fn apply_seal_check(
        &mut self,
        proposed_economic_hash: &str,
        proposed_financial_semantic_version: u32,
    ) -> Result<(), String> {
        let (_paper_log_path, _) = self
            .financial_log_paths
            .as_ref()
            .ok_or_else(|| "qualification seal check is unavailable before Start".to_owned())?;
        let era = crate::paper_recovery::paper_era(
            self.paper_writer
                .snapshot()
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
        let candidate = self
            .source_receipts
            .as_ref()
            .ok_or_else(|| {
                "qualification seal check is unavailable before the receipt index is installed"
                    .to_owned()
            })?
            .current_tail_binding()
            .map_err(|error| error.to_string())?;
        self.seal_qualification(
            reason,
            OffsetDateTime::now_utc().unix_timestamp(),
            candidate,
        )
    }

    /// Boot must settle and seal the old qualification before resuming paper decisions.
    pub async fn seal_before_resume(
        &mut self,
        proposed_economic_hash: &str,
        proposed_financial_semantic_version: u32,
    ) -> Result<(), String> {
        self.reconcile_oldest_financial_prepared().await?;
        let (_paper_log_path, _) = self
            .financial_log_paths
            .as_ref()
            .ok_or_else(|| "qualification seal check is unavailable before Start".to_owned())?;
        let era = crate::paper_recovery::paper_era(
            self.paper_writer
                .snapshot()
                .map_err(|error| error.to_string())?,
        );
        let seal_needed = era.start.as_ref().is_some_and(|(_, start)| {
            Self::qualification_seal_reason(
                start,
                proposed_economic_hash,
                proposed_financial_semantic_version,
            )
            .is_some()
        }) && !era.frames.iter().any(|frame| {
            matches!(
                &frame.frame,
                PaperLogFrame::Record(PaperLogRecord::QualificationSealed(_))
            )
        });
        if seal_needed && !self.pending_boot.is_empty() {
            // The seal digest requires terminal decisions. Finish only the preceding
            // generation's frozen continuations before sealing; no new source producer is
            // running yet, and continuation 7 may never be decided under an unsealed Start.
            if self
                .pending_continuations
                .values()
                .any(|continuation| continuation.version() == 7)
            {
                return Err(
                    "current-semantic continuation is open before qualification seal".to_owned(),
                );
            }
            self.resume_pending_before_producers()
                .await
                .map_err(|error| error.to_string())?;
        }
        self.apply_seal_check(proposed_economic_hash, proposed_financial_semantic_version)
    }

    /// Converge the verified oldest paper Prepared before any successor financial control.
    /// The shared recovery routine owns frozen-request reconstruction and appends at most the
    /// missing Final; no control-specific cursor or retry state is maintained here.
    async fn reconcile_oldest_financial_prepared(&mut self) -> Result<(), String> {
        let (_paper_log_path, _source_log_path) = self
            .financial_log_paths
            .clone()
            .ok_or_else(|| "active financial log paths are not configured".to_owned())?;
        let authority = self
            .supabase_state
            .clone()
            .ok_or_else(|| "active financial era has no authority client".to_owned())?;
        let source_evidence = runtime_source_evidence(self.source_receipts.as_ref())?;
        reconcile_active_financial_frames(
            &authority,
            &self.paper_state,
            source_evidence,
            &self.paper_writer,
        )
        .await
        .map_err(|error| error.to_string())?;
        let era = crate::paper_recovery::paper_era(
            self.paper_writer
                .snapshot()
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
        let (_paper_log_path, _source_log_path) = self
            .financial_log_paths
            .clone()
            .ok_or_else(|| "active financial log paths are not configured".to_owned())?;
        let start = self
            .paper_state
            .financial_start()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "resolution candidate precedes QualificationStarted".to_owned())?;
        let era = crate::paper_recovery::paper_era(
            self.paper_writer
                .snapshot()
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
        let source_receipts = self.source_receipts.clone();
        let source_evidence = runtime_source_evidence(source_receipts.as_ref())?;
        let settled_at_unix = resolution_source_received_at(
            source_evidence,
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
            source_evidence,
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
        paper_freshness: Option<&(PaperFreshnessPolicy, SourceTimestamp)>,
        evidence: &mut Option<DecisionEvidenceAccumulator>,
    ) -> Result<ActiveFinancialFill, String> {
        if economic.market.outcome_index > 1 {
            return Err(format!(
                "active paper fill outcome {} is not binary",
                economic.market.outcome_index
            ));
        }
        self.reconcile_oldest_financial_prepared().await?;

        let (_paper_log_path, _source_log_path) = self
            .financial_log_paths
            .clone()
            .ok_or_else(|| "active financial log paths are not configured".to_owned())?;
        let authority = self
            .supabase_state
            .clone()
            .ok_or_else(|| "active financial era has no authority client".to_owned())?;

        let era = crate::paper_recovery::paper_era(
            self.paper_writer
                .snapshot()
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
        if let Some((policy, source_time)) = paper_freshness {
            let admission_at = self.admission_now();
            let evidence = evidence
                .as_mut()
                .ok_or_else(|| "paper decision evidence is missing".to_owned())?;
            evidence
                .record_precise_clock("paper_prepared_staleness_gate", admission_at)
                .map_err(|error| error.to_string())?;
            if policy.expired(source_time.clone(), admission_at) {
                return Ok(ActiveFinancialFill::Expired);
            }
            evidence.record_clock("paper_dispatch", unix_millis(admission_at));
            self.paper_state
                .checkpoint_decision_pending(
                    &trade.source_trade_id,
                    &evidence
                        .checkpoint_json()
                        .map_err(|error| error.to_string())?,
                    admission_at.unix_timestamp(),
                )
                .map_err(|error| error.to_string())?;
        }
        #[cfg(feature = "scenario")]
        if self.take_scenario_fault(|hooks| &hooks.fail_next_prepared_append) {
            return Err("injected failure after checkpoint before Prepared".to_owned());
        }
        let source_receipts = self.source_receipts.clone();
        let source_evidence = runtime_source_evidence(source_receipts.as_ref())?;
        let prepared_receipt = self.append_paper_record(&PaperLogRecord::FinancialPrepared {
            expected_authority: expected.clone(),
            payload: payload.clone(),
        })?;
        #[cfg(feature = "scenario")]
        if self.crash_after_frame_boundary(FrameCrashBoundary::Prepared) {
            return Err("injected crash after frame Prepared".to_owned());
        }
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
        #[cfg(feature = "scenario")]
        if self.crash_after_frame_boundary(FrameCrashBoundary::Authority) {
            return Err("injected crash after frame authority".to_owned());
        }
        let result = crate::paper_recovery::FinancialResult::Fill { canonical };
        apply_financial_result(
            &self.paper_state,
            start,
            &expected,
            prepared_receipt,
            &payload,
            &result,
            source_evidence,
        )
        .map_err(|error| error.to_string())?;
        let final_receipt = self.append_paper_record(&PaperLogRecord::FinancialFinal {
            prepared_receipt,
            result: result.clone(),
        })?;
        #[cfg(feature = "scenario")]
        if self.crash_after_frame_boundary(FrameCrashBoundary::Final) {
            return Err("injected crash after frame Final".to_owned());
        }
        terminalize_final_fill_decision(&self.paper_state, &payload, &result, final_receipt)
            .map_err(|error| error.to_string())?;
        if let Some(dispatch_id) = dispatch_id {
            let flipped = self
                .paper_state
                .flip_dispatch_ready(dispatch_id, "fill")
                .map_err(|error| error.to_string())?;
            if flipped {
                self.notify_live_dispatch_ready();
            }
        }
        let crate::paper_recovery::FinancialResult::Fill { canonical } = result else {
            return Err("internal fill Final kind mismatch".to_owned());
        };
        self.bankroll = canonical.bankroll;
        Ok(ActiveFinancialFill::Committed(final_receipt))
    }

    /// Build the paper owner's coherent risk snapshot from the active financial prefix.
    async fn active_paper_risk_snapshot(
        &mut self,
        signal: &LeaderSignal,
        proposed_debit: CollateralAmount,
        per_trade_cap_bps: i32,
        financial_semantic_version: u32,
    ) -> Result<(pe_execution_core::RiskAudit, pe_event_log::AppendReceipt), ActivePaperRiskFailure>
    {
        let mut attempt = WinnerFollowRiskInputEvidence {
            financial_prefix: None,
            price_receipts: Vec::new(),
            evaluated_at_unix_ms: i64::MAX,
            proposed_debit,
            per_trade_cap_bps,
        };
        let (_paper_log_path, _) = self.financial_log_paths.as_ref().cloned().ok_or_else(|| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;
        let source_receipts = self.source_receipts.as_ref().ok_or_else(|| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;
        let era = crate::paper_recovery::paper_era(self.paper_writer.snapshot().map_err(|_| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?);
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
            financial_semantic_version,
        )
        .map_err(|cause| ActivePaperRiskFailure::new(cause, &attempt))?;
        self.synchronize_paper_risk_halts(
            &snapshot,
            evaluated_at_unix_ms,
            financial_semantic_version,
        )
        .map_err(|_| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;
        let financial_prefix = attempt.financial_prefix.ok_or_else(|| {
            ActivePaperRiskFailure::new(RiskInputsUnavailable::SnapshotSequenceMismatch, &attempt)
        })?;
        self.apply_global_halts_to_snapshot(&mut snapshot, financial_semantic_version);
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
        current_book_policy: bool,
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
        let inputs = pe_execution_core::EconomicInputs {
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
            chase_ceiling: if current_book_policy {
                Price::ONE
            } else {
                signal.leader_price
            },
            band_floor,
            band_ceiling_exclusive,
            applied_configuration_hash,
        };
        if current_book_policy {
            pe_execution_core::EconomicPrepared::compose_wire_two(inputs)
        } else {
            pe_execution_core::EconomicPrepared::compose(inputs)
        }
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
        let (_paper_log_path, _source_log_path) =
            self.financial_log_paths.as_ref().cloned().ok_or_else(|| {
                "daily boundary is unavailable before QualificationStarted".to_owned()
            })?;
        let mark_fetcher =
            Arc::clone(self.boundary_mark_fetcher.as_ref().ok_or_else(|| {
                "daily boundary historical-price reader is unavailable".to_owned()
            })?);
        let era = crate::paper_recovery::paper_era(
            self.paper_writer
                .snapshot()
                .map_err(|error| error.to_string())?,
        );
        let source_receipts = self
            .source_receipts
            .as_ref()
            .ok_or_else(|| "daily boundary source receipt index is unavailable".to_owned())?;
        let completed = crate::risk_inputs::completed_prepared_before_boundary_indexed(
            &era,
            source_receipts,
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
                    closure_receipt: None,
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
                Some(mark) => Ok((mark, None)),
                None => {
                    mark_fetcher
                        .fetch_paper(&key.0, token_id, cutoff_unix)
                        .await
                }
            };
            match fetched {
                Ok((mark, closure_receipt)) => {
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
                        closure_receipt,
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
                        closure_receipt: None,
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
        let (source_tail, byte_offset, last_receipt, previous_hash) = source_receipts
            .tail_with_last_frame()
            .map_err(|error| error.to_string())?;
        let (last_frame, end_offset) = pe_event_log::Reader::read_at(
            &source_tail.path,
            byte_offset,
            last_receipt.sequence,
            previous_hash,
        )
        .map_err(|error| error.to_string())?;
        if end_offset != source_tail.physical_tail
            || Some(last_frame.seq) != source_tail.last_sequence
            || last_frame.this_hash != source_tail.last_hash
        {
            return Err(
                "daily boundary source receipt index tail differs from log frame".to_owned(),
            );
        }
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
                self.paper_writer
                    .snapshot()
                    .map_err(|error| error.to_string())?,
            );
            crate::qualification::qualification_completion_for_causal_facts(&era, &completed)
        } else {
            None
        };
        if let Some(reason) = Self::automatic_completion_seal_reason(mark_is_valid, completion) {
            self.seal_qualification(reason, cutoff_unix, source_tail)?;
        }
        Ok(())
    }

    #[cfg(feature = "scenario")]
    fn crash_after_frame_boundary(&self, boundary: FrameCrashBoundary) -> bool {
        self.scenario_hooks.as_ref().is_some_and(|hooks| {
            let mut selected = hooks
                .frame_crash_boundary
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *selected == Some(boundary) {
                *selected = None;
                true
            } else {
                false
            }
        })
    }

    async fn apply_activity_frame(
        &mut self,
        receipt: pe_event_log::AppendReceipt,
    ) -> Result<(), String> {
        let index = self
            .source_receipts
            .clone()
            .ok_or_else(|| "frame source index missing".to_owned())?;
        let source_log = self
            .frame_source_log
            .clone()
            .ok_or_else(|| "frame source writer missing".to_owned())?;
        let writer_lock = self.watchlist_writer_lock.clone();
        let guard = match &writer_lock {
            Some(lock) => Some(lock.lock().await),
            None => None,
        };
        let source = index
            .source_envelope(receipt)
            .map_err(|error| error.to_string())?;
        let observation =
            pe_source_polymarket_public::parse_activity_trade_observation(&source.payload)
                .map_err(|error| error.to_string())?;
        let watchlist = self.live_watchlist.snapshot();
        let entry = watchlist
            .entries
            .iter()
            .find(|entry| entry.wallet == observation.wallet);
        let configuration = self
            .runtime_config
            .as_ref()
            .map(|config| config.snapshot().as_ref().clone())
            .ok_or_else(|| "frame configuration snapshot missing".to_owned())?;
        let tail = self
            .paper_writer
            .verified_tail()
            .map_err(|error| error.to_string())?;
        let paper_prefix = tail
            .last_sequence
            .map(|sequence| pe_event_log::AppendReceipt {
                sequence,
                this_hash: tail.last_hash,
            });
        let latch = self.feed_latch.clone();
        let admitted_at = self.financial_now();
        let route = self
            .bucket_engine
            .prepare_activity_frame(
                receipt,
                &index,
                crate::bucket_commit::FrameAdmissionContext {
                    admitted_at,
                    stale_secs: self.frame_stale_secs,
                    quality: entry.map_or(
                        pe_core_types::ReconstructionQuality::new(0)
                            .map_err(|error| error.to_string())?,
                        |entry| entry.reconstruction_quality,
                    ),
                    signal_config: self.signal_config.clone(),
                    copy_eligible: entry
                        .is_some_and(|entry| entry.tier == pe_trader_index::WatchlistTier::Active),
                    configuration,
                    basis: crate::bucket_commit::FrozenDecisionBasis {
                        win_rate_p: self.win_rate_p_for(&watchlist, &TraderId(observation.wallet)),
                        bankroll: self.bankroll,
                    },
                    latch,
                    paper_prefix,
                },
            )
            .map_err(|error| error.to_string())?;
        let (source_id, payload) = match &route {
            crate::bucket_commit::FrameRoute::Ignored => return Ok(()),
            crate::bucket_commit::FrameRoute::Fallback(artifact) => (
                crate::frame_admission::FRAME_FALLBACK_SOURCE_ID,
                crate::frame_admission::canonical_bytes(artifact),
            ),
            crate::bucket_commit::FrameRoute::Admission(capture) => (
                crate::frame_admission::FRAME_ADMISSION_SOURCE_ID,
                crate::frame_admission::FrameAdmissionArtifact::from_inputs(&capture.0)
                    .and_then(|artifact| crate::frame_admission::canonical_bytes(&artifact)),
            ),
        };
        let admission_receipt = source_log
            .append(EnvelopeIn {
                source_id: SourceId(source_id.to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: pe_core_types::SourceTimestamp(admitted_at),
                received_at: pe_core_types::ReceivedAt(admitted_at),
                content_type: pe_event_log::ContentType::Json,
                payload: payload.map_err(|error| error.to_string())?,
            })
            .await
            .map_err(|error| error.to_string())?;
        let crate::bucket_commit::FrameRoute::Admission(capture) = route else {
            return Ok(());
        };
        let (inputs, context) = *capture;
        let id = self
            .bucket_engine
            .commit_activity_frame(
                crate::frame_admission::FrameDecisionProof {
                    admission_receipt,
                    inputs,
                },
                PaperFreshnessPolicy {
                    activity_ws_enabled: self.activity_ws_enabled,
                    copy_latency_budget_secs: self.copy_latency_budget_secs,
                },
                context,
                &index,
            )
            .map_err(|error| error.to_string())?;
        drop(guard);
        #[cfg(feature = "scenario")]
        if self.crash_after_frame_boundary(FrameCrashBoundary::FrameCommit) {
            return Err("injected crash after frame commit".to_owned());
        }
        self.resume_committed_rows(&[id]).await
    }

    fn apply_feed_audit_update(
        &mut self,
        update: crate::orchestrator_control::FeedAuditUpdate,
    ) -> Result<crate::orchestrator_control::FeedAuditAcknowledgement, String> {
        use crate::orchestrator_control::{FeedAuditAcknowledgement, FeedAuditUpdate};
        use crate::paper_recovery::{HaltState, PaperLogRecord};
        let index = self
            .source_receipts
            .as_ref()
            .ok_or_else(|| "feed audit source index is missing".to_owned())?;
        match update {
            FeedAuditUpdate::RetireObservation {
                receipt,
                source_trade_id,
                unbound,
                verified_read,
            } => self.bucket_engine.retire_observation(
                receipt,
                &source_trade_id,
                unbound,
                verified_read.as_deref(),
            ),
            FeedAuditUpdate::Frontier(frontier, read) => self
                .bucket_engine
                .publish_frontier(frontier, index, read.as_deref())
                .map(|()| FeedAuditAcknowledgement::Applied),
            FeedAuditUpdate::Incident(incident, read) => {
                let identity = self
                    .paper_state
                    .activity_frame_decision_index(None)
                    .map_err(|error| error.to_string())?
                    .into_iter()
                    .find(|frame| frame.observed_source_receipt == Some(incident.frame_receipt))
                    .ok_or_else(|| "incident has no durable admitted frame".to_owned())?;
                let row = self
                    .paper_state
                    .decision_pending_for(&identity.source_trade_id)
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| "incident decision missing".to_owned())?;
                let frame =
                    DecisionContinuationV3::from_durable(&row).map_err(|e| e.to_string())?;
                if let Some(read) = read {
                    frame
                        .verify_activity_frame_with_index(index)
                        .map_err(|error| error.to_string())?;
                    if read.receipt != incident.deciding_commitment_receipt
                        || frame.observed_source_receipt != Some(incident.frame_receipt)
                        || incident.engagement_receipt.is_some()
                    {
                        return Err("incident differs from authenticated read/frame".to_owned());
                    }
                    crate::feed_audit::verify_incident_conclusion(&frame, &incident, &read)
                        .map_err(|error| error.to_string())?;
                } else {
                    crate::feed_audit::verify_incident(&frame, &incident, &mut |receipt| {
                        index
                            .source_envelope(receipt)
                            .map(crate::bucket_commit::CompleteActivityPage::from)
                    })
                    .map_err(|error| error.to_string())?;
                }
                let era = crate::paper_recovery::paper_era(
                    self.paper_writer.snapshot().map_err(|e| e.to_string())?,
                );
                if crate::feed_audit::audited_receipts(&era).contains(&incident.frame_receipt) {
                    return Ok(FeedAuditAcknowledgement::Applied);
                }
                let incident_index = index.clone();
                let receipt = self.append_paper_record(&PaperLogRecord::FeedIncidentChanged {
                    incident: incident.clone(),
                    state: HaltState::Engaged,
                })?;
                incident_index.remember_frame_incident(&incident);
                self.feed_latch = crate::frame_admission::FeedLatchBasis {
                    latest_incident: Some(receipt),
                    release: None,
                };
                self.bucket_engine
                    .retire_frame_audit(incident.frame_receipt);
                {
                    let mut health = self
                        .health
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    health.feed_latch = self.feed_latch.clone();
                    health.feed_incident = Some(incident.clone());
                }
                error!(cause = ?incident.cause, frame_receipt = ?incident.frame_receipt, deciding_commitment = ?incident.deciding_commitment_receipt,
                    counterpart = ?incident.counterpart_identity, engagement = ?receipt, "feed audit incident engaged; frames wait for history");
                Ok(FeedAuditAcknowledgement::Applied)
            }
            FeedAuditUpdate::Release {
                expected_engagement_hash,
            } => {
                let era = crate::paper_recovery::paper_era(
                    self.paper_writer.snapshot().map_err(|e| e.to_string())?,
                );
                let basis =
                    crate::paper_recovery::feed_latch_basis(&era).map_err(|e| e.to_string())?;
                if !basis.engaged()
                    || basis
                        .latest_incident
                        .is_none_or(|receipt| receipt.this_hash != expected_engagement_hash)
                {
                    return Ok(FeedAuditAcknowledgement::Applied);
                }
                let Some(mut incident) = crate::feed_audit::latest_incident(&era) else {
                    return Err("latest feed engagement is missing".to_owned());
                };
                incident.engagement_receipt = basis.latest_incident;
                let release = self.append_paper_record(&PaperLogRecord::FeedIncidentChanged {
                    incident,
                    state: HaltState::Released,
                })?;
                self.feed_latch = crate::frame_admission::FeedLatchBasis {
                    latest_incident: basis.latest_incident,
                    release: Some(release),
                };
                self.publish_feed_latch_health();
                Ok(FeedAuditAcknowledgement::Applied)
            }
        }
    }

    fn publish_feed_latch_health(&self) {
        self.health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .feed_latch = self.feed_latch.clone();
    }

    async fn apply_control_message(&mut self, message: OrchestratorControl) {
        match message {
            OrchestratorControl::ActivityFrameDecision { receipt } => {
                if let Err(error) = self.apply_activity_frame(receipt).await {
                    self.pending_load_failure = Some(error);
                    self.intake_stopped = true;
                }
            }
            OrchestratorControl::FeedAuditUpdate {
                update,
                acknowledged,
            } => {
                let result = self.apply_feed_audit_update(update);
                let _ = acknowledged.send(result);
            }
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
                #[cfg(feature = "scenario")]
                if let Some(hooks) = &self.scenario_hooks {
                    hooks
                        .frame_barriers
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(wallet, self.bucket_engine.unresolved_receipts(wallet));
                }
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
                    let (frozen_basis, policy) = self.freeze_decision_basis(&aggregates);
                    let result = self.bucket_engine.commit_with_freshness_policy(
                        aggregates,
                        context.as_ref(),
                        frozen_basis,
                        Some(policy),
                    );
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
                    let (frozen_basis, policy) = self.freeze_decision_basis(&aggregates);
                    let result = self.bucket_engine.commit_with_freshness_policy(
                        aggregates,
                        context.as_ref(),
                        frozen_basis,
                        Some(policy),
                    );
                    if let Ok(result) = &result
                        && result.newly_fenced.is_some()
                    {
                        let mut fenced = HashSet::new();
                        fenced.insert(result.wallet);
                        self.live_watchlist.remove_fenced(&fenced);
                    }
                    result
                };
                if let Ok(result) = &result
                    && let Err(msg) = self.resume_committed_rows(&result.pending).await
                {
                    self.pending_load_failure = Some(msg.clone());
                    let _ = committed.send(Err(msg));
                    return;
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
                checks,
                acknowledged,
            } => {
                let writer_lock = self.watchlist_writer_lock.clone();
                let _writer = match &writer_lock {
                    Some(lock) => Some(lock.lock().await),
                    None => None,
                };
                if let Err(error) = checks.recheck_and_seed(
                    &self.paper_state,
                    &self.live_watchlist,
                    &change,
                    &replacements,
                ) {
                    let _ = acknowledged.send(Err(error.into_publish()));
                    return;
                }
                let removed = change.removed.iter().copied().collect::<HashSet<_>>();
                let capacity = change.capacity;
                let additions = replacements
                    .iter()
                    .filter(|entry| change.added.contains(&entry.wallet))
                    .cloned()
                    .collect::<Vec<_>>();
                let receipt = self.append_paper_record(&change.clone().into_record());
                if receipt.is_ok() {
                    #[cfg(feature = "scenario")]
                    if let Some(hooks) = &self.scenario_hooks {
                        let pause = hooks
                            .pause_after_membership_append
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .take();
                        if let Some((entered, release)) = pause {
                            let _ = entered.send(());
                            let _ = release.await;
                        }
                    }
                    self.live_watchlist
                        .commit_structural_change(&change.removed, &change.added);
                    self.live_watchlist.replace(&removed, &additions, capacity);
                    crate::watchlist_maintenance::apply_live_reentries(
                        &self.live_watchlist,
                        &self.paper_state,
                        &checks.reentries,
                        &replacements,
                        capacity,
                    );
                    checks.commit_capacity();
                } else {
                    self.intake_stopped = true;
                }
                let result =
                    receipt.map_err(crate::watchlist_maintenance::PublishError::UncertainAppend);
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
                let result = self
                    .seal_before_resume(
                        &proposed_economic_hash,
                        proposed_financial_semantic_version,
                    )
                    .await;
                let _ = acknowledged.send(result);
            }
        }
    }
}

pub(crate) fn verify_retained_terminal_decisions(
    paper_state: &PaperStateDb,
    journal_path: Option<&Path>,
) -> Result<(), anyhow::Error> {
    let rows = paper_state
        .decision_pending_history()
        .map_err(|error| anyhow::anyhow!("load decision continuation history: {error}"))?;
    let targets = paper_state
        .retained_terminal_dispatch_targets()
        .map_err(|error| anyhow::anyhow!("load retained terminal dispatch targets: {error}"))?;
    verify_retained_live_references(&rows, &targets, journal_path)
}

pub(crate) fn verify_retained_live_references(
    rows: &[pe_paper_state::DecisionPendingRow],
    targets: &[pe_paper_state::DispatchTargetRow],
    journal_path: Option<&Path>,
) -> Result<(), anyhow::Error> {
    let journal_events = if let Some(path) = journal_path {
        pe_execution_core::live_journal::replay_all(path)
            .map_err(|error| anyhow::anyhow!("verify retained live journal references: {error}"))?
    } else {
        Vec::new()
    };
    let mut selected_by_sequence = HashMap::new();
    let mut not_armed = HashSet::new();
    for event in journal_events {
        match event.payload {
            pe_execution_core::LiveJournalPayload::StagedDispatchControl(audit) => {
                selected_by_sequence.insert(event.seq, audit.dispatch_id.clone());
            }
            pe_execution_core::LiveJournalPayload::PendingTargetControl(audit)
                if audit.outcome
                    == pe_execution_core::live_journal::LiveTargetControlOutcome::NotArmed =>
            {
                not_armed.insert((
                    audit.dispatch_id.clone(),
                    audit.account_id.as_str().to_owned(),
                    audit.exec_rank,
                ));
            }
            _ => {}
        }
    }
    for row in rows
        .iter()
        .filter(|row| row.state == pe_paper_state::DecisionPendingState::Terminal)
    {
        let replayed = replay_decision_pending(row).map_err(|error| {
            anyhow::anyhow!("verify terminal pending {}: {error}", row.source_trade_id)
        })?;
        let terminal = &replayed.post_boundary.body.terminal;
        if let Some(seq) = terminal.dispatch_control_journal_seq {
            let dispatch_id = terminal.dispatch_id.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "terminal pending {} lacks dispatch binding",
                    row.source_trade_id
                )
            })?;
            if journal_path.is_none() {
                anyhow::bail!(
                    "terminal pending {} references live journal sequence {seq}, but the live journal file is unavailable",
                    row.source_trade_id
                );
            }
            if selected_by_sequence.get(&seq).map(String::as_str) != Some(dispatch_id) {
                anyhow::bail!(
                    "terminal pending {} references a missing or wrong-dispatch selected-target record at live journal sequence {seq}",
                    row.source_trade_id
                );
            }
        }
    }
    if journal_path.is_none() && !targets.is_empty() {
        anyhow::bail!("live journal file is unavailable for retained terminal target verification");
    }
    for target in targets {
        let rank = u64::try_from(target.exec_rank)
            .map_err(|_| anyhow::anyhow!("negative retained target rank"))?;
        if not_armed.contains(&(target.dispatch_id.clone(), target.account_id.clone(), rank))
            && target.terminal_reason.as_deref() != Some("not_armed")
        {
            anyhow::bail!(
                "terminal target {}/{} reason contradicts bound NotArmed control audit",
                target.dispatch_id,
                target.account_id
            );
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn verify_retained_terminal_decision(
    row: &pe_paper_state::DecisionPendingRow,
    journal: Option<&pe_execution_core::LiveJournal>,
) -> Result<(), ReplayDecisionError> {
    let replayed = replay_decision_pending(row)?;
    if replayed
        .post_boundary
        .body
        .terminal
        .dispatch_control_journal_seq
        .is_some()
    {
        let journal = journal.ok_or_else(|| {
            ReplayDecisionError::ControlJournal("live journal is unavailable".to_owned())
        })?;
        replay_decision_pending_with_control(row, journal.path())?;
    }
    Ok(())
}

impl<F: PageFetcher + Send + Sync, B: ClobBookFetcher> Orchestrator<F, B> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        live_watchlist: LiveWatchlist,
        config: OrchestratorConfig,
        strategy: WinnerFollowStrategy,
        paper_writer: crate::paper_recovery::PaperLog,
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
        paper_writer: crate::paper_recovery::PaperLog,
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
        paper_writer: crate::paper_recovery::PaperLog,
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
        let feed_era = crate::paper_recovery::paper_era(paper_writer.snapshot()?);
        let feed_latch = crate::paper_recovery::feed_latch_basis(&feed_era)?;
        {
            let mut health = health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            health.feed_latch = feed_latch.clone();
            health.feed_incident = crate::feed_audit::latest_incident(&feed_era);
        }
        verify_retained_terminal_decisions(
            &paper_state,
            config.live_journal.as_ref().map(LiveJournalAccess::path),
        )?;
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
            live_dispatch_ready: None,
            live_journal: config.live_journal.and_then(|access| match access {
                LiveJournalAccess::Writable(journal) => Some(journal),
                LiveJournalAccess::ReadOnly(_) => None,
            }),
            intake_stopped: false,
            watchlist_writer_lock: config.watchlist_writer_lock,
            pending_boot,
            pending_continuations,
            pending_load_failure: None,
            resuming_boot: false,
            financial_log_paths: None,
            source_receipts: None,
            frame_source_log: None,
            frame_stale_secs: 0,
            qualification_start: None,
            admission_builder: None,
            boundary_mark_fetcher: None,
            active_risk_halts: HashSet::new(),
            feed_latch,
        })
    }

    /// Restore unresolved ordering barriers and route only unfinished synchronized frames.
    pub async fn resume_activity_frames_before_producers(
        &mut self,
        all: &[pe_event_log::AppendReceipt],
        undelivered: &[pe_event_log::AppendReceipt],
    ) -> Result<(), String> {
        let index = self
            .source_receipts
            .clone()
            .ok_or_else(|| "frame source index missing".to_owned())?;
        self.bucket_engine
            .restore_frame_prefix(all, undelivered, &index)?;
        self.refresh_feed_latch()?;
        self.publish_feed_latch_health();
        for receipt in undelivered {
            self.apply_activity_frame(*receipt).await?;
        }
        Ok(())
    }

    /// Refresh after a synchronized incident/release edge; boot uses this same era reducer.
    pub(crate) fn refresh_feed_latch(&mut self) -> Result<(), String> {
        let era = crate::paper_recovery::paper_era(
            self.paper_writer
                .snapshot()
                .map_err(|error| error.to_string())?,
        );
        if let Some(index) = &self.source_receipts {
            self.bucket_engine.verify_feed_incidents(&era, index)?;
        }
        self.feed_latch =
            crate::paper_recovery::feed_latch_basis(&era).map_err(|error| error.to_string())?;
        self.health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .feed_incident = crate::feed_audit::latest_incident(&era);
        Ok(())
    }

    /// Install the source writer and the boot-resolved poll-round bound once.
    #[must_use]
    pub fn with_activity_frames(
        mut self,
        source_log: crate::activity_ingest::SourceLogHandle,
        poll_round_stale_secs: i64,
    ) -> Self {
        self.frame_source_log = Some(source_log);
        self.frame_stale_secs = poll_round_stale_secs;
        self
    }

    /// Share the process source receipt index. Every generation-5 attempt authenticates its
    /// earliest bound source time through it before the shared admission gates, in every
    /// financial posture; the Start-bound protocol installs the same index again with its logs.
    #[must_use]
    pub fn with_source_receipt_index(mut self, source_receipts: SourceReceiptIndex) -> Self {
        self.bucket_engine = self
            .bucket_engine
            .with_source_receipt_index(source_receipts.clone());
        self.source_receipts = Some(source_receipts);
        self
    }

    /// Wake the sequential live owner after a committed paper outcome releases a seed.
    #[must_use]
    pub fn with_live_dispatch_ready(mut self, ready: Arc<tokio::sync::Notify>) -> Self {
        self.live_dispatch_ready = Some(ready);
        self
    }

    fn notify_live_dispatch_ready(&self) {
        if let Some(ready) = &self.live_dispatch_ready {
            // One consumer; retain a permit when it is busy or its select is cancelled.
            ready.notify_one();
        }
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
        let canonical_path =
            std::fs::canonicalize(&source_log_path).map_err(pe_event_log::LogError::Io)?;
        if source_receipts.canonical_path() != Some(canonical_path.as_path()) {
            return Err(crate::paper_recovery::PaperLogScanError::SourceIndexPathMismatch);
        }
        let era = crate::paper_recovery::paper_era(crate::paper_recovery::scan_paper_log(
            &paper_log_path,
        )?);
        self.active_risk_halts = crate::paper_recovery::active_risk_halts(&era);
        self.feed_latch = crate::paper_recovery::feed_latch_basis(&era)?;
        self.publish_feed_latch_health();
        self.qualification_start = era.start.as_ref().map(|(receipt, _)| *receipt);
        self.financial_log_paths = Some((paper_log_path, source_log_path));
        self.bucket_engine
            .set_source_receipt_index(source_receipts.clone());
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
            .decision_pending_for(source_trade_id)
            .map_err(|error| anyhow::anyhow!("read pending {source_trade_id}: {error}"))?;
        let Some(row) = row else {
            return Ok(None);
        };
        if row.state != pe_paper_state::DecisionPendingState::Open {
            return Ok(None);
        }
        let continuation = DecisionContinuationV3::from_durable(&row)
            .map_err(|error| anyhow::anyhow!("decode pending {source_trade_id}: {error}"))?;
        let trade = continuation
            .incoming_trade()
            .map_err(|error| anyhow::anyhow!("rebuild pending {source_trade_id}: {error}"))?;
        self.pending_continuations
            .insert(source_trade_id.clone(), continuation);
        Ok(Some(trade))
    }

    /// Resume the continuations a bucket commit created, in commit order. The loop stops at the
    /// first row that leaves paper durability uncertain: the remaining rows stay pending for the
    /// restart, which rebuilds the halt set from the log before resuming them.
    async fn resume_committed_rows(&mut self, pending: &[SourceTradeId]) -> Result<(), String> {
        for source_trade_id in pending {
            if self.intake_stopped {
                break;
            }
            match self.load_pending_continuation(source_trade_id) {
                Ok(Some(trade)) => self.handle_trade(trade).await,
                Ok(None) => {}
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(())
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
            if let Some(msg) = self.pending_load_failure.take() {
                return Err(OrchestratorRunError::PendingRecovery(msg));
            }
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
        continuation_version: u16,
        probability: Probability,
        sizing_bankroll: Decimal,
        admission: &pe_execution_core::LiveAdmissionArtifact,
        early_book: Option<BookReadResult>,
    ) -> Result<GatePlanEvidence, GatePlanFailure> {
        let current_book_policy = matches!(continuation_version, 6 | 7);
        let cap_bps = u64::try_from(self.price_impact_cap_bps).map_err(|_| GatePlanFailure {
            reason: "impact gate cap invalid",
            book: Box::new(book_failure(
                None,
                "not_read",
                None,
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
                    None,
                    "outcome had no CLOB token id",
                )),
                checked_at_unix_ms: None,
            });
        };
        let book_read = match early_book {
            Some(result) => result,
            None => {
                tokio::time::timeout(
                    Duration::from_secs(CLOB_BOOK_HOT_PATH_TIMEOUT_SECS),
                    self.book_fetcher
                        .fetch_book(&admission.market.condition_id.0, &token_id),
                )
                .await
            }
        };
        let book = match book_read {
            Ok(Ok(book)) => book,
            Ok(Err(_)) => {
                return Err(GatePlanFailure {
                    reason: "price-impact book unusable: /book fetch failed (fail closed)",
                    book: Box::new(book_failure(
                        Some(&token_id),
                        "fetch_failed",
                        None,
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
                    None,
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
                    None,
                    "ask ladder was empty",
                )),
                checked_at_unix_ms: None,
            });
        };
        #[cfg(feature = "scenario")]
        let checked_at_unix_ms = u64::try_from(unix_millis(self.financial_now())).unwrap_or(0);
        #[cfg(not(feature = "scenario"))]
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
        let ceiling = (if current_book_policy {
            pe_execution_core::economic::current_book_impact_ceiling(
                best,
                self.price_impact_cap_bps,
            )
        } else {
            // Preserve the historical semantic-1 arithmetic exactly.
            let ceiling_raw = (best.0
                * (Decimal::from(10_000u32 + u32::try_from(cap_bps).unwrap_or(10_000))
                    / Decimal::from(10_000u32)))
            .min(Decimal::ONE);
            Price::new(ceiling_raw).map_err(pe_execution_core::EconomicError::from)
        })
        .map_err(|_| GatePlanFailure {
            reason: "price-impact band ceiling not constructible",
            book: Box::new(book_failure(
                Some(&token_id),
                "arithmetic_failure",
                Some(&book.response_blake3),
                Some(book.fetched_at_ms),
                None,
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
                    None,
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
                        None,
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
            SizingMode::Dollar { usd } => {
                let budget = to_budget(usd)?;
                if continuation_version == 7 {
                    BuySizing::DollarUpTo { budget }
                } else {
                    BuySizing::Dollar { budget }
                }
            }
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
                None,
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
                    None,
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
            if current_book_policy {
                Price::ONE
            } else {
                signal.leader_price
            },
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
                                None,
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
            Err(decline @ (LadderError::BelowBandAsk | LadderError::InsufficientDepth)) => {
                Err(GatePlanFailure {
                    reason: "price-impact ladder walk failed (fail closed)",
                    book: Box::new(book_failure(
                        Some(&token_id),
                        "ladder_rejected",
                        Some(&book.response_blake3),
                        Some(book.fetched_at_ms),
                        Some(best),
                        &format!("{decline:?}"),
                    )),
                    checked_at_unix_ms: Some(checked_at_unix_ms),
                })
            }
            Err(
                decline @ (LadderError::BelowMinimum
                | LadderError::CapExceeded
                | LadderError::NoEdge),
            ) => {
                let (outcome, reason) = match decline {
                    LadderError::BelowMinimum => (
                        "below_minimum",
                        "ladder quantity is below the venue minimum",
                    ),
                    LadderError::CapExceeded => {
                        ("cap_exceeded", "ladder debit exceeds the monetary cap")
                    }
                    _ => ("no_edge", "ladder sizing produced no allocation"),
                };
                Err(GatePlanFailure {
                    reason,
                    book: Box::new(book_failure(
                        Some(&token_id),
                        outcome,
                        Some(&book.response_blake3),
                        Some(book.fetched_at_ms),
                        Some(best),
                        &format!("{decline:?}"),
                    )),
                    checked_at_unix_ms: Some(checked_at_unix_ms),
                })
            }
            Err(
                decline @ (LadderError::Amount | LadderError::KellySizing | LadderError::Fee(_)),
            ) => Err(GatePlanFailure {
                reason: "price-impact ladder arithmetic failed (fail closed)",
                book: Box::new(book_failure(
                    Some(&token_id),
                    "arithmetic_failure",
                    Some(&book.response_blake3),
                    Some(book.fetched_at_ms),
                    Some(best),
                    &format!("{decline:?}"),
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
        continuation_version: u16,
    ) -> Result<Option<String>, ()> {
        if let Some(dispatch_id) = evidence
            .as_ref()
            .and_then(DecisionEvidenceAccumulator::staged_dispatch_id)
        {
            return Ok(Some(dispatch_id.to_owned()));
        }
        let Some(live) = self.live_accounts.as_ref() else {
            return Ok(None);
        };
        let dispatch_id = pe_strategy_winner_follow::build_idempotency_key(signal);
        match self.paper_state.dispatch_seed(&dispatch_id) {
            Ok(Some(seed)) => {
                let journal_seq = serde_json::from_str::<serde_json::Value>(&seed.signal_json)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("control_journal_seq")
                            .and_then(serde_json::Value::as_u64)
                    });
                if let Some(evidence) = evidence.as_mut() {
                    evidence.record_staged_dispatch(dispatch_id.clone(), journal_seq);
                }
                return Ok(Some(dispatch_id));
            }
            Ok(None) => {}
            Err(error) => {
                error!(%error, "read existing dispatch seed before staging failed");
                return Err(());
            }
        }
        let snapshot = live.snapshot();
        // #514: no NEW live aggregates while blind — a stale/never-successful accounts
        // snapshot stages nothing. Paper execution proceeds unchanged; in-flight recovery
        // and redemption reconciliation do not gate on freshness.
        #[cfg(feature = "scenario")]
        let accounts_at = self.financial_now();
        #[cfg(not(feature = "scenario"))]
        let accounts_at = OffsetDateTime::now_utc();
        record_clock(evidence, "live_accounts_freshness", accounts_at);
        if !snapshot.is_fresh(accounts_at.unix_timestamp()) {
            warn!(
                "live accounts snapshot is stale; dispatch staging paused (no new live aggregates)"
            );
            return Ok(None);
        }
        let selected = snapshot
            .armed_targets()
            .into_iter()
            .filter(|account| {
                snapshot.credential_metadata_available && account.credential_binding.is_some()
            })
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Ok(None);
        }
        let targets = selected
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
        #[cfg(feature = "scenario")]
        let staged_at = self.financial_now();
        #[cfg(not(feature = "scenario"))]
        let staged_at = OffsetDateTime::now_utc();
        let Some(journal) = self.live_journal.as_ref() else {
            error!(dispatch_id = %dispatch_id, "live journal is unavailable for dispatch staging");
            return Err(());
        };
        let selected_audit = selected
            .iter()
            .enumerate()
            .map(|(rank, account)| {
                let (version, key_id) = account.credential_binding.as_ref().ok_or(())?;
                Ok(
                    pe_execution_core::live_journal::LiveStagedTargetControlAudit {
                        account_id: account.account_id.clone(),
                        exec_rank: u64::try_from(rank).map_err(|_| ())?,
                        is_primary: account.is_primary,
                        execution_order: account.execution_order,
                        control: snapshot
                            .control_observation(Some(account), staged_at.unix_timestamp()),
                        observed_credential_key_id: key_id.clone(),
                        frozen_binding: pe_execution_core::CredentialBindingIdentity {
                            version: *version,
                            key_id: key_id.clone(),
                        },
                    },
                )
            })
            .collect::<Result<Vec<_>, ()>>()?;
        let staged_audit = pe_execution_core::live_journal::LiveStagedDispatchControlAudit {
            dispatch_id: dispatch_id.clone(),
            targets: selected_audit,
        };
        if !staged_audit.valid() {
            error!(dispatch_id = %dispatch_id, "selected-target control record is invalid");
            return Err(());
        }
        let Some(first_account) = staged_audit
            .targets
            .first()
            .map(|target| target.account_id.clone())
        else {
            return Ok(None);
        };
        let staged_event = journal.append(
            first_account, staged_at,
            pe_execution_core::LiveJournalPayload::StagedDispatchControl(Box::new(staged_audit)),
        ).map_err(|error| {
            error!(%error, dispatch_id = %dispatch_id, "journal selected targets before staging failed");
        })?;
        if let Some(evidence) = evidence.as_mut() {
            evidence.record_staged_dispatch(dispatch_id.clone(), Some(staged_event.seq));
        }
        // The frozen signal + decision identity: redelivery replays THIS, never current config.
        let frozen = serde_json::json!({
            "schema_version": 1,
            "signal": signal,
            "observation": observation,
            "price_impact_cap_bps": self.price_impact_cap_bps,
            "mode": format!("{:?}", self.mode),
            "control_journal_seq": staged_event.seq,
        });
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
        let checkpoint = matches!(continuation_version, 5..=7);
        let pending = if checkpoint {
            evidence
                .as_ref()
                .map(|evidence| {
                    evidence
                        .checkpoint_json()
                        .map(|json| (json, staged_at.unix_timestamp()))
                })
                .transpose()
        } else {
            render_pending_evidence(
                evidence.as_ref(),
                AuthorityEvidence::not_read("dispatch_staged_before_fill_authority"),
                TerminalDispositionEvidence::dispatch_staged(dispatch_id.clone()),
            )
        };
        let pending = match pending {
            Ok(value) => value,
            Err(error) => {
                error!(%error, dispatch_id = %dispatch_id, "encode dispatch decision evidence failed");
                return Err(());
            }
        };
        let staging = pending.as_ref().map(|pending| {
            if checkpoint {
                pe_paper_state::DispatchStagingEvidence::PaperOutcomeCheckpoint(pending_terminal(
                    pending,
                ))
            } else {
                pe_paper_state::DispatchStagingEvidence::LegacyTerminal(pending_terminal(pending))
            }
        });
        match self
            .paper_state
            .stage_dispatch_seed_pending(&record, staging)
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
        // Authenticate once for every generation-five attempt, including the first resume
        // immediately after bucket commit. Keep the history epoch on the trade for identity.
        let paper_freshness = pending
            .as_ref()
            .and_then(|continuation| {
                continuation
                    .facts
                    .paper_freshness_policy
                    .map(|policy| (continuation, policy))
            })
            .map(|(continuation, policy)| {
                let source_receipts = self
                    .source_receipts
                    .as_ref()
                    .ok_or_else(|| "paper source receipt index is missing".to_owned())?;
                let authenticated = if continuation.is_activity_frame() {
                    continuation.verify_activity_frame_with_index(source_receipts)
                } else {
                    continuation.verified_source_time(&mut |receipt| {
                        source_receipts
                            .source_envelope(receipt)
                            .map(crate::bucket_commit::CompleteActivityPage::from)
                    })
                };
                authenticated
                    .map(|(source_time, asset)| (policy, source_time, asset))
                    .map_err(|error| error.to_string())
            })
            .transpose();
        let paper_freshness = match paper_freshness {
            Ok(freshness) => freshness,
            Err(error) => {
                error!(%error, trade = %trade.source_trade_id, "authenticate paper source clock failed; stopping producer intake");
                self.intake_stopped = true;
                return;
            }
        };
        let authenticated_asset = paper_freshness
            .as_ref()
            .and_then(|(_, _, asset)| asset.clone());
        let paper_freshness = paper_freshness.map(|(policy, source_time, _)| (policy, source_time));
        let mut decision_evidence = pending
            .as_ref()
            .map(DecisionEvidenceAccumulator::for_continuation);
        if pending
            .as_ref()
            .is_some_and(|continuation| matches!(continuation.version(), 5..=7))
        {
            let restored = self
                .paper_state
                .decision_pending_for(&trade.source_trade_id)
                .map_err(|error| error.to_string())
                .and_then(|row| {
                    row.filter(|row| row.post_commit_inputs_json != "[]")
                        .map(|row| {
                            DecisionEvidenceAccumulator::resume_staging_checkpoint(&row)
                                .map_err(|error| error.to_string())
                        })
                        .transpose()
                });
            match restored {
                Ok(Some(evidence)) => decision_evidence = Some(evidence),
                Ok(None) => {}
                Err(error) => {
                    error!(%error, trade = %trade.source_trade_id, "restore dispatch checkpoint failed");
                    self.intake_stopped = true;
                    return;
                }
            }
        }
        let already_staged = decision_evidence
            .as_ref()
            .and_then(DecisionEvidenceAccumulator::staged_dispatch_id)
            .is_some();
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
            if !already_staged
                && let Some(disposition) =
                    self.continuation_stale_no_copy(&trade, stale_at, paper_freshness.as_ref())
            {
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
        let financial_semantic_version = pending
            .as_ref()
            .map_or(1, DecisionContinuationV3::financial_semantic);
        let continuation_version = pending.as_ref().map_or(0, DecisionContinuationV3::version);
        let continuation_seven = continuation_version == 7;
        let current_book_policy = matches!(financial_semantic_version, 2 | 3);
        let mut early_book = authenticated_asset.map(|token_id| {
            let fetcher = Arc::clone(&self.book_fetcher);
            let condition = condition_id.clone();
            let asset = token_id.clone();
            let deadline =
                tokio::time::Instant::now() + Duration::from_secs(CLOB_BOOK_HOT_PATH_TIMEOUT_SECS);
            EarlyBookRead {
                token_id,
                future: Box::pin(tokio::time::timeout_at(deadline, async move {
                    fetcher.fetch_book(&condition.0, &asset.0).await
                })),
                completed: None,
            }
        });
        #[cfg(all(test, feature = "scenario"))]
        let admission_wait = self
            .scenario_hooks
            .as_ref()
            .and_then(|hooks| hooks.admission_wait.lock().ok()?.take());
        let admission_read = async {
            #[cfg(all(test, feature = "scenario"))]
            if let Some(wait) = admission_wait {
                let _ = wait.await;
            }
            #[cfg(all(test, feature = "scenario"))]
            if self.scenario_hooks.as_ref().is_some_and(|hooks| {
                hooks
                    .admission_failure
                    .load(std::sync::atomic::Ordering::SeqCst)
            }) {
                return Some(Err(
                    crate::live_venue_adapter::LiveVenueAdapterError::MarketValidation(
                        "injected admission refusal".to_owned(),
                    ),
                ));
            }
            if let Some(admission) = scenario_admission {
                Some(Ok(admission))
            } else if let Some(builder) = &self.admission_builder {
                Some(if current_book_policy {
                    builder
                        .build_paper(&condition_id, OffsetDateTime::now_utc())
                        .await
                } else {
                    builder
                        .build(&condition_id, OffsetDateTime::now_utc())
                        .await
                })
            } else {
                None
            }
        };
        let admission = match early_book.as_mut() {
            Some(book) => book.during(admission_read).await,
            None => admission_read.await,
        };
        let admission = match admission {
            Some(Ok(admission)) => admission,
            Some(Err(error)) => {
                drop(early_book);
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
            None => {
                drop(early_book);
                error!(market = %signal.market_id, "active financial era has no admission builder");
                self.intake_stopped = true;
                return;
            }
        };

        if let Some(book) = early_book.as_ref()
            && admission
                .market
                .ordered_outcome_token_ids
                .get(usize::from(signal.outcome_id.0))
                != Some(&book.token_id)
        {
            let reason = "price-impact book unusable: token mismatch (fail closed)";
            if let Some(evidence) = decision_evidence.as_mut() {
                evidence.record_book(book_failure(
                    Some(&book.token_id.0),
                    "token_mismatch",
                    None,
                    None,
                    None,
                    "authenticated asset differs from admission's ordered outcome token",
                ));
            }
            drop(early_book);
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
                drop(early_book);
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
        // absent mid. Continuation 7 relies on admission and the mandatory fresh book.
        if !continuation_seven {
            let mid_read = self
                .mid_price_cache
                .fetch_mids(std::slice::from_ref(&signal.market_id));
            let mids = match early_book.as_mut() {
                Some(book) => book.during(mid_read).await,
                None => mid_read.await,
            };
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
                    drop(early_book);
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
        let early_book = match early_book {
            Some(book) => Some(book.finish().await.1),
            None => None,
        };
        let gate_evidence = match self
            .plan_impact_gate(
                &signal,
                continuation_version,
                p,
                sizing_bankroll,
                &admission,
                early_book,
            )
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
        if !continuation_seven
            && signal.leader_side == Side::Buy
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
        if !continuation_seven
            && signal.leader_side == Side::Buy
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
        // A staged generation-five decision has already passed shared admission. Its
        // paper-only freshness outcome belongs to the final Prepared boundary on resume.
        if !already_staged
            && let Some(disposition) =
                self.continuation_stale_no_copy(&trade, dispatch_stale_at, paper_freshness.as_ref())
        {
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

        let dispatch_id = match self.stage_dispatch_if_targeted(
            &signal,
            &observation,
            &mut decision_evidence,
            continuation.version(),
        ) {
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
        #[cfg(feature = "scenario")]
        if self.crash_after_frame_boundary(FrameCrashBoundary::Staging) {
            self.intake_stopped = true;
            return;
        }
        let pos_key = MarketOutcomeId::new(signal.market_id.clone(), signal.outcome_id);
        let held = !continuation_seven && self.filled_positions.contains(&pos_key);
        if held {
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
            .active_paper_risk_snapshot(
                &signal,
                planned_worst_case_all_in_debit,
                per_trade_cap_bps,
                financial_semantic_version,
            )
            .await
        {
            Ok(risk) => risk,
            Err(_) if self.intake_stopped => {
                // A risk halt append failed inside this evaluation: the halt state's durability is
                // uncertain, so the decision stays pending for the restart instead of recording a
                // decline that qualification could not replay.
                error!(trade = %trade.source_trade_id, "paper risk halt append failed; decision left pending");
                return;
            }
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
            current_book_policy,
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
        match self.strategy.evaluate_at_price_with_limit(
            &signal,
            economic.sizing.all_in_price,
            if current_book_policy {
                plan.limit_price
            } else {
                signal.leader_price
            },
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
            Ok(mut intent) => {
                if continuation_seven
                    && matches!(
                        self.strategy.config().sizing_mode,
                        SizingMode::Dollar { .. }
                    )
                {
                    intent.contracts = match pe_venue_polymarket::ladder::signed_share_contracts(
                        plan.shares,
                    ) {
                        Ok(contracts) => contracts,
                        Err(error) => {
                            error!(%error, trade = %trade.source_trade_id, "signed quantity cannot convert to whole contracts");
                            self.intake_stopped = true;
                            return;
                        }
                    };
                }
                if pending
                    .as_ref()
                    .is_none_or(|continuation| !matches!(continuation.version(), 5..=7))
                {
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
                }
                match self
                    .apply_active_financial_fill(
                        &trade,
                        economic,
                        dispatch_id.as_deref(),
                        paper_freshness.as_ref(),
                        &mut decision_evidence,
                    )
                    .await
                {
                    Ok(ActiveFinancialFill::Committed(final_receipt)) => {
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
                    Ok(ActiveFinancialFill::Expired) => {
                        self.no_fill_or_rollback(
                            &trade,
                            &leader_row,
                            dispatch_id.as_deref(),
                            "paper_stale_before_prepared",
                            &rb,
                            Some(&signal.market_id),
                            decision_evidence.as_ref(),
                        )
                        .await;
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
    /// keeps Δ at 2 s while the owner permits copies through the 120 s window. An observation
    /// from EITHER source older than the copy budget is admitted for bookkeeping — seen-state,
    /// leader ledger, and a typed disposition in ONE transaction, so the held cursor (#511)
    /// advances — but stages no copy. Strict full-`Duration` compare (#530 review F6): 120.5s old
    /// with a 120s budget IS stale; whole-second truncation would admit up to budget+1s. `now` is
    /// sampled ONCE per decision and stamps both `age_secs` and `recorded_at_unix`.
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
        let outcome = format!("no_fill:{}", disposition.reason);
        let flip = evidence
            .and_then(DecisionEvidenceAccumulator::staged_dispatch_id)
            .map(|dispatch_id| pe_paper_state::DispatchFlip {
                dispatch_id,
                paper_outcome: &outcome,
            });
        if let Err(e) = self.paper_state.commit_seen_no_copy_with_flip_pending(
            &trade.source_trade_id,
            leader,
            disposition,
            flip,
            pending.as_ref().map(pending_terminal),
        ) {
            error!(error = %e, trade = %trade.source_trade_id,
                "no-copy disposition commit failed; rolling back admission");
            self.rollback_admission(rb, unrecord_market);
        } else if flip.is_some() {
            self.notify_live_dispatch_ready();
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

    /// Terminal no-fill commits atomically with its staged dispatch handoff. Failed legacy
    /// admission rolls back; durable continuations retain consumed state for restart recovery.
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
        let dispatch_id = dispatch_id
            .or_else(|| evidence.and_then(DecisionEvidenceAccumulator::staged_dispatch_id));
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
                Ok(()) => {
                    if flip.is_some() {
                        self.notify_live_dispatch_ready();
                    }
                    return true;
                }
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
                        "paper-state no-fill terminalization failed after in-process retries; \
                         durable continuations remain pending for restart recovery, while \
                         legacy admission rolls back for redelivery"
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
    ) -> (
        crate::bucket_commit::FrozenDecisionBasis,
        crate::bucket_commit::PaperFreshnessPolicy,
    ) {
        let watchlist = self.live_watchlist.snapshot();
        let leader = aggregates
            .first()
            .map(|aggregate| TraderId(aggregate.group_id.components().wallet));
        (
            crate::bucket_commit::FrozenDecisionBasis {
                win_rate_p: leader
                    .map(|leader| self.win_rate_p_for(&watchlist, &leader))
                    .unwrap_or(Probability::ZERO),
                bankroll: self.bankroll,
            },
            crate::bucket_commit::PaperFreshnessPolicy {
                activity_ws_enabled: self.activity_ws_enabled,
                copy_latency_budget_secs: self.copy_latency_budget_secs,
            },
        )
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

    fn continuation_stale_no_copy(
        &self,
        trade: &IncomingTrade,
        now: OffsetDateTime,
        paper_freshness: Option<&(PaperFreshnessPolicy, SourceTimestamp)>,
    ) -> Option<pe_paper_state::NoCopyDisposition> {
        let Some((policy, source_time)) = paper_freshness else {
            return self.stale_no_copy(trade, now);
        };
        if !policy.expired(source_time.clone(), now) {
            return None;
        }
        let (provenance, reason) = match trade.provenance {
            TradeProvenance::RestPoll => ("rest_poll", "stale_fallback_past_copy_budget"),
            TradeProvenance::ActivityWs => ("activity_ws", "stale_activity_ws_past_copy_budget"),
        };
        Some(pe_paper_state::NoCopyDisposition {
            provenance: provenance.to_owned(),
            age_secs: (now - source_time.0).whole_seconds(),
            reason: reason.to_owned(),
            recorded_at_unix: now.unix_timestamp(),
        })
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
    use std::collections::{HashMap, HashSet};
    use std::io::{Read as _, Seek as _, Write as _};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use pe_core_types::{
        AccountId, BasisPoints, CollateralAmount, EventSeq, KellyFraction, PolymarketConditionId,
        PolymarketTokenId, ReceivedAt, SourceId, SourceTimestamp,
    };
    use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, LogError, Scanner, Writer};
    use pe_execution_core::{
        AdmissionReceipts, BalanceAudit, ECONOMIC_PREPARED_VERSION, EconomicPrepared, FeeAudit,
        LadderAskAudit, LadderPlanAudit, LiveAdmissionArtifactAudit, LiveMarketEvidenceAudit,
        MarketSelection, ObservationEvidence, RiskAudit, RiskDecisionAudit, SizingAudit,
        SizingModeAudit,
    };
    use pe_paper_state::{FillRecord, PaperStateDb};
    use pe_resolver_card::{
        VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
    };
    use pe_risk_engine::{
        ConcentrationCaps, RiskBlock, RiskDecision, RiskHaltCause, RiskSnapshot, evaluate_risk,
    };
    use pe_source_polymarket_public::FixtureFetcher;
    use pe_strategy_winner_follow::{ExecutionMode, WinnerFollowConfig, WinnerFollowStrategy};
    use pe_trader_index::Watchlist;
    use pe_venue_polymarket::CompactFeeSchedule;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use tokio::sync::{mpsc, oneshot};
    use tracing::field::{Field, Visit};
    use tracing::instrument::WithSubscriber as _;
    use tracing::{Event, Subscriber};
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::{Layer, Registry};

    use crate::paper_recovery::{
        CanonicalFillResult, CanonicalResolutionResult, ExpectedAuthority,
        FINANCIAL_SEMANTIC_VERSION, FinancialPayload, FinancialResult, PAPER_LOG_SCHEMA_VERSION,
        PaperFillOperationIdentity, PaperLogFrame, PaperLogRecord, PortfolioMark,
        QualificationSealed, QualificationStarted, RiskHaltOwner, SealReason, TailBinding,
        paper_era, scan_paper_log,
    };
    use crate::qualification::QualificationCompletion;

    use super::{Orchestrator, check_resolution_horizon, runtime_source_evidence};
    use crate::clob_book::FixtureClobBookFetcher;
    use crate::entry_gate::CopyEntryGateConfig;
    use crate::health::new_shared_health;
    use crate::live_watchlist::LiveWatchlist;
    use crate::mark_prices::HistoricalMarkAdapter;
    use crate::mid_price_cache::MidPriceCache;
    use crate::orchestrator_control::OrchestratorControl;
    use crate::risk_inputs::{SourceReceiptIndex, apply_global_risk_halts};
    use crate::supabase_state::{SourceEvidence, SupabaseStateClient};

    #[cfg(feature = "scenario")]
    mod early_book_tests {
        use super::*;
        use crate::bucket_commit::continuation_v3_tests::{binding_fixture, binding_state};
        use crate::clob_book::{BookLevel, ClobBookError, ClobBookFetcher, OrderBook};
        use pe_copy_signal_engine::TradeProvenance;
        use pe_core_types::{Price, ShareAmount};
        use pe_execution_core::LiveAdmissionArtifact;
        use pe_strategy_winner_follow::{PerTradeCap, SizingMode};
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::oneshot;

        struct Books {
            book: StdMutex<OrderBook>,
            stalled: bool,
            started: StdMutex<Vec<(String, String)>>,
            dropped: Arc<AtomicUsize>,
        }

        struct DropRead(Arc<AtomicUsize>);
        impl Drop for DropRead {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        impl ClobBookFetcher for Books {
            async fn fetch_book(
                &self,
                condition: &str,
                token: &str,
            ) -> Result<OrderBook, ClobBookError> {
                let _read = DropRead(Arc::clone(&self.dropped));
                self.started
                    .lock()
                    .unwrap()
                    .push((condition.to_owned(), token.to_owned()));
                if self.stalled {
                    std::future::pending::<()>().await;
                }
                Ok(self.book.lock().unwrap().clone())
            }
        }

        struct Harness {
            _fixture: crate::bucket_commit::continuation_v3_tests::BindingFixture,
            owner: Orchestrator<FixtureFetcher, Books>,
            trade: pe_copy_signal_engine::IncomingTrade,
            books: Arc<Books>,
            hooks: Arc<super::super::ScenarioHooks>,
            release: Option<oneshot::Sender<()>>,
        }

        impl Harness {
            fn new(provenance: TradeProvenance, authenticated: bool, stalled: bool) -> Self {
                let fixture = binding_fixture("valid");
                let state = binding_state(&fixture);
                let row = state
                    .decision_pending_for(&fixture.continuation.facts.source_trade_id)
                    .unwrap()
                    .unwrap();
                let mut continuation =
                    crate::bucket_commit::DecisionContinuationV3::from_durable(&row).unwrap();
                continuation.facts.provenance = provenance;
                if provenance == TradeProvenance::RestPoll {
                    continuation.observed_source_receipt = None;
                }
                let config = &mut continuation.facts.applied_configuration;
                config.min_resolution_horizon_secs = 0;
                config.max_resolution_horizon_secs = 0;
                config.min_fill_price = Decimal::ZERO;
                config.max_fill_price = dec!(0.85);
                config.per_trade_cap = PerTradeCap::Unlimited;
                config.sizing_mode = SizingMode::Dollar { usd: dec!(25) };
                config.sizing_dollar_usd = dec!(25);
                continuation.facts.applied_configuration_hash = config.canonical_hash();
                if !authenticated {
                    continuation.facts.paper_freshness_policy = None;
                    continuation = crate::bucket_commit::DecisionContinuationV3::new(
                        continuation.facts,
                        continuation.observed_source_receipt,
                        continuation.page_occurrences,
                        None,
                    );
                }
                rusqlite::Connection::open(fixture.dir.path().join("paper.db")).unwrap().execute(
                    "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
                    rusqlite::params![serde_json::to_string(&continuation).unwrap(), row.source_trade_id.0],
                ).unwrap();
                let now = u64::try_from(SEAL_START_UNIX * 1000).unwrap();
                let books = Arc::new(Books {
                    book: StdMutex::new(OrderBook {
                        asks: vec![BookLevel {
                            price: dec!(0.5),
                            size: dec!(1),
                        }],
                        fetched_at_ms: now,
                        response_blake3: "early-book".to_owned(),
                        source_receipt: None,
                    }),
                    stalled,
                    started: StdMutex::new(Vec::new()),
                    dropped: Arc::new(AtomicUsize::new(0)),
                });
                let (control, receiver) = mpsc::channel(4);
                drop(control);
                let writer =
                    crate::paper_recovery::PaperLog::open(fixture.dir.path().join("paper.log"))
                        .unwrap();
                let base = "https://offline.invalid";
                let mids = serde_json::to_vec(&serde_json::json!([{
                    "conditionId": "new", "outcomePrices": "[\"0.5\",\"0.5\"]", "clobTokenIds": "[\"123\",\"456\"]"
                }])).unwrap();
                let mid_fetcher = FixtureFetcher::new(HashMap::from([(
                    format!("{base}/markets?condition_ids=new&limit=500"),
                    mids,
                )]));
                let mut owner = Orchestrator::new(
                    LiveWatchlist::new(Watchlist {
                        entries: Vec::new(),
                        snapshot_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
                        active_count: 0,
                        incubator_count: 0,
                    }),
                    super::super::OrchestratorConfig {
                        bankroll: dec!(100),
                        mode: ExecutionMode::Paper,
                        signal_config: Default::default(),
                        max_resolution_horizon_secs: 0,
                        min_resolution_horizon_secs: 0,
                        max_fill_price: dec!(0.85),
                        min_fill_price: Decimal::ZERO,
                        price_impact_cap_bps: 100,
                        entry_gate_config: CopyEntryGateConfig,
                        runtime_config: None,
                        live_accounts: None,
                        live_journal: None,
                        activity_ws_enabled: false,
                        copy_latency_budget_secs: 2,
                        watchlist_writer_lock: None,
                    },
                    WinnerFollowStrategy::new(WinnerFollowConfig::default()),
                    writer,
                    state.clone(),
                    crate::paper_recovery::build_leader_ledger(&state).unwrap(),
                    new_shared_health(false),
                    MidPriceCache::with_fetcher(mid_fetcher, base.to_owned()),
                    receiver,
                    None,
                    None,
                    None,
                    books.clone(),
                )
                .unwrap();
                owner.resuming_boot = true;
                owner.source_receipts = Some(fixture.index.clone());
                owner.qualification_start = Some(fixture.metadata_receipt);
                let hooks = Arc::new(super::super::ScenarioHooks::default());
                hooks
                    .age_clock
                    .lock()
                    .unwrap()
                    .push_back(time::OffsetDateTime::from_unix_timestamp(101).unwrap());
                hooks
                    .financial_clock_unix
                    .store(SEAL_START_UNIX, Ordering::SeqCst);
                let economic =
                    seal_test_economic(fixture.metadata_receipt, fixture.metadata_receipt);
                let mut market = pe_source_polymarket_public::LiveMarketEvidence {
                    condition_id: PolymarketConditionId("new".to_owned()),
                    ordered_outcome_token_ids: [
                        PolymarketTokenId("123".to_owned()),
                        PolymarketTokenId("456".to_owned()),
                    ],
                    neg_risk: false,
                    minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                    minimum_order_size: ShareAmount::from_whole(5).unwrap(),
                    scheduled_end_unix: None,
                    observed_at_unix: SEAL_START_UNIX,
                    schema_version: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                };
                market.scheduled_end_unix =
                    Some(time::OffsetDateTime::now_utc().unix_timestamp() + 3600);
                hooks
                    .admission_artifacts
                    .lock()
                    .unwrap()
                    .push_back(LiveAdmissionArtifact {
                        market,
                        settlement: economic.admission.settlement,
                        fee_schedule: CompactFeeSchedule::Zero,
                        receipts: economic.admission.receipts,
                    });
                let (release, wait) = oneshot::channel();
                *hooks.admission_wait.lock().unwrap() = Some(wait);
                owner.set_scenario_hooks(hooks.clone());
                let trade = continuation.incoming_trade().unwrap();
                Self {
                    _fixture: fixture,
                    owner,
                    trade,
                    books,
                    hooks,
                    release: Some(release),
                }
            }

            fn change_config(
                &mut self,
                change: impl FnOnce(&mut crate::runtime_config::RuntimeConfig),
            ) {
                let continuation = self
                    .owner
                    .pending_continuations
                    .get_mut(&self.trade.source_trade_id)
                    .unwrap();
                change(&mut continuation.facts.applied_configuration);
                continuation.facts.applied_configuration_hash =
                    continuation.facts.applied_configuration.canonical_hash();
                rusqlite::Connection::open(self._fixture.dir.path().join("paper.db")).unwrap().execute(
                    "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
                    rusqlite::params![serde_json::to_string(continuation).unwrap(), self.trade.source_trade_id.0],
                ).unwrap();
            }

            fn evidence(&self) -> crate::decision_replay::DecisionPostBoundaryEvidence {
                let row = self
                    .owner
                    .paper_state
                    .decision_pending_for(&self.trade.source_trade_id)
                    .unwrap()
                    .unwrap();
                let replayed = crate::decision_replay::replay_decision_pending(&row).unwrap();
                assert_eq!(
                    replayed.continuation.facts.source_trade_id,
                    self.trade.source_trade_id
                );
                serde_json::from_str(&row.post_commit_inputs_json).unwrap()
            }
        }

        #[tokio::test(start_paused = true)]
        async fn websocket_and_rest_book_overlap_admission_and_match_ordered_token() {
            for provenance in [TradeProvenance::ActivityWs, TradeProvenance::RestPoll] {
                let mut harness = Harness::new(provenance, true, false);
                let mut decision = Box::pin(harness.owner.handle_trade_once(harness.trade.clone()));
                assert!(futures::poll!(&mut decision).is_pending());
                assert_eq!(
                    *harness.books.started.lock().unwrap(),
                    [("new".to_owned(), "123".to_owned())]
                );
                harness.release.take().unwrap().send(()).unwrap();
                decision.await;
                let evidence = harness.evidence();
                let book = evidence.body.book.unwrap();
                assert_eq!(book.request_token_id.as_deref(), Some("123"));
                assert_eq!(book.outcome, "below_minimum");
                assert_eq!(book.reason.as_deref(), Some("BelowMinimum"));
                assert_eq!(book.best_ask.as_deref(), Some("0.5"));
            }
        }

        #[tokio::test(start_paused = true)]
        async fn ladder_rejections_retain_specific_reason_and_best_ask() {
            for (floor, depth, expected) in [
                (dec!(0.6), dec!(100), "BelowBandAsk"),
                (Decimal::ZERO, dec!(100), "BelowMinimum"),
            ] {
                let mut harness = Harness::new(TradeProvenance::RestPoll, true, false);
                harness.change_config(|config| {
                    config.min_fill_price = floor;
                    config.sizing_dollar_usd = dec!(1);
                    config.sizing_mode = SizingMode::Dollar { usd: dec!(1) };
                });
                harness.books.book.lock().unwrap().asks[0].size = depth;
                harness.release.take().unwrap().send(()).unwrap();
                harness.owner.handle_trade_once(harness.trade.clone()).await;
                let book = harness.evidence().body.book.unwrap();
                assert_eq!(book.reason.as_deref(), Some(expected));
                assert_eq!(book.best_ask.as_deref(), Some("0.5"));
                assert_eq!(book.response_blake3.as_deref(), Some("early-book"));
            }
        }

        #[tokio::test(start_paused = true)]
        async fn mismatch_drops_stalled_book_before_mid_or_planning() {
            let mut harness = Harness::new(TradeProvenance::RestPoll, true, true);
            harness.hooks.admission_artifacts.lock().unwrap()[0]
                .market
                .ordered_outcome_token_ids
                .swap(0, 1);
            let mut decision = Box::pin(harness.owner.handle_trade_once(harness.trade.clone()));
            assert!(futures::poll!(&mut decision).is_pending());
            harness.release.take().unwrap().send(()).unwrap();
            assert!(futures::poll!(&mut decision).is_ready());
            drop(decision);
            assert_eq!(harness.books.dropped.load(Ordering::SeqCst), 1);
            let evidence = harness.evidence();
            assert!(evidence.body.market_price.is_none());
            assert_eq!(evidence.body.book.unwrap().outcome, "token_mismatch");
        }

        #[tokio::test(start_paused = true)]
        async fn book_age_is_checked_at_use_after_admission_waits() {
            let mut harness = Harness::new(TradeProvenance::ActivityWs, true, false);
            let mut decision = Box::pin(harness.owner.handle_trade_once(harness.trade.clone()));
            assert!(futures::poll!(&mut decision).is_pending());
            harness
                .hooks
                .financial_clock_unix
                .store(SEAL_START_UNIX + 3, Ordering::SeqCst);
            harness.release.take().unwrap().send(()).unwrap();
            decision.await;
            let evidence = harness.evidence();
            assert_eq!(evidence.body.book.unwrap().outcome, "stale");
            assert_eq!(
                evidence
                    .body
                    .clocks
                    .iter()
                    .find(|clock| clock.purpose == "book_staleness_check")
                    .unwrap()
                    .unix_millis,
                (SEAL_START_UNIX + 3) * 1000
            );
        }

        #[tokio::test(start_paused = true)]
        async fn admission_and_horizon_refusals_drop_stalled_book_immediately() {
            for admission_failure in [true, false] {
                let mut harness = Harness::new(TradeProvenance::ActivityWs, true, true);
                if admission_failure {
                    harness
                        .hooks
                        .admission_failure
                        .store(true, Ordering::SeqCst);
                } else {
                    harness.change_config(|config| config.min_resolution_horizon_secs = 60);
                    harness.hooks.admission_artifacts.lock().unwrap()[0]
                        .market
                        .scheduled_end_unix = None;
                }
                let mut decision = Box::pin(harness.owner.handle_trade_once(harness.trade.clone()));
                assert!(futures::poll!(&mut decision).is_pending());
                assert_eq!(harness.books.started.lock().unwrap().len(), 1);
                harness.release.take().unwrap().send(()).unwrap();
                assert!(futures::poll!(&mut decision).is_ready());
                drop(decision);
                assert_eq!(harness.books.dropped.load(Ordering::SeqCst), 1);
                let row = harness
                    .owner
                    .paper_state
                    .decision_pending_for(&harness.trade.source_trade_id)
                    .unwrap()
                    .unwrap();
                let evidence: crate::decision_replay::DecisionPostBoundaryEvidence =
                    serde_json::from_str(&row.post_commit_inputs_json).unwrap();
                assert!(evidence.body.book.is_none());
                assert!(evidence.body.market_price.is_none());
            }
        }

        #[tokio::test(start_paused = true)]
        async fn unauthenticated_continuation_keeps_admission_first_order() {
            let mut harness = Harness::new(TradeProvenance::RestPoll, false, false);
            let mut decision = Box::pin(harness.owner.handle_trade_once(harness.trade.clone()));
            assert!(futures::poll!(&mut decision).is_pending());
            assert!(harness.books.started.lock().unwrap().is_empty());
            harness.release.take().unwrap().send(()).unwrap();
            decision.await;
            assert_eq!(harness.books.started.lock().unwrap().len(), 1);
            assert_eq!(
                harness.evidence().body.book.unwrap().outcome,
                "ladder_rejected"
            );
        }

        #[tokio::test(start_paused = true)]
        async fn stalled_book_timeout_is_measured_from_launch() {
            let mut harness = Harness::new(TradeProvenance::RestPoll, true, true);
            let mut decision = Box::pin(harness.owner.handle_trade_once(harness.trade.clone()));
            assert!(futures::poll!(&mut decision).is_pending());
            tokio::time::advance(std::time::Duration::from_millis(1500)).await;
            harness.release.take().unwrap().send(()).unwrap();
            assert!(futures::poll!(&mut decision).is_pending());
            tokio::time::advance(std::time::Duration::from_millis(501)).await;
            assert!(futures::poll!(&mut decision).is_ready());
            drop(decision);
            assert_eq!(
                harness.evidence().body.book.unwrap().outcome,
                "fetch_timed_out"
            );
            assert_eq!(harness.books.dropped.load(Ordering::SeqCst), 1);
        }
    }

    const NOW: i64 = 1_700_000_000;
    const SEAL_START_UNIX: i64 = 1_800_057_600;

    type TestOrchestrator = Orchestrator<FixtureFetcher, FixtureClobBookFetcher>;

    fn source_input(source_id: &str, unix: i64, payload: Vec<u8>) -> EnvelopeIn {
        let at = time::OffsetDateTime::from_unix_timestamp(unix).unwrap();
        EnvelopeIn {
            source_id: SourceId(source_id.to_owned()),
            schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            content_type: ContentType::Json,
            payload,
        }
    }

    fn append_source(
        writer: &mut Writer,
        source_id: &str,
        unix: i64,
        payload: Vec<u8>,
    ) -> AppendReceipt {
        writer
            .append_synced(source_input(source_id, unix, payload))
            .unwrap()
    }

    fn append_paper(
        writer: &crate::paper_recovery::PaperLog,
        record: &PaperLogRecord,
        unix: i64,
    ) -> AppendReceipt {
        let at = time::OffsetDateTime::from_unix_timestamp(unix).unwrap();
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: PAPER_LOG_SCHEMA_VERSION,
                parser_version: 1,
                observed_at: SourceTimestamp(at),
                received_at: ReceivedAt(at),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(record).unwrap(),
            })
            .unwrap()
    }

    fn qualification_start(
        paper_prefix: TailBinding,
        source_prefix: TailBinding,
        hot_config_hash: &str,
    ) -> PaperLogRecord {
        PaperLogRecord::QualificationStarted(Arc::new(QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(dec!(1_000)).unwrap(),
            paper_prefix,
            source_prefix,
            live_prefix: TailBinding {
                physical_tail: 0,
                last_sequence: None,
                last_hash: "00".repeat(32),
            },
            artifact_blake3: "seal-test-artifact".to_owned(),
            static_config_hash: "seal-test-static".to_owned(),
            hot_config_hash: hot_config_hash.to_owned(),
            generation: "seal-test-generation".to_owned(),
            activation_id: "seal-test-activation".to_owned(),
            ranking_batch_id: 574,
            membership: Vec::new(),
            membership_proofs_hash: "seal-test-membership".to_owned(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: FINANCIAL_SEMANTIC_VERSION,
        }))
    }

    fn seal_test_economic(
        source_receipt: AppendReceipt,
        financial_prefix: AppendReceipt,
    ) -> EconomicPrepared {
        let condition = PolymarketConditionId("seal-test-condition".to_owned());
        let price = pe_core_types::Price::new(dec!(0.5)).unwrap();
        let shares = pe_core_types::ShareAmount::from_whole(1).unwrap();
        let principal = CollateralAmount::from_decimal_exact(dec!(0.5)).unwrap();
        EconomicPrepared {
            version: ECONOMIC_PREPARED_VERSION,
            market: MarketSelection {
                condition_id: condition.clone(),
                outcome_index: 0,
                token_id: PolymarketTokenId("seal-test-token-0".to_owned()),
                side: pe_core_types::Side::Buy,
                market_id: condition.0.clone(),
            },
            admission: LiveAdmissionArtifactAudit {
                market: LiveMarketEvidenceAudit {
                    condition_id: condition.clone(),
                    ordered_outcome_token_ids: [
                        PolymarketTokenId("seal-test-token-0".to_owned()),
                        PolymarketTokenId("seal-test-token-1".to_owned()),
                    ],
                    neg_risk: false,
                    minimum_tick_size: pe_core_types::Price::new(dec!(0.01)).unwrap(),
                    minimum_order_size: shares,
                    observed_at_unix: SEAL_START_UNIX,
                    schema_version: 1,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                settlement: VenueSettlementRecord {
                    schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                    condition_id: condition,
                    status: VenueResolutionStatus::Unresolved,
                    raw_evidence_hash: "seal-test-settlement".to_owned(),
                    source_timestamp_unix: Some(SEAL_START_UNIX),
                    observed_at_unix: SEAL_START_UNIX,
                    parser_version: 1,
                    freshness_window_secs: 60,
                },
                fee_schedule: CompactFeeSchedule::Zero,
                scheduled_end_unix: Some(SEAL_START_UNIX + 86_400),
                receipts: AdmissionReceipts {
                    gamma: source_receipt,
                    clob_long: source_receipt,
                    clob_compact: source_receipt,
                },
            },
            ladder: LadderPlanAudit {
                used_asks: vec![LadderAskAudit { price, shares }],
                best_ask: price,
                limit_price: price,
                minimum_shares: shares,
                principal,
            },
            book_receipt: source_receipt,
            observation: Some(ObservationEvidence {
                source_receipt,
                complete_bound_receipt: source_receipt,
                observed_unix_ms: SEAL_START_UNIX * 1_000,
                provenance: "seal_test".to_owned(),
            }),
            sizing: SizingAudit {
                mode: SizingModeAudit::Kelly {
                    fraction: KellyFraction::new(dec!(0.25)).unwrap(),
                    probability: pe_core_types::Probability::new(dec!(0.6)).unwrap(),
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
                snapshot: healthy_risk_snapshot(),
                decision: RiskDecisionAudit::Approved,
                price_receipts: Vec::new(),
                evaluated_at_unix_ms: SEAL_START_UNIX * 1_000,
            },
            balance: BalanceAudit {
                cash_before: CollateralAmount::from_decimal_exact(dec!(1_000)).unwrap(),
                worst_case_debit: principal,
                price_impact_cap_bps: 100,
                chase_ceiling: price,
                band_floor: pe_core_types::Price::ZERO,
                band_ceiling_exclusive: pe_core_types::Price::ONE,
            },
            applied_configuration_hash: "seal-test-config".to_owned(),
        }
    }

    struct StartedSealFixture {
        _dir: tempfile::TempDir,
        paper_path: PathBuf,
        source_path: PathBuf,
        state: Arc<PaperStateDb>,
        paper_writer: crate::paper_recovery::PaperLog,
        start: AppendReceipt,
    }

    fn started_seal_fixture(hot_config_hash: &str) -> StartedSealFixture {
        started_seal_fixture_with_semantic(hot_config_hash, FINANCIAL_SEMANTIC_VERSION)
    }

    fn started_seal_fixture_with_semantic(
        hot_config_hash: &str,
        financial_semantic_version: u32,
    ) -> StartedSealFixture {
        let dir = tempfile::tempdir().unwrap();
        let paper_path = dir.path().join("paper.log");
        let source_path = dir.path().join("source.log");
        drop(Writer::open(&source_path).unwrap());
        drop(pe_execution_core::LiveJournal::open(dir.path().join("live_journal.log")).unwrap());
        let paper_writer = crate::paper_recovery::PaperLog::open(&paper_path).unwrap();
        let mut started = qualification_start(
            TailBinding::from(&Scanner::verify(&paper_path).unwrap()),
            TailBinding::from(&Scanner::verify(&source_path).unwrap()),
            hot_config_hash,
        );
        if let PaperLogRecord::QualificationStarted(start) = &mut started {
            Arc::make_mut(start).financial_semantic_version = financial_semantic_version;
        }
        let start = append_paper(&paper_writer, &started, SEAL_START_UNIX);
        let state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        state
            .reset_financial_era(
                start,
                CollateralAmount::from_decimal_exact(dec!(1_000)).unwrap(),
            )
            .unwrap();
        StartedSealFixture {
            _dir: dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            start,
        }
    }

    fn test_orchestrator(
        paper_path: PathBuf,
        source_path: PathBuf,
        paper_writer: crate::paper_recovery::PaperLog,
        state: Arc<PaperStateDb>,
        source_receipts: SourceReceiptIndex,
    ) -> TestOrchestrator {
        let (_control_tx, control_rx) = mpsc::channel(4);
        let mut orchestrator = build_test_orchestrator(
            paper_writer,
            Arc::clone(&state),
            control_rx,
            String::new(),
            Some(SupabaseStateClient::new(
                reqwest::Client::new(),
                "https://offline.invalid",
                "seal-test-anon",
                "seal-test-secret",
            )),
        );
        let (source_log, _source_rx) = crate::activity_ingest::SourceLogHandle::channel(1);
        orchestrator.boundary_mark_fetcher = Some(Arc::new(HistoricalMarkAdapter::new(
            reqwest::Client::new(),
            "https://offline.invalid",
            source_log,
        )));
        orchestrator.financial_log_paths = Some((paper_path, source_path));
        orchestrator.source_receipts = Some(source_receipts);
        orchestrator
    }

    fn build_test_orchestrator(
        paper_writer: crate::paper_recovery::PaperLog,
        state: Arc<PaperStateDb>,
        control_rx: mpsc::Receiver<OrchestratorControl>,
        mid_price_base_url: String,
        supabase_state: Option<SupabaseStateClient>,
    ) -> TestOrchestrator {
        Orchestrator::new(
            LiveWatchlist::new(Watchlist {
                entries: Vec::new(),
                snapshot_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
                active_count: 0,
                incubator_count: 0,
            }),
            super::OrchestratorConfig {
                bankroll: dec!(1_000),
                mode: ExecutionMode::Paper,
                signal_config: Default::default(),
                max_resolution_horizon_secs: 0,
                min_resolution_horizon_secs: 0,
                max_fill_price: Decimal::ZERO,
                min_fill_price: Decimal::ZERO,
                price_impact_cap_bps: 100,
                entry_gate_config: CopyEntryGateConfig,
                runtime_config: None,
                live_accounts: None,
                live_journal: None,
                activity_ws_enabled: false,
                copy_latency_budget_secs: 2,
                watchlist_writer_lock: None,
            },
            WinnerFollowStrategy::new(WinnerFollowConfig::default()),
            paper_writer,
            Arc::clone(&state),
            crate::paper_recovery::build_leader_ledger(&state).unwrap(),
            new_shared_health(false),
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), mid_price_base_url),
            control_rx,
            None,
            None,
            supabase_state,
            Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
        )
        .unwrap()
    }

    #[derive(Debug, Default)]
    struct RecordedSealEvent {
        message: String,
        debug: HashMap<String, String>,
        unsigned: HashMap<String, u64>,
    }

    impl Visit for RecordedSealEvent {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            let rendered = format!("{value:?}");
            if field.name() == "message" {
                self.message = rendered.trim_matches('"').to_owned();
            } else {
                self.debug.insert(field.name().to_owned(), rendered);
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "message" {
                self.message = value.to_owned();
            } else {
                self.debug.insert(field.name().to_owned(), value.to_owned());
            }
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.unsigned.insert(field.name().to_owned(), value);
        }
    }

    #[derive(Clone)]
    struct SealEventLayer {
        events: Arc<StdMutex<Vec<RecordedSealEvent>>>,
        append_on_start: Option<(PathBuf, Arc<AtomicBool>)>,
    }

    impl<S> Layer<S> for SealEventLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            let mut recorded = RecordedSealEvent::default();
            event.record(&mut recorded);
            if recorded.message == "qualification seal started"
                && let Some((path, once)) = &self.append_on_start
                && once.swap(false, Ordering::SeqCst)
            {
                let mut writer = Writer::open(path).unwrap();
                append_source(
                    &mut writer,
                    crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
                    SEAL_START_UNIX + 10,
                    br#"{"kind":"after-seal-start"}"#.to_vec(),
                );
            }
            self.events
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(recorded);
        }
    }

    fn sealed_records(path: &Path) -> Vec<QualificationSealed> {
        paper_era(scan_paper_log(path).unwrap())
            .frames
            .into_iter()
            .filter_map(|frame| match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::QualificationSealed(seal)) => {
                    Some(seal.as_ref().clone())
                }
                _ => None,
            })
            .collect()
    }

    async fn apply_seal_check_control(
        orchestrator: &mut TestOrchestrator,
        proposed_hash: &str,
    ) -> Result<(), String> {
        let (acknowledged, response) = oneshot::channel();
        orchestrator
            .apply_control_message(OrchestratorControl::SealCheck {
                proposed_economic_hash: proposed_hash.to_owned(),
                proposed_financial_semantic_version: FINANCIAL_SEMANTIC_VERSION,
                acknowledged,
            })
            .await;
        response.await.expect("SealCheck acknowledgement")
    }

    /// Bytes requested through read syscalls, including page-cache hits.
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

    #[cfg(target_os = "linux")]
    fn filler_payload(seed: u64) -> Vec<u8> {
        let mut value = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        (0..1_400)
            .map(|_| {
                value ^= value << 13;
                value ^= value >> 7;
                value ^= value << 17;
                value.to_le_bytes()[0]
            })
            .collect()
    }

    struct ContinuationOrchestratorFixture {
        dir: tempfile::TempDir,
        paper_state: Arc<PaperStateDb>,
        rows: Vec<pe_paper_state::DecisionPendingRow>,
        orchestrator: TestOrchestrator,
        _control_tx: mpsc::Sender<OrchestratorControl>,
    }

    /// Producer-created continuations with an orchestrator that has no boot-owned rows.
    fn continuation_orchestrator_fixture() -> ContinuationOrchestratorFixture {
        let (dir, paper_state, _index) =
            crate::bucket_commit::continuation_validation_tests::producer_fixture();
        let rows = paper_state.open_decision_pending().unwrap();
        let (control_tx, control_rx) = mpsc::channel(2);
        let mut orchestrator = build_test_orchestrator(
            crate::paper_recovery::PaperLog::open(dir.path().join("paper.log")).unwrap(),
            Arc::clone(&paper_state),
            control_rx,
            "https://gamma.test".to_owned(),
            None,
        );
        orchestrator.pending_boot.clear();
        orchestrator.pending_continuations.clear();
        ContinuationOrchestratorFixture {
            dir,
            paper_state,
            rows,
            orchestrator,
            _control_tx: control_tx,
        }
    }

    /// A ready handoff wakes only after commit; failed flips retain the open seed and no permit.
    #[tokio::test(start_paused = true)]
    async fn live_dispatch_ready_no_copy_and_no_fill_follow_successful_commits() {
        use super::{DecisionContinuationV3, DecisionEvidenceAccumulator, RollbackCtx};
        use pe_core_types::{MarketOutcomeId, ShareAmount};

        for no_copy in [false, true] {
            let ContinuationOrchestratorFixture {
                dir,
                paper_state,
                rows,
                mut orchestrator,
                _control_tx,
            } = continuation_orchestrator_fixture();
            let continuation = DecisionContinuationV3::from_durable(&rows[0]).unwrap();
            let trade = continuation.incoming_trade().unwrap();
            let ready = Arc::new(tokio::sync::Notify::new());
            orchestrator = orchestrator.with_live_dispatch_ready(ready.clone());
            let mut evidence = DecisionEvidenceAccumulator::for_continuation(&continuation);
            evidence.record_staged_dispatch("wake-test".to_owned(), None);
            let checkpoint = evidence.checkpoint_json().unwrap();
            paper_state
                .stage_dispatch_seed_pending(
                    &pe_paper_state::DispatchSeedRecord {
                        dispatch_id: "wake-test".to_owned(),
                        signal_json: "{}".to_owned(),
                        source_trade_id: trade.source_trade_id.0.clone(),
                        created_at_unix: trade.received_at.unix_timestamp(),
                        targets: Vec::new(),
                    },
                    Some(
                        pe_paper_state::DispatchStagingEvidence::PaperOutcomeCheckpoint(
                            pe_paper_state::PendingTerminalEvidence {
                                post_commit_inputs_json: &checkpoint,
                                updated_at_unix: trade.received_at.unix_timestamp(),
                            },
                        ),
                    ),
                )
                .unwrap();
            let leader = pe_paper_state::LeaderPositionRow {
                wallet: trade.wallet,
                market_id: trade.market_id.clone(),
                outcome_id: trade.outcome_id,
                long_contracts: ShareAmount::ZERO,
                short_contracts: ShareAmount::ZERO,
            };
            let rollback = RollbackCtx {
                wallet: trade.wallet,
                key: MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id),
                prev: None,
                enabled: false,
            };
            let disposition = pe_paper_state::NoCopyDisposition {
                provenance: "rest_poll".to_owned(),
                age_secs: 121,
                reason: "stale_fallback_past_copy_budget".to_owned(),
                recorded_at_unix: trade.received_at.unix_timestamp(),
            };
            let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
            connection.execute_batch("CREATE TRIGGER refuse_ready BEFORE UPDATE ON dispatch_seeds BEGIN SELECT RAISE(ABORT, 'injected ready failure'); END;").unwrap();
            for succeeds in [false, true] {
                if succeeds {
                    connection
                        .execute_batch("DROP TRIGGER refuse_ready;")
                        .unwrap();
                }
                if no_copy {
                    orchestrator.commit_no_copy_or_rollback(
                        &trade,
                        &leader,
                        &disposition,
                        &rollback,
                        None,
                        Some(&evidence),
                    );
                } else {
                    assert_eq!(
                        orchestrator
                            .commit_no_fill_flipping(
                                &trade,
                                &leader,
                                Some("wake-test"),
                                "declined",
                                None,
                                Some(&evidence),
                            )
                            .await,
                        succeeds
                    );
                }
                assert_eq!(
                    paper_state
                        .dispatch_seed("wake-test")
                        .unwrap()
                        .unwrap()
                        .state,
                    if succeeds { "ready" } else { "pending_paper" }
                );
                let notified = ready.notified();
                tokio::pin!(notified);
                assert_eq!(futures::poll!(&mut notified).is_ready(), succeeds);
            }
        }
    }

    /// PASS: once paper durability is uncertain, resuming a committed bucket loads and handles no
    /// further row: every row stays open with its frozen inputs and no continuation is registered.
    /// FAIL: a row is loaded, resumed, or terminalized after the latch.
    #[tokio::test]
    async fn resume_committed_rows_stops_once_paper_durability_is_uncertain() {
        let ContinuationOrchestratorFixture {
            dir: _dir,
            paper_state,
            rows: before,
            mut orchestrator,
            _control_tx,
        } = continuation_orchestrator_fixture();
        let ids = before
            .iter()
            .map(|row| row.source_trade_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(ids.len(), 2);
        orchestrator.intake_stopped = true;

        orchestrator.resume_committed_rows(&ids).await.unwrap();

        assert!(orchestrator.pending_continuations.is_empty());
        let after = paper_state.open_decision_pending().unwrap();
        assert_eq!(after.len(), 2);
        for (row, expected) in after.iter().zip(&before) {
            assert_eq!(row.source_trade_id, expected.source_trade_id);
            assert_eq!(row.state, pe_paper_state::DecisionPendingState::Open);
            assert_eq!(row.frozen_inputs_json, expected.frozen_inputs_json);
        }
    }

    /// PASS: keyed recovery ignores a corrupt unrelated open row, rebuilds/caches only the requested
    /// open row, skips missing/terminal rows, and reports the requested identity on a load error.
    /// FAIL: recovery scans all open rows, resumes terminal rows, or hides decode/read failures.
    #[tokio::test]
    async fn load_pending_continuation_uses_single_row_lookup_and_resumes_only_open_rows() {
        use crate::bucket_commit::DecisionContinuationV3;

        let ContinuationOrchestratorFixture {
            dir,
            paper_state,
            rows,
            mut orchestrator,
            _control_tx,
        } = continuation_orchestrator_fixture();
        let target = &rows[1];

        // A full scan now fails at the durable row parser, before any JSON is decoded.
        let conn = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
        conn.execute(
            "UPDATE decision_pending SET wallet_hex = 'invalid-wallet' WHERE source_trade_id = ?1",
            [&rows[0].source_trade_id.0],
        )
        .unwrap();
        assert!(paper_state.open_decision_pending().is_err());
        let missing = pe_core_types::SourceTradeId("missing".to_owned());
        assert!(
            orchestrator
                .load_pending_continuation(&missing)
                .unwrap()
                .is_none()
        );
        let expected = DecisionContinuationV3::from_durable(target).unwrap();
        let trade = orchestrator
            .load_pending_continuation(&target.source_trade_id)
            .unwrap()
            .unwrap();
        assert_eq!(trade.source_trade_id, target.source_trade_id);
        assert_eq!(trade.price, expected.facts.price);
        assert_eq!(trade.contracts, expected.facts.share_amount);
        assert_eq!(
            orchestrator
                .pending_continuations
                .get(&target.source_trade_id),
            Some(&expected)
        );
        assert_eq!(orchestrator.pending_continuations.len(), 1);

        orchestrator.pending_continuations.clear();
        paper_state
            .close_decision_pending(&target.source_trade_id, "{}", "test_terminal", NOW + 20)
            .unwrap();
        conn.execute(
            "UPDATE decision_pending SET frozen_inputs_json = '{}' WHERE source_trade_id = ?1",
            [&target.source_trade_id.0],
        )
        .unwrap();
        assert!(
            orchestrator
                .load_pending_continuation(&target.source_trade_id)
                .unwrap()
                .is_none()
        );
        assert!(orchestrator.pending_continuations.is_empty());

        conn.execute("UPDATE decision_pending SET state = 'open', terminal_disposition = NULL WHERE source_trade_id = ?1",
            [&target.source_trade_id.0]).unwrap();
        let error = orchestrator
            .load_pending_continuation(&target.source_trade_id)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("decode pending {}", target.source_trade_id))
        );
        let error = orchestrator
            .load_pending_continuation(&rows[0].source_trade_id)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("read pending {}", rows[0].source_trade_id))
        );
        assert!(orchestrator.pending_continuations.is_empty());
    }

    /// Active financial work requires the installed receipt index before writing Prepared.
    #[test]
    fn active_financial_runtime_selects_the_installed_source_index() {
        let index = SourceReceiptIndex::default();
        assert!(matches!(
            runtime_source_evidence(Some(&index)),
            Ok(SourceEvidence::Index(selected)) if std::ptr::eq(selected, &index)
        ));
        assert!(runtime_source_evidence(None).is_err());
    }

    #[test]
    fn financial_log_install_rejects_index_for_another_path_before_configuration() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            ..
        } = started_seal_fixture("start-hash");
        let other_path = paper_path.with_file_name("other-source.log");
        drop(Writer::open(&other_path).unwrap());
        let other_index = SourceReceiptIndex::replay(&other_path).unwrap();
        let (_control_tx, control_rx) = mpsc::channel(4);
        let mut orchestrator =
            build_test_orchestrator(paper_writer, state, control_rx, String::new(), None)
                .with_source_receipt_index(other_index.clone());
        let (source_log, _source_rx) = crate::activity_ingest::SourceLogHandle::channel(1);
        let result = orchestrator.configure_financial_log_paths(
            paper_path,
            source_path,
            crate::live_venue_adapter::LiveAdmissionBuilder::new(
                reqwest::Client::new(),
                "https://offline.invalid",
                "https://offline.invalid",
                source_log.clone(),
            ),
            Arc::new(HistoricalMarkAdapter::new(
                reqwest::Client::new(),
                "https://offline.invalid",
                source_log,
            )),
            other_index,
        );
        assert!(matches!(
            result,
            Err(crate::paper_recovery::PaperLogScanError::SourceIndexPathMismatch)
        ));
        assert!(orchestrator.financial_log_paths.is_none());
        assert!(orchestrator.admission_builder.is_none());
        assert!(orchestrator.boundary_mark_fetcher.is_none());
    }

    #[tokio::test]
    async fn missing_runtime_index_refuses_fill_before_prepared_or_authority() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            start,
        } = started_seal_fixture("start-hash");
        let source_receipt = AppendReceipt {
            sequence: EventSeq(0),
            this_hash: blake3::Hash::from_bytes([1; 32]),
        };
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path.clone(),
            paper_writer,
            state,
            SourceReceiptIndex::replay(&source_path).unwrap(),
        );
        let calls = orchestrator.supabase_state.as_ref().unwrap().call_counter();
        orchestrator.source_receipts = None;
        let at = time::OffsetDateTime::from_unix_timestamp(SEAL_START_UNIX).unwrap();
        let trade = pe_copy_signal_engine::IncomingTrade {
            wallet: pe_core_types::WalletAddress::from_hex(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )
            .unwrap(),
            market_id: pe_core_types::MarketId(pe_core_types::VenueMarketId(
                "seal-test-condition".to_owned(),
            )),
            outcome_id: pe_core_types::OutcomeId(0),
            side: pe_core_types::Side::Buy,
            price: pe_core_types::Price::new(dec!(0.5)).unwrap(),
            contracts: pe_core_types::ShareAmount::from_whole(1).unwrap(),
            observed_at: at,
            received_at: at,
            source_trade_id: pe_core_types::SourceTradeId("missing-index".to_owned()),
            transaction_hash: None,
            provenance: pe_copy_signal_engine::TradeProvenance::default(),
        };
        let error = orchestrator
            .apply_active_financial_fill(
                &trade,
                seal_test_economic(source_receipt, start),
                None,
                None,
                &mut None,
            )
            .await
            .err()
            .unwrap();
        assert!(error.contains("source receipt index is unavailable"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            !scan_paper_log(&paper_path)
                .unwrap()
                .iter()
                .any(|frame| matches!(
                    frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { .. })
                ))
        );
    }

    #[tokio::test]
    async fn daily_mark_rejects_unconfirmed_index_tail_without_appending() {
        for stray_bytes in [true, false] {
            let StartedSealFixture {
                _dir,
                paper_path,
                source_path,
                state,
                paper_writer,
                ..
            } = started_seal_fixture("start-hash");
            let cutoff = SEAL_START_UNIX + 86_400;
            let mut source_writer = Writer::open(&source_path).unwrap();
            let boundary = append_source(
                &mut source_writer,
                crate::trade_poller::DAILY_BOUNDARY_SOURCE_ID,
                cutoff,
                serde_json::to_vec(&serde_json::json!({
                    "kind": "daily_boundary",
                    "cutoff_unix": cutoff,
                }))
                .unwrap(),
            );
            drop(source_writer);
            let index = if stray_bytes {
                let mut staging = SourceReceiptIndex::staging(&source_path).unwrap();
                for item in pe_event_log::Reader::replay_with_offsets(&source_path).unwrap() {
                    let (offset, _, envelope) = item.unwrap();
                    staging.observe(offset, &envelope).unwrap();
                }
                let mut binding = Scanner::verify(&source_path).unwrap();
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&source_path)
                    .unwrap()
                    .write_all(b"stray bytes")
                    .unwrap();
                binding.physical_tail = std::fs::metadata(&source_path).unwrap().len();
                staging.complete(&binding).unwrap()
            } else {
                // Index the final frame under a different hash: its offset and sequence still
                // match the log, so only the hash comparison can reject the tail.
                let mut staging = SourceReceiptIndex::staging(&source_path).unwrap();
                let mut binding = Scanner::verify(&source_path).unwrap();
                for item in pe_event_log::Reader::replay_with_offsets(&source_path).unwrap() {
                    let (offset, _, mut envelope) = item.unwrap();
                    if Some(envelope.seq) == binding.last_sequence {
                        envelope.this_hash = blake3::Hash::from_bytes([7; 32]);
                        binding.last_hash = envelope.this_hash;
                    }
                    staging.observe(offset, &envelope).unwrap();
                }
                let index = staging.complete(&binding).unwrap();
                assert_eq!(
                    std::fs::metadata(&source_path).unwrap().len(),
                    binding.physical_tail
                );
                index
            };
            let mut orchestrator =
                test_orchestrator(paper_path.clone(), source_path, paper_writer, state, index);
            assert!(
                orchestrator
                    .mark_at_boundary(cutoff, boundary)
                    .await
                    .is_err()
            );
            assert!(
                !scan_paper_log(&paper_path)
                    .unwrap()
                    .iter()
                    .any(|frame| matches!(
                        frame.frame,
                        PaperLogFrame::Record(PaperLogRecord::PortfolioMark(_))
                    ))
            );
        }
    }

    #[derive(Clone, Default)]
    struct GapFillAuthority {
        calls: Arc<std::sync::atomic::AtomicUsize>,
        mutations: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl crate::supabase_state::SupabaseStateTrait for GapFillAuthority {
        async fn commit_fill_v2(
            &self,
            _: &crate::supabase_sink::SupabaseFillRow,
        ) -> Result<crate::supabase_state::FillV2Outcome, crate::supabase_state::SupabaseStateError>
        {
            Err(crate::supabase_state::SupabaseStateError::Corrupt(
                "legacy authority route is outside this fixture".to_owned(),
            ))
        }

        async fn commit_prepared_fill(
            &self,
            request: &crate::supabase_state::PreparedFillRequest,
        ) -> Result<CanonicalFillResult, crate::supabase_state::SupabaseStateError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                self.mutations.fetch_add(1, Ordering::SeqCst);
            }
            Ok(CanonicalFillResult {
                outcome: if call == 0 { "applied" } else { "existing" }.to_owned(),
                bankroll: dec!(999.5),
                applied_prepared_seq: request.prepared_receipt.sequence,
                quantity: request.quantity,
                principal: request.principal,
                fee: request.fee,
                fill_price: request.fill_price,
            })
        }

        async fn apply_prepared_resolution(
            &self,
            request: &crate::supabase_state::PreparedResolutionRequest,
        ) -> Result<CanonicalResolutionResult, crate::supabase_state::SupabaseStateError> {
            Ok(CanonicalResolutionResult {
                outcome: "applied".to_owned(),
                bankroll: dec!(1000.5),
                applied_prepared_seq: request.prepared_receipt.sequence,
                credit: CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                settled_at_unix: request.settled_at_unix,
            })
        }
    }

    #[cfg(feature = "scenario")]
    #[tokio::test]
    async fn financial_risk_fill_and_resolution_use_shared_paper_frames_without_scans() {
        use super::*;
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            start,
        } = started_seal_fixture("start-hash");
        let mut source_writer = Writer::open(&source_path).unwrap();
        let fill_source = append_source(
            &mut source_writer,
            "seal-test-financial-source",
            SEAL_START_UNIX + 1,
            b"{}".to_vec(),
        );
        let resolution_source = append_source(&mut source_writer, "polymarket.clob.market", SEAL_START_UNIX + 3,
            br#"{"condition_id":"seal-test-condition","closed":true,"is_50_50_outcome":false,"tokens":[{"token_id":"yes","outcome":"Yes","price":1,"winner":true},{"token_id":"no","outcome":"No","price":0,"winner":false}]}"#.to_vec());
        drop(source_writer);
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(SEAL_START_UNIX + 1).unwrap();
        let (_tx, rx) = mpsc::channel(1);
        let authority = GapFillAuthority::default();
        let mut owner = Orchestrator::new_inner(
            LiveWatchlist::new(Watchlist {
                entries: Vec::new(),
                snapshot_at: SourceTimestamp(now),
                active_count: 0,
                incubator_count: 0,
            }),
            OrchestratorConfig {
                bankroll: dec!(1000),
                mode: ExecutionMode::Paper,
                signal_config: Default::default(),
                max_resolution_horizon_secs: 0,
                min_resolution_horizon_secs: 0,
                max_fill_price: Decimal::ZERO,
                min_fill_price: Decimal::ZERO,
                price_impact_cap_bps: 100,
                entry_gate_config: CopyEntryGateConfig,
                runtime_config: None,
                live_accounts: None,
                live_journal: None,
                activity_ws_enabled: false,
                copy_latency_budget_secs: 2,
                watchlist_writer_lock: None,
            },
            WinnerFollowStrategy::new(WinnerFollowConfig::default()),
            paper_writer.clone(),
            state.clone(),
            crate::paper_recovery::build_leader_ledger(&state).unwrap(),
            new_shared_health(false),
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), String::new())
                .with_clock(Arc::new(move || now)),
            rx,
            None,
            None,
            Some(authority),
            Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
        )
        .unwrap();
        owner.financial_log_paths = Some((paper_path.clone(), source_path));
        owner.source_receipts = Some(index);
        let hooks = Arc::new(ScenarioHooks::default());
        hooks
            .financial_clock_unix
            .store(SEAL_START_UNIX + 2, Ordering::SeqCst);
        owner.scenario_hooks = Some(hooks.clone());
        let trade = IncomingTrade {
            wallet: WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            market_id: MarketId(pe_core_types::VenueMarketId(
                "seal-test-condition".to_owned(),
            )),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            price: Price::new(dec!(0.5)).unwrap(),
            contracts: ShareAmount::from_whole(1).unwrap(),
            observed_at: now,
            received_at: now,
            source_trade_id: SourceTradeId(format!("g2:{}", "a".repeat(64))),
            transaction_hash: None,
            provenance: TradeProvenance::RestPoll,
        };
        let signal = LeaderSignal {
            leader: TraderId(trade.wallet),
            venue: VenueId::polymarket(),
            market_id: trade.market_id.clone(),
            outcome_id: trade.outcome_id,
            action: pe_core_types::LeaderAction::Entry,
            leader_side: Side::Buy,
            leader_price: trade.price,
            leader_size: trade.contracts,
            observed_at: now,
            received_at: now,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: trade.source_trade_id.clone(),
            action_confidence_ppm: pe_core_types::ProbabilityPpm(1_000_000),
        };
        let ready = Arc::new(tokio::sync::Notify::new());
        owner = owner.with_live_dispatch_ready(ready.clone());
        state
            .stage_dispatch_seed(&pe_paper_state::DispatchSeedRecord {
                dispatch_id: "fill-wake".to_owned(),
                signal_json: "{}".to_owned(),
                source_trade_id: trade.source_trade_id.0.clone(),
                created_at_unix: now.unix_timestamp(),
                targets: Vec::new(),
            })
            .unwrap();
        let paper_walks = pe_event_log::scan_metrics::count(&paper_path).unwrap();
        let (risk, _) = owner
            .active_paper_risk_snapshot(
                &signal,
                CollateralAmount::ZERO,
                10_000,
                FINANCIAL_SEMANTIC_VERSION,
            )
            .await
            .map_err(|error| error.cause)
            .unwrap();
        assert_eq!(risk.decision, RiskDecisionAudit::Approved);
        assert!(matches!(
            owner
                .apply_active_financial_fill(
                    &trade,
                    seal_test_economic(fill_source, start),
                    Some("fill-wake"),
                    None,
                    &mut None
                )
                .await
                .unwrap(),
            ActiveFinancialFill::Committed(_)
        ));
        assert_eq!(
            state.dispatch_seed("fill-wake").unwrap().unwrap().state,
            "ready"
        );
        let notified = ready.notified();
        tokio::pin!(notified);
        assert!(futures::poll!(&mut notified).is_ready());
        hooks
            .financial_clock_unix
            .store(SEAL_START_UNIX + 4, Ordering::SeqCst);
        owner
            .apply_resolution_candidate(
                PolymarketConditionId("seal-test-condition".to_owned()),
                "[\"1\",\"0\"]".to_owned(),
                resolution_source,
            )
            .await
            .unwrap();
        assert_eq!(
            state.financial_snapshot(SEAL_START_UNIX + 4).unwrap().cash,
            dec!(1000.5)
        );
        assert_eq!(
            pe_event_log::scan_metrics::count(&paper_path).unwrap(),
            paper_walks
        );
        let cached = paper_writer.snapshot().unwrap();
        let scanned = scan_paper_log(&paper_path).unwrap();
        assert_eq!(cached.len(), scanned.len());
        for (cached, scanned) in cached.iter().zip(scanned) {
            assert_eq!(cached.envelope, scanned.envelope);
        }
    }

    #[tokio::test]
    async fn index_gap_redrive_rebuilds_receipt_without_duplicate_authority_mutation() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            start,
        } = started_seal_fixture("start-hash");
        let stale_index = SourceReceiptIndex::replay(&source_path).unwrap();
        let mut source_writer = Writer::open(&source_path).unwrap();
        let source_receipt = append_source(
            &mut source_writer,
            "seal-test-financial-source",
            SEAL_START_UNIX + 1,
            b"synchronized-but-unindexed".to_vec(),
        );
        drop(source_writer);
        append_paper(
            &paper_writer,
            &PaperLogRecord::FinancialPrepared {
                expected_authority: ExpectedAuthority {
                    qualification_start_receipt: start,
                    prior_completed_prepared_sequence: None,
                },
                payload: FinancialPayload::Fill {
                    operation: PaperFillOperationIdentity {
                        leader_wallet: pe_core_types::WalletAddress::from_hex(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        )
                        .unwrap(),
                        source_trade_id: pe_core_types::SourceTradeId("gap-fill".to_owned()),
                        observed_at_bucket: SEAL_START_UNIX,
                    },
                    economic: seal_test_economic(source_receipt, start),
                },
            },
            SEAL_START_UNIX + 1,
        );
        let authority = GapFillAuthority::default();
        let error = crate::supabase_state::reconcile_active_financial_frames(
            &authority,
            &state,
            SourceEvidence::Index(&stale_index),
            &paper_writer,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("absent from the source receipt index")
        );
        assert_eq!(authority.calls.load(Ordering::SeqCst), 1);
        assert_eq!(authority.mutations.load(Ordering::SeqCst), 1);
        assert!(
            !scan_paper_log(&paper_path)
                .unwrap()
                .iter()
                .any(|frame| matches!(
                    frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialFinal { .. })
                ))
        );
        drop(paper_writer);

        // Boot rebuilds this same index (scenario_source_log_boot) and redrives through this call.
        let rebuilt_index = SourceReceiptIndex::replay(&source_path).unwrap();
        let reopened = crate::paper_recovery::PaperLog::open(&paper_path).unwrap();
        assert_eq!(
            crate::supabase_state::reconcile_active_financial_frames(
                &authority,
                &state,
                SourceEvidence::Index(&rebuilt_index),
                &reopened,
            )
            .await
            .unwrap(),
            1
        );
        assert_eq!(authority.calls.load(Ordering::SeqCst), 2);
        assert_eq!(authority.mutations.load(Ordering::SeqCst), 1);
        assert_eq!(
            scan_paper_log(&paper_path)
                .unwrap()
                .iter()
                .filter(|frame| matches!(
                    frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialFinal { .. })
                ))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn boot_semantic_seal_is_durable_and_idempotent_before_resume() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            ..
        } = started_seal_fixture_with_semantic("start-hash", 1);
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path.clone(),
            paper_writer,
            Arc::clone(&state),
            source_receipts,
        );
        let paper_walks = pe_event_log::scan_metrics::count(&paper_path).unwrap();
        let financial_prefix = orchestrator.paper_writer.verified_tail().unwrap();
        orchestrator
            .seal_before_resume("start-hash", FINANCIAL_SEMANTIC_VERSION)
            .await
            .unwrap();
        assert_eq!(
            pe_event_log::scan_metrics::count(&paper_path).unwrap(),
            paper_walks
        );
        let seals = sealed_records(&paper_path);
        assert_eq!(seals.len(), 1);
        assert_eq!(
            seals[0].financial_prefix,
            TailBinding::from(&financial_prefix)
        );
        assert!(matches!(
            seals[0].reason,
            SealReason::InsufficientEvidence(_)
        ));
        orchestrator
            .seal_before_resume("start-hash", FINANCIAL_SEMANTIC_VERSION)
            .await
            .unwrap();
        assert_eq!(sealed_records(&paper_path), seals);
        drop(orchestrator);
        let paper_writer = crate::paper_recovery::PaperLog::open(&paper_path).unwrap();
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let mut restarted = test_orchestrator(
            paper_path.clone(),
            source_path,
            paper_writer,
            state,
            source_receipts,
        );
        restarted
            .seal_before_resume("start-hash", FINANCIAL_SEMANTIC_VERSION)
            .await
            .unwrap();
        assert_eq!(sealed_records(&paper_path), seals);
    }

    #[tokio::test]
    async fn boot_semantic_three_resumes_open_six_but_refuses_open_seven_before_seal() {
        for version in [6_u16, 7] {
            let StartedSealFixture {
                _dir: dir,
                paper_path,
                source_path,
                state,
                paper_writer,
                ..
            } = started_seal_fixture_with_semantic("start-hash", 2);
            let fixture = crate::bucket_commit::continuation_v3_tests::binding_fixture("poll_only");
            std::fs::copy(fixture.dir.path().join("binding.log"), &source_path).unwrap();
            let continuation = fixture.continuation.current_paper();
            let facts = &continuation.facts;
            let source_trade_id = facts.source_trade_id.clone();
            let mut frozen = serde_json::to_value(&continuation).unwrap();
            frozen["version"] = serde_json::json!(version);
            if version == 6 {
                frozen.as_object_mut().unwrap().remove("source_authority");
            }
            rusqlite::Connection::open(dir.path().join("paper.db"))
                .unwrap()
                .execute(
                    "INSERT INTO decision_pending (source_trade_id, semantic_revision, wallet_hex, source_epoch, frozen_inputs_json, post_commit_inputs_json, state, updated_at_unix) VALUES (?1, ?2, ?3, ?4, ?5, '[]', 'open', ?4)",
                    rusqlite::params![facts.source_trade_id.0, facts.semantic_revision, facts.wallet.to_string(), facts.source_epoch, frozen.to_string()],
                )
                .unwrap();
            let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
            let mut orchestrator = test_orchestrator(
                paper_path.clone(),
                source_path,
                paper_writer,
                Arc::clone(&state),
                source_receipts,
            );
            assert_eq!(orchestrator.pending_boot.len(), 1);
            let result = orchestrator
                .seal_before_resume("start-hash", FINANCIAL_SEMANTIC_VERSION)
                .await;
            if version == 7 {
                assert_eq!(
                    result.unwrap_err(),
                    "current-semantic continuation is open before qualification seal"
                );
                assert_eq!(orchestrator.pending_boot.len(), 1);
                assert_eq!(state.open_decision_pending().unwrap().len(), 1);
                assert!(sealed_records(&paper_path).is_empty());
            } else {
                result.unwrap();
                assert!(orchestrator.pending_boot.is_empty());
                assert!(state.open_decision_pending().unwrap().is_empty());
                assert_eq!(sealed_records(&paper_path).len(), 1);
                let terminal = state
                    .decision_pending_for(&source_trade_id)
                    .unwrap()
                    .unwrap();
                let replay = crate::decision_replay::replay_decision_pending(&terminal).unwrap();
                assert_eq!(replay.continuation.version(), 6);
                assert_eq!(replay.post_boundary.financial_semantic_version, 2);
            }
        }
    }

    /// PASS: a bracket whose install never committed leaves a recorded activity page with a trade
    /// that has no durable group; the semantic-change seal still closes the verified prefix.
    #[tokio::test]
    async fn boot_semantic_seal_closes_the_prefix_when_decision_evidence_is_unavailable() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            ..
        } = started_seal_fixture_with_semantic("start-hash", 1);
        let page = serde_json::to_vec(&serde_json::json!([{
            "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "type": "TRADE",
            "conditionId": "0xuncommitted",
            "asset": "123",
            "side": "BUY",
            "size": "5",
            "usdcSize": "2.5",
            "price": "0.5",
            "timestamp": (SEAL_START_UNIX + 1).to_string(),
            "transactionHash": "0xuncommitted",
            "outcomeIndex": "0"
        }]))
        .unwrap();
        let mut source_writer = Writer::open(&source_path).unwrap();
        append_source(
            &mut source_writer,
            crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
            SEAL_START_UNIX + 2,
            page,
        );
        drop(source_writer);
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path,
            paper_writer,
            state,
            source_receipts,
        );
        orchestrator
            .seal_before_resume("start-hash", FINANCIAL_SEMANTIC_VERSION)
            .await
            .unwrap();
        let seals = sealed_records(&paper_path);
        assert_eq!(seals.len(), 1);
        assert!(
            matches!(&seals[0].reason, SealReason::InsufficientEvidence(detail)
                if detail.contains("financial semantic version changed from 1 to 3")
                    && detail.contains("decision evidence unavailable")
                    && detail.contains("has no durable activity group")),
            "{:?}",
            seals[0].reason
        );
        assert_eq!(
            seals[0].decision_evidence_digest,
            blake3::hash(&[]).to_hex().to_string()
        );
    }

    /// PASS: configuration drift seals exactly the receipt-index tail supplied by `SealCheck`.
    #[tokio::test]
    async fn seal_check_records_the_index_tail_candidate() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            ..
        } = started_seal_fixture("start-hash");
        let mut source_writer = Writer::open(&source_path).unwrap();
        append_source(
            &mut source_writer,
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            SEAL_START_UNIX + 1,
            br#"{"kind":"indexed"}"#.to_vec(),
        );
        drop(source_writer);
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let candidate = source_receipts.current_tail_binding().unwrap();
        let mut source_writer = Writer::open(&source_path).unwrap();
        append_source(
            &mut source_writer,
            crate::trade_poller::ACTIVITY_POLL_SOURCE_ID,
            SEAL_START_UNIX + 2,
            b"[]".to_vec(),
        );
        drop(source_writer);
        let file_tail = Scanner::verify(&source_path).unwrap();
        assert_ne!(file_tail, candidate);
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path,
            paper_writer,
            state,
            source_receipts,
        );

        assert_eq!(
            apply_seal_check_control(&mut orchestrator, "changed-hash").await,
            Ok(())
        );

        let seals = sealed_records(&paper_path);
        assert_eq!(seals.len(), 1);
        assert_eq!(seals[0].source_prefix, TailBinding::from(&candidate));
        assert_ne!(seals[0].source_prefix, TailBinding::from(&file_tail));
    }

    /// PASS: a clean source-log end before the indexed candidate crosses `SealCheck` as the exact
    /// evidence-insufficient acknowledgement and does not append a seal.
    #[tokio::test]
    async fn seal_check_refuses_clean_truncation_before_index_candidate() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            ..
        } = started_seal_fixture("start-hash");
        let mut source_writer = Writer::open(&source_path).unwrap();
        append_source(
            &mut source_writer,
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            SEAL_START_UNIX + 1,
            br#"{"kind":"retained"}"#.to_vec(),
        );
        let preceding_tail = Scanner::verify(&source_path).unwrap();
        append_source(
            &mut source_writer,
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            SEAL_START_UNIX + 2,
            br#"{"kind":"truncated"}"#.to_vec(),
        );
        drop(source_writer);
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let candidate = source_receipts.current_tail_binding().unwrap();
        assert_ne!(candidate, preceding_tail);
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path.clone(),
            paper_writer,
            state,
            source_receipts,
        );
        let source_file = std::fs::OpenOptions::new()
            .write(true)
            .open(&source_path)
            .unwrap();
        source_file.set_len(preceding_tail.physical_tail).unwrap();
        source_file.sync_all().unwrap();
        drop(source_file);
        assert_eq!(Scanner::verify(&source_path).unwrap(), preceding_tail);

        assert_eq!(
            apply_seal_check_control(&mut orchestrator, "changed-hash").await,
            Err("insufficient qualification evidence: source observations do not reach the sealed sequence/hash prefix".to_owned())
        );
        assert!(sealed_records(&paper_path).is_empty());
    }

    /// PASS: an existing seal returns through `SealCheck` before opening its renamed source path.
    #[tokio::test]
    async fn already_sealed_seal_check_performs_no_source_read() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            ..
        } = started_seal_fixture("start-hash");
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path.clone(),
            paper_writer,
            state,
            source_receipts,
        );
        let paper_walks = pe_event_log::scan_metrics::count(&paper_path).unwrap();
        assert_eq!(
            apply_seal_check_control(&mut orchestrator, "changed-hash").await,
            Ok(())
        );
        let renamed = source_path.with_extension("renamed");
        std::fs::rename(&source_path, &renamed).unwrap();

        assert_eq!(
            apply_seal_check_control(&mut orchestrator, "start-hash").await,
            Ok(())
        );
        assert_eq!(
            apply_seal_check_control(&mut orchestrator, "changed-hash").await,
            Ok(())
        );
        assert_eq!(
            pe_event_log::scan_metrics::count(&paper_path).unwrap(),
            paper_walks
        );
        assert_eq!(sealed_records(&paper_path).len(), 1);
    }

    #[cfg(feature = "scenario")]
    #[tokio::test]
    async fn seal_tail_sync_uncertainty_stops_intake_and_snapshots_until_reopen() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            ..
        } = started_seal_fixture("start-hash");
        let index = SourceReceiptIndex::replay(&source_path).unwrap();
        let shared = paper_writer.clone();
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path.clone(),
            paper_writer,
            state.clone(),
            index,
        );
        shared.scenario_fail_next_sync().unwrap();
        let paper_walks = pe_event_log::scan_metrics::count(&paper_path).unwrap();
        let error = apply_seal_check_control(&mut orchestrator, "changed-hash")
            .await
            .unwrap_err();
        assert!(error.contains("injected synchronization uncertainty"));
        assert!(orchestrator.intake_stopped);
        assert!(shared.snapshot().is_err());
        assert!(
            orchestrator
                .append_paper_record(&PaperLogRecord::RiskHaltChanged {
                    owner: RiskHaltOwner::Paper,
                    cause: RiskHaltCause::AbsoluteLoss,
                    state: crate::paper_recovery::HaltState::Engaged,
                    evidence: serde_json::json!({}),
                })
                .is_err()
        );
        assert_eq!(
            pe_event_log::scan_metrics::count(&paper_path).unwrap(),
            paper_walks
        );
        assert!(sealed_records(&paper_path).is_empty());
        drop(orchestrator);
        drop(shared);
        let reopened = crate::paper_recovery::PaperLog::open(&paper_path).unwrap();
        assert_eq!(reopened.snapshot().unwrap().len(), 1);
        let mut restarted = test_orchestrator(
            paper_path,
            source_path.clone(),
            reopened,
            state,
            SourceReceiptIndex::replay(&source_path).unwrap(),
        );
        apply_seal_check_control(&mut restarted, "changed-hash")
            .await
            .unwrap();
    }

    /// PASS: unmatched Prepared refuses before source I/O and preserves the exact legacy error.
    #[test]
    fn unmatched_prepared_refuses_before_opening_source_candidate() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            start,
        } = started_seal_fixture("start-hash");
        let mut source_writer = Writer::open(&source_path).unwrap();
        let source_receipt = append_source(
            &mut source_writer,
            "seal-test-source",
            SEAL_START_UNIX + 1,
            b"source".to_vec(),
        );
        drop(source_writer);
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let candidate = source_receipts.current_tail_binding().unwrap();
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path.clone(),
            paper_writer,
            state,
            source_receipts,
        );
        orchestrator
            .append_paper_record(&PaperLogRecord::FinancialPrepared {
                expected_authority: ExpectedAuthority {
                    qualification_start_receipt: start,
                    prior_completed_prepared_sequence: None,
                },
                payload: FinancialPayload::Fill {
                    operation: PaperFillOperationIdentity {
                        leader_wallet: pe_core_types::WalletAddress::from_hex(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        )
                        .unwrap(),
                        source_trade_id: pe_core_types::SourceTradeId(
                            "unmatched-prepared".to_owned(),
                        ),
                        observed_at_bucket: SEAL_START_UNIX,
                    },
                    economic: seal_test_economic(source_receipt, start),
                },
            })
            .unwrap();
        let renamed = source_path.with_extension("renamed");
        std::fs::rename(&source_path, renamed).unwrap();

        assert_eq!(
            orchestrator.seal_qualification(SealReason::Complete, SEAL_START_UNIX + 2, candidate,),
            Err("qualification seal waits for the oldest unmatched Prepared".to_owned())
        );
        assert!(sealed_records(&paper_path).is_empty());
    }

    /// PASS: a physical prefix failure crosses the `SealCheck` boundary as bare `LogError` text.
    #[tokio::test]
    async fn seal_check_preserves_physical_log_error_text() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            ..
        } = started_seal_fixture("start-hash");
        let mut source_writer = Writer::open(&source_path).unwrap();
        append_source(
            &mut source_writer,
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            SEAL_START_UNIX + 1,
            b"physical-failure".to_vec(),
        );
        drop(source_writer);
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let frame_start = pe_event_log::Reader::replay_with_offsets(&source_path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .0;
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&source_path)
            .unwrap();
        file.seek(std::io::SeekFrom::Start(frame_start + 4))
            .unwrap();
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0xff;
        file.seek(std::io::SeekFrom::Start(frame_start + 4))
            .unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let physical = Scanner::verify(&source_path).unwrap_err();
        assert!(matches!(physical, LogError::CrcMismatch { .. }));
        let expected = physical.to_string();
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path,
            paper_writer,
            state,
            source_receipts,
        );

        let acknowledged = apply_seal_check_control(&mut orchestrator, "changed-hash")
            .await
            .unwrap_err();

        assert_eq!(acknowledged.as_bytes(), expected.as_bytes());
        assert!(!acknowledged.starts_with("event log:"));
        assert!(sealed_records(&paper_path).is_empty());
    }

    /// PASS: `DailyBoundary` retains the verified mark candidate while the index and file advance
    /// on opposite sides of the seal-start event, and both lifecycle events expose their fields.
    #[tokio::test]
    async fn daily_boundary_seals_retained_mark_tail_across_concurrent_append() {
        let StartedSealFixture {
            _dir,
            paper_path,
            source_path,
            state,
            paper_writer,
            start,
        } = started_seal_fixture("start-hash");
        let final_cutoff = SEAL_START_UNIX + 30 * 86_400;
        let mut source_writer = Writer::open(&source_path).unwrap();
        let financial_source = append_source(
            &mut source_writer,
            "seal-test-financial-source",
            SEAL_START_UNIX + 1,
            b"financial-source".to_vec(),
        );
        let boundary_receipt = append_source(
            &mut source_writer,
            crate::trade_poller::DAILY_BOUNDARY_SOURCE_ID,
            final_cutoff,
            serde_json::to_vec(&serde_json::json!({
                "kind": "daily_boundary",
                "cutoff_unix": final_cutoff,
            }))
            .unwrap(),
        );
        drop(source_writer);
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let index_candidate = source_receipts.current_tail_binding().unwrap();
        let mut source_writer = Writer::open(&source_path).unwrap();
        append_source(
            &mut source_writer,
            crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
            final_cutoff,
            br#"{"kind":"synchronized-unindexed"}"#.to_vec(),
        );
        drop(source_writer);
        let mark_candidate = Scanner::verify(&source_path).unwrap();
        assert_ne!(mark_candidate, index_candidate);

        append_paper(
            &paper_writer,
            &PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                boundary_receipt,
                cutoff_unix: SEAL_START_UNIX,
                source_tail: TailBinding::from(&mark_candidate),
                financial_prefix_seq: None,
                prices: Vec::new(),
                cash: dec!(1_000),
                equity: dec!(1_000),
                invalid: None,
            })),
            SEAL_START_UNIX,
        );

        let leader =
            pe_core_types::WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .unwrap();
        let condition = pe_core_types::MarketId(pe_core_types::VenueMarketId(
            "seal-test-condition".to_owned(),
        ));
        let mut prior = None;
        let mut cash = dec!(1_000);
        for index in 0..90_u64 {
            let economic = seal_test_economic(financial_source, start);
            let prepared = append_paper(
                &paper_writer,
                &PaperLogRecord::FinancialPrepared {
                    expected_authority: ExpectedAuthority {
                        qualification_start_receipt: start,
                        prior_completed_prepared_sequence: prior,
                    },
                    payload: FinancialPayload::Fill {
                        operation: PaperFillOperationIdentity {
                            leader_wallet: leader,
                            source_trade_id: pe_core_types::SourceTradeId(format!(
                                "seal-close-{index}"
                            )),
                            observed_at_bucket: SEAL_START_UNIX,
                        },
                        economic: economic.clone(),
                    },
                },
                SEAL_START_UNIX + 2,
            );
            cash -= dec!(0.5);
            let canonical = CanonicalFillResult {
                outcome: "applied".to_owned(),
                bankroll: cash,
                applied_prepared_seq: prepared.sequence,
                quantity: economic.sizing.expected_shares,
                principal: economic.sizing.principal,
                fee: economic.fee.expected_fee,
                fill_price: economic.sizing.expected_vwap,
            };
            state
                .apply_financial_fill(
                    start,
                    prior,
                    prepared.sequence,
                    financial_source,
                    SEAL_START_UNIX + 1,
                    &FillRecord {
                        idempotency_key: format!("seal-fill-{index}"),
                        market_id: condition.clone(),
                        outcome_id: pe_core_types::OutcomeId(0),
                        side: pe_core_types::Side::Buy,
                        quantity: canonical.quantity,
                        fill_price: canonical.fill_price,
                        principal: canonical.principal,
                        fee: canonical.fee,
                    },
                    canonical.bankroll,
                )
                .unwrap();
            append_paper(
                &paper_writer,
                &PaperLogRecord::FinancialFinal {
                    prepared_receipt: prepared,
                    result: FinancialResult::Fill { canonical },
                },
                SEAL_START_UNIX + 2,
            );
            prior = Some(prepared.sequence);
        }
        let resolution_prepared = append_paper(
            &paper_writer,
            &PaperLogRecord::FinancialPrepared {
                expected_authority: ExpectedAuthority {
                    qualification_start_receipt: start,
                    prior_completed_prepared_sequence: prior,
                },
                payload: FinancialPayload::Resolution {
                    condition_id: PolymarketConditionId(condition.to_string()),
                    payout_by_outcome_index_json: "[\"1\",\"0\"]".to_owned(),
                    resolution_source_receipt: financial_source,
                },
            },
            SEAL_START_UNIX + 3,
        );
        let credit = CollateralAmount::from_decimal_exact(dec!(90)).unwrap();
        cash += credit.to_decimal();
        state
            .apply_financial_resolution(
                start,
                prior,
                resolution_prepared.sequence,
                &condition,
                "[\"1\",\"0\"]",
                financial_source,
                SEAL_START_UNIX + 3,
                credit,
                cash,
            )
            .unwrap();
        append_paper(
            &paper_writer,
            &PaperLogRecord::FinancialFinal {
                prepared_receipt: resolution_prepared,
                result: FinancialResult::Resolution {
                    canonical: CanonicalResolutionResult {
                        outcome: "applied".to_owned(),
                        bankroll: cash,
                        applied_prepared_seq: resolution_prepared.sequence,
                        credit,
                        settled_at_unix: SEAL_START_UNIX + 3,
                    },
                },
            },
            SEAL_START_UNIX + 3,
        );
        for day in 1..30_i64 {
            append_paper(
                &paper_writer,
                &PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                    boundary_receipt,
                    cutoff_unix: SEAL_START_UNIX + day * 86_400,
                    source_tail: TailBinding::from(&mark_candidate),
                    financial_prefix_seq: Some(resolution_prepared.sequence),
                    prices: Vec::new(),
                    cash,
                    equity: cash,
                    invalid: None,
                })),
                SEAL_START_UNIX + day * 86_400,
            );
        }
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path.clone(),
            paper_writer,
            Arc::clone(&state),
            source_receipts,
        );
        let events = Arc::new(StdMutex::new(Vec::new()));
        let appended = Arc::new(AtomicBool::new(true));
        let subscriber = Registry::default().with(SealEventLayer {
            events: Arc::clone(&events),
            append_on_start: Some((source_path.clone(), Arc::clone(&appended))),
        });
        let (acknowledged, response) = oneshot::channel();

        let paper_walks = pe_event_log::scan_metrics::count(&paper_path).unwrap();
        orchestrator
            .apply_control_message(OrchestratorControl::DailyBoundary {
                cutoff_unix: final_cutoff,
                boundary_receipt,
                acknowledged,
            })
            .with_subscriber(subscriber)
            .await;

        assert_eq!(response.await.unwrap(), Ok(()));
        assert_eq!(
            pe_event_log::scan_metrics::count(&paper_path).unwrap(),
            paper_walks
        );
        assert!(!appended.load(Ordering::SeqCst));
        let file_tail = Scanner::verify(&source_path).unwrap();
        assert_ne!(file_tail, mark_candidate);
        let era = paper_era(scan_paper_log(&paper_path).unwrap());
        let marks = era
            .frames
            .iter()
            .filter_map(|frame| match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => Some(mark.as_ref()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(marks.len(), 31);
        assert_eq!(
            marks.last().unwrap().source_tail,
            TailBinding::from(&index_candidate)
        );
        assert_eq!(
            Scanner::walk_prefix(&index_candidate, &mut |_, _| {}).unwrap(),
            Some(index_candidate.clone())
        );
        let seals = sealed_records(&paper_path);
        assert_eq!(seals.len(), 1);
        assert_eq!(seals[0].source_prefix, TailBinding::from(&index_candidate));
        assert_ne!(seals[0].source_prefix, TailBinding::from(&file_tail));

        let events = events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let started = events
            .iter()
            .find(|event| event.message == "qualification seal started")
            .expect("seal-start event");
        assert!(started.debug.contains_key("reason"));
        assert!(started.debug.contains_key("candidate_sequence"));
        let completed = events
            .iter()
            .find(|event| event.message == "qualification seal completed")
            .expect("seal-completion event");
        assert!(completed.unsigned.contains_key("elapsed_ms"));
        assert_eq!(
            completed.unsigned.get("frames_walked"),
            index_candidate
                .last_sequence
                .map(|sequence| sequence.0 + 1)
                .as_ref()
        );
    }

    /// PASS: refusal of mismatched decision evidence leaves the preceding durable mark untouched.
    #[test]
    fn completion_evidence_refusal_preserves_portfolio_mark_without_seal() {
        let (dir, state, source_receipts) =
            crate::bucket_commit::continuation_validation_tests::producer_fixture();
        let source_path = source_receipts.current_tail_binding().unwrap().path;
        let candidate = source_receipts.current_tail_binding().unwrap();
        let paper_path = dir.path().join("paper.log");
        drop(pe_execution_core::LiveJournal::open(dir.path().join("live_journal.log")).unwrap());
        let paper_writer = crate::paper_recovery::PaperLog::open(&paper_path).unwrap();
        let start = append_paper(
            &paper_writer,
            &qualification_start(
                TailBinding::from(&Scanner::verify(&paper_path).unwrap()),
                TailBinding {
                    physical_tail: 5,
                    last_sequence: None,
                    last_hash: "00".repeat(32),
                },
                "start-hash",
            ),
            SEAL_START_UNIX,
        );
        state
            .reset_financial_era(
                start,
                CollateralAmount::from_decimal_exact(dec!(1_000)).unwrap(),
            )
            .unwrap();
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path,
            paper_writer,
            Arc::clone(&state),
            source_receipts,
        );
        let rows = state.open_decision_pending().unwrap();
        let mut changed: serde_json::Value =
            serde_json::from_str(&rows[0].frozen_inputs_json).unwrap();
        changed["read_commitment"]["this_hash"] =
            serde_json::json!(blake3::hash(b"wrong").to_hex().to_string());
        let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
        connection
            .execute(
                "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
                rusqlite::params![changed.to_string(), rows[0].source_trade_id.0],
            )
            .unwrap();
        for row in &rows {
            state
                .close_decision_pending(
                    &row.source_trade_id,
                    "{}",
                    "seal-test-terminal",
                    SEAL_START_UNIX + 1,
                )
                .unwrap();
        }
        let boundary_receipt = AppendReceipt {
            sequence: candidate.last_sequence.unwrap(),
            this_hash: candidate.last_hash,
        };
        orchestrator
            .append_paper_record(&PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                boundary_receipt,
                cutoff_unix: SEAL_START_UNIX + 1,
                source_tail: TailBinding::from(&candidate),
                financial_prefix_seq: None,
                prices: Vec::new(),
                cash: dec!(1_000),
                equity: dec!(1_000),
                invalid: None,
            })))
            .unwrap();

        let error = orchestrator
            .seal_qualification(SealReason::Complete, SEAL_START_UNIX + 1, candidate)
            .unwrap_err();

        assert_eq!(
            error,
            "insufficient qualification evidence: decision source receipt does not match the sealed source prefix"
        );
        let era = paper_era(scan_paper_log(&paper_path).unwrap());
        assert_eq!(
            era.frames
                .iter()
                .filter(|frame| matches!(
                    &frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::PortfolioMark(_))
                ))
                .count(),
            1
        );
        assert!(sealed_records(&paper_path).is_empty());
    }

    /// PASS: sealing a large candidate performs one bounded source walk plus indexed point reads
    /// for its selected decision, not a second verify pass.
    #[cfg(target_os = "linux")]
    #[test]
    fn qualification_seal_reads_large_source_prefix_once() {
        let (dir, state, source_receipts) =
            crate::bucket_commit::continuation_validation_tests::producer_fixture();
        let source_path = source_receipts.current_tail_binding().unwrap().path;
        let rows = state.open_decision_pending().unwrap();
        assert_eq!(rows.len(), 2);
        let selected = rows[0].clone();
        let excluded = &rows[1];
        let connection = rusqlite::Connection::open(dir.path().join("paper.db")).unwrap();
        connection
            .execute(
                "DELETE FROM decision_pending WHERE source_trade_id = ?1",
                [&excluded.source_trade_id.0],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE activity_groups SET disposition = 'not_buy' WHERE source_trade_id = ?1",
                [&excluded.source_trade_id.0],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE activity_group_revisions SET disposition = 'not_buy' \
                 WHERE source_trade_id = ?1",
                [&excluded.source_trade_id.0],
            )
            .unwrap();
        drop(connection);

        let paper_path = dir.path().join("paper.log");
        drop(pe_execution_core::LiveJournal::open(dir.path().join("live_journal.log")).unwrap());
        let paper_writer = crate::paper_recovery::PaperLog::open(&paper_path).unwrap();
        let start = append_paper(
            &paper_writer,
            &qualification_start(
                TailBinding::from(&Scanner::verify(&paper_path).unwrap()),
                TailBinding {
                    physical_tail: 5,
                    last_sequence: None,
                    last_hash: "00".repeat(32),
                },
                "start-hash",
            ),
            SEAL_START_UNIX,
        );
        state
            .reset_financial_era(
                start,
                CollateralAmount::from_decimal_exact(dec!(1_000)).unwrap(),
            )
            .unwrap();
        let mut source_writer = Writer::open(&source_path).unwrap();
        for index in 0..20_000_u64 {
            source_writer
                .append(source_input(
                    crate::activity_ingest::ACTIVITY_WS_SOURCE_ID,
                    SEAL_START_UNIX + 2 + i64::try_from(index).unwrap(),
                    filler_payload(index),
                ))
                .unwrap();
        }
        source_writer.sync().unwrap();
        drop(source_writer);
        let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let candidate = source_receipts.current_tail_binding().unwrap();
        let source_length = candidate.physical_tail;
        assert!(
            source_length >= 20_000_000,
            "fixture log is {source_length} bytes"
        );
        assert!(std::fs::metadata(&paper_path).unwrap().len() < 200_000);
        assert!(
            std::fs::metadata(paper_path.with_file_name("live_journal.log"))
                .unwrap()
                .len()
                < 200_000
        );
        let mut orchestrator = test_orchestrator(
            paper_path.clone(),
            source_path,
            paper_writer,
            Arc::clone(&state),
            source_receipts,
        );
        state
            .close_decision_pending(
                &selected.source_trade_id,
                "{}",
                "seal-test-terminal",
                SEAL_START_UNIX + 1,
            )
            .unwrap();
        assert_eq!(state.decision_pending_history().unwrap().len(), 1);
        let decision_keys = vec![(
            selected.source_trade_id.clone(),
            selected.semantic_revision.clone(),
        )];
        let expected_decision_evidence = state
            .seal_decision_evidence_for_source_prefix(
                &decision_keys,
                &decision_keys,
                candidate.last_sequence,
            )
            .unwrap();
        let expected_digest = blake3::hash(&expected_decision_evidence)
            .to_hex()
            .to_string();
        let expected_decision_evidence: serde_json::Value =
            serde_json::from_slice(&expected_decision_evidence).unwrap();
        assert_eq!(
            expected_decision_evidence["rows"][0]["source_trade_id"],
            selected.source_trade_id.0
        );
        assert_eq!(
            expected_decision_evidence["rows"][0]["semantic_revision"],
            selected.semantic_revision
        );
        let before = read_chars();
        orchestrator
            .seal_qualification(
                SealReason::InsufficientEvidence("read-volume-test".to_owned()),
                SEAL_START_UNIX + 30_000,
                candidate,
            )
            .unwrap();
        let read = read_chars() - before;

        assert!(
            read >= source_length,
            "the bounded walk read {read} of {source_length} bytes"
        );
        assert!(
            read < source_length + source_length / 2,
            "more than one source-log pass: read {read} bytes for {source_length} bytes"
        );
        let seals = sealed_records(&paper_path);
        assert_eq!(seals.len(), 1);
        assert_eq!(seals[0].decision_evidence_digest, expected_digest);
    }

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

        apply_global_risk_halts(&active, &mut snapshot, true);

        assert_eq!(
            evaluate_risk(&snapshot),
            RiskDecision::Blocked(RiskBlock::KillSwitchDrawdown)
        );
    }

    #[test]
    fn paper_halt_sync_selects_recorded_semantic_without_releasing_latency_history() {
        let dir = tempfile::tempdir().unwrap();
        let paper_path = dir.path().join("paper.log");
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let (_, control_rx) = mpsc::channel(4);
        let mut owner = build_test_orchestrator(
            crate::paper_recovery::PaperLog::open(&paper_path).unwrap(),
            paper,
            control_rx,
            String::new(),
            None,
        );
        let latency = (RiskHaltOwner::Paper, RiskHaltCause::CopyLatency);
        owner.active_risk_halts.insert(latency.clone());
        let mut snapshot = healthy_risk_snapshot();
        owner
            .synchronize_paper_risk_halts(&snapshot, 1_000, 2)
            .unwrap();
        assert!(owner.active_risk_halts.contains(&latency));
        assert!(scan_paper_log(&paper_path).unwrap().is_empty());

        snapshot.intraday_pnl_bps.0 = pe_risk_engine::INTRADAY_STOP_BPS;
        owner
            .synchronize_paper_risk_halts(&snapshot, 2_000, 2)
            .unwrap();
        assert!(owner.active_risk_halts.contains(&latency));
        assert!(
            owner
                .active_risk_halts
                .contains(&(RiskHaltOwner::Paper, RiskHaltCause::IntradayDrawdown))
        );
        assert_eq!(scan_paper_log(&paper_path).unwrap().len(), 1);

        snapshot.intraday_pnl_bps.0 = 0;
        owner
            .synchronize_paper_risk_halts(&snapshot, 3_000, 1)
            .unwrap();
        assert!(!owner.active_risk_halts.contains(&latency));
        assert_eq!(scan_paper_log(&paper_path).unwrap().len(), 3);
    }

    /// PASS: the first valid boundary that reaches both canonical completion counts requests the
    /// existing Complete seal; an invalid mark or either below-threshold count requests no seal.
    /// FAIL: the boundary can seal early, seal invalid evidence, or misses the first valid mark.
    #[test]
    fn daily_boundary_requests_automatic_complete_seal_at_exact_threshold() {
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
