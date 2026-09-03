//! Event dispatch loop: routes decoded trade events to the copy-signal-engine,
//! then gates signals through strategy evaluation and execution dispatch.
//!
//! The orchestrator is dispatch + sizing: its I/O is the `ExecutionDispatcher`, the mid-price
//! cache (Gamma), and the CLOB `/book` fetcher — read on every paper `clob_best_ask` BUY for the
//! best-ask fill basis (#486) and, when the price-impact gate is on, for the size cap (#398 WS2).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use pe_copy_signal_engine::{
    IncomingTrade, LeaderSignal, SignalConfig, TradeProvenance, classify_trade,
};
use pe_core_types::{
    CollateralAmount, EventSeq, MarketId, MarketOutcomeId, Price, Probability,
    ReconstructionQuality, ShareAmount, Side, SourceTimestamp, SourceTradeId, TraderId, VenueId,
    WalletAddress,
};
use pe_execution_core::{DispatchResult, ExecutionDispatcher, ExecutionError};
use pe_paper_state::{
    FillRecord, FillRow, LeaderPositionRow, PaperStateDb, PendingTerminalEvidence,
};
use pe_position_ledger::PositionLedger;
use pe_risk_engine::{RiskSnapshot, TradingMode};
use pe_source_core::SourceStatus;
use pe_source_polymarket_public::PageFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, FillSource, PaperExecutionError, PaperExecutor, PaperFill, SizingMode,
    WinnerFollowStrategy,
};
use pe_trader_index::Watchlist;
use pe_venue_polymarket::{AskLevel, LadderError, LadderPlan, ladder_is_stale, plan_budget_buy};
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info, warn};

use crate::bucket_commit::{BucketCommitEngine, DecisionContinuationV2};
use crate::clob_book::ClobBookFetcher;
use crate::decision_replay::{
    AuthorityEvidence, BookEvidence, DecisionEvidenceAccumulator, MarketEndEvidence,
    MarketPriceEvidence, TerminalDispositionEvidence, ladder_plan_blake3, replay_decision_pending,
};
use crate::entry_gate::CopyEntryGateConfig;
use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
use crate::market_end_cache::MarketEndCache;
use crate::mid_price_cache::MidPriceCache;
use crate::orchestrator_control::OrchestratorControl;
use crate::runtime_config::{self, FillMode, LiveRuntimeConfig};
use crate::snapshot_worker::{SnapshotHandle, enqueue_if_buy};
use crate::supabase_sink::{SinkHandle, SupabaseFillRow, supabase_fill_from};
use crate::supabase_state::{
    AuthoritativeFillOutcome, SupabaseStateClient, commit_fill_authoritative,
};

/// Hot-path `/book` fetch timeout for the price-impact gate (#398 WS2). Tighter than the worker's
/// 5 s per-request timeout so a slow book fails open (no cap) without stalling the trade.
/// Canonical default in `docs/_GLOSSARY.md`: `clob_book_hot_path_timeout_secs`.
const CLOB_BOOK_HOT_PATH_TIMEOUT_SECS: u64 = 2;

/// Bounded in-process retry for a failed local paper-outcome commit (#508 round-4: every
/// failed local finalization surfaces and enters an in-process reconcile pass — never a
/// silent log-and-continue). Restart recovery remains the durable backstop.
const LOCAL_COMMIT_RETRIES: u32 = 3;
const LOCAL_COMMIT_RETRY_DELAY_MS: u64 = 100;

/// #511 parked-fill retry cadence (frozen v2 retries; matches the poll interval scale).
const PARKED_RETRY_SECS: u64 = 30;

/// Outcome of the enabled price-impact gate for one admitted signal (#508).
enum GatePlan {
    /// The budget planner produced a within-band plan (quantity/VWAP/limit).
    Planned(LadderPlan),
    /// The book read SUCCEEDED but the in-band ladder affords no whole share of the paper
    /// budget — a paper-only skip that must never suppress live targets (Decision 10). The
    /// best ask anchors the shared band gate.
    NothingAffordable { best_ask: Price },
}

struct GatePlanEvidence {
    gate: GatePlan,
    book: BookEvidence,
    checked_at_unix_ms: u64,
}

struct GatePlanFailure {
    reason: &'static str,
    book: Box<BookEvidence>,
    checked_at_unix_ms: Option<u64>,
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
        record.contracts,
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

/// A post-frame fill whose authority commit has not reached a terminal outcome (#511).
/// Everything needed to retry the FROZEN decision without re-running admission.
struct ParkedFill {
    leader: LeaderPositionRow,
    record: FillRecord,
    seq: EventSeq,
    /// `Some` in authoritative mode (the frozen v2 request); `None` in legacy mode
    /// (the frozen local commit is the terminal protocol).
    sup_row: Option<SupabaseFillRow>,
    dispatch_id: Option<String>,
    filled_key: MarketOutcomeId,
    decision_evidence: Option<DecisionEvidenceAccumulator>,
}

/// Terminal-vs-parked result of a paper fill commit (#511).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PaperCommitResult {
    /// Applied (or converged on an existing canonical fill) — mark the contract filled.
    Filled,
    /// Authority/settled refusal — terminal typed no-fill; do NOT mark filled.
    Refused,
    /// No terminal outcome yet — frozen retry owns the trade (ticker + boot walk).
    Parked,
    /// Remote authority committed but the local mirror failed (#544 review):
    /// financial state is uncertain — the caller fails readiness and stops
    /// producer intake; the durable frame + idempotent RPC reconverge at boot.
    DurabilityUncertain,
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
    /// BUY-side paper-fill haircut (bps); mirrors [`crate::config::ServiceConfig`]
    /// `paper_fill_haircut_bps`. Used to derive the realistic fill price a copy is
    /// sized and band-gated against (see [`Orchestrator::handle_trade`]), so sizing
    /// and the recorded fill stay on one price rather than a stale market mid.
    pub paper_fill_haircut_bps: u32,
    /// SELL-side paper-fill slippage (bps); mirrors `paper_fill_slippage_bps`. Kept for
    /// the shared [`WinnerFollowStrategy`]/[`pe_strategy_winner_follow::PaperExecutor::fill_price`]
    /// formula; the copy path is BUY-only, so this only affects a hypothetical SELL copy.
    pub paper_fill_slippage_bps: u32,
    /// Paper fill-price mode (#486): `ClobBestAsk` (a paper BUY fills at the fresh CLOB best-ask,
    /// with sizing/band-gates keyed off it) or `LeaderHaircut` (the pre-#486 boot-frozen haircut).
    /// Refreshed per event from the runtime-config snapshot when `runtime_config` is `Some`.
    pub fill_mode: FillMode,
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

/// Scenario-only deterministic seams (#546): fixed admission-clock instants consumed in
/// order by each copy-budget check, and one-shot faults immediately before the two durable
/// writes whose rollback the fan-in acceptance suite must prove. Compiled only with the
/// `scenario` feature; production has no clock injection and no fault path.
#[cfg(feature = "scenario")]
#[derive(Debug, Default)]
pub struct ScenarioHooks {
    pub age_clock: std::sync::Mutex<std::collections::VecDeque<OffsetDateTime>>,
    pub fail_next_stage_seed: std::sync::atomic::AtomicBool,
    pub fail_next_no_copy_commit: std::sync::atomic::AtomicBool,
}

pub struct Orchestrator<F: PageFetcher + Send + Sync, B: ClobBookFetcher> {
    trade_rx: mpsc::Receiver<IncomingTrade>,
    bucket_engine: BucketCommitEngine,
    live_watchlist: LiveWatchlist,
    signal_config: SignalConfig,
    strategy: WinnerFollowStrategy,
    dispatcher: ExecutionDispatcher,
    mode: ExecutionMode,
    bankroll: Decimal,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,
    market_end_cache: MarketEndCache,
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
    // Paper-fill haircut/slippage (bps): derive the realistic fill price a copy is sized
    // and gated against, via `PaperExecutor::fill_price`. Boot-frozen (NOT runtime-refreshed)
    // to stay identical to the boot-built `PaperExecutor`'s own bps, so the sizing basis and
    // the recorded fill can never disagree.
    paper_fill_haircut_bps: u32,
    paper_fill_slippage_bps: u32,
    // Tracks (market, outcome) pairs we already hold a paper position in.
    // Prevents multiple leaders entering the same contract from stacking fills.
    filled_positions: HashSet<MarketOutcomeId>,
    /// #511 parked post-frame fills: the frame is durable but the authority commit has
    /// not reached a terminal outcome. Retried on a coarse ticker with the FROZEN
    /// record via `commit_fill_v2` by idempotency key — never by re-running admission
    /// (mutable gates would misclassify a redelivery). Keyed by `source_trade_id`
    /// (1:1 with the signal-derived idempotency key for a given trade). In-memory
    /// only: across a crash the frozen #510 watermark marks the frames pending and
    /// the boot frame-walk resolves them.
    parked: HashMap<SourceTradeId, ParkedFill>,
    // Copy-entry gate: admits only a leader's first-ever BUY entry into a market
    // (#290; price band removed in #339).
    // Sentinel quality (0) returned for any wallet not found in the watchlist.
    // Zero quality → LeaderAction::Unknown → classify_trade returns None, so no signal.
    min_quality: ReconstructionQuality,
    control_rx: mpsc::Receiver<OrchestratorControl>,
    // Best-effort Supabase analytics sink (issue #343). `None` when disabled — and always
    // `None` in authoritative mode (issue #397), where `run_sink` is not spawned, so the
    // best-effort `send_fill` below is suppressed automatically.
    sink: Option<SinkHandle>,
    // Liquidity-at-fill snapshot enqueue handle (issue #350 WS2 PR-H). `None` when capture is
    // disabled. Buy-only; enqueue is non-blocking (drop-on-full), off the trade hot path.
    snapshot_sink: Option<SnapshotHandle>,
    // Authoritative Supabase paper-state client (issue #397). `Some` when
    // `PE_SUPABASE_AUTHORITATIVE=1`: a paper fill writes the `commit_fill` RPC first
    // (fail-closed), then mirrors to SQLite. `None` → the legacy SQLite-authoritative path.
    supabase_state: Option<SupabaseStateClient>,
    // Supabase-authoritative runtime config (#398 WS1). `Some` in production; the per-event
    // rebuild at the top of `handle_trade` reads one snapshot. `None` in tests (boot config).
    runtime_config: Option<LiveRuntimeConfig>,
    // Live CLOB /book fetcher for the price-impact gate (#398 WS2). Shared `Arc` with the snapshot
    // worker so the 5 rps rate gate is global. Consulted for every admitted signal.
    book_fetcher: Arc<B>,
    // Mandatory price-impact gate cap in bps, rebuilt per event from the runtime-config snapshot.
    price_impact_cap_bps: i32,
    // Paper fill-price mode (#486), rebuilt per event from the runtime-config snapshot. Gates
    // whether a paper BUY fetches the CLOB best-ask (`ClobBestAsk`) or uses the boot-frozen
    // haircut (`LeaderHaircut`).
    fill_mode: FillMode,
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
    pending_continuations: HashMap<SourceTradeId, DecisionContinuationV2>,
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

impl<F: PageFetcher + Send + Sync, B: ClobBookFetcher> Orchestrator<F, B> {
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
        }
    }
}

impl<F: PageFetcher + Send + Sync, B: ClobBookFetcher> Orchestrator<F, B> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trade_rx: mpsc::Receiver<IncomingTrade>,
        live_watchlist: LiveWatchlist,
        config: OrchestratorConfig,
        strategy: WinnerFollowStrategy,
        dispatcher: ExecutionDispatcher,
        paper_state: Arc<PaperStateDb>,
        leader_ledger: PositionLedger,
        health: SharedHealth,
        market_end_cache: MarketEndCache,
        mid_price_cache: MidPriceCache<F>,
        control_rx: mpsc::Receiver<OrchestratorControl>,
        sink: Option<SinkHandle>,
        snapshot_sink: Option<SnapshotHandle>,
        supabase_state: Option<SupabaseStateClient>,
        book_fetcher: Arc<B>,
    ) -> Result<Self, anyhow::Error> {
        let min_quality = ReconstructionQuality::new(0)
            .map_err(|_| anyhow::anyhow!("internal: ReconstructionQuality::new(0) failed"))?;
        if !(1..=10_000).contains(&config.price_impact_cap_bps) {
            return Err(anyhow::anyhow!(
                "price_impact_cap_bps must be in 1..=10_000"
            ));
        }

        // Seed the dedup set from any positions already in the DB (crash-restart safety).
        let filled_positions: HashSet<MarketOutcomeId> = paper_state
            .paper_positions()
            .map_err(|e| anyhow::anyhow!("load paper positions: {e}"))?
            .into_iter()
            .filter(|p| p.long_contracts > 0 || p.short_contracts > 0)
            .map(|p| MarketOutcomeId::new(p.market_id, p.outcome_id))
            .collect();

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
            let continuation = DecisionContinuationV2::from_durable(&row).map_err(|error| {
                anyhow::anyhow!("decode pending {}: {error}", row.source_trade_id)
            })?;
            let trade = continuation.incoming_trade().map_err(|error| {
                anyhow::anyhow!("rebuild pending {}: {error}", row.source_trade_id)
            })?;
            pending_continuations.insert(row.source_trade_id, continuation);
            pending_boot.push_back(trade);
        }
        Ok(Self {
            trade_rx,
            bucket_engine,
            live_watchlist,
            signal_config: config.signal_config,
            strategy,
            dispatcher,
            mode: config.mode,
            bankroll: config.bankroll,
            paper_state,
            health,
            market_end_cache,
            mid_price_cache,
            activity_ws_enabled: config.activity_ws_enabled,
            copy_latency_budget_secs: config.copy_latency_budget_secs,
            #[cfg(feature = "scenario")]
            scenario_hooks: None,
            max_resolution_horizon_secs: config.max_resolution_horizon_secs,
            min_resolution_horizon_secs: config.min_resolution_horizon_secs,
            max_fill_price: config.max_fill_price,
            min_fill_price: config.min_fill_price,
            paper_fill_haircut_bps: config.paper_fill_haircut_bps,
            paper_fill_slippage_bps: config.paper_fill_slippage_bps,
            filled_positions,
            parked: HashMap::new(),
            min_quality,
            control_rx,
            sink,
            snapshot_sink,
            supabase_state,
            runtime_config: config.runtime_config,
            book_fetcher,
            price_impact_cap_bps: config.price_impact_cap_bps,
            fill_mode: config.fill_mode,
            live_accounts: config.live_accounts,
            intake_stopped: false,
            watchlist_writer_lock: config.watchlist_writer_lock,
            pending_boot,
            pending_continuations,
        })
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
        let continuation = DecisionContinuationV2::from_durable(&row)
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

    /// Run the dispatch loop until the trade channel closes OR until the
    /// provided `shutdown` future resolves.
    ///
    /// On shutdown, remaining trades already buffered in the channel are drained
    /// and processed before returning — no in-flight fills are lost.
    pub async fn resume_pending_before_producers(&mut self) -> Result<(), anyhow::Error> {
        // Production boot recovery runs before either producer receiver is polled.
        // `handle_trade` detects the durable pending owner and skips ledger/gate/history.
        while let Some(trade) = self.pending_boot.pop_front() {
            self.handle_trade(trade).await;
            if self.intake_stopped {
                return Err(anyhow::anyhow!(
                    "paper durability became uncertain while resuming decision_pending"
                ));
            }
        }
        Ok(())
    }

    pub async fn run(mut self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        if let Err(error) = self.resume_pending_before_producers().await {
            error!(%error, "decision_pending boot recovery failed");
            return;
        }
        let mut trades_done = false;
        let mut control_done = false;
        // #511 frozen-retry ticker: drives parked post-frame fills to a terminal outcome.
        let mut parked_tick = tokio::time::interval(Duration::from_secs(PARKED_RETRY_SECS));
        parked_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            if trades_done {
                break;
            }

            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    while let Ok(trade) = self.trade_rx.try_recv() {
                        self.handle_trade(trade).await;
                    }
                    break;
                }
                _ = parked_tick.tick(), if !self.parked.is_empty() => {
                    self.retry_parked().await;
                }
                result = self.control_rx.recv(), if !control_done => {
                    match result {
                        Some(message) => self.apply_control_message(message).await,
                        None => control_done = true,
                    }
                }
                result = self.trade_rx.recv(), if !trades_done => {
                    match result {
                        Some(trade) => {
                            self.handle_trade(trade).await;
                            if self.intake_stopped {
                                trades_done = true;
                            }
                        },
                        None => trades_done = true,
                    }
                }
            }
        }
    }

    /// Supervised production loop. Once `shutdown` resolves this owner drains both accepted
    /// input channels and every parked durable continuation before returning. A channel closure
    /// before coordinated shutdown is a typed critical failure.
    pub async fn run_coordinated(
        mut self,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), OrchestratorRunError> {
        tokio::pin!(shutdown);
        self.resume_pending_before_producers()
            .await
            .map_err(|error| OrchestratorRunError::PendingRecovery(error.to_string()))?;
        let mut draining = false;
        let mut trades_done = false;
        let mut control_done = false;
        let mut parked_tick = tokio::time::interval(Duration::from_secs(PARKED_RETRY_SECS));
        parked_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            if self.intake_stopped {
                return Err(OrchestratorRunError::PaperDurabilityUncertain);
            }
            if draining && trades_done && control_done && self.parked.is_empty() {
                return Ok(());
            }

            tokio::select! {
                biased;
                _ = &mut shutdown, if !draining => draining = true,
                _ = parked_tick.tick(), if !self.parked.is_empty() => {
                    self.retry_parked().await;
                }
                result = self.control_rx.recv(), if !control_done => {
                    match result {
                        Some(message) => self.apply_control_message(message).await,
                        None if draining => control_done = true,
                        None => return Err(OrchestratorRunError::PrematureInputClosure {
                            channel: "orchestrator_control",
                        }),
                    }
                }
                result = self.trade_rx.recv(), if !trades_done => {
                    match result {
                        Some(trade) => self.handle_trade(trade).await,
                        None if draining => trades_done = true,
                        None => return Err(OrchestratorRunError::PrematureInputClosure {
                            channel: "trade_input",
                        }),
                    }
                }
            }
        }
    }

    // ── Private handlers ──────────────────────────────────────────────────────

    /// Impact-gate ladder plan (#508 Phase A): ONE `/book` fetch per admitted signal feeds
    /// the band gate, the budget planner, and (in `clob_best_ask` paper mode) the fill basis.
    ///
    /// Called only when the gate is enabled (`price_impact_cap_bps ≥ 1`). Every failure is a
    /// typed skip reason and **fails closed** — unusable book (missing token, fetch
    /// error/timeout, corrupt levels, empty ladder, stale snapshot) and an in-band ladder
    /// affording no whole share both produce no order. The planner budget follows the sizing
    /// mode: `Dollar` plans within its USD notional; `Contract` caps at the requested count
    /// within the available bankroll; `Kelly` plans the full in-band depth within bankroll.
    async fn plan_impact_gate(
        &self,
        signal: &LeaderSignal,
        sizing_bankroll: Decimal,
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
        let snaps = self
            .mid_price_cache
            .fetch_snapshots(std::slice::from_ref(&signal.market_id))
            .await;
        let Some(token_id) = snaps.get(&signal.market_id).and_then(|s| {
            s.clob_token_ids
                .get(usize::from(signal.outcome_id.0))
                .cloned()
        }) else {
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
            self.book_fetcher.fetch_book(&token_id),
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
        let (budget, max_shares) = match self.strategy.config().sizing_mode {
            SizingMode::Dollar { usd } => (to_budget(usd)?, None),
            SizingMode::Contract { contracts } => (to_budget(sizing_bankroll)?, Some(contracts)),
            SizingMode::Kelly => (to_budget(sizing_bankroll)?, None),
        };
        let max_price = Price::new(Decimal::ONE).map_err(|_| GatePlanFailure {
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
        match plan_budget_buy(&ladder, budget, max_shares, Price::ZERO, max_price, ceiling) {
            Ok(plan) => Ok(GatePlanEvidence {
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
                checked_at_unix_ms,
            }),
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
                    reason: Some("budget afforded no whole share".to_owned()),
                },
                gate: GatePlan::NothingAffordable { best_ask: best },
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
            Err(LadderError::Amount) => Err(GatePlanFailure {
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
            "fill_mode": format!("{:?}", self.fill_mode),
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

    /// Resolve the local paper fill basis for paths that do not use the mandatory CLOB plan.
    /// A paper `clob_best_ask` BUY is rejected before this point unless the single impact-gate
    /// request produced a usable ladder basis (#544).
    fn resolve_fill_price(
        &self,
        signal: &LeaderSignal,
    ) -> Result<(Price, FillSource), PaperExecutionError> {
        let price = PaperExecutor::fill_price(
            signal.leader_side,
            signal.leader_price,
            self.paper_fill_haircut_bps,
            self.paper_fill_slippage_bps,
        )?;
        Ok((price, FillSource::LeaderHaircut))
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
        self.fill_mode = rc.fill_mode;
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
        let mut decision_evidence = pending.as_ref().map(DecisionEvidenceAccumulator::new);
        // New decisions use the current hot snapshot. A committed continuation instead
        // reinstalls its complete frozen 17-key snapshot before any post-boundary read.
        let applied_runtime = pending
            .as_ref()
            .map(|continuation| continuation.applied_configuration.clone())
            .or_else(|| {
                self.runtime_config
                    .as_ref()
                    .map(|live| live.snapshot().as_ref().clone())
            });
        if let Some(rc) = applied_runtime.as_ref() {
            self.apply_runtime_snapshot(rc);
            // NOTE: paper_fill_haircut/slippage_bps are deliberately NOT refreshed here. The
            // `PaperExecutor` that records the fill bakes them in at boot with no runtime setter,
            // so refreshing only the sizing side would desync sizing from the recorded fill after
            // a live edit. They stay boot-frozen on both sides; changing them needs a restart.
        }

        // Mark polymarket freshness; in the same lock, evaluate the #530
        // dual-unhealthy admission block (websocket stale-or-worse AND the poll
        // source unhealthy). A blocked trade is refused BEFORE any state write -
        // it stays unseen, so the held cursor / firehose backstop redelivers it
        // exactly once when a source recovers.
        let admission_blocked = pending.is_none() && {
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

        // #511: a parked post-frame fill owns this trade — the frozen-retry ticker
        // resolves it via the authority; a redelivered copy must not re-run admission
        // (or re-ingest the leader ledger).
        if self.parked.contains_key(&trade.source_trade_id) {
            return;
        }

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
                    action: continuation.pre_bucket_action,
                    leader_side: trade.side,
                    leader_price: trade.price,
                    leader_size: trade.contracts,
                    observed_at: trade.observed_at,
                    received_at: trade.received_at,
                    reconstruction_quality: continuation.reconstruction_quality,
                    source_trade_id: trade.source_trade_id.clone(),
                    action_confidence_ppm: continuation.action_confidence_ppm,
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

        // Resolution-horizon gate (#290, #339): copy only markets whose resolution time
        // (umaEndDate, else the always-present endDate) is known AND sits within
        // [min, max] seconds from now. Too far out locks capital for months; too soon
        // cannot be filled and held. Fail closed when the resolution time is unknown —
        // we cannot confirm the horizon, so we do not enter. One lookup serves both
        // bounds.
        if self.max_resolution_horizon_secs > 0 || self.min_resolution_horizon_secs > 0 {
            let resolution = self.market_end_cache.resolution(&signal.market_id).await;
            let resolution_unix = resolution.resolution_unix;
            if let Some(evidence) = decision_evidence.as_mut() {
                evidence.record_market_end(MarketEndEvidence {
                    market_id: signal.market_id.to_string(),
                    resolution_unix,
                    source: resolution
                        .source
                        .unwrap_or_else(|| "gamma.unavailable".to_owned()),
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
        let market_mid = {
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

        // Mandatory price-impact gate (#508 Phase A, #544): ONE `/book` fetch produces the
        // executable-ladder plan that feeds the band gate, the size cap, and (in
        // `clob_best_ask` paper mode) the VWAP fill basis. An unusable book — missing token,
        // fetch error/timeout, corrupt/empty/stale — fails CLOSED (skip), as does an in-band
        // ladder that affords no whole share.
        // A resumed continuation evaluates under its frozen basis; only a fresh
        // decision reads the live watchlist and bankroll (#544 review round 3).
        // Selected BEFORE the impact gate so ladder budget, VWAP, and strategy
        // sizing all share one basis.
        let p = pending
            .as_ref()
            .map(|continuation| continuation.frozen_basis.win_rate_p)
            .unwrap_or_else(|| self.win_rate_p_for(&watchlist, &signal.leader));
        let sizing_bankroll = match pending.as_ref() {
            Some(continuation) => continuation.frozen_basis.bankroll,
            None => self.bankroll,
        };
        let gate_evidence = match self.plan_impact_gate(&signal, sizing_bankroll).await {
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
        let gate = gate_evidence.gate;
        let gate_plan: Option<&LadderPlan> = match &gate {
            GatePlan::Planned(plan) => Some(plan),
            _ => None,
        };

        // Realistic fill basis (#339 revisited, #486, #508): the price the copy will ACTUALLY
        // fill at. With the mandatory impact gate in paper `clob_best_ask` mode, a BUY prices at
        // the planned ladder VWAP (`estimated_ladder_spend / shares` — multi-level exact, from
        // the same single `/book` fetch as the gate). Otherwise: in paper `clob_best_ask` mode
        // a BUY uses the same successful CLOB evidence; other modes
        // use the leader price adjusted by the boot-frozen paper haircut
        // (`PaperExecutor::fill_price`). Size and band-gate against THIS, so notional ==
        // `sizing_dollar_usd` and the gates check the price actually paid. In paper mode the
        // basis is recorded verbatim by the executor (via `observed_fill_price` below). The
        // fail-closed arms are defensive.
        let clob_basis_applies = self.mode == ExecutionMode::Paper
            && self.fill_mode == FillMode::ClobBestAsk
            && signal.leader_side == Side::Buy;
        let planned_vwap_basis =
            gate_plan.and_then(|plan| clob_basis_applies.then(|| plan.vwap()).flatten());
        // Zero-absorb reads still anchor the SHARED band gate on the successful best ask
        // (#508 Decision 10 — the paper-only skip happens after staging, below).
        let zero_absorb_basis = match &gate {
            GatePlan::NothingAffordable { best_ask } if clob_basis_applies => Some(*best_ask),
            _ => None,
        };
        let (fill_basis, fill_source) = if let Some(vwap) = planned_vwap_basis {
            (vwap, FillSource::ClobBestAsk)
        } else if let Some(best_ask) = zero_absorb_basis {
            (best_ask, FillSource::ClobBestAsk)
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
            match self.resolve_fill_price(&signal) {
                Ok(pair) => pair,
                Err(_) => {
                    info!(
                        reason = "leader price yields no constructible fill price",
                        market = %signal.market_id,
                        leader_price = %signal.leader_price.0,
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
            }
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

        let dispatch_id = match self.stage_dispatch_if_targeted(&signal, &mut decision_evidence) {
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
        // could not absorb one whole share of the paper budget.
        if matches!(&gate, GatePlan::NothingAffordable { .. }) {
            info!(
                reason = "impact band absorbs no whole share of the paper budget (paper-only)",
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

        // Size cap from the mandatory ladder plan (#508): the planned whole-share quantity IS the
        // within-band, within-budget maximum. The no-affordable-shares arm returned above.
        let book_cap_contracts = gate_plan.map(|plan| plan.shares.atomic() / 1_000_000);

        let snapshot = zeroed_risk_snapshot();
        // Sizing basis: pass the leader's price as the RAW `current_price` (Kelly cost `c`,
        // per-trade cap, exposure bps) and `fill_basis` as the dollar-sizing price, so the
        // Dollar arm computes `floor(sizing_dollar_usd / fill_basis)` (→ `contracts × fill ==
        // sizing_dollar_usd`) while the fee-additive Kelly `c` is NOT double-counted against a
        // fill-inclusive price. (Kelly is not used on the live copy path, but this keeps both
        // arms correct rather than relying on that as an invariant.)
        match self.strategy.evaluate_at_price(
            &signal,
            signal.leader_price,
            p,
            snapshot,
            sizing_bankroll,
            self.mode,
            book_cap_contracts,
            Some(fill_basis),
        ) {
            Err(e) => {
                info!(reason = %e, "signal did not produce order");
                let reason = format!("paper_reject:{e}");
                self.no_fill_or_rollback(
                    &trade,
                    &leader_row,
                    dispatch_id.as_deref(),
                    &reason,
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
                let now = SourceTimestamp(execution_at);
                // Exact recorded basis (#508): when the ladder plan priced this BUY and the
                // strategy sized BELOW the planned quantity (Kelly/Contract clamps), re-price
                // the VWAP over the ladder prefix actually consumed by the final count, so
                // the recorded fill is exact for the executed size. (Dollar sizing lands on
                // the planned quantity, where the prefix VWAP equals the plan VWAP.)
                let recorded_basis = if planned_vwap_basis.is_some() {
                    gate_plan
                        .and_then(|plan| prefix_vwap(&plan.used_asks, intent.contracts.0))
                        .unwrap_or(fill_basis)
                } else {
                    fill_basis
                };
                // Paper mode records the resolved basis verbatim. Shadow recomputes the identical
                // boot-frozen haircut from `None`. Ordinary live modes fail closed in the dispatcher.
                let observed_fill_price =
                    (self.mode == ExecutionMode::Paper).then_some((recorded_basis, fill_source));
                match self
                    .dispatcher
                    .execute(&intent, self.mode, now, observed_fill_price)
                    .await
                {
                    Err(ExecutionError::PaperDurabilityUncertain { reason }) => {
                        error!(%reason, "paper durability uncertain; stopping producer intake");
                        {
                            let mut health = self
                                .health
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            health.paper_durability_uncertain = true;
                            health.refresh_event_log_writable();
                        }
                        self.rollback_admission(&rb, None);
                        self.intake_stopped = true;
                    }
                    Err(e) => {
                        error!(error = %e, "execution dispatcher failed");
                        self.no_fill_or_rollback(
                            &trade,
                            &leader_row,
                            dispatch_id.as_deref(),
                            "paper_dispatch_error",
                            &rb,
                            Some(&signal.market_id),
                            decision_evidence.as_ref(),
                        )
                        .await;
                    }
                    Ok(DispatchResult::Paper { fill, seq }) => {
                        let filled_key = MarketOutcomeId::new(
                            fill.intent.market_id.clone(),
                            fill.intent.outcome_id,
                        );
                        // #511: Filled marks the contract; Refused is a terminal typed
                        // no-fill; Parked means the frozen-retry protocol owns the trade.
                        if PaperCommitResult::Filled
                            == self
                                .commit_paper_fill(
                                    &trade,
                                    &leader_row,
                                    &fill,
                                    seq,
                                    dispatch_id.as_deref(),
                                    decision_evidence.as_ref(),
                                )
                                .await
                        {
                            self.filled_positions.insert(filled_key);
                            info!(
                                kind = "paper_fill",
                                idempotency_key = %fill.intent.idempotency_key,
                                market = %fill.intent.market_id,
                                side = ?fill.intent.side,
                                contracts = fill.intent.contracts.0,
                                fill_price = %fill.simulated_fill_price.0,
                                // Fill provenance (#486): `ClobBestAsk` = `fill_price` IS the fresh
                                // best-ask; `Fallback`/`LeaderHaircut` = the haircut basis. The
                                // fallback rate is the metric for the thin-book Open risk.
                                fill_source = ?fill.fill_source,
                                // Observability: the leader's trade price is the sizing/gating
                                // basis; `market_mid` is the Gamma mid we no longer size off.
                                // Their divergence characterises the mid's failure mode live.
                                leader_price = %signal.leader_price.0,
                                market_mid = %market_mid.0,
                            );
                        }
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
            .commit_no_fill_flipping(trade, leader, dispatch_id, no_fill_reason, evidence)
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
        evidence: Option<&DecisionEvidenceAccumulator>,
    ) -> bool {
        let durable_reason = if no_fill_reason.is_empty() {
            "no_order"
        } else {
            no_fill_reason
        };
        let pending = match render_pending_evidence(
            evidence,
            AuthorityEvidence::not_read("terminal_before_fill_authority"),
            TerminalDispositionEvidence::no_fill(durable_reason),
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

    /// Commit dedup + leader-ledger mirror + fill accounting, and update the in-memory
    /// bankroll to the new persisted value (drawdown-aware sizing). Returns `true` when the
    /// fill committed (the caller then marks the contract filled); `false` only on an
    /// authoritative fail-closed skip.
    ///
    /// Authoritative mode (#397/#511): `commit_fill_v2` first (typed outcome), then one
    /// local convergence transaction; on RPC failure the FROZEN record is retried bounded
    /// and then PARKED (#511 — never re-admitted; the ticker + boot frame-walk converge).
    /// Legacy mode: SQLite authoritative with the same settled-refusal semantics.
    async fn commit_paper_fill(
        &mut self,
        trade: &IncomingTrade,
        leader: &LeaderPositionRow,
        fill: &PaperFill,
        seq: EventSeq,
        dispatch_id: Option<&str>,
        decision_evidence: Option<&DecisionEvidenceAccumulator>,
    ) -> PaperCommitResult {
        let record = FillRecord {
            idempotency_key: fill.intent.idempotency_key.clone(),
            market_id: fill.intent.market_id.clone(),
            outcome_id: fill.intent.outcome_id,
            side: fill.intent.side,
            contracts: fill.intent.contracts.0,
            fill_price: fill.simulated_fill_price,
        };
        let filled_key =
            MarketOutcomeId::new(fill.intent.market_id.clone(), fill.intent.outcome_id);

        // Authoritative path (issue #397): Supabase RPC first, then SQLite mirror. The client
        // and Arc are cloned (cheap) so neither borrows `self` across the `.await`.
        if self.supabase_state.is_some() {
            let event_seq = i64::try_from(seq.0).unwrap_or(i64::MAX);
            let fill_row = FillRow {
                idempotency_key: record.idempotency_key.clone(),
                market_id: record.market_id.clone(),
                outcome_id: record.outcome_id,
                side: record.side,
                contracts: record.contracts,
                fill_price: record.fill_price,
                event_seq,
            };
            // Every Winner-Follow fill is `wf|`-keyed; a non-`wf` key has no leader and cannot
            // satisfy `paper_fills.leader_wallet` (NOT NULL) — skip it fail-closed.
            let Some(sup_row) = supabase_fill_from(&fill_row) else {
                error!(
                    key = %record.idempotency_key,
                    "authoritative fill: non-winner-follow key has no leader; skipping (fail-closed)"
                );
                return PaperCommitResult::Parked; // frame is durable; boot frame-walk skips it
            };
            let parked = ParkedFill {
                leader: leader.clone(),
                record: record.clone(),
                seq,
                sup_row: Some(sup_row),
                dispatch_id: dispatch_id.map(str::to_owned),
                filled_key,
                decision_evidence: decision_evidence.cloned(),
            };
            // Bounded in-process attempts before parking (#508 round-4 posture).
            let mut result = PaperCommitResult::Parked;
            for attempt in 1u32..=3 {
                result = self
                    .attempt_parked_commit(trade.source_trade_id.clone(), &parked)
                    .await;
                if !matches!(result, PaperCommitResult::Parked) {
                    break;
                }
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_millis(LOCAL_COMMIT_RETRY_DELAY_MS)).await;
                }
            }
            if matches!(result, PaperCommitResult::Parked) {
                error!(
                    key = %record.idempotency_key,
                    seq = seq.0,
                    "authoritative fill: no terminal outcome after bounded retries; PARKED \
                     (#511 frozen retry — ticker + boot frame-walk converge; never re-admitted)"
                );
                self.parked.insert(trade.source_trade_id.clone(), parked);
            } else if let PaperCommitResult::Filled = result {
                // Liquidity-at-fill capture (#350) stays alive in authoritative mode (its
                // sink is a separate worker, not gated off with `run_sink`).
                enqueue_if_buy(
                    self.snapshot_sink.as_ref(),
                    fill.intent.side,
                    &fill.intent.idempotency_key,
                    &fill.intent.market_id,
                    fill.intent.outcome_id,
                    OffsetDateTime::now_utc().unix_timestamp(),
                );
            }
            return result;
        }

        // Legacy path: SQLite authoritative + best-effort Supabase sink mirror. A failed
        // commit retries in-process (#508 round-4) and finally parks (#511) — the caller
        // must NOT mark the contract filled on an unacknowledged commit.
        let flip = dispatch_id.map(|id| pe_paper_state::DispatchFlip {
            dispatch_id: id,
            paper_outcome: "fill",
        });
        let fill_pending = match render_pending_evidence(
            decision_evidence,
            AuthorityEvidence::local("committed"),
            recorded_fill_terminal(&record, seq),
        ) {
            Ok(value) => value,
            Err(error) => {
                error!(%error, trade = %trade.source_trade_id, "encode fill decision evidence failed");
                return PaperCommitResult::Parked;
            }
        };
        let settled_pending = match render_pending_evidence(
            decision_evidence,
            AuthorityEvidence::local("settled_refusal"),
            TerminalDispositionEvidence::settled_refusal(),
        ) {
            Ok(value) => value,
            Err(error) => {
                error!(%error, trade = %trade.source_trade_id, "encode refusal decision evidence failed");
                return PaperCommitResult::Parked;
            }
        };
        let mut committed = None;
        for attempt in 1..=LOCAL_COMMIT_RETRIES {
            match self.paper_state.commit_fill_with_flip_pending(
                &trade.source_trade_id,
                leader,
                &record,
                seq,
                flip,
                fill_pending.as_ref().map(pending_terminal),
                settled_pending.as_ref().map(pending_terminal),
            ) {
                Ok(outcome) => {
                    committed = Some(outcome);
                    break;
                }
                Err(e) if attempt < LOCAL_COMMIT_RETRIES => {
                    error!(error = %e, attempt, "paper-state commit_fill failed; retrying in-process");
                    tokio::time::sleep(Duration::from_millis(LOCAL_COMMIT_RETRY_DELAY_MS)).await;
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "paper-state commit_fill failed after in-process retries; PARKED \
                         (#511 frozen retry via the ticker)"
                    );
                }
            }
        }
        match committed {
            Some(pe_paper_state::FillCommitOutcome::Applied(new_bankroll)) => {
                self.bankroll = new_bankroll;
                // Best-effort mirror to Supabase (issue #343). Never blocks the trade path.
                if let Some(sink) = &self.sink {
                    sink.send_fill(FillRow {
                        idempotency_key: record.idempotency_key,
                        market_id: record.market_id,
                        outcome_id: record.outcome_id,
                        side: record.side,
                        contracts: record.contracts,
                        fill_price: record.fill_price,
                        event_seq: i64::try_from(seq.0).unwrap_or(i64::MAX),
                    });
                }
                // Liquidity-at-fill capture (issue #350 WS2 PR-H): enqueue a snapshot request
                // for BUY fills only, off the hot path. Non-blocking (drop-on-full); `record`
                // is moved into `FillRow` above, so read the still-borrowed `fill.intent`.
                enqueue_if_buy(
                    self.snapshot_sink.as_ref(),
                    fill.intent.side,
                    &fill.intent.idempotency_key,
                    &fill.intent.market_id,
                    fill.intent.outcome_id,
                    OffsetDateTime::now_utc().unix_timestamp(),
                );
                PaperCommitResult::Filled
            }
            Some(pe_paper_state::FillCommitOutcome::RefusedSettled(bankroll)) => {
                self.bankroll = bankroll;
                info!(
                    key = %record.idempotency_key,
                    market = %record.market_id,
                    "fill refused: market already settled (terminal no-fill disposition)"
                );
                PaperCommitResult::Refused
            }
            None => {
                self.parked.insert(
                    trade.source_trade_id.clone(),
                    ParkedFill {
                        leader: leader.clone(),
                        record,
                        seq,
                        sup_row: None,
                        dispatch_id: dispatch_id.map(str::to_owned),
                        filled_key,
                        decision_evidence: decision_evidence.cloned(),
                    },
                );
                PaperCommitResult::Parked
            }
        }
    }

    /// One bounded attempt to bring a frozen post-frame fill to a terminal outcome via
    /// the authority (#511). Updates bankroll / filled-positions on success. Returns
    /// `Parked` when no terminal outcome was reached.
    async fn attempt_parked_commit(
        &mut self,
        source_trade_id: SourceTradeId,
        parked: &ParkedFill,
    ) -> PaperCommitResult {
        let Some(supabase) = self.supabase_state.clone() else {
            // Legacy: the frozen local commit is the terminal protocol.
            let flip = parked
                .dispatch_id
                .as_deref()
                .map(|id| pe_paper_state::DispatchFlip {
                    dispatch_id: id,
                    paper_outcome: "fill",
                });
            let fill_pending = match render_pending_evidence(
                parked.decision_evidence.as_ref(),
                AuthorityEvidence::local("committed"),
                recorded_fill_terminal(&parked.record, parked.seq),
            ) {
                Ok(value) => value,
                Err(error) => {
                    error!(%error, trade = %source_trade_id, "encode parked fill evidence failed");
                    return PaperCommitResult::Parked;
                }
            };
            let settled_pending = match render_pending_evidence(
                parked.decision_evidence.as_ref(),
                AuthorityEvidence::local("settled_refusal"),
                TerminalDispositionEvidence::settled_refusal(),
            ) {
                Ok(value) => value,
                Err(error) => {
                    error!(%error, trade = %source_trade_id, "encode parked refusal evidence failed");
                    return PaperCommitResult::Parked;
                }
            };
            return match self.paper_state.commit_fill_with_flip_pending(
                &source_trade_id,
                &parked.leader,
                &parked.record,
                parked.seq,
                flip,
                fill_pending.as_ref().map(pending_terminal),
                settled_pending.as_ref().map(pending_terminal),
            ) {
                Ok(pe_paper_state::FillCommitOutcome::Applied(b)) => {
                    self.bankroll = b;
                    self.filled_positions.insert(parked.filled_key.clone());
                    PaperCommitResult::Filled
                }
                Ok(pe_paper_state::FillCommitOutcome::RefusedSettled(b)) => {
                    self.bankroll = b;
                    PaperCommitResult::Refused
                }
                Err(e) => {
                    error!(error = %e, "parked legacy fill commit failed; will retry");
                    PaperCommitResult::Parked
                }
            };
        };
        let Some(sup_row) = parked.sup_row.as_ref() else {
            // Unreachable: authoritative parks always carry the frozen v2 request.
            error!("parked authoritative fill without a frozen request; will retry");
            return PaperCommitResult::Parked;
        };
        let paper_state = self.paper_state.clone();
        let flip = parked
            .dispatch_id
            .as_deref()
            .map(|id| pe_paper_state::DispatchFlip {
                dispatch_id: id,
                paper_outcome: "fill",
            });
        match commit_fill_authoritative(
            &supabase,
            &paper_state,
            &source_trade_id,
            &parked.leader,
            &parked.record,
            parked.seq,
            sup_row,
            flip,
            parked.decision_evidence.as_ref(),
        )
        .await
        {
            Ok(AuthoritativeFillOutcome::Filled(bankroll)) => {
                self.bankroll = bankroll;
                self.filled_positions.insert(parked.filled_key.clone());
                PaperCommitResult::Filled
            }
            Ok(AuthoritativeFillOutcome::LocalDurabilityUncertain(bankroll)) => {
                // Remote committed, local did not converge: financial state is
                // uncertain. Do NOT advance in-memory bankroll/positions past
                // durable local truth; fail readiness and stop producer intake —
                // the durable frame + idempotent RPC reconverge at boot (#544).
                error!(
                    key = %parked.record.idempotency_key,
                    market = %parked.record.market_id,
                    authority_bankroll = %bankroll,
                    "authoritative fill uncertain: remote committed, local mirror failed; stopping producers"
                );
                {
                    let mut health = self
                        .health
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    health.paper_durability_uncertain = true;
                    health.refresh_event_log_writable();
                }
                self.intake_stopped = true;
                PaperCommitResult::DurabilityUncertain
            }
            Ok(AuthoritativeFillOutcome::RefusedSettled(bankroll)) => {
                self.bankroll = bankroll;
                info!(
                    key = %parked.record.idempotency_key,
                    market = %parked.record.market_id,
                    "fill refused by authority: market already settled (terminal)"
                );
                PaperCommitResult::Refused
            }
            Err(e) => {
                warn!(error = %e, key = %parked.record.idempotency_key, "authoritative fill attempt failed");
                PaperCommitResult::Parked
            }
        }
    }

    /// #511 frozen-retry ticker: drive every parked fill toward a terminal outcome.
    pub async fn retry_parked(&mut self) {
        let ids: Vec<SourceTradeId> = self.parked.keys().cloned().collect();
        for id in ids {
            let Some(parked) = self.parked.remove(&id) else {
                continue;
            };
            match self.attempt_parked_commit(id.clone(), &parked).await {
                PaperCommitResult::Parked => {
                    self.parked.insert(id, parked);
                }
                // Uncertainty keeps the frozen obligation: boot reconverges it.
                PaperCommitResult::DurabilityUncertain => {
                    self.parked.insert(id, parked);
                }
                PaperCommitResult::Filled | PaperCommitResult::Refused => {}
            }
        }
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

/// VWAP over the first `contracts` whole shares of a planned ladder prefix (#508). Used to
/// re-price the recorded paper fill when the strategy sizes below the planned quantity, so
/// the recorded price is exact for the executed size. `None` when the prefix cannot cover
/// `contracts` (caller falls back to the plan VWAP) or the count is zero.
fn prefix_vwap(used_asks: &[AskLevel], contracts: u64) -> Option<Price> {
    if contracts == 0 {
        return None;
    }
    let mut remaining = contracts.checked_mul(1_000_000)?;
    let mut spend = Decimal::ZERO;
    for level in used_asks {
        let take = remaining.min(level.shares.atomic());
        spend = spend.checked_add(
            ShareAmount::from_atomic(take)
                .to_decimal()
                .checked_mul(level.price.0)?,
        )?;
        remaining -= take;
        if remaining == 0 {
            break;
        }
    }
    if remaining != 0 {
        return None;
    }
    Price::new(spend / Decimal::from(contracts)).ok()
}

// ── Stub risk snapshot ────────────────────────────────────────────────────────

/// Build a zeroed [`RiskSnapshot`] with LiveTiny mode.
///
/// All exposure and PnL fields are zero; no anti-gaming flags; source healthy.
/// Phase 0B stub — real exposure tracking is a separate later issue.
///
/// `concentration_caps` is `None`: concentration enforcement is un-enforced by owner
/// decision on the production copy path (#508 Phase A; recorded in docs/19-). The
/// drawdown/latency kill switches stay armed — they are inert here only because the
/// PnL/latency inputs are zeroed by this stub.
///
/// `evaluate()` overwrites `trading_mode` and `proposed_trade_bps` before calling
/// the risk gate, so their initial values here are overridden.
fn zeroed_risk_snapshot() -> RiskSnapshot {
    use pe_core_types::BasisPoints;
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        onchain_source_status: SourceStatus::Healthy,
        copy_latency_p95_ms: 0,
        trading_mode: TradingMode::LiveTiny, // overridden by evaluate()
        proposed_trade_bps: BasisPoints(0),  // overridden by evaluate()
        per_trade_cap_bps: 0,                // overridden by evaluate()
        concentration_caps: None,            // un-enforced by owner decision (#508)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::check_resolution_horizon;

    const NOW: i64 = 1_700_000_000;

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
