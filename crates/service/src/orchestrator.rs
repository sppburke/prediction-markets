//! Event dispatch loop: routes decoded trade events to the copy-signal-engine,
//! then gates signals through strategy evaluation and execution dispatch.
//!
//! The orchestrator is dispatch + sizing: its I/O is the `ExecutionDispatcher`, the mid-price
//! cache (Gamma), and the CLOB `/book` fetcher — read on every paper `clob_best_ask` BUY for the
//! best-ask fill basis (#486) and, when the price-impact gate is on, for the size cap (#398 WS2).

use std::collections::{HashMap, HashSet};
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Duration;

use pe_copy_signal_engine::{IncomingTrade, LeaderSignal, SignalConfig, classify_trade};
use pe_core_types::{
    EventSeq, MarketId, MarketOutcomeId, Price, Probability, ReconstructionQuality, Side,
    SourceTimestamp, TraderId, VenueId, WalletAddress,
};
use pe_execution_core::{DispatchResult, ExecutionDispatcher};
use pe_paper_state::{FillRecord, FillRow, LeaderPositionRow, PaperStateDb};
use pe_position_ledger::PositionLedger;
use pe_risk_engine::{RiskSnapshot, TradingMode};
use pe_source_core::SourceStatus;
use pe_source_polymarket_public::PageFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, FillSource, PaperExecutionError, PaperExecutor, PaperFill, WinnerFollowStrategy,
};
use pe_trader_index::Watchlist;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::clob_book::ClobBookFetcher;
use crate::entry_gate::{CopyEntryGate, CopyEntryGateConfig};
use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
use crate::market_end_cache::MarketEndCache;
use crate::mid_price_cache::MidPriceCache;
use crate::orchestrator_control::OrchestratorControl;
use crate::runtime_config::{self, FillMode, LiveRuntimeConfig};
use crate::snapshot_worker::{SnapshotHandle, absorbable_contracts_within_bps, enqueue_if_buy};
use crate::supabase_sink::{SinkHandle, supabase_fill_from};
use crate::supabase_state::{SupabaseStateClient, commit_fill_authoritative};

/// Hot-path `/book` fetch timeout for the price-impact gate (#398 WS2). Tighter than the worker's
/// 5 s per-request timeout so a slow book fails open (no cap) without stalling the trade.
/// Canonical default in `docs/_GLOSSARY.md`: `clob_book_hot_path_timeout_secs`.
const CLOB_BOOK_HOT_PATH_TIMEOUT_SECS: u64 = 2;

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
    /// Fallback BUY haircut (bps) applied to the leader price when a `clob_best_ask` fill has no
    /// usable best-ask (#486). Refreshed per event alongside `fill_mode`.
    pub clob_best_ask_fallback_haircut_bps: u32,
    /// Copy-entry gate config (first-entry/fail-closed posture). The per-wallet
    /// market history is supplied separately to [`Orchestrator::new`].
    pub entry_gate_config: CopyEntryGateConfig,
    /// Supabase-authoritative runtime config (#398 WS1). `Some` in production: `handle_trade`
    /// rebuilds the strategy/mode/gate knobs from its snapshot per event. `None` (tests) keeps the
    /// boot config — the per-event rebuild is skipped.
    pub runtime_config: Option<LiveRuntimeConfig>,
}

pub struct Orchestrator<F: PageFetcher + Send + Sync, B: ClobBookFetcher> {
    trade_rx: mpsc::Receiver<IncomingTrade>,
    position_ledger: PositionLedger,
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
    // Copy-entry gate: admits only a leader's first-ever BUY entry into a market
    // (#290; price band removed in #339).
    entry_gate: CopyEntryGate,
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
    // worker so the 5 rps rate gate is global. Consulted only when `price_impact_cap_bps != 0`.
    book_fetcher: Arc<B>,
    // Price-impact gate cap in bps, rebuilt per event from the runtime-config snapshot. `0`
    // disables the gate (fail-open; no `/book` fetch).
    price_impact_cap_bps: i32,
    // Paper fill-price mode (#486), rebuilt per event from the runtime-config snapshot. Gates
    // whether a paper BUY fetches the CLOB best-ask (`ClobBestAsk`) or uses the boot-frozen
    // haircut (`LeaderHaircut`).
    fill_mode: FillMode,
    // Fallback BUY haircut (bps) for a `clob_best_ask` fill with no usable ask (#486), rebuilt
    // per event alongside `fill_mode`.
    clob_best_ask_fallback_haircut_bps: u32,
}

fn apply_control_message(
    entry_gate: &mut CopyEntryGate,
    position_ledger: &mut PositionLedger,
    message: OrchestratorControl,
) {
    match message {
        OrchestratorControl::PositionReseed(map) => {
            let wallets = map.len();
            position_ledger.overlay(map);
            info!(wallets, "leader ledger reseeded from live positions API");
        }
        OrchestratorControl::PrepareAdmissions {
            history,
            positions,
            acknowledged,
        } => {
            let wallets = positions.len();
            entry_gate.merge_history(history);
            position_ledger.overlay(positions);
            // The acknowledgement is intentionally last: membership remains unpublished until
            // both single-owner ledgers contain the admission prerequisites.
            let _ = acknowledged.send(());
            info!(wallets, "hot-watchlist admission state prepared");
        }
    }
}

impl<F: PageFetcher + Send + Sync, B: ClobBookFetcher> Orchestrator<F, B> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trade_rx: mpsc::Receiver<IncomingTrade>,
        live_watchlist: LiveWatchlist,
        config: OrchestratorConfig,
        history_map: HashMap<WalletAddress, HashSet<MarketId>>,
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

        // Seed the dedup set from any positions already in the DB (crash-restart safety).
        let filled_positions: HashSet<MarketOutcomeId> = paper_state
            .paper_positions()
            .map_err(|e| anyhow::anyhow!("load paper positions: {e}"))?
            .into_iter()
            .filter(|p| p.long_contracts > 0 || p.short_contracts > 0)
            .map(|p| MarketOutcomeId::new(p.market_id, p.outcome_id))
            .collect();

        Ok(Self {
            trade_rx,
            position_ledger: leader_ledger,
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
            max_resolution_horizon_secs: config.max_resolution_horizon_secs,
            min_resolution_horizon_secs: config.min_resolution_horizon_secs,
            max_fill_price: config.max_fill_price,
            min_fill_price: config.min_fill_price,
            paper_fill_haircut_bps: config.paper_fill_haircut_bps,
            paper_fill_slippage_bps: config.paper_fill_slippage_bps,
            filled_positions,
            entry_gate: CopyEntryGate::new(config.entry_gate_config, history_map),
            min_quality,
            control_rx,
            sink,
            snapshot_sink,
            supabase_state,
            runtime_config: config.runtime_config,
            book_fetcher,
            price_impact_cap_bps: 0,
            fill_mode: config.fill_mode,
            clob_best_ask_fallback_haircut_bps: config.clob_best_ask_fallback_haircut_bps,
        })
    }

    /// Run the dispatch loop until the trade channel closes OR until the
    /// provided `shutdown` future resolves.
    ///
    /// On shutdown, remaining trades already buffered in the channel are drained
    /// and processed before returning — no in-flight fills are lost.
    pub async fn run(mut self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        let mut trades_done = false;
        let mut control_done = false;

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
                result = self.control_rx.recv(), if !control_done => {
                    match result {
                        Some(message) => apply_control_message(
                            &mut self.entry_gate,
                            &mut self.position_ledger,
                            message,
                        ),
                        None => control_done = true,
                    }
                }
                result = self.trade_rx.recv(), if !trades_done => {
                    match result {
                        Some(trade) => self.handle_trade(trade).await,
                        None => trades_done = true,
                    }
                }
            }
        }
    }

    // ── Private handlers ──────────────────────────────────────────────────────

    /// Contracts absorbable within `price_impact_cap_bps` of best ask from the live `/book`
    /// (#398 WS2 step 5c). Returns:
    /// - `None` — gate disabled, missing CLOB token, or a `/book` fetch error/timeout: **fail-open**
    ///   (no cap, risk #4), or an implausibly deep book whose contract count overflows `u64`.
    /// - `Some(0)` — a successful read with nothing absorbable within the band (empty ask book, or
    ///   no level within `bps`): the trade is **skipped** (distinct from fail-open).
    /// - `Some(n)` — cap the size at `n` contracts.
    async fn book_cap_contracts(&self, signal: &LeaderSignal) -> Option<u64> {
        let cap_bps = self.price_impact_cap_bps;
        if cap_bps <= 0 {
            return None; // gate disabled → fail-open
        }
        let bps = u64::try_from(cap_bps).ok()?;
        // The outcome's CLOB token id (served from the mid cache, warm from the price fetch above).
        let snaps = self
            .mid_price_cache
            .fetch_snapshots(std::slice::from_ref(&signal.market_id))
            .await;
        let Some(token_id) = snaps.get(&signal.market_id).and_then(|s| {
            s.clob_token_ids
                .get(usize::from(signal.outcome_id.0))
                .cloned()
        }) else {
            return None; // missing token → fail-open (risk #4)
        };
        match tokio::time::timeout(
            Duration::from_secs(CLOB_BOOK_HOT_PATH_TIMEOUT_SECS),
            self.book_fetcher.fetch_book(&token_id),
        )
        .await
        {
            Ok(Ok(book)) => match absorbable_contracts_within_bps(&book, bps) {
                Some(d) => Some(d.floor().to_u64().unwrap_or(u64::MAX)),
                // `None` = no best ask (empty asks → 0 absorbable → skip) or a Decimal overflow on
                // a non-empty (implausibly deep) book → fail-open.
                None if book.asks.is_empty() => Some(0),
                None => None,
            },
            Ok(Err(e)) => {
                info!(error = %e, market = %signal.market_id, "price-impact /book fetch failed; gate fails open");
                None
            }
            Err(_) => {
                info!(market = %signal.market_id, "price-impact /book fetch timed out; gate fails open");
                None
            }
        }
    }

    /// Resolve the paper fill basis + its provenance for `signal` (#486).
    ///
    /// Paper mode with `fill_mode == ClobBestAsk` and a BUY fetches the fresh CLOB `/book` for the
    /// signal's outcome token and returns the best-ask ([`FillSource::ClobBestAsk`]) when usable,
    /// else the `clob_best_ask_fallback_haircut_bps` fallback ([`FillSource::Fallback`]). The
    /// production copy-entry gate rejects SELLs before this method. Its defensive non-BUY branch
    /// retains the generic executor's SELL haircut but is unreachable from Winner-Follow.
    /// `LeaderHaircut` mode and every non-paper mode return the boot-frozen haircut price
    /// ([`FillSource::LeaderHaircut`]), byte-identical to the pre-#486 basis. Only the paper-mode
    /// result is threaded to the executor as `observed_fill_price`; other modes pass `None` and
    /// the executor recomputes the identical haircut.
    async fn resolve_fill_price(
        &self,
        signal: &LeaderSignal,
    ) -> Result<(Price, FillSource), PaperExecutionError> {
        // Non-paper modes and the leader_haircut fill mode: the boot-frozen local haircut, no fetch.
        if self.mode != ExecutionMode::Paper || self.fill_mode != FillMode::ClobBestAsk {
            let price = PaperExecutor::fill_price(
                signal.leader_side,
                signal.leader_price,
                self.paper_fill_haircut_bps,
                self.paper_fill_slippage_bps,
            )?;
            return Ok((price, FillSource::LeaderHaircut));
        }
        // clob_best_ask paper mode. The best-ask is the BUY-side price the copy would cross, and
        // the `/book` has no bids, so only a BUY fetches; a SELL falls through to the shared SELL
        // haircut branch below.
        if signal.leader_side == Side::Buy
            && let Some(ask) = self.fetch_best_ask(signal).await
        {
            return Ok((ask, FillSource::ClobBestAsk));
        }
        // Fallback: leader_price × (1 + clob_best_ask_fallback_haircut_bps/10_000) on a BUY, or the
        // shared SELL slippage branch. The SELL branch of `fill_price` ignores the haircut arg and
        // applies only `slippage_bps`, so pass the boot-frozen `paper_fill_slippage_bps` to keep a
        // SELL byte-identical to the pre-#486 fill; the haircut arg is dead there and drives a BUY.
        let price = PaperExecutor::fill_price(
            signal.leader_side,
            signal.leader_price,
            self.clob_best_ask_fallback_haircut_bps,
            self.paper_fill_slippage_bps,
        )?;
        Ok((price, FillSource::Fallback))
    }

    /// Fetch the fresh CLOB best-ask (positive price AND positive size) for `signal`'s outcome
    /// token, or `None` on any non-usable outcome — missing CLOB token, `/book` fetch error or
    /// timeout, empty book, a best-ask that fails `Price::new`, or a zero / zero-size level — so
    /// the caller takes the fallback haircut (#486). The 2 s timeout mirrors the price-impact gate.
    async fn fetch_best_ask(&self, signal: &LeaderSignal) -> Option<Price> {
        // The outcome's CLOB token id (served from the mid cache, warm from the mid gate above).
        let snaps = self
            .mid_price_cache
            .fetch_snapshots(std::slice::from_ref(&signal.market_id))
            .await;
        let token_id = snaps.get(&signal.market_id).and_then(|s| {
            s.clob_token_ids
                .get(usize::from(signal.outcome_id.0))
                .cloned()
        })?;
        let book = match tokio::time::timeout(
            Duration::from_secs(CLOB_BOOK_HOT_PATH_TIMEOUT_SECS),
            self.book_fetcher.fetch_book(&token_id),
        )
        .await
        {
            Ok(Ok(book)) => book,
            Ok(Err(e)) => {
                info!(error = %e, market = %signal.market_id, "best-ask /book fetch failed; using fallback haircut");
                return None;
            }
            Err(_) => {
                info!(market = %signal.market_id, "best-ask /book fetch timed out; using fallback haircut");
                return None;
            }
        };
        // `OrderBook::best_ask` ignores level size, so compute the min price among positive-size
        // levels here: a zero-size dust level must not set the fill basis (and slip a copy past the
        // band gate). A non-positive best-ask (unreachable at 0.01 ticks) is guarded defensively.
        let best_ask = book
            .asks
            .iter()
            .filter(|l| l.size > Decimal::ZERO && l.price > Decimal::ZERO)
            .map(|l| l.price)
            .min()?;
        Price::new(best_ask).ok()
    }

    async fn handle_trade(&mut self, trade: IncomingTrade) {
        // #398 WS1: rebuild the runtime-mutable knobs from the latest Supabase config snapshot,
        // once per event, so an admin edit takes effect within one poll with no restart. Reads
        // only — the running `self.bankroll` (owned by the fill-commit path, #397) is untouched.
        if let Some(rc) = self.runtime_config.as_ref().map(|live| live.snapshot()) {
            self.strategy.set_config(rc.winner_follow_config());
            if let Some(m) = runtime_config::parse_execution_mode(&rc.mode) {
                self.mode = m;
            }
            if let Ok(price) = Decimal::from_str(&rc.max_fill_price) {
                self.max_fill_price = price;
            }
            if let Ok(price) = Decimal::from_str(&rc.min_fill_price) {
                self.min_fill_price = price;
            }
            self.max_resolution_horizon_secs = rc.max_resolution_horizon_secs;
            self.min_resolution_horizon_secs = rc.min_resolution_horizon_secs;
            self.entry_gate.set_fail_closed(rc.entry_gate_fail_closed);
            self.price_impact_cap_bps = rc.price_impact_cap_bps;
            // #486: fill mode + fallback haircut ARE runtime-mutable — the executor is a pure
            // recorder of the orchestrator-resolved basis, so no boot-frozen executor knob desyncs.
            self.fill_mode = rc.fill_mode;
            self.clob_best_ask_fallback_haircut_bps = rc.clob_best_ask_fallback_haircut_bps;
            // NOTE: paper_fill_haircut/slippage_bps are deliberately NOT refreshed here. The
            // `PaperExecutor` that records the fill bakes them in at boot with no runtime setter,
            // so refreshing only the sizing side would desync sizing from the recorded fill after
            // a live edit. They stay boot-frozen on both sides; changing them needs a restart.
        }

        // Mark polymarket freshness.
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
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                error!(error = %e, "paper-state is_seen failed; skipping trade");
                return;
            }
        }

        // One consistent watchlist snapshot for this event (ArcSwap hot path): every
        // watchlist lookup below reads the same generation.
        let watchlist = self.live_watchlist.snapshot();

        // Look up wallet in watchlist; skip non-watchlisted wallets.
        let quality = self.quality_for(&watchlist, &trade.wallet);

        // Capture pre-trade snapshot: classify_action uses pre-trade position to determine
        // Entry/Add/Flip/Trim/Exit. Ingest must follow so the ledger advances after
        // classification, not before.
        let position = self.position_ledger.position(&trade.wallet).cloned();

        // Advance position ledger after classification inputs are captured.
        self.position_ledger.ingest(&trade);

        // Snapshot the leader's post-ingest position for this (market, outcome) so it
        // is mirrored into paper-state on every processed trade — fill or no fill.
        let leader_row = self.leader_position_row(&trade);

        let Some(signal) = classify_trade(
            &trade,
            position.as_ref(),
            &watchlist,
            quality,
            VenueId::polymarket(),
            &self.signal_config,
        ) else {
            // No signal: still mark seen + mirror the leader ledger (AC2).
            self.commit_no_fill(&trade, &leader_row);
            return;
        };

        // Single-position-per-contract gate: drop if we already hold this (market, outcome).
        let pos_key = MarketOutcomeId::new(signal.market_id.clone(), signal.outcome_id);
        if self.filled_positions.contains(&pos_key) {
            info!(
                reason = "already hold position in this market outcome",
                market = %signal.market_id,
                outcome = signal.outcome_id.0,
                "signal did not produce order",
            );
            self.commit_no_fill(&trade, &leader_row);
            return;
        }

        // Copy-entry gate: copy only a leader's first-ever BUY entry into a market
        // (#290; price band removed in #339).
        if let Some(reason) = self.entry_gate.admit(&signal) {
            info!(
                reason = %reason,
                market = %signal.market_id,
                leader_price = %signal.leader_price.0,
                "signal did not produce order",
            );
            self.commit_no_fill(&trade, &leader_row);
            return;
        }
        // Record the admitted BUY entry so a same-session re-entry into this market is
        // blocked even if a later gate or the strategy rejects this signal.
        self.entry_gate
            .record_entry(signal.leader.0, &signal.market_id);

        // Resolution-horizon gate (#290, #339): copy only markets whose resolution time
        // (umaEndDate, else the always-present endDate) is known AND sits within
        // [min, max] seconds from now. Too far out locks capital for months; too soon
        // cannot be filled and held. Fail closed when the resolution time is unknown —
        // we cannot confirm the horizon, so we do not enter. One lookup serves both
        // bounds.
        if self.max_resolution_horizon_secs > 0 || self.min_resolution_horizon_secs > 0 {
            let resolution_unix = self
                .market_end_cache
                .resolution_unix(&signal.market_id)
                .await;
            let now_unix = OffsetDateTime::now_utc().unix_timestamp();
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
                self.commit_no_fill(&trade, &leader_row);
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
                Some(p) => p,
                None => {
                    info!(
                        reason = "current market price unavailable",
                        market = %signal.market_id,
                        outcome = signal.outcome_id.0,
                        "signal did not produce order",
                    );
                    self.commit_no_fill(&trade, &leader_row);
                    return;
                }
            }
        };

        // Realistic fill basis (#339 revisited, #486): the price the copy will ACTUALLY fill at.
        // In paper `clob_best_ask` mode a BUY resolves to the fresh CLOB best-ask (else the
        // fallback haircut); otherwise the leader price adjusted by the boot-frozen paper haircut
        // (`PaperExecutor::fill_price`). Size and band-gate against THIS, so notional ==
        // `sizing_dollar_usd` and the gates check the price actually paid. The haircut basis mirrors
        // the backtest's slippage-adjusted `fill_price` (`crates/backtest` `simulation.rs`); the
        // best-ask basis is a fresher, more conservative live-execution proxy with no backtest
        // analog (the ranker models the fill as trade-print + 1¢; #486 Context). In paper mode the
        // basis is recorded verbatim by the executor (via `observed_fill_price` below). The
        // fail-closed arm is defensive.
        let (fill_basis, fill_source) = match self.resolve_fill_price(&signal).await {
            Ok(pair) => pair,
            Err(_) => {
                info!(
                    reason = "leader price yields no constructible fill price",
                    market = %signal.market_id,
                    leader_price = %signal.leader_price.0,
                    "signal did not produce order",
                );
                self.commit_no_fill(&trade, &leader_row);
                return;
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
            self.commit_no_fill(&trade, &leader_row);
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
            self.commit_no_fill(&trade, &leader_row);
            return;
        }

        // Price-impact gate (#398 WS2 step 5c): when enabled, cap the size at the contracts
        // absorbable within `price_impact_cap_bps` of best ask from the live /book.
        let book_cap_contracts = self.book_cap_contracts(&signal).await;

        let p = self.win_rate_p_for(&watchlist, &signal.leader);
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
            self.bankroll,
            self.mode,
            book_cap_contracts,
            Some(fill_basis),
        ) {
            Err(e) => {
                info!(reason = %e, "signal did not produce order");
                self.commit_no_fill(&trade, &leader_row);
            }
            Ok(intent) => {
                let now = SourceTimestamp(OffsetDateTime::now_utc());
                // Paper mode records the resolved basis verbatim. Shadow recomputes the identical
                // boot-frozen haircut from `None`. Ordinary live modes fail closed in the dispatcher.
                let observed_fill_price =
                    (self.mode == ExecutionMode::Paper).then_some((fill_basis, fill_source));
                match self
                    .dispatcher
                    .execute(&intent, self.mode, now, observed_fill_price)
                    .await
                {
                    Err(e) => {
                        error!(error = %e, "execution dispatcher failed");
                        self.commit_no_fill(&trade, &leader_row);
                    }
                    Ok(DispatchResult::Paper { fill, seq }) => {
                        let filled_key = MarketOutcomeId::new(
                            fill.intent.market_id.clone(),
                            fill.intent.outcome_id,
                        );
                        // `false` = authoritative fail-closed skip: the Supabase RPC failed, so
                        // neither the position nor the dedup advanced (the event log holds the
                        // fill and replays on restart). Do not mark the contract filled.
                        if self
                            .commit_paper_fill(&trade, &leader_row, &fill, seq)
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

    /// Build the leader-position mirror row for the `(market, outcome)` this trade
    /// touched, read from the in-memory ledger *after* the trade was ingested.
    fn leader_position_row(&self, trade: &IncomingTrade) -> LeaderPositionRow {
        let key = MarketOutcomeId::new(trade.market_id.clone(), trade.outcome_id);
        let (long, short) = self
            .position_ledger
            .position(&trade.wallet)
            .and_then(|snap| snap.positions.get(&key))
            .map(|st| (st.long_contracts, st.short_contracts))
            .unwrap_or((0, 0));
        LeaderPositionRow {
            wallet: trade.wallet,
            market_id: trade.market_id.clone(),
            outcome_id: trade.outcome_id,
            long_contracts: long,
            short_contracts: short,
        }
    }

    /// Commit dedup + leader-ledger mirror for a processed trade that produced no fill.
    fn commit_no_fill(&self, trade: &IncomingTrade, leader: &LeaderPositionRow) {
        if let Err(e) = self
            .paper_state
            .commit_seen_no_fill(&trade.source_trade_id, leader)
        {
            error!(error = %e, "paper-state commit_seen_no_fill failed");
        }
    }

    /// Commit dedup + leader-ledger mirror + fill accounting, and update the in-memory
    /// bankroll to the new persisted value (drawdown-aware sizing). Returns `true` when the
    /// fill committed (the caller then marks the contract filled); `false` only on an
    /// authoritative fail-closed skip.
    ///
    /// Authoritative mode (issue #397, `self.supabase_state.is_some()`): the `commit_fill` RPC
    /// is awaited FIRST (fail-closed — on RPC error, log + return `false`; the event log holds
    /// the fill and replays on restart), then SQLite is mirrored and `self.bankroll` is set to
    /// the RPC return. The RPC `.await` completes before the SQLite mutex is taken (inside
    /// `PaperStateDb::commit_fill`), so the lock is never held across the await. Legacy mode:
    /// SQLite stays authoritative with a best-effort Supabase sink mirror — behaviour unchanged.
    async fn commit_paper_fill(
        &mut self,
        trade: &IncomingTrade,
        leader: &LeaderPositionRow,
        fill: &PaperFill,
        seq: EventSeq,
    ) -> bool {
        let record = FillRecord {
            idempotency_key: fill.intent.idempotency_key.clone(),
            market_id: fill.intent.market_id.clone(),
            outcome_id: fill.intent.outcome_id,
            side: fill.intent.side,
            contracts: fill.intent.contracts.0,
            fill_price: fill.simulated_fill_price,
        };

        // Authoritative path (issue #397): Supabase RPC first, then SQLite mirror. The client
        // and Arc are cloned (cheap) so neither borrows `self` across the `.await`.
        if let Some(supabase) = self.supabase_state.clone() {
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
                return false;
            };
            let paper_state = self.paper_state.clone();
            match commit_fill_authoritative(
                &supabase,
                &paper_state,
                &trade.source_trade_id,
                leader,
                &record,
                seq,
                &sup_row,
            )
            .await
            {
                Ok(new_bankroll) => self.bankroll = new_bankroll,
                Err(e) => {
                    error!(
                        error = %e,
                        "authoritative fill: Supabase commit_fill RPC failed; fail-closed skip \
                         (event log holds the fill, replays on restart)"
                    );
                    return false;
                }
            }
            // Liquidity-at-fill capture (#350) stays alive in authoritative mode (its sink is a
            // separate worker, not gated off with `run_sink`).
            enqueue_if_buy(
                self.snapshot_sink.as_ref(),
                fill.intent.side,
                &fill.intent.idempotency_key,
                &fill.intent.market_id,
                fill.intent.outcome_id,
                OffsetDateTime::now_utc().unix_timestamp(),
            );
            return true;
        }

        // Legacy path: SQLite authoritative + best-effort Supabase sink mirror.
        match self
            .paper_state
            .commit_fill(&trade.source_trade_id, leader, &record, seq)
        {
            Ok(new_bankroll) => {
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
            }
            Err(e) => error!(error = %e, "paper-state commit_fill failed"),
        }
        true
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

/// Build a zeroed [`RiskSnapshot`] with LiveTiny mode.
///
/// All exposure and PnL fields are zero; no anti-gaming flags; source healthy.
/// Phase 0B stub — real exposure tracking is a separate later issue.
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
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use pe_copy_signal_engine::{LeaderSignal, PositionSnapshot, PositionState};
    use pe_core_types::{
        ContractQty, LeaderAction, MarketId, MarketOutcomeId, OutcomeId, Price, ProbabilityPpm,
        Quantity, ReconstructionQuality, Side, SourceTradeId, TraderId, VenueId, VenueMarketId,
        WalletAddress,
    };
    use pe_position_ledger::PositionLedger;
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;
    use tokio::sync::oneshot;

    use super::{apply_control_message, check_resolution_horizon};
    use crate::entry_gate::{CopyEntryGate, CopyEntryGateConfig, GateReject};
    use crate::orchestrator_control::OrchestratorControl;

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

    #[tokio::test]
    async fn admission_control_acknowledges_only_after_both_ledgers_are_ready() {
        let wallet = WalletAddress([9; 20]);
        let market = MarketId(VenueMarketId("0xknown".to_string()));
        let outcome = MarketOutcomeId::new(market.clone(), OutcomeId(0));
        let mut gate =
            CopyEntryGate::new(CopyEntryGateConfig { fail_closed: true }, HashMap::new());
        let mut ledger = PositionLedger::new();
        let (acknowledged, acknowledgement) = oneshot::channel();
        apply_control_message(
            &mut gate,
            &mut ledger,
            OrchestratorControl::PrepareAdmissions {
                history: HashMap::from([(wallet, HashSet::from([market.clone()]))]),
                positions: HashMap::from([(
                    wallet,
                    PositionSnapshot {
                        wallet,
                        positions: HashMap::from([(
                            outcome,
                            PositionState {
                                long_contracts: 7,
                                short_contracts: 0,
                            },
                        )]),
                    },
                )]),
                acknowledged,
            },
        );
        acknowledgement.await.unwrap();

        assert_eq!(
            ledger
                .position(&wallet)
                .unwrap()
                .positions
                .values()
                .next()
                .unwrap()
                .long_contracts,
            7
        );
        let signal = LeaderSignal {
            leader: TraderId(wallet),
            venue: VenueId::polymarket(),
            market_id: market,
            outcome_id: OutcomeId(0),
            action: LeaderAction::Entry,
            leader_side: Side::Buy,
            leader_price: Price(dec!(0.50)),
            leader_size: Quantity(ContractQty(1)),
            observed_at: OffsetDateTime::UNIX_EPOCH,
            received_at: OffsetDateTime::UNIX_EPOCH,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            source_trade_id: SourceTradeId("control-test".to_string()),
            action_confidence_ppm: ProbabilityPpm(1_000_000),
        };
        assert_eq!(gate.admit(&signal), Some(GateReject::NotFirstEntry));
    }
}
