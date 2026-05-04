//! Event dispatch loop: routes decoded source events to the funding-graph accumulator
//! and copy-signal-engine, then gates signals through strategy evaluation and paper
//! execution.
//!
//! # Architecture
//!
//! ```text
//! polygon_rx (SourceEvent) ──► FundingGraphAccumulator
//! trade_rx   (IncomingTrade) ──► classify_trade ──► WinnerFollowStrategy::evaluate
//!                                                ──► PaperExecutor::execute
//! ```
//!
//! The orchestrator is pure dispatch: it owns no I/O except through `PaperExecutor`.

use std::collections::HashSet;

use pe_copy_signal_engine::{IncomingTrade, SignalConfig, WalletProfile, classify_trade};
use pe_core_types::{ReconstructionQuality, SourceTimestamp, VenueId, WalletAddress};
use pe_funding_graph::FundingGraphAccumulator;
use pe_operator_graph::AntiGamingFlag;
use pe_position_ledger::{ClusterObservationTracker, PositionLedger};
use pe_risk_engine::{RiskSnapshot, TradingMode};
use pe_source_core::{SourceEvent, SourceStatus};
use pe_source_onchain_polygon::PolygonEvent;
use pe_strategy_winner_follow::{ExecutionMode, PaperExecutor, WinnerFollowStrategy};
use pe_trader_index::Watchlist;
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::health::SharedHealth;

/// Orchestrator configuration.
pub struct OrchestratorConfig {
    pub bankroll: Decimal,
    pub mode: ExecutionMode,
    pub signal_config: SignalConfig,
    /// How long [`ClusterObservationTracker`] retains entries in seconds.
    /// Default from `_GLOSSARY.md`: `cluster_observation_window_secs = 300`.
    pub cluster_observation_window_secs: u64,
}

pub struct Orchestrator {
    polygon_rx: mpsc::Receiver<SourceEvent>,
    trade_rx: mpsc::Receiver<IncomingTrade>,
    accumulator: FundingGraphAccumulator,
    position_ledger: PositionLedger,
    cluster_tracker: ClusterObservationTracker,
    watchlist: Watchlist,
    signal_config: SignalConfig,
    strategy: WinnerFollowStrategy,
    paper_executor: PaperExecutor,
    mode: ExecutionMode,
    bankroll: Decimal,
    health: SharedHealth,
    // Sentinel quality (0) returned for any wallet not found in the watchlist.
    // Zero quality → LeaderAction::Unknown → classify_trade returns None, so no signal.
    min_quality: ReconstructionQuality,
}

impl Orchestrator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        polygon_rx: mpsc::Receiver<SourceEvent>,
        trade_rx: mpsc::Receiver<IncomingTrade>,
        accumulator: FundingGraphAccumulator,
        watchlist: Watchlist,
        config: OrchestratorConfig,
        strategy: WinnerFollowStrategy,
        paper_executor: PaperExecutor,
        health: SharedHealth,
    ) -> Result<Self, anyhow::Error> {
        let min_quality = ReconstructionQuality::new(0)
            .map_err(|_| anyhow::anyhow!("internal: ReconstructionQuality::new(0) failed"))?;
        Ok(Self {
            polygon_rx,
            trade_rx,
            accumulator,
            position_ledger: PositionLedger::new(),
            cluster_tracker: ClusterObservationTracker::new(config.cluster_observation_window_secs),
            watchlist,
            signal_config: config.signal_config,
            strategy,
            paper_executor,
            mode: config.mode,
            bankroll: config.bankroll,
            health,
            min_quality,
        })
    }

    /// Run the dispatch loop until both source channels are closed OR until the
    /// provided `shutdown` future resolves.
    ///
    /// On shutdown, remaining events already buffered in both channels are drained
    /// and processed before returning — no in-flight fills are lost.
    pub async fn run(mut self, shutdown: impl std::future::Future<Output = ()>) {
        tokio::pin!(shutdown);
        let mut polygon_done = false;
        let mut trades_done = false;

        loop {
            // Both channels closed naturally — exit without waiting for shutdown.
            if polygon_done && trades_done {
                break;
            }

            tokio::select! {
                biased;
                _ = &mut shutdown => {
                    // Drain remaining buffered events synchronously before exiting.
                    while let Ok(event) = self.polygon_rx.try_recv() {
                        self.handle_polygon(event);
                    }
                    while let Ok(trade) = self.trade_rx.try_recv() {
                        self.handle_trade(trade);
                    }
                    break;
                }
                result = self.polygon_rx.recv(), if !polygon_done => {
                    match result {
                        Some(event) => self.handle_polygon(event),
                        None => polygon_done = true,
                    }
                }
                result = self.trade_rx.recv(), if !trades_done => {
                    match result {
                        Some(trade) => self.handle_trade(trade),
                        None => trades_done = true,
                    }
                }
            }
        }
    }

    // ── Private handlers ──────────────────────────────────────────────────────

    fn handle_polygon(&mut self, event: SourceEvent) {
        match serde_json::from_slice::<PolygonEvent>(&event.payload) {
            Err(e) => warn!(error = %e, "polygon payload decode failed"),
            Ok(pe) => {
                self.accumulator.ingest(pe);
                let mut h = self
                    .health
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                h.polygon_last_event_at = Some(OffsetDateTime::now_utc());
            }
        }
    }

    fn handle_trade(&mut self, trade: IncomingTrade) {
        // Mark polymarket freshness.
        {
            let mut h = self
                .health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            h.polymarket_last_event_at = Some(OffsetDateTime::now_utc());
        }

        // Look up wallet in watchlist; skip non-watchlisted wallets.
        let quality = self.quality_for(&trade.wallet);
        let profile = WalletProfile {
            wallet: trade.wallet,
            closed_trade_count: 0, // honest sentinel per Phase 0B scope
            age_seconds: 0,        // honest sentinel; treated as "fresh" conservative stub
        };

        // Operator identity is deferred to Phase 1 (operator-graph wiring).
        let operator_id = self
            .watchlist
            .entries
            .iter()
            .find(|e| e.wallet == trade.wallet)
            .and_then(|e| e.operator_id);

        // Capture pre-trade snapshot: classify_action uses pre-trade position to determine
        // Entry/Add/Flip/Trim/Exit. Ingest must follow so the ledger advances after
        // classification, not before.
        let position = self.position_ledger.position(&trade.wallet).cloned();

        // Record cluster entry when operator is known; prune stale entries.
        // Ingest into tracker first so the current trade is included in cluster_obs_for.
        if let Some(op) = operator_id {
            self.cluster_tracker.ingest(&trade, op);
        }

        let cluster_obs =
            operator_id.and_then(|op| self.cluster_tracker.cluster_obs_for(&trade, op));

        // Advance position ledger after classification inputs are captured.
        self.position_ledger.ingest(&trade);

        let Some(signal) = classify_trade(
            &trade,
            position.as_ref(),
            &self.watchlist,
            &profile,
            cluster_obs.as_ref(),
            operator_id,
            quality,
            VenueId::polymarket(),
            &self.signal_config,
        ) else {
            return;
        };

        let snapshot = zeroed_risk_snapshot();
        match self
            .strategy
            .evaluate(&signal, snapshot, self.bankroll, self.mode)
        {
            Err(e) => info!(reason = %e, "signal did not produce order"),
            Ok(intent) => {
                let now = SourceTimestamp(OffsetDateTime::now_utc());
                match self.paper_executor.execute(&intent, now) {
                    Err(e) => error!(error = %e, "paper executor failed"),
                    Ok(fill) => {
                        info!(
                            kind = "paper_fill",
                            idempotency_key = %fill.intent.idempotency_key,
                            market = %fill.intent.market_id,
                            side = ?fill.intent.side,
                            contracts = fill.intent.contracts.0,
                            fill_price = %fill.simulated_fill_price.0,
                        );
                    }
                }
            }
        }
    }

    fn quality_for(&self, wallet: &WalletAddress) -> ReconstructionQuality {
        self.watchlist
            .entries
            .iter()
            .find(|e| &e.wallet == wallet)
            .map(|e| e.reconstruction_quality)
            .unwrap_or(self.min_quality)
    }
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
        operator_exposure_bps: BasisPoints(0),
        market_exposure_bps: BasisPoints(0),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(0),
        funder_inherited_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(0),
        rolling_7d_pnl_bps: BasisPoints(0),
        anti_gaming_flags: HashSet::<AntiGamingFlag>::new(),
        onchain_source_status: SourceStatus::Healthy,
        proxy_funder_mapping_proven: true, // stub: assume proven so inherited-prior isn't blocked
        funder_seeding_rate_suspicious: false,
        cluster_membership_stable: true,
        funding_hop_count: None,
        copy_latency_p95_ms: 0,
        trading_mode: TradingMode::LiveTiny, // overridden by evaluate()
        proposed_trade_bps: BasisPoints(0),  // overridden by evaluate()
    }
}
