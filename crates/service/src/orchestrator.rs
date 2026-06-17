//! Event dispatch loop: routes decoded trade events to the copy-signal-engine,
//! then gates signals through strategy evaluation and execution dispatch.
//!
//! The orchestrator is pure dispatch: it owns no I/O except through `ExecutionDispatcher`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, PositionSnapshot, SignalConfig, classify_trade};
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
use pe_strategy_winner_follow::{ExecutionMode, PaperFill, WinnerFollowStrategy};
use pe_trader_index::Watchlist;
use pe_venue_polymarket::CLOBClient;
use rust_decimal::Decimal;
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::entry_gate::{CopyEntryGate, CopyEntryGateConfig};
use crate::health::SharedHealth;
use crate::live_watchlist::LiveWatchlist;
use crate::market_end_cache::MarketEndCache;
use crate::mid_price_cache::MidPriceCache;
use crate::snapshot_worker::{SnapshotHandle, enqueue_if_buy};
use crate::supabase_sink::SinkHandle;

/// Orchestrator configuration.
pub struct OrchestratorConfig {
    pub bankroll: Decimal,
    pub mode: ExecutionMode,
    pub signal_config: SignalConfig,
    /// Drop signals whose market `endDate` is further than this many seconds into
    /// the future. 0 disables the upper bound. Default: 72 h (259_200 s).
    pub max_resolution_horizon_secs: u64,
    /// Drop signals whose market resolves sooner than this many seconds from now.
    /// 0 disables the lower bound. Default: 60 s (docs/29 copy floor).
    pub min_resolution_horizon_secs: u64,
    /// Maximum current price at which a BUY copy will fill (issue #142 parity).
    /// `Decimal::ZERO` disables the cap.
    pub max_fill_price: Decimal,
    /// Copy-entry gate config (first-entry/fail-closed posture). The per-wallet
    /// market history is supplied separately to [`Orchestrator::new`].
    pub entry_gate_config: CopyEntryGateConfig,
}

pub struct Orchestrator<C: CLOBClient, F: PageFetcher + Send + Sync> {
    trade_rx: mpsc::Receiver<IncomingTrade>,
    position_ledger: PositionLedger,
    live_watchlist: LiveWatchlist,
    signal_config: SignalConfig,
    strategy: WinnerFollowStrategy,
    dispatcher: ExecutionDispatcher<C>,
    mode: ExecutionMode,
    bankroll: Decimal,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,
    market_end_cache: MarketEndCache,
    // Live current-price source (Gamma mids) for the post-latency sizing basis (#339).
    mid_price_cache: MidPriceCache<F>,
    max_resolution_horizon_secs: u64,
    min_resolution_horizon_secs: u64,
    // Skip BUYs whose current price is >= this (issue #142 parity). ZERO disables.
    max_fill_price: Decimal,
    // Tracks (market, outcome) pairs we already hold a paper position in.
    // Prevents multiple leaders entering the same contract from stacking fills.
    filled_positions: HashSet<MarketOutcomeId>,
    // Copy-entry gate: admits only a leader's first-ever entry into a market
    // (#290; price band removed in #339).
    entry_gate: CopyEntryGate,
    // Sentinel quality (0) returned for any wallet not found in the watchlist.
    // Zero quality → LeaderAction::Unknown → classify_trade returns None, so no signal.
    min_quality: ReconstructionQuality,
    reseed_rx: mpsc::Receiver<HashMap<WalletAddress, PositionSnapshot>>,
    // Best-effort Supabase analytics sink (issue #343). `None` when disabled.
    sink: Option<SinkHandle>,
    // Liquidity-at-fill snapshot enqueue handle (issue #350 WS2 PR-H). `None` when capture is
    // disabled. Buy-only; enqueue is non-blocking (drop-on-full), off the trade hot path.
    snapshot_sink: Option<SnapshotHandle>,
}

impl<C: CLOBClient, F: PageFetcher + Send + Sync> Orchestrator<C, F> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trade_rx: mpsc::Receiver<IncomingTrade>,
        live_watchlist: LiveWatchlist,
        config: OrchestratorConfig,
        history_map: HashMap<WalletAddress, HashSet<MarketId>>,
        strategy: WinnerFollowStrategy,
        dispatcher: ExecutionDispatcher<C>,
        paper_state: Arc<PaperStateDb>,
        leader_ledger: PositionLedger,
        health: SharedHealth,
        market_end_cache: MarketEndCache,
        mid_price_cache: MidPriceCache<F>,
        reseed_rx: mpsc::Receiver<HashMap<WalletAddress, PositionSnapshot>>,
        sink: Option<SinkHandle>,
        snapshot_sink: Option<SnapshotHandle>,
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
            filled_positions,
            entry_gate: CopyEntryGate::new(config.entry_gate_config, history_map),
            min_quality,
            reseed_rx,
            sink,
            snapshot_sink,
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
        let mut reseed_done = false;

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
                result = self.reseed_rx.recv(), if !reseed_done => {
                    match result {
                        Some(map) => {
                            let n = map.len();
                            self.position_ledger.overlay(map);
                            info!(wallets = n, "leader ledger reseeded from live positions API");
                        }
                        None => reseed_done = true,
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

    async fn handle_trade(&mut self, trade: IncomingTrade) {
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

        // Copy-entry gate: copy only a leader's first-ever entry into a market
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
        // Record the admitted entry so a same-session re-entry into this market is
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

        // Post-latency price basis (#339): size the copy against the CURRENT market
        // price (the copy fills now, not when the leader entered). Fail closed when the
        // current price is unavailable — we cannot size or cost-adjust without it.
        let current_price = {
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

        // max_fill_price safety rail (#142 parity): skip BUYs whose current price is at
        // or above the cap (catastrophic payoff geometry near $1). ZERO disables.
        if signal.leader_side == Side::Buy
            && self.max_fill_price > Decimal::ZERO
            && current_price.0 >= self.max_fill_price
        {
            info!(
                reason = "current price at or above max_fill_price",
                market = %signal.market_id,
                current_price = %current_price.0,
                max_fill_price = %self.max_fill_price,
                "signal did not produce order",
            );
            self.commit_no_fill(&trade, &leader_row);
            return;
        }

        let p = self.win_rate_p_for(&watchlist, &signal.leader);
        let snapshot = zeroed_risk_snapshot();
        match self.strategy.evaluate_at_price(
            &signal,
            current_price,
            p,
            snapshot,
            self.bankroll,
            self.mode,
        ) {
            Err(e) => {
                info!(reason = %e, "signal did not produce order");
                self.commit_no_fill(&trade, &leader_row);
            }
            Ok(intent) => {
                let now = SourceTimestamp(OffsetDateTime::now_utc());
                match self.dispatcher.execute(&intent, self.mode, now).await {
                    Err(e) => {
                        error!(error = %e, "execution dispatcher failed");
                        self.commit_no_fill(&trade, &leader_row);
                    }
                    Ok(DispatchResult::Paper { fill, seq }) => {
                        let filled_key = MarketOutcomeId::new(
                            fill.intent.market_id.clone(),
                            fill.intent.outcome_id,
                        );
                        self.commit_paper_fill(&trade, &leader_row, &fill, seq);
                        self.filled_positions.insert(filled_key);
                        info!(
                            kind = "paper_fill",
                            idempotency_key = %fill.intent.idempotency_key,
                            market = %fill.intent.market_id,
                            side = ?fill.intent.side,
                            contracts = fill.intent.contracts.0,
                            fill_price = %fill.simulated_fill_price.0,
                        );
                    }
                    Ok(DispatchResult::Live(result)) => {
                        // Live execution is out of paper-state's fill scope; still mark
                        // seen + mirror the leader ledger so dedup holds across modes.
                        self.commit_no_fill(&trade, &leader_row);
                        info!(
                            kind = "live_execution",
                            idempotency_key = %intent.idempotency_key,
                            market = %intent.market_id,
                            outcome = ?result.outcome(),
                        );
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

    /// Commit dedup + leader-ledger mirror + fill accounting in one transaction, and
    /// update the in-memory bankroll to the new persisted value (drawdown-aware sizing).
    fn commit_paper_fill(
        &mut self,
        trade: &IncomingTrade,
        leader: &LeaderPositionRow,
        fill: &PaperFill,
        seq: EventSeq,
    ) {
        let record = FillRecord {
            idempotency_key: fill.intent.idempotency_key.clone(),
            market_id: fill.intent.market_id.clone(),
            outcome_id: fill.intent.outcome_id,
            side: fill.intent.side,
            contracts: fill.intent.contracts.0,
            fill_price: fill.simulated_fill_price,
        };
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
