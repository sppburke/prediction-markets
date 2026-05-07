//! Phase 1: walk-forward simulation.
//!
//! Walk-forward invariant (enforced): at simulated date D, only trades with
//! `timestamp.date() < D` are visible to the ranker. Trades with `timestamp.date() == D`
//! are the new signals; the ranker was built from strictly prior data.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write as _;
use std::path::Path;

use pe_bootstrap::cache::{LeaderboardSnapshots, ResolutionIndex};
use pe_copy_signal_engine::LeaderSignal;
use pe_core_types::{
    BasisPoints, LeaderAction, MarketId, OperatorId, OutcomeId, Probability, ProbabilityPpm,
    Quantity, ReconstructionQuality, Side, SourceTimestamp, TraderId, VenueId, WalletAddress,
    WinnerFollowSignalKind,
};
use pe_operator_graph::OperatorIdentity;
use pe_risk_engine::RiskSnapshot;
use pe_risk_engine::snapshot::TradingMode;
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::WinnerFollowStrategy;
use pe_trader_index::ledger::TraderLedger;
use pe_trader_index::snapshot::{RawTrade, TradeSnapshot};
use pe_trader_index::{LedgerConfig, RankerConfig, build_trader_ledgers, build_watchlist};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use time::{Date, OffsetDateTime};
use tracing::info;

use crate::config::BacktestConfig;
use crate::error::BacktestError;
use crate::funder_graph::{FunderGraphTimeline, build_operator_identities_at};
use crate::report::{
    PnlAccumulator, TradeFill, WinnerFollowReport, max_drawdown_pct, sharpe_ratio,
};

// Warn threshold for expiry-filter suppression per quarter.
// Canonical default: `expiry_filter_suppression_warn_threshold = 30%` in `docs/_GLOSSARY.md`.
const SUPPRESSION_WARN_THRESHOLD: u32 = 30;

/// Per-quarter statistics for the `max_hours_to_expiry` suppression diagnostic.
#[derive(Debug, Default)]
struct QuarterStats {
    total: u64,
    suppressed: u64,
}

/// Tracks how many buy signals are suppressed by the `max_hours_to_expiry` filter,
/// broken down by calendar quarter. Emits a warning when any quarter exceeds the
/// canonical 30% threshold.
#[derive(Debug, Default)]
struct SuppressionTracker {
    by_quarter: BTreeMap<(i32, u8), QuarterStats>,
}

impl SuppressionTracker {
    fn record(&mut self, date: Date, suppressed: bool) {
        let quarter = (date.month() as u8 - 1) / 3 + 1;
        let stats = self.by_quarter.entry((date.year(), quarter)).or_default();
        stats.total += 1;
        if suppressed {
            stats.suppressed += 1;
        }
    }

    fn suppression_pct_global(&self) -> Decimal {
        let total: u64 = self.by_quarter.values().map(|s| s.total).sum();
        let suppressed: u64 = self.by_quarter.values().map(|s| s.suppressed).sum();
        if total == 0 {
            return Decimal::ZERO;
        }
        Decimal::from(suppressed * 100) / Decimal::from(total)
    }

    fn per_quarter_suppression(&self) -> std::collections::BTreeMap<String, Decimal> {
        self.by_quarter
            .iter()
            .map(|((year, q), stats)| {
                let key = format!("{year}-Q{q}");
                let pct = if stats.total == 0 {
                    Decimal::ZERO
                } else {
                    Decimal::from(stats.suppressed * 100) / Decimal::from(stats.total)
                };
                (key, pct)
            })
            .collect()
    }

    fn warn_high_quarters(&self, threshold_pct: u32) {
        let threshold = Decimal::from(threshold_pct);
        for ((year, q), stats) in &self.by_quarter {
            if stats.total == 0 {
                continue;
            }
            let pct = Decimal::from(stats.suppressed * 100) / Decimal::from(stats.total);
            if pct > threshold {
                tracing::warn!(
                    quarter = %format!("{year}-Q{q}"),
                    suppression_pct = %pct,
                    "expiry filter suppression exceeds {threshold_pct}% — \
                     consider disabling max_hours_to_expiry for this data range"
                );
            }
        }
    }
}

// Canonical default in `docs/_GLOSSARY.md` "Backtest defaults".
const DEFAULT_SLIPPAGE_BPS: u32 = 100;

/// Open position entry (copies we've taken but not yet closed).
#[derive(Debug, Clone)]
struct OpenPosition {
    contracts: u64,
    avg_fill_price: Decimal,
    operator_id: Option<OperatorId>,
    /// Calendar date on which the copy was opened. Used by the resolution sweep to
    /// guard against anomalies where a market's resolved_at precedes the bought_on date.
    bought_on: Date,
}

/// Position key: (market, outcome, BUY side).
type PosKey = (MarketId, OutcomeId);

/// Exposure tracker: per-leader, per-market, per-operator.
#[derive(Debug, Default)]
struct ExposureTracker {
    /// Open exposure in bps of bankroll per leader.
    by_leader: HashMap<WalletAddress, i32>,
    /// Open exposure in bps of bankroll per operator.
    by_operator: HashMap<OperatorId, i32>,
    /// Open exposure in bps of bankroll per market.
    by_market: HashMap<MarketId, i32>,
    /// Total copy exposure across all positions.
    total: i32,
}

impl ExposureTracker {
    fn leader_bps(&self, w: WalletAddress) -> i32 {
        self.by_leader.get(&w).copied().unwrap_or(0)
    }

    /// Returns 0 when `op` is `None` — unclustered wallets bypass operator concentration.
    ///
    /// The operator concentration cap prevents overexposure to a single multi-wallet operator.
    /// A wallet with no known operator is standalone and is instead governed by the per-leader cap.
    fn operator_bps(&self, op: Option<&OperatorId>) -> i32 {
        let Some(op) = op else { return 0 };
        self.by_operator.get(op).copied().unwrap_or(0)
    }

    fn market_bps(&self, m: &MarketId) -> i32 {
        self.by_market.get(m).copied().unwrap_or(0)
    }

    fn add(&mut self, w: WalletAddress, op: Option<&OperatorId>, m: &MarketId, bps: i32) {
        *self.by_leader.entry(w).or_default() += bps;
        if let Some(op) = op {
            *self.by_operator.entry(*op).or_default() += bps;
        }
        *self.by_market.entry(m.clone()).or_default() += bps;
        self.total += bps;
    }

    fn remove(&mut self, w: WalletAddress, op: Option<&OperatorId>, m: &MarketId, bps: i32) {
        self.add(w, op, m, -bps);
    }
}

/// Run the walk-forward simulation and produce a `WinnerFollowReport`.
///
/// `snapshots` constrains the candidate-wallet pool at each weekly boundary to
/// match what the live system would have seen at that point in time. When
/// [`LeaderboardSnapshots::is_empty`] is true the simulation falls back to
/// "all wallets present in the trade history" with a single warning log —
/// preserves backwards compatibility with caches predating the snapshots feature.
///
/// `resolutions` drives the per-day resolution sweep that closes open positions
/// whose underlying market settled on-chain. Pass `&ResolutionIndex::new()` when
/// no resolution data is available (sweep is a no-op; positions remain open at horizon).
///
/// Writes `report.json` and `trades.ndjson` to `config.output_dir` when `write_output` is true.
///
/// Pass `write_output = false` from sweep mode — per-run files are suppressed and only
/// the sweep-level JSON is written by the caller.
#[allow(clippy::too_many_arguments)]
pub fn run_simulation(
    config: &BacktestConfig,
    mut all_trades: Vec<RawTrade>,
    funder_timeline: &FunderGraphTimeline,
    snapshots: &LeaderboardSnapshots,
    resolutions: &ResolutionIndex,
    ranker_config: &RankerConfig,
    ledger_config: &LedgerConfig,
    strategy: &WinnerFollowStrategy,
    write_output: bool,
) -> Result<WinnerFollowReport, BacktestError> {
    // Sort all trades ascending by timestamp for walk-forward processing.
    all_trades.sort_by_key(|t| t.timestamp.0);

    if all_trades.is_empty() {
        return Err(BacktestError::Internal("no trades in cache".to_owned()));
    }

    // Backwards-compatibility fallback. Caches predating the snapshots feature
    // (or fresh caches that have never been bootstrapped) have no leaderboard
    // rows; in that mode the simulation runs with the full wallet history,
    // which is survivorship-biased. Surfaced as a single warning so it isn't
    // missed in long runs but doesn't spam per-day.
    if snapshots.is_empty() {
        tracing::warn!(
            "no leaderboard snapshots available — running with full wallet history; \
             results subject to survivorship bias. Run pe-bootstrap to populate \
             leaderboard_snapshots."
        );
    }

    // Collect unique simulation dates.
    let all_dates: Vec<Date> = {
        let mut dates: Vec<Date> = all_trades
            .iter()
            .map(|t| t.timestamp.0.date())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        dates.sort();
        dates
    };

    let simulation_start: OffsetDateTime = all_trades[0].timestamp.0;
    let simulation_end: OffsetDateTime = all_trades
        .last()
        .map(|t| t.timestamp.0)
        .unwrap_or(simulation_start);

    let slippage_bps = DEFAULT_SLIPPAGE_BPS;
    let slippage = Decimal::from(slippage_bps) / Decimal::from(10_000u32);

    let mut bankroll = config.bankroll_usd;
    let bankroll_initial = bankroll;

    // Open positions per (wallet, market, outcome) → position entry.
    let mut open_positions: HashMap<(WalletAddress, PosKey), OpenPosition> = HashMap::new();

    let mut exposure = ExposureTracker::default();
    let mut pnl_accum = PnlAccumulator::default();
    let mut daily_bankroll: Vec<Decimal> = Vec::new();
    let mut daily_pnl: Vec<Decimal> = Vec::new();

    // Intraday and rolling 7-day PnL tracking (in bps of bankroll).
    let mut intraday_realized_pnl = Decimal::ZERO;
    let mut last_intraday_reset: Option<Date> = None;

    let mut fills_writer = maybe_open_trades_ndjson(&config.output_dir, write_output)?;

    // Emit funder graph temporal filter diagnostic once before the main loop.
    if !funder_timeline.is_empty() {
        let first_day_unix = all_dates
            .first()
            .map_or(0, |d| d.midnight().assume_utc().unix_timestamp());
        info!(
            edges_visible_at_sim_start = funder_timeline.view_at(first_day_unix).len(),
            total_edges = funder_timeline.total_edge_count(),
            "funder graph temporal filter active"
        );
    }

    // Tracks leaderboard snapshot transitions for the log gate (replaces the
    // misleading `day_idx % 30` gate that fired every 30 array indices, not days).
    let mut last_logged_snapshot_unix: Option<i64> = None;

    // Tracks buy-signal suppression from the max_hours_to_expiry filter.
    let mut suppression_tracker = SuppressionTracker::default();

    // Group trades by date for efficient walk-forward lookup.
    let mut trades_by_date: BTreeMap<Date, Vec<&RawTrade>> = BTreeMap::new();
    for trade in &all_trades {
        trades_by_date
            .entry(trade.timestamp.0.date())
            .or_default()
            .push(trade);
    }

    let step = config.step_days.max(1) as usize;

    for (day_idx, &sim_date) in all_dates.iter().enumerate() {
        // Reset intraday PnL at day boundary.
        if last_intraday_reset != Some(sim_date) {
            let day_pnl = intraday_realized_pnl;
            daily_pnl.push(day_pnl);
            daily_bankroll.push(bankroll);
            intraday_realized_pnl = Decimal::ZERO;
            last_intraday_reset = Some(sim_date);
        }

        // Resolution sweep: close positions whose underlying market resolved on or before sim_date.
        let sim_date_unix = sim_date.midnight().assume_utc().unix_timestamp();
        let to_close: Vec<(WalletAddress, PosKey)> = open_positions
            .iter()
            .filter_map(|((leader, pos_key), open)| {
                let res = resolutions.get(&pos_key.0)?;
                let bought_unix = open.bought_on.midnight().assume_utc().unix_timestamp();
                if res.resolved_at_unix > sim_date_unix {
                    return None;
                }
                if res.resolved_at_unix < bought_unix {
                    return None;
                }
                Some((*leader, pos_key.clone()))
            })
            .collect();

        for (leader, pos_key) in to_close {
            let Some(open) = open_positions.remove(&(leader, pos_key.clone())) else {
                continue;
            };
            let Some(res) = resolutions.get(&pos_key.0) else {
                continue;
            };
            let close_price = if res.winning_outcome_id == pos_key.1.0 {
                Decimal::ONE
            } else {
                Decimal::ZERO
            };

            let revenue = Decimal::from(open.contracts) * close_price;
            let cost = Decimal::from(open.contracts) * open.avg_fill_price;
            let pnl = revenue - cost;
            bankroll += revenue;
            intraday_realized_pnl += pnl;

            let bps_removed = proposed_trade_bps(open.contracts, open.avg_fill_price, bankroll);
            exposure.remove(leader, open.operator_id.as_ref(), &pos_key.0, bps_removed);

            pnl_accum.record(open.operator_id.as_ref(), pnl);

            let fill = TradeFill {
                simulated_at: sim_date.midnight().assume_utc(),
                leader_wallet: leader.to_string(),
                operator_id: open.operator_id.as_ref().map(|o| o.to_string()),
                market_id: pos_key.0.0.0.clone(),
                outcome_id: pos_key.1.0,
                side: "resolution".to_owned(),
                contracts: open.contracts,
                signal_price: close_price,
                fill_price: close_price,
            };
            write_fill(fills_writer.as_mut(), &fill)?;
        }

        // Skip days that fall outside the step window.
        if day_idx % step != 0 {
            continue;
        }

        // Ranker state: all trades strictly BEFORE today.
        let ranker_cutoff_unix = sim_date.midnight().assume_utc().unix_timestamp();
        let ranker_trades: Vec<RawTrade> = all_trades
            .iter()
            .filter(|t| t.timestamp.0.unix_timestamp() < ranker_cutoff_unix)
            .cloned()
            .collect();

        if ranker_trades.is_empty() {
            continue;
        }

        // Build operator identities using only edges visible at this sim date.
        let operator_identities =
            build_operator_identities_at(funder_timeline, &all_trades, sim_date_unix)?;
        let wallet_to_operator: HashMap<WalletAddress, &OperatorIdentity> = operator_identities
            .iter()
            .flat_map(|op| op.member_wallets.iter().map(move |w| (*w, op)))
            .collect();

        let snapshot = TradeSnapshot {
            trades: ranker_trades,
            snapshot_at: SourceTimestamp(sim_date.midnight().assume_utc()),
            audit_window_days: config.audit_window_days,
        };
        let ledgers = build_trader_ledgers(&snapshot, &operator_identities, ledger_config);

        // Constrain the candidate pool to wallets present in the most-recent
        // leaderboard snapshot ≤ sim_date. Wallet-level filter applied BEFORE
        // operator grouping inside build_watchlist — strict "we didn't know
        // about this wallet at week T" semantics. When snapshots are absent
        // the filter degrades to a no-op (warned at start).
        let filtered_ledgers: Vec<TraderLedger> = if snapshots.is_empty() {
            ledgers
        } else {
            match snapshots.for_date(ranker_cutoff_unix) {
                Some(pool) => ledgers
                    .into_iter()
                    .filter(|l| pool.contains(&l.wallet))
                    .collect(),
                // Simulation date precedes the first seeded snapshot — no candidates yet.
                None => Vec::new(),
            }
        };

        if filtered_ledgers.is_empty() {
            continue;
        }

        let watchlist = build_watchlist(
            &filtered_ledgers,
            snapshot.snapshot_at.clone(),
            ranker_config,
        );

        // Log on leaderboard snapshot transitions (replaces misleading day_idx % 30 gate).
        let snapshot_unix = snapshots.snapshot_at_for_date(ranker_cutoff_unix);
        if snapshot_unix != last_logged_snapshot_unix {
            info!(
                date = %sim_date,
                snapshot_unix,
                filtered_ledgers = filtered_ledgers.len(),
                active = watchlist.active_count,
                incubator = watchlist.incubator_count,
                "watchlist snapshot transition"
            );
            last_logged_snapshot_unix = snapshot_unix;
        }

        // Build wallet → ledger map for O(1) win-rate lookup.
        let ledger_by_wallet: HashMap<WalletAddress, &TraderLedger> =
            filtered_ledgers.iter().map(|l| (l.wallet, l)).collect();

        // Build wallet → reconstruction quality map from watchlist entries.
        let quality_by_wallet: HashMap<WalletAddress, ReconstructionQuality> = watchlist
            .entries
            .iter()
            .map(|e| (e.wallet, e.reconstruction_quality))
            .collect();

        // Watchlisted wallets (Active + Incubator).
        let watchlisted: HashSet<WalletAddress> =
            watchlist.entries.iter().map(|e| e.wallet).collect();

        // New signals: trades happening exactly on sim_date from watchlisted leaders.
        let Some(todays_trades) = trades_by_date.get(&sim_date) else {
            continue;
        };

        // Rolling 7-day PnL: sum of realized PnL for the last 7 days in bps.
        let rolling_7d_realized: Decimal = {
            let cutoff = day_idx.saturating_sub(7);
            daily_pnl.iter().skip(cutoff).sum()
        };
        // Intraday and rolling 7-day PnL in bps.
        let intraday_bps = decimal_to_bps(intraday_realized_pnl, bankroll);
        let rolling_7d_bps = decimal_to_bps(rolling_7d_realized, bankroll);

        for trade in todays_trades {
            if !watchlisted.contains(&trade.wallet) {
                continue;
            }

            let leader = trade.wallet;
            let op_identity = wallet_to_operator.get(&leader).copied();
            let operator_id = op_identity.map(|op| &op.operator_id);
            // When leaderboard snapshots are in use the snapshot membership serves
            // as the funder/quality proxy — treat every snapshot wallet as having a
            // proven funder mapping so the risk engine's funder-check doesn't block
            // all signals in the absence of Etherscan data.
            let has_funder = op_identity.is_some() || !snapshots.is_empty();

            let pos_key = (trade.market_id.clone(), trade.outcome_id);
            let wallet_pos_key = (leader, pos_key.clone());

            match trade.side {
                Side::Buy => {
                    // Open a new copy position if we don't already have one for this key.
                    if open_positions.contains_key(&wallet_pos_key) {
                        continue; // Already tracking this leader's position.
                    }

                    // Time-to-expiry filter: skip trades where the market resolves
                    // more than max_hours_to_expiry hours after the trade date.
                    // Markets with unknown resolution (None) are allowed — at sim time
                    // we could not have known the market was unresolved, so skipping
                    // them would be survivorship-biased.
                    if let Some(max_hours) = config.max_hours_to_expiry {
                        let max_secs = i64::from(max_hours) * 3600;
                        let suppressed = matches!(
                            resolutions.get(&trade.market_id),
                            Some(res) if res.resolved_at_unix - sim_date_unix > max_secs
                        );
                        suppression_tracker.record(sim_date, suppressed);
                        if suppressed {
                            continue;
                        }
                    }

                    let fill_price = {
                        let raw = trade.price.0 + slippage;
                        // Clamp to (0, 1).
                        if raw >= Decimal::ONE {
                            continue;
                        }
                        raw
                    };
                    // Win-rate probability from the leader's ledger (Blocker 2).
                    let Some(p) = leader_win_rate_p(ledger_by_wallet.get(&leader).copied()) else {
                        continue;
                    };

                    // Quality from watchlist; always present for watchlisted wallets.
                    let Some(quality) = quality_by_wallet.get(&leader).copied() else {
                        continue;
                    };

                    let signal = raw_trade_to_leader_signal(trade, operator_id.cloned(), quality);

                    let risk_snapshot = build_risk_snapshot(&RiskContext {
                        exposure: &exposure,
                        leader,
                        operator_id,
                        market_id: &trade.market_id,
                        intraday_bps,
                        rolling_7d_bps,
                        has_funder,
                        proposed_bps: 0, // evaluate() overwrites this
                    });

                    let contracts_count = match strategy.evaluate(
                        &signal,
                        p,
                        risk_snapshot,
                        bankroll,
                        pe_strategy_winner_follow::ExecutionMode::Paper,
                    ) {
                        Ok(intent) => intent.contracts.0,
                        Err(e) => {
                            tracing::debug!(wallet = %leader, reason = %e, "signal blocked");
                            continue;
                        }
                    };

                    if contracts_count == 0 {
                        continue;
                    }

                    // Record fill.
                    let notional = Decimal::from(contracts_count) * fill_price;
                    bankroll -= notional;

                    let actual_bps = proposed_trade_bps(contracts_count, fill_price, bankroll);
                    exposure.add(leader, operator_id, &trade.market_id, actual_bps);

                    open_positions.insert(
                        wallet_pos_key,
                        OpenPosition {
                            contracts: contracts_count,
                            avg_fill_price: fill_price,
                            operator_id: operator_id.cloned(),
                            bought_on: sim_date,
                        },
                    );

                    let fill = TradeFill {
                        simulated_at: sim_date.midnight().assume_utc(),
                        leader_wallet: leader.to_string(),
                        operator_id: operator_id.map(|o| o.to_string()),
                        market_id: trade.market_id.0.0.clone(),
                        outcome_id: trade.outcome_id.0,
                        side: "buy".to_owned(),
                        contracts: contracts_count,
                        signal_price: trade.price.0,
                        fill_price,
                    };
                    write_fill(fills_writer.as_mut(), &fill)?;
                }

                Side::Sell => {
                    // Close existing copy position for this leader on this (market, outcome).
                    let Some(open) = open_positions.remove(&wallet_pos_key) else {
                        continue;
                    };

                    let fill_price = {
                        let raw = trade.price.0 - slippage;
                        if raw <= Decimal::ZERO {
                            Decimal::new(1, 4)
                        } else {
                            raw
                        }
                    };

                    let closed_contracts = open.contracts.min(trade.contracts.0);
                    let revenue = Decimal::from(closed_contracts) * fill_price;
                    let cost = Decimal::from(closed_contracts) * open.avg_fill_price;
                    let pnl = revenue - cost;

                    bankroll += revenue;
                    intraday_realized_pnl += pnl;

                    let bps_removed =
                        proposed_trade_bps(closed_contracts, open.avg_fill_price, bankroll);
                    exposure.remove(
                        leader,
                        open.operator_id.as_ref(),
                        &trade.market_id,
                        bps_removed,
                    );

                    pnl_accum.record(open.operator_id.as_ref(), pnl);

                    let fill = TradeFill {
                        simulated_at: sim_date.midnight().assume_utc(),
                        leader_wallet: leader.to_string(),
                        operator_id: open.operator_id.as_ref().map(|o| o.to_string()),
                        market_id: trade.market_id.0.0.clone(),
                        outcome_id: trade.outcome_id.0,
                        side: "sell".to_owned(),
                        contracts: closed_contracts,
                        signal_price: trade.price.0,
                        fill_price,
                    };
                    write_fill(fills_writer.as_mut(), &fill)?;
                }
            }
        }
    }

    // Positions still open at horizon are excluded from realized PnL — their final
    // value is unknown. Capital remains tied up in the bankroll (cost was deducted at BUY).
    let open_at_horizon = u64::try_from(open_positions.len()).unwrap_or(u64::MAX);
    // Do NOT modify bankroll or record PnL for open positions.

    daily_bankroll.push(bankroll);
    daily_pnl.push(intraday_realized_pnl);

    let total_pnl_usd = daily_pnl.iter().sum::<Decimal>();
    let sharpe = sharpe_ratio(&daily_pnl);
    let max_dd = max_drawdown_pct(&daily_bankroll);

    suppression_tracker.warn_high_quarters(SUPPRESSION_WARN_THRESHOLD);

    let report = WinnerFollowReport {
        total_pnl_usd,
        sharpe_ratio: sharpe,
        max_drawdown_pct: max_dd,
        total_copies: pnl_accum.total_copies,
        win_rate_pct: pnl_accum.win_rate_pct(),
        per_operator_pnl: pnl_accum.per_operator,
        simulation_start,
        simulation_end,
        bankroll_initial,
        bankroll_final: bankroll,
        slippage_assumption_bps: slippage_bps,
        open_at_horizon,
        funder_graph_snapshot_caveat: false,
        expiry_filter_suppression_pct: suppression_tracker.suppression_pct_global(),
        expiry_suppression_by_quarter: suppression_tracker.per_quarter_suppression(),
    };

    if write_output {
        let report_path = config.output_dir.join("report.json");
        let json = serde_json::to_vec_pretty(&report)?;
        let tmp = report_path.with_extension("json.tmp");
        std::fs::write(&tmp, &json)?;
        std::fs::rename(&tmp, &report_path)?;
    }

    info!(
        total_copies = report.total_copies,
        total_pnl_usd = %report.total_pnl_usd,
        win_rate_pct = %report.win_rate_pct,
        open_at_horizon = report.open_at_horizon,
        "simulation complete"
    );

    Ok(report)
}

// ── helpers ───────────────────────────────────────────────────────────────────

/// Build a `LeaderSignal` from a raw simulation trade for routing through evaluate().
///
/// All backtest signals use `NormalLeaderFollow` kind and `Add` action (BUY entry).
fn raw_trade_to_leader_signal(
    trade: &RawTrade,
    operator_id: Option<OperatorId>,
    quality: ReconstructionQuality,
) -> LeaderSignal {
    LeaderSignal {
        leader: TraderId(trade.wallet),
        operator_id,
        venue: VenueId::polymarket(),
        market_id: trade.market_id.clone(),
        outcome_id: trade.outcome_id,
        action: LeaderAction::Add,
        leader_side: trade.side,
        leader_price: trade.price,
        leader_size: Quantity(trade.contracts),
        observed_at: trade.timestamp.0,
        received_at: trade.timestamp.0,
        reconstruction_quality: quality,
        signal_kind: WinnerFollowSignalKind::NormalLeaderFollow,
        inherited_prior: None,
        source_trade_id: trade.source_trade_id.clone(),
        action_confidence_ppm: ProbabilityPpm(700_000),
    }
}

/// Compute the empirical win-rate probability from a leader's ledger.
///
/// Returns `None` when the ledger has no closed trades (no data to derive edge from).
///
/// # Precondition
/// Caller should ensure the ledger has passed the bootstrap filter (>15 closed trades,
/// >95% win rate). This function does not re-apply those thresholds.
fn leader_win_rate_p(ledger: Option<&TraderLedger>) -> Option<Probability> {
    let ledger = ledger?;
    let total = ledger.closed_trades.len();
    if total == 0 {
        return None;
    }
    let wins = ledger
        .closed_trades
        .iter()
        .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
        .count();
    let p_raw = Decimal::from(wins) / Decimal::from(total);
    // Clamp defensively: wins/total is always in [0,1] but saturating arithmetic protects against
    // any edge case in ledger reconstruction.
    Probability::new(p_raw.clamp(Decimal::ZERO, Decimal::ONE)).ok()
}

fn proposed_trade_bps(contracts: u64, price: Decimal, bankroll: Decimal) -> i32 {
    if bankroll <= Decimal::ZERO {
        return 0;
    }
    let notional = Decimal::from(contracts) * price;
    let bps = (notional / bankroll) * Decimal::from(10_000u32);
    bps.floor().to_i32().unwrap_or(0)
}

fn decimal_to_bps(pnl: Decimal, bankroll: Decimal) -> i32 {
    if bankroll <= Decimal::ZERO {
        return 0;
    }
    let bps = (pnl / bankroll) * Decimal::from(10_000u32);
    bps.floor().to_i32().unwrap_or(0)
}

struct RiskContext<'a> {
    exposure: &'a ExposureTracker,
    leader: WalletAddress,
    operator_id: Option<&'a OperatorId>,
    market_id: &'a MarketId,
    intraday_bps: i32,
    rolling_7d_bps: i32,
    has_funder: bool,
    proposed_bps: i32,
}

fn build_risk_snapshot(ctx: &RiskContext<'_>) -> RiskSnapshot {
    RiskSnapshot {
        leader_exposure_bps: BasisPoints(ctx.exposure.leader_bps(ctx.leader)),
        operator_exposure_bps: BasisPoints(ctx.exposure.operator_bps(ctx.operator_id)),
        market_exposure_bps: BasisPoints(ctx.exposure.market_bps(ctx.market_id)),
        family_exposure_bps: BasisPoints(0),
        total_copy_exposure_bps: BasisPoints(ctx.exposure.total),
        funder_inherited_exposure_bps: BasisPoints(0),
        intraday_pnl_bps: BasisPoints(ctx.intraday_bps),
        rolling_7d_pnl_bps: BasisPoints(ctx.rolling_7d_bps),
        anti_gaming_flags: std::collections::HashSet::new(),
        onchain_source_status: SourceStatus::Healthy,
        proxy_funder_mapping_proven: ctx.has_funder,
        funder_seeding_rate_suspicious: false,
        cluster_membership_stable: true,
        funding_hop_count: None,
        copy_latency_p95_ms: 0,
        trading_mode: TradingMode::LiveTiny,
        proposed_trade_bps: BasisPoints(ctx.proposed_bps),
        per_trade_cap_bps: 0, // evaluate() overwrites with resolved cap from WinnerFollowConfig
    }
}

fn maybe_open_trades_ndjson(
    output_dir: &Path,
    write_output: bool,
) -> Result<Option<std::fs::File>, BacktestError> {
    if !write_output {
        return Ok(None);
    }
    std::fs::create_dir_all(output_dir)?;
    let path = output_dir.join("trades.ndjson");
    Ok(Some(std::fs::File::create(path)?))
}

fn write_fill(writer: Option<&mut std::fs::File>, fill: &TradeFill) -> Result<(), BacktestError> {
    let Some(w) = writer else {
        return Ok(());
    };
    let line = serde_json::to_string(fill)?;
    writeln!(w, "{line}")?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn market(s: &str) -> MarketId {
        use pe_core_types::VenueMarketId;
        MarketId(VenueMarketId(s.to_owned()))
    }

    fn op(s: &str) -> OperatorId {
        OperatorId(blake3::hash(s.as_bytes()))
    }

    fn wallet(b: u8) -> WalletAddress {
        WalletAddress::from_hex(&format!("0x{:040x}", b)).unwrap()
    }

    // ── ExposureTracker: operator_bps with None ────────────────────────────────

    #[test]
    fn operator_bps_none_returns_zero_initially() {
        let tracker = ExposureTracker::default();
        assert_eq!(tracker.operator_bps(None), 0);
    }

    #[test]
    fn operator_bps_none_stays_zero_after_none_adds() {
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-a");
        // Add 3 different wallets all with op=None — must not accumulate in any shared bucket.
        for i in 0u8..3 {
            tracker.add(wallet(i), None, &m, 25);
        }
        assert_eq!(
            tracker.operator_bps(None),
            0,
            "None operator must never accumulate exposure"
        );
    }

    #[test]
    fn operator_bps_none_does_not_affect_named_operator() {
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-b");
        let known_op = op("op-alpha");
        tracker.add(wallet(1), Some(&known_op), &m, 50);
        tracker.add(wallet(2), None, &m, 100);
        // The known operator's bps is 50, None's is still 0.
        assert_eq!(tracker.operator_bps(Some(&known_op)), 50);
        assert_eq!(tracker.operator_bps(None), 0);
    }

    #[test]
    fn operator_bps_named_operator_accumulates_correctly() {
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-c");
        let op_a = op("op-a");
        let op_b = op("op-b");
        // Two wallets in op-a each contribute 25 bps.
        tracker.add(wallet(1), Some(&op_a), &m, 25);
        tracker.add(wallet(2), Some(&op_a), &m, 25);
        tracker.add(wallet(3), Some(&op_b), &m, 30);
        assert_eq!(tracker.operator_bps(Some(&op_a)), 50);
        assert_eq!(tracker.operator_bps(Some(&op_b)), 30);
        assert_eq!(tracker.operator_bps(None), 0);
    }

    #[test]
    fn remove_none_operator_does_not_panic_or_corrupt_state() {
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-d");
        tracker.add(wallet(1), None, &m, 25);
        // Remove should mirror add — must not panic and must leave leader/market correct.
        tracker.remove(wallet(1), None, &m, 25);
        assert_eq!(tracker.leader_bps(wallet(1)), 0);
        assert_eq!(tracker.market_bps(&m), 0);
        assert_eq!(tracker.total, 0);
        assert_eq!(tracker.operator_bps(None), 0);
    }

    #[test]
    fn operator_concentration_cap_does_not_block_unrelated_none_wallets() {
        // Verifies the original bug is fixed: N wallets with None operator must not
        // hit the 300 bps concentration cap from each other's exposure.
        let mut tracker = ExposureTracker::default();
        let m = market("mkt-e");
        // Add 20 different wallets each at 25 bps — total 500 bps through None path.
        // operator_bps(None) must stay 0 throughout so the risk gate never fires.
        for i in 0u8..20 {
            tracker.add(wallet(i), None, &m, 25);
            assert_eq!(
                tracker.operator_bps(None),
                0,
                "operator_bps(None) must be 0 after {} adds",
                i + 1
            );
        }
        // per-leader and total are still tracked correctly.
        assert_eq!(tracker.leader_bps(wallet(5)), 25);
        assert_eq!(tracker.total, 500);
    }

    // ── ExposureTracker: leader and market caps still apply for None-operator wallets ──

    #[test]
    fn leader_bps_and_market_bps_track_none_operator_wallets() {
        let mut tracker = ExposureTracker::default();
        let m1 = market("mkt-f");
        let m2 = market("mkt-g");
        tracker.add(wallet(1), None, &m1, 25);
        tracker.add(wallet(1), None, &m2, 25);
        tracker.add(wallet(2), None, &m1, 30);
        assert_eq!(tracker.leader_bps(wallet(1)), 50);
        assert_eq!(tracker.leader_bps(wallet(2)), 30);
        assert_eq!(tracker.market_bps(&m1), 55);
        assert_eq!(tracker.market_bps(&m2), 25);
        assert_eq!(tracker.total, 80);
    }
}
