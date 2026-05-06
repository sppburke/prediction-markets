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
use pe_risk_engine::block::RiskBlock;
use pe_risk_engine::snapshot::TradingMode;
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::{WinnerFollowError, WinnerFollowStrategy};
use pe_trader_index::ledger::TraderLedger;
use pe_trader_index::snapshot::{RawTrade, TradeSnapshot};
use pe_trader_index::{LedgerConfig, RankerConfig, build_trader_ledgers, build_watchlist};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use time::{Date, OffsetDateTime};
use tracing::info;

use crate::config::BacktestConfig;
use crate::error::BacktestError;
use crate::report::{
    PnlAccumulator, TradeFill, WinnerFollowReport, max_drawdown_pct, sharpe_ratio,
};

// Canonical default in `docs/_GLOSSARY.md` "Backtest defaults".
const DEFAULT_SLIPPAGE_BPS: u32 = 100;

// Per-trade size cap in bps of bankroll (LiveTiny mode). Mirrors risk-engine's cap.
const LIVETINY_PER_TRADE_CAP_BPS: u32 = 25;

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
    by_operator: HashMap<String, i32>,
    /// Open exposure in bps of bankroll per market.
    by_market: HashMap<MarketId, i32>,
    /// Total copy exposure across all positions.
    total: i32,
}

impl ExposureTracker {
    fn leader_bps(&self, w: WalletAddress) -> i32 {
        self.by_leader.get(&w).copied().unwrap_or(0)
    }

    fn operator_bps(&self, op: Option<&OperatorId>) -> i32 {
        let key = op.map_or_else(|| "unknown".to_owned(), |o| o.to_string());
        self.by_operator.get(&key).copied().unwrap_or(0)
    }

    fn market_bps(&self, m: &MarketId) -> i32 {
        self.by_market.get(m).copied().unwrap_or(0)
    }

    fn add(&mut self, w: WalletAddress, op: Option<&OperatorId>, m: &MarketId, bps: i32) {
        *self.by_leader.entry(w).or_default() += bps;
        let key = op.map_or_else(|| "unknown".to_owned(), |o| o.to_string());
        *self.by_operator.entry(key).or_default() += bps;
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
/// Writes `report.json` and `trades.ndjson` to `config.output_dir`.
#[allow(clippy::too_many_arguments)]
pub fn run_simulation(
    config: &BacktestConfig,
    mut all_trades: Vec<RawTrade>,
    operator_identities: Vec<OperatorIdentity>,
    snapshots: &LeaderboardSnapshots,
    resolutions: &ResolutionIndex,
    ranker_config: &RankerConfig,
    ledger_config: &LedgerConfig,
    strategy: &WinnerFollowStrategy,
) -> Result<WinnerFollowReport, BacktestError> {
    // Build wallet → operator map for quick lookup.
    let wallet_to_operator: HashMap<WalletAddress, &OperatorIdentity> = operator_identities
        .iter()
        .flat_map(|op| op.member_wallets.iter().map(move |w| (*w, op)))
        .collect();

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

    let mut fills_writer = open_trades_ndjson(&config.output_dir)?;

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
            write_fill(&mut fills_writer, &fill)?;
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
            let has_funder = op_identity.is_some();

            let pos_key = (trade.market_id.clone(), trade.outcome_id);
            let wallet_pos_key = (leader, pos_key.clone());

            match trade.side {
                Side::Buy => {
                    // Open a new copy position if we don't already have one for this key.
                    if open_positions.contains_key(&wallet_pos_key) {
                        continue; // Already tracking this leader's position.
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
                        Err(WinnerFollowError::Blocked(RiskBlock::PerTradeSizeExceeded)) => {
                            // Kelly exceeds the 25-bps cap. Clip to the cap-sized position.
                            let max_notional = Decimal::from(LIVETINY_PER_TRADE_CAP_BPS) * bankroll
                                / Decimal::from(10_000u32);
                            let max = (max_notional / fill_price).floor();
                            max.to_u64().unwrap_or(0)
                        }
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
                    write_fill(&mut fills_writer, &fill)?;
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
                    write_fill(&mut fills_writer, &fill)?;
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
        funder_graph_snapshot_caveat: true,
    };

    // Write report.json.
    let report_path = config.output_dir.join("report.json");
    let json = serde_json::to_vec_pretty(&report)?;
    let tmp = report_path.with_extension("json.tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, &report_path)?;

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
    }
}

fn open_trades_ndjson(output_dir: &Path) -> Result<std::fs::File, BacktestError> {
    std::fs::create_dir_all(output_dir)?;
    let path = output_dir.join("trades.ndjson");
    Ok(std::fs::File::create(path)?)
}

fn write_fill(writer: &mut std::fs::File, fill: &TradeFill) -> Result<(), BacktestError> {
    let line = serde_json::to_string(fill)?;
    writeln!(writer, "{line}")?;
    Ok(())
}
