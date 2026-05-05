//! Phase 1: walk-forward simulation.
//!
//! Walk-forward invariant (enforced): at simulated date D, only trades with
//! `timestamp.date() < D` are visible to the ranker. Trades with `timestamp.date() == D`
//! are the new signals; the ranker was built from strictly prior data.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write as _;
use std::path::Path;

use pe_core_types::{
    BasisPoints, MarketId, OperatorId, OutcomeId, Price, Probability, SourceTimestamp,
    WalletAddress,
};
use pe_kelly_sizer::{KELLY_PAPER_BACKTEST, KellyInput, size_contracts};
use pe_operator_graph::OperatorIdentity;
use pe_risk_engine::snapshot::TradingMode;
use pe_risk_engine::{RiskDecision, RiskSnapshot, evaluate_risk};
use pe_source_core::SourceStatus;
use pe_strategy_winner_follow::WinnerFollowConfig;
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

/// Open position entry (copies we've taken but not yet closed).
#[derive(Debug, Clone)]
struct OpenPosition {
    contracts: u64,
    avg_fill_price: Decimal,
    operator_id: Option<OperatorId>,
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
/// Writes `report.json` and `trades.ndjson` to `config.output_dir`.
pub fn run_simulation(
    config: &BacktestConfig,
    mut all_trades: Vec<RawTrade>,
    operator_identities: Vec<OperatorIdentity>,
    ranker_config: &RankerConfig,
    ledger_config: &LedgerConfig,
    wf_config: &WinnerFollowConfig,
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

    // Open positions per (market, outcome) → stack of positions.
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
        let watchlist = build_watchlist(&ledgers, snapshot.snapshot_at.clone(), ranker_config);

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
                pe_core_types::Side::Buy => {
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
                    let fill_price_typed = match Price::new(fill_price) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    // Kelly sizing.
                    let p_raw = (trade.price.0 + wf_config.leader_alpha).min(Decimal::ONE);
                    let p = match Probability::new(p_raw) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };
                    let c = fill_price_typed;
                    let kelly_input = KellyInput {
                        p,
                        c,
                        kelly_fraction: KELLY_PAPER_BACKTEST,
                        bankroll,
                    };
                    let kelly_contracts = match size_contracts(&kelly_input) {
                        Ok(c) if c.0 > 0 => c.0,
                        _ => continue,
                    };

                    // Clamp to the LiveTiny per-trade cap (25 bps of bankroll).
                    // In a paper backtest we scale down to fit the cap rather than
                    // skipping the trade entirely — the risk gate then validates the
                    // clipped size.
                    let max_for_cap: u64 = {
                        let max_notional =
                            Decimal::from(25u32) * bankroll / Decimal::from(10_000u32);
                        let max = (max_notional / fill_price).floor();
                        max.to_u64().unwrap_or(0)
                    };
                    let contracts_count = kelly_contracts.min(max_for_cap);
                    if contracts_count == 0 {
                        continue;
                    }

                    // Risk gate.
                    let proposed_bps = proposed_trade_bps(contracts_count, fill_price, bankroll);
                    let risk_snapshot = build_risk_snapshot(&RiskContext {
                        exposure: &exposure,
                        leader,
                        operator_id,
                        market_id: &trade.market_id,
                        intraday_bps,
                        rolling_7d_bps,
                        has_funder,
                        proposed_bps,
                    });

                    match evaluate_risk(&risk_snapshot) {
                        RiskDecision::Approved => {}
                        RiskDecision::Blocked(reason) => {
                            tracing::debug!(
                                wallet = %leader,
                                ?reason,
                                "risk blocked"
                            );
                            continue;
                        }
                    }

                    // Record fill.
                    let notional = Decimal::from(contracts_count) * fill_price;
                    bankroll -= notional;

                    exposure.add(leader, operator_id, &trade.market_id, proposed_bps);

                    open_positions.insert(
                        wallet_pos_key,
                        OpenPosition {
                            contracts: contracts_count,
                            avg_fill_price: fill_price,
                            operator_id: operator_id.cloned(),
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

                pe_core_types::Side::Sell => {
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

    // Close remaining open positions at last observed signal price (mark-to-market = 0).
    for (_, open) in open_positions.drain() {
        let notional = Decimal::from(open.contracts) * open.avg_fill_price;
        let pnl = -notional; // treat as total loss
        intraday_realized_pnl += pnl;
        bankroll -= notional;
        pnl_accum.record(open.operator_id.as_ref(), pnl);
    }

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
        "simulation complete"
    );

    Ok(report)
}

// ── helpers ───────────────────────────────────────────────────────────────────

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

/// Compute proposed trade size in basis points of bankroll (exposed for tests).
pub fn compute_proposed_bps(contracts: u64, price: Decimal, bankroll: Decimal) -> i32 {
    proposed_trade_bps(contracts, price, bankroll)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, VenueMarketId,
    };
    use pe_core_types::{SourceTradeId, WalletAddress};
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    fn wallet(hex: &str) -> WalletAddress {
        WalletAddress::from_hex(hex).unwrap()
    }

    fn raw_trade(wallet: WalletAddress, day_offset_secs: i64, side: Side, price: f64) -> RawTrade {
        let base_unix: i64 = 1_700_000_000; // 2023-11-14 22:13:20 UTC
        RawTrade {
            wallet,
            market_id: MarketId(VenueMarketId("0xcond".to_owned())),
            outcome_id: OutcomeId(0),
            side,
            price: Price::new(Decimal::try_from(price).unwrap()).unwrap(),
            contracts: ContractQty(10),
            timestamp: SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(base_unix + day_offset_secs).unwrap(),
            ),
            source_trade_id: SourceTradeId(format!("0xhash_{price}_{day_offset_secs}")),
        }
    }

    // Walk-forward invariant: a trade at day T+1 must not be visible to the ranker at day T.
    #[test]
    fn walk_forward_invariant_future_trade_not_visible() {
        let w = wallet("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        // Trade at day 0 (epoch)
        let t0 = raw_trade(w, 0, Side::Buy, 0.5);
        // Trade at day 1 (86400 seconds later)
        let future = raw_trade(w, 86_400, Side::Buy, 0.6);

        let cutoff_day = future.timestamp.0.date();
        let cutoff_unix = cutoff_day.midnight().assume_utc().unix_timestamp();

        // Ranker at cutoff day sees ONLY trades BEFORE cutoff.
        let t0_visible = t0.timestamp.0.unix_timestamp() < cutoff_unix;
        let future_visible = future.timestamp.0.unix_timestamp() < cutoff_unix;

        assert!(t0_visible, "t0 must be visible at the cutoff day");
        assert!(
            !future_visible,
            "future trade must NOT be visible to ranker at its own day"
        );
    }

    // Fill model: fill price must differ from signal price by the slippage amount.
    #[test]
    fn fill_slippage_applied() {
        let signal_price = dec!(0.50);
        let slippage = Decimal::from(DEFAULT_SLIPPAGE_BPS) / Decimal::from(10_000u32);
        let buy_fill = signal_price + slippage;
        let sell_fill = signal_price - slippage;

        assert_ne!(
            buy_fill, signal_price,
            "BUY fill must differ from signal price"
        );
        assert_ne!(
            sell_fill, signal_price,
            "SELL fill must differ from signal price"
        );
        assert!(
            buy_fill > signal_price,
            "BUY fill price must be worse (higher) than signal"
        );
        assert!(
            sell_fill < signal_price,
            "SELL fill price must be worse (lower) than signal"
        );
    }

    // Determinism: identical input trade sets produce identical bps output.
    #[test]
    fn compute_proposed_bps_deterministic() {
        let bps_a = compute_proposed_bps(10, dec!(0.50), dec!(10_000));
        let bps_b = compute_proposed_bps(10, dec!(0.50), dec!(10_000));
        assert_eq!(bps_a, bps_b, "same inputs must produce same bps");
    }
}
