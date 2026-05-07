//! `WinnerFollowReport` — output of the walk-forward backtest.

use std::collections::HashMap;
use std::path::PathBuf;

use pe_core_types::{KellyFraction, OperatorId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Summary report for a walk-forward Winner-Follow backtest run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WinnerFollowReport {
    pub total_pnl_usd: Decimal,
    /// Annualised Sharpe ratio from daily PnL series (0 if fewer than 2 days).
    pub sharpe_ratio: Decimal,
    /// Maximum peak-to-trough drawdown as a percentage (0–100).
    pub max_drawdown_pct: Decimal,
    pub total_copies: u64,
    /// Win rate as a percentage (0–100).
    pub win_rate_pct: Decimal,
    pub per_operator_pnl: HashMap<String, Decimal>,
    #[serde(with = "time::serde::rfc3339")]
    pub simulation_start: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub simulation_end: OffsetDateTime,
    pub bankroll_initial: Decimal,
    pub bankroll_final: Decimal,
    /// Slippage applied to each fill, in basis points of the signal price.
    /// Canonical default: `backtest_slippage_bps = 100`.
    pub slippage_assumption_bps: u32,
    /// Number of copy positions still open at the simulation horizon.
    /// These are excluded from `total_pnl_usd` because their final PnL is unknown.
    pub open_at_horizon: u64,
    /// True when the funder graph was built from today's Etherscan data, not from data
    /// as it existed at each simulated time T. Relationships established after T_past
    /// may appear in the graph — a known conservative approximation.
    pub funder_graph_snapshot_caveat: bool,
}

/// One run within a Kelly-fraction sweep.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KellySweepRun {
    pub kelly_fraction: KellyFraction,
    pub report: WinnerFollowReport,
}

/// Output of a Kelly-fraction sweep — N sequential backtests on identical data.
///
/// Written to `${PE_BACKTEST_OUTPUT_DIR}/kelly-sweep-{ISO8601}.json`.
/// Per-run `report.json` / `trades.ndjson` are suppressed in sweep mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KellySweepReport {
    pub runs: Vec<KellySweepRun>,
    pub cache_path: PathBuf,
    #[serde(with = "time::serde::rfc3339")]
    pub executed_at: OffsetDateTime,
}

impl KellySweepReport {
    /// Render a markdown comparison table to stdout.
    pub fn to_markdown_table(&self) -> String {
        let mut out = String::new();
        out.push_str("| Kelly fraction | Total PnL (USD) | Sharpe | Max DD % | Win rate % | Copies | Open at horizon |\n");
        out.push_str("|---:|---:|---:|---:|---:|---:|---:|\n");
        for run in &self.runs {
            let r = &run.report;
            out.push_str(&format!(
                "| {:.2} | {:.2} | {:.3} | {:.1} | {:.1} | {} | {} |\n",
                run.kelly_fraction.0,
                r.total_pnl_usd,
                r.sharpe_ratio,
                r.max_drawdown_pct,
                r.win_rate_pct,
                r.total_copies,
                r.open_at_horizon,
            ));
        }
        out
    }
}

/// A single paper-fill record written to trades.ndjson.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeFill {
    #[serde(with = "time::serde::rfc3339")]
    pub simulated_at: OffsetDateTime,
    pub leader_wallet: String,
    pub operator_id: Option<String>,
    pub market_id: String,
    pub outcome_id: u8,
    pub side: String,
    pub contracts: u64,
    pub signal_price: Decimal,
    pub fill_price: Decimal,
}

/// Per-operator PnL accumulator.
#[derive(Debug, Default)]
pub struct PnlAccumulator {
    pub per_operator: HashMap<String, Decimal>,
    pub total_wins: u64,
    pub total_copies: u64,
}

impl PnlAccumulator {
    pub fn record(&mut self, operator_id: Option<&OperatorId>, pnl: Decimal) {
        let key = operator_id.map_or_else(|| "unknown".to_owned(), |o| o.to_string());
        *self.per_operator.entry(key).or_default() += pnl;
        self.total_copies += 1;
        if pnl > Decimal::ZERO {
            self.total_wins += 1;
        }
    }

    pub fn win_rate_pct(&self) -> Decimal {
        if self.total_copies == 0 {
            return Decimal::ZERO;
        }
        Decimal::from(self.total_wins * 100) / Decimal::from(self.total_copies)
    }
}

/// Compute annualised Sharpe ratio from daily PnL values.
///
/// Returns 0 if fewer than 2 data points.
pub fn sharpe_ratio(daily_pnl: &[Decimal]) -> Decimal {
    if daily_pnl.len() < 2 {
        return Decimal::ZERO;
    }
    let n = Decimal::from(daily_pnl.len());
    let mean = daily_pnl.iter().sum::<Decimal>() / n;
    let variance = daily_pnl
        .iter()
        .map(|x| {
            let diff = x - mean;
            diff * diff
        })
        .sum::<Decimal>()
        / (n - Decimal::ONE);

    if variance <= Decimal::ZERO {
        return Decimal::ZERO;
    }

    // std_dev approximation: use Newton's method for sqrt on Decimal
    let std_dev = decimal_sqrt(variance);
    if std_dev == Decimal::ZERO {
        return Decimal::ZERO;
    }

    // Annualise: multiply daily Sharpe by sqrt(365).
    // sqrt(365) ≈ 19.105. Use 3-decimal precision.
    let annualisation = Decimal::new(19_105, 3);
    (mean / std_dev) * annualisation
}

/// Compute max peak-to-trough drawdown percentage from daily bankroll series.
pub fn max_drawdown_pct(daily_bankroll: &[Decimal]) -> Decimal {
    if daily_bankroll.len() < 2 {
        return Decimal::ZERO;
    }
    let mut peak = daily_bankroll[0];
    let mut max_dd = Decimal::ZERO;
    for &b in daily_bankroll {
        if b > peak {
            peak = b;
        }
        if peak > Decimal::ZERO {
            let dd = (peak - b) / peak * Decimal::from(100u32);
            if dd > max_dd {
                max_dd = dd;
            }
        }
    }
    max_dd
}

/// Integer Newton's method sqrt for Decimal.
///
/// Accurate to within 1 ULP for values typical of daily PnL variance.
fn decimal_sqrt(x: Decimal) -> Decimal {
    if x <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    // Seed with f64 sqrt for fast convergence.
    use rust_decimal::prelude::ToPrimitive as _;
    let seed = x.to_f64().unwrap_or(1.0).sqrt();
    let mut g = Decimal::try_from(seed).unwrap_or(Decimal::ONE);
    if g <= Decimal::ZERO {
        g = Decimal::ONE;
    }
    // 10 Newton iterations are sufficient for any Decimal value.
    for _ in 0..10 {
        let g2 = (g + x / g) / Decimal::TWO;
        if g2 == g {
            break;
        }
        g = g2;
    }
    g
}
