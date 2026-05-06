//! Walk-forward scoring: daily return series and LCB_5pct computation.
//!
//! All arithmetic uses `rust_decimal::Decimal` (workspace `maths` feature supplies
//! `Decimal::sqrt()`). No `f64` is used.

use std::collections::{HashMap, HashSet};

use pe_core_types::{BasisPoints, MarketId};
use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal_macros::dec;

use crate::ledger::ClosedTrade;

/// Pre-computed stats for a candidate within the eligibility window.
pub(crate) struct CandidateStats {
    /// LCB at the 5th percentile of the daily-return distribution (basis points).
    pub lcb_5pct_bps: BasisPoints,
    /// Composite ranking score: LCB_5pct + bonus/penalty terms (basis points).
    pub leader_score_bps: BasisPoints,
    /// Empirical win rate: wins / closed_trades, expressed in basis points (0–10 000).
    pub win_rate_bps: BasisPoints,
    /// Closed trades observed in the window.
    pub closed_trades_in_window: u32,
    /// Distinct markets traded in the window.
    pub distinct_markets_in_window: u32,
}

/// Compute [`CandidateStats`] for `trades` that fall within `[window_start_unix, now_unix]`.
///
/// Returns `None` when there are no closed trades in the window (score undefined).
pub(crate) fn compute_stats(
    trades: &[ClosedTrade],
    window_start_unix: i64,
    now_unix: i64,
) -> Option<CandidateStats> {
    // Filter to window.
    let in_window: Vec<&ClosedTrade> = trades
        .iter()
        .filter(|t| t.closed_at_unix >= window_start_unix && t.closed_at_unix <= now_unix)
        .collect();

    if in_window.is_empty() {
        return None;
    }

    let closed_trades_in_window = in_window.len() as u32;

    // Empirical win rate: count trades where realized_pnl_usd > 0.
    let wins = in_window
        .iter()
        .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
        .count();
    let win_rate_bps = BasisPoints::from_decimal(
        Decimal::from(wins) / Decimal::from(in_window.len()) * dec!(10_000),
    );

    let distinct_markets_in_window: u32 = in_window
        .iter()
        .map(|t| &t.market_id)
        .collect::<HashSet<&MarketId>>()
        .len() as u32;

    // Bucket returns by UTC calendar day: day = floor(closed_at_unix / 86400).
    let mut daily_returns: HashMap<i64, Decimal> = HashMap::new();
    for trade in &in_window {
        let day = trade.closed_at_unix.div_euclid(86_400);
        let cost = trade.entry_price.0 * Decimal::from(trade.contracts.0);
        let daily_return = if cost.is_zero() {
            Decimal::ZERO
        } else {
            trade.realized_pnl_usd / cost
        };
        *daily_returns.entry(day).or_insert(Decimal::ZERO) += daily_return;
    }

    let n = daily_returns.len() as u32;
    let lcb_5pct_bps = lcb_5pct(&daily_returns.values().copied().collect::<Vec<_>>(), n);

    // leader_score = lcb_5pct + 0  (bonus/penalty terms reserved for future walk-forward tuning).
    // TODO: add diversity bonus, concentration penalty, freshness bonus per 03-PHASE-MODEL-ENGINE.md.
    let leader_score_bps = lcb_5pct_bps;

    Some(CandidateStats {
        lcb_5pct_bps,
        leader_score_bps,
        win_rate_bps,
        closed_trades_in_window,
        distinct_markets_in_window,
    })
}

/// LCB_5pct = mean − 1.645 × stderr, expressed in basis points.
///
/// `stderr = sqrt(variance / n)` using `Decimal::sqrt()` (maths feature).
/// Returns `BasisPoints(i32::MIN)` when `n < 2` (score undefined — caller treats as ineligible).
fn lcb_5pct(returns: &[Decimal], n: u32) -> BasisPoints {
    if n < 2 {
        return BasisPoints(i32::MIN);
    }
    let n_dec = Decimal::from(n);
    let mean = returns.iter().copied().fold(Decimal::ZERO, |a, r| a + r) / n_dec;
    let variance = returns.iter().copied().fold(Decimal::ZERO, |acc, r| {
        let diff = r - mean;
        acc + diff * diff
    }) / n_dec;

    // stderr = sqrt(variance / n); sqrt requires the `maths` feature.
    let stderr = (variance / n_dec).sqrt().unwrap_or(Decimal::ZERO);

    // z_{0.05} ≈ 1.645 (one-tailed 5th percentile of standard normal).
    let lcb = mean - dec!(1.645) * stderr;
    BasisPoints::from_decimal(lcb * dec!(10000))
}
