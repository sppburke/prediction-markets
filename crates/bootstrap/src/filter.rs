//! Bootstrap post-filter: selects wallets with strong win-rate, recency, and
//! timing characteristics derived from the local `TraderLedger`.
//!
//! The four conditions mirror the Dune SQL query applied at wallet-discovery
//! time (see `dune::WALLET_DISCOVERY_SQL`). Applying them here provides a
//! second, independent quality gate on local ledger data.
//!
//! Canonical thresholds in `docs/_GLOSSARY.md` "Bootstrap defaults" section.

use pe_trader_index::TraderLedger;
use rust_decimal::Decimal;

// Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub const DEFAULT_MIN_CLOSED_TRADES: usize = 15;
pub const DEFAULT_MIN_WIN_RATE_PCT: u8 = 95;
pub const DEFAULT_ACTIVE_WINDOW_DAYS: u32 = 30;
pub const DEFAULT_MAX_AVG_HOURS_TO_RESOLUTION: u32 = 72;

/// Configuration for the bootstrap post-filter.
///
/// All fields correspond to canonical defaults in `docs/_GLOSSARY.md`
/// "Bootstrap defaults" section.
pub struct FilterConfig {
    /// Minimum closed trades (exclusive: must have > this many).
    pub min_closed_trades: usize,
    /// Minimum win-rate percent (exclusive: must exceed this percentage).
    pub min_win_rate_pct: u8,
    /// Recency window in calendar days: at least one trade must have been
    /// opened within this many days of `snapshot_at_unix`.
    pub active_window_days: u32,
    /// Maximum average hours from first entry on a market to that market's
    /// resolution; filters out late entrants (exclusive: average must be < this).
    pub max_avg_hours_to_resolution: u32,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            min_closed_trades: DEFAULT_MIN_CLOSED_TRADES,
            min_win_rate_pct: DEFAULT_MIN_WIN_RATE_PCT,
            active_window_days: DEFAULT_ACTIVE_WINDOW_DAYS,
            max_avg_hours_to_resolution: DEFAULT_MAX_AVG_HOURS_TO_RESOLUTION,
        }
    }
}

/// Returns `true` when `ledger` passes all four bootstrap post-filter conditions.
///
/// Conditions:
/// 1. `> min_closed_trades` distinct closed trades.
/// 2. Win rate `> min_win_rate_pct` (integer arithmetic, no f64).
/// 3. At least one trade opened within `active_window_days` calendar days before
///    `snapshot_at_unix`.
/// 4. Average hold duration on winning trades (opened_at → closed_at) is
///    `< max_avg_hours_to_resolution` hours. Only winning trades where
///    `closed_at_unix > opened_at_unix` contribute (mirrors Dune's
///    `resolved_at > first_trade_time` guard).
///
/// # Precondition
/// `min_win_rate_pct` must be in 0–100; values above 100 always return `false`.
/// `snapshot_at_unix` should be a recent Unix timestamp; stale values relax condition 3.
pub fn passes_filter(ledger: &TraderLedger, snapshot_at_unix: i64, config: &FilterConfig) -> bool {
    let total = ledger.closed_trades.len();

    // Condition 1: must have strictly more than min_closed_trades.
    if total <= config.min_closed_trades {
        return false;
    }

    // Condition 2: win rate > min_win_rate_pct (integer arithmetic, no f64).
    let wins = ledger
        .closed_trades
        .iter()
        .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
        .count();
    if wins.saturating_mul(100) <= total.saturating_mul(usize::from(config.min_win_rate_pct)) {
        return false;
    }

    // Condition 3: at least one trade opened within the active window.
    let cutoff_unix = snapshot_at_unix
        .saturating_sub(i64::from(config.active_window_days).saturating_mul(86_400));
    if !ledger
        .closed_trades
        .iter()
        .any(|t| t.opened_at_unix >= cutoff_unix)
    {
        return false;
    }

    // Condition 4: average hold on winning trades < max_avg_hours_to_resolution.
    // Only count trades where closed_at_unix > opened_at_unix (guards against
    // data artifacts where settlement and entry share the same timestamp).
    let (winning_count, total_hold_secs) = ledger
        .closed_trades
        .iter()
        .filter(|t| t.realized_pnl_usd > Decimal::ZERO && t.closed_at_unix > t.opened_at_unix)
        .fold((0u64, 0u64), |(n, sum), t| {
            let hold = u64::try_from(t.closed_at_unix - t.opened_at_unix).unwrap_or(0);
            (n + 1, sum.saturating_add(hold))
        });
    if winning_count > 0 {
        let max_secs = u64::from(config.max_avg_hours_to_resolution)
            .saturating_mul(3_600)
            .saturating_mul(winning_count);
        if total_hold_secs >= max_secs {
            return false;
        }
    }

    true
}

/// Win rate expressed as basis points (0–10_000) for use as `leader_score_bps`.
///
/// # Precondition
/// Caller must guarantee `total > 0`; returns 0 if `total == 0`.
pub fn win_rate_bps(wins: usize, total: usize) -> i32 {
    if total == 0 {
        return 0;
    }
    let bps = wins.saturating_mul(10_000) / total;
    i32::try_from(bps.min(10_000)).unwrap_or(i32::MAX)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceTradeId,
        VenueMarketId, WalletAddress,
    };
    use pe_trader_index::ledger::{ClosedTrade, TraderLedger};
    use rust_decimal_macros::dec;

    const BASE_TS: i64 = 1_700_000_000;
    const SNAP: i64 = BASE_TS + 86_400; // 1 day after BASE_TS

    fn make_trade(pnl: rust_decimal::Decimal, opened_at: i64, hold_secs: i64) -> ClosedTrade {
        ClosedTrade {
            market_id: MarketId(VenueMarketId("0xcond".to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            entry_price: Price::new(dec!(0.40)).unwrap(),
            exit_price: Price::new(dec!(0.60)).unwrap(),
            contracts: ContractQty(10),
            hold_duration_seconds: u64::try_from(hold_secs).unwrap_or(0),
            realized_pnl_usd: pnl,
            opened_at_unix: opened_at,
            closed_at_unix: opened_at + hold_secs,
            source_trade_ids: vec![SourceTradeId("0xtx".to_owned())],
        }
    }

    fn make_ledger(trades: Vec<ClosedTrade>) -> TraderLedger {
        TraderLedger {
            wallet: WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            operator_id: None,
            reconstruction_quality: ReconstructionQuality::new(80).unwrap(),
            closed_trades: trades,
            open_positions: vec![],
            audit_window_days: 90,
        }
    }

    /// 16 wins with recent activity and short avg hold — passes all 4 conditions.
    #[test]
    fn passes_all_four_conditions() {
        let trades = (0..16)
            .map(|_| make_trade(dec!(2.00), BASE_TS, 7_200)) // 2h hold, within SNAP's 30-day window
            .collect();
        let ledger = make_ledger(trades);
        assert!(passes_filter(&ledger, SNAP, &FilterConfig::default()));
    }

    /// Exactly 15 trades — not strictly more than 15 → fails condition 1.
    #[test]
    fn fails_insufficient_trades() {
        let trades = (0..15)
            .map(|_| make_trade(dec!(2.00), BASE_TS, 7_200))
            .collect();
        let ledger = make_ledger(trades);
        assert!(!passes_filter(&ledger, SNAP, &FilterConfig::default()));
    }

    /// 16 trades, 14 wins = 87.5% < 95% → fails condition 2.
    #[test]
    fn fails_low_win_rate() {
        let mut trades: Vec<ClosedTrade> = (0..14)
            .map(|_| make_trade(dec!(2.00), BASE_TS, 7_200))
            .collect();
        trades.extend((0..2).map(|_| make_trade(dec!(-1.00), BASE_TS, 7_200)));
        let ledger = make_ledger(trades);
        assert!(!passes_filter(&ledger, SNAP, &FilterConfig::default()));
    }

    /// 16 wins but last trade was 31 days before snapshot → fails condition 3.
    #[test]
    fn fails_not_recently_active() {
        let stale = SNAP - 31 * 86_400;
        let trades = (0..16)
            .map(|_| make_trade(dec!(2.00), stale, 7_200))
            .collect();
        let ledger = make_ledger(trades);
        assert!(!passes_filter(&ledger, SNAP, &FilterConfig::default()));
    }

    /// 16 wins each with 73h hold → avg = 73h ≥ 72h limit → fails condition 4.
    #[test]
    fn fails_avg_hold_too_long() {
        let trades = (0..16)
            .map(|_| make_trade(dec!(2.00), BASE_TS, 73 * 3_600))
            .collect();
        let ledger = make_ledger(trades);
        assert!(!passes_filter(&ledger, SNAP, &FilterConfig::default()));
    }

    /// 71h59m59s avg hold — just under the 72h limit → passes condition 4.
    #[test]
    fn passes_exactly_at_avg_hold_boundary() {
        let trades = (0..16)
            .map(|_| make_trade(dec!(2.00), BASE_TS, 72 * 3_600 - 1))
            .collect();
        let ledger = make_ledger(trades);
        assert!(passes_filter(&ledger, SNAP, &FilterConfig::default()));
    }

    #[test]
    fn win_rate_bps_computation() {
        assert_eq!(win_rate_bps(9, 10), 9_000);
        assert_eq!(win_rate_bps(0, 10), 0);
        assert_eq!(win_rate_bps(10, 10), 10_000);
        assert_eq!(win_rate_bps(0, 0), 0);
    }
}
