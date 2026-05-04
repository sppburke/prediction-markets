//! Bootstrap pre-filter: selects wallets with a strong win-rate signal.
//!
//! Canonical thresholds in `docs/_GLOSSARY.md` "Bootstrap defaults" section:
//! `bootstrap_min_closed_trades = 10`, `bootstrap_min_win_rate_pct = 80`.

use pe_trader_index::TraderLedger;
use rust_decimal::Decimal;

// Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub const DEFAULT_MIN_CLOSED_TRADES: usize = 10;
pub const DEFAULT_MIN_WIN_RATE_PCT: u8 = 80;

/// Returns `true` when `ledger` passes the bootstrap pre-filter.
///
/// # Precondition
/// `min_win_rate_pct` must be in 0–100; values above 100 always return `false`.
pub fn passes_filter(
    ledger: &TraderLedger,
    min_closed_trades: usize,
    min_win_rate_pct: u8,
) -> bool {
    let total = ledger.closed_trades.len();
    if total <= min_closed_trades {
        return false;
    }
    let wins = ledger
        .closed_trades
        .iter()
        .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
        .count();
    // wins * 100 > total * min_win_rate_pct  (integer arithmetic, no f64)
    wins.saturating_mul(100) > total.saturating_mul(min_win_rate_pct as usize)
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

    fn make_trade(pnl: rust_decimal::Decimal) -> ClosedTrade {
        ClosedTrade {
            market_id: MarketId(VenueMarketId("0xcond".to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            entry_price: Price::new(dec!(0.40)).unwrap(),
            exit_price: Price::new(dec!(0.60)).unwrap(),
            contracts: ContractQty(10),
            hold_duration_seconds: 3600,
            realized_pnl_usd: pnl,
            opened_at_unix: 1_700_000_000,
            closed_at_unix: 1_700_003_600,
            source_trade_ids: vec![SourceTradeId("0xtx".to_owned())],
        }
    }

    fn make_ledger(wins: usize, losses: usize) -> TraderLedger {
        let mut closed_trades = Vec::new();
        for _ in 0..wins {
            closed_trades.push(make_trade(dec!(2.00)));
        }
        for _ in 0..losses {
            closed_trades.push(make_trade(dec!(-1.00)));
        }
        TraderLedger {
            wallet: WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            operator_id: None,
            reconstruction_quality: ReconstructionQuality::new(80).unwrap(),
            closed_trades,
            open_positions: vec![],
            audit_window_days: 90,
        }
    }

    #[test]
    fn passes_with_11_trades_and_90pct_win_rate() {
        // 10 wins, 1 loss = 90.9% win rate; > 10 trades
        let ledger = make_ledger(10, 1);
        assert!(passes_filter(
            &ledger,
            DEFAULT_MIN_CLOSED_TRADES,
            DEFAULT_MIN_WIN_RATE_PCT
        ));
    }

    #[test]
    fn fails_insufficient_trades() {
        // Exactly 10 trades; filter requires strictly MORE than 10
        let ledger = make_ledger(9, 1);
        assert!(!passes_filter(
            &ledger,
            DEFAULT_MIN_CLOSED_TRADES,
            DEFAULT_MIN_WIN_RATE_PCT
        ));
    }

    #[test]
    fn fails_low_win_rate() {
        // 11 trades but only 7/11 = 63.6% win rate
        let ledger = make_ledger(7, 4);
        assert!(!passes_filter(
            &ledger,
            DEFAULT_MIN_CLOSED_TRADES,
            DEFAULT_MIN_WIN_RATE_PCT
        ));
    }

    #[test]
    fn win_rate_bps_computation() {
        // 9 wins out of 10 = 90% = 9000 bps
        assert_eq!(win_rate_bps(9, 10), 9_000);
        // 0 wins out of 10 = 0 bps
        assert_eq!(win_rate_bps(0, 10), 0);
        // 10/10 = 100% = 10000 bps
        assert_eq!(win_rate_bps(10, 10), 10_000);
        // Zero total: safe
        assert_eq!(win_rate_bps(0, 0), 0);
    }
}
