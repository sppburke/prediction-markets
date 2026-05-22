//! Per-wallet feature extraction over a wallet's train-window ledger (issue #212).
//!
//! This slice computes the **simple deterministic** feature batch — counts,
//! realized PnL, ROI, win rate, and mean hold — directly from the reconstructed
//! `ClosedTrade` vector. The distribution-moment features (Sharpe / skewness /
//! kurtosis / LCB), calibration, and buy-and-hold benchmark land in a later
//! slice; the sign-randomization skill statistic and Deflated Sharpe are filled
//! by the skill-test / selection phases. Because this produces a
//! [`DeterministicFeatures`] value and writes nothing, no field is ever
//! persisted with a placeholder.
//!
//! **Why not reuse `pe_trader_index::compute_stats`?** Its `win_rate_bps` /
//! `lcb_5pct_bps` are double-scaled — it passes `ratio × 10_000` into
//! `BasisPoints::from_decimal`, which multiplies by 10_000 again (`score.rs`
//! win-rate / `lcb_5pct`), yielding values ×10_000 too large. The ranker is
//! internally self-consistent so its *ranking* is unaffected, but the absolute
//! "bps" are wrong, and DSR/BHq selection here needs correct magnitudes. So this
//! crate computes its bps directly with a single ×10_000. (Tracked as an
//! out-of-scope trader-index fix.)
//!
//! All money/ratio arithmetic uses `rust_decimal` (no `f64`); basis-point
//! conversions saturate rather than wrap.

use std::collections::HashMap;
use std::collections::HashSet;

use pe_trader_index::TraderLedger;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// The simple deterministic per-wallet feature batch computed at a given cutoff.
///
/// A subset of the persisted `wallet_features` columns; grows additively as
/// later slices add the distribution-moment / calibration / buy-and-hold
/// features. Assembled into the full `WalletFeatures` row (with the skill-test
/// and DSR outputs) by the extraction phase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeterministicFeatures {
    /// `0x`-prefixed lowercase wallet address.
    pub wallet_hex: String,
    /// Train/forward split this batch was computed at (unix seconds, UTC).
    pub cutoff_unix: i64,
    /// FIFO reconstruction quality, 0..=100.
    pub reconstruction_quality: u8,
    /// Closed trades in the train window (`closed_at ≤ cutoff`).
    pub closed_trades: u32,
    /// Distinct traded markets in the train window.
    pub distinct_markets: u32,
    /// Distinct events (neg-risk bundles, via the market→event map) in the window.
    pub distinct_events: u32,
    /// Sum of realized PnL over the train window (USD).
    pub total_pnl_usd: Decimal,
    /// Return on cost (`total_pnl / total_cost`), basis points; `0` when cost is zero.
    pub roi_bps: i64,
    /// Empirical win rate (`wins / closed`), basis points (0–10 000).
    pub win_rate_bps: i32,
    /// Mean hold duration over closed trades, seconds.
    pub avg_hold_secs: i64,
}

/// Compute the simple deterministic feature batch for one wallet's train ledger.
///
/// Returns `None` when the wallet has no closed trades with `closed_at ≤
/// cutoff_unix`, or fewer than `min_closed_trades` of them (ineligible).
///
/// # Precondition
/// `ledger` should already be reconstructed from trades `≤ cutoff_unix` (the
/// extraction phase partitions raw trades at the cutoff before building the
/// ledger). This function additionally filters closed trades by `closed_at ≤
/// cutoff_unix` defensively, so a stray post-cutoff close cannot leak into the
/// train features.
pub fn extract_features(
    ledger: &TraderLedger,
    cutoff_unix: i64,
    event_map: &HashMap<String, String>,
    min_closed_trades: u32,
) -> Option<DeterministicFeatures> {
    let windowed: Vec<&_> = ledger
        .closed_trades
        .iter()
        .filter(|t| t.closed_at_unix <= cutoff_unix)
        .collect();

    if windowed.is_empty() {
        return None;
    }
    let closed_trades = u32::try_from(windowed.len()).unwrap_or(u32::MAX);
    if closed_trades < min_closed_trades {
        return None;
    }

    // Money: exact Decimal sums; ROI in basis points, saturating on conversion.
    let total_pnl_usd: Decimal = windowed.iter().map(|t| t.realized_pnl_usd).sum();
    let total_cost: Decimal = windowed
        .iter()
        .map(|t| t.entry_price.0 * Decimal::from(t.contracts.0))
        .sum();
    let roi_bps = if total_cost.is_zero() {
        0
    } else {
        decimal_to_bps_i64(total_pnl_usd / total_cost)
    };

    // Win rate over the window: fraction of closed trades with positive PnL.
    let wins = windowed
        .iter()
        .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
        .count();
    let win_ratio = Decimal::from(u64::try_from(wins).unwrap_or(0)) / Decimal::from(closed_trades);
    let win_rate_bps = i32::try_from(decimal_to_bps_i64(win_ratio)).unwrap_or(10_000);

    let distinct_markets = windowed
        .iter()
        .map(|t| &t.market_id.0.0)
        .collect::<HashSet<&String>>()
        .len();

    // Distinct events: map each market to its event_id; orphans self-map to the
    // market id (so an unmapped market still counts as its own event).
    let distinct_events = windowed
        .iter()
        .map(|t| {
            let market = &t.market_id.0.0;
            event_map.get(market).unwrap_or(market)
        })
        .collect::<HashSet<&String>>()
        .len();

    // Mean hold: sum in u128 to avoid overflow, then saturate into i64.
    let total_hold: u128 = windowed
        .iter()
        .map(|t| u128::from(t.hold_duration_seconds))
        .sum();
    let count = u128::try_from(windowed.len()).unwrap_or(1).max(1);
    let avg_hold_secs = i64::try_from(total_hold / count).unwrap_or(i64::MAX);

    Some(DeterministicFeatures {
        wallet_hex: ledger.wallet.to_string(),
        cutoff_unix,
        reconstruction_quality: ledger.reconstruction_quality.get(),
        closed_trades,
        distinct_markets: u32::try_from(distinct_markets).unwrap_or(u32::MAX),
        distinct_events: u32::try_from(distinct_events).unwrap_or(u32::MAX),
        total_pnl_usd,
        roi_bps,
        win_rate_bps,
        avg_hold_secs,
    })
}

/// Convert a ratio to basis points (`× 10_000`, rounded half-even), saturating
/// to the `i64` range rather than wrapping or panicking. No `f64` — `to_i64`
/// reads the `Decimal` representation directly.
fn decimal_to_bps_i64(ratio: Decimal) -> i64 {
    let bps = (ratio * Decimal::from(10_000i64))
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointNearestEven);
    bps.to_i64().unwrap_or(if bps.is_sign_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, ReconstructionQuality, Side, VenueMarketId,
        WalletAddress,
    };
    use pe_trader_index::ClosedTrade;
    use rust_decimal_macros::dec;

    fn wallet() -> WalletAddress {
        WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    /// A closed trade with the given market, entry, size, pnl, hold, close time.
    fn closed(
        market: &str,
        entry: Decimal,
        contracts: u64,
        pnl: Decimal,
        hold_secs: u64,
        closed_at: i64,
    ) -> ClosedTrade {
        ClosedTrade {
            market_id: MarketId(VenueMarketId(market.to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            entry_price: Price::new(entry).unwrap(),
            exit_price: Price::new(dec!(1.0)).unwrap(),
            contracts: ContractQty(contracts),
            hold_duration_seconds: hold_secs,
            realized_pnl_usd: pnl,
            opened_at_unix: closed_at - i64::try_from(hold_secs).unwrap_or(0),
            closed_at_unix: closed_at,
            source_trade_ids: vec![],
        }
    }

    fn ledger(closed_trades: Vec<ClosedTrade>) -> TraderLedger {
        TraderLedger {
            wallet: wallet(),
            operator_id: None,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            closed_trades,
            open_positions: vec![],
            audit_window_days: 0,
        }
    }

    #[test]
    fn computes_deterministic_batch_by_hand() {
        // Markets m1,m2 share event "evtA"; m3 unmapped → self-maps.
        let mut events = HashMap::new();
        events.insert("0xm1".to_owned(), "evtA".to_owned());
        events.insert("0xm2".to_owned(), "evtA".to_owned());
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(10.0), 3_600, 1_000),
            closed("0xm2", dec!(0.50), 100, dec!(-5.0), 7_200, 2_000),
            closed("0xm3", dec!(0.20), 50, dec!(2.0), 1_800, 3_000),
        ]);

        let f = extract_features(&l, 10_000, &events, 1).unwrap();

        assert_eq!(f.total_pnl_usd, dec!(7.0)); // 10 - 5 + 2
        assert_eq!(f.roi_bps, 636); // 7 / 110 = 0.063636… → 636 bps
        assert_eq!(f.closed_trades, 3);
        assert_eq!(f.distinct_markets, 3);
        assert_eq!(f.distinct_events, 2); // {evtA, 0xm3}
        assert_eq!(f.avg_hold_secs, 4_200); // (3600+7200+1800)/3
        assert_eq!(f.reconstruction_quality, 100);
        assert_eq!(f.wallet_hex, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(f.win_rate_bps, 6_667); // 2/3 → 6666.67 → 6667 (single ×10000, correct)
    }

    #[test]
    fn excludes_post_cutoff_closes() {
        let events = HashMap::new();
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(10.0), 3_600, 1_000),
            closed("0xm2", dec!(0.50), 100, dec!(99.0), 3_600, 9_999), // after cutoff
        ]);
        let f = extract_features(&l, 5_000, &events, 1).unwrap();
        assert_eq!(f.closed_trades, 1);
        assert_eq!(f.total_pnl_usd, dec!(10.0));
        assert_eq!(f.win_rate_bps, 10_000); // the one in-window trade won
    }

    #[test]
    fn none_below_min_closed_trades() {
        let events = HashMap::new();
        let l = ledger(vec![closed("0xm1", dec!(0.5), 10, dec!(1.0), 60, 1_000)]);
        assert!(extract_features(&l, 10_000, &events, 5).is_none());
    }

    #[test]
    fn none_when_no_closed_trades() {
        let events = HashMap::new();
        let l = ledger(vec![]);
        assert!(extract_features(&l, 10_000, &events, 1).is_none());
    }
}
