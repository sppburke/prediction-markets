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

use pe_trader_index::{ClosedTrade, TraderLedger};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, MathematicalOps};

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
    /// Distinct UTC calendar days with a closed trade (the daily-return series length).
    pub trading_days: u32,
    /// Mean of the daily return-on-cost series, basis points.
    pub mean_daily_return_bps: i64,
    /// Population standard deviation of the daily-return series, basis points.
    pub std_daily_return_bps: i64,
    /// Per-period Sharpe (`mean / std`) of the daily-return series × 10_000;
    /// `0` when `std` is zero (DSR applies the √n scaling later).
    pub sharpe_bps: i64,
    /// Fisher skewness of the daily-return series × 10_000; `0` when `std` is zero.
    pub skewness_bps: i64,
    /// Excess kurtosis (kurtosis − 3) of the daily-return series × 10_000; `0` when `std` is zero.
    pub excess_kurtosis_bps: i64,
    /// Lower-confidence bound `mean − 1.645·(std/√n)` of the daily-return series,
    /// basis points (5th-pct one-sided). Equals the mean when `n < 2`.
    pub lcb_5pct_bps: i32,
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

    // Daily return-on-cost series → distribution moments (DSR / LCB inputs).
    let daily = daily_return_series(&windowed);
    let m = compute_moments(&daily);

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
        trading_days: u32::try_from(daily.len()).unwrap_or(u32::MAX),
        mean_daily_return_bps: decimal_to_bps_i64(m.mean),
        std_daily_return_bps: decimal_to_bps_i64(m.std),
        sharpe_bps: decimal_to_bps_i64(m.sharpe),
        skewness_bps: decimal_to_bps_i64(m.skewness),
        excess_kurtosis_bps: decimal_to_bps_i64(m.excess_kurtosis),
        lcb_5pct_bps: i32::try_from(decimal_to_bps_i64(m.lcb_5pct)).unwrap_or_else(|_| {
            if m.lcb_5pct.is_sign_negative() {
                i32::MIN
            } else {
                i32::MAX
            }
        }),
    })
}

/// Build the per-UTC-day return-on-cost series: each trade's return is
/// `realized_pnl / (entry_price × contracts)`; per-day returns sum the trades
/// that closed that day (`day = floor(closed_at / 86_400)`). Mirrors the daily
/// bucketing convention used elsewhere in the workspace.
fn daily_return_series(windowed: &[&ClosedTrade]) -> Vec<Decimal> {
    let mut by_day: HashMap<i64, Decimal> = HashMap::new();
    for t in windowed {
        let cost = t.entry_price.0 * Decimal::from(t.contracts.0);
        let r = if cost.is_zero() {
            Decimal::ZERO
        } else {
            t.realized_pnl_usd / cost
        };
        let day = t.closed_at_unix.div_euclid(86_400);
        *by_day.entry(day).or_insert(Decimal::ZERO) += r;
    }
    by_day.into_values().collect()
}

/// Distribution moments of a return series (all `Decimal`, no `f64`).
struct Moments {
    mean: Decimal,
    std: Decimal,
    sharpe: Decimal,
    skewness: Decimal,
    excess_kurtosis: Decimal,
    lcb_5pct: Decimal,
}

/// Population moments of `series`. `std`/`sharpe`/`skewness`/`excess_kurtosis`
/// are `0` when the series has fewer than 2 points or zero dispersion (no
/// dispersion ⇒ those shape stats are undefined; `0` is the safe sentinel).
/// `lcb_5pct = mean − 1.645·(std/√n)` (one-sided 5th pct); equals `mean` when
/// `n < 2`. Higher powers use repeated multiplication (no `powi`); `sqrt` via
/// the `maths` feature.
fn compute_moments(series: &[Decimal]) -> Moments {
    let n = series.len();
    if n == 0 {
        return Moments {
            mean: Decimal::ZERO,
            std: Decimal::ZERO,
            sharpe: Decimal::ZERO,
            skewness: Decimal::ZERO,
            excess_kurtosis: Decimal::ZERO,
            lcb_5pct: Decimal::ZERO,
        };
    }
    let n_dec = Decimal::from(u64::try_from(n).unwrap_or(u64::MAX));
    let mean = series.iter().copied().sum::<Decimal>() / n_dec;

    if n < 2 {
        // Single point: no dispersion; LCB is the mean (stderr = 0).
        return Moments {
            mean,
            std: Decimal::ZERO,
            sharpe: Decimal::ZERO,
            skewness: Decimal::ZERO,
            excess_kurtosis: Decimal::ZERO,
            lcb_5pct: mean,
        };
    }

    let mut m2 = Decimal::ZERO;
    let mut m3 = Decimal::ZERO;
    let mut m4 = Decimal::ZERO;
    for &x in series {
        let d = x - mean;
        let d2 = d * d;
        m2 += d2;
        m3 += d2 * d;
        m4 += d2 * d2;
    }
    m2 /= n_dec; // variance (population)
    m3 /= n_dec;
    m4 /= n_dec;

    let std = m2.sqrt().unwrap_or(Decimal::ZERO);
    let (sharpe, skewness, excess_kurtosis) = if std.is_zero() {
        (Decimal::ZERO, Decimal::ZERO, Decimal::ZERO)
    } else {
        let std3 = std * std * std;
        let std4 = std3 * std;
        // std3/std4 can underflow to Decimal::ZERO when std is extremely small
        // (Decimal has 28-digit precision; std^3 for std≈1e-10 ≈ 1e-30, below the floor).
        // Treat underflow the same as zero dispersion: shape stats are undefined.
        if std3.is_zero() || std4.is_zero() {
            (mean / std, Decimal::ZERO, Decimal::ZERO)
        } else {
            (mean / std, m3 / std3, m4 / std4 - Decimal::from(3u32))
        }
    };

    // stderr = std / sqrt(n); LCB = mean − 1.645 · stderr.
    let stderr = std / n_dec.sqrt().unwrap_or(Decimal::ONE);
    let lcb_5pct = mean - dec_const_1_645() * stderr;

    Moments {
        mean,
        std,
        sharpe,
        skewness,
        excess_kurtosis,
        lcb_5pct,
    }
}

/// The one-sided 5th-percentile normal z-score, `1.645`, as an exact `Decimal`.
fn dec_const_1_645() -> Decimal {
    Decimal::new(1645, 3)
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

    #[test]
    fn distribution_moments_by_hand() {
        // Three distinct days; each trade cost = 0.50 × 100 = 50, so
        // return = pnl/50: pnl 5/10/15 → daily returns 0.1 / 0.2 / 0.3.
        let events = HashMap::new();
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(5.0), 60, 86_400),
            closed("0xm2", dec!(0.50), 100, dec!(10.0), 60, 172_800),
            closed("0xm3", dec!(0.50), 100, dec!(15.0), 60, 259_200),
        ]);
        let f = extract_features(&l, 10_000_000, &events, 1).unwrap();

        assert_eq!(f.trading_days, 3);
        // mean = 0.2 → 2000 bps (exact)
        assert_eq!(f.mean_daily_return_bps, 2_000);
        // symmetric series → skewness 0 (exact)
        assert_eq!(f.skewness_bps, 0);
        // m4/std^4 = 1.5 exactly → excess kurtosis -1.5 → -15000 bps (exact)
        assert_eq!(f.excess_kurtosis_bps, -15_000);
        // sqrt-dependent: std≈0.08165 (816 bps), sharpe≈2.449 (24495), lcb≈0.1225 (1225)
        assert!(
            (f.std_daily_return_bps - 816).abs() <= 2,
            "std={}",
            f.std_daily_return_bps
        );
        assert!(
            (f.sharpe_bps - 24_495).abs() <= 5,
            "sharpe={}",
            f.sharpe_bps
        );
        assert!(
            (f.lcb_5pct_bps - 1_225).abs() <= 3,
            "lcb={}",
            f.lcb_5pct_bps
        );
    }

    #[test]
    fn single_day_has_zero_dispersion_and_lcb_equals_mean() {
        // All trades on one UTC day → 1 daily-return point → no dispersion.
        let events = HashMap::new();
        let l = ledger(vec![
            closed("0xm1", dec!(0.50), 100, dec!(5.0), 60, 1_000),
            closed("0xm2", dec!(0.50), 100, dec!(5.0), 60, 2_000),
        ]);
        let f = extract_features(&l, 10_000, &events, 1).unwrap();
        assert_eq!(f.trading_days, 1);
        assert_eq!(f.std_daily_return_bps, 0);
        assert_eq!(f.sharpe_bps, 0);
        assert_eq!(f.skewness_bps, 0);
        assert_eq!(f.excess_kurtosis_bps, 0);
        // both trades same day: returns 0.1 + 0.1 = 0.2 → mean 2000 bps; lcb == mean
        assert_eq!(f.mean_daily_return_bps, 2_000);
        assert_eq!(f.lcb_5pct_bps, 2_000);
    }

    #[test]
    fn tiny_std_does_not_panic() {
        // Regression: when std is positive but tiny, std^3 underflows to Decimal::ZERO
        // (Decimal precision is 28 digits; std≈1e-10 → std^3≈1e-30, below the floor).
        // The fix: treat std3/std4 underflow as zero dispersion — skewness/kurtosis = 0.
        // Use many slightly-different returns across distinct days so std is non-zero but
        // extremely small (pnl differences of 1 sub-cent across 1000-contract positions).
        let events = HashMap::new();
        // 30 trades on 30 distinct days; entry 0.50, contracts 1_000_000, tiny pnl diffs.
        // daily return ≈ pnl / (0.50 × 1_000_000) = pnl / 500_000.
        // With pnl ranging 1e-7..1e-7 + 29e-9, daily returns ≈ 2e-13, std ≈ tiny.
        let trades: Vec<ClosedTrade> = (0u64..30)
            .map(|i| {
                let pnl = rust_decimal_macros::dec!(0.0000001) + Decimal::new(i as i64, 9); // adds i × 1e-9
                closed(
                    &format!("0xm{i}"),
                    dec!(0.50),
                    1_000_000,
                    pnl,
                    60,
                    (i as i64 + 1) * 86_400,
                )
            })
            .collect();
        let l = ledger(trades);
        // Must not panic. When std^3 underflows to Decimal::ZERO, skewness = 0.
        let f = extract_features(&l, i64::MAX, &events, 1).unwrap();
        assert_eq!(f.trading_days, 30);
        assert_eq!(f.skewness_bps, 0);
        assert_eq!(f.excess_kurtosis_bps, 0);
    }
}
