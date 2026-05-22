//! Forward-test harness (issue #212): the held-out validation of a selection.
//!
//! For each selected wallet, take its **post-cutoff buys** and **hold each to
//! market resolution** (copy-and-hold semantics, mirroring the backtest's
//! resolution sweep `crates/backtest/src/simulation.rs`): a buy of `outcome` in
//! `market` at price `c` resolves to `close = 1` if `winning_outcome_id ==
//! outcome` else `0`. Two PnL series accumulate per resolved position on a $1
//! unit:
//!   - **flat-$1** (headline): `(close − c) / c` — $1 staked at `c`.
//!   - **Kelly-f** (secondary): stake `f · max(0, (p − c)/(1 − c))` of $1, where
//!     `p` is the wallet's **≤cutoff** resolution-win-rate in `c`'s entry-price
//!     bucket (the ex-ante calibration prior, no look-ahead). A bucket with
//!     fewer than `min_bucket_trades` ≤cutoff resolved buys is too sparse for a
//!     reliable `p` → that position takes the **neutral base stake `f` of $1**
//!     (flagged), not a full $1. (A full-$1 fallback gave uncalibrated positions
//!     10× the stake of calibrated ones — `f·edge ≤ f` — letting the sparse set
//!     dominate the Kelly book; staking `f` keeps every position on one scale.)
//!
//! **Fees:** the April-2026 holdout is entirely post-fee (Polymarket fees since
//! 2026-03-30; Akey et al. SSRN 6443103). v1 reports PnL **gross of fees** —
//! `ForwardReport::gross_of_fees == true`; the headline overstates net edge by
//! ≈ the taker fee. Netting awaits a per-market `takerBaseFee` backfill.
//!
//! Positions with no resolution, or whose market resolved before the buy
//! (data anomaly), are excluded. All arithmetic is `rust_decimal` (no `f64`).

use std::collections::HashMap;

use pe_bootstrap::cache::{ResolutionIndex, WalletCache};
use pe_core_types::Side;
use pe_trader_index::RawTrade;
use rust_decimal::Decimal;
use tracing::info;

use crate::error::SkillSelectError;

/// Aggregate forward-test result over the selected wallets.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForwardReport {
    /// Selected wallets evaluated.
    pub wallets: usize,
    /// Post-cutoff buys that resolved and were scored.
    pub resolved_positions: u32,
    /// Post-cutoff buys excluded (no resolution, or resolved-before-bought).
    pub excluded_positions: u32,
    /// Resolved positions that used the neutral base-`f` Kelly fallback (sparse bucket).
    pub kelly_fallback_positions: u32,
    /// Total flat-$1 PnL across all resolved positions (USD).
    pub flat_pnl_usd: Decimal,
    /// Total Kelly-f PnL across all resolved positions (USD).
    pub kelly_pnl_usd: Decimal,
    /// Always `true` in v1 — PnL is gross of taker fees (see module docs).
    pub gross_of_fees: bool,
}

/// Per-wallet forward PnL (the per-wallet half of [`ForwardReport`]).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForwardPnl {
    pub resolved_positions: u32,
    pub excluded_positions: u32,
    pub kelly_fallback_positions: u32,
    pub flat_pnl_usd: Decimal,
    pub kelly_pnl_usd: Decimal,
}

/// Compute the forward PnL for one wallet's trades, split at `cutoff_unix`.
///
/// `trades` is the wallet's full trade history (any order); only **buys** are
/// held to resolution. `kelly_fraction`, `bucket_width`, and `min_bucket_trades`
/// are the sizing/calibration parameters. Resolution-win is `winning_outcome_id
/// == bought outcome`; unresolved or resolved-before-bought buys are excluded.
///
/// # Precondition
/// `bucket_width` must be `> 0` (price-bucket granularity); `0 < c < 1` buys are
/// the scorable set — a buy at `c == 0` is excluded (cannot price), and `c >= 1`
/// gets no Kelly stake (no upside) but still contributes flat PnL.
pub fn forward_pnl_for_wallet(
    trades: &[RawTrade],
    cutoff_unix: i64,
    resolutions: &ResolutionIndex,
    kelly_fraction: Decimal,
    bucket_width: Decimal,
    min_bucket_trades: u32,
) -> ForwardPnl {
    // ≤cutoff calibration prior: resolution-win-rate per entry-price bucket.
    // bucket -> (wins, total) over resolved ≤cutoff buys.
    let mut buckets: HashMap<u64, (u64, u64)> = HashMap::new();
    for t in trades
        .iter()
        .filter(|t| t.side == Side::Buy && t.timestamp.0.unix_timestamp() <= cutoff_unix)
    {
        if let Some(won) = resolution_outcome(t, resolutions) {
            let key = bucket_key(t.price.0, bucket_width);
            let entry = buckets.entry(key).or_insert((0, 0));
            entry.1 += 1;
            if won {
                entry.0 += 1;
            }
        }
    }

    let mut pnl = ForwardPnl::default();
    for t in trades
        .iter()
        .filter(|t| t.side == Side::Buy && t.timestamp.0.unix_timestamp() > cutoff_unix)
    {
        let c = t.price.0;
        let Some(won) = resolution_outcome(t, resolutions) else {
            pnl.excluded_positions += 1;
            continue;
        };
        if c.is_zero() {
            // Cannot price a zero-cost buy — exclude rather than divide by zero.
            pnl.excluded_positions += 1;
            continue;
        }
        let close = if won { Decimal::ONE } else { Decimal::ZERO };
        // Flat-$1 return on a position staked at c: (close - c) / c.
        let unit_return = (close - c) / c;
        pnl.flat_pnl_usd += unit_return;

        // Kelly stake fraction of $1: f * max(0, (p - c)/(1 - c)); sparse → flat $1.
        let key = bucket_key(c, bucket_width);
        let kelly_stake = match buckets.get(&key) {
            Some(&(wins, total)) if total >= u64::from(min_bucket_trades) && c < Decimal::ONE => {
                let p = Decimal::from(wins) / Decimal::from(total);
                let edge = (p - c) / (Decimal::ONE - c);
                if edge > Decimal::ZERO {
                    kelly_fraction * edge
                } else {
                    Decimal::ZERO
                }
            }
            _ => {
                // Sparse bucket (or unpriceable c): no edge estimate → take the
                // neutral base stake `f` (same scale as calibrated `f·edge`).
                pnl.kelly_fallback_positions += 1;
                kelly_fraction
            }
        };
        pnl.kelly_pnl_usd += kelly_stake * unit_return;
        pnl.resolved_positions += 1;
    }
    pnl
}

/// Resolution outcome for a buy: `Some(true)` if the bought outcome won,
/// `Some(false)` if it lost, `None` if unresolved or the market resolved before
/// the buy (data anomaly — excluded, mirroring the backtest sweep guard).
fn resolution_outcome(t: &RawTrade, resolutions: &ResolutionIndex) -> Option<bool> {
    let res = resolutions.get(&t.market_id)?;
    if res.resolved_at_unix < t.timestamp.0.unix_timestamp() {
        return None;
    }
    Some(res.winning_outcome_id == t.outcome_id)
}

/// Entry-price bucket index: `floor(price / width)`. Saturates to `0` on a
/// non-positive width (the precondition forbids that).
fn bucket_key(price: Decimal, width: Decimal) -> u64 {
    if width <= Decimal::ZERO {
        return 0;
    }
    use rust_decimal::prelude::ToPrimitive;
    (price / width).floor().to_u64().unwrap_or(0)
}

/// Run the forward test over a set of selected wallets, reading their trades
/// from the bootstrap cache (read-only) and resolving against `load_all_resolutions`.
///
/// `kelly_fraction` is `f` (e.g. `0.10`); `bucket_width`/`min_bucket_trades`
/// govern the calibration prior. Returns the aggregate [`ForwardReport`].
pub fn run_forward_test(
    cache_path: &std::path::Path,
    selected: &[String],
    cutoff_unix: i64,
    kelly_fraction: Decimal,
    bucket_width: Decimal,
    min_bucket_trades: u32,
) -> Result<ForwardReport, SkillSelectError> {
    let cache = WalletCache::open_read_only(cache_path)?;
    let resolutions = cache.load_all_resolutions()?;

    let mut report = ForwardReport {
        wallets: selected.len(),
        gross_of_fees: true,
        ..ForwardReport::default()
    };
    for hex in selected {
        let trades = cache.trades_for(hex);
        let w = forward_pnl_for_wallet(
            &trades,
            cutoff_unix,
            &resolutions,
            kelly_fraction,
            bucket_width,
            min_bucket_trades,
        );
        report.resolved_positions += w.resolved_positions;
        report.excluded_positions += w.excluded_positions;
        report.kelly_fallback_positions += w.kelly_fallback_positions;
        report.flat_pnl_usd += w.flat_pnl_usd;
        report.kelly_pnl_usd += w.kelly_pnl_usd;
    }

    info!(
        wallets = report.wallets,
        resolved = report.resolved_positions,
        excluded = report.excluded_positions,
        kelly_fallback = report.kelly_fallback_positions,
        flat_pnl_usd = %report.flat_pnl_usd,
        kelly_pnl_usd = %report.kelly_pnl_usd,
        gross_of_fees = report.gross_of_fees,
        "skill-select forward-test: complete (PnL is GROSS of taker fees)"
    );
    Ok(report)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_bootstrap::cache::MarketResolution;
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, SourceTimestamp, SourceTradeId, VenueMarketId,
        WalletAddress,
    };
    use rust_decimal_macros::dec;
    use time::OffsetDateTime;

    fn buy(market: &str, outcome: u16, price: Decimal, ts: i64) -> RawTrade {
        RawTrade {
            wallet: WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            market_id: MarketId(VenueMarketId(market.to_owned())),
            outcome_id: OutcomeId(outcome),
            side: Side::Buy,
            price: Price::new(price).unwrap(),
            contracts: ContractQty(100),
            timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
            source_trade_id: SourceTradeId(format!("{market}-{outcome}-{ts}")),
        }
    }

    fn res(market: &str, winner: u16, resolved_at: i64) -> (MarketId, MarketResolution) {
        (
            MarketId(VenueMarketId(market.to_owned())),
            MarketResolution {
                winning_outcome_id: OutcomeId(winner),
                resolved_at_unix: resolved_at,
            },
        )
    }

    #[test]
    fn flat_pnl_win_and_loss_by_hand() {
        // post-cutoff buy at 0.50 that wins → (1-0.5)/0.5 = +1.0; one at 0.50 that
        // loses → (0-0.5)/0.5 = -1.0. Net flat = 0. No ≤cutoff history → all Kelly
        // positions take the base-f fallback; the net still nets to 0 here.
        let trades = vec![
            buy("0xwin", 0, dec!(0.50), 2_000),
            buy("0xlose", 0, dec!(0.50), 2_000),
        ];
        let resolutions: ResolutionIndex = [res("0xwin", 0, 3_000), res("0xlose", 1, 3_000)]
            .into_iter()
            .collect();
        let p = forward_pnl_for_wallet(&trades, 1_000, &resolutions, dec!(0.10), dec!(0.10), 5);
        assert_eq!(p.resolved_positions, 2);
        assert_eq!(p.flat_pnl_usd, dec!(0.0));
        assert_eq!(p.kelly_fallback_positions, 2); // no ≤cutoff calibration data
        assert_eq!(p.kelly_pnl_usd, dec!(0.0)); // f·(+1) + f·(-1) = 0
    }

    #[test]
    fn sparse_fallback_stakes_base_f_not_full_dollar() {
        // One post-cutoff winning buy at 0.50, no ≤cutoff history → sparse fallback.
        // flat = +1.0; Kelly stakes the base fraction f=0.10 → kelly = 0.10·1.0 = 0.10
        // (the old behaviour staked a full $1 → would have been 1.0).
        let trades = vec![buy("0xwin", 0, dec!(0.50), 2_000)];
        let resolutions: ResolutionIndex = [res("0xwin", 0, 3_000)].into_iter().collect();
        let p = forward_pnl_for_wallet(&trades, 1_000, &resolutions, dec!(0.10), dec!(0.10), 5);
        assert_eq!(p.resolved_positions, 1);
        assert_eq!(p.kelly_fallback_positions, 1);
        assert_eq!(p.flat_pnl_usd, dec!(1.0));
        assert_eq!(p.kelly_pnl_usd, dec!(0.10)); // base-f, not full $1
    }

    #[test]
    fn unresolved_and_anomalous_positions_excluded() {
        let trades = vec![
            buy("0xnone", 0, dec!(0.40), 2_000),  // no resolution
            buy("0xearly", 0, dec!(0.40), 5_000), // resolved before bought
            buy("0xok", 0, dec!(0.40), 2_000),    // good
        ];
        let resolutions: ResolutionIndex = [
            res("0xearly", 0, 4_000), // resolved_at 4000 < bought 5000
            res("0xok", 0, 6_000),
        ]
        .into_iter()
        .collect();
        let p = forward_pnl_for_wallet(&trades, 1_000, &resolutions, dec!(0.10), dec!(0.10), 5);
        assert_eq!(p.resolved_positions, 1);
        assert_eq!(p.excluded_positions, 2);
    }

    #[test]
    fn kelly_uses_precutoff_bucket_winrate() {
        // Calibration prior: 6 ≤cutoff buys at ~0.50 (bucket 5), 5 win → p=5/6≈0.833.
        // Post-cutoff buy at 0.50 wins: flat = +1.0; kelly stake = f·(p-c)/(1-c)
        //   = 0.10·(0.8333-0.5)/0.5 = 0.10·0.6667 = 0.066667; kelly pnl = stake·1.0.
        let mut trades = Vec::new();
        for i in 0..6 {
            // 5 wins (outcome 0), 1 loss (outcome 0 but winner 1)
            trades.push(buy(&format!("0xc{i}"), 0, dec!(0.50), 100));
        }
        trades.push(buy("0xfwd", 0, dec!(0.50), 2_000));
        let mut resolutions: ResolutionIndex = (0..6)
            .map(|i| res(&format!("0xc{i}"), if i < 5 { 0 } else { 1 }, 500))
            .collect();
        let (k, v) = res("0xfwd", 0, 3_000);
        resolutions.insert(k, v);

        let p = forward_pnl_for_wallet(&trades, 1_000, &resolutions, dec!(0.10), dec!(0.10), 5);
        assert_eq!(p.resolved_positions, 1);
        assert_eq!(p.kelly_fallback_positions, 0); // bucket has 6 ≥ min 5
        assert_eq!(p.flat_pnl_usd, dec!(1.0));
        // 0.10 * ((5/6 - 0.5)/0.5) ; 5/6 = 0.8333..., edge = 0.6666..., *0.10 = 0.06666...
        let expected = dec!(0.10) * ((dec!(5) / dec!(6) - dec!(0.5)) / dec!(0.5));
        assert_eq!(p.kelly_pnl_usd, expected);
    }

    #[test]
    fn no_kelly_stake_when_prior_below_price() {
        // Bucket win-rate 1/6 ≈ 0.167 < price 0.50 → no edge → kelly stake 0.
        let mut trades = Vec::new();
        for i in 0..6 {
            trades.push(buy(&format!("0xc{i}"), 0, dec!(0.50), 100));
        }
        trades.push(buy("0xfwd", 0, dec!(0.50), 2_000));
        let mut resolutions: ResolutionIndex = (0..6)
            .map(|i| res(&format!("0xc{i}"), if i < 1 { 0 } else { 1 }, 500))
            .collect();
        let (k, v) = res("0xfwd", 0, 3_000);
        resolutions.insert(k, v);
        let p = forward_pnl_for_wallet(&trades, 1_000, &resolutions, dec!(0.10), dec!(0.10), 5);
        assert_eq!(p.kelly_fallback_positions, 0);
        assert_eq!(p.kelly_pnl_usd, dec!(0.0)); // edge ≤ 0 → no stake
        assert_eq!(p.flat_pnl_usd, dec!(1.0)); // flat still books the win
    }
}
