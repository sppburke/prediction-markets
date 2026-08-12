//! Shared executable-ask-ladder planner (#508 Phase A).
//!
//! One owner for the ladder math that both the isolated canary and the ordinary paper/live
//! copy path use: walk the ask ladder inside a price band, accumulate shares, and report the
//! exact spend (`estimated_ladder_spend`), the VWAP-consistent fill basis, the worst accepted
//! tick (`limit_price`), and the signed-FOK maximum (`worst_case_debit = shares × limit`).
//!
//! Two entry points share one exact walk:
//! - [`plan_exact_shares`] — fill exactly N shares or fail (`InsufficientDepth`); the
//!   all-or-nothing contract [`crate::canary_market::executable_ladder`] wraps (canary
//!   behavior must not drift — its wrapper keeps the staleness and minimum-order checks).
//! - [`plan_budget_buy`] — the budget-based planner: the largest WHOLE-share quantity whose
//!   ladder spend stays within `budget` (and, optionally, within `max_shares`), then the
//!   exact walk for that quantity. Zero affordable whole shares is the typed
//!   [`LadderError::NothingAffordable`] (the caller skips the trade; distinct from an
//!   unusable book, which the caller never hands to the planner).
//!
//! Band semantics (both entry points): a level priced below `minimum_price` fails closed
//! (`InsufficientDepth` — an executable below-band ask is evidence the fill would land below
//! the band); a level at/above `maximum_price_exclusive` or above the inclusive
//! `origin_ceiling` ends the walk. Levels must be sorted ascending by price with positive
//! sizes — [`plan_budget_buy`] callers filter zero-size dust before planning.

use pe_core_types::{CollateralAmount, Price, ShareAmount};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use crate::canary_market::AskLevel;

/// Atomic share units per whole share (`ShareAmount` fixed-point scale).
const ATOMICS_PER_SHARE: u64 = 1_000_000;

/// Shared ladder staleness bound (ms). The canary's reviewed 2 s guard, now the one canonical
/// bound for every ladder consumer: a book snapshot older than this (or from the future) must
/// not price an order.
pub const LADDER_MAX_AGE_MS: u64 = 2_000;

/// `true` when a book snapshot observed at `observed_ms` is unusable at `now_ms`: from the
/// future, or older than [`LADDER_MAX_AGE_MS`].
#[must_use]
pub fn ladder_is_stale(now_ms: u64, observed_ms: u64) -> bool {
    now_ms < observed_ms || now_ms - observed_ms > LADDER_MAX_AGE_MS
}

/// Typed planner failures. Every variant fails closed at the caller (no order).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LadderError {
    /// An executable ask sits below the band floor — the fill would land below band.
    #[error("ask ladder holds an executable level below the band floor")]
    BelowBandAsk,
    /// The in-band ladder cannot fill the requested share count.
    #[error("ask ladder cannot fill the request within its price bounds")]
    InsufficientDepth,
    /// The budget affords no whole share inside the band (or `max_shares` was zero).
    #[error("budget affords no whole share within the band")]
    NothingAffordable,
    /// Exact `Decimal`/amount arithmetic failed (implausible book magnitudes).
    #[error("exact amount arithmetic failed")]
    Amount,
}

/// One planned BUY over an ask ladder: the exact levels used, the price bounds, and the
/// canonical money figures (`_GLOSSARY.md` #508: `estimated_ladder_spend`,
/// `worst_case_debit`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LadderPlan {
    /// The ladder levels consumed, in ascending price order, with the used share amounts.
    pub used_asks: Vec<AskLevel>,
    /// Lowest used level — the fresh best ask actually crossed.
    pub best_ask: Price,
    /// Worst (highest) used level: the live FOK limit price.
    pub limit_price: Price,
    /// Planned share quantity.
    pub shares: ShareAmount,
    /// Σ(level price × used quantity) — the exact expected spend; `paper_fill_vwap =
    /// estimated_ladder_spend / shares`.
    pub estimated_ladder_spend: CollateralAmount,
    /// `shares × limit_price` — the signed-FOK maximum debit (≥ `estimated_ladder_spend`).
    pub worst_case_debit: CollateralAmount,
}

impl LadderPlan {
    /// Volume-weighted average fill price: `estimated_ladder_spend / shares`. `None` only
    /// when the resulting value is not a valid `Price` (degenerate zero-share plan — the
    /// constructors never produce one).
    #[must_use]
    pub fn vwap(&self) -> Option<Price> {
        let shares = self.shares.to_decimal();
        if shares <= Decimal::ZERO {
            return None;
        }
        Price::new(self.estimated_ladder_spend.to_decimal() / shares).ok()
    }
}

/// Fill exactly `requested_shares` from `asks` within the band, or fail. The extracted body
/// of the reviewed canary walk — [`crate::canary_market::executable_ladder`] wraps this with
/// its staleness and minimum-order checks, so canary behavior cannot drift.
pub fn plan_exact_shares(
    asks: &[AskLevel],
    requested_shares: ShareAmount,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    origin_ceiling: Price,
) -> Result<LadderPlan, LadderError> {
    let mut remaining = requested_shares.atomic();
    let mut used_asks = Vec::new();
    for level in asks {
        if level.price < minimum_price {
            return Err(LadderError::BelowBandAsk);
        }
        if level.price >= maximum_price_exclusive || level.price > origin_ceiling {
            break;
        }
        let used = remaining.min(level.shares.atomic());
        if used > 0 {
            used_asks.push(AskLevel {
                price: level.price,
                shares: ShareAmount::from_atomic(used),
            });
            remaining -= used;
        }
        if remaining == 0 {
            break;
        }
    }
    if remaining != 0 {
        return Err(LadderError::InsufficientDepth);
    }
    let best_ask = used_asks
        .first()
        .map(|level| level.price)
        .ok_or(LadderError::InsufficientDepth)?;
    let limit_price = used_asks
        .last()
        .map(|level| level.price)
        .ok_or(LadderError::InsufficientDepth)?;
    let mut spend = Decimal::ZERO;
    for level in &used_asks {
        let cost = level
            .shares
            .to_decimal()
            .checked_mul(level.price.0)
            .ok_or(LadderError::Amount)?;
        spend = spend.checked_add(cost).ok_or(LadderError::Amount)?;
    }
    let estimated_ladder_spend =
        CollateralAmount::from_decimal_exact(spend).map_err(|_| LadderError::Amount)?;
    let worst_case_debit =
        CollateralAmount::from_decimal_exact(requested_shares.to_decimal() * limit_price.0)
            .map_err(|_| LadderError::Amount)?;
    Ok(LadderPlan {
        used_asks,
        best_ask,
        limit_price,
        shares: requested_shares,
        estimated_ladder_spend,
        worst_case_debit,
    })
}

/// The budget-based planner (#508 Phase A): the largest whole-share quantity whose ladder
/// spend stays within `budget` — and within `max_shares` when supplied — planned over the
/// same band walk as [`plan_exact_shares`].
///
/// The affordability walk consumes atomic share units level-by-level while the exact spend
/// stays ≤ `budget`, floors the total to whole shares (paper/live contracts are whole), then
/// delegates to [`plan_exact_shares`] for the exact spend/limit of that quantity. Callers
/// pass a zero-size-filtered, ascending ladder; staleness is guarded separately via
/// [`ladder_is_stale`].
pub fn plan_budget_buy(
    asks: &[AskLevel],
    budget: CollateralAmount,
    max_shares: Option<u64>,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    origin_ceiling: Price,
) -> Result<LadderPlan, LadderError> {
    let mut share_cap_atomic = max_shares
        .unwrap_or(u64::MAX / ATOMICS_PER_SHARE)
        .saturating_mul(ATOMICS_PER_SHARE);
    let mut remaining_budget = budget.to_decimal();
    let mut affordable_atomic: u64 = 0;
    for level in asks {
        if share_cap_atomic == 0 || remaining_budget <= Decimal::ZERO {
            break;
        }
        if level.price < minimum_price {
            return Err(LadderError::BelowBandAsk);
        }
        if level.price >= maximum_price_exclusive || level.price > origin_ceiling {
            break;
        }
        // Whole-atomic affordability at this level's price (exact Decimal math, floor).
        let price = level.price.0;
        if price <= Decimal::ZERO {
            return Err(LadderError::Amount);
        }
        let afford = (remaining_budget / price)
            .checked_mul(Decimal::from(ATOMICS_PER_SHARE))
            .ok_or(LadderError::Amount)?
            .floor()
            .to_u64()
            .ok_or(LadderError::Amount)?;
        let take = level.shares.atomic().min(afford).min(share_cap_atomic);
        if take == 0 {
            break;
        }
        let cost = ShareAmount::from_atomic(take)
            .to_decimal()
            .checked_mul(price)
            .ok_or(LadderError::Amount)?;
        remaining_budget = remaining_budget
            .checked_sub(cost)
            .ok_or(LadderError::Amount)?;
        affordable_atomic = affordable_atomic
            .checked_add(take)
            .ok_or(LadderError::Amount)?;
        share_cap_atomic -= take;
        if take < level.shares.atomic() {
            break; // budget or share cap exhausted mid-level
        }
    }
    let whole_shares = affordable_atomic / ATOMICS_PER_SHARE;
    if whole_shares == 0 {
        return Err(LadderError::NothingAffordable);
    }
    plan_exact_shares(
        asks,
        ShareAmount::from_atomic(whole_shares * ATOMICS_PER_SHARE),
        minimum_price,
        maximum_price_exclusive,
        origin_ceiling,
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use rust_decimal_macros::dec;

    use super::*;

    fn level(price: Decimal, shares: Decimal) -> AskLevel {
        AskLevel {
            price: Price::new(price).unwrap(),
            shares: ShareAmount::from_decimal_exact(shares).unwrap(),
        }
    }

    fn wide_band() -> (Price, Price, Price) {
        (
            Price::ZERO,
            Price::new(dec!(1)).unwrap(),
            Price::new(dec!(1)).unwrap(),
        )
    }

    #[test]
    fn budget_planner_reproduces_the_500_to_230_downsize() {
        // The #508 canonical shape: a $500 budget against a band absorbing only $230.
        // Band: best 0.50, 100 bps ceiling = 0.505 (inclusive via origin_ceiling).
        // Levels: 300 @ 0.50 ($150) + 160 @ 0.50125 ($80.20) in-band; 1000 @ 0.60 outside.
        let asks = vec![
            level(dec!(0.50), dec!(300)),
            level(dec!(0.50125), dec!(160)),
            level(dec!(0.60), dec!(1000)),
        ];
        let plan = plan_budget_buy(
            &asks,
            CollateralAmount::from_decimal_exact(dec!(500)).unwrap(),
            None,
            Price::ZERO,
            Price::new(dec!(1)).unwrap(),
            Price::new(dec!(0.505)).unwrap(),
        )
        .unwrap();
        assert_eq!(plan.shares.to_decimal(), dec!(460));
        assert_eq!(plan.estimated_ladder_spend.to_decimal(), dec!(230.20));
        assert_eq!(plan.best_ask.0, dec!(0.50));
        assert_eq!(plan.limit_price.0, dec!(0.50125));
        // worst_case_debit = shares × limit ≥ estimated_ladder_spend.
        assert_eq!(plan.worst_case_debit.to_decimal(), dec!(230.575));
        assert!(plan.worst_case_debit >= plan.estimated_ladder_spend);
        // VWAP is the exact spend over shares (Decimal division, 28-digit precision).
        let vwap = plan.vwap().unwrap();
        assert_eq!(vwap.0, dec!(230.20) / dec!(460));
    }

    #[test]
    fn budget_planner_rounds_down_to_whole_shares() {
        // $1 at 0.30/share affords 3.33… shares → 3 whole shares, spend 0.90.
        let asks = vec![level(dec!(0.30), dec!(50))];
        let (min, max, ceil) = wide_band();
        let plan = plan_budget_buy(
            &asks,
            CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
            None,
            min,
            max,
            ceil,
        )
        .unwrap();
        assert_eq!(plan.shares.to_decimal(), dec!(3));
        assert_eq!(plan.estimated_ladder_spend.to_decimal(), dec!(0.90));
        assert_eq!(plan.worst_case_debit.to_decimal(), dec!(0.90));
    }

    #[test]
    fn budget_planner_vwap_spans_levels() {
        // $100 over 100 @ 0.40 + deep 0.404: buys 100 + 148 = 248 shares.
        // spend = 40 + 148×0.404 = 99.792; vwap = 99.792/248; limit = 0.404.
        let asks = vec![level(dec!(0.40), dec!(100)), level(dec!(0.404), dec!(1000))];
        let (min, max, _) = wide_band();
        let plan = plan_budget_buy(
            &asks,
            CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
            None,
            min,
            max,
            Price::new(dec!(0.404)).unwrap(),
        )
        .unwrap();
        assert_eq!(plan.shares.to_decimal(), dec!(248));
        assert_eq!(plan.estimated_ladder_spend.to_decimal(), dec!(99.792));
        assert_eq!(plan.limit_price.0, dec!(0.404));
        assert_eq!(plan.worst_case_debit.to_decimal(), dec!(100.192));
    }

    #[test]
    fn nothing_affordable_is_typed() {
        // Budget below one whole share.
        let asks = vec![level(dec!(0.50), dec!(10))];
        let (min, max, ceil) = wide_band();
        assert_eq!(
            plan_budget_buy(
                &asks,
                CollateralAmount::from_decimal_exact(dec!(0.49)).unwrap(),
                None,
                min,
                max,
                ceil,
            ),
            Err(LadderError::NothingAffordable)
        );
        // An empty in-band ladder (ceiling below every level) is likewise unaffordable.
        assert_eq!(
            plan_budget_buy(
                &asks,
                CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
                None,
                min,
                max,
                Price::new(dec!(0.40)).unwrap(),
            ),
            Err(LadderError::NothingAffordable)
        );
    }

    #[test]
    fn max_shares_caps_the_budget_walk() {
        let asks = vec![level(dec!(0.50), dec!(100))];
        let (min, max, ceil) = wide_band();
        let plan = plan_budget_buy(
            &asks,
            CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
            Some(7),
            min,
            max,
            ceil,
        )
        .unwrap();
        assert_eq!(plan.shares.to_decimal(), dec!(7));
        assert_eq!(
            plan_budget_buy(
                &asks,
                CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
                Some(0),
                min,
                max,
                ceil,
            ),
            Err(LadderError::NothingAffordable)
        );
    }

    #[test]
    fn below_band_executable_ask_fails_closed() {
        let asks = vec![level(dec!(0.04), dec!(1)), level(dec!(0.10), dec!(5))];
        assert_eq!(
            plan_budget_buy(
                &asks,
                CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                None,
                Price::new(dec!(0.05)).unwrap(),
                Price::new(dec!(0.99)).unwrap(),
                Price::new(dec!(0.10)).unwrap(),
            ),
            Err(LadderError::BelowBandAsk)
        );
    }

    #[test]
    fn staleness_guard_bounds_both_directions() {
        assert!(!ladder_is_stale(2_000, 1_000));
        assert!(!ladder_is_stale(3_000, 1_000));
        assert!(ladder_is_stale(3_001, 1_000), "older than 2 s is stale");
        assert!(ladder_is_stale(999, 1_000), "future books are stale");
    }
}
