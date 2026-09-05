//! Shared collateral-path executable-ask planner (#545).
//!
//! Ordinary BUYs sign an exact collateral principal. The expected paper quantity is the sum of
//! improved ask quantities, while `shares` is the minimum taker quantity signed at the worst
//! accepted tick. The isolated canary keeps its exact-share behavior through
//! [`plan_exact_shares`], which uses the same private ask walk.

use pe_core_types::{CollateralAmount, Price, ShareAmount};
use rust_decimal::{Decimal, RoundingStrategy};

use crate::canary_market::AskLevel;
use crate::fee::{CompactFeeSchedule, FeeError, fee_reserve, principal_for_budget};

const ATOMICS_PER_SHARE: u64 = 1_000_000;

pub const LADDER_MAX_AGE_MS: u64 = 2_000;

#[must_use]
pub fn ladder_is_stale(now_ms: u64, observed_ms: u64) -> bool {
    now_ms < observed_ms || now_ms - observed_ms > LADDER_MAX_AGE_MS
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LadderError {
    #[error("ask ladder holds an executable level below the band floor")]
    BelowBandAsk,
    #[error("ask ladder cannot fill the request within its price bounds")]
    InsufficientDepth,
    #[error("budget affords no atomic share within the band")]
    NothingAffordable,
    #[error("signed minimum shares are below the venue minimum")]
    BelowMinimum,
    #[error("principal plus fee reserve exceeds a monetary cap")]
    CapExceeded,
    #[error("fee calculation failed: {0}")]
    Fee(#[from] FeeError),
    #[error("exact amount arithmetic failed")]
    Amount,
}

/// One collateral-path BUY plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LadderPlan {
    /// Expected improved quantities at each crossed ask.
    pub used_asks: Vec<AskLevel>,
    pub best_ask: Price,
    /// Worst accepted tick and therefore the signed price.
    pub limit_price: Price,
    /// Signed minimum taker quantity, `floor_atomic(principal / limit_price)`.
    pub shares: ShareAmount,
    /// Exact signed maker principal.
    pub worst_case_debit: CollateralAmount,
}

impl LadderPlan {
    pub fn expected_shares(&self) -> Result<ShareAmount, LadderError> {
        self.used_asks
            .iter()
            .try_fold(ShareAmount::ZERO, |total, ask| {
                total
                    .checked_add(ask.shares)
                    .map_err(|_| LadderError::Amount)
            })
    }

    pub fn expected_spend(&self) -> Result<CollateralAmount, LadderError> {
        collateral_floor(expected_spend_decimal(&self.used_asks)?)
    }

    #[must_use]
    pub fn vwap(&self) -> Option<Price> {
        let shares = self.expected_shares().ok()?.to_decimal();
        if shares <= Decimal::ZERO {
            return None;
        }
        let spend = self.expected_spend().ok()?.to_decimal();
        Price::new(spend.checked_div(shares)?).ok()
    }
}

/// Requested sizing before the common ladder and cap checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuySizing {
    Dollar { budget: CollateralAmount },
    Contract { contracts: u64 },
    Kelly { contracts: u64 },
}

/// A plan together with the exact conservative fee reserve used by every monetary cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SizedBuyPlan {
    pub ladder: LadderPlan,
    pub budget: CollateralAmount,
    pub reserve: CollateralAmount,
}

impl SizedBuyPlan {
    pub fn worst_case_all_in_debit(&self) -> Result<CollateralAmount, LadderError> {
        self.ladder
            .worst_case_debit
            .checked_add(self.reserve)
            .map_err(|_| LadderError::Amount)
    }
}

/// Plan Dollar, Contract, or Kelly output through one collateral ladder and cap owner.
#[allow(clippy::too_many_arguments)]
pub fn plan_sized_buy(
    asks: &[AskLevel],
    schedule: CompactFeeSchedule,
    sizing: BuySizing,
    monetary_caps: &[CollateralAmount],
    minimum_order_size: ShareAmount,
    minimum_tick_size: Price,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    chase_ceiling: Price,
    impact_ceiling: Price,
) -> Result<SizedBuyPlan, LadderError> {
    let signed_share_scale = minimum_tick_size
        .0
        .normalize()
        .scale()
        .checked_add(2)
        .filter(|scale| *scale <= 6)
        .ok_or(LadderError::Amount)?;
    if minimum_tick_size == Price::ZERO {
        return Err(LadderError::Amount);
    }
    let ceiling = chase_ceiling.min(impact_ceiling);
    let best_ask = first_eligible_ask(asks, minimum_price, maximum_price_exclusive, ceiling)?;
    let smallest_cap = monetary_caps
        .iter()
        .copied()
        .min()
        .unwrap_or(CollateralAmount::from_atomic(u64::MAX));

    let (budget, ladder) = match sizing {
        BuySizing::Dollar { budget } => {
            let budget = budget.min(smallest_cap);
            let principal = principal_for_budget(schedule, budget, best_ask, ceiling)?;
            let ladder = plan_principal_buy_with_scale(
                asks,
                principal,
                signed_share_scale,
                minimum_price,
                maximum_price_exclusive,
                chase_ceiling,
                impact_ceiling,
            )
            .map_err(|error| {
                if error == LadderError::NothingAffordable
                    && minimum_order_size != ShareAmount::ZERO
                {
                    LadderError::BelowMinimum
                } else {
                    error
                }
            })?;
            (budget, ladder)
        }
        BuySizing::Contract { contracts } | BuySizing::Kelly { contracts } => {
            let requested = whole_shares(contracts)?;
            if requested < minimum_order_size {
                return Err(LadderError::BelowMinimum);
            }
            let exact = plan_exact_shares(
                asks,
                requested,
                minimum_price,
                maximum_price_exclusive,
                ceiling,
            )?;
            let ladder = plan_principal_buy_with_scale(
                asks,
                exact.worst_case_debit,
                signed_share_scale,
                minimum_price,
                maximum_price_exclusive,
                chase_ceiling,
                impact_ceiling,
            )?;
            (smallest_cap, ladder)
        }
    };

    let reserve = fee_reserve(
        schedule,
        ladder.worst_case_debit,
        ladder.shares,
        ladder.best_ask,
        ladder.limit_price,
    )?;
    let all_in = ladder
        .worst_case_debit
        .checked_add(reserve)
        .map_err(|_| LadderError::Amount)?;

    if all_in > budget || monetary_caps.iter().any(|cap| all_in > *cap) {
        return Err(LadderError::CapExceeded);
    }
    if ladder.shares < minimum_order_size {
        return Err(LadderError::BelowMinimum);
    }
    Ok(SizedBuyPlan {
        ladder,
        budget,
        reserve,
    })
}

/// Walk the full signed principal across asks. The signed minimum quantity is determined only by
/// the worst accepted tick; improved quantities remain in `used_asks` for paper/replay.
pub fn plan_principal_buy(
    asks: &[AskLevel],
    principal: CollateralAmount,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    chase_ceiling: Price,
    impact_ceiling: Price,
) -> Result<LadderPlan, LadderError> {
    plan_principal_buy_with_scale(
        asks,
        principal,
        6,
        minimum_price,
        maximum_price_exclusive,
        chase_ceiling,
        impact_ceiling,
    )
}

fn plan_principal_buy_with_scale(
    asks: &[AskLevel],
    principal: CollateralAmount,
    signed_share_scale: u32,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    chase_ceiling: Price,
    impact_ceiling: Price,
) -> Result<LadderPlan, LadderError> {
    if principal == CollateralAmount::ZERO {
        return Err(LadderError::NothingAffordable);
    }
    let ceiling = chase_ceiling.min(impact_ceiling);
    let walked = walk_principal(
        asks,
        principal,
        minimum_price,
        maximum_price_exclusive,
        ceiling,
    )?;
    let shares = shares_for_principal(principal, walked.limit_price, signed_share_scale)?;
    if shares == ShareAmount::ZERO {
        return Err(LadderError::NothingAffordable);
    }
    Ok(LadderPlan {
        used_asks: walked.used_asks,
        best_ask: walked.best_ask,
        limit_price: walked.limit_price,
        shares,
        worst_case_debit: principal,
    })
}

/// Exact-share compatibility adapter used by the isolated canary.
pub fn plan_exact_shares(
    asks: &[AskLevel],
    requested_shares: ShareAmount,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    origin_ceiling: Price,
) -> Result<LadderPlan, LadderError> {
    if requested_shares == ShareAmount::ZERO {
        return Err(LadderError::NothingAffordable);
    }
    let used_asks = walk_shares(
        asks,
        requested_shares,
        minimum_price,
        maximum_price_exclusive,
        origin_ceiling,
    )?;
    let best_ask = used_asks
        .first()
        .map(|ask| ask.price)
        .ok_or(LadderError::InsufficientDepth)?;
    let limit_price = used_asks
        .last()
        .map(|ask| ask.price)
        .ok_or(LadderError::InsufficientDepth)?;
    let principal = CollateralAmount::from_decimal_exact(
        requested_shares
            .to_decimal()
            .checked_mul(limit_price.0)
            .ok_or(LadderError::Amount)?,
    )
    .map_err(|_| LadderError::Amount)?;
    Ok(LadderPlan {
        used_asks,
        best_ask,
        limit_price,
        shares: requested_shares,
        worst_case_debit: principal,
    })
}

/// Backward-compatible budget entry point. `budget` is now the exact signed principal.
pub fn plan_budget_buy(
    asks: &[AskLevel],
    budget: CollateralAmount,
    max_shares: Option<u64>,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    origin_ceiling: Price,
) -> Result<LadderPlan, LadderError> {
    if let Some(maximum) = max_shares {
        let capped = plan_exact_shares(
            asks,
            whole_shares(maximum)?,
            minimum_price,
            maximum_price_exclusive,
            origin_ceiling,
        )?;
        if capped.worst_case_debit <= budget {
            return Ok(capped);
        }
    }
    plan_principal_buy(
        asks,
        budget,
        minimum_price,
        maximum_price_exclusive,
        origin_ceiling,
        origin_ceiling,
    )
}

struct PrincipalWalk {
    used_asks: Vec<AskLevel>,
    best_ask: Price,
    limit_price: Price,
}

fn walk_principal(
    asks: &[AskLevel],
    principal: CollateralAmount,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    ceiling: Price,
) -> Result<PrincipalWalk, LadderError> {
    let mut remaining = principal.to_decimal();
    let mut used_asks = Vec::new();
    let mut in_band_capacity = Decimal::ZERO;
    let mut best_ask = None;
    let mut limit_price = None;
    for level in asks {
        validate_level_order(&used_asks, level)?;
        if level.price < minimum_price {
            return Err(LadderError::BelowBandAsk);
        }
        if level.price >= maximum_price_exclusive || level.price > ceiling {
            break;
        }
        if level.price == Price::ZERO || level.shares == ShareAmount::ZERO {
            return Err(LadderError::Amount);
        }
        best_ask.get_or_insert(level.price);
        limit_price = Some(level.price);
        let level_cost = level
            .shares
            .to_decimal()
            .checked_mul(level.price.0)
            .ok_or(LadderError::Amount)?;
        in_band_capacity = in_band_capacity
            .checked_add(level_cost)
            .ok_or(LadderError::Amount)?;
        if remaining >= level_cost {
            used_asks.push(level.clone());
            remaining = remaining
                .checked_sub(level_cost)
                .ok_or(LadderError::Amount)?;
            if remaining == Decimal::ZERO {
                break;
            }
            continue;
        }
        let shares = decimal_to_atomic_shares(
            remaining
                .checked_div(level.price.0)
                .ok_or(LadderError::Amount)?,
        )?;
        if shares != ShareAmount::ZERO {
            used_asks.push(AskLevel {
                price: level.price,
                shares,
            });
        }
        break;
    }
    if in_band_capacity < principal.to_decimal() {
        return Err(LadderError::InsufficientDepth);
    }
    let best_ask = best_ask.ok_or(LadderError::NothingAffordable)?;
    let limit_price = limit_price.ok_or(LadderError::NothingAffordable)?;
    Ok(PrincipalWalk {
        used_asks,
        best_ask,
        limit_price,
    })
}

fn walk_shares(
    asks: &[AskLevel],
    requested_shares: ShareAmount,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    ceiling: Price,
) -> Result<Vec<AskLevel>, LadderError> {
    let mut remaining = requested_shares.atomic();
    let mut used_asks = Vec::new();
    for level in asks {
        validate_level_order(&used_asks, level)?;
        if level.price < minimum_price {
            return Err(LadderError::BelowBandAsk);
        }
        if level.price >= maximum_price_exclusive || level.price > ceiling {
            break;
        }
        if level.price == Price::ZERO || level.shares == ShareAmount::ZERO {
            return Err(LadderError::Amount);
        }
        let used = remaining.min(level.shares.atomic());
        if used != 0 {
            used_asks.push(AskLevel {
                price: level.price,
                shares: ShareAmount::from_atomic(used),
            });
            remaining = remaining.checked_sub(used).ok_or(LadderError::Amount)?;
        }
        if remaining == 0 {
            return Ok(used_asks);
        }
    }
    Err(LadderError::InsufficientDepth)
}

fn validate_level_order(used: &[AskLevel], next: &AskLevel) -> Result<(), LadderError> {
    if used.last().is_some_and(|prior| next.price < prior.price) {
        return Err(LadderError::Amount);
    }
    Ok(())
}

fn first_eligible_ask(
    asks: &[AskLevel],
    minimum_price: Price,
    maximum_price_exclusive: Price,
    ceiling: Price,
) -> Result<Price, LadderError> {
    let Some(first) = asks.first() else {
        return Err(LadderError::InsufficientDepth);
    };
    if first.price < minimum_price {
        return Err(LadderError::BelowBandAsk);
    }
    if first.price >= maximum_price_exclusive || first.price > ceiling {
        return Err(LadderError::InsufficientDepth);
    }
    Ok(first.price)
}

fn shares_for_principal(
    principal: CollateralAmount,
    price: Price,
    scale: u32,
) -> Result<ShareAmount, LadderError> {
    if price == Price::ZERO {
        return Err(LadderError::Amount);
    }
    ShareAmount::from_decimal_exact(
        principal
            .to_decimal()
            .checked_div(price.0)
            .ok_or(LadderError::Amount)?
            .round_dp_with_strategy(scale, RoundingStrategy::ToNegativeInfinity),
    )
    .map_err(|_| LadderError::Amount)
}

fn decimal_to_atomic_shares(value: Decimal) -> Result<ShareAmount, LadderError> {
    ShareAmount::from_decimal_exact(
        value.round_dp_with_strategy(6, RoundingStrategy::ToNegativeInfinity),
    )
    .map_err(|_| LadderError::Amount)
}

fn collateral_floor(value: Decimal) -> Result<CollateralAmount, LadderError> {
    CollateralAmount::from_decimal_exact(
        value.round_dp_with_strategy(6, RoundingStrategy::ToNegativeInfinity),
    )
    .map_err(|_| LadderError::Amount)
}

fn whole_shares(value: u64) -> Result<ShareAmount, LadderError> {
    value
        .checked_mul(ATOMICS_PER_SHARE)
        .map(ShareAmount::from_atomic)
        .ok_or(LadderError::Amount)
}

fn expected_spend_decimal(asks: &[AskLevel]) -> Result<Decimal, LadderError> {
    asks.iter().try_fold(Decimal::ZERO, |total, ask| {
        ask.shares
            .to_decimal()
            .checked_mul(ask.price.0)
            .and_then(|cost| total.checked_add(cost))
            .ok_or(LadderError::Amount)
    })
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

    fn price(value: Decimal) -> Price {
        Price::new(value).unwrap()
    }

    #[test]
    fn collateral_walk_separates_signed_minimum_from_improved_quantity() {
        let asks = vec![level(dec!(0.40), dec!(1)), level(dec!(0.50), dec!(10))];
        let plan = plan_principal_buy(
            &asks,
            CollateralAmount::from_decimal_exact(dec!(1.40)).unwrap(),
            price(dec!(0.05)),
            price(dec!(0.99)),
            price(dec!(0.60)),
            price(dec!(0.50)),
        )
        .unwrap();
        assert_eq!(
            plan.used_asks,
            vec![level(dec!(0.40), dec!(1)), level(dec!(0.50), dec!(2))]
        );
        assert_eq!(plan.shares.to_decimal(), dec!(2.8));
        assert_eq!(plan.expected_shares().unwrap().to_decimal(), dec!(3));
        assert_eq!(plan.expected_spend().unwrap().to_decimal(), dec!(1.4));
        assert_eq!(plan.vwap().unwrap().0, dec!(1.4) / dec!(3));
        assert_eq!(plan.worst_case_debit.to_decimal(), dec!(1.4));
    }

    #[test]
    fn exact_share_adapter_preserves_canary_output() {
        let asks = vec![level(dec!(0.10), dec!(2)), level(dec!(0.11), dec!(10))];
        let plan = plan_exact_shares(
            &asks,
            ShareAmount::from_decimal_exact(dec!(5)).unwrap(),
            price(dec!(0.05)),
            price(dec!(0.99)),
            price(dec!(0.11)),
        )
        .unwrap();
        assert_eq!(
            plan.used_asks,
            vec![level(dec!(0.10), dec!(2)), level(dec!(0.11), dec!(3))]
        );
        assert_eq!(plan.shares.to_decimal(), dec!(5));
        assert_eq!(plan.worst_case_debit.to_decimal(), dec!(0.55));
    }

    #[test]
    fn lesser_chase_impact_and_band_bounds_are_independent() {
        let asks = vec![level(dec!(0.40), dec!(1)), level(dec!(0.41), dec!(10))];
        let principal = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();
        assert_eq!(
            plan_principal_buy(
                &asks,
                principal,
                price(dec!(0.41)),
                price(dec!(0.99)),
                price(dec!(0.50)),
                price(dec!(0.50))
            ),
            Err(LadderError::BelowBandAsk)
        );
        assert_eq!(
            plan_principal_buy(
                &asks,
                principal,
                Price::ZERO,
                price(dec!(0.41)),
                price(dec!(0.50)),
                price(dec!(0.50))
            ),
            Err(LadderError::InsufficientDepth)
        );
        assert_eq!(
            plan_principal_buy(
                &asks,
                principal,
                Price::ZERO,
                price(dec!(0.99)),
                price(dec!(0.40)),
                price(dec!(0.50))
            ),
            Err(LadderError::InsufficientDepth)
        );
        assert_eq!(
            plan_principal_buy(
                &asks,
                principal,
                Price::ZERO,
                price(dec!(0.99)),
                price(dec!(0.50)),
                price(dec!(0.40))
            ),
            Err(LadderError::InsufficientDepth)
        );
    }

    #[test]
    fn sized_buy_enforces_minimum_and_principal_plus_reserve_caps() {
        let asks = vec![level(dec!(0.40), dec!(100))];
        let schedule = CompactFeeSchedule::Taker { rate: dec!(0.04) };
        let cap = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();
        let sized = plan_sized_buy(
            &asks,
            schedule,
            BuySizing::Dollar { budget: cap },
            &[cap],
            ShareAmount::from_decimal_exact(dec!(2)).unwrap(),
            price(dec!(0.01)),
            Price::ZERO,
            price(dec!(0.99)),
            price(dec!(0.50)),
            price(dec!(0.50)),
        )
        .unwrap();
        assert!(sized.worst_case_all_in_debit().unwrap() <= cap);
        assert!(sized.ladder.shares >= ShareAmount::from_decimal_exact(dec!(2)).unwrap());

        assert_eq!(
            plan_sized_buy(
                &asks,
                schedule,
                BuySizing::Contract { contracts: 3 },
                &[CollateralAmount::from_decimal_exact(dec!(1.20)).unwrap()],
                ShareAmount::from_decimal_exact(dec!(1)).unwrap(),
                price(dec!(0.01)),
                Price::ZERO,
                price(dec!(0.99)),
                price(dec!(0.50)),
                price(dec!(0.50)),
            ),
            Err(LadderError::CapExceeded)
        );
    }

    #[test]
    fn contract_sizing_fixes_minimum_but_retains_price_improvement() {
        let asks = vec![level(dec!(0.10), dec!(2)), level(dec!(0.11), dec!(10))];
        let plan = plan_sized_buy(
            &asks,
            CompactFeeSchedule::Zero,
            BuySizing::Contract { contracts: 5 },
            &[CollateralAmount::from_decimal_exact(dec!(10)).unwrap()],
            ShareAmount::from_decimal_exact(dec!(1)).unwrap(),
            price(dec!(0.01)),
            price(dec!(0.05)),
            price(dec!(0.99)),
            price(dec!(0.11)),
            price(dec!(0.11)),
        )
        .unwrap();
        assert_eq!(plan.ladder.limit_price, price(dec!(0.11)));
        assert_eq!(plan.ladder.worst_case_debit.to_decimal(), dec!(0.55));
        assert_eq!(plan.ladder.shares.to_decimal(), dec!(5));
        assert_eq!(
            plan.ladder.expected_shares().unwrap().to_decimal(),
            dec!(5.181818)
        );
        assert_eq!(
            plan.ladder.expected_spend().unwrap().to_decimal(),
            dec!(0.549999)
        );
        assert_eq!(
            plan.ladder.vwap().unwrap().0,
            dec!(0.549999) / dec!(5.181818)
        );
    }

    #[test]
    fn empty_stale_and_dust_inputs_fail_closed() {
        assert!(ladder_is_stale(3_001, 1_000));
        assert!(ladder_is_stale(999, 1_000));
        assert_eq!(
            plan_principal_buy(
                &[],
                CollateralAmount::from_atomic(1),
                Price::ZERO,
                price(dec!(1)),
                price(dec!(1)),
                price(dec!(1)),
            ),
            Err(LadderError::InsufficientDepth)
        );
        assert_eq!(
            plan_sized_buy(
                &[level(dec!(0.50), dec!(1))],
                CompactFeeSchedule::Zero,
                BuySizing::Dollar {
                    budget: CollateralAmount::from_atomic(1),
                },
                &[CollateralAmount::from_atomic(1)],
                ShareAmount::from_decimal_exact(dec!(1)).unwrap(),
                price(dec!(0.01)),
                Price::ZERO,
                price(dec!(1)),
                price(dec!(1)),
                price(dec!(1)),
            ),
            Err(LadderError::BelowMinimum)
        );
        assert_eq!(
            plan_sized_buy(
                &[level(dec!(0.50), dec!(10))],
                CompactFeeSchedule::Zero,
                BuySizing::Contract { contracts: 0 },
                &[],
                ShareAmount::from_decimal_exact(dec!(1)).unwrap(),
                price(dec!(0.01)),
                Price::ZERO,
                price(dec!(1)),
                price(dec!(1)),
                price(dec!(1)),
            ),
            Err(LadderError::BelowMinimum)
        );
    }
}
