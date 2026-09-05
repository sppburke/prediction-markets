//! Exact, pure portfolio and latency inputs shared by paper and live risk (#545).

use std::collections::BTreeMap;

use pe_core_types::{BasisPoints, CollateralAmount, Price, ShareAmount};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use crate::RiskMathError;

/// Current cash and net-long positions valued at complete current prices.
pub struct EquityInputs<'a> {
    pub cash: Decimal,
    pub positions: &'a [(ShareAmount, Price)],
}

/// Exact current equity: `cash + sum(net_long * current_price)`.
pub fn current_equity(inputs: &EquityInputs<'_>) -> Result<Decimal, RiskMathError> {
    inputs
        .positions
        .iter()
        .try_fold(inputs.cash, |equity, (quantity, price)| {
            quantity
                .to_decimal()
                .checked_mul(price.0)
                .and_then(|value| equity.checked_add(value))
                .ok_or(RiskMathError::Overflow)
        })
}

/// Failures while deriving the exact collateral credit for one resolution.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ResolutionCreditError {
    #[error("resolution payout is missing outcome {outcome_index}")]
    MissingOutcome { outcome_index: u16 },
    #[error("resolution payout for outcome {outcome_index} is outside [0, 1]")]
    InvalidPayout { outcome_index: u16 },
    #[error("resolution credit arithmetic overflow")]
    Overflow,
}

/// Aggregate exact shares by outcome, multiply once, floor once to collateral atomic units,
/// then checked-sum the outcome credits.
pub fn aggregate_resolution_credit(
    positions: &[(u16, ShareAmount)],
    payout_by_outcome: &BTreeMap<u16, Decimal>,
) -> Result<CollateralAmount, ResolutionCreditError> {
    let shares_by_outcome = positions.iter().try_fold(
        BTreeMap::<u16, u64>::new(),
        |mut aggregated, (outcome_index, shares)| {
            let prior = aggregated.get(outcome_index).copied().unwrap_or_default();
            let total = prior
                .checked_add(shares.atomic())
                .ok_or(ResolutionCreditError::Overflow)?;
            aggregated.insert(*outcome_index, total);
            Ok(aggregated)
        },
    )?;

    shares_by_outcome.into_iter().try_fold(
        CollateralAmount::ZERO,
        |credit, (outcome_index, shares)| {
            let payout = payout_by_outcome
                .get(&outcome_index)
                .copied()
                .ok_or(ResolutionCreditError::MissingOutcome { outcome_index })?;
            if !(Decimal::ZERO..=Decimal::ONE).contains(&payout) {
                return Err(ResolutionCreditError::InvalidPayout { outcome_index });
            }
            // One share atomic unit pays one collateral atomic unit at payout 1. Multiplying
            // atomic shares directly therefore leaves the result in collateral atomic units.
            let outcome_atomic = Decimal::from(shares)
                .checked_mul(payout)
                .map(|value| value.floor())
                .and_then(|value| value.to_u64())
                .ok_or(ResolutionCreditError::Overflow)?;
            credit
                .checked_add(CollateralAmount::from_atomic(outcome_atomic))
                .map_err(|_| ResolutionCreditError::Overflow)
        },
    )
}

/// Fixed-baseline inputs for paper or one live account.
pub struct PnlWindow {
    pub starting_bankroll: Decimal,
    /// The unique valid immediately preceding UTC-midnight mark. Before the first mark,
    /// callers pass `None` and the starting bankroll is the intraday baseline.
    pub preceding_mark_equity: Option<Decimal>,
    /// Realized closes whose Final timestamps are in the caller-proven `(A - 7d, A]` window.
    pub realized_closes_7d: Decimal,
}

/// Profit/loss ratios used by the shared risk snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PnlBps {
    pub intraday: BasisPoints,
    pub rolling_7d: BasisPoints,
    pub absolute: BasisPoints,
}

/// Checked sum of closes whose Final endpoints are in `(at - 7 days, at]`.
pub fn realized_closes_7d(
    closes: &[(i64, Decimal)],
    at_unix: i64,
) -> Result<Decimal, RiskMathError> {
    let lower_exclusive = at_unix
        .checked_sub(7 * 24 * 60 * 60)
        .ok_or(RiskMathError::Overflow)?;
    closes
        .iter()
        .filter(|(final_unix, _)| *final_unix > lower_exclusive && *final_unix <= at_unix)
        .try_fold(Decimal::ZERO, |total, (_, pnl)| {
            total.checked_add(*pnl).ok_or(RiskMathError::Overflow)
        })
}

/// Compute all PnL ratios against the owner's fixed starting bankroll.
pub fn pnl_bps(equity: Decimal, window: &PnlWindow) -> Result<PnlBps, RiskMathError> {
    if window.starting_bankroll <= Decimal::ZERO {
        return Err(RiskMathError::NonPositive);
    }
    let intraday_baseline = window
        .preceding_mark_equity
        .unwrap_or(window.starting_bankroll);
    let intraday_numerator = equity
        .checked_sub(intraday_baseline)
        .ok_or(RiskMathError::Overflow)?;
    let absolute_numerator = equity
        .checked_sub(window.starting_bankroll)
        .ok_or(RiskMathError::Overflow)?;

    Ok(PnlBps {
        intraday: ratio_bps(intraday_numerator, window.starting_bankroll)?,
        rolling_7d: ratio_bps(window.realized_closes_7d, window.starting_bankroll)?,
        absolute: ratio_bps(absolute_numerator, window.starting_bankroll)?,
    })
}

fn ratio_bps(numerator: Decimal, denominator: Decimal) -> Result<BasisPoints, RiskMathError> {
    BasisPoints::from_ratio_floor(numerator, denominator).map_err(|_| RiskMathError::Overflow)
}

pub const COPY_LATENCY_ENGAGE_MS: u64 = 3_000;
pub const COPY_LATENCY_RELEASE_MS: u64 = 2_000;

/// Nearest-rank p95 for one completed clock hour. Empty hours have no value.
#[must_use]
pub fn nearest_rank_p95(samples_ms: &[u64]) -> Option<u64> {
    if samples_ms.is_empty() {
        return None;
    }
    let mut sorted = samples_ms.to_vec();
    sorted.sort_unstable();
    // ceil(95 * n / 100), converted from one-based rank to a zero-based index.
    let rank = sorted.len().checked_mul(95)?.checked_add(99)? / 100;
    sorted.get(rank.checked_sub(1)?).copied()
}

/// Apply the two-hour engage and hysteretic release rules without storing a tracker.
#[must_use]
pub fn latency_switch(active: bool, previous_hour: Option<u64>, latest_hour: Option<u64>) -> bool {
    if active {
        return latest_hour.is_none_or(|value| value > COPY_LATENCY_RELEASE_MS);
    }
    previous_hour.is_some_and(|value| value > COPY_LATENCY_ENGAGE_MS)
        && latest_hour.is_some_and(|value| value > COPY_LATENCY_ENGAGE_MS)
}

/// Combine owner-local ratios without summing paper and live bankrolls.
#[must_use]
pub fn worst_owner(inputs: &[PnlBps]) -> Option<PnlBps> {
    let first = *inputs.first()?;
    Some(inputs.iter().skip(1).fold(first, |worst, input| PnlBps {
        intraday: worst.intraday.min(input.intraday),
        rolling_7d: worst.rolling_7d.min(input.rolling_7d),
        absolute: worst.absolute.min(input.absolute),
    }))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{CollateralAmount, Price, ShareAmount};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;

    fn price(value: Decimal) -> Price {
        Price::new(value).unwrap()
    }

    /// PASS: exact fractional shares are marked at the improved current price and added to cash.
    #[test]
    fn current_equity_values_exact_fractional_positions() {
        let positions = [
            (
                ShareAmount::from_decimal_exact(dec!(2.5)).unwrap(),
                price(dec!(0.6)),
            ),
            (
                ShareAmount::from_decimal_exact(dec!(1.25)).unwrap(),
                price(dec!(0.4)),
            ),
        ];
        assert_eq!(
            current_equity(&EquityInputs {
                cash: dec!(98),
                positions: &positions,
            })
            .unwrap(),
            dec!(100)
        );
    }

    /// PASS: shares are aggregated by outcome before a half-payout is floored once.
    #[test]
    fn resolution_credit_aggregates_before_flooring() {
        let positions = [
            (0, ShareAmount::from_atomic(1)),
            (0, ShareAmount::from_atomic(1)),
        ];
        let payouts = BTreeMap::from([(0, dec!(0.5))]);
        assert_eq!(
            aggregate_resolution_credit(&positions, &payouts).unwrap(),
            CollateralAmount::from_atomic(1)
        );
    }

    /// PASS: a losing outcome credits exactly zero without a sentinel or clamp.
    #[test]
    fn losing_resolution_credits_zero() {
        let positions = [(1, ShareAmount::from_decimal_exact(dec!(8.125)).unwrap())];
        let payouts = BTreeMap::from([(1, Decimal::ZERO)]);
        assert_eq!(
            aggregate_resolution_credit(&positions, &payouts).unwrap(),
            CollateralAmount::ZERO
        );
    }

    /// PASS: missing or invalid payouts and aggregate-share overflow fail closed.
    #[test]
    fn resolution_credit_rejects_invalid_evidence_and_overflow() {
        let one = [(7, ShareAmount::from_atomic(1))];
        assert_eq!(
            aggregate_resolution_credit(&one, &BTreeMap::new()),
            Err(ResolutionCreditError::MissingOutcome { outcome_index: 7 })
        );
        assert_eq!(
            aggregate_resolution_credit(&one, &BTreeMap::from([(7, dec!(1.000001))])),
            Err(ResolutionCreditError::InvalidPayout { outcome_index: 7 })
        );
        let overflowing = [
            (7, ShareAmount::from_atomic(u64::MAX)),
            (7, ShareAmount::from_atomic(1)),
        ];
        assert_eq!(
            aggregate_resolution_credit(&overflowing, &BTreeMap::from([(7, Decimal::ONE)])),
            Err(ResolutionCreditError::Overflow)
        );
    }

    /// PASS: losing value and fees already debited from cash remain visible in exact equity.
    #[test]
    fn equity_preserves_fee_loss_and_losing_resolution_effects() {
        let open = [(ShareAmount::from_whole(10).unwrap(), price(dec!(0.4)))];
        assert_eq!(
            current_equity(&EquityInputs {
                cash: dec!(94.99),
                positions: &open,
            })
            .unwrap(),
            dec!(98.99)
        );
        assert_eq!(
            current_equity(&EquityInputs {
                cash: dec!(94.99),
                positions: &[],
            })
            .unwrap(),
            dec!(94.99)
        );
    }

    /// PASS: custody reconciliation that only moves a winning payout between receivable and cash
    /// supplies the same combined cash-plus-position equity to this formula.
    #[test]
    fn custody_timing_does_not_change_combined_equity_input() {
        let receivable = [(ShareAmount::from_whole(5).unwrap(), Price::ONE)];
        let before = current_equity(&EquityInputs {
            cash: dec!(95),
            positions: &receivable,
        })
        .unwrap();
        let after = current_equity(&EquityInputs {
            cash: dec!(100),
            positions: &[],
        })
        .unwrap();
        assert_eq!(before, after);
    }

    /// PASS: checked accumulation returns overflow instead of retaining a partial equity value.
    #[test]
    fn current_equity_overflow_fails_closed() {
        let positions = [(ShareAmount::from_whole(1).unwrap(), Price::ONE)];
        assert_eq!(
            current_equity(&EquityInputs {
                cash: Decimal::MAX,
                positions: &positions,
            }),
            Err(RiskMathError::Overflow)
        );
    }

    /// PASS: the starting bankroll remains the denominator for intraday, rolling, and absolute PnL.
    #[test]
    fn pnl_uses_fixed_denominator_and_preceding_mark() {
        assert_eq!(
            pnl_bps(
                dec!(92),
                &PnlWindow {
                    starting_bankroll: dec!(100),
                    preceding_mark_equity: Some(dec!(95)),
                    realized_closes_7d: dec!(-6),
                },
            )
            .unwrap(),
            PnlBps {
                intraday: BasisPoints(-300),
                rolling_7d: BasisPoints(-600),
                absolute: BasisPoints(-800),
            }
        );
    }

    /// PASS: the rolling window excludes the lower endpoint, includes the Final at `A`, and ages
    /// the same close out exactly one second later.
    #[test]
    fn realized_close_window_has_exact_endpoints_and_aging() {
        let at = 1_000_000;
        let lower = at - 7 * 24 * 60 * 60;
        let closes = [
            (lower, dec!(100)),
            (lower + 1, dec!(-2)),
            (at, dec!(3)),
            (at + 1, dec!(200)),
        ];
        assert_eq!(realized_closes_7d(&closes, at).unwrap(), dec!(1));
        assert_eq!(realized_closes_7d(&closes, at + 1).unwrap(), dec!(203));
    }

    /// PASS: timestamp subtraction and realized-PnL accumulation fail on overflow.
    #[test]
    fn realized_close_window_overflow_fails_closed() {
        assert_eq!(
            realized_closes_7d(&[], i64::MIN),
            Err(RiskMathError::Overflow)
        );
        assert_eq!(
            realized_closes_7d(&[(0, Decimal::MAX), (1, Decimal::ONE)], 1),
            Err(RiskMathError::Overflow)
        );
    }

    /// PASS: before the first daily mark the starting bankroll is the intraday baseline.
    #[test]
    fn pnl_before_first_mark_uses_starting_bankroll() {
        assert_eq!(
            pnl_bps(
                dec!(98),
                &PnlWindow {
                    starting_bankroll: dec!(100),
                    preceding_mark_equity: None,
                    realized_closes_7d: Decimal::ZERO,
                },
            )
            .unwrap()
            .intraday,
            BasisPoints(-200)
        );
    }

    /// PASS: sub-basis-point losses round toward negative infinity for every ratio.
    #[test]
    fn pnl_rounds_negative_values_toward_negative_infinity() {
        assert_eq!(
            pnl_bps(
                dec!(99.99999),
                &PnlWindow {
                    starting_bankroll: dec!(100),
                    preceding_mark_equity: Some(dec!(100)),
                    realized_closes_7d: dec!(-0.00001),
                },
            )
            .unwrap(),
            PnlBps {
                intraday: BasisPoints(-1),
                rolling_7d: BasisPoints(-1),
                absolute: BasisPoints(-1),
            }
        );
    }

    /// PASS: a nonpositive baseline and unrepresentable ratio return typed math failures.
    #[test]
    fn pnl_invalid_baseline_and_overflow_fail_closed() {
        for starting_bankroll in [Decimal::ZERO, Decimal::NEGATIVE_ONE] {
            assert_eq!(
                pnl_bps(
                    Decimal::ONE,
                    &PnlWindow {
                        starting_bankroll,
                        preceding_mark_equity: None,
                        realized_closes_7d: Decimal::ZERO,
                    },
                ),
                Err(RiskMathError::NonPositive)
            );
        }
        assert_eq!(
            pnl_bps(
                Decimal::MAX,
                &PnlWindow {
                    starting_bankroll: dec!(0.000001),
                    preceding_mark_equity: Some(Decimal::ZERO),
                    realized_closes_7d: Decimal::ZERO,
                },
            ),
            Err(RiskMathError::Overflow)
        );
    }

    /// PASS: threshold values are represented exactly on the fixed denominator.
    #[test]
    fn pnl_threshold_endpoints_are_exact() {
        let values = pnl_bps(
            dec!(90),
            &PnlWindow {
                starting_bankroll: dec!(100),
                preceding_mark_equity: Some(dec!(92)),
                realized_closes_7d: dec!(-6),
            },
        )
        .unwrap();
        assert_eq!(
            values.absolute,
            BasisPoints(crate::KILL_SWITCH_DRAWDOWN_BPS)
        );
        assert_eq!(values.intraday, BasisPoints(crate::INTRADAY_STOP_BPS));
        assert_eq!(values.rolling_7d, BasisPoints(crate::ROLLING_7D_STOP_BPS));
    }

    /// PASS: nearest-rank p95 sorts samples and uses `ceil(0.95 * n)` without interpolation.
    #[test]
    fn p95_uses_nearest_rank() {
        assert_eq!(nearest_rank_p95(&[]), None);
        assert_eq!(nearest_rank_p95(&[17]), Some(17));
        let descending = (1_u64..=20).rev().collect::<Vec<_>>();
        assert_eq!(nearest_rank_p95(&descending), Some(19));
        let one_hundred = (1_u64..=100).rev().collect::<Vec<_>>();
        assert_eq!(nearest_rank_p95(&one_hundred), Some(95));
    }

    /// PASS: inactive state needs two available over-threshold hours; a missing hour breaks the pair.
    #[test]
    fn latency_engage_requires_two_consecutive_available_hours() {
        assert!(latency_switch(false, Some(3_001), Some(3_001)));
        assert!(!latency_switch(false, Some(3_000), Some(3_001)));
        assert!(!latency_switch(false, None, Some(3_001)));
        assert!(!latency_switch(false, Some(3_001), None));
    }

    /// PASS: active state releases at 2,000 ms and otherwise holds, including sample starvation.
    #[test]
    fn latency_release_is_hysteretic_and_missing_holds() {
        assert!(!latency_switch(true, None, Some(2_000)));
        assert!(latency_switch(true, Some(1), Some(2_001)));
        assert!(latency_switch(true, Some(1), Some(3_000)));
        assert!(latency_switch(true, Some(1), None));
    }

    /// PASS: worst-owner composition selects each independent minimum and never sums bankrolls.
    #[test]
    fn worst_owner_selects_each_dimension_independently() {
        let paper = PnlBps {
            intraday: BasisPoints(-100),
            rolling_7d: BasisPoints(-700),
            absolute: BasisPoints(-300),
        };
        let live = PnlBps {
            intraday: BasisPoints(-250),
            rolling_7d: BasisPoints(-500),
            absolute: BasisPoints(-1_000),
        };
        assert_eq!(
            worst_owner(&[paper, live]),
            Some(PnlBps {
                intraday: BasisPoints(-250),
                rolling_7d: BasisPoints(-700),
                absolute: BasisPoints(-1_000),
            })
        );
        assert_eq!(worst_owner(&[]), None);
    }

    /// PASS: exact amount conversion used by equity retains all six collateral decimals.
    #[test]
    fn amount_fixture_is_exact_at_atomic_precision() {
        let amount = CollateralAmount::from_atomic(1_000_001);
        assert_eq!(amount.to_decimal(), dec!(1.000001));
    }
}
