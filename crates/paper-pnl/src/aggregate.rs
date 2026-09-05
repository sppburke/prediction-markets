//! Exact paper/live resolution aggregation (#545).

use std::collections::BTreeMap;

use pe_core_types::{CollateralAmount, ShareAmount};
use pe_source_polymarket_public::BinaryPayoutVector;
use rust_decimal::RoundingStrategy;

/// One nonnegative net-long quantity entering resolution accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolutionPosition {
    pub outcome_index: u8,
    pub net_shares: ShareAmount,
}

/// Exact failures from aggregate payout accounting.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResolutionMathError {
    #[error("resolution outcome index {0} is not binary")]
    InvalidOutcome(u8),
    #[error("resolution share aggregation overflow")]
    ShareOverflow,
    #[error("resolution payout arithmetic overflow")]
    ArithmeticOverflow,
    #[error("resolution payout cannot be represented as collateral: {0}")]
    Amount(#[from] pe_core_types::Error),
}

/// Aggregate shares by outcome, floor each outcome payout once to the six-decimal
/// collateral quantum, then sum the outcome credits. This is the sole arithmetic used
/// by paper authority verification and ordinary-live resolution projection.
pub fn aggregate_resolution_credit(
    positions: &[ResolutionPosition],
    payout: &BinaryPayoutVector,
) -> Result<CollateralAmount, ResolutionMathError> {
    let mut shares_by_outcome = BTreeMap::<u8, ShareAmount>::new();
    for position in positions {
        if usize::from(position.outcome_index) >= payout.decimals().len() {
            return Err(ResolutionMathError::InvalidOutcome(position.outcome_index));
        }
        let current = shares_by_outcome
            .get(&position.outcome_index)
            .copied()
            .unwrap_or(ShareAmount::ZERO);
        shares_by_outcome.insert(
            position.outcome_index,
            current
                .checked_add(position.net_shares)
                .map_err(|_| ResolutionMathError::ShareOverflow)?,
        );
    }

    let mut total = CollateralAmount::ZERO;
    for (outcome_index, shares) in shares_by_outcome {
        let payout_fraction = payout.decimals()[usize::from(outcome_index)];
        let raw = shares
            .to_decimal()
            .checked_mul(payout_fraction)
            .ok_or(ResolutionMathError::ArithmeticOverflow)?;
        let floored = raw.round_dp_with_strategy(6, RoundingStrategy::ToNegativeInfinity);
        total = total.checked_add(CollateralAmount::from_decimal_exact(floored)?)?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn half_payout_floors_once_after_same_outcome_aggregation() {
        let atomic = ShareAmount::from_atomic(1);
        let credit = aggregate_resolution_credit(
            &[
                ResolutionPosition {
                    outcome_index: 0,
                    net_shares: atomic,
                },
                ResolutionPosition {
                    outcome_index: 0,
                    net_shares: atomic,
                },
            ],
            &BinaryPayoutVector::fifty_fifty(),
        )
        .unwrap();
        assert_eq!(credit.atomic(), 1);
    }

    #[test]
    fn outcome_credits_are_floored_independently_then_summed() {
        let credit = aggregate_resolution_credit(
            &[
                ResolutionPosition {
                    outcome_index: 0,
                    net_shares: ShareAmount::from_atomic(3),
                },
                ResolutionPosition {
                    outcome_index: 1,
                    net_shares: ShareAmount::from_atomic(3),
                },
            ],
            &BinaryPayoutVector::fifty_fifty(),
        )
        .unwrap();
        assert_eq!(credit.atomic(), 2);
    }

    #[test]
    fn invalid_outcome_and_aggregate_overflow_fail_closed() {
        assert_eq!(
            aggregate_resolution_credit(
                &[ResolutionPosition {
                    outcome_index: 2,
                    net_shares: ShareAmount::from_atomic(1),
                }],
                &BinaryPayoutVector::fifty_fifty(),
            ),
            Err(ResolutionMathError::InvalidOutcome(2))
        );
        assert_eq!(
            aggregate_resolution_credit(
                &[
                    ResolutionPosition {
                        outcome_index: 0,
                        net_shares: ShareAmount::from_atomic(u64::MAX),
                    },
                    ResolutionPosition {
                        outcome_index: 0,
                        net_shares: ShareAmount::from_atomic(1),
                    },
                ],
                &BinaryPayoutVector::fifty_fifty(),
            ),
            Err(ResolutionMathError::ShareOverflow)
        );
    }
}
