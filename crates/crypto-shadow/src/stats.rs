//! Small aggregate-statistics helpers shared by the report layer and the #310
//! offline sweep. Pure; all values are [`Decimal`] (no `f64`).

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

/// Mean of `values`, or `None` when empty.
pub(crate) fn mean_decimal(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let sum: Decimal = values.iter().copied().sum();
    Some(sum / Decimal::from(values.len()))
}

/// Nearest-rank percentile (`q` in [0,1]) over an already-sorted slice.
pub(crate) fn percentile_decimal(sorted: &[Decimal], q: Decimal) -> Option<Decimal> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (q * Decimal::from(sorted.len()))
        .ceil()
        .to_i64()
        .unwrap_or(1)
        .max(1);
    let idx = usize::try_from(rank - 1).unwrap_or(0).min(sorted.len() - 1);
    sorted.get(idx).copied()
}

/// Nearest-rank percentile (`q` in [0,1]) over an already-sorted `i64` slice.
pub(crate) fn percentile_i64(sorted: &[i64], q: Decimal) -> Option<i64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (q * Decimal::from(sorted.len()))
        .ceil()
        .to_i64()
        .unwrap_or(1)
        .max(1);
    let idx = usize::try_from(rank - 1).unwrap_or(0).min(sorted.len() - 1);
    sorted.get(idx).copied()
}

/// Fraction of `values` strictly greater than zero, or `None` when empty.
pub(crate) fn frac_positive(values: &[Decimal]) -> Option<Decimal> {
    if values.is_empty() {
        return None;
    }
    let pos = values.iter().filter(|v| **v > Decimal::ZERO).count();
    Some(Decimal::from(pos) / Decimal::from(values.len()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn percentile_nearest_rank() {
        let v = vec![dec!(1), dec!(2), dec!(3), dec!(4), dec!(5)];
        assert_eq!(percentile_decimal(&v, dec!(0.5)), Some(dec!(3)));
        assert_eq!(percentile_decimal(&v, dec!(0.95)), Some(dec!(5)));
        assert_eq!(percentile_decimal(&[], dec!(0.5)), None);
        assert_eq!(percentile_i64(&[10, 20, 30], dec!(0.5)), Some(20));
        assert_eq!(percentile_i64(&[], dec!(0.5)), None);
    }

    #[test]
    fn mean_and_frac_positive() {
        let v = vec![dec!(-1), dec!(1), dec!(3)];
        assert_eq!(mean_decimal(&v), Some(dec!(1)));
        assert_eq!(frac_positive(&v), Some(dec!(2) / dec!(3)));
        assert_eq!(mean_decimal(&[]), None);
        assert_eq!(frac_positive(&[]), None);
    }
}
