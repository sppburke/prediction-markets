//! Exact six-decimal venue amounts used at financial boundaries.

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::{Deserialize, Serialize};

use crate::Error;

const ATOMIC_SCALE: u64 = 1_000_000;

macro_rules! exact_amount {
    ($name:ident, $field:literal) => {
        #[derive(
            Debug,
            Clone,
            Copy,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            Serialize,
            Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(u64);

        impl $name {
            pub const ZERO: Self = Self(0);

            #[must_use]
            pub const fn from_atomic(atomic: u64) -> Self {
                Self(atomic)
            }

            #[must_use]
            pub const fn atomic(self) -> u64 {
                self.0
            }

            pub fn from_decimal_exact(value: Decimal) -> Result<Self, Error> {
                if value.is_sign_negative() {
                    return Err(Error::OutOfRange { field: $field });
                }
                let scaled = value * Decimal::from(ATOMIC_SCALE);
                if scaled.fract() != Decimal::ZERO {
                    return Err(Error::ScaleError {
                        max_dp: 6,
                        actual_dp: value.scale(),
                    });
                }
                scaled
                    .to_u64()
                    .map(Self)
                    .ok_or(Error::OutOfRange { field: $field })
            }

            #[must_use]
            pub fn to_decimal(self) -> Decimal {
                Decimal::from(self.0) / Decimal::from(ATOMIC_SCALE)
            }

            pub fn checked_add(self, rhs: Self) -> Result<Self, Error> {
                self.0
                    .checked_add(rhs.0)
                    .map(Self)
                    .ok_or(Error::OutOfRange { field: $field })
            }

            pub fn checked_sub(self, rhs: Self) -> Result<Self, Error> {
                self.0
                    .checked_sub(rhs.0)
                    .map(Self)
                    .ok_or(Error::OutOfRange { field: $field })
            }
        }
    };
}

exact_amount!(CollateralAmount, "CollateralAmount");
exact_amount!(ShareAmount, "ShareAmount");

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use rust_decimal_macros::dec;

    use super::*;

    #[test]
    fn collateral_round_trips_six_decimals() {
        let amount = CollateralAmount::from_decimal_exact(dec!(1.000001)).unwrap();
        assert_eq!(amount.atomic(), 1_000_001);
        assert_eq!(amount.to_decimal(), dec!(1.000001));
    }

    #[test]
    fn amount_rejects_loss_and_negative_values() {
        assert!(CollateralAmount::from_decimal_exact(dec!(0.0000001)).is_err());
        assert!(ShareAmount::from_decimal_exact(dec!(-1)).is_err());
    }
}
