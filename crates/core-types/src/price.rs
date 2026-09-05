use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::{Deserialize, Serialize};

use crate::Error;

/// Policy for rounding an f64 value when converting to a Decimal-based type.
/// Required because f64 → Decimal is inherently imprecise; callers must be explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundingPolicy {
    HalfEven,
    HalfUp,
    Truncate,
}

fn decimal_from_f64(v: f64, policy: RoundingPolicy) -> Result<Decimal, Error> {
    if !v.is_finite() {
        return Err(Error::ConvError {
            message: format!("non-finite f64: {v}"),
        });
    }
    let s = format!("{v}");
    let d: Decimal = s.parse().map_err(|_| Error::ConvError {
        message: format!("cannot represent f64 {v} as Decimal"),
    })?;
    let rounded = match policy {
        RoundingPolicy::HalfEven => {
            d.round_dp_with_strategy(10, rust_decimal::RoundingStrategy::MidpointNearestEven)
        }
        RoundingPolicy::HalfUp => {
            d.round_dp_with_strategy(10, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
        }
        RoundingPolicy::Truncate => {
            d.round_dp_with_strategy(10, rust_decimal::RoundingStrategy::ToZero)
        }
    };
    Ok(rounded)
}

/// Market price on [0, 1]. Venue-agnostic; venue adapters convert to this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Price(pub Decimal);

impl Price {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);

    pub fn new(d: Decimal) -> Result<Self, Error> {
        if d < Decimal::ZERO || d > Decimal::ONE {
            return Err(Error::OutOfRange { field: "Price" });
        }
        Ok(Self(d))
    }

    pub fn from_f64_rounding(v: f64, policy: RoundingPolicy) -> Result<Self, Error> {
        Self::new(decimal_from_f64(v, policy)?)
    }
}

/// Model/strategy probability estimate on [0, 1]. Distinct from market price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Probability(pub Decimal);

impl Probability {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);

    pub fn new(d: Decimal) -> Result<Self, Error> {
        if d < Decimal::ZERO || d > Decimal::ONE {
            return Err(Error::OutOfRange {
                field: "Probability",
            });
        }
        Ok(Self(d))
    }

    pub fn from_f64_rounding(v: f64, policy: RoundingPolicy) -> Result<Self, Error> {
        Self::new(decimal_from_f64(v, policy)?)
    }

    /// Convert to parts-per-million, rounding half-even. Always succeeds.
    pub fn to_ppm(self) -> ProbabilityPpm {
        let scaled = (self.0 * Decimal::from(1_000_000u32))
            .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointNearestEven);
        // self.0 ∈ [0, 1] → scaled ∈ [0, 1_000_000]; to_u32 cannot fail here
        let n = scaled.to_u32().unwrap_or(1_000_000).min(1_000_000);
        ProbabilityPpm(n)
    }

    /// Convert to parts-per-million only if exact (no rounding needed).
    pub fn to_ppm_lossless(self) -> Result<ProbabilityPpm, Error> {
        let scaled = self.0 * Decimal::from(1_000_000u32);
        if scaled.fract() != Decimal::ZERO {
            return Err(Error::LossyRounding);
        }
        let n = scaled.to_u32().ok_or(Error::OutOfRange {
            field: "ProbabilityPpm",
        })?;
        if n > 1_000_000 {
            return Err(Error::OutOfRange {
                field: "ProbabilityPpm",
            });
        }
        Ok(ProbabilityPpm(n))
    }
}

impl From<ProbabilityPpm> for Probability {
    fn from(ppm: ProbabilityPpm) -> Self {
        Self(Decimal::from(ppm.0) / Decimal::from(1_000_000u32))
    }
}

/// Signed price movement. Unbounded; can be negative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PriceDelta(pub Decimal);

impl PriceDelta {
    pub fn from_f64_rounding(v: f64, policy: RoundingPolicy) -> Result<Self, Error> {
        decimal_from_f64(v, policy).map(PriceDelta)
    }
}

/// Kalshi price in whole cents: 0..=100.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KalshiPriceCents(pub u8);

impl KalshiPriceCents {
    pub const ZERO: Self = Self(0);
    pub const HUNDRED: Self = Self(100);

    pub fn new(n: u8) -> Result<Self, Error> {
        if n > 100 {
            return Err(Error::OutOfRange {
                field: "KalshiPriceCents",
            });
        }
        Ok(Self(n))
    }

    pub fn from_f64_rounding(v: f64, _policy: RoundingPolicy) -> Result<Self, Error> {
        if !v.is_finite() {
            return Err(Error::ConvError {
                message: format!("non-finite f64: {v}"),
            });
        }
        let n = v.round();
        if !(0.0..=100.0).contains(&n) {
            return Err(Error::OutOfRange {
                field: "KalshiPriceCents",
            });
        }
        Ok(Self(n as u8))
    }
}

impl Default for KalshiPriceCents {
    fn default() -> Self {
        Self::ZERO
    }
}

impl From<KalshiPriceCents> for Probability {
    fn from(c: KalshiPriceCents) -> Self {
        Self(Decimal::from(u32::from(c.0)) / Decimal::from(100u32))
    }
}

/// Polymarket price on [0, 1] with at most 4 decimal places.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PolymarketPriceDecimal(pub Decimal);

impl PolymarketPriceDecimal {
    pub fn new(d: Decimal) -> Result<Self, Error> {
        if d < Decimal::ZERO || d > Decimal::ONE {
            return Err(Error::OutOfRange {
                field: "PolymarketPriceDecimal",
            });
        }
        if d.scale() > 4 {
            return Err(Error::ScaleError {
                max_dp: 4,
                actual_dp: d.scale(),
            });
        }
        Ok(Self(d))
    }

    pub fn from_f64_rounding(v: f64, policy: RoundingPolicy) -> Result<Self, Error> {
        let d = decimal_from_f64(v, policy)?;
        let d4 = d.round_dp_with_strategy(4, rust_decimal::RoundingStrategy::MidpointNearestEven);
        Self::new(d4)
    }
}

impl From<PolymarketPriceDecimal> for Price {
    fn from(p: PolymarketPriceDecimal) -> Self {
        Self(p.0)
    }
}

/// Probability in parts per million: 0..=1_000_000.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct ProbabilityPpm(pub u32);

impl ProbabilityPpm {
    pub fn new(n: u32) -> Result<Self, Error> {
        if n > 1_000_000 {
            return Err(Error::OutOfRange {
                field: "ProbabilityPpm",
            });
        }
        Ok(Self(n))
    }

    pub fn from_f64_rounding(v: f64, _policy: RoundingPolicy) -> Result<Self, Error> {
        if !v.is_finite() {
            return Err(Error::ConvError {
                message: format!("non-finite f64: {v}"),
            });
        }
        let n = v.round();
        if !(0.0..=1_000_000.0).contains(&n) {
            return Err(Error::OutOfRange {
                field: "ProbabilityPpm",
            });
        }
        Ok(Self(n as u32))
    }
}

impl TryFrom<Probability> for ProbabilityPpm {
    type Error = Error;

    fn try_from(p: Probability) -> Result<Self, Self::Error> {
        p.to_ppm_lossless()
    }
}

/// Fee or spread in basis points (1 bps = 0.01%). Signed; negative = rebate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BasisPoints(pub i32);

impl BasisPoints {
    pub const ZERO: Self = Self(0);

    /// Convert to Decimal fraction (1 bps → 0.0001).
    pub fn to_decimal(self) -> Decimal {
        Decimal::from(self.0) / Decimal::from(10_000i32)
    }

    /// Convert from Decimal fraction, rounding half-even.
    pub fn from_decimal(d: Decimal) -> Self {
        let bps = (d * Decimal::from(10_000i32))
            .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointNearestEven);
        // saturate preserving sign: negative overflow → i32::MIN, positive → i32::MAX
        let n = bps.to_i32().unwrap_or_else(|| {
            if bps.is_sign_negative() {
                i32::MIN
            } else {
                i32::MAX
            }
        });
        Self(n)
    }

    /// Convert an exact ratio to basis points, rounding toward negative infinity.
    ///
    /// `denominator <= 0`, overflowing arithmetic, or a result outside `i32`
    /// returns [`Error::OutOfRange`].
    pub fn from_ratio_floor(numerator: Decimal, denominator: Decimal) -> Result<Self, Error> {
        if denominator <= Decimal::ZERO {
            return Err(Error::OutOfRange {
                field: "BasisPoints",
            });
        }
        let scaled = numerator
            .checked_div(denominator)
            .and_then(|ratio| ratio.checked_mul(Decimal::from(10_000i32)))
            .ok_or(Error::OutOfRange {
                field: "BasisPoints",
            })?
            .floor();
        scaled.to_i32().map(Self).ok_or(Error::OutOfRange {
            field: "BasisPoints",
        })
    }

    pub fn from_f64_rounding(v: f64, _policy: RoundingPolicy) -> Result<Self, Error> {
        if !v.is_finite() {
            return Err(Error::ConvError {
                message: format!("non-finite f64: {v}"),
            });
        }
        let n = v.round();
        if n < i32::MIN as f64 || n > i32::MAX as f64 {
            return Err(Error::OutOfRange {
                field: "BasisPoints",
            });
        }
        Ok(Self(n as i32))
    }
}

impl Default for BasisPoints {
    fn default() -> Self {
        Self::ZERO
    }
}

/// Fractional-Kelly bet size on [0, 1].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KellyFraction(pub Decimal);

impl KellyFraction {
    pub const ZERO: Self = Self(Decimal::ZERO);
    pub const ONE: Self = Self(Decimal::ONE);

    pub fn new(d: Decimal) -> Result<Self, Error> {
        if d < Decimal::ZERO || d > Decimal::ONE {
            return Err(Error::OutOfRange {
                field: "KellyFraction",
            });
        }
        Ok(Self(d))
    }

    pub fn from_f64_rounding(v: f64, policy: RoundingPolicy) -> Result<Self, Error> {
        Self::new(decimal_from_f64(v, policy)?)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    /// PASS: a negative fractional basis point floors toward negative infinity.
    #[test]
    fn basis_points_ratio_floor_rounds_negative_values_down() {
        assert_eq!(
            BasisPoints::from_ratio_floor(dec!(-0.00015), Decimal::ONE).unwrap(),
            BasisPoints(-2)
        );
    }

    /// PASS: zero and both exact `i32` boundaries convert without loss.
    #[test]
    fn basis_points_ratio_floor_accepts_zero_and_exact_i32_boundaries() {
        assert_eq!(
            BasisPoints::from_ratio_floor(Decimal::ZERO, Decimal::ONE).unwrap(),
            BasisPoints::ZERO
        );
        for expected in [i32::MIN, i32::MAX] {
            let numerator = Decimal::from(expected) / Decimal::from(10_000i32);
            assert_eq!(
                BasisPoints::from_ratio_floor(numerator, Decimal::ONE).unwrap(),
                BasisPoints(expected)
            );
        }
    }

    /// PASS: zero and negative denominators return the frozen out-of-range error.
    #[test]
    fn basis_points_ratio_floor_rejects_nonpositive_denominator() {
        for denominator in [Decimal::ZERO, Decimal::NEGATIVE_ONE] {
            assert_eq!(
                BasisPoints::from_ratio_floor(Decimal::ONE, denominator),
                Err(Error::OutOfRange {
                    field: "BasisPoints"
                })
            );
        }
    }

    /// PASS: Decimal arithmetic overflow and a result above `i32::MAX` fail closed.
    #[test]
    fn basis_points_ratio_floor_rejects_arithmetic_and_i32_overflow() {
        assert_eq!(
            BasisPoints::from_ratio_floor(Decimal::MAX, dec!(0.1)),
            Err(Error::OutOfRange {
                field: "BasisPoints"
            })
        );
        let above_i32 = Decimal::from(i64::from(i32::MAX) + 1) / Decimal::from(10_000i32);
        assert_eq!(
            BasisPoints::from_ratio_floor(above_i32, Decimal::ONE),
            Err(Error::OutOfRange {
                field: "BasisPoints"
            })
        );
    }
}
