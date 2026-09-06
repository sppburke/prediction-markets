//! Exact compact-market fee economics (#545).

use pe_core_types::{CollateralAmount, Price, ShareAmount};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use std::collections::BTreeMap;

/// Compact `/clob-markets/{condition}` fee shape accepted for economics (#545): zero-fee or
/// taker-only exponent-one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum CompactFeeSchedule {
    Zero,
    Taker { rate: Decimal },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FeeScheduleError {
    #[error("maker fee is nonzero or ambiguous")]
    MakerFee,
    #[error("fee exponent is not one")]
    Exponent,
    #[error("fee rate is not exactly representable")]
    Unrepresentable,
    #[error("compact fee fields contradict each other")]
    Contradictory,
    #[error("compact base fees are unknown or nonzero without a schedule")]
    UnknownBase,
    #[error("compact fee field is malformed")]
    Malformed,
}

/// Parse the compact wire's exact `fd` / `mbf` / `tbf` lexemes.
///
/// This deliberately reads the raw JSON lexemes before the shared SDK DTO is deserialized by the
/// market parser. Financial decimals therefore never traverse `f64`, and absent fields remain
/// distinguishable from explicit `null`.
pub fn parse_compact_fee_schedule(bytes: &[u8]) -> Result<CompactFeeSchedule, FeeScheduleError> {
    let fields: BTreeMap<String, Box<RawValue>> =
        serde_json::from_slice(bytes).map_err(|_| FeeScheduleError::Malformed)?;
    let maker_base = optional_base_fee(&fields, "mbf")?;
    if maker_base.is_some_and(|value| value != Decimal::ZERO) {
        return Err(FeeScheduleError::MakerFee);
    }
    let taker_base = optional_base_fee(&fields, "tbf")?;
    let Some(details) = fields.get("fd") else {
        return if taker_base.is_none_or(|value| value == Decimal::ZERO) {
            Ok(CompactFeeSchedule::Zero)
        } else {
            Err(FeeScheduleError::UnknownBase)
        };
    };
    if details.get().trim() == "null" {
        return Err(FeeScheduleError::Malformed);
    }
    if taker_base.is_some_and(|value| value != Decimal::ZERO) {
        return Err(FeeScheduleError::Contradictory);
    }

    let detail_fields: BTreeMap<String, Box<RawValue>> =
        serde_json::from_str(details.get()).map_err(|_| FeeScheduleError::Malformed)?;
    if detail_fields.len() != 3
        || !detail_fields.contains_key("r")
        || !detail_fields.contains_key("e")
        || !detail_fields.contains_key("to")
    {
        return Err(FeeScheduleError::Malformed);
    }
    let rate = parse_decimal_lexeme(detail_fields.get("r").ok_or(FeeScheduleError::Malformed)?)?;
    let exponent: i64 = serde_json::from_str(
        detail_fields
            .get("e")
            .ok_or(FeeScheduleError::Malformed)?
            .get(),
    )
    .map_err(|_| FeeScheduleError::Malformed)?;
    let taker_only: bool = serde_json::from_str(
        detail_fields
            .get("to")
            .ok_or(FeeScheduleError::Malformed)?
            .get(),
    )
    .map_err(|_| FeeScheduleError::Malformed)?;
    if !taker_only {
        return Err(FeeScheduleError::MakerFee);
    }
    if exponent != 1 {
        return Err(FeeScheduleError::Exponent);
    }
    if rate < Decimal::ZERO || rate >= Decimal::ONE {
        return Err(FeeScheduleError::Unrepresentable);
    }
    Ok(CompactFeeSchedule::Taker { rate })
}

fn optional_base_fee(
    fields: &BTreeMap<String, Box<RawValue>>,
    key: &str,
) -> Result<Option<Decimal>, FeeScheduleError> {
    fields
        .get(key)
        .map(|raw| {
            if raw.get().trim() == "null" {
                Err(FeeScheduleError::UnknownBase)
            } else {
                parse_decimal_lexeme(raw).map_err(|_| FeeScheduleError::UnknownBase)
            }
        })
        .transpose()
}

pub(crate) fn parse_decimal_lexeme(raw: &RawValue) -> Result<Decimal, FeeScheduleError> {
    let lexeme = raw.get().trim();
    let owned;
    let unquoted = if lexeme.starts_with('"') {
        owned = serde_json::from_str::<String>(lexeme).map_err(|_| FeeScheduleError::Malformed)?;
        owned.as_str()
    } else {
        lexeme
    };
    Decimal::from_str_exact(unquoted)
        .or_else(|_| Decimal::from_scientific(unquoted))
        .map_err(|_| FeeScheduleError::Unrepresentable)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FeeError {
    #[error("exact fee arithmetic failed")]
    Arithmetic,
    #[error("price band is empty or inverted")]
    Band,
}

/// Platform taker fee: `truncate_5(shares × rate × p × (1 − p))`.
pub fn taker_fee(
    schedule: CompactFeeSchedule,
    shares: ShareAmount,
    price: Price,
) -> Result<CollateralAmount, FeeError> {
    let Some(rate) = validated_rate(schedule)? else {
        return Ok(CollateralAmount::ZERO);
    };
    let fee = shares
        .to_decimal()
        .checked_mul(rate)
        .and_then(|value| value.checked_mul(price.0))
        .and_then(|value| value.checked_mul(Decimal::ONE.checked_sub(price.0)?))
        .ok_or(FeeError::Arithmetic)?
        .round_dp_with_strategy(5, RoundingStrategy::ToZero);
    CollateralAmount::from_decimal_exact(fee).map_err(|_| FeeError::Arithmetic)
}

/// Greatest fee reachable for the signed minimum shares over the inclusive price band.
///
/// The supported exponent-one curve reaches its maximum at one half, so only the interval
/// endpoints and `0.5` can be the maximizer. The result is rounded upward to the venue's
/// five-decimal fee quantum.
pub fn fee_reserve(
    schedule: CompactFeeSchedule,
    principal: CollateralAmount,
    minimum_shares: ShareAmount,
    floor: Price,
    ceiling: Price,
) -> Result<CollateralAmount, FeeError> {
    validate_band(floor, ceiling)?;
    let Some(rate) = validated_rate(schedule)? else {
        return Ok(CollateralAmount::ZERO);
    };
    if principal == CollateralAmount::ZERO && minimum_shares == ShareAmount::ZERO {
        return Ok(CollateralAmount::ZERO);
    }
    if principal == CollateralAmount::ZERO
        || minimum_shares == ShareAmount::ZERO
        || ceiling == Price::ZERO
        || minimum_shares
            .to_decimal()
            .checked_mul(ceiling.0)
            .is_none_or(|signed_notional| signed_notional > principal.to_decimal())
    {
        return Err(FeeError::Arithmetic);
    }
    let curve_price = maximum_curve_price(floor, ceiling);
    let curve = curve_price
        .checked_mul(
            Decimal::ONE
                .checked_sub(curve_price)
                .ok_or(FeeError::Arithmetic)?,
        )
        .ok_or(FeeError::Arithmetic)?;
    let reserve = minimum_shares
        .to_decimal()
        .checked_mul(rate)
        .and_then(|value| value.checked_mul(curve))
        .ok_or(FeeError::Arithmetic)?
        .round_dp_with_strategy(5, RoundingStrategy::ToPositiveInfinity);
    CollateralAmount::from_decimal_exact(reserve).map_err(|_| FeeError::Arithmetic)
}

/// Conservative principal whose principal plus fee reserve fits the monetary budget.
pub fn principal_for_budget(
    schedule: CompactFeeSchedule,
    budget: CollateralAmount,
    floor: Price,
    ceiling: Price,
) -> Result<CollateralAmount, FeeError> {
    validate_band(floor, ceiling)?;
    let Some(rate) = validated_rate(schedule)? else {
        return Ok(budget);
    };
    let fee_ratio = rate
        .checked_mul(
            Decimal::ONE
                .checked_sub(floor.0)
                .ok_or(FeeError::Arithmetic)?,
        )
        .ok_or(FeeError::Arithmetic)?;
    let divisor = Decimal::ONE
        .checked_add(fee_ratio)
        .ok_or(FeeError::Arithmetic)?;
    let raw = budget
        .to_decimal()
        .checked_div(divisor)
        .ok_or(FeeError::Arithmetic)?
        .round_dp_with_strategy(6, RoundingStrategy::ToNegativeInfinity);
    let mut principal =
        CollateralAmount::from_decimal_exact(raw).map_err(|_| FeeError::Arithmetic)?;
    let rechecked = all_in_reserve(schedule, principal, floor, ceiling)?;
    if rechecked > budget {
        let excess = rechecked
            .checked_sub(budget)
            .map_err(|_| FeeError::Arithmetic)?;
        principal = if excess >= principal {
            CollateralAmount::ZERO
        } else {
            principal
                .checked_sub(excess)
                .map_err(|_| FeeError::Arithmetic)?
        };
    }
    if all_in_reserve(schedule, principal, floor, ceiling)? > budget {
        return Err(FeeError::Arithmetic);
    }
    Ok(principal)
}

/// Live finality check: the charged fee fits its reserve and lies on the five-decimal quantum.
#[must_use]
pub fn fee_within_reserve(fee: CollateralAmount, reserve: CollateralAmount) -> bool {
    fee <= reserve && fee.atomic().is_multiple_of(10)
}

fn validate_band(floor: Price, ceiling: Price) -> Result<(), FeeError> {
    if floor > ceiling {
        return Err(FeeError::Band);
    }
    Ok(())
}

fn validated_rate(schedule: CompactFeeSchedule) -> Result<Option<Decimal>, FeeError> {
    match schedule {
        CompactFeeSchedule::Zero => Ok(None),
        CompactFeeSchedule::Taker { rate } if rate >= Decimal::ZERO && rate < Decimal::ONE => {
            Ok(Some(rate))
        }
        CompactFeeSchedule::Taker { .. } => Err(FeeError::Arithmetic),
    }
}

fn maximum_curve_price(floor: Price, ceiling: Price) -> Decimal {
    let half = Decimal::new(5, 1);
    if floor.0 <= half && ceiling.0 >= half {
        half
    } else if ceiling.0 < half {
        ceiling.0
    } else {
        floor.0
    }
}

fn all_in_reserve(
    schedule: CompactFeeSchedule,
    principal: CollateralAmount,
    floor: Price,
    _ceiling: Price,
) -> Result<CollateralAmount, FeeError> {
    let Some(rate) = validated_rate(schedule)? else {
        return Ok(principal);
    };
    let reserve = principal
        .to_decimal()
        .checked_mul(rate)
        .and_then(|value| value.checked_mul(Decimal::ONE.checked_sub(floor.0)?))
        .ok_or(FeeError::Arithmetic)?
        .round_dp_with_strategy(5, RoundingStrategy::ToPositiveInfinity);
    principal
        .checked_add(
            CollateralAmount::from_decimal_exact(reserve).map_err(|_| FeeError::Arithmetic)?,
        )
        .map_err(|_| FeeError::Arithmetic)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use rust_decimal_macros::dec;
    use serde_json::{Value, json};

    use super::*;

    fn compact(fields: Value) -> Vec<u8> {
        let mut base = json!({
            "c": "0x4c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ee",
            "mts": 0.001
        });
        let base_object = base.as_object_mut().unwrap();
        for (key, value) in fields.as_object().unwrap() {
            base_object.insert(key.clone(), value.clone());
        }
        serde_json::to_vec(&base).unwrap()
    }

    #[test]
    fn classifies_every_accepted_compact_shape() {
        for fields in [
            json!({}),
            json!({"mbf": 0}),
            json!({"tbf": 0}),
            json!({"mbf": 0, "tbf": 0}),
        ] {
            assert_eq!(
                parse_compact_fee_schedule(&compact(fields)).unwrap(),
                CompactFeeSchedule::Zero
            );
        }
        for fields in [
            json!({"fd": {"r": 0, "e": 1, "to": true}}),
            json!({"fd": {"r": 0.0025, "e": 1, "to": true}}),
            json!({"fd": {"r": 0.0025, "e": 1, "to": true}, "mbf": 0, "tbf": 0}),
        ] {
            let expected_rate = fields
                .get("fd")
                .and_then(|details| details.get("r"))
                .and_then(Value::as_i64)
                .map_or(dec!(0.0025), Decimal::from);
            assert_eq!(
                parse_compact_fee_schedule(&compact(fields.clone())).unwrap(),
                CompactFeeSchedule::Taker {
                    rate: expected_rate
                }
            );
        }
        assert_eq!(
            parse_compact_fee_schedule(br#"{"fd":{"r":"0.0000001","e":1,"to":true}}"#,).unwrap(),
            CompactFeeSchedule::Taker {
                rate: dec!(0.0000001)
            }
        );
    }

    #[test]
    fn rejects_every_invalid_compact_fee_class() {
        let cases = [
            (json!({"mbf": null}), FeeScheduleError::UnknownBase),
            (json!({"tbf": null}), FeeScheduleError::UnknownBase),
            (json!({"fd": null}), FeeScheduleError::Malformed),
            (json!({"mbf": 1}), FeeScheduleError::MakerFee),
            (json!({"tbf": 1}), FeeScheduleError::UnknownBase),
            (
                json!({"fd": {"r": 0.0025, "e": 1, "to": false}}),
                FeeScheduleError::MakerFee,
            ),
            (
                json!({"fd": {"r": 0.0025, "e": 2, "to": true}}),
                FeeScheduleError::Exponent,
            ),
            (
                json!({"fd": {"r": 1, "e": 1, "to": true}}),
                FeeScheduleError::Unrepresentable,
            ),
            (
                json!({"fd": {"r": 0.0025, "e": 1, "to": true}, "tbf": 1}),
                FeeScheduleError::Contradictory,
            ),
            (
                json!({"fd": {"r": 0.0025, "rate": 0.003, "e": 1, "to": true}}),
                FeeScheduleError::Malformed,
            ),
            (
                json!({"fd": {"r": 0.0025, "rate": 0.0025, "e": 1, "to": true}}),
                FeeScheduleError::Malformed,
            ),
            (
                json!({"fd": {"rate": 0.0025, "exponent": 1, "taker_only": true}}),
                FeeScheduleError::Malformed,
            ),
            (
                json!({"fd": {"r": 0.0025, "e": 1, "takerOnly": true}}),
                FeeScheduleError::Malformed,
            ),
            (
                json!({"fd": {"r": 0.0025, "e": 1, "to": true, "unknown": 1}}),
                FeeScheduleError::Malformed,
            ),
            (
                json!({"fd": {"e": 1, "to": true}}),
                FeeScheduleError::Malformed,
            ),
            (
                json!({"fd": {"r": 0.0025, "to": true}}),
                FeeScheduleError::Malformed,
            ),
            (
                json!({"fd": {"r": 0.0025, "e": 1}}),
                FeeScheduleError::Malformed,
            ),
        ];
        for (fields, expected) in cases {
            assert_eq!(parse_compact_fee_schedule(&compact(fields)), Err(expected));
        }
        assert_eq!(
            parse_compact_fee_schedule(br#"{"fd":"bad"}"#),
            Err(FeeScheduleError::Malformed)
        );
    }

    #[test]
    fn directly_constructed_invalid_schedule_fails_economics() {
        assert_eq!(
            taker_fee(
                CompactFeeSchedule::Taker { rate: dec!(1) },
                ShareAmount::from_decimal_exact(dec!(1)).unwrap(),
                Price::new(dec!(0.5)).unwrap(),
            ),
            Err(FeeError::Arithmetic)
        );
    }

    #[test]
    fn taker_fee_truncates_at_five_decimals() {
        let schedule = CompactFeeSchedule::Taker { rate: dec!(0.0025) };
        assert_eq!(
            taker_fee(
                schedule,
                ShareAmount::from_decimal_exact(dec!(0.499)).unwrap(),
                Price::new(dec!(0.4)).unwrap()
            )
            .unwrap()
            .to_decimal(),
            dec!(0.00029)
        );
        assert_eq!(
            taker_fee(
                schedule,
                ShareAmount::from_decimal_exact(dec!(259.064)).unwrap(),
                Price::new(dec!(0.5)).unwrap()
            )
            .unwrap()
            .to_decimal(),
            dec!(0.16191)
        );
    }

    #[test]
    fn reserve_rounds_up_and_budget_recheck_holds() {
        let schedule = CompactFeeSchedule::Taker { rate: dec!(0.0025) };
        let floor = Price::new(dec!(0.51)).unwrap();
        let ceiling = Price::new(dec!(0.75)).unwrap();
        assert_eq!(
            fee_reserve(
                schedule,
                CollateralAmount::from_decimal_exact(dec!(1)).unwrap(),
                ShareAmount::from_decimal_exact(dec!(1.333333)).unwrap(),
                floor,
                ceiling
            )
            .unwrap()
            .to_decimal(),
            dec!(0.00084)
        );

        let budget = CollateralAmount::from_decimal_exact(dec!(100)).unwrap();
        let principal = principal_for_budget(schedule, budget, floor, ceiling).unwrap();
        let shares = ShareAmount::from_decimal_exact(
            (principal.to_decimal() / ceiling.0)
                .round_dp_with_strategy(6, RoundingStrategy::ToZero),
        )
        .unwrap();
        let reserve = fee_reserve(schedule, principal, shares, floor, ceiling).unwrap();
        assert!(principal.checked_add(reserve).unwrap() <= budget);
        for price in [floor, ceiling] {
            let fee = taker_fee(schedule, shares, price).unwrap();
            assert!(fee <= reserve);
            assert!(principal.checked_add(fee).unwrap() <= budget);
        }

        assert_eq!(
            principal_for_budget(schedule, CollateralAmount::from_atomic(2), floor, ceiling)
                .unwrap(),
            CollateralAmount::ZERO
        );
    }

    #[test]
    fn zero_paths_and_finality_quantum_are_exact() {
        let floor = Price::new(dec!(0.4)).unwrap();
        let ceiling = Price::new(dec!(0.5)).unwrap();
        let budget = CollateralAmount::from_decimal_exact(dec!(10)).unwrap();
        assert_eq!(
            taker_fee(CompactFeeSchedule::Zero, ShareAmount::ZERO, floor).unwrap(),
            CollateralAmount::ZERO
        );
        assert_eq!(
            fee_reserve(
                CompactFeeSchedule::Zero,
                budget,
                ShareAmount::from_decimal_exact(dec!(20)).unwrap(),
                floor,
                ceiling
            )
            .unwrap(),
            CollateralAmount::ZERO
        );
        assert_eq!(
            principal_for_budget(CompactFeeSchedule::Zero, budget, floor, ceiling).unwrap(),
            budget
        );
        assert!(fee_within_reserve(
            CollateralAmount::from_atomic(20),
            CollateralAmount::from_atomic(30)
        ));
        assert!(!fee_within_reserve(
            CollateralAmount::from_atomic(21),
            CollateralAmount::from_atomic(30)
        ));
        assert!(matches!(
            fee_reserve(
                CompactFeeSchedule::Zero,
                budget,
                ShareAmount::from_decimal_exact(dec!(20)).unwrap(),
                ceiling,
                floor
            ),
            Err(FeeError::Band)
        ));
    }

    #[test]
    fn aggregate_fee_is_computed_once_not_per_level() {
        let schedule = CompactFeeSchedule::Taker { rate: dec!(0.04) };
        let price = Price::new(dec!(0.5)).unwrap();
        let aggregate = taker_fee(
            schedule,
            ShareAmount::from_decimal_exact(dec!(0.302)).unwrap(),
            price,
        )
        .unwrap();
        let per_level = taker_fee(
            schedule,
            ShareAmount::from_decimal_exact(dec!(0.1509)).unwrap(),
            price,
        )
        .unwrap()
        .checked_add(
            taker_fee(
                schedule,
                ShareAmount::from_decimal_exact(dec!(0.1511)).unwrap(),
                price,
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(aggregate.atomic(), 3_020);
        assert_eq!(per_level.atomic(), 3_010);
    }

    #[test]
    fn signed_price_is_the_only_fee_price() {
        let schedule = CompactFeeSchedule::Taker { rate: dec!(0.0025) };
        let shares = ShareAmount::from_decimal_exact(dec!(0.32)).unwrap();
        assert_eq!(
            taker_fee(schedule, shares, Price::new(dec!(0.4)).unwrap())
                .unwrap()
                .atomic(),
            190
        );
        assert_eq!(
            taker_fee(schedule, shares, Price::new(dec!(0.5)).unwrap())
                .unwrap()
                .atomic(),
            200
        );
    }
}
