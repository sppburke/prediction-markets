//! Exact compact-market fee economics (#545).

use pe_core_types::{CollateralAmount, Price, ShareAmount};
use polymarket_client_sdk_v2::clob::types::response::ClobMarketInfoResponse;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

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

/// Classify the compact market's `fd` / `mbf` / `tbf` fields.
pub fn compact_fee_schedule(
    info: &ClobMarketInfoResponse,
) -> Result<CompactFeeSchedule, FeeScheduleError> {
    let maker_base = info.maker_base_fee.unwrap_or(Decimal::ZERO);
    if maker_base != Decimal::ZERO {
        return Err(FeeScheduleError::MakerFee);
    }

    let taker_base = info.taker_base_fee.unwrap_or(Decimal::ZERO);
    let Some(details) = info.fee_details.as_ref() else {
        return if taker_base == Decimal::ZERO {
            Ok(CompactFeeSchedule::Zero)
        } else {
            Err(FeeScheduleError::UnknownBase)
        };
    };

    if taker_base != Decimal::ZERO {
        return Err(FeeScheduleError::Contradictory);
    }
    if !details.taker_only {
        return Err(FeeScheduleError::MakerFee);
    }
    if details.exponent != 1 {
        return Err(FeeScheduleError::Exponent);
    }
    if details.rate <= Decimal::ZERO || details.rate >= Decimal::ONE || details.rate.scale() > 6 {
        return Err(FeeScheduleError::Unrepresentable);
    }

    Ok(CompactFeeSchedule::Taker { rate: details.rate })
}

/// Deserialize a compact response into the vendored SDK wire DTO, then validate its fee fields.
pub fn parse_compact_fee_schedule(bytes: &[u8]) -> Result<CompactFeeSchedule, FeeScheduleError> {
    let mut raw: Value = serde_json::from_slice(bytes).map_err(|_| FeeScheduleError::Malformed)?;
    if let Some(details) = raw
        .as_object_mut()
        .ok_or(FeeScheduleError::Malformed)?
        .get_mut("fd")
        && !details.is_null()
    {
        normalize_fee_details(details)?;
    }
    let info: ClobMarketInfoResponse =
        serde_json::from_value(raw).map_err(|_| FeeScheduleError::Malformed)?;
    compact_fee_schedule(&info)
}

fn normalize_fee_details(details: &mut Value) -> Result<(), FeeScheduleError> {
    let fields = details.as_object_mut().ok_or(FeeScheduleError::Malformed)?;
    let rate = take_alias(fields, &["r", "rate"])?;
    let exponent = take_alias(fields, &["e", "exponent"])?;
    let taker_only = take_alias(fields, &["to", "taker_only", "takerOnly"])?;
    if !fields.is_empty() {
        return Err(FeeScheduleError::Malformed);
    }
    if let Some(value) = rate {
        fields.insert("r".to_owned(), value);
    }
    if let Some(value) = exponent {
        fields.insert("e".to_owned(), value);
    }
    if let Some(value) = taker_only {
        fields.insert("to".to_owned(), value);
    }
    Ok(())
}

fn take_alias(
    fields: &mut Map<String, Value>,
    aliases: &[&str],
) -> Result<Option<Value>, FeeScheduleError> {
    let mut selected = None;
    for alias in aliases {
        let Some(value) = fields.remove(*alias) else {
            continue;
        };
        if selected.as_ref().is_some_and(|current| current != &value) {
            return Err(FeeScheduleError::Contradictory);
        }
        selected = Some(value);
    }
    Ok(selected)
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
    let CompactFeeSchedule::Taker { rate } = schedule else {
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

/// Greatest fee reachable for `principal` over the inclusive signed-price band.
pub fn fee_reserve(
    schedule: CompactFeeSchedule,
    principal: CollateralAmount,
    floor: Price,
    ceiling: Price,
) -> Result<CollateralAmount, FeeError> {
    validate_band(floor, ceiling)?;
    let CompactFeeSchedule::Taker { rate } = schedule else {
        return Ok(CollateralAmount::ZERO);
    };
    let reserve = principal
        .to_decimal()
        .checked_mul(rate)
        .and_then(|value| value.checked_mul(Decimal::ONE.checked_sub(floor.0)?))
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
    let CompactFeeSchedule::Taker { rate } = schedule else {
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

fn all_in_reserve(
    schedule: CompactFeeSchedule,
    principal: CollateralAmount,
    floor: Price,
    ceiling: Price,
) -> Result<CollateralAmount, FeeError> {
    principal
        .checked_add(fee_reserve(schedule, principal, floor, ceiling)?)
        .map_err(|_| FeeError::Arithmetic)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use rust_decimal_macros::dec;
    use serde_json::json;

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
            json!({"fd": {"r": 0.0025, "e": 1, "to": true}}),
            json!({"fd": {"rate": 0.0025, "exponent": 1, "taker_only": true}}),
            json!({"fd": {"rate": 0.0025, "exponent": 1, "takerOnly": true}}),
            json!({"fd": {"r": 0.0025, "e": 1, "to": true}, "mbf": 0, "tbf": 0}),
        ] {
            assert_eq!(
                parse_compact_fee_schedule(&compact(fields)).unwrap(),
                CompactFeeSchedule::Taker { rate: dec!(0.0025) }
            );
        }
    }

    #[test]
    fn rejects_every_invalid_compact_fee_class() {
        let cases = [
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
                json!({"fd": {"r": 0, "e": 1, "to": true}}),
                FeeScheduleError::Unrepresentable,
            ),
            (
                json!({"fd": {"r": 1, "e": 1, "to": true}}),
                FeeScheduleError::Unrepresentable,
            ),
            (
                json!({"fd": {"r": "0.0000001", "e": 1, "to": true}}),
                FeeScheduleError::Unrepresentable,
            ),
            (
                json!({"fd": {"r": 0.0025, "e": 1, "to": true}, "tbf": 1}),
                FeeScheduleError::Contradictory,
            ),
            (
                json!({"fd": {"r": 0.0025, "rate": 0.003, "e": 1, "to": true}}),
                FeeScheduleError::Contradictory,
            ),
            (
                json!({"fd": {"r": 0.0025, "e": 1, "to": true, "unknown": 1}}),
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
                floor,
                ceiling
            )
            .unwrap()
            .to_decimal(),
            dec!(0.00123)
        );

        let budget = CollateralAmount::from_decimal_exact(dec!(100)).unwrap();
        let principal = principal_for_budget(schedule, budget, floor, ceiling).unwrap();
        let reserve = fee_reserve(schedule, principal, floor, ceiling).unwrap();
        assert!(principal.checked_add(reserve).unwrap() <= budget);
        for price in [floor, ceiling] {
            let shares = ShareAmount::from_decimal_exact(
                (principal.to_decimal() / price.0)
                    .round_dp_with_strategy(6, RoundingStrategy::ToZero),
            )
            .unwrap();
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
            fee_reserve(CompactFeeSchedule::Zero, budget, floor, ceiling).unwrap(),
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
            fee_reserve(CompactFeeSchedule::Zero, budget, ceiling, floor),
            Err(FeeError::Band)
        ));
    }
}
