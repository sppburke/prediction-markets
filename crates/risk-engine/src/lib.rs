//! Pure, deterministic risk gate. Takes a typed `RiskSnapshot` (no live queries) and returns
//! `RiskApproved` or a `RiskBlock` variant with the reason. No I/O, no network, deterministic
//! from the snapshot.

pub mod block;
pub mod engine;
pub mod snapshot;

pub use block::RiskBlock;
pub use engine::{RiskDecision, evaluate_risk};
pub use snapshot::{RiskSnapshot, TradingMode};

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

/// Clamp `contracts` so the notional cost stays within `cap_bps / 10_000` of `bankroll`.
///
/// Returns 0 when `cap_bps ≤ 0`, `bankroll ≤ 0`, or `price ≤ 0` (no trade can be sized).
/// When `available_bankroll < price`, returns 0 — no fractional contracts.
///
/// # Precondition
/// `price` must be the per-contract limit price (same value used in Kelly sizing).
pub fn clamp_contracts_to_cap(
    contracts: u64,
    price: Decimal,
    bankroll: Decimal,
    cap_bps: i32,
) -> u64 {
    if cap_bps <= 0 || bankroll <= Decimal::ZERO || price <= Decimal::ZERO {
        return 0;
    }
    let cap_usd = bankroll * Decimal::from(cap_bps) / Decimal::from(10_000i32);
    let max_contracts_dec = (cap_usd / price).floor();
    let max_contracts = max_contracts_dec.to_u64().unwrap_or(u64::MAX);
    contracts.min(max_contracts)
}

/// Clamp `contracts` so notional ≤ `take_fraction * liquidity_usd`.
///
/// Returns 0 when `fill_price ≤ 0` (no trade possible).
///
/// Returns `contracts` unchanged (passthrough) when:
///   - `liquidity_usd < min_required_usd` (covers both "missing from cache → caller
///     supplies `Decimal::ZERO`" and "below noise floor"; Gamma's `liquidity` field
///     is unreliable at small values),
///   - `take_fraction ≤ 0` (gate disabled).
///
/// See `docs/_GLOSSARY.md` `liquidity_take_fraction_default` and
/// `liquidity_min_required_usd_default` for canonical defaults.
///
/// # Precondition
/// `fill_price` must be the per-contract limit price used in Kelly sizing.
pub fn clamp_contracts_to_liquidity(
    contracts: u64,
    liquidity_usd: Decimal,
    take_fraction: Decimal,
    min_required_usd: Decimal,
    fill_price: Decimal,
) -> u64 {
    if fill_price <= Decimal::ZERO {
        return 0;
    }
    if take_fraction <= Decimal::ZERO || liquidity_usd < min_required_usd {
        return contracts;
    }
    let max_notional = liquidity_usd * take_fraction;
    let max_contracts_dec = (max_notional / fill_price).floor();
    let max_contracts = max_contracts_dec.to_u64().unwrap_or(u64::MAX);
    contracts.min(max_contracts)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests_liquidity_clamp {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn returns_zero_for_non_positive_fill_price() {
        // Highest-precedence early return: fill_price ≤ 0.
        assert_eq!(
            clamp_contracts_to_liquidity(100, dec!(5000), dec!(0.05), dec!(200), dec!(0)),
            0
        );
        assert_eq!(
            clamp_contracts_to_liquidity(100, dec!(5000), dec!(0.05), dec!(200), dec!(-0.01)),
            0
        );
    }

    #[test]
    fn passthrough_when_take_fraction_disabled() {
        // take_fraction = 0 → gate disabled, return original regardless of other values.
        assert_eq!(
            clamp_contracts_to_liquidity(123, dec!(5000), dec!(0), dec!(200), dec!(0.50)),
            123
        );
        // Even with liquidity below floor, gate-disabled wins precedence over below-floor.
        assert_eq!(
            clamp_contracts_to_liquidity(456, dec!(50), dec!(0), dec!(200), dec!(0.50)),
            456
        );
    }

    #[test]
    fn passthrough_when_liquidity_below_min_required() {
        // Below noise floor: passthrough.
        assert_eq!(
            clamp_contracts_to_liquidity(100, dec!(199.99), dec!(0.05), dec!(200), dec!(0.50)),
            100
        );
        // Zero liquidity (missing-from-cache sentinel): passthrough.
        assert_eq!(
            clamp_contracts_to_liquidity(100, dec!(0), dec!(0.05), dec!(200), dec!(0.50)),
            100
        );
    }

    #[test]
    fn normal_clamp_floors_division() {
        // 100 contracts proposed; liquidity=5000, take=0.05 → 250 USD allowed;
        // at fill=0.50 → 500 contracts allowed → clamp returns min(100, 500) = 100 (no reduction).
        assert_eq!(
            clamp_contracts_to_liquidity(100, dec!(5000), dec!(0.05), dec!(200), dec!(0.50)),
            100
        );
        // 1000 contracts proposed; same gate → 500 allowed → clamped to 500.
        assert_eq!(
            clamp_contracts_to_liquidity(1000, dec!(5000), dec!(0.05), dec!(200), dec!(0.50)),
            500
        );
        // Exact-value worked example from issue #128: liquidity=100, take=0.01, fill=0.50
        // → 100*0.01/0.50 = 2.0 → floor = 2.
        assert_eq!(
            clamp_contracts_to_liquidity(1000, dec!(100), dec!(0.01), dec!(50), dec!(0.50)),
            2
        );
    }

    #[test]
    fn full_kelly_take_fraction_uses_all_depth() {
        // take_fraction = 1.0 → clamp = floor(liquidity_usd / fill_price).
        // liquidity=1000, fill=0.25 → 4000 contracts allowed → no clamp at 100.
        assert_eq!(
            clamp_contracts_to_liquidity(100, dec!(1000), dec!(1.0), dec!(200), dec!(0.25)),
            100
        );
        // 10000 proposed → clamped to 4000.
        assert_eq!(
            clamp_contracts_to_liquidity(10_000, dec!(1000), dec!(1.0), dec!(200), dec!(0.25)),
            4000
        );
    }

    #[test]
    fn passthrough_when_proposed_is_already_below_clamp() {
        // 5 contracts proposed; clamp would allow 500 → return 5 unchanged.
        assert_eq!(
            clamp_contracts_to_liquidity(5, dec!(5000), dec!(0.05), dec!(200), dec!(0.50)),
            5
        );
    }
}
