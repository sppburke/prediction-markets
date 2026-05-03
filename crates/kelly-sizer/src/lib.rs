//! Fractional-Kelly contract sizing.
//!
//! Pure, deterministic function — no I/O, no network.
//!
//! # Formula
//! ```text
//! b         = (1 - c) / c            // odds ratio
//! f_full    = (p - c) / (1 - c)      // full Kelly fraction
//! f_live    = max(0, f_full) * kelly_fraction   // scaled, clamped >= 0
//! contracts = floor(bankroll * f_live / c)      // whole contracts only
//! ```

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;

use pe_core_types::{ContractQty, KellyFraction, Price, Probability};

// ── Public constants ────────────────────────────────────────────────────────

use rust_decimal_macros::dec;

pub const KELLY_NORMAL: KellyFraction = KellyFraction(dec!(0.25));
pub const KELLY_INHERITED_PRIOR: KellyFraction = KellyFraction(dec!(0.05));
pub const KELLY_CLUSTER_COORDINATION: KellyFraction = KellyFraction(dec!(0.15));
pub const KELLY_PAPER_BACKTEST: KellyFraction = KellyFraction(dec!(0.10));
pub const KELLY_HARD_MAX: KellyFraction = KellyFraction(dec!(0.50));

// ── Error ───────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum KellyError {
    #[error("net price c must be in (0, 1), got {value}")]
    InvalidNetPrice { value: Decimal },
    #[error("probability p must be in [0, 1], got {value}")]
    InvalidProbability { value: Decimal },
    #[error("bankroll must be positive, got {value}")]
    InvalidBankroll { value: Decimal },
    #[error("contract count overflow: floored value {value} out of u64 range")]
    ContractOverflow { value: Decimal },
}

// ── Input ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct KellyInput {
    /// Estimated win probability (0..1).
    pub p: Probability,
    /// Net price after fees + slippage + adverse-selection buffer (0..1). Caller computes.
    pub c: Price,
    /// Kelly fraction to apply (configured per mode).
    pub kelly_fraction: KellyFraction,
    /// Current bankroll in USD.
    pub bankroll: Decimal,
}

// ── Core function ───────────────────────────────────────────────────────────

/// Compute the number of whole contracts to buy given the Kelly inputs.
///
/// Returns `ContractQty(0)` when the edge is zero or negative — that is *not*
/// an error; it simply means "don't trade".  An `Err` is returned only for
/// invalid inputs.
pub fn size_contracts(input: &KellyInput) -> Result<ContractQty, KellyError> {
    let p = input.p.0;
    let c = input.c.0;
    let kf = input.kelly_fraction.0;
    let bankroll = input.bankroll;

    // ── Validate ──────────────────────────────────────────────────────────
    if p < Decimal::ZERO || p > Decimal::ONE {
        return Err(KellyError::InvalidProbability { value: p });
    }
    if c <= Decimal::ZERO || c >= Decimal::ONE {
        return Err(KellyError::InvalidNetPrice { value: c });
    }
    if bankroll <= Decimal::ZERO {
        return Err(KellyError::InvalidBankroll { value: bankroll });
    }

    // ── Kelly formula ─────────────────────────────────────────────────────
    // f_full = (p - c) / (1 - c)
    let one = Decimal::ONE;
    let f_full = (p - c) / (one - c);

    // f_live = max(0, f_full) * kelly_fraction
    let f_live = if f_full <= Decimal::ZERO {
        Decimal::ZERO
    } else {
        f_full * kf
    };

    // Short-circuit: no edge → 0 contracts, no division needed.
    if f_live <= Decimal::ZERO {
        return Ok(ContractQty(0));
    }

    // contracts = floor(bankroll * f_live / c)
    let raw = bankroll * f_live / c;
    let floored = raw.floor();

    // Guard against negative result (shouldn't happen given validated inputs,
    // but be defensive) and u64 overflow.
    if floored < Decimal::ZERO || floored > Decimal::from(u64::MAX) {
        return Err(KellyError::ContractOverflow { value: floored });
    }

    let contracts = floored
        .to_u64()
        .ok_or(KellyError::ContractOverflow { value: floored })?;

    Ok(ContractQty(contracts))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use rust_decimal_macros::dec;

    fn make_input(p: Decimal, c: Decimal, kf: KellyFraction, bankroll: Decimal) -> KellyInput {
        KellyInput {
            p: Probability(p),
            c: Price(c),
            kelly_fraction: kf,
            bankroll,
        }
    }

    #[test]
    fn basic_case() {
        // p=0.60, c=0.50, kelly=0.25, bankroll=10000
        // f_full = (0.60 - 0.50) / (1 - 0.50) = 0.10 / 0.50 = 0.20
        // f_live = 0.20 * 0.25 = 0.05
        // contracts = floor(10000 * 0.05 / 0.50) = floor(1000) = 1000
        let input = make_input(dec!(0.60), dec!(0.50), KELLY_NORMAL, dec!(10000));
        let result = size_contracts(&input).unwrap();
        assert_eq!(result.0, 1000u64);
    }

    #[test]
    fn negative_edge_returns_zero() {
        // p=0.40, c=0.50 → f_full < 0 → f_live = 0 → 0 contracts (not error)
        let input = make_input(dec!(0.40), dec!(0.50), KELLY_NORMAL, dec!(10000));
        let result = size_contracts(&input).unwrap();
        assert_eq!(result.0, 0u64);
    }

    #[test]
    fn zero_edge_returns_zero() {
        // p == c → f_full = 0 exactly
        let input = make_input(dec!(0.50), dec!(0.50), KELLY_NORMAL, dec!(10000));
        let result = size_contracts(&input).unwrap();
        assert_eq!(result.0, 0u64);
    }

    #[test]
    fn invalid_net_price_zero() {
        let input = make_input(dec!(0.60), dec!(0), KELLY_NORMAL, dec!(10000));
        assert!(matches!(
            size_contracts(&input),
            Err(KellyError::InvalidNetPrice { .. })
        ));
    }

    #[test]
    fn invalid_net_price_one() {
        let input = make_input(dec!(0.60), dec!(1), KELLY_NORMAL, dec!(10000));
        assert!(matches!(
            size_contracts(&input),
            Err(KellyError::InvalidNetPrice { .. })
        ));
    }

    #[test]
    fn negative_bankroll() {
        let input = make_input(dec!(0.60), dec!(0.50), KELLY_NORMAL, dec!(-1000));
        assert!(matches!(
            size_contracts(&input),
            Err(KellyError::InvalidBankroll { .. })
        ));
    }

    #[test]
    fn invalid_probability_above_one() {
        let input = make_input(dec!(1.10), dec!(0.50), KELLY_NORMAL, dec!(10000));
        assert!(matches!(
            size_contracts(&input),
            Err(KellyError::InvalidProbability { .. })
        ));
    }

    #[test]
    fn invalid_probability_negative() {
        let input = make_input(dec!(-0.10), dec!(0.50), KELLY_NORMAL, dec!(10000));
        assert!(matches!(
            size_contracts(&input),
            Err(KellyError::InvalidProbability { .. })
        ));
    }

    proptest::proptest! {
        #[test]
        fn contracts_never_negative(
            p_raw in 0u32..=1_000_000u32,
            c_raw in 1u32..=999_999u32,
            bankroll_raw in 1u64..=1_000_000u64,
        ) {
            let p_dec = Decimal::from(p_raw) / Decimal::from(1_000_000u32);
            let c_dec = Decimal::from(c_raw) / Decimal::from(1_000_000u32);
            let bankroll = Decimal::from(bankroll_raw);
            let input = KellyInput {
                p: Probability(p_dec),
                c: Price(c_dec),
                kelly_fraction: KELLY_NORMAL,
                bankroll,
            };
            if let Ok(qty) = size_contracts(&input) {
                // ContractQty is u64, always >= 0 — just assert no panic
                let _ = qty;
            }
        }
    }
}
