// Scenario: fractional Kelly sizing produces correct contract count for a known input
#![cfg(feature = "scenario")]
#![allow(clippy::expect_used)]
use pe_core_types::{Price, Probability};
use pe_kelly_sizer::{KELLY_NORMAL, KellyInput, size_contracts};
use rust_decimal_macros::dec;

#[test]
fn known_input_produces_correct_contract_count() {
    // PASS: p=0.60, c=0.50, kelly=0.25, bankroll=10000 → 1000 contracts
    // FAIL: any other count, or an error
    let input = KellyInput {
        p: Probability(dec!(0.60)),
        c: Price(dec!(0.50)),
        kelly_fraction: KELLY_NORMAL,
        bankroll: dec!(10000),
    };
    let result = size_contracts(&input).expect("valid input");
    assert_eq!(result.0, 1000u64);
}

#[test]
fn below_edge_returns_zero_contracts() {
    // PASS: p <= c returns 0 contracts, not an error
    let input = KellyInput {
        p: Probability(dec!(0.40)),
        c: Price(dec!(0.50)),
        kelly_fraction: KELLY_NORMAL,
        bankroll: dec!(10000),
    };
    let result = size_contracts(&input).expect("valid input even when edge is negative");
    assert_eq!(result.0, 0u64);
}
