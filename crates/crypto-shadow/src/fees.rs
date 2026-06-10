//! Polymarket `crypto_fees_v2` taker fee — the single source of truth for the
//! harness's net-edge computation.
//!
//! **VERIFIED 2026-06-08** against Polymarket's official fee docs
//! (<https://docs.polymarket.com/trading/fees>): the per-share taker fee is
//!
//! ```text
//! fee = shares * rate * p * (p * (1 - p))^exponent
//! ```
//!
//! which for the Crypto category (`rate = 0.07`, `exponent = 1`) collapses to a
//! per-share fee of `rate * p * (1 - p)`. This reproduces Polymarket's published
//! fee table to the cent (100 shares @ p = 0.50 -> $1.75).
//!
//! The three candidate formulas the design phase proposed (`rate*p`,
//! `rate*(1-p)`, `rate*min(p,1-p)`) were **all wrong** — measuring first
//! resolved the ambiguity before any execution work.
//!
//! Caveats encoded in [`CRYPTO_FEES_V2_PROVENANCE`]:
//! - `rate = 0.07` is the per-market `feeSchedule.rate`; the maker-rebates page
//!   lists 0.072 (pooled/stale) — the per-market value governs.
//! - Taker-only: makers pay nothing. The sell-side taker fee is ambiguous
//!   between two official Polymarket sources, so this models the **entry**
//!   (taker buy) fee only; round-trip cost is flagged in the report provenance.

use rust_decimal::Decimal;

/// Crypto-category taker fee rate (`feeSchedule.rate` = 0.07).
/// See `docs/_GLOSSARY.md`: `crypto_fees_v2_rate`.
pub fn crypto_fees_v2_rate() -> Decimal {
    // 7 * 10^-2 = 0.07. `Decimal::new` avoids `f64` and the macro crate.
    Decimal::new(7, 2)
}

/// Per-share Crypto taker fee at YES transaction price `price`:
/// `rate * p * (1 - p)` (exponent = 1). Returns `0` outside the open interval
/// `(0, 1)` where no taker fee applies.
pub fn taker_fee_per_share(price: Decimal) -> Decimal {
    if price <= Decimal::ZERO || price >= Decimal::ONE {
        return Decimal::ZERO;
    }
    crypto_fees_v2_rate() * price * (Decimal::ONE - price)
}

/// Maker rebate rate for the Crypto category: the per-market Gamma
/// `feeSchedule.rebateRate`.
///
/// **VERIFIED LIVE 2026-06-09** on a `btc-up-or-down-5m` market:
/// `feeSchedule = {"exponent": 1, "rate": 0.07, "takerOnly": true,
/// "rebateRate": 0.2}` (`feeType = crypto_fees_v2`). The per-market value
/// governs over the Polymarket maker-rebates docs page, per the 0.072-vs-0.07
/// precedent above; the docs page is cited only for the **pool structure**
/// (rebates are a daily pro-rata, liquidity-weighted pool of collected fees,
/// not a per-fill credit). See `docs/_GLOSSARY.md`: `maker_rebate_rate`.
pub fn maker_rebate_rate() -> Decimal {
    // 20 * 10^-2 = 0.20.
    Decimal::new(20, 2)
}

/// Per-share maker rebate at fill price `price`:
/// `maker_rebate_rate() * taker_fee_per_share(price)`.
///
/// An **upper-bound idealization** of the real rebate (issue #310): the live
/// pool is daily pro-rata and liquidity-weighted across all makers, so a
/// per-fill credit of the full 20% of the fill's taker-fee equivalent is a
/// ceiling, never an expectation. Stamped as such in the sweep output.
pub fn maker_rebate_per_share(price: Decimal) -> Decimal {
    maker_rebate_rate() * taker_fee_per_share(price)
}

/// Provenance string stamped into `meta` and the `report` header so a recompute
/// under a corrected fee is unambiguous and self-documenting.
pub const CRYPTO_FEES_V2_PROVENANCE: &str = "crypto_fees_v2: fee_per_share = 0.07*p*(1-p) (exponent=1); \
verified 2026-06-08 docs.polymarket.com/trading/fees; entry/taker-buy only; sell-side ambiguous";

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn fee_matches_published_table_at_anchor_prices() {
        // Values verified against docs.polymarket.com/trading/fees (per share).
        assert_eq!(taker_fee_per_share(dec!(0.50)), dec!(0.0175));
        assert_eq!(taker_fee_per_share(dec!(0.45)), dec!(0.017325));
        assert_eq!(taker_fee_per_share(dec!(0.30)), dec!(0.0147));
        assert_eq!(taker_fee_per_share(dec!(0.70)), dec!(0.0147));
    }

    #[test]
    fn fee_is_zero_outside_open_interval() {
        assert_eq!(taker_fee_per_share(dec!(0)), Decimal::ZERO);
        assert_eq!(taker_fee_per_share(dec!(1)), Decimal::ZERO);
        assert_eq!(taker_fee_per_share(dec!(-0.1)), Decimal::ZERO);
        assert_eq!(taker_fee_per_share(dec!(1.5)), Decimal::ZERO);
    }

    #[test]
    fn fee_is_dollar_symmetric_around_half() {
        assert_eq!(
            taker_fee_per_share(dec!(0.30)),
            taker_fee_per_share(dec!(0.70))
        );
        assert_eq!(
            taker_fee_per_share(dec!(0.10)),
            taker_fee_per_share(dec!(0.90))
        );
    }

    #[test]
    fn rate_is_seven_percent() {
        assert_eq!(crypto_fees_v2_rate(), dec!(0.07));
    }

    #[test]
    fn maker_rebate_is_a_fifth_of_the_taker_fee() {
        assert_eq!(maker_rebate_rate(), dec!(0.20));
        // 0.20 * 0.07 * 0.48 * 0.52 = 0.00349440
        assert_eq!(maker_rebate_per_share(dec!(0.48)), dec!(0.0034944));
        assert_eq!(maker_rebate_per_share(dec!(0)), Decimal::ZERO);
    }
}
