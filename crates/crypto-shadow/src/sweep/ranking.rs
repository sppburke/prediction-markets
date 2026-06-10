//! Fixed venue ranking for the sweep's `top_n_venues` axis. No config surface
//! in v1 (issue #310). Canonical: `docs/_GLOSSARY.md`
//! `crypto_shadow_sweep_venue_priority`.

use crate::types::ExchangeVenue;

/// Priority order: coinbase (USD-quoted; bybit/okx carry a ~5 bps USDT basis)
/// then okx then bybit (freshness per the `docs/27` bake-off).
pub(super) const VENUE_PRIORITY: [ExchangeVenue; 3] = [
    ExchangeVenue::Coinbase,
    ExchangeVenue::Okx,
    ExchangeVenue::Bybit,
];

/// The venues whose ticks a `top_n = n` cell feeds into the join state.
pub(super) fn venues_for_top_n(n: usize) -> &'static [ExchangeVenue] {
    &VENUE_PRIORITY[..n.min(VENUE_PRIORITY.len())]
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn top_n_slices_in_priority_order() {
        assert_eq!(
            venues_for_top_n(2),
            &[ExchangeVenue::Coinbase, ExchangeVenue::Okx]
        );
        assert_eq!(venues_for_top_n(3), &VENUE_PRIORITY);
        // Out-of-range n clamps to the full pool rather than panicking.
        assert_eq!(venues_for_top_n(9), &VENUE_PRIORITY);
    }
}
