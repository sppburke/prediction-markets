//! Event-level sign-randomization skill test (issue #212; SSRN 6617059).
//!
//! Under the null "the wallet has no skill", the win/loss *direction* of each
//! independent bet is a coin flip. Because bets within one event (a neg-risk
//! bundle) are correlated, the randomization unit is the **event**, not the
//! individual market: each event's signed PnL is flipped with probability 0.5,
//! and the permuted total PnL forms the null distribution. The observed total
//! PnL is then compared against that distribution.
//!
//! Deterministic: a fixed-seed `SplitMix64` draws one bit per (permutation,
//! event), so identical `(trades, event_map, permutations, seed)` inputs yield
//! identical results. All PnL arithmetic is `rust_decimal` (no `f64`).

use std::collections::HashMap;

use pe_trader_index::ClosedTrade;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

/// Outcome of the sign-randomization permutation test for one wallet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillTestResult {
    /// Observed total realized PnL (the test statistic), USD.
    pub observed_pnl: Decimal,
    /// One-sided permutation p-value × 10_000 (0..=10_000): the fraction of
    /// permutations whose null PnL is `≥ observed`, with the `+1` continuity
    /// correction `(count + 1) / (permutations + 1)`. Low ⇒ skill.
    pub pvalue_bps: u32,
    /// Permutation count actually used.
    pub permutations: u32,
}

/// Run the event-level sign-randomization test over a wallet's closed trades.
///
/// Groups `closed_trades` into events via `event_map` (orphans self-map to the
/// market id), flips each event's signed PnL with probability 0.5 per
/// permutation, and returns the one-sided permutation p-value of the observed
/// total PnL. `seed` makes the run reproducible.
///
/// # Precondition
/// `closed_trades` should be the train-window set (`closed_at ≤ cutoff`);
/// windowing is the caller's responsibility. With no closed trades the observed
/// PnL is `0` and the p-value is `1.0` (10_000 bps).
pub fn sign_randomization_test(
    closed_trades: &[ClosedTrade],
    event_map: &HashMap<String, String>,
    permutations: u32,
    seed: u64,
) -> SkillTestResult {
    // Aggregate signed PnL per event — the unit of randomization.
    let mut event_pnls: HashMap<&String, Decimal> = HashMap::new();
    for t in closed_trades {
        let market = &t.market_id.0.0;
        let event = event_map.get(market).unwrap_or(market);
        *event_pnls.entry(event).or_insert(Decimal::ZERO) += t.realized_pnl_usd;
    }
    let pnls: Vec<Decimal> = event_pnls.into_values().collect();
    let observed_pnl: Decimal = pnls.iter().copied().sum();

    if pnls.is_empty() || permutations == 0 {
        // No events (or no permutations requested): p = 1.0 by convention.
        return SkillTestResult {
            observed_pnl,
            pvalue_bps: 10_000,
            permutations,
        };
    }

    let mut rng = SplitMix64::new(seed);
    let mut at_least_as_extreme: u64 = 0;
    for _ in 0..permutations {
        let mut permuted = Decimal::ZERO;
        for &pnl in &pnls {
            // Even draw ⇒ flip this event's sign; odd ⇒ keep.
            if rng.next_u64() & 1 == 0 {
                permuted -= pnl;
            } else {
                permuted += pnl;
            }
        }
        if permuted >= observed_pnl {
            at_least_as_extreme += 1;
        }
    }

    // (count + 1) / (permutations + 1), as basis points (Phipson–Smyth correction).
    let numer = Decimal::from(at_least_as_extreme + 1) * Decimal::from(10_000u32);
    let denom = Decimal::from(u64::from(permutations) + 1);
    let pvalue_bps = (numer / denom)
        .round()
        .to_u32()
        .unwrap_or(10_000)
        .min(10_000);

    SkillTestResult {
        observed_pnl,
        pvalue_bps,
        permutations,
    }
}

/// Minimal deterministic PRNG (SplitMix64). Pure integer arithmetic — no `rand`
/// dependency, no `f64`. Seeded once; advances one `u64` per call.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use pe_core_types::{
        ContractQty, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId,
    };
    use rust_decimal_macros::dec;

    fn trade(market: &str, pnl: Decimal) -> ClosedTrade {
        ClosedTrade {
            market_id: MarketId(VenueMarketId(market.to_owned())),
            outcome_id: OutcomeId(0),
            side: Side::Buy,
            entry_price: Price::new(dec!(0.5)).unwrap(),
            exit_price: Price::new(dec!(1.0)).unwrap(),
            contracts: ContractQty(1),
            hold_duration_seconds: 60,
            realized_pnl_usd: pnl,
            opened_at_unix: 0,
            closed_at_unix: 1_000,
            source_trade_ids: vec![SourceTradeId("0xt".to_owned())],
        }
    }

    #[test]
    fn strong_positive_skill_has_low_pvalue() {
        // 12 distinct events, all positive PnL: the observed sum is the global
        // maximum, so only the all-"keep" permutation reaches it (p ≈ 2^-12).
        let events = HashMap::new();
        let trades: Vec<ClosedTrade> = (0..12)
            .map(|i| trade(&format!("0xm{i}"), dec!(10.0)))
            .collect();
        let r = sign_randomization_test(&trades, &events, 999, 42);
        assert_eq!(r.observed_pnl, dec!(120.0));
        assert!(
            r.pvalue_bps < 200,
            "expected p<0.02, got {} bps",
            r.pvalue_bps
        );
    }

    #[test]
    fn deterministic_for_fixed_seed() {
        let events = HashMap::new();
        let trades: Vec<ClosedTrade> = (0..8)
            .map(|i| trade(&format!("0xm{i}"), dec!(3.0)))
            .collect();
        let a = sign_randomization_test(&trades, &events, 500, 7);
        let b = sign_randomization_test(&trades, &events, 500, 7);
        assert_eq!(a, b, "same seed must reproduce identical result");
    }

    #[test]
    fn within_event_trades_share_one_sign() {
        // Two markets in one event with opposite PnL: they net to zero within
        // the event, so every permutation yields 0 == observed → p = 1.0.
        let mut events = HashMap::new();
        events.insert("0xm1".to_owned(), "evt".to_owned());
        events.insert("0xm2".to_owned(), "evt".to_owned());
        let trades = vec![trade("0xm1", dec!(5.0)), trade("0xm2", dec!(-5.0))];
        let r = sign_randomization_test(&trades, &events, 100, 1);
        assert_eq!(r.observed_pnl, dec!(0.0));
        assert_eq!(r.pvalue_bps, 10_000); // all 100 permutations: 0 >= 0
    }

    #[test]
    fn empty_trades_yield_pvalue_one() {
        let events = HashMap::new();
        let r = sign_randomization_test(&[], &events, 999, 42);
        assert_eq!(r.observed_pnl, dec!(0));
        assert_eq!(r.pvalue_bps, 10_000);
    }
}
