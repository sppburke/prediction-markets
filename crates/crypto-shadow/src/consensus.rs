//! Cross-exchange consensus median + outsized-move detector — the observation
//! **trigger** for the shadow harness.
//!
//! Pure and deterministic: every clock value arrives as data (`observed_at_ms`),
//! so there is no `SystemTime::now()` here and the gate exercises the full
//! detection logic offline. The design follows the feed bake-off (`docs/27`):
//! detect on the **median across venues** (not a single trigger feed) so no one
//! venue's jitter or brief outage can fire or miss a move alone.

use std::collections::HashMap;
use std::collections::VecDeque;

use rust_decimal::Decimal;

use crate::types::{ExchangeVenue, MoveDirection, MoveEvent};

/// Tuning for the consensus move detector. Canonical defaults live in
/// `docs/_GLOSSARY.md` ("BTC shadow harness defaults").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusParams {
    /// Minimum number of venues with a price before a median is computed.
    pub min_venues: usize,
    /// Move size that triggers an observation, in basis points.
    pub threshold_bps: Decimal,
    /// Look-back window over which the move is measured, in milliseconds.
    pub window_ms: i64,
    /// Minimum gap between two fires, in milliseconds (debounce).
    pub cooldown_ms: i64,
}

/// Latest price per venue → median across the venues currently present.
#[derive(Debug, Clone)]
pub struct MedianTracker {
    min_venues: usize,
    latest: HashMap<ExchangeVenue, Decimal>,
}

impl MedianTracker {
    /// New tracker requiring at least `min_venues` (clamped to ≥1) before
    /// [`median`](Self::median) returns a value.
    pub fn new(min_venues: usize) -> Self {
        Self {
            min_venues: min_venues.max(1),
            latest: HashMap::new(),
        }
    }

    /// Record the latest price for one venue (overwrites the prior price).
    pub fn update(&mut self, venue: ExchangeVenue, price: Decimal) {
        self.latest.insert(venue, price);
    }

    /// Median of the present venues' latest prices, or `None` until at least
    /// `min_venues` have reported. Even counts average the two middle prices.
    ///
    /// # Precondition
    /// Returns `None` before `min_venues` distinct venues have been seen — a
    /// sentinel that fails safely (no median ⇒ no move ⇒ no observation).
    pub fn median(&self) -> Option<Decimal> {
        if self.latest.len() < self.min_venues {
            return None;
        }
        let mut prices: Vec<Decimal> = self.latest.values().copied().collect();
        prices.sort_unstable();
        let n = prices.len();
        let mid = n / 2;
        let median = if n % 2 == 1 {
            prices[mid]
        } else {
            (prices[mid - 1] + prices[mid]) / Decimal::from(2u32)
        };
        Some(median)
    }
}

/// Detects an outsized move on a stream of consensus medians. Stateful but pure:
/// callers feed `(observed_at_ms, median)` in source-clock order and receive a
/// [`MoveEvent`] when the median has shifted by ≥ `threshold_bps` versus the
/// median ≈`window_ms` ago, subject to a `cooldown_ms` debounce.
#[derive(Debug, Clone)]
pub struct MoveDetector {
    threshold_bps: Decimal,
    window_ms: i64,
    cooldown_ms: i64,
    /// `(observed_at_ms, median)` samples within the look-back window.
    samples: VecDeque<(i64, Decimal)>,
    last_fire_ms: Option<i64>,
}

impl MoveDetector {
    /// New detector from [`ConsensusParams`] (only the detection fields are used).
    pub fn new(params: ConsensusParams) -> Self {
        Self {
            threshold_bps: params.threshold_bps,
            window_ms: params.window_ms.max(0),
            cooldown_ms: params.cooldown_ms.max(0),
            samples: VecDeque::new(),
            last_fire_ms: None,
        }
    }

    /// Feed a consensus `median` observed at `t` (source clock, epoch ms).
    /// Returns a [`MoveEvent`] when an outsized move is detected and the cooldown
    /// has elapsed; otherwise `None`. Samples are retained regardless of the
    /// cooldown, so a sustained move re-fires once the cooldown lapses.
    ///
    /// # Precondition
    /// `t` is expected non-decreasing across calls (source-clock order). The
    /// reference is the oldest sample still inside `[t − window_ms, t]`, so out-
    /// of-order ticks only widen the reference window, never panic.
    pub fn observe(&mut self, t: i64, median: Decimal) -> Option<MoveEvent> {
        // Drop samples older than the look-back window.
        while let Some(&(ts, _)) = self.samples.front() {
            if t - ts > self.window_ms {
                self.samples.pop_front();
            } else {
                break;
            }
        }
        // Reference = oldest sample still in the window, captured *before* the
        // current sample is pushed (so the first-ever tick has no reference).
        let reference = self.samples.front().copied();
        self.samples.push_back((t, median));

        let (_, ref_price) = reference?;
        if ref_price.is_zero() {
            return None;
        }
        let change = median - ref_price;
        let bps = (change / ref_price) * Decimal::from(10_000u32);
        if bps.abs() < self.threshold_bps {
            return None;
        }
        if let Some(last) = self.last_fire_ms
            && t - last < self.cooldown_ms
        {
            return None;
        }
        self.last_fire_ms = Some(t);
        let direction = if change > Decimal::ZERO {
            MoveDirection::Up
        } else {
            MoveDirection::Down
        };
        Some(MoveEvent {
            direction,
            median_price: median,
            ref_price,
            magnitude_bps: bps.abs(),
            observed_at_ms: t,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn params() -> ConsensusParams {
        ConsensusParams {
            min_venues: 2,
            threshold_bps: dec!(3.0),
            window_ms: 300,
            cooldown_ms: 1000,
        }
    }

    #[test]
    fn median_needs_min_venues() {
        let mut t = MedianTracker::new(2);
        assert_eq!(t.median(), None);
        t.update(ExchangeVenue::Bybit, dec!(60000));
        assert_eq!(t.median(), None); // only one venue
        t.update(ExchangeVenue::Coinbase, dec!(60010));
        // two venues -> average of the two middle prices
        assert_eq!(t.median(), Some(dec!(60005)));
    }

    #[test]
    fn median_of_three_is_the_middle() {
        let mut t = MedianTracker::new(2);
        t.update(ExchangeVenue::Bybit, dec!(60020));
        t.update(ExchangeVenue::Okx, dec!(60000));
        t.update(ExchangeVenue::Coinbase, dec!(60010));
        assert_eq!(t.median(), Some(dec!(60010)));
    }

    #[test]
    fn latest_price_overwrites_per_venue() {
        let mut t = MedianTracker::new(1);
        t.update(ExchangeVenue::Bybit, dec!(60000));
        t.update(ExchangeVenue::Bybit, dec!(60100));
        assert_eq!(t.median(), Some(dec!(60100)));
    }

    #[test]
    fn flat_series_never_fires() {
        let mut d = MoveDetector::new(params());
        for t in (0..2000).step_by(50) {
            assert_eq!(d.observe(t, dec!(60000)), None, "flat must not fire at {t}");
        }
    }

    #[test]
    fn upward_jump_over_threshold_fires_once() {
        let mut d = MoveDetector::new(params());
        // Baseline samples within the window.
        assert_eq!(d.observe(0, dec!(60000)), None);
        assert_eq!(d.observe(100, dec!(60000)), None);
        // +4 bps in <300ms: 60000 -> 60024 is 4 bps. Threshold is 3 bps.
        let ev = d.observe(200, dec!(60024)).expect("should fire");
        assert_eq!(ev.direction, MoveDirection::Up);
        assert_eq!(ev.ref_price, dec!(60000));
        assert_eq!(ev.median_price, dec!(60024));
        assert_eq!(ev.magnitude_bps, dec!(4));
        // Cooldown (1000ms) suppresses an immediate re-fire on the next tick.
        assert_eq!(d.observe(250, dec!(60030)), None);
    }

    #[test]
    fn downward_jump_fires_down() {
        let mut d = MoveDetector::new(params());
        assert_eq!(d.observe(0, dec!(60000)), None);
        // -5 bps: 60000 -> 59970.
        let ev = d.observe(150, dec!(59970)).expect("should fire");
        assert_eq!(ev.direction, MoveDirection::Down);
        assert_eq!(ev.magnitude_bps, dec!(5));
    }

    #[test]
    fn sub_threshold_move_does_not_fire() {
        let mut d = MoveDetector::new(params());
        assert_eq!(d.observe(0, dec!(60000)), None);
        // +2 bps (60000 -> 60012) is below the 3 bps threshold.
        assert_eq!(d.observe(100, dec!(60012)), None);
    }

    #[test]
    fn slow_drift_beyond_window_does_not_fire() {
        // A 4 bps drift spread over > window_ms is not an outsized *fast* move:
        // the reference rolls forward, so the in-window delta stays small.
        let mut d = MoveDetector::new(params());
        let mut price = dec!(60000);
        let mut fired = false;
        for t in (0..3000).step_by(100) {
            // +0.1 bps per 100ms => ~3 bps per 3000ms, but < 3 bps in any 300ms.
            price += dec!(0.6); // 0.6/60000 = 0.1 bps
            if d.observe(t, price).is_some() {
                fired = true;
            }
        }
        assert!(!fired, "a drift slower than the window must not trigger");
    }

    #[test]
    fn cooldown_then_refire_on_continued_move() {
        let mut d = MoveDetector::new(params());
        assert_eq!(d.observe(0, dec!(60000)), None);
        assert!(d.observe(200, dec!(60030)).is_some()); // +5 bps, fires; last_fire=200
        assert_eq!(d.observe(300, dec!(60060)), None); // within cooldown
        // Establish a fresh in-window reference after the cooldown (the earlier
        // samples have aged out of the 300ms window), then move again.
        assert_eq!(d.observe(1100, dec!(60060)), None); // no in-window reference yet
        let ev = d.observe(1300, dec!(60120)); // +~10 bps vs the 1100 reference
        assert!(
            ev.is_some(),
            "should re-fire after cooldown on a fresh in-window move"
        );
    }

    #[test]
    fn zero_reference_is_safe() {
        let mut d = MoveDetector::new(params());
        assert_eq!(d.observe(0, dec!(0)), None);
        // Reference price is zero -> guarded, no division, no fire.
        assert_eq!(d.observe(100, dec!(60000)), None);
    }
}
