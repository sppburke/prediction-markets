//! Feed-join + edge computation. Pure and deterministic: no clock, no I/O — all
//! clock values arrive as data on the inbound tick. This is the gate-critical
//! core exercised by unit tests and the offline scenario test.
//!
//! The observation **trigger** is an outsized move on the exchange-consensus
//! median ([`crate::consensus`]): exchange trade ticks update the per-venue
//! median; when the [`MoveDetector`] fires, one observation is emitted per
//! active market, pricing the move's signal direction against the latest book
//! net of the verified `crypto_fees_v2` taker fee. The Chainlink feed is the
//! deferred settlement reader and does not drive observations here (issue #300
//! Phase 2 / AC2.3).

use std::cmp::Ordering;
use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::consensus::{ConsensusParams, MedianTracker, MoveDetector};
use crate::fees::taker_fee_per_share;
use crate::types::{BookUpdate, BtcMarketMeta, EdgeObservation, ExchangeTick, MoveDirection};

/// Binary indicator P(up): `1` if `c > r`, `0.5` on a tie, `0` if `c < r`.
fn indicator_prob_up(c: Decimal, r: Decimal) -> Decimal {
    match c.cmp(&r) {
        Ordering::Greater => Decimal::ONE,
        Ordering::Equal => Decimal::new(5, 1), // 0.5
        Ordering::Less => Decimal::ZERO,
    }
}

/// Compute one [`EdgeObservation`] for a market given the triggering consensus
/// median `c` observed at `observed_at_ms` and received at the node at
/// `signal_received_ms`, the captured window-open reference `range_start` (if
/// known), the latest `book` paired with its own node-receive clock (if seen),
/// and the magnitude/direction of the triggering move. Pure — the receive clocks
/// arrive as data, so no clock is read here.
#[allow(clippy::too_many_arguments)]
pub fn compute_observation(
    meta: &BtcMarketMeta,
    c: Decimal,
    observed_at_ms: i64,
    signal_received_ms: i64,
    range_start: Option<Decimal>,
    book: Option<(&BookUpdate, i64)>,
    move_magnitude_bps: Decimal,
    move_direction: MoveDirection,
) -> EdgeObservation {
    let prob_up = range_start.map(|r| indicator_prob_up(c, r));
    let best_ask = book.and_then(|(b, _)| b.best_ask).map(|p| p.0);
    let mid = book.and_then(|(b, _)| b.mid());
    // Book staleness at the node: how long ago the latest book was received,
    // relative to this triggering median tick's receipt. One coherent
    // at-the-node clock for both feeds. `None` only when no book has been seen.
    let feed_to_book_lag_ms =
        book.map(|(_, book_received_ms)| signal_received_ms - book_received_ms);

    // Gross edge of buying YES = P(up) - price paid. Only defined when both the
    // probability indicator and the relevant price are available.
    let gross_edge_vs_ask = match (prob_up, best_ask) {
        (Some(p), Some(ask)) => Some(p - ask),
        _ => None,
    };
    let gross_edge_vs_mid = match (prob_up, mid) {
        (Some(p), Some(m)) => Some(p - m),
        _ => None,
    };

    // Fee is the entry taker fee at the price actually transacted (the ask for a
    // marketable buy); for the mid leg we use the mid as the price proxy.
    let fee_cost = best_ask.map(taker_fee_per_share);
    let net_edge_vs_ask = match (gross_edge_vs_ask, fee_cost) {
        (Some(g), Some(f)) => Some(g - f),
        _ => None,
    };
    let net_edge_vs_mid = match (gross_edge_vs_mid, mid) {
        (Some(g), Some(m)) => Some(g - taker_fee_per_share(m)),
        _ => None,
    };

    EdgeObservation {
        condition_id: meta.condition_id.clone(),
        series: meta.series,
        observed_at_ms,
        signal_value: c,
        range_start_value: range_start,
        instantaneous_prob_up: prob_up,
        best_ask,
        mid,
        gross_edge_vs_ask,
        gross_edge_vs_mid,
        fee_cost,
        net_edge_vs_ask,
        net_edge_vs_mid,
        feed_to_book_lag_ms,
        move_magnitude_bps,
        move_direction,
    }
}

/// The latest book for a market paired with the node-receive clock at which the
/// runner read it. Kept alongside the book so the join can compute a coherent
/// at-the-node `feed_to_book_lag_ms` without [`BookUpdate`] carrying a clock it
/// would be trusted to stamp post-hoc (which an offline `raw_ticks` recompute
/// would ship as a placeholder).
#[derive(Debug, Clone)]
struct BookEntry {
    book: BookUpdate,
    received_ms: i64,
}

/// Stateful join across the feeds. Holds market metadata, the latest book per
/// market, the captured window-open reference per market, and the consensus
/// median tracker + move detector that gate observation emission.
///
/// The consensus median is shared across all active BTC markets, so one detected
/// move produces one observation per active market.
pub struct JoinState {
    markets: HashMap<String, BtcMarketMeta>,
    token_to_condition: HashMap<String, String>,
    latest_book: HashMap<String, BookEntry>,
    range_start_value: HashMap<String, Decimal>,
    median: MedianTracker,
    detector: MoveDetector,
}

impl JoinState {
    /// Build a join state over the given markets with the consensus tuning.
    pub fn new(markets: Vec<BtcMarketMeta>, params: ConsensusParams) -> Self {
        let mut state = Self {
            markets: HashMap::new(),
            token_to_condition: HashMap::new(),
            latest_book: HashMap::new(),
            range_start_value: HashMap::new(),
            median: MedianTracker::new(params.min_venues),
            detector: MoveDetector::new(params),
        };
        for m in markets {
            state.upsert_market(m);
        }
        state
    }

    /// Add or replace a market (used on startup and on periodic refresh).
    pub fn upsert_market(&mut self, meta: BtcMarketMeta) {
        self.token_to_condition
            .insert(meta.yes_token_id.clone(), meta.condition_id.clone());
        self.markets.insert(meta.condition_id.clone(), meta);
    }

    /// Number of tracked markets.
    pub fn market_count(&self) -> usize {
        self.markets.len()
    }

    /// Apply a book update with the node-receive clock at which the runner read
    /// it, keyed by the market whose YES token it belongs to. Unknown tokens are
    /// ignored (book for a market we are not tracking).
    pub fn on_book_update(&mut self, update: BookUpdate, received_ms: i64) {
        if let Some(condition) = self.token_to_condition.get(&update.token_id).cloned() {
            self.latest_book.insert(
                condition,
                BookEntry {
                    book: update,
                    received_ms,
                },
            );
        }
    }

    /// Apply an exchange trade tick: update the consensus median, capture the
    /// window-open reference for any market whose window has opened, and — only
    /// when the move detector fires — emit one observation per market currently
    /// inside its `[range_start, range_end]` window.
    ///
    /// Returns an empty vec on every tick that does not complete a detected move
    /// (the common case): observations are **sparse**, tied to outsized moves.
    pub fn on_exchange_tick(
        &mut self,
        tick: &ExchangeTick,
        received_ms: i64,
    ) -> Vec<EdgeObservation> {
        self.median.update(tick.venue, tick.price);
        let Some(median) = self.median.median() else {
            return Vec::new();
        };
        // Window membership and reference capture use the *source* clock `t`,
        // never `received_ms` (which feeds only the lag stat) — so the set of
        // in-window ticks is source-clock-defined and cannot drift.
        let t = tick.observed_at_ms;

        // Collect active markets and capture each one's window-open reference
        // (the first in-window median), independent of whether a move fires.
        let mut active: Vec<String> = Vec::new();
        for (condition, meta) in &self.markets {
            if t < meta.range_start_ms || t > meta.range_end_ms {
                continue;
            }
            active.push(condition.clone());
        }
        for condition in &active {
            self.range_start_value
                .entry(condition.clone())
                .or_insert(median);
        }

        // Gate emission on a detected outsized move in the consensus median.
        let Some(mv) = self.detector.observe(t, median) else {
            return Vec::new();
        };

        let mut out = Vec::with_capacity(active.len());
        for condition in active {
            let Some(meta) = self.markets.get(&condition) else {
                continue;
            };
            let range_start = self.range_start_value.get(&condition).copied();
            let book = self
                .latest_book
                .get(&condition)
                .map(|e| (&e.book, e.received_ms));
            out.push(compute_observation(
                meta,
                median,
                t,
                received_ms,
                range_start,
                book,
                mv.magnitude_bps,
                mv.direction,
            ));
        }
        out
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::types::{BtcSeriesKind, ExchangeVenue};
    use pe_core_types::Price;
    use rust_decimal_macros::dec;

    fn params() -> ConsensusParams {
        ConsensusParams {
            min_venues: 2,
            threshold_bps: dec!(3.0),
            window_ms: 300,
            cooldown_ms: 1000,
        }
    }

    fn meta() -> BtcMarketMeta {
        BtcMarketMeta {
            condition_id: "0xcond".to_string(),
            yes_token_id: "tok-yes".to_string(),
            series: BtcSeriesKind::Five,
            range_start_ms: 1_000,
            range_end_ms: 301_000,
            tick: dec!(0.01),
        }
    }

    fn book(bid: &str, ask: &str, ts: i64) -> BookUpdate {
        BookUpdate {
            token_id: "tok-yes".to_string(),
            best_bid: Some(Price(bid.parse().unwrap())),
            best_ask: Some(Price(ask.parse().unwrap())),
            observed_at_ms: Some(ts),
        }
    }

    fn etick(venue: ExchangeVenue, price: Decimal, t: i64) -> ExchangeTick {
        ExchangeTick {
            venue,
            price,
            observed_at_ms: t,
        }
    }

    #[test]
    fn indicator_handles_up_down_tie() {
        assert_eq!(indicator_prob_up(dec!(101), dec!(100)), Decimal::ONE);
        assert_eq!(indicator_prob_up(dec!(100), dec!(100)), dec!(0.5));
        assert_eq!(indicator_prob_up(dec!(99), dec!(100)), Decimal::ZERO);
    }

    #[test]
    fn net_edge_subtracts_verified_fee() {
        let m = meta();
        let b = book("0.48", "0.52", 2_000);
        // c > r => prob_up = 1; gross vs ask = 1 - 0.52 = 0.48; fee at 0.52.
        // Lag = signal_received_ms - book_received_ms = 2_500 - 2_000 = 500.
        let obs = compute_observation(
            &m,
            dec!(60000),
            2_500,
            2_500,
            Some(dec!(59000)),
            Some((&b, 2_000)),
            dec!(4),
            MoveDirection::Up,
        );
        assert_eq!(obs.signal_value, dec!(60000));
        assert_eq!(obs.instantaneous_prob_up, Some(Decimal::ONE));
        assert_eq!(obs.best_ask, Some(dec!(0.52)));
        assert_eq!(obs.gross_edge_vs_ask, Some(dec!(0.48)));
        // fee = 0.07 * 0.52 * 0.48 = 0.017472
        assert_eq!(obs.fee_cost, Some(dec!(0.017472)));
        assert_eq!(obs.net_edge_vs_ask, Some(dec!(0.48) - dec!(0.017472)));
        assert_eq!(obs.feed_to_book_lag_ms, Some(500));
        assert_eq!(obs.move_magnitude_bps, dec!(4));
        assert_eq!(obs.move_direction, MoveDirection::Up);
    }

    #[test]
    fn no_range_start_yields_null_prob_and_edges() {
        let m = meta();
        let b = book("0.40", "0.60", 100);
        let obs = compute_observation(
            &m,
            dec!(60000),
            200,
            200,
            None,
            Some((&b, 100)),
            dec!(3),
            MoveDirection::Up,
        );
        assert_eq!(obs.instantaneous_prob_up, None);
        assert_eq!(obs.gross_edge_vs_ask, None);
        assert_eq!(obs.net_edge_vs_ask, None);
        // book-derived fields still present
        assert_eq!(obs.best_ask, Some(dec!(0.60)));
    }

    #[test]
    fn no_book_yields_null_price_fields() {
        let m = meta();
        let obs = compute_observation(
            &m,
            dec!(60000),
            2_000,
            2_000,
            Some(dec!(59000)),
            None,
            dec!(3),
            MoveDirection::Up,
        );
        assert_eq!(obs.instantaneous_prob_up, Some(Decimal::ONE));
        assert_eq!(obs.best_ask, None);
        assert_eq!(obs.fee_cost, None);
        assert_eq!(obs.net_edge_vs_ask, None);
        assert_eq!(obs.feed_to_book_lag_ms, None);
    }

    #[test]
    fn book_without_source_timestamp_still_has_received_lag() {
        let m = meta();
        let b = BookUpdate {
            token_id: "tok-yes".to_string(),
            best_bid: Some(Price(dec!(0.40))),
            best_ask: Some(Price(dec!(0.60))),
            observed_at_ms: None, // missing source timestamp
        };
        let obs = compute_observation(
            &m,
            dec!(60000),
            2_500,
            2_500,
            Some(dec!(59000)),
            Some((&b, 2_000)),
            dec!(3),
            MoveDirection::Up,
        );
        assert_eq!(obs.best_ask, Some(dec!(0.60)));
        assert_eq!(obs.mid, Some(dec!(0.50)));
        assert_eq!(obs.feed_to_book_lag_ms, Some(500)); // received-clock lag, not null
    }

    #[test]
    fn move_trigger_emits_one_observation_per_active_market() {
        let mut state = JoinState::new(vec![meta()], params());
        state.on_book_update(book("0.48", "0.52", 1_500), 1_400);

        // Single venue -> no median -> nothing.
        assert!(
            state
                .on_exchange_tick(&etick(ExchangeVenue::Bybit, dec!(60000), 1_100), 1_100)
                .is_empty()
        );
        // Second venue -> median 60000 captured as window-open reference; first
        // detector sample, no move yet.
        assert!(
            state
                .on_exchange_tick(&etick(ExchangeVenue::Coinbase, dec!(60000), 1_150), 1_150)
                .is_empty()
        );
        // Median drifts to 60012 (+2 bps) — below threshold, no fire.
        assert!(
            state
                .on_exchange_tick(&etick(ExchangeVenue::Bybit, dec!(60024), 1_400), 1_400)
                .is_empty()
        );
        // Median reaches 60024 (+4 bps vs the 60000 reference) within the window
        // -> fires; one observation for the single active market.
        let obs =
            state.on_exchange_tick(&etick(ExchangeVenue::Coinbase, dec!(60024), 1_450), 1_450);
        assert_eq!(obs.len(), 1);
        let o = &obs[0];
        assert_eq!(o.move_direction, MoveDirection::Up);
        assert_eq!(o.move_magnitude_bps, dec!(4));
        assert_eq!(o.signal_value, dec!(60024));
        assert_eq!(o.range_start_value, Some(dec!(60000)));
        assert_eq!(o.instantaneous_prob_up, Some(Decimal::ONE));
        assert_eq!(o.best_ask, Some(dec!(0.52)));
        // lag = signal_received (1450) - book_received (1400)
        assert_eq!(o.feed_to_book_lag_ms, Some(50));
    }

    #[test]
    fn flat_consensus_emits_nothing() {
        let mut state = JoinState::new(vec![meta()], params());
        let mut out = Vec::new();
        for t in (1_000..2_000).step_by(50) {
            out.extend(state.on_exchange_tick(&etick(ExchangeVenue::Bybit, dec!(60000), t), t));
            out.extend(
                state.on_exchange_tick(&etick(ExchangeVenue::Okx, dec!(60000), t + 1), t + 1),
            );
        }
        assert!(
            out.is_empty(),
            "a flat median must not trigger observations"
        );
    }

    #[test]
    fn move_outside_any_window_emits_nothing() {
        let mut state = JoinState::new(vec![meta()], params());
        // Ticks before range_start (1000): a move fires on the detector but no
        // market is active, so nothing is emitted.
        assert!(
            state
                .on_exchange_tick(&etick(ExchangeVenue::Bybit, dec!(60000), 500), 500)
                .is_empty()
        );
        assert!(
            state
                .on_exchange_tick(&etick(ExchangeVenue::Okx, dec!(60000), 520), 520)
                .is_empty()
        );
        let out = state.on_exchange_tick(&etick(ExchangeVenue::Bybit, dec!(60030), 600), 600);
        assert!(out.is_empty(), "no active market -> no observation");
    }

    #[test]
    fn below_min_venues_yields_no_median_no_obs() {
        let mut state = JoinState::new(vec![meta()], params());
        // Only one venue ever reports: median stays None, so even a huge jump on
        // that single feed cannot trigger.
        assert!(
            state
                .on_exchange_tick(&etick(ExchangeVenue::Bybit, dec!(60000), 1_100), 1_100)
                .is_empty()
        );
        assert!(
            state
                .on_exchange_tick(&etick(ExchangeVenue::Bybit, dec!(61000), 1_200), 1_200)
                .is_empty()
        );
    }
}
