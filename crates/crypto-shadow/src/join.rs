//! Feed-join + edge computation. Pure and deterministic: no clock, no I/O — the
//! observation time comes from the inbound tick. This is the gate-critical core
//! exercised by unit tests and the offline scenario test.

use std::cmp::Ordering;
use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::fees::taker_fee_per_share;
use crate::types::{BookUpdate, BtcMarketMeta, ChainlinkTick, EdgeObservation};

/// Binary indicator P(up): `1` if `c > r`, `0.5` on a tie, `0` if `c < r`.
fn indicator_prob_up(c: Decimal, r: Decimal) -> Decimal {
    match c.cmp(&r) {
        Ordering::Greater => Decimal::ONE,
        Ordering::Equal => Decimal::new(5, 1), // 0.5
        Ordering::Less => Decimal::ZERO,
    }
}

/// Compute one [`EdgeObservation`] for a market given the latest Chainlink value
/// `c` at `observed_at_ms`, the captured range-start value `range_start` (if
/// known), and the latest `book` (if seen). Pure.
pub fn compute_observation(
    meta: &BtcMarketMeta,
    c: Decimal,
    observed_at_ms: i64,
    range_start: Option<Decimal>,
    book: Option<&BookUpdate>,
) -> EdgeObservation {
    let prob_up = range_start.map(|r| indicator_prob_up(c, r));
    let best_ask = book.and_then(|b| b.best_ask).map(|p| p.0);
    let mid = book.and_then(BookUpdate::mid);
    let feed_to_book_lag_ms = book.map(|b| observed_at_ms - b.observed_at_ms);

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
        chainlink_value: c,
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
    }
}

/// Stateful join across the two feeds. Holds market metadata, the latest book
/// per market, and the captured range-start BTC/USD value per market.
///
/// The Chainlink BTC/USD value is shared across all active BTC markets, so one
/// tick produces one observation per active market.
#[derive(Debug, Default)]
pub struct JoinState {
    markets: HashMap<String, BtcMarketMeta>,
    token_to_condition: HashMap<String, String>,
    latest_book: HashMap<String, BookUpdate>,
    range_start_value: HashMap<String, Decimal>,
}

impl JoinState {
    /// Build a join state over the given markets.
    pub fn new(markets: Vec<BtcMarketMeta>) -> Self {
        let mut state = Self::default();
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

    /// Apply a book update, keyed by the market whose YES token it belongs to.
    /// Unknown tokens are ignored (book for a market we are not tracking).
    pub fn on_book_update(&mut self, update: BookUpdate) {
        if let Some(condition) = self.token_to_condition.get(&update.token_id).cloned() {
            self.latest_book.insert(condition, update);
        }
    }

    /// Apply a Chainlink tick: capture range-start for any market whose window
    /// has opened (first tick at/after `range_start_ms`), then emit one
    /// observation per market currently inside its `[range_start, range_end]`
    /// window.
    pub fn on_chainlink_tick(&mut self, tick: &ChainlinkTick) -> Vec<EdgeObservation> {
        let c = tick.value.0;
        let t = tick.observed_at_ms;

        // Capture range-start values first (mutating borrow), collecting the set
        // of markets to emit for.
        let mut active: Vec<String> = Vec::new();
        for (condition, meta) in &self.markets {
            if t < meta.range_start_ms || t > meta.range_end_ms {
                continue;
            }
            active.push(condition.clone());
        }
        for condition in &active {
            if let Some(meta) = self.markets.get(condition)
                && t >= meta.range_start_ms
            {
                self.range_start_value.entry(condition.clone()).or_insert(c);
            }
        }

        let mut out = Vec::with_capacity(active.len());
        for condition in active {
            let Some(meta) = self.markets.get(&condition) else {
                continue;
            };
            let range_start = self.range_start_value.get(&condition).copied();
            let book = self.latest_book.get(&condition);
            out.push(compute_observation(meta, c, t, range_start, book));
        }
        out
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::types::{BtcSeriesKind, BtcUsdPrice};
    use pe_core_types::Price;
    use rust_decimal_macros::dec;

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
            observed_at_ms: ts,
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
        let obs = compute_observation(&m, dec!(60000), 2_500, Some(dec!(59000)), Some(&b));
        assert_eq!(obs.instantaneous_prob_up, Some(Decimal::ONE));
        assert_eq!(obs.best_ask, Some(dec!(0.52)));
        assert_eq!(obs.gross_edge_vs_ask, Some(dec!(0.48)));
        // fee = 0.07 * 0.52 * 0.48 = 0.017472
        assert_eq!(obs.fee_cost, Some(dec!(0.017472)));
        assert_eq!(obs.net_edge_vs_ask, Some(dec!(0.48) - dec!(0.017472)));
        assert_eq!(obs.feed_to_book_lag_ms, Some(500));
    }

    #[test]
    fn no_range_start_yields_null_prob_and_edges() {
        let m = meta();
        let b = book("0.40", "0.60", 100);
        let obs = compute_observation(&m, dec!(60000), 200, None, Some(&b));
        assert_eq!(obs.instantaneous_prob_up, None);
        assert_eq!(obs.gross_edge_vs_ask, None);
        assert_eq!(obs.net_edge_vs_ask, None);
        // book-derived fields still present
        assert_eq!(obs.best_ask, Some(dec!(0.60)));
    }

    #[test]
    fn no_book_yields_null_price_fields() {
        let m = meta();
        let obs = compute_observation(&m, dec!(60000), 2_000, Some(dec!(59000)), None);
        assert_eq!(obs.instantaneous_prob_up, Some(Decimal::ONE));
        assert_eq!(obs.best_ask, None);
        assert_eq!(obs.fee_cost, None);
        assert_eq!(obs.net_edge_vs_ask, None);
        assert_eq!(obs.feed_to_book_lag_ms, None);
    }

    #[test]
    fn join_emits_one_observation_per_active_market_and_captures_range_start() {
        let mut state = JoinState::new(vec![meta()]);
        state.on_book_update(book("0.48", "0.52", 1_500));
        // First in-window tick captures range start = 59000.
        let first = state.on_chainlink_tick(&ChainlinkTick {
            symbol: "btc/usd".to_string(),
            observed_at_ms: 1_200,
            value: BtcUsdPrice(dec!(59000)),
        });
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].range_start_value, Some(dec!(59000)));
        // Later tick keeps the captured range start, recomputes the indicator.
        let later = state.on_chainlink_tick(&ChainlinkTick {
            symbol: "btc/usd".to_string(),
            observed_at_ms: 2_000,
            value: BtcUsdPrice(dec!(60000)),
        });
        assert_eq!(later[0].range_start_value, Some(dec!(59000)));
        assert_eq!(later[0].instantaneous_prob_up, Some(Decimal::ONE));
    }

    #[test]
    fn out_of_window_tick_emits_nothing() {
        let mut state = JoinState::new(vec![meta()]);
        let before = state.on_chainlink_tick(&ChainlinkTick {
            symbol: "btc/usd".to_string(),
            observed_at_ms: 500, // before range_start_ms = 1000
            value: BtcUsdPrice(dec!(59000)),
        });
        assert!(before.is_empty());
    }
}
