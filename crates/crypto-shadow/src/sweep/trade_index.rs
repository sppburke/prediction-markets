//! Per-token trade-print index — the MM fill-simulation input (issue #310 PR3).
//! Prints are keyed by `received_at_ms` (the node clock, per the scorer clock
//! discipline); `traded_at_ms` rides along as data.

use std::collections::HashMap;

use rust_decimal::Decimal;

use crate::db::ClobTradeRow;

/// One indexed print (node clock, price, raw taker side).
#[derive(Debug, Clone, Copy, PartialEq)]
struct IndexedTrade {
    received_at_ms: i64,
    price: Decimal,
    taker_is_buy: bool,
}

/// Trade prints grouped per token, sorted by node-receive clock.
#[derive(Debug, Default)]
pub(super) struct TradeIndex {
    per_token: HashMap<String, Vec<IndexedTrade>>,
}

impl TradeIndex {
    /// Build from the `clob_trades` rows; sorts each token's prints by
    /// `received_at_ms` (stable — DB id order breaks ties).
    pub(super) fn new(rows: &[ClobTradeRow]) -> Self {
        let mut per_token: HashMap<String, Vec<IndexedTrade>> = HashMap::new();
        for row in rows {
            per_token
                .entry(row.token_id.clone())
                .or_default()
                .push(IndexedTrade {
                    received_at_ms: row.received_at_ms,
                    price: row.price,
                    taker_is_buy: row.taker_is_buy,
                });
        }
        for trades in per_token.values_mut() {
            trades.sort_by_key(|t| t.received_at_ms);
        }
        Self { per_token }
    }

    /// First print on `token_id` that fills a maker **buy** resting at
    /// `resting_bid`: `taker_is_buy = false` (a taker sell) crossing at
    /// `price <= resting_bid`, received within
    /// `[fire_received_ms, fire_received_ms + window_ms]` on the node clock.
    /// Returns the fill print's `received_at_ms`, or `None` when nothing
    /// crossed in the window.
    pub(super) fn first_maker_fill(
        &self,
        token_id: &str,
        resting_bid: Decimal,
        fire_received_ms: i64,
        window_ms: i64,
    ) -> Option<i64> {
        let trades = self.per_token.get(token_id)?;
        let start = trades.partition_point(|t| t.received_at_ms < fire_received_ms);
        trades[start..]
            .iter()
            .take_while(|t| t.received_at_ms <= fire_received_ms + window_ms)
            .find(|t| !t.taker_is_buy && t.price <= resting_bid)
            .map(|t| t.received_at_ms)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn row(token: &str, price: Decimal, taker_is_buy: bool, received_at_ms: i64) -> ClobTradeRow {
        ClobTradeRow {
            token_id: token.to_string(),
            price,
            taker_is_buy,
            traded_at_ms: received_at_ms - 50,
            received_at_ms,
        }
    }

    #[test]
    fn first_fill_filters_side_price_and_window() {
        let ix = TradeIndex::new(&[
            // Before the fire: never a fill.
            row("y", dec!(0.40), false, 1_000),
            // Taker BUY inside the window: wrong side, skipped.
            row("y", dec!(0.45), true, 1_600),
            // Taker sell above the resting bid: no cross, skipped.
            row("y", dec!(0.49), false, 1_700),
            // Taker sell at <= bid inside the window: THE fill.
            row("y", dec!(0.47), false, 1_900),
            // Later qualifying print: not first.
            row("y", dec!(0.46), false, 2_100),
            // Outside the window entirely.
            row("y", dec!(0.30), false, 9_000),
        ]);
        assert_eq!(
            ix.first_maker_fill("y", dec!(0.48), 1_450, 1_000),
            Some(1_900)
        );
        // Window cut just before the fill print -> no fill.
        assert_eq!(ix.first_maker_fill("y", dec!(0.48), 1_450, 400), None);
        // A tighter quote the 0.47 print does not cross... 0.47 <= 0.47 fills.
        assert_eq!(
            ix.first_maker_fill("y", dec!(0.47), 1_450, 1_000),
            Some(1_900)
        );
        assert_eq!(ix.first_maker_fill("y", dec!(0.41), 1_450, 1_000), None);
        // Unknown token.
        assert_eq!(ix.first_maker_fill("n", dec!(0.48), 1_450, 1_000), None);
    }

    #[test]
    fn fill_window_is_inclusive_on_both_ends() {
        let ix = TradeIndex::new(&[
            row("y", dec!(0.45), false, 1_450), // same node-ms as the fire
            row("y", dec!(0.44), false, 2_450), // exactly fire + window
        ]);
        assert_eq!(
            ix.first_maker_fill("y", dec!(0.48), 1_450, 1_000),
            Some(1_450)
        );
        assert_eq!(
            ix.first_maker_fill("y", dec!(0.44), 2_000, 450),
            Some(2_450)
        );
    }
}
