//! Strategy scorers over replayed fires: buy-hold (PR1) and scalp (PR2); MM is
//! PR3 (issue #310).

use std::collections::{BTreeMap, HashMap};

use rust_decimal::Decimal;
use serde::Serialize;

use crate::db::RealizedRow;
use crate::fees::taker_fee_per_share;
use crate::report::{RealizedGroup, build_realized};
use crate::stats::{frac_positive, mean_decimal, percentile_decimal};
use crate::types::{BtcMarketMeta, EdgeObservation, MoveDirection};

use super::book_index::BookIndex;

/// One replayed fire: the rebuilt observation (books resolved from the
/// per-frame index through the same pure `compute_observation` live uses) plus
/// its fire-frame tape position for divergence reporting.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ReplayFire {
    pub obs: EdgeObservation,
    pub fire_tape_id: i64,
    /// Node-receive clock of the fire frame — the scalp/MM scorers' time base.
    pub fire_received_ms: i64,
}

/// Buy-hold: buy the direction-side ask at fire, hold to resolution.
/// `net = (won?1:0) − ask − taker_fee(ask)` — **delegates to the live
/// `build_realized`** (up → YES at `best_ask`, down → NO at `no_best_ask`;
/// unresolved or book-absent legs unscored), so the sweep's buy-hold column is
/// the live realized formula by construction, not a reimplementation.
pub(super) fn buy_hold_groups(
    fires: &[ReplayFire],
    resolutions: &HashMap<String, bool>,
) -> Vec<RealizedGroup> {
    let rows: Vec<RealizedRow> = fires
        .iter()
        .map(|f| RealizedRow {
            series: f.obs.series.as_str().to_string(),
            move_direction: f.obs.move_direction.as_str().to_string(),
            best_ask: f.obs.best_ask,
            no_best_ask: f.obs.no_best_ask,
            yes_won: resolutions.get(&f.obs.condition_id).copied(),
        })
        .collect();
    build_realized(&rows)
}

/// Scalp exit horizons, in seconds. Canonical: `docs/_GLOSSARY.md`
/// `crypto_shadow_scalp_horizons_s`.
pub(super) const SCALP_HORIZONS_S: [i64; 4] = [10, 30, 60, 120];

/// One (series × direction × horizon) scalp aggregate.
/// `net = exit_bid(fire+H) − entry_ask − taker_fee(entry_ask) −
/// taker_fee(exit_bid)` — the exit-leg fee is charged at the exit price and is
/// *conservative-if-charged* (the official sell-side taker fee is ambiguous;
/// see `fees.rs` provenance). All time arithmetic runs on the node receive
/// clock. Legs are unscored when `fire+H > range_end_ms`, the direction-side
/// entry ask is absent, or no exit book/bid exists at `fire+H`.
#[derive(Debug, Serialize, PartialEq)]
pub struct ScalpGroup {
    pub series: String,
    pub move_direction: String,
    pub horizon_s: i64,
    pub count_scored: usize,
    pub mean_net: Option<Decimal>,
    pub p50_net: Option<Decimal>,
    pub frac_positive: Option<Decimal>,
}

/// Score every fire at every scalp horizon, direction-aware: an up-move enters
/// and exits on the YES token's book, a down-move on the NO token's own book.
pub(super) fn score_scalp(
    fires: &[ReplayFire],
    books: &BookIndex,
    markets_by_condition: &HashMap<String, BtcMarketMeta>,
) -> Vec<ScalpGroup> {
    let mut nets: BTreeMap<(String, String, i64), Vec<Decimal>> = BTreeMap::new();
    for fire in fires {
        let Some(meta) = markets_by_condition.get(&fire.obs.condition_id) else {
            continue;
        };
        let (entry_ask, exit_token) = match fire.obs.move_direction {
            MoveDirection::Up => (fire.obs.best_ask, meta.yes_token_id.as_str()),
            MoveDirection::Down => (fire.obs.no_best_ask, meta.no_token_id.as_str()),
        };
        let Some(ask) = entry_ask else {
            continue; // direction-side entry book absent at fire — unscored
        };
        for horizon_s in SCALP_HORIZONS_S {
            let exit_at_ms = fire.fire_received_ms + horizon_s * 1_000;
            if exit_at_ms > meta.range_end_ms {
                continue; // market settles before the exit — unscored
            }
            let Some((exit_book, _)) = books.book_at_time(exit_token, exit_at_ms) else {
                continue;
            };
            let Some(exit_bid) = exit_book.best_bid.map(|p| p.0) else {
                continue; // bid side absent at the exit — unscored
            };
            let net = exit_bid - ask - taker_fee_per_share(ask) - taker_fee_per_share(exit_bid);
            nets.entry((
                fire.obs.series.as_str().to_string(),
                fire.obs.move_direction.as_str().to_string(),
                horizon_s,
            ))
            .or_default()
            .push(net);
        }
    }

    let q50 = Decimal::new(5, 1);
    let mut out = Vec::with_capacity(nets.len());
    for ((series, move_direction, horizon_s), mut values) in nets {
        values.sort_unstable();
        out.push(ScalpGroup {
            series,
            move_direction,
            horizon_s,
            count_scored: values.len(),
            mean_net: mean_decimal(&values),
            p50_net: percentile_decimal(&values, q50),
            frac_positive: frac_positive(&values),
        });
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    use crate::types::BtcSeriesKind;

    fn fire(condition: &str, direction: MoveDirection) -> ReplayFire {
        ReplayFire {
            obs: EdgeObservation {
                condition_id: condition.to_string(),
                series: BtcSeriesKind::Five,
                observed_at_ms: 1_450,
                signal_value: dec!(60024),
                range_start_value: Some(dec!(60000)),
                instantaneous_prob_up: Some(dec!(1)),
                best_ask: Some(dec!(0.52)),
                mid: Some(dec!(0.50)),
                no_best_ask: Some(dec!(0.50)),
                no_mid: Some(dec!(0.48)),
                gross_edge_vs_ask: Some(dec!(0.48)),
                gross_edge_vs_mid: Some(dec!(0.50)),
                fee_cost: Some(dec!(0.017472)),
                net_edge_vs_ask: Some(dec!(0.462528)),
                net_edge_vs_mid: Some(dec!(0.4825)),
                feed_to_book_lag_ms: Some(150),
                move_magnitude_bps: dec!(4),
                move_direction: direction,
            },
            fire_tape_id: 7,
            fire_received_ms: 1_450,
        }
    }

    #[test]
    fn buy_hold_scores_each_direction_on_its_entry_side() {
        let fires = vec![
            fire("0xup", MoveDirection::Up),     // YES @ 0.52, resolved Up: won
            fire("0xdown", MoveDirection::Down), // NO @ 0.50, resolved Up: lost
            fire("0xopen", MoveDirection::Up),   // unresolved: unscored
        ];
        let mut resolutions = HashMap::new();
        resolutions.insert("0xup".to_string(), true);
        resolutions.insert("0xdown".to_string(), true);

        let groups = buy_hold_groups(&fires, &resolutions);
        assert_eq!(groups.len(), 2);

        let up = groups.iter().find(|g| g.move_direction == "up").unwrap();
        assert_eq!(up.count_scored, 1, "the unresolved fire is unscored");
        let expected_up = dec!(1) - dec!(0.52) - taker_fee_per_share(dec!(0.52));
        assert_eq!(up.mean_realized_net_vs_ask, Some(expected_up));

        let down = groups.iter().find(|g| g.move_direction == "down").unwrap();
        let expected_down = dec!(0) - dec!(0.50) - taker_fee_per_share(dec!(0.50));
        assert_eq!(down.mean_realized_net_vs_ask, Some(expected_down));
    }

    #[test]
    fn scalp_scores_direction_side_exits_with_guards() {
        use super::super::book_index::BookIndex;
        use crate::types::BookUpdate;
        use pe_core_types::Price;

        let mut markets = HashMap::new();
        markets.insert(
            "0xup".to_string(),
            BtcMarketMeta {
                condition_id: "0xup".to_string(),
                yes_token_id: "y".to_string(),
                no_token_id: "n".to_string(),
                series: BtcSeriesKind::Five,
                range_start_ms: 1_000,
                // Settles at 41_450: h=10/30 scoreable from fire 1_450; h=60/120 not.
                range_end_ms: 41_450,
                tick: dec!(0.01),
            },
        );
        let mut books = BookIndex::new(1_000);
        let entry = BookUpdate {
            token_id: "y".to_string(),
            best_bid: Some(Price(dec!(0.48))),
            best_ask: Some(Price(dec!(0.52))),
            observed_at_ms: None,
        };
        books.push(1, 1_050, &entry);
        let exit = BookUpdate {
            token_id: "y".to_string(),
            best_bid: Some(Price(dec!(0.60))),
            best_ask: Some(Price(dec!(0.63))),
            observed_at_ms: None,
        };
        books.push(5, 9_000, &exit); // before fire+10s -> the h=10 exit book

        let fires = vec![fire("0xup", MoveDirection::Up)]; // ask 0.52, fire at 1_450
        let groups = score_scalp(&fires, &books, &markets);

        // h=10s and h=30s scored (exit bid 0.60); h=60/120 cut by range_end.
        assert_eq!(groups.len(), 2);
        let expected = dec!(0.60)
            - dec!(0.52)
            - taker_fee_per_share(dec!(0.52))
            - taker_fee_per_share(dec!(0.60));
        for g in &groups {
            assert_eq!((g.series.as_str(), g.move_direction.as_str()), ("5m", "up"));
            assert!(g.horizon_s == 10 || g.horizon_s == 30, "h={}", g.horizon_s);
            assert_eq!(g.count_scored, 1);
            assert_eq!(g.mean_net, Some(expected));
            assert_eq!(g.frac_positive, Some(dec!(1)));
        }
    }

    #[test]
    fn scalp_down_move_exits_on_the_no_book_and_skips_absent_sides() {
        use super::super::book_index::BookIndex;
        use crate::types::BookUpdate;
        use pe_core_types::Price;

        let mut markets = HashMap::new();
        markets.insert(
            "0xdown".to_string(),
            BtcMarketMeta {
                condition_id: "0xdown".to_string(),
                yes_token_id: "y".to_string(),
                no_token_id: "n".to_string(),
                series: BtcSeriesKind::Five,
                range_start_ms: 1_000,
                range_end_ms: 301_000,
                tick: dec!(0.01),
            },
        );
        let mut books = BookIndex::new(1_000);
        // NO book at fire+10s has NO bid side -> h=10 unscored; bid appears
        // before fire+30s -> h=30 scored on the NO book.
        books.push(
            1,
            2_000,
            &BookUpdate {
                token_id: "n".to_string(),
                best_bid: None,
                best_ask: Some(Price(dec!(0.55))),
                observed_at_ms: None,
            },
        );
        books.push(
            2,
            20_000,
            &BookUpdate {
                token_id: "n".to_string(),
                best_bid: Some(Price(dec!(0.58))),
                best_ask: Some(Price(dec!(0.61))),
                observed_at_ms: None,
            },
        );

        let fires = vec![fire("0xdown", MoveDirection::Down)]; // no_best_ask 0.50
        let groups = score_scalp(&fires, &books, &markets);
        let horizons: Vec<i64> = groups.iter().map(|g| g.horizon_s).collect();
        assert_eq!(horizons, vec![30, 60, 120], "h=10 skipped: absent bid side");
        let expected = dec!(0.58)
            - dec!(0.50)
            - taker_fee_per_share(dec!(0.50))
            - taker_fee_per_share(dec!(0.58));
        assert_eq!(groups[0].mean_net, Some(expected));
    }
}
