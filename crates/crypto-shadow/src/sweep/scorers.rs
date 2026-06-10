//! Strategy scorers over replayed fires. PR1 ships buy-hold; scalp and MM are
//! PR2/PR3 (issue #310).

use std::collections::HashMap;

use crate::db::RealizedRow;
use crate::report::{RealizedGroup, build_realized};
use crate::types::EdgeObservation;

/// One replayed fire: the rebuilt observation (books resolved from the
/// per-frame index through the same pure `compute_observation` live uses) plus
/// its fire-frame tape position for divergence reporting.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct ReplayFire {
    pub obs: EdgeObservation,
    pub fire_tape_id: i64,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    use crate::fees::taker_fee_per_share;
    use crate::types::{BtcSeriesKind, MoveDirection};

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
}
