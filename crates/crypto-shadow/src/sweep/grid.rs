//! The 480-cell parameter grid (issue #310). Axes are canonical in
//! `docs/_GLOSSARY.md` (`crypto_shadow_sweep_grid_axes`); every cell runs the
//! real detection path (`JoinState` + `MoveDetector`) — never a reimplementation.

use rust_decimal::Decimal;

use crate::consensus::ConsensusParams;

/// `threshold_bps` axis: 1..=10 bps.
const THRESHOLDS_BPS: [i64; 10] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
/// `window_ms` axis. The 50 ms window is near-degenerate (~40 ms aggregate
/// inter-tick floor) — included but flagged via [`Cell::near_degenerate_window`].
const WINDOWS_MS: [i64; 8] = [50, 100, 150, 200, 300, 500, 750, 1000];
/// `cooldown_ms` axis.
const COOLDOWNS_MS: [i64; 3] = [500, 1000, 2000];
/// `top_n_venues` axis.
const TOP_N: [usize; 2] = [2, 3];
/// Every grid cell runs the live `min_venues` default. Canonical:
/// `docs/_GLOSSARY.md` `crypto_shadow_min_venues` (cross-reference, no second
/// numeric copy — with `top_n = 2` the median then requires both retained venues).
const GRID_MIN_VENUES: usize = 2;

/// One grid cell's trigger parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Cell {
    pub threshold_bps: Decimal,
    pub window_ms: i64,
    pub cooldown_ms: i64,
    pub top_n: usize,
}

impl Cell {
    /// Detector tuning for this cell (`min_venues` pinned to the live default).
    pub(super) fn consensus_params(&self) -> ConsensusParams {
        ConsensusParams {
            min_venues: GRID_MIN_VENUES,
            threshold_bps: self.threshold_bps,
            window_ms: self.window_ms,
            cooldown_ms: self.cooldown_ms,
        }
    }

    /// Whether this cell's window sits at the near-degenerate floor (50 ms,
    /// ≈ the aggregate inter-tick floor) — flagged, never silently excluded.
    pub(super) fn near_degenerate_window(&self) -> bool {
        self.window_ms == WINDOWS_MS[0]
    }
}

/// The full 480-cell grid, in (threshold, window, cooldown, top_n) order.
pub(super) fn cells() -> Vec<Cell> {
    let mut out = Vec::with_capacity(
        THRESHOLDS_BPS.len() * WINDOWS_MS.len() * COOLDOWNS_MS.len() * TOP_N.len(),
    );
    for &threshold in &THRESHOLDS_BPS {
        for &window_ms in &WINDOWS_MS {
            for &cooldown_ms in &COOLDOWNS_MS {
                for &top_n in &TOP_N {
                    out.push(Cell {
                        threshold_bps: Decimal::from(threshold),
                        window_ms,
                        cooldown_ms,
                        top_n,
                    });
                }
            }
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn grid_has_480_unique_cells() {
        let all = cells();
        assert_eq!(all.len(), 480);
        let mut dedup = all.clone();
        dedup.sort_by_key(|c| {
            (
                c.threshold_bps.mantissa(),
                c.window_ms,
                c.cooldown_ms,
                c.top_n,
            )
        });
        dedup.dedup();
        assert_eq!(dedup.len(), 480, "no duplicate cells");
    }

    #[test]
    fn cell_params_pin_min_venues_and_flag_degenerate_window() {
        let all = cells();
        assert!(all.iter().all(|c| c.consensus_params().min_venues == 2));
        let degenerate = all.iter().filter(|c| c.near_degenerate_window()).count();
        // 50 ms appears once per (threshold × cooldown × top_n) combination.
        assert_eq!(degenerate, 10 * 3 * 2);
        let c = &all[0];
        assert_eq!(c.threshold_bps, dec!(1));
        assert_eq!(c.window_ms, 50);
    }
}
