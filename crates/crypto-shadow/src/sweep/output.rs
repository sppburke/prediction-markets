//! Sweep output assembly: the tape-validity block, per-cell results, JSON, and
//! the ASCII table (issue #310).

use std::collections::BTreeMap;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::report::RealizedGroup;
use crate::types::EdgeObservation;

use super::events::DecodeStats;
use super::scorers::{MmGroup, ReplayFire, ScalpGroup};

/// Stamped into the output because the tape does not record the capture run's
/// trigger params (`meta` carries only schema/fee/vantage/lag-clock/drop keys).
pub const CAPTURE_CONFIG_PROVENANCE: &str =
    "capture config assumed = sweep config (tape does not record trigger params)";

/// Buy-hold fee assumption, stamped per the issue #310 fee-model boundary.
pub const BUY_HOLD_FEE_PROVENANCE: &str = "buy-hold: net = (won?1:0) - entry_ask - \
     taker_fee(entry_ask); entry/taker-buy leg only (crypto_fees_v2)";

/// Scalp fee assumption: the exit-leg fee is charged at the exit price and is
/// **conservative-if-charged** — the official sell-side taker fee is ambiguous
/// between two Polymarket sources (`fees.rs` provenance).
pub const SCALP_FEE_PROVENANCE: &str = "scalp: net = exit_bid(fire+H) - entry_ask - \
     taker_fee(entry_ask) - taker_fee(exit_bid); exit-leg fee conservative-if-charged \
     (sell-side officially ambiguous, see crypto_fees_v2 provenance)";

/// MM fee/fill assumptions — an **upper bound twice over**: front-of-queue
/// fills (v1) + the per-fill 20% rebate idealization of the daily pro-rata,
/// liquidity-weighted pool. Rebate rate source: per-market Gamma
/// `feeSchedule.rebateRate = 0.2` (see `fees.rs`).
pub const MM_FEE_PROVENANCE: &str = "mm: net = (won?1:0) - resting_bid + \
     0.20*taker_fee(resting_bid); UPPER BOUND x2 (front-of-queue fills + per-fill \
     rebate idealization of the daily pro-rata pool); rebate rate from per-market \
     feeSchedule.rebateRate (verified live 2026-06-09)";

/// Trigger parameters of the reference cell (read from the sweep invocation's
/// `ShadowConfig`; `top_n = 3` = all live venues).
#[derive(Debug, Serialize)]
pub struct ReferenceParams {
    pub threshold_bps: Decimal,
    pub window_ms: i64,
    pub cooldown_ms: i64,
    pub min_venues: usize,
    pub top_n: usize,
}

/// Reference-cell divergence vs the live `observations` table, reported with
/// missing and extra rows **separately**: on a clean single-process tape the
/// divergence is one-directional by construction (live registration precedes a
/// market's first CLOB frame, so replay activation is strictly later) —
/// missing rows are registration-timing residue or a config mismatch, while
/// **extra rows indicate a replay bug**.
#[derive(Debug, Serialize)]
pub struct FidelitySummary {
    pub live_rows: usize,
    pub replay_rows: usize,
    pub matched: usize,
    /// Live rows the replay did not reproduce.
    pub missing_rows: usize,
    /// Replay rows live never emitted (a replay bug on a clean tape).
    pub extra_rows: usize,
    pub first_divergence: Option<String>,
}

/// Tape-validity block: `frames_dropped_*` status, decode stats, reference-cell
/// params + fidelity, and the fee/config assumption strings.
#[derive(Debug, Serialize)]
pub struct TapeValidity {
    /// `true` only when every `frames_dropped_*` key is present and zero.
    pub valid: bool,
    /// `clean`, `invalid: ...` (non-zero drops, run-invalidating per the #311
    /// runner contract), or `unknown: ...` (**missing keys — a crashed capture
    /// never reaches the run-end stamp — treated as not-clean, never as 0**).
    pub frames_dropped_status: String,
    /// Per-key value; `None` = key absent from `meta`.
    pub frames_dropped: BTreeMap<String, Option<u64>>,
    pub decode_stats: DecodeStats,
    pub reference_params: ReferenceParams,
    pub capture_config_provenance: String,
    pub fidelity: FidelitySummary,
    pub fee_provenance: Vec<String>,
}

/// One grid cell's replay result (buy-hold column; scalp/MM land in PR2/PR3).
#[derive(Debug, Serialize)]
pub struct CellResult {
    pub threshold_bps: Decimal,
    pub window_ms: i64,
    pub cooldown_ms: i64,
    pub top_n: usize,
    /// Total replayed observation rows (one per active market per fire).
    pub fires: usize,
    /// The 50 ms window sits at the ~40 ms aggregate inter-tick floor — flagged.
    pub near_degenerate_window: bool,
    /// Per (series × direction) buy-hold realized stats.
    pub buy_hold: Vec<RealizedGroup>,
    /// Per (series × direction × horizon) scalp stats (PR2).
    pub scalp: Vec<ScalpGroup>,
    /// Per (series × direction) maker stats (PR3) — read as a ceiling.
    pub mm: Vec<MmGroup>,
}

/// Full sweep output: tape validity + the 480-cell grid.
#[derive(Debug, Serialize)]
pub struct SweepOutput {
    pub tape_validity: TapeValidity,
    pub cells: Vec<CellResult>,
}

impl SweepOutput {
    /// Pretty JSON for `--out` / stdout.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Human-readable ASCII table: validity banner, fidelity line, then one row
    /// per cell with per (series × direction) buy-hold stats.
    pub fn to_table(&self) -> String {
        let v = &self.tape_validity;
        let mut out = String::new();
        if v.valid {
            out.push_str("TAPE: clean (frames_dropped_* all zero)\n");
        } else {
            out.push_str(&format!(
                "!! TAPE NOT CLEAN — {} — results are faithful to the captured bytes, not the market\n",
                v.frames_dropped_status
            ));
        }
        let f = &v.fidelity;
        out.push_str(&format!(
            "reference cell ({} bps / {} ms / {} ms cooldown / min_venues {} / top_n {}): \
             live {} replay {} matched {} missing {} extra {}\n",
            v.reference_params.threshold_bps,
            v.reference_params.window_ms,
            v.reference_params.cooldown_ms,
            v.reference_params.min_venues,
            v.reference_params.top_n,
            f.live_rows,
            f.replay_rows,
            f.matched,
            f.missing_rows,
            f.extra_rows,
        ));
        if let Some(d) = &f.first_divergence {
            out.push_str(&format!("first divergence: {d}\n"));
        }
        out.push_str(&format!("{}\n", v.capture_config_provenance));
        out.push('\n');

        out.push_str(
            "  bps  win   cd  topN  fires  flag | buy-hold n/mean per series x direction\n",
        );
        for c in &self.cells {
            let flag = if c.near_degenerate_window {
                "~50ms"
            } else {
                ""
            };
            let mut groups = String::new();
            for g in &c.buy_hold {
                let mean = g
                    .mean_realized_net_vs_ask
                    .map_or("-".to_string(), |m| m.round_dp(4).to_string());
                groups.push_str(&format!(
                    " {}/{}: n={} mean={}",
                    g.series, g.move_direction, g.count_scored, mean
                ));
            }
            out.push_str(&format!(
                "{:>5} {:>4} {:>4}  {:>4} {:>6} {:>5} |{}\n",
                c.threshold_bps, c.window_ms, c.cooldown_ms, c.top_n, c.fires, flag, groups
            ));
            // Scalp sub-lines: one per (series, direction), horizons inline.
            let mut by_leg: BTreeMap<(&str, &str), Vec<&ScalpGroup>> = BTreeMap::new();
            for g in &c.scalp {
                by_leg
                    .entry((g.series.as_str(), g.move_direction.as_str()))
                    .or_default()
                    .push(g);
            }
            for ((series, direction), legs) in by_leg {
                let mut cols = String::new();
                for g in legs {
                    let mean = g
                        .mean_net
                        .map_or("-".to_string(), |m| m.round_dp(4).to_string());
                    cols.push_str(&format!(
                        " h{}s n={} mean={}",
                        g.horizon_s, g.count_scored, mean
                    ));
                }
                out.push_str(&format!("        scalp {series}/{direction}:{cols}\n"));
            }
            for g in &c.mm {
                let mean = g
                    .mean_net
                    .map_or("-".to_string(), |m| m.round_dp(4).to_string());
                out.push_str(&format!(
                    "        mm    {}/{}: rest={} fill={} scored={} mean={} (ceiling)\n",
                    g.series,
                    g.move_direction,
                    g.count_resting,
                    g.count_filled,
                    g.count_scored,
                    mean
                ));
            }
        }
        out
    }
}

/// Validity + status from the `frames_dropped_*` meta values. Missing keys
/// (`None`) stamp `unknown` — **never defaulted to 0**: the runner writes them
/// only after `drive` returns, so a crashed/killed capture has no keys at all.
pub(super) fn frames_dropped_validity(values: &BTreeMap<String, Option<u64>>) -> (bool, String) {
    let missing: Vec<&str> = values
        .iter()
        .filter(|(_, v)| v.is_none())
        .map(|(k, _)| k.as_str())
        .collect();
    if !missing.is_empty() {
        return (
            false,
            format!(
                "unknown: missing meta keys [{}] (crashed capture?) — treated as not-clean",
                missing.join(", ")
            ),
        );
    }
    let nonzero: Vec<String> = values
        .iter()
        .filter_map(|(k, v)| match v {
            Some(n) if *n > 0 => Some(format!("{k}={n}")),
            _ => None,
        })
        .collect();
    if !nonzero.is_empty() {
        return (
            false,
            format!(
                "invalid: frames dropped [{}] — run-invalidating per the #311 contract",
                nonzero.join(", ")
            ),
        );
    }
    (true, "clean".to_string())
}

/// Compare live `observations` rows against the reference-cell replay.
///
/// Both sides are first sorted by `(observed_at_ms, condition_id)`: live
/// within-fire emission order is HashMap-arbitrary (`join.rs` iterates the
/// markets map), so raw insertion order would false-diverge on row order within
/// a single fire. After canonicalization, a two-pointer pass counts matched /
/// missing (live-only) / extra (replay-only) rows; full-row equality via
/// `EdgeObservation: PartialEq`.
pub(super) fn fidelity_summary(live: &[EdgeObservation], replay: &[ReplayFire]) -> FidelitySummary {
    let sort_key = |o: &EdgeObservation| (o.observed_at_ms, o.condition_id.clone());
    let mut live_sorted: Vec<&EdgeObservation> = live.iter().collect();
    live_sorted.sort_by_key(|o| sort_key(o));
    let mut replay_sorted: Vec<&ReplayFire> = replay.iter().collect();
    replay_sorted.sort_by_key(|f| sort_key(&f.obs));

    let mut i = 0;
    let mut j = 0;
    let mut matched = 0;
    let mut missing = 0;
    let mut first_divergence: Option<String> = None;
    while i < live_sorted.len() && j < replay_sorted.len() {
        if *live_sorted[i] == replay_sorted[j].obs {
            matched += 1;
            i += 1;
            j += 1;
        } else {
            if first_divergence.is_none() {
                first_divergence = Some(format!(
                    "live row (condition {}, observed_at_ms {}) != replay row (condition {}, \
                     observed_at_ms {}, fire tape_id {})",
                    live_sorted[i].condition_id,
                    live_sorted[i].observed_at_ms,
                    replay_sorted[j].obs.condition_id,
                    replay_sorted[j].obs.observed_at_ms,
                    replay_sorted[j].fire_tape_id,
                ));
            }
            missing += 1;
            i += 1;
        }
    }
    missing += live_sorted.len() - i;
    let extra = replay_sorted.len() - j;
    if first_divergence.is_none() && extra > 0 {
        first_divergence = Some(format!(
            "replay has {extra} rows live never emitted, first at fire tape_id {} \
             (condition {}) — on a clean tape this indicates a replay bug",
            replay_sorted[j].fire_tape_id, replay_sorted[j].obs.condition_id,
        ));
    }

    FidelitySummary {
        live_rows: live.len(),
        replay_rows: replay.len(),
        matched,
        missing_rows: missing,
        extra_rows: extra,
        first_divergence,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    use crate::types::{BtcSeriesKind, MoveDirection};

    #[test]
    fn frames_dropped_missing_keys_are_unknown_never_zero() {
        let mut values = BTreeMap::new();
        values.insert("frames_dropped_clob".to_string(), Some(0_u64));
        values.insert("frames_dropped_bybit".to_string(), None);
        let (valid, status) = frames_dropped_validity(&values);
        assert!(!valid);
        assert!(status.starts_with("unknown"), "{status}");
        assert!(status.contains("frames_dropped_bybit"));
    }

    #[test]
    fn frames_dropped_nonzero_is_invalid_and_all_zero_is_clean() {
        let mut values = BTreeMap::new();
        values.insert("frames_dropped_clob".to_string(), Some(7_u64));
        values.insert("frames_dropped_bybit".to_string(), Some(0_u64));
        let (valid, status) = frames_dropped_validity(&values);
        assert!(!valid);
        assert!(status.starts_with("invalid"), "{status}");
        assert!(status.contains("frames_dropped_clob=7"));

        let clean: BTreeMap<String, Option<u64>> = [
            ("frames_dropped_clob".to_string(), Some(0_u64)),
            ("frames_dropped_bybit".to_string(), Some(0_u64)),
        ]
        .into_iter()
        .collect();
        assert_eq!(frames_dropped_validity(&clean), (true, "clean".to_string()));
    }

    fn obs(condition: &str, t: i64, ask: Option<Decimal>) -> EdgeObservation {
        EdgeObservation {
            condition_id: condition.to_string(),
            series: BtcSeriesKind::Five,
            observed_at_ms: t,
            signal_value: dec!(60000),
            range_start_value: Some(dec!(59000)),
            instantaneous_prob_up: Some(dec!(1)),
            best_ask: ask,
            mid: None,
            no_best_ask: None,
            no_mid: None,
            gross_edge_vs_ask: None,
            gross_edge_vs_mid: None,
            fee_cost: None,
            net_edge_vs_ask: None,
            net_edge_vs_mid: None,
            feed_to_book_lag_ms: None,
            move_magnitude_bps: dec!(4),
            move_direction: MoveDirection::Up,
        }
    }

    fn replay(o: EdgeObservation, tape_id: i64) -> ReplayFire {
        ReplayFire {
            obs: o,
            fire_tape_id: tape_id,
            fire_received_ms: 0,
        }
    }

    #[test]
    fn fidelity_exact_match_is_order_insensitive_within_a_fire() {
        // Two rows from ONE fire (same observed_at_ms) in opposite orders —
        // live within-fire order is HashMap-arbitrary, so this must match.
        let live = vec![obs("0xa", 1_450, None), obs("0xb", 1_450, None)];
        let rep = vec![
            replay(obs("0xb", 1_450, None), 9),
            replay(obs("0xa", 1_450, None), 9),
        ];
        let f = fidelity_summary(&live, &rep);
        assert_eq!((f.matched, f.missing_rows, f.extra_rows), (2, 0, 0));
        assert!(f.first_divergence.is_none());
    }

    #[test]
    fn fidelity_splits_missing_and_extra_directionally() {
        // Live has a row replay missed (registration residue)...
        let live = vec![obs("0xa", 1_450, None), obs("0xb", 2_450, None)];
        let rep = vec![replay(obs("0xb", 2_450, None), 12)];
        let f = fidelity_summary(&live, &rep);
        assert_eq!((f.matched, f.missing_rows, f.extra_rows), (1, 1, 0));
        assert!(f.first_divergence.unwrap().contains("0xa"));

        // ...and a replay-only row is EXTRA (a replay bug on a clean tape).
        let live2 = vec![obs("0xa", 1_450, None)];
        let rep2 = vec![
            replay(obs("0xa", 1_450, None), 5),
            replay(obs("0xz", 9_450, None), 44),
        ];
        let f2 = fidelity_summary(&live2, &rep2);
        assert_eq!((f2.matched, f2.missing_rows, f2.extra_rows), (1, 0, 1));
        assert!(f2.first_divergence.unwrap().contains("replay bug"));
    }

    #[test]
    fn fidelity_field_level_mismatch_diverges() {
        // Same key, different best_ask -> not a match (full-row equality).
        let live = vec![obs("0xa", 1_450, Some(dec!(0.52)))];
        let rep = vec![replay(obs("0xa", 1_450, Some(dec!(0.53))), 5)];
        let f = fidelity_summary(&live, &rep);
        assert_eq!((f.matched, f.missing_rows, f.extra_rows), (0, 1, 1));
    }
}
