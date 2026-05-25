//! Stage-2 weighted-composite ranker over the BHq-significant cohort
//! (docs/24- §2 "PR 4" outline — minimum-viable subset, hand-picked weights).
//!
//! The v1 ranker (`selection.rs`) ranks by deflated Sharpe alone, which ignores
//! every PR-1 candidate-features column (EV, Brier, HHI, Kelly, …). The
//! composite ranker takes the same BHq-significant cohort and re-ranks it by a
//! configurable **z-score-weighted linear combination** of the 12 features
//! (`sharpe_bps` + the 11 PR-1 columns). The deferred ONC clustering /
//! clustered MDA / non-negative elastic-net / PBO-deflated full version of
//! docs/24- PR 4 lands separately.
//!
//! ## Why z-score
//!
//! Raw feature magnitudes differ by orders of magnitude:
//! - `sharpe_bps` ∈ [−50000, +50000]
//! - `concentration_hhi_bps` ∈ [0, 10000]
//! - `median_first_entry_to_resolution_secs` is in raw seconds (e.g. 604800 = 1
//!   week), not bps at all
//!
//! Linear weighting on raw values would let whichever feature has the largest
//! native scale dominate. Z-scoring (`(x − μ_cohort) / σ_cohort`) makes weights
//! interpretable as "importance per standard-deviation move" and unit-agnostic.
//! Cohort μ / σ are computed at composite time over the BHq-significant set
//! (the only set the composite ever scores), so the standardisation is
//! self-contained and reproducible from `(features, weights)` alone.
//!
//! ## Direction
//!
//! Weight signs encode "higher is better" (positive) vs "lower is better"
//! (negative). Defaults negate Brier score, HHI, RPC, and median-time-to-
//! resolution — see [`CompositeWeights`]. Cohort std of `0` for any feature
//! falls back to a per-feature z-score of `0` (that feature contributes
//! nothing for this cohort), so a degenerate-distribution feature can't NaN
//! the composite.
//!
//! All arithmetic is `rust_decimal` (no `f64`).

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, MathematicalOps};

use crate::db::WalletFeatures;

/// Signed per-feature weights for the composite ranker, in basis points
/// (`1000` = 0.10). **Sign** encodes direction — positive = higher-is-better,
/// negative = lower-is-better. The 12-feature surface mirrors
/// [`crate::DeterministicFeatures`]'s post-PR-1 columns plus the existing
/// `sharpe_bps`.
///
/// Default weights follow docs/24- §3.1 group structure ("per-bet quality
/// dominant; equal-weight within families"). Override individually via
/// `PE_SKILL_COMPOSITE_WEIGHT_*` env vars on [`crate::SkillConfig`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompositeWeights {
    // ── Daily-return moments ───────────────────────────────────────────────
    /// `sharpe_bps` — the v1 ranker's sole feature. Carried into the composite.
    pub w_sharpe_bps: i32,
    // ── Per-bet quality (docs/24- §3.1 group A) ────────────────────────────
    pub w_ev_mean_bps: i32,
    pub w_ev_tstat_bps: i32,
    pub w_bb_shrunk_edge_bps: i32,
    pub w_kelly_log_growth_bps: i32,
    /// Brier *score* — lower is better calibration; default weight is negative.
    pub w_brier_score_bps: i32,
    pub w_brier_resolution_bps: i32,
    // ── Concentration (docs/24- §3.1 group F) ──────────────────────────────
    /// HHI — lower is better diversification; default weight is negative.
    pub w_concentration_hhi_bps: i32,
    pub w_concentration_n_eff_bps: i32,
    /// Rank-weighted concentration — higher = more concentrated; default
    /// weight is negative (we prefer breadth, matching the paper's skilled
    /// vs lucky-winner discrimination).
    pub w_concentration_rpc_bps: i32,
    // ── Activity (docs/24- §3.1 group C) ───────────────────────────────────
    pub w_first_entries_per_active_day_bps: i32,
    // ── Capital velocity (docs/24- §3.1 group D) ──────────────────────────
    /// Median time-to-resolution — lower (faster recycle) is better; default
    /// weight is negative.
    pub w_median_first_entry_to_resolution_secs: i32,
}

impl Default for CompositeWeights {
    /// docs/24- §3.1 group-equal-within-family weights, per-bet quality dominant.
    /// Family totals (bps): per-bet quality 5000, sharpe 1500, concentration
    /// 1500, activity 1000, capital velocity 1000 — sum 10000 for readability,
    /// but **only relative magnitude matters after z-score normalisation**.
    fn default() -> Self {
        Self {
            w_sharpe_bps: 1500,
            w_ev_mean_bps: 833,
            w_ev_tstat_bps: 833,
            w_bb_shrunk_edge_bps: 833,
            w_kelly_log_growth_bps: 833,
            w_brier_score_bps: -833,
            w_brier_resolution_bps: 833,
            w_concentration_hhi_bps: -500,
            w_concentration_n_eff_bps: 500,
            w_concentration_rpc_bps: -500,
            w_first_entries_per_active_day_bps: 1000,
            w_median_first_entry_to_resolution_secs: -1000,
        }
    }
}

/// Composite-ranked outcome for one wallet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeResult {
    /// `0x`-prefixed wallet address.
    pub wallet_hex: String,
    /// Composite score × 10⁴ (basis-points scale, signed). Saturates to
    /// `i64::{MIN, MAX}` rather than wrapping.
    pub composite_score_bps: i64,
    /// Sign-randomization permutation p-value × 10⁴ (carried through for the
    /// downstream watchlist line; not used in scoring).
    pub skill_pvalue_bps: u32,
    /// Raw per-period Sharpe × 10⁴ (carried through; not used in scoring).
    pub sharpe_bps: i64,
    /// Passed the Benjamini–Hochberg FDR gate at `q`.
    pub bhq_significant: bool,
    /// In the final selection (`bhq_significant` ∧ `trading_days ≥
    /// min_trading_days` ∧ in top-`n` by composite score).
    pub selected: bool,
}

/// Rank the cohort by composite score. Two-stage:
///
/// 1. **BHq FDR gate** on `skill_pvalue_bps` at `bhq_q_bps` (basis points; the
///    standard 1000 = 0.10 default). Identical procedure to
///    [`crate::select_wallets`].
/// 2. **Composite rank** over the BHq-significant subset further restricted to
///    wallets with `trading_days ≥ min_trading_days` (Sharpe is degenerate on
///    very few daily-return points and the composite carries `sharpe_bps`).
///    `top_n` caps the `selected` set; the rest of the BHq-significant cohort
///    is returned with `selected = false` and a real composite score for
///    diagnostic comparison.
///
/// Returns one [`CompositeResult`] per input row, ordered `selected` first
/// then by composite score descending, then by `wallet_hex` ascending for
/// determinism. Tie-break does **not** use Sharpe — the composite score
/// already weighs Sharpe via `w_sharpe_bps`.
pub fn rank_by_composite(
    rows: &[WalletFeatures],
    weights: &CompositeWeights,
    bhq_q_bps: u32,
    top_n: usize,
    min_trading_days: u32,
) -> Vec<CompositeResult> {
    let m = rows.len();
    if m == 0 {
        return Vec::new();
    }

    let bhq_significant = bhq_gate(rows, bhq_q_bps);
    let stats = CohortStats::from_rows(rows);

    let scores: Vec<i64> = rows
        .iter()
        .map(|w| decimal_to_bps_i64(composite_score(w, weights, &stats)))
        .collect();

    // Ranking candidates: BHq-significant ∧ has min_trading_days.
    let mut rank_idx: Vec<usize> = (0..m)
        .filter(|&i| bhq_significant[i] && rows[i].features.trading_days >= min_trading_days)
        .collect();
    rank_idx.sort_by(|&a, &b| {
        scores[b].cmp(&scores[a]).then_with(|| {
            rows[a]
                .features
                .wallet_hex
                .cmp(&rows[b].features.wallet_hex)
        })
    });
    let selected_set: std::collections::HashSet<usize> =
        rank_idx.iter().take(top_n).copied().collect();

    // Build outputs in display order: selected first, then BHq-significant non-selected
    // (by composite desc), then the rest.
    let mut display_idx: Vec<usize> = (0..m).collect();
    display_idx.sort_by(|&a, &b| {
        let sel_a = selected_set.contains(&a);
        let sel_b = selected_set.contains(&b);
        sel_b
            .cmp(&sel_a)
            .then_with(|| scores[b].cmp(&scores[a]))
            .then_with(|| {
                rows[a]
                    .features
                    .wallet_hex
                    .cmp(&rows[b].features.wallet_hex)
            })
    });
    display_idx
        .into_iter()
        .map(|i| CompositeResult {
            wallet_hex: rows[i].features.wallet_hex.clone(),
            composite_score_bps: scores[i],
            skill_pvalue_bps: rows[i].skill_pvalue_bps,
            sharpe_bps: rows[i].features.sharpe_bps,
            bhq_significant: bhq_significant[i],
            selected: selected_set.contains(&i),
        })
        .collect()
}

/// Cohort-wide μ and σ per feature (z-score basis). σ of `0` is preserved so
/// the scorer can short-circuit to a `0` contribution for that feature without
/// dividing by zero.
struct CohortStats {
    sharpe_bps: (Decimal, Decimal),
    ev_mean_bps: (Decimal, Decimal),
    ev_tstat_bps: (Decimal, Decimal),
    bb_shrunk_edge_bps: (Decimal, Decimal),
    kelly_log_growth_bps: (Decimal, Decimal),
    brier_score_bps: (Decimal, Decimal),
    brier_resolution_bps: (Decimal, Decimal),
    concentration_hhi_bps: (Decimal, Decimal),
    concentration_n_eff_bps: (Decimal, Decimal),
    concentration_rpc_bps: (Decimal, Decimal),
    first_entries_per_active_day_bps: (Decimal, Decimal),
    median_first_entry_to_resolution_secs: (Decimal, Decimal),
}

impl CohortStats {
    fn from_rows(rows: &[WalletFeatures]) -> Self {
        let extract: &[fn(&WalletFeatures) -> Decimal] = &[
            |w| Decimal::from(w.features.sharpe_bps),
            |w| Decimal::from(w.features.ev_mean_bps),
            |w| Decimal::from(w.features.ev_tstat_bps),
            |w| Decimal::from(w.features.bb_shrunk_edge_bps),
            |w| Decimal::from(w.features.kelly_log_growth_bps),
            |w| Decimal::from(w.features.brier_score_bps),
            |w| Decimal::from(w.features.brier_resolution_bps),
            |w| Decimal::from(w.features.concentration_hhi_bps),
            |w| Decimal::from(w.features.concentration_n_eff_bps),
            |w| Decimal::from(w.features.concentration_rpc_bps),
            |w| Decimal::from(w.features.first_entries_per_active_day_bps),
            |w| Decimal::from(w.features.median_first_entry_to_resolution_secs),
        ];
        let stats: Vec<(Decimal, Decimal)> = extract.iter().map(|f| mean_std(rows, *f)).collect();
        // Position-indexed unpack to keep the struct labels honest with the closure order above.
        Self {
            sharpe_bps: stats[0],
            ev_mean_bps: stats[1],
            ev_tstat_bps: stats[2],
            bb_shrunk_edge_bps: stats[3],
            kelly_log_growth_bps: stats[4],
            brier_score_bps: stats[5],
            brier_resolution_bps: stats[6],
            concentration_hhi_bps: stats[7],
            concentration_n_eff_bps: stats[8],
            concentration_rpc_bps: stats[9],
            first_entries_per_active_day_bps: stats[10],
            median_first_entry_to_resolution_secs: stats[11],
        }
    }
}

/// Population mean + standard deviation of `f(row)` over `rows`. Empty input
/// returns `(0, 0)`; `n=1` returns `(x, 0)` (no dispersion). All `Decimal`.
fn mean_std(rows: &[WalletFeatures], f: fn(&WalletFeatures) -> Decimal) -> (Decimal, Decimal) {
    let n = rows.len();
    if n == 0 {
        return (Decimal::ZERO, Decimal::ZERO);
    }
    let n_dec = Decimal::from(u64::try_from(n).unwrap_or(u64::MAX));
    let mean: Decimal = rows.iter().map(f).sum::<Decimal>() / n_dec;
    if n < 2 {
        return (mean, Decimal::ZERO);
    }
    let mut m2 = Decimal::ZERO;
    for r in rows {
        let d = f(r) - mean;
        m2 += d * d;
    }
    m2 /= n_dec;
    let std = m2.sqrt().unwrap_or(Decimal::ZERO);
    (mean, std)
}

/// Composite score `Σ w_i · z_i` for one wallet, where `z_i = (x_i − μ) / σ`.
/// `σ = 0` for any feature contributes `0` (the cohort has no spread on that
/// feature, so it offers no signal). Weight `w_i` is the configured bps weight
/// divided by 10⁴ (so `1000` bps = 0.10).
///
/// Module-private (and `CohortStats` along with it): external callers always
/// go through [`rank_by_composite`], which owns the cohort-stats build and
/// the per-row scoring loop together.
fn composite_score(
    row: &WalletFeatures,
    weights: &CompositeWeights,
    stats: &CohortStats,
) -> Decimal {
    let z = |x: Decimal, (mu, sigma): (Decimal, Decimal)| {
        if sigma.is_zero() {
            Decimal::ZERO
        } else {
            (x - mu) / sigma
        }
    };
    let w = |b: i32| Decimal::from(b) / Decimal::from(10_000i32);
    let f = &row.features;

    w(weights.w_sharpe_bps) * z(Decimal::from(f.sharpe_bps), stats.sharpe_bps)
        + w(weights.w_ev_mean_bps) * z(Decimal::from(f.ev_mean_bps), stats.ev_mean_bps)
        + w(weights.w_ev_tstat_bps) * z(Decimal::from(f.ev_tstat_bps), stats.ev_tstat_bps)
        + w(weights.w_bb_shrunk_edge_bps)
            * z(
                Decimal::from(f.bb_shrunk_edge_bps),
                stats.bb_shrunk_edge_bps,
            )
        + w(weights.w_kelly_log_growth_bps)
            * z(
                Decimal::from(f.kelly_log_growth_bps),
                stats.kelly_log_growth_bps,
            )
        + w(weights.w_brier_score_bps) * z(Decimal::from(f.brier_score_bps), stats.brier_score_bps)
        + w(weights.w_brier_resolution_bps)
            * z(
                Decimal::from(f.brier_resolution_bps),
                stats.brier_resolution_bps,
            )
        + w(weights.w_concentration_hhi_bps)
            * z(
                Decimal::from(f.concentration_hhi_bps),
                stats.concentration_hhi_bps,
            )
        + w(weights.w_concentration_n_eff_bps)
            * z(
                Decimal::from(f.concentration_n_eff_bps),
                stats.concentration_n_eff_bps,
            )
        + w(weights.w_concentration_rpc_bps)
            * z(
                Decimal::from(f.concentration_rpc_bps),
                stats.concentration_rpc_bps,
            )
        + w(weights.w_first_entries_per_active_day_bps)
            * z(
                Decimal::from(f.first_entries_per_active_day_bps),
                stats.first_entries_per_active_day_bps,
            )
        + w(weights.w_median_first_entry_to_resolution_secs)
            * z(
                Decimal::from(f.median_first_entry_to_resolution_secs),
                stats.median_first_entry_to_resolution_secs,
            )
}

/// Benjamini–Hochberg FDR gate at `q_bps`. Identical procedure to the one in
/// `selection.rs`; duplicated to keep modules independent (a tiny refactor
/// later could lift this into a shared helper).
fn bhq_gate(rows: &[WalletFeatures], q_bps: u32) -> Vec<bool> {
    let m = rows.len();
    if m == 0 {
        return Vec::new();
    }
    let mut order: Vec<usize> = (0..m).collect();
    order.sort_by(|&a, &b| {
        rows[a]
            .skill_pvalue_bps
            .cmp(&rows[b].skill_pvalue_bps)
            .then_with(|| {
                rows[a]
                    .features
                    .wallet_hex
                    .cmp(&rows[b].features.wallet_hex)
            })
    });
    // Largest k with p_(k) ≤ (k/m)·q. Compare in bps-scaled integer form to
    // avoid an extra Decimal round-trip per row.
    let m_u64 = u64::try_from(m).unwrap_or(u64::MAX);
    let q_u64 = u64::from(q_bps);
    let mut k_star: Option<usize> = None;
    for (rank0, &idx) in order.iter().enumerate() {
        let k_u64 = u64::try_from(rank0 + 1).unwrap_or(u64::MAX);
        // Threshold: p ≤ (k/m)·q  ⇔  p·m ≤ k·q  (both sides u64).
        if u64::from(rows[idx].skill_pvalue_bps).saturating_mul(m_u64) <= k_u64 * q_u64 {
            k_star = Some(rank0 + 1);
        }
    }
    let mut significant = vec![false; m];
    if let Some(k) = k_star {
        for &idx in order.iter().take(k) {
            significant[idx] = true;
        }
    }
    significant
}

/// Decimal → i64 bps, saturating. Mirrors the helper in `features.rs` / `selection.rs`.
fn decimal_to_bps_i64(x: Decimal) -> i64 {
    let bps = (x * Decimal::from(10_000i64))
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::MidpointNearestEven);
    bps.to_i64().unwrap_or(if bps.is_sign_negative() {
        i64::MIN
    } else {
        i64::MAX
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::features::DeterministicFeatures;
    use rust_decimal_macros::dec;

    fn wf(hex: &str, sharpe: i64, ev_mean: i64, hhi: i32, days: u32, p_bps: u32) -> WalletFeatures {
        WalletFeatures {
            features: DeterministicFeatures {
                wallet_hex: hex.to_owned(),
                cutoff_unix: 100,
                reconstruction_quality: 100,
                closed_trades: 25,
                distinct_markets: 20,
                distinct_events: 15,
                total_pnl_usd: dec!(0.0),
                roi_bps: 0,
                win_rate_bps: 5_000,
                avg_hold_secs: 0,
                trading_days: days,
                mean_daily_return_bps: 0,
                std_daily_return_bps: 0,
                sharpe_bps: sharpe,
                skewness_bps: 0,
                excess_kurtosis_bps: 0,
                lcb_5pct_bps: 0,
                ev_mean_bps: ev_mean,
                ev_tstat_bps: 0,
                bb_shrunk_edge_bps: 0,
                kelly_log_growth_bps: 0,
                brier_score_bps: 0,
                brier_resolution_bps: 0,
                concentration_hhi_bps: hhi,
                concentration_n_eff_bps: 0,
                concentration_rpc_bps: 0,
                first_entries_per_active_day_bps: 0,
                median_first_entry_to_resolution_secs: 0,
            },
            extracted_at_unix: 1,
            skill_pnl_usd: dec!(0.0),
            skill_pvalue_bps: p_bps,
            skill_permutations: 999,
        }
    }

    #[test]
    fn z_score_zero_when_cohort_has_no_dispersion() {
        // All wallets identical → σ=0 → composite is the empty sum = 0 for every wallet.
        let rows = vec![
            wf("0xa", 1000, 100, 1000, 25, 10),
            wf("0xb", 1000, 100, 1000, 25, 10),
        ];
        let stats = CohortStats::from_rows(&rows);
        let score = composite_score(&rows[0], &CompositeWeights::default(), &stats);
        assert_eq!(score, Decimal::ZERO);
    }

    /// Build a `CompositeWeights` with all weights at zero except `w_sharpe_bps`.
    /// Lets single-feature tests express "rank by Sharpe alone" without
    /// reassigning every field on a `default()` instance (which would trip
    /// `clippy::field_reassign_with_default`).
    fn weights_sharpe_only(w_sharpe_bps: i32) -> CompositeWeights {
        CompositeWeights {
            w_sharpe_bps,
            w_ev_mean_bps: 0,
            w_ev_tstat_bps: 0,
            w_bb_shrunk_edge_bps: 0,
            w_kelly_log_growth_bps: 0,
            w_brier_score_bps: 0,
            w_brier_resolution_bps: 0,
            w_concentration_hhi_bps: 0,
            w_concentration_n_eff_bps: 0,
            w_concentration_rpc_bps: 0,
            w_first_entries_per_active_day_bps: 0,
            w_median_first_entry_to_resolution_secs: 0,
        }
    }

    #[test]
    fn higher_sharpe_outscores_lower_when_only_sharpe_is_weighted() {
        // Single feature carries all the weight; ranking must follow sharpe_bps.
        let rows = vec![
            wf("0xlow", 1_000, 0, 0, 25, 10),
            wf("0xmid", 5_000, 0, 0, 25, 10),
            wf("0xhigh", 10_000, 0, 0, 25, 10),
        ];
        let out = rank_by_composite(&rows, &weights_sharpe_only(10_000), 1_000, 10, 0);
        let selected: Vec<&str> = out
            .iter()
            .filter(|r| r.selected)
            .map(|r| r.wallet_hex.as_str())
            .collect();
        assert_eq!(selected, vec!["0xhigh", "0xmid", "0xlow"]);
    }

    #[test]
    fn negative_weight_inverts_direction() {
        // Same data, but flip the sharpe weight to negative → lowest sharpe wins.
        let rows = vec![
            wf("0xlow", 1_000, 0, 0, 25, 10),
            wf("0xmid", 5_000, 0, 0, 25, 10),
            wf("0xhigh", 10_000, 0, 0, 25, 10),
        ];
        let out = rank_by_composite(&rows, &weights_sharpe_only(-10_000), 1_000, 10, 0);
        let selected: Vec<&str> = out
            .iter()
            .filter(|r| r.selected)
            .map(|r| r.wallet_hex.as_str())
            .collect();
        assert_eq!(selected, vec!["0xlow", "0xmid", "0xhigh"]);
    }

    #[test]
    fn bhq_gate_filters_before_composite_ranks() {
        // Two BHq-significant wallets (p=10) and one not (p=5000) — the latter
        // never enters the composite-rank candidate set even if its features dominate.
        let rows = vec![
            wf("0xfail", 100_000, 100_000, 0, 25, 5_000),
            wf("0xa", 1_000, 0, 0, 25, 10),
            wf("0xb", 2_000, 0, 0, 25, 10),
        ];
        let out = rank_by_composite(&rows, &CompositeWeights::default(), 1_000, 10, 0);
        let selected: Vec<&str> = out
            .iter()
            .filter(|r| r.selected)
            .map(|r| r.wallet_hex.as_str())
            .collect();
        // 0xfail is BHq-significant only if the FDR critical p at its rank is ≥ 5000;
        // with m=3 and q_bps=1000, the highest critical at rank-3 is (3/3)·0.10 = 0.10
        // = 1000 bps, so 5000 > 1000 → not significant; the composite ranks only 0xa/0xb.
        assert_eq!(selected.len(), 2);
        assert!(!selected.contains(&"0xfail"));
    }

    #[test]
    fn min_trading_days_excludes_low_day_wallets() {
        // Two BHq-significant wallets; the one with too-few days is excluded
        // from `selected` while staying in the BHq-significant cohort (still
        // gets a composite score, just doesn't rank).
        let rows = vec![
            wf("0xfew", 50_000, 0, 0, 2, 10),
            wf("0xmany", 1_000, 0, 0, 25, 10),
        ];
        let out = rank_by_composite(&rows, &CompositeWeights::default(), 1_000, 10, 20);
        let selected: Vec<&str> = out
            .iter()
            .filter(|r| r.selected)
            .map(|r| r.wallet_hex.as_str())
            .collect();
        assert_eq!(selected, vec!["0xmany"]);
        // Both still report `bhq_significant=true`.
        assert!(out.iter().all(|r| r.bhq_significant));
    }

    #[test]
    fn top_n_caps_selected_set() {
        let rows = vec![
            wf("0xa", 1_000, 0, 0, 25, 10),
            wf("0xb", 2_000, 0, 0, 25, 10),
            wf("0xc", 3_000, 0, 0, 25, 10),
        ];
        let out = rank_by_composite(&rows, &CompositeWeights::default(), 1_000, 2, 0);
        let selected = out.iter().filter(|r| r.selected).count();
        assert_eq!(selected, 2);
    }
}
