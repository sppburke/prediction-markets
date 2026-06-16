//! Per-wallet realized-edge statistics for the live-watchlist maintenance tick
//! (#350 WS1). Pure and deterministic: groups **settled** paper fills by leader
//! (from the local, authoritative `paper_state.db` `list_fills()` + the in-process
//! [`ResolutionStore`] — never the best-effort Supabase mirror), reuses
//! [`pe_paper_pnl::realized_edge`] for each fill's realized P&L (so the "loser"
//! verdict is definitionally identical to the dashboard's realized P&L), and layers
//! empirical-Bernstein confidence bounds on top.
//!
//! `rust_decimal` throughout — no `f64`. No I/O beyond reading the already-loaded
//! `fills` slice and the in-memory resolution store.
//!
//! This module only *computes* statistics; the maintenance tick that consumes them
//! (eviction / demotion / backfill) lands in PR-D. `α` and `min_trades` are passed
//! in by the caller (config-backed in PR-C/PR-D), so this module introduces no
//! tunable threshold of its own.
//!
//! ## Confidence-bound input (validity)
//!
//! Empirical-Bernstein bounds require a bounded sample. The bounds therefore consume
//! the **per-share** edge `realized_edge / contracts = side_sign × (resolved −
//! fill_price)`, which is bounded in `[-1, 1]` for binary settlement (`resolved ∈
//! {0,1}`, `fill_price ∈ [0,1]`) — so the empirical-Bernstein range constant is
//! `R = 2`. The realized-P&L gate, by contrast, uses the **dollar** sum of per-fill
//! edges. The bounded-range premise is fixed for validity; the precise constants /
//! `α` are post-ship-tunable (#350 open risk).

use std::collections::HashMap;

use pe_paper_pnl::{ResolutionStore, realized_edge};
use pe_paper_state::FillRow;
use rust_decimal::{Decimal, MathematicalOps};

use crate::paper_api::ParsedKey;

/// One settled fill's realized edge, in both the dollar and per-share forms.
#[derive(Debug, Clone, Copy)]
struct SettledEdge {
    /// Dollar realized P&L for this fill (`pe_paper_pnl::realized_edge`).
    dollar: Decimal,
    /// Per-share edge `= dollar / contracts ∈ [-1, 1]` (the CB sample).
    per_share: Decimal,
}

/// Realized-edge statistics for one leader's settled fills.
///
/// `lower_cb` / `upper_cb` are one-sided empirical-Bernstein confidence bounds on the
/// **mean per-share edge** at the level `α` passed to [`wallet_edge_stats`]; they are
/// `None` when fewer than two settled fills exist (the unbiased sample variance is
/// undefined). `realized_pnl` is the **dollar** sum of per-fill realized edges.
#[derive(Debug, Clone)]
pub struct WalletEdgeStats {
    /// Number of settled fills contributing to the series.
    pub settled_count: usize,
    /// Dollar sum of per-fill realized edges — the realized-P&L gate input.
    pub realized_pnl: Decimal,
    /// Lower one-sided empirical-Bernstein CB on the mean per-share edge.
    pub lower_cb: Option<Decimal>,
    /// Upper one-sided empirical-Bernstein CB on the mean per-share edge.
    pub upper_cb: Option<Decimal>,
}

impl WalletEdgeStats {
    /// Underperformance knockout: **all three** must hold — `upper_cb < 0`,
    /// `realized_pnl < 0`, and `settled_count ≥ min_trades`. The `realized_pnl < 0`
    /// conjunct is the safety net: a wallet with realized P&L `≥ 0` is **never**
    /// demoted, regardless of the (post-ship-tunable) CB constants.
    pub fn should_demote(&self, min_trades: usize) -> bool {
        self.settled_count >= min_trades
            && self.realized_pnl < Decimal::ZERO
            && matches!(self.upper_cb, Some(u) if u < Decimal::ZERO)
    }

    /// Proven-winner predicate for the inactivity keep-exception: `lower_cb > 0` AND
    /// `settled_count ≥ min_trades`. A proven winner is spared 72h-inactivity
    /// eviction (up to the 7d hard cap, enforced by the caller).
    pub fn is_proven_winner(&self, min_trades: usize) -> bool {
        self.settled_count >= min_trades && matches!(self.lower_cb, Some(l) if l > Decimal::ZERO)
    }
}

/// Build per-leader realized-edge statistics from the local paper-state fills and the
/// in-process resolution store, at confidence level `alpha`.
///
/// Only **settled** fills (market present in `resolutions`) carrying a parseable
/// Winner-Follow leader (`ParsedKey::from_key`) and non-zero `contracts` contribute.
/// Open fills, legacy/foreign idempotency keys, and zero-contract fills are skipped.
/// The returned map is keyed by the leader hex parsed from the idempotency key.
pub fn wallet_edge_stats(
    fills: &[FillRow],
    resolutions: &ResolutionStore,
    alpha: Decimal,
) -> HashMap<String, WalletEdgeStats> {
    let mut series: HashMap<String, Vec<SettledEdge>> = HashMap::new();
    for f in fills {
        // Settled only: an open market has no settlement info, so no realized edge.
        let Some(info) = resolutions.settlement_info(&f.market_id) else {
            continue;
        };
        // Leader must be parseable from the Winner-Follow idempotency key.
        let Some(leader) = ParsedKey::from_key(&f.idempotency_key).leader else {
            continue;
        };
        // Guard the per-share division; a zero-contract fill carries no edge anyway.
        if f.contracts == 0 {
            continue;
        }
        let dollar = realized_edge(f, &info);
        let Some(per_share) = dollar.checked_div(Decimal::from(f.contracts)) else {
            continue;
        };
        series
            .entry(leader)
            .or_default()
            .push(SettledEdge { dollar, per_share });
    }

    series
        .into_iter()
        .map(|(leader, edges)| (leader, stats_from_series(&edges, alpha)))
        .collect()
}

/// Reduce one leader's settled-edge series to its [`WalletEdgeStats`].
fn stats_from_series(edges: &[SettledEdge], alpha: Decimal) -> WalletEdgeStats {
    let settled_count = edges.len();
    let realized_pnl = edges
        .iter()
        .map(|e| e.dollar)
        .fold(Decimal::ZERO, |acc, d| acc.checked_add(d).unwrap_or(acc));

    let per_share: Vec<Decimal> = edges.iter().map(|e| e.per_share).collect();
    let (lower_cb, upper_cb) = match empirical_bernstein_half_width(&per_share, alpha) {
        Some((mean, half_width)) => (mean.checked_sub(half_width), mean.checked_add(half_width)),
        None => (None, None),
    };

    WalletEdgeStats {
        settled_count,
        realized_pnl,
        lower_cb,
        upper_cb,
    }
}

/// Empirical-Bernstein (Maurer & Pontil, 2009) one-sided confidence half-width at
/// level `alpha`, over a `sample` of per-share edges bounded in `[-1, 1]` (range
/// `R = 2`). Returns `(mean, half_width)`, or `None` when `sample.len() < 2` (the
/// unbiased sample variance is undefined) or any `rust_decimal` step overflows.
///
/// `half_width = sqrt(2·V·L / n) + 7·R·L / (3·(n−1))`, where `V` is the unbiased
/// sample variance and `L = ln(2/α)`. `mean + half_width` is an upper CB on the true
/// mean; `mean − half_width` is a lower CB. The constants / `α` are post-ship-tunable
/// (#350); the bounded-range premise `R = 2` is fixed for validity.
fn empirical_bernstein_half_width(
    sample: &[Decimal],
    alpha: Decimal,
) -> Option<(Decimal, Decimal)> {
    let n = sample.len();
    if n < 2 {
        return None;
    }
    let n_dec = Decimal::from(u64::try_from(n).unwrap_or(u64::MAX));
    let n_minus_1 = Decimal::from(u64::try_from(n - 1).unwrap_or(u64::MAX));

    // Sample mean.
    let sum = sample
        .iter()
        .try_fold(Decimal::ZERO, |acc, &x| acc.checked_add(x))?;
    let mean = sum.checked_div(n_dec)?;

    // Unbiased sample variance V = Σ(x − mean)² / (n − 1).
    let mut sq_sum = Decimal::ZERO;
    for &x in sample {
        let d = x.checked_sub(mean)?;
        sq_sum = sq_sum.checked_add(d.checked_mul(d)?)?;
    }
    let variance = sq_sum.checked_div(n_minus_1)?;

    let two = Decimal::from(2u64);
    let l = two.checked_div(alpha)?.checked_ln()?; // ln(2/α); checked → None on non-positive

    // term1 = sqrt(2·V·L / n).
    let inside = two
        .checked_mul(variance)?
        .checked_mul(l)?
        .checked_div(n_dec)?;
    let term1 = inside.sqrt()?; // inside ≥ 0 for α < 2

    // term2 = 7·R·L / (3·(n−1)), with the per-share range R = 2.
    let numerator = Decimal::from(7u64).checked_mul(two)?.checked_mul(l)?;
    let denominator = Decimal::from(3u64).checked_mul(n_minus_1)?;
    let term2 = numerator.checked_div(denominator)?;

    let half_width = term1.checked_add(term2)?;
    Some((mean, half_width))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use pe_core_types::{MarketId, OutcomeId, Price, Side, VenueMarketId};
    use pe_paper_state::PaperStateDb;
    use rust_decimal_macros::dec;

    const ALPHA: Decimal = dec!(0.10);

    fn mid(s: &str) -> MarketId {
        MarketId(VenueMarketId(s.to_string()))
    }

    /// A fill whose idempotency key encodes `leader` at index 1 of the pipe-delimited
    /// Winner-Follow key (`wf|leader|src|market|outcome|side|observed_at`).
    fn fill(
        leader: &str,
        market: &str,
        outcome: u16,
        side: Side,
        contracts: u64,
        price: Decimal,
    ) -> FillRow {
        let side_str = match side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        };
        FillRow {
            idempotency_key: format!("wf|{leader}|0xsrc|{market}|{outcome}|{side_str}|1700000000"),
            market_id: mid(market),
            outcome_id: OutcomeId(outcome),
            side,
            contracts,
            fill_price: Price(price),
            event_seq: 1,
        }
    }

    /// A resolution store seeded with the given settled markets (price array + credit).
    fn store_with(
        settled: &[(&str, Vec<Decimal>, Decimal)],
    ) -> (tempfile::TempDir, ResolutionStore) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
        let mut store = ResolutionStore::load(db).unwrap();
        for (m, prices, credit) in settled {
            store
                .mark_settled(mid(m), prices.clone(), *credit, 1_700_000_000)
                .unwrap();
        }
        (dir, store)
    }

    // ── empirical_bernstein_half_width ─────────────────────────────────────────

    #[test]
    fn eb_returns_none_below_two_samples() {
        assert!(empirical_bernstein_half_width(&[], ALPHA).is_none());
        assert!(empirical_bernstein_half_width(&[dec!(-0.5)], ALPHA).is_none());
    }

    #[test]
    fn eb_upper_bound_goes_negative_for_a_clear_loser() {
        // 40 fills all at per-share −0.5 (zero variance): mean is exact, and with a
        // large-enough n the additive term shrinks below |mean|, so the UPPER CB < 0.
        let sample = vec![dec!(-0.5); 40];
        let (mean, half_width) = empirical_bernstein_half_width(&sample, ALPHA).unwrap();
        assert_eq!(mean, dec!(-0.5));
        assert!(half_width > Decimal::ZERO);
        let upper = mean + half_width;
        assert!(upper < Decimal::ZERO, "upper CB should be < 0, got {upper}");
    }

    #[test]
    fn eb_lower_bound_goes_positive_for_a_clear_winner() {
        // Mirror image: 40 fills at per-share +0.5 → LOWER CB > 0 (proven winner).
        let sample = vec![dec!(0.5); 40];
        let (mean, half_width) = empirical_bernstein_half_width(&sample, ALPHA).unwrap();
        assert_eq!(mean, dec!(0.5));
        let lower = mean - half_width;
        assert!(lower > Decimal::ZERO, "lower CB should be > 0, got {lower}");
    }

    #[test]
    fn eb_small_zero_mean_sample_straddles_zero() {
        // n = 10 at the mean 0: the band is wide, so neither bound clears 0 — the
        // "unproven middle" that is kept while active but evicted on 72h inactivity.
        let sample = vec![dec!(0); 10];
        let (mean, half_width) = empirical_bernstein_half_width(&sample, ALPHA).unwrap();
        assert_eq!(mean, dec!(0));
        assert!(half_width > Decimal::ZERO);
        assert!(mean + half_width > Decimal::ZERO);
        assert!(mean - half_width < Decimal::ZERO);
    }

    // ── predicates ─────────────────────────────────────────────────────────────

    #[test]
    fn should_demote_requires_all_three_conjuncts() {
        let base = WalletEdgeStats {
            settled_count: 40,
            realized_pnl: dec!(-200),
            lower_cb: Some(dec!(-0.9)),
            upper_cb: Some(dec!(-0.14)),
        };
        assert!(base.should_demote(10));

        // realized P&L ≥ 0 is the safety net — never demote (AC-explicit).
        let profitable = WalletEdgeStats {
            realized_pnl: dec!(5),
            ..base.clone()
        };
        assert!(!profitable.should_demote(10));

        // upper CB ≥ 0 → not a proven loser.
        let unproven = WalletEdgeStats {
            upper_cb: Some(dec!(0.01)),
            ..base.clone()
        };
        assert!(!unproven.should_demote(10));

        // too few settled fills → no knockout on a small sample.
        let small = WalletEdgeStats {
            settled_count: 9,
            ..base.clone()
        };
        assert!(!small.should_demote(10));

        // no CB at all (n < 2) → not demoted.
        let no_cb = WalletEdgeStats {
            upper_cb: None,
            ..base
        };
        assert!(!no_cb.should_demote(10));
    }

    #[test]
    fn is_proven_winner_requires_positive_lower_cb_and_enough_trades() {
        let winner = WalletEdgeStats {
            settled_count: 40,
            realized_pnl: dec!(200),
            lower_cb: Some(dec!(0.14)),
            upper_cb: Some(dec!(0.9)),
        };
        assert!(winner.is_proven_winner(10));

        // lower CB ≤ 0 → not proven.
        let unproven = WalletEdgeStats {
            lower_cb: Some(dec!(0)),
            ..winner.clone()
        };
        assert!(!unproven.is_proven_winner(10));

        // too few settled fills → cannot be proven.
        let small = WalletEdgeStats {
            settled_count: 9,
            ..winner.clone()
        };
        assert!(!small.is_proven_winner(10));

        // no CB (n < 2) → not proven.
        let no_cb = WalletEdgeStats {
            lower_cb: None,
            ..winner
        };
        assert!(!no_cb.is_proven_winner(10));
    }

    // ── wallet_edge_stats (series build over real fills + resolutions) ──────────

    #[test]
    fn wallet_edge_stats_groups_by_leader_and_sums_dollar_pnl() {
        // 0xA: two settled losers on 0xm (YES resolved to 0): BUY YES 100 @0.50 → −50,
        //      BUY YES 50 @0.40 → −20. realized_pnl = −70, per-share each −0.50 / −0.40.
        // 0xB: one settled winner on 0xn (YES resolved to 1): BUY YES 100 @0.30 → +70.
        let (_d, store) = store_with(&[
            ("0xm", vec![dec!(0), dec!(1)], dec!(0)),
            ("0xn", vec![dec!(1), dec!(0)], dec!(100)),
        ]);
        let fills = vec![
            fill("0xA", "0xm", 0, Side::Buy, 100, dec!(0.50)),
            fill("0xA", "0xm", 0, Side::Buy, 50, dec!(0.40)),
            fill("0xB", "0xn", 0, Side::Buy, 100, dec!(0.30)),
            // Open market (not in the store) → skipped.
            fill("0xA", "0xopen", 0, Side::Buy, 100, dec!(0.50)),
            // Foreign/legacy key (not "wf|...") → skipped.
            FillRow {
                idempotency_key: "legacy-key".to_string(),
                market_id: mid("0xm"),
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                contracts: 100,
                fill_price: Price(dec!(0.50)),
                event_seq: 1,
            },
            // Zero-contract fill → skipped (guards the per-share division).
            fill("0xA", "0xm", 0, Side::Buy, 0, dec!(0.50)),
        ];

        let stats = wallet_edge_stats(&fills, &store, ALPHA);
        assert_eq!(stats.len(), 2, "only 0xA and 0xB have settled wf fills");

        let a = stats.get("0xA").unwrap();
        assert_eq!(
            a.settled_count, 2,
            "open/foreign/zero-contract fills excluded"
        );
        assert_eq!(a.realized_pnl, dec!(-70)); // −50 + −20

        let b = stats.get("0xB").unwrap();
        assert_eq!(b.settled_count, 1);
        assert_eq!(b.realized_pnl, dec!(70));
        // One sample → no CB (variance undefined).
        assert!(b.lower_cb.is_none() && b.upper_cb.is_none());

        // 0xA's realized_pnl equals Σ realized_edge over its settled fills — the
        // structural-reuse guarantee the demotion verdict depends on.
        let a_info = store.settlement_info(&mid("0xm")).unwrap();
        let expected: Decimal =
            realized_edge(&fill("0xA", "0xm", 0, Side::Buy, 100, dec!(0.50)), &a_info)
                + realized_edge(&fill("0xA", "0xm", 0, Side::Buy, 50, dec!(0.40)), &a_info);
        assert_eq!(a.realized_pnl, expected);
    }

    #[test]
    fn wallet_edge_stats_demotes_a_proven_losing_leader() {
        // 40 settled losing fills for one leader: BUY YES 100 @0.50, YES → 0, each
        // dollar −50, per-share −0.50. With n = 40 the upper CB clears below 0, so the
        // full underperformance knockout fires.
        let (_d, store) = store_with(&[("0xm", vec![dec!(0), dec!(1)], dec!(0))]);
        let fills: Vec<FillRow> = (0..40)
            .map(|_| fill("0xLOSER", "0xm", 0, Side::Buy, 100, dec!(0.50)))
            .collect();

        let stats = wallet_edge_stats(&fills, &store, ALPHA);
        let s = stats.get("0xLOSER").unwrap();
        assert_eq!(s.settled_count, 40);
        assert_eq!(s.realized_pnl, dec!(-2000)); // 40 × −50
        assert!(s.should_demote(10), "proven loser with n=40 should demote");
        assert!(!s.is_proven_winner(10));
    }
}
