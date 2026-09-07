//! Per-wallet realized-edge statistics for the live-watchlist maintenance tick
//! (#350 WS1, dollar-gate rework per the 2026-07-01 ranker decision record item 3.1).
//! Pure and deterministic: groups **settled** paper fills by leader (from the local,
//! authoritative `paper_state.db` `list_fills()` + the in-process [`ResolutionStore`]
//! — never the best-effort Supabase mirror), reuses [`pe_paper_pnl::realized_edge`]
//! for each fill's realized P&L (so the "loser" verdict is definitionally identical
//! to the dashboard's realized P&L), and layers empirical-Bernstein confidence
//! bounds on top.
//!
//! `rust_decimal` throughout — no `f64`. No I/O beyond reading the already-loaded
//! `fills` slice and the in-memory resolution store.
//!
//! ## Confidence-bound input (validity)
//!
//! The bounds consume the per-fill **dollar** realized edge, with the **observed
//! range** `R̂ = max − min` as the bounded-support proxy (Maurer-Pontil empirical
//! variant) — the same series/constant scheme as the CI-tested ranker-harness
//! template (`scripts/ranker/demotion.py`), keeping the live gate and the harness
//! in formula parity (the harness↔live parity requirement #440 also carries; note
//! #440's *scope* text still describes the pre-rework per-share/lifetime semantics
//! and should be read against this module as the new baseline). The previous
//! per-share gate with
//! the fixed worst-case range `R = 2` was structurally unable to fire below 15
//! settled fills (`term2 = 7·2·ln(2/α)/(3·(n−1)) > 1` for all `n ≤ 14` at α = 0.10,
//! exceeding the entire per-share support) — a measured live bleed the gate could
//! never cut.
//!
//! ## Optional-stopping caveat (#440)
//!
//! This remains a FIXED-n Maurer-Pontil bound queried on a growing `n` each tick,
//! so the nominal `α` is not a time-uniform ("anytime-valid") guarantee over the
//! trajectory. Accepted here as in the harness: demotion stays AND-gated by the
//! trailing-window realized-P&L conjunct and `min_trades`, and the docs/19 hard
//! stop-losses bound damage independently. The confidence-sequence upgrade is
//! issue #440.

use std::collections::HashMap;

use pe_paper_pnl::{ResolutionStore, realized_edge};
use pe_paper_state::FillRow;
use rust_decimal::{Decimal, MathematicalOps};

use crate::paper_api::ParsedKey;

/// One settled fill's realized edge and when it settled.
#[derive(Debug, Clone, Copy)]
struct SettledEdge {
    /// Dollar realized P&L for this fill (`pe_paper_pnl::realized_edge`) — the CB sample.
    dollar: Decimal,
    /// Settlement time of the fill's market (`SettlementInfo::settled_at_unix`).
    settled_at_unix: i64,
}

/// Realized-edge statistics for one leader's settled fills.
///
/// `lower_cb` / `upper_cb` are one-sided empirical-Bernstein confidence bounds on the
/// **mean per-fill dollar edge** at the level `α` passed to [`wallet_edge_stats`]; they
/// are `None` when fewer than two settled fills exist (the unbiased sample variance is
/// undefined). `realized_pnl` is the lifetime dollar sum (audit); `windowed_pnl` is the
/// dollar sum over fills settled within the trailing P&L window — the demotion conjunct.
#[derive(Debug, Clone)]
pub struct WalletEdgeStats {
    /// Number of settled fills contributing to the series.
    pub settled_count: usize,
    /// Lifetime dollar sum of per-fill realized edges — audit/reporting only.
    pub realized_pnl: Decimal,
    /// Dollar sum of per-fill realized edges settled within the trailing window
    /// (`demotion_pnl_window_secs`) — the demotion safety-net conjunct. A large
    /// historical winner no longer carries an unbounded bleed allowance: only its
    /// recent window protects it.
    pub windowed_pnl: Decimal,
    /// Lower one-sided empirical-Bernstein CB on the mean per-fill dollar edge.
    pub lower_cb: Option<Decimal>,
    /// Upper one-sided empirical-Bernstein CB on the mean per-fill dollar edge.
    pub upper_cb: Option<Decimal>,
}

impl WalletEdgeStats {
    /// Underperformance knockout: **all three** must hold — `upper_cb < 0`,
    /// `windowed_pnl < 0`, and `settled_count ≥ min_trades`. The `windowed_pnl < 0`
    /// conjunct is the safety net: a wallet whose trailing-window realized P&L is
    /// `≥ 0` is **never** demoted, regardless of the CB constants. (Previously the
    /// conjunct was *lifetime* P&L, which granted a big historical winner an
    /// unbounded allowance to bleed.)
    pub fn should_demote(&self, min_trades: usize) -> bool {
        self.settled_count >= min_trades
            && self.windowed_pnl < Decimal::ZERO
            && matches!(self.upper_cb, Some(u) if u < Decimal::ZERO)
    }

    /// Proven-winner predicate for the inactivity keep-exception: `lower_cb > 0` AND
    /// `settled_count ≥ min_trades`. A proven winner is spared 72h-inactivity
    /// eviction (up to the 7d hard cap, enforced by the caller). Deliberately NOT
    /// windowed: sparing eviction is low-risk, and windowing would churn winners
    /// through quiet stretches.
    pub fn is_proven_winner(&self, min_trades: usize) -> bool {
        self.settled_count >= min_trades && matches!(self.lower_cb, Some(l) if l > Decimal::ZERO)
    }
}

/// Build per-leader realized-edge statistics from the local paper-state fills and the
/// in-process resolution store, at confidence level `alpha`. `now_unix` and
/// `pnl_window_secs` define the trailing window for [`WalletEdgeStats::windowed_pnl`]:
/// a fill counts as in-window when its market settled at or after
/// `now_unix − pnl_window_secs`.
///
/// Only **settled** fills (market present in `resolutions`) carrying a parseable
/// Winner-Follow leader (`ParsedKey::from_key`) and non-zero quantity contribute.
/// Open fills, legacy/foreign idempotency keys, and zero-quantity fills (which carry
/// no edge) are skipped. The returned map is keyed by the leader hex parsed from the
/// idempotency key.
pub fn wallet_edge_stats(
    fills: &[FillRow],
    resolutions: &ResolutionStore,
    alpha: Decimal,
    now_unix: i64,
    pnl_window_secs: u64,
) -> HashMap<String, WalletEdgeStats> {
    let window = i64::try_from(pnl_window_secs).unwrap_or(i64::MAX);
    let cutoff = now_unix.saturating_sub(window);

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
        // A zero-quantity fill carries no edge; skip it rather than dilute the sample.
        if f.quantity == pe_core_types::ShareAmount::ZERO {
            continue;
        }
        series.entry(leader).or_default().push(SettledEdge {
            dollar: realized_edge(f, &info),
            settled_at_unix: info.settled_at_unix,
        });
    }

    series
        .into_iter()
        .map(|(leader, edges)| (leader, stats_from_series(&edges, alpha, cutoff)))
        .collect()
}

/// Reduce one leader's settled-edge series to its [`WalletEdgeStats`]. `cutoff` is the
/// trailing-window start: edges settled at or after it contribute to `windowed_pnl`.
fn stats_from_series(edges: &[SettledEdge], alpha: Decimal, cutoff: i64) -> WalletEdgeStats {
    let settled_count = edges.len();
    let realized_pnl = edges
        .iter()
        .map(|e| e.dollar)
        .fold(Decimal::ZERO, |acc, d| acc.checked_add(d).unwrap_or(acc));
    let windowed_pnl = edges
        .iter()
        .filter(|e| e.settled_at_unix >= cutoff)
        .map(|e| e.dollar)
        .fold(Decimal::ZERO, |acc, d| acc.checked_add(d).unwrap_or(acc));

    let dollars: Vec<Decimal> = edges.iter().map(|e| e.dollar).collect();
    let (lower_cb, upper_cb) = match empirical_bernstein_half_width(&dollars, alpha) {
        Some((mean, half_width)) => (mean.checked_sub(half_width), mean.checked_add(half_width)),
        None => (None, None),
    };

    WalletEdgeStats {
        settled_count,
        realized_pnl,
        windowed_pnl,
        lower_cb,
        upper_cb,
    }
}

/// Empirical-Bernstein (Maurer & Pontil, 2009) one-sided confidence half-width at
/// level `alpha`, over a `sample` of per-fill dollar edges, with the **observed range**
/// `R̂ = max − min` (floored to 1 when the sample is constant) as the bounded-support
/// proxy. Returns `(mean, half_width)`, or `None` when `sample.len() < 2` (the
/// unbiased sample variance is undefined) or any `rust_decimal` step overflows.
///
/// `half_width = sqrt(2·V·L / n) + 3·R̂·L / n`, where `V` is the unbiased sample
/// variance and `L = ln(2/α)` — the exact formula of the CI-tested harness template
/// (`scripts/ranker/demotion.py`, `_demote_from_pnl`); harness↔live formula parity.
/// `mean + half_width` is an upper CB on the true mean; `mean − half_width` a lower CB.
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

    // Observed range R̂ = max − min, floored to 1 when the sample is constant
    // (template: `rng if rng > 0 else 1.0`).
    let mut lo = sample[0];
    let mut hi = sample[0];
    for &x in sample {
        if x < lo {
            lo = x;
        }
        if x > hi {
            hi = x;
        }
    }
    let mut range = hi.checked_sub(lo)?;
    if range <= Decimal::ZERO {
        range = Decimal::ONE;
    }

    let two = Decimal::from(2u64);
    let l = two.checked_div(alpha)?.checked_ln()?; // ln(2/α); checked → None on non-positive

    // term1 = sqrt(2·V·L / n).
    let inside = two
        .checked_mul(variance)?
        .checked_mul(l)?
        .checked_div(n_dec)?;
    let term1 = inside.sqrt()?; // inside ≥ 0 for α < 2

    // term2 = 3·R̂·L / n (observed-range Maurer-Pontil, template constant).
    let term2 = Decimal::from(3u64)
        .checked_mul(range)?
        .checked_mul(l)?
        .checked_div(n_dec)?;

    let half_width = term1.checked_add(term2)?;
    Some((mean, half_width))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use pe_core_types::{
        CollateralAmount, EventSeq, MarketId, OutcomeId, Price, ShareAmount, Side, VenueMarketId,
    };
    use pe_paper_state::PaperStateDb;
    use rust_decimal_macros::dec;

    const ALPHA: Decimal = dec!(0.10);
    const NOW: i64 = 1_900_000_000;
    const WINDOW: u64 = 2_592_000; // 30 d — glossary `demotion_pnl_window_secs`

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
        let quantity = ShareAmount::from_whole(contracts).unwrap();
        FillRow {
            idempotency_key: format!("wf|{leader}|0xsrc|{market}|{outcome}|{side_str}|1700000000"),
            market_id: mid(market),
            outcome_id: OutcomeId(outcome),
            side,
            quantity,
            fill_price: Price(price),
            principal: CollateralAmount::from_decimal_exact(price * quantity.to_decimal()).unwrap(),
            fee: CollateralAmount::ZERO,
            event_seq: EventSeq(1),
            prepared_seq: EventSeq(1),
            source_receipt_seq: None,
        }
    }

    /// A resolution store seeded with the given settled markets
    /// (price array + credit + settled-at unix).
    fn store_with(
        settled: &[(&str, Vec<Decimal>, Decimal, i64)],
    ) -> (tempfile::TempDir, ResolutionStore) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
        let mut store = ResolutionStore::load(db).unwrap();
        for (m, prices, credit, at) in settled {
            store
                .mark_settled(mid(m), prices.clone(), *credit, *at)
                .unwrap();
        }
        (dir, store)
    }

    // ── empirical_bernstein_half_width (template parity) ───────────────────────

    #[test]
    fn eb_returns_none_below_two_samples() {
        assert!(empirical_bernstein_half_width(&[], ALPHA).is_none());
        assert!(empirical_bernstein_half_width(&[dec!(-12.5)], ALPHA).is_none());
    }

    /// Template-parity vector (mixed, n = 8): the Python template
    /// (`scripts/ranker/demotion.py`, δ = 0.10) computes mean = −9.125,
    /// deviation = 31.841723, upper = +22.716723 → NOT demotable. The Rust port must
    /// agree to 1e-4 (Decimal vs binary-float sqrt/ln rounding).
    #[test]
    fn eb_matches_python_template_mixed_vector() {
        let sample = [
            dec!(-10),
            dec!(-5),
            dec!(-20),
            dec!(3),
            dec!(-8),
            dec!(-12),
            dec!(-6),
            dec!(-15),
        ];
        let (mean, half_width) = empirical_bernstein_half_width(&sample, ALPHA).unwrap();
        assert_eq!(mean, dec!(-9.125));
        let upper = mean + half_width;
        assert!(
            (upper - dec!(22.716723)).abs() < dec!(0.0001),
            "upper CB {upper} != template 22.716723"
        );
        assert!(
            upper > Decimal::ZERO,
            "mixed n=8 must NOT be a proven loser"
        );
    }

    /// Template-parity vector (constant loser, n = 12): template upper =
    /// −11.751067 → demotable. Under the OLD per-share R=2 gate this was structurally
    /// impossible (term2 = 13.98/(n−1) > 1 for all n ≤ 14) — the point of the rework.
    #[test]
    fn eb_matches_python_template_constant_loser_n12() {
        let sample = vec![dec!(-12.5); 12];
        let (mean, half_width) = empirical_bernstein_half_width(&sample, ALPHA).unwrap();
        assert_eq!(mean, dec!(-12.5));
        let upper = mean + half_width;
        assert!(
            (upper - dec!(-11.751067)).abs() < dec!(0.0001),
            "upper CB {upper} != template -11.751067"
        );
        assert!(upper < Decimal::ZERO);
    }

    /// A consistent small-sample bleeder (n = 6) now proves out: template upper =
    /// −11.002134. The observed-range floor (constant sample → R̂ = 1) is what makes
    /// small-n zero-variance evidence usable at all.
    #[test]
    fn eb_constant_loser_fires_at_n6() {
        let sample = vec![dec!(-12.5); 6];
        let (mean, half_width) = empirical_bernstein_half_width(&sample, ALPHA).unwrap();
        let upper = mean + half_width;
        assert!(
            (upper - dec!(-11.002134)).abs() < dec!(0.0001),
            "upper CB {upper} != template -11.002134"
        );
        assert!(upper < Decimal::ZERO);
    }

    #[test]
    fn eb_lower_bound_goes_positive_for_a_clear_winner() {
        // Constant winner (+$12.5 × 12): LOWER CB > 0 (proven winner).
        let sample = vec![dec!(12.5); 12];
        let (mean, half_width) = empirical_bernstein_half_width(&sample, ALPHA).unwrap();
        assert_eq!(mean, dec!(12.5));
        let lower = mean - half_width;
        assert!(lower > Decimal::ZERO, "lower CB should be > 0, got {lower}");
    }

    #[test]
    fn eb_small_zero_mean_sample_straddles_zero() {
        // n = 10 at mean 0: the band is wide, so neither bound clears 0 — the
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
            windowed_pnl: dec!(-60),
            lower_cb: Some(dec!(-22)),
            upper_cb: Some(dec!(-3.5)),
        };
        assert!(base.should_demote(10));

        // Windowed P&L ≥ 0 is the safety net — never demote.
        let recently_profitable = WalletEdgeStats {
            windowed_pnl: dec!(5),
            ..base.clone()
        };
        assert!(!recently_profitable.should_demote(10));

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

    /// Reverse direction of the window divergence (intentional): a lifetime LOSER
    /// whose trailing window is flat-or-positive is NOT demoted this tick — the
    /// windowed conjunct is the sole P&L gate, so the wallet remains when that conjunct is false.
    /// (It stays demotable the moment its window turns red again; inactivity
    /// eviction still applies independently.)
    #[test]
    fn lifetime_loser_with_flat_window_is_not_demoted() {
        let dormant_loser = WalletEdgeStats {
            settled_count: 40,
            realized_pnl: dec!(-500), // lifetime deep red
            windowed_pnl: dec!(0),    // nothing settled red in the window
            lower_cb: Some(dec!(-22)),
            upper_cb: Some(dec!(-3.5)),
        };
        assert!(!dormant_loser.should_demote(10));
    }

    /// The behaviour change of the rework: a large HISTORICAL winner that is bleeding
    /// in the trailing window IS demotable — lifetime P&L no longer shields it.
    #[test]
    fn historic_winner_bleeding_in_window_is_demotable() {
        let goose_gone_cold = WalletEdgeStats {
            settled_count: 40,
            realized_pnl: dec!(4110), // lifetime still deep green
            windowed_pnl: dec!(-60),  // trailing window bleeding
            lower_cb: Some(dec!(-22)),
            upper_cb: Some(dec!(-3.5)),
        };
        assert!(goose_gone_cold.should_demote(10));
    }

    #[test]
    fn is_proven_winner_requires_positive_lower_cb_and_enough_trades() {
        let winner = WalletEdgeStats {
            settled_count: 40,
            realized_pnl: dec!(200),
            windowed_pnl: dec!(50),
            lower_cb: Some(dec!(3.5)),
            upper_cb: Some(dec!(22)),
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
        //      BUY YES 50 @0.40 → −20. realized_pnl = −70. Settled in-window.
        // 0xB: one settled winner on 0xn (YES resolved to 1): BUY YES 100 @0.30 → +70.
        let (_d, store) = store_with(&[
            ("0xm", vec![dec!(0), dec!(1)], dec!(0), NOW - 1_000),
            ("0xn", vec![dec!(1), dec!(0)], dec!(100), NOW - 1_000),
        ]);
        let fills = vec![
            fill("0xA", "0xm", 0, Side::Buy, 100, dec!(0.50)),
            fill("0xA", "0xm", 0, Side::Buy, 50, dec!(0.40)),
            fill("0xB", "0xn", 0, Side::Buy, 100, dec!(0.30)),
            // Open market (not in the store) → skipped.
            fill("0xA", "0xopen", 0, Side::Buy, 100, dec!(0.50)),
            // Foreign/legacy key (not "wf|...") → skipped.
            FillRow {
                idempotency_key: "legacy-key".to_owned(),
                ..fill("ignored", "0xm", 0, Side::Buy, 100, dec!(0.50))
            },
            // Zero-quantity fill → skipped (carries no edge).
            fill("0xA", "0xm", 0, Side::Buy, 0, dec!(0.50)),
        ];

        let stats = wallet_edge_stats(&fills, &store, ALPHA, NOW, WINDOW);
        assert_eq!(stats.len(), 2, "only 0xA and 0xB have settled wf fills");

        let a = stats.get("0xA").unwrap();
        assert_eq!(a.settled_count, 2, "open/foreign/zero-quantity excluded");
        assert_eq!(a.realized_pnl, dec!(-70)); // −50 + −20
        assert_eq!(a.windowed_pnl, dec!(-70)); // both settled in-window

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

    /// Windowing: an old big win settled OUTSIDE the trailing window is excluded from
    /// `windowed_pnl` (but kept in lifetime `realized_pnl` and in the CB series), so
    /// the full underperformance knockout can fire on a historic winner now bleeding.
    /// The boundary is inclusive: settled exactly at `now − window` is in-window.
    #[test]
    fn wallet_edge_stats_windows_the_pnl_conjunct() {
        let win = i64::try_from(WINDOW).unwrap();
        let (_d, store) = store_with(&[
            // Old glory: YES resolved 1, settled just OUTSIDE the window.
            ("0xold", vec![dec!(1), dec!(0)], dec!(0), NOW - win - 1),
            // Boundary: settled exactly AT the cutoff — in-window (inclusive).
            ("0xedge", vec![dec!(0), dec!(1)], dec!(0), NOW - win),
            // Recent bleed: YES resolved 0, settled yesterday.
            ("0xnew", vec![dec!(0), dec!(1)], dec!(0), NOW - 86_400),
        ]);
        // 1 old +$350 win (500 sh @0.30 → +0.70/sh), 1 boundary −$12.5 loss, then
        // 12 identical recent −$12.5 losses (25 sh @0.50).
        let mut fills = vec![fill("0xW", "0xold", 0, Side::Buy, 500, dec!(0.30))]; // +350
        fills.push(fill("0xW", "0xedge", 0, Side::Buy, 25, dec!(0.50))); // −12.5 at cutoff
        for _ in 0..12 {
            fills.push(fill("0xW", "0xnew", 0, Side::Buy, 25, dec!(0.50))); // −12.5 each
        }

        let stats = wallet_edge_stats(&fills, &store, ALPHA, NOW, WINDOW);
        let s = stats.get("0xW").unwrap();
        assert_eq!(s.settled_count, 14);
        assert_eq!(s.realized_pnl, dec!(187.5)); // +350 − 13×12.5 — lifetime GREEN
        assert_eq!(s.windowed_pnl, dec!(-162.5)); // 13 in-window losses (incl. boundary)
        // Lifetime conjunct would have shielded it; the windowed conjunct does not.
        assert!(s.realized_pnl > Decimal::ZERO && s.windowed_pnl < Decimal::ZERO);
    }

    /// End-to-end: 12 consistent −$12.5 settled fills → upper CB < 0 (template
    /// −11.75) AND windowed P&L < 0 AND n ≥ min_trades → the knockout fires at a
    /// sample size where the old per-share R=2 gate could never fire (n ≤ 14).
    #[test]
    fn wallet_edge_stats_demotes_a_small_sample_consistent_bleeder() {
        let (_d, store) = store_with(&[("0xm", vec![dec!(0), dec!(1)], dec!(0), NOW - 86_400)]);
        let fills: Vec<FillRow> = (0..12)
            .map(|_| fill("0xLOSER", "0xm", 0, Side::Buy, 25, dec!(0.50)))
            .collect();

        let stats = wallet_edge_stats(&fills, &store, ALPHA, NOW, WINDOW);
        let s = stats.get("0xLOSER").unwrap();
        assert_eq!(s.settled_count, 12);
        assert_eq!(s.realized_pnl, dec!(-150)); // 12 × −12.5
        assert_eq!(s.windowed_pnl, dec!(-150));
        assert!(s.should_demote(10), "n=12 consistent bleeder must demote");
        assert!(!s.is_proven_winner(10));
    }
}
