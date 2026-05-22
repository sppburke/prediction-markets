//! Selection-bias-controlled wallet selection (issue #212; #205).
//!
//! Selecting the best-of-~182k wallets is a massive multiple-testing problem.
//! Two controls are layered:
//!
//! 1. **Primary gate — Benjamini–Hochberg FDR** over the sign-randomization
//!    permutation p-values (`skill_test`): at false-discovery-rate `q`, the
//!    significant set is the wallets at sorted ranks `1..=k`, where `k` is the
//!    largest rank with `p_(k) ≤ (k/m)·q`. This is the rigorous, well-defined
//!    multiple-testing control.
//! 2. **Secondary rank — Deflated-Sharpe haircut** (Bailey & López de Prado,
//!    leading order): each wallet's Sharpe is reduced by the expected maximum
//!    Sharpe under `m` trials, approximated by `√(2·ln m)` (the leading-order
//!    expected max of `m` standard normals). The BHq-significant set is then
//!    ranked by this deflated Sharpe and capped at `top_n`.
//!
//! The haircut is a documented **heuristic** multiple-testing penalty in Sharpe
//! units, not the fully-calibrated DSR (which needs the trial-Sharpe variance +
//! the inverse-normal CDF + skew/kurtosis terms); the rigorous control is the
//! BHq p-value gate. All arithmetic is `rust_decimal` (no `f64`).

use std::collections::HashSet;

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, MathematicalOps};

/// Per-wallet inputs to selection: the skill-test p-value and the per-period
/// Sharpe, both already in basis points (from `skill_test` / `features`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionInput {
    /// `0x`-prefixed wallet address.
    pub wallet_hex: String,
    /// Sign-randomization permutation p-value × 10_000 (0..=10_000).
    pub skill_pvalue_bps: u32,
    /// Per-period Sharpe × 10_000 (from the daily-return moments).
    pub sharpe_bps: i64,
}

/// Per-wallet selection outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectionResult {
    /// `0x`-prefixed wallet address.
    pub wallet_hex: String,
    /// The input p-value × 10_000, carried through.
    pub skill_pvalue_bps: u32,
    /// Sharpe after the multiple-testing haircut (`sharpe_bps − √(2 ln m)`), bps.
    pub deflated_sharpe_bps: i64,
    /// Passed the Benjamini–Hochberg FDR gate at `q`.
    pub bhq_significant: bool,
    /// In the final selection (BHq-significant ∩ top-`n` by deflated Sharpe).
    pub selected: bool,
}

/// Select wallets: BHq(`bhq_q_bps`) gate over p-values, then the significant set
/// ranked by deflated Sharpe and capped at `top_n`.
///
/// Returns one [`SelectionResult`] per input, ordered `selected` first, then by
/// deflated Sharpe descending, then `wallet_hex` (deterministic). `bhq_q_bps` is
/// the FDR `q` in basis points (e.g. `1000` = 0.10).
pub fn select_wallets(
    inputs: &[SelectionInput],
    bhq_q_bps: u32,
    top_n: usize,
) -> Vec<SelectionResult> {
    let m = inputs.len();
    if m == 0 {
        return Vec::new();
    }

    // Multiple-testing Sharpe haircut: expected max of m trial Sharpes ≈ √(2 ln m).
    // m < 2 ⇒ no deflation (ln is undefined/zero).
    let e_max_bps: i64 = if m < 2 {
        0
    } else {
        let m_dec = Decimal::from(u64::try_from(m).unwrap_or(u64::MAX));
        let e_max = (Decimal::from(2u32) * m_dec.ln())
            .sqrt()
            .unwrap_or(Decimal::ZERO);
        decimal_to_bps_i64(e_max)
    };
    let deflated: Vec<i64> = inputs
        .iter()
        .map(|x| x.sharpe_bps.saturating_sub(e_max_bps))
        .collect();

    // BHq: rank by p-value ascending (ties broken by wallet for determinism),
    // find the largest k with p_(k) ≤ (k/m)·q.
    let mut order: Vec<usize> = (0..m).collect();
    order.sort_by(|&a, &b| {
        inputs[a]
            .skill_pvalue_bps
            .cmp(&inputs[b].skill_pvalue_bps)
            .then_with(|| inputs[a].wallet_hex.cmp(&inputs[b].wallet_hex))
    });
    let m_dec = Decimal::from(u64::try_from(m).unwrap_or(u64::MAX));
    let q_dec = Decimal::from(bhq_q_bps);
    let mut max_k = 0usize;
    for (rank0, &i) in order.iter().enumerate() {
        let k = rank0 + 1;
        let critical = Decimal::from(u64::try_from(k).unwrap_or(u64::MAX)) * q_dec / m_dec;
        if Decimal::from(inputs[i].skill_pvalue_bps) <= critical {
            max_k = k;
        }
    }
    let significant: HashSet<usize> = order[..max_k].iter().copied().collect();

    // Rank the significant set by deflated Sharpe desc (tie: p asc, wallet asc),
    // cap at top_n.
    let mut sig_ranked: Vec<usize> = order[..max_k].to_vec();
    sig_ranked.sort_by(|&a, &b| {
        deflated[b]
            .cmp(&deflated[a])
            .then_with(|| inputs[a].skill_pvalue_bps.cmp(&inputs[b].skill_pvalue_bps))
            .then_with(|| inputs[a].wallet_hex.cmp(&inputs[b].wallet_hex))
    });
    let selected: HashSet<usize> = sig_ranked.into_iter().take(top_n).collect();

    let mut out: Vec<SelectionResult> = (0..m)
        .map(|i| SelectionResult {
            wallet_hex: inputs[i].wallet_hex.clone(),
            skill_pvalue_bps: inputs[i].skill_pvalue_bps,
            deflated_sharpe_bps: deflated[i],
            bhq_significant: significant.contains(&i),
            selected: selected.contains(&i),
        })
        .collect();
    out.sort_by(|a, b| {
        b.selected
            .cmp(&a.selected)
            .then_with(|| b.deflated_sharpe_bps.cmp(&a.deflated_sharpe_bps))
            .then_with(|| a.wallet_hex.cmp(&b.wallet_hex))
    });
    out
}

/// Convert a Sharpe-scale `Decimal` to basis points (`×10_000`, rounded
/// half-even), saturating to `i64`. No `f64`.
fn decimal_to_bps_i64(v: Decimal) -> i64 {
    let bps = (v * Decimal::from(10_000i64))
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

    fn input(hex: &str, pvalue_bps: u32, sharpe_bps: i64) -> SelectionInput {
        SelectionInput {
            wallet_hex: hex.to_owned(),
            skill_pvalue_bps: pvalue_bps,
            sharpe_bps,
        }
    }

    fn result_for<'a>(out: &'a [SelectionResult], hex: &str) -> &'a SelectionResult {
        out.iter().find(|r| r.wallet_hex == hex).unwrap()
    }

    #[test]
    fn bhq_selects_the_significant_prefix() {
        // m=4, q=0.10 (1000 bps). Critical at rank k = k*1000/4 = 250/500/750/1000.
        // p = 100,200,9000,9500 → ranks 1,2 pass (100≤250, 200≤500); 3,4 fail.
        let inputs = vec![
            input("0xa", 100, 30_000),
            input("0xb", 200, 20_000),
            input("0xc", 9_000, 90_000),
            input("0xd", 9_500, 10_000),
        ];
        let out = select_wallets(&inputs, 1_000, 10);
        assert!(result_for(&out, "0xa").bhq_significant);
        assert!(result_for(&out, "0xb").bhq_significant);
        assert!(!result_for(&out, "0xc").bhq_significant);
        assert!(!result_for(&out, "0xd").bhq_significant);
        // both significant + top_n=10 ⇒ both selected
        assert!(result_for(&out, "0xa").selected);
        assert!(result_for(&out, "0xb").selected);
    }

    #[test]
    fn top_n_caps_significant_set_by_deflated_sharpe() {
        // All p tiny ⇒ all BHq-significant; top_n=1 ⇒ only the highest deflated Sharpe.
        let inputs = vec![
            input("0xa", 10, 30_000),
            input("0xb", 10, 90_000), // highest Sharpe
            input("0xc", 10, 50_000),
        ];
        let out = select_wallets(&inputs, 1_000, 1);
        assert!(out.iter().all(|r| r.bhq_significant));
        let selected: Vec<&str> = out
            .iter()
            .filter(|r| r.selected)
            .map(|r| r.wallet_hex.as_str())
            .collect();
        assert_eq!(selected, vec!["0xb"], "only the top deflated-Sharpe wallet");
    }

    #[test]
    fn deflated_sharpe_applies_multiple_testing_haircut() {
        // m=2 ⇒ e_max = √(2·ln 2) = √1.386 ≈ 1.1774 → 11774 bps.
        let inputs = vec![input("0xa", 10, 50_000), input("0xb", 10, 50_000)];
        let out = select_wallets(&inputs, 1_000, 10);
        let r = result_for(&out, "0xa");
        // 50000 - 11774 = 38226 (±3 for sqrt/ln rounding)
        assert!(
            (r.deflated_sharpe_bps - 38_226).abs() <= 3,
            "deflated={}",
            r.deflated_sharpe_bps
        );
    }

    #[test]
    fn single_wallet_no_deflation_and_bhq_uses_q() {
        // m=1 ⇒ no haircut (deflated == sharpe); critical at rank 1 = q.
        let pass = select_wallets(&[input("0xa", 900, 12_345)], 1_000, 10);
        assert_eq!(pass[0].deflated_sharpe_bps, 12_345);
        assert!(pass[0].bhq_significant); // 900 ≤ 1000
        assert!(pass[0].selected);
        let fail = select_wallets(&[input("0xa", 1_001, 12_345)], 1_000, 10);
        assert!(!fail[0].bhq_significant); // 1001 > 1000
        assert!(!fail[0].selected);
    }

    #[test]
    fn empty_input_yields_empty() {
        assert!(select_wallets(&[], 1_000, 10).is_empty());
    }
}
