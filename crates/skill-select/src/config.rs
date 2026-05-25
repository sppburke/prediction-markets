//! Configuration for the skill-selection pipeline (issue #212).
//!
//! Loaded via `figment` from an optional TOML file overlaid with `PE_SKILL_*`
//! environment variables, mirroring `pe-bootstrap`'s loader. Numeric defaults
//! are the canonical values in `docs/_GLOSSARY.md` "Skill-selection defaults".

use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Env, Format, Toml};
use serde::Deserialize;

use crate::composite::CompositeWeights;
use crate::error::SkillSelectError;

/// Train/forward cutoff default: `2026-03-31T23:59:59Z`. Derived from the date
/// (not a hand-typed unix constant) so it cannot drift to the wrong year.
fn default_cutoff_unix() -> i64 {
    time::macros::datetime!(2026-03-31 23:59:59 UTC).unix_timestamp()
}
fn default_cache_path() -> PathBuf {
    PathBuf::from("wallet_cache.db")
}
fn default_min_closed_trades() -> u32 {
    20
}
fn default_min_trading_days() -> u32 {
    20
}
fn default_permutations() -> u32 {
    999
}
fn default_rng_seed() -> u64 {
    42
}
fn default_bhq_q_bps() -> u32 {
    1_000
}
fn default_top_n() -> usize {
    50
}
fn default_kelly_fraction_bps() -> u32 {
    1_000
}
fn default_forward_min_bucket_trades() -> u32 {
    5
}
fn default_forward_price_bucket_width_bps() -> u32 {
    1_000
}
fn default_min_distinct_events() -> u32 {
    // SSRN 6617059 §C — skilled cohort has ≥10 distinct events traded.
    10
}
fn default_beta_binomial_alpha() -> u32 {
    // Laplace prior (α=β=1): mildest non-degenerate shrinkage on edge.
    1
}
fn default_beta_binomial_beta() -> u32 {
    1
}
fn default_extract_threads() -> usize {
    // `0` means "rayon's default" — `rayon::current_num_threads()`, which
    // respects `RAYON_NUM_THREADS` and falls back to the CPU count. Keeps the
    // tuning surface single-knob: callers either accept the platform default
    // or override via `PE_SKILL_EXTRACT_THREADS=N`.
    0
}

// Composite-ranker per-feature weight defaults (basis-points; sign = direction).
// Mirror [`crate::composite::CompositeWeights::default`]; both must agree.
fn default_w_sharpe_bps() -> i32 {
    1_500
}
fn default_w_ev_mean_bps() -> i32 {
    833
}
fn default_w_ev_tstat_bps() -> i32 {
    833
}
fn default_w_bb_shrunk_edge_bps() -> i32 {
    833
}
fn default_w_kelly_log_growth_bps() -> i32 {
    833
}
fn default_w_brier_score_bps() -> i32 {
    -833
}
fn default_w_brier_resolution_bps() -> i32 {
    833
}
fn default_w_concentration_hhi_bps() -> i32 {
    -500
}
fn default_w_concentration_n_eff_bps() -> i32 {
    500
}
fn default_w_concentration_rpc_bps() -> i32 {
    -500
}
fn default_w_first_entries_per_active_day_bps() -> i32 {
    1_000
}
fn default_w_median_first_entry_to_resolution_secs() -> i32 {
    -1_000
}

/// Skill-selection configuration. Every field has a default; `PE_SKILL_*` env
/// vars (and an optional TOML file) override.
#[derive(Debug, Clone, Deserialize)]
pub struct SkillConfig {
    /// Path to the bootstrap `wallet_cache.db`. `PE_SKILL_CACHE_PATH`.
    #[serde(default = "default_cache_path")]
    pub cache_path: PathBuf,
    /// Train/forward split (unix seconds, UTC). `PE_SKILL_CUTOFF_UNIX`.
    #[serde(default = "default_cutoff_unix")]
    pub cutoff_unix: i64,
    /// Minimum train-window closed trades for a wallet to be scored. `PE_SKILL_MIN_CLOSED_TRADES`.
    #[serde(default = "default_min_closed_trades")]
    pub min_closed_trades: u32,
    /// Minimum distinct trading days (daily-return observations) for a wallet to
    /// be eligible for the deflated-Sharpe ranking. Below this the Sharpe is
    /// degenerate; the wallet stays BHq-significant but is not selected.
    /// `PE_SKILL_MIN_TRADING_DAYS`.
    #[serde(default = "default_min_trading_days")]
    pub min_trading_days: u32,
    /// Sign-randomization permutation count. `PE_SKILL_PERMUTATIONS`.
    #[serde(default = "default_permutations")]
    pub permutations: u32,
    /// Fixed RNG seed for the permutation test. `PE_SKILL_RNG_SEED`.
    #[serde(default = "default_rng_seed")]
    pub rng_seed: u64,
    /// Benjamini–Hochberg FDR `q` in basis points (1000 = 0.10). `PE_SKILL_BHQ_Q_BPS`.
    #[serde(default = "default_bhq_q_bps")]
    pub bhq_q_bps: u32,
    /// Watchlist cap. `PE_SKILL_TOP_N`.
    #[serde(default = "default_top_n")]
    pub top_n: usize,
    /// Kelly fraction `f` in basis points (1000 = 0.10). `PE_SKILL_KELLY_FRACTION_BPS`.
    #[serde(default = "default_kelly_fraction_bps")]
    pub kelly_fraction_bps: u32,
    /// Min ≤cutoff resolved buys in an entry-price bucket for a reliable calibration
    /// `p` (else flat-$1 fallback). `PE_SKILL_FORWARD_MIN_BUCKET_TRADES`.
    #[serde(default = "default_forward_min_bucket_trades")]
    pub forward_min_bucket_trades: u32,
    /// Entry-price bucket width in basis points (1000 = 0.10). `PE_SKILL_FORWARD_PRICE_BUCKET_WIDTH_BPS`.
    #[serde(default = "default_forward_price_bucket_width_bps")]
    pub forward_price_bucket_width_bps: u32,
    /// Minimum distinct events a wallet must have traded for extraction (SSRN
    /// 6617059 §C threshold). Below this `extract_features` returns `None` and
    /// the wallet is skipped. `PE_SKILL_MIN_DISTINCT_EVENTS`.
    #[serde(default = "default_min_distinct_events")]
    pub min_distinct_events: u32,
    /// Beta-binomial conjugate-prior `α` for shrunk-edge feature. Default 1
    /// (Laplace). `PE_SKILL_BETA_BINOMIAL_ALPHA`.
    #[serde(default = "default_beta_binomial_alpha")]
    pub beta_binomial_alpha: u32,
    /// Beta-binomial conjugate-prior `β` for shrunk-edge feature. Default 1
    /// (Laplace). `PE_SKILL_BETA_BINOMIAL_BETA`.
    #[serde(default = "default_beta_binomial_beta")]
    pub beta_binomial_beta: u32,
    /// Rayon worker count for the per-wallet extraction loop. `0` (default)
    /// means rayon's auto-tuning (`current_num_threads()` — honours
    /// `RAYON_NUM_THREADS`, else CPU count). `PE_SKILL_EXTRACT_THREADS`.
    #[serde(default = "default_extract_threads")]
    pub extract_threads: usize,
    // ── Composite ranker per-feature weights (bps; signed) ─────────────────
    // Flattened onto `SkillConfig` so env-override is a clean
    // `PE_SKILL_COMPOSITE_W_<NAME>` form. Mirrors
    // [`crate::composite::CompositeWeights`] field-for-field; the conversion
    // helper [`Self::composite_weights`] keeps them in sync at call time.
    #[serde(default = "default_w_sharpe_bps")]
    pub composite_w_sharpe_bps: i32,
    #[serde(default = "default_w_ev_mean_bps")]
    pub composite_w_ev_mean_bps: i32,
    #[serde(default = "default_w_ev_tstat_bps")]
    pub composite_w_ev_tstat_bps: i32,
    #[serde(default = "default_w_bb_shrunk_edge_bps")]
    pub composite_w_bb_shrunk_edge_bps: i32,
    #[serde(default = "default_w_kelly_log_growth_bps")]
    pub composite_w_kelly_log_growth_bps: i32,
    #[serde(default = "default_w_brier_score_bps")]
    pub composite_w_brier_score_bps: i32,
    #[serde(default = "default_w_brier_resolution_bps")]
    pub composite_w_brier_resolution_bps: i32,
    #[serde(default = "default_w_concentration_hhi_bps")]
    pub composite_w_concentration_hhi_bps: i32,
    #[serde(default = "default_w_concentration_n_eff_bps")]
    pub composite_w_concentration_n_eff_bps: i32,
    #[serde(default = "default_w_concentration_rpc_bps")]
    pub composite_w_concentration_rpc_bps: i32,
    #[serde(default = "default_w_first_entries_per_active_day_bps")]
    pub composite_w_first_entries_per_active_day_bps: i32,
    #[serde(default = "default_w_median_first_entry_to_resolution_secs")]
    pub composite_w_median_first_entry_to_resolution_secs: i32,
}

impl SkillConfig {
    /// Build a [`CompositeWeights`] from the flattened composite-weight
    /// fields. Mirrors the post-flatten env shape onto the ranker's
    /// struct-shape boundary so callers stay terse.
    pub fn composite_weights(&self) -> CompositeWeights {
        CompositeWeights {
            w_sharpe_bps: self.composite_w_sharpe_bps,
            w_ev_mean_bps: self.composite_w_ev_mean_bps,
            w_ev_tstat_bps: self.composite_w_ev_tstat_bps,
            w_bb_shrunk_edge_bps: self.composite_w_bb_shrunk_edge_bps,
            w_kelly_log_growth_bps: self.composite_w_kelly_log_growth_bps,
            w_brier_score_bps: self.composite_w_brier_score_bps,
            w_brier_resolution_bps: self.composite_w_brier_resolution_bps,
            w_concentration_hhi_bps: self.composite_w_concentration_hhi_bps,
            w_concentration_n_eff_bps: self.composite_w_concentration_n_eff_bps,
            w_concentration_rpc_bps: self.composite_w_concentration_rpc_bps,
            w_first_entries_per_active_day_bps: self.composite_w_first_entries_per_active_day_bps,
            w_median_first_entry_to_resolution_secs: self
                .composite_w_median_first_entry_to_resolution_secs,
        }
    }
}

impl SkillConfig {
    /// Load config from an optional TOML file overlaid with `PE_SKILL_*` env.
    pub fn load(path: Option<&std::path::Path>) -> Result<SkillConfig, SkillSelectError> {
        let mut fig = Figment::new();
        if let Some(p) = path {
            fig = fig.merge(Toml::file(p));
        }
        let cfg = fig
            .merge(Env::prefixed("PE_SKILL_").lowercase(true))
            .extract()?;
        Ok(cfg)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_cutoff_is_in_2026() {
        // Guard against the off-by-one-year unix mis-encoding (#212 review note):
        // 2026-03-31 is unix 1_775_001_599 (NOT the 2025 value 1_743_465_599).
        assert_eq!(default_cutoff_unix(), 1_775_001_599);
    }

    #[test]
    // `Jail::expect_with`'s closure must return `Result<(), figment::Error>`;
    // the large Err is the API's, not ours.
    #[allow(clippy::result_large_err)]
    fn defaults_load_with_no_toml_no_env() {
        // Jail isolates env so stray PE_SKILL_* can't pollute; assert inside it
        // (Jail::expect_with returns `()`, so the checks live in the closure).
        figment::Jail::expect_with(|_| {
            let cfg = SkillConfig::load(None).expect("defaults must load");
            assert_eq!(cfg.permutations, 999);
            assert_eq!(cfg.bhq_q_bps, 1_000);
            assert_eq!(cfg.top_n, 50);
            assert_eq!(cfg.min_closed_trades, 20);
            assert_eq!(cfg.min_trading_days, 20);
            assert_eq!(cfg.rng_seed, 42);
            assert_eq!(cfg.cutoff_unix, 1_775_001_599);
            assert_eq!(cfg.min_distinct_events, 10);
            assert_eq!(cfg.beta_binomial_alpha, 1);
            assert_eq!(cfg.beta_binomial_beta, 1);
            assert_eq!(cfg.extract_threads, 0);
            // Composite weights — both flattened defaults and the assembled
            // `CompositeWeights::default()` must agree (the two are mirrored).
            assert_eq!(cfg.composite_w_sharpe_bps, 1_500);
            assert_eq!(cfg.composite_w_ev_mean_bps, 833);
            assert_eq!(cfg.composite_w_brier_score_bps, -833);
            assert_eq!(cfg.composite_w_concentration_hhi_bps, -500);
            assert_eq!(
                cfg.composite_w_median_first_entry_to_resolution_secs,
                -1_000
            );
            assert_eq!(cfg.composite_weights(), CompositeWeights::default());
            Ok(())
        });
    }
}
