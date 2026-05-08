//! Scenario tests for `BacktestConfig` TOML loading and env-var overlay (issue #113).
//!
//! Each test exercises a distinct semantic of the figment-based config loader:
//!
//! 1. `defaults_apply_without_toml` — `load(None)` with only env-var required fields
//!    applies struct defaults for every optional field.
//! 2. `toml_file_overrides_defaults` — a TOML file with explicit values overrides every
//!    default field it touches.
//! 3. `env_var_overrides_toml` — `PE_BANKROLL_USD` set in the environment beats the TOML value.
//! 4. `pe_backtest_prefix_beats_pe_prefix` — `PE_BACKTEST_STEP_DAYS` takes priority over
//!    `PE_STEP_DAYS` for the same field.
//! 5. `kelly_sweep_csv_env` — `PE_BACKTEST_KELLY_SWEEP=0.25,0.50` parses into two fractions.
//! 6. `kelly_sweep_toml_array` — TOML `kelly_sweep_fractions = [0.10, 0.50]` is read as an array.
//! 7. `strategy_sub_table_loaded` — `[strategy]` TOML sub-table sets `slippage_rate`.
//! 8. `output_dir_alias` — `PE_BACKTEST_OUTPUT_DIR` maps to `output_dir` via alias.
//! 9. `missing_output_dir_fails` — `load(None)` without `output_dir` in any source returns an error.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
// env::set_var / remove_var are unsafe in Rust 1.80+; nextest provides process isolation.
#![allow(unsafe_code)]

use std::io::Write as _;

use pe_backtest::config::load;
use pe_strategy_winner_follow::WinnerFollowConfig;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::NamedTempFile;

/// Isolated environment for a single test — captures/restores env vars and directory.
struct EnvGuard {
    to_restore: Vec<(String, Option<String>)>,
}

impl EnvGuard {
    fn new() -> Self {
        Self {
            to_restore: Vec::new(),
        }
    }

    fn set(&mut self, key: &str, value: &str) {
        let old = std::env::var(key).ok();
        self.to_restore.push((key.to_owned(), old));
        // SAFETY: tests run in nextest's process-per-test isolation.
        unsafe { std::env::set_var(key, value) };
    }

    fn remove(&mut self, key: &str) {
        let old = std::env::var(key).ok();
        self.to_restore.push((key.to_owned(), old));
        // SAFETY: tests run in nextest's process-per-test isolation.
        unsafe { std::env::remove_var(key) };
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, val) in &self.to_restore {
            match val {
                // SAFETY: tests run in nextest's process-per-test isolation.
                Some(v) => unsafe { std::env::set_var(key, v) },
                None => unsafe { std::env::remove_var(key) },
            }
        }
    }
}

// ── Scenario 1 ───────────────────────────────────────────────────────────────

/// PASS: `load(None)` with only the required `output_dir` env var set produces a config
///       where all optional fields are at their documented defaults.
/// FAIL: any optional field deviates from its default value.
#[test]
fn defaults_apply_without_toml() {
    let mut env = EnvGuard::new();
    env.remove("PE_BANKROLL_USD");
    env.remove("PE_BOOTSTRAP_CACHE_PATH");
    env.remove("PE_BACKTEST_STEP_DAYS");
    env.remove("PE_BACKTEST_KELLY_SWEEP");
    env.set("PE_BACKTEST_OUTPUT_DIR", "/tmp/pe-test-output");

    let cfg = load(None).expect("load(None) must succeed with output_dir set");

    assert_eq!(cfg.bankroll_usd, Decimal::from(10_000u32));
    assert_eq!(cfg.step_days, 1);
    assert_eq!(cfg.audit_window_days, 90);
    assert_eq!(cfg.ranker_min_quality, 0);
    assert_eq!(cfg.ranker_active_min_closed, 10);
    assert_eq!(cfg.ranker_active_min_markets, 1);
    assert_eq!(cfg.ranker_incubator_min_closed, 3);
    assert_eq!(cfg.ranker_incubator_min_markets, 1);
    assert_eq!(cfg.kelly_p_prior_alpha, 10);
    assert_eq!(cfg.kelly_p_prior_beta, 10);
    assert_eq!(cfg.kelly_p_k_per_market, 6);
    assert!(cfg.kelly_sweep_fractions.is_none());
    assert!(cfg.max_hours_to_expiry.is_none());
}

// ── Scenario 2 ───────────────────────────────────────────────────────────────

/// PASS: fields written explicitly in the TOML file override struct defaults.
/// FAIL: any explicitly-set TOML field retains the default value.
#[test]
fn toml_file_overrides_defaults() {
    let mut f = NamedTempFile::new().unwrap();
    write!(
        f,
        r#"
output_dir = "/tmp/pe-backtest-toml"
bankroll_usd = "25000"
step_days = 7
audit_window_days = 180
kelly_p_prior_alpha = 5
kelly_p_prior_beta = 5
kelly_p_k_per_market = 3
"#
    )
    .unwrap();

    let mut env = EnvGuard::new();
    env.remove("PE_BACKTEST_OUTPUT_DIR");
    env.remove("PE_BANKROLL_USD");
    env.remove("PE_BACKTEST_STEP_DAYS");

    let cfg = load(Some(f.path())).expect("TOML load must succeed");

    assert_eq!(cfg.bankroll_usd, Decimal::from(25_000u32));
    assert_eq!(cfg.step_days, 7);
    assert_eq!(cfg.audit_window_days, 180);
    assert_eq!(cfg.kelly_p_prior_alpha, 5);
    assert_eq!(cfg.kelly_p_prior_beta, 5);
    assert_eq!(cfg.kelly_p_k_per_market, 3);
}

// ── Scenario 3 ───────────────────────────────────────────────────────────────

/// PASS: `PE_BANKROLL_USD` set in the environment beats the TOML `bankroll_usd = "5000"`.
/// FAIL: the TOML value is returned instead of the env var.
#[test]
fn env_var_overrides_toml() {
    let mut f = NamedTempFile::new().unwrap();
    write!(
        f,
        r#"
output_dir = "/tmp/pe-backtest-env-override"
bankroll_usd = "5000"
"#
    )
    .unwrap();

    let mut env = EnvGuard::new();
    env.set("PE_BANKROLL_USD", "99000");

    let cfg = load(Some(f.path())).expect("load must succeed");

    assert_eq!(
        cfg.bankroll_usd,
        Decimal::from(99_000u32),
        "env var must win over TOML"
    );
}

// ── Scenario 4 ───────────────────────────────────────────────────────────────

/// PASS: `PE_BACKTEST_STEP_DAYS=3` takes priority over `PE_STEP_DAYS=99` for the same field.
/// FAIL: the lower-priority `PE_STEP_DAYS` value is used.
#[test]
fn pe_backtest_prefix_beats_pe_prefix() {
    let mut env = EnvGuard::new();
    env.set("PE_BACKTEST_OUTPUT_DIR", "/tmp/pe-prefix-test");
    env.set("PE_STEP_DAYS", "99");
    env.set("PE_BACKTEST_STEP_DAYS", "3");

    let cfg = load(None).expect("load must succeed");

    assert_eq!(
        cfg.step_days, 3,
        "PE_BACKTEST_STEP_DAYS must beat PE_STEP_DAYS"
    );
}

// ── Scenario 5 ───────────────────────────────────────────────────────────────

/// PASS: `PE_BACKTEST_KELLY_SWEEP=0.25,0.50` is parsed into two `KellyFraction` values.
/// FAIL: the env var is ignored or only one fraction is parsed.
#[test]
fn kelly_sweep_csv_env() {
    let mut env = EnvGuard::new();
    env.set("PE_BACKTEST_OUTPUT_DIR", "/tmp/pe-sweep-csv");
    env.set("PE_BACKTEST_KELLY_SWEEP", "0.25,0.50");

    let cfg = load(None).expect("load must succeed");

    let fracs = cfg
        .kelly_sweep_fractions
        .expect("kelly_sweep_fractions must be Some");
    assert_eq!(fracs.len(), 2);
    assert_eq!(fracs[0].0, dec!(0.25));
    assert_eq!(fracs[1].0, dec!(0.50));
}

// ── Scenario 6 ───────────────────────────────────────────────────────────────

/// PASS: TOML `kelly_sweep_fractions = [0.10, 0.50]` is deserialized as a two-element array.
/// FAIL: the TOML array is not parsed or produces wrong values.
#[test]
fn kelly_sweep_toml_array() {
    let mut f = NamedTempFile::new().unwrap();
    write!(
        f,
        r#"
output_dir = "/tmp/pe-sweep-toml"
kelly_sweep_fractions = [0.10, 0.50]
"#
    )
    .unwrap();

    let mut env = EnvGuard::new();
    env.remove("PE_BACKTEST_KELLY_SWEEP");
    env.remove("PE_BACKTEST_OUTPUT_DIR");

    let cfg = load(Some(f.path())).expect("load must succeed");

    let fracs = cfg
        .kelly_sweep_fractions
        .expect("kelly_sweep_fractions must be Some");
    assert_eq!(fracs.len(), 2);
    assert_eq!(fracs[0].0, dec!(0.10));
    assert_eq!(fracs[1].0, dec!(0.50));
}

// ── Scenario 7 ───────────────────────────────────────────────────────────────

/// PASS: `[strategy]` TOML sub-table sets `strategy.slippage_rate`; all other strategy
///       fields retain `WinnerFollowConfig::default()` values.
/// FAIL: the strategy sub-table is ignored or the slippage rate is wrong.
#[test]
fn strategy_sub_table_loaded() {
    let mut f = NamedTempFile::new().unwrap();
    write!(
        f,
        r#"
output_dir = "/tmp/pe-strategy-toml"

[strategy]
slippage_rate = "0.02"
"#
    )
    .unwrap();

    let mut env = EnvGuard::new();
    env.remove("PE_BACKTEST_OUTPUT_DIR");

    let cfg = load(Some(f.path())).expect("load must succeed");

    assert_eq!(cfg.strategy.slippage_rate, dec!(0.02));
    let defaults = WinnerFollowConfig::default();
    assert_eq!(cfg.strategy.per_trade_cap, defaults.per_trade_cap);
}

// ── Scenario 8 ───────────────────────────────────────────────────────────────

/// PASS: `PE_BACKTEST_OUTPUT_DIR` env var (stripped by `PE_BACKTEST_` prefix) maps to the
///       `output_dir` field, and the path is correctly set.
/// FAIL: the alias is not recognised and loading fails or produces a wrong path.
#[test]
fn output_dir_alias() {
    let mut env = EnvGuard::new();
    env.remove("PE_BACKTEST_OUTPUT_DIR");
    env.set("PE_BACKTEST_OUTPUT_DIR", "/tmp/alias-output");

    let cfg = load(None).expect("load must succeed via alias");
    assert_eq!(cfg.output_dir.to_str().unwrap(), "/tmp/alias-output");
}

// ── Scenario 9 ───────────────────────────────────────────────────────────────

/// PASS: `load(None)` without `output_dir` in any source returns an error — the field
///       is required and has no default.
/// FAIL: loading succeeds (which would silently use a wrong path).
#[test]
fn missing_output_dir_fails() {
    let mut env = EnvGuard::new();
    env.remove("PE_BACKTEST_OUTPUT_DIR");
    env.remove("PE_OUTPUT_DIR");

    let result = load(None);
    assert!(result.is_err(), "load without output_dir must fail");
}
