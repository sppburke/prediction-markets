//! `BacktestConfig` sourced from environment variables.

use std::path::PathBuf;

use rust_decimal::Decimal;

use crate::error::BacktestError;

// Canonical defaults added to `docs/_GLOSSARY.md` "Backtest defaults" section.
const DEFAULT_STEP_DAYS: u32 = 1;
const DEFAULT_AUDIT_WINDOW_DAYS: u32 = 90;

/// Backtest configuration sourced from environment variables.
pub struct BacktestConfig {
    /// `PE_BOOTSTRAP_CACHE_PATH` — path to bootstrap wallet trade cache JSON.
    pub cache_path: PathBuf,
    /// `PE_BACKTEST_OUTPUT_DIR` — directory where report.json and trades.ndjson are written.
    pub output_dir: PathBuf,
    /// `PE_BANKROLL_USD` — starting bankroll in USD (default: 10_000).
    pub bankroll_usd: Decimal,
    /// `PE_BACKTEST_STEP_DAYS` — walk-forward step in days (default: 1).
    pub step_days: u32,
    /// `PE_ETHERSCAN_API_KEY` — required for Phase 0 funder discovery.
    pub etherscan_api_key: Option<String>,
    /// Trade lookback window for ledger reconstruction (default: 90 days).
    pub audit_window_days: u32,
}

impl BacktestConfig {
    pub fn from_env() -> Result<Self, BacktestError> {
        fn require(key: &str) -> Result<String, BacktestError> {
            std::env::var(key).map_err(|_| BacktestError::MissingEnv(key.to_owned()))
        }
        fn optional(key: &str, default: &str) -> String {
            std::env::var(key).unwrap_or_else(|_| default.to_owned())
        }
        fn optional_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }

        Ok(Self {
            cache_path: PathBuf::from(optional("PE_BOOTSTRAP_CACHE_PATH", "wallet_cache.json")),
            output_dir: PathBuf::from(require("PE_BACKTEST_OUTPUT_DIR")?),
            bankroll_usd: optional_parse("PE_BANKROLL_USD", Decimal::from(10_000u32)),
            step_days: optional_parse("PE_BACKTEST_STEP_DAYS", DEFAULT_STEP_DAYS),
            etherscan_api_key: std::env::var("PE_ETHERSCAN_API_KEY").ok(),
            audit_window_days: optional_parse(
                "PE_BACKTEST_AUDIT_WINDOW_DAYS",
                DEFAULT_AUDIT_WINDOW_DAYS,
            ),
        })
    }
}
