//! `BacktestConfig` sourced from environment variables.

use std::path::PathBuf;

use pe_core_types::KellyFraction;
use rust_decimal::Decimal;

use crate::error::BacktestError;

// Canonical defaults added to `docs/_GLOSSARY.md` "Backtest defaults" section.
const DEFAULT_STEP_DAYS: u32 = 1;
const DEFAULT_AUDIT_WINDOW_DAYS: u32 = 90;

// Backtest-specific ranker defaults (lower than live-system RankerConfig defaults).
// Polymarket's trade API captures CLOB buys/sells only — market resolution redemptions
// do not appear as sell trades, so most positions look "open" and reconstruction quality
// is artificially low. The leaderboard snapshot already encodes quality; these thresholds
// reflect what the data can actually support. See `docs/_GLOSSARY.md` "Backtest defaults".
const DEFAULT_BT_MIN_QUALITY: u8 = 0;
const DEFAULT_BT_ACTIVE_MIN_CLOSED: u32 = 10;
const DEFAULT_BT_ACTIVE_MIN_MARKETS: u32 = 5;
const DEFAULT_BT_INCUBATOR_MIN_CLOSED: u32 = 3;
const DEFAULT_BT_INCUBATOR_MIN_MARKETS: u32 = 2;

/// Backtest configuration sourced from environment variables.
pub struct BacktestConfig {
    /// `PE_BOOTSTRAP_CACHE_PATH` — path to bootstrap wallet trade SQLite cache.
    pub cache_path: PathBuf,
    /// `PE_BACKTEST_OUTPUT_DIR` — directory where report.json and trades.ndjson are written.
    pub output_dir: PathBuf,
    /// `PE_BANKROLL_USD` — starting bankroll in USD (default: 10_000).
    pub bankroll_usd: Decimal,
    /// `PE_BACKTEST_STEP_DAYS` — walk-forward step in days (default: 1).
    pub step_days: u32,
    /// `PE_DUNE_API_KEY` — when set, fetches on-chain market resolutions from
    /// `ctf_evt_conditionresolution` before running the simulation.
    pub dune_api_key: Option<String>,
    /// `PE_DUNE_NAMESPACE` — Dune username (e.g. `apexurellc`). When set, the resolution
    /// fetch uploads market IDs as a lookup table and uses a server-side JOIN so only
    /// the caller's markets are returned, greatly reducing credit cost.
    pub dune_namespace: Option<String>,
    /// `PE_BACKTEST_MAX_HOURS_TO_EXPIRY` — only copy trades where the market resolves
    /// within this many hours of the trade date. `None` = no filter (default).
    pub max_hours_to_expiry: Option<u32>,
    /// Trade lookback window for ledger reconstruction (default: 90 days).
    pub audit_window_days: u32,
    // ── Ranker eligibility overrides (backtest-specific defaults) ──────────────
    /// `PE_BACKTEST_MIN_QUALITY` — min reconstruction quality for watchlist eligibility.
    /// Default 0: leaderboard snapshot already encodes quality; CLOB-only data produces
    /// artificially low quality scores because market resolutions are not captured as sells.
    pub ranker_min_quality: u8,
    /// `PE_BACKTEST_ACTIVE_MIN_CLOSED` — min closed trades in 180-day window for active tier.
    pub ranker_active_min_closed: u32,
    /// `PE_BACKTEST_ACTIVE_MIN_MARKETS` — min distinct markets in 180-day window for active tier.
    pub ranker_active_min_markets: u32,
    /// `PE_BACKTEST_INCUBATOR_MIN_CLOSED` — min closed trades in 90-day window for incubator tier.
    pub ranker_incubator_min_closed: u32,
    /// `PE_BACKTEST_INCUBATOR_MIN_MARKETS` — min distinct markets in 90-day window for incubator tier.
    pub ranker_incubator_min_markets: u32,
    /// `PE_BACKTEST_KELLY_SWEEP` — comma-separated list of Kelly fractions to sweep.
    /// When `Some`, triggers sweep mode: N sequential backtests, one per fraction.
    /// Each fraction must be in `(0.0, 1.0]` (strictly positive, at most full Kelly).
    /// Default fractions when the env var is present but empty: see
    /// `docs/_GLOSSARY.md` `backtest_kelly_sweep_fractions_default`.
    pub kelly_sweep_fractions: Option<Vec<KellyFraction>>,
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

        let kelly_sweep_fractions = parse_kelly_sweep()?;

        Ok(Self {
            cache_path: PathBuf::from(optional("PE_BOOTSTRAP_CACHE_PATH", "wallet_cache.db")),
            output_dir: PathBuf::from(require("PE_BACKTEST_OUTPUT_DIR")?),
            bankroll_usd: optional_parse("PE_BANKROLL_USD", Decimal::from(10_000u32)),
            step_days: optional_parse("PE_BACKTEST_STEP_DAYS", DEFAULT_STEP_DAYS),
            dune_api_key: std::env::var("PE_DUNE_API_KEY").ok(),
            dune_namespace: std::env::var("PE_DUNE_NAMESPACE").ok(),
            max_hours_to_expiry: std::env::var("PE_BACKTEST_MAX_HOURS_TO_EXPIRY")
                .ok()
                .and_then(|v| v.parse().ok()),
            audit_window_days: optional_parse(
                "PE_BACKTEST_AUDIT_WINDOW_DAYS",
                DEFAULT_AUDIT_WINDOW_DAYS,
            ),
            ranker_min_quality: optional_parse("PE_BACKTEST_MIN_QUALITY", DEFAULT_BT_MIN_QUALITY),
            ranker_active_min_closed: optional_parse(
                "PE_BACKTEST_ACTIVE_MIN_CLOSED",
                DEFAULT_BT_ACTIVE_MIN_CLOSED,
            ),
            ranker_active_min_markets: optional_parse(
                "PE_BACKTEST_ACTIVE_MIN_MARKETS",
                DEFAULT_BT_ACTIVE_MIN_MARKETS,
            ),
            ranker_incubator_min_closed: optional_parse(
                "PE_BACKTEST_INCUBATOR_MIN_CLOSED",
                DEFAULT_BT_INCUBATOR_MIN_CLOSED,
            ),
            ranker_incubator_min_markets: optional_parse(
                "PE_BACKTEST_INCUBATOR_MIN_MARKETS",
                DEFAULT_BT_INCUBATOR_MIN_MARKETS,
            ),
            kelly_sweep_fractions,
        })
    }
}

// Canonical default fractions — see `docs/_GLOSSARY.md` `backtest_kelly_sweep_fractions_default`.
const SWEEP_DEFAULTS: &str = "0.10,0.25,0.50,0.75,1.0";

/// Parse `PE_BACKTEST_KELLY_SWEEP` into validated fractions.
///
/// Returns `None` when the env var is absent (single-run mode).
/// Returns `Some(fractions)` when present; uses `SWEEP_DEFAULTS` when the value is empty.
/// Each fraction must be in `(0.0, 1.0]`.
fn parse_kelly_sweep() -> Result<Option<Vec<KellyFraction>>, BacktestError> {
    let raw = match std::env::var("PE_BACKTEST_KELLY_SWEEP") {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let src = if raw.trim().is_empty() {
        SWEEP_DEFAULTS
    } else {
        raw.trim()
    };
    let mut fractions = Vec::new();
    for part in src.split(',') {
        let trimmed = part.trim();
        let d: Decimal = trimmed.parse().map_err(|_| {
            BacktestError::InvalidKellySweep(format!("'{trimmed}' is not a valid decimal"))
        })?;
        if d <= Decimal::ZERO || d > Decimal::ONE {
            return Err(BacktestError::InvalidKellySweep(format!(
                "{d} is not in (0.0, 1.0] — fractions must be strictly positive and at most 1.0 (full Kelly)"
            )));
        }
        fractions.push(KellyFraction(d));
    }
    if fractions.is_empty() {
        return Err(BacktestError::InvalidKellySweep(
            "PE_BACKTEST_KELLY_SWEEP produced no fractions".to_owned(),
        ));
    }
    Ok(Some(fractions))
}
