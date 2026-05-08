//! `BacktestConfig` — loaded from an optional TOML file with `PE_*` env var overlay.

use std::path::{Path, PathBuf};

use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use pe_core_types::KellyFraction;
use pe_strategy_winner_follow::WinnerFollowConfig;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::error::BacktestError;

// Canonical defaults in `docs/_GLOSSARY.md` "Backtest defaults" section.
const DEFAULT_STEP_DAYS: u32 = 1;
const DEFAULT_AUDIT_WINDOW_DAYS: u32 = 90;
const DEFAULT_BT_MIN_QUALITY: u8 = 0;
const DEFAULT_BT_ACTIVE_MIN_CLOSED: u32 = 10;
const DEFAULT_BT_INCUBATOR_MIN_CLOSED: u32 = 3;
const DEFAULT_BT_KELLY_P_PRIOR_ALPHA: u32 = 10;
const DEFAULT_BT_KELLY_P_PRIOR_BETA: u32 = 10;
const DEFAULT_BT_KELLY_P_K_PER_MARKET: u32 = 6;

// Default fractions for sweep mode when env var is set but empty.
// Canonical: `docs/_GLOSSARY.md` `backtest_kelly_sweep_fractions_default`.
const SWEEP_DEFAULTS: &str = "0.10,0.25,0.50,0.75,1.0";

/// Backtest configuration loaded from an optional TOML file with `PE_*` env var overlay.
///
/// ## Loading order (lowest → highest priority)
/// 1. Struct defaults (`#[serde(default)]`).
/// 2. TOML file (when a path is provided as the first CLI argument).
/// 3. `PE_*` environment variables.
/// 4. `PE_BACKTEST_*` environment variables (highest priority).
///
/// ## TOML structure
/// ```toml
/// bootstrap_cache_path = "/home/user/backtest-data/wallet_cache.db"
/// output_dir = "/home/user/backtest-output"
/// bankroll_usd = "10000"
///
/// [strategy]
/// per_trade_cap = { kind = "unlimited" }
/// slippage_rate = "0.01"
/// ```
///
/// Run `pe-backtest --print-config` to emit the full default configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestConfig {
    /// Path to the bootstrap wallet trade SQLite cache.
    /// `PE_BOOTSTRAP_CACHE_PATH` overrides.
    #[serde(default = "default_cache_path")]
    pub bootstrap_cache_path: PathBuf,

    /// Output directory for `report.json` and `trades.ndjson`. **Required.**
    ///
    /// Set via TOML `output_dir = "..."` or env `PE_BACKTEST_OUTPUT_DIR`.
    /// Loading fails loudly if absent from both TOML and env.
    pub output_dir: PathBuf,

    /// Starting bankroll in USD. `PE_BANKROLL_USD` overrides.
    #[serde(default = "default_bankroll")]
    pub bankroll_usd: Decimal,

    /// Walk-forward step in days. `PE_BACKTEST_STEP_DAYS` overrides.
    #[serde(default = "default_step_days")]
    pub step_days: u32,

    /// Dune Analytics API key for on-chain resolution fetch.
    /// `PE_DUNE_API_KEY` overrides. Never commit this value in TOML.
    #[serde(default)]
    pub dune_api_key: Option<String>,

    /// Dune username for server-side JOIN (reduces credit cost).
    /// `PE_DUNE_NAMESPACE` overrides.
    #[serde(default)]
    pub dune_namespace: Option<String>,

    /// Only copy trades where the market resolves within this many hours.
    /// `None` = no filter. `PE_BACKTEST_MAX_HOURS_TO_EXPIRY` overrides.
    #[serde(default)]
    pub max_hours_to_expiry: Option<u32>,

    /// Trade lookback window for ledger reconstruction. `PE_BACKTEST_AUDIT_WINDOW_DAYS` overrides.
    #[serde(default = "default_audit_window_days")]
    pub audit_window_days: u32,

    /// Min reconstruction quality for watchlist eligibility (default: 0).
    /// `PE_BACKTEST_MIN_QUALITY` overrides.
    #[serde(default = "default_min_quality", alias = "min_quality")]
    pub ranker_min_quality: u8,

    /// Min closed trades in 180-day window for active tier (default: 10).
    /// `PE_BACKTEST_ACTIVE_MIN_CLOSED` overrides.
    #[serde(default = "default_active_min_closed", alias = "active_min_closed")]
    pub ranker_active_min_closed: u32,

    /// Min distinct markets in 180-day window for active tier (default: 1).
    /// `PE_BACKTEST_ACTIVE_MIN_MARKETS` overrides.
    #[serde(default = "default_one_u32", alias = "active_min_markets")]
    pub ranker_active_min_markets: u32,

    /// Min closed trades in 90-day window for incubator tier (default: 3).
    /// `PE_BACKTEST_INCUBATOR_MIN_CLOSED` overrides.
    #[serde(
        default = "default_incubator_min_closed",
        alias = "incubator_min_closed"
    )]
    pub ranker_incubator_min_closed: u32,

    /// Min distinct markets in 90-day window for incubator tier (default: 1).
    /// `PE_BACKTEST_INCUBATOR_MIN_MARKETS` overrides.
    #[serde(default = "default_one_u32", alias = "incubator_min_markets")]
    pub ranker_incubator_min_markets: u32,

    /// Kelly fractions to sweep (`None` = single-run mode).
    ///
    /// TOML: `kelly_sweep_fractions = [0.10, 0.25, 0.50, 1.0]`.
    /// Env: `PE_BACKTEST_KELLY_SWEEP=0.10,0.25,0.50,1.0` (CSV; empty = default fractions).
    /// Each fraction must be in (0.0, 1.0].
    #[serde(
        default,
        alias = "kelly_sweep",
        deserialize_with = "deserialize_kelly_sweep_opt"
    )]
    pub kelly_sweep_fractions: Option<Vec<KellyFraction>>,

    /// α of the Beta prior on leader win-rate (default: 10). Set both to 0 for raw rate.
    /// `PE_BACKTEST_KELLY_P_PRIOR_ALPHA` overrides.
    #[serde(default = "default_kelly_p_prior_alpha")]
    pub kelly_p_prior_alpha: u32,

    /// β of the Beta prior on leader win-rate (default: 10).
    /// `PE_BACKTEST_KELLY_P_PRIOR_BETA` overrides.
    #[serde(default = "default_kelly_p_prior_beta")]
    pub kelly_p_prior_beta: u32,

    /// `N_eff = min(total, distinct_markets × k)` scaling factor (default: 6).
    /// `k = 0` bypasses N_eff. `PE_BACKTEST_KELLY_P_K_PER_MARKET` overrides.
    #[serde(default = "default_kelly_p_k_per_market")]
    pub kelly_p_k_per_market: u32,

    /// Strategy configuration — all Winner-Follow parameters.
    ///
    /// TOML sub-table `[strategy]`. When absent, `WinnerFollowConfig::default()` applies:
    /// mode-based Kelly fractions, 100 bps slippage, `ModeDefault` per-trade cap.
    #[serde(default)]
    pub strategy: WinnerFollowConfig,
}

// ── Default helpers ───────────────────────────────────────────────────────────

fn default_cache_path() -> PathBuf {
    PathBuf::from("wallet_cache.db")
}

fn default_bankroll() -> Decimal {
    Decimal::from(10_000u32)
}

const fn default_step_days() -> u32 {
    DEFAULT_STEP_DAYS
}

const fn default_audit_window_days() -> u32 {
    DEFAULT_AUDIT_WINDOW_DAYS
}

const fn default_min_quality() -> u8 {
    DEFAULT_BT_MIN_QUALITY
}

const fn default_active_min_closed() -> u32 {
    DEFAULT_BT_ACTIVE_MIN_CLOSED
}

const fn default_one_u32() -> u32 {
    1
}

const fn default_incubator_min_closed() -> u32 {
    DEFAULT_BT_INCUBATOR_MIN_CLOSED
}

const fn default_kelly_p_prior_alpha() -> u32 {
    DEFAULT_BT_KELLY_P_PRIOR_ALPHA
}

const fn default_kelly_p_prior_beta() -> u32 {
    DEFAULT_BT_KELLY_P_PRIOR_BETA
}

const fn default_kelly_p_k_per_market() -> u32 {
    DEFAULT_BT_KELLY_P_K_PER_MARKET
}

// ── Default impl ──────────────────────────────────────────────────────────────

impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            bootstrap_cache_path: default_cache_path(),
            output_dir: PathBuf::from("./pe-backtest-output"),
            bankroll_usd: default_bankroll(),
            step_days: default_step_days(),
            dune_api_key: None,
            dune_namespace: None,
            max_hours_to_expiry: None,
            audit_window_days: default_audit_window_days(),
            ranker_min_quality: default_min_quality(),
            ranker_active_min_closed: default_active_min_closed(),
            ranker_active_min_markets: default_one_u32(),
            ranker_incubator_min_closed: default_incubator_min_closed(),
            ranker_incubator_min_markets: default_one_u32(),
            kelly_sweep_fractions: None,
            kelly_p_prior_alpha: default_kelly_p_prior_alpha(),
            kelly_p_prior_beta: default_kelly_p_prior_beta(),
            kelly_p_k_per_market: default_kelly_p_k_per_market(),
            strategy: WinnerFollowConfig::default(),
        }
    }
}

// ── Loader ────────────────────────────────────────────────────────────────────

/// Load `BacktestConfig` from an optional TOML file with `PE_*` env vars overlaid.
///
/// When `path` is `Some`, the TOML file is read first; env vars override individual fields.
/// When `path` is `None`, only env vars and struct defaults apply.
///
/// `PE_BACKTEST_*` env vars take priority over `PE_*` env vars; both are supported.
/// `output_dir` is **required** — loading fails if absent from both TOML and env.
pub fn load(path: Option<&Path>) -> Result<BacktestConfig, BacktestError> {
    let mut fig = Figment::new();
    if let Some(p) = path {
        fig = fig.merge(Toml::file(p));
    }
    let cfg = fig
        .merge(
            Env::prefixed("PE_")
                .lowercase(true)
                .filter(|k| !k.starts_with("BACKTEST_")),
        )
        .merge(Env::prefixed("PE_BACKTEST_").lowercase(true))
        .extract()?;
    Ok(cfg)
}

// ── Custom deserializers ──────────────────────────────────────────────────────

fn deserialize_kelly_sweep_opt<'de, D>(d: D) -> Result<Option<Vec<KellyFraction>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, SeqAccess, Visitor};

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<KellyFraction>>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(
                "null, empty string, a comma-separated string, \
                 or array of Kelly fractions in (0.0, 1.0]",
            )
        }

        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let v = v.trim();
            // Empty env var → default fractions (matches old parse_kelly_sweep).
            let src = if v.is_empty() { SWEEP_DEFAULTS } else { v };
            parse_csv_fractions(src)
                .map(Some)
                .map_err(de::Error::custom)
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut fractions = Vec::new();
            while let Some(d) = seq.next_element::<Decimal>()? {
                if d <= Decimal::ZERO || d > Decimal::ONE {
                    return Err(de::Error::custom(format!(
                        "{d} is not in (0.0, 1.0] — \
                         fractions must be strictly positive and at most 1.0"
                    )));
                }
                fractions.push(KellyFraction(d));
            }
            if fractions.is_empty() {
                Ok(None)
            } else {
                Ok(Some(fractions))
            }
        }
    }

    d.deserialize_any(V)
}

fn parse_csv_fractions(src: &str) -> Result<Vec<KellyFraction>, String> {
    let mut fractions = Vec::new();
    for part in src.split(',') {
        let trimmed = part.trim();
        let d: Decimal = trimmed
            .parse()
            .map_err(|_| format!("'{trimmed}' is not a valid decimal"))?;
        if d <= Decimal::ZERO || d > Decimal::ONE {
            return Err(format!(
                "{d} is not in (0.0, 1.0] — \
                 fractions must be strictly positive and at most 1.0 (full Kelly)"
            ));
        }
        fractions.push(KellyFraction(d));
    }
    if fractions.is_empty() {
        return Err("no fractions found in CSV string".to_owned());
    }
    Ok(fractions)
}
