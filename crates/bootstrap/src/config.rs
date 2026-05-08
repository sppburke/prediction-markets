//! `BootstrapConfig` — loaded from an optional TOML file with `PE_*` env var overlay.

use std::path::{Path, PathBuf};

use figment::{
    Figment,
    providers::{Env, Format, Toml},
};
use pe_core_types::WalletAddress;
use pe_source_onchain_polygon::contracts::CTF_EXCHANGE_V1_DEPLOY_BLOCK;
use serde::{Deserialize, Serialize};

use crate::{
    WalletSource,
    error::BootstrapError,
    filter::{
        DEFAULT_ACTIVE_WINDOW_DAYS, DEFAULT_MAX_AVG_HOURS_TO_RESOLUTION, DEFAULT_MIN_CLOSED_TRADES,
        DEFAULT_MIN_WIN_RATE_PCT,
    },
};

// Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
const DEFAULT_DUNE_MIN_CLOSED_MARKETS: u32 = 15;
const DEFAULT_DUNE_MIN_WIN_RATE_PCT: u32 = 95;
const DEFAULT_DUNE_ACTIVE_WINDOW_DAYS: u32 = 30;
const DEFAULT_DUNE_MAX_AVG_HOURS_TO_RESOLUTION: u32 = 72;
const DEFAULT_POLYMARKET_BASE_URL: &str = "https://data-api.polymarket.com";
const DEFAULT_POLYMARKET_CONCURRENCY: usize = 16;

/// Bootstrap configuration loaded from an optional TOML file with `PE_*` env var overlay.
///
/// ## Loading order (lowest → highest priority)
/// 1. Struct defaults (`#[serde(default)]`).
/// 2. TOML file (when a path is provided as the first CLI argument).
/// 3. `PE_*` environment variables.
/// 4. `PE_BOOTSTRAP_*` environment variables (highest priority).
///
/// ## TOML structure
/// ```toml
/// wallet_source = "etherscan"
/// etherscan_api_key = "..."
/// output_path = "/home/user/watchlist.json"
/// cache_path = "/home/user/backtest-data/wallet_cache.db"
///
/// operator_addresses = ["0xaaa...", "0xbbb..."]
/// ```
///
/// Run `pe-bootstrap --print-config` to emit the full default configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapConfig {
    /// `PE_WALLET_SOURCE` — `"etherscan"` (default) or `"dune"`.
    #[serde(default = "default_wallet_source")]
    pub wallet_source: WalletSource,

    /// Required when `wallet_source = "dune"`. `PE_DUNE_API_KEY` overrides.
    #[serde(default)]
    pub dune_api_key: Option<String>,

    /// Dune username for server-side JOIN. `PE_DUNE_NAMESPACE` overrides.
    #[serde(default)]
    pub dune_namespace: Option<String>,

    /// Required when `wallet_source = "etherscan"`. `PE_ETHERSCAN_API_KEY` overrides.
    #[serde(default)]
    pub etherscan_api_key: Option<String>,

    /// Start block for Etherscan scan (default: CTF V1 deploy block).
    /// `PE_WALLET_FROM_BLOCK` overrides.
    #[serde(default = "default_wallet_from_block")]
    pub wallet_from_block: u64,

    /// End block for Etherscan scan (`None` = current chain head).
    /// `PE_WALLET_TO_BLOCK` overrides.
    #[serde(default)]
    pub wallet_to_block: Option<u64>,

    /// Matching-engine operator addresses to exclude. TOML: array of hex strings.
    /// Env `PE_POLYMARKET_OPERATOR_ADDRESSES`: comma-separated hex.
    #[serde(
        default,
        alias = "polymarket_operator_addresses",
        deserialize_with = "deserialize_operator_addresses"
    )]
    pub operator_addresses: Vec<WalletAddress>,

    /// Output path for the watchlist JSON. **Required.**
    ///
    /// Set via TOML `output_path = "..."` or env `PE_BOOTSTRAP_OUTPUT`.
    /// Aliases: `output` (PE_BOOTSTRAP_ prefix), `bootstrap_output` (PE_ prefix).
    #[serde(alias = "output", alias = "bootstrap_output")]
    pub output_path: PathBuf,

    /// Path to the wallet trade SQLite cache. `PE_BOOTSTRAP_CACHE_PATH` overrides.
    #[serde(default = "default_cache_path", alias = "bootstrap_cache_path")]
    pub cache_path: PathBuf,

    /// Path to the enumerated wallet address list. `PE_BOOTSTRAP_WALLET_SET_PATH` overrides.
    #[serde(
        default = "default_wallet_set_path",
        alias = "bootstrap_wallet_set_path"
    )]
    pub wallet_set_path: PathBuf,

    /// Minimum distinct resolved markets for Dune discovery.
    /// `PE_BOOTSTRAP_DUNE_MIN_MARKETS` overrides.
    #[serde(
        default = "default_dune_min_closed_markets",
        alias = "dune_min_markets",
        alias = "bootstrap_dune_min_markets"
    )]
    pub dune_min_closed_markets: u32,

    /// Minimum win-rate percent for Dune discovery.
    /// `PE_BOOTSTRAP_DUNE_MIN_WIN_RATE_PCT` overrides.
    #[serde(
        default = "default_dune_min_win_rate_pct",
        alias = "bootstrap_dune_min_win_rate_pct"
    )]
    pub dune_min_win_rate_pct: u32,

    /// Recency window in days for Dune discovery. `PE_BOOTSTRAP_DUNE_ACTIVE_DAYS` overrides.
    #[serde(
        default = "default_dune_active_window_days",
        alias = "dune_active_days",
        alias = "bootstrap_dune_active_days"
    )]
    pub dune_active_window_days: u32,

    /// Maximum average hours to resolution for Dune discovery.
    /// `PE_BOOTSTRAP_DUNE_MAX_AVG_HOURS` overrides.
    #[serde(
        default = "default_dune_max_avg_hours",
        alias = "dune_max_avg_hours",
        alias = "bootstrap_dune_max_avg_hours"
    )]
    pub dune_max_avg_hours_to_resolution: u32,

    /// Trade lookback in days (`None` = unlimited).
    /// Env `PE_BOOTSTRAP_AUDIT_WINDOW_DAYS`: integer, `"unlimited"`, or `"none"`.
    #[serde(
        default,
        alias = "bootstrap_audit_window_days",
        deserialize_with = "deserialize_audit_window"
    )]
    pub audit_window_days: Option<u32>,

    /// Minimum closed trades for post-filter. `PE_BOOTSTRAP_MIN_CLOSED_TRADES` overrides.
    #[serde(
        default = "default_min_closed_trades",
        alias = "bootstrap_min_closed_trades"
    )]
    pub min_closed_trades: usize,

    /// Minimum win-rate percent for post-filter. `PE_BOOTSTRAP_MIN_WIN_RATE_PCT` overrides.
    #[serde(
        default = "default_min_win_rate_pct",
        alias = "bootstrap_min_win_rate_pct"
    )]
    pub min_win_rate_pct: u8,

    /// Recency window in days for post-filter.
    /// `PE_BOOTSTRAP_POST_FILTER_ACTIVE_DAYS` overrides.
    #[serde(
        default = "default_post_filter_active_window_days",
        alias = "post_filter_active_days",
        alias = "bootstrap_post_filter_active_days"
    )]
    pub post_filter_active_window_days: u32,

    /// Maximum average hours to resolution for post-filter.
    /// `PE_BOOTSTRAP_POST_FILTER_MAX_AVG_HOURS` overrides.
    #[serde(
        default = "default_post_filter_max_avg_hours",
        alias = "post_filter_max_avg_hours",
        alias = "bootstrap_post_filter_max_avg_hours"
    )]
    pub post_filter_max_avg_hours_to_resolution: u32,

    /// Base URL for the Polymarket Data API. `PE_POLYMARKET_BASE_URL` overrides.
    #[serde(default = "default_polymarket_base_url")]
    pub polymarket_base_url: String,

    /// Concurrent wallet fetches against the Polymarket Data API.
    /// `PE_BOOTSTRAP_POLYMARKET_CONCURRENCY` overrides.
    #[serde(
        default = "default_polymarket_concurrency",
        alias = "bootstrap_polymarket_concurrency"
    )]
    pub polymarket_concurrency: usize,

    /// Fetch market resolutions from Gamma after trade fetch (off by default).
    /// Env `PE_BOOTSTRAP_FETCH_RESOLUTIONS`: `"1"` or `"true"` to enable.
    #[serde(
        default,
        alias = "bootstrap_fetch_resolutions",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub fetch_resolutions: bool,

    /// Gamma API base URL. `PE_GAMMA_BASE_URL` overrides.
    #[serde(default = "default_gamma_base_url")]
    pub gamma_base_url: String,

    /// Fetch funder edges via Etherscan after trade fetch (off by default).
    /// Env `PE_BOOTSTRAP_FETCH_FUNDER_GRAPH`: `"1"` or `"true"` to enable.
    #[serde(
        default,
        alias = "bootstrap_fetch_funder_graph",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub fetch_funder_graph: bool,

    /// Skip the Polymarket trade-fetch step (off by default).
    /// Env `PE_BOOTSTRAP_SKIP_TRADE_FETCH`: `"1"` or `"true"` to enable.
    #[serde(
        default,
        alias = "bootstrap_skip_trade_fetch",
        deserialize_with = "deserialize_bool_or_01"
    )]
    pub skip_trade_fetch: bool,
}

impl BootstrapConfig {
    fn validate(&self) -> Result<(), BootstrapError> {
        match self.wallet_source {
            WalletSource::Dune if self.dune_api_key.is_none() => {
                Err(BootstrapError::MissingEnv("PE_DUNE_API_KEY".to_owned()))
            }
            WalletSource::Etherscan if self.etherscan_api_key.is_none() => Err(
                BootstrapError::MissingEnv("PE_ETHERSCAN_API_KEY".to_owned()),
            ),
            _ => Ok(()),
        }
    }
}

// ── Default helpers ───────────────────────────────────────────────────────────

fn default_wallet_source() -> WalletSource {
    WalletSource::Etherscan
}

const fn default_wallet_from_block() -> u64 {
    CTF_EXCHANGE_V1_DEPLOY_BLOCK
}

fn default_cache_path() -> PathBuf {
    PathBuf::from("wallet_cache.db")
}

fn default_wallet_set_path() -> PathBuf {
    PathBuf::from("wallet_set.json")
}

const fn default_dune_min_closed_markets() -> u32 {
    DEFAULT_DUNE_MIN_CLOSED_MARKETS
}

const fn default_dune_min_win_rate_pct() -> u32 {
    DEFAULT_DUNE_MIN_WIN_RATE_PCT
}

const fn default_dune_active_window_days() -> u32 {
    DEFAULT_DUNE_ACTIVE_WINDOW_DAYS
}

const fn default_dune_max_avg_hours() -> u32 {
    DEFAULT_DUNE_MAX_AVG_HOURS_TO_RESOLUTION
}

fn default_min_closed_trades() -> usize {
    DEFAULT_MIN_CLOSED_TRADES
}

const fn default_min_win_rate_pct() -> u8 {
    DEFAULT_MIN_WIN_RATE_PCT
}

const fn default_post_filter_active_window_days() -> u32 {
    DEFAULT_ACTIVE_WINDOW_DAYS
}

const fn default_post_filter_max_avg_hours() -> u32 {
    DEFAULT_MAX_AVG_HOURS_TO_RESOLUTION
}

fn default_polymarket_base_url() -> String {
    DEFAULT_POLYMARKET_BASE_URL.to_owned()
}

const fn default_polymarket_concurrency() -> usize {
    DEFAULT_POLYMARKET_CONCURRENCY
}

fn default_gamma_base_url() -> String {
    crate::gamma::DEFAULT_GAMMA_BASE_URL.to_owned()
}

// ── Default impl ──────────────────────────────────────────────────────────────

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            wallet_source: default_wallet_source(),
            dune_api_key: None,
            dune_namespace: None,
            etherscan_api_key: None,
            wallet_from_block: default_wallet_from_block(),
            wallet_to_block: None,
            operator_addresses: Vec::new(),
            output_path: PathBuf::from("./watchlist.json"),
            cache_path: default_cache_path(),
            wallet_set_path: default_wallet_set_path(),
            dune_min_closed_markets: default_dune_min_closed_markets(),
            dune_min_win_rate_pct: default_dune_min_win_rate_pct(),
            dune_active_window_days: default_dune_active_window_days(),
            dune_max_avg_hours_to_resolution: default_dune_max_avg_hours(),
            audit_window_days: None,
            min_closed_trades: default_min_closed_trades(),
            min_win_rate_pct: default_min_win_rate_pct(),
            post_filter_active_window_days: default_post_filter_active_window_days(),
            post_filter_max_avg_hours_to_resolution: default_post_filter_max_avg_hours(),
            polymarket_base_url: default_polymarket_base_url(),
            polymarket_concurrency: default_polymarket_concurrency(),
            fetch_resolutions: false,
            gamma_base_url: default_gamma_base_url(),
            fetch_funder_graph: false,
            skip_trade_fetch: false,
        }
    }
}

// ── Loader ────────────────────────────────────────────────────────────────────

/// Load `BootstrapConfig` from an optional TOML file with `PE_*` env vars overlaid.
///
/// When `path` is `Some`, the TOML file is read first; env vars override individual fields.
/// When `path` is `None`, only env vars and struct defaults apply.
///
/// `PE_BOOTSTRAP_*` env vars take priority over `PE_*` env vars; both are supported.
/// Post-load validation checks that the required API key for the chosen `wallet_source`
/// is present.
pub fn load(path: Option<&Path>) -> Result<BootstrapConfig, BootstrapError> {
    let mut fig = Figment::new();
    if let Some(p) = path {
        fig = fig.merge(Toml::file(p));
    }
    let cfg: BootstrapConfig = fig
        .merge(Env::prefixed("PE_").lowercase(true))
        .merge(Env::prefixed("PE_BOOTSTRAP_").lowercase(true))
        .extract()?;
    cfg.validate()?;
    Ok(cfg)
}

// ── Custom deserializers ──────────────────────────────────────────────────────

fn deserialize_bool_or_01<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = bool;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("bool, '0', or '1'")
        }

        fn visit_bool<E: de::Error>(self, v: bool) -> Result<bool, E> {
            Ok(v)
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<bool, E> {
            match v.trim() {
                "1" | "true" => Ok(true),
                "0" | "false" | "" => Ok(false),
                other => Err(de::Error::custom(format!(
                    "expected '0', '1', 'true', or 'false'; got '{other}'"
                ))),
            }
        }
    }

    d.deserialize_any(V)
}

fn deserialize_audit_window<'de, D>(d: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<u32>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("integer, null, 'unlimited', or 'none'")
        }

        fn visit_none<E: de::Error>(self) -> Result<Option<u32>, E> {
            Ok(None)
        }

        fn visit_unit<E: de::Error>(self) -> Result<Option<u32>, E> {
            Ok(None)
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Option<u32>, E> {
            u32::try_from(v)
                .map(Some)
                .map_err(|_| de::Error::custom(format!("{v} overflows u32")))
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Option<u32>, E> {
            u32::try_from(v)
                .map(Some)
                .map_err(|_| de::Error::custom(format!("{v} is not a valid positive integer")))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Option<u32>, E> {
            let v = v.trim().to_lowercase();
            if v.is_empty() || v == "none" || v == "unlimited" {
                return Ok(None);
            }
            v.parse::<u32>()
                .map(Some)
                .map_err(|_| de::Error::custom(format!("'{v}' is not a valid u32 or 'unlimited'")))
        }
    }

    d.deserialize_any(V)
}

fn deserialize_operator_addresses<'de, D>(d: D) -> Result<Vec<WalletAddress>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, SeqAccess, Visitor};

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Vec<WalletAddress>;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("comma-separated hex addresses or array of hex addresses")
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<Vec<WalletAddress>, E> {
            if v.trim().is_empty() {
                return Ok(Vec::new());
            }
            v.split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|hex| {
                    WalletAddress::from_hex(hex.trim())
                        .map_err(|e| de::Error::custom(format!("invalid address '{hex}': {e}")))
                })
                .collect()
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<WalletAddress>, A::Error> {
            let mut addrs = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                addrs.push(
                    WalletAddress::from_hex(s.trim())
                        .map_err(|e| de::Error::custom(format!("invalid address '{s}': {e}")))?,
                );
            }
            Ok(addrs)
        }
    }

    d.deserialize_any(V)
}
