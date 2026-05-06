//! `pe-bootstrap` — wallet discovery and seed watchlist generation.
//!
//! Pipeline:
//! 1. Discover wallets — load from per-contract checkpoint if available, else
//!    enumerate via Dune Analytics or Etherscan (selected by `PE_WALLET_SOURCE`).
//!    Dune path applies four quality filters at query time (see `dune::WALLET_DISCOVERY_SQL`).
//!    Etherscan: checkpoint saved after each contract; resume skips completed ones.
//! 2. Fetch trade history per wallet from the Polymarket Data API (permanent SQLite cache,
//!    incremental per run).
//! 3. Reconstruct `TraderLedger`s via `pe-trader-index`.
//! 4. Pre-filter: keep wallets with > `min_closed_trades` closed trades and > `min_win_rate_pct`
//!    win rate. Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
//! 5. Build a seed `Watchlist` and write it to `output_path`.
//!
//! Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.

pub mod cache;
pub mod dune;
pub mod error;
pub mod filter;
pub mod polymarket;
pub mod wallet_set;

use std::path::{Path, PathBuf};
use std::time::Duration;

use pe_core_types::{BasisPoints, SourceTimestamp, WalletAddress};
use pe_operator_graph::OperatorIdentity;
use pe_source_onchain_polygon::{
    EnumerationConfig, PolymarketTraderEnumeration,
    contracts::{ALL_EXCHANGE_CONTRACTS, CTF_EXCHANGE_V1_DEPLOY_BLOCK},
};
use pe_source_polymarket_public::ReqwestFetcher;
use pe_trader_index::{
    LedgerConfig, TraderLedger, Watchlist, WatchlistEntry, WatchlistTier, build_trader_ledgers,
    snapshot::TradeSnapshot,
};
use time::OffsetDateTime;

use cache::WalletCache;
use dune::DuneClient;
use error::BootstrapError;
use filter::{DEFAULT_MIN_CLOSED_TRADES, DEFAULT_MIN_WIN_RATE_PCT, passes_filter, win_rate_bps};
use polymarket::PolymarketBulkFetcher;
use rust_decimal::Decimal;

// Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
const DEFAULT_DUNE_MIN_CLOSED_MARKETS: u32 = 15;
const DEFAULT_DUNE_MIN_WIN_RATE_PCT: u32 = 95;
const DEFAULT_DUNE_ACTIVE_WINDOW_DAYS: u32 = 30;
const DEFAULT_DUNE_MAX_AVG_HOURS_TO_RESOLUTION: u32 = 72;
const DEFAULT_POLYMARKET_BASE_URL: &str = "https://data-api.polymarket.com";
const DEFAULT_ETHERSCAN_BASE_URL: &str = "https://api.etherscan.io/v2/api";
// bootstrap_eth_block_timeout_secs = 30
const ETH_BLOCK_TIMEOUT_SECS: u64 = 30;

/// Wallet discovery backend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalletSource {
    /// Use Dune Analytics (legacy path; requires `PE_DUNE_API_KEY`).
    Dune,
    /// Use Etherscan `eth_getLogs` on Polygon (requires `PE_ETHERSCAN_API_KEY`).
    Etherscan,
}

impl WalletSource {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "dune" => Self::Dune,
            "etherscan" => Self::Etherscan,
            other => {
                tracing::warn!(
                    value = other,
                    "unrecognised PE_WALLET_SOURCE; defaulting to etherscan"
                );
                Self::Etherscan
            }
        }
    }
}

/// Configuration for a bootstrap run, sourced from environment variables.
pub struct BootstrapConfig {
    /// `PE_WALLET_SOURCE` — `"etherscan"` (default) or `"dune"`.
    pub wallet_source: WalletSource,
    /// `PE_DUNE_API_KEY` — required when `wallet_source = dune`.
    pub dune_api_key: Option<String>,
    /// `PE_ETHERSCAN_API_KEY` — required when `wallet_source = etherscan`.
    pub etherscan_api_key: Option<String>,
    /// `PE_WALLET_FROM_BLOCK` — start block for Etherscan scan (default: CTF V1 deploy block).
    /// Ignored when `wallet_source = dune`.
    pub wallet_from_block: u64,
    /// `PE_WALLET_TO_BLOCK` — end block for Etherscan scan (default: current chain head).
    /// Ignored when `wallet_source = dune`.
    pub wallet_to_block: Option<u64>,
    /// `PE_POLYMARKET_OPERATOR_ADDRESSES` — comma-separated hex addresses to exclude from the
    /// enumerated wallet set (e.g. Polymarket matching operators). Ignored when `wallet_source = dune`.
    pub operator_addresses: Vec<WalletAddress>,
    /// `PE_BOOTSTRAP_OUTPUT` — path where the `Watchlist` JSON is written.
    pub output_path: PathBuf,
    /// `PE_BOOTSTRAP_CACHE_PATH` — path to the wallet trade SQLite cache file.
    pub cache_path: PathBuf,
    /// `PE_BOOTSTRAP_WALLET_SET_PATH` — path to the enumerated wallet address list.
    /// If the file exists, Etherscan/Dune enumeration is skipped entirely.
    /// Delete the file to force a fresh scan. Default: `wallet_set.json`.
    pub wallet_set_path: PathBuf,
    /// Minimum distinct resolved markets a wallet must have traded (Dune filter).
    /// Default `bootstrap_dune_min_closed_markets = 15`. Env: `PE_BOOTSTRAP_DUNE_MIN_MARKETS`.
    pub dune_min_closed_markets: u32,
    /// Minimum win-rate percent for Dune wallet discovery (default `bootstrap_dune_min_win_rate_pct = 95`).
    /// Env: `PE_BOOTSTRAP_DUNE_MIN_WIN_RATE_PCT`.
    pub dune_min_win_rate_pct: u32,
    /// Recency window for Dune wallet discovery: wallet must have a trade on a resolved market
    /// within this many days (default `bootstrap_dune_active_window_days = 30`).
    /// Env: `PE_BOOTSTRAP_DUNE_ACTIVE_DAYS`.
    pub dune_active_window_days: u32,
    /// Maximum average hours from first entry to market resolution for Dune wallets
    /// (default `bootstrap_dune_max_avg_hours_to_resolution = 72`).
    /// Env: `PE_BOOTSTRAP_DUNE_MAX_AVG_HOURS`.
    pub dune_max_avg_hours_to_resolution: u32,
    /// Trade lookback window for ledger reconstruction.
    /// `None` = unlimited (default); `Some(n)` = at most n calendar days.
    /// Env var `PE_BOOTSTRAP_AUDIT_WINDOW_DAYS`: an integer, or empty / `"unlimited"` / `"none"` for unlimited.
    /// Canonical default in `docs/_GLOSSARY.md`: `bootstrap_polymarket_audit_window_days = None (unlimited)`.
    pub audit_window_days: Option<u32>,
    /// Minimum closed trades to pass the pre-filter (default `bootstrap_min_closed_trades = 10`).
    pub min_closed_trades: usize,
    /// Minimum win-rate percent to pass the pre-filter (default `bootstrap_min_win_rate_pct = 80`).
    pub min_win_rate_pct: u8,
    /// Base URL for the Polymarket Data API.
    pub polymarket_base_url: String,
    /// Concurrent wallet fetches against the Polymarket Data API
    /// (default `bootstrap_polymarket_concurrency = 16`).
    pub polymarket_concurrency: usize,
}

impl BootstrapConfig {
    /// Build from environment variables. Returns an error for missing required vars.
    pub fn from_env() -> Result<Self, BootstrapError> {
        fn require(key: &str) -> Result<String, BootstrapError> {
            std::env::var(key).map_err(|_| BootstrapError::MissingEnv(key.to_owned()))
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
        fn parse_audit_window(key: &str) -> Option<u32> {
            match std::env::var(key) {
                Err(_) => None,
                Ok(v) => {
                    let v = v.trim().to_lowercase();
                    if v.is_empty() || v == "none" || v == "unlimited" {
                        None
                    } else {
                        match v.parse::<u32>() {
                            Ok(n) => Some(n),
                            Err(_) => {
                                tracing::warn!(
                                    value = %v,
                                    "PE_BOOTSTRAP_AUDIT_WINDOW_DAYS: not a valid u32 or 'unlimited'; using unlimited"
                                );
                                None
                            }
                        }
                    }
                }
            }
        }

        let wallet_source = WalletSource::from_str(&optional("PE_WALLET_SOURCE", "etherscan"));

        let (dune_api_key, etherscan_api_key) = match &wallet_source {
            WalletSource::Dune => (Some(require("PE_DUNE_API_KEY")?), None),
            WalletSource::Etherscan => (None, Some(require("PE_ETHERSCAN_API_KEY")?)),
        };

        // Parse optional wallet_to_block; warn if set but unparseable (so the operator
        // knows their explicit value was ignored rather than silently falling back to
        // fetching the current chain head).
        let wallet_to_block = match std::env::var("PE_WALLET_TO_BLOCK") {
            Err(_) => None,
            Ok(v) => match v.parse::<u64>() {
                Ok(n) => Some(n),
                Err(_) => {
                    tracing::warn!(
                        value = v,
                        "PE_WALLET_TO_BLOCK is not a valid u64; falling back to current chain head"
                    );
                    None
                }
            },
        };

        let operator_addresses = std::env::var("PE_POLYMARKET_OPERATOR_ADDRESSES")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|hex| {
                WalletAddress::from_hex(hex.trim())
                    .map_err(|e| {
                        tracing::warn!(address = hex, error = %e, "skipping invalid operator address");
                    })
                    .ok()
            })
            .collect();

        Ok(Self {
            wallet_source,
            dune_api_key,
            etherscan_api_key,
            wallet_from_block: optional_parse("PE_WALLET_FROM_BLOCK", CTF_EXCHANGE_V1_DEPLOY_BLOCK),
            wallet_to_block,
            operator_addresses,
            output_path: PathBuf::from(require("PE_BOOTSTRAP_OUTPUT")?),
            cache_path: PathBuf::from(optional("PE_BOOTSTRAP_CACHE_PATH", "wallet_cache.db")),
            wallet_set_path: PathBuf::from(optional(
                "PE_BOOTSTRAP_WALLET_SET_PATH",
                "wallet_set.json",
            )),
            dune_min_closed_markets: optional_parse(
                "PE_BOOTSTRAP_DUNE_MIN_MARKETS",
                DEFAULT_DUNE_MIN_CLOSED_MARKETS,
            ),
            dune_min_win_rate_pct: optional_parse(
                "PE_BOOTSTRAP_DUNE_MIN_WIN_RATE_PCT",
                DEFAULT_DUNE_MIN_WIN_RATE_PCT,
            ),
            dune_active_window_days: optional_parse(
                "PE_BOOTSTRAP_DUNE_ACTIVE_DAYS",
                DEFAULT_DUNE_ACTIVE_WINDOW_DAYS,
            ),
            dune_max_avg_hours_to_resolution: optional_parse(
                "PE_BOOTSTRAP_DUNE_MAX_AVG_HOURS",
                DEFAULT_DUNE_MAX_AVG_HOURS_TO_RESOLUTION,
            ),
            audit_window_days: parse_audit_window("PE_BOOTSTRAP_AUDIT_WINDOW_DAYS"),
            min_closed_trades: optional_parse(
                "PE_BOOTSTRAP_MIN_CLOSED_TRADES",
                DEFAULT_MIN_CLOSED_TRADES,
            ),
            min_win_rate_pct: optional_parse(
                "PE_BOOTSTRAP_MIN_WIN_RATE_PCT",
                DEFAULT_MIN_WIN_RATE_PCT,
            ),
            polymarket_base_url: optional("PE_POLYMARKET_BASE_URL", DEFAULT_POLYMARKET_BASE_URL),
            polymarket_concurrency: optional_parse(
                "PE_BOOTSTRAP_POLYMARKET_CONCURRENCY",
                polymarket::DEFAULT_CONCURRENCY,
            ),
        })
    }
}

/// Run the full bootstrap pipeline and return the seed [`Watchlist`].
///
/// Writes the watchlist as pretty-printed JSON to `config.output_path`.
pub async fn run(config: &BootstrapConfig) -> Result<Watchlist, BootstrapError> {
    // 1. Discover wallets — load from checkpoint if available; enumerate per-contract
    //    and checkpoint after each (Etherscan) or all-at-once (Dune).
    //    Legacy bare-array files (pre-checkpoint format) are upgraded in-place.
    let wallets: Vec<WalletAddress> = {
        let mut state = match wallet_set::load_state(&config.wallet_set_path)? {
            Some(s) => s,
            None => {
                // Try legacy bare-array format written by pre-checkpoint binary.
                match wallet_set::load(&config.wallet_set_path)? {
                    Some(legacy) => {
                        tracing::info!(
                            count = legacy.len(),
                            path = %config.wallet_set_path.display(),
                            "bootstrap: upgrading legacy wallet set to checkpoint format"
                        );
                        let upgraded = wallet_set::WalletSetState {
                            completed_contracts: ALL_EXCHANGE_CONTRACTS
                                .iter()
                                .map(|c| format!("0x{c:x}"))
                                .collect(),
                            wallets: legacy.iter().map(|w| w.to_string()).collect(),
                        };
                        wallet_set::save_state(&config.wallet_set_path, &upgraded)?;
                        upgraded
                    }
                    None => wallet_set::WalletSetState::default(),
                }
            }
        };

        let total_contracts = ALL_EXCHANGE_CONTRACTS.len();
        if state.completed_contracts.len() >= total_contracts {
            tracing::info!(
                count = state.wallets.len(),
                path = %config.wallet_set_path.display(),
                "bootstrap: all contracts enumerated — skipping wallet discovery"
            );
        } else {
            match &config.wallet_source {
                WalletSource::Dune => {
                    let api_key = config
                        .dune_api_key
                        .clone()
                        .ok_or(BootstrapError::Internal)?;
                    tracing::info!(
                        min_closed_markets = config.dune_min_closed_markets,
                        min_win_rate_pct = config.dune_min_win_rate_pct,
                        active_window_days = config.dune_active_window_days,
                        max_avg_hours_to_resolution = config.dune_max_avg_hours_to_resolution,
                        "bootstrap: querying dune for wallets"
                    );
                    let dune = DuneClient::new(api_key);
                    let found = dune
                        .discover_wallets(
                            config.dune_min_closed_markets,
                            config.dune_min_win_rate_pct,
                            config.dune_active_window_days,
                            config.dune_max_avg_hours_to_resolution,
                        )
                        .await?;
                    tracing::info!(count = found.len(), "bootstrap: dune returned wallets");
                    state.wallets = found.iter().map(|w| w.to_string()).collect();
                    // Mark all contracts complete so subsequent runs skip Dune.
                    state.completed_contracts = ALL_EXCHANGE_CONTRACTS
                        .iter()
                        .map(|c| format!("0x{c:x}"))
                        .collect();
                    wallet_set::save_state(&config.wallet_set_path, &state)?;
                }
                WalletSource::Etherscan => {
                    let api_key = config
                        .etherscan_api_key
                        .clone()
                        .ok_or(BootstrapError::Internal)?;
                    let to_block = match config.wallet_to_block {
                        Some(b) => b,
                        None => fetch_current_block(&api_key).await?,
                    };
                    tracing::info!(
                        from_block = config.wallet_from_block,
                        to_block,
                        operators = config.operator_addresses.len(),
                        contracts_done = state.completed_contracts.len(),
                        contracts_total = total_contracts,
                        "bootstrap: enumerating wallets via etherscan"
                    );
                    let enum_config = EnumerationConfig {
                        from_block: config.wallet_from_block,
                        to_block,
                        operator_addresses: config.operator_addresses.clone(),
                    };
                    let enumerator = PolymarketTraderEnumeration::new(api_key, enum_config);
                    for contract in &ALL_EXCHANGE_CONTRACTS {
                        let contract_hex = format!("0x{contract:x}");
                        if state.completed_contracts.contains(&contract_hex) {
                            tracing::info!(
                                contract = %contract_hex,
                                "bootstrap: contract already in checkpoint — skipping"
                            );
                            continue;
                        }
                        let found = enumerator.enumerate_one_contract(*contract).await?;
                        tracing::info!(
                            contract = %contract_hex,
                            found = found.len(),
                            "bootstrap: contract enumerated"
                        );
                        state.wallets.extend(found.iter().map(|w| w.to_string()));
                        state.completed_contracts.push(contract_hex);
                        wallet_set::save_state(&config.wallet_set_path, &state)?;
                    }
                }
            }
        }

        // Deduplicate (per-contract sets may overlap at wallet level) and parse.
        let mut seen = std::collections::HashSet::new();
        state
            .wallets
            .iter()
            .filter(|h| seen.insert(h.as_str()))
            .filter_map(|h| {
                WalletAddress::from_hex(h)
                    .map_err(|e| {
                        tracing::warn!(address = %h, error = %e, "bootstrap: skipping unparseable wallet");
                    })
                    .ok()
            })
            .collect()
    };

    // 2. Fetch trade history — permanent cache (SQLite, WAL), incremental per run.
    //    On first run fetches full history. On subsequent runs fetches only trades
    //    newer than the newest cached id per wallet.
    let mut cache = WalletCache::open(&config.cache_path)?;
    // Short pool_idle_timeout avoids reusing connections the server has silently
    // closed (Polymarket servers enforce per-IP connection limits under load).
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    let fetcher = PolymarketBulkFetcher::new(
        config.polymarket_base_url.clone(),
        ReqwestFetcher::new(client),
    )
    .with_concurrency(config.polymarket_concurrency);
    fetcher.fetch_all(&wallets, &mut cache).await?;
    tracing::info!(wallets = wallets.len(), "bootstrap: trade fetch complete");

    // 3. Reconstruct ledgers — per-wallet streaming from SQLite to keep peak
    //    memory bounded. Each wallet's trades are loaded, reconstructed, and
    //    dropped before the next wallet is processed.
    // audit_window_days=None → unlimited (u32::MAX sentinel passed downstream).
    // Reserved for future per-snapshot windowing; currently unused by build_trader_ledgers.
    let snapshot_at = SourceTimestamp(OffsetDateTime::now_utc());
    let audit_window_days = config.audit_window_days.unwrap_or(u32::MAX);
    let empty_operators: &[OperatorIdentity] = &[];
    let ledger_config = LedgerConfig::default();
    let mut ledgers: Vec<TraderLedger> = Vec::with_capacity(wallets.len());
    let mut total_trades: usize = 0;
    for wallet in &wallets {
        let trades = cache.trades_for(&wallet.to_string());
        if trades.is_empty() {
            continue;
        }
        total_trades += trades.len();
        let snapshot = TradeSnapshot {
            trades,
            snapshot_at: snapshot_at.clone(),
            audit_window_days,
        };
        ledgers.extend(build_trader_ledgers(
            &snapshot,
            empty_operators,
            &ledger_config,
        ));
    }
    tracing::info!(
        ledgers = ledgers.len(),
        trades = total_trades,
        "bootstrap: reconstructed ledgers"
    );

    // 4. Pre-filter and 5. Build seed watchlist.
    let watchlist = build_seed_watchlist(
        ledgers,
        snapshot_at,
        config.min_closed_trades,
        config.min_win_rate_pct,
    );
    tracing::info!(
        active = watchlist.active_count,
        incubator = watchlist.incubator_count,
        "bootstrap: watchlist built"
    );

    // Write output.
    write_watchlist(&watchlist, &config.output_path)?;

    Ok(watchlist)
}

/// Fetch the current Polygon chain head block number from Etherscan.
async fn fetch_current_block(api_key: &str) -> Result<u64, BootstrapError> {
    let url = format!(
        "{DEFAULT_ETHERSCAN_BASE_URL}?chainid=137&module=proxy&action=eth_blockNumber&apikey={api_key}"
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(ETH_BLOCK_TIMEOUT_SECS))
        .build()
        .map_err(|e| BootstrapError::Etherscan {
            message: format!("build client: {e}"),
        })?;
    let bytes = client
        .get(url)
        .send()
        .await
        .map_err(|e| BootstrapError::Etherscan {
            message: format!("eth_blockNumber GET: {e}"),
        })?
        .bytes()
        .await
        .map_err(|e| BootstrapError::Etherscan {
            message: format!("eth_blockNumber read body: {e}"),
        })?;

    #[derive(serde::Deserialize)]
    struct Resp {
        result: String,
    }
    let parsed: Resp = serde_json::from_slice(&bytes).map_err(|e| BootstrapError::Etherscan {
        message: format!("eth_blockNumber parse: {e}"),
    })?;
    u64::from_str_radix(parsed.result.trim_start_matches("0x"), 16).map_err(|e| {
        BootstrapError::Etherscan {
            message: format!(
                "eth_blockNumber hex parse '{result}': {e}",
                result = parsed.result
            ),
        }
    })
}

/// Build a seed [`Watchlist`] from reconstructed ledgers using the bootstrap pre-filter.
///
/// Uses win-rate basis points as the score (no historical LCB_5pct available at bootstrap).
/// All passing wallets are assigned `Active` tier; `operator_id` is always `None` since
/// operator attribution requires `source-onchain-polygon` data not available here.
pub fn build_seed_watchlist(
    ledgers: Vec<TraderLedger>,
    snapshot_at: SourceTimestamp,
    min_closed_trades: usize,
    min_win_rate_pct: u8,
) -> Watchlist {
    let mut entries: Vec<WatchlistEntry> = Vec::new();

    for ledger in &ledgers {
        if !passes_filter(ledger, min_closed_trades, min_win_rate_pct) {
            continue;
        }

        let total = ledger.closed_trades.len();
        let wins = ledger
            .closed_trades
            .iter()
            .filter(|t| t.realized_pnl_usd > Decimal::ZERO)
            .count();

        let score = BasisPoints(win_rate_bps(wins, total));
        entries.push(WatchlistEntry {
            wallet: ledger.wallet,
            operator_id: None,
            tier: WatchlistTier::Active,
            leader_score_bps: score,
            lcb_5pct_bps: BasisPoints(0),
            closed_trades_in_window: u32::try_from(total).unwrap_or(u32::MAX),
            reconstruction_quality: ledger.reconstruction_quality,
        });
    }

    // Sort descending by score.
    entries.sort_by_key(|e| std::cmp::Reverse(e.leader_score_bps.0));

    let active_count = entries.len();
    Watchlist {
        entries,
        snapshot_at,
        active_count,
        incubator_count: 0,
    }
}

fn write_watchlist(watchlist: &Watchlist, path: &Path) -> Result<(), BootstrapError> {
    let json = serde_json::to_vec_pretty(watchlist)?;
    // Atomic write: tmp → rename.
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}
