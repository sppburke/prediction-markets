//! `pe-bootstrap` — wallet discovery and seed watchlist generation.
//!
//! Pipeline:
//! 1. Discover wallets via Dune Analytics or Etherscan (selected by `PE_WALLET_SOURCE`).
//! 2. Fetch trade history per wallet from the Polymarket Data API (7-day JSON cache).
//! 3. Reconstruct `TraderLedger`s via `pe-trader-index`.
//! 4. Pre-filter: keep wallets with > 10 closed trades and > 80 % win rate.
//! 5. Build a seed `Watchlist` and write it to `output_path`.
//!
//! Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.

pub mod cache;
pub mod dune;
pub mod error;
pub mod filter;
pub mod polymarket;

use std::path::{Path, PathBuf};

use pe_core_types::{BasisPoints, SourceTimestamp, WalletAddress};
use pe_operator_graph::OperatorIdentity;
use pe_source_onchain_polygon::{
    EnumerationConfig, PolymarketTraderEnumeration, contracts::CTF_EXCHANGE_V1_DEPLOY_BLOCK,
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
const DEFAULT_DUNE_WALLET_LIMIT: u32 = 10_000;
const DEFAULT_AUDIT_WINDOW_DAYS: u32 = 90;
const DEFAULT_POLYMARKET_BASE_URL: &str = "https://data-api.polymarket.com";
const DEFAULT_ETHERSCAN_BASE_URL: &str = "https://api.etherscan.io/v2/api";

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
            _ => Self::Etherscan,
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
    pub wallet_from_block: u64,
    /// `PE_WALLET_TO_BLOCK` — end block for Etherscan scan (default: current chain head).
    pub wallet_to_block: Option<u64>,
    /// `PE_BOOTSTRAP_OUTPUT` — path where the `Watchlist` JSON is written.
    pub output_path: PathBuf,
    /// `PE_BOOTSTRAP_CACHE_PATH` — path to the wallet trade cache JSON file.
    pub cache_path: PathBuf,
    /// Maximum distinct wallets to pull from Dune (default `bootstrap_dune_wallet_limit = 10000`).
    pub dune_wallet_limit: u32,
    /// Trade lookback window for ledger reconstruction (default `bootstrap_polymarket_audit_window_days = 90`).
    pub audit_window_days: u32,
    /// Minimum closed trades to pass the pre-filter (default `bootstrap_min_closed_trades = 10`).
    pub min_closed_trades: usize,
    /// Minimum win-rate percent to pass the pre-filter (default `bootstrap_min_win_rate_pct = 80`).
    pub min_win_rate_pct: u8,
    /// Base URL for the Polymarket Data API.
    pub polymarket_base_url: String,
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

        let wallet_source = WalletSource::from_str(&optional("PE_WALLET_SOURCE", "etherscan"));

        let (dune_api_key, etherscan_api_key) = match &wallet_source {
            WalletSource::Dune => (Some(require("PE_DUNE_API_KEY")?), None),
            WalletSource::Etherscan => (None, Some(require("PE_ETHERSCAN_API_KEY")?)),
        };

        Ok(Self {
            wallet_source,
            dune_api_key,
            etherscan_api_key,
            wallet_from_block: optional_parse("PE_WALLET_FROM_BLOCK", CTF_EXCHANGE_V1_DEPLOY_BLOCK),
            wallet_to_block: std::env::var("PE_WALLET_TO_BLOCK")
                .ok()
                .and_then(|v| v.parse().ok()),
            output_path: PathBuf::from(require("PE_BOOTSTRAP_OUTPUT")?),
            cache_path: PathBuf::from(optional("PE_BOOTSTRAP_CACHE_PATH", "wallet_cache.json")),
            dune_wallet_limit: optional_parse("PE_BOOTSTRAP_DUNE_LIMIT", DEFAULT_DUNE_WALLET_LIMIT),
            audit_window_days: optional_parse(
                "PE_BOOTSTRAP_AUDIT_WINDOW_DAYS",
                DEFAULT_AUDIT_WINDOW_DAYS,
            ),
            min_closed_trades: optional_parse(
                "PE_BOOTSTRAP_MIN_CLOSED_TRADES",
                DEFAULT_MIN_CLOSED_TRADES,
            ),
            min_win_rate_pct: optional_parse(
                "PE_BOOTSTRAP_MIN_WIN_RATE_PCT",
                DEFAULT_MIN_WIN_RATE_PCT,
            ),
            polymarket_base_url: optional("PE_POLYMARKET_BASE_URL", DEFAULT_POLYMARKET_BASE_URL),
        })
    }
}

/// Run the full bootstrap pipeline and return the seed [`Watchlist`].
///
/// Writes the watchlist as pretty-printed JSON to `config.output_path`.
pub async fn run(config: &BootstrapConfig) -> Result<Watchlist, BootstrapError> {
    // 1. Discover wallets.
    let wallets: Vec<WalletAddress> = match &config.wallet_source {
        WalletSource::Dune => {
            let api_key = config
                .dune_api_key
                .clone()
                .ok_or(BootstrapError::Internal)?;
            tracing::info!(
                limit = config.dune_wallet_limit,
                "bootstrap: querying dune for wallets"
            );
            let dune = DuneClient::new(api_key);
            let wallets = dune.discover_wallets(config.dune_wallet_limit).await?;
            tracing::info!(count = wallets.len(), "bootstrap: dune returned wallets");
            wallets
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
                "bootstrap: enumerating wallets via etherscan"
            );
            let enum_config = EnumerationConfig {
                from_block: config.wallet_from_block,
                to_block,
                operator_addresses: Vec::new(),
            };
            let enumerator = PolymarketTraderEnumeration::new(api_key, enum_config);
            let wallet_set = enumerator.enumerate().await?;
            let wallets: Vec<WalletAddress> = wallet_set.into_iter().collect();
            tracing::info!(
                count = wallets.len(),
                "bootstrap: etherscan enumeration complete"
            );
            wallets
        }
    };

    // 2. Fetch trade history (cache-first).
    let mut cache = WalletCache::open(&config.cache_path)?;
    let client = reqwest::Client::new();
    let mut fetcher = PolymarketBulkFetcher::new(
        config.polymarket_base_url.clone(),
        ReqwestFetcher::new(client),
    );
    let all_trades = fetcher.fetch_all(&wallets, &mut cache).await;
    cache.save()?;
    tracing::info!(count = all_trades.len(), "bootstrap: fetched trades");

    // 3. Reconstruct ledgers.
    let snapshot_at = SourceTimestamp(OffsetDateTime::now_utc());
    let snapshot = TradeSnapshot {
        trades: all_trades,
        snapshot_at: snapshot_at.clone(),
        audit_window_days: config.audit_window_days,
    };
    let empty_operators: &[OperatorIdentity] = &[];
    let ledgers: Vec<TraderLedger> =
        build_trader_ledgers(&snapshot, empty_operators, &LedgerConfig::default());
    tracing::info!(count = ledgers.len(), "bootstrap: reconstructed ledgers");

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
    let bytes = reqwest::get(url)
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
            message: format!("eth_blockNumber hex parse: {e}"),
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
