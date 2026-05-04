//! `pe-bootstrap` — wallet discovery and seed watchlist generation.
//!
//! Pipeline:
//! 1. Query Dune Analytics for all wallets that have ever traded on Polymarket.
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

/// Configuration for a bootstrap run, sourced from environment variables.
pub struct BootstrapConfig {
    /// `PE_DUNE_API_KEY` — required.
    pub dune_api_key: String,
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

        Ok(Self {
            dune_api_key: require("PE_DUNE_API_KEY")?,
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
    // 1. Discover wallets via Dune.
    tracing::info!(
        limit = config.dune_wallet_limit,
        "bootstrap: querying dune for wallets"
    );
    let dune = DuneClient::new(config.dune_api_key.clone());
    let wallets: Vec<WalletAddress> = dune.discover_wallets(config.dune_wallet_limit).await?;
    tracing::info!(count = wallets.len(), "bootstrap: dune returned wallets");

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
