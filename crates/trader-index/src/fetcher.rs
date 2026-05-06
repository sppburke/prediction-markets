//! [`WatchlistFetcher`] — async leaderboard → [`Watchlist`] bootstrapper.
//!
//! Queries the Polymarket public leaderboard endpoint (via a [`PageFetcher`])
//! and converts the response into a fully-formed [`Watchlist`] ready for
//! `pe-copy-signal-engine`.
//!
//! This is the only module in `pe-trader-index` that performs I/O. All other
//! modules remain pure and synchronous.

use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use serde::Deserialize;
use thiserror::Error;

use crate::watchlist::{Watchlist, WatchlistEntry, WatchlistTier};

// Canonical defaults live in `docs/_GLOSSARY.md` "Watchlist auto-fetcher" section.
const DEFAULT_WATCHLIST_SIZE: usize = 20;
const DEFAULT_BASE_URL: &str = "https://data-api.polymarket.com";

/// Configuration for the leaderboard auto-fetch.
#[derive(Debug, Clone)]
pub struct WatchlistFetchConfig {
    /// Base URL for the Polymarket Data API (no trailing slash).
    pub base_url: String,
    /// Maximum number of leaderboard entries to include in the watchlist.
    /// Default: `watchlist_size = 20` (see `docs/_GLOSSARY.md`).
    pub watchlist_size: usize,
}

impl Default for WatchlistFetchConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_owned(),
            watchlist_size: DEFAULT_WATCHLIST_SIZE,
        }
    }
}

/// Errors returned by [`WatchlistFetcher::fetch_watchlist`].
#[derive(Debug, Error)]
pub enum WatchlistFetchError {
    #[error("network error fetching leaderboard: {message}")]
    Network { message: String },
    #[error("failed to parse leaderboard response: {message}")]
    Parse { message: String },
    /// Should not occur in practice; indicates a coding invariant was broken.
    #[error("internal error")]
    Internal,
}

// ── Internal JSON DTOs ────────────────────────────────────────────────────────

// v1 API returns a JSON array directly (no wrapper object).
type LeaderboardResponse = Vec<LeaderboardEntry>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeaderboardEntry {
    proxy_wallet: String,
}

// ── Fetcher ───────────────────────────────────────────────────────────────────

/// Fetches the Polymarket leaderboard and returns a top-N [`Watchlist`].
///
/// Scoring is rank-inverted: position 1 receives the highest score
/// (`watchlist_size × 100` basis points), decreasing by 100 bps per rank.
/// No historical distribution data is available at fetch time; `lcb_5pct_bps`
/// is set equal to `leader_score_bps` as a placeholder.
pub struct WatchlistFetcher<F: PageFetcher> {
    config: WatchlistFetchConfig,
    fetcher: F,
}

impl<F: PageFetcher> WatchlistFetcher<F> {
    /// Create a new fetcher with the given config and page fetcher.
    pub fn new(config: WatchlistFetchConfig, fetcher: F) -> Self {
        Self { config, fetcher }
    }

    /// Fetch the leaderboard and return a fully-formed [`Watchlist`].
    ///
    /// Returns [`WatchlistFetchError::Network`] on transport failures,
    /// [`WatchlistFetchError::Parse`] on malformed JSON or invalid addresses.
    pub async fn fetch_watchlist(&mut self) -> Result<Watchlist, WatchlistFetchError> {
        let url = PolymarketEndpoint::Leaderboard.url(&self.config.base_url);
        let bytes =
            self.fetcher
                .fetch_page(&url)
                .await
                .map_err(|e| WatchlistFetchError::Network {
                    message: e.to_string(),
                })?;

        let response: LeaderboardResponse =
            serde_json::from_slice(&bytes).map_err(|e| WatchlistFetchError::Parse {
                message: e.to_string(),
            })?;

        let n = response.len().min(self.config.watchlist_size);
        let quality = ReconstructionQuality::new(100).map_err(|_| WatchlistFetchError::Internal)?;

        let mut entries = Vec::with_capacity(n);
        for (idx, entry) in response.iter().take(n).enumerate() {
            let wallet = WalletAddress::from_hex(&entry.proxy_wallet).map_err(|e| {
                WatchlistFetchError::Parse {
                    message: format!("invalid proxyWallet: {e}"),
                }
            })?;
            // Rank-inverted basis-point score: rank 1 → n×100, rank n → 100.
            // saturating_mul prevents usize overflow; try_into caps at i32::MAX for
            // pathological watchlist_size values (safe at the default of 20).
            let score_value = (n - idx).saturating_mul(100).try_into().unwrap_or(i32::MAX);
            let rank_score = BasisPoints(score_value);
            entries.push(WatchlistEntry {
                wallet,
                operator_id: None,
                tier: WatchlistTier::Active,
                leader_score_bps: rank_score,
                lcb_5pct_bps: rank_score,
                // No trade data at fetch time — honest zero sentinels.
                // The service layer merges this with ranker output before
                // passing to copy-signal-engine.
                win_rate_bps: BasisPoints(0),
                closed_trades_in_window: 0,
                reconstruction_quality: quality,
            });
        }

        let active_count = entries.len();
        Ok(Watchlist {
            entries,
            snapshot_at: SourceTimestamp(time::OffsetDateTime::now_utc()),
            active_count,
            incubator_count: 0,
        })
    }
}
