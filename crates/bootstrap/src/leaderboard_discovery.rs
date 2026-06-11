//! Polymarket leaderboard wallet discovery (issue #324).
//!
//! Fetches all 4 `(sort × window)` slices from `/v1/leaderboard`, deduplicates
//! `proxyWallet` addresses, upserts them with `SRC_LEADERBOARD` bits, and applies
//! activation rules. Stateless-idempotent: no source cursor is written.

use pe_source_polymarket_public::endpoint::{
    LeaderboardSort, LeaderboardWindow, PolymarketEndpoint,
};
use pe_source_polymarket_public::fetcher::PageFetcher;
use serde::Deserialize;

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::error::BootstrapError;
use crate::pile::{self, SRC_LEADERBOARD};

/// The 4 canonical `(sort, window)` combinations for full leaderboard coverage.
pub const ALL_SLICES: [(LeaderboardSort, LeaderboardWindow); 4] = [
    (LeaderboardSort::Profit, LeaderboardWindow::Monthly),
    (LeaderboardSort::Profit, LeaderboardWindow::AllTime),
    (LeaderboardSort::Volume, LeaderboardWindow::Monthly),
    (LeaderboardSort::Volume, LeaderboardWindow::AllTime),
];

/// Per-run discovery counts.
#[derive(Debug, Default, Clone, Copy)]
pub struct LeaderboardDiscoveryReport {
    pub slices_fetched: usize,
    pub raw_entries: usize,
    pub unique_wallets: usize,
    pub activated: usize,
}

/// HTTP fetcher scoped to the Polymarket leaderboard.
pub struct LeaderboardFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher + Send + Sync> LeaderboardFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    async fn fetch_slice(
        &self,
        sort: LeaderboardSort,
        window: LeaderboardWindow,
        limit: u32,
    ) -> Result<Vec<LeaderboardEntry>, BootstrapError> {
        let ep = PolymarketEndpoint::Leaderboard {
            sort,
            window,
            limit,
        };
        let url = ep.url(&self.base_url);
        let bytes =
            self.fetcher
                .fetch_page(&url)
                .await
                .map_err(|e| BootstrapError::Leaderboard {
                    message: format!("HTTP: {e}"),
                })?;
        serde_json::from_slice(&bytes).map_err(|e| BootstrapError::Leaderboard {
            message: format!("JSON: {e}"),
        })
    }
}

/// Leaderboard entry — only `proxyWallet` is required for upsert.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeaderboardEntry {
    proxy_wallet: String,
}

/// Fetch all 4 slices, deduplicate addresses, upsert, and apply activation rules.
///
/// Returns `Err(Leaderboard { .. })` if every slice returns zero valid addresses —
/// guarding against a silent empty ingest.
pub async fn run_leaderboard_discovery<F: PageFetcher + Send + Sync>(
    fetcher: &LeaderboardFetcher<F>,
    top_n: u32,
    cache: &mut WalletCache,
) -> Result<LeaderboardDiscoveryReport, BootstrapError> {
    let mut all: Vec<String> = Vec::new();
    let mut slices_fetched = 0usize;

    for (sort, window) in ALL_SLICES {
        match fetcher.fetch_slice(sort, window, top_n).await {
            Ok(entries) => {
                slices_fetched += 1;
                for e in entries {
                    let hex = e.proxy_wallet.to_ascii_lowercase();
                    if hex.starts_with("0x") && hex.len() == 42 {
                        all.push(hex);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "leaderboard: slice failed, continuing");
            }
        }
    }

    let raw_entries = all.len();
    if raw_entries == 0 {
        return Err(BootstrapError::Leaderboard {
            message: "all 4 slices returned zero valid wallet addresses — refusing empty upsert"
                .to_owned(),
        });
    }

    all.sort_unstable();
    all.dedup();
    let unique_wallets = all.len();

    let upserts: Vec<WalletUpsertRow> = all
        .into_iter()
        .map(|hex| (hex, SRC_LEADERBOARD, false, None, None, None, 0))
        .collect();
    cache.upsert_wallets_bulk(&upserts)?;

    let activated = pile::apply_activation_rules(cache)?;

    tracing::info!(
        slices_fetched,
        raw_entries,
        unique_wallets,
        activated,
        "leaderboard_discovery: complete"
    );
    Ok(LeaderboardDiscoveryReport {
        slices_fetched,
        raw_entries,
        unique_wallets,
        activated,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use pe_source_polymarket_public::FixtureFetcher;
    use tempfile::TempDir;

    use super::*;
    use crate::cache::WalletCache;

    fn open_temp_cache(dir: &TempDir) -> WalletCache {
        WalletCache::open(&dir.path().join("test.db")).unwrap()
    }

    /// AC3 — idempotent: running discovery twice with the same wallet set must not
    /// double-count or error. `unique_wallets` equals the distinct address count.
    #[tokio::test]
    async fn idempotent_double_run() {
        let dir = TempDir::new().unwrap();
        let mut cache = open_temp_cache(&dir);

        let wallets = [
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ];
        let fixture_slices: Vec<_> = ALL_SLICES
            .iter()
            .map(|&(s, w)| {
                let ep = PolymarketEndpoint::Leaderboard {
                    sort: s,
                    window: w,
                    limit: 500,
                };
                let url = ep.url("https://data-api.polymarket.com");
                let body: Vec<serde_json::Value> = wallets
                    .iter()
                    .map(|w| serde_json::json!({ "proxyWallet": w }))
                    .collect();
                (url, serde_json::to_vec(&body).unwrap())
            })
            .collect();
        let responses: HashMap<String, Vec<u8>> = fixture_slices.into_iter().collect();
        let fetcher_impl = FixtureFetcher::new(responses.clone());
        let lb =
            LeaderboardFetcher::new("https://data-api.polymarket.com".to_owned(), fetcher_impl);

        let r1 = run_leaderboard_discovery(&lb, 500, &mut cache)
            .await
            .unwrap();
        assert_eq!(
            r1.unique_wallets, 2,
            "first run must upsert 2 unique wallets"
        );

        let fetcher_impl2 = FixtureFetcher::new(responses);
        let lb2 =
            LeaderboardFetcher::new("https://data-api.polymarket.com".to_owned(), fetcher_impl2);
        let r2 = run_leaderboard_discovery(&lb2, 500, &mut cache)
            .await
            .unwrap();
        assert_eq!(
            r2.unique_wallets, 2,
            "second run must still report 2 unique wallets (idempotent upsert)"
        );
    }

    /// AC1 — zero-result guard: all-empty slices must return Err, not silently upsert nothing.
    #[tokio::test]
    async fn zero_wallet_guard() {
        let dir = TempDir::new().unwrap();
        let mut cache = open_temp_cache(&dir);

        let responses: HashMap<String, Vec<u8>> = ALL_SLICES
            .iter()
            .map(|&(s, w)| {
                let ep = PolymarketEndpoint::Leaderboard {
                    sort: s,
                    window: w,
                    limit: 500,
                };
                (ep.url("https://data-api.polymarket.com"), b"[]".to_vec())
            })
            .collect();
        let fetcher_impl = FixtureFetcher::new(responses);
        let lb =
            LeaderboardFetcher::new("https://data-api.polymarket.com".to_owned(), fetcher_impl);

        let result = run_leaderboard_discovery(&lb, 500, &mut cache).await;
        assert!(
            matches!(result, Err(BootstrapError::Leaderboard { .. })),
            "expected Leaderboard error for all-empty slices, got: {result:?}"
        );
    }

    /// AC2 — URL contract: leaderboard endpoint builds the correct query string.
    #[test]
    fn url_contract() {
        let ep = PolymarketEndpoint::Leaderboard {
            sort: LeaderboardSort::Profit,
            window: LeaderboardWindow::Monthly,
            limit: 500,
        };
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/v1/leaderboard?sort=profit&window=monthly&limit=500"
        );
        let ep2 = PolymarketEndpoint::Leaderboard {
            sort: LeaderboardSort::Volume,
            window: LeaderboardWindow::AllTime,
            limit: 100,
        };
        assert_eq!(
            ep2.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/v1/leaderboard?sort=volume&window=allTime&limit=100"
        );
    }
}
