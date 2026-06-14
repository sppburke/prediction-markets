//! Polymarket leaderboard wallet discovery (issue #324; all-category in #335).
//!
//! Sweeps the `category × sort × window` matrix of `/v1/leaderboard`, deduplicates
//! `proxyWallet` addresses, upserts them with `SRC_LEADERBOARD` bits, and applies
//! activation rules. Stateless-idempotent: no source cursor is written.
//!
//! A category the API rejects (HTTP 4xx — e.g. a future renamed category) is
//! skipped with a `warn!`; one bad category never aborts the rest of the sweep.

use pe_source_polymarket_public::endpoint::{
    LeaderboardCategory, LeaderboardSort, LeaderboardWindow, PolymarketEndpoint,
};
use pe_source_polymarket_public::fetcher::PageFetcher;
use serde::Deserialize;

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::error::BootstrapError;
use crate::pile::{self, SRC_LEADERBOARD};

/// The 8 `(sort, window)` combinations swept per category:
/// {PNL, VOL} × {DAY, WEEK, MONTH, ALL}.
pub const SORT_WINDOW_SLICES: [(LeaderboardSort, LeaderboardWindow); 8] = [
    (LeaderboardSort::Profit, LeaderboardWindow::Day),
    (LeaderboardSort::Profit, LeaderboardWindow::Week),
    (LeaderboardSort::Profit, LeaderboardWindow::Monthly),
    (LeaderboardSort::Profit, LeaderboardWindow::AllTime),
    (LeaderboardSort::Volume, LeaderboardWindow::Day),
    (LeaderboardSort::Volume, LeaderboardWindow::Week),
    (LeaderboardSort::Volume, LeaderboardWindow::Monthly),
    (LeaderboardSort::Volume, LeaderboardWindow::AllTime),
];

/// Per-run discovery counts.
#[derive(Debug, Default, Clone, Copy)]
pub struct LeaderboardDiscoveryReport {
    pub slices_attempted: usize,
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
        category: LeaderboardCategory,
        limit: u32,
    ) -> Result<Vec<LeaderboardEntry>, BootstrapError> {
        let ep = PolymarketEndpoint::Leaderboard {
            sort,
            window,
            category,
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

/// Sweep `categories × SORT_WINDOW_SLICES`, deduplicate addresses, upsert with
/// `SRC_LEADERBOARD`, and apply activation rules.
///
/// Per-slice failures (including an HTTP 4xx from an unsupported category) are
/// logged and skipped — a single bad category never aborts the sweep. Returns
/// `Err(Leaderboard { .. })` only if *every* attempted slice yields zero valid
/// addresses, guarding against a silent empty ingest.
pub async fn run_leaderboard_discovery<F: PageFetcher + Send + Sync>(
    fetcher: &LeaderboardFetcher<F>,
    categories: &[LeaderboardCategory],
    top_n: u32,
    cache: &mut WalletCache,
) -> Result<LeaderboardDiscoveryReport, BootstrapError> {
    let mut all: Vec<String> = Vec::new();
    let mut slices_attempted = 0usize;
    let mut slices_fetched = 0usize;

    for &category in categories {
        for (sort, window) in SORT_WINDOW_SLICES {
            slices_attempted += 1;
            match fetcher.fetch_slice(sort, window, category, top_n).await {
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
                    tracing::warn!(
                        error = %e,
                        category = category.as_param(),
                        "leaderboard: slice failed (e.g. 4xx for an unsupported category), continuing"
                    );
                }
            }
        }
    }

    let raw_entries = all.len();
    if raw_entries == 0 {
        return Err(BootstrapError::Leaderboard {
            message: format!(
                "all {slices_attempted} slices returned zero valid wallet addresses — refusing empty upsert"
            ),
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
        slices_attempted,
        slices_fetched,
        raw_entries,
        unique_wallets,
        activated,
        "leaderboard_discovery: complete"
    );
    Ok(LeaderboardDiscoveryReport {
        slices_attempted,
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

    const BASE: &str = "https://data-api.polymarket.com";

    fn open_temp_cache(dir: &TempDir) -> WalletCache {
        WalletCache::open(&dir.path().join("test.db")).unwrap()
    }

    /// Build a fixture map: every `(category × slice)` URL → the same JSON body.
    fn fixtures_for(
        categories: &[LeaderboardCategory],
        wallets: &[&str],
        top_n: u32,
    ) -> HashMap<String, Vec<u8>> {
        let body: Vec<serde_json::Value> = wallets
            .iter()
            .map(|w| serde_json::json!({ "proxyWallet": w }))
            .collect();
        let bytes = serde_json::to_vec(&body).unwrap();
        let mut map = HashMap::new();
        for &category in categories {
            for (sort, window) in SORT_WINDOW_SLICES {
                let ep = PolymarketEndpoint::Leaderboard {
                    sort,
                    window,
                    category,
                    limit: top_n,
                };
                map.insert(ep.url(BASE), bytes.clone());
            }
        }
        map
    }

    /// AC3 — idempotent + cross-matrix dedup: a wallet present in every
    /// `(category × slice)` slice is counted exactly once, across reruns.
    #[tokio::test]
    async fn idempotent_double_run_dedups_across_matrix() {
        let dir = TempDir::new().unwrap();
        let mut cache = open_temp_cache(&dir);
        let categories = [LeaderboardCategory::Overall, LeaderboardCategory::Crypto];
        let wallets = [
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ];
        let responses = fixtures_for(&categories, &wallets, 50);

        let lb = LeaderboardFetcher::new(BASE.to_owned(), FixtureFetcher::new(responses.clone()));
        let r1 = run_leaderboard_discovery(&lb, &categories, 50, &mut cache)
            .await
            .unwrap();
        assert_eq!(
            r1.slices_attempted,
            categories.len() * SORT_WINDOW_SLICES.len()
        );
        assert_eq!(r1.slices_fetched, r1.slices_attempted);
        assert_eq!(r1.unique_wallets, 2, "deduped across the whole matrix");

        let lb2 = LeaderboardFetcher::new(BASE.to_owned(), FixtureFetcher::new(responses));
        let r2 = run_leaderboard_discovery(&lb2, &categories, 50, &mut cache)
            .await
            .unwrap();
        assert_eq!(r2.unique_wallets, 2, "idempotent across reruns");
    }

    /// AC1 — zero-result guard: all-empty slices return Err, never a silent upsert.
    #[tokio::test]
    async fn zero_wallet_guard() {
        let dir = TempDir::new().unwrap();
        let mut cache = open_temp_cache(&dir);
        let categories = [LeaderboardCategory::Overall];
        let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
        for (sort, window) in SORT_WINDOW_SLICES {
            let ep = PolymarketEndpoint::Leaderboard {
                sort,
                window,
                category: LeaderboardCategory::Overall,
                limit: 50,
            };
            responses.insert(ep.url(BASE), b"[]".to_vec());
        }
        let lb = LeaderboardFetcher::new(BASE.to_owned(), FixtureFetcher::new(responses));
        let result = run_leaderboard_discovery(&lb, &categories, 50, &mut cache).await;
        assert!(
            matches!(result, Err(BootstrapError::Leaderboard { .. })),
            "expected Leaderboard error for all-empty slices, got: {result:?}"
        );
    }

    /// A category the API rejects (here: a missing fixture → fetch error) is
    /// skipped; the sweep still succeeds from the categories that returned wallets.
    #[tokio::test]
    async fn unsupported_category_is_skipped() {
        let dir = TempDir::new().unwrap();
        let mut cache = open_temp_cache(&dir);
        // Only OVERALL has fixtures; CRYPTO slices error (no fixture registered).
        let responses = fixtures_for(
            &[LeaderboardCategory::Overall],
            &["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
            50,
        );
        let categories = [LeaderboardCategory::Overall, LeaderboardCategory::Crypto];
        let lb = LeaderboardFetcher::new(BASE.to_owned(), FixtureFetcher::new(responses));
        let r = run_leaderboard_discovery(&lb, &categories, 50, &mut cache)
            .await
            .unwrap();
        assert_eq!(r.slices_attempted, 16, "2 categories × 8 slices attempted");
        assert_eq!(r.slices_fetched, 8, "only OVERALL's 8 slices succeeded");
        assert_eq!(r.unique_wallets, 1);
    }

    /// AC2 — URL contract: the leaderboard endpoint builds the documented
    /// `orderBy`/`timePeriod`/`category` query string with the capped `limit`.
    #[test]
    fn url_contract() {
        let ep = PolymarketEndpoint::Leaderboard {
            sort: LeaderboardSort::Profit,
            window: LeaderboardWindow::Monthly,
            category: LeaderboardCategory::Overall,
            limit: 50,
        };
        assert_eq!(
            ep.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/v1/leaderboard?orderBy=PNL&timePeriod=MONTH&category=OVERALL&limit=50"
        );
        let ep2 = PolymarketEndpoint::Leaderboard {
            sort: LeaderboardSort::Volume,
            window: LeaderboardWindow::Day,
            category: LeaderboardCategory::Crypto,
            limit: 50,
        };
        assert_eq!(
            ep2.url("https://data-api.polymarket.com"),
            "https://data-api.polymarket.com/v1/leaderboard?orderBy=VOL&timePeriod=DAY&category=CRYPTO&limit=50"
        );
    }
}
