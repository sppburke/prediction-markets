//! Startup per-wallet market-history backfill for the copy-entry gate.
//!
//! [`WalletHistoryLoader::load`] fetches each watchlisted wallet's complete set
//! of previously-entered markets from the free Polymarket Data API
//! (`/activity?type=TRADE`, cursor-paginated to completeness), unions it with a
//! stale JSON sidecar (so the gate works immediately and survives an API outage),
//! persists the merged map atomically, and warns for every wallet it could not
//! populate. The returned map seeds [`crate::entry_gate::CopyEntryGate`].
//!
//! Lives in `crates/service` (not the pure, no-I/O `copy-signal-engine`) because
//! it performs network and filesystem I/O.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use pe_core_types::{MarketId, VenueMarketId, WalletAddress};
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Safety backstop: stop paginating after this many pages per wallet. At 500
/// trades/page that is 100k trades; a wallet exceeding it gets partial history
/// (and a loud warn) — a market entered before the cap could be missed, causing
/// a false "first entry".
const HISTORY_MAX_PAGES: u32 = 200;

/// Page size hardcoded by [`PolymarketEndpoint::UserTradeActivity`] (`limit=500`).
/// A page shorter than this signals the wallet's trade history is exhausted.
const HISTORY_PAGE_SIZE: usize = 500;

/// Consecutive fully-cached pages required before the incremental walk stops.
/// `/activity` is paginated newest-first by timestamp but only ~1 s-reliably
/// ordered (see `trade_poller`), so a 2-page (1000-trade) margin absorbs boundary
/// jitter: a genuinely-new market would have to land >1000 trades past its own
/// timestamp to be skipped, far beyond the observed ordering noise.
const KNOWN_PAGE_MARGIN: u32 = 2;

// ── Sidecar ─────────────────────────────────────────────────────────────────

/// On-disk JSON sidecar: each wallet's previously-entered markets.
#[derive(Debug, Default, Serialize, Deserialize)]
struct WalletHistorySidecar {
    wallets: Vec<WalletHistoryEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WalletHistoryEntry {
    wallet: WalletAddress,
    markets: Vec<MarketId>,
}

// ── Fetch DTO ───────────────────────────────────────────────────────────────

/// Minimal projection of a `/activity?type=TRADE` item: the market entered and
/// the trade timestamp used as the pagination cursor.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryItem {
    condition_id: String,
    timestamp: i64,
}

// ── Fetch ─────────────────────────────────────────────────────────────────────

/// Fetch the markets `wallet` has traded that are not already in `known`,
/// paginating the trade-activity endpoint backwards by timestamp.
///
/// Incremental: the walk stops after [`KNOWN_PAGE_MARGIN`] consecutive full pages
/// that contain no market outside `known`. `/activity` is paginated newest-first by
/// timestamp, so a run of fully-known pages means older history is already cached;
/// the multi-page margin guards against the endpoint's ~1 s ordering jitter dropping
/// a market that straddles the boundary. The caller unions the result with the
/// sidecar, so under that ordering assumption the merged set stays complete; a wallet
/// with no cached history (`known` empty) is walked in full, and any fetch failure
/// falls back to the complete stale sidecar.
async fn fetch_history_for_wallet<F: PageFetcher>(
    wallet: WalletAddress,
    base_url: &str,
    fetcher: &F,
    known: &HashSet<MarketId>,
) -> Result<HashSet<MarketId>, anyhow::Error> {
    let mut markets: HashSet<MarketId> = HashSet::new();
    let mut end: Option<i64> = None;
    let mut known_page_streak: u32 = 0;

    for page_num in 0..HISTORY_MAX_PAGES {
        let url = PolymarketEndpoint::UserTradeActivity {
            user: wallet.to_string(),
            end,
            start: None,
        }
        .url(base_url);

        let bytes = fetcher
            .fetch_page(&url)
            .await
            .map_err(|e| anyhow::anyhow!("fetch page {page_num}: {e}"))?;
        let items: Vec<HistoryItem> = serde_json::from_slice(&bytes)
            .map_err(|e| anyhow::anyhow!("parse page {page_num}: {e}"))?;

        if items.is_empty() {
            break;
        }

        let mut oldest = i64::MAX;
        let mut page_has_unknown = false;
        for item in &items {
            // Normalise ms → s (mirrors trade_parser) so the cursor stays in seconds.
            let ts = if item.timestamp > 9_999_999_999 {
                item.timestamp / 1_000
            } else {
                item.timestamp
            };
            oldest = oldest.min(ts);
            let market = MarketId(VenueMarketId(item.condition_id.clone()));
            if !known.contains(&market) {
                page_has_unknown = true;
            }
            markets.insert(market);
        }

        if items.len() < HISTORY_PAGE_SIZE {
            break; // short page → history exhausted
        }
        // Stop only after a margin of consecutive fully-cached pages, so a market
        // straddling the boundary under the endpoint's ~1 s ordering jitter is not
        // dropped; any unknown market resets the streak.
        if page_has_unknown {
            known_page_streak = 0;
        } else {
            known_page_streak += 1;
            if known_page_streak >= KNOWN_PAGE_MARGIN {
                break;
            }
        }
        if page_num + 1 >= HISTORY_MAX_PAGES {
            warn!(
                wallet = %wallet,
                pages = HISTORY_MAX_PAGES,
                "wallet history fetch hit page cap; older markets may be missed (possible false first-entry)"
            );
            break;
        }
        // `end` is inclusive; step strictly older to avoid re-fetching the boundary.
        end = Some(oldest.saturating_sub(1));
    }

    Ok(markets)
}

// ── Loader ──────────────────────────────────────────────────────────────────

/// Startup loader for the per-wallet market history consumed by the copy-entry gate.
pub struct WalletHistoryLoader;

impl WalletHistoryLoader {
    /// Load the per-wallet market history: stale sidecar ∪ fresh API fetch.
    ///
    /// Sequence: read the stale sidecar (warn and continue on error) → fetch each
    /// wallet's complete history warn-and-continue → union into the map → persist
    /// atomically (tmp + rename) → warn for every wallet still absent from the
    /// final map (fetch failed and no stale data).
    ///
    /// A wallet present with an empty set is *known* to have no prior markets (every
    /// entry is a first entry); a wallet absent is *unknown* and is governed by
    /// `entry_gate_fail_closed` in [`crate::entry_gate::CopyEntryGate`].
    pub async fn load<F: PageFetcher>(
        wallets: &[WalletAddress],
        base_url: &str,
        sidecar_path: &Path,
        fetcher: &F,
    ) -> HashMap<WalletAddress, HashSet<MarketId>> {
        let mut map = load_sidecar(sidecar_path);

        for &wallet in wallets {
            // Pass the cached set so the walk stops at already-known history
            // instead of re-fetching every wallet's full trade log each boot.
            let known = map.get(&wallet).cloned().unwrap_or_default();
            match fetch_history_for_wallet(wallet, base_url, fetcher, &known).await {
                Ok(markets) => {
                    map.entry(wallet).or_default().extend(markets);
                }
                Err(e) => {
                    warn!(
                        wallet = %wallet,
                        error = %e,
                        "wallet history fetch failed; using stale sidecar if present"
                    );
                }
            }
        }

        if let Err(e) = persist_sidecar(sidecar_path, &map) {
            warn!(
                path = %sidecar_path.display(),
                error = %e,
                "failed to persist wallet history sidecar"
            );
        }

        for &wallet in wallets {
            if !map.contains_key(&wallet) {
                warn!(
                    wallet = %wallet,
                    "no market history (fetch failed, no stale sidecar); first-entry gate governed by entry_gate_fail_closed"
                );
            }
        }

        map
    }
}

/// Read the sidecar into a map. Missing file → empty map (first run). A malformed
/// file is warned and treated as empty rather than aborting startup.
fn load_sidecar(path: &Path) -> HashMap<WalletAddress, HashSet<MarketId>> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
        Err(e) => {
            warn!(
                path = %path.display(),
                error = %e,
                "failed to read wallet history sidecar; starting empty"
            );
            return HashMap::new();
        }
    };
    match serde_json::from_slice::<WalletHistorySidecar>(&bytes) {
        Ok(s) => s
            .wallets
            .into_iter()
            .map(|e| (e.wallet, e.markets.into_iter().collect()))
            .collect(),
        Err(e) => {
            warn!(
                path = %path.display(),
                error = %e,
                "malformed wallet history sidecar; starting empty"
            );
            HashMap::new()
        }
    }
}

/// Persist the map atomically (tmp + rename), mirroring `paper-pnl`'s sidecar write.
fn persist_sidecar(
    path: &Path,
    map: &HashMap<WalletAddress, HashSet<MarketId>>,
) -> Result<(), anyhow::Error> {
    let sidecar = WalletHistorySidecar {
        wallets: map
            .iter()
            .map(|(wallet, markets)| WalletHistoryEntry {
                wallet: *wallet,
                markets: markets.iter().cloned().collect(),
            })
            .collect(),
    };
    let bytes = serde_json::to_vec_pretty(&sidecar)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &bytes)?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use pe_source_polymarket_public::FixtureFetcher;

    use super::*;

    const BASE: &str = "https://api.example.com";

    fn wallet() -> WalletAddress {
        serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
    }

    fn activity_url(w: WalletAddress, end: Option<i64>) -> String {
        PolymarketEndpoint::UserTradeActivity {
            user: w.to_string(),
            end,
            start: None,
        }
        .url(BASE)
    }

    /// Build a JSON activity array of `n` items at a fixed `timestamp`, condition
    /// IDs `0x<prefix><i>`.
    fn page(prefix: &str, n: usize, timestamp: i64) -> Vec<u8> {
        let items: Vec<String> = (0..n)
            .map(|i| format!(r#"{{"conditionId":"0x{prefix}{i}","timestamp":{timestamp}}}"#))
            .collect();
        format!("[{}]", items.join(",")).into_bytes()
    }

    /// The market-id set matching `page(prefix, n, _)`.
    fn market_set(prefix: &str, n: usize) -> HashSet<MarketId> {
        (0..n)
            .map(|i| MarketId(VenueMarketId(format!("0x{prefix}{i}"))))
            .collect()
    }

    #[tokio::test]
    async fn single_short_page_extracts_markets() {
        let w = wallet();
        let mut responses = HashMap::new();
        responses.insert(activity_url(w, None), page("c", 3, 2_000));
        let fetcher = FixtureFetcher::new(responses);

        let markets = fetch_history_for_wallet(w, BASE, &fetcher, &HashSet::new())
            .await
            .unwrap();
        assert_eq!(markets.len(), 3);
        assert!(markets.contains(&MarketId(VenueMarketId("0xc0".into()))));
    }

    #[tokio::test]
    async fn paginates_until_short_page() {
        let w = wallet();
        let mut responses = HashMap::new();
        // Page 1: a full page of 500 items at ts=2000 → oldest=2000 → next end=1999.
        responses.insert(activity_url(w, None), page("a", HISTORY_PAGE_SIZE, 2_000));
        // Page 2: a short page (1 item) at end=1999 → stop.
        responses.insert(activity_url(w, Some(1_999)), page("b", 1, 1_500));
        let fetcher = FixtureFetcher::new(responses);

        let markets = fetch_history_for_wallet(w, BASE, &fetcher, &HashSet::new())
            .await
            .unwrap();
        // 500 distinct from page 1 + 1 from page 2.
        assert_eq!(markets.len(), HISTORY_PAGE_SIZE + 1);
        assert!(markets.contains(&MarketId(VenueMarketId("0xb0".into()))));
    }

    #[tokio::test]
    async fn incremental_stops_after_known_page_margin() {
        // Caught-up wallet: stops after KNOWN_PAGE_MARGIN (2) consecutive fully-known
        // full pages. Provide exactly the margin's worth and no more; a third fetch
        // (no fixture) would error, proving the walk stopped at the margin.
        let w = wallet();
        let known = market_set("a", HISTORY_PAGE_SIZE);
        let mut responses = HashMap::new();
        responses.insert(activity_url(w, None), page("a", HISTORY_PAGE_SIZE, 2_000));
        responses.insert(
            activity_url(w, Some(1_999)),
            page("a", HISTORY_PAGE_SIZE, 1_500),
        );
        let fetcher = FixtureFetcher::new(responses);

        let markets = fetch_history_for_wallet(w, BASE, &fetcher, &known)
            .await
            .unwrap();
        assert_eq!(markets.len(), HISTORY_PAGE_SIZE);
    }

    #[tokio::test]
    async fn incremental_margin_does_not_stop_on_single_known_page() {
        // A single fully-known page (margin not yet reached) must NOT stop: a new
        // market straddling the boundary onto the next page is still captured.
        let w = wallet();
        let known = market_set("a", HISTORY_PAGE_SIZE);
        let mut responses = HashMap::new();
        // Page 1: all known. Page 2: a straddling new market (then short → stop).
        responses.insert(activity_url(w, None), page("a", HISTORY_PAGE_SIZE, 2_000));
        responses.insert(activity_url(w, Some(1_999)), page("late", 1, 1_500));
        let fetcher = FixtureFetcher::new(responses);

        let markets = fetch_history_for_wallet(w, BASE, &fetcher, &known)
            .await
            .unwrap();
        assert!(
            markets.contains(&MarketId(VenueMarketId("0xlate0".into()))),
            "market past a single known page must not be dropped"
        );
    }

    #[tokio::test]
    async fn incremental_walks_new_then_stops_at_cache_boundary() {
        // New activity exists: page 1 is fresh markets, then 2 cached pages reach the
        // margin and stop (no page-4 fixture). Union with the sidecar stays complete.
        let w = wallet();
        let known = market_set("a", HISTORY_PAGE_SIZE);
        let mut responses = HashMap::new();
        responses.insert(activity_url(w, None), page("new", HISTORY_PAGE_SIZE, 3_000));
        responses.insert(
            activity_url(w, Some(2_999)),
            page("a", HISTORY_PAGE_SIZE, 2_000),
        );
        responses.insert(
            activity_url(w, Some(1_999)),
            page("a", HISTORY_PAGE_SIZE, 1_500),
        );
        let fetcher = FixtureFetcher::new(responses);

        let markets = fetch_history_for_wallet(w, BASE, &fetcher, &known)
            .await
            .unwrap();
        assert!(markets.contains(&MarketId(VenueMarketId("0xnew0".into()))));
        // 500 new ∪ 500 known boundary = 1000 distinct.
        assert_eq!(markets.len(), HISTORY_PAGE_SIZE * 2);
    }

    #[tokio::test]
    async fn empty_first_page_yields_empty_history() {
        let w = wallet();
        let mut responses = HashMap::new();
        responses.insert(activity_url(w, None), b"[]".to_vec());
        let fetcher = FixtureFetcher::new(responses);

        let markets = fetch_history_for_wallet(w, BASE, &fetcher, &HashSet::new())
            .await
            .unwrap();
        assert!(markets.is_empty());
    }

    #[tokio::test]
    async fn load_unions_stale_sidecar_with_fetch() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wallet_market_history.json");
        let w = wallet();

        // Seed a stale sidecar with one market the API will not return.
        let stale = WalletHistorySidecar {
            wallets: vec![WalletHistoryEntry {
                wallet: w,
                markets: vec![MarketId(VenueMarketId("0xstale".into()))],
            }],
        };
        std::fs::write(&path, serde_json::to_vec_pretty(&stale).unwrap()).unwrap();

        let mut responses = HashMap::new();
        responses.insert(activity_url(w, None), page("fresh", 2, 2_000));
        let fetcher = FixtureFetcher::new(responses);

        let map = WalletHistoryLoader::load(&[w], BASE, &path, &fetcher).await;
        let set = map.get(&w).expect("wallet present");
        assert!(
            set.contains(&MarketId(VenueMarketId("0xstale".into()))),
            "stale retained"
        );
        assert!(
            set.contains(&MarketId(VenueMarketId("0xfresh0".into()))),
            "fresh unioned"
        );
        assert_eq!(set.len(), 3);
    }

    #[tokio::test]
    async fn load_fetch_failure_keeps_stale() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wallet_market_history.json");
        let w = wallet();

        let stale = WalletHistorySidecar {
            wallets: vec![WalletHistoryEntry {
                wallet: w,
                markets: vec![MarketId(VenueMarketId("0xstale".into()))],
            }],
        };
        std::fs::write(&path, serde_json::to_vec_pretty(&stale).unwrap()).unwrap();

        // No fixture for the activity URL → fetch fails; stale entry must remain.
        let fetcher = FixtureFetcher::new(HashMap::new());
        let map = WalletHistoryLoader::load(&[w], BASE, &path, &fetcher).await;
        let set = map.get(&w).expect("wallet present from stale");
        assert!(set.contains(&MarketId(VenueMarketId("0xstale".into()))));
    }

    #[tokio::test]
    async fn load_absent_wallet_omitted_on_fetch_failure() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wallet_market_history.json");
        let w = wallet();

        // No sidecar, no fixture → fetch fails and wallet is absent from the map
        // (the gate then governs it via entry_gate_fail_closed).
        let fetcher = FixtureFetcher::new(HashMap::new());
        let map = WalletHistoryLoader::load(&[w], BASE, &path, &fetcher).await;
        assert!(!map.contains_key(&w), "absent wallet not in map");
    }

    #[tokio::test]
    async fn load_persists_atomically_and_round_trips() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("wallet_market_history.json");
        let w = wallet();

        let mut responses = HashMap::new();
        responses.insert(activity_url(w, None), page("m", 2, 2_000));
        let fetcher = FixtureFetcher::new(responses);

        WalletHistoryLoader::load(&[w], BASE, &path, &fetcher).await;
        assert!(path.exists(), "sidecar persisted");

        // Re-load from disk only (empty fetcher, fetch fails) → persisted data survives.
        let empty = FixtureFetcher::new(HashMap::new());
        let map = WalletHistoryLoader::load(&[w], BASE, &path, &empty).await;
        let set = map.get(&w).expect("wallet present from persisted sidecar");
        assert!(set.contains(&MarketId(VenueMarketId("0xm0".into()))));
    }
}
