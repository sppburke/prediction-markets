//! Startup and periodic re-seed of the leader [`PositionLedger`] from
//! the Polymarket positions API.
//!
//! Parsing is isolated in [`parse_positions`]; the paginated HTTP fetch lives in
//! the private [`fetch_positions_for_wallet`]; [`seed_all`] sequences wallets
//! warn-and-continuing on per-wallet failures. [`run_reseed_loop`] is the
//! background task wired into the orchestrator.

use std::collections::HashMap;

use pe_copy_signal_engine::{PositionSnapshot, PositionState};
use pe_core_types::{MarketId, MarketOutcomeId, OutcomeId, VenueMarketId, WalletAddress};
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::warn;

/// Safety backstop: stop paginating after this many pages per wallet.
/// A wallet exceeding `page_limit × POSITION_MAX_PAGES` live positions gets partial coverage;
/// a `warn!` is emitted so hub-like wallets are visible in logs.
const POSITION_MAX_PAGES: u32 = 20;

// ── DTO ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPosition {
    condition_id: String,
    #[serde(default)]
    outcome_index: u16,
    size: Decimal,
}

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum PositionParseError {
    #[error("failed to deserialize positions response: {0}")]
    Json(#[from] serde_json::Error),
}

// ── Parse ─────────────────────────────────────────────────────────────────────

/// Parse a flat-array positions response into a [`PositionSnapshot`].
///
/// Dust positions (floor to 0 contracts) are skipped. Holdings are always long
/// (`short_contracts = 0`), as the API returns only outcome tokens held.
pub fn parse_positions(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<PositionSnapshot, PositionParseError> {
    let raw: Vec<RawPosition> = serde_json::from_slice(bytes)?;
    let mut positions = HashMap::new();
    for item in raw {
        let qty: u64 = match item.size.floor().to_u64() {
            Some(q) if q > 0 => q,
            _ => continue,
        };
        let market_id = MarketId(VenueMarketId(item.condition_id));
        let key = MarketOutcomeId::new(market_id, OutcomeId(item.outcome_index));
        positions.insert(
            key,
            PositionState {
                long_contracts: qty,
                short_contracts: 0,
            },
        );
    }
    Ok(PositionSnapshot { wallet, positions })
}

// ── Fetch ─────────────────────────────────────────────────────────────────────

async fn fetch_positions_for_wallet<F: PageFetcher>(
    wallet: WalletAddress,
    base_url: &str,
    page_limit: u32,
    size_threshold: u32,
    fetcher: &F,
) -> Result<PositionSnapshot, anyhow::Error> {
    let mut all_positions: HashMap<MarketOutcomeId, PositionState> = HashMap::new();
    let mut offset: u32 = 0;

    for page_num in 0..POSITION_MAX_PAGES {
        let url = PolymarketEndpoint::CurrentPositions {
            user: wallet.to_string(),
            limit: Some(page_limit),
            offset: Some(offset),
            redeemable: Some(false),
            size_threshold: Some(size_threshold),
        }
        .url(base_url);

        let bytes = fetcher
            .fetch_page(&url)
            .await
            .map_err(|e| anyhow::anyhow!("fetch page {page_num}: {e}"))?;

        let snap = parse_positions(&bytes, wallet)
            .map_err(|e| anyhow::anyhow!("parse page {page_num}: {e}"))?;

        let page_count = snap.positions.len() as u32;
        all_positions.extend(snap.positions);

        if page_count < page_limit {
            break;
        }

        if page_num + 1 >= POSITION_MAX_PAGES {
            warn!(
                wallet = %wallet,
                pages = POSITION_MAX_PAGES,
                "position fetch hit page cap; wallet may have more open positions"
            );
            break;
        }

        offset = offset.saturating_add(page_limit);
    }

    Ok(PositionSnapshot {
        wallet,
        positions: all_positions,
    })
}

// ── seed_all ──────────────────────────────────────────────────────────────────

/// Fetch positions for all `wallets` sequentially; warn-and-continue on per-wallet failure.
///
/// Returns a map for every wallet whose fetch succeeded (including wallets with no open
/// positions). A wallet absent from the returned map had a fetch or parse failure; the
/// caller should retain the existing ledger state for that wallet.
pub async fn seed_all<F: PageFetcher>(
    wallets: &[WalletAddress],
    base_url: &str,
    page_limit: u32,
    size_threshold: u32,
    fetcher: &F,
) -> HashMap<WalletAddress, PositionSnapshot> {
    let mut out = HashMap::new();
    for &wallet in wallets {
        match fetch_positions_for_wallet(wallet, base_url, page_limit, size_threshold, fetcher)
            .await
        {
            Ok(snap) => {
                out.insert(wallet, snap);
            }
            Err(e) => {
                warn!(
                    wallet = %wallet,
                    error = %e,
                    "position seed failed for wallet; retaining existing ledger state"
                );
            }
        }
    }
    out
}

// ── run_reseed_loop ───────────────────────────────────────────────────────────

/// Periodically re-seed the orchestrator's leader ledger from the positions API.
///
/// Reads the CURRENT live watchlist each round (2026-07-03 cutover fix): the loop used to
/// iterate the boot wallet list forever, so wallets admitted post-boot (backfill, and now
/// every 4h full-re-rank swap) never received leader-ledger seeds and their Adds were
/// misclassified as Entries. Never advances poll cursors — cursor advancement is
/// startup-only. Exits when `tx` is closed (orchestrator shut down).
pub async fn run_reseed_loop<F: PageFetcher + Send + 'static>(
    live: crate::live_watchlist::LiveWatchlist,
    base_url: String,
    page_limit: u32,
    size_threshold: u32,
    interval_secs: u64,
    fetcher: F,
    tx: mpsc::Sender<HashMap<WalletAddress, PositionSnapshot>>,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        let wallets: Vec<WalletAddress> =
            live.snapshot().entries.iter().map(|e| e.wallet).collect();
        let map = seed_all(&wallets, &base_url, page_limit, size_threshold, &fetcher).await;
        if tx.send(map).await.is_err() {
            break;
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;

    use pe_source_polymarket_public::FixtureFetcher;

    use super::*;

    fn wallet() -> WalletAddress {
        serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
    }

    #[test]
    fn parse_basic() {
        let w = wallet();
        let bytes = br#"[{"conditionId":"0xccc","outcomeIndex":0,"size":"150"}]"#;
        let snap = parse_positions(bytes, w).unwrap();
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xccc".into())), OutcomeId(0));
        let state = snap.positions[&key];
        assert_eq!(state.long_contracts, 150);
        assert_eq!(state.short_contracts, 0);
    }

    #[test]
    fn parse_dust_skipped() {
        let w = wallet();
        // size = 0.4 → floor = 0 → skipped
        let bytes = br#"[{"conditionId":"0xccc","outcomeIndex":0,"size":"0.4"}]"#;
        let snap = parse_positions(bytes, w).unwrap();
        assert!(snap.positions.is_empty());
    }

    #[test]
    fn parse_fractional_size_floored() {
        let w = wallet();
        // size = 75.8 → floor = 75
        let bytes = br#"[{"conditionId":"0xccc","outcomeIndex":1,"size":"75.8"}]"#;
        let snap = parse_positions(bytes, w).unwrap();
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xccc".into())), OutcomeId(1));
        assert_eq!(snap.positions[&key].long_contracts, 75);
    }

    #[test]
    fn parse_empty_array() {
        let w = wallet();
        let snap = parse_positions(b"[]", w).unwrap();
        assert!(snap.positions.is_empty());
    }

    #[test]
    fn parse_missing_outcome_index_defaults_zero() {
        let w = wallet();
        // outcomeIndex absent → #[serde(default)] → 0
        let bytes = br#"[{"conditionId":"0xccc","size":"10"}]"#;
        let snap = parse_positions(bytes, w).unwrap();
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xccc".into())), OutcomeId(0));
        assert!(snap.positions.contains_key(&key));
    }

    #[tokio::test]
    async fn seed_all_failure_omits_wallet() {
        // FixtureFetcher returns Fatal for unknown URLs → seed_all omits the wallet.
        let w = wallet();
        let fetcher = FixtureFetcher::new(HashMap::new());
        let map = seed_all(&[w], "https://api.example.com", 500, 1, &fetcher).await;
        assert!(map.is_empty(), "failed wallet should be omitted from map");
    }

    #[tokio::test]
    async fn seed_all_success_empty_included() {
        // A wallet with no positions (empty array) is still included in the returned map.
        let w = wallet();
        let url = PolymarketEndpoint::CurrentPositions {
            user: w.to_string(),
            limit: Some(500),
            offset: Some(0),
            redeemable: Some(false),
            size_threshold: Some(1),
        }
        .url("https://api.example.com");
        let mut responses = HashMap::new();
        responses.insert(url, b"[]".to_vec());
        let fetcher = FixtureFetcher::new(responses);
        let map = seed_all(&[w], "https://api.example.com", 500, 1, &fetcher).await;
        assert!(map.contains_key(&w));
        assert!(map[&w].positions.is_empty());
    }

    #[tokio::test]
    async fn seed_all_positions_returned() {
        let w = wallet();
        let url = PolymarketEndpoint::CurrentPositions {
            user: w.to_string(),
            limit: Some(500),
            offset: Some(0),
            redeemable: Some(false),
            size_threshold: Some(1),
        }
        .url("https://api.example.com");
        let mut responses = HashMap::new();
        responses.insert(
            url,
            br#"[{"conditionId":"0xaaa","outcomeIndex":0,"size":"100"}]"#.to_vec(),
        );
        let fetcher = FixtureFetcher::new(responses);
        let map = seed_all(&[w], "https://api.example.com", 500, 1, &fetcher).await;
        assert_eq!(map.len(), 1);
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xaaa".into())), OutcomeId(0));
        assert_eq!(map[&w].positions[&key].long_contracts, 100);
    }

    #[tokio::test]
    async fn max_pages_cap_stops_and_returns() {
        // Register POSITION_MAX_PAGES pages of 1 item each (each returns page_limit=1 items).
        // The function must stop at the cap and return without error.
        let w = wallet();
        let base = "https://api.example.com";
        let mut responses = HashMap::new();
        for page_num in 0..POSITION_MAX_PAGES {
            let url = PolymarketEndpoint::CurrentPositions {
                user: w.to_string(),
                limit: Some(1),
                offset: Some(page_num),
                redeemable: Some(false),
                size_threshold: Some(1),
            }
            .url(base);
            responses.insert(
                url,
                format!(r#"[{{"conditionId":"0xcond{page_num:04}","outcomeIndex":0,"size":"5"}}]"#)
                    .into_bytes(),
            );
        }
        let fetcher = FixtureFetcher::new(responses);
        let snap = fetch_positions_for_wallet(w, base, 1, 1, &fetcher)
            .await
            .unwrap();
        // 20 distinct conditionIds → 20 entries.
        assert_eq!(snap.positions.len() as u32, POSITION_MAX_PAGES);
    }
}
