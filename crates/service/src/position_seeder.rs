//! Strict Polymarket positions-API parsing for read-only reconciliation evidence.
//!
//! Parsing is isolated in [`parse_positions`]; the paginated HTTP fetch lives in
//! the private [`fetch_positions_for_wallet`]; [`seed_all`] sequences wallets
//! warn-and-continuing on per-wallet failures. The service no longer runs a
//! wholesale-replacement reseed loop (#544); Lane D may reuse this parser for
//! causal before/after position brackets.

use std::collections::HashMap;

use pe_copy_signal_engine::{PositionSnapshot, PositionState};
use pe_core_types::{
    MarketId, MarketOutcomeId, OutcomeId, ShareAmount, VenueMarketId, WalletAddress,
};
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;
use tracing::warn;

/// Safety backstop: stop paginating after this many pages per wallet.
/// A wallet whose response rows exceed `page_limit × POSITION_MAX_PAGES` cannot be snapshotted
/// completely, so the fetch fails rather than returning a truncated portfolio (#542): a missing
/// open position classifies the leader's next BUY as a first Entry.
const POSITION_MAX_PAGES: u32 = 20;

// ── DTO ───────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPosition {
    condition_id: String,
    #[serde(default)]
    outcome_index: Option<u16>,
    size: Decimal,
}

// ── Errors ────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum PositionParseError {
    #[error("failed to deserialize positions response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("position omitted outcomeIndex")]
    MissingOutcomeIndex,
    #[error("position has invalid exact size {value}: {reason}")]
    InvalidAmount { value: Decimal, reason: String },
}

// ── Parse ─────────────────────────────────────────────────────────────────────

/// Parse a flat-array positions response into a [`PositionSnapshot`].
///
/// Exact zero positions are skipped. Holdings are always long
/// (`short_contracts = 0`), as the API returns only outcome tokens held.
pub fn parse_positions(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<PositionSnapshot, PositionParseError> {
    parse_positions_counted(bytes, wallet).map(|(snapshot, _)| snapshot)
}

/// Parse a page and also report the number of rows the response actually carried.
///
/// Pagination must advance on the decoded row count, never on the retained position count:
/// zero rows are skipped and duplicate `(market, outcome)` keys collapse, so a full page can
/// retain fewer entries than `limit` and would otherwise be read as "history exhausted" (#542).
fn parse_positions_counted(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<(PositionSnapshot, usize), PositionParseError> {
    let raw: Vec<RawPosition> = serde_json::from_slice(bytes)?;
    let decoded_rows = raw.len();
    let mut positions = HashMap::new();
    for item in raw {
        let qty = ShareAmount::from_decimal_exact(item.size).map_err(|error| {
            PositionParseError::InvalidAmount {
                value: item.size,
                reason: error.to_string(),
            }
        })?;
        if qty == ShareAmount::ZERO {
            continue;
        }
        let market_id = MarketId(VenueMarketId(item.condition_id));
        let key = MarketOutcomeId::new(market_id, OutcomeId(item.outcome_index.unwrap_or(0)));
        positions.insert(
            key,
            PositionState {
                long_contracts: qty,
                short_contracts: ShareAmount::ZERO,
            },
        );
    }
    Ok((PositionSnapshot { wallet, positions }, decoded_rows))
}

/// Parse live-canary positions without the ordinary parser's legacy outcome-zero default.
pub fn parse_positions_strict(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<PositionSnapshot, PositionParseError> {
    let raw: Vec<RawPosition> = serde_json::from_slice(bytes)?;
    if raw.iter().any(|position| position.outcome_index.is_none()) {
        return Err(PositionParseError::MissingOutcomeIndex);
    }
    parse_positions(bytes, wallet)
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

        let (snap, decoded_rows) = parse_positions_counted(&bytes, wallet)
            .map_err(|e| anyhow::anyhow!("parse page {page_num}: {e}"))?;

        all_positions.extend(snap.positions);

        // Short page → the wallet's positions are exhausted. Counted on decoded rows, not on
        // retained entries, so a page filtered down to fewer positions still continues.
        if decoded_rows < page_limit as usize {
            break;
        }

        // A full final permitted page means completeness cannot be proven without another page.
        // Fail rather than return a possibly truncated portfolio (the organic canary's
        // precedent, `organic_canary.rs`).
        if page_num + 1 >= POSITION_MAX_PAGES {
            return Err(anyhow::anyhow!(
                "position fetch hit the {POSITION_MAX_PAGES}-page cap with a full final page; \
                 snapshot is incomplete"
            ));
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
        assert_eq!(
            state.long_contracts,
            pe_core_types::ShareAmount::from_whole(150).unwrap()
        );
        assert_eq!(state.short_contracts, pe_core_types::ShareAmount::ZERO);
    }

    #[test]
    fn parse_fractional_position_is_exact() {
        let w = wallet();
        let bytes = br#"[{"conditionId":"0xccc","outcomeIndex":0,"size":"0.4"}]"#;
        let snap = parse_positions(bytes, w).unwrap();
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xccc".into())), OutcomeId(0));
        assert_eq!(
            snap.positions[&key].long_contracts,
            ShareAmount::from_decimal_exact(Decimal::new(4, 1)).unwrap()
        );
    }

    #[test]
    fn parse_fractional_size_is_not_floored() {
        let w = wallet();
        let bytes = br#"[{"conditionId":"0xccc","outcomeIndex":1,"size":"75.8"}]"#;
        let snap = parse_positions(bytes, w).unwrap();
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xccc".into())), OutcomeId(1));
        assert_eq!(
            snap.positions[&key].long_contracts,
            ShareAmount::from_decimal_exact(Decimal::new(758, 1)).unwrap()
        );
    }

    #[test]
    fn parse_empty_array() {
        let w = wallet();
        let snap = parse_positions(b"[]", w).unwrap();
        assert!(snap.positions.is_empty());
    }

    #[test]
    fn ordinary_missing_outcome_index_keeps_legacy_default() {
        let w = wallet();
        let bytes = br#"[{"conditionId":"0xccc","size":"10"}]"#;
        let snap = parse_positions(bytes, w).unwrap();
        let key = MarketOutcomeId::new(MarketId(VenueMarketId("0xccc".into())), OutcomeId(0));
        assert!(snap.positions.contains_key(&key));
        assert!(matches!(
            parse_positions_strict(bytes, w),
            Err(PositionParseError::MissingOutcomeIndex)
        ));
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
        assert_eq!(
            map[&w].positions[&key].long_contracts,
            pe_core_types::ShareAmount::from_whole(100).unwrap()
        );
    }

    /// Register `pages` full pages of one row each at `limit = 1`, condition IDs `0xcondNNNN`.
    fn full_pages(w: WalletAddress, base: &str, pages: u32) -> HashMap<String, Vec<u8>> {
        (0..pages)
            .map(|page_num| {
                let url = PolymarketEndpoint::CurrentPositions {
                    user: w.to_string(),
                    limit: Some(1),
                    offset: Some(page_num),
                    redeemable: Some(false),
                    size_threshold: Some(1),
                }
                .url(base);
                let body = format!(
                    r#"[{{"conditionId":"0xcond{page_num:04}","outcomeIndex":0,"size":"5"}}]"#
                )
                .into_bytes();
                (url, body)
            })
            .collect()
    }

    #[tokio::test]
    async fn full_final_page_at_cap_fails_instead_of_truncating() {
        // POSITION_MAX_PAGES full pages: the snapshot is knowably incomplete, so the fetch
        // must fail rather than hand a truncated portfolio to classification (#542).
        let w = wallet();
        let base = "https://api.example.com";
        let fetcher = FixtureFetcher::new(full_pages(w, base, POSITION_MAX_PAGES));
        let error = fetch_positions_for_wallet(w, base, 1, 1, &fetcher)
            .await
            .expect_err("a full final permitted page must fail");
        assert!(
            error.to_string().contains("snapshot is incomplete"),
            "unexpected error: {error}"
        );
        // seed_all keeps its warn-and-omit contract for the failed wallet.
        let map = seed_all(&[w], base, 1, 1, &fetcher).await;
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn short_page_before_the_cap_succeeds() {
        let w = wallet();
        let base = "https://api.example.com";
        let mut responses = full_pages(w, base, POSITION_MAX_PAGES - 1);
        let last = PolymarketEndpoint::CurrentPositions {
            user: w.to_string(),
            limit: Some(1),
            offset: Some(POSITION_MAX_PAGES - 1),
            redeemable: Some(false),
            size_threshold: Some(1),
        }
        .url(base);
        responses.insert(last, b"[]".to_vec());
        let fetcher = FixtureFetcher::new(responses);
        let snap = fetch_positions_for_wallet(w, base, 1, 1, &fetcher)
            .await
            .unwrap();
        assert_eq!(
            snap.positions.len(),
            usize::try_from(POSITION_MAX_PAGES - 1).unwrap()
        );
    }

    #[tokio::test]
    async fn filtered_full_page_continues_pagination() {
        // Page 0 is FULL (3 decoded rows) but retains 2 positions: one fractional row and two
        // rows sharing a `(market, outcome)` key. Counting retained positions would
        // read the page as short and lose page 1.
        let w = wallet();
        let base = "https://api.example.com";
        let page_url = |offset: u32| {
            PolymarketEndpoint::CurrentPositions {
                user: w.to_string(),
                limit: Some(3),
                offset: Some(offset),
                redeemable: Some(false),
                size_threshold: Some(1),
            }
            .url(base)
        };
        let mut responses = HashMap::new();
        responses.insert(
            page_url(0),
            br#"[{"conditionId":"0xdust","outcomeIndex":0,"size":"0.4"},
                 {"conditionId":"0xdup","outcomeIndex":0,"size":"5"},
                 {"conditionId":"0xdup","outcomeIndex":0,"size":"7"}]"#
                .to_vec(),
        );
        responses.insert(
            page_url(3),
            br#"[{"conditionId":"0xreal","outcomeIndex":1,"size":"42"}]"#.to_vec(),
        );
        let fetcher = FixtureFetcher::new(responses);
        let snap = fetch_positions_for_wallet(w, base, 3, 1, &fetcher)
            .await
            .unwrap();
        let real = MarketOutcomeId::new(MarketId(VenueMarketId("0xreal".into())), OutcomeId(1));
        let dup = MarketOutcomeId::new(MarketId(VenueMarketId("0xdup".into())), OutcomeId(0));
        assert_eq!(
            snap.positions[&real].long_contracts,
            pe_core_types::ShareAmount::from_whole(42).unwrap()
        );
        assert_eq!(
            snap.positions[&dup].long_contracts,
            pe_core_types::ShareAmount::from_whole(7).unwrap()
        );
        let fractional =
            MarketOutcomeId::new(MarketId(VenueMarketId("0xdust".into())), OutcomeId(0));
        assert_eq!(
            snap.positions[&fractional].long_contracts,
            ShareAmount::from_decimal_exact(Decimal::new(4, 1)).unwrap()
        );
        assert_eq!(snap.positions.len(), 3);
    }
}
