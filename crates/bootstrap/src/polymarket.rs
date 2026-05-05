//! Polymarket bulk trade fetcher.
//!
//! Fetches all trades for each wallet via paginated `GET /trades?user=<wallet>&limit=500&offset=N`,
//! checking the [`WalletCache`] first and only hitting the network on cache misses.
//! Skips wallets whose fetch returns a non-fatal error (logs a warning).

use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_source_polymarket_public::{PageFetcher, PolymarketEndpoint};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Deserialize;
use time::OffsetDateTime;

use crate::cache::WalletCache;
use crate::error::BootstrapError;

const TRADE_FETCH_LIMIT: u32 = 500;

// ── JSON DTOs ─────────────────────────────────────────────────────────────────

type TradeResponse = Vec<PolymarketTrade>;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PolymarketTrade {
    transaction_hash: String,
    condition_id: String,
    side: String,
    size: Decimal,
    price: Decimal,
    timestamp: i64,
    #[serde(default)]
    outcome_index: Option<u8>,
}

// ── Fetcher ───────────────────────────────────────────────────────────────────

/// Fetches trade history for a slice of wallets, using the cache to avoid redundant requests.
pub struct PolymarketBulkFetcher<F: PageFetcher> {
    base_url: String,
    fetcher: F,
}

impl<F: PageFetcher> PolymarketBulkFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self { base_url, fetcher }
    }

    /// Fetch trades for all `wallets`, updating `cache` for each cache miss.
    ///
    /// Returns a flat `Vec<RawTrade>` across all wallets. Wallets that produce a
    /// network error are skipped with a `tracing::warn!`; parse errors for individual
    /// wallets are also skipped rather than aborting the whole run.
    pub async fn fetch_all(
        &mut self,
        wallets: &[WalletAddress],
        cache: &mut WalletCache,
    ) -> Vec<RawTrade> {
        let mut all_trades: Vec<RawTrade> = Vec::new();

        for wallet in wallets {
            let wallet_hex = wallet.to_string();

            if let Some(cached) = cache.get(&wallet_hex) {
                all_trades.extend_from_slice(cached);
                continue;
            }

            match self.fetch_wallet(*wallet).await {
                Ok(trades) => {
                    all_trades.extend_from_slice(&trades);
                    cache.insert(wallet_hex, trades);
                }
                Err(e) => {
                    tracing::warn!(wallet = %wallet_hex, error = %e, "polymarket: skipping wallet");
                }
            }
        }

        all_trades
    }

    async fn fetch_wallet(
        &mut self,
        wallet: WalletAddress,
    ) -> Result<Vec<RawTrade>, BootstrapError> {
        let wallet_hex = wallet.to_string();
        let endpoint = PolymarketEndpoint::UserTrades {
            user: wallet_hex.clone(),
        }
        .url(&self.base_url);

        let mut all_trades: Vec<RawTrade> = Vec::new();
        let mut offset: u32 = 0;

        loop {
            let url = format!("{endpoint}&limit={TRADE_FETCH_LIMIT}&offset={offset}");

            let bytes =
                self.fetcher
                    .fetch_page(&url)
                    .await
                    .map_err(|e| BootstrapError::Polymarket {
                        wallet: wallet_hex.clone(),
                        message: e.to_string(),
                    })?;

            let (page, raw_count) = parse_trades_with_count(&bytes, wallet).map_err(|e| {
                BootstrapError::TradeParse {
                    wallet: wallet_hex.clone(),
                    message: e,
                }
            })?;

            all_trades.extend(page);

            // A partial page (fewer raw entries than the limit) signals the last page.
            if raw_count < TRADE_FETCH_LIMIT as usize {
                break;
            }
            offset += TRADE_FETCH_LIMIT;
        }

        Ok(all_trades)
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Parse `bytes` into trades, also returning the raw JSON array length for
/// pagination termination. The raw count is used (not the parsed count) so that
/// individual trade parse failures do not cause early pagination termination.
fn parse_trades_with_count(
    bytes: &[u8],
    wallet: WalletAddress,
) -> Result<(Vec<RawTrade>, usize), String> {
    let response: TradeResponse =
        serde_json::from_slice(bytes).map_err(|e| format!("json: {e}"))?;
    let raw_count = response.len();
    let mut out = Vec::with_capacity(raw_count);
    for raw in response {
        match convert_trade(raw, wallet) {
            Ok(t) => out.push(t),
            Err(e) => tracing::warn!(wallet = %wallet, error = %e, "skipping unparseable trade"),
        }
    }
    Ok((out, raw_count))
}

fn convert_trade(raw: PolymarketTrade, wallet: WalletAddress) -> Result<RawTrade, String> {
    let price_dec = raw.price;
    let price = Price::new(price_dec).map_err(|e| format!("invalid price {price_dec}: {e}"))?;

    let contracts = raw
        .size
        .floor()
        .to_u64()
        .filter(|&n| n > 0)
        .map(ContractQty)
        .ok_or_else(|| format!("size {} floors to zero contracts", raw.size))?;

    let side = match raw.side.to_uppercase().as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => return Err(format!("unknown side '{other}'")),
    };

    // Normalise to seconds; Polymarket sometimes uses milliseconds.
    let ts_secs = if raw.timestamp > 9_999_999_999 {
        raw.timestamp / 1_000
    } else {
        raw.timestamp
    };
    let dt = OffsetDateTime::from_unix_timestamp(ts_secs)
        .map_err(|_| format!("invalid timestamp {ts_secs}"))?;

    Ok(RawTrade {
        wallet,
        market_id: MarketId(VenueMarketId(raw.condition_id)),
        outcome_id: OutcomeId(raw.outcome_index.unwrap_or(0)),
        side,
        price,
        contracts,
        timestamp: SourceTimestamp(dt),
        source_trade_id: SourceTradeId(raw.transaction_hash),
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

    const BASE_URL: &str = "https://data-api.polymarket.com";

    fn wallet_a() -> WalletAddress {
        WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    fn trade_json(i: usize) -> String {
        format!(
            r#"{{"transactionHash":"0xhash{i:06}","conditionId":"0xcond","side":"BUY","size":1,"price":0.60,"timestamp":1704067200}}"#,
        )
    }

    fn page_json(n: usize) -> Vec<u8> {
        let entries: Vec<String> = (0..n).map(trade_json).collect();
        format!("[{}]", entries.join(",")).into_bytes()
    }

    fn trade_url(wallet: WalletAddress, offset: u32) -> String {
        format!(
            "{}&limit={TRADE_FETCH_LIMIT}&offset={offset}",
            PolymarketEndpoint::UserTrades {
                user: wallet.to_string()
            }
            .url(BASE_URL)
        )
    }

    fn parse(bytes: &[u8]) -> Vec<RawTrade> {
        parse_trades_with_count(bytes, wallet_a()).unwrap().0
    }

    #[test]
    fn parse_valid_buy_trade() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":10,"price":0.60,"timestamp":1704067200}]"#;
        let trades = parse(json);
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].side, Side::Buy);
        assert_eq!(trades[0].contracts.0, 10);
    }

    #[test]
    fn parse_millisecond_timestamp() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"SELL","size":5,"price":0.40,"timestamp":1704067200000}]"#;
        let trades = parse(json);
        assert_eq!(trades[0].timestamp.0.unix_timestamp(), 1_704_067_200);
    }

    #[test]
    fn parse_unknown_side_skipped() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"UNKNOWN","size":5,"price":0.40,"timestamp":1704067200}]"#;
        let trades = parse(json);
        assert!(trades.is_empty());
    }

    #[test]
    fn parse_fractional_size_skipped() {
        // 0.75 contracts floors to 0 — trade must be skipped with a warning.
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":0.75,"price":0.40,"timestamp":1704067200}]"#;
        let trades = parse(json);
        assert!(trades.is_empty());
    }

    #[test]
    fn parse_empty_array() {
        let trades = parse(b"[]");
        assert!(trades.is_empty());
    }

    #[test]
    fn parse_outcome_index_propagated() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":1,"price":0.50,"timestamp":1704067200,"outcomeIndex":1}]"#;
        let trades = parse(json);
        assert_eq!(trades[0].outcome_id, OutcomeId(1));
    }

    #[test]
    fn raw_count_independent_of_parse_failures() {
        // 2 entries in JSON; 1 has unknown side (skipped). raw_count must be 2.
        let json = br#"[
            {"transactionHash":"0xhash1","conditionId":"0xcond","side":"BUY","size":1,"price":0.60,"timestamp":1704067200},
            {"transactionHash":"0xhash2","conditionId":"0xcond","side":"UNKNOWN","size":1,"price":0.60,"timestamp":1704067200}
        ]"#;
        let (trades, raw_count) = parse_trades_with_count(json, wallet_a()).unwrap();
        assert_eq!(
            raw_count, 2,
            "raw count should include the unparseable entry"
        );
        assert_eq!(trades.len(), 1, "only the parseable entry is returned");
    }

    #[tokio::test]
    async fn pagination_concatenates_two_full_pages() {
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        responses.insert(trade_url(wallet, 0), page_json(500));
        responses.insert(trade_url(wallet, 500), page_json(500));
        responses.insert(trade_url(wallet, 1000), page_json(0)); // terminal empty page

        let fetcher = FixtureFetcher::new(responses);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.json")).unwrap();
        let mut bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);
        let trades = bulk.fetch_all(&[wallet], &mut cache).await;

        assert_eq!(
            trades.len(),
            1000,
            "expected 1000 trades from two full pages"
        );
    }

    #[tokio::test]
    async fn pagination_stops_on_partial_page() {
        let wallet = wallet_a();
        let mut responses = HashMap::new();
        // 499 entries — partial page, must stop without requesting offset=500.
        responses.insert(trade_url(wallet, 0), page_json(499));

        let fetcher = FixtureFetcher::new(responses);
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.json")).unwrap();
        let mut bulk = PolymarketBulkFetcher::new(BASE_URL.to_owned(), fetcher);
        let trades = bulk.fetch_all(&[wallet], &mut cache).await;

        assert_eq!(
            trades.len(),
            499,
            "expected 499 trades from one partial page"
        );
    }
}
