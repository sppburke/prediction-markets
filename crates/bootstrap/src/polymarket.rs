//! Polymarket bulk trade fetcher.
//!
//! Fetches the most recent trades for each wallet via `GET /trades?user=<wallet>&limit=500`,
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
        let url = format!(
            "{}&limit={TRADE_FETCH_LIMIT}",
            PolymarketEndpoint::UserTrades {
                user: wallet_hex.clone(),
            }
            .url(&self.base_url)
        );

        let bytes =
            self.fetcher
                .fetch_page(&url)
                .await
                .map_err(|e| BootstrapError::Polymarket {
                    wallet: wallet_hex.clone(),
                    message: e.to_string(),
                })?;

        parse_trades(&bytes, wallet).map_err(|e| BootstrapError::TradeParse {
            wallet: wallet_hex,
            message: e,
        })
    }
}

// ── Parser ────────────────────────────────────────────────────────────────────

fn parse_trades(bytes: &[u8], wallet: WalletAddress) -> Result<Vec<RawTrade>, String> {
    let response: TradeResponse =
        serde_json::from_slice(bytes).map_err(|e| format!("json: {e}"))?;

    let mut out = Vec::with_capacity(response.len());
    for raw in response {
        match convert_trade(raw, wallet) {
            Ok(t) => out.push(t),
            Err(e) => tracing::warn!(wallet = %wallet, error = %e, "skipping unparseable trade"),
        }
    }
    Ok(out)
}

fn convert_trade(raw: PolymarketTrade, wallet: WalletAddress) -> Result<RawTrade, String> {
    let price_dec = raw.price;
    let price = Price::new(price_dec).map_err(|e| format!("invalid price {price_dec}: {e}"))?;

    let contracts = raw
        .size
        .floor()
        .to_u64()
        .map(ContractQty)
        .ok_or_else(|| format!("size {} out of u64 range", raw.size))?;

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
    use super::*;

    fn wallet_a() -> WalletAddress {
        WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
    }

    #[test]
    fn parse_valid_buy_trade() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":10,"price":0.60,"timestamp":1704067200}]"#;
        let trades = parse_trades(json, wallet_a()).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].side, Side::Buy);
        assert_eq!(trades[0].contracts.0, 10);
    }

    #[test]
    fn parse_millisecond_timestamp() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"SELL","size":5,"price":0.40,"timestamp":1704067200000}]"#;
        let trades = parse_trades(json, wallet_a()).unwrap();
        assert_eq!(trades[0].timestamp.0.unix_timestamp(), 1_704_067_200);
    }

    #[test]
    fn parse_unknown_side_skipped() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"UNKNOWN","size":5,"price":0.40,"timestamp":1704067200}]"#;
        let trades = parse_trades(json, wallet_a()).unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn parse_empty_array() {
        let trades = parse_trades(b"[]", wallet_a()).unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn parse_outcome_index_propagated() {
        let json = br#"[{"transactionHash":"0xhash","conditionId":"0xcond","side":"BUY","size":1,"price":0.50,"timestamp":1704067200,"outcomeIndex":1}]"#;
        let trades = parse_trades(json, wallet_a()).unwrap();
        assert_eq!(trades[0].outcome_id, OutcomeId(1));
    }
}
