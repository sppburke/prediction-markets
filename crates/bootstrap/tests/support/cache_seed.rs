use pe_bootstrap::cache::WalletCache;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_trader_index::RawTrade;
use rust_decimal::Decimal;
use std::str::FromStr;
use time::OffsetDateTime;

/// Seed recorded trade fixtures directly; acquisition is outside these cache contracts.
pub fn insert_page(cache: &mut WalletCache, wallet: WalletAddress, bytes: &[u8]) {
    let rows: Vec<serde_json::Value> = serde_json::from_slice(bytes).unwrap();
    let trades = rows
        .iter()
        .map(|r| RawTrade {
            wallet,
            market_id: MarketId(VenueMarketId(r["conditionId"].as_str().unwrap().into())),
            outcome_id: OutcomeId(u16::try_from(r["outcomeIndex"].as_u64().unwrap_or(0)).unwrap()),
            side: if r["side"] == "SELL" {
                Side::Sell
            } else {
                Side::Buy
            },
            price: Price(
                Decimal::from_str(
                    &r["price"]
                        .as_str()
                        .map_or_else(|| r["price"].to_string(), str::to_owned),
                )
                .unwrap(),
            ),
            contracts: ContractQty(
                r["size"]
                    .as_str()
                    .map_or_else(|| r["size"].as_u64().unwrap(), |s| s.parse().unwrap()),
            ),
            timestamp: SourceTimestamp(
                OffsetDateTime::from_unix_timestamp(r["timestamp"].as_i64().unwrap()).unwrap(),
            ),
            source_trade_id: SourceTradeId(r["transactionHash"].as_str().unwrap().into()),
        })
        .collect();
    cache.insert_new(&wallet.to_string(), trades).unwrap();
}
