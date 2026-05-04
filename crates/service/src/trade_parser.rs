//! Parse Polymarket `UserTrades` API responses into typed [`IncomingTrade`]s.

use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::{ContractQty, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Deserialize;
use thiserror::Error;
use time::OffsetDateTime;

#[derive(Debug, Error)]
pub enum TradeParseError {
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid price '{value}': {reason}")]
    InvalidPrice { value: String, reason: String },
    #[error("invalid size '{value}': {reason}")]
    InvalidSize { value: String, reason: String },
    #[error("invalid side '{0}'")]
    InvalidSide(String),
    #[error("invalid timestamp {0}")]
    InvalidTimestamp(i64),
}

// ── JSON DTOs ─────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct TradeResponse {
    data: Vec<RawTrade>,
}

#[derive(Deserialize)]
struct RawTrade {
    id: String,
    market: String,
    side: String,
    /// Decimal string (e.g. `"100"`) — contracts/size.
    size: String,
    /// Decimal string in [0, 1] (e.g. `"0.65"`) — yes-outcome price.
    price: String,
    /// Unix timestamp in **milliseconds** if > 9_999_999_999, otherwise seconds.
    timestamp: i64,
    /// Polymarket token ID for the outcome; maps to OutcomeId 0 (YES) when absent.
    #[serde(default)]
    asset_id: String,
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Parse a raw `UserTrades` response for `wallet` into a vec of [`IncomingTrade`]s.
///
/// Trades whose price or size cannot be parsed are skipped with a warning log; other
/// parse errors return `Err`.
pub fn parse_trades(
    bytes: &[u8],
    wallet: pe_core_types::WalletAddress,
) -> Result<Vec<IncomingTrade>, TradeParseError> {
    let response: TradeResponse = serde_json::from_slice(bytes)?;
    let now = OffsetDateTime::now_utc();

    let mut out = Vec::with_capacity(response.data.len());
    for raw in response.data {
        match convert_trade(raw, wallet, now) {
            Ok(t) => out.push(t),
            Err(e) => tracing::warn!(error = %e, "skipping unparseable trade"),
        }
    }
    Ok(out)
}

fn convert_trade(
    raw: RawTrade,
    wallet: pe_core_types::WalletAddress,
    received_at: OffsetDateTime,
) -> Result<IncomingTrade, TradeParseError> {
    let price_decimal: Decimal =
        raw.price
            .parse()
            .map_err(|e: rust_decimal::Error| TradeParseError::InvalidPrice {
                value: raw.price.clone(),
                reason: e.to_string(),
            })?;
    let price = Price(price_decimal);

    let size_decimal: Decimal =
        raw.size
            .parse()
            .map_err(|e: rust_decimal::Error| TradeParseError::InvalidSize {
                value: raw.size.clone(),
                reason: e.to_string(),
            })?;
    // Floor to integer contracts (fractional contracts are not valid).
    let contracts = size_decimal
        .floor()
        .to_u64()
        .map(ContractQty)
        .ok_or_else(|| TradeParseError::InvalidSize {
            value: raw.size.clone(),
            reason: "out of u64 range".to_owned(),
        })?;

    let side = match raw.side.to_uppercase().as_str() {
        "BUY" => Side::Buy,
        "SELL" => Side::Sell,
        other => return Err(TradeParseError::InvalidSide(other.to_owned())),
    };

    // Normalise to seconds; Polymarket sometimes uses milliseconds.
    let ts_secs = if raw.timestamp > 9_999_999_999 {
        raw.timestamp / 1_000
    } else {
        raw.timestamp
    };
    let observed_at = OffsetDateTime::from_unix_timestamp(ts_secs)
        .map_err(|_| TradeParseError::InvalidTimestamp(ts_secs))?;

    // Outcome: Polymarket YES token → 0, NO token → 1, unknown → 0.
    let outcome_id = if raw.asset_id.is_empty() {
        OutcomeId(0)
    } else {
        raw.asset_id
            .parse::<u8>()
            .map(OutcomeId)
            .unwrap_or(OutcomeId(0))
    };

    Ok(IncomingTrade {
        wallet,
        market_id: MarketId(VenueMarketId(raw.market)),
        outcome_id,
        side,
        price,
        contracts,
        observed_at,
        received_at,
        source_trade_id: SourceTradeId(raw.id),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn dummy_wallet() -> pe_core_types::WalletAddress {
        serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
    }

    #[test]
    fn parses_valid_trade() {
        let json = br#"{"data":[{"id":"t1","market":"mkt_abc","side":"BUY","size":"50","price":"0.65","timestamp":1704067200}]}"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].contracts.0, 50);
        assert_eq!(trades[0].side, Side::Buy);
        assert_eq!(trades[0].market_id.0.0, "mkt_abc");
    }

    #[test]
    fn millisecond_timestamp_normalised() {
        let json = br#"{"data":[{"id":"t2","market":"m","side":"SELL","size":"10","price":"0.40","timestamp":1704067200000}]}"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert_eq!(trades.len(), 1);
        // timestamp normalised to seconds
        assert_eq!(trades[0].observed_at.unix_timestamp(), 1_704_067_200);
    }

    #[test]
    fn empty_response() {
        let json = br#"{"data":[]}"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert!(trades.is_empty());
    }
}
