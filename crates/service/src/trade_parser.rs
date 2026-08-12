//! Parse Polymarket `UserTradeActivity` API responses into typed [`IncomingTrade`]s.

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
    #[error("invalid size '{value}': {reason}")]
    InvalidSize { value: String, reason: String },
    #[error("invalid side '{0}'")]
    InvalidSide(String),
    #[error("invalid timestamp {0}")]
    InvalidTimestamp(i64),
    #[error("trade omitted outcomeIndex")]
    MissingOutcomeIndex,
}

// ── JSON DTOs ─────────────────────────────────────────────────────────────────

// GET /activity?user=<wallet>&type=TRADE returns a JSON array directly (no wrapper object).
type TradeResponse = Vec<RawTrade>;

// Field names match the camelCase keys returned by GET /activity?type=TRADE.
// size and price arrive as JSON numbers; rust_decimal's serde feature handles
// both number and string representations.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTrade {
    /// Unique trade identifier (on-chain tx hash).
    transaction_hash: String,
    /// Market condition ID (hex string).
    condition_id: String,
    side: String,
    /// Number of contracts (integral in practice; floored before use).
    size: Decimal,
    /// Yes-outcome price in [0, 1].
    price: Decimal,
    /// Unix timestamp in seconds or milliseconds — normalised below.
    timestamp: i64,
    /// Outcome index: 0 = YES, 1 = NO. Older records may omit it; ordinary ingestion preserves
    /// its historical outcome-zero default while the canary's strict parser rejects the omission.
    #[serde(default)]
    outcome_index: Option<u16>,
}

// ── Parser ────────────────────────────────────────────────────────────────────

/// Parse a raw `UserTradeActivity` response for `wallet` into a vec of [`IncomingTrade`]s.
///
/// Trades whose size cannot be converted to u64 are skipped with a warning log;
/// other parse errors return `Err`.
pub fn parse_trades(
    bytes: &[u8],
    wallet: pe_core_types::WalletAddress,
) -> Result<Vec<IncomingTrade>, TradeParseError> {
    parse_trades_counted(bytes, wallet).map(|(trades, _)| trades)
}

/// [`parse_trades`] that also reports how many rows were dropped as unparseable (#511):
/// a dropped row is a trade the poller can never deliver or hold for, so the caller must
/// FREEZE the cursor rather than advance over it (advancing would be the same silent-loss
/// class the held cursor exists to close).
pub fn parse_trades_counted(
    bytes: &[u8],
    wallet: pe_core_types::WalletAddress,
) -> Result<(Vec<IncomingTrade>, usize), TradeParseError> {
    let response: TradeResponse = serde_json::from_slice(bytes)?;
    let now = OffsetDateTime::now_utc();

    let mut out = Vec::with_capacity(response.len());
    let mut malformed = 0usize;
    for raw in response {
        match convert_trade(raw, wallet, now) {
            Ok(t) => out.push(t),
            Err(e) => {
                malformed += 1;
                tracing::warn!(error = %e, "unparseable trade (cursor will freeze)");
            }
        }
    }
    Ok((out, malformed))
}

/// Parse a live-canary page without dropping an individual malformed trade. Ordinary paper
/// ingestion keeps its warn-and-skip posture; a canary wallet is unavailable unless its complete
/// page can be reconstructed.
pub fn parse_trades_strict(
    bytes: &[u8],
    wallet: pe_core_types::WalletAddress,
) -> Result<Vec<IncomingTrade>, TradeParseError> {
    let response: TradeResponse = serde_json::from_slice(bytes)?;
    let now = OffsetDateTime::now_utc();
    response
        .into_iter()
        .map(|raw| {
            if raw.outcome_index.is_none() {
                return Err(TradeParseError::MissingOutcomeIndex);
            }
            convert_trade(raw, wallet, now)
        })
        .collect()
}

fn convert_trade(
    raw: RawTrade,
    wallet: pe_core_types::WalletAddress,
    received_at: OffsetDateTime,
) -> Result<IncomingTrade, TradeParseError> {
    let price = Price(raw.price);

    // Floor to integer contracts (fractional contracts are not valid).
    let contracts =
        raw.size
            .floor()
            .to_u64()
            .map(ContractQty)
            .ok_or_else(|| TradeParseError::InvalidSize {
                value: raw.size.to_string(),
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

    let outcome_id = OutcomeId(raw.outcome_index.unwrap_or(0));

    Ok(IncomingTrade {
        wallet,
        market_id: MarketId(VenueMarketId(raw.condition_id)),
        outcome_id,
        side,
        price,
        contracts,
        observed_at,
        received_at,
        source_trade_id: SourceTradeId(raw.transaction_hash),
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
        let json = br#"[{"transactionHash":"0xabc","conditionId":"0xcond","side":"BUY","size":50,"price":0.65,"timestamp":1704067200,"outcomeIndex":0}]"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].contracts.0, 50);
        assert_eq!(trades[0].side, Side::Buy);
        assert_eq!(trades[0].market_id.0.0, "0xcond");
        assert_eq!(trades[0].source_trade_id.0, "0xabc");
    }

    #[test]
    fn millisecond_timestamp_normalised() {
        let json = br#"[{"transactionHash":"0xabc","conditionId":"0xcond","side":"SELL","size":10,"price":0.40,"timestamp":1704067200000,"outcomeIndex":0}]"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].observed_at.unix_timestamp(), 1_704_067_200);
    }

    #[test]
    fn empty_response() {
        let json = br#"[]"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn outcome_index_propagated() {
        let json = br#"[{"transactionHash":"0xabc","conditionId":"0xcond","side":"BUY","size":5,"price":0.55,"timestamp":1704067200,"outcomeIndex":1}]"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert_eq!(trades[0].outcome_id, OutcomeId(1));
    }

    #[test]
    fn ordinary_missing_outcome_index_keeps_legacy_default() {
        let json = br#"[{"transactionHash":"0xabc","conditionId":"0xcond","side":"BUY","size":5,"price":0.55,"timestamp":1704067200}]"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert_eq!(trades[0].outcome_id, OutcomeId(0));
        assert!(matches!(
            parse_trades_strict(json, dummy_wallet()),
            Err(TradeParseError::MissingOutcomeIndex)
        ));
    }

    #[test]
    fn strict_parser_rejects_an_individually_invalid_trade() {
        let json = br#"[{"transactionHash":"0xabc","conditionId":"0xcond","side":"INVALID","size":10,"price":0.40,"timestamp":1704067200,"outcomeIndex":0}]"#;
        assert!(matches!(
            parse_trades_strict(json, dummy_wallet()),
            Err(TradeParseError::InvalidSide(_))
        ));
        assert!(parse_trades(json, dummy_wallet()).unwrap().is_empty());
    }

    // Issue #159: outcomeIndex > 255 must parse, matching the bootstrap-side DTO.
    #[test]
    fn outcome_index_above_u8_max_propagated() {
        let json = br#"[{"transactionHash":"0xabc","conditionId":"0xcond","side":"BUY","size":5,"price":0.55,"timestamp":1704067200,"outcomeIndex":999}]"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert_eq!(trades[0].outcome_id, OutcomeId(999));
    }
}
