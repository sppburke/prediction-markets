//! Parse Polymarket trade observations into typed [`IncomingTrade`]s.
//!
//! ONE normalization path serves both transports (#530): the REST
//! `/activity?type=TRADE` poller and the live-data activity websocket carry the
//! same camelCase payload shape, so both funnel through [`convert_trade`] and
//! produce identical trades by construction — the transport differs only in
//! [`TradeProvenance`]. The websocket sends its numerics as JSON strings where
//! REST sends numbers; the flexible deserializers below accept both.

use pe_copy_signal_engine::{IncomingTrade, TradeProvenance};
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTradeId, VenueMarketId, WalletAddress,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::{Deserialize, Deserializer};
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
    #[error("invalid proxyWallet '{value}'")]
    InvalidWallet { value: String },
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
    /// Unix timestamp in seconds or milliseconds — normalised below. REST sends
    /// a number; the websocket sends a decimal string; both accepted.
    #[serde(deserialize_with = "de_i64_flexible")]
    timestamp: i64,
    /// Outcome index: 0 = YES, 1 = NO. Older records may omit it; ordinary ingestion preserves
    /// its historical outcome-zero default while the canary's strict parser rejects the omission.
    /// REST sends a number; the websocket sends a decimal string; both accepted.
    #[serde(default, deserialize_with = "de_opt_u16_flexible")]
    outcome_index: Option<u16>,
    /// Trading wallet — present on websocket payloads; the REST caller already
    /// knows which wallet it polled, so it is optional here.
    #[serde(default)]
    proxy_wallet: Option<String>,
}

/// Accept an integer from either a JSON number or a decimal string.
fn de_i64_flexible<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Flex {
        Num(i64),
        Str(String),
    }
    match Flex::deserialize(d)? {
        Flex::Num(n) => Ok(n),
        Flex::Str(s) => s.trim().parse::<i64>().map_err(serde::de::Error::custom),
    }
}

/// Accept an optional u16 from either a JSON number or a decimal string.
fn de_opt_u16_flexible<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u16>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Flex {
        Num(u16),
        Str(String),
    }
    match Option::<Flex>::deserialize(d)? {
        None => Ok(None),
        Some(Flex::Num(n)) => Ok(Some(n)),
        Some(Flex::Str(s)) => s
            .trim()
            .parse::<u16>()
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
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

/// Parse one websocket activity payload (a single JSON object, not an array)
/// into an [`IncomingTrade`] with [`TradeProvenance::ActivityWs`].
///
/// The wallet comes from the payload's own `proxyWallet` (the websocket is a
/// platform-wide firehose; there is no per-wallet request context). All other
/// normalization is byte-identical to the REST path via [`convert_trade`].
pub fn parse_ws_trade(payload: &[u8]) -> Result<IncomingTrade, TradeParseError> {
    let raw: RawTrade = serde_json::from_slice(payload)?;
    let wallet_hex = raw.proxy_wallet.clone().unwrap_or_default();
    let wallet = WalletAddress::from_hex(&wallet_hex)
        .map_err(|_| TradeParseError::InvalidWallet { value: wallet_hex })?;
    let mut trade = convert_trade(raw, wallet, OffsetDateTime::now_utc())?;
    trade.provenance = TradeProvenance::ActivityWs;
    Ok(trade)
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
        provenance: TradeProvenance::RestPoll,
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

    // #530: the SAME trade observed via websocket (string numerics, proxyWallet in
    // payload, extra UI fields) and via REST (numbers, wallet from request context)
    // must normalize identically — dedup-equivalence by construction.
    #[test]
    fn ws_payload_normalizes_identically_to_rest() {
        let ws = br#"{"proxyWallet":"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "conditionId":"0xcond","side":"BUY","size":"50.7","price":"0.65",
            "timestamp":"1704067200","transactionHash":"0xabc","outcomeIndex":"1",
            "fee":"0","eventSlug":"slug","title":"T","pseudonym":"p","bio":""}"#;
        let rest = br#"[{"transactionHash":"0xabc","conditionId":"0xcond","side":"BUY",
            "size":50.7,"price":0.65,"timestamp":1704067200,"outcomeIndex":1}]"#;
        let w = parse_ws_trade(ws).unwrap();
        let r = &parse_trades(rest, dummy_wallet()).unwrap()[0];
        assert_eq!(w.wallet, r.wallet);
        assert_eq!(w.market_id.0.0, r.market_id.0.0);
        assert_eq!(w.outcome_id, r.outcome_id);
        assert_eq!(w.side, r.side);
        assert_eq!(w.price, r.price);
        assert_eq!(w.contracts, r.contracts);
        assert_eq!(w.observed_at, r.observed_at);
        assert_eq!(w.source_trade_id, r.source_trade_id);
        assert_eq!(w.provenance, TradeProvenance::ActivityWs);
        assert_eq!(r.provenance, TradeProvenance::RestPoll);
    }

    #[test]
    fn ws_payload_without_valid_wallet_rejected() {
        let ws = br#"{"proxyWallet":"nonsense","conditionId":"0xcond","side":"BUY",
            "size":"1","price":"0.5","timestamp":"1704067200","transactionHash":"0xabc"}"#;
        assert!(matches!(
            parse_ws_trade(ws),
            Err(TradeParseError::InvalidWallet { .. })
        ));
        let ws_missing = br#"{"conditionId":"0xcond","side":"BUY","size":"1",
            "price":"0.5","timestamp":"1704067200","transactionHash":"0xabc"}"#;
        assert!(matches!(
            parse_ws_trade(ws_missing),
            Err(TradeParseError::InvalidWallet { .. })
        ));
    }

    // Issue #159: outcomeIndex > 255 must parse, matching the bootstrap-side DTO.
    #[test]
    fn outcome_index_above_u8_max_propagated() {
        let json = br#"[{"transactionHash":"0xabc","conditionId":"0xcond","side":"BUY","size":5,"price":0.55,"timestamp":1704067200,"outcomeIndex":999}]"#;
        let trades = parse_trades(json, dummy_wallet()).unwrap();
        assert_eq!(trades[0].outcome_id, OutcomeId(999));
    }
}
