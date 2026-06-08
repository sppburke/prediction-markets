//! Chainlink RTDS feed task: subscribes to `crypto_prices_chainlink` (`btc/usd`,
//! the 5m/15m settling value) and forwards raw frames to the join loop.
//!
//! [`parse_chainlink_frame`] is pure and gate-tested. The exact RTDS frame shape
//! and subscribe payload are re-verified in the manual live smoke (issue AC3(b)).

use std::str::FromStr as _;

use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::types::{BtcUsdPrice, ChainlinkTick, DecodeError, FeedFrame, FeedSource, now_unix_ms};
use crate::ws::ws_reconnect_loop;

#[derive(Debug, Deserialize)]
struct ChainlinkFrame {
    symbol: String,
    /// Source observation time, epoch milliseconds (re-verify unit in live smoke).
    timestamp: i64,
    /// BTC/USD value as a decimal string (never `f64`).
    value: String,
}

/// Decode one RTDS `crypto_prices_chainlink` text frame. Pure.
pub fn parse_chainlink_frame(raw: &str) -> Result<ChainlinkTick, DecodeError> {
    let f: ChainlinkFrame =
        serde_json::from_str(raw).map_err(|e| DecodeError::Json(e.to_string()))?;
    let value = Decimal::from_str(&f.value).map_err(|_| DecodeError::Decimal(f.value.clone()))?;
    Ok(ChainlinkTick {
        symbol: f.symbol,
        observed_at_ms: f.timestamp,
        value: BtcUsdPrice(value),
    })
}

/// RTDS subscribe payload for the BTC/USD Chainlink stream.
pub fn subscribe_message() -> String {
    r#"{"action":"subscribe","subscriptions":[{"topic":"crypto_prices_chainlink","symbol":"btc/usd"}]}"#
        .to_string()
}

/// Spawn the Chainlink WS task. Forwards each raw text frame as a [`FeedFrame`]
/// to `tx`; on a full channel it drops the frame (declared backpressure:
/// drop-newest) and on a closed channel it stops.
pub fn spawn(ws_url: String, tx: mpsc::Sender<FeedFrame>) -> JoinHandle<()> {
    tokio::spawn(async move {
        ws_reconnect_loop(ws_url, subscribe_message(), move |raw| {
            let frame = FeedFrame {
                source: FeedSource::Chainlink,
                received_ms: now_unix_ms(),
                raw: raw.to_string(),
            };
            match tx.try_send(frame) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!("chainlink: channel full, dropping frame");
                    true
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        })
        .await;
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn decodes_well_formed_frame() {
        let raw = r#"{"symbol":"btc/usd","timestamp":1717848000123,"value":"60123.45"}"#;
        let tick = parse_chainlink_frame(raw).unwrap();
        assert_eq!(tick.symbol, "btc/usd");
        assert_eq!(tick.observed_at_ms, 1_717_848_000_123);
        assert_eq!(tick.value, BtcUsdPrice(dec!(60123.45)));
    }

    #[test]
    fn rejects_non_decimal_value() {
        let raw = r#"{"symbol":"btc/usd","timestamp":1,"value":"NaN"}"#;
        assert!(matches!(
            parse_chainlink_frame(raw),
            Err(DecodeError::Decimal(_))
        ));
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(matches!(
            parse_chainlink_frame("{not json"),
            Err(DecodeError::Json(_))
        ));
    }
}
