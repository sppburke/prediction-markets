//! Chainlink RTDS settlement-feed task: subscribes to the `crypto_prices` topic
//! (Chainlink is a *source* within it) for `btc/usd` — the value the 5m/15m
//! markets settle on — and forwards raw frames to the join loop.
//!
//! **Issue #300 fix 3 (AC2.2):** the previous subscribe (topic
//! `crypto_prices_chainlink` with a bare `symbol`) was rejected
//! (`Invalid request body`). The live RTDS protocol (captured during the #300
//! Phase-2 discovery run) is: topic `crypto_prices`, `type:"update"` required,
//! and the symbol set under `filters` **as a stringified JSON object** (the
//! server regex rejects a bare string). The tick fields live under `payload`,
//! with the exact value in `payload.full_accuracy_value` (a decimal string) and
//! the time in `payload.timestamp` (epoch ms).
//!
//! The decoder is pure and gate-tested. Live data needs a sponsored Chainlink
//! key (AC2.3, deferred); until then this captures only `raw_ticks` and does not
//! drive observations — the observation trigger is the exchange-consensus median
//! ([`crate::exchange_ws`] / [`crate::consensus`]).

use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::types::{BtcUsdPrice, ChainlinkTick, DecodeError, FeedFrame, FeedSource, now_unix_ms};
use crate::ws::ws_reconnect_loop;

/// RTDS frame envelope: the tick fields live under `payload`.
#[derive(Debug, Deserialize)]
struct ChainlinkFrame {
    payload: ChainlinkPayload,
}

/// The `payload` of a `crypto_prices` Chainlink update.
#[derive(Debug, Deserialize)]
struct ChainlinkPayload {
    #[serde(default)]
    symbol: String,
    /// Source observation time, epoch milliseconds.
    timestamp: i64,
    /// Exact BTC/USD value as a decimal string (never `f64`).
    full_accuracy_value: String,
}

/// Decode one RTDS `crypto_prices` Chainlink text frame. Pure. Non-payload
/// frames (subscribe acks, heartbeats) fail to deserialize and surface as a
/// [`DecodeError`]; the caller tallies these and persists every raw frame.
pub fn parse_chainlink_frame(raw: &str) -> Result<ChainlinkTick, DecodeError> {
    let f: ChainlinkFrame =
        serde_json::from_str(raw).map_err(|e| DecodeError::Json(e.to_string()))?;
    let value = Decimal::from_str(&f.payload.full_accuracy_value)
        .map_err(|_| DecodeError::Decimal(f.payload.full_accuracy_value.clone()))?;
    Ok(ChainlinkTick {
        symbol: f.payload.symbol,
        observed_at_ms: f.payload.timestamp,
        value: BtcUsdPrice(value),
    })
}

/// RTDS subscribe payload for the BTC/USD Chainlink stream. `filters` is a
/// **stringified** JSON object (the server rejects a bare string), built via
/// `serde_json` so the escaping is correct by construction.
pub fn subscribe_message() -> String {
    let filters = serde_json::json!({ "symbol": "btc/usd" }).to_string();
    serde_json::json!({
        "action": "subscribe",
        "subscriptions": [{
            "topic": "crypto_prices",
            "type": "update",
            "filters": filters,
        }],
    })
    .to_string()
}

/// Spawn the Chainlink WS task. Forwards each raw text frame as a [`FeedFrame`]
/// to `tx`; on a full channel it drops the frame (declared backpressure:
/// drop-newest; counted into `frames_dropped`, not logged per-frame — the
/// runner surfaces the tally periodically + in `meta`, issue #311) and on a
/// closed channel it stops.
pub fn spawn(
    ws_url: String,
    tx: mpsc::Sender<FeedFrame>,
    frames_dropped: Arc<AtomicU64>,
) -> JoinHandle<()> {
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
                    frames_dropped.fetch_add(1, Ordering::Relaxed);
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
    fn subscribe_payload_has_corrected_shape() {
        let msg = subscribe_message();
        assert!(msg.contains("\"topic\":\"crypto_prices\""));
        assert!(msg.contains("\"type\":\"update\""));
        // filters is a stringified JSON object, so the inner quotes are escaped.
        assert!(msg.contains("\"filters\":\"{\\\"symbol\\\":\\\"btc/usd\\\"}\""));
        assert!(!msg.contains("crypto_prices_chainlink"));
    }

    #[test]
    fn decodes_nested_payload_frame() {
        // Captured live shape: fields under `payload`, exact value as a string.
        let raw = r#"{"topic":"crypto_prices","type":"update",
          "payload":{"symbol":"btc/usd","timestamp":1717848000123,"value":60123.45,
                     "full_accuracy_value":"60123.45000000"}}"#;
        let tick = parse_chainlink_frame(raw).unwrap();
        assert_eq!(tick.symbol, "btc/usd");
        assert_eq!(tick.observed_at_ms, 1_717_848_000_123);
        assert_eq!(tick.value, BtcUsdPrice(dec!(60123.45000000)));
    }

    #[test]
    fn rejects_non_decimal_value() {
        let raw = r#"{"payload":{"symbol":"btc/usd","timestamp":1,"full_accuracy_value":"NaN"}}"#;
        assert!(matches!(
            parse_chainlink_frame(raw),
            Err(DecodeError::Decimal(_))
        ));
    }

    #[test]
    fn rejects_frame_without_payload() {
        // A subscribe ack has no `payload` → a decode error (tallied, not fatal).
        assert!(matches!(
            parse_chainlink_frame(r#"{"type":"subscribed","topic":"crypto_prices"}"#),
            Err(DecodeError::Json(_))
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
