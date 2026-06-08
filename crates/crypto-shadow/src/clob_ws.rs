//! CLOB market WS task: subscribes to the YES token ids and forwards raw book
//! frames to the join loop.
//!
//! [`parse_clob_frame`] is pure and gate-tested. Only `book` snapshots are
//! decoded for best bid/ask; incremental `price_change` handling is deferred —
//! `raw_ticks` preserves every frame for offline recompute. The exact frame
//! shape and subscribe payload are re-verified in the manual live smoke (AC3(b)).

use std::str::FromStr as _;

use pe_core_types::Price;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::warn;

use crate::types::{BookUpdate, DecodeError, FeedFrame, FeedSource, now_unix_ms};
use crate::ws::ws_reconnect_loop;

#[derive(Debug, Deserialize)]
struct ClobLevel {
    price: String,
}

#[derive(Debug, Deserialize)]
struct ClobFrame {
    event_type: String,
    asset_id: String,
    #[serde(default)]
    bids: Vec<ClobLevel>,
    #[serde(default)]
    asks: Vec<ClobLevel>,
    #[serde(default)]
    timestamp: Option<String>,
}

fn best_price(levels: &[ClobLevel], want_highest: bool) -> Option<Price> {
    let mut best: Option<Price> = None;
    for level in levels {
        let Ok(d) = Decimal::from_str(&level.price) else {
            continue;
        };
        let Ok(p) = Price::new(d) else {
            continue;
        };
        best = Some(match best {
            None => p,
            Some(cur) if (want_highest && p > cur) || (!want_highest && p < cur) => p,
            Some(cur) => cur,
        });
    }
    best
}

/// Decode a CLOB market text frame (object or array) into book updates. Pure.
/// Non-`book` events are skipped.
pub fn parse_clob_frame(raw: &str) -> Result<Vec<BookUpdate>, DecodeError> {
    let value: Value = serde_json::from_str(raw).map_err(|e| DecodeError::Json(e.to_string()))?;
    let frames: Vec<ClobFrame> = match &value {
        Value::Array(_) => {
            serde_json::from_value(value).map_err(|e| DecodeError::Json(e.to_string()))?
        }
        Value::Object(_) => {
            let f: ClobFrame =
                serde_json::from_value(value).map_err(|e| DecodeError::Json(e.to_string()))?;
            vec![f]
        }
        _ => return Err(DecodeError::Json("expected object or array".to_string())),
    };

    let mut out = Vec::new();
    for f in frames {
        if f.event_type != "book" {
            continue;
        }
        // Absent/unparseable timestamp -> None, so the join produces a null lag
        // rather than a spurious epoch-sized one (the frame shape is unverified
        // until the live smoke; a field-name mismatch must degrade safely).
        let observed_at_ms = f.timestamp.as_deref().and_then(|t| t.parse::<i64>().ok());
        out.push(BookUpdate {
            token_id: f.asset_id,
            best_bid: best_price(&f.bids, true),
            best_ask: best_price(&f.asks, false),
            observed_at_ms,
        });
    }
    Ok(out)
}

/// CLOB `market` subscribe payload for the given YES token ids.
pub fn subscribe_message(token_ids: &[String]) -> String {
    let assets = serde_json::to_string(token_ids).unwrap_or_else(|_| "[]".to_string());
    format!(r#"{{"type":"market","assets_ids":{assets}}}"#)
}

/// Spawn the CLOB market WS task over the given YES token ids. Same backpressure
/// (drop-newest on full) and stop-on-closed semantics as the Chainlink task.
pub fn spawn(
    ws_url: String,
    token_ids: Vec<String>,
    tx: mpsc::Sender<FeedFrame>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let subscribe = subscribe_message(&token_ids);
        ws_reconnect_loop(ws_url, subscribe, move |raw| {
            let frame = FeedFrame {
                source: FeedSource::Clob,
                received_ms: now_unix_ms(),
                raw: raw.to_string(),
            };
            match tx.try_send(frame) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!("clob: channel full, dropping frame");
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

    const BOOK: &str = r#"{"event_type":"book","asset_id":"0xyes",
        "bids":[{"price":"0.47","size":"10"},{"price":"0.48","size":"5"}],
        "asks":[{"price":"0.53","size":"10"},{"price":"0.52","size":"5"}],
        "timestamp":"1717848000123"}"#;

    #[test]
    fn decodes_best_bid_and_ask() {
        let updates = parse_clob_frame(BOOK).unwrap();
        assert_eq!(updates.len(), 1);
        let u = &updates[0];
        assert_eq!(u.token_id, "0xyes");
        assert_eq!(u.best_bid, Some(Price(dec!(0.48)))); // highest bid
        assert_eq!(u.best_ask, Some(Price(dec!(0.52)))); // lowest ask
        assert_eq!(u.observed_at_ms, Some(1_717_848_000_123));
    }

    #[test]
    fn decodes_array_of_frames() {
        let raw = format!("[{BOOK}]");
        let updates = parse_clob_frame(&raw).unwrap();
        assert_eq!(updates.len(), 1);
    }

    #[test]
    fn skips_non_book_events() {
        let raw = r#"{"event_type":"price_change","asset_id":"0xyes"}"#;
        assert!(parse_clob_frame(raw).unwrap().is_empty());
    }

    #[test]
    fn one_sided_book_yields_partial_quote() {
        let raw = r#"{"event_type":"book","asset_id":"0xyes","bids":[{"price":"0.40"}],"asks":[]}"#;
        let u = &parse_clob_frame(raw).unwrap()[0];
        assert_eq!(u.best_bid, Some(Price(dec!(0.40))));
        assert_eq!(u.best_ask, None);
    }

    #[test]
    fn missing_timestamp_yields_none_not_zero() {
        let raw = r#"{"event_type":"book","asset_id":"0xyes","bids":[{"price":"0.40"}],"asks":[{"price":"0.60"}]}"#;
        let u = &parse_clob_frame(raw).unwrap()[0];
        assert_eq!(u.observed_at_ms, None);
    }

    #[test]
    fn subscribe_message_lists_assets() {
        let msg = subscribe_message(&["a".to_string(), "b".to_string()]);
        assert!(msg.contains("\"assets_ids\":[\"a\",\"b\"]"));
        assert!(msg.contains("\"type\":\"market\""));
    }
}
