//! CLOB market WS task: subscribes to the YES token ids and forwards raw book
//! frames to the join loop.
//!
//! [`parse_clob_frame`] is pure and gate-tested. It decodes the two real CLOB
//! shapes by **structure**, with no `event_type` tag (issue #300 fix 4): an
//! array of per-asset book elements carrying `bids`/`asks` (the initial
//! snapshot), and a `price_change` object carrying a `price_changes` array with
//! per-asset `best_bid`/`best_ask` (the dominant live source). Every other CLOB
//! message type is skipped, and `raw_ticks` preserves every frame for offline
//! recompute. The shapes were captured from a live run (AC3(b)).

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

/// One element of an initial book-snapshot array: a full bid/ask ladder for a
/// single asset, carrying its own source `timestamp`.
#[derive(Debug, Deserialize)]
struct BookSnapshotElem {
    asset_id: String,
    #[serde(default)]
    bids: Vec<ClobLevel>,
    #[serde(default)]
    asks: Vec<ClobLevel>,
    #[serde(default)]
    timestamp: Option<String>,
}

/// A `price_change` envelope: per-asset best-bid/ask deltas. Carries no
/// timestamp, so each resulting [`BookUpdate`] has `observed_at_ms = None`.
#[derive(Debug, Deserialize)]
struct PriceChangeEnvelope {
    #[serde(default)]
    price_changes: Vec<PriceChangeEntry>,
}

/// One per-asset entry of a `price_change` frame. `best_bid`/`best_ask` are
/// decimal strings; either may be absent (yielding `None` on that side, never a
/// zero quote).
#[derive(Debug, Deserialize)]
struct PriceChangeEntry {
    asset_id: String,
    #[serde(default)]
    best_bid: Option<String>,
    #[serde(default)]
    best_ask: Option<String>,
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

/// Parse an optional decimal-string quote into a [`Price`]. `None` when absent
/// or out of the `[0, 1]` price range — mirroring `best_price` returning `None`
/// on an empty ladder, never a spurious zero quote.
fn parse_price_opt(raw: Option<&str>) -> Option<Price> {
    let d = Decimal::from_str(raw?).ok()?;
    Price::new(d).ok()
}

/// Decode a CLOB market text frame into book updates by **structure** (no
/// `event_type` tag). Pure.
///
/// - A JSON **array** whose elements carry `bids`/`asks` is an initial book
///   snapshot → one [`BookUpdate`] per element (best bid = highest, best ask =
///   lowest, `observed_at_ms` from the element's `timestamp` string).
/// - A JSON **object** carrying a `price_changes` array is a `price_change`
///   frame → one [`BookUpdate`] per entry, reading the per-asset `best_bid`/
///   `best_ask` strings directly (`observed_at_ms = None`).
/// - Every other message type (`tick_size_change`, `last_trade_price`, or any
///   shape carrying neither `bids`/`asks` nor `price_changes`) yields no update.
pub fn parse_clob_frame(raw: &str) -> Result<Vec<BookUpdate>, DecodeError> {
    let value: Value = serde_json::from_str(raw).map_err(|e| DecodeError::Json(e.to_string()))?;
    match value {
        // Initial book snapshot: an array of per-asset ladders.
        Value::Array(elems) => {
            let mut out = Vec::new();
            for el in elems {
                // Only elements carrying a book ladder are snapshot rows; any
                // other array element is a non-book message → skip.
                if el.get("bids").is_none() && el.get("asks").is_none() {
                    continue;
                }
                let elem: BookSnapshotElem =
                    serde_json::from_value(el).map_err(|e| DecodeError::Json(e.to_string()))?;
                let observed_at_ms = elem
                    .timestamp
                    .as_deref()
                    .and_then(|t| t.parse::<i64>().ok());
                out.push(BookUpdate {
                    token_id: elem.asset_id,
                    best_bid: best_price(&elem.bids, true),
                    best_ask: best_price(&elem.asks, false),
                    observed_at_ms,
                });
            }
            Ok(out)
        }
        // price_change: an object carrying a `price_changes` array.
        Value::Object(map) if map.contains_key("price_changes") => {
            let env: PriceChangeEnvelope = serde_json::from_value(Value::Object(map))
                .map_err(|e| DecodeError::Json(e.to_string()))?;
            let mut out = Vec::with_capacity(env.price_changes.len());
            for pc in env.price_changes {
                out.push(BookUpdate {
                    token_id: pc.asset_id,
                    best_bid: parse_price_opt(pc.best_bid.as_deref()),
                    best_ask: parse_price_opt(pc.best_ask.as_deref()),
                    observed_at_ms: None, // price_change frames carry no timestamp
                });
            }
            Ok(out)
        }
        // tick_size_change, last_trade_price, or any other shape: no book update.
        _ => Ok(Vec::new()),
    }
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

    // Captured initial book-snapshot shape: an array of per-asset ladders with
    // no `event_type` (issue #300 fix 4), seeded from a live `raw_ticks` frame.
    const BOOK: &str = r#"[{"market":"0xmkt","asset_id":"0xyes","hash":"h",
        "bids":[{"price":"0.47","size":"10"},{"price":"0.48","size":"5"}],
        "asks":[{"price":"0.53","size":"10"},{"price":"0.52","size":"5"}],
        "timestamp":"1717848000123"}]"#;

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
    fn decodes_multi_asset_snapshot() {
        // A snapshot array yields one BookUpdate per asset element.
        let raw = r#"[
          {"asset_id":"a","bids":[{"price":"0.40"}],"asks":[{"price":"0.60"}],"timestamp":"1"},
          {"asset_id":"b","bids":[{"price":"0.30"}],"asks":[{"price":"0.70"}],"timestamp":"2"}
        ]"#;
        let updates = parse_clob_frame(raw).unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].token_id, "a");
        assert_eq!(updates[1].token_id, "b");
        assert_eq!(updates[1].best_ask, Some(Price(dec!(0.70))));
    }

    #[test]
    fn price_change_yields_book_update() {
        // The dominant live frame: per-asset best_bid/best_ask, no timestamp.
        let raw = r#"{"market":"0xmkt","price_changes":[
          {"asset_id":"0xyes","price":"0.4","size":"353","side":"BUY","hash":"h",
           "best_bid":"0.5","best_ask":"0.51"}
        ]}"#;
        let updates = parse_clob_frame(raw).unwrap();
        assert_eq!(updates.len(), 1);
        let u = &updates[0];
        assert_eq!(u.token_id, "0xyes");
        assert_eq!(u.best_bid, Some(Price(dec!(0.5))));
        assert_eq!(u.best_ask, Some(Price(dec!(0.51))));
        assert_eq!(u.observed_at_ms, None); // price_change carries no timestamp
    }

    #[test]
    fn price_change_missing_side_yields_none_not_zero() {
        let raw = r#"{"price_changes":[{"asset_id":"0xyes","best_bid":"0.5"}]}"#;
        let u = &parse_clob_frame(raw).unwrap()[0];
        assert_eq!(u.best_bid, Some(Price(dec!(0.5))));
        assert_eq!(u.best_ask, None); // absent side -> None, never a zero quote
    }

    #[test]
    fn skips_non_book_message_types() {
        // tick_size_change object: neither bids/asks nor price_changes.
        let tsc = r#"{"event_type":"tick_size_change","asset_id":"0xyes","new_tick_size":"0.001"}"#;
        assert!(parse_clob_frame(tsc).unwrap().is_empty());
        // An array element carrying neither bids nor asks is skipped too.
        let arr = r#"[{"asset_id":"0xyes","foo":"bar"}]"#;
        assert!(parse_clob_frame(arr).unwrap().is_empty());
    }

    #[test]
    fn one_sided_book_yields_partial_quote() {
        let raw = r#"[{"asset_id":"0xyes","bids":[{"price":"0.40"}],"asks":[]}]"#;
        let u = &parse_clob_frame(raw).unwrap()[0];
        assert_eq!(u.best_bid, Some(Price(dec!(0.40))));
        assert_eq!(u.best_ask, None);
    }

    #[test]
    fn missing_timestamp_yields_none_not_zero() {
        let raw = r#"[{"asset_id":"0xyes","bids":[{"price":"0.40"}],"asks":[{"price":"0.60"}]}]"#;
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
