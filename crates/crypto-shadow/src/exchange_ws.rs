//! Exchange trade/ticker WS tasks: subscribe to the free bybit/okx/coinbase BTC
//! spot streams and forward raw frames to the join loop, where the consensus
//! median move detector ([`crate::consensus`]) turns them into observations.
//!
//! [`parse_trade_frame`] is pure and gate-tested. The exact subscribe payloads
//! and trade-frame shapes are taken verbatim from the committed feed bake-off
//! collector (`scripts/feed-bakeoff/feed_bakeoff_v2.py`), which captured them
//! live during the 9 h run behind `docs/27`:
//! - **bybit** `wss://stream.bybit.com/v5/public/spot`, `publicTrade.BTCUSDT`,
//!   trades in `data[].p` (price) / `data[].T` (epoch-ms number).
//! - **okx** `wss://ws.okx.com:8443/ws/v5/public`, `trades`/`BTC-USDT`, in
//!   `data[].px` / `data[].ts` (epoch-ms string).
//! - **coinbase** `wss://ws-feed.exchange.coinbase.com`, `ticker`/`BTC-USD`,
//!   `price` (string) / `time` (RFC3339).
//!
//! Live-path only for the socket; the pure decoder below is what the CI gate
//! covers (no network).

use std::str::FromStr as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use rust_decimal::Decimal;
use serde_json::Value;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::types::{DecodeError, ExchangeTick, ExchangeVenue, FeedFrame, FeedSource, now_unix_ms};
use crate::ws::ws_reconnect_loop;

/// Subscribe payload for `venue`'s BTC spot trade/ticker stream.
pub fn subscribe_message(venue: ExchangeVenue) -> String {
    match venue {
        ExchangeVenue::Bybit => r#"{"op":"subscribe","args":["publicTrade.BTCUSDT"]}"#.to_string(),
        ExchangeVenue::Okx => {
            r#"{"op":"subscribe","args":[{"channel":"trades","instId":"BTC-USDT"}]}"#.to_string()
        }
        ExchangeVenue::Coinbase => {
            r#"{"type":"subscribe","product_ids":["BTC-USD"],"channels":["ticker"]}"#.to_string()
        }
    }
}

/// Read a decimal-string field into [`Decimal`]; never `f64`.
fn decimal_field(obj: &Value, key: &'static str) -> Result<Decimal, DecodeError> {
    let s = obj
        .get(key)
        .and_then(Value::as_str)
        .ok_or(DecodeError::Missing(key))?;
    Decimal::from_str(s).map_err(|_| DecodeError::Decimal(s.to_string()))
}

/// Read an epoch-ms field encoded as either a JSON number (bybit `T`) or a
/// numeric string (okx `ts`).
fn ms_field(obj: &Value, key: &'static str) -> Result<i64, DecodeError> {
    match obj.get(key) {
        Some(Value::Number(n)) => n.as_i64().ok_or(DecodeError::Missing(key)),
        Some(Value::String(s)) => s
            .parse::<i64>()
            .map_err(|_| DecodeError::Decimal(s.clone())),
        _ => Err(DecodeError::Missing(key)),
    }
}

/// Parse an RFC3339 timestamp (coinbase `time`) into epoch ms.
fn iso_to_ms(s: &str) -> Result<i64, DecodeError> {
    let odt =
        OffsetDateTime::parse(s, &Rfc3339).map_err(|_| DecodeError::Decimal(s.to_string()))?;
    i64::try_from(odt.unix_timestamp_nanos() / 1_000_000)
        .map_err(|_| DecodeError::Decimal(s.to_string()))
}

/// Decode one exchange WS text frame into the latest [`ExchangeTick`] it carries.
///
/// Returns `Ok(None)` for non-trade frames (subscribe acks, heartbeats) — these
/// are normal and not decode errors. For a frame that *is* a trade frame but is
/// malformed, returns `Err`. When a frame carries several trades, the **last**
/// (most recent) is taken, so one frame yields at most one median update. Pure.
pub fn parse_trade_frame(
    venue: ExchangeVenue,
    raw: &str,
) -> Result<Option<ExchangeTick>, DecodeError> {
    let v: Value = serde_json::from_str(raw).map_err(|e| DecodeError::Json(e.to_string()))?;
    match venue {
        ExchangeVenue::Bybit => last_array_tick(venue, &v, "p", "T"),
        ExchangeVenue::Okx => last_array_tick(venue, &v, "px", "ts"),
        ExchangeVenue::Coinbase => {
            if v.get("type").and_then(Value::as_str) != Some("ticker") {
                return Ok(None); // subscriptions / heartbeat / other
            }
            let price = decimal_field(&v, "price")?;
            let observed_at_ms = match v.get("time").and_then(Value::as_str) {
                Some(t) => iso_to_ms(t)?,
                None => return Err(DecodeError::Missing("time")),
            };
            Ok(Some(ExchangeTick {
                venue,
                price,
                observed_at_ms,
            }))
        }
    }
}

/// Shared bybit/okx decode: take the last entry of `data` that carries the
/// price field. `data` absent ⇒ not a trade frame ⇒ `Ok(None)`.
fn last_array_tick(
    venue: ExchangeVenue,
    v: &Value,
    price_key: &'static str,
    ts_key: &'static str,
) -> Result<Option<ExchangeTick>, DecodeError> {
    let Some(arr) = v.get("data").and_then(Value::as_array) else {
        return Ok(None);
    };
    let Some(last) = arr.iter().rev().find(|x| x.get(price_key).is_some()) else {
        return Ok(None);
    };
    let price = decimal_field(last, price_key)?;
    let observed_at_ms = ms_field(last, ts_key)?;
    Ok(Some(ExchangeTick {
        venue,
        price,
        observed_at_ms,
    }))
}

/// Spawn one exchange WS task. Same backpressure (drop-newest on full) and
/// stop-on-closed semantics as the other feed tasks. `frames_dropped` counts
/// frames lost to a full channel (counted, not logged per-frame — the runner
/// surfaces the tally periodically + in `meta`, issue #311).
pub fn spawn(
    venue: ExchangeVenue,
    ws_url: String,
    tx: mpsc::Sender<FeedFrame>,
    frames_dropped: Arc<AtomicU64>,
) -> JoinHandle<()> {
    let source = FeedSource::from(venue);
    tokio::spawn(async move {
        ws_reconnect_loop(ws_url, subscribe_message(venue), move |raw| {
            let frame = FeedFrame {
                source,
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
    fn subscribe_payloads_match_captured_shapes() {
        assert!(subscribe_message(ExchangeVenue::Bybit).contains("publicTrade.BTCUSDT"));
        assert!(subscribe_message(ExchangeVenue::Okx).contains("\"instId\":\"BTC-USDT\""));
        assert!(
            subscribe_message(ExchangeVenue::Coinbase).contains("\"product_ids\":[\"BTC-USD\"]")
        );
    }

    #[test]
    fn decodes_bybit_trade() {
        let raw = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1717848000200,
          "data":[{"T":1717848000123,"s":"BTCUSDT","S":"Buy","v":"0.01","p":"60123.4","L":"PlusTick","i":"x","BT":false}]}"#;
        let tick = parse_trade_frame(ExchangeVenue::Bybit, raw)
            .unwrap()
            .unwrap();
        assert_eq!(tick.venue, ExchangeVenue::Bybit);
        assert_eq!(tick.price, dec!(60123.4));
        assert_eq!(tick.observed_at_ms, 1_717_848_000_123);
    }

    #[test]
    fn decodes_okx_trade_with_string_ts() {
        let raw = r#"{"arg":{"channel":"trades","instId":"BTC-USDT"},
          "data":[{"instId":"BTC-USDT","tradeId":"1","px":"60100.5","sz":"0.1","side":"buy","ts":"1717848000123"}]}"#;
        let tick = parse_trade_frame(ExchangeVenue::Okx, raw).unwrap().unwrap();
        assert_eq!(tick.venue, ExchangeVenue::Okx);
        assert_eq!(tick.price, dec!(60100.5));
        assert_eq!(tick.observed_at_ms, 1_717_848_000_123);
    }

    #[test]
    fn decodes_coinbase_ticker() {
        let raw = r#"{"type":"ticker","sequence":1,"product_id":"BTC-USD",
          "price":"60050.25","time":"2024-06-08T12:00:00.123456Z","best_bid":"60050","best_ask":"60051"}"#;
        let tick = parse_trade_frame(ExchangeVenue::Coinbase, raw)
            .unwrap()
            .unwrap();
        assert_eq!(tick.venue, ExchangeVenue::Coinbase);
        assert_eq!(tick.price, dec!(60050.25));
        // 2024-06-08T12:00:00.123Z -> epoch ms ends in 123.
        assert_eq!(tick.observed_at_ms % 1000, 123);
    }

    #[test]
    fn takes_last_trade_in_a_multi_trade_frame() {
        let raw = r#"{"data":[{"p":"60000","T":1},{"p":"60100","T":2},{"p":"60200","T":3}]}"#;
        let tick = parse_trade_frame(ExchangeVenue::Bybit, raw)
            .unwrap()
            .unwrap();
        assert_eq!(tick.price, dec!(60200));
        assert_eq!(tick.observed_at_ms, 3);
    }

    #[test]
    fn subscribe_ack_is_not_a_tick_not_an_error() {
        // bybit/okx acks carry no `data`; coinbase sends a `subscriptions` frame.
        assert_eq!(
            parse_trade_frame(ExchangeVenue::Bybit, r#"{"success":true,"op":"subscribe"}"#)
                .unwrap(),
            None
        );
        assert_eq!(
            parse_trade_frame(ExchangeVenue::Okx, r#"{"event":"subscribe","arg":{}}"#).unwrap(),
            None
        );
        assert_eq!(
            parse_trade_frame(
                ExchangeVenue::Coinbase,
                r#"{"type":"subscriptions","channels":[]}"#
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn malformed_json_is_an_error() {
        assert!(matches!(
            parse_trade_frame(ExchangeVenue::Bybit, "{not json"),
            Err(DecodeError::Json(_))
        ));
    }

    #[test]
    fn non_decimal_price_is_an_error() {
        let raw = r#"{"data":[{"p":"NaN","T":1}]}"#;
        assert!(matches!(
            parse_trade_frame(ExchangeVenue::Bybit, raw),
            Err(DecodeError::Decimal(_))
        ));
    }
}
