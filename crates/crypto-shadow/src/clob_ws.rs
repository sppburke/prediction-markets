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

use std::collections::HashSet;
use std::str::FromStr as _;
use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use pe_core_types::Price;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tracing::{debug, warn};

use crate::types::{BookUpdate, ClobTrade, DecodeError, FeedFrame, FeedSource, now_unix_ms};
use crate::ws::MAX_BACKOFF_SECS;

/// Bounded capacity for the subscribe-command channel from `drive` to the CLOB
/// task. One slot per incremental subscribe batch. A dropped command is
/// self-healing: the next reconnect re-subscribes the full cumulative set.
/// See `docs/_GLOSSARY.md`: `crypto_shadow_clob_subscribe_channel_capacity`.
pub const CLOB_SUBSCRIBE_CHANNEL_CAPACITY: usize = 16;

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

/// Raw JSON shape of a `last_trade_price` CLOB frame (the trade print). The live
/// market channel sends exactly: `asset_id`, `market`, `price`, `size`, `side`,
/// `timestamp`, `fee_rate_bps`, `event_type`, `transaction_hash` (verified
/// 2026-06-09 — see `docs/15-SOURCES.md`).
#[derive(Debug, Deserialize)]
struct LastTradePriceFrame {
    asset_id: String,
    market: String, // condition_id — authoritative, present on every print
    price: String,
    size: String,
    side: String,      // "BUY" | "SELL" (taker side)
    timestamp: String, // epoch milliseconds as a string
    #[serde(default)]
    fee_rate_bps: String, // "0" or absent
    transaction_hash: String,
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

/// Decode a `last_trade_price` CLOB frame into a [`ClobTrade`]. Pure.
///
/// `Ok(None)` for any valid-JSON frame that is **not** a `last_trade_price`
/// event (book-snapshot array, `price_change`, `tick_size_change`, …) — the
/// caller runs this on every raw string alongside [`parse_clob_frame`], and a
/// frame is at most one of the two. `Err` only on malformed JSON or an
/// unparseable field within a structural trade frame.
pub fn parse_clob_trade(raw: &str) -> Result<Option<ClobTrade>, DecodeError> {
    let value: Value = serde_json::from_str(raw).map_err(|e| DecodeError::Json(e.to_string()))?;
    let Value::Object(ref map) = value else {
        return Ok(None); // an array is a book snapshot, never a trade
    };
    if map.get("event_type").and_then(Value::as_str) != Some("last_trade_price") {
        return Ok(None); // price_change, tick_size_change, etc.
    }
    let frame: LastTradePriceFrame =
        serde_json::from_value(value).map_err(|e| DecodeError::Json(e.to_string()))?;

    let price_d =
        Decimal::from_str(&frame.price).map_err(|_| DecodeError::Decimal(frame.price.clone()))?;
    let price = Price::new(price_d).map_err(|_| DecodeError::Decimal(frame.price.clone()))?;
    let size =
        Decimal::from_str(&frame.size).map_err(|_| DecodeError::Decimal(frame.size.clone()))?;
    let taker_is_buy = match frame.side.as_str() {
        "BUY" => true,
        "SELL" => false,
        other => return Err(DecodeError::InvalidValue(format!("side {other:?}"))),
    };
    let traded_at_ms = frame
        .timestamp
        .parse::<i64>()
        .map_err(|_| DecodeError::Decimal(frame.timestamp.clone()))?;
    let fee_rate_bps: u32 = if frame.fee_rate_bps.is_empty() {
        0
    } else {
        frame
            .fee_rate_bps
            .parse::<u32>()
            .map_err(|_| DecodeError::Decimal(frame.fee_rate_bps.clone()))?
    };

    Ok(Some(ClobTrade {
        token_id: frame.asset_id,
        condition_id: frame.market,
        price,
        size,
        taker_is_buy,
        traded_at_ms,
        fee_rate_bps,
        transaction_hash: frame.transaction_hash,
    }))
}

/// CLOB `market` subscribe payload for the given outcome token ids.
pub fn subscribe_message(token_ids: &[String]) -> String {
    let assets = serde_json::to_string(token_ids).unwrap_or_else(|_| "[]".to_string());
    format!(r#"{{"type":"market","assets_ids":{assets}}}"#)
}

/// Handle for pushing incremental subscription additions to the live CLOB task,
/// so markets enumerated after startup get their YES+NO books subscribed.
pub struct ClobSubscribeHandle {
    tx: mpsc::Sender<Vec<String>>,
}

impl ClobSubscribeHandle {
    /// Test-only constructor: a handle whose receiver is already dropped, so
    /// `add_tokens` is a no-op without a live task.
    #[cfg(test)]
    pub(crate) fn disconnected_for_test() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        Self { tx }
    }

    /// Push a batch of new token ids onto the subscribe channel. Non-blocking
    /// (`try_send`): a full channel is logged and dropped — self-healing, since
    /// the next reconnect re-subscribes the full cumulative set. Returns `false`
    /// if dropped or the task has exited.
    pub fn add_tokens(&self, tokens: Vec<String>) -> bool {
        if tokens.is_empty() {
            return true;
        }
        match self.tx.try_send(tokens) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!("clob: subscribe channel full, incremental add dropped (heals on reconnect)");
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }
}

/// Spawn the CLOB market WS task over the initial YES+NO token ids, returning a
/// [`ClobSubscribeHandle`] for incremental additions from `drive`'s refresh arm.
/// The task owns the cumulative token set and re-subscribes the **full** set on
/// every reconnect, so a transient drop never loses coverage.
pub fn spawn(
    ws_url: String,
    initial_token_ids: Vec<String>,
    tx: mpsc::Sender<FeedFrame>,
) -> (JoinHandle<()>, ClobSubscribeHandle) {
    let (sub_tx, sub_rx) = mpsc::channel(CLOB_SUBSCRIBE_CHANNEL_CAPACITY);
    let handle = ClobSubscribeHandle { tx: sub_tx };
    let join = tokio::spawn(clob_task(ws_url, initial_token_ids, tx, sub_rx));
    (join, handle)
}

enum ClobStreamOutcome {
    /// The frame consumer (mpsc receiver) or the subscribe sender was dropped.
    ConsumerClosed,
    /// Connect/subscribe failed before any frame streamed — keep backing off.
    ConnectFailed(String),
    /// Stream was active then ended — reset backoff on the next attempt.
    StreamEnded(String),
}

/// CLOB-specific reconnect loop. Owns the cumulative `(all_tokens, seen)` set so
/// every reconnect re-subscribes everything. `ws::ws_reconnect_loop` is left
/// unchanged (still used by the static Chainlink subscription).
async fn clob_task(
    ws_url: String,
    initial_token_ids: Vec<String>,
    tx: mpsc::Sender<FeedFrame>,
    mut sub_rx: mpsc::Receiver<Vec<String>>,
) {
    let mut seen: HashSet<String> = initial_token_ids.iter().cloned().collect();
    let mut all_tokens: Vec<String> = initial_token_ids;
    let mut backoff_secs: u64 = 1;
    loop {
        match clob_connect_and_stream(&ws_url, &mut all_tokens, &mut seen, &tx, &mut sub_rx).await {
            ClobStreamOutcome::ConsumerClosed => {
                debug!("clob: consumer closed, stopping");
                return;
            }
            ClobStreamOutcome::ConnectFailed(reason) => {
                warn!(reason, backoff_secs, "clob: connect failed; retrying");
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
            }
            ClobStreamOutcome::StreamEnded(reason) => {
                warn!(reason, "clob: stream ended; reconnecting");
                backoff_secs = 1;
            }
        }
    }
}

async fn clob_connect_and_stream(
    url: &str,
    all_tokens: &mut Vec<String>,
    seen: &mut HashSet<String>,
    tx: &mpsc::Sender<FeedFrame>,
    sub_rx: &mut mpsc::Receiver<Vec<String>>,
) -> ClobStreamOutcome {
    let (ws, _resp) = match tokio_tungstenite::connect_async(url).await {
        Ok(v) => v,
        Err(e) => return ClobStreamOutcome::ConnectFailed(e.to_string()),
    };
    let (mut write, mut read) = ws.split();

    // Re-subscribe the FULL cumulative set on every (re)connect.
    let sub_msg = subscribe_message(all_tokens);
    if let Err(e) = write.send(Message::Text(Utf8Bytes::from(sub_msg))).await {
        return ClobStreamOutcome::ConnectFailed(format!("subscribe send: {e}"));
    }

    loop {
        tokio::select! {
            maybe_msg = read.next() => {
                match maybe_msg {
                    Some(Ok(Message::Text(t))) => {
                        let frame = FeedFrame {
                            source: FeedSource::Clob,
                            received_ms: now_unix_ms(),
                            raw: t.to_string(),
                        };
                        match tx.try_send(frame) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                warn!("clob: channel full, dropping frame");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                return ClobStreamOutcome::ConsumerClosed;
                            }
                        }
                    }
                    Some(Ok(_)) => {} // ping/pong/binary/close: ignore
                    Some(Err(e)) => return ClobStreamOutcome::StreamEnded(e.to_string()),
                    None => return ClobStreamOutcome::StreamEnded("stream end".to_string()),
                }
            }
            maybe_new = sub_rx.recv() => {
                match maybe_new {
                    None => return ClobStreamOutcome::ConsumerClosed,
                    Some(new_tokens) => {
                        // Dedup against the cumulative set; push truly-new tokens
                        // (so a later reconnect re-subscribes them) and send an
                        // incremental subscribe for just those.
                        let truly_new: Vec<String> = new_tokens
                            .into_iter()
                            .filter(|t| {
                                let is_new = seen.insert(t.clone());
                                if is_new {
                                    all_tokens.push(t.clone());
                                }
                                is_new
                            })
                            .collect();
                        if truly_new.is_empty() {
                            continue;
                        }
                        let inc_msg = subscribe_message(&truly_new);
                        if let Err(e) = write.send(Message::Text(Utf8Bytes::from(inc_msg))).await {
                            // `all_tokens` already holds the new tokens, so the
                            // reconnect re-subscribes them.
                            return ClobStreamOutcome::StreamEnded(format!(
                                "incremental subscribe: {e}"
                            ));
                        }
                    }
                }
            }
        }
    }
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

    // A real `last_trade_price` frame captured verbatim from a 2026-06-09 live
    // `raw_ticks` row (full-length token id + tx hash, the venue's whitespace).
    const TRADE_FRAME: &str = r#"{"market":"0x4cfa48e6eb11a784e978e7798ac4cf94283f749f9d67d705cde99a6d6861bf04", "asset_id":"28288731664375269942632075758054005863241563384094117462481799570396361563475", "price":"0.55", "size":"3.581817", "fee_rate_bps":"0", "side":"BUY", "timestamp":"1781034208488", "event_type":"last_trade_price", "transaction_hash":"0x1b855f6954e3af5de7b5cd058f2808e871dbc1f91745ef02489a7b2a78f504b0"}"#;

    #[test]
    fn decodes_last_trade_price() {
        let t = parse_clob_trade(TRADE_FRAME).unwrap().unwrap();
        assert_eq!(
            t.token_id,
            "28288731664375269942632075758054005863241563384094117462481799570396361563475"
        );
        assert_eq!(
            t.condition_id,
            "0x4cfa48e6eb11a784e978e7798ac4cf94283f749f9d67d705cde99a6d6861bf04"
        );
        assert_eq!(t.price, Price(dec!(0.55)));
        assert_eq!(t.size, dec!(3.581817));
        assert!(t.taker_is_buy);
        assert_eq!(t.traded_at_ms, 1_781_034_208_488);
        // ms, not seconds (a seconds value would be ~1.78e9, not ~1.78e12).
        assert!(t.traded_at_ms > 1_700_000_000_000 && t.traded_at_ms < 2_000_000_000_000);
        assert_eq!(t.fee_rate_bps, 0);
        assert_eq!(
            t.transaction_hash,
            "0x1b855f6954e3af5de7b5cd058f2808e871dbc1f91745ef02489a7b2a78f504b0"
        );
    }

    #[test]
    fn trade_decoder_ignores_book_and_price_change() {
        // A book snapshot and a price_change are not trades.
        assert!(parse_clob_trade(BOOK).unwrap().is_none());
        let pc = r#"{"market":"0xmkt","price_changes":[{"asset_id":"a","best_bid":"0.5","best_ask":"0.51"}]}"#;
        assert!(parse_clob_trade(pc).unwrap().is_none());
        // And the book decoder ignores a trade frame -> no double-count.
        assert!(parse_clob_frame(TRADE_FRAME).unwrap().is_empty());
    }

    #[test]
    fn trade_decoder_returns_none_for_tick_size_change() {
        let tsc = r#"{"event_type":"tick_size_change","asset_id":"0xyes","new_tick_size":"0.001"}"#;
        assert!(parse_clob_trade(tsc).unwrap().is_none());
    }

    #[test]
    fn no_token_sell_trade_is_side_agnostic() {
        // NO token + SELL: the decoder stores the raw token_id + taker_is_buy
        // and the frame's own `market` as condition_id; the YES/NO side
        // resolution happens in the join, not here.
        let raw = r#"{"market":"0xcondX","asset_id":"tok-no","price":"0.49","size":"10","side":"SELL","timestamp":"1781032143544","event_type":"last_trade_price","transaction_hash":"0xabc","fee_rate_bps":"0"}"#;
        let t = parse_clob_trade(raw).unwrap().unwrap();
        assert_eq!(t.token_id, "tok-no");
        assert_eq!(t.condition_id, "0xcondX");
        assert!(!t.taker_is_buy);
    }

    #[test]
    fn unknown_side_is_invalid_value_err() {
        let raw = r#"{"market":"0xc","asset_id":"t","price":"0.5","size":"1","side":"WAT","timestamp":"1","event_type":"last_trade_price","transaction_hash":"0x1"}"#;
        assert!(matches!(
            parse_clob_trade(raw),
            Err(DecodeError::InvalidValue(_))
        ));
    }
}
