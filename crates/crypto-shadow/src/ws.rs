//! Shared WebSocket reconnect loop for the two feed tasks, mirroring the
//! bounded-`mpsc` + exponential-backoff pattern in
//! `crates/source-onchain-polygon/src/live.rs`. That connector is alloy-RPC-WS
//! (a different transport), so the pattern is mirrored, not shared.
//!
//! Live-path only: not exercised by the offline CI gate (which has no network).
//! The pure frame decoders that *are* gate-tested live in `chainlink_ws` /
//! `clob_ws`.

use std::time::Duration;

use futures::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tracing::{debug, warn};

/// Maximum WS reconnect backoff. See `docs/_GLOSSARY.md`:
/// `crypto_shadow_ws_max_backoff_secs`.
pub const MAX_BACKOFF_SECS: u64 = 60;

enum StreamOutcome {
    /// The consumer (mpsc receiver) was dropped — stop reconnecting.
    ConsumerClosed,
    /// Connect/subscribe failed before any frame streamed — keep backing off.
    ConnectFailed(String),
    /// Stream was active then ended — reset backoff on the next attempt.
    StreamEnded(String),
}

/// Connect to `url`, send `subscribe` (if non-empty), and forward every text
/// frame to `on_text`. `on_text` returns `false` when the downstream consumer
/// has closed, which stops the reconnect loop.
pub async fn ws_reconnect_loop<F>(url: String, subscribe: String, mut on_text: F)
where
    F: FnMut(&str) -> bool,
{
    let mut backoff_secs: u64 = 1;
    loop {
        match connect_and_stream(&url, &subscribe, &mut on_text).await {
            StreamOutcome::ConsumerClosed => {
                debug!("ws: consumer closed, stopping");
                return;
            }
            StreamOutcome::ConnectFailed(reason) => {
                warn!(reason, backoff_secs, "ws: connect failed; retrying");
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
            }
            StreamOutcome::StreamEnded(reason) => {
                warn!(reason, "ws: stream ended; reconnecting");
                backoff_secs = 1;
            }
        }
    }
}

async fn connect_and_stream<F>(url: &str, subscribe: &str, on_text: &mut F) -> StreamOutcome
where
    F: FnMut(&str) -> bool,
{
    let (ws, _resp) = match tokio_tungstenite::connect_async(url).await {
        Ok(v) => v,
        Err(e) => return StreamOutcome::ConnectFailed(e.to_string()),
    };
    let (mut write, mut read) = ws.split();

    if !subscribe.is_empty() {
        let msg = Message::Text(Utf8Bytes::from(subscribe.to_string()));
        if let Err(e) = write.send(msg).await {
            return StreamOutcome::ConnectFailed(format!("subscribe send: {e}"));
        }
    }

    loop {
        match read.next().await {
            Some(Ok(Message::Text(t))) => {
                if !on_text(&t) {
                    return StreamOutcome::ConsumerClosed;
                }
            }
            Some(Ok(_)) => {} // ping/pong/binary/close-handshake frames: ignore
            Some(Err(e)) => return StreamOutcome::StreamEnded(e.to_string()),
            None => return StreamOutcome::StreamEnded("stream end".to_string()),
        }
    }
}
