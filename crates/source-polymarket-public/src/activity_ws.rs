//! Polymarket live-data activity websocket transport (#530, #546).
//!
//! `wss://ws-live-data.polymarket.com` is the officially listed real-time
//! data endpoint whose activity subscription and payload are published by the
//! first-party client, without published completeness, uptime, ordering,
//! continuity, or resume guarantees (`docs/15-SOURCES.md` carries the entry
//! and the re-check policy). It streams every platform trade with wallet
//! attribution (`proxyWallet` — the same identity axis the REST `/activity`
//! endpoint reports), measured at p50 0.80s / p95 1.32s versus the leader's
//! trade timestamp. The CLOB market websocket remains wallet-anonymous
//! (#282/#300) — this feed is the only attributed push path.
//!
//! Division of ownership: this module owns the transport (dial + subscribe,
//! read), the frame envelope (topic routing, exact payload extraction), the
//! policy constants, and the per-connection reconnect backoff. Normalizing a
//! payload into an `IncomingTrade` and deciding liveness from normalized rows
//! belong to the service's `activity_ingest`, which runs
//! [`ACTIVITY_WS_READER_COUNT`] independent readers over this transport: a
//! connection can stay open and acknowledged while delivering no activity
//! (#546 experiments, 2026-08-31), so only a parser-accepted activity row
//! proves a reader alive.

use serde::Deserialize;
use serde_json::value::RawValue;

/// Feed endpoint. Re-check the first-party contract per `docs/15` before a deploy that relies on it.
pub const ACTIVITY_WS_URL: &str = "wss://ws-live-data.polymarket.com";

/// Subscription frame sent once per (re)connect.
pub const ACTIVITY_WS_SUBSCRIBE: &str =
    r#"{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"}]}"#;

/// Version of the frame envelope shape this module understands.
pub const ACTIVITY_WS_SCHEMA_VERSION: u32 = 1;
/// Version of the envelope parser below.
pub const ACTIVITY_WS_PARSER_VERSION: u32 = 1;

/// Independent reader connections per service process (#546). Three keeps two
/// live readers after the measured connection-local silent failure.
/// Canonical home: `docs/_GLOSSARY.md` (`activity_ws_reader_count`).
pub const ACTIVITY_WS_READER_COUNT: usize = 3;
/// A reader with no normalized activity row for this long is not live. While
/// reading, it drops its socket at the deadline and re-dials after its own
/// backoff; while blocked on a full fan-in send it keeps the socket and its
/// retained row (health still derives it non-live) and drops only after that
/// frame drains. Six times the largest activity gap measured on a healthy
/// connection (4.775s, #546).
/// Canonical home: `docs/_GLOSSARY.md` (`activity_ws_normalized_activity_timeout_secs`).
pub const ACTIVITY_WS_NORMALIZED_ACTIVITY_TIMEOUT_SECS: u64 = 30;
/// Reconnect backoff doubles from 1s and is capped here.
pub const ACTIVITY_WS_BACKOFF_CAP_SECS: u64 = 60;

#[derive(Debug, thiserror::Error)]
pub enum ActivityWsError {
    #[error("frame json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("websocket: {message}")]
    Transport { message: String },
}

fn transport(error: impl std::fmt::Display) -> ActivityWsError {
    ActivityWsError::Transport {
        message: error.to_string(),
    }
}

// ── Frame envelope ───────────────────────────────────────────────────────────

/// One envelope as delivered by the feed. `payload` is captured raw so the
/// exact bytes reach the source event log unmodified (replay invariant).
#[derive(Deserialize)]
struct ActivityFrame<'a> {
    #[serde(default)]
    topic: Option<String>,
    #[serde(default, rename = "type")]
    kind: Option<String>,
    #[serde(default, borrow)]
    payload: Option<&'a RawValue>,
}

/// Parse one websocket text frame into the exact activity payload bytes it
/// carries.
///
/// Returns the payload slices (borrowed from `raw`, byte-identical to the
/// wire) plus the count of activity frames that carried no payload. Empty
/// keepalives, acknowledgements, and other topics contribute nothing and are
/// not errors. Whether a payload is a usable trade is decided exactly once
/// downstream by the service normalizer; this envelope layer makes no
/// field-level claim, so it can never certify a payload the normalizer rejects.
pub fn parse_activity_frame(raw: &str) -> Result<(Vec<&[u8]>, usize), ActivityWsError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let frames: Vec<ActivityFrame<'_>> = if trimmed.starts_with('[') {
        serde_json::from_str(trimmed)?
    } else {
        vec![serde_json::from_str(trimmed)?]
    };

    let mut payloads = Vec::new();
    let mut missing = 0usize;
    for frame in frames {
        let is_activity_trade =
            frame.topic.as_deref() == Some("activity") && frame.kind.as_deref() == Some("trades");
        if !is_activity_trade {
            continue;
        }
        match frame.payload {
            Some(payload) => payloads.push(payload.get().as_bytes()),
            None => missing += 1,
        }
    }
    Ok((payloads, missing))
}

// ── Reconnect backoff (per reader, persistent across connection cycles) ──────

/// Exponential backoff: 1s, 2s, 4s, … capped at [`ACTIVITY_WS_BACKOFF_CAP_SECS`].
pub fn backoff_secs(consecutive_reconnects: u32) -> u64 {
    let exp = consecutive_reconnects.min(6); // 2^6 = 64 > cap; avoids overflow
    (1u64 << exp).min(ACTIVITY_WS_BACKOFF_CAP_SECS)
}

/// Reader-owned reconnect accounting (#530 review F3): ONE persistent counter
/// across connection cycles, so connect-success/immediate-silence cycles
/// escalate instead of hammering at 1s forever. Only a normalized activity
/// row counts as progress (#546): connect, acknowledgement, keepalive, and
/// envelope-level parsing prove nothing about delivery.
#[derive(Debug, Clone, Default)]
pub struct ReconnectBackoff {
    consecutive: u32,
}

impl ReconnectBackoff {
    /// A cycle ended without progress: returns this failure's backoff and
    /// advances the counter.
    pub fn on_cycle_failed(&mut self) -> u64 {
        let backoff = backoff_secs(self.consecutive);
        self.consecutive = self.consecutive.saturating_add(1);
        backoff
    }

    /// Progress was made (a payload passed the production normalizer): reset.
    pub fn on_normalized_row(&mut self) {
        self.consecutive = 0;
    }

    /// Completed reconnect attempts since the last normalized row (observability).
    pub fn consecutive(&self) -> u32 {
        self.consecutive
    }
}

// ── Transport ────────────────────────────────────────────────────────────────

use futures::{SinkExt as _, StreamExt as _};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Object-safe byte stream under the websocket framing: production is TLS over
/// TCP; scenario builds may substitute an in-process pipe (see [`ActivityWsPeer`]).
trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

type WsInner = WebSocketStream<MaybeTlsStream<Box<dyn AsyncStream>>>;

/// A connected, subscribed live-data socket. The service's reader drives it
/// (read loop, normalized-row deadline, health); this type only owns the wire
/// mechanics so the protocol lives beside its constants.
pub struct ActivityWsStream {
    inner: WsInner,
}

/// Install the process-default rustls crypto provider exactly once.
///
/// The dependency graph carries TWO providers — `ring` (reqwest's
/// `rustls-tls`) and `aws-lc-rs` (tokio-tungstenite's
/// `rustls-tls-webpki-roots`) — so rustls 0.23 has no implicit default and
/// `ClientConfig::builder()` PANICS on first use. That panic crash-looped
/// pe-service on the 2026-08-25 #530 deploy (rustls crypto/mod.rs:249);
/// tests never caught it because every websocket test uses fake streams.
/// Pinning `ring` adds no compilation (both are already built).
fn ensure_crypto_provider() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Err = a default was already installed elsewhere — equally fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

impl ActivityWsStream {
    /// Dial [`ACTIVITY_WS_URL`] (TCP, then TLS + websocket handshake) and send
    /// [`ACTIVITY_WS_SUBSCRIBE`]. Production is fixed to this endpoint.
    pub async fn connect_and_subscribe() -> Result<Self, ActivityWsError> {
        ensure_crypto_provider();
        let request = ACTIVITY_WS_URL.into_client_request().map_err(transport)?;
        let host = request
            .uri()
            .host()
            .ok_or_else(|| transport("activity websocket url has no host"))?
            .to_owned();
        let port = request.uri().port_u16().unwrap_or(443);
        let tcp = tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .map_err(transport)?;
        let stream: Box<dyn AsyncStream> = Box::new(tcp);
        let (inner, _response) =
            tokio_tungstenite::client_async_tls_with_config(request, stream, None, None)
                .await
                .map_err(transport)?;
        Self::subscribed(inner).await
    }

    async fn subscribed(inner: WsInner) -> Result<Self, ActivityWsError> {
        let mut stream = Self { inner };
        stream
            .inner
            .send(Message::Text(ACTIVITY_WS_SUBSCRIBE.into()))
            .await
            .map_err(transport)?;
        Ok(stream)
    }

    /// Next frame; `Ok(None)` means the socket closed. Ping/pong and binary
    /// frames surface as [`WireFrame::NonText`] (tungstenite answers pings on
    /// read) so the reader can count them as transport liveness without ever
    /// treating them as activity. Cancel safe: partial-frame state lives in the
    /// stream, not in this future.
    pub async fn next_frame(&mut self) -> Result<Option<WireFrame>, ActivityWsError> {
        match self.inner.next().await {
            None => Ok(None),
            Some(Ok(Message::Text(text))) => Ok(Some(WireFrame::Text(text.to_string()))),
            Some(Ok(Message::Close(_))) => Ok(None),
            Some(Ok(_)) => Ok(Some(WireFrame::NonText)),
            Some(Err(e)) => Err(transport(e)),
        }
    }
}

/// One frame received on the socket.
#[derive(Debug)]
pub enum WireFrame {
    /// A text frame: the only kind that can carry activity payloads.
    Text(String),
    /// Ping, pong, or binary: proof the transport is alive, never activity.
    NonText,
}

/// Scenario-only server half of an in-process activity socket pair. Tests use
/// it to observe exactly what a reader sends and to deliver frames, closes, or
/// silence without any network; production never constructs one.
#[cfg(feature = "scenario")]
pub struct ActivityWsPeer {
    inner: WebSocketStream<tokio::io::DuplexStream>,
}

#[cfg(feature = "scenario")]
impl ActivityWsPeer {
    /// Client/server pair over a duplex pipe. The client half is a real
    /// [`ActivityWsStream`] that has already sent the production subscription.
    pub async fn pair() -> Result<(ActivityWsStream, Self), ActivityWsError> {
        use tokio_tungstenite::tungstenite::protocol::Role;
        let (client_io, server_io) = tokio::io::duplex(1 << 16);
        let client_io: Box<dyn AsyncStream> = Box::new(client_io);
        let client =
            WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(client_io), Role::Client, None)
                .await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        Ok((
            ActivityWsStream::subscribed(client).await?,
            Self { inner: server },
        ))
    }

    /// Next text frame from the reader; `None` once the reader dropped its socket.
    pub async fn recv_text(&mut self) -> Option<String> {
        loop {
            match self.inner.next().await {
                Some(Ok(Message::Text(text))) => return Some(text.to_string()),
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return None,
                Some(Ok(_)) => continue,
            }
        }
    }

    /// A text frame that is already available without waiting, else `None`.
    pub fn try_recv_text(&mut self) -> Option<String> {
        use futures::FutureExt as _;
        self.recv_text().now_or_never().flatten()
    }

    /// Deliver one text frame to the reader.
    pub async fn send_text(&mut self, text: &str) -> Result<(), ActivityWsError> {
        self.inner
            .send(Message::Text(text.into()))
            .await
            .map_err(transport)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const TRADE: &str = r#"{"topic":"activity","type":"trades","timestamp":1787603430123,
        "payload":{"proxyWallet":"0xAbC0000000000000000000000000000000000001",
        "conditionId":"0xcond","asset":"123","side":"BUY","size":"12.5","price":"0.41",
        "timestamp":"1787603429","transactionHash":"0xhash","outcomeIndex":"1","fee":"0"}}"#;

    #[test]
    fn parses_single_activity_frame_keeps_exact_payload_bytes() {
        let (payloads, missing) = parse_activity_frame(TRADE).unwrap();
        assert_eq!(missing, 0);
        assert_eq!(payloads.len(), 1);
        // Raw payload bytes preserved exactly (replay invariant): no lowercasing,
        // no re-serialization.
        let payload = std::str::from_utf8(payloads[0]).unwrap();
        assert!(payload.starts_with(r#"{"proxyWallet":"0xAbC"#));
        assert!(payload.ends_with(r#""fee":"0"}"#));
    }

    #[test]
    fn array_frame_and_non_activity_topics() {
        let frame = format!(r#"[{TRADE},{{"topic":"comments","type":"new","payload":{{}}}}]"#);
        let (payloads, missing) = parse_activity_frame(&frame).unwrap();
        assert_eq!((payloads.len(), missing), (1, 0));
    }

    #[test]
    fn empty_keepalive_and_acknowledgement_frames_carry_nothing() {
        for frame in ["", "   ", r#"{"status":"ok"}"#, r#"{"type":"pong"}"#] {
            let (payloads, missing) = parse_activity_frame(frame).unwrap();
            assert!(payloads.is_empty(), "frame {frame:?} must carry no payload");
            assert_eq!(missing, 0, "frame {frame:?} is not an activity frame");
        }
    }

    #[test]
    fn activity_frame_without_payload_is_counted_missing() {
        let frame = r#"{"topic":"activity","type":"trades"}"#;
        let (payloads, missing) = parse_activity_frame(frame).unwrap();
        assert!(payloads.is_empty());
        assert_eq!(missing, 1);
    }

    #[test]
    fn envelope_layer_makes_no_field_claims() {
        // A payload without a wallet is still an activity payload at this layer;
        // the service normalizer is the single acceptance boundary (#546).
        let frame = r#"{"topic":"activity","type":"trades","payload":{"price":"0.5"}}"#;
        let (payloads, missing) = parse_activity_frame(frame).unwrap();
        assert_eq!((payloads.len(), missing), (1, 0));
        assert_eq!(payloads[0], br#"{"price":"0.5"}"#);
    }

    #[test]
    fn malformed_frame_is_an_error() {
        assert!(parse_activity_frame("{not json").is_err());
    }

    #[test]
    fn crypto_provider_installs_and_tls_config_builds() {
        // Regression for the 2026-08-25 crash-loop: without an installed
        // process default, this exact builder call panics under the dual-
        // provider graph. No network involved.
        ensure_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
        let roots = rustls::RootCertStore::empty();
        let _config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
    }

    #[test]
    fn reconnect_backoff_accumulates_across_cycles_and_resets_on_normalized_rows() {
        let mut rb = ReconnectBackoff::default();
        assert_eq!(rb.on_cycle_failed(), 1);
        assert_eq!(rb.on_cycle_failed(), 2);
        assert_eq!(rb.on_cycle_failed(), 4);
        assert_eq!(rb.consecutive(), 3);
        rb.on_normalized_row();
        assert_eq!(rb.on_cycle_failed(), 1, "progress resets the ladder");
    }

    #[test]
    fn backoff_doubles_and_caps() {
        assert_eq!(backoff_secs(0), 1);
        assert_eq!(backoff_secs(1), 2);
        assert_eq!(backoff_secs(5), 32);
        assert_eq!(backoff_secs(6), 60);
        assert_eq!(backoff_secs(60), 60);
    }
}
