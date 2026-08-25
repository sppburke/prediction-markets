//! Polymarket UI live-data websocket transport (#530).
//!
//! `wss://ws-live-data.polymarket.com` streams every platform trade with wallet
//! attribution (`proxyWallet` — the same identity axis the REST `/activity`
//! endpoint reports), measured at p50 0.80s / p95 1.32s versus the leader's
//! trade timestamp. It is an **unofficial UI feed** with no documented
//! contract: `docs/15-SOURCES.md` carries its entry, re-check policy, and the
//! 14-hour soak evidence behind the policy constants below. The CLOB market
//! websocket remains wallet-anonymous (#282/#300) — this feed is the only
//! attributed push path.
//!
//! Division of ownership: this module owns the transport (connect), the frame
//! envelope (subscription protocol, topic routing, raw payload extraction) and
//! the reconnect/resubscribe **policy** (a pure, clock-injected state machine —
//! the 14h soak proved subscriptions silently lapse on ping-alive sockets, so
//! silence handling is load-bearing, not hardening). Normalization of a payload
//! into an `IncomingTrade` is owned by the service's trade parser, shared with
//! the REST path so both produce identical trades by construction.

use serde::Deserialize;
use serde_json::value::RawValue;

/// Feed endpoint. Unofficial; re-check before each deploy per `docs/15`.
pub const ACTIVITY_WS_URL: &str = "wss://ws-live-data.polymarket.com";

/// Subscription frame sent once per (re)connect and on each resubscribe.
pub const ACTIVITY_WS_SUBSCRIBE: &str =
    r#"{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"}]}"#;

/// Version of the frame envelope shape this module understands.
pub const ACTIVITY_WS_SCHEMA_VERSION: u32 = 1;
/// Version of the envelope parser below.
pub const ACTIVITY_WS_PARSER_VERSION: u32 = 1;

/// Silence (no frame on a live socket) before re-sending the subscription.
/// Canonical home: `docs/_GLOSSARY.md` (`activity_ws_silence_resubscribe_secs`).
pub const ACTIVITY_WS_SILENCE_RESUBSCRIBE_SECS: i64 = 30;
/// No valid frame for this long ⇒ the websocket source is STALE (health).
pub const ACTIVITY_WS_STALE_SECS: i64 = 120;
/// No valid frame for this long ⇒ the websocket source is DEAD (health).
pub const ACTIVITY_WS_DEAD_SECS: i64 = 300;
/// Reconnect backoff doubles from 1s and is capped here.
pub const ACTIVITY_WS_BACKOFF_CAP_SECS: u64 = 60;

#[derive(Debug, thiserror::Error)]
pub enum ActivityWsError {
    #[error("frame json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("websocket: {message}")]
    Transport { message: String },
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

/// Probe for the one field the ingest filter needs before normalization.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WalletProbe {
    proxy_wallet: String,
}

/// One activity trade lifted from a frame: the filter key plus the exact
/// payload bytes (normalization happens in the service's trade parser).
#[derive(Debug, Clone)]
pub struct ActivityTradeRaw {
    /// Lowercased `proxyWallet` — the watchlist filter key.
    pub proxy_wallet: String,
    /// The exact payload object bytes as received.
    pub payload_json: Vec<u8>,
}

/// Parse one websocket text frame into its activity-trade payloads.
///
/// Returns the extracted trades plus the count of activity payloads that were
/// present but unusable (no `proxyWallet`): those are counted, never silently
/// ignored, so the ingest task can surface parse-health. Non-activity frames
/// and empty keepalive frames contribute nothing and are not errors.
pub fn parse_activity_frame(raw: &str) -> Result<(Vec<ActivityTradeRaw>, usize), ActivityWsError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok((Vec::new(), 0));
    }
    let frames: Vec<ActivityFrame<'_>> = if trimmed.starts_with('[') {
        serde_json::from_str(trimmed)?
    } else {
        vec![serde_json::from_str(trimmed)?]
    };

    let mut out = Vec::new();
    let mut malformed = 0usize;
    for frame in frames {
        let is_activity_trade =
            frame.topic.as_deref() == Some("activity") && frame.kind.as_deref() == Some("trades");
        if !is_activity_trade {
            continue;
        }
        let Some(payload) = frame.payload else {
            malformed += 1;
            continue;
        };
        match serde_json::from_str::<WalletProbe>(payload.get()) {
            Ok(probe) if !probe.proxy_wallet.is_empty() => out.push(ActivityTradeRaw {
                proxy_wallet: probe.proxy_wallet.to_lowercase(),
                payload_json: payload.get().as_bytes().to_vec(),
            }),
            _ => malformed += 1,
        }
    }
    Ok((out, malformed))
}

// ── Reconnect / resubscribe policy (pure, clock-injected) ────────────────────

/// What the transport loop must do next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyAction {
    /// Keep reading.
    None,
    /// Re-send [`ACTIVITY_WS_SUBSCRIBE`] on the live socket (silent lapse).
    Resubscribe,
    /// Drop the socket and reconnect after `backoff_secs`.
    Reconnect { backoff_secs: u64 },
}

/// Health classification of the websocket source, for the service health split.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsStaleness {
    Fresh,
    /// ≥ [`ACTIVITY_WS_STALE_SECS`] without a valid frame.
    Stale,
    /// ≥ [`ACTIVITY_WS_DEAD_SECS`] without a valid frame.
    Dead,
}

/// Pure state machine driving silence → resubscribe → reconnect, from the 14h
/// soak's measured failure mode: the subscription lapses while the socket stays
/// ping-alive (1,442 silence windows vs 16 hard disconnects). All transitions
/// take an explicit `now_unix` so behavior is fully deterministic in tests.
#[derive(Debug, Clone)]
pub struct ActivityWsPolicy {
    last_frame_unix: i64,
    last_valid_frame_unix: i64,
    resubscribed_this_silence: bool,
    consecutive_reconnects: u32,
}

impl ActivityWsPolicy {
    /// State for a socket that just connected (and subscribed) at `now_unix`.
    pub fn connected(now_unix: i64) -> Self {
        Self {
            last_frame_unix: now_unix,
            last_valid_frame_unix: now_unix,
            resubscribed_this_silence: false,
            consecutive_reconnects: 0,
        }
    }

    /// Record a received frame; `valid` = it parsed and contained activity trades
    /// or was a recognized non-activity frame (anything except a parse failure).
    pub fn on_frame(&mut self, now_unix: i64, valid: bool) {
        self.last_frame_unix = now_unix;
        self.resubscribed_this_silence = false;
        if valid {
            self.last_valid_frame_unix = now_unix;
            self.consecutive_reconnects = 0;
        }
    }

    /// Record a completed reconnect attempt (socket re-established, re-subscribed).
    pub fn on_reconnected(&mut self, now_unix: i64) {
        self.consecutive_reconnects = self.consecutive_reconnects.saturating_add(1);
        self.last_frame_unix = now_unix;
        self.resubscribed_this_silence = false;
    }

    /// Decide the next action for a silent interval ending at `now_unix`.
    ///
    /// First silence past the threshold gets one resubscribe on the live socket;
    /// if silence persists past a second threshold interval, reconnect with
    /// exponential backoff. A frame at any point resets the episode.
    pub fn tick(&mut self, now_unix: i64) -> PolicyAction {
        let silent_for = now_unix.saturating_sub(self.last_frame_unix);
        if silent_for < ACTIVITY_WS_SILENCE_RESUBSCRIBE_SECS {
            return PolicyAction::None;
        }
        if !self.resubscribed_this_silence {
            self.resubscribed_this_silence = true;
            return PolicyAction::Resubscribe;
        }
        if silent_for >= 2 * ACTIVITY_WS_SILENCE_RESUBSCRIBE_SECS {
            return PolicyAction::Reconnect {
                backoff_secs: backoff_secs(self.consecutive_reconnects),
            };
        }
        PolicyAction::None
    }

    /// Health classification from the age of the last VALID frame.
    pub fn staleness(&self, now_unix: i64) -> WsStaleness {
        let age = now_unix.saturating_sub(self.last_valid_frame_unix);
        if age >= ACTIVITY_WS_DEAD_SECS {
            WsStaleness::Dead
        } else if age >= ACTIVITY_WS_STALE_SECS {
            WsStaleness::Stale
        } else {
            WsStaleness::Fresh
        }
    }

    /// Age of the last frame of any kind (observability surface).
    pub fn last_frame_age_secs(&self, now_unix: i64) -> i64 {
        now_unix.saturating_sub(self.last_frame_unix)
    }

    /// Age of the last valid frame (observability surface).
    pub fn last_valid_frame_age_secs(&self, now_unix: i64) -> i64 {
        now_unix.saturating_sub(self.last_valid_frame_unix)
    }

    /// Completed reconnects since the last valid frame (observability surface).
    pub fn consecutive_reconnects(&self) -> u32 {
        self.consecutive_reconnects
    }
}

/// Exponential backoff: 1s, 2s, 4s, … capped at [`ACTIVITY_WS_BACKOFF_CAP_SECS`].
pub fn backoff_secs(consecutive_reconnects: u32) -> u64 {
    let exp = consecutive_reconnects.min(6); // 2^6 = 64 > cap; avoids overflow
    (1u64 << exp).min(ACTIVITY_WS_BACKOFF_CAP_SECS)
}

/// Driver-owned reconnect accounting (#530 review F3): ONE persistent counter
/// across connection cycles — a fresh [`ActivityWsPolicy`] per connection must
/// not reset backoff, or connect-success/immediate-death cycles hammer at 1s
/// forever (the measured zombie mode makes this a real shape, not a hypothesis).
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

    /// Progress was made (a valid frame arrived): reset.
    pub fn on_valid_frame(&mut self) {
        self.consecutive = 0;
    }

    /// Completed reconnect attempts since the last valid frame (observability).
    pub fn consecutive(&self) -> u32 {
        self.consecutive
    }
}

// ── Transport ────────────────────────────────────────────────────────────────

use futures::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::Message;

type WsInner =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A connected, subscribed live-data socket. The service's ingest task drives
/// it (read loop, policy ticks via timeout, health); this type only owns the
/// wire mechanics so the protocol lives beside its constants.
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
    /// Dial [`ACTIVITY_WS_URL`] and send [`ACTIVITY_WS_SUBSCRIBE`].
    pub async fn connect_and_subscribe() -> Result<Self, ActivityWsError> {
        ensure_crypto_provider();
        let (inner, _response) = tokio_tungstenite::connect_async(ACTIVITY_WS_URL)
            .await
            .map_err(|e| ActivityWsError::Transport {
                message: e.to_string(),
            })?;
        let mut stream = Self { inner };
        stream.resubscribe().await?;
        Ok(stream)
    }

    /// Re-send the subscription on the live socket (silent-lapse recovery).
    pub async fn resubscribe(&mut self) -> Result<(), ActivityWsError> {
        self.inner
            .send(Message::Text(ACTIVITY_WS_SUBSCRIBE.into()))
            .await
            .map_err(|e| ActivityWsError::Transport {
                message: e.to_string(),
            })
    }

    /// Next text frame; `Ok(None)` means the socket closed. Ping/pong and
    /// binary frames are skipped (tungstenite answers pings on read).
    pub async fn next_text(&mut self) -> Result<Option<String>, ActivityWsError> {
        loop {
            match self.inner.next().await {
                None => return Ok(None),
                Some(Ok(Message::Text(text))) => return Ok(Some(text.to_string())),
                Some(Ok(Message::Close(_))) => return Ok(None),
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    return Err(ActivityWsError::Transport {
                        message: e.to_string(),
                    });
                }
            }
        }
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
    fn parses_single_activity_frame_lowercases_wallet_keeps_raw_payload() {
        let (trades, malformed) = parse_activity_frame(TRADE).unwrap();
        assert_eq!(malformed, 0);
        assert_eq!(trades.len(), 1);
        assert_eq!(
            trades[0].proxy_wallet,
            "0xabc0000000000000000000000000000000000001"
        );
        // Raw payload bytes preserved exactly (replay invariant).
        let payload = std::str::from_utf8(&trades[0].payload_json).unwrap();
        assert!(payload.contains(r#""proxyWallet":"0xAbC"#));
        assert!(payload.contains(r#""fee":"0""#));
    }

    #[test]
    fn array_frame_and_non_activity_topics() {
        let frame = format!(r#"[{TRADE},{{"topic":"comments","type":"new","payload":{{}}}}]"#);
        let (trades, malformed) = parse_activity_frame(&frame).unwrap();
        assert_eq!((trades.len(), malformed), (1, 0));
    }

    #[test]
    fn empty_keepalive_frame_is_not_an_error() {
        let (trades, malformed) = parse_activity_frame("").unwrap();
        assert!(trades.is_empty());
        assert_eq!(malformed, 0);
    }

    #[test]
    fn activity_payload_without_wallet_is_counted_malformed() {
        let frame = r#"{"topic":"activity","type":"trades","payload":{"price":"0.5"}}"#;
        let (trades, malformed) = parse_activity_frame(frame).unwrap();
        assert!(trades.is_empty());
        assert_eq!(malformed, 1);
    }

    #[test]
    fn optional_fee_absent_still_parses() {
        let frame = r#"{"topic":"activity","type":"trades",
            "payload":{"proxyWallet":"0xa","conditionId":"0xc","side":"SELL",
            "size":"1","price":"0.5","timestamp":"1787603429","transactionHash":"0xh"}}"#;
        let (trades, malformed) = parse_activity_frame(frame).unwrap();
        assert_eq!((trades.len(), malformed), (1, 0));
    }

    #[test]
    fn policy_silence_resubscribes_once_then_reconnects_with_backoff() {
        let mut p = ActivityWsPolicy::connected(1000);
        assert_eq!(p.tick(1000 + 29), PolicyAction::None);
        assert_eq!(p.tick(1000 + 30), PolicyAction::Resubscribe);
        // Still inside the same silence episode: no duplicate resubscribe.
        assert_eq!(p.tick(1000 + 45), PolicyAction::None);
        assert_eq!(
            p.tick(1000 + 60),
            PolicyAction::Reconnect { backoff_secs: 1 }
        );
        p.on_reconnected(1000 + 61);
        assert_eq!(p.tick(1000 + 91), PolicyAction::Resubscribe);
        assert_eq!(
            p.tick(1000 + 121),
            PolicyAction::Reconnect { backoff_secs: 2 }
        );
    }

    #[test]
    fn policy_frame_resets_the_silence_episode_and_backoff() {
        let mut p = ActivityWsPolicy::connected(1000);
        assert_eq!(p.tick(1030), PolicyAction::Resubscribe);
        p.on_frame(1031, true);
        assert_eq!(p.tick(1060), PolicyAction::None);
        assert_eq!(p.consecutive_reconnects(), 0);
        assert_eq!(p.tick(1061), PolicyAction::Resubscribe);
    }

    #[test]
    fn staleness_thresholds() {
        let mut p = ActivityWsPolicy::connected(0);
        assert_eq!(p.staleness(119), WsStaleness::Fresh);
        assert_eq!(p.staleness(120), WsStaleness::Stale);
        assert_eq!(p.staleness(299), WsStaleness::Stale);
        assert_eq!(p.staleness(300), WsStaleness::Dead);
        // Invalid frames advance last_frame but NOT last_valid_frame.
        p.on_frame(200, false);
        assert_eq!(p.staleness(320), WsStaleness::Dead);
        p.on_frame(320, true);
        assert_eq!(p.staleness(320), WsStaleness::Fresh);
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
    fn reconnect_backoff_accumulates_across_cycles_and_resets_on_frames() {
        let mut rb = ReconnectBackoff::default();
        assert_eq!(rb.on_cycle_failed(), 1);
        assert_eq!(rb.on_cycle_failed(), 2);
        assert_eq!(rb.on_cycle_failed(), 4);
        assert_eq!(rb.consecutive(), 3);
        rb.on_valid_frame();
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
