//! #530: websocket-primary trade ingest task.
//!
//! Reads the live-data activity firehose, filters to the current watchlist
//! (one hash-set lookup per platform trade — observation cost is flat in
//! watchlist size), durably appends each watched raw payload to the source
//! event log, normalizes through the SAME parser as the REST path, and fans
//! into the orchestrator's existing bounded trade channel. The REST poller
//! stays always-on as the correctness backstop; both paths converge on
//! `source_trade_id` dedup ("RTDS then poll backstop", orchestrator).
//!
//! Failure semantics (review-settled single state machine):
//! - full trade channel → `send().await` blocks → TCP backpressure on the
//!   websocket reader; no watchlisted trade is dropped.
//! - source-log append/sync failure → sink poisons and THIS task enters a
//!   bounded-backoff reopen/revalidate loop, holding the current trade until
//!   it durably appends (#530 review F4/F1: websocket delivery must block
//!   while poisoned; the trade is retained, never assumed onto REST).
//! - watched payload fails normalization → skipped here; the REST path parses
//!   the same shape with the same converter, so the poller either delivers it
//!   or freezes the wallet cursor on it (#511) — the existing no-silent-loss
//!   posture, not a new assumption.
//! - subscription silence → resubscribe once, then reconnect; reconnect
//!   backoff is a DRIVER-owned persistent counter (#530 review F3), so
//!   connect-success/immediate-death cycles escalate instead of hammering.

use std::collections::HashSet;
use std::time::Duration;

use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_source_polymarket_public::{
    ACTIVITY_WS_PARSER_VERSION, ACTIVITY_WS_SCHEMA_VERSION, ActivityWsPolicy, ActivityWsStream,
    PolicyAction, ReconnectBackoff, backoff_secs, parse_activity_frame,
};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::live_watchlist::LiveWatchlist;
use crate::source_event_sink::SourceEventSink;
use crate::trade_parser;

/// Source id stamped on every websocket envelope in the source event log.
pub const ACTIVITY_WS_SOURCE_ID: &str = "polymarket-activity-ws";

/// Outcome of one connection's read loop.
struct CycleEnd {
    shutdown: bool,
    saw_valid_frame: bool,
}

pub struct ActivityIngest {
    live_watchlist: LiveWatchlist,
    sink: SourceEventSink,
    trade_tx: mpsc::Sender<IncomingTrade>,
    health: crate::health::SharedHealth,
}

impl ActivityIngest {
    pub fn new(
        live_watchlist: LiveWatchlist,
        sink: SourceEventSink,
        trade_tx: mpsc::Sender<IncomingTrade>,
        health: crate::health::SharedHealth,
    ) -> Self {
        Self {
            live_watchlist,
            sink,
            trade_tx,
            health,
        }
    }

    /// Run until the trade channel closes (service shutdown).
    pub async fn run(mut self) {
        let mut backoff = ReconnectBackoff::default();
        loop {
            let stream = match ActivityWsStream::connect_and_subscribe().await {
                Ok(s) => s,
                Err(error) => {
                    let sleep_secs = backoff.on_cycle_failed();
                    warn!(error = %error, backoff_secs = sleep_secs, "activity ws connect failed");
                    self.set_health(|h| {
                        h.ws_connected = false;
                        h.ws_consecutive_reconnects = backoff.consecutive();
                    });
                    tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
                    continue;
                }
            };
            // A (re)connect cycle is also a sink-recovery point.
            let sink_ok = self.sink.try_reopen();
            self.set_health(|h| {
                h.ws_connected = true;
                h.ws_sink_poisoned = !sink_ok;
            });
            info!("activity ws connected and subscribed");

            let mut policy =
                ActivityWsPolicy::connected(OffsetDateTime::now_utc().unix_timestamp());
            let end = self.read_loop(stream, &mut policy).await;
            if end.shutdown {
                return;
            }
            if end.saw_valid_frame {
                backoff.on_valid_frame();
            }
            let sleep_secs = backoff.on_cycle_failed();
            self.set_health(|h| {
                h.ws_connected = false;
                h.ws_consecutive_reconnects = backoff.consecutive();
            });
            tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
        }
    }

    /// Read frames until reconnect is required or the service shuts down.
    async fn read_loop(
        &mut self,
        mut stream: ActivityWsStream,
        policy: &mut ActivityWsPolicy,
    ) -> CycleEnd {
        let mut saw_valid_frame = false;
        loop {
            match tokio::time::timeout(Duration::from_secs(1), stream.next_text()).await {
                Err(_elapsed) => {
                    let now = OffsetDateTime::now_utc().unix_timestamp();
                    match policy.tick(now) {
                        PolicyAction::None => {}
                        PolicyAction::Resubscribe => {
                            info!("activity ws silent; resubscribing");
                            if let Err(error) = stream.resubscribe().await {
                                warn!(error = %error, "resubscribe failed; reconnecting");
                                return CycleEnd {
                                    shutdown: false,
                                    saw_valid_frame,
                                };
                            }
                        }
                        PolicyAction::Reconnect { .. } => {
                            warn!("activity ws silence persisted; reconnecting");
                            return CycleEnd {
                                shutdown: false,
                                saw_valid_frame,
                            };
                        }
                    }
                }
                Ok(Ok(None)) => {
                    warn!("activity ws closed by peer");
                    return CycleEnd {
                        shutdown: false,
                        saw_valid_frame,
                    };
                }
                Ok(Err(error)) => {
                    warn!(error = %error, "activity ws read error");
                    return CycleEnd {
                        shutdown: false,
                        saw_valid_frame,
                    };
                }
                Ok(Ok(Some(text))) => {
                    let now = OffsetDateTime::now_utc();
                    match parse_activity_frame(&text) {
                        Err(error) => {
                            policy.on_frame(now.unix_timestamp(), false);
                            self.set_health(|h| h.ws_last_frame_at = Some(now));
                            warn!(error = %error, "unparseable activity frame");
                        }
                        Ok((trades, malformed)) => {
                            saw_valid_frame = true;
                            policy.on_frame(now.unix_timestamp(), true);
                            self.set_health(|h| {
                                h.ws_last_frame_at = Some(now);
                                h.ws_last_valid_frame_at = Some(now);
                                h.ws_consecutive_reconnects = 0;
                            });
                            if malformed > 0 {
                                warn!(malformed, "activity payloads without proxyWallet");
                            }
                            if self.deliver(trades, now).await {
                                return CycleEnd {
                                    shutdown: true,
                                    saw_valid_frame,
                                };
                            }
                        }
                    }
                }
            }
        }
    }

    /// Filter, durably log, normalize, and deliver watched trades.
    /// Returns `true` when the trade channel has closed (shutdown).
    async fn deliver(
        &mut self,
        trades: Vec<pe_source_polymarket_public::ActivityTradeRaw>,
        frame_received_at: OffsetDateTime,
    ) -> bool {
        if trades.is_empty() {
            return false;
        }
        // One snapshot per frame: every lookup sees the same generation.
        let watchlist = self.live_watchlist.snapshot();
        let watched: HashSet<WalletAddress> = watchlist.entries.iter().map(|e| e.wallet).collect();

        for raw in trades {
            let Ok(wallet) = WalletAddress::from_hex(&raw.proxy_wallet) else {
                continue; // counted upstream as malformed only when unfilterable
            };
            if !watched.contains(&wallet) {
                continue;
            }
            // Normalize FIRST (pure, with the frame receipt injected — replay
            // passes the envelope's recorded instant instead, review F7), then
            // durable-append, then deliver.
            let trade = match trade_parser::parse_ws_trade(&raw.payload_json, frame_received_at) {
                Ok(t) => t,
                Err(error) => {
                    warn!(error = %error, wallet = %wallet,
                        "watched ws payload failed normalization; the REST path (same parser) delivers or cursor-freezes it (#511)");
                    continue;
                }
            };
            if self.append_with_recovery(&raw.payload_json, &trade).await {
                return true;
            }
            // Bounded channel: a full queue blocks here → TCP backpressure on
            // the reader; a watchlisted trade is never dropped.
            if self.trade_tx.send(trade).await.is_err() {
                return true;
            }
        }
        false
    }

    /// Durably append one watched payload, entering the poison-recovery loop on
    /// failure: bounded-backoff reopen + full revalidation, retrying THIS
    /// payload until it lands (#530 review F4/F1 — delivery blocks while
    /// poisoned; the trade is retained, not assumed onto the REST path).
    /// Returns `true` on shutdown (trade channel closed).
    async fn append_with_recovery(&mut self, payload: &[u8], trade: &IncomingTrade) -> bool {
        let envelope = |payload: &[u8], trade: &IncomingTrade| EnvelopeIn {
            source_id: SourceId(ACTIVITY_WS_SOURCE_ID.to_string()),
            schema_version: ACTIVITY_WS_SCHEMA_VERSION,
            parser_version: ACTIVITY_WS_PARSER_VERSION,
            observed_at: SourceTimestamp(trade.observed_at),
            received_at: ReceivedAt(trade.received_at),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        };
        match self.sink.append_durable(envelope(payload, trade)) {
            Ok(_) => return false,
            Err(error) => {
                self.set_health(|h| h.ws_sink_poisoned = true);
                warn!(error = %error, trade = %trade.source_trade_id,
                    "source log append failed; sink poisoned — holding delivery and retrying reopen");
            }
        }
        let mut attempt: u32 = 0;
        loop {
            if self.trade_tx.is_closed() {
                return true;
            }
            tokio::time::sleep(Duration::from_secs(backoff_secs(attempt))).await;
            attempt = attempt.saturating_add(1);
            if !self.sink.try_reopen() {
                continue;
            }
            match self.sink.append_durable(envelope(payload, trade)) {
                Ok(_) => {
                    self.set_health(|h| h.ws_sink_poisoned = false);
                    info!(trade = %trade.source_trade_id,
                        "source log recovered; held payload appended durably");
                    return false;
                }
                Err(error) => {
                    warn!(error = %error, "source log re-poisoned immediately after reopen");
                }
            }
        }
    }

    fn set_health(&self, f: impl FnOnce(&mut crate::health::HealthState)) {
        let mut h = self
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut h);
    }
}
