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
//! - source-log append/sync failure → sink poisons; websocket delivery blocks
//!   (REST carries the trades); reconnect cycles retry `try_reopen`.
//! - subscription silence → resubscribe once, then reconnect with capped
//!   exponential backoff (soak-proven zombie mode).

use std::collections::HashSet;
use std::time::Duration;

use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_source_polymarket_public::{
    ACTIVITY_WS_PARSER_VERSION, ACTIVITY_WS_SCHEMA_VERSION, ActivityWsPolicy, ActivityWsStream,
    PolicyAction, parse_activity_frame,
};
use time::OffsetDateTime;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::live_watchlist::LiveWatchlist;
use crate::source_event_sink::SourceEventSink;
use crate::trade_parser;

/// Source id stamped on every websocket envelope in the source event log.
pub const ACTIVITY_WS_SOURCE_ID: &str = "polymarket-activity-ws";

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
        let mut reconnects_for_backoff: u32 = 0;
        loop {
            let stream = match ActivityWsStream::connect_and_subscribe().await {
                Ok(s) => s,
                Err(error) => {
                    let backoff = pe_source_polymarket_public::backoff_secs(reconnects_for_backoff);
                    reconnects_for_backoff = reconnects_for_backoff.saturating_add(1);
                    warn!(error = %error, backoff_secs = backoff, "activity ws connect failed");
                    self.set_health(|h| {
                        h.ws_connected = false;
                        h.ws_consecutive_reconnects = h.ws_consecutive_reconnects.saturating_add(1);
                    });
                    tokio::time::sleep(Duration::from_secs(backoff)).await;
                    continue;
                }
            };
            // A (re)connect cycle is also the sink-recovery point.
            let sink_ok = self.sink.try_reopen();
            self.set_health(|h| {
                h.ws_connected = true;
                h.ws_sink_poisoned = !sink_ok;
            });
            info!("activity ws connected and subscribed");

            let mut policy =
                ActivityWsPolicy::connected(OffsetDateTime::now_utc().unix_timestamp());
            if self.read_loop(stream, &mut policy).await {
                return; // channel closed → shutdown
            }
            reconnects_for_backoff = policy.consecutive_reconnects().saturating_add(1);
            self.set_health(|h| {
                h.ws_connected = false;
                h.ws_consecutive_reconnects = reconnects_for_backoff;
            });
            let backoff =
                pe_source_polymarket_public::backoff_secs(reconnects_for_backoff.saturating_sub(1));
            tokio::time::sleep(Duration::from_secs(backoff)).await;
        }
    }

    /// Read frames until reconnect is required. Returns `true` on shutdown.
    async fn read_loop(
        &mut self,
        mut stream: ActivityWsStream,
        policy: &mut ActivityWsPolicy,
    ) -> bool {
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
                                return false;
                            }
                        }
                        PolicyAction::Reconnect { backoff_secs } => {
                            warn!(backoff_secs, "activity ws silence persisted; reconnecting");
                            return false;
                        }
                    }
                }
                Ok(Ok(None)) => {
                    warn!("activity ws closed by peer");
                    return false;
                }
                Ok(Err(error)) => {
                    warn!(error = %error, "activity ws read error");
                    return false;
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
                            policy.on_frame(now.unix_timestamp(), true);
                            self.set_health(|h| {
                                h.ws_last_frame_at = Some(now);
                                h.ws_last_valid_frame_at = Some(now);
                                h.ws_consecutive_reconnects = 0;
                            });
                            if malformed > 0 {
                                warn!(malformed, "activity payloads without proxyWallet");
                            }
                            if self.deliver(trades).await {
                                return true;
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
            // Normalize FIRST (pure), then durable-append, then deliver — the
            // append-before-decision-delivery contract.
            let trade = match trade_parser::parse_ws_trade(&raw.payload_json) {
                Ok(t) => t,
                Err(error) => {
                    warn!(error = %error, wallet = %wallet,
                        "watched ws payload failed normalization; REST backstop will carry it");
                    continue;
                }
            };
            let envelope = EnvelopeIn {
                source_id: SourceId(ACTIVITY_WS_SOURCE_ID.to_string()),
                schema_version: ACTIVITY_WS_SCHEMA_VERSION,
                parser_version: ACTIVITY_WS_PARSER_VERSION,
                observed_at: SourceTimestamp(trade.observed_at),
                received_at: ReceivedAt(trade.received_at),
                content_type: ContentType::Json,
                payload: raw.payload_json,
            };
            if let Err(error) = self.sink.append_durable(envelope) {
                self.set_health(|h| h.ws_sink_poisoned = true);
                warn!(error = %error, trade = %trade.source_trade_id,
                    "source log append failed; sink poisoned — REST fallback carries the trade");
                continue;
            }
            // Bounded channel: a full queue blocks here → TCP backpressure on
            // the reader; a watchlisted trade is never dropped.
            if self.trade_tx.send(trade).await.is_err() {
                return true;
            }
        }
        false
    }

    fn set_health(&self, f: impl FnOnce(&mut crate::health::HealthState)) {
        let mut h = self
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut h);
    }
}
