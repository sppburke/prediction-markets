//! #530/#546: websocket-primary trade ingest — three independent readers, one
//! coordinator.
//!
//! Each reader owns one live-data socket and one persistent reconnect backoff.
//! It parses every frame's activity payloads, normalizes EACH payload exactly
//! once through the same parser as the REST path
//! (`trade_parser::parse_ws_trade`), and only then filters on the normalized
//! wallet against one watchlist snapshot per frame. A parser-accepted row —
//! watched or not — is the ONLY evidence a reader is alive: the 2026-08-31
//! experiments (#546) showed a connection can stay open and acknowledged while
//! delivering nothing, so 30 s without one drops the socket and the reader
//! re-dials after its own backoff while the other readers keep delivering.
//!
//! Watched rows travel `(slot, exact raw bytes, normalized trade)` over ONE
//! bounded fan-in channel (capacity = the trade channel's) to the coordinator,
//! which keeps the pre-#546 order: durable source-log append+sync, then the
//! orchestrator's bounded trade channel. Reader copies of one trade are not
//! coalesced — each is raw evidence — and the orchestrator's durable
//! `seen_trades` check suppresses repeat financial effects after the first
//! successful commit; a first copy rolled back before commit stays unseen so
//! the next copy retries.
//!
//! Failure semantics:
//! - full fan-in or trade channel → the sender blocks holding its one item
//!   (`fan_in_blocked` in health) and reads no further wire frame, so
//!   backpressure reaches the socket; health derives the slot non-live once its
//!   last row ages past the timeout, the socket is kept, and after the frame
//!   drains the deadline check drops it before any further read. Nothing is
//!   deliberately dropped while both downstream receivers are open; a closed
//!   receiver is orderly shutdown. Owner abort or process failure may discard
//!   in-memory pre-log work (the declared whole-process boundary; polling and
//!   #544 own recovery).
//! - source-log append/sync failure → the sink poisons and the coordinator
//!   enters a bounded-backoff reopen/revalidate loop holding the current item,
//!   blocking delivery from EVERY reader until it durably appends (#530 review
//!   F4/F1: the trade is retained, never assumed onto REST).
//! - payload rejected by the normalizer → counted and warned once per frame;
//!   the REST path parses the same shape with the same converter, so the
//!   poller either delivers it or freezes the wallet cursor on it (#511).
//! - reader silence → drop + backoff + re-dial, per slot. `ActivityIngest::run`
//!   owns every task in one `JoinSet`; no detached reader survives it.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use pe_copy_signal_engine::IncomingTrade;
use pe_core_types::{ReceivedAt, SourceId, SourceTimestamp, WalletAddress};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_source_polymarket_public::{
    ACTIVITY_WS_NORMALIZED_ACTIVITY_TIMEOUT_SECS, ACTIVITY_WS_PARSER_VERSION,
    ACTIVITY_WS_READER_COUNT, ACTIVITY_WS_SCHEMA_VERSION, ActivityWsError, ActivityWsStream,
    ReconnectBackoff, WireFrame, backoff_secs, parse_activity_frame,
};
use time::OffsetDateTime;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::health::{HealthState, ReaderHealth, SharedHealth};
use crate::live_watchlist::LiveWatchlist;
use crate::source_event_sink::SourceEventSink;
use crate::trade_parser;

/// Source id stamped on every websocket envelope in the source event log.
pub const ACTIVITY_WS_SOURCE_ID: &str = "polymarket-activity-ws";

/// Opens one subscribed socket for reader `slot`. Production dials the fixed
/// endpoint; scenario builds may inject in-process peers.
pub type Dialer = Arc<
    dyn Fn(usize) -> BoxFuture<'static, Result<ActivityWsStream, ActivityWsError>> + Send + Sync,
>;

fn activity_timeout() -> Duration {
    Duration::from_secs(ACTIVITY_WS_NORMALIZED_ACTIVITY_TIMEOUT_SECS)
}

/// One watched, normalized observation on its way to the coordinator.
struct Observation {
    slot: usize,
    payload: Vec<u8>,
    trade: IncomingTrade,
}

/// A downstream receiver closed: the service is shutting down.
struct Shutdown;

pub struct ActivityIngest {
    live_watchlist: LiveWatchlist,
    sink: SourceEventSink,
    trade_tx: mpsc::Sender<IncomingTrade>,
    health: SharedHealth,
    dialer: Dialer,
}

impl ActivityIngest {
    pub fn new(
        live_watchlist: LiveWatchlist,
        sink: SourceEventSink,
        trade_tx: mpsc::Sender<IncomingTrade>,
        health: SharedHealth,
    ) -> Self {
        Self {
            live_watchlist,
            sink,
            trade_tx,
            health,
            dialer: Arc::new(|_slot| Box::pin(ActivityWsStream::connect_and_subscribe())),
        }
    }

    /// [`Self::new`] with an injected dialer (in-process peers). Scenario builds only;
    /// production is fixed to the endpoint dialed by `new`.
    #[cfg(feature = "scenario")]
    pub fn with_dialer(
        live_watchlist: LiveWatchlist,
        sink: SourceEventSink,
        trade_tx: mpsc::Sender<IncomingTrade>,
        health: SharedHealth,
        dialer: Dialer,
    ) -> Self {
        Self {
            live_watchlist,
            sink,
            trade_tx,
            health,
            dialer,
        }
    }

    /// Run until the trade channel closes (service shutdown).
    ///
    /// Owns every reader and the coordinator in one `JoinSet`: the first child
    /// to finish (a closed downstream channel) ends the pool — the rest are
    /// aborted and drained. Dropping this future (external abort) drops the
    /// set, which aborts its children without joining them.
    pub async fn run(self) {
        let (fan_in_tx, fan_in_rx) = mpsc::channel(self.trade_tx.max_capacity());
        let mut tasks = JoinSet::new();
        for slot in 0..ACTIVITY_WS_READER_COUNT {
            tasks.spawn(
                Reader {
                    slot,
                    dialer: self.dialer.clone(),
                    live_watchlist: self.live_watchlist.clone(),
                    health: self.health.clone(),
                    fan_in: fan_in_tx.clone(),
                }
                .run(),
            );
        }
        drop(fan_in_tx);
        tasks.spawn(
            Coordinator {
                sink: self.sink,
                trade_tx: self.trade_tx,
                health: self.health,
                fan_in: fan_in_rx,
            }
            .run(),
        );
        if tasks.join_next().await.is_some() {
            info!("activity ingest child finished; stopping the reader pool");
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
    }
}

// ── Reader ───────────────────────────────────────────────────────────────────

struct Reader {
    slot: usize,
    dialer: Dialer,
    live_watchlist: LiveWatchlist,
    health: SharedHealth,
    fan_in: mpsc::Sender<Observation>,
}

impl Reader {
    async fn run(mut self) {
        let mut backoff = ReconnectBackoff::default();
        loop {
            let stream = match (self.dialer)(self.slot).await {
                Ok(stream) => stream,
                Err(error) => {
                    let sleep_secs = backoff.on_cycle_failed();
                    warn!(slot = self.slot, error = %error, backoff_secs = sleep_secs,
                        "activity ws connect failed");
                    self.set_health(|r| {
                        r.connected = false;
                        r.consecutive_reconnects = backoff.consecutive();
                    });
                    tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
                    continue;
                }
            };
            // Connected is not live: this socket proves nothing until its first
            // normalized row.
            self.set_health(|r| {
                r.connected = true;
                r.last_normalized_activity_at = None;
            });
            info!(slot = self.slot, "activity ws connected and subscribed");
            if self.read_cycle(stream, &mut backoff).await.is_err() {
                return;
            }
            let sleep_secs = backoff.on_cycle_failed();
            self.set_health(|r| {
                r.connected = false;
                r.fan_in_blocked = false;
                r.consecutive_reconnects = backoff.consecutive();
            });
            tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
        }
    }

    /// Read frames until the normalized-activity deadline expires or the peer
    /// closes (`Ok`: reconnect) or downstream closes (`Err`: shutdown). Dropping
    /// the stream closes the socket without a close handshake.
    async fn read_cycle(
        &mut self,
        mut stream: ActivityWsStream,
        backoff: &mut ReconnectBackoff,
    ) -> Result<(), Shutdown> {
        let mut deadline = Instant::now() + activity_timeout();
        loop {
            let frame = tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => None,
                frame = stream.next_frame() => Some(frame),
            };
            // One deadline check per iteration, BEFORE any normalization could
            // refresh it: covers expiry while parked, a frame that became ready
            // at the same instant, and a fan-in block inside the previous frame
            // that outlived the deadline (the drop then precedes the next read).
            let now = Instant::now();
            if now >= deadline {
                warn!(
                    slot = self.slot,
                    timeout_secs = ACTIVITY_WS_NORMALIZED_ACTIVITY_TIMEOUT_SECS,
                    "activity ws reader produced no normalized activity row; dropping socket"
                );
                return Ok(());
            }
            let text = match frame {
                None => continue,
                Some(Ok(Some(WireFrame::Text(text)))) => text,
                Some(Ok(Some(WireFrame::NonText))) => {
                    // Ping/pong/binary: the transport is alive; nothing to normalize.
                    self.set_health(|r| r.last_wire_frame_at = Some(now));
                    continue;
                }
                Some(Ok(None)) => {
                    warn!(slot = self.slot, "activity ws closed by peer");
                    return Ok(());
                }
                Some(Err(error)) => {
                    warn!(slot = self.slot, error = %error, "activity ws read error");
                    return Ok(());
                }
            };
            self.on_frame(&text, now, &mut deadline, backoff).await?;
        }
    }

    /// Parse one text frame received at `now`, normalize every payload once,
    /// refresh liveness on the first accepted payload, and deliver the watched
    /// rows in order.
    async fn on_frame(
        &mut self,
        text: &str,
        now: Instant,
        deadline: &mut Instant,
        backoff: &mut ReconnectBackoff,
    ) -> Result<(), Shutdown> {
        let received_at = OffsetDateTime::now_utc();
        self.set_health(|r| r.last_wire_frame_at = Some(now));
        let (payloads, missing) = match parse_activity_frame(text) {
            Ok(parsed) => parsed,
            Err(error) => {
                warn!(slot = self.slot, error = %error, "unparseable activity frame");
                return Ok(());
            }
        };
        if missing > 0 {
            warn!(slot = self.slot, missing, "activity frames without payload");
        }
        if payloads.is_empty() {
            return Ok(());
        }
        // One snapshot per frame: every lookup sees the same generation.
        let watchlist = self.live_watchlist.snapshot();
        let watched: HashSet<WalletAddress> = watchlist.entries.iter().map(|e| e.wallet).collect();

        let mut normalized: u64 = 0;
        let mut rejected: usize = 0;
        let mut last_rejection = None;
        for payload in payloads {
            // Normalize FIRST — exactly once per payload, through the parser the
            // REST path uses (the frame receipt is injected; replay passes the
            // envelope's recorded instant instead, #530 review F7) — then filter
            // on the normalized wallet and reuse that same value for delivery.
            let trade = match trade_parser::parse_ws_trade(payload, received_at) {
                Ok(trade) => trade,
                Err(error) => {
                    rejected += 1;
                    last_rejection = Some(error.to_string());
                    continue;
                }
            };
            normalized += 1;
            if normalized == 1 {
                // Parser acceptance — watched or not — is the liveness and
                // backoff-progress signal; acknowledgements and rejected payloads
                // never are (#546).
                *deadline = now + activity_timeout();
                backoff.on_normalized_row();
                self.set_health(|r| {
                    r.last_normalized_activity_at = Some(now);
                    r.consecutive_reconnects = 0;
                });
            }
            if !watched.contains(&trade.wallet) {
                continue;
            }
            self.deliver(Observation {
                slot: self.slot,
                payload: payload.to_vec(),
                trade,
            })
            .await?;
        }
        self.set_health(|r| {
            r.normalized_activity_rows_total =
                r.normalized_activity_rows_total.saturating_add(normalized);
        });
        if rejected > 0 {
            warn!(
                slot = self.slot,
                rejected,
                normalized,
                last_rejection,
                "activity payloads rejected by the normalizer"
            );
        }
        Ok(())
    }

    /// Hand one watched observation to the coordinator. A full channel blocks
    /// here holding the item — no further wire frame is read, so backpressure
    /// reaches the socket; a closed channel is orderly shutdown.
    async fn deliver(&self, observation: Observation) -> Result<(), Shutdown> {
        let observation = match self.fan_in.try_send(observation) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Closed(_)) => return Err(Shutdown),
            Err(TrySendError::Full(observation)) => observation,
        };
        self.set_health(|r| r.fan_in_blocked = true);
        let sent = self.fan_in.send(observation).await;
        self.set_health(|r| r.fan_in_blocked = false);
        sent.map_err(|_| Shutdown)
    }

    fn set_health(&self, f: impl FnOnce(&mut ReaderHealth)) {
        let mut h = self
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(reader) = h.ws_readers.get_mut(self.slot) {
            f(reader);
        }
    }
}

// ── Coordinator ──────────────────────────────────────────────────────────────

/// Single owner of the source event log and the orchestrator's trade sender.
struct Coordinator {
    sink: SourceEventSink,
    trade_tx: mpsc::Sender<IncomingTrade>,
    health: SharedHealth,
    fan_in: mpsc::Receiver<Observation>,
}

impl Coordinator {
    /// Run until every reader is gone or the trade channel closes — the latter
    /// is noticed immediately, not only at the next delivery.
    async fn run(mut self) {
        loop {
            let observation = tokio::select! {
                biased;
                () = self.trade_tx.closed() => return,
                received = self.fan_in.recv() => match received {
                    Some(observation) => observation,
                    None => return,
                },
            };
            if self.append_with_recovery(&observation).await.is_err() {
                return;
            }
            // Bounded channel: a full queue blocks here → the fan-in fills →
            // the readers block on their sockets; a watched trade is never dropped.
            if self.trade_tx.send(observation.trade).await.is_err() {
                return;
            }
        }
    }

    /// Durably append one watched payload, entering the poison-recovery loop on
    /// failure: bounded-backoff reopen + full revalidation, retrying THIS
    /// payload until it lands (#530 review F4/F1 — delivery from every reader
    /// blocks while poisoned; the trade is retained, not assumed onto REST).
    async fn append_with_recovery(&mut self, observation: &Observation) -> Result<(), Shutdown> {
        let trade = &observation.trade;
        let envelope = || EnvelopeIn {
            source_id: SourceId(ACTIVITY_WS_SOURCE_ID.to_string()),
            schema_version: ACTIVITY_WS_SCHEMA_VERSION,
            parser_version: ACTIVITY_WS_PARSER_VERSION,
            observed_at: SourceTimestamp(trade.observed_at),
            received_at: ReceivedAt(trade.received_at),
            content_type: ContentType::Json,
            payload: observation.payload.clone(),
        };
        match self.sink.append_durable(envelope()) {
            Ok(_) => return Ok(()),
            Err(error) => {
                self.set_health(|h| h.ws_sink_poisoned = true);
                warn!(error = %error, slot = observation.slot, trade = %trade.source_trade_id,
                    "source log append failed; sink poisoned — holding delivery and retrying reopen");
            }
        }
        let mut attempt: u32 = 0;
        loop {
            if self.trade_tx.is_closed() {
                return Err(Shutdown);
            }
            tokio::time::sleep(Duration::from_secs(backoff_secs(attempt))).await;
            attempt = attempt.saturating_add(1);
            if !self.sink.try_reopen() {
                continue;
            }
            match self.sink.append_durable(envelope()) {
                Ok(_) => {
                    self.set_health(|h| h.ws_sink_poisoned = false);
                    info!(trade = %trade.source_trade_id,
                        "source log recovered; held payload appended durably");
                    return Ok(());
                }
                Err(error) => {
                    warn!(error = %error, "source log re-poisoned immediately after reopen");
                }
            }
        }
    }

    fn set_health(&self, f: impl FnOnce(&mut HealthState)) {
        let mut h = self
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut h);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::health::new_shared_health_with_ws;
    use pe_event_log::Reader as LogReader;

    const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn observation(tx: &str) -> Observation {
        let payload = format!(
            r#"{{"proxyWallet":"{WALLET}","conditionId":"0xc1","side":"BUY","size":"5","price":"0.5","timestamp":"1704067200","transactionHash":"{tx}","outcomeIndex":"0"}}"#
        )
        .into_bytes();
        let received = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
        let trade = trade_parser::parse_ws_trade(&payload, received).unwrap();
        Observation {
            slot: 0,
            payload,
            trade,
        }
    }

    async fn settle() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }

    /// Sink uncertainty holds the current item, blocks delivery from every
    /// reader, retries that exact item first after a successful reopen, and
    /// resumes in order; a failed reopen keeps it held.
    #[tokio::test(start_paused = true)]
    async fn coordinator_holds_item_through_poison_and_resumes_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let mut sink = SourceEventSink::open(&path).unwrap();
        sink.fail_next_append();
        sink.fail_next_reopen();
        let (fan_in_tx, fan_in_rx) = mpsc::channel(8);
        let (trade_tx, mut trade_rx) = mpsc::channel(8);
        let health = new_shared_health_with_ws(false, true, 90);
        let task = tokio::spawn(
            Coordinator {
                sink,
                trade_tx,
                health: health.clone(),
                fan_in: fan_in_rx,
            }
            .run(),
        );
        fan_in_tx.send(observation("0xa")).await.unwrap();
        fan_in_tx.send(observation("0xb")).await.unwrap();
        settle().await;
        assert!(
            health.lock().unwrap().ws_sink_poisoned,
            "first append poisons"
        );
        assert!(
            trade_rx.try_recv().is_err(),
            "delivery blocked while poisoned"
        );

        // Attempt 0 (1s backoff): reopen fails (armed) — still poisoned, still held.
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert!(health.lock().unwrap().ws_sink_poisoned);
        assert!(trade_rx.try_recv().is_err());

        // Attempt 1 (2s backoff): reopen revalidates, the HELD item lands first,
        // then the queued one, in order.
        tokio::time::advance(Duration::from_secs(2)).await;
        settle().await;
        assert!(!health.lock().unwrap().ws_sink_poisoned);
        assert_eq!(trade_rx.recv().await.unwrap().source_trade_id.0, "0xa");
        assert_eq!(trade_rx.recv().await.unwrap().source_trade_id.0, "0xb");

        drop(fan_in_tx);
        task.await.unwrap();
        let ids: Vec<String> = LogReader::replay(&path)
            .unwrap()
            .map(|item| {
                let (_seq, env) = item.unwrap();
                trade_parser::parse_ws_trade(&env.payload, env.received_at.0)
                    .unwrap()
                    .source_trade_id
                    .0
            })
            .collect();
        assert_eq!(ids, vec!["0xa".to_string(), "0xb".to_string()]);
    }

    /// A receiver closed before the coordinator runs wins over buffered fan-in
    /// work: nothing is appended for a destination that no longer exists.
    #[tokio::test(start_paused = true)]
    async fn coordinator_prefers_closed_receiver_over_buffered_observations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("source.log");
        let sink = SourceEventSink::open(&path).unwrap();
        let (fan_in_tx, fan_in_rx) = mpsc::channel(8);
        let (trade_tx, trade_rx) = mpsc::channel(8);
        fan_in_tx.send(observation("0xa")).await.unwrap();
        drop(trade_rx);
        Coordinator {
            sink,
            trade_tx,
            health: new_shared_health_with_ws(false, true, 90),
            fan_in: fan_in_rx,
        }
        .run()
        .await;
        assert_eq!(LogReader::replay(&path).unwrap().count(), 0);
    }

    /// A closed trade channel ends the coordinator (orderly), even mid-recovery.
    #[tokio::test(start_paused = true)]
    async fn coordinator_exits_when_trade_channel_closes_during_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = SourceEventSink::open(dir.path().join("source.log")).unwrap();
        sink.fail_next_append();
        let (fan_in_tx, fan_in_rx) = mpsc::channel(8);
        let (trade_tx, trade_rx) = mpsc::channel(8);
        let health = new_shared_health_with_ws(false, true, 90);
        let task = tokio::spawn(
            Coordinator {
                sink,
                trade_tx,
                health,
                fan_in: fan_in_rx,
            }
            .run(),
        );
        fan_in_tx.send(observation("0xa")).await.unwrap();
        settle().await;
        drop(trade_rx);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("coordinator must exit once downstream closes")
            .unwrap();
    }
}
