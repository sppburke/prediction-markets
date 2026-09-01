//! #530/#546: websocket-primary trade ingest — three independent readers, one
//! coordinator.
//!
//! Each reader owns one live-data socket and one persistent reconnect backoff.
//! It parses every frame's activity payloads, normalizes EACH payload exactly
//! once through the same parser as the REST path
//! (`parse_activity_trade_observation`), and only then filters on the normalized
//! wallet against one watchlist snapshot per frame. A parser-accepted row —
//! watched or not — is the ONLY evidence a reader is alive: the 2026-08-31
//! experiments (#546) showed a connection can stay open and acknowledged while
//! delivering nothing, so 30 s without one drops the socket and the reader
//! re-dials after its own backoff while the other readers keep delivering.
//!
//! Watched rows travel `(slot, exact raw bytes, normalized trigger)` over ONE
//! bounded fan-in channel (capacity = the trigger channel's) to the coordinator,
//! which keeps the pre-#546 order: durable source-log append+sync, then the
//! bounded reconciliation-trigger channel. Public polling pages enter that
//! same coordinator through a second bounded input and wait for the same durable
//! acknowledgement. Reader copies are never coalesced before recording; the
//! reconciliation owner coalesces their durable obligations per wallet.
//!
//! Failure semantics:
//! - full fan-in or trigger channel → the sender blocks holding its one item
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
//!   F4/F1: the observation is retained, never assumed onto REST).
//! - payload rejected by the normalizer → counted and warned once per frame;
//!   the REST path parses the same shape with the same converter, so the
//!   poller either delivers it or freezes the wallet cursor on it (#511).
//! - reader silence → drop + backoff + re-dial, per slot. `ActivityIngest::run`
//!   owns every task in one `JoinSet`; no detached reader survives it.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use pe_copy_signal_engine::TradeProvenance;
use pe_core_types::{
    EventSeq, ReceivedAt, SourceId, SourceTimestamp, SourceTradeId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn};
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ACTIVITY_WS_NORMALIZED_ACTIVITY_TIMEOUT_SECS,
    ACTIVITY_WS_READER_COUNT, ActivityWsError, ActivityWsStream, ReconnectBackoff, WireFrame,
    backoff_secs, parse_activity_frame, parse_activity_trade_observation,
};
use time::OffsetDateTime;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::health::{HealthState, ReaderHealth, SharedHealth};
use crate::live_watchlist::LiveWatchlist;
use crate::source_event_sink::SourceEventSink;

/// Source id stamped on every websocket envelope in the source event log.
pub const ACTIVITY_WS_SOURCE_ID: &str = "polymarket-activity-ws";

/// A durable raw observation that requires complete fixed-end reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationTrigger {
    pub wallet: WalletAddress,
    pub source_time: OffsetDateTime,
    pub source_trade_id: SourceTradeId,
    pub provenance: TradeProvenance,
    pub received_at: OffsetDateTime,
}

/// Bounded producer handle for non-websocket source pages. The coordinator
/// acknowledges only after append and synchronization complete (#544).
#[derive(Clone)]
pub struct SourceLogHandle {
    tx: mpsc::Sender<SourceLogRequest>,
}

struct SourceLogRequest {
    envelope: EnvelopeIn,
    appended: oneshot::Sender<EventSeq>,
}

pub struct SourceLogReceiver {
    rx: mpsc::Receiver<SourceLogRequest>,
}

#[derive(Debug, thiserror::Error)]
pub enum SourceLogHandleError {
    #[error("source-log coordinator closed")]
    Closed,
}

impl SourceLogHandle {
    /// Build the bounded external input owned by [`ActivityIngest`].
    pub fn channel(capacity: usize) -> (Self, SourceLogReceiver) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx }, SourceLogReceiver { rx })
    }

    /// Record one source page and wait for its durable append acknowledgement.
    pub async fn append(&self, envelope: EnvelopeIn) -> Result<EventSeq, SourceLogHandleError> {
        let (appended, acknowledgement) = oneshot::channel();
        self.tx
            .send(SourceLogRequest { envelope, appended })
            .await
            .map_err(|_| SourceLogHandleError::Closed)?;
        acknowledgement
            .await
            .map_err(|_| SourceLogHandleError::Closed)
    }
}

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
    trigger: ReconciliationTrigger,
}

/// A downstream receiver closed: the service is shutting down.
struct Shutdown;

pub struct ActivityIngest {
    reader: Option<ReaderConfig>,
    sink: SourceEventSink,
    source_rx: SourceLogReceiver,
    trigger_tx: mpsc::Sender<ReconciliationTrigger>,
    health: SharedHealth,
}

struct ReaderConfig {
    live_watchlist: LiveWatchlist,
    dialer: Dialer,
}

impl ActivityIngest {
    pub fn new(
        live_watchlist: LiveWatchlist,
        sink: SourceEventSink,
        source_rx: SourceLogReceiver,
        trigger_tx: mpsc::Sender<ReconciliationTrigger>,
        health: SharedHealth,
    ) -> Self {
        Self {
            reader: Some(ReaderConfig {
                live_watchlist,
                dialer: Arc::new(|_slot| Box::pin(ActivityWsStream::connect_and_subscribe())),
            }),
            sink,
            source_rx,
            trigger_tx,
            health,
        }
    }

    /// Source-log coordinator without websocket readers (poll-only rollback posture).
    pub fn poll_only(
        sink: SourceEventSink,
        source_rx: SourceLogReceiver,
        trigger_tx: mpsc::Sender<ReconciliationTrigger>,
        health: SharedHealth,
    ) -> Self {
        Self {
            reader: None,
            sink,
            source_rx,
            trigger_tx,
            health,
        }
    }

    /// [`Self::new`] with an injected dialer (in-process peers). Scenario builds only;
    /// production is fixed to the endpoint dialed by `new`.
    #[cfg(feature = "scenario")]
    pub fn with_dialer(
        live_watchlist: LiveWatchlist,
        sink: SourceEventSink,
        source_rx: SourceLogReceiver,
        trigger_tx: mpsc::Sender<ReconciliationTrigger>,
        health: SharedHealth,
        dialer: Dialer,
    ) -> Self {
        Self {
            reader: Some(ReaderConfig {
                live_watchlist,
                dialer,
            }),
            sink,
            source_rx,
            trigger_tx,
            health,
        }
    }

    /// Run until the trigger channel closes (service shutdown).
    ///
    /// Owns every reader and the coordinator in one `JoinSet`: the first child
    /// to finish (a closed downstream channel) ends the pool — the rest are
    /// aborted and drained. Dropping this future (external abort) drops the
    /// set, which aborts its children without joining them.
    pub async fn run(self) {
        let (fan_in_tx, fan_in_rx) = mpsc::channel(self.trigger_tx.max_capacity());
        let mut tasks = JoinSet::new();
        let fan_in_guard = if let Some(reader) = self.reader {
            for slot in 0..ACTIVITY_WS_READER_COUNT {
                tasks.spawn(
                    Reader {
                        slot,
                        dialer: reader.dialer.clone(),
                        live_watchlist: reader.live_watchlist.clone(),
                        health: self.health.clone(),
                        fan_in: fan_in_tx.clone(),
                    }
                    .run(),
                );
            }
            None
        } else {
            // Keep the unused fan-in open so poll-only mode is owned solely by
            // the external source channel.
            Some(fan_in_tx.clone())
        };
        drop(fan_in_tx);
        tasks.spawn(
            Coordinator {
                sink: self.sink,
                trigger_tx: self.trigger_tx,
                health: self.health,
                fan_in: fan_in_rx,
                source_rx: self.source_rx.rx,
            }
            .run(),
        );
        if tasks.join_next().await.is_some() {
            info!("activity ingest child finished; stopping the reader pool");
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
        drop(fan_in_guard);
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
            let activity = match parse_activity_trade_observation(payload) {
                Ok(activity) => activity,
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
            if !watched.contains(&activity.wallet) {
                continue;
            }
            self.deliver(Observation {
                slot: self.slot,
                payload: payload.to_vec(),
                trigger: ReconciliationTrigger {
                    wallet: activity.wallet,
                    source_time: activity.source_time.0,
                    source_trade_id: activity.group_id.key().clone(),
                    provenance: TradeProvenance::ActivityWs,
                    received_at,
                },
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

/// Single owner of the source event log and reconciliation-trigger sender.
struct Coordinator {
    sink: SourceEventSink,
    trigger_tx: mpsc::Sender<ReconciliationTrigger>,
    health: SharedHealth,
    fan_in: mpsc::Receiver<Observation>,
    source_rx: mpsc::Receiver<SourceLogRequest>,
}

impl Coordinator {
    /// Run until every producer is gone or the trigger channel closes — the latter
    /// is noticed immediately, not only at the next delivery.
    async fn run(mut self) {
        loop {
            enum Input {
                Reader(Observation),
                Source(SourceLogRequest),
            }
            let input = tokio::select! {
                biased;
                () = self.trigger_tx.closed() => return,
                input = async {
                    tokio::select! {
                        received = self.fan_in.recv() => received.map(Input::Reader),
                        received = self.source_rx.recv() => received.map(Input::Source),
                    }
                } => match input {
                    Some(input) => input,
                    None => return,
                }
            };
            match input {
                Input::Reader(observation) => {
                    let envelope = EnvelopeIn {
                        source_id: SourceId(ACTIVITY_WS_SOURCE_ID.to_string()),
                        schema_version: ACTIVITY_SCHEMA_VERSION,
                        parser_version: ACTIVITY_PARSER_VERSION,
                        observed_at: SourceTimestamp(observation.trigger.source_time),
                        received_at: ReceivedAt(observation.trigger.received_at),
                        content_type: ContentType::Json,
                        payload: observation.payload.clone(),
                    };
                    let label = observation.trigger.source_trade_id.clone();
                    let slot = Some(observation.slot);
                    if self
                        .append_with_recovery(envelope, &label, slot)
                        .await
                        .is_err()
                    {
                        return;
                    }
                    // Bounded channel: a full queue blocks here → the fan-in fills →
                    // readers block on their sockets. Trigger delivery always follows sync.
                    if self.trigger_tx.send(observation.trigger).await.is_err() {
                        return;
                    }
                }
                Input::Source(request) => {
                    let label = SourceTradeId("poll-page".to_owned());
                    let seq = match self
                        .append_with_recovery(request.envelope, &label, None)
                        .await
                    {
                        Ok(seq) => seq,
                        Err(Shutdown) => return,
                    };
                    let _ = request.appended.send(seq);
                }
            }
        }
    }

    /// Durably append one watched payload, entering the poison-recovery loop on
    /// failure: bounded-backoff reopen + full revalidation, retrying THIS
    /// payload until it lands (#530 review F4/F1 — delivery from every reader
    /// blocks while poisoned; the observation is retained, not assumed onto REST).
    async fn append_with_recovery(
        &mut self,
        envelope: EnvelopeIn,
        label: &SourceTradeId,
        slot: Option<usize>,
    ) -> Result<EventSeq, Shutdown> {
        match self.sink.append_durable(duplicate_envelope(&envelope)) {
            Ok(seq) => return Ok(seq),
            Err(error) => {
                self.set_health(|h| h.ws_sink_poisoned = true);
                warn!(error = %error, ?slot, trade = %label,
                    "source log append failed; sink poisoned — holding delivery and retrying reopen");
            }
        }
        let mut attempt: u32 = 0;
        loop {
            if self.trigger_tx.is_closed() {
                return Err(Shutdown);
            }
            tokio::time::sleep(Duration::from_secs(backoff_secs(attempt))).await;
            attempt = attempt.saturating_add(1);
            if !self.sink.try_reopen() {
                continue;
            }
            match self.sink.append_durable(duplicate_envelope(&envelope)) {
                Ok(seq) => {
                    self.set_health(|h| h.ws_sink_poisoned = false);
                    info!(trade = %label,
                        "source log recovered; held payload appended durably");
                    return Ok(seq);
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

fn duplicate_envelope(envelope: &EnvelopeIn) -> EnvelopeIn {
    EnvelopeIn {
        source_id: envelope.source_id.clone(),
        schema_version: envelope.schema_version,
        parser_version: envelope.parser_version,
        observed_at: envelope.observed_at.clone(),
        received_at: envelope.received_at.clone(),
        content_type: envelope.content_type.clone(),
        payload: envelope.payload.clone(),
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
            r#"{{"proxyWallet":"{WALLET}","conditionId":"0xc1","asset":"123","side":"BUY","size":"5","price":"0.5","timestamp":"1704067200","transactionHash":"{tx}","outcomeIndex":"0"}}"#
        )
        .into_bytes();
        let received = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
        let activity = parse_activity_trade_observation(&payload).unwrap();
        Observation {
            slot: 0,
            payload,
            trigger: ReconciliationTrigger {
                wallet: activity.wallet,
                source_time: activity.source_time.0,
                source_trade_id: activity.group_id.key().clone(),
                provenance: TradeProvenance::ActivityWs,
                received_at: received,
            },
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
        let (trigger_tx, mut trigger_rx) = mpsc::channel(8);
        let (_source_log, source_rx) = SourceLogHandle::channel(8);
        let health = new_shared_health_with_ws(false, true, 90);
        let task = tokio::spawn(
            Coordinator {
                sink,
                trigger_tx,
                health: health.clone(),
                fan_in: fan_in_rx,
                source_rx: source_rx.rx,
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
            trigger_rx.try_recv().is_err(),
            "delivery blocked while poisoned"
        );

        // Attempt 0 (1s backoff): reopen fails (armed) — still poisoned, still held.
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert!(health.lock().unwrap().ws_sink_poisoned);
        assert!(trigger_rx.try_recv().is_err());

        // Attempt 1 (2s backoff): reopen revalidates, the HELD item lands first,
        // then the queued one, in order.
        tokio::time::advance(Duration::from_secs(2)).await;
        settle().await;
        assert!(!health.lock().unwrap().ws_sink_poisoned);
        let first = trigger_rx.recv().await.unwrap().source_trade_id;
        let second = trigger_rx.recv().await.unwrap().source_trade_id;
        assert_ne!(first, second);

        drop(fan_in_tx);
        task.await.unwrap();
        let ids: Vec<String> = LogReader::replay(&path)
            .unwrap()
            .map(|item| {
                let (_seq, env) = item.unwrap();
                parse_activity_trade_observation(&env.payload)
                    .unwrap()
                    .group_id
                    .components()
                    .transaction_hash
                    .clone()
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
        let (trigger_tx, trigger_rx) = mpsc::channel(8);
        let (_source_log, source_rx) = SourceLogHandle::channel(8);
        fan_in_tx.send(observation("0xa")).await.unwrap();
        drop(trigger_rx);
        Coordinator {
            sink,
            trigger_tx,
            health: new_shared_health_with_ws(false, true, 90),
            fan_in: fan_in_rx,
            source_rx: source_rx.rx,
        }
        .run()
        .await;
        assert_eq!(LogReader::replay(&path).unwrap().count(), 0);
    }

    /// A closed trigger channel ends the coordinator (orderly), even mid-recovery.
    #[tokio::test(start_paused = true)]
    async fn coordinator_exits_when_trade_channel_closes_during_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut sink = SourceEventSink::open(dir.path().join("source.log")).unwrap();
        sink.fail_next_append();
        let (fan_in_tx, fan_in_rx) = mpsc::channel(8);
        let (trigger_tx, trigger_rx) = mpsc::channel(8);
        let (_source_log, source_rx) = SourceLogHandle::channel(8);
        let health = new_shared_health_with_ws(false, true, 90);
        let task = tokio::spawn(
            Coordinator {
                sink,
                trigger_tx,
                health,
                fan_in: fan_in_rx,
                source_rx: source_rx.rx,
            }
            .run(),
        );
        fan_in_tx.send(observation("0xa")).await.unwrap();
        settle().await;
        drop(trigger_rx);
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("coordinator must exit once downstream closes")
            .unwrap();
    }
}
