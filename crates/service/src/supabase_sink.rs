//! Best-effort Supabase analytics sink (issue #343 PR2).
//!
//! Dual-writes paper fills and market settlements to Supabase for the historical-vs-live
//! analytics site. The local SQLite paper-state ([`PaperStateDb`]) stays authoritative;
//! Supabase is a display/analytics mirror. Writes are **best-effort**: a Supabase outage
//! never blocks or errors the trade path. The trade path enqueues events through a
//! non-blocking [`SinkHandle::send_fill`] / [`SinkHandle::send_resolution`] (dropped on a
//! full channel, with a counter), and dropped or failed writes self-heal on the next
//! reconcile.
//!
//! Reconcile model:
//! - **Fills** use a contiguous-prefix high-water-mark (`event_seq`) over the ordered
//!   [`PaperStateDb::list_fills`] stream. The HWM advances along the prefix while each
//!   successive fill is confirmed-written (or intentionally skipped — see
//!   [`supabase_fill_from`]) and **halts at the first failed write**. It is *not* integer
//!   adjacency (`== hwm + 1`): `event_seq` is the sparse event-log frame number, so the
//!   prefix is over the *ordering*, not the integer values. Re-upserts are idempotent on
//!   `idempotency_key`, so re-scanning a confirmed prefix never double-writes.
//! - **Settlements** carry no cursor; each reconcile re-upserts the full (small)
//!   [`PaperStateDb::list_settled_markets`] set, idempotent on `market_id`. The canonical
//!   `outcome_prices` JSON comes straight from SQLite, so live and reconcile writes agree.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use pe_core_types::Side;
use pe_paper_state::{
    FillMarketSnapshot, FillRow, PaperStateDb, PaperStateError, SettledMarketRow,
};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::paper_api::ParsedKey;
use crate::supabase_reader::auth_token;

/// Events the trade path enqueues for the sink. Best-effort; dropped on a full channel.
#[derive(Debug)]
pub enum SinkEvent {
    /// A paper fill just committed. Carries the row so the sink upserts it immediately for
    /// freshness; the periodic reconcile re-derives the same row from `list_fills()`.
    Fill(FillRow),
    /// One or more markets settled this tick. Triggers a full re-upsert of the
    /// `list_settled_markets()` set (SQLite-canonical JSON, idempotent on `market_id`).
    Resolution,
}

/// Cheap, clonable handle the trade path uses to enqueue events without ever blocking.
#[derive(Clone)]
pub struct SinkHandle {
    tx: mpsc::Sender<SinkEvent>,
    dropped: Arc<AtomicU64>,
}

impl SinkHandle {
    /// Build a handle + its receiver with a bounded channel of `capacity`.
    pub fn channel(capacity: usize) -> (SinkHandle, mpsc::Receiver<SinkEvent>) {
        let (tx, rx) = mpsc::channel(capacity);
        (
            SinkHandle {
                tx,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            rx,
        )
    }

    /// The shared dropped-event counter, handed to [`run_sink`] for periodic logging.
    pub fn dropped_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.dropped)
    }

    /// Enqueue a fill. Non-blocking: on a full/closed channel the event is dropped and the
    /// dropped-counter incremented (the periodic reconcile heals it). Never errors, never
    /// blocks, never panics — safe to call from the trade hot path.
    pub fn send_fill(&self, row: FillRow) {
        if self.tx.try_send(SinkEvent::Fill(row)).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Enqueue a "settled set changed" nudge. Non-blocking; see [`SinkHandle::send_fill`].
    pub fn send_resolution(&self) {
        if self.tx.try_send(SinkEvent::Resolution).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Errors from the Supabase writer boundary. Consumed inside [`run_sink`] / the reconcile
/// helpers (logged; the fill HWM halts on the offending row) — never propagated into the
/// trade path.
#[derive(Debug, thiserror::Error)]
pub enum SinkError {
    #[error("supabase transport: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("supabase status {0}")]
    Status(u16),
    #[error("supabase decode: {0}")]
    Decode(#[source] reqwest::Error),
    #[error("paper-state: {0}")]
    PaperState(#[from] PaperStateError),
    #[error("serialize outcome_prices: {0}")]
    Serialize(#[from] serde_json::Error),
}

/// A fill enriched with the leader / source-trade-id / entry-unix parsed from its
/// idempotency key, ready for the `paper_fills` upsert.
#[derive(Debug, Clone)]
pub struct SupabaseFillRow {
    pub leader_wallet: String,
    pub source_trade_id: Option<String>,
    pub entry_unix: Option<i64>,
    pub fill: FillRow,
}

/// Parse a [`FillRow`] into a [`SupabaseFillRow`]. Returns `None` for a non-`wf|` key (the
/// idempotency key has no leader) — such rows are logged and skipped rather than written
/// with a null leader, which would orphan from the `wallet_live_stats` view. In practice
/// every Winner-Follow fill is `wf`-keyed.
pub fn supabase_fill_from(row: &FillRow) -> Option<SupabaseFillRow> {
    let parsed = ParsedKey::from_key(&row.idempotency_key);
    let leader_wallet = parsed.leader?;
    Some(SupabaseFillRow {
        leader_wallet,
        source_trade_id: parsed.source_trade_id,
        entry_unix: parsed.entry_unix,
        fill: row.clone(),
    })
}

const fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// Idempotent Supabase upserts + the fill catch-up cursor. Abstracted as a trait so
/// scenario tests can drive [`run_sink`] / [`reconcile_fills`] with an in-memory fake and
/// inject write failures deterministically (no live network).
pub trait SinkWriter: Send + Sync + 'static {
    fn upsert_fill(
        &self,
        row: &SupabaseFillRow,
    ) -> impl Future<Output = Result<(), SinkError>> + Send;
    fn upsert_settled(
        &self,
        row: &SettledMarketRow,
    ) -> impl Future<Output = Result<(), SinkError>> + Send;
    /// Upsert a fill's liquidity-at-fill snapshot (issue #350 WS2 PR-H). Best-effort: the
    /// canonical row already lives in SQLite, so a failure here is logged, never fatal.
    fn upsert_snapshot(
        &self,
        row: &FillMarketSnapshot,
    ) -> impl Future<Output = Result<(), SinkError>> + Send;
    fn read_hwm(&self) -> impl Future<Output = Result<i64, SinkError>> + Send;
    fn write_hwm(&self, last_event_seq: i64) -> impl Future<Output = Result<(), SinkError>> + Send;
}

#[derive(Debug, Deserialize)]
struct HwmRow {
    last_event_seq: i64,
}

/// Production [`SinkWriter`]: PostgREST upserts over reqwest, mirroring the header pattern
/// in [`crate::supabase_reader`] (the same token in both `apikey` and `Authorization`,
/// preferring the service-role secret so writes bypass RLS).
pub struct SupabaseWriter {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

impl SupabaseWriter {
    pub fn new(client: reqwest::Client, base_url: &str, anon_key: &str, secret_key: &str) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: auth_token(anon_key, secret_key).to_string(),
        }
    }

    /// `POST {base}/rest/v1/{table}?on_conflict={key}` with merge-duplicates upsert.
    async fn post_upsert(
        &self,
        table: &str,
        on_conflict: &str,
        body: &serde_json::Value,
    ) -> Result<(), SinkError> {
        let url = format!(
            "{}/rest/v1/{}?on_conflict={}",
            self.base_url, table, on_conflict
        );
        let resp = self
            .client
            .post(&url)
            .header("apikey", &self.token)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .header("Prefer", "resolution=merge-duplicates")
            .json(body)
            .send()
            .await
            .map_err(SinkError::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(SinkError::Status(status.as_u16()));
        }
        Ok(())
    }
}

impl SinkWriter for SupabaseWriter {
    async fn upsert_fill(&self, row: &SupabaseFillRow) -> Result<(), SinkError> {
        // Decimals are sent as strings (no f64) — Postgres coerces text → numeric.
        let body = serde_json::json!([{
            "idempotency_key": row.fill.idempotency_key,
            "leader_wallet": row.leader_wallet,
            "source_trade_id": row.source_trade_id,
            "market_id": row.fill.market_id.0.0,
            "outcome_id": row.fill.outcome_id.0,
            "side": side_str(row.fill.side),
            "contracts": row.fill.contracts,
            "fill_price": row.fill.fill_price.0.to_string(),
            "entry_unix": row.entry_unix,
            "event_seq": row.fill.event_seq,
        }]);
        self.post_upsert("paper_fills", "idempotency_key", &body)
            .await
    }

    async fn upsert_settled(&self, row: &SettledMarketRow) -> Result<(), SinkError> {
        // `outcome_prices_json` is a JSON array of text decimals; embed it as a jsonb value.
        let outcome_prices: serde_json::Value = serde_json::from_str(&row.outcome_prices_json)?;
        let body = serde_json::json!([{
            "market_id": row.market_id.0.0,
            "outcome_prices": outcome_prices,
            "credit_applied": row.credit_applied.to_string(),
            "settled_at_unix": row.settled_at_unix,
        }]);
        self.post_upsert("settled_markets", "market_id", &body)
            .await
    }

    async fn upsert_snapshot(&self, row: &FillMarketSnapshot) -> Result<(), SinkError> {
        // Decimals as strings (no f64); Postgres coerces text → numeric. `ask_levels_json`
        // is a JSON array text → re-embed as a jsonb value (a SQL null when absent).
        let ask_levels: Option<serde_json::Value> = row
            .ask_levels_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?;
        let body = serde_json::json!([{
            "idempotency_key": row.idempotency_key,
            "liquidity": row.liquidity.map(|d| d.to_string()),
            "volume": row.volume.map(|d| d.to_string()),
            "absorbable_usd_100bps": row.absorbable_usd_100bps.map(|d| d.to_string()),
            "ask_levels_json": ask_levels,
            "captured_at_unix": row.captured_at_unix,
        }]);
        self.post_upsert("fill_market_snapshots", "idempotency_key", &body)
            .await
    }

    async fn read_hwm(&self) -> Result<i64, SinkError> {
        let url = format!(
            "{}/rest/v1/supabase_sink_hwm?select=last_event_seq&id=eq.1",
            self.base_url
        );
        let resp = self
            .client
            .get(&url)
            .header("apikey", &self.token)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .send()
            .await
            .map_err(SinkError::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(SinkError::Status(status.as_u16()));
        }
        let rows: Vec<HwmRow> = resp.json().await.map_err(SinkError::Decode)?;
        // Absent row = "nothing sunk" = -1, matching the schema seed (#510): the `> hwm`
        // filter then includes fill seq 0.
        Ok(rows.first().map(|r| r.last_event_seq).unwrap_or(-1))
    }

    async fn write_hwm(&self, last_event_seq: i64) -> Result<(), SinkError> {
        let body = serde_json::json!([{ "id": 1, "last_event_seq": last_event_seq }]);
        self.post_upsert("supabase_sink_hwm", "id", &body).await
    }
}

/// Catch up `paper_fills` from the high-water-mark, returning `(new_hwm, complete)`.
///
/// Scans the ordered `list_fills()` stream for `event_seq > hwm`, upserting each. The HWM
/// advances along the prefix on every confirmed write (or an intentional non-`wf` skip) and
/// **halts at the first failed write**, so a transient Supabase error leaves the tail for
/// the next reconcile rather than silently skipping it. `complete` is `false` when the
/// sweep halted early (#510): the best-effort runtime sink ignores it (the next reconcile
/// retries), but the one-time `--backfill-supabase` must NOT seed cursors on a partial
/// sweep — that would permanently hide the omitted fills.
pub async fn reconcile_fills<W: SinkWriter>(
    writer: &W,
    paper_state: &PaperStateDb,
    hwm: i64,
) -> Result<(i64, bool), SinkError> {
    let mut new_hwm = hwm;
    let mut complete = true;
    for row in paper_state
        .list_fills()?
        .into_iter()
        .filter(|f| f.event_seq > hwm)
    {
        match supabase_fill_from(&row) {
            Some(sup) => match writer.upsert_fill(&sup).await {
                Ok(()) => new_hwm = row.event_seq,
                Err(e) => {
                    warn!(
                        error = %e,
                        event_seq = row.event_seq,
                        "supabase sink: fill upsert failed; halting catch-up at last confirmed prefix"
                    );
                    complete = false;
                    break;
                }
            },
            // Permanently un-writable (no leader): advance past it, never block the prefix.
            None => {
                warn!(
                    key = %row.idempotency_key,
                    "supabase sink: skipping non-winner-follow fill (no leader in idempotency key)"
                );
                new_hwm = row.event_seq;
            }
        }
    }
    Ok((new_hwm, complete))
}

/// Re-upsert the full settled-market set (idempotent on `market_id`). A single upsert
/// failure is logged and skipped — the next reconcile retries it.
pub async fn reconcile_settled<W: SinkWriter>(
    writer: &W,
    paper_state: &PaperStateDb,
) -> Result<(), SinkError> {
    for row in paper_state.list_settled_markets()? {
        if let Err(e) = writer.upsert_settled(&row).await {
            warn!(
                market = %row.market_id.0.0,
                error = %e,
                "supabase sink: settled upsert failed; will retry next reconcile"
            );
        }
    }
    Ok(())
}

/// One full reconcile pass: read the HWM, catch up fills, persist the advanced HWM, then
/// re-upsert the settled set. All failures are logged and swallowed (best-effort).
async fn reconcile<W: SinkWriter>(writer: &W, paper_state: &PaperStateDb) {
    match writer.read_hwm().await {
        // Best-effort path: partial completion is fine — the next reconcile retries the tail.
        Ok(hwm) => match reconcile_fills(writer, paper_state, hwm)
            .await
            .map(|(h, _)| h)
        {
            Ok(new_hwm) if new_hwm > hwm => {
                if let Err(e) = writer.write_hwm(new_hwm).await {
                    warn!(error = %e, new_hwm, "supabase sink: write hwm failed; will re-advance next reconcile");
                }
            }
            Ok(_) => {}
            Err(e) => warn!(error = %e, "supabase sink: fill reconcile failed"),
        },
        Err(e) => {
            warn!(error = %e, "supabase sink: read hwm failed; skipping fill catch-up this round")
        }
    }
    if let Err(e) = reconcile_settled(writer, paper_state).await {
        warn!(error = %e, "supabase sink: settled reconcile failed");
    }
}

/// The sink background task: an initial catch-up reconcile, then a `select!` over live
/// events and a periodic reconcile ticker. Returns when the event channel closes.
pub async fn run_sink<W: SinkWriter>(
    writer: W,
    paper_state: Arc<PaperStateDb>,
    mut rx: mpsc::Receiver<SinkEvent>,
    reconcile_interval: Duration,
    dropped: Arc<AtomicU64>,
) {
    info!(
        reconcile_secs = reconcile_interval.as_secs(),
        "supabase sink started"
    );
    // Startup catch-up: heal anything written while the service was down.
    reconcile(&writer, &paper_state).await;

    let mut ticker = tokio::time::interval(reconcile_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await; // consume the immediate first tick (we just reconciled)

    loop {
        tokio::select! {
            maybe = rx.recv() => match maybe {
                Some(SinkEvent::Fill(row)) => match supabase_fill_from(&row) {
                    Some(sup) => {
                        if let Err(e) = writer.upsert_fill(&sup).await {
                            warn!(error = %e, event_seq = row.event_seq, "supabase sink: live fill upsert failed; will heal on reconcile");
                        }
                    }
                    None => warn!(key = %row.idempotency_key, "supabase sink: skipping non-winner-follow fill (no leader)"),
                },
                Some(SinkEvent::Resolution) => {
                    if let Err(e) = reconcile_settled(&writer, &paper_state).await {
                        warn!(error = %e, "supabase sink: live settled re-upsert failed; will heal on reconcile");
                    }
                }
                None => {
                    info!("supabase sink: event channel closed; shutting down");
                    break;
                }
            },
            _ = ticker.tick() => {
                let n = dropped.swap(0, Ordering::Relaxed);
                if n > 0 {
                    warn!(dropped = n, "supabase sink: events dropped since last reconcile (self-healing)");
                }
                reconcile(&writer, &paper_state).await;
            }
        }
    }
}
