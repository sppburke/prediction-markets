//! Agent-friendly health snapshot (`status.json`).
//!
//! A periodic task atomically overwrites a single small JSON file with pe-service's current
//! state, so an agent (or operator) answers "how is it doing?" by reading **one tiny file** —
//! no grepping the rolling JSONL. Money fields are exact decimal strings (never `f64`); the
//! counts are cheap `COUNT(*)`s; the write is atomic (temp + rename) so a reader never sees a
//! half-written file.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pe_paper_state::PaperStateDb;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tracing::warn;

use crate::live_watchlist::LiveWatchlist;
use crate::runtime_config::AppliedWatchlistCapacity;

/// One snapshot of pe-service health, serialized to `status.json`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    /// RFC-3339 UTC instant this snapshot was written.
    pub updated_at: String,
    pub uptime_secs: u64,
    /// Execution mode string (`shadow` | `paper` | `live_tiny` | `promoted`).
    pub mode: String,
    /// Whether Supabase is the authoritative system of record (issue #397).
    pub authoritative: bool,
    /// Exact bankroll as a decimal string; `None` if uninitialised.
    pub bankroll: Option<String>,
    pub open_positions: usize,
    pub fills_total: usize,
    pub settled_total: usize,
    /// Highest event-log seq mirrored into SQLite.
    pub last_event_seq: u64,
    /// Live watchlist size (wallets currently copied).
    pub watchlist_size: usize,
    /// Last successfully applied Supabase-configured cap. It can differ from `watchlist_size`
    /// when the ranking bench cannot fill every requested slot or maintenance is between fills.
    pub watchlist_target_size: usize,
    /// Cumulative authoritative RPC calls (`commit_fill` + `apply_resolution`) since boot;
    /// `0` when not in authoritative mode. Diff two snapshots for the Supabase write rate.
    pub supabase_rpc_calls: u64,
}

/// Build a snapshot from the (cheap) live counters. Deterministic given its scalar inputs —
/// the time/uptime/size values are injected by the caller so this is unit-testable.
#[allow(clippy::too_many_arguments)]
pub fn build_snapshot(
    paper_state: &PaperStateDb,
    mode: &str,
    authoritative: bool,
    uptime_secs: u64,
    now_unix: i64,
    watchlist_size: usize,
    watchlist_target_size: usize,
    supabase_rpc_calls: u64,
) -> StatusSnapshot {
    StatusSnapshot {
        updated_at: OffsetDateTime::from_unix_timestamp(now_unix)
            .ok()
            .and_then(|t| t.format(&Rfc3339).ok())
            .unwrap_or_default(),
        uptime_secs,
        mode: mode.to_string(),
        authoritative,
        // Best-effort reads: a transient SQLite error surfaces as a missing/zero field rather
        // than failing the whole snapshot (the next tick retries).
        bankroll: paper_state.bankroll().ok().flatten().map(|d| d.to_string()),
        open_positions: paper_state.positions_count().unwrap_or(0),
        fills_total: paper_state.fills_count().unwrap_or(0),
        settled_total: paper_state.settled_count().unwrap_or(0),
        last_event_seq: paper_state
            .last_applied_event_seq()
            .map(|s| s.0)
            .unwrap_or(0),
        watchlist_size,
        watchlist_target_size,
        supabase_rpc_calls,
    }
}

/// Atomically write `snapshot` to `path` (write a sibling temp file, then rename over `path`),
/// so a concurrent reader always sees a complete JSON document.
pub fn write_snapshot(path: &Path, snapshot: &StatusSnapshot) -> std::io::Result<()> {
    let json = serde_json::to_vec_pretty(snapshot).map_err(std::io::Error::other)?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, &json)?;
    std::fs::rename(&tmp, path)
}

/// Periodic status-writer task: every `interval`, build + atomically write the snapshot.
/// Best-effort — a write error is logged (to `errors.jsonl`), never fatal.
#[allow(clippy::too_many_arguments)]
pub async fn run_status_writer(
    path: PathBuf,
    interval: Duration,
    paper_state: Arc<PaperStateDb>,
    watchlist: LiveWatchlist,
    applied_capacity: AppliedWatchlistCapacity,
    mode: String,
    authoritative: bool,
    supabase_rpc_calls: Option<Arc<AtomicU64>>,
) {
    let started_at = Instant::now();
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let calls = supabase_rpc_calls
            .as_ref()
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0);
        let snap = build_snapshot(
            &paper_state,
            &mode,
            authoritative,
            started_at.elapsed().as_secs(),
            OffsetDateTime::now_utc().unix_timestamp(),
            watchlist.snapshot().entries.len(),
            applied_capacity.load().target,
            calls,
        );
        if let Err(e) = write_snapshot(&path, &snap) {
            warn!(error = %e, path = %path.display(), "status writer: write failed");
        }
    }
}
