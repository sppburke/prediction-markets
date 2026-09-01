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

use crate::health::{HealthState, SharedHealth};
use crate::live_watchlist::LiveWatchlist;
use crate::runtime_config::AppliedWatchlistCapacity;

/// #530/#546: split trade-source health for the observability surface. Ages
/// are in seconds; `None` = never. Emitted ONLY when the websocket is enabled —
/// with the flag off, `status.json` stays byte-identical to pre-#530 (review F8;
/// the disabled posture IS the rollback contract). The `ws_*` aggregates keep
/// their pre-#546 names and are derived from the per-reader records: any
/// connected reader, the newest wire/normalized timestamps, the largest
/// reconnect count. `ws_last_valid_frame_age_secs` now means the newest
/// normalizer-accepted activity row, not an empty successful parse.
#[derive(Debug, Clone, Serialize)]
pub struct SourceHealthStatus {
    pub activity_ws_enabled: bool,
    pub ws_connected: bool,
    pub ws_last_frame_age_secs: Option<i64>,
    pub ws_last_valid_frame_age_secs: Option<i64>,
    pub ws_consecutive_reconnects: u32,
    pub ws_sink_poisoned: bool,
    /// Readers that are connected AND normalized a row within the timeout (#546).
    pub ws_live_reader_count: usize,
    /// One record per reader slot, fixed length `activity_ws_reader_count`.
    pub ws_readers: Vec<ReaderStatus>,
    pub poll_last_round_age_secs: Option<i64>,
    pub poll_error_streak: u32,
    pub copy_admission_blocked: bool,
}

/// One reader slot in `status.json` (#546).
#[derive(Debug, Clone, Serialize)]
pub struct ReaderStatus {
    pub slot: usize,
    pub connected: bool,
    pub fan_in_blocked: bool,
    pub last_wire_frame_age_secs: Option<i64>,
    pub last_normalized_activity_age_secs: Option<i64>,
    pub normalized_activity_rows_total: u64,
    pub consecutive_reconnects: u32,
}

impl SourceHealthStatus {
    /// Project the shared health state at `now` (pure; unit-tested for the
    /// aggregate derivations).
    pub fn from_health(h: &HealthState, now: OffsetDateTime) -> Self {
        let age = |t: Option<OffsetDateTime>| t.map(|t| (now - t).whole_seconds());
        let readers = h.ws_readers.iter();
        Self {
            activity_ws_enabled: h.activity_ws_enabled,
            ws_connected: readers.clone().any(|r| r.connected),
            ws_last_frame_age_secs: age(readers.clone().filter_map(|r| r.last_wire_frame_at).max()),
            ws_last_valid_frame_age_secs: age(readers
                .clone()
                .filter_map(|r| r.last_normalized_activity_at)
                .max()),
            ws_consecutive_reconnects: readers
                .clone()
                .map(|r| r.consecutive_reconnects)
                .max()
                .unwrap_or(0),
            ws_sink_poisoned: h.ws_sink_poisoned,
            ws_live_reader_count: h.ws_live_reader_count(now),
            ws_readers: readers
                .clone()
                .enumerate()
                .map(|(slot, r)| ReaderStatus {
                    slot,
                    connected: r.connected,
                    fan_in_blocked: r.fan_in_blocked,
                    last_wire_frame_age_secs: age(r.last_wire_frame_at),
                    last_normalized_activity_age_secs: age(r.last_normalized_activity_at),
                    normalized_activity_rows_total: r.normalized_activity_rows_total,
                    consecutive_reconnects: r.consecutive_reconnects,
                })
                .collect(),
            poll_last_round_age_secs: age(h.poll_last_round_at),
            poll_error_streak: h.poll_error_streak,
            copy_admission_blocked: h.copy_admission_blocked(now),
        }
    }
}

/// One snapshot of pe-service health, serialized to `status.json`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    /// RFC-3339 UTC instant this snapshot was written.
    pub updated_at: String,
    /// #530: split websocket / REST-poll source health (enabled mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_health: Option<SourceHealthStatus>,
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
    /// Per-account live execution block (#508) — additive shape; `None` until the live
    /// accounts poll is configured. Present-but-empty when configured with no accounts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live: Option<LiveStatusBlock>,
}

/// Additive live-execution status (#508): dispatch-aggregate depth + one row per account.
#[derive(Debug, Clone, Serialize)]
pub struct LiveStatusBlock {
    /// Staged aggregates awaiting their paper outcome.
    pub pending_dispatch_seeds: usize,
    /// Ready, not-yet-finalized aggregates awaiting the live fan-out.
    pub ready_dispatch_seeds: usize,
    /// Unix time of the last SUCCESSFUL accounts poll; `null` before one succeeds (#514).
    /// A strictly advancing value across two snapshots proves the poll is decoding.
    pub fetched_at_unix: Option<i64>,
    /// Whether the accounts snapshot is stale (never-successful, too old, or
    /// future-dated); while `true` the service stages no new live work (#514).
    pub stale: bool,
    pub accounts: Vec<LiveAccountStatus>,
}

/// One account's live posture in `status.json` (#508).
#[derive(Debug, Clone, Serialize)]
pub struct LiveAccountStatus {
    pub account_id: String,
    pub is_primary: bool,
    pub enabled: bool,
    pub requested_live_mode: String,
    pub effective_live_mode: String,
    pub armed: bool,
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
    live_accounts: Option<&crate::live_accounts::LiveAccountsSnapshot>,
) -> StatusSnapshot {
    StatusSnapshot {
        source_health: None,
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
        live: live_accounts.map(|snapshot| LiveStatusBlock {
            pending_dispatch_seeds: paper_state
                .pending_dispatch_seeds()
                .map(|v| v.len())
                .unwrap_or(0),
            ready_dispatch_seeds: paper_state
                .unfinalized_ready_dispatch_seeds()
                .map(|v| v.len())
                .unwrap_or(0),
            fetched_at_unix: snapshot.fetched_at_unix,
            stale: !snapshot.is_fresh(now_unix),
            accounts: snapshot
                .accounts
                .iter()
                .map(|a| LiveAccountStatus {
                    account_id: a.account_id.as_str().to_owned(),
                    is_primary: a.is_primary,
                    enabled: a.enabled,
                    requested_live_mode: a.requested_live_mode.clone(),
                    effective_live_mode: a.effective_live_mode.clone(),
                    armed: a.is_armed(),
                })
                .collect(),
        }),
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
    live_accounts: Option<crate::live_accounts::LiveAccounts>,
    health: Option<SharedHealth>,
) {
    let started_at = Instant::now();
    let mut ticker = tokio::time::interval(interval);
    loop {
        ticker.tick().await;
        let calls = supabase_rpc_calls
            .as_ref()
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0);
        let source_health = health.as_ref().and_then(|h| {
            let h = h.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            h.activity_ws_enabled
                .then(|| SourceHealthStatus::from_health(&h, OffsetDateTime::now_utc()))
        });
        let mut snap = build_snapshot(
            &paper_state,
            &mode,
            authoritative,
            started_at.elapsed().as_secs(),
            OffsetDateTime::now_utc().unix_timestamp(),
            watchlist.snapshot().entries.len(),
            applied_capacity.load().target,
            calls,
            live_accounts.as_ref().map(|l| l.snapshot()).as_deref(),
        );
        snap.source_health = source_health;
        if let Err(e) = write_snapshot(&path, &snap) {
            warn!(error = %e, path = %path.display(), "status writer: write failed");
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::health::{ReaderHealth, new_shared_health_with_ws};

    fn t0() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()
    }

    #[test]
    fn aggregate_fields_derive_from_the_reader_records() {
        let health = new_shared_health_with_ws(false, true, 90);
        let now = t0() + time::Duration::seconds(100);
        {
            let mut h = health.lock().unwrap();
            h.ws_readers[0] = ReaderHealth {
                connected: true,
                fan_in_blocked: false,
                last_wire_frame_at: Some(t0() + time::Duration::seconds(95)),
                last_normalized_activity_at: Some(t0() + time::Duration::seconds(90)),
                normalized_activity_rows_total: 7,
                consecutive_reconnects: 0,
            };
            h.ws_readers[2] = ReaderHealth {
                connected: false,
                fan_in_blocked: false,
                last_wire_frame_at: Some(t0() + time::Duration::seconds(60)),
                last_normalized_activity_at: Some(t0() + time::Duration::seconds(99)),
                normalized_activity_rows_total: 3,
                consecutive_reconnects: 4,
            };
        }
        let h = health.lock().unwrap();
        let s = SourceHealthStatus::from_health(&h, now);
        assert!(s.ws_connected, "any connected reader");
        assert_eq!(s.ws_last_frame_age_secs, Some(5), "newest wire frame");
        assert_eq!(
            s.ws_last_valid_frame_age_secs,
            Some(1),
            "newest normalized row, even from a reader that is now disconnected"
        );
        assert_eq!(s.ws_consecutive_reconnects, 4, "largest reconnect count");
        assert_eq!(
            s.ws_live_reader_count, 1,
            "only reader 0 is connected AND fresh"
        );
        assert_eq!(s.ws_readers.len(), 3);
        assert_eq!(
            s.ws_readers.iter().map(|r| r.slot).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(s.ws_readers[0].normalized_activity_rows_total, 7);
        assert_eq!(s.ws_readers[2].last_normalized_activity_age_secs, Some(1));
        assert_eq!(s.ws_readers[1].last_wire_frame_age_secs, None);
        assert!(!s.copy_admission_blocked);

        let json = serde_json::to_value(&s).unwrap();
        for key in [
            "ws_connected",
            "ws_last_frame_age_secs",
            "ws_last_valid_frame_age_secs",
            "ws_consecutive_reconnects",
            "ws_sink_poisoned",
            "ws_live_reader_count",
            "ws_readers",
            "poll_last_round_age_secs",
            "poll_error_streak",
            "copy_admission_blocked",
        ] {
            assert!(json.get(key).is_some(), "status key {key} present");
        }
        assert_eq!(json["ws_readers"].as_array().unwrap().len(), 3);
        assert_eq!(json["ws_readers"][1]["slot"], 1);
        assert_eq!(json["ws_readers"][1]["connected"], false);
    }

    #[test]
    fn disabled_mode_omits_source_health_entirely() {
        let dir = tempfile::tempdir().unwrap();
        let paper_state = PaperStateDb::open(&dir.path().join("p.db")).unwrap();
        let snap = build_snapshot(&paper_state, "paper", false, 1, 1_000_000, 0, 0, 0, None);
        assert!(snap.source_health.is_none());
        let json = serde_json::to_value(&snap).unwrap();
        assert!(
            json.get("source_health").is_none(),
            "byte-identical disabled shape"
        );
    }
}
