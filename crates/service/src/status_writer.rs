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
use std::time::Duration;

use tokio::time::Instant;

use crate::health::{HealthState, SharedHealth};
use crate::live_watchlist::LiveWatchlist;
use crate::runtime_config::{
    AppliedWatchlistCapacity, LiveRuntimeConfig, RuntimeConfigStatus, RuntimeConfigStatusSnapshot,
};
use crate::supabase_refresh::{WatchlistProjectionStatus, WatchlistProjectionStatusSnapshot};
use crate::supervisor::{TaskStateSnapshot, TaskStatus};
use pe_core_types::WalletAddress;
use pe_paper_state::PaperStateDb;
use serde::Serialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

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
    /// Project the shared health state (pure; unit-tested for the aggregate
    /// derivations). `now` is the wall clock for poll ages; `now_mono` is the
    /// monotonic clock reader timestamps are stamped on.
    pub fn from_health(h: &HealthState, now: OffsetDateTime, now_mono: Instant) -> Self {
        let age = |t: Option<OffsetDateTime>| t.map(|t| (now - t).whole_seconds());
        let age_mono = |t: Option<Instant>| {
            t.map(|t| {
                i64::try_from(now_mono.saturating_duration_since(t).as_secs()).unwrap_or(i64::MAX)
            })
        };
        let readers = h.ws_readers.iter();
        Self {
            activity_ws_enabled: h.activity_ws_enabled,
            ws_connected: readers.clone().any(|r| r.connected),
            ws_last_frame_age_secs: age_mono(
                readers.clone().filter_map(|r| r.last_wire_frame_at).max(),
            ),
            ws_last_valid_frame_age_secs: age_mono(
                readers
                    .clone()
                    .filter_map(|r| r.last_normalized_activity_at)
                    .max(),
            ),
            ws_consecutive_reconnects: readers
                .clone()
                .map(|r| r.consecutive_reconnects)
                .max()
                .unwrap_or(0),
            ws_sink_poisoned: h.ws_sink_poisoned,
            ws_live_reader_count: h.ws_live_reader_count(now_mono),
            ws_readers: readers
                .clone()
                .enumerate()
                .map(|(slot, r)| ReaderStatus {
                    slot,
                    connected: r.connected,
                    fan_in_blocked: r.fan_in_blocked,
                    last_wire_frame_age_secs: age_mono(r.last_wire_frame_at),
                    last_normalized_activity_age_secs: age_mono(r.last_normalized_activity_at),
                    normalized_activity_rows_total: r.normalized_activity_rows_total,
                    consecutive_reconnects: r.consecutive_reconnects,
                })
                .collect(),
            poll_last_round_age_secs: age(h.poll_last_round_at),
            poll_error_streak: h.poll_error_streak,
            copy_admission_blocked: h.copy_admission_blocked(now, now_mono),
        }
    }
}

/// One snapshot of pe-service health, serialized to `status.json`.
#[derive(Debug, Clone, Serialize)]
pub struct StatusSnapshot {
    /// Checked-out Git object embedded by the clean-build identity boundary (#544).
    pub revision: String,
    /// Canonical identity of the complete runtime configuration actually applied.
    pub applied_config_hash: String,
    /// Sticky state of every named production owner.
    pub tasks: Vec<TaskStateSnapshot>,
    /// Latest typed status sampling error; financial fields retain their last-good values.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_error: Option<StatusWriterIssue>,
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
    /// Age of the oldest wallet's latest anchor, or `None` before any anchor.
    pub oldest_anchor_age_secs: Option<u64>,
    /// Highest event-log seq mirrored into SQLite.
    pub last_event_seq: u64,
    /// Live watchlist size (wallets currently copied).
    pub watchlist_size: usize,
    /// Last successfully applied Supabase-configured cap. It can differ from `watchlist_size`
    /// when the ranking bench cannot fill every requested slot or maintenance is between fills.
    pub watchlist_target_size: usize,
    /// One canonical hash of the complete configuration actually applied, plus any separately
    /// typed rejected raw proposal (#544).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_config: Option<RuntimeConfigStatusSnapshot>,
    /// Pending/applied serialized watchlist projection and last typed analytics error (#544).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watchlist_projection: Option<WatchlistProjectionStatusSnapshot>,
    /// Cumulative authoritative RPC calls (`commit_fill` + `apply_resolution`) since boot;
    /// `0` when not in authoritative mode. Diff two snapshots for the Supabase write rate.
    pub supabase_rpc_calls: u64,
    /// Per-account live execution block (#508) — additive shape; `None` until the live
    /// accounts poll is configured. Present-but-empty when configured with no accounts.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub live: Option<LiveStatusBlock>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusWriterIssue {
    pub kind: &'static str,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StatusWriterError {
    #[error("write status snapshot {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Clone)]
struct FinancialValues {
    bankroll: Option<String>,
    open_positions: usize,
    fills_total: usize,
    settled_total: usize,
    oldest_anchor_age_secs: Option<u64>,
    last_event_seq: u64,
}

fn read_financial_values(
    paper_state: &PaperStateDb,
    live_wallets: &[WalletAddress],
    now_unix: i64,
) -> Result<FinancialValues, pe_paper_state::PaperStateError> {
    Ok(FinancialValues {
        bankroll: paper_state.bankroll()?.map(|value| value.to_string()),
        open_positions: paper_state.positions_count()?,
        fills_total: paper_state.fills_count()?,
        settled_total: paper_state.settled_count()?,
        oldest_anchor_age_secs: paper_state
            .oldest_anchor_age(live_wallets, now_unix)?
            .and_then(|age| u64::try_from(age).ok()),
        last_event_seq: paper_state.last_applied_event_seq()?.0,
    })
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
    live_wallets: &[WalletAddress],
    watchlist_target_size: usize,
    supabase_rpc_calls: u64,
    live_accounts: Option<&crate::live_accounts::LiveAccountsSnapshot>,
) -> StatusSnapshot {
    StatusSnapshot {
        revision: crate::build_info::embedded().source_revision.to_owned(),
        applied_config_hash: String::new(),
        tasks: Vec::new(),
        status_error: None,
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
        oldest_anchor_age_secs: paper_state
            .oldest_anchor_age(live_wallets, now_unix)
            .ok()
            .flatten()
            .and_then(|age| u64::try_from(age).ok()),
        last_event_seq: paper_state
            .last_applied_event_seq()
            .map(|s| s.0)
            .unwrap_or(0),
        watchlist_size,
        watchlist_target_size,
        runtime_config: None,
        watchlist_projection: None,
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

/// Periodic status-writer task. A write error is a typed owner failure; financial read errors
/// remain visible while retaining the most recent complete financial sample (#544).
#[allow(clippy::too_many_arguments)]
pub async fn run_status_writer(
    path: PathBuf,
    interval: Option<Duration>,
    paper_state: Arc<PaperStateDb>,
    watchlist: LiveWatchlist,
    applied_capacity: AppliedWatchlistCapacity,
    runtime_config: LiveRuntimeConfig,
    runtime_config_status: RuntimeConfigStatus,
    projection_status: WatchlistProjectionStatus,
    authoritative: bool,
    supabase_rpc_calls: Option<Arc<AtomicU64>>,
    live_accounts: Option<crate::live_accounts::LiveAccounts>,
    health: Option<SharedHealth>,
    task_status: TaskStatus,
    shutdown: impl std::future::Future<Output = ()>,
) -> Result<(), StatusWriterError> {
    let started_at = Instant::now();
    let mut ticker = interval.map(tokio::time::interval);
    let mut last_good_financials = None;
    tokio::pin!(shutdown);
    loop {
        let final_write = tokio::select! {
            () = &mut shutdown => true,
            () = async {
                match &mut ticker {
                    Some(ticker) => { ticker.tick().await; }
                    None => std::future::pending::<()>().await,
                }
            } => false,
        };
        let calls = supabase_rpc_calls
            .as_ref()
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0);
        let source_health = health.as_ref().and_then(|h| {
            let h = h.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            h.activity_ws_enabled.then(|| {
                SourceHealthStatus::from_health(&h, OffsetDateTime::now_utc(), Instant::now())
            })
        });
        let applied_config = runtime_config.snapshot();
        let now_unix = OffsetDateTime::now_utc().unix_timestamp();
        let watchlist_snapshot = watchlist.snapshot();
        let live_wallets = watchlist_snapshot
            .entries
            .iter()
            .map(|entry| entry.wallet)
            .collect::<Vec<_>>();
        let status_error = match read_financial_values(&paper_state, &live_wallets, now_unix) {
            Ok(values) => {
                last_good_financials = Some(values);
                None
            }
            Err(error) => Some(StatusWriterIssue {
                kind: "paper_state_read",
                message: error.to_string(),
            }),
        };
        let mut snap = build_snapshot(
            &paper_state,
            &applied_config.mode,
            authoritative,
            started_at.elapsed().as_secs(),
            now_unix,
            watchlist_snapshot.entries.len(),
            &live_wallets,
            applied_capacity.load().target,
            calls,
            live_accounts.as_ref().map(|l| l.snapshot()).as_deref(),
        );
        snap.source_health = source_health;
        let runtime_status = runtime_config_status.snapshot();
        snap.applied_config_hash = runtime_status.applied_hash.clone();
        snap.runtime_config = Some(runtime_status.as_ref().clone());
        snap.watchlist_projection = Some(projection_status.snapshot().as_ref().clone());
        snap.tasks = task_status.snapshot();
        snap.status_error = status_error;
        if let Some(values) = &last_good_financials {
            snap.bankroll.clone_from(&values.bankroll);
            snap.open_positions = values.open_positions;
            snap.fills_total = values.fills_total;
            snap.settled_total = values.settled_total;
            snap.oldest_anchor_age_secs = values.oldest_anchor_age_secs;
            snap.last_event_seq = values.last_event_seq;
        }
        write_snapshot(&path, &snap).map_err(|source| StatusWriterError::Write {
            path: path.clone(),
            source,
        })?;
        if final_write {
            return Ok(());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::ServiceConfig;
    use crate::health::{ReaderHealth, new_shared_health_with_ws};
    use crate::runtime_config::RuntimeConfig;
    use crate::supervisor::TaskName;
    use pe_core_types::SourceTimestamp;
    use pe_trader_index::Watchlist;

    fn t0() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()
    }

    #[test]
    fn aggregate_fields_derive_from_the_reader_records() {
        let health = new_shared_health_with_ws(false, true, 90);
        let now = t0() + time::Duration::seconds(100);
        // Monotonic reference comfortably after process start.
        let m0 = Instant::now() + Duration::from_secs(1_000);
        let now_mono = m0 + Duration::from_secs(100);
        {
            let mut h = health.lock().unwrap();
            h.ws_readers[0] = ReaderHealth {
                connected: true,
                fan_in_blocked: false,
                last_wire_frame_at: Some(m0 + Duration::from_secs(95)),
                last_normalized_activity_at: Some(m0 + Duration::from_secs(90)),
                normalized_activity_rows_total: 7,
                consecutive_reconnects: 0,
            };
            h.ws_readers[2] = ReaderHealth {
                connected: false,
                fan_in_blocked: false,
                last_wire_frame_at: Some(m0 + Duration::from_secs(60)),
                last_normalized_activity_at: Some(m0 + Duration::from_secs(99)),
                normalized_activity_rows_total: 3,
                consecutive_reconnects: 4,
            };
        }
        let h = health.lock().unwrap();
        let s = SourceHealthStatus::from_health(&h, now, now_mono);
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
        let snap = build_snapshot(
            &paper_state,
            "paper",
            false,
            1,
            1_000_000,
            0,
            &[],
            0,
            0,
            None,
        );
        assert!(snap.source_health.is_none());
        let json = serde_json::to_value(&snap).unwrap();
        assert!(
            json.get("source_health").is_none(),
            "byte-identical disabled shape"
        );
    }

    #[tokio::test]
    async fn final_snapshot_carries_build_config_task_and_projection_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("status.json");
        let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let config = RuntimeConfig::from_service_config(&ServiceConfig::default());
        let runtime = LiveRuntimeConfig::new(config.clone());
        let runtime_status = RuntimeConfigStatus::new(&config);
        let task_status = TaskStatus::new();
        task_status.register(TaskName::Orchestrator);
        let watchlist = LiveWatchlist::new(Watchlist {
            entries: Vec::new(),
            snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            active_count: 0,
            incubator_count: 0,
        });

        run_status_writer(
            path.clone(),
            None,
            paper_state,
            watchlist,
            AppliedWatchlistCapacity::new(config.active_watchlist_size),
            runtime,
            runtime_status.clone(),
            WatchlistProjectionStatus::default(),
            false,
            None,
            None,
            None,
            task_status,
            async {},
        )
        .await
        .unwrap();

        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(
            value["revision"],
            crate::build_info::embedded().source_revision
        );
        assert_eq!(
            value["applied_config_hash"],
            runtime_status.snapshot().applied_hash
        );
        assert_eq!(value["tasks"][0]["name"], "orchestrator");
        assert!(value["watchlist_projection"].is_object());
    }
}
