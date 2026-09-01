//! Service health state, shared between the orchestrator and HTTP handlers.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{Json, extract::State, http::StatusCode};
use pe_source_core::SourceStatus;
use pe_source_polymarket_public::{
    ACTIVITY_WS_NORMALIZED_ACTIVITY_TIMEOUT_SECS, ACTIVITY_WS_READER_COUNT,
};
use serde::Serialize;
use time::OffsetDateTime;
use tokio::time::Instant;

/// Maximum seconds between source events before the source is considered stale.
///
/// See `docs/_GLOSSARY.md`: `source_freshness_window_seconds`.
const FRESHNESS_WINDOW_SECS: i64 = 60;

/// One activity-websocket reader slot (#546). Written by that reader's task only;
/// read by readiness and `status.json`. Timestamps are monotonic instants — the
/// same clock the reader's own drop deadline runs on — so liveness cannot flap
/// on a wall-clock step; `status.json` reports them as elapsed ages.
#[derive(Debug, Clone, Default)]
pub struct ReaderHealth {
    /// Socket currently established and subscribed.
    pub connected: bool,
    /// Blocked on the bounded fan-in channel with one retained watched row
    /// (downstream backpressure, not upstream silence).
    pub fan_in_blocked: bool,
    /// Last frame of ANY kind on this socket, control frames included (wire
    /// health only).
    pub last_wire_frame_at: Option<Instant>,
    /// Last payload accepted by the production normalizer on the CURRENT
    /// connection; reset on every (re)connect, so a new socket is not live until
    /// it proves delivery. Liveness keys on THIS one.
    pub last_normalized_activity_at: Option<Instant>,
    /// Normalizer-accepted payloads across all of this slot's connections.
    pub normalized_activity_rows_total: u64,
    /// Completed reconnects since the last normalized row.
    pub consecutive_reconnects: u32,
}

impl ReaderHealth {
    /// Live = connected AND a normalized activity row younger than the
    /// normalized-activity timeout. Acknowledgements, keepalives, and rejected
    /// payloads never make a reader live. A reader blocked on a full fan-in
    /// keeps its socket but derives non-live here once its row ages out.
    pub fn is_live(&self, now: Instant) -> bool {
        let timeout = Duration::from_secs(ACTIVITY_WS_NORMALIZED_ACTIVITY_TIMEOUT_SECS);
        self.connected
            && self
                .last_normalized_activity_at
                .is_some_and(|t| now.saturating_duration_since(t) < timeout)
    }
}

/// Mutable health state updated by the orchestrator.
#[derive(Debug)]
pub struct HealthState {
    pub polygon_status: SourceStatus,
    pub polymarket_last_event_at: Option<OffsetDateTime>,
    pub polygon_last_event_at: Option<OffsetDateTime>,
    pub event_log_writable: bool,
    /// Paper executor crossed an uncertain sync boundary and is permanently poisoned.
    pub paper_durability_uncertain: bool,
    /// Whether the live Polygon WS source is configured. When `false` (empty
    /// `polygon_ws_url`, i.e. "etherscan-only mode"), the absence of polygon
    /// events is intentional and must not flag the source as stale/dead — that
    /// would make `/health/ready` permanently red and hide a genuine feed break.
    pub polygon_enabled: bool,

    // ── #530/#546: activity-websocket vs REST-poll health split ──────────────
    // `polymarket_last_event_at` (above) is the LEGACY liveness mark — refreshed
    // by successful poll fetches and by gate-passing trades (pre-#530 semantics,
    // unchanged). The fields below separate transport health so an always-on
    // poller can never mask silent websocket readers and vice versa. Mirrors the
    // `polygon_enabled` skip-when-disabled precedent.
    /// Whether the activity websocket is configured (`polymarket_activity_ws_enabled`).
    pub activity_ws_enabled: bool,
    /// One record per reader slot (#546); the aggregate fields `status.json`
    /// still publishes are derived from these.
    pub ws_readers: [ReaderHealth; ACTIVITY_WS_READER_COUNT],
    /// Source event log append/sync failed; websocket delivery from EVERY reader
    /// is blocked until the log reopens and revalidates (REST fallback carries
    /// the trades).
    pub ws_sink_poisoned: bool,
    /// Last SUCCESSFUL poll round (even an empty one) — round health, not trade health.
    pub poll_last_round_at: Option<OffsetDateTime>,
    /// When the poller started its first round — bounds the never-succeeded case:
    /// `None` last-round is unhealthy once this is older than the stale bound
    /// (#530 review F2: without it, a poller that never succeeds is healthy forever).
    pub poll_started_at: Option<OffsetDateTime>,
    /// Consecutive failed poll rounds.
    pub poll_error_streak: u32,
    /// Poll round age beyond which the poll source counts unhealthy
    /// (3 × the configured poll interval, resolved at boot).
    pub poll_round_stale_secs: i64,
}

/// Consecutive failed poll rounds at which the REST source counts unhealthy.
/// Canonical home: `docs/_GLOSSARY.md` (`poll_unhealthy_error_streak`).
pub const POLL_UNHEALTHY_ERROR_STREAK: u32 = 3;

impl HealthState {
    /// REST poll source unhealthy: error streak at threshold, or the last
    /// successful round is older than the stale bound (never-successful counts
    /// as unhealthy once the process has been up longer than the bound —
    /// callers pass `now`; boot seeds `poll_last_round_at = None`).
    pub fn poll_unhealthy(&self, now: OffsetDateTime) -> bool {
        if self.poll_error_streak >= POLL_UNHEALTHY_ERROR_STREAK {
            return true;
        }
        match (self.poll_last_round_at, self.poll_started_at) {
            (Some(t), _) => (now - t).whole_seconds() > self.poll_round_stale_secs,
            // Never succeeded: unhealthy once the poller has been running longer
            // than the stale bound. Before the poller starts (or in poll-less
            // tests) there is nothing to distrust yet.
            (None, Some(started)) => (now - started).whole_seconds() > self.poll_round_stale_secs,
            (None, None) => false,
        }
    }

    /// Readers currently live (#546). Only meaningful when enabled.
    pub fn ws_live_reader_count(&self, now: Instant) -> usize {
        self.ws_readers.iter().filter(|r| r.is_live(now)).count()
    }

    /// Websocket source unavailable (#546): no live reader, or the shared sink
    /// is poisoned. One live reader is degraded but still available.
    pub fn ws_unavailable(&self, now: Instant) -> bool {
        self.ws_sink_poisoned || self.ws_live_reader_count(now) == 0
    }

    /// Dual-unhealthy admission block (#530): with the websocket enabled, BOTH
    /// transports unhealthy means no new copy work may stage — queued inputs
    /// are held (not dropped) until a source recovers, and redelivery (held
    /// cursor / firehose backstop) admits each trade exactly once. Poll health
    /// is wall-clock (`now`); reader liveness is monotonic (`now_mono`).
    pub fn copy_admission_blocked(&self, now: OffsetDateTime, now_mono: Instant) -> bool {
        self.activity_ws_enabled && self.ws_unavailable(now_mono) && self.poll_unhealthy(now)
    }
}

pub type SharedHealth = Arc<Mutex<HealthState>>;

pub fn new_shared_health(polygon_enabled: bool) -> SharedHealth {
    new_shared_health_with_ws(polygon_enabled, false, 90)
}

/// [`new_shared_health`] with the #530 websocket/poll-split posture.
pub fn new_shared_health_with_ws(
    polygon_enabled: bool,
    activity_ws_enabled: bool,
    poll_round_stale_secs: i64,
) -> SharedHealth {
    Arc::new(Mutex::new(HealthState {
        polygon_status: SourceStatus::Healthy,
        polymarket_last_event_at: None,
        polygon_last_event_at: None,
        event_log_writable: true,
        paper_durability_uncertain: false,
        polygon_enabled,
        activity_ws_enabled,
        ws_readers: Default::default(),
        ws_sink_poisoned: false,
        poll_last_round_at: None,
        poll_started_at: None,
        poll_error_streak: 0,
        poll_round_stale_secs,
    }))
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct ReadyResponse {
    ready: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    issues: Vec<&'static str>,
}

pub async fn live() -> &'static str {
    "ok"
}

/// Readiness issues for the current state (pure; the handler wraps it). `now`
/// is the wall clock for source freshness and poll health; `now_mono` is the
/// monotonic clock for reader liveness.
pub fn readiness_issues(
    h: &HealthState,
    now: OffsetDateTime,
    now_mono: Instant,
) -> Vec<&'static str> {
    let mut issues: Vec<&'static str> = Vec::new();

    // Skip polygon liveness checks when the WS source is intentionally disabled
    // (empty polygon_ws_url). Otherwise readiness would be permanently red and a
    // real feed break would be indistinguishable from a deliberate shutoff.
    if h.polygon_enabled {
        if h.polygon_status == SourceStatus::Dead {
            issues.push("polygon_source_dead");
        }

        if h.polygon_last_event_at
            .is_none_or(|t| (now - t).whole_seconds() > FRESHNESS_WINDOW_SECS)
        {
            issues.push("polygon_source_stale");
        }
    }

    if h.polymarket_last_event_at
        .is_none_or(|t| (now - t).whole_seconds() > FRESHNESS_WINDOW_SECS)
    {
        issues.push("polymarket_source_stale");
    }

    if !h.event_log_writable {
        issues.push("event_log_not_writable");
    }
    if h.paper_durability_uncertain {
        issues.push("paper_durability_uncertain");
    }

    // #530/#546: websocket-source issues (skip-when-disabled, like polygon above)
    // and the dual-unhealthy admission block. Polling health never hides a
    // reader condition: redundancy loss is reported even while copying continues.
    if h.activity_ws_enabled {
        if h.ws_sink_poisoned {
            issues.push("activity_ws_sink_poisoned");
        }
        if h.ws_unavailable(now_mono) {
            issues.push("activity_ws_unavailable");
        } else if h.ws_live_reader_count(now_mono) == 1 {
            issues.push("activity_ws_redundancy_degraded");
        }
        if h.copy_admission_blocked(now, now_mono) {
            issues.push("copy_admission_blocked");
        }
    }

    issues
}

pub async fn ready(State(health): State<SharedHealth>) -> (StatusCode, Json<ReadyResponse>) {
    let now = OffsetDateTime::now_utc();
    let now_mono = Instant::now();
    // Acquire the lock, compute, then release immediately — no await across lock.
    let issues = {
        let h = health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        readiness_issues(&h, now, now_mono)
    };

    let ready = issues.is_empty();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(ReadyResponse { ready, issues }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Build health state with a fresh polymarket event so only the polygon
    /// checks decide readiness, then run the `ready` handler.
    async fn ready_issues(polygon_enabled: bool) -> Vec<&'static str> {
        let health = new_shared_health(polygon_enabled);
        {
            let mut h = health.lock().unwrap();
            // Polymarket fresh; polygon left silent (last_event_at == None).
            h.polymarket_last_event_at = Some(OffsetDateTime::now_utc());
        }
        let (_status, Json(resp)) = ready(State(health)).await;
        resp.issues
    }

    #[tokio::test]
    async fn polygon_disabled_does_not_flag_stale() {
        // Etherscan-only mode: silent polygon feed is intentional, not a fault.
        assert!(ready_issues(false).await.is_empty());
    }

    #[tokio::test]
    async fn polygon_enabled_flags_stale_when_silent() {
        // WS configured but no events: a genuine staleness signal must still fire.
        assert_eq!(ready_issues(true).await, vec!["polygon_source_stale"]);
    }

    fn t0() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_000_000).unwrap()
    }

    /// A monotonic reference comfortably after process start, so readers may be
    /// stamped in its past without underflow.
    fn m0() -> Instant {
        Instant::now() + Duration::from_secs(1_000)
    }

    /// Enabled websocket posture with fresh polymarket + poll health, so only the
    /// reader/sink vectors decide the websocket issues.
    fn ws_base() -> HealthState {
        HealthState {
            polygon_status: SourceStatus::Healthy,
            polymarket_last_event_at: Some(t0()),
            polygon_last_event_at: None,
            event_log_writable: true,
            paper_durability_uncertain: false,
            polygon_enabled: false,
            activity_ws_enabled: true,
            ws_readers: Default::default(),
            ws_sink_poisoned: false,
            poll_last_round_at: Some(t0()),
            poll_started_at: Some(t0()),
            poll_error_streak: 0,
            poll_round_stale_secs: 90,
        }
    }

    fn live_reader(at: Instant) -> ReaderHealth {
        ReaderHealth {
            connected: true,
            last_wire_frame_at: Some(at),
            last_normalized_activity_at: Some(at),
            ..ReaderHealth::default()
        }
    }

    #[test]
    fn reader_liveness_keys_on_normalized_rows_within_the_timeout() {
        let m0 = m0();
        let mut r = live_reader(m0);
        assert!(r.is_live(m0 + Duration::from_millis(29_999)));
        assert!(
            !r.is_live(m0 + Duration::from_secs(30)),
            "30s without a normalized row is the non-live boundary"
        );
        // Wire traffic alone (acks, keepalives, control frames) never makes a reader live.
        r.last_normalized_activity_at = None;
        r.last_wire_frame_at = Some(m0 + Duration::from_secs(60));
        assert!(!r.is_live(m0 + Duration::from_secs(60)));
        // A disconnected reader is never live, however fresh its last row was.
        r.last_normalized_activity_at = Some(m0);
        r.connected = false;
        assert!(!r.is_live(m0));
    }

    #[test]
    fn readiness_reports_actual_reader_redundancy() {
        let (now, m0) = (t0(), m0());
        let mut h = ws_base();
        assert_eq!(
            readiness_issues(&h, now, m0),
            vec!["activity_ws_unavailable"]
        );
        h.ws_readers[1] = live_reader(m0);
        assert_eq!(
            readiness_issues(&h, now, m0),
            vec!["activity_ws_redundancy_degraded"]
        );
        h.ws_readers[0] = live_reader(m0);
        assert!(readiness_issues(&h, now, m0).is_empty());
        h.ws_readers[2] = live_reader(m0);
        assert!(readiness_issues(&h, now, m0).is_empty());
        // Sink poison is unavailable at EVERY reader count, and reported as both.
        h.ws_sink_poisoned = true;
        assert_eq!(
            readiness_issues(&h, now, m0),
            vec!["activity_ws_sink_poisoned", "activity_ws_unavailable"]
        );
    }

    #[test]
    fn polling_health_never_hides_reader_conditions() {
        let (now, m0) = (t0(), m0());
        let mut h = ws_base();
        h.ws_readers[0] = live_reader(m0);
        // Healthy polling: degraded redundancy is still reported.
        assert_eq!(
            readiness_issues(&h, now, m0),
            vec!["activity_ws_redundancy_degraded"]
        );
        // Unhealthy polling with one live reader: still only degraded (no block).
        h.poll_error_streak = POLL_UNHEALTHY_ERROR_STREAK;
        assert_eq!(
            readiness_issues(&h, now, m0),
            vec!["activity_ws_redundancy_degraded"]
        );
        // Unhealthy polling with zero live readers: unavailable AND blocked.
        h.ws_readers[0] = ReaderHealth::default();
        assert_eq!(
            readiness_issues(&h, now, m0),
            vec!["activity_ws_unavailable", "copy_admission_blocked"]
        );
    }

    #[test]
    fn disabled_mode_reports_no_websocket_issue() {
        let mut h = ws_base();
        h.activity_ws_enabled = false;
        h.poll_error_streak = POLL_UNHEALTHY_ERROR_STREAK;
        assert!(readiness_issues(&h, t0(), m0()).is_empty());
        assert!(!h.copy_admission_blocked(t0(), m0()));
    }

    #[test]
    fn poll_never_succeeded_becomes_unhealthy_after_stale_bound() {
        // #530 review F2a: without poll_started_at, a poller that never succeeds
        // stays healthy forever and dual-unhealthy can never engage.
        let mut h = ws_base();
        h.poll_last_round_at = None;
        h.poll_started_at = None;
        assert!(
            !h.poll_unhealthy(t0()),
            "pre-start there is nothing to distrust"
        );
        h.poll_started_at = Some(t0());
        assert!(!h.poll_unhealthy(t0() + time::Duration::seconds(90)));
        assert!(
            h.poll_unhealthy(t0() + time::Duration::seconds(91)),
            "never-succeeded past the stale bound is unhealthy"
        );
    }

    #[test]
    fn dual_unhealthy_requires_both_sources_down() {
        let m0 = m0();
        let mut h = ws_base();
        h.poll_last_round_at = None;
        h.poll_error_streak = POLL_UNHEALTHY_ERROR_STREAK;
        // No reader ever normalized a row => websocket unavailable.
        assert!(h.copy_admission_blocked(t0(), m0));
        // A healthy poll round clears the block.
        h.poll_error_streak = 0;
        h.poll_last_round_at = Some(t0());
        assert!(!h.copy_admission_blocked(t0(), m0));
        // One live reader also clears it, whatever polling says.
        h.poll_error_streak = POLL_UNHEALTHY_ERROR_STREAK;
        h.ws_readers[2] = live_reader(m0);
        assert!(!h.copy_admission_blocked(t0(), m0));
        // ...but a poisoned sink fails the whole pool closed again.
        h.ws_sink_poisoned = true;
        assert!(h.copy_admission_blocked(t0(), m0));
    }
}
