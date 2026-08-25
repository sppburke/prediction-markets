//! Service health state, shared between the orchestrator and HTTP handlers.

use std::sync::{Arc, Mutex};

use axum::{Json, extract::State, http::StatusCode};
use pe_source_core::SourceStatus;
use serde::Serialize;
use time::OffsetDateTime;

/// Maximum seconds between source events before the source is considered stale.
///
/// See `docs/_GLOSSARY.md`: `source_freshness_window_seconds`.
const FRESHNESS_WINDOW_SECS: i64 = 60;

/// Mutable health state updated by the orchestrator.
#[derive(Debug)]
pub struct HealthState {
    pub polygon_status: SourceStatus,
    pub polymarket_last_event_at: Option<OffsetDateTime>,
    pub polygon_last_event_at: Option<OffsetDateTime>,
    pub event_log_writable: bool,
    /// Whether the live Polygon WS source is configured. When `false` (empty
    /// `polygon_ws_url`, i.e. "etherscan-only mode"), the absence of polygon
    /// events is intentional and must not flag the source as stale/dead — that
    /// would make `/health/ready` permanently red and hide a genuine feed break.
    pub polygon_enabled: bool,

    // ── #530: activity-websocket vs REST-poll health split ───────────────────
    // `polymarket_last_event_at` (above) records ADMITTED TRADES only; the
    // fields below separate transport health so an always-on poller can never
    // mask a silently dead websocket (the soak-proven zombie mode) and vice
    // versa. Mirrors the `polygon_enabled` skip-when-disabled precedent.
    /// Whether the activity websocket is configured (`polymarket_activity_ws_enabled`).
    pub activity_ws_enabled: bool,
    /// Socket currently established and subscribed.
    pub ws_connected: bool,
    /// Last frame of ANY kind on the socket.
    pub ws_last_frame_at: Option<OffsetDateTime>,
    /// Last frame that parsed (staleness/health keys on THIS one).
    pub ws_last_valid_frame_at: Option<OffsetDateTime>,
    /// Completed reconnects since the last valid frame.
    pub ws_consecutive_reconnects: u32,
    /// Source event log append/sync failed; websocket delivery is blocked until
    /// the log reopens and revalidates (REST fallback carries the trades).
    pub ws_sink_poisoned: bool,
    /// Last SUCCESSFUL poll round (even an empty one) — round health, not trade health.
    pub poll_last_round_at: Option<OffsetDateTime>,
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
        match self.poll_last_round_at {
            Some(t) => (now - t).whole_seconds() > self.poll_round_stale_secs,
            None => false, // pre-first-round grace: the streak covers real failures
        }
    }

    /// Websocket source stale-or-worse (#530): no valid frame within the stale
    /// window, or the sink is poisoned. Only meaningful when enabled.
    pub fn ws_stale_or_worse(&self, now: OffsetDateTime) -> bool {
        if self.ws_sink_poisoned {
            return true;
        }
        match self.ws_last_valid_frame_at {
            Some(t) => {
                (now - t).whole_seconds() >= pe_source_polymarket_public::ACTIVITY_WS_STALE_SECS
            }
            None => !self.ws_connected,
        }
    }

    /// Dual-unhealthy admission block (#530): with the websocket enabled, BOTH
    /// transports unhealthy means no new copy work may stage — queued inputs
    /// drain as refusals without state writes, and redelivery (held cursor /
    /// firehose backstop) admits each trade exactly once on recovery.
    pub fn copy_admission_blocked(&self, now: OffsetDateTime) -> bool {
        self.activity_ws_enabled && self.ws_stale_or_worse(now) && self.poll_unhealthy(now)
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
        polygon_enabled,
        activity_ws_enabled,
        ws_connected: false,
        ws_last_frame_at: None,
        ws_last_valid_frame_at: None,
        ws_consecutive_reconnects: 0,
        ws_sink_poisoned: false,
        poll_last_round_at: None,
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

pub async fn ready(State(health): State<SharedHealth>) -> (StatusCode, Json<ReadyResponse>) {
    let mut issues: Vec<&'static str> = Vec::new();

    let now = OffsetDateTime::now_utc();

    // Acquire the lock, check state, then release immediately — no await across lock.
    let (polygon_status, polygon_last, polymarket_last, log_ok, polygon_enabled) = {
        let h = health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            h.polygon_status,
            h.polygon_last_event_at,
            h.polymarket_last_event_at,
            h.event_log_writable,
            h.polygon_enabled,
        )
    };

    // Skip polygon liveness checks when the WS source is intentionally disabled
    // (empty polygon_ws_url). Otherwise readiness would be permanently red and a
    // real feed break would be indistinguishable from a deliberate shutoff.
    if polygon_enabled {
        if polygon_status == SourceStatus::Dead {
            issues.push("polygon_source_dead");
        }

        if polygon_last.is_none_or(|t| (now - t).whole_seconds() > FRESHNESS_WINDOW_SECS) {
            issues.push("polygon_source_stale");
        }
    }

    if polymarket_last.is_none_or(|t| (now - t).whole_seconds() > FRESHNESS_WINDOW_SECS) {
        issues.push("polymarket_source_stale");
    }

    if !log_ok {
        issues.push("event_log_not_writable");
    }

    // #530: websocket-source issues (skip-when-disabled, like polygon above) and
    // the dual-unhealthy admission block.
    {
        let h = health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if h.activity_ws_enabled {
            if h.ws_sink_poisoned {
                issues.push("activity_ws_sink_poisoned");
            }
            let valid_age = h.ws_last_valid_frame_at.map(|t| (now - t).whole_seconds());
            if valid_age.is_none_or(|a| a >= pe_source_polymarket_public::ACTIVITY_WS_DEAD_SECS)
                && !h.ws_connected
                || valid_age
                    .is_some_and(|a| a >= pe_source_polymarket_public::ACTIVITY_WS_DEAD_SECS)
            {
                issues.push("activity_ws_dead");
            } else if h.ws_stale_or_worse(now) {
                issues.push("activity_ws_stale");
            }
            if h.copy_admission_blocked(now) {
                issues.push("copy_admission_blocked");
            }
        }
    }

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
}
