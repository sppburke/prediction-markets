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
}

pub type SharedHealth = Arc<Mutex<HealthState>>;

pub fn new_shared_health(polygon_enabled: bool) -> SharedHealth {
    Arc::new(Mutex::new(HealthState {
        polygon_status: SourceStatus::Healthy,
        polymarket_last_event_at: None,
        polygon_last_event_at: None,
        event_log_writable: true,
        polygon_enabled,
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
