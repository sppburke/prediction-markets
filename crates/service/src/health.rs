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
}

pub type SharedHealth = Arc<Mutex<HealthState>>;

pub fn new_shared_health() -> SharedHealth {
    Arc::new(Mutex::new(HealthState {
        polygon_status: SourceStatus::Healthy,
        polymarket_last_event_at: None,
        polygon_last_event_at: None,
        event_log_writable: true,
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
    let (polygon_status, polygon_last, polymarket_last, log_ok) = {
        let h = health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            h.polygon_status,
            h.polygon_last_event_at,
            h.polymarket_last_event_at,
            h.event_log_writable,
        )
    };

    if polygon_status == SourceStatus::Dead {
        issues.push("polygon_source_dead");
    }

    if polygon_last.is_none_or(|t| (now - t).whole_seconds() > FRESHNESS_WINDOW_SECS) {
        issues.push("polygon_source_stale");
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
