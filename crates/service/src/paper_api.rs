//! HTTP handlers for paper-trader P&L, positions, fills, and dashboard endpoints.

use std::path::PathBuf;
use std::sync::Arc;

use axum::{Json, extract::Extension, http::StatusCode, response::Html};
use pe_paper_pnl::{PnlLedger, ResolutionStore, render_dashboard_html};
use pe_paper_state::PaperStateDb;
use rust_decimal::Decimal;
use serde::Serialize;

/// Shared state for paper API handlers (threaded in via `Extension`).
#[derive(Clone)]
pub struct PaperApiState {
    pub paper_state: Arc<PaperStateDb>,
    pub resolutions_path: PathBuf,
    pub initial_bankroll: Decimal,
}

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct ErrorBody {
    error: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `GET /paper/pnl` — current portfolio snapshot as JSON.
pub async fn pnl(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<pe_paper_pnl::PortfolioSnapshot>, (StatusCode, Json<ErrorBody>)> {
    api_snapshot(&state).map(Json)
}

/// `GET /paper/positions` — open paper positions as JSON.
pub async fn positions(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<Vec<PositionDto>>, (StatusCode, Json<ErrorBody>)> {
    let rows = state.paper_state.paper_positions().map_err(internal_err)?;
    let dtos = rows
        .into_iter()
        .filter(|p| p.long_contracts > 0 || p.short_contracts > 0)
        .map(|p| PositionDto {
            market_id: p.market_id.to_string(),
            outcome_id: p.outcome_id.0,
            long_contracts: p.long_contracts,
            short_contracts: p.short_contracts,
        })
        .collect();
    Ok(Json(dtos))
}

#[derive(Serialize)]
pub struct PositionDto {
    market_id: String,
    outcome_id: u16,
    long_contracts: u64,
    short_contracts: u64,
}

/// `GET /paper/fills` — fill count as JSON (full fill list kept in SQLite for future use).
pub async fn fills(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let count = state.paper_state.fills_count().map_err(internal_err)?;
    Ok(Json(serde_json::json!({ "fills_count": count })))
}

/// `GET /paper/status` — bankroll + fill count.
pub async fn status(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let bankroll = state
        .paper_state
        .bankroll()
        .map_err(internal_err)?
        .unwrap_or(Decimal::ZERO);
    let fills_count = state.paper_state.fills_count().map_err(internal_err)?;
    Ok(Json(serde_json::json!({
        "bankroll": bankroll,
        "initial_bankroll": state.initial_bankroll,
        "fills_count": fills_count
    })))
}

/// `GET /dashboard` — embedded HTML dashboard.
pub async fn dashboard(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Html<String>, (StatusCode, Json<ErrorBody>)> {
    let snapshot = api_snapshot(&state)?;
    Ok(Html(render_dashboard_html(&snapshot)))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn api_snapshot(
    state: &PaperApiState,
) -> Result<pe_paper_pnl::PortfolioSnapshot, (StatusCode, Json<ErrorBody>)> {
    let store = ResolutionStore::load(&state.resolutions_path).map_err(internal_err)?;
    PnlLedger::snapshot(&state.paper_state, &store, state.initial_bankroll).map_err(internal_err)
}

fn internal_err<E: std::fmt::Display>(e: E) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: e.to_string(),
        }),
    )
}
