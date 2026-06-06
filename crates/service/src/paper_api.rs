//! HTTP handlers for paper-trader P&L, positions, fills, and dashboard endpoints.

use std::path::PathBuf;
use std::sync::Arc;

use axum::{Json, extract::Extension, http::StatusCode, response::Html};
use pe_core_types::Side;
use pe_paper_pnl::{PnlLedger, ResolutionStore, TradeView, render_dashboard_html};
use pe_paper_state::PaperStateDb;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::market_end_cache::MarketEndCache;

/// Shared state for paper API handlers (threaded in via `Extension`).
#[derive(Clone)]
pub struct PaperApiState {
    pub paper_state: Arc<PaperStateDb>,
    pub resolutions_path: PathBuf,
    pub initial_bankroll: Decimal,
    /// Shared with the orchestrator so dashboard expiration lookups reuse the cache.
    pub market_end_cache: MarketEndCache,
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

/// `GET /paper/positions` — genuinely-open paper positions as JSON. Settled
/// markets are excluded (their rows linger until rebuild but are no longer open).
pub async fn positions(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<Vec<PositionDto>>, (StatusCode, Json<ErrorBody>)> {
    let store = ResolutionStore::load(&state.resolutions_path).map_err(internal_err)?;
    let rows = state.paper_state.paper_positions().map_err(internal_err)?;
    let dtos = rows
        .into_iter()
        .filter(|p| p.long_contracts > 0 || p.short_contracts > 0)
        .filter(|p| !store.is_settled(&p.market_id))
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

/// `GET /paper/fills` — full list of entered trades, each enriched with the
/// leader, entry time and market expiration.
pub async fn fills(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let trades = build_trade_views(&state).await?;
    Ok(Json(serde_json::json!({
        "fills_count": trades.len(),
        "trades": trades.iter().map(TradeView::to_json).collect::<Vec<_>>(),
    })))
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

/// `GET /dashboard` — embedded HTML dashboard with per-trade detail.
pub async fn dashboard(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Html<String>, (StatusCode, Json<ErrorBody>)> {
    let snapshot = api_snapshot(&state)?;
    let trades = build_trade_views(&state).await?;
    Ok(Html(render_dashboard_html(&snapshot, &trades)))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn api_snapshot(
    state: &PaperApiState,
) -> Result<pe_paper_pnl::PortfolioSnapshot, (StatusCode, Json<ErrorBody>)> {
    let store = ResolutionStore::load(&state.resolutions_path).map_err(internal_err)?;
    PnlLedger::snapshot(&state.paper_state, &store, state.initial_bankroll).map_err(internal_err)
}

/// Build the enriched per-trade views from recorded fills: each fill's leader,
/// source trade id and entry time are parsed from its idempotency key, and the
/// market's scheduled close is resolved via the shared Gamma end-date cache.
async fn build_trade_views(
    state: &PaperApiState,
) -> Result<Vec<TradeView>, (StatusCode, Json<ErrorBody>)> {
    let fills = state.paper_state.list_fills().map_err(internal_err)?;
    let mut views = Vec::with_capacity(fills.len());
    for fill in fills {
        let parsed = ParsedKey::from_key(&fill.idempotency_key);
        // The cache dedups across markets; repeated markets hit the fast path.
        let resolution = state.market_end_cache.resolution(&fill.market_id).await;
        views.push(TradeView {
            market_id: fill.market_id.to_string(),
            outcome_id: fill.outcome_id.0,
            side: side_str(fill.side).to_string(),
            contracts: fill.contracts,
            fill_price: fill.fill_price.0,
            leader: parsed.leader.unwrap_or_default(),
            source_trade_id: parsed.source_trade_id.unwrap_or_default(),
            operator_id: parsed.operator_id,
            event_seq: fill.event_seq,
            entry_unix: parsed.entry_unix,
            resolution_unix: resolution.resolution_unix,
            resolution_status: resolution.status,
        });
    }
    Ok(views)
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// Fields recovered from a Winner-Follow idempotency key:
/// `wf|{leader}|{source_trade_id}|{market}|{outcome}|{side}|{observed_at}[|{operator}]`.
/// (Format defined in `strategy-winner-follow::evaluate::build_idempotency_key`.)
#[derive(Default)]
struct ParsedKey {
    leader: Option<String>,
    source_trade_id: Option<String>,
    entry_unix: Option<i64>,
    operator_id: Option<String>,
}

impl ParsedKey {
    fn from_key(key: &str) -> Self {
        let parts: Vec<&str> = key.split('|').collect();
        // Index 0 is the "wf" tag; 1=leader, 2=source_trade_id, 6=observed_at bucket,
        // 7=operator (cluster-coordination only). Anything shorter is a legacy/foreign
        // key — leave fields empty rather than guess.
        if parts.len() < 7 || parts[0] != "wf" {
            return Self::default();
        }
        Self {
            leader: Some(parts[1].to_string()),
            source_trade_id: Some(parts[2].to_string()),
            entry_unix: parts[6].parse().ok(),
            operator_id: parts.get(7).map(|s| s.to_string()),
        }
    }
}

fn internal_err<E: std::fmt::Display>(e: E) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: e.to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_leader_and_entry_from_idempotency_key() {
        // wf|leader|source_trade_id|market|outcome|side|observed_at
        let k = "wf|0xleader|0xsrc|0xmarket|3|buy|1705320000";
        let p = ParsedKey::from_key(k);
        assert_eq!(p.leader.as_deref(), Some("0xleader"));
        assert_eq!(p.source_trade_id.as_deref(), Some("0xsrc"));
        assert_eq!(p.entry_unix, Some(1_705_320_000));
        assert_eq!(p.operator_id, None);
    }

    #[test]
    fn parses_operator_for_cluster_keys() {
        let k = "wf|0xleader|0xsrc|0xmarket|3|sell|1705320000|op-42";
        let p = ParsedKey::from_key(k);
        assert_eq!(p.operator_id.as_deref(), Some("op-42"));
    }

    #[test]
    fn legacy_or_foreign_key_yields_empty_fields() {
        let p = ParsedKey::from_key("not-a-winner-follow-key");
        assert!(p.leader.is_none() && p.entry_unix.is_none());
    }
}
