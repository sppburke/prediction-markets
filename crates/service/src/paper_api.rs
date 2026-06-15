//! HTTP handlers for paper-trader P&L, positions, fills, and dashboard endpoints.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{Json, extract::Extension, http::StatusCode, response::Html};
use pe_core_types::{MarketId, Side};
use pe_paper_pnl::{
    FillOutcome, PortfolioSnapshot, ResolutionStore, TradeView, render_dashboard_html,
    value_portfolio,
};
use pe_paper_state::PaperStateDb;
use rust_decimal::Decimal;
use serde::Serialize;
use tracing::warn;

use crate::market_end_cache::MarketEndCache;
use crate::mid_price_cache::MidPriceCache;

/// Shared state for paper API handlers (threaded in via `Extension`).
#[derive(Clone)]
pub struct PaperApiState {
    pub paper_state: Arc<PaperStateDb>,
    pub resolutions_path: PathBuf,
    pub initial_bankroll: Decimal,
    /// Shared with the orchestrator so dashboard expiration lookups reuse the cache.
    pub market_end_cache: MarketEndCache,
    /// 60 s-TTL cache of live Gamma mids for marking open positions to market.
    pub mid_price_cache: MidPriceCache,
}

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct ErrorBody {
    error: String,
}

// ── Handlers ──────────────────────────────────────────────────────────────────

/// `GET /paper/pnl` — current portfolio snapshot as JSON (open positions marked
/// to market).
pub async fn pnl(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<PortfolioSnapshot>, (StatusCode, Json<ErrorBody>)> {
    let (snapshot, _trades) = build_dashboard(&state).await?;
    Ok(Json(snapshot))
}

/// `GET /paper/positions` — genuinely-open paper positions as JSON. Settled
/// markets are excluded (their rows linger until rebuild but are no longer open).
pub async fn positions(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<Vec<PositionDto>>, (StatusCode, Json<ErrorBody>)> {
    let store = ResolutionStore::load(Arc::clone(&state.paper_state), &state.resolutions_path)
        .map_err(internal_err)?;
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
    let (_snapshot, trades) = build_dashboard(&state).await?;
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
    let (snapshot, trades) = build_dashboard(&state).await?;
    Ok(Html(render_dashboard_html(&snapshot, &trades)))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// One valuation pass behind every paper endpoint. Loads the resolution store and
/// the fills/positions/bankroll, marks open markets to market via the mid cache,
/// then derives **both** the summary snapshot and the per-trade table from a single
/// [`value_portfolio`] call — so the card and the table cannot drift.
///
/// Open vs settled is classified by the resolution store alone. For per-trade
/// status, settled markets are authoritative from the store (`resolved` /
/// `settled_at_unix`); open markets keep the scheduled-end lookup from the
/// [`MarketEndCache`], which is never used to decide whether a row is settled.
async fn build_dashboard(
    state: &PaperApiState,
) -> Result<(PortfolioSnapshot, Vec<TradeView>), (StatusCode, Json<ErrorBody>)> {
    let store = ResolutionStore::load(Arc::clone(&state.paper_state), &state.resolutions_path)
        .map_err(internal_err)?;
    let fills = state.paper_state.list_fills().map_err(internal_err)?;
    let positions = state.paper_state.paper_positions().map_err(internal_err)?;
    let current_bankroll = state
        .paper_state
        .bankroll()
        .map_err(internal_err)?
        .unwrap_or(Decimal::ZERO);

    // Open markets = fills' markets not settled. Dedup before fetching mids.
    let open_markets: Vec<MarketId> = {
        let mut seen = HashSet::new();
        fills
            .iter()
            .map(|f| &f.market_id)
            .filter(|m| !store.is_settled(m))
            .filter(|m| seen.insert((*m).clone()))
            .cloned()
            .collect()
    };
    let open_mids = state.mid_price_cache.fetch_mids(&open_markets).await;

    let valuation = value_portfolio(
        &fills,
        &positions,
        &store,
        &open_mids,
        current_bankroll,
        state.initial_bankroll,
    );

    // Soft reconciliation: log (never panic) if realized P&L diverges from the
    // bankroll-implied settlement credits beyond a cent.
    if valuation.reconciliation_drift.abs() > Decimal::new(1, 2) {
        warn!(
            drift = %valuation.reconciliation_drift,
            "paper-dashboard: realized/bankroll reconciliation drift"
        );
    }

    let snapshot = PortfolioSnapshot::from_valuation(
        &valuation,
        current_bankroll,
        state.initial_bankroll,
        store.total_credits(),
        store.settled_count(),
        fills.len(),
    );

    let mut views = Vec::with_capacity(fills.len());
    for (fill, valued) in fills.iter().zip(valuation.trades.iter()) {
        let parsed = ParsedKey::from_key(&fill.idempotency_key);
        let (resolution_unix, resolution_status) = match store.settlement_info(&fill.market_id) {
            Some(info) => (Some(info.settled_at_unix), Some("resolved".to_string())),
            None => {
                let r = state.market_end_cache.resolution(&fill.market_id).await;
                (r.resolution_unix, r.status)
            }
        };
        views.push(TradeView {
            market_id: fill.market_id.to_string(),
            outcome_id: fill.outcome_id.0,
            side: side_str(fill.side).to_string(),
            contracts: fill.contracts,
            fill_price: fill.fill_price.0,
            leader: parsed.leader.unwrap_or_default(),
            source_trade_id: parsed.source_trade_id.unwrap_or_default(),
            event_seq: fill.event_seq,
            entry_unix: parsed.entry_unix,
            resolution_unix,
            resolution_status,
            outcome: outcome_str(valued.outcome),
            realized_pnl: valued.realized_pnl,
            current_mid: valued.current_mid,
        });
    }
    Ok((snapshot, views))
}

/// Settled-outcome label for the per-trade table; `None` while the market is open.
fn outcome_str(outcome: FillOutcome) -> Option<String> {
    match outcome {
        FillOutcome::Won => Some("won".to_string()),
        FillOutcome::Lost => Some("lost".to_string()),
        FillOutcome::Open => None,
    }
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

/// Fields recovered from a Winner-Follow idempotency key:
/// `wf|{leader}|{source_trade_id}|{market}|{outcome}|{side}|{observed_at}`.
/// (Format defined in `strategy-winner-follow::evaluate::build_idempotency_key`.)
#[derive(Default)]
pub(crate) struct ParsedKey {
    pub(crate) leader: Option<String>,
    pub(crate) source_trade_id: Option<String>,
    pub(crate) entry_unix: Option<i64>,
}

impl ParsedKey {
    pub(crate) fn from_key(key: &str) -> Self {
        let parts: Vec<&str> = key.split('|').collect();
        // Index 0 is the "wf" tag; 1=leader, 2=source_trade_id, 6=observed_at bucket.
        // Anything shorter is a legacy/foreign key — leave fields empty rather than guess.
        if parts.len() < 7 || parts[0] != "wf" {
            return Self::default();
        }
        Self {
            leader: Some(parts[1].to_string()),
            source_trade_id: Some(parts[2].to_string()),
            entry_unix: parts[6].parse().ok(),
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
    }

    #[test]
    fn legacy_or_foreign_key_yields_empty_fields() {
        let p = ParsedKey::from_key("not-a-winner-follow-key");
        assert!(p.leader.is_none() && p.entry_unix.is_none());
    }
}
