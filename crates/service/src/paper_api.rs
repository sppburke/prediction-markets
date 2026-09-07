//! HTTP views over one coherent exact paper financial snapshot (#545).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::{Json, extract::Extension, http::StatusCode};
use pe_core_types::{MarketOutcomeId, Side};
use pe_paper_state::{FinancialSnapshot, PaperStateDb};
use rust_decimal::Decimal;
use serde::Serialize;
use time::OffsetDateTime;

use crate::market_end_cache::MarketEndCache;
use crate::mid_price_cache::MidPriceCache;

#[derive(Clone)]
pub struct PaperApiState {
    pub paper_state: Arc<PaperStateDb>,
    pub initial_bankroll: Decimal,
    pub market_end_cache: MarketEndCache,
    pub mid_price_cache: MidPriceCache,
}

#[derive(Serialize)]
pub struct ErrorBody {
    error: String,
}

#[derive(Debug, Serialize)]
pub struct PaperPnlDto {
    bankroll: String,
    initial_bankroll: String,
    open_market_value: String,
    equity: String,
    absolute_pnl: String,
    open_positions: usize,
    settlements_7d: usize,
}

#[derive(Debug, Serialize)]
pub struct PositionDto {
    market_id: String,
    outcome_id: u16,
    long_contracts: String,
    short_contracts: String,
}

#[derive(Debug, Serialize)]
pub struct FillDto {
    idempotency_key: String,
    leader: String,
    source_trade_id: String,
    market_id: String,
    outcome_id: u16,
    side: &'static str,
    quantity: String,
    fill_price: String,
    principal: String,
    fee: String,
    prepared_seq: u64,
    entry_unix: Option<i64>,
    resolution_unix: Option<i64>,
    resolution_status: Option<String>,
}

/// `GET /paper/pnl` — exact decimal strings derived from one SQLite snapshot.
pub async fn pnl(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<PaperPnlDto>, (StatusCode, Json<ErrorBody>)> {
    let snapshot = financial_snapshot(&state)?;
    let ids = snapshot
        .positions
        .iter()
        .map(|position| MarketOutcomeId::new(position.market_id.clone(), position.outcome_id))
        .collect::<Vec<_>>();
    let mids = state
        .mid_price_cache
        .fetch_mids_strict(&ids)
        .await
        .map_err(unavailable_err)?;
    let open_market_value = mark_open_positions(&snapshot, &mids).map_err(internal_err)?;
    let equity = snapshot
        .cash
        .checked_add(open_market_value)
        .ok_or_else(|| internal_err("paper equity overflow"))?;
    let absolute_pnl = equity
        .checked_sub(state.initial_bankroll)
        .ok_or_else(|| internal_err("paper P&L overflow"))?;
    Ok(Json(PaperPnlDto {
        bankroll: decimal_string(snapshot.cash),
        initial_bankroll: decimal_string(state.initial_bankroll),
        open_market_value: decimal_string(open_market_value),
        equity: decimal_string(equity),
        absolute_pnl: decimal_string(absolute_pnl),
        open_positions: snapshot.positions.len(),
        settlements_7d: snapshot.settlements_7d.len(),
    }))
}

/// `GET /paper/positions` — exact six-decimal quantities as decimal strings.
pub async fn positions(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<Vec<PositionDto>>, (StatusCode, Json<ErrorBody>)> {
    let snapshot = financial_snapshot(&state)?;
    Ok(Json(
        snapshot
            .positions
            .into_iter()
            .map(|position| PositionDto {
                market_id: position.market_id.to_string(),
                outcome_id: position.outcome_id.0,
                long_contracts: decimal_string(position.long.to_decimal()),
                short_contracts: decimal_string(position.short.to_decimal()),
            })
            .collect(),
    ))
}

/// `GET /paper/fills` — the exact lifetime fill history.
pub async fn fills(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let fills = state.paper_state.list_fills().map_err(internal_err)?;
    let settled = state
        .paper_state
        .list_settled_markets()
        .map_err(internal_err)?
        .iter()
        .map(|row| (row.market_id.clone(), row.settled_at_unix))
        .collect::<HashMap<_, _>>();
    let mut rows = Vec::with_capacity(fills.len());
    for fill in fills {
        let parsed = ParsedKey::from_key(&fill.idempotency_key);
        let (resolution_unix, resolution_status) = match settled.get(&fill.market_id) {
            Some(settled_at) => (Some(*settled_at), Some("resolved".to_owned())),
            None => {
                let resolution = state.market_end_cache.resolution(&fill.market_id).await;
                (resolution.resolution_unix, resolution.status)
            }
        };
        rows.push(FillDto {
            idempotency_key: fill.idempotency_key,
            leader: parsed.leader.unwrap_or_default(),
            source_trade_id: parsed.source_trade_id.unwrap_or_default(),
            market_id: fill.market_id.to_string(),
            outcome_id: fill.outcome_id.0,
            side: side_str(fill.side),
            quantity: decimal_string(fill.quantity.to_decimal()),
            fill_price: decimal_string(fill.fill_price.0),
            principal: decimal_string(fill.principal.to_decimal()),
            fee: decimal_string(fill.fee.to_decimal()),
            prepared_seq: fill.prepared_seq.0,
            entry_unix: parsed.entry_unix,
            resolution_unix,
            resolution_status,
        });
    }
    Ok(Json(serde_json::json!({
        "fills_count": rows.len(),
        "trades": rows,
    })))
}

/// `GET /paper/status` — exact bankroll strings from the coherent snapshot.
pub async fn status(
    Extension(state): Extension<Arc<PaperApiState>>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorBody>)> {
    let snapshot = financial_snapshot(&state)?;
    let fills_count = state.paper_state.fills_count().map_err(internal_err)?;
    Ok(Json(serde_json::json!({
        "bankroll": decimal_string(snapshot.cash),
        "initial_bankroll": decimal_string(state.initial_bankroll),
        "fills_count": fills_count,
        "last_prepared_seq": snapshot.last_prepared_seq.map(|value| value.0),
    })))
}

fn financial_snapshot(
    state: &PaperApiState,
) -> Result<FinancialSnapshot, (StatusCode, Json<ErrorBody>)> {
    state
        .paper_state
        .financial_snapshot(OffsetDateTime::now_utc().unix_timestamp())
        .map_err(internal_err)
}

fn mark_open_positions(
    snapshot: &FinancialSnapshot,
    mids: &BTreeMap<(String, u16), crate::mid_price_cache::MidPriceObservation>,
) -> Result<Decimal, &'static str> {
    let mut total = Decimal::ZERO;
    for position in &snapshot.positions {
        let price = mids
            .get(&(position.market_id.to_string(), position.outcome_id.0))
            .ok_or("paper position price unavailable")?
            .price;
        let net = position
            .long
            .to_decimal()
            .checked_sub(position.short.to_decimal())
            .ok_or("paper position subtraction overflow")?;
        let value = net
            .checked_mul(price.0)
            .ok_or("paper position mark overflow")?;
        total = total
            .checked_add(value)
            .ok_or("paper portfolio mark overflow")?;
    }
    Ok(total)
}

fn decimal_string(value: Decimal) -> String {
    value.normalize().to_string()
}

fn side_str(side: Side) -> &'static str {
    match side {
        Side::Buy => "buy",
        Side::Sell => "sell",
    }
}

#[derive(Default)]
pub(crate) struct ParsedKey {
    pub(crate) leader: Option<String>,
    pub(crate) source_trade_id: Option<String>,
    pub(crate) entry_unix: Option<i64>,
}

impl ParsedKey {
    pub(crate) fn from_key(key: &str) -> Self {
        let parts: Vec<&str> = key.split('|').collect();
        if parts.len() < 7 || parts[0] != "wf" {
            return Self::default();
        }
        Self {
            leader: Some(parts[1].to_owned()),
            source_trade_id: Some(parts[2].to_owned()),
            entry_unix: parts[6].parse().ok(),
        }
    }
}

fn internal_err<E: std::fmt::Display>(error: E) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: error.to_string(),
        }),
    )
}

fn unavailable_err<E: std::fmt::Display>(error: E) -> (StatusCode, Json<ErrorBody>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorBody {
            error: format!("paper valuation unavailable: {error}"),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_leader_source_and_entry_from_idempotency_key() {
        let parsed = ParsedKey::from_key("wf|0xleader|g2:source|0xmarket|3|buy|1705320000");
        assert_eq!(parsed.leader.as_deref(), Some("0xleader"));
        assert_eq!(parsed.source_trade_id.as_deref(), Some("g2:source"));
        assert_eq!(parsed.entry_unix, Some(1_705_320_000));
    }

    #[test]
    fn legacy_or_foreign_key_yields_empty_fields() {
        let parsed = ParsedKey::from_key("not-a-winner-follow-key");
        assert!(parsed.leader.is_none());
        assert!(parsed.source_trade_id.is_none());
        assert!(parsed.entry_unix.is_none());
    }
}
