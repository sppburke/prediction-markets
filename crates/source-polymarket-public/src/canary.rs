//! Strict, additive Gamma evidence parser for live canary admission.

use pe_core_types::{OutcomeId, PolymarketConditionId, PolymarketTokenId};
use serde::Deserialize;

pub const GEOPOLITICS_TAG_ID: &str = "100265";
pub const GEOPOLITICS_TAG_SLUG: &str = "geopolitics";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictGammaMarket {
    pub condition_id: PolymarketConditionId,
    pub outcome_id: OutcomeId,
    pub token_id: PolymarketTokenId,
    pub token_ids: [PolymarketTokenId; 2],
    pub raw_hash: blake3::Hash,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StrictGammaError {
    #[error("Gamma JSON is invalid: {0}")]
    Json(String),
    #[error("Gamma tag identity is not the reviewed Geopolitics tag")]
    WrongTag,
    #[error("requested market is missing or duplicated")]
    MarketCardinality,
    #[error("market is inactive, closed, not accepting orders, or has no order book")]
    Inactive,
    #[error("market is Neg-Risk")]
    NegRisk,
    #[error("market does not explicitly prove zero fees")]
    FeeEvidence,
    #[error("market has an explicit positive delay")]
    Delayed,
    #[error("market is not a two-outcome binary")]
    NotBinary,
    #[error("outcome/token mapping is missing or malformed")]
    TokenMapping,
}

#[derive(Deserialize)]
struct Tag {
    id: String,
    slug: String,
}

pub fn verify_geopolitics_tag(raw: &[u8]) -> Result<blake3::Hash, StrictGammaError> {
    let tag: Tag =
        serde_json::from_slice(raw).map_err(|e| StrictGammaError::Json(e.to_string()))?;
    if tag.id != GEOPOLITICS_TAG_ID || tag.slug != GEOPOLITICS_TAG_SLUG {
        return Err(StrictGammaError::WrongTag);
    }
    Ok(blake3::hash(raw))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Market {
    condition_id: String,
    active: bool,
    closed: bool,
    accepting_orders: bool,
    enable_order_book: bool,
    neg_risk: bool,
    outcomes: String,
    clob_token_ids: String,
    fees_enabled: Option<bool>,
    fee_schedule: Option<serde_json::Value>,
    seconds_delay: Option<u64>,
    events: Vec<MarketEvent>,
}

#[derive(Deserialize)]
struct MarketEvent {
    tags: Vec<Tag>,
}

pub fn parse_strict_market(
    raw: &[u8],
    expected_condition: &PolymarketConditionId,
    outcome_id: OutcomeId,
) -> Result<StrictGammaMarket, StrictGammaError> {
    let markets: Vec<Market> =
        serde_json::from_slice(raw).map_err(|e| StrictGammaError::Json(e.to_string()))?;
    let mut matches = markets
        .into_iter()
        .filter(|market| market.condition_id == expected_condition.0);
    let market = matches.next().ok_or(StrictGammaError::MarketCardinality)?;
    if matches.next().is_some() {
        return Err(StrictGammaError::MarketCardinality);
    }
    if !market.active || market.closed || !market.accepting_orders || !market.enable_order_book {
        return Err(StrictGammaError::Inactive);
    }
    if market.neg_risk {
        return Err(StrictGammaError::NegRisk);
    }
    if market.fees_enabled != Some(false) || market.fee_schedule.is_some() {
        return Err(StrictGammaError::FeeEvidence);
    }
    if market.seconds_delay.is_some_and(|delay| delay > 0) {
        return Err(StrictGammaError::Delayed);
    }
    if !market.events.iter().any(|event| {
        event
            .tags
            .iter()
            .any(|tag| tag.id == GEOPOLITICS_TAG_ID && tag.slug == GEOPOLITICS_TAG_SLUG)
    }) {
        return Err(StrictGammaError::WrongTag);
    }
    let outcomes: Vec<String> =
        serde_json::from_str(&market.outcomes).map_err(|_| StrictGammaError::NotBinary)?;
    if outcomes.len() != 2 || outcomes[0] == outcomes[1] {
        return Err(StrictGammaError::NotBinary);
    }
    let tokens: Vec<String> =
        serde_json::from_str(&market.clob_token_ids).map_err(|_| StrictGammaError::TokenMapping)?;
    let token_ids: [PolymarketTokenId; 2] = tokens
        .into_iter()
        .map(PolymarketTokenId)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| StrictGammaError::TokenMapping)?;
    if token_ids.iter().any(|token| token.0.is_empty()) {
        return Err(StrictGammaError::TokenMapping);
    }
    let index = usize::from(outcome_id.0);
    let token_id = token_ids
        .get(index)
        .cloned()
        .ok_or(StrictGammaError::TokenMapping)?;
    Ok(StrictGammaMarket {
        condition_id: expected_condition.clone(),
        outcome_id,
        token_id,
        token_ids,
        raw_hash: blake3::hash(raw),
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    fn market(fees: &str, delay: &str) -> Vec<u8> {
        format!(
            r#"[{{"conditionId":"0xc","active":true,"closed":false,"acceptingOrders":true,"enableOrderBook":true,"negRisk":false,"outcomes":"[\"Yes\",\"No\"]","clobTokenIds":"[\"11\",\"22\"]","feesEnabled":{fees},"feeSchedule":null,"secondsDelay":{delay},"events":[{{"tags":[{{"id":"100265","slug":"geopolitics"}}]}}]}}]"#
        )
        .into_bytes()
    }

    #[test]
    fn strict_mapping_uses_requested_ordinal() {
        let parsed = parse_strict_market(
            &market("false", "null"),
            &PolymarketConditionId("0xc".to_owned()),
            OutcomeId(1),
        )
        .expect("strict fixture should parse");
        assert_eq!(parsed.token_id.0, "22");
    }

    #[test]
    fn fee_and_delay_fail_closed() {
        let condition = PolymarketConditionId("0xc".to_owned());
        assert_eq!(
            parse_strict_market(&market("true", "null"), &condition, OutcomeId(0)),
            Err(StrictGammaError::FeeEvidence)
        );
        assert_eq!(
            parse_strict_market(&market("false", "1"), &condition, OutcomeId(0)),
            Err(StrictGammaError::Delayed)
        );
    }
}
