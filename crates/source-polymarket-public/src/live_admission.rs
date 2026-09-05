//! Fail-closed two-source market admission for ordinary live Polymarket copies.
//!
//! This generalizes the isolated canary checks without changing them. Unlike canary admission,
//! NegRisk markets are admitted and `neg_risk` is carried into order preparation. Gamma and CLOB
//! legacy fee flags/rates remain available during the #545 transition. The scheduled end is an
//! explicit source-owned evidence field parsed from the CLOB long-market row.

use pe_core_types::{PolymarketConditionId, PolymarketTokenId, Price, ShareAmount};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use crate::clob_resolution::parse_clob_end_date;

pub const LIVE_MARKET_SCHEMA_VERSION: u32 = 1;
pub const LIVE_MARKET_PARSER_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveFeeEvidence {
    pub gamma_fees_enabled: Option<Value>,
    pub gamma_fee_schedule: Option<Value>,
    pub gamma_maker_base_fee_bps: Option<Value>,
    pub gamma_taker_base_fee_bps: Option<Value>,
    pub clob_maker_base_fee_bps: Option<Value>,
    pub clob_taker_base_fee_bps: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveMarketEvidence {
    pub condition_id: PolymarketConditionId,
    pub ordered_outcome_token_ids: [PolymarketTokenId; 2],
    pub neg_risk: bool,
    pub minimum_tick_size: Price,
    pub minimum_order_size: ShareAmount,
    pub scheduled_end_unix: Option<i64>,
    pub fee_evidence: LiveFeeEvidence,
    pub raw_gamma_market_hash: blake3::Hash,
    pub raw_clob_market_hash: blake3::Hash,
    pub observed_at_unix: i64,
    pub schema_version: u32,
    pub parser_version: u32,
    pub freshness_window_secs: u64,
}

impl LiveMarketEvidence {
    pub fn validate_fresh(&self, now_unix: i64) -> Result<(), LiveMarketError> {
        let age = now_unix
            .checked_sub(self.observed_at_unix)
            .ok_or(LiveMarketError::FutureObservation)?;
        let age = u64::try_from(age).map_err(|_| LiveMarketError::FutureObservation)?;
        if age > self.freshness_window_secs {
            return Err(LiveMarketError::Stale);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LiveMarketError {
    #[error("{origin} market JSON is invalid: {message}")]
    Json {
        origin: &'static str,
        message: String,
    },
    #[error("requested Gamma market is missing or duplicated")]
    MarketCardinality,
    #[error("market is inactive")]
    Inactive,
    #[error("market is closed")]
    Closed,
    #[error("market is not accepting orders")]
    NotAcceptingOrders,
    #[error("market does not enable its order book")]
    OrderBookDisabled,
    #[error("condition/outcome/token mapping is missing or malformed")]
    MissingMapping,
    #[error("Gamma and CLOB market evidence disagree")]
    SourceDisagreement,
    #[error("tick size or minimum order size is missing or invalid")]
    MissingMarketRules,
    #[error("market has a nonzero matching delay")]
    NonzeroDelay,
    #[error("market matching delay is missing")]
    MissingDelay,
    #[error("market observation is from the future")]
    FutureObservation,
    #[error("market observation is stale")]
    Stale,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    condition_id: String,
    active: Option<bool>,
    closed: Option<bool>,
    accepting_orders: Option<bool>,
    enable_order_book: Option<bool>,
    neg_risk: Option<bool>,
    outcomes: Option<Value>,
    clob_token_ids: Option<Value>,
    order_price_min_tick_size: Option<Value>,
    order_min_size: Option<Value>,
    seconds_delay: Option<u64>,
    fees_enabled: Option<Value>,
    fee_schedule: Option<Value>,
    maker_base_fee: Option<Value>,
    taker_base_fee: Option<Value>,
}

#[derive(Deserialize)]
struct ClobMarket {
    condition_id: String,
    end_date_iso: Option<String>,
    active: Option<bool>,
    closed: Option<bool>,
    accepting_orders: Option<bool>,
    enable_order_book: Option<bool>,
    minimum_order_size: Option<Value>,
    minimum_tick_size: Option<Value>,
    neg_risk: Option<bool>,
    seconds_delay: Option<u64>,
    tokens: Option<Vec<ClobToken>>,
    maker_base_fee: Option<Value>,
    taker_base_fee: Option<Value>,
}

#[derive(Deserialize)]
struct ClobToken {
    token_id: String,
    outcome: String,
}

pub fn validate_live_market(
    gamma_raw: &[u8],
    clob_raw: &[u8],
    expected_condition: &PolymarketConditionId,
    observed_at_unix: i64,
    freshness_window_secs: u64,
) -> Result<LiveMarketEvidence, LiveMarketError> {
    let gamma_markets: Vec<GammaMarket> =
        serde_json::from_slice(gamma_raw).map_err(|error| LiveMarketError::Json {
            origin: "Gamma",
            message: error.to_string(),
        })?;
    let mut matching = gamma_markets
        .into_iter()
        .filter(|market| market.condition_id == expected_condition.0);
    let gamma = matching.next().ok_or(LiveMarketError::MarketCardinality)?;
    if matching.next().is_some() {
        return Err(LiveMarketError::MarketCardinality);
    }
    let clob: ClobMarket =
        serde_json::from_slice(clob_raw).map_err(|error| LiveMarketError::Json {
            origin: "CLOB",
            message: error.to_string(),
        })?;

    validate_state(&gamma, &clob)?;
    if clob.condition_id != expected_condition.0 {
        return Err(LiveMarketError::SourceDisagreement);
    }

    let gamma_outcomes = string_array(gamma.outcomes.as_ref())?;
    let gamma_tokens = string_array(gamma.clob_token_ids.as_ref())?;
    let clob_tokens = clob
        .tokens
        .as_ref()
        .ok_or(LiveMarketError::MissingMapping)?;
    if gamma_outcomes.len() != 2
        || gamma_tokens.len() != 2
        || clob_tokens.len() != 2
        || gamma_outcomes
            .iter()
            .any(|outcome| outcome.trim().is_empty())
        || gamma_tokens.iter().any(|token| token.trim().is_empty())
        || clob_tokens
            .iter()
            .any(|token| token.outcome.trim().is_empty() || token.token_id.trim().is_empty())
        || gamma_outcomes[0] == gamma_outcomes[1]
        || gamma_tokens[0] == gamma_tokens[1]
        || clob_tokens[0].token_id == clob_tokens[1].token_id
    {
        return Err(LiveMarketError::MissingMapping);
    }
    if gamma_outcomes
        .iter()
        .zip(clob_tokens)
        .any(|(outcome, token)| outcome != &token.outcome)
        || gamma_tokens
            .iter()
            .zip(clob_tokens)
            .any(|(token_id, token)| token_id != &token.token_id)
    {
        return Err(LiveMarketError::SourceDisagreement);
    }

    let gamma_neg_risk = gamma.neg_risk.ok_or(LiveMarketError::MissingMapping)?;
    let clob_neg_risk = clob.neg_risk.ok_or(LiveMarketError::MissingMapping)?;
    if gamma_neg_risk != clob_neg_risk {
        return Err(LiveMarketError::SourceDisagreement);
    }

    let gamma_tick = price(gamma.order_price_min_tick_size.as_ref())?;
    let clob_tick = price(clob.minimum_tick_size.as_ref())?;
    let gamma_minimum = shares(gamma.order_min_size.as_ref())?;
    let clob_minimum = shares(clob.minimum_order_size.as_ref())?;
    if gamma_tick == Price::ZERO
        || gamma_minimum == ShareAmount::ZERO
        || gamma_tick != clob_tick
        || gamma_minimum != clob_minimum
    {
        return Err(LiveMarketError::SourceDisagreement);
    }

    let ordered_outcome_token_ids: [PolymarketTokenId; 2] = gamma_tokens
        .into_iter()
        .map(PolymarketTokenId)
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| LiveMarketError::MissingMapping)?;
    let scheduled_end_unix = clob.end_date_iso.as_deref().and_then(parse_clob_end_date);
    Ok(LiveMarketEvidence {
        condition_id: expected_condition.clone(),
        ordered_outcome_token_ids,
        neg_risk: gamma_neg_risk,
        minimum_tick_size: gamma_tick,
        minimum_order_size: gamma_minimum,
        scheduled_end_unix,
        fee_evidence: LiveFeeEvidence {
            gamma_fees_enabled: gamma.fees_enabled,
            gamma_fee_schedule: gamma.fee_schedule,
            gamma_maker_base_fee_bps: gamma.maker_base_fee,
            gamma_taker_base_fee_bps: gamma.taker_base_fee,
            clob_maker_base_fee_bps: clob.maker_base_fee,
            clob_taker_base_fee_bps: clob.taker_base_fee,
        },
        raw_gamma_market_hash: blake3::hash(gamma_raw),
        raw_clob_market_hash: blake3::hash(clob_raw),
        observed_at_unix,
        schema_version: LIVE_MARKET_SCHEMA_VERSION,
        parser_version: LIVE_MARKET_PARSER_VERSION,
        freshness_window_secs,
    })
}

fn validate_state(gamma: &GammaMarket, clob: &ClobMarket) -> Result<(), LiveMarketError> {
    if gamma.active != Some(true) || clob.active != Some(true) {
        return Err(LiveMarketError::Inactive);
    }
    if gamma.closed != Some(false) || clob.closed != Some(false) {
        return Err(LiveMarketError::Closed);
    }
    if gamma.accepting_orders != Some(true) || clob.accepting_orders != Some(true) {
        return Err(LiveMarketError::NotAcceptingOrders);
    }
    if gamma.enable_order_book != Some(true) || clob.enable_order_book != Some(true) {
        return Err(LiveMarketError::OrderBookDisabled);
    }
    match (gamma.seconds_delay, clob.seconds_delay) {
        (Some(0), Some(0)) => Ok(()),
        (Some(_), Some(_)) => Err(LiveMarketError::NonzeroDelay),
        _ => Err(LiveMarketError::MissingDelay),
    }
}

fn string_array(value: Option<&Value>) -> Result<Vec<String>, LiveMarketError> {
    let value = value.ok_or(LiveMarketError::MissingMapping)?;
    match value {
        Value::String(encoded) => {
            serde_json::from_str(encoded).map_err(|_| LiveMarketError::MissingMapping)
        }
        Value::Array(items) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or(LiveMarketError::MissingMapping)
            })
            .collect(),
        _ => Err(LiveMarketError::MissingMapping),
    }
}

fn decimal(value: Option<&Value>) -> Result<Decimal, LiveMarketError> {
    let value = value.ok_or(LiveMarketError::MissingMarketRules)?;
    let encoded = match value {
        Value::String(encoded) => encoded.clone(),
        Value::Number(number) => number.to_string(),
        _ => return Err(LiveMarketError::MissingMarketRules),
    };
    encoded
        .parse::<Decimal>()
        .map_err(|_| LiveMarketError::MissingMarketRules)
}

fn price(value: Option<&Value>) -> Result<Price, LiveMarketError> {
    Price::new(decimal(value)?).map_err(|_| LiveMarketError::MissingMarketRules)
}

fn shares(value: Option<&Value>) -> Result<ShareAmount, LiveMarketError> {
    ShareAmount::from_decimal_exact(decimal(value)?)
        .map_err(|_| LiveMarketError::MissingMarketRules)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use serde_json::json;

    use super::*;

    fn condition() -> PolymarketConditionId {
        PolymarketConditionId("0xc".to_owned())
    }

    fn gamma(neg_risk: bool) -> Value {
        json!([{
            "conditionId": "0xc",
            "active": true,
            "closed": false,
            "acceptingOrders": true,
            "enableOrderBook": true,
            "negRisk": neg_risk,
            "outcomes": "[\"Yes\",\"No\"]",
            "clobTokenIds": "[\"11\",\"22\"]",
            "orderPriceMinTickSize": "0.01",
            "orderMinSize": "5",
            "secondsDelay": 0,
            "feesEnabled": false,
            "feeSchedule": null,
            "makerBaseFee": 0,
            "takerBaseFee": 0
        }])
    }

    fn clob(neg_risk: bool) -> Value {
        json!({
            "condition_id": "0xc",
            "end_date_iso": "2026-08-11T12:00:00Z",
            "active": true,
            "closed": false,
            "accepting_orders": true,
            "enable_order_book": true,
            "minimum_order_size": "5",
            "minimum_tick_size": "0.01",
            "neg_risk": neg_risk,
            "seconds_delay": 0,
            "tokens": [
                {"token_id": "11", "outcome": "Yes"},
                {"token_id": "22", "outcome": "No"}
            ],
            "maker_base_fee": 0,
            "taker_base_fee": 0
        })
    }

    fn validate(gamma: &Value, clob: &Value) -> Result<LiveMarketEvidence, LiveMarketError> {
        validate_live_market(
            &serde_json::to_vec(gamma).unwrap(),
            &serde_json::to_vec(clob).unwrap(),
            &condition(),
            1_000,
            30,
        )
    }

    #[test]
    fn negrisk_market_is_admitted_and_carried() {
        let evidence = validate(&gamma(true), &clob(true)).unwrap();
        assert!(evidence.neg_risk);
        assert_eq!(evidence.ordered_outcome_token_ids[0].0, "11");
        assert_eq!(evidence.ordered_outcome_token_ids[1].0, "22");
        assert_eq!(evidence.scheduled_end_unix, Some(1_786_449_600));
    }

    #[test]
    fn fee_flagged_market_is_admitted_with_verbatim_evidence() {
        let mut gamma = gamma(false);
        gamma[0]["feesEnabled"] = json!(true);
        gamma[0]["feeSchedule"] = json!({"rate": 25, "takerOnly": true});
        gamma[0]["makerBaseFee"] = json!(7);
        gamma[0]["takerBaseFee"] = json!(9);
        let mut clob = clob(false);
        clob["maker_base_fee"] = json!("7");
        clob["taker_base_fee"] = json!("9");

        let evidence = validate(&gamma, &clob).unwrap();
        assert_eq!(evidence.fee_evidence.gamma_fees_enabled, Some(json!(true)));
        assert_eq!(
            evidence.fee_evidence.gamma_fee_schedule,
            Some(json!({"rate": 25, "takerOnly": true}))
        );
        assert_eq!(
            evidence.fee_evidence.clob_taker_base_fee_bps,
            Some(json!("9"))
        );
    }

    #[test]
    fn inactive_market_fails_closed() {
        let mut gamma = gamma(false);
        gamma[0]["active"] = json!(false);
        assert_eq!(
            validate(&gamma, &clob(false)),
            Err(LiveMarketError::Inactive)
        );
    }

    #[test]
    fn closed_market_fails_closed() {
        let mut clob = clob(false);
        clob["closed"] = json!(true);
        assert_eq!(validate(&gamma(false), &clob), Err(LiveMarketError::Closed));
    }

    #[test]
    fn non_accepting_market_fails_closed() {
        let mut gamma = gamma(false);
        gamma[0]["acceptingOrders"] = json!(false);
        assert_eq!(
            validate(&gamma, &clob(false)),
            Err(LiveMarketError::NotAcceptingOrders)
        );
    }

    #[test]
    fn missing_mapping_fails_closed() {
        let mut gamma = gamma(false);
        gamma[0].as_object_mut().unwrap().remove("clobTokenIds");
        assert_eq!(
            validate(&gamma, &clob(false)),
            Err(LiveMarketError::MissingMapping)
        );
    }

    #[test]
    fn nonzero_delay_fails_closed() {
        let mut clob = clob(false);
        clob["seconds_delay"] = json!(1);
        assert_eq!(
            validate(&gamma(false), &clob),
            Err(LiveMarketError::NonzeroDelay)
        );
    }

    #[test]
    fn source_disagreement_fails_closed() {
        assert_eq!(
            validate(&gamma(false), &clob(true)),
            Err(LiveMarketError::SourceDisagreement)
        );
    }

    #[test]
    fn freshness_rejects_stale_and_future_evidence() {
        let evidence = validate(&gamma(false), &clob(false)).unwrap();
        assert_eq!(evidence.validate_fresh(1_030), Ok(()));
        assert_eq!(evidence.validate_fresh(1_031), Err(LiveMarketError::Stale));
        assert_eq!(
            evidence.validate_fresh(999),
            Err(LiveMarketError::FutureObservation)
        );
    }
}
