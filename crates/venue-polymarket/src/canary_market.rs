//! Strict CLOB metadata and book parsing for the isolated canary.

use pe_core_types::{
    CollateralAmount, PolymarketConditionId, PolymarketTokenId, Price, ShareAmount,
};
use polymarket_client_sdk_v2::clob::types::response::ClobMarketInfoResponse;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use std::collections::BTreeMap;

use crate::fee::{
    CompactFeeSchedule, FeeScheduleError, parse_compact_fee_schedule, parse_decimal_lexeme,
};

/// Economics-bearing projection of the compact `/clob-markets/{condition}` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactMarketEvidence {
    pub condition_id: PolymarketConditionId,
    pub token_ids: [PolymarketTokenId; 2],
    pub minimum_order_size: ShareAmount,
    pub minimum_tick_size: Price,
    pub neg_risk: bool,
    pub fee_schedule: CompactFeeSchedule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClobMarketEvidence {
    pub condition_id: PolymarketConditionId,
    pub token_ids: [PolymarketTokenId; 2],
    pub minimum_order_size: ShareAmount,
    pub minimum_tick_size: Price,
    pub raw_market_hash: blake3::Hash,
    pub raw_clob_market_hash: blake3::Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AskLevel {
    pub price: Price,
    pub shares: ShareAmount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanaryBookSnapshot {
    pub condition_id: PolymarketConditionId,
    pub token_id: PolymarketTokenId,
    pub observed_timestamp_ms: u64,
    pub minimum_order_size: ShareAmount,
    pub minimum_tick_size: Price,
    pub asks: Vec<AskLevel>,
    pub raw_hash: blake3::Hash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ExecutableLadder {
    pub used_asks: Vec<AskLevel>,
    pub best_ask: Price,
    pub limit_price: Price,
    pub shares: ShareAmount,
    pub maximum_collateral: CollateralAmount,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CanaryMarketError {
    #[error("CLOB JSON is invalid: {0}")]
    Json(String),
    #[error("condition or token mapping disagrees across sources")]
    IdentityMismatch,
    #[error("market is inactive, closed, not accepting, delayed, or Neg-Risk")]
    Ineligible,
    #[error("fee metadata is nonzero or ambiguous")]
    FeeEvidence,
    #[error("compact fee schedule is unsupported: {0}")]
    FeeSchedule(#[from] FeeScheduleError),
    #[error("tick or minimum order size is invalid or inconsistent")]
    MarketRules,
    #[error("book is stale or from the future")]
    StaleBook,
    #[error("book is empty, malformed, or one-sided")]
    InvalidBook,
    #[error("requested shares are below the venue minimum")]
    BelowMinimum,
    #[error("ask ladder cannot fill the request within its price bounds")]
    InsufficientDepth,
    #[error("exact amount arithmetic failed")]
    Amount,
}

#[derive(Deserialize)]
struct LongMarket {
    condition_id: String,
    active: bool,
    closed: bool,
    accepting_orders: bool,
    minimum_order_size: Decimal,
    minimum_tick_size: Decimal,
    neg_risk: bool,
    seconds_delay: Option<u64>,
    tokens: Vec<LongToken>,
}

#[derive(Deserialize)]
struct LongToken {
    token_id: String,
}

/// Parse and validate the vendored compact market wire shape for shared economics.
pub fn parse_compact_market(
    short_raw: &[u8],
    expected_condition: &PolymarketConditionId,
    expected_tokens: &[PolymarketTokenId; 2],
) -> Result<CompactMarketEvidence, CanaryMarketError> {
    // Classify from the raw wire bytes before deserializing the shared DTO so an explicit
    // `null` base fee cannot collapse into the same `Option::None` as an absent field.
    let fee_schedule = parse_compact_fee_schedule(short_raw)?;
    let raw_fields: BTreeMap<String, Box<RawValue>> = serde_json::from_slice(short_raw)
        .map_err(|error| CanaryMarketError::Json(error.to_string()))?;
    let minimum_order_size = raw_fields
        .get("mos")
        .ok_or(CanaryMarketError::MarketRules)
        .and_then(|raw| parse_decimal_lexeme(raw).map_err(|_| CanaryMarketError::MarketRules))
        .and_then(|value| {
            ShareAmount::from_decimal_exact(value).map_err(|_| CanaryMarketError::MarketRules)
        })?;
    let minimum_tick_size = raw_fields
        .get("mts")
        .ok_or(CanaryMarketError::MarketRules)
        .and_then(|raw| parse_decimal_lexeme(raw).map_err(|_| CanaryMarketError::MarketRules))
        .and_then(|value| Price::new(value).map_err(|_| CanaryMarketError::MarketRules))?;
    let short: ClobMarketInfoResponse = serde_json::from_slice(short_raw)
        .map_err(|error| CanaryMarketError::Json(error.to_string()))?;
    let condition_id = PolymarketConditionId(short.condition_id.to_string());
    let token_ids: [PolymarketTokenId; 2] = short
        .tokens
        .into_iter()
        .map(|token| {
            token
                .map(|token| PolymarketTokenId(token.token_id.to_string()))
                .ok_or(CanaryMarketError::IdentityMismatch)
        })
        .collect::<Result<Vec<_>, _>>()?
        .try_into()
        .map_err(|_| CanaryMarketError::IdentityMismatch)?;
    if &condition_id != expected_condition || &token_ids != expected_tokens {
        return Err(CanaryMarketError::IdentityMismatch);
    }
    if minimum_order_size == ShareAmount::ZERO || minimum_tick_size == Price::ZERO {
        return Err(CanaryMarketError::MarketRules);
    }
    Ok(CompactMarketEvidence {
        condition_id,
        token_ids,
        minimum_order_size,
        minimum_tick_size,
        neg_risk: short.neg_risk,
        fee_schedule,
    })
}

pub fn parse_market_evidence(
    long_raw: &[u8],
    short_raw: &[u8],
    expected_condition: &PolymarketConditionId,
    expected_tokens: &[PolymarketTokenId; 2],
) -> Result<ClobMarketEvidence, CanaryMarketError> {
    let long: LongMarket =
        serde_json::from_slice(long_raw).map_err(|e| CanaryMarketError::Json(e.to_string()))?;
    let short =
        parse_compact_market(short_raw, expected_condition, expected_tokens).map_err(|error| {
            match error {
                CanaryMarketError::FeeSchedule(_) => CanaryMarketError::FeeEvidence,
                other => other,
            }
        })?;
    if long.condition_id != expected_condition.0 {
        return Err(CanaryMarketError::IdentityMismatch);
    }
    if !long.active
        || long.closed
        || !long.accepting_orders
        || long.neg_risk
        || long.seconds_delay != Some(0)
        || short.neg_risk
    {
        return Err(CanaryMarketError::Ineligible);
    }
    if short.fee_schedule != CompactFeeSchedule::Zero {
        return Err(CanaryMarketError::FeeEvidence);
    }
    let long_tokens: [PolymarketTokenId; 2] = long
        .tokens
        .into_iter()
        .map(|token| PolymarketTokenId(token.token_id))
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| CanaryMarketError::IdentityMismatch)?;
    if &long_tokens != expected_tokens || &short.token_ids != expected_tokens {
        return Err(CanaryMarketError::IdentityMismatch);
    }
    let minimum_order_size = ShareAmount::from_decimal_exact(long.minimum_order_size)
        .map_err(|_| CanaryMarketError::MarketRules)?;
    let short_minimum = short.minimum_order_size;
    let minimum_tick_size =
        Price::new(long.minimum_tick_size).map_err(|_| CanaryMarketError::MarketRules)?;
    let short_tick = short.minimum_tick_size;
    if minimum_order_size == ShareAmount::ZERO
        || minimum_tick_size == Price::ZERO
        || short_minimum != minimum_order_size
        || short_tick != minimum_tick_size
    {
        return Err(CanaryMarketError::MarketRules);
    }
    Ok(ClobMarketEvidence {
        condition_id: expected_condition.clone(),
        token_ids: expected_tokens.clone(),
        minimum_order_size,
        minimum_tick_size,
        raw_market_hash: blake3::hash(long_raw),
        raw_clob_market_hash: blake3::hash(short_raw),
    })
}

#[derive(Deserialize)]
struct RawBook {
    market: String,
    asset_id: String,
    timestamp: String,
    min_order_size: Decimal,
    tick_size: Decimal,
    neg_risk: bool,
    bids: Vec<RawLevel>,
    asks: Vec<RawLevel>,
}

#[derive(Deserialize)]
struct RawLevel {
    price: Decimal,
    size: Decimal,
}

pub fn parse_book(
    raw: &[u8],
    evidence: &ClobMarketEvidence,
    expected_token: &PolymarketTokenId,
) -> Result<CanaryBookSnapshot, CanaryMarketError> {
    let book: RawBook =
        serde_json::from_slice(raw).map_err(|e| CanaryMarketError::Json(e.to_string()))?;
    if book.market != evidence.condition_id.0 || book.asset_id != expected_token.0 {
        return Err(CanaryMarketError::IdentityMismatch);
    }
    if book.neg_risk {
        return Err(CanaryMarketError::Ineligible);
    }
    if book.bids.is_empty() || book.asks.is_empty() {
        return Err(CanaryMarketError::InvalidBook);
    }
    let minimum = ShareAmount::from_decimal_exact(book.min_order_size)
        .map_err(|_| CanaryMarketError::MarketRules)?;
    let tick = Price::new(book.tick_size).map_err(|_| CanaryMarketError::MarketRules)?;
    if minimum != evidence.minimum_order_size || tick != evidence.minimum_tick_size {
        return Err(CanaryMarketError::MarketRules);
    }
    let mut asks = book
        .asks
        .into_iter()
        .map(|level| {
            Ok(AskLevel {
                price: Price::new(level.price).map_err(|_| CanaryMarketError::InvalidBook)?,
                shares: ShareAmount::from_decimal_exact(level.size)
                    .map_err(|_| CanaryMarketError::InvalidBook)?,
            })
        })
        .collect::<Result<Vec<_>, CanaryMarketError>>()?;
    if asks.iter().any(|level| {
        level.price == Price::ZERO
            || level.shares == ShareAmount::ZERO
            || level.price.0 % tick.0 != Decimal::ZERO
    }) {
        return Err(CanaryMarketError::InvalidBook);
    }
    asks.sort_by_key(|level| level.price);
    let observed_timestamp_ms = book
        .timestamp
        .parse::<u64>()
        .map_err(|_| CanaryMarketError::InvalidBook)?;
    Ok(CanaryBookSnapshot {
        condition_id: evidence.condition_id.clone(),
        token_id: expected_token.clone(),
        observed_timestamp_ms,
        minimum_order_size: minimum,
        minimum_tick_size: tick,
        asks,
        raw_hash: blake3::hash(raw),
    })
}

/// The canary's all-or-nothing ladder: exactly `requested_shares` inside the band, priced by
/// the shared planner (#508 Phase A). A thin wrapper over
/// [`crate::ladder::plan_exact_shares`] so reviewed canary behavior cannot drift: this
/// wrapper keeps the canary's staleness bound ([`crate::ladder::LADDER_MAX_AGE_MS`]) and
/// minimum-order check, and maps every planner failure onto the pre-existing
/// [`CanaryMarketError`] surface (a below-band executable ask remains `InsufficientDepth`).
pub fn executable_ladder(
    snapshot: &CanaryBookSnapshot,
    now_ms: u64,
    requested_shares: ShareAmount,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    origin_ceiling: Price,
) -> Result<ExecutableLadder, CanaryMarketError> {
    if crate::ladder::ladder_is_stale(now_ms, snapshot.observed_timestamp_ms) {
        return Err(CanaryMarketError::StaleBook);
    }
    if requested_shares < snapshot.minimum_order_size {
        return Err(CanaryMarketError::BelowMinimum);
    }
    let plan = crate::ladder::plan_exact_shares(
        &snapshot.asks,
        requested_shares,
        minimum_price,
        maximum_price_exclusive,
        origin_ceiling,
    )
    .map_err(|e| match e {
        crate::ladder::LadderError::Amount | crate::ladder::LadderError::Fee(_) => {
            CanaryMarketError::Amount
        }
        crate::ladder::LadderError::BelowBandAsk
        | crate::ladder::LadderError::InsufficientDepth
        | crate::ladder::LadderError::NothingAffordable
        | crate::ladder::LadderError::BelowMinimum
        | crate::ladder::LadderError::CapExceeded
        | crate::ladder::LadderError::NoEdge
        | crate::ladder::LadderError::KellySizing => CanaryMarketError::InsufficientDepth,
    })?;
    Ok(ExecutableLadder {
        used_asks: plan.used_asks,
        best_ask: plan.best_ask,
        limit_price: plan.limit_price,
        shares: plan.shares,
        maximum_collateral: plan.worst_case_debit,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{PolymarketConditionId, PolymarketTokenId};
    use rust_decimal_macros::dec;

    use super::*;

    const CONDITION: &str = "0x4c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ee";

    fn compact_with_fee(fee: &str) -> Vec<u8> {
        format!(
            r#"{{"c":"{CONDITION}","t":[{{"t":"11","o":"Yes"}},{{"t":"22","o":"No"}}],"mts":0.01,"mos":5,"nr":false,{fee}}}"#
        )
        .into_bytes()
    }

    fn compact_identity() -> (PolymarketConditionId, [PolymarketTokenId; 2]) {
        (
            PolymarketConditionId(CONDITION.to_owned()),
            [
                PolymarketTokenId("11".to_owned()),
                PolymarketTokenId("22".to_owned()),
            ],
        )
    }

    fn evidence() -> ClobMarketEvidence {
        ClobMarketEvidence {
            condition_id: PolymarketConditionId("0xc".to_owned()),
            token_ids: [
                PolymarketTokenId("11".to_owned()),
                PolymarketTokenId("22".to_owned()),
            ],
            minimum_order_size: ShareAmount::from_atomic(5_000_000),
            minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
            raw_market_hash: blake3::hash(b"long"),
            raw_clob_market_hash: blake3::hash(b"short"),
        }
    }

    #[test]
    fn exact_ladder_chooses_lowest_complete_limit() {
        let raw = br#"{"market":"0xc","asset_id":"11","timestamp":"1000","min_order_size":"5","tick_size":"0.01","neg_risk":false,"bids":[{"price":"0.08","size":"10"}],"asks":[{"price":"0.11","size":"10"},{"price":"0.10","size":"2"}]}"#;
        let book = parse_book(raw, &evidence(), &PolymarketTokenId("11".to_owned())).unwrap();
        let ladder = executable_ladder(
            &book,
            2_000,
            ShareAmount::from_atomic(5_000_000),
            Price::new(dec!(0.05)).unwrap(),
            Price::new(dec!(0.99)).unwrap(),
            Price::new(dec!(0.11)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            ladder,
            ExecutableLadder {
                used_asks: vec![
                    AskLevel {
                        price: Price::new(dec!(0.10)).unwrap(),
                        shares: ShareAmount::from_decimal_exact(dec!(2)).unwrap(),
                    },
                    AskLevel {
                        price: Price::new(dec!(0.11)).unwrap(),
                        shares: ShareAmount::from_decimal_exact(dec!(3)).unwrap(),
                    },
                ],
                best_ask: Price::new(dec!(0.10)).unwrap(),
                limit_price: Price::new(dec!(0.11)).unwrap(),
                shares: ShareAmount::from_decimal_exact(dec!(5)).unwrap(),
                maximum_collateral: CollateralAmount::from_decimal_exact(dec!(0.55)).unwrap(),
            }
        );
        assert_eq!(
            serde_json::to_vec(&ladder).unwrap(),
            br#"{"used_asks":[{"price":"0.10","shares":2000000},{"price":"0.11","shares":3000000}],"best_ask":"0.10","limit_price":"0.11","shares":5000000,"maximum_collateral":550000}"#
        );
    }

    #[test]
    fn stale_and_insufficient_books_fail_closed() {
        let raw = br#"{"market":"0xc","asset_id":"11","timestamp":"1000","min_order_size":"5","tick_size":"0.01","neg_risk":false,"bids":[{"price":"0.08","size":"10"}],"asks":[{"price":"0.10","size":"5"}]}"#;
        let book = parse_book(raw, &evidence(), &PolymarketTokenId("11".to_owned())).unwrap();
        let args = (
            ShareAmount::from_atomic(5_000_000),
            Price::new(dec!(0.05)).unwrap(),
            Price::new(dec!(0.99)).unwrap(),
            Price::new(dec!(0.09)).unwrap(),
        );
        assert_eq!(
            executable_ladder(&book, 3_001, args.0, args.1, args.2, args.3),
            Err(CanaryMarketError::StaleBook)
        );
        assert_eq!(
            executable_ladder(&book, 2_000, args.0, args.1, args.2, args.3),
            Err(CanaryMarketError::InsufficientDepth)
        );
    }

    #[test]
    fn executable_below_band_ask_fails_closed() {
        let raw = br#"{"market":"0xc","asset_id":"11","timestamp":"1000","min_order_size":"5","tick_size":"0.01","neg_risk":false,"bids":[{"price":"0.08","size":"10"}],"asks":[{"price":"0.04","size":"1"},{"price":"0.10","size":"5"}]}"#;
        let book = parse_book(raw, &evidence(), &PolymarketTokenId("11".to_owned())).unwrap();
        assert_eq!(
            executable_ladder(
                &book,
                2_000,
                ShareAmount::from_atomic(5_000_000),
                Price::new(dec!(0.05)).unwrap(),
                Price::new(dec!(0.99)).unwrap(),
                Price::new(dec!(0.10)).unwrap(),
            ),
            Err(CanaryMarketError::InsufficientDepth)
        );
    }

    #[test]
    fn shared_compact_parser_retains_supported_nonzero_schedule() {
        let (condition, tokens) = compact_identity();
        let parsed = parse_compact_market(
            &compact_with_fee(r#""fd":{"r":0.0025,"e":1,"to":true}"#),
            &condition,
            &tokens,
        )
        .unwrap();
        assert_eq!(
            parsed.fee_schedule,
            CompactFeeSchedule::Taker { rate: dec!(0.0025) }
        );
        assert_eq!(parsed.minimum_order_size.to_decimal(), dec!(5));
        assert_eq!(parsed.minimum_tick_size.0, dec!(0.01));
    }

    #[test]
    fn compact_rules_preserve_exact_raw_numeric_lexemes() {
        let (condition, tokens) = compact_identity();
        let raw = format!(
            r#"{{"c":"{CONDITION}","t":[{{"t":"11","o":"Yes"}},{{"t":"22","o":"No"}}],"mts":"1e-2","mos":"5.000001","nr":false,"fd":{{"r":"1e-7","e":1,"to":true}}}}"#
        );
        let parsed = parse_compact_market(raw.as_bytes(), &condition, &tokens).unwrap();
        assert_eq!(parsed.minimum_order_size.to_decimal(), dec!(5.000001));
        assert_eq!(parsed.minimum_tick_size.0, dec!(0.01));
        assert_eq!(
            parsed.fee_schedule,
            CompactFeeSchedule::Taker {
                rate: dec!(0.0000001)
            }
        );
    }

    #[test]
    fn canary_wrapper_preserves_nonzero_fee_rejection() {
        let (condition, tokens) = compact_identity();
        let long = format!(
            r#"{{"condition_id":"{CONDITION}","active":true,"closed":false,"accepting_orders":true,"minimum_order_size":5,"minimum_tick_size":0.01,"neg_risk":false,"seconds_delay":0,"tokens":[{{"token_id":"11"}},{{"token_id":"22"}}]}}"#
        );
        assert_eq!(
            parse_market_evidence(
                long.as_bytes(),
                &compact_with_fee(r#""fd":{"r":0.0025,"e":1,"to":true}"#),
                &condition,
                &tokens,
            ),
            Err(CanaryMarketError::FeeEvidence)
        );
    }
}
