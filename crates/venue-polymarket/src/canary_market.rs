//! Strict CLOB metadata and book parsing for the isolated canary.

use pe_core_types::{
    CollateralAmount, PolymarketConditionId, PolymarketTokenId, Price, ShareAmount,
};
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClobMarketEvidence {
    pub condition_id: PolymarketConditionId,
    pub token_ids: [PolymarketTokenId; 2],
    pub minimum_order_size: ShareAmount,
    pub minimum_tick_size: Price,
    pub raw_market_hash: blake3::Hash,
    pub raw_clob_market_hash: blake3::Hash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Deserialize)]
struct ShortMarket {
    t: Vec<ShortToken>,
    mos: Decimal,
    mts: Decimal,
    mbf: Option<i64>,
    tbf: Option<i64>,
    itode: Option<bool>,
    fd: Option<serde_json::Value>,
    #[serde(default)]
    nr: Option<bool>,
}

#[derive(Deserialize)]
struct ShortToken {
    t: String,
}

pub fn parse_market_evidence(
    long_raw: &[u8],
    short_raw: &[u8],
    expected_condition: &PolymarketConditionId,
    expected_tokens: &[PolymarketTokenId; 2],
) -> Result<ClobMarketEvidence, CanaryMarketError> {
    let long: LongMarket =
        serde_json::from_slice(long_raw).map_err(|e| CanaryMarketError::Json(e.to_string()))?;
    let short: ShortMarket =
        serde_json::from_slice(short_raw).map_err(|e| CanaryMarketError::Json(e.to_string()))?;
    if long.condition_id != expected_condition.0 {
        return Err(CanaryMarketError::IdentityMismatch);
    }
    if !long.active
        || long.closed
        || !long.accepting_orders
        || long.neg_risk
        || long.seconds_delay != Some(0)
        || short.itode == Some(true)
        || short.nr == Some(true)
    {
        return Err(CanaryMarketError::Ineligible);
    }
    if short.mbf.is_some_and(|fee| fee != 0)
        || short.tbf.is_some_and(|fee| fee != 0)
        || short.fd.as_ref().is_some_and(fee_detail_is_nonzero)
    {
        return Err(CanaryMarketError::FeeEvidence);
    }
    let long_tokens: [PolymarketTokenId; 2] = long
        .tokens
        .into_iter()
        .map(|token| PolymarketTokenId(token.token_id))
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| CanaryMarketError::IdentityMismatch)?;
    let short_tokens: [PolymarketTokenId; 2] = short
        .t
        .into_iter()
        .map(|token| PolymarketTokenId(token.t))
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|_| CanaryMarketError::IdentityMismatch)?;
    if &long_tokens != expected_tokens || &short_tokens != expected_tokens {
        return Err(CanaryMarketError::IdentityMismatch);
    }
    let minimum_order_size = ShareAmount::from_decimal_exact(long.minimum_order_size)
        .map_err(|_| CanaryMarketError::MarketRules)?;
    let short_minimum =
        ShareAmount::from_decimal_exact(short.mos).map_err(|_| CanaryMarketError::MarketRules)?;
    let minimum_tick_size =
        Price::new(long.minimum_tick_size).map_err(|_| CanaryMarketError::MarketRules)?;
    let short_tick = Price::new(short.mts).map_err(|_| CanaryMarketError::MarketRules)?;
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

fn fee_detail_is_nonzero(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Number(number) => number.as_i64() != Some(0),
        serde_json::Value::String(text) => text.parse::<Decimal>() != Ok(Decimal::ZERO),
        serde_json::Value::Object(map) => !map.is_empty(),
        _ => true,
    }
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

pub fn executable_ladder(
    snapshot: &CanaryBookSnapshot,
    now_ms: u64,
    requested_shares: ShareAmount,
    minimum_price: Price,
    maximum_price_exclusive: Price,
    origin_ceiling: Price,
) -> Result<ExecutableLadder, CanaryMarketError> {
    if now_ms < snapshot.observed_timestamp_ms || now_ms - snapshot.observed_timestamp_ms > 2_000 {
        return Err(CanaryMarketError::StaleBook);
    }
    if requested_shares < snapshot.minimum_order_size {
        return Err(CanaryMarketError::BelowMinimum);
    }
    let mut remaining = requested_shares.atomic();
    let mut used_asks = Vec::new();
    for level in &snapshot.asks {
        if level.price < minimum_price {
            return Err(CanaryMarketError::InsufficientDepth);
        }
        if level.price >= maximum_price_exclusive || level.price > origin_ceiling {
            break;
        }
        let used = remaining.min(level.shares.atomic());
        if used > 0 {
            used_asks.push(AskLevel {
                price: level.price,
                shares: ShareAmount::from_atomic(used),
            });
            remaining -= used;
        }
        if remaining == 0 {
            break;
        }
    }
    if remaining != 0 {
        return Err(CanaryMarketError::InsufficientDepth);
    }
    let best_ask = used_asks
        .first()
        .map(|level| level.price)
        .ok_or(CanaryMarketError::InsufficientDepth)?;
    let limit_price = used_asks
        .last()
        .map(|level| level.price)
        .ok_or(CanaryMarketError::InsufficientDepth)?;
    let maximum_collateral =
        CollateralAmount::from_decimal_exact(requested_shares.to_decimal() * limit_price.0)
            .map_err(|_| CanaryMarketError::Amount)?;
    Ok(ExecutableLadder {
        used_asks,
        best_ask,
        limit_price,
        shares: requested_shares,
        maximum_collateral,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use pe_core_types::{PolymarketConditionId, PolymarketTokenId};
    use rust_decimal_macros::dec;

    use super::*;

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
        assert_eq!(ladder.best_ask.0, dec!(0.10));
        assert_eq!(ladder.limit_price.0, dec!(0.11));
        assert_eq!(ladder.maximum_collateral.to_decimal(), dec!(0.55));
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
}
