//! Shared Polymarket CLOB market-resolution normalization (#544).
//!
//! This module is the sole owner of the CLOB `/markets` response shape used for
//! version-two payout evidence. It preserves presence for closure, fifty-fifty,
//! winner, token identity, and price fields; malformed prices remain typed
//! malformed evidence and are never coerced to zero.

use std::collections::HashSet;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest as _, Sha256};

/// Version of the normalized CLOB payout-evidence schema introduced by #544.
pub const CLOB_RESOLUTION_SCHEMA_VERSION: u32 = 2;
/// Version of the source-owned CLOB resolution parser introduced by #544.
pub const CLOB_RESOLUTION_PARSER_VERSION: u32 = 2;
/// Documented terminal cursor returned by the CLOB `/markets` walk.
pub const CLOB_END_CURSOR: &str = "LTE=";

/// A binary payout vector held as exact decimals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryPayoutVector([Decimal; 2]);

impl BinaryPayoutVector {
    #[must_use]
    pub fn fifty_fifty() -> Self {
        Self([Decimal::new(5, 1), Decimal::new(5, 1)])
    }

    pub fn winner(outcome_index: usize) -> Result<Self, ClobCoverageManifestError> {
        match outcome_index {
            0 => Ok(Self([Decimal::ONE, Decimal::ZERO])),
            1 => Ok(Self([Decimal::ZERO, Decimal::ONE])),
            other => Err(ClobCoverageManifestError::InvalidPayoutVector(format!(
                "binary winner index {other} is out of range"
            ))),
        }
    }

    #[must_use]
    pub const fn decimals(&self) -> &[Decimal; 2] {
        &self.0
    }

    /// Canonical storage spelling shared by SQLite, Parquet, DuckDB, and Python.
    /// Decimal strings avoid every binary-float conversion at reader boundaries.
    #[must_use]
    pub fn canonical_json(&self) -> String {
        format!(
            "[\"{}\",\"{}\"]",
            canonical_decimal(self.0[0]),
            canonical_decimal(self.0[1])
        )
    }

    pub fn from_canonical_json(value: &str) -> Result<Self, ClobCoverageManifestError> {
        let values: Vec<String> = serde_json::from_str(value)
            .map_err(|error| ClobCoverageManifestError::InvalidPayoutVector(error.to_string()))?;
        let [left, right] = values.as_slice() else {
            return Err(ClobCoverageManifestError::InvalidPayoutVector(
                "payout vector must contain exactly two decimal strings".to_owned(),
            ));
        };
        let left = Decimal::from_str_exact(left)
            .map_err(|error| ClobCoverageManifestError::InvalidPayoutVector(error.to_string()))?;
        let right = Decimal::from_str_exact(right)
            .map_err(|error| ClobCoverageManifestError::InvalidPayoutVector(error.to_string()))?;
        let half = Decimal::new(5, 1);
        let is_winner = (left == Decimal::ONE && right == Decimal::ZERO)
            || (left == Decimal::ZERO && right == Decimal::ONE);
        if !is_winner && (left != half || right != half) {
            return Err(ClobCoverageManifestError::InvalidPayoutVector(
                "binary CLOB payout must be [1,0], [0,1], or [0.5,0.5]".to_owned(),
            ));
        }
        let vector = Self([left, right]);
        if vector.canonical_json() != value {
            return Err(ClobCoverageManifestError::InvalidPayoutVector(
                "payout vector is not in canonical storage form".to_owned(),
            ));
        }
        Ok(vector)
    }
}

fn canonical_decimal(value: Decimal) -> String {
    value.normalize().to_string()
}

/// Why complete executable payout evidence could not be derived.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClobPayoutUnresolvedReason {
    OpenMarket,
    IncompleteEvidence,
    ConflictingEvidence,
    MalformedTokenPrice,
}

impl ClobPayoutUnresolvedReason {
    #[must_use]
    pub const fn storage_status(self) -> &'static str {
        match self {
            Self::OpenMarket => "unresolved_open",
            Self::IncompleteEvidence => "unresolved_incomplete",
            Self::ConflictingEvidence => "unresolved_conflicting",
            Self::MalformedTokenPrice => "unresolved_malformed_price",
        }
    }

    pub fn from_storage_status(value: &str) -> Result<Self, ClobCoverageManifestError> {
        match value {
            "unresolved_open" => Ok(Self::OpenMarket),
            "unresolved_incomplete" => Ok(Self::IncompleteEvidence),
            "unresolved_conflicting" => Ok(Self::ConflictingEvidence),
            "unresolved_malformed_price" => Ok(Self::MalformedTokenPrice),
            other => Err(ClobCoverageManifestError::InvalidPayoutVector(format!(
                "unknown payout status {other}"
            ))),
        }
    }
}

/// Typed resolution result. Unresolved evidence is retained with its reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClobPayoutResolution {
    Resolved(BinaryPayoutVector),
    Unresolved(ClobPayoutUnresolvedReason),
}

impl ClobPayoutResolution {
    #[must_use]
    pub const fn storage_status(&self) -> &'static str {
        match self {
            Self::Resolved(_) => "resolved",
            Self::Unresolved(reason) => reason.storage_status(),
        }
    }

    #[must_use]
    pub fn payout_vector_json(&self) -> Option<String> {
        match self {
            Self::Resolved(vector) => Some(vector.canonical_json()),
            Self::Unresolved(_) => None,
        }
    }

    pub fn from_storage(
        status: &str,
        payout_vector_json: Option<&str>,
    ) -> Result<Self, ClobCoverageManifestError> {
        if status == "resolved" {
            let value = payout_vector_json.ok_or_else(|| {
                ClobCoverageManifestError::InvalidPayoutVector(
                    "resolved payout row omitted its vector".to_owned(),
                )
            })?;
            return BinaryPayoutVector::from_canonical_json(value).map(Self::Resolved);
        }
        if payout_vector_json.is_some() {
            return Err(ClobCoverageManifestError::InvalidPayoutVector(
                "unresolved payout row carried a vector".to_owned(),
            ));
        }
        ClobPayoutUnresolvedReason::from_storage_status(status).map(Self::Unresolved)
    }
}

/// Presence-preserving CLOB token price.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ClobTokenPrice {
    #[default]
    Missing,
    Valid(Decimal),
    Malformed(String),
}

impl Serialize for ClobTokenPrice {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(tag = "kind", content = "value", rename_all = "snake_case")]
        enum StoredPrice<'a> {
            Missing,
            Valid(&'a str),
            Malformed(&'a str),
        }

        let normalized;
        let stored = match self {
            Self::Missing => StoredPrice::Missing,
            Self::Valid(value) => {
                normalized = canonical_decimal(*value);
                StoredPrice::Valid(&normalized)
            }
            Self::Malformed(raw) => StoredPrice::Malformed(raw),
        };
        stored.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ClobTokenPrice {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw: Box<RawValue> = Box::deserialize(deserializer)?;
        let lexeme = raw.get().trim();
        if lexeme == "null" {
            return Ok(Self::Missing);
        }
        let unquoted = lexeme
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .unwrap_or(lexeme);
        let parsed =
            Decimal::from_str_exact(unquoted).or_else(|_| Decimal::from_scientific(unquoted));
        Ok(match parsed {
            Ok(value) => Self::Valid(value),
            Err(_) => Self::Malformed(lexeme.to_owned()),
        })
    }
}

/// One CLOB market token with every payout-relevant field retained.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClobToken {
    #[serde(default)]
    pub token_id: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub winner: Option<bool>,
    #[serde(default)]
    pub price: ClobTokenPrice,
}

/// Shared CLOB `/markets/{condition_id}` row.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ClobMarket {
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub end_date_iso: Option<String>,
    pub closed: Option<bool>,
    pub active: Option<bool>,
    #[serde(default)]
    pub accepting_orders: Option<bool>,
    #[serde(default)]
    pub enable_order_book: Option<bool>,
    #[serde(default)]
    pub minimum_order_size: Option<serde_json::Value>,
    #[serde(default)]
    pub minimum_tick_size: Option<serde_json::Value>,
    #[serde(default)]
    pub neg_risk: Option<bool>,
    #[serde(default)]
    pub seconds_delay: Option<u64>,
    /// Legacy long-row maker fee field retained only by the raw source envelope.
    #[serde(default)]
    pub maker_base_fee: Option<serde_json::Value>,
    /// Legacy long-row taker fee field retained only by the raw source envelope.
    #[serde(default)]
    pub taker_base_fee: Option<serde_json::Value>,
    pub is_50_50_outcome: Option<bool>,
    #[serde(default)]
    pub tokens: Vec<ClobToken>,
}

impl ClobMarket {
    #[must_use]
    pub fn resolution_evidence(&self) -> ClobResolutionEvidence {
        ClobResolutionEvidence {
            condition_id: self.condition_id.clone(),
            end_date_iso: self.end_date_iso.clone(),
            closed: self.closed,
            active: self.active,
            is_50_50_outcome: self.is_50_50_outcome,
            tokens: self.tokens.clone(),
            payout: derive_payout(self),
        }
    }
}

/// Shared CLOB paginated response.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ClobMarketsPage {
    #[serde(default)]
    pub data: Vec<ClobMarket>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// Fully retained payout evidence for one parsed market.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClobResolutionEvidence {
    pub condition_id: Option<String>,
    pub end_date_iso: Option<String>,
    pub closed: Option<bool>,
    pub active: Option<bool>,
    pub is_50_50_outcome: Option<bool>,
    pub tokens: Vec<ClobToken>,
    pub payout: ClobPayoutResolution,
}

impl ClobResolutionEvidence {
    pub fn canonical_tokens_json(&self) -> Result<String, ClobResolutionParseError> {
        serde_json::to_string(&self.tokens).map_err(|error| ClobResolutionParseError::Json {
            message: error.to_string(),
        })
    }
}

/// Terminal winner classification retained for the sealed v1 consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClobWinnerVerdict {
    Resolved(u16),
    Voided,
    Pending,
    Invalid,
}

/// Full winner analysis used by both the page walk and per-market audit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClobWinnerAnalysis {
    pub verdict: ClobWinnerVerdict,
    pub has_explicit_winner: bool,
}

#[must_use]
pub fn analyze_clob_winners(tokens: &[ClobToken]) -> ClobWinnerAnalysis {
    let winners: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| token.winner == Some(true))
        .map(|(index, _)| index)
        .collect();
    let has_explicit_winner = !winners.is_empty();
    let verdict = match winners.as_slice() {
        [_, _, ..] => ClobWinnerVerdict::Invalid,
        [only] => match u16::try_from(*only) {
            Err(_) => ClobWinnerVerdict::Invalid,
            Ok(_index) if tokens.iter().any(|token| token.winner.is_none()) => {
                ClobWinnerVerdict::Pending
            }
            Ok(index) => ClobWinnerVerdict::Resolved(index),
        },
        [] if tokens.is_empty() || tokens.iter().any(|token| token.winner.is_none()) => {
            ClobWinnerVerdict::Pending
        }
        [] => ClobWinnerVerdict::Voided,
    };
    ClobWinnerAnalysis {
        verdict,
        has_explicit_winner,
    }
}

fn derive_payout(market: &ClobMarket) -> ClobPayoutResolution {
    if market.closed == Some(false) {
        return ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::OpenMarket);
    }
    if market.closed.is_none() {
        return ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::IncompleteEvidence);
    }
    if market
        .tokens
        .iter()
        .any(|token| matches!(token.price, ClobTokenPrice::Malformed(_)))
    {
        return ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::MalformedTokenPrice);
    }
    if market.tokens.len() != 2
        || market.is_50_50_outcome.is_none()
        || !complete_distinct_token_identity(&market.tokens)
        || market.tokens.iter().any(|token| token.winner.is_none())
    {
        return ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::IncompleteEvidence);
    }
    let prices: Option<Vec<Decimal>> = market
        .tokens
        .iter()
        .map(|token| match token.price {
            ClobTokenPrice::Valid(value) if value >= Decimal::ZERO && value <= Decimal::ONE => {
                Some(value)
            }
            _ => None,
        })
        .collect();
    let Some(prices) = prices else {
        return ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::MalformedTokenPrice);
    };
    let half = Decimal::new(5, 1);
    if market.is_50_50_outcome == Some(true) {
        if market
            .tokens
            .iter()
            .all(|token| token.winner == Some(false))
            && prices.as_slice() == [half, half]
        {
            return ClobPayoutResolution::Resolved(BinaryPayoutVector::fifty_fifty());
        }
        return ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::ConflictingEvidence);
    }

    let winners: Vec<usize> = market
        .tokens
        .iter()
        .enumerate()
        .filter(|(_, token)| token.winner == Some(true))
        .map(|(index, _)| index)
        .collect();
    let [winner] = winners.as_slice() else {
        return ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::ConflictingEvidence);
    };
    let loser = 1usize.saturating_sub(*winner);
    if prices[*winner] != Decimal::ONE || prices[loser] != Decimal::ZERO {
        return ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::ConflictingEvidence);
    }
    match BinaryPayoutVector::winner(*winner) {
        Ok(vector) => ClobPayoutResolution::Resolved(vector),
        Err(_) => ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::ConflictingEvidence),
    }
}

fn complete_distinct_token_identity(tokens: &[ClobToken]) -> bool {
    let mut token_ids = HashSet::with_capacity(tokens.len());
    let mut outcomes = HashSet::with_capacity(tokens.len());
    tokens.iter().all(|token| {
        let token_id = token
            .token_id
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty());
        let outcome = token
            .outcome
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty());
        match (token_id, outcome) {
            (Some(token_id), Some(outcome)) => {
                token_ids.insert(token_id) && outcomes.insert(outcome)
            }
            _ => false,
        }
    })
}

/// Parse failures for the shared CLOB response parser.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ClobResolutionParseError {
    #[error("clob resolution json: {message}")]
    Json { message: String },
}

pub fn parse_clob_market(bytes: &[u8]) -> Result<ClobMarket, ClobResolutionParseError> {
    serde_json::from_slice(bytes).map_err(|error| ClobResolutionParseError::Json {
        message: error.to_string(),
    })
}

pub fn parse_clob_markets_page(bytes: &[u8]) -> Result<ClobMarketsPage, ClobResolutionParseError> {
    serde_json::from_slice(bytes).map_err(|error| ClobResolutionParseError::Json {
        message: error.to_string(),
    })
}

/// Parse an RFC 3339 CLOB end date into Unix seconds.
#[must_use]
pub fn parse_clob_end_date(value: &str) -> Option<i64> {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .ok()
        .map(|timestamp| timestamp.unix_timestamp())
}

#[must_use]
pub fn hash_clob_page_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[must_use]
pub fn is_clob_terminal_cursor(cursor: Option<&str>) -> bool {
    cursor.is_none_or(|value| value.is_empty() || value == CLOB_END_CURSOR)
}

/// Per-page proof retained by a complete CLOB closed-market walk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClobCoveragePage {
    pub ordinal: u64,
    pub request_cursor: Option<String>,
    pub returned_next_cursor: Option<String>,
    pub raw_sha256: String,
    pub market_count: u64,
    pub closed_market_count: u64,
    pub resolved_payout_count: u64,
    pub unresolved_payout_count: u64,
    pub explicit_fifty_fifty_count: u64,
}

impl ClobCoveragePage {
    pub fn from_response(
        ordinal: u64,
        request_cursor: Option<String>,
        raw: &[u8],
        page: &ClobMarketsPage,
    ) -> Result<Self, ClobCoverageManifestError> {
        let market_count =
            u64::try_from(page.data.len()).map_err(|_| ClobCoverageManifestError::CountOverflow)?;
        let closed_market_count = count_markets(&page.data, |market| market.closed == Some(true))?;
        let resolved_payout_count = count_markets(&page.data, |market| {
            matches!(
                market.resolution_evidence().payout,
                ClobPayoutResolution::Resolved(_)
            )
        })?;
        let unresolved_payout_count = market_count
            .checked_sub(resolved_payout_count)
            .ok_or(ClobCoverageManifestError::CountOverflow)?;
        let explicit_fifty_fifty_count =
            count_markets(&page.data, |market| market.is_50_50_outcome == Some(true))?;
        Ok(Self {
            ordinal,
            request_cursor: normalize_cursor(request_cursor),
            returned_next_cursor: page.next_cursor.clone(),
            raw_sha256: hash_clob_page_sha256(raw),
            market_count,
            closed_market_count,
            resolved_payout_count,
            unresolved_payout_count,
            explicit_fifty_fifty_count,
        })
    }
}

fn count_markets(
    markets: &[ClobMarket],
    predicate: impl Fn(&ClobMarket) -> bool,
) -> Result<u64, ClobCoverageManifestError> {
    u64::try_from(markets.iter().filter(|market| predicate(market)).count())
        .map_err(|_| ClobCoverageManifestError::CountOverflow)
}

fn normalize_cursor(cursor: Option<String>) -> Option<String> {
    cursor.filter(|value| !value.is_empty())
}

/// Aggregate counts bound into the coverage manifest.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClobCoverageCounts {
    pub pages: u64,
    pub markets: u64,
    pub closed_markets: u64,
    pub resolved_payouts: u64,
    pub unresolved_payouts: u64,
    pub explicit_fifty_fifty: u64,
}

/// How the last response proved the walk terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClobTerminalKind {
    EndCursor,
    EmptyCursor,
    MissingCursor,
}

/// Hash-bound proof for the terminal response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClobTerminalProof {
    pub kind: ClobTerminalKind,
    pub returned_next_cursor: Option<String>,
    pub terminal_page_sha256: String,
}

/// Coverage required before a version-two payout generation can be installed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClobCoverageManifest {
    pub schema_version: u32,
    pub parser_version: u32,
    pub generation: u64,
    pub walked_start_cursor: Option<String>,
    pub walked_end_cursor: Option<String>,
    pub pages: Vec<ClobCoveragePage>,
    pub counts: ClobCoverageCounts,
    pub terminal_proof: ClobTerminalProof,
}

impl ClobCoverageManifest {
    pub fn complete(
        generation: u64,
        pages: Vec<ClobCoveragePage>,
    ) -> Result<Self, ClobCoverageManifestError> {
        let first = pages.first().ok_or(ClobCoverageManifestError::EmptyWalk)?;
        if first.ordinal != 0 || first.request_cursor.is_some() {
            return Err(ClobCoverageManifestError::DidNotStartAtBeginning);
        }
        let mut counts = ClobCoverageCounts::default();
        for (index, page) in pages.iter().enumerate() {
            let expected_ordinal =
                u64::try_from(index).map_err(|_| ClobCoverageManifestError::CountOverflow)?;
            if page.ordinal != expected_ordinal {
                return Err(ClobCoverageManifestError::NonContiguousPageOrdinal {
                    expected: expected_ordinal,
                    actual: page.ordinal,
                });
            }
            if let Some(next_page) = pages.get(index.saturating_add(1))
                && (is_clob_terminal_cursor(page.returned_next_cursor.as_deref())
                    || normalize_cursor(page.returned_next_cursor.clone())
                        != next_page.request_cursor)
            {
                return Err(ClobCoverageManifestError::BrokenCursorChain {
                    ordinal: page.ordinal,
                });
            }
            counts.pages = counts
                .pages
                .checked_add(1)
                .ok_or(ClobCoverageManifestError::CountOverflow)?;
            counts.markets = counts
                .markets
                .checked_add(page.market_count)
                .ok_or(ClobCoverageManifestError::CountOverflow)?;
            counts.closed_markets = counts
                .closed_markets
                .checked_add(page.closed_market_count)
                .ok_or(ClobCoverageManifestError::CountOverflow)?;
            counts.resolved_payouts = counts
                .resolved_payouts
                .checked_add(page.resolved_payout_count)
                .ok_or(ClobCoverageManifestError::CountOverflow)?;
            counts.unresolved_payouts = counts
                .unresolved_payouts
                .checked_add(page.unresolved_payout_count)
                .ok_or(ClobCoverageManifestError::CountOverflow)?;
            counts.explicit_fifty_fifty = counts
                .explicit_fifty_fifty
                .checked_add(page.explicit_fifty_fifty_count)
                .ok_or(ClobCoverageManifestError::CountOverflow)?;
        }
        let last = pages.last().ok_or(ClobCoverageManifestError::EmptyWalk)?;
        let terminal_kind = match last.returned_next_cursor.as_deref() {
            Some(CLOB_END_CURSOR) => ClobTerminalKind::EndCursor,
            Some("") => ClobTerminalKind::EmptyCursor,
            None => ClobTerminalKind::MissingCursor,
            Some(value) => {
                return Err(ClobCoverageManifestError::NonTerminalCursor(
                    value.to_owned(),
                ));
            }
        };
        Ok(Self {
            schema_version: CLOB_RESOLUTION_SCHEMA_VERSION,
            parser_version: CLOB_RESOLUTION_PARSER_VERSION,
            generation,
            walked_start_cursor: first.request_cursor.clone(),
            walked_end_cursor: last.returned_next_cursor.clone(),
            terminal_proof: ClobTerminalProof {
                kind: terminal_kind,
                returned_next_cursor: last.returned_next_cursor.clone(),
                terminal_page_sha256: last.raw_sha256.clone(),
            },
            pages,
            counts,
        })
    }
}

/// Invalid coverage or canonical payout storage.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ClobCoverageManifestError {
    #[error("complete CLOB walk contained no pages")]
    EmptyWalk,
    #[error("complete CLOB walk did not start at page one")]
    DidNotStartAtBeginning,
    #[error("CLOB page ordinal is not contiguous: expected {expected}, got {actual}")]
    NonContiguousPageOrdinal { expected: u64, actual: u64 },
    #[error("CLOB cursor chain broke after page {ordinal}")]
    BrokenCursorChain { ordinal: u64 },
    #[error("CLOB walk ended at non-terminal cursor {0}")]
    NonTerminalCursor(String),
    #[error("CLOB coverage count overflow")]
    CountOverflow,
    #[error("invalid canonical payout vector: {0}")]
    InvalidPayoutVector(String),
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn market(body: &str) -> ClobResolutionEvidence {
        parse_clob_market(body.as_bytes())
            .unwrap()
            .resolution_evidence()
    }

    #[test]
    fn canonical_vectors_round_trip_without_floats() {
        for vector in [
            BinaryPayoutVector::fifty_fifty(),
            BinaryPayoutVector::winner(0).unwrap(),
            BinaryPayoutVector::winner(1).unwrap(),
        ] {
            let stored = vector.canonical_json();
            assert_eq!(
                BinaryPayoutVector::from_canonical_json(&stored).unwrap(),
                vector
            );
        }
        assert!(BinaryPayoutVector::from_canonical_json(r#"["0.25","0.75"]"#).is_err());
    }

    #[test]
    fn malformed_price_is_not_zero_and_open_or_conflicting_stay_unresolved() {
        let malformed = market(
            r#"{"condition_id":"m","closed":true,"is_50_50_outcome":false,
                "tokens":[{"token_id":"a","outcome":"Yes","price":"bad","winner":true},
                          {"token_id":"b","outcome":"No","price":0,"winner":false}]}"#,
        );
        assert_eq!(
            malformed.payout,
            ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::MalformedTokenPrice)
        );
        assert!(matches!(
            malformed.tokens[0].price,
            ClobTokenPrice::Malformed(_)
        ));

        let open = market(
            r#"{"condition_id":"m","closed":false,"is_50_50_outcome":false,
                "tokens":[{"token_id":"a","outcome":"Yes","price":1,"winner":true},
                          {"token_id":"b","outcome":"No","price":0,"winner":false}]}"#,
        );
        assert_eq!(
            open.payout,
            ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::OpenMarket)
        );

        let conflicting = market(
            r#"{"condition_id":"m","closed":true,"is_50_50_outcome":true,
                "tokens":[{"token_id":"a","outcome":"Yes","price":1,"winner":true},
                          {"token_id":"b","outcome":"No","price":0,"winner":false}]}"#,
        );
        assert_eq!(
            conflicting.payout,
            ClobPayoutResolution::Unresolved(ClobPayoutUnresolvedReason::ConflictingEvidence)
        );
    }

    #[test]
    fn manifest_requires_a_page_one_to_terminal_cursor_chain() {
        let page0 = ClobCoveragePage {
            ordinal: 0,
            request_cursor: None,
            returned_next_cursor: Some("page-2".to_owned()),
            raw_sha256: "a".repeat(64),
            market_count: 1,
            closed_market_count: 1,
            resolved_payout_count: 1,
            unresolved_payout_count: 0,
            explicit_fifty_fifty_count: 0,
        };
        let page1 = ClobCoveragePage {
            ordinal: 1,
            request_cursor: Some("page-2".to_owned()),
            returned_next_cursor: Some(CLOB_END_CURSOR.to_owned()),
            raw_sha256: "b".repeat(64),
            market_count: 1,
            closed_market_count: 1,
            resolved_payout_count: 0,
            unresolved_payout_count: 1,
            explicit_fifty_fifty_count: 1,
        };
        let manifest = ClobCoverageManifest::complete(7, vec![page0, page1]).unwrap();
        assert_eq!(manifest.counts.pages, 2);
        assert_eq!(manifest.counts.markets, 2);
        assert_eq!(manifest.terminal_proof.kind, ClobTerminalKind::EndCursor);
    }
}
