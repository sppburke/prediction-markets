//! Complete fixed-end activity and current-position reads (#544).
//!
//! These readers prove one captured response generation complete. They do not
//! claim either public endpoint is final, so callers must use the causal
//! activity/positions bracket before accepting a leader position proof.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;

use pe_core_types::{
    OutcomeId, PolymarketConditionId, PolymarketTokenId, ReceivedAt, ShareAmount, SourceId,
    SourceTimestamp, WalletAddress,
};
use pe_source_core::SourceError;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;

use crate::activity::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAggregate, ActivityParseContext,
    ActivityParseError, ActivityTransport, NormalizedActivity, aggregate_activity_rows,
    parse_activity_response,
};
use crate::endpoint::{PolymarketEndpoint, PositionPartition};
use crate::fetcher::PageFetcher;

/// Fixed public-API page size for both reconciliation readers.
pub const RECONCILIATION_PAGE_LIMIT: u32 = 500;
/// Greatest offset the Data API documents for `/activity`.
pub const ACTIVITY_MAX_OFFSET: u32 = 5_000;
/// Greatest offset the Data API documents for `/positions`.
pub const POSITIONS_MAX_OFFSET: u32 = 10_000;
/// Version of the current-position semantic proof.
pub const POSITION_PROOF_VERSION: u32 = 1;

/// Object-safe adapter over [`PageFetcher`] for service-owned admission actors.
pub trait ReconciliationFetcher: Send + Sync {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>>;
}

impl<T> ReconciliationFetcher for T
where
    T: PageFetcher + Send + Sync,
{
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(self.fetch_page(url))
    }
}

/// One fixed request interval. `start` is exclusive and `end` is inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityRequestBounds {
    pub start: Option<i64>,
    pub end: i64,
}

/// Page-level evidence. None of these transport fields participates in the
/// current-position semantic equality proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationPageEvidence {
    pub request_url: String,
    pub bounds: Option<ActivityRequestBounds>,
    pub partition: Option<PositionPartition>,
    pub offset: u32,
    pub row_count: u32,
    pub canonical_page_hash: String,
    pub raw_page_hash: String,
    pub received_at: ReceivedAt,
    pub schema_version: u32,
    pub parser_version: u32,
}

/// Complete activity response, normalized into stable ascending order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompleteActivityRead {
    pub requested_wallet: WalletAddress,
    pub fixed_end: i64,
    pub rows: Vec<NormalizedActivity>,
    pub pages: Vec<ReconciliationPageEvidence>,
}

impl CompleteActivityRead {
    /// Complete per-second aggregates in ascending causal order.
    pub fn buckets(&self) -> Result<Vec<Vec<ActivityAggregate>>, ActivityReadError> {
        let aggregates = aggregate_activity_rows(&self.rows)?;
        let mut by_epoch: BTreeMap<i64, Vec<ActivityAggregate>> = BTreeMap::new();
        for aggregate in aggregates {
            by_epoch
                .entry(aggregate.source_time.0.unix_timestamp())
                .or_default()
                .push(aggregate);
        }
        Ok(by_epoch.into_values().collect())
    }

    /// Derive the complete asset mapping used by the positions parser.
    pub fn asset_mapping(&self) -> Result<ActivityAssetMapping, PositionReadError> {
        ActivityAssetMapping::from_rows(&self.rows)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ActivityReadError {
    #[error("activity fetch failed for {url}: {source}")]
    Fetch { url: String, source: SourceError },
    #[error("activity page parse failed: {0}")]
    Parse(#[from] ActivityParseError),
    #[error("activity aggregation failed: {0}")]
    Aggregate(#[from] crate::activity::ActivityAggregationError),
    #[error("activity identity failed: {0}")]
    Identity(#[from] crate::activity::ActivityIdentityError),
    #[error("activity page offset {offset} is not aligned to {limit}")]
    InvalidOffset { offset: u32, limit: u32 },
    #[error("activity row timestamp {timestamp} is outside ({start:?}, {end}]")]
    RowOutsideBounds {
        timestamp: i64,
        start: Option<i64>,
        end: i64,
    },
    #[error("activity window ({start:?}, {end}] cannot be split at {boundary}")]
    InvalidSplit {
        start: Option<i64>,
        end: i64,
        boundary: i64,
    },
    #[error("one-second activity window ending at {end} is still full at offset {offset}")]
    SaturatedTerminalSecond { end: i64, offset: u32 },
    #[error("activity page evidence could not encode a canonical page: {0}")]
    CanonicalPage(serde_json::Error),
    #[error("activity page row count exceeds u32")]
    RowCountOverflow,
    #[error("activity page returned {row_count} rows, above the requested limit {limit}")]
    PageTooLarge { row_count: u32, limit: u32 },
}

struct ActivitySegment {
    bounds: ActivityRequestBounds,
    rows: Vec<NormalizedActivity>,
    pages: Vec<ReconciliationPageEvidence>,
}

/// Fetch one complete position-changing activity history ending at `fixed_end`.
/// Saturated windows are split at an observed integer-second boundary; a
/// saturated one-second window is typed incomplete.
pub async fn fetch_complete_activity(
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    requested_wallet: WalletAddress,
    start: Option<i64>,
    fixed_end: i64,
) -> Result<CompleteActivityRead, ActivityReadError> {
    let mut pending = vec![ActivityRequestBounds {
        start,
        end: fixed_end,
    }];
    let mut complete = Vec::new();

    while let Some(bounds) = pending.pop() {
        match fetch_activity_segment(fetcher, base_url, requested_wallet, bounds).await? {
            SegmentResult::Complete(segment) => complete.push(segment),
            SegmentResult::Saturated { boundary, pages } => {
                complete.push(ActivitySegment {
                    bounds,
                    rows: Vec::new(),
                    pages,
                });
                let terminal_start =
                    boundary
                        .checked_sub(1)
                        .ok_or(ActivityReadError::InvalidSplit {
                            start: bounds.start,
                            end: bounds.end,
                            boundary,
                        })?;
                if bounds.start.is_some_and(|value| value >= terminal_start)
                    && bounds.end <= boundary
                {
                    return Err(ActivityReadError::SaturatedTerminalSecond {
                        end: boundary,
                        offset: ACTIVITY_MAX_OFFSET,
                    });
                }
                if boundary > bounds.end || bounds.start.is_some_and(|value| boundary <= value) {
                    return Err(ActivityReadError::InvalidSplit {
                        start: bounds.start,
                        end: bounds.end,
                        boundary,
                    });
                }

                // Stack is LIFO: push newest first so older windows are fetched first.
                if boundary < bounds.end {
                    pending.push(ActivityRequestBounds {
                        start: Some(boundary),
                        end: bounds.end,
                    });
                }
                pending.push(ActivityRequestBounds {
                    start: Some(terminal_start),
                    end: boundary,
                });
                if bounds.start.is_none_or(|value| value < terminal_start) {
                    pending.push(ActivityRequestBounds {
                        start: bounds.start,
                        end: terminal_start,
                    });
                }
            }
        }
    }

    complete.sort_by_key(|segment| (segment.bounds.end, segment.bounds.start));
    let rows = complete
        .iter_mut()
        .flat_map(|segment| std::mem::take(&mut segment.rows))
        .collect::<Vec<_>>();
    let mut keyed_rows = rows
        .into_iter()
        .map(|row| {
            Ok((
                row.source_time.0.unix_timestamp(),
                row.group_id()?.to_string(),
                row.semantic_row_encoding()?,
                row,
            ))
        })
        .collect::<Result<Vec<_>, ActivityReadError>>()?;
    keyed_rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let rows = keyed_rows.into_iter().map(|(_, _, _, row)| row).collect();
    let mut pages = complete
        .into_iter()
        .flat_map(|segment| segment.pages)
        .collect::<Vec<_>>();
    pages.sort_by_key(|page| {
        let bounds = page.bounds.unwrap_or(ActivityRequestBounds {
            start: None,
            end: i64::MIN,
        });
        (bounds.end, bounds.start, page.offset)
    });
    Ok(CompleteActivityRead {
        requested_wallet,
        fixed_end,
        rows,
        pages,
    })
}

enum SegmentResult {
    Complete(ActivitySegment),
    Saturated {
        boundary: i64,
        pages: Vec<ReconciliationPageEvidence>,
    },
}

async fn fetch_activity_segment(
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    requested_wallet: WalletAddress,
    bounds: ActivityRequestBounds,
) -> Result<SegmentResult, ActivityReadError> {
    let mut rows = Vec::new();
    let mut pages = Vec::new();
    let mut offset = 0_u32;
    loop {
        if !offset.is_multiple_of(RECONCILIATION_PAGE_LIMIT) {
            return Err(ActivityReadError::InvalidOffset {
                offset,
                limit: RECONCILIATION_PAGE_LIMIT,
            });
        }
        let url = PolymarketEndpoint::UserPositionActivityPage {
            user: requested_wallet.to_string(),
            end: bounds.end,
            start: bounds.start,
            offset,
        }
        .url(base_url);
        let raw = fetcher
            .fetch(&url)
            .await
            .map_err(|source| ActivityReadError::Fetch {
                url: url.clone(),
                source,
            })?;
        let received_at = ReceivedAt::now_utc();
        let context = ActivityParseContext {
            source_id: SourceId("polymarket-public.activity-reconciliation".to_owned()),
            observed_at: SourceTimestamp(received_at.0),
            received_at: received_at.clone(),
            transport: ActivityTransport::Rest,
        };
        let page = parse_activity_response(&raw, requested_wallet, &context)?;
        for row in &page.rows {
            let timestamp = row.source_time.0.unix_timestamp();
            if timestamp > bounds.end || bounds.start.is_some_and(|start| timestamp <= start) {
                return Err(ActivityReadError::RowOutsideBounds {
                    timestamp,
                    start: bounds.start,
                    end: bounds.end,
                });
            }
        }
        let row_count =
            u32::try_from(page.rows.len()).map_err(|_| ActivityReadError::RowCountOverflow)?;
        if row_count > RECONCILIATION_PAGE_LIMIT {
            return Err(ActivityReadError::PageTooLarge {
                row_count,
                limit: RECONCILIATION_PAGE_LIMIT,
            });
        }
        pages.push(
            page_evidence(
                &raw,
                PageEvidenceContext {
                    request_url: &url,
                    bounds: Some(bounds),
                    partition: None,
                    offset,
                    row_count,
                    received_at,
                    schema_version: ACTIVITY_SCHEMA_VERSION,
                    parser_version: ACTIVITY_PARSER_VERSION,
                },
            )
            .map_err(ActivityReadError::CanonicalPage)?,
        );
        rows.extend(page.rows);
        if row_count < RECONCILIATION_PAGE_LIMIT {
            return Ok(SegmentResult::Complete(ActivitySegment {
                bounds,
                rows,
                pages,
            }));
        }
        if offset == ACTIVITY_MAX_OFFSET {
            let boundary = rows
                .iter()
                .map(|row| row.source_time.0.unix_timestamp())
                .min()
                .ok_or(ActivityReadError::SaturatedTerminalSecond {
                    end: bounds.end,
                    offset,
                })?;
            return Ok(SegmentResult::Saturated { boundary, pages });
        }
        offset = offset.checked_add(RECONCILIATION_PAGE_LIMIT).ok_or(
            ActivityReadError::InvalidOffset {
                offset,
                limit: RECONCILIATION_PAGE_LIMIT,
            },
        )?;
    }
}

/// Ordinary/combo identity established only by complete activity evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionClassification {
    Ordinary,
    Combo,
}

/// One activity-proven asset identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityAssetIdentity {
    pub condition_id: PolymarketConditionId,
    pub outcome: OutcomeId,
    pub classification: PositionClassification,
}

/// Complete bidirectional mapping from activity token IDs to condition/outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityAssetMapping {
    by_asset: HashMap<PolymarketTokenId, ActivityAssetIdentity>,
    by_outcome: HashMap<(PolymarketConditionId, OutcomeId), PolymarketTokenId>,
}

impl ActivityAssetMapping {
    pub fn from_rows(rows: &[NormalizedActivity]) -> Result<Self, PositionReadError> {
        let mut by_asset = HashMap::new();
        let mut by_outcome = HashMap::new();
        for row in rows {
            let Some(asset) = row.asset.clone() else {
                continue;
            };
            let (Some(condition_id), Some(outcome)) = (row.condition_id.clone(), row.outcome)
            else {
                // SPLIT/MERGE rows may legitimately omit an outcome even when
                // another row supplies an asset. Such a row cannot establish a
                // mapping; a position that relies on it fails later as missing.
                continue;
            };
            let identity = ActivityAssetIdentity {
                condition_id: condition_id.clone(),
                outcome,
                classification: if row.is_combo {
                    PositionClassification::Combo
                } else {
                    PositionClassification::Ordinary
                },
            };
            if let Some(existing) = by_asset.get(&asset)
                && existing != &identity
            {
                return Err(PositionReadError::ConflictingActivityMapping { asset: asset.0 });
            }
            let outcome_key = (condition_id, outcome);
            if let Some(existing_asset) = by_outcome.get(&outcome_key)
                && existing_asset != &asset
            {
                return Err(PositionReadError::ConflictingOutcomeMapping {
                    condition_id: outcome_key.0.0,
                    outcome: outcome.0,
                });
            }
            by_outcome.insert(outcome_key, asset.clone());
            by_asset.insert(asset, identity);
        }
        Ok(Self {
            by_asset,
            by_outcome,
        })
    }

    #[must_use]
    pub fn identity(&self, asset: &PolymarketTokenId) -> Option<&ActivityAssetIdentity> {
        self.by_asset.get(asset)
    }
}

/// One position member of the canonical semantic proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalPosition {
    pub wallet: WalletAddress,
    pub asset: PolymarketTokenId,
    pub condition_id: PolymarketConditionId,
    pub outcome: OutcomeId,
    pub classification: PositionClassification,
    pub size: ShareAmount,
}

/// One independently complete union of the two explicit position partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletePositionsRead {
    pub requested_wallet: WalletAddress,
    pub positions: Vec<CanonicalPosition>,
    pub pages: Vec<ReconciliationPageEvidence>,
    semantic_hash: String,
}

impl CompletePositionsRead {
    #[must_use]
    pub fn semantic_hash(&self) -> &str {
        &self.semantic_hash
    }

    #[must_use]
    pub fn semantically_equal(&self, other: &Self) -> bool {
        self.positions == other.positions
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PositionReadError {
    #[error("positions fetch failed for {url}: {source}")]
    Fetch { url: String, source: SourceError },
    #[error("positions response json: {0}")]
    Json(serde_json::Error),
    #[error("positions row {row_index} omitted {field}")]
    MissingField {
        row_index: usize,
        field: &'static str,
    },
    #[error("positions row {row_index} has invalid wallet {value:?}")]
    InvalidWallet { row_index: usize, value: String },
    #[error(
        "positions row {row_index} wallet {payload_wallet} does not match requested wallet {requested_wallet}"
    )]
    WalletMismatch {
        row_index: usize,
        requested_wallet: WalletAddress,
        payload_wallet: WalletAddress,
    },
    #[error("positions row {row_index} has invalid exact size {value}: {reason}")]
    InvalidAmount {
        row_index: usize,
        value: Decimal,
        reason: String,
    },
    #[error("position asset {asset} has no complete activity mapping")]
    MissingActivityMapping { asset: String },
    #[error("activity asset {asset} has an invalid condition/outcome mapping")]
    InvalidActivityMapping { asset: String },
    #[error("activity asset {asset} has conflicting mappings")]
    ConflictingActivityMapping { asset: String },
    #[error("activity condition {condition_id} outcome {outcome} maps to multiple assets")]
    ConflictingOutcomeMapping { condition_id: String, outcome: u16 },
    #[error("position asset {asset} conflicts with its activity condition/outcome mapping")]
    PositionMappingConflict { asset: String },
    #[error("duplicate or overlapping position asset {asset}")]
    DuplicateAsset { asset: String },
    #[error("positions partition {partition:?} is still full at terminal offset {offset}")]
    SaturatedTerminalPage {
        partition: PositionPartition,
        offset: u32,
    },
    #[error("positions page evidence could not encode a canonical page: {0}")]
    CanonicalPage(serde_json::Error),
    #[error("positions semantic proof component is too long")]
    ProofComponentTooLong,
    #[error("positions page row count exceeds u32")]
    RowCountOverflow,
    #[error("positions page returned {row_count} rows, above the requested limit {limit}")]
    PageTooLarge { row_count: u32, limit: u32 },
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPosition<'a> {
    #[serde(borrow)]
    proxy_wallet: Option<&'a RawValue>,
    #[serde(borrow)]
    asset: Option<&'a RawValue>,
    #[serde(borrow)]
    condition_id: Option<&'a RawValue>,
    outcome_index: Option<u16>,
    size: ExactDecimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExactDecimal(Decimal);

impl<'de> Deserialize<'de> for ExactDecimal {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw: Box<RawValue> = Box::deserialize(deserializer)?;
        let lexeme = raw.get().trim();
        let unquoted = lexeme
            .strip_prefix('"')
            .and_then(|rest| rest.strip_suffix('"'))
            .unwrap_or(lexeme);
        Decimal::from_str_exact(unquoted)
            .map(Self)
            .map_err(serde::de::Error::custom)
    }
}

/// Fetch and union independently complete `redeemable=false` and `true`
/// partitions. Partition layout and page provenance are excluded from semantic
/// equality; duplicate assets across either walk reject the read.
pub async fn fetch_complete_positions(
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    requested_wallet: WalletAddress,
    mapping: &ActivityAssetMapping,
) -> Result<CompletePositionsRead, PositionReadError> {
    let mut positions = BTreeMap::<String, CanonicalPosition>::new();
    let mut pages = Vec::new();
    for partition in [
        PositionPartition::NotRedeemable,
        PositionPartition::Redeemable,
    ] {
        let mut offset = 0_u32;
        loop {
            let url = PolymarketEndpoint::CurrentPositionsReconciliationPage {
                user: requested_wallet.to_string(),
                partition,
                offset,
            }
            .url(base_url);
            let raw = fetcher
                .fetch(&url)
                .await
                .map_err(|source| PositionReadError::Fetch {
                    url: url.clone(),
                    source,
                })?;
            let received_at = ReceivedAt::now_utc();
            let raw_rows: Vec<Box<RawValue>> =
                serde_json::from_slice(&raw).map_err(PositionReadError::Json)?;
            let row_count =
                u32::try_from(raw_rows.len()).map_err(|_| PositionReadError::RowCountOverflow)?;
            if row_count > RECONCILIATION_PAGE_LIMIT {
                return Err(PositionReadError::PageTooLarge {
                    row_count,
                    limit: RECONCILIATION_PAGE_LIMIT,
                });
            }
            for (row_index, raw_row) in raw_rows.into_iter().enumerate() {
                let decoded: RawPosition<'_> =
                    serde_json::from_str(raw_row.get()).map_err(PositionReadError::Json)?;
                let wallet_text =
                    required_raw_string(decoded.proxy_wallet, row_index, "proxyWallet")?;
                let payload_wallet = WalletAddress::from_hex(&wallet_text).map_err(|_| {
                    PositionReadError::InvalidWallet {
                        row_index,
                        value: wallet_text.clone(),
                    }
                })?;
                if payload_wallet != requested_wallet {
                    return Err(PositionReadError::WalletMismatch {
                        row_index,
                        requested_wallet,
                        payload_wallet,
                    });
                }
                let asset =
                    PolymarketTokenId(required_raw_string(decoded.asset, row_index, "asset")?);
                let condition_id = PolymarketConditionId(
                    required_raw_string(decoded.condition_id, row_index, "conditionId")?
                        .to_ascii_lowercase(),
                );
                let outcome = OutcomeId(decoded.outcome_index.ok_or(
                    PositionReadError::MissingField {
                        row_index,
                        field: "outcomeIndex",
                    },
                )?);
                let identity = mapping.identity(&asset).ok_or_else(|| {
                    PositionReadError::MissingActivityMapping {
                        asset: asset.0.clone(),
                    }
                })?;
                if identity.condition_id != condition_id || identity.outcome != outcome {
                    return Err(PositionReadError::PositionMappingConflict { asset: asset.0 });
                }
                let size = ShareAmount::from_decimal_exact(decoded.size.0).map_err(|error| {
                    PositionReadError::InvalidAmount {
                        row_index,
                        value: decoded.size.0,
                        reason: error.to_string(),
                    }
                })?;
                let asset_key = asset.0.clone();
                let position = CanonicalPosition {
                    wallet: requested_wallet,
                    asset,
                    condition_id,
                    outcome,
                    classification: identity.classification,
                    size,
                };
                if positions.insert(asset_key.clone(), position).is_some() {
                    return Err(PositionReadError::DuplicateAsset { asset: asset_key });
                }
            }
            pages.push(
                page_evidence(
                    &raw,
                    PageEvidenceContext {
                        request_url: &url,
                        bounds: None,
                        partition: Some(partition),
                        offset,
                        row_count,
                        received_at,
                        schema_version: POSITION_PROOF_VERSION,
                        parser_version: POSITION_PROOF_VERSION,
                    },
                )
                .map_err(PositionReadError::CanonicalPage)?,
            );
            if row_count < RECONCILIATION_PAGE_LIMIT {
                break;
            }
            if offset == POSITIONS_MAX_OFFSET {
                return Err(PositionReadError::SaturatedTerminalPage { partition, offset });
            }
            offset = offset
                .checked_add(RECONCILIATION_PAGE_LIMIT)
                .ok_or(PositionReadError::SaturatedTerminalPage { partition, offset })?;
        }
    }
    let positions = positions.into_values().collect::<Vec<_>>();
    let semantic_hash = position_semantic_hash(&positions)?;
    Ok(CompletePositionsRead {
        requested_wallet,
        positions,
        pages,
        semantic_hash,
    })
}

fn required_raw_string(
    value: Option<&RawValue>,
    row_index: usize,
    field: &'static str,
) -> Result<String, PositionReadError> {
    let value = value.ok_or(PositionReadError::MissingField { row_index, field })?;
    serde_json::from_str::<String>(value.get())
        .map_err(PositionReadError::Json)
        .and_then(|value| {
            if value.trim().is_empty() {
                Err(PositionReadError::MissingField { row_index, field })
            } else {
                Ok(value)
            }
        })
}

fn position_semantic_hash(positions: &[CanonicalPosition]) -> Result<String, PositionReadError> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"prediction-edge/polymarket-position-proof/v1\0");
    proof_component(
        &mut hasher,
        &u64::try_from(positions.len())
            .map_err(|_| PositionReadError::ProofComponentTooLong)?
            .to_be_bytes(),
    )?;
    for position in positions {
        proof_component(&mut hasher, position.wallet.to_string().as_bytes())?;
        proof_component(&mut hasher, position.asset.0.as_bytes())?;
        proof_component(&mut hasher, position.condition_id.0.as_bytes())?;
        proof_component(&mut hasher, &position.outcome.0.to_be_bytes())?;
        proof_component(
            &mut hasher,
            match position.classification {
                PositionClassification::Ordinary => b"ordinary",
                PositionClassification::Combo => b"combo",
            },
        )?;
        proof_component(&mut hasher, &position.size.atomic().to_be_bytes())?;
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn proof_component(hasher: &mut blake3::Hasher, value: &[u8]) -> Result<(), PositionReadError> {
    let len = u64::try_from(value.len()).map_err(|_| PositionReadError::ProofComponentTooLong)?;
    hasher.update(&len.to_be_bytes());
    hasher.update(value);
    Ok(())
}

struct PageEvidenceContext<'a> {
    request_url: &'a str,
    bounds: Option<ActivityRequestBounds>,
    partition: Option<PositionPartition>,
    offset: u32,
    row_count: u32,
    received_at: ReceivedAt,
    schema_version: u32,
    parser_version: u32,
}

fn page_evidence(
    raw: &[u8],
    context: PageEvidenceContext<'_>,
) -> Result<ReconciliationPageEvidence, serde_json::Error> {
    let value: serde_json::Value = serde_json::from_slice(raw)?;
    let canonical = serde_json::to_vec(&value)?;
    Ok(ReconciliationPageEvidence {
        request_url: context.request_url.to_owned(),
        bounds: context.bounds,
        partition: context.partition,
        offset: context.offset,
        row_count: context.row_count,
        canonical_page_hash: blake3::hash(&canonical).to_hex().to_string(),
        raw_page_hash: blake3::hash(raw).to_hex().to_string(),
        received_at: context.received_at,
        schema_version: context.schema_version,
        parser_version: context.parser_version,
    })
}
