//! Cursor pages share the v1 row semantics, but have a separate completeness proof.

use super::*;
use crate::activity::{NormalizedActivityWindow, parse_row_with_acceptance};
use crate::endpoint::{ActivitySortDirection, V2_PAGE_LIMIT, activity_v2_url, positions_v2_url};

/// Page-evidence schema for v2 activity cursor pages. Row schema remains 2.
pub const ACTIVITY_V2_PAGE_SCHEMA_VERSION: u32 = 3;
/// Page-evidence schema/parser for v2 OPEN positions. The semantic proof remains v1.
pub const POSITION_V2_PROOF_VERSION: u32 = 2;

const POSITION_CHANGING_TYPES: &[ActivityType] = &[
    ActivityType::Trade,
    ActivityType::Split,
    ActivityType::Merge,
    ActivityType::Redeem,
    ActivityType::Conversion,
];

/// The unmodified pagination fields returned by the source.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct V2Pagination {
    pub limit: u32,
    pub offset: u32,
    pub has_more: bool,
    // A terminal cursor must be explicitly null, rather than absent evidence.
    #[serde(deserialize_with = "Option::<String>::deserialize")]
    pub next_cursor: Option<String>,
}

/// A decoded v2 envelope borrowing every row directly from the original body.
#[derive(Debug, Deserialize)]
pub struct V2Page<'a> {
    #[serde(borrow)]
    pub data: Vec<&'a RawValue>,
    pub pagination: V2Pagination,
}

#[derive(Debug, thiserror::Error)]
pub enum V2PageError {
    #[error("invalid v2 page json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("contradictory v2 pagination has_more={has_more}, next_cursor={next_cursor:?}")]
    ContradictoryPagination {
        has_more: bool,
        next_cursor: Option<String>,
    },
    #[error("v2 pagination limit {0} exceeds the source maximum")]
    InvalidLimit(u32),
    #[error("v2 page has {row_count} rows, above pagination limit {limit}")]
    PageTooLarge { row_count: usize, limit: u32 },
}

/// Decode the selected v2 format; bare v1 arrays and incomplete envelopes reject.
pub fn decode_v2_page(raw: &[u8]) -> Result<V2Page<'_>, V2PageError> {
    let page: V2Page<'_> = serde_json::from_slice(raw)?;
    let pagination = &page.pagination;
    if pagination.has_more != pagination.next_cursor.is_some()
        || pagination
            .next_cursor
            .as_ref()
            .is_some_and(String::is_empty)
    {
        return Err(V2PageError::ContradictoryPagination {
            has_more: pagination.has_more,
            next_cursor: pagination.next_cursor.clone(),
        });
    }
    if pagination.limit > V2_PAGE_LIMIT {
        return Err(V2PageError::InvalidLimit(pagination.limit));
    }
    if u32::try_from(page.data.len()).map_or(true, |count| count > pagination.limit) {
        return Err(V2PageError::PageTooLarge {
            row_count: page.data.len(),
            limit: pagination.limit,
        });
    }
    Ok(page)
}

/// Caller-owned filters and parsing policy; no cursor or accumulated state is retained here.
#[derive(Debug, Clone, Copy)]
pub struct V2ActivityRequest<'a> {
    pub requested_wallet: WalletAddress,
    pub types: &'a [ActivityType],
    pub bounds: ActivityRequestBounds,
    pub direction: ActivitySortDirection,
    pub row_acceptance: ActivityRowAcceptance,
    pub retain_provenance: bool,
}

impl V2ActivityRequest<'static> {
    /// Service position-changing read; `None` and `Some(0)` both request full history.
    #[must_use]
    pub fn position_changes(
        requested_wallet: WalletAddress,
        start: Option<i64>,
        fixed_end: i64,
        row_acceptance: ActivityRowAcceptance,
    ) -> Self {
        Self {
            requested_wallet,
            types: POSITION_CHANGING_TYPES,
            bounds: ActivityRequestBounds {
                start,
                end: fixed_end,
            },
            direction: ActivitySortDirection::Desc,
            row_acceptance,
            retain_provenance: true,
        }
    }
}

impl V2ActivityRequest<'_> {
    /// Translate the internal exclusive start once, for first and cursor requests alike.
    pub fn url(&self, base_url: &str, cursor: Option<&str>) -> Result<String, ActivityReadError> {
        let invalid = || ActivityReadError::InvalidBounds {
            start: self.bounds.start,
            end: self.bounds.end,
        };
        // As in v1, a start at or before the epoch reads from the beginning; v2 wants start >= 1.
        let start = match self.bounds.start {
            Some(start) => start.checked_add(1).ok_or_else(invalid)?.max(1),
            None => 1,
        };
        if start > self.bounds.end {
            return Err(invalid());
        }
        Ok(activity_v2_url(
            base_url,
            self.requested_wallet,
            self.types,
            start,
            self.bounds.end,
            self.direction,
            cursor,
        ))
    }
}

/// One parsed page, with completeness fields copied verbatim from its envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V2ActivityPage {
    pub rows: Vec<NormalizedActivity>,
    pub anomalies: Vec<ActivityRowAnomaly>,
    pub evidence: ReconciliationPageEvidence,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub row_count: u32,
}

/// Parse original response bytes with the existing exact-lexeme row parser.
/// Recorders can call this before deciding whether an anomaly boundary is required.
pub fn parse_activity_page_v2(
    raw: &[u8],
    request_url: &str,
    request: &V2ActivityRequest<'_>,
    context: &ActivityParseContext,
) -> Result<V2ActivityPage, ActivityReadError> {
    let page = decode_v2_page(raw)?;
    let row_count =
        u32::try_from(page.data.len()).map_err(|_| ActivityReadError::RowCountOverflow)?;
    let (mut rows, anomalies) = parse_v2_rows(
        page.data,
        request.requested_wallet,
        context,
        request.row_acceptance,
    )?;
    if !request.retain_provenance {
        for row in &mut rows {
            row.raw_row_json.clear();
            row.raw_row_hash.clear();
            row.source_id = SourceId(String::new());
        }
    }
    for timestamp in rows
        .iter()
        .map(|row| row.source_time.0.unix_timestamp())
        .chain(
            anomalies
                .iter()
                .map(|row| row.source_time.0.unix_timestamp()),
        )
    {
        if timestamp > request.bounds.end
            || timestamp < 1
            || request.bounds.start.is_some_and(|start| timestamp <= start)
        {
            return Err(ActivityReadError::RowOutsideBounds {
                timestamp,
                start: request.bounds.start,
                end: request.bounds.end,
            });
        }
    }
    let mut evidence = v2_page_evidence(
        raw,
        PageEvidenceContext {
            request_url,
            bounds: Some(request.bounds),
            partition: None,
            offset: page.pagination.offset,
            row_count,
            received_at: context.received_at.clone(),
            schema_version: ACTIVITY_V2_PAGE_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
        },
        &page.pagination,
    )
    .map_err(ActivityReadError::CanonicalPage)?;
    evidence.anomalies = anomalies.clone();
    Ok(V2ActivityPage {
        rows,
        anomalies,
        evidence,
        next_cursor: page.pagination.next_cursor,
        has_more: page.pagination.has_more,
        row_count,
    })
}

/// Parse a recorded v2 page's rows exactly as `parse_activity_response_with_acceptance` parses a
/// v1 array. The envelope is validated here; completeness belongs to the cursor walk.
pub fn parse_activity_response_v2_with_acceptance(
    raw: &[u8],
    requested_wallet: WalletAddress,
    context: &ActivityParseContext,
    row_acceptance: ActivityRowAcceptance,
) -> Result<NormalizedActivityWindow, ActivityParseError> {
    let page = decode_v2_page(raw).map_err(|error| ActivityParseError::Json {
        message: error.to_string(),
    })?;
    let (rows, anomalies) = parse_v2_rows(page.data, requested_wallet, context, row_acceptance)?;
    Ok(NormalizedActivityWindow {
        requested_wallet,
        rows,
        anomalies,
    })
}

fn parse_v2_rows(
    data: Vec<&RawValue>,
    requested_wallet: WalletAddress,
    context: &ActivityParseContext,
    row_acceptance: ActivityRowAcceptance,
) -> Result<(Vec<NormalizedActivity>, Vec<ActivityRowAnomaly>), ActivityParseError> {
    let mut rows = Vec::with_capacity(data.len());
    let mut anomalies = Vec::new();
    for (index, raw_row) in data.into_iter().enumerate() {
        match parse_row_with_acceptance(
            raw_row.get(),
            Some(requested_wallet),
            context,
            index,
            row_acceptance,
        )? {
            crate::activity::ParsedActivityRow::Row(row) => rows.push(row),
            crate::activity::ParsedActivityRow::Anomaly(anomaly) => anomalies.push(anomaly),
        }
    }
    Ok((rows, anomalies))
}

/// Fetch only the caller's requested page. The injected fetcher owns per-page retries.
pub async fn fetch_activity_page_v2(
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    request: &V2ActivityRequest<'_>,
    request_cursor: Option<&str>,
) -> Result<V2ActivityPage, ActivityReadError> {
    let url = request.url(base_url, request_cursor)?;
    let raw = fetcher
        .fetch(&url)
        .await
        .map_err(|source| ActivityReadError::Fetch {
            url: url.clone(),
            source,
        })?;
    parse_activity_page_v2(
        &raw,
        &url,
        request,
        &activity_context(ReceivedAt::now_utc()),
    )
}

/// Complete one v2 cursor walk, without v1 offset ceilings or window splitting.
pub async fn fetch_complete_activity_v2(
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    request: &V2ActivityRequest<'_>,
) -> Result<CompleteActivityRead, ActivityReadError> {
    let mut walk = CursorWalk::default();
    let mut rows = Vec::new();
    let mut anomalies = Vec::new();
    let mut pages = Vec::new();
    while !walk.terminal {
        let page =
            fetch_activity_page_v2(fetcher, base_url, request, walk.cursor.as_deref()).await?;
        walk.advance(page.next_cursor)?;
        rows.extend(page.rows);
        anomalies.extend(page.anomalies);
        pages.push(page.evidence);
    }
    assemble_v2_activity(request, rows, anomalies, pages)
}

/// Receipt-bound bodies in acquisition order. Receipt/envelope authentication belongs to the caller.
#[derive(Debug, Clone, Copy)]
pub struct V2RawPage<'a> {
    pub request_url: &'a str,
    pub body: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum V2AuthenticationError {
    #[error("v2 raw page count differs from evidence count")]
    PageCountMismatch,
    #[error("v2 cursor repeated: {0}")]
    RepeatedCursor(String),
    #[error("v2 page follows the terminal page")]
    PageAfterTermination,
    #[error("v2 read has no terminal page")]
    MissingTerminalPage,
    #[error("v2 page {index} request URL differs from its cursor chain")]
    RequestUrlMismatch { index: usize },
    #[error("v2 page {index} evidence differs from its raw response")]
    EvidenceMismatch { index: usize },
}

/// Reparse and authenticate a complete v2 activity read. Never infer termination from evidence,
/// row count, or cosmetic offsets; the raw cursor chain and exactly one terminal page prove it.
pub fn reauthenticate_activity_v2(
    base_url: &str,
    request: &V2ActivityRequest<'_>,
    raw_pages: &[V2RawPage<'_>],
    evidence: &[ReconciliationPageEvidence],
) -> Result<CompleteActivityRead, ActivityReadError> {
    if raw_pages.len() != evidence.len() {
        return Err(V2AuthenticationError::PageCountMismatch.into());
    }
    let mut walk = CursorWalk::default();
    let mut rows = Vec::new();
    let mut anomalies = Vec::new();
    for (index, (raw, proof)) in raw_pages.iter().zip(evidence).enumerate() {
        walk.require_open()?;
        if raw.request_url != request.url(base_url, walk.cursor.as_deref())? {
            return Err(V2AuthenticationError::RequestUrlMismatch { index }.into());
        }
        let page = parse_activity_page_v2(
            raw.body,
            raw.request_url,
            request,
            &activity_context(proof.received_at.clone()),
        )?;
        if page.evidence != *proof {
            return Err(V2AuthenticationError::EvidenceMismatch { index }.into());
        }
        walk.advance(page.next_cursor)?;
        rows.extend(page.rows);
        anomalies.extend(page.anomalies);
    }
    walk.require_terminal()?;
    assemble_v2_activity(request, rows, anomalies, evidence.to_vec())
}

fn assemble_v2_activity(
    request: &V2ActivityRequest<'_>,
    rows: Vec<NormalizedActivity>,
    anomalies: Vec<ActivityRowAnomaly>,
    pages: Vec<ReconciliationPageEvidence>,
) -> Result<CompleteActivityRead, ActivityReadError> {
    let mut read = assemble_activity(
        request.requested_wallet,
        request.bounds.end,
        vec![ActivitySegment {
            bounds: request.bounds,
            rows,
            anomalies,
            pages: Vec::new(),
        }],
    )?;
    // Cursor acquisition order is authoritative; pagination.offset is only cosmetic.
    read.pages = pages;
    Ok(read)
}

fn activity_context(received_at: ReceivedAt) -> ActivityParseContext {
    ActivityParseContext {
        source_id: SourceId("polymarket-public.activity-reconciliation".to_owned()),
        observed_at: SourceTimestamp(received_at.0),
        received_at,
        transport: ActivityTransport::Rest,
    }
}

#[derive(Debug, Deserialize)]
struct RawPositionV2<'a> {
    #[serde(borrow)]
    proxy_wallet: Option<&'a RawValue>,
    #[serde(borrow)]
    token_id: Option<&'a RawValue>,
    #[serde(borrow)]
    condition_id: Option<&'a RawValue>,
    outcome_index: Option<u16>,
    current_size: ExactDecimal,
    redeemable: Option<bool>,
    negative_risk: Option<bool>,
}

fn parse_positions_page_v2(
    raw: &[u8],
    url: &str,
    requested_wallet: WalletAddress,
    mapping: &ActivityAssetMapping,
    received_at: ReceivedAt,
    positions: &mut BTreeMap<String, CanonicalPosition>,
) -> Result<(ReconciliationPageEvidence, Option<String>), PositionReadError> {
    let page = decode_v2_page(raw)?;
    let row_count =
        u32::try_from(page.data.len()).map_err(|_| PositionReadError::RowCountOverflow)?;
    for (row_index, row) in page.data.into_iter().enumerate() {
        let decoded: RawPositionV2<'_> =
            serde_json::from_str(row.get()).map_err(PositionReadError::Json)?;
        let position = normalize_position(
            RawPosition {
                proxy_wallet: decoded.proxy_wallet,
                asset: decoded.token_id,
                condition_id: decoded.condition_id,
                outcome_index: decoded.outcome_index,
                size: decoded.current_size,
                neg_risk: decoded.negative_risk,
            },
            requested_wallet,
            mapping,
            row_index,
            decoded.redeemable.ok_or(PositionReadError::MissingField {
                row_index,
                field: "redeemable",
            })?,
        )?;
        let asset = position.asset.0.clone();
        if positions.insert(asset.clone(), position).is_some() {
            return Err(PositionReadError::DuplicateAsset { asset });
        }
    }
    let evidence = v2_page_evidence(
        raw,
        PageEvidenceContext {
            request_url: url,
            bounds: None,
            partition: None,
            offset: page.pagination.offset,
            row_count,
            received_at,
            schema_version: POSITION_V2_PROOF_VERSION,
            parser_version: POSITION_V2_PROOF_VERSION,
        },
        &page.pagination,
    )
    .map_err(PositionReadError::CanonicalPage)?;
    Ok((evidence, page.pagination.next_cursor))
}

/// Fetch the OPEN superset in one cursor walk, using each row's redeemable flag and current size.
pub async fn fetch_complete_positions_v2(
    fetcher: &dyn ReconciliationFetcher,
    base_url: &str,
    requested_wallet: WalletAddress,
    mapping: &ActivityAssetMapping,
) -> Result<CompletePositionsRead, PositionReadError> {
    let mut walk = CursorWalk::default();
    let mut positions = BTreeMap::new();
    let mut pages = Vec::new();
    while !walk.terminal {
        let url = positions_v2_url(base_url, requested_wallet, walk.cursor.as_deref());
        let raw = fetcher
            .fetch(&url)
            .await
            .map_err(|source| PositionReadError::Fetch {
                url: url.clone(),
                source,
            })?;
        let (evidence, cursor) = parse_positions_page_v2(
            &raw,
            &url,
            requested_wallet,
            mapping,
            ReceivedAt::now_utc(),
            &mut positions,
        )?;
        walk.advance(cursor)?;
        pages.push(evidence);
    }
    assemble_v2_positions(requested_wallet, positions, pages)
}

/// Authenticate a recorded OPEN positions cursor walk with the same normalization as acquisition.
pub fn reauthenticate_positions_v2(
    base_url: &str,
    requested_wallet: WalletAddress,
    mapping: &ActivityAssetMapping,
    raw_pages: &[V2RawPage<'_>],
    evidence: &[ReconciliationPageEvidence],
) -> Result<CompletePositionsRead, PositionReadError> {
    if raw_pages.len() != evidence.len() {
        return Err(V2AuthenticationError::PageCountMismatch.into());
    }
    let mut walk = CursorWalk::default();
    let mut positions = BTreeMap::new();
    for (index, (raw, proof)) in raw_pages.iter().zip(evidence).enumerate() {
        walk.require_open()?;
        if raw.request_url != positions_v2_url(base_url, requested_wallet, walk.cursor.as_deref()) {
            return Err(V2AuthenticationError::RequestUrlMismatch { index }.into());
        }
        let (actual, cursor) = parse_positions_page_v2(
            raw.body,
            raw.request_url,
            requested_wallet,
            mapping,
            proof.received_at.clone(),
            &mut positions,
        )?;
        if actual != *proof {
            return Err(V2AuthenticationError::EvidenceMismatch { index }.into());
        }
        walk.advance(cursor)?;
    }
    walk.require_terminal()?;
    assemble_v2_positions(requested_wallet, positions, evidence.to_vec())
}

fn assemble_v2_positions(
    requested_wallet: WalletAddress,
    positions: BTreeMap<String, CanonicalPosition>,
    pages: Vec<ReconciliationPageEvidence>,
) -> Result<CompletePositionsRead, PositionReadError> {
    let positions = positions.into_values().collect::<Vec<_>>();
    let semantic_hash = position_semantic_hash(&positions)?;
    Ok(CompletePositionsRead {
        requested_wallet,
        positions,
        pages,
        semantic_hash,
    })
}

#[derive(Default)]
struct CursorWalk {
    cursor: Option<String>,
    seen: HashSet<String>,
    terminal: bool,
}

impl CursorWalk {
    fn advance(&mut self, next_cursor: Option<String>) -> Result<(), V2AuthenticationError> {
        self.require_open()?;
        if let Some(cursor) = &next_cursor
            && !self.seen.insert(cursor.clone())
        {
            return Err(V2AuthenticationError::RepeatedCursor(cursor.clone()));
        }
        self.terminal = next_cursor.is_none();
        self.cursor = next_cursor;
        Ok(())
    }

    fn require_open(&self) -> Result<(), V2AuthenticationError> {
        if self.terminal {
            Err(V2AuthenticationError::PageAfterTermination)
        } else {
            Ok(())
        }
    }

    fn require_terminal(&self) -> Result<(), V2AuthenticationError> {
        if self.terminal {
            Ok(())
        } else {
            Err(V2AuthenticationError::MissingTerminalPage)
        }
    }
}

fn v2_page_evidence(
    raw: &[u8],
    context: PageEvidenceContext<'_>,
    pagination: &V2Pagination,
) -> Result<ReconciliationPageEvidence, serde_json::Error> {
    Ok(ReconciliationPageEvidence {
        request_url: context.request_url.to_owned(),
        bounds: context.bounds,
        partition: context.partition,
        offset: context.offset,
        row_count: context.row_count,
        canonical_page_hash: canonical_page_hash(raw)?,
        raw_page_hash: blake3::hash(raw).to_hex().to_string(),
        received_at: context.received_at,
        schema_version: context.schema_version,
        parser_version: context.parser_version,
        anomalies: Vec::new(),
        has_more: Some(pagination.has_more),
        next_cursor: pagination.next_cursor.clone(),
    })
}
