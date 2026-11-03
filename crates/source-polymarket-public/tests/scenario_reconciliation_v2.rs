#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Mutex;

use pe_core_types::{
    OutcomeId, PolymarketConditionId, PolymarketTokenId, ReceivedAt, SourceId, SourceTimestamp,
    WalletAddress,
};
use pe_source_core::SourceError;
use pe_source_polymarket_public::*;
use time::OffsetDateTime;

const BASE: &str = "https://data.example";
const WALLET: &str = "0x1408f8931ceb6bb139c3c1b25c3621250f2eafbf";
// Captured 2026-11-02; presentation fields trimmed, numeric lexemes and pagination retained.
const ACTIVITY_1: &[u8] = include_bytes!("fixtures/v2_activity_p1.json");
const ACTIVITY_2: &[u8] = include_bytes!("fixtures/v2_activity_p2.json");
const POSITIONS: &[u8] = include_bytes!("fixtures/v2_positions_p1.json");

fn wallet() -> WalletAddress {
    WalletAddress::from_hex(WALLET).unwrap()
}

fn request() -> V2ActivityRequest<'static> {
    V2ActivityRequest::position_changes(
        wallet(),
        None,
        1_800_000_000,
        ActivityRowAcceptance::StrictWithAnomalies,
    )
}

fn context() -> ActivityParseContext {
    let now = OffsetDateTime::from_unix_timestamp(200).unwrap();
    ActivityParseContext {
        source_id: SourceId("fixture".to_owned()),
        observed_at: SourceTimestamp(now),
        received_at: ReceivedAt(now),
        transport: ActivityTransport::Rest,
    }
}

fn envelope(rows: &[String], offset: u32, cursor: Option<&str>) -> Vec<u8> {
    format!(r#"{{"data":[{}],"pagination":{{"limit":1000,"offset":{offset},"has_more":{},"next_cursor":{}}}}}"#,
        rows.join(","), cursor.is_some(), serde_json::to_string(&cursor).unwrap()).into_bytes()
}

fn trade(timestamp: i64, tx: u32) -> String {
    format!(
        r#"{{"proxy_wallet":"{WALLET}","timestamp":{timestamp},"condition_id":"condition","type":"TRADE","size":1.000001,"usdc_size":0.500001,"transaction_hash":"tx-{tx}","price":0.5,"token_id":"token","side":"BUY","outcome_index":0,"outcome":"Yes"}}"#
    )
}

#[derive(Default)]
struct FakeSource {
    pages: HashMap<String, Vec<u8>>,
    requested: Mutex<Vec<String>>,
}

impl PageFetcher for FakeSource {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        self.requested.lock().unwrap().push(url.to_owned());
        self.pages
            .get(url)
            .cloned()
            .ok_or_else(|| SourceError::Fatal {
                message: format!("unexpected URL {url}"),
            })
    }
}

fn source(pages: &[(String, Vec<u8>)]) -> FakeSource {
    FakeSource {
        pages: pages.iter().cloned().collect(),
        ..FakeSource::default()
    }
}

fn raw_pages(pages: &[(String, Vec<u8>)]) -> Vec<V2RawPage<'_>> {
    pages
        .iter()
        .map(|(url, body)| V2RawPage {
            request_url: url,
            body,
        })
        .collect()
}

fn evidence(
    pages: &[(String, Vec<u8>)],
    request: &V2ActivityRequest<'_>,
) -> Vec<ReconciliationPageEvidence> {
    pages
        .iter()
        .map(|(url, raw)| {
            parse_activity_page_v2(raw, url, request, &context())
                .unwrap()
                .evidence
        })
        .collect()
}

fn small_walk() -> Vec<(String, Vec<u8>)> {
    let request = request();
    vec![
        (
            request.url(BASE, None).unwrap(),
            envelope(&[trade(30, 0)], 0, Some("first+/=")),
        ),
        (
            request.url(BASE, Some("first+/=")).unwrap(),
            envelope(&[trade(20, 1)], 1, Some("second")),
        ),
        (
            request.url(BASE, Some("second")).unwrap(),
            envelope(&[trade(10, 2)], 2, None),
        ),
    ]
}

#[test]
fn v2_builders_pin_first_and_cursor_urls() {
    let request = request();
    assert_eq!(
        request.url(BASE, None).unwrap(),
        format!(
            "{BASE}/v2/activity?user={WALLET}&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&start=1&end=1800000000&sort_direction=DESC&limit=1000"
        )
    );
    assert_eq!(
        request.url(BASE, Some("a+/= &?%")).unwrap(),
        format!(
            "{BASE}/v2/activity?user={WALLET}&type=TRADE%2CSPLIT%2CMERGE%2CREDEEM%2CCONVERSION&start=1&end=1800000000&sort_direction=DESC&cursor=a%2B%2F%3D%20%26%3F%25"
        )
    );
    assert_eq!(
        activity_v2_url(
            BASE,
            wallet(),
            &[ActivityType::Trade],
            11,
            100,
            ActivitySortDirection::Asc,
            None
        ),
        format!(
            "{BASE}/v2/activity?user={WALLET}&type=TRADE&start=11&end=100&sort_direction=ASC&limit=1000"
        )
    );
    assert_eq!(
        activity_v2_url(
            BASE,
            wallet(),
            &[ActivityType::Redeem],
            11,
            100,
            ActivitySortDirection::Asc,
            Some("next")
        ),
        format!(
            "{BASE}/v2/activity?user={WALLET}&type=REDEEM&start=11&end=100&sort_direction=ASC&cursor=next"
        )
    );
    assert_eq!(
        positions_v2_url(BASE, wallet(), None),
        format!(
            "{BASE}/v2/positions?user={WALLET}&status=OPEN&filter_type=TOKENS&filter_amount=0&include_archived=true&sort_by=TOKENS&sort_direction=ASC&limit=1000"
        )
    );
    assert_eq!(
        positions_v2_url(BASE, wallet(), Some("a+/=")),
        format!(
            "{BASE}/v2/positions?user={WALLET}&status=OPEN&filter_type=TOKENS&filter_amount=0&include_archived=true&sort_by=TOKENS&sort_direction=ASC&cursor=a%2B%2F%3D"
        )
    );
    let mut bounded = request;
    bounded.bounds.start = Some(10);
    assert!(bounded.url(BASE, None).unwrap().contains("&start=11&"));
    for start in [Some(0), Some(-1), Some(i64::MIN)] {
        bounded.bounds.start = start;
        assert_eq!(
            bounded.url(BASE, None).unwrap(),
            request.url(BASE, None).unwrap()
        );
    }
    bounded.bounds.start = Some(i64::MAX);
    assert!(matches!(
        bounded.url(BASE, None),
        Err(ActivityReadError::InvalidBounds { .. })
    ));
}

#[test]
fn v2_envelope_requires_array_and_complete_valid_pagination() {
    for raw in [
        r#"[]"#,
        r#"{"pagination":{"limit":1000,"offset":0,"has_more":false,"next_cursor":null}}"#,
        r#"{"data":null,"pagination":{"limit":1000,"offset":0,"has_more":false,"next_cursor":null}}"#,
        r#"{"data":{},"pagination":{"limit":1000,"offset":0,"has_more":false,"next_cursor":null}}"#,
        r#"{"data":[]}"#,
        r#"{"data":[],"pagination":null}"#,
        r#"{"data":[],"pagination":{"offset":0,"has_more":false,"next_cursor":null}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"has_more":false,"next_cursor":null}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":0,"next_cursor":null}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":0,"has_more":false}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":-1,"has_more":false,"next_cursor":null}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":1.5,"has_more":false,"next_cursor":null}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":0,"has_more":"false","next_cursor":null}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":0,"has_more":true,"next_cursor":null}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":0,"has_more":false,"next_cursor":"next"}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":0,"has_more":true,"next_cursor":""}}"#,
        r#"{"data":[],"pagination":{"limit":1000,"offset":0,"has_more":true,"next_cursor":1}}"#,
        r#"{"data":[],"pagination":{"limit":1001,"offset":0,"has_more":false,"next_cursor":null}}"#,
        r#"{"data":[{}],"pagination":{"limit":0,"offset":0,"has_more":false,"next_cursor":null}}"#,
    ] {
        assert!(decode_v2_page(raw.as_bytes()).is_err(), "accepted {raw}");
    }
    assert!(decode_v2_page(br#"{"data":[],"pagination":{"limit":0,"offset":0,"has_more":false,"next_cursor":null}}"#).is_ok());
    for raw in [ACTIVITY_1, ACTIVITY_2, POSITIONS] {
        let page = decode_v2_page(raw).unwrap();
        assert!(page.pagination.has_more);
        // The slices point into the original allocation, including its numeric lexemes.
        assert!(
            page.data
                .iter()
                .all(|row| raw.as_ptr_range().contains(&row.get().as_ptr()))
        );
    }
}

#[tokio::test]
async fn v2_single_page_is_caller_driven_for_both_directions_and_all_modes() {
    for direction in [ActivitySortDirection::Asc, ActivitySortDirection::Desc] {
        for mode in [
            ActivityRowAcceptance::Strict,
            ActivityRowAcceptance::StrictWithAnomalies,
            ActivityRowAcceptance::Acquisition3,
            ActivityRowAcceptance::Acquisition4,
        ] {
            for retain_provenance in [true, false] {
                let request = V2ActivityRequest {
                    direction,
                    row_acceptance: mode,
                    retain_provenance,
                    ..request()
                };
                let pages = vec![
                    (request.url(BASE, None).unwrap(), ACTIVITY_1.to_vec()),
                    (
                        request.url(BASE, Some("caller-cursor")).unwrap(),
                        ACTIVITY_2.to_vec(),
                    ),
                ];
                let api = source(&pages);
                for (cursor, fixture) in [(None, ACTIVITY_1), (Some("caller-cursor"), ACTIVITY_2)] {
                    let page = fetch_activity_page_v2(&api, BASE, &request, cursor)
                        .await
                        .unwrap();
                    let decoded = decode_v2_page(fixture).unwrap();
                    assert_eq!(page.row_count, 5);
                    assert_eq!(page.rows.len(), 5);
                    assert!(page.anomalies.is_empty());
                    assert_eq!(page.has_more, decoded.pagination.has_more);
                    assert_eq!(page.next_cursor, decoded.pagination.next_cursor);
                    assert_eq!(page.evidence.offset, decoded.pagination.offset);
                    assert_eq!(
                        page.evidence.raw_page_hash,
                        blake3::hash(fixture).to_hex().as_str()
                    );
                    assert_eq!(page.evidence.schema_version, 3);
                    for (row, raw) in page.rows.iter().zip(decoded.data) {
                        assert_eq!(row.schema_version, ACTIVITY_SCHEMA_VERSION);
                        assert_eq!(row.raw_row_json.is_empty(), !retain_provenance);
                        assert_eq!(row.raw_row_hash.is_empty(), !retain_provenance);
                        assert_eq!(row.source_id.0.is_empty(), !retain_provenance);
                        if retain_provenance {
                            assert_eq!(row.raw_row_json, raw.get());
                            assert_eq!(
                                row.raw_row_hash,
                                blake3::hash(raw.get().as_bytes()).to_hex().as_str()
                            );
                        }
                    }
                }
                assert_eq!(api.requested.lock().unwrap().len(), 2);
            }
        }
    }
}

#[test]
fn v2_exact_decimals_and_sentinel_semantics_match_v1() {
    let exact = trade(50, 1).replace("1.000001", "10000000000000.000001");
    let raw = envelope(std::slice::from_ref(&exact), 0, None);
    let page = parse_activity_page_v2(&raw, "url", &request(), &context()).unwrap();
    assert_eq!(
        page.rows[0].share_amount.atomic(),
        10_000_000_000_000_000_001
    );
    assert_eq!(page.rows[0].source_usdc_amount.atomic(), 500_001);
    let legacy = exact.replace("token_id", "asset");
    let v1 =
        parse_activity_response(format!("[{legacy}]").as_bytes(), wallet(), &context()).unwrap();
    assert_eq!(
        page.rows[0].semantic_row_encoding().unwrap(),
        v1.rows[0].semantic_row_encoding().unwrap()
    );
    assert_eq!(
        page.rows[0].group_id().unwrap(),
        v1.rows[0].group_id().unwrap()
    );
    // Evidence reuses both existing page hashes; only the raw hash keeps every lexeme.
    assert_eq!(
        page.evidence.canonical_page_hash,
        canonical_page_hash(&raw).unwrap()
    );
    assert_eq!(
        page.evidence.raw_page_hash,
        blake3::hash(&raw).to_hex().to_string()
    );
    let changed = envelope(
        &[exact.replace("10000000000000.000001", "10000000000000.000002")],
        0,
        None,
    );
    assert_ne!(
        parse_activity_page_v2(&changed, "url", &request(), &context())
            .unwrap()
            .evidence
            .raw_page_hash,
        page.evidence.raw_page_hash
    );

    let sentinel = trade(50, 1).replace(
        "\"outcome_index\":0,\"outcome\":\"Yes\"",
        "\"outcome_index\":999,\"outcome\":\"\"",
    );
    let rows = vec![
        sentinel.replace("TRADE", "REDEEM"),
        sentinel.replace("\"side\":\"BUY\"", "\"is_combo\":true,\"side\":\"\""),
        sentinel.replace("1.000001", "0.0"),
        sentinel.clone(),
    ];
    let raw = envelope(&rows, 0, None);
    let parsed = parse_activity_page_v2(&raw, "url", &request(), &context()).unwrap();
    assert_eq!(parsed.rows.len(), 3);
    assert!(parsed.rows.iter().all(|row| row.outcome.is_none()));
    assert_eq!(parsed.anomalies.len(), 1);
    assert_eq!(parsed.anomalies[0].row_index, 3);
    assert_eq!(
        parsed.anomalies[0].kind,
        ActivityRowAnomalyKind::UnmappedOutcome
    );
    assert_eq!(
        parsed.anomalies[0].raw_row_hash,
        blake3::hash(sentinel.as_bytes()).to_hex().as_str()
    );
    for row in rows {
        let legacy = row.replace("token_id", "asset");
        let v1 = parse_activity_response_with_acceptance(
            format!("[{legacy}]").as_bytes(),
            wallet(),
            &context(),
            ActivityRowAcceptance::StrictWithAnomalies,
        )
        .unwrap();
        let v2 = parse_activity_page_v2(&envelope(&[row], 0, None), "url", &request(), &context())
            .unwrap();
        assert_eq!(v1.rows.len(), v2.rows.len());
        assert_eq!(v1.anomalies.len(), v2.anomalies.len());
    }
    let strict = V2ActivityRequest {
        row_acceptance: ActivityRowAcceptance::Strict,
        ..request()
    };
    assert!(matches!(
        parse_activity_page_v2(&raw, "url", &strict, &context()),
        Err(ActivityReadError::Parse(ActivityParseError::InvalidRow {
            row_index: 3,
            ..
        }))
    ));
}

#[tokio::test]
async fn v2_complete_walk_and_reauthentication_preserve_order_and_terminal_state() {
    let pages = small_walk();
    let api = source(&pages);
    let read = fetch_complete_activity_v2(&api, BASE, &request())
        .await
        .unwrap();
    assert_eq!(
        read.rows
            .iter()
            .map(|row| row.source_time.0.unix_timestamp())
            .collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    assert_eq!(
        *api.requested.lock().unwrap(),
        pages.iter().map(|(url, _)| url.clone()).collect::<Vec<_>>()
    );
    assert_eq!(read.pages.last().unwrap().has_more, Some(false));
    assert_eq!(read.pages.last().unwrap().next_cursor, None);
    let authenticated =
        reauthenticate_activity_v2(BASE, &request(), &raw_pages(&pages), &read.pages).unwrap();
    assert_eq!(authenticated, read);
    assert_eq!(
        read.pages
            .iter()
            .filter(|page| page.has_more == Some(false))
            .count(),
        1
    );

    // Offsets authenticate raw equality only; they cannot reorder or truncate a cursor walk.
    let mut cosmetic = pages.clone();
    cosmetic[0].1 = envelope(&[trade(30, 0)], 10_000, Some("first+/="));
    cosmetic[1].1 = envelope(&[trade(20, 1)], 0, Some("second"));
    let read = fetch_complete_activity_v2(&source(&cosmetic), BASE, &request())
        .await
        .unwrap();
    assert_eq!(
        read.pages.iter().map(|p| p.offset).collect::<Vec<_>>(),
        vec![10_000, 0, 2]
    );
    assert!(
        reauthenticate_activity_v2(BASE, &request(), &raw_pages(&cosmetic), &read.pages).is_ok()
    );
}

#[tokio::test]
async fn v2_5500_rows_finish_at_offset_5000_without_splitting() {
    let mut request = request();
    request.bounds = ActivityRequestBounds {
        start: Some(0),
        end: 100,
    };
    let mut pages = Vec::new();
    for index in 0..6_u32 {
        let count = if index == 5 { 500 } else { 1000 };
        let mut rows = (0..count)
            .map(|row| trade(50, index * 1000 + row))
            .collect::<Vec<_>>();
        if index == 5 {
            rows[0] = rows[0].replace(
                "\"outcome_index\":0,\"outcome\":\"Yes\"",
                "\"outcome_index\":999,\"outcome\":\"\"",
            );
        }
        let cursor = (index != 5).then(|| format!("page-{}", index + 1));
        let previous = (index != 0).then(|| format!("page-{index}"));
        pages.push((
            request.url(BASE, previous.as_deref()).unwrap(),
            envelope(&rows, index * 1000, cursor.as_deref()),
        ));
    }
    let read = fetch_complete_activity_v2(&source(&pages), BASE, &request)
        .await
        .unwrap();
    assert_eq!(read.rows.len(), 5499);
    assert_eq!(read.anomalies.len(), 1);
    assert_eq!(read.pages.len(), 6);
    assert_eq!(read.pages.iter().map(|p| p.row_count).sum::<u32>(), 5500);
    assert_eq!(read.pages.last().unwrap().offset, 5000);
    assert_eq!(read.pages.last().unwrap().has_more, Some(false));
    assert_eq!(
        reauthenticate_activity_v2(BASE, &request, &raw_pages(&pages), &read.pages).unwrap(),
        read
    );
}

#[test]
fn v2_reauthentication_rejects_forged_evidence_and_broken_cursor_chains() {
    let pages = small_walk();
    let proofs = evidence(&pages, &request());
    assert!(reauthenticate_activity_v2(BASE, &request(), &raw_pages(&pages), &proofs).is_ok());
    for field in [
        "terminal",
        "cursor",
        "offset",
        "rows",
        "canonical",
        "raw",
        "anomaly",
        "schema",
        "parser",
        "partition",
        "bounds",
        "url",
    ] {
        let mut forged = proofs.clone();
        match field {
            "terminal" => {
                forged[0].has_more = Some(false);
                forged[0].next_cursor = None;
            }
            "cursor" => forged[0].next_cursor = Some("substituted".to_owned()),
            "offset" => forged[0].offset += 1,
            "rows" => forged[0].row_count += 1,
            "canonical" => forged[0].canonical_page_hash = "forged".to_owned(),
            "raw" => forged[0].raw_page_hash = "forged".to_owned(),
            "anomaly" => {
                let raw = envelope(&[trade(30, 0).replace("0.5,", "1.5,")], 0, Some("first+/="));
                forged[0].anomalies =
                    parse_activity_page_v2(&raw, &pages[0].0, &request(), &context())
                        .unwrap()
                        .anomalies;
            }
            "schema" => forged[0].schema_version = 2,
            "parser" => forged[0].parser_version = 1,
            "partition" => forged[0].partition = Some(PositionPartition::Redeemable),
            "bounds" => forged[0].bounds = None,
            "url" => forged[0].request_url.push_str("&extra=1"),
            _ => unreachable!(),
        }
        assert!(
            matches!(
                reauthenticate_activity_v2(BASE, &request(), &raw_pages(&pages), &forged),
                Err(ActivityReadError::V2Authentication(
                    V2AuthenticationError::EvidenceMismatch { .. }
                ))
            ),
            "accepted {field}"
        );
    }
    for changed in [
        vec![pages[0].clone(), pages[2].clone()],
        vec![pages[1].clone(), pages[0].clone(), pages[2].clone()],
        vec![pages[0].clone(), pages[1].clone()],
        vec![
            pages[0].clone(),
            pages[1].clone(),
            pages[2].clone(),
            pages[2].clone(),
        ],
    ] {
        let proofs = evidence(&changed, &request());
        assert!(
            reauthenticate_activity_v2(BASE, &request(), &raw_pages(&changed), &proofs).is_err()
        );
    }
    let mut substituted = pages.clone();
    substituted[1].0 = request().url(BASE, Some("substituted")).unwrap();
    assert!(matches!(
        reauthenticate_activity_v2(
            BASE,
            &request(),
            &raw_pages(&substituted),
            &evidence(&substituted, &request())
        ),
        Err(ActivityReadError::V2Authentication(
            V2AuthenticationError::RequestUrlMismatch { index: 1 }
        ))
    ));
    let mut cycle = pages.clone();
    cycle[1].1 = envelope(&[trade(20, 1)], 1, Some("first+/="));
    assert!(matches!(
        reauthenticate_activity_v2(
            BASE,
            &request(),
            &raw_pages(&cycle),
            &evidence(&cycle, &request())
        ),
        Err(ActivityReadError::V2Authentication(
            V2AuthenticationError::RepeatedCursor(_)
        ))
    ));
    for (more, cursor) in [(true, "null"), (false, "\"tail\"")] {
        let mut contradictory = pages.clone();
        contradictory[2].1 = format!(r#"{{"data":[],"pagination":{{"limit":1000,"offset":2,"has_more":{more},"next_cursor":{cursor}}}}}"#).into_bytes();
        assert!(matches!(
            reauthenticate_activity_v2(BASE, &request(), &raw_pages(&contradictory), &proofs),
            Err(ActivityReadError::V2Envelope(
                V2PageError::ContradictoryPagination { .. }
            ))
        ));
    }
    let mut mixed = pages.clone();
    mixed[1].1 = format!("[{}]", trade(20, 1)).into_bytes();
    assert!(reauthenticate_activity_v2(BASE, &request(), &raw_pages(&mixed), &proofs).is_err());
    assert!(reauthenticate_activity_v2(BASE, &request(), &[], &[]).is_err());
    assert!(
        reauthenticate_activity_v2(BASE, &request(), &raw_pages(&pages), &proofs[..2]).is_err()
    );
}

#[test]
fn v2_reauthentication_rejects_rows_and_anomalies_outside_bounds() {
    let pages = small_walk();
    let proofs = evidence(&pages, &request());
    for anomaly in [false, true] {
        let row = if anomaly {
            trade(1_800_000_001, 0).replace("0.5,", "1.5,")
        } else {
            trade(1_800_000_001, 0)
        };
        let mut outside = pages.clone();
        outside[0].1 = envelope(&[row], 0, Some("first+/="));
        assert!(matches!(
            reauthenticate_activity_v2(BASE, &request(), &raw_pages(&outside), &proofs),
            Err(ActivityReadError::RowOutsideBounds { .. })
        ));
    }
    let bounded = V2ActivityRequest {
        bounds: ActivityRequestBounds {
            start: Some(10),
            end: 100,
        },
        ..request()
    };
    let raw = envelope(&[trade(10, 0)], 0, None);
    assert!(matches!(
        parse_activity_page_v2(&raw, "url", &bounded, &context()),
        Err(ActivityReadError::RowOutsideBounds { .. })
    ));
}

fn position_mapping() -> ActivityAssetMapping {
    let mut mapping = ActivityAssetMapping::from_rows(&[]);
    for row in decode_v2_page(POSITIONS).unwrap().data {
        #[derive(serde::Deserialize)]
        struct Identity {
            token_id: String,
            condition_id: String,
            outcome_index: u16,
        }
        let row: Identity = serde_json::from_str(row.get()).unwrap();
        mapping
            .insert_verified_ordinary(
                PolymarketTokenId(row.token_id),
                PolymarketConditionId(row.condition_id),
                OutcomeId(row.outcome_index),
            )
            .unwrap();
    }
    mapping
}

fn position_rows() -> Vec<String> {
    decode_v2_page(POSITIONS)
        .unwrap()
        .data
        .iter()
        .map(|row| row.get().to_owned())
        .collect()
}

#[tokio::test]
async fn v2_positions_open_walk_uses_current_size_and_row_flags_with_v1_hash_parity() {
    let mapping = position_mapping();
    let mut rows = position_rows();
    rows[0] = rows[0]
        .replace("\"current_size\":5.0", "\"current_size\":4.000001")
        .replace("\"total_size\":5.0", "\"total_size\":123456789.987654");
    rows[1] = rows[1]
        .replace("\"redeemable\":true", "\"redeemable\":false")
        .replace("\"status\":\"REDEEMABLE\"", "\"status\":\"OPEN\"")
        .replace("\"negative_risk\":false", "\"negative_risk\":true");
    let pages = vec![
        (
            positions_v2_url(BASE, wallet(), None),
            envelope(&rows[..1], 0, Some("positions+/=")),
        ),
        (
            positions_v2_url(BASE, wallet(), Some("positions+/=")),
            envelope(&rows[1..], 1, None),
        ),
    ];
    let read = fetch_complete_positions_v2(&source(&pages), BASE, wallet(), &mapping)
        .await
        .unwrap();
    assert_eq!(read.positions.len(), 3);
    assert_eq!(read.positions[0].size.atomic(), 4_000_001);
    assert!(read.positions[0].redeemable);
    assert!(!read.positions[0].neg_risk);
    assert!(!read.positions[1].redeemable);
    assert!(read.positions[1].neg_risk);
    assert!(
        read.pages
            .iter()
            .all(|p| p.schema_version == 2 && p.parser_version == 2 && p.partition.is_none())
    );
    assert_eq!(
        reauthenticate_positions_v2(BASE, wallet(), &mapping, &raw_pages(&pages), &read.pages)
            .unwrap(),
        read
    );

    let legacy_rows = rows
        .iter()
        .map(|row| {
            row.replace("proxy_wallet", "proxyWallet")
                .replace("token_id", "asset")
                .replace("condition_id", "conditionId")
                .replace("outcome_index", "outcomeIndex")
                .replace("current_size", "size")
                .replace("negative_risk", "negativeRisk")
        })
        .collect::<Vec<_>>();
    let legacy = FakeSource {
        pages: [
            (
                PolymarketEndpoint::CurrentPositionsReconciliationPage {
                    user: WALLET.to_owned(),
                    partition: PositionPartition::NotRedeemable,
                    offset: 0,
                }
                .url(BASE),
                format!("[{}]", legacy_rows[1]).into_bytes(),
            ),
            (
                PolymarketEndpoint::CurrentPositionsReconciliationPage {
                    user: WALLET.to_owned(),
                    partition: PositionPartition::Redeemable,
                    offset: 0,
                }
                .url(BASE),
                format!("[{},{}]", legacy_rows[0], legacy_rows[2]).into_bytes(),
            ),
        ]
        .into(),
        ..FakeSource::default()
    };
    let v1 = fetch_complete_positions(&legacy, BASE, wallet(), &mapping)
        .await
        .unwrap();
    assert_eq!(read.positions, v1.positions);
    assert_eq!(read.semantic_hash(), v1.semantic_hash());

    rows.reverse();
    let same = vec![(
        positions_v2_url(BASE, wallet(), None),
        envelope(&rows, 55, None),
    )];
    let same = fetch_complete_positions_v2(&source(&same), BASE, wallet(), &mapping)
        .await
        .unwrap();
    assert_eq!(same.semantic_hash(), read.semantic_hash());
    let changed = rows
        .iter()
        .map(|r| r.replace("\"negative_risk\":false", "\"negative_risk\":true"))
        .collect::<Vec<_>>();
    let changed = vec![(
        positions_v2_url(BASE, wallet(), None),
        envelope(&changed, 0, None),
    )];
    assert_ne!(
        fetch_complete_positions_v2(&source(&changed), BASE, wallet(), &mapping)
            .await
            .unwrap()
            .semantic_hash(),
        read.semantic_hash()
    );
    let mut total_ignored = rows.clone();
    total_ignored[2] = total_ignored[2].replace("123456789.987654", "\"not a size\"");
    let ignored = vec![(
        positions_v2_url(BASE, wallet(), None),
        envelope(&total_ignored, 0, None),
    )];
    assert_eq!(
        fetch_complete_positions_v2(&source(&ignored), BASE, wallet(), &mapping)
            .await
            .unwrap()
            .semantic_hash(),
        read.semantic_hash()
    );
}

#[tokio::test]
async fn v2_positions_fail_closed_on_missing_flags_wallet_duplicates_and_identity() {
    let rows = position_rows();
    let mapping = position_mapping();
    for (field, text) in [("negative_risk", "false"), ("redeemable", "true")] {
        for replacement in [String::new(), format!("\"{field}\":null,")] {
            let bad = rows[0].replace(&format!("\"{field}\":{text},"), &replacement);
            let api = source(&[(
                positions_v2_url(BASE, wallet(), None),
                envelope(&[bad], 0, None),
            )]);
            assert!(matches!(
                fetch_complete_positions_v2(&api, BASE, wallet(), &mapping).await,
                Err(PositionReadError::MissingField { .. })
            ));
        }
    }
    let mismatch = rows[0].replace(WALLET, "0x1111111111111111111111111111111111111111");
    let api = source(&[(
        positions_v2_url(BASE, wallet(), None),
        envelope(&[mismatch], 0, None),
    )]);
    assert!(matches!(
        fetch_complete_positions_v2(&api, BASE, wallet(), &mapping).await,
        Err(PositionReadError::WalletMismatch { .. })
    ));
    let duplicate = vec![
        (
            positions_v2_url(BASE, wallet(), None),
            envelope(&rows[..1], 0, Some("next")),
        ),
        (
            positions_v2_url(BASE, wallet(), Some("next")),
            envelope(&rows[..1], 1, None),
        ),
    ];
    assert!(matches!(
        fetch_complete_positions_v2(&source(&duplicate), BASE, wallet(), &mapping).await,
        Err(PositionReadError::DuplicateAsset { .. })
    ));
    let api = source(&[(
        positions_v2_url(BASE, wallet(), None),
        envelope(&rows[..1], 0, None),
    )]);
    assert!(matches!(
        fetch_complete_positions_v2(&api, BASE, wallet(), &ActivityAssetMapping::from_rows(&[]))
            .await,
        Err(PositionReadError::MissingActivityMapping { .. })
    ));
    let activity = parse_activity_page_v2(ACTIVITY_1, "url", &request(), &context()).unwrap();
    let unverified = ActivityAssetMapping::from_rows(&activity.rows);
    assert!(matches!(
        fetch_complete_positions_v2(&api, BASE, wallet(), &unverified).await,
        Err(PositionReadError::MetadataUnresolved { .. })
    ));
    let wrong_identity = rows[0].replace("\"outcome_index\":0", "\"outcome_index\":1");
    let api = source(&[(
        positions_v2_url(BASE, wallet(), None),
        envelope(&[wrong_identity], 0, None),
    )]);
    assert!(matches!(
        fetch_complete_positions_v2(&api, BASE, wallet(), &mapping).await,
        Err(PositionReadError::ConflictingActivityMapping { .. })
    ));
}

#[test]
fn recorded_v2_rows_parse_like_the_page_reader_and_envelope_errors_reject() {
    let page = parse_activity_page_v2(ACTIVITY_1, "url", &request(), &context()).unwrap();
    let window = parse_activity_response_v2_with_acceptance(
        ACTIVITY_1,
        wallet(),
        &context(),
        ActivityRowAcceptance::StrictWithAnomalies,
    )
    .unwrap();
    assert_eq!(window.rows, page.rows);
    assert_eq!(window.anomalies, page.anomalies);
    for bad in [
        &b"[]"[..],
        br#"{"data":null,"pagination":{"limit":1,"offset":0,"has_more":false,"next_cursor":null}}"#,
    ] {
        assert!(matches!(
            parse_activity_response_v2_with_acceptance(
                bad,
                wallet(),
                &context(),
                ActivityRowAcceptance::StrictWithAnomalies,
            ),
            Err(ActivityParseError::Json { .. })
        ));
    }
}

#[test]
fn v1_evidence_serialization_and_receipt_commitment_digest_are_unchanged() {
    // Frozen before v2: use the existing service receipt preimage, including its old evidence.
    let preimage =
        include_bytes!("../../service/tests/fixtures/activity_read_commitment_v1_preimage.json");
    let mut value: serde_json::Value = serde_json::from_slice(preimage).unwrap();
    let legacy = value["pages"][0][1].clone();
    let evidence: ReconciliationPageEvidence = serde_json::from_value(legacy.clone()).unwrap();
    assert_eq!(evidence.has_more, None);
    assert_eq!(evidence.next_cursor, None);
    assert_eq!(
        blake3::hash(&serde_json::to_vec(&evidence).unwrap())
            .to_hex()
            .as_str(),
        "a4a23f79451eeefb4c5134a8a8ea8b43fca6c8ba55ee1179b1dc56d758abb814"
    );
    assert_eq!(serde_json::to_value(&evidence).unwrap(), legacy);
    let canonical = serde_json::to_vec(&legacy).unwrap();
    assert_eq!(
        blake3::hash(&canonical).to_hex().as_str(),
        "0f90d2960a3a24e9a65bfd28c24329763e045dc567ab674338eace7cd17015f6"
    );
    value["pages"][0][1] = serde_json::to_value(&evidence).unwrap();
    let restored = serde_json::to_vec(&value).unwrap();
    assert_eq!(restored, preimage);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"prediction-edge/activity-read-commitment/v1");
    hasher.update(&restored);
    assert_eq!(
        hasher.finalize().to_hex().as_str(),
        "5be087a9985d65f62b48782a32bbf852d63c280b7cc149757effefd9c19a02f0"
    );
}
