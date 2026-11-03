use super::*;

use pe_core_types::PolymarketTokenId;
use pe_source_polymarket_public::{V2ActivityRequest, positions_v2_url};
use serde_json::{Value, json};

#[path = "../../../tests/support/data_api_v2.rs"]
mod fake;

const WALLET: &str = "0x1111111111111111111111111111111111111111";

fn query(url: &str) -> HashMap<String, String> {
    reqwest::Url::parse(url)
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

pub(super) fn positions_v2_page(rows: Vec<Value>) -> Value {
    let url = positions_v2_url(
        "https://offline.invalid",
        WalletAddress::from_hex(WALLET).unwrap(),
        None,
    );
    fake::DataApiV2::default()
        .reply("/v2/positions", &rows, &query(&url), None)
        .unwrap()
}

fn mapping() -> ActivityAssetMapping {
    let mut mapping = ActivityAssetMapping::from_rows(&[]);
    for (token, outcome) in [("123", 0), ("124", 1)] {
        mapping
            .insert_verified_ordinary(
                PolymarketTokenId(token.to_owned()),
                finality_prepared().economic.market.condition_id.clone(),
                OutcomeId(outcome),
            )
            .unwrap();
    }
    mapping
}

fn rows() -> Vec<Value> {
    vec![
        json!({
            "proxyWallet": WALLET, "asset": "123",
            "conditionId": finality_prepared().economic.market.condition_id.0,
            "outcomeIndex": 0, "size": "0.000001", "redeemable": false,
            "negativeRisk": false,
        }),
        json!({
            "proxyWallet": WALLET, "asset": "124",
            "conditionId": finality_prepared().economic.market.condition_id.0,
            "outcomeIndex": 1, "size": "3.125", "redeemable": true,
            "negativeRisk": false,
        }),
    ]
}

fn replay_both(
    evidence: &LivePositionEvidenceAudit,
    sources: &[EventEnvelope],
    binding: &LiveAccountBindingAudit,
) -> [Result<Vec<CanonicalPositionAudit>, ProjectionReducerError>; 2] {
    let mapping = mapping();
    [
        replay_position_evidence(evidence, sources, binding, Some(&mapping)),
        replay_position_receipts_legacy(
            &evidence
                .pages
                .iter()
                .map(|page| page.receipt)
                .collect::<Vec<_>>(),
            sources,
            WALLET,
            binding,
            Some(&mapping),
        ),
    ]
}

/// The recorder retains both cursor bodies and binds their ordered queries and page indices;
/// both replay paths recover current holdings, including dust and redeemable positions.
#[tokio::test]
async fn two_page_parser_two_inventory_records_and_replays() {
    const CURSOR: &str = "next/+? =:%~*";
    async fn positions(
        State(state): State<Arc<Mutex<fake::DataApiV2>>>,
        Query(mut query): Query<HashMap<String, String>>,
    ) -> Result<Json<Value>, StatusCode> {
        if let Some(cursor) = query.get_mut("cursor") {
            assert_eq!(cursor, CURSOR);
            *cursor = "fake-v2-0".to_owned();
        }
        let mut body = state
            .lock()
            .unwrap()
            .reply("/v2/positions", &rows(), &query, Some(1))?;
        if body["pagination"]["has_more"] == true {
            body["pagination"]["next_cursor"] = CURSOR.into();
        }
        Ok(Json(body))
    }
    let app = Router::new()
        .route("/v2/positions", get(positions))
        .with_state(Arc::new(Mutex::new(fake::DataApiV2::default())));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let dir = tempdir().unwrap();
    let db = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let mut state = fanout_state(&dir, db, armed_snapshot("acct"), &base, None);
    let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    state.clock = Arc::new(move || now);
    state.config.data_base_url = base;
    let binding = account_binding_fixture(&AccountId::new("acct").unwrap(), WALLET);
    let inventory = fetch_complete_venue_inventory(&state, WALLET, &binding, Some(&mapping()))
        .await
        .unwrap();
    server.abort();
    assert_eq!(inventory.evidence.pages.len(), 2);
    assert_eq!(inventory.positions[0].size.atomic(), 1);
    assert!(!inventory.positions[0].redeemable);
    assert_eq!(inventory.positions[1].size.to_decimal(), dec!(3.125));
    assert!(inventory.positions[1].redeemable);
    assert_eq!(inventory.redeemable.len(), 1);
    assert_eq!(inventory.redeemable[0].size, dec!(3.125));
    let sources = inventory
        .evidence
        .pages
        .iter()
        .map(|page| {
            state
                .config
                .source_receipts
                .source_envelope(page.receipt)
                .unwrap()
        })
        .collect::<Vec<_>>();
    for (index, source) in sources.iter().enumerate() {
        assert_eq!(source.source_id.0, COMPLETE_POSITIONS_SOURCE_ID);
        assert_eq!(source.schema_version, 2);
        assert_eq!(source.parser_version, 2);
        assert_eq!(source.received_at.0, now);
        let retained: RetainedPositionResponse = serde_json::from_slice(&source.payload).unwrap();
        assert_eq!(retained.request.path, "/v2/positions");
        assert_eq!(retained.request.partition, "status=OPEN");
        assert_eq!(retained.request.offset, u64::try_from(index).unwrap());
        assert_eq!(
            retained.request,
            position_request_descriptor(
                &binding,
                &inventory.evidence.pages[index].request_identity,
                2,
                u64::try_from(index).unwrap(),
            )
            .unwrap()
        );
        let page = pe_source_polymarket_public::decode_v2_page(&retained.body).unwrap();
        assert_eq!(page.pagination.has_more, index == 0);
        assert_eq!(page.data.len(), 1);
    }
    assert!(
        inventory.evidence.pages[0]
            .request_identity
            .ends_with("&limit=1000")
    );
    assert!(
        inventory.evidence.pages[1]
            .request_identity
            .ends_with("&cursor=next%2F%2B%3F%20%3D%3A%25~%2A")
    );
    let [current, legacy] = replay_both(&inventory.evidence, &sources, &binding);
    assert_eq!(current.unwrap(), inventory.positions);
    assert_eq!(legacy.unwrap(), inventory.positions);

    let mut truncated = inventory.evidence.clone();
    truncated.pages.pop();
    let [current, legacy] = replay_both(&truncated, &sources, &binding);
    assert!(matches!(
        current,
        Err(ProjectionReducerError::InvalidPositionEvidence)
    ));
    assert!(matches!(
        legacy,
        Err(ProjectionReducerError::InvalidPositionEvidence)
    ));

    let mut mixed = inventory.evidence.clone();
    let (v1, v1_sources) = position_evidence(100, now, &binding, [b"[]".to_vec(), b"[]".to_vec()]);
    mixed.pages[1] = v1.pages[0].clone();
    let mut mixed_sources = sources.clone();
    mixed_sources.extend(v1_sources);
    let [current, legacy] = replay_both(&mixed, &mixed_sources, &binding);
    assert!(matches!(
        current,
        Err(ProjectionReducerError::InvalidPositionEvidence)
    ));
    assert!(matches!(
        legacy,
        Err(ProjectionReducerError::InvalidPositionEvidence)
    ));
}

#[test]
fn retained_parser_one_inventory_replays_unchanged() {
    let binding = account_binding_fixture(&AccountId::new("acct").unwrap(), WALLET);
    let mut rows = rows();
    let redeemable = rows.pop().unwrap();
    let (evidence, sources) = position_evidence(
        10,
        OffsetDateTime::UNIX_EPOCH,
        &binding,
        [
            serde_json::to_vec(&rows).unwrap(),
            serde_json::to_vec(&[redeemable]).unwrap(),
        ],
    );
    assert!(sources.iter().all(|source| source.parser_version == 1));
    let [current, legacy] = replay_both(&evidence, &sources, &binding);
    let current = current.unwrap();
    assert_eq!(legacy.unwrap(), current);
    assert_eq!(current.len(), 2);
    assert_eq!(current[0].size.atomic(), 1);
    assert_eq!(current[1].size.to_decimal(), dec!(3.125));
    assert!(current[1].redeemable);
}

#[test]
fn observation_index_uses_v2_page_evidence_for_both_envelope_schemas() {
    let (prepared, _, recorded, _) = observation_replay_fixture_for(false);
    let admission = approved_admission(&prepared);
    let receipt = admission
        .economic
        .observation
        .as_ref()
        .unwrap()
        .complete_bound_receipt;
    let bound = recorded
        .iter()
        .find(|(candidate, _)| *candidate == receipt)
        .unwrap()
        .1
        .clone();
    let wallet = WalletAddress::from_hex(
        &admission
            .identity
            .fill_projection
            .as_ref()
            .unwrap()
            .leader_wallet,
    )
    .unwrap();
    let request = V2ActivityRequest::position_changes(
        wallet,
        Some(17),
        18,
        pe_source_polymarket_public::ActivityRowAcceptance::StrictWithAnomalies,
    );
    let url = request
        .url("https://data-api.polymarket.com", None)
        .unwrap();
    let rows: Vec<Value> = serde_json::from_slice(&bound.payload).unwrap();
    let body = fake::DataApiV2::default()
        .reply("/v2/activity", &rows, &query(&url), None)
        .unwrap();
    for schema in [
        crate::trade_poller::ACTIVITY_HISTORICAL_V2_PAGE_SCHEMA_VERSION,
        crate::trade_poller::ACTIVITY_POLL_V2_PAGE_SCHEMA_VERSION,
    ] {
        let mut source = source_envelope(
            receipt,
            &bound.source_id,
            serde_json::to_vec(&body).unwrap(),
            bound.received_at.0,
        );
        source.schema_version = schema;
        source.parser_version = pe_source_polymarket_public::ACTIVITY_PARSER_VERSION;
        let pages = produced_observation_index(&admission, std::slice::from_ref(&source)).unwrap();
        let (continuation, correction) = pages.values().next().unwrap();
        assert!(correction.is_none());
        assert_eq!(continuation.page_occurrences[0].request_url, url);
        let proof = &continuation.facts.decision_inputs["pages"][0];
        assert_eq!(
            proof["schema_version"],
            pe_source_polymarket_public::ACTIVITY_V2_PAGE_SCHEMA_VERSION
        );
        assert_eq!(proof["request_url"], url);
        assert_eq!(proof["has_more"], false);
        assert!(proof["next_cursor"].is_null());
        assert_eq!(
            proof["raw_page_hash"],
            blake3::hash(&source.payload).to_hex().to_string()
        );
    }
}
