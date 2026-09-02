#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: shared batched `GammaMarketsClient` (issue #382).
//!
//! Drives the client against `FixtureFetcher` keyed on the exact repeat-key batch URLs, with no
//! network. Covers the batching mechanics the bootstrap end-to-end tests sit on top of:
//! - chunking at the batch-size boundary;
//! - demux-by-`conditionId` with no cross-market leak;
//! - an id Gamma omits from a 200 batch is absent from the map (the caller's NULL/skip case);
//! - a per-chunk fatal (4xx / unmapped URL) is skipped, not an error — other chunks still land;
//! - `ClosedOnly` appends `&closed=true`.

use std::collections::HashMap;

use pe_core_types::SourceId;
use pe_source_polymarket_public::{
    FixtureFetcher, GAMMA_MARKETS_PARSER_VERSION, GAMMA_MARKETS_SCHEMA_VERSION,
    GAMMA_MARKETS_SOURCE_ID, GammaMarketsClient, GammaMarketsError, MarketFilter,
};

const BASE: &str = "https://g";

/// Replicates `build_batch_url` so the fixture keys match what the client requests exactly.
fn batch_url(ids: &[&str], closed: bool) -> String {
    let mut u = format!("{BASE}/markets?");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            u.push('&');
        }
        u.push_str("condition_ids=");
        u.push_str(id);
    }
    if closed {
        u.push_str("&closed=true");
    }
    u.push_str("&limit=500");
    u
}

fn token_batch_url(ids: &[&str], closed: bool) -> String {
    let mut url = format!("{BASE}/markets?");
    for (index, id) in ids.iter().enumerate() {
        if index > 0 {
            url.push('&');
        }
        url.push_str("clob_token_ids=");
        url.push_str(id);
    }
    url.push_str("&limit=500");
    if closed {
        url.push_str("&closed=true");
    }
    url
}

fn market(id: &str, end_date: Option<&str>, liquidity: Option<&str>) -> String {
    let mut fields = vec![format!(r#""conditionId":"{id}""#)];
    if let Some(e) = end_date {
        fields.push(format!(r#""endDate":"{e}""#));
    }
    if let Some(l) = liquidity {
        fields.push(format!(r#""liquidity":{l}"#));
    }
    format!("{{{}}}", fields.join(","))
}

fn array(markets: &[String]) -> Vec<u8> {
    format!("[{}]", markets.join(",")).into_bytes()
}

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_owned()).collect()
}

fn client(
    responses: HashMap<String, Vec<u8>>,
    batch_size: usize,
) -> GammaMarketsClient<FixtureFetcher> {
    GammaMarketsClient::new(BASE.to_owned(), FixtureFetcher::new(responses))
        .with_batch_size(batch_size)
}

#[tokio::test]
async fn chunks_at_batch_boundary_and_merges() {
    // batch_size=2, three ids → two chunks: [A,B] and [C]. Both must be fetched and merged.
    let mut responses = HashMap::new();
    responses.insert(
        batch_url(&["A", "B"], false),
        array(&[
            market("A", Some("2024-01-15T00:00:00Z"), None),
            market("B", None, Some("12.5")),
        ]),
    );
    responses.insert(
        batch_url(&["C"], false),
        array(&[market("C", Some("2024-02-01T00:00:00Z"), None)]),
    );

    let out = client(responses, 2)
        .fetch_markets(&ids(&["A", "B", "C"]), MarketFilter::OpenOnly)
        .await
        .unwrap();

    assert_eq!(out.markets.len(), 3, "both chunks merged into one map");
    assert_eq!(out.markets["A"].end_date_unix, Some(1_705_276_800));
    assert_eq!(out.markets["B"].end_date_unix, None);
    assert_eq!(out.markets["B"].liquidity.unwrap().to_string(), "12.5");
    assert!(out.markets.contains_key("C"));
    assert!(out.unfetched.is_empty(), "no chunk failed");
}

#[tokio::test]
async fn id_omitted_from_response_is_absent_from_map() {
    // Asked for A,B,C in one batch; Gamma returns only A and C (B unknown). B must be absent.
    let mut responses = HashMap::new();
    responses.insert(
        batch_url(&["A", "B", "C"], false),
        array(&[
            market("A", Some("2024-01-15T00:00:00Z"), None),
            market("C", None, None),
        ]),
    );

    let out = client(responses, 50)
        .fetch_markets(&ids(&["A", "B", "C"]), MarketFilter::OpenOnly)
        .await
        .unwrap();

    assert_eq!(out.markets.len(), 2);
    assert!(out.markets.contains_key("A") && out.markets.contains_key("C"));
    assert!(
        !out.markets.contains_key("B"),
        "unknown id must be absent from the map"
    );
    assert!(
        !out.unfetched.contains(&"B".to_owned()),
        "B is a known-absent 200 omission, NOT an unfetched 4xx — caller writes NULL, not retry"
    );
}

#[tokio::test]
async fn fatal_chunk_is_reported_unfetched_other_chunks_land() {
    // batch_size=2 → chunks [A,B] (mapped) and [C,D] (unmapped → FixtureFetcher Fatal). The call must
    // return Ok with A,B in the map and C,D reported as unfetched (retry-able, NOT a known-absent NULL).
    let mut responses = HashMap::new();
    responses.insert(
        batch_url(&["A", "B"], false),
        array(&[
            market("A", Some("2024-01-15T00:00:00Z"), None),
            market("B", None, None),
        ]),
    );
    // batch_url(["C","D"]) deliberately NOT mapped.

    let out = client(responses, 2)
        .fetch_markets(&ids(&["A", "B", "C", "D"]), MarketFilter::OpenOnly)
        .await
        .expect("a fatal chunk is skipped, not a hard error");

    assert_eq!(out.markets.len(), 2, "only the mapped chunk's markets land");
    assert!(out.markets.contains_key("A") && out.markets.contains_key("B"));
    assert!(!out.markets.contains_key("C") && !out.markets.contains_key("D"));
    let mut unfetched = out.unfetched.clone();
    unfetched.sort();
    assert_eq!(
        unfetched,
        vec!["C".to_owned(), "D".to_owned()],
        "the fatal chunk's ids are reported unfetched so the caller retries them"
    );
}

#[tokio::test]
async fn demux_keys_each_market_by_condition_id() {
    let mut responses = HashMap::new();
    responses.insert(
        batch_url(&["A", "B"], false),
        array(&[
            market("A", Some("2024-01-15T00:00:00Z"), Some("1")),
            market("B", Some("2024-02-01T00:00:00Z"), Some("2")),
        ]),
    );

    let out = client(responses, 50)
        .fetch_markets(&ids(&["A", "B"]), MarketFilter::OpenOnly)
        .await
        .unwrap();

    assert_eq!(out.markets["A"].condition_id, "A");
    assert_eq!(out.markets["B"].condition_id, "B");
    assert_eq!(out.markets["A"].end_date_unix, Some(1_705_276_800));
    assert_eq!(out.markets["B"].end_date_unix, Some(1_706_745_600));
}

#[tokio::test]
async fn closed_filter_uses_closed_true_url() {
    // Map ONLY the &closed=true URL; if the client requested the plain URL it would miss (Fatal) and
    // the market would be absent.
    let mut responses = HashMap::new();
    responses.insert(
        batch_url(&["A"], true),
        array(&[market("A", Some("2024-01-15T00:00:00Z"), None)]),
    );

    let out = client(responses, 50)
        .fetch_markets(&ids(&["A"]), MarketFilter::ClosedOnly)
        .await
        .unwrap();

    assert_eq!(out.markets.len(), 1, "&closed=true URL must be used");
    assert_eq!(out.markets["A"].end_date_unix, Some(1_705_276_800));
}

#[tokio::test]
async fn token_lookup_deduplicates_one_request_and_returns_raw_evidence() {
    let first_raw = br#"[
      {"conditionId":"condition-a","clobTokenIds":["A","B"]}
    ]"#
    .to_vec();
    let mut responses = HashMap::new();
    responses.insert(token_batch_url(&["A", "B"], true), first_raw.clone());

    let out = client(responses, 2)
        .fetch_markets_by_token_ids(&ids(&["A", "A", "B"]), MarketFilter::ClosedOnly)
        .await
        .unwrap();

    assert_eq!(out.markets.markets.len(), 1);
    let (page, raw) = out.page.unwrap();
    assert_eq!(page.request_url, token_batch_url(&["A", "B"], true));
    assert_eq!(raw, first_raw);
    let value: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    let canonical = serde_json::to_vec(&value).unwrap();
    assert_eq!(page.raw_page_hash, blake3::hash(&raw).to_hex().to_string());
    assert_eq!(
        page.canonical_page_hash,
        blake3::hash(&canonical).to_hex().to_string()
    );
    assert_eq!(page.source_id, SourceId(GAMMA_MARKETS_SOURCE_ID.to_owned()));
    assert_eq!(page.schema_version, GAMMA_MARKETS_SCHEMA_VERSION);
    assert_eq!(page.parser_version, GAMMA_MARKETS_PARSER_VERSION);
}

#[tokio::test]
async fn token_lookup_rejects_more_than_one_request_batch() {
    let result = client(HashMap::new(), 2)
        .fetch_markets_by_token_ids(&ids(&["A", "B", "C"]), MarketFilter::OpenOnly)
        .await;
    assert!(matches!(
        result,
        Err(error) if matches!(
            error.source,
            GammaMarketsError::TooManyTokenIds {
                tokens: 3,
                limit: 2
            }
        ) && error.page.is_none()
    ));
}

#[tokio::test]
async fn token_lookup_rejects_comma_joined_input() {
    let result = client(HashMap::new(), 50)
        .fetch_markets_by_token_ids(&ids(&["A,B"]), MarketFilter::OpenOnly)
        .await;
    assert!(matches!(
        result,
        Err(error) if matches!(
            &error.source,
            GammaMarketsError::InvalidTokenId { token } if token == "A,B"
        ) && error.page.is_none()
    ));
}

#[tokio::test]
async fn malformed_token_lookup_returns_parse_error_with_raw_page_evidence() {
    for raw in [b"not-json".to_vec(), br#"{"not":"an array"}"#.to_vec()] {
        let mut responses = HashMap::new();
        responses.insert(token_batch_url(&["A"], false), raw.clone());

        let error = client(responses, 50)
            .fetch_markets_by_token_ids(&ids(&["A"]), MarketFilter::OpenOnly)
            .await
            .err()
            .expect("malformed response must fail");

        assert!(matches!(error.source, GammaMarketsError::Parse(_)));
        let (evidence, recorded) = error.page.expect("successful transport retains its page");
        assert_eq!(recorded, raw);
        assert_eq!(
            evidence.raw_page_hash,
            blake3::hash(&recorded).to_hex().to_string()
        );
        assert_eq!(evidence.source_id.0, GAMMA_MARKETS_SOURCE_ID);
        assert_eq!(evidence.schema_version, GAMMA_MARKETS_SCHEMA_VERSION);
        assert_eq!(evidence.parser_version, GAMMA_MARKETS_PARSER_VERSION);
    }
}
