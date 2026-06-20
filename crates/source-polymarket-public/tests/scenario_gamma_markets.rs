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

use pe_source_polymarket_public::{FixtureFetcher, GammaMarketsClient, MarketFilter};

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

    assert_eq!(out.len(), 3, "both chunks merged into one map");
    assert_eq!(out["A"].end_date_unix, Some(1_705_276_800));
    assert_eq!(out["B"].end_date_unix, None);
    assert_eq!(out["B"].liquidity.unwrap().to_string(), "12.5");
    assert!(out.contains_key("C"));
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

    assert_eq!(out.len(), 2);
    assert!(out.contains_key("A") && out.contains_key("C"));
    assert!(
        !out.contains_key("B"),
        "unknown id must be absent (caller treats as NULL/skip)"
    );
}

#[tokio::test]
async fn fatal_chunk_is_skipped_other_chunks_land() {
    // batch_size=2 → chunks [A,B] (mapped) and [C,D] (unmapped → FixtureFetcher Fatal). The call must
    // return Ok with A,B; C,D are simply absent (not an error).
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

    assert_eq!(out.len(), 2, "only the mapped chunk's markets land");
    assert!(out.contains_key("A") && out.contains_key("B"));
    assert!(!out.contains_key("C") && !out.contains_key("D"));
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

    assert_eq!(out["A"].condition_id, "A");
    assert_eq!(out["B"].condition_id, "B");
    assert_eq!(out["A"].end_date_unix, Some(1_705_276_800));
    assert_eq!(out["B"].end_date_unix, Some(1_706_745_600));
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

    assert_eq!(out.len(), 1, "&closed=true URL must be used");
    assert_eq!(out["A"].end_date_unix, Some(1_705_276_800));
}
