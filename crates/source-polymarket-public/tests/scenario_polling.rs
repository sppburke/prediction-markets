//! Scenario test: FixtureFetcher drives all 4 endpoint shapes.
//!
//! PASS: connector emits exactly 4 `SourceEvent`s (one per endpoint, one rotation),
//!       each with non-empty payload matching the fixture bytes.
//! FAIL: wrong count, empty payload, or payload mismatch.

#![cfg(feature = "scenario")]
#![allow(clippy::panic, clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_core_types::SourceId;
use pe_source_core::SourceConnector;
use pe_source_polymarket_public::{
    FixtureFetcher, PollingConfig, PolymarketEndpoint, PolymarketPublicConnector,
};

const BASE: &str = "https://data-api.polymarket.com";
const USER: &str = "0x1234abcd";

fn fixture(name: &str) -> Vec<u8> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("cannot read fixture {path}: {e}"))
}

fn build_connector() -> PolymarketPublicConnector<FixtureFetcher> {
    let endpoints = vec![
        PolymarketEndpoint::Leaderboard,
        PolymarketEndpoint::UserTradeActivity {
            user: USER.into(),
            end: None,
            start: None,
        },
        PolymarketEndpoint::CurrentPositions {
            user: USER.into(),
            limit: None,
            offset: None,
            redeemable: None,
            size_threshold: None,
        },
        PolymarketEndpoint::ClosedPositions { user: USER.into() },
    ];

    let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
    responses.insert(
        PolymarketEndpoint::Leaderboard.url(BASE),
        fixture("leaderboard.json"),
    );
    responses.insert(
        PolymarketEndpoint::UserTradeActivity {
            user: USER.into(),
            end: None,
            start: None,
        }
        .url(BASE),
        fixture("user_trade_activity.json"),
    );
    responses.insert(
        PolymarketEndpoint::CurrentPositions {
            user: USER.into(),
            limit: None,
            offset: None,
            redeemable: None,
            size_threshold: None,
        }
        .url(BASE),
        fixture("current_positions.json"),
    );
    responses.insert(
        PolymarketEndpoint::ClosedPositions { user: USER.into() }.url(BASE),
        fixture("closed_positions.json"),
    );

    let fetcher = FixtureFetcher::new(responses);

    let config = PollingConfig {
        base_url: BASE.into(),
        endpoint_configs: HashMap::new(),
        default_interval_secs: 0,
    };

    PolymarketPublicConnector::new(
        SourceId("polymarket-public".into()),
        config,
        endpoints,
        fetcher,
    )
}

#[tokio::test]
async fn all_four_endpoints_emit_non_empty_events() {
    let mut connector = build_connector();

    let fixture_bytes = [
        fixture("leaderboard.json"),
        fixture("user_trade_activity.json"),
        fixture("current_positions.json"),
        fixture("closed_positions.json"),
    ];

    for (i, expected_bytes) in fixture_bytes.iter().enumerate() {
        let event = connector
            .next_event()
            .await
            .unwrap_or_else(|e| panic!("next_event() #{i} failed: {e}"));

        assert!(!event.payload.is_empty(), "event #{i} payload is empty");
        assert_eq!(
            event.payload, *expected_bytes,
            "event #{i} payload mismatch"
        );
        assert_eq!(event.source_id.0, "polymarket-public");
        assert_eq!(event.schema_version, 1);
        assert_eq!(event.parser_version, 1);
    }
}

#[tokio::test]
async fn health_starts_healthy() {
    use pe_source_core::{SourceConnector, SourceStatus};
    let connector = build_connector();
    assert_eq!(connector.health().status, SourceStatus::Healthy);
    assert!(connector.health().last_event_at.is_none());
}
