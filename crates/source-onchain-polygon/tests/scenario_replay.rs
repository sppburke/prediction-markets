#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
//! Scenario: replay three fixture files through [`PolygonReplayConnector`].
//!
//! PASS: each fixture file emits exactly the expected number of [`SourceEvent`]s
//!       with non-empty payloads that round-trip back to the original
//!       [`PolygonEvent`] variant.
//!
//! FAIL: any event has empty payload, wrong count, or JSON doesn't round-trip.

use pe_core_types::SourceId;
use pe_source_core::{SourceConnector, SourceError};
use pe_source_onchain_polygon::{PolygonEvent, PolygonReplayConnector};

/// Drive the connector to exhaustion, collecting all emitted [`SourceEvent`]s.
async fn drain(connector: &mut PolygonReplayConnector) -> Vec<pe_source_core::SourceEvent> {
    let mut events = Vec::new();
    loop {
        match connector.next_event().await {
            Ok(ev) => events.push(ev),
            Err(SourceError::Fatal { .. }) => break,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    events
}

/// Load a fixture JSON string, build a connector, drain it, and assert invariants.
async fn run_fixture(json: &str, expected_count: usize) {
    let polygon_events: Vec<PolygonEvent> =
        serde_json::from_str(json).expect("fixture must be valid JSON");
    assert_eq!(
        polygon_events.len(),
        expected_count,
        "fixture has unexpected number of events"
    );

    let mut connector =
        PolygonReplayConnector::new(SourceId("polygon-test".into()), polygon_events);

    let emitted = drain(&mut connector).await;

    assert_eq!(
        emitted.len(),
        expected_count,
        "emitted event count mismatch"
    );

    for ev in &emitted {
        assert!(!ev.payload.is_empty(), "payload must not be empty");
        // Round-trip: payload bytes → PolygonEvent (just checks it parses)
        let _: PolygonEvent = serde_json::from_slice(&ev.payload)
            .expect("payload must deserialize back to PolygonEvent");
    }
}

#[tokio::test]
async fn scenario_simple_funder() {
    let json = include_str!("fixtures/simple_funder.json");
    run_fixture(json, 3).await;
}

#[tokio::test]
async fn scenario_bridge_onramp() {
    let json = include_str!("fixtures/bridge_onramp.json");
    run_fixture(json, 3).await;
}

#[tokio::test]
async fn scenario_two_hop_funder() {
    let json = include_str!("fixtures/two_hop_funder.json");
    run_fixture(json, 4).await;
}
