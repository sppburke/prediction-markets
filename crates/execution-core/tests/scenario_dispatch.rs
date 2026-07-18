//! Scenario tests for the ordinary paper-only dispatcher boundary.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceId, SourceTimestamp, StrategyId,
    VenueMarketId,
};
use pe_event_log::Writer;
use pe_execution_core::{DispatchResult, ExecutionDispatcher};
use pe_strategy_winner_follow::{ExecutionMode, PaperExecutor};
use pe_venue_core::OrderIntent;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

fn frozen_now() -> SourceTimestamp {
    SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap())
}

fn test_intent() -> OrderIntent {
    OrderIntent {
        strategy_id: StrategyId("winner-follow".into()),
        market_id: MarketId(VenueMarketId("market".into())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        contracts: ContractQty(5),
        limit_price: Price::new(dec!(0.70)).unwrap(),
        validity_seconds: 60,
        idempotency_key: "winner-follow|t1|market|0|buy|1700000000".into(),
    }
}

fn dispatcher(dir: &TempDir) -> ExecutionDispatcher {
    let writer = Writer::open(dir.path().join("paper.log")).unwrap();
    ExecutionDispatcher::paper_only(PaperExecutor::new(
        writer,
        SourceId("paper".into()),
        500,
        100,
    ))
}

#[tokio::test]
async fn paper_and_shadow_route_to_paper_executor() {
    for mode in [ExecutionMode::Paper, ExecutionMode::Shadow] {
        let dir = TempDir::new().unwrap();
        let result = dispatcher(&dir)
            .execute(&test_intent(), mode, frozen_now(), None)
            .await
            .unwrap();
        assert!(matches!(result, DispatchResult::Paper { .. }));
    }
}

#[tokio::test]
async fn ordinary_live_modes_fail_closed_without_constructing_live_components() {
    for mode in [ExecutionMode::LiveTiny, ExecutionMode::Promoted] {
        let dir = TempDir::new().unwrap();
        let error = dispatcher(&dir)
            .execute(&test_intent(), mode, frozen_now(), None)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ordinary credentialed dispatch is retired")
        );
        assert!(!dir.path().join("live.log").exists());
    }
}
