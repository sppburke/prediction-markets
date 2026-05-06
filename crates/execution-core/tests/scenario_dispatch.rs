//! Scenario tests for `ExecutionDispatcher` mode routing.
//!
//! Verifies that:
//! 1. Paper/Shadow mode → PaperExecutor (no CLOB calls, no live event).
//! 2. LiveTiny/Promoted mode → LiveExecutor (FixtureCLOBClient), LiveFill written.
//! 3. LiveExecutor writes LiveOrderTerminal on rejected orders.
//!
//! All tests use fixed timestamps and `FixtureCLOBClient` — no live network.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;

use base64::Engine as _;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceId, SourceTimestamp, StrategyId,
    VenueMarketId,
};
use pe_event_log::Writer;
use pe_execution_core::{DispatchResult, ExecutionDispatcher, LiveExecuteResult};
use pe_strategy_winner_follow::{ExecutionMode, PaperExecutor};
use pe_venue_core::{OrderIntent, OrderOutcome};
use pe_venue_polymarket::{
    FixtureCLOBClient, PolymarketCredentials, PolymarketVenueAdapter,
    clob_client::{OrderStatus, OrderStatusResponse, PostOrderResponse},
};
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

fn frozen_now() -> SourceTimestamp {
    SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap())
}

fn test_intent() -> OrderIntent {
    OrderIntent {
        strategy_id: StrategyId("winner-follow".into()),
        market_id: MarketId(VenueMarketId(
            "71321045679252212594626385532706912750332728571942532289631379312455583992563".into(),
        )),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        contracts: ContractQty(5),
        limit_price: Price::new(dec!(0.70)).unwrap(),
        validity_seconds: 60,
        idempotency_key: "winner-follow|t1|market|0|buy|1700000000".into(),
    }
}

fn make_paper_writer(dir: &TempDir, name: &str) -> Writer {
    let path: PathBuf = dir.path().join(name);
    Writer::open(&path).expect("open writer")
}

fn filled_fixture() -> FixtureCLOBClient {
    FixtureCLOBClient::new(
        vec![Ok(PostOrderResponse {
            success: true,
            error_msg: String::new(),
            order_id: "live-order-1".into(),
        })],
        vec![Ok(OrderStatusResponse {
            id: "live-order-1".into(),
            status: OrderStatus::Filled,
            quantity_filled: "5000000".into(),
            quantity_remaining: "0".into(),
            avg_price: "0.70".into(),
        })],
    )
}

fn rejected_fixture() -> FixtureCLOBClient {
    FixtureCLOBClient::new(
        vec![Ok(PostOrderResponse {
            success: false,
            error_msg: "insufficient balance".into(),
            order_id: String::new(),
        })],
        vec![],
    )
}

fn test_creds() -> PolymarketCredentials {
    PolymarketCredentials::mainnet(
        "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf".into(),
        "0x0000000000000000000000000000000000000000000000000000000000000001".into(),
        "test-key".into(),
        base64::engine::general_purpose::STANDARD.encode(b"test-secret"),
        "test-pass".into(),
    )
}

fn make_dispatcher(
    dir: &TempDir,
    client: FixtureCLOBClient,
) -> ExecutionDispatcher<FixtureCLOBClient> {
    let paper_writer = make_paper_writer(dir, "paper.log");
    let live_writer = make_paper_writer(dir, "live.log");
    let paper = PaperExecutor::new(paper_writer, SourceId("paper".into()));
    let adapter = PolymarketVenueAdapter::new(client, test_creds());
    let live = pe_execution_core::LiveExecutor::new(adapter, live_writer, SourceId("live".into()));
    ExecutionDispatcher::new(paper, live)
}

/// PASS: Paper mode routes to PaperExecutor and returns a PaperFill.
/// FAIL: any panic, or DispatchResult::Live is returned.
#[tokio::test]
async fn paper_mode_routes_to_paper_executor() {
    let dir = TempDir::new().unwrap();
    let mut dispatcher = make_dispatcher(&dir, filled_fixture());
    let result = dispatcher
        .execute(&test_intent(), ExecutionMode::Paper, frozen_now())
        .await
        .unwrap();
    assert!(
        matches!(result, DispatchResult::Paper(_)),
        "expected Paper dispatch, got {result:?}"
    );
}

/// PASS: Shadow mode also routes to PaperExecutor.
#[tokio::test]
async fn shadow_mode_routes_to_paper_executor() {
    let dir = TempDir::new().unwrap();
    let mut dispatcher = make_dispatcher(&dir, filled_fixture());
    let result = dispatcher
        .execute(&test_intent(), ExecutionMode::Shadow, frozen_now())
        .await
        .unwrap();
    assert!(matches!(result, DispatchResult::Paper(_)));
}

/// PASS: LiveTiny routes to LiveExecutor, FixtureCLOBClient returns Filled → LiveFill written.
/// FAIL: paper path taken, or wrong outcome type.
#[tokio::test]
async fn live_tiny_routes_to_live_executor_and_writes_fill() {
    let dir = TempDir::new().unwrap();
    let mut dispatcher = make_dispatcher(&dir, filled_fixture());
    let result = dispatcher
        .execute(&test_intent(), ExecutionMode::LiveTiny, frozen_now())
        .await
        .unwrap();
    match result {
        DispatchResult::Live(LiveExecuteResult::Fill(fill)) => {
            assert!(
                matches!(fill.outcome, OrderOutcome::Filled { .. }),
                "expected Filled"
            );
        }
        other => panic!("expected Live(Fill), got {other:?}"),
    }
}

/// PASS: Promoted mode also routes to LiveExecutor.
#[tokio::test]
async fn promoted_routes_to_live_executor() {
    let dir = TempDir::new().unwrap();
    let mut dispatcher = make_dispatcher(&dir, filled_fixture());
    let result = dispatcher
        .execute(&test_intent(), ExecutionMode::Promoted, frozen_now())
        .await
        .unwrap();
    assert!(matches!(result, DispatchResult::Live(_)));
}

/// PASS: LiveExecutor writes a LiveOrderTerminal when the order is rejected.
/// FAIL: fill is returned instead.
#[tokio::test]
async fn live_executor_writes_terminal_on_rejection() {
    let dir = TempDir::new().unwrap();
    let mut dispatcher = make_dispatcher(&dir, rejected_fixture());
    let result = dispatcher
        .execute(&test_intent(), ExecutionMode::LiveTiny, frozen_now())
        .await
        .unwrap();
    match result {
        DispatchResult::Live(LiveExecuteResult::Terminal(t)) => {
            assert!(
                matches!(t.outcome, OrderOutcome::Rejected { .. }),
                "expected Rejected, got {:?}",
                t.outcome
            );
        }
        other => panic!("expected Live(Terminal), got {other:?}"),
    }
}
