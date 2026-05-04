//! Scenario: source fixtures → orchestrator lifecycle verification.
//!
//! Scenarios:
//!   1. e2e_clean_exit    — trade from watchlisted wallet processed; orchestrator exits cleanly
//!      when both channels close.
//!   2. graceful_shutdown — trades buffered before shutdown are drained before orchestrator exits.
//!
//! Note: with the Phase 0B placeholder (p = c = leader_price in evaluate.rs), the strategy always
//! returns NoEdge and no paper fills are written. These tests verify orchestrator lifecycle
//! correctness (no hang, no panic) rather than fill counts. Fill assertions land in Phase 0C when
//! the model engine provides calibrated probabilities.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::str::FromStr;

use pe_copy_signal_engine::{IncomingTrade, SignalConfig};
use pe_core_types::{
    BasisPoints, ContractQty, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::Writer;
use pe_funding_graph::FundingGraphAccumulator;
use pe_service::health::new_shared_health;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_source_core::SourceEvent;
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn wallet_a() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn make_watchlist(wallet: WalletAddress) -> Watchlist {
    let quality = ReconstructionQuality::new(100).unwrap();
    let score = BasisPoints(200);
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet,
            operator_id: None,
            tier: WatchlistTier::Active,
            leader_score_bps: score,
            lcb_5pct_bps: score,
            closed_trades_in_window: 0,
            reconstruction_quality: quality,
        }],
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count: 1,
        incubator_count: 0,
    }
}

fn make_trade(wallet: WalletAddress) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(1_704_067_200).unwrap();
    IncomingTrade {
        wallet,
        market_id: MarketId(VenueMarketId(
            "0x1111111111111111111111111111111111111111".to_string(),
        )),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(Decimal::from_str("0.65").unwrap()),
        contracts: ContractQty(100),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId("trade_a1".to_string()),
    }
}

fn make_paper_executor(dir: &TempDir) -> PaperExecutor {
    let path = dir.path().join("paper.log");
    let writer = Writer::open(&path).unwrap();
    PaperExecutor::new(writer, SourceId("test".into()))
}

// ── Scenario 1: e2e_clean_exit ────────────────────────────────────────────────
//
// PASS: one IncomingTrade from a watchlisted wallet is processed; orchestrator
//       exits cleanly when both channels close (no hang, no panic).
//       paper.log has the 5-byte header written at executor init.
// FAIL: orchestrator hangs (test timeout) or panics.

#[tokio::test]
async fn scenario_e2e_clean_exit() {
    let dir = TempDir::new().unwrap();
    let wallet = wallet_a();

    let (polygon_tx, polygon_rx) = mpsc::channel::<SourceEvent>(16);
    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(16);

    // Send one trade then close both channels so the orchestrator exits cleanly.
    trade_tx.send(make_trade(wallet)).await.unwrap();
    drop(trade_tx);
    drop(polygon_tx);

    let orch = Orchestrator::new(
        polygon_rx,
        trade_rx,
        FundingGraphAccumulator::new(),
        make_watchlist(wallet),
        OrchestratorConfig {
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
        },
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        make_paper_executor(&dir),
        new_shared_health(),
    )
    .unwrap();

    // Runs until both channels are closed (shutdown future never resolves).
    orch.run(std::future::pending::<()>()).await;

    // Assert paper.log was created (executor initialised) and orchestrator exited cleanly.
    let log_path = dir.path().join("paper.log");
    let len = std::fs::metadata(&log_path).unwrap().len();
    assert!(
        len >= 5,
        "paper.log should have at least the 5-byte header; got {len} bytes"
    );
}

// ── Scenario 2: graceful_shutdown ────────────────────────────────────────────
//
// PASS: two trades are buffered in the channel when shutdown fires; both are
//       drained before the orchestrator exits (no hang, no panic).
//       paper.log has the 5-byte header written at executor init.
// FAIL: orchestrator hangs (test timeout) or panics.

#[tokio::test]
async fn scenario_graceful_shutdown() {
    let dir = TempDir::new().unwrap();
    let wallet = wallet_a();

    let (polygon_tx, polygon_rx) = mpsc::channel::<SourceEvent>(16);
    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(16);

    // Pre-fill channel with 2 trades before the orchestrator starts.
    trade_tx.send(make_trade(wallet)).await.unwrap();
    trade_tx.send(make_trade(wallet)).await.unwrap();
    // Senders deliberately kept alive — simulates producers still running at shutdown.

    // Shutdown resolves immediately.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    shutdown_tx.send(()).unwrap();

    let orch = Orchestrator::new(
        polygon_rx,
        trade_rx,
        FundingGraphAccumulator::new(),
        make_watchlist(wallet),
        OrchestratorConfig {
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
        },
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        make_paper_executor(&dir),
        new_shared_health(),
    )
    .unwrap();

    orch.run(async {
        shutdown_rx.await.ok();
    })
    .await;

    // Executor was initialised; orchestrator exited cleanly without hanging.
    let log_path = dir.path().join("paper.log");
    let len = std::fs::metadata(&log_path).unwrap().len();
    assert!(
        len >= 5,
        "paper.log should have at least the 5-byte header; got {len} bytes"
    );

    drop(polygon_tx);
    drop(trade_tx);
}
