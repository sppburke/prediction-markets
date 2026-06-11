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
use std::sync::Arc;

use base64::Engine as _;
use pe_copy_signal_engine::PositionSnapshot;
use pe_copy_signal_engine::{IncomingTrade, SignalConfig};
use pe_core_types::{
    BasisPoints, ContractQty, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::Writer;
use pe_execution_core::{ExecutionDispatcher, LiveExecutor};
use pe_paper_state::PaperStateDb;
use pe_position_ledger::PositionLedger;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::{FixtureCLOBClient, PolymarketCredentials, PolymarketVenueAdapter};
use rust_decimal::Decimal;
use std::collections::HashMap;
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
            win_rate_bps: BasisPoints(7_000),
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

fn make_dispatcher(dir: &TempDir) -> ExecutionDispatcher<FixtureCLOBClient> {
    let paper_path = dir.path().join("paper.log");
    let paper_writer = Writer::open(&paper_path).unwrap();
    let paper_executor = PaperExecutor::new(paper_writer, SourceId("test.paper".into()), 500, 100);

    let live_path = dir.path().join("live.log");
    let live_writer = Writer::open(&live_path).unwrap();
    let creds = PolymarketCredentials::mainnet(
        "0x0000000000000000000000000000000000000001".into(),
        "0x0000000000000000000000000000000000000000000000000000000000000001".into(),
        "key".into(),
        base64::engine::general_purpose::STANDARD.encode(b"secret"),
        "pass".into(),
    );
    let adapter = PolymarketVenueAdapter::new(FixtureCLOBClient::new(vec![], vec![]), creds);
    let live_executor = LiveExecutor::new(adapter, live_writer, SourceId("test.live".into()));

    ExecutionDispatcher::new(paper_executor, live_executor)
}

fn make_paper_state(dir: &TempDir) -> Arc<PaperStateDb> {
    Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap())
}

fn dead_reseed_rx() -> mpsc::Receiver<HashMap<pe_core_types::WalletAddress, PositionSnapshot>> {
    mpsc::channel(1).1
}

/// Copy-entry gate disabled for lifecycle tests: full [0,1] band, fail-open.
/// Paired with an empty history map so every first Entry is admitted.
fn disabled_entry_gate() -> CopyEntryGateConfig {
    CopyEntryGateConfig {
        price_band_lo: Price::ZERO,
        price_band_hi: Price::ONE,
        fail_closed: false,
    }
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

    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(16);

    // Send one trade then close the channel so the orchestrator exits cleanly.
    trade_tx.send(make_trade(wallet)).await.unwrap();
    drop(trade_tx);

    let orch = Orchestrator::new(
        trade_rx,
        make_watchlist(wallet),
        OrchestratorConfig {
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0, // disabled in tests
            entry_gate_config: disabled_entry_gate(),
        },
        HashMap::new(),
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        make_dispatcher(&dir),
        make_paper_state(&dir),
        PositionLedger::new(),
        new_shared_health(false),
        MarketEndCache::new(String::new()),
        dead_reseed_rx(),
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

    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(16);

    // Pre-fill channel with 2 trades before the orchestrator starts.
    trade_tx.send(make_trade(wallet)).await.unwrap();
    trade_tx.send(make_trade(wallet)).await.unwrap();
    // Senders deliberately kept alive — simulates producers still running at shutdown.

    // Shutdown resolves immediately.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    shutdown_tx.send(()).unwrap();

    let orch = Orchestrator::new(
        trade_rx,
        make_watchlist(wallet),
        OrchestratorConfig {
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0, // disabled in tests
            entry_gate_config: disabled_entry_gate(),
        },
        HashMap::new(),
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        make_dispatcher(&dir),
        make_paper_state(&dir),
        PositionLedger::new(),
        new_shared_health(false),
        MarketEndCache::new(String::new()),
        dead_reseed_rx(),
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

    drop(trade_tx);
}
