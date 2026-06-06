//! Scenario tests for the copy-entry gate (issue #290) end-to-end through the
//! orchestrator: a leader trade flows `handle_trade` → `CopyEntryGate::admit` →
//! strategy → dispatcher, and we assert on the resulting paper-fill count.
//!
//! A fill is produced only when the gate admits the signal AND the flat-sizing
//! strategy path fills; the contrast against an otherwise-identical admitted case
//! isolates the gate as the cause of a 0-fill outcome.
//!
//! Determinism: fixed wallet/market/timestamps, no clocks, no network. Each
//! scenario prints a PASS line.
//!
//! Run with: cargo nextest run -p pe-service --features scenario

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use base64::Engine as _;
use pe_copy_signal_engine::PositionSnapshot;
use pe_copy_signal_engine::{IncomingTrade, SignalConfig};
use pe_core_types::{
    BasisPoints, ContractQty, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{Reader, Writer};
use pe_execution_core::{ExecutionDispatcher, LiveExecutor};
use pe_funding_graph::FundingGraphAccumulator;
use pe_operator_graph::OperatorIdentity;
use pe_paper_state::PaperStateDb;
use pe_position_ledger::PositionLedger;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_source_core::SourceEvent;
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::{FixtureCLOBClient, PolymarketCredentials, PolymarketVenueAdapter};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::{mpsc, watch};

// ── Helpers ───────────────────────────────────────────────────────────────────

fn leader_wallet() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn market(hex: &str) -> MarketId {
    MarketId(VenueMarketId(hex.to_string()))
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

/// A BUY (Entry from a fresh ledger) into `market_id` at `price`, 100 contracts.
fn entry_trade(id: &str, market_id: MarketId, price: Decimal) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    IncomingTrade {
        wallet: leader_wallet(),
        market_id,
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(price),
        contracts: ContractQty(100),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(id.to_string()),
    }
}

/// Strategy config that deterministically fills via the flat sizing path.
fn flat_fill_config() -> WinnerFollowConfig {
    WinnerFollowConfig {
        flat_usd_per_trade: Some(dec!(100)),
        ..WinnerFollowConfig::default()
    }
}

/// Gate with the cohort band [0.40, 0.80] and the given fail-closed posture.
fn band_config(fail_closed: bool) -> CopyEntryGateConfig {
    CopyEntryGateConfig {
        price_band_lo: Price(dec!(0.40)),
        price_band_hi: Price(dec!(0.80)),
        fail_closed,
    }
}

fn make_dispatcher(dir: &TempDir) -> ExecutionDispatcher<FixtureCLOBClient> {
    let paper_writer = Writer::open(dir.path().join("paper.log")).unwrap();
    let paper_executor = PaperExecutor::new(paper_writer, SourceId("test.paper".into()), 500, 100);
    let live_writer = Writer::open(dir.path().join("live.log")).unwrap();
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

fn empty_operator_rx() -> watch::Receiver<Vec<OperatorIdentity>> {
    watch::channel(Vec::new()).1
}

fn dead_reseed_rx() -> mpsc::Receiver<HashMap<WalletAddress, PositionSnapshot>> {
    mpsc::channel(1).1
}

fn paper_fill_count(dir: &TempDir) -> usize {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return 0;
    }
    Reader::replay(&path).unwrap().count()
}

/// Feed `trades` through a fresh orchestrator wired with `gate_config` + `history`
/// and a flat-fill strategy in Paper mode; return the resulting paper-fill count.
async fn run_gate(
    dir: &TempDir,
    gate_config: CopyEntryGateConfig,
    history: HashMap<WalletAddress, HashSet<MarketId>>,
    trades: Vec<IncomingTrade>,
) -> usize {
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    let (polygon_tx, polygon_rx) = mpsc::channel::<SourceEvent>(16);
    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(64);
    for t in trades {
        trade_tx.send(t).await.unwrap();
    }
    drop(trade_tx);
    drop(polygon_tx);

    let orch = Orchestrator::new(
        polygon_rx,
        trade_rx,
        Arc::new(Mutex::new(FundingGraphAccumulator::new())),
        empty_operator_rx(),
        make_watchlist(leader_wallet()),
        OrchestratorConfig {
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            cluster_observation_window_secs: 300,
            max_resolution_horizon_secs: 0, // horizon gate disabled; isolate the entry gate
            entry_gate_config: gate_config,
        },
        history,
        WinnerFollowStrategy::new(flat_fill_config()),
        make_dispatcher(dir),
        paper_state,
        PositionLedger::new(),
        new_shared_health(true),
        MarketEndCache::new(String::new()),
        dead_reseed_rx(),
    )
    .unwrap();
    orch.run(std::future::pending::<()>()).await;

    paper_fill_count(dir)
}

fn history_with(
    wallet: WalletAddress,
    markets: &[MarketId],
) -> HashMap<WalletAddress, HashSet<MarketId>> {
    let mut map = HashMap::new();
    map.insert(wallet, markets.iter().cloned().collect());
    map
}

// ── Price-band scenarios ──────────────────────────────────────────────────────

/// PASS: an in-band first entry (price 0.60 ∈ [0.40, 0.80]) produces exactly one fill.
/// FAIL: zero fills (gate wrongly blocked) or more than one.
#[tokio::test]
async fn price_band_admits_in_band_entry() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("in-band", market("0xnew"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 1);
    println!("PASS: price band admits an in-band first entry (1 fill)");
}

/// PASS: an entry below the band (price 0.30 < 0.40) produces zero fills.
/// FAIL: any fill (gate failed to block a below-band entry).
#[tokio::test]
async fn price_band_blocks_below_band() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("below", market("0xnew"), dec!(0.30))],
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: price band blocks a below-band entry (0 fills)");
}

/// PASS: an entry above the band (price 0.90 > 0.80) produces zero fills.
/// FAIL: any fill (gate failed to block an above-band entry).
#[tokio::test]
async fn price_band_blocks_above_band() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("above", market("0xnew"), dec!(0.90))],
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: price band blocks an above-band entry (0 fills)");
}

// ── First-entry scenarios ─────────────────────────────────────────────────────

/// PASS: a first entry into a market NOT in the leader's history (but the leader is
///       known) produces exactly one fill.
/// FAIL: zero fills (gate wrongly treated a new market as a re-entry).
#[tokio::test]
async fn first_entry_admits_new_market() {
    let dir = TempDir::new().unwrap();
    let history = history_with(leader_wallet(), &[market("0xother")]);
    let fills = run_gate(
        &dir,
        band_config(false),
        history,
        vec![entry_trade("new-market", market("0xnew"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 1);
    println!("PASS: first-entry admits a market absent from leader history (1 fill)");
}

/// PASS: an entry into a market ALREADY in the leader's history produces zero fills.
/// FAIL: any fill (gate failed to block a re-entry).
#[tokio::test]
async fn first_entry_blocks_known_market() {
    let dir = TempDir::new().unwrap();
    let history = history_with(leader_wallet(), &[market("0xknown")]);
    let fills = run_gate(
        &dir,
        band_config(false),
        history,
        vec![entry_trade("known", market("0xknown"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: first-entry blocks a market already in leader history (0 fills)");
}

// ── Fail-open / fail-closed posture ───────────────────────────────────────────

/// PASS: with the leader ABSENT from the history map and the gate fail-open, an
///       in-band first entry is admitted (one fill).
/// FAIL: zero fills (fail-open wrongly blocked an unknown wallet).
#[tokio::test]
async fn fail_open_admits_absent_wallet() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate(
        &dir,
        band_config(false),
        HashMap::new(), // leader absent → unknown history
        vec![entry_trade("fail-open", market("0xnew"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 1);
    println!("PASS: fail-open admits an entry from a wallet absent from history (1 fill)");
}

/// PASS: with the leader ABSENT from the history map and the gate fail-closed, an
///       in-band entry is blocked (zero fills).
/// FAIL: any fill (fail-closed wrongly admitted an unknown wallet).
#[tokio::test]
async fn fail_closed_blocks_absent_wallet() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate(
        &dir,
        band_config(true),
        HashMap::new(), // leader absent → unknown history
        vec![entry_trade("fail-closed", market("0xnew"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: fail-closed blocks an entry from a wallet absent from history (0 fills)");
}
