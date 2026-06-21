//! Scenario tests for the #398 WS2 price-impact gate, end-to-end through the orchestrator.
//!
//! With `price_impact_cap_bps > 0`, `handle_trade` fetches the live CLOB `/book` for the outcome's
//! token (resolved from the mid cache's `clobTokenIds`) and `min`s the size to the contracts
//! absorbable within the band. Three outcomes are asserted:
//! - a shallow book caps the fill to its depth;
//! - an empty book (0 absorbable) yields `Some(0)` and SKIPS the trade;
//! - a `/book` fetch error (token absent) FAILS OPEN — full size fills.
//!
//! Determinism: fixed wallet/market/timestamps, no clocks, no network. Each scenario prints PASS.
//!
//! Run with: cargo nextest run -p pe-service --features scenario

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use pe_copy_signal_engine::{IncomingTrade, SignalConfig};
use pe_core_types::{
    BasisPoints, ContractQty, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{Reader, Writer};
use pe_execution_core::{ExecutionDispatcher, LiveExecutor};
use pe_paper_state::PaperStateDb;
use pe_position_ledger::PositionLedger;
use pe_service::clob_book::{BookLevel, FixtureClobBookFetcher, OrderBook};
use pe_service::config::ServiceConfig;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::runtime_config::{LiveRuntimeConfig, RuntimeConfig};
use pe_source_polymarket_public::FixtureFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, PerTradeCap, SizingMode, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::{FixtureCLOBClient, PolymarketCredentials, PolymarketVenueAdapter};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

const COND: &str = "0xpig";
const TOKEN: &str = "111"; // outcome 0's CLOB token id (clobTokenIds = ["111","222"])

fn leader_wallet() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn make_watchlist() -> Watchlist {
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet: leader_wallet(),
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(200),
            lcb_5pct_bps: BasisPoints(200),
            win_rate_bps: BasisPoints(7_000),
            closed_trades_in_window: 0,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        }],
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count: 1,
        incubator_count: 0,
    }
}

fn entry_trade() -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    IncomingTrade {
        wallet: leader_wallet(),
        market_id: MarketId(VenueMarketId(COND.to_string())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(dec!(0.50)),
        contracts: ContractQty(100),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId("pig-1".to_string()),
    }
}

/// Mid fixture quoting 0.50 for both outcomes AND carrying `clobTokenIds` so the gate can resolve
/// the outcome's token (outcome 0 → "111").
fn mid_cache() -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "http://gamma.test";
    let mut fx = HashMap::new();
    let url = format!("{BASE}/markets?condition_ids={COND}&limit=500");
    let body = format!(
        r#"[{{"conditionId":"{COND}","outcomePrices":"[\"0.50\",\"0.50\"]","clobTokenIds":"[\"111\",\"222\"]"}}]"#
    );
    fx.insert(url, body.into_bytes());
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string())
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

fn paper_fill_count(dir: &TempDir) -> usize {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return 0;
    }
    Reader::replay(&path).unwrap().count()
}

/// Snapshot that dollar-sizes to 200 contracts ($100 / 0.50), per-trade cap removed so the BOOK
/// cap is the only binding constraint, with the price-impact gate set to `cap_bps`.
fn gate_snapshot(cap_bps: i32) -> RuntimeConfig {
    let mut rc = RuntimeConfig::from_service_config(&ServiceConfig::default());
    rc.sizing_mode = SizingMode::Dollar { usd: dec!(100) };
    rc.per_trade_cap = PerTradeCap::Unlimited;
    rc.price_impact_cap_bps = cap_bps;
    rc.max_resolution_horizon_secs = 0;
    rc.min_resolution_horizon_secs = 0;
    rc.max_fill_price = "0.90".to_string();
    rc
}

/// Run one BUY through an orchestrator wired with `books` (token → /book) and the gate at
/// `cap_bps`. Returns (fill count, filled contracts).
async fn run_gate(dir: &TempDir, books: HashMap<String, OrderBook>, cap_bps: i32) -> (usize, u64) {
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    let (tx, rx) = mpsc::channel::<IncomingTrade>(8);
    tx.send(entry_trade()).await.unwrap();
    drop(tx);

    let orch = Orchestrator::new(
        rx,
        LiveWatchlist::new(make_watchlist()),
        OrchestratorConfig {
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            entry_gate_config: CopyEntryGateConfig { fail_closed: false },
            runtime_config: Some(LiveRuntimeConfig::new(gate_snapshot(cap_bps))),
        },
        HashMap::new(),
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        make_dispatcher(dir),
        paper_state.clone(),
        PositionLedger::new(),
        new_shared_health(false),
        MarketEndCache::new(String::new()),
        mid_cache(),
        mpsc::channel(1).1,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .unwrap();
    orch.run(std::future::pending::<()>()).await;

    let contracts = paper_state
        .paper_positions()
        .unwrap()
        .first()
        .map(|p| p.long_contracts)
        .unwrap_or(0);
    (paper_fill_count(dir), contracts)
}

fn book(asks: &[(Decimal, Decimal)]) -> OrderBook {
    OrderBook {
        asks: asks
            .iter()
            .map(|&(price, size)| BookLevel { price, size })
            .collect(),
    }
}

// ── Scenarios ───────────────────────────────────────────────────────────────────

/// PASS: a shallow book (3 contracts at best ask) caps the dollar-sized 200 down to 3.
/// FAIL: 200 (book cap ignored) or 0 (wrongly skipped).
#[tokio::test]
async fn shallow_book_caps_the_fill() {
    let dir = TempDir::new().unwrap();
    let books = HashMap::from([(TOKEN.to_string(), book(&[(dec!(0.50), dec!(3))]))]);
    let (fills, contracts) = run_gate(&dir, books, 100).await;
    assert_eq!(fills, 1);
    assert_eq!(
        contracts, 3,
        "book cap must reduce the fill to the absorbable depth"
    );
    println!("PASS: shallow /book caps the dollar-sized 200 to absorbable depth 3");
}

/// PASS: an empty book (0 absorbable) yields Some(0) and skips the trade.
/// FAIL: any fill (a successful-but-empty book must NOT fail open).
#[tokio::test]
async fn empty_book_zero_absorbable_skips_trade() {
    let dir = TempDir::new().unwrap();
    let books = HashMap::from([(TOKEN.to_string(), book(&[]))]);
    let (fills, _) = run_gate(&dir, books, 100).await;
    assert_eq!(fills, 0, "Some(0) book cap must skip the trade");
    println!("PASS: empty /book (0 absorbable) skips the trade (Some(0), distinct from fail-open)");
}

/// PASS: a /book fetch error (token absent from the fixture) FAILS OPEN — the full dollar-sized
///       200 fills, identical to no gate.
/// FAIL: 0 fills (a fetch error wrongly skipped) or a capped count.
#[tokio::test]
async fn book_fetch_error_fails_open() {
    let dir = TempDir::new().unwrap();
    // Empty fixture → fetch_book(TOKEN) returns MissingFixture error → gate fails open.
    let (fills, contracts) = run_gate(&dir, HashMap::new(), 100).await;
    assert_eq!(fills, 1);
    assert_eq!(
        contracts, 200,
        "a /book error must fail open (full dollar size)"
    );
    println!("PASS: /book fetch error fails open (full size 200, no cap)");
}
