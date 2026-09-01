//! Scenario tests for the paper-state correctness slice (issue #282 Phase 1).
//!
//! Scenarios:
//!   AC1 — a trade delivered twice (same source_trade_id) ⇒ exactly one fill and one
//!         leader-ledger ingest.
//!   AC2 — a watchlisted trade that produces no order (Shadow) is still marked seen, so
//!         its duplicate is not re-ingested into the leader ledger.
//!   AC4 — after "restart", the leader PositionLedger is rehydrated from paper-state and
//!         a previously-filled trade does not re-fire.
//!   AC5 — a fill written to the event log but not committed to SQLite (crash) is
//!         replayed on restart, exactly once.
//!
//! Run with: cargo nextest run -p pe-service --features scenario

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use pe_copy_signal_engine::TradeProvenance;
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, SignalConfig};
use pe_core_types::{
    BasisPoints, ContractQty, MarketId, MarketOutcomeId, OutcomeId, Price, ReconstructionQuality,
    Side, SourceId, SourceTimestamp, SourceTradeId, StrategyId, VenueMarketId, WalletAddress,
};
use pe_event_log::{Reader, Writer};
use pe_execution_core::ExecutionDispatcher;
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_service::clob_book::{BookLevel, FixtureClobBookFetcher, OrderBook};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::paper_recovery::{build_leader_ledger, reconcile_paper_state};
use pe_service::runtime_config::FillMode;
use pe_source_polymarket_public::FixtureFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, SizingMode, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_core::OrderIntent;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn leader_wallet() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn market() -> MarketId {
    MarketId(VenueMarketId(
        "0x1111111111111111111111111111111111111111".to_string(),
    ))
}

fn make_watchlist(wallet: WalletAddress) -> Watchlist {
    let quality = ReconstructionQuality::new(100).unwrap();
    let score = BasisPoints(200);
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet,
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

/// A BUY trade from `leader_wallet` in `market`, 100 contracts at 0.50.
fn trade(source_trade_id: &str, side: Side, contracts: u64, observed_unix: i64) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(observed_unix).unwrap();
    IncomingTrade {
        wallet: leader_wallet(),
        market_id: market(),
        outcome_id: OutcomeId(0),
        side,
        price: Price(dec!(0.50)),
        contracts: pe_core_types::ShareAmount::from_whole(contracts).unwrap(),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(source_trade_id.to_string()),
        transaction_hash: None,
        provenance: TradeProvenance::RestPoll,
    }
}

/// Strategy config that deterministically fills via the flat sizing path.
fn flat_fill_config() -> WinnerFollowConfig {
    WinnerFollowConfig {
        sizing_mode: SizingMode::Dollar { usd: dec!(100) },
        ..WinnerFollowConfig::default()
    }
}

fn make_dispatcher(dir: &TempDir) -> ExecutionDispatcher {
    let paper_writer = Writer::open(dir.path().join("paper.log")).unwrap();
    let paper_executor = PaperExecutor::new(paper_writer, SourceId("test.paper".into()), 500, 100);
    ExecutionDispatcher::paper_only(paper_executor)
}

fn dead_reseed_rx() -> mpsc::Receiver<pe_service::orchestrator_control::OrchestratorControl> {
    mpsc::channel(1).1
}

/// Copy-entry gate disabled for these correctness tests: fail-open (no band since #339).
/// Paired with an empty history map so every first Entry is admitted.
fn disabled_entry_gate() -> CopyEntryGateConfig {
    CopyEntryGateConfig
}

/// Mid-price cache fixture quoting `price` for both outcomes of every market in `markets`,
/// so an admitted signal can fetch a current price and reach a fill (#339).
fn mid_cache_for(markets: &[MarketId], price: &str) -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "http://gamma.test";
    let mut fx = HashMap::new();
    for m in markets {
        // Single-id `OpenOnly` batch URL the shared GammaMarketsClient builds (#382 Phase 3b);
        // the orchestrator fetches one market per signal, so each is a batch-of-one.
        let url = format!("{BASE}/markets?condition_ids={m}&limit=500");
        let body = format!(
            r#"[{{"conditionId":"{m}","outcomePrices":"[\"{price}\",\"{price}\"]","clobTokenIds":"[\"{m}-0\",\"{m}-1\"]"}}]"#
        );
        fx.insert(url, body.into_bytes());
    }
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string())
}

fn paper_state_at(dir: &TempDir) -> Arc<PaperStateDb> {
    Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap())
}

/// Feed `trades` through a fresh orchestrator (with the given mode/config/leader-ledger)
/// and run until both channels close. Shares `dir` so the paper.log / paper_state.db persist.
async fn run_trades(
    dir: &TempDir,
    paper_state: Arc<PaperStateDb>,
    leader_ledger: PositionLedger,
    mode: ExecutionMode,
    strategy_cfg: WinnerFollowConfig,
    trades: Vec<IncomingTrade>,
) {
    paper_state
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: leader_wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    // Quote every traded market at 0.50 so an admitted signal can fetch a current price.
    let markets: Vec<MarketId> = trades.iter().map(|t| t.market_id.clone()).collect();
    let mid_price_cache = mid_cache_for(&markets, "0.50");
    let books = trades
        .iter()
        .map(|trade| {
            (
                format!("{}-{}", trade.market_id, trade.outcome_id.0),
                OrderBook {
                    asks: vec![BookLevel {
                        price: dec!(0.50),
                        size: dec!(10000),
                    }],
                    fetched_at_ms: 0,
                },
            )
        })
        .collect();

    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(64);
    for t in trades {
        trade_tx.send(t).await.unwrap();
    }
    drop(trade_tx);

    let orch = Orchestrator::new(
        trade_rx,
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
            bankroll: Decimal::from(10_000u32),
            mode,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0, // disabled in tests
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            min_fill_price: Decimal::ZERO,
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            // #486: pin the pre-feature haircut basis so this existing assertion stays byte-
            // identical (no /book fetch; leader × 1.05).
            fill_mode: FillMode::LeaderHaircut,
            price_impact_cap_bps: 100,
            entry_gate_config: disabled_entry_gate(),
            runtime_config: None,
            live_accounts: None,
        },
        WinnerFollowStrategy::new(strategy_cfg),
        make_dispatcher(dir),
        paper_state,
        leader_ledger,
        new_shared_health(false),
        MarketEndCache::new(String::new()),
        mid_price_cache,
        dead_reseed_rx(),
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .unwrap();
    orch.run(std::future::pending::<()>()).await;
}

/// Count `PaperFill` frames in the paper event log (each fill is one frame).
fn paper_fill_count(dir: &TempDir) -> usize {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return 0;
    }
    Reader::replay(&path).unwrap().count()
}

fn leader_long(paper_state: &PaperStateDb) -> u64 {
    let rows = paper_state.leader_positions().unwrap();
    let row = rows
        .iter()
        .find(|r| {
            r.wallet == leader_wallet() && r.market_id == market() && r.outcome_id == OutcomeId(0)
        })
        .expect("leader position row");
    row.long_contracts.atomic()
}

fn whole_shares(value: u64) -> u64 {
    pe_core_types::ShareAmount::from_whole(value)
        .unwrap()
        .atomic()
}

// ── AC1 ──────────────────────────────────────────────────────────────────────

/// PASS: a trade delivered twice (same source_trade_id, distinct observed_at buckets)
///       yields exactly one fill and one leader-ledger ingest.
/// FAIL: two fills, or leader long-contracts doubled.
#[tokio::test]
async fn ac1_duplicate_trade_fills_and_ingests_once() {
    let dir = TempDir::new().unwrap();
    let paper_state = paper_state_at(&dir);
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    // Same source_trade_id, two distinct observed_at buckets (Open risk #6).
    let t1 = trade("dup-tx", Side::Buy, 100, 1_700_000_000);
    let t2 = trade("dup-tx", Side::Buy, 100, 1_700_000_010);

    run_trades(
        &dir,
        paper_state.clone(),
        PositionLedger::new(),
        ExecutionMode::Paper,
        flat_fill_config(),
        vec![t1, t2],
    )
    .await;

    assert_eq!(paper_fill_count(&dir), 1, "exactly one PaperFill");
    assert_eq!(
        leader_long(&paper_state),
        whole_shares(100),
        "leader ledger ingested once"
    );
    assert!(
        paper_state
            .is_seen(&SourceTradeId("dup-tx".to_string()))
            .unwrap()
    );
    println!("PASS: AC1 duplicate trade fills and ingests exactly once");
}

// ── AC2 ──────────────────────────────────────────────────────────────────────

/// PASS: a watchlisted trade that produces no order (Shadow mode) is still marked
///       seen, so its duplicate does not re-ingest the leader ledger (no double-count).
/// FAIL: a fill is recorded, or leader long-contracts doubled on the duplicate.
#[tokio::test]
async fn ac2_no_fill_trade_is_still_deduped() {
    let dir = TempDir::new().unwrap();
    let paper_state = paper_state_at(&dir);
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    let t1 = trade("noedge-tx", Side::Buy, 100, 1_700_000_000);
    let t2 = trade("noedge-tx", Side::Buy, 100, 1_700_000_010);

    // Shadow mode → evaluate returns ShadowMode → no order produced.
    run_trades(
        &dir,
        paper_state.clone(),
        PositionLedger::new(),
        ExecutionMode::Shadow,
        WinnerFollowConfig::default(),
        vec![t1, t2],
    )
    .await;

    assert_eq!(paper_fill_count(&dir), 0, "no fill in Shadow mode");
    assert_eq!(
        leader_long(&paper_state),
        whole_shares(100),
        "leader ledger ingested once despite no fill"
    );
    assert!(
        paper_state
            .is_seen(&SourceTradeId("noedge-tx".to_string()))
            .unwrap()
    );
    println!("PASS: AC2 no-fill trade is marked seen and ingested once");
}

// ── AC4 ──────────────────────────────────────────────────────────────────────

/// PASS: after a restart, the leader PositionLedger is rehydrated from paper-state
///       (so the next trade sees the existing position, not a fresh Entry), and a
///       previously-filled trade does not re-fire.
/// FAIL: the rehydrated ledger lacks the leader's position, or the filled trade is
///       no longer marked seen.
#[tokio::test]
async fn ac4_leader_ledger_rehydrates_on_restart() {
    let dir = TempDir::new().unwrap();

    // Run 1: a BUY 100 establishes the leader's position and a fill.
    {
        let paper_state = paper_state_at(&dir);
        paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();
        run_trades(
            &dir,
            paper_state.clone(),
            PositionLedger::new(),
            ExecutionMode::Paper,
            flat_fill_config(),
            vec![trade("entry-tx", Side::Buy, 100, 1_700_000_000)],
        )
        .await;
        assert_eq!(leader_long(&paper_state), whole_shares(100));
    }

    // "Restart": reopen the persisted DB and rebuild the in-memory ledger.
    let restarted = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    let ledger = build_leader_ledger(&restarted).unwrap();
    let key = MarketOutcomeId::new(market(), OutcomeId(0));
    let state = ledger
        .position(&leader_wallet())
        .and_then(|snap| snap.positions.get(&key).copied())
        .expect("rehydrated leader position");
    assert_eq!(
        state.long_contracts,
        pe_core_types::ShareAmount::from_whole(100).unwrap(),
        "leader position rehydrated"
    );
    assert!(
        restarted
            .is_seen(&SourceTradeId("entry-tx".to_string()))
            .unwrap(),
        "filled trade still marked seen after restart"
    );
    println!("PASS: AC4 leader ledger rehydrates and filled trade does not re-fire");
}

// ── AC5 ──────────────────────────────────────────────────────────────────────

fn fill_intent() -> OrderIntent {
    OrderIntent {
        strategy_id: StrategyId("winner-follow".to_string()),
        market_id: market(),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        contracts: ContractQty(100),
        limit_price: Price(dec!(0.50)),
        validity_seconds: 30,
        idempotency_key: "wf|0xaa|crash-tx|mkt|0|buy|1700000000".to_string(),
    }
}

/// PASS: a fill written to the event log but not committed to SQLite (crash) is
///       replayed exactly once on restart — bankroll is debited and a second
///       reconcile is a no-op.
/// FAIL: the fill is not replayed, or is applied twice (bankroll debited twice).
#[tokio::test]
async fn ac5_crash_between_log_and_sqlite_reconciles() {
    let dir = TempDir::new().unwrap();
    let log_path = dir.path().join("paper.log");
    let paper_state = PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap();
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    // Simulate the crash: a fill is written + synced to the event log, but the
    // paper-state commit never runs (last_applied stays 0).
    {
        let writer = Writer::open(&log_path).unwrap();
        let mut executor = PaperExecutor::new(writer, SourceId("test.paper".into()), 0, 0);
        let (_fill, _seq) = executor
            .execute(
                &fill_intent(),
                SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
                None,
            )
            .unwrap();
    }
    assert_eq!(
        paper_state.bankroll().unwrap(),
        Some(Decimal::from(10_000u32))
    );

    // Restart reconciliation replays the uncommitted fill exactly once.
    let applied = reconcile_paper_state(&log_path, &paper_state).unwrap();
    assert_eq!(applied, 1, "one fill replayed");
    // BUY 100 @ 0.50, haircut 0 → 50 debited.
    assert_eq!(paper_state.bankroll().unwrap(), Some(dec!(9950.00)));

    // A second reconcile is a no-op (no double-apply).
    let again = reconcile_paper_state(&log_path, &paper_state).unwrap();
    assert_eq!(again, 0, "no fill replayed twice");
    assert_eq!(paper_state.bankroll().unwrap(), Some(dec!(9950.00)));
    println!("PASS: AC5 crash between log and SQLite reconciles exactly once");
}
