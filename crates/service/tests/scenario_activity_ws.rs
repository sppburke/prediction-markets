//! Scenario tests for websocket-primary ingestion (#530).
//!
//! Scenarios (v5 artifact acceptance):
//!   WS1 — the same fill delivered via websocket then REST (one `source_trade_id`)
//!         produces exactly one decision and one leader-ledger ingest.
//!   WS2 — websocket-primary mode: a STALE REST-fallback observation commits the
//!         typed no-copy disposition (seen + ledger + disposition, one transaction)
//!         and stages no fill, while a fresh websocket observation still fills.
//!   WS3 — flag disabled: an old REST observation processes byte-identically to
//!         the pre-#530 path (fills; no disposition row) — the rollback posture.
//!   WS4 — dual-unhealthy admission block: the trade is refused BEFORE any state
//!         write (not seen, no ledger row), so redelivery admits it exactly once.
//!   WS5 — source-log replay: envelopes appended by the sink replay through the
//!         event-log `Reader` and the production websocket parser into trades
//!         identical to the originals (fan-in/dedup equivalence from the log).
//!
//! Staleness margins are 30x the budget (60s vs 2s), so wall-clock jitter cannot
//! flip an assertion; the stale rule itself has clock-free unit coverage.
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

use pe_copy_signal_engine::{IncomingTrade, SignalConfig, TradeProvenance};
use pe_core_types::{
    BasisPoints, ContractQty, MarketId, OutcomeId, Price, ReceivedAt, ReconstructionQuality, Side,
    SourceId, SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Reader, Writer};
use pe_execution_core::ExecutionDispatcher;
use pe_paper_state::PaperStateDb;
use pe_position_ledger::PositionLedger;
use pe_service::clob_book::FixtureClobBookFetcher;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::{SharedHealth, new_shared_health_with_ws};
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::source_event_sink::SourceEventSink;
use pe_service::trade_parser;
use pe_source_polymarket_public::{
    ACTIVITY_WS_PARSER_VERSION, ACTIVITY_WS_SCHEMA_VERSION, FixtureFetcher, parse_activity_frame,
};
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, SizingMode, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

// ── Helpers (mirrors scenario_paper_state.rs; scenario files are self-contained) ──

fn leader_wallet() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn market() -> MarketId {
    MarketId(VenueMarketId(
        "0x2222222222222222222222222222222222222222".to_string(),
    ))
}

fn market_b() -> MarketId {
    MarketId(VenueMarketId(
        "0x3333333333333333333333333333333333333333".to_string(),
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

fn trade_at(
    source_trade_id: &str,
    market_id: MarketId,
    observed_at: OffsetDateTime,
    provenance: TradeProvenance,
) -> IncomingTrade {
    IncomingTrade {
        wallet: leader_wallet(),
        market_id,
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(dec!(0.50)),
        contracts: ContractQty(100),
        observed_at,
        received_at: OffsetDateTime::now_utc(),
        source_trade_id: SourceTradeId(source_trade_id.to_string()),
        provenance,
    }
}

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

fn disabled_entry_gate() -> CopyEntryGateConfig {
    CopyEntryGateConfig { fail_closed: false }
}

fn mid_cache_for(markets: &[MarketId], price: &str) -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "http://gamma.test";
    let mut fx = HashMap::new();
    for m in markets {
        let url = format!("{BASE}/markets?condition_ids={m}&limit=500");
        let body =
            format!(r#"[{{"conditionId":"{m}","outcomePrices":"[\"{price}\",\"{price}\"]"}}]"#);
        fx.insert(url, body.into_bytes());
    }
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string())
}

/// Run `trades` through a fresh orchestrator with the given #530 posture.
async fn run_trades_ws(
    dir: &TempDir,
    paper_state: Arc<PaperStateDb>,
    activity_ws_enabled: bool,
    health: SharedHealth,
    trades: Vec<IncomingTrade>,
) {
    let markets: Vec<MarketId> = trades.iter().map(|t| t.market_id.clone()).collect();
    let mid_price_cache = mid_cache_for(&markets, "0.50");

    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(64);
    for t in trades {
        trade_tx.send(t).await.unwrap();
    }
    drop(trade_tx);

    let orch = Orchestrator::new(
        trade_rx,
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled,
            copy_latency_budget_secs: 2,
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            min_fill_price: Decimal::ZERO,
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            fill_mode: pe_service::runtime_config::FillMode::LeaderHaircut,
            clob_best_ask_fallback_haircut_bps: 100,
            entry_gate_config: disabled_entry_gate(),
            runtime_config: None,
            live_accounts: None,
        },
        HashMap::new(),
        WinnerFollowStrategy::new(flat_fill_config()),
        make_dispatcher(dir),
        paper_state,
        PositionLedger::new(),
        health,
        MarketEndCache::new(String::new()),
        mid_price_cache,
        dead_reseed_rx(),
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
    )
    .unwrap();
    orch.run(std::future::pending::<()>()).await;
}

fn paper_fill_count(dir: &TempDir) -> usize {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return 0;
    }
    Reader::replay(&path).unwrap().count()
}

fn healthy_ws_health() -> SharedHealth {
    new_shared_health_with_ws(false, true, 90)
}

// ── WS1: websocket + REST duplicate ⇒ exactly one decision ───────────────────

#[tokio::test]
async fn ws1_duplicate_ws_then_rest_yields_one_decision() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let now = OffsetDateTime::now_utc();
    run_trades_ws(
        &dir,
        paper_state.clone(),
        true,
        healthy_ws_health(),
        vec![
            trade_at("0xdup", market(), now, TradeProvenance::ActivityWs),
            trade_at("0xdup", market(), now, TradeProvenance::RestPoll),
        ],
    )
    .await;
    assert_eq!(paper_fill_count(&dir), 1, "duplicate must not double-fill");
    let rows = paper_state.leader_positions().unwrap();
    assert_eq!(
        rows.iter()
            .find(|r| r.wallet == leader_wallet())
            .unwrap()
            .long_contracts,
        100,
        "leader ledger must ingest the duplicate exactly once"
    );
}

// ── WS2: stale REST fallback ⇒ disposition, no fill; fresh websocket fills ───

#[tokio::test]
async fn ws2_stale_rest_fallback_disposition_fresh_ws_fills() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let now = OffsetDateTime::now_utc();
    let stale_id = SourceTradeId("0xstale".to_string());
    run_trades_ws(
        &dir,
        paper_state.clone(),
        true,
        healthy_ws_health(),
        vec![
            // 60s old on the fallback path: 30x the 2s budget.
            trade_at(
                "0xstale",
                market(),
                now - time::Duration::seconds(60),
                TradeProvenance::RestPoll,
            ),
            // Fresh websocket observation in another market: must still fill.
            trade_at("0xfresh", market_b(), now, TradeProvenance::ActivityWs),
        ],
    )
    .await;
    assert_eq!(
        paper_fill_count(&dir),
        1,
        "only the fresh websocket trade may fill"
    );
    assert!(
        paper_state.is_seen(&stale_id).unwrap(),
        "stale trade is admitted (seen)"
    );
    let (provenance, age, reason) = paper_state
        .no_copy_disposition(&stale_id)
        .unwrap()
        .expect("stale fallback trade must carry a disposition");
    assert_eq!(provenance, "rest_poll");
    assert!(
        age >= 58,
        "recorded age must reflect the observation age (got {age})"
    );
    assert_eq!(reason, "stale_fallback_past_copy_budget");
    // The stale trade still advanced the leader ledger (bookkeeping intact).
    let rows = paper_state.leader_positions().unwrap();
    assert!(
        rows.iter()
            .any(|r| r.market_id == market() && r.long_contracts == 100),
        "stale trade must still mirror the leader position"
    );
}

// ── WS3: disabled flag ⇒ byte-identical legacy admission (rollback posture) ──

#[tokio::test]
async fn ws3_disabled_mode_processes_old_rest_trades_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let now = OffsetDateTime::now_utc();
    let id = SourceTradeId("0xlegacy".to_string());
    run_trades_ws(
        &dir,
        paper_state.clone(),
        false, // flag off
        new_shared_health_with_ws(false, false, 90),
        vec![trade_at(
            "0xlegacy",
            market(),
            now - time::Duration::seconds(60),
            TradeProvenance::RestPoll,
        )],
    )
    .await;
    assert_eq!(
        paper_fill_count(&dir),
        1,
        "legacy path must fill as before #530"
    );
    assert!(
        paper_state.no_copy_disposition(&id).unwrap().is_none(),
        "disabled mode must write no disposition"
    );
}

// ── WS4: dual-unhealthy ⇒ refused before any state write ─────────────────────

#[tokio::test]
async fn ws4_dual_unhealthy_leaves_trade_unseen_for_redelivery() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let health = healthy_ws_health();
    {
        let mut h = health.lock().unwrap();
        // Websocket stale-or-worse: never connected, no valid frame.
        h.ws_connected = false;
        // REST unhealthy: error streak at the threshold.
        h.poll_error_streak = 3;
    }
    let now = OffsetDateTime::now_utc();
    let id = SourceTradeId("0xblocked".to_string());
    run_trades_ws(
        &dir,
        paper_state.clone(),
        true,
        health.clone(),
        vec![trade_at(
            "0xblocked",
            market(),
            now,
            TradeProvenance::ActivityWs,
        )],
    )
    .await;
    assert_eq!(paper_fill_count(&dir), 0, "blocked trade must not fill");
    assert!(
        !paper_state.is_seen(&id).unwrap(),
        "blocked trade must stay unseen so redelivery admits it"
    );
    assert!(
        paper_state.leader_positions().unwrap().is_empty(),
        "blocked trade must not touch the leader ledger"
    );

    // Recovery: sources healthy again → the redelivered trade admits exactly once.
    {
        let mut h = health.lock().unwrap();
        h.poll_error_streak = 0;
        h.ws_connected = true;
        h.ws_last_valid_frame_at = Some(OffsetDateTime::now_utc());
    }
    run_trades_ws(
        &dir,
        paper_state.clone(),
        true,
        health,
        vec![trade_at(
            "0xblocked",
            market(),
            now,
            TradeProvenance::ActivityWs,
        )],
    )
    .await;
    assert_eq!(
        paper_fill_count(&dir),
        1,
        "redelivery after recovery fills once"
    );
    assert!(paper_state.is_seen(&id).unwrap());
}

// ── WS5: source-log replay reconstructs identical trades ─────────────────────

#[test]
fn ws5_source_log_replay_reconstructs_identical_trades() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.log");

    // Two real-shape frames (string numerics, extra UI fields) as the feed sends them.
    let frames = [
        r#"{"topic":"activity","type":"trades","payload":{"proxyWallet":"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","conditionId":"0xc1","side":"BUY","size":"50.7","price":"0.65","timestamp":"1704067200","transactionHash":"0xt1","outcomeIndex":"1","fee":"0","title":"A"}}"#,
        r#"{"topic":"activity","type":"trades","payload":{"proxyWallet":"0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","conditionId":"0xc2","side":"SELL","size":"3","price":"0.41","timestamp":"1704067300","transactionHash":"0xt2"}}"#,
    ];

    let mut originals = Vec::new();
    {
        let mut sink = SourceEventSink::open(&path).unwrap();
        for frame in frames {
            let (raws, malformed) = parse_activity_frame(frame).unwrap();
            assert_eq!((raws.len(), malformed), (1, 0));
            let raw = &raws[0];
            let trade = trade_parser::parse_ws_trade(&raw.payload_json).unwrap();
            sink.append_durable(EnvelopeIn {
                source_id: SourceId("polymarket-activity-ws".to_string()),
                schema_version: ACTIVITY_WS_SCHEMA_VERSION,
                parser_version: ACTIVITY_WS_PARSER_VERSION,
                observed_at: SourceTimestamp(trade.observed_at),
                received_at: ReceivedAt(trade.received_at),
                content_type: ContentType::Json,
                payload: raw.payload_json.clone(),
            })
            .unwrap();
            originals.push(trade);
        }
    }

    // Replay through the event-log Reader + the PRODUCTION parser.
    let replayed: Vec<IncomingTrade> = Reader::replay(&path)
        .unwrap()
        .map(|item| {
            let (_seq, env) = item.unwrap();
            assert_eq!(env.schema_version, ACTIVITY_WS_SCHEMA_VERSION);
            trade_parser::parse_ws_trade(&env.payload).unwrap()
        })
        .collect();

    assert_eq!(replayed.len(), originals.len());
    for (orig, replay) in originals.iter().zip(&replayed) {
        assert_eq!(orig.source_trade_id, replay.source_trade_id);
        assert_eq!(orig.wallet, replay.wallet);
        assert_eq!(orig.market_id.0.0, replay.market_id.0.0);
        assert_eq!(orig.outcome_id, replay.outcome_id);
        assert_eq!(orig.side, replay.side);
        assert_eq!(orig.price, replay.price);
        assert_eq!(orig.contracts, replay.contracts);
        assert_eq!(orig.observed_at, replay.observed_at);
        assert_eq!(orig.provenance, replay.provenance);
        // received_at is stamped at parse time by design; identity is the decision
        // inputs above, which drive fan-in and dedup.
    }
}
