//! Scenario tests for websocket-primary ingestion (#530, #546).
//!
//! Reader-pool scenarios run three real readers over in-process pipes
//! (`ActivityWsPeer`, scenario feature) with the PRODUCTION timing constants
//! under a paused tokio clock: time moves only by explicit `advance`, one
//! boundary at a time, after the runtime has settled. Health ages are wall
//! clock, so derived-liveness assertions pass an explicit instant to the pure
//! rule; the reader's own deadline is tokio-`Instant` based and is what the
//! paused clock drives.
//!
//!   R1  — acks, keepalive text, unrelated topics, envelope errors, missing
//!         payloads, and parser-rejected payloads never refresh liveness; the
//!         socket drops at exactly 30 s; re-dials follow the 1 s, 2 s backoff.
//!   R2  — a mixed frame delivers the watched row once, uses a parser-accepted
//!         unwatched row for liveness only, records only the watched row.
//!   R4  — one permanently silent slot re-dials while the other two deliver a
//!         watched row through the durable source log to ONE decision; a
//!         distinct identifier is also delivered; no polling input exists.
//!   R5  — each slot's reconnect backoff is independent.
//!   R6  — three byte-identical reader copies with a one-shot staging fault on
//!         the first copy: three raw envelopes, one seen row, one leader delta,
//!         one fill, one dispatch seed, the frozen armed targets; then the
//!         source log replays into a fresh orchestrator to the same result.
//!   R7  — full fan-in behind a delayed append: the reader retains its frame,
//!         item, and socket beyond 30 s, then credits liveness from the fresh
//!         post-delivery instant and continues on the same connection.
//!   R8  — the early gate applies the strict `age > budget` rule to BOTH
//!         provenances with a fixed clock; exact-boundary rows stay eligible.
//!   R9  — fresh at the early gate, stale immediately before staging: no-copy
//!         with the entry retained; a one-shot no-copy commit fault rolls the
//!         admission back so a later trade stages exactly once.
//!   R10 — closed downstream ends the pool orderly; external abort stops every
//!         reader; neither re-dials.
//!   WS1–WS5 (#530) are retained unchanged in intent.
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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use pe_copy_signal_engine::{IncomingTrade, SignalConfig, TradeProvenance};
use pe_core_types::{
    BasisPoints, CollateralAmount, MarketId, OutcomeId, PolymarketConditionId, PolymarketTokenId,
    Price, ReceivedAt, ReconstructionQuality, ShareAmount, Side, SourceId, SourceTimestamp,
    SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Reader, Scanner, Writer};
use pe_execution_core::{AdmissionReceipts, LiveAdmissionArtifact};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_service::activity_ingest::{ACTIVITY_WS_SOURCE_ID, ActivityIngest, Dialer, SourceLogHandle};
use pe_service::bucket_commit::BucketDecisionContext;
use pe_service::clob_book::{BookLevel, FixtureClobBookFetcher, OrderBook, ReqwestClobBookFetcher};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::{ReaderHealth, SharedHealth, new_shared_health_with_ws, readiness_issues};
use pe_service::live_accounts::{
    AccountRow, CredentialMetaRow, LiveAccounts, LiveAccountsSnapshot,
};
use pe_service::live_venue_adapter::LiveAdmissionBuilder;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig, ScenarioHooks};
use pe_service::paper_recovery::{
    PAPER_LOG_SCHEMA_VERSION, PaperLogRecord, QualificationStarted, TailBinding,
    build_leader_ledger,
};
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::runtime_config::RuntimeConfig;
use pe_service::source_event_sink::SourceEventSink;
use pe_service::trade_parser;
use pe_service::{config::ServiceConfig, mark_prices::HistoricalMarkAdapter};
use pe_source_polymarket_public::{
    ACTIVITY_WS_PARSER_VERSION, ACTIVITY_WS_SCHEMA_VERSION, ACTIVITY_WS_SUBSCRIBE, ActivityWsError,
    ActivityWsPeer, FixtureFetcher, LiveMarketEvidence, parse_activity_frame,
    parse_activity_trade_observation,
};
use pe_strategy_winner_follow::{
    ExecutionMode, SizingMode, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::CompactFeeSchedule;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

mod support;

// ── Helpers (mirrors scenario_paper_state.rs; scenario files are self-contained) ──

const LEADER: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const STRANGER: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn leader_wallet() -> WalletAddress {
    serde_json::from_str(&format!("\"{LEADER}\"")).unwrap()
}

fn market_with(hex: char) -> MarketId {
    MarketId(VenueMarketId(format!("0x{}", hex.to_string().repeat(40))))
}

fn market() -> MarketId {
    market_with('2')
}

fn market_b() -> MarketId {
    market_with('3')
}

fn market_c() -> MarketId {
    market_with('4')
}

fn market_d() -> MarketId {
    market_with('5')
}

fn now_unix() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
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
        contracts: pe_core_types::ShareAmount::from_whole(100).unwrap(),
        observed_at,
        received_at: OffsetDateTime::now_utc(),
        source_trade_id: SourceTradeId(source_trade_id.to_string()),
        transaction_hash: None,
        provenance,
    }
}

fn bucket_id(transaction_hash: &str, market_id: MarketId) -> SourceTradeId {
    support::bucket_source_trade_id(&trade_at(
        transaction_hash,
        market_id,
        OffsetDateTime::UNIX_EPOCH,
        TradeProvenance::ActivityWs,
    ))
}

/// One real-shape websocket activity payload (string numerics, as the feed sends them).
fn payload(tx: &str, wallet: &str, market: &MarketId, observed_unix: i64) -> String {
    format!(
        r#"{{"proxyWallet":"{wallet}","conditionId":"{market}","asset":"123","side":"BUY","size":"100","price":"0.50","timestamp":"{observed_unix}","transactionHash":"{tx}","outcomeIndex":"0"}}"#
    )
}

/// One websocket text frame (array form) carrying the given activity payloads.
fn activity_frame(payloads: &[String]) -> String {
    let frames: Vec<String> = payloads
        .iter()
        .map(|p| format!(r#"{{"topic":"activity","type":"trades","payload":{p}}}"#))
        .collect();
    format!("[{}]", frames.join(","))
}

fn flat_fill_config() -> WinnerFollowConfig {
    WinnerFollowConfig {
        sizing_mode: SizingMode::Dollar { usd: dec!(100) },
        ..WinnerFollowConfig::default()
    }
}

fn make_writer(dir: &Path) -> Writer {
    Writer::open(dir.join("paper.log")).unwrap()
}

fn disabled_entry_gate() -> CopyEntryGateConfig {
    CopyEntryGateConfig
}

fn mid_cache_for(markets: &[MarketId], price: &str) -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "http://gamma.test";
    let mut fx = HashMap::new();
    for m in markets {
        let url = format!("{BASE}/markets?condition_ids={m}&limit=500");
        let body = format!(
            r#"[{{"conditionId":"{m}","outcomePrices":"[\"{price}\",\"{price}\"]","clobTokenIds":"[\"{m}-0\",\"{m}-1\"]"}}]"#
        );
        fx.insert(url, body.into_bytes());
    }
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string())
}

fn healthy_ws_health() -> SharedHealth {
    new_shared_health_with_ws(false, true, 90)
}

fn paper_fill_count(dir: &Path) -> usize {
    let path = dir.join("paper.log");
    if !path.exists() {
        return 0;
    }
    Reader::replay(&path).unwrap().count()
}

fn leader_long(paper_state: &PaperStateDb, market_id: &MarketId) -> Option<u64> {
    paper_state
        .leader_positions()
        .unwrap()
        .iter()
        .find(|r| r.wallet == leader_wallet() && r.market_id == *market_id)
        .map(|r| r.long_contracts.atomic())
}

fn whole_shares(value: u64) -> u64 {
    pe_core_types::ShareAmount::from_whole(value)
        .unwrap()
        .atomic()
}

// ── Orchestrator fixture ─────────────────────────────────────────────────────

struct OrchOpts {
    ws_enabled: bool,
    live_accounts: Option<LiveAccounts>,
    hooks: Option<Arc<ScenarioHooks>>,
    markets: Vec<MarketId>,
}

impl OrchOpts {
    fn ws(markets: Vec<MarketId>) -> Self {
        Self {
            ws_enabled: true,
            live_accounts: None,
            hooks: None,
            markets,
        }
    }
}

fn build_orchestrator(
    dir: &Path,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,
    opts: OrchOpts,
) -> (
    Orchestrator<FixtureFetcher, FixtureClobBookFetcher>,
    mpsc::Sender<pe_service::orchestrator_control::OrchestratorControl>,
) {
    paper_state
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: leader_wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    support::install_empty_anchor(&paper_state, leader_wallet(), 0);
    let mid_price_cache = mid_cache_for(&opts.markets, "0.50");
    let books = opts
        .markets
        .iter()
        .flat_map(|market| {
            (0..=1).map(move |outcome| {
                (
                    format!("{market}-{outcome}"),
                    OrderBook {
                        asks: vec![BookLevel {
                            price: dec!(0.50),
                            size: dec!(10000),
                        }],
                        response_blake3: String::new(),
                        fetched_at_ms: 0,
                        source_receipt: None,
                    },
                )
            })
        })
        .collect();
    let leader_ledger = build_leader_ledger(&paper_state).unwrap();
    let (control_tx, control_rx) = mpsc::channel(64);
    let mut orch = Orchestrator::new(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled: opts.ws_enabled,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,

            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            min_fill_price: Decimal::ZERO,
            price_impact_cap_bps: 100,
            entry_gate_config: disabled_entry_gate(),
            runtime_config: None,
            live_accounts: opts.live_accounts,
        },
        WinnerFollowStrategy::new(flat_fill_config()),
        make_writer(dir),
        paper_state,
        leader_ledger,
        health,
        mid_price_cache,
        control_rx,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .unwrap();
    if let Some(hooks) = opts.hooks {
        orch.set_scenario_hooks(hooks);
    }
    (orch, control_tx)
}

/// Run `trades` (in order) through a fresh orchestrator to completion.
async fn run_trades(
    dir: &Path,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,
    opts: OrchOpts,
    trades: Vec<IncomingTrade>,
) {
    let (orch, control_tx) = build_orchestrator(dir, paper_state, health, opts);
    let run = tokio::spawn(orch.run(std::future::pending::<()>()));
    for trade in trades {
        support::send_trade_bucket(&control_tx, trade).await;
    }
    drop(control_tx);
    run.await.unwrap();
}

fn spawn_orchestrator(
    orch: Orchestrator<FixtureFetcher, FixtureClobBookFetcher>,
) -> (JoinHandle<()>, oneshot::Sender<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(orch.run(async move {
        rx.await.ok();
    }));
    (task, tx)
}

fn forward_trade_buckets(
    mut trades: mpsc::Receiver<IncomingTrade>,
    control: mpsc::Sender<pe_service::orchestrator_control::OrchestratorControl>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(trade) = trades.recv().await {
            support::send_trade_bucket(&control, trade).await;
        }
    })
}

// ── Reader-pool fixture: three real readers over in-process pipes ─────────────

/// Dial fixture: every dial for a slot is recorded with its (paused) instant;
/// a successful dial hands the reader a fresh pipe and parks the server half
/// here for the test to drive. Slots in `failing` refuse to connect.
#[derive(Default)]
struct PipeNet {
    dials: Mutex<Vec<(usize, Instant)>>,
    servers: Mutex<Vec<(usize, ActivityWsPeer)>>,
    failing: Mutex<HashSet<usize>>,
}

impl PipeNet {
    fn dialer(self: &Arc<Self>) -> Dialer {
        let net = Arc::clone(self);
        Arc::new(move |slot| {
            let net = Arc::clone(&net);
            Box::pin(async move {
                net.dials.lock().unwrap().push((slot, Instant::now()));
                if net.failing.lock().unwrap().contains(&slot) {
                    return Err(ActivityWsError::Transport {
                        message: "connection refused".to_string(),
                    });
                }
                let (client, server) = ActivityWsPeer::pair().await?;
                net.servers.lock().unwrap().push((slot, server));
                Ok(client)
            })
        })
    }

    fn dial_count(&self, slot: usize) -> usize {
        self.dials
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, _)| *s == slot)
            .count()
    }

    fn dial_times(&self, slot: usize) -> Vec<Instant> {
        self.dials
            .lock()
            .unwrap()
            .iter()
            .filter(|(s, _)| *s == slot)
            .map(|(_, t)| *t)
            .collect()
    }

    /// The oldest not-yet-taken server half for `slot`, with the production
    /// subscription frame already asserted and consumed.
    fn take_server(&self, slot: usize) -> ActivityWsPeer {
        let mut servers = self.servers.lock().unwrap();
        let idx = servers
            .iter()
            .position(|(s, _)| *s == slot)
            .expect("a connection exists for this slot");
        let mut server = servers.remove(idx).1;
        assert_eq!(
            server.try_recv_text().as_deref(),
            Some(ACTIVITY_WS_SUBSCRIBE),
            "every connection starts with the production subscription"
        );
        server
    }
}

struct Pool {
    net: Arc<PipeNet>,
    health: SharedHealth,
    source_log: PathBuf,
    task: JoinHandle<()>,
    _dir: TempDir,
}

impl Pool {
    fn reader(&self, slot: usize) -> ReaderHealth {
        self.health.lock().unwrap().ws_readers[slot].clone()
    }

    fn live_count(&self) -> usize {
        self.health
            .lock()
            .unwrap()
            .ws_live_reader_count(Instant::now())
    }

    /// Source-log contents in order, as trade identifiers.
    fn source_log_ids(&self) -> Vec<String> {
        Reader::replay(&self.source_log)
            .unwrap()
            .map(|item| {
                let (_seq, env) = item.unwrap();
                trade_parser::parse_ws_trade(&env.payload, env.received_at.0)
                    .unwrap()
                    .source_trade_id
                    .0
            })
            .collect()
    }
}

/// Start the production ingest owner over pipes and settle until all three
/// slots have dialed (at the paused clock's t0).
async fn start_pool_with(
    net: Arc<PipeNet>,
    capacity: usize,
) -> (Pool, mpsc::Receiver<IncomingTrade>) {
    start_pool_with_gate(net, capacity, None).await
}

async fn start_pool_with_gate(
    net: Arc<PipeNet>,
    capacity: usize,
    reader_append_gate: Option<Arc<Semaphore>>,
) -> (Pool, mpsc::Receiver<IncomingTrade>) {
    let dir = tempfile::tempdir().unwrap();
    let source_log = dir.path().join("source.log");
    let sink = SourceEventSink::open(&source_log).unwrap();
    let (source_log_handle, source_rx) = SourceLogHandle::channel(capacity);
    let (trigger_tx, mut trigger_rx) = mpsc::channel(capacity);
    let (trade_tx, trade_rx) = mpsc::channel(capacity);
    let health = healthy_ws_health();
    let mut ingest = ActivityIngest::with_dialer(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        sink,
        source_rx,
        trigger_tx,
        health.clone(),
        net.dialer(),
    );
    if let Some(gate) = reader_append_gate {
        ingest = ingest.with_reader_append_gate(gate);
    }
    let replay_path = source_log.clone();
    // Projection for the retained #546 reader-pool scenarios:
    // production emits only reconciliation triggers, while these tests still
    // exercise the old orchestrator seam. Reparse the row only after the source
    // log proves it durable; no production path uses this projection.
    let task = tokio::spawn(async move {
        let _source_log_handle = source_log_handle;
        let ingest = ingest.run();
        tokio::pin!(ingest);
        loop {
            tokio::select! {
                () = trade_tx.closed() => return,
                () = &mut ingest => return,
                trigger = trigger_rx.recv() => {
                    let Some(trigger) = trigger else { return; };
                    let trade = Reader::replay(&replay_path)
                        .unwrap()
                        .filter_map(Result::ok)
                        .find_map(|(_seq, envelope)| {
                            let observation = parse_activity_trade_observation(&envelope.payload).ok()?;
                            if observation.group_id.key() != &trigger.source_trade_id {
                                return None;
                            }
                            trade_parser::parse_ws_trade(&envelope.payload, envelope.received_at.0).ok()
                        })
                        .expect("durable trigger has its raw source row");
                    if trade_tx.send(trade).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    settle().await;
    for slot in 0..3 {
        assert_eq!(net.dial_count(slot), 1, "all three slots dial immediately");
    }
    (
        Pool {
            net,
            health,
            source_log,
            task,
            _dir: dir,
        },
        trade_rx,
    )
}

async fn start_pool(capacity: usize) -> (Pool, mpsc::Receiver<IncomingTrade>) {
    start_pool_with(Arc::new(PipeNet::default()), capacity).await
}

/// Let every runnable task make progress (no time passes under a paused clock).
async fn settle() {
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

/// Yield until `pred` holds (bounded) — for chains that cross several tasks.
async fn settle_until(mut pred: impl FnMut() -> bool) -> bool {
    for _ in 0..4_000 {
        if pred() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    pred()
}

async fn advance(d: Duration) {
    tokio::time::advance(d).await;
    settle().await;
}

fn wall_now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

// ── R1: only normalized rows refresh liveness; exact drop; backoff schedule ───

#[tokio::test(start_paused = true)]
async fn r1_only_normalized_rows_refresh_liveness_drop_is_exact_and_backoff_follows() {
    let (pool, _trade_rx) = start_pool(64).await;
    let mut server = pool.net.take_server(0);

    let non_refreshing: Vec<String> = vec![
        String::new(),                                             // empty keepalive
        r#"{"status":"subscribed"}"#.to_string(),                  // acknowledgement
        "pong".to_string(),                                        // keepalive/heartbeat text
        r#"{"topic":"comments","type":"new","payload":{}}"#.to_string(), // unrelated topic
        "{not json".to_string(),                                   // envelope parse error
        r#"{"topic":"activity","type":"trades"}"#.to_string(),     // missing payload
        // Payloads rejected by the current production normalizer:
        activity_frame(&[
            r#"{"proxyWallet":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","conditionId":"0xc","side":"BUY","size":"1","price":"0.5","timestamp":"1704067200"}"#.to_string(),
        ]), // missing required field (transactionHash)
        activity_frame(&[payload("0xr1", "nonsense", &market(), 1_704_067_200)]), // invalid wallet
        activity_frame(&[payload("0xr2", LEADER, &market(), 1_704_067_200).replace("\"BUY\"", "\"HOLD\"")]), // invalid side
        activity_frame(&[payload("0xr3", LEADER, &market(), 1_704_067_200).replace("\"1704067200\"", "\"soon\"")]), // invalid timestamp
        activity_frame(&[payload("0xr4", LEADER, &market(), 1_704_067_200).replace("\"size\":\"100\"", "\"size\":\"99999999999999999999999999\"")]), // size beyond u64
    ];
    let mut elapsed = Duration::ZERO;
    for input in &non_refreshing {
        advance(Duration::from_secs(1)).await;
        elapsed += Duration::from_secs(1);
        server.send_text(input).await.unwrap();
        settle().await;
        let r = pool.reader(0);
        assert!(r.connected, "{input:?}: still connected");
        assert!(
            r.last_wire_frame_at.is_some(),
            "{input:?}: wire health advances"
        );
        assert!(
            r.last_normalized_activity_at.is_none(),
            "{input:?}: must not refresh normalized liveness"
        );
        assert_eq!(r.normalized_activity_rows_total, 0, "{input:?}");
        assert!(!r.is_live(Instant::now()), "{input:?}: never live");
    }

    // 29.999 s after connect: still holding the socket.
    advance(Duration::from_millis(29_999) - elapsed).await;
    assert!(pool.reader(0).connected);
    assert_eq!(pool.net.dial_count(0), 1);
    // Exactly 30 s: dropped without a close handshake; the re-dial waits 1 s.
    advance(Duration::from_millis(1)).await;
    assert!(!pool.reader(0).connected, "drops at the exact 30s boundary");
    assert!(
        server.recv_text().await.is_none(),
        "the peer sees end-of-stream"
    );
    assert_eq!(pool.net.dial_count(0), 1);
    advance(Duration::from_millis(999)).await;
    assert_eq!(
        pool.net.dial_count(0),
        1,
        "backoff is 1s after the first failure"
    );
    advance(Duration::from_millis(1)).await;
    assert_eq!(pool.net.dial_count(0), 2);
    assert_eq!(pool.reader(0).consecutive_reconnects, 1);
    assert!(pool.reader(0).connected);
    assert!(
        pool.reader(0).last_normalized_activity_at.is_none(),
        "a new connection is not live until its first normalized row"
    );

    // Second cycle: a parser-accepted frame becomes ready at the exact deadline
    // instant. The frame wins, refreshes liveness, and resets the backoff.
    let mut server = pool.net.take_server(0);
    server
        .send_text(&activity_frame(&[payload(
            "0xtie",
            STRANGER,
            &market_b(),
            1_704_067_200,
        )]))
        .await
        .unwrap();
    advance(Duration::from_secs(30)).await;
    let r = pool.reader(0);
    assert!(r.connected, "a ready frame keeps the socket connected");
    assert_eq!(
        r.normalized_activity_rows_total, 1,
        "a frame ready at the deadline is consumed before expiry"
    );
    assert!(r.last_normalized_activity_at.is_some());
    assert_eq!(r.consecutive_reconnects, 0);
    advance(Duration::from_millis(29_999)).await;
    assert!(pool.reader(0).connected);
    assert_eq!(pool.net.dial_count(0), 2);
    advance(Duration::from_millis(1)).await;
    assert!(!pool.reader(0).connected);
    assert!(server.recv_text().await.is_none());
    advance(Duration::from_millis(999)).await;
    assert_eq!(pool.net.dial_count(0), 2);
    advance(Duration::from_millis(1)).await;
    assert_eq!(pool.net.dial_count(0), 3);
    assert_eq!(pool.reader(0).consecutive_reconnects, 1);
    pool.task.abort();
}

#[tokio::test(start_paused = true)]
async fn buffered_unwatched_frames_at_deadline_credit_all_three_readers_before_drop() {
    let (pool, _trade_rx) = start_pool(64).await;
    let mut servers: Vec<ActivityWsPeer> = (0..3).map(|slot| pool.net.take_server(slot)).collect();

    for (slot, server) in servers.iter_mut().enumerate() {
        server
            .send_text(&activity_frame(&[payload(
                &format!("0xprime-{slot}"),
                STRANGER,
                &market_b(),
                now_unix(),
            )]))
            .await
            .unwrap();
    }
    settle().await;
    for slot in 0..3 {
        assert_eq!(pool.reader(slot).normalized_activity_rows_total, 1);
        assert!(pool.reader(slot).is_live(Instant::now()));
    }

    advance(Duration::from_millis(29_999)).await;
    for (slot, server) in servers.iter_mut().enumerate() {
        server
            .send_text(&activity_frame(&[payload(
                &format!("0xdeadline-{slot}"),
                STRANGER,
                &market_b(),
                now_unix(),
            )]))
            .await
            .unwrap();
    }
    advance(Duration::from_millis(1)).await;

    for slot in 0..3 {
        let reader = pool.reader(slot);
        assert!(reader.connected, "reader {slot} keeps its socket");
        assert_eq!(reader.normalized_activity_rows_total, 2);
        assert!(reader.is_live(Instant::now()));
        assert_eq!(pool.net.dial_count(slot), 1);
    }
    pool.task.abort();
}

#[tokio::test(start_paused = true)]
async fn buffered_acknowledgements_at_deadline_drop_all_three_silent_readers() {
    let (pool, _trade_rx) = start_pool(64).await;
    let mut servers: Vec<ActivityWsPeer> = (0..3).map(|slot| pool.net.take_server(slot)).collect();

    for (slot, server) in servers.iter_mut().enumerate() {
        server
            .send_text(&activity_frame(&[payload(
                &format!("0xprime-{slot}"),
                STRANGER,
                &market_b(),
                now_unix(),
            )]))
            .await
            .unwrap();
    }
    settle().await;
    let credited_at: Vec<Instant> = (0..3)
        .map(|slot| pool.reader(slot).last_normalized_activity_at.unwrap())
        .collect();

    advance(Duration::from_millis(29_999)).await;
    for server in &mut servers {
        server
            .send_text(r#"{"status":"subscribed"}"#)
            .await
            .unwrap();
    }
    advance(Duration::from_millis(1)).await;

    for (slot, credited_at) in credited_at.into_iter().enumerate() {
        let reader = pool.reader(slot);
        assert!(!reader.connected, "reader {slot} drops genuine silence");
        assert_eq!(reader.normalized_activity_rows_total, 1);
        assert_eq!(reader.last_normalized_activity_at, Some(credited_at));
        assert_eq!(reader.last_wire_frame_at, Some(Instant::now()));
        assert_eq!(pool.net.dial_count(slot), 1);
    }
    pool.task.abort();
}

// ── R2: normalize once, deliver only the watched row, unwatched rows keep liveness ──

#[tokio::test(start_paused = true)]
async fn r2_mixed_frame_delivers_watched_row_once_and_unwatched_rows_only_refresh_liveness() {
    let (pool, mut trade_rx) = start_pool(64).await;
    let mut server = pool.net.take_server(0);
    advance(Duration::from_secs(29)).await; // one second from the deadline

    let frame = activity_frame(&[
        payload("0xwatched", LEADER, &market(), now_unix()),
        payload("0xstranger", STRANGER, &market_b(), now_unix()),
        payload("0xbad", LEADER, &market(), now_unix()).replace("\"BUY\"", "\"HOLD\""),
    ]);
    server.send_text(&frame).await.unwrap();
    settle().await;

    let delivered = trade_rx.try_recv().expect("the watched row is delivered");
    assert_eq!(delivered.source_trade_id.0, "0xwatched");
    assert_eq!(delivered.wallet, leader_wallet());
    assert_eq!(delivered.provenance, TradeProvenance::ActivityWs);
    assert!(
        trade_rx.try_recv().is_err(),
        "unwatched and rejected rows are never delivered"
    );
    let r = pool.reader(0);
    assert_eq!(
        r.normalized_activity_rows_total, 2,
        "two payloads passed the normalizer, one was rejected"
    );
    assert!(r.is_live(Instant::now()));
    assert_eq!(pool.source_log_ids(), vec!["0xwatched".to_string()]);

    // The frame refreshed the deadline: 29 more silent seconds keep the socket.
    advance(Duration::from_secs(29)).await;
    assert!(pool.reader(0).connected);
    // A parser-accepted UNWATCHED row alone refreshes liveness (a quiet watched
    // cohort must not look like a quiet platform) and delivers nothing.
    server
        .send_text(&activity_frame(&[payload(
            "0xstranger2",
            STRANGER,
            &market_b(),
            now_unix(),
        )]))
        .await
        .unwrap();
    settle().await;
    advance(Duration::from_secs(29)).await;
    assert!(pool.reader(0).connected);
    assert_eq!(pool.reader(0).normalized_activity_rows_total, 3);
    assert!(trade_rx.try_recv().is_err());
    assert_eq!(pool.source_log_ids().len(), 1);
    advance(Duration::from_secs(1)).await;
    assert!(!pool.reader(0).connected, "30s after the last accepted row");
    pool.task.abort();
}

#[tokio::test(start_paused = true)]
async fn unwatched_accepted_rows_alone_keep_reader_live_beyond_thirty_seconds() {
    let (pool, mut trade_rx) = start_pool(8).await;
    let mut server = pool.net.take_server(0);
    for tx in ["0xstranger-a", "0xstranger-b", "0xstranger-c"] {
        advance(Duration::from_secs(20)).await;
        server
            .send_text(&activity_frame(&[payload(
                tx,
                STRANGER,
                &market_b(),
                now_unix(),
            )]))
            .await
            .unwrap();
        settle().await;
        assert!(pool.reader(0).connected);
    }
    assert_eq!(pool.net.dial_count(0), 1);
    assert_eq!(pool.reader(0).normalized_activity_rows_total, 3);
    assert!(trade_rx.try_recv().is_err());
    assert!(pool.source_log_ids().is_empty());
    pool.task.abort();
}

// ── R4: one silent slot never interrupts delivery; one decision per identifier ──

#[tokio::test(start_paused = true)]
async fn r4_one_silent_reader_cannot_interrupt_delivery_to_one_decision() {
    let (pool, trade_rx) = start_pool(64).await;
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    // Fixed admission clock: the two unique acknowledged decisions each sample once.
    let t = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
    let hooks = Arc::new(ScenarioHooks::default());
    hooks.age_clock.lock().unwrap().extend([t, t]);
    let (orch, control_tx) = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        pool.health.clone(),
        OrchOpts {
            hooks: Some(Arc::clone(&hooks)),
            ..OrchOpts::ws(vec![market(), market_b()])
        },
    );
    let forward = forward_trade_buckets(trade_rx, control_tx);
    let (orch_task, shutdown) = spawn_orchestrator(orch);
    let mut s0 = pool.net.take_server(0);
    let mut s1 = pool.net.take_server(1); // silent forever
    let mut s2 = pool.net.take_server(2);

    advance(Duration::from_secs(1)).await;
    let observed_unix = t.unix_timestamp() - 2;
    let frame = activity_frame(&[payload("0xsame", LEADER, &market(), observed_unix)]);
    s0.send_text(&frame).await.unwrap();
    s2.send_text(&frame).await.unwrap();
    assert!(
        settle_until(|| { paper_state.is_seen(&bucket_id("0xsame", market())).unwrap() }).await,
        "two reader copies reach one decision with no polling input"
    );
    // A distinct identifier delivered by one reader only is also copied.
    s2.send_text(&activity_frame(&[payload(
        "0xother",
        LEADER,
        &market_b(),
        observed_unix + 1,
    )]))
    .await
    .unwrap();
    assert!(
        settle_until(|| {
            paper_state
                .is_seen(&bucket_id("0xother", market_b()))
                .unwrap()
        })
        .await
    );
    assert!(
        paper_state
            .no_copy_disposition(&bucket_id("0xother", market_b()))
            .unwrap()
            .is_none(),
        "fresh pre-Start refusal is not a stale no-copy"
    );
    assert_eq!(paper_fill_count(dir.path()), 0);
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(100)),
        "one leader delta"
    );
    assert_eq!(
        leader_long(&paper_state, &market_b()),
        Some(whole_shares(100))
    );
    assert!(
        hooks.age_clock.lock().unwrap().is_empty(),
        "two unique acknowledged decisions sample once"
    );

    // Keep 0 and 2 alive past slot 1's deadline with parser-accepted unwatched rows.
    advance(Duration::from_secs(24)).await; // t = 25 s
    for s in [&mut s0, &mut s2] {
        s.send_text(&activity_frame(&[payload(
            "0xnoise",
            STRANGER,
            &market_b(),
            now_unix(),
        )]))
        .await
        .unwrap();
    }
    settle().await;
    advance(Duration::from_secs(5)).await; // t = 30 s: slot 1 drops
    assert!(!pool.reader(1).connected);
    assert!(s1.recv_text().await.is_none());
    assert_eq!(pool.net.dial_count(1), 1);
    advance(Duration::from_secs(1)).await; // t = 31 s: slot 1 re-dials alone
    assert_eq!(pool.net.dial_count(1), 2);
    assert_eq!(pool.net.dial_count(0), 1);
    assert_eq!(pool.net.dial_count(2), 1);
    assert_eq!(
        pool.live_count(),
        2,
        "two live readers, one reconnected-not-live"
    );
    {
        let h = pool.health.lock().unwrap();
        assert!(
            readiness_issues(&h, wall_now(), Instant::now())
                .iter()
                .all(|i| !i.starts_with("activity_ws")),
            "two live readers is healthy"
        );
    }
    let mut ids = pool.source_log_ids();
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "0xother".to_string(),
            "0xsame".to_string(),
            "0xsame".to_string()
        ],
        "every watched reader copy is raw evidence; unwatched rows are not recorded"
    );
    shutdown.send(()).unwrap();
    orch_task.await.unwrap();
    pool.task.abort();
    forward.abort();
}

// ── R5: independent per-slot backoff ─────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn r5_each_reader_has_independent_reconnect_backoff() {
    let net = Arc::new(PipeNet::default());
    net.failing.lock().unwrap().insert(1);
    let (pool, _trade_rx) = start_pool_with(Arc::clone(&net), 64).await;
    let t0 = net.dial_times(1)[0];
    let mut s0 = net.take_server(0);
    assert!(pool.reader(0).connected && !pool.reader(1).connected);

    // Slot 1: 1 s, 2 s, 4 s ladder; slots 0 and 2 keep their first connection.
    advance(Duration::from_secs(1)).await;
    assert_eq!(net.dial_times(1), vec![t0, t0 + Duration::from_secs(1)]);
    advance(Duration::from_secs(2)).await;
    assert_eq!(net.dial_times(1).len(), 3);
    assert_eq!(net.dial_times(1)[2], t0 + Duration::from_secs(3));
    // Progress on slot 0 must not reset slot 1's ladder.
    s0.send_text(&activity_frame(&[payload(
        "0xnoise",
        STRANGER,
        &market_b(),
        now_unix(),
    )]))
    .await
    .unwrap();
    settle().await;
    assert_eq!(pool.reader(0).consecutive_reconnects, 0);
    assert_eq!(pool.reader(1).consecutive_reconnects, 3);
    advance(Duration::from_secs(4)).await;
    assert_eq!(net.dial_times(1)[3], t0 + Duration::from_secs(7));
    assert_eq!(pool.reader(1).consecutive_reconnects, 4);
    assert_eq!(net.dial_count(0), 1);
    assert_eq!(net.dial_count(2), 1);

    // Slot 1 recovers on its own schedule: connected, not live until a row.
    net.failing.lock().unwrap().clear();
    advance(Duration::from_secs(8)).await;
    assert_eq!(net.dial_times(1)[4], t0 + Duration::from_secs(15));
    let r1 = pool.reader(1);
    assert!(r1.connected && !r1.is_live(Instant::now()));
    assert_eq!(
        r1.consecutive_reconnects, 4,
        "reset only by a normalized row"
    );
    let mut s1 = net.take_server(1);
    s1.send_text(&activity_frame(&[payload(
        "0xnoise2",
        STRANGER,
        &market_b(),
        now_unix(),
    )]))
    .await
    .unwrap();
    settle().await;
    assert_eq!(pool.reader(1).consecutive_reconnects, 0);
    assert!(pool.reader(1).is_live(Instant::now()));
    pool.task.abort();
}

// ── R6: three byte-identical copies → one decision; source-log replay agrees ──

#[tokio::test(start_paused = true)]
async fn r6_three_reader_copies_yield_one_decision_and_the_source_log_replays_to_the_same() {
    // Fixed admission clock for both runs: the one deduplicated decision samples once.
    let t = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
    let hooks = Arc::new(ScenarioHooks::default());
    hooks.age_clock.lock().unwrap().push_back(t);
    let (pool, trade_rx) = start_pool(64).await;
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let (orch, control_tx) = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        pool.health.clone(),
        OrchOpts {
            hooks: Some(Arc::clone(&hooks)),
            ..OrchOpts::ws(vec![market()])
        },
    );
    let forward = forward_trade_buckets(trade_rx, control_tx);
    let (orch_task, shutdown) = spawn_orchestrator(orch);

    let row = payload("0xthree", LEADER, &market(), t.unix_timestamp() - 1);
    let frame = activity_frame(std::slice::from_ref(&row));
    for slot in 0..3 {
        pool.net.take_server(slot).send_text(&frame).await.unwrap();
    }
    assert!(
        settle_until(|| paper_state
            .is_seen(&bucket_id("0xthree", market()))
            .unwrap()
            && pool.source_log_ids().len() == 3)
        .await
    );
    // Give the third copy time to reach the seen check.
    settle().await;
    settle().await;

    let assert_one_decision = |state: &PaperStateDb, paper_dir: &Path| {
        assert!(
            state.is_seen(&bucket_id("0xthree", market())).unwrap(),
            "one committed seen row"
        );
        assert_eq!(
            leader_long(state, &market()),
            Some(whole_shares(100)),
            "one leader delta"
        );
        assert_eq!(
            state.fills_count().unwrap(),
            0,
            "pre-Start classification cannot create a financial fill"
        );
        assert_eq!(paper_fill_count(paper_dir), 0);
        let mut seeds = state.pending_dispatch_seeds().unwrap();
        seeds.extend(state.unfinalized_ready_dispatch_seeds().unwrap());
        assert_eq!(
            seeds.len(),
            0,
            "pre-Start classification cannot stage live dispatch"
        );
        assert!(
            state
                .no_copy_disposition(&bucket_id("0xthree", market()))
                .unwrap()
                .is_none(),
            "fresh pre-Start refusal is not a stale no-copy"
        );
    };
    assert_one_decision(&paper_state, dir.path());
    assert!(
        hooks.age_clock.lock().unwrap().is_empty(),
        "one deduplicated decision sample"
    );
    shutdown.send(()).unwrap();
    orch_task.await.unwrap();
    pool.task.abort();
    forward.abort();
    settle().await;

    // Replay: three ordered envelopes, each byte-identical raw row with its own receipt.
    let envelopes: Vec<_> = Reader::replay(&pool.source_log)
        .unwrap()
        .map(|item| item.unwrap())
        .collect();
    assert_eq!(envelopes.len(), 3, "three raw observations retained");
    let reference =
        trade_parser::parse_ws_trade(row.as_bytes(), envelopes[0].1.received_at.0).unwrap();
    let mut replayed = Vec::new();
    for (i, (_seq, env)) in envelopes.iter().enumerate() {
        assert_eq!(env.source_id.0, ACTIVITY_WS_SOURCE_ID);
        assert_eq!(
            env.schema_version,
            pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION
        );
        assert_eq!(
            env.parser_version,
            pe_source_polymarket_public::ACTIVITY_PARSER_VERSION
        );
        assert_eq!(
            env.payload,
            row.as_bytes(),
            "envelope {i}: exact raw row bytes"
        );
        assert_eq!(env.content_type, ContentType::Json);
        assert_eq!(
            env.observed_at.0, reference.observed_at,
            "envelope {i}: observed_at"
        );
        assert_eq!(
            env.raw_payload_hash,
            blake3::hash(row.as_bytes()),
            "envelope {i}: raw payload hash"
        );
        let t = trade_parser::parse_ws_trade(&env.payload, env.received_at.0).unwrap();
        assert_eq!(
            t.received_at, env.received_at.0,
            "envelope {i}: its own receipt time"
        );
        assert_eq!(t.source_trade_id, reference.source_trade_id);
        assert_eq!(t.wallet, reference.wallet);
        assert_eq!(t.market_id.0.0, reference.market_id.0.0);
        assert_eq!(t.outcome_id, reference.outcome_id);
        assert_eq!(t.side, reference.side);
        assert_eq!(t.price, reference.price);
        assert_eq!(t.contracts, reference.contracts);
        assert_eq!(t.observed_at, reference.observed_at);
        assert_eq!(t.provenance, TradeProvenance::ActivityWs);
        replayed.push(t);
    }

    // Fresh state: replay reproduces the single acknowledged decision.
    let dir2 = tempfile::tempdir().unwrap();
    let paper2 = Arc::new(PaperStateDb::open(&dir2.path().join("p.db")).unwrap());
    assert_eq!(paper2.fills_count().unwrap(), 0);
    assert!(!paper2.is_seen(&bucket_id("0xthree", market())).unwrap());
    assert!(paper2.pending_dispatch_seeds().unwrap().is_empty());
    let hooks2 = Arc::new(ScenarioHooks::default());
    hooks2.age_clock.lock().unwrap().push_back(t);
    run_trades(
        dir2.path(),
        paper2.clone(),
        healthy_ws_health(),
        OrchOpts {
            hooks: Some(Arc::clone(&hooks2)),
            ..OrchOpts::ws(vec![market()])
        },
        replayed,
    )
    .await;
    assert_one_decision(&paper2, dir2.path());
    assert!(hooks2.age_clock.lock().unwrap().is_empty());
}

// ── R7: delayed append saturation refreshes from post-delivery time ─────────

#[tokio::test(start_paused = true)]
async fn r7_full_fan_in_drains_after_thirty_seconds_without_dropping_socket() {
    let append_gate = Arc::new(Semaphore::new(0));
    let (pool, _trade_rx) = start_pool_with_gate(
        Arc::new(PipeNet::default()),
        1,
        Some(Arc::clone(&append_gate)),
    )
    .await;
    let mut server = pool.net.take_server(0);
    let rows: Vec<String> = ["0xa", "0xb", "0xc"]
        .iter()
        .map(|tx| payload(tx, LEADER, &market(), now_unix()))
        .collect();
    server.send_text(&activity_frame(&rows)).await.unwrap();
    assert!(
        settle_until(|| pool.reader(0).fan_in_blocked).await,
        "the frame blocks behind the delayed first append"
    );
    let blocked_at = Instant::now();
    assert!(pool.reader(0).connected);
    assert_eq!(pool.reader(0).normalized_activity_rows_total, 0);
    assert!(pool.source_log_ids().is_empty());

    advance(Duration::from_secs(31)).await;
    let r = pool.reader(0);
    assert!(r.connected && r.fan_in_blocked);
    assert!(!r.is_live(Instant::now()));
    assert!(r.last_normalized_activity_at.unwrap() < blocked_at + Duration::from_secs(31));
    assert_eq!(pool.net.dial_count(0), 1, "no reconnect while blocked");

    append_gate.add_permits(rows.len());
    assert!(
        settle_until(|| pool.reader(0).normalized_activity_rows_total == 3).await,
        "the retained frame drains after append resumes"
    );
    let drained = pool.reader(0);
    assert!(drained.connected);
    assert!(!drained.fan_in_blocked);
    assert!(drained.is_live(Instant::now()));
    assert!(
        drained.last_normalized_activity_at.unwrap() >= blocked_at + Duration::from_secs(31),
        "watched liveness uses a fresh instant after every fan-in delivery succeeds"
    );
    assert_eq!(
        pool.source_log_ids(),
        vec!["0xa".to_owned(), "0xb".to_owned(), "0xc".to_owned()],
        "retained rows landed in order without eviction"
    );
    advance(Duration::from_secs(29)).await;
    assert!(pool.reader(0).connected);
    assert_eq!(pool.net.dial_count(0), 1);
    pool.task.abort();
}

// ── R8: strict budget for both provenances at the early gate (fixed clock) ───

#[tokio::test]
async fn r8_early_gate_applies_the_strict_budget_to_both_provenances() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let t = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
    let hooks = Arc::new(ScenarioHooks::default());
    // Each acknowledged pending decision consumes one final staleness sample.
    hooks.age_clock.lock().unwrap().extend([
        t - time::Duration::seconds(1),
        t,
        t - time::Duration::seconds(1),
        t,
    ]);
    run_trades(
        dir.path(),
        paper_state.clone(),
        healthy_ws_health(),
        OrchOpts {
            hooks: Some(Arc::clone(&hooks)),
            ..OrchOpts::ws(vec![market(), market_b(), market_c(), market_d()])
        },
        vec![
            trade_at(
                "0xrest60",
                market_b(),
                t - time::Duration::seconds(61),
                TradeProvenance::RestPoll,
            ),
            trade_at(
                "0xws60",
                market(),
                t - time::Duration::seconds(60),
                TradeProvenance::ActivityWs,
            ),
            // Exact boundary (age == budget) stays eligible for both provenances.
            trade_at(
                "0xrestedge",
                market_d(),
                t - time::Duration::seconds(3),
                TradeProvenance::RestPoll,
            ),
            trade_at(
                "0xwsedge",
                market_c(),
                t - time::Duration::seconds(2),
                TradeProvenance::ActivityWs,
            ),
        ],
    )
    .await;
    assert!(
        hooks.age_clock.lock().unwrap().is_empty(),
        "every check sampled once"
    );
    // Both boundary rows are fresh but fail closed at the pre-Start financial boundary.
    assert_eq!(
        paper_fill_count(dir.path()),
        0,
        "pre-Start classification never writes a financial fill"
    );
    assert_eq!(
        paper_state
            .no_copy_disposition(&bucket_id("0xws60", market()))
            .unwrap()
            .unwrap(),
        (
            "activity_ws".to_string(),
            60,
            "stale_activity_ws_past_copy_budget".to_string()
        )
    );
    assert_eq!(
        paper_state
            .no_copy_disposition(&bucket_id("0xrest60", market_b()))
            .unwrap()
            .unwrap(),
        (
            "rest_poll".to_string(),
            60,
            "stale_fallback_past_copy_budget".to_string()
        )
    );
    for (transaction_hash, market_id) in [
        ("0xws60", market()),
        ("0xrest60", market_b()),
        ("0xwsedge", market_c()),
        ("0xrestedge", market_d()),
    ] {
        assert!(
            paper_state
                .is_seen(&bucket_id(transaction_hash, market_id))
                .unwrap(),
            "{transaction_hash} seen"
        );
    }
    for (transaction_hash, market_id) in [("0xwsedge", market_c()), ("0xrestedge", market_d())] {
        assert!(
            paper_state
                .no_copy_disposition(&bucket_id(transaction_hash, market_id))
                .unwrap()
                .is_none(),
            "fresh pre-Start refusal is not a stale no-copy"
        );
    }
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(100)),
        "stale rows still mirror the leader"
    );
}

// ── R9: fresh early, stale before staging; commit fault rolls back cleanly ───

#[tokio::test]
async fn r9_stale_acknowledged_bucket_commits_no_copy() {
    let t = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();

    // A committed continuation that is stale at its final admission check records no-copy.
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let hooks = Arc::new(ScenarioHooks::default());
    hooks
        .age_clock
        .lock()
        .unwrap()
        .push_back(t + time::Duration::seconds(5));
    run_trades(
        dir.path(),
        paper_state.clone(),
        healthy_ws_health(),
        OrchOpts {
            hooks: Some(Arc::clone(&hooks)),
            ..OrchOpts::ws(vec![market()])
        },
        vec![
            trade_at(
                "0xlate",
                market(),
                t - time::Duration::seconds(1),
                TradeProvenance::ActivityWs,
            ),
            trade_at("0xsecond", market(), t, TradeProvenance::ActivityWs),
        ],
    )
    .await;
    assert!(hooks.age_clock.lock().unwrap().is_empty());
    assert!(paper_state.is_seen(&bucket_id("0xlate", market())).unwrap());
    assert_eq!(
        paper_state
            .no_copy_disposition(&bucket_id("0xlate", market()))
            .unwrap()
            .unwrap(),
        (
            "activity_ws".to_string(),
            6,
            "stale_activity_ws_past_copy_budget".to_string()
        )
    );
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(200)),
        "both rows mirror the leader"
    );
    assert_eq!(
        paper_fill_count(dir.path()),
        0,
        "no fill, no seed, no target"
    );
    assert!(paper_state.pending_dispatch_seeds().unwrap().is_empty());
    // The same-session entry was retained: the second entry into this market is
    // NotFirstEntry (seen, no disposition, no fill).
    assert!(
        paper_state
            .is_seen(&bucket_id("0xsecond", market()))
            .unwrap()
    );
    assert!(
        paper_state
            .is_seen(&bucket_id("0xsecond", market()))
            .unwrap()
    );
}

/// PASS: receipt-scoped V3 observation resolution advances the deterministic clock from an age of
/// 1,999 ms to 2,001 ms before the final sample, so no live dispatch target is staged.
/// FAIL: the final age sample precedes observation resolution and the armed account receives a
/// dispatch seed.
#[tokio::test]
async fn r9_observation_resolution_precedes_the_final_dispatch_age_sample() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let paper_path = dir.path().join("paper.log");
    let state_path = dir.path().join("paper.db");
    let observed_at = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
    let condition = market();
    let token_id = format!("{condition}-0");
    let activity = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": leader_wallet().to_string(),
        "timestamp": observed_at.unix_timestamp(),
        "conditionId": condition.to_string(),
        "type": "TRADE",
        "size": "100",
        "usdcSize": "50",
        "transactionHash": "0xresolution-delay",
        "price": "0.50",
        "asset": token_id,
        "side": "BUY",
        "outcomeIndex": 0,
        "outcome": "Yes",
        "isCombo": false
    }]))
    .unwrap();
    let mut source_writer = Writer::open(&source_path).unwrap();
    let (read, commitment_receipt) = support::append_committed_read_v1(
        &mut source_writer,
        leader_wallet(),
        &activity,
        observed_at.unix_timestamp(),
        observed_at.unix_timestamp(),
    );
    let page_receipt = read.page.receipt;
    drop(source_writer);
    let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();

    let paper_state = Arc::new(PaperStateDb::open(&state_path).unwrap());
    paper_state
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: leader_wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: observed_at.unix_timestamp(),
        })
        .unwrap();
    support::install_empty_anchor(&paper_state, leader_wallet(), 0);

    let mut runtime = RuntimeConfig::from_service_config(&ServiceConfig::default());
    runtime.mode = "paper".to_owned();
    runtime.max_resolution_horizon_secs = 0;
    runtime.min_resolution_horizon_secs = 0;
    runtime.price_impact_cap_bps = 100;
    runtime.sizing_mode = SizingMode::Dollar { usd: dec!(10) };
    let mut paper_writer = Writer::open(&paper_path).unwrap();
    let start_at = observed_at - time::Duration::seconds(1);
    let paper_prefix = TailBinding::from(&Scanner::verify(&paper_path).unwrap());
    let source_prefix = TailBinding::from(&Scanner::verify(&source_path).unwrap());
    let start = paper_writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: PAPER_LOG_SCHEMA_VERSION,
            parser_version: 1,
            observed_at: SourceTimestamp(start_at),
            received_at: ReceivedAt(start_at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::QualificationStarted(Box::new(
                QualificationStarted {
                    starting_bankroll: CollateralAmount::from_decimal_exact(dec!(10_000)).unwrap(),
                    paper_prefix,
                    source_prefix,
                    live_prefix: TailBinding {
                        physical_tail: 0,
                        last_sequence: None,
                        last_hash: "00".repeat(32),
                    },
                    artifact_blake3: "scenario".to_owned(),
                    static_config_hash: "scenario".to_owned(),
                    hot_config_hash: runtime.canonical_hash(),
                    generation: "scenario".to_owned(),
                    activation_id: "scenario".to_owned(),
                    ranking_batch_id: 545,
                    membership: vec![leader_wallet()],
                    membership_proofs_hash: "scenario".to_owned(),
                    schema_version: 3,
                    parser_version: 1,
                    financial_semantic_version: 1,
                },
            )))
            .unwrap(),
        })
        .unwrap();
    paper_state
        .reset_financial_era(
            start,
            CollateralAmount::from_decimal_exact(dec!(10_000)).unwrap(),
        )
        .unwrap();

    let mut accounts = LiveAccountsSnapshot::from_rows(
        vec![AccountRow {
            account_id: "latency-test".to_owned(),
            is_primary: true,
            enabled: true,
            execution_order: 0,
            requested_live_mode: "live_tiny".to_owned(),
            effective_live_mode: "live_tiny".to_owned(),
            live_price_impact_cap_bps: 100,
            custody_wallet_address: None,
            custody_wallet_kind: None,
        }],
        &[CredentialMetaRow {
            account_id: "latency-test".to_owned(),
            bundle_version: 1,
            key_id: "latency-key".to_owned(),
        }],
    );
    accounts.fetched_at_unix = Some(OffsetDateTime::now_utc().unix_timestamp());

    let admission = LiveAdmissionArtifact {
        market: LiveMarketEvidence {
            condition_id: PolymarketConditionId(condition.to_string()),
            ordered_outcome_token_ids: [
                PolymarketTokenId(token_id.clone()),
                PolymarketTokenId(format!("{condition}-1")),
            ],
            neg_risk: false,
            minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
            minimum_order_size: ShareAmount::from_whole(1).unwrap(),
            scheduled_end_unix: None,
            observed_at_unix: observed_at.unix_timestamp(),
            schema_version: pe_source_polymarket_public::LIVE_MARKET_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::LIVE_MARKET_PARSER_VERSION,
            freshness_window_secs: 60,
        },
        settlement: VenueSettlementRecord {
            schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: PolymarketConditionId(condition.to_string()),
            status: VenueResolutionStatus::Unresolved,
            raw_evidence_hash: "scenario".to_owned(),
            source_timestamp_unix: None,
            observed_at_unix: observed_at.unix_timestamp(),
            parser_version: 1,
            freshness_window_secs: 60,
        },
        fee_schedule: CompactFeeSchedule::Zero,
        receipts: AdmissionReceipts {
            gamma: page_receipt,
            clob_long: page_receipt,
            clob_compact: page_receipt,
        },
    };
    let hooks = Arc::new(ScenarioHooks::default());
    hooks.age_clock.lock().unwrap().extend([
        observed_at + time::Duration::milliseconds(1_999),
        observed_at + time::Duration::milliseconds(1_999),
    ]);
    hooks
        .observation_resolution_advance_millis
        .store(2, std::sync::atomic::Ordering::SeqCst);
    hooks
        .admission_artifacts
        .lock()
        .unwrap()
        .push_back(admission);

    let book = OrderBook {
        asks: vec![BookLevel {
            price: dec!(0.50),
            size: dec!(10_000),
        }],
        response_blake3: "scenario-book".to_owned(),
        fetched_at_ms: 0,
        source_receipt: Some(page_receipt),
    };
    let (control_tx, control_rx) = mpsc::channel(4);
    let (source_log, _source_rx) = SourceLogHandle::channel(4);
    let http = reqwest::Client::new();
    let mut orchestrator = Orchestrator::new(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            bankroll: dec!(10_000),
            mode: ExecutionMode::Paper,

            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            min_fill_price: Decimal::ZERO,
            price_impact_cap_bps: 100,
            entry_gate_config: disabled_entry_gate(),
            runtime_config: None,
            live_accounts: Some(LiveAccounts::new(accounts)),
            activity_ws_enabled: true,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
        },
        WinnerFollowStrategy::new(runtime.winner_follow_config()),
        paper_writer,
        Arc::clone(&paper_state),
        build_leader_ledger(&paper_state).unwrap(),
        healthy_ws_health(),
        mid_cache_for(std::slice::from_ref(&condition), "0.50"),
        control_rx,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(HashMap::from([(
            token_id, book,
        )]))),
    )
    .unwrap();
    orchestrator.set_scenario_hooks(Arc::clone(&hooks));
    orchestrator
        .configure_financial_log_paths(
            paper_path,
            source_path,
            LiveAdmissionBuilder::new(
                http.clone(),
                "http://unused.invalid",
                "http://unused.invalid",
                source_log.clone(),
            ),
            Arc::new(HistoricalMarkAdapter::new(
                http,
                "http://unused.invalid",
                source_log,
            )),
            source_receipts,
        )
        .unwrap();
    let run = tokio::spawn(orchestrator.run(std::future::pending::<()>()));

    let source_trade_id = read.aggregates[0].group_id.key().clone();
    let aggregates = read.aggregates;
    let decision_inputs_json = read.decision_inputs_json;
    let occurrence = read.page;
    let (committed, acknowledgement) = oneshot::channel();
    control_tx
        .send(
            pe_service::orchestrator_control::OrchestratorControl::CommitActivityBucket {
                aggregates,
                context: Arc::new(BucketDecisionContext {
                    applied_configuration: runtime,
                    decision_inputs_json,
                    page_occurrences: vec![occurrence],
                    observed_source_receipts: HashMap::new(),
                    reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
                    read_commitment: Some(
                        pe_service::bucket_commit::ActivityReadCommitmentReceipt::LegacyV1(
                            commitment_receipt,
                        ),
                    ),

                    signal_config: SignalConfig::default(),
                    copy_eligible: true,
                    bracket_commit: false,
                    recorded_at_unix: observed_at.unix_timestamp(),
                    observation_provenance: HashMap::from([(
                        source_trade_id.clone(),
                        TradeProvenance::RestPoll,
                    )]),
                    no_copy_dispositions: HashMap::new(),
                    identity_overrides: HashMap::new(),
                    identity_unresolved: HashSet::new(),
                    history_status: None,
                }),
                committed,
            },
        )
        .await
        .unwrap();
    acknowledgement.await.unwrap().unwrap();
    drop(control_tx);
    run.await.unwrap();

    assert!(hooks.age_clock.lock().unwrap().is_empty());
    assert_eq!(
        hooks
            .observation_resolution_advance_millis
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert!(paper_state.pending_dispatch_seeds().unwrap().is_empty());
    assert!(
        paper_state
            .unfinalized_ready_dispatch_seeds()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        paper_state
            .no_copy_disposition(&source_trade_id)
            .unwrap()
            .map(|(_, _, reason)| reason),
        Some("stale_fallback_past_copy_budget".to_owned())
    );
}

#[derive(Clone)]
struct WrongMarketBookLoopback {
    expected_token: String,
    wrong_condition: String,
    book_requests: Arc<std::sync::atomic::AtomicUsize>,
}

async fn wrong_market_book_response(
    State(state): State<WrongMarketBookLoopback>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    assert_eq!(
        query.get("token_id"),
        Some(&state.expected_token),
        "runtime fetch requests the admitted outcome token"
    );
    state
        .book_requests
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    Json(serde_json::json!({
        "market": state.wrong_condition,
        "asset_id": state.expected_token,
        "asks": [{"price": "0.50", "size": "10000"}]
    }))
}

/// PASS: the production `/book` fetch receives the admitted token with a usable ask ladder under
/// a different market identity, fails closed, and creates no dispatch seed or paper preparation.
/// FAIL: token-only validation admits the substituted condition or any downstream dispatch work.
#[tokio::test]
async fn clob_book_wrong_market_with_right_asset_stops_before_dispatch_or_preparation() {
    let condition = market();
    let token_id = format!("{condition}-0");
    let book_requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let loopback_state = WrongMarketBookLoopback {
        expected_token: token_id.clone(),
        wrong_condition: market_b().to_string(),
        book_requests: Arc::clone(&book_requests),
    };
    let app = Router::new()
        .route("/book", get(wrong_market_book_response))
        .with_state(loopback_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let paper_path = dir.path().join("paper.log");
    let state_path = dir.path().join("paper.db");
    let observed_at = OffsetDateTime::now_utc();
    let activity = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": leader_wallet().to_string(),
        "timestamp": observed_at.unix_timestamp(),
        "conditionId": condition.to_string(),
        "type": "TRADE",
        "size": "100",
        "usdcSize": "50",
        "transactionHash": "0xwrong-market-book",
        "price": "0.50",
        "asset": token_id,
        "side": "BUY",
        "outcomeIndex": 0,
        "outcome": "Yes",
        "isCombo": false
    }]))
    .unwrap();
    let mut source_writer = Writer::open(&source_path).unwrap();
    let (read, commitment_receipt) = support::append_committed_read_v1(
        &mut source_writer,
        leader_wallet(),
        &activity,
        observed_at.unix_timestamp(),
        observed_at.unix_timestamp(),
    );
    let page_receipt = read.page.receipt;
    drop(source_writer);
    let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
    let source_sink = SourceEventSink::open(&source_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(8);
    let (trigger_tx, _trigger_rx) = mpsc::channel(1);
    let source_coordinator = tokio::spawn(
        ActivityIngest::poll_only(
            source_sink,
            source_rx,
            trigger_tx,
            new_shared_health_with_ws(false, true, 90),
        )
        .with_source_receipt_index(source_receipts.clone())
        .run(),
    );

    let paper_state = Arc::new(PaperStateDb::open(&state_path).unwrap());
    paper_state
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: leader_wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: observed_at.unix_timestamp(),
        })
        .unwrap();
    support::install_empty_anchor(&paper_state, leader_wallet(), 0);
    let mut runtime = RuntimeConfig::from_service_config(&ServiceConfig::default());
    runtime.mode = "paper".to_owned();
    runtime.max_resolution_horizon_secs = 0;
    runtime.min_resolution_horizon_secs = 0;
    runtime.price_impact_cap_bps = 100;
    runtime.sizing_mode = SizingMode::Dollar { usd: dec!(10) };

    let mut paper_writer = Writer::open(&paper_path).unwrap();
    let start_at = observed_at - time::Duration::seconds(1);
    let start = paper_writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: PAPER_LOG_SCHEMA_VERSION,
            parser_version: 1,
            observed_at: SourceTimestamp(start_at),
            received_at: ReceivedAt(start_at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::QualificationStarted(Box::new(
                QualificationStarted {
                    starting_bankroll: CollateralAmount::from_decimal_exact(dec!(10_000)).unwrap(),
                    paper_prefix: TailBinding::from(&Scanner::verify(&paper_path).unwrap()),
                    source_prefix: TailBinding::from(&Scanner::verify(&source_path).unwrap()),
                    live_prefix: TailBinding {
                        physical_tail: 0,
                        last_sequence: None,
                        last_hash: "00".repeat(32),
                    },
                    artifact_blake3: "scenario".to_owned(),
                    static_config_hash: "scenario".to_owned(),
                    hot_config_hash: runtime.canonical_hash(),
                    generation: "scenario".to_owned(),
                    activation_id: "scenario".to_owned(),
                    ranking_batch_id: 545,
                    membership: vec![leader_wallet()],
                    membership_proofs_hash: "scenario".to_owned(),
                    schema_version: 3,
                    parser_version: 1,
                    financial_semantic_version: 1,
                },
            )))
            .unwrap(),
        })
        .unwrap();
    paper_state
        .reset_financial_era(
            start,
            CollateralAmount::from_decimal_exact(dec!(10_000)).unwrap(),
        )
        .unwrap();

    let mut accounts = LiveAccountsSnapshot::from_rows(
        vec![AccountRow {
            account_id: "identity-test".to_owned(),
            is_primary: true,
            enabled: true,
            execution_order: 0,
            requested_live_mode: "live_tiny".to_owned(),
            effective_live_mode: "live_tiny".to_owned(),
            live_price_impact_cap_bps: 100,
            custody_wallet_address: None,
            custody_wallet_kind: None,
        }],
        &[CredentialMetaRow {
            account_id: "identity-test".to_owned(),
            bundle_version: 1,
            key_id: "identity-key".to_owned(),
        }],
    );
    accounts.fetched_at_unix = Some(observed_at.unix_timestamp());
    let admission = LiveAdmissionArtifact {
        market: LiveMarketEvidence {
            condition_id: PolymarketConditionId(condition.to_string()),
            ordered_outcome_token_ids: [
                PolymarketTokenId(token_id),
                PolymarketTokenId(format!("{condition}-1")),
            ],
            neg_risk: false,
            minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
            minimum_order_size: ShareAmount::from_whole(1).unwrap(),
            scheduled_end_unix: None,
            observed_at_unix: observed_at.unix_timestamp(),
            schema_version: pe_source_polymarket_public::LIVE_MARKET_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::LIVE_MARKET_PARSER_VERSION,
            freshness_window_secs: 60,
        },
        settlement: VenueSettlementRecord {
            schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: PolymarketConditionId(condition.to_string()),
            status: VenueResolutionStatus::Unresolved,
            raw_evidence_hash: "scenario".to_owned(),
            source_timestamp_unix: None,
            observed_at_unix: observed_at.unix_timestamp(),
            parser_version: 1,
            freshness_window_secs: 60,
        },
        fee_schedule: CompactFeeSchedule::Zero,
        receipts: AdmissionReceipts {
            gamma: page_receipt,
            clob_long: page_receipt,
            clob_compact: page_receipt,
        },
    };
    let hooks = Arc::new(ScenarioHooks::default());
    hooks
        .admission_artifacts
        .lock()
        .unwrap()
        .push_back(admission);

    let (control_tx, control_rx) = mpsc::channel(4);
    let http = reqwest::Client::new();
    let book_fetcher = ReqwestClobBookFetcher::new(http.clone())
        .with_base_url(format!("http://{address}"))
        .with_source_log(source_log.clone());
    let mut orchestrator = Orchestrator::new(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            bankroll: dec!(10_000),
            mode: ExecutionMode::Paper,

            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            min_fill_price: Decimal::ZERO,
            price_impact_cap_bps: 100,
            entry_gate_config: disabled_entry_gate(),
            runtime_config: None,
            live_accounts: Some(LiveAccounts::new(accounts)),
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
        },
        WinnerFollowStrategy::new(runtime.winner_follow_config()),
        paper_writer,
        Arc::clone(&paper_state),
        build_leader_ledger(&paper_state).unwrap(),
        healthy_ws_health(),
        mid_cache_for(std::slice::from_ref(&condition), "0.50"),
        control_rx,
        None,
        None,
        None,
        Arc::new(book_fetcher),
    )
    .unwrap();
    orchestrator.set_scenario_hooks(Arc::clone(&hooks));
    orchestrator
        .configure_financial_log_paths(
            paper_path.clone(),
            source_path,
            LiveAdmissionBuilder::new(
                http.clone(),
                "http://unused.invalid",
                "http://unused.invalid",
                source_log.clone(),
            ),
            Arc::new(HistoricalMarkAdapter::new(
                http,
                "http://unused.invalid",
                source_log,
            )),
            source_receipts,
        )
        .unwrap();
    let run = tokio::spawn(orchestrator.run(std::future::pending::<()>()));
    let source_trade_id = read.aggregates[0].group_id.key().clone();
    let aggregates = read.aggregates;
    let decision_inputs_json = read.decision_inputs_json;
    let occurrence = read.page;
    let (committed, acknowledgement) = oneshot::channel();
    control_tx
        .send(
            pe_service::orchestrator_control::OrchestratorControl::CommitActivityBucket {
                aggregates,
                context: Arc::new(BucketDecisionContext {
                    applied_configuration: runtime,
                    decision_inputs_json,
                    page_occurrences: vec![occurrence],
                    observed_source_receipts: HashMap::new(),
                    reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
                    read_commitment: Some(
                        pe_service::bucket_commit::ActivityReadCommitmentReceipt::LegacyV1(
                            commitment_receipt,
                        ),
                    ),

                    signal_config: SignalConfig::default(),
                    copy_eligible: true,
                    bracket_commit: false,
                    recorded_at_unix: observed_at.unix_timestamp(),
                    observation_provenance: HashMap::from([(
                        source_trade_id,
                        TradeProvenance::RestPoll,
                    )]),
                    no_copy_dispositions: HashMap::new(),
                    identity_overrides: HashMap::new(),
                    identity_unresolved: HashSet::new(),
                    history_status: None,
                }),
                committed,
            },
        )
        .await
        .unwrap();
    acknowledgement.await.unwrap().unwrap();
    drop(control_tx);
    run.await.unwrap();

    assert_eq!(book_requests.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(paper_state.pending_dispatch_seeds().unwrap().is_empty());
    assert!(
        paper_state
            .unfinalized_ready_dispatch_seeds()
            .unwrap()
            .is_empty()
    );
    let financial_prepared = pe_service::paper_recovery::scan_paper_log(&paper_path)
        .unwrap()
        .into_iter()
        .filter(|frame| {
            matches!(
                frame.frame,
                pe_service::paper_recovery::PaperLogFrame::Record(
                    PaperLogRecord::FinancialPrepared { .. }
                )
            )
        })
        .count();
    assert_eq!(financial_prepared, 0);

    source_coordinator.abort();
    server.abort();
}

// ── R10: shutdown paths ───────────────────────────────────────────────────────

#[tokio::test(start_paused = true)]
async fn r10a_closed_trade_channel_ends_the_pool_orderly_without_redials() {
    let (pool, trade_rx) = start_pool(8).await;
    let mut servers: Vec<ActivityWsPeer> = (0..3).map(|s| pool.net.take_server(s)).collect();
    // Closing the idle trade receiver alone ends the coordinator; the owner
    // aborts every reader and returns — no frame is needed to notice it.
    drop(trade_rx);
    assert!(settle_until(|| pool.task.is_finished()).await);
    assert!(pool.source_log_ids().is_empty());
    advance(Duration::from_secs(120)).await;
    for slot in 0..3 {
        assert_eq!(
            pool.net.dial_count(slot),
            1,
            "slot {slot}: no re-dial after shutdown"
        );
    }
    for server in &mut servers {
        assert!(server.recv_text().await.is_none(), "every socket is gone");
    }
}

#[tokio::test(start_paused = true)]
async fn r10b_external_abort_of_the_owner_stops_every_reader() {
    let (pool, _trade_rx) = start_pool(8).await;
    let mut servers: Vec<ActivityWsPeer> = (0..3).map(|s| pool.net.take_server(s)).collect();
    pool.task.abort();
    settle().await;
    advance(Duration::from_secs(120)).await;
    for slot in 0..3 {
        assert_eq!(
            pool.net.dial_count(slot),
            1,
            "slot {slot}: aborted, not re-dialed"
        );
    }
    for server in &mut servers {
        assert!(server.recv_text().await.is_none());
    }
}

// ── WS1: websocket + REST duplicate ⇒ exactly one decision ───────────────────

#[tokio::test]
async fn ws1_duplicate_ws_then_rest_yields_one_decision() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let now = OffsetDateTime::now_utc();
    run_trades(
        dir.path(),
        paper_state.clone(),
        healthy_ws_health(),
        OrchOpts::ws(vec![market()]),
        vec![
            trade_at("0xdup", market(), now, TradeProvenance::ActivityWs),
            trade_at("0xdup", market(), now, TradeProvenance::RestPoll),
        ],
    )
    .await;
    assert_eq!(
        paper_fill_count(dir.path()),
        0,
        "pre-Start duplicate cannot write a financial fill"
    );
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(100)),
        "leader ledger must ingest the duplicate exactly once"
    );
}

// ── WS2: stale REST fallback and fresh websocket dispositions stay distinct ─

#[tokio::test]
async fn ws2_stale_rest_fallback_and_fresh_ws_dispositions() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let now = OffsetDateTime::now_utc();
    let stale_id = bucket_id("0xstale", market());
    run_trades(
        dir.path(),
        paper_state.clone(),
        healthy_ws_health(),
        OrchOpts::ws(vec![market(), market_b(), market_c()]),
        vec![
            // 60s old on the fallback path: 30x the 2s budget.
            trade_at(
                "0xstale",
                market(),
                now - time::Duration::seconds(60),
                TradeProvenance::RestPoll,
            ),
            // Fractional boundary (review F6): 2.5s old with a 2s budget IS stale —
            // whole-second truncation would have admitted it (2 > 2 false).
            trade_at(
                "0xboundary",
                market_c(),
                now - time::Duration::milliseconds(2_500),
                TradeProvenance::RestPoll,
            ),
            // Fresh websocket observation in another market reaches the financial-era guard.
            trade_at("0xfresh", market_b(), now, TradeProvenance::ActivityWs),
        ],
    )
    .await;
    assert_eq!(
        paper_fill_count(dir.path()),
        0,
        "pre-Start observations cannot write financial fills"
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
    assert!(
        paper_state
            .no_copy_disposition(&bucket_id("0xfresh", market_b()))
            .unwrap()
            .is_none(),
        "fresh pre-Start refusal is not a stale no-copy"
    );
    let boundary_id = bucket_id("0xboundary", market_c());
    assert!(
        paper_state
            .no_copy_disposition(&boundary_id)
            .unwrap()
            .is_some(),
        "a 2.5s-old fallback trade must be stale under a 2s budget (no truncation)"
    );
    // The stale trade still advanced the leader ledger (bookkeeping intact).
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(100)),
        "stale trade must still mirror the leader position"
    );
}

// ── WS3: disabled websocket mode retains REST classification pre-Start ───────

#[tokio::test]
async fn ws3_disabled_mode_processes_rest_trade_without_financial_write() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let now = OffsetDateTime::now_utc();
    let id = bucket_id("0xlegacy", market());
    run_trades(
        dir.path(),
        paper_state.clone(),
        new_shared_health_with_ws(false, false, 90),
        OrchOpts {
            ws_enabled: false,
            ..OrchOpts::ws(vec![market()])
        },
        vec![trade_at(
            "0xlegacy",
            market(),
            now - time::Duration::seconds(60),
            TradeProvenance::RestPoll,
        )],
    )
    .await;
    assert_eq!(
        paper_fill_count(dir.path()),
        0,
        "pre-Start REST input cannot write a schema-one fill"
    );
    assert!(
        paper_state.no_copy_disposition(&id).unwrap().is_none(),
        "disabled websocket mode adds no staleness disposition"
    );
}

// ── WS4: acknowledged pre-Start bucket remains financially closed ──────────

#[tokio::test]
async fn ws4_acknowledged_bucket_stays_financially_closed() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let health = healthy_ws_health();
    {
        let mut h = health.lock().unwrap();
        // Websocket unavailable: no reader ever normalized a row (the default).
        // REST unhealthy: error streak at the threshold.
        h.poll_error_streak = 3;
    }
    let now = OffsetDateTime::now_utc();
    let id = bucket_id("0xblocked", market());

    let dir_path = dir.path().to_path_buf();
    let run_state = paper_state.clone();
    let run_health = health.clone();
    let run = tokio::spawn(async move {
        run_trades(
            &dir_path,
            run_state,
            run_health,
            OrchOpts::ws(vec![market()]),
            vec![trade_at(
                "0xblocked",
                market(),
                now,
                TradeProvenance::ActivityWs,
            )],
        )
        .await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !run.is_finished(),
        "the acknowledged continuation remains held while both sources are unhealthy"
    );
    assert!(
        paper_state.is_seen(&id).unwrap(),
        "the bucket commit is durable before its continuation runs"
    );
    {
        let mut current = health.lock().unwrap();
        current.poll_error_streak = 0;
        current.poll_last_round_at = Some(OffsetDateTime::now_utc());
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("the continuation completes after source recovery")
        .unwrap();
    assert_eq!(
        paper_fill_count(dir.path()),
        0,
        "acknowledged pre-Start bucket remains financially fail-closed"
    );
    assert!(paper_state.is_seen(&id).unwrap());
}

// ── #544 decision_pending: boot resumes without reapplying bucket state ──────

#[tokio::test]
async fn decision_pending_boot_resume_is_terminal_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    support::install_empty_anchor(&paper_state, leader_wallet(), 0);
    let source_epoch = 1_i64;
    let body = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": leader_wallet().to_string(), "timestamp": source_epoch,
        "conditionId": market().to_string(), "type": "TRADE", "size": "1", "usdcSize": "0.5",
        "transactionHash": "0xpending", "price": "0.5", "asset": "pending-token",
        "side": "BUY", "outcomeIndex": 0, "outcome": "Yes", "isCombo": false,
    }]))
    .unwrap();
    let read = support::producer_shaped_read_v1(
        leader_wallet(),
        &body,
        source_epoch,
        2,
        support::scenario_receipt(2),
    );
    let source_trade_id = read.aggregates[0].group_id.key().clone();
    let mut context = support::read_context(&read, support::scenario_receipt(3), 2);
    context
        .observation_provenance
        .insert(source_trade_id.clone(), TradeProvenance::ActivityWs);
    context
        .observed_source_receipts
        .insert(source_trade_id.clone(), support::scenario_receipt(1));
    context.history_status = Some(WalletHistoryStatusRecord {
        wallet: leader_wallet(),
        complete: true,
        proof_json: "{}".to_owned(),
        updated_at_unix: 2,
    });
    let mut engine = pe_service::bucket_commit::BucketCommitEngine::load(
        Arc::clone(&paper_state),
        build_leader_ledger(&paper_state).unwrap(),
    )
    .unwrap();
    assert_eq!(
        engine
            .commit(
                read.aggregates,
                &context,
                pe_service::bucket_commit::FrozenDecisionBasis {
                    win_rate_p: pe_core_types::Probability::ZERO,
                    bankroll: Decimal::ZERO,
                }
            )
            .unwrap()
            .pending,
        vec![source_trade_id.clone()]
    );
    drop(engine);

    let (mut first, first_control) = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        healthy_ws_health(),
        OrchOpts::ws(vec![market()]),
    );
    drop(first_control);
    first.resume_pending_before_producers().await.unwrap();
    drop(first);

    assert!(paper_state.open_decision_pending().unwrap().is_empty());
    assert_eq!(leader_long(&paper_state, &market()), Some(1_000_000));
    assert_eq!(paper_fill_count(dir.path()), 0);
    assert_eq!(
        paper_state.gate_history().unwrap()[&leader_wallet()].len(),
        1
    );
    assert_eq!(
        paper_state
            .no_copy_disposition(&source_trade_id)
            .unwrap()
            .map(|(_, _, reason)| reason),
        Some("stale_activity_ws_past_copy_budget".to_owned())
    );

    let (mut restarted, restarted_control) = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        healthy_ws_health(),
        OrchOpts::ws(vec![market()]),
    );
    drop(restarted_control);
    restarted.resume_pending_before_producers().await.unwrap();
    assert_eq!(leader_long(&paper_state, &market()), Some(1_000_000));
    assert_eq!(paper_state.decision_pending_history().unwrap().len(), 1);
    assert_eq!(paper_fill_count(dir.path()), 0);
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
            let (payloads, missing) = parse_activity_frame(frame).unwrap();
            assert_eq!((payloads.len(), missing), (1, 0));
            let raw = payloads[0];
            let received = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
            let trade = trade_parser::parse_ws_trade(raw, received).unwrap();
            sink.append_durable(EnvelopeIn {
                source_id: SourceId(ACTIVITY_WS_SOURCE_ID.to_string()),
                schema_version: ACTIVITY_WS_SCHEMA_VERSION,
                parser_version: ACTIVITY_WS_PARSER_VERSION,
                observed_at: SourceTimestamp(trade.observed_at),
                received_at: ReceivedAt(trade.received_at),
                content_type: ContentType::Json,
                payload: raw.to_vec(),
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
            // Review F7: replay injects the envelope's recorded receipt instant,
            // reconstructing the EXACT original trade (received_at included).
            trade_parser::parse_ws_trade(&env.payload, env.received_at.0).unwrap()
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
        assert_eq!(
            orig.received_at, replay.received_at,
            "replay must reconstruct the exact original (envelope receipt injected)"
        );
    }
}
