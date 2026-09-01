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
//!   R7  — full fan-in: the reader retains its frame, item, and socket, reads
//!         no second frame, derives non-live at 30 s, drains in order once
//!         capacity returns (stale rows become `activity_ws` no-copies), then
//!         drops and re-dials.
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

use pe_copy_signal_engine::{IncomingTrade, SignalConfig, TradeProvenance};
use pe_core_types::{
    BasisPoints, LeaderAction, MarketId, OutcomeId, Price, ProbabilityPpm, ReceivedAt,
    ReconstructionQuality, ShareAmount, Side, SourceId, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Reader, Writer};
use pe_execution_core::ExecutionDispatcher;
use pe_paper_state::{
    ActivityBucketCommit, ActivityDispositionRecord, DecisionPendingRecord, EntryGateResultRecord,
    LeaderPositionRow, MarketHistoryRecord, PaperStateDb, WalletHistoryStatusRecord,
};
use pe_service::activity_ingest::{ACTIVITY_WS_SOURCE_ID, ActivityIngest, Dialer, SourceLogHandle};
use pe_service::bucket_commit::DecisionContinuationV2;
use pe_service::clob_book::FixtureClobBookFetcher;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::{ReaderHealth, SharedHealth, new_shared_health_with_ws, readiness_issues};
use pe_service::live_accounts::{
    AccountRow, CredentialMetaRow, LiveAccounts, LiveAccountsSnapshot,
};
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig, ScenarioHooks};
use pe_service::paper_recovery::build_leader_ledger;
use pe_service::source_event_sink::SourceEventSink;
use pe_service::trade_parser;
use pe_source_polymarket_public::{
    ACTIVITY_WS_PARSER_VERSION, ACTIVITY_WS_SCHEMA_VERSION, ACTIVITY_WS_SUBSCRIBE, ActivityWsError,
    ActivityWsPeer, FixtureFetcher, parse_activity_frame, parse_activity_trade_observation,
};
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, SizingMode, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

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

fn id(s: &str) -> SourceTradeId {
    SourceTradeId(s.to_string())
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

fn make_dispatcher(dir: &Path) -> ExecutionDispatcher {
    let paper_writer = Writer::open(dir.join("paper.log")).unwrap();
    let paper_executor = PaperExecutor::new(paper_writer, SourceId("test.paper".into()), 500, 100);
    ExecutionDispatcher::paper_only(paper_executor)
}

fn dead_reseed_rx() -> mpsc::Receiver<pe_service::orchestrator_control::OrchestratorControl> {
    mpsc::channel(1).1
}

fn disabled_entry_gate() -> CopyEntryGateConfig {
    CopyEntryGateConfig
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

// ── Armed live accounts (mirrors scenario_dispatch.rs) ────────────────────────

fn account_row(
    id: &str,
    primary: bool,
    enabled: bool,
    execution_order: i64,
    mode: &str,
) -> AccountRow {
    AccountRow {
        account_id: id.to_string(),
        is_primary: primary,
        enabled,
        execution_order,
        requested_live_mode: mode.to_string(),
        effective_live_mode: mode.to_string(),
        live_price_impact_cap_bps: 100,
        custody_wallet_address: None,
        custody_wallet_kind: None,
    }
}

fn standard_armed_accounts() -> LiveAccounts {
    let rows = vec![
        account_row("partner", false, true, 1, "live_tiny"),
        account_row("primary-acct", true, true, 9, "live_tiny"),
        account_row("bench", false, false, 0, "live_tiny"),
    ];
    let credentials: Vec<CredentialMetaRow> = rows
        .iter()
        .map(|row| CredentialMetaRow {
            account_id: row.account_id.clone(),
            bundle_version: 1,
            key_id: "key-1".to_string(),
        })
        .collect();
    let mut snapshot = LiveAccountsSnapshot::from_rows(rows, &credentials);
    snapshot.fetched_at_unix = Some(now_unix());
    LiveAccounts::new(snapshot)
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
    trade_rx: mpsc::Receiver<IncomingTrade>,
    health: SharedHealth,
    opts: OrchOpts,
) -> Orchestrator<FixtureFetcher, FixtureClobBookFetcher> {
    paper_state
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: leader_wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    let mid_price_cache = mid_cache_for(&opts.markets, "0.50");
    let leader_ledger = build_leader_ledger(&paper_state).unwrap();
    let mut orch = Orchestrator::new(
        trade_rx,
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
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            fill_mode: pe_service::runtime_config::FillMode::LeaderHaircut,
            clob_best_ask_fallback_haircut_bps: 100,
            entry_gate_config: disabled_entry_gate(),
            runtime_config: None,
            live_accounts: opts.live_accounts,
        },
        WinnerFollowStrategy::new(flat_fill_config()),
        make_dispatcher(dir),
        paper_state,
        leader_ledger,
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
    if let Some(hooks) = opts.hooks {
        orch.set_scenario_hooks(hooks);
    }
    orch
}

/// Run `trades` (in order) through a fresh orchestrator to completion.
async fn run_trades(
    dir: &Path,
    paper_state: Arc<PaperStateDb>,
    health: SharedHealth,
    opts: OrchOpts,
    trades: Vec<IncomingTrade>,
) {
    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(64);
    for t in trades {
        trade_tx.send(t).await.unwrap();
    }
    drop(trade_tx);
    build_orchestrator(dir, paper_state, trade_rx, health, opts)
        .run(std::future::pending::<()>())
        .await;
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
    let dir = tempfile::tempdir().unwrap();
    let source_log = dir.path().join("source.log");
    let sink = SourceEventSink::open(&source_log).unwrap();
    let (source_log_handle, source_rx) = SourceLogHandle::channel(capacity);
    let (trigger_tx, mut trigger_rx) = mpsc::channel(capacity);
    let (trade_tx, trade_rx) = mpsc::channel(capacity);
    let health = healthy_ws_health();
    let ingest = ActivityIngest::with_dialer(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        sink,
        source_rx,
        trigger_tx,
        health.clone(),
        net.dialer(),
    );
    let replay_path = source_log.clone();
    // Compatibility projection for the retained #546 reader-pool scenarios:
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
    // instant — the deadline wins (no refresh, no read); then a 2 s backoff.
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
    assert!(server.recv_text().await.is_none());
    let r = pool.reader(0);
    assert_eq!(
        r.normalized_activity_rows_total, 0,
        "a frame ready at the deadline cannot refresh liveness"
    );
    assert!(r.last_normalized_activity_at.is_none());
    advance(Duration::from_millis(1_999)).await;
    assert_eq!(pool.net.dial_count(0), 2);
    advance(Duration::from_millis(1)).await;
    assert_eq!(pool.net.dial_count(0), 3);
    assert_eq!(pool.reader(0).consecutive_reconnects, 2);
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

// ── R4: one silent slot never interrupts delivery; one decision per identifier ──

#[tokio::test(start_paused = true)]
async fn r4_one_silent_reader_cannot_interrupt_delivery_to_one_decision() {
    let (pool, trade_rx) = start_pool(64).await;
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    // Fixed admission clock: 0xsame copy 1 samples early+final (fills), copy 2 is
    // seen (no sample), 0xother samples early+final. Rows are observed 1 s before.
    let t = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
    let hooks = Arc::new(ScenarioHooks::default());
    hooks.age_clock.lock().unwrap().extend([t, t, t, t]);
    let orch = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        trade_rx,
        pool.health.clone(),
        OrchOpts {
            hooks: Some(Arc::clone(&hooks)),
            ..OrchOpts::ws(vec![market(), market_b()])
        },
    );
    let (orch_task, shutdown) = spawn_orchestrator(orch);
    let mut s0 = pool.net.take_server(0);
    let mut s1 = pool.net.take_server(1); // silent forever
    let mut s2 = pool.net.take_server(2);

    advance(Duration::from_secs(1)).await;
    let observed_unix = t.unix_timestamp() - 1;
    let frame = activity_frame(&[payload("0xsame", LEADER, &market(), observed_unix)]);
    s0.send_text(&frame).await.unwrap();
    s2.send_text(&frame).await.unwrap();
    assert!(
        settle_until(
            || paper_fill_count(dir.path()) == 1 && paper_state.is_seen(&id("0xsame")).unwrap()
        )
        .await,
        "two reader copies reach one decision with no polling input"
    );
    // A distinct identifier delivered by one reader only is also copied.
    s2.send_text(&activity_frame(&[payload(
        "0xother",
        LEADER,
        &market_b(),
        observed_unix,
    )]))
    .await
    .unwrap();
    assert!(settle_until(|| paper_state.is_seen(&id("0xother")).unwrap()).await);
    assert!(
        paper_state
            .no_copy_disposition(&id("0xother"))
            .unwrap()
            .is_none(),
        "delivered fresh: admitted without a stale disposition"
    );
    // The same leader's second entry reaches the strategy, which sizes it to zero
    // under the existing per-leader Kelly rule ("no edge") — delivery is proven by
    // admission, not by a second fill.
    assert_eq!(paper_fill_count(dir.path()), 1);
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
        "four budget samples"
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
    // Fixed admission clock for both runs: copy 1 samples early+final (fault),
    // copy 2 samples early+final (fills), copy 3 is seen (no sample).
    let t = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
    let hooks = Arc::new(ScenarioHooks::default());
    hooks
        .fail_next_stage_seed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    hooks.age_clock.lock().unwrap().extend([t, t, t, t]);
    let (pool, trade_rx) = start_pool(64).await;
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let orch = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        trade_rx,
        pool.health.clone(),
        OrchOpts {
            hooks: Some(Arc::clone(&hooks)),
            live_accounts: Some(standard_armed_accounts()),
            ..OrchOpts::ws(vec![market()])
        },
    );
    let (orch_task, shutdown) = spawn_orchestrator(orch);

    let row = payload("0xthree", LEADER, &market(), t.unix_timestamp() - 1);
    let frame = activity_frame(std::slice::from_ref(&row));
    for slot in 0..3 {
        pool.net.take_server(slot).send_text(&frame).await.unwrap();
    }
    assert!(
        settle_until(|| paper_state.fills_count().unwrap() == 1
            && pool.source_log_ids().len() == 3)
        .await
    );
    // Give the third copy time to reach the seen check.
    settle().await;
    settle().await;

    let assert_one_decision = |state: &PaperStateDb, paper_dir: &Path| {
        assert!(
            state.is_seen(&id("0xthree")).unwrap(),
            "one committed seen row"
        );
        assert_eq!(
            leader_long(state, &market()),
            Some(whole_shares(100)),
            "one leader delta"
        );
        assert_eq!(
            state.fills_count().unwrap(),
            1,
            "exactly one eligible paper fill"
        );
        assert_eq!(paper_fill_count(paper_dir), 1);
        let mut seeds = state.pending_dispatch_seeds().unwrap();
        seeds.extend(state.unfinalized_ready_dispatch_seeds().unwrap());
        assert_eq!(seeds.len(), 1, "one dispatch seed");
        assert_eq!(seeds[0].source_trade_id, "0xthree");
        let targets = state.dispatch_targets(&seeds[0].dispatch_id).unwrap();
        let mut accounts: Vec<&str> = targets.iter().map(|t| t.account_id.as_str()).collect();
        accounts.sort_unstable();
        assert_eq!(
            accounts,
            vec!["partner", "primary-acct"],
            "one target per eligible armed account with a credential binding"
        );
    };
    assert!(
        !hooks
            .fail_next_stage_seed
            .load(std::sync::atomic::Ordering::SeqCst),
        "the one-shot staging fault was consumed by the first copy"
    );
    assert_one_decision(&paper_state, dir.path());
    assert!(
        hooks.age_clock.lock().unwrap().is_empty(),
        "four budget samples"
    );
    shutdown.send(()).unwrap();
    orch_task.await.unwrap();
    pool.task.abort();
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

    // Fresh state, same fault: replay reproduces the single decision.
    let dir2 = tempfile::tempdir().unwrap();
    let paper2 = Arc::new(PaperStateDb::open(&dir2.path().join("p.db")).unwrap());
    assert_eq!(paper2.fills_count().unwrap(), 0);
    assert!(!paper2.is_seen(&id("0xthree")).unwrap());
    assert!(paper2.pending_dispatch_seeds().unwrap().is_empty());
    let hooks2 = Arc::new(ScenarioHooks::default());
    hooks2
        .fail_next_stage_seed
        .store(true, std::sync::atomic::Ordering::SeqCst);
    hooks2.age_clock.lock().unwrap().extend([t, t, t, t]);
    run_trades(
        dir2.path(),
        paper2.clone(),
        healthy_ws_health(),
        OrchOpts {
            hooks: Some(Arc::clone(&hooks2)),
            live_accounts: Some(standard_armed_accounts()),
            ..OrchOpts::ws(vec![market()])
        },
        replayed,
    )
    .await;
    assert!(
        !hooks2
            .fail_next_stage_seed
            .load(std::sync::atomic::Ordering::SeqCst),
        "replay consumed the fault exactly once"
    );
    assert_one_decision(&paper2, dir2.path());
    assert!(hooks2.age_clock.lock().unwrap().is_empty());
}

// ── R7: saturation retains the frame, item, and socket; drains in order ──────

#[tokio::test(start_paused = true)]
async fn r7_full_fan_in_retains_frame_and_socket_then_drains_in_order_and_drops() {
    // Trade channel capacity 1 ⇒ fan-in capacity 1.
    let (pool, trade_rx) = start_pool(1).await;
    let mut server = pool.net.take_server(0);
    let stale_unix = now_unix() - 60;
    let rows: Vec<String> = ["0xa", "0xb", "0xc", "0xd", "0xf", "0xg"]
        .iter()
        .map(|tx| payload(tx, LEADER, &market(), stale_unix))
        .collect();
    server.send_text(&activity_frame(&rows)).await.unwrap();
    assert!(
        settle_until(|| pool.reader(0).fan_in_blocked).await,
        "the sixth row blocks after the test-only trigger projection adds two retained slots"
    );
    let before = pool.reader(0);
    assert!(before.connected && before.is_live(Instant::now()));
    assert_eq!(
        pool.source_log_ids(),
        vec![
            "0xa".to_string(),
            "0xb".to_string(),
            "0xc".to_string(),
            "0xd".to_string(),
        ]
    );

    // No second wire frame is read while blocked.
    server
        .send_text(&activity_frame(&[payload(
            "0xe",
            STRANGER,
            &market_b(),
            now_unix(),
        )]))
        .await
        .unwrap();
    settle().await;
    assert_eq!(pool.reader(0).normalized_activity_rows_total, 0);
    assert!(pool.reader(0).fan_in_blocked);

    // 30 s later: non-live by the derived rule, still connected, item and socket retained.
    advance(Duration::from_secs(30)).await;
    let r = pool.reader(0);
    assert!(r.connected && r.fan_in_blocked);
    assert_eq!(
        r.last_normalized_activity_at,
        before.last_normalized_activity_at
    );
    assert!(
        !r.is_live(Instant::now()),
        "derived non-live after 30 s on the reader's own clock"
    );
    assert_eq!(pool.net.dial_count(0), 1, "no reconnect while blocked");

    // Release capacity: an orchestrator drains every retained row IN ORDER; each
    // stale websocket row is seen with an `activity_ws` no-copy disposition.
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let orch = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        trade_rx,
        pool.health.clone(),
        OrchOpts::ws(vec![market()]),
    );
    let (orch_task, shutdown) = spawn_orchestrator(orch);
    let all = ["0xa", "0xb", "0xc", "0xd", "0xf", "0xg"];
    assert!(settle_until(|| all.iter().all(|t| paper_state.is_seen(&id(t)).unwrap())).await);
    assert_eq!(
        pool.source_log_ids(),
        all.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        "retained rows landed in order without eviction"
    );
    for t in all {
        let (provenance, age, reason) = paper_state.no_copy_disposition(&id(t)).unwrap().unwrap();
        assert_eq!(provenance, "activity_ws");
        assert!(age >= 60, "{t}: age {age}");
        assert_eq!(reason, "stale_activity_ws_past_copy_budget");
    }
    assert_eq!(paper_fill_count(dir.path()), 0, "no fill");
    assert!(
        paper_state.pending_dispatch_seeds().unwrap().is_empty(),
        "no seed"
    );
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(600)),
        "leader bookkeeping kept"
    );

    // After the drain the deadline check drops the socket BEFORE the next read, so
    // the second frame is never consumed; the slot re-dials after its backoff.
    assert!(settle_until(|| !pool.reader(0).connected).await);
    assert!(!pool.reader(0).fan_in_blocked);
    assert!(server.recv_text().await.is_none());
    assert_eq!(pool.reader(0).normalized_activity_rows_total, 6);
    advance(Duration::from_secs(1)).await;
    assert_eq!(pool.net.dial_count(0), 2);
    shutdown.send(()).unwrap();
    orch_task.await.unwrap();
    pool.task.abort();
}

// ── R8: strict budget for both provenances at the early gate (fixed clock) ───

#[tokio::test]
async fn r8_early_gate_applies_the_strict_budget_to_both_provenances() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let t = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();
    let hooks = Arc::new(ScenarioHooks::default());
    // Stale rows consume one instant (early gate); eligible rows consume two
    // (early gate + the pre-staging re-check).
    hooks.age_clock.lock().unwrap().extend([t, t, t, t, t, t]);
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
                "0xws60",
                market(),
                t - time::Duration::seconds(60),
                TradeProvenance::ActivityWs,
            ),
            trade_at(
                "0xrest60",
                market_b(),
                t - time::Duration::seconds(60),
                TradeProvenance::RestPoll,
            ),
            // Exact boundary (age == budget) stays eligible for both provenances.
            trade_at(
                "0xwsedge",
                market_c(),
                t - time::Duration::seconds(2),
                TradeProvenance::ActivityWs,
            ),
            trade_at(
                "0xrestedge",
                market_d(),
                t - time::Duration::seconds(2),
                TradeProvenance::RestPoll,
            ),
        ],
    )
    .await;
    assert!(
        hooks.age_clock.lock().unwrap().is_empty(),
        "every check sampled once"
    );
    // Both boundary rows are admitted as eligible (seen, no disposition, both sampled
    // twice). The first fills; the second reaches the strategy and is sized to zero by
    // the existing per-leader Kelly rule — stale rows never get that far.
    assert_eq!(
        paper_fill_count(dir.path()),
        1,
        "a boundary row fills; stale rows never do"
    );
    assert_eq!(
        paper_state
            .no_copy_disposition(&id("0xws60"))
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
            .no_copy_disposition(&id("0xrest60"))
            .unwrap()
            .unwrap(),
        (
            "rest_poll".to_string(),
            60,
            "stale_fallback_past_copy_budget".to_string()
        )
    );
    for t in ["0xws60", "0xrest60", "0xwsedge", "0xrestedge"] {
        assert!(paper_state.is_seen(&id(t)).unwrap(), "{t} seen");
    }
    assert!(
        paper_state
            .no_copy_disposition(&id("0xwsedge"))
            .unwrap()
            .is_none()
    );
    assert!(
        paper_state
            .no_copy_disposition(&id("0xrestedge"))
            .unwrap()
            .is_none()
    );
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(100)),
        "stale rows still mirror the leader"
    );
}

// ── R9: fresh early, stale before staging; commit fault rolls back cleanly ───

#[tokio::test]
async fn r9_stale_before_staging_commits_no_copy_and_a_commit_fault_rolls_back() {
    let t = OffsetDateTime::from_unix_timestamp(1_704_070_000).unwrap();

    // Part 1: fresh at the early gate, stale at the pre-staging re-check.
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let hooks = Arc::new(ScenarioHooks::default());
    hooks.age_clock.lock().unwrap().extend([
        t,                              // 0xlate early: age 1s, fresh
        t + time::Duration::seconds(5), // 0xlate final: age 6s, stale
        t,                              // 0xsecond early: fresh; then NotFirstEntry
    ]);
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
            trade_at(
                "0xsecond",
                market(),
                t - time::Duration::seconds(1),
                TradeProvenance::ActivityWs,
            ),
        ],
    )
    .await;
    assert!(hooks.age_clock.lock().unwrap().is_empty());
    assert!(paper_state.is_seen(&id("0xlate")).unwrap());
    assert_eq!(
        paper_state
            .no_copy_disposition(&id("0xlate"))
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
    assert!(paper_state.is_seen(&id("0xsecond")).unwrap());
    assert!(
        paper_state
            .no_copy_disposition(&id("0xsecond"))
            .unwrap()
            .is_none()
    );

    // Part 2: the one-shot no-copy commit fault leaves the trade unseen and restores
    // both the leader ledger and the tentative entry, so a distinct new trade for the
    // same leader and market stages exactly once.
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let hooks = Arc::new(ScenarioHooks::default());
    hooks
        .fail_next_no_copy_commit
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let t2 = t + time::Duration::seconds(100);
    hooks.age_clock.lock().unwrap().extend([
        t,                              // 0xrb early: fresh
        t + time::Duration::seconds(5), // 0xrb final: stale → commit fault → rollback
        t2,                             // 0xnew early: fresh
        t2,                             // 0xnew final: fresh → stages
    ]);
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
                "0xrb",
                market(),
                t - time::Duration::seconds(1),
                TradeProvenance::ActivityWs,
            ),
            trade_at(
                "0xnew",
                market(),
                t2 - time::Duration::seconds(1),
                TradeProvenance::ActivityWs,
            ),
        ],
    )
    .await;
    assert!(
        !hooks
            .fail_next_no_copy_commit
            .load(std::sync::atomic::Ordering::SeqCst),
        "fault consumed exactly once"
    );
    assert!(hooks.age_clock.lock().unwrap().is_empty());
    assert!(
        !paper_state.is_seen(&id("0xrb")).unwrap(),
        "rolled back unseen"
    );
    assert!(
        paper_state
            .no_copy_disposition(&id("0xrb"))
            .unwrap()
            .is_none()
    );
    assert!(paper_state.is_seen(&id("0xnew")).unwrap());
    assert_eq!(
        paper_fill_count(dir.path()),
        1,
        "the new trade stages exactly once"
    );
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(100)),
        "the rolled-back ingest was restored: only the new trade advanced the ledger"
    );
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
        1,
        "duplicate must not double-fill"
    );
    assert_eq!(
        leader_long(&paper_state, &market()),
        Some(whole_shares(100)),
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
            // Fresh websocket observation in another market: must still fill.
            trade_at("0xfresh", market_b(), now, TradeProvenance::ActivityWs),
            // Fractional boundary (review F6): 2.5s old with a 2s budget IS stale —
            // whole-second truncation would have admitted it (2 > 2 false).
            trade_at(
                "0xboundary",
                market_c(),
                now - time::Duration::milliseconds(2_500),
                TradeProvenance::RestPoll,
            ),
        ],
    )
    .await;
    assert_eq!(
        paper_fill_count(dir.path()),
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
    let boundary_id = SourceTradeId("0xboundary".to_string());
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

// ── WS3: disabled flag ⇒ byte-identical legacy admission (rollback posture) ──

#[tokio::test]
async fn ws3_disabled_mode_processes_old_rest_trades_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let now = OffsetDateTime::now_utc();
    let id = SourceTradeId("0xlegacy".to_string());
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
        1,
        "legacy path must fill as before #530"
    );
    assert!(
        paper_state.no_copy_disposition(&id).unwrap().is_none(),
        "disabled mode must write no disposition"
    );
}

// ── WS4: dual-unhealthy ⇒ the trade is HELD, then admits exactly once ────────

#[tokio::test]
async fn ws4_dual_unhealthy_holds_trade_until_recovery_then_admits_once() {
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
    let id = SourceTradeId("0xblocked".to_string());

    // Run the orchestrator concurrently: the blocked trade must be HELD (review
    // F1 — dropping it would orphan a websocket trade behind the poll cursor),
    // with no state write while both sources are unhealthy.
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
    tokio::time::sleep(std::time::Duration::from_millis(900)).await;
    assert!(
        !run.is_finished(),
        "orchestrator must hold the blocked trade, not drop it"
    );
    assert_eq!(paper_fill_count(dir.path()), 0, "no fill while blocked");
    assert!(
        !paper_state.is_seen(&id).unwrap(),
        "no state write while blocked"
    );

    // Recovery: one source healthy again -> the HELD trade admits exactly once.
    {
        let mut h = health.lock().unwrap();
        h.poll_error_streak = 0;
        h.poll_last_round_at = Some(OffsetDateTime::now_utc());
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), run)
        .await
        .expect("orchestrator must finish after recovery")
        .unwrap();
    assert_eq!(
        paper_fill_count(dir.path()),
        1,
        "held trade fills exactly once after recovery"
    );
    assert!(paper_state.is_seen(&id).unwrap());
}

// ── #544 decision_pending: boot resumes without reapplying bucket state ──────

#[tokio::test]
async fn decision_pending_boot_resume_is_terminal_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("p.db")).unwrap());
    let source_trade_id = SourceTradeId(format!("g2:{}", "a".repeat(64)));
    let source_epoch = 1_i64;
    let semantic_revision = "revision-v2".to_owned();
    let frozen = DecisionContinuationV2 {
        version: 2,
        source_trade_id: source_trade_id.clone(),
        semantic_revision: semantic_revision.clone(),
        transaction_hash: "0xpending".to_owned(),
        wallet: leader_wallet(),
        source_epoch,
        market_id: market(),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(dec!(0.50)),
        share_amount: ShareAmount::from_whole(1).unwrap(),
        provenance: TradeProvenance::ActivityWs,
        pre_bucket_action: LeaderAction::Entry,
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        action_confidence_ppm: ProbabilityPpm(1_000_000),
        gate_result: "admitted".to_owned(),
        applied_configuration_hash: "config-v2".to_owned(),
        decision_inputs: serde_json::json!({"source_window":"complete"}),
    };
    paper_state
        .commit_activity_bucket(&ActivityBucketCommit {
            wallet: leader_wallet(),
            source_epoch,
            dispositions: vec![ActivityDispositionRecord {
                source_trade_id: source_trade_id.clone(),
                transaction_hash: "0xpending".to_owned(),
                wallet: leader_wallet(),
                source_epoch,
                semantic_revision: semantic_revision.clone(),
                activity_type: "TRADE".to_owned(),
                disposition: "decision_pending".to_owned(),
                proof_json: "{\"bucket_epoch\":1}".to_owned(),
                no_copy: None,
            }],
            leader_positions: vec![LeaderPositionRow {
                wallet: leader_wallet(),
                market_id: market(),
                outcome_id: OutcomeId(0),
                long_contracts: ShareAmount::from_whole(1).unwrap(),
                short_contracts: ShareAmount::ZERO,
            }],
            gate_results: vec![EntryGateResultRecord {
                source_trade_id: source_trade_id.clone(),
                wallet: leader_wallet(),
                market_id: market(),
                source_epoch,
                result: "admitted".to_owned(),
                history_consumed: true,
            }],
            history_effects: vec![MarketHistoryRecord {
                wallet: leader_wallet(),
                market_id: market(),
                first_epoch: source_epoch,
                source_trade_id: source_trade_id.clone(),
            }],
            history_status: Some(WalletHistoryStatusRecord {
                wallet: leader_wallet(),
                complete: true,
                proof_json: "{\"fixed_end_walk\":\"complete\"}".to_owned(),
                updated_at_unix: 2,
            }),
            pending: vec![DecisionPendingRecord {
                source_trade_id: source_trade_id.clone(),
                semantic_revision,
                wallet: leader_wallet(),
                source_epoch,
                frozen_inputs_json: serde_json::to_string(&frozen).unwrap(),
                updated_at_unix: 2,
            }],
            fence: None,
            advance_cursor: true,
        })
        .unwrap();

    let (_trade_tx, trade_rx) = mpsc::channel(1);
    let mut first = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        trade_rx,
        healthy_ws_health(),
        OrchOpts::ws(vec![market()]),
    );
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

    let (_trade_tx, trade_rx) = mpsc::channel(1);
    let mut restarted = build_orchestrator(
        dir.path(),
        paper_state.clone(),
        trade_rx,
        healthy_ws_health(),
        OrchOpts::ws(vec![market()]),
    );
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
