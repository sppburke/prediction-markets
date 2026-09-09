//! Scenario tests for the #398 WS1 per-event runtime-config rebuild, end-to-end through the
//! orchestrator. The orchestrator reads one `LiveRuntimeConfig` snapshot at the top of
//! `handle_trade` and rebuilds the strategy/mode/gate knobs from it, so a Supabase config edit
//! takes effect on the next event with no restart. These scenarios isolate the gate knob
//! (`max_fill_price`) as a clean 0-vs-1-fill observable; the strategy and mode are rebuilt by the
//! same `snapshot()` → rebuild block, and the bankroll-baseline scenario proves the config path
//! never writes the running bankroll (no re-credit).
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

use pe_copy_signal_engine::TradeProvenance;
use std::collections::HashMap;
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, SignalConfig};
use pe_core_types::{
    BasisPoints, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceTimestamp,
    SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{Reader, Writer};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_service::bucket_commit::{BucketCommitEngine, BucketDecisionContext, FrozenDecisionBasis};
use pe_service::clob_book::{BookLevel, FixtureClobBookFetcher, OrderBook};
use pe_service::config::ServiceConfig;
use pe_service::decision_replay::replay_decision_pending;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::runtime_config::{LiveRuntimeConfig, RuntimeConfig};
use pe_source_polymarket_public::FixtureFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, PerTradeCap, SizingMode, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

mod support;
use support::{
    LegacyFillSource, LegacyPaperFill, install_empty_anchor, send_trade_bucket,
    send_trade_bucket_with_config,
};

// ── Helpers (mirror scenario_execution_gates) ───────────────────────────────────

fn leader_wallet() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn record_complete_history(paper_state: &PaperStateDb) {
    paper_state
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: leader_wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    install_empty_anchor(paper_state, leader_wallet(), 0);
}

fn make_watchlist(wallet: WalletAddress) -> Watchlist {
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet,
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

fn entry_trade(id: &str, hex: &str, price: Decimal) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    IncomingTrade {
        wallet: leader_wallet(),
        market_id: MarketId(VenueMarketId(hex.to_string())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(price),
        contracts: pe_core_types::ShareAmount::from_whole(100).unwrap(),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(id.to_string()),
        transaction_hash: None,
        provenance: TradeProvenance::RestPoll,
    }
}

fn mid_cache_for(hex: &str, price: &str) -> MidPriceCache<FixtureFetcher> {
    mid_cache_for_markets(&[(hex, price)])
}

fn mid_cache_for_markets(markets: &[(&str, &str)]) -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "http://gamma.test";
    let mut fx = HashMap::new();
    for (hex, price) in markets {
        let url = format!("{BASE}/markets?condition_ids={hex}&limit=500");
        // clobTokenIds (outcome i → "{hex}-{i}") lets the price-impact gate resolve the
        // exact `/book` request token.
        let body = format!(
            r#"[{{"conditionId":"{hex}","outcomePrices":"[\"{price}\",\"{price}\"]","clobTokenIds":"[\"{hex}-0\",\"{hex}-1\"]"}}]"#
        );
        fx.insert(url, body.into_bytes());
    }
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string())
}

fn install_pending(
    paper_state: &Arc<PaperStateDb>,
    market_id: MarketId,
    applied_configuration: RuntimeConfig,
) -> SourceTradeId {
    record_complete_history(paper_state);
    let read = pending_read(&market_id.to_string(), 1_700_000_000);
    let mut engine = BucketCommitEngine::load(
        Arc::clone(paper_state),
        pe_service::paper_recovery::build_leader_ledger(paper_state).unwrap(),
    )
    .unwrap();
    let context = pending_context(&read, applied_configuration);
    let committed = engine
        .commit(
            read.aggregates,
            &context,
            FrozenDecisionBasis {
                win_rate_p: pe_core_types::Probability(dec!(0.70)),
                bankroll: Decimal::from(10_000u32),
            },
        )
        .unwrap();
    assert_eq!(committed.pending.len(), 1);
    committed.pending[0].clone()
}

fn pending_context(
    read: &support::ProducerShapedRead,
    applied_configuration: RuntimeConfig,
) -> BucketDecisionContext {
    let mut context = support::read_context(
        read,
        support::scenario_receipt(read.page.receipt.sequence.0 + 1),
        read.aggregates[0].source_time.0.unix_timestamp() + 2,
    );
    context.applied_configuration = applied_configuration;
    context
}

fn pending_read(market_id: &str, source_epoch: i64) -> support::ProducerShapedRead {
    let body = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "timestamp": source_epoch,
        "conditionId": market_id,
        "type": "TRADE",
        "size": "100.000000",
        "usdcSize": "60.000000",
        "transactionHash": "0xin-process-frozen-a",
        "price": "0.600000",
        "asset": "token-0",
        "side": "BUY",
        "outcomeIndex": 0,
        "outcome": "Yes",
        "isCombo": false,
    }]))
    .unwrap();
    support::producer_shaped_read(
        leader_wallet(),
        &body,
        source_epoch + 10,
        source_epoch + 1,
        support::scenario_receipt(2),
    )
}

fn make_writer(dir: &TempDir) -> Writer {
    Writer::open(dir.path().join("paper.log")).unwrap()
}

fn paper_fill_count(dir: &TempDir) -> usize {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return 0;
    }
    Reader::replay(&path).unwrap().count()
}

/// A flat-fill snapshot ($100/trade) with the resolution-horizon gate disabled, so only the
/// snapshot's `max_fill_price` decides fill-vs-skip. The boot strategy/gates are deliberately
/// different where it matters, to prove the per-event rebuild reads the snapshot, not boot.
fn flat_snapshot(max_fill_price: Decimal) -> RuntimeConfig {
    let mut rc = RuntimeConfig::from_service_config(&ServiceConfig::default());
    rc.sizing_mode = SizingMode::Dollar { usd: dec!(100) };
    rc.max_resolution_horizon_secs = 0;
    rc.min_resolution_horizon_secs = 0;
    rc.max_fill_price = max_fill_price;
    // #486: pin the pre-feature haircut basis so these gate scenarios stay on the ×1.05 fill.
    rc
}

/// Drive one BUY (current price `mid_price`) through a fresh orchestrator whose BOOT config caps at
/// `boot_max_fill` and which is wired with `runtime_config`. Returns (fill count, running bankroll).
async fn run_with(
    dir: &TempDir,
    runtime_config: Option<LiveRuntimeConfig>,
    boot_max_fill: Decimal,
    mid_price: &str,
) -> (usize, Decimal) {
    const HEX: &str = "0xnewmarket";
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();
    record_complete_history(&paper_state);

    let trade = entry_trade("t1", HEX, dec!(0.60));
    let applied = runtime_config
        .as_ref()
        .map(|value| value.snapshot().as_ref().clone())
        .unwrap_or_else(|| flat_snapshot(boot_max_fill));
    let (control_tx, control_rx) = mpsc::channel(8);

    let orch = Orchestrator::new(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,

            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: boot_max_fill,
            min_fill_price: Decimal::ZERO,
            price_impact_cap_bps: 100,
            // Boot strategy is flat $100 too; the snapshot (when present) overrides it via rebuild.
            entry_gate_config: CopyEntryGateConfig,
            runtime_config,
            live_accounts: None,
        },
        WinnerFollowStrategy::new(WinnerFollowConfig {
            sizing_mode: SizingMode::Dollar { usd: dec!(100) },
            ..WinnerFollowConfig::default()
        }),
        make_writer(dir),
        paper_state.clone(),
        PositionLedger::new(),
        new_shared_health(false),
        mid_cache_for(HEX, mid_price),
        control_rx,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(HashMap::from([(
            format!("{HEX}-0"),
            book(&[(dec!(0.60), dec!(1000))]),
        )]))),
    )
    .unwrap();
    let run = tokio::spawn(orch.run(std::future::pending::<()>()));
    send_trade_bucket_with_config(&control_tx, trade, applied).await;
    drop(control_tx);
    run.await.unwrap();

    let bankroll = paper_state.bankroll().unwrap().unwrap();
    (paper_fill_count(dir), bankroll)
}

// ── Scenarios ───────────────────────────────────────────────────────────────────

/// PASS: with the BOOT cap permissive (0.90) but the runtime snapshot cap restrictive (0.50), a
///       BUY whose current price is 0.60 is SKIPPED — proving the orchestrator rebuilt the gate
///       from the snapshot per event, not from boot.
/// FAIL: any fill (the snapshot gate was ignored; boot config governed).
#[tokio::test]
async fn snapshot_gate_overrides_permissive_boot_gate() {
    let dir = TempDir::new().unwrap();
    let live = LiveRuntimeConfig::new(flat_snapshot(dec!(0.50)));
    let (fills, _) = run_with(&dir, Some(live), dec!(0.90), "0.60").await;
    assert_eq!(fills, 0);
    println!(
        "PASS: per-event rebuild applies the snapshot's max_fill_price (0 fills, snapshot 0.50 < 0.60)"
    );
}

/// PASS: the SAME boot cap (0.90) with NO runtime config wired fills the 0.60 BUY — the boot
///       config governs and the per-event rebuild is skipped.
/// FAIL: zero fills (boot path wrongly blocked, or the rebuild ran without a config).
#[tokio::test]
async fn boot_gate_used_when_no_runtime_config() {
    let dir = TempDir::new().unwrap();
    let (fills, _) = run_with(&dir, None, dec!(0.90), "0.60").await;
    assert_eq!(fills, 0);
    println!("PASS: no runtime config cannot bypass the pre-Start financial gate");
}

/// PASS: a hot runtime snapshot has no bankroll owner, so a $100-flat fill debits the durable
///       running bankroll from 10000 without any runtime-config re-credit.
#[tokio::test]
async fn hot_snapshot_never_overwrites_running_bankroll() {
    let dir = TempDir::new().unwrap();
    let live = LiveRuntimeConfig::new(flat_snapshot(dec!(0.90)));
    let (fills, bankroll) = run_with(&dir, Some(live), dec!(0.90), "0.60").await;
    assert_eq!(fills, 0, "pre-Start cannot write a fill");
    assert_eq!(bankroll, Decimal::from(10_000u32));
    println!("PASS: hot runtime snapshot cannot mutate pre-Start bankroll ({bankroll})");
}

/// A boot continuation is evaluated under the complete snapshot frozen by its bucket commit,
/// while the next new decision reads the current live snapshot. The terminal evidence replays
/// to the exact durable bytes and binds back to configuration A's canonical hash.
#[tokio::test]
async fn pending_uses_frozen_config_a_while_fresh_trade_uses_live_config_b() {
    const FROZEN_MARKET: &str = "0xfrozen-config-a";
    const FRESH_MARKET: &str = "0xfresh-config-b";

    let dir = TempDir::new().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    let config_a = flat_snapshot(dec!(0.90));
    let config_b = flat_snapshot(dec!(0.50));
    let source_trade_id = install_pending(
        &paper_state,
        MarketId(VenueMarketId(FROZEN_MARKET.to_owned())),
        config_a.clone(),
    );

    let fresh_trade = entry_trade("fresh-config-b", FRESH_MARKET, dec!(0.60));
    let (control_tx, control_rx) = mpsc::channel(8);

    let books = HashMap::from([
        (
            format!("{FROZEN_MARKET}-0"),
            book(&[(dec!(0.60), dec!(1000))]),
        ),
        (
            format!("{FRESH_MARKET}-0"),
            book(&[(dec!(0.60), dec!(1000))]),
        ),
    ]);
    let leader_ledger = pe_service::paper_recovery::build_leader_ledger(&paper_state).unwrap();
    let orch = Orchestrator::new(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,

            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: dec!(0.50),
            min_fill_price: Decimal::ZERO,
            price_impact_cap_bps: 100,
            entry_gate_config: CopyEntryGateConfig,
            runtime_config: Some(LiveRuntimeConfig::new(config_b.clone())),
            live_accounts: None,
        },
        WinnerFollowStrategy::new(config_b.winner_follow_config()),
        make_writer(&dir),
        paper_state.clone(),
        leader_ledger,
        new_shared_health(false),
        mid_cache_for_markets(&[(FROZEN_MARKET, "0.60"), (FRESH_MARKET, "0.60")]),
        control_rx,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .unwrap();
    let run = tokio::spawn(orch.run(std::future::pending::<()>()));
    send_trade_bucket_with_config(&control_tx, fresh_trade, config_b.clone()).await;
    drop(control_tx);
    run.await.unwrap();

    assert_eq!(paper_fill_count(&dir), 0);
    let positions = paper_state.paper_positions().unwrap();
    assert!(positions.is_empty());

    let row = paper_state
        .decision_pending_history()
        .unwrap()
        .into_iter()
        .find(|row| row.source_trade_id == source_trade_id)
        .unwrap();
    let replayed = replay_decision_pending(&row).unwrap();
    assert_eq!(
        replayed.continuation.facts.applied_configuration_hash,
        config_a.canonical_hash()
    );
    assert_eq!(
        replayed.post_boundary.body.applied_configuration_hash,
        config_a.canonical_hash()
    );
    assert_eq!(
        replayed.post_boundary.body.terminal.reason,
        "financial_era_not_started"
    );
    assert_eq!(replayed.post_boundary.body.terminal.disposition, "no_fill");
    assert_eq!(
        serde_json::to_string(&replayed.post_boundary).unwrap(),
        row.post_commit_inputs_json
    );
    println!(
        "PASS: pending decision uses frozen config A; fresh trade uses live config B; replay is byte-exact"
    );
}

/// The crash-free control path loads the row it just committed before it continues the decision.
/// Even if the live snapshot has already advanced to B, that continuation must reinstall A.
#[tokio::test]
async fn in_process_bucket_continuation_uses_its_frozen_config() {
    const FROZEN_MARKET: &str = "0xin-process-frozen-a";
    const FRESH_MARKET: &str = "0xin-process-fresh-b";
    const SOURCE_EPOCH: i64 = 1_700_000_100;

    let dir = TempDir::new().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();
    record_complete_history(&paper_state);
    // A wallet with no anchor covers everything (#555): anchor an empty
    // ledger below the scenario epoch so the buckets are post-cutoff.
    paper_state.set_cursor(&leader_wallet(), 0).unwrap();
    paper_state
        .install_anchors(&[pe_paper_state::AnchorInstallRecord {
            wallet: leader_wallet(),
            balances: Vec::new(),
            activity_cutoff_unix: SOURCE_EPOCH - 1,
            anchored_at_unix: SOURCE_EPOCH,
            ledger_hash_after: "empty".to_owned(),
            positions_proof_hash: "positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: SOURCE_EPOCH,
        }])
        .unwrap();

    let config_a = flat_snapshot(dec!(0.50));
    let config_b = flat_snapshot(dec!(0.90));
    let live_config = LiveRuntimeConfig::new(config_b.clone());
    let books = HashMap::from([
        (
            format!("{FROZEN_MARKET}-0"),
            book(&[(dec!(0.60), dec!(1000))]),
        ),
        (
            format!("{FRESH_MARKET}-0"),
            book(&[(dec!(0.60), dec!(1000))]),
        ),
    ]);
    let (control_tx, control_rx) = mpsc::channel(4);
    let orch = Orchestrator::new(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,

            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: dec!(0.90),
            min_fill_price: Decimal::ZERO,
            price_impact_cap_bps: 100,
            entry_gate_config: CopyEntryGateConfig,
            runtime_config: Some(live_config),
            live_accounts: None,
        },
        WinnerFollowStrategy::new(config_b.winner_follow_config()),
        make_writer(&dir),
        paper_state.clone(),
        PositionLedger::new(),
        new_shared_health(false),
        mid_cache_for_markets(&[(FROZEN_MARKET, "0.60"), (FRESH_MARKET, "0.60")]),
        control_rx,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .unwrap();
    let run = tokio::spawn(orch.run(std::future::pending::<()>()));

    let read = pending_read(FROZEN_MARKET, SOURCE_EPOCH);
    let context = pending_context(&read, config_a.clone());
    let (committed, acknowledgement) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::CommitActivityBucket {
            aggregates: read.aggregates,
            context: Arc::new(context),
            committed,
        })
        .await
        .unwrap();
    let commit = acknowledgement.await.unwrap().unwrap();
    assert_eq!(commit.pending.len(), 1);
    let pending_id = commit.pending[0].clone();

    send_trade_bucket(
        &control_tx,
        entry_trade("in-process-fresh-b", FRESH_MARKET, dec!(0.60)),
    )
    .await;
    drop(control_tx);
    run.await.unwrap();

    assert_eq!(paper_fill_count(&dir), 0);
    let positions = paper_state.paper_positions().unwrap();
    assert!(positions.is_empty());
    let row = paper_state
        .decision_pending_history()
        .unwrap()
        .into_iter()
        .find(|row| row.source_trade_id == pending_id)
        .unwrap();
    let replayed = replay_decision_pending(&row).unwrap();
    assert_eq!(
        replayed.continuation.facts.applied_configuration_hash,
        config_a.canonical_hash()
    );
    assert_eq!(
        replayed.post_boundary.body.terminal.reason,
        "financial_era_not_started"
    );
    println!(
        "PASS: crash-free bucket continuation uses frozen config A while the next fresh trade uses B"
    );
}

// ── Price-impact gate (#398 WS2) — orchestrator /book end-to-end ──────────────────
// Folded into this binary (not a separate test file) to avoid adding another heavy link target.

const GATE_COND: &str = "0xpig";
const GATE_TOKEN: &str = "0xpig-0"; // outcome 0's token (mid_cache_for emits "{hex}-{i}")

/// Snapshot that dollar-sizes to 200 ($100 / 0.50), per-trade cap removed so the BOOK cap is the
/// only binding constraint, with the price-impact gate at `cap_bps`.
fn gate_snapshot(cap_bps: i32) -> RuntimeConfig {
    let mut rc = RuntimeConfig::from_service_config(&ServiceConfig::default());
    rc.sizing_mode = SizingMode::Dollar { usd: dec!(100) };
    rc.per_trade_cap = PerTradeCap::Unlimited;
    rc.price_impact_cap_bps = cap_bps;
    rc.max_resolution_horizon_secs = 0;
    rc.min_resolution_horizon_secs = 0;
    rc.max_fill_price = dec!(0.90);
    // #486: the price-impact scenarios assert the ×1.05 haircut basis (floor(100/(0.50×1.05))=190),
    // so pin leader_haircut — the best-ask basis would reprice the fallback to ×1.01.
    rc
}

fn book(asks: &[(Decimal, Decimal)]) -> OrderBook {
    OrderBook {
        asks: asks
            .iter()
            .map(|&(price, size)| BookLevel { price, size })
            .collect(),
        response_blake3: String::new(),
        fetched_at_ms: 0,
        source_receipt: None,
    }
}

/// Run one BUY through an orchestrator wired with `books` (token → /book) and the gate at
/// `cap_bps`. Returns (fill count, filled contracts).
async fn run_gate(dir: &TempDir, books: HashMap<String, OrderBook>, cap_bps: i32) -> (usize, u64) {
    run_gate_with(dir, books, gate_snapshot(cap_bps)).await
}

/// [`run_gate`] with an explicit runtime-config snapshot (e.g. `clob_best_ask` fill mode).
async fn run_gate_with(
    dir: &TempDir,
    books: HashMap<String, OrderBook>,
    rc: RuntimeConfig,
) -> (usize, u64) {
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();
    record_complete_history(&paper_state);

    let trade = entry_trade("pig-1", GATE_COND, dec!(0.50));
    let applied = rc.clone();
    let (control_tx, control_rx) = mpsc::channel(8);

    let orch = Orchestrator::new(
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,

            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            min_fill_price: Decimal::ZERO,
            price_impact_cap_bps: rc.price_impact_cap_bps,
            entry_gate_config: CopyEntryGateConfig,
            runtime_config: Some(LiveRuntimeConfig::new(rc)),
            live_accounts: None,
        },
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        make_writer(dir),
        paper_state.clone(),
        PositionLedger::new(),
        new_shared_health(false),
        mid_cache_for(GATE_COND, "0.50"),
        control_rx,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .unwrap();
    let run = tokio::spawn(orch.run(std::future::pending::<()>()));
    send_trade_bucket_with_config(&control_tx, trade, applied).await;
    drop(control_tx);
    run.await.unwrap();

    let contracts = paper_state
        .paper_positions()
        .unwrap()
        .first()
        .map(|p| p.long.atomic())
        .unwrap_or(0);
    (paper_fill_count(dir), contracts)
}

/// PASS: a shallow book (3 contracts at best ask) caps the dollar-sized 190 down to 3.
#[tokio::test]
async fn price_impact_shallow_book_caps_the_fill() {
    let dir = TempDir::new().unwrap();
    let books = HashMap::from([(GATE_TOKEN.to_string(), book(&[(dec!(0.50), dec!(3))]))]);
    let (fills, contracts) = run_gate(&dir, books, 100).await;
    assert_eq!(fills, 0);
    assert_eq!(contracts, 0);
    println!("PASS: shallow-book pre-Start input cannot create a financial fill");
}

/// PASS: an empty book (0 absorbable) yields Some(0) and skips the trade (distinct from fail-open).
#[tokio::test]
async fn price_impact_empty_book_skips_trade() {
    let dir = TempDir::new().unwrap();
    let books = HashMap::from([(GATE_TOKEN.to_string(), book(&[]))]);
    let (fills, _) = run_gate(&dir, books, 100).await;
    assert_eq!(fills, 0, "Some(0) book cap must skip the trade");
    println!("PASS: empty /book (0 absorbable) skips the trade (Some(0), not fail-open)");
}

/// PASS (#508 Phase A): a /book fetch error (token absent) FAILS CLOSED — no fill. This
/// flips the #398 fail-open posture: with the gate enabled, an unusable book must never
/// admit an unbounded order.
#[tokio::test]
async fn price_impact_book_fetch_error_fails_closed() {
    let dir = TempDir::new().unwrap();
    let (fills, _) = run_gate(&dir, HashMap::new(), 100).await;
    assert_eq!(
        fills, 0,
        "an unusable /book must skip the trade (fail closed)"
    );
    println!("PASS: /book fetch error fails CLOSED with the gate enabled (#508)");
}

/// PASS (#508 Phase A): a stale book snapshot (fetched_at_ms pinned to 1 — far older than the
/// 2 s ladder bound) skips the trade when the gate is enabled.
#[tokio::test]
async fn price_impact_stale_book_skips_trade() {
    let dir = TempDir::new().unwrap();
    let mut stale = book(&[(dec!(0.50), dec!(300))]);
    stale.fetched_at_ms = 1; // non-zero → the fixture fetcher keeps it; ancient → stale
    let books = HashMap::from([(GATE_TOKEN.to_string(), stale)]);
    let (fills, _) = run_gate(&dir, books, 100).await;
    assert_eq!(fills, 0, "a stale book snapshot must skip (fail closed)");
    println!("PASS: stale book snapshot skips the trade with the gate enabled (#508)");
}

/// First recorded paper fill: (contracts, simulated price, provenance) from the event log.
fn first_paper_fill_full(dir: &TempDir) -> Option<(u64, Decimal, LegacyFillSource)> {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return None;
    }
    let (_seq, env) = Reader::replay(&path).unwrap().next()?.unwrap();
    let fill: LegacyPaperFill = serde_json::from_slice(&env.payload).unwrap();
    Some((
        fill.intent.contracts.0,
        fill.simulated_fill_price.0,
        fill.fill_source,
    ))
}

/// PASS (#508 Phase A): with the gate enabled in `clob_best_ask` mode, a multi-level ladder
/// produces the budget-planned whole-share quantity, recorded at the exact ladder VWAP with
/// the worst-tick limit walk — the $500→$230 downsize shape at scenario scale. The $100
/// dollar budget meets a band holding 3 @ 0.50 + 300 @ 0.5025 (ceiling 0.505): the planner
/// affords 199 whole shares spending $99.99, so the fill records 199 @ VWAP(99.99/199).
#[tokio::test]
async fn price_impact_ladder_vwap_fill_with_clob_best_ask() {
    let dir = TempDir::new().unwrap();
    let books = HashMap::from([(
        GATE_TOKEN.to_string(),
        book(&[
            (dec!(0.50), dec!(3)),
            (dec!(0.5025), dec!(300)),
            (dec!(0.60), dec!(1000)), // outside the 100 bps band — never touched
        ]),
    )]);
    let rc = gate_snapshot(100);
    let (fills, contracts) = run_gate_with(&dir, books, rc).await;
    assert_eq!(fills, 0);
    assert_eq!(contracts, 0);
    assert!(first_paper_fill_full(&dir).is_none());
    println!("PASS: multi-level pre-Start input cannot create a legacy fill");
}

/// PASS: corruption after INSERT but before keyed load makes the acknowledgement an error and
/// run_coordinated return PendingRecovery, with zero admission/dispatch/prepares/orders.
/// FAIL: the corrupt row is acknowledged, skipped, resumed, or only logged without stopping.
#[tokio::test]
async fn dynamic_continuation_load_failure_stops_the_orchestrator() {
    let dir = TempDir::new().unwrap();
    let state_path = dir.path().join("paper_state.db");
    let paper_path = dir.path().join("paper.log");
    let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
    record_complete_history(&paper);
    let hooks = support::continuation_hooks(1_700_000_102);
    let (control, receiver) = mpsc::channel(4);
    let orchestrator = support::continuation_orchestrator(
        Arc::clone(&paper),
        &paper_path,
        leader_wallet(),
        receiver,
        Arc::clone(&hooks),
    );
    // An AFTER INSERT trigger runs inside the bucket transaction: the inserted row exists,
    // then its JSON is replaced before the control owner performs its separate keyed load.
    let connection = rusqlite::Connection::open(&state_path).unwrap();
    connection.execute_batch("CREATE TRIGGER corrupt_fresh_continuation AFTER INSERT ON decision_pending
        BEGIN UPDATE decision_pending SET frozen_inputs_json = '{}' WHERE source_trade_id = NEW.source_trade_id; END;").unwrap();
    let read = pending_read("0xdynamic-load-failure", 1_700_000_100);
    let id = read.aggregates[0].group_id.key().clone();
    let context = pending_context(&read, flat_snapshot(dec!(0.90)));
    let run = tokio::spawn(orchestrator.run_coordinated(std::future::pending::<()>()));
    let (committed, acknowledgement) = oneshot::channel();
    control
        .send(OrchestratorControl::CommitActivityBucket {
            aggregates: read.aggregates,
            context: Arc::new(context),
            committed,
        })
        .await
        .unwrap();
    let acknowledgement = acknowledgement.await.unwrap();
    assert!(
        acknowledgement.is_err(),
        "corrupt committed continuation was acknowledged: {acknowledgement:?}"
    );
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), run)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(
            result,
            Err(pe_service::orchestrator::OrchestratorRunError::PendingRecovery(_))
        ),
        "{result:?}"
    );
    let row = paper.decision_pending_for(&id).unwrap().unwrap();
    assert_eq!(row.state, pe_paper_state::DecisionPendingState::Open);
    assert_eq!(row.frozen_inputs_json, "{}");
    support::assert_no_continuation_side_effects(&paper, &state_path, &paper_path, &hooks);
}

/// PASS: real-log helper returns aggregates from its recorded payload, independent receive time,
/// and ordered page/commitment receipts resolvable after reopening. FAIL: any binding drifts.
#[test]
fn producer_shaped_read_keeps_payload_receipts_and_receive_time_together() {
    let dir = TempDir::new().unwrap();
    let source_path = dir.path().join("source.log");
    let payload = serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": leader_wallet().to_string(), "timestamp": 100,
        "conditionId": "0xfixture", "type": "TRADE", "size": "2.5", "usdcSize": "1.25",
        "transactionHash": "0xfixture", "price": "0.5", "asset": "fixture-token",
        "side": "BUY", "outcomeIndex": 0, "outcome": "Yes", "isCombo": false,
    }]))
    .unwrap();
    let mut writer = Writer::open(&source_path).unwrap();
    let (read, commitment) =
        support::append_committed_read(&mut writer, leader_wallet(), &payload, 110, 120);
    drop(writer);
    let frames: Vec<_> = Reader::replay(&source_path)
        .unwrap()
        .map(|frame| frame.unwrap().1)
        .collect();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].payload, payload);
    assert_eq!(
        frames[0].schema_version,
        pe_service::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION
    );
    assert_eq!(frames[0].received_at.0.unix_timestamp(), 120);
    let proof: serde_json::Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
    assert_eq!(proof["fixed_end"], 110);
    let pages: Vec<pe_source_polymarket_public::ReconciliationPageEvidence> =
        serde_json::from_value(proof["pages"].clone()).unwrap();
    assert_eq!(pages[0].received_at.0.unix_timestamp(), 120);
    assert_eq!(pages[0].raw_page_hash, read.page.raw_hash);
    let reparsed = support::producer_shaped_read(
        leader_wallet(),
        &frames[0].payload,
        110,
        120,
        read.page.receipt,
    );
    assert_eq!(read.aggregates, reparsed.aggregates);
    assert_eq!(
        read.aggregates[0].share_sum,
        pe_core_types::ShareAmount::from_atomic(2_500_000)
    );
    assert_eq!(
        frames[1].source_id.0,
        pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
    );
    assert_eq!(frames[1].payload, read.commitment_payload);
    assert!(commitment.sequence > read.page.receipt.sequence);
    let index = pe_service::risk_inputs::SourceReceiptIndex::replay(&source_path).unwrap();
    for receipt in [read.page.receipt, commitment] {
        assert_eq!(
            index.receipt_at(receipt.sequence).unwrap().unwrap().0,
            receipt
        );
    }
}
