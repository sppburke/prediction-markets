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
    BasisPoints, LeaderAction, MarketId, OutcomeId, Price, ProbabilityPpm, ReceivedAt,
    ReconstructionQuality, ShareAmount, Side, SourceId, SourceTimestamp, SourceTradeId,
    VenueMarketId, WalletAddress,
};
use pe_event_log::{Reader, Writer};
use pe_paper_state::{
    ActivityBucketCommit, ActivityDispositionRecord, DecisionPendingRecord, EntryGateResultRecord,
    LeaderPositionRow, MarketHistoryRecord, PaperStateDb, WalletHistoryStatusRecord,
};
use pe_position_ledger::PositionLedger;
use pe_service::bucket_commit::{BucketDecisionContext, DecisionContinuationV2};
use pe_service::clob_book::{BookLevel, FixtureClobBookFetcher, OrderBook};
use pe_service::config::ServiceConfig;
use pe_service::decision_replay::replay_decision_pending;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{LegacyFillSource, LegacyPaperFill};
use pe_service::runtime_config::{LiveRuntimeConfig, RuntimeConfig};
use pe_source_polymarket_public::{
    ActivityAggregate, ActivityParseContext, ActivityTransport, FixtureFetcher,
    parse_activity_response,
};
use pe_strategy_winner_follow::{
    ExecutionMode, PerTradeCap, SizingMode, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

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
    paper_state: &PaperStateDb,
    market_id: MarketId,
    applied_configuration: RuntimeConfig,
) -> SourceTradeId {
    let source_trade_id = SourceTradeId(format!("g2:{}", "a".repeat(64)));
    let semantic_revision = "frozen-config-a".to_owned();
    let source_epoch = 1_700_000_000;
    let continuation = DecisionContinuationV2 {
        version: 2,
        source_trade_id: source_trade_id.clone(),
        semantic_revision: semantic_revision.clone(),
        transaction_hash: "0xfrozen-a".to_owned(),
        wallet: leader_wallet(),
        source_epoch,
        market_id: market_id.clone(),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(dec!(0.60)),
        share_amount: ShareAmount::from_whole(100).unwrap(),
        provenance: TradeProvenance::RestPoll,
        pre_bucket_action: LeaderAction::Entry,
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        action_confidence_ppm: ProbabilityPpm(1_000_000),
        gate_result: "admitted".to_owned(),
        // Freeze the config-A world's mutable basis: the watchlist's 7000 bps
        // win rate and the seeded $10k bankroll — a resumed continuation must
        // decide under these even after live state changes (#544 round 3).
        frozen_basis: pe_service::bucket_commit::FrozenDecisionBasis {
            win_rate_p: pe_core_types::Probability(rust_decimal_macros::dec!(0.70)),
            bankroll: rust_decimal::Decimal::from(10_000u32),
        },
        applied_configuration_hash: applied_configuration.canonical_hash(),
        applied_configuration,
        decision_inputs: serde_json::json!({"fixed_end": source_epoch + 10, "pages": 1}),
        observed_source_receipt: None,
        page_occurrences: Vec::new(),
    };
    paper_state
        .commit_activity_bucket(&ActivityBucketCommit {
            wallet: leader_wallet(),
            source_epoch,
            dispositions: vec![ActivityDispositionRecord {
                source_trade_id: source_trade_id.clone(),
                transaction_hash: continuation.transaction_hash.clone(),
                wallet: leader_wallet(),
                source_epoch,
                semantic_revision: semantic_revision.clone(),
                activity_type: "TRADE".to_owned(),
                disposition: "decision_pending".to_owned(),
                proof_json: serde_json::json!({"bucket_epoch": source_epoch}).to_string(),
                no_copy: None,
            }],
            leader_positions: vec![LeaderPositionRow {
                wallet: leader_wallet(),
                market_id: market_id.clone(),
                outcome_id: OutcomeId(0),
                long_contracts: ShareAmount::from_whole(100).unwrap(),
                short_contracts: ShareAmount::ZERO,
            }],
            gate_results: vec![EntryGateResultRecord {
                source_trade_id: source_trade_id.clone(),
                wallet: leader_wallet(),
                market_id: market_id.clone(),
                source_epoch,
                result: "admitted".to_owned(),
                history_consumed: true,
            }],
            history_effects: vec![MarketHistoryRecord {
                wallet: leader_wallet(),
                market_id,
                first_epoch: source_epoch,
                source_trade_id: source_trade_id.clone(),
            }],
            history_status: Some(WalletHistoryStatusRecord {
                wallet: leader_wallet(),
                complete: true,
                proof_json: "{\"scenario\":\"complete\"}".to_owned(),
                updated_at_unix: source_epoch,
            }),
            pending: vec![DecisionPendingRecord {
                source_trade_id: source_trade_id.clone(),
                semantic_revision,
                wallet: leader_wallet(),
                source_epoch,
                frozen_inputs_json: serde_json::to_string(&continuation).unwrap(),
                updated_at_unix: source_epoch,
            }],
            fence: None,
            reanchor: None,
            advance_cursor: true,
        })
        .unwrap();
    source_trade_id
}

fn pending_aggregate(market_id: &str, source_epoch: i64) -> ActivityAggregate {
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
    let observed = OffsetDateTime::from_unix_timestamp(source_epoch + 1).unwrap();
    let context = ActivityParseContext {
        source_id: SourceId("polymarket-data-api".to_owned()),
        observed_at: SourceTimestamp(observed),
        received_at: ReceivedAt(observed),
        transport: ActivityTransport::Rest,
    };
    let window = parse_activity_response(&body, leader_wallet(), &context).unwrap();
    let mut aggregates = window.aggregates().unwrap();
    assert_eq!(aggregates.len(), 1);
    aggregates.remove(0)
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

    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(8);
    trade_tx
        .send(entry_trade("t1", HEX, dec!(0.60)))
        .await
        .unwrap();
    drop(trade_tx);

    let orch = Orchestrator::new_with_trade_input(
        trade_rx,
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
        mpsc::channel(1).1,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(HashMap::from([(
            format!("{HEX}-0"),
            book(&[(dec!(0.60), dec!(1000))]),
        )]))),
    )
    .unwrap();
    orch.run(std::future::pending::<()>()).await;

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
    assert_eq!(fills, 1);
    println!("PASS: no runtime config → boot max_fill_price governs (1 fill at 0.60 < boot 0.90)");
}

/// PASS: a hot runtime snapshot has no bankroll owner, so a $100-flat fill debits the durable
///       running bankroll from 10000 without any runtime-config re-credit.
#[tokio::test]
async fn hot_snapshot_never_overwrites_running_bankroll() {
    let dir = TempDir::new().unwrap();
    let live = LiveRuntimeConfig::new(flat_snapshot(dec!(0.90)));
    let (fills, bankroll) = run_with(&dir, Some(live), dec!(0.90), "0.60").await;
    assert_eq!(fills, 1, "the BUY should fill under the 0.90 cap");
    assert!(
        bankroll < Decimal::from(10_000u32) && bankroll > Decimal::ZERO,
        "running bankroll {bankroll} must be debited from 10000 by the fill"
    );
    println!("PASS: hot runtime snapshot has no bankroll writer ({bankroll})");
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

    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(8);
    trade_tx
        .send(entry_trade("fresh-config-b", FRESH_MARKET, dec!(0.60)))
        .await
        .unwrap();
    drop(trade_tx);

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
    let orch = Orchestrator::new_with_trade_input(
        trade_rx,
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
        mpsc::channel(1).1,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .unwrap();
    orch.run(std::future::pending::<()>()).await;

    assert_eq!(
        paper_fill_count(&dir),
        1,
        "config A must fill the pending 0.60 trade; config B must reject the fresh one"
    );
    let positions = paper_state.paper_positions().unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].market_id.to_string(), FROZEN_MARKET);

    let row = paper_state
        .decision_pending_history()
        .unwrap()
        .into_iter()
        .find(|row| row.source_trade_id == source_trade_id)
        .unwrap();
    let replayed = replay_decision_pending(&row).unwrap();
    assert_eq!(
        replayed.continuation.applied_configuration_hash,
        config_a.canonical_hash()
    );
    assert_eq!(
        replayed.post_boundary.body.applied_configuration_hash,
        config_a.canonical_hash()
    );
    assert_eq!(
        replayed.post_boundary.body.terminal.reason,
        "paper_fill_committed"
    );
    assert_eq!(replayed.post_boundary.body.terminal.disposition, "fill");
    let recorded_book = replayed.post_boundary.body.book.as_ref().unwrap();
    assert_eq!(
        recorded_book.request_token_id.as_deref(),
        Some("0xfrozen-config-a-0")
    );
    assert_eq!(
        recorded_book.response_blake3.as_deref().map(str::len),
        Some(64)
    );
    assert_eq!(
        replayed.post_boundary.body.authority.kind,
        "paper_state_sqlite"
    );
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
    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(8);
    let (control_tx, control_rx) = mpsc::channel(4);
    let orch = Orchestrator::new_with_trade_input(
        trade_rx,
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

    let (committed, acknowledgement) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::CommitActivityBucket {
            aggregates: vec![pending_aggregate(FROZEN_MARKET, SOURCE_EPOCH)],
            context: Arc::new(BucketDecisionContext {
                applied_configuration: config_a.clone(),
                decision_inputs_json: serde_json::json!({
                    "fixed_end": SOURCE_EPOCH + 10,
                    "pages": 1,
                })
                .to_string(),
                page_occurrences: Vec::new(),
                observed_source_receipts: HashMap::new(),
                reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
                signal_config: SignalConfig::default(),
                copy_eligible: true,
                bracket_commit: false,
                recorded_at_unix: SOURCE_EPOCH + 2,
                observation_provenance: HashMap::new(),
                no_copy_dispositions: HashMap::new(),
                identity_overrides: HashMap::new(),
                identity_unresolved: Default::default(),
                history_status: None,
            }),
            committed,
        })
        .await
        .unwrap();
    let commit = acknowledgement.await.unwrap().unwrap();
    assert_eq!(commit.pending.len(), 1);
    let pending_id = commit.pending[0].clone();

    trade_tx
        .send(entry_trade("in-process-fresh-b", FRESH_MARKET, dec!(0.60)))
        .await
        .unwrap();
    drop(trade_tx);
    drop(control_tx);
    run.await.unwrap();

    assert_eq!(paper_fill_count(&dir), 1);
    let positions = paper_state.paper_positions().unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].market_id.to_string(), FRESH_MARKET);
    let row = paper_state
        .decision_pending_history()
        .unwrap()
        .into_iter()
        .find(|row| row.source_trade_id == pending_id)
        .unwrap();
    let replayed = replay_decision_pending(&row).unwrap();
    assert_eq!(
        replayed.continuation.applied_configuration_hash,
        config_a.canonical_hash()
    );
    assert_eq!(
        replayed.post_boundary.body.terminal.reason,
        "fill_price_at_or_above_max"
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

    let (tx, rx) = mpsc::channel::<IncomingTrade>(8);
    tx.send(entry_trade("pig-1", GATE_COND, dec!(0.50)))
        .await
        .unwrap();
    drop(tx);

    let orch = Orchestrator::new_with_trade_input(
        rx,
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

/// PASS: a shallow book (3 contracts at best ask) caps the dollar-sized 190 down to 3.
#[tokio::test]
async fn price_impact_shallow_book_caps_the_fill() {
    let dir = TempDir::new().unwrap();
    let books = HashMap::from([(GATE_TOKEN.to_string(), book(&[(dec!(0.50), dec!(3))]))]);
    let (fills, contracts) = run_gate(&dir, books, 100).await;
    assert_eq!(fills, 1);
    assert_eq!(
        contracts, 3,
        "book cap must reduce the fill to the absorbable depth"
    );
    println!("PASS: shallow /book caps the dollar-sized 190 to absorbable depth 3");
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
    let mut rc = gate_snapshot(100);
    let (fills, contracts) = run_gate_with(&dir, books, rc).await;
    assert_eq!(fills, 1);
    assert_eq!(
        contracts, 199,
        "budget-planned whole shares within the band"
    );
    let (fill_contracts, price, source) = first_paper_fill_full(&dir).expect("fill recorded");
    assert_eq!(fill_contracts, 199);
    assert_eq!(
        price,
        dec!(99.99) / dec!(199),
        "recorded fill == exact ladder VWAP (multi-level, not best-ask)"
    );
    assert_eq!(source, LegacyFillSource::ClobBestAsk);
    println!("PASS: gate-on clob_best_ask fill = 199 contracts at exact ladder VWAP (#508)");
}
