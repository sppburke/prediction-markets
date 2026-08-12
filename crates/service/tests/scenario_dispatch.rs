//! Scenario matrix for #508 dispatch aggregates.
//!
//! The harness mirrors `scenario_runtime_config`: fixed watchlist/mid-cache/book fixtures,
//! `OrchestratorConfig`, a paper dispatcher/state database, and one BUY driven through
//! `Orchestrator::run`. There is no network I/O; trade and recovery timestamps are fixed.
//!
//! Run with:
//! `cargo nextest run -p pe-service --features scenario scenario_dispatch`

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::too_many_arguments
)]

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, LeaderSignal, SignalConfig};
use pe_core_types::{
    BasisPoints, ContractQty, MarketId, OutcomeId, Price, ProbabilityPpm, Quantity,
    ReconstructionQuality, Side, SourceId, SourceTimestamp, SourceTradeId, StrategyId, TraderId,
    VenueId, VenueMarketId, WalletAddress,
};
use pe_event_log::{Reader, Writer};
use pe_execution_core::ExecutionDispatcher;
use pe_paper_state::{DispatchSeedRecord, DispatchTargetSeed, PaperStateDb};
use pe_position_ledger::PositionLedger;
use pe_service::clob_book::{BookLevel, FixtureClobBookFetcher, OrderBook};
use pe_service::config::ServiceConfig;
use pe_service::dispatch_recovery::{STUCK_SEED_OUTCOME, resume_dispatch_seeds};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_accounts::{
    AccountRow, CredentialMetaRow, LiveAccounts, LiveAccountsSnapshot,
};
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::runtime_config::{FillMode, LiveRuntimeConfig, RuntimeConfig};
use pe_source_polymarket_public::FixtureFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, PaperExecutor, PaperFill, PerTradeCap, SizingMode, WinnerFollowConfig,
    WinnerFollowStrategy, build_idempotency_key,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_core::OrderIntent;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

const OBSERVED_UNIX: i64 = 1_700_000_000;
const MARKET: &str = "0xdispatch";

fn leader_wallet() -> WalletAddress {
    WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

fn second_wallet() -> WalletAddress {
    WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap()
}

fn observed_at() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(OBSERVED_UNIX).unwrap()
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

fn entry_trade(id: &str, market: &str, price: Decimal) -> IncomingTrade {
    let ts = observed_at();
    IncomingTrade {
        wallet: leader_wallet(),
        market_id: MarketId(VenueMarketId(market.to_string())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(price),
        contracts: ContractQty(100),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(id.to_string()),
    }
}

fn signal_for(trade: &IncomingTrade) -> LeaderSignal {
    LeaderSignal {
        leader: TraderId(trade.wallet),
        venue: VenueId::polymarket(),
        market_id: trade.market_id.clone(),
        outcome_id: trade.outcome_id,
        action: pe_core_types::LeaderAction::Entry,
        leader_side: trade.side,
        leader_price: trade.price,
        leader_size: Quantity(trade.contracts),
        observed_at: trade.observed_at,
        received_at: trade.received_at,
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        source_trade_id: trade.source_trade_id.clone(),
        action_confidence_ppm: ProbabilityPpm(1_000_000),
    }
}

fn dispatch_id_for(trade: &IncomingTrade) -> String {
    build_idempotency_key(&signal_for(trade))
}

fn mid_cache_for(market: &str, price: &str) -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "http://gamma.test";
    let url = format!("{BASE}/markets?condition_ids={market}&limit=500");
    let body = format!(
        r#"[{{"conditionId":"{market}","outcomePrices":"[\"{price}\",\"{price}\"]","clobTokenIds":"[\"{market}-0\",\"{market}-1\"]"}}]"#
    );
    MidPriceCache::with_fetcher(
        FixtureFetcher::new(HashMap::from([(url, body.into_bytes())])),
        BASE.to_string(),
    )
}

fn make_dispatcher(dir: &TempDir) -> ExecutionDispatcher {
    let writer = Writer::open(dir.path().join("paper.log")).unwrap();
    let executor = PaperExecutor::new(writer, SourceId("test.paper".into()), 500, 100);
    ExecutionDispatcher::paper_only(executor)
}

fn open_paper_state(dir: &TempDir) -> Arc<PaperStateDb> {
    let state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    state.init_bankroll(dec!(10000)).unwrap();
    state
}

fn paper_log_fills(dir: &TempDir) -> Vec<PaperFill> {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return Vec::new();
    }
    Reader::replay(path)
        .unwrap()
        .map(|frame| {
            let (_seq, envelope) = frame.unwrap();
            serde_json::from_slice(&envelope.payload).unwrap()
        })
        .collect()
}

fn base_snapshot() -> RuntimeConfig {
    let mut rc = RuntimeConfig::from_service_config(&ServiceConfig::default());
    rc.sizing_mode = SizingMode::Dollar { usd: dec!(100) };
    rc.per_trade_cap = PerTradeCap::Unlimited;
    rc.price_impact_cap_bps = 0;
    rc.max_resolution_horizon_secs = 0;
    rc.min_resolution_horizon_secs = 0;
    rc.max_fill_price = "0.90".to_string();
    rc.min_fill_price = "0".to_string();
    rc.fill_mode = FillMode::LeaderHaircut;
    rc
}

fn book(asks: &[(Decimal, Decimal)]) -> OrderBook {
    OrderBook {
        asks: asks
            .iter()
            .map(|&(price, size)| BookLevel { price, size })
            .collect(),
        fetched_at_ms: 0,
    }
}

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
        live_sizing_mode: None,
        live_sizing_dollar_usd: None,
        live_sizing_contracts: None,
        live_price_impact_cap_bps: 100,
        custody_wallet_address: None,
        custody_wallet_kind: None,
    }
}

fn credential(id: &str) -> CredentialMetaRow {
    CredentialMetaRow {
        account_id: id.to_string(),
        bundle_version: 1,
        key_id: "key-1".to_string(),
    }
}

fn live_accounts(rows: Vec<AccountRow>) -> LiveAccounts {
    let credentials = rows
        .iter()
        .map(|row| credential(&row.account_id))
        .collect::<Vec<_>>();
    LiveAccounts::new(LiveAccountsSnapshot::from_rows(rows, &credentials))
}

fn standard_armed_accounts() -> LiveAccounts {
    live_accounts(vec![
        account_row("partner", false, true, 1, "live_tiny"),
        account_row("primary-acct", true, true, 9, "live_tiny"),
        account_row("bench", false, false, 0, "live_tiny"),
    ])
}

fn all_unarmed_accounts() -> LiveAccounts {
    live_accounts(vec![
        account_row("primary-acct", true, true, 9, "off"),
        account_row("partner", false, false, 1, "live_tiny"),
    ])
}

async fn run_trade(
    dir: &TempDir,
    paper_state: Arc<PaperStateDb>,
    trade: IncomingTrade,
    runtime: RuntimeConfig,
    accounts: Option<LiveAccounts>,
    books: HashMap<String, OrderBook>,
    mid_price: &str,
) {
    let market = trade.market_id.0.0.clone();
    let (trade_tx, trade_rx) = mpsc::channel::<IncomingTrade>(8);
    trade_tx.send(trade).await.unwrap();
    drop(trade_tx);

    let orchestrator = Orchestrator::new(
        trade_rx,
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            bankroll: dec!(10000),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: dec!(0.90),
            min_fill_price: Decimal::ZERO,
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            fill_mode: FillMode::LeaderHaircut,
            clob_best_ask_fallback_haircut_bps: 100,
            entry_gate_config: CopyEntryGateConfig { fail_closed: false },
            runtime_config: Some(LiveRuntimeConfig::new(runtime)),
            live_accounts: accounts,
        },
        HashMap::new(),
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        make_dispatcher(dir),
        paper_state,
        PositionLedger::new(),
        new_shared_health(false),
        MarketEndCache::new(String::new()),
        mid_cache_for(&market, mid_price),
        mpsc::channel(1).1,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .unwrap();
    orchestrator.run(std::future::pending::<()>()).await;
}

fn assert_standard_targets(state: &PaperStateDb, dispatch_id: &str) {
    let targets = state.dispatch_targets(dispatch_id).unwrap();
    assert_eq!(
        targets
            .iter()
            .map(|target| target.account_id.as_str())
            .collect::<Vec<_>>(),
        vec!["primary-acct", "partner"]
    );
    assert_eq!(
        targets
            .iter()
            .map(|target| target.exec_rank)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(targets.iter().all(|target| {
        target.credential_bundle_version == 1 && target.credential_key_id == "key-1"
    }));
    assert!(targets.iter().all(|target| target.account_id != "bench"));
}

/// Return a deterministic representation of a one-fill paper log after replacing only the
/// orchestrator-owned wall-clock fields. The raw files are read first; their timestamps and
/// timestamp-derived hashes cannot be byte-identical because `handle_trade` calls `now_utc()`.
fn normalized_paper_log_bytes(dir: &TempDir) -> Vec<u8> {
    let path = dir.path().join("paper.log");
    let raw = fs::read(&path).unwrap();
    assert!(
        !raw.is_empty(),
        "paper log must contain its framed header and fill"
    );
    let mut normalized = Vec::new();
    for frame in Reader::replay(path).unwrap() {
        let (seq, envelope) = frame.unwrap();
        let mut fill: PaperFill = serde_json::from_slice(&envelope.payload).unwrap();
        fill.simulated_at = SourceTimestamp(OffsetDateTime::UNIX_EPOCH);
        normalized.extend(
            serde_json::to_vec(&serde_json::json!({
                "seq": seq,
                "source_id": envelope.source_id,
                "schema_version": envelope.schema_version,
                "parser_version": envelope.parser_version,
                "content_type": envelope.content_type,
                "payload": fill,
            }))
            .unwrap(),
        );
    }
    normalized
}

fn staged_seed(
    dispatch_id: &str,
    signal_json: String,
    source_trade_id: &str,
) -> DispatchSeedRecord {
    DispatchSeedRecord {
        dispatch_id: dispatch_id.to_string(),
        signal_json,
        source_trade_id: source_trade_id.to_string(),
        created_at_unix: 1_000,
        targets: vec![DispatchTargetSeed {
            account_id: "primary-acct".to_string(),
            credential_bundle_version: 1,
            credential_key_id: "key-1".to_string(),
        }],
    }
}

fn recovery_intent(dispatch_id: &str) -> OrderIntent {
    OrderIntent {
        strategy_id: StrategyId("winner-follow".to_string()),
        market_id: MarketId(VenueMarketId(MARKET.to_string())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        contracts: ContractQty(1),
        limit_price: Price(dec!(0.50)),
        validity_seconds: 30,
        idempotency_key: dispatch_id.to_string(),
    }
}

fn frozen_signal_json(wallet: WalletAddress) -> String {
    serde_json::json!({
        "schema_version": 1,
        "signal": {
            "leader": wallet,
            "observed_at": SourceTimestamp(observed_at()),
        }
    })
    .to_string()
}

/// PASS: one admitted paper BUY creates a seed whose dispatch id equals the fill idempotency key,
/// flips it to `ready/fill`, and freezes only the two armed accounts primary-first with their
/// credential bindings. FAIL: missing/wrong seed state, target order/binding, or disabled target.
#[tokio::test]
async fn scenario_dispatch_paper_fill_flips_ready_with_primary_first_targets() {
    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    run_trade(
        &dir,
        state.clone(),
        entry_trade("fill-1", MARKET, dec!(0.50)),
        base_snapshot(),
        Some(standard_armed_accounts()),
        HashMap::new(),
        "0.50",
    )
    .await;

    let fills = state.list_fills().unwrap();
    assert_eq!(fills.len(), 1);
    let dispatch_id = &fills[0].idempotency_key;
    let seed = state.dispatch_seed(dispatch_id).unwrap().unwrap();
    assert_eq!(seed.dispatch_id, *dispatch_id);
    assert_eq!(seed.state, "ready");
    assert_eq!(seed.paper_outcome.as_deref(), Some("fill"));
    assert_standard_targets(&state, dispatch_id);
    println!("PASS: paper fill flips ready/fill with frozen primary-first armed targets");
}

/// PASS: a zero-contract paper evaluation stages the live aggregate, records a typed
/// `no_fill:paper_reject:*` outcome, preserves both targets, and records no paper fill.
/// FAIL: no aggregate, an untyped/wrong outcome, target drift, or any fill.
#[tokio::test]
async fn scenario_dispatch_paper_no_edge_flips_typed_no_fill_with_targets_intact() {
    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    let trade = entry_trade("no-edge-1", MARKET, dec!(0.50));
    let dispatch_id = dispatch_id_for(&trade);
    let mut runtime = base_snapshot();
    runtime.sizing_mode = SizingMode::Contract { contracts: 0 };
    run_trade(
        &dir,
        state.clone(),
        trade,
        runtime,
        Some(standard_armed_accounts()),
        HashMap::new(),
        "0.50",
    )
    .await;

    let seed = state.dispatch_seed(&dispatch_id).unwrap().unwrap();
    assert_eq!(seed.state, "ready");
    assert!(
        seed.paper_outcome
            .as_deref()
            .is_some_and(|outcome| outcome.starts_with("no_fill:paper_reject:"))
    );
    assert_standard_targets(&state, &dispatch_id);
    assert!(state.list_fills().unwrap().is_empty());
    assert!(paper_log_fills(&dir).is_empty());
    println!("PASS: paper NoEdge flips a typed no-fill while frozen targets stay intact");
}

/// PASS: a paper position present before orchestrator construction moves the hold gate after
/// staging: the seed is `ready/no_fill:paper_held`, targets remain intact, and no new fill exists.
/// FAIL: the held paper position suppresses staging/live targets or permits another fill.
#[tokio::test]
async fn scenario_dispatch_paper_held_flips_typed_no_fill_without_second_fill() {
    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    state
        .upsert_position(
            &MarketId(VenueMarketId(MARKET.to_string())),
            OutcomeId(0),
            1,
            0,
        )
        .unwrap();
    let trade = entry_trade("held-1", MARKET, dec!(0.50));
    let dispatch_id = dispatch_id_for(&trade);
    run_trade(
        &dir,
        state.clone(),
        trade,
        base_snapshot(),
        Some(standard_armed_accounts()),
        HashMap::new(),
        "0.50",
    )
    .await;

    let seed = state.dispatch_seed(&dispatch_id).unwrap().unwrap();
    assert_eq!(seed.state, "ready");
    assert_eq!(seed.paper_outcome.as_deref(), Some("no_fill:paper_held"));
    assert_standard_targets(&state, &dispatch_id);
    assert!(state.list_fills().unwrap().is_empty());
    assert!(paper_log_fills(&dir).is_empty());
    println!("PASS: a held paper position flips paper_held after live-target staging");
}

/// PASS: a usable 100-bps book with only 0.5 shares at 0.50 cannot absorb one whole share of
/// the $100 paper budget, but still stages targets before flipping
/// `ready/no_fill:impact_absorbs_zero`. FAIL: pre-staging suppression or a paper fill.
#[tokio::test]
async fn scenario_dispatch_zero_absorb_is_paper_only_after_staging() {
    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    let trade = entry_trade("zero-absorb-1", MARKET, dec!(0.50));
    let dispatch_id = dispatch_id_for(&trade);
    let mut runtime = base_snapshot();
    runtime.price_impact_cap_bps = 100;
    let books = HashMap::from([(format!("{MARKET}-0"), book(&[(dec!(0.50), dec!(0.5))]))]);
    run_trade(
        &dir,
        state.clone(),
        trade,
        runtime,
        Some(standard_armed_accounts()),
        books,
        "0.50",
    )
    .await;

    let seed = state.dispatch_seed(&dispatch_id).unwrap().unwrap();
    assert_eq!(seed.state, "ready");
    assert_eq!(
        seed.paper_outcome.as_deref(),
        Some("no_fill:impact_absorbs_zero")
    );
    assert_standard_targets(&state, &dispatch_id);
    assert!(state.list_fills().unwrap().is_empty());
    println!("PASS: zero absorb is paper-only and flips after aggregate staging");
}

/// PASS: with the impact gate enabled and the token absent from the fixture book map, the shared
/// quote is unusable and suppresses every destination before staging: no seed and no fill.
/// FAIL: any dispatch aggregate or paper fill exists.
#[tokio::test]
async fn scenario_dispatch_unusable_shared_quote_suppresses_all_destinations_pre_staging() {
    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    let trade = entry_trade("quote-missing-1", MARKET, dec!(0.50));
    let dispatch_id = dispatch_id_for(&trade);
    let mut runtime = base_snapshot();
    runtime.price_impact_cap_bps = 100;
    run_trade(
        &dir,
        state.clone(),
        trade,
        runtime,
        Some(standard_armed_accounts()),
        HashMap::new(),
        "0.50",
    )
    .await;

    assert!(state.dispatch_seed(&dispatch_id).unwrap().is_none());
    assert!(state.pending_dispatch_seeds().unwrap().is_empty());
    assert!(state.unfinalized_ready_dispatch_seeds().unwrap().is_empty());
    assert!(state.list_fills().unwrap().is_empty());
    println!("PASS: unusable shared quote suppresses all destinations before staging");
}

/// PASS: gate-off CLOB best-ask basis 0.60 is at/above `max_fill_price=0.50`, so the shared band
/// rejects the BUY before staging and records neither a seed nor a fill.
/// FAIL: any aggregate or fill is created.
#[tokio::test]
async fn scenario_dispatch_shared_band_rejection_creates_no_aggregate() {
    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    let trade = entry_trade("band-reject-1", MARKET, dec!(0.50));
    let dispatch_id = dispatch_id_for(&trade);
    let mut runtime = base_snapshot();
    runtime.max_fill_price = "0.50".to_string();
    runtime.fill_mode = FillMode::ClobBestAsk;
    let books = HashMap::from([(format!("{MARKET}-0"), book(&[(dec!(0.60), dec!(100))]))]);
    run_trade(
        &dir,
        state.clone(),
        trade,
        runtime,
        Some(standard_armed_accounts()),
        books,
        "0.60",
    )
    .await;

    assert!(state.dispatch_seed(&dispatch_id).unwrap().is_none());
    assert!(state.list_fills().unwrap().is_empty());
    println!("PASS: shared fill-price band rejection creates no dispatch aggregate");
}

/// PASS: `live_accounts=None` and an all-unarmed snapshot produce the same one-fill paper state,
/// no dispatch seeds, and byte-identical normalized event-log content after replacing only the
/// unavoidable `now_utc()` fields. FAIL: paper behavior diverges or either path stages a seed.
#[tokio::test]
async fn scenario_dispatch_zero_live_targets_preserve_phase_a_baseline() {
    let none_dir = TempDir::new().unwrap();
    let unarmed_dir = TempDir::new().unwrap();
    let none_state = open_paper_state(&none_dir);
    let unarmed_state = open_paper_state(&unarmed_dir);
    let trade = entry_trade("baseline-1", MARKET, dec!(0.50));
    let dispatch_id = dispatch_id_for(&trade);

    run_trade(
        &none_dir,
        none_state.clone(),
        trade.clone(),
        base_snapshot(),
        None,
        HashMap::new(),
        "0.50",
    )
    .await;
    run_trade(
        &unarmed_dir,
        unarmed_state.clone(),
        trade,
        base_snapshot(),
        Some(all_unarmed_accounts()),
        HashMap::new(),
        "0.50",
    )
    .await;

    let none_fills = none_state.list_fills().unwrap();
    let unarmed_fills = unarmed_state.list_fills().unwrap();
    assert_eq!(none_fills.len(), 1);
    assert_eq!(unarmed_fills.len(), 1);
    assert_eq!(
        none_fills[0].idempotency_key,
        unarmed_fills[0].idempotency_key
    );
    assert_eq!(none_fills[0].market_id, unarmed_fills[0].market_id);
    assert_eq!(none_fills[0].outcome_id, unarmed_fills[0].outcome_id);
    assert_eq!(none_fills[0].side, unarmed_fills[0].side);
    assert_eq!(none_fills[0].contracts, unarmed_fills[0].contracts);
    assert_eq!(none_fills[0].fill_price, unarmed_fills[0].fill_price);
    assert!(none_state.dispatch_seed(&dispatch_id).unwrap().is_none());
    assert!(unarmed_state.dispatch_seed(&dispatch_id).unwrap().is_none());
    assert_eq!(
        normalized_paper_log_bytes(&none_dir),
        normalized_paper_log_bytes(&unarmed_dir),
        "live-account plumbing must not alter deterministic paper event content"
    );
    println!("PASS: zero live targets preserve the Phase-A one-fill paper baseline");
}

/// PASS: boot recovery finds a durable `PaperFill` whose idempotency key matches a pending seed
/// and flips exactly that seed to `ready/fill`. FAIL: the seed remains pending or is finalized as
/// a no-fill.
#[tokio::test]
async fn scenario_dispatch_boot_resume_flips_seed_from_durable_fill() {
    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    let dispatch_id = "wf|durable-fill";
    state
        .stage_dispatch_seed(&staged_seed(dispatch_id, "{}".to_string(), "recover-fill"))
        .unwrap();
    let log_path = dir.path().join("paper.log");
    let writer = Writer::open(&log_path).unwrap();
    let mut executor = PaperExecutor::new(writer, SourceId("test.paper".into()), 0, 0);
    executor
        .execute(
            &recovery_intent(dispatch_id),
            SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            None,
        )
        .unwrap();

    let resumed = resume_dispatch_seeds(&log_path, &state).unwrap();
    assert_eq!(resumed.flipped_fill, 1);
    assert_eq!(resumed.finalized_stuck, 0);
    assert_eq!(resumed.left_pending, 0);
    let seed = state.dispatch_seed(dispatch_id).unwrap().unwrap();
    assert_eq!(seed.state, "ready");
    assert_eq!(seed.paper_outcome.as_deref(), Some("fill"));
    println!("PASS: boot resume flips a pending seed from its durable fill frame");
}

/// PASS: with no fill frame, a cursor past the frozen signal finalizes the seed with
/// `STUCK_SEED_OUTCOME`, while a twin wallet cursor behind its observed time leaves that seed
/// pending for redelivery. FAIL: either recovery rule crosses into the other case.
#[tokio::test]
async fn scenario_dispatch_boot_resume_finalizes_stuck_seed_and_leaves_redeliverable_twin_pending()
{
    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    state
        .stage_dispatch_seed(&staged_seed(
            "stuck-seed",
            frozen_signal_json(leader_wallet()),
            "stuck-source",
        ))
        .unwrap();
    state
        .stage_dispatch_seed(&staged_seed(
            "redeliverable-seed",
            frozen_signal_json(second_wallet()),
            "redeliverable-source",
        ))
        .unwrap();
    state
        .set_cursor(&leader_wallet(), OBSERVED_UNIX + 1)
        .unwrap();
    state
        .set_cursor(&second_wallet(), OBSERVED_UNIX - 1)
        .unwrap();

    let resumed = resume_dispatch_seeds(&dir.path().join("missing-paper.log"), &state).unwrap();
    assert_eq!(resumed.flipped_fill, 0);
    assert_eq!(resumed.finalized_stuck, 1);
    assert_eq!(resumed.left_pending, 1);
    let stuck = state.dispatch_seed("stuck-seed").unwrap().unwrap();
    assert_eq!(stuck.state, "ready");
    assert_eq!(stuck.paper_outcome.as_deref(), Some(STUCK_SEED_OUTCOME));
    let redeliverable = state.dispatch_seed("redeliverable-seed").unwrap().unwrap();
    assert_eq!(redeliverable.state, "pending_paper");
    assert_eq!(redeliverable.paper_outcome, None);
    println!("PASS: boot resume finalizes only the stuck seed and leaves its twin pending");
}

/// PASS: account sorting is primary-first then `(execution_order, account_id)`, and the current
/// v1 maximum freezes only `[primary-acct, alpha]` for both three- and four-armed snapshots.
/// FAIL: tie-break ordering changes or an excess account escapes the two-target bound.
#[tokio::test]
async fn scenario_dispatch_ordering_and_v1_target_bound_are_frozen() {
    let three_rows = vec![
        account_row("zeta", false, true, 1, "live_tiny"),
        account_row("primary-acct", true, true, 9, "live_tiny"),
        account_row("alpha", false, true, 1, "live_tiny"),
    ];
    let three_snapshot = {
        let creds = three_rows
            .iter()
            .map(|row| credential(&row.account_id))
            .collect::<Vec<_>>();
        LiveAccountsSnapshot::from_rows(three_rows, &creds)
    };
    assert_eq!(
        three_snapshot
            .accounts
            .iter()
            .map(|account| account.account_id.as_str())
            .collect::<Vec<_>>(),
        vec!["primary-acct", "alpha", "zeta"],
        "the full snapshot proves the alpha/zeta account-id tie-break"
    );
    assert_eq!(
        three_snapshot
            .armed_targets()
            .iter()
            .map(|account| account.account_id.as_str())
            .collect::<Vec<_>>(),
        vec!["primary-acct", "alpha"],
        "the v1 armed-target bound is two"
    );

    let dir = TempDir::new().unwrap();
    let state = open_paper_state(&dir);
    let trade = entry_trade("ordering-4", MARKET, dec!(0.50));
    let dispatch_id = dispatch_id_for(&trade);
    let four_accounts = live_accounts(vec![
        account_row("zeta", false, true, 1, "live_tiny"),
        account_row("beta", false, true, 2, "live_tiny"),
        account_row("primary-acct", true, true, 9, "live_tiny"),
        account_row("alpha", false, true, 1, "live_tiny"),
    ]);
    run_trade(
        &dir,
        state.clone(),
        trade,
        base_snapshot(),
        Some(four_accounts),
        HashMap::new(),
        "0.50",
    )
    .await;
    let targets = state.dispatch_targets(&dispatch_id).unwrap();
    assert_eq!(
        targets
            .iter()
            .map(|target| target.account_id.as_str())
            .collect::<Vec<_>>(),
        vec!["primary-acct", "alpha"]
    );
    assert_eq!(targets[0].exec_rank, 0);
    assert_eq!(targets[1].exec_rank, 1);
    println!("PASS: primary/alpha/zeta ordering is deterministic and v1 freezes only two targets");
}
