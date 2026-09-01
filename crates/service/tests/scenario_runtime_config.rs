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
    BasisPoints, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{Reader, Writer};
use pe_execution_core::ExecutionDispatcher;
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_service::clob_book::{BookLevel, FixtureClobBookFetcher, OrderBook};
use pe_service::config::ServiceConfig;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::runtime_config::{FillMode, LiveRuntimeConfig, RuntimeConfig};
use pe_source_polymarket_public::FixtureFetcher;
use pe_strategy_winner_follow::{
    ExecutionMode, FillSource, PaperExecutor, PaperFill, PerTradeCap, SizingMode,
    WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

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
    const BASE: &str = "http://gamma.test";
    let mut fx = HashMap::new();
    let url = format!("{BASE}/markets?condition_ids={hex}&limit=500");
    // clobTokenIds (outcome i → "{hex}-{i}") lets the price-impact gate resolve the /book token.
    let body = format!(
        r#"[{{"conditionId":"{hex}","outcomePrices":"[\"{price}\",\"{price}\"]","clobTokenIds":"[\"{hex}-0\",\"{hex}-1\"]"}}]"#
    );
    fx.insert(url, body.into_bytes());
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string())
}

fn make_dispatcher(dir: &TempDir) -> ExecutionDispatcher {
    let paper_writer = Writer::open(dir.path().join("paper.log")).unwrap();
    let paper_executor = PaperExecutor::new(paper_writer, SourceId("test.paper".into()), 500, 100);
    ExecutionDispatcher::paper_only(paper_executor)
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
fn flat_snapshot(max_fill_price: &str, bankroll_usd: &str) -> RuntimeConfig {
    let mut rc = RuntimeConfig::from_service_config(&ServiceConfig::default());
    rc.sizing_mode = SizingMode::Dollar { usd: dec!(100) };
    rc.max_resolution_horizon_secs = 0;
    rc.min_resolution_horizon_secs = 0;
    rc.max_fill_price = max_fill_price.to_string();
    rc.bankroll_usd = bankroll_usd.to_string();
    // #486: pin the pre-feature haircut basis so these gate scenarios stay on the ×1.05 fill.
    rc.fill_mode = FillMode::LeaderHaircut;
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

    let orch = Orchestrator::new(
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
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            fill_mode: FillMode::LeaderHaircut,
            clob_best_ask_fallback_haircut_bps: 100,
            // Boot strategy is flat $100 too; the snapshot (when present) overrides it via rebuild.
            entry_gate_config: CopyEntryGateConfig,
            runtime_config,
            live_accounts: None,
        },
        WinnerFollowStrategy::new(WinnerFollowConfig {
            sizing_mode: SizingMode::Dollar { usd: dec!(100) },
            ..WinnerFollowConfig::default()
        }),
        make_dispatcher(dir),
        paper_state.clone(),
        PositionLedger::new(),
        new_shared_health(false),
        MarketEndCache::new(String::new()),
        mid_cache_for(HEX, mid_price),
        mpsc::channel(1).1,
        None,
        None,
        None,
        Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
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
    let live = LiveRuntimeConfig::new(flat_snapshot("0.50", "10000"));
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

/// PASS: a config `bankroll_usd` baseline of "1" never becomes the running bankroll — after a
///       $100-flat fill the running bankroll is debited from 10000 (not reset to the baseline),
///       proving the config path has no writer to the running bankroll (no re-credit).
/// FAIL: the running bankroll equals the "1" baseline (the config wrongly overwrote it).
#[tokio::test]
async fn config_bankroll_baseline_never_becomes_running_bankroll() {
    let dir = TempDir::new().unwrap();
    let live = LiveRuntimeConfig::new(flat_snapshot("0.90", "1"));
    let (fills, bankroll) = run_with(&dir, Some(live), dec!(0.90), "0.60").await;
    assert_eq!(fills, 1, "the BUY should fill under the 0.90 cap");
    assert!(
        bankroll != dec!(1) && bankroll < Decimal::from(10_000u32) && bankroll > Decimal::ZERO,
        "running bankroll {bankroll} must be debited from 10000 by the fill, never reset to the baseline (1)"
    );
    println!(
        "PASS: config bankroll_usd baseline never re-credits the running bankroll ({bankroll})"
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
    rc.max_fill_price = "0.90".to_string();
    // #486: the price-impact scenarios assert the ×1.05 haircut basis (floor(100/(0.50×1.05))=190),
    // so pin leader_haircut — the best-ask basis would reprice the fallback to ×1.01.
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

    let orch = Orchestrator::new(
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
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            fill_mode: FillMode::LeaderHaircut,
            clob_best_ask_fallback_haircut_bps: 100,
            entry_gate_config: CopyEntryGateConfig,
            runtime_config: Some(LiveRuntimeConfig::new(rc)),
            live_accounts: None,
        },
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        make_dispatcher(dir),
        paper_state.clone(),
        PositionLedger::new(),
        new_shared_health(false),
        MarketEndCache::new(String::new()),
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
fn first_paper_fill_full(dir: &TempDir) -> Option<(u64, Decimal, FillSource)> {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return None;
    }
    let (_seq, env) = Reader::replay(&path).unwrap().next()?.unwrap();
    let fill: PaperFill = serde_json::from_slice(&env.payload).unwrap();
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
    rc.fill_mode = FillMode::ClobBestAsk; // the production fill mode (#486)
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
    assert_eq!(source, FillSource::ClobBestAsk);
    println!("PASS: gate-on clob_best_ask fill = 199 contracts at exact ladder VWAP (#508)");
}
