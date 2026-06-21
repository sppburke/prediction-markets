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
    ExecutionMode, PaperExecutor, WinnerFollowConfig, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::{FixtureCLOBClient, PolymarketCredentials, PolymarketVenueAdapter};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::sync::mpsc;

// ── Helpers (mirror scenario_execution_gates) ───────────────────────────────────

fn leader_wallet() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
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
        contracts: ContractQty(100),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(id.to_string()),
    }
}

fn mid_cache_for(hex: &str, price: &str) -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "http://gamma.test";
    let mut fx = HashMap::new();
    let url = format!("{BASE}/markets?condition_ids={hex}&limit=500");
    let body =
        format!(r#"[{{"conditionId":"{hex}","outcomePrices":"[\"{price}\",\"{price}\"]"}}]"#);
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

/// A flat-fill snapshot ($100/trade) with the resolution-horizon gate disabled, so only the
/// snapshot's `max_fill_price` decides fill-vs-skip. The boot strategy/gates are deliberately
/// different where it matters, to prove the per-event rebuild reads the snapshot, not boot.
fn flat_snapshot(max_fill_price: &str, bankroll_usd: &str) -> RuntimeConfig {
    let mut rc = RuntimeConfig::from_service_config(&ServiceConfig::default());
    rc.flat_usd_per_trade = Some(dec!(100));
    rc.max_resolution_horizon_secs = 0;
    rc.min_resolution_horizon_secs = 0;
    rc.max_fill_price = max_fill_price.to_string();
    rc.bankroll_usd = bankroll_usd.to_string();
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
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: boot_max_fill,
            // Boot strategy is flat $100 too; the snapshot (when present) overrides it via rebuild.
            entry_gate_config: CopyEntryGateConfig { fail_closed: false },
            runtime_config,
        },
        HashMap::new(),
        WinnerFollowStrategy::new(WinnerFollowConfig {
            flat_usd_per_trade: Some(dec!(100)),
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
