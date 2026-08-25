//! Scenario tests for the copy-entry gate (issue #290) end-to-end through the
//! orchestrator: a leader trade flows `handle_trade` → `CopyEntryGate::admit` →
//! strategy → dispatcher, and we assert on the resulting paper-fill count.
//!
//! A fill is produced only when the gate admits the signal AND the flat-sizing
//! strategy path fills; the contrast against an otherwise-identical admitted case
//! isolates the gate as the cause of a 0-fill outcome.
//!
//! Determinism: fixed wallet/market/timestamps, no clocks, no network. Each
//! scenario prints a PASS line.
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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, SignalConfig};
use pe_core_types::{
    BasisPoints, ContractQty, MarketId, OutcomeId, Price, ReconstructionQuality, Side, SourceId,
    SourceTimestamp, SourceTradeId, VenueMarketId, WalletAddress,
};
use pe_event_log::{Reader, Writer};
use pe_execution_core::ExecutionDispatcher;
use pe_paper_state::PaperStateDb;
use pe_position_ledger::PositionLedger;
use pe_service::clob_book::{
    BookLevel, ClobBookError, ClobBookFetcher, FixtureClobBookFetcher, OrderBook,
};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::market_end_cache::MarketEndCache;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::runtime_config::FillMode;
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

// ── Helpers ───────────────────────────────────────────────────────────────────

fn leader_wallet() -> WalletAddress {
    serde_json::from_str("\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"").unwrap()
}

fn market(hex: &str) -> MarketId {
    MarketId(VenueMarketId(hex.to_string()))
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

/// A BUY (Entry from a fresh ledger) into `market_id` at `price`, 100 contracts.
fn entry_trade(id: &str, market_id: MarketId, price: Decimal) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    IncomingTrade {
        wallet: leader_wallet(),
        market_id,
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(price),
        contracts: ContractQty(100),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(id.to_string()),
        provenance: TradeProvenance::RestPoll,
    }
}

/// Strategy config that deterministically fills via the flat sizing path. The per-trade
/// cap is `Unlimited` so these gate/sizing scenarios measure the sizing math itself, not a
/// bankroll-percentage clamp (the cap is exercised by the risk-engine's own tests).
fn flat_fill_config() -> WinnerFollowConfig {
    WinnerFollowConfig {
        sizing_mode: SizingMode::Dollar { usd: dec!(100) },
        per_trade_cap: PerTradeCap::Unlimited,
        ..WinnerFollowConfig::default()
    }
}

/// First-entry gate with the given fail-closed posture (no band since #339).
fn band_config(fail_closed: bool) -> CopyEntryGateConfig {
    CopyEntryGateConfig { fail_closed }
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
        let body =
            format!(r#"[{{"conditionId":"{m}","outcomePrices":"[\"{price}\",\"{price}\"]"}}]"#);
        fx.insert(url, body.into_bytes());
    }
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string())
}

fn make_dispatcher(dir: &TempDir) -> ExecutionDispatcher {
    let paper_writer = Writer::open(dir.path().join("paper.log")).unwrap();
    let paper_executor = PaperExecutor::new(paper_writer, SourceId("test.paper".into()), 500, 100);
    ExecutionDispatcher::paper_only(paper_executor)
}

fn dead_reseed_rx() -> mpsc::Receiver<pe_service::orchestrator_control::OrchestratorControl> {
    mpsc::channel(1).1
}

fn paper_fill_count(dir: &TempDir) -> usize {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return 0;
    }
    Reader::replay(&path).unwrap().count()
}

/// The FIRST paper fill's `(contracts, simulated_fill_price)`, replayed from the log.
fn first_paper_fill(dir: &TempDir) -> Option<(u64, Decimal)> {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return None;
    }
    let (_seq, env) = Reader::replay(&path).unwrap().next()?.unwrap();
    let fill: PaperFill = serde_json::from_slice(&env.payload).unwrap();
    Some((fill.intent.contracts.0, fill.simulated_fill_price.0))
}

/// Every paper fill side in event-sequence order.
fn paper_fill_sides(dir: &TempDir) -> Vec<Side> {
    let path = dir.path().join("paper.log");
    if !path.exists() {
        return Vec::new();
    }
    Reader::replay(&path)
        .unwrap()
        .map(|frame| {
            let (_seq, env) = frame.unwrap();
            let fill: PaperFill = serde_json::from_slice(&env.payload).unwrap();
            fill.intent.side
        })
        .collect()
}

/// Feed `trades` through a fresh orchestrator wired with `gate_config` + `history`
/// and a flat-fill strategy in Paper mode; return the resulting paper-fill count.
async fn run_gate(
    dir: &TempDir,
    gate_config: CopyEntryGateConfig,
    history: HashMap<WalletAddress, HashSet<MarketId>>,
    trades: Vec<IncomingTrade>,
) -> usize {
    // Fill-price band disabled; markets quoted at 0.60 (in flat-fill range).
    run_gate_capped(
        dir,
        gate_config,
        history,
        trades,
        Decimal::ZERO,
        Decimal::ZERO,
        "0.60",
    )
    .await
}

/// Like [`run_gate`] but with an explicit fill-price band (`max_fill_price` cap +
/// `min_fill_price` floor) and `mid_price` quote, to exercise the #339 current-price
/// cap gate and the run28 band-floor gate (#468 parity).
async fn run_gate_capped(
    dir: &TempDir,
    gate_config: CopyEntryGateConfig,
    history: HashMap<WalletAddress, HashSet<MarketId>>,
    trades: Vec<IncomingTrade>,
    max_fill_price: Decimal,
    min_fill_price: Decimal,
    mid_price: &str,
) -> usize {
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    let markets: Vec<MarketId> = trades.iter().map(|t| t.market_id.clone()).collect();
    let mid_price_cache = mid_cache_for(&markets, mid_price);

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
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0, // horizon gate disabled; isolate the entry gate
            min_resolution_horizon_secs: 0,
            max_fill_price,
            min_fill_price,
            // Match the dispatcher's PaperExecutor haircut/slippage so sizing (fill_basis
            // = leader × 1.05) equals the recorded fill.
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            // #486: pin the pre-feature haircut basis (no /book fetch) so these existing gate
            // scenarios stay on the ×1.05 fill; the best-ask path is exercised separately below.
            fill_mode: FillMode::LeaderHaircut,
            clob_best_ask_fallback_haircut_bps: 100,
            entry_gate_config: gate_config,
            runtime_config: None,
            live_accounts: None,
        },
        history,
        WinnerFollowStrategy::new(flat_fill_config()),
        make_dispatcher(dir),
        paper_state,
        PositionLedger::new(),
        new_shared_health(false),
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

    paper_fill_count(dir)
}

fn history_with(
    wallet: WalletAddress,
    markets: &[MarketId],
) -> HashMap<WalletAddress, HashSet<MarketId>> {
    let mut map = HashMap::new();
    map.insert(wallet, markets.iter().cloned().collect());
    map
}

// ── First-entry scenarios ─────────────────────────────────────────────────────

/// PASS: a first entry into a market NOT in the leader's history (but the leader is
///       known) produces exactly one fill.
/// FAIL: zero fills (gate wrongly treated a new market as a re-entry).
#[tokio::test]
async fn first_entry_admits_new_market() {
    let dir = TempDir::new().unwrap();
    let history = history_with(leader_wallet(), &[market("0xother")]);
    let fills = run_gate(
        &dir,
        band_config(false),
        history,
        vec![entry_trade("new-market", market("0xnew"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 1);
    println!("PASS: first-entry admits a market absent from leader history (1 fill)");
}

/// PASS: an entry into a market ALREADY in the leader's history produces zero fills.
/// FAIL: any fill (gate failed to block a re-entry).
#[tokio::test]
async fn first_entry_blocks_known_market() {
    let dir = TempDir::new().unwrap();
    let history = history_with(leader_wallet(), &[market("0xknown")]);
    let fills = run_gate(
        &dir,
        band_config(false),
        history,
        vec![entry_trade("known", market("0xknown"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: first-entry blocks a market already in leader history (0 fills)");
}

/// PASS: a SELL observed from a flat leader ledger is rejected, and because it does not
///       consume market-level first-entry history, a later BUY of the other outcome in the
///       same market is admitted. The only recorded fill side is BUY.
/// FAIL: a SELL fill is recorded, or the rejected SELL prevents the later BUY.
#[tokio::test]
async fn sell_is_rejected_without_consuming_first_entry_history() {
    let dir = TempDir::new().unwrap();
    let shared_market = market("0xbuy-only");
    let mut sell = entry_trade("sell-first", shared_market.clone(), dec!(0.50));
    sell.side = Side::Sell;
    let mut buy = entry_trade("buy-second", shared_market, dec!(0.50));
    buy.outcome_id = OutcomeId(1);

    let fills = run_gate(&dir, band_config(false), HashMap::new(), vec![sell, buy]).await;

    assert_eq!(fills, 1);
    assert_eq!(paper_fill_sides(&dir), vec![Side::Buy]);
    println!("PASS: SELL is rejected without consuming first-entry history; later BUY fills once");
}

// ── Fail-open / fail-closed posture ───────────────────────────────────────────

/// PASS: with the leader ABSENT from the history map and the gate fail-open, an
///       in-band first entry is admitted (one fill).
/// FAIL: zero fills (fail-open wrongly blocked an unknown wallet).
#[tokio::test]
async fn fail_open_admits_absent_wallet() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate(
        &dir,
        band_config(false),
        HashMap::new(), // leader absent → unknown history
        vec![entry_trade("fail-open", market("0xnew"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 1);
    println!("PASS: fail-open admits an entry from a wallet absent from history (1 fill)");
}

/// PASS: with the leader ABSENT from the history map and the gate fail-closed, an
///       in-band entry is blocked (zero fills).
/// FAIL: any fill (fail-closed wrongly admitted an unknown wallet).
#[tokio::test]
async fn fail_closed_blocks_absent_wallet() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate(
        &dir,
        band_config(true),
        HashMap::new(), // leader absent → unknown history
        vec![entry_trade("fail-closed", market("0xnew"), dec!(0.60))],
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: fail-closed blocks an entry from a wallet absent from history (0 fills)");
}

// ── Current-price cap scenarios (#339) ────────────────────────────────────────

/// PASS: a first entry whose CURRENT market price (0.60) is at/above `max_fill_price`
///       (0.50) produces zero fills — the BUY cap suppresses it.
/// FAIL: any fill (the max_fill_price cap failed to fire).
#[tokio::test]
async fn max_fill_price_blocks_high_current_price() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate_capped(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("capped", market("0xnew"), dec!(0.60))],
        dec!(0.50), // cap below the 0.60 current price
        Decimal::ZERO,
        "0.60",
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: max_fill_price caps a BUY whose current price is at/above the cap (0 fills)");
}

/// PASS: the same entry with the cap ABOVE the current price (0.70 > 0.60) fills.
/// FAIL: zero fills (the cap wrongly suppressed an in-range BUY).
#[tokio::test]
async fn max_fill_price_admits_below_cap() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate_capped(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("under-cap", market("0xnew"), dec!(0.60))],
        dec!(0.70), // cap above the 0.60 current price
        Decimal::ZERO,
        "0.60",
    )
    .await;
    assert_eq!(fills, 1);
    println!("PASS: max_fill_price admits a BUY whose current price is below the cap (1 fill)");
}

// ── Band-floor scenarios (run28 cutover; #468 selection↔deployment parity) ─────

/// PASS: a first entry whose CURRENT market price (0.10) is below `min_fill_price`
///       (0.15) produces zero fills — the band floor suppresses it.
/// FAIL: any fill (the min_fill_price floor failed to fire).
#[tokio::test]
async fn min_fill_price_blocks_low_current_price() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate_capped(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("floored", market("0xnew"), dec!(0.10))],
        Decimal::ZERO,
        dec!(0.15), // floor above the 0.10 current price
        "0.10",
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: min_fill_price blocks a BUY whose current price is below the floor (0 fills)");
}

/// PASS: a copy whose FILL price is EXACTLY the floor fills — the bound is inclusive
///       (`< floor` skips), mirroring the backtest `min_signal_price` semantics. Leader
///       0.15 → fill 0.15 × 1.05 = 0.1575, and the floor is set to exactly 0.1575, so the
///       comparison is `0.1575 < 0.1575` = false → admit. (Pins the strict-`<` boundary
///       on the fill basis; a `<`→`<=` regression would flip this to 0 fills.)
/// FAIL: zero fills (the floor wrongly suppressed the boundary value).
#[tokio::test]
async fn min_fill_price_admits_boundary_value() {
    let dir = TempDir::new().unwrap();
    let fills = run_gate_capped(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("at-floor", market("0xnew"), dec!(0.15))],
        Decimal::ZERO,
        dec!(0.1575), // floor == the fill price (leader 0.15 × 1.05)
        "0.60",       // mid irrelevant to gating now; kept in flat-fill range
    )
    .await;
    assert_eq!(fills, 1);
    println!("PASS: min_fill_price admits a BUY whose fill price is exactly the floor (1 fill)");
}

// ── Fill-basis sizing/gating (this PR: size + gate off the fill price, not the mid) ──

/// PASS: with the Gamma mid (0.20) diverging sharply from the leader price (0.50), the
///       flat-$100 copy's notional (contracts × fill) is ~$100 — sized off the fill
///       price, NOT the mid. The old mid-based sizing (floor(100/0.20)=500 contracts)
///       would have produced a ~$260 notional.
/// FAIL: notional tracks the divergent mid (≫ $100).
#[tokio::test]
async fn notional_stable_when_mid_diverges_from_leader() {
    let dir = TempDir::new().unwrap();
    // Caps disabled (Decimal::ZERO) to isolate sizing; leader 0.50, mid 0.20 (2.5× off).
    let fills = run_gate_capped(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("diverge", market("0xnew"), dec!(0.50))],
        Decimal::ZERO,
        Decimal::ZERO,
        "0.20",
    )
    .await;
    assert_eq!(fills, 1);
    let (contracts, fill_price) = first_paper_fill(&dir).expect("a fill was recorded");
    // fill = 0.50 × 1.05 = 0.525; contracts = floor(100/0.525) = 190; notional = 99.75.
    let notional = Decimal::from(contracts) * fill_price;
    assert!(
        (notional - dec!(100)).abs() <= fill_price,
        "notional {notional} must be within one contract of $100 \
         (fill {fill_price}, contracts {contracts})"
    );
    println!(
        "PASS: notional ${notional} sized off the fill price, not the 0.20 mid \
         (contracts {contracts} @ {fill_price})"
    );
}

/// PASS: a leader BUY at 0.86 is rejected by the 0.85 max cap, because the FILL price
///       0.86 × 1.05 = 0.903 ≥ 0.85 — the exact case a mid-based gate let through live.
/// FAIL: any fill (the cap keyed off a mid < 0.85 and missed the 0.903 fill).
#[tokio::test]
async fn max_cap_rejects_leader_whose_fill_exceeds_cap() {
    let dir = TempDir::new().unwrap();
    // Mid quotes 0.83 (would pass a mid-based 0.85 cap); leader 0.86 → fill 0.903.
    let fills = run_gate_capped(
        &dir,
        band_config(false),
        HashMap::new(),
        vec![entry_trade("over-cap-fill", market("0xnew"), dec!(0.86))],
        dec!(0.85),
        Decimal::ZERO,
        "0.83",
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: max cap rejects leader 0.86 (fill 0.903 ≥ 0.85) despite a 0.83 mid");
}

// ── Best-ask fill basis (#486) ────────────────────────────────────────────────
// A paper BUY in `clob_best_ask` mode fills at the fresh CLOB best-ask (sizing + band-gate key
// off it); no usable ask falls back to `leader × (1 + fallback_haircut)`. The production
// copy-entry gate rejects SELLs before this path; every non-paper mode takes the boot-frozen
// haircut with no `/book` fetch.

const BA_HEX: &str = "0xbestask";
const BA_TOKEN: &str = "0xbestask-0"; // outcome 0's token (mid_cache_with_tokens emits "{hex}-{i}")

/// A `{price, size}` ask book.
fn book(asks: &[(Decimal, Decimal)]) -> OrderBook {
    OrderBook {
        asks: asks
            .iter()
            .map(|&(price, size)| BookLevel { price, size })
            .collect(),
        fetched_at_ms: 0,
    }
}

/// Mid-cache fixture that also emits `clobTokenIds` (outcome i → "{hex}-{i}") so the best-ask
/// path can resolve the outcome's `/book` token from the warm mid-cache snapshot.
fn mid_cache_with_tokens(hex: &str, price: &str) -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "http://gamma.test";
    let mut fx = HashMap::new();
    let url = format!("{BASE}/markets?condition_ids={hex}&limit=500");
    let body = format!(
        r#"[{{"conditionId":"{hex}","outcomePrices":"[\"{price}\",\"{price}\"]","clobTokenIds":"[\"{hex}-0\",\"{hex}-1\"]"}}]"#
    );
    fx.insert(url, body.into_bytes());
    MidPriceCache::with_fetcher(FixtureFetcher::new(fx), BASE.to_string())
}

/// The FIRST paper fill's `(contracts, price, fill_source)`, replayed from the log — the
/// event-log round-trip that also exercises AC7(i) (a `Some`-recorded fill reconstructs its
/// exact stored price + provenance on replay).
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

/// A trade (`side` at `leader_price`, 100 contracts) into `BA_HEX`/outcome 0.
fn bestask_trade(id: &str, side: Side, leader_price: Decimal) -> IncomingTrade {
    let ts = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    IncomingTrade {
        wallet: leader_wallet(),
        market_id: market(BA_HEX),
        outcome_id: OutcomeId(0),
        side,
        price: Price(leader_price),
        contracts: ContractQty(100),
        observed_at: ts,
        received_at: ts,
        source_trade_id: SourceTradeId(id.to_string()),
        provenance: TradeProvenance::RestPoll,
    }
}

/// Feed one trade through a paper-mode orchestrator in `fill_mode`, wired with `book_fetcher`
/// and a `max_fill_price` cap (`ZERO` disables). Returns the paper-fill count and the first fill
/// `(contracts, price, fill_source)`.
#[allow(clippy::too_many_arguments)]
async fn run_bestask<B: ClobBookFetcher + 'static>(
    dir: &TempDir,
    fill_mode: FillMode,
    side: Side,
    leader_price: Decimal,
    max_fill_price: Decimal,
    book_fetcher: Arc<B>,
) -> (usize, Option<(u64, Decimal, FillSource)>) {
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    let (tx, rx) = mpsc::channel::<IncomingTrade>(8);
    tx.send(bestask_trade("ba-1", side, leader_price))
        .await
        .unwrap();
    drop(tx);

    let orch = Orchestrator::new(
        rx,
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price,
            min_fill_price: Decimal::ZERO,
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            fill_mode,
            clob_best_ask_fallback_haircut_bps: 100,
            entry_gate_config: band_config(false),
            runtime_config: None,
            live_accounts: None,
        },
        HashMap::new(),
        WinnerFollowStrategy::new(flat_fill_config()),
        make_dispatcher(dir),
        paper_state.clone(),
        PositionLedger::new(),
        new_shared_health(false),
        MarketEndCache::new(String::new()),
        mid_cache_with_tokens(BA_HEX, "0.60"),
        dead_reseed_rx(),
        None,
        None,
        None,
        book_fetcher,
    )
    .unwrap();
    orch.run(std::future::pending::<()>()).await;

    (paper_fill_count(dir), first_paper_fill_full(dir))
}

/// AC1 (+AC7(i)): a paper BUY in `clob_best_ask` mode fills at the fresh best-ask, sized off it,
/// tagged `ClobBestAsk`, and the price/provenance survive an event-log replay. The book carries a
/// zero-size dust level at a lower price that the `size > 0` guard must skip.
/// PASS: recorded price == best-ask 0.55, contracts == floor(100/0.55) == 181, source ClobBestAsk.
#[tokio::test]
async fn best_ask_buy_fills_at_ask_and_sizes_off_it() {
    let dir = TempDir::new().unwrap();
    // Dust level 0.50 @ size 0 must NOT set the basis; the usable best-ask is 0.55.
    let books = HashMap::from([(
        BA_TOKEN.to_string(),
        book(&[(dec!(0.50), dec!(0)), (dec!(0.55), dec!(10))]),
    )]);
    let (fills, first) = run_bestask(
        &dir,
        FillMode::ClobBestAsk,
        Side::Buy,
        dec!(0.50),
        Decimal::ZERO,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .await;
    assert_eq!(fills, 1);
    let (contracts, price, source) = first.expect("a fill was recorded");
    assert_eq!(
        price,
        dec!(0.55),
        "recorded fill == best-ask (dust 0.50 skipped)"
    );
    assert_eq!(contracts, 181, "sized off the ask: floor(100/0.55)");
    assert_eq!(source, FillSource::ClobBestAsk);
    println!("PASS: AC1/AC7(i) — best-ask BUY fills at 0.55 (181 ct, ClobBestAsk), replay-stable");
}

/// AC2: a paper BUY with no usable ask (empty book, or a `/book` fetch error) falls back to
/// `leader × (1 + 1%)` and is tagged `Fallback`, keeping the position.
/// PASS: both the empty-book and fetch-error cases record 0.505 (0.50 × 1.01), source Fallback.
#[tokio::test]
async fn best_ask_fallback_on_no_usable_ask() {
    // (b) empty ask book → fallback.
    let dir_empty = TempDir::new().unwrap();
    let (fills, first) = run_bestask(
        &dir_empty,
        FillMode::ClobBestAsk,
        Side::Buy,
        dec!(0.50),
        Decimal::ZERO,
        Arc::new(FixtureClobBookFetcher::new(HashMap::from([(
            BA_TOKEN.to_string(),
            book(&[]),
        )]))),
    )
    .await;
    assert_eq!(fills, 1);
    let (_c, price, source) = first.expect("empty-book fill recorded");
    assert_eq!(price, dec!(0.505), "empty book → leader × 1.01 fallback");
    assert_eq!(source, FillSource::Fallback);

    // (c) /book fetch error (token absent from the fixture map) → fallback.
    let dir_err = TempDir::new().unwrap();
    let (fills, first) = run_bestask(
        &dir_err,
        FillMode::ClobBestAsk,
        Side::Buy,
        dec!(0.50),
        Decimal::ZERO,
        Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
    )
    .await;
    assert_eq!(fills, 1);
    let (_c, price, source) = first.expect("fetch-error fill recorded");
    assert_eq!(price, dec!(0.505), "fetch error → leader × 1.01 fallback");
    assert_eq!(source, FillSource::Fallback);
    println!("PASS: AC2 — no usable ask (empty book / fetch error) → 0.505 fallback (Fallback)");
}

/// AC3: the band gate keys on the ASK. A leader BUY at 0.80 (below the 0.85 cap) whose best-ask
/// ran to 0.86 (≥ cap) is rejected — the exact case a leader-price gate would let through.
/// PASS: 0 fills.
#[tokio::test]
async fn band_gate_rejects_on_ask_above_cap() {
    let dir = TempDir::new().unwrap();
    let books = HashMap::from([(BA_TOKEN.to_string(), book(&[(dec!(0.86), dec!(10))]))]);
    let (fills, _) = run_bestask(
        &dir,
        FillMode::ClobBestAsk,
        Side::Buy,
        dec!(0.80), // leader in-band (< 0.85)
        dec!(0.85),
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .await;
    assert_eq!(fills, 0);
    println!("PASS: AC3 — band gate rejects on the ask 0.86 ≥ 0.85 though leader 0.80 is in-band");
}

/// AC4: a SELL entry in `clob_best_ask` mode is rejected by the production copy-entry gate
/// before fill-price resolution. `PanicBookFetcher` proves the `/book` path is never reached.
/// PASS: no panic and zero paper fills.
#[tokio::test]
async fn sell_entry_is_rejected_before_best_ask_fetch() {
    let dir = TempDir::new().unwrap();
    let (fills, first) = run_bestask(
        &dir,
        FillMode::ClobBestAsk,
        Side::Sell,
        dec!(0.50),
        Decimal::ZERO,
        Arc::new(PanicBookFetcher),
    )
    .await;
    assert_eq!(fills, 0);
    assert!(first.is_none());
    println!("PASS: AC4 — SELL entry rejected before /book fetch, 0 fills");
}

/// AC5: `leader_haircut` mode ignores the `/book` entirely and records the boot-frozen 5% haircut
/// fill, tagged `LeaderHaircut` — byte-identical to the pre-#486 behaviour (rollback).
/// PASS: recorded price == 0.525 (0.50 × 1.05), contracts == 190, source LeaderHaircut.
#[tokio::test]
async fn leader_haircut_mode_records_haircut_fill() {
    let dir = TempDir::new().unwrap();
    // A book is present but must be ignored in leader_haircut mode.
    let books = HashMap::from([(BA_TOKEN.to_string(), book(&[(dec!(0.55), dec!(100))]))]);
    let (fills, first) = run_bestask(
        &dir,
        FillMode::LeaderHaircut,
        Side::Buy,
        dec!(0.50),
        Decimal::ZERO,
        Arc::new(FixtureClobBookFetcher::new(books)),
    )
    .await;
    assert_eq!(fills, 1);
    let (contracts, price, source) = first.expect("haircut fill recorded");
    assert_eq!(price, dec!(0.525), "leader_haircut → 0.50 × 1.05");
    assert_eq!(
        contracts, 190,
        "sized off the haircut basis: floor(100/0.525)"
    );
    assert_eq!(source, FillSource::LeaderHaircut);
    println!("PASS: AC5 — leader_haircut mode records the 0.525 haircut fill (LeaderHaircut)");
}

/// A `ClobBookFetcher` that panics if `fetch_book` is ever called. AC4 uses it to prove a SELL is
/// rejected before fill-price resolution; AC6 uses it to prove live mode performs no fill-path
/// `/book` fetch.
struct PanicBookFetcher;

impl ClobBookFetcher for PanicBookFetcher {
    async fn fetch_book(&self, _token_id: &str) -> Result<OrderBook, ClobBookError> {
        panic!("hard gate: the /book fetcher must not be called");
    }
}

/// A dispatcher whose LiveTiny path completes cleanly with a filled fixture order (AC6 asserts the
/// BOOK fetcher is untouched, not the order path).
fn make_live_dispatcher(dir: &TempDir) -> ExecutionDispatcher {
    make_dispatcher(dir)
}

/// AC6 (HARD GATE): an ordinary live mode never reaches the `/book` or a POST path. The dispatcher
/// rejects it, and no paper or live event is written.
#[tokio::test]
async fn live_mode_never_fetches_book_ac6() {
    let dir = TempDir::new().unwrap();
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper_state.db")).unwrap());
    paper_state.init_bankroll(Decimal::from(10_000u32)).unwrap();

    let (tx, rx) = mpsc::channel::<IncomingTrade>(8);
    tx.send(bestask_trade("ac6", Side::Buy, dec!(0.50)))
        .await
        .unwrap();
    drop(tx);

    let orch = Orchestrator::new(
        rx,
        LiveWatchlist::new(make_watchlist(leader_wallet())),
        OrchestratorConfig {
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            bankroll: Decimal::from(10_000u32),
            mode: ExecutionMode::LiveTiny,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            min_fill_price: Decimal::ZERO,
            paper_fill_haircut_bps: 500,
            paper_fill_slippage_bps: 100,
            // clob_best_ask is set deliberately: the mode gate — not the fill mode — must suppress
            // the fetch in a live mode.
            fill_mode: FillMode::ClobBestAsk,
            clob_best_ask_fallback_haircut_bps: 100,
            entry_gate_config: band_config(false),
            runtime_config: None,
            live_accounts: None,
        },
        HashMap::new(),
        WinnerFollowStrategy::new(flat_fill_config()),
        make_live_dispatcher(&dir),
        paper_state.clone(),
        PositionLedger::new(),
        new_shared_health(false),
        MarketEndCache::new(String::new()),
        mid_cache_with_tokens(BA_HEX, "0.60"),
        dead_reseed_rx(),
        None,
        None,
        None,
        Arc::new(PanicBookFetcher),
    )
    .unwrap();
    orch.run(std::future::pending::<()>()).await;

    assert_eq!(
        paper_fill_count(&dir),
        0,
        "retired live mode writes no paper fill"
    );
    assert!(!dir.path().join("live.log").exists());
    println!(
        "PASS: AC6 — live mode performs no /book fetch (panic fetcher untouched), 0 paper fills"
    );
}
