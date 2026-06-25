#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario (#436 Phase E): the injected-set `pe-backtest` values still-open
//! positions at a configured forward-MTM window `(as_of, horizon]` using the CLOB
//! `market_price_history` series, emitting the per-wallet `is_horizon_mtm` row.
//!
//! The mark is a FLOW: `Σ_{open at horizon}(mark_H − cost) − Σ_{open at as_of}(mark_as_of − cost)`,
//! so an adjacent window's mark cancels and the trajectory telescopes with no
//! double-count (the literal `mark − cost` stock would double-count spanning
//! positions). Coverage is bounded: a position with no CLOB series at the horizon
//! contributes 0 and is reported uncovered.
//!
//! Fixture (deterministic — fixed timestamps/prices, no clock, no RNG). All copies
//! size to `floor(flat_usd / fill_price) = floor(25 / 0.50) = 50` contracts at a
//! `0.50` cost basis (slippage 0). Window: `as_of = D10`, `horizon = D30`.
//!   - Wallet A buys `mkt-cover` at **D25** (after as_of), never resolves → open at
//!     horizon only. CLOB mark 0.70 at D28. Flow = (0.70−0.50)·50 = **+10**.
//!   - Wallet B buys `mkt-span` at **D5** (before as_of), never resolves → open at
//!     BOTH boundaries. CLOB 0.55 at D8 (≤ as_of), 0.65 at D28 (≤ horizon). Flow =
//!     (0.65−0.50)·50 − (0.55−0.50)·50 = 7.5 − 2.5 = **+5** (the as_of leg is the
//!     flow's defining subtraction).
//!   - Wallet D buys `mkt-nocover` at D15, never resolves → open at horizon, NO CLOB
//!     series → flow **0**, open_at_horizon 1, marked_at_horizon 0 (coverage-bounded).
//!   - Wallet E buys `mkt-resolved` at D15; it resolves (E wins) at **D20** (< horizon)
//!     → realized on a day row, NOT marked (closed before the horizon). A's D25 trade
//!     extends the date axis so the sweep closes it.
//!
//! Four independent PASS criteria, one per test (no compound assertions).

use std::collections::HashSet;

use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::{load_injected_trades, run_simulation_with};
use pe_bootstrap::cache::WalletCache;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_strategy_winner_follow::{WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::RankerConfig;
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use time::OffsetDateTime;

const T0: i64 = 1_700_000_000; // a UTC instant; "Dn" below means T0 + n*SEC_PER_DAY
const SEC_PER_DAY: i64 = 86_400;
const AS_OF: i64 = T0 + 10 * SEC_PER_DAY;
const HORIZON: i64 = T0 + 30 * SEC_PER_DAY;

#[derive(Deserialize)]
struct PnlRow {
    wallet: String,
    period_end: i64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    #[allow(dead_code)]
    n_fills: u64,
    #[allow(dead_code)]
    notional: f64,
    is_horizon_mtm: bool,
    open_at_horizon: u64,
    marked_at_horizon: u64,
}

fn wallet(b: u8) -> WalletAddress {
    WalletAddress::from_hex(&format!("0x{b:040x}")).unwrap()
}

fn buy(w: WalletAddress, mkt: &str, id: &str, price: Decimal, ts: i64) -> RawTrade {
    RawTrade {
        wallet: w,
        market_id: MarketId(VenueMarketId(mkt.to_owned())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(price),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        source_trade_id: SourceTradeId(id.to_owned()),
    }
}

/// Build the fixture cache (trades + resolutions + CLOB token map + price series),
/// run the injected-set backtest with the forward-MTM window, and return the parsed
/// `pnl_by_period.ndjson` rows plus the four wallet hexes (A, B, D, E).
fn run_fixture() -> (Vec<PnlRow>, String, String, String, String) {
    let dir = tempfile::TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let out_dir = dir.path().join("out");

    let a = wallet(0xa1); // covered, opened-in-window
    let b = wallet(0xb2); // spanning (open at both boundaries)
    let d = wallet(0xd4); // open-at-horizon, uncovered
    let e = wallet(0xe5); // resolves in-window

    {
        let mut cache = WalletCache::open(&cache_path).unwrap();
        cache
            .insert_new(
                &a.to_string(),
                vec![buy(
                    a,
                    "mkt-cover",
                    "a-1",
                    dec!(0.50),
                    T0 + 25 * SEC_PER_DAY,
                )],
            )
            .unwrap();
        cache
            .insert_new(
                &b.to_string(),
                vec![buy(b, "mkt-span", "b-1", dec!(0.50), T0 + 5 * SEC_PER_DAY)],
            )
            .unwrap();
        cache
            .insert_new(
                &d.to_string(),
                vec![buy(
                    d,
                    "mkt-nocover",
                    "d-1",
                    dec!(0.50),
                    T0 + 15 * SEC_PER_DAY,
                )],
            )
            .unwrap();
        cache
            .insert_new(
                &e.to_string(),
                vec![buy(
                    e,
                    "mkt-resolved",
                    "e-1",
                    dec!(0.50),
                    T0 + 15 * SEC_PER_DAY,
                )],
            )
            .unwrap();

        // mkt-resolved → outcome 0 wins (E's side) at D20; the D25 sweep closes it.
        // No resolution rows for the other markets → they stay open through the horizon.
        let r = T0 + 20 * SEC_PER_DAY;
        cache
            .insert_resolution("mkt-resolved", Some(0), r, r)
            .unwrap();

        // CLOB token map (outcome 0) + price series for the two covered markets.
        cache
            .upsert_token_conditions_batch(
                &[
                    ("tok-cover".to_owned(), "mkt-cover".to_owned(), 0),
                    ("tok-span".to_owned(), "mkt-span".to_owned(), 0),
                ],
                T0,
            )
            .unwrap();
        cache
            .insert_price_history_batch(
                &[
                    // mkt-cover: one sample at D28 (≤ horizon) → mark 0.70.
                    (
                        "mkt-cover".to_owned(),
                        "tok-cover".to_owned(),
                        T0 + 28 * SEC_PER_DAY,
                        "0.70".to_owned(),
                    ),
                    // mkt-span: 0.55 at D8 (≤ as_of) and 0.65 at D28 (≤ horizon).
                    (
                        "mkt-span".to_owned(),
                        "tok-span".to_owned(),
                        T0 + 8 * SEC_PER_DAY,
                        "0.55".to_owned(),
                    ),
                    (
                        "mkt-span".to_owned(),
                        "tok-span".to_owned(),
                        T0 + 28 * SEC_PER_DAY,
                        "0.65".to_owned(),
                    ),
                ],
                "clob",
            )
            .unwrap();
        // mkt-nocover intentionally has NO token map / no price series (uncovered).
    }

    let cache = WalletCache::open(&cache_path).unwrap();
    let injected = vec![a, b, d, e];
    let mut all_trades = load_injected_trades(&cache, &injected);
    all_trades.sort_by_key(|t| t.timestamp.0); // run_simulation precondition

    let resolutions = pe_bootstrap::gamma::load_resolutions(&cache).unwrap();
    let schedules = pe_bootstrap::gamma::load_schedules(&cache).unwrap();
    let liq = pe_bootstrap::gamma::load_liquidity(&cache).unwrap();
    let snapshots = cache.load_all_snapshots().unwrap();

    // The forward-MTM index, bounded to the injected markets (mirrors main.rs).
    let markets: HashSet<MarketId> = all_trades.iter().map(|t| t.market_id.clone()).collect();
    let marks = cache.load_clob_marks(&markets, HORIZON).unwrap();

    let config = BacktestConfig {
        bootstrap_cache_path: cache_path.clone(),
        output_dir: out_dir.clone(),
        flat_usd: Some(dec!(25)),
        max_signal_price: None, // prices are below any cap; keep it explicit
        mtm_window_start_unix: Some(AS_OF),
        mtm_window_end_unix: Some(HORIZON),
        // slippage 0 so fill_price == signal price (0.50) and the contract math is exact.
        strategy: WinnerFollowConfig {
            slippage_rate: dec!(0),
            ..WinnerFollowConfig::default()
        },
        ..BacktestConfig::default()
    };
    let ranker_config = RankerConfig::default();
    let strategy = WinnerFollowStrategy::new(config.strategy.clone());
    let injected_set: HashSet<WalletAddress> = injected.iter().copied().collect();

    run_simulation_with(
        &config,
        &all_trades,
        &snapshots,
        &resolutions,
        &schedules,
        &liq,
        &ranker_config,
        &strategy,
        true,
        Some(&injected_set),
        Some(&marks),
    )
    .unwrap();

    let text = std::fs::read_to_string(out_dir.join("pnl_by_period.ndjson")).unwrap();
    let rows: Vec<PnlRow> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    (
        rows,
        a.to_string(),
        b.to_string(),
        d.to_string(),
        e.to_string(),
    )
}

/// The single forward-MTM horizon row for `hex`, if any.
fn mtm_row<'a>(rows: &'a [PnlRow], hex: &str) -> Option<&'a PnlRow> {
    rows.iter().find(|r| r.wallet == hex && r.is_horizon_mtm)
}

// Marks are exact f64 (10.0 / 5.0 / 0.0 / 25.0 from exact Decimal arithmetic), so exact `==` is
// correct here — and `clippy::float_cmp` exempts comparisons against a constant literal.

#[test]
fn scenario_mtm_marks_opened_in_window_covered_position() {
    // PASS: a position opened inside the window and open at the horizon is marked at its CLOB mid,
    // unrealized flow = (mark_H − cost) × contracts = (0.70 − 0.50) × 50 = +10. FAIL otherwise.
    let (rows, a, _b, _d, _e) = run_fixture();
    let row = mtm_row(&rows, &a).expect("wallet A must have a horizon MTM row");
    let pass = row.unrealized_pnl == 10.0
        && row.is_horizon_mtm
        && row.period_end == HORIZON
        && row.open_at_horizon == 1
        && row.marked_at_horizon == 1
        && row.realized_pnl == 0.0;
    println!(
        "PASS={pass} mtm.marks_opened_in_window: A unrealized={} (want 10.0), period_end={} (want {HORIZON}), open={}, marked={}",
        row.unrealized_pnl, row.period_end, row.open_at_horizon, row.marked_at_horizon
    );
    assert!(
        pass,
        "covered opened-in-window position must mark to +10 at the horizon"
    );
}

#[test]
fn scenario_mtm_flow_subtracts_as_of_leg_for_spanning_position() {
    // PASS: a position open at BOTH boundaries contributes the FLOW (mark_H − mark_as_of)·contracts,
    // i.e. (0.65 − 0.55) × 50 = +5 — NOT the stock (0.65 − 0.50) × 50 = 7.5. The as_of subtraction is
    // what prevents the cross-window double-count. FAIL otherwise.
    let (rows, _a, b, _d, _e) = run_fixture();
    let row = mtm_row(&rows, &b).expect("wallet B must have a horizon MTM row");
    let pass = row.unrealized_pnl == 5.0 && row.open_at_horizon == 1 && row.marked_at_horizon == 1;
    println!(
        "PASS={pass} mtm.flow_subtracts_as_of: B unrealized={} (flow want 5.0, NOT stock 7.5), open={}, marked={}",
        row.unrealized_pnl, row.open_at_horizon, row.marked_at_horizon
    );
    assert!(
        pass,
        "spanning position must use the FLOW (mark_H − mark_as_of), giving +5 not +7.5"
    );
}

#[test]
fn scenario_mtm_uncovered_open_position_is_zero_but_counted() {
    // PASS: a position open at the horizon with no CLOB series contributes 0 to the mark but is
    // counted as open_at_horizon=1 / marked_at_horizon=0 (coverage-bounded, E3). FAIL otherwise.
    let (rows, _a, _b, d, _e) = run_fixture();
    let row = mtm_row(&rows, &d).expect("wallet D must have a horizon MTM row (open, uncovered)");
    let pass = row.unrealized_pnl == 0.0 && row.open_at_horizon == 1 && row.marked_at_horizon == 0;
    println!(
        "PASS={pass} mtm.uncovered_zero_but_counted: D unrealized={} (want 0.0), open={} (want 1), marked={} (want 0)",
        row.unrealized_pnl, row.open_at_horizon, row.marked_at_horizon
    );
    assert!(
        pass,
        "uncovered open position must contribute 0 yet count against coverage"
    );
}

#[test]
fn scenario_mtm_resolved_position_is_realized_not_marked() {
    // PASS: a position that resolves before the horizon is realized on a day row (is_horizon_mtm=false)
    // and is NOT marked (no horizon MTM row for that wallet) — realized and MTM stay separate. FAIL otherwise.
    let (rows, _a, _b, _d, e) = run_fixture();
    let realized_day: f64 = rows
        .iter()
        .filter(|r| r.wallet == e && !r.is_horizon_mtm)
        .map(|r| r.realized_pnl)
        .sum();
    let has_mtm = mtm_row(&rows, &e).is_some();
    // E wins outcome 0: realized = (1.0 − 0.50) × 50 = +25.
    let pass = realized_day == 25.0 && !has_mtm;
    println!(
        "PASS={pass} mtm.resolved_realized_not_marked: E realized_day={realized_day} (want 25.0), has_mtm_row={has_mtm} (want false)"
    );
    assert!(
        pass,
        "a position resolved before the horizon must be realized-only, never marked"
    );
}
