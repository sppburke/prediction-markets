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
    positions_in_window: u64,
    resolution_lags_secs: Vec<i64>,
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

/// Run the injected backtest over a single spanning position — wallet S buys `mkt-s` at D1, never
/// resolves; CLOB covered only from D20 onward (NO sample at-or-before D10) — for one forward-MTM
/// window, returning S's emitted horizon-MTM `unrealized_pnl` flow.
fn run_span_window(as_of: i64, horizon: i64) -> f64 {
    let dir = tempfile::TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let out_dir = dir.path().join("out");
    let s = wallet(0x55);
    {
        let mut cache = WalletCache::open(&cache_path).unwrap();
        cache
            .insert_new(
                &s.to_string(),
                vec![buy(s, "mkt-s", "s-1", dec!(0.50), T0 + SEC_PER_DAY)],
            )
            .unwrap();
        cache
            .upsert_token_conditions_batch(&[("tok-s".to_owned(), "mkt-s".to_owned(), 0)], T0)
            .unwrap();
        // No sample ≤ D10 (uncovered at as_of_1); 0.60 at D20; 0.70 at D30.
        cache
            .insert_price_history_batch(
                &[
                    (
                        "mkt-s".to_owned(),
                        "tok-s".to_owned(),
                        T0 + 20 * SEC_PER_DAY,
                        "0.60".to_owned(),
                    ),
                    (
                        "mkt-s".to_owned(),
                        "tok-s".to_owned(),
                        T0 + 30 * SEC_PER_DAY,
                        "0.70".to_owned(),
                    ),
                ],
                "clob",
            )
            .unwrap();
    }
    let cache = WalletCache::open(&cache_path).unwrap();
    let injected = vec![s];
    let mut all_trades = load_injected_trades(&cache, &injected);
    all_trades.sort_by_key(|t| t.timestamp.0);
    let resolutions = pe_bootstrap::gamma::load_resolutions(&cache).unwrap();
    let schedules = pe_bootstrap::gamma::load_schedules(&cache).unwrap();
    let liq = pe_bootstrap::gamma::load_liquidity(&cache).unwrap();
    let snapshots = cache.load_all_snapshots().unwrap();
    let markets: HashSet<MarketId> = all_trades.iter().map(|t| t.market_id.clone()).collect();
    let marks = cache.load_clob_marks(&markets, horizon).unwrap();
    let config = BacktestConfig {
        bootstrap_cache_path: cache_path.clone(),
        output_dir: out_dir.clone(),
        flat_usd: Some(dec!(25)),
        max_signal_price: None,
        mtm_window_start_unix: Some(as_of),
        mtm_window_end_unix: Some(horizon),
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
    rows.iter()
        .filter(|r| r.wallet == s.to_string() && r.is_horizon_mtm)
        .map(|r| r.unrealized_pnl)
        .sum()
}

#[test]
fn scenario_mtm_partial_coverage_telescopes_no_double_count() {
    // A spanning position covered only from D20 (uncovered at as_of_1=D10), across two ADJACENT
    // windows (as_of_2 = horizon_1 = D20). Telescoping requires the per-window flows to sum to the
    // single true gain (mark_H2 − cost)·50 = (0.70 − 0.50)·50 = 10:
    //   - w1 (D10→D20): as_of uncovered → 0; horizon 0.60 → first credit (0.60−0.50)·50 = +5.
    //   - w2 (D20→D30): as_of 0.60 (covered, == w1's horizon) → −5; horizon 0.70 → +10; flow = +5.
    // The literal `mark − cost` stock would give w2 = +10 (no as_of subtraction) → 5 + 10 = 15, a
    // double-count. PASS = both flows are +5 (so they sum to the true +10), NOT w2 = +10.
    let d10 = T0 + 10 * SEC_PER_DAY;
    let d20 = T0 + 20 * SEC_PER_DAY;
    let d30 = T0 + 30 * SEC_PER_DAY;
    let flow_w1 = run_span_window(d10, d20);
    let flow_w2 = run_span_window(d20, d30);
    let pass = flow_w1 == 5.0 && flow_w2 == 5.0;
    println!(
        "PASS={pass} mtm.partial_coverage_telescopes: flow_w1={flow_w1} (want 5.0) + flow_w2={flow_w2} (want 5.0, NOT stock 10.0) = true gain 10.0"
    );
    assert!(
        pass,
        "adjacent-window flows must telescope to (mark_H − cost) with no double-count under partial coverage"
    );
}

/// Build a fixture for the F3a resolution-lag + open-fraction diagnostics (issue
/// #436 Phase F). Window `as_of = D10`, `horizon = D30`. No CLOB marks — the rows
/// survive via `open_at_horizon > 0`; coverage is not under test here, the
/// resolution lag and the open-fraction denominator are.
///   - P (0x77): buys `mkt-late` at **D25**; it resolves at **D40** (after the
///     horizon) → open at the horizon, resolution lag = D40 − as_of(D10) = 30 days.
///     P's D25 trade also extends the sim's date axis past D20 so R's quick-resolver
///     is swept.
///   - Q (0x88): buys `mkt-never` at D15; never resolves → open at horizon,
///     **censored** lag = −1.
///   - R (0x99): buys `mkt-r-open` at D12 (never resolves → open at horizon) AND
///     `mkt-r-closed` at D12 (resolves at **D20**, inside the window → realized,
///     closed in `(D10, D30]`) → open_at_horizon = 1 but positions_in_window = 2.
fn run_lag_fixture() -> (Vec<PnlRow>, String, String, String) {
    let dir = tempfile::TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let out_dir = dir.path().join("out");

    let p = wallet(0x77);
    let q = wallet(0x88);
    let r = wallet(0x99);

    {
        let mut cache = WalletCache::open(&cache_path).unwrap();
        cache
            .insert_new(
                &p.to_string(),
                vec![buy(p, "mkt-late", "p-1", dec!(0.50), T0 + 25 * SEC_PER_DAY)],
            )
            .unwrap();
        cache
            .insert_new(
                &q.to_string(),
                vec![buy(
                    q,
                    "mkt-never",
                    "q-1",
                    dec!(0.50),
                    T0 + 15 * SEC_PER_DAY,
                )],
            )
            .unwrap();
        cache
            .insert_new(
                &r.to_string(),
                vec![
                    buy(r, "mkt-r-open", "r-1", dec!(0.50), T0 + 12 * SEC_PER_DAY),
                    buy(r, "mkt-r-closed", "r-2", dec!(0.50), T0 + 12 * SEC_PER_DAY),
                ],
            )
            .unwrap();

        // mkt-late resolves AFTER the horizon (D40) → P stays open at D30, lag = 30d.
        let late = T0 + 40 * SEC_PER_DAY;
        cache
            .insert_resolution("mkt-late", Some(0), late, late)
            .unwrap();
        // mkt-r-closed resolves INSIDE the window (D20) → realized, closed-in-window.
        let early = T0 + 20 * SEC_PER_DAY;
        cache
            .insert_resolution("mkt-r-closed", Some(0), early, early)
            .unwrap();
        // mkt-never and mkt-r-open have NO resolution rows → censored / open at horizon.
    }

    let cache = WalletCache::open(&cache_path).unwrap();
    let injected = vec![p, q, r];
    let mut all_trades = load_injected_trades(&cache, &injected);
    all_trades.sort_by_key(|t| t.timestamp.0);

    let resolutions = pe_bootstrap::gamma::load_resolutions(&cache).unwrap();
    let schedules = pe_bootstrap::gamma::load_schedules(&cache).unwrap();
    let liq = pe_bootstrap::gamma::load_liquidity(&cache).unwrap();
    let snapshots = cache.load_all_snapshots().unwrap();
    let markets: HashSet<MarketId> = all_trades.iter().map(|t| t.market_id.clone()).collect();
    let marks = cache.load_clob_marks(&markets, HORIZON).unwrap();

    let config = BacktestConfig {
        bootstrap_cache_path: cache_path.clone(),
        output_dir: out_dir.clone(),
        flat_usd: Some(dec!(25)),
        max_signal_price: None,
        mtm_window_start_unix: Some(AS_OF),
        mtm_window_end_unix: Some(HORIZON),
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
    (rows, p.to_string(), q.to_string(), r.to_string())
}

#[test]
fn scenario_mtm_resolution_lag_after_horizon() {
    // PASS: an open-at-horizon position whose market resolves AFTER the horizon reports the resolution
    // lag `resolved_at − as_of` = D40 − D10 = 30 days. FAIL otherwise.
    let (rows, p, _q, _r) = run_lag_fixture();
    let row = mtm_row(&rows, &p).expect("wallet P must have a horizon MTM row");
    let want = vec![30 * SEC_PER_DAY];
    let pass = row.resolution_lags_secs == want && row.open_at_horizon == 1;
    println!(
        "PASS={pass} mtm.resolution_lag_after_horizon: P lags={:?} (want {want:?}), open={}",
        row.resolution_lags_secs, row.open_at_horizon
    );
    assert!(
        pass,
        "open-at-horizon position must report lag = resolved_at − as_of"
    );
}

#[test]
fn scenario_mtm_resolution_lag_censored_when_unresolved() {
    // PASS: an open-at-horizon position whose market never resolves is censored (lag = −1). FAIL otherwise.
    let (rows, _p, q, _r) = run_lag_fixture();
    let row = mtm_row(&rows, &q).expect("wallet Q must have a horizon MTM row");
    let pass = row.resolution_lags_secs == vec![-1] && row.open_at_horizon == 1;
    println!(
        "PASS={pass} mtm.resolution_lag_censored: Q lags={:?} (want [-1]), open={}",
        row.resolution_lags_secs, row.open_at_horizon
    );
    assert!(
        pass,
        "an unresolved open position must be censored with lag = −1"
    );
}

#[test]
fn scenario_mtm_open_fraction_denominator_counts_closed_in_window() {
    // PASS: the open-fraction denominator counts a wallet's closed-in-window position too — R holds one
    // position open at the horizon and one that resolved inside the window, so open_at_horizon=1 but
    // positions_in_window=2 (fraction 1/2). FAIL otherwise.
    let (rows, _p, _q, r) = run_lag_fixture();
    let row = mtm_row(&rows, &r).expect("wallet R must have a horizon MTM row");
    let pass = row.open_at_horizon == 1 && row.positions_in_window == 2;
    println!(
        "PASS={pass} mtm.open_fraction_denominator: R open={} (want 1), positions_in_window={} (want 2)",
        row.open_at_horizon, row.positions_in_window
    );
    assert!(
        pass,
        "positions_in_window must include closed-in-window positions for a horizon-exposed wallet"
    );
}

/// #445 defect 3 fixture: a single QUIET wallet whose only trade is a BUY that resolves INSIDE the
/// window with NO later trade to advance the simulation date axis past the resolution. Window
/// `as_of = D10`, `horizon = D30`.
///   - Wallet Z buys `mkt-quiet` at **D12**; it resolves (Z wins, outcome 0) at **D20** (inside the
///     window). Z has no other trade, so the per-day resolution sweep — whose last date is D12 —
///     never reaches D20. A STALE pre-resolution CLOB mark (0.60 at D18) exists, so WITHOUT the
///     end-of-run sweep Z would be marked at (0.60−0.50)·50 = +5 (wrong) instead of realized at
///     (1.0−0.50)·50 = +25.
fn run_quiet_resolution_fixture() -> (Vec<PnlRow>, String) {
    let dir = tempfile::TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let out_dir = dir.path().join("out");
    let z = wallet(0x2c);
    {
        let mut cache = WalletCache::open(&cache_path).unwrap();
        cache
            .insert_new(
                &z.to_string(),
                vec![buy(
                    z,
                    "mkt-quiet",
                    "z-1",
                    dec!(0.50),
                    T0 + 12 * SEC_PER_DAY,
                )],
            )
            .unwrap();
        // Resolves INSIDE the window at D20 (Z wins outcome 0). No later trade exists, so the per-day
        // sweep (its last date is D12) never reaches it — only the end-of-run sweep can close it.
        let r = T0 + 20 * SEC_PER_DAY;
        cache.insert_resolution("mkt-quiet", Some(0), r, r).unwrap();
        // A STALE pre-resolution CLOB mark (0.60 at D18): without the fix Z is mismarked at +5.
        cache
            .upsert_token_conditions_batch(
                &[("tok-quiet".to_owned(), "mkt-quiet".to_owned(), 0)],
                T0,
            )
            .unwrap();
        cache
            .insert_price_history_batch(
                &[(
                    "mkt-quiet".to_owned(),
                    "tok-quiet".to_owned(),
                    T0 + 18 * SEC_PER_DAY,
                    "0.60".to_owned(),
                )],
                "clob",
            )
            .unwrap();
    }

    let cache = WalletCache::open(&cache_path).unwrap();
    let injected = vec![z];
    let mut all_trades = load_injected_trades(&cache, &injected);
    all_trades.sort_by_key(|t| t.timestamp.0);

    let resolutions = pe_bootstrap::gamma::load_resolutions(&cache).unwrap();
    let schedules = pe_bootstrap::gamma::load_schedules(&cache).unwrap();
    let liq = pe_bootstrap::gamma::load_liquidity(&cache).unwrap();
    let snapshots = cache.load_all_snapshots().unwrap();
    let markets: HashSet<MarketId> = all_trades.iter().map(|t| t.market_id.clone()).collect();
    let marks = cache.load_clob_marks(&markets, HORIZON).unwrap();

    let config = BacktestConfig {
        bootstrap_cache_path: cache_path.clone(),
        output_dir: out_dir.clone(),
        flat_usd: Some(dec!(25)),
        max_signal_price: None,
        mtm_window_start_unix: Some(AS_OF),
        mtm_window_end_unix: Some(HORIZON),
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
    (rows, z.to_string())
}

#[test]
fn scenario_mtm_quiet_wallet_in_window_resolution_is_realized_not_marked() {
    // PASS: a copied position that resolves INSIDE the window with no later trade to advance the date
    // axis is realized on a day row INSIDE `(as_of, horizon]` (+25) and is NOT marked at the stale
    // CLOB mid (no horizon MTM row). Without the #445 end-of-run sweep it would be mismarked at +5
    // (the stale 0.60 mark) and emit no realized row. FAIL otherwise.
    let (rows, z) = run_quiet_resolution_fixture();
    let realized: Vec<&PnlRow> = rows
        .iter()
        .filter(|r| r.wallet == z && !r.is_horizon_mtm)
        .collect();
    let realized_sum: f64 = realized.iter().map(|r| r.realized_pnl).sum();
    let all_in_window = realized
        .iter()
        .all(|r| r.period_end > AS_OF && r.period_end <= HORIZON);
    let has_mtm = mtm_row(&rows, &z).is_some();
    let pass = realized_sum == 25.0 && all_in_window && !has_mtm && !realized.is_empty();
    println!(
        "PASS={pass} mtm.quiet_wallet_resolution: Z realized_sum={realized_sum} (want 25.0), \
         realized_in_window={all_in_window} (want true), has_mtm={has_mtm} (want false), \
         realized_rows={}",
        realized.len()
    );
    assert!(
        pass,
        "a quiet-wallet in-window resolution must be realized inside the window, never marked"
    );
}
