#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario (#453): the injected-set `pe-backtest` loads resolutions/schedules for
//! ONLY the markets the injected wallets traded (per-market PK lookups), instead of
//! scanning the whole `market_resolutions` / `market_schedules` tables. This proves
//! the optimization is **bit-identical**: a fixture seeded with extra unrelated
//! resolution/schedule rows produces the exact same `pnl_by_period.ndjson` whether
//! the simulation is fed the FULL index (`load_all_*`) or the BOUNDED index
//! (`load_*_for_markets`) — the extra markets never leak into any result.
//!
//! The fixture is built to exercise every resolution/schedule reader in
//! `run_simulation_with`, so the comparison is a strong net for the correctness
//! claim (all readers are point-lookups keyed on a traded market):
//!   - (a) per-day resolution sweep — `mkt-a-resolved` (W1) resolves at D8 and is
//!     swept while the date axis is live (last trade D25).
//!   - (b) `max_hours_to_expiry` TTR filter — set, so every copied trade reads the
//!     schedule index (a hit for `mkt-a-resolved`) then the resolution fallback.
//!   - (c) in-window realization (#445 defect 3) — `mkt-inwindow` (W2) resolves at
//!     D28, AFTER the last trade (D25), so the per-day sweep never reaches it; the
//!     forward-MTM pass realizes it inside `(D10, D30]`.
//!   - (d) open-at-horizon resolution-lag read — `mkt-late` (W1) resolves at D40
//!     (after the horizon), is open at D30, and its lag is read from the resolution
//!     index.
//!
//! Deterministic: fixed timestamps/prices, no clock, no RNG. `pnl_by_period.ndjson`
//! is written in a fixed sort order (wallet, period_end, kind), so a byte compare is
//! a valid bit-identity check.

use std::collections::HashSet;
use std::path::Path;

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
use rust_decimal_macros::dec;
use time::OffsetDateTime;

const T0: i64 = 1_700_000_000; // a UTC instant; "Dn" means T0 + n*SEC_PER_DAY
const SEC_PER_DAY: i64 = 86_400;
const AS_OF: i64 = T0 + 10 * SEC_PER_DAY;
const HORIZON: i64 = T0 + 30 * SEC_PER_DAY;

fn day(n: i64) -> i64 {
    T0 + n * SEC_PER_DAY
}

fn wallet(b: u8) -> WalletAddress {
    WalletAddress::from_hex(&format!("0x{b:040x}")).unwrap()
}

fn mkt(id: &str) -> MarketId {
    MarketId(VenueMarketId(id.to_owned()))
}

fn buy(w: WalletAddress, market: &str, id: &str, ts: i64) -> RawTrade {
    RawTrade {
        wallet: w,
        market_id: mkt(market),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price(dec!(0.50)),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(ts).unwrap()),
        source_trade_id: SourceTradeId(id.to_owned()),
    }
}

/// Seed a fixture cache: two injected wallets trading three markets (one resolving
/// in-axis, one resolving in-window after the last trade, one open past the horizon),
/// plus extra UNRELATED resolution/schedule rows that the bounded load must drop.
/// Returns the injected wallet list.
fn seed_cache(path: &Path) -> Vec<WalletAddress> {
    let w1 = wallet(0xa1);
    let w2 = wallet(0xb2);
    let mut cache = WalletCache::open(path).unwrap();

    // Injected trades. W1's D25 `mkt-late` trade is the last on the axis.
    cache
        .insert_new(
            &w1.to_string(),
            vec![
                buy(w1, "mkt-a-resolved", "w1-1", day(5)),
                buy(w1, "mkt-late", "w1-2", day(25)),
            ],
        )
        .unwrap();
    cache
        .insert_new(
            &w2.to_string(),
            vec![buy(w2, "mkt-inwindow", "w2-1", day(12))],
        )
        .unwrap();

    // Resolutions for the traded markets — one per reader.
    cache
        .insert_resolution("mkt-a-resolved", Some(0), day(8), day(8))
        .unwrap(); // reader (a): swept in-axis
    cache
        .insert_resolution("mkt-inwindow", Some(0), day(28), day(28))
        .unwrap(); // reader (c): resolves after the last trade (D25), inside the window
    cache
        .insert_resolution("mkt-late", Some(0), day(40), day(40))
        .unwrap(); // reader (d): resolves after the horizon → open at horizon

    // A schedule for one traded market → reader (b) hits the schedule index (the
    // others fall through to the resolution index).
    cache
        .insert_schedule("mkt-a-resolved", Some(day(9)), day(0))
        .unwrap();

    // CLOB marks (token map + price series) for the open positions.
    cache
        .upsert_token_conditions_batch(
            &[
                ("tok-late".to_owned(), "mkt-late".to_owned(), 0),
                ("tok-inwindow".to_owned(), "mkt-inwindow".to_owned(), 0),
            ],
            T0,
        )
        .unwrap();
    cache
        .insert_price_history_batch(
            &[
                // mkt-late: covered at the horizon → marked.
                (
                    "mkt-late".to_owned(),
                    "tok-late".to_owned(),
                    day(28),
                    "0.70".to_owned(),
                ),
                // mkt-inwindow: a STALE pre-resolution mark — reader (c) must realize
                // at the 0/1 outcome, never mark at this 0.60.
                (
                    "mkt-inwindow".to_owned(),
                    "tok-inwindow".to_owned(),
                    day(18),
                    "0.60".to_owned(),
                ),
            ],
            "clob",
        )
        .unwrap();

    // EXTRA unrelated markets — present in the full tables, never traded by an
    // injected wallet, so the bounded load must exclude them. `mkt-extra-void` is
    // voided (NULL winner) and is excluded even from the full resolutions load.
    cache
        .insert_resolution("mkt-extra-resolved", Some(0), day(8), day(8))
        .unwrap();
    cache
        .insert_resolution("mkt-extra-void", None, day(12), day(12))
        .unwrap();
    cache
        .insert_schedule("mkt-extra-resolved", Some(day(9)), day(0))
        .unwrap();
    cache
        .insert_schedule("mkt-extra-void", None, day(0))
        .unwrap();
    cache
        .insert_schedule("mkt-extra-sched", Some(day(100)), day(0))
        .unwrap();

    vec![w1, w2]
}

/// Run the injected-set forward-MTM backtest with either the FULL or the BOUNDED
/// resolutions/schedules index, returning the emitted `pnl_by_period.ndjson` text.
/// Everything else (trades, snapshots, liquidity, CLOB marks, config) is identical.
fn run(cache_path: &Path, out_dir: &Path, injected: &[WalletAddress], bounded: bool) -> String {
    let cache = WalletCache::open(cache_path).unwrap();
    let mut all_trades = load_injected_trades(&cache, injected);
    all_trades.sort_by_key(|t| t.timestamp.0); // run_simulation precondition
    let markets: HashSet<MarketId> = all_trades.iter().map(|t| t.market_id.clone()).collect();

    let (resolutions, schedules) = if bounded {
        (
            pe_bootstrap::gamma::load_resolutions_for_markets(&cache, &markets).unwrap(),
            pe_bootstrap::gamma::load_schedules_for_markets(&cache, &markets).unwrap(),
        )
    } else {
        (
            pe_bootstrap::gamma::load_resolutions(&cache).unwrap(),
            pe_bootstrap::gamma::load_schedules(&cache).unwrap(),
        )
    };
    let liq = pe_bootstrap::gamma::load_liquidity(&cache).unwrap();
    let snapshots = cache.load_all_snapshots().unwrap();
    // CLOB marks are already bounded to the injected markets in both arms.
    let marks = cache.load_clob_marks(&markets, HORIZON).unwrap();

    let config = BacktestConfig {
        bootstrap_cache_path: cache_path.to_path_buf(),
        output_dir: out_dir.to_path_buf(),
        flat_usd: Some(dec!(25)),
        max_signal_price: None,
        // Exercises reader (b) on every copied trade without suppressing any (every
        // market closes well within 60 days of its trade date).
        max_hours_to_expiry: Some(1440),
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

    std::fs::read_to_string(out_dir.join("pnl_by_period.ndjson")).unwrap()
}

#[test]
fn scenario_bounded_load_is_bit_identical_to_full() {
    // PASS: the injected backtest emits a byte-identical pnl_by_period.ndjson whether
    // fed the full resolutions/schedules index or the market-bounded one — the extra
    // unrelated markets in the full index never affect any result. FAIL otherwise.
    let dir = tempfile::TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let injected = seed_cache(&cache_path);

    // Note: the byte comparison is valid because this fixture gives each wallet at
    // most ONE open-at-horizon position, so every `resolution_lags_secs` array has
    // length ≤ 1 and is order-invariant. `build_mtm_rows` collects those lags in
    // `open_positions` HashMap-iteration order, which differs between processes/runs
    // (a pre-existing nondeterminism unrelated to #453); a fixture with a multi-open
    // wallet would need the lag arrays sorted before comparing.
    let full = run(&cache_path, &dir.path().join("out_full"), &injected, false);
    let bounded = run(
        &cache_path,
        &dir.path().join("out_bounded"),
        &injected,
        true,
    );

    let pass = full == bounded && !full.trim().is_empty();
    println!(
        "PASS={pass} bounded_load.bit_identical: full_bytes={} bounded_bytes={} equal={}",
        full.len(),
        bounded.len(),
        full == bounded
    );
    assert!(
        pass,
        "bounded resolutions/schedules load must yield a byte-identical pnl_by_period.ndjson"
    );
}

#[test]
fn scenario_bounded_load_drops_untraded_markets() {
    // PASS: the bounded index is a STRICT subset of the full index — the full load
    // carries extra untraded markets the bounded load omits — so the bit-identity
    // test above is non-vacuous (it proves the extras don't matter, not that the two
    // indexes are trivially equal). FAIL otherwise.
    let dir = tempfile::TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let injected = seed_cache(&cache_path);

    let cache = WalletCache::open(&cache_path).unwrap();
    let mut all_trades = load_injected_trades(&cache, &injected);
    all_trades.sort_by_key(|t| t.timestamp.0);
    let markets: HashSet<MarketId> = all_trades.iter().map(|t| t.market_id.clone()).collect();

    let full_res = pe_bootstrap::gamma::load_resolutions(&cache).unwrap();
    let bounded_res = pe_bootstrap::gamma::load_resolutions_for_markets(&cache, &markets).unwrap();
    let full_sch = pe_bootstrap::gamma::load_schedules(&cache).unwrap();
    let bounded_sch = pe_bootstrap::gamma::load_schedules_for_markets(&cache, &markets).unwrap();

    // The bounded set is strictly smaller (it drops mkt-extra-*) and every bounded
    // key is both a traded market and present in the full index.
    let res_ok = full_res.len() > bounded_res.len()
        && bounded_res.keys().all(|k| markets.contains(k))
        && bounded_res.keys().all(|k| full_res.contains_key(k));
    let sch_ok = full_sch.len() > bounded_sch.len()
        && bounded_sch.keys().all(|k| markets.contains(k))
        && bounded_sch.keys().all(|k| full_sch.contains_key(k));
    let pass = res_ok && sch_ok;
    println!(
        "PASS={pass} bounded_load.drops_untraded: resolutions full={} bounded={}, schedules full={} bounded={}",
        full_res.len(),
        bounded_res.len(),
        full_sch.len(),
        bounded_sch.len()
    );
    assert!(
        pass,
        "bounded load must drop the untraded markets the full load carries"
    );
}
