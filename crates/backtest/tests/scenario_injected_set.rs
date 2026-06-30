#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario (#421 PR3): the `pe-backtest` injected-set path follows an explicit
//! wallet list (bypassing the ranker) and emits a per-period, per-wallet copy
//! P&L table (`pnl_by_period.ndjson`), bounded to the injected wallets.
//!
//! Fixture (deterministic — fixed timestamps/prices, no clock, no RNG):
//!   - Wallet A (injected) buys a market it WINS on day D1, plus a second
//!     market on day D3 (extends the date axis so the D3 resolution sweep
//!     closes the D1 positions).
//!   - Wallet B (injected) buys a market it LOSES on day D1.
//!   - Wallet C (NOT injected) is present in the cache but must never be loaded,
//!     copied, or emitted.
//!
//! Three independent PASS criteria, one per test:
//!   1. per-wallet realized P&L has the correct sign (winner +, loser −);
//!   2. the emitted table excludes the non-injected wallet (bounded-load);
//!   3. the emitted schema is well-formed (n_fills/notional/period_end/sentinel).

use std::collections::HashSet;

use pe_backtest::config::BacktestConfig;
use pe_backtest::simulation::{load_injected_trades, run_simulation_with};
use pe_bootstrap::cache::WalletCache;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_strategy_winner_follow::WinnerFollowStrategy;
use pe_trader_index::RankerConfig;
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use time::OffsetDateTime;

const T1: i64 = 1_700_000_000; // a UTC instant on day D1 (the buys)
const SEC_PER_DAY: i64 = 86_400;

#[derive(Deserialize)]
struct PnlRow {
    wallet: String,
    period_end: i64,
    realized_pnl: f64,
    unrealized_pnl: f64,
    n_fills: u64,
    notional: f64,
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

/// Build the fixture cache, run the injected-set backtest, and return the parsed
/// `pnl_by_period.ndjson` rows plus the three wallet hexes (A, B, C).
fn run_fixture() -> (Vec<PnlRow>, String, String, String) {
    let dir = tempfile::TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let out_dir = dir.path().join("out");

    let a = wallet(0xa1);
    let b = wallet(0xb2);
    let c = wallet(0xc3);

    {
        let mut cache = WalletCache::open(&cache_path).unwrap();
        cache
            .insert_new(
                &a.to_string(),
                vec![
                    buy(a, "mkt-win", "a-win", dec!(0.60), T1),
                    buy(a, "mkt-open", "a-open", dec!(0.50), T1 + 2 * SEC_PER_DAY),
                ],
            )
            .unwrap();
        cache
            .insert_new(
                &b.to_string(),
                vec![buy(b, "mkt-lose", "b-lose", dec!(0.40), T1)],
            )
            .unwrap();
        // Present in the cache, but NOT injected — bounded load must skip it.
        cache
            .insert_new(&c.to_string(), vec![buy(c, "mkt-c", "c-1", dec!(0.30), T1)])
            .unwrap();

        // Resolutions one day after D1: mkt-win → outcome 0 (A's side wins);
        // mkt-lose → outcome 1 (B's side 0 loses). Both are caught by the D3
        // sweep (D1 < resolution < D3). mkt-open never resolves (stays open).
        let r = T1 + SEC_PER_DAY;
        cache.insert_resolution("mkt-win", Some(0), r, r).unwrap();
        cache.insert_resolution("mkt-lose", Some(1), r, r).unwrap();
    }

    let cache = WalletCache::open(&cache_path).unwrap();
    let injected = vec![a, b];
    let mut all_trades = load_injected_trades(&cache, &injected);
    all_trades.sort_by_key(|t| t.timestamp.0); // run_simulation precondition

    let resolutions = pe_bootstrap::gamma::load_resolutions(&cache).unwrap();
    let schedules = pe_bootstrap::gamma::load_schedules(&cache).unwrap();
    let liq = pe_bootstrap::gamma::load_liquidity(&cache).unwrap();
    let snapshots = cache.load_all_snapshots().unwrap();

    let config = BacktestConfig {
        bootstrap_cache_path: cache_path.clone(),
        output_dir: out_dir.clone(),
        flat_usd: Some(dec!(25)),
        max_signal_price: None, // prices are well below any cap; keep it explicit
        min_signal_price: None,
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
        None, // no forward-MTM window in this scenario (unrealized stays the 0.0 sentinel)
    )
    .unwrap();

    let text = std::fs::read_to_string(out_dir.join("pnl_by_period.ndjson")).unwrap();
    let rows: Vec<PnlRow> = text
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    (rows, a.to_string(), b.to_string(), c.to_string())
}

fn realized_for(rows: &[PnlRow], hex: &str) -> f64 {
    rows.iter()
        .filter(|r| r.wallet == hex)
        .map(|r| r.realized_pnl)
        .sum()
}

fn fills_for(rows: &[PnlRow], hex: &str) -> u64 {
    rows.iter()
        .filter(|r| r.wallet == hex)
        .map(|r| r.n_fills)
        .sum()
}

#[test]
fn scenario_injected_per_wallet_pnl_signs() {
    let (rows, a_hex, b_hex, _c_hex) = run_fixture();
    let a_realized = realized_for(&rows, &a_hex);
    let b_realized = realized_for(&rows, &b_hex);

    // PASS: the winning copy (A) shows positive realized P&L and the losing
    // copy (B) shows negative — per-wallet attribution is correct.
    let pass = a_realized > 0.0 && b_realized < 0.0;
    println!(
        "Scenario: injected per-wallet P&L signs — {} (A={a_realized:.3}, B={b_realized:.3})",
        if pass { "PASS" } else { "FAIL" }
    );
    assert!(
        pass,
        "winner A must be +, loser B must be − (A={a_realized}, B={b_realized})"
    );
}

#[test]
fn scenario_injected_emit_excludes_non_injected() {
    let (rows, a_hex, b_hex, c_hex) = run_fixture();

    // PASS: the emitted table contains rows, and the non-injected wallet C never
    // appears — the run was bounded to the injected set end-to-end.
    let pass = !rows.is_empty()
        && rows.iter().all(|r| r.wallet != c_hex)
        && rows.iter().all(|r| r.wallet == a_hex || r.wallet == b_hex);
    println!(
        "Scenario: injected emit excludes non-injected wallet — {} ({} rows)",
        if pass { "PASS" } else { "FAIL" },
        rows.len()
    );
    assert!(
        pass,
        "non-injected wallet C must never appear in pnl_by_period.ndjson"
    );
}

#[test]
fn scenario_injected_schema_well_formed() {
    let (rows, a_hex, b_hex, _c_hex) = run_fixture();
    let a_fills = fills_for(&rows, &a_hex);
    let b_fills = fills_for(&rows, &b_hex);

    // PASS: A copied two buys (win + open), B copied one (lose), every row's
    // unrealized P&L is the documented 0.0 sentinel, period_end is a real
    // post-D1 day boundary, and notional is non-negative.
    let pass = a_fills == 2
        && b_fills == 1
        && rows
            .iter()
            .all(|r| r.unrealized_pnl == 0.0 && r.period_end > T1 && r.notional >= 0.0);
    println!(
        "Scenario: injected emit schema well-formed — {} (A fills={a_fills}, B fills={b_fills})",
        if pass { "PASS" } else { "FAIL" }
    );
    assert!(
        pass,
        "expected A n_fills=2, B n_fills=1, unrealized=0 sentinel, period_end>D1, notional>=0"
    );
}
