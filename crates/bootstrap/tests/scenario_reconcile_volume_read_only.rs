//! Scenario tests for the read-only `reconcile-volume` dispatch path.
//!
//! Mirrors the seed-RW-then-call-RO pattern of `scenario_coverage.rs`
//! (issue #208). Each scenario seeds a cache via the read-write handle,
//! drops it, then exercises `run_reconcile_volume_read_only` — which is the
//! path the `pe-bootstrap reconcile-volume` subcommand now uses by default
//! so it can run alongside a long-running writer (e.g. `counterparty-edges`)
//! without conflicting on the `CacheMutationLock`.
//!
//! Each scenario has a single PASS/FAIL criterion written before the test
//! body. No network calls; all data is constructed in-process. Clock is
//! fixed via hardcoded timestamps; no RNG is used.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::reconcile_volume::{run_reconcile_volume, run_reconcile_volume_read_only};
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tempfile::TempDir;
use time::OffsetDateTime;

const WALLET_HEX: &str = "0x1111111111111111111111111111111111111111";
const MARKET: &str = "0xaaaa";
const TOKEN_ID: &str = "111";
/// 1 USDC = 10^6 raw uint256 units (USDC.e on Polygon). Matches the helper in
/// `reconcile_volume.rs`'s inline unit tests.
fn usdc(amount: u64) -> String {
    (amount * 1_000_000).to_string()
}

fn wallet(hex: &str) -> WalletAddress {
    WalletAddress::from_hex(hex).unwrap()
}

/// Seed: one V2 OrderFilled leg, `side = 0` (maker BUY → USDC = `maker_amount`),
/// `maker_asset_id_dec = TOKEN_ID` which resolves to `MARKET` via
/// `token_conditions`. 5 USDC on-chain volume.
///
/// Plus one Data-API trade on the same market with `price = 0.50`,
/// `contracts = 100` → USD volume = 50. So the per-market inflation ratio is
/// `50 / 5 = 10.0` — non-default, easy to verify visually if it ever shows up
/// in a failure log.
fn seed_populated(cache: &mut WalletCache) {
    cache
        .upsert_token_conditions_batch(&[(TOKEN_ID.to_owned(), MARKET.to_owned())], 1_700_000_000)
        .unwrap();

    cache
        .upsert_counterparty_edges_batch(&[(
            "0xdeadbeef".to_owned(), // tx_hash
            0i64,                    // log_index
            100i64,                  // block_number
            1_700_000_100i64,        // block_ts_unix
            "0xexchange".to_owned(), // contract_addr
            2i64,                    // contract_version (V2)
            "0xmaker".to_owned(),    // maker_hex
            "0xtaker".to_owned(),    // taker_hex
            TOKEN_ID.to_owned(),     // maker_asset_id_dec (resolves to MARKET)
            None::<String>,          // taker_asset_id_dec (V2 → NULL)
            Some(0i64),              // side = 0 → USDC = maker_amount
            usdc(5),                 // maker_amount_raw = 5 USDC
            "10".to_owned(),         // taker_amount_raw (position tokens, irrelevant)
            "0".to_owned(),          // fee_raw
        )])
        .unwrap();

    let trade = RawTrade {
        wallet: wallet(WALLET_HEX),
        market_id: MarketId(VenueMarketId(MARKET.to_owned())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price::new(dec!(0.50)).unwrap(),
        contracts: ContractQty(100),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_700_000_200).unwrap()),
        source_trade_id: SourceTradeId("trade-1".to_owned()),
    };
    cache.insert_new(WALLET_HEX, vec![trade]).unwrap();
}

// ── Scenario 1 — empty cache ────────────────────────────────────────────────
//
// PASS: `run_reconcile_volume_read_only` on a freshly-migrated, empty cache
//       returns the default report (`markets_reconciled = 0`,
//       `data_api_volume_usd = 0`, no ratios) without error.
// FAIL: any error from the read-only path, or non-default fields.
#[test]
fn empty_cache_yields_default_report() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("empty.db");

    // Migrate the empty schema via the RW handle, then drop so the RO open
    // sees a quiescent file (matches the `coverage` scenario pattern).
    drop(WalletCache::open(&db_path).unwrap());

    let report = run_reconcile_volume_read_only(&db_path)
        .expect("read-only reconcile-volume must not error on empty cache");

    assert_eq!(report.markets_reconciled, 0);
    assert_eq!(report.markets_only_data_api, 0);
    assert_eq!(report.markets_only_on_chain, 0);
    assert_eq!(report.data_api_volume_usd, Decimal::ZERO);
    assert_eq!(report.on_chain_volume_usd, Decimal::ZERO);
    assert!(report.aggregate_inflation_ratio.is_none());
    assert!(report.median_inflation_ratio.is_none());

    println!("PASS: empty cache yields default ReconcileReport via read-only path");
}

// ── Scenario 2 — populated cache, RO matches manual-RO baseline ────────────
//
// PASS: the report from `run_reconcile_volume_read_only(path)` is bit-for-bit
//       equal to the report from manually opening read-only via
//       `WalletCache::open_read_only(path)` and calling
//       `run_reconcile_volume(&cache)`, AND the report is non-default
//       (`markets_reconciled > 0`).
// FAIL: any field of the two reports differs, or the report is trivially
//       empty (would mean the seed didn't land).
#[test]
fn populated_cache_read_only_path_matches_manual_baseline() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("populated.db");

    // Seed via RW handle, then drop so both subsequent opens are independent
    // read-only handles (no held writer).
    {
        let mut cache = WalletCache::open(&db_path).unwrap();
        seed_populated(&mut cache);
    }

    // Baseline: explicit `open_read_only` + `run_reconcile_volume`. This is
    // what the new `run_reconcile_volume_read_only` wrapper is supposed to be
    // equivalent to — any divergence is a wiring bug in the wrapper.
    let baseline = {
        let cache_ro = WalletCache::open_read_only(&db_path).unwrap();
        run_reconcile_volume(&cache_ro).expect("manual RO baseline must not error")
    };

    let wrapped = run_reconcile_volume_read_only(&db_path)
        .expect("read-only wrapper must not error on populated cache");

    assert!(
        baseline.markets_reconciled > 0,
        "seed produced a trivially-empty report — fixture is wrong, not the wrapper"
    );
    assert_eq!(
        wrapped, baseline,
        "read-only wrapper should produce a bit-for-bit identical report to a \
         manual open_read_only + run_reconcile_volume"
    );

    println!("PASS: populated cache — read-only wrapper matches manual RO baseline");
}
