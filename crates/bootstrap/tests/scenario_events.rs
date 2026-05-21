//! Scenario tests for the Gamma `/events` sweep (issue #206).
//!
//! Each scenario has a single PASS/FAIL criterion written before the test body.
//! No network calls — the sweep runs against a `FixtureFetcher`. Clock is fixed
//! via hardcoded timestamps; no RNG is used.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::events::GammaEventsFetcher;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_source_polymarket_public::FixtureFetcher;
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use tempfile::TempDir;
use time::OffsetDateTime;

const BASE: &str = "https://gamma-api.polymarket.com";
const WALLET: &str = "0x1111111111111111111111111111111111111111";

fn trade(market: &str, id: &str) -> RawTrade {
    RawTrade {
        wallet: WalletAddress::from_hex(WALLET).unwrap(),
        market_id: MarketId(VenueMarketId(market.to_owned())),
        outcome_id: OutcomeId(0),
        side: Side::Buy,
        price: Price::new(Decimal::new(60, 2)).unwrap(),
        contracts: ContractQty(1),
        timestamp: SourceTimestamp(OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap()),
        source_trade_id: SourceTradeId(id.to_owned()),
    }
}

/// Seed three traded markets — 0xaa and 0xbb belong to one Gamma event, 0xcc has
/// no event — and run the sweep against a two-page fixture (one populated page,
/// then an empty page that terminates the sweep).
fn run_fixture_sweep() -> (WalletCache, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();
    cache
        .insert_new(
            WALLET,
            vec![
                trade("0xaa", "t-aa"),
                trade("0xbb", "t-bb"),
                trade("0xcc", "t-cc"),
            ],
        )
        .unwrap();

    let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
    responses.insert(
        format!("{BASE}/events?limit=500&offset=0"),
        br#"[{"id": 491919, "slug": "btc-event", "markets": [
            {"conditionId": "0xaa"}, {"conditionId": "0xbb"}
        ]}]"#
            .to_vec(),
    );
    responses.insert(
        format!("{BASE}/events?limit=500&offset=500"),
        b"[]".to_vec(),
    );

    let fetcher = GammaEventsFetcher::new(BASE.to_owned(), FixtureFetcher::new(responses));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let report = rt.block_on(fetcher.sweep(&mut cache)).unwrap();

    // Sanity on the report shape (not the scenario PASS criteria themselves).
    assert_eq!(report.total_traded_markets, 3);
    assert_eq!(report.conditions_mapped, 2);
    assert_eq!(report.orphan_self_mapped, 1);

    (cache, dir)
}

#[test]
fn scenario_events_groups_markets_under_shared_event() {
    // PASS: the two markets of one Gamma event map to the same event_id.
    // FAIL: they map to different event_ids, or are absent.
    let (cache, _dir) = run_fixture_sweep();
    let map = cache.load_market_event_map().unwrap();
    let aa = map.get("0xaa").map(String::as_str);
    let bb = map.get("0xbb").map(String::as_str);
    let pass = aa == Some("491919") && bb == Some("491919");
    println!(
        "Scenario events_groups_markets_under_shared_event: {} (0xaa={aa:?} 0xbb={bb:?})",
        if pass { "PASS" } else { "FAIL" }
    );
    assert!(pass, "0xaa and 0xbb must share event_id 491919");
}

#[test]
fn scenario_events_orphan_self_maps_and_full_coverage() {
    // PASS: every traded market has a market_events row (AC2), and the market
    //       with no Gamma event self-maps to itself (event_id == condition_id).
    // FAIL: any traded market is unmapped, or the orphan does not self-map.
    let (cache, _dir) = run_fixture_sweep();
    let map = cache.load_market_event_map().unwrap();
    let all_mapped = ["0xaa", "0xbb", "0xcc"]
        .iter()
        .all(|c| map.contains_key(*c));
    let orphan_self = map.get("0xcc").map(String::as_str) == Some("0xcc");
    let pass = all_mapped && orphan_self && map.len() == 3;
    println!(
        "Scenario events_orphan_self_maps_and_full_coverage: {} (mapped={} orphan_self={orphan_self})",
        if pass { "PASS" } else { "FAIL" },
        map.len()
    );
    assert!(
        pass,
        "all 3 traded markets must be mapped; 0xcc must self-map to itself"
    );
}
