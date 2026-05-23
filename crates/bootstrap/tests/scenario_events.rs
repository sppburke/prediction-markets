//! Scenario tests for the Gamma `/events` sweep (issue #206).
//!
//! Each scenario has a single PASS/FAIL criterion written before the test body.
//! No network calls — the sweep runs against a `FixtureFetcher`. Clock is fixed
//! via hardcoded timestamps; no RNG is used.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::events::GammaEventsFetcher;
use pe_core_types::{
    ContractQty, MarketId, OutcomeId, Price, Side, SourceTimestamp, SourceTradeId, VenueMarketId,
    WalletAddress,
};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{FixtureFetcher, PageFetcher};
use pe_trader_index::snapshot::RawTrade;
use rust_decimal::Decimal;
use tempfile::TempDir;
use time::OffsetDateTime;

/// A `PageFetcher` that serves one good page then returns HTTP 422, simulating
/// Gamma's behaviour when `offset >= total_event_count`.
struct OneThenHttp422Fetcher {
    good_response: Vec<u8>,
    calls: Arc<AtomicUsize>,
}

impl OneThenHttp422Fetcher {
    fn new(good_response: Vec<u8>) -> Self {
        Self {
            good_response,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl PageFetcher for OneThenHttp422Fetcher {
    async fn fetch_page(&self, _url: &str) -> Result<Vec<u8>, SourceError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            Ok(self.good_response.clone())
        } else {
            Err(SourceError::Fatal {
                message: "HTTP 422".to_owned(),
            })
        }
    }
}

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
            {"conditionId": "0xaa", "clobTokenIds": "[\"111\",\"222\"]"},
            {"conditionId": "0xbb", "clobTokenIds": "[\"333\",\"444\"]"}
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
    assert_eq!(report.tokens_mapped, 4); // 2 markets × 2 clobTokenIds
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

#[test]
fn scenario_events_maps_token_ids_to_conditions() {
    // PASS: every clobTokenId from the swept markets resolves to its market's
    //       conditionId in token_conditions (issue #207 — the on-chain join map).
    // FAIL: any token id is missing or maps to the wrong condition.
    let (cache, _dir) = run_fixture_sweep();
    let pass = cache.token_condition_count() == 4
        && cache.condition_for_token("111").as_deref() == Some("0xaa")
        && cache.condition_for_token("222").as_deref() == Some("0xaa")
        && cache.condition_for_token("333").as_deref() == Some("0xbb")
        && cache.condition_for_token("444").as_deref() == Some("0xbb");
    println!(
        "Scenario events_maps_token_ids_to_conditions: {} (count={})",
        if pass { "PASS" } else { "FAIL" },
        cache.token_condition_count()
    );
    assert!(
        pass,
        "all 4 clobTokenIds must map to their market's conditionId"
    );
}

#[test]
fn scenario_events_422_at_page_boundary_treated_as_end_of_data() {
    // PASS: sweep completes without error when Gamma returns HTTP 422 on the
    //       second page (Gamma's "offset >= total" signal). Events from the
    //       first page must be mapped correctly.
    // FAIL: sweep returns Err (422 fatally aborts the sweep).
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("c.db")).unwrap();

    let good_page = br#"[{"id": 1, "slug": "s", "markets": [
        {"conditionId": "0xdd", "clobTokenIds": "[\"555\"]"}
    ]}]"#
        .to_vec();

    let fetcher = GammaEventsFetcher::new(BASE.to_owned(), OneThenHttp422Fetcher::new(good_page));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = rt.block_on(fetcher.sweep(&mut cache));
    let pass = result.is_ok() && result.unwrap().events_seen == 1;
    println!(
        "Scenario events_422_at_page_boundary_treated_as_end_of_data: {}",
        if pass { "PASS" } else { "FAIL" }
    );
    assert!(
        pass,
        "HTTP 422 from Gamma must terminate sweep cleanly, not abort"
    );
}
