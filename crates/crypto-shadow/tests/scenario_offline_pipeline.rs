#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! AC3(a) — offline, deterministic end-to-end smoke for `pe-crypto-shadow`.
//!
//! PASS: enumerate (injected `PageFetcher`) + decode (in-memory WS frame
//! fixtures) + fee-math + "write nothing" on a fresh DB + a real write/report
//! round-trip — all with NO live socket.
//! FAIL: any step errors, the fee math is wrong, or a fresh DB is non-empty.
//!
//! Determinism: no clock (timestamps derive from the parsed market window), no
//! RNG, no network (FixtureFetcher + literal frame strings), DB under a TempDir.

use std::collections::HashMap;

use pe_crypto_shadow::chainlink_ws::parse_chainlink_frame;
use pe_crypto_shadow::clob_ws::parse_clob_frame;
use pe_crypto_shadow::db::ShadowDb;
use pe_crypto_shadow::gamma::BtcMarketFetcher;
use pe_crypto_shadow::join::JoinState;
use pe_crypto_shadow::types::{BtcSeriesKind, FeedSource};
use pe_crypto_shadow::{ShadowConfig, generate_report};
use pe_source_polymarket_public::FixtureFetcher;
use rust_decimal_macros::dec;

const EVENTS_5M: &str = r#"[{"markets":[
  {"conditionId":"0xcond5","clobTokenIds":"[\"0xyes5\",\"0xno5\"]",
   "startDate":"2026-06-08T12:00:00Z","endDate":"2026-06-08T12:05:00Z",
   "orderPriceMinTickSize":"0.01"}
]}]"#;

#[tokio::test]
async fn scenario_offline_pipeline() {
    // 1. ENUMERATE via injected PageFetcher (no network).
    let mut fixtures = HashMap::new();
    fixtures.insert(
        "https://gamma.test/events?series_slug=btc-up-or-down-5m&closed=false".to_string(),
        EVENTS_5M.as_bytes().to_vec(),
    );
    let gamma = BtcMarketFetcher::new(
        "https://gamma.test".to_string(),
        FixtureFetcher::new(fixtures),
    );
    let markets = gamma.fetch_markets(&[BtcSeriesKind::Five]).await.unwrap();
    assert_eq!(markets.len(), 1, "enumerate one market");
    let start = markets[0].range_start_ms;

    // 2. DECODE in-memory WS frames + JOIN.
    let book_raw = format!(
        r#"{{"event_type":"book","asset_id":"0xyes5","bids":[{{"price":"0.48"}}],"asks":[{{"price":"0.52"}}],"timestamp":"{}"}}"#,
        start + 1000
    );
    let updates = parse_clob_frame(&book_raw).unwrap();
    assert_eq!(updates.len(), 1, "decode one book update");

    let cl_start = format!(
        r#"{{"symbol":"btc/usd","timestamp":{},"value":"59000"}}"#,
        start + 500
    );
    let cl_later = format!(
        r#"{{"symbol":"btc/usd","timestamp":{},"value":"60000"}}"#,
        start + 2000
    );
    let tick_start = parse_chainlink_frame(&cl_start).unwrap();
    let tick_later = parse_chainlink_frame(&cl_later).unwrap();

    let mut state = JoinState::new(markets);
    for u in updates {
        state.on_book_update(u);
    }
    let _ = state.on_chainlink_tick(&tick_start); // captures range-start = 59000
    let obs = state.on_chainlink_tick(&tick_later);
    assert_eq!(obs.len(), 1, "one observation per active market");

    // 3. FEE MATH: c=60000 > r=59000 => prob_up=1; ask=0.52; fee=0.07*0.52*0.48.
    let o = &obs[0];
    assert_eq!(o.instantaneous_prob_up, Some(dec!(1)));
    assert_eq!(o.best_ask, Some(dec!(0.52)));
    assert_eq!(o.fee_cost, Some(dec!(0.017472)));
    assert_eq!(o.net_edge_vs_ask, Some(dec!(0.48) - dec!(0.017472)));
    assert_eq!(o.feed_to_book_lag_ms, Some(1000));

    // 4. WRITE NOTHING on a fresh DB ...
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("s.db");
    let db = ShadowDb::open(&db_path).unwrap();
    assert_eq!(db.observation_count().unwrap(), 0, "dry-run writes nothing");
    assert_eq!(db.raw_tick_count().unwrap(), 0, "dry-run writes nothing");

    // ... then a real write + report round-trip.
    db.insert_raw_tick(FeedSource::Chainlink, start + 500, &cl_start)
        .unwrap();
    db.insert_observations(&obs).unwrap();
    assert_eq!(db.observation_count().unwrap(), 1);
    assert_eq!(db.raw_tick_count().unwrap(), 1);
    drop(db);

    let cfg = ShadowConfig {
        db_path: db_path.to_string_lossy().into_owned(),
        ..ShadowConfig::default()
    };
    let report = generate_report(&cfg).unwrap();
    assert!(
        report.contains("\"total_observations\": 1"),
        "report counts the observation"
    );
    assert!(
        report.contains("crypto_fees_v2"),
        "report stamps fee provenance"
    );

    println!(
        "PASS: scenario_offline_pipeline (enumerate + decode + fee-math + write-nothing + roundtrip)"
    );
}
