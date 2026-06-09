#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Offline, deterministic end-to-end smoke for `pe-crypto-shadow` (issue #300
//! Phase 2 — median-trigger model).
//!
//! PASS: enumerate via the **event-slug** window (injected `PageFetcher`, Fix 2),
//! decode a CLOB book, decode the corrected Chainlink settlement frame (Fix 3),
//! then drive the exchange-consensus median across an outsized move so the
//! detector fires ONE observation with the right direction / fee / lag, and a
//! real write/report round-trip — all with NO live socket.
//! FAIL: any step errors, the window is not 5 min, the fee math is wrong, the
//! move does not fire exactly one observation, or a fresh DB is non-empty.
//!
//! Determinism: every clock value is a fixture (no wall clock), no RNG, no
//! network (FixtureFetcher + literal frame strings), DB under a TempDir.

use std::collections::HashMap;

use pe_crypto_shadow::chainlink_ws::parse_chainlink_frame;
use pe_crypto_shadow::clob_ws::parse_clob_frame;
use pe_crypto_shadow::db::ShadowDb;
use pe_crypto_shadow::gamma::BtcMarketFetcher;
use pe_crypto_shadow::join::JoinState;
use pe_crypto_shadow::types::{
    BtcSeriesKind, ExchangeTick, ExchangeVenue, FeedSource, MoveDirection,
};
use pe_crypto_shadow::{ShadowConfig, generate_report};
use pe_source_polymarket_public::FixtureFetcher;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

// Live-shaped event: window comes from the slug (5-min), ISO dates span ≈1 day.
// 1765192500 = 2025-12-08T12:35:00Z.
const EVENTS_5M: &str = r#"[{"slug":"btc-updown-5m-1765192500",
  "startDate":"2025-12-08T00:00:00Z","endDate":"2025-12-09T00:00:00Z",
  "markets":[
    {"conditionId":"0xcond5","clobTokenIds":"[\"0xyes5\",\"0xno5\"]",
     "orderPriceMinTickSize":"0.01"}
  ]}]"#;

fn etick(venue: ExchangeVenue, price: Decimal, t: i64) -> ExchangeTick {
    ExchangeTick {
        venue,
        price,
        observed_at_ms: t,
    }
}

#[tokio::test]
async fn scenario_offline_pipeline() {
    // 1. ENUMERATE via injected PageFetcher (no network). Window from the slug.
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
    let rs = markets[0].range_start_ms;
    assert_eq!(rs, 1_765_192_500_000, "range start from slug");
    assert_eq!(
        markets[0].range_end_ms - rs,
        5 * 60 * 1000,
        "5-min window from slug"
    );

    // 2. DECODE the corrected Chainlink settlement frame (Fix 3) — proves the
    //    nested-payload decoder works offline (the value is captured, not yet
    //    used to drive observations).
    let cl_raw = r#"{"topic":"crypto_prices","type":"update",
      "payload":{"symbol":"btc/usd","timestamp":1765192600000,"full_accuracy_value":"60024.00000000"}}"#;
    let cl_tick = parse_chainlink_frame(cl_raw).unwrap();
    assert_eq!(cl_tick.value.0, dec!(60024));

    // 3. DECODE a CLOB book frame + JOIN.
    let book_raw = format!(
        r#"[{{"asset_id":"0xyes5","bids":[{{"price":"0.48"}}],"asks":[{{"price":"0.52"}}],"timestamp":"{}"}}]"#,
        rs + 1000
    );
    let updates = parse_clob_frame(&book_raw).unwrap();
    assert_eq!(updates.len(), 1, "decode one book update");

    let mut state = JoinState::new(markets, ShadowConfig::default().consensus_params());
    for u in updates {
        state.on_book_update(u, rs + 1400); // book received at rs+1400
    }

    // 4. DRIVE the consensus median across a +4 bps move within the 300ms window.
    assert!(
        state
            .on_exchange_tick(
                &etick(ExchangeVenue::Bybit, dec!(60000), rs + 1100),
                rs + 1100
            )
            .is_empty(),
        "single venue -> no median"
    );
    assert!(
        state
            .on_exchange_tick(
                &etick(ExchangeVenue::Coinbase, dec!(60000), rs + 1150),
                rs + 1150
            )
            .is_empty(),
        "median 60000 captured as reference; first sample"
    );
    assert!(
        state
            .on_exchange_tick(
                &etick(ExchangeVenue::Bybit, dec!(60024), rs + 1400),
                rs + 1400
            )
            .is_empty(),
        "median 60012 (+2 bps) is below threshold"
    );
    let obs = state.on_exchange_tick(
        &etick(ExchangeVenue::Coinbase, dec!(60024), rs + 1450),
        rs + 1450,
    );
    assert_eq!(
        obs.len(),
        1,
        "one observation per active market on the move"
    );

    // 5. FEE MATH + move metadata. signal=60024 > ref=60000 => prob_up=1;
    //    ask=0.52; fee=0.07*0.52*0.48; lag = 1450 - 1400 = 50.
    let o = &obs[0];
    assert_eq!(o.move_direction, MoveDirection::Up);
    assert_eq!(o.move_magnitude_bps, dec!(4));
    assert_eq!(o.signal_value, dec!(60024));
    assert_eq!(o.instantaneous_prob_up, Some(dec!(1)));
    assert_eq!(o.best_ask, Some(dec!(0.52)));
    assert_eq!(o.fee_cost, Some(dec!(0.017472)));
    assert_eq!(o.net_edge_vs_ask, Some(dec!(0.48) - dec!(0.017472)));
    assert_eq!(o.feed_to_book_lag_ms, Some(50));

    // 6. WRITE NOTHING on a fresh DB ...
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("s.db");
    let db = ShadowDb::open(&db_path).unwrap();
    assert_eq!(db.observation_count().unwrap(), 0, "dry-run writes nothing");
    assert_eq!(db.raw_tick_count().unwrap(), 0, "dry-run writes nothing");

    // ... then a real write + report round-trip.
    db.insert_raw_tick(FeedSource::Coinbase, rs + 1450, "{}")
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
        "PASS: scenario_offline_pipeline (slug-enumerate + chainlink-decode + median-move-trigger + fee-math + roundtrip)"
    );
}
