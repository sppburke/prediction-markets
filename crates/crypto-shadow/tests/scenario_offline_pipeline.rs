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
use pe_crypto_shadow::clob_ws::{parse_clob_frame, parse_clob_trade};
use pe_crypto_shadow::db::ShadowDb;
use pe_crypto_shadow::gamma::BtcMarketFetcher;
use pe_crypto_shadow::join::JoinState;
use pe_crypto_shadow::report::build_realized;
use pe_crypto_shadow::types::{
    BtcSeriesKind, EdgeObservation, ExchangeTick, ExchangeVenue, FeedSource, MoveDirection,
};
use pe_crypto_shadow::{BtcResolutionFetcher, ShadowConfig, generate_report};
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

/// CLOB trade-tape capture (v2 market-making data): the runner's Clob arm runs
/// BOTH decoders on every frame — `parse_clob_frame` (book) and
/// `parse_clob_trade` (the `last_trade_price` print) — and persists each trade
/// with its resolved condition/series for the offline maker-vs-taker comparison.
///
/// PASS: a `last_trade_price` frame on a known token decodes, resolves to its
/// market via `lookup_token`, and inserts exactly one `clob_trades` row;
/// re-delivering the same transaction hash is idempotent; and a `price_change`
/// frame is not a trade, leaving the count unchanged.
/// FAIL: the trade is dropped, double-counted, attributed to no market, or a
/// price_change frame is mistaken for a trade.
#[tokio::test]
async fn scenario_trade_capture() {
    // Enumerate one 5m market so the join knows token "0xyes5".
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
    let state = JoinState::new(markets, ShadowConfig::default().consensus_params());

    // A live-shaped trade print on the KNOWN YES token; its `market` field is
    // the condition_id "0xcond5" (matches the enumerated market).
    let trade_frame = r#"{"market":"0xcond5","asset_id":"0xyes5","price":"0.78","size":"5.166663","fee_rate_bps":"0","side":"BUY","timestamp":"1781032143544","event_type":"last_trade_price","transaction_hash":"0xdeadbeef"}"#;

    // The runner's Clob arm: the book decoder yields nothing on a trade frame ...
    assert!(
        parse_clob_frame(trade_frame).unwrap().is_empty(),
        "a trade frame is not a book update (no double-count)"
    );
    // ... and the trade decoder yields exactly the print, self-attributed to its
    // market via the frame's own `market` field (no join lookup needed).
    let trade = parse_clob_trade(trade_frame).unwrap().unwrap();
    assert_eq!(trade.token_id, "0xyes5");
    assert_eq!(
        trade.condition_id, "0xcond5",
        "condition_id from frame's market"
    );

    // The join supplies only the series label for a known token.
    let (_cond, series) = state.lookup_token(&trade.token_id);
    assert_eq!(series, Some("5m"));

    let dir = tempfile::tempdir().unwrap();
    let db = ShadowDb::open(&dir.path().join("s.db")).unwrap();
    db.insert_clob_trade(&trade, 1_781_032_143_600, series)
        .unwrap();
    assert_eq!(db.clob_trade_count().unwrap(), 1, "one trade persisted");

    // Idempotent on transaction_hash (INSERT OR IGNORE): a re-delivered print
    // (different received clock, same hash) does not double-count.
    db.insert_clob_trade(&trade, 1_781_032_143_999, series)
        .unwrap();
    assert_eq!(
        db.clob_trade_count().unwrap(),
        1,
        "dedup on transaction_hash"
    );

    // ROBUSTNESS: a trade prints on a token the join does NOT yet know (a new 5m
    // market that traded before its book was enumerated). It is STILL attributed
    // to its market via the frame's `market` field — series is NULL (recoverable
    // offline), but the trade is never dropped or left unattributed.
    let unknown_frame = r#"{"market":"0xcondZ","asset_id":"0xunknown","price":"0.51","size":"2","fee_rate_bps":"0","side":"SELL","timestamp":"1781032144000","event_type":"last_trade_price","transaction_hash":"0xfeed01"}"#;
    let unknown = parse_clob_trade(unknown_frame).unwrap().unwrap();
    let (_uc, useries) = state.lookup_token(&unknown.token_id);
    assert_eq!(useries, None, "unknown token has no series from the join");
    assert_eq!(
        unknown.condition_id, "0xcondZ",
        "still attributed via frame"
    );
    db.insert_clob_trade(&unknown, 1_781_032_144_050, useries)
        .unwrap();
    assert_eq!(
        db.clob_trade_count().unwrap(),
        2,
        "unknown-token trade captured"
    );

    // A price_change frame is book state, not a trade -> no row added.
    let pc = r#"{"market":"0xcond5","price_changes":[{"asset_id":"0xyes5","best_bid":"0.77","best_ask":"0.79"}]}"#;
    assert!(
        parse_clob_trade(pc).unwrap().is_none(),
        "price_change is not a trade"
    );
    assert_eq!(
        db.clob_trade_count().unwrap(),
        2,
        "price_change adds no trade row"
    );

    println!(
        "PASS: scenario_trade_capture (self-attributed via frame market incl. unknown token, dedup on tx hash, price_change ignored)"
    );
}

/// A 5m **up**-move observation in market `cid` priced at YES ask `yes_ask`.
/// `no_best_ask` is set but unused by up-move scoring (the up leg buys YES).
fn up_obs(cid: &str, yes_ask: Decimal) -> EdgeObservation {
    obs(cid, MoveDirection::Up, yes_ask, dec!(0.49))
}

/// A 5m **down**-move observation in market `cid` priced at the real NO ask
/// `no_ask` (the Down-buy entry). `best_ask` (YES) is set but unused down-scoring.
fn down_obs(cid: &str, no_ask: Decimal) -> EdgeObservation {
    obs(cid, MoveDirection::Down, dec!(0.50), no_ask)
}

fn obs(cid: &str, dir: MoveDirection, yes_ask: Decimal, no_ask: Decimal) -> EdgeObservation {
    let up = dir == MoveDirection::Up;
    EdgeObservation {
        condition_id: cid.to_string(),
        series: BtcSeriesKind::Five,
        observed_at_ms: 1_000,
        signal_value: dec!(60024),
        range_start_value: Some(dec!(60000)),
        instantaneous_prob_up: Some(if up { dec!(1) } else { dec!(0) }),
        best_ask: Some(yes_ask),
        mid: Some(yes_ask),
        no_best_ask: Some(no_ask),
        no_mid: Some(no_ask),
        gross_edge_vs_ask: None,
        gross_edge_vs_mid: None,
        fee_cost: None,
        net_edge_vs_ask: None,
        net_edge_vs_mid: None,
        feed_to_book_lag_ms: Some(50),
        move_magnitude_bps: dec!(4),
        move_direction: dir,
    }
}

/// Key-free realized-edge join (issue #300 AC2.3 closed without the Chainlink
/// settlement key): the exact sequence `runner::resolve` runs, but with an
/// injected `PageFetcher` so it is offline + deterministic — now **direction
/// aware**: up-moves score the YES buy, down-moves score the real NO buy.
///
/// PASS: four observed markets (Up-won, Down-won, still-open, and a down-move
/// that settles Down) resolve through the real SQLite LEFT JOIN into an `up`
/// group scored on the YES ask (mean `-0.077136`, open market excluded) and a
/// `down` group scored on the NO ask (mean `0.532675`).
/// FAIL: the open market is scored, a down-move is scored on the YES side, the
/// join drops a resolved row, or the report omits a realized section.
#[tokio::test]
async fn scenario_realized_edge_join() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("s.db");
    let db = ShadowDb::open(&db_path).unwrap();

    // condA up/Up-won, condB up/Down-won, condC up/open, condD down/Down-won.
    db.insert_observations(&[
        up_obs("0xcondA", dec!(0.52)),
        up_obs("0xcondB", dec!(0.60)),
        up_obs("0xcondC", dec!(0.55)),
        down_obs("0xcondD", dec!(0.45)),
    ])
    .unwrap();

    // The markets `resolve` will fetch outcomes for — exactly the observed set.
    let mut cids = db.distinct_observation_condition_ids().unwrap();
    cids.sort();
    assert_eq!(
        cids,
        vec![
            "0xcondA".to_string(),
            "0xcondB".to_string(),
            "0xcondC".to_string(),
            "0xcondD".to_string()
        ],
        "resolve targets every observed market"
    );

    // Injected Gamma resolutions: condA Up-won, condB & condD Down-won, condC
    // still open (empty list ⇒ skipped, mirroring an in-progress 5m market).
    let mut fx = HashMap::new();
    fx.insert(
        "https://gamma.test/markets?condition_ids=0xcondA&closed=true".to_string(),
        br#"[{"conditionId":"0xcondA","closed":true,"outcomePrices":"[\"1\",\"0\"]"}]"#.to_vec(),
    );
    fx.insert(
        "https://gamma.test/markets?condition_ids=0xcondB&closed=true".to_string(),
        br#"[{"conditionId":"0xcondB","closed":true,"outcomePrices":"[\"0\",\"1\"]"}]"#.to_vec(),
    );
    fx.insert(
        "https://gamma.test/markets?condition_ids=0xcondC&closed=true".to_string(),
        b"[]".to_vec(),
    );
    fx.insert(
        "https://gamma.test/markets?condition_ids=0xcondD&closed=true".to_string(),
        br#"[{"conditionId":"0xcondD","closed":true,"outcomePrices":"[\"0\",\"1\"]"}]"#.to_vec(),
    );

    let resolver =
        BtcResolutionFetcher::new("https://gamma.test".to_string(), FixtureFetcher::new(fx));
    let resolutions = resolver.fetch_resolutions(&cids).await.unwrap();
    assert_eq!(resolutions.len(), 3, "the open market yields no resolution");
    // Persist with a fixture clock (no wall clock), as `runner::resolve` does.
    for r in &resolutions {
        db.upsert_resolution(&r.condition_id, r.yes_won, 1_765_000_000_000)
            .unwrap();
    }
    assert_eq!(db.resolution_count().unwrap(), 3);

    // The real LEFT JOIN keeps all four observations; three carry an outcome.
    let rows = db.all_realized_rows().unwrap();
    assert_eq!(rows.len(), 4, "LEFT JOIN keeps the unresolved observation");
    let scored = rows.iter().filter(|r| r.yes_won.is_some()).count();
    assert_eq!(scored, 3, "only resolved markets are scored");

    let groups = build_realized(&rows);
    assert_eq!(groups.len(), 2, "an up group and a down group");

    let up = groups.iter().find(|g| g.move_direction == "up").unwrap();
    assert_eq!(up.series, "5m");
    assert_eq!(up.count_scored, 2, "open market excluded from the score");
    assert_eq!(up.frac_realized_positive, Some(dec!(0.5)));
    // YES leg: (1 - 0.52 - fee(0.52)) + (0 - 0.60 - fee(0.60)) = 0.462528 - 0.6168
    assert_eq!(up.mean_realized_net_vs_ask, Some(dec!(-0.077136)));

    let down = groups.iter().find(|g| g.move_direction == "down").unwrap();
    assert_eq!(down.count_scored, 1);
    // NO leg: Down won, NO ask 0.45 -> 1 - 0.45 - fee(0.45) = 0.55 - 0.017325
    assert_eq!(down.mean_realized_net_vs_ask, Some(dec!(0.532675)));
    assert_eq!(down.frac_realized_positive, Some(dec!(1)));

    drop(db);

    // `generate_report` surfaces both realized groups over the same DB.
    let cfg = ShadowConfig {
        db_path: db_path.to_string_lossy().into_owned(),
        ..ShadowConfig::default()
    };
    let report = generate_report(&cfg).unwrap();
    assert!(
        report.contains("\"realized\""),
        "report has a realized section"
    );
    assert!(
        report.contains("\"move_direction\": \"up\""),
        "report carries the up realized group"
    );
    assert!(
        report.contains("\"move_direction\": \"down\""),
        "report carries the down realized group"
    );

    println!(
        "PASS: scenario_realized_edge_join (direction-aware: up YES-buy mean -0.077136, down NO-buy mean 0.532675)"
    );
}
