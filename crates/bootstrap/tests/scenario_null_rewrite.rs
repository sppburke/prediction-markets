#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Scenario: Null-schedule rewrite pass (issue #137 Sub-PR 2).
//!
//! Operator-level end-to-end exercise of `GammaFetcher::rewrite_null_schedules`
//! mirrored against the realistic mix the live cache contains today (post-PR
//! #139 / pre-#137 Sub-PR 2):
//!
//! - Trade-set markets with **NULL `end_date_unix`** from the broken plain-URL
//!   path — these are the rewrite targets. Empirical curl: 99% are populated
//!   by `&closed=true`, 1% genuinely return `endDate=null`.
//! - Trade-set markets that already have a **populated `end_date_unix`** —
//!   either CLOB-sourced or post-fix Gamma-sourced. Must NOT be touched
//!   (regression guard on the cache method's `WHERE end_date_unix IS NULL`
//!   clause).
//! - Non-trade-set markets that happen to be in `market_schedules` from the
//!   CLOB global walk — must NOT be passed to the rewrite call by the caller's
//!   scope filter (lib.rs intersects with the trade set).
//!
//! Determinism: no live network — `FixtureFetcher` serves canned responses
//! keyed by URL. No `SystemTime::now()` dependence in assertions.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::gamma::GammaFetcher;
use pe_core_types::{MarketId, VenueMarketId};
use pe_source_polymarket_public::FixtureFetcher;
use tempfile::TempDir;

const BASE_URL: &str = "https://gamma-api.polymarket.com";

fn closed_populated_fixture(condition_id: &str, end_date_iso: &str) -> Vec<u8> {
    format!(
        r#"[{{"conditionId":"{condition_id}","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomePrices":"[\"1\",\"0\"]","endDate":"{end_date_iso}","outcomes":"[\"Yes\",\"No\"]"}}]"#
    )
    .into_bytes()
}

fn closed_no_enddate_fixture(condition_id: &str) -> Vec<u8> {
    format!(
        r#"[{{"conditionId":"{condition_id}","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomes":"[\"Yes\",\"No\"]"}}]"#
    )
    .into_bytes()
}

fn closed_url(condition_id: &str) -> String {
    format!("{BASE_URL}/markets?condition_ids={condition_id}&closed=true")
}

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

/// PASS criterion (single, falsifiable):
///   Given an N-market trade-set with 60% NULL-populatable, 20% NULL-no-enddate,
///   and 20% already-populated, the rewrite pass produces exactly 60% rewritten
///   rows AND leaves 20% NULL AND preserves all 20% populated rows untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_trade_set_rewrites_only_populatable_nulls() {
    let (_dir, mut cache) = open_cache();

    // 60 NULL rows that the &closed=true URL will populate with valid endDates.
    let mut null_populatable: Vec<String> = Vec::with_capacity(60);
    for i in 0..60 {
        let id = format!("0xnullpop{i:04}");
        cache.insert_schedule(&id, None, 1_700_000_000).unwrap();
        null_populatable.push(id);
    }

    // 20 NULL rows that &closed=true will return with endDate=null (the 1/100
    // empirical case — closed market exists in Gamma but lacks endDate).
    let mut null_no_enddate: Vec<String> = Vec::with_capacity(20);
    for i in 0..20 {
        let id = format!("0xnullbare{i:04}");
        cache.insert_schedule(&id, None, 1_700_000_000).unwrap();
        null_no_enddate.push(id);
    }

    // 20 rows already populated by an earlier source (simulates CLOB-sourced
    // rows or post-fix Gamma-sourced rows). The rewrite pass MUST leave these
    // untouched — guarded by `WHERE end_date_unix IS NULL` in the cache method.
    let mut populated_preserved: Vec<String> = Vec::with_capacity(20);
    let preserved_ts = 1_705_276_800_i64; // 2024-01-15T00:00:00Z
    for i in 0..20 {
        let id = format!("0xpop{i:04}");
        cache
            .insert_schedule_with_source(&id, Some(preserved_ts), 1_700_000_000, "clob")
            .unwrap();
        populated_preserved.push(id);
    }

    // Build fixture map. The rewrite pass MUST only request URLs for the
    // 80 NULL rows; if it requests a populated row's URL, that's a behaviour
    // bug (we never filter to NULL inside the gamma method — caller does).
    // To verify, we map URLs only for the NULL rows. (Caller filtering is
    // exercised by passing only NULL ids to `rewrite_null_schedules`.)
    let mut responses = HashMap::new();
    let rewritten_target_ts = 1_710_004_800_i64; // distinct from preserved_ts
    let target_iso = "2024-03-09T17:20:00Z"; // 1_710_000_000 + 4800s = 1_710_004_800
    for id in &null_populatable {
        responses.insert(closed_url(id), closed_populated_fixture(id, target_iso));
    }
    for id in &null_no_enddate {
        responses.insert(closed_url(id), closed_no_enddate_fixture(id));
    }

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    // Combine the trade-set scope the caller would intersect:
    //   all_market_ids ∩ null_schedule_market_ids
    let trade_set_nulls: Vec<String> = null_populatable
        .iter()
        .chain(null_no_enddate.iter())
        .cloned()
        .collect();
    assert_eq!(
        trade_set_nulls.len(),
        80,
        "test setup: 60 populatable + 20 no-endDate"
    );

    let rewritten = gamma
        .rewrite_null_schedules(&trade_set_nulls, &mut cache)
        .await
        .unwrap();

    assert_eq!(
        rewritten, 60,
        "exactly the 60 populatable rows must be rewritten; no-endDate rows must contribute 0"
    );

    // Verify each cohort's post-state.
    let schedules = cache.load_all_schedules().unwrap();
    for id in &null_populatable {
        let sched = schedules
            .get(&MarketId(VenueMarketId(id.clone())))
            .unwrap_or_else(|| panic!("populated NULL row missing: {id}"));
        assert_eq!(
            sched.end_date_unix,
            Some(rewritten_target_ts),
            "null_populatable row {id} must hold the parsed endDate"
        );
    }
    for id in &null_no_enddate {
        let sched = schedules
            .get(&MarketId(VenueMarketId(id.clone())))
            .unwrap_or_else(|| panic!("no-endDate row missing: {id}"));
        assert_eq!(
            sched.end_date_unix, None,
            "null_no_enddate row {id} must remain NULL"
        );
    }
    for id in &populated_preserved {
        let sched = schedules
            .get(&MarketId(VenueMarketId(id.clone())))
            .unwrap_or_else(|| panic!("preserved row missing: {id}"));
        assert_eq!(
            sched.end_date_unix,
            Some(preserved_ts),
            "populated_preserved row {id} must hold its original value, not the rewrite candidate value"
        );
    }
}

/// PASS criterion:
///   `cache.null_schedule_market_ids()` returns exactly the NULL rows; the
///   intersection with the trade set (mirroring lib.rs stage 6f) is the
///   correct candidate scope, never producing requests for markets outside
///   the trade set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trade_set_intersection_scope_excludes_non_traded_markets() {
    let (_dir, mut cache) = open_cache();

    // Trade-set NULL markets (the wallet actually traded these).
    let trade_set_nulls = vec!["0xtraded_null_a".to_owned(), "0xtraded_null_b".to_owned()];
    for id in &trade_set_nulls {
        cache.insert_schedule(id, None, 1_700_000_000).unwrap();
    }

    // Non-trade-set NULL markets (came in from CLOB global walk). Stage 6f
    // must not request these — the `all_market_ids ∩ null_schedule_market_ids`
    // intersection in lib.rs excludes them.
    let non_traded_nulls = vec![
        "0xglobal_null_a".to_owned(),
        "0xglobal_null_b".to_owned(),
        "0xglobal_null_c".to_owned(),
    ];
    for id in &non_traded_nulls {
        cache
            .insert_schedule_with_source(id, None, 1_700_000_000, "clob")
            .unwrap();
    }

    // Simulate the trade-set the caller computes from `cache.all_market_ids()`.
    let trade_set: std::collections::HashSet<String> = trade_set_nulls.iter().cloned().collect();
    let all_nulls = cache.null_schedule_market_ids();
    let scope: Vec<String> = all_nulls.intersection(&trade_set).cloned().collect();

    assert_eq!(
        scope.len(),
        2,
        "intersection must yield exactly the traded NULL markets"
    );
    for id in &trade_set_nulls {
        assert!(
            scope.contains(id),
            "traded NULL market {id} must be in the rewrite scope"
        );
    }
    for id in &non_traded_nulls {
        assert!(
            !scope.contains(id),
            "non-traded NULL market {id} must NOT be in the rewrite scope"
        );
    }

    // Map fixture only for the in-scope IDs. If the implementation regressed
    // and tried to fetch a non-traded ID, FixtureFetcher would return Fatal,
    // and the in-test assertion below would still pass (we'd just see a warn
    // and a rewritten count below expectations). The explicit non-presence in
    // the scope set IS the operator assertion.
    let mut responses = HashMap::new();
    for id in &scope {
        responses.insert(
            closed_url(id),
            closed_populated_fixture(id, "2024-03-09T17:20:00Z"),
        );
    }

    let fetcher = FixtureFetcher::new(responses);
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);
    let rewritten = gamma
        .rewrite_null_schedules(&scope, &mut cache)
        .await
        .unwrap();
    assert_eq!(
        rewritten,
        scope.len(),
        "every scope market must be rewritten"
    );

    // Sanity: the non-traded global-NULL rows stay NULL.
    let schedules = cache.load_all_schedules().unwrap();
    for id in &non_traded_nulls {
        let sched = schedules
            .get(&MarketId(VenueMarketId(id.clone())))
            .unwrap_or_else(|| panic!("non-traded row vanished: {id}"));
        assert_eq!(
            sched.end_date_unix, None,
            "non-traded global-NULL {id} must stay NULL (rewrite must not touch it)"
        );
    }
}

/// PASS criterion:
///   Calling `rewrite_null_schedules` with an empty slice is a no-op:
///   returns Ok(0), issues zero HTTP requests, leaves all rows unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn empty_scope_is_noop() {
    let (_dir, mut cache) = open_cache();
    cache
        .insert_schedule("0xa", Some(1_700_000_000), 1_700_000_001)
        .unwrap();
    cache.insert_schedule("0xb", None, 1_700_000_002).unwrap();

    // No fixture URLs mapped — any HTTP request would Fatal-error out.
    let fetcher = FixtureFetcher::new(HashMap::new());
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), fetcher);

    let rewritten = gamma.rewrite_null_schedules(&[], &mut cache).await.unwrap();
    assert_eq!(rewritten, 0);

    // Both rows untouched.
    let schedules = cache.load_all_schedules().unwrap();
    assert_eq!(schedules.len(), 2);
    assert_eq!(
        schedules
            .get(&MarketId(VenueMarketId("0xa".to_owned())))
            .unwrap()
            .end_date_unix,
        Some(1_700_000_000)
    );
    assert_eq!(
        schedules
            .get(&MarketId(VenueMarketId("0xb".to_owned())))
            .unwrap()
            .end_date_unix,
        None
    );
}
