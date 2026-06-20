#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Scenario: Null-schedule rewrite pass (issue #137 Sub-PR 2; batched in #382).
//!
//! Operator-level end-to-end exercise of `GammaFetcher::rewrite_null_schedules` mirrored against the
//! realistic mix the live cache contains today:
//!
//! - Trade-set markets with **NULL `end_date_unix`** from the broken plain-URL path — the rewrite
//!   targets. Empirical curl: 99% are populated by `&closed=true`, 1% genuinely return `endDate=null`.
//! - Trade-set markets that already have a **populated `end_date_unix`** — must NOT be touched
//!   (regression guard on the cache method's `WHERE end_date_unix IS NULL` clause).
//! - Non-trade-set markets in `market_schedules` from the CLOB global walk — must NOT be passed to
//!   the rewrite call by the caller's scope filter (lib.rs intersects with the trade set).
//!
//! As of #382 the pass batches via the shared `GammaMarketsClient`, so fixtures are keyed on the
//! repeat-key `&closed=true&limit=500` batch URLs (chunked by `GAMMA_BATCH_SIZE`, input order).
//! Determinism: no live network — `FixtureFetcher` serves canned responses keyed by URL.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::gamma::GammaFetcher;
use pe_core_types::{MarketId, VenueMarketId};
use pe_source_polymarket_public::{FixtureFetcher, GAMMA_BATCH_SIZE};
use tempfile::TempDir;

const BASE_URL: &str = "https://gamma-api.polymarket.com";

/// The batched `&closed=true` URL for an in-order chunk of ids — matches `GammaMarketsClient`.
fn closed_batch_url(ids: &[String]) -> String {
    let mut u = format!("{BASE_URL}/markets?");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            u.push('&');
        }
        u.push_str("condition_ids=");
        u.push_str(id);
    }
    u.push_str("&closed=true&limit=500");
    u
}

/// Map the batched closed URLs for `ids` (chunked in order). Every id is a closed market; `end_iso`
/// returns `Some(iso)` for a populated `endDate` or `None` for the genuine `endDate=null` case.
fn closed_responses(
    ids: &[String],
    end_iso: impl Fn(&str) -> Option<&str>,
) -> HashMap<String, Vec<u8>> {
    let mut responses = HashMap::new();
    for chunk in ids.chunks(GAMMA_BATCH_SIZE) {
        let items: Vec<String> = chunk
            .iter()
            .map(|id| match end_iso(id) {
                Some(iso) => format!(
                    r#"{{"conditionId":"{id}","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomePrices":"[\"1\",\"0\"]","endDate":"{iso}","outcomes":"[\"Yes\",\"No\"]"}}"#
                ),
                None => format!(
                    r#"{{"conditionId":"{id}","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomes":"[\"Yes\",\"No\"]"}}"#
                ),
            })
            .collect();
        responses.insert(
            closed_batch_url(chunk),
            format!("[{}]", items.join(",")).into_bytes(),
        );
    }
    responses
}

fn open_cache() -> (TempDir, WalletCache) {
    let dir = TempDir::new().unwrap();
    let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    (dir, cache)
}

/// PASS criterion (single, falsifiable):
///   Given an N-market trade-set with 60% NULL-populatable, 20% NULL-no-enddate, and 20%
///   already-populated, the rewrite pass produces exactly 60% rewritten rows AND leaves 20% NULL
///   AND preserves all 20% populated rows untouched.
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

    // 20 NULL rows that &closed=true returns with endDate=null (the 1/100 empirical case).
    let mut null_no_enddate: Vec<String> = Vec::with_capacity(20);
    for i in 0..20 {
        let id = format!("0xnullbare{i:04}");
        cache.insert_schedule(&id, None, 1_700_000_000).unwrap();
        null_no_enddate.push(id);
    }

    // 20 rows already populated by an earlier source. The rewrite pass MUST leave these untouched —
    // guarded by `WHERE end_date_unix IS NULL` in the cache method.
    let mut populated_preserved: Vec<String> = Vec::with_capacity(20);
    let preserved_ts = 1_705_276_800_i64; // 2024-01-15T00:00:00Z
    for i in 0..20 {
        let id = format!("0xpop{i:04}");
        cache
            .insert_schedule_with_source(&id, Some(preserved_ts), 1_700_000_000, "clob")
            .unwrap();
        populated_preserved.push(id);
    }

    // Caller scope: trade-set ∩ NULL rows = the 80 NULL ids (populatable + no-endDate), in order.
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

    let rewritten_target_ts = 1_710_004_800_i64; // distinct from preserved_ts
    let target_iso = "2024-03-09T17:20:00Z"; // = 1_710_004_800
    // Populatable ids carry an endDate; the no-endDate cohort is present but without one.
    let responses = closed_responses(&trade_set_nulls, |id| {
        id.starts_with("0xnullpop").then_some(target_iso)
    });

    let gamma = GammaFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));
    let rewritten = gamma
        .rewrite_null_schedules(&trade_set_nulls, &mut cache)
        .await
        .unwrap();

    assert_eq!(
        rewritten, 60,
        "exactly the 60 populatable rows must be rewritten; no-endDate rows contribute 0"
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
            "populated_preserved row {id} must hold its original value"
        );
    }
}

/// PASS criterion:
///   The `all_market_ids ∩ null_schedule_market_ids` scope (mirroring lib.rs stage 6f) never
///   produces requests for markets outside the trade set, and every in-scope market is rewritten.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trade_set_intersection_scope_excludes_non_traded_markets() {
    let (_dir, mut cache) = open_cache();

    // Trade-set NULL markets (the wallet actually traded these).
    let trade_set_nulls = vec!["0xtraded_null_a".to_owned(), "0xtraded_null_b".to_owned()];
    for id in &trade_set_nulls {
        cache.insert_schedule(id, None, 1_700_000_000).unwrap();
    }

    // Non-trade-set NULL markets (from the CLOB global walk). The intersection must exclude them.
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

    // Map fixtures for exactly the in-scope ids (in scope order, so the batch URL matches).
    let responses = closed_responses(&scope, |_| Some("2024-03-09T17:20:00Z"));
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));
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
///   Calling `rewrite_null_schedules` with an empty slice is a no-op: returns Ok(0), issues zero
///   HTTP requests, leaves all rows unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn empty_scope_is_noop() {
    let (_dir, mut cache) = open_cache();
    cache
        .insert_schedule("0xa", Some(1_700_000_000), 1_700_000_001)
        .unwrap();
    cache.insert_schedule("0xb", None, 1_700_000_002).unwrap();

    // No fixture URLs mapped — any HTTP request would Fatal-error out.
    let gamma = GammaFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(HashMap::new()));
    let rewritten = gamma.rewrite_null_schedules(&[], &mut cache).await.unwrap();
    assert_eq!(rewritten, 0);

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
