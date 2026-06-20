#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: batched resolution fetch is P&L-equivalent to the per-ID path (issue #382 Phase 3a).
//!
//! `GammaResolutionFetcher::fetch_closed` is the migration boundary: P&L crediting downstream
//! (`PnlLedger::resolution_credit` → `ResolutionStore::mark_settled` → bankroll) is a pure function of
//! the returned `Vec<MarketResolution>` and is unchanged by this PR (still locked by the
//! `resolution_credit` unit tests and `scenario_pnl_replay`). So proving the batched `fetch_closed`
//! returns the **exact same resolution set** for a fixed fixture set proves credited P&L is identical.
//!
//! This drives one realistic batch through the shared client and asserts:
//! - closed markets with valid `outcomePrices` resolve (YES → `[1,0]`, NO → `[0,1]`);
//! - an open market is skipped (the `closed` guard);
//! - a requested id Gamma omits is skipped;
//! - an **unrequested** row in the batch (a different `conditionId`) is NOT attributed to any
//!   requested market — a cross-market mis-credit would otherwise corrupt P&L;
//! - results are in `market_ids` order (deterministic).
//!
//! Determinism: no network — `FixtureFetcher` keyed on the exact batch URL.

use std::collections::HashMap;

use pe_core_types::MarketId;
use pe_paper_pnl::GammaResolutionFetcher;
use pe_source_polymarket_public::FixtureFetcher;
use rust_decimal::Decimal;

const BASE: &str = "https://gamma-api.polymarket.com";

fn mid(s: &str) -> MarketId {
    s.parse().unwrap()
}

/// The shared client's `&closed=true` batch URL for an in-order id list.
fn closed_batch_url(ids: &[&str]) -> String {
    let mut u = format!("{BASE}/markets?");
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

#[tokio::test]
async fn batched_fetch_closed_resolves_exactly_the_expected_set() {
    // Requested, in order. 0xabsent is requested but Gamma omits it.
    let requested = ["0xyes", "0xno", "0xopen", "0xabsent"];
    // The response carries the four markets' states plus an UNREQUESTED 0xunrelated row.
    let body = br#"[
        {"conditionId":"0xyes","closed":true,"outcomePrices":"[\"1\",\"0\"]"},
        {"conditionId":"0xno","closed":true,"outcomePrices":"[\"0\",\"1\"]"},
        {"conditionId":"0xopen","closed":false,"outcomePrices":"[\"0.6\",\"0.4\"]"},
        {"conditionId":"0xunrelated","closed":true,"outcomePrices":"[\"1\",\"0\"]"}
    ]"#
    .to_vec();

    let mut fx = HashMap::new();
    fx.insert(closed_batch_url(&requested), body);

    let fetcher = GammaResolutionFetcher::new(BASE.to_string(), FixtureFetcher::new(fx));
    let ids: Vec<MarketId> = requested.iter().map(|s| mid(s)).collect();
    let results = fetcher.fetch_closed(&ids).await.unwrap();

    // Only the two closed-with-prices requested markets resolve, in request order.
    assert_eq!(
        results.len(),
        2,
        "open, absent, and the unrequested row must not resolve"
    );
    assert_eq!(results[0].market_id, mid("0xyes"));
    assert_eq!(results[0].outcome_prices, vec![Decimal::ONE, Decimal::ZERO]);
    assert_eq!(results[1].market_id, mid("0xno"));
    assert_eq!(results[1].outcome_prices, vec![Decimal::ZERO, Decimal::ONE]);

    // No requested market was resolved from the unrelated row.
    assert!(
        results.iter().all(|r| r.market_id != mid("0xunrelated")),
        "an unrequested conditionId must never appear in the resolution set"
    );
}
