#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Scenario: CLOB token-order cross-check quarantines mispriced markets (issue #429).
//!
//! The full-universe token→condition map (with positional `outcome_index`) the
//! true_clv join relies on is correct only if CLOB `/markets` `tokens[]` order
//! matches the authoritative Gamma `clob_token_ids` order. `token_id` is the
//! on-chain CTF positionId — a globally-unique `(condition, outcome)` anchor — so
//! a token whose CLOB array position differs from its stored Gamma
//! `outcome_index` proves the two orderings disagree.
//!
//! This scenario seeds a Gamma-sourced map (as the `events` sweep would write it,
//! now with `outcome_index`), then runs a CLOB closed-markets fixture in which one
//! market's tokens are in DIVERGING order and another is fresh/consistent.
//!
//! PASS: the diverging market is quarantined — its prior token rows are left
//!       UNCHANGED (never overwritten with the wrong order) and counted in
//!       `order_mismatches`; the consistent market's tokens are written with the
//!       correct positional `outcome_index`.
//! FAIL: the diverging market's rows are overwritten (silent misprice), it is not
//!       counted, or the consistent market fails to map.

use std::collections::HashMap;

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::clob::ClobFetcher;
use pe_source_polymarket_public::FixtureFetcher;
use tempfile::TempDir;

const BASE_URL: &str = "https://clob.example";

fn page_url() -> String {
    format!("{BASE_URL}/markets?closed=true&limit=1000")
}

/// Two closed markets on one page:
/// - `0xdiverge`: tokens `[T1, T0]` — the REVERSE of the seeded Gamma order
///   (Gamma has T0 at outcome_index 0, T1 at 1), so CLOB position 0 → T1 diverges.
/// - `0xfresh`: tokens `[F0, F1]` — no prior map, consistent, must write 0/1.
fn page_body() -> Vec<u8> {
    br#"{
      "data": [
        {"condition_id":"0xdiverge","end_date_iso":"2024-01-15T00:00:00Z","closed":true,
         "tokens":[{"token_id":"T1","winner":false},{"token_id":"T0","winner":true}]},
        {"condition_id":"0xfresh","end_date_iso":"2024-02-01T00:00:00Z","closed":true,
         "tokens":[{"token_id":"F0","winner":true},{"token_id":"F1","winner":false}]}
      ],
      "next_cursor":"LTE="
    }"#
    .to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clob_token_order_divergence_is_quarantined() {
    let dir = TempDir::new().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

    // Seed the authoritative Gamma-sourced map for 0xdiverge: T0 = outcome 0 (YES),
    // T1 = outcome 1 (NO) — as the `events` sweep writes it with outcome_index.
    cache
        .upsert_token_conditions_batch(
            &[
                ("T0".to_owned(), "0xdiverge".to_owned(), 0),
                ("T1".to_owned(), "0xdiverge".to_owned(), 1),
            ],
            100,
        )
        .unwrap();

    let mut responses: HashMap<String, Vec<u8>> = HashMap::new();
    responses.insert(page_url(), page_body());
    let clob = ClobFetcher::new(BASE_URL.to_owned(), FixtureFetcher::new(responses));

    let report = clob.fetch_closed_markets(&mut cache).await.unwrap();

    // 0xdiverge quarantined: counted once, its prior rows untouched.
    assert_eq!(
        report.order_mismatches, 1,
        "the reversed-order market must be quarantined exactly once"
    );
    assert_eq!(
        cache.token_condition_outcome("T0"),
        Some(("0xdiverge".to_owned(), Some(0))),
        "T0 must keep its Gamma outcome_index 0 — NOT overwritten by the diverging CLOB order"
    );
    assert_eq!(
        cache.token_condition_outcome("T1"),
        Some(("0xdiverge".to_owned(), Some(1))),
        "T1 must keep its Gamma outcome_index 1 — NOT overwritten to 0"
    );

    // 0xfresh: no prior map, consistent → written with correct positional index.
    assert_eq!(
        report.tokens_mapped, 2,
        "only the consistent market's two tokens map; the quarantined market's are skipped"
    );
    assert_eq!(
        cache.token_condition_outcome("F0"),
        Some(("0xfresh".to_owned(), Some(0)))
    );
    assert_eq!(
        cache.token_condition_outcome("F1"),
        Some(("0xfresh".to_owned(), Some(1)))
    );

    println!(
        "PASS: CLOB token-order divergence quarantined (rows preserved, order_mismatches=1); \
         consistent market mapped (tokens_mapped=2)"
    );
}
