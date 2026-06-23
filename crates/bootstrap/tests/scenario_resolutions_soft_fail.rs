//! Scenario: post-#369 resolution-stage failure modes.
//!
//! After issue #369, CLOB is the **primary, hard-fail** resolution source and
//! the Gamma stages remain soft-fail. Two cases:
//!
//! 1. CLOB unreachable (refused port) ⇒ `fetch_resolutions_and_schedules`
//!    propagates `Err` and aborts the pipeline (hard-fail).
//! 2. CLOB succeeds while Gamma is unreachable ⇒ the function returns `Ok` with
//!    only `"gamma"` recorded in `stages_failed` (soft-fail).
//!
//! Deterministic, no live network: case 1 hits a refused port; case 2 makes the
//! CLOB stage a no-op by persisting the `LTE=` pagination terminator (so
//! `fetch_closed_markets` returns an empty `ClobReport` before issuing any HTTP
//! request), leaving only the refused Gamma stage to soft-fail.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::{BootstrapConfig, fetch_resolutions_and_schedules};
use tempfile::TempDir;

/// PASS: an unreachable CLOB endpoint makes the primary stage hard-fail, so
/// `fetch_resolutions_and_schedules` returns `Err` and aborts the run.
/// FAIL: the function returns `Ok` despite CLOB being unreachable.
#[tokio::test]
async fn resolutions_hard_fail_when_clob_unreachable() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();

    // CLOB points at a port with no listener → connection refused → hard-fail.
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: "http://127.0.0.1:1".to_owned(),
        gamma_base_url: "http://127.0.0.1:1".to_owned(),
        ..BootstrapConfig::default()
    };

    let market_ids = vec!["0xmarket0001".to_owned()];

    let result = fetch_resolutions_and_schedules(&config, &mut cache, &market_ids).await;

    assert!(
        result.is_err(),
        "an unreachable CLOB primary stage must hard-fail (propagate Err), got Ok"
    );
    println!("PASS: CLOB unreachable ⇒ fetch_resolutions_and_schedules returns Err");
}

/// PASS: with CLOB a no-op (cursor at the `LTE=` terminator ⇒ early `Ok`, no
/// HTTP) and Gamma unreachable, the function returns `Ok` with exactly `["gamma"]`
/// in `stages_failed`.
/// FAIL: the function returns `Err`, or `stages_failed` is not exactly `["gamma"]`.
#[tokio::test]
async fn gamma_only_soft_fails_when_clob_is_noop() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();

    // Persist the CLOB pagination terminator so `fetch_closed_markets` returns
    // early with an empty `ClobReport` and issues zero HTTP requests — a
    // deterministic CLOB success without a mock server.
    cache.set_source_cursor("clob_closed", "LTE=").unwrap();

    // Gamma points at a refused port; the CLOB base URL is never contacted.
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: "http://127.0.0.1:1".to_owned(),
        gamma_base_url: "http://127.0.0.1:1".to_owned(),
        ..BootstrapConfig::default()
    };

    // Non-empty market set so the Gamma open-markets fetch has work to attempt
    // (fresh cache ⇒ the market is unresolved ⇒ it is an open id).
    let market_ids = vec!["0xmarket0001".to_owned()];

    let report = fetch_resolutions_and_schedules(&config, &mut cache, &market_ids)
        .await
        .expect("CLOB no-op + Gamma soft-fail must return Ok, not propagate Err");

    assert_eq!(
        report.stages_failed,
        vec!["gamma"],
        "only the refused Gamma stage may soft-fail; CLOB no-ops and the empty \
         null-rewrite / schedule-backfill stages issue no request"
    );
    println!("PASS: CLOB no-op + Gamma refused ⇒ Ok with stages_failed == [\"gamma\"]");
}
