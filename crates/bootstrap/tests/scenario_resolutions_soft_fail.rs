//! Scenario: resolutions per-stage soft-fail (issue #201).
//!
//! PASS: when the optional CLOB and Gamma stages are unreachable (refused port),
//!       `fetch_resolutions_and_schedules` returns `Ok(report)` with those stages
//!       recorded in `report.stages_failed` — it does NOT propagate `Err` and
//!       abort the pipeline.
//! FAIL: the function returns `Err`, or `stages_failed` is empty.
//!
//! Polygon is disabled (`polygon_rpc_url: None`) so the test is deterministic with
//! no live network calls; the soft-fail logic is identical across stages.

#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use pe_bootstrap::cache::WalletCache;
use pe_bootstrap::{BootstrapConfig, fetch_resolutions_and_schedules};
use tempfile::TempDir;

#[tokio::test]
async fn resolutions_soft_fails_unreachable_optional_stages() {
    let dir = TempDir::new().unwrap();
    let cache_path = dir.path().join("cache.db");
    let mut cache = WalletCache::open(&cache_path).unwrap();

    // CLOB + Gamma point at a port with no listener → connection refused.
    // Dune + Polygon disabled (None) so only the two refused stages run.
    let config = BootstrapConfig {
        cache_path,
        clob_base_url: "http://127.0.0.1:1".to_owned(),
        gamma_base_url: "http://127.0.0.1:1".to_owned(),
        polygon_rpc_url: None,
        ..BootstrapConfig::default()
    };

    // Non-empty market set so the Gamma open-markets fetch has work to attempt
    // (fresh cache ⇒ the market is unresolved ⇒ it is an open id).
    let market_ids = vec!["0xmarket0001".to_owned()];

    let report = fetch_resolutions_and_schedules(&config, &mut cache, &market_ids)
        .await
        .expect("soft-fail must return Ok, not propagate Err");

    assert!(
        report.has_failures(),
        "expected optional stages to soft-fail, got none"
    );
    assert!(
        report.stages_failed.contains(&"clob"),
        "clob should soft-fail against a refused port; stages_failed={:?}",
        report.stages_failed
    );
    assert!(
        report.stages_failed.contains(&"gamma"),
        "gamma should soft-fail against a refused port; stages_failed={:?}",
        report.stages_failed
    );
}
