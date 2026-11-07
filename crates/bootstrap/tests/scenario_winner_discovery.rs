//! The retained leaderboard source refuses acquisition before building an HTTP client.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used)]

use pe_bootstrap::{BootstrapConfig, cache::WalletCache, winner_discovery::run_winner_discovery};

#[tokio::test]
async fn leaderboard_acquisition_is_retired_without_network() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
    let config = BootstrapConfig {
        cache_path: dir.path().join("cache.db"),
        leaderboard_base_url: Some("invalid URL must never be fetched".into()),
        ..BootstrapConfig::default()
    };
    let error = run_winner_discovery(&config, &mut cache, Some(&dir.path().join("audit.json")))
        .await
        .unwrap_err();
    assert_eq!(error.exit_code(), 1);
    assert!(
        error
            .to_string()
            .contains("retired with Data API v1 (#842)")
    );
    assert!(
        error
            .to_string()
            .contains("PE_BOOTSTRAP_LEADERBOARD_TOP_N=0")
    );
    assert_eq!(cache.wallet_pile_size().unwrap(), 0);
}
