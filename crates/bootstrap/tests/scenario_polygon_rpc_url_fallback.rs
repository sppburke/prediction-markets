//! Scenario: `config::load()` Polygon-RPC URL resolution (issue #188 Item 1).
//!
//! Relocated from `scenario_wallet_enum_followup.rs` in #326 PR4 (deleted with
//! the on-chain enumeration path). These two tests pin the surviving
//! `config::load()` env fallback that feeds the Polygon resolution scan
//! (`polygon_ctf::scan_resolutions`):
//!
//! - `PE_POLYGON_HTTP_URL` (the workspace-shared var) populates `polygon_rpc_url`.
//! - `PE_BOOTSTRAP_POLYGON_RPC_URL` (the bootstrap-specific override) wins when
//!   both are set.

#![cfg(feature = "scenario")]
// figment::Error is ~208 bytes; the Jail closures below return it from a
// `Result` — matches the allow on config.rs's own Jail tests.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::result_large_err)]

use pe_bootstrap::config;

/// PASS: setting only `PE_POLYGON_HTTP_URL` makes `config::load()` produce a
///       `BootstrapConfig` whose `polygon_rpc_url` is `Some(...)`. Deployments
///       use the canonical `.env` (`PE_POLYGON_HTTP_URL`); without this fallback
///       the surviving Polygon resolution scan would never receive an RPC URL.
/// FAIL: the fallback doesn't fire and `polygon_rpc_url` is `None`, OR the
///       env-var binding silently maps to a different field.
#[test]
fn polygon_http_url_env_var_populates_polygon_rpc_url() {
    figment::Jail::expect_with(|jail| {
        jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
        jail.set_env(
            "PE_POLYGON_HTTP_URL",
            "https://alchemy.invalid/v2/shared-key",
        );
        let cfg = config::load(Some(std::path::Path::new("config.toml")))
            .map_err(|e| figment::Error::from(e.to_string()))?;
        assert_eq!(
            cfg.polygon_rpc_url.as_deref(),
            Some("https://alchemy.invalid/v2/shared-key"),
            "PE_POLYGON_HTTP_URL must populate polygon_rpc_url via the load()-time fallback"
        );
        Ok(())
    });
}

/// PASS: `PE_BOOTSTRAP_POLYGON_RPC_URL` takes precedence over `PE_POLYGON_HTTP_URL`
///       even when both are set — the bootstrap-specific override must win.
/// FAIL: the override doesn't fire (the bootstrap-specific knob is silently ignored).
#[test]
fn bootstrap_specific_env_var_overrides_polygon_http_url() {
    figment::Jail::expect_with(|jail| {
        jail.create_file("config.toml", r#"output_path = "/tmp/watchlist.json""#)?;
        jail.set_env(
            "PE_POLYGON_HTTP_URL",
            "https://shared.invalid/v2/shared-key",
        );
        jail.set_env(
            "PE_BOOTSTRAP_POLYGON_RPC_URL",
            "https://bootstrap.invalid/v2/bootstrap-key",
        );
        let cfg = config::load(Some(std::path::Path::new("config.toml")))
            .map_err(|e| figment::Error::from(e.to_string()))?;
        assert_eq!(
            cfg.polygon_rpc_url.as_deref(),
            Some("https://bootstrap.invalid/v2/bootstrap-key"),
            "PE_BOOTSTRAP_POLYGON_RPC_URL must take precedence over PE_POLYGON_HTTP_URL"
        );
        Ok(())
    });
}
