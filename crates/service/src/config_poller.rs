//! Supabase `service_config` poll loop (issue #398 WS1).
//!
//! Mirrors `supabase_refresh::run_supabase_refresh_loop`: every [`CONFIG_POLL_INTERVAL_SECS`] it
//! fetches the `service_config` KV table, parses it onto the last-known-good snapshot with
//! [`crate::runtime_config::parse_config`] (precedence KV > env > compiled), and publishes the
//! result into the [`LiveRuntimeConfig`] `ArcSwap`. The orchestrator reads one snapshot per event,
//! so an admin edit takes effect within one poll with no restart. Fail-soft: any fetch error keeps
//! the current config (never a default revert).

use std::time::Duration;

use tracing::{info, warn};

use crate::runtime_config::{ConfigRow, LiveRuntimeConfig, parse_config};
use crate::supabase_reader::{SupabaseError, auth_token};

/// Seconds between `service_config` polls. Boot-frozen (the poll cadence cannot govern itself).
/// Canonical default lives in `docs/_GLOSSARY.md`: `config_poll_interval_secs`.
pub const CONFIG_POLL_INTERVAL_SECS: u64 = 60;

/// PostgREST URL that selects every `service_config` row (key, value, value_type).
fn service_config_url(base_url: &str) -> String {
    format!(
        "{}/rest/v1/service_config?select=key,value,value_type",
        base_url.trim_end_matches('/')
    )
}

/// Fetch all `service_config` rows. The same token goes in BOTH the `apikey` and
/// `Authorization: Bearer` headers (see [`auth_token`]); prefer the service-role secret. Public
/// so `main` can do the one-shot boot fetch before spawning the loop.
pub async fn fetch_service_config(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
) -> Result<Vec<ConfigRow>, SupabaseError> {
    let url = service_config_url(base_url);
    let token = auth_token(anon_key, secret_key);
    let resp = client
        .get(&url)
        .header("apikey", token)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .map_err(SupabaseError::Transport)?;
    let status = resp.status();
    if !status.is_success() {
        return Err(SupabaseError::Status(status.as_u16()));
    }
    resp.json().await.map_err(SupabaseError::Decode)
}

/// A source of `service_config` rows. Trait-injected so [`poll_once`] is testable without network.
pub trait ConfigFetcher: Send + Sync {
    fn fetch(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<ConfigRow>, SupabaseError>> + Send;
}

/// Production fetcher: a PostgREST GET against `service_config`.
pub struct SupabaseConfigFetcher {
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
}

impl SupabaseConfigFetcher {
    pub fn new(
        client: reqwest::Client,
        base_url: String,
        anon_key: String,
        secret_key: String,
    ) -> Self {
        Self {
            client,
            base_url,
            anon_key,
            secret_key,
        }
    }
}

impl ConfigFetcher for SupabaseConfigFetcher {
    async fn fetch(&self) -> Result<Vec<ConfigRow>, SupabaseError> {
        fetch_service_config(
            &self.client,
            &self.base_url,
            &self.anon_key,
            &self.secret_key,
        )
        .await
    }
}

/// One poll cycle: fetch → parse onto last-known-good → publish. Fail-soft on any fetch error.
pub async fn poll_once<F: ConfigFetcher>(
    live: &LiveRuntimeConfig,
    fetcher: &F,
    clob_creds_present: bool,
) {
    match fetcher.fetch().await {
        Ok(rows) => {
            let last = live.snapshot();
            let next = parse_config(&rows, &last, clob_creds_present);
            live.store(next);
        }
        Err(e) => warn!(error = %e, "service_config poll failed; keeping last-known-good config"),
    }
}

/// Run the poll loop forever: sleep `interval_secs`, then [`poll_once`]. Sleeps BEFORE the first
/// poll because the initial config is fetched synchronously at boot.
pub async fn run_config_poll_loop<F: ConfigFetcher>(
    live: LiveRuntimeConfig,
    fetcher: F,
    interval_secs: u64,
    clob_creds_present: bool,
) {
    let interval = Duration::from_secs(interval_secs);
    info!(interval_secs, "service_config poll loop started");
    loop {
        tokio::time::sleep(interval).await;
        poll_once(&live, &fetcher, clob_creds_present).await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::config::ServiceConfig;
    use crate::runtime_config::RuntimeConfig;

    fn boot() -> RuntimeConfig {
        RuntimeConfig::from_service_config(&ServiceConfig::default())
    }

    fn cfg_row(key: &str, value: &str) -> ConfigRow {
        ConfigRow {
            key: key.to_string(),
            value: value.to_string(),
            value_type: "text".to_string(),
        }
    }

    struct OkFetcher(Vec<ConfigRow>);
    impl ConfigFetcher for OkFetcher {
        async fn fetch(&self) -> Result<Vec<ConfigRow>, SupabaseError> {
            Ok(self.0.clone())
        }
    }

    struct ErrFetcher;
    impl ConfigFetcher for ErrFetcher {
        async fn fetch(&self) -> Result<Vec<ConfigRow>, SupabaseError> {
            Err(SupabaseError::Status(503))
        }
    }

    #[test]
    fn url_trims_trailing_slash() {
        assert_eq!(
            service_config_url("https://x.supabase.co/"),
            "https://x.supabase.co/rest/v1/service_config?select=key,value,value_type"
        );
    }

    #[tokio::test]
    async fn poll_once_applies_a_valid_edit() {
        let live = LiveRuntimeConfig::new(boot());
        poll_once(
            &live,
            &OkFetcher(vec![cfg_row("max_fill_price", "0.50")]),
            false,
        )
        .await;
        assert_eq!(live.snapshot().max_fill_price, "0.50");
    }

    #[tokio::test]
    async fn poll_once_keeps_last_good_on_fetch_error() {
        let live = LiveRuntimeConfig::new(boot());
        let before = live.snapshot().max_fill_price.clone();
        poll_once(&live, &ErrFetcher, false).await;
        assert_eq!(live.snapshot().max_fill_price, before);
    }
}
