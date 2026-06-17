//! Background loop that periodically refreshes the [`LiveWatchlist`] from Supabase
//! (issue #339) and publishes the resulting live-set size for the analytics site.
//!
//! Spawned by `main.rs` only when `supabase_url` is set and the interval is `> 0`. The
//! initial wallet set is fetched synchronously at bootstrap, so this loop sleeps *before*
//! its first fetch. Fetch failures are logged and the current watchlist is kept (the live
//! set degrades to "stale", never "empty").
//!
//! After each refresh the loop best-effort publishes the live-set size to Supabase's
//! `service_runtime` row (the count lives only in service memory; the site cannot derive it
//! from `latest_ranking` because it does not know `SUPABASE_FETCH_LIMIT`). A publish failure
//! never affects the refresh — the analytics write is strictly downstream of copy decisions.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pe_trader_index::Watchlist;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::live_watchlist::LiveWatchlist;
use crate::supabase_reader::{self, SupabaseError};

/// Publishes the live watchlist size to Supabase so the site can show "N watched".
/// Abstracted as a trait so scenario tests drive [`refresh_and_publish`] with an in-memory
/// fake and inject failures deterministically (no live network). Mirrors `SinkWriter`.
pub trait WatchlistSizePublisher: Send + Sync {
    fn publish(&self, size: usize) -> impl Future<Output = Result<(), SupabaseError>> + Send;
}

/// PostgREST-backed [`WatchlistSizePublisher`]: upserts the single `service_runtime` row
/// (`POST /rest/v1/service_runtime?on_conflict=id`, merge-duplicates). The write needs the
/// service-role secret (anon is read-only under RLS), so the caller only constructs this
/// when a secret key is configured.
pub struct HttpWatchlistPublisher {
    client: reqwest::Client,
    base_url: String,
    /// The single token sent in BOTH headers (see [`supabase_reader::auth_token`]).
    token: String,
}

impl HttpWatchlistPublisher {
    pub fn new(client: reqwest::Client, base_url: &str, anon_key: &str, secret_key: &str) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            token: supabase_reader::auth_token(anon_key, secret_key).to_string(),
        }
    }
}

impl WatchlistSizePublisher for HttpWatchlistPublisher {
    async fn publish(&self, size: usize) -> Result<(), SupabaseError> {
        let url = format!("{}/rest/v1/service_runtime?on_conflict=id", self.base_url);
        let resp = self
            .client
            .post(&url)
            .header("apikey", &self.token)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .header("Prefer", "resolution=merge-duplicates")
            .json(&runtime_upsert_body(size))
            .send()
            .await
            .map_err(SupabaseError::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(SupabaseError::Status(status.as_u16()));
        }
        Ok(())
    }
}

/// Build the PostgREST upsert body for the single `service_runtime` row. Pure (no clock /
/// network) so the published payload is unit- and scenario-testable. `updated_at` is left to
/// the column default on insert; the merge-duplicates upsert refreshes `watchlist_size`.
pub(crate) fn runtime_upsert_body(size: usize) -> serde_json::Value {
    serde_json::json!([{ "id": 1, "watchlist_size": size }])
}

/// Apply a score-update-only refresh of `live` from `fresh` (issue #350 WS1: never adds or
/// evicts — membership is changed only by the maintenance tick) and best-effort publish the
/// resulting live-set size. Returns the live-set size; a publish failure is logged, never
/// propagated — the refresh must not depend on the analytics write.
pub async fn refresh_and_publish(
    live: &LiveWatchlist,
    fresh: &Watchlist,
    publisher: &impl WatchlistSizePublisher,
) -> usize {
    let live_total = live.apply_refresh(fresh);
    if let Err(e) = publisher.publish(live_total).await {
        warn!(error = %e, "failed to publish watchlist size to supabase");
    }
    live_total
}

/// Refresh `live` from Supabase every `interval_secs` (score-update-only — see
/// [`LiveWatchlist::apply_refresh`]) and publish the live-set size after each refresh.
///
/// `fetch_limit` bounds the `latest_ranking` query (`?limit=`). The refresh never changes
/// membership (issue #350 WS1): the maintenance tick is the sole evictor/backfiller.
/// Publishing needs the service-role secret; when it is absent the loop still refreshes but
/// skips the publish.
#[allow(clippy::too_many_arguments)]
pub async fn run_supabase_refresh_loop(
    live: LiveWatchlist,
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
    fetch_limit: usize,
    interval_secs: u64,
    writer_lock: Arc<Mutex<()>>,
) {
    // The watched-count write needs the service-role key (anon is read-only under RLS).
    let publisher = (!secret_key.is_empty())
        .then(|| HttpWatchlistPublisher::new(client.clone(), &base_url, &anon_key, &secret_key));
    if publisher.is_none() {
        info!("watchlist-size publish disabled: no supabase secret key");
    }

    let interval = Duration::from_secs(interval_secs);
    loop {
        tokio::time::sleep(interval).await;
        match supabase_reader::fetch(&client, &base_url, &anon_key, &secret_key, fetch_limit).await
        {
            // Score-update-only refresh: the last-trade side-map (#357) is unused here (the
            // refresh never adds/evicts, so it seeds no cursors); destructure for the tuple.
            Ok((fresh, _last_trade)) => {
                let fetched = fresh.entries.len();
                // Serialize the ArcSwap write against the maintenance tick's `replace`
                // (#350 WS1 PR-D); readers stay lock-free.
                let _writer = writer_lock.lock().await;
                let live_total = match &publisher {
                    Some(p) => refresh_and_publish(&live, &fresh, p).await,
                    None => live.apply_refresh(&fresh),
                };
                info!(
                    fetched,
                    live_total, "live watchlist refreshed from supabase"
                );
            }
            Err(e) => {
                warn!(error = %e, "supabase refresh failed; keeping current watchlist");
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn runtime_upsert_body_is_single_row_keyed_on_id_1() {
        let body = runtime_upsert_body(25);
        // PostgREST upsert bodies are arrays; merge-duplicates keys on the `id=1` singleton.
        let arr = body.as_array().expect("body is a JSON array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], 1);
        assert_eq!(arr[0]["watchlist_size"], 25);
        // `updated_at` is owned by the column default / merge, never sent from Rust.
        assert!(arr[0].get("updated_at").is_none());
    }

    #[test]
    fn runtime_upsert_body_carries_zero_when_no_wallets() {
        let body = runtime_upsert_body(0);
        assert_eq!(body.as_array().unwrap()[0]["watchlist_size"], 0);
    }
}
