//! Background loop that periodically refreshes the [`LiveWatchlist`] from Supabase
//! (issue #339).
//!
//! Spawned by `main.rs` only when `supabase_url` is set and the interval is `> 0`. The
//! initial wallet set is fetched synchronously at bootstrap, so this loop sleeps *before*
//! its first fetch. Fetch failures are logged and the current watchlist is kept (the live
//! set degrades to "stale", never "empty").

use std::time::Duration;

use tracing::{info, warn};

use crate::live_watchlist::LiveWatchlist;
use crate::supabase_reader;

/// Refresh `live` from Supabase every `interval_secs`, merging additively up to `live_cap`.
///
/// `fetch_limit` bounds the `latest_ranking` query (`?limit=`); `live_cap` bounds the
/// accumulated live set across refreshes (additive, never-evict — see
/// [`LiveWatchlist::apply_refresh`]).
#[allow(clippy::too_many_arguments)]
pub async fn run_supabase_refresh_loop(
    live: LiveWatchlist,
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
    fetch_limit: usize,
    live_cap: usize,
    interval_secs: u64,
) {
    let interval = Duration::from_secs(interval_secs);
    loop {
        tokio::time::sleep(interval).await;
        match supabase_reader::fetch(&client, &base_url, &anon_key, &secret_key, fetch_limit).await
        {
            Ok(fresh) => {
                let fetched = fresh.entries.len();
                let live_total = live.apply_refresh(&fresh, live_cap);
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
