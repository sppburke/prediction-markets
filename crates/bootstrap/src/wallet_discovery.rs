//! Source-agnostic winner-discovery dispatch (issue #324).
//!
//! [`WalletDiscoverySource`] enumerates discovery sources.
//! The binary acquires the cache mutation lock centrally before its read-write
//! open (#544); discovery must not nest another acquisition.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use pe_source_polymarket_public::{RECONCILIATION_RATE_LIMIT_RETRY_SECS, ReqwestFetcher};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::cache::WalletCache;
use crate::config::BootstrapConfig;
use crate::datadash_discovery::{
    DatadashDiscoveryReport, ReqwestCohortFetcher, run_datadash_discovery_with_policy,
};
use crate::error::BootstrapError;
use crate::leaderboard_discovery::{
    LeaderboardDiscoveryReport, LeaderboardFetcher, run_leaderboard_discovery_with_policy,
};
use crate::market_scope::{MarketScope, ScopeFetcher, ScopePage, recent_market_scope};
use crate::pile::{self, ActivationPolicy, SRC_MARKET_PARTICIPANT};

const PARTICIPANT_SOURCE_ID: &str = "polymarket-data-api-trades";
const PARTICIPANT_MIN_INTERVAL_MS: u64 = 500;
const PARTICIPANT_MARKET_CHUNK: usize = 40;
const PARTICIPANT_PAGE_LIMIT: usize = 10_000;

/// The supported wallet-discovery sources for `winner-discovery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletDiscoverySource {
    Leaderboard,
    Datadash,
    MarketParticipants,
}

/// Aggregate per-source counts returned by [`run_source_discovery`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SourceDiscoveryResult {
    pub unique_wallets: usize,
    pub activated: usize,
    pub status: &'static str,
}

/// Run discovery for `source`. Production callers already hold the central
/// cache mutation lock for the lifetime of `cache` (#544).
pub async fn run_source_discovery(
    source: WalletDiscoverySource,
    config: &BootstrapConfig,
    cache: &mut WalletCache,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    run_source_discovery_with_policy(source, config, cache, ActivationPolicy::Immediate).await
}

/// Source discovery with an explicit activation policy. The rank-and-push
/// pipeline uses `Deferred`; standalone callers retain `Immediate`.
pub async fn run_source_discovery_with_policy(
    source: WalletDiscoverySource,
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    activation_policy: ActivationPolicy,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    match source {
        WalletDiscoverySource::Leaderboard => {
            if config.leaderboard_top_n == 0 {
                return Ok(SourceDiscoveryResult::default());
            }
            let base_url = config
                .leaderboard_base_url
                .as_deref()
                .unwrap_or(&config.polymarket_base_url)
                .to_owned();
            let client = reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(15))
                .build()
                .map_err(|_| BootstrapError::Internal)?;
            let fetcher = LeaderboardFetcher::new(
                base_url,
                ReqwestFetcher::new(client)
                    .with_min_interval_ms(config.leaderboard_request_interval_ms),
            );
            let r: LeaderboardDiscoveryReport = run_leaderboard_discovery_with_policy(
                &fetcher,
                &config.leaderboard_categories,
                config.leaderboard_top_n,
                cache,
                activation_policy,
            )
            .await?;
            Ok(SourceDiscoveryResult {
                unique_wallets: r.unique_wallets,
                activated: r.activated,
                ..SourceDiscoveryResult::default()
            })
        }
        WalletDiscoverySource::Datadash => {
            // Datadash is on by default; an empty/unset URL disables it.
            let Some(base_url) = config.datadash_api_url.as_deref() else {
                tracing::debug!("wallet_discovery: Datadash skipped — datadash_api_url not set");
                return Ok(SourceDiscoveryResult::default());
            };
            // Network-facing failures in this arm map to
            // `BootstrapError::Datadash` (client build, lock acquire, and every
            // fetch/parse/empty path inside `run_datadash_discovery`) so the
            // caller's soft-fail (`winner_discovery`) catches every datadash
            // *outage*. A genuine DB failure (upsert/activation) still propagates
            // as its native `Sqlite`/`Cache` variant (fatal) — see that
            // function's docs.
            let client = reqwest::Client::builder()
                .pool_idle_timeout(Duration::from_secs(15))
                .build()
                .map_err(|e| BootstrapError::Datadash {
                    message: format!("client build: {e}"),
                })?;
            let fetcher = ReqwestCohortFetcher::new(
                base_url.to_owned(),
                client,
                config.datadash_request_interval_ms,
            );
            let r: DatadashDiscoveryReport = run_datadash_discovery_with_policy(
                &fetcher,
                &config.datadash_exclude_ids,
                &config.datadash_exclude_titles,
                config.datadash_max_cohort_wallets,
                cache,
                activation_policy,
            )
            .await?;
            Ok(SourceDiscoveryResult {
                unique_wallets: r.unique_wallets,
                activated: r.activated,
                ..SourceDiscoveryResult::default()
            })
        }
        WalletDiscoverySource::MarketParticipants => {
            let now = OffsetDateTime::now_utc().unix_timestamp();
            run_participant_discovery(
                config,
                cache,
                activation_policy,
                now,
                participant_fetcher,
                |lo| recent_market_scope(config, lo, &config.market_recurrences, None),
            )
            .await
        }
    }
}

struct ParticipantRecord {
    source_id: &'static str,
    schema_version: u32,
    parser_version: u32,
    window_start_unix: Option<i64>,
    window_end_unix: i64,
    market_scope: Option<MarketScope>,
    trades_pages: Vec<ScopePage>,
    wallet_count: usize,
    status: &'static str,
}

async fn run_participant_discovery<F, S, Fut>(
    config: &BootstrapConfig,
    cache: &mut WalletCache,
    policy: ActivationPolicy,
    now: i64,
    make_fetcher: impl FnOnce() -> Result<F, BootstrapError>,
    scope: S,
) -> Result<SourceDiscoveryResult, BootstrapError>
where
    F: ScopeFetcher,
    S: FnOnce(i64) -> Fut,
    Fut: Future<Output = Result<MarketScope, BootstrapError>>,
{
    let lo = i64::try_from(config.participant_lookback_secs)
        .ok()
        .and_then(|lookback| now.checked_sub(lookback));
    let mut record = ParticipantRecord {
        source_id: PARTICIPANT_SOURCE_ID,
        schema_version: 1,
        parser_version: 1,
        window_start_unix: lo,
        window_end_unix: now,
        market_scope: None,
        trades_pages: Vec::new(),
        wallet_count: 0,
        status: "disabled",
    };
    let collected = async {
        if config.participant_lookback_secs == 0 {
            return Ok(Vec::new());
        }
        let lo = lo.ok_or("failed:window")?;
        let market_scope = scope(lo)
            .await
            .map_err(|error| participant_failure("failed:gamma", &error))?;
        record.market_scope = Some(market_scope);
        let fetcher =
            make_fetcher().map_err(|error| participant_failure("failed:client", &error))?;
        let wallets = collect_participant_trades(
            &fetcher,
            &config.polymarket_base_url,
            record.market_scope.as_ref().ok_or("failed:gamma")?,
            lo,
            now,
            &mut record.trades_pages,
        )
        .await?;
        record.status = "ok";
        Ok(wallets)
    }
    .await;
    let wallets = match collected {
        Ok(wallets) => wallets,
        Err(status) => {
            tracing::warn!(status, "winner_discovery: participant collection stopped");
            record.status = status;
            Vec::new()
        }
    };
    record.wallet_count = wallets.len();
    // One summary line plus one line per page: the whole record measured 179 KB, past journald's
    // 48 KB LineMax. Each trades page URL lists its condition ids; the scope digest covers them all.
    let scope = record.market_scope.as_ref();
    let summary = serde_json::json!({
        "source_id": record.source_id,
        "schema_version": record.schema_version,
        "parser_version": record.parser_version,
        "window_start_unix": record.window_start_unix,
        "window_end_unix": record.window_end_unix,
        "status": record.status,
        "wallet_count": record.wallet_count,
        "scope": scope.map(|s| serde_json::json!({
            "version": s.version,
            "source_id": s.source_id,
            "schema_version": s.schema_version,
            "parser_version": s.parser_version,
            "recurrence": s.recurrence,
            "end_date_min_unix": s.end_date_min_unix,
            "started_at_unix": s.started_at_unix,
            "completed_at_unix": s.completed_at_unix,
            "series": s.series,
            "count": s.count,
            "digest": s.digest,
        })),
        "gamma_pages": scope.map_or(0, |s| s.pages.len()),
        "trades_pages": record.trades_pages.len(),
    });
    tracing::info!(record = %summary, "winner_discovery: participants");
    for page in scope
        .into_iter()
        .flat_map(|s| &s.pages)
        .chain(&record.trades_pages)
    {
        tracing::info!(page = %serde_json::to_string(page)?, "winner_discovery: participant page");
    }
    if record.status != "ok" {
        return Ok(SourceDiscoveryResult {
            status: record.status,
            ..SourceDiscoveryResult::default()
        });
    }
    let rows: Vec<_> = wallets
        .into_iter()
        .map(|hex| (hex, SRC_MARKET_PARTICIPANT, false, None, None, None, 0))
        .collect();
    cache.upsert_wallets_bulk(&rows)?;
    let activated = pile::apply_activation_policy(cache, policy)?;
    Ok(SourceDiscoveryResult {
        unique_wallets: record.wallet_count,
        activated,
        status: record.status,
    })
}

fn participant_fetcher() -> Result<ReqwestFetcher, BootstrapError> {
    let client = reqwest::Client::builder()
        .pool_idle_timeout(Duration::from_secs(15))
        .build()
        .map_err(|_| BootstrapError::Internal)?;
    // A venue `Retry-After: 1` answer to a `/trades` page is waited out in-line rather than
    // ending the source for the whole cycle (#800).
    Ok(ReqwestFetcher::new(client)
        .with_min_interval_ms(PARTICIPANT_MIN_INTERVAL_MS)
        .with_rate_limit_retry_max_secs(RECONCILIATION_RATE_LIMIT_RETRY_SECS))
}

fn participant_failure(reason: &'static str, error: &impl std::fmt::Display) -> &'static str {
    tracing::warn!(error = %error, status = reason, "winner_discovery: participant source failed");
    reason
}

async fn collect_participant_trades<F: ScopeFetcher>(
    fetcher: &F,
    base_url: &str,
    scope: &MarketScope,
    lo: i64,
    hi: i64,
    pages: &mut Vec<ScopePage>,
) -> Result<Vec<String>, &'static str> {
    let mut wallets = BTreeSet::new();
    for ids in scope.condition_ids.chunks(PARTICIPANT_MARKET_CHUNK) {
        let markets = ids.join(",");
        let mut windows = vec![(lo, hi)];
        while let Some((lo, hi)) = windows.pop() {
            let mut window_wallets = BTreeSet::new();
            let mut complete = false;
            for offset in [0, PARTICIPANT_PAGE_LIMIT] {
                let url = format!(
                    "{base_url}/trades?market={markets}&limit={PARTICIPANT_PAGE_LIMIT}&offset={offset}&start={lo}&end={}",
                    hi - 1
                );
                let page = fetcher
                    .fetch(&url, "trades", PARTICIPANT_SOURCE_ID)
                    .await
                    .map_err(|error| participant_failure("failed:trades", &error))?;
                let parsed = serde_json::from_slice::<Vec<Value>>(&page.body);
                let rows = parsed.as_ref().map_or(0, Vec::len);
                pages.push(ScopePage {
                    ordinal: u64::try_from(pages.len()).map_err(|_| "failed:page-count")?,
                    kind: "trades",
                    series_id: None,
                    url,
                    request_cursor: None,
                    returned_next_cursor: None,
                    raw_sha256: format!("{:x}", Sha256::digest(&page.body)),
                    rows: u64::try_from(rows).map_err(|_| "failed:page-count")?,
                    observed_at_unix: page.observed_at_unix,
                    received_at_unix: page.received_at_unix,
                    source_at_unix: page.source_at_unix,
                });
                let trades =
                    parsed.map_err(|error| participant_failure("failed:trades-json", &error))?;
                if rows > PARTICIPANT_PAGE_LIMIT {
                    return Err("failed:trades-limit");
                }
                for row in trades {
                    let Some(wallet) = row.get("proxyWallet").and_then(Value::as_str) else {
                        continue;
                    };
                    let wallet = wallet.to_ascii_lowercase();
                    if wallet.len() == 42
                        && wallet.starts_with("0x")
                        && wallet[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
                    {
                        window_wallets.insert(wallet);
                    }
                }
                if rows < PARTICIPANT_PAGE_LIMIT {
                    complete = true;
                    break;
                }
            }
            if complete {
                wallets.extend(window_wallets);
            } else if hi - lo <= 1 {
                return Err("incomplete");
            } else {
                let mid = lo + (hi - lo) / 2;
                windows.push((mid, hi));
                windows.push((lo, mid));
            }
        }
    }
    Ok(wallets.into_iter().collect())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use pe_source_core::SourceError;
    use serde_json::json;

    use crate::market_scope::{FetchedPage, ScopeSeries};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn leaderboard_zero_skips_and_participants_still_run() {
        let (_dir, mut cache) = tmp_cache();
        let config = BootstrapConfig {
            leaderboard_top_n: 0,
            leaderboard_base_url: Some("invalid URL must never be fetched".into()),
            participant_lookback_secs: 5,
            ..BootstrapConfig::default()
        };
        let result = run_source_discovery(WalletDiscoverySource::Leaderboard, &config, &mut cache)
            .await
            .unwrap();
        assert_eq!((result.unique_wallets, result.activated), (0, 0));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        let fetcher = fetcher(|_: &str| Ok(json!([{"proxyWallet": wallet(1)}])));
        let participants = run_participant_discovery(
            &config,
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            |_| async { Ok(scope(1)) },
        )
        .await
        .unwrap();
        assert_eq!(participants.status, "ok");
        assert_eq!(
            (participants.unique_wallets, participants.activated),
            (1, 1)
        );
    }

    /// Datadash is on by default (URL set from the compiled default) but skips
    /// silently with **no HTTP call** when the URL is unset. Mirrors the retired
    /// Radion gate test: an unset URL must yield `Ok(default())`, not a fetch.
    #[tokio::test]
    async fn datadash_skips_without_api_url() {
        let dir = TempDir::new().unwrap();
        let cache_path = dir.path().join("cache.db");
        let mut cache = WalletCache::open(&cache_path).unwrap();
        let config = BootstrapConfig {
            cache_path,
            datadash_api_url: None,
            ..BootstrapConfig::default()
        };

        let r = run_source_discovery(WalletDiscoverySource::Datadash, &config, &mut cache)
            .await
            .unwrap();
        assert_eq!(r.unique_wallets, 0);
        assert_eq!(r.activated, 0);
    }

    const BASE: &str = "http://trades.test";
    const NOW: i64 = 105;

    struct TestFetcher<G> {
        serve: G,
        urls: Arc<Mutex<Vec<String>>>,
    }

    impl<G: Fn(&str) -> Result<Value, SourceError> + Sync> ScopeFetcher for TestFetcher<G> {
        async fn fetch(
            &self,
            url: &str,
            endpoint_kind: &'static str,
            source_id: &'static str,
        ) -> Result<FetchedPage, SourceError> {
            assert_eq!(endpoint_kind, "trades");
            assert_eq!(source_id, PARTICIPANT_SOURCE_ID);
            self.urls.lock().unwrap().push(url.to_owned());
            Ok(FetchedPage {
                body: serde_json::to_vec(&(self.serve)(url)?).unwrap(),
                observed_at_unix: 200,
                received_at_unix: 201,
                source_at_unix: Some(199),
            })
        }
    }

    fn fetcher<G>(serve: G) -> TestFetcher<G> {
        TestFetcher {
            serve,
            urls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn query(url: &str) -> BTreeMap<String, String> {
        reqwest::Url::parse(url)
            .unwrap()
            .query_pairs()
            .into_owned()
            .collect()
    }

    fn wallet(n: u8) -> String {
        format!("0x{n:040x}")
    }

    fn scope(markets: usize) -> MarketScope {
        MarketScope {
            version: 1,
            source_id: "polymarket-gamma",
            schema_version: 1,
            parser_version: 1,
            recurrence: "15m".into(),
            end_date_min_unix: 100,
            started_at_unix: NOW,
            completed_at_unix: NOW,
            series: vec![ScopeSeries {
                id: "2".into(),
                slug: None,
            }],
            pages: Vec::new(),
            condition_ids: (0..markets).map(|n| format!("0x{n:064x}")).collect(),
            count: u64::try_from(markets).unwrap(),
            digest: "fixture".into(),
        }
    }

    fn config(lookback: u64) -> BootstrapConfig {
        BootstrapConfig {
            polymarket_base_url: BASE.into(),
            participant_lookback_secs: lookback,
            ..BootstrapConfig::default()
        }
    }

    fn full_page(n: u8, timestamp: i64) -> Value {
        json!(vec![
            json!({"proxyWallet": wallet(n), "timestamp": timestamp});
            PARTICIPANT_PAGE_LIMIT
        ])
    }

    fn tmp_cache() -> (TempDir, WalletCache) {
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        (dir, cache)
    }

    #[tokio::test]
    async fn participants_chunk_ids_normalize_wallets_and_record_pages() {
        let body = json!([
            {"proxyWallet": wallet(0xbb).to_uppercase()},
            {"proxyWallet": wallet(0xaa)},
            {"proxyWallet": wallet(0xbb)},
            {"proxyWallet": "0x123"},
            {"proxyWallet": format!("0x{}", "g".repeat(40))},
            {"proxyWallet": format!("1x{}", "a".repeat(40))},
            {"proxyWallet": "é".repeat(21)},
            {"proxyWallet": null},
            {}
        ]);
        let fetcher = fetcher(|_: &str| Ok(body.clone()));
        let mut pages = Vec::new();
        let wallets = collect_participant_trades(&fetcher, BASE, &scope(81), 100, NOW, &mut pages)
            .await
            .unwrap();
        assert_eq!(wallets, vec![wallet(0xaa), wallet(0xbb)]);
        let urls = fetcher.urls.lock().unwrap();
        assert_eq!(urls.len(), 3);
        for (i, url) in urls.iter().enumerate() {
            let q = query(url);
            assert_eq!(q["market"].split(',').count(), if i == 2 { 1 } else { 40 });
            assert_eq!(q["limit"], "10000");
            assert_eq!(q["offset"], "0");
            assert_eq!(q["start"], "100");
            assert_eq!(q["end"], "104");
            let page = &pages[i];
            assert_eq!(page.ordinal, u64::try_from(i).unwrap());
            assert_eq!(page.url, *url);
            assert_eq!(page.kind, "trades");
            assert_eq!(page.rows, 9);
            assert_eq!(
                page.raw_sha256,
                format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap()))
            );
            assert_eq!(
                (
                    page.observed_at_unix,
                    page.received_at_unix,
                    page.source_at_unix
                ),
                (200, 201, Some(199))
            );
        }
        let ids: Vec<_> = urls
            .iter()
            .flat_map(|url| {
                query(url)["market"]
                    .split(',')
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(ids, scope(81).condition_ids);
    }

    #[tokio::test]
    async fn participants_full_first_page_reads_terminal_offset() {
        let fetcher = fetcher(|url: &str| {
            Ok(if query(url)["offset"] == "0" {
                full_page(1, 100)
            } else {
                json!([{"proxyWallet": wallet(2), "timestamp": 104}])
            })
        });
        let mut pages = Vec::new();
        let wallets = collect_participant_trades(&fetcher, BASE, &scope(1), 100, NOW, &mut pages)
            .await
            .unwrap();
        assert_eq!(wallets, vec![wallet(1), wallet(2)]);
        assert_eq!(pages.len(), 2);
        assert_eq!(query(&pages[1].url)["offset"], "10000");
        assert_eq!(pages[0].rows, 10_000);
        assert_eq!(pages[1].rows, 1);
    }

    #[tokio::test]
    async fn participants_full_terminal_page_splits_exact_half_open_bounds() {
        let fetcher = fetcher(|url: &str| {
            let q = query(url);
            Ok(match (q["start"].as_str(), q["end"].as_str()) {
                ("100", "104") => full_page(99, 102),
                ("100", "101") => json!([{"proxyWallet": wallet(1), "timestamp": 101}]),
                ("102", "104") => json!([{"proxyWallet": wallet(2), "timestamp": 102}]),
                _ => unreachable!("unexpected window {url}"),
            })
        });
        let mut pages = Vec::new();
        let wallets = collect_participant_trades(&fetcher, BASE, &scope(1), 100, NOW, &mut pages)
            .await
            .unwrap();
        assert_eq!(
            wallets,
            vec![wallet(1), wallet(2)],
            "saturated parent rows are discarded"
        );
        let bounds: Vec<_> = pages
            .iter()
            .map(|page| {
                let q = query(&page.url);
                (q["start"].clone(), q["end"].clone(), q["offset"].clone())
            })
            .collect();
        assert_eq!(
            bounds,
            [
                ("100", "104", "0"),
                ("100", "104", "10000"),
                ("100", "101", "0"),
                ("102", "104", "0")
            ]
            .map(|(lo, hi, offset)| (lo.into(), hi.into(), offset.into()))
        );
    }

    #[tokio::test]
    async fn participants_one_second_saturation_is_incomplete_without_upsert() {
        let (_dir, mut cache) = tmp_cache();
        let fetcher = fetcher(|_: &str| Ok(full_page(1, 100)));
        let urls = Arc::clone(&fetcher.urls);
        let r = run_participant_discovery(
            &config(1),
            &mut cache,
            ActivationPolicy::Immediate,
            101,
            || Ok(fetcher),
            |_| async { Ok(scope(1)) },
        )
        .await
        .unwrap();
        assert_eq!(r.status, "incomplete");
        assert_eq!((r.unique_wallets, r.activated), (0, 0));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        assert_eq!(urls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn participants_gamma_failure_is_soft_and_does_not_fetch_or_upsert() {
        let (_dir, mut cache) = tmp_cache();
        let fetcher = fetcher(|_: &str| Ok(json!([])));
        let urls = Arc::clone(&fetcher.urls);
        let r = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            |lo| async move {
                assert_eq!(lo, 100);
                Err(BootstrapError::Gamma {
                    message: "fixture outage".into(),
                })
            },
        )
        .await
        .unwrap();
        assert_eq!(r.status, "failed:gamma");
        assert_eq!((r.unique_wallets, r.activated), (0, 0));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        assert!(urls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn participants_disabled_never_fetches_scope_or_trades() {
        let (_dir, mut cache) = tmp_cache();
        let fetcher = fetcher(|_: &str| Ok(json!([])));
        let built = std::cell::Cell::new(false);
        let r = run_participant_discovery(
            &config(0),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || {
                built.set(true);
                Ok(fetcher)
            },
            |_| async { panic!("disabled must not fetch Gamma") },
        )
        .await
        .unwrap();
        assert_eq!(r.status, "disabled");
        assert!(!built.get());
        assert_eq!((r.unique_wallets, r.activated), (0, 0));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
    }

    #[tokio::test]
    async fn participants_future_markets_without_trades_are_ok() {
        let (_dir, mut cache) = tmp_cache();
        let fetcher = fetcher(|_: &str| Ok(json!([])));
        let urls = Arc::clone(&fetcher.urls);
        let r = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            |_| async { Ok(scope(1)) },
        )
        .await
        .unwrap();
        assert_eq!(r.status, "ok");
        assert_eq!((r.unique_wallets, r.activated), (0, 0));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        assert_eq!(urls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn participants_trades_failures_discard_earlier_chunks() {
        for non_array in [true, false] {
            let (_dir, mut cache) = tmp_cache();
            let fetcher = fetcher(|url: &str| {
                if query(url)["market"].split(',').count() == 40 {
                    Ok(json!([{"proxyWallet": wallet(1)}]))
                } else if non_array {
                    Ok(json!({"error": "not an array"}))
                } else {
                    Err(SourceError::Fatal {
                        message: "HTTP 400".into(),
                    })
                }
            });
            let r = run_participant_discovery(
                &config(5),
                &mut cache,
                ActivationPolicy::Immediate,
                NOW,
                || Ok(fetcher),
                |_| async { Ok(scope(41)) },
            )
            .await
            .unwrap();
            assert!(r.status.starts_with("failed:trades"));
            assert_eq!((r.unique_wallets, r.activated), (0, 0));
            assert_eq!(cache.wallet_pile_size().unwrap(), 0);
        }
    }

    /// The production fetcher waits out the venue's `Retry-After: 1` (#800); the page, its hash
    /// and its times come from the successful reply only.
    #[tokio::test]
    async fn participants_wait_out_a_short_rate_limit_answer() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let ok_body = serde_json::to_vec(&json!([{"proxyWallet": wallet(0xaa)}])).unwrap();
        let replies = [
            (
                "429 Too Many Requests\r\nRetry-After: 1\r\nDate: Mon, 26 Oct 2026 00:00:00 GMT",
                serde_json::to_vec(&json!([{"proxyWallet": wallet(0xcc)}])).unwrap(),
            ),
            (
                "200 OK\r\nDate: Mon, 26 Oct 2026 00:00:05 GMT",
                ok_body.clone(),
            ),
        ];
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&requests);
        // On the test's runtime, so the stub ends with the test even after a single request.
        tokio::spawn(async move {
            for (head, body) in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                }
                let line = String::from_utf8(request).unwrap();
                seen.lock()
                    .unwrap()
                    .push(line.lines().next().unwrap().to_owned());
                let head = format!(
                    "HTTP/1.1 {head}\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(head.as_bytes()).await.unwrap();
                stream.write_all(&body).await.unwrap();
            }
        });
        let mut pages = Vec::new();
        let wallets = collect_participant_trades(
            &participant_fetcher().unwrap(),
            &base,
            &scope(1),
            100,
            NOW,
            &mut pages,
        )
        .await
        .unwrap();
        assert_eq!(wallets, vec![wallet(0xaa)]);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].rows, 1);
        assert_eq!(
            pages[0].raw_sha256,
            format!("{:x}", Sha256::digest(&ok_body))
        );
        assert_eq!(pages[0].source_at_unix, Some(1_792_972_805));
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
        assert!(requests[0].starts_with("GET /trades?market="));
    }

    #[tokio::test]
    async fn participants_rediscovery_tags_existing_wallet_and_honors_policy() {
        for policy in [ActivationPolicy::Immediate, ActivationPolicy::Deferred] {
            let (_dir, mut cache) = tmp_cache();
            cache
                .upsert_wallet(&wallet(1), pile::SRC_TRADES, false, None, None, None)
                .unwrap();
            let fetcher = fetcher(|_: &str| Ok(json!([{"proxyWallet": wallet(1)}])));
            let r = run_participant_discovery(
                &config(5),
                &mut cache,
                policy,
                NOW,
                || Ok(fetcher),
                |_| async { Ok(scope(1)) },
            )
            .await
            .unwrap();
            assert_eq!(r.status, "ok");
            assert_eq!(r.unique_wallets, 1);
            assert_eq!(
                r.activated,
                usize::from(policy == ActivationPolicy::Immediate)
            );
            assert_eq!(
                cache.conn_for_test_source_bits(&wallet(1)),
                pile::SRC_TRADES | SRC_MARKET_PARTICIPANT
            );
            assert_eq!(cache.wallet_pile_size().unwrap(), 1);
        }
    }

    #[tokio::test]
    async fn participants_cache_failures_propagate() {
        let (dir, cache) = tmp_cache();
        drop(cache);
        let mut cache = WalletCache::open_read_only(&dir.path().join("cache.db")).unwrap();
        let fetcher = fetcher(|_: &str| Ok(json!([{"proxyWallet": wallet(1)}])));
        let error = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            |_| async { Ok(scope(1)) },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            BootstrapError::Sqlite(_) | BootstrapError::Cache { .. }
        ));
        assert_eq!(cache.wallet_pile_size().unwrap(), 0);
    }
    #[derive(Clone, Default)]
    struct AuditLog(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for AuditLog {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn participants_audit_is_a_versioned_summary_plus_one_line_per_page() {
        let (_dir, mut cache) = tmp_cache();
        let log = AuditLog::default();
        let writer = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let body = json!([{"proxyWallet": wallet(1)}]);
        let fetcher = fetcher(|_: &str| Ok(body.clone()));
        let mut gamma = scope(1);
        (gamma.version, gamma.schema_version, gamma.parser_version) = (7, 8, 9);
        gamma.pages.push(ScopePage {
            ordinal: 0,
            kind: "events",
            series_id: Some("2".into()),
            url: "http://gamma.test/events/keyset?series_id=2".into(),
            request_cursor: None,
            returned_next_cursor: None,
            raw_sha256: format!("{:x}", Sha256::digest(b"fixture Gamma page")),
            rows: 1,
            observed_at_unix: 110,
            received_at_unix: 111,
            source_at_unix: Some(109),
        });
        let gamma_page = serde_json::to_value(&gamma.pages[0]).unwrap();
        let (digest, count) = (gamma.digest.clone(), gamma.count);
        let versions = (gamma.version, gamma.schema_version, gamma.parser_version);
        run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Deferred,
            NOW,
            || Ok(fetcher),
            |_| async { Ok(gamma) },
        )
        .await
        .unwrap();
        let bytes = log.0.lock().unwrap();
        let lines: Vec<Value> = std::str::from_utf8(&bytes)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect();
        let field = |message: &str, key: &str| -> Vec<Value> {
            lines
                .iter()
                .filter(|line| line["fields"]["message"] == message)
                .map(|line| serde_json::from_str(line["fields"][key].as_str().unwrap()).unwrap())
                .collect()
        };
        let records = field("winner_discovery: participants", "record");
        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record["source_id"], PARTICIPANT_SOURCE_ID);
        assert_eq!(record["schema_version"], 1);
        assert_eq!(record["parser_version"], 1);
        assert_eq!(record["window_start_unix"], 100);
        assert_eq!(record["window_end_unix"], NOW);
        assert_eq!(record["status"], "ok");
        assert_eq!(record["wallet_count"], 1);
        assert_eq!(record["scope"]["digest"], digest);
        assert_eq!(record["scope"]["count"], count);
        assert_eq!(record["scope"]["source_id"], "polymarket-gamma");
        assert_eq!(
            (
                record["scope"]["version"].clone(),
                record["scope"]["schema_version"].clone(),
                record["scope"]["parser_version"].clone()
            ),
            (json!(versions.0), json!(versions.1), json!(versions.2)),
            "Gamma's own versions stay in the summary"
        );
        assert!(
            record["scope"].get("condition_ids").is_none(),
            "ids stay out of the summary"
        );
        assert_eq!(
            (
                record["gamma_pages"].clone(),
                record["trades_pages"].clone()
            ),
            (json!(1), json!(1))
        );
        let pages = field("winner_discovery: participant page", "page");
        assert_eq!(
            pages.len(),
            2,
            "every Gamma and trades page gets its own line"
        );
        assert_eq!(pages[0], gamma_page);
        assert_eq!(pages[1]["kind"], "trades");
        assert_eq!(pages[1]["rows"], 1);
        assert_eq!(
            pages[1]["raw_sha256"],
            format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap()))
        );
        assert_eq!(pages[1]["observed_at_unix"], 200);
        assert_eq!(pages[1]["received_at_unix"], 201);
        assert_eq!(pages[1]["source_at_unix"], 199);
    }

    #[tokio::test]
    async fn participants_activation_errors_propagate_after_upsert() {
        let (_dir, mut cache) = tmp_cache();
        cache
            .raw_conn_for_test()
            .execute_batch(
                "CREATE TRIGGER fail_activation BEFORE UPDATE OF is_active ON wallets \
             BEGIN SELECT RAISE(ABORT, 'fixture activation failure'); END;",
            )
            .unwrap();
        let fetcher = fetcher(|_: &str| Ok(json!([{"proxyWallet": wallet(1)}])));
        let error = run_participant_discovery(
            &config(5),
            &mut cache,
            ActivationPolicy::Immediate,
            NOW,
            || Ok(fetcher),
            |_| async { Ok(scope(1)) },
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("fixture activation failure"));
        assert_eq!(
            cache.conn_for_test_source_bits(&wallet(1)),
            SRC_MARKET_PARTICIPANT
        );
        assert_eq!(cache.active_wallet_count().unwrap(), 0);
    }
}
