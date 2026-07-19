//! datadash.xyz cohort wallet discovery (issue #365).
//!
//! Adds the public datadash.xyz cohort API as a wallet-discovery source
//! alongside the Polymarket leaderboard ([`crate::leaderboard_discovery`]). It
//! ingests the distinct wallet addresses from
//! every datadash cohort *except* the configured exclusions (by exact id or exact
//! title) and any cohort whose advertised `numWallets` exceeds the magnitude cap,
//! adding only wallets not already in `wallet_cache.db` (the bulk upsert OR-merges
//! source bits, so re-adding an existing wallet just tags `SRC_DATADASH`).
//!
//! ## API contract (reverse-engineered, verified live 2026-06-17 — not documented)
//!
//! Unauthenticated Connect-RPC JSON-over-POST. Two methods are used (the exact
//! shape is pinned by the committed fixtures under `tests/fixtures/datadash_*.json`
//! and exercised by `tests/scenario_datadash.rs`):
//!
//! - `POST {base}/cohort.rpc.v1.CohortService/ListCohorts` body
//!   `{"skipExecutionStatus":true}` → `{"cohorts":[{id,title,numWallets,…}]}`,
//!   where `numWallets` is a JSON **string**.
//! - `POST {base}/cohort.rpc.v1.CohortService/ListCohortWallets` body
//!   `{"cohortId":"<id>","page":{"limit":<n>,"offset":<m>}}` →
//!   `{"addresses":["0x… lowercase 42-char", …]}`.
//!
//! ## Compliance (CLAUDE.md CrowdIntel rule)
//!
//! datadash contributes wallet *addresses* only. Cohort labels/scores are never
//! stored — provenance is the [`SRC_DATADASH`] source bit alone. Ingested wallets
//! earn their place via the same independent backfill + ranking as every other
//! wallet; the live-decision gate is the downstream ranker → `export-watchlist` →
//! pe-service path, not this ingest.

use std::sync::Mutex;
use std::time::{Duration, Instant};

#[cfg(any(test, feature = "scenario"))]
use std::collections::HashMap;

use serde::Deserialize;

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::error::BootstrapError;
use crate::pile::{self, SRC_DATADASH};

/// Connect-RPC method path for listing cohorts (relative to the base URL).
const LIST_COHORTS_PATH: &str = "/cohort.rpc.v1.CohortService/ListCohorts";
/// Connect-RPC method path for listing a cohort's wallet addresses.
const LIST_COHORT_WALLETS_PATH: &str = "/cohort.rpc.v1.CohortService/ListCohortWallets";

/// Page size requested per `ListCohortWallets` call. The largest live cohort is
/// 706 wallets (< this cap → one call each); the paging loop is defensive for the
/// case a cohort ever exceeds the limit (verified live 2026-06-17).
const DATADASH_PAGE_LIMIT: u32 = 1000;

/// Per-request timeout for datadash calls.
const DATADASH_REQUEST_TIMEOUT_SECS: u64 = 15;

/// User-Agent sent with datadash requests. datadash advertises no anti-scraping;
/// a descriptive UA is sent defensively (a browser-style UA answered 200 during
/// the reverse-engineering capture).
const DATADASH_USER_AGENT: &str = "prediction-edge/1.0 (+pe-bootstrap winner-discovery)";

/// Cohort metadata from `ListCohorts`. Extra fields in the live payload
/// (`createdBy`, `createdAt`, `isStatic`, `lastExecutionStatusResponse`, …) are
/// ignored — only `id`, `title`, and the wallet magnitude are needed.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CohortMeta {
    /// Stable cohort id (e.g. `07NQHFRAGB6HV`).
    pub id: String,
    /// Human-readable cohort title.
    pub title: String,
    /// Advertised wallet count. The API sends this as a JSON **string** (verified
    /// live 2026-06-17); parsed to `u64` here so it can gate the magnitude cap.
    #[serde(deserialize_with = "deserialize_u64_from_str_or_int")]
    pub num_wallets: u64,
}

/// `ListCohorts` response envelope.
#[derive(Debug, Clone, Deserialize)]
struct ListCohortsResponse {
    #[serde(default)]
    cohorts: Vec<CohortMeta>,
}

/// `ListCohortWallets` response envelope.
#[derive(Debug, Clone, Deserialize)]
struct ListCohortWalletsResponse {
    #[serde(default)]
    addresses: Vec<String>,
}

/// Parse a `ListCohorts` response body into cohort metadata.
///
/// Public so `tests/scenario_datadash.rs` can pin the live contract shape against
/// a committed fixture. Returns [`BootstrapError::Datadash`] on malformed JSON.
pub fn parse_list_cohorts(bytes: &[u8]) -> Result<Vec<CohortMeta>, BootstrapError> {
    let parsed: ListCohortsResponse =
        serde_json::from_slice(bytes).map_err(|e| BootstrapError::Datadash {
            message: format!("ListCohorts JSON: {e}"),
        })?;
    Ok(parsed.cohorts)
}

/// Parse a `ListCohortWallets` response body into the address list.
///
/// Public so `tests/scenario_datadash.rs` can pin the `addresses[]` field name
/// against a committed fixture. Returns [`BootstrapError::Datadash`] on malformed
/// JSON.
pub fn parse_list_cohort_wallets(bytes: &[u8]) -> Result<Vec<String>, BootstrapError> {
    let parsed: ListCohortWalletsResponse =
        serde_json::from_slice(bytes).map_err(|e| BootstrapError::Datadash {
            message: format!("ListCohortWallets JSON: {e}"),
        })?;
    Ok(parsed.addresses)
}

/// Abstracts the two datadash Connect-RPC calls so production and tests share the
/// discovery logic. `&self` (not `&mut self`) so a single fetcher can be reused
/// across calls; implementations use interior mutability for any per-call state
/// (the rate-limit clock).
#[allow(async_fn_in_trait)]
pub trait CohortFetcher {
    /// List all cohorts (`ListCohorts`).
    async fn list_cohorts(&self) -> Result<Vec<CohortMeta>, BootstrapError>;

    /// List one page of wallet addresses for `cohort_id` (`ListCohortWallets`).
    async fn list_cohort_wallets(
        &self,
        cohort_id: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<String>, BootstrapError>;
}

/// Production [`CohortFetcher`] backed by a [`reqwest::Client`].
///
/// Posts Connect-RPC JSON bodies and enforces a minimum inter-request interval
/// (`datadash_request_interval_ms`) via a shared clock, mirroring
/// `ReqwestFetcher`'s rate-limit gate in `source-polymarket-public`.
pub struct ReqwestCohortFetcher {
    base_url: String,
    client: reqwest::Client,
    timeout: Duration,
    min_interval_ms: u64,
    /// Shared rate-limit clock: serializes the gate across concurrent callers.
    last_request_at: Mutex<Option<Instant>>,
}

impl ReqwestCohortFetcher {
    /// Wrap an existing `reqwest::Client` for the datadash base URL.
    pub fn new(base_url: String, client: reqwest::Client, min_interval_ms: u64) -> Self {
        Self {
            base_url,
            client,
            timeout: Duration::from_secs(DATADASH_REQUEST_TIMEOUT_SECS),
            min_interval_ms,
            last_request_at: Mutex::new(None),
        }
    }

    /// Rate-limit gate: claim the next available slot and sleep until it, so
    /// wall-clock throughput stays at ≤ 1 / `min_interval_ms` even when callers
    /// share this fetcher. Mirrors `ReqwestFetcher::fetch_page`.
    async fn rate_limit_gate(&self) {
        let min_interval = Duration::from_millis(self.min_interval_ms);
        let sleep_for = {
            let mut guard = self
                .last_request_at
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let now = Instant::now();
            let next_slot = match *guard {
                None => now,
                Some(last) => last.max(now) + min_interval,
            };
            *guard = Some(next_slot);
            next_slot.checked_duration_since(now)
        };
        if let Some(d) = sleep_for {
            tokio::time::sleep(d).await;
        }
    }

    /// POST `body` (raw JSON bytes) to `path` and return the response bytes.
    /// Every failure maps to [`BootstrapError::Datadash`] so the caller's
    /// soft-fail catches all datadash network/HTTP errors.
    async fn post(&self, path: &str, body: Vec<u8>) -> Result<Vec<u8>, BootstrapError> {
        self.rate_limit_gate().await;
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .client
            .post(&url)
            .timeout(self.timeout)
            .header("content-type", "application/json")
            .header("user-agent", DATADASH_USER_AGENT)
            .body(body)
            .send()
            .await
            .map_err(|e| BootstrapError::Datadash {
                message: format!("POST {path}: {e}"),
            })?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| BootstrapError::Datadash {
            message: format!("read body {path}: {e}"),
        })?;
        if !status.is_success() {
            return Err(BootstrapError::Datadash {
                message: format!("{path} returned HTTP {status}"),
            });
        }
        Ok(bytes.to_vec())
    }
}

impl CohortFetcher for ReqwestCohortFetcher {
    async fn list_cohorts(&self) -> Result<Vec<CohortMeta>, BootstrapError> {
        let body = serde_json::to_vec(&serde_json::json!({ "skipExecutionStatus": true }))
            .map_err(|e| BootstrapError::Datadash {
                message: format!("serialize ListCohorts request: {e}"),
            })?;
        let bytes = self.post(LIST_COHORTS_PATH, body).await?;
        parse_list_cohorts(&bytes)
    }

    async fn list_cohort_wallets(
        &self,
        cohort_id: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<String>, BootstrapError> {
        let body = serde_json::to_vec(&serde_json::json!({
            "cohortId": cohort_id,
            "page": { "limit": limit, "offset": offset },
        }))
        .map_err(|e| BootstrapError::Datadash {
            message: format!("serialize ListCohortWallets request: {e}"),
        })?;
        let bytes = self.post(LIST_COHORT_WALLETS_PATH, body).await?;
        parse_list_cohort_wallets(&bytes)
    }
}

/// Per-run discovery counts.
#[derive(Debug, Default, Clone, Copy)]
pub struct DatadashDiscoveryReport {
    /// Cohorts returned by `ListCohorts`.
    pub cohorts_listed: usize,
    /// Cohorts that passed exclusion + magnitude filters (attempted for ingest).
    pub cohorts_kept: usize,
    /// Valid addresses collected across kept cohorts, before dedup.
    pub raw_addresses: usize,
    /// Distinct wallets upserted with `SRC_DATADASH`.
    pub unique_wallets: usize,
    /// Wallets newly activated by [`pile::apply_activation_rules`].
    pub activated: usize,
}

/// Returns `true` if `meta` matches any exclusion by **exact** id or **exact**
/// title (never substring). The default config drops
/// `Polymarket Twitter/X Linked Traders` / `07NQHFRAGB6HV` while keeping the
/// distinct `Polymarket Twitter/X Linked with PnL >$100k` cohort.
fn cohort_is_excluded(
    meta: &CohortMeta,
    exclude_ids: &[String],
    exclude_titles: &[String],
) -> bool {
    exclude_ids.iter().any(|x| x == &meta.id) || exclude_titles.iter().any(|x| x == &meta.title)
}

/// Sweep all datadash cohorts, dropping the configured exclusions and any cohort
/// over the magnitude cap, deduplicate addresses across the kept cohorts, upsert
/// with [`SRC_DATADASH`], and apply activation rules.
///
/// Resilience contract:
/// - A single cohort's `ListCohortWallets` failure is `warn!`-logged and skipped;
///   the sweep still succeeds from the cohorts that returned wallets (AC6).
/// - Returns [`BootstrapError::Datadash`] if `ListCohorts` fails, or if every
///   kept cohort yields zero valid addresses — never a silent empty upsert (AC5).
///   The network/parse/empty paths all return the `Datadash` variant so the
///   caller's soft-fail ([`crate::winner_discovery`]) catches every datadash
///   *outage*. DB errors from the shared cache (`upsert_wallets_bulk`,
///   `apply_activation_rules`) propagate as their native `Sqlite`/`Cache` variant
///   (fatal, matching `leaderboard_discovery`) — a broken cache is not a datadash
///   outage and must halt the run.
///
/// # Precondition
/// `cache` must be open and writable; the caller holds the `CacheMutationLock`.
pub async fn run_datadash_discovery<F: CohortFetcher + Send + Sync>(
    fetcher: &F,
    exclude_ids: &[String],
    exclude_titles: &[String],
    max_cohort_wallets: u64,
    cache: &mut WalletCache,
) -> Result<DatadashDiscoveryReport, BootstrapError> {
    run_datadash_discovery_with_policy(
        fetcher,
        exclude_ids,
        exclude_titles,
        max_cohort_wallets,
        cache,
        pile::ActivationPolicy::Immediate,
    )
    .await
}

/// Discovery variant used by the full pipeline to defer global activation to
/// its single controlled batch.
pub async fn run_datadash_discovery_with_policy<F: CohortFetcher + Send + Sync>(
    fetcher: &F,
    exclude_ids: &[String],
    exclude_titles: &[String],
    max_cohort_wallets: u64,
    cache: &mut WalletCache,
    activation_policy: pile::ActivationPolicy,
) -> Result<DatadashDiscoveryReport, BootstrapError> {
    let cohorts = fetcher.list_cohorts().await?;
    let cohorts_listed = cohorts.len();

    let mut all: Vec<String> = Vec::new();
    let mut cohorts_kept = 0usize;

    for cohort in &cohorts {
        if cohort_is_excluded(cohort, exclude_ids, exclude_titles) {
            tracing::debug!(id = %cohort.id, title = %cohort.title, "datadash: cohort excluded");
            continue;
        }
        if cohort.num_wallets > max_cohort_wallets {
            tracing::warn!(
                id = %cohort.id,
                title = %cohort.title,
                num_wallets = cohort.num_wallets,
                max_cohort_wallets,
                "datadash: cohort over magnitude cap — skipping before fetch"
            );
            continue;
        }
        cohorts_kept += 1;

        // Page through the cohort defensively. Every live cohort is < the page
        // limit (one call each); the loop only iterates if a cohort ever exceeds
        // `DATADASH_PAGE_LIMIT`.
        let mut offset = 0u32;
        loop {
            match fetcher
                .list_cohort_wallets(&cohort.id, DATADASH_PAGE_LIMIT, offset)
                .await
            {
                Ok(page) => {
                    let page_len = page.len();
                    for addr in page {
                        let hex = addr.to_ascii_lowercase();
                        if hex.starts_with("0x") && hex.len() == 42 {
                            all.push(hex);
                        }
                    }
                    // Last page reached when the API returns fewer than a full
                    // page. (`DATADASH_PAGE_LIMIT as usize` is a widening cast.)
                    if page_len < DATADASH_PAGE_LIMIT as usize {
                        break;
                    }
                    offset = offset.saturating_add(DATADASH_PAGE_LIMIT);
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        id = %cohort.id,
                        title = %cohort.title,
                        "datadash: cohort wallet fetch failed — skipping cohort, continuing sweep"
                    );
                    break;
                }
            }
        }
    }

    let raw_addresses = all.len();
    if raw_addresses == 0 {
        return Err(BootstrapError::Datadash {
            message: format!(
                "all {cohorts_kept} kept cohort(s) (of {cohorts_listed} listed) returned zero valid wallet addresses — refusing empty upsert"
            ),
        });
    }

    all.sort_unstable();
    all.dedup();
    let unique_wallets = all.len();

    let upserts: Vec<WalletUpsertRow> = all
        .into_iter()
        .map(|hex| (hex, SRC_DATADASH, false, None, None, None, 0))
        .collect();
    cache.upsert_wallets_bulk(&upserts)?;

    let activated = pile::apply_activation_policy(cache, activation_policy)?;

    tracing::info!(
        cohorts_listed,
        cohorts_kept,
        raw_addresses,
        unique_wallets,
        activated,
        "datadash_discovery: complete"
    );
    Ok(DatadashDiscoveryReport {
        cohorts_listed,
        cohorts_kept,
        raw_addresses,
        unique_wallets,
        activated,
    })
}

/// Deserialize a `u64` from either a JSON integer or a decimal string. datadash
/// sends `numWallets` as a string; this tolerates an integer too in case the
/// contract ever changes.
fn deserialize_u64_from_str_or_int<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{self, Visitor};

    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = u64;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a u64 as a JSON integer or decimal string")
        }

        fn visit_u64<E: de::Error>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }

        fn visit_i64<E: de::Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| de::Error::custom(format!("{v} is negative")))
        }

        fn visit_str<E: de::Error>(self, v: &str) -> Result<u64, E> {
            v.trim()
                .parse::<u64>()
                .map_err(|_| de::Error::custom(format!("'{v}' is not a valid u64")))
        }
    }

    d.deserialize_any(V)
}

// ── In-memory fetcher for deterministic tests ───────────────────────────────────
//
// Gated on `cfg(test)` for the inline unit tests and `feature = "scenario"` for
// the `tests/scenario_datadash.rs` integration test (which compiles the lib as a
// normal dependency, so `cfg(test)` is not active there).

/// In-memory [`CohortFetcher`] for deterministic tests. Built from a cohort list
/// plus an `id → addresses` map; honors `limit`/`offset` so the paging loop is
/// exercised. A cohort id absent from the map yields an `Err(Datadash)` (models a
/// per-cohort fetch failure for the resilience test).
#[cfg(any(test, feature = "scenario"))]
pub struct FixtureCohortFetcher {
    cohorts: Vec<CohortMeta>,
    wallets: HashMap<String, Vec<String>>,
}

#[cfg(any(test, feature = "scenario"))]
impl FixtureCohortFetcher {
    /// Build a fixture fetcher from cohort metadata and an `id → addresses` map.
    pub fn new(cohorts: Vec<CohortMeta>, wallets: HashMap<String, Vec<String>>) -> Self {
        Self { cohorts, wallets }
    }
}

#[cfg(any(test, feature = "scenario"))]
impl CohortFetcher for FixtureCohortFetcher {
    async fn list_cohorts(&self) -> Result<Vec<CohortMeta>, BootstrapError> {
        Ok(self.cohorts.clone())
    }

    async fn list_cohort_wallets(
        &self,
        cohort_id: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<String>, BootstrapError> {
        let Some(all) = self.wallets.get(cohort_id) else {
            return Err(BootstrapError::Datadash {
                message: format!("fixture: no wallets registered for cohort {cohort_id}"),
            });
        };
        // `as usize` widening casts (u32 → usize): safe on all supported targets.
        let start = (offset as usize).min(all.len());
        let end = start.saturating_add(limit as usize).min(all.len());
        Ok(all[start..end].to_vec())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const EXCLUDE_ID: &str = "07NQHFRAGB6HV";
    const EXCLUDE_TITLE: &str = "Polymarket Twitter/X Linked Traders";

    fn tmp_cache() -> (TempDir, WalletCache) {
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        (dir, cache)
    }

    fn meta(id: &str, title: &str, num_wallets: u64) -> CohortMeta {
        CohortMeta {
            id: id.to_owned(),
            title: title.to_owned(),
            num_wallets,
        }
    }

    /// `n` distinct valid lowercase addresses, offset by `base` so cohorts don't
    /// collide (`base * 1_000_000 + i`).
    fn addrs(base: u32, n: u32) -> Vec<String> {
        (0..n)
            .map(|i| format!("0x{:040x}", base * 1_000_000 + i))
            .collect()
    }

    fn default_excludes() -> (Vec<String>, Vec<String>) {
        (vec![EXCLUDE_ID.to_owned()], vec![EXCLUDE_TITLE.to_owned()])
    }

    /// AC1 — exclusion is exact id OR exact title, never substring; the distinct
    /// PnL>$100k cohort is kept.
    #[tokio::test]
    async fn exclusion_exact_id_or_title_never_substring() {
        let (_d, mut cache) = tmp_cache();
        let (ex_ids, ex_titles) = default_excludes();
        let cohorts = vec![
            meta(EXCLUDE_ID, EXCLUDE_TITLE, 50), // excluded by id AND title (under cap)
            meta(
                "07NRZ3Y60B1VT",
                "Polymarket Twitter/X Linked with PnL >$100k",
                102,
            ), // kept — distinct title
            meta("SUBSTR", "Polymarket Twitter/X Linked Traders Plus", 10), // kept — substring, not exact
            meta("WHALES", "Soccer Whales", 5),                             // kept
        ];
        let mut w = HashMap::new();
        w.insert(EXCLUDE_ID.to_owned(), addrs(1, 50));
        w.insert("07NRZ3Y60B1VT".to_owned(), addrs(2, 4));
        w.insert("SUBSTR".to_owned(), addrs(3, 4));
        w.insert("WHALES".to_owned(), addrs(4, 4));
        let f = FixtureCohortFetcher::new(cohorts, w);

        let r = run_datadash_discovery(&f, &ex_ids, &ex_titles, 10_000, &mut cache)
            .await
            .unwrap();

        let pass = r.cohorts_listed == 4 && r.cohorts_kept == 3 && r.unique_wallets == 12;
        println!(
            "{}: exclusion_exact_id_or_title_never_substring \
             (listed={}, kept={}, unique={})",
            if pass { "PASS" } else { "FAIL" },
            r.cohorts_listed,
            r.cohorts_kept,
            r.unique_wallets,
        );
        assert!(pass, "expected listed=4, kept=3, unique=12; got {r:?}");

        let ingested = cache.wallets_with_source_bit(SRC_DATADASH).unwrap();
        assert!(
            !ingested.contains(&addrs(1, 50)[0]),
            "excluded cohort wallets must not be ingested"
        );
        assert_eq!(ingested.len(), 12);
    }

    /// AC2 — a cohort whose `numWallets` exceeds the cap is skipped before fetch.
    #[tokio::test]
    async fn magnitude_cap_skips_oversized_cohort() {
        let (_d, mut cache) = tmp_cache();
        let cohorts = vec![
            meta("BIG", "Huge Cohort", 20_000), // over cap
            meta("OK", "Small Cohort", 5),
        ];
        let mut w = HashMap::new();
        w.insert("BIG".to_owned(), addrs(1, 30)); // never fetched
        w.insert("OK".to_owned(), addrs(2, 5));
        let f = FixtureCohortFetcher::new(cohorts, w);

        let r = run_datadash_discovery(&f, &[], &[], 10_000, &mut cache)
            .await
            .unwrap();

        let pass = r.cohorts_kept == 1 && r.unique_wallets == 5;
        println!(
            "{}: magnitude_cap_skips_oversized_cohort (kept={}, unique={})",
            if pass { "PASS" } else { "FAIL" },
            r.cohorts_kept,
            r.unique_wallets,
        );
        assert!(pass, "expected kept=1, unique=5; got {r:?}");
        let ingested = cache.wallets_with_source_bit(SRC_DATADASH).unwrap();
        assert!(
            !ingested.contains(&addrs(1, 30)[0]),
            "over-cap cohort wallets must not be ingested"
        );
    }

    /// AC3 — addresses are lowercased; non-`0x`/non-42-char entries are dropped.
    #[tokio::test]
    async fn address_hygiene_lowercases_and_rejects_invalid() {
        let (_d, mut cache) = tmp_cache();
        let cohorts = vec![meta("C", "Cohort", 5)];
        let upper = format!("0x{}", "A".repeat(40)); // 42 chars, uppercase → lowercased
        let valid = format!("0x{:040x}", 7u32);
        let mut w = HashMap::new();
        w.insert(
            "C".to_owned(),
            vec![
                upper.clone(),
                "0xabc".to_owned(),    // too short → dropped
                "deadbeef".to_owned(), // no 0x → dropped
                valid.clone(),
            ],
        );
        let f = FixtureCohortFetcher::new(cohorts, w);

        let r = run_datadash_discovery(&f, &[], &[], 10_000, &mut cache)
            .await
            .unwrap();

        let pass = r.raw_addresses == 2 && r.unique_wallets == 2;
        println!(
            "{}: address_hygiene_lowercases_and_rejects_invalid (raw={}, unique={})",
            if pass { "PASS" } else { "FAIL" },
            r.raw_addresses,
            r.unique_wallets,
        );
        assert!(pass, "expected raw=2, unique=2; got {r:?}");
        let ingested = cache.wallets_with_source_bit(SRC_DATADASH).unwrap();
        assert!(
            ingested.contains(&upper.to_ascii_lowercase()),
            "uppercase address must be stored lowercased"
        );
        assert!(
            !ingested.contains(&upper),
            "raw uppercase must not be stored"
        );
    }

    /// AC4 — a wallet in multiple kept cohorts is upserted once; reruns are
    /// idempotent (source_bits OR-merge, no duplicate rows).
    #[tokio::test]
    async fn cross_cohort_dedup_and_idempotent_rerun() {
        let (_d, mut cache) = tmp_cache();
        let shared = format!("0x{:040x}", 999u32);
        let cohorts = vec![meta("A", "A", 2), meta("B", "B", 2)];
        let mut w = HashMap::new();
        w.insert(
            "A".to_owned(),
            vec![shared.clone(), format!("0x{:040x}", 1u32)],
        );
        w.insert(
            "B".to_owned(),
            vec![shared.clone(), format!("0x{:040x}", 2u32)],
        );
        let f = FixtureCohortFetcher::new(cohorts.clone(), w.clone());

        let r1 = run_datadash_discovery(&f, &[], &[], 10_000, &mut cache)
            .await
            .unwrap();
        let f2 = FixtureCohortFetcher::new(cohorts, w);
        let r2 = run_datadash_discovery(&f2, &[], &[], 10_000, &mut cache)
            .await
            .unwrap();

        let rows = cache.wallets_with_source_bit(SRC_DATADASH).unwrap().len();
        let pass =
            r1.raw_addresses == 4 && r1.unique_wallets == 3 && r2.unique_wallets == 3 && rows == 3;
        println!(
            "{}: cross_cohort_dedup_and_idempotent_rerun \
             (raw={}, unique1={}, unique2={}, rows={})",
            if pass { "PASS" } else { "FAIL" },
            r1.raw_addresses,
            r1.unique_wallets,
            r2.unique_wallets,
            rows,
        );
        assert!(
            pass,
            "expected raw=4, unique=3 both runs, 3 rows; got {r1:?} / {r2:?} / rows={rows}"
        );
    }

    /// AC5 — every kept cohort empty → `Err(Datadash)`, never a silent upsert.
    #[tokio::test]
    async fn all_empty_returns_datadash_err() {
        let (_d, mut cache) = tmp_cache();
        let cohorts = vec![meta("A", "A", 0), meta("B", "B", 0)];
        let mut w = HashMap::new();
        w.insert("A".to_owned(), Vec::new());
        w.insert("B".to_owned(), Vec::new());
        let f = FixtureCohortFetcher::new(cohorts, w);

        let r = run_datadash_discovery(&f, &[], &[], 10_000, &mut cache).await;
        let pass = matches!(r, Err(BootstrapError::Datadash { .. }));
        println!(
            "{}: all_empty_returns_datadash_err (got Err(Datadash)={pass})",
            if pass { "PASS" } else { "FAIL" }
        );
        assert!(pass, "expected Err(Datadash); got {r:?}");
    }

    /// AC5 (variant) — every cohort excluded → `Err(Datadash)`.
    #[tokio::test]
    async fn all_excluded_returns_datadash_err() {
        let (_d, mut cache) = tmp_cache();
        let (ex_ids, ex_titles) = default_excludes();
        let cohorts = vec![meta(EXCLUDE_ID, EXCLUDE_TITLE, 50)];
        let mut w = HashMap::new();
        w.insert(EXCLUDE_ID.to_owned(), addrs(1, 5));
        let f = FixtureCohortFetcher::new(cohorts, w);

        let r = run_datadash_discovery(&f, &ex_ids, &ex_titles, 10_000, &mut cache).await;
        let pass = matches!(r, Err(BootstrapError::Datadash { .. }));
        println!(
            "{}: all_excluded_returns_datadash_err (got Err(Datadash)={pass})",
            if pass { "PASS" } else { "FAIL" }
        );
        assert!(pass, "expected Err(Datadash); got {r:?}");
    }

    /// AC6 — a single cohort's fetch failure is skipped; the sweep still succeeds.
    #[tokio::test]
    async fn per_cohort_failure_is_skipped_sweep_succeeds() {
        let (_d, mut cache) = tmp_cache();
        let cohorts = vec![meta("GOOD", "Good", 3), meta("BAD", "Bad", 3)];
        let mut w = HashMap::new();
        w.insert("GOOD".to_owned(), addrs(1, 3));
        // "BAD" absent from the map → fixture returns Err for it.
        let f = FixtureCohortFetcher::new(cohorts, w);

        let r = run_datadash_discovery(&f, &[], &[], 10_000, &mut cache)
            .await
            .unwrap();

        let pass = r.cohorts_kept == 2 && r.unique_wallets == 3;
        println!(
            "{}: per_cohort_failure_is_skipped_sweep_succeeds (kept={}, unique={})",
            if pass { "PASS" } else { "FAIL" },
            r.cohorts_kept,
            r.unique_wallets,
        );
        assert!(pass, "expected kept=2, unique=3; got {r:?}");
    }

    /// AC7 — datadash wallets bypass the 100-trade activation gate immediately.
    #[tokio::test]
    async fn datadash_wallets_activate_immediately() {
        let (_d, mut cache) = tmp_cache();
        let cohorts = vec![meta("A", "A", 3)];
        let mut w = HashMap::new();
        w.insert("A".to_owned(), addrs(1, 3));
        let f = FixtureCohortFetcher::new(cohorts, w);

        let r = run_datadash_discovery(&f, &[], &[], 10_000, &mut cache)
            .await
            .unwrap();

        let active = cache.active_wallet_count().unwrap();
        let pass = r.activated == 3 && active == 3;
        println!(
            "{}: datadash_wallets_activate_immediately (activated={}, active_count={})",
            if pass { "PASS" } else { "FAIL" },
            r.activated,
            active,
        );
        assert!(
            pass,
            "expected activated=3, active=3; got {r:?}, active={active}"
        );
    }

    /// Paging — a cohort larger than one page is fetched across multiple pages.
    #[tokio::test]
    async fn paging_fetches_all_pages() {
        let (_d, mut cache) = tmp_cache();
        let big: Vec<String> = (0..1500u32).map(|i| format!("0x{i:040x}")).collect();
        let cohorts = vec![meta("BIG", "Big", 1500)]; // 1500 < cap 10_000
        let mut w = HashMap::new();
        w.insert("BIG".to_owned(), big);
        let f = FixtureCohortFetcher::new(cohorts, w);

        let r = run_datadash_discovery(&f, &[], &[], 10_000, &mut cache)
            .await
            .unwrap();

        let pass = r.unique_wallets == 1500;
        println!(
            "{}: paging_fetches_all_pages (unique={}, expected 1000+500)",
            if pass { "PASS" } else { "FAIL" },
            r.unique_wallets,
        );
        assert!(pass, "expected 1500 across two pages; got {r:?}");
    }

    /// Shape pin — `numWallets` deserializes from a JSON string.
    #[test]
    fn cohort_meta_parses_num_wallets_from_string() {
        let json = br#"{"cohorts":[{"id":"X","title":"T","numWallets":"102871","isStatic":false,"createdBy":"u"}]}"#;
        let cohorts = parse_list_cohorts(json).unwrap();
        assert_eq!(cohorts.len(), 1);
        assert_eq!(cohorts[0].num_wallets, 102_871);
        assert_eq!(cohorts[0].id, "X");
        assert_eq!(cohorts[0].title, "T");
    }

    /// Shape pin — the wallet envelope exposes `addresses[]`.
    #[test]
    fn addresses_parse_from_envelope() {
        let json = br#"{"addresses":["0xabc","0xdef"],"page":{"limit":1000}}"#;
        let a = parse_list_cohort_wallets(json).unwrap();
        assert_eq!(a, vec!["0xabc".to_owned(), "0xdef".to_owned()]);
    }
}
