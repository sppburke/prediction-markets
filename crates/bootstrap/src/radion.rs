//! Radion trader-analysis wallet discovery (issue #373).
//!
//! Adds the Radion REST API as a third wallet-discovery source alongside the
//! Polymarket leaderboard ([`crate::leaderboard_discovery`]) and datadash
//! ([`crate::datadash_discovery`]). It walks the `traders/analysis` snapshot list
//! (ordered by `traderScore`, best first) and upserts each distinct Radion-tracked
//! trader — on Polymarket the `traderId` IS the wallet address — with
//! [`SRC_RADION`], activating it immediately so the next `backfill` fetches its
//! trades. The bulk upsert OR-merges source bits, so re-adding an existing wallet
//! just tags `SRC_RADION`; no duplicate rows.
//!
//! ## API contract (validated live against `api.radion.app` 2026-06-18)
//!
//! - `GET {base}/v1/polymarket/traders/analysis?limit=<≤10>[&cursor=<opaque>]`
//!   with header `X-API-Key: <raw key>` (no `Bearer`). Response envelope
//!   `{ "data": [ { "traderId": "0x… 42-char lowercase", … }, … ],
//!      "nextCursor": string|null }`. Cursor pagination: stop when `nextCursor` is
//!   `null`. The shape is pinned by the committed fixtures under
//!   `tests/fixtures/radion_*.json` and exercised by `tests/scenario_radion.rs`.
//! - Quotas are per-account (Free → 50/hr, 300/mo). With ≤10 wallets/request that
//!   is a hard ceiling of ~3,000 wallets/month, so the sweep is **resumable**: the
//!   `nextCursor` is persisted in the `source_cursor` kv table under
//!   [`RADION_CURSOR_KEY`] and each run resumes deeper into the score ranking,
//!   accumulating distinct wallets over time within the quota and resetting when
//!   the sweep exhausts (`nextCursor=null`).
//!
//! ## Failure policy
//!
//! Every network/HTTP/parse failure (incl. `429`) maps to
//! [`BootstrapError::Radion`], which [`crate::winner_discovery`] **soft-fails**
//! (warn + zero counts) so a Radion outage or quota wall never breaks the nightly
//! `discover → backfill → rank` run. The fetcher never sleeps on `Retry-After`:
//! at the monthly wall that value is seconds-until-month-rollover (up to days), so
//! [`run_radion_discovery`] persists the cursor reached and bails, resuming next
//! run instead of blocking the pipeline.
//!
//! ## Compliance (CLAUDE.md CrowdIntel rule)
//!
//! Radion contributes wallet *addresses* only. `traderScore` and the per-trader
//! metrics are never stored — provenance is the [`SRC_RADION`] source bit alone.
//! Ingested wallets earn their place via the same independent backfill + ranking
//! as every other wallet; the live-decision gate is the downstream ranker →
//! `export-watchlist` → pe-service path, not this ingest.

use std::sync::Mutex;
use std::time::{Duration, Instant};

#[cfg(any(test, feature = "scenario"))]
use std::collections::{HashMap, HashSet};
#[cfg(any(test, feature = "scenario"))]
use std::sync::atomic::{AtomicU32, Ordering};

use serde::Deserialize;

use crate::cache::{WalletCache, WalletUpsertRow};
use crate::error::BootstrapError;
use crate::pile::{self, SRC_RADION};
use crate::wallet_discovery::SourceDiscoveryResult;

/// Path of the single enumeration endpoint, relative to the base URL.
const TRADERS_ANALYSIS_PATH: &str = "/v1/polymarket/traders/analysis";

/// Page size requested per call. The API caps `limit` at 10 — not configurable.
const RADION_PAGE_LIMIT: u32 = 10;

/// Per-request timeout for Radion calls.
const RADION_REQUEST_TIMEOUT_SECS: u64 = 15;

/// `source_cursor` key under which the resumable sweep checkpoint is stored. An
/// empty stored value (written on reset) means "start at the top next run".
pub const RADION_CURSOR_KEY: &str = "radion_traders_analysis";

/// One trader-analysis snapshot. Only `traderId` is read; every other field
/// (`venue`, `traderScore`, the ~19 metrics) is ignored — the struct is **not**
/// `deny_unknown_fields`, and no unread field is declared (an unread serde field
/// would trip `dead_code` under the `-D warnings` gate).
#[derive(Debug, Clone, Deserialize)]
pub struct TraderRow {
    /// The Polymarket wallet address. The endpoint is already `/polymarket`-scoped
    /// (so no `venue` filter is needed); the value is validated (`0x` + len 42) and
    /// lowercased before upsert.
    #[serde(rename = "traderId")]
    pub trader_id: String,
}

/// One page of the cursor-paginated `traders/analysis` response.
#[derive(Debug, Clone, Deserialize)]
pub struct CursorPageTraderAnalysis {
    /// Trader snapshots on this page (ordered by `traderScore`, best first).
    #[serde(default)]
    pub data: Vec<TraderRow>,
    /// Opaque cursor for the next page; `null`/absent marks the last page.
    #[serde(rename = "nextCursor", default)]
    pub next_cursor: Option<String>,
}

/// Parse a `traders/analysis` response body.
///
/// Public so `tests/scenario_radion.rs` can pin the live `{data, nextCursor}`
/// shape against a committed fixture. Returns [`BootstrapError::Radion`] on
/// malformed JSON.
pub fn parse_traders_analysis(bytes: &[u8]) -> Result<CursorPageTraderAnalysis, BootstrapError> {
    serde_json::from_slice(bytes).map_err(|e| BootstrapError::Radion {
        message: format!("traders/analysis JSON: {e}"),
    })
}

/// Map a non-2xx HTTP status into [`BootstrapError::Radion`]. A `429` carries its
/// `Retry-After` value in the message for the operator logs; the value is **not**
/// acted on (no sleep) — see the module-level failure policy.
///
/// Pure (no I/O) so the `429` mapping is unit-testable without a network mock.
fn map_http_status_error(status: u16, retry_after: Option<String>) -> BootstrapError {
    let detail = match retry_after {
        Some(ra) if status == 429 => format!(" (retry-after: {ra})"),
        _ => String::new(),
    };
    BootstrapError::Radion {
        message: format!("{TRADERS_ANALYSIS_PATH} returned HTTP {status}{detail}"),
    }
}

/// Abstracts the single Radion enumeration call so production and tests share the
/// discovery logic. `&self` (not `&mut self`) so a single fetcher is reused across
/// pages; implementations use interior mutability for per-call state (the
/// rate-limit clock / the fixture call counter).
#[allow(async_fn_in_trait)]
pub trait TraderFetcher {
    /// Fetch one page of `traders/analysis`, resuming from `cursor` (`None` = top).
    async fn fetch_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<CursorPageTraderAnalysis, BootstrapError>;
}

/// Production [`TraderFetcher`] backed by a [`reqwest::Client`].
///
/// Sends `GET {base}/v1/polymarket/traders/analysis` with the `X-API-Key` header
/// and enforces a minimum inter-request interval (`radion_request_interval_ms`)
/// via a shared clock, mirroring [`crate::datadash_discovery::ReqwestCohortFetcher`].
pub struct ReqwestTraderFetcher {
    base_url: String,
    client: reqwest::Client,
    api_key: String,
    timeout: Duration,
    min_interval_ms: u64,
    /// Shared rate-limit clock: serializes the gate across concurrent callers.
    last_request_at: Mutex<Option<Instant>>,
}

impl ReqwestTraderFetcher {
    /// Wrap an existing `reqwest::Client` for the Radion base URL + API key.
    pub fn new(
        base_url: String,
        client: reqwest::Client,
        api_key: String,
        min_interval_ms: u64,
    ) -> Self {
        Self {
            base_url,
            client,
            api_key,
            timeout: Duration::from_secs(RADION_REQUEST_TIMEOUT_SECS),
            min_interval_ms,
            last_request_at: Mutex::new(None),
        }
    }

    /// Rate-limit gate: claim the next available slot and sleep until it, so
    /// wall-clock throughput stays at ≤ 1 / `min_interval_ms`. The sleep duration
    /// is computed inside the lock and released before the `await` (no lock held
    /// across `.await`). Mirrors `ReqwestCohortFetcher::rate_limit_gate`.
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
}

impl TraderFetcher for ReqwestTraderFetcher {
    async fn fetch_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<CursorPageTraderAnalysis, BootstrapError> {
        self.rate_limit_gate().await;
        let url = format!("{}{TRADERS_ANALYSIS_PATH}", self.base_url);
        let mut query: Vec<(&str, String)> = vec![("limit", RADION_PAGE_LIMIT.to_string())];
        if let Some(c) = cursor {
            query.push(("cursor", c.to_owned()));
        }
        let resp = self
            .client
            .get(&url)
            .timeout(self.timeout)
            .header("x-api-key", &self.api_key)
            .query(&query)
            .send()
            .await
            .map_err(|e| BootstrapError::Radion {
                message: format!("GET {TRADERS_ANALYSIS_PATH}: {e}"),
            })?;
        let status = resp.status();
        if !status.is_success() {
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned);
            return Err(map_http_status_error(status.as_u16(), retry_after));
        }
        let bytes = resp.bytes().await.map_err(|e| BootstrapError::Radion {
            message: format!("read body {TRADERS_ANALYSIS_PATH}: {e}"),
        })?;
        parse_traders_analysis(&bytes)
    }
}

/// Walk `traders/analysis` page-by-page resuming from the persisted cursor, honor
/// a per-run request budget, then upsert the collected wallets with [`SRC_RADION`]
/// and apply activation rules.
///
/// Resumable-sweep contract:
/// - Resumes from the [`RADION_CURSOR_KEY`] checkpoint (empty value → start at top).
/// - On any `fetch_page` error (incl. `429`): persist the cursor reached and return
///   the [`BootstrapError::Radion`] — soft-failed upstream, resumes next run. Never
///   blocks on `Retry-After`. (The partial collection of this run's earlier pages is
///   dropped; it is re-collected idempotently on the next reset-on-exhaustion cycle.)
/// - On a page with `nextCursor=null`: reset the checkpoint to `""` so the next run
///   restarts at the top.
/// - On hitting the budget without exhaustion: persist the cursor for next run.
/// - Returns [`BootstrapError::Radion`] if zero valid wallets were collected — never
///   a silent empty upsert. A clean exhaustion that collected ≥1 wallet is success.
///
/// DB errors from the shared cache (`set_source_cursor`, `upsert_wallets_bulk`,
/// `apply_activation_rules`) propagate as their native `Sqlite`/`Cache` variant
/// (fatal) — a broken cache is not a Radion outage and must halt the run.
///
/// Cursor drift: scores are recomputed between runs, so a persisted keyset cursor
/// may straddle a shifted boundary (minor skips/repeats) — healed by the DB dedup
/// (`wallet_hex` PK + `source_bits` OR-merge) and the periodic reset-on-exhaustion.
///
/// # Precondition
/// `cache` must be open and writable; the caller holds the `CacheMutationLock`.
pub async fn run_radion_discovery<F: TraderFetcher + Send + Sync>(
    fetcher: &F,
    max_requests_per_run: u32,
    cache: &mut WalletCache,
) -> Result<SourceDiscoveryResult, BootstrapError> {
    // Resume point: a prior reset stored `""`, which normalises to "start at top".
    let mut cursor: Option<String> = cache
        .get_source_cursor(RADION_CURSOR_KEY)
        .filter(|s| !s.is_empty());

    let mut collected: Vec<String> = Vec::new();
    let mut requests: u32 = 0;
    let mut exhausted = false;

    while requests < max_requests_per_run {
        requests += 1;
        match fetcher.fetch_page(cursor.as_deref()).await {
            Err(e) => {
                // 429 or any fetch error: persist the cursor we failed on so the
                // next run resumes there, then bail (soft-failed upstream). Never
                // block on Retry-After.
                cache.set_source_cursor(RADION_CURSOR_KEY, cursor.as_deref().unwrap_or(""))?;
                return Err(e);
            }
            Ok(page) => {
                for row in page.data {
                    let hex = row.trader_id.to_ascii_lowercase();
                    if hex.starts_with("0x") && hex.len() == 42 {
                        collected.push(hex);
                    }
                }
                match page.next_cursor {
                    None => {
                        // Sweep exhausted — reset so the next run starts at the top.
                        cache.set_source_cursor(RADION_CURSOR_KEY, "")?;
                        exhausted = true;
                        break;
                    }
                    Some(next) => cursor = Some(next),
                }
            }
        }
    }

    // Budget hit without exhaustion — persist the resume point for the next run.
    if !exhausted {
        cache.set_source_cursor(RADION_CURSOR_KEY, cursor.as_deref().unwrap_or(""))?;
    }

    collected.sort_unstable();
    collected.dedup();
    let unique_wallets = collected.len();
    if unique_wallets == 0 {
        return Err(BootstrapError::Radion {
            message: format!(
                "{requests} request(s) collected zero valid wallet addresses — refusing empty upsert"
            ),
        });
    }

    let upserts: Vec<WalletUpsertRow> = collected
        .into_iter()
        .map(|hex| (hex, SRC_RADION, false, None, None, None, 0))
        .collect();
    cache.upsert_wallets_bulk(&upserts)?;

    let activated = pile::apply_activation_rules(cache)?;

    tracing::info!(
        requests,
        unique_wallets,
        activated,
        exhausted,
        "radion_discovery: complete"
    );
    Ok(SourceDiscoveryResult {
        unique_wallets,
        activated,
    })
}

// ── In-memory fetcher for deterministic tests ───────────────────────────────────
//
// Gated on `cfg(test)` for the inline unit tests and `feature = "scenario"` for
// `tests/scenario_radion.rs` (which compiles the lib as a normal dependency, so
// `cfg(test)` is not active there).

/// In-memory [`TraderFetcher`] for deterministic tests. Serves canned pages keyed
/// by the request cursor (`""` = the top / `None`), models per-cursor failures
/// (for the 429/error path), and counts `fetch_page` calls via an [`AtomicU32`] so
/// the per-run request budget can be asserted (the `&self` trait can't count
/// calls otherwise).
#[cfg(any(test, feature = "scenario"))]
pub struct FixtureTraderFetcher {
    pages: HashMap<String, CursorPageTraderAnalysis>,
    fail_keys: HashSet<String>,
    calls: AtomicU32,
}

#[cfg(any(test, feature = "scenario"))]
impl FixtureTraderFetcher {
    /// Build a fixture serving `pages` (cursor-key → page; `""` is the start page).
    pub fn new(pages: HashMap<String, CursorPageTraderAnalysis>) -> Self {
        Self {
            pages,
            fail_keys: HashSet::new(),
            calls: AtomicU32::new(0),
        }
    }

    /// Build a fixture that returns `Err(Radion)` when asked for any cursor in
    /// `fail_keys` (models a `429` / outage on a specific page).
    pub fn with_failures(
        pages: HashMap<String, CursorPageTraderAnalysis>,
        fail_keys: HashSet<String>,
    ) -> Self {
        Self {
            pages,
            fail_keys,
            calls: AtomicU32::new(0),
        }
    }

    /// Number of `fetch_page` calls made against this fixture (AC3 budget cap).
    pub fn call_count(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[cfg(any(test, feature = "scenario"))]
impl TraderFetcher for FixtureTraderFetcher {
    async fn fetch_page(
        &self,
        cursor: Option<&str>,
    ) -> Result<CursorPageTraderAnalysis, BootstrapError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let key = cursor.unwrap_or("").to_owned();
        if self.fail_keys.contains(&key) {
            return Err(BootstrapError::Radion {
                message: format!("fixture: forced failure at cursor {key:?}"),
            });
        }
        self.pages
            .get(&key)
            .cloned()
            .ok_or_else(|| BootstrapError::Radion {
                message: format!("fixture: no page registered for cursor {key:?}"),
            })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp_cache() -> (TempDir, WalletCache) {
        let dir = TempDir::new().unwrap();
        let cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        (dir, cache)
    }

    /// A page from `n` valid lowercase addresses (offset by `base`) plus an
    /// optional `next_cursor`.
    fn page(base: u32, n: u32, next: Option<&str>) -> CursorPageTraderAnalysis {
        CursorPageTraderAnalysis {
            data: (0..n)
                .map(|i| TraderRow {
                    trader_id: format!("0x{:040x}", base * 1_000_000 + i),
                })
                .collect(),
            next_cursor: next.map(str::to_owned),
        }
    }

    /// Shape pin — the live `{data:[{traderId}], nextCursor}` envelope parses, and
    /// unknown fields (`venue`, `traderScore`) are ignored.
    #[test]
    fn parse_traders_analysis_pins_shape() {
        let json = br#"{"data":[{"traderId":"0xABC","venue":"polymarket","traderScore":12.5}],"nextCursor":"eyJ0cmFkZXJfc2NvcmUiOjF9"}"#;
        let p = parse_traders_analysis(json).unwrap();
        assert_eq!(p.data.len(), 1);
        assert_eq!(p.data[0].trader_id, "0xABC");
        assert_eq!(p.next_cursor.as_deref(), Some("eyJ0cmFkZXJfc2NvcmUiOjF9"));
    }

    /// Shape pin — a `null` cursor on the last page deserializes to `None`.
    #[test]
    fn parse_traders_analysis_null_cursor_is_none() {
        let json = br#"{"data":[],"nextCursor":null}"#;
        let p = parse_traders_analysis(json).unwrap();
        assert!(p.next_cursor.is_none());
    }

    /// AC8 (unit) — a `429` maps to `Radion` and carries `Retry-After` in the
    /// message; a non-429 status maps without a retry-after suffix.
    #[test]
    fn http_status_maps_to_radion_with_retry_after() {
        let e = map_http_status_error(429, Some("3600".to_owned()));
        let msg = e.to_string();
        let pass = matches!(e, BootstrapError::Radion { .. })
            && msg.contains("429")
            && msg.contains("retry-after: 3600");
        println!(
            "{}: http_status_maps_to_radion_with_retry_after (msg={msg})",
            if pass { "PASS" } else { "FAIL" }
        );
        assert!(pass, "expected Radion w/ 429 + retry-after; got {msg}");

        let e500 = map_http_status_error(500, None);
        assert!(matches!(e500, BootstrapError::Radion { .. }));
        assert!(!e500.to_string().contains("retry-after"));
    }

    /// AC5 (unit) — non-`0x`/wrong-length ids are dropped; valid ids lowercased.
    #[tokio::test]
    async fn address_hygiene_drops_invalid_lowercases_valid() {
        let (_d, mut cache) = tmp_cache();
        let upper = format!("0x{}", "A".repeat(40)); // 42 chars, uppercase
        let valid = format!("0x{:040x}", 7u32);
        let p = CursorPageTraderAnalysis {
            data: vec![
                TraderRow {
                    trader_id: upper.clone(),
                },
                TraderRow {
                    trader_id: "0xabc".to_owned(), // too short → dropped
                },
                TraderRow {
                    trader_id: "deadbeef".to_owned(), // no 0x → dropped
                },
                TraderRow {
                    trader_id: valid.clone(),
                },
            ],
            next_cursor: None,
        };
        let f = FixtureTraderFetcher::new(HashMap::from([(String::new(), p)]));

        let r = run_radion_discovery(&f, 8, &mut cache).await.unwrap();

        let pass = r.unique_wallets == 2;
        println!(
            "{}: address_hygiene_drops_invalid_lowercases_valid (unique={})",
            if pass { "PASS" } else { "FAIL" },
            r.unique_wallets,
        );
        assert!(pass, "expected unique=2; got {r:?}");
        let ingested = cache.wallets_with_source_bit(SRC_RADION).unwrap();
        assert!(ingested.contains(&upper.to_ascii_lowercase()));
        assert!(!ingested.contains(&upper));
    }

    /// AC7 (unit) — a run collecting zero valid wallets returns `Err(Radion)`.
    #[tokio::test]
    async fn empty_collection_returns_radion_err() {
        let (_d, mut cache) = tmp_cache();
        let p = CursorPageTraderAnalysis {
            data: vec![TraderRow {
                trader_id: "not-a-wallet".to_owned(),
            }],
            next_cursor: None,
        };
        let f = FixtureTraderFetcher::new(HashMap::from([(String::new(), p)]));

        let r = run_radion_discovery(&f, 8, &mut cache).await;
        let pass = matches!(r, Err(BootstrapError::Radion { .. }));
        println!(
            "{}: empty_collection_returns_radion_err (Err(Radion)={pass})",
            if pass { "PASS" } else { "FAIL" }
        );
        assert!(pass, "expected Err(Radion); got {r:?}");
    }

    /// AC4 — discovered wallets are activated immediately (bypass the trade gate).
    #[tokio::test]
    async fn wallets_activate_immediately() {
        let (_d, mut cache) = tmp_cache();
        let f = FixtureTraderFetcher::new(HashMap::from([(String::new(), page(1, 3, None))]));

        let r = run_radion_discovery(&f, 8, &mut cache).await.unwrap();

        let active = cache.active_wallet_count().unwrap();
        let pass = r.activated == 3 && active == 3;
        println!(
            "{}: wallets_activate_immediately (activated={}, active={})",
            if pass { "PASS" } else { "FAIL" },
            r.activated,
            active,
        );
        assert!(
            pass,
            "expected activated=3, active=3; got {r:?}, active={active}"
        );
    }

    /// AC3 — at most `max_requests_per_run` `fetch_page` calls per run (a
    /// self-looping fixture would otherwise page forever).
    #[tokio::test]
    async fn respects_request_budget() {
        let (_d, mut cache) = tmp_cache();
        // "" → loop page, "L" → loop page (self-cycle), so the sweep never exhausts.
        let pages = HashMap::from([
            (String::new(), page(1, 1, Some("L"))),
            ("L".to_owned(), page(2, 1, Some("L"))),
        ]);
        let f = FixtureTraderFetcher::new(pages);

        let _ = run_radion_discovery(&f, 3, &mut cache).await.unwrap();

        let calls = f.call_count();
        let pass = calls == 3;
        println!(
            "{}: respects_request_budget (calls={calls}, budget=3)",
            if pass { "PASS" } else { "FAIL" }
        );
        assert!(pass, "expected exactly 3 fetch calls; got {calls}");
    }
}
