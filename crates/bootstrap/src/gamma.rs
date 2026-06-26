//! Polymarket Gamma API client — fetches market schedule (`endDate`) and `liquidity` data for the
//! bootstrap cache.
//!
//! Endpoint: `GET https://gamma-api.polymarket.com/markets?condition_ids=…`. As of issue #382 this
//! delegates to the shared [`GammaMarketsClient`] in `pe-source-polymarket-public`, which batches
//! many condition_ids per request via repeat-key query params (`condition_ids=A&condition_ids=B&…`,
//! 50/request). A Tier-1 live probe (#382 Phase 0, `scripts/probe_gamma_ua.py`) confirmed repeat-key
//! batching works for **both** the plain (open) and `&closed=true` variants — the earlier "multi-ID
//! batching fails silently" claim was wrong for the repeat-key form. Batched throughput is ~50× the
//! old per-ID ~20 req/s.
//!
//! Rate limit: the global 20 req/s gate (`GAMMA_MIN_INTERVAL_MS` = 50 ms) is still enforced by
//! [`ReqwestFetcher`](pe_source_polymarket_public::ReqwestFetcher)'s shared mutex; batching cuts the
//! request *count* 50× at the same req/s. See `bootstrap_gamma_min_interval_ms`, `gamma_batch_size`,
//! and `gamma_browser_ua` in `docs/_GLOSSARY.md`.
//!
//! Each loop reads its skip-set once, fetches the remainder in batches, then writes the cache serially
//! as the demuxed map is iterated. An id Gamma does not return (an unknown market — Gamma answers
//! `200 []`) is treated exactly as the pre-#382 per-ID path treated an empty response: a NULL schedule
//! row (schedule passes) or a skip (liquidity pass).

use std::collections::HashSet;

use pe_core_types::MarketId;
use pe_source_polymarket_public::{
    GammaMarketsClient, GammaMarketsError, MarketFilter, PageFetcher,
};
use time::OffsetDateTime;
use tracing::info;

use crate::cache::{LiquidityIndex, ResolutionIndex, ScheduleIndex, WalletCache};
use crate::error::BootstrapError;

// Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
pub(crate) const DEFAULT_GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";
pub(crate) const GAMMA_MIN_INTERVAL_MS: u64 = 50; // 20 req/s; live-tested limit ≥27 req/s

/// Map a shared-client error onto the bootstrap error domain.
fn gamma_err(e: GammaMarketsError) -> BootstrapError {
    BootstrapError::Gamma {
        message: e.to_string(),
    }
}

/// Fetches market schedule and liquidity data from the Polymarket Gamma API.
///
/// Generic over [`PageFetcher`] so production code uses
/// [`ReqwestFetcher`](pe_source_polymarket_public::ReqwestFetcher) and tests use
/// [`FixtureFetcher`](pe_source_polymarket_public::FixtureFetcher) with no live network calls.
pub struct GammaFetcher<F: PageFetcher> {
    client: GammaMarketsClient<F>,
}

impl<F: PageFetcher + Send + Sync> GammaFetcher<F> {
    pub fn new(base_url: String, fetcher: F) -> Self {
        Self {
            client: GammaMarketsClient::new(base_url, fetcher),
        }
    }

    /// Fetch scheduled `endDate` for every market ID not already present in `cache`.
    ///
    /// Calls [`WalletCache::scheduled_market_ids`] once up front to build the skip set, then fetches
    /// the remainder via the plain (open) batched endpoint. Each fetched ID is inserted via
    /// `INSERT OR IGNORE` whether or not `end_date_unix` is `Some`, so it enters the skip-set and
    /// will not be re-fetched on the next run.
    ///
    /// Returns the count of fetched IDs that received a row (including NULL rows).
    pub async fn fetch_schedules(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        let already_scheduled = cache.scheduled_market_ids();
        let to_fetch: Vec<String> = market_ids
            .iter()
            .filter(|id| !already_scheduled.contains(*id))
            .cloned()
            .collect();

        info!(
            total = to_fetch.len(),
            already_cached = already_scheduled.len(),
            "gamma: starting schedule fetch (batched)"
        );

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let res = self
            .client
            .fetch_markets(&to_fetch, MarketFilter::OpenOnly)
            .await
            .map_err(gamma_err)?;
        let unfetched: HashSet<&str> = res.unfetched.iter().map(String::as_str).collect();

        let mut inserted = 0usize;
        for id in &to_fetch {
            // A 4xx-skipped chunk's ids are left untouched (no NULL row) so they retry next run,
            // matching the pre-#382 per-ID `Fatal → skip`. An id Gamma omits from a 200 (unknown
            // market) is absent from `markets` → NULL row, as the per-ID empty response produced.
            if unfetched.contains(id.as_str()) {
                continue;
            }
            let end_date_unix = res.markets.get(id).and_then(|m| m.end_date_unix);
            cache.insert_schedule(id, end_date_unix, fetched_at)?;
            inserted += 1;
        }

        info!(
            inserted,
            total = to_fetch.len(),
            "gamma: schedule fetch complete"
        );
        Ok(inserted)
    }

    /// Rewrite NULL `end_date_unix` rows in `market_schedules` by re-fetching with the `&closed=true`
    /// batched variant, then updating each row via [`WalletCache::update_schedule_end_date`].
    ///
    /// Background: Gamma's plain endpoint silently returns an empty list for closed markets; the
    /// `&closed=true` variant surfaces them with `endDate` populated. Caller scopes `market_ids` to
    /// the NULL-row ∩ trade-set. Returns the count of rows actually rewritten — markets where Gamma
    /// returns `endDate=null`, or that Gamma no longer lists, leave the row NULL and contribute 0
    /// (guarded by the cache method's `WHERE end_date_unix IS NULL` clause).
    pub async fn rewrite_null_schedules(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        info!(
            total = market_ids.len(),
            "gamma: starting null-schedule rewrite pass (batched)"
        );

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let res = self
            .client
            .fetch_markets(market_ids, MarketFilter::ClosedOnly)
            .await
            .map_err(gamma_err)?;

        let mut rewritten = 0usize;
        for id in market_ids {
            // Unfetched (4xx chunk), absent (Gamma doesn't list it), or present-without-endDate → no
            // UPDATE (row stays NULL), matching the per-ID `Fatal/None → continue`. UPDATE-only, so
            // an unfetched id is harmless either way (no row written).
            let Some(end_date_unix) = res.markets.get(id).and_then(|m| m.end_date_unix) else {
                continue;
            };
            if cache.update_schedule_end_date(id, end_date_unix, fetched_at)? {
                rewritten += 1;
            }
        }

        info!(
            rewritten,
            total = market_ids.len(),
            "gamma: null-schedule rewrite complete"
        );
        Ok(rewritten)
    }

    /// Backfill `market_schedules` rows for resolved markets that never had their schedule fetched
    /// while open (issue #137 durable follow-up).
    ///
    /// Unlike [`Self::rewrite_null_schedules`] (which UPDATEs existing NULL rows), this INSERTs via
    /// [`WalletCache::insert_schedule`] (`INSERT OR IGNORE`) using the `&closed=true` batched variant.
    /// A row is inserted even when `endDate` is `None`, so the market enters the skip-set. Caller
    /// scopes `market_ids` to the unscheduled-resolved set. Returns the count of rows that received a
    /// non-NULL `endDate`.
    pub async fn backfill_missing_schedules(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        info!(
            total = market_ids.len(),
            "gamma: starting missing-schedule backfill pass (batched)"
        );

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let res = self
            .client
            .fetch_markets(market_ids, MarketFilter::ClosedOnly)
            .await
            .map_err(gamma_err)?;
        let unfetched: HashSet<&str> = res.unfetched.iter().map(String::as_str).collect();

        let mut inserted = 0usize;
        for id in market_ids {
            // A 4xx-skipped chunk's ids are left untouched (no row) so they retry next run.
            if unfetched.contains(id.as_str()) {
                continue;
            }
            let end_date_unix = res.markets.get(id).and_then(|m| m.end_date_unix);
            cache.insert_schedule(id, end_date_unix, fetched_at)?;
            if end_date_unix.is_some() {
                inserted += 1;
            }
        }

        info!(
            inserted,
            total = market_ids.len(),
            "gamma: missing-schedule backfill complete"
        );
        Ok(inserted)
    }

    /// Backfill `start_date_unix` (Gamma `createdAt`) on existing schedule rows whose value is NULL,
    /// by re-fetching with the `&closed=true` batched variant and UPDATEing each row via
    /// [`WalletCache::update_schedule_start_date`] (issue #421 PR4 — the entry-timing CLV feature).
    ///
    /// Mirrors [`Self::rewrite_null_schedules`] (the `endDate` analogue): UPDATE-only with a
    /// `WHERE start_date_unix IS NULL` guard, so an unfetched/4xx id, a market Gamma no longer lists,
    /// or one returning no `createdAt` leaves the row NULL and contributes 0. Caller scopes
    /// `market_ids` to the missing-start-date ∩ resolved set. Returns the count of rows populated.
    pub async fn backfill_start_dates(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        info!(
            total = market_ids.len(),
            "gamma: starting createdAt → start_date backfill pass (batched)"
        );

        let res = self
            .client
            .fetch_markets(market_ids, MarketFilter::ClosedOnly)
            .await
            .map_err(gamma_err)?;

        let mut updated = 0usize;
        for id in market_ids {
            // Unfetched (4xx chunk), absent (Gamma doesn't list it), or present-without-createdAt →
            // no UPDATE (row stays NULL). UPDATE-only, so an unfetched id is harmless.
            let Some(created_at_unix) = res.markets.get(id).and_then(|m| m.created_at_unix) else {
                continue;
            };
            if cache.update_schedule_start_date(id, created_at_unix)? {
                updated += 1;
            }
        }

        info!(
            updated,
            total = market_ids.len(),
            "gamma: createdAt → start_date backfill complete"
        );
        Ok(updated)
    }

    /// Fetch Gamma `liquidity` (current order-book depth indicator) for every market ID not already
    /// present in `cache`.
    ///
    /// Calls [`WalletCache::liquid_market_ids`] once up front to build the skip set, then fetches the
    /// remainder via the plain (open) batched endpoint. The `liquidity` value changes over time, so
    /// re-fetches use `INSERT OR REPLACE`; the skip-set still suppresses re-fetch within one run.
    /// Markets where Gamma returns no `liquidity` (or that Gamma no longer lists) are skipped — no row
    /// is written and the market is re-attempted next run.
    ///
    /// Returns the count of rows upserted.
    pub async fn fetch_market_liquidity(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<usize, BootstrapError> {
        let already_fetched = cache.liquid_market_ids();
        let to_fetch: Vec<String> = market_ids
            .iter()
            .filter(|id| !already_fetched.contains(*id))
            .cloned()
            .collect();

        info!(
            total = to_fetch.len(),
            already_cached = already_fetched.len(),
            "gamma: starting liquidity fetch (batched)"
        );

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let res = self
            .client
            .fetch_markets(&to_fetch, MarketFilter::OpenOnly)
            .await
            .map_err(gamma_err)?;

        let mut inserted = 0usize;
        for id in &to_fetch {
            // Unfetched (4xx chunk) or no-liquidity → skip (no upsert), matching the per-ID path
            // (upsert-only on Some, so an unfetched id is harmless — no row written).
            let Some(liquidity_usd) = res.markets.get(id).and_then(|m| m.liquidity) else {
                continue;
            };
            cache.upsert_market_liquidity(id, liquidity_usd, fetched_at)?;
            inserted += 1;
        }

        info!(
            inserted,
            total = to_fetch.len(),
            "gamma: liquidity fetch complete"
        );
        Ok(inserted)
    }

    /// Fused open-market pass: fetch schedules **and** liquidity for `market_ids` in a single
    /// batched `OpenOnly` request (issue #382 — the deferred schedule/liquidity fusion). Gamma's
    /// open `/markets` row already carries both `endDate` and `liquidity`, so the two separate passes
    /// ([`Self::fetch_schedules`] + [`Self::fetch_market_liquidity`]) refetch the same overlapping
    /// markets twice on a cold cache; fusing them halves the open-pass request volume.
    ///
    /// Each field is written **conditionally**, guarded by its own skip-set, so the cache writes are
    /// identical to running the two passes separately on a clean run: a market already scheduled but
    /// not yet liquid receives only its liquidity row, and vice-versa. Returns
    /// `(schedule_rows, liquidity_rows)` — the same counts the two passes report. Under a 4xx chunk
    /// both fields are skipped for those ids (retry next run), exactly as each separate pass does.
    pub async fn fetch_schedules_and_liquidity(
        &self,
        market_ids: &[String],
        cache: &mut WalletCache,
    ) -> Result<(usize, usize), BootstrapError> {
        let already_scheduled = cache.scheduled_market_ids();
        let already_liquid = cache.liquid_market_ids();

        // Fetch the union of the two passes' work — a market missing from *either* skip-set. Each
        // field is still written only when its own skip-set lacks the id (below), so the writes match
        // the separate passes; this only collapses the two fetches of the overlapping ids into one.
        let to_fetch: Vec<String> = market_ids
            .iter()
            .filter(|id| !already_scheduled.contains(*id) || !already_liquid.contains(*id))
            .cloned()
            .collect();

        info!(
            total = to_fetch.len(),
            already_scheduled = already_scheduled.len(),
            already_liquid = already_liquid.len(),
            "gamma: starting fused schedule+liquidity fetch (batched)"
        );

        let fetched_at = OffsetDateTime::now_utc().unix_timestamp();
        let res = self
            .client
            .fetch_markets(&to_fetch, MarketFilter::OpenOnly)
            .await
            .map_err(gamma_err)?;
        let unfetched: HashSet<&str> = res.unfetched.iter().map(String::as_str).collect();

        let mut schedule_rows = 0usize;
        let mut liquidity_rows = 0usize;
        for id in &to_fetch {
            // A 4xx-skipped chunk's ids are left untouched for both fields (no schedule NULL row, no
            // liquidity row) so they retry next run — matching each separate pass's `Fatal → skip`.
            if unfetched.contains(id.as_str()) {
                continue;
            }
            let market = res.markets.get(id);
            // Schedule (`INSERT OR IGNORE`, NULL row included) — only for ids not already scheduled.
            if !already_scheduled.contains(id) {
                let end_date_unix = market.and_then(|m| m.end_date_unix);
                cache.insert_schedule(id, end_date_unix, fetched_at)?;
                schedule_rows += 1;
            }
            // Liquidity (`INSERT OR REPLACE`) — only for ids not already liquid AND with a value.
            if !already_liquid.contains(id)
                && let Some(liquidity_usd) = market.and_then(|m| m.liquidity)
            {
                cache.upsert_market_liquidity(id, liquidity_usd, fetched_at)?;
                liquidity_rows += 1;
            }
        }

        info!(
            schedule_rows,
            liquidity_rows,
            total = to_fetch.len(),
            "gamma: fused schedule+liquidity fetch complete"
        );
        Ok((schedule_rows, liquidity_rows))
    }
}

// ── Convenience: build indexes without running a full bootstrap ───────────────

/// Load all resolutions from `cache` into a [`ResolutionIndex`].
pub fn load_resolutions(cache: &WalletCache) -> Result<ResolutionIndex, BootstrapError> {
    cache.load_all_resolutions()
}

/// Load all schedule rows from `cache` into a [`ScheduleIndex`].
pub fn load_schedules(cache: &WalletCache) -> Result<ScheduleIndex, BootstrapError> {
    cache.load_all_schedules()
}

/// Load all liquidity rows from `cache` into a [`LiquidityIndex`].
pub fn load_liquidity(cache: &WalletCache) -> Result<LiquidityIndex, BootstrapError> {
    cache.load_all_liquidity()
}

/// Load resolutions for only `markets` into a [`ResolutionIndex`] (the injected-set
/// bake-off path, #453; bounded to the traded markets, mirroring the bounded CLOB-mark
/// load). Identical to [`load_resolutions`] restricted to `markets`.
pub fn load_resolutions_for_markets(
    cache: &WalletCache,
    markets: &HashSet<MarketId>,
) -> Result<ResolutionIndex, BootstrapError> {
    cache.load_resolutions_for_markets(markets)
}

/// Load schedules for only `markets` into a [`ScheduleIndex`] (the injected-set
/// bake-off path, #453). Identical to [`load_schedules`] restricted to `markets`.
pub fn load_schedules_for_markets(
    cache: &WalletCache,
    markets: &HashSet<MarketId>,
) -> Result<ScheduleIndex, BootstrapError> {
    cache.load_schedules_for_markets(markets)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use pe_source_polymarket_public::FixtureFetcher;
    use tempfile::TempDir;

    use super::*;
    use crate::cache::WalletCache;

    const BASE: &str = "https://gamma-api.polymarket.com";

    fn fixture_bytes(json: &str) -> Vec<u8> {
        json.as_bytes().to_vec()
    }

    /// The shared client appends `&limit=500` and (for closed) `&closed=true` after the
    /// `condition_ids=` keys — these helpers build the exact URL keys FixtureFetcher expects.
    fn open_url(id: &str) -> String {
        format!("{BASE}/markets?condition_ids={id}&limit=500")
    }
    fn closed_url(id: &str) -> String {
        format!("{BASE}/markets?condition_ids={id}&closed=true&limit=500")
    }

    fn open_with_end_date_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":false,"closedTime":null,"outcomePrices":null,"endDate":"2026-07-31T12:00:00Z","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn open_no_end_date_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":false,"closedTime":null,"outcomePrices":null,"outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn closed_with_end_date_fixture() -> Vec<u8> {
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomePrices":"[\"1\",\"0\"]","endDate":"2024-01-15T00:00:00Z","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn closed_no_end_date_fixture() -> Vec<u8> {
        // Closed market that genuinely has endDate=null — the 1/100 case in the empirical probe.
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":true,"closedTime":"2024-01-15 12:00:00+00","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    fn gamma(responses: HashMap<String, Vec<u8>>) -> GammaFetcher<FixtureFetcher> {
        GammaFetcher::new(BASE.to_owned(), FixtureFetcher::new(responses))
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn fetch_schedules_skips_already_cached_ids() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Pre-insert one market as already-scheduled.
        cache
            .insert_schedule("0xa", Some(1_700_000_000), 1_700_000_001)
            .unwrap();

        // Fixture only handles "0xb"; if "0xa" were fetched its batch URL would miss → Fatal → skip.
        let mut responses = HashMap::new();
        responses.insert(open_url("0xb"), open_with_end_date_fixture());
        let gamma = gamma(responses);

        let inserted = rt()
            .block_on(gamma.fetch_schedules(&["0xa".to_owned(), "0xb".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(inserted, 1, "only 0xb must be fetched (0xa was cached)");
        assert_eq!(cache.load_all_schedules().unwrap().len(), 2);
    }

    #[test]
    fn fetch_schedules_inserts_null_for_missing_end_date() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        let mut responses = HashMap::new();
        responses.insert(open_url("0xcond"), open_no_end_date_fixture());
        let gamma = gamma(responses);

        let inserted = rt()
            .block_on(gamma.fetch_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(inserted, 1, "missing endDate must still insert a row");
        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(idx.len(), 1, "NULL row must be present in index");
        assert_eq!(idx.values().next().unwrap().end_date_unix, None);
        // NULL row must also be in the skip-set.
        assert_eq!(cache.scheduled_market_ids().len(), 1);
    }

    #[test]
    fn rewrite_null_schedules_populates_via_closed_endpoint() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .insert_schedule("0xcond", None, 1_700_000_000)
            .unwrap();

        let mut responses = HashMap::new();
        responses.insert(closed_url("0xcond"), closed_with_end_date_fixture());
        let gamma = gamma(responses);

        let rewritten = rt()
            .block_on(gamma.rewrite_null_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(rewritten, 1, "single populated response must rewrite 1 row");

        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(
            idx.values().next().unwrap().end_date_unix,
            Some(1_705_276_800),
            "end_date must be the parsed closed-market value"
        );
        assert!(cache.null_schedule_market_ids().is_empty());
    }

    #[test]
    fn rewrite_null_schedules_leaves_row_null_when_gamma_returns_no_enddate() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .insert_schedule("0xcond", None, 1_700_000_000)
            .unwrap();

        let mut responses = HashMap::new();
        responses.insert(closed_url("0xcond"), closed_no_end_date_fixture());
        let gamma = gamma(responses);

        let rewritten = rt()
            .block_on(gamma.rewrite_null_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(rewritten, 0, "no endDate in response → no row rewritten");
        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(
            idx.values().next().unwrap().end_date_unix,
            None,
            "NULL must be preserved"
        );
    }

    #[test]
    fn rewrite_null_schedules_uses_closed_true_url_not_plain() {
        // Regression guard: the URL MUST include &closed=true. We map only the &closed=true batch
        // URL; if the implementation regressed to the plain URL, the request would miss under
        // FixtureFetcher (Fatal → skipped) and rewritten would stay 0.
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .insert_schedule("0xcond", None, 1_700_000_000)
            .unwrap();

        let mut responses = HashMap::new();
        responses.insert(closed_url("0xcond"), closed_with_end_date_fixture());
        let gamma = gamma(responses);

        let rewritten = rt()
            .block_on(gamma.rewrite_null_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(
            rewritten, 1,
            "&closed=true URL must be used; rewritten=0 here would indicate a regression to the plain URL"
        );
    }

    fn closed_with_created_at_fixture() -> Vec<u8> {
        // issue #421 PR4: a closed market carrying both createdAt and endDate.
        fixture_bytes(
            r#"[{"conditionId":"0xcond","closed":true,"createdAt":"2024-01-10T00:00:00Z","endDate":"2024-01-15T00:00:00Z","outcomes":"[\"Yes\",\"No\"]"}]"#,
        )
    }

    #[test]
    fn backfill_start_dates_populates_via_closed_endpoint() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        // Existing schedule row; start_date_unix is NULL after a plain insert.
        cache
            .insert_schedule("0xcond", Some(1_700_000_000), 1_700_000_001)
            .unwrap();
        assert!(cache.market_ids_missing_start_date().contains("0xcond"));

        let mut responses = HashMap::new();
        responses.insert(closed_url("0xcond"), closed_with_created_at_fixture());
        let gamma = gamma(responses);

        let updated = rt()
            .block_on(gamma.backfill_start_dates(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(updated, 1, "createdAt present → start_date populated");
        assert!(
            !cache.market_ids_missing_start_date().contains("0xcond"),
            "start_date_unix must no longer be NULL after backfill"
        );
    }

    #[test]
    fn backfill_start_dates_leaves_null_when_no_created_at() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        cache
            .insert_schedule("0xcond", Some(1_700_000_000), 1_700_000_001)
            .unwrap();

        // closed_with_end_date_fixture has endDate but NO createdAt.
        let mut responses = HashMap::new();
        responses.insert(closed_url("0xcond"), closed_with_end_date_fixture());
        let gamma = gamma(responses);

        let updated = rt()
            .block_on(gamma.backfill_start_dates(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(updated, 0, "no createdAt → no update");
        assert!(
            cache.market_ids_missing_start_date().contains("0xcond"),
            "start_date_unix must remain NULL when Gamma returns no createdAt"
        );
    }

    #[test]
    fn backfill_missing_schedules_inserts_parsed_end_date_for_closed_market() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        assert!(cache.scheduled_market_ids().is_empty());

        let mut responses = HashMap::new();
        responses.insert(closed_url("0xcond"), closed_with_end_date_fixture());
        let gamma = gamma(responses);

        let inserted = rt()
            .block_on(gamma.backfill_missing_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(
            inserted, 1,
            "closed market with endDate must count as 1 insert"
        );

        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(idx.len(), 1, "exactly one schedule row must be inserted");
        assert_eq!(
            idx.values().next().unwrap().end_date_unix,
            Some(1_705_276_800),
            "end_date must be the parsed closed-market value"
        );
        assert_eq!(cache.scheduled_market_ids().len(), 1);
    }

    #[test]
    fn backfill_missing_schedules_inserts_null_row_when_no_end_date() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();
        assert!(cache.scheduled_market_ids().is_empty());

        let mut responses = HashMap::new();
        responses.insert(closed_url("0xcond"), closed_no_end_date_fixture());
        let gamma = gamma(responses);

        let inserted = rt()
            .block_on(gamma.backfill_missing_schedules(&["0xcond".to_owned()], &mut cache))
            .unwrap();
        assert_eq!(inserted, 0, "no endDate → non-NULL counter stays 0");

        let idx = cache.load_all_schedules().unwrap();
        assert_eq!(
            idx.len(),
            1,
            "a NULL row must still be inserted (marked attempted)"
        );
        assert_eq!(idx.values().next().unwrap().end_date_unix, None);
        assert_eq!(cache.scheduled_market_ids().len(), 1);
    }

    #[test]
    fn fetch_market_liquidity_upserts_when_present_and_skips_when_absent() {
        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // Both ids land in ONE batch request; the array carries 0xliq (with liquidity) and 0xnone
        // (no liquidity field → skipped at the call site).
        let mut responses = HashMap::new();
        responses.insert(
            format!("{BASE}/markets?condition_ids=0xliq&condition_ids=0xnone&limit=500"),
            fixture_bytes(
                r#"[{"conditionId":"0xliq","liquidity":12345.5},{"conditionId":"0xnone","closed":false}]"#,
            ),
        );
        let gamma = gamma(responses);

        let inserted = rt()
            .block_on(
                gamma
                    .fetch_market_liquidity(&["0xliq".to_owned(), "0xnone".to_owned()], &mut cache),
            )
            .unwrap();
        assert_eq!(
            inserted, 1,
            "only the market with a liquidity field is upserted"
        );
        let idx = cache.load_all_liquidity().unwrap();
        assert_eq!(idx.len(), 1, "exactly one liquidity row");
        assert_eq!(
            idx.values().next().unwrap().to_string(),
            "12345.5",
            "the upserted row carries 0xliq's parsed liquidity"
        );
    }

    #[test]
    fn fused_pass_writes_each_field_only_for_its_skip_set() {
        use rust_decimal_macros::dec;

        let dir = TempDir::new().unwrap();
        let mut cache = WalletCache::open(&dir.path().join("cache.db")).unwrap();

        // 0xb is already scheduled (needs only liquidity); 0xc is already liquid (needs only a
        // schedule); 0xa needs both. The fused pass serves all three from ONE batched OpenOnly
        // request, writing each field only where its own skip-set lacks the id.
        cache
            .insert_schedule("0xb", Some(1_700_000_000), 1_700_000_001)
            .unwrap();
        cache
            .upsert_market_liquidity("0xc", dec!(50), 1_700_000_001)
            .unwrap();

        let mut responses = HashMap::new();
        responses.insert(
            format!("{BASE}/markets?condition_ids=0xa&condition_ids=0xb&condition_ids=0xc&limit=500"),
            fixture_bytes(
                r#"[{"conditionId":"0xa","closed":false,"endDate":"2026-07-31T12:00:00Z","liquidity":111},
                    {"conditionId":"0xb","closed":false,"endDate":"2026-07-31T12:00:00Z","liquidity":222},
                    {"conditionId":"0xc","closed":false,"endDate":"2026-07-31T12:00:00Z","liquidity":333}]"#,
            ),
        );
        let gamma = gamma(responses);

        let (schedule_rows, liquidity_rows) = rt()
            .block_on(gamma.fetch_schedules_and_liquidity(
                &["0xa".to_owned(), "0xb".to_owned(), "0xc".to_owned()],
                &mut cache,
            ))
            .unwrap();

        // Schedule written for the not-yet-scheduled (0xa, 0xc); 0xb suppressed by its skip-set.
        assert_eq!(schedule_rows, 2, "schedule written for 0xa + 0xc, not 0xb");
        // Liquidity written for the not-yet-liquid (0xa, 0xb); 0xc suppressed by its skip-set.
        assert_eq!(
            liquidity_rows, 2,
            "liquidity written for 0xa + 0xb, not 0xc"
        );

        // All three end up scheduled (0xa, 0xc new + 0xb pre-existing) and liquid (0xa, 0xb new + 0xc
        // pre-existing). 0xc's liquidity was NOT re-upserted — had the skip-set been ignored,
        // `liquidity_rows` would be 3 and 0xc's value would have moved from 50 to 333.
        assert_eq!(cache.load_all_schedules().unwrap().len(), 3);
        assert_eq!(cache.load_all_liquidity().unwrap().len(), 3);
    }
}
