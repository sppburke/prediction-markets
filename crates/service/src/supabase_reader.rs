//! Supabase (PostgREST) reader for the copy-trade wallet-ranking handoff (issue #339).
//!
//! The local ranker pushes append-only ranking batches to Supabase
//! (`scripts/push_ranking_to_supabase.py`); `pe-service` reads the `latest_ranking`
//! view and maps each row to a [`WatchlistEntry`]. The mapping is pure and unit-tested;
//! [`fetch`] is the only I/O.
//!
//! Numeric columns are decoded exactly through the query-level `::text` aliases
//! (`hit_rate_text`/`ls_tstat_text`, [`RANKING_EXACT_SELECT`]): PostgREST serializes an
//! uncast `numeric` as a JSON number, which `serde_json` (no `arbitrary_precision`)
//! stores as **f64** — lossy at precision boundaries (#514). The alias-free
//! [`serde_json::Value`] path remains as the fallback for previously recorded bodies
//! (canary replay), and is best-effort at f64 precision only.

use std::collections::HashMap;
use std::collections::HashSet;
use std::str::FromStr as _;

use pe_core_types::{
    BasisPoints, RawHttpAttempt, RawHttpResponse, RawTransportFailure, ReconstructionQuality,
    SourceTimestamp, TransportErrorClass, WalletAddress,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive as _;
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// t-stat → basis-points scale for `leader_score_bps`. The score only orders entries
/// within the live set (`watchlist.rs`: entries sorted descending by `leader_score_bps`);
/// it is not a gate. A t-stat of 2.5 maps to 2500 bps. See `docs/_GLOSSARY.md`.
const LS_TSTAT_BPS_SCALE: i64 = 1_000;

/// Candidate freshness window in hours (#357): a benched wallet is an eligible backfill
/// candidate only if its real last trade (`last_trade_unix`) is within this window. Mirrors
/// the canonical `upload_active_window_hours` = 72 in `docs/_GLOSSARY.md` (the ranker drops
/// wallets idle > 72h at upload; this is the read-side gate for the same window). Typed `i64`
/// for direct unix-second arithmetic with the poll clock — no narrowing cast.
pub const ACTIVE_WINDOW_HOURS: i64 = 72;
pub const CANARY_RANKING_MAX_AGE_SECS: i64 = 21_600;

/// Reconstruction quality assigned to Supabase-sourced wallets. The ranker has already
/// applied its own data-quality gates, so these wallets are treated as fully reconstructed
/// (`100`) for the copy path's `LeaderAction` classification.
const SUPABASE_RECONSTRUCTION_QUALITY: u8 = 100;

/// Error surface for a Supabase fetch. The refresh loop logs and continues on any of these.
#[derive(Debug, thiserror::Error)]
pub enum SupabaseError {
    /// The HTTP request itself failed (DNS, connect, timeout, …).
    #[error("supabase request failed: {0}")]
    Transport(reqwest::Error),
    /// The endpoint returned a non-2xx status.
    #[error("supabase returned HTTP {0}")]
    Status(u16),
    /// The 2xx body could not be decoded as the expected JSON rows.
    #[error("supabase response decode failed: {0}")]
    Decode(reqwest::Error),
    #[error("supabase response JSON failed: {0}")]
    Json(serde_json::Error),
    #[error("supabase canary observation failed: {reason}")]
    CanaryObserved {
        reason: String,
        attempts: Vec<RawHttpAttempt>,
    },
}

fn observed_canary_contract(
    reason: impl Into<String>,
    responses: &[RawHttpResponse],
) -> SupabaseError {
    SupabaseError::CanaryObserved {
        reason: reason.into(),
        attempts: responses
            .iter()
            .cloned()
            .map(RawHttpAttempt::Response)
            .collect(),
    }
}

/// One row of the `latest_ranking` view. Unused columns (`batch_id`, `rank`, `ls_edge`,
/// `fill_rate`, `avg_price`) are ignored by serde.
#[derive(Debug, Deserialize)]
struct RankingRow {
    #[serde(default)]
    batch_id: Option<i64>,
    #[serde(default)]
    rank: Option<i64>,
    wallet_hex: String,
    /// Mean payoff among filled positions ∈ [0,1] = Kelly `p`. → `win_rate_bps`.
    #[serde(default)]
    hit_rate: Option<serde_json::Value>,
    /// Latency-shifted net-edge t-stat. → `leader_score_bps` (ordering only).
    #[serde(default)]
    ls_tstat: Option<serde_json::Value>,
    /// Number of filled positions in the eligibility window. → `closed_trades_in_window`.
    #[serde(default)]
    n_trades: Option<i64>,
    /// Wallet's real last on-chain trade time (unix seconds), stamped by the ranker (#357).
    /// Absent column or JSON `null` → `None`. Drives the candidate freshness filter and seeds the
    /// poll cursor / inactivity clock (#357 PR-3); it never affects the row→entry map.
    #[serde(default)]
    last_trade_unix: Option<i64>,
    /// Exact text projection of `hit_rate` ([`RANKING_EXACT_SELECT`], #514). Preferred
    /// when present; absent on previously recorded alias-free bodies, which fall back to
    /// the f64-precision `Value` path.
    #[serde(default)]
    hit_rate_text: Option<String>,
    /// Exact text projection of `ls_tstat`; same preference/fallback as `hit_rate_text`.
    #[serde(default)]
    ls_tstat_text: Option<String>,
}

/// PostgREST select for every `latest_ranking` read: all columns plus exact `::text`
/// aliases for the two numeric score columns (#514). Additive — no column enumeration —
/// so a schema change cannot silently drop a column from the paper watchlist read.
const RANKING_EXACT_SELECT: &str = "*,hit_rate_text:hit_rate::text,ls_tstat_text:ls_tstat::text";

/// `latest_ranking` read URL shared by the ordinary fetch and the canary (#514).
fn latest_ranking_url(base_url: &str, limit: usize) -> String {
    format!(
        "{}/rest/v1/latest_ranking?select={RANKING_EXACT_SELECT}&order=rank&limit={limit}",
        base_url.trim_end_matches('/')
    )
}

/// Strict canary-only ranking read. Unlike the ordinary paper reader, this rejects malformed,
/// stale, duplicate, incomplete, or cross-batch rows and raw-captures the owning batch timestamp.
pub async fn fetch_canary_observed(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    limit: usize,
) -> Result<(Watchlist, HashMap<WalletAddress, i64>, Vec<RawHttpResponse>), SupabaseError> {
    let ranking_url = latest_ranking_url(base_url, limit);
    let ranking = get_observation(client, &ranking_url, anon_key).await?;
    let mut observations = vec![ranking.clone()];
    let rows: Vec<RankingRow> = serde_json::from_slice(&ranking.body)
        .map_err(|error| observed_canary_contract(error.to_string(), &observations))?;
    if rows.is_empty() {
        return Err(observed_canary_contract(
            "latest ranking is empty",
            &observations,
        ));
    }
    if rows.len() > limit {
        return Err(observed_canary_contract(
            "latest ranking exceeded the requested limit",
            &observations,
        ));
    }
    let now = OffsetDateTime::now_utc();
    let mut batch_id = None;
    let mut wallets = HashSet::new();
    let mut ranks = HashSet::new();
    let mut previous_rank = None;
    let mut entries = Vec::with_capacity(rows.len());
    let mut cursors = HashMap::new();
    for row in &rows {
        let row_batch = row.batch_id.ok_or_else(|| {
            observed_canary_contract("ranking row omitted batch_id", &observations)
        })?;
        if batch_id
            .replace(row_batch)
            .is_some_and(|batch| batch != row_batch)
        {
            return Err(observed_canary_contract(
                "latest ranking mixed multiple batches",
                &observations,
            ));
        }
        let rank = row.rank.filter(|rank| *rank > 0).ok_or_else(|| {
            observed_canary_contract("ranking row omitted a valid rank", &observations)
        })?;
        if !ranks.insert(rank) {
            return Err(observed_canary_contract(
                "latest ranking repeated a rank",
                &observations,
            ));
        }
        if previous_rank.is_some_and(|previous| rank <= previous) {
            return Err(observed_canary_contract(
                "latest ranking was not ordered by increasing rank",
                &observations,
            ));
        }
        previous_rank = Some(rank);
        let entry = map_row(row).ok_or_else(|| {
            observed_canary_contract("ranking row has an invalid wallet", &observations)
        })?;
        if !wallets.insert(entry.wallet) {
            return Err(observed_canary_contract(
                "latest ranking repeated a wallet",
                &observations,
            ));
        }
        let hit_rate = exact_cell(row.hit_rate_text.as_deref(), row.hit_rate.as_ref());
        let score = exact_cell(row.ls_tstat_text.as_deref(), row.ls_tstat.as_ref());
        let score_fits = score
            .and_then(|value| (value * Decimal::from(LS_TSTAT_BPS_SCALE)).round().to_i32())
            .is_some();
        let trade_count_fits = row
            .n_trades
            .and_then(|trades| u32::try_from(trades).ok())
            .is_some();
        if hit_rate.is_none_or(|value| !(Decimal::ZERO..=Decimal::ONE).contains(&value))
            || !score_fits
            || !trade_count_fits
        {
            return Err(observed_canary_contract(
                "ranking row has an invalid required score field",
                &observations,
            ));
        }
        let last_trade = row.last_trade_unix.ok_or_else(|| {
            observed_canary_contract("ranking row omitted last_trade_unix", &observations)
        })?;
        let age = now.unix_timestamp().saturating_sub(last_trade);
        if age < 0 || age > ACTIVE_WINDOW_HOURS.saturating_mul(3_600) {
            return Err(observed_canary_contract(
                "ranking row last trade is stale or future-dated",
                &observations,
            ));
        }
        cursors.insert(entry.wallet, last_trade);
        entries.push(entry);
    }
    let batch_id = batch_id.ok_or_else(|| {
        observed_canary_contract("latest ranking omitted batch identity", &observations)
    })?;
    let batch_url = format!(
        "{}/rest/v1/ranking_batches?select=batch_id,created_at&batch_id=eq.{batch_id}&limit=2",
        base_url.trim_end_matches('/')
    );
    let batch = match get_observation(client, &batch_url, anon_key).await {
        Ok(batch) => batch,
        Err(SupabaseError::CanaryObserved {
            reason,
            mut attempts,
        }) => {
            let mut prior = observations
                .iter()
                .cloned()
                .map(RawHttpAttempt::Response)
                .collect::<Vec<_>>();
            prior.append(&mut attempts);
            return Err(SupabaseError::CanaryObserved {
                reason,
                attempts: prior,
            });
        }
        Err(error) => return Err(error),
    };
    observations.push(batch.clone());
    #[derive(Deserialize)]
    struct BatchRow {
        batch_id: i64,
        created_at: String,
    }
    let batch_rows: Vec<BatchRow> = serde_json::from_slice(&batch.body)
        .map_err(|error| observed_canary_contract(error.to_string(), &observations))?;
    let [batch_row] = batch_rows.as_slice() else {
        return Err(observed_canary_contract(
            "ranking batch lookup did not return exactly one row",
            &observations,
        ));
    };
    let created_at = OffsetDateTime::parse(&batch_row.created_at, &Rfc3339).map_err(|error| {
        observed_canary_contract(
            format!("ranking batch created_at is invalid: {error}"),
            &observations,
        )
    })?;
    let age = now - created_at;
    if batch_row.batch_id != batch_id
        || age.is_negative()
        || age.whole_seconds() > CANARY_RANKING_MAX_AGE_SECS
    {
        return Err(observed_canary_contract(
            "ranking batch is mismatched, stale, or future-dated",
            &observations,
        ));
    }
    let active_count = entries.len();
    Ok((
        Watchlist {
            entries,
            snapshot_at: SourceTimestamp(created_at),
            active_count,
            incubator_count: 0,
        },
        cursors,
        observations,
    ))
}

async fn get_observation(
    client: &reqwest::Client,
    url: &str,
    anon_key: &str,
) -> Result<RawHttpResponse, SupabaseError> {
    let parsed = reqwest::Url::parse(url).map_err(|error| SupabaseError::CanaryObserved {
        reason: error.to_string(),
        attempts: Vec::new(),
    })?;
    let path = parsed.path().to_owned();
    let ordered_query = parsed
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let observed_at = OffsetDateTime::now_utc();
    let response = client
        .get(url)
        .header("apikey", anon_key)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {anon_key}"))
        .send()
        .await
        .map_err(|error| SupabaseError::CanaryObserved {
            reason: error.to_string(),
            attempts: vec![RawHttpAttempt::TransportFailure(RawTransportFailure {
                source_id: "supabase-ranking".to_owned(),
                endpoint_kind: path.clone(),
                method: "GET".to_owned(),
                path: path.clone(),
                ordered_query: ordered_query.clone(),
                attempt_ordinal: 1,
                observed_at,
                received_at: OffsetDateTime::now_utc(),
                error_class: if error.is_timeout() {
                    TransportErrorClass::Timeout
                } else if error.is_connect() {
                    TransportErrorClass::Connect
                } else {
                    TransportErrorClass::Other
                },
                schema_version: 1,
                parser_version: 1,
                adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
            })],
        })?;
    let status = response.status();
    let headers: Vec<(String, String)> = response
        .headers()
        .iter()
        .filter_map(|(name, value)| {
            matches!(
                name.as_str(),
                "content-type" | "date" | "etag" | "retry-after" | "x-request-id"
            )
            .then(|| {
                value
                    .to_str()
                    .ok()
                    .map(|value| (name.to_string(), value.to_owned()))
            })
            .flatten()
        })
        .collect();
    let body = response
        .bytes()
        .await
        .map_err(|error| SupabaseError::CanaryObserved {
            reason: error.to_string(),
            attempts: vec![RawHttpAttempt::TransportFailure(RawTransportFailure {
                source_id: "supabase-ranking".to_owned(),
                endpoint_kind: path.clone(),
                method: "GET".to_owned(),
                path: path.clone(),
                ordered_query: ordered_query.clone(),
                attempt_ordinal: 1,
                observed_at,
                received_at: OffsetDateTime::now_utc(),
                error_class: TransportErrorClass::BodyRead,
                schema_version: 1,
                parser_version: 1,
                adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
            })],
        })?
        .to_vec();
    let source_at = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("date"))
        .and_then(|(_, value)| {
            OffsetDateTime::parse(value, &time::format_description::well_known::Rfc2822).ok()
        });
    let observation = RawHttpResponse {
        source_id: "supabase-ranking".to_owned(),
        endpoint_kind: path.clone(),
        method: "GET".to_owned(),
        path,
        ordered_query,
        observed_at,
        received_at: OffsetDateTime::now_utc(),
        status: status.as_u16(),
        headers,
        body,
        attempt_ordinal: 1,
        source_at,
        schema_version: 1,
        parser_version: 1,
        adapter_version: env!("CARGO_PKG_VERSION").to_owned(),
    };
    if !status.is_success() {
        return Err(SupabaseError::CanaryObserved {
            reason: format!("HTTP {}", status.as_u16()),
            attempts: vec![RawHttpAttempt::Response(observation)],
        });
    }
    Ok(observation)
}

/// Parse a PostgREST numeric cell (JSON number or string) into a [`Decimal`].
///
/// This path is best-effort, NOT lossless (#514): `serde_json` without
/// `arbitrary_precision` stores a fractional JSON number as f64, so
/// `Number::to_string` yields the f64 round-trip literal, not the column's exact value
/// (e.g. `0.000149999999999999999999` arrives as `0.00015`). Exactness comes from the
/// `::text` aliases ([`exact_cell`]); this fallback remains for previously recorded
/// alias-free bodies.
fn cell_to_decimal(v: Option<&serde_json::Value>) -> Option<Decimal> {
    match v {
        Some(serde_json::Value::Number(n)) => Decimal::from_str(&n.to_string()).ok(),
        Some(serde_json::Value::String(s)) => Decimal::from_str(s.trim()).ok(),
        _ => None,
    }
}

/// Decode one ranking score cell: the exact `::text` alias when present (a malformed
/// alias fails to `None` rather than silently degrading to f64), else the recorded-body
/// [`cell_to_decimal`] fallback.
fn exact_cell(text: Option<&str>, value: Option<&serde_json::Value>) -> Option<Decimal> {
    match text {
        Some(text) => Decimal::from_str(text.trim()).ok(),
        None => cell_to_decimal(value),
    }
}

/// Map one ranking row to a [`WatchlistEntry`]. Returns `None` only when `wallet_hex` is
/// not a valid address (the row is skipped); all other fields fall back to honest zeros.
fn map_row(row: &RankingRow) -> Option<WatchlistEntry> {
    // Reuse the address serde impl (validates `0x` + 40 hex). Bad hex → skip the row.
    let wallet: WalletAddress =
        serde_json::from_value(serde_json::Value::String(row.wallet_hex.clone())).ok()?;

    let win_rate_bps = exact_cell(row.hit_rate_text.as_deref(), row.hit_rate.as_ref())
        .map(|hr| (hr * Decimal::from(10_000)).round())
        .and_then(|d| d.to_i32())
        .unwrap_or(0)
        .clamp(0, 10_000);

    let leader_score_bps = exact_cell(row.ls_tstat_text.as_deref(), row.ls_tstat.as_ref())
        .map(|t| (t * Decimal::from(LS_TSTAT_BPS_SCALE)).round())
        .and_then(|d| d.to_i32())
        .unwrap_or(0);

    let closed_trades_in_window = row
        .n_trades
        .filter(|&n| n >= 0)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0);

    // `100` is statically in range, so `new` cannot fail here; `.ok()?` keeps the lint
    // and the proof-of-validity together without an `unwrap`.
    let reconstruction_quality =
        ReconstructionQuality::new(SUPABASE_RECONSTRUCTION_QUALITY).ok()?;

    Some(WatchlistEntry {
        wallet,
        tier: WatchlistTier::Active,
        leader_score_bps: BasisPoints(leader_score_bps),
        lcb_5pct_bps: BasisPoints(0),
        win_rate_bps: BasisPoints(win_rate_bps),
        closed_trades_in_window,
        reconstruction_quality,
    })
}

/// Assemble fetched rows into a [`Watchlist`] (all tier `Active`, snapshot stamped now) plus
/// the side-map of each valid wallet's real last-trade time (#357).
///
/// The map is keyed by the same validated [`WalletAddress`] that enters the watchlist — a
/// bad-hex row is skipped from BOTH — and holds only rows that carry a `last_trade_unix`
/// (absent → omitted, never a sentinel). #357 PR-3 consumes it to seed each wallet's poll cursor
/// (the inactivity clock) at bootstrap (`main.rs`) and backfill admission.
fn to_watchlist(rows: &[RankingRow]) -> (Watchlist, HashMap<WalletAddress, i64>) {
    let mut entries: Vec<WatchlistEntry> = Vec::with_capacity(rows.len());
    let mut last_trade: HashMap<WalletAddress, i64> = HashMap::new();
    for row in rows {
        let Some(entry) = map_row(row) else { continue };
        if let Some(ts) = row.last_trade_unix {
            last_trade.insert(entry.wallet, ts);
        }
        entries.push(entry);
    }
    let active_count = entries
        .iter()
        .filter(|e| e.tier == WatchlistTier::Active)
        .count();
    let total = entries.len();
    let watchlist = Watchlist {
        entries,
        snapshot_at: SourceTimestamp(OffsetDateTime::now_utc()),
        active_count,
        incubator_count: total - active_count,
    };
    (watchlist, last_trade)
}

/// Select the single API token to send in BOTH the `apikey` and `Authorization: Bearer`
/// headers.
///
/// Supabase's modern `sb_publishable_`/`sb_secret_` keys are NOT JWTs, and PostgREST rejects
/// a request whose two headers carry *different* tokens — it tries to parse the Bearer as a
/// 3-part JWT and fails (`PGRST301: Expected 3 parts in JWT; got 1`). So both headers must
/// use one token. Prefer the service-role secret (bypasses RLS for the server-side read);
/// fall back to the publishable/anon key when no secret is configured.
pub(crate) fn auth_token<'a>(anon_key: &'a str, secret_key: &'a str) -> &'a str {
    if secret_key.is_empty() {
        anon_key
    } else {
        secret_key
    }
}

/// Issue an authenticated `GET {url}` against PostgREST and map the ranking rows to a
/// [`Watchlist`]. Shared by [`fetch`] and [`fetch_candidates`]: the same `token` goes in BOTH
/// the `apikey` and `Authorization: Bearer` headers (see [`auth_token`]).
async fn get_ranking(
    client: &reqwest::Client,
    url: &str,
    token: &str,
) -> Result<(Watchlist, HashMap<WalletAddress, i64>), SupabaseError> {
    let resp = client
        .get(url)
        .header("apikey", token)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .map_err(SupabaseError::Transport)?;

    let status = resp.status();
    if !status.is_success() {
        return Err(SupabaseError::Status(status.as_u16()));
    }
    let rows: Vec<RankingRow> = resp.json().await.map_err(SupabaseError::Decode)?;
    Ok(to_watchlist(&rows))
}

/// Fetch the latest ranking from Supabase and map it to a [`Watchlist`] plus the last-trade
/// side-map (#357).
///
/// `GET {base_url}/rest/v1/latest_ranking?order=rank&limit={limit}` with the SAME token in
/// both the `apikey` and `Authorization: Bearer` headers (see [`auth_token`]). This is the
/// bootstrap + score-refresh path: it is intentionally NOT freshness-filtered (the bootstrap
/// admits the top-`limit` by rank; freshness for live wallets is enforced by the poll cursor +
/// maintenance tick, and the refresh must not drop live members). See [`fetch_candidates`].
pub async fn fetch(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    limit: usize,
) -> Result<(Watchlist, HashMap<WalletAddress, i64>), SupabaseError> {
    get_ranking(
        client,
        &latest_ranking_url(base_url, limit),
        auth_token(anon_key, secret_key),
    )
    .await
}

/// Build the PostgREST query string for [`fetch_candidates`]: the top-`n` `latest_ranking`
/// rows (with the [`RANKING_EXACT_SELECT`] score aliases, #514) excluding `exclude`,
/// optionally freshness-filtered, ordered by rank. Pure (no network)
/// so the `not.in.` and `gte` filters are unit-testable. Excluded wallets render as canonical
/// lowercase `0x` hex (matching `latest_ranking.wallet_hex`) and are sorted + deduped for a
/// deterministic, cache-friendly URL. An empty `exclude` omits that filter (PostgREST rejects
/// an empty `in.()` list).
///
/// `freshness_cutoff` (#357), when `Some(cutoff)`, appends `last_trade_unix=gte.{cutoff}` so
/// only wallets that traded at/after `cutoff` are returned. NULL `last_trade_unix` fails `gte`
/// and is excluded — a not-yet-populated bench pauses backfill, it never empties the live set.
fn candidates_query(exclude: &[WalletAddress], n: usize, freshness_cutoff: Option<i64>) -> String {
    let mut filters: Vec<String> = vec![format!("select={RANKING_EXACT_SELECT}")];
    if !exclude.is_empty() {
        let mut hexes: Vec<String> = exclude.iter().map(ToString::to_string).collect();
        hexes.sort_unstable();
        hexes.dedup();
        filters.push(format!("wallet_hex=not.in.({})", hexes.join(",")));
    }
    if let Some(cutoff) = freshness_cutoff {
        filters.push(format!("last_trade_unix=gte.{cutoff}"));
    }
    filters.push(format!("order=rank&limit={n}"));
    filters.join("&")
}

/// Fetch the top-`n` on-deck candidate wallets from `latest_ranking`, excluding any wallet in
/// `exclude` (the current live ∪ evicted set), ordered by rank. Used by the maintenance tick
/// (issue #350 WS1 PR-D) to backfill freed live slots from the Supabase bench.
///
/// `GET {base_url}/rest/v1/latest_ranking?select=<exact-aliases>&wallet_hex=not.in.(<exclude>)&last_trade_unix=gte.<cutoff>&order=rank&limit={n}`
/// with the same token in both headers (see [`auth_token`]). The server-side `not.in.` filter
/// is an over-fetch optimisation, not a correctness boundary: it is matched case-sensitively
/// against `latest_ranking.wallet_hex` (canonical lowercase), and
/// [`crate::live_watchlist::LiveWatchlist::replace`] independently dedups the results against
/// the live and evicted sets by byte-equality, so a casing miss cannot re-admit a wallet.
///
/// `now_unix` (unix seconds) anchors the candidate freshness gate (#357): only wallets whose
/// real last trade is within [`ACTIVE_WINDOW_HOURS`] of `now_unix` are returned, so a stale
/// bench wallet is never backfilled into the live set. Returns the [`Watchlist`] plus the
/// last-trade side-map (consumed by the PR-3 admission cursor-seed).
pub async fn fetch_candidates(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    exclude: &[WalletAddress],
    n: usize,
    now_unix: i64,
) -> Result<(Watchlist, HashMap<WalletAddress, i64>), SupabaseError> {
    let freshness_cutoff = now_unix - ACTIVE_WINDOW_HOURS * 3600;
    let url = format!(
        "{}/rest/v1/latest_ranking?{}",
        base_url.trim_end_matches('/'),
        candidates_query(exclude, n, Some(freshness_cutoff))
    );
    get_ranking(client, &url, auth_token(anon_key, secret_key)).await
}

/// Fetch the current (max) `batch_id` from `ranking_batches`, or `None` when no batch exists.
///
/// The maintenance tick (#350 WS1 PR-D) calls this each round to detect a fresh ranking push:
/// when the batch id changes it clears its in-memory evicted-set, so a wallet evicted under the
/// previous batch can be re-admitted once the ranker re-promotes it on new information. On any
/// error the caller keeps its current batch marker (no spurious clear).
///
/// `GET {base_url}/rest/v1/ranking_batches?select=batch_id&order=batch_id.desc&limit=1`.
pub async fn fetch_latest_batch_id(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
) -> Result<Option<i64>, SupabaseError> {
    let url = format!(
        "{}/rest/v1/ranking_batches?select=batch_id&order=batch_id.desc&limit=1",
        base_url.trim_end_matches('/')
    );
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
    #[derive(Deserialize)]
    struct BatchRow {
        batch_id: i64,
    }
    let rows: Vec<BatchRow> = resp.json().await.map_err(SupabaseError::Decode)?;
    Ok(rows.first().map(|r| r.batch_id))
}

/// Build the single-row PostgREST insert body for a `wallet_lifecycle_events` `demote` row.
/// Pure (no network) so the payload shape is unit-testable. `live_pnl` is emitted as a decimal
/// *string* — Postgres coerces text → `numeric`, so no `f64` ever touches the money column — and
/// `from_batch_id` is always `null` (the demotion is driven by live P&L, not a ranking batch).
///
/// `last_trade_unix` (#357) is the wallet's real last-trade time (its poll cursor at eviction =
/// the inactivity clock), recorded for the audit row; `None` (a never-polled wallet evicted on
/// another trigger) serializes as JSON `null`, never a sentinel.
fn lifecycle_demote_body(
    wallet_hex: &str,
    reason: &str,
    live_pnl: Option<Decimal>,
    trades_observed: i64,
    last_trade_unix: Option<i64>,
) -> serde_json::Value {
    serde_json::json!([{
        "wallet_hex": wallet_hex,
        "event": "demote",
        "reason": reason,
        "live_pnl": live_pnl.map(|d| d.to_string()),
        "trades_observed": trades_observed,
        "from_batch_id": serde_json::Value::Null,
        "last_trade_unix": last_trade_unix,
    }])
}

/// Append a best-effort `demote` audit row to `wallet_lifecycle_events`.
///
/// The service-role secret bypasses RLS (the table is RLS-enabled with no anon policy — see
/// `scripts/supabase_schema.sql`); with only the anon key this POST is rejected and the caller
/// logs + continues, since the eviction itself is already durable in the live set.
///
/// `POST {base_url}/rest/v1/wallet_lifecycle_events`.
#[allow(clippy::too_many_arguments)]
pub async fn write_lifecycle_event(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    wallet_hex: &str,
    reason: &str,
    live_pnl: Option<Decimal>,
    trades_observed: i64,
    last_trade_unix: Option<i64>,
) -> Result<(), SupabaseError> {
    let url = format!(
        "{}/rest/v1/wallet_lifecycle_events",
        base_url.trim_end_matches('/')
    );
    let token = auth_token(anon_key, secret_key);
    let resp = client
        .post(&url)
        .header("apikey", token)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .json(&lifecycle_demote_body(
            wallet_hex,
            reason,
            live_pnl,
            trades_observed,
            last_trade_unix,
        ))
        .send()
        .await
        .map_err(SupabaseError::Transport)?;
    let status = resp.status();
    if !status.is_success() {
        return Err(SupabaseError::Status(status.as_u16()));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    const HEX_A: &str = "0x0000000000000000000000000000000000000001";
    const HEX_B: &str = "0x0000000000000000000000000000000000000002";

    fn row(
        wallet_hex: &str,
        hit: serde_json::Value,
        tstat: serde_json::Value,
        n: Option<i64>,
    ) -> RankingRow {
        RankingRow {
            batch_id: None,
            rank: None,
            wallet_hex: wallet_hex.to_string(),
            hit_rate: Some(hit),
            ls_tstat: Some(tstat),
            n_trades: n,
            last_trade_unix: None,
            hit_rate_text: None,
            ls_tstat_text: None,
        }
    }

    #[test]
    fn hit_rate_number_maps_to_win_rate_bps() {
        let e = map_row(&row(HEX_A, json!(0.63), json!(2.5), Some(42))).unwrap();
        assert_eq!(e.win_rate_bps.0, 6300);
        assert_eq!(e.leader_score_bps.0, 2500);
        assert_eq!(e.closed_trades_in_window, 42);
        assert_eq!(e.tier, WatchlistTier::Active);
        assert_eq!(e.lcb_5pct_bps.0, 0);
    }

    #[test]
    fn hit_rate_string_maps_identically() {
        // PostgREST may quote numerics; the string path must match the number path.
        let e = map_row(&row(HEX_A, json!("0.63"), json!("2.5"), Some(42))).unwrap();
        assert_eq!(e.win_rate_bps.0, 6300);
        assert_eq!(e.leader_score_bps.0, 2500);
    }

    #[test]
    fn null_or_zero_numerics_fall_back_to_zero() {
        let mut r = row(HEX_A, json!(null), json!(null), None);
        r.hit_rate = None;
        r.ls_tstat = None;
        let e = map_row(&r).unwrap();
        assert_eq!(e.win_rate_bps.0, 0);
        assert_eq!(e.leader_score_bps.0, 0);
        assert_eq!(e.closed_trades_in_window, 0);

        let e0 = map_row(&row(HEX_A, json!(0), json!(0), Some(0))).unwrap();
        assert_eq!(e0.win_rate_bps.0, 0);
        assert_eq!(e0.leader_score_bps.0, 0);
    }

    #[test]
    fn win_rate_is_clamped_to_basis_point_range() {
        // A degenerate hit_rate above 1.0 must not exceed 10_000 bps (Kelly p ≤ 1).
        let e = map_row(&row(HEX_A, json!(1.5), json!(0), Some(1))).unwrap();
        assert_eq!(e.win_rate_bps.0, 10_000);
    }

    #[test]
    fn bad_hex_row_is_skipped() {
        assert!(map_row(&row("not-a-wallet", json!(0.5), json!(1.0), Some(1))).is_none());
        assert!(map_row(&row("0x123", json!(0.5), json!(1.0), Some(1))).is_none());
    }

    #[test]
    fn to_watchlist_collects_valid_rows_only() {
        let rows = vec![
            row(HEX_A, json!(0.6), json!(2.0), Some(10)),
            row("bad", json!(0.6), json!(2.0), Some(10)),
        ];
        let (wl, last_trade) = to_watchlist(&rows);
        assert_eq!(wl.entries.len(), 1);
        assert_eq!(wl.active_count, 1);
        assert_eq!(wl.incubator_count, 0);
        // No row carried a last_trade_unix, so the side-map is empty (#357).
        assert!(last_trade.is_empty());
    }

    #[test]
    fn auth_token_prefers_secret_for_both_headers() {
        // Both set -> secret (sent in BOTH apikey + Bearer; mixing 401s with `sb_` keys).
        assert_eq!(auth_token("anon", "secret"), "secret");
        // No secret -> fall back to the publishable/anon key for both headers.
        assert_eq!(auth_token("anon", ""), "anon");
        assert_eq!(auth_token("", "secret"), "secret");
        assert_eq!(auth_token("", ""), "");
    }

    const EXACT_SELECT: &str =
        "select=*,hit_rate_text:hit_rate::text,ls_tstat_text:ls_tstat::text";

    #[test]
    fn candidates_query_empty_exclude_omits_filter() {
        // PostgREST rejects an empty `in.()`; with nothing to exclude this is a plain top-n.
        assert_eq!(
            candidates_query(&[], 5, None),
            format!("{EXACT_SELECT}&order=rank&limit=5")
        );
    }

    #[test]
    fn candidates_query_excludes_sorted_lowercase_deduped() {
        let a = WalletAddress::from_hex("0x00000000000000000000000000000000000000AA").unwrap();
        let b = WalletAddress::from_hex("0x0000000000000000000000000000000000000001").unwrap();
        // Out of order + a duplicate + upper-case input -> sorted, deduped, lowercase output.
        let q = candidates_query(&[a, b, a], 3, None);
        assert_eq!(
            q,
            format!(
                "{EXACT_SELECT}&wallet_hex=not.in.(0x0000000000000000000000000000000000000001,\
                 0x00000000000000000000000000000000000000aa)&order=rank&limit=3"
            )
        );
    }

    #[test]
    fn candidates_query_appends_freshness_filter() {
        // No exclude + a cutoff -> the gte filter precedes order/limit (#357).
        assert_eq!(
            candidates_query(&[], 5, Some(1_000)),
            format!("{EXACT_SELECT}&last_trade_unix=gte.1000&order=rank&limit=5")
        );
    }

    #[test]
    fn candidates_query_combines_exclude_and_freshness() {
        let a = WalletAddress::from_hex(HEX_A).unwrap();
        assert_eq!(
            candidates_query(&[a], 3, Some(1_000)),
            format!(
                "{EXACT_SELECT}&wallet_hex=not.in.(0x0000000000000000000000000000000000000001)\
                 &last_trade_unix=gte.1000&order=rank&limit=3"
            )
        );
    }

    #[test]
    fn every_latest_ranking_read_carries_the_exact_aliases() {
        // All three read sites (#514): the shared ordinary/canary URL and the candidates
        // query request the exact `::text` score projections.
        assert_eq!(
            latest_ranking_url("https://example.test/", 25),
            format!("https://example.test/rest/v1/latest_ranking?{EXACT_SELECT}&order=rank&limit=25")
        );
        assert!(candidates_query(&[], 5, None).starts_with(EXACT_SELECT));
    }

    #[test]
    fn exact_alias_beats_the_f64_value_path_at_the_bps_boundary() {
        // Kelly `p` boundary (#514): the exact column value rounds to 1 bps, but the same
        // value through serde_json's f64 number arrives as 0.00015 and rounds to 2 bps.
        const BOUNDARY: &str = "0.000149999999999999999999";
        let mut exact = row(HEX_A, json!(null), json!(null), Some(1));
        exact.hit_rate_text = Some(BOUNDARY.to_string());
        assert_eq!(map_row(&exact).unwrap().win_rate_bps.0, 1);
        let lossy = row(
            HEX_A,
            serde_json::from_str::<serde_json::Value>(BOUNDARY).unwrap(),
            json!(null),
            Some(1),
        );
        assert_eq!(
            map_row(&lossy).unwrap().win_rate_bps.0,
            2,
            "the alias-free Value path is f64-lossy — the aliases are load-bearing"
        );

        // `ls_tstat` boundary: exact 1.4999…e-3 × 1000 rounds to 1; via f64 it becomes
        // 0.0015 × 1000 = 1.5 and banker's-rounds to 2, flipping watchlist ordering.
        const TSTAT_BOUNDARY: &str = "0.001499999999999999999999";
        let mut exact_t = row(HEX_A, json!(null), json!(null), Some(1));
        exact_t.ls_tstat_text = Some(TSTAT_BOUNDARY.to_string());
        assert_eq!(map_row(&exact_t).unwrap().leader_score_bps.0, 1);
        let lossy_t = row(
            HEX_A,
            json!(null),
            serde_json::from_str::<serde_json::Value>(TSTAT_BOUNDARY).unwrap(),
            Some(1),
        );
        assert_eq!(map_row(&lossy_t).unwrap().leader_score_bps.0, 2);
    }

    #[test]
    fn alias_absent_rows_fall_back_to_the_recorded_body_path() {
        // Previously recorded (canary) bodies carry no aliases and must keep decoding.
        let absent: RankingRow = serde_json::from_value(
            json!({"wallet_hex": HEX_A, "hit_rate": 0.63, "ls_tstat": 2.5, "n_trades": 42}),
        )
        .unwrap();
        assert_eq!(absent.hit_rate_text, None);
        let entry = map_row(&absent).unwrap();
        assert_eq!(entry.win_rate_bps.0, 6300);
        assert_eq!(entry.leader_score_bps.0, 2500);
        // An aliased body prefers the text cells over the numbers beside them.
        let aliased: RankingRow = serde_json::from_value(json!({
            "wallet_hex": HEX_A,
            "hit_rate": 0.63,
            "ls_tstat": 2.5,
            "n_trades": 42,
            "hit_rate_text": "0.63",
            "ls_tstat_text": "2.5"
        }))
        .unwrap();
        let entry = map_row(&aliased).unwrap();
        assert_eq!(entry.win_rate_bps.0, 6300);
        assert_eq!(entry.leader_score_bps.0, 2500);
    }

    #[test]
    fn ranking_row_last_trade_unix_deser_int_null_absent() {
        // PostgREST sends the column as an int, JSON null, or omits it (serde default) → None.
        let present: RankingRow = serde_json::from_value(
            json!({"wallet_hex": HEX_A, "last_trade_unix": 1_700_000_000_i64}),
        )
        .unwrap();
        assert_eq!(present.last_trade_unix, Some(1_700_000_000));
        let null: RankingRow =
            serde_json::from_value(json!({"wallet_hex": HEX_A, "last_trade_unix": null})).unwrap();
        assert_eq!(null.last_trade_unix, None);
        let absent: RankingRow = serde_json::from_value(json!({"wallet_hex": HEX_A})).unwrap();
        assert_eq!(absent.last_trade_unix, None);
    }

    #[test]
    fn to_watchlist_builds_last_trade_map_for_valid_rows_with_ts() {
        let mut with_ts = row(HEX_A, json!(0.6), json!(2.0), Some(10));
        with_ts.last_trade_unix = Some(1_700_000_000);
        let no_ts = row(HEX_B, json!(0.6), json!(2.0), Some(10)); // valid hex, no last_trade_unix
        let mut bad = row("bad", json!(0.6), json!(2.0), Some(10));
        bad.last_trade_unix = Some(999); // bad hex -> excluded from BOTH entries and the map
        let (wl, last_trade) = to_watchlist(&[with_ts, no_ts, bad]);
        assert_eq!(wl.entries.len(), 2); // HEX_A + HEX_B valid; bad skipped
        assert_eq!(last_trade.len(), 1); // only the valid row carrying a ts
        let wa = WalletAddress::from_hex(HEX_A).unwrap();
        assert_eq!(last_trade.get(&wa), Some(&1_700_000_000)); // keyed by the validated wallet
    }

    #[test]
    fn lifecycle_body_is_single_row_demote_with_null_batch() {
        let body = lifecycle_demote_body(
            HEX_A,
            "inactive>72h",
            Some(dec!(-12.5)),
            14,
            Some(1_700_000_000),
        );
        let arr = body.as_array().expect("body is a JSON array");
        assert_eq!(arr.len(), 1, "single-row insert");
        let entry = &arr[0];
        assert_eq!(entry["wallet_hex"], json!(HEX_A));
        assert_eq!(entry["event"], json!("demote"));
        assert_eq!(entry["reason"], json!("inactive>72h"));
        // Money serialized as a decimal string (text -> numeric coercion), never f64.
        assert_eq!(entry["live_pnl"], json!("-12.5"));
        assert_eq!(entry["trades_observed"], json!(14));
        assert!(
            entry["from_batch_id"].is_null(),
            "from_batch_id is always null"
        );
        // The real last-trade time (the inactivity clock) is recorded for the audit (#357).
        assert_eq!(entry["last_trade_unix"], json!(1_700_000_000));
    }

    #[test]
    fn lifecycle_body_emits_null_pnl_and_last_trade_when_absent() {
        let body = lifecycle_demote_body(HEX_A, "inactive>72h", None, 0, None);
        let entry = &body.as_array().unwrap()[0];
        assert!(
            entry["live_pnl"].is_null(),
            "absent realized P&L serializes as null, not 0"
        );
        assert!(
            entry["last_trade_unix"].is_null(),
            "absent last-trade time serializes as null, not a sentinel (#357)"
        );
    }

    #[derive(Clone)]
    struct CanaryRankingFixture {
        created_at: String,
        last_trade_unix: i64,
    }

    async fn latest_ranking(
        axum::extract::State(state): axum::extract::State<CanaryRankingFixture>,
    ) -> axum::Json<serde_json::Value> {
        axum::Json(json!([{
            "batch_id": 7,
            "rank": 1,
            "wallet_hex": HEX_A,
            "hit_rate": "0.63",
            "ls_tstat": "2.5",
            "n_trades": 42,
            "last_trade_unix": state.last_trade_unix
        }]))
    }

    async fn ranking_batch(
        axum::extract::State(state): axum::extract::State<CanaryRankingFixture>,
    ) -> axum::Json<serde_json::Value> {
        axum::Json(json!([{"batch_id": 7, "created_at": state.created_at}]))
    }

    async fn canary_ranking_server(created_at: OffsetDateTime) -> String {
        let state = CanaryRankingFixture {
            created_at: created_at.format(&Rfc3339).unwrap(),
            last_trade_unix: OffsetDateTime::now_utc().unix_timestamp(),
        };
        let app = axum::Router::new()
            .route(
                "/rest/v1/latest_ranking",
                axum::routing::get(latest_ranking),
            )
            .route(
                "/rest/v1/ranking_batches",
                axum::routing::get(ranking_batch),
            )
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn canary_ranking_requires_a_fresh_matching_batch() {
        let base = canary_ranking_server(OffsetDateTime::now_utc()).await;
        let (watchlist, cursors, evidence) =
            fetch_canary_observed(&reqwest::Client::new(), &base, "anon", 100)
                .await
                .unwrap();
        assert_eq!(watchlist.entries.len(), 1);
        assert_eq!(cursors.len(), 1);
        assert_eq!(evidence.len(), 2);
    }

    #[tokio::test]
    async fn canary_ranking_rejects_a_stale_batch() {
        let base = canary_ranking_server(
            OffsetDateTime::now_utc() - time::Duration::seconds(CANARY_RANKING_MAX_AGE_SECS + 1),
        )
        .await;
        assert!(matches!(
            fetch_canary_observed(&reqwest::Client::new(), &base, "anon", 100).await,
            Err(SupabaseError::CanaryObserved { .. })
        ));
    }

    #[tokio::test]
    async fn canary_ranking_non_success_retains_response_evidence() {
        let app = axum::Router::new().route(
            "/rest/v1/latest_ranking",
            axum::routing::get(|| async {
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    [("retry-after", "9")],
                    "unavailable",
                )
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let error = fetch_canary_observed(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            "anon",
            100,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SupabaseError::CanaryObserved { .. }));
        let SupabaseError::CanaryObserved { attempts, .. } = error else {
            return;
        };
        assert!(matches!(attempts.as_slice(), [RawHttpAttempt::Response(_)]));
    }

    #[tokio::test]
    async fn canary_ranking_transport_failure_retains_request_identity() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let error = fetch_canary_observed(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            "anon",
            100,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SupabaseError::CanaryObserved { .. }));
        let SupabaseError::CanaryObserved { attempts, .. } = error else {
            return;
        };
        assert!(matches!(
            attempts.as_slice(),
            [RawHttpAttempt::TransportFailure(_)]
        ));
        let [RawHttpAttempt::TransportFailure(failure)] = attempts.as_slice() else {
            return;
        };
        assert_eq!(failure.path, "/rest/v1/latest_ranking");
        assert_eq!(failure.attempt_ordinal, 1);
        assert!(failure.received_at >= failure.observed_at);
    }
}
