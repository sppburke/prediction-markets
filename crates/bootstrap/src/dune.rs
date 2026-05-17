//! Dune Analytics API client for wallet discovery.
//!
//! Workflow: POST /api/v1/sql/execute → poll execution until complete → parse wallet rows.
//! Uses the direct SQL execution endpoint — no stored query is created, so the
//! per-account private-query quota is never touched.
//! Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" table.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use pe_core_types::WalletAddress;
use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Iso8601;

use crate::error::BootstrapError;

// Defaults — canonical values live in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
const BASE_URL: &str = "https://api.dune.com/api/v1";
const POLL_INTERVAL_SECS: u64 = 3;
const MAX_WAIT_SECS: u64 = 300;
const HTTP_TIMEOUT_SECS: u64 = 30;
const TABLE_UPLOAD_TIMEOUT_SECS: u64 = 120;

/// Stable name for the per-namespace market-ID lookup table used by the JOIN resolution path.
const RESOLUTION_TABLE: &str = "pe_resolution_lookup";

/// SQL for wallet discovery: wallets with strong win rates on resolved binary markets,
/// active within a configurable recent window, and entering close to resolution.
///
/// DuneSQL (Trino) notes:
/// - `condition_id` in `market_trades` and `conditionid` in `ctf_evt_conditionresolution`
///   are both `varbinary` — compared directly, no casting needed.
/// - `payoutnumerators[1] > 0` identifies the winning outcome for binary (2-slot) markets.
/// - Counts unique resolved markets per wallet (not raw trade rows) so `closed_markets`
///   is a cleaner "number of distinct bets" metric.
/// - `avg_hours_entry_to_resolution` is the mean time (hours) from a wallet's first trade on
///   a condition to when that condition resolved; only winning conditions with positive time
///   deltas contribute.
///
/// Look-ahead invariant: every reference to a trade or resolution is bounded by
/// `{as_of}`. Three forward-looking surfaces are explicitly fenced:
///   1. `resolved` CTE — only resolutions strictly before `{as_of}`.
///   2. `recently_active` CTE — only trades in `[as_of - active_window, as_of)`.
///   3. `wallet_condition` join — only trades strictly before `{as_of}`.
///
/// Without all three, a snapshot taken "as of" a past date would still leak future
/// market outcomes through the win-rate calculation.
///
/// Placeholders filled at runtime:
///   `{as_of}` (Trino TIMESTAMP literal, e.g. `TIMESTAMP '2026-03-08 00:00:00'`),
///   `{min_closed_markets}`, `{min_win_rate}` (decimal, e.g. "0.95"),
///   `{active_window_days}`, `{max_avg_hours_to_resolution}`.
const WALLET_DISCOVERY_SQL: &str = "\
WITH resolved AS (\
  SELECT \
    conditionid, \
    CASE WHEN payoutnumerators[1] > 0 THEN 'Yes' ELSE 'No' END AS winning_outcome, \
    evt_block_time AS resolved_at \
  FROM polymarket_polygon.ctf_evt_conditionresolution \
  WHERE outcomeslotcount = 2 \
    AND evt_block_time < {as_of} \
), \
recently_active AS (\
  SELECT DISTINCT maker \
  FROM polymarket_polygon.market_trades \
  WHERE block_time >= {as_of} - INTERVAL '{active_window_days}' DAY \
    AND block_time < {as_of} \
    AND maker IS NOT NULL \
), \
wallet_condition AS (\
  SELECT \
    t.maker AS wallet, \
    r.conditionid, \
    r.winning_outcome, \
    r.resolved_at, \
    MIN(t.block_time) AS first_trade_time, \
    MAX(CASE WHEN t.token_outcome = r.winning_outcome THEN 1 ELSE 0 END) AS on_winning_side \
  FROM polymarket_polygon.market_trades t \
  JOIN resolved r ON t.condition_id = r.conditionid \
  JOIN recently_active ra ON t.maker = ra.maker \
  WHERE t.maker IS NOT NULL \
    AND t.block_time < {as_of} \
  GROUP BY t.maker, r.conditionid, r.winning_outcome, r.resolved_at \
), \
wallet_stats AS (\
  SELECT \
    wallet, \
    COUNT(*) AS closed_markets, \
    SUM(on_winning_side) AS winning_markets, \
    AVG(\
      CASE WHEN on_winning_side = 1 AND resolved_at > first_trade_time \
      THEN CAST(date_diff('minute', first_trade_time, resolved_at) AS DOUBLE) / 60.0 \
      ELSE NULL END \
    ) AS avg_hours_entry_to_resolution \
  FROM wallet_condition \
  GROUP BY wallet \
) \
SELECT CAST(wallet AS VARCHAR) AS wallet \
FROM wallet_stats \
WHERE closed_markets > {min_closed_markets} \
  AND winning_markets > 0 \
  AND (1.0 * winning_markets / closed_markets) > {min_win_rate} \
  AND avg_hours_entry_to_resolution < {max_avg_hours_to_resolution}";

/// SQL for market resolution fetch: all binary-market resolutions settled after
/// `{last_resolved_at}` Unix seconds. Timestamp-bounded for incremental runs.
///
/// Winner encoding matches Gamma convention:
///   0 = YES (outcome index 0) won, 1 = NO (outcome index 1) won, NULL = voided.
///
/// `CAST(conditionid AS VARCHAR)` in DuneSQL returns a `\x`-prefixed lowercase hex string
/// (e.g. `\x0aff…`). `parse_resolution_rows` normalises this to `0x`-prefixed for
/// consistency with the trade cache.
///
/// Placeholder: `{last_resolved_at}` — Unix seconds (0 on first run).
/// Used as the fallback path when no namespace is configured.
/// Multi-outcome markets surface as rows with `winning_outcome_id = NULL` so the
/// `evt_block_time` (precise resolution timestamp) lands in the cache and the
/// backtest's NULL-winner filter then excludes them from `ResolutionIndex`.
/// Tied binary payouts (`[1, 1]`) also resolve to NULL — mirrors the
/// unique-non-zero rule in `polygon_ctf::decode_resolution_log`.
const RESOLUTION_SQL: &str = "\
SELECT \
  CAST(conditionid AS VARCHAR) AS condition_id, \
  CASE \
    WHEN outcomeslotcount = 2 \
      AND payoutnumerators[1] > 0 \
      AND payoutnumerators[2] = 0 THEN 0 \
    WHEN outcomeslotcount = 2 \
      AND payoutnumerators[1] = 0 \
      AND payoutnumerators[2] > 0 THEN 1 \
    ELSE NULL \
  END AS winning_outcome_id, \
  CAST(TO_UNIXTIME(evt_block_time) AS BIGINT) AS resolved_at_unix \
FROM polymarket_polygon.ctf_evt_conditionresolution \
WHERE evt_block_time > FROM_UNIXTIME({last_resolved_at})";

/// Render the JOIN-based resolution SQL that queries only the caller's markets.
///
/// Before calling this, upload market IDs via [`DuneClient::upload_market_ids`] to
/// create `dune.{namespace}.pe_resolution_lookup`. The INNER JOIN restricts Dune to
/// returning only rows that match the uploaded condition IDs — dramatically reducing
/// the result set compared to scanning all resolved binary markets.
///
/// `TO_HEX(conditionid)` returns lowercase hex without a prefix; prepending `'0x'`
/// and lowercasing both sides produces a stable join key matching our `0x<hex>` cache format.
pub(crate) fn render_resolution_sql_with_join(namespace: &str, last_resolved_at: i64) -> String {
    // Winner extraction mirrors [`RESOLUTION_SQL`]: binary unique-non-zero only;
    // multi-outcome and tied-payout rows return `winning_outcome_id = NULL` so
    // their precise `evt_block_time` lands in the cache while the backtest's
    // NULL-winner filter excludes them from `ResolutionIndex`.
    format!(
        "SELECT DISTINCT \
           m.condition_id, \
           CASE \
             WHEN r.outcomeslotcount = 2 \
               AND r.payoutnumerators[1] > 0 \
               AND r.payoutnumerators[2] = 0 THEN 0 \
             WHEN r.outcomeslotcount = 2 \
               AND r.payoutnumerators[1] = 0 \
               AND r.payoutnumerators[2] > 0 THEN 1 \
             ELSE NULL \
           END AS winning_outcome_id, \
           CAST(TO_UNIXTIME(r.evt_block_time) AS BIGINT) AS resolved_at_unix \
         FROM dune.{namespace}.{RESOLUTION_TABLE} m \
         INNER JOIN polymarket_polygon.ctf_evt_conditionresolution r \
           ON '0x' || LOWER(TO_HEX(r.conditionid)) = m.condition_id \
         WHERE r.evt_block_time > FROM_UNIXTIME({last_resolved_at})"
    )
}

/// Render `as_of` as a Trino-compatible `TIMESTAMP 'YYYY-MM-DD HH:MM:SS'` literal in UTC.
///
/// Trino accepts SQL standard timestamp literals to second precision; we drop sub-second
/// fractions to keep the string short and predictable for snapshot tests.
fn format_as_of(at: OffsetDateTime) -> String {
    // Convert to UTC and truncate to the second.
    let utc = at.to_offset(time::UtcOffset::UTC);
    // Iso8601::DEFAULT uses "T" separator; Trino accepts "T" or " ". Use space for readability.
    // Format manually: YYYY-MM-DD HH:MM:SS.
    let _ = Iso8601::DEFAULT; // keep import live in case of future formatting changes
    format!(
        "TIMESTAMP '{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
        utc.year(),
        u8::from(utc.month()),
        utc.day(),
        utc.hour(),
        utc.minute(),
        utc.second(),
    )
}

// ── JSON DTOs ─────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ExecuteResponse {
    execution_id: String,
}

#[derive(Deserialize)]
struct ResultsResponse {
    state: String,
    #[serde(default)]
    result: Option<ResultBody>,
    #[serde(default)]
    error: Option<DuneErrorBody>,
}

#[derive(Deserialize)]
struct ResultBody {
    rows: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct DuneErrorBody {
    #[serde(default)]
    message: String,
}

// ── Client ────────────────────────────────────────────────────────────────────

/// HTTP client for the Dune Analytics API v1.
pub struct DuneClient {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
}

impl DuneClient {
    pub fn new(api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
            base_url: BASE_URL.to_owned(),
        }
    }

    /// Override base URL — used in tests to point at a mock server.
    #[cfg(test)]
    pub fn with_base_url(mut self, url: &str) -> Self {
        self.base_url = url.to_owned();
        self
    }

    /// Discover wallet addresses from Polymarket trade history via Dune.
    ///
    /// `as_of` is the cutoff timestamp: only trades and resolutions strictly before
    /// `as_of` participate in the query. For the live bootstrap path pass
    /// `OffsetDateTime::now_utc()`; for historical seeding pass a past timestamp.
    ///
    /// Returns wallets satisfying all four quality filters:
    /// - more than `min_closed_markets` distinct resolved binary markets traded
    /// - win rate above `min_win_rate_pct` percent on those markets
    /// - at least one trade (on any market) within `active_window_days` days of `as_of`
    /// - average hours from first entry to market resolution below `max_avg_hours_to_resolution`
    ///
    /// No hard limit on result count; all qualifying wallets are returned.
    pub async fn discover_wallets(
        &self,
        as_of: OffsetDateTime,
        min_closed_markets: u32,
        min_win_rate_pct: u32,
        active_window_days: u32,
        max_avg_hours_to_resolution: u32,
    ) -> Result<Vec<WalletAddress>, BootstrapError> {
        let sql = render_wallet_discovery_sql(
            as_of,
            min_closed_markets,
            min_win_rate_pct,
            active_window_days,
            max_avg_hours_to_resolution,
        );
        let execution_id = self.execute_sql(&sql).await?;
        let rows = self.wait_for_results(&execution_id).await?;
        parse_wallet_rows(rows)
    }

    /// Fetch binary-market resolutions settled after `last_resolved_at` (Unix seconds).
    ///
    /// When `namespace` is `Some`, uploads `wanted` as a Dune lookup table and queries
    /// with a server-side INNER JOIN so only the caller's markets are returned — avoiding
    /// a full scan of all resolved binary markets. When `None`, falls back to a broad scan
    /// with client-side filtering.
    ///
    /// Returns `(market_id, winner, resolved_at_unix)` for each resolved market in `wanted`.
    /// Winner: `Some(0)` = YES won, `Some(1)` = NO won, `None` = voided.
    pub async fn fetch_resolutions(
        &self,
        wanted: &HashSet<String>,
        last_resolved_at: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<(String, Option<u16>, i64)>, BootstrapError> {
        if let Some(ns) = namespace {
            self.upload_market_ids(ns, wanted).await?;
            let sql = render_resolution_sql_with_join(ns, last_resolved_at);
            let execution_id = self.execute_sql(&sql).await?;
            let rows = self.wait_for_results(&execution_id).await?;
            Ok(parse_resolution_rows(rows))
        } else {
            let sql = render_resolution_sql(last_resolved_at);
            let execution_id = self.execute_sql(&sql).await?;
            let rows = self.wait_for_results(&execution_id).await?;
            let parsed = parse_resolution_rows(rows);
            Ok(parsed
                .into_iter()
                .filter(|(id, _, _)| wanted.contains(id.as_str()))
                .collect())
        }
    }

    /// Upload market condition IDs to a Dune user table for the JOIN resolution path.
    ///
    /// Thin wrapper over [`Self::upload_table`] preserving the call site signature.
    async fn upload_market_ids(
        &self,
        namespace: &str,
        ids: &HashSet<String>,
    ) -> Result<(), BootstrapError> {
        let rows: Vec<&str> = ids.iter().map(String::as_str).collect();
        self.upload_table(
            namespace,
            RESOLUTION_TABLE,
            "condition_id",
            rows.iter().copied(),
        )
        .await
    }

    /// Replace a single-VARCHAR-column Dune user table with the given rows.
    ///
    /// Flow: DELETE (ignore 404) → CREATE → INSERT CSV. Dune requires an explicit
    /// CREATE before INSERT; DELETE + re-CREATE ensures repeated runs replace
    /// stale rows rather than accumulating them.
    pub async fn upload_table<'a, I>(
        &self,
        namespace: &str,
        table_name: &str,
        col_name: &str,
        rows: I,
    ) -> Result<(), BootstrapError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        // 1. Delete any existing table (ignore errors — 404 on first run is expected).
        let table_url = format!("{}/table/{namespace}/{table_name}", self.base_url);
        let _ = self
            .client
            .delete(&table_url)
            .header("X-DUNE-API-KEY", &self.api_key)
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .send()
            .await;

        // 2. Create the table with a single varchar column.
        #[derive(serde::Serialize)]
        struct ColDef<'a> {
            name: &'a str,
            #[serde(rename = "type")]
            ty: &'static str,
        }
        #[derive(serde::Serialize)]
        struct CreateBody<'a> {
            namespace: String,
            table_name: &'a str,
            schema: Vec<ColDef<'a>>,
            is_private: bool,
        }
        let create_body = CreateBody {
            namespace: namespace.to_owned(),
            table_name,
            schema: vec![ColDef {
                name: col_name,
                ty: "varchar",
            }],
            is_private: false,
        };
        let create_url = format!("{}/table/create", self.base_url);
        let create_resp = self
            .client
            .post(&create_url)
            .header("X-DUNE-API-KEY", &self.api_key)
            .json(&create_body)
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .send()
            .await
            .map_err(|e| BootstrapError::Dune {
                message: format!("upload_table CREATE {table_name} failed: {e}"),
            })?;
        let create_status = create_resp.status().as_u16();
        if create_status >= 400 {
            let bytes = create_resp.bytes().await.unwrap_or_default();
            return Err(BootstrapError::Dune {
                message: format!(
                    "upload_table CREATE {table_name} HTTP {create_status}: {}",
                    String::from_utf8_lossy(&bytes)
                ),
            });
        }

        // 3. Insert all rows as CSV.
        let mut csv = String::new();
        csv.push_str(col_name);
        csv.push('\n');
        let mut count = 0usize;
        for row in rows {
            csv.push_str(row);
            csv.push('\n');
            count += 1;
        }

        let insert_url = format!("{}/table/{namespace}/{table_name}/insert", self.base_url);
        let resp = self
            .client
            .post(&insert_url)
            .header("X-DUNE-API-KEY", &self.api_key)
            .header("Content-Type", "text/csv")
            .body(csv)
            .timeout(Duration::from_secs(TABLE_UPLOAD_TIMEOUT_SECS))
            .send()
            .await
            .map_err(|e| BootstrapError::Dune {
                message: format!("upload_table INSERT {table_name} failed: {e}"),
            })?;

        let status = resp.status().as_u16();
        if status >= 400 {
            let bytes = resp.bytes().await.unwrap_or_default();
            return Err(BootstrapError::Dune {
                message: format!(
                    "upload_table INSERT {table_name} HTTP {status}: {}",
                    String::from_utf8_lossy(&bytes)
                ),
            });
        }

        tracing::info!(
            count,
            namespace,
            table = table_name,
            "dune: lookup table uploaded"
        );
        Ok(())
    }

    /// Run incremental wallet discovery against `polymarket_polygon.market_trades_raw`.
    ///
    /// Anti-joins against the user-uploaded `known_wallets` table in `namespace`;
    /// returns only NEW makers active since `last_run_unix` that have at least
    /// `min_trades_per_wallet` trades in `market_trades_raw`.
    ///
    /// Output rows: `(wallet_hex, first_seen_at_unix, dune_trade_count)`. The
    /// `wallet_hex` is canonical: `"0x" + 40 lowercase hex chars`. The Dune SQL
    /// uses `LOWER(CAST(maker AS VARCHAR))`; addresses on Polygon are
    /// case-insensitive but Dune may store mixed-case so the cast is mandatory.
    ///
    /// # Precondition
    /// The caller MUST have called [`Self::upload_table`] with the current pile's
    /// wallet set into `<namespace>.<known_table_name>` before invoking this method —
    /// otherwise the anti-join returns the full universe of makers.
    pub async fn run_discovery(
        &self,
        namespace: &str,
        known_table_name: &str,
        last_run_unix: i64,
        min_trades_per_wallet: i64,
    ) -> Result<Vec<(String, i64, i64)>, BootstrapError> {
        let sql = render_discovery_sql(
            namespace,
            known_table_name,
            last_run_unix,
            min_trades_per_wallet,
        );
        let execution_id = self.execute_sql(&sql).await?;
        let rows = self.wait_for_results(&execution_id).await?;
        Ok(parse_discovery_rows(rows))
    }

    /// Submit SQL for direct execution without creating a stored query.
    ///
    /// Uses POST /api/v1/sql/execute — no private query is created so the
    /// per-account private-query quota is never consumed.
    async fn execute_sql(&self, sql: &str) -> Result<String, BootstrapError> {
        #[derive(serde::Serialize)]
        struct Body<'a> {
            sql: &'a str,
            performance: &'a str,
        }

        let url = format!("{}/sql/execute", self.base_url);
        let body = Body {
            sql,
            performance: "free",
        };

        let resp = self
            .client
            .post(&url)
            .header("X-DUNE-API-KEY", &self.api_key)
            .json(&body)
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .send()
            .await
            .map_err(|e| BootstrapError::Dune {
                message: format!("execute_sql POST failed: {e}"),
            })?;

        let status = resp.status().as_u16();
        let bytes = resp.bytes().await.map_err(|e| BootstrapError::Dune {
            message: format!("execute_sql read body failed: {e}"),
        })?;

        if status >= 400 {
            return Err(BootstrapError::Dune {
                message: format!(
                    "execute_sql HTTP {status}: {}",
                    String::from_utf8_lossy(&bytes)
                ),
            });
        }

        let parsed: ExecuteResponse =
            serde_json::from_slice(&bytes).map_err(|e| BootstrapError::Dune {
                message: format!("execute_sql parse failed: {e}"),
            })?;
        Ok(parsed.execution_id)
    }

    async fn wait_for_results(
        &self,
        execution_id: &str,
    ) -> Result<Vec<serde_json::Value>, BootstrapError> {
        let url = format!("{}/execution/{execution_id}/results", self.base_url);
        let deadline = Instant::now() + Duration::from_secs(MAX_WAIT_SECS);
        let poll_interval = Duration::from_secs(POLL_INTERVAL_SECS);

        loop {
            if Instant::now() > deadline {
                return Err(BootstrapError::DuneTimeout {
                    execution_id: execution_id.to_owned(),
                    secs: MAX_WAIT_SECS,
                });
            }

            let poll_result = async {
                let resp = self
                    .client
                    .get(&url)
                    .header("X-DUNE-API-KEY", &self.api_key)
                    .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
                    .send()
                    .await
                    .map_err(|e| format!("poll GET failed: {e}"))?;
                let status = resp.status().as_u16();
                let bytes = resp
                    .bytes()
                    .await
                    .map_err(|e| format!("poll read body failed: {e}"))?;
                Ok::<_, String>((status, bytes))
            }
            .await;

            let (status, bytes) = match poll_result {
                Ok(v) => v,
                Err(e) => {
                    // Transient network error — log and retry after back-off.
                    tracing::warn!(execution_id, error = %e, "dune: transient poll error, retrying");
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
            };

            if status >= 400 {
                return Err(BootstrapError::Dune {
                    message: format!("poll HTTP {status}: {}", String::from_utf8_lossy(&bytes)),
                });
            }

            let parsed: ResultsResponse =
                serde_json::from_slice(&bytes).map_err(|e| BootstrapError::Dune {
                    message: format!("poll parse failed: {e}"),
                })?;

            match parsed.state.as_str() {
                "QUERY_STATE_COMPLETED" | "QUERY_STATE_COMPLETED_PARTIAL" => {
                    let rows = parsed.result.map(|r| r.rows).unwrap_or_default();
                    return Ok(rows);
                }
                "QUERY_STATE_FAILED" | "QUERY_STATE_CANCELLED" => {
                    let msg = parsed
                        .error
                        .map(|e| e.message)
                        .unwrap_or_else(|| "unknown".to_owned());
                    return Err(BootstrapError::DuneExecutionFailed {
                        state: parsed.state,
                        message: msg,
                    });
                }
                // QUERY_STATE_PENDING | QUERY_STATE_EXECUTING — keep polling
                _ => {
                    tracing::debug!(state = %parsed.state, execution_id, "dune: still executing");
                    tokio::time::sleep(poll_interval).await;
                }
            }
        }
    }
}

// ── SQL rendering ─────────────────────────────────────────────────────────────

/// Substitute every placeholder in [`WALLET_DISCOVERY_SQL`].
///
/// Exposed at crate level so unit tests can snapshot the rendered SQL without
/// touching the network.
pub(crate) fn render_wallet_discovery_sql(
    as_of: OffsetDateTime,
    min_closed_markets: u32,
    min_win_rate_pct: u32,
    active_window_days: u32,
    max_avg_hours_to_resolution: u32,
) -> String {
    let win_rate_decimal = format!("{}.{:02}", min_win_rate_pct / 100, min_win_rate_pct % 100);
    let as_of_literal = format_as_of(as_of);
    WALLET_DISCOVERY_SQL
        .replace("{as_of}", &as_of_literal)
        .replace("{min_closed_markets}", &min_closed_markets.to_string())
        .replace("{min_win_rate}", &win_rate_decimal)
        .replace("{active_window_days}", &active_window_days.to_string())
        .replace(
            "{max_avg_hours_to_resolution}",
            &max_avg_hours_to_resolution.to_string(),
        )
}

/// Substitute `{last_resolved_at}` in [`RESOLUTION_SQL`] with the given Unix timestamp.
pub(crate) fn render_resolution_sql(last_resolved_at: i64) -> String {
    RESOLUTION_SQL.replace("{last_resolved_at}", &last_resolved_at.to_string())
}

// ── Row parsing ───────────────────────────────────────────────────────────────

fn parse_wallet_rows(rows: Vec<serde_json::Value>) -> Result<Vec<WalletAddress>, BootstrapError> {
    let mut wallets = Vec::with_capacity(rows.len());
    for row in rows {
        let hex = row
            .get("wallet")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if hex.is_empty() {
            continue;
        }
        match WalletAddress::from_hex(hex) {
            Ok(addr) => wallets.push(addr),
            Err(e) => {
                tracing::warn!(address = %hex, error = %e, "dune: skipping unparseable wallet address")
            }
        }
    }
    Ok(wallets)
}

/// Normalise a `CAST(conditionid AS VARCHAR)` result to `0x`-prefixed lowercase hex.
///
/// DuneSQL returns varbinary casts with a `\x` prefix (e.g. `\x0aff…`). The cache
/// uses `0x`-prefixed strings to match the Polymarket trade data format.
fn normalise_condition_id(raw: &str) -> String {
    if let Some(hex) = raw.strip_prefix("\\x") {
        format!("0x{hex}")
    } else {
        raw.to_owned()
    }
}

/// Render the incremental wallet-discovery SQL.
///
/// Anti-joins `polymarket_polygon.market_trades_raw` against the user-uploaded
/// `<namespace>.<known_table_name>` table so only NEW makers active since
/// `last_run_unix` come through. `HAVING COUNT(*) >= min_trades` prevents
/// low-activity wallets from entering the pile (they'd never qualify for
/// activation anyway and just bloat the daily Dune upload).
pub(crate) fn render_discovery_sql(
    namespace: &str,
    known_table_name: &str,
    last_run_unix: i64,
    min_trades_per_wallet: i64,
) -> String {
    format!(
        "WITH known AS (\
            SELECT wallet_hex FROM dune.{namespace}.{known_table_name}\
         ),\
         new_makers AS (\
            SELECT LOWER(CAST(maker AS VARCHAR)) AS wallet_hex,\
                   CAST(TO_UNIXTIME(MIN(block_time)) AS BIGINT) AS first_seen_at_unix,\
                   CAST(COUNT(*) AS BIGINT) AS dune_trade_count\
            FROM polymarket_polygon.market_trades_raw\
            WHERE block_time >= FROM_UNIXTIME({last_run_unix}) AND maker IS NOT NULL\
            GROUP BY maker\
            HAVING COUNT(*) >= {min_trades_per_wallet}\
         )\
         SELECT n.wallet_hex, n.first_seen_at_unix, n.dune_trade_count\
         FROM new_makers n LEFT JOIN known k ON n.wallet_hex = k.wallet_hex\
         WHERE k.wallet_hex IS NULL"
    )
}

/// Parse raw Dune discovery rows into `(wallet_hex, first_seen_at_unix, dune_trade_count)`.
///
/// Normalises `wallet_hex` to the canonical form (`"0x" + 40 lowercase hex chars`).
/// Missing or unparseable rows are warned and skipped.
pub(crate) fn parse_discovery_rows(rows: Vec<serde_json::Value>) -> Vec<(String, i64, i64)> {
    let mut out = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.into_iter().enumerate() {
        let raw = match row.get("wallet_hex").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s,
            _ => {
                tracing::warn!(
                    row_index,
                    row = %row,
                    "dune: discovery row missing wallet_hex, skipping"
                );
                continue;
            }
        };
        let normalised = normalise_wallet_hex(raw);
        let first_seen = match row.get("first_seen_at_unix").and_then(|v| v.as_i64()) {
            Some(t) => t,
            None => {
                tracing::warn!(
                    wallet = %raw,
                    "dune: discovery row missing first_seen_at_unix, skipping"
                );
                continue;
            }
        };
        let trade_count = match row.get("dune_trade_count").and_then(|v| v.as_i64()) {
            Some(c) => c,
            None => {
                tracing::warn!(
                    wallet = %raw,
                    "dune: discovery row missing dune_trade_count, skipping"
                );
                continue;
            }
        };
        out.push((normalised, first_seen, trade_count));
    }
    out
}

/// Normalise a wallet hex string to the canonical form: `"0x" + 40 lowercase hex chars`.
///
/// Accepts: bare 40-hex, 0x-prefixed 40-hex, mixed case. Returns the input
/// unchanged if it doesn't look like a wallet address (caller decides what
/// to do with garbage).
pub(crate) fn normalise_wallet_hex(raw: &str) -> String {
    let trimmed = raw.trim();
    let body = trimmed.strip_prefix("0x").unwrap_or(trimmed);
    if body.len() == 40 && body.chars().all(|c| c.is_ascii_hexdigit()) {
        format!("0x{}", body.to_ascii_lowercase())
    } else {
        trimmed.to_string()
    }
}

/// Parse raw Dune resolution rows into `(market_id, winner, resolved_at_unix)` tuples.
///
/// Missing or empty `condition_id` → warn and skip. `winning_outcome_id` absent →
/// warn and skip. `winning_outcome_id` JSON null → `None` (voided, included).
/// Out-of-range integer → warn and skip.
fn parse_resolution_rows(rows: Vec<serde_json::Value>) -> Vec<(String, Option<u16>, i64)> {
    let mut out = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.into_iter().enumerate() {
        let raw_id = match row.get("condition_id").and_then(|v| v.as_str()) {
            Some(s) if !s.is_empty() => s,
            _ => {
                tracing::warn!(
                    row_index,
                    row = %row,
                    "dune: resolution row missing or empty condition_id, skipping"
                );
                continue;
            }
        };
        let condition_id = normalise_condition_id(raw_id);
        let ts = match row.get("resolved_at_unix").and_then(|v| v.as_i64()) {
            Some(ts) => ts,
            None => {
                tracing::warn!(
                    condition_id = %raw_id,
                    "dune: resolution row missing resolved_at_unix, skipping"
                );
                continue;
            }
        };
        let winner: Option<u16> = match row.get("winning_outcome_id") {
            None => {
                tracing::warn!(
                    condition_id = %raw_id,
                    "dune: resolution row missing winning_outcome_id key, skipping"
                );
                continue;
            }
            Some(v) if v.is_null() => None,
            Some(v) => match v.as_u64().and_then(|n| u16::try_from(n).ok()) {
                Some(w) => Some(w),
                None => {
                    tracing::warn!(
                        condition_id = %raw_id,
                        value = ?v,
                        "dune: resolution row has out-of-range winning_outcome_id, skipping"
                    );
                    continue;
                }
            },
        };
        out.push((condition_id, winner, ts));
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn parse_wallet_rows_valid() {
        let rows = vec![
            serde_json::json!({"wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}),
            serde_json::json!({"wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}),
        ];
        let wallets = parse_wallet_rows(rows).unwrap();
        assert_eq!(wallets.len(), 2);
        assert_eq!(
            wallets[0].to_string(),
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[test]
    fn parse_wallet_rows_skips_invalid() {
        let rows = vec![
            serde_json::json!({"wallet": "not-a-wallet"}),
            serde_json::json!({"wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}),
        ];
        let wallets = parse_wallet_rows(rows).unwrap();
        assert_eq!(wallets.len(), 1);
    }

    #[test]
    fn parse_wallet_rows_skips_empty() {
        let rows = vec![
            serde_json::json!({"wallet": ""}),
            serde_json::json!({"other_col": "irrelevant"}),
        ];
        let wallets = parse_wallet_rows(rows).unwrap();
        assert!(wallets.is_empty());
    }

    // ── SQL rendering / look-ahead invariant ─────────────────────────────────

    #[test]
    fn rendered_sql_contains_no_now_call() {
        // Look-ahead invariant: the live `NOW()` substring must be eliminated
        // entirely once the as_of placeholder is bound. If this regresses, every
        // historical snapshot would silently use wall-clock time.
        let as_of = time::macros::datetime!(2026-03-08 00:00:00 UTC);
        let sql = render_wallet_discovery_sql(as_of, 15, 95, 30, 72);
        assert!(
            !sql.contains("NOW("),
            "rendered SQL must not contain NOW() — would leak wall-clock into historical queries"
        );
    }

    #[test]
    fn rendered_sql_fences_all_three_forward_surfaces() {
        // 1. resolved CTE must filter resolutions before as_of.
        // 2. recently_active CTE must bound trades both above (active window) and below (as_of).
        // 3. wallet_condition join must filter trades before as_of.
        let as_of = time::macros::datetime!(2026-03-08 00:00:00 UTC);
        let sql = render_wallet_discovery_sql(as_of, 15, 95, 30, 72);
        let literal = "TIMESTAMP '2026-03-08 00:00:00'";
        // 3 explicit < {as_of} guards plus 1 >= {as_of} - INTERVAL bound.
        let lt_count = sql.matches(&format!("< {literal}")).count();
        assert!(
            lt_count >= 3,
            "expected at least 3 `< as_of` guards, found {lt_count}; sql:\n{sql}"
        );
        assert!(
            sql.contains(&format!(">= {literal} - INTERVAL '30' DAY")),
            "recently_active window must be relative to as_of"
        );
    }

    #[test]
    fn rendered_sql_substitutes_all_placeholders() {
        let as_of = time::macros::datetime!(2026-03-08 00:00:00 UTC);
        let sql = render_wallet_discovery_sql(as_of, 15, 95, 30, 72);
        for placeholder in [
            "{as_of}",
            "{min_closed_markets}",
            "{min_win_rate}",
            "{active_window_days}",
            "{max_avg_hours_to_resolution}",
        ] {
            assert!(
                !sql.contains(placeholder),
                "placeholder `{placeholder}` not substituted; sql:\n{sql}"
            );
        }
    }

    #[test]
    fn format_as_of_uses_utc_and_second_precision() {
        // Even when given a non-UTC offset, the literal must be UTC so the
        // historical query is unambiguous.
        let at = time::macros::datetime!(2026-03-08 13:45:30 +05:30);
        let s = format_as_of(at);
        assert_eq!(s, "TIMESTAMP '2026-03-08 08:15:30'");
    }

    #[test]
    fn format_as_of_drops_subsecond_precision() {
        let at = OffsetDateTime::from_unix_timestamp_nanos(1_741_392_000_123_456_789).unwrap();
        let s = format_as_of(at);
        assert!(
            !s.contains('.'),
            "subsecond fragment leaked into literal: {s}"
        );
    }

    // ── Resolution SQL rendering ─────────────────────────────────────────────

    #[test]
    fn render_resolution_sql_substitutes_zero() {
        let sql = render_resolution_sql(0);
        assert!(
            sql.contains("FROM_UNIXTIME(0)"),
            "first-run cold start must use FROM_UNIXTIME(0)"
        );
        assert!(
            !sql.contains("{last_resolved_at}"),
            "placeholder must be fully substituted"
        );
    }

    #[test]
    fn render_resolution_sql_substitutes_nonzero_timestamp() {
        let sql = render_resolution_sql(1_700_000_000);
        assert!(
            sql.contains("FROM_UNIXTIME(1700000000)"),
            "timestamp must appear verbatim in rendered SQL"
        );
    }

    #[test]
    fn render_resolution_sql_omits_outcomeslotcount_filter() {
        // Multi-outcome markets must surface so their evt_block_time lands in
        // the cache; the binary-vs-multi distinction lives in winner extraction.
        let sql = render_resolution_sql(0);
        assert!(
            !sql.contains("outcomeslotcount = 2 \n") && !sql.contains("WHERE outcomeslotcount = 2"),
            "WHERE-clause outcomeslotcount filter must be removed; got: {sql}"
        );
    }

    #[test]
    fn render_resolution_sql_winner_extraction_gates_on_slotcount() {
        let sql = render_resolution_sql(0);
        // Winner extraction now requires slotcount=2 inside the CASE so
        // multi-outcome rows return NULL instead of mis-attributing index 0/1.
        assert!(
            sql.contains("outcomeslotcount = 2"),
            "winner CASE must still check outcomeslotcount=2; got: {sql}"
        );
        // Unique-non-zero contract: tied [1,1] returns NULL.
        assert!(
            sql.contains("payoutnumerators[2] = 0") && sql.contains("payoutnumerators[1] = 0"),
            "winner CASE must require the other slot to be 0 to avoid tied-payout mis-tagging; got: {sql}"
        );
    }

    #[test]
    fn render_resolution_sql_with_join_omits_outcomeslotcount_filter() {
        let sql = render_resolution_sql_with_join("apexurellc", 0);
        assert!(
            !sql.contains("WHERE r.outcomeslotcount = 2"),
            "JOIN path WHERE-clause filter must be removed; got: {sql}"
        );
        // Winner extraction still constrained to binary.
        assert!(
            sql.contains("r.outcomeslotcount = 2"),
            "JOIN path winner CASE must still gate on r.outcomeslotcount=2; got: {sql}"
        );
        // Tied-payout disambiguation present in JOIN path too.
        assert!(
            sql.contains("r.payoutnumerators[2] = 0") && sql.contains("r.payoutnumerators[1] = 0"),
            "JOIN path must apply unique-non-zero rule; got: {sql}"
        );
    }

    // ── Resolution row parsing ───────────────────────────────────────────────

    #[test]
    fn parse_resolution_rows_yes_win() {
        let rows = vec![serde_json::json!({
            "condition_id": "0xaabbcc",
            "winning_outcome_id": 0,
            "resolved_at_unix": 1_700_000_000i64,
        })];
        let parsed = parse_resolution_rows(rows);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0, "0xaabbcc");
        assert_eq!(parsed[0].1, Some(0u16));
        assert_eq!(parsed[0].2, 1_700_000_000i64);
    }

    #[test]
    fn parse_resolution_rows_no_win() {
        let rows = vec![serde_json::json!({
            "condition_id": "0xaabbcc",
            "winning_outcome_id": 1,
            "resolved_at_unix": 1_700_000_000i64,
        })];
        let parsed = parse_resolution_rows(rows);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].1, Some(1u16));
    }

    #[test]
    fn parse_resolution_rows_voided_null_included() {
        let rows = vec![serde_json::json!({
            "condition_id": "0xaabbcc",
            "winning_outcome_id": null,
            "resolved_at_unix": 1_700_000_000i64,
        })];
        let parsed = parse_resolution_rows(rows);
        assert_eq!(
            parsed.len(),
            1,
            "voided market must be included in parse output"
        );
        assert_eq!(parsed[0].1, None);
    }

    #[test]
    fn parse_resolution_rows_skips_missing_condition_id() {
        let rows = vec![
            // Missing condition_id entirely.
            serde_json::json!({"winning_outcome_id": 0, "resolved_at_unix": 1_700_000_000i64}),
            // Valid row that must survive.
            serde_json::json!({"condition_id": "0xgood", "winning_outcome_id": 0, "resolved_at_unix": 1_700_000_001i64}),
        ];
        let parsed = parse_resolution_rows(rows);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0, "0xgood");
    }

    #[test]
    fn parse_resolution_rows_skips_missing_resolved_at() {
        let rows = vec![serde_json::json!({
            "condition_id": "0xaabbcc",
            "winning_outcome_id": 0,
        })];
        let parsed = parse_resolution_rows(rows);
        assert!(parsed.is_empty());
    }

    #[test]
    fn parse_resolution_rows_normalises_backslash_x_prefix() {
        let rows = vec![serde_json::json!({
            "condition_id": r"\xaabbcc1234",
            "winning_outcome_id": 0,
            "resolved_at_unix": 1_700_000_000i64,
        })];
        let parsed = parse_resolution_rows(rows);
        assert_eq!(parsed.len(), 1);
        assert_eq!(
            parsed[0].0, "0xaabbcc1234",
            "\\x prefix must be normalised to 0x"
        );
    }

    #[test]
    fn parse_resolution_rows_passthrough_0x_prefix() {
        let rows = vec![serde_json::json!({
            "condition_id": "0xdeadbeef",
            "winning_outcome_id": 1,
            "resolved_at_unix": 1_700_000_000i64,
        })];
        let parsed = parse_resolution_rows(rows);
        assert_eq!(parsed.len(), 1);
        assert_eq!(
            parsed[0].0, "0xdeadbeef",
            "0x prefix must pass through unchanged"
        );
    }

    // ── issue #166: discovery SQL + parser ───────────────────────────────────

    #[test]
    fn render_discovery_sql_substitutes_namespace_and_cursor() {
        let sql = render_discovery_sql("apexurellc", "known_wallets", 1_700_000_000, 100);
        assert!(
            sql.contains("dune.apexurellc.known_wallets"),
            "rendered SQL missing namespace/table: {sql}"
        );
        assert!(
            sql.contains("FROM_UNIXTIME(1700000000)"),
            "rendered SQL missing cursor: {sql}"
        );
        assert!(
            sql.contains("HAVING COUNT(*) >= 100"),
            "rendered SQL missing activation gate: {sql}"
        );
    }

    #[test]
    fn render_discovery_sql_anti_joins_known_table() {
        let sql = render_discovery_sql("ns", "known_wallets", 0, 100);
        assert!(
            sql.contains("LEFT JOIN known k") && sql.contains("k.wallet_hex IS NULL"),
            "rendered SQL must anti-join: {sql}"
        );
    }

    #[test]
    fn render_discovery_sql_selects_required_columns() {
        let sql = render_discovery_sql("ns", "known_wallets", 0, 100);
        for col in ["n.wallet_hex", "n.first_seen_at_unix", "n.dune_trade_count"] {
            assert!(sql.contains(col), "missing column `{col}` in rendered SQL");
        }
    }

    #[test]
    fn parse_discovery_rows_normalises_canonical_form() {
        let rows = vec![serde_json::json!({
            "wallet_hex": "0xAAAaaaaAaAAAAAAaAaaaaAAaaaAaAAaAAAAaAAAA",
            "first_seen_at_unix": 1_700_000_000i64,
            "dune_trade_count": 150i64,
        })];
        let parsed = parse_discovery_rows(rows);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].0, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(parsed[0].1, 1_700_000_000);
        assert_eq!(parsed[0].2, 150);
    }

    #[test]
    fn parse_discovery_rows_adds_0x_when_missing() {
        let rows = vec![serde_json::json!({
            "wallet_hex": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "first_seen_at_unix": 1i64,
            "dune_trade_count": 100i64,
        })];
        let parsed = parse_discovery_rows(rows);
        assert_eq!(parsed[0].0, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    }

    #[test]
    fn parse_discovery_rows_skips_missing_columns() {
        let rows = vec![
            serde_json::json!({"wallet_hex": ""}),
            serde_json::json!({"wallet_hex": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}),
            serde_json::json!({"wallet_hex": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "first_seen_at_unix": 1i64}),
            serde_json::json!({"wallet_hex": "0xcccccccccccccccccccccccccccccccccccccccc", "first_seen_at_unix": 1i64, "dune_trade_count": 100i64}),
        ];
        let parsed = parse_discovery_rows(rows);
        assert_eq!(parsed.len(), 1, "only the fully-populated row survives");
    }

    #[test]
    fn normalise_wallet_hex_passes_through_canonical() {
        assert_eq!(
            normalise_wallet_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }
}
