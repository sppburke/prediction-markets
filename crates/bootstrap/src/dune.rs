//! Dune Analytics API client for wallet discovery.
//!
//! Workflow: POST /api/v1/sql/execute → poll execution until complete → parse wallet rows.
//! Uses the direct SQL execution endpoint — no stored query is created, so the
//! per-account private-query quota is never touched.
//! Canonical defaults in `docs/_GLOSSARY.md` "Bootstrap defaults" table.

use std::time::{Duration, Instant};

use pe_core_types::WalletAddress;
use serde::Deserialize;

use crate::error::BootstrapError;

// Defaults — canonical values live in `docs/_GLOSSARY.md` "Bootstrap defaults" section.
const BASE_URL: &str = "https://api.dune.com/api/v1";
const POLL_INTERVAL_SECS: u64 = 3;
const MAX_WAIT_SECS: u64 = 300;
const HTTP_TIMEOUT_SECS: u64 = 30;

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
/// Placeholders filled at runtime:
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
), \
recently_active AS (\
  SELECT DISTINCT maker \
  FROM polymarket_polygon.market_trades \
  WHERE block_time >= NOW() - INTERVAL '{active_window_days}' DAY \
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
    /// Returns wallets satisfying all four quality filters:
    /// - more than `min_closed_markets` distinct resolved binary markets traded
    /// - win rate above `min_win_rate_pct` percent on those markets
    /// - at least one trade (on any market) within `active_window_days` days
    /// - average hours from first entry to market resolution below `max_avg_hours_to_resolution`
    ///
    /// No hard limit on result count; all qualifying wallets are returned.
    pub async fn discover_wallets(
        &self,
        min_closed_markets: u32,
        min_win_rate_pct: u32,
        active_window_days: u32,
        max_avg_hours_to_resolution: u32,
    ) -> Result<Vec<WalletAddress>, BootstrapError> {
        let win_rate_decimal = format!("{}.{:02}", min_win_rate_pct / 100, min_win_rate_pct % 100);
        let sql = WALLET_DISCOVERY_SQL
            .replace("{min_closed_markets}", &min_closed_markets.to_string())
            .replace("{min_win_rate}", &win_rate_decimal)
            .replace("{active_window_days}", &active_window_days.to_string())
            .replace(
                "{max_avg_hours_to_resolution}",
                &max_avg_hours_to_resolution.to_string(),
            );
        let execution_id = self.execute_sql(&sql).await?;
        let rows = self.wait_for_results(&execution_id).await?;
        parse_wallet_rows(rows)
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
}
