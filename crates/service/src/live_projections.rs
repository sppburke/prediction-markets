//! Account-scoped Supabase live projections + effective-mode control writes (#508).
//!
//! The service is the sole writer of `live_fills` / `live_positions` /
//! `live_account_state` (service-role only from birth) and commits its effective-mode
//! transitions through the `account_set_effective_mode` control RPC so state + audit
//! event land atomically (Decision 8 / the #397 sole-writer precedent). Projection
//! writes are idempotent (insert-only fills ignore duplicates; mutable rows merge) and
//! converge on the per-account reconcile pass (the `supabase_sink` self-heal precedent);
//! the account-tagged raw journal remains the audit/replay history — these rows exist
//! for the site.

use rust_decimal::Decimal;
use serde::Serialize;

use crate::supabase_reader::auth_token;

/// Typed projection-write failures (surfaced; the reconcile pass self-heals).
#[derive(Debug, thiserror::Error)]
pub enum LiveProjectionError {
    #[error("transport: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("postgrest status {status}: {body}")]
    Status { status: u16, body: String },
}

/// One `live_fills` row (PK `(account_id, idempotency_key)` — inserts are idempotent).
#[derive(Debug, Clone, Serialize)]
pub struct LiveFillRow {
    pub account_id: String,
    pub idempotency_key: String,
    pub leader_wallet: String,
    pub source_trade_id: Option<String>,
    pub market_id: String,
    pub outcome_id: i64,
    pub side: String,
    pub contracts: Decimal,
    pub fill_price: Decimal,
    pub entry_unix: Option<i64>,
    pub event_seq: i64,
}

/// One `live_positions` current-state row (PK `(account_id, market_id, outcome_id)`).
#[derive(Debug, Clone, Serialize)]
pub struct LivePositionRow {
    pub account_id: String,
    pub market_id: String,
    pub outcome_id: i64,
    pub long_contracts: Decimal,
    pub short_contracts: Decimal,
    pub cost_basis: Decimal,
}

/// The one current-state row per account (`live_account_state`).
#[derive(Debug, Clone, Serialize)]
pub struct LiveAccountStateRow {
    pub account_id: String,
    pub free_collateral: Decimal,
    pub reserved: Decimal,
    pub unredeemed_value: Decimal,
    pub last_reconciled_at: Option<String>,
    pub admission_closed_reason: Option<String>,
}

/// PostgREST writer for the live projections + control RPC.
#[derive(Clone)]
pub struct LiveProjectionWriter {
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
}

impl LiveProjectionWriter {
    #[must_use]
    pub fn new(client: reqwest::Client, base_url: &str, anon_key: &str, secret_key: &str) -> Self {
        Self {
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
            anon_key: anon_key.to_owned(),
            secret_key: secret_key.to_owned(),
        }
    }

    async fn post_upsert<T: Serialize>(
        &self,
        table: &str,
        rows: &[T],
    ) -> Result<(), LiveProjectionError> {
        if rows.is_empty() {
            return Ok(());
        }
        let token = auth_token(&self.anon_key, &self.secret_key);
        let url = format!("{}/rest/v1/{table}", self.base_url);
        let resp = self
            .client
            .post(&url)
            .header("apikey", token)
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
            .header("Prefer", upsert_preference(table))
            .json(rows)
            .send()
            .await
            .map_err(LiveProjectionError::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(LiveProjectionError::Status {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }

    /// Idempotent fill-projection upsert (a duplicate dispatch converges to one row).
    pub async fn upsert_fills(&self, rows: &[LiveFillRow]) -> Result<(), LiveProjectionError> {
        self.post_upsert("live_fills", rows).await
    }

    /// Idempotent current-position upsert.
    pub async fn upsert_positions(
        &self,
        rows: &[LivePositionRow],
    ) -> Result<(), LiveProjectionError> {
        self.post_upsert("live_positions", rows).await
    }

    /// Idempotent per-account current-state upsert.
    pub async fn upsert_account_state(
        &self,
        row: &LiveAccountStateRow,
    ) -> Result<(), LiveProjectionError> {
        self.post_upsert("live_account_state", std::slice::from_ref(row))
            .await
    }

    /// Commit an effective-mode transition through the `account_set_effective_mode`
    /// control RPC (state + one sanitized audit event in one transaction — the service
    /// is the SOLE writer of the effective value, Decision 8).
    pub async fn set_effective_mode(
        &self,
        account_id: &str,
        effective_mode: &str,
        reason: &str,
    ) -> Result<(), LiveProjectionError> {
        let token = auth_token(&self.anon_key, &self.secret_key);
        let url = format!("{}/rest/v1/rpc/account_set_effective_mode", self.base_url);
        let body = serde_json::json!({
            "p_account_id": account_id,
            "p_effective_mode": effective_mode,
            "p_actor": "pe-service",
            "p_reason": reason,
        });
        let resp = self
            .client
            .post(&url)
            .header("apikey", token)
            .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
            .json(&body)
            .send()
            .await
            .map_err(LiveProjectionError::Transport)?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(LiveProjectionError::Status {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }
}

fn upsert_preference(table: &str) -> &'static str {
    if table == "live_fills" {
        "resolution=ignore-duplicates"
    } else {
        "resolution=merge-duplicates"
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use rust_decimal_macros::dec;

    use super::{LivePositionRow, upsert_preference};

    #[test]
    fn immutable_fills_ignore_duplicates_while_mutable_tables_merge() {
        assert_eq!(
            upsert_preference("live_fills"),
            "resolution=ignore-duplicates"
        );
        assert_eq!(
            upsert_preference("live_positions"),
            "resolution=merge-duplicates"
        );
        assert_eq!(
            upsert_preference("live_account_state"),
            "resolution=merge-duplicates"
        );
    }

    /// PASS: fractional live quantities serialize as exact PostgREST numeric values.
    #[test]
    fn live_position_serialization_preserves_fractional_quantities() {
        let row = LivePositionRow {
            account_id: "account".to_owned(),
            market_id: "market".to_owned(),
            outcome_id: 1,
            long_contracts: dec!(3.125001),
            short_contracts: dec!(0.000001),
            cost_basis: dec!(2.5),
        };
        let value = serde_json::to_value(row).unwrap();
        assert_eq!(value["long_contracts"], serde_json::json!("3.125001"));
        assert_eq!(value["short_contracts"], serde_json::json!("0.000001"));
    }
}
