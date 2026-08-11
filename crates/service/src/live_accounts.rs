//! Live account contexts (#508): the 30-second Supabase poll of the `accounts` control
//! table (+ credential-binding metadata) into an `ArcSwap` snapshot the orchestrator reads
//! per event.
//!
//! Accounts are LIVE-ONLY identities (Decision 1): nothing here touches the shared paper
//! book. The snapshot orders armed targets primary-first, then `(execution_order,
//! account_id)` (Decision 4), and enforces the v1 `live_armed_accounts_max = 2` bound
//! (`_GLOSSARY.md`) as defense-in-depth — the effective-mode machine refuses to arm a
//! third account, and this snapshot additionally refuses to TARGET one if the control
//! table ever carries more. An account whose slug fails the Rust [`AccountId`] grammar is
//! excluded loudly (fail closed for that account; the SQL `CHECK` should make this
//! unreachable). With no armed accounts the snapshot is empty and the copy path behaves
//! exactly as the Phase-A baseline (no dispatch seeds are staged).

use std::sync::Arc;

use arc_swap::ArcSwap;
use pe_core_types::AccountId;
use serde::Deserialize;
use tracing::{error, warn};

use crate::supabase_reader::{SupabaseError, auth_token};

/// v1 bound on simultaneously armed live accounts (#508 Decision 4). Canonical:
/// `_GLOSSARY.md` `live_armed_accounts_max`. Bounded by the p95 ≤ 2.0 s end-to-end and
/// CLOB ≤ 5 req/s budgets; raising it requires re-validating those budgets.
pub const LIVE_ARMED_ACCOUNTS_MAX: usize = 2;

/// One `accounts` row as returned by PostgREST (service-role read), joined client-side
/// with its credential-binding metadata.
#[derive(Debug, Clone, Deserialize)]
pub struct AccountRow {
    pub account_id: String,
    pub is_primary: bool,
    pub enabled: bool,
    pub execution_order: i64,
    pub requested_live_mode: String,
    pub effective_live_mode: String,
    pub live_sizing_mode: Option<String>,
    pub live_sizing_dollar_usd: Option<String>,
    pub live_sizing_contracts: Option<i64>,
    pub live_price_impact_cap_bps: i64,
    pub custody_wallet_address: Option<String>,
    pub custody_wallet_kind: Option<String>,
}

/// Credential-binding metadata for one account (never the sealed bundle — the poller
/// reads only what target freezing needs; decryption happens at admission).
#[derive(Debug, Clone, Deserialize)]
pub struct CredentialMetaRow {
    pub account_id: String,
    pub bundle_version: i64,
    pub key_id: String,
}

/// One validated live account context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountContext {
    pub account_id: AccountId,
    pub is_primary: bool,
    pub enabled: bool,
    pub execution_order: i64,
    pub requested_live_mode: String,
    pub effective_live_mode: String,
    pub live_price_impact_cap_bps: i64,
    pub custody_wallet_address: Option<String>,
    pub custody_wallet_kind: Option<String>,
    /// Credential binding frozen into dispatch targets (Decision 10); `None` when no
    /// sealed bundle exists yet (the account can never arm without one).
    pub credential_binding: Option<(i64, String)>,
}

impl AccountContext {
    /// Armed = the service's own effective transition reached `live_tiny` AND the
    /// account is enabled AND a credential binding exists to freeze into targets.
    #[must_use]
    pub fn is_armed(&self) -> bool {
        self.enabled && self.effective_live_mode == "live_tiny" && self.credential_binding.is_some()
    }
}

/// A point-in-time snapshot of every live account context.
#[derive(Debug, Clone, Default)]
pub struct LiveAccountsSnapshot {
    pub accounts: Vec<AccountContext>,
}

impl LiveAccountsSnapshot {
    /// Validate raw rows into a snapshot. Invalid slugs are excluded loudly; ordering is
    /// primary first, then `(execution_order, account_id)` (Decision 4).
    #[must_use]
    pub fn from_rows(rows: Vec<AccountRow>, creds: &[CredentialMetaRow]) -> Self {
        let mut accounts = Vec::with_capacity(rows.len());
        for row in rows {
            let Ok(account_id) = AccountId::new(&row.account_id) else {
                error!(
                    slug = %row.account_id,
                    "live accounts: slug violates the canonical grammar; account excluded (fail closed)"
                );
                continue;
            };
            let credential_binding = creds
                .iter()
                .find(|c| c.account_id == row.account_id)
                .map(|c| (c.bundle_version, c.key_id.clone()));
            accounts.push(AccountContext {
                account_id,
                is_primary: row.is_primary,
                enabled: row.enabled,
                execution_order: row.execution_order,
                requested_live_mode: row.requested_live_mode,
                effective_live_mode: row.effective_live_mode,
                live_price_impact_cap_bps: row.live_price_impact_cap_bps,
                custody_wallet_address: row.custody_wallet_address,
                custody_wallet_kind: row.custody_wallet_kind,
                credential_binding,
            });
        }
        accounts.sort_by(|a, b| {
            b.is_primary
                .cmp(&a.is_primary)
                .then(a.execution_order.cmp(&b.execution_order))
                .then(a.account_id.cmp(&b.account_id))
        });
        Self { accounts }
    }

    /// The armed dispatch targets in frozen execution order: primary first, then
    /// `(execution_order, account_id)`. Truncated to [`LIVE_ARMED_ACCOUNTS_MAX`] with a
    /// loud error if the control table ever carries more (defense-in-depth; the mode
    /// machine refuses to arm a third).
    #[must_use]
    pub fn armed_targets(&self) -> Vec<&AccountContext> {
        let mut armed: Vec<&AccountContext> =
            self.accounts.iter().filter(|a| a.is_armed()).collect();
        if armed.len() > LIVE_ARMED_ACCOUNTS_MAX {
            error!(
                armed = armed.len(),
                max = LIVE_ARMED_ACCOUNTS_MAX,
                "live accounts: more armed accounts than the v1 bound; refusing the excess"
            );
            armed.truncate(LIVE_ARMED_ACCOUNTS_MAX);
        }
        armed
    }
}

/// `ArcSwap` holder mirroring [`crate::runtime_config::LiveRuntimeConfig`].
#[derive(Clone)]
pub struct LiveAccounts {
    inner: Arc<ArcSwap<LiveAccountsSnapshot>>,
}

impl LiveAccounts {
    #[must_use]
    pub fn new(initial: LiveAccountsSnapshot) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
        }
    }

    /// Wait-free load; take exactly one per event.
    #[must_use]
    pub fn snapshot(&self) -> Arc<LiveAccountsSnapshot> {
        self.inner.load_full()
    }

    pub fn store(&self, snapshot: LiveAccountsSnapshot) {
        self.inner.store(Arc::new(snapshot));
    }
}

/// Fetch the `accounts` control rows + credential metadata (service-role; PostgREST).
/// A fetch failure keeps the last-known-good snapshot (the caller logs and retries on
/// the next poll — the #398 config-poller posture).
pub async fn fetch_live_accounts(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
) -> Result<LiveAccountsSnapshot, SupabaseError> {
    let token = auth_token(anon_key, secret_key);
    let base = base_url.trim_end_matches('/');
    let accounts_url = format!(
        "{base}/rest/v1/accounts?select=account_id,is_primary,enabled,execution_order,\
         requested_live_mode,effective_live_mode,live_sizing_mode,live_sizing_dollar_usd,\
         live_sizing_contracts,live_price_impact_cap_bps,custody_wallet_address,custody_wallet_kind"
    );
    let creds_url =
        format!("{base}/rest/v1/account_credentials?select=account_id,bundle_version,key_id");
    let rows: Vec<AccountRow> = fetch_json(client, &accounts_url, token).await?;
    let creds: Vec<CredentialMetaRow> = fetch_json(client, &creds_url, token).await?;
    Ok(LiveAccountsSnapshot::from_rows(rows, &creds))
}

async fn fetch_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    token: &str,
) -> Result<T, SupabaseError> {
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
    resp.json().await.map_err(SupabaseError::Decode)
}

/// Poll loop: refresh the snapshot every `interval_secs`, keeping last-known-good on any
/// failure. Spawned beside the config poller with the same 30 s cadence (#508).
pub async fn run_live_accounts_poller(
    live: LiveAccounts,
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
    interval_secs: u64,
) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match fetch_live_accounts(&client, &base_url, &anon_key, &secret_key).await {
            Ok(snapshot) => live.store(snapshot),
            Err(e) => warn!(error = %e, "live accounts poll failed; keeping last-known-good"),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn row(id: &str, primary: bool, enabled: bool, order: i64, mode: &str) -> AccountRow {
        AccountRow {
            account_id: id.to_string(),
            is_primary: primary,
            enabled,
            execution_order: order,
            requested_live_mode: mode.to_string(),
            effective_live_mode: mode.to_string(),
            live_sizing_mode: None,
            live_sizing_dollar_usd: None,
            live_sizing_contracts: None,
            live_price_impact_cap_bps: 100,
            custody_wallet_address: None,
            custody_wallet_kind: None,
        }
    }

    fn cred(id: &str) -> CredentialMetaRow {
        CredentialMetaRow {
            account_id: id.to_string(),
            bundle_version: 1,
            key_id: "key-1".to_string(),
        }
    }

    #[test]
    fn ordering_is_primary_first_then_execution_order_then_id() {
        let snap = LiveAccountsSnapshot::from_rows(
            vec![
                row("zeta", false, true, 1, "live_tiny"),
                row("alpha", false, true, 1, "live_tiny"),
                row("primary-acct", true, true, 9, "live_tiny"),
                row("beta", false, true, 0, "live_tiny"),
            ],
            &[
                cred("zeta"),
                cred("alpha"),
                cred("primary-acct"),
                cred("beta"),
            ],
        );
        let order: Vec<&str> = snap
            .accounts
            .iter()
            .map(|a| a.account_id.as_str())
            .collect();
        assert_eq!(order, vec!["primary-acct", "beta", "alpha", "zeta"]);
        // Armed targets respect the same order but the v1 bound truncates to 2.
        let targets: Vec<&str> = snap
            .armed_targets()
            .iter()
            .map(|a| a.account_id.as_str())
            .collect();
        assert_eq!(targets, vec!["primary-acct", "beta"]);
    }

    #[test]
    fn unarmed_disabled_credentialless_and_invalid_accounts_never_target() {
        let snap = LiveAccountsSnapshot::from_rows(
            vec![
                row("off-mode", false, true, 0, "off"),
                row("disabled", false, false, 1, "live_tiny"),
                row("no-creds", false, true, 2, "live_tiny"),
                row("BadSlug", false, true, 3, "live_tiny"),
            ],
            &[cred("off-mode"), cred("disabled"), cred("BadSlug")],
        );
        assert!(snap.armed_targets().is_empty());
        // The invalid slug is excluded from the snapshot entirely (Rust-layer rejection).
        assert_eq!(snap.accounts.len(), 3);
    }
}
