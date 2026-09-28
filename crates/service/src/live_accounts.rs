//! Live account contexts (#508): the 30-second Supabase poll of the `accounts` control
//! table into an `ArcSwap` snapshot the orchestrator reads
//! per event.
//!
//! Accounts are LIVE-ONLY identities (Decision 1): nothing here touches the shared paper
//! book. The snapshot orders targets primary-first, then `(execution_order,
//! account_id)`. An account whose slug fails the Rust [`AccountId`] grammar is
//! excluded loudly (fail closed for that account; the SQL `CHECK` should make this
//! unreachable). With no armed accounts the snapshot is empty and the copy path behaves
//! exactly as the Phase-A baseline (no dispatch seeds are staged).

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use pe_core_types::AccountId;
use pe_execution_core::LiveControlMode;
use pe_execution_core::live_journal::{LiveControlAvailability, LiveControlObservation};
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::live_mode::{ModeDecision, evaluate_mode};
use crate::live_projections::LiveProjectionWriter;
use crate::supabase_reader::{SupabaseError, auth_token};

/// Most attempts a single poll tick may make (#620); [`live_accounts_attempt_plan`] lowers it when
/// the interval cannot pay for them. Supabase returns intermittent gateway 504s in short bursts;
/// with one attempt per tick, four consecutive bursts exhaust [`LIVE_ACCOUNTS_STALE_AFTER_SECS`] and
/// the snapshot is marked stale — which skips live mode passes and failed the #545 financial-era
/// preparation gate after the downtime was already spent.
const LIVE_ACCOUNTS_POLL_ATTEMPTS: u32 = 3;

/// Wait between attempts. Deliberately short: the whole sequence must fit inside one tick.
const LIVE_ACCOUNTS_RETRY_BACKOFF: Duration = Duration::from_millis(500);

/// Percentage of the poll interval the attempt sequence may consume. The remainder is headroom — a
/// sequence that overran its tick would stretch the effective cadence and cause the staleness this
/// retry exists to prevent.
const LIVE_ACCOUNTS_POLL_BUDGET_PERCENT: u32 = 80;

/// The smallest per-request timeout worth issuing. An attempt count whose requests would each get
/// less than this is not affordable, so the plan drops an attempt instead of shrinking the timeout
/// below something that could answer.
const LIVE_ACCOUNTS_MIN_REQUEST_TIMEOUT: Duration = Duration::from_millis(250);

/// Control and metadata are fetched independently. Each control attempt makes one request.
const LIVE_ACCOUNTS_REQUESTS_PER_ATTEMPT: u32 = 1;

/// Snapshot staleness bound (#514): 4 × the 30 s accounts poll cadence
/// ([`crate::config_poller::CONFIG_POLL_INTERVAL_SECS`]), the `_GLOSSARY.md` polled-source
/// block threshold. Canonical: `docs/_GLOSSARY.md` `live_accounts_stale_after_secs`.
pub const LIVE_ACCOUNTS_STALE_AFTER_SECS: i64 = 120;

/// One `accounts` row as returned by PostgREST (service-role read), joined client-side
/// with its credential-binding metadata. The sizing columns are deliberately absent: the
/// poller never consumed them (the fan-out owns sizing and reads them with an exact
/// `::text` cast), and the `numeric` dollar column decoded as a JSON number broke this
/// DTO's `Option<String>` declaration, freezing the snapshot at its boot value (#514).
#[derive(Debug, Clone, Deserialize)]
pub struct AccountRow {
    pub account_id: String,
    pub is_primary: bool,
    pub enabled: bool,
    pub execution_order: i64,
    pub requested_live_mode: String,
    pub effective_live_mode: String,
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
    /// sealed bundle has been verified for this control generation.
    pub credential_binding: Option<(i64, String)>,
}

impl AccountContext {
    /// Mode state only. Credential binding readiness is a separate observation.
    #[must_use]
    pub fn is_armed(&self) -> bool {
        self.requested_live_mode == "live_tiny" && self.effective_live_mode == "live_tiny"
    }
}

/// A point-in-time snapshot of every live account context.
#[derive(Debug, Clone, Default)]
pub struct LiveAccountsSnapshot {
    pub accounts: Vec<AccountContext>,
    /// Unix time of the last SUCCESSFUL accounts fetch that produced this snapshot;
    /// `None` = never successful (the `Default` posture when the boot fetch fails), which
    /// is always stale (#514).
    pub fetched_at_unix: Option<i64>,
    /// A failed read marks control unavailable, even while the last success is recent.
    pub control_available: bool,
    /// Increments on each successful control publication. Metadata from an older
    /// generation cannot install bindings into this snapshot.
    pub generation: u64,
    /// A failed metadata read is distinct from a confirmed empty credential table.
    pub credential_metadata_available: bool,
}

impl LiveAccountsSnapshot {
    #[must_use]
    pub fn control_observation(
        &self,
        account: Option<&AccountContext>,
        now_unix: i64,
    ) -> LiveControlObservation {
        let read_is_recent = self.fetched_at_unix.is_some_and(|fetched| {
            now_unix
                .checked_sub(fetched)
                .is_some_and(|age| (0..LIVE_ACCOUNTS_STALE_AFTER_SECS).contains(&age))
        });
        let availability = if !read_is_recent {
            LiveControlAvailability::Stale
        } else if !self.control_available {
            LiveControlAvailability::FailedRead
        } else {
            LiveControlAvailability::Fresh
        };
        LiveControlObservation {
            decided_at_unix: now_unix,
            last_successful_read_unix: self.fetched_at_unix,
            stale_after_secs: LIVE_ACCOUNTS_STALE_AFTER_SECS,
            availability,
            account_present: account.is_some(),
            requested_mode: account.map(|row| mode_value(&row.requested_live_mode)),
            effective_mode: account.map(|row| mode_value(&row.effective_live_mode)),
            credential_version_available: account.is_some() && self.credential_metadata_available,
            bundle_version: account
                .filter(|_| self.credential_metadata_available)
                .and_then(|row| row.credential_binding.as_ref().map(|binding| binding.0)),
        }
    }

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
        Self {
            accounts,
            fetched_at_unix: None,
            control_available: true,
            generation: 0,
            credential_metadata_available: !creds.is_empty(),
        }
    }

    /// Whether this snapshot is recent enough to admit NEW live work — dispatch staging,
    /// pending-target dispatch, and effective-mode writes (#514). A never-successful
    /// snapshot, an age ≥ [`LIVE_ACCOUNTS_STALE_AFTER_SECS`], or a future `fetched_at_unix`
    /// all fail closed. In-flight order recovery and redemption reconciliation deliberately
    /// do NOT gate on freshness: suppressing them would be the worse harm.
    #[must_use]
    pub fn is_fresh(&self, now_unix: i64) -> bool {
        self.control_available
            && self.fetched_at_unix.is_some_and(|fetched| {
                now_unix
                    .checked_sub(fetched)
                    .is_some_and(|age| (0..LIVE_ACCOUNTS_STALE_AFTER_SECS).contains(&age))
            })
    }

    /// The armed dispatch targets in frozen execution order: primary first, then
    /// `(execution_order, account_id)`.
    #[must_use]
    pub fn armed_targets(&self) -> Vec<&AccountContext> {
        self.accounts.iter().filter(|a| a.is_armed()).collect()
    }
}

fn mode_value(value: &str) -> LiveControlMode {
    if value == "live_tiny" {
        LiveControlMode::LiveTiny
    } else {
        LiveControlMode::Off
    }
}

/// `ArcSwap` holder mirroring [`crate::runtime_config::LiveRuntimeConfig`].
#[derive(Clone)]
pub struct LiveAccounts {
    inner: Arc<ArcSwap<LiveAccountsSnapshot>>,
    publication: Arc<std::sync::Mutex<()>>,
}

impl LiveAccounts {
    #[must_use]
    pub fn new(initial: LiveAccountsSnapshot) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(initial)),
            publication: Arc::new(std::sync::Mutex::new(())),
        }
    }

    /// Wait-free load; take exactly one per event.
    #[must_use]
    pub fn snapshot(&self) -> Arc<LiveAccountsSnapshot> {
        self.inner.load_full()
    }

    pub fn store(&self, snapshot: LiveAccountsSnapshot) {
        let _guard = self
            .publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.store(Arc::new(snapshot));
    }

    /// Order control publication against the in-process pre-POST submitted transition.
    pub fn publication_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.publication
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn apply_metadata(
        &self,
        generation: u64,
        result: Result<Vec<CredentialMetaRow>, SupabaseError>,
    ) {
        let _guard = self.publication_guard();
        let current = self.snapshot();
        if current.generation != generation {
            return;
        }
        let mut next = current.as_ref().clone();
        match result {
            Ok(rows) => {
                next.credential_metadata_available = true;
                for account in &mut next.accounts {
                    account.credential_binding = rows
                        .iter()
                        .find(|row| row.account_id == account.account_id.as_str())
                        .map(|row| (row.bundle_version, row.key_id.clone()));
                }
            }
            Err(error) => {
                warn!(%error, "live credential metadata unavailable");
                next.credential_metadata_available = false;
            }
        }
        self.inner.store(Arc::new(next));
    }

    fn mark_control_unavailable(&self) {
        let _guard = self.publication_guard();
        let mut next = self.snapshot().as_ref().clone();
        next.control_available = false;
        self.inner.store(Arc::new(next));
    }
}

/// PostgREST select for the `accounts` control read. Excludes the sizing columns
/// ([`AccountRow`] explains why).
fn accounts_url(base_url: &str) -> String {
    format!(
        "{}/rest/v1/accounts?select=account_id,is_primary,enabled,execution_order,\
         requested_live_mode,effective_live_mode,live_price_impact_cap_bps,\
         custody_wallet_address,custody_wallet_kind",
        base_url.trim_end_matches('/')
    )
}

fn credentials_url(base_url: &str) -> String {
    format!(
        "{}/rest/v1/account_credentials?select=account_id,bundle_version,key_id",
        base_url.trim_end_matches('/')
    )
}

/// Fetch only `accounts` control rows (service-role; PostgREST).
/// A fetch failure keeps the last-known-good snapshot (the caller logs and retries on
/// the next poll — the #398 config-poller posture).
pub async fn fetch_live_accounts(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
) -> Result<LiveAccountsSnapshot, SupabaseError> {
    fetch_live_accounts_within(client, base_url, anon_key, secret_key, None).await
}

/// As [`fetch_live_accounts`], with an optional per-request timeout. The poller derives one from its
/// interval so a hung request cannot consume the whole tick; a timeout surfaces as
/// [`SupabaseError::Transport`], which is exactly what it is.
async fn fetch_live_accounts_within(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    timeout: Option<Duration>,
) -> Result<LiveAccountsSnapshot, SupabaseError> {
    let token = auth_token(anon_key, secret_key);
    let rows: Vec<AccountRow> = fetch_json(client, &accounts_url(base_url), token, timeout).await?;
    let mut snapshot = LiveAccountsSnapshot::from_rows(rows, &[]);
    snapshot.fetched_at_unix = Some(time::OffsetDateTime::now_utc().unix_timestamp());
    Ok(snapshot)
}

async fn fetch_credential_metadata_within(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    timeout: Duration,
) -> Result<Vec<CredentialMetaRow>, SupabaseError> {
    fetch_json(
        client,
        &credentials_url(base_url),
        auth_token(anon_key, secret_key),
        Some(timeout),
    )
    .await
}

async fn fetch_json<T: serde::de::DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    timeout: Option<Duration>,
) -> Result<T, SupabaseError> {
    let mut request = client
        .get(url)
        .header("apikey", token)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"));
    if let Some(timeout) = timeout {
        request = request.timeout(timeout);
    }
    let resp = request.send().await.map_err(SupabaseError::Transport)?;
    let status = resp.status();
    if !status.is_success() {
        return Err(SupabaseError::Status(status.as_u16()));
    }
    resp.json().await.map_err(SupabaseError::Decode)
}

/// How many attempts this tick can afford and how long each of their requests may take. Every
/// attempt issues [`LIVE_ACCOUNTS_REQUESTS_PER_ATTEMPT`] requests, so the returned pair always
/// satisfies `attempts * requests * timeout + backoffs <= interval`: the sequence can never overrun
/// its own tick, which is itself what marks the snapshot stale. A short interval buys fewer attempts
/// rather than a timeout too small to answer with.
fn live_accounts_attempt_plan(interval: Duration) -> (u32, Duration) {
    // Saturating: `Duration`'s multiply panics on overflow; this must not be a panic path.
    let budget = interval.saturating_mul(LIVE_ACCOUNTS_POLL_BUDGET_PERCENT) / 100;
    let mut attempts = LIVE_ACCOUNTS_POLL_ATTEMPTS;
    loop {
        let backoffs = LIVE_ACCOUNTS_RETRY_BACKOFF.saturating_mul(attempts.saturating_sub(1));
        let per_request =
            budget.saturating_sub(backoffs) / (attempts * LIVE_ACCOUNTS_REQUESTS_PER_ATTEMPT);
        if per_request >= LIVE_ACCOUNTS_MIN_REQUEST_TIMEOUT || attempts == 1 {
            return (attempts, per_request.max(LIVE_ACCOUNTS_MIN_REQUEST_TIMEOUT));
        }
        attempts -= 1;
    }
}

/// Whether another attempt inside this tick could plausibly succeed. Gateway and transport failures
/// are the bursty ones worth retrying; a 4xx (notably the 401/403 authorization denial) will answer
/// identically however many times it is asked, so it fails fast and keeps the budget.
fn live_accounts_error_is_transient(error: &SupabaseError) -> bool {
    match error {
        SupabaseError::Transport(_) => true,
        SupabaseError::Status(status) => *status >= 500,
        // `RequestBuilder::timeout` covers the response body too, and a timeout struck while
        // reading it surfaces from `Response::json` — so it arrives here as `Decode`, not
        // `Transport`. It is still a timeout and still worth another attempt; a genuine
        // JSON/schema error is not.
        SupabaseError::Decode(error) => error.is_timeout(),
        _ => false,
    }
}

/// One poll: as many attempts as the tick affords, stopping early on success or on a non-transient
/// error.
async fn poll_live_accounts_once(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    interval: Duration,
) -> Result<LiveAccountsSnapshot, SupabaseError> {
    let (attempts, request_timeout) = live_accounts_attempt_plan(interval);
    let mut retried = false;
    // Every attempt but the last: a transient failure sleeps and tries again; anything else is the
    // answer. The final attempt falls through so its own result is the poll's result — there is no
    // synthetic error to invent when the budget runs out.
    for attempt in 1..attempts {
        match fetch_live_accounts_within(
            client,
            base_url,
            anon_key,
            secret_key,
            Some(request_timeout),
        )
        .await
        {
            Ok(snapshot) => {
                if retried {
                    info!(attempt, "live accounts poll recovered inside the tick");
                }
                return Ok(snapshot);
            }
            Err(error) if !live_accounts_error_is_transient(&error) => return Err(error),
            Err(error) => warn!(
                attempt,
                error = %error,
                "live accounts poll attempt failed; retrying inside the tick"
            ),
        }
        retried = true;
        tokio::time::sleep(LIVE_ACCOUNTS_RETRY_BACKOFF).await;
    }
    let outcome = fetch_live_accounts_within(
        client,
        base_url,
        anon_key,
        secret_key,
        Some(request_timeout),
    )
    .await;
    if outcome.is_ok() && retried {
        info!(
            attempt = attempts,
            "live accounts poll recovered inside the tick"
        );
    }
    outcome
}

/// Poll loop: refresh the snapshot every `interval_secs`, keeping last-known-good only after every
/// attempt in the tick has failed. Spawned beside the config poller with the same 30 s cadence
/// (#508). Retrying inside the tick matters because the staleness bound is exactly four intervals:
/// without it, a burst of four failed polls marks the snapshot stale (#620).
pub async fn run_live_accounts_poller(
    live: LiveAccounts,
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
    interval_secs: u64,
) {
    let interval = Duration::from_secs(interval_secs.max(1));
    let projection = LiveProjectionWriter::new(client.clone(), &base_url, &anon_key, &secret_key);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut generation = live.snapshot().generation;
    let mut cursor = 0_usize;
    loop {
        ticker.tick().await;
        match poll_live_accounts_once(&client, &base_url, &anon_key, &secret_key, interval).await {
            Ok(mut snapshot) => {
                generation = generation.saturating_add(1);
                snapshot.generation = generation;
                // Publishing the control result is independent of metadata and mode RPCs.
                let candidate = next_mode_candidate(&snapshot, &mut cursor);
                live.store(snapshot);
                let metadata_live = live.clone();
                let metadata_client = client.clone();
                let metadata_url = base_url.clone();
                let metadata_anon = anon_key.clone();
                let metadata_secret = secret_key.clone();
                tokio::spawn(async move {
                    let result = fetch_credential_metadata_within(
                        &metadata_client,
                        &metadata_url,
                        &metadata_anon,
                        &metadata_secret,
                        interval / 2,
                    )
                    .await;
                    metadata_live.apply_metadata(generation, result);
                });
                if let Some(account) = candidate {
                    let projection = projection.clone();
                    tokio::spawn(async move {
                        if let ModeDecision::SetEffective { mode, reason } = evaluate_mode(
                            &account.requested_live_mode,
                            &account.effective_live_mode,
                        ) {
                            match tokio::time::timeout(
                                interval / 2,
                                projection.set_effective_mode(
                                    account.account_id.as_str(),
                                    mode,
                                    reason,
                                ),
                            )
                            .await
                            {
                                Ok(Ok(())) => {}
                                Ok(Err(error)) => {
                                    warn!(account_id = %account.account_id, %error, "effective mode write failed")
                                }
                                Err(_) => {
                                    warn!(account_id = %account.account_id, "effective mode write timed out")
                                }
                            }
                        }
                    });
                }
            }
            Err(e) => {
                live.mark_control_unavailable();
                warn!(error = %e, "live accounts control poll failed; last-good rows are display only");
            }
        }
    }
}

fn next_mode_candidate(
    snapshot: &LiveAccountsSnapshot,
    cursor: &mut usize,
) -> Option<AccountContext> {
    let len = snapshot.accounts.len();
    if len == 0 {
        return None;
    }
    let found = (0..len)
        .map(|offset| (*cursor + offset) % len)
        .find(|index| {
            let account = &snapshot.accounts[*index];
            evaluate_mode(&account.requested_live_mode, &account.effective_live_mode)
                != ModeDecision::Keep
        });
    *cursor = found.map_or((*cursor + 1) % len, |index| (index + 1) % len);
    found.map(|index| snapshot.accounts[index].clone())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use axum::response::IntoResponse;

    use super::*;

    fn row(id: &str, primary: bool, enabled: bool, order: i64, mode: &str) -> AccountRow {
        AccountRow {
            account_id: id.to_string(),
            is_primary: primary,
            enabled,
            execution_order: order,
            requested_live_mode: mode.to_string(),
            effective_live_mode: mode.to_string(),
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
        // Armed targets retain the full deterministic order.
        let targets: Vec<&str> = snap
            .armed_targets()
            .iter()
            .map(|a| a.account_id.as_str())
            .collect();
        assert_eq!(targets, vec!["primary-acct", "beta", "alpha", "zeta"]);
    }

    #[test]
    fn real_postgrest_body_with_numeric_sizing_column_decodes() {
        // The live `accounts` row that broke the poller (#514): `live_sizing_dollar_usd` is
        // `numeric`, so PostgREST serializes it as a JSON NUMBER. A body that still carries
        // the sizing columns must decode (the select omits them; serde ignores extras).
        let body = r#"[{
            "account_id": "sppburke",
            "is_primary": true,
            "enabled": true,
            "execution_order": 0,
            "requested_live_mode": "off",
            "effective_live_mode": "off",
            "live_sizing_mode": "dollar",
            "live_sizing_dollar_usd": 1,
            "live_sizing_contracts": null,
            "live_price_impact_cap_bps": 100,
            "custody_wallet_address": null,
            "custody_wallet_kind": null
        }]"#;
        let rows: Vec<AccountRow> = serde_json::from_str(body).unwrap();
        let snap = LiveAccountsSnapshot::from_rows(rows, &[cred("sppburke")]);
        assert_eq!(snap.accounts.len(), 1);
        assert_eq!(snap.accounts[0].account_id.as_str(), "sppburke");
        assert!(!snap.accounts[0].is_armed());
    }

    #[test]
    fn freshness_fails_closed_on_never_future_and_threshold() {
        let mut snap = LiveAccountsSnapshot::default();
        // Never-successful (the failed-boot-fetch posture) is stale.
        assert!(!snap.is_fresh(1_000));
        snap.fetched_at_unix = Some(1_000);
        snap.control_available = true;
        assert!(snap.is_fresh(1_000));
        assert!(snap.is_fresh(1_000 + LIVE_ACCOUNTS_STALE_AFTER_SECS - 1));
        // Exactly the threshold is stale (age ≥ bound).
        assert!(!snap.is_fresh(1_000 + LIVE_ACCOUNTS_STALE_AFTER_SECS));
        // A future timestamp fails closed.
        assert!(!snap.is_fresh(999));
        snap.control_available = false;
        assert!(!snap.is_fresh(1_001));
        snap.control_available = true;
        // A later successful poll clears staleness.
        snap.fetched_at_unix = Some(2_000);
        assert!(snap.is_fresh(2_000 + LIVE_ACCOUNTS_STALE_AFTER_SECS - 1));
    }

    #[test]
    fn control_outcomes_separate_failed_read_stale_and_binding_metadata() {
        let mut snap = LiveAccountsSnapshot::from_rows(
            vec![row("acct", false, false, 0, "live_tiny")],
            &[cred("acct")],
        );
        snap.credential_metadata_available = false;
        let account = &snap.accounts[0];
        assert!(account.is_armed());
        assert_eq!(
            snap.control_observation(Some(account), 1_000).availability,
            LiveControlAvailability::Stale,
        );
        snap.fetched_at_unix = Some(1_000);
        let account = &snap.accounts[0];
        let fresh = snap.control_observation(Some(account), 1_001);
        assert_eq!(fresh.availability, LiveControlAvailability::Fresh);
        assert!(!fresh.credential_version_available);
        assert_eq!(fresh.bundle_version, None);
        snap.control_available = false;
        assert_eq!(
            snap.control_observation(Some(account), 1_001).availability,
            LiveControlAvailability::FailedRead,
        );
        assert_eq!(
            snap.control_observation(Some(account), 999).availability,
            LiveControlAvailability::Stale,
        );
        assert_eq!(
            snap.control_observation(None, 1_001).availability,
            LiveControlAvailability::FailedRead,
        );
        snap.control_available = true;
        snap.credential_metadata_available = true;
        let verified = snap.control_observation(Some(account), 1_001);
        assert!(verified.permits_mode());
        assert_eq!(verified.bundle_version, Some(1));
    }

    #[test]
    fn mode_cursor_advances_across_all_accounts_after_each_attempt() {
        let mut snapshot = LiveAccountsSnapshot::from_rows(
            vec![
                row("one", false, false, 0, "live_tiny"),
                row("two", false, false, 1, "live_tiny"),
                row("three", false, false, 2, "live_tiny"),
            ],
            &[],
        );
        for account in &mut snapshot.accounts {
            account.effective_live_mode = "off".to_owned();
        }
        let mut cursor = 0;
        let attempted: Vec<String> = (0..6)
            .filter_map(|_| next_mode_candidate(&snapshot, &mut cursor))
            .map(|account| account.account_id.as_str().to_owned())
            .collect();
        assert_eq!(attempted, ["one", "two", "three", "one", "two", "three"]);
    }

    #[test]
    fn delayed_metadata_cannot_replace_newer_control_generation_or_version() {
        let mut initial =
            LiveAccountsSnapshot::from_rows(vec![row("acct", true, false, 0, "live_tiny")], &[]);
        initial.generation = 1;
        let live = LiveAccounts::new(initial);
        live.apply_metadata(
            1,
            Ok(vec![CredentialMetaRow {
                account_id: "acct".to_owned(),
                bundle_version: 1,
                key_id: "old".to_owned(),
            }]),
        );
        let mut newer =
            LiveAccountsSnapshot::from_rows(vec![row("acct", true, false, 0, "live_tiny")], &[]);
        newer.generation = 2;
        live.store(newer);
        live.apply_metadata(
            1,
            Ok(vec![CredentialMetaRow {
                account_id: "acct".to_owned(),
                bundle_version: 1,
                key_id: "old".to_owned(),
            }]),
        );
        assert_eq!(live.snapshot().accounts[0].credential_binding, None);
        live.apply_metadata(
            2,
            Ok(vec![CredentialMetaRow {
                account_id: "acct".to_owned(),
                bundle_version: 2,
                key_id: "new".to_owned(),
            }]),
        );
        assert_eq!(
            live.snapshot().accounts[0].credential_binding,
            Some((2, "new".to_owned()))
        );
        live.apply_metadata(2, Err(SupabaseError::Status(503)));
        let unavailable = live.snapshot();
        assert!(!unavailable.credential_metadata_available);
        assert_eq!(
            unavailable.accounts[0].credential_binding,
            Some((2, "new".to_owned()))
        );
        assert_eq!(
            unavailable
                .control_observation(Some(&unavailable.accounts[0]), 1_000)
                .bundle_version,
            None
        );
    }

    #[test]
    fn staleness_bound_is_four_poll_intervals() {
        assert_eq!(
            u64::try_from(LIVE_ACCOUNTS_STALE_AFTER_SECS).unwrap(),
            4 * crate::config_poller::CONFIG_POLL_INTERVAL_SECS
        );
    }

    #[test]
    fn accounts_select_omits_the_sizing_columns() {
        let url = accounts_url("https://example.test/");
        for field in [
            "live_sizing_mode",
            "live_sizing_dollar_usd",
            "live_sizing_contracts",
        ] {
            assert!(!url.contains(field), "{field} must not be selected");
        }
        assert!(url.starts_with("https://example.test/rest/v1/accounts?select=account_id,"));
    }

    /// #620. Scenario: the accounts endpoint returns HTTP 504 for the first two attempts of a tick
    /// and succeeds on the third — the shape Supabase's gateway actually produces.
    /// PASS: the poll returns a snapshot within the single tick, having made 3 accounts requests.
    /// FAIL: the poll returns an error, i.e. the burst cost the whole interval (the pre-fix
    /// behavior, which after four such intervals marks the snapshot stale).
    #[tokio::test]
    async fn transient_gateway_failures_recover_inside_one_tick() {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let app = axum::Router::new()
            .route(
                "/rest/v1/accounts",
                axum::routing::get(move || {
                    let counter = Arc::clone(&counter);
                    async move {
                        let seen = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if seen < 2 {
                            return axum::http::StatusCode::GATEWAY_TIMEOUT.into_response();
                        }
                        axum::Json(serde_json::json!([])).into_response()
                    }
                }),
            )
            .route(
                "/rest/v1/account_credentials",
                axum::routing::get(|| async { axum::Json(serde_json::json!([])) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let polled = poll_live_accounts_once(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            "publishable-key",
            "",
            Duration::from_secs(30),
        )
        .await;

        assert!(
            polled.is_ok(),
            "a two-failure burst must not cost the whole tick, got {polled:?}"
        );
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "expected two retries inside the tick"
        );
        server.abort();
    }

    /// #620. An authorization denial answers identically however many times it is asked, so it must
    /// not consume the retry budget.
    /// PASS: exactly one accounts request, and the 401 is returned.
    /// FAIL: more than one request (budget wasted on an error that cannot change).
    #[tokio::test]
    async fn authorization_denial_is_not_retried() {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let app = axum::Router::new().route(
            "/rest/v1/accounts",
            axum::routing::get(move || {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    axum::http::StatusCode::UNAUTHORIZED
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let polled = poll_live_accounts_once(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            "publishable-key",
            "",
            Duration::from_secs(30),
        )
        .await;

        assert!(
            matches!(&polled, Err(SupabaseError::Status(401))),
            "expected an unretried 401, got {polled:?}"
        );
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a 4xx must not be retried"
        );
        server.abort();
    }

    /// #620. `RequestBuilder::timeout` covers the response body, so a stall *after* the 200 status
    /// line surfaces from `Response::json` as `Decode`, not `Transport`.
    /// PASS: the poll recovers inside the tick, having made 3 accounts requests.
    /// FAIL: the poll returns the first `Decode` after one attempt — the pre-fix classification,
    /// which leaves body-phase stalls costing a whole interval each.
    #[tokio::test]
    async fn body_phase_timeouts_recover_inside_one_tick() {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&hits);
        let app = axum::Router::new()
            .route(
                "/rest/v1/accounts",
                axum::routing::get(move || {
                    let counter = Arc::clone(&counter);
                    async move {
                        let seen = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // Headers go out immediately; the body stalls well past the derived
                        // per-request timeout on the first two attempts.
                        let stall = if seen < 2 {
                            Duration::from_secs(3)
                        } else {
                            Duration::ZERO
                        };
                        axum::body::Body::from_stream(futures::stream::once(async move {
                            tokio::time::sleep(stall).await;
                            Ok::<&'static str, std::io::Error>("[]")
                        }))
                        .into_response()
                    }
                }),
            )
            .route(
                "/rest/v1/account_credentials",
                axum::routing::get(|| async { axum::Json(serde_json::json!([])) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        // 5 s buys three bounded attempts, each shorter than the 3 s stall.
        let polled = poll_live_accounts_once(
            &reqwest::Client::new(),
            &format!("http://{address}"),
            "publishable-key",
            "",
            Duration::from_secs(5),
        )
        .await;

        assert!(
            polled.is_ok(),
            "a body-phase stall must be retried like any other timeout, got {polled:?}"
        );
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "expected two retries inside the tick"
        );
        server.abort();
    }

    /// A hanging credential metadata response and a hanging effective-mode RPC cannot
    /// hold the control read cadence or starve another account's mode proposal.
    #[tokio::test]
    async fn control_poller_progresses_while_metadata_and_mode_rpc_hang() {
        let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mode_attempts = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let read_counter = reads.clone();
        let mode_log = mode_attempts.clone();
        let app = axum::Router::new()
            .route(
                "/rest/v1/accounts",
                axum::routing::get(move || {
                    let reads = read_counter.clone();
                    async move {
                        reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        axum::Json(serde_json::json!([
                            {
                                "account_id": "one", "is_primary": true, "enabled": false,
                                "execution_order": 0, "requested_live_mode": "live_tiny",
                                "effective_live_mode": "off", "live_price_impact_cap_bps": 100,
                                "custody_wallet_address": null, "custody_wallet_kind": null
                            },
                            {
                                "account_id": "two", "is_primary": false, "enabled": false,
                                "execution_order": 1, "requested_live_mode": "live_tiny",
                                "effective_live_mode": "off", "live_price_impact_cap_bps": 100,
                                "custody_wallet_address": null, "custody_wallet_kind": null
                            }
                        ]))
                    }
                }),
            )
            .route(
                "/rest/v1/account_credentials",
                axum::routing::get(|| async {
                    std::future::pending::<axum::Json<serde_json::Value>>().await
                }),
            )
            .route(
                "/rest/v1/rpc/account_set_effective_mode",
                axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let mode_log = mode_log.clone();
                    async move {
                        if let Some(account_id) =
                            body.get("p_account_id").and_then(serde_json::Value::as_str)
                        {
                            mode_log.lock().unwrap().push(account_id.to_owned());
                        }
                        std::future::pending::<axum::http::StatusCode>().await
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let live = LiveAccounts::new(LiveAccountsSnapshot::default());
        let poller = tokio::spawn(run_live_accounts_poller(
            live.clone(),
            reqwest::Client::new(),
            format!("http://{address}"),
            "publishable-key".to_owned(),
            String::new(),
            1,
        ));
        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                if live.snapshot().generation >= 3 && mode_attempts.lock().unwrap().len() >= 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(reads.load(std::sync::atomic::Ordering::SeqCst) >= 3);
        assert!(live.snapshot().control_available);
        assert!(!live.snapshot().credential_metadata_available);
        assert_eq!(&mode_attempts.lock().unwrap()[..2], ["one", "two"]);
        poller.abort();
        server.abort();
    }

    /// The server commits the first proposal but loses its response. A restarted poller
    /// rereads effective mode before making another proposal, then applies a later `off`.
    #[tokio::test]
    async fn lost_effective_response_and_restart_reconcile_latest_owner_request() {
        let modes = Arc::new(std::sync::Mutex::new((
            "live_tiny".to_owned(),
            "off".to_owned(),
        )));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let read_modes = modes.clone();
        let write_modes = modes.clone();
        let write_calls = calls.clone();
        let app = axum::Router::new()
            .route(
                "/rest/v1/accounts",
                axum::routing::get(move || {
                    let modes = read_modes.clone();
                    async move {
                        let (requested, effective) = modes.lock().unwrap().clone();
                        axum::Json(serde_json::json!([{
                            "account_id": "acct", "is_primary": true, "enabled": false,
                            "execution_order": 0, "requested_live_mode": requested,
                            "effective_live_mode": effective, "live_price_impact_cap_bps": 100,
                            "custody_wallet_address": null, "custody_wallet_kind": null
                        }]))
                    }
                }),
            )
            .route(
                "/rest/v1/account_credentials",
                axum::routing::get(|| async { axum::Json(serde_json::json!([])) }),
            )
            .route(
                "/rest/v1/rpc/account_set_effective_mode",
                axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                    let modes = write_modes.clone();
                    let calls = write_calls.clone();
                    async move {
                        let proposed = body
                            .get("p_effective_mode")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        {
                            let mut modes = modes.lock().unwrap();
                            if proposed == modes.0 {
                                modes.1 = proposed.to_owned();
                            }
                        }
                        let call = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if call == 0 {
                            std::future::pending::<axum::http::StatusCode>().await
                        } else {
                            axum::http::StatusCode::OK
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let url = format!("http://{address}");
        let first = LiveAccounts::new(LiveAccountsSnapshot::default());
        let first_poller = tokio::spawn(run_live_accounts_poller(
            first,
            reqwest::Client::new(),
            url.clone(),
            "key".to_owned(),
            String::new(),
            1,
        ));
        tokio::time::timeout(Duration::from_secs(4), async {
            while calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        first_poller.abort();
        let restarted = LiveAccounts::new(LiveAccountsSnapshot::default());
        let restarted_poller = tokio::spawn(run_live_accounts_poller(
            restarted.clone(),
            reqwest::Client::new(),
            url,
            "key".to_owned(),
            String::new(),
            1,
        ));
        tokio::time::timeout(Duration::from_secs(4), async {
            while restarted.snapshot().generation < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(restarted.snapshot().accounts[0].is_armed());
        modes.lock().unwrap().0 = "off".to_owned();
        tokio::time::timeout(Duration::from_secs(4), async {
            while modes.lock().unwrap().1 != "off" {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        restarted_poller.abort();
        server.abort();
    }

    /// The attempt sequence must fit inside its tick with no exception: overrunning would stretch
    /// the effective cadence, which is itself what marks the snapshot stale. `interval_secs.max(1)`
    /// in the poller makes 1 s the smallest interval this can be asked about.
    #[test]
    fn attempt_sequence_fits_inside_the_tick() {
        for secs in [1_u64, 2, 3, 5, 30, 120] {
            let interval = Duration::from_secs(secs);
            let (attempts, per_request) = live_accounts_attempt_plan(interval);
            let backoffs = LIVE_ACCOUNTS_RETRY_BACKOFF.saturating_mul(attempts.saturating_sub(1));
            // Every request in every attempt timing out is the worst the tick can cost.
            let worst_case = per_request
                .saturating_mul(LIVE_ACCOUNTS_REQUESTS_PER_ATTEMPT)
                .saturating_mul(attempts)
                + backoffs;
            assert!(
                worst_case <= interval,
                "interval {secs}s: worst case {worst_case:?} exceeds the tick"
            );
            assert!(per_request >= LIVE_ACCOUNTS_MIN_REQUEST_TIMEOUT);
            assert!((1..=LIVE_ACCOUNTS_POLL_ATTEMPTS).contains(&attempts));
        }
    }

    /// The production cadence must still buy the full retry budget; a short interval may not.
    #[test]
    fn the_production_interval_affords_every_attempt() {
        let (attempts, _) = live_accounts_attempt_plan(Duration::from_secs(
            crate::config_poller::CONFIG_POLL_INTERVAL_SECS,
        ));
        assert_eq!(attempts, LIVE_ACCOUNTS_POLL_ATTEMPTS);
    }

    /// Only gateway/transport failures are worth another attempt inside the tick.
    #[test]
    fn only_transient_errors_are_retried() {
        assert!(live_accounts_error_is_transient(&SupabaseError::Status(
            504
        )));
        assert!(live_accounts_error_is_transient(&SupabaseError::Status(
            502
        )));
        assert!(live_accounts_error_is_transient(&SupabaseError::Status(
            500
        )));
        assert!(!live_accounts_error_is_transient(&SupabaseError::Status(
            401
        )));
        assert!(!live_accounts_error_is_transient(&SupabaseError::Status(
            403
        )));
        assert!(!live_accounts_error_is_transient(&SupabaseError::Status(
            404
        )));
    }

    /// PASS: PostgREST authorization denial on the first protected account read produces the
    /// same empty, never-fresh snapshot selected by the service's boot fallback.
    #[tokio::test]
    async fn authorization_denial_yields_stale_empty_boot_snapshot() {
        for status in [
            axum::http::StatusCode::UNAUTHORIZED,
            axum::http::StatusCode::FORBIDDEN,
        ] {
            let app = axum::Router::new().route(
                "/rest/v1/accounts",
                axum::routing::get(move || async move { status }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

            let fetched = fetch_live_accounts(
                &reqwest::Client::new(),
                &format!("http://{address}"),
                "publishable-key",
                "",
            )
            .await;
            assert!(
                matches!(
                    &fetched,
                    Err(SupabaseError::Status(actual)) if *actual == status.as_u16()
                ),
                "expected account read status {status}, got {fetched:?}"
            );
            let snapshot = fetched.unwrap_or_default();
            assert!(snapshot.accounts.is_empty());
            assert_eq!(snapshot.fetched_at_unix, None);
            assert!(!snapshot.is_fresh(time::OffsetDateTime::now_utc().unix_timestamp()));

            server.abort();
            let _ = server.await;
        }
    }

    #[test]
    fn owner_mode_arms_independent_of_enabled_and_credentials() {
        let snap = LiveAccountsSnapshot::from_rows(
            vec![
                row("off-mode", false, true, 0, "off"),
                row("disabled", false, false, 1, "live_tiny"),
                row("no-creds", false, true, 2, "live_tiny"),
                row("BadSlug", false, true, 3, "live_tiny"),
            ],
            &[cred("off-mode"), cred("disabled"), cred("BadSlug")],
        );
        let targets: Vec<&str> = snap
            .armed_targets()
            .iter()
            .map(|account| account.account_id.as_str())
            .collect();
        assert_eq!(targets, vec!["disabled", "no-creds"]);
        // The invalid slug is excluded from the snapshot entirely (Rust-layer rejection).
        assert_eq!(snap.accounts.len(), 3);
    }
}
