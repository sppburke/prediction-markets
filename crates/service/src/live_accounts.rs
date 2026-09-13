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
use std::time::Duration;

use arc_swap::ArcSwap;
use pe_core_types::AccountId;
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::supabase_reader::{SupabaseError, auth_token};

/// v1 bound on simultaneously armed live accounts (#508 Decision 4). Canonical:
/// `_GLOSSARY.md` `live_armed_accounts_max`. Bounded by the p95 ≤ 2.0 s end-to-end and
/// CLOB ≤ 5 req/s budgets; raising it requires re-validating those budgets.
pub const LIVE_ARMED_ACCOUNTS_MAX: usize = 2;

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

/// Requests one attempt issues: `accounts` then `account_credentials`. The timeout is applied to
/// each of them, so the budget has to be divided by both the attempts and these.
const LIVE_ACCOUNTS_REQUESTS_PER_ATTEMPT: u32 = 2;

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
    /// Unix time of the last SUCCESSFUL accounts fetch that produced this snapshot;
    /// `None` = never successful (the `Default` posture when the boot fetch fails), which
    /// is always stale (#514).
    pub fetched_at_unix: Option<i64>,
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
        Self {
            accounts,
            fetched_at_unix: None,
        }
    }

    /// Whether this snapshot is recent enough to admit NEW live work — dispatch staging,
    /// pending-target dispatch, and effective-mode writes (#514). A never-successful
    /// snapshot, an age ≥ [`LIVE_ACCOUNTS_STALE_AFTER_SECS`], or a future `fetched_at_unix`
    /// all fail closed. In-flight order recovery and redemption reconciliation deliberately
    /// do NOT gate on freshness: suppressing them would be the worse harm.
    #[must_use]
    pub fn is_fresh(&self, now_unix: i64) -> bool {
        self.fetched_at_unix.is_some_and(|fetched| {
            now_unix
                .checked_sub(fetched)
                .is_some_and(|age| (0..LIVE_ACCOUNTS_STALE_AFTER_SECS).contains(&age))
        })
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

/// Fetch the `accounts` control rows + credential metadata (service-role; PostgREST).
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
    let creds: Vec<CredentialMetaRow> =
        fetch_json(client, &credentials_url(base_url), token, timeout).await?;
    let mut snapshot = LiveAccountsSnapshot::from_rows(rows, &creds);
    snapshot.fetched_at_unix = Some(time::OffsetDateTime::now_utc().unix_timestamp());
    Ok(snapshot)
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
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match poll_live_accounts_once(&client, &base_url, &anon_key, &secret_key, interval).await {
            Ok(snapshot) => live.store(snapshot),
            Err(e) => warn!(error = %e, "live accounts poll failed; keeping last-known-good"),
        }
    }
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
        // Armed targets respect the same order but the v1 bound truncates to 2.
        let targets: Vec<&str> = snap
            .armed_targets()
            .iter()
            .map(|a| a.account_id.as_str())
            .collect();
        assert_eq!(targets, vec!["primary-acct", "beta"]);
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
        assert!(snap.is_fresh(1_000));
        assert!(snap.is_fresh(1_000 + LIVE_ACCOUNTS_STALE_AFTER_SECS - 1));
        // Exactly the threshold is stale (age ≥ bound).
        assert!(!snap.is_fresh(1_000 + LIVE_ACCOUNTS_STALE_AFTER_SECS));
        // A future timestamp fails closed.
        assert!(!snap.is_fresh(999));
        // A later successful poll clears staleness.
        snap.fetched_at_unix = Some(2_000);
        assert!(snap.is_fresh(2_000 + LIVE_ACCOUNTS_STALE_AFTER_SECS - 1));
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

        // 5 s buys three attempts at 500 ms each — six times shorter than the 3 s stall.
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
