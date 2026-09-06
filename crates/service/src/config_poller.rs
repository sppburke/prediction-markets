//! Supabase `service_config` poll loop (issue #398 WS1).
//!
//! Every [`CONFIG_POLL_INTERVAL_SECS`] the coordinator fetches the KV table, parses it onto the
//! last-known-good snapshot, and publishes ordinary runtime edits immediately. A watchlist-size
//! edit is different: the poller places a coalescing request on a dedicated worker and keeps
//! polling while that worker performs potentially slow history/position preparation. Only a
//! successfully applied membership generation becomes the runtime snapshot's active capacity.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc, watch};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::health::SharedHealth;
use crate::orchestrator_control::OrchestratorControl;
use crate::paper_recovery::{HaltState, active_risk_halts, paper_era, scan_paper_log};
use crate::risk_inputs::{audited_halt_release, paper_latency_samples};
use crate::runtime_config::{
    AppliedWatchlistCapacity, ConfigEra, ConfigRow, LiveRuntimeConfig, RISK_HALT_RELEASE_HASH_KEY,
    RuntimeConfigStatus, WatchlistCapacityEpoch, parse_config,
};
use crate::supabase_reader::{SupabaseError, auth_token};

/// Seconds between `service_config` polls. Boot-frozen (the poll cadence cannot govern itself).
/// Canonical default lives in `docs/_GLOSSARY.md`: `config_poll_interval_secs`.
pub const CONFIG_POLL_INTERVAL_SECS: u64 = 30;

/// Non-economic incident control separated from one fetched config proposal (#545).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionedConfigRows {
    pub economic_rows: Vec<ConfigRow>,
    pub risk_halt_release_hash: Option<String>,
    pub warning: Option<RiskHaltReleaseRowWarning>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskHaltReleaseRowWarning {
    Duplicate,
    WrongValueType,
    Malformed,
}

/// Remove the optional audited incident-control row before economic parsing and hashing.
#[must_use]
pub fn partition_risk_halt_release_hash(rows: &[ConfigRow]) -> PartitionedConfigRows {
    let mut economic_rows = Vec::with_capacity(rows.len());
    let release_rows = rows
        .iter()
        .filter(|row| row.key == RISK_HALT_RELEASE_HASH_KEY)
        .collect::<Vec<_>>();
    economic_rows.extend(
        rows.iter()
            .filter(|row| row.key != RISK_HALT_RELEASE_HASH_KEY)
            .cloned(),
    );

    let (risk_halt_release_hash, warning) = match release_rows.as_slice() {
        [] => (None, None),
        [row] if row.value_type != "text" => {
            (None, Some(RiskHaltReleaseRowWarning::WrongValueType))
        }
        [row] if row.value.is_empty() => (None, None),
        [row]
            if row.value.len() == 64
                && row
                    .value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) =>
        {
            (Some(row.value.clone()), None)
        }
        [_] => (None, Some(RiskHaltReleaseRowWarning::Malformed)),
        _ => (None, Some(RiskHaltReleaseRowWarning::Duplicate)),
    };
    PartitionedConfigRows {
        economic_rows,
        risk_halt_release_hash,
        warning,
    }
}

/// A config read may consume only part of a poll period. This prevents a stalled socket from
/// stopping later 30-second reloads indefinitely.
const CONFIG_FETCH_TIMEOUT_SECS: u64 = 20;

/// Failed capacity transitions retry independently of config reads at this cadence.
const CAPACITY_RETRY_INTERVAL_SECS: u64 = 30;

/// One coalescing capacity request. The generation distinguishes superseded requests even when
/// an operator changes A -> B -> A while the first A is still preparing.
pub type CapacityRequest = WatchlistCapacityEpoch;

/// Sender side of the coalescing request channel.
///
/// Request publication shares the structural-writer mutex. Consequently a request change and a
/// final membership swap have a total order: a transition either observes itself as current and
/// commits, or the newer request wins first and the old transition is cancelled as superseded.
#[derive(Clone)]
pub struct CapacityRequestHandle {
    tx: watch::Sender<CapacityRequest>,
    writer_lock: Arc<Mutex<()>>,
}

impl CapacityRequestHandle {
    /// Current desired target/generation without marking it observed by any worker.
    pub fn current(&self) -> CapacityRequest {
        *self.tx.borrow()
    }

    /// Publish `target` only when it differs from the current desired value.
    pub async fn request(&self, target: usize) -> CapacityRequest {
        let _writer = self.writer_lock.lock().await;
        self.tx.send_if_modified(|current| {
            if current.target == target {
                false
            } else {
                *current = CapacityRequest {
                    generation: current.generation.wrapping_add(1),
                    target,
                };
                true
            }
        });
        self.current()
    }
}

/// Build the process-local desired-capacity channel, initially aligned with boot membership.
pub fn capacity_request_channel(
    initial: usize,
    writer_lock: Arc<Mutex<()>>,
) -> (CapacityRequestHandle, watch::Receiver<CapacityRequest>) {
    let (tx, rx) = watch::channel(CapacityRequest {
        generation: 0,
        target: initial,
    });
    (CapacityRequestHandle { tx, writer_lock }, rx)
}

/// A completed capacity generation sent back to the sole runtime-config writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityApplyResult {
    request: CapacityRequest,
    actual: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum CapacityWorkerError {
    #[error("capacity request channel closed")]
    RequestChannelClosed,
    #[error("capacity result channel closed")]
    ResultChannelClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigPollError {
    #[error("capacity result channel closed")]
    CapacityResultChannelClosed,
}

/// Applies the optional incident release through the paper serializer. Re-reading the paper log
/// makes the row naturally consume-once: after the synchronized release edge the named cause is no
/// longer active and the same hash cannot match again.
#[derive(Clone)]
pub struct RiskHaltReleaseHandle {
    paper_log_path: PathBuf,
    source_log_path: PathBuf,
    control: mpsc::Sender<OrchestratorControl>,
}

/// Orders the active qualification seal check before an economic proposal is published.
#[derive(Clone)]
pub struct QualificationSealHandle {
    control: mpsc::Sender<OrchestratorControl>,
}

impl QualificationSealHandle {
    pub fn new(control: mpsc::Sender<OrchestratorControl>) -> Self {
        Self { control }
    }

    pub async fn apply(
        &self,
        proposed_economic_hash: String,
        proposed_financial_semantic_version: u32,
    ) -> Result<(), String> {
        let (acknowledged, response) = tokio::sync::oneshot::channel();
        self.control
            .send(OrchestratorControl::SealCheck {
                proposed_economic_hash,
                proposed_financial_semantic_version,
                acknowledged,
            })
            .await
            .map_err(|_| "orchestrator control channel closed during seal check".to_owned())?;
        response
            .await
            .map_err(|_| "orchestrator dropped seal-check acknowledgement".to_owned())?
    }
}

impl RiskHaltReleaseHandle {
    pub fn new(
        paper_log_path: PathBuf,
        source_log_path: PathBuf,
        control: mpsc::Sender<OrchestratorControl>,
    ) -> Self {
        Self {
            paper_log_path,
            source_log_path,
            control,
        }
    }

    pub async fn apply(&self, release_hash: &str) -> Result<(), String> {
        let era =
            paper_era(scan_paper_log(&self.paper_log_path).map_err(|error| error.to_string())?);
        let active = active_risk_halts(&era);
        let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
        let latest_latency_p95_ms = paper_latency_samples(&era, &self.source_log_path, now_unix)
            .map_err(|error| format!("derive release latency evidence: {error}"))?
            .latest
            .p95_ms;
        let Some(release) =
            audited_halt_release(&era, &active, release_hash, latest_latency_p95_ms)
        else {
            return Ok(());
        };
        let (acknowledged, response) = tokio::sync::oneshot::channel();
        self.control
            .send(OrchestratorControl::RiskHaltChange {
                owner: release.owner,
                cause: release.cause,
                state: HaltState::Released,
                evidence: serde_json::json!({
                    "engaged_receipt": release.engaged_receipt,
                    "release_hash": release_hash,
                    "latest_latency_p95_ms": latest_latency_p95_ms,
                }),
                acknowledged,
            })
            .await
            .map_err(|_| "orchestrator control channel closed during risk release".to_owned())?;
        response
            .await
            .map_err(|_| "orchestrator dropped risk release acknowledgement".to_owned())?
            .map(|_| ())
    }
}

fn publish_generation_health(
    health: Option<&SharedHealth>,
    capacity_requests: &CapacityRequestHandle,
    applied_capacity: &AppliedWatchlistCapacity,
) {
    if let Some(health) = health {
        let mut health = health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        health.configuration_generation_pending =
            capacity_requests.current() != applied_capacity.load();
    }
}

/// PostgREST URL selecting every `service_config` row.
fn service_config_url(base_url: &str) -> String {
    format!(
        "{}/rest/v1/service_config?select=key,value,value_type",
        base_url.trim_end_matches('/')
    )
}

/// Fetch all `service_config` rows. The same token goes in BOTH the `apikey` and
/// `Authorization: Bearer` headers (see [`auth_token`]); prefer the service-role secret. Public
/// so `main` can do the one-shot boot fetch before spawning the loop.
pub async fn fetch_service_config(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
) -> Result<Vec<ConfigRow>, SupabaseError> {
    let url = service_config_url(base_url);
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
    resp.json().await.map_err(SupabaseError::Decode)
}

/// A source of `service_config` rows. Trait-injected so [`poll_once`] is testable without network.
pub trait ConfigFetcher: Send + Sync {
    fn fetch(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<ConfigRow>, SupabaseError>> + Send;
}

/// Applies one desired capacity generation.
///
/// On `Ok`, the implementation must already have atomically published membership and updated
/// [`AppliedWatchlistCapacity`] while holding the structural-writer mutex. On `Err`, membership
/// and applied capacity must be unchanged.
pub trait WatchlistCapacityApplier: Send + Sync {
    /// Reconcile the live set to `request.target` and return the resulting actual size.
    fn apply(
        &self,
        request: CapacityRequest,
    ) -> impl std::future::Future<Output = Result<usize, String>> + Send;
}

/// Production fetcher: a PostgREST GET against `service_config`.
pub struct SupabaseConfigFetcher {
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
}

impl SupabaseConfigFetcher {
    pub fn new(
        client: reqwest::Client,
        base_url: String,
        anon_key: String,
        secret_key: String,
    ) -> Self {
        Self {
            client,
            base_url,
            anon_key,
            secret_key,
        }
    }
}

impl ConfigFetcher for SupabaseConfigFetcher {
    async fn fetch(&self) -> Result<Vec<ConfigRow>, SupabaseError> {
        fetch_service_config(
            &self.client,
            &self.base_url,
            &self.anon_key,
            &self.secret_key,
        )
        .await
    }
}

async fn fetch_with_timeout<F: ConfigFetcher>(
    fetcher: &F,
    timeout: Duration,
) -> Result<Vec<ConfigRow>, Option<SupabaseError>> {
    match tokio::time::timeout(timeout, fetcher.fetch()).await {
        Ok(Ok(rows)) => Ok(rows),
        Ok(Err(error)) => Err(Some(error)),
        Err(_) => Err(None),
    }
}

/// One bounded poll cycle: fetch, parse, publish ordinary edits, and coalesce a capacity request.
/// A potentially long capacity transition is never awaited here.
#[allow(clippy::too_many_arguments)]
pub async fn poll_once<F: ConfigFetcher>(
    live: &LiveRuntimeConfig,
    status: &RuntimeConfigStatus,
    fetcher: &F,
    capacity_requests: &CapacityRequestHandle,
    clob_creds_present: bool,
    era: ConfigEra,
    risk_release: Option<&RiskHaltReleaseHandle>,
    qualification_seal: Option<&QualificationSealHandle>,
) {
    let rows =
        match fetch_with_timeout(fetcher, Duration::from_secs(CONFIG_FETCH_TIMEOUT_SECS)).await {
            Ok(rows) => rows,
            Err(Some(error)) => {
                warn!(%error, "service_config poll failed; keeping last-known-good config");
                return;
            }
            Err(None) => {
                warn!(
                    timeout_secs = CONFIG_FETCH_TIMEOUT_SECS,
                    "service_config poll timed out; keeping last-known-good config"
                );
                return;
            }
        };

    let partitioned = partition_risk_halt_release_hash(&rows);
    if let Some(warning) = partitioned.warning {
        warn!(?warning, "risk halt release row ignored");
    }
    let applied = live.snapshot();
    let parsed = match parse_config(
        &partitioned.economic_rows,
        &applied,
        clob_creds_present,
        era,
    ) {
        Ok(parsed) => parsed,
        Err(error) => {
            warn!(%error, "service_config snapshot rejected; keeping whole last-good config");
            status.record_rejected(&partitioned.economic_rows, error);
            return;
        }
    };
    let requested_target = parsed.active_watchlist_size;

    if let Some(handle) = qualification_seal
        && let Err(error) = handle
            .apply(
                parsed.canonical_hash(),
                crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION,
            )
            .await
    {
        warn!(%error, "qualification seal check was not synchronized; keeping last-known-good config");
        return;
    }

    if let (Some(handle), Some(release_hash)) =
        (risk_release, partitioned.risk_halt_release_hash.as_deref())
        && let Err(error) = handle.apply(release_hash).await
    {
        warn!(%error, "risk halt release was not synchronized; keeping last-known-good config");
        return;
    }

    // This task is the sole RuntimeConfig writer. Other valid edits take effect immediately;
    // active_watchlist_size continues to describe the last successfully applied membership cap.
    let mut immediately_applied = parsed;
    immediately_applied.active_watchlist_size = applied.active_watchlist_size;
    live.store(immediately_applied.clone());
    status.record_applied(&immediately_applied);
    let request = capacity_requests.request(requested_target).await;
    if request.target != applied.active_watchlist_size {
        info!(
            target = request.target,
            generation = request.generation,
            "runtime watchlist capacity requested"
        );
    }
}

/// Dedicated, cancellable capacity worker. New requests cancel slow preparation and the watch
/// channel coalesces directly to the newest target. Failures retry without delaying config polls.
pub async fn run_capacity_worker<A: WatchlistCapacityApplier>(
    applier: A,
    applied_capacity: AppliedWatchlistCapacity,
    mut requests: watch::Receiver<CapacityRequest>,
    results: mpsc::Sender<CapacityApplyResult>,
) -> Result<(), CapacityWorkerError> {
    loop {
        let request = *requests.borrow_and_update();
        if applied_capacity.load() == request {
            if requests.changed().await.is_err() {
                return Err(CapacityWorkerError::RequestChannelClosed);
            }
            continue;
        }

        let apply = applier.apply(request);
        tokio::pin!(apply);
        let outcome = tokio::select! {
            biased;
            changed = requests.changed() => {
                if changed.is_err() {
                    return Err(CapacityWorkerError::RequestChannelClosed);
                }
                continue;
            }
            outcome = &mut apply => outcome,
        };

        match outcome {
            Ok(actual)
                if actual > 0 && actual <= request.target && applied_capacity.load() == request =>
            {
                if results
                    .send(CapacityApplyResult { request, actual })
                    .await
                    .is_err()
                {
                    return Err(CapacityWorkerError::ResultChannelClosed);
                }
            }
            Ok(actual) => warn!(
                target = request.target,
                generation = request.generation,
                actual,
                applied = applied_capacity.load().target,
                "runtime watchlist capacity returned an invalid/incomplete commit; retrying"
            ),
            Err(error) => warn!(
                target = request.target,
                generation = request.generation,
                %error,
                "runtime watchlist capacity change failed; retrying"
            ),
        }

        if applied_capacity.load() == request {
            continue;
        }
        tokio::select! {
            changed = requests.changed() => {
                if changed.is_err() {
                    return Err(CapacityWorkerError::RequestChannelClosed);
                }
            }
            () = tokio::time::sleep(Duration::from_secs(CAPACITY_RETRY_INTERVAL_SECS)) => {}
        }
    }
}

/// Run the fixed-cadence config coordinator. The boot fetch has already happened, so the first
/// periodic fetch is one full interval after startup. Capacity results are committed immediately
/// between ticks without creating a second RuntimeConfig writer.
#[allow(clippy::too_many_arguments)]
pub async fn run_config_poll_loop<F: ConfigFetcher>(
    live: LiveRuntimeConfig,
    status: RuntimeConfigStatus,
    fetcher: F,
    capacity_requests: CapacityRequestHandle,
    applied_capacity: AppliedWatchlistCapacity,
    mut capacity_results: mpsc::Receiver<CapacityApplyResult>,
    interval_secs: u64,
    clob_creds_present: bool,
    era: ConfigEra,
    health: Option<SharedHealth>,
    risk_release: Option<RiskHaltReleaseHandle>,
    qualification_seal: Option<QualificationSealHandle>,
) -> Result<(), ConfigPollError> {
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await; // consume interval's immediate first tick; boot already fetched once
    info!(interval_secs, "service_config poll loop started");

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                poll_once(
                    &live,
                    &status,
                    &fetcher,
                    &capacity_requests,
                    clob_creds_present,
                    era,
                    risk_release.as_ref(),
                    qualification_seal.as_ref(),
                ).await;
                publish_generation_health(health.as_ref(), &capacity_requests, &applied_capacity);
            }
            result = capacity_results.recv() => {
                match result {
                    // Publish every result that still describes the membership actually applied.
                    // The desired request may already be newer; retaining this intermediate
                    // successful cap is still the truthful last-known-good value if that newer
                    // transition subsequently fails.
                    Some(result) if applied_capacity.load() == result.request =>
                    {
                        let mut next = live.snapshot().as_ref().clone();
                        next.active_watchlist_size = result.request.target;
                        live.store(next.clone());
                        status.record_applied(&next);
                        info!(
                            target = result.request.target,
                            generation = result.request.generation,
                            actual = result.actual,
                            "runtime watchlist capacity applied"
                        );
                        publish_generation_health(health.as_ref(), &capacity_requests, &applied_capacity);
                    }
                    Some(result) => info!(
                        target = result.request.target,
                        generation = result.request.generation,
                        "ignoring superseded capacity completion"
                    ),
                    None => return Err(ConfigPollError::CapacityResultChannelClosed),
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use rust_decimal::Decimal;
    use tokio::sync::Notify;

    use super::*;
    use crate::config::ServiceConfig;
    use crate::runtime_config::RuntimeConfig;

    fn boot() -> RuntimeConfig {
        RuntimeConfig::from_service_config(&ServiceConfig::default())
    }

    fn cfg_row(key: &str, value: &str, value_type: &str) -> ConfigRow {
        ConfigRow {
            key: key.to_string(),
            value: value.to_string(),
            value_type: value_type.to_string(),
        }
    }

    fn complete_rows() -> Vec<ConfigRow> {
        [
            ("active_watchlist_size", "100", "integer"),
            ("mode", "paper", "text"),
            ("max_fill_price", "0.85", "decimal"),
            ("min_fill_price", "0.15", "decimal"),
            ("min_resolution_horizon_secs", "60", "integer"),
            ("max_resolution_horizon_secs", "172800", "integer"),
            ("price_impact_cap_bps", "100", "integer"),
            ("flip_human_approved", "false", "bool"),
            (
                "kelly_fraction_above_default_human_approved",
                "false",
                "bool",
            ),
            ("per_trade_cap", "unlimited", "text"),
            ("slippage_rate", "0.01", "decimal"),
            ("sizing_mode", "dollar", "text"),
            ("sizing_dollar_usd", "25", "decimal"),
            ("sizing_contracts", "1", "integer"),
        ]
        .into_iter()
        .map(|(key, value, value_type)| cfg_row(key, value, value_type))
        .collect()
    }

    fn rows_with(key: &str, value: &str) -> Vec<ConfigRow> {
        let mut rows = complete_rows();
        rows.iter_mut()
            .find(|row| row.key == key)
            .expect("complete row")
            .value = value.to_owned();
        rows
    }

    fn request_channel(
        initial: usize,
    ) -> (CapacityRequestHandle, watch::Receiver<CapacityRequest>) {
        capacity_request_channel(initial, Arc::new(Mutex::new(())))
    }

    struct OkFetcher(Vec<ConfigRow>);
    impl ConfigFetcher for OkFetcher {
        async fn fetch(&self) -> Result<Vec<ConfigRow>, SupabaseError> {
            Ok(self.0.clone())
        }
    }

    struct ErrFetcher;
    impl ConfigFetcher for ErrFetcher {
        async fn fetch(&self) -> Result<Vec<ConfigRow>, SupabaseError> {
            Err(SupabaseError::Status(503))
        }
    }

    struct PendingFetcher;
    impl ConfigFetcher for PendingFetcher {
        async fn fetch(&self) -> Result<Vec<ConfigRow>, SupabaseError> {
            std::future::pending().await
        }
    }

    #[test]
    fn url_trims_trailing_slash() {
        assert_eq!(CONFIG_POLL_INTERVAL_SECS, 30);
        assert_eq!(CONFIG_FETCH_TIMEOUT_SECS, 20);
        assert_eq!(
            service_config_url("https://x.supabase.co/"),
            "https://x.supabase.co/rest/v1/service_config?select=key,value,value_type"
        );
    }

    /// PASS: a valid incident hash is excluded from economic rows and round-trips exactly.
    #[test]
    fn partitions_valid_risk_halt_release_hash() {
        let mut rows = rows_with("max_fill_price", "0.50");
        rows.push(cfg_row(
            RISK_HALT_RELEASE_HASH_KEY,
            &"ab".repeat(32),
            "text",
        ));
        let partitioned = partition_risk_halt_release_hash(&rows);
        assert_eq!(partitioned.risk_halt_release_hash, Some("ab".repeat(32)));
        assert_eq!(partitioned.warning, None);
        assert!(
            partitioned
                .economic_rows
                .iter()
                .all(|row| row.key != RISK_HALT_RELEASE_HASH_KEY)
        );
        let without = parse_config(
            &rows[..rows.len() - 1],
            &boot(),
            false,
            ConfigEra::Financial15,
        )
        .unwrap();
        let partitioned_config = parse_config(
            &partitioned.economic_rows,
            &boot(),
            false,
            ConfigEra::Financial15,
        )
        .unwrap();
        assert_eq!(
            without.canonical_hash(),
            partitioned_config.canonical_hash()
        );
    }

    /// PASS: empty is unseeded, while malformed, uppercase, wrong-type, and duplicate rows are
    /// excluded with typed warnings and never reach the economic parser.
    #[test]
    fn invalid_release_rows_warn_without_poisoning_economics() {
        let cases = [
            (vec![cfg_row(RISK_HALT_RELEASE_HASH_KEY, "", "text")], None),
            (
                vec![cfg_row(RISK_HALT_RELEASE_HASH_KEY, "a", "text")],
                Some(RiskHaltReleaseRowWarning::Malformed),
            ),
            (
                vec![cfg_row(
                    RISK_HALT_RELEASE_HASH_KEY,
                    &"AB".repeat(32),
                    "text",
                )],
                Some(RiskHaltReleaseRowWarning::Malformed),
            ),
            (
                vec![cfg_row(
                    RISK_HALT_RELEASE_HASH_KEY,
                    &"ab".repeat(32),
                    "decimal",
                )],
                Some(RiskHaltReleaseRowWarning::WrongValueType),
            ),
            (
                vec![
                    cfg_row(RISK_HALT_RELEASE_HASH_KEY, &"ab".repeat(32), "text"),
                    cfg_row(RISK_HALT_RELEASE_HASH_KEY, &"cd".repeat(32), "text"),
                ],
                Some(RiskHaltReleaseRowWarning::Duplicate),
            ),
        ];
        for (release_rows, expected_warning) in cases {
            let mut rows = rows_with("max_fill_price", "0.50");
            rows.extend(release_rows);
            let partitioned = partition_risk_halt_release_hash(&rows);
            assert_eq!(partitioned.warning, expected_warning);
            assert!(partitioned.risk_halt_release_hash.is_none());
            assert!(
                parse_config(
                    &partitioned.economic_rows,
                    &boot(),
                    false,
                    ConfigEra::Financial15,
                )
                .is_ok()
            );
        }
    }

    #[tokio::test]
    async fn bounded_fetch_drops_a_stalled_request() {
        let result = fetch_with_timeout(&PendingFetcher, Duration::from_millis(1)).await;
        assert!(matches!(result, Err(None)));
    }

    #[tokio::test]
    async fn poll_once_applies_an_ordinary_edit() {
        let live = LiveRuntimeConfig::new(boot());
        let status = RuntimeConfigStatus::new(&live.snapshot());
        let (requests, _rx) = request_channel(100);
        poll_once(
            &live,
            &status,
            &OkFetcher(rows_with("max_fill_price", "0.50")),
            &requests,
            false,
            ConfigEra::Financial15,
            None,
            None,
        )
        .await;
        assert_eq!(live.snapshot().max_fill_price, Decimal::new(50, 2));
        assert_eq!(requests.current().target, 100);
    }

    /// PASS: a financial-era proposal is not published until the orchestrator acknowledges the
    /// exact economic hash and financial-semantic version carried by `SealCheck`.
    #[tokio::test]
    async fn poll_once_seal_check_precedes_config_publication() {
        let live = LiveRuntimeConfig::new(boot());
        let status = RuntimeConfigStatus::new(&live.snapshot());
        let (requests, _rx) = request_channel(100);
        let (control, mut control_rx) = mpsc::channel(1);
        let seal = QualificationSealHandle::new(control);
        let observed = live.clone();
        let responder = tokio::spawn(async move {
            let Some(message) = control_rx.recv().await else {
                return Err("control channel closed before SealCheck");
            };
            let OrchestratorControl::SealCheck {
                proposed_economic_hash,
                proposed_financial_semantic_version,
                acknowledged,
            } = message
            else {
                return Err("received another control before SealCheck");
            };
            assert_eq!(observed.snapshot().max_fill_price, Decimal::new(85, 2));
            assert_eq!(
                proposed_financial_semantic_version,
                crate::paper_recovery::FINANCIAL_SEMANTIC_VERSION
            );
            assert_eq!(
                proposed_economic_hash,
                parse_config(
                    &rows_with("max_fill_price", "0.50"),
                    &boot(),
                    false,
                    ConfigEra::Financial15,
                )
                .unwrap()
                .canonical_hash()
            );
            acknowledged
                .send(Ok(()))
                .map_err(|_| "seal acknowledgement receiver dropped")?;
            Ok(())
        });

        poll_once(
            &live,
            &status,
            &OkFetcher(rows_with("max_fill_price", "0.50")),
            &requests,
            false,
            ConfigEra::Financial15,
            None,
            Some(&seal),
        )
        .await;
        assert_eq!(responder.await.unwrap(), Ok(()));
        assert_eq!(live.snapshot().max_fill_price, Decimal::new(50, 2));
    }

    #[tokio::test]
    async fn poll_once_keeps_last_good_on_fetch_error() {
        let live = LiveRuntimeConfig::new(boot());
        let status = RuntimeConfigStatus::new(&live.snapshot());
        let (requests, _rx) = request_channel(100);
        let before = live.snapshot().max_fill_price;
        poll_once(
            &live,
            &status,
            &ErrFetcher,
            &requests,
            false,
            ConfigEra::Financial15,
            None,
            None,
        )
        .await;
        assert_eq!(live.snapshot().max_fill_price, before);
        assert_eq!(requests.current().target, 100);
    }

    #[tokio::test]
    async fn capacity_edit_is_queued_without_publishing_it_as_applied() {
        let live = LiveRuntimeConfig::new(boot());
        let status = RuntimeConfigStatus::new(&live.snapshot());
        let (requests, _rx) = request_channel(100);
        poll_once(
            &live,
            &status,
            &OkFetcher({
                let mut rows = rows_with("active_watchlist_size", "150");
                rows.iter_mut()
                    .find(|row| row.key == "max_fill_price")
                    .expect("complete row")
                    .value = "0.50".to_owned();
                rows
            }),
            &requests,
            false,
            ConfigEra::Financial15,
            None,
            None,
        )
        .await;
        let snapshot = live.snapshot();
        assert_eq!(snapshot.active_watchlist_size, 100);
        assert_eq!(snapshot.max_fill_price, Decimal::new(50, 2));
        assert_eq!(requests.current().target, 150);
        assert_eq!(status.snapshot().applied_hash, snapshot.canonical_hash());
    }

    #[tokio::test]
    async fn malformed_followup_retains_the_valid_pending_target() {
        let live = LiveRuntimeConfig::new(boot());
        let status = RuntimeConfigStatus::new(&live.snapshot());
        let (requests, _rx) = request_channel(100);
        poll_once(
            &live,
            &status,
            &OkFetcher(rows_with("active_watchlist_size", "150")),
            &requests,
            false,
            ConfigEra::Financial15,
            None,
            None,
        )
        .await;
        poll_once(
            &live,
            &status,
            &OkFetcher(rows_with("active_watchlist_size", "invalid")),
            &requests,
            false,
            ConfigEra::Financial15,
            None,
            None,
        )
        .await;
        assert_eq!(requests.current().target, 150);
        assert_eq!(live.snapshot().active_watchlist_size, 100);
    }

    struct CancellableApplier {
        applied: AppliedWatchlistCapacity,
        writer_lock: Arc<Mutex<()>>,
        calls: Arc<StdMutex<Vec<usize>>>,
        first_started: Arc<Notify>,
    }

    struct ImmediateApplier(AppliedWatchlistCapacity);

    impl WatchlistCapacityApplier for ImmediateApplier {
        async fn apply(&self, request: CapacityRequest) -> Result<usize, String> {
            self.0.store(request);
            Ok(request.target)
        }
    }

    #[tokio::test]
    async fn capacity_and_config_result_channel_loss_are_typed_owner_errors() {
        let applied = AppliedWatchlistCapacity::new(100);
        let (_requests, request_rx) = request_channel(150);
        let (result_tx, result_rx) = mpsc::channel(1);
        drop(result_rx);
        assert!(matches!(
            run_capacity_worker(
                ImmediateApplier(applied.clone()),
                applied,
                request_rx,
                result_tx,
            )
            .await,
            Err(CapacityWorkerError::ResultChannelClosed)
        ));

        let live = LiveRuntimeConfig::new(boot());
        let applied = AppliedWatchlistCapacity::new(100);
        let (requests, _request_rx) = request_channel(100);
        let (result_tx, result_rx) = mpsc::channel(1);
        drop(result_tx);
        assert!(matches!(
            run_config_poll_loop(
                live.clone(),
                RuntimeConfigStatus::new(&live.snapshot()),
                OkFetcher(Vec::new()),
                requests,
                applied,
                result_rx,
                3_600,
                false,
                ConfigEra::Financial15,
                None,
                None,
                None,
            )
            .await,
            Err(ConfigPollError::CapacityResultChannelClosed)
        ));
    }

    impl WatchlistCapacityApplier for CancellableApplier {
        async fn apply(&self, request: CapacityRequest) -> Result<usize, String> {
            self.calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(request.target);
            if request.target == 150 {
                self.first_started.notify_one();
                std::future::pending::<()>().await;
            }
            let _writer = self.writer_lock.lock().await;
            self.applied.store(request);
            Ok(request.target)
        }
    }

    #[tokio::test]
    async fn capacity_worker_cancels_slow_work_and_coalesces_to_latest_target() {
        let applied = AppliedWatchlistCapacity::new(100);
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let first_started = Arc::new(Notify::new());
        let (requests, rx) = request_channel(100);
        let (results_tx, mut results_rx) = mpsc::channel(2);
        let worker = tokio::spawn(run_capacity_worker(
            CancellableApplier {
                applied: applied.clone(),
                writer_lock: Arc::clone(&requests.writer_lock),
                calls: Arc::clone(&calls),
                first_started: Arc::clone(&first_started),
            },
            applied.clone(),
            rx,
            results_tx,
        ));

        requests.request(150).await;
        first_started.notified().await;
        requests.request(80).await;

        let result = tokio::time::timeout(Duration::from_secs(1), results_rx.recv())
            .await
            .expect("latest transition should complete")
            .expect("worker result channel should stay open");
        assert_eq!(result.request.target, 80);
        assert_eq!(result.actual, 80);
        assert_eq!(applied.load().target, 80);
        assert_eq!(
            *calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![150, 80]
        );
        worker.abort();
    }

    #[tokio::test]
    async fn capacity_worker_distinguishes_a_b_a_generations() {
        let applied = AppliedWatchlistCapacity::new(100);
        let calls = Arc::new(StdMutex::new(Vec::new()));
        let first_started = Arc::new(Notify::new());
        let (requests, rx) = request_channel(100);
        let (results_tx, mut results_rx) = mpsc::channel(2);
        let worker = tokio::spawn(run_capacity_worker(
            CancellableApplier {
                applied: applied.clone(),
                writer_lock: Arc::clone(&requests.writer_lock),
                calls: Arc::clone(&calls),
                first_started: Arc::clone(&first_started),
            },
            applied.clone(),
            rx,
            results_tx,
        ));

        let request_b = requests.request(150).await;
        first_started.notified().await;
        let request_a_again = requests.request(100).await;

        let result = tokio::time::timeout(Duration::from_secs(1), results_rx.recv())
            .await
            .expect("restored target should complete")
            .expect("worker result channel should stay open");
        assert_eq!(request_b.generation, 1);
        assert_eq!(request_a_again.generation, 2);
        assert_eq!(result.request, request_a_again);
        assert_eq!(applied.load(), request_a_again);
        assert_eq!(
            *calls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![150, 100]
        );
        worker.abort();
    }

    #[tokio::test]
    async fn coordinator_reports_an_applied_intermediate_cap_while_newer_work_is_pending() {
        let live = LiveRuntimeConfig::new(boot());
        let applied = AppliedWatchlistCapacity::new(100);
        let (requests, _rx) = request_channel(100);
        let (results_tx, results_rx) = mpsc::channel(2);
        let coordinator = tokio::spawn(run_config_poll_loop(
            live.clone(),
            RuntimeConfigStatus::new(&live.snapshot()),
            OkFetcher(Vec::new()),
            requests.clone(),
            applied.clone(),
            results_rx,
            3_600,
            false,
            ConfigEra::Financial15,
            None,
            None,
            None,
        ));

        let intermediate = requests.request(150).await;
        applied.store(intermediate);
        let pending = requests.request(80).await;
        results_tx
            .send(CapacityApplyResult {
                request: intermediate,
                actual: 150,
            })
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if live.snapshot().active_watchlist_size == 150 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("coordinator should report the last successful cap");
        assert_eq!(requests.current(), pending);
        assert_eq!(applied.load(), intermediate);
        coordinator.abort();
    }
}
