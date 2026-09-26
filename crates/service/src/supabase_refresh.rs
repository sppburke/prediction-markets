//! Sole Supabase watchlist refresh and serialized projection worker (#339, #544).
//!
//! Score refreshes remain fail-soft and never empty the live set. Every successful score refresh,
//! membership swap, or durable-fence removal coalesces through one bounded dirty signal. This task
//! snapshots effective membership (live minus durable fences) and replaces the public projection
//! through `service_watchlist_replace_v1` using the last applied `service_runtime.updated_at` token.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use pe_core_types::WalletAddress;
use pe_paper_state::PaperStateDb;
use pe_trader_index::Watchlist;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::sync::{Mutex, watch};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::live_watchlist::LiveWatchlist;
use crate::runtime_config::MAX_ACTIVE_WATCHLIST_SIZE;
use crate::supabase_reader::{self, SupabaseError};

#[derive(Debug, Error)]
pub enum BootScoreRefreshError {
    #[error("boot watchlist score refresh failed: {0}")]
    Fetch(#[from] SupabaseError),
    #[error("source producer start gate closed after boot score refresh")]
    ProducerGateClosed,
}

/// Fetch and apply the same survivor-score batch at boot and on the periodic cadence.
/// The writer lock keeps this score update serialized with membership publication.
async fn refresh_scores_once(
    live: &LiveWatchlist,
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    writer_lock: &Mutex<()>,
) -> Result<(usize, usize), SupabaseError> {
    let fresh = fetch_score_batch(client, base_url, anon_key, secret_key).await?;
    let fetched = fresh.entries.len();
    let live_total = {
        let _writer = writer_lock.lock().await;
        live.apply_refresh(&fresh)
    };
    Ok((fetched, live_total))
}

async fn fetch_score_batch(
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
) -> Result<Watchlist, SupabaseError> {
    let (fresh, _) = supabase_reader::fetch(
        client,
        base_url,
        anon_key,
        secret_key,
        MAX_ACTIVE_WATCHLIST_SIZE,
    )
    .await?;
    Ok(fresh)
}

/// Establish a current score for every boot-live wallet before producers can read it.
/// `apply_refresh` deliberately retains the replayed score of a wallet the current batch omits,
/// so boot removes such a wallet from live only. It stays structural; the next ranked change
/// decides its membership and can re-admit it with a current score.
pub async fn boot_refresh_scores_and_release(
    live: &LiveWatchlist,
    client: &reqwest::Client,
    base_url: &str,
    anon_key: &str,
    secret_key: &str,
    writer_lock: &Mutex<()>,
    producer_start: &watch::Sender<bool>,
) -> Result<(), BootScoreRefreshError> {
    let fresh = fetch_score_batch(client, base_url, anon_key, secret_key).await?;
    apply_boot_scores_and_release(live, &fresh, writer_lock, producer_start).await
}

async fn apply_boot_scores_and_release(
    live: &LiveWatchlist,
    fresh: &Watchlist,
    writer_lock: &Mutex<()>,
    producer_start: &watch::Sender<bool>,
) -> Result<(), BootScoreRefreshError> {
    let _writer = writer_lock.lock().await;
    let fetched_wallets = fresh
        .entries
        .iter()
        .map(|entry| entry.wallet)
        .collect::<HashSet<_>>();
    let unscored = live
        .snapshot()
        .entries
        .iter()
        .map(|entry| entry.wallet)
        .filter(|wallet| !fetched_wallets.contains(wallet))
        .collect::<HashSet<_>>();
    if !unscored.is_empty() {
        warn!(
            wallets = unscored.len(),
            "boot score refresh: wallets absent from the current batch leave live until a ranked change"
        );
        live.remove_fenced(&unscored);
    }
    live.apply_refresh(fresh);
    producer_start
        .send(true)
        .map_err(|_| BootScoreRefreshError::ProducerGateClosed)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectionEntry {
    pub wallet_hex: String,
    pub rank: i32,
    pub leader_score_bps: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ProjectionApply {
    pub new_token: String,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeToken {
    pub token: String,
    pub count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionErrorKind {
    MissingServiceRole,
    Transport,
    HttpStatus,
    Decode,
    Conflict,
    RuntimeRowMissing,
    FenceRead,
    RankOverflow,
    CountMismatch,
}

#[derive(Debug, Error)]
pub enum ProjectionError {
    #[error("Supabase service-role key is required for watchlist projection")]
    MissingServiceRole,
    #[error("watchlist projection transport failed: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("watchlist projection HTTP {status}: {body}")]
    Status { status: u16, body: String },
    #[error("watchlist projection response decode failed: {0}")]
    Decode(#[source] serde_json::Error),
    #[error("service_watchlist_replace_v1 compare-and-swap conflict")]
    Conflict,
    #[error("service_runtime row id=1 is missing")]
    RuntimeRowMissing,
    #[error("read durable wallet fences failed: {0}")]
    FenceRead(String),
    #[error("projection rank exceeds PostgreSQL integer range")]
    RankOverflow,
    #[error("projection RPC returned count {actual}, expected {expected}")]
    CountMismatch { expected: usize, actual: usize },
}

impl ProjectionError {
    pub const fn kind(&self) -> ProjectionErrorKind {
        match self {
            Self::MissingServiceRole => ProjectionErrorKind::MissingServiceRole,
            Self::Transport(_) => ProjectionErrorKind::Transport,
            Self::Status { .. } => ProjectionErrorKind::HttpStatus,
            Self::Decode(_) => ProjectionErrorKind::Decode,
            Self::Conflict => ProjectionErrorKind::Conflict,
            Self::RuntimeRowMissing => ProjectionErrorKind::RuntimeRowMissing,
            Self::FenceRead(_) => ProjectionErrorKind::FenceRead,
            Self::RankOverflow => ProjectionErrorKind::RankOverflow,
            Self::CountMismatch { .. } => ProjectionErrorKind::CountMismatch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectionStatusPoint {
    pub token: String,
    pub count: usize,
    pub time: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProjectionStatusError {
    pub kind: ProjectionErrorKind,
    pub time: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct WatchlistProjectionStatusSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending: Option<ProjectionStatusPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied: Option<ProjectionStatusPoint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<ProjectionStatusError>,
}

/// Shared projection status consumed by `status.json` and Lane J's supervisor status surface.
#[derive(Clone)]
pub struct WatchlistProjectionStatus {
    inner: Arc<ArcSwap<WatchlistProjectionStatusSnapshot>>,
}

impl Default for WatchlistProjectionStatus {
    fn default() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(
                WatchlistProjectionStatusSnapshot::default(),
            )),
        }
    }
}

impl WatchlistProjectionStatus {
    pub fn snapshot(&self) -> Arc<WatchlistProjectionStatusSnapshot> {
        self.inner.load_full()
    }

    fn pending(&self, token: &str, count: usize) {
        let current = self.inner.load_full();
        self.inner
            .store(Arc::new(WatchlistProjectionStatusSnapshot {
                pending: Some(point(token, count)),
                applied: current.applied.clone(),
                last_error: current.last_error.clone(),
            }));
    }

    fn applied(&self, token: &str, count: usize) {
        self.inner
            .store(Arc::new(WatchlistProjectionStatusSnapshot {
                pending: None,
                applied: Some(point(token, count)),
                last_error: None,
            }));
    }

    fn failed(&self, error: &ProjectionError) {
        let current = self.inner.load_full();
        self.inner
            .store(Arc::new(WatchlistProjectionStatusSnapshot {
                pending: current.pending.clone(),
                applied: current.applied.clone(),
                last_error: Some(ProjectionStatusError {
                    kind: error.kind(),
                    time: now_text(),
                    message: error.to_string(),
                }),
            }));
    }
}

fn point(token: &str, count: usize) -> ProjectionStatusPoint {
    ProjectionStatusPoint {
        token: token.to_owned(),
        count,
        time: now_text(),
    }
}

fn now_text() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

pub trait WatchlistProjector: Send + Sync {
    fn load_token(
        &self,
    ) -> impl std::future::Future<Output = Result<RuntimeToken, ProjectionError>> + Send;

    fn replace(
        &self,
        expected_token: &str,
        entries: &[ProjectionEntry],
    ) -> impl std::future::Future<Output = Result<ProjectionApply, ProjectionError>> + Send;
}

pub struct HttpWatchlistProjector {
    client: reqwest::Client,
    base_url: String,
    token: String,
}

impl HttpWatchlistProjector {
    pub fn new(
        client: reqwest::Client,
        base_url: &str,
        secret_key: &str,
    ) -> Result<Self, ProjectionError> {
        if secret_key.is_empty() {
            return Err(ProjectionError::MissingServiceRole);
        }
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_owned(),
            token: secret_key.to_owned(),
        })
    }

    fn headers(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header("apikey", &self.token).header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", self.token),
        )
    }
}

#[derive(Deserialize)]
struct RuntimeRow {
    watchlist_size: usize,
    updated_at: String,
}

#[derive(Deserialize)]
struct PostgrestError {
    code: Option<String>,
}

impl WatchlistProjector for HttpWatchlistProjector {
    async fn load_token(&self) -> Result<RuntimeToken, ProjectionError> {
        let url = format!(
            "{}/rest/v1/service_runtime?select=watchlist_size,updated_at&id=eq.1",
            self.base_url
        );
        let response = self
            .headers(self.client.get(url))
            .send()
            .await
            .map_err(ProjectionError::Transport)?;
        let status = response.status();
        let body = response.text().await.map_err(ProjectionError::Transport)?;
        if !status.is_success() {
            return Err(ProjectionError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let mut rows: Vec<RuntimeRow> =
            serde_json::from_str(&body).map_err(ProjectionError::Decode)?;
        let row = rows.pop().ok_or(ProjectionError::RuntimeRowMissing)?;
        if !rows.is_empty() {
            return Err(ProjectionError::Status {
                status: 200,
                body: "service_runtime id=1 returned multiple rows".to_owned(),
            });
        }
        Ok(RuntimeToken {
            token: row.updated_at,
            count: row.watchlist_size,
        })
    }

    async fn replace(
        &self,
        expected_token: &str,
        entries: &[ProjectionEntry],
    ) -> Result<ProjectionApply, ProjectionError> {
        let url = format!("{}/rest/v1/rpc/service_watchlist_replace_v1", self.base_url);
        let response = self
            .headers(self.client.post(url))
            .json(&serde_json::json!({
                "expected_token": expected_token,
                "entries": entries,
            }))
            .send()
            .await
            .map_err(ProjectionError::Transport)?;
        let status = response.status();
        let body = response.text().await.map_err(ProjectionError::Transport)?;
        if !status.is_success() {
            if serde_json::from_str::<PostgrestError>(&body)
                .ok()
                .and_then(|error| error.code)
                .as_deref()
                == Some("P5441")
            {
                return Err(ProjectionError::Conflict);
            }
            return Err(ProjectionError::Status {
                status: status.as_u16(),
                body,
            });
        }
        let mut rows: Vec<ProjectionApply> =
            serde_json::from_str(&body).map_err(ProjectionError::Decode)?;
        let applied = rows.pop().ok_or_else(|| ProjectionError::Status {
            status: 200,
            body: "projection RPC returned no result row".to_owned(),
        })?;
        if !rows.is_empty() {
            return Err(ProjectionError::Status {
                status: 200,
                body: "projection RPC returned multiple result rows".to_owned(),
            });
        }
        if applied.count != entries.len() {
            return Err(ProjectionError::CountMismatch {
                expected: entries.len(),
                actual: applied.count,
            });
        }
        Ok(applied)
    }
}

/// Snapshot effective projection rows from the canonical Rust score owner, removing every durable
/// fence even if a local membership transition has not yet published.
pub fn effective_projection_entries(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
) -> Result<Vec<ProjectionEntry>, ProjectionError> {
    let fenced: HashSet<WalletAddress> = paper_state
        .wallet_fences()
        .map_err(|error| ProjectionError::FenceRead(error.to_string()))?
        .into_iter()
        .map(|fence| fence.wallet)
        .collect();
    live.snapshot()
        .entries
        .iter()
        .filter(|entry| !fenced.contains(&entry.wallet))
        .enumerate()
        .map(|(index, entry)| {
            let rank = i32::try_from(index.saturating_add(1))
                .map_err(|_| ProjectionError::RankOverflow)?;
            Ok(ProjectionEntry {
                wallet_hex: entry.wallet.to_string(),
                rank,
                leader_score_bps: entry.leader_score_bps.0,
            })
        })
        .collect()
}

/// Execute one latest-state projection attempt. On conflict the caller clears its token and
/// retries from a fresh runtime-token read on the existing cadence.
pub async fn project_once<P: WatchlistProjector>(
    live: &LiveWatchlist,
    paper_state: &PaperStateDb,
    projector: &P,
    token: &mut Option<String>,
    status: &WatchlistProjectionStatus,
) -> Result<(), ProjectionError> {
    if token.is_none() {
        *token = Some(projector.load_token().await?.token);
    }
    let entries = effective_projection_entries(live, paper_state)?;
    let expected = token.as_deref().ok_or(ProjectionError::RuntimeRowMissing)?;
    status.pending(expected, entries.len());
    match projector.replace(expected, &entries).await {
        Ok(applied) => {
            status.applied(&applied.new_token, applied.count);
            *token = Some(applied.new_token);
            Ok(())
        }
        Err(ProjectionError::Conflict) => {
            *token = None;
            Err(ProjectionError::Conflict)
        }
        Err(error) => Err(error),
    }
}

/// Refresh scores and serialize projection writes in the same owner. Boot projects immediately;
/// later dirty signals project promptly, while failures and CAS conflicts retry on the refresh
/// cadence. Projection degradation never alters the live set or trading readiness.
#[allow(clippy::too_many_arguments)]
pub async fn run_supabase_refresh_loop(
    live: LiveWatchlist,
    paper_state: Arc<PaperStateDb>,
    client: reqwest::Client,
    base_url: String,
    anon_key: String,
    secret_key: String,
    interval_secs: u64,
    writer_lock: Arc<Mutex<()>>,
    mut dirty: watch::Receiver<u64>,
    projection_status: WatchlistProjectionStatus,
) {
    let projector = HttpWatchlistProjector::new(client.clone(), &base_url, &secret_key);
    let mut projection_token = None;
    if let Err(error) = &projector {
        projection_status.failed(error);
        warn!(%error, "watchlist projection unavailable; score refresh remains active");
    }

    // A zero score-refresh interval retains its documented disable semantic, but projection
    // retries still need a bounded cadence after an analytics-only failure.
    let refresh_enabled = interval_secs > 0;
    let interval = Duration::from_secs(if refresh_enabled { interval_secs } else { 30 });
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;
    let mut projection_pending = projector.is_ok();

    loop {
        let mut retry_projection = false;
        if projection_pending && let Ok(projector) = &projector {
            match project_once(
                &live,
                &paper_state,
                projector,
                &mut projection_token,
                &projection_status,
            )
            .await
            {
                Ok(()) => {}
                Err(error) => {
                    retry_projection = true;
                    projection_status.failed(&error);
                    warn!(%error, "watchlist projection failed; retrying on refresh cadence");
                }
            }
        }
        projection_pending = tokio::select! {
            changed = dirty.changed() => {
                if changed.is_err() {
                    return;
                }
                dirty.borrow_and_update();
                true
            }
            _ = ticker.tick() => {
                let mut next_projection = retry_projection;
                if refresh_enabled {
                    match refresh_scores_once(
                        &live,
                        &client,
                        &base_url,
                        &anon_key,
                        &secret_key,
                        &writer_lock,
                    ).await {
                        Ok((fetched, live_total)) => {
                            // Consume the generation emitted by `apply_refresh`: this branch
                            // already schedules that newest snapshot and must not project it twice.
                            dirty.borrow_and_update();
                            next_projection = true;
                            info!(fetch_limit = MAX_ACTIVE_WATCHLIST_SIZE, fetched, live_total, "live watchlist refreshed from supabase");
                        }
                        Err(error) => {
                            warn!(%error, "supabase refresh failed; keeping current watchlist");
                        }
                    }
                }
                // A prior projection failure retries even when the score fetch also failed.
                next_projection
            }
        };
    }
}

/// Preserve the old refresh error type in public signatures used by scenario helpers.
pub type RefreshError = SupabaseError;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp};
    use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};

    fn boot_live(wallet: WalletAddress, win_rate_bps: i32) -> LiveWatchlist {
        LiveWatchlist::new(Watchlist {
            entries: vec![WatchlistEntry {
                wallet,
                tier: WatchlistTier::Active,
                leader_score_bps: BasisPoints(100),
                lcb_5pct_bps: BasisPoints(0),
                win_rate_bps: BasisPoints(win_rate_bps),
                closed_trades_in_window: 0,
                reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            }],
            snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            active_count: 1,
            incubator_count: 0,
        })
    }

    #[tokio::test]
    async fn boot_reentry_score_is_current_before_first_producer_read() {
        let wallet = WalletAddress([41; 20]);
        // A live-only reentry used a newer score, while replay still starts from the old entry.
        let prior_live = boot_live(wallet, 6_000);
        prior_live.remove_fenced(&HashSet::from([wallet]));
        let mut reentry = boot_live(wallet, 8_500).snapshot().entries[0].clone();
        reentry.leader_score_bps = BasisPoints(900);
        prior_live.replace(&HashSet::new(), &[reentry], 1);
        assert_eq!(
            prior_live.snapshot().entries[0].win_rate_bps,
            BasisPoints(8_500)
        );
        let restarted = boot_live(wallet, 6_000);
        let fresh = boot_live(wallet, 8_500).snapshot();
        let (start, mut ready) = watch::channel(false);
        let producer_live = restarted.clone();
        let producer = tokio::spawn(async move {
            ready.wait_for(|started| *started).await.unwrap();
            producer_live.snapshot().entries[0].win_rate_bps
        });
        tokio::task::yield_now().await;
        assert!(!producer.is_finished());
        apply_boot_scores_and_release(&restarted, &fresh, &Mutex::new(()), &start)
            .await
            .unwrap();
        assert_eq!(producer.await.unwrap(), BasisPoints(8_500));
    }

    #[tokio::test]
    async fn failing_boot_score_fetch_does_not_release_producers() {
        let live = boot_live(WalletAddress([42; 20]), 6_000);
        let (start, ready) = watch::channel(false);
        let result = boot_refresh_scores_and_release(
            &live,
            &reqwest::Client::new(),
            "not a URL",
            "anon",
            "",
            &Mutex::new(()),
            &start,
        )
        .await;
        assert!(matches!(
            result,
            Err(BootScoreRefreshError::Fetch(SupabaseError::Transport(_)))
        ));
        assert!(!*ready.borrow());
        assert_eq!(live.snapshot().entries[0].win_rate_bps, BasisPoints(6_000));
    }

    #[tokio::test]
    async fn boot_batch_missing_live_wallet_leaves_live_only() {
        let wallet = WalletAddress([43; 20]);
        let live = boot_live(wallet, 6_000);
        let structural = live.structural_membership();
        let fresh = boot_live(WalletAddress([44; 20]), 8_500).snapshot();
        let (start, ready) = watch::channel(false);
        apply_boot_scores_and_release(&live, &fresh, &Mutex::new(()), &start)
            .await
            .unwrap();
        assert!(*ready.borrow());
        assert!(
            live.snapshot()
                .entries
                .iter()
                .all(|entry| entry.wallet != wallet)
        );
        assert_eq!(live.structural_membership(), structural);
    }

    #[test]
    fn rpc_payload_has_score_owner_fields() {
        let entries = [ProjectionEntry {
            wallet_hex: "0x1111111111111111111111111111111111111111".to_owned(),
            rank: 1,
            leader_score_bps: 2500,
        }];
        let value = serde_json::json!({
            "expected_token": "2026-09-01T00:00:00Z",
            "entries": entries,
        });
        assert_eq!(value["entries"][0]["rank"], 1);
        assert_eq!(value["entries"][0]["leader_score_bps"], 2500);
    }

    #[test]
    fn missing_secret_is_typed_analytics_degradation() {
        let result =
            HttpWatchlistProjector::new(reqwest::Client::new(), "https://example.test", "");
        assert!(matches!(result, Err(ProjectionError::MissingServiceRole)));
    }

    #[test]
    fn status_keeps_pending_and_applied_generations_with_typed_error() {
        let status = WatchlistProjectionStatus::default();
        status.pending("old-token", 2);
        status.failed(&ProjectionError::Conflict);

        let failed = status.snapshot();
        assert_eq!(failed.pending.as_ref().unwrap().token, "old-token");
        assert_eq!(failed.pending.as_ref().unwrap().count, 2);
        assert_eq!(
            failed.last_error.as_ref().unwrap().kind,
            ProjectionErrorKind::Conflict
        );

        status.applied("new-token", 2);
        let applied = status.snapshot();
        assert!(applied.pending.is_none());
        assert!(applied.last_error.is_none());
        assert_eq!(applied.applied.as_ref().unwrap().token, "new-token");
        assert_eq!(applied.applied.as_ref().unwrap().count, 2);
    }
    #[tokio::test]
    async fn score_refresh_fetches_the_survivor_bench_without_changing_membership() {
        use crate::live_watchlist::projection_dirty_channel;
        use axum::{Json, Router, extract::Query, routing::get};
        use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
        use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
        let retained = WalletAddress([3; 20]);
        let app = Router::new().route("/rest/v1/latest_ranking", get(move |Query(query): Query<std::collections::HashMap<String, String>>| async move {
            assert_eq!(query["limit"], MAX_ACTIVE_WATCHLIST_SIZE.to_string());
            assert_eq!(query["survives"], "is.true");
            let rows = [WalletAddress([1; 20]), WalletAddress([2; 20]), retained].map(|wallet| serde_json::json!({
                "wallet_hex": wallet.to_string(), "hit_rate_text": "0.6", "ls_tstat_text": "2", "n_trades": 10,
            }));
            Json(rows.into_iter().take(query["limit"].parse::<usize>().unwrap()).collect::<Vec<_>>())
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (dirty, rx) = projection_dirty_channel();
        let mut observed = rx.clone();
        let live = LiveWatchlist::new_with_projection(
            Watchlist {
                entries: vec![WatchlistEntry {
                    wallet: retained,
                    tier: WatchlistTier::Active,
                    leader_score_bps: BasisPoints(0),
                    lcb_5pct_bps: BasisPoints(0),
                    win_rate_bps: BasisPoints(0),
                    closed_trades_in_window: 0,
                    reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
                }],
                snapshot_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
                active_count: 1,
                incubator_count: 0,
            },
            dirty,
        );
        let dir = tempfile::tempdir().unwrap();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        let task = tokio::spawn(run_supabase_refresh_loop(
            live.clone(),
            paper,
            reqwest::Client::new(),
            base,
            "anon".to_owned(),
            String::new(),
            1,
            Arc::new(Mutex::new(())),
            rx,
            WatchlistProjectionStatus::default(),
        ));
        tokio::time::timeout(Duration::from_secs(5), observed.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(live.snapshot().entries.len(), 1);
        assert_eq!(live.snapshot().entries[0].wallet, retained);
        assert_eq!(
            live.snapshot().entries[0].leader_score_bps,
            BasisPoints(2_000)
        );
        task.abort();
        server.abort();
    }
}
