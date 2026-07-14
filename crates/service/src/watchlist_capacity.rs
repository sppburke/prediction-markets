//! Supabase-backed runtime watchlist-capacity changes.
//!
//! A config-driven grow is deliberately more than an `ArcSwap` replacement: every newly
//! admitted wallet has its prior-market history and current positions loaded first, those maps
//! are applied by the single-owner orchestrator, and only then is the new membership generation
//! published. Shrinks use the same atomic full-rerank primitive. Any failure leaves membership
//! and the last-known-good capacity unchanged so the next config poll can retry.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pe_core_types::WalletAddress;
use pe_paper_state::PaperStateDb;
use pe_source_polymarket_public::ReqwestFetcher;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tracing::{info, warn};

use crate::config_poller::{CapacityRequest, WatchlistCapacityApplier};
use crate::live_watchlist::LiveWatchlist;
use crate::orchestrator_control::OrchestratorControl;
use crate::position_seeder::seed_all;
use crate::runtime_config::AppliedWatchlistCapacity;
use crate::supabase_reader::{self, SupabaseError};
use crate::supabase_refresh::{HttpWatchlistPublisher, WatchlistSizePublisher};
use crate::wallet_history::WalletHistoryLoader;
use crate::watchlist_maintenance::{MembershipApplyError, apply_ranked_membership_locked};

/// Maximum time to wait for the orchestrator to apply pre-admission history and positions.
const ADMISSION_PREPARE_ACK_TIMEOUT_SECS: u64 = 30;

/// Failure surface for one capacity transition. Every variant is fail-soft to the caller.
#[derive(Debug, thiserror::Error)]
enum CapacityError {
    #[error("fetch top-ranked wallets: {0}")]
    Ranking(#[from] SupabaseError),
    #[error("latest_ranking returned no valid wallets for target {target}")]
    EmptyRanking { target: usize },
    #[error("latest_ranking returned duplicate wallets ({unique} unique of {rows} rows)")]
    DuplicateRanking { unique: usize, rows: usize },
    #[error("build admission HTTP client: {0}")]
    BuildClient(reqwest::Error),
    #[error("market history unavailable for {missing} newly admitted wallet(s)")]
    MissingHistory { missing: usize },
    #[error("current positions unavailable for {missing} newly admitted wallet(s)")]
    MissingPositions { missing: usize },
    #[error("orchestrator control channel closed before admission preparation")]
    ControlClosed,
    #[error("orchestrator admission preparation acknowledgement closed")]
    AcknowledgementClosed,
    #[error("orchestrator admission preparation exceeded {0} seconds")]
    AcknowledgementTimeout(u64),
    #[error("capacity request was superseded before membership commit")]
    Superseded,
    #[error("{missing} newly admitted wallet(s) were not admission-ready at commit")]
    UnpreparedAdmission { missing: usize },
    #[error("apply ranked membership: {0}")]
    Membership(#[from] MembershipApplyError),
}

/// Production capacity applier used by the 30-second `service_config` poller.
pub struct SupabaseWatchlistCapacity {
    live: LiveWatchlist,
    paper_state: Arc<PaperStateDb>,
    writer_lock: Arc<Mutex<()>>,
    applied_capacity: AppliedWatchlistCapacity,
    desired_capacity: watch::Receiver<CapacityRequest>,
    control_tx: mpsc::Sender<OrchestratorControl>,
    client: reqwest::Client,
    supabase_url: String,
    supabase_anon_key: String,
    supabase_secret_key: String,
    polymarket_base_url: String,
    wallet_history_path: PathBuf,
    position_page_limit: u32,
    position_size_threshold: u32,
}

impl SupabaseWatchlistCapacity {
    /// Build the production applier. All credentials remain private fields and are never logged.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        live: LiveWatchlist,
        paper_state: Arc<PaperStateDb>,
        writer_lock: Arc<Mutex<()>>,
        applied_capacity: AppliedWatchlistCapacity,
        desired_capacity: watch::Receiver<CapacityRequest>,
        control_tx: mpsc::Sender<OrchestratorControl>,
        client: reqwest::Client,
        supabase_url: String,
        supabase_anon_key: String,
        supabase_secret_key: String,
        polymarket_base_url: String,
        wallet_history_path: PathBuf,
        position_page_limit: u32,
        position_size_threshold: u32,
    ) -> Self {
        Self {
            live,
            paper_state,
            writer_lock,
            applied_capacity,
            desired_capacity,
            control_tx,
            client,
            supabase_url,
            supabase_anon_key,
            supabase_secret_key,
            polymarket_base_url,
            wallet_history_path,
            position_page_limit,
            position_size_threshold,
        }
    }

    async fn prepare_additions(&self, additions: &[WalletAddress]) -> Result<(), CapacityError> {
        if additions.is_empty() {
            return Ok(());
        }

        // Match startup's bounded HTTP posture. The history loader persists its merged sidecar,
        // making a retry incremental; position snapshots include successful empty portfolios.
        let history_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(CapacityError::BuildClient)?;
        let history_fetcher = ReqwestFetcher::new(history_client);
        let mut history = WalletHistoryLoader::load(
            additions,
            &self.polymarket_base_url,
            &self.wallet_history_path,
            &history_fetcher,
        )
        .await;
        let missing_history = additions
            .iter()
            .filter(|wallet| !history.contains_key(wallet))
            .count();
        if missing_history > 0 {
            return Err(CapacityError::MissingHistory {
                missing: missing_history,
            });
        }
        let addition_set: HashSet<WalletAddress> = additions.iter().copied().collect();
        history.retain(|wallet, _| addition_set.contains(wallet));

        let position_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .map_err(CapacityError::BuildClient)?;
        let position_fetcher = ReqwestFetcher::new(position_client);
        let positions = seed_all(
            additions,
            &self.polymarket_base_url,
            self.position_page_limit,
            self.position_size_threshold,
            &position_fetcher,
        )
        .await;
        let missing_positions = additions
            .iter()
            .filter(|wallet| !positions.contains_key(wallet))
            .count();
        if missing_positions > 0 {
            return Err(CapacityError::MissingPositions {
                missing: missing_positions,
            });
        }

        let (acknowledged, acknowledgement) = oneshot::channel();
        let command = OrchestratorControl::PrepareAdmissions {
            history,
            positions,
            acknowledged,
        };
        tokio::time::timeout(
            Duration::from_secs(ADMISSION_PREPARE_ACK_TIMEOUT_SECS),
            async {
                self.control_tx
                    .send(command)
                    .await
                    .map_err(|_| CapacityError::ControlClosed)?;
                acknowledgement
                    .await
                    .map_err(|_| CapacityError::AcknowledgementClosed)
            },
        )
        .await
        .map_err(|_| CapacityError::AcknowledgementTimeout(ADMISSION_PREPARE_ACK_TIMEOUT_SECS))??;
        Ok(())
    }

    async fn apply_inner(&self, request: CapacityRequest) -> Result<usize, CapacityError> {
        let target = request.target;
        let (incoming, incoming_last_trade) = supabase_reader::fetch(
            &self.client,
            &self.supabase_url,
            &self.supabase_anon_key,
            &self.supabase_secret_key,
            target,
        )
        .await?;
        if incoming.entries.is_empty() {
            return Err(CapacityError::EmptyRanking { target });
        }
        validate_unique_ranking(&incoming.entries)?;

        let initially_ready: HashSet<WalletAddress> = self
            .live
            .snapshot()
            .entries
            .iter()
            .map(|entry| entry.wallet)
            .collect();
        let additions: Vec<WalletAddress> = incoming
            .entries
            .iter()
            .map(|entry| entry.wallet)
            .filter(|wallet| !initially_ready.contains(wallet))
            .collect();
        self.prepare_additions(&additions).await?;

        let admission_ready: HashSet<WalletAddress> = initially_ready
            .into_iter()
            .chain(additions.iter().copied())
            .collect();
        let _writer = self.writer_lock.lock().await;
        if *self.desired_capacity.borrow() != request {
            return Err(CapacityError::Superseded);
        }
        let current_now: HashSet<WalletAddress> = self
            .live
            .snapshot()
            .entries
            .iter()
            .map(|entry| entry.wallet)
            .collect();
        let missing_ready = incoming
            .entries
            .iter()
            .take(target)
            .map(|entry| entry.wallet)
            .filter(|wallet| !current_now.contains(wallet) && !admission_ready.contains(wallet))
            .count();
        if missing_ready > 0 {
            return Err(CapacityError::UnpreparedAdmission {
                missing: missing_ready,
            });
        }
        let (actual, dropped) = apply_ranked_membership_locked(
            &self.live,
            &self.paper_state,
            &incoming.entries,
            &incoming_last_trade,
            target,
        )?;
        self.applied_capacity.store(request);
        drop(_writer);

        // Telemetry is downstream of membership. Never roll back a successful atomic swap because
        // the analytics upsert failed; detach the bounded HTTP call so it cannot delay the worker.
        if !self.supabase_secret_key.is_empty() {
            let publisher = HttpWatchlistPublisher::new(
                self.client.clone(),
                &self.supabase_url,
                &self.supabase_anon_key,
                &self.supabase_secret_key,
            );
            std::mem::drop(tokio::spawn(async move {
                if let Err(error) = publisher.publish(actual).await {
                    warn!(%error, "failed to publish resized watchlist count");
                }
            }));
        }

        info!(
            target,
            fetched = incoming.entries.len(),
            admitted = additions.len(),
            dropped = dropped.len(),
            actual,
            "runtime watchlist membership resized"
        );
        Ok(actual)
    }
}

fn validate_unique_ranking(
    entries: &[pe_trader_index::WatchlistEntry],
) -> Result<(), CapacityError> {
    let unique: HashSet<WalletAddress> = entries.iter().map(|entry| entry.wallet).collect();
    if unique.len() == entries.len() {
        Ok(())
    } else {
        Err(CapacityError::DuplicateRanking {
            unique: unique.len(),
            rows: entries.len(),
        })
    }
}

impl WatchlistCapacityApplier for SupabaseWatchlistCapacity {
    async fn apply(&self, request: CapacityRequest) -> Result<usize, String> {
        self.apply_inner(request)
            .await
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config_poller::capacity_request_channel;
    use axum::{Json, Router, routing::get};
    use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp};
    use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
    use tempfile::TempDir;
    use tokio::sync::Notify;

    fn entry(wallet: WalletAddress) -> WatchlistEntry {
        WatchlistEntry {
            wallet,
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(0),
            lcb_5pct_bps: BasisPoints(0),
            win_rate_bps: BasisPoints(0),
            closed_trades_in_window: 0,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        }
    }

    fn watchlist(entries: Vec<WatchlistEntry>) -> Watchlist {
        let active_count = entries.len();
        Watchlist {
            entries,
            snapshot_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
            active_count,
            incubator_count: 0,
        }
    }

    #[test]
    fn duplicate_ranking_detection_uses_wallet_identity() {
        let wallet = WalletAddress([7; 20]);
        let entries: Vec<WatchlistEntry> = vec![entry(wallet), entry(wallet)];
        assert!(matches!(
            validate_unique_ranking(&entries),
            Err(CapacityError::DuplicateRanking { unique: 1, rows: 2 })
        ));

        let distinct = vec![entry(WalletAddress([7; 20])), entry(WalletAddress([8; 20]))];
        assert!(validate_unique_ranking(&distinct).is_ok());
    }

    #[tokio::test]
    async fn production_transition_prepares_and_acks_before_membership_publication() {
        let existing = WalletAddress([1; 20]);
        let newcomer = WalletAddress([2; 20]);
        let newcomer_last_trade = 1_700_000_123_i64;
        let ranking = Arc::new(vec![
            serde_json::json!({
                "wallet_hex": existing.to_string(),
                "hit_rate": "0.60",
                "ls_tstat": "2.0",
                "n_trades": 10,
                "last_trade_unix": 1_700_000_100_i64
            }),
            serde_json::json!({
                "wallet_hex": newcomer.to_string(),
                "hit_rate": "0.55",
                "ls_tstat": "1.5",
                "n_trades": 8,
                "last_trade_unix": newcomer_last_trade
            }),
        ]);
        let app = Router::new()
            .route(
                "/rest/v1/latest_ranking",
                get({
                    let ranking = Arc::clone(&ranking);
                    move || {
                        let ranking = Arc::clone(&ranking);
                        async move { Json(ranking.as_ref().clone()) }
                    }
                }),
            )
            .route(
                "/activity",
                get(|| async {
                    Json(vec![serde_json::json!({
                        "conditionId": "0xprior-market",
                        "timestamp": 1_699_999_000_i64
                    })])
                }),
            )
            .route(
                "/positions",
                get(|| async { Json(Vec::<serde_json::Value>::new()) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let base_url = format!("http://{address}");

        let live = LiveWatchlist::new(watchlist(vec![entry(existing)]));
        let temp = TempDir::new().unwrap();
        let paper_state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
        let writer_lock = Arc::new(Mutex::new(()));
        let applied = AppliedWatchlistCapacity::new(1);
        let (requests, desired_rx) = capacity_request_channel(1, Arc::clone(&writer_lock));
        let request = requests.request(2).await;

        let (control_tx, mut control_rx) = mpsc::channel(1);
        let prepared = Arc::new(Notify::new());
        let release_ack = Arc::new(Notify::new());
        let live_at_control = live.clone();
        let prepared_task = Arc::clone(&prepared);
        let release_task = Arc::clone(&release_ack);
        let control = tokio::spawn(async move {
            let command = control_rx.recv().await.unwrap();
            match command {
                OrchestratorControl::PrepareAdmissions {
                    history,
                    positions,
                    acknowledged,
                } => {
                    assert_eq!(live_at_control.snapshot().entries.len(), 1);
                    assert!(history.contains_key(&newcomer));
                    assert!(positions.contains_key(&newcomer));
                    prepared_task.notify_one();
                    release_task.notified().await;
                    acknowledged.send(()).unwrap();
                }
                OrchestratorControl::PositionReseed(_) => {
                    panic!("capacity transition sent a periodic reseed")
                }
            }
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let applier = SupabaseWatchlistCapacity::new(
            live.clone(),
            Arc::clone(&paper_state),
            Arc::clone(&writer_lock),
            applied.clone(),
            desired_rx,
            control_tx,
            client,
            base_url.clone(),
            "anon".to_string(),
            String::new(),
            base_url,
            temp.path().join("history.json"),
            500,
            1,
        );
        let apply = tokio::spawn(async move { applier.apply(request).await });

        prepared.notified().await;
        assert_eq!(live.snapshot().entries.len(), 1);
        assert_eq!(applied.load().target, 1);
        assert_eq!(paper_state.cursor(&newcomer).unwrap(), None);

        release_ack.notify_one();
        assert_eq!(apply.await.unwrap().unwrap(), 2);
        control.await.unwrap();
        assert_eq!(live.snapshot().entries.len(), 2);
        assert_eq!(applied.load(), request);
        assert_eq!(
            paper_state.cursor(&newcomer).unwrap(),
            Some(newcomer_last_trade)
        );
        server.abort();
    }
}
