//! Supabase-backed runtime watchlist-capacity changes.
//!
//! A config-driven grow is deliberately more than an `ArcSwap` replacement: every newly
//! admitted wallet is prepared through the shared [`crate::watchlist_admission`] preparer — its
//! prior-market history and current positions are loaded, applied by the single-owner
//! orchestrator, and acknowledged — and only then is the new membership generation published.
//! Shrinks use the same atomic full-rerank primitive. Any failure leaves membership and the
//! last-known-good capacity unchanged for the worker's independent 30-second retry.

use std::collections::HashSet;
use std::sync::Arc;

use pe_core_types::WalletAddress;
use pe_paper_state::PaperStateDb;
use tokio::sync::{Mutex, watch};
use tracing::{info, warn};

use crate::config_poller::{CapacityRequest, WatchlistCapacityApplier};
use crate::live_watchlist::LiveWatchlist;
use crate::runtime_config::AppliedWatchlistCapacity;
use crate::supabase_reader::{self, SupabaseError};
use crate::supabase_refresh::{HttpWatchlistPublisher, WatchlistSizePublisher};
use crate::watchlist_admission::{AdmissionError, AdmissionPreparer};
use crate::watchlist_maintenance::{
    MembershipApplyError, apply_ranked_membership_locked, ranked_membership_change,
};

/// Failure surface for one capacity transition. Every variant is fail-soft to the caller.
#[derive(Debug, thiserror::Error)]
enum CapacityError {
    #[error("fetch top-ranked wallets: {0}")]
    Ranking(#[from] SupabaseError),
    #[error("latest_ranking returned no valid wallets for target {target}")]
    EmptyRanking { target: usize },
    #[error("latest_ranking returned duplicate wallets ({unique} unique of {rows} rows)")]
    DuplicateRanking { unique: usize, rows: usize },
    #[error("prepare admissions: {0}")]
    Admission(#[from] AdmissionError),
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
    preparer: AdmissionPreparer,
    client: reqwest::Client,
    supabase_url: String,
    supabase_anon_key: String,
    supabase_secret_key: String,
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
        preparer: AdmissionPreparer,
        client: reqwest::Client,
        supabase_url: String,
        supabase_anon_key: String,
        supabase_secret_key: String,
    ) -> Self {
        Self {
            live,
            paper_state,
            writer_lock,
            applied_capacity,
            desired_capacity,
            preparer,
            client,
            supabase_url,
            supabase_anon_key,
            supabase_secret_key,
        }
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

        let (_, additions) =
            ranked_membership_change(&self.live.snapshot().entries, &incoming.entries, target);
        self.preparer.prepare(&additions).await?;
        let prepared: HashSet<WalletAddress> = additions.iter().copied().collect();

        let _writer = self.writer_lock.lock().await;
        if *self.desired_capacity.borrow() != request {
            return Err(CapacityError::Superseded);
        }
        // Readiness is proven only by THIS attempt (#542). The admissions the locked apply will
        // publish are recomputed against current membership: a wallet that was live when the
        // additions were planned but has since been evicted by maintenance is a genuine new
        // admission that was never prepared; the worker retries and prepares it next round.
        let (_, required) =
            ranked_membership_change(&self.live.snapshot().entries, &incoming.entries, target);
        let missing_ready = required
            .iter()
            .filter(|wallet| !prepared.contains(wallet))
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
    fn fake_accept_validations(paper_state: &PaperStateDb, wallets: &[WalletAddress]) {
        // Mirror the real orchestrator's successful acceptance so the publication
        // recheck sees a current causal position validation; the bracket itself is
        // proven in scenario_position_bracket.rs.
        let validations: Vec<pe_paper_state::PositionValidationRecord> = wallets
            .iter()
            .map(|wallet| pe_paper_state::PositionValidationRecord {
                wallet: *wallet,
                ledger_hash: "test-ledger".to_owned(),
                positions_proof_hash: "test-proof".to_owned(),
                activity_bounds_json: "{}".to_owned(),
                source_log_generation: "test-gen".to_owned(),
                proof_json: "{}".to_owned(),
                recorded_at_unix: 0,
            })
            .collect();
        paper_state
            .record_position_validations(&validations)
            .unwrap();
    }

    use std::time::Duration;

    use super::*;
    use crate::config_poller::capacity_request_channel;
    use crate::orchestrator_control::OrchestratorControl;
    use axum::{Json, Router, routing::get};
    use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp};
    use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
    use tempfile::TempDir;
    use tokio::sync::Notify;
    use tokio::sync::mpsc;

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
    async fn wallet_evicted_during_preparation_is_not_readmitted_unprepared() {
        // Capacity plans against a live set {existing, departing} and prepares only the
        // newcomer. While that preparation is in flight, maintenance evicts `departing`. The
        // final readiness check must see `departing` as an unprepared admission and refuse to
        // publish; the worker's next attempt then plans it as an addition and prepares it.
        let existing = WalletAddress([1; 20]);
        let departing = WalletAddress([2; 20]);
        let newcomer = WalletAddress([3; 20]);
        let ranking: Vec<serde_json::Value> = [existing, departing, newcomer]
            .iter()
            .map(|w| {
                serde_json::json!({
                    "wallet_hex": w.to_string(),
                    "hit_rate": "0.60",
                    "ls_tstat": "2.0",
                    "n_trades": 10,
                    "last_trade_unix": 1_700_000_100_i64
                })
            })
            .collect();
        let app = Router::new()
            .route(
                "/rest/v1/latest_ranking",
                get(move || {
                    let ranking = ranking.clone();
                    async move { Json(ranking) }
                }),
            )
            .route(
                "/activity",
                get(|| async { Json(Vec::<serde_json::Value>::new()) }),
            )
            .route(
                "/positions",
                get(|| async { Json(Vec::<serde_json::Value>::new()) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let base_url = format!("http://{address}");

        let live = LiveWatchlist::new(watchlist(vec![entry(existing), entry(departing)]));
        let temp = TempDir::new().unwrap();
        let paper_state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
        for wallet in [departing, newcomer] {
            paper_state
                .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                    wallet,
                    complete: true,
                    proof_json: "{\"test\":true}".to_owned(),
                    updated_at_unix: 1,
                })
                .unwrap();
        }
        let writer_lock = Arc::new(Mutex::new(()));
        let applied = AppliedWatchlistCapacity::new(2);
        let (requests, desired_rx) = capacity_request_channel(2, Arc::clone(&writer_lock));
        let request = requests.request(3).await;

        // The control consumer records each prepared set and, on the FIRST attempt only,
        // evicts `departing` from the live set before acknowledging.
        let (control_tx, mut control_rx) = mpsc::channel(2);
        let prepared_sets = Arc::new(std::sync::Mutex::new(Vec::<Vec<WalletAddress>>::new()));
        let control = {
            let live = live.clone();
            let prepared_sets = Arc::clone(&prepared_sets);
            let fake_paper_state = Arc::clone(&paper_state);
            tokio::spawn(async move {
                let mut attempts = 0;
                while let Some(command) = control_rx.recv().await {
                    let OrchestratorControl::PrepareAdmissions {
                        wallets,
                        acknowledged,
                    } = command
                    else {
                        panic!("capacity transition sent an activity bucket")
                    };
                    let mut wallets = wallets;
                    wallets.sort_unstable_by_key(|w| w.0);
                    fake_accept_validations(&fake_paper_state, &wallets);
                    prepared_sets
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(wallets);
                    attempts += 1;
                    if attempts == 1 {
                        let removed: HashSet<WalletAddress> = [departing].into_iter().collect();
                        live.replace(&removed, &[], 2);
                    }
                    acknowledged.send(()).unwrap();
                }
            })
        };

        let preparer = AdmissionPreparer::new(control_tx, Arc::clone(&paper_state));
        let applier = SupabaseWatchlistCapacity::new(
            live.clone(),
            Arc::clone(&paper_state),
            Arc::clone(&writer_lock),
            applied.clone(),
            desired_rx,
            preparer,
            reqwest::Client::new(),
            base_url,
            "anon".to_string(),
            String::new(),
        );

        let first = applier.apply(request).await.unwrap_err();
        assert!(
            first.contains("not admission-ready"),
            "expected an unprepared-admission refusal, got: {first}"
        );
        assert_eq!(
            live.snapshot().entries.len(),
            1,
            "membership must be unchanged"
        );
        assert_eq!(applied.load().target, 2, "applied target must be unchanged");

        // The retry plans against the current set, so `departing` is now a prepared addition.
        assert_eq!(applier.apply(request).await.unwrap(), 3);
        assert_eq!(live.snapshot().entries.len(), 3);
        assert_eq!(applied.load(), request);
        assert_eq!(
            *prepared_sets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![vec![newcomer], vec![departing, newcomer]]
        );
        control.abort();
        server.abort();
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
        paper_state
            .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                wallet: newcomer,
                complete: true,
                proof_json: "{\"test\":true}".to_owned(),
                updated_at_unix: 1,
            })
            .unwrap();
        let writer_lock = Arc::new(Mutex::new(()));
        let applied = AppliedWatchlistCapacity::new(1);
        let (requests, desired_rx) = capacity_request_channel(1, Arc::clone(&writer_lock));
        let request = requests.request(2).await;

        let (control_tx, mut control_rx) = mpsc::channel(1);
        let prepared = Arc::new(Notify::new());
        let release_ack = Arc::new(Notify::new());
        let live_at_control = live.clone();
        let fake_paper_state = Arc::clone(&paper_state);
        let prepared_task = Arc::clone(&prepared);
        let release_task = Arc::clone(&release_ack);
        let control = tokio::spawn(async move {
            let command = control_rx.recv().await.unwrap();
            match command {
                OrchestratorControl::PrepareAdmissions {
                    wallets,
                    acknowledged,
                } => {
                    assert_eq!(live_at_control.snapshot().entries.len(), 1);
                    assert_eq!(wallets, vec![newcomer]);
                    fake_accept_validations(&fake_paper_state, &wallets);
                    prepared_task.notify_one();
                    release_task.notified().await;
                    acknowledged.send(()).unwrap();
                }
                OrchestratorControl::CommitActivityBucket { .. } => {
                    panic!("capacity transition sent an activity bucket")
                }
                OrchestratorControl::PrepareValidatedAdmissions { .. }
                | OrchestratorControl::CaptureAdmissionLedger { .. } => {
                    panic!("legacy admission test sent a causal-bracket command")
                }
            }
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let preparer = AdmissionPreparer::new(control_tx, Arc::clone(&paper_state));
        let applier = SupabaseWatchlistCapacity::new(
            live.clone(),
            Arc::clone(&paper_state),
            Arc::clone(&writer_lock),
            applied.clone(),
            desired_rx,
            preparer,
            client,
            base_url,
            "anon".to_string(),
            String::new(),
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
