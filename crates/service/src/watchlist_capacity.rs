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
use tracing::info;

use crate::config_poller::{CapacityRequest, WatchlistCapacityApplier};
use crate::live_watchlist::LiveWatchlist;
use crate::paper_recovery::{MembershipReason, SealedMembershipEvidence};
use crate::runtime_config::{AppliedWatchlistCapacity, MAX_ACTIVE_WATCHLIST_SIZE};
use crate::supabase_reader::{self, SupabaseError};
use crate::watchlist_admission::{AdmissionError, AdmissionPreparer};
use crate::watchlist_maintenance::{
    MembershipApplyError, MembershipCapacityCheck, MembershipPublication,
    apply_ranked_membership_locked, planned_live_reentries, ranked_membership_change_set,
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
    #[error("encode capacity membership evidence: {0}")]
    Evidence(#[from] serde_json::Error),
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
            MAX_ACTIVE_WATCHLIST_SIZE,
        )
        .await?;
        let fenced = self
            .paper_state
            .wallet_fences()
            .map_err(MembershipApplyError::from)?
            .into_iter()
            .map(|record| record.wallet)
            .collect();
        let (incoming, incoming_last_trade) =
            supabase_reader::select_membership(incoming, incoming_last_trade, &fenced, target);
        if incoming.entries.is_empty() {
            return Err(CapacityError::EmptyRanking { target });
        }
        validate_unique_ranking(&incoming.entries)?;

        let (_, additions) = ranked_membership_change_set(
            &self.live.structural_membership(),
            &incoming.entries,
            target,
        );
        self.preparer.prepare(&additions).await?;
        let reentry_candidates = planned_live_reentries(&self.live, &incoming.entries, target);
        let reentries = self
            .preparer
            .prepare_live_reentries(&reentry_candidates)
            .await;
        let admission_receipts = self.preparer.record_admission_proofs(&additions).await?;
        let prepared: HashSet<WalletAddress> = additions.iter().copied().collect();

        let _writer = self.writer_lock.lock().await;
        if *self.desired_capacity.borrow() != request {
            return Err(CapacityError::Superseded);
        }
        // Readiness is proven only by THIS attempt (#542). The admissions the locked apply will
        // publish are recomputed against current membership: a wallet that was live when the
        // additions were planned but has since been evicted by maintenance is a genuine new
        // admission that was never prepared; the worker retries and prepares it next round.
        let (_, required) = ranked_membership_change_set(
            &self.live.structural_membership(),
            &incoming.entries,
            target,
        );
        let missing_ready = required
            .iter()
            .filter(|wallet| !prepared.contains(wallet))
            .count();
        if missing_ready > 0 {
            return Err(CapacityError::UnpreparedAdmission {
                missing: missing_ready,
            });
        }
        let config_receipt = self
            .preparer
            .record_capacity_config(request.generation, request.target, incoming.entries.clone())
            .await?;
        let evidence = SealedMembershipEvidence::capacity_change(
            request.generation,
            config_receipt,
            admission_receipts,
        )?;
        let (actual, dropped) = apply_ranked_membership_locked(
            &self.live,
            &self.paper_state,
            &self.preparer,
            MembershipPublication {
                reason: MembershipReason::CapacityChange,
                ranking_batch_id: None,
                evidence,
            },
            &incoming.entries,
            &incoming_last_trade,
            target,
            _writer,
            Some(MembershipCapacityCheck::Transition {
                applied: self.applied_capacity.clone(),
                desired: self.desired_capacity.clone(),
                request,
            }),
            &reentries,
        )
        .await?;

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
    fn fake_install_anchors(paper_state: &PaperStateDb, wallets: &[WalletAddress], cursor: i64) {
        // Mirror the real orchestrator's successful acceptance so the publication
        // recheck sees a current causal position validation; the bracket itself is
        // proven in scenario_position_bracket.rs.
        paper_state
            .seed_cursors_if_absent(
                &wallets
                    .iter()
                    .copied()
                    .map(|wallet| (wallet, cursor))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        let installs: Vec<pe_paper_state::AnchorInstallRecord> = wallets
            .iter()
            .map(|wallet| pe_paper_state::AnchorInstallRecord {
                history_status: None,
                wallet: *wallet,
                balances: Vec::new(),
                activity_cutoff_unix: cursor,
                anchored_at_unix: cursor,
                ledger_hash_after: "test-ledger".to_owned(),
                positions_proof_hash: "test-proof".to_owned(),
                activity_bounds_json: "{}".to_owned(),
                source_log_generation: "test-gen".to_owned(),
                proof_json: "{}".to_owned(),
                recorded_at_unix: cursor,
            })
            .collect();
        paper_state.install_anchors(&installs).unwrap();
    }

    use std::time::Duration;

    use super::*;
    use crate::activity_ingest::{ActivityIngest, SourceLogHandle};
    use crate::config_poller::capacity_request_channel;
    use crate::health::new_shared_health_with_ws;
    use crate::orchestrator_control::OrchestratorControl;
    use crate::source_event_sink::SourceEventSink;
    use axum::{Json, Router, routing::get};
    use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp};
    use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
    use tempfile::TempDir;
    use tokio::sync::Notify;
    use tokio::sync::mpsc;

    struct TestSourceLog {
        handle: SourceLogHandle,
        path: std::path::PathBuf,
        task: tokio::task::JoinHandle<()>,
        _triggers: mpsc::Receiver<crate::activity_ingest::ReconciliationTrigger>,
    }

    fn test_source_log(temp: &TempDir) -> TestSourceLog {
        let path = temp.path().join("source.log");
        let sink = SourceEventSink::open(&path).unwrap();
        let (handle, source_rx) = SourceLogHandle::channel(4);
        let (trigger_tx, triggers) = mpsc::channel(1);
        let ingest = ActivityIngest::poll_only(
            sink,
            source_rx,
            trigger_tx,
            new_shared_health_with_ws(false, false, 1),
        );
        let task = tokio::spawn(ingest.run());
        TestSourceLog {
            handle,
            path,
            task,
            _triggers: triggers,
        }
    }

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
    async fn capacity_publication_round_trips_membership_verifier() {
        let temp = TempDir::new().unwrap();
        let source_log = test_source_log(&temp);
        let paper_state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let verifier_source_log = source_log.path.clone();
        let control = tokio::spawn(async move {
            let command = control_rx.recv().await.unwrap();
            let OrchestratorControl::PublishMembership {
                change,
                acknowledged,
                ..
            } = command
            else {
                panic!("capacity publisher sent a non-publication command");
            };
            // This round trip publishes a full replacement: the membership before the change is
            // exactly the removed set.
            let current_membership: HashSet<WalletAddress> =
                change.removed.iter().copied().collect();
            let result = crate::qualification::verify_published_membership_change(
                &change.into_record(),
                &verifier_source_log,
                &current_membership,
            )
            .map(|()| pe_event_log::AppendReceipt {
                sequence: pe_core_types::EventSeq(1),
                this_hash: blake3::hash(b"capacity-round-trip"),
            })
            .map_err(|error| error.to_string());
            acknowledged.send(result).unwrap();
        });
        let preparer = AdmissionPreparer::new(control_tx, paper_state)
            .with_source_log(source_log.handle.clone());
        let generation = 1;
        let target = 2;
        let config_receipt = preparer
            .record_capacity_config(generation, target, Vec::new())
            .await
            .unwrap();
        let evidence =
            SealedMembershipEvidence::capacity_change(generation, config_receipt, Vec::new())
                .unwrap();
        preparer
            .publish_membership(
                crate::paper_recovery::MembershipChange {
                    reason: MembershipReason::CapacityChange,
                    removed: Vec::new(),
                    added: Vec::new(),
                    capacity: target,
                    ranking_batch_id: None,
                    evidence,
                },
                Vec::new(),
                Default::default(),
            )
            .await
            .unwrap();
        control.await.unwrap();

        let source_records = pe_event_log::Reader::replay(&source_log.path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(source_records.len(), 1);
        assert_eq!(
            source_records[0].1.source_id.0,
            "pe-service.watchlist-capacity-config"
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&source_records[0].1.payload).unwrap(),
            serde_json::json!({
                "generation": generation,
                "target": target,
                "published_entries": []
            })
        );
        source_log.task.abort();
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
        let source_log = test_source_log(&temp);
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
            let verifier_source_log = source_log.path.clone();
            tokio::spawn(async move {
                let mut attempts = 0;
                while let Some(command) = control_rx.recv().await {
                    match command {
                        OrchestratorControl::PrepareAdmissions {
                            wallets,
                            acknowledged,
                        } => {
                            let mut wallets = wallets;
                            wallets.sort_unstable_by_key(|w| w.0);
                            fake_install_anchors(&fake_paper_state, &wallets, 1_700_000_100);
                            prepared_sets
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push(wallets);
                            attempts += 1;
                            if attempts == 1 {
                                // Model a durable structural eviction before this capacity
                                // attempt's locked recheck, not a live-only boot deferral.
                                let removed: HashSet<WalletAddress> =
                                    [departing].into_iter().collect();
                                live.replace(&removed, &[], 2);
                                live.commit_structural_change(&[departing], &[]);
                            }
                            acknowledged.send(()).unwrap();
                        }
                        OrchestratorControl::PublishMembership {
                            change,
                            replacements,
                            checks,
                            acknowledged,
                        } => {
                            if let Err(error) = checks.recheck_and_seed(
                                &fake_paper_state,
                                &live,
                                &change,
                                &replacements,
                            ) {
                                acknowledged.send(Err(error.to_string())).unwrap();
                                continue;
                            }

                            if let Err(error) =
                                crate::qualification::verify_published_membership_change(
                                    &change.clone().into_record(),
                                    &verifier_source_log,
                                    &live.structural_membership(),
                                )
                            {
                                acknowledged.send(Err(error.to_string())).unwrap();
                                continue;
                            }
                            let removed = change.removed.iter().copied().collect::<HashSet<_>>();
                            let additions = replacements
                                .iter()
                                .filter(|entry| change.added.contains(&entry.wallet))
                                .cloned()
                                .collect::<Vec<_>>();
                            live.commit_structural_change(&change.removed, &change.added);
                            live.replace(&removed, &additions, change.capacity);
                            crate::watchlist_maintenance::apply_live_reentries(
                                &live,
                                &fake_paper_state,
                                &checks.reentries,
                                &replacements,
                                change.capacity,
                            );
                            checks.commit_capacity();
                            acknowledged
                                .send(Ok(pe_event_log::AppendReceipt {
                                    sequence: pe_core_types::EventSeq(1),
                                    this_hash: blake3::hash(b"test-capacity"),
                                }))
                                .unwrap();
                        }
                        _ => panic!("capacity transition sent an unrelated control"),
                    }
                }
            })
        };

        let preparer = AdmissionPreparer::new(control_tx, Arc::clone(&paper_state))
            .with_source_log(source_log.handle.clone());
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
        assert_eq!(live.structural_membership().len(), 3);
        assert_eq!(applied.load(), request);
        assert_eq!(
            *prepared_sets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![vec![newcomer], vec![departing, newcomer]]
        );
        control.abort();
        source_log.task.abort();
        server.abort();
    }

    #[tokio::test]
    async fn production_transition_prepares_and_acks_before_membership_publication() {
        let existing = WalletAddress([1; 20]);
        let newcomer = WalletAddress([2; 20]);
        let fenced = WalletAddress([3; 20]);
        let newcomer_last_trade = 1_700_000_123_i64;
        let ranking = Arc::new(std::sync::Mutex::new(vec![
            serde_json::json!({
                "wallet_hex": fenced.to_string(), "hit_rate": "0.70", "ls_tstat": "3.0", "n_trades": 10,
            }),
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
        ]));
        let fetches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let app = Router::new()
            .route(
                "/rest/v1/latest_ranking",
                get({
                    let ranking = Arc::clone(&ranking);
                    let fetches = fetches.clone();
                    move |axum::extract::Query(query): axum::extract::Query<
                        std::collections::HashMap<String, String>,
                    >| {
                        let ranking = Arc::clone(&ranking);
                        let fetches = fetches.clone();
                        async move {
                            assert_eq!(query["limit"], MAX_ACTIVE_WATCHLIST_SIZE.to_string());
                            assert_eq!(query["survives"], "is.true");
                            assert!(!query.contains_key("batch_id"));
                            fetches.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            let limit = query["limit"].parse::<usize>().unwrap();
                            Json(
                                ranking
                                    .lock()
                                    .unwrap()
                                    .iter()
                                    .take(limit)
                                    .cloned()
                                    .collect::<Vec<_>>(),
                            )
                        }
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
        let source_log = test_source_log(&temp);
        let paper_state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
        paper_state
            .record_reconciled_history_status(&pe_paper_state::WalletHistoryStatusRecord {
                wallet: newcomer,
                complete: true,
                proof_json: "{\"test\":true}".to_owned(),
                updated_at_unix: 1,
            })
            .unwrap();
        let conn = rusqlite::Connection::open(temp.path().join("paper.db")).unwrap();
        conn.execute("INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'test', 'invalid_mapping', '{}', 1)", [fenced.to_string()]).unwrap();
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
        let verifier_source_log = source_log.path.clone();
        let control = tokio::spawn(async move {
            while let Some(command) = control_rx.recv().await {
                match command {
                    OrchestratorControl::PrepareAdmissions {
                        wallets,
                        acknowledged,
                    } => {
                        assert_eq!(live_at_control.snapshot().entries.len(), 1);
                        assert_eq!(wallets, vec![newcomer]);
                        fake_install_anchors(&fake_paper_state, &wallets, newcomer_last_trade);
                        prepared_task.notify_one();
                        release_task.notified().await;
                        acknowledged.send(()).unwrap();
                    }
                    OrchestratorControl::CommitActivityBucket { .. } => {
                        panic!("capacity transition sent an activity bucket")
                    }
                    OrchestratorControl::InstallAnchors { .. }
                    | OrchestratorControl::CaptureAdmissionLedger { .. } => {
                        panic!("legacy admission test sent a causal-bracket command")
                    }
                    OrchestratorControl::PublishMembership {
                        change,
                        replacements,
                        checks,
                        acknowledged,
                    } => {
                        if let Err(error) = checks.recheck_and_seed(
                            &fake_paper_state,
                            &live_at_control,
                            &change,
                            &replacements,
                        ) {
                            acknowledged.send(Err(error.to_string())).unwrap();
                            continue;
                        }

                        if let Err(error) = crate::qualification::verify_published_membership_change(
                            &change.clone().into_record(),
                            &verifier_source_log,
                            &live_at_control.structural_membership(),
                        ) {
                            acknowledged.send(Err(error.to_string())).unwrap();
                            continue;
                        }
                        let removed = change.removed.iter().copied().collect::<HashSet<_>>();
                        let additions = replacements
                            .iter()
                            .filter(|entry| change.added.contains(&entry.wallet))
                            .cloned()
                            .collect::<Vec<_>>();
                        live_at_control.commit_structural_change(&change.removed, &change.added);
                        live_at_control.replace(&removed, &additions, change.capacity);
                        crate::watchlist_maintenance::apply_live_reentries(
                            &live_at_control,
                            &fake_paper_state,
                            &checks.reentries,
                            &replacements,
                            change.capacity,
                        );
                        checks.commit_capacity();
                        acknowledged
                            .send(Ok(pe_event_log::AppendReceipt {
                                sequence: pe_core_types::EventSeq(1),
                                this_hash: blake3::hash(b"test-capacity"),
                            }))
                            .unwrap();
                    }
                    OrchestratorControl::ResolutionCandidate { .. }
                    | OrchestratorControl::RiskHaltChange { .. }
                    | OrchestratorControl::DailyBoundary { .. }
                    | OrchestratorControl::SealCheck { .. } => {
                        panic!("capacity transition sent an unrelated financial control")
                    }
                }
            }
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let preparer = AdmissionPreparer::new(control_tx, Arc::clone(&paper_state))
            .with_source_log(source_log.handle.clone());
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
        assert_eq!(
            paper_state.cursor(&newcomer).unwrap(),
            Some(newcomer_last_trade)
        );

        // A newer moving batch arrives while this selected vector waits for installation.
        ranking.lock().unwrap().clear();
        release_ack.notify_one();
        assert_eq!(apply.await.unwrap().unwrap(), 2);
        control.await.unwrap();
        assert_eq!(live.snapshot().entries.len(), 2);
        assert_eq!(live.structural_membership().len(), 2);
        assert_eq!(applied.load(), request);
        assert_eq!(fetches.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(
            !live
                .snapshot()
                .entries
                .iter()
                .any(|entry| entry.wallet == fenced)
        );
        assert_eq!(
            paper_state.cursor(&newcomer).unwrap(),
            Some(newcomer_last_trade)
        );
        let source_records = pe_event_log::Reader::replay(&source_log.path)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        // The newcomer's admission proof is retained before the capacity-change publication,
        // then the configuration receipt that the membership evidence references.
        let source_ids: Vec<&str> = source_records
            .iter()
            .map(|(_, envelope)| envelope.source_id.0.as_str())
            .collect();
        assert_eq!(
            source_ids,
            vec![
                "pe-service.watchlist-admission",
                "pe-service.watchlist-capacity-config"
            ]
        );
        // The configuration receipt binds the generation, the target, and the exact published set.
        let config_payload =
            serde_json::from_slice::<serde_json::Value>(&source_records[1].1.payload).unwrap();
        assert_eq!(
            config_payload["generation"],
            serde_json::json!(request.generation)
        );
        assert_eq!(config_payload["target"], serde_json::json!(request.target));
        let published_wallets: Vec<String> = config_payload["published_entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["wallet"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            published_wallets,
            vec![format!("{existing:?}"), format!("{newcomer:?}")]
                .into_iter()
                .map(|wallet| wallet
                    .trim_start_matches("WalletAddress(")
                    .trim_end_matches(')')
                    .to_owned())
                .collect::<Vec<_>>()
        );
        source_log.task.abort();
        server.abort();
    }
    #[tokio::test]
    async fn empty_selection_preserves_each_consumer_failure_contract() {
        let (existing, fenced) = (WalletAddress([41; 20]), WalletAddress([42; 20]));
        let app = Router::new().route("/rest/v1/latest_ranking", get(move || async move {
            Json(vec![serde_json::json!({
                "wallet_hex": fenced.to_string(), "hit_rate": "0.6", "ls_tstat": "2", "n_trades": 10,
            })])
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let temp = TempDir::new().unwrap();
        let paper_state = Arc::new(PaperStateDb::open(&temp.path().join("paper.db")).unwrap());
        let conn = rusqlite::Connection::open(temp.path().join("paper.db")).unwrap();
        conn.execute("INSERT INTO wallet_fences (wallet_hex, source_trade_id, cause, proof_json, fenced_at_unix) VALUES (?1, 'test', 'invalid_mapping', '{}', 1)", [fenced.to_string()]).unwrap();
        let live = LiveWatchlist::new(watchlist(vec![entry(existing)]));
        let writer_lock = Arc::new(Mutex::new(()));
        let applied = AppliedWatchlistCapacity::new(1);
        let before = applied.load();
        let (requests, desired) = capacity_request_channel(1, writer_lock.clone());
        let request = requests.request(2).await;
        let (tx, mut rx) = mpsc::channel(1);
        let applier = SupabaseWatchlistCapacity::new(
            live.clone(),
            paper_state.clone(),
            writer_lock,
            applied.clone(),
            desired,
            AdmissionPreparer::new(tx, paper_state.clone()),
            reqwest::Client::new(),
            format!("http://{address}"),
            "anon".to_owned(),
            String::new(),
        );
        assert!(matches!(
            applier.apply_inner(request).await,
            Err(CapacityError::EmptyRanking { target: 2 })
        ));
        assert_eq!(applied.load(), before);
        assert_eq!(
            live.snapshot()
                .entries
                .iter()
                .map(|entry| entry.wallet)
                .collect::<Vec<_>>(),
            vec![existing]
        );
        assert!(paper_state.cursor(&fenced).unwrap().is_none());
        assert!(
            paper_state
                .wallet_history_status(&fenced)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        server.abort();
    }
}
