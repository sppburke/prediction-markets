#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pe_copy_signal_engine::SignalConfig;
use pe_core_types::{BasisPoints, ReconstructionQuality, SourceTimestamp, WalletAddress};
use pe_paper_state::{AnchorInstallRecord, PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
use pe_service::asset_identity::AssetIdentityResolver;
use pe_service::bucket_commit::{AnchorInstallError, BucketCommitEngine, FrozenDecisionBasis};
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::position_seeder::{CausalPositionValidator, ledger_capture};
use pe_service::runtime_config::{LiveRuntimeConfig, RuntimeConfig};
use pe_service::source_event_sink::SourceEventSink;
use pe_service::trade_poller::{
    ReconciliationObligations, TradePoller, TradePollerConfig, TradePollerOwnerError,
};
use pe_service::watchlist_admission::{AdmissionPreparer, AnchorRefreshOutcome};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    GAMMA_BATCH_SIZE, PageFetcher, PolymarketEndpoint, PositionPartition, ReconciliationFetcher,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use serde_json::json;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

const BASE: &str = "https://api.example.com";
const NOW: i64 = 100;

struct PollFetcher {
    pages: Mutex<VecDeque<Vec<u8>>>,
    calls: AtomicUsize,
}

impl PollFetcher {
    fn empty_pages(count: usize) -> Self {
        Self {
            pages: Mutex::new((0..count).map(|_| b"[]".to_vec()).collect()),
            calls: AtomicUsize::new(0),
        }
    }
}

impl ReconciliationFetcher for PollFetcher {
    fn fetch<'a>(
        &'a self,
        _url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.pages
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
                .ok_or_else(|| SourceError::Fatal {
                    message: "unexpected poll page fetch".to_owned(),
                })
        })
    }
}

struct MapFetcher {
    responses: Mutex<HashMap<String, VecDeque<Vec<u8>>>>,
}

impl MapFetcher {
    fn new(responses: HashMap<String, Vec<Vec<u8>>>) -> Self {
        Self {
            responses: Mutex::new(
                responses
                    .into_iter()
                    .map(|(url, pages)| (url, pages.into()))
                    .collect(),
            ),
        }
    }
}

impl PageFetcher for MapFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        if url.contains("/markets?clob_token_ids=") {
            let markets = url
                .split('?')
                .nth(1)
                .into_iter()
                .flat_map(|query| query.split('&'))
                .filter_map(|part| part.strip_prefix("clob_token_ids="))
                .filter_map(|token| {
                    token.strip_prefix("asset-").map(|index| {
                        json!({
                            "conditionId": format!("condition-{index}"),
                            "clobTokenIds": [token]
                        })
                    })
                })
                .collect::<Vec<_>>();
            return Ok(serde_json::to_vec(&markets).unwrap());
        }
        self.responses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(url)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| SourceError::Fatal {
                message: format!("no queued bracket fixture for {url}"),
            })
    }
}

fn wallet(byte: u8) -> WalletAddress {
    WalletAddress([byte; 20])
}

fn activity_url(wallet: WalletAddress) -> String {
    PolymarketEndpoint::UserPositionActivityPage {
        user: wallet.to_string(),
        end: NOW,
        start: None,
        offset: 0,
    }
    .url(BASE)
}

fn position_url(wallet: WalletAddress, partition: PositionPartition) -> String {
    PolymarketEndpoint::CurrentPositionsReconciliationPage {
        user: wallet.to_string(),
        partition,
        offset: 0,
    }
    .url(BASE)
}

fn stable_bracket_responses(wallets: &[WalletAddress]) -> HashMap<String, Vec<Vec<u8>>> {
    let mut responses = HashMap::new();
    for (index, wallet) in wallets.iter().enumerate() {
        let activity = serde_json::to_vec(&vec![json!({
            "proxyWallet": wallet,
            "timestamp": 10,
            "conditionId": format!("condition-{index}"),
            "type": "TRADE",
            "size": "1",
            "usdcSize": "0.5",
            "transactionHash": format!("0x{index}"),
            "price": "0.5",
            "asset": format!("asset-{index}"),
            "side": "BUY",
            "outcomeIndex": 0,
            "outcome": "Yes",
            "isCombo": false
        })])
        .unwrap();
        let positions = serde_json::to_vec(&vec![json!({
            "proxyWallet": wallet,
            "asset": format!("asset-{index}"),
            "conditionId": format!("condition-{index}"),
            "size": "1",
            "outcomeIndex": 0
        })])
        .unwrap();
        responses.insert(
            activity_url(*wallet),
            vec![activity.clone(), activity.clone(), activity],
        );
        responses.insert(
            position_url(*wallet, PositionPartition::NotRedeemable),
            vec![positions.clone(), positions],
        );
        responses.insert(
            position_url(*wallet, PositionPartition::Redeemable),
            vec![b"[]".to_vec(), b"[]".to_vec()],
        );
    }
    responses
}

fn live_watchlist(wallets: &[WalletAddress]) -> LiveWatchlist {
    let entries = wallets
        .iter()
        .copied()
        .map(|wallet| WatchlistEntry {
            wallet,
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(100),
            lcb_5pct_bps: BasisPoints(100),
            win_rate_bps: BasisPoints(7_000),
            closed_trades_in_window: 1,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        })
        .collect::<Vec<_>>();
    LiveWatchlist::new(Watchlist {
        active_count: entries.len(),
        incubator_count: 0,
        entries,
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
    })
}

fn paper(wallets: &[WalletAddress]) -> (tempfile::TempDir, Arc<PaperStateDb>) {
    let dir = tempfile::tempdir().unwrap();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    for wallet in wallets {
        paper
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: *wallet,
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: 1,
            })
            .unwrap();
    }
    (dir, paper)
}

fn validator(responses: HashMap<String, Vec<Vec<u8>>>) -> CausalPositionValidator {
    let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(MapFetcher::new(responses));
    let dir = tempfile::tempdir().unwrap();
    let source_log = Arc::new(tokio::sync::Mutex::new(
        SourceEventSink::open(dir.path().join("source.log")).unwrap(),
    ));
    let resolver = Arc::new(AssetIdentityResolver::new(
        Arc::clone(&fetcher),
        BASE.to_owned(),
        GAMMA_BATCH_SIZE,
        source_log,
    ));
    CausalPositionValidator::new(fetcher, BASE, "scenario", resolver).with_clock(Arc::new(|| NOW))
}

struct PollerHarness {
    poller: TradePoller,
    ingest: tokio::task::JoinHandle<()>,
    actor: tokio::task::JoinHandle<()>,
    preparer: Arc<AdmissionPreparer>,
}

#[allow(clippy::too_many_arguments)]
fn poller_harness(
    dir: &tempfile::TempDir,
    paper: Arc<PaperStateDb>,
    wallets: &[WalletAddress],
    bracket_responses: HashMap<String, Vec<Vec<u8>>>,
    rounds: usize,
    fail_install_transaction: bool,
    installed: Arc<Mutex<Vec<(WalletAddress, usize)>>>,
    shutdown: Option<oneshot::Sender<()>>,
) -> PollerHarness {
    if fail_install_transaction {
        rusqlite::Connection::open(dir.path().join("paper.db"))
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER fail_anchor_validation
                 BEFORE INSERT ON position_validations
                 BEGIN SELECT RAISE(FAIL, 'injected anchor transaction failure'); END;",
            )
            .unwrap();
    }
    let source_log_path = dir.path().join("source.log");
    let sink = SourceEventSink::open(&source_log_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(8);
    let asset_identity = Arc::new(AssetIdentityResolver::new_runtime(
        Arc::new(MapFetcher::new(HashMap::new())),
        BASE.to_owned(),
        GAMMA_BATCH_SIZE,
        source_log.clone(),
    ));
    let (trigger_tx, trigger_rx) = mpsc::channel(8);
    let health = new_shared_health_with_ws(false, true, 90);
    let ingest =
        tokio::spawn(ActivityIngest::poll_only(sink, source_rx, trigger_tx, health.clone()).run());
    let (control_tx, mut control_rx) = mpsc::channel(4);
    let actor_paper = Arc::clone(&paper);
    let poll_fetcher = Arc::new(PollFetcher::empty_pages(wallets.len() * rounds));
    let actor_poll_fetcher = Arc::clone(&poll_fetcher);
    let actor_installed = Arc::clone(&installed);
    let actor = tokio::spawn(async move {
        let mut engine =
            BucketCommitEngine::load(Arc::clone(&actor_paper), PositionLedger::new()).unwrap();
        let mut shutdown = shutdown;
        while let Some(command) = control_rx.recv().await {
            match command {
                OrchestratorControl::PrepareAdmissions { acknowledged, .. } => {
                    let _ = acknowledged.send(());
                }
                OrchestratorControl::CommitActivityBucket {
                    aggregates,
                    context,
                    committed,
                } => {
                    let _ = committed.send(
                        engine
                            .commit(
                                aggregates,
                                &context,
                                FrozenDecisionBasis {
                                    win_rate_p: pe_core_types::Probability::ZERO,
                                    bankroll: rust_decimal::Decimal::ZERO,
                                },
                            )
                            .map_err(|error| error.to_string()),
                    );
                }
                OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                    let _ = captured.send(
                        ledger_capture(engine.ledger(), &actor_paper, wallet)
                            .map_err(|error| error.to_string()),
                    );
                }
                OrchestratorControl::InstallAnchors {
                    installs,
                    acknowledged,
                } => {
                    let wallets = installs
                        .iter()
                        .map(|install| install.wallet)
                        .collect::<Vec<_>>();
                    let result = engine.install_anchors(&installs);
                    if result.is_ok() {
                        let calls = actor_poll_fetcher.calls.load(Ordering::SeqCst);
                        actor_installed
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .extend(wallets.into_iter().map(|wallet| (wallet, calls)));
                        if actor_installed
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .len()
                            == rounds
                            && let Some(sender) = shutdown.take()
                        {
                            let _ = sender.send(());
                        }
                    }
                    let _ = acknowledged.send(result);
                }
            }
        }
    });
    let preparer = Arc::new(AdmissionPreparer::with_validator(
        control_tx.clone(),
        Arc::clone(&paper),
        validator(bracket_responses),
    ));
    let poller = TradePoller::new(
        TradePollerConfig {
            base_url: BASE.to_owned(),
            poll_interval_secs: 1,
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
        },
        live_watchlist(wallets),
        poll_fetcher,
        asset_identity,
        source_log,
        trigger_rx,
        control_tx,
        paper,
        health,
        SignalConfig::default(),
        LiveRuntimeConfig::new(RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        )),
        ReconciliationObligations::default(),
        Some(preparer.as_ref().clone()),
    )
    .with_clock(Arc::new(|| {
        OffsetDateTime::from_unix_timestamp(NOW).unwrap()
    }));
    PollerHarness {
        poller,
        ingest,
        actor,
        preparer,
    }
}

async fn finish_harness(
    ingest: tokio::task::JoinHandle<()>,
    actor: tokio::task::JoinHandle<()>,
    preparer: Arc<AdmissionPreparer>,
) {
    drop(preparer);
    ingest.await.unwrap();
    actor.await.unwrap();
}

async fn refresh_outcome_for_install_rejection(
    rejection: AnchorInstallError,
) -> AnchorRefreshOutcome {
    let wallet = wallet(0x5a);
    let (_dir, paper) = paper(&[wallet]);
    let (control_tx, mut control_rx) = mpsc::channel(4);
    let actor_paper = Arc::clone(&paper);
    let actor = tokio::spawn(async move {
        let mut engine =
            BucketCommitEngine::load(Arc::clone(&actor_paper), PositionLedger::new()).unwrap();
        let mut rejection = Some(rejection);
        while let Some(command) = control_rx.recv().await {
            match command {
                OrchestratorControl::PrepareAdmissions { acknowledged, .. } => {
                    let _ = acknowledged.send(());
                }
                OrchestratorControl::CommitActivityBucket {
                    aggregates,
                    context,
                    committed,
                } => {
                    let _ = committed.send(
                        engine
                            .commit(
                                aggregates,
                                &context,
                                FrozenDecisionBasis {
                                    win_rate_p: pe_core_types::Probability::ZERO,
                                    bankroll: rust_decimal::Decimal::ZERO,
                                },
                            )
                            .map_err(|error| error.to_string()),
                    );
                }
                OrchestratorControl::CaptureAdmissionLedger { wallet, captured } => {
                    let _ = captured.send(
                        ledger_capture(engine.ledger(), &actor_paper, wallet)
                            .map_err(|error| error.to_string()),
                    );
                }
                OrchestratorControl::InstallAnchors { acknowledged, .. } => {
                    let _ = acknowledged.send(Err(rejection.take().unwrap()));
                }
            }
        }
    });
    let preparer = AdmissionPreparer::with_validator(
        control_tx,
        paper,
        validator(stable_bracket_responses(&[wallet])),
    );
    let outcome = preparer.prepare_if_due(wallet, NOW, 3_600).await.unwrap();
    drop(preparer);
    actor.await.unwrap();
    outcome
}

#[tokio::test]
async fn refreshes_at_most_one_due_wallet_per_round_in_round_robin_order() {
    let wallets = [wallet(0x11), wallet(0x22)];
    let (dir, paper) = paper(&wallets);
    let installed = Arc::new(Mutex::new(Vec::new()));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let harness = poller_harness(
        &dir,
        Arc::clone(&paper),
        &wallets,
        stable_bracket_responses(&wallets),
        2,
        false,
        Arc::clone(&installed),
        Some(shutdown_tx),
    );
    let PollerHarness {
        poller,
        ingest,
        actor,
        preparer,
    } = harness;
    let result = poller
        .run_until(async {
            let _ = shutdown_rx.await;
        })
        .await;
    assert!(result.is_ok());
    assert_eq!(
        *installed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec![(wallets[0], 2), (wallets[1], 4)]
    );
    assert_eq!(paper.position_anchors(&wallets[0]).unwrap().len(), 1);
    assert_eq!(paper.position_anchors(&wallets[1]).unwrap().len(), 1);
    finish_harness(ingest, actor, preparer).await;
}

#[tokio::test]
async fn mutex_recheck_skips_a_fresh_anchor_without_a_validator() {
    let wallet = wallet(0x33);
    let (_dir, paper) = paper(&[wallet]);
    paper.set_cursor(&wallet, 10).unwrap();
    paper
        .install_anchors(&[AnchorInstallRecord {
            wallet,
            balances: Vec::new(),
            activity_cutoff_unix: 10,
            anchored_at_unix: NOW,
            ledger_hash_after: "unused".to_owned(),
            positions_proof_hash: "positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: NOW,
        }])
        .unwrap();
    let (control_tx, _control_rx) = mpsc::channel(1);
    let preparer = AdmissionPreparer::new(control_tx, paper);
    assert_eq!(
        preparer.prepare_if_due(wallet, NOW, 3_600).await.unwrap(),
        AnchorRefreshOutcome::Skipped
    );
}

#[tokio::test]
async fn contended_mutex_rereads_fresh_anchor_before_refreshing() {
    let blocker = wallet(0x34);
    let target = wallet(0x35);
    let (_dir, paper) = paper(&[blocker, target]);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let (received_tx, received_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let actor = tokio::spawn(async move {
        let Some(OrchestratorControl::PrepareAdmissions { acknowledged, .. }) =
            control_rx.recv().await
        else {
            return;
        };
        let _ = received_tx.send(());
        let _ = release_rx.await;
        let _ = acknowledged.send(());
    });
    let preparer = Arc::new(AdmissionPreparer::new(control_tx, Arc::clone(&paper)));
    let first = {
        let preparer = Arc::clone(&preparer);
        tokio::spawn(async move { preparer.prepare(&[blocker]).await })
    };
    received_rx.await.unwrap();
    let mut second = {
        let preparer = Arc::clone(&preparer);
        tokio::spawn(async move { preparer.prepare_if_due(target, NOW, 3_600).await })
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(10), &mut second)
            .await
            .is_err()
    );

    paper.set_cursor(&target, 10).unwrap();
    paper
        .install_anchors(&[AnchorInstallRecord {
            wallet: target,
            balances: Vec::new(),
            activity_cutoff_unix: 10,
            anchored_at_unix: NOW,
            ledger_hash_after: "unused".to_owned(),
            positions_proof_hash: "positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: NOW,
        }])
        .unwrap();
    release_tx.send(()).unwrap();

    first.await.unwrap().unwrap();
    assert_eq!(
        second.await.unwrap().unwrap(),
        AnchorRefreshOutcome::Skipped
    );
    drop(preparer);
    actor.await.unwrap();
}

#[tokio::test]
async fn deferred_refresh_continues_the_poller() {
    let wallet = wallet(0x44);
    let (dir, paper) = paper(&[wallet]);
    let mut responses = HashMap::new();
    responses.insert(activity_url(wallet), vec![b"[]".to_vec()]);
    responses.insert(
        position_url(wallet, PositionPartition::NotRedeemable),
        vec![
            serde_json::to_vec(&vec![json!({
                "proxyWallet": wallet,
                "asset": "unmapped",
                "conditionId": "condition",
                "size": "1",
                "outcomeIndex": 0
            })])
            .unwrap(),
        ],
    );
    responses.insert(
        position_url(wallet, PositionPartition::Redeemable),
        vec![b"[]".to_vec()],
    );
    let installed = Arc::new(Mutex::new(Vec::new()));
    let harness = poller_harness(
        &dir,
        Arc::clone(&paper),
        &[wallet],
        responses,
        1,
        false,
        Arc::clone(&installed),
        None,
    );
    let PollerHarness {
        poller,
        ingest,
        actor,
        preparer,
    } = harness;
    assert!(poller.run_until(async {}).await.is_ok());
    assert!(installed.lock().unwrap().is_empty());
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    finish_harness(ingest, actor, preparer).await;
}

#[tokio::test]
async fn every_install_rejection_defers_refresh() {
    let wallet = wallet(0x5a);
    let rejections = [
        AnchorInstallError::CoverageGenerationChanged { wallet },
        AnchorInstallError::CursorChanged { wallet },
        AnchorInstallError::LedgerHashChanged { wallet },
        AnchorInstallError::AnchorSeqChanged { wallet },
        AnchorInstallError::Fenced { wallet },
        AnchorInstallError::CutoffRegression {
            wallet,
            stored: 10,
            candidate: 9,
        },
    ];
    for rejection in rejections {
        assert_eq!(
            refresh_outcome_for_install_rejection(rejection).await,
            AnchorRefreshOutcome::Deferred
        );
    }
}

#[tokio::test]
async fn real_transaction_failure_terminates_the_poller_without_swapping() {
    let wallet = wallet(0x55);
    let (dir, paper) = paper(&[wallet]);
    let installed = Arc::new(Mutex::new(Vec::new()));
    let harness = poller_harness(
        &dir,
        Arc::clone(&paper),
        &[wallet],
        stable_bracket_responses(&[wallet]),
        1,
        true,
        installed,
        None,
    );
    let PollerHarness {
        poller,
        ingest,
        actor,
        preparer,
    } = harness;
    let result = poller.run_until(std::future::pending::<()>()).await;
    assert!(matches!(
        result,
        Err(TradePollerOwnerError::AnchorRefresh(_))
    ));
    assert!(paper.position_anchors(&wallet).unwrap().is_empty());
    assert!(paper.leader_positions().unwrap().is_empty());
    finish_harness(ingest, actor, preparer).await;
}
