//! Scenario: durable websocket obligations hold and revisit the fixed source second (#544).

#![cfg(feature = "scenario")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pe_copy_signal_engine::SignalConfig;
use pe_core_types::{
    BasisPoints, ReceivedAt, ReconstructionQuality, SourceId, SourceTimestamp, WalletAddress,
};
use pe_event_log::{ContentType, EnvelopeIn, Reader, Writer};
use pe_paper_state::{AnchorInstallRecord, PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_service::activity_ingest::{ACTIVITY_WS_SOURCE_ID, ActivityIngest, SourceLogHandle};
use pe_service::asset_identity::AssetIdentityResolver;
use pe_service::bucket_commit::BucketCommitEngine;
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::source_event_sink::SourceEventSink;
use pe_service::trade_poller::{
    ACTIVITY_POLL_SOURCE_ID, ReconciliationObligations, TradePoller, TradePollerConfig,
    rebuild_reconciliation_obligations,
};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, GAMMA_BATCH_SIZE, GAMMA_MARKETS_SOURCE_ID,
    ReconciliationFetcher, parse_activity_trade_observation,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use time::OffsetDateTime;
use tokio::sync::mpsc;

const BASE_URL: &str = "https://data.example.test";
const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct QueueFetcher {
    pages: Mutex<VecDeque<Vec<u8>>>,
    calls: AtomicUsize,
}

impl QueueFetcher {
    fn new(pages: impl IntoIterator<Item = Vec<u8>>) -> Self {
        Self {
            pages: Mutex::new(pages.into_iter().collect()),
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ReconciliationFetcher for QueueFetcher {
    fn fetch<'a>(
        &'a self,
        _url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.pages
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| SourceError::Fatal {
                    message: "unexpected page fetch".to_owned(),
                })
        })
    }
}

struct GammaFetcher {
    payload: Vec<u8>,
}

impl ReconciliationFetcher for GammaFetcher {
    fn fetch<'a>(
        &'a self,
        _url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async { Ok(self.payload.clone()) })
    }
}

fn verified_gamma_payload() -> Vec<u8> {
    br#"[{"conditionId":"0xverified-condition","clobTokenIds":["123"]}]"#.to_vec()
}

async fn settle_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if predicate() {
            return;
        }
        tokio::task::yield_now().await;
    }
    assert!(predicate(), "condition did not settle");
}

fn wallet() -> WalletAddress {
    WalletAddress::from_hex(WALLET).unwrap()
}

fn watchlist() -> Watchlist {
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet: wallet(),
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(100),
            lcb_5pct_bps: BasisPoints(100),
            win_rate_bps: BasisPoints(7_000),
            closed_trades_in_window: 1,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        }],
        snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
        active_count: 1,
        incubator_count: 0,
    }
}

fn ws_payload(epoch: i64) -> Vec<u8> {
    format!(
        r#"{{"proxyWallet":"{WALLET}","conditionId":"0xcondition","asset":"123","side":"BUY","size":"5","price":"0.5","timestamp":"{epoch}","transactionHash":"0xabc","outcomeIndex":"0"}}"#
    )
    .into_bytes()
}

fn rest_payload(epoch: i64) -> Vec<u8> {
    format!(
        r#"[{{"proxyWallet":"{WALLET}","type":"TRADE","conditionId":"0xcondition","asset":"123","side":"BUY","size":"5","usdcSize":"2.5","price":"0.5","timestamp":"{epoch}","transactionHash":"0xabc","outcomeIndex":"0"}}]"#
    )
    .into_bytes()
}

fn append_ws(path: &std::path::Path, payload: Vec<u8>, epoch: i64) {
    let timestamp = OffsetDateTime::from_unix_timestamp(epoch).unwrap();
    let mut writer = Writer::open(path).unwrap();
    writer
        .append(EnvelopeIn {
            source_id: SourceId(ACTIVITY_WS_SOURCE_ID.to_owned()),
            schema_version: ACTIVITY_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload,
        })
        .unwrap();
    writer.sync().unwrap();
}

async fn run_once(
    source_log_path: &std::path::Path,
    paper_state: Arc<PaperStateDb>,
    obligations: ReconciliationObligations,
    response: Vec<u8>,
    gamma_response: Vec<u8>,
    now: OffsetDateTime,
) {
    let sink = SourceEventSink::open(source_log_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(4);
    let asset_identity = Arc::new(AssetIdentityResolver::new_runtime(
        Arc::new(GammaFetcher {
            payload: gamma_response,
        }),
        BASE_URL.to_owned(),
        GAMMA_BATCH_SIZE,
        source_log.clone(),
    ));
    let (trigger_tx, trigger_rx) = mpsc::channel(4);
    let health = new_shared_health_with_ws(false, true, 90);
    // Production replays the source receipt index from the on-disk log before the ingest starts
    // (`main.rs`); a restart against a non-empty log must do the same or every synchronized
    // append is refused as non-contiguous.
    let source_receipts =
        pe_service::risk_inputs::SourceReceiptIndex::replay(source_log_path).unwrap();
    let ingest = tokio::spawn(
        ActivityIngest::poll_only(sink, source_rx, trigger_tx, health.clone())
            .with_source_receipt_index(source_receipts)
            .run(),
    );
    let (control_tx, mut control_rx) = mpsc::channel(2);
    let control_paper = paper_state.clone();
    let control_source_log = source_log_path.to_owned();
    let control = tokio::spawn(async move {
        let mut engine = BucketCommitEngine::load(control_paper, PositionLedger::new()).unwrap();
        while let Some(command) = control_rx.recv().await {
            if let OrchestratorControl::CommitActivityBucket {
                aggregates,
                context,
                committed,
            } = command
            {
                assert!(
                    Reader::replay(&control_source_log)
                        .unwrap()
                        .map(|item| item.unwrap().1.source_id.0)
                        .any(|source| source == ACTIVITY_POLL_SOURCE_ID),
                    "bucket delivery cannot precede its raw polling page"
                );
                let _ = committed.send(
                    engine
                        .commit(
                            aggregates,
                            context.as_ref(),
                            pe_service::bucket_commit::FrozenDecisionBasis {
                                win_rate_p: pe_core_types::Probability::ZERO,
                                bankroll: rust_decimal::Decimal::ZERO,
                            },
                        )
                        .map_err(|error| error.to_string()),
                );
            }
        }
    });
    TradePoller::new(
        TradePollerConfig {
            base_url: BASE_URL.to_owned(),
            poll_interval_secs: 0,
            activity_ws_enabled: true,
            copy_latency_budget_secs: 2,
        },
        LiveWatchlist::new(watchlist()),
        Arc::new(QueueFetcher::new([response])),
        asset_identity,
        source_log,
        trigger_rx,
        control_tx,
        paper_state,
        health,
        SignalConfig::default(),
        pe_service::runtime_config::LiveRuntimeConfig::new(
            pe_service::runtime_config::RuntimeConfig::from_service_config(
                &pe_service::config::ServiceConfig::default(),
            ),
        ),
        obligations,
        None,
    )
    .with_clock(Arc::new(move || now))
    .run()
    .await;
    ingest.await.unwrap();
    control.await.unwrap();
}

#[tokio::test]
async fn delayed_indexing_restart_and_four_paths_apply_one_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let source_log = dir.path().join("source.log");
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    paper_state
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    let epoch = 1_900_000_000;
    let now = OffsetDateTime::from_unix_timestamp(epoch + 3).unwrap();
    paper_state
        .set_cursor(&wallet(), epoch.saturating_add(20))
        .unwrap();
    // A wallet with no anchor covers everything (#555): anchor an empty
    // ledger below the scenario epoch so the bucket is post-cutoff.
    paper_state
        .install_anchors(&[AnchorInstallRecord {
            wallet: wallet(),
            balances: Vec::new(),
            activity_cutoff_unix: epoch - 1,
            anchored_at_unix: epoch,
            ledger_hash_after: "empty".to_owned(),
            positions_proof_hash: "positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: epoch,
        }])
        .unwrap();
    for _reader in 0..3 {
        append_ws(&source_log, ws_payload(epoch), epoch);
    }

    let obligations = rebuild_reconciliation_obligations(&source_log, &paper_state).unwrap();
    assert_eq!(obligations.len(), 1, "three reader copies coalesce");
    run_once(
        &source_log,
        paper_state.clone(),
        obligations,
        b"[]".to_vec(),
        verified_gamma_payload(),
        now,
    )
    .await;
    assert_eq!(
        paper_state.cursor(&wallet()).unwrap(),
        Some(epoch.saturating_add(20)),
        "a cursor already ahead cannot abandon the fixed source second"
    );
    let after_restart = rebuild_reconciliation_obligations(&source_log, &paper_state).unwrap();
    assert_eq!(after_restart.len(), 1, "unindexed group survives restart");

    run_once(
        &source_log,
        paper_state.clone(),
        after_restart,
        rest_payload(epoch),
        verified_gamma_payload(),
        now,
    )
    .await;
    assert!(
        rebuild_reconciliation_obligations(&source_log, &paper_state)
            .unwrap()
            .is_empty()
    );
    let positions = paper_state.leader_positions().unwrap();
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].market_id.to_string(), "0xverified-condition");
    assert_eq!(positions[0].outcome_id.0, 0);
    assert_eq!(positions[0].long_contracts.atomic(), 5_000_000);
    let group_id = parse_activity_trade_observation(&ws_payload(epoch))
        .unwrap()
        .group_id
        .key()
        .clone();
    let (provenance, age, reason) = paper_state
        .no_copy_disposition(&group_id)
        .unwrap()
        .expect("slow websocket catch-up is ledger-only");
    assert_eq!(provenance, "activity_ws");
    assert!(age > 2);
    assert_eq!(reason, "stale_activity_ws_past_copy_budget");
    let durable_group = paper_state
        .activity_groups_after(&wallet(), epoch.saturating_sub(1))
        .unwrap()
        .into_iter()
        .find(|group| group.source_trade_id == group_id)
        .expect("the raw websocket group id is the durable corrected group");
    let effect =
        pe_position_ledger::LedgerEffect::from_document(&durable_group.proof_json).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&durable_group.proof_json).unwrap()["version"],
        2
    );
    assert_eq!(
        effect.correction().unwrap().verified.market().to_string(),
        "0xverified-condition"
    );

    let sources: Vec<String> = Reader::replay(&source_log)
        .unwrap()
        .map(|item| item.unwrap().1.source_id.0)
        .collect();
    assert_eq!(
        sources
            .iter()
            .filter(|source| source.as_str() == ACTIVITY_WS_SOURCE_ID)
            .count(),
        3
    );
    assert_eq!(
        sources
            .iter()
            .filter(|source| source.as_str() == ACTIVITY_POLL_SOURCE_ID)
            .count(),
        2,
        "both the pre-index and indexed polling responses were recorded"
    );
}

#[tokio::test]
async fn first_seen_unverified_poller_group_is_raw_only_and_reanchors() {
    let dir = tempfile::tempdir().unwrap();
    let source_log = dir.path().join("source.log");
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    paper_state
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: wallet(),
            complete: true,
            proof_json: "{\"scenario\":\"complete\"}".to_owned(),
            updated_at_unix: 1,
        })
        .unwrap();
    let epoch = 1_900_000_100;
    paper_state.set_cursor(&wallet(), epoch - 1).unwrap();
    paper_state
        .install_anchors(&[AnchorInstallRecord {
            wallet: wallet(),
            balances: Vec::new(),
            activity_cutoff_unix: epoch - 1,
            anchored_at_unix: epoch - 1,
            ledger_hash_after: "empty".to_owned(),
            positions_proof_hash: "positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: epoch - 1,
        }])
        .unwrap();
    let now = OffsetDateTime::from_unix_timestamp(epoch + 3).unwrap();

    run_once(
        &source_log,
        Arc::clone(&paper_state),
        ReconciliationObligations::default(),
        rest_payload(epoch),
        b"[]".to_vec(),
        now,
    )
    .await;

    let group_id = parse_activity_trade_observation(&ws_payload(epoch))
        .unwrap()
        .group_id
        .key()
        .clone();
    assert_eq!(
        paper_state
            .activity_group_state(&group_id)
            .unwrap()
            .unwrap()
            .disposition,
        "raw_only"
    );
    assert_eq!(
        paper_state.no_copy_disposition(&group_id).unwrap(),
        Some(("rest_poll".to_owned(), 3, "identity_unresolved".to_owned()))
    );
    assert!(
        paper_state
            .wallet_coverage(&wallet())
            .unwrap()
            .reanchor_required
    );
    assert!(paper_state.leader_positions().unwrap().is_empty());
    assert!(paper_state.open_decision_pending().unwrap().is_empty());
    let gamma_pages = Reader::replay(&source_log)
        .unwrap()
        .map(|entry| entry.unwrap().1.source_id.0)
        .filter(|source_id| source_id == GAMMA_MARKETS_SOURCE_ID)
        .count();
    assert_eq!(gamma_pages, 2, "open and closed misses are both recorded");
}

#[tokio::test(start_paused = true)]
async fn reader_burst_coalesces_until_the_existing_poll_cadence() {
    let dir = tempfile::tempdir().unwrap();
    let source_log_path = dir.path().join("source.log");
    let paper_state = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    let sink = SourceEventSink::open(&source_log_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(4);
    let asset_identity = Arc::new(AssetIdentityResolver::new_runtime(
        Arc::new(GammaFetcher {
            payload: verified_gamma_payload(),
        }),
        BASE_URL.to_owned(),
        GAMMA_BATCH_SIZE,
        source_log.clone(),
    ));
    let (trigger_tx, trigger_rx) = mpsc::channel(8);
    let trigger_inject = trigger_tx.clone();
    let health = new_shared_health_with_ws(false, true, 90);
    let ingest =
        tokio::spawn(ActivityIngest::poll_only(sink, source_rx, trigger_tx, health.clone()).run());
    let (control_tx, _control_rx) = mpsc::channel(1);
    let fetcher = Arc::new(QueueFetcher::new([b"[]".to_vec(), b"[]".to_vec()]));
    let now = OffsetDateTime::from_unix_timestamp(1_900_000_003).unwrap();
    let poller = tokio::spawn(
        TradePoller::new(
            TradePollerConfig {
                base_url: BASE_URL.to_owned(),
                poll_interval_secs: 20,
                activity_ws_enabled: true,
                copy_latency_budget_secs: 2,
            },
            LiveWatchlist::new(watchlist()),
            fetcher.clone(),
            asset_identity,
            source_log,
            trigger_rx,
            control_tx,
            paper_state,
            health,
            SignalConfig::default(),
            pe_service::runtime_config::LiveRuntimeConfig::new(
                pe_service::runtime_config::RuntimeConfig::from_service_config(
                    &pe_service::config::ServiceConfig::default(),
                ),
            ),
            ReconciliationObligations::default(),
            None,
        )
        .with_clock(Arc::new(move || now))
        .run(),
    );
    settle_until(|| fetcher.calls() == 1).await;

    for received_at in [1_900_000_001, 1_900_000_002, 1_900_000_003] {
        trigger_inject
            .send(pe_service::activity_ingest::ReconciliationTrigger {
                wallet: wallet(),
                source_time: OffsetDateTime::from_unix_timestamp(1_900_000_000).unwrap(),
                source_trade_id: pe_core_types::SourceTradeId("g2:same".to_owned()),
                provenance: pe_copy_signal_engine::TradeProvenance::ActivityWs,
                received_at: OffsetDateTime::from_unix_timestamp(received_at).unwrap(),
                receipt: pe_event_log::AppendReceipt {
                    sequence: pe_core_types::EventSeq(u64::try_from(received_at).unwrap()),
                    this_hash: blake3::Hash::from_bytes([0; 32]),
                },
            })
            .await
            .unwrap();
    }
    tokio::task::yield_now().await;
    assert_eq!(
        fetcher.calls(),
        1,
        "reader rows do not start per-row fetches"
    );
    tokio::time::advance(std::time::Duration::from_secs(19)).await;
    tokio::task::yield_now().await;
    assert_eq!(fetcher.calls(), 1);
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    settle_until(|| fetcher.calls() == 2).await;

    poller.abort();
    ingest.abort();
}
