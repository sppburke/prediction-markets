#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pe_core_types::{
    MarketId, OutcomeId, ReceivedAt, ShareAmount, SourceId, SourceTimestamp, VenueMarketId,
    WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord, WalletRetentionWait};
use pe_position_ledger::PositionLedger;
use pe_service::bucket_commit::{AnchorInstallError, BucketCommitEngine};
use pe_service::clob_book::FixtureClobBookFetcher;
use pe_service::database_retention::{
    DatabaseRetention, DatabaseRetentionInputs, DatabaseRetentionReport, RetentionWait,
    run_database_retention,
};
use pe_service::entry_gate::{CopyEntryGate, CopyEntryGateConfig};
use pe_service::health::new_shared_health;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{MembershipReason, PaperLog, PaperLogRecord, build_leader_ledger};
use pe_service::position_seeder::{AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture};
use pe_service::source_checkpoint::SOURCE_RETENTION_BUFFER_SECS as RETENTION_BUFFER_SECS;
use pe_source_polymarket_public::FixtureFetcher;
use pe_strategy_winner_follow::{ExecutionMode, WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::Watchlist;
use rusqlite::{Connection, params};
use rust_decimal::Decimal;
use serde_json::json;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

mod support;

const NOW: i64 = 2_000_000;
fn wallet() -> WalletAddress {
    WalletAddress([1; 20])
}
fn market() -> MarketId {
    MarketId(VenueMarketId("market".to_owned()))
}

fn append_removal(paper: &PaperLog, unix: i64) {
    let at = OffsetDateTime::from_unix_timestamp(unix).unwrap();
    paper
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: 2,
            parser_version: 1,
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::MembershipChanged {
                reason: MembershipReason::FullRerank,
                removed: vec![wallet()],
                added: Vec::new(),
                capacity: 1,
                ranking_batch_id: None,
                evidence: json!({}),
            })
            .unwrap(),
        })
        .unwrap();
}

fn seed(sql: &Connection) {
    let w = wallet().to_string();
    sql.execute(
        "INSERT INTO wallet_history_status_v2 VALUES (?1, 1, '{}', 1)",
        params![w],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO wallet_market_history_v2 VALUES (?1, 'market', 1, 'old-entry', 'activity_v2')",
        params![w],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO leader_positions VALUES (?1, 'market', 0, '10', '0')",
        params![w],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO poll_cursors(wallet_hex,last_ts_unix) VALUES (?1,1)",
        params![w],
    )
    .unwrap();
    for seq in 0..2 {
        sql.execute(
            "INSERT INTO position_anchors VALUES (?1,?2,1,1,'[]','hash','{\"proof\":1}')",
            params![w, seq],
        )
        .unwrap();
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    db: Arc<PaperStateDb>,
    sql: Connection,
    paper: PaperLog,
    live: LiveWatchlist,
    preparer: AdmissionPreparer,
    tx: mpsc::Sender<OrchestratorControl>,
    retention: Arc<DatabaseRetention>,
    owner: tokio::task::JoinHandle<()>,
    stop: oneshot::Sender<()>,
}
use pe_service::watchlist_admission::AdmissionPreparer;

impl Fixture {
    fn new(removal: Option<i64>, boot_obligation: bool) -> Self {
        Self::with_source(removal, boot_obligation, None)
    }

    fn with_source(
        removal: Option<i64>,
        boot_obligation: bool,
        index: Option<pe_service::risk_inputs::SourceReceiptIndex>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("paper.db");
        let db = Arc::new(PaperStateDb::open(&path).unwrap());
        let sql = Connection::open(path).unwrap();
        seed(&sql);
        let paper = PaperLog::open(dir.path().join("paper.log")).unwrap();
        if let Some(at) = removal {
            append_removal(&paper, at);
        }
        let live = LiveWatchlist::new(Watchlist {
            entries: Vec::new(),
            snapshot_at: SourceTimestamp(OffsetDateTime::UNIX_EPOCH),
            active_count: 0,
            incubator_count: 0,
        });
        let (tx, rx) = mpsc::channel(4);
        let preparer = AdmissionPreparer::new(tx.clone(), db.clone());
        let retention = Arc::new(
            DatabaseRetention::new(
                db.clone(),
                paper.clone(),
                live.clone(),
                preparer.clone(),
                tx.clone(),
                if boot_obligation {
                    HashSet::from([wallet()])
                } else {
                    HashSet::new()
                },
            )
            .unwrap(),
        );
        let mut orchestrator = Orchestrator::new(
            live.clone(),
            OrchestratorConfig {
                bankroll: Decimal::ZERO,
                mode: ExecutionMode::Paper,
                signal_config: Default::default(),
                max_resolution_horizon_secs: 0,
                min_resolution_horizon_secs: 0,
                max_fill_price: Decimal::ZERO,
                min_fill_price: Decimal::ZERO,
                price_impact_cap_bps: 100,
                entry_gate_config: CopyEntryGateConfig,
                runtime_config: None,
                live_accounts: None,
                live_journal: None,
                activity_ws_enabled: false,
                copy_latency_budget_secs: 120,
                watchlist_writer_lock: None,
            },
            WinnerFollowStrategy::new(WinnerFollowConfig::default()),
            paper.clone(),
            db.clone(),
            build_leader_ledger(&db).unwrap(),
            new_shared_health(false),
            MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), String::new()),
            rx,
            None,
            None,
            None,
            Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
        )
        .unwrap();
        if let Some(index) = index {
            orchestrator = orchestrator.with_source_receipt_index(index);
        }
        let (stop, stopped) = oneshot::channel();
        let owner = tokio::spawn(orchestrator.run(async {
            let _ = stopped.await;
        }));
        Self {
            _dir: dir,
            db,
            sql,
            paper,
            live,
            preparer,
            tx,
            retention,
            owner,
            stop,
        }
    }

    async fn run(
        &self,
        now: i64,
        walk: &HashSet<WalletAddress>,
        pins: &HashSet<WalletAddress>,
        observations: &HashSet<WalletAddress>,
        current: bool,
        complete: bool,
    ) -> DatabaseRetentionReport {
        run_database_retention(
            &self.retention,
            DatabaseRetentionInputs {
                now_unix: now,
                committed_boundary_current: current,
                verified_walk_complete: complete,
                walk_wallets: walk,
                reducer_pin_wallets: pins,
                published_observation_wallets: observations,
                obligation_wallets: &HashSet::new(),
            },
            &AtomicBool::new(false),
        )
        .await
        .unwrap()
    }

    async fn finish(self) {
        let _ = self.stop.send(());
        self.owner.await.unwrap();
    }
}

#[tokio::test]
async fn retention_orchestrator_clears_running_ledger_cursor_and_persisted_frontier() {
    let fixture = Fixture::new(Some(NOW - RETENTION_BUFFER_SECS), false);
    let empty = HashSet::new();
    fixture.db.publish_feed_history_frontiers(&json!({"version":1,"frontiers":[{
        "version":1,"wallet":wallet(),"fixed_end":1,"commitment":AppendReceipt {sequence:pe_core_types::EventSeq(1),this_hash:blake3::Hash::from([1;32])},"page_occurrences":[],"pages":[]
    }]})).unwrap();
    let report = fixture.run(NOW, &empty, &empty, &empty, true, true).await;
    assert_eq!(report.anchors_blanked, 1);
    assert_eq!(report.wallets_swapped_out, 1);
    assert!(report.wallets_waiting.is_empty());
    // The removal and one drain transaction (this wallet has no groups or gate results left, so
    // it clears the list entry).
    assert_eq!(
        (report.wallets_drained, report.wallets_listed),
        (1, Some(0))
    );
    assert!(fixture.db.retirement_drains().unwrap().result.is_empty());
    let (captured, capture) = oneshot::channel();
    fixture
        .tx
        .send(OrchestratorControl::CaptureAdmissionLedger {
            wallet: wallet(),
            captured,
        })
        .await
        .unwrap();
    assert_eq!(
        capture.await.unwrap().unwrap(),
        ledger_capture(&PositionLedger::new(), &fixture.db, wallet()).unwrap()
    );
    assert!(!fixture.db.wallet_history_complete(&wallet()).unwrap());
    assert_eq!(
        fixture.db.feed_history_frontiers().unwrap()["frontiers"],
        json!([])
    );
    assert!(
        CopyEntryGate::new(CopyEntryGateConfig, fixture.db.gate_history().unwrap())
            .has_market(&wallet(), &market())
    );
    fixture.finish().await;
}

#[tokio::test]
async fn retention_waits_for_current_boundary_complete_walk_and_both_rebuild_inputs() {
    let fixture = Fixture::new(None, false);
    let empty = HashSet::new();
    let named = HashSet::from([wallet()]);
    for (current, complete, walk, pins, observations, reason) in [
        (
            false,
            true,
            &empty,
            &empty,
            &empty,
            RetentionWait::BoundaryNotCurrent,
        ),
        (
            true,
            false,
            &empty,
            &empty,
            &empty,
            RetentionWait::WalkIncomplete,
        ),
        (
            true,
            true,
            &named,
            &empty,
            &empty,
            RetentionWait::RetainedWindow,
        ),
        (
            true,
            true,
            &empty,
            &named,
            &empty,
            RetentionWait::ReducerPin,
        ),
        (
            true,
            true,
            &empty,
            &empty,
            &named,
            RetentionWait::PublishedObservation,
        ),
    ] {
        let report = fixture
            .run(NOW, walk, pins, observations, current, complete)
            .await;
        assert_eq!(report.wallets_swapped_out, 0);
        assert_eq!(report.wallets_waiting, vec![(wallet(), reason)]);
        assert!(fixture.db.cursor(&wallet()).unwrap().is_some());
    }
    assert_eq!(
        fixture
            .run(NOW, &empty, &empty, &empty, true, true)
            .await
            .wallets_swapped_out,
        1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn retention_keeps_recent_removal_boot_obligation_and_process_removal() {
    let empty = HashSet::new();
    for (removal, boot_obligation, reason) in [
        (
            Some(NOW - RETENTION_BUFFER_SECS + 1),
            false,
            RetentionWait::RecentMembership,
        ),
        (None, true, RetentionWait::BootObligation),
    ] {
        let fixture = Fixture::new(removal, boot_obligation);
        assert_eq!(
            fixture
                .run(NOW, &empty, &empty, &empty, true, true)
                .await
                .wallets_waiting,
            vec![(wallet(), reason)]
        );
        fixture.finish().await;
    }
    let fixture = Fixture::new(None, false);
    append_removal(&fixture.paper, NOW - RETENTION_BUFFER_SECS - 1);
    // Even a backdated removal belongs to this process, and keeps a held feed delivery safe.
    for now in [NOW, NOW + RETENTION_BUFFER_SECS * 2] {
        assert_eq!(
            fixture
                .run(now, &empty, &empty, &empty, true, true)
                .await
                .wallets_waiting,
            vec![(wallet(), RetentionWait::MembershipDuringProcess)]
        );
    }
    fixture.finish().await;
}

#[tokio::test]
async fn retention_recent_terminal_guard_includes_buffer_boundary() {
    let fixture = Fixture::new(None, false);
    let empty = HashSet::new();
    fixture.sql.execute("INSERT INTO decision_pending VALUES ('old', 'r', ?1, 1, '{}', '{}', 'terminal', 'no_copy', ?2)", params![wallet().to_string(), NOW]).unwrap();
    for now in [NOW, NOW + 86400 * 2, NOW + RETENTION_BUFFER_SECS] {
        assert_eq!(
            fixture
                .run(now, &empty, &empty, &empty, true, true)
                .await
                .wallets_waiting,
            vec![(
                wallet(),
                RetentionWait::Durable(WalletRetentionWait::RecentDecision)
            )]
        );
    }
    assert_eq!(
        fixture
            .run(
                NOW + RETENTION_BUFFER_SECS + 1,
                &empty,
                &empty,
                &empty,
                true,
                true
            )
            .await
            .wallets_swapped_out,
        1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn retention_attempt_lock_waits_rechecks_and_stays_held_through_acknowledgement() {
    let fixture = Fixture::new(None, false);
    {
        let attempt = fixture.preparer.lock_for_retention().await;
        let empty = HashSet::new();
        let run = fixture.run(NOW, &empty, &empty, &empty, true, true);
        tokio::pin!(run);
        // Paced blanking and paging finish well within this; the job then waits for the lock.
        tokio::select! {
            _ = &mut run => panic!("the job finished while the attempt lock was held"),
            () = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
        }
        assert!(fixture.db.cursor(&wallet()).unwrap().is_some());
        // A bracket completed before releasing its lock; the job must repeat its guard query.
        fixture
            .sql
            .execute(
                "UPDATE position_anchors SET anchored_at_unix=?1",
                params![NOW],
            )
            .unwrap();
        drop(attempt);
        assert_eq!(
            run.await.wallets_waiting,
            vec![(
                wallet(),
                RetentionWait::Durable(WalletRetentionWait::RecentAnchor)
            )]
        );
    }
    let empty = HashSet::new();
    // Use a parked owner to prove the lock cannot be released just after sending RetireWallet.
    let (tx, mut rx) = mpsc::channel(1);
    let preparer = AdmissionPreparer::new(tx.clone(), fixture.db.clone());
    fixture
        .sql
        .execute("UPDATE position_anchors SET anchored_at_unix=1", [])
        .unwrap();
    let state = DatabaseRetention::new(
        fixture.db.clone(),
        fixture.paper.clone(),
        fixture.live.clone(),
        preparer.clone(),
        tx,
        HashSet::new(),
    )
    .unwrap();
    let cancel = AtomicBool::new(false);
    let run = run_database_retention(
        &state,
        DatabaseRetentionInputs {
            now_unix: NOW,
            committed_boundary_current: true,
            verified_walk_complete: true,
            walk_wallets: &empty,
            reducer_pin_wallets: &empty,
            published_observation_wallets: &empty,
            obligation_wallets: &empty,
        },
        &cancel,
    );
    tokio::pin!(run);
    let message = tokio::select! {
        _ = &mut run => panic!("the job finished before handing off its removal"),
        message = rx.recv() => message.unwrap(),
    };
    let lock = preparer.lock_for_retention();
    tokio::pin!(lock);
    assert!(futures::poll!(&mut lock).is_pending());
    if let OrchestratorControl::RetireWallet { acknowledged, .. } = message {
        acknowledged
            .send(Ok(pe_service::database_retention::WalletRetirement {
                waiting: Some(RetentionWait::StructuralMember),
                lock_time: None,
            }))
            .unwrap();
    }
    assert_eq!(run.await.unwrap().wallets_swapped_out, 0);
    drop(lock.await);
    fixture.finish().await;
}

#[tokio::test]
async fn retention_orchestrator_rechecks_membership_and_open_decisions() {
    let fixture = Fixture::new(None, false);
    fixture
        .live
        .scenario_commit_structural_change(&[], &[wallet()]);
    for expected in [
        RetentionWait::StructuralMember,
        RetentionWait::Durable(WalletRetentionWait::OpenDecision),
    ] {
        let (acknowledged, ack) = oneshot::channel();
        fixture
            .tx
            .send(OrchestratorControl::RetireWallet {
                wallet: wallet(),
                recent_since_unix: NOW - RETENTION_BUFFER_SECS,
                acknowledged,
            })
            .await
            .unwrap();
        assert_eq!(ack.await.unwrap().unwrap().waiting, Some(expected));
        assert!(fixture.db.cursor(&wallet()).unwrap().is_some());
        fixture
            .live
            .scenario_commit_structural_change(&[wallet()], &[]);
        fixture.sql.execute("INSERT OR IGNORE INTO decision_pending VALUES ('open', 'r', ?1, 1, '{}', '{}', 'open', NULL, 1)", params![wallet().to_string()]).unwrap();
    }
    fixture.finish().await;
}

#[tokio::test]
async fn retention_cancelled_while_waiting_for_admission_leaves_wallet_intact() {
    let fixture = Fixture::new(None, false);
    {
        let _attempt = fixture.preparer.lock_for_retention().await;
        let empty = HashSet::new();
        let cancel = AtomicBool::new(false);
        let run = run_database_retention(
            &fixture.retention,
            DatabaseRetentionInputs {
                now_unix: NOW,
                committed_boundary_current: true,
                verified_walk_complete: true,
                walk_wallets: &empty,
                reducer_pin_wallets: &empty,
                published_observation_wallets: &empty,
                obligation_wallets: &empty,
            },
            &cancel,
        );
        tokio::pin!(run);
        tokio::select! {
            _ = &mut run => panic!("the job finished while the attempt lock was held"),
            () = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
        }
        cancel.store(true, Ordering::Release);
        assert!(run.await.unwrap().cancelled);
        assert!(fixture.db.cursor(&wallet()).unwrap().is_some());
        drop(_attempt);
    }
    fixture.finish().await;
}

#[test]
fn retention_rejects_anchor_capture_from_before_swap_out() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("paper.db");
    let db = Arc::new(PaperStateDb::open(&path).unwrap());
    let sql = Connection::open(&path).unwrap();
    seed(&sql);
    let mut engine =
        BucketCommitEngine::load(db.clone(), build_leader_ledger(&db).unwrap()).unwrap();
    let before = ledger_capture(engine.ledger(), &db, wallet()).unwrap();
    let make_install =
        |capture: pe_service::orchestrator_control::AdmissionLedgerCapture| AnchorInstall {
            fresh_history: Vec::new(),
            expected_fence: None,
            history_status: Some(WalletHistoryStatusRecord {
                wallet: wallet(),
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: NOW,
            }),
            wallet: wallet(),
            balances: vec![(market(), OutcomeId(0), ShareAmount::from_whole(25).unwrap())],
            cutoff: NOW,
            proof: AnchorProof {
                positions_proof_hash: "proof".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "g".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix: NOW,
            },
            expected: AnchorExpectation {
                ledger_hash: capture.hash,
                cursor: capture.cursor,
                anchor_seq: capture.anchor_seq,
                coverage_generation: capture.coverage_generation,
            },
        };
    assert_eq!(
        engine
            .retire_wallet(wallet(), NOW - RETENTION_BUFFER_SECS, None)
            .unwrap()
            .result,
        None
    );
    assert!(engine.ledger().position(&wallet()).is_none());
    assert!(!engine.history_complete(&wallet()));
    assert!(matches!(
        engine.install_anchors(&[make_install(before)]),
        Err(AnchorInstallError::LedgerHashChanged { .. })
    ));
}

fn feed_payload(source_unix: i64) -> Vec<u8> {
    serde_json::to_vec(&json!({"proxyWallet":wallet(),"conditionId":"market","asset":"123","side":"BUY","size":"5","price":"0.5",
        "timestamp":source_unix,"transactionHash":format!("0x{}", "1".repeat(64)),"outcomeIndex":0})).unwrap()
}

fn append_source(
    path: &std::path::Path,
    source: &str,
    schema: u32,
    payload: Vec<u8>,
    at: i64,
) -> AppendReceipt {
    let mut writer = pe_event_log::Writer::open(path).unwrap();
    let at = OffsetDateTime::from_unix_timestamp(at).unwrap();
    let parser_version = if source == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
    {
        pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_PARSER_VERSION
    } else {
        pe_source_polymarket_public::ACTIVITY_PARSER_VERSION
    };
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(source.to_owned()),
            schema_version: schema,
            parser_version,
            observed_at: SourceTimestamp(at),
            received_at: ReceivedAt(at),
            content_type: ContentType::Json,
            payload,
        })
        .unwrap()
}

#[tokio::test]
async fn retention_feed_and_commitment_walk_guards_cover_deferred_quiet_and_backfill_logs() {
    use pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID;
    use pe_service::bucket_commit::{
        ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION, ACTIVITY_READ_COMMITMENT_SOURCE_ID,
        ActivityReadCommitment, activity_read_commitment_payload,
    };
    for (source_unix, received_unix, current, commitment) in [
        (1, 1, false, false),  // Deferred advance cannot retire even an old observation.
        (1, 1, true, false),   // A quiet log still keeps its last frame at the boundary.
        (1, NOW, true, false), // Old trade delivered by a recent backfill.
        (1, NOW, true, true),
    ] {
        let fixture = Fixture::new(None, false);
        let path = fixture._dir.path().join("source.log");
        let (source, schema, payload) = if commitment {
            (
                ACTIVITY_READ_COMMITMENT_SOURCE_ID,
                ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
                activity_read_commitment_payload(wallet(), source_unix, &[], &[]).unwrap(),
            )
        } else {
            (
                ACTIVITY_WS_SOURCE_ID,
                pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                feed_payload(source_unix),
            )
        };
        append_source(&path, source, schema, payload, received_unix);
        let mut wallets = HashSet::new();
        for frame in pe_event_log::Reader::replay(&path).unwrap() {
            let (_, envelope) = frame.unwrap();
            let named = if commitment {
                serde_json::from_slice::<ActivityReadCommitment>(&envelope.payload)
                    .unwrap()
                    .wallet
            } else {
                pe_source_polymarket_public::parse_activity_trade_observation(&envelope.payload)
                    .unwrap()
                    .wallet
            };
            wallets.insert(named);
        }
        let empty = HashSet::new();
        let report = fixture
            .run(NOW, &wallets, &empty, &empty, current, true)
            .await;
        assert_eq!(
            report.wallets_waiting,
            vec![(
                wallet(),
                if current {
                    RetentionWait::RetainedWindow
                } else {
                    RetentionWait::BoundaryNotCurrent
                }
            )]
        );
        fixture.finish().await;
    }
}

#[tokio::test]
async fn retention_process_removal_keeps_a_held_observation_released_later() {
    let fixture = Fixture::new(None, false);
    let held = feed_payload(1);
    append_removal(&fixture.paper, NOW - RETENTION_BUFFER_SECS - 1);
    let path = fixture._dir.path().join("source.log");
    append_source(
        &path,
        pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID,
        pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
        held,
        1,
    );
    assert_eq!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(&path, &fixture.db)
            .unwrap()
            .len(),
        1
    );
    let empty = HashSet::new();
    assert_eq!(
        fixture
            .run(NOW, &empty, &empty, &empty, true, true)
            .await
            .wallets_waiting,
        vec![(wallet(), RetentionWait::MembershipDuringProcess)]
    );
    assert_eq!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(&path, &fixture.db)
            .unwrap()
            .frame_recovery_receipts()
            .0
            .len(),
        1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn retention_failed_catchup_group_still_waits_for_pin_and_published_observation() {
    let fixture = Fixture::new(None, false);
    fixture
        .sql
        .execute_batch("DELETE FROM position_anchors;")
        .unwrap();
    let path = fixture._dir.path().join("source.log");
    let payload = feed_payload(1);
    let observed = pe_source_polymarket_public::parse_activity_trade_observation(&payload).unwrap();
    append_source(
        &path,
        pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID,
        pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
        payload,
        1,
    );
    fixture
        .sql
        .execute(
            "INSERT INTO activity_groups VALUES (?1,'tx',?2,1,'r','TRADE','applied','{}')",
            params![observed.group_id.key().0, wallet().to_string()],
        )
        .unwrap();
    let reopened = PaperStateDb::open(&fixture._dir.path().join("paper.db")).unwrap();
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(&path, &reopened)
            .unwrap()
            .is_empty()
    );
    let named = HashSet::from([wallet()]);
    let empty = HashSet::new();
    // These jobs have nothing to advance; the committed authority and last published state
    // are still independent rebuild inputs despite the group suppressing boot obligations.
    for (pins, observations, reason) in [
        (&named, &named, RetentionWait::ReducerPin),
        (&empty, &named, RetentionWait::PublishedObservation),
    ] {
        assert_eq!(
            fixture
                .run(NOW, &empty, pins, observations, true, true)
                .await
                .wallets_waiting,
            vec![(wallet(), reason)]
        );
    }
    assert_eq!(
        fixture
            .run(NOW, &empty, &empty, &empty, true, true)
            .await
            .wallets_swapped_out,
        1
    );
    fixture.finish().await;
}

#[tokio::test]
async fn retention_checks_current_obligations_and_cancellation_before_mutation() {
    let fixture = Fixture::new(None, false);
    let empty = HashSet::new();
    let named = HashSet::from([wallet()]);
    let cancel = AtomicBool::new(true);
    let inputs = |obligations| DatabaseRetentionInputs {
        now_unix: NOW,
        committed_boundary_current: true,
        verified_walk_complete: true,
        walk_wallets: &empty,
        reducer_pin_wallets: &empty,
        published_observation_wallets: &empty,
        obligation_wallets: obligations,
    };
    let report = run_database_retention(&fixture.retention, inputs(&named), &cancel)
        .await
        .unwrap();
    assert!(report.cancelled);
    assert_eq!(report.anchors_blanked, 0);
    cancel.store(false, Ordering::Release);
    assert_eq!(
        run_database_retention(&fixture.retention, inputs(&named), &cancel)
            .await
            .unwrap()
            .wallets_waiting,
        vec![(wallet(), RetentionWait::CurrentObligation)]
    );
    fixture.finish().await;
}

#[tokio::test]
async fn retention_forgets_engine_and_shared_index_verified_frontiers() {
    use pe_service::orchestrator_control::ReconciliationUpdate;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.log");
    let mut writer = pe_event_log::Writer::open(&path).unwrap();
    let (read, commitment) =
        support::append_committed_read_v2(&mut writer, wallet(), b"[]", 1, NOW);
    let proof: serde_json::Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
    let frontier = pe_service::frame_admission::FeedHistoryFrontier {
        version: 1,
        wallet: wallet(),
        fixed_end: 1,
        commitment,
        page_occurrences: vec![read.page],
        pages: serde_json::from_value(proof["pages"].clone()).unwrap(),
    };
    let index = pe_service::risk_inputs::SourceReceiptIndex::replay(&path).unwrap();
    let fixture = Fixture::with_source(None, false, Some(index.clone()));
    for _ in 0..2 {
        let (acknowledged, ack) = oneshot::channel();
        fixture
            .tx
            .send(OrchestratorControl::ReconciliationUpdate {
                update: ReconciliationUpdate::Frontier(frontier.clone(), None),
                acknowledged,
            })
            .await
            .unwrap();
        ack.await.unwrap().unwrap();
        assert_eq!(index.read_verification_count(commitment), 1);
    }
    let attempt = fixture.preparer.lock_for_retention().await;
    let (acknowledged, ack) = oneshot::channel();
    fixture
        .tx
        .send(OrchestratorControl::RetireWallet {
            wallet: wallet(),
            recent_since_unix: NOW - RETENTION_BUFFER_SECS,
            acknowledged,
        })
        .await
        .unwrap();
    assert_eq!(ack.await.unwrap().unwrap().waiting, None);
    drop(attempt);
    let (acknowledged, ack) = oneshot::channel();
    fixture
        .tx
        .send(OrchestratorControl::ReconciliationUpdate {
            update: ReconciliationUpdate::Frontier(frontier, None),
            acknowledged,
        })
        .await
        .unwrap();
    ack.await.unwrap().unwrap();
    assert_eq!(
        index.read_verification_count(commitment),
        2,
        "retirement must remove the authenticated cache entry"
    );
    assert_eq!(
        fixture.db.feed_history_frontiers().unwrap()["frontiers"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "the old engine frontier must not suppress a returning wallet's publication"
    );
    fixture.finish().await;
}

fn seed_trade_rows(sql: &Connection, wallet: WalletAddress, groups: usize) {
    let w = wallet.to_string();
    for n in 0..groups {
        let id = format!("group-{w}-{n}");
        sql.execute(
            "INSERT INTO activity_groups VALUES (?1, 'tx', ?2, 1, 'r', 'TRADE', 'applied', '{}')",
            params![id, w],
        )
        .unwrap();
        sql.execute(
            "INSERT INTO seen_trades_v2 VALUES (?1, 2, 'tx')",
            params![id],
        )
        .unwrap();
        sql.execute(
            "INSERT INTO activity_group_revisions VALUES (?1, 'r', 'tx', 'applied', '{}', 1)",
            params![id],
        )
        .unwrap();
    }
    let gate = format!("gate-{w}");
    sql.execute(
        "INSERT INTO entry_gate_results VALUES (?1, ?2, 'market', 1, 'admitted', 1)",
        params![gate, w],
    )
    .unwrap();
    sql.execute(
        "INSERT INTO seen_trades_v2 VALUES (?1, 2, 'tx')",
        params![gate],
    )
    .unwrap();
}

fn rows(sql: &Connection, table: &str) -> i64 {
    sql.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .unwrap()
}

/// Remove the wallet through the orchestrator and stop before its drain, as a stop mid-drain leaves it.
async fn remove_without_drain(fixture: &Fixture) {
    let _attempt = fixture.preparer.lock_for_retention().await;
    let (acknowledged, ack) = oneshot::channel();
    fixture
        .tx
        .send(OrchestratorControl::RetireWallet {
            wallet: wallet(),
            recent_since_unix: NOW - RETENTION_BUFFER_SECS,
            acknowledged,
        })
        .await
        .unwrap();
    assert_eq!(ack.await.unwrap().unwrap().waiting, None);
    assert_eq!(
        fixture.db.retirement_drains().unwrap().result,
        vec![wallet()]
    );
}

/// A listed wallet (removed earlier; three groups and a gate result left), a fenced candidate, and an
/// eligible departed candidate with one group and a gate result.
async fn job_fixture() -> (Fixture, WalletAddress, WalletAddress) {
    let fixture = Fixture::new(Some(NOW - RETENTION_BUFFER_SECS), false);
    seed_trade_rows(&fixture.sql, wallet(), 3);
    remove_without_drain(&fixture).await;
    let fenced = WalletAddress([2; 20]);
    let departed = WalletAddress([3; 20]);
    fixture
        .sql
        .execute(
            "INSERT INTO wallet_fences VALUES (?1, 'fenced-trade', 'fixture', '{}', 1)",
            params![fenced.to_string()],
        )
        .unwrap();
    fixture
        .sql
        .execute(
            "INSERT INTO poll_cursors(wallet_hex,last_ts_unix) VALUES (?1,1)",
            params![fenced.to_string()],
        )
        .unwrap();
    seed_trade_rows(&fixture.sql, departed, 1);
    (fixture, fenced, departed)
}

/// One database hold as the job logged it, stamped on Tokio's clock.
struct Hold {
    message: String,
    wallet: Option<String>,
    at_micros: u128,
    held_micros: u128,
    count: Option<u128>,
}

fn micros(value: &serde_json::Value) -> u128 {
    value
        .as_u64()
        .map(u128::from)
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .unwrap()
}

fn holds(logs: &support::RetentionLogs) -> Vec<Hold> {
    logs.text()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .filter(|event| {
            matches!(
                event["message"].as_str(),
                Some(
                    "source retention drain list read"
                        | "source retention drain transaction"
                        | "source retention database transaction"
                        | "source retention database scan"
                        | "source retention eligibility check"
                )
            )
        })
        .map(|event| Hold {
            message: event["message"].as_str().unwrap().to_owned(),
            wallet: event["wallet"].as_str().map(str::to_owned),
            at_micros: micros(&event["timestamp"]),
            held_micros: micros(&event["lock_time_micros"]),
            count: event
                .get("anchors_blanked")
                .or_else(|| event.get("wallets"))
                .or_else(|| event.get("trade_ids"))
                .map(micros),
        })
        .collect()
}

fn assert_paced(holds: &[Hold], returned_at_micros: u128) {
    // Synchronous holds start and end at the same paused-clock instant; each log stamps that instant
    // after releasing the connection. Compare the next hold's start (or return) with this hold's end.
    for (index, hold) in holds.iter().enumerate() {
        let next = holds
            .get(index + 1)
            .map_or(returned_at_micros, |next| next.at_micros);
        let required = hold.held_micros.max(50_000);
        let gap = next - hold.at_micros;
        assert!(
            gap >= required,
            "hold {index} ({}) was followed after {gap} us, not {required} us",
            hold.message
        );
    }
}

#[tokio::test(start_paused = true)]
async fn retention_pauses_after_every_hold_and_drains_listed_wallets_first() {
    let logs = support::RetentionLogs::default();
    let start = tokio::time::Instant::now();
    let _subscriber = tracing::subscriber::set_default(logs.subscriber_on_tokio_clock(start));
    let (fixture, fenced, departed) = job_fixture().await;
    // Keep an anchor-bearing wallet: two consecutive full blanking batches, then the empty batch.
    for seq in 0..33 {
        fixture
            .sql
            .execute(
                "INSERT INTO position_anchors VALUES (?1,?2,1,1,'[]','hash','{\"proof\":1}')",
                params![fenced.to_string(), seq],
            )
            .unwrap();
    }
    // 128 fenced candidates plus the departed one produce a full page and a partial page.
    for byte in 4..=130 {
        let w = WalletAddress([byte; 20]).to_string();
        fixture
            .sql
            .execute(
                "INSERT INTO wallet_fences VALUES (?1, 'fenced-trade', 'fixture', '{}', 1)",
                params![w],
            )
            .unwrap();
        fixture
            .sql
            .execute(
                "INSERT INTO poll_cursors(wallet_hex,last_ts_unix) VALUES (?1,1)",
                params![w],
            )
            .unwrap();
    }
    let empty = HashSet::new();
    let report = fixture.run(NOW, &empty, &empty, &empty, true, true).await;
    let returned_at = start.elapsed().as_micros();
    assert_eq!(report.anchors_blanked, 32);
    assert_eq!(report.wallets_waiting.len(), 128);
    assert!(
        report
            .wallets_waiting
            .iter()
            .all(|(_, reason)| *reason == RetentionWait::Durable(WalletRetentionWait::Fenced))
    );
    assert_eq!(
        (
            report.wallets_swapped_out,
            report.wallets_drained,
            report.wallets_listed
        ),
        (1, 2, Some(0))
    );
    let holds = holds(&logs);
    assert_eq!(holds.len(), 145);
    let listed = wallet().to_string();
    let list = "source retention drain list read";
    let drain = "source retention drain transaction";
    let transaction = "source retention database transaction";
    let scan = "source retention database scan";
    let check = "source retention eligibility check";
    assert_eq!(
        holds[..4]
            .iter()
            .map(|hold| (hold.message.as_str(), hold.wallet.as_deref()))
            .collect::<Vec<_>>(),
        vec![
            (list, None),
            (drain, Some(listed.as_str())),
            (drain, Some(listed.as_str())),
            (drain, Some(listed.as_str())),
        ]
    );
    assert_eq!(
        holds
            .iter()
            .filter(|hold| hold.message == transaction && hold.wallet.is_none())
            .map(|hold| hold.count.unwrap())
            .collect::<Vec<_>>(),
        vec![16, 16, 0]
    );
    assert_eq!(
        holds
            .iter()
            .filter(|hold| hold.message == scan)
            .map(|hold| hold.count.unwrap())
            .collect::<Vec<_>>(),
        vec![128, 1, 0]
    );
    assert_eq!(
        holds
            .iter()
            .filter(|hold| hold.message == check
                && hold.wallet.as_deref() == Some(departed.to_string().as_str()))
            .count(),
        2
    );
    assert_eq!(holds.last().unwrap().message, list);
    assert_paced(&holds, returned_at);
    fixture.finish().await;
}

#[tokio::test(start_paused = true)]
async fn admission_drain_pauses_after_the_list_read_and_every_transaction_before_return() {
    let logs = support::RetentionLogs::default();
    let start = tokio::time::Instant::now();
    let _subscriber = tracing::subscriber::set_default(logs.subscriber_on_tokio_clock(start));
    let fixture = Fixture::new(Some(NOW - RETENTION_BUFFER_SECS), false);
    seed_trade_rows(&fixture.sql, wallet(), 501);
    remove_without_drain(&fixture).await;
    let result = fixture.preparer.prepare(&[wallet()]).await.unwrap();
    let returned_at = start.elapsed().as_micros();
    assert!(result.admitted.is_empty());
    assert_eq!(result.deferred[0].kind, "history.missing");
    assert!(fixture.db.retirement_drains().unwrap().result.is_empty());
    let holds = holds(&logs);
    assert_eq!(
        holds
            .iter()
            .map(|hold| (hold.message.as_str(), hold.count.unwrap()))
            .collect::<Vec<_>>(),
        vec![
            ("source retention drain list read", 1),
            ("source retention drain transaction", 500),
            ("source retention drain transaction", 1),
            ("source retention drain transaction", 1),
            ("source retention drain transaction", 0),
        ]
    );
    assert_paced(&holds, returned_at);
    fixture.finish().await;
}

#[tokio::test(start_paused = true)]
async fn retention_cancelled_during_a_pause_starts_no_further_hold_and_the_next_job_resumes() {
    // Cancel during the pause after the drain list read, the first drain transaction, blanking, the
    // first page, the fenced wallet's check, the departed wallet's second check, its removal and the
    // terminal empty page. No further hold starts (the cancelled run leaves the list unread);
    // whatever is still listed drains first in the next job.
    for stop_after in [0, 1, 4, 5, 6, 8, 9, 13] {
        let logs = support::RetentionLogs::default();
        let _subscriber = tracing::subscriber::set_default(
            logs.subscriber_on_tokio_clock(tokio::time::Instant::now()),
        );
        let (fixture, _, departed) = job_fixture().await;
        let empty = HashSet::new();
        let cancel = AtomicBool::new(false);
        let inputs = || DatabaseRetentionInputs {
            now_unix: NOW,
            committed_boundary_current: true,
            verified_walk_complete: true,
            walk_wallets: &empty,
            reducer_pin_wallets: &empty,
            published_observation_wallets: &empty,
            obligation_wallets: &empty,
        };
        let report = {
            let run = run_database_retention(&fixture.retention, inputs(), &cancel);
            tokio::pin!(run);
            loop {
                tokio::select! {
                    biased;
                    () = tokio::time::sleep(std::time::Duration::from_millis(1)) => {
                        if holds(&logs).len() > stop_after {
                            cancel.store(true, Ordering::Release);
                        }
                    }
                    report = &mut run => break report.unwrap(),
                }
            }
        };
        assert!(report.cancelled, "stop after hold {stop_after}");
        assert_eq!(
            holds(&logs).len(),
            stop_after + 1,
            "stop after hold {stop_after}"
        );
        assert_eq!(report.wallets_listed, None, "stop after hold {stop_after}");
        let listed = fixture.db.retirement_drains().unwrap().result;
        match stop_after {
            0 | 1 => assert_eq!(listed, vec![wallet()]),
            9 => assert_eq!(listed, vec![departed]),
            _ => assert!(listed.is_empty(), "stop after hold {stop_after}"),
        }
        // Only the fenced wallet's check (hold 6) records a wait before these boundaries.
        assert_eq!(
            report.wallets_waiting.len(),
            usize::from(stop_after >= 6),
            "stop after hold {stop_after}"
        );
        cancel.store(false, Ordering::Release);
        let report = run_database_retention(&fixture.retention, inputs(), &cancel)
            .await
            .unwrap();
        assert!(!report.cancelled);
        assert_eq!(report.wallets_listed, Some(0));
        for table in ["activity_groups", "entry_gate_results"] {
            assert_eq!(
                rows(&fixture.sql, table),
                0,
                "{table} after resuming from hold {stop_after}"
            );
        }
        fixture.finish().await;
    }
}
