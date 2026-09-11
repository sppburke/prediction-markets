//! Boot recovery needs the source sink before observation producers are released.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pe_copy_signal_engine::SignalConfig;
use pe_core_types::{
    AccountId, BasisPoints, CollateralAmount, PolymarketConditionId, Probability, ReceivedAt,
    ReconstructionQuality, SourceId, SourceTimestamp, SourceTradeId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Writer};
use pe_execution_core::{AdmissionReceipts, LiveAdmissionArtifact};
use pe_paper_state::{DecisionPendingState, PaperStateDb, WalletHistoryStatusRecord};
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_service::activity_ingest::{
    ActivityIngest, ActivityIngestError, Dialer, ReconciliationTrigger, SourceLogHandle,
};
use pe_service::bucket_commit::{
    BucketCommitEngine, DecisionContinuationV3, FrozenDecisionBasis, PaperFreshnessPolicy,
    validate_open_continuations,
};
use pe_service::clob_book::{FixtureClobBookFetcher, OrderBook};
use pe_service::decision_replay::replay_decision_pending;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::{SharedHealth, new_shared_health_with_ws};
use pe_service::live_accounts::{AccountContext, LiveAccounts, LiveAccountsSnapshot};
use pe_service::live_venue_adapter::LiveAdmissionBuilder;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mark_prices::HistoricalMarkAdapter;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{
    Orchestrator, OrchestratorConfig, SCENARIO_TERMINAL_CLOCK, ScenarioHooks,
};
use pe_service::paper_recovery::{
    CanonicalFillResult, CanonicalResolutionResult, PaperLogFrame, PaperLogRecord,
    QualificationStarted, TailBinding, build_leader_ledger, scan_paper_log,
};
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::runtime_config::RuntimeConfig;
use pe_service::source_event_sink::SourceEventSink;
use pe_service::supabase_sink::SupabaseFillRow;
use pe_service::supabase_state::{
    FillV2Outcome, PreparedFillRequest, PreparedResolutionRequest, SupabaseStateError,
    SupabaseStateTrait,
};
use pe_service::supervisor::{ShutdownPhase, TaskName};
use pe_source_polymarket_public::{
    ACTIVITY_WS_READER_COUNT, ReconciliationFetcher, validate_live_market,
};
use pe_strategy_winner_follow::{ExecutionMode, PerTradeCap, SizingMode, WinnerFollowStrategy};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::parse_compact_market;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::Value;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot, watch};

const EPOCH: i64 = 1_800_000_000;
const CASH: Decimal = dec!(1000);
const GAMMA: &[u8] = include_bytes!("fixtures/golden_stream_v1/gamma_long.json");
const LONG: &[u8] = include_bytes!("fixtures/golden_stream_v1/clob_long.json");
const COMPACT: &[u8] = include_bytes!("fixtures/golden_stream_v1/clob_compact.json");

fn at() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(EPOCH).unwrap()
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("boot-order barrier timed out")
}

fn wallet() -> WalletAddress {
    WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap()
}

fn watchlist() -> LiveWatchlist {
    LiveWatchlist::new(Watchlist {
        entries: vec![WatchlistEntry {
            wallet: wallet(),
            tier: WatchlistTier::Active,
            leader_score_bps: BasisPoints(100),
            lcb_5pct_bps: BasisPoints(100),
            win_rate_bps: BasisPoints(7000),
            closed_trades_in_window: 90,
            reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        }],
        snapshot_at: SourceTimestamp(at()),
        active_count: 1,
        incubator_count: 0,
    })
}

fn envelope(source: &str, payload: &[u8]) -> EnvelopeIn {
    EnvelopeIn {
        source_id: SourceId(source.to_owned()),
        schema_version: 1,
        parser_version: 1,
        observed_at: SourceTimestamp(at()),
        received_at: ReceivedAt(at()),
        content_type: ContentType::Json,
        payload: payload.to_vec(),
    }
}

struct IngestOwner {
    source_path: std::path::PathBuf,
    source: SourceLogHandle,
    index: SourceReceiptIndex,
    health: SharedHealth,
    start: watch::Sender<bool>,
    dials: mpsc::Receiver<usize>,
    dial_count: Arc<AtomicUsize>,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), ActivityIngestError>>,
    triggers: mpsc::Receiver<ReconciliationTrigger>,
}

impl IngestOwner {
    fn start(path: &Path, readers: bool) -> Self {
        let sink = SourceEventSink::open(path).unwrap();
        let index = SourceReceiptIndex::replay(path).unwrap();
        let (source, source_rx) = SourceLogHandle::channel(8);
        let (trigger_tx, triggers) = mpsc::channel(1);
        let health = new_shared_health_with_ws(false, readers, 90);
        let (start, gate) = watch::channel(false);
        let (dialed, dials) = mpsc::channel(ACTIVITY_WS_READER_COUNT);
        let dial_count = Arc::new(AtomicUsize::new(0));
        let dialer: Dialer = {
            let dial_count = dial_count.clone();
            Arc::new(move |slot| {
                dial_count.fetch_add(1, Ordering::SeqCst);
                let dialed = dialed.clone();
                Box::pin(async move {
                    dialed.send(slot).await.unwrap();
                    std::future::pending().await
                })
            })
        };
        // Mirror main: start the sink owner now; gate only the websocket reader pool.
        let ingest = if readers {
            ActivityIngest::with_dialer(
                watchlist(),
                sink,
                source_rx,
                trigger_tx,
                health.clone(),
                dialer,
            )
            .with_reader_start_gate(gate)
        } else {
            ActivityIngest::poll_only(sink, source_rx, trigger_tx, health.clone())
        }
        .with_source_receipt_index(index.clone());
        let dropped = ingest.reconciliation_triggers_dropped_counter();
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        assert_eq!(
            TaskName::ActivityIngest.stop_phase(),
            ShutdownPhase::StopSinks
        );
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(ingest.run_until(async move {
            let _ = stopped.await;
        }));
        Self {
            source_path: path.to_owned(),
            source,
            index,
            health,
            start,
            dials,
            dial_count,
            stop,
            task,
            triggers,
        }
    }

    fn captures(&self) -> Vec<pe_event_log::EventEnvelope> {
        pe_event_log::Reader::replay(&self.source_path)
            .unwrap()
            .map(|frame| {
                let (sequence, envelope) = frame.unwrap();
                assert_eq!(
                    self.index.receipt_at(sequence).unwrap().unwrap().0,
                    AppendReceipt {
                        sequence,
                        this_hash: envelope.this_hash
                    }
                );
                envelope
            })
            .collect()
    }

    fn captured_source(&self, receipt: AppendReceipt) -> String {
        let captures = self.captures();
        let capture = captures.last().unwrap();
        assert_eq!(capture.this_hash, receipt.this_hash);
        capture.source_id.0.clone()
    }

    fn assert_readers_held(&mut self) {
        assert!(!*self.start.borrow());
        assert_eq!(self.dial_count.load(Ordering::SeqCst), 0);
        assert!(self.dials.try_recv().is_err());
        assert!(self.health.lock().unwrap().ws_readers.iter().all(|reader| {
            !reader.connected && !reader.fan_in_blocked && reader.last_wire_frame_at.is_none()
        }));
        assert!(!self.task.is_finished());
    }

    async fn release_readers(&mut self) {
        self.start.send(true).unwrap();
        let mut slots = BTreeSet::new();
        for _ in 0..ACTIVITY_WS_READER_COUNT {
            assert!(slots.insert(bounded(self.dials.recv()).await.unwrap()));
        }
        assert_eq!(slots, (0..ACTIVITY_WS_READER_COUNT).collect());
    }

    async fn stop(self) {
        self.stop.send(()).unwrap();
        bounded(self.task).await.unwrap().unwrap();
        assert!(
            self.source
                .append(envelope("after-stop", b"{}"))
                .await
                .is_err()
        );
    }
}

/// PASS: durable appends complete with readers held; all slots dial only after release and
/// closing trigger intake still leaves the source sink available until its sink-phase shutdown.
#[tokio::test(start_paused = true)]
async fn coordinator_serves_before_reader_start_and_through_producer_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = IngestOwner::start(&dir.path().join("source.log"), true);
    let receipt = bounded(owner.source.append(envelope("boot-admission", b"{}")))
        .await
        .unwrap();
    assert_eq!(owner.captured_source(receipt), "boot-admission");
    owner.assert_readers_held();
    owner.release_readers().await;
    owner.triggers.close();
    let receipt = bounded(owner.source.append(envelope("draining-admission", b"{}")))
        .await
        .unwrap();
    assert_eq!(owner.captured_source(receipt), "draining-admission");
    owner.stop().await;
}

/// PASS: losing the start sender exits the pool with its typed channel-closed cause, joins the
/// coordinator, rejects later appends, and never calls the dialer.
#[tokio::test(start_paused = true)]
async fn closed_reader_start_gate_ends_owner_without_dialing() {
    let dir = tempfile::tempdir().unwrap();
    let owner = IngestOwner::start(&dir.path().join("source.log"), true);
    bounded(owner.source.append(envelope("boot-admission", b"{}")))
        .await
        .unwrap();
    drop(owner.start);
    assert!(matches!(
        bounded(owner.task).await.unwrap(),
        Err(ActivityIngestError::ProducerStartClosed)
    ));
    assert_eq!(owner.dial_count.load(Ordering::SeqCst), 0);
    assert!(
        owner
            .source
            .append(envelope("after-close", b"{}"))
            .await
            .is_err()
    );
}

/// PASS: sink-phase shutdown joins readers still waiting for boot recovery without dialing.
#[tokio::test(start_paused = true)]
async fn shutdown_joins_readers_waiting_for_start() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = IngestOwner::start(&dir.path().join("source.log"), true);
    bounded(owner.source.append(envelope("boot-admission", b"{}")))
        .await
        .unwrap();
    owner.assert_readers_held();
    let dial_count = owner.dial_count.clone();
    owner.stop().await;
    assert_eq!(dial_count.load(Ordering::SeqCst), 0);
}

/// PASS: poll-only construction serves source appends immediately without retaining a start gate.
#[tokio::test(start_paused = true)]
async fn poll_only_coordinator_needs_no_producer_gate() {
    let dir = tempfile::tempdir().unwrap();
    let owner = IngestOwner::start(&dir.path().join("source.log"), false);
    assert_eq!(owner.start.receiver_count(), 0);
    let receipt = bounded(owner.source.append(envelope("poll-only", b"{}")))
        .await
        .unwrap();
    assert_eq!(owner.captured_source(receipt), "poll-only");
    assert_eq!(owner.dial_count.load(Ordering::SeqCst), 0);
    owner.stop().await;
}

#[derive(Clone)]
struct NoFinancialMutation;
impl SupabaseStateTrait for NoFinancialMutation {
    async fn commit_fill_v2(
        &self,
        _: &SupabaseFillRow,
    ) -> Result<FillV2Outcome, SupabaseStateError> {
        panic!("boot fixture called legacy fill authority")
    }
    async fn commit_prepared_fill(
        &self,
        _: &PreparedFillRequest,
    ) -> Result<CanonicalFillResult, SupabaseStateError> {
        panic!("expired boot continuation called financial authority")
    }
    async fn apply_prepared_resolution(
        &self,
        _: &PreparedResolutionRequest,
    ) -> Result<CanonicalResolutionResult, SupabaseStateError> {
        panic!("boot fixture called resolution authority")
    }
}

struct RestartFixture {
    dir: tempfile::TempDir,
    config: RuntimeConfig,
    admission: LiveAdmissionArtifact,
    books: Arc<FixtureClobBookFetcher>,
    id: SourceTradeId,
}

impl RestartFixture {
    async fn staged() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        paper
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: wallet(),
                complete: true,
                proof_json: "{}".to_owned(),
                updated_at_unix: EPOCH - 1,
            })
            .unwrap();
        support::install_verified_empty_anchor(&paper, wallet(), 0);
        let mut config =
            RuntimeConfig::from_service_config(&pe_service::config::ServiceConfig::default());
        config.mode = "paper".to_owned();
        config.min_resolution_horizon_secs = 0;
        config.max_resolution_horizon_secs = 0;
        config.price_impact_cap_bps = 300;
        config.per_trade_cap = PerTradeCap::Bps(1000);
        config.slippage_rate = Decimal::ZERO;
        config.sizing_mode = SizingMode::Contract { contracts: 5 };
        config.sizing_contracts = 5;
        let live_path = dir.path().join("live.log");
        drop(pe_execution_core::LiveJournal::open(&live_path).unwrap());
        let empty = TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        };
        let start = QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(CASH).unwrap(),
            paper_prefix: empty.clone(),
            source_prefix: empty,
            live_prefix: TailBinding::from(
                &pe_execution_core::LiveJournal::verified_tail(&live_path).unwrap(),
            ),
            artifact_blake3: "fixture".to_owned(),
            static_config_hash: "fixture".to_owned(),
            hot_config_hash: config.canonical_hash(),
            generation: "boot-order".to_owned(),
            activation_id: "boot-order".to_owned(),
            ranking_batch_id: 588,
            membership: vec![wallet()],
            membership_proofs_hash: pe_service::qualification::scenario_membership_proofs_hash(
                &paper,
                &[wallet()],
            )
            .unwrap(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: 1,
        };
        let mut paper_writer = Writer::open(dir.path().join("paper.log")).unwrap();
        let mut start_envelope = envelope(
            "pe-service.paper",
            &serde_json::to_vec(&PaperLogRecord::QualificationStarted(Box::new(start))).unwrap(),
        );
        start_envelope.schema_version = 2;
        let start_receipt = paper_writer.append_synced(start_envelope).unwrap();
        drop(paper_writer);
        paper
            .reset_financial_era(
                start_receipt,
                CollateralAmount::from_decimal_exact(CASH).unwrap(),
            )
            .unwrap();
        let mut writer = Writer::open(dir.path().join("source.log")).unwrap();
        let receipts = AdmissionReceipts {
            gamma: writer
                .append_synced(envelope("polymarket.gamma.markets", GAMMA))
                .unwrap(),
            clob_long: writer
                .append_synced(envelope("polymarket.clob.markets", LONG))
                .unwrap(),
            clob_compact: writer
                .append_synced(envelope("polymarket.clob.compact-market", COMPACT))
                .unwrap(),
        };
        let book_body = include_bytes!("fixtures/golden_stream_v1/book.json");
        let book_receipt = writer
            .append_synced(envelope("polymarket.clob.book", book_body))
            .unwrap();
        let (read, commitment) = support::append_committed_read_v2(
            &mut writer,
            wallet(),
            include_bytes!("fixtures/golden_stream_v1/activity_page.json"),
            EPOCH,
            EPOCH,
        );
        drop(writer);
        let id = read.aggregates[0].group_id.key().clone();
        let mut context = support::read_context(&read, commitment, EPOCH);
        context.applied_configuration = config.clone();
        let mut engine =
            BucketCommitEngine::load(paper.clone(), build_leader_ledger(&paper).unwrap()).unwrap();
        let result = engine
            .commit_with_freshness_policy(
                read.aggregates,
                &context,
                FrozenDecisionBasis {
                    win_rate_p: Probability::new(dec!(0.7)).unwrap(),
                    bankroll: CASH,
                },
                Some(PaperFreshnessPolicy {
                    activity_ws_enabled: true,
                    copy_latency_budget_secs: 2,
                }),
            )
            .unwrap();
        assert_eq!(result.pending, vec![id.clone()]);
        drop(engine);
        let condition = PolymarketConditionId(
            serde_json::from_slice::<Value>(LONG).unwrap()["condition_id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
        let market = validate_live_market(GAMMA, LONG, &condition, EPOCH, 60).unwrap();
        let compact =
            parse_compact_market(COMPACT, &condition, &market.ordered_outcome_token_ids).unwrap();
        let admission = LiveAdmissionArtifact {
            market,
            settlement: VenueSettlementRecord {
                schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                condition_id: condition,
                status: VenueResolutionStatus::Unresolved,
                raw_evidence_hash: blake3::hash(LONG).to_hex().to_string(),
                source_timestamp_unix: None,
                observed_at_unix: EPOCH,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            fee_schedule: compact.fee_schedule,
            receipts,
        };
        let mut book = OrderBook::from_book_json(book_body).unwrap();
        book.source_receipt = Some(book_receipt);
        book.fetched_at_ms = u64::try_from(EPOCH * 1000).unwrap();
        let fixture = Self {
            dir,
            config,
            admission,
            books: Arc::new(FixtureClobBookFetcher::new(HashMap::from([(
                "11".to_owned(),
                book,
            )]))),
            id,
        };
        let owner = IngestOwner::start(&fixture.dir.path().join("source.log"), false);
        let hooks = fixture.hooks(at());
        hooks
            .admission_artifacts
            .lock()
            .unwrap()
            .push_back(fixture.admission.clone());
        hooks
            .fail_next_prepared_append
            .store(true, Ordering::SeqCst);
        let mut orch = fixture.orchestrator(
            paper.clone(),
            &owner,
            hooks.clone(),
            true,
            "http://unused.invalid",
        );
        assert!(
            bounded(orch.resume_pending_before_producers())
                .await
                .is_err(),
            "expected pre-Prepared interruption, decision: {:?}",
            paper.decision_pending_for(&fixture.id).unwrap()
        );
        assert!(
            !hooks.fail_next_prepared_append.load(Ordering::SeqCst),
            "reached pre-Prepared crash point"
        );
        drop(orch);
        let row = paper.decision_pending_for(&fixture.id).unwrap().unwrap();
        assert_eq!(row.state, DecisionPendingState::Open);
        assert_eq!(
            DecisionContinuationV3::from_durable(&row)
                .unwrap()
                .version(),
            5
        );
        let checkpoint: Value = serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        assert_eq!(
            checkpoint["dispatch_id"],
            paper.pending_dispatch_seeds().unwrap()[0].dispatch_id
        );
        fixture.assert_no_prepared(&paper);
        owner.stop().await;
        drop(paper);
        fixture
    }

    fn hooks(&self, final_at: OffsetDateTime) -> Arc<ScenarioHooks> {
        let hooks = Arc::new(ScenarioHooks::default());
        hooks.financial_clock_unix.store(EPOCH, Ordering::SeqCst);
        hooks
            .age_clock
            .lock()
            .unwrap()
            .extend([at(), at(), final_at]);
        hooks
    }

    fn orchestrator(
        &self,
        paper: Arc<PaperStateDb>,
        ingest: &IngestOwner,
        hooks: Arc<ScenarioHooks>,
        arm: bool,
        base: &str,
    ) -> Orchestrator<support::Prices, FixtureClobBookFetcher, NoFinancialMutation> {
        let live = LiveAccounts::new(if arm {
            LiveAccountsSnapshot {
                accounts: vec![AccountContext {
                    account_id: AccountId::new("stored-target").unwrap(),
                    is_primary: true,
                    enabled: true,
                    execution_order: 0,
                    requested_live_mode: "live_tiny".to_owned(),
                    effective_live_mode: "live_tiny".to_owned(),
                    live_price_impact_cap_bps: 100,
                    custody_wallet_address: None,
                    custody_wallet_kind: None,
                    credential_binding: Some((7, "stored-key".to_owned())),
                }],
                fetched_at_unix: Some(EPOCH),
            }
        } else {
            LiveAccountsSnapshot::default()
        });
        let (_control, control_rx) = mpsc::channel(1);
        let mut gamma: Value = serde_json::from_slice(GAMMA).unwrap();
        gamma[0]["outcomePrices"] = "[\"0.50\",\"0.50\"]".into();
        let prices = support::Prices {
            gate: Arc::new(support::PriceGate::default()),
            markets: Arc::new(Mutex::new(HashMap::from([(
                self.admission.market.condition_id.0.clone(),
                gamma[0].take(),
            )]))),
        };
        let mut orch = Orchestrator::new_with_authority(
            watchlist(),
            OrchestratorConfig {
                bankroll: CASH,
                mode: ExecutionMode::Paper,
                signal_config: SignalConfig::default(),
                max_resolution_horizon_secs: 0,
                min_resolution_horizon_secs: 0,
                max_fill_price: self.config.max_fill_price,
                min_fill_price: self.config.min_fill_price,
                price_impact_cap_bps: 300,
                entry_gate_config: CopyEntryGateConfig,
                runtime_config: None,
                live_accounts: Some(live),
                activity_ws_enabled: true,
                copy_latency_budget_secs: 2,
                watchlist_writer_lock: None,
            },
            WinnerFollowStrategy::new(self.config.winner_follow_config()),
            Writer::open(self.dir.path().join("paper.log")).unwrap(),
            paper.clone(),
            build_leader_ledger(&paper).unwrap(),
            ingest.health.clone(),
            MidPriceCache::with_fetcher(prices, "fixture://gamma".to_owned())
                .with_source_log(ingest.source.clone())
                .with_clock(Arc::new(at)),
            control_rx,
            None,
            NoFinancialMutation,
            self.books.clone(),
        )
        .unwrap();
        orch.set_scenario_hooks(hooks);
        orch.configure_financial_log_paths(
            self.dir.path().join("paper.log"),
            self.dir.path().join("source.log"),
            LiveAdmissionBuilder::new(reqwest::Client::new(), base, base, ingest.source.clone())
                .with_clock(Arc::new(at)),
            Arc::new(HistoricalMarkAdapter::new(
                reqwest::Client::new(),
                "http://unused.invalid",
                ingest.source.clone(),
            )),
            ingest.index.clone(),
        )
        .unwrap();
        orch
    }

    fn assert_no_prepared(&self, paper: &PaperStateDb) {
        assert_eq!(paper.financial_last_prepared_seq().unwrap(), None);
        assert_eq!(paper.fills_count().unwrap(), 0);
        assert_eq!(paper.bankroll().unwrap(), Some(CASH));
        assert!(paper.open_positions().unwrap().is_empty());
        assert!(
            scan_paper_log(&self.dir.path().join("paper.log"))
                .unwrap()
                .iter()
                .all(|frame| !matches!(
                    frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { .. })
                ))
        );
    }

    fn assert_expired(&self, paper: &PaperStateDb) {
        let row = paper.decision_pending_for(&self.id).unwrap().unwrap();
        assert_eq!(row.state, DecisionPendingState::Terminal);
        let replay = replay_decision_pending(&row).unwrap();
        assert_eq!(
            replay.post_boundary.body.terminal.reason,
            "paper_stale_before_prepared"
        );
        let dispatch_id = replay.post_boundary.body.terminal.dispatch_id.unwrap();
        let seed = paper.dispatch_seed(&dispatch_id).unwrap().unwrap();
        assert_eq!(seed.state, "ready");
        assert_eq!(
            seed.paper_outcome.as_deref(),
            Some("no_fill:paper_stale_before_prepared")
        );
        self.assert_no_prepared(paper);
    }
}

/// PASS: the socket-free fixture stages through production, restarts with an open generation-five
/// decision and no Prepared, and records final paper expiry while websocket readers remain held.
#[tokio::test(start_paused = true)]
async fn staged_restart_fixture_expires_before_readers_start() {
    let fixture = RestartFixture::staged().await;
    let paper = Arc::new(PaperStateDb::open(&fixture.dir.path().join("paper.db")).unwrap());
    let mut ingest = IngestOwner::start(&fixture.dir.path().join("source.log"), true);
    assert_eq!(
        validate_open_continuations(&paper, &ingest.index).unwrap(),
        1
    );
    let hooks = fixture.hooks(at() + time::Duration::seconds(3));
    hooks
        .admission_artifacts
        .lock()
        .unwrap()
        .push_back(fixture.admission.clone());
    let mut orch = fixture.orchestrator(
        paper.clone(),
        &ingest,
        hooks,
        false,
        "http://unused.invalid",
    );
    bounded(SCENARIO_TERMINAL_CLOCK.scope(
        at() + time::Duration::seconds(3),
        orch.resume_pending_before_producers(),
    ))
    .await
    .unwrap();
    fixture.assert_expired(&paper);
    ingest.assert_readers_held();
    ingest.release_readers().await;
    drop(orch);
    ingest.stop().await;
}

async fn admission_response(
    axum::extract::State(fetcher): axum::extract::State<Arc<support::GatedFetcher>>,
    uri: axum::http::Uri,
) -> Vec<u8> {
    fetcher.fetch(&uri.to_string()).await.unwrap()
}

/// PASS: real admission GETs and their durable source-log acknowledgements finish during boot
/// recovery, terminalizing the staged decision before any websocket dial; poll-only boot also
/// completes. Request channels and joined recovery tasks establish ordering, with timeout bounds.
#[tokio::test]
async fn staged_recovery_records_admission_before_observation_producers_start() {
    for readers in [true, false] {
        let fixture = RestartFixture::staged().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (requests, mut requested) = mpsc::channel(3);
        let router = axum::Router::new()
            .fallback(axum::routing::get(admission_response))
            .with_state(Arc::new(support::GatedFetcher { requests }));
        let (stop_server, server_stopped) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = server_stopped.await;
                })
                .await
                .unwrap();
        });
        let paper = Arc::new(PaperStateDb::open(&fixture.dir.path().join("paper.db")).unwrap());
        let mut ingest = IngestOwner::start(&fixture.dir.path().join("source.log"), readers);
        assert_eq!(
            validate_open_continuations(&paper, &ingest.index).unwrap(),
            1
        );
        let before = ingest.captures().len();
        let hooks = fixture.hooks(at() + time::Duration::seconds(3));
        assert!(hooks.admission_artifacts.lock().unwrap().is_empty());
        let mut orch = fixture.orchestrator(paper.clone(), &ingest, hooks, false, &base);
        let recovery = tokio::spawn(
            SCENARIO_TERMINAL_CLOCK.scope(at() + time::Duration::seconds(3), async move {
                orch.resume_pending_before_producers().await
            }),
        );
        let mut responses = Vec::new();
        for _ in 0..3 {
            responses.push(bounded(requested.recv()).await.unwrap());
        }
        ingest.assert_readers_held();
        assert!(!recovery.is_finished());
        let mut sources = BTreeSet::new();
        for request in responses {
            let (source, body) = if request.url.starts_with("/markets?") {
                ("polymarket.gamma.markets", GAMMA)
            } else if request.url.starts_with("/markets/") {
                ("polymarket.clob.markets", LONG)
            } else {
                assert!(request.url.starts_with("/clob-markets/"));
                ("polymarket.clob.compact-market", COMPACT)
            };
            assert!(sources.insert(source));
            request.respond.send(body.to_vec()).unwrap();
        }
        bounded(recovery).await.unwrap().unwrap();
        fixture.assert_expired(&paper);
        ingest.assert_readers_held();
        let after = ingest.captures();
        for (source, body) in [
            ("polymarket.gamma.markets", GAMMA),
            ("polymarket.clob.markets", LONG),
            ("polymarket.clob.compact-market", COMPACT),
        ] {
            assert!(sources.remove(source));
            assert_eq!(
                after
                    .iter()
                    .skip(before)
                    .filter(|capture| { capture.source_id.0 == source && capture.payload == body })
                    .count(),
                1,
                "admission response captured exactly once: {source}"
            );
        }
        assert!(sources.is_empty());
        if readers {
            ingest.release_readers().await;
        } else {
            assert_eq!(ingest.start.receiver_count(), 0);
        }
        ingest.stop().await;
        stop_server.send(()).unwrap();
        bounded(server).await.unwrap();
    }
}
