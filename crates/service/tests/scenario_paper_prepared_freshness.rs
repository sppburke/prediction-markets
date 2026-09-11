//! Continuation-five freshness through the poller, source coordinator, and paper owner.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pe_copy_signal_engine::SignalConfig;
use pe_core_types::{
    AccountId, BasisPoints, CollateralAmount, EventSeq, PolymarketConditionId, ReceivedAt,
    ReconstructionQuality, SourceId, SourceTimestamp, SourceTradeId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Writer};
use pe_execution_core::{AdmissionReceipts, LiveAdmissionArtifact};
use pe_paper_state::{
    DecisionPendingRow, DecisionPendingState, PaperStateDb, WalletHistoryStatusRecord,
};
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
use pe_service::asset_identity::AssetIdentityResolver;
use pe_service::bucket_commit::{BucketCommitEngine, FrozenDecisionBasis, PaperFreshnessPolicy};
use pe_service::clob_book::{ClobBookError, ClobBookFetcher, OrderBook};
use pe_service::decision_replay::{
    DecisionClockEvidence, DecisionPostBoundaryEvidence, replay_decision_pending,
};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_accounts::{AccountContext, LiveAccounts, LiveAccountsSnapshot};
use pe_service::live_venue_adapter::LiveAdmissionBuilder;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mark_prices::HistoricalMarkAdapter;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig, ScenarioHooks};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{
    CanonicalFillResult, PaperLogFrame, PaperLogRecord, QualificationStarted, TailBinding,
    build_leader_ledger, scan_paper_log,
};
use pe_service::risk_inputs::SourceReceiptIndex;
use pe_service::runtime_config::{LiveRuntimeConfig, RuntimeConfig};
use pe_service::source_event_sink::SourceEventSink;
use pe_service::supabase_sink::SupabaseFillRow;
use pe_service::supabase_state::{
    FillV2Outcome, PreparedFillRequest, SourceEvidence, SupabaseStateError, SupabaseStateTrait,
    reconcile_active_financial_frames,
};
use pe_service::trade_poller::{ReconciliationObligations, TradePoller, TradePollerConfig};
use pe_source_core::SourceError;
use pe_source_polymarket_public::{
    GAMMA_BATCH_SIZE, PageFetcher, ReconciliationFetcher, validate_live_market,
};
use pe_strategy_winner_follow::{ExecutionMode, PerTradeCap, SizingMode, WinnerFollowStrategy};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::parse_compact_market;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::sync::{Notify, mpsc};

const EPOCH: i64 = 1_800_000_000;
const CASH: Decimal = dec!(1000);
const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn at() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(EPOCH).unwrap()
}
fn wallet() -> WalletAddress {
    WalletAddress::from_hex(WALLET).unwrap()
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
fn runtime() -> RuntimeConfig {
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
    config
}

#[derive(Default)]
struct PriceGate {
    blocked: AtomicBool,
    market: Mutex<Option<String>>,
    started: Notify,
    release: Notify,
}
#[derive(Clone)]
struct Prices {
    gate: Arc<PriceGate>,
    markets: Arc<Mutex<HashMap<String, Value>>>,
}
impl PageFetcher for Prices {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, SourceError> {
        if self
            .gate
            .market
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|market| url.contains(&format!("condition_ids={market}")))
            && self.gate.blocked.swap(false, Ordering::SeqCst)
        {
            self.gate.started.notify_one();
            self.gate.release.notified().await;
        }
        let markets = self.markets.lock().unwrap();
        let rows = url
            .split(['?', '&'])
            .filter_map(|part| part.strip_prefix("condition_ids="))
            .filter_map(|condition| markets.get(condition).cloned())
            .collect::<Vec<_>>();
        Ok(serde_json::to_vec(&rows).unwrap())
    }
}
struct Page(Vec<u8>);
impl ReconciliationFetcher for Page {
    fn fetch<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}
#[derive(Default)]
struct Books(Mutex<HashMap<String, OrderBook>>);
impl ClobBookFetcher for Books {
    async fn fetch_book(&self, _: &str, token: &str) -> Result<OrderBook, ClobBookError> {
        self.0
            .lock()
            .unwrap()
            .get(token)
            .cloned()
            .ok_or_else(|| ClobBookError::MissingFixture(token.to_owned()))
    }
}
#[derive(Clone)]
struct Authority {
    inner: Arc<Mutex<AuthorityState>>,
    fail: Arc<AtomicBool>,
}
struct AuthorityState {
    start: AppendReceipt,
    prior: Option<EventSeq>,
    cash: Decimal,
    fills: Vec<(PreparedFillRequest, CanonicalFillResult)>,
}
impl SupabaseStateTrait for Authority {
    async fn commit_fill_v2(
        &self,
        _: &SupabaseFillRow,
    ) -> Result<FillV2Outcome, SupabaseStateError> {
        panic!("active era used legacy authority")
    }
    async fn apply_prepared_resolution(
        &self,
        _: &pe_service::supabase_state::PreparedResolutionRequest,
    ) -> Result<pe_service::paper_recovery::CanonicalResolutionResult, SupabaseStateError> {
        panic!("freshness scenario requested a resolution")
    }
    async fn commit_prepared_fill(
        &self,
        request: &PreparedFillRequest,
    ) -> Result<CanonicalFillResult, SupabaseStateError> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(SupabaseStateError::Corrupt(
                "injected authority crash".to_owned(),
            ));
        }
        let mut state = self.inner.lock().unwrap();
        assert_eq!(
            state.start,
            request.expected_authority.qualification_start_receipt
        );
        if let Some((stored, result)) = state
            .fills
            .iter()
            .find(|(stored, _)| stored.idempotency_key == request.idempotency_key)
        {
            assert_eq!(stored, request);
            let mut result = result.clone();
            result.outcome = "existing".to_owned();
            return Ok(result);
        }
        assert_eq!(
            state.prior,
            request.expected_authority.prior_completed_prepared_sequence
        );
        state.cash -= request
            .principal
            .checked_add(request.fee)
            .unwrap()
            .to_decimal();
        let result = CanonicalFillResult {
            outcome: "applied".to_owned(),
            bankroll: state.cash,
            applied_prepared_seq: request.prepared_receipt.sequence,
            quantity: request.quantity,
            principal: request.principal,
            fee: request.fee,
            fill_price: request.fill_price,
        };
        state.prior = Some(request.prepared_receipt.sequence);
        state.fills.push((request.clone(), result.clone()));
        Ok(result)
    }
}

struct Recorded {
    epoch: i64,
    activity: Vec<u8>,
    gamma: Vec<u8>,
    id: SourceTradeId,
    admission: LiveAdmissionArtifact,
}
struct Harness {
    config: RuntimeConfig,
    probability: pe_core_types::Probability,
    dir: tempfile::TempDir,
    paper: Arc<PaperStateDb>,
    source: SourceLogHandle,
    index: SourceReceiptIndex,
    hooks: Arc<ScenarioHooks>,
    books: Arc<Books>,
    prices: Prices,
    authority: Authority,
    live: LiveAccounts,
    control: Option<mpsc::Sender<OrchestratorControl>>,
    task:
        Option<tokio::task::JoinHandle<Result<(), pe_service::orchestrator::OrchestratorRunError>>>,
    coordinator: tokio::task::JoinHandle<()>,
    _trigger_rx: mpsc::Receiver<pe_service::activity_ingest::ReconciliationTrigger>,
}
impl Harness {
    async fn new() -> Self {
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
        support::install_empty_anchor(&paper, wallet(), 0);
        let empty = TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        };
        let record = PaperLogRecord::QualificationStarted(Box::new(QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(CASH).unwrap(),
            paper_prefix: empty.clone(),
            source_prefix: empty.clone(),
            live_prefix: empty,
            artifact_blake3: "fixture".to_owned(),
            static_config_hash: "fixture".to_owned(),
            hot_config_hash: runtime().canonical_hash(),
            generation: "prepared-freshness".to_owned(),
            activation_id: "prepared-freshness".to_owned(),
            ranking_batch_id: 588,
            membership: vec![wallet()],
            membership_proofs_hash: "fixture".to_owned(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version: 1,
        }));
        let mut writer = Writer::open(dir.path().join("paper.log")).unwrap();
        let start = writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: 2,
                parser_version: 1,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&record).unwrap(),
            })
            .unwrap();
        drop(writer);
        paper
            .reset_financial_era(start, CollateralAmount::from_decimal_exact(CASH).unwrap())
            .unwrap();
        let sink = SourceEventSink::open(dir.path().join("source.log")).unwrap();
        let index = SourceReceiptIndex::replay(&dir.path().join("source.log")).unwrap();
        let (source, receiver) = SourceLogHandle::channel(8);
        let (trigger, trigger_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(
            ActivityIngest::poll_only(
                sink,
                receiver,
                trigger,
                new_shared_health_with_ws(false, true, 90),
            )
            .with_source_receipt_index(index.clone())
            .run(),
        );
        let hooks = Arc::new(ScenarioHooks::default());
        hooks.financial_clock_unix.store(EPOCH, Ordering::SeqCst);
        Self {
            config: runtime(),
            probability: pe_core_types::Probability::new(dec!(0.7)).unwrap(),
            dir,
            paper,
            source,
            index,
            hooks,
            books: Arc::new(Books::default()),
            prices: Prices {
                gate: Arc::new(PriceGate::default()),
                markets: Arc::new(Mutex::new(HashMap::new())),
            },
            authority: Authority {
                inner: Arc::new(Mutex::new(AuthorityState {
                    start,
                    prior: None,
                    cash: CASH,
                    fills: Vec::new(),
                })),
                fail: Arc::new(AtomicBool::new(false)),
            },
            live: LiveAccounts::new(LiveAccountsSnapshot::default()),
            control: None,
            task: None,
            coordinator,
            _trigger_rx: trigger_rx,
        }
    }
    async fn append(&self, source: &str, payload: &[u8]) -> AppendReceipt {
        self.source
            .append(EnvelopeIn {
                source_id: SourceId(source.to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: payload.to_vec(),
            })
            .await
            .unwrap()
    }
    async fn record(&self, ordinal: u32) -> Recorded {
        let condition = format!("0x{ordinal:064x}");
        let token = (ordinal * 2 + 11).to_string();
        let other = (ordinal * 2 + 12).to_string();
        let mut activity: Value = serde_json::from_slice(include_bytes!(
            "fixtures/golden_stream_v1/activity_page.json"
        ))
        .unwrap();
        let epoch = EPOCH + i64::from(ordinal) - 1;
        activity[0]["timestamp"] = epoch.into();
        activity[0]["conditionId"] = condition.clone().into();
        activity[0]["asset"] = token.clone().into();
        activity[0]["transactionHash"] = format!("0x{:064x}", ordinal + 100).into();
        let mut gamma: Value =
            serde_json::from_slice(include_bytes!("fixtures/golden_stream_v1/gamma_long.json"))
                .unwrap();
        gamma[0]["conditionId"] = condition.clone().into();
        gamma[0]["clobTokenIds"] = json!([token, other]).to_string().into();
        gamma[0]["outcomePrices"] = "[\"0.50\",\"0.50\"]".into();
        self.prices
            .markets
            .lock()
            .unwrap()
            .insert(condition.clone(), gamma[0].clone());
        let mut long: Value =
            serde_json::from_slice(include_bytes!("fixtures/golden_stream_v1/clob_long.json"))
                .unwrap();
        long["condition_id"] = condition.clone().into();
        long["tokens"][0]["token_id"] = token.clone().into();
        long["tokens"][1]["token_id"] = other.clone().into();
        let mut compact: Value = serde_json::from_slice(include_bytes!(
            "fixtures/golden_stream_v1/clob_compact.json"
        ))
        .unwrap();
        compact["c"] = condition.clone().into();
        compact["t"][0]["t"] = token.clone().into();
        compact["t"][1]["t"] = other.into();
        let mut book: Value =
            serde_json::from_slice(include_bytes!("fixtures/golden_stream_v1/book.json")).unwrap();
        book["market"] = condition.clone().into();
        book["asset_id"] = token.clone().into();
        let activity = serde_json::to_vec(&activity).unwrap();
        let gamma = serde_json::to_vec(&gamma).unwrap();
        let long = serde_json::to_vec(&long).unwrap();
        let compact = serde_json::to_vec(&compact).unwrap();
        let book = serde_json::to_vec(&book).unwrap();
        let receipts = AdmissionReceipts {
            gamma: self.append("polymarket.gamma.markets", &gamma).await,
            clob_long: self.append("polymarket.clob.markets", &long).await,
            clob_compact: self
                .append("polymarket.clob.compact-market", &compact)
                .await,
        };
        let book_receipt = self.append("polymarket.clob.book", &book).await;
        let condition = PolymarketConditionId(condition);
        let market = validate_live_market(&gamma, &long, &condition, EPOCH, 60).unwrap();
        let fee_schedule =
            parse_compact_market(&compact, &condition, &market.ordered_outcome_token_ids)
                .unwrap()
                .fee_schedule;
        let admission = LiveAdmissionArtifact {
            market,
            fee_schedule,
            receipts,
            settlement: VenueSettlementRecord {
                schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                condition_id: condition,
                status: VenueResolutionStatus::Unresolved,
                raw_evidence_hash: blake3::hash(&long).to_hex().to_string(),
                source_timestamp_unix: None,
                observed_at_unix: EPOCH,
                parser_version: 1,
                freshness_window_secs: 60,
            },
        };
        let mut parsed = OrderBook::from_book_json(&book).unwrap();
        parsed.source_receipt = Some(book_receipt);
        parsed.fetched_at_ms = u64::try_from(EPOCH * 1000).unwrap();
        self.books.0.lock().unwrap().insert(token, parsed);
        let read = support::producer_shaped_read_v2(
            wallet(),
            &activity,
            epoch,
            epoch,
            support::scenario_receipt(1),
        );
        Recorded {
            epoch,
            id: read.aggregates[0].group_id.key().clone(),
            activity,
            gamma,
            admission,
        }
    }
    fn start(&mut self, enabled: bool) {
        let (control, receiver) = mpsc::channel(4);
        let mut owner = Orchestrator::new_with_authority(
            watchlist(),
            OrchestratorConfig {
                bankroll: self.paper.bankroll().unwrap().unwrap_or(CASH),
                mode: ExecutionMode::Paper,
                signal_config: SignalConfig::default(),
                max_resolution_horizon_secs: 0,
                min_resolution_horizon_secs: 0,
                max_fill_price: dec!(0.85),
                min_fill_price: dec!(0.15),
                price_impact_cap_bps: 300,
                entry_gate_config: CopyEntryGateConfig,
                runtime_config: None,
                live_accounts: Some(self.live.clone()),
                activity_ws_enabled: enabled,
                copy_latency_budget_secs: 2,
                watchlist_writer_lock: None,
            },
            WinnerFollowStrategy::new(self.config.winner_follow_config()),
            Writer::open(self.dir.path().join("paper.log")).unwrap(),
            self.paper.clone(),
            build_leader_ledger(&self.paper).unwrap(),
            new_shared_health_with_ws(false, true, 90),
            MidPriceCache::with_fetcher(self.prices.clone(), "fixture://gamma".to_owned())
                .with_source_log(self.source.clone())
                .with_clock(Arc::new(at)),
            receiver,
            None,
            self.authority.clone(),
            self.books.clone(),
        )
        .unwrap();
        owner.set_scenario_hooks(self.hooks.clone());
        owner
            .configure_financial_log_paths(
                self.dir.path().join("paper.log"),
                self.dir.path().join("source.log"),
                LiveAdmissionBuilder::new(
                    reqwest::Client::new(),
                    "http://unused.invalid",
                    "http://unused.invalid",
                    self.source.clone(),
                ),
                Arc::new(HistoricalMarkAdapter::new(
                    reqwest::Client::new(),
                    "http://unused.invalid",
                    self.source.clone(),
                )),
                self.index.clone(),
            )
            .unwrap();
        self.control = Some(control);
        self.task = Some(tokio::spawn(
            owner.run_coordinated(std::future::pending::<()>()),
        ));
    }
    fn attempt(&self, recorded: &Recorded, final_at: OffsetDateTime) {
        self.hooks.age_clock.lock().unwrap().extend([
            OffsetDateTime::from_unix_timestamp(recorded.epoch).unwrap(),
            OffsetDateTime::from_unix_timestamp(recorded.epoch).unwrap(),
            final_at,
        ]);
        self.hooks
            .admission_artifacts
            .lock()
            .unwrap()
            .push_back(recorded.admission.clone());
    }
    async fn poll(&self, recorded: &Recorded) {
        let now = OffsetDateTime::from_unix_timestamp(recorded.epoch).unwrap();
        let (_trigger, receiver) = mpsc::channel(1);
        let (progress, mut completed) = mpsc::channel(8);
        TradePoller::new(
            TradePollerConfig {
                base_url: "fixture://activity".to_owned(),
                poll_interval_secs: 30,
                activity_ws_enabled: false,
                copy_latency_budget_secs: 2,
            },
            watchlist(),
            Arc::new(Page(recorded.activity.clone())),
            Arc::new(AssetIdentityResolver::new_runtime(
                Arc::new(Page(recorded.gamma.clone())),
                "fixture://gamma".to_owned(),
                GAMMA_BATCH_SIZE,
                self.source.clone(),
            )),
            self.source.clone(),
            receiver,
            self.control.as_ref().unwrap().clone(),
            self.paper.clone(),
            new_shared_health_with_ws(false, true, 90),
            SignalConfig::default(),
            LiveRuntimeConfig::new(self.config.clone()),
            ReconciliationObligations::default(),
            None,
        )
        .with_source_receipt_index(self.index.clone())
        .with_progress(progress)
        .with_clock(Arc::new(move || now))
        .run_until(async move {
            while let Some(progress) = completed.recv().await {
                if matches!(
                    progress,
                    pe_service::trade_poller::PollerProgress::RoundCompleted
                ) {
                    break;
                }
            }
        })
        .await
        .unwrap();
    }
    fn terminal(&self, recorded: &Recorded) -> DecisionPendingRow {
        self.paper
            .decision_pending_for(&recorded.id)
            .unwrap()
            .unwrap()
    }
    fn prepared_count(&self) -> usize {
        scan_paper_log(&self.dir.path().join("paper.log"))
            .unwrap()
            .iter()
            .filter(|frame| {
                matches!(
                    frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { .. })
                )
            })
            .count()
    }
    async fn stop(&mut self) {
        self.control.take();
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
    fn arm(&self) {
        self.live.store(LiveAccountsSnapshot {
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
        });
    }

    async fn freeze(&self, recorded: &Recorded, enabled: bool) {
        let page = self
            .source
            .append(EnvelopeIn {
                source_id: SourceId(pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: pe_service::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
                parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: recorded.activity.clone(),
            })
            .await
            .unwrap();
        let read = support::producer_shaped_read_v2(
            wallet(),
            &recorded.activity,
            recorded.epoch,
            recorded.epoch,
            page,
        );
        let commitment = self
            .source
            .append(EnvelopeIn {
                source_id: SourceId(
                    pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned(),
                ),
                schema_version: pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
                parser_version: pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: read.commitment_payload.clone(),
            })
            .await
            .unwrap();
        let mut context = support::read_context(&read, commitment, recorded.epoch);
        context.applied_configuration = self.config.clone();
        let mut engine = BucketCommitEngine::load(
            self.paper.clone(),
            build_leader_ledger(&self.paper).unwrap(),
        )
        .unwrap();
        let result = engine
            .commit_with_freshness_policy(
                read.aggregates,
                &context,
                FrozenDecisionBasis {
                    win_rate_p: self.probability,
                    bankroll: CASH,
                },
                Some(PaperFreshnessPolicy {
                    activity_ws_enabled: enabled,
                    copy_latency_budget_secs: 2,
                }),
            )
            .unwrap();
        assert_eq!(result.pending, vec![recorded.id.clone()]);
    }

    async fn barrier(&self) {
        let (acknowledged, receiver) = tokio::sync::oneshot::channel();
        self.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::PrepareAdmissions {
                wallets: vec![wallet()],
                acknowledged,
            })
            .await
            .unwrap();
        receiver.await.unwrap();
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        self.coordinator.abort();
    }
}

fn assert_expired(h: &Harness, recorded: &Recorded) {
    let row = h.terminal(recorded);
    let replayed = replay_decision_pending(&row).unwrap();
    assert_eq!(row.state, DecisionPendingState::Terminal);
    assert_eq!(row.terminal_disposition.as_deref(), Some("no_fill"));
    assert_eq!(
        replayed.post_boundary.body.terminal.reason,
        "paper_stale_before_prepared"
    );
    assert!(h.paper.gate_history().unwrap().contains_key(&wallet()));
    assert!(
        h.paper
            .activity_group_state(&recorded.id)
            .unwrap()
            .is_some()
    );
}

/// PASS: a cold portfolio read can expire paper after staging, without undoing the entry or its live target.
#[tokio::test]
async fn cold_portfolio_prices_expire_before_new_prepared_and_release_live_targets() {
    let mut h = Harness::new().await;
    let held = h.record(1).await;
    h.attempt(&held, at());
    h.start(true);
    h.poll(&held).await;
    assert_eq!(h.prepared_count(), 1);
    assert_eq!(h.paper.open_positions().unwrap().len(), 1);
    let cash = h.paper.bankroll().unwrap();
    h.stop().await;
    let recorded = h.record(2).await;
    h.arm();
    h.start(true);
    *h.prices.gate.market.lock().unwrap() = Some(held.admission.market.condition_id.0.clone());
    h.attempt(
        &recorded,
        at() + time::Duration::seconds(3) + time::Duration::nanoseconds(1),
    );
    h.prices.gate.blocked.store(true, Ordering::SeqCst);
    let gate = h.prices.gate.clone();
    let pending = h.poll(&recorded);
    tokio::pin!(pending);
    tokio::select! { biased; _ = gate.started.notified() => {}, _ = &mut pending => panic!("copy completed before cold-price gate") }
    let staged = h.paper.pending_dispatch_seeds().unwrap();
    assert_eq!(staged.len(), 1, "{:?}", h.terminal(&recorded));
    let open = h.terminal(&recorded);
    assert_eq!(open.state, DecisionPendingState::Open);
    let checkpoint: Value = serde_json::from_str(&open.post_commit_inputs_json).unwrap();
    assert_eq!(checkpoint["dispatch_id"], staged[0].dispatch_id);
    assert!(checkpoint.get("terminal").is_none());
    let targets = h.paper.dispatch_targets(&staged[0].dispatch_id).unwrap();
    assert_eq!(targets.len(), 1);
    gate.release.notify_one();
    pending.await;
    assert_expired(&h, &recorded);
    assert_eq!(h.prepared_count(), 1);
    assert_eq!(h.authority.inner.lock().unwrap().fills.len(), 1);
    assert_eq!(h.paper.bankroll().unwrap(), cash);
    assert_eq!(h.paper.open_positions().unwrap().len(), 1);
    let ready = h
        .paper
        .dispatch_seed(&staged[0].dispatch_id)
        .unwrap()
        .unwrap();
    assert_eq!(ready.state, "ready");
    assert_eq!(
        ready.paper_outcome.as_deref(),
        Some("no_fill:paper_stale_before_prepared")
    );
    assert_eq!(
        h.paper.dispatch_targets(&staged[0].dispatch_id).unwrap(),
        targets
    );
}

/// PASS: exactly two seconds fills; one nanosecond later expires, with the predicate's exact clock.
#[tokio::test]
async fn paper_prepared_gate_preserves_nanosecond_boundary() {
    for remainder in [0, 1] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        let gate = at() + time::Duration::seconds(2) + time::Duration::nanoseconds(remainder);
        h.attempt(&recorded, gate);
        h.start(true);
        h.poll(&recorded).await;
        let replayed = replay_decision_pending(&h.terminal(&recorded)).unwrap();
        let clocks = replayed
            .post_boundary
            .body
            .clocks
            .iter()
            .filter(|clock| clock.purpose == "paper_prepared_staleness_gate")
            .collect::<Vec<_>>();
        assert_eq!(
            clocks,
            vec![
                &DecisionClockEvidence::precise(
                    "paper_prepared_staleness_gate",
                    gate.unix_timestamp_nanos()
                )
                .unwrap()
            ]
        );
        assert_eq!(h.prepared_count(), usize::from(remainder == 0));
        if remainder == 1 {
            assert_expired(&h, &recorded);
        }
    }
}

/// PASS: restart boot settings cannot replace either enabled or disabled frozen freshness policy.
#[tokio::test]
async fn resumed_decision_uses_frozen_paper_freshness_policy() {
    for enabled in [false, true] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        h.freeze(&recorded, enabled).await;
        h.attempt(&recorded, at() + time::Duration::seconds(3));
        if !enabled {
            *h.hooks.age_clock.lock().unwrap() = [at() + time::Duration::seconds(3); 3].into();
        }
        h.start(!enabled);
        h.barrier().await;
        assert_eq!(h.prepared_count(), usize::from(!enabled));
        let replayed = replay_decision_pending(&h.terminal(&recorded)).unwrap();
        assert_eq!(
            replayed
                .continuation
                .facts
                .paper_freshness_policy
                .unwrap()
                .activity_ws_enabled,
            enabled
        );
        if enabled {
            assert_expired(&h, &recorded);
        } else {
            assert_eq!(replayed.post_boundary.body.terminal.disposition, "fill");
        }
    }
}

/// PASS: failed terminal transactions retain consumed entry, pending seed/target, and stop intake;
/// removing the fault and restarting terminalizes exactly once.
#[tokio::test]
async fn expiry_terminal_failure_preserves_pending_state_and_staged_targets() {
    let mut h = Harness::new().await;
    let held = h.record(1).await;
    h.attempt(&held, at());
    h.start(true);
    h.poll(&held).await;
    h.stop().await;
    let recorded = h.record(2).await;
    h.arm();
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    h.attempt(&recorded, at() + time::Duration::seconds(4));
    h.start(true);
    *h.prices.gate.market.lock().unwrap() = Some(held.admission.market.condition_id.0.clone());
    h.prices.gate.blocked.store(true, Ordering::SeqCst);
    let gate = h.prices.gate.clone();
    {
        let pending = h.poll(&recorded);
        tokio::pin!(pending);
        tokio::select! { biased; _ = gate.started.notified() => {}, _ = &mut pending => panic!("copy completed before staging barrier") }
        assert_eq!(h.paper.pending_dispatch_seeds().unwrap().len(), 1);
        assert_eq!(h.terminal(&recorded).state, DecisionPendingState::Open);
        connection.execute_batch("CREATE TRIGGER fail_expiry BEFORE UPDATE ON decision_pending WHEN NEW.state = 'terminal' BEGIN SELECT RAISE(FAIL, 'injected expiry terminal failure'); END;").unwrap();
        gate.release.notify_one();
        pending.await;
    }
    let error = h.task.take().unwrap().await.unwrap().unwrap_err();
    assert!(matches!(
        error,
        pe_service::orchestrator::OrchestratorRunError::PaperDurabilityUncertain
    ));
    assert_eq!(h.prepared_count(), 1);
    assert_eq!(h.authority.inner.lock().unwrap().fills.len(), 1);
    assert_eq!(h.terminal(&recorded).state, DecisionPendingState::Open);
    assert!(
        h.paper.gate_history().unwrap()[&wallet()]
            .iter()
            .any(|market| market.to_string() == recorded.admission.market.condition_id.0)
    );
    let seeds = h.paper.pending_dispatch_seeds().unwrap();
    assert_eq!(seeds.len(), 1, "{:?}", h.terminal(&recorded));
    let targets = h.paper.dispatch_targets(&seeds[0].dispatch_id).unwrap();
    assert_eq!(targets.len(), 1);
    connection
        .execute_batch("DROP TRIGGER fail_expiry;")
        .unwrap();
    h.stop().await;
    h.attempt(&recorded, at() + time::Duration::seconds(4));
    h.start(false);
    h.barrier().await;
    assert_expired(&h, &recorded);
    assert_eq!(h.prepared_count(), 1);
    assert_eq!(
        h.paper
            .dispatch_seed(&seeds[0].dispatch_id)
            .unwrap()
            .unwrap()
            .paper_outcome
            .as_deref(),
        Some("no_fill:paper_stale_before_prepared")
    );
    assert_eq!(
        h.paper.dispatch_targets(&seeds[0].dispatch_id).unwrap(),
        targets
    );
}

/// PASS: an accepted checkpoint alone is re-evaluated; it grants no durable financial admission.
#[tokio::test]
async fn accepted_checkpoint_without_prepared_is_rechecked_on_restart() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.hooks
        .fail_next_prepared_append
        .store(true, Ordering::SeqCst);
    h.attempt(&recorded, at() + time::Duration::seconds(2));
    h.start(true);
    h.poll(&recorded).await;
    assert!(h.task.take().unwrap().await.unwrap().is_err());
    let checkpoint: Value =
        serde_json::from_str(&h.terminal(&recorded).post_commit_inputs_json).unwrap();
    assert_eq!(
        checkpoint["clocks"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|clock| clock["purpose"] == "paper_prepared_staleness_gate")
            .count(),
        1
    );
    assert_eq!(h.prepared_count(), 0);
    h.stop().await;
    h.attempt(&recorded, at() + time::Duration::seconds(3));
    h.start(false);
    h.barrier().await;
    assert_expired(&h, &recorded);
    assert_eq!(h.prepared_count(), 0);
    assert!(h.authority.inner.lock().unwrap().fills.is_empty());
}

/// PASS: an aged durable Prepared recovers its stored operation and original exact gate clock.
#[tokio::test]
async fn durable_prepared_recovers_after_copy_budget() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.arm();
    h.authority.fail.store(true, Ordering::SeqCst);
    h.attempt(&recorded, at() + time::Duration::seconds(2));
    h.start(true);
    h.poll(&recorded).await;
    assert!(h.task.take().unwrap().await.unwrap().is_err());
    let staged = h.paper.pending_dispatch_seeds().unwrap().remove(0);
    let targets = h.paper.dispatch_targets(&staged.dispatch_id).unwrap();
    let checkpoint: Value =
        serde_json::from_str(&h.terminal(&recorded).post_commit_inputs_json).unwrap();
    assert_eq!(h.prepared_count(), 1);
    h.stop().await;
    h.hooks
        .financial_clock_unix
        .store(EPOCH + 86_400, Ordering::SeqCst);
    let paper_log = h.dir.path().join("paper.log");
    let mut writer = Writer::open(&paper_log).unwrap();
    assert_eq!(
        reconcile_active_financial_frames(
            &h.authority,
            &h.paper,
            &paper_log,
            SourceEvidence::Index(&h.index),
            &mut writer
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        reconcile_active_financial_frames(
            &h.authority,
            &h.paper,
            &paper_log,
            SourceEvidence::Index(&h.index),
            &mut writer
        )
        .await
        .unwrap(),
        0
    );
    let replayed = replay_decision_pending(&h.terminal(&recorded)).unwrap();
    assert_eq!(replayed.post_boundary.body.terminal.disposition, "fill");
    let clocks = &replayed.post_boundary.body.clocks;
    let saved = checkpoint["clocks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|clock| clock["purpose"] == "paper_prepared_staleness_gate")
        .unwrap();
    assert_eq!(
        &serde_json::to_value(
            clocks
                .iter()
                .find(|clock| clock.purpose == "paper_prepared_staleness_gate")
                .unwrap()
        )
        .unwrap(),
        saved
    );
    assert_eq!(h.authority.inner.lock().unwrap().fills.len(), 1);
    assert_eq!(h.paper.list_fills().unwrap().len(), 1);
    assert_eq!(
        replayed.post_boundary.body.terminal.dispatch_id.as_deref(),
        Some(staged.dispatch_id.as_str())
    );
    let recovery =
        pe_service::dispatch_recovery::resume_dispatch_seeds(&paper_log, &h.paper).unwrap();
    assert_eq!(recovery.flipped_fill, 1);
    assert_eq!(
        pe_service::dispatch_recovery::resume_dispatch_seeds(&paper_log, &h.paper).unwrap(),
        pe_service::dispatch_recovery::DispatchResume::default()
    );
    assert_eq!(
        h.paper
            .dispatch_seed(&staged.dispatch_id)
            .unwrap()
            .unwrap()
            .paper_outcome
            .as_deref(),
        Some("fill")
    );
    assert_eq!(
        h.paper.dispatch_targets(&staged.dispatch_id).unwrap(),
        targets
    );
}

/// PASS: malformed policy or exact clock, duplicates, and contradictory terminal shapes reject even with fresh row hashes.
#[tokio::test]
async fn continuation_five_policy_and_clock_shape_rejects_invalid_evidence() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.arm();
    h.attempt(&recorded, at() + time::Duration::seconds(2));
    h.start(true);
    h.poll(&recorded).await;
    let original = h.terminal(&recorded);
    for policy in [
        Value::Null,
        json!({"activity_ws_enabled": true}),
        json!({"copy_latency_budget_secs": 2}),
        json!({"activity_ws_enabled": "true", "copy_latency_budget_secs": 2}),
        json!({"activity_ws_enabled": true, "copy_latency_budget_secs": "2"}),
        json!({"activity_ws_enabled": true, "copy_latency_budget_secs": 0}),
        json!({"activity_ws_enabled": false, "copy_latency_budget_secs": 3601}),
    ] {
        let mut row = original.clone();
        let mut frozen: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
        if policy.is_null() {
            frozen
                .as_object_mut()
                .unwrap()
                .remove("paper_freshness_policy");
        } else {
            frozen["paper_freshness_policy"] = policy;
        }
        row.frozen_inputs_json = frozen.to_string();
        assert!(replay_decision_pending(&row).is_err());
    }
    for mutation in [
        "missing",
        "duplicate",
        "absent_precision",
        "invalid_precision",
        "wrong_terminal",
        "expiry_with_fill",
        "dispatch_staged",
        "missing_dispatch_id",
        "wrong_dispatch_id",
        "missing_staging_clock",
        "duplicate_staging_clock",
    ] {
        let mut row = original.clone();
        let mut document: DecisionPostBoundaryEvidence =
            serde_json::from_str(&row.post_commit_inputs_json).unwrap();
        let index = document
            .body
            .clocks
            .iter()
            .position(|clock| clock.purpose == "paper_prepared_staleness_gate")
            .unwrap();
        match mutation {
            "missing" => {
                document.body.clocks.remove(index);
            }
            "duplicate" => document
                .body
                .clocks
                .push(document.body.clocks[index].clone()),
            "absent_precision" => document.body.clocks[index].submillisecond_nanos = None,
            "invalid_precision" => {
                document.body.clocks[index].submillisecond_nanos = Some(1_000_000)
            }
            "wrong_terminal" => {
                document.body.terminal.reason = "another_decline".to_owned();
                document.body.terminal.disposition = "no_fill".to_owned();
                document.body.terminal.final_receipt = None;
                row.terminal_disposition = Some("no_fill".to_owned());
            }
            "expiry_with_fill" => {
                document.body.terminal.reason = "paper_stale_before_prepared".to_owned()
            }
            "dispatch_staged" => {
                document.body.version = 4;
                document.body.clocks.remove(index);
                document.body.terminal.disposition = "dispatch_staged".to_owned();
                document.body.terminal.reason = "live_targets_staged".to_owned();
                document.body.terminal.final_receipt = None;
                document.body.authority.kind = "not_read".to_owned();
                document.body.authority.outcome =
                    "dispatch_staged_before_fill_authority".to_owned();
                row.terminal_disposition = Some("dispatch_staged".to_owned());
            }
            "missing_dispatch_id" => document.body.terminal.dispatch_id = None,
            "wrong_dispatch_id" => {
                document.body.terminal.dispatch_id = Some("another-dispatch".to_owned())
            }
            "missing_staging_clock" => document
                .body
                .clocks
                .retain(|clock| clock.purpose != "dispatch_seed_created"),
            "duplicate_staging_clock" => document.body.clocks.push(
                document
                    .body
                    .clocks
                    .iter()
                    .find(|clock| clock.purpose == "dispatch_seed_created")
                    .unwrap()
                    .clone(),
            ),
            _ => unreachable!(),
        }
        row.post_commit_inputs_json =
            serde_json::to_string(&DecisionPostBoundaryEvidence::from_body(document.body).unwrap())
                .unwrap();
        assert!(replay_decision_pending(&row).is_err(), "{mutation}");
    }
}

/// PASS: minimum, cap, and no-edge refusals retain distinct book evidence through terminal replay;
/// a checked quantity overflow remains arithmetic_failure and no decline submits an order.
#[tokio::test]
async fn book_business_declines_survive_terminal_replay() {
    for expected in [
        "below_minimum",
        "cap_exceeded",
        "no_edge",
        "arithmetic_failure",
    ] {
        let mut h = Harness::new().await;
        match expected {
            "below_minimum" => {
                h.config.sizing_mode = SizingMode::Contract { contracts: 1 };
                h.config.sizing_contracts = 1;
            }
            "cap_exceeded" => h.config.per_trade_cap = PerTradeCap::Bps(1),
            "no_edge" => {
                h.config.sizing_mode = SizingMode::Kelly;
                h.probability = pe_core_types::Probability::new(dec!(0.4)).unwrap();
            }
            "arithmetic_failure" => {
                h.config.sizing_mode = SizingMode::Contract {
                    contracts: u64::MAX,
                };
                h.config.sizing_contracts = u64::MAX;
            }
            _ => unreachable!(),
        }
        let recorded = h.record(1).await;
        h.freeze(&recorded, true).await;
        h.attempt(&recorded, at());
        h.start(true);
        h.barrier().await;
        let replayed = replay_decision_pending(&h.terminal(&recorded)).unwrap();
        let book = replayed.post_boundary.body.book.unwrap();
        assert_eq!(book.outcome, expected);
        assert!(book.reason.is_some());
        assert_eq!(h.prepared_count(), 0);
        assert!(h.authority.inner.lock().unwrap().fills.is_empty());
        assert_eq!(h.paper.bankroll().unwrap(), Some(CASH));
        assert!(h.paper.open_positions().unwrap().is_empty());
    }
}

fn stage_legacy_decision(h: &Harness) -> DecisionPendingRow {
    let frozen = include_str!("fixtures/decision_continuation_v4.json");
    let continuation: pe_service::bucket_commit::DecisionContinuationV3 =
        serde_json::from_str(frozen).unwrap();
    let facts = &continuation.facts;
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    connection.execute(
        "INSERT INTO decision_pending (source_trade_id, semantic_revision, wallet_hex, source_epoch, frozen_inputs_json, post_commit_inputs_json, state, updated_at_unix) VALUES (?1, ?2, ?3, ?4, ?5, '[]', 'open', ?4)",
        rusqlite::params![facts.source_trade_id.0, facts.semantic_revision, facts.wallet.to_string(), facts.source_epoch, frozen],
    ).unwrap();
    let dispatch_id = "legacy-dispatch";
    let body = serde_json::from_value(json!({
        "version": 4,
        "owners": ["source_log", "paper_log"],
        "source_trade_id": facts.source_trade_id,
        "applied_configuration_hash": facts.applied_configuration_hash,
        "market_end": null,
        "market_price": null,
        "book": null,
        "clocks": [{"purpose": "dispatch_seed_created", "unix_millis": EPOCH * 1000}],
        "authority": {"kind": "not_read", "outcome": "dispatch_staged_before_fill_authority", "bankroll": null},
        "terminal": {"disposition": "dispatch_staged", "reason": "live_targets_staged", "fill": null, "dispatch_id": dispatch_id}
    })).unwrap();
    let json =
        serde_json::to_string(&DecisionPostBoundaryEvidence::from_body(body).unwrap()).unwrap();
    let seed = pe_paper_state::DispatchSeedRecord {
        dispatch_id: dispatch_id.to_owned(),
        signal_json: json!({"signal": {"leader": wallet(), "observed_at": "2023-11-14T22:13:20Z"}})
            .to_string(),
        source_trade_id: facts.source_trade_id.0.clone(),
        created_at_unix: EPOCH,
        targets: vec![pe_paper_state::DispatchTargetSeed {
            account_id: "legacy-target".to_owned(),
            credential_bundle_version: 3,
            credential_key_id: "legacy-key".to_owned(),
        }],
    };
    let pending = pe_paper_state::DispatchStagingEvidence::LegacyTerminal(
        pe_paper_state::PendingTerminalEvidence {
            post_commit_inputs_json: &json,
            updated_at_unix: EPOCH,
        },
    );
    assert!(
        h.paper
            .stage_dispatch_seed_pending(&seed, Some(pending))
            .unwrap()
    );
    assert!(
        !h.paper
            .stage_dispatch_seed_pending(&seed, Some(pending))
            .unwrap()
    );
    let row = h
        .paper
        .decision_pending_for(&facts.source_trade_id)
        .unwrap()
        .unwrap();
    assert_eq!(row.state, DecisionPendingState::Terminal);
    assert_eq!(row.terminal_disposition.as_deref(), Some("dispatch_staged"));
    assert_eq!(row.post_commit_inputs_json, json);
    let replayed = replay_decision_pending(&row).unwrap();
    assert_eq!(replayed.continuation.version(), 4);
    assert_eq!(replayed.recorded_decision_json, json);
    assert!(!json.contains("submillisecond_nanos"));
    row
}

/// PASS: generation four still closes at staging; recovery preserves the exact replay bytes.
#[tokio::test]
async fn generation_four_staging_terminal_and_replay_remain_unchanged() {
    let h = Harness::new().await;
    let row = stage_legacy_decision(&h);
    h.paper.set_cursor(&wallet(), EPOCH + 1).unwrap();
    let recovery = pe_service::dispatch_recovery::resume_dispatch_seeds(
        &h.dir.path().join("paper.log"),
        &h.paper,
    )
    .unwrap();
    assert_eq!(recovery.finalized_stuck, 1);
    assert_eq!(
        h.paper
            .decision_pending_for(&row.source_trade_id)
            .unwrap()
            .unwrap(),
        row
    );
    assert_eq!(
        replay_decision_pending(&row)
            .unwrap()
            .recorded_decision_json,
        row.post_commit_inputs_json
    );
    assert_eq!(
        h.paper
            .dispatch_seed("legacy-dispatch")
            .unwrap()
            .unwrap()
            .paper_outcome
            .as_deref(),
        Some(pe_service::dispatch_recovery::STUCK_SEED_OUTCOME)
    );
}

/// PASS: either boot order yields one paper terminal and one seed flip, preserving frozen
/// targets and staging evidence, for fresh fill and aged expiry; legacy rows remain unchanged.
#[tokio::test]
async fn staged_generation_five_crash_between_staging_and_outcome_converges_once() {
    for recovery_first in [true, false] {
        for expired in [false, true] {
            let mut h = Harness::new().await;
            let held = h.record(1).await;
            h.attempt(&held, at());
            h.start(true);
            h.poll(&held).await;
            h.stop().await;
            let recorded = h.record(2).await;
            h.arm();
            h.attempt(&recorded, at());
            h.start(true);
            *h.prices.gate.market.lock().unwrap() =
                Some(held.admission.market.condition_id.0.clone());
            h.prices.gate.blocked.store(true, Ordering::SeqCst);
            let gate = h.prices.gate.clone();
            {
                let pending = h.poll(&recorded);
                tokio::pin!(pending);
                tokio::select! { biased; _ = gate.started.notified() => {}, _ = &mut pending => panic!("copy completed before crash barrier") }
            }
            h.stop().await;
            let open = h.terminal(&recorded);
            assert_eq!(open.state, DecisionPendingState::Open);
            let staged: Value = serde_json::from_str(&open.post_commit_inputs_json).unwrap();
            let staging_clock = staged["clocks"]
                .as_array()
                .unwrap()
                .iter()
                .find(|clock| clock["purpose"] == "dispatch_seed_created")
                .unwrap()
                .clone();
            assert!(
                staged["clocks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|clock| clock["purpose"] != "paper_prepared_staleness_gate")
            );
            let seed = h.paper.pending_dispatch_seeds().unwrap().remove(0);
            let targets = h.paper.dispatch_targets(&seed.dispatch_id).unwrap();
            let cash = h.paper.bankroll().unwrap();
            let legacy = stage_legacy_decision(&h);
            h.paper.set_cursor(&wallet(), EPOCH + 100).unwrap();
            let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
            connection.execute_batch("CREATE TABLE outcome_transitions (kind TEXT NOT NULL, identity TEXT NOT NULL);
                CREATE TRIGGER count_seed_flip AFTER UPDATE OF state ON dispatch_seeds WHEN OLD.state = 'pending_paper' AND NEW.state = 'ready' BEGIN INSERT INTO outcome_transitions VALUES ('seed', NEW.dispatch_id); END;
                CREATE TRIGGER count_paper_terminal AFTER UPDATE OF state ON decision_pending WHEN OLD.state = 'open' AND NEW.state = 'terminal' BEGIN INSERT INTO outcome_transitions VALUES ('terminal', NEW.source_trade_id); END;").unwrap();
            if recovery_first {
                for pass in 0..2 {
                    let recovery = pe_service::dispatch_recovery::resume_dispatch_seeds(
                        &h.dir.path().join("paper.log"),
                        &h.paper,
                    )
                    .unwrap();
                    assert_eq!(recovery.left_pending, 1);
                    assert_eq!(recovery.flipped_fill, 0);
                    assert_eq!(recovery.finalized_stuck, usize::from(pass == 0));
                }
                assert_eq!(h.terminal(&recorded), open);
                assert_eq!(
                    h.paper.dispatch_seed(&seed.dispatch_id).unwrap().unwrap(),
                    seed
                );
            }
            // Resume with no armed accounts and aged early clocks: the existing staged
            // aggregate is reused, and only its final paper gate chooses freshness.
            h.live.store(LiveAccountsSnapshot::default());
            h.hooks.age_clock.lock().unwrap().clear();
            let final_at = OffsetDateTime::from_unix_timestamp(recorded.epoch).unwrap()
                + time::Duration::seconds(if expired { 3 } else { 2 });
            h.attempt(&recorded, final_at);
            if expired {
                *h.hooks.age_clock.lock().unwrap() = [final_at; 3].into();
            }
            h.start(false);
            h.barrier().await;
            h.stop().await;
            let terminal = h.terminal(&recorded);
            let replayed = replay_decision_pending(&terminal).unwrap();
            let evidence = &replayed.post_boundary.body;
            assert_eq!(
                evidence.terminal.dispatch_id.as_deref(),
                Some(seed.dispatch_id.as_str())
            );
            assert_eq!(
                serde_json::to_value(
                    evidence
                        .clocks
                        .iter()
                        .find(|clock| clock.purpose == "dispatch_seed_created")
                        .unwrap()
                )
                .unwrap(),
                staging_clock
            );
            let final_clocks = evidence
                .clocks
                .iter()
                .filter(|clock| clock.purpose == "paper_prepared_staleness_gate")
                .collect::<Vec<_>>();
            assert_eq!(final_clocks.len(), 1);
            assert_eq!(
                *final_clocks[0],
                DecisionClockEvidence::precise(
                    "paper_prepared_staleness_gate",
                    final_at.unix_timestamp_nanos()
                )
                .unwrap()
            );
            if expired {
                assert_expired(&h, &recorded);
                assert_eq!(h.paper.bankroll().unwrap(), cash);
            } else {
                assert_eq!(terminal.terminal_disposition.as_deref(), Some("fill"));
            }
            assert_eq!(h.prepared_count(), 1 + usize::from(!expired));
            assert_eq!(
                h.authority.inner.lock().unwrap().fills.len(),
                1 + usize::from(!expired)
            );
            assert_eq!(
                h.paper.list_fills().unwrap().len(),
                1 + usize::from(!expired)
            );
            let ready = h.paper.dispatch_seed(&seed.dispatch_id).unwrap().unwrap();
            assert_eq!(ready.state, "ready");
            assert_eq!(
                ready.paper_outcome.as_deref(),
                Some(if expired {
                    "no_fill:paper_stale_before_prepared"
                } else {
                    "fill"
                })
            );
            for pass in 0..2 {
                let recovery = pe_service::dispatch_recovery::resume_dispatch_seeds(
                    &h.dir.path().join("paper.log"),
                    &h.paper,
                )
                .unwrap();
                assert_eq!(recovery.left_pending, 0);
                assert_eq!(recovery.flipped_fill, 0);
                assert_eq!(
                    recovery.finalized_stuck,
                    usize::from(!recovery_first && pass == 0)
                );
            }
            assert_eq!(
                h.paper.dispatch_seed(&seed.dispatch_id).unwrap().unwrap(),
                ready
            );
            assert_eq!(
                h.paper.dispatch_targets(&seed.dispatch_id).unwrap(),
                targets
            );
            assert_eq!(h.terminal(&recorded), terminal);
            assert_eq!(
                h.paper
                    .decision_pending_for(&legacy.source_trade_id)
                    .unwrap()
                    .unwrap(),
                legacy
            );
            for (kind, identity) in [("seed", &seed.dispatch_id), ("terminal", &recorded.id.0)] {
                let count: usize = connection.query_row("SELECT count(*) FROM outcome_transitions WHERE kind = ?1 AND identity = ?2", rusqlite::params![kind, identity], |row| row.get(0)).unwrap();
                assert_eq!(
                    count, 1,
                    "{kind}, recovery_first={recovery_first}, expired={expired}"
                );
            }
        }
    }
}
