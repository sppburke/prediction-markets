//! Continuation-six freshness through the poller, source coordinator, and paper owner.
#![cfg(feature = "scenario")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use pe_copy_signal_engine::SignalConfig;
use pe_core_types::{
    AccountId, BasisPoints, CollateralAmount, EventSeq, PolymarketConditionId, ReceivedAt,
    ReconstructionQuality, SourceId, SourceTimestamp, SourceTradeId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Reader, Scanner, Writer};
use pe_execution_core::{
    AdmissionReceipts, CredentialBindingIdentity, FrozenLiveTarget, LiveAdmissionArtifact,
    LiveControlMode, LiveExecutedAmounts, LiveExecutor, LiveJournal, LiveJournalPayload,
    LiveModeSnapshot, LiveOrderIdentity, LiveOrderOutcome, LiveOrderRequest, LiveOrderVenue,
    LivePostClassification, LivePostParseError, LivePrepareResult, LiveVenueAccountReadError,
    LiveVenueAccountState, LiveVenuePrepareRequest, LiveVenuePrepared, LiveVenueReconciliation,
    LiveVenueReconciliationError,
};
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
    CanonicalFillResult, CanonicalResolutionResult, ExpectedAuthority, FinancialPayload,
    FinancialResult, PaperLogFrame, PaperLogRecord, QualificationStarted, TailBinding,
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
use pe_source_polymarket_public::{GAMMA_BATCH_SIZE, validate_live_market, validate_paper_market};
use pe_strategy_winner_follow::{ExecutionMode, PerTradeCap, SizingMode, WinnerFollowStrategy};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::{AskLevel, LadderPlan, PreparedPolymarketBuy, parse_compact_market};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{Value, json};
use time::OffsetDateTime;
use tokio::sync::mpsc;

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

use support::{Page, PriceGate, Prices};

#[derive(Default)]
struct Books(Mutex<HashMap<String, OrderBook>>);
struct DelayedPage {
    page: Vec<u8>,
    misses: AtomicUsize,
    reads: AtomicUsize,
}
impl pe_source_polymarket_public::ReconciliationFetcher for DelayedPage {
    fn fetch<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, pe_source_core::SourceError>> + Send + 'a>>
    {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let remaining =
                self.misses
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        (remaining > 0).then(|| remaining - 1)
                    });
            if remaining.is_ok() {
                Ok(b"[]".to_vec())
            } else {
                Ok(self.page.clone())
            }
        })
    }
}
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
    copy_budget_secs: u64,
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
    admission_builder: Option<LiveAdmissionBuilder>,
    control: Option<mpsc::Sender<OrchestratorControl>>,
    task:
        Option<tokio::task::JoinHandle<Result<(), pe_service::orchestrator::OrchestratorRunError>>>,
    coordinator: tokio::task::JoinHandle<()>,
    _trigger_rx: mpsc::Receiver<pe_service::activity_ingest::ReconciliationTrigger>,
}
impl Harness {
    async fn new() -> Self {
        Self::new_with_semantic(pe_service::paper_recovery::FINANCIAL_SEMANTIC_VERSION).await
    }

    async fn new_with_semantic(financial_semantic_version: u32) -> Self {
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
        let live_path = dir.path().join("live_journal.log");
        drop(pe_execution_core::LiveJournal::open(&live_path).unwrap());
        let empty = TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        };
        let record = PaperLogRecord::QualificationStarted(Box::new(QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(CASH).unwrap(),
            paper_prefix: empty.clone(),
            source_prefix: empty.clone(),
            live_prefix: TailBinding::from(
                &pe_execution_core::LiveJournal::verified_tail(&live_path).unwrap(),
            ),
            artifact_blake3: "fixture".to_owned(),
            static_config_hash: "fixture".to_owned(),
            hot_config_hash: runtime().canonical_hash(),
            generation: "prepared-freshness".to_owned(),
            activation_id: "prepared-freshness".to_owned(),
            ranking_batch_id: 588,
            membership: vec![wallet()],
            membership_proofs_hash: pe_service::qualification::scenario_membership_proofs_hash(
                &paper,
                &[wallet()],
            )
            .unwrap(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version,
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
            copy_budget_secs: 2,
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
            admission_builder: None,
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
        self.record_with_rule(ordinal, None).await
    }

    async fn record_with_rule(
        &self,
        ordinal: u32,
        paper_rule: Option<(Decimal, Decimal, Option<i64>)>,
    ) -> Recorded {
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
        if let Some((leader, _, _)) = paper_rule {
            let source_shares = if leader == dec!(0.5799999992) {
                dec!(1250)
            } else {
                dec!(5)
            };
            activity[0]["price"] = leader.normalize().to_string().into();
            activity[0]["size"] = source_shares.to_string().into();
            activity[0]["usdcSize"] = (leader * source_shares).normalize().to_string().into();
        }
        let mut gamma: Value =
            serde_json::from_slice(include_bytes!("fixtures/golden_stream_v1/gamma_long.json"))
                .unwrap();
        gamma[0]["conditionId"] = condition.clone().into();
        gamma[0]["clobTokenIds"] = json!([token, other]).to_string().into();
        gamma[0]["outcomePrices"] = "[\"0.50\",\"0.50\"]".into();
        // Wire shape observed 2026-09-15 (#638): Gamma omits `secondsDelay`, and fee-bearing
        // markets report legacy base fees of 1000 bps in every payload beside the compact fee curve.
        gamma[0].as_object_mut().unwrap().remove("secondsDelay");
        gamma[0]["makerBaseFee"] = 1000.into();
        gamma[0]["takerBaseFee"] = 1000.into();
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
        long["maker_base_fee"] = 1000.into();
        long["taker_base_fee"] = 1000.into();
        if let Some((_, _, delay)) = paper_rule {
            match delay {
                Some(seconds) => long["seconds_delay"] = seconds.into(),
                None => {
                    long.as_object_mut().unwrap().remove("seconds_delay");
                }
            }
        }
        let mut compact: Value = serde_json::from_slice(include_bytes!(
            "fixtures/golden_stream_v1/clob_compact.json"
        ))
        .unwrap();
        compact["c"] = condition.clone().into();
        compact["t"][0]["t"] = token.clone().into();
        compact["t"][1]["t"] = other.into();
        compact["mbf"] = 1000.into();
        compact["tbf"] = 1000.into();
        let mut book: Value =
            serde_json::from_slice(include_bytes!("fixtures/golden_stream_v1/book.json")).unwrap();
        book["market"] = condition.clone().into();
        book["asset_id"] = token.clone().into();
        if let Some((_, ask, _)) = paper_rule {
            book["asks"] = json!([{"price": ask.normalize().to_string(), "size": "100"}]);
        }
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
        let market = if paper_rule.is_some() {
            validate_paper_market(&gamma, &long, &condition, EPOCH, 60)
        } else {
            validate_live_market(&gamma, &long, &condition, EPOCH, 60)
        }
        .unwrap();
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
                live_journal: Some(
                    Arc::new(
                        pe_execution_core::LiveJournal::open(
                            self.dir.path().join("live_journal.log"),
                        )
                        .unwrap(),
                    )
                    .into(),
                ),
                activity_ws_enabled: enabled,
                copy_latency_budget_secs: self.copy_budget_secs,
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
                self.admission_builder.clone().unwrap_or_else(|| {
                    LiveAdmissionBuilder::new(
                        reqwest::Client::new(),
                        "http://unused.invalid",
                        "http://unused.invalid",
                        self.source.clone(),
                    )
                }),
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
            pe_service::orchestrator::SCENARIO_TERMINAL_CLOCK
                .scope(at(), owner.run_coordinated(std::future::pending::<()>())),
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
        self.poll_source(
            recorded,
            None,
            OffsetDateTime::from_unix_timestamp(recorded.epoch).unwrap(),
        )
        .await;
    }
    async fn poll_source(
        &self,
        recorded: &Recorded,
        stream_epoch: Option<i64>,
        now: OffsetDateTime,
    ) {
        self.poll_source_with_misses(recorded, stream_epoch, now, 0)
            .await;
    }
    async fn poll_source_with_misses(
        &self,
        recorded: &Recorded,
        stream_epoch: Option<i64>,
        now: OffsetDateTime,
        misses: usize,
    ) {
        let (trigger, receiver) = mpsc::channel(1);
        if let Some(epoch) = stream_epoch {
            let mut stream: Value = serde_json::from_slice(&recorded.activity).unwrap();
            let row = &mut stream[0];
            row["timestamp"] = epoch.into();
            let payload = serde_json::to_vec(row).unwrap();
            let observation =
                pe_source_polymarket_public::parse_activity_trade_observation(&payload).unwrap();
            let receipt = self
                .source
                .append(EnvelopeIn {
                    source_id: SourceId(
                        pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned(),
                    ),
                    schema_version: pe_source_polymarket_public::ACTIVITY_SCHEMA_VERSION,
                    parser_version: pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
                    observed_at: SourceTimestamp(now),
                    received_at: ReceivedAt(now),
                    content_type: ContentType::Json,
                    payload,
                })
                .await
                .unwrap();
            trigger
                .send(pe_service::activity_ingest::ReconciliationTrigger {
                    wallet: wallet(),
                    source_time: observation.source_time.0,
                    source_trade_id: observation.group_id.key().clone(),
                    provenance: pe_copy_signal_engine::TradeProvenance::ActivityWs,
                    received_at: now,
                    receipt,
                })
                .await
                .unwrap();
        }
        let (progress, mut completed) = mpsc::channel(8);
        let delayed = Arc::new(DelayedPage {
            page: recorded.activity.clone(),
            misses: AtomicUsize::new(misses),
            reads: AtomicUsize::new(0),
        });
        let clock = Arc::new(AtomicI64::new(now.unix_timestamp()));
        let poll_clock = Arc::clone(&clock);
        let poller = TradePoller::new(
            TradePollerConfig {
                base_url: "fixture://activity".to_owned(),
                poll_interval_secs: 30,
                activity_ws_enabled: stream_epoch.is_some(),
                copy_latency_budget_secs: self.copy_budget_secs,
            },
            watchlist(),
            delayed.clone(),
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
        .with_clock(Arc::new(move || {
            OffsetDateTime::from_unix_timestamp(poll_clock.load(Ordering::SeqCst)).unwrap()
        }));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(poller.run_until(async move {
            let _ = stopped.await;
        }));
        while let Some(progress) = completed.recv().await {
            if misses == 0
                && matches!(
                    progress,
                    pe_service::trade_poller::PollerProgress::RoundCompleted
                )
            {
                break;
            }
            if misses > 0
                && matches!(
                    progress,
                    pe_service::trade_poller::PollerProgress::Completed { .. }
                )
            {
                if self
                    .paper
                    .activity_group_state(&recorded.id)
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                clock.fetch_add(1, Ordering::SeqCst);
                tokio::time::advance(std::time::Duration::from_secs(1)).await;
            }
        }
        assert!(delayed.reads.load(Ordering::SeqCst) > misses);
        stop.send(()).unwrap();
        task.await.unwrap().unwrap();
        if misses > 0 {
            self.barrier().await;
        }
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
                custody_wallet_address: None,
                custody_wallet_kind: None,
                credential_binding: Some((7, "stored-key".to_owned())),
            }],
            fetched_at_unix: Some(EPOCH),
            control_available: true,
            generation: 1,
            credential_metadata_available: true,
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
                    copy_latency_budget_secs: self.copy_budget_secs,
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
impl Harness {
    async fn boot_barrier(&mut self) {
        let (captured, receiver) = tokio::sync::oneshot::channel();
        self.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::CaptureAdmissionLedger {
                wallet: wallet(),
                captured,
            })
            .await
            .unwrap();
        if receiver.await.is_err() {
            panic!(
                "boot recovery stopped: {:?}",
                self.task.take().unwrap().await
            );
        }
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

fn assert_shared_stale(
    h: &Harness,
    recorded: &Recorded,
    initial_at: OffsetDateTime,
    dispatch_at: Option<OffsetDateTime>,
) {
    let row = h.terminal(recorded);
    let replayed = replay_decision_pending(&row).unwrap();
    let body = &replayed.post_boundary.body;
    assert_eq!(row.state, DecisionPendingState::Terminal);
    assert_eq!(
        row.terminal_disposition.as_deref(),
        Some("no_copy:stale_activity_ws_past_copy_budget")
    );
    assert_eq!(body.terminal.reason, "stale_activity_ws_past_copy_budget");
    assert!(body.terminal.dispatch_id.is_none());
    assert_eq!(body.authority.kind, "not_read");
    assert_eq!(body.authority.outcome, "terminal_before_fill_authority");
    let mut expected = vec![("initial_staleness_gate", initial_at)];
    if let Some(dispatch_at) = dispatch_at {
        expected.push(("pre_dispatch_staleness_gate", dispatch_at));
    }
    expected.push(("terminal_transition", at()));
    let clocks = body
        .clocks
        .iter()
        .filter(|clock| clock.purpose != "book_staleness_check")
        .map(|clock| {
            assert_eq!(clock.submillisecond_nanos, None);
            (clock.purpose.as_str(), clock.unix_millis)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        clocks,
        expected
            .into_iter()
            .map(|(purpose, time)| (
                purpose,
                i64::try_from(time.unix_timestamp_nanos() / 1_000_000).unwrap()
            ))
            .collect::<Vec<_>>()
    );
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    let disposition: (String, i64, String, i64) = connection
        .query_row(
            "SELECT provenance, age_secs, reason, recorded_at_unix FROM no_copy_dispositions WHERE source_trade_id = ?1",
            [&recorded.id.0],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(
        disposition,
        (
            "activity_ws".to_owned(),
            i64::try_from(h.copy_budget_secs).unwrap(),
            "stale_activity_ws_past_copy_budget".to_owned(),
            dispatch_at.unwrap_or(initial_at).unix_timestamp()
        )
    );
    let seeds: usize = connection
        .query_row("SELECT count(*) FROM dispatch_seeds", [], |row| row.get(0))
        .unwrap();
    assert_eq!(seeds, 0);
    assert_eq!(h.prepared_count(), 0);
    assert!(h.authority.inner.lock().unwrap().fills.is_empty());
    assert!(h.paper.list_fills().unwrap().is_empty());
    assert_eq!(h.paper.bankroll().unwrap(), Some(CASH));
    assert!(
        h.paper.gate_history().unwrap()[&wallet()].contains(&pe_core_types::MarketId(
            pe_core_types::VenueMarketId(recorded.admission.market.condition_id.0.clone())
        ))
    );
}

fn assert_bound_clocks(h: &Harness, recorded: &Recorded, stream_epoch: i64) {
    let row = h.terminal(recorded);
    let continuation =
        pe_service::bucket_commit::DecisionContinuationV3::from_durable(&row).unwrap();
    assert_eq!(continuation.version(), 6);
    assert_eq!(continuation.facts.source_epoch, recorded.epoch);
    assert_eq!(
        continuation
            .incoming_trade()
            .unwrap()
            .observed_at
            .unix_timestamp(),
        recorded.epoch
    );
    let receipt = continuation.observed_source_receipt.unwrap();
    let stream = source_envelope(h, receipt);
    let observation =
        pe_source_polymarket_public::parse_activity_trade_observation(&stream.payload).unwrap();
    assert_eq!(observation.source_time.0.unix_timestamp(), stream_epoch);
    let commitment = source_envelope(h, continuation.read_commitment.unwrap());
    let commitment: pe_service::bucket_commit::ActivityReadCommitment =
        serde_json::from_slice(&commitment.payload).unwrap();
    let bindings = commitment.bindings.unwrap();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].stream_receipt, receipt);
    assert_eq!(bindings[0].history_group_id, recorded.id);
    assert_eq!(bindings[0].semantic_revision, row.semantic_revision);
}

/// PASS: a missing authenticated page fails before either shared gate or admission, leaves
/// the exact open continuation unchanged, and stops boot recovery without financial effects.
#[tokio::test]
async fn bound_source_clock_reconstruction_failure_stops_before_admission() {
    let mut h = Harness::new().await;
    // An index of the configured log taken before the page is appended lacks its receipt.
    let stale = SourceReceiptIndex::replay(&h.dir.path().join("source.log")).unwrap();
    let recorded = h.record(1).await;
    h.freeze(&recorded, true).await;
    let open = h.terminal(&recorded);
    h.index = stale;
    h.attempt(&recorded, at());
    h.arm();
    h.start(true);
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), h.task.take().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(pe_service::orchestrator::OrchestratorRunError::PendingRecovery(ref reason))
        if reason == "paper durability became uncertain while resuming decision_pending")
    );
    assert_eq!(h.terminal(&recorded), open);
    assert_eq!(h.hooks.admission_artifacts.lock().unwrap().len(), 1);
    assert_eq!(h.hooks.age_clock.lock().unwrap().len(), 3);
    assert!(h.paper.pending_dispatch_seeds().unwrap().is_empty());
    assert_eq!(h.prepared_count(), 0);
    assert!(h.authority.inner.lock().unwrap().fills.is_empty());
}

/// PASS: an earlier bound stream clock expires shared admission after a held price GET,
/// both immediately after bucket commit and after restarting that open continuation.
#[tokio::test]
async fn bound_source_clock_expires_shared_dispatch_on_first_pass_and_restart() {
    for restart in [false, true] {
        let mut h = Harness::new().await;
        let recorded = h.record(2).await; // history T+1, stream T
        let initial_at = at() + time::Duration::milliseconds(1500);
        let dispatch_at = at() + time::Duration::milliseconds(2500);
        h.arm();
        h.hooks.age_clock.lock().unwrap().push_back(initial_at);
        h.hooks
            .admission_artifacts
            .lock()
            .unwrap()
            .push_back(recorded.admission.clone());
        *h.prices.gate.market.lock().unwrap() =
            Some(recorded.admission.market.condition_id.0.clone());
        h.prices.gate.blocked.store(true, Ordering::SeqCst);
        let gate = h.prices.gate.clone();
        h.start(true);
        {
            let pending = h.poll_source(&recorded, Some(EPOCH), initial_at);
            tokio::pin!(pending);
            tokio::select! {
                _ = gate.started.notified() => {},
                _ = &mut pending => panic!("decision completed before price barrier"),
            }
            assert_bound_clocks(&h, &recorded, EPOCH);
            assert_eq!(h.terminal(&recorded).state, DecisionPendingState::Open);
            assert_eq!(h.prepared_count(), 0);
            if !restart {
                h.hooks.age_clock.lock().unwrap().push_back(dispatch_at);
                gate.release.notify_one();
                pending.await;
            }
        }
        if restart {
            h.stop().await;
            h.index = SourceReceiptIndex::replay(&h.dir.path().join("source.log")).unwrap();
            h.hooks.age_clock.lock().unwrap().push_back(initial_at);
            h.hooks
                .admission_artifacts
                .lock()
                .unwrap()
                .push_back(recorded.admission.clone());
            gate.blocked.store(true, Ordering::SeqCst);
            h.start(false); // The committed generation-six policy remains enabled.
            tokio::time::timeout(std::time::Duration::from_secs(5), gate.started.notified())
                .await
                .unwrap();
            h.hooks.age_clock.lock().unwrap().push_back(dispatch_at);
            gate.release.notify_one();
            h.boot_barrier().await;
        }
        assert_shared_stale(&h, &recorded, initial_at, Some(dispatch_at));
    }
}

/// PASS: initial admission uses the authenticated stream clock before consuming an admission
/// artifact, with no dispatch seed, Prepared, or authority call.
#[tokio::test]
async fn bound_source_clock_expires_initial_shared_gate() {
    let mut h = Harness::new().await;
    let recorded = h.record(2).await;
    let initial_at = at() + time::Duration::milliseconds(2500);
    h.arm();
    h.hooks.age_clock.lock().unwrap().push_back(initial_at);
    h.hooks
        .admission_artifacts
        .lock()
        .unwrap()
        .push_back(recorded.admission.clone());
    h.start(true);
    h.poll_source(
        &recorded,
        Some(EPOCH),
        at() + time::Duration::milliseconds(1500),
    )
    .await;
    assert_bound_clocks(&h, &recorded, EPOCH);
    assert_shared_stale(&h, &recorded, initial_at, None);
    assert_eq!(h.hooks.admission_artifacts.lock().unwrap().len(), 1);
}

/// PASS: either direction of the source-second correction admits exactly at the earliest
/// source deadline and refuses at deadline +1 ns; accepted operation identity stays historical.
#[tokio::test]
async fn bound_source_clock_shared_gate_exact_boundary_in_both_directions() {
    for budget in [2, 120] {
        for initial_gate in [false, true] {
            for history_later in [false, true] {
                for expired in [false, true] {
                    let mut h = Harness::new().await;
                    h.copy_budget_secs = budget;
                    let recorded = h.record(if history_later { 2 } else { 1 }).await;
                    let stream_epoch = if history_later { EPOCH } else { EPOCH + 1 };
                    let reconciled_at = at() + time::Duration::milliseconds(1500);
                    let deadline = at() + time::Duration::seconds(i64::try_from(budget).unwrap());
                    let dispatch_at = deadline + time::Duration::nanoseconds(i64::from(expired));
                    let initial_at = if initial_gate {
                        dispatch_at
                    } else {
                        reconciled_at
                    };
                    h.arm();
                    h.hooks.age_clock.lock().unwrap().extend([
                        initial_at,
                        dispatch_at,
                        dispatch_at,
                    ]);
                    h.hooks
                        .admission_artifacts
                        .lock()
                        .unwrap()
                        .push_back(recorded.admission.clone());
                    h.start(true);
                    h.poll_source(&recorded, Some(stream_epoch), reconciled_at)
                        .await;
                    assert_bound_clocks(&h, &recorded, stream_epoch);
                    if expired {
                        assert_shared_stale(
                            &h,
                            &recorded,
                            initial_at,
                            (!initial_gate).then_some(dispatch_at),
                        );
                    } else {
                        let row = h.terminal(&recorded);
                        let replayed = replay_decision_pending(&row).unwrap();
                        assert_eq!(row.terminal_disposition.as_deref(), Some("fill"));
                        assert_eq!(h.prepared_count(), 1);
                        assert_eq!(h.authority.inner.lock().unwrap().fills.len(), 1);
                        assert_eq!(h.paper.list_fills().unwrap().len(), 1);
                        let frames = scan_paper_log(&h.dir.path().join("paper.log")).unwrap();
                        let operation_epoch = frames
                            .iter()
                            .find_map(|frame| match &frame.frame {
                                PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                                    payload:
                                        pe_service::paper_recovery::FinancialPayload::Fill {
                                            operation,
                                            ..
                                        },
                                    ..
                                }) => Some(operation.observed_at_bucket),
                                _ => None,
                            })
                            .unwrap();
                        assert_eq!(operation_epoch, recorded.epoch);
                        let clock = replayed
                            .post_boundary
                            .body
                            .clocks
                            .iter()
                            .find(|clock| clock.purpose == "paper_prepared_staleness_gate")
                            .unwrap();
                        assert_eq!(
                            *clock,
                            DecisionClockEvidence::precise(
                                "paper_prepared_staleness_gate",
                                deadline.unix_timestamp_nanos()
                            )
                            .unwrap()
                        );
                    }
                }
            }
        }
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
    for budget in [2, 120] {
        let mut h = Harness::new().await;
        h.copy_budget_secs = budget;
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
            OffsetDateTime::from_unix_timestamp(recorded.epoch).unwrap()
                + time::Duration::seconds(i64::try_from(budget).unwrap())
                + time::Duration::nanoseconds(1),
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
}

/// PASS: recorded lifted asks fill above the frozen leader price and replay with either delay shape.
#[tokio::test]
async fn recorded_ask_above_leader_fills_with_both_paper_delay_policies() {
    for (ordinal, leader, ask, delay) in [
        (1, dec!(0.71), dec!(0.73), Some(2)),
        (1, dec!(0.5799999992), dec!(0.58), None),
    ] {
        let mut h = Harness::new().await;
        h.copy_budget_secs = 120;
        h.probability = pe_core_types::Probability::new(dec!(0.9)).unwrap();
        let recorded = h
            .record_with_rule(ordinal, Some((leader, ask, delay)))
            .await;
        let gamma = source_envelope(&h, recorded.admission.receipts.gamma).payload;
        let clob = source_envelope(&h, recorded.admission.receipts.clob_long).payload;
        assert!(
            validate_live_market(
                &gamma,
                &clob,
                &recorded.admission.market.condition_id,
                EPOCH,
                60,
            )
            .is_err()
        );
        h.arm();
        h.attempt(&recorded, at());
        h.start(true);
        h.poll(&recorded).await;
        let row = h.terminal(&recorded);
        assert_eq!(row.terminal_disposition.as_deref(), Some("fill"));
        let staged = h.paper.unfinalized_ready_dispatch_seeds().unwrap();
        assert_eq!(staged.len(), 1);
        assert_eq!(
            h.paper
                .dispatch_targets(&staged[0].dispatch_id)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            pe_service::bucket_commit::DecisionContinuationV3::from_durable(&row)
                .unwrap()
                .version(),
            6
        );
        let frames = scan_paper_log(&h.dir.path().join("paper.log")).unwrap();
        let economic = frames
            .iter()
            .find_map(|frame| match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                    payload: pe_service::paper_recovery::FinancialPayload::Fill { economic, .. },
                    ..
                }) => Some(economic),
                _ => None,
            })
            .unwrap();
        assert_eq!(economic.version, 2);
        assert_eq!(economic.balance.chase_ceiling, pe_core_types::Price::ONE);
        assert_eq!(
            economic.ladder.best_ask,
            pe_core_types::Price::new(ask).unwrap()
        );
        assert_eq!(
            economic.ladder.limit_price,
            pe_core_types::Price::new(ask).unwrap()
        );
        assert_ne!(
            economic.ladder.limit_price,
            pe_core_types::Price::new(leader).unwrap()
        );
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "{:?}", report.reasons);
        assert_eq!(report.replay.fills, 1);
    }
}

/// The real bucket continuation, financial Final, and priced daily mark do not walk the source log.
#[tokio::test]
async fn paper_fill_and_daily_mark_do_not_scan_source_log() {
    let mut h = Harness::new().await;
    h.probability = pe_core_types::Probability::new(dec!(0.9)).unwrap();
    let recorded = h
        .record_with_rule(1, Some((dec!(0.50), dec!(0.50), Some(2))))
        .await;
    h.arm();
    h.attempt(&recorded, at());
    h.start(true);
    let source_path = h.dir.path().join("source.log");
    let before = pe_event_log::scan_metrics::count(&source_path).unwrap();
    h.poll(&recorded).await;
    assert_eq!(
        h.terminal(&recorded).terminal_disposition.as_deref(),
        Some("fill")
    );
    assert_eq!(
        pe_event_log::scan_metrics::count(&source_path).unwrap(),
        before
    );
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{:?}", report.reasons);
}

/// A priced S1 mark remains verifiable when its position resolves before the completion seal.
#[tokio::test]
async fn priced_mark_resolved_before_completion_seal_verifies() {
    let mut h = Harness::new().await;
    h.probability = pe_core_types::Probability::new(dec!(0.9)).unwrap();
    let recorded = h
        .record_with_rule(1, Some((dec!(0.50), dec!(0.50), Some(2))))
        .await;
    h.arm();
    h.attempt(&recorded, at());
    h.start(true);
    h.poll(&recorded).await;
    assert_eq!(
        h.terminal(&recorded).terminal_disposition.as_deref(),
        Some("fill")
    );
    assert!(h.qualify_one_fill().await.replay.exact);

    let paper_path = h.dir.path().join("paper.log");
    let source_path = h.dir.path().join("source.log");
    let cutoff = EPOCH - EPOCH.rem_euclid(86_400) + 86_400;
    let frames = scan_paper_log(&paper_path).unwrap();
    let (fill_prepared, economic) = frames
        .iter()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                payload: FinancialPayload::Fill { economic, .. },
                ..
            }) => Some((frame.receipt, economic.clone())),
            _ => None,
        })
        .unwrap();
    let mark = frames
        .iter()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => Some(mark),
            _ => None,
        })
        .unwrap();
    assert_eq!(mark.financial_prefix_seq, Some(fill_prepared.sequence));
    assert_eq!(mark.prices.len(), 1);
    assert!(mark.prices[0].receipt.is_some());

    let seal_offset = Reader::replay_with_offsets(&paper_path)
        .unwrap()
        .find_map(|frame| {
            let (offset, _, envelope) = frame.unwrap();
            let record: PaperLogRecord = serde_json::from_slice(&envelope.payload).unwrap();
            matches!(record, PaperLogRecord::QualificationSealed(_)).then_some(offset)
        })
        .unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&paper_path)
        .unwrap();
    file.set_len(seal_offset).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let resolved_at = OffsetDateTime::from_unix_timestamp(cutoff + 1).unwrap();
    let resolution_receipt = h
        .source
        .append(EnvelopeIn {
            source_id: SourceId("polymarket.clob.market".to_owned()),
            schema_version: pe_source_polymarket_public::CLOB_RESOLUTION_SCHEMA_VERSION,
            parser_version: pe_source_polymarket_public::CLOB_RESOLUTION_PARSER_VERSION,
            observed_at: SourceTimestamp(resolved_at),
            received_at: ReceivedAt(resolved_at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&json!({
                "condition_id": economic.market.condition_id.0.clone(),
                "closed": true,
                "is_50_50_outcome": false,
                "tokens": [
                    {"token_id": economic.market.token_id.0.clone(), "outcome": "Yes", "price": 1, "winner": true},
                    {"token_id": economic.admission.market.ordered_outcome_token_ids[1].0.clone(), "outcome": "No", "price": 0, "winner": false}
                ]
            }))
            .unwrap(),
        })
        .await
        .unwrap();
    let start = h.authority.inner.lock().unwrap().start;
    let payout_json = "[\"1\",\"0\"]";
    let mut writer = Writer::open(&paper_path).unwrap();
    let prepared = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: 2,
            parser_version: 1,
            observed_at: SourceTimestamp(resolved_at),
            received_at: ReceivedAt(resolved_at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::FinancialPrepared {
                expected_authority: ExpectedAuthority {
                    qualification_start_receipt: start,
                    prior_completed_prepared_sequence: Some(fill_prepared.sequence),
                },
                payload: FinancialPayload::Resolution {
                    condition_id: economic.market.condition_id.clone(),
                    payout_by_outcome_index_json: payout_json.to_owned(),
                    resolution_source_receipt: resolution_receipt,
                },
            })
            .unwrap(),
        })
        .unwrap();
    let credit =
        CollateralAmount::from_decimal_exact(economic.sizing.expected_shares.to_decimal()).unwrap();
    let bankroll = h.paper.bankroll().unwrap().unwrap() + credit.to_decimal();
    h.paper
        .apply_financial_resolution(
            start,
            Some(fill_prepared.sequence),
            prepared.sequence,
            &pe_core_types::MarketId(pe_core_types::VenueMarketId(
                economic.market.market_id.clone(),
            )),
            payout_json,
            resolution_receipt,
            cutoff + 1,
            credit,
            bankroll,
        )
        .unwrap();
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: 2,
            parser_version: 1,
            observed_at: SourceTimestamp(resolved_at),
            received_at: ReceivedAt(resolved_at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::FinancialFinal {
                prepared_receipt: prepared,
                result: FinancialResult::Resolution {
                    canonical: CanonicalResolutionResult {
                        outcome: "applied".to_owned(),
                        bankroll,
                        applied_prepared_seq: prepared.sequence,
                        credit,
                        settled_at_unix: cutoff + 1,
                    },
                },
            })
            .unwrap(),
        })
        .unwrap();
    let source_prefix = TailBinding::from(&Scanner::verify(&source_path).unwrap());
    let financial_prefix = TailBinding::from(&Scanner::verify(&paper_path).unwrap());
    let keys = h
        .paper
        .decision_pending_history()
        .unwrap()
        .into_iter()
        .map(|row| (row.source_trade_id, row.semantic_revision))
        .collect::<Vec<_>>();
    let digest = h
        .paper
        .seal_decision_evidence_for_source_prefix(&keys, &keys, source_prefix.last_sequence)
        .unwrap();
    let seal = pe_service::paper_recovery::QualificationSealed {
        start_receipt: start,
        source_prefix,
        financial_prefix,
        live_prefix: TailBinding::from(
            &pe_execution_core::LiveJournal::verified_tail(h.dir.path().join("live_journal.log"))
                .unwrap(),
        ),
        decision_evidence_digest: blake3::hash(&digest).to_hex().to_string(),
        sealed_cutoff_unix: cutoff,
        reason: pe_service::paper_recovery::SealReason::Complete,
    };
    let seal_receipt = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: 2,
            parser_version: 1,
            observed_at: SourceTimestamp(resolved_at),
            received_at: ReceivedAt(resolved_at),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::QualificationSealed(Box::new(seal)))
                .unwrap(),
        })
        .unwrap();
    drop(writer);
    let options = pe_service::qualification::QualifyOptions {
        paper_log: paper_path,
        source_log: source_path,
        live_journal: Some(h.dir.path().join("live_journal.log")),
        paper_state: h.dir.path().join("paper.db"),
        seal_hash: seal_receipt.this_hash.to_hex().to_string(),
        output: h.dir.path().join("resolved-qualification.json"),
    };
    pe_service::qualification::run_qualify(&options)
        .await
        .unwrap();
    let report: pe_service::qualification::QualificationReport =
        serde_json::from_slice(&std::fs::read(options.output).unwrap()).unwrap();
    assert!(report.replay.exact, "{:?}", report.reasons);
}

#[tokio::test]
async fn priced_completion_seal_rejects_altered_price_and_receipt() {
    let mut h = Harness::new().await;
    h.probability = pe_core_types::Probability::new(dec!(0.9)).unwrap();
    let recorded = h
        .record_with_rule(1, Some((dec!(0.50), dec!(0.50), Some(2))))
        .await;
    h.arm();
    h.attempt(&recorded, at());
    h.start(true);
    h.poll(&recorded).await;
    assert_eq!(
        h.terminal(&recorded).terminal_disposition.as_deref(),
        Some("fill")
    );
    assert!(h.qualify_one_fill().await.replay.exact);

    let original = h.dir.path().join("paper.log");
    let frames = Reader::replay(&original)
        .unwrap()
        .map(|frame| frame.unwrap().1)
        .collect::<Vec<_>>();
    for alter_price in [true, false] {
        let name = if alter_price {
            "wrong-price"
        } else {
            "wrong-receipt"
        };
        let paper_log = h.dir.path().join(format!("{name}.log"));
        let output = h.dir.path().join(format!("{name}.json"));
        let mut writer = Writer::open(&paper_log).unwrap();
        let mut seal_receipt = None;
        for envelope in &frames {
            let mut record: PaperLogRecord = serde_json::from_slice(&envelope.payload).unwrap();
            match &mut record {
                PaperLogRecord::PortfolioMark(mark) => {
                    assert_eq!(mark.prices.len(), 1);
                    if alter_price {
                        mark.prices[0].price = Some(pe_core_types::Price::new(dec!(0.51)).unwrap());
                    } else {
                        mark.prices[0].receipt.as_mut().unwrap().this_hash =
                            blake3::hash(b"wrong receipt");
                    }
                }
                PaperLogRecord::QualificationSealed(seal) => {
                    seal.financial_prefix =
                        TailBinding::from(&Scanner::verify(&paper_log).unwrap());
                }
                _ => {}
            }
            let appended = writer
                .append_synced(EnvelopeIn {
                    source_id: envelope.source_id.clone(),
                    schema_version: envelope.schema_version,
                    parser_version: envelope.parser_version,
                    observed_at: envelope.observed_at.clone(),
                    received_at: envelope.received_at.clone(),
                    content_type: envelope.content_type.clone(),
                    payload: serde_json::to_vec(&record).unwrap(),
                })
                .unwrap();
            if matches!(record, PaperLogRecord::QualificationSealed(_)) {
                seal_receipt = Some(appended);
            }
        }
        drop(writer);
        let options = pe_service::qualification::QualifyOptions {
            paper_log,
            source_log: h.dir.path().join("source.log"),
            live_journal: Some(h.dir.path().join("live_journal.log")),
            paper_state: h.dir.path().join("paper.db"),
            seal_hash: seal_receipt.unwrap().this_hash.to_hex().to_string(),
            output: output.clone(),
        };
        pe_service::qualification::run_qualify(&options)
            .await
            .unwrap();
        let report: pe_service::qualification::QualificationReport =
            serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap();
        assert_eq!(
            report.verdict,
            pe_service::qualification::QualificationVerdict::InsufficientEvidence
        );
        assert!(
            report
                .reasons
                .iter()
                .any(|reason| reason.contains("PortfolioMark")),
            "{:?}",
            report.reasons
        );
    }
}

struct StagedLiveVenue {
    posts: AtomicBool,
    checked_at: OffsetDateTime,
}

impl LiveOrderVenue for StagedLiveVenue {
    type Submission = ();

    fn prepare<'a>(
        &'a self,
        request: LiveVenuePrepareRequest,
    ) -> pe_execution_core::LiveVenuePrepareFuture<'a, Self::Submission> {
        Box::pin(async move {
            Ok(LiveVenuePrepared::new(
                PreparedPolymarketBuy {
                    condition_id: request.condition_id,
                    outcome_id: request.outcome_id,
                    token_id: request.token_id,
                    maker: "fixture-maker".to_owned(),
                    signer: "fixture-signer".to_owned(),
                    funder: "fixture-funder".to_owned(),
                    verifying_contract: "fixture-spender".to_owned(),
                    spender: "fixture-spender".to_owned(),
                    exchange_domain_version: 2,
                    neg_risk: request.neg_risk,
                    side: "BUY".to_owned(),
                    salt: "705".to_owned(),
                    timestamp_ms: 1,
                    expiration: "0".to_owned(),
                    maker_collateral: request.maximum_collateral,
                    taker_shares: request.shares,
                    limit_price: request.limit_price,
                    minimum_tick_size: request.tick_size,
                    signature_type: 3,
                    order_type: "FOK".to_owned(),
                    post_only: false,
                    defer_exec: false,
                    metadata: "0x00".to_owned(),
                    builder: "0x00".to_owned(),
                    order_hash: "fixture-order-hash".to_owned(),
                    post_body_hash: "fixture-body-hash".to_owned(),
                    sdk_version: "fixture".to_owned(),
                    sdk_archive_sha256: "fixture".to_owned(),
                    metadata_hashes: request.metadata_hashes,
                    worst_case_debit: request.maximum_collateral,
                },
                (),
            ))
        })
    }

    fn post_once<'a>(
        &'a self,
        _: Self::Submission,
        wall_clock_deadline: Option<OffsetDateTime>,
    ) -> pe_execution_core::LivePostFuture<'a> {
        Box::pin(async move {
            tokio::task::yield_now().await;
            if wall_clock_deadline.is_some_and(|deadline| self.checked_at > deadline) {
                return Ok(pe_core_types::RawPostAttempt::NotAttempted(
                    pe_core_types::ExpiredAt {
                        checked_at: self.checked_at,
                    },
                ));
            }
            assert!(!self.posts.swap(true, Ordering::SeqCst));
            Ok(pe_core_types::RawPostAttempt::Attempted(pe_core_types::RawHttpResponse {
                source_id: "polymarket-clob-v2".to_owned(),
                endpoint_kind: "order-post".to_owned(),
                method: "POST".to_owned(),
                path: "/order".to_owned(),
                ordered_query: Vec::new(),
                status: 200,
                headers: Vec::new(),
                body: br#"{"success":true,"orderID":"fixture-venue-order","makingAmount":"2.5","takingAmount":"5"}"#.to_vec(),
                attempt_ordinal: 1,
                source_at: None,
                observed_at: self.checked_at,
                received_at: self.checked_at,
                schema_version: 1,
                parser_version: 1,
                adapter_version: "fixture".to_owned(),
            }))
        })
    }

    fn classify_post_response(
        &self,
        _: &pe_core_types::RawHttpResponse,
    ) -> Result<LivePostClassification, LivePostParseError> {
        Ok(LivePostClassification::Matched {
            venue_order_id: "fixture-venue-order".to_owned(),
            executed: LiveExecutedAmounts {
                making_amount: dec!(2.5),
                taking_amount: dec!(5),
            },
            transaction_hashes: vec![format!("0x{}", "11".repeat(32))],
        })
    }

    fn reconcile_and_cancel_by_order_hash<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<LiveVenueReconciliation, LiveVenueReconciliationError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async { panic!("a classified POST must not reconcile") })
    }

    fn read_balance_and_allowance<'a>(
        &'a self,
        _: bool,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<LiveVenueAccountState, LiveVenueAccountReadError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            let cash = CollateralAmount::from_decimal_exact(CASH).unwrap();
            Ok(LiveVenueAccountState {
                observed_at: at(),
                closed_only: false,
                geoblocked: false,
                selected_spender: "fixture-spender".to_owned(),
                collateral_balance: cash,
                allowance: cash,
                reconciled_free_collateral: cash,
                schema_version: 1,
                parser_version: 1,
                evidence: Vec::new(),
                request_descriptor_hashes: Vec::new(),
            })
        })
    }
}

/// PASS: one orchestrator staging transaction freezes all three selected targets in the
/// recorded order, with one journal reference shared by the seed and paper decision.
#[tokio::test]
async fn owner_mode_stages_one_record_for_three_ordered_accounts() {
    let mut h = Harness::new().await;
    h.probability = pe_core_types::Probability::new(dec!(0.9)).unwrap();
    let recorded = h
        .record_with_rule(1, Some((dec!(0.50), dec!(0.50), Some(2))))
        .await;
    h.arm();
    let mut snapshot = h.live.snapshot().as_ref().clone();
    let mut lower = snapshot.accounts[0].clone();
    lower.account_id = AccountId::new("lower").unwrap();
    lower.is_primary = false;
    lower.enabled = false;
    lower.execution_order = 1;
    lower.credential_binding = Some((8, "lower-key".to_owned()));
    let mut higher = lower.clone();
    higher.account_id = AccountId::new("higher").unwrap();
    higher.execution_order = 2;
    higher.credential_binding = Some((9, "higher-key".to_owned()));
    snapshot.accounts.extend([lower, higher]);
    h.live.store(snapshot);

    h.attempt(&recorded, at());
    h.start(true);
    h.poll_source(&recorded, Some(EPOCH - 1), at()).await;
    h.barrier().await;
    let seeds = h.paper.unfinalized_ready_dispatch_seeds().unwrap();
    assert_eq!(seeds.len(), 1, "{:?}", h.terminal(&recorded));
    let seed = &seeds[0];
    let targets = h.paper.dispatch_targets(&seed.dispatch_id).unwrap();
    assert_eq!(targets.len(), 3);
    assert_eq!(
        targets
            .iter()
            .map(|target| target.account_id.as_str())
            .collect::<Vec<_>>(),
        ["stored-target", "lower", "higher"]
    );
    assert_eq!(
        targets
            .iter()
            .map(|target| target.exec_rank)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    let seq = serde_json::from_str::<Value>(&seed.signal_json).unwrap()["control_journal_seq"]
        .as_u64()
        .unwrap();
    let journal_path = h.dir.path().join("live_journal.log");
    let recorded_control = pe_execution_core::live_journal::staged_dispatch_control_at(
        &journal_path,
        seq,
        &seed.dispatch_id,
    )
    .unwrap()
    .unwrap();
    assert_eq!(recorded_control.targets.len(), 3);
    for (recorded_target, target) in recorded_control.targets.iter().zip(&targets) {
        assert_eq!(recorded_target.account_id.as_str(), target.account_id);
        assert_eq!(
            recorded_target.exec_rank,
            u64::try_from(target.exec_rank).unwrap()
        );
        assert_eq!(
            recorded_target.frozen_binding.version,
            target.credential_bundle_version
        );
        assert_eq!(
            recorded_target.frozen_binding.key_id,
            target.credential_key_id
        );
        assert_eq!(
            recorded_target.observed_credential_key_id,
            target.credential_key_id
        );
    }
    let staged_count = pe_execution_core::live_journal::replay_account(
        &journal_path,
        &AccountId::new("stored-target").unwrap(),
    )
    .unwrap()
    .into_iter()
    .filter(|event| {
        matches!(
            event.payload,
            pe_execution_core::LiveJournalPayload::StagedDispatchControl(_)
        )
    })
    .count();
    assert_eq!(staged_count, 1);
    let decision = h.terminal(&recorded);
    assert_eq!(
        pe_service::decision_replay::replay_decision_pending_with_control(
            &decision,
            &journal_path,
        )
        .unwrap()
        .post_boundary
        .body
        .terminal
        .dispatch_control_journal_seq,
        Some(seq)
    );
    h.stop().await;
}

/// PASS: a failure after the selected-target append leaves one replayable orphan attempt and
/// no committed seed or target that could be dispatched.
#[tokio::test]
async fn selected_target_append_before_failed_seed_write_is_uncommitted() {
    let mut h = Harness::new().await;
    h.probability = pe_core_types::Probability::new(dec!(0.9)).unwrap();
    let recorded = h
        .record_with_rule(1, Some((dec!(0.50), dec!(0.50), Some(2))))
        .await;
    h.arm();
    h.hooks.fail_next_stage_seed.store(true, Ordering::SeqCst);
    h.attempt(&recorded, at());
    h.start(true);
    h.poll_source(&recorded, Some(EPOCH - 1), at()).await;
    assert!(h.task.take().unwrap().await.unwrap().is_err());
    assert!(h.paper.pending_dispatch_seeds().unwrap().is_empty());
    assert!(
        h.paper
            .unfinalized_ready_dispatch_seeds()
            .unwrap()
            .is_empty()
    );
    let journal_path = h.dir.path().join("live_journal.log");
    let attempts = pe_execution_core::live_journal::replay_account(
        &journal_path,
        &AccountId::new("stored-target").unwrap(),
    )
    .unwrap()
    .into_iter()
    .filter(|event| {
        matches!(
            event.payload,
            pe_execution_core::LiveJournalPayload::StagedDispatchControl(_)
        )
    })
    .count();
    assert_eq!(attempts, 1);
}

#[tokio::test]
async fn live_mode_without_verified_binding_stages_no_control_record() {
    for metadata_unavailable in [false, true] {
        let mut h = Harness::new().await;
        h.probability = pe_core_types::Probability::new(dec!(0.9)).unwrap();
        let recorded = h
            .record_with_rule(1, Some((dec!(0.50), dec!(0.50), Some(2))))
            .await;
        h.arm();
        let mut snapshot = h.live.snapshot().as_ref().clone();
        if metadata_unavailable {
            snapshot.credential_metadata_available = false;
        } else {
            snapshot.accounts[0].credential_binding = None;
        }
        h.live.store(snapshot);
        h.attempt(&recorded, at());
        h.start(true);
        h.poll_source(&recorded, Some(EPOCH - 1), at()).await;
        h.barrier().await;
        assert!(h.paper.pending_dispatch_seeds().unwrap().is_empty());
        assert!(
            h.paper
                .unfinalized_ready_dispatch_seeds()
                .unwrap()
                .is_empty()
        );
        let events = pe_execution_core::live_journal::replay_account(
            h.dir.path().join("live_journal.log"),
            &AccountId::new("stored-target").unwrap(),
        )
        .unwrap();
        assert!(!events.iter().any(|event| matches!(
            event.payload,
            pe_execution_core::LiveJournalPayload::StagedDispatchControl(_)
        )));
        h.stop().await;
    }
}

/// One recorded websocket observation survives two empty REST reads, reaches one replayable
/// paper FinancialFinal/fill and one staged target, then exercises the wire-2 POST deadline at
/// equality and one nanosecond beyond it against the same identifier-bound path.
#[tokio::test(start_paused = true)]
async fn version_six_staged_target_strict_live_admission_submits_and_replays() {
    for beyond_deadline in [false, true] {
        let mut h = Harness::new().await;
        h.copy_budget_secs = 120;
        h.probability = pe_core_types::Probability::new(dec!(0.9)).unwrap();
        let recorded = h
            .record_with_rule(1, Some((dec!(0.50), dec!(0.50), Some(2))))
            .await;
        h.arm();
        h.attempt(&recorded, at());
        h.start(true);
        h.poll_source_with_misses(&recorded, Some(EPOCH - 1), at(), 2)
            .await;
        let row = h.terminal(&recorded);
        assert_eq!(row.terminal_disposition.as_deref(), Some("fill"));
        assert_eq!(h.paper.list_fills().unwrap().len(), 1);
        assert_eq!(
            scan_paper_log(&h.dir.path().join("paper.log"))
                .unwrap()
                .iter()
                .filter(|frame| matches!(
                    frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialFinal { .. })
                ))
                .count(),
            1
        );
        assert_bound_clocks(&h, &recorded, EPOCH - 1);
        let replayed = replay_decision_pending(&row).unwrap();
        assert_eq!(replayed.post_boundary.body.terminal.disposition, "fill");
        let continuation =
            pe_service::bucket_commit::DecisionContinuationV3::from_durable(&row).unwrap();
        assert_eq!(continuation.version(), 6);
        let staged = h.paper.unfinalized_ready_dispatch_seeds().unwrap();
        assert_eq!(staged.len(), 1);
        let target = h.paper.dispatch_targets(&staged[0].dispatch_id).unwrap();
        assert_eq!(target.len(), 1);
        assert_eq!(target[0].account_id, "stored-target");
        assert_eq!(target[0].credential_bundle_version, 7);
        h.stop().await;

        let gamma = source_envelope(&h, recorded.admission.receipts.gamma).payload;
        let delayed = source_envelope(&h, recorded.admission.receipts.clob_long).payload;
        let condition = &recorded.admission.market.condition_id;
        let venue = StagedLiveVenue {
            posts: AtomicBool::new(false),
            checked_at: OffsetDateTime::from_unix_timestamp(
                EPOCH - 1 + i64::try_from(h.copy_budget_secs).unwrap(),
            )
            .unwrap()
                + time::Duration::nanoseconds(i64::from(beyond_deadline)),
        };
        assert_eq!(
            validate_live_market(&gamma, &delayed, condition, EPOCH, 60),
            Err(pe_source_polymarket_public::LiveMarketError::NonzeroDelay)
        );
        assert!(
            !venue.posts.load(Ordering::SeqCst),
            "strict refusal precedes POST"
        );
        let mut strict_long: Value = serde_json::from_slice(&delayed).unwrap();
        strict_long["seconds_delay"] = 0.into();
        let strict_long = serde_json::to_vec(&strict_long).unwrap();
        let strict_market =
            validate_live_market(&gamma, &strict_long, condition, EPOCH, 60).unwrap();
        let compact = source_envelope(&h, recorded.admission.receipts.clob_compact).payload;
        let admission = LiveAdmissionArtifact {
            market: strict_market,
            fee_schedule: recorded.admission.fee_schedule,
            receipts: AdmissionReceipts {
                gamma: h.append("polymarket.gamma.markets", &gamma).await,
                clob_long: h.append("polymarket.clob.markets", &strict_long).await,
                clob_compact: h.append("polymarket.clob.compact-market", &compact).await,
            },
            settlement: VenueSettlementRecord {
                raw_evidence_hash: blake3::hash(&strict_long).to_hex().to_string(),
                ..recorded.admission.settlement.clone()
            },
        };
        let paper_economic = scan_paper_log(&h.dir.path().join("paper.log"))
            .unwrap()
            .into_iter()
            .find_map(|frame| match frame.frame {
                PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                    payload: pe_service::paper_recovery::FinancialPayload::Fill { economic, .. },
                    ..
                }) => Some(economic),
                _ => None,
            })
            .unwrap();
        assert_eq!(paper_economic.version, 2);
        let mut economic = paper_economic.clone();
        economic.admission = pe_execution_core::LiveAdmissionArtifactAudit::new(
            &admission.market,
            &admission.settlement,
            admission.fee_schedule,
            admission.receipts,
        );
        assert_eq!(economic.version, 2);
        assert_eq!(economic.balance.chase_ceiling, pe_core_types::Price::ONE);
        assert_eq!(
            economic.balance.price_impact_cap_bps,
            h.config.price_impact_cap_bps
        );
        assert_eq!(economic.observation, paper_economic.observation);
        let ladder = LadderPlan {
            used_asks: economic
                .ladder
                .used_asks
                .iter()
                .map(|ask| AskLevel {
                    price: ask.price,
                    shares: ask.shares,
                })
                .collect(),
            best_ask: economic.ladder.best_ask,
            limit_price: economic.ladder.limit_price,
            shares: economic.ladder.minimum_shares,
            worst_case_debit: economic.ladder.principal,
        };
        let account_id = AccountId::new("stored-target").unwrap();
        let binding = CredentialBindingIdentity {
            version: 7,
            key_id: "stored-key".to_owned(),
        };
        let identity = LiveOrderIdentity {
            dispatch_id: staged[0].dispatch_id.clone(),
            idempotency_key: LiveOrderIdentity::idempotency_key_for(
                &staged[0].dispatch_id,
                &account_id,
            ),
            quote_id: "fresh-strict-live-quote".to_owned(),
            config_hash: economic.applied_configuration_hash.clone(),
            decision_hash: row.semantic_revision.clone(),
            evidence_hashes: vec![admission.settlement.raw_evidence_hash.clone()],
            fill_projection: Some(Box::new(pe_execution_core::LiveFillProjectionIdentity {
                leader_wallet: wallet().to_string(),
                source_trade_id: Some(recorded.id.0.clone()),
                market_id: economic.market.market_id.clone(),
                outcome_id: i64::from(u16::from(economic.market.outcome_index)),
                side: "buy".to_owned(),
            })),
            schema_version: 1,
            parser_version: 1,
        };
        let request = LiveOrderRequest {
            target: FrozenLiveTarget {
                account_id: account_id.clone(),
                credential_binding: binding.clone(),
            },
            current_credential_binding: binding,
            mode: LiveModeSnapshot {
                requested: LiveControlMode::LiveTiny,
                effective: LiveControlMode::LiveTiny,
            },
            identity,
            condition_id: condition.clone(),
            outcome_id: pe_core_types::OutcomeId(0),
            token_id: admission.market.ordered_outcome_token_ids[0].clone(),
            admission,
            ladder,
            economic,
        };
        let path = h.dir.path().join("live_journal.log");
        let journal = LiveJournal::open(&path).unwrap();
        let executor = LiveExecutor::new(&venue, &journal);
        let prepared = match executor.prepare_with_clock(request, at).await.unwrap() {
            LivePrepareResult::Prepared(prepared) => prepared,
            LivePrepareResult::Terminal(outcome) => {
                panic!("fresh strict live target refused: {outcome:?}")
            }
        };
        let deadline = OffsetDateTime::from_unix_timestamp(
            EPOCH - 1 + i64::try_from(h.copy_budget_secs).unwrap(),
        )
        .unwrap();
        let outcome = executor
            .submit_with_clock_and_deadline(prepared, || venue.checked_at, Some(deadline))
            .await
            .unwrap();
        if beyond_deadline {
            assert_eq!(outcome.terminal_reason(), Some("copy_expired_before_post"));
            assert!(!venue.posts.load(Ordering::SeqCst));
        } else {
            assert!(matches!(outcome, LiveOrderOutcome::Matched { .. }));
            assert!(venue.posts.load(Ordering::SeqCst));
        }
        let events = pe_execution_core::live_journal::replay_account(&path, &account_id).unwrap();
        assert!(matches!(
            events.first().map(|event| &event.payload),
            Some(LiveJournalPayload::StagedDispatchControl(_))
        ));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event.payload,
                    LiveJournalPayload::StagedDispatchControl(_)
                ))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.payload, LiveJournalPayload::OrderPosted(_)))
                .count(),
            usize::from(!beyond_deadline)
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(
                    event.payload,
                    LiveJournalPayload::OrderPreparationFailed(_)
                ))
                .count(),
            usize::from(beyond_deadline)
        );
        assert!(events.iter().all(|event| match &event.payload {
            LiveJournalPayload::AdmissionEvaluated(value) =>
                value.identity.dispatch_id == staged[0].dispatch_id,
            LiveJournalPayload::OrderPrepared(value) =>
                value.identity.dispatch_id == staged[0].dispatch_id,
            LiveJournalPayload::OrderPosted(value) =>
                value.identity.dispatch_id == staged[0].dispatch_id,
            LiveJournalPayload::OrderPreparationFailed(value) =>
                value.identity.dispatch_id == staged[0].dispatch_id,
            _ => true,
        }));
        h.stop().await;
    }
}

/// PASS: the frozen exact boundary fills; one nanosecond later expires, with the predicate's exact clock.
#[tokio::test]
async fn paper_prepared_gate_preserves_nanosecond_boundary() {
    for (budget, remainder) in [(2, 0), (2, 1), (120, 0), (120, 1)] {
        let mut h = Harness::new().await;
        h.copy_budget_secs = budget;
        let recorded = h.record(1).await;
        let gate = at()
            + time::Duration::seconds(i64::try_from(budget).unwrap())
            + time::Duration::nanoseconds(remainder);
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
        } else {
            let report = h.qualify_one_fill().await;
            assert!(report.replay.exact, "{:?}", report.reasons);
            assert_eq!(report.replay.fills, 1);
            assert_eq!(
                report.verdict,
                pe_service::qualification::QualificationVerdict::Fail
            );
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
        h.copy_budget_secs = 120;
        h.attempt(&recorded, at() + time::Duration::seconds(3));
        if !enabled {
            *h.hooks.age_clock.lock().unwrap() = [at() + time::Duration::seconds(3); 3].into();
        }
        h.start(!enabled);
        h.barrier().await;
        assert_eq!(h.prepared_count(), usize::from(!enabled));
        let replayed = replay_decision_pending(&h.terminal(&recorded)).unwrap();
        let frozen = replayed.continuation.facts.paper_freshness_policy.unwrap();
        assert_eq!(frozen.activity_ws_enabled, enabled);
        assert_eq!(frozen.copy_latency_budget_secs, 2);
        if enabled {
            assert_expired(&h, &recorded);
        } else {
            assert_eq!(replayed.post_boundary.body.terminal.disposition, "fill");
        }
    }
}

/// A stopped-cut boot finishes a continuation-five decision against its active historical
/// latency cause. A later continuation-six decision in the same generation ignores that cause.
#[tokio::test]
async fn mixed_era_boot_keeps_old_latency_audit_and_new_decision_false() {
    let mut h = Harness::new_with_semantic(1).await;
    let old = h.record(1).await;
    h.freeze(&old, true).await;
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    let row = h.terminal(&old);
    let mut frozen: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
    assert_eq!(frozen["version"], json!(6));
    frozen["version"] = json!(5);
    connection
        .execute(
            "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
            rusqlite::params![frozen.to_string(), old.id.0],
        )
        .unwrap();
    let paper_path = h.dir.path().join("paper.log");
    Writer::open(&paper_path)
        .unwrap()
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: 2,
            parser_version: 1,
            observed_at: SourceTimestamp(at()),
            received_at: ReceivedAt(at()),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::RiskHaltChanged {
                owner: pe_service::paper_recovery::RiskHaltOwner::Paper,
                cause: pe_risk_engine::RiskHaltCause::CopyLatency,
                state: pe_service::paper_recovery::HaltState::Engaged,
                evidence: json!({"fixture": "pre-cutover"}),
            })
            .unwrap(),
        })
        .unwrap();
    h.attempt(&old, at());
    h.start(true);
    h.barrier().await;
    let old_row = h.terminal(&old);
    let old_replay = replay_decision_pending(&old_row).unwrap();
    assert_eq!(old_replay.continuation.version(), 5);
    assert_eq!(old_replay.post_boundary.financial_semantic_version, 1);
    let old_economic = old_replay
        .post_boundary
        .body
        .terminal
        .decline
        .as_ref()
        .and_then(|decline| match &decline.inputs {
            pe_service::decision_replay::WinnerFollowDecisionInputs::Evaluated { economic } => {
                Some(economic)
            }
            _ => None,
        })
        .unwrap();
    assert!(old_economic.risk.snapshot.copy_latency_kill_switch_active);
    assert!(matches!(
        old_economic.risk.decision,
        pe_execution_core::RiskDecisionAudit::Blocked {
            reason: pe_risk_engine::RiskBlock::CopyLatencyKillSwitch
        }
    ));
    let old_hash = old_replay.post_boundary.document_blake3.clone();
    let old_bytes = old_row.post_commit_inputs_json.clone();
    h.stop().await;

    let source_prefix =
        TailBinding::from(&pe_event_log::Scanner::verify(h.dir.path().join("source.log")).unwrap());
    let keys = vec![(
        old_row.source_trade_id.clone(),
        old_row.semantic_revision.clone(),
    )];
    let digest = h
        .paper
        .seal_decision_evidence_for_source_prefix(&keys, &keys, source_prefix.last_sequence)
        .unwrap();
    let seal = pe_service::paper_recovery::QualificationSealed {
        start_receipt: h.authority.inner.lock().unwrap().start,
        source_prefix,
        financial_prefix: TailBinding::from(&pe_event_log::Scanner::verify(&paper_path).unwrap()),
        live_prefix: TailBinding::from(
            &LiveJournal::verified_tail(h.dir.path().join("live_journal.log")).unwrap(),
        ),
        decision_evidence_digest: blake3::hash(&digest).to_hex().to_string(),
        sealed_cutoff_unix: EPOCH,
        reason: pe_service::paper_recovery::SealReason::InsufficientEvidence(
            "financial semantic version changed".to_owned(),
        ),
    };
    Writer::open(&paper_path)
        .unwrap()
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: 2,
            parser_version: 1,
            observed_at: SourceTimestamp(at()),
            received_at: ReceivedAt(at()),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::QualificationSealed(Box::new(seal)))
                .unwrap(),
        })
        .unwrap();

    h.copy_budget_secs = 120;
    let current = h.record(2).await;
    h.attempt(&current, at() + time::Duration::seconds(1));
    h.start(true);
    h.poll(&current).await;
    let current_replay = replay_decision_pending(&h.terminal(&current)).unwrap();
    assert_eq!(current_replay.continuation.version(), 6);
    assert_eq!(current_replay.post_boundary.financial_semantic_version, 2);
    let current_economic = scan_paper_log(&paper_path)
        .unwrap()
        .iter()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                payload: pe_service::paper_recovery::FinancialPayload::Fill { economic, .. },
                ..
            }) if economic.version == 2 => Some(economic.clone()),
            _ => None,
        })
        .unwrap();
    assert!(
        !current_economic
            .risk
            .snapshot
            .copy_latency_kill_switch_active
    );
    let preserved = h.terminal(&old);
    assert_eq!(preserved.post_commit_inputs_json, old_bytes);
    assert_eq!(
        replay_decision_pending(&preserved)
            .unwrap()
            .post_boundary
            .document_blake3,
        old_hash
    );
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
async fn continuation_six_policy_and_clock_shape_rejects_invalid_evidence() {
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
    let mut historical = DecisionPostBoundaryEvidence::from_body(body).unwrap();
    historical.financial_semantic_version = 1;
    historical.document_blake3 =
        blake3::hash(&serde_json::to_vec(&(1u32, &historical.body)).unwrap())
            .to_hex()
            .to_string();
    let json = serde_json::to_string(&historical).unwrap();
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
async fn staged_generation_six_crash_between_staging_and_outcome_converges_once() {
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

/// PASS: an actual boot bracket fence releases a staged seed with the same refusal in either
/// recovery order; a failed terminal transaction preserves both owners until restart.
#[tokio::test]
async fn boot_fence_completes_staged_handoff_atomically_in_both_recovery_orders() {
    for recovery_first in [true, false] {
        for fail_terminal in [false, true] {
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
                tokio::select! { biased; _ = gate.started.notified() => {}, _ = &mut pending => panic!("copy completed before staging crash") }
            }
            h.stop().await;
            let open = h.terminal(&recorded);
            assert_eq!(open.state, DecisionPendingState::Open);
            let seed = h.paper.pending_dispatch_seeds().unwrap().remove(0);
            let targets = h.paper.dispatch_targets(&seed.dispatch_id).unwrap();
            let cash = h.paper.bankroll().unwrap();
            let positions = h.paper.open_positions().unwrap();
            let history = h.paper.gate_history().unwrap();
            assert_eq!(h.prepared_count(), 1);

            // The same source group with a changed semantic revision fences during boot's
            // first activity read, through the real validator and bucket commit owner.
            let mut changed: Value = serde_json::from_slice(&held.activity).unwrap();
            changed[0]["size"] = "11".into();
            let validator = pe_service::position_seeder::CausalPositionValidator::new(
                Arc::new(Page(serde_json::to_vec(&changed).unwrap())),
                "fixture://activity",
                "prepared-freshness",
                Arc::new(AssetIdentityResolver::new_runtime(
                    Arc::new(Page(held.gamma.clone())),
                    "fixture://gamma".to_owned(),
                    GAMMA_BATCH_SIZE,
                    h.source.clone(),
                )),
            )
            .with_clock(Arc::new(|| EPOCH + 10));
            let mut engine =
                BucketCommitEngine::load(h.paper.clone(), build_leader_ledger(&h.paper).unwrap())
                    .unwrap();
            assert!(
                validator
                    .validate_direct(&[wallet()], &mut engine, &h.paper)
                    .await
                    .unwrap()
                    .is_empty()
            );
            assert!(h.paper.is_wallet_fenced(&wallet()).unwrap());
            assert_eq!(h.terminal(&recorded), open);

            let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
            connection.execute_batch("CREATE TABLE fence_transitions (kind TEXT NOT NULL);
                CREATE TRIGGER count_fence_seed AFTER UPDATE OF state ON dispatch_seeds WHEN OLD.state = 'pending_paper' AND NEW.state = 'ready' BEGIN INSERT INTO fence_transitions VALUES ('seed'); END;
                CREATE TRIGGER count_fence_terminal AFTER UPDATE OF state ON decision_pending WHEN OLD.state = 'open' AND NEW.state = 'terminal' BEGIN INSERT INTO fence_transitions VALUES ('terminal'); END;").unwrap();
            if fail_terminal {
                connection.execute_batch("CREATE TRIGGER fail_fence_terminal BEFORE UPDATE ON decision_pending WHEN NEW.state = 'terminal' BEGIN SELECT RAISE(FAIL, 'injected fence terminal failure'); END;").unwrap();
            }
            let recover_seeds = || {
                pe_service::dispatch_recovery::resume_dispatch_seeds(
                    &h.dir.path().join("paper.log"),
                    &h.paper,
                )
                .unwrap()
            };
            if recovery_first {
                let recovery = recover_seeds();
                assert_eq!(recovery.left_pending, 1);
                assert_eq!(recovery.finalized_stuck, 0);
                assert_eq!(recovery.flipped_fill, 0);
            }
            h.hooks.age_clock.lock().unwrap().clear();
            h.hooks.age_clock.lock().unwrap().push_back(at());
            h.start(false);
            if fail_terminal {
                let error = h.task.take().unwrap().await.unwrap().unwrap_err();
                assert!(matches!(
                    error,
                    pe_service::orchestrator::OrchestratorRunError::PendingRecovery(_)
                ));
            } else {
                h.boot_barrier().await;
            }
            h.stop().await;
            if fail_terminal {
                assert_eq!(h.terminal(&recorded), open);
                assert_eq!(
                    h.paper.dispatch_seed(&seed.dispatch_id).unwrap().unwrap(),
                    seed
                );
                assert!(h.paper.no_copy_disposition(&recorded.id).unwrap().is_none());
                let count: usize = connection
                    .query_row("SELECT count(*) FROM fence_transitions", [], |row| {
                        row.get(0)
                    })
                    .unwrap();
                assert_eq!(count, 0, "the seed flip rolls back with terminalization");
                let recovery = pe_service::dispatch_recovery::resume_dispatch_seeds(
                    &h.dir.path().join("paper.log"),
                    &h.paper,
                )
                .unwrap();
                assert_eq!(recovery.left_pending, 1);
                assert_eq!(
                    h.paper.dispatch_targets(&seed.dispatch_id).unwrap(),
                    targets
                );
                connection
                    .execute_batch("DROP TRIGGER fail_fence_terminal;")
                    .unwrap();
                h.hooks.age_clock.lock().unwrap().push_back(at());
                h.start(false);
                h.boot_barrier().await;
                h.stop().await;
            }
            let terminal = h.terminal(&recorded);
            let replayed = replay_decision_pending(&terminal).unwrap();
            assert_eq!(
                terminal.terminal_disposition.as_deref(),
                Some("no_copy:wallet_fenced_before_dispatch")
            );
            assert_eq!(
                replayed.post_boundary.body.terminal.reason,
                "wallet_fenced_before_dispatch"
            );
            assert_eq!(
                replayed.post_boundary.body.terminal.dispatch_id.as_deref(),
                Some(seed.dispatch_id.as_str())
            );
            assert_eq!(
                h.paper
                    .no_copy_disposition(&recorded.id)
                    .unwrap()
                    .unwrap()
                    .2,
                "wallet_fenced_before_dispatch"
            );
            let ready = h.paper.dispatch_seed(&seed.dispatch_id).unwrap().unwrap();
            assert_eq!(ready.state, "ready");
            assert_eq!(
                ready.paper_outcome.as_deref(),
                Some("no_fill:wallet_fenced_before_dispatch")
            );
            for _ in 0..2 {
                let recovery = pe_service::dispatch_recovery::resume_dispatch_seeds(
                    &h.dir.path().join("paper.log"),
                    &h.paper,
                )
                .unwrap();
                assert_eq!(
                    (
                        recovery.left_pending,
                        recovery.flipped_fill,
                        recovery.finalized_stuck
                    ),
                    (0, 0, 0)
                );
                h.start(false);
                h.boot_barrier().await;
                h.stop().await;
                assert_eq!(h.terminal(&recorded), terminal);
                assert_eq!(
                    h.paper.dispatch_seed(&seed.dispatch_id).unwrap().unwrap(),
                    ready
                );
            }
            assert_eq!(
                h.paper.dispatch_targets(&seed.dispatch_id).unwrap(),
                targets
            );
            assert_eq!(h.paper.gate_history().unwrap(), history);
            assert_eq!(h.paper.bankroll().unwrap(), cash);
            assert_eq!(h.paper.open_positions().unwrap(), positions);
            assert_eq!(h.prepared_count(), 1);
            assert_eq!(h.authority.inner.lock().unwrap().fills.len(), 1);
            for kind in ["seed", "terminal"] {
                let count: usize = connection
                    .query_row(
                        "SELECT count(*) FROM fence_transitions WHERE kind = ?1",
                        [kind],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(
                    count, 1,
                    "{kind}, recovery_first={recovery_first}, fail_terminal={fail_terminal}"
                );
            }
        }
    }
}

/// PASS: resumed generations 2/3/4 execute the legacy initial gate with identical terminal
/// bytes, including its original millisecond-only clock and hash; no new Prepared gate appears.
#[tokio::test]
async fn resumed_legacy_generations_keep_initial_gate_terminal_bytes() {
    let body = r#"{"version":4,"owners":["source_log","paper_log"],"source_trade_id":"g2:fill","applied_configuration_hash":"f602cee694f90f8e48cdd43e70d6d9398879a9991662492af82ec4f7df31b222","market_end":null,"market_price":null,"book":null,"clocks":[{"purpose":"initial_staleness_gate","unix_millis":1700000003000},{"purpose":"terminal_transition","unix_millis":1800000000000}],"authority":{"kind":"not_read","outcome":"terminal_before_fill_authority","bankroll":null},"terminal":{"disposition":"no_copy:stale_fallback_past_copy_budget","reason":"stale_fallback_past_copy_budget","fill":null,"dispatch_id":null}}"#;
    let hash = blake3::hash(format!("[1,{body}]").as_bytes())
        .to_hex()
        .to_string();
    let expected = format!(
        "{},\"financial_semantic_version\":1,\"document_blake3\":\"{hash}\"}}",
        body.strip_suffix('}').unwrap()
    );
    for version in [2, 3, 4] {
        let mut h = Harness::new().await;
        let wire = support::legacy_continuation_wire(version);
        let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
        connection.execute("INSERT INTO decision_pending (source_trade_id, semantic_revision, wallet_hex, source_epoch, frozen_inputs_json, post_commit_inputs_json, state, updated_at_unix) VALUES ('g2:fill', 'semantic-v2', ?1, 1700000000, ?2, '[]', 'open', 1700000000)", rusqlite::params![wallet().to_string(), wire.to_string()]).unwrap();
        h.hooks
            .age_clock
            .lock()
            .unwrap()
            .push_back(OffsetDateTime::from_unix_timestamp(1_700_000_003).unwrap());
        h.start(true);
        h.boot_barrier().await;
        h.stop().await;
        let row = h
            .paper
            .decision_pending_for(&SourceTradeId("g2:fill".to_owned()))
            .unwrap()
            .unwrap();
        assert_eq!(
            row.post_commit_inputs_json, expected,
            "generation {version}"
        );
        assert_eq!(row.frozen_inputs_json, wire.to_string());
        let replayed = replay_decision_pending(&row).unwrap();
        assert_eq!(replayed.continuation.version(), version);
        assert_eq!(h.prepared_count(), 0);
        assert!(h.authority.inner.lock().unwrap().fills.is_empty());
        h.start(false);
        h.boot_barrier().await;
        h.stop().await;
        assert_eq!(
            h.paper
                .decision_pending_for(&row.source_trade_id)
                .unwrap()
                .unwrap(),
            row
        );
    }
}

struct AdmissionResponses {
    bodies: [Vec<u8>; 3],
    started: mpsc::Sender<usize>,
    release: [tokio::sync::Semaphore; 3],
}

async fn admission_response(
    axum::extract::State(responses): axum::extract::State<Arc<AdmissionResponses>>,
    uri: axum::http::Uri,
) -> Vec<u8> {
    let index = if uri.path() == "/markets" {
        0
    } else if uri.path().starts_with("/clob-markets/") {
        2
    } else {
        1
    };
    responses.started.send(index).await.unwrap();
    responses.release[index].acquire().await.unwrap().forget();
    responses.bodies[index].clone()
}

impl Harness {
    async fn qualify_one_fill(&mut self) -> pe_service::qualification::QualificationReport {
        let source_path = self.dir.path().join("source.log");
        let scans_before_mark = pe_event_log::scan_metrics::count(&source_path).unwrap();
        let cutoff = EPOCH - EPOCH.rem_euclid(86_400) + 86_400;
        let timestamp = OffsetDateTime::from_unix_timestamp(cutoff).unwrap();
        let boundary = self
            .source
            .append(EnvelopeIn {
                source_id: SourceId("pe-service.boundary".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(
                    &json!({"kind":"daily_boundary", "cutoff_unix":cutoff}),
                )
                .unwrap(),
            })
            .await
            .unwrap();
        let price = self
            .source
            .append(EnvelopeIn {
                source_id: SourceId("pe-service.clob-prices-history".to_owned()),
                schema_version: 1,
                parser_version: 1,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&json!({"history":[{"t":cutoff,"p":"0.50"}]})).unwrap(),
            })
            .await
            .unwrap();
        self.hooks.boundary_mark_prices.lock().unwrap().push_back(
            pe_service::risk_inputs::HistoricalMarkPrice {
                price: pe_core_types::Price::new(dec!(0.50)).unwrap(),
                sample_unix: cutoff,
                receipt: price,
            },
        );
        self.hooks
            .financial_clock_unix
            .store(cutoff, Ordering::SeqCst);
        let (acknowledged, receiver) = tokio::sync::oneshot::channel();
        self.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::DailyBoundary {
                cutoff_unix: cutoff,
                boundary_receipt: boundary,
                acknowledged,
            })
            .await
            .unwrap();
        receiver.await.unwrap().unwrap();
        assert_eq!(
            pe_event_log::scan_metrics::count(&source_path).unwrap(),
            scans_before_mark
        );
        let mark = scan_paper_log(&self.dir.path().join("paper.log"))
            .unwrap()
            .into_iter()
            .find_map(|frame| match frame.frame {
                PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => Some(mark),
                _ => None,
            })
            .unwrap();
        assert_eq!(mark.prices.len(), 1);
        assert!(mark.prices[0].receipt.is_some());
        let mut verified_mark = *mark.clone();
        verified_mark.source_tail =
            TailBinding::from(&pe_event_log::Scanner::verify(&source_path).unwrap());
        assert_eq!(
            serde_json::to_vec(&*mark).unwrap(),
            serde_json::to_vec(&verified_mark).unwrap()
        );
        self.stop().await;
        let paper_path = self.dir.path().join("paper.log");
        let source_path = self.dir.path().join("source.log");
        let live_path = self.dir.path().join("live_journal.log");
        let source_prefix =
            TailBinding::from(&pe_event_log::Scanner::verify(&source_path).unwrap());
        let keys = self
            .paper
            .decision_pending_history()
            .unwrap()
            .into_iter()
            .map(|row| (row.source_trade_id, row.semantic_revision))
            .collect::<Vec<_>>();
        let digest = self
            .paper
            .seal_decision_evidence_for_source_prefix(&keys, &keys, source_prefix.last_sequence)
            .unwrap();
        let seal = pe_service::paper_recovery::QualificationSealed {
            start_receipt: self.authority.inner.lock().unwrap().start,
            source_prefix,
            financial_prefix: TailBinding::from(
                &pe_event_log::Scanner::verify(&paper_path).unwrap(),
            ),
            live_prefix: TailBinding::from(
                &pe_execution_core::LiveJournal::verified_tail(&live_path).unwrap(),
            ),
            decision_evidence_digest: blake3::hash(&digest).to_hex().to_string(),
            sealed_cutoff_unix: cutoff,
            reason: pe_service::paper_recovery::SealReason::Complete,
        };
        // Seal this short fixture explicitly so the complete verifier reaches performance gates.
        // Exact replay must succeed; one fill cannot satisfy the promotion sample requirements.
        let receipt = Writer::open(&paper_path)
            .unwrap()
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: 2,
                parser_version: 1,
                observed_at: SourceTimestamp(timestamp),
                received_at: ReceivedAt(timestamp),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&PaperLogRecord::QualificationSealed(Box::new(seal)))
                    .unwrap(),
            })
            .unwrap();
        let options = pe_service::qualification::QualifyOptions {
            paper_log: paper_path,
            source_log: source_path,
            live_journal: Some(live_path),
            paper_state: self.dir.path().join("paper.db"),
            seal_hash: receipt.this_hash.to_hex().to_string(),
            output: self.dir.path().join("qualification.json"),
        };
        pe_service::qualification::run_qualify(&options)
            .await
            .unwrap();
        serde_json::from_slice(&std::fs::read(options.output).unwrap()).unwrap()
    }
}

async fn hold_admission_responses(
    h: &mut Harness,
    recorded: &Recorded,
    source: SourceLogHandle,
) -> (
    Arc<AdmissionResponses>,
    mpsc::Receiver<usize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (started, requests) = mpsc::channel(3);
    let responses = Arc::new(AdmissionResponses {
        bodies: [
            recorded.admission.receipts.gamma,
            recorded.admission.receipts.clob_long,
            recorded.admission.receipts.clob_compact,
        ]
        .map(|receipt| source_envelope(h, receipt).payload),
        started,
        release: std::array::from_fn(|_| tokio::sync::Semaphore::new(0)),
    });
    let router = axum::Router::new()
        .fallback(axum::routing::get(admission_response))
        .with_state(responses.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    h.admission_builder = Some(
        LiveAdmissionBuilder::new(reqwest::Client::new(), &base, &base, source)
            .with_clock(Arc::new(at)),
    );
    (responses, requests, server)
}

/// PASS: a real admission GET held from T+1.5 to T+2.5 cannot stage a live target when
/// history T+1 is bound to stream T, and the terminal retains the shared stale clocks.
#[tokio::test]
async fn bound_source_clock_held_admission_get_expires_shared_dispatch() {
    let mut h = Harness::new().await;
    let recorded = h.record(2).await;
    let source = h.source.clone();
    let (responses, mut requests, server) =
        hold_admission_responses(&mut h, &recorded, source).await;
    let initial_at = at() + time::Duration::milliseconds(1500);
    let dispatch_at = at() + time::Duration::milliseconds(2500);
    h.arm();
    h.hooks.age_clock.lock().unwrap().push_back(initial_at);
    h.start(true);
    {
        let pending = h.poll_source(&recorded, Some(EPOCH), initial_at);
        tokio::pin!(pending);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..3 {
            tokio::select! {
                request = requests.recv() => { seen.insert(request.unwrap()); },
                _ = &mut pending => panic!("decision completed before admission barrier"),
            }
        }
        assert_eq!(seen, std::collections::BTreeSet::from([0, 1, 2]));
        assert_bound_clocks(&h, &recorded, EPOCH);
        assert_eq!(h.prepared_count(), 0);
        assert!(h.paper.pending_dispatch_seeds().unwrap().is_empty());
        h.hooks.age_clock.lock().unwrap().push_back(dispatch_at);
        for release in &responses.release {
            release.add_permits(1);
        }
        pending.await;
    }
    server.abort();
    let _ = server.await;
    assert_shared_stale(&h, &recorded, initial_at, Some(dispatch_at));
}

/// PASS: three real GETs overlap and complete compact/long/Gamma; the resulting named receipts
/// flow through the orchestrator's financial fill and exact sealed qualification replay.
#[tokio::test]
async fn reverse_completion_admission_executes_and_qualifies_exactly() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    let (admission_source, mut captures) = SourceLogHandle::channel(3);
    let (responses, mut requests, server) =
        hold_admission_responses(&mut h, &recorded, admission_source).await;
    h.hooks.age_clock.lock().unwrap().extend([at(); 3]);
    assert!(h.hooks.admission_artifacts.lock().unwrap().is_empty());
    h.start(true);
    let receipts = {
        let pending = h.poll(&recorded);
        tokio::pin!(pending);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..3 {
            tokio::select! {
                request = requests.recv() => { seen.insert(request.unwrap()); },
                _ = &mut pending => panic!("financial decision completed before all admission requests started"),
            }
        }
        assert_eq!(seen, std::collections::BTreeSet::from([0, 1, 2]));
        let mut receipts = Vec::new();
        for (request, source) in [
            (2, "polymarket.clob.compact-market"),
            (1, "polymarket.clob.markets"),
            (0, "polymarket.gamma.markets"),
        ] {
            responses.release[request].add_permits(1);
            let (envelope, acknowledged) =
                tokio::time::timeout(std::time::Duration::from_secs(5), captures.recv_for_test())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(envelope.source_id.0, source);
            assert_eq!(envelope.payload, responses.bodies[request]);
            let receipt = h.source.append(envelope).await.unwrap();
            assert_eq!(
                source_envelope(&h, receipt).payload,
                responses.bodies[request]
            );
            acknowledged.send(receipt).unwrap();
            receipts.push(receipt);
        }
        pending.await;
        receipts
    };
    server.abort();
    let row = h.terminal(&recorded);
    assert_eq!(row.terminal_disposition.as_deref(), Some("fill"));
    assert_eq!(h.prepared_count(), 1);
    assert_eq!(h.authority.inner.lock().unwrap().fills.len(), 1);
    let frames = scan_paper_log(&h.dir.path().join("paper.log")).unwrap();
    let economic = frames
        .iter()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                payload: pe_service::paper_recovery::FinancialPayload::Fill { economic, .. },
                ..
            }) => Some(economic),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        [
            economic.admission.receipts.clob_compact,
            economic.admission.receipts.clob_long,
            economic.admission.receipts.gamma
        ],
        receipts.as_slice()
    );
    let core_hash = economic.core_hash().unwrap();
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{:?}", report.reasons);
    assert_eq!(
        (
            report.replay.financial_prepared,
            report.replay.financial_final,
            report.replay.decisions,
            report.replay.fills
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(report.evidence.economic_core_hashes, vec![core_hash]);
    assert_eq!(
        report.verdict,
        pe_service::qualification::QualificationVerdict::Fail
    );
}

fn source_envelope(h: &Harness, receipt: AppendReceipt) -> pe_event_log::EventEnvelope {
    let (_, envelope) = pe_event_log::Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(Result::unwrap)
        .find(|(sequence, _)| *sequence == receipt.sequence)
        .unwrap();
    assert_eq!(envelope.this_hash, receipt.this_hash);
    envelope
}
