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
use pe_service::trade_poller::{TradePoller, TradePollerConfig};
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
struct Books {
    values: Mutex<HashMap<String, OrderBook>>,
    gate: Arc<PriceGate>,
}

#[derive(Clone)]
struct CountingPrices {
    inner: Prices,
    requests: Arc<AtomicUsize>,
}
impl pe_source_polymarket_public::PageFetcher for CountingPrices {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, pe_source_core::SourceError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.inner.fetch_page(url).await
    }
}

#[derive(Clone)]
struct FrameIdentityPages {
    prices: Prices,
    gate: Arc<PriceGate>,
}
impl pe_source_polymarket_public::ReconciliationFetcher for FrameIdentityPages {
    fn fetch<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, pe_source_core::SourceError>> + Send + 'a>>
    {
        Box::pin(async move {
            if self.gate.blocked.swap(false, Ordering::SeqCst) {
                self.gate.started.notify_one();
                self.gate.release.notified().await;
            }
            let rows = self
                .prices
                .markets
                .lock()
                .unwrap()
                .values()
                .cloned()
                .collect::<Vec<_>>();
            Ok(serde_json::to_vec(&rows).unwrap())
        })
    }
}

struct BookEconomics {
    depth: Decimal,
    fee_free: bool,
}
struct DelayedPage {
    page: Vec<u8>,
    failed: bool,
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
            if self.failed {
                return Err(pe_source_core::SourceError::Transient {
                    message: "injected read failure".to_owned(),
                });
            }
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
struct MetadataPage {
    page: Vec<u8>,
    failures: Arc<AtomicUsize>,
}
impl pe_source_polymarket_public::ReconciliationFetcher for MetadataPage {
    fn fetch<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, pe_source_core::SourceError>> + Send + 'a>>
    {
        Box::pin(async move {
            if self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                Err(pe_source_core::SourceError::Transient {
                    message: "injected retained-routing metadata failure".to_owned(),
                })
            } else {
                Ok(self.page.clone())
            }
        })
    }
}
impl ClobBookFetcher for Books {
    async fn fetch_book(&self, condition: &str, token: &str) -> Result<OrderBook, ClobBookError> {
        if self.gate.market.lock().unwrap().as_deref() == Some(condition)
            && self.gate.blocked.swap(false, Ordering::SeqCst)
        {
            self.gate.started.notify_one();
            self.gate.release.notified().await;
        }
        self.values
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

#[derive(Clone)]
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
    health: pe_service::health::SharedHealth,
    terminal_clock: OffsetDateTime,
    poller_crash: Arc<Mutex<Option<pe_service::trade_poller::ReconciliationCrashBoundary>>>,
    poller_boot_rebuild: bool,
    frame_owner: bool,
    poller_fetch_failed: bool,
    poller_metadata_failures: Arc<AtomicUsize>,
    poller_trigger: Mutex<Option<mpsc::Sender<pe_service::activity_ingest::ReconciliationTrigger>>>,
    pending_poller_trigger: Mutex<Option<pe_service::activity_ingest::ReconciliationTrigger>>,
    books: Arc<Books>,
    prices: Prices,
    mid_requests: Arc<AtomicUsize>,
    identity_gate: Arc<PriceGate>,
    watchlist: LiveWatchlist,
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
        Self::new_with_configuration(financial_semantic_version, runtime()).await
    }

    async fn new_with_configuration(
        financial_semantic_version: u32,
        config: RuntimeConfig,
    ) -> Self {
        Self::new_with_leaders(financial_semantic_version, config, vec![wallet()]).await
    }

    async fn new_with_leaders(
        financial_semantic_version: u32,
        config: RuntimeConfig,
        leaders: Vec<WalletAddress>,
    ) -> Self {
        Self::new_with_pre_start_frame(financial_semantic_version, config, leaders, None).await
    }

    async fn new_with_pre_start_frame(
        financial_semantic_version: u32,
        config: RuntimeConfig,
        leaders: Vec<WalletAddress>,
        pre_start_frame: Option<Value>,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
        for leader in &leaders {
            paper
                .record_reconciled_history_status(&WalletHistoryStatusRecord {
                    wallet: *leader,
                    complete: true,
                    proof_json: "{}".to_owned(),
                    updated_at_unix: EPOCH - 1,
                })
                .unwrap();
            support::install_verified_empty_anchor(&paper, *leader, 0);
        }
        let live_path = dir.path().join("live_journal.log");
        drop(pe_execution_core::LiveJournal::open(&live_path).unwrap());
        let empty = TailBinding {
            physical_tail: 5,
            last_sequence: None,
            last_hash: "00".repeat(32),
        };
        let mut source_writer = Writer::open(dir.path().join("source.log")).unwrap();
        if let Some(frame) = pre_start_frame {
            source_writer
                .append_synced(EnvelopeIn {
                    source_id: SourceId(
                        pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned(),
                    ),
                    schema_version: 2,
                    parser_version: 2,
                    observed_at: SourceTimestamp(at()),
                    received_at: ReceivedAt(at()),
                    content_type: ContentType::Json,
                    payload: serde_json::to_vec(&frame).unwrap(),
                })
                .unwrap();
        }
        drop(source_writer);
        let source_prefix =
            TailBinding::from(&Scanner::verify(dir.path().join("source.log")).unwrap());
        let record = PaperLogRecord::QualificationStarted(Arc::new(QualificationStarted {
            starting_bankroll: CollateralAmount::from_decimal_exact(CASH).unwrap(),
            paper_prefix: empty.clone(),
            source_prefix,
            live_prefix: TailBinding::from(
                &pe_execution_core::LiveJournal::verified_tail(&live_path).unwrap(),
            ),
            artifact_blake3: "fixture".to_owned(),
            static_config_hash: "fixture".to_owned(),
            hot_config_hash: config.canonical_hash(),
            generation: "prepared-freshness".to_owned(),
            activation_id: "prepared-freshness".to_owned(),
            ranking_batch_id: 588,
            membership: leaders.clone(),
            membership_proofs_hash: pe_service::qualification::scenario_membership_proofs_hash(
                &paper, &leaders,
            )
            .unwrap(),
            schema_version: 3,
            parser_version: 1,
            financial_semantic_version,
        }));
        let mut writer = Writer::open(dir.path().join("paper.log")).unwrap();
        let start = if financial_semantic_version == 0 {
            // Pre-Start fixtures exercise source delivery with no financial protocol.
            support::scenario_receipt(0)
        } else {
            writer
                .append_synced(EnvelopeIn {
                    source_id: SourceId("pe-service.paper".to_owned()),
                    schema_version: 2,
                    parser_version: 1,
                    observed_at: SourceTimestamp(at()),
                    received_at: ReceivedAt(at()),
                    content_type: ContentType::Json,
                    payload: serde_json::to_vec(&record).unwrap(),
                })
                .unwrap()
        };
        drop(writer);
        if financial_semantic_version != 0 {
            paper
                .reset_financial_era(start, CollateralAmount::from_decimal_exact(CASH).unwrap())
                .unwrap();
        }
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
        let mut membership = (*watchlist().snapshot()).clone();
        let entry = membership.entries[0].clone();
        membership.entries = leaders
            .into_iter()
            .map(|wallet| pe_trader_index::WatchlistEntry {
                wallet,
                ..entry.clone()
            })
            .collect();
        membership.active_count = membership.entries.len();
        Self {
            config,
            copy_budget_secs: 2,
            probability: pe_core_types::Probability::new(dec!(0.7)).unwrap(),
            dir,
            paper,
            source,
            index,
            hooks,
            health: new_shared_health_with_ws(false, true, 90),
            terminal_clock: at(),
            poller_crash: Arc::new(Mutex::new(None)),
            poller_boot_rebuild: false,
            frame_owner: false,
            poller_fetch_failed: false,
            poller_metadata_failures: Arc::new(AtomicUsize::new(0)),
            poller_trigger: Mutex::new(None),
            pending_poller_trigger: Mutex::new(None),
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
            mid_requests: Arc::new(AtomicUsize::new(0)),
            identity_gate: Arc::new(PriceGate::default()),
            watchlist: LiveWatchlist::new(membership),
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
        self.record_with_economics(ordinal, paper_rule, None).await
    }

    async fn record_with_economics(
        &self,
        ordinal: u32,
        paper_rule: Option<(Decimal, Decimal, Option<i64>)>,
        economics: Option<BookEconomics>,
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
        if let Some(economics) = economics {
            book["asks"][0]["size"] = economics.depth.normalize().to_string().into();
            gamma[0]["orderPriceMinTickSize"] = "0.0001".into();
            long["minimum_tick_size"] = "0.0001".into();
            compact["mts"] = json!("0.0001");
            if economics.fee_free {
                compact["fd"]["r"] = json!(0);
                for field in ["makerBaseFee", "takerBaseFee"] {
                    gamma[0][field] = json!(0);
                }
                for field in ["maker_base_fee", "taker_base_fee"] {
                    long[field] = json!(0);
                }
                compact["mbf"] = json!(0);
                compact["tbf"] = json!(0);
            }
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
        self.books.values.lock().unwrap().insert(token, parsed);
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
        self.start_owner(enabled, false);
    }
    fn start_frames(&mut self) {
        self.start_owner(true, true);
    }
    fn start_owner(&mut self, enabled: bool, frames: bool) {
        self.start_owner_with_frame_recovery(enabled, frames, frames);
    }
    fn start_owner_with_frame_recovery(
        &mut self,
        enabled: bool,
        frames: bool,
        recover_frames: bool,
    ) {
        self.frame_owner = frames;
        let (control, receiver) = mpsc::channel(4);
        let risk_clock = self.hooks.clone();
        let terminal_clock = self.terminal_clock;
        let mut owner = Orchestrator::new_with_authority(
            self.watchlist.clone(),
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
                runtime_config: frames.then(|| {
                    pe_service::runtime_config::LiveRuntimeConfig::new(self.config.clone())
                }),
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
            pe_service::paper_recovery::PaperLog::open(self.dir.path().join("paper.log")).unwrap(),
            self.paper.clone(),
            build_leader_ledger(&self.paper).unwrap(),
            self.health.clone(),
            MidPriceCache::with_fetcher(
                CountingPrices {
                    inner: self.prices.clone(),
                    requests: self.mid_requests.clone(),
                },
                "fixture://gamma".to_owned(),
            )
            .with_source_log(self.source.clone())
            .with_clock(Arc::new(move || {
                if frames {
                    OffsetDateTime::from_unix_timestamp(
                        risk_clock.financial_clock_unix.load(Ordering::SeqCst),
                    )
                    .unwrap()
                } else {
                    at()
                }
            })),
            receiver,
            None,
            self.authority.clone(),
            self.books.clone(),
        )
        .unwrap();
        let started = self.paper.financial_start().unwrap().is_some();
        if started {
            owner = owner.with_activity_frames(
                self.source.clone(),
                90,
                Arc::new(AssetIdentityResolver::new_runtime(
                    Arc::new(FrameIdentityPages {
                        prices: self.prices.clone(),
                        gate: self.identity_gate.clone(),
                    }),
                    "fixture://gamma".to_owned(),
                    GAMMA_BATCH_SIZE,
                    self.source.clone(),
                )),
            );
        }
        owner.set_scenario_hooks(self.hooks.clone());
        if started {
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
        }
        let recovery = (recover_frames && started).then(|| {
            let obligations =
                pe_service::trade_poller::rebuild_reconciliation_obligations_with_index(
                    &self.dir.path().join("source.log"),
                    &self.paper,
                    &self.index,
                )
                .unwrap();
            obligations.frame_recovery_receipts()
        });
        self.control = Some(control);
        self.task = Some(tokio::spawn(
            pe_service::orchestrator::SCENARIO_TERMINAL_CLOCK.scope(terminal_clock, async move {
                if let Some((prefix, undelivered)) = recovery {
                    owner
                        .resume_pending_before_producers()
                        .await
                        .map_err(|error| {
                            pe_service::orchestrator::OrchestratorRunError::PendingRecovery(
                                error.to_string(),
                            )
                        })?;
                    owner
                        .resume_activity_frames_before_producers(&prefix, &undelivered)
                        .await
                        .map_err(pe_service::orchestrator::OrchestratorRunError::PendingRecovery)?;
                }
                owner.run_coordinated(std::future::pending::<()>()).await
            }),
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
        self.poll_source_result(recorded, stream_epoch, now, misses)
            .await
            .unwrap();
    }
    async fn poll_owner_failure(&self, recorded: &Recorded, stream_epoch: Option<i64>) {
        // A failed owner can close intake before acknowledging the newly required frontier.
        // The scenario asserts the exact durable checkpoint and owner failure separately.
        let now = OffsetDateTime::from_unix_timestamp(recorded.epoch).unwrap();
        let result = self
            .poll_source_result(recorded, stream_epoch, now, 0)
            .await;
        assert!(matches!(
            result,
            Ok(()) | Err(pe_service::trade_poller::TradePollerOwnerError::Reconciliation(_))
        ));
    }
    async fn poll_source_result(
        &self,
        recorded: &Recorded,
        stream_epoch: Option<i64>,
        now: OffsetDateTime,
        misses: usize,
    ) -> Result<(), pe_service::trade_poller::TradePollerOwnerError> {
        let (trigger, receiver) = mpsc::channel(1);
        *self.poller_trigger.lock().unwrap() = Some(trigger.clone());
        let pending = self.pending_poller_trigger.lock().unwrap().take();
        if let Some(pending) = pending {
            trigger.send(pending).await.unwrap();
        }
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
                    qualifying_buy: true,
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
        let metadata_failure_expected = self.poller_metadata_failures.load(Ordering::SeqCst) > 0;
        let delayed = Arc::new(DelayedPage {
            page: recorded.activity.clone(),
            failed: self.poller_fetch_failed,
            misses: AtomicUsize::new(misses),
            reads: AtomicUsize::new(0),
        });
        let clock = Arc::new(AtomicI64::new(now.unix_timestamp()));
        let poll_clock = Arc::clone(&clock);
        let mut obligations = pe_service::trade_poller::ReconciliationObligations::default();
        for row in self.paper.decision_pending_history().unwrap() {
            let continuation =
                pe_service::bucket_commit::DecisionContinuationV3::from_durable(&row).unwrap();
            if !continuation.is_activity_frame() {
                continue;
            }
            let proof: pe_service::frame_admission::FrameDecisionProof =
                serde_json::from_value(continuation.facts.decision_inputs.clone()).unwrap();
            obligations.insert(pe_service::activity_ingest::ReconciliationTrigger {
                qualifying_buy: true,
                wallet: row.wallet,
                source_time: proof.inputs.source_time,
                source_trade_id: row.source_trade_id,
                provenance: pe_copy_signal_engine::TradeProvenance::ActivityWs,
                received_at: proof.inputs.received_at,
                receipt: proof.inputs.frame_receipt,
            });
        }
        if self.poller_boot_rebuild || self.frame_owner {
            obligations = pe_service::trade_poller::rebuild_reconciliation_obligations_with_index(
                &self.dir.path().join("source.log"),
                &self.paper,
                &self.index,
            )
            .unwrap();
        }
        let poller = TradePoller::new(
            TradePollerConfig {
                base_url: "fixture://activity".to_owned(),
                poll_interval_secs: 30,
                activity_ws_enabled: stream_epoch.is_some(),
                copy_latency_budget_secs: self.copy_budget_secs,
            },
            self.watchlist.clone(),
            delayed.clone(),
            Arc::new(AssetIdentityResolver::new_runtime(
                Arc::new(MetadataPage {
                    page: recorded.gamma.clone(),
                    failures: self.poller_metadata_failures.clone(),
                }),
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
            obligations,
            None,
        )
        .with_source_receipt_index(self.index.clone())
        .with_progress(progress)
        .with_crash_boundary(self.poller_crash.clone())
        .with_clock(Arc::new(move || {
            OffsetDateTime::from_unix_timestamp(poll_clock.load(Ordering::SeqCst)).unwrap()
        }));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(poller.run_until(async move {
            let _ = stopped.await;
        }));
        let mut unresolved = None;
        while let Some(progress) = completed.recv().await {
            if let pe_service::trade_poller::PollerProgress::Completed {
                wallet: source_wallet,
                unresolved: receipts,
                ..
            } = &progress
                && *source_wallet == wallet()
            {
                unresolved = Some(receipts.clone());
            }
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
        let _ = stop.send(());
        let result = task.await.unwrap();
        self.poller_trigger.lock().unwrap().take();
        assert!(
            delayed.reads.load(Ordering::SeqCst) > misses
                || (metadata_failure_expected
                    && self.poller_metadata_failures.load(Ordering::SeqCst) == 0),
            "poll did not reach acquisition: {result:?}, unresolved={unresolved:?}, now={now}"
        );
        result?;
        if self.frame_owner {
            self.assert_frame_barrier(unresolved.as_deref()).await;
        }
        if misses > 0 {
            self.barrier().await;
        }
        Ok(())
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
        let activity: Value = serde_json::from_slice(&recorded.activity).unwrap();
        let source_wallet =
            WalletAddress::from_hex(activity[0]["proxyWallet"].as_str().unwrap()).unwrap();
        let read = support::producer_shaped_read_v2(
            source_wallet,
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

    fn set_continuation_version(&self, recorded: &Recorded, version: u16) {
        let row = self.terminal(recorded);
        let mut wire: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
        wire["version"] = json!(version);
        if version <= 6 {
            wire.as_object_mut().unwrap().remove("source_authority");
        }
        rusqlite::Connection::open(self.dir.path().join("paper.db"))
            .unwrap()
            .execute(
                "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
                rusqlite::params![wire.to_string(), recorded.id.0],
            )
            .unwrap();
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
        if self.frame_owner {
            self.assert_frame_barrier(None).await;
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
    assert_eq!(continuation.version(), 7);
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
        *h.books.gate.market.lock().unwrap() =
            Some(recorded.admission.market.condition_id.0.clone());
        h.books.gate.blocked.store(true, Ordering::SeqCst);
        let gate = h.books.gate.clone();
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
            7
        );
        let frames = scan_paper_log(&h.dir.path().join("paper.log")).unwrap();
        let economic = frames
            .iter()
            .find_map(|frame| match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                    payload: pe_service::paper_recovery::FinancialPayload::Fill { economic, .. },
                    ..
                }) => Some(economic.clone()),
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
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{:?}", report);

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
            PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => Some(mark.clone()),
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
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{:?}", report);

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
    h.poll_owner_failure(&recorded, Some(EPOCH - 1)).await;
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
        assert_eq!(continuation.version(), 7);
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
            .find_map(|frame| match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                    payload: pe_service::paper_recovery::FinancialPayload::Fill { economic, .. },
                    ..
                }) => Some(economic.clone()),
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
/// latency cause. A later continuation-seven decision in the same generation ignores that cause.
#[tokio::test]
async fn mixed_era_boot_keeps_old_latency_audit_and_new_decision_false() {
    let mut h = Harness::new_with_semantic(1).await;
    let old = h.record(1).await;
    h.freeze(&old, true).await;
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    let row = h.terminal(&old);
    let mut frozen: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
    assert_eq!(frozen["version"], json!(7));
    frozen["version"] = json!(5);
    frozen.as_object_mut().unwrap().remove("source_authority");
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
    assert_eq!(current_replay.continuation.version(), 7);
    assert_eq!(current_replay.post_boundary.financial_semantic_version, 3);
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
        let pending = h.poll_owner_failure(&recorded, None);
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
    h.poll_owner_failure(&recorded, None).await;
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
    h.poll_owner_failure(&recorded, None).await;
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
    let writer = pe_service::paper_recovery::PaperLog::open(&paper_log).unwrap();
    assert_eq!(
        reconcile_active_financial_frames(
            &h.authority,
            &h.paper,
            SourceEvidence::Index(&h.index),
            &writer
        )
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        reconcile_active_financial_frames(
            &h.authority,
            &h.paper,
            SourceEvidence::Index(&h.index),
            &writer
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
            serde_json::to_string(&support::terminal_evidence(document.body).unwrap()).unwrap();
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
    let mut historical = support::terminal_evidence(body).unwrap();
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
            // first activity read, through the real validator and bucket commit owner. A novel
            // revision invalidates that attempt before any positions read or anchor install.
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
            let anchors_before = h.paper.position_anchors(&wallet()).unwrap();
            let outcome = validator
                .validate_direct_with_deferrals(&[wallet()], &mut engine, &h.paper)
                .await
                .unwrap();
            assert!(outcome.accepted.is_empty());
            assert!(matches!(
                outcome.deferred.as_slice(),
                [(deferred_wallet, pe_service::position_seeder::CausalPositionError::Fenced { wallet: fenced_wallet })]
                    if *deferred_wallet == wallet() && *fenced_wallet == wallet()
            ));
            assert_eq!(h.paper.position_anchors(&wallet()).unwrap(), anchors_before);
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
    async fn requalify_current_rows(&self) -> pe_service::qualification::QualificationReport {
        let paper_path = self.dir.path().join("paper.log");
        let source_path = self.dir.path().join("source.log");
        let live_path = self.dir.path().join("live_journal.log");
        let mut seal = scan_paper_log(&paper_path)
            .unwrap()
            .iter()
            .rev()
            .find_map(|frame| match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::QualificationSealed(seal)) => {
                    Some((**seal).clone())
                }
                _ => None,
            })
            .unwrap();
        seal.source_prefix = TailBinding::from(&Scanner::verify(&source_path).unwrap());
        seal.financial_prefix = TailBinding::from(&Scanner::verify(&paper_path).unwrap());
        let keys = self
            .paper
            .decision_pending_history()
            .unwrap()
            .into_iter()
            .map(|row| (row.source_trade_id, row.semantic_revision))
            .collect::<Vec<_>>();
        let digest = self
            .paper
            .seal_decision_evidence_for_source_prefix(
                &keys,
                &keys,
                seal.source_prefix.last_sequence,
            )
            .unwrap();
        seal.decision_evidence_digest = blake3::hash(&digest).to_hex().to_string();
        let timestamp = OffsetDateTime::from_unix_timestamp(seal.sealed_cutoff_unix).unwrap();
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
            output: self.dir.path().join("altered-qualification.json"),
        };
        pe_service::qualification::run_qualify(&options)
            .await
            .unwrap();
        serde_json::from_slice(&std::fs::read(options.output).unwrap()).unwrap()
    }

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
        let position_count = self.paper.open_positions().unwrap().len();
        for _ in 0..position_count {
            let price = self
                .source
                .append(EnvelopeIn {
                    source_id: SourceId("pe-service.clob-prices-history".to_owned()),
                    schema_version: 1,
                    parser_version: 1,
                    observed_at: SourceTimestamp(timestamp),
                    received_at: ReceivedAt(timestamp),
                    content_type: ContentType::Json,
                    payload: serde_json::to_vec(&json!({"history":[{"t":cutoff,"p":"0.50"}]}))
                        .unwrap(),
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
        }
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
            .find_map(|frame| match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => Some(mark.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(mark.prices.len(), position_count);
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
            }) => Some(economic.clone()),
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

fn dollar_runtime() -> RuntimeConfig {
    let mut config = runtime();
    config.sizing_mode = SizingMode::Dollar { usd: dec!(25) };
    config.sizing_dollar_usd = dec!(25);
    config
}

#[tokio::test]
async fn floor_fill_without_mid() {
    for version in [7, 6] {
        let mut h =
            Harness::new_with_configuration(if version == 7 { 3 } else { 2 }, dollar_runtime())
                .await;
        let recorded = h
            .record_with_economics(
                1,
                Some((dec!(0.15), dec!(0.15), Some(0))),
                Some(BookEconomics {
                    depth: dec!(1000),
                    fee_free: true,
                }),
            )
            .await;
        h.freeze(&recorded, true).await;
        h.set_continuation_version(&recorded, version);
        h.attempt(&recorded, at());
        h.start(true);
        h.barrier().await;
        let row = h.terminal(&recorded);
        let replay = replay_decision_pending(&row).unwrap();
        let book = replay.post_boundary.body.book.as_ref().unwrap();
        let expected_shares = (dec!(25) / dec!(0.15))
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToNegativeInfinity);
        let expected_spend = (expected_shares * dec!(0.15))
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToNegativeInfinity);
        assert_eq!(expected_shares, dec!(166.666666));
        assert_eq!(expected_spend, dec!(24.999999));
        let vwap = expected_spend / expected_shares;
        assert!(vwap < dec!(0.15));
        assert_eq!(
            book.vwap_basis
                .as_ref()
                .unwrap()
                .parse::<Decimal>()
                .unwrap(),
            vwap
        );
        if version == 7 {
            assert_eq!(h.mid_requests.load(Ordering::SeqCst), 0);
            assert!(replay.post_boundary.body.market_price.is_none());
            assert_eq!(row.terminal_disposition.as_deref(), Some("fill"));
            let fills = h.authority.inner.lock().unwrap();
            assert_eq!(fills.fills.len(), 1);
            assert_eq!(fills.fills[0].0.quantity.to_decimal(), expected_shares);
            assert_eq!(fills.fills[0].0.principal.to_decimal(), dec!(25));
            assert_eq!(fills.fills[0].0.fill_price.0, vwap);
            assert_eq!(h.prepared_count(), 1);
            assert!(replay.post_boundary.body.terminal.final_receipt.is_some());
        } else {
            assert_eq!(h.mid_requests.load(Ordering::SeqCst), 1);
            assert!(
                replay
                    .post_boundary
                    .body
                    .market_price
                    .as_ref()
                    .unwrap()
                    .mid_price
                    .is_some()
            );
            assert_eq!(
                replay.post_boundary.body.terminal.reason,
                "fill_price_below_min"
            );
            assert_eq!(h.prepared_count(), 0);
        }
        h.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn semantic_three_partial_replay() {
    for frame_authority in [false, true] {
        let mut h = Harness::new_with_configuration(3, dollar_runtime()).await;
        let recorded = h
            .record_with_economics(
                1,
                Some((dec!(0.50), dec!(0.50), Some(0))),
                Some(BookEconomics {
                    depth: dec!(20.123457),
                    fee_free: false,
                }),
            )
            .await;
        h.attempt(&recorded, at());
        if frame_authority {
            h.start_frames();
            h.empty_frontier(EPOCH - 1).await;
            h.deliver_frame(&recorded, |_| {}).await;
            assert!(h.paper.leader_positions().unwrap().is_empty());
        } else {
            h.start(true);
        }
        h.poll(&recorded).await;
        assert_eq!(
            h.terminal(&recorded).terminal_disposition.as_deref(),
            Some("fill")
        );
        let replay = replay_decision_pending(&h.terminal(&recorded)).unwrap();
        assert_eq!(replay.continuation.is_activity_frame(), frame_authority);
        assert_eq!(replay.continuation.version(), 7);
        assert_eq!(replay.post_boundary.financial_semantic_version, 3);
        let fills = h.authority.inner.lock().unwrap().fills.clone();
        assert_eq!(fills.len(), 1);
        assert!(fills[0].0.principal.to_decimal() < dec!(25));
        assert!(fills[0].0.fee.to_decimal() > Decimal::ZERO);
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "{:?}", report.replay);
        assert_eq!(report.replay.fills, 1);
        h.stop().await;
    }
}

#[tokio::test]
async fn homogeneous_same_second_production_and_qualification() {
    let mut h = Harness::new().await;
    let mut recorded = h.record(1).await;
    let mut activity: Value = serde_json::from_slice(&recorded.activity).unwrap();
    let mut second = activity[0].clone();
    second["transactionHash"] = format!("0x{:064x}", 900).into();
    activity.as_array_mut().unwrap().push(second);
    recorded.activity = serde_json::to_vec(&activity).unwrap();
    let read = support::producer_shaped_read_v2(
        wallet(),
        &recorded.activity,
        recorded.epoch,
        recorded.epoch,
        support::scenario_receipt(1),
    );
    recorded.id = read
        .aggregates
        .iter()
        .map(|aggregate| aggregate.group_id.key().clone())
        .min_by(|a, b| a.0.cmp(&b.0))
        .unwrap();
    h.attempt(&recorded, at());
    h.start(true);
    h.poll(&recorded).await;
    assert_eq!(h.paper.decision_pending_history().unwrap().len(), 1);
    assert_eq!(h.paper.gate_history().unwrap()[&wallet()].len(), 1);
    assert_eq!(h.paper.leader_positions().unwrap().len(), 1);
    assert_eq!(
        h.paper.leader_positions().unwrap()[0]
            .long_contracts
            .to_decimal(),
        dec!(10)
    );
    assert_eq!(h.prepared_count(), 1);
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{:?}", report.replay);
    assert_eq!(report.replay.fills, 1);
}

#[tokio::test]
async fn second_leader_first_entry_is_copied() {
    let other = WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    for (version, outcome) in [(7, 0), (7, 1), (6, 0), (6, 1)] {
        let mut h = Harness::new_with_leaders(
            if version == 7 { 3 } else { 2 },
            runtime(),
            vec![wallet(), other],
        )
        .await;
        let held = h.record(1).await;
        h.freeze(&held, true).await;
        h.set_continuation_version(&held, version);
        let market = held.admission.market.condition_id.0.clone();
        let token = held.admission.market.ordered_outcome_token_ids[outcome]
            .0
            .clone();
        let payload = serde_json::to_vec(
            &json!({"market":market,"asset_id":token,"asks":[{"price":"0.50","size":"100"}]}),
        )
        .unwrap();
        let receipt = h.append("polymarket.clob.book", &payload).await;
        let mut book = OrderBook::from_book_json(&payload).unwrap();
        book.source_receipt = Some(receipt);
        book.fetched_at_ms = u64::try_from(EPOCH * 1000).unwrap();
        h.books.values.lock().unwrap().insert(token.clone(), book);
        let mut activity: Value = serde_json::from_slice(&held.activity).unwrap();
        activity[0]["proxyWallet"] = other.to_string().into();
        activity[0]["asset"] = token.into();
        activity[0]["outcomeIndex"] = json!(outcome);
        activity[0]["outcome"] = json!(if outcome == 0 { "Yes" } else { "No" });
        activity[0]["timestamp"] = json!(EPOCH + 1);
        activity[0]["transactionHash"] = format!("0x{:064x}", 901).into();
        let activity = serde_json::to_vec(&activity).unwrap();
        let read = support::producer_shaped_read_v2(
            other,
            &activity,
            EPOCH + 1,
            EPOCH + 1,
            support::scenario_receipt(1),
        );
        let second = Recorded {
            epoch: EPOCH + 1,
            activity,
            gamma: held.gamma.clone(),
            id: read.aggregates[0].group_id.key().clone(),
            admission: held.admission.clone(),
        };
        h.freeze(&second, true).await;
        h.set_continuation_version(&second, version);
        h.attempt(&held, at());
        h.attempt(&second, at() + time::Duration::seconds(1));
        h.arm();
        h.start(true);
        h.barrier().await;
        assert_eq!(
            h.terminal(&held).terminal_disposition.as_deref(),
            Some("fill")
        );
        let row = h.terminal(&second);
        let replay = replay_decision_pending(&row).unwrap();
        assert!(
            h.paper.gate_history().unwrap()[&other].contains(&pe_core_types::MarketId(
                pe_core_types::VenueMarketId(market)
            ))
        );
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 2);
        if version == 6 && outcome == 0 {
            assert_eq!(row.terminal_disposition.as_deref(), Some("no_fill"));
            assert_eq!(replay.post_boundary.body.terminal.reason, "paper_held");
            assert_eq!(h.prepared_count(), 1);
            assert_eq!(h.paper.open_positions().unwrap().len(), 1);
            assert!(replay.post_boundary.body.terminal.dispatch_id.is_some());
        } else {
            assert_eq!(row.terminal_disposition.as_deref(), Some("fill"));
            assert_eq!(h.prepared_count(), 2);
            assert_eq!(
                h.paper.open_positions().unwrap().len(),
                if outcome == 0 { 1 } else { 2 }
            );
            if outcome == 0 {
                let total = h
                    .paper
                    .list_fills()
                    .unwrap()
                    .iter()
                    .map(|fill| fill.quantity.to_decimal())
                    .sum::<rust_decimal::Decimal>();
                assert_eq!(
                    h.paper.open_positions().unwrap()[0].long.to_decimal(),
                    total
                );
            }
        }
        h.stop().await;
    }
}

#[tokio::test]
async fn mixed_same_second_outcomes_stay_ambiguous_in_production() {
    let mut h = Harness::new().await;
    let mut recorded = h.record(1).await;
    let mut activity: Value = serde_json::from_slice(&recorded.activity).unwrap();
    let mut second = activity[0].clone();
    second["transactionHash"] = format!("0x{:064x}", 902).into();
    second["asset"] = recorded.admission.market.ordered_outcome_token_ids[1]
        .0
        .clone()
        .into();
    second["outcomeIndex"] = json!(1);
    second["outcome"] = json!("No");
    activity.as_array_mut().unwrap().push(second);
    recorded.activity = serde_json::to_vec(&activity).unwrap();
    h.start(true);
    h.poll(&recorded).await;
    assert!(h.paper.decision_pending_history().unwrap().is_empty());
    assert_eq!(h.paper.gate_history().unwrap()[&wallet()].len(), 1);
    assert_eq!(h.paper.leader_positions().unwrap().len(), 2);
    for aggregate in support::producer_shaped_read_v2(
        wallet(),
        &recorded.activity,
        recorded.epoch,
        recorded.epoch,
        support::scenario_receipt(1),
    )
    .aggregates
    {
        let group = h
            .paper
            .activity_group_state(aggregate.group_id.key())
            .unwrap()
            .unwrap();
        assert_eq!(group.disposition, "ambiguous_first_entry_same_second");
    }
    assert_eq!(h.prepared_count(), 0);
    h.stop().await;
}

impl Harness {
    fn install_checkpoint_metadata(&self) {
        let tail = |path: std::path::PathBuf| {
            let binding = pe_event_log::Scanner::verify(&path).unwrap();
            json!({"path":path, "physical_tail":binding.physical_tail, "last_sequence":binding.last_sequence.map(|seq| seq.0), "last_hash":binding.last_hash.to_hex().to_string()})
        };
        let bindings = json!({"source":tail(self.dir.path().join("source.log")), "paper":tail(self.dir.path().join("paper.log")), "live_journal":tail(self.dir.path().join("live_journal.log"))});
        let metadata = json!({"version_one_boundary":bindings, "activation_tails":bindings, "phase":"installed", "side_main_path":self.dir.path().join("synthetic-side.db"), "input_hashes":{}});
        rusqlite::Connection::open(self.dir.path().join("paper.db"))
            .unwrap()
            .execute(
                "INSERT INTO meta(key,value) VALUES ('trustworthy_v2_migration_record', ?1)",
                [metadata.to_string()],
            )
            .unwrap();
    }

    async fn start_frames_with_reader(
        &mut self,
    ) -> (
        tokio::sync::watch::Sender<bool>,
        mpsc::Receiver<pe_source_polymarket_public::ActivityWsPeer>,
    ) {
        self.coordinator.abort();
        let _ = (&mut self.coordinator).await;
        let (source, source_rx) = SourceLogHandle::channel(8);
        self.source = source;
        let (trigger_tx, trigger_rx) = mpsc::channel(8);
        self._trigger_rx = trigger_rx;
        self.start_frames();
        let (started, start) = tokio::sync::watch::channel(false);
        let (peers, ready) = mpsc::channel(3);
        let dialer: pe_service::activity_ingest::Dialer = Arc::new(move |_| {
            let peers = peers.clone();
            Box::pin(async move {
                let (client, server) = pe_source_polymarket_public::ActivityWsPeer::pair().await?;
                peers.send(server).await.map_err(|_| {
                    pe_source_polymarket_public::ActivityWsError::Transport {
                        message: "scenario peer closed".to_owned(),
                    }
                })?;
                Ok(client)
            })
        });
        let ingest = ActivityIngest::with_dialer(
            self.watchlist.clone(),
            SourceEventSink::open(self.dir.path().join("source.log")).unwrap(),
            source_rx,
            trigger_tx,
            new_shared_health_with_ws(false, true, 90),
            dialer,
        )
        .with_source_receipt_index(self.index.clone())
        .with_reader_start_gate(start)
        .with_scenario_receive_clock(Arc::new(at));
        let ingest = if self.paper.financial_start().unwrap().is_some() {
            ingest.with_control_sender(self.control.as_ref().unwrap().downgrade())
        } else {
            ingest
        };
        self.coordinator = tokio::spawn(ingest.run());
        (started, ready)
    }

    async fn empty_frontier(&self, fixed_end: i64) {
        self.empty_frontier_for(wallet(), fixed_end).await;
    }

    async fn empty_frontier_for(&self, source_wallet: WalletAddress, fixed_end: i64) {
        let page = self
            .source
            .append(EnvelopeIn {
                source_id: SourceId(pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: 3,
                parser_version: 2,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: b"[]".to_vec(),
            })
            .await
            .unwrap();
        let read = support::producer_shaped_read_v2(source_wallet, b"[]", fixed_end, EPOCH, page);
        let commitment = self
            .source
            .append(EnvelopeIn {
                source_id: SourceId(
                    pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned(),
                ),
                schema_version: 2,
                parser_version: 1,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: read.commitment_payload.clone(),
            })
            .await
            .unwrap();
        let proof: Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
        let (acknowledged, received) = tokio::sync::oneshot::channel();
        self.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ReconciliationUpdate {
                update: pe_service::orchestrator_control::ReconciliationUpdate::Frontier(
                    pe_service::frame_admission::FeedHistoryFrontier {
                        version: 1,
                        wallet: source_wallet,
                        fixed_end,
                        commitment,
                        page_occurrences: vec![read.page],
                        pages: serde_json::from_value(proof["pages"].clone()).unwrap(),
                    },
                    None,
                ),
                acknowledged,
            })
            .await
            .unwrap();
        received.await.unwrap().unwrap();
    }

    async fn append_frame(
        &self,
        recorded: &Recorded,
        change: impl FnOnce(&mut Value),
    ) -> AppendReceipt {
        let rows: Value = serde_json::from_slice(&recorded.activity).unwrap();
        let mut frame = rows[0].clone();
        change(&mut frame);
        let timestamp = frame["timestamp"].as_i64().unwrap();
        let observation = pe_source_polymarket_public::parse_activity_trade_observation(
            &serde_json::to_vec(&frame).unwrap(),
        )
        .unwrap();
        let receipt = self
            .source
            .append(EnvelopeIn {
                source_id: SourceId(pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned()),
                schema_version: 2,
                parser_version: 2,
                observed_at: SourceTimestamp(
                    OffsetDateTime::from_unix_timestamp(timestamp).unwrap(),
                ),
                received_at: ReceivedAt(
                    OffsetDateTime::from_unix_timestamp(
                        self.hooks.financial_clock_unix.load(Ordering::SeqCst),
                    )
                    .unwrap(),
                ),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&frame).unwrap(),
            })
            .await
            .unwrap();
        let trigger = self.poller_trigger.lock().unwrap().clone();
        if let Some(trigger) = trigger {
            trigger
                .send(pe_service::activity_ingest::ReconciliationTrigger {
                    qualifying_buy: observation.group_id.components().side
                        == Some(pe_core_types::Side::Buy)
                        && observation.share_amount != pe_core_types::ShareAmount::ZERO
                        && !observation.is_combo,
                    wallet: observation.wallet,
                    source_time: observation.source_time.0,
                    source_trade_id: observation.group_id.key().clone(),
                    provenance: pe_copy_signal_engine::TradeProvenance::ActivityWs,
                    received_at: OffsetDateTime::from_unix_timestamp(
                        self.hooks.financial_clock_unix.load(Ordering::SeqCst),
                    )
                    .unwrap(),
                    receipt,
                })
                .await
                .unwrap();
        }
        receipt
    }

    async fn deliver_frame(
        &self,
        recorded: &Recorded,
        change: impl FnOnce(&mut Value),
    ) -> AppendReceipt {
        let receipt = self.append_frame(recorded, change).await;
        self.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt })
            .await
            .unwrap();
        self.assert_frame_barrier(None).await;
        receipt
    }

    async fn assert_frame_barrier(&self, live: Option<&[AppendReceipt]>) {
        self.boot_barrier_readonly().await;
        let obligations = pe_service::trade_poller::rebuild_reconciliation_obligations(
            &self.dir.path().join("source.log"),
            &self.paper,
        )
        .unwrap();
        let expected = obligations.unresolved_receipts(wallet());
        let actual = self
            .hooks
            .frame_barriers
            .lock()
            .unwrap()
            .get(&wallet())
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            actual, expected,
            "owner barrier differs from rebuilt poller obligations"
        );
        if let Some(live) = live {
            assert_eq!(
                actual, live,
                "owner barrier differs from running poller obligations"
            );
        }
    }

    async fn boot_barrier_readonly(&self) {
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
        receiver.await.unwrap().unwrap();
    }

    async fn restart_frames(&mut self) {
        self.stop().await;
        self.coordinator.abort();
        let _ = (&mut self.coordinator).await;
        self.index = SourceReceiptIndex::replay(&self.dir.path().join("source.log")).unwrap();
        let (source, receiver) = SourceLogHandle::channel(8);
        self.source = source;
        let sink = SourceEventSink::open(self.dir.path().join("source.log")).unwrap();
        let (trigger, trigger_rx) = mpsc::channel(1);
        self._trigger_rx = trigger_rx;
        self.coordinator = tokio::spawn(
            ActivityIngest::poll_only(
                sink,
                receiver,
                trigger,
                new_shared_health_with_ws(false, true, 90),
            )
            .with_source_receipt_index(self.index.clone())
            .run(),
        );
        self.start_frames();
        self.boot_barrier().await;
    }
}

fn identifier_counts(h: &Harness, id: &SourceTradeId) -> (i64, i64, i64, i64) {
    rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap().query_row(
        "SELECT (SELECT count(*) FROM seen_trades WHERE source_trade_id=?1), (SELECT count(*) FROM fills WHERE idempotency_key LIKE 'wf|%|' || ?1 || '|%'), (SELECT count(*) FROM dispatch_seeds WHERE source_trade_id=?1), (SELECT count(*) FROM no_copy_dispositions WHERE source_trade_id=?1)",
        [&id.0], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).unwrap()
}

#[tokio::test(start_paused = true)]
async fn frame_final_before_rest_then_reconcile() {
    let mut h = Harness::new().await;
    h.install_checkpoint_metadata();
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    let (started, mut peers) = h.start_frames_with_reader().await;
    h.empty_frontier(EPOCH - 1).await;
    started.send(true).unwrap();
    let mut peer = peers.recv().await.unwrap();
    let rows: Value = serde_json::from_slice(&recorded.activity).unwrap();
    peer.send_text(&json!({"topic":"activity", "type":"trades", "payload":rows[0]}).to_string())
        .await
        .unwrap();
    let receipt = h._trigger_rx.recv().await.unwrap().receipt;
    h.boot_barrier_readonly().await;
    let before = h.terminal(&recorded);
    assert_eq!(before.terminal_disposition.as_deref(), Some("fill"));
    let replay = replay_decision_pending(&before).unwrap();
    assert!(replay.continuation.is_activity_frame());
    assert_eq!(replay.post_boundary.financial_semantic_version, 3);
    assert_eq!(replay.continuation.observed_source_receipt, Some(receipt));
    assert_eq!(h.prepared_count(), 1);
    assert!(h.paper.leader_positions().unwrap().is_empty());
    assert!(
        h.paper
            .activity_group_state(&recorded.id)
            .unwrap()
            .is_none()
    );
    let counts = identifier_counts(&h, &recorded.id);
    assert_eq!((counts.0, counts.1, counts.3), (0, 1, 0));
    assert!(counts.2 <= 1);
    let history = h.paper.gate_history().unwrap();
    let financial = h.paper.financial_snapshot(EPOCH).unwrap();
    h.stop().await;
    let reopened = PaperStateDb::open(&h.dir.path().join("paper.db")).unwrap();
    assert_eq!(
        reopened
            .decision_pending_for(&recorded.id)
            .unwrap()
            .unwrap(),
        before
    );
    pe_service::source_log_boot::SourceLogBoot::prepare_checkpoint(&h.dir.path().join("paper.db"))
        .unwrap();
    h.start_frames();
    h.boot_barrier().await;
    assert_eq!(h.terminal(&recorded), before);
    // An equal-ID REST echo changes its epoch without replacing the durable frame gate.
    let mut rest: Value = serde_json::from_slice(&recorded.activity).unwrap();
    rest[0]["timestamp"] = json!(EPOCH + 1);
    let echo = Recorded {
        epoch: EPOCH + 1,
        activity: serde_json::to_vec(&rest).unwrap(),
        gamma: recorded.gamma.clone(),
        id: recorded.id.clone(),
        admission: recorded.admission.clone(),
    };
    h.poll(&echo).await;
    assert_eq!(identifier_counts(&h, &recorded.id), (1, 1, counts.2, 0));
    assert_eq!(h.terminal(&recorded), before);
    assert_eq!(h.paper.gate_history().unwrap(), history);
    assert_eq!(h.paper.financial_snapshot(EPOCH).unwrap(), financial);
    assert_eq!(h.paper.leader_positions().unwrap().len(), 1);
    assert_eq!(
        h.paper
            .activity_group_state(&recorded.id)
            .unwrap()
            .unwrap()
            .disposition,
        "applied"
    );
    let post_rest_rows = h.paper.leader_positions().unwrap();
    let post_rest_hash = pe_service::position_seeder::ledger_capture(
        &build_leader_ledger(&h.paper).unwrap(),
        &h.paper,
        wallet(),
    )
    .unwrap()
    .hash;
    let post_rest_counts = identifier_counts(&h, &recorded.id);
    h.stop().await;
    h.paper = Arc::new(PaperStateDb::open(&h.dir.path().join("paper.db")).unwrap());
    let recovery_index = SourceReceiptIndex::replay(&h.dir.path().join("source.log")).unwrap();
    let writer =
        pe_service::paper_recovery::PaperLog::open(h.dir.path().join("paper.log")).unwrap();
    assert_eq!(
        reconcile_active_financial_frames(
            &h.authority,
            &h.paper,
            SourceEvidence::Index(&recovery_index),
            &writer,
        )
        .await
        .unwrap(),
        0
    );
    drop(writer);
    h.start_frames();
    h.boot_barrier().await;
    assert_eq!(h.terminal(&recorded), before);
    assert_eq!(h.paper.financial_snapshot(EPOCH).unwrap(), financial);
    assert_eq!(h.paper.gate_history().unwrap(), history);
    assert_eq!(h.paper.leader_positions().unwrap(), post_rest_rows);
    assert_eq!(
        pe_service::position_seeder::ledger_capture(
            &build_leader_ledger(&h.paper).unwrap(),
            &h.paper,
            wallet(),
        )
        .unwrap()
        .hash,
        post_rest_hash
    );
    assert_eq!(identifier_counts(&h, &recorded.id), post_rest_counts);
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{:?}", report.replay);
    assert_eq!(report.replay.fills, 1);
    h.stop().await;
    // Equal identifiers also retain their original late and fenced route pairings.
    for route in ["late", "fenced"] {
        let mut echo_owner = Harness::new().await;
        let frame = echo_owner.record(1).await;
        echo_owner.attempt(&frame, at());
        echo_owner.start_frames();
        echo_owner.empty_frontier(EPOCH - 1).await;
        echo_owner.deliver_frame(&frame, |_| {}).await;
        let terminal = echo_owner.terminal(&frame);
        echo_owner.stop().await;
        if route == "late" {
            echo_owner.paper.set_cursor(&wallet(), EPOCH + 10).unwrap();
            rusqlite::Connection::open(echo_owner.dir.path().join("paper.db")).unwrap().execute(
                "INSERT INTO activity_groups(source_trade_id,transaction_hash,wallet_hex,source_epoch,semantic_revision,activity_type,disposition,proof_json) VALUES ('fixture-future','future',?1,?2,'future','REDEEM','raw_only',?3)",
                rusqlite::params![wallet().to_string(), EPOCH + 10, "{\"effect\":{\"kind\":\"raw_only\"}}"],
            ).unwrap();
        } else {
            rusqlite::Connection::open(echo_owner.dir.path().join("paper.db")).unwrap().execute(
                "INSERT INTO wallet_fences(wallet_hex,source_trade_id,cause,proof_json,fenced_at_unix) VALUES (?1,?2,'ineligible_mapping','{}',?3)",
                rusqlite::params![wallet().to_string(), "fixture-fence", EPOCH],
            ).unwrap();
        }
        echo_owner.start_frames();
        echo_owner.boot_barrier().await;
        echo_owner.poll(&frame).await;
        assert_eq!(echo_owner.terminal(&frame), terminal);
        assert_eq!(identifier_counts(&echo_owner, &frame.id), (1, 1, 0, 0));
        let group = echo_owner
            .paper
            .activity_group_state(&frame.id)
            .unwrap()
            .unwrap();
        assert_eq!(
            group.disposition,
            if route == "late" {
                "reanchor_required_late_group"
            } else {
                "wallet_fenced_applied"
            }
        );
        assert_eq!(
            echo_owner.paper.leader_positions().unwrap().len(),
            usize::from(route == "fenced")
        );
        assert!(echo_owner.feed_edges().is_empty());
        echo_owner.stop().await;
    }
}

#[tokio::test]
async fn frame_ordering_combo_zero_and_first_piece() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&recorded, |row| {
        row["isCombo"] = json!(true);
        row["transactionHash"] = json!("combo");
    })
    .await;
    h.deliver_frame(&recorded, |row| {
        row["size"] = json!("0");
        row["transactionHash"] = json!("zero");
    })
    .await;
    assert!(h.paper.decision_pending_history().unwrap().is_empty());
    h.deliver_frame(&recorded, |_| {}).await;
    let first = h.terminal(&recorded);
    h.deliver_frame(&recorded, |_| {}).await;
    h.deliver_frame(&recorded, |row| {
        row["transactionHash"] = json!("second-piece");
    })
    .await;
    h.deliver_frame(&recorded, |row| {
        row["transactionHash"] = json!("other-outcome");
        row["outcomeIndex"] = json!(1);
        row["asset"] = json!("456");
    })
    .await;
    assert_eq!(h.paper.decision_pending_history().unwrap().len(), 1);
    assert_eq!(h.terminal(&recorded), first);
    assert!(h.paper.leader_positions().unwrap().is_empty());
    h.stop().await;
}

#[tokio::test]
async fn frontier_fallback_artifact_survives_empty_refresh_and_restart() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.start_frames();
    h.empty_frontier(EPOCH - 91).await;
    let receipt = h.deliver_frame(&recorded, |_| {}).await;
    assert!(h.paper.decision_pending_history().unwrap().is_empty());
    h.control
        .as_ref()
        .unwrap()
        .send(OrchestratorControl::ActivityFrameDecision { receipt })
        .await
        .unwrap();
    h.boot_barrier_readonly().await;
    h.empty_frontier(EPOCH).await;
    let frontiers = h.paper.feed_history_frontiers().unwrap();
    h.stop().await;
    h.start_frames();
    h.boot_barrier().await;
    assert_eq!(h.paper.feed_history_frontiers().unwrap(), frontiers);
    let artifacts = pe_event_log::Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(|frame| frame.unwrap().1)
        .filter(|frame| frame.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID)
        .collect::<Vec<_>>();
    assert_eq!(artifacts.len(), 1);
    let artifact: pe_service::frame_admission::FrameFallbackArtifact =
        serde_json::from_slice(&artifacts[0].payload).unwrap();
    assert_eq!(artifact.frame_receipt, receipt);
    assert_eq!(
        artifact.reason,
        pe_service::frame_admission::FrameFallbackReason::HistoryBehind
    );
    assert_eq!(artifact.frontier.unwrap().fixed_end, EPOCH - 91);
    h.stop().await;
}

#[tokio::test]
async fn frame_frontier_bound_stale_wallets_and_empty_poller_restart() {
    for stale in [90, 91, 5 * 86400] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        h.attempt(&recorded, at());
        h.start_frames();
        h.empty_frontier(EPOCH - stale).await;
        h.deliver_frame(&recorded, |_| {}).await;
        assert_eq!(
            h.paper.decision_pending_history().unwrap().len(),
            usize::from(stale == 90)
        );
        if stale > 90 {
            // The production poller publishes a proofless commitment for an empty complete read.
            let empty = Recorded {
                epoch: EPOCH,
                activity: b"[]".to_vec(),
                gamma: recorded.gamma.clone(),
                id: recorded.id.clone(),
                admission: recorded.admission.clone(),
            };
            let before = h.paper.feed_history_frontiers().unwrap();
            h.poll(&empty).await;
            let stored = h.paper.feed_history_frontiers().unwrap();
            assert_eq!(stored, before);
            h.stop().await;
            h.start_frames();
            h.boot_barrier().await;
            assert_eq!(h.paper.feed_history_frontiers().unwrap(), stored);
            // Restored unresolved BUY blocks only this wallet/market after the frontier refresh.
            h.deliver_frame(&recorded, |row| {
                row["transactionHash"] = json!("blocked-later-buy")
            })
            .await;
            assert!(h.paper.decision_pending_history().unwrap().is_empty());
            let artifacts = Reader::replay(h.dir.path().join("source.log"))
                .unwrap()
                .map(|frame| frame.unwrap().1)
                .filter(|frame| {
                    frame.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID
                })
                .map(|frame| {
                    serde_json::from_slice::<pe_service::frame_admission::FrameFallbackArtifact>(
                        &frame.payload,
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                artifacts.last().unwrap().reason,
                pe_service::frame_admission::FrameFallbackReason::EarlierUnresolvedBuy
            );
            // Another market still falls back while H is stale and this observation is unresolved.
            h.hooks.age_clock.lock().unwrap().clear();
            h.hooks.admission_artifacts.lock().unwrap().clear();
            let next = h.record(2).await;
            h.hooks
                .financial_clock_unix
                .store(next.epoch, Ordering::SeqCst);
            h.attempt(
                &next,
                OffsetDateTime::from_unix_timestamp(next.epoch).unwrap(),
            );
            h.deliver_frame(&next, |_| {}).await;
            assert!(h.paper.decision_pending_history().unwrap().is_empty());
            assert_eq!(h.prepared_count(), 0);
        }
        h.stop().await;
    }
}

#[tokio::test]
async fn frontier_publication_waits_for_ack_and_rejects_failed_or_incomplete_reads() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let before = h.paper.feed_history_frontiers().unwrap();
    *h.books.gate.market.lock().unwrap() = Some(recorded.admission.market.condition_id.0.clone());
    h.books.gate.blocked.store(true, Ordering::SeqCst);
    let gate = h.books.gate.clone();
    {
        let pending = h.poll(&recorded);
        tokio::pin!(pending);
        tokio::select! {
            biased;
            _ = gate.started.notified() => {},
            _ = &mut pending => panic!("read completed before its bucket acknowledgement"),
        }
        assert_eq!(h.paper.feed_history_frontiers().unwrap(), before);
        gate.release.notify_one();
        pending.await;
    }
    assert_eq!(
        h.paper.feed_history_frontiers().unwrap()["frontiers"][0]["fixed_end"],
        EPOCH
    );
    h.stop().await;

    // A transaction failure is acknowledged as a failure; that read cannot publish H.
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let before = h.paper.feed_history_frontiers().unwrap();
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    connection.execute_batch("CREATE TRIGGER fail_frontier_bucket BEFORE INSERT ON activity_groups BEGIN SELECT RAISE(FAIL, 'injected bucket failure'); END;").unwrap();
    assert!(matches!(
        h.poll_source_result(&recorded, None, at(), 0).await,
        Err(pe_service::trade_poller::TradePollerOwnerError::Reconciliation(_))
    ));
    assert_eq!(h.paper.feed_history_frontiers().unwrap(), before);
    assert!(
        h.paper
            .activity_group_state(&recorded.id)
            .unwrap()
            .is_none()
    );
    h.stop().await;
    connection
        .execute_batch("DROP TRIGGER fail_frontier_bucket;")
        .unwrap();
    h.start_frames();
    h.boot_barrier().await;
    assert_eq!(h.paper.feed_history_frontiers().unwrap(), before);
    h.stop().await;

    // The real complete-read owner rejects a malformed page before commitment/publication.
    let mut h = Harness::new().await;
    let mut recorded = h.record(1).await;
    recorded.activity = b"[".to_vec();
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let before = h.paper.feed_history_frontiers().unwrap();
    h.poll(&recorded).await;
    assert_eq!(h.paper.feed_history_frontiers().unwrap(), before);
    h.stop().await;
    h.start_frames();
    h.boot_barrier().await;
    assert_eq!(h.paper.feed_history_frontiers().unwrap(), before);
    h.stop().await;
}

#[tokio::test]
async fn frame_pending_checkpoint_boot_and_prepared_recovery_skip_leader() {
    let mut h = Harness::new().await;
    h.install_checkpoint_metadata();
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    h.authority.fail.store(true, Ordering::SeqCst);
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let receipt = h.append_frame(&recorded, |_| {}).await;
    h.control
        .as_ref()
        .unwrap()
        .send(OrchestratorControl::ActivityFrameDecision { receipt })
        .await
        .unwrap();
    assert!(h.task.take().unwrap().await.unwrap().is_err());
    assert_eq!(h.prepared_count(), 1);
    assert_eq!(h.paper.open_decision_pending().unwrap().len(), 1);
    assert!(h.paper.leader_positions().unwrap().is_empty());
    pe_service::bucket_commit::validate_open_continuations(&h.paper, &h.index).unwrap();
    pe_service::source_log_boot::SourceLogBoot::prepare_checkpoint(&h.dir.path().join("paper.db"))
        .unwrap();
    h.stop().await;
    let writer =
        pe_service::paper_recovery::PaperLog::open(h.dir.path().join("paper.log")).unwrap();
    assert_eq!(
        pe_service::orchestrator::SCENARIO_TERMINAL_CLOCK
            .scope(
                at(),
                reconcile_active_financial_frames(
                    &h.authority,
                    &h.paper,
                    SourceEvidence::Index(&h.index),
                    &writer
                )
            )
            .await
            .unwrap(),
        1
    );
    drop(writer);
    assert!(h.paper.leader_positions().unwrap().is_empty());
    h.start_frames();
    h.boot_barrier().await;
    assert_eq!(
        h.terminal(&recorded).terminal_disposition.as_deref(),
        Some("fill")
    );
    assert_eq!(h.prepared_count(), 1);
    assert!(h.paper.leader_positions().unwrap().is_empty());
    assert_eq!(h.paper.gate_history().unwrap()[&wallet()].len(), 1);
    h.poll(&recorded).await;
    let report = h.qualify_one_fill().await;
    assert!(
        report.replay.exact,
        "{}",
        serde_json::to_string(&report).unwrap()
    );
}

#[tokio::test]
async fn frame_validation_rejects_each_altered_admission_input() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&recorded, |_| {}).await;
    let original = h.terminal(&recorded);
    h.stop().await;
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    let original_wire: Value = serde_json::from_str(&original.frozen_inputs_json).unwrap();
    connection
        .execute(
            "UPDATE decision_pending SET state = 'open' WHERE source_trade_id = ?1",
            [&recorded.id.0],
        )
        .unwrap();
    pe_service::bucket_commit::validate_open_continuations(&h.paper, &h.index).unwrap();
    let mut fact = original_wire.clone();
    fact["price"] = json!("0.60");
    connection
        .execute(
            "UPDATE decision_pending SET frozen_inputs_json = ?1 WHERE source_trade_id = ?2",
            rusqlite::params![fact.to_string(), recorded.id.0],
        )
        .unwrap();
    let error =
        pe_service::bucket_commit::validate_open_continuations(&h.paper, &h.index).unwrap_err();
    assert!(
        error
            .cause
            .contains("frame facts differ from authenticated envelope"),
        "{error}"
    );
    assert!(
        original_wire["decision_inputs"]["inputs"]
            .get("position")
            .is_none()
    );
    // Each negative is a fresh valid source envelope, with its frame revision recomputed.
    // The refusal is a semantic admission invariant, never a decode or receipt-hash error.
    for (pointer, changed, refusal) in [
        (
            "/decision_inputs/inputs/ledger_capture/hash",
            json!("altered"),
            "frozen ledger capture hash differs",
        ),
        (
            "/decision_inputs/inputs/ledger_capture/cursor",
            json!(999),
            "frozen ledger boundary differs",
        ),
        (
            "/decision_inputs/inputs/anchor_balances",
            json!([[0, 1]]),
            "confirmed market position is not an entry",
        ),
        (
            "/decision_inputs/inputs/copy_eligible",
            json!(false),
            "wallet not copy eligible",
        ),
        (
            "/decision_inputs/inputs/history_complete",
            json!(false),
            "wallet history incomplete",
        ),
        (
            "/decision_inputs/inputs/fenced",
            json!(true),
            "wallet fenced",
        ),
        (
            "/decision_inputs/inputs/coverage/reanchor_required",
            json!(true),
            "wallet requires reanchor",
        ),
        (
            "/decision_inputs/inputs/market_consumed",
            json!(true),
            "market history consumed",
        ),
        (
            "/decision_inputs/inputs/admitted_at",
            serde_json::to_value(at() + time::Duration::seconds(91)).unwrap(),
            "history frontier not current",
        ),
        (
            "/decision_inputs/inputs/frontier/fixed_end",
            json!(EPOCH - 2),
            "complete activity read commitment differs from its frozen proof",
        ),
        (
            "/decision_inputs/inputs/identity/provenance/asset",
            json!(recorded.admission.market.ordered_outcome_token_ids[1].0),
            "frame identity provenance differs",
        ),
        (
            "/decision_inputs/inputs/identity/provenance/canonical_page_hash",
            json!("00".repeat(32)),
            "binding metadata has the wrong source contract or page hash",
        ),
        (
            "/decision_inputs/inputs/latch/latest_incident",
            original_wire["observed_source_receipt"].clone(),
            "feed latch engaged",
        ),
    ] {
        connection.execute("UPDATE decision_pending SET state = 'open', semantic_revision = ?1, frozen_inputs_json = ?2 WHERE source_trade_id = ?3", rusqlite::params![original.semantic_revision, original.frozen_inputs_json, recorded.id.0]).unwrap();
        pe_service::bucket_commit::validate_open_continuations(&h.paper, &h.index).unwrap();
        let mut wire = original_wire.clone();
        *wire.pointer_mut(pointer).unwrap() = changed;
        h.authenticate_frame_wire(&mut wire).await;
        let revision = wire["semantic_revision"].as_str().unwrap();
        connection.execute("UPDATE decision_pending SET semantic_revision = ?1, frozen_inputs_json = ?2 WHERE source_trade_id = ?3", rusqlite::params![revision, wire.to_string(), recorded.id.0]).unwrap();
        let error =
            pe_service::bucket_commit::validate_open_continuations(&h.paper, &h.index).unwrap_err();
        assert!(error.cause.contains(refusal), "{pointer}: {error}");
    }
    connection.execute("UPDATE decision_pending SET state = 'terminal', semantic_revision = ?1, frozen_inputs_json = ?2 WHERE source_trade_id = ?3", rusqlite::params![original.semantic_revision, original.frozen_inputs_json, recorded.id.0]).unwrap();
    h.start_frames();
    h.boot_barrier().await;
    h.poll(&recorded).await;
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{:?}", report);
}

impl Harness {
    fn feed_era(&self) -> pe_service::paper_recovery::PaperEra {
        pe_service::paper_recovery::paper_era(
            scan_paper_log(&self.dir.path().join("paper.log")).unwrap(),
        )
    }

    fn feed_edges(
        &self,
    ) -> Vec<(
        AppendReceipt,
        pe_service::paper_recovery::FeedIncident,
        pe_service::paper_recovery::HaltState,
    )> {
        self.feed_era()
            .frames
            .iter()
            .filter_map(|frame| match &frame.frame {
                PaperLogFrame::Record(PaperLogRecord::FeedIncidentChanged { incident, state }) => {
                    Some((frame.receipt, incident.clone(), *state))
                }
                _ => None,
            })
            .collect()
    }

    async fn release_risk_halt(&self, hash: &str) {
        pe_service::config_poller::RiskHaltReleaseHandle::new(
            self.dir.path().join("paper.log"),
            self.index.clone(),
            self.control.as_ref().unwrap().clone(),
        )
        .apply(hash)
        .await
        .unwrap();
        self.assert_frame_barrier(None).await;
    }

    fn rest_counterpart(&self, recorded: &Recorded, change: impl FnOnce(&mut Value)) -> Recorded {
        let mut rows: Value = serde_json::from_slice(&recorded.activity).unwrap();
        change(&mut rows[0]);
        let activity = serde_json::to_vec(&rows).unwrap();
        let read = support::producer_shaped_read_v2(
            wallet(),
            &activity,
            recorded.epoch,
            recorded.epoch,
            support::scenario_receipt(1),
        );
        Recorded {
            epoch: recorded.epoch,
            activity,
            gamma: recorded.gamma.clone(),
            id: read.aggregates[0].group_id.key().clone(),
            admission: recorded.admission.clone(),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn frame_copy_ownership_does_not_suppress_another_transaction_leg() {
    for (rest_first, restart) in [(false, false), (false, true), (true, false), (true, true)] {
        let mut h = Harness::new().await;
        let first = h.record(1).await;
        h.attempt(&first, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        if rest_first {
        } else {
            h.deliver_frame(&first, |_| {}).await;
        }
        h.poll(&first).await;
        h.assert_frame_barrier(None).await;
        let first_terminal = h.terminal(&first);
        if restart {
            h.restart_frames().await;
            h.assert_frame_barrier(None).await;
        }
        let next = h.record(2).await;
        let first_rows: Value = serde_json::from_slice(&first.activity).unwrap();
        let next = h.rest_counterpart(&next, |row| {
            row["transactionHash"] = first_rows[0]["transactionHash"].clone()
        });
        h.hooks
            .financial_clock_unix
            .store(next.epoch, Ordering::SeqCst);
        h.attempt(
            &next,
            OffsetDateTime::from_unix_timestamp(next.epoch).unwrap(),
        );
        let mut read = next.clone();
        if rest_first {
            h.empty_frontier(EPOCH + 1).await;
            h.deliver_frame(&next, |_| {}).await;
            assert!(
                replay_decision_pending(&h.terminal(&next))
                    .unwrap()
                    .continuation
                    .is_activity_frame()
            );
            let mut rows: Value = serde_json::from_slice(&read.activity).unwrap();
            rows.as_array_mut().unwrap().push(first_rows[0].clone());
            read.activity = serde_json::to_vec(&rows).unwrap();
            let mut gamma: Value = serde_json::from_slice(&next.gamma).unwrap();
            gamma.as_array_mut().unwrap().extend(
                serde_json::from_slice::<Value>(&first.gamma)
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .clone(),
            );
            read.gamma = serde_json::to_vec(&gamma).unwrap();
        }
        let next_terminal = rest_first.then(|| h.terminal(&next));
        h.poll(&read).await;
        if rest_first {
            assert_eq!(
                h.paper
                    .activity_group_state(&next.id)
                    .unwrap()
                    .unwrap()
                    .disposition,
                "applied"
            );
            assert_eq!(h.terminal(&next), next_terminal.unwrap());
            h.assert_frame_barrier(None).await;
            h.restart_frames().await;
            h.poll(&read).await;
            assert_eq!(h.paper.decision_pending_history().unwrap().len(), 2);
            assert!(h.feed_edges().is_empty());
            assert_eq!(h.paper.leader_positions().unwrap().len(), 2);
        }
        assert!(h.feed_edges().is_empty());
        assert_eq!(h.terminal(&first), first_terminal);
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 2);
        assert_eq!(
            h.terminal(&next).terminal_disposition.as_deref(),
            Some("fill")
        );
        assert_eq!(h.paper.leader_positions().unwrap().len(), 2);
        for (_, commitment) in read_commitments(&h) {
            assert!(commitment.bindings.unwrap_or_default().is_empty());
        }
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "restart={restart}: {report:?}");
        assert_eq!(report.replay.fills, 2);
    }
}

#[tokio::test(start_paused = true)]
async fn independent_risk_halt_release_still_applies() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&recorded, |_| {}).await;
    let (acknowledged, response) = tokio::sync::oneshot::channel();
    h.control
        .as_ref()
        .unwrap()
        .send(OrchestratorControl::RiskHaltChange {
            owner: pe_service::paper_recovery::RiskHaltOwner::Paper,
            cause: pe_risk_engine::RiskHaltCause::AbsoluteLoss,
            state: pe_service::paper_recovery::HaltState::Engaged,
            evidence: json!({"fixture":"independent cause"}),
            acknowledged,
        })
        .await
        .unwrap();
    let risk = response.await.unwrap().unwrap();
    h.release_risk_halt(risk.this_hash.to_hex().as_str()).await;
    assert!(pe_service::paper_recovery::active_risk_halts(&h.feed_era()).is_empty());
    assert!(h.feed_edges().is_empty());
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn frontier_fallback_five_day_wallet_beside_current_wallet() {
    let other = WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let mut h = Harness::new_with_leaders(3, runtime(), vec![wallet(), other]).await;
    let stale = h.record(1).await;
    let mut current = stale.clone();
    let mut rows: Value = serde_json::from_slice(&current.activity).unwrap();
    rows[0]["proxyWallet"] = json!(other.to_string());
    rows[0]["transactionHash"] = json!("current-wallet");
    current.activity = serde_json::to_vec(&rows).unwrap();
    current.id = support::producer_shaped_read_v2(
        other,
        &current.activity,
        EPOCH,
        EPOCH,
        support::scenario_receipt(1),
    )
    .aggregates[0]
        .group_id
        .key()
        .clone();
    h.start_frames();
    h.empty_frontier(EPOCH - 5 * 86400).await;
    h.empty_frontier_for(other, EPOCH - 1).await;
    h.deliver_frame(&stale, |_| {}).await;
    h.attempt(&current, at());
    h.deliver_frame(&current, |_| {}).await;
    assert!(h.paper.decision_pending_for(&stale.id).unwrap().is_none());
    assert_eq!(
        h.terminal(&current).terminal_disposition.as_deref(),
        Some("fill")
    );
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn queue_shutdown_crashes() {
    use pe_service::orchestrator::FrameCrashBoundary;
    for boundary in [
        FrameCrashBoundary::FrameCommit,
        FrameCrashBoundary::Staging,
        FrameCrashBoundary::Prepared,
        FrameCrashBoundary::Authority,
        FrameCrashBoundary::Final,
    ] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        h.arm();
        h.attempt(&recorded, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        *h.hooks.frame_crash_boundary.lock().unwrap() = Some(boundary);
        let receipt = h.append_frame(&recorded, |_| {}).await;
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt })
            .await
            .unwrap();
        assert!(
            h.task.take().unwrap().await.unwrap().is_err(),
            "{boundary:?}"
        );
        assert_eq!(h.paper.open_decision_pending().unwrap().len(), 1);
        assert!(h.paper.leader_positions().unwrap().is_empty());
        h.stop().await;
        let writer =
            pe_service::paper_recovery::PaperLog::open(h.dir.path().join("paper.log")).unwrap();
        pe_service::orchestrator::SCENARIO_TERMINAL_CLOCK
            .scope(
                at(),
                reconcile_active_financial_frames(
                    &h.authority,
                    &h.paper,
                    SourceEvidence::Index(&h.index),
                    &writer,
                ),
            )
            .await
            .unwrap();
        drop(writer);
        h.hooks.age_clock.lock().unwrap().clear();
        h.hooks.admission_artifacts.lock().unwrap().clear();
        h.attempt(&recorded, at());
        h.start_frames();
        h.boot_barrier().await;
        assert_eq!(
            h.terminal(&recorded).terminal_disposition.as_deref(),
            Some("fill"),
            "{boundary:?}"
        );
        assert_eq!(h.prepared_count(), 1, "{boundary:?}");
        assert_eq!(h.authority.inner.lock().unwrap().fills.len(), 1);
        assert_eq!(identifier_counts(&h, &recorded.id), (0, 1, 1, 0));
        h.poll(&recorded).await;
        assert_eq!(identifier_counts(&h, &recorded.id), (1, 1, 1, 0));
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "{:?}", report);
    }
    for boundary in [
        pe_service::trade_poller::ReconciliationCrashBoundary::Commitment,
        pe_service::trade_poller::ReconciliationCrashBoundary::Bucket,
    ] {
        let mut h = Harness::new().await;
        h.poller_boot_rebuild = true;
        let recorded = h.record(1).await;
        h.attempt(&recorded, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.deliver_frame(&recorded, |_| {}).await;
        let terminal = h.terminal(&recorded);
        *h.poller_crash.lock().unwrap() = Some(boundary);
        assert!(
            h.poll_source_result(&recorded, None, at(), 0)
                .await
                .is_err()
        );
        h.restart_frames().await;
        let mut empty = recorded.clone();
        empty.epoch = EPOCH + 1;
        h.poll(&empty).await;
        assert!(
            h.feed_edges().is_empty(),
            "retained authenticated match lost at {boundary:?}"
        );
        h.assert_frame_barrier(None).await;
        assert_eq!(h.terminal(&recorded), terminal);
        assert_eq!(
            h.paper.leader_positions().unwrap()[0]
                .long_contracts
                .to_decimal(),
            dec!(5)
        );
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 1);
        assert_eq!(identifier_counts(&h, &recorded.id), (1, 1, 0, 0));
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "{:?}", report);
    }
}

#[tokio::test(start_paused = true)]
async fn equal_identifier_reconciliation_preserves_later_frame_no_copy_reason() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    *h.hooks.frame_crash_boundary.lock().unwrap() =
        Some(pe_service::orchestrator::FrameCrashBoundary::FrameCommit);
    let receipt = h.append_frame(&recorded, |_| {}).await;
    h.control
        .as_ref()
        .unwrap()
        .send(OrchestratorControl::ActivityFrameDecision { receipt })
        .await
        .unwrap();
    assert!(h.task.take().unwrap().await.unwrap().is_err());
    h.stop().await;
    let page = h
        .source
        .append(EnvelopeIn {
            source_id: SourceId(pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
            schema_version: 3,
            parser_version: 2,
            observed_at: SourceTimestamp(at()),
            received_at: ReceivedAt(at()),
            content_type: ContentType::Json,
            payload: recorded.activity.clone(),
        })
        .await
        .unwrap();
    let read = support::producer_shaped_read_v2(wallet(), &recorded.activity, EPOCH, EPOCH, page);
    let commitment = h
        .source
        .append(EnvelopeIn {
            source_id: SourceId(
                pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned(),
            ),
            schema_version: 2,
            parser_version: 1,
            observed_at: SourceTimestamp(at()),
            received_at: ReceivedAt(at()),
            content_type: ContentType::Json,
            payload: read.commitment_payload.clone(),
        })
        .await
        .unwrap();
    let mut context = support::read_context(&read, commitment, EPOCH);
    context.applied_configuration = h.config.clone();
    let pages =
        serde_json::from_str::<Value>(&context.decision_inputs_json).unwrap()["pages"].clone();
    context.verified_read =
        Some(Arc::new(
            pe_service::bucket_commit::verified_read_for_routing(
                commitment,
                wallet(),
                EPOCH,
                &context.page_occurrences,
                &serde_json::from_value::<
                    Vec<pe_source_polymarket_public::ReconciliationPageEvidence>,
                >(pages)
                .unwrap(),
                &h.index,
            )
            .unwrap(),
        ));
    let mut engine =
        BucketCommitEngine::load(h.paper.clone(), build_leader_ledger(&h.paper).unwrap())
            .unwrap()
            .with_source_receipt_index(h.index.clone());
    engine
        .commit_with_freshness_policy(
            read.aggregates,
            &context,
            FrozenDecisionBasis {
                win_rate_p: h.probability,
                bankroll: CASH,
            },
            Some(PaperFreshnessPolicy {
                activity_ws_enabled: true,
                copy_latency_budget_secs: h.copy_budget_secs,
            }),
        )
        .unwrap();
    assert_eq!(identifier_counts(&h, &recorded.id), (1, 0, 0, 0));
    h.attempt(&recorded, at());
    h.hooks.age_clock.lock().unwrap().clear();
    h.hooks
        .age_clock
        .lock()
        .unwrap()
        .push_back(at() + time::Duration::seconds(3));
    h.start_frames();
    h.boot_barrier().await;
    let terminal = h.terminal(&recorded);
    let reason = replay_decision_pending(&terminal)
        .unwrap()
        .post_boundary
        .body
        .terminal
        .reason;
    assert_eq!(reason, "stale_activity_ws_past_copy_budget");
    assert_eq!(
        h.paper
            .no_copy_disposition(&recorded.id)
            .unwrap()
            .unwrap()
            .2,
        reason
    );
    assert_eq!(identifier_counts(&h, &recorded.id), (1, 0, 0, 1));
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn historical_mixed_buy_corrected_sell_checkpoint_boot_and_qualification() {
    let mut h = Harness::new().await;
    h.poller_boot_rebuild = true;
    h.install_checkpoint_metadata();
    let recorded = h.record(1).await;
    h.start(true);
    let buy = h.append_frame(&recorded, |_| {}).await;
    let sell = h
        .append_frame(&recorded, |row| {
            row["side"] = json!("SELL");
            row["conditionId"] = json!(format!("0x{:064x}", 999));
        })
        .await;
    let mut mixed = recorded.clone();
    let mut rows: Value = serde_json::from_slice(&recorded.activity).unwrap();
    let mut sold = rows[0].clone();
    sold["side"] = json!("SELL");
    rows.as_array_mut().unwrap().push(sold);
    mixed.activity = serde_json::to_vec(&rows).unwrap();
    h.poll(&mixed).await;
    let bindings = pe_event_log::Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(|frame| frame.unwrap().1)
        .filter(|frame| {
            frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        })
        .flat_map(|frame| {
            serde_json::from_slice::<pe_service::bucket_commit::ActivityReadCommitment>(
                &frame.payload,
            )
            .unwrap()
            .bindings
            .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let exact = bindings
        .iter()
        .find(|binding| binding.stream_receipt == buy)
        .unwrap();
    let corrected = bindings
        .iter()
        .find(|binding| binding.stream_receipt == sell)
        .unwrap();
    assert!(exact.frame_admission_receipt.is_none());
    assert!(corrected.frame_admission_receipt.is_none());
    assert!(exact.identity_receipt.is_none());
    assert!(corrected.identity_receipt.is_some());
    assert_ne!(exact.history_group_id, corrected.history_group_id);
    assert!(h.feed_edges().is_empty());
    let next = h.record(2).await;
    h.stop().await;
    h.hooks
        .financial_clock_unix
        .store(next.epoch, Ordering::SeqCst);
    h.terminal_clock = OffsetDateTime::from_unix_timestamp(next.epoch).unwrap();
    h.start_frames();
    h.boot_barrier().await;
    h.attempt(&next, h.terminal_clock);
    h.poll(&next).await;
    assert_eq!(
        h.terminal(&next).terminal_disposition.as_deref(),
        Some("fill")
    );
    pe_service::source_log_boot::SourceLogBoot::prepare_checkpoint(&h.dir.path().join("paper.db"))
        .unwrap();
    let report = h.qualify_one_fill().await;
    assert!(
        report.replay.exact,
        "{}",
        serde_json::to_string(&report).unwrap()
    );
    h.coordinator.abort();
    let _ = (&mut h.coordinator).await;
    // Remove only the generated test checkpoint to exercise the complete source walk.
    std::fs::remove_file(h.dir.path().join("source.log.boot-checkpoint")).unwrap();
    let paths = pe_service::paper_migration::PaperMigrationPaths {
        fixed_main: h.dir.path().join("paper.db"),
        source_log: h.dir.path().join("source.log"),
        paper_log: h.dir.path().join("paper.log"),
        live_journal: h.dir.path().join("live_journal.log"),
        legacy_history: h.dir.path().join("wallet_market_history.json"),
        binary_identity: "mixed-leg-scenario".to_owned(),
    };
    h.paper
        .record_migration_activation_facts(
            &json!({"fixture":"synthetic mixed-leg activation"}),
            &paths.binary_identity,
        )
        .unwrap();
    let mut opened = pe_service::source_log_boot::SourceLogBoot::open(&paths, true)
        .unwrap()
        .unwrap();
    opened.boot.extend(&mut opened.sink).unwrap();
    let obligations = opened.boot.obligations(&h.paper, &paths.paper_log).unwrap();
    assert!(obligations.is_empty());
}

#[tokio::test(start_paused = true)]
async fn qualification_semantic_negatives_keep_valid_frame_admission_and_frontier_hashes() {
    for (fact, refusal) in [
        ("frame", "frame facts differ from authenticated envelope"),
        (
            "capture_digest",
            "frame admission prefix differs from authenticated capture",
        ),
        ("admission", "wallet not copy eligible"),
        (
            "frontier",
            "complete activity read commitment differs from its frozen proof",
        ),
        (
            "paper_prefix",
            "empty frame paper prefix follows existing paper evidence",
        ),
    ] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        h.attempt(&recorded, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.deliver_frame(&recorded, |_| {}).await;
        h.poll(&recorded).await;
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "{:?}", report);
        let original = h.terminal(&recorded);
        let mut wire: Value = serde_json::from_str(&original.frozen_inputs_json).unwrap();
        {
            if fact == "frame" {
                wire["price"] = json!("0.60");
            } else {
                if fact == "admission" {
                    wire["decision_inputs"]["inputs"]["copy_eligible"] = json!(false);
                } else if fact == "paper_prefix" {
                    wire["decision_inputs"]["inputs"]["paper_prefix"] = Value::Null;
                } else if fact != "capture_digest" {
                    wire["decision_inputs"]["inputs"]["frontier"]["fixed_end"] = json!(EPOCH - 2);
                }
                h.authenticate_frame_wire(&mut wire).await;
                if fact == "capture_digest" {
                    let inputs: pe_service::frame_admission::FrameAdmissionInputs =
                        serde_json::from_value(wire["decision_inputs"]["inputs"].clone()).unwrap();
                    let mut artifact =
                        pe_service::frame_admission::FrameAdmissionArtifact::from_inputs(&inputs)
                            .unwrap();
                    artifact.capture_digest = "00".repeat(32);
                    let receipt = h
                        .source
                        .append(EnvelopeIn {
                            source_id: SourceId(
                                pe_service::frame_admission::FRAME_ADMISSION_SOURCE_ID.to_owned(),
                            ),
                            schema_version: 1,
                            parser_version: 1,
                            observed_at: SourceTimestamp(inputs.admitted_at),
                            received_at: ReceivedAt(inputs.admitted_at),
                            content_type: ContentType::Json,
                            payload: serde_json::to_vec(&artifact).unwrap(),
                        })
                        .await
                        .unwrap();
                    wire["decision_inputs"]["admission_receipt"] =
                        serde_json::to_value(receipt).unwrap();
                }
            }
            rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap().execute(
                "UPDATE decision_pending SET semantic_revision=?1, frozen_inputs_json=?2 WHERE source_trade_id=?3",
                rusqlite::params![wire["semantic_revision"].as_str().unwrap(), wire.to_string(), recorded.id.0],
            ).unwrap();
        }
        let report = h.requalify_current_rows().await;
        assert_eq!(
            report.verdict,
            pe_service::qualification::QualificationVerdict::InsufficientEvidence
        );
        assert!(
            report.reasons.iter().any(|reason| reason.contains(refusal)),
            "{fact}: {:?}",
            report.reasons
        );
    }
}

impl Harness {
    async fn authenticate_frame_wire(&self, wire: &mut Value) {
        let inputs: pe_service::frame_admission::FrameAdmissionInputs =
            serde_json::from_value(wire["decision_inputs"]["inputs"].clone()).unwrap();
        let admission = self
            .source
            .append(EnvelopeIn {
                source_id: SourceId(
                    pe_service::frame_admission::FRAME_ADMISSION_SOURCE_ID.to_owned(),
                ),
                schema_version: u32::from(inputs.version),
                parser_version: 1,
                observed_at: SourceTimestamp(inputs.admitted_at),
                received_at: ReceivedAt(inputs.admitted_at),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(
                    &pe_service::frame_admission::FrameAdmissionArtifact::from_inputs(&inputs)
                        .unwrap(),
                )
                .unwrap(),
            })
            .await
            .unwrap();
        wire["decision_inputs"]["admission_receipt"] = serde_json::to_value(admission).unwrap();
        wire["semantic_revision"] = json!(
            pe_service::frame_admission::FrameAdmissionArtifact::from_inputs(&inputs)
                .unwrap()
                .capture_digest
        );
    }
}

struct HeldPoll {
    _triggers: mpsc::Sender<pe_service::activity_ingest::ReconciliationTrigger>,
    pages: mpsc::Receiver<support::RequestedPage>,
    progress: mpsc::Receiver<pe_service::trade_poller::PollerProgress>,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), pe_service::trade_poller::TradePollerOwnerError>>,
    clock: Arc<AtomicI64>,
}
impl HeldPoll {
    async fn finish(mut self, response: support::RequestedPage, payload: Vec<u8>) {
        response.respond.send(payload).unwrap();
        while let Some(progress) = self.progress.recv().await {
            if matches!(
                progress,
                pe_service::trade_poller::PollerProgress::RoundCompleted
            ) {
                break;
            }
        }
        let _ = self.stop.send(());
        self.task.await.unwrap().unwrap();
    }
}
impl Harness {
    fn held_poll(&self, recorded: &Recorded, receipt: Option<AppendReceipt>) -> HeldPoll {
        self.held_poll_with_source(recorded, receipt, self.source.clone())
    }
    fn held_poll_with_source(
        &self,
        recorded: &Recorded,
        receipt: Option<AppendReceipt>,
        source: SourceLogHandle,
    ) -> HeldPoll {
        self.held_poll_with_source_and_control(
            recorded,
            receipt,
            source,
            self.control.as_ref().unwrap().clone(),
            false,
            self.watchlist.clone(),
        )
    }

    fn held_poll_with_source_and_control(
        &self,
        recorded: &Recorded,
        receipt: Option<AppendReceipt>,
        source: SourceLogHandle,
        control: mpsc::Sender<OrchestratorControl>,
        activity_ws_enabled: bool,
        watchlist: LiveWatchlist,
    ) -> HeldPoll {
        let (requests, pages) = mpsc::channel(4);
        let (progress, completion) = mpsc::channel(8);
        let (triggers, receiver) = mpsc::channel(1);
        let mut obligations = pe_service::trade_poller::ReconciliationObligations::default();
        if let Some(receipt) = receipt {
            let source = Reader::replay(self.dir.path().join("source.log"))
                .unwrap()
                .map(|frame| frame.unwrap().1)
                .find(|source| {
                    source.seq == receipt.sequence && source.this_hash == receipt.this_hash
                })
                .unwrap();
            let observation =
                pe_source_polymarket_public::parse_activity_trade_observation(&source.payload)
                    .unwrap();
            obligations.insert(pe_service::activity_ingest::ReconciliationTrigger {
                qualifying_buy: observation.group_id.components().side
                    == Some(pe_core_types::Side::Buy)
                    && observation.share_amount != pe_core_types::ShareAmount::ZERO
                    && !observation.is_combo,
                wallet: wallet(),
                source_time: observation.source_time.0,
                source_trade_id: observation.group_id.key().clone(),
                provenance: pe_copy_signal_engine::TradeProvenance::ActivityWs,
                received_at: source.received_at.0,
                receipt,
            });
        }
        let clock = Arc::new(AtomicI64::new(EPOCH));
        let poll_clock = clock.clone();
        let poller = TradePoller::new(
            TradePollerConfig {
                base_url: "fixture://activity".to_owned(),
                poll_interval_secs: 30,
                activity_ws_enabled,
                copy_latency_budget_secs: self.copy_budget_secs,
            },
            watchlist,
            Arc::new(support::GatedFetcher { requests }),
            Arc::new(AssetIdentityResolver::new_runtime(
                Arc::new(Page(recorded.gamma.clone())),
                "fixture://gamma".to_owned(),
                GAMMA_BATCH_SIZE,
                self.source.clone(),
            )),
            source,
            receiver,
            control,
            self.paper.clone(),
            new_shared_health_with_ws(false, true, 90),
            SignalConfig::default(),
            LiveRuntimeConfig::new(self.config.clone()),
            obligations,
            None,
        )
        .with_source_receipt_index(self.index.clone())
        .with_crash_boundary(self.poller_crash.clone())
        .with_progress(progress)
        .with_clock(Arc::new(move || {
            OffsetDateTime::from_unix_timestamp(poll_clock.load(Ordering::SeqCst)).unwrap()
        }));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(poller.run_until(async move {
            let _ = stopped.await;
        }));
        HeldPoll {
            _triggers: triggers,
            pages,
            progress: completion,
            stop,
            task,
            clock,
        }
    }
}

#[tokio::test(start_paused = true)]
async fn admission_during_rest_read_keeps_independent_asset() {
    for trigger_first in [false, true] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        let other = h.record(2).await;
        h.attempt(&recorded, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        let receipt = h.append_frame(&recorded, |_| {}).await;
        let mut counterpart = h.rest_counterpart(&recorded, |row| {
            row["conditionId"] = json!(other.admission.market.condition_id.0);
            row["asset"] = json!(other.admission.market.ordered_outcome_token_ids[0].0);
            row["timestamp"] = json!(EPOCH + 1);
        });
        counterpart.gamma = other.gamma;
        counterpart.epoch = EPOCH + 1;
        h.attempt(&counterpart, at() + time::Duration::seconds(1));
        let mut held = h.held_poll(&counterpart, trigger_first.then_some(receipt));
        held.clock.store(EPOCH + 1, Ordering::SeqCst);
        let response = held.pages.recv().await.unwrap();
        assert!(
            h.paper
                .decision_pending_for(&recorded.id)
                .unwrap()
                .is_none()
        );
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt })
            .await
            .unwrap();
        h.boot_barrier_readonly().await;
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 1);
        h.hooks
            .financial_clock_unix
            .store(EPOCH + 1, Ordering::SeqCst);
        held.finish(response, counterpart.activity.clone()).await;
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 2);
        assert_eq!(h.paper.leader_positions().unwrap().len(), 1);
        assert_eq!(
            h.paper.leader_positions().unwrap()[0]
                .long_contracts
                .to_decimal(),
            dec!(5)
        );
        assert!(h.feed_edges().is_empty());
        let terminal = h.terminal(&recorded);
        h.stop().await;
        h.start_frames();
        h.boot_barrier().await;
        assert_eq!(h.terminal(&recorded), terminal);
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 2);
        assert_eq!(h.paper.leader_positions().unwrap().len(), 1);
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "{:?}", report);
    }
}

#[tokio::test(start_paused = true)]
async fn frame_capture_size_is_independent_of_unrelated_wallet_state() {
    let mut sizes = Vec::new();
    for large in [false, true] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        if large {
            let mut engine =
                BucketCommitEngine::load(h.paper.clone(), build_leader_ledger(&h.paper).unwrap())
                    .unwrap();
            let capture =
                pe_service::position_seeder::ledger_capture(engine.ledger(), &h.paper, wallet())
                    .unwrap();
            engine
                .install_anchors(&[pe_service::position_seeder::AnchorInstall {
                    wallet: wallet(),
                    balances: (0..4000)
                        .map(|ordinal| {
                            (
                                pe_core_types::MarketId(pe_core_types::VenueMarketId(format!(
                                    "unrelated-position-{ordinal}"
                                ))),
                                pe_core_types::OutcomeId(0),
                                pe_core_types::ShareAmount::from_whole(1).unwrap(),
                            )
                        })
                        .collect(),
                    cutoff: 0,
                    fresh_history: Vec::new(),
                    expected_fence: None,
                    history_status: None,
                    expected: pe_service::position_seeder::AnchorExpectation {
                        ledger_hash: capture.hash,
                        cursor: capture.cursor,
                        anchor_seq: capture.anchor_seq,
                        coverage_generation: capture.coverage_generation,
                    },
                    proof: pe_service::position_seeder::AnchorProof {
                        positions_proof_hash: "synthetic-large".to_owned(),
                        activity_bounds_json: "[]".to_owned(),
                        source_log_generation: "scenario".to_owned(),
                        document: "{}".to_owned(),
                        recorded_at_unix: 0,
                    },
                }])
                .unwrap();
            let mut connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
            let transaction = connection.transaction().unwrap();
            for ordinal in 0..30_000 {
                transaction.execute("INSERT INTO wallet_market_history_v2(wallet_hex,market_id,first_epoch,source_trade_id,origin) VALUES (?1,?2,0,'synthetic-history','activity_v2')", rusqlite::params![wallet().to_string(), format!("unrelated-consumed-{ordinal}")]).unwrap();
            }
            transaction.commit().unwrap();
            let other = WalletAddress([0xbb; 20]);
            for ordinal in 0..3 {
                let rows: Value = serde_json::from_slice(&recorded.activity).unwrap();
                let mut frame = rows[0].clone();
                frame["proxyWallet"] = serde_json::to_value(other).unwrap();
                frame["transactionHash"] = json!(format!("0xresolved-{ordinal}"));
                let payload = serde_json::to_vec(&frame).unwrap();
                let observation =
                    pe_source_polymarket_public::parse_activity_trade_observation(&payload)
                        .unwrap();
                h.source
                    .append(EnvelopeIn {
                        source_id: SourceId(
                            pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID.to_owned(),
                        ),
                        schema_version: 2,
                        parser_version: 2,
                        observed_at: SourceTimestamp(at()),
                        received_at: ReceivedAt(at()),
                        content_type: ContentType::Json,
                        payload,
                    })
                    .await
                    .unwrap();
                connection.execute("INSERT INTO activity_groups(source_trade_id,transaction_hash,wallet_hex,source_epoch,semantic_revision,activity_type,disposition,proof_json) VALUES (?1,?2,?3,?4,'synthetic-resolved','TRADE','raw_only',?5)",
                    rusqlite::params![observation.group_id.key().0, observation.group_id.components().transaction_hash, other.to_string(), EPOCH, r#"{"effect":{"kind":"raw_only"}}"#]).unwrap();
            }
        }
        h.attempt(&recorded, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.deliver_frame(&recorded, |_| {}).await;
        let wire: Value = serde_json::from_str(&h.terminal(&recorded).frozen_inputs_json).unwrap();
        let capture = &wire["decision_inputs"]["inputs"];
        assert_eq!(capture["anchor_balances"], json!([]));
        assert_eq!(capture["ledger_groups"], json!([]));
        assert_eq!(capture["earlier_frames"], json!([]));
        sizes.push(serde_json::to_vec(capture).unwrap().len());
        h.poll(&recorded).await;
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "{report:?}");
    }
    assert!(sizes[1] <= sizes[0] + 256, "capture sizes: {sizes:?}");
}

impl Harness {
    async fn install_runtime_anchor(
        &self,
        cutoff: i64,
        balances: Vec<(
            pe_core_types::MarketId,
            pe_core_types::OutcomeId,
            pe_core_types::ShareAmount,
        )>,
    ) {
        let (captured, received) = tokio::sync::oneshot::channel();
        self.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::CaptureAdmissionLedger {
                wallet: wallet(),
                captured,
            })
            .await
            .unwrap();
        let capture = received.await.unwrap().unwrap();
        let (acknowledged, response) = tokio::sync::oneshot::channel();
        self.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::InstallAnchors {
                installs: vec![pe_service::position_seeder::AnchorInstall {
                    wallet: wallet(),
                    balances,
                    cutoff,
                    fresh_history: Vec::new(),
                    expected_fence: self.paper.wallet_fence(&wallet()).unwrap(),
                    history_status: Some(pe_paper_state::WalletHistoryStatusRecord {
                        wallet: wallet(),
                        complete: true,
                        proof_json: "{}".to_owned(),
                        updated_at_unix: cutoff,
                    }),
                    expected: pe_service::position_seeder::AnchorExpectation {
                        ledger_hash: capture.hash,
                        cursor: capture.cursor,
                        anchor_seq: capture.anchor_seq,
                        coverage_generation: capture.coverage_generation,
                    },
                    proof: pe_service::position_seeder::AnchorProof {
                        positions_proof_hash: "synthetic-anchor".to_owned(),
                        activity_bounds_json: "[]".to_owned(),
                        source_log_generation: "scenario".to_owned(),
                        document: "{}".to_owned(),
                        recorded_at_unix: cutoff,
                    },
                }],
                acknowledged,
            })
            .await
            .unwrap();
        response.await.unwrap().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn covered_and_fenced_matches_retire_barriers_before_later_wallet_frame() {
    for route in ["covered", "fenced"] {
        for restart in [false, true] {
            let mut h = Harness::new().await;
            let first = h.record(1).await;
            h.attempt(&first, at());
            h.start_frames();
            h.empty_frontier(EPOCH - 1).await;
            h.deliver_frame(&first, |_| {}).await;
            if route == "covered" {
                h.install_runtime_anchor(
                    EPOCH,
                    vec![(
                        pe_core_types::MarketId(pe_core_types::VenueMarketId(
                            first.admission.market.condition_id.0.clone(),
                        )),
                        pe_core_types::OutcomeId(0),
                        pe_core_types::ShareAmount::from_whole(5).unwrap(),
                    )],
                )
                .await;
            } else {
                h.stop().await;
                rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap().execute(
                    "INSERT INTO wallet_fences(wallet_hex,source_trade_id,cause,proof_json,fenced_at_unix) VALUES (?1,?2,'position_underflow',?3,?4)",
                    rusqlite::params![wallet().to_string(), first.id.0, json!({"bucket_epoch": EPOCH}).to_string(), EPOCH],
                ).unwrap();
                h.start_frames();
                h.boot_barrier().await;
            }
            h.poll(&first).await;
            assert_eq!(
                h.paper
                    .activity_group_state(&first.id)
                    .unwrap()
                    .unwrap()
                    .disposition,
                if route == "covered" {
                    "anchor_covered_late"
                } else {
                    "wallet_fenced_applied"
                }
            );
            assert_eq!(
                pe_service::trade_poller::rebuild_reconciliation_obligations(
                    &h.dir.path().join("source.log"),
                    &h.paper
                )
                .unwrap()
                .len(),
                0
            );
            // A late covered match requests reanchoring; a recoverable fence does too.
            // Restore readiness through the production anchor owner before testing ordering.
            let balances = h
                .paper
                .leader_positions()
                .unwrap()
                .into_iter()
                .map(|row| (row.market_id, row.outcome_id, row.long_contracts))
                .collect();
            h.install_runtime_anchor(EPOCH + 1, balances).await;
            assert!(!h.paper.is_wallet_fenced(&wallet()).unwrap());
            assert!(
                !h.paper
                    .wallet_coverage(&wallet())
                    .unwrap()
                    .reanchor_required
            );
            if restart {
                h.stop().await;
                h.start_frames();
                h.boot_barrier().await;
            }
            // A fresh frontier after the old observation's stale bound exposes any surviving
            // wallet-age barrier even though the next market has no BUY barrier of its own.
            h.hooks
                .financial_clock_unix
                .store(EPOCH + 100, Ordering::SeqCst);
            h.empty_frontier(EPOCH + 99).await;
            let next = h.record(101).await;
            h.attempt(&next, at() + time::Duration::seconds(100));
            h.deliver_frame(&next, |_| {}).await;
            let row = h
                .paper
                .decision_pending_for(&next.id)
                .unwrap()
                .unwrap_or_else(|| {
                    panic!("{route}, restart={restart}: later frame was not admitted")
                });
            assert!(
                replay_decision_pending(&row)
                    .unwrap()
                    .continuation
                    .is_activity_frame(),
                "{route}, restart={restart}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn split_second_transaction_legs_use_one_authenticated_complete_read() {
    let mut h = Harness::new().await;
    let frame = h.record(1).await;
    let mut independent = h.record(2).await;
    let mut independent_rows: Value = serde_json::from_slice(&independent.activity).unwrap();
    let frame_rows: Value = serde_json::from_slice(&frame.activity).unwrap();
    independent_rows[0]["transactionHash"] = frame_rows[0]["transactionHash"].clone();
    independent.activity = serde_json::to_vec(&independent_rows).unwrap();
    independent.id = pe_source_polymarket_public::parse_activity_trade_observation(
        &serde_json::to_vec(&independent_rows[0]).unwrap(),
    )
    .unwrap()
    .group_id
    .key()
    .clone();
    h.attempt(&frame, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&frame, |_| {}).await;
    let terminal = h.terminal(&frame);
    let mut rows = frame_rows.as_array().unwrap().clone();
    rows.push(independent_rows[0].clone());
    // Many distinct seconds route through the serialized owner, including before the frame.
    for ordinal in 1..=256 {
        let mut raw = frame_rows[0].clone();
        raw["transactionHash"] = json!(format!("raw-only-{ordinal}"));
        raw["timestamp"] = json!(EPOCH - ordinal);
        raw["size"] = json!("0");
        raw["usdcSize"] = json!("0");
        rows.push(raw);
    }
    let mut gamma: Value = serde_json::from_slice(&frame.gamma).unwrap();
    gamma.as_array_mut().unwrap().extend(
        serde_json::from_slice::<Value>(&independent.gamma)
            .unwrap()
            .as_array()
            .unwrap()
            .clone(),
    );
    let read = Recorded {
        epoch: independent.epoch,
        activity: serde_json::to_vec(&rows).unwrap(),
        gamma: serde_json::to_vec(&gamma).unwrap(),
        id: independent.id.clone(),
        admission: independent.admission.clone(),
    };
    h.attempt(&independent, at() + time::Duration::seconds(1));
    h.poll(&read).await;
    assert_eq!(h.terminal(&frame), terminal);
    assert_eq!(
        h.terminal(&independent).terminal_disposition.as_deref(),
        Some("fill")
    );
    assert_eq!(h.paper.decision_pending_history().unwrap().len(), 2);
    assert_eq!(identifier_counts(&h, &frame.id), (1, 1, 0, 0));
    assert_eq!(identifier_counts(&h, &independent.id), (1, 1, 0, 0));
    assert!(h.feed_edges().is_empty());
    let commitment = Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(|frame| frame.unwrap().1)
        .filter(|frame| {
            frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        })
        .last()
        .unwrap();
    let receipt = AppendReceipt {
        sequence: commitment.seq,
        this_hash: commitment.this_hash,
    };
    assert_eq!(h.index.read_verification_count(receipt), 1);
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(
            &h.dir.path().join("source.log"),
            &h.paper,
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(h.paper.leader_positions().unwrap().len(), 2);
}

#[tokio::test(start_paused = true)]
async fn rest_zero_share_refusal_owns_identity_before_positive_frame_and_restart() {
    for restart in [false, true] {
        let mut h = Harness::new().await;
        let positive = h.record(1).await;
        let mut zero = h.rest_counterpart(&positive, |row| {
            row["size"] = json!("0");
            row["usdcSize"] = json!("0");
        });
        assert_eq!(zero.id, positive.id);
        zero.epoch = EPOCH - 10;
        let mut rows: Value = serde_json::from_slice(&zero.activity).unwrap();
        rows[0]["timestamp"] = json!(zero.epoch);
        zero.activity = serde_json::to_vec(&rows).unwrap();
        h.start_frames();
        // Exercise the production stale REST refusal beside a raw-only zero-share effect.
        h.poll_source(&zero, Some(zero.epoch), at()).await;
        let counts = identifier_counts(&h, &positive.id);
        assert_eq!(counts, (1, 0, 0, 1));
        assert!(h.paper.gate_history().unwrap()[&wallet()].is_empty());
        assert!(
            h.paper
                .decision_pending_for(&positive.id)
                .unwrap()
                .is_none()
        );
        if restart {
            h.stop().await;
            h.paper = Arc::new(PaperStateDb::open(&h.dir.path().join("paper.db")).unwrap());
            h.start_frames();
            h.boot_barrier().await;
        }
        h.deliver_frame(&positive, |_| {}).await;
        assert!(
            h.paper
                .decision_pending_for(&positive.id)
                .unwrap()
                .is_none()
        );
        assert_eq!(identifier_counts(&h, &positive.id), counts);
        let artifacts = Reader::replay(h.dir.path().join("source.log"))
            .unwrap()
            .map(|frame| frame.unwrap().1)
            .filter(|frame| {
                frame.source_id.0 == pe_service::frame_admission::FRAME_ADMISSION_SOURCE_ID
                    || frame.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID
            })
            .count();
        assert_eq!(artifacts, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn frame_below_minimum_counts_and_terminal_survive_rest_and_financial_boot() {
    let mut h = Harness::new_with_configuration(3, dollar_runtime()).await;
    let frame = h
        .record_with_economics(
            1,
            Some((dec!(0.84), dec!(0.84), Some(0))),
            Some(BookEconomics {
                depth: dec!(0.000199),
                fee_free: true,
            }),
        )
        .await;
    let initial_rows = h.paper.leader_positions().unwrap();
    let initial_hash = pe_service::position_seeder::ledger_capture(
        &build_leader_ledger(&h.paper).unwrap(),
        &h.paper,
        wallet(),
    )
    .unwrap()
    .hash;
    h.attempt(&frame, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&frame, |_| {}).await;
    let terminal = h.terminal(&frame);
    assert_eq!(terminal.terminal_disposition.as_deref(), Some("no_fill"));
    let replay = replay_decision_pending(&terminal).unwrap();
    assert_eq!(
        replay.post_boundary.body.terminal.reason,
        "ladder quantity is below the venue minimum"
    );
    assert_eq!(identifier_counts(&h, &frame.id), (1, 0, 0, 0));
    assert_eq!(h.paper.leader_positions().unwrap(), initial_rows);
    assert_eq!(
        pe_service::position_seeder::ledger_capture(
            &build_leader_ledger(&h.paper).unwrap(),
            &h.paper,
            wallet(),
        )
        .unwrap()
        .hash,
        initial_hash
    );
    h.poll(&frame).await;
    assert_eq!(h.terminal(&frame), terminal);
    assert_eq!(identifier_counts(&h, &frame.id), (1, 0, 0, 0));
    let financial = h.paper.financial_snapshot(EPOCH).unwrap();
    let history = h.paper.gate_history().unwrap();
    let rows = h.paper.leader_positions().unwrap();
    let hash = pe_service::position_seeder::ledger_capture(
        &build_leader_ledger(&h.paper).unwrap(),
        &h.paper,
        wallet(),
    )
    .unwrap()
    .hash;
    h.stop().await;
    h.paper = Arc::new(PaperStateDb::open(&h.dir.path().join("paper.db")).unwrap());
    let index = SourceReceiptIndex::replay(&h.dir.path().join("source.log")).unwrap();
    let writer =
        pe_service::paper_recovery::PaperLog::open(h.dir.path().join("paper.log")).unwrap();
    assert_eq!(
        reconcile_active_financial_frames(
            &h.authority,
            &h.paper,
            SourceEvidence::Index(&index),
            &writer
        )
        .await
        .unwrap(),
        0
    );
    drop(writer);
    h.start_frames();
    h.boot_barrier().await;
    assert_eq!(h.terminal(&frame), terminal);
    assert_eq!(identifier_counts(&h, &frame.id), (1, 0, 0, 0));
    assert_eq!(h.paper.financial_snapshot(EPOCH).unwrap(), financial);
    assert_eq!(h.paper.gate_history().unwrap(), history);
    assert_eq!(h.paper.leader_positions().unwrap(), rows);
    assert_eq!(
        pe_service::position_seeder::ledger_capture(
            &build_leader_ledger(&h.paper).unwrap(),
            &h.paper,
            wallet(),
        )
        .unwrap()
        .hash,
        hash
    );
}

#[tokio::test(start_paused = true)]
async fn ordinary_fenced_observation_retirement_survives_anchor_and_restart() {
    for restart in [false, true] {
        let mut h = Harness::new().await;
        h.poller_boot_rebuild = true;
        let old = h.record(1).await;
        h.start_frames(); // missing frontier deliberately routes the observation to history
        let receipt = h.deliver_frame(&old, |_| {}).await;
        assert!(h.paper.decision_pending_for(&old.id).unwrap().is_none());
        h.stop().await;
        rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap().execute(
            "INSERT INTO wallet_fences(wallet_hex,source_trade_id,cause,proof_json,fenced_at_unix) VALUES (?1,?2,'position_underflow',?3,?4)",
            rusqlite::params![wallet().to_string(), old.id.0, json!({"bucket_epoch": EPOCH}).to_string(), EPOCH],
        ).unwrap();
        h.start_frames();
        h.boot_barrier().await;
        let mut empty = old.clone();
        empty.activity = b"[]".to_vec();
        h.poll(&empty).await;
        assert!(h.paper.activity_observation_retired(receipt).unwrap());
        h.install_runtime_anchor(EPOCH + 1, Vec::new()).await;
        if restart {
            h.stop().await;
            h.paper = Arc::new(PaperStateDb::open(&h.dir.path().join("paper.db")).unwrap());
            h.start_frames();
            h.boot_barrier().await;
        }
        assert!(
            pe_service::trade_poller::rebuild_reconciliation_obligations(
                &h.dir.path().join("source.log"),
                &h.paper,
            )
            .unwrap()
            .is_empty()
        );
        h.hooks
            .financial_clock_unix
            .store(EPOCH + 91, Ordering::SeqCst);
        h.empty_frontier(EPOCH + 90).await;
        let next = h.record(93).await;
        h.attempt(&next, at() + time::Duration::seconds(92));
        h.deliver_frame(&next, |_| {}).await;
        assert!(
            replay_decision_pending(&h.terminal(&next))
                .unwrap()
                .continuation
                .is_activity_frame()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn normal_frame_fill_does_not_verify_completed_frame_history() {
    let mut h = Harness::new().await;
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let frontier: pe_service::frame_admission::FrontierCollection =
        serde_json::from_value(h.paper.feed_history_frontiers().unwrap()).unwrap();
    let commitment = frontier.frontiers[0].commitment;
    let frontier_verifications = h.index.read_verification_count(commitment);
    assert_eq!(frontier_verifications, 1);
    let mut completed = Vec::new();
    for ordinal in 1..=3 {
        let frame = h.record(ordinal).await;
        h.attempt(&frame, at());
        h.deliver_frame(&frame, |_| {}).await;
        let replay = replay_decision_pending(&h.terminal(&frame)).unwrap();
        let proof: pe_service::frame_admission::FrameDecisionProof =
            serde_json::from_value(replay.continuation.facts.decision_inputs).unwrap();
        completed.push(proof.inputs.frame_receipt);
    }
    assert_eq!(
        h.index.read_verification_count(commitment),
        frontier_verifications,
        "admission, freshness and observation reconstructed the frontier again"
    );
    h.stop().await;
    let boot_index = SourceReceiptIndex::replay(&h.dir.path().join("source.log")).unwrap();
    let obligations = pe_service::trade_poller::rebuild_reconciliation_obligations_with_index(
        &h.dir.path().join("source.log"),
        &h.paper,
        &boot_index,
    )
    .unwrap();
    assert!(obligations.is_empty());
    pe_service::bucket_commit::validate_open_continuations(&h.paper, &boot_index).unwrap();
    assert_eq!(boot_index.read_verification_count(commitment), 0);
    for receipt in completed {
        assert_eq!(boot_index.frame_verification_count(receipt), 0);
    }
}

#[tokio::test(start_paused = true)]
async fn frontier_publication_rechecks_frame_admitted_after_read_snapshot() {
    for admitted in [true, false] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        h.start_frames();
        h.empty_frontier(if admitted { EPOCH } else { EPOCH - 91 })
            .await;
        let original = h.paper.feed_history_frontiers().unwrap();
        // Freeze the empty read before admission, then publish its queued update afterward.
        let page = h
            .source
            .append(EnvelopeIn {
                source_id: SourceId(pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
                schema_version: 3,
                parser_version: 2,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: b"[]".to_vec(),
            })
            .await
            .unwrap();
        let read = support::producer_shaped_read_v2(wallet(), b"[]", EPOCH + 80, EPOCH, page);
        let commitment = h
            .source
            .append(EnvelopeIn {
                source_id: SourceId(
                    pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned(),
                ),
                schema_version: 2,
                parser_version: 1,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: read.commitment_payload.clone(),
            })
            .await
            .unwrap();
        h.hooks
            .financial_clock_unix
            .store(EPOCH + 70, Ordering::SeqCst);
        h.attempt(&recorded, at() + time::Duration::seconds(70));
        h.deliver_frame(&recorded, |frame| frame["timestamp"] = json!(EPOCH + 70))
            .await;
        if admitted {
            assert!(h.terminal(&recorded).terminal_disposition.is_some());
        } else {
            assert!(h.paper.decision_pending_history().unwrap().is_empty());
        }
        let proof: Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
        let (acknowledged, received) = tokio::sync::oneshot::channel();
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ReconciliationUpdate {
                update: pe_service::orchestrator_control::ReconciliationUpdate::Frontier(
                    pe_service::frame_admission::FeedHistoryFrontier {
                        version: 1,
                        wallet: wallet(),
                        fixed_end: EPOCH + 80,
                        commitment,
                        page_occurrences: vec![read.page],
                        pages: serde_json::from_value(proof["pages"].clone()).unwrap(),
                    },
                    None,
                ),
                acknowledged,
            })
            .await
            .unwrap();
        received.await.unwrap().unwrap();
        if admitted {
            assert_ne!(h.paper.feed_history_frontiers().unwrap(), original);
        } else {
            assert_eq!(h.paper.feed_history_frontiers().unwrap(), original);
        }
        h.hooks
            .financial_clock_unix
            .store(EPOCH + 100, Ordering::SeqCst);
        let later = h.record(2).await;
        let receipt = h
            .deliver_frame(&later, |frame| frame["timestamp"] = json!(EPOCH + 100))
            .await;
        if admitted {
            assert!(h.paper.decision_pending_for(&later.id).unwrap().is_some());
        } else {
            assert!(h.paper.decision_pending_for(&later.id).unwrap().is_none());
            let artifact = Reader::replay(h.dir.path().join("source.log"))
                .unwrap()
                .map(|frame| frame.unwrap().1)
                .filter(|frame| {
                    frame.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID
                })
                .map(|frame| {
                    serde_json::from_slice::<pe_service::frame_admission::FrameFallbackArtifact>(
                        &frame.payload,
                    )
                    .unwrap()
                })
                .find(|artifact| artifact.frame_receipt == receipt)
                .unwrap();
            assert_eq!(
                artifact.reason,
                pe_service::frame_admission::FrameFallbackReason::HistoryBehind
            );
        }
        h.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn frame_wallet_not_ready_before_delivery_survives_readiness_and_restart() {
    for readiness in ["incomplete_history", "fenced", "reanchor_required"] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
        match readiness {
            "incomplete_history" => {
                connection
                    .execute(
                        "UPDATE wallet_history_status_v2 SET complete=0 WHERE wallet_hex=?1",
                        [wallet().to_string()],
                    )
                    .unwrap();
            }
            "fenced" => {
                connection.execute("INSERT INTO wallet_fences(wallet_hex,source_trade_id,cause,proof_json,fenced_at_unix) VALUES (?1,?2,'unknown_effect','{}',?3)", rusqlite::params![wallet().to_string(), recorded.id.0, EPOCH]).unwrap();
            }
            _ => {
                connection
                    .execute(
                        "UPDATE poll_cursors SET reanchor_required=1 WHERE wallet_hex=?1",
                        [wallet().to_string()],
                    )
                    .unwrap();
            }
        }
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        let receipt = h.deliver_frame(&recorded, |_| {}).await;
        h.stop().await;
        assert!(
            h.paper.decision_pending_history().unwrap().is_empty(),
            "{readiness}"
        );
        assert_eq!(h.prepared_count(), 0);
        assert!(h.authority.inner.lock().unwrap().fills.is_empty());
        assert!(
            h.paper
                .market_history_record(
                    &wallet(),
                    &pe_core_types::MarketId(pe_core_types::VenueMarketId(
                        recorded.admission.market.condition_id.0.clone()
                    ))
                )
                .unwrap()
                .is_none()
        );
        connection
            .execute(
                "UPDATE wallet_history_status_v2 SET complete=1 WHERE wallet_hex=?1",
                [wallet().to_string()],
            )
            .unwrap();
        connection
            .execute(
                "DELETE FROM wallet_fences WHERE wallet_hex=?1",
                [wallet().to_string()],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE poll_cursors SET reanchor_required=0 WHERE wallet_hex=?1",
                [wallet().to_string()],
            )
            .unwrap();
        h.start_frames();
        h.boot_barrier_readonly().await;
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt })
            .await
            .unwrap();
        h.boot_barrier_readonly().await;
        let artifacts = Reader::replay(h.dir.path().join("source.log"))
            .unwrap()
            .map(|frame| frame.unwrap().1)
            .filter(|frame| {
                frame.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID
            })
            .map(|frame| {
                serde_json::from_slice::<pe_service::frame_admission::FrameFallbackArtifact>(
                    &frame.payload,
                )
                .unwrap()
            })
            .filter(|artifact| artifact.frame_receipt == receipt)
            .collect::<Vec<_>>();
        assert_eq!(artifacts.len(), 1, "{readiness}");
        assert_eq!(
            artifacts[0].reason,
            pe_service::frame_admission::FrameFallbackReason::WalletNotReady
        );
        assert!(h.paper.decision_pending_history().unwrap().is_empty());
        assert_eq!(h.prepared_count(), 0);
        h.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn recovered_pre_start_frame_admitted_after_fresh_read_qualifies() {
    let mut activity: Value = serde_json::from_slice(include_bytes!(
        "fixtures/golden_stream_v1/activity_page.json"
    ))
    .unwrap();
    activity[0]["timestamp"] = json!(EPOCH);
    activity[0]["conditionId"] = json!(format!("0x{:064x}", 1));
    activity[0]["asset"] = json!("13");
    activity[0]["transactionHash"] = json!(format!("0x{:064x}", 101));
    let mut h =
        Harness::new_with_pre_start_frame(3, runtime(), vec![wallet()], Some(activity[0].clone()))
            .await;
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    h.start_owner_with_frame_recovery(true, true, false);
    h.empty_frontier(EPOCH - 1).await;
    let receipt = h.index.receipt_at(EventSeq(0)).unwrap().unwrap().0;
    h.control
        .as_ref()
        .unwrap()
        .send(OrchestratorControl::ActivityFrameDecision { receipt })
        .await
        .unwrap();
    h.boot_barrier_readonly().await;
    let replay = replay_decision_pending(&h.terminal(&recorded)).unwrap();
    assert!(replay.continuation.is_activity_frame());
    let proof: pe_service::frame_admission::FrameDecisionProof =
        serde_json::from_value(replay.continuation.facts.decision_inputs.clone()).unwrap();
    assert_eq!(proof.inputs.frame_receipt.sequence, EventSeq(0));
    assert_eq!(proof.inputs.version, 2);
    let cutoff = i64::try_from(proof.admission_receipt.sequence.0).unwrap();
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{report:?}");
    assert_eq!(report.replay.fills, 1);
    let recipe = include_str!("../../../docs/29-ACTIVITY-LATENCY-MEASUREMENT.md");
    let sql_start = recipe.find("SELECT f.*, d.source_trade_id").unwrap();
    let sql_end = recipe[sql_start..]
        .find("\"\"\", (int(sys.argv[4])")
        .unwrap()
        + sql_start;
    let sql = &recipe[sql_start..sql_end];
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    let selected = |cutoff| {
        connection
            .prepare(sql)
            .unwrap()
            .query_map(rusqlite::params![cutoff, 20], |row| {
                row.get::<_, String>("source_trade_id")
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    assert_eq!(selected(cutoff), vec![recorded.id.0]);
    assert!(selected(cutoff + 1).is_empty());
    let snapshot = census_snapshot(&h, &CensusLogs::default());
    assert_eq!(inspection_cohort_size(&h.dir.path().join("capture")), 1);
    assert_eq!(snapshot.population["frames"].as_array().unwrap().len(), 1);
}

struct FrameBracketPages {
    activity: Vec<u8>,
    positions: Vec<u8>,
}
impl pe_source_polymarket_public::PageFetcher for FrameBracketPages {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, pe_source_core::SourceError> {
        Ok(if url.contains("/activity?") {
            self.activity.clone()
        } else if url.contains("redeemable=false") {
            self.positions.clone()
        } else {
            b"[]".to_vec()
        })
    }
}

#[tokio::test(start_paused = true)]
async fn reconciled_frame_survives_routine_position_refresh_and_boot_anchor_walk() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&recorded, |_| {}).await;
    h.poll(&recorded).await;
    let terminal = h.terminal(&recorded);
    let history = h.paper.gate_history().unwrap();
    let row: Value = serde_json::from_slice(&recorded.activity).unwrap();
    let positions = serde_json::to_vec(&json!([{
        "proxyWallet": wallet(), "asset": row[0]["asset"],
        "conditionId": row[0]["conditionId"], "outcomeIndex": row[0]["outcomeIndex"],
        "size": row[0]["size"], "cashPnl": "0", "negativeRisk": false
    }]))
    .unwrap();
    let validator = pe_service::position_seeder::CausalPositionValidator::new(
        Arc::new(FrameBracketPages {
            activity: recorded.activity.clone(),
            positions,
        }),
        "fixture://activity",
        "prepared-freshness",
        Arc::new(AssetIdentityResolver::new_runtime(
            Arc::new(Page(recorded.gamma.clone())),
            "fixture://gamma".to_owned(),
            GAMMA_BATCH_SIZE,
            h.source.clone(),
        )),
    )
    .with_clock(Arc::new(|| EPOCH + 10));
    let outcomes = validator
        .validate_via_control(
            &[wallet()],
            h.control.as_ref().unwrap(),
            &h.paper,
            pe_service::position_seeder::ValidationPurpose::RoutineRefresh { cutoff: 0 },
            None,
        )
        .await;
    assert!(outcomes.shared.is_none(), "{:?}", outcomes.shared);
    assert!(outcomes.deferred.is_empty(), "{:?}", outcomes.deferred);
    assert_eq!(outcomes.accepted.len(), 1);
    h.stop().await;
    // This is the production boot path before it installs a source-receipt index.
    let mut engine =
        BucketCommitEngine::load(h.paper.clone(), build_leader_ledger(&h.paper).unwrap()).unwrap();
    let installs = validator
        .validate_direct(&[wallet()], &mut engine, &h.paper)
        .await
        .unwrap();
    assert_eq!(installs.len(), 1);
    assert_eq!(h.terminal(&recorded), terminal);
    assert_eq!(h.paper.gate_history().unwrap(), history);
    assert_eq!(h.paper.list_fills().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn independent_leg_arriving_alone_is_copied_across_restart() {
    for restart in [false, true] {
        let mut h = Harness::new().await;
        let frame = h.record(1).await;
        let other = h.record(2).await;
        h.attempt(&frame, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.deliver_frame(&frame, |_| {}).await;
        let original = h.terminal(&frame);
        if restart {
            h.restart_frames().await;
        }
        let frame_rows: Value = serde_json::from_slice(&frame.activity).unwrap();
        let mut other = h.rest_counterpart(&other, |row| {
            row["transactionHash"] = frame_rows[0]["transactionHash"].clone();
            row["timestamp"] = json!(EPOCH + 1);
        });
        other.epoch = EPOCH + 1;
        h.hooks
            .financial_clock_unix
            .store(other.epoch, Ordering::SeqCst);
        h.attempt(&other, at() + time::Duration::seconds(1));
        h.poll(&other).await;
        assert_eq!(h.terminal(&frame), original);
        assert_eq!(
            h.terminal(&other).terminal_disposition.as_deref(),
            Some("fill")
        );
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 2);
        assert_eq!(h.paper.leader_positions().unwrap().len(), 1);
        assert!(h.feed_edges().is_empty());
        h.assert_frame_barrier(Some(&[])).await;
        h.restart_frames().await;
        h.poll(&other).await;
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 2);
        assert_eq!(h.prepared_count(), 2);
        h.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn frame_first_matching_group_restamp_and_partial_fill_copy_once() {
    for shape in ["matching", "restamp", "partial_fill"] {
        let mut h = Harness::new().await;
        let frame = h.record(1).await;
        h.attempt(&frame, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.deliver_frame(&frame, |_| {}).await;
        let terminal = h.terminal(&frame);
        h.assert_frame_barrier(Some(&[])).await;
        let mut echo = frame.clone();
        let mut rows: Value = serde_json::from_slice(&frame.activity).unwrap();
        if shape == "restamp" {
            let mut original = frame.clone();
            let mut unattributed = rows.clone();
            unattributed[0]["outcomeIndex"] = json!(999);
            original.activity = serde_json::to_vec(&unattributed).unwrap();
            h.poll(&original).await;
            rows.as_array_mut().unwrap().push(unattributed[0].clone());
        } else if shape == "partial_fill" {
            let mut second = rows[0].clone();
            rows[0]["size"] = json!("2");
            rows[0]["usdcSize"] = json!("1");
            second["size"] = json!("3");
            second["usdcSize"] = json!("1.5");
            rows.as_array_mut().unwrap().push(second);
        }
        echo.activity = serde_json::to_vec(&rows).unwrap();
        h.poll(&echo).await;
        assert_eq!(h.terminal(&frame), terminal, "{shape}");
        assert_eq!(h.prepared_count(), 1, "{shape}");
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 1);
        assert_eq!(
            h.paper
                .activity_group_state(&frame.id)
                .unwrap()
                .unwrap()
                .disposition,
            if shape == "restamp" {
                "raw_only"
            } else {
                "applied"
            },
            "{shape}",
        );
        assert_eq!(
            h.paper.leader_positions().unwrap()[0]
                .long_contracts
                .to_decimal(),
            dec!(5)
        );
        for (_, commitment) in read_commitments(&h) {
            assert!(commitment.bindings.unwrap_or_default().is_empty());
            assert!(commitment.read_proof.is_none());
        }
        assert!(h.feed_edges().is_empty());
        assert!(!h.paper.is_wallet_fenced(&wallet()).unwrap(), "{shape}");
        h.restart_frames().await;
        h.poll(&echo).await;
        assert_eq!(h.terminal(&frame), terminal);
        assert_eq!(h.prepared_count(), 1);
        h.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn admitted_frame_does_not_age_the_next_current_frontier() {
    let mut h = Harness::new().await;
    let first = h.record(1).await;
    h.attempt(&first, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&first, |_| {}).await;
    h.assert_frame_barrier(Some(&[])).await;
    h.copy_budget_secs = 120;
    h.hooks
        .financial_clock_unix
        .store(EPOCH + 100, Ordering::SeqCst);
    h.empty_frontier(EPOCH + 99).await;
    let mut later = h.record(2).await;
    later.epoch = EPOCH + 100;
    h.attempt(&later, at() + time::Duration::seconds(100));
    h.deliver_frame(&later, |row| row["timestamp"] = json!(EPOCH + 100))
        .await;
    assert!(
        replay_decision_pending(&h.terminal(&later))
            .unwrap()
            .continuation
            .is_activity_frame()
    );
    h.assert_frame_barrier(Some(&[])).await;
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn restart_requires_a_fresh_read_before_frame_admission() {
    let mut h = Harness::new().await;
    let first = h.record(1).await;
    h.attempt(&first, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&first, |_| {}).await;
    h.restart_frames().await;
    let stored = h.paper.feed_history_frontiers().unwrap();
    h.poller_fetch_failed = true;
    let empty = Recorded {
        activity: b"[]".to_vec(),
        ..first.clone()
    };
    h.poll(&empty).await;
    assert_eq!(h.paper.feed_history_frontiers().unwrap(), stored);
    h.poller_fetch_failed = false;
    let second = h.record(2).await;
    h.hooks
        .financial_clock_unix
        .store(second.epoch, Ordering::SeqCst);
    let receipt = h.deliver_frame(&second, |_| {}).await;
    assert!(h.paper.decision_pending_for(&second.id).unwrap().is_none());
    let fallback: pe_service::frame_admission::FrameFallbackArtifact =
        Reader::replay(h.dir.path().join("source.log"))
            .unwrap()
            .map(|item| item.unwrap().1)
            .filter(|source| {
                source.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID
            })
            .map(|source| {
                serde_json::from_slice::<pe_service::frame_admission::FrameFallbackArtifact>(
                    &source.payload,
                )
                .unwrap()
            })
            .find(|artifact| artifact.frame_receipt == receipt)
            .unwrap();
    assert_eq!(
        fallback.reason,
        pe_service::frame_admission::FrameFallbackReason::HistoryBehind
    );
    assert!(fallback.frontier.is_none());
    h.attempt(&second, at() + time::Duration::seconds(1));
    h.poll(&second).await;
    let third = h.record(3).await;
    h.hooks
        .financial_clock_unix
        .store(third.epoch, Ordering::SeqCst);
    h.attempt(&third, at() + time::Duration::seconds(2));
    h.deliver_frame(&third, |_| {}).await;
    assert!(
        replay_decision_pending(&h.terminal(&third))
            .unwrap()
            .continuation
            .is_activity_frame()
    );
    assert_eq!(h.prepared_count(), 3);
    h.assert_frame_barrier(Some(&[])).await;
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn frontier_publication_preserves_other_wallet_read_hints_across_restarts() {
    let other = WalletAddress::from_hex("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let mut h = Harness::new_with_leaders(
        pe_service::paper_recovery::FINANCIAL_SEMANTIC_VERSION,
        runtime(),
        vec![wallet(), other],
    )
    .await;
    let recorded = h.record(1).await;
    h.start_frames();
    h.empty_frontier_for(wallet(), EPOCH - 1).await;
    h.empty_frontier_for(other, EPOCH - 30).await;
    let stored: pe_service::frame_admission::FrontierCollection =
        serde_json::from_value(h.paper.feed_history_frontiers().unwrap()).unwrap();
    let other_hint = stored
        .frontiers
        .into_iter()
        .find(|frontier| frontier.wallet == other)
        .unwrap();
    // Frame delivery can leave the cursor well ahead of this wallet's last completed read.
    h.paper.set_cursor(&other, EPOCH + 10).unwrap();
    h.restart_frames().await;
    h.empty_frontier_for(wallet(), EPOCH).await;
    let stored: pe_service::frame_admission::FrontierCollection =
        serde_json::from_value(h.paper.feed_history_frontiers().unwrap()).unwrap();
    assert_eq!(
        stored
            .frontiers
            .iter()
            .find(|frontier| frontier.wallet == other),
        Some(&other_hint)
    );

    for restart in [false, true] {
        if restart {
            h.restart_frames().await;
            h.empty_frontier_for(wallet(), EPOCH + 1).await;
        }
        let mut membership = (*h.watchlist.snapshot()).clone();
        membership.entries.retain(|entry| entry.wallet == other);
        membership.active_count = membership.entries.len();
        let mut held = h.held_poll_with_source_and_control(
            &recorded,
            None,
            h.source.clone(),
            h.control.as_ref().unwrap().clone(),
            false,
            LiveWatchlist::new(membership),
        );
        held.clock.store(EPOCH + 11, Ordering::SeqCst);
        let response = held.pages.recv().await.unwrap();
        let url = reqwest::Url::parse(&response.url).unwrap();
        assert!(
            url.query_pairs()
                .any(|(key, value)| key == "user" && value == other.to_string())
        );
        // The REST query uses an inclusive start; the read window starts exclusively at H - 1.
        assert!(
            url.query_pairs()
                .any(|(key, value)| key == "start" && value == other_hint.fixed_end.to_string()),
            "{}",
            response.url
        );
        // Stop before B's first fresh read completes; its hint must also survive another restart.
        held.stop.send(()).unwrap();
        drop(response);
        held.task.await.unwrap().unwrap();
        let stored: pe_service::frame_admission::FrontierCollection =
            serde_json::from_value(h.paper.feed_history_frontiers().unwrap()).unwrap();
        assert_eq!(
            stored
                .frontiers
                .iter()
                .find(|frontier| frontier.wallet == other),
            Some(&other_hint)
        );
    }
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn boot_authenticates_only_surviving_obligation_commitments() {
    let mut h = Harness::new().await;
    let disposed = h.record(1).await;
    h.attempt(&disposed, at());
    h.start(false);
    h.poll_source(&disposed, Some(EPOCH), at()).await;
    let old = read_commitments(&h).last().unwrap().0;
    let pending = h.record(2).await;
    *h.poller_crash.lock().unwrap() =
        Some(pe_service::trade_poller::ReconciliationCrashBoundary::Commitment);
    assert!(
        h.poll_source_result(
            &pending,
            Some(pending.epoch),
            at() + time::Duration::seconds(1),
            0
        )
        .await
        .is_err()
    );
    let unfinished = read_commitments(&h).last().unwrap().0;
    assert_ne!(old, unfinished);
    h.stop().await;
    let index = SourceReceiptIndex::replay(&h.dir.path().join("source.log")).unwrap();
    let obligations = pe_service::trade_poller::rebuild_reconciliation_obligations_with_index(
        &h.dir.path().join("source.log"),
        &h.paper,
        &index,
    )
    .unwrap();
    assert_eq!(obligations.len(), 1);
    assert_eq!(index.read_verification_count(old), 0);
    assert_eq!(index.read_verification_count(unfinished), 1);
    assert_eq!(index.binding_verification_counts(old), [0, 0, 0]);
    assert_eq!(index.binding_verification_counts(unfinished), [0, 1, 0]);
    pe_service::bucket_commit::validate_open_continuations(&h.paper, &index).unwrap();
    assert_eq!(
        index.read_verification_count(old),
        0,
        "stored frontier was authenticated at boot"
    );
    let frames = Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(|item| item.unwrap().1)
        .filter(|source| source.source_id.0 == pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID);
    for frame in frames {
        assert_eq!(
            index.frame_verification_count(AppendReceipt {
                sequence: frame.seq,
                this_hash: frame.this_hash
            }),
            0
        );
    }
}

#[tokio::test(start_paused = true)]
async fn admitted_frames_do_not_force_complete_reconciliation_reads() {
    let mut h = Harness::new().await;
    let frame = h.record(1).await;
    h.attempt(&frame, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let receipt = h.deliver_frame(&frame, |_| {}).await;
    h.paper.set_cursor(&wallet(), EPOCH - 2).unwrap();
    let mut held = h.held_poll(&frame, Some(receipt));
    held.clock.store(EPOCH + 1, Ordering::SeqCst);
    let response = held.pages.recv().await.unwrap();
    assert!(
        response.url.contains(&format!("start={}", EPOCH - 2)),
        "{}",
        response.url
    );
    held.finish(response, frame.activity.clone()).await;
    for (_, commitment) in read_commitments(&h) {
        assert!(commitment.bindings.unwrap_or_default().is_empty());
        assert!(commitment.read_proof.is_none());
    }
    h.assert_frame_barrier(Some(&[])).await;
    h.stop().await;
}

fn read_commitments(
    h: &Harness,
) -> Vec<(
    AppendReceipt,
    pe_service::bucket_commit::ActivityReadCommitment,
)> {
    Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(|frame| frame.unwrap().1)
        .filter(|frame| {
            frame.source_id.0 == pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SOURCE_ID
        })
        .map(|frame| {
            (
                AppendReceipt {
                    sequence: frame.seq,
                    this_hash: frame.this_hash,
                },
                serde_json::from_slice(&frame.payload).unwrap(),
            )
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn rest_first_condition_stamp_mismatch_copies_once_across_restart() {
    for restart in [false, true] {
        let mut h = Harness::new().await;
        let frame = h.record(1).await;
        let other = h.record(2).await;
        let counterpart = h.rest_counterpart(&frame, |row| {
            row["conditionId"] = json!(other.admission.market.condition_id.0);
        });
        h.start_frames();
        h.attempt(&counterpart, at());
        h.poll(&counterpart).await;
        let terminal = h.terminal(&counterpart);
        assert_eq!(terminal.terminal_disposition.as_deref(), Some("fill"));
        if restart {
            h.restart_frames().await;
        }
        h.empty_frontier(EPOCH - 1).await;
        let retired_frame = h.deliver_frame(&frame, |_| {}).await;
        assert!(h.paper.decision_pending_for(&frame.id).unwrap().is_none());
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 1);
        assert_eq!(h.prepared_count(), 1);
        assert!(h.feed_edges().is_empty());
        h.poll(&counterpart).await;
        assert!(h.paper.activity_observation_retired(retired_frame).unwrap());
        h.restart_frames().await;
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision {
                receipt: retired_frame,
            })
            .await
            .unwrap();
        h.assert_frame_barrier(Some(&[])).await;
        *h.pending_poller_trigger.lock().unwrap() =
            Some(pe_service::activity_ingest::ReconciliationTrigger {
                qualifying_buy: true,
                wallet: wallet(),
                source_time: at(),
                source_trade_id: frame.id.clone(),
                provenance: pe_copy_signal_engine::TradeProvenance::ActivityWs,
                received_at: at(),
                receipt: retired_frame,
            });
        h.poll(&counterpart).await;
        h.assert_frame_barrier(Some(&[])).await;
        let repeat = h.append_frame(&frame, |_| {}).await;
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt: repeat })
            .await
            .unwrap();
        h.boot_barrier_readonly().await;
        assert_eq!(h.prepared_count(), 1);
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 1);
        // A newly appended receipt is not covered by the earlier receipt's exact retirement.
        assert_eq!(
            h.hooks.frame_barriers.lock().unwrap().get(&wallet()),
            Some(&vec![repeat])
        );
        assert!(
            pe_service::trade_poller::rebuild_reconciliation_obligations(
                &h.dir.path().join("source.log"),
                &h.paper,
            )
            .unwrap()
            .is_empty()
        );
        // This harness starts one poller per read; deliver the live trigger for this exact receipt.
        *h.pending_poller_trigger.lock().unwrap() =
            Some(pe_service::activity_ingest::ReconciliationTrigger {
                qualifying_buy: true,
                wallet: wallet(),
                source_time: at(),
                source_trade_id: frame.id.clone(),
                provenance: pe_copy_signal_engine::TradeProvenance::ActivityWs,
                received_at: at(),
                receipt: repeat,
            });
        h.poll(&counterpart).await;
        assert_eq!(h.terminal(&counterpart), terminal);
        assert_eq!(h.paper.decision_pending_history().unwrap().len(), 1);
        assert_eq!(h.prepared_count(), 1);
        assert!(h.feed_edges().is_empty());
        h.assert_frame_barrier(Some(&[])).await;
        let report = h.qualify_one_fill().await;
        assert!(report.replay.exact, "{report:?}");
        assert_eq!(report.replay.fills, 1);
    }
}

#[tokio::test(start_paused = true)]
async fn frame_asset_market_mismatch_falls_back_without_consuming_history() {
    let mut h = Harness::new().await;
    let frame = h.record(1).await;
    let other = h.record(2).await;
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let receipt = h
        .deliver_frame(&frame, |row| {
            row["asset"] = json!(other.admission.market.ordered_outcome_token_ids[0].0);
        })
        .await;
    let fallback = Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(|item| item.unwrap().1)
        .filter(|source| {
            source.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID
        })
        .map(|source| {
            serde_json::from_slice::<pe_service::frame_admission::FrameFallbackArtifact>(
                &source.payload,
            )
            .unwrap()
        })
        .find(|artifact| artifact.frame_receipt == receipt)
        .unwrap();
    assert_eq!(
        fallback.reason,
        pe_service::frame_admission::FrameFallbackReason::IdentityUnverified
    );
    assert!(h.paper.decision_pending_history().unwrap().is_empty());
    assert!(
        h.paper
            .market_history_record(
                &wallet(),
                &pe_core_types::MarketId(pe_core_types::VenueMarketId(
                    frame.admission.market.condition_id.0.clone()
                ))
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(h.prepared_count(), 0);
    h.assert_frame_barrier(Some(&[receipt])).await;
    h.stop().await;
}

#[tokio::test(start_paused = true)]
async fn before_start_frame_only_triggers_reconciliation() {
    let mut h = Harness::new_with_semantic(0).await;
    let recorded = h.record(1).await;
    let (started, mut peers) = h.start_frames_with_reader().await;
    started.send(true).unwrap();
    let mut peer = peers.recv().await.unwrap();
    let rows: Value = serde_json::from_slice(&recorded.activity).unwrap();
    peer.send_text(&json!({"topic":"activity", "type":"trades", "payload":rows[0]}).to_string())
        .await
        .unwrap();
    let trigger = h._trigger_rx.recv().await.unwrap();
    assert_eq!(trigger.source_trade_id, recorded.id);
    h.boot_barrier_readonly().await;
    assert!(h.paper.financial_start().unwrap().is_none());
    assert!(h.paper.decision_pending_history().unwrap().is_empty());
    assert!(h.feed_edges().is_empty());
    assert_eq!(h.prepared_count(), 0);
    assert!(
        Reader::replay(h.dir.path().join("source.log"))
            .unwrap()
            .all(|record| !matches!(
                record.unwrap().1.source_id.0.as_str(),
                "pe-service.activity-frame-admission" | "pe-service.activity-frame-fallback"
            ))
    );
    h.stop().await;
}

/// A saturated read cannot refresh a frontier.
#[tokio::test(start_paused = true)]
async fn saturated_one_second_read_keeps_frontier_without_admitted_obligation_across_restart() {
    use pe_source_polymarket_public::{ACTIVITY_MAX_OFFSET, RECONCILIATION_PAGE_LIMIT};
    let mut h = Harness::new().await;
    let frame = h.record(1).await;
    let original: Value = serde_json::from_slice(&frame.activity).unwrap();
    let rows = (0..RECONCILIATION_PAGE_LIMIT)
        .map(|index| {
            let mut row = original[0].clone();
            row["transactionHash"] = json!(format!("0xterminal{index}"));
            row["asset"] = json!(format!("asset-terminal-{index}"));
            row
        })
        .collect::<Vec<_>>();
    let page = serde_json::to_vec(&rows).unwrap();
    h.attempt(&frame, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let receipt = h.deliver_frame(&frame, |_| {}).await;
    let frontier = h.paper.feed_history_frontiers().unwrap();
    let commitments_before = read_commitments(&h).len();
    let terminal = h.terminal(&frame);
    for restart in [false, true] {
        if restart {
            h.restart_frames().await;
        }
        h.paper.set_cursor(&wallet(), EPOCH - 1).unwrap();
        let mut held = h.held_poll(&frame, Some(receipt));
        let mut terminal_offset_seen = false;
        let mut unresolved = None;
        loop {
            tokio::select! {
                request = held.pages.recv() => {
                    let request = request.unwrap();
                    let url = reqwest::Url::parse(&request.url).unwrap();
                    let query = url.query_pairs().collect::<HashMap<_, _>>();
                    let end = query["end"].parse::<i64>().unwrap();
                    if end == EPOCH {
                        if query["start"] == EPOCH.to_string()
                            && query["offset"] == ACTIVITY_MAX_OFFSET.to_string() {
                            terminal_offset_seen = true;
                        }
                        request.respond.send(page.clone()).unwrap();
                    } else {
                        assert_eq!(end, EPOCH - 1);
                        request.respond.send(b"[]".to_vec()).unwrap();
                    }
                }
                progress = held.progress.recv() => {
                    match progress.unwrap() {
                        pe_service::trade_poller::PollerProgress::Completed { unresolved: receipts, .. } => {
                            unresolved = Some(receipts);
                        }
                        pe_service::trade_poller::PollerProgress::RoundCompleted => break,
                        _ => {}
                    }
                }
            }
        }
        let _ = held.stop.send(());
        held.task.await.unwrap().unwrap();
        assert!(terminal_offset_seen);
        assert!(unresolved.unwrap().is_empty());
        assert_eq!(h.paper.feed_history_frontiers().unwrap(), frontier);
        assert_eq!(read_commitments(&h).len(), commitments_before);
        assert!(h.feed_edges().is_empty());
        assert_eq!(h.terminal(&frame), terminal);
        let obligations = pe_service::trade_poller::rebuild_reconciliation_obligations_with_index(
            &h.dir.path().join("source.log"),
            &h.paper,
            &h.index,
        )
        .unwrap();
        assert!(obligations.unresolved_receipts(wallet()).is_empty());
        h.assert_frame_barrier(Some(&[])).await;
    }
    h.stop().await;
}

fn recipe_extract(start: &str, terminator: &str) -> String {
    let recipe = include_str!("../../../docs/29-ACTIVITY-LATENCY-MEASUREMENT.md");
    let begin = recipe.find(start).unwrap();
    let end = begin + recipe[begin..].find(terminator).unwrap();
    recipe[begin..end].to_owned()
}

fn ac16_sql(start: &str) -> String {
    recipe_extract(start, "\nSQL\n").replace("readfile('ac16-population.json')", "?1")
}

/// Export the authenticated fixture receipts in the documented population shape.
fn ac16_population(h: &Harness, buys: Value) -> Value {
    let sources = Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(|record| record.unwrap().1)
        .collect::<Vec<_>>();
    let admitted = h
        .paper
        .activity_frame_decision_index(Some(&wallet()))
        .unwrap();
    let frame_row = |id: &SourceTradeId, source: &pe_event_log::EventEnvelope| {
        let observation =
            pe_source_polymarket_public::parse_activity_trade_observation(&source.payload).unwrap();
        json!({"id": id.0, "frame_seq": source.seq.0, "frame_hash": source.this_hash.to_hex().to_string(),
            "admitted": admitted.iter().any(|frame| frame.observed_source_receipt.is_some_and(|receipt|
                receipt.sequence == source.seq && receipt.this_hash == source.this_hash)),
            "qualifying": observation.group_id.components().side == Some(pe_core_types::Side::Buy)
                && observation.share_amount != pe_core_types::ShareAmount::ZERO && !observation.is_combo})
    };
    let mut frames = Vec::new();
    let mut fallbacks = Vec::new();
    for source in &sources {
        if source.source_id.0 == pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID {
            let observation =
                pe_source_polymarket_public::parse_activity_trade_observation(&source.payload)
                    .unwrap();
            frames.push(frame_row(observation.group_id.key(), source));
        } else if source.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID {
            let artifact: pe_service::frame_admission::FrameFallbackArtifact =
                serde_json::from_slice(&source.payload).unwrap();
            fallbacks.push(json!({"frame_seq": artifact.frame_receipt.sequence.0,
                "frame_hash": artifact.frame_receipt.this_hash.to_hex().to_string(),
                "artifact_seq": source.seq.0, "reason": artifact.reason}));
        }
    }
    for (_, read) in read_commitments(h) {
        for binding in read.bindings.unwrap_or_default() {
            let source = sources
                .iter()
                .find(|source| {
                    source.seq == binding.stream_receipt.sequence
                        && source.this_hash == binding.stream_receipt.this_hash
                })
                .unwrap();
            frames.push(frame_row(&binding.history_group_id, source));
        }
    }
    json!({"frames": frames, "fallbacks": fallbacks, "buys": buys,
        "deploy_source_seq": 0, "window_start": EPOCH - 1, "window_end": EPOCH + 100})
}

#[tokio::test(start_paused = true)]
async fn ac16_unexplained_buy_query_joins_successful_frame_fill_with_null_terminal_key() {
    let mut h = Harness::new().await;
    let frame = h.record(1).await;
    h.attempt(&frame, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    let zero = h
        .deliver_frame(&frame, |row| {
            row["size"] = json!("0");
            row["usdcSize"] = json!("0");
        })
        .await;
    let receipt = h.deliver_frame(&frame, |_| {}).await;
    assert_ne!(zero, receipt);
    assert_eq!(
        h.terminal(&frame).terminal_disposition.as_deref(),
        Some("fill")
    );
    let connection = rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap();
    let key: Option<String> = connection.query_row(
        "SELECT json_extract(post_commit_inputs_json,'$.terminal.fill.idempotency_key') FROM decision_pending WHERE source_trade_id=?1",
        [&frame.id.0], |row| row.get(0)).unwrap();
    assert!(key.is_none());
    let buy = json!({"id": frame.id.0, "wallet": wallet().to_string(),
        "market": frame.admission.market.condition_id.0, "epoch": frame.epoch});
    let population = ac16_population(&h, json!([buy]));
    let count = connection
        .prepare(&ac16_sql("WITH buys AS"))
        .unwrap()
        .query_map([population.to_string()], |_| Ok(()))
        .unwrap()
        .count();
    assert_eq!(count, 0, "successful frame fill was classified unexplained");
    // Keep the delimiter boundary: a prefix of a different source ID is not a fill.
    let mut missing = population.clone();
    missing["buys"][0]["id"] = json!(&frame.id.0[..frame.id.0.len() - 1]);
    let count = connection
        .prepare(&ac16_sql("WITH buys AS"))
        .unwrap()
        .query_map([missing.to_string()], |_| Ok(()))
        .unwrap()
        .count();
    assert_eq!(count, 1);
    h.stop().await;
}

#[derive(Clone, Default)]
struct CensusLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CensusLogs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CensusLogs {
    type Writer = Self;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn census_python(code: &str, args: &[String], bin: &std::path::Path) -> std::process::Output {
    census_python_with(code, args, bin, Some(env!("CARGO_BIN_EXE_pe-service")))
}

fn census_python_with(
    code: &str,
    args: &[String],
    bin: &std::path::Path,
    pe_service: Option<&str>,
) -> std::process::Output {
    use std::io::Write;
    let path = std::env::join_paths(std::iter::once(bin.to_path_buf()).chain(
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()),
    ))
    .unwrap();
    let mut command = std::process::Command::new("python3");
    command.arg("-").args(args).env("PATH", path);
    match pe_service {
        Some(binary) => command.env("PE_SERVICE_BIN", binary),
        None => command.env_remove("PE_SERVICE_BIN"),
    };
    let mut child = command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    // Operational clock output is fixed too; none of the extracted logic is replaced.
    writeln!(
        child.stdin.as_mut().unwrap(),
        "import time\ntime.time = lambda: {}\ntime.time_ns = lambda: {}\n{code}",
        EPOCH + 120,
        (EPOCH + 120) * 1_000_000_000
    )
    .unwrap();
    drop(child.stdin.take());
    child.wait_with_output().unwrap()
}

struct CensusSnapshot {
    population: Value,
    ignored: Value,
    connection: rusqlite::Connection,
}
impl CensusSnapshot {
    fn rows(&self) -> Vec<Value> {
        let sql = ac16_sql("WITH census_inputs AS").replace("readfile('ignored.json')", "?2");
        let mut statement = self.connection.prepare(&sql).unwrap();
        let columns = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        statement
            .query_map(
                rusqlite::named_params! {
                    "?1": self.population.to_string(), "?2": self.ignored.to_string(),
                    ":listening_ns": EPOCH * 1_000_000_000,
                    ":window_end_ns": (EPOCH + 60) * 1_000_000_000,
                    ":capture_cutoff_ns": (EPOCH + 120) * 1_000_000_000,
                },
                |row| {
                    let mut object = serde_json::Map::new();
                    for (index, column) in columns.iter().enumerate() {
                        let value = match row.get_ref(index)? {
                            rusqlite::types::ValueRef::Null => Value::Null,
                            rusqlite::types::ValueRef::Integer(value) => json!(value),
                            rusqlite::types::ValueRef::Text(value) => {
                                json!(std::str::from_utf8(value).unwrap())
                            }
                            other => panic!("unexpected census value {other:?}"),
                        };
                        object.insert(column.clone(), value);
                    }
                    Ok(Value::Object(object))
                },
            )
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }
    fn assert_row(&self, receipt: AppendReceipt, class: &str, verdict: &str) -> Value {
        let rows = self.rows();
        let row = rows
            .iter()
            .find(|row| row["seq"] == receipt.sequence.0)
            .unwrap_or_else(|| panic!("missing census receipt {}: {rows:?}", receipt.sequence.0));
        assert_eq!(row["hash"], receipt.this_hash.to_hex().to_string());
        assert_eq!(row["disposition_class"], class, "{row}");
        assert_eq!(row["verdict"], verdict, "{row}");
        row.clone()
    }
}

fn census_snapshot(h: &Harness, logs: &CensusLogs) -> CensusSnapshot {
    let path = h.dir.path();
    let bin = path.join("bin");
    std::fs::create_dir(&bin).unwrap();
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_pe-scenario-b3sum"), bin.join("b3sum")).unwrap();
    let capture = path.join("capture");
    let args = [
        path.join("paper.db"),
        path.join("source.log"),
        path.join("paper.log"),
        capture.clone(),
    ]
    .map(|path| path.to_str().unwrap().to_owned());
    let output = census_python(
        &recipe_extract(
            "import ctypes, ctypes.util, json, os, re, sqlite3",
            "\nPY\n",
        ),
        &[
            args[0].clone(),
            args[1].clone(),
            args[2].clone(),
            args[3].clone(),
            "5".to_owned(),
            "0".to_owned(),
        ],
        &bin,
    );
    assert!(
        output.status.success(),
        "capture: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("snapshot started"));
    let output = inspection_run(h, &capture, Some(env!("CARGO_BIN_EXE_pe-service")));
    assert!(
        output.status.success(),
        "inspection: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let population: Value =
        serde_json::from_slice(&std::fs::read(capture.join("ac16-population.json")).unwrap())
            .unwrap();
    let raw = Reader::replay(path.join("source.log"))
        .unwrap()
        .map(|frame| frame.unwrap().1)
        .filter(|frame| frame.source_id.0 == pe_service::activity_ingest::ACTIVITY_WS_SOURCE_ID)
        .count();
    assert_eq!(population["raw_receipt_count"], raw);
    assert_eq!(population["receipts"].as_array().unwrap().len(), raw);
    assert!(String::from_utf8_lossy(&output.stdout).contains(&format!(
        "unique captured feed receipts {raw} exported receipts {raw}"
    )));
    let cohort_size = inspection_cohort_size(&capture);
    if cohort_size == 0 {
        assert!(String::from_utf8_lossy(&output.stdout).contains("FROZEN COHORT []"));
    } else {
        assert!(!String::from_utf8_lossy(&output.stdout).contains("FROZEN COHORT []"));
        assert_eq!(
            String::from_utf8_lossy(&output.stdout)
                .matches("Prepared/Final equal")
                .count(),
            cohort_size
        );
    }

    // A journal entry contains the exact flattened subscriber line in MESSAGE. The synthetic
    // journal's fixed microsecond timestamp is after window_end, before the snapshot cutoff.
    let journal = String::from_utf8(std::mem::take(&mut *logs.0.lock().unwrap())).unwrap().lines()
        .map(|line| json!({"MESSAGE": line, "__REALTIME_TIMESTAMP": ((EPOCH + 90) * 1_000_000).to_string()}).to_string())
        .chain([json!({"MESSAGE": "non-JSON systemd line"}).to_string(),
            json!({"MESSAGE": json!({"fields": {"message": "frame admission ignored"}}).to_string()}).to_string()])
        .collect::<Vec<_>>().join("\n");
    let journal_path = capture.join("journal.json");
    let ignored_path = capture.join("ignored.json");
    std::fs::write(&journal_path, journal).unwrap();
    let output = census_python(
        &recipe_extract("import json, re, sys\nfrom decimal", "\nPY\n"),
        &[
            journal_path.to_str().unwrap().to_owned(),
            ignored_path.to_str().unwrap().to_owned(),
        ],
        &bin,
    );
    assert!(
        output.status.success(),
        "journal: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let ignored = serde_json::from_slice(&std::fs::read(&ignored_path).unwrap()).unwrap();
    CensusSnapshot {
        population,
        ignored,
        connection: rusqlite::Connection::open(capture.join("paper_state.db")).unwrap(),
    }
}

#[test]
fn ac_b_closing_batch_query() {
    let sql = recipe_extract("WITH closing_batch_inputs AS", "\nSQL\n")
        .replace("readfile('ac-b-closing-batch.json')", "?1");
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let check = |input: Value, expected_batch: Option<i64>, verdict: &str| {
        let result: (Option<i64>, String) = connection
            .query_row(&sql, [input.to_string()], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap();
        assert_eq!(result, (expected_batch, verdict.to_owned()), "{input}");
    };
    // A straddling read and more distant reads must not replace the closest brackets.
    let input = json!({"s_unix_ms": 1000, "reads": [
        {"started_unix_ms": 700, "completed_unix_ms": 800, "batch_id": 85},
        {"started_unix_ms": 900, "completed_unix_ms": 1000, "batch_id": 86},
        {"started_unix_ms": 999, "completed_unix_ms": 1001, "batch_id": 99},
        {"started_unix_ms": 1000, "completed_unix_ms": 1100, "batch_id": 86},
        {"started_unix_ms": 1200, "completed_unix_ms": 1300, "batch_id": 88}]});
    check(input.clone(), Some(86), "pass");
    let mut differing = input.clone();
    differing["reads"][3]["batch_id"] = json!(87);
    check(differing.clone(), None, "incomplete");
    differing["publications"] = json!([{"batch_id": 87, "committed_unix_ms": 1001,
        "evidence": "retained transaction commit acknowledgement and checksum"}]);
    check(differing.clone(), Some(86), "pass");
    let mut skipped = differing.clone();
    skipped["reads"][3]["batch_id"] = json!(88);
    skipped["publications"] = json!([
        {"batch_id": 87, "committed_unix_ms": 999, "evidence": "retained commit of intervening batch"},
        {"batch_id": 88, "committed_unix_ms": 1001, "evidence": "retained later commit"}]);
    check(skipped, None, "incomplete");
    // Equality at S is insufficient for the strictly-after exception, as is created_at alone.
    differing["publications"][0]["committed_unix_ms"] = json!(1000);
    check(differing.clone(), None, "incomplete");
    differing["publications"] = json!([{"batch_id": 87, "created_at": 1001}]);
    check(differing, None, "incomplete");
    let mut unbracketed = input.clone();
    unbracketed["reads"] = json!([input["reads"][1]]);
    check(unbracketed, None, "incomplete");
    let mut malformed = input.clone();
    malformed["reads"][0]["completed_unix_ms"] = json!(600);
    check(malformed, None, "incomplete");
    let mut tied = input.clone();
    tied["reads"].as_array_mut().unwrap().push(json!({
        "started_unix_ms": 901, "completed_unix_ms": 1000, "batch_id": 87}));
    check(tied, None, "incomplete");
    let mut empty_maximum = input;
    empty_maximum["reads"][1]["batch_id"] = Value::Null;
    check(empty_maximum, None, "incomplete");
}

fn ac_b_reference_rows(population: &Value) -> Vec<Value> {
    let connection = rusqlite::Connection::open_in_memory().unwrap();
    let mut statement = connection
        .prepare(&ac16_sql("WITH membership_inputs AS"))
        .unwrap();
    statement
        .query_map([population.to_string()], |row| {
            Ok(
                json!({"seq": row.get::<_, u64>(0)?, "hash": row.get::<_, String>(1)?,
            "received_at_ns": row.get::<_, i64>(2)?, "reason": row.get::<_, String>(3)?,
            "kind": row.get::<_, String>(4)?, "verdict": row.get::<_, String>(5)?,
            "failing_references": serde_json::from_str::<Value>(&row.get::<_, String>(6)?).unwrap(),
            "evidence_errors": serde_json::from_str::<Value>(&row.get::<_, String>(7)?).unwrap()}),
            )
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn ac_b_capture(
    h: &Harness,
    name: &str,
    offset: u64,
    sequence: u64,
    membership_from: Option<u64>,
) -> std::process::Output {
    let mut args = [
        h.dir.path().join("paper.db"),
        h.dir.path().join("source.log"),
        h.dir.path().join("paper.log"),
        h.dir.path().join(name),
    ]
    .map(|path| path.to_str().unwrap().to_owned())
    .to_vec();
    args.extend([offset.to_string(), sequence.to_string()]);
    args.extend(membership_from.map(|value| value.to_string()));
    census_python(
        &recipe_extract(
            "import ctypes, ctypes.util, json, os, re, sqlite3",
            "\nPY\n",
        ),
        &args,
        &h.dir.path().join("bin"),
    )
}

fn ac_b_inspect(h: &Harness, capture: &std::path::Path) -> Value {
    let output = inspection_run(h, capture, Some(env!("CARGO_BIN_EXE_pe-service")));
    assert!(
        output.status.success(),
        "inspection: {}\n{}",
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    serde_json::from_slice(&std::fs::read(capture.join("ac16-population.json")).unwrap()).unwrap()
}

fn inspection_cohort_size(capture: &std::path::Path) -> usize {
    let connection = rusqlite::Connection::open_with_flags(
        capture.join("paper_state.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let sql = recipe_extract("SELECT f.*, d.source_trade_id", "\"\"\", (int(sys.argv[4])");
    connection
        .query_row(
            &format!("SELECT count(*) FROM ({sql})"),
            rusqlite::params![0, i64::MAX],
            |row| row.get(0),
        )
        .unwrap()
}

fn inspection_run(
    h: &Harness,
    capture: &std::path::Path,
    pe_service: Option<&str>,
) -> std::process::Output {
    census_python_with(
        &recipe_extract("import calendar, ctypes", "\nPY\n"),
        &[
            capture.join("paper_state.db").to_str().unwrap().to_owned(),
            capture
                .join("source_filtered.log")
                .to_str()
                .unwrap()
                .to_owned(),
            capture.join("paper.log").to_str().unwrap().to_owned(),
            "0".to_owned(),
            inspection_cohort_size(capture).to_string(),
            at().format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            (at() + time::Duration::seconds(60))
                .format(&time::format_description::well_known::Rfc3339)
                .unwrap(),
            capture.to_str().unwrap().to_owned(),
        ],
        &h.dir.path().join("bin"),
        pe_service,
    )
}

#[tokio::test(start_paused = true)]
async fn measurement_recipe_authenticates_version_two_admission_and_new_fallbacks() {
    let mut h = Harness::new().await;
    let admitted = h.record(1).await;
    let mismatched = h.record(2).await;
    let expired = h.record(3).await;
    h.prices
        .markets
        .lock()
        .unwrap()
        .get_mut(&admitted.admission.market.condition_id.0)
        .unwrap()["numeric_hash_fixture"] =
        serde_json::from_str("[1e-7,1e-5,0.12345678901234567]").unwrap();
    h.attempt(&admitted, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&admitted, |_| {}).await;
    let mismatch = h
        .deliver_frame(&mismatched, |row| {
            row["asset"] = json!(admitted.admission.market.ordered_outcome_token_ids[0].0);
        })
        .await;
    let expiry = h
        .deliver_frame(&expired, |row| {
            row["timestamp"] = json!(EPOCH - 3);
        })
        .await;
    let replay = replay_decision_pending(&h.terminal(&admitted)).unwrap();
    assert_eq!(
        replay.continuation.facts.decision_inputs["inputs"]["version"],
        2
    );
    h.stop().await;
    let snapshot = census_snapshot(&h, &CensusLogs::default());
    assert_eq!(inspection_cohort_size(&h.dir.path().join("capture")), 1);
    let fallbacks = snapshot.population["window_fallbacks"].as_array().unwrap();
    for (receipt, reason) in [(mismatch, "identity_unverified"), (expiry, "copy_expired")] {
        assert!(fallbacks.iter().any(|row| {
            row["frame_seq"] == receipt.sequence.0
                && row["frame_hash"] == receipt.this_hash.to_hex().to_string()
                && row["reason"] == reason
        }));
    }
    let row = h.paper.decision_pending_for(&admitted.id).unwrap().unwrap();
    let wire: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
    for (field, value, refusal) in [
        (
            "asset",
            json!("foreign-token"),
            "frame identity asset differs",
        ),
        (
            "source_log_sequence",
            json!(u64::MAX),
            "frame identity receipt sequence differs",
        ),
        (
            "canonical_page_hash",
            json!("00".repeat(32)),
            "binding metadata has the wrong source contract or page hash",
        ),
    ] {
        let mut changed = wire.clone();
        changed["decision_inputs"]["inputs"]["identity"]["provenance"][field] = value;
        snapshot
            .connection
            .execute(
                "UPDATE decision_pending SET frozen_inputs_json=?1 WHERE source_trade_id=?2",
                rusqlite::params![changed.to_string(), admitted.id.0],
            )
            .unwrap();
        let output = inspection_run(
            &h,
            &h.dir.path().join("capture"),
            Some(env!("CARGO_BIN_EXE_pe-service")),
        );
        assert!(!output.status.success(), "{field}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(refusal),
            "{field}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test(start_paused = true)]
async fn measurement_recipe_authenticates_historical_version_one_admission() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    h.attempt(&recorded, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&recorded, |_| {}).await;
    let row = h.paper.decision_pending_for(&recorded.id).unwrap().unwrap();
    let mut wire: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
    wire["decision_inputs"]["inputs"]["version"] = json!(1);
    wire["decision_inputs"]["inputs"]
        .as_object_mut()
        .unwrap()
        .remove("identity");
    h.authenticate_frame_wire(&mut wire).await;
    rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap().execute(
        "UPDATE decision_pending SET semantic_revision=?1, frozen_inputs_json=?2 WHERE source_trade_id=?3",
        rusqlite::params![wire["semantic_revision"].as_str().unwrap(), wire.to_string(), recorded.id.0],
    ).unwrap();
    h.stop().await;
    census_snapshot(&h, &CensusLogs::default());
    assert_eq!(inspection_cohort_size(&h.dir.path().join("capture")), 1);
}

/// Reframe a deliberately invalid receipt reference, retaining all other captured paper records.
fn ac_b_rewrite_paper(capture: &std::path::Path, sequence: u64, edit: impl Fn(&mut Value)) {
    let path = capture.join("paper.log");
    let records = Reader::replay(&path)
        .unwrap()
        .map(|row| row.unwrap().1)
        .collect::<Vec<_>>();
    let temporary = capture.join("edited-paper.log");
    let mut writer = Writer::open(&temporary).unwrap();
    for mut record in records {
        if record.seq.0 == sequence {
            let mut payload: Value = serde_json::from_slice(&record.payload).unwrap();
            edit(&mut payload);
            record.payload = serde_json::to_vec(&payload).unwrap();
        }
        writer
            .append_synced(EnvelopeIn {
                source_id: record.source_id,
                schema_version: record.schema_version,
                parser_version: record.parser_version,
                observed_at: record.observed_at,
                received_at: record.received_at,
                content_type: record.content_type,
                payload: record.payload,
            })
            .unwrap();
    }
    drop(writer);
    std::fs::rename(temporary, path).unwrap();
}

#[tokio::test(start_paused = true)]
async fn ac_b_membership_reference_checks() {
    use pe_service::config_poller::{WatchlistCapacityApplier, capacity_request_channel};
    use pe_service::runtime_config::AppliedWatchlistCapacity;
    use pe_service::watchlist_admission::AdmissionPreparer;
    use pe_service::watchlist_capacity::SupabaseWatchlistCapacity;

    let mut h = Harness::new().await;
    let newcomer = WalletAddress([0xbb; 20]);
    let missing_history = WalletAddress([0xcc; 20]);
    h.paper
        .record_reconciled_history_status(&WalletHistoryStatusRecord {
            wallet: newcomer,
            complete: true,
            proof_json: "{}".to_owned(),
            updated_at_unix: EPOCH,
        })
        .unwrap();
    support::install_verified_empty_anchor(&h.paper, newcomer, 0);
    h.start_frames();
    let writer_lock = Arc::new(tokio::sync::Mutex::new(()));
    let preparer = AdmissionPreparer::new(h.control.as_ref().unwrap().clone(), h.paper.clone())
        .with_source_log(h.source.clone());
    let prepared = preparer.prepare(&[newcomer]).await.unwrap();
    assert_eq!(prepared.admitted, vec![newcomer]);
    let original = h.watchlist.snapshot().entries[0].clone();
    preparer
        .scenario_publish_ranking(
            &h.watchlist,
            &writer_lock,
            vec![
                original.clone(),
                WatchlistEntry {
                    wallet: newcomer,
                    ..original
                },
            ],
            &HashMap::from([(newcomer, EPOCH - 1)]),
            2,
        )
        .await
        .unwrap();

    // The real capacity worker uses a loopback fixture for its read-only ranking input.
    // Its preparer records both typed capacity artifacts and full wallet-scoped deferrals.
    let ranking_row = |rank, wallet: WalletAddress| {
        json!({"batch_id": 546, "rank": rank,
        "wallet_hex": wallet.to_string(), "hit_rate_text": "0.7", "ls_tstat_text": "2.0",
        "n_trades": 90, "last_trade_unix": EPOCH - 1, "survives": true})
    };
    let rows = Arc::new(Mutex::new(json!([
        ranking_row(1, wallet()),
        ranking_row(2, newcomer)
    ])));
    let server_rows = rows.clone();
    let router = axum::Router::new().route(
        "/rest/v1/latest_ranking",
        axum::routing::get(move || {
            let rows = server_rows.clone();
            async move { axum::Json(rows.lock().unwrap().clone()) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let applied = AppliedWatchlistCapacity::new(2);
    let (requests, desired) = capacity_request_channel(2, writer_lock.clone());
    let capacity = SupabaseWatchlistCapacity::new(
        h.watchlist.clone(),
        h.paper.clone(),
        writer_lock,
        applied,
        desired,
        preparer,
        reqwest::Client::new(),
        format!("http://{address}"),
        "fixture".to_owned(),
        "fixture".to_owned(),
    );
    assert_eq!(capacity.apply(requests.request(1).await).await.unwrap(), 1);
    assert_eq!(
        h.watchlist.structural_membership(),
        std::collections::HashSet::from([wallet()])
    );
    *rows.lock().unwrap() = json!([ranking_row(1, wallet()), ranking_row(2, missing_history)]);
    assert_eq!(capacity.apply(requests.request(2).await).await.unwrap(), 1);
    server.abort();
    let config_payload = Reader::replay(h.dir.path().join("source.log"))
        .unwrap()
        .map(|row| row.unwrap().1)
        .find(|e| e.source_id.0 == "pe-service.watchlist-capacity-config")
        .unwrap()
        .payload;
    let mut bad_payload: Value = serde_json::from_slice(&config_payload).unwrap();
    bad_payload["published_entries"][0]["leader_score_bps"] = json!(true);
    let bad_type = h
        .append(
            "pe-service.watchlist-capacity-config",
            &serde_json::to_vec(&bad_payload).unwrap(),
        )
        .await;
    // Rust requires Vec<WatchlistEntry>; a present key holding null must not pass.
    let mut null_entries: Value = serde_json::from_slice(&config_payload).unwrap();
    null_entries["published_entries"] = Value::Null;
    let null_entries = h
        .append(
            "pe-service.watchlist-capacity-config",
            &serde_json::to_vec(&null_entries).unwrap(),
        )
        .await;
    let mut extended: Value = serde_json::from_slice(&config_payload).unwrap();
    extended["published_entries"][0]["future_field"] = json!("Rust accepts unknown entry fields");
    let unknown_field = h
        .append(
            "pe-service.watchlist-capacity-config",
            &serde_json::to_vec(&extended).unwrap(),
        )
        .await;
    let wrong_source = h
        .append("pe-service.watchlist-ranking", &config_payload)
        .await;
    let wrong_parser = h
        .source
        .append(EnvelopeIn {
            source_id: SourceId("pe-service.watchlist-capacity-config".to_owned()),
            schema_version: 1,
            parser_version: 2,
            observed_at: SourceTimestamp(at()),
            received_at: ReceivedAt(at()),
            content_type: ContentType::Json,
            payload: config_payload,
        })
        .await
        .unwrap();
    h.stop().await;

    let snapshot = census_snapshot(&h, &CensusLogs::default());
    let capture = h.dir.path().join("capture");
    let baseline = snapshot.population;
    let records = baseline["membership_records"].as_array().unwrap();
    let ranking = records.iter().find(|r| r["kind"] == "full_rerank").unwrap();
    let exclusion = records
        .iter()
        .find(|r| r["kind"] == "capacity_change" && r["removed"] == json!([newcomer]))
        .unwrap();
    let ranking_sequence = ranking["seq"].as_u64().unwrap();
    let exclusion_sequence = exclusion["seq"].as_u64().unwrap();
    for record in [ranking, exclusion] {
        assert_eq!(record["received_at_ns"], EPOCH * 1_000_000_000);
        assert!(
            record["references"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["status"] == "verified"),
            "{record}"
        );
    }
    let judged = ac_b_reference_rows(&baseline);
    assert_eq!(judged.len(), records.len());
    assert!(judged.iter().all(|r| r["verdict"] == "pass"), "{judged:?}");
    let deferrals = baseline["deferrals"].as_array().unwrap();
    assert_eq!(deferrals.len(), 1);
    let deferral = &deferrals[0]["deferrals"][0];
    assert_eq!(deferral["wallet"], missing_history.to_string());
    assert_eq!(deferral["class"], "wallet_persistent");
    assert_eq!(deferral["kind"], "history.missing");
    assert!(
        deferral["message"]
            .as_str()
            .unwrap()
            .contains(&missing_history.to_string())
    );
    let sources = Reader::replay_with_offsets(h.dir.path().join("source.log"))
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let (_, _, artifact) = sources
        .iter()
        .find(|(_, seq, _)| seq.0 == deferrals[0]["seq"])
        .unwrap();
    assert_eq!(
        deferrals[0]["hash"],
        artifact.this_hash.to_hex().to_string()
    );
    assert_eq!(
        deferrals[0]["received_at_ns"],
        i64::try_from(artifact.received_at.0.unix_timestamp_nanos()).unwrap()
    );

    // Remove one authentic captured frame without altering any other receipt's identity.
    let config_ref = &exclusion["references"][0];
    assert_eq!(
        config_ref["expected_source_id"],
        "pe-service.watchlist-capacity-config"
    );
    let source_path = capture.join("source_filtered.log");
    let source_bytes = std::fs::read(&source_path).unwrap();
    assert_eq!(
        source_bytes,
        std::fs::read(h.dir.path().join("source.log")).unwrap()
    );
    let (offset, _, _) = sources
        .iter()
        .find(|(_, seq, _)| seq.0 == config_ref["seq"])
        .unwrap();
    let offset = usize::try_from(*offset).unwrap();
    let size = usize::try_from(u32::from_le_bytes(
        source_bytes[offset..offset + 4].try_into().unwrap(),
    ))
    .unwrap();
    let mut missing = source_bytes.clone();
    missing.drain(offset..offset + 4 + size + 4);
    std::fs::write(&source_path, missing).unwrap();
    let rows = ac_b_reference_rows(&ac_b_inspect(&h, &capture));
    let failed = rows
        .iter()
        .find(|r| r["seq"] == exclusion_sequence)
        .unwrap();
    assert_eq!(failed["verdict"], "incomplete");
    assert_eq!(failed["failing_references"][0]["status"], "missing");
    assert_eq!(failed["failing_references"][0]["seq"], config_ref["seq"]);
    assert_eq!(
        rows.iter().find(|r| r["seq"] == ranking_sequence).unwrap()["verdict"],
        "pass"
    );
    std::fs::write(&source_path, source_bytes).unwrap();

    ac_b_rewrite_paper(&capture, exclusion_sequence, |r| {
        r["evidence"]["config_receipt"]["this_hash"] = json!("00".repeat(32));
    });
    let rows = ac_b_reference_rows(&ac_b_inspect(&h, &capture));
    let failed = rows
        .iter()
        .find(|r| r["seq"] == exclusion_sequence)
        .unwrap();
    assert_eq!(failed["verdict"], "incomplete");
    assert_eq!(failed["failing_references"][0]["status"], "mismatched");
    assert_eq!(
        failed["failing_references"][0]["error"],
        "receipt hash differs"
    );

    ac_b_rewrite_paper(&capture, exclusion_sequence, |r| {
        r["evidence"]["config_receipt"] = serde_json::to_value(unknown_field).unwrap();
    });
    let rows = ac_b_reference_rows(&ac_b_inspect(&h, &capture));
    assert_eq!(
        rows.iter()
            .find(|r| r["seq"] == exclusion_sequence)
            .unwrap()["verdict"],
        "pass"
    );
    // The inspection refuses to run without the deployed decoder, including a directory value.
    for pe_service in [None, Some(capture.to_str().unwrap())] {
        let refused = inspection_run(&h, &capture, pe_service);
        assert!(!refused.status.success());
        assert!(String::from_utf8_lossy(&refused.stderr).contains("PE_SERVICE_BIN must name"));
    }
    // The replay and both exports read canonical wallets: an uppercase removal decodes as itself.
    ac_b_rewrite_paper(&capture, exclusion_sequence, |r| {
        let upper = r["removed"][0].as_str().unwrap()[2..].to_uppercase();
        r["removed"][0] = json!(format!("0x{upper}"));
    });
    let population = ac_b_inspect(&h, &capture);
    let change = population["membership_changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["seq"] == exclusion_sequence)
        .unwrap();
    assert_eq!(change["removed"], json!([newcomer]));
    assert_eq!(
        ac_b_reference_rows(&population)
            .iter()
            .find(|r| r["seq"] == exclusion_sequence)
            .unwrap()["verdict"],
        "pass"
    );
    // AppendReceipt ignores unknown fields and parses hex in either case, so neither may fail.
    ac_b_rewrite_paper(&capture, exclusion_sequence, |r| {
        let receipt = &mut r["evidence"]["config_receipt"];
        receipt["audit_note"] = json!("x");
        receipt["this_hash"] = json!(receipt["this_hash"].as_str().unwrap().to_uppercase());
    });
    let rows = ac_b_reference_rows(&ac_b_inspect(&h, &capture));
    assert_eq!(
        rows.iter()
            .find(|r| r["seq"] == exclusion_sequence)
            .unwrap()["verdict"],
        "pass"
    );

    for (receipt, error) in [
        (bad_type, "artifact decode: "),
        (null_entries, "artifact decode: "),
        (wrong_source, "artifact envelope"),
        (wrong_parser, "artifact envelope"),
    ] {
        ac_b_rewrite_paper(&capture, exclusion_sequence, |r| {
            r["evidence"]["config_receipt"] = serde_json::to_value(receipt).unwrap();
        });
        let rows = ac_b_reference_rows(&ac_b_inspect(&h, &capture));
        let failed = rows
            .iter()
            .find(|r| r["seq"] == exclusion_sequence)
            .unwrap();
        assert_eq!(failed["verdict"], "incomplete");
        assert_eq!(failed["failing_references"][0]["status"], "mismatched");
        assert!(
            failed["failing_references"][0]["error"]
                .as_str()
                .unwrap()
                .starts_with(error),
            "{failed}"
        );
    }

    // A contextual mismatch must still export all referenced receipts, even after the first.
    ac_b_rewrite_paper(&capture, ranking_sequence, |r| {
        r["ranking_batch_id"] = json!(999)
    });
    let population = ac_b_inspect(&h, &capture);
    let changed = population["membership_records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["seq"] == ranking_sequence)
        .unwrap();
    assert_eq!(
        changed["references"].as_array().unwrap().len(),
        ranking["references"].as_array().unwrap().len()
    );
    assert_eq!(
        ac_b_reference_rows(&population)
            .iter()
            .find(|r| r["seq"] == ranking_sequence)
            .unwrap()["verdict"],
        "incomplete"
    );
    ac_b_rewrite_paper(&capture, ranking_sequence, |r| {
        r["ranking_batch_id"] = json!(546)
    });

    // All refs being verified is insufficient when evidence omits an added wallet's proof.
    ac_b_rewrite_paper(&capture, ranking_sequence, |r| {
        r["evidence"]["admission_receipts"] = json!([])
    });
    let rows = ac_b_reference_rows(&ac_b_inspect(&h, &capture));
    assert_eq!(
        rows.iter().find(|r| r["seq"] == ranking_sequence).unwrap()["verdict"],
        "incomplete"
    );

    // The ranking record cites source seq 0. Starting at its admission receipt omits that
    // ranking; an inclusive paper boundary fails and names 0. An older record is ignored.
    let (offset, seq, _) = &sources[1];
    let output = ac_b_capture(&h, "covered", *offset, seq.0, Some(ranking_sequence));
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("('capture again from a receipt at or before', 0)"),
        "{output:?}"
    );
    let output = ac_b_capture(&h, "older", *offset, seq.0, Some(ranking_sequence + 1));
    assert!(output.status.success(), "{output:?}");
    let older = ac_b_reference_rows(&ac_b_inspect(&h, &h.dir.path().join("older")));
    assert_eq!(
        older.iter().find(|r| r["seq"] == ranking_sequence).unwrap()["verdict"],
        "incomplete"
    );
    assert_eq!(
        older
            .iter()
            .find(|r| r["seq"] == exclusion_sequence)
            .unwrap()["verdict"],
        "pass"
    );
    let output = ac_b_capture(&h, "omitted", *offset, seq.0, None);
    assert!(output.status.success(), "{output:?}");

    // A record the verifier cannot decode stops the inspection and names it.
    ac_b_rewrite_paper(&capture, exclusion_sequence, |r| {
        r["evidence"]["kind"] = json!("not_a_membership_kind");
    });
    let stopped = inspection_run(&h, &capture, Some(env!("CARGO_BIN_EXE_pe-service")));
    assert!(!stopped.status.success());
    let stderr = String::from_utf8_lossy(&stopped.stderr);
    assert!(
        stderr.contains(&format!("membership record {exclusion_sequence} "))
            && stderr.contains("does not decode"),
        "{stderr}"
    );
}

#[tokio::test(start_paused = true)]
async fn ac_c_receipt_census_query() {
    let logs = CensusLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .without_time()
        .with_ansi(false)
        .with_writer(logs.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    // Same identity: excluded zero, SELL, combo; one admitted positive receipt, one reader echo.
    {
        let mut h = Harness::new().await;
        h.copy_budget_secs = 120;
        let frame = h.record(1).await;
        h.attempt(&frame, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        let zero = h
            .deliver_frame(&frame, |r| {
                r["size"] = json!("0");
                r["usdcSize"] = json!("0");
            })
            .await;
        let sell = h.deliver_frame(&frame, |r| r["side"] = json!("SELL")).await;
        let combo = h
            .deliver_frame(&frame, |r| r["isCombo"] = json!(true))
            .await;
        let raw = h.deliver_frame(&frame, |_| {}).await;
        h.poll(&frame).await;
        let echo = h
            .deliver_frame(&frame, |r| r["outcomeIndex"] = json!("0"))
            .await;
        h.stop().await;
        let snapshot = census_snapshot(&h, &logs);
        // This capture holds no membership record, yet the inspection still refuses to run
        // without the deployed decoder.
        assert_eq!(snapshot.population["membership_records"], json!([]));
        let refused = inspection_run(&h, &h.dir.path().join("capture"), None);
        assert!(!refused.status.success());
        assert!(String::from_utf8_lossy(&refused.stderr).contains("PE_SERVICE_BIN must name"));
        for excluded in [zero, sell, combo] {
            assert!(
                !snapshot
                    .rows()
                    .iter()
                    .any(|r| r["seq"] == excluded.sequence.0)
            );
        }
        snapshot.assert_row(raw, "a", "pass");
        snapshot.assert_row(echo, "b", "pass");
        assert_eq!(
            snapshot
                .rows()
                .iter()
                .filter(|r| r["disposition_class"] == "a")
                .count(),
            1
        );
        let exported = snapshot.population["receipts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["seq"] == raw.sequence.0)
            .unwrap();
        assert!(exported["history_group_ids"].as_array().unwrap().is_empty());
        assert_eq!(exported["received_at_ns"], EPOCH * 1_000_000_000);
        let string_outcome = snapshot.population["receipts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["seq"] == echo.sequence.0)
            .unwrap();
        assert_eq!(string_outcome["outcome"], 0);
        assert_eq!(snapshot.ignored[0]["reason"], "identity_seen");
        // A contradictory snapshot which names the echo as the admitted receipt must count
        // both its decision and its real router line. Identity equality alone must not pass it.
        snapshot.connection.execute("UPDATE decision_pending SET frozen_inputs_json=json_set(frozen_inputs_json,'$.observed_source_receipt.sequence',?1,'$.observed_source_receipt.this_hash',?2) WHERE source_trade_id=?3",
            rusqlite::params![echo.sequence.0, echo.this_hash.to_hex().to_string(), frame.id.0]).unwrap();
        let double = snapshot.assert_row(echo, "a,b", "fail");
        assert_eq!(double["disposition_count"], 2);
        snapshot
            .connection
            .execute("DELETE FROM decision_pending", [])
            .unwrap();
        snapshot.assert_row(raw, "missing", "fail");
        // Admission removed the earlier-frame barrier; the duplicate names its durable decision.
        snapshot.assert_row(echo, "b", "fail");
        // Fail extraction of a kept event with a missing reason-specific field.
        let mut malformed = snapshot.ignored[0].clone();
        malformed
            .as_object_mut()
            .unwrap()
            .remove("continuation_semantic_revision");
        let path = h.dir.path().join("capture/journal.json");
        std::fs::write(&path, json!({"MESSAGE": malformed.to_string(), "__REALTIME_TIMESTAMP": ((EPOCH+90)*1_000_000).to_string()}).to_string()).unwrap();
        let output = census_python(
            &recipe_extract("import json, re, sys\nfrom decimal", "\nPY\n"),
            &[
                path.to_str().unwrap().to_owned(),
                h.dir
                    .path()
                    .join("capture/ignored.json")
                    .to_str()
                    .unwrap()
                    .to_owned(),
            ],
            &h.dir.path().join("bin"),
        );
        assert!(!output.status.success());
    }

    // Trade-time precedes listening and initial live membership by 30 seconds. Receipt time
    // controls AC-C, including when this wallet only became live between trade and receipt.
    {
        let mut h = Harness::new().await;
        h.copy_budget_secs = 120;
        let frame = h.record(1).await;
        let frame = h.rest_counterpart(&frame, |r| r["timestamp"] = json!(EPOCH - 30));
        h.attempt(&frame, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.hooks
            .financial_clock_unix
            .store(EPOCH + 1, Ordering::SeqCst);
        let receipt = h.deliver_frame(&frame, |_| {}).await;
        h.stop().await;
        let snapshot = census_snapshot(&h, &logs);
        snapshot.assert_row(receipt, "a", "pass");
        assert!(snapshot.population["buys"].as_array().unwrap().is_empty());
        assert_eq!(snapshot.population["receipts"][0]["epoch"], EPOCH - 30);
        assert_eq!(
            snapshot.population["receipts"][0]["received_at_ns"],
            (EPOCH + 1) * 1_000_000_000
        );
        // Half-open receive boundaries, independent of the trade epoch.
        let mut outside = snapshot;
        outside.population["receipts"][0]["received_at_ns"] = json!((EPOCH - 1) * 1_000_000_000);
        assert!(outside.rows().is_empty());
        outside.population["receipts"][0]["received_at_ns"] = json!((EPOCH + 60) * 1_000_000_000);
        assert!(outside.rows().is_empty());
    }

    // Converted inventory has no BUY history. An Add remains an unresolved BUY barrier for
    // a later Entry, including the opposite outcome and trades before listening.
    for opposite in [false, true] {
        let mut h = Harness::new().await;
        h.copy_budget_secs = 120;
        let frame = h.record(1).await;
        let add = h.rest_counterpart(&frame, |r| r["timestamp"] = json!(EPOCH - 30));
        let entry = h.rest_counterpart(&add, |r| {
            r["transactionHash"] = json!("later-entry");
            if opposite {
                r["outcomeIndex"] = json!(1);
                r["outcome"] = json!("No");
                r["asset"] = json!(frame.admission.market.ordered_outcome_token_ids[1].0);
            }
        });
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        let market = pe_core_types::MarketId(pe_core_types::VenueMarketId(
            frame.admission.market.condition_id.0.clone(),
        ));
        h.install_runtime_anchor(
            EPOCH - 31,
            vec![(
                market,
                pe_core_types::OutcomeId(0),
                pe_core_types::ShareAmount::from_whole(3).unwrap(),
            )],
        )
        .await;
        let earlier = h.deliver_frame(&add, |_| {}).await;
        if !opposite {
            h.install_runtime_anchor(EPOCH - 31, Vec::new()).await;
        }
        let fallback = h.deliver_frame(&entry, |_| {}).await;
        // REST convergence is produced by the complete-read router for both identities.
        let mut complete = add.clone();
        let mut rows: Value = serde_json::from_slice(&add.activity).unwrap();
        let entry_rows: Value = serde_json::from_slice(&entry.activity).unwrap();
        rows.as_array_mut().unwrap().push(entry_rows[0].clone());
        complete.activity = serde_json::to_vec(&rows).unwrap();
        h.poll(&complete).await;
        h.stop().await;
        let mut snapshot = census_snapshot(&h, &logs);
        snapshot.assert_row(earlier, "b", "pass");
        let add_line = snapshot
            .ignored
            .as_array()
            .unwrap()
            .iter()
            .find(|line| line["receipt_sequence"] == earlier.sequence.0)
            .unwrap();
        assert_eq!(add_line["reason"], "not_entry");
        assert_eq!(add_line["action"], "Add");
        assert_eq!(add_line["balance"], "3");
        let row = snapshot.assert_row(fallback, "c", "pass");
        assert_eq!(row["blocking_seq"], earlier.sequence.0);
        assert_eq!(row["same_trade_second"], 1);
        let receipts = snapshot.population["receipts"].as_array().unwrap();
        assert_eq!(receipts[0]["received_at_ns"], receipts[1]["received_at_ns"]);
        assert!(earlier.sequence < fallback.sequence);
        assert!(
            snapshot.population["frames"]
                .as_array()
                .unwrap()
                .iter()
                .all(|r| r["frame_seq"] != fallback.sequence.0)
        );
        assert!(
            snapshot.population["window_fallbacks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|r| r["frame_seq"] == fallback.sequence.0)
        );
        // A larger durable sequence cannot block even with a converged Entry and identical
        // receive timestamps. Restore it before testing independently missing history rows.
        let earlier_index = snapshot.population["receipts"]
            .as_array()
            .unwrap()
            .iter()
            .position(|r| r["seq"] == earlier.sequence.0)
            .unwrap();
        snapshot.population["receipts"][earlier_index]["seq"] = json!(fallback.sequence.0 + 100);
        let row = snapshot.assert_row(fallback, "c", "fail");
        assert!(row["blocking_seq"].is_null());
        snapshot.population["receipts"][earlier_index]["seq"] = json!(earlier.sequence.0);
        snapshot.assert_row(fallback, "c", "pass");
        let gate: String = snapshot
            .connection
            .query_row(
                "SELECT result FROM entry_gate_results WHERE source_trade_id=?1",
                [&entry.id.0],
                |row| row.get(0),
            )
            .unwrap();
        snapshot
            .connection
            .execute(
                "UPDATE entry_gate_results SET result='admitted' WHERE source_trade_id=?1",
                [&entry.id.0],
            )
            .unwrap();
        snapshot
            .connection
            .execute(
                "DELETE FROM decision_pending WHERE source_trade_id=?1",
                [&entry.id.0],
            )
            .unwrap();
        snapshot.assert_row(fallback, "c", "fail");
        snapshot
            .connection
            .execute(
                "UPDATE entry_gate_results SET result=?1 WHERE source_trade_id=?2",
                rusqlite::params![gate, entry.id.0],
            )
            .unwrap();
        // Only the earlier Add converged: its rows cannot satisfy the Entry's obligation.
        snapshot
            .connection
            .execute(
                "DELETE FROM entry_gate_results WHERE source_trade_id=?1",
                [&entry.id.0],
            )
            .unwrap();
        snapshot
            .connection
            .execute(
                "DELETE FROM activity_groups WHERE source_trade_id=?1",
                [&entry.id.0],
            )
            .unwrap();
        snapshot.assert_row(fallback, "c", "fail");
    }

    // HistoryBehind fails even when the receipt is outside the old source-time frame keys.
    {
        let mut h = Harness::new().await;
        let frame = h.record(1).await;
        let frame = h.rest_counterpart(&frame, |r| r["timestamp"] = json!(EPOCH - 30));
        h.start_frames();
        let receipt = h.deliver_frame(&frame, |_| {}).await;
        h.stop().await;
        let snapshot = census_snapshot(&h, &logs);
        let row = snapshot.assert_row(receipt, "c", "fail");
        assert!(row["reason"].as_str().unwrap().contains("history_behind"));
        assert!(
            snapshot.population["fallbacks"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            snapshot.population["window_fallbacks"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    // A pre-end receipt routes after the window. The named market history must exist.
    {
        let mut h = Harness::new().await;
        let first = h.record(1).await;
        let next = h.rest_counterpart(&first, |r| {
            r["transactionHash"] = json!("consumed-market-echo")
        });
        h.attempt(&first, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.deliver_frame(&first, |_| {}).await;
        h.hooks
            .financial_clock_unix
            .store(EPOCH + 59, Ordering::SeqCst);
        let receipt = h.append_frame(&next, |_| {}).await;
        h.hooks
            .financial_clock_unix
            .store(EPOCH + 90, Ordering::SeqCst);
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt })
            .await
            .unwrap();
        h.boot_barrier_readonly().await;
        h.stop().await;
        let mut snapshot = census_snapshot(&h, &logs);
        snapshot.assert_row(receipt, "b", "pass");
        let lines = snapshot.ignored.clone();
        snapshot.ignored = json!([]);
        snapshot.assert_row(receipt, "missing", "fail");
        snapshot.ignored = lines;
        snapshot
            .connection
            .execute("DELETE FROM wallet_market_history_v2", [])
            .unwrap();
        snapshot.assert_row(receipt, "b", "fail");
    }

    // Selection was live; the wallet was fenced out while its receipt waited in the queue.
    {
        let mut h = Harness::new().await;
        let frame = h.record(1).await;
        h.start_frames();
        // An empty confirmed snapshot keeps Entry classification even after quality falls to
        // zero on removal; a missing snapshot would correctly classify Unknown first.
        h.install_runtime_anchor(EPOCH - 1, Vec::new()).await;
        let receipt = h.append_frame(&frame, |_| {}).await;
        assert_eq!(h.watchlist.snapshot().entries.len(), 1);
        h.watchlist
            .remove_fenced(&std::collections::HashSet::from([wallet()]));
        // Durable removal evidence is recorded before the queued receipt routes.
        rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap().execute(
            "INSERT INTO wallet_fences(wallet_hex,source_trade_id,cause,proof_json,fenced_at_unix) VALUES (?1,?2,'conversion','{}',?3)",
            rusqlite::params![wallet().to_string(), frame.id.0, EPOCH + 1]).unwrap();
        h.hooks
            .financial_clock_unix
            .store(EPOCH + 90, Ordering::SeqCst);
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt })
            .await
            .unwrap();
        h.boot_barrier_readonly().await;
        h.stop().await;
        // The captured fence clock is strictly between selection and routing.
        let snapshot = census_snapshot(&h, &logs);
        snapshot.assert_row(receipt, "b", "pass");
        snapshot
            .connection
            .execute("DELETE FROM wallet_fences", [])
            .unwrap();
        snapshot.assert_row(receipt, "b", "incomplete");
    }
}

#[tokio::test(start_paused = true)]
async fn qualification_requires_audits_only_for_version_one_admissions() {
    for version in [1, 2] {
        let mut h = Harness::new().await;
        let recorded = h.record(1).await;
        h.attempt(&recorded, at());
        *h.hooks.frame_crash_boundary.lock().unwrap() =
            Some(pe_service::orchestrator::FrameCrashBoundary::FrameCommit);
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        let receipt = h.append_frame(&recorded, |_| {}).await;
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt })
            .await
            .unwrap();
        assert!(h.task.take().unwrap().await.unwrap().is_err());
        assert_eq!(h.prepared_count(), 0);
        if version == 1 {
            let row = h.paper.decision_pending_for(&recorded.id).unwrap().unwrap();
            let mut wire: Value = serde_json::from_str(&row.frozen_inputs_json).unwrap();
            wire["decision_inputs"]["inputs"]["version"] = json!(1);
            wire["decision_inputs"]["inputs"]
                .as_object_mut()
                .unwrap()
                .remove("identity");
            h.authenticate_frame_wire(&mut wire).await;
            rusqlite::Connection::open(h.dir.path().join("paper.db")).unwrap().execute(
                "UPDATE decision_pending SET semantic_revision=?1, frozen_inputs_json=?2 WHERE source_trade_id=?3",
                rusqlite::params![wire["semantic_revision"].as_str().unwrap(), wire.to_string(), recorded.id.0],
            ).unwrap();
        }
        let boot_index = SourceReceiptIndex::replay(&h.dir.path().join("source.log")).unwrap();
        pe_service::bucket_commit::validate_open_continuations(&h.paper, &boot_index).unwrap();
        let continuation = pe_service::bucket_commit::DecisionContinuationV3::from_durable(
            &h.paper.decision_pending_for(&recorded.id).unwrap().unwrap(),
        )
        .unwrap();
        let proof: pe_service::frame_admission::FrameDecisionProof =
            serde_json::from_value(continuation.facts.decision_inputs).unwrap();
        assert_eq!(
            boot_index.binding_verification_counts(proof.inputs.frontier.commitment),
            [0, 0, 1]
        );
        assert_eq!(
            boot_index.frame_verification_count(proof.inputs.frame_receipt),
            1
        );
        h.start_frames();
        h.boot_barrier().await;
        assert_eq!(
            h.terminal(&recorded).terminal_disposition.as_deref(),
            Some("fill")
        );
        // No REST counterpart or audit has been produced in either version.
        assert!(h.paper.leader_positions().unwrap().is_empty());
        assert!(
            read_commitments(&h)
                .iter()
                .all(|(_, read)| read.bindings.as_ref().is_none_or(Vec::is_empty))
        );
        let report = h.qualify_one_fill().await;
        if version == 1 {
            assert!(!report.replay.exact);
            assert!(
                serde_json::to_string(&report)
                    .unwrap()
                    .contains("unresolved frame audits"),
                "{report:?}"
            );
        } else {
            assert!(report.replay.exact, "{report:?}");
            assert_eq!(report.replay.fills, 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn qualification_new_admission_ignores_released_historical_incident_basis() {
    let mut h = Harness::new().await;
    let recorded = h.record(1).await;
    let mut writer = Writer::open(h.dir.path().join("paper.log")).unwrap();
    let mut incident = pe_service::paper_recovery::FeedIncident {
        cause: pe_service::paper_recovery::FeedIncidentCause::Contradiction,
        frame_receipt: support::scenario_receipt(0),
        deciding_commitment_receipt: support::scenario_receipt(1),
        counterpart_identity: Some(recorded.id.clone()),
        engagement_receipt: None,
    };
    let append_incident = |writer: &mut Writer, incident, state| {
        writer
            .append_synced(EnvelopeIn {
                source_id: SourceId("pe-service.paper".to_owned()),
                schema_version: 2,
                parser_version: 1,
                observed_at: SourceTimestamp(at()),
                received_at: ReceivedAt(at()),
                content_type: ContentType::Json,
                payload: serde_json::to_vec(&PaperLogRecord::FeedIncidentChanged {
                    incident,
                    state,
                })
                .unwrap(),
            })
            .unwrap()
    };
    let engaged = append_incident(
        &mut writer,
        incident.clone(),
        pe_service::paper_recovery::HaltState::Engaged,
    );
    incident.engagement_receipt = Some(engaged);
    let released = append_incident(
        &mut writer,
        incident,
        pe_service::paper_recovery::HaltState::Released,
    );
    drop(writer);
    let basis = pe_service::paper_recovery::feed_latch_basis(&h.feed_era()).unwrap();
    assert_eq!(basis.latest_incident, Some(engaged));
    assert_eq!(basis.release, Some(released));
    assert!(!basis.engaged());
    h.attempt(&recorded, at());
    h.start_frames();
    h.empty_frontier(EPOCH - 1).await;
    h.deliver_frame(&recorded, |_| {}).await;
    let replay = replay_decision_pending(&h.terminal(&recorded)).unwrap();
    let proof: pe_service::frame_admission::FrameDecisionProof =
        serde_json::from_value(replay.continuation.facts.decision_inputs).unwrap();
    assert_eq!(proof.inputs.version, 2);
    assert_eq!(proof.inputs.paper_prefix, Some(released));
    assert_eq!(
        proof.inputs.latch,
        pe_service::frame_admission::FeedLatchBasis::default()
    );
    let report = h.qualify_one_fill().await;
    assert!(report.replay.exact, "{report:?}");
    assert_eq!(report.replay.fills, 1);
}

#[tokio::test(start_paused = true)]
async fn restart_catches_missed_buy_before_resuming_feed_after_transient_failure() {
    for still_eligible in [true, false] {
        let mut h = Harness::new().await;
        let original = h.record(1).await;
        h.attempt(&original, at());
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.deliver_frame(&original, |_| {}).await;
        h.restart_frames().await;
        let missed = h.record(2).await;
        let subsequent = h.rest_counterpart(&missed, |row| {
            row["transactionHash"] = json!(format!("0x{:064x}", 999));
            row["timestamp"] = json!(EPOCH + 2);
        });
        let now = EPOCH + if still_eligible { 2 } else { 122 };
        h.hooks.financial_clock_unix.store(now, Ordering::SeqCst);
        let receipt = h.deliver_frame(&subsequent, |_| {}).await;
        assert!(
            h.paper
                .decision_pending_for(&subsequent.id)
                .unwrap()
                .is_none()
        );
        let fallback = Reader::replay(h.dir.path().join("source.log"))
            .unwrap()
            .map(|item| item.unwrap().1)
            .filter(|source| {
                source.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID
            })
            .map(|source| {
                serde_json::from_slice::<pe_service::frame_admission::FrameFallbackArtifact>(
                    &source.payload,
                )
                .unwrap()
            })
            .find(|artifact| artifact.frame_receipt == receipt)
            .unwrap();
        assert!(fallback.frontier.is_none());
        assert_eq!(
            fallback.reason,
            pe_service::frame_admission::FrameFallbackReason::HistoryBehind
        );
        let mut rows: Value = serde_json::from_slice(&missed.activity).unwrap();
        let later: Value = serde_json::from_slice(&subsequent.activity).unwrap();
        rows.as_array_mut().unwrap().push(later[0].clone());
        let history = Recorded {
            activity: serde_json::to_vec(&rows).unwrap(),
            ..missed.clone()
        };
        h.poller_fetch_failed = true;
        h.poll_source(
            &history,
            None,
            OffsetDateTime::from_unix_timestamp(now).unwrap(),
        )
        .await;
        assert!(h.paper.decision_pending_for(&missed.id).unwrap().is_none());
        h.poller_fetch_failed = false;
        h.attempt(&missed, OffsetDateTime::from_unix_timestamp(now).unwrap());
        h.poll_source(
            &history,
            None,
            OffsetDateTime::from_unix_timestamp(now).unwrap(),
        )
        .await;
        assert!(
            h.paper
                .decision_pending_for(&subsequent.id)
                .unwrap()
                .is_none()
        );
        assert_eq!(h.prepared_count(), 1 + usize::from(still_eligible));
        if still_eligible {
            assert_eq!(
                h.terminal(&missed).terminal_disposition.as_deref(),
                Some("fill")
            );
        }
        let mut post_restart = h.record(3).await;
        post_restart.epoch = now + 1;
        h.hooks
            .financial_clock_unix
            .store(now + 1, Ordering::SeqCst);
        h.books
            .values
            .lock()
            .unwrap()
            .get_mut(&post_restart.admission.market.ordered_outcome_token_ids[0].0)
            .unwrap()
            .fetched_at_ms = u64::try_from((now + 1) * 1000).unwrap();
        h.attempt(
            &post_restart,
            OffsetDateTime::from_unix_timestamp(now + 1).unwrap(),
        );
        h.deliver_frame(&post_restart, |row| row["timestamp"] = json!(now + 1))
            .await;
        assert!(
            replay_decision_pending(&h.terminal(&post_restart))
                .unwrap()
                .continuation
                .is_activity_frame()
        );
        assert_eq!(
            h.terminal(&post_restart).terminal_disposition.as_deref(),
            Some("fill"),
            "{:?}",
            h.terminal(&post_restart)
        );
        assert_eq!(h.prepared_count(), 2 + usize::from(still_eligible));
        h.assert_frame_barrier(Some(&[])).await;
        h.stop().await;
    }
}

#[tokio::test(start_paused = true)]
async fn frame_identity_wait_rechecks_freshness_and_eligibility_before_consuming_history() {
    for expires in [true, false] {
        let mut h = Harness::new().await;
        let frame = h.record(1).await;
        h.start_frames();
        h.empty_frontier(EPOCH - 1).await;
        h.identity_gate.blocked.store(true, Ordering::SeqCst);
        let receipt = h.append_frame(&frame, |_| {}).await;
        h.control
            .as_ref()
            .unwrap()
            .send(OrchestratorControl::ActivityFrameDecision { receipt })
            .await
            .unwrap();
        h.identity_gate.started.notified().await;
        assert!(h.paper.decision_pending_history().unwrap().is_empty());
        if expires {
            h.hooks
                .financial_clock_unix
                .store(EPOCH + 3, Ordering::SeqCst);
        } else {
            h.watchlist
                .remove_fenced(&std::collections::HashSet::from([wallet()]));
        }
        h.identity_gate.release.notify_one();
        h.assert_frame_barrier(Some(&[receipt])).await;
        assert!(h.paper.decision_pending_history().unwrap().is_empty());
        let market = pe_core_types::MarketId(pe_core_types::VenueMarketId(
            frame.admission.market.condition_id.0.clone(),
        ));
        assert!(
            h.paper
                .market_history_record(&wallet(), &market)
                .unwrap()
                .is_none()
        );
        if expires {
            let fallback = Reader::replay(h.dir.path().join("source.log"))
                .unwrap()
                .map(|item| item.unwrap().1)
                .filter(|source| {
                    source.source_id.0 == pe_service::frame_admission::FRAME_FALLBACK_SOURCE_ID
                })
                .map(|source| {
                    serde_json::from_slice::<pe_service::frame_admission::FrameFallbackArtifact>(
                        &source.payload,
                    )
                    .unwrap()
                })
                .find(|artifact| artifact.frame_receipt == receipt)
                .unwrap();
            assert_eq!(
                fallback.reason,
                pe_service::frame_admission::FrameFallbackReason::CopyExpired
            );
        }
        h.stop().await;
    }
}
