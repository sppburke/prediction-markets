//! Producer-shaped activity fixtures and deterministic continuation harnesses.
//! Historical wire encoders remain test-only compatibility fixtures.

#![allow(dead_code)]

use pe_source_core::SourceError;
use pe_source_polymarket_public::{PageFetcher, ReconciliationFetcher};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use pe_copy_signal_engine::{IncomingTrade, SignalConfig, TradeProvenance};
use pe_core_types::{
    EventSeq, Price, ReceivedAt, ReconstructionQuality, Side, SourceId, SourceTimestamp,
    WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Writer};
use pe_service::bucket_commit::{
    ACTIVITY_READ_COMMITMENT_PARSER_VERSION, ACTIVITY_READ_COMMITMENT_SOURCE_ID,
    ACTIVITY_READ_COMMITMENT_V1_SCHEMA_VERSION, BucketDecisionContext, PageOccurrence,
    activity_read_commitment_payload_v1,
};
use pe_service::config::ServiceConfig;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::runtime_config::RuntimeConfig;
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, ActivityAggregate, ActivityParseContext,
    ActivityRequestBounds, ActivityTransport, PolymarketEndpoint, ReconciliationPageEvidence,
    canonical_page_hash, parse_activity_response,
};
use pe_venue_core::OrderIntent;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LegacyFillSource {
    ClobBestAsk,
    Fallback,
    #[default]
    LeaderHaircut,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyPaperFill {
    pub intent: OrderIntent,
    pub simulated_fill_price: Price,
    pub simulated_at: SourceTimestamp,
    #[serde(default)]
    pub fill_source: LegacyFillSource,
}

/// One complete producer read: aggregates and evidence derived from identical payload bytes.
pub struct ProducerShapedRead {
    pub aggregates: Vec<ActivityAggregate>,
    pub decision_inputs_json: String,
    pub page: PageOccurrence,
    pub commitment_payload: Vec<u8>,
    pub commitment_version: u16,
}

/// Parse a short offset-zero page with no lower bound. Receive time is independent of the end.
pub fn producer_shaped_read_v1(
    wallet: WalletAddress,
    payload: &[u8],
    fixed_end: i64,
    received_unix: i64,
    page_receipt: AppendReceipt,
) -> ProducerShapedRead {
    let request_url = PolymarketEndpoint::UserPositionActivityPage {
        user: wallet.to_string(),
        end: fixed_end,
        start: None,
        offset: 0,
    }
    .url("https://data-api.polymarket.com");
    let received_at = ReceivedAt(time::OffsetDateTime::from_unix_timestamp(received_unix).unwrap());
    let parsed = parse_activity_response(
        payload,
        wallet,
        &ActivityParseContext {
            source_id: SourceId(pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
            observed_at: SourceTimestamp(received_at.0),
            received_at: received_at.clone(),
            transport: ActivityTransport::Rest,
        },
    )
    .unwrap();
    let row_count = u32::try_from(parsed.rows.len()).unwrap();
    assert!(row_count < pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT);
    let raw_hash = blake3::hash(payload).to_hex().to_string();
    let evidence = ReconciliationPageEvidence {
        request_url: request_url.clone(),
        bounds: Some(ActivityRequestBounds {
            start: None,
            end: fixed_end,
        }),
        partition: None,
        offset: 0,
        row_count,
        canonical_page_hash: canonical_page_hash(payload).unwrap(),
        raw_page_hash: raw_hash.clone(),
        received_at,
        schema_version: ACTIVITY_SCHEMA_VERSION,
        parser_version: ACTIVITY_PARSER_VERSION,
    };
    let page = PageOccurrence {
        request_url,
        raw_hash,
        receipt: page_receipt,
    };
    let commitment_payload = activity_read_commitment_payload_v1(
        wallet,
        fixed_end,
        std::slice::from_ref(&page),
        std::slice::from_ref(&evidence),
    )
    .unwrap();
    ProducerShapedRead {
        aggregates: parsed.aggregates().unwrap(),
        decision_inputs_json: serde_json::json!({"fixed_end": fixed_end, "pages": [evidence]})
            .to_string(),
        page,
        commitment_payload,
        commitment_version: 1,
    }
}

/// Current v2 read with an explicit empty binding list; pair with continuation five.
pub fn producer_shaped_read_v2(
    wallet: WalletAddress,
    payload: &[u8],
    fixed_end: i64,
    received_unix: i64,
    page_receipt: AppendReceipt,
) -> ProducerShapedRead {
    let mut read = producer_shaped_read_v1(wallet, payload, fixed_end, received_unix, page_receipt);
    let proof: serde_json::Value = serde_json::from_str(&read.decision_inputs_json).unwrap();
    let pages =
        serde_json::from_value::<Vec<ReconciliationPageEvidence>>(proof["pages"].clone()).unwrap();
    read.commitment_payload = pe_service::bucket_commit::activity_read_commitment_payload(
        wallet,
        fixed_end,
        std::slice::from_ref(&read.page),
        &pages,
    )
    .unwrap();
    read.commitment_version = 2;
    read
}

/// Synthetic receipt for scenarios that exercise the engine without a source log.
pub fn scenario_receipt(sequence: u64) -> AppendReceipt {
    AppendReceipt {
        sequence: EventSeq(sequence),
        this_hash: blake3::hash(format!("scenario-receipt-{sequence}").as_bytes()),
    }
}

/// Append a successor-generation page and its genuine commitment to a real source log.
pub fn append_committed_read_v1(
    writer: &mut Writer,
    wallet: WalletAddress,
    payload: &[u8],
    fixed_end: i64,
    received_unix: i64,
) -> (ProducerShapedRead, AppendReceipt) {
    let received_at = ReceivedAt(time::OffsetDateTime::from_unix_timestamp(received_unix).unwrap());
    let page_receipt = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
            schema_version: pe_service::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
            observed_at: SourceTimestamp(received_at.0),
            received_at: received_at.clone(),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        })
        .unwrap();
    let read = producer_shaped_read_v1(wallet, payload, fixed_end, received_unix, page_receipt);
    let commitment = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned()),
            schema_version: ACTIVITY_READ_COMMITMENT_V1_SCHEMA_VERSION,
            parser_version: ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
            observed_at: SourceTimestamp(received_at.0),
            received_at: received_at.clone(),
            content_type: ContentType::Json,
            payload: read.commitment_payload.clone(),
        })
        .unwrap();
    (read, commitment)
}

/// Append current page and commitment v2 receipts for a continuation-five fixture.
pub fn append_committed_read_v2(
    writer: &mut Writer,
    wallet: WalletAddress,
    payload: &[u8],
    fixed_end: i64,
    received_unix: i64,
) -> (ProducerShapedRead, AppendReceipt) {
    let received_at = ReceivedAt(time::OffsetDateTime::from_unix_timestamp(received_unix).unwrap());
    let page_receipt = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(pe_service::trade_poller::ACTIVITY_POLL_SOURCE_ID.to_owned()),
            schema_version: pe_service::trade_poller::ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
            parser_version: ACTIVITY_PARSER_VERSION,
            observed_at: SourceTimestamp(received_at.0),
            received_at: received_at.clone(),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        })
        .unwrap();
    let read = producer_shaped_read_v2(wallet, payload, fixed_end, received_unix, page_receipt);
    let commitment = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned()),
            schema_version: pe_service::bucket_commit::ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
            parser_version: ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
            observed_at: SourceTimestamp(received_at.0),
            received_at: received_at.clone(),
            content_type: ContentType::Json,
            payload: read.commitment_payload.clone(),
        })
        .unwrap();
    (read, commitment)
}

pub fn install_empty_anchor(
    paper_state: &pe_paper_state::PaperStateDb,
    wallet: pe_core_types::WalletAddress,
    cutoff_unix: i64,
) {
    if !paper_state.position_anchors(&wallet).unwrap().is_empty()
        || paper_state
            .leader_positions()
            .unwrap()
            .iter()
            .any(|position| position.wallet == wallet)
    {
        return;
    }
    paper_state.set_cursor(&wallet, cutoff_unix).unwrap();
    paper_state
        .install_anchors(&[pe_paper_state::AnchorInstallRecord {
            history_status: None,
            wallet,
            balances: Vec::new(),
            activity_cutoff_unix: cutoff_unix,
            anchored_at_unix: cutoff_unix,
            ledger_hash_after: "scenario-empty".to_owned(),
            positions_proof_hash: "scenario-positions".to_owned(),
            activity_bounds_json: "[]".to_owned(),
            source_log_generation: "scenario".to_owned(),
            proof_json: "{}".to_owned(),
            recorded_at_unix: cutoff_unix,
        }])
        .unwrap();
}

fn activity_body(trade: &IncomingTrade) -> Vec<u8> {
    let side = match trade.side {
        Side::Buy => "BUY",
        Side::Sell => "SELL",
    };
    serde_json::to_vec(&serde_json::json!([{
        "proxyWallet": trade.wallet.to_string(),
        "timestamp": trade.observed_at.unix_timestamp(),
        "conditionId": trade.market_id.to_string(),
        "type": "TRADE",
        "size": trade.contracts.to_decimal().to_string(),
        "usdcSize": trade.contracts.to_decimal().checked_mul(trade.price.0).unwrap().to_string(),
        "transactionHash": trade.transaction_hash.clone().unwrap_or_else(|| trade.source_trade_id.0.clone()),
        "price": trade.price.0.to_string(),
        "asset": format!("{}-{}", trade.market_id, trade.outcome_id.0),
        "side": side,
        "outcomeIndex": trade.outcome_id.0,
        "outcome": if trade.outcome_id.0 == 0 { "Yes" } else { "No" },
        "isCombo": false,
    }]))
    .unwrap()
}

fn activity_context(trade: &IncomingTrade) -> ActivityParseContext {
    ActivityParseContext {
        source_id: SourceId("scenario.activity".to_owned()),
        observed_at: SourceTimestamp(trade.observed_at),
        received_at: ReceivedAt(trade.received_at),
        transport: ActivityTransport::Rest,
    }
}

pub fn bucket_source_trade_id(trade: &IncomingTrade) -> pe_core_types::SourceTradeId {
    parse_activity_response(
        &activity_body(trade),
        trade.wallet,
        &activity_context(trade),
    )
    .unwrap()
    .aggregates()
    .unwrap()[0]
        .group_id
        .key()
        .clone()
}

pub async fn send_trade_bucket(control: &mpsc::Sender<OrchestratorControl>, trade: IncomingTrade) {
    send_trade_bucket_with_config(
        control,
        trade,
        RuntimeConfig::from_service_config(&ServiceConfig::default()),
    )
    .await;
}

pub async fn send_trade_bucket_with_config(
    control: &mpsc::Sender<OrchestratorControl>,
    trade: IncomingTrade,
    applied_configuration: RuntimeConfig,
) {
    let body = activity_body(&trade);
    let read = producer_shaped_read_v1(
        trade.wallet,
        &body,
        trade.observed_at.unix_timestamp(),
        trade.received_at.unix_timestamp(),
        scenario_receipt(2),
    );
    let observation_provenance = read
        .aggregates
        .iter()
        .map(|aggregate| (aggregate.group_id.key().clone(), trade.provenance))
        .collect();
    let observed_source_receipts = if trade.provenance == TradeProvenance::ActivityWs {
        read.aggregates
            .iter()
            .map(|aggregate| (aggregate.group_id.key().clone(), scenario_receipt(1)))
            .collect()
    } else {
        HashMap::new()
    };
    let mut context = read_context(
        &read,
        scenario_receipt(read.page.receipt.sequence.0 + 1),
        trade.received_at.unix_timestamp(),
    );
    context.applied_configuration = applied_configuration;
    context.observation_provenance = observation_provenance;
    context.observed_source_receipts = observed_source_receipts;
    let (committed, acknowledged) = oneshot::channel();
    control
        .send(OrchestratorControl::CommitActivityBucket {
            aggregates: read.aggregates,
            context: Arc::new(context),
            committed,
        })
        .await
        .unwrap();
    acknowledged.await.unwrap().unwrap();
}

/// Standard admitted-entry context, with receipt identity supplied by the caller's log.
pub fn read_context(
    read: &ProducerShapedRead,
    commitment: AppendReceipt,
    recorded_at_unix: i64,
) -> BucketDecisionContext {
    BucketDecisionContext {
        applied_configuration: RuntimeConfig::from_service_config(&ServiceConfig::default()),
        decision_inputs_json: read.decision_inputs_json.clone(),
        page_occurrences: vec![read.page.clone()],
        read_commitment: Some(if read.commitment_version == 2 {
            pe_service::bucket_commit::ActivityReadCommitmentReceipt::BindingsV2(commitment)
        } else {
            pe_service::bucket_commit::ActivityReadCommitmentReceipt::LegacyV1(commitment)
        }),
        observed_source_receipts: HashMap::new(),
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        signal_config: SignalConfig::default(),
        copy_eligible: true,
        bracket_commit: false,
        recorded_at_unix,
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
        identity_overrides: HashMap::new(),
        identity_unresolved: Default::default(),
        history_status: None,
    }
}

/// Production continuation owner with deterministic clocks and an admission sentinel.
/// A consumed sentinel counts an admission read; the DB and paper log count downstream effects.
pub fn continuation_orchestrator(
    paper: Arc<pe_paper_state::PaperStateDb>,
    paper_path: &std::path::Path,
    wallet: WalletAddress,
    control_rx: mpsc::Receiver<OrchestratorControl>,
    hooks: Arc<pe_service::orchestrator::ScenarioHooks>,
) -> pe_service::orchestrator::Orchestrator<
    pe_source_polymarket_public::FixtureFetcher,
    pe_service::clob_book::FixtureClobBookFetcher,
> {
    use pe_core_types::{BasisPoints, SourceTimestamp};
    use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
    use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
    let runtime = RuntimeConfig::from_service_config(&ServiceConfig::default());
    let ledger = pe_service::paper_recovery::build_leader_ledger(&paper).unwrap();
    let mut orchestrator = Orchestrator::new(
        pe_service::live_watchlist::LiveWatchlist::new(Watchlist {
            entries: vec![WatchlistEntry {
                wallet,
                tier: WatchlistTier::Active,
                leader_score_bps: BasisPoints(100),
                lcb_5pct_bps: BasisPoints(100),
                win_rate_bps: BasisPoints(7_000),
                closed_trades_in_window: 1,
                reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
            }],
            snapshot_at: SourceTimestamp(time::OffsetDateTime::UNIX_EPOCH),
            active_count: 1,
            incubator_count: 0,
        }),
        OrchestratorConfig {
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
            bankroll: rust_decimal::Decimal::from(10_000u32),
            mode: pe_strategy_winner_follow::ExecutionMode::Paper,
            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: rust_decimal::Decimal::ZERO,
            min_fill_price: rust_decimal::Decimal::ZERO,
            price_impact_cap_bps: 100,
            entry_gate_config: pe_service::entry_gate::CopyEntryGateConfig,
            runtime_config: None,
            live_accounts: None,
        },
        pe_strategy_winner_follow::WinnerFollowStrategy::new(runtime.winner_follow_config()),
        Writer::open(paper_path).unwrap(),
        paper,
        ledger,
        pe_service::health::new_shared_health(false),
        pe_service::mid_price_cache::MidPriceCache::with_fetcher(
            pe_source_polymarket_public::FixtureFetcher::new(HashMap::new()),
            "https://scenario.test".to_owned(),
        ),
        control_rx,
        None,
        None,
        None,
        Arc::new(pe_service::clob_book::FixtureClobBookFetcher::new(
            HashMap::new(),
        )),
    )
    .unwrap();
    orchestrator.set_scenario_hooks(hooks);
    orchestrator
}

pub fn continuation_hooks(epoch: i64) -> Arc<pe_service::orchestrator::ScenarioHooks> {
    use pe_core_types::{PolymarketConditionId, PolymarketTokenId, ShareAmount};
    use pe_execution_core::{AdmissionReceipts, LiveAdmissionArtifact};
    use pe_resolver_card::{
        VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
    };
    use pe_source_polymarket_public::LiveMarketEvidence;
    use pe_venue_polymarket::CompactFeeSchedule;
    let hooks = Arc::new(pe_service::orchestrator::ScenarioHooks::default());
    hooks.age_clock.lock().unwrap().extend(std::iter::repeat_n(
        time::OffsetDateTime::from_unix_timestamp(epoch).unwrap(),
        32,
    ));
    hooks
        .financial_clock_unix
        .store(epoch, std::sync::atomic::Ordering::SeqCst);
    hooks
        .admission_artifacts
        .lock()
        .unwrap()
        .push_back(LiveAdmissionArtifact {
            market: LiveMarketEvidence {
                condition_id: PolymarketConditionId("scenario-admission-sentinel".to_owned()),
                ordered_outcome_token_ids: [
                    PolymarketTokenId("sentinel-0".to_owned()),
                    PolymarketTokenId("sentinel-1".to_owned()),
                ],
                neg_risk: false,
                minimum_tick_size: Price::new(rust_decimal_macros::dec!(0.01)).unwrap(),
                minimum_order_size: ShareAmount::from_whole(1).unwrap(),
                scheduled_end_unix: None,
                observed_at_unix: epoch,
                schema_version: pe_source_polymarket_public::LIVE_MARKET_SCHEMA_VERSION,
                parser_version: pe_source_polymarket_public::LIVE_MARKET_PARSER_VERSION,
                freshness_window_secs: 60,
            },
            settlement: VenueSettlementRecord {
                schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                condition_id: PolymarketConditionId("scenario-admission-sentinel".to_owned()),
                status: VenueResolutionStatus::Unresolved,
                raw_evidence_hash: "scenario".to_owned(),
                source_timestamp_unix: None,
                observed_at_unix: epoch,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            fee_schedule: CompactFeeSchedule::Zero,
            receipts: AdmissionReceipts {
                gamma: scenario_receipt(1),
                clob_long: scenario_receipt(1),
                clob_compact: scenario_receipt(1),
            },
        });
    hooks
}

pub fn assert_no_continuation_side_effects(
    paper: &pe_paper_state::PaperStateDb,
    state_path: &std::path::Path,
    paper_path: &std::path::Path,
    hooks: &pe_service::orchestrator::ScenarioHooks,
) {
    assert_eq!(
        hooks.admission_artifacts.lock().unwrap().len(),
        1,
        "zero admission reads"
    );
    let connection = rusqlite::Connection::open(state_path).unwrap();
    let seeds: u64 = connection
        .query_row("SELECT COUNT(*) FROM dispatch_seeds", [], |row| row.get(0))
        .unwrap();
    assert_eq!(seeds, 0, "zero dispatch seeds, including terminal seeds");
    assert_eq!(
        paper.financial_last_prepared_seq().unwrap(),
        None,
        "zero prepares"
    );
    assert_eq!(paper.fills_count().unwrap(), 0, "zero paper orders/fills");
    let records = pe_service::paper_recovery::scan_paper_log(paper_path).unwrap();
    assert_eq!(records.len(), 0, "zero Prepared/Final/order records");
}

/// Seal a source-census fixture through the public qualification entry point. The fixture owns
/// no membership or financial mutation; callers assert the exact source-verification outcome.
pub async fn qualify_source_census(
    directory: &std::path::Path,
    source_log: &std::path::Path,
    paper_state: &std::path::Path,
    now_unix: i64,
) -> pe_service::qualification::QualificationReport {
    use pe_event_log::Scanner;
    use pe_service::paper_recovery::{
        PAPER_LOG_SCHEMA_VERSION, PaperLogRecord, QualificationSealed, QualificationStarted,
        SealReason, TailBinding,
    };
    std::fs::create_dir_all(directory).unwrap();
    let paper_log = directory.join("paper.log");
    let live_journal = directory.join("live.log");
    let mut writer = Writer::open(&paper_log).unwrap();
    drop(pe_execution_core::LiveJournal::open(&live_journal).unwrap());
    let empty = TailBinding::from(&Scanner::verify(&paper_log).unwrap());
    // Exact empty membership manifest and binding field order owned by MembershipProofBinding.
    let manifest = r#"{"membership":[],"proofs":[]}"#;
    let proof_hash = blake3::hash(manifest.as_bytes()).to_hex().to_string();
    let start = QualificationStarted {
        starting_bankroll: pe_core_types::CollateralAmount::ZERO,
        paper_prefix: empty.clone(),
        source_prefix: empty.clone(),
        live_prefix: empty.clone(),
        artifact_blake3: "scenario".to_owned(),
        static_config_hash: "scenario".to_owned(),
        hot_config_hash: "scenario".to_owned(),
        generation: "scenario".to_owned(),
        activation_id: "scenario".to_owned(),
        ranking_batch_id: 1,
        membership: Vec::new(),
        membership_proofs_hash: format!(
            r#"{{"version":1,"proof_hash":"{proof_hash}","manifest":{manifest}}}"#
        ),
        schema_version: PAPER_LOG_SCHEMA_VERSION,
        parser_version: 1,
        financial_semantic_version: 1,
    };
    let envelope = |record: PaperLogRecord| {
        let timestamp = time::OffsetDateTime::from_unix_timestamp(now_unix).unwrap();
        EnvelopeIn {
            source_id: SourceId("pe-service.qualification".to_owned()),
            schema_version: PAPER_LOG_SCHEMA_VERSION,
            parser_version: 1,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&record).unwrap(),
        }
    };
    let start_receipt = writer
        .append_synced(envelope(PaperLogRecord::QualificationStarted(Box::new(
            start,
        ))))
        .unwrap();
    let financial_prefix = TailBinding::from(&Scanner::verify(&paper_log).unwrap());
    let seal = QualificationSealed {
        start_receipt,
        source_prefix: TailBinding::from(&Scanner::verify(source_log).unwrap()),
        financial_prefix,
        live_prefix: empty,
        decision_evidence_digest: blake3::hash(b"[]").to_hex().to_string(),
        sealed_cutoff_unix: now_unix,
        reason: SealReason::Complete,
    };
    let seal_receipt = writer
        .append_synced(envelope(PaperLogRecord::QualificationSealed(Box::new(
            seal,
        ))))
        .unwrap();
    drop(writer);
    let output = directory.join("qualification.json");
    pe_service::qualification::run_qualify(&pe_service::qualification::QualifyOptions {
        paper_log,
        source_log: source_log.to_owned(),
        live_journal: Some(live_journal),
        paper_state: paper_state.to_owned(),
        seal_hash: seal_receipt.this_hash.to_hex().to_string(),
        output: output.clone(),
    })
    .await
    .unwrap();
    serde_json::from_slice(&std::fs::read(output).unwrap()).unwrap()
}

/// Retained generation-four facts in each historical continuation wire encoding.
pub fn legacy_continuation_wire(version: u16) -> serde_json::Value {
    let mut wire: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/decision_continuation_v4.json")).unwrap();
    wire["version"] = version.into();
    if version < 4 {
        wire.as_object_mut().unwrap().remove("read_commitment");
    }
    if version == 2 {
        wire.as_object_mut().unwrap().remove("page_occurrences");
        wire.as_object_mut()
            .unwrap()
            .remove("observed_source_receipt");
        let config = wire["applied_configuration"].as_object_mut().unwrap();
        assert_eq!(config.remove("era").unwrap(), "legacy17");
        let compatibility = config.remove("legacy_compatibility").unwrap();
        for (key, value) in compatibility.as_object().unwrap() {
            config.insert(key.clone(), value.clone());
        }
    }
    wire
}

/// Install an empty anchor through the bucket owner so qualification can verify its balance hash.
pub fn install_verified_empty_anchor(
    paper: &Arc<pe_paper_state::PaperStateDb>,
    wallet: WalletAddress,
    cutoff: i64,
) {
    paper.set_cursor(&wallet, cutoff).unwrap();
    use pe_service::position_seeder::{
        AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture,
    };
    let mut engine = pe_service::bucket_commit::BucketCommitEngine::load(
        paper.clone(),
        pe_service::paper_recovery::build_leader_ledger(paper).unwrap(),
    )
    .unwrap();
    let captured = ledger_capture(engine.ledger(), paper, wallet).unwrap();
    engine
        .install_anchors(&[AnchorInstall {
            wallet,
            balances: Vec::new(),
            cutoff,
            proof: AnchorProof {
                positions_proof_hash: "scenario-positions".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "scenario".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix: cutoff,
            },
            expected: AnchorExpectation {
                ledger_hash: captured.hash,
                cursor: captured.cursor,
                anchor_seq: captured.anchor_seq,
                coverage_generation: captured.coverage_generation,
            },
            history_status: None,
        }])
        .unwrap();
}

// Explicit response barriers shared by the poller and deployed-flow scenarios.
pub struct RequestedPage {
    pub url: String,
    pub respond: PageResponse,
}

pub struct PageResponse(oneshot::Sender<Result<Vec<u8>, SourceError>>);

impl PageResponse {
    pub fn send(self, payload: Vec<u8>) -> Result<(), Result<Vec<u8>, SourceError>> {
        self.0.send(Ok(payload))
    }

    pub fn fail(self) {
        self.0
            .send(Err(SourceError::Transient {
                message: "injected retryable read failure".to_owned(),
            }))
            .unwrap();
    }
}

pub struct GatedFetcher {
    pub requests: mpsc::Sender<RequestedPage>,
}

impl ReconciliationFetcher for GatedFetcher {
    fn fetch<'a>(
        &'a self,
        url: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async move {
            let (respond, response) = oneshot::channel();
            self.requests
                .send(RequestedPage {
                    url: url.to_owned(),
                    respond: PageResponse(respond),
                })
                .await
                .map_err(|_| SourceError::Fatal {
                    message: "request barrier closed".to_owned(),
                })?;
            response.await.map_err(|_| SourceError::Fatal {
                message: "response barrier closed".to_owned(),
            })?
        })
    }
}

// The cold portfolio-price barrier used by the final paper freshness scenarios.
#[derive(Default)]
pub struct PriceGate {
    pub blocked: AtomicBool,
    pub market: Mutex<Option<String>>,
    pub started: Notify,
    pub release: Notify,
}
#[derive(Clone)]
pub struct Prices {
    pub gate: Arc<PriceGate>,
    pub markets: Arc<Mutex<HashMap<String, Value>>>,
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
pub struct Page(pub Vec<u8>);
impl ReconciliationFetcher for Page {
    fn fetch<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, SourceError>> + Send + 'a>> {
        Box::pin(async { Ok(self.0.clone()) })
    }
}
