//! Producer-shaped activity fixtures and deterministic continuation harnesses.
//! Historical wire encoders remain test-only compatibility fixtures.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use pe_copy_signal_engine::{IncomingTrade, SignalConfig, TradeProvenance};
use pe_core_types::{
    EventSeq, Price, ReceivedAt, ReconstructionQuality, Side, SourceId, SourceTimestamp,
    WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Writer};
use pe_service::bucket_commit::{
    ACTIVITY_READ_COMMITMENT_PARSER_VERSION, ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
    ACTIVITY_READ_COMMITMENT_SOURCE_ID, BucketDecisionContext, DecisionContinuationFacts,
    PageOccurrence, activity_read_commitment_payload,
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
}

/// Parse a short offset-zero page with no lower bound. Receive time is independent of the end.
pub fn producer_shaped_read(
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
    let rows: Vec<serde_json::Value> = serde_json::from_slice(payload).unwrap();
    assert!(
        rows.len()
            < usize::try_from(pe_source_polymarket_public::RECONCILIATION_PAGE_LIMIT).unwrap()
    );
    let raw_hash = blake3::hash(payload).to_hex().to_string();
    let evidence = ReconciliationPageEvidence {
        request_url: request_url.clone(),
        bounds: Some(ActivityRequestBounds {
            start: None,
            end: fixed_end,
        }),
        partition: None,
        offset: 0,
        row_count: u32::try_from(rows.len()).unwrap(),
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
    let commitment_payload = activity_read_commitment_payload(
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
    }
}

/// Synthetic receipt for scenarios that exercise the engine without a source log.
pub fn scenario_receipt(sequence: u64) -> AppendReceipt {
    AppendReceipt {
        sequence: EventSeq(sequence),
        this_hash: blake3::hash(format!("scenario-receipt-{sequence}").as_bytes()),
    }
}

/// Append a successor-generation page and its genuine commitment to a real source log.
pub fn append_committed_read(
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
    let read = producer_shaped_read(wallet, payload, fixed_end, received_unix, page_receipt);
    let commitment = writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(ACTIVITY_READ_COMMITMENT_SOURCE_ID.to_owned()),
            schema_version: ACTIVITY_READ_COMMITMENT_SCHEMA_VERSION,
            parser_version: ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
            observed_at: SourceTimestamp(received_at.0),
            received_at: received_at.clone(),
            content_type: ContentType::Json,
            payload: read.commitment_payload.clone(),
        })
        .unwrap();
    (read, commitment)
}

pub fn legacy_continuation_v2_json(facts: &DecisionContinuationFacts) -> String {
    let facts = serde_json::to_string(facts).unwrap();
    format!(r#"{{"version":2,{}"#, facts.strip_prefix('{').unwrap())
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
    let read = producer_shaped_read(
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
        read_commitment: Some(commitment),
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
