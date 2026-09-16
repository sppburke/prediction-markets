//! I16 golden future-stream source and economic replay scenario.

#![cfg(feature = "scenario")]
#![allow(dead_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

#[path = "mod.rs"]
mod support;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pe_copy_signal_engine::{SignalConfig, TradeProvenance};
use pe_core_types::{
    AccountId, BasisPoints, CollateralAmount, EventSeq, OutcomeId, PolymarketConditionId, Price,
    ReceivedAt, ReconstructionQuality, ShareAmount, Side, SourceId, SourceTimestamp, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Reader, Scanner, Writer};
use pe_execution_core::{
    AdmissionReceipts, CredentialBindingIdentity, EconomicInputs, EconomicPrepared,
    FrozenLiveTarget, LiveAdmissionArtifact, LiveControlMode, LiveExecutor, LiveJournal,
    LiveModeSnapshot, LiveOrderIdentity, LiveOrderPreparedAudit, LiveOrderRequest, LiveOrderVenue,
    LivePostClassification, LivePostFuture, LivePostParseError, LivePrepareResult,
    LiveReconciliationFuture, LiveVenueAccountReadError, LiveVenueAccountState,
    LiveVenuePrepareFuture, LiveVenuePrepareRequest, LiveVenuePrepared, RiskDecisionAudit,
};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_risk_engine::{BinaryPayout, RiskBlock, RiskHaltCause, aggregate_resolution_credit};
use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
use pe_service::bucket_commit::{
    ACTIVITY_READ_COMMITMENT_PARSER_VERSION, ACTIVITY_READ_COMMITMENT_SOURCE_ID,
    ActivityReadCommitment, BucketCommitEngine, BucketDecisionContext, DecisionContinuationV3,
};
use pe_service::clob_book::{ClobBookError, ClobBookFetcher, OrderBook};
use pe_service::decision_replay::{
    DecisionPostBoundaryEvidence, WinnerFollowDecisionInputs, replay_decision_pending,
};
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_venue_adapter::LiveAdmissionBuilder;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mark_prices::HistoricalMarkAdapter;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig, ScenarioHooks};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{
    CanonicalFillResult, CanonicalResolutionResult, FINANCIAL_SEMANTIC_VERSION, HaltState,
    MembershipChange, MembershipReason, PAPER_LOG_SCHEMA_VERSION, PaperLogFrame, PaperLogRecord,
    RiskHaltOwner, SealReason, TailBinding, paper_era, scan_paper_log,
};
use pe_service::position_seeder::{AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture};
use pe_service::qualification::{
    FinancialEraCommand, FinancialEraManifest, FinancialEraPaths, FinancialEraPreparation,
    QualificationReport, QualificationVerdict, qualification_completion, run_financial_era,
};
use pe_service::risk_inputs::{RiskInputsUnavailable, SourceReceiptIndex};
use pe_service::runtime_config::{ConfigRow, RuntimeConfig};
use pe_service::source_event_sink::SourceEventSink;
use pe_service::supabase_sink::SupabaseFillRow;
use pe_service::supabase_state::{
    FillV2Outcome, PreparedFillRequest, PreparedResolutionRequest, SupabaseStateError,
    SupabaseStateTrait,
};
use pe_service::trade_poller::{ACTIVITY_POLL_PAGE_SCHEMA_VERSION, ACTIVITY_POLL_SOURCE_ID};
use pe_source_polymarket_public::{
    BinaryPayoutVector, CLOB_RESOLUTION_PARSER_VERSION, CLOB_RESOLUTION_SCHEMA_VERSION,
    LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION, PageFetcher, validate_live_market,
};
use pe_strategy_winner_follow::{
    ExecutionMode, PerTradeCap, SizingMode, WinnerFollowDeclineAudit, WinnerFollowStrategy,
};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::{AskLevel, LadderPlan, PreparedPolymarketBuy, parse_compact_market};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

const FIXTURE: &str = "fixtures/golden_stream_v1";
const CONDITION: &str = "0x4c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ee";
const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FIXED_UNIX: i64 = 1_800_000_000;
const DAY_SECS: i64 = 86_400;
const QUALIFICATION_DAYS: usize = 30;
const COPIES_PER_DAY: usize = 3;
const ORIGINAL_COPIES: usize = QUALIFICATION_DAYS * COPIES_PER_DAY;
const HELD_COPY: usize = COPIES_PER_DAY;
const PRICE_CONFLICT_DECISION: usize = ORIGINAL_COPIES;
// Three normal wins earn $7.47; eleven $2.55 losses then leave the day at -$20.58.
const LOSS_COPIES: usize = 11;
const FIRST_LOSS_COPY: usize = PRICE_CONFLICT_DECISION + 1;
const BLOCKED_DECISION: usize = FIRST_LOSS_COPY + LOSS_COPIES;
const TOTAL_DECISIONS: usize = BLOCKED_DECISION + 1;
/// Corpus day that streams the losing copies: the intraday stop engages here and releases
/// mechanically at the next day's first evaluation.
const LOSS_DAY: usize = 2;
const TOTAL_FILLS: usize = ORIGINAL_COPIES + LOSS_COPIES;
const STARTING_BANKROLL: Decimal = dec!(1_000);

fn fixture(name: &str) -> &'static [u8] {
    match name {
        "activity_page" => include_bytes!("../fixtures/golden_stream_v1/activity_page.json"),
        "websocket_trigger" => {
            include_bytes!("../fixtures/golden_stream_v1/websocket_trigger.json")
        }
        "gamma_long" => include_bytes!("../fixtures/golden_stream_v1/gamma_long.json"),
        "clob_long" => include_bytes!("../fixtures/golden_stream_v1/clob_long.json"),
        "clob_compact" => include_bytes!("../fixtures/golden_stream_v1/clob_compact.json"),
        "book" => include_bytes!("../fixtures/golden_stream_v1/book.json"),
        "prices_history" => include_bytes!("../fixtures/golden_stream_v1/prices_history.json"),
        "clob_resolution" => include_bytes!("../fixtures/golden_stream_v1/clob_resolution.json"),
        "expected" => include_bytes!("../fixtures/golden_stream_v1/expected.json"),
        other => panic!("unknown golden fixture {other}"),
    }
}

async fn append_source_at(
    source_log: &SourceLogHandle,
    source_id: &str,
    payload: &[u8],
    received_at_unix: i64,
) -> AppendReceipt {
    let timestamp = OffsetDateTime::from_unix_timestamp(received_at_unix).unwrap();
    let version = if source_id == "polymarket.clob.market" {
        CLOB_RESOLUTION_SCHEMA_VERSION
    } else if matches!(
        source_id,
        "polymarket-activity-ws" | "polymarket-public.activity-reconciliation"
    ) {
        2
    } else {
        1
    };
    let (schema_version, parser_version) = match source_id {
        ACTIVITY_POLL_SOURCE_ID => (
            ACTIVITY_POLL_PAGE_SCHEMA_VERSION,
            pe_source_polymarket_public::ACTIVITY_PARSER_VERSION,
        ),
        ACTIVITY_READ_COMMITMENT_SOURCE_ID => (
            u32::from(
                serde_json::from_slice::<ActivityReadCommitment>(payload)
                    .unwrap()
                    .version,
            ),
            ACTIVITY_READ_COMMITMENT_PARSER_VERSION,
        ),
        _ => (version, version),
    };
    source_log
        .append(EnvelopeIn {
            source_id: SourceId(source_id.to_owned()),
            schema_version,
            parser_version,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        })
        .await
        .unwrap()
}

fn append_qualification_start(
    writer: &mut Writer,
    start: &pe_service::paper_recovery::QualificationStarted,
    received_at_unix: i64,
) -> AppendReceipt {
    let timestamp = OffsetDateTime::from_unix_timestamp(received_at_unix).unwrap();
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.qualification".to_owned()),
            schema_version: PAPER_LOG_SCHEMA_VERSION,
            parser_version: start.parser_version,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(&PaperLogRecord::QualificationStarted(Box::new(
                start.clone(),
            )))
            .unwrap(),
        })
        .unwrap()
}

fn golden_source_unix(anchor_cutoff: i64, index: usize) -> i64 {
    if index == PRICE_CONFLICT_DECISION {
        return golden_source_unix(anchor_cutoff, HELD_COPY) + 5;
    }
    if index >= FIRST_LOSS_COPY {
        return golden_source_unix(anchor_cutoff, (LOSS_DAY + 1) * COPIES_PER_DAY - 1)
            + 10 * i64::try_from(index - FIRST_LOSS_COPY + 1).unwrap();
    }
    let day = index / COPIES_PER_DAY;
    let within_day = index % COPIES_PER_DAY;
    if day == 0 {
        FIXED_UNIX + i64::try_from(within_day).unwrap() * 10
    } else {
        anchor_cutoff
            + i64::try_from(day).unwrap() * DAY_SECS
            + 3_600
            + i64::try_from(within_day).unwrap() * 10
    }
}

#[derive(Clone)]
struct GoldenTradeBodies {
    wallet: WalletAddress,
    condition: PolymarketConditionId,
    activity: Vec<u8>,
    websocket: Vec<u8>,
    gamma: Vec<u8>,
    clob_long: Vec<u8>,
    compact: Vec<u8>,
    book: Vec<u8>,
    resolution: Vec<u8>,
}

#[derive(Clone, Copy)]
struct GoldenAdmissionBodies<'a> {
    gamma: &'a [u8],
    clob: &'a [u8],
    compact: &'a [u8],
    book: &'a [u8],
}

struct GoldenRecordedTrade {
    bodies: GoldenTradeBodies,
    websocket_receipt: AppendReceipt,
    page_receipt: AppendReceipt,
    commitment_receipt: AppendReceipt,
    admission: LiveAdmissionArtifact,
    book: OrderBook,
    resolution_receipt: AppendReceipt,
}

fn golden_trade_bodies(index: usize, source_unix: i64) -> GoldenTradeBodies {
    if index == 0 {
        return GoldenTradeBodies {
            wallet: WalletAddress::from_hex(WALLET).unwrap(),
            condition: PolymarketConditionId(CONDITION.to_owned()),
            activity: fixture("activity_page").to_vec(),
            websocket: fixture("websocket_trigger").to_vec(),
            gamma: fixture("gamma_long").to_vec(),
            clob_long: fixture("clob_long").to_vec(),
            compact: fixture("clob_compact").to_vec(),
            book: fixture("book").to_vec(),
            resolution: fixture("clob_resolution").to_vec(),
        };
    }

    let wallet_hex = if index >= ORIGINAL_COPIES {
        WALLET.to_owned()
    } else {
        format!("0x{:040x}", index + 1)
    };
    let condition = format!("0x{:064x}", index + 1);
    let transaction = format!("0x{:064x}", index + 10_000);
    let yes_token = (index * 2 + 101).to_string();
    let no_token = (index * 2 + 102).to_string();

    let mut activity: serde_json::Value = serde_json::from_slice(fixture("activity_page")).unwrap();
    activity[0]["proxyWallet"] = wallet_hex.clone().into();
    activity[0]["timestamp"] = source_unix.into();
    activity[0]["conditionId"] = condition.clone().into();
    activity[0]["transactionHash"] = transaction.clone().into();
    activity[0]["asset"] = yes_token.clone().into();

    let mut websocket: serde_json::Value =
        serde_json::from_slice(fixture("websocket_trigger")).unwrap();
    websocket["proxyWallet"] = wallet_hex.clone().into();
    websocket["timestamp"] = source_unix.to_string().into();
    websocket["conditionId"] = condition.clone().into();
    websocket["transactionHash"] = transaction.into();
    websocket["asset"] = yes_token.clone().into();

    let mut gamma: serde_json::Value = serde_json::from_slice(fixture("gamma_long")).unwrap();
    gamma[0]["conditionId"] = condition.clone().into();
    gamma[0]["clobTokenIds"] = serde_json::to_string(&[yes_token.clone(), no_token.clone()])
        .unwrap()
        .into();

    let mut clob_long: serde_json::Value = serde_json::from_slice(fixture("clob_long")).unwrap();
    clob_long["condition_id"] = condition.clone().into();
    clob_long["end_date_iso"] = "2030-01-01T00:00:00Z".into();
    clob_long["tokens"][0]["token_id"] = yes_token.clone().into();
    clob_long["tokens"][1]["token_id"] = no_token.clone().into();

    let mut compact: serde_json::Value = serde_json::from_slice(fixture("clob_compact")).unwrap();
    compact["c"] = condition.clone().into();
    compact["t"][0]["t"] = yes_token.clone().into();
    compact["t"][1]["t"] = no_token.clone().into();

    let mut book: serde_json::Value = serde_json::from_slice(fixture("book")).unwrap();
    book["market"] = condition.clone().into();
    book["asset_id"] = yes_token.clone().into();
    book["timestamp"] = source_unix.checked_mul(1_000).unwrap().to_string().into();

    let mut resolution: serde_json::Value =
        serde_json::from_slice(fixture("clob_resolution")).unwrap();
    resolution["condition_id"] = condition.clone().into();
    resolution["tokens"][0]["token_id"] = yes_token.into();
    resolution["tokens"][1]["token_id"] = no_token.into();

    if (FIRST_LOSS_COPY..BLOCKED_DECISION).contains(&index) {
        resolution["tokens"][0]["winner"] = false.into();
        resolution["tokens"][0]["price"] = "0".into();
        resolution["tokens"][1]["winner"] = true.into();
        resolution["tokens"][1]["price"] = "1".into();
    }

    GoldenTradeBodies {
        wallet: WalletAddress::from_hex(&wallet_hex).unwrap(),
        condition: PolymarketConditionId(condition),
        activity: serde_json::to_vec(&activity).unwrap(),
        websocket: serde_json::to_vec(&websocket).unwrap(),
        gamma: serde_json::to_vec(&gamma).unwrap(),
        clob_long: serde_json::to_vec(&clob_long).unwrap(),
        compact: serde_json::to_vec(&compact).unwrap(),
        book: serde_json::to_vec(&book).unwrap(),
        resolution: serde_json::to_vec(&resolution).unwrap(),
    }
}

fn admission_and_book(
    condition: &PolymarketConditionId,
    observed_at_unix: i64,
    bodies: GoldenAdmissionBodies<'_>,
    receipts: AdmissionReceipts,
    book_receipt: AppendReceipt,
) -> (LiveAdmissionArtifact, OrderBook) {
    let market =
        validate_live_market(bodies.gamma, bodies.clob, condition, observed_at_unix, 60).unwrap();
    let compact =
        parse_compact_market(bodies.compact, condition, &market.ordered_outcome_token_ids).unwrap();
    let admission = LiveAdmissionArtifact {
        market,
        settlement: VenueSettlementRecord {
            schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: condition.clone(),
            status: VenueResolutionStatus::Unresolved,
            raw_evidence_hash: blake3::hash(bodies.clob).to_hex().to_string(),
            source_timestamp_unix: None,
            observed_at_unix,
            parser_version: 1,
            freshness_window_secs: 60,
        },
        fee_schedule: compact.fee_schedule,
        receipts,
    };
    let mut book = OrderBook::from_book_json(bodies.book).unwrap();
    book.source_receipt = Some(book_receipt);
    (admission, book)
}

#[derive(Clone)]
struct GoldenAuthority {
    state: Arc<Mutex<GoldenAuthorityState>>,
}

struct GoldenAuthorityState {
    start: AppendReceipt,
    last_prepared: Option<EventSeq>,
    bankroll: Decimal,
    positions: HashMap<(String, u16), ShareAmount>,
    fills: HashMap<String, (PreparedFillRequest, CanonicalFillResult)>,
    resolutions: HashMap<String, (PreparedResolutionRequest, CanonicalResolutionResult)>,
    mutations: usize,
}

impl GoldenAuthority {
    fn new(start: AppendReceipt) -> Self {
        Self {
            state: Arc::new(Mutex::new(GoldenAuthorityState {
                start,
                last_prepared: None,
                bankroll: STARTING_BANKROLL,
                positions: HashMap::new(),
                fills: HashMap::new(),
                resolutions: HashMap::new(),
                mutations: 0,
            })),
        }
    }

    fn mutations(&self) -> usize {
        self.state.lock().unwrap().mutations
    }
}

impl SupabaseStateTrait for GoldenAuthority {
    async fn commit_fill_v2(
        &self,
        _row: &SupabaseFillRow,
    ) -> Result<FillV2Outcome, SupabaseStateError> {
        Err(SupabaseStateError::Corrupt(
            "golden active era must use commit_prepared_fill".to_owned(),
        ))
    }

    async fn commit_prepared_fill(
        &self,
        request: &PreparedFillRequest,
    ) -> Result<CanonicalFillResult, SupabaseStateError> {
        let mut state = self.state.lock().unwrap();
        if state.start != request.expected_authority.qualification_start_receipt {
            return Err(SupabaseStateError::Conflict {
                reason: "financial Start differs".to_owned(),
            });
        }
        if let Some((stored_request, stored_result)) = state.fills.get(&request.idempotency_key) {
            if stored_request != request
                || state.last_prepared != Some(request.prepared_receipt.sequence)
            {
                return Err(SupabaseStateError::Conflict {
                    reason: "fill retry identity or economics changed".to_owned(),
                });
            }
            let mut result = stored_result.clone();
            result.outcome = "existing".to_owned();
            return Ok(result);
        }
        if state.last_prepared != request.expected_authority.prior_completed_prepared_sequence {
            return Err(SupabaseStateError::Conflict {
                reason: "fill predecessor differs".to_owned(),
            });
        }
        if request.side != Side::Buy {
            return Err(SupabaseStateError::Corrupt(
                "golden authority received a non-BUY prepared fill".to_owned(),
            ));
        }
        let debit = request
            .principal
            .checked_add(request.fee)
            .map_err(|error| SupabaseStateError::Corrupt(error.to_string()))?
            .to_decimal();
        state.bankroll = state
            .bankroll
            .checked_sub(debit)
            .filter(|cash| *cash >= Decimal::ZERO)
            .ok_or_else(|| SupabaseStateError::Conflict {
                reason: "fill debit exceeds bankroll".to_owned(),
            })?;
        let position = state
            .positions
            .entry((request.market_id.clone(), request.outcome_id))
            .or_insert(ShareAmount::ZERO);
        *position = position
            .checked_add(request.quantity)
            .map_err(|error| SupabaseStateError::Corrupt(error.to_string()))?;
        let result = CanonicalFillResult {
            outcome: "applied".to_owned(),
            bankroll: state.bankroll,
            applied_prepared_seq: request.prepared_receipt.sequence,
            quantity: request.quantity,
            principal: request.principal,
            fee: request.fee,
            fill_price: request.fill_price,
        };
        state.fills.insert(
            request.idempotency_key.clone(),
            (request.clone(), result.clone()),
        );
        state.last_prepared = Some(request.prepared_receipt.sequence);
        state.mutations += 1;
        Ok(result)
    }

    async fn apply_prepared_resolution(
        &self,
        request: &PreparedResolutionRequest,
    ) -> Result<CanonicalResolutionResult, SupabaseStateError> {
        let mut state = self.state.lock().unwrap();
        if state.start != request.expected_authority.qualification_start_receipt {
            return Err(SupabaseStateError::Conflict {
                reason: "financial Start differs".to_owned(),
            });
        }
        if let Some((stored_request, stored_result)) = state.resolutions.get(&request.condition.0) {
            if stored_request != request
                || state.last_prepared != Some(request.prepared_receipt.sequence)
            {
                return Err(SupabaseStateError::Conflict {
                    reason: "resolution retry identity or economics changed".to_owned(),
                });
            }
            let mut result = stored_result.clone();
            result.outcome = "existing".to_owned();
            return Ok(result);
        }
        if state.last_prepared != request.expected_authority.prior_completed_prepared_sequence {
            return Err(SupabaseStateError::Conflict {
                reason: "resolution predecessor differs".to_owned(),
            });
        }
        let payout = BinaryPayoutVector::from_canonical_json(&request.payout_by_outcome_index_json)
            .map_err(|error| SupabaseStateError::Corrupt(error.to_string()))?;
        let binary = BinaryPayout::new(payout.decimals()[0], payout.decimals()[1])
            .map_err(|error| SupabaseStateError::Corrupt(error.to_string()))?;
        let positions = state
            .positions
            .iter()
            .filter(|((market, _), _)| market == &request.condition.0)
            .map(|((_, outcome), quantity)| (*outcome, *quantity))
            .collect::<Vec<_>>();
        let credit = aggregate_resolution_credit(&positions, &binary)
            .map_err(|error| SupabaseStateError::Corrupt(error.to_string()))?;
        state
            .positions
            .retain(|(market, _), _| market != &request.condition.0);
        state.bankroll = state
            .bankroll
            .checked_add(credit.to_decimal())
            .ok_or_else(|| SupabaseStateError::Corrupt("resolution credit overflow".to_owned()))?;
        let result = CanonicalResolutionResult {
            outcome: "applied".to_owned(),
            bankroll: state.bankroll,
            applied_prepared_seq: request.prepared_receipt.sequence,
            credit,
            settled_at_unix: request.settled_at_unix,
        };
        state.resolutions.insert(
            request.condition.0.clone(),
            (request.clone(), result.clone()),
        );
        state.last_prepared = Some(request.prepared_receipt.sequence);
        state.mutations += 1;
        Ok(result)
    }
}

#[derive(Default)]
struct GoldenBookFetcher {
    books: Mutex<HashMap<String, OrderBook>>,
}

impl GoldenBookFetcher {
    fn insert(&self, token_id: String, book: OrderBook) {
        self.books.lock().unwrap().insert(token_id, book);
    }
}

impl ClobBookFetcher for GoldenBookFetcher {
    async fn fetch_book(
        &self,
        _condition_id: &str,
        token_id: &str,
    ) -> Result<OrderBook, ClobBookError> {
        let mut book = self
            .books
            .lock()
            .unwrap()
            .get(token_id)
            .cloned()
            .ok_or_else(|| ClobBookError::MissingFixture(token_id.to_owned()))?;
        if book.fetched_at_ms == 0 {
            book.fetched_at_ms =
                u64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000)
                    .unwrap_or(0);
        }
        Ok(book)
    }
}

fn golden_watchlist(wallets: &[WalletAddress]) -> Watchlist {
    let quality = ReconstructionQuality::new(100).unwrap();
    Watchlist {
        entries: wallets
            .iter()
            .copied()
            .map(|wallet| WatchlistEntry {
                wallet,
                tier: WatchlistTier::Active,
                leader_score_bps: BasisPoints(200),
                lcb_5pct_bps: BasisPoints(200),
                win_rate_bps: BasisPoints(6_000),
                closed_trades_in_window: 90,
                reconstruction_quality: quality,
            })
            .collect(),
        snapshot_at: SourceTimestamp(OffsetDateTime::from_unix_timestamp(FIXED_UNIX).unwrap()),
        active_count: wallets.len(),
        incubator_count: 0,
    }
}

/// Gamma fixture that answers the production client's repeat-key batches
/// (`condition_ids=A&condition_ids=B&…`) with every requested market it knows, in request order.
struct GoldenGammaFetcher {
    markets: HashMap<String, serde_json::Value>,
    gate: Option<Arc<support::PriceGate>>,
}

impl PageFetcher for GoldenGammaFetcher {
    async fn fetch_page(&self, url: &str) -> Result<Vec<u8>, pe_source_core::SourceError> {
        if let Some(gate) = &self.gate
            && gate
                .market
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|market| url.contains(&format!("condition_ids={market}")))
            && gate
                .blocked
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            gate.started.notify_one();
            gate.release.notified().await;
        }
        let query = url.split_once('?').map_or("", |(_, query)| query);
        let markets = query
            .split('&')
            .filter_map(|pair| pair.strip_prefix("condition_ids="))
            .filter_map(|condition| self.markets.get(condition).cloned())
            .flat_map(|market| {
                if self.gate.is_none()
                    && market["conditionId"] == format!("0x{:064x}", HELD_COPY + 1)
                {
                    let mut conflicting = market.clone();
                    conflicting["outcomePrices"] = "[\"0.60\",\"0.40\"]".into();
                    vec![market, conflicting]
                } else {
                    vec![market]
                }
            })
            .collect::<Vec<_>>();
        Ok(serde_json::to_vec(&markets).unwrap())
    }
}

fn golden_mid_cache(anchor_cutoff: i64) -> MidPriceCache<GoldenGammaFetcher> {
    const BASE: &str = "fixture://gamma";
    let markets = (0..TOTAL_DECISIONS)
        .map(|index| {
            let source_unix = golden_source_unix(anchor_cutoff, index);
            let bodies = golden_trade_bodies(index, source_unix);
            let mut gamma: serde_json::Value = serde_json::from_slice(&bodies.gamma).unwrap();
            gamma[0]["outcomePrices"] = "[\"0.50\",\"0.50\"]".into();
            (bodies.condition.0.clone(), gamma[0].take())
        })
        .collect();
    MidPriceCache::with_fetcher(
        GoldenGammaFetcher {
            markets,
            gate: None,
        },
        BASE.to_owned(),
    )
}

struct GoldenLiveVenue;

impl LiveOrderVenue for GoldenLiveVenue {
    type Submission = ();

    fn prepare<'a>(
        &'a self,
        request: LiveVenuePrepareRequest,
    ) -> LiveVenuePrepareFuture<'a, Self::Submission> {
        Box::pin(async move {
            Ok(LiveVenuePrepared::new(
                PreparedPolymarketBuy {
                    condition_id: request.condition_id,
                    outcome_id: request.outcome_id,
                    token_id: request.token_id,
                    maker: "golden-maker".to_owned(),
                    signer: "golden-signer".to_owned(),
                    funder: "golden-funder".to_owned(),
                    verifying_contract: "golden-spender".to_owned(),
                    spender: "golden-spender".to_owned(),
                    exchange_domain_version: 2,
                    neg_risk: request.neg_risk,
                    side: "BUY".to_owned(),
                    salt: "545".to_owned(),
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
                    order_hash: "golden-order-hash".to_owned(),
                    post_body_hash: "golden-post-body-hash".to_owned(),
                    sdk_version: "golden-fixture".to_owned(),
                    sdk_archive_sha256: "golden-fixture-sha256".to_owned(),
                    metadata_hashes: request.metadata_hashes,
                    worst_case_debit: request.maximum_collateral,
                },
                (),
            ))
        })
    }

    fn post_once<'a>(&'a self, _submission: Self::Submission) -> LivePostFuture<'a> {
        Box::pin(std::future::pending())
    }

    fn classify_post_response(
        &self,
        _response: &pe_core_types::RawHttpResponse,
    ) -> Result<LivePostClassification, LivePostParseError> {
        Err(LivePostParseError::InvalidResponse)
    }

    fn reconcile_and_cancel_by_order_hash<'a>(
        &'a self,
        _order_hash: &'a str,
    ) -> LiveReconciliationFuture<'a> {
        Box::pin(std::future::pending())
    }

    fn read_balance_and_allowance<'a>(
        &'a self,
        _neg_risk: bool,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<LiveVenueAccountState, LiveVenueAccountReadError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let cash = CollateralAmount::from_decimal_exact(STARTING_BANKROLL).unwrap();
            Ok(LiveVenueAccountState {
                observed_at: OffsetDateTime::from_unix_timestamp(FIXED_UNIX).unwrap(),
                closed_only: false,
                geoblocked: false,
                selected_spender: "golden-spender".to_owned(),
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

fn ladder_from_economic(economic: &EconomicPrepared) -> LadderPlan {
    LadderPlan {
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
    }
}

fn run_qualify_cli(
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    live_path: &std::path::Path,
    state_path: &std::path::Path,
    seal_receipt: AppendReceipt,
    output_path: &std::path::Path,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_pe-service"))
        .arg("--qualify")
        .arg("--paper-log")
        .arg(paper_path)
        .arg("--source-log")
        .arg(source_path)
        .arg("--live-journal")
        .arg(live_path)
        .arg("--paper-state")
        .arg(state_path)
        .arg("--seal-hash")
        .arg(seal_receipt.this_hash.to_hex().as_str())
        .arg("--output")
        .arg(output_path)
        .output()
        .unwrap()
}

#[derive(Clone, Copy)]
enum PreimageMutation {
    Delete,
    Tamper,
}

impl PreimageMutation {
    fn label(self) -> &'static str {
        match self {
            Self::Delete => "deleted",
            Self::Tamper => "tampered",
        }
    }
}

type ReceiptMap = HashMap<(u64, String), AppendReceipt>;
type PrefixMap = HashMap<(u64, String), TailBinding>;

struct RewrittenSource {
    receipts: ReceiptMap,
    prefixes: PrefixMap,
}

fn receipt_map_key(receipt: AppendReceipt) -> (u64, String) {
    (receipt.sequence.0, receipt.this_hash.to_hex().to_string())
}

fn rewrite_source_preimage(
    source_path: &std::path::Path,
    destination: &std::path::Path,
    target: AppendReceipt,
    mutation: PreimageMutation,
) -> RewrittenSource {
    let mut writer = Writer::open(destination).unwrap();
    let mut found = false;
    let mut receipts = ReceiptMap::new();
    let mut prefixes = PrefixMap::new();
    let mut latest_page: Option<(Vec<u8>, i64, AppendReceipt)> = None;
    for item in Reader::replay(source_path).unwrap() {
        let (_, envelope) = item.unwrap();
        let original_receipt = AppendReceipt {
            sequence: envelope.seq,
            this_hash: envelope.this_hash,
        };
        let mut payload = envelope.payload;
        if envelope.seq == target.sequence {
            assert_eq!(envelope.this_hash, target.this_hash);
            found = true;
            match mutation {
                PreimageMutation::Delete => payload.clear(),
                PreimageMutation::Tamper => {
                    let byte = payload.last_mut().unwrap();
                    *byte ^= 1;
                }
            }
        }
        if envelope.source_id.0 == ACTIVITY_READ_COMMITMENT_SOURCE_ID {
            let commitment: ActivityReadCommitment = serde_json::from_slice(&payload).unwrap();
            let (page_payload, received_unix, receipt) = latest_page.as_ref().unwrap();
            let builder = if commitment.version == 2 {
                support::producer_shaped_read_v2
            } else {
                support::producer_shaped_read_v1
            };
            payload = builder(
                commitment.wallet,
                page_payload,
                commitment.fixed_end,
                *received_unix,
                *receipt,
            )
            .commitment_payload;
        }
        let page = (envelope.source_id.0 == ACTIVITY_POLL_SOURCE_ID)
            .then(|| (payload.clone(), envelope.received_at.0.unix_timestamp()));
        let rewritten_receipt = writer
            .append_synced(EnvelopeIn {
                source_id: envelope.source_id,
                schema_version: envelope.schema_version,
                parser_version: envelope.parser_version,
                observed_at: envelope.observed_at,
                received_at: envelope.received_at,
                content_type: envelope.content_type,
                payload,
            })
            .unwrap();
        if let Some((payload, received_unix)) = page {
            latest_page = Some((payload, received_unix, rewritten_receipt));
        }
        let physical_tail = std::fs::metadata(destination).unwrap().len();
        receipts.insert(receipt_map_key(original_receipt), rewritten_receipt);
        prefixes.insert(
            receipt_map_key(original_receipt),
            TailBinding {
                physical_tail,
                last_sequence: Some(rewritten_receipt.sequence),
                last_hash: rewritten_receipt.this_hash.to_hex().to_string(),
            },
        );
    }
    assert!(found);
    drop(writer);
    Scanner::verify(destination).unwrap();
    RewrittenSource { receipts, prefixes }
}

fn remap_receipts(
    value: &mut serde_json::Value,
    receipt_maps: &[&ReceiptMap],
    prefix_maps: &[&PrefixMap],
    preserved_source_receipt: Option<AppendReceipt>,
) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                remap_receipts(value, receipt_maps, prefix_maps, preserved_source_receipt);
            }
        }
        serde_json::Value::Object(object) => {
            if object.len() == 2
                && object.contains_key("sequence")
                && object.contains_key("this_hash")
            {
                let receipt: AppendReceipt =
                    serde_json::from_value(serde_json::Value::Object(object.clone())).unwrap();
                if Some(receipt) != preserved_source_receipt {
                    let key = receipt_map_key(receipt);
                    if let Some(rewritten) = receipt_maps.iter().find_map(|map| map.get(&key)) {
                        *value = serde_json::to_value(rewritten).unwrap();
                    }
                }
                return;
            }
            if object.len() == 3
                && object.contains_key("physical_tail")
                && object.contains_key("last_sequence")
                && object.contains_key("last_hash")
            {
                let binding: TailBinding =
                    serde_json::from_value(serde_json::Value::Object(object.clone())).unwrap();
                if let Some(sequence) = binding.last_sequence {
                    let key = (sequence.0, binding.last_hash);
                    if let Some(rewritten) = prefix_maps.iter().find_map(|map| map.get(&key)) {
                        *value = serde_json::to_value(rewritten).unwrap();
                    }
                }
                return;
            }
            for nested in object.values_mut() {
                remap_receipts(nested, receipt_maps, prefix_maps, preserved_source_receipt);
            }
        }
        _ => {}
    }
}

fn rewrite_paper_for_source(
    paper_path: &std::path::Path,
    destination: &std::path::Path,
    source: &RewrittenSource,
    preserved_source_receipt: AppendReceipt,
    decision_evidence_digest: Option<&str>,
) -> (AppendReceipt, ReceiptMap, PrefixMap) {
    let mut writer = Writer::open(destination).unwrap();
    let mut seal_receipt = None;
    let mut receipts = ReceiptMap::new();
    let mut prefixes = PrefixMap::new();
    let mut previous_binding = None;
    for item in Reader::replay(paper_path).unwrap() {
        let (_, envelope) = item.unwrap();
        let original_receipt = AppendReceipt {
            sequence: envelope.seq,
            this_hash: envelope.this_hash,
        };
        let mut record_value: serde_json::Value =
            serde_json::from_slice(&envelope.payload).unwrap();
        remap_receipts(
            &mut record_value,
            &[&source.receipts, &receipts],
            &[&source.prefixes, &prefixes],
            Some(preserved_source_receipt),
        );
        let mut record: PaperLogRecord = serde_json::from_value(record_value).unwrap();
        if let PaperLogRecord::QualificationSealed(seal) = &mut record {
            // `remap_receipts` already rebound `source_prefix` to the rewritten record at the
            // original sealed sequence; the corpus continues past the seal, so the whole-log
            // tail would widen the sealed range.
            seal.financial_prefix = previous_binding.clone().unwrap();
            if let Some(digest) = decision_evidence_digest {
                seal.decision_evidence_digest = digest.to_owned();
            }
        }
        let is_seal = matches!(record, PaperLogRecord::QualificationSealed(_));
        let receipt = writer
            .append_synced(EnvelopeIn {
                source_id: envelope.source_id,
                schema_version: envelope.schema_version,
                parser_version: envelope.parser_version,
                observed_at: envelope.observed_at,
                received_at: envelope.received_at,
                content_type: envelope.content_type,
                payload: serde_json::to_vec(&record).unwrap(),
            })
            .unwrap();
        let binding = TailBinding {
            physical_tail: std::fs::metadata(destination).unwrap().len(),
            last_sequence: Some(receipt.sequence),
            last_hash: receipt.this_hash.to_hex().to_string(),
        };
        receipts.insert(receipt_map_key(original_receipt), receipt);
        prefixes.insert(receipt_map_key(original_receipt), binding.clone());
        previous_binding = Some(binding);
        if is_seal {
            seal_receipt = Some(receipt);
        }
    }
    (seal_receipt.unwrap(), receipts, prefixes)
}

fn rewrite_state_receipts(
    state_path: &std::path::Path,
    source_receipts: &ReceiptMap,
    paper_receipts: &ReceiptMap,
    source_prefixes: &PrefixMap,
    paper_prefixes: &PrefixMap,
    preserved_source_receipt: AppendReceipt,
) {
    let connection = rusqlite::Connection::open(state_path).unwrap();
    let rows = {
        let mut statement = connection
            .prepare(
                "SELECT source_trade_id, frozen_inputs_json, post_commit_inputs_json \
                 FROM decision_pending WHERE state = 'terminal'",
            )
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    };
    for (source_trade_id, frozen, post_commit) in rows {
        let mut frozen: serde_json::Value = serde_json::from_str(&frozen).unwrap();
        let mut post_commit: serde_json::Value = serde_json::from_str(&post_commit).unwrap();
        for value in [&mut frozen, &mut post_commit] {
            remap_receipts(
                value,
                &[source_receipts, paper_receipts],
                &[source_prefixes, paper_prefixes],
                Some(preserved_source_receipt),
            );
        }
        let post_commit_evidence: DecisionPostBoundaryEvidence =
            serde_json::from_value(post_commit).unwrap();
        let post_commit = serde_json::to_value(
            DecisionPostBoundaryEvidence::from_body(post_commit_evidence.body).unwrap(),
        )
        .unwrap();
        connection
            .execute(
                "UPDATE decision_pending SET frozen_inputs_json = ?2, \
                 post_commit_inputs_json = ?3 WHERE source_trade_id = ?1",
                rusqlite::params![
                    source_trade_id,
                    serde_json::to_string(&frozen).unwrap(),
                    serde_json::to_string(&post_commit).unwrap(),
                ],
            )
            .unwrap();
    }
}

/// Snapshot the write-ahead-log state database; a raw file copy drops frames still in the log.
fn clone_state(state_path: &std::path::Path, destination: &std::path::Path) {
    let source = rusqlite::Connection::open_with_flags(
        state_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    source
        .execute("VACUUM INTO ?1", [destination.to_str().unwrap()])
        .unwrap();
}

fn decision_evidence_digest(paper_path: &std::path::Path, state_path: &std::path::Path) -> String {
    let era = paper_era(scan_paper_log(paper_path).unwrap());
    let sealed_source_prefix = era
        .frames
        .iter()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::QualificationSealed(seal)) => {
                Some(seal.source_prefix.clone())
            }
            _ => None,
        })
        .unwrap();
    let state = PaperStateDb::open(state_path).unwrap();
    let mut rows = state.decision_pending_history().unwrap();
    // Every golden trade has one websocket observation preceding its sole activity page.
    // Qualification orders by that earliest receipt, not by SQLite's source-epoch order.
    rows.sort_by_key(|row| {
        let continuation = DecisionContinuationV3::from_durable(row).unwrap();
        let websocket = continuation.observed_source_receipt.unwrap();
        assert_eq!(continuation.page_occurrences.len(), 1);
        assert!(websocket.sequence < continuation.page_occurrences[0].receipt.sequence);
        (
            websocket.sequence,
            row.source_trade_id.0.clone(),
            row.semantic_revision.clone(),
        )
    });
    let keys = rows
        .into_iter()
        .map(|row| (row.source_trade_id, row.semantic_revision))
        .collect::<Vec<_>>();
    let evidence = state
        .seal_decision_evidence_for_source_prefix(&keys, &keys, sealed_source_prefix.last_sequence)
        .unwrap();
    blake3::hash(&evidence).to_hex().to_string()
}

fn assert_qualify_insufficient(
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    live_path: &std::path::Path,
    state_path: &std::path::Path,
    seal_receipt: AppendReceipt,
    output_path: &std::path::Path,
    expected_reason: &str,
) {
    let expected_reason = format!("insufficient qualification evidence: {expected_reason}");
    assert_qualify_reason(
        paper_path,
        source_path,
        live_path,
        state_path,
        seal_receipt,
        output_path,
        &expected_reason,
    );
}

fn assert_qualify_reason(
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    live_path: &std::path::Path,
    state_path: &std::path::Path,
    seal_receipt: AppendReceipt,
    output_path: &std::path::Path,
    expected_reason: &str,
) {
    let output = run_qualify_cli(
        paper_path,
        source_path,
        live_path,
        state_path,
        seal_receipt,
        output_path,
    );
    assert!(
        output.status.success(),
        "pe-service --qualify stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("verdict=InsufficientEvidence"));
    let report: QualificationReport =
        serde_json::from_slice(&std::fs::read(output_path).unwrap()).unwrap();
    assert_eq!(report.verdict, QualificationVerdict::InsufficientEvidence);
    assert!(!report.replay.exact);
    assert_eq!(report.reasons, [expected_reason]);
}

#[allow(clippy::too_many_arguments)]
fn assert_source_preimage_rejected(
    root: &std::path::Path,
    class: &str,
    mutation: PreimageMutation,
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    state_path: &std::path::Path,
    live_path: &std::path::Path,
    target: AppendReceipt,
    expected_reason: &str,
) {
    let case = root.join(format!("{class}-{}", mutation.label()));
    std::fs::create_dir(&case).unwrap();
    let cloned_source = case.join("source.log");
    let cloned_paper = case.join("paper.log");
    let cloned_state = case.join("paper.db");
    let cloned_live = case.join("live_journal.log");
    clone_state(state_path, &cloned_state);
    std::fs::copy(live_path, &cloned_live).unwrap();
    let source = rewrite_source_preimage(source_path, &cloned_source, target, mutation);
    let (_, paper_receipts, paper_prefixes) =
        rewrite_paper_for_source(paper_path, &cloned_paper, &source, target, None);
    rewrite_state_receipts(
        &cloned_state,
        &source.receipts,
        &paper_receipts,
        &source.prefixes,
        &paper_prefixes,
        target,
    );
    let digest = decision_evidence_digest(&cloned_paper, &cloned_state);
    std::fs::remove_file(&cloned_paper).unwrap();
    let (cloned_seal, _, _) =
        rewrite_paper_for_source(paper_path, &cloned_paper, &source, target, Some(&digest));
    assert_qualify_insufficient(
        &cloned_paper,
        &cloned_source,
        &cloned_live,
        &cloned_state,
        cloned_seal,
        &case.join("qualification.json"),
        expected_reason,
    );
}

#[allow(clippy::too_many_arguments)]
fn assert_decision_preimage_rejected(
    root: &std::path::Path,
    mutation: PreimageMutation,
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    state_path: &std::path::Path,
    live_path: &std::path::Path,
    seal_receipt: AppendReceipt,
    source_trade_id: &pe_core_types::SourceTradeId,
) {
    let case = root.join(format!("decision-{}", mutation.label()));
    std::fs::create_dir(&case).unwrap();
    let cloned_source = case.join("source.log");
    let cloned_paper = case.join("paper.log");
    let cloned_state = case.join("paper.db");
    let cloned_live = case.join("live_journal.log");
    std::fs::copy(source_path, &cloned_source).unwrap();
    std::fs::copy(paper_path, &cloned_paper).unwrap();
    clone_state(state_path, &cloned_state);
    std::fs::copy(live_path, &cloned_live).unwrap();
    let connection = rusqlite::Connection::open(&cloned_state).unwrap();
    let expected_reason = match mutation {
        PreimageMutation::Delete => {
            assert_eq!(
                connection
                    .execute(
                        "DELETE FROM decision_pending WHERE source_trade_id = ?1",
                        rusqlite::params![source_trade_id.0],
                    )
                    .unwrap(),
                1
            );
            format!(
                "source-log trade {source_trade_id} has no reconstructable receipt-bearing decision"
            )
        }
        PreimageMutation::Tamper => {
            let frozen: String = connection
                .query_row(
                    "SELECT frozen_inputs_json FROM decision_pending WHERE source_trade_id = ?1",
                    rusqlite::params![source_trade_id.0],
                    |row| row.get(0),
                )
                .unwrap();
            let mut bytes = frozen.into_bytes();
            let marker = br#""this_hash":""#;
            let start = bytes
                .windows(marker.len())
                .position(|window| window == marker)
                .unwrap()
                + marker.len();
            bytes[start] = if bytes[start] == b'a' { b'b' } else { b'a' };
            let tampered = String::from_utf8(bytes).unwrap();
            assert_eq!(
                connection
                    .execute(
                        "UPDATE decision_pending SET frozen_inputs_json = ?2 \
                         WHERE source_trade_id = ?1",
                        rusqlite::params![source_trade_id.0, tampered],
                    )
                    .unwrap(),
                1
            );
            "decision source receipt does not match the sealed source prefix".to_owned()
        }
    };
    drop(connection);
    assert_qualify_insufficient(
        &cloned_paper,
        &cloned_source,
        &cloned_live,
        &cloned_state,
        seal_receipt,
        &case.join("qualification.json"),
        &expected_reason,
    );
}

fn rewrite_live_wrapper_preimage(
    source_path: &std::path::Path,
    destination_path: &std::path::Path,
    mutation: PreimageMutation,
) {
    drop(LiveJournal::open(destination_path).unwrap());
    let mut writer = Writer::open(destination_path).unwrap();
    let mut mutated = false;
    let mut next_sequence = 0u64;
    for item in Reader::replay(source_path).unwrap() {
        let (_, envelope) = item.unwrap();
        let mut event: pe_execution_core::LiveJournalEvent =
            serde_json::from_slice(&envelope.payload).unwrap();
        if matches!(
            event.payload,
            pe_execution_core::LiveJournalPayload::OrderPrepared(_)
        ) {
            assert!(!mutated);
            mutated = true;
            if matches!(mutation, PreimageMutation::Delete) {
                continue;
            }
            let pe_execution_core::LiveJournalPayload::OrderPrepared(wrapper) = &mut event.payload
            else {
                unreachable!()
            };
            let replacement = if wrapper.economic.applied_configuration_hash.starts_with('a') {
                "b"
            } else {
                "a"
            };
            wrapper
                .economic
                .applied_configuration_hash
                .replace_range(..1, replacement);
        }
        event.seq = next_sequence;
        next_sequence += 1;
        writer
            .append_synced(EnvelopeIn {
                source_id: envelope.source_id,
                schema_version: envelope.schema_version,
                parser_version: envelope.parser_version,
                observed_at: envelope.observed_at,
                received_at: envelope.received_at,
                content_type: envelope.content_type,
                payload: serde_json::to_vec(&event).unwrap(),
            })
            .unwrap();
    }
    assert!(mutated);
}

#[allow(clippy::too_many_arguments)]
fn assert_live_wrapper_preimage_rejected(
    root: &std::path::Path,
    mutation: PreimageMutation,
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    state_path: &std::path::Path,
    live_path: &std::path::Path,
    seal_receipt: AppendReceipt,
) {
    let case = root.join(format!("live-wrapper-{}", mutation.label()));
    std::fs::create_dir(&case).unwrap();
    let cloned_source = case.join("source.log");
    let cloned_paper = case.join("paper.log");
    let cloned_state = case.join("paper.db");
    let cloned_live = case.join("live_journal.log");
    std::fs::copy(source_path, &cloned_source).unwrap();
    std::fs::copy(paper_path, &cloned_paper).unwrap();
    clone_state(state_path, &cloned_state);
    rewrite_live_wrapper_preimage(live_path, &cloned_live, mutation);
    let expected_reason = match mutation {
        PreimageMutation::Delete => {
            "event log: I/O error: event log is shorter than the recorded migration boundary"
        }
        PreimageMutation::Tamper => {
            "event log: I/O error: event log does not match the recorded migration boundary"
        }
    };
    assert_qualify_reason(
        &cloned_paper,
        &cloned_source,
        &cloned_live,
        &cloned_state,
        seal_receipt,
        &case.join("qualification.json"),
        expected_reason,
    );
}

fn golden_config_rows(runtime: &RuntimeConfig) -> Vec<ConfigRow> {
    let mut rows = vec![
        (
            "active_watchlist_size",
            runtime.active_watchlist_size.to_string(),
            "integer",
        ),
        ("mode", runtime.mode.clone(), "text"),
        (
            "max_fill_price",
            runtime.max_fill_price.normalize().to_string(),
            "decimal",
        ),
        (
            "min_fill_price",
            runtime.min_fill_price.normalize().to_string(),
            "decimal",
        ),
        (
            "min_resolution_horizon_secs",
            runtime.min_resolution_horizon_secs.to_string(),
            "integer",
        ),
        (
            "max_resolution_horizon_secs",
            runtime.max_resolution_horizon_secs.to_string(),
            "integer",
        ),
        (
            "price_impact_cap_bps",
            runtime.price_impact_cap_bps.to_string(),
            "integer",
        ),
        (
            "flip_human_approved",
            runtime.flip_human_approved.to_string(),
            "bool",
        ),
        (
            "kelly_fraction_above_default_human_approved",
            runtime
                .kelly_fraction_above_default_human_approved
                .to_string(),
            "bool",
        ),
        (
            "per_trade_cap",
            match runtime.per_trade_cap {
                PerTradeCap::ModeDefault => "mode_default".to_owned(),
                PerTradeCap::Unlimited => "unlimited".to_owned(),
                PerTradeCap::Bps(value) => format!("bps:{value}"),
            },
            "text",
        ),
        (
            "slippage_rate",
            runtime.slippage_rate.normalize().to_string(),
            "decimal",
        ),
        (
            "sizing_mode",
            match runtime.sizing_mode {
                SizingMode::Kelly => "kelly",
                SizingMode::Dollar { .. } => "dollar",
                SizingMode::Contract { .. } => "contract",
            }
            .to_owned(),
            "text",
        ),
        (
            "sizing_dollar_usd",
            runtime.sizing_dollar_usd.normalize().to_string(),
            "decimal",
        ),
        (
            "sizing_contracts",
            runtime.sizing_contracts.to_string(),
            "integer",
        ),
    ];
    if let Some(fraction) = runtime.kelly_fraction_override {
        rows.push((
            "kelly_fraction_override",
            fraction.0.normalize().to_string(),
            "decimal",
        ));
    }
    rows.into_iter()
        .map(|(key, value, value_type)| ConfigRow {
            key: key.to_owned(),
            value,
            value_type: value_type.to_owned(),
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn derive_golden_start(
    dir: &std::path::Path,
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    live_path: &std::path::Path,
    state_path: &std::path::Path,
    start_unix: i64,
    wallets: &[WalletAddress],
    runtime: &RuntimeConfig,
) -> FinancialEraPreparation {
    let status_path = dir.join("status.json");
    std::fs::write(
        &status_path,
        br#"{"live":{"pending_dispatch_seeds":0,"ready_dispatch_seeds":0,"stale":false,"accounts":[]}}"#,
    )
    .unwrap();
    let rows_path = dir.join("financial-config.json");
    std::fs::write(
        &rows_path,
        serde_json::to_vec(&golden_config_rows(runtime)).unwrap(),
    )
    .unwrap();
    let manifest = FinancialEraManifest {
        kind: "financial-era-v1".to_owned(),
        state: "prepared".to_owned(),
        activation_id: "golden-stream-v1".to_owned(),
        generation: "golden-stream-v1".to_owned(),
        fresh_bankroll: CollateralAmount::from_decimal_exact(STARTING_BANKROLL).unwrap(),
        target_revision: "1".repeat(40),
        artifact_blake3: "a".repeat(64),
        static_config_hash: "b".repeat(64),
        ranking_batch_id: 545,
        membership: wallets.to_vec(),
        schema_version: 2,
        parser_version: 1,
        financial_semantic_version: FINANCIAL_SEMANTIC_VERSION,
        start_unix,
        paths: FinancialEraPaths {
            paper_log: paper_path.to_path_buf(),
            source_log: source_path.to_path_buf(),
            live_journal: live_path.to_path_buf(),
            paper_state: state_path.to_path_buf(),
        },
        old_artifact_sha256: "d".repeat(64),
        target_artifact_sha256: "e".repeat(64),
        old_config_sha256: "f".repeat(64),
        target_config_sha256: "0".repeat(64),
        old_environment_sha256: "1".repeat(64),
        target_environment_sha256: "2".repeat(64),
        preparation: None,
        stop_invoked: false,
        service_was_active: None,
        backup: None,
        remote_census: None,
        guarded_logs: None,
        start_receipt: None,
        started_unix: None,
    };
    let manifest_path = dir.join("financial-era.json");
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let mut config = pe_service::config::ServiceConfig {
        event_log_path: paper_path.to_path_buf(),
        source_event_log_path: source_path.to_path_buf(),
        ..pe_service::config::ServiceConfig::default()
    };
    config.paper_state_db_path = state_path.to_path_buf();
    config.status_path = status_path;
    config.supabase_authoritative = true;
    config.supabase_url = "https://unused.invalid".to_owned();
    config.supabase_secret_key = "golden-not-a-secret".to_owned();
    let preparation = run_financial_era(
        FinancialEraCommand::Prepare,
        &manifest_path,
        &config,
        Some(&rows_path),
    )
    .unwrap();
    serde_json::from_str(&preparation).unwrap()
}

async fn resolve_golden_trade(
    control_tx: &mpsc::Sender<OrchestratorControl>,
    hooks: &ScenarioHooks,
    bodies: &GoldenTradeBodies,
    resolution_receipt: AppendReceipt,
    expected_shares: ShareAmount,
    expected_credit: CollateralAmount,
    resolution_unix: i64,
) {
    let parsed_resolution =
        pe_source_polymarket_public::parse_clob_market(&bodies.resolution).unwrap();
    assert_eq!(
        parsed_resolution.condition_id.as_deref(),
        Some(bodies.condition.0.as_str())
    );
    let raw_resolution: serde_json::Value = serde_json::from_slice(&bodies.resolution).unwrap();
    let winner = raw_resolution["tokens"]
        .as_array()
        .unwrap()
        .iter()
        .position(|token| token["winner"] == true)
        .unwrap();
    let payout = BinaryPayoutVector::winner(winner).unwrap();
    let binary_payout = BinaryPayout::new(payout.decimals()[0], payout.decimals()[1]).unwrap();
    assert_eq!(
        aggregate_resolution_credit(&[(0, expected_shares)], &binary_payout).unwrap(),
        expected_credit
    );
    hooks
        .financial_clock_unix
        .store(resolution_unix, std::sync::atomic::Ordering::SeqCst);
    let (resolved, resolution_acknowledgement) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::ResolutionCandidate {
            condition: bodies.condition.clone(),
            payout_by_outcome_index_json: payout.canonical_json(),
            receipt: resolution_receipt,
            acknowledged: resolved,
        })
        .await
        .unwrap();
    resolution_acknowledgement.await.unwrap().unwrap();
}

/// I16-GOLDEN-SOURCE-ECONOMIC-V1
///
/// Preconditions: the checked-in v1 corpus seeds one deterministic 30-day future stream; every
/// websocket and complete-page receipt enters the source log before its acknowledged bucket commit.
/// PASS: runtime writes Start, 101 exact fill/resolution pairs, a full-rerank transition,
/// an IntradayDrawdownStop decline whose halt releases mechanically the next day, a
/// held-position PriceConflict decline (neither has a
/// Financial Final), 31 marks, and a Complete seal; the
/// network-free `pe-service --qualify` replay is exact and Pass with identical classifications,
/// admission audits, economic core hashes, risk snapshots/decisions, and exact arithmetic while
/// paper/live wrapper hashes differ.
/// FAIL: any semantic output diverges, the CLI constructs a client, replay is inexact/non-Pass, or
/// a wrapper collision hides its distinct outer protocol.
///
/// I16-GOLDEN-PREIMAGE-V1
///
/// Preconditions: independent clones of the completed corpus delete or one-byte-tamper one
/// admission, book, price, decision-continuation, and resolution preimages, plus the bound live
/// wrapper prefix.
/// PASS: every clone reaches the real `pe-service --qualify` command and returns the exact
/// class-specific `InsufficientEvidence` reason with inexact replay; live mutations fail at the
/// seal's recorded journal boundary before semantic wrapper replay.
/// FAIL: any altered corpus passes, reports another class, or bypasses the offline verifier.
pub(crate) async fn golden_source_stream_replays_exact_economic_core() {
    let scenario_started = Instant::now();
    let start_phase_started = Instant::now();
    assert!(std::path::Path::new(FIXTURE).is_relative());
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let paper_path = dir.path().join("paper.log");
    let live_path = dir.path().join("live_journal.log");
    let state_path = dir.path().join("paper.db");
    drop(Writer::open(&source_path).unwrap());
    drop(Writer::open(&paper_path).unwrap());
    let live_journal = LiveJournal::open(&live_path).unwrap();

    let anchor_cutoff = FIXED_UNIX - FIXED_UNIX.rem_euclid(DAY_SECS);
    let start_unix = anchor_cutoff - 1;
    let mut runtime_config =
        RuntimeConfig::from_service_config(&pe_service::config::ServiceConfig::default());
    runtime_config.mode = "paper".to_owned();
    runtime_config.min_resolution_horizon_secs = 0;
    runtime_config.max_resolution_horizon_secs = 0;
    runtime_config.min_fill_price = dec!(0.15);
    runtime_config.max_fill_price = dec!(0.85);
    runtime_config.price_impact_cap_bps = 300;
    runtime_config.per_trade_cap = PerTradeCap::Bps(1_000);
    runtime_config.slippage_rate = Decimal::ZERO;
    runtime_config.sizing_mode = SizingMode::Contract { contracts: 5 };
    runtime_config.sizing_contracts = 5;
    let applied_configuration_hash = runtime_config.canonical_hash();
    let wallets = (0..QUALIFICATION_DAYS * COPIES_PER_DAY)
        .map(|index| {
            let source_unix = golden_source_unix(anchor_cutoff, index);
            golden_trade_bodies(index, source_unix).wallet
        })
        .collect::<Vec<_>>();

    let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
    for wallet in &wallets {
        paper.set_cursor(wallet, 0).unwrap();
        paper
            .record_reconciled_history_status(&WalletHistoryStatusRecord {
                wallet: *wallet,
                complete: true,
                proof_json: format!("{{\"fixture\":\"golden_stream_v1\",\"wallet\":\"{wallet}\"}}"),
                updated_at_unix: start_unix,
            })
            .unwrap();
    }
    let mut seed_engine =
        BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    let installs = wallets
        .iter()
        .map(|wallet| {
            let captured = ledger_capture(seed_engine.ledger(), &paper, *wallet).unwrap();
            AnchorInstall {
                history_status: None,
                wallet: *wallet,
                balances: Vec::new(),
                cutoff: 0,
                proof: AnchorProof {
                    positions_proof_hash: format!("golden-empty-{wallet}"),
                    activity_bounds_json: "[]".to_owned(),
                    source_log_generation: "golden-stream-v1".to_owned(),
                    document: format!(
                        "{{\"fixture\":\"golden_stream_v1\",\"wallet\":\"{wallet}\"}}"
                    ),
                    recorded_at_unix: start_unix,
                },
                expected: AnchorExpectation {
                    ledger_hash: captured.hash,
                    cursor: captured.cursor,
                    anchor_seq: captured.anchor_seq,
                    coverage_generation: captured.coverage_generation,
                },
            }
        })
        .collect::<Vec<_>>();
    seed_engine.install_anchors(&installs).unwrap();
    let leader_ledger = seed_engine.into_ledger();
    let preparation = derive_golden_start(
        dir.path(),
        &paper_path,
        &source_path,
        &live_path,
        &state_path,
        start_unix,
        &wallets,
        &runtime_config,
    );
    let start = preparation.start;
    assert_eq!(start.hot_config_hash, applied_configuration_hash);
    assert_ne!(start.membership_proofs_hash, "golden-membership-v1");
    let mut paper_writer = Writer::open(&paper_path).unwrap();
    let start_receipt = append_qualification_start(&mut paper_writer, &start, start_unix);
    assert_eq!(start_receipt, preparation.expected_receipt);
    drop(paper_writer);

    paper
        .reset_financial_era(
            start_receipt,
            CollateralAmount::from_decimal_exact(STARTING_BANKROLL).unwrap(),
        )
        .unwrap();
    let authority = GoldenAuthority::new(start_receipt);

    let source_sink = SourceEventSink::open(&source_path).unwrap();
    let source_receipts = SourceReceiptIndex::replay(&source_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(64);
    let (trigger_tx, _trigger_rx) = mpsc::channel(1);
    let coordinator = tokio::spawn(
        ActivityIngest::poll_only(
            source_sink,
            source_rx,
            trigger_tx,
            new_shared_health_with_ws(false, true, 90),
        )
        .with_source_receipt_index(source_receipts.clone())
        .run(),
    );

    let initial_boundary_payload = serde_json::to_vec(&serde_json::json!({
        "kind": "daily_boundary",
        "cutoff_unix": anchor_cutoff,
    }))
    .unwrap();
    let initial_boundary = append_source_at(
        &source_log,
        "pe-service.boundary",
        &initial_boundary_payload,
        anchor_cutoff,
    )
    .await;

    let preimage_append_started = Instant::now();
    let mut recorded_trades = Vec::with_capacity(TOTAL_DECISIONS);
    for index in 0..TOTAL_DECISIONS {
        let source_unix = golden_source_unix(anchor_cutoff, index);
        let bodies = golden_trade_bodies(index, source_unix);
        let websocket_receipt = append_source_at(
            &source_log,
            "polymarket-activity-ws",
            &bodies.websocket,
            source_unix,
        )
        .await;
        let page_receipt = append_source_at(
            &source_log,
            "polymarket-public.activity-reconciliation",
            &bodies.activity,
            source_unix,
        )
        .await;
        let read = support::producer_shaped_read_v2(
            bodies.wallet,
            &bodies.activity,
            source_unix,
            source_unix,
            page_receipt,
        );
        let commitment_receipt = append_source_at(
            &source_log,
            ACTIVITY_READ_COMMITMENT_SOURCE_ID,
            &read.commitment_payload,
            source_unix,
        )
        .await;
        let gamma_receipt = append_source_at(
            &source_log,
            "polymarket.gamma.markets",
            &bodies.gamma,
            source_unix,
        )
        .await;
        let clob_receipt = append_source_at(
            &source_log,
            "polymarket.clob.markets",
            &bodies.clob_long,
            source_unix,
        )
        .await;
        let compact_receipt = append_source_at(
            &source_log,
            "polymarket.clob.compact-market",
            &bodies.compact,
            source_unix,
        )
        .await;
        let book_receipt = append_source_at(
            &source_log,
            "polymarket.clob.book",
            &bodies.book,
            source_unix,
        )
        .await;
        let receipts = AdmissionReceipts {
            gamma: gamma_receipt,
            clob_long: clob_receipt,
            clob_compact: compact_receipt,
        };
        let (admission, book) = admission_and_book(
            &bodies.condition,
            source_unix,
            GoldenAdmissionBodies {
                gamma: &bodies.gamma,
                clob: &bodies.clob_long,
                compact: &bodies.compact,
                book: &bodies.book,
            },
            receipts,
            book_receipt,
        );
        let resolution_receipt = append_source_at(
            &source_log,
            "polymarket.clob.market",
            &bodies.resolution,
            source_unix + 1,
        )
        .await;
        recorded_trades.push(GoldenRecordedTrade {
            bodies,
            websocket_receipt,
            page_receipt,
            commitment_receipt,
            admission,
            book,
            resolution_receipt,
        });
    }
    let preimage_append_elapsed = preimage_append_started.elapsed();

    let hooks = Arc::new(ScenarioHooks::default());
    hooks
        .financial_clock_unix
        .store(anchor_cutoff, std::sync::atomic::Ordering::SeqCst);
    let book_fetcher = Arc::new(GoldenBookFetcher::default());
    let (control_tx, control_rx) = mpsc::channel(4);
    let mut orchestrator = Orchestrator::new_with_authority(
        LiveWatchlist::new(golden_watchlist(&wallets)),
        OrchestratorConfig {
            bankroll: STARTING_BANKROLL,
            mode: ExecutionMode::Paper,

            signal_config: SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: dec!(0.85),
            min_fill_price: dec!(0.15),
            price_impact_cap_bps: 300,
            entry_gate_config: CopyEntryGateConfig,
            runtime_config: None,
            live_accounts: None,
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
        },
        WinnerFollowStrategy::new(runtime_config.winner_follow_config()),
        Writer::open(&paper_path).unwrap(),
        Arc::clone(&paper),
        leader_ledger,
        new_shared_health_with_ws(false, true, 90),
        golden_mid_cache(anchor_cutoff)
            .with_source_log(source_log.clone())
            .with_clock({
                let hooks = hooks.clone();
                Arc::new(move || {
                    OffsetDateTime::from_unix_timestamp(
                        hooks
                            .financial_clock_unix
                            .load(std::sync::atomic::Ordering::SeqCst),
                    )
                    .unwrap()
                })
            }),
        control_rx,
        None,
        authority.clone(),
        Arc::clone(&book_fetcher),
    )
    .unwrap();
    orchestrator.set_scenario_hooks(Arc::clone(&hooks));
    orchestrator
        .configure_financial_log_paths(
            paper_path.clone(),
            source_path.clone(),
            LiveAdmissionBuilder::new(
                reqwest::Client::new(),
                "http://unused.invalid",
                "http://unused.invalid",
                source_log.clone(),
            ),
            Arc::new(HistoricalMarkAdapter::new(
                reqwest::Client::new(),
                "http://unused.invalid",
                source_log.clone(),
            )),
            source_receipts,
        )
        .unwrap();
    let control = tokio::spawn(orchestrator.run(std::future::pending::<()>()));
    let (marked, mark_acknowledgement) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::DailyBoundary {
            cutoff_unix: anchor_cutoff,
            boundary_receipt: initial_boundary,
            acknowledged: marked,
        })
        .await
        .unwrap();
    mark_acknowledgement.await.unwrap().unwrap();

    let expected: serde_json::Value = serde_json::from_slice(fixture("expected")).unwrap();
    let expected_shares = ShareAmount::from_decimal_exact(
        Decimal::from_str_exact(expected["payout_credit"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    let expected_credit = CollateralAmount::from_decimal_exact(
        Decimal::from_str_exact(expected["payout_credit"].as_str().unwrap()).unwrap(),
    )
    .unwrap();
    let mut first_admission = None;
    let live_account_id = AccountId::new("golden-account").unwrap();
    let live_binding = CredentialBindingIdentity {
        version: 1,
        key_id: "golden-key".to_owned(),
    };
    let mut sealed_live_wrapper = None;

    eprintln!(
        "PERF golden_stream phase=start elapsed={:?} total={:?}",
        start_phase_started.elapsed(),
        scenario_started.elapsed()
    );
    let stream_started = Instant::now();
    let mut bucket_commit_elapsed = Duration::ZERO;
    let mut financial_commit_elapsed = Duration::ZERO;
    let mut mark_elapsed = Duration::ZERO;
    let mut first_day_resolutions = Vec::new();
    let mut membership_publication = None;
    let mut conflict_trade_id = None;
    let mut blocked_trade_id = None;
    let mut conflict_price_receipt = None;

    for day in 0..QUALIFICATION_DAYS {
        let mut indices = (day * COPIES_PER_DAY..(day + 1) * COPIES_PER_DAY).collect::<Vec<_>>();
        if day == 1 {
            indices.insert(1, PRICE_CONFLICT_DECISION);
        }
        if day == LOSS_DAY {
            indices.extend(FIRST_LOSS_COPY..TOTAL_DECISIONS);
        }
        for index in indices {
            let source_unix = golden_source_unix(anchor_cutoff, index);
            let recorded = &recorded_trades[index];
            let bodies = &recorded.bodies;
            hooks
                .admission_artifacts
                .lock()
                .unwrap()
                .push_back(recorded.admission.clone());
            let mut fixed_book = recorded.book.clone();
            fixed_book.fetched_at_ms = u64::try_from(source_unix * 1_000).unwrap();
            book_fetcher.insert(
                recorded.admission.market.ordered_outcome_token_ids[0].to_string(),
                fixed_book,
            );
            hooks
                .financial_clock_unix
                .store(source_unix, std::sync::atomic::Ordering::SeqCst);
            hooks.age_clock.lock().unwrap().clear();
            hooks.age_clock.lock().unwrap().extend(std::iter::repeat_n(
                OffsetDateTime::from_unix_timestamp(source_unix).unwrap(),
                3,
            ));

            let mut read = support::producer_shaped_read_v2(
                bodies.wallet,
                &bodies.activity,
                source_unix,
                source_unix,
                recorded.page_receipt,
            );
            assert_eq!(read.aggregates.len(), 1);
            let aggregate = read.aggregates.remove(0);
            let source_trade_id = aggregate.group_id.key().clone();
            if index == 0 {
                first_admission = Some(recorded.admission.clone());
            }
            let context = BucketDecisionContext {
                applied_configuration: runtime_config.clone(),
                decision_inputs_json: read.decision_inputs_json,
                page_occurrences: vec![read.page],
                observed_source_receipts: HashMap::from([(
                    source_trade_id.clone(),
                    recorded.websocket_receipt,
                )]),
                reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
                read_commitment: Some(
                    pe_service::bucket_commit::ActivityReadCommitmentReceipt::BindingsV2(
                        recorded.commitment_receipt,
                    ),
                ),

                signal_config: Default::default(),
                copy_eligible: true,
                bracket_commit: false,
                recorded_at_unix: source_unix,
                observation_provenance: HashMap::from([(
                    source_trade_id.clone(),
                    TradeProvenance::ActivityWs,
                )]),
                no_copy_dispositions: HashMap::new(),
                identity_overrides: HashMap::new(),
                identity_unresolved: Default::default(),
                history_status: Some(WalletHistoryStatusRecord {
                    wallet: bodies.wallet,
                    complete: true,
                    proof_json: format!("{{\"fixture\":\"golden_stream_v1\",\"ordinal\":{index}}}"),
                    updated_at_unix: source_unix,
                }),
            };
            let (committed, acknowledgement) = oneshot::channel();
            let bucket_commit_started = Instant::now();
            let financial_commit_started = Instant::now();
            control_tx
                .send(OrchestratorControl::CommitActivityBucket {
                    aggregates: vec![aggregate],
                    context: Arc::new(context),
                    committed,
                })
                .await
                .unwrap();
            let result = acknowledgement.await.unwrap().unwrap();
            bucket_commit_elapsed += bucket_commit_started.elapsed();
            assert_eq!(result.dispositions[&source_trade_id.0], "decision_pending");
            assert!(!paper.is_decision_pending_open(&source_trade_id).unwrap());
            if index == PRICE_CONFLICT_DECISION || index == BLOCKED_DECISION {
                let row = paper
                    .decision_pending_for(&source_trade_id)
                    .unwrap()
                    .unwrap();
                let replayed = replay_decision_pending(&row).unwrap();
                let terminal = &replayed.post_boundary.body.terminal;
                assert!(terminal.final_receipt.is_none());
                assert!(terminal.fill.is_none());
                let decline = terminal.decline.as_ref().unwrap();
                if index == PRICE_CONFLICT_DECISION {
                    assert_eq!(
                        decline.outcome,
                        WinnerFollowDeclineAudit::RiskInputsUnavailable
                    );
                    let WinnerFollowDecisionInputs::RiskInputsUnavailable { cause, evidence } =
                        &decline.inputs
                    else {
                        panic!("price conflict did not record its risk acquisition failure");
                    };
                    assert_eq!(*cause, RiskInputsUnavailable::PriceConflict);
                    assert_eq!(evidence.price_receipts.len(), 1);
                    conflict_price_receipt = Some(evidence.price_receipts[0]);
                    conflict_trade_id = Some(source_trade_id);
                    let held = &recorded_trades[HELD_COPY];
                    assert_ne!(held.bodies.condition, bodies.condition);
                    let open = paper.financial_snapshot(source_unix).unwrap().positions;
                    assert_eq!(open.len(), 1);
                    assert_eq!(open[0].market_id.to_string(), held.bodies.condition.0);
                    resolve_golden_trade(
                        &control_tx,
                        &hooks,
                        &held.bodies,
                        held.resolution_receipt,
                        expected_shares,
                        expected_credit,
                        source_unix + 1,
                    )
                    .await;
                } else {
                    assert_eq!(
                        decline.outcome,
                        WinnerFollowDeclineAudit::Blocked(RiskBlock::IntradayDrawdownStop)
                    );
                    let WinnerFollowDecisionInputs::Evaluated { economic } = &decline.inputs else {
                        panic!("drawdown decline did not record evaluated economics");
                    };
                    assert_eq!(
                        economic.risk.decision,
                        RiskDecisionAudit::Blocked {
                            reason: RiskBlock::IntradayDrawdownStop
                        }
                    );
                    assert_eq!(economic.risk.snapshot.intraday_pnl_bps, BasisPoints(-206));
                    assert_eq!(economic.all_in_debit().unwrap().to_decimal(), dec!(2.55));
                    blocked_trade_id = Some(source_trade_id);
                }
                assert!(
                    paper
                        .financial_snapshot(source_unix + 1)
                        .unwrap()
                        .positions
                        .is_empty()
                );
                financial_commit_elapsed += financial_commit_started.elapsed();
                continue;
            }
            let after_fill = paper.financial_snapshot(source_unix).unwrap();
            assert!(
                after_fill
                    .positions
                    .iter()
                    .any(|position| position.long == expected_shares),
                "golden fill index {index}"
            );

            if index == COPIES_PER_DAY - 1 {
                first_day_resolutions.push(index);
            } else if index != HELD_COPY {
                let resolution_unix = source_unix + 1;
                resolve_golden_trade(
                    &control_tx,
                    &hooks,
                    bodies,
                    recorded.resolution_receipt,
                    expected_shares,
                    if (FIRST_LOSS_COPY..BLOCKED_DECISION).contains(&index) {
                        CollateralAmount::ZERO
                    } else {
                        expected_credit
                    },
                    resolution_unix,
                )
                .await;
                assert!(
                    paper
                        .financial_snapshot(resolution_unix)
                        .unwrap()
                        .positions
                        .is_empty()
                );
            }
            financial_commit_elapsed += financial_commit_started.elapsed();
        }

        let mark_started = Instant::now();
        let cutoff = anchor_cutoff + i64::try_from(day + 1).unwrap() * DAY_SECS;
        let boundary_payload = serde_json::to_vec(&serde_json::json!({
            "kind": "daily_boundary",
            "cutoff_unix": cutoff,
        }))
        .unwrap();
        let boundary_receipt = append_source_at(
            &source_log,
            "pe-service.boundary",
            &boundary_payload,
            cutoff,
        )
        .await;
        let snapshot = paper.financial_snapshot(cutoff).unwrap();
        if day == 0 {
            assert!(!snapshot.positions.is_empty());
            let mut prices: serde_json::Value =
                serde_json::from_slice(fixture("prices_history")).unwrap();
            prices["history"][0]["t"] = (cutoff - 60).into();
            prices["history"][1]["t"] = cutoff.into();
            let price_body = serde_json::to_vec(&prices).unwrap();
            for _ in &snapshot.positions {
                let receipt = append_source_at(
                    &source_log,
                    "pe-service.clob-prices-history",
                    &price_body,
                    cutoff,
                )
                .await;
                hooks.boundary_mark_prices.lock().unwrap().push_back(
                    pe_service::risk_inputs::HistoricalMarkPrice {
                        price: Price::new(dec!(0.50)).unwrap(),
                        sample_unix: cutoff,
                        receipt,
                    },
                );
            }
        } else {
            assert!(snapshot.positions.is_empty());
        }
        hooks
            .financial_clock_unix
            .store(cutoff, std::sync::atomic::Ordering::SeqCst);
        if day + 1 == QUALIFICATION_DAYS {
            let first_prepared_record = paper_era(scan_paper_log(&paper_path).unwrap())
                .frames
                .iter()
                .find_map(|frame| match &frame.frame {
                    PaperLogFrame::Record(
                        record @ PaperLogRecord::FinancialPrepared {
                            payload: pe_service::paper_recovery::FinancialPayload::Fill { .. },
                            ..
                        },
                    ) => Some(record.clone()),
                    _ => None,
                })
                .unwrap();
            let PaperLogRecord::FinancialPrepared {
                payload:
                    pe_service::paper_recovery::FinancialPayload::Fill {
                        operation: first_operation,
                        economic: first_economic,
                    },
                ..
            } = &first_prepared_record
            else {
                panic!("first production Prepared record was not a fill");
            };
            let first_admission = first_admission.as_ref().unwrap();
            let token_id = first_economic.market.token_id.clone();
            let live_ladder = ladder_from_economic(first_economic);
            let live_economic = EconomicPrepared::compose(EconomicInputs {
                market: first_economic.market.clone(),
                admission: first_admission,
                plan: &live_ladder,
                book_receipt: first_economic.book_receipt,
                observation: first_economic.observation.clone(),
                sizing_mode: first_economic.sizing.mode,
                budget: first_economic.sizing.budget,
                slippage_rate: first_economic.sizing.slippage_rate,
                risk: first_economic.risk.clone(),
                cash_before: first_economic.balance.cash_before,
                price_impact_cap_bps: first_economic.balance.price_impact_cap_bps,
                chase_ceiling: first_economic.balance.chase_ceiling,
                band_floor: first_economic.balance.band_floor,
                band_ceiling_exclusive: first_economic.balance.band_ceiling_exclusive,
                applied_configuration_hash: first_economic.applied_configuration_hash.clone(),
            })
            .unwrap();
            assert!(live_economic.observation.is_some());
            assert_eq!(
                live_economic.core_hash().unwrap(),
                first_economic.core_hash().unwrap()
            );
            let live_dispatch_id = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                &first_operation.leader_wallet.to_string(),
                &first_operation.source_trade_id.0,
                &first_economic.market.market_id,
                u16::from(first_economic.market.outcome_index),
                first_economic.market.side,
                first_operation.observed_at_bucket,
            );
            let live_request = LiveOrderRequest {
                target: FrozenLiveTarget {
                    account_id: live_account_id.clone(),
                    credential_binding: live_binding.clone(),
                },
                current_credential_binding: live_binding.clone(),
                mode: LiveModeSnapshot {
                    requested: LiveControlMode::LiveTiny,
                    effective: LiveControlMode::LiveTiny,
                },
                identity: LiveOrderIdentity {
                    dispatch_id: live_dispatch_id.clone(),
                    idempotency_key: LiveOrderIdentity::idempotency_key_for(
                        &live_dispatch_id,
                        &live_account_id,
                    ),
                    quote_id: "golden-quote".to_owned(),
                    config_hash: first_economic.applied_configuration_hash.clone(),
                    decision_hash: "golden-decision".to_owned(),
                    evidence_hashes: vec![
                        first_economic
                            .admission
                            .settlement
                            .raw_evidence_hash
                            .clone(),
                    ],
                    fill_projection: Some(Box::new(
                        pe_execution_core::LiveFillProjectionIdentity {
                            leader_wallet: first_operation.leader_wallet.to_string(),
                            source_trade_id: Some(first_operation.source_trade_id.0.clone()),
                            market_id: first_economic.market.market_id.clone(),
                            outcome_id: i64::from(u16::from(first_economic.market.outcome_index)),
                            side: "buy".to_owned(),
                        },
                    )),
                    schema_version: 1,
                    parser_version: 1,
                },
                condition_id: first_economic.market.condition_id.clone(),
                outcome_id: OutcomeId(u16::from(first_economic.market.outcome_index)),
                token_id,
                admission: first_admission.clone(),
                ladder: live_ladder,
                economic: live_economic,
            };
            let live_now =
                OffsetDateTime::from_unix_timestamp(first_admission.market.observed_at_unix)
                    .unwrap();
            let live_venue = GoldenLiveVenue;
            let live_executor = LiveExecutor::new(&live_venue, &live_journal);
            let live_prepared = match live_executor
                .prepare_with_clock(live_request, move || live_now)
                .await
                .unwrap()
            {
                LivePrepareResult::Prepared(prepared) => prepared,
                LivePrepareResult::Terminal(outcome) => {
                    panic!("golden live wrapper preparation terminated: {outcome:?}")
                }
            };
            let live_audit: &LiveOrderPreparedAudit = live_prepared.audit();
            assert_eq!(live_audit.economic, *first_economic);
            assert_eq!(
                live_audit.economic.core_hash().unwrap(),
                first_economic.core_hash().unwrap()
            );
            sealed_live_wrapper = Some(serde_json::to_vec(live_audit).unwrap());
        }
        let (marked, mark_acknowledgement) = oneshot::channel();
        control_tx
            .send(OrchestratorControl::DailyBoundary {
                cutoff_unix: cutoff,
                boundary_receipt,
                acknowledged: marked,
            })
            .await
            .unwrap();
        mark_acknowledgement.await.unwrap().unwrap();
        mark_elapsed += mark_started.elapsed();

        if day == 0 {
            let resolution_started = Instant::now();
            for (offset, bodies) in first_day_resolutions.drain(..).enumerate() {
                let recorded = &recorded_trades[bodies];
                resolve_golden_trade(
                    &control_tx,
                    &hooks,
                    &recorded.bodies,
                    recorded.resolution_receipt,
                    expected_shares,
                    expected_credit,
                    cutoff + 1 + i64::try_from(offset).unwrap(),
                )
                .await;
            }
            assert!(
                paper
                    .financial_snapshot(cutoff + i64::try_from(COPIES_PER_DAY).unwrap())
                    .unwrap()
                    .positions
                    .is_empty()
            );
            financial_commit_elapsed += resolution_started.elapsed();
        }
        if day == 1 {
            assert!(paper.open_decision_pending().unwrap().is_empty());
            assert!(
                paper
                    .financial_snapshot(cutoff)
                    .unwrap()
                    .positions
                    .is_empty()
            );
            let replacements = golden_watchlist(&wallets)
                .entries
                .into_iter()
                .filter(|entry| entry.wallet != wallets[1])
                .collect::<Vec<_>>();
            // These artifact/evidence types and their source constant are crate-private;
            // use their exact serialized producer contract at this integration boundary.
            let ranking_receipt = append_source_at(
                &source_log,
                "pe-service.watchlist-ranking",
                &serde_json::to_vec(&serde_json::json!({"batch_id": 565, "entries": replacements}))
                    .unwrap(),
                cutoff,
            )
            .await;
            let change = MembershipChange {
                reason: MembershipReason::FullRerank,
                removed: vec![wallets[1]],
                added: Vec::new(),
                capacity: runtime_config.active_watchlist_size,
                ranking_batch_id: Some(565),
                evidence: serde_json::json!({"kind": "full_rerank", "ranking_receipt": ranking_receipt, "admission_receipts": []}),
            };
            let (acknowledged, acknowledgement) = oneshot::channel();
            control_tx
                .send(OrchestratorControl::PublishMembership {
                    change: change.clone(),
                    replacements,
                    checks: Default::default(),
                    acknowledged,
                })
                .await
                .unwrap();
            let receipt = acknowledgement.await.unwrap().unwrap();
            membership_publication = Some((receipt, change));
        }
    }

    eprintln!(
        "PERF golden_stream phase=stream wall={:?} source_append={:?} bucket_commit={:?} financial_commit={:?} marks={:?} total={:?}",
        stream_started.elapsed(),
        preimage_append_elapsed,
        bucket_commit_elapsed,
        financial_commit_elapsed,
        mark_elapsed,
        scenario_started.elapsed()
    );

    let seal_started = Instant::now();
    drop(control_tx);
    control.await.unwrap();
    drop(source_log);
    coordinator.await.unwrap();
    assert!(paper.open_decision_pending().unwrap().is_empty());
    let sealed_era = paper_era(scan_paper_log(&paper_path).unwrap());
    let completion = qualification_completion(&sealed_era).unwrap();
    assert_eq!(completion.complete_days, QUALIFICATION_DAYS);
    assert_eq!(completion.causal_closes, TOTAL_FILLS);
    let halt_transitions = sealed_era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::RiskHaltChanged {
                owner,
                cause,
                state,
                ..
            }) => Some((owner.clone(), *cause, *state)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        halt_transitions,
        vec![
            (
                RiskHaltOwner::Paper,
                RiskHaltCause::IntradayDrawdown,
                HaltState::Engaged
            ),
            (
                RiskHaltOwner::Paper,
                RiskHaltCause::IntradayDrawdown,
                HaltState::Released
            ),
        ]
    );

    let (membership_receipt, membership_change) = membership_publication.unwrap();
    let membership_frames = sealed_era
        .frames
        .iter()
        .filter(|frame| {
            matches!(
                frame.frame,
                PaperLogFrame::Record(PaperLogRecord::MembershipChanged { .. })
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(membership_frames.len(), 1);
    assert_eq!(membership_frames[0].receipt, membership_receipt);
    let PaperLogFrame::Record(recorded_membership) = &membership_frames[0].frame else {
        panic!("membership frame was not a typed record");
    };
    assert_eq!(*recorded_membership, membership_change.into_record());
    let price_receipt = conflict_price_receipt.unwrap();
    let (_, acquisition) = Reader::replay(&source_path)
        .unwrap()
        .map(Result::unwrap)
        .find(|(_, envelope)| envelope.seq == price_receipt.sequence)
        .unwrap();
    assert_eq!(acquisition.this_hash, price_receipt.this_hash);
    assert_eq!(acquisition.source_id.0, "polymarket.gamma.markets");
    assert_eq!(
        (acquisition.schema_version, acquisition.parser_version),
        (2, 1)
    );
    let acquisition: serde_json::Value = serde_json::from_slice(&acquisition.payload).unwrap();
    assert_eq!(acquisition["result"], "page");
    assert_eq!(acquisition["usable"], true);
    let raw: Vec<u8> = serde_json::from_value(acquisition["payload"].clone()).unwrap();
    let rows: Vec<serde_json::Value> = serde_json::from_slice(&raw).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["conditionId"],
        recorded_trades[HELD_COPY].bodies.condition.0
    );
    assert_eq!(rows[0]["conditionId"], rows[1]["conditionId"]);
    assert_eq!(rows[0]["outcomePrices"], "[\"0.50\",\"0.50\"]");
    assert_eq!(rows[1]["outcomePrices"], "[\"0.60\",\"0.40\"]");

    let decision_rows = paper.decision_pending_history().unwrap();
    assert_eq!(decision_rows.len(), TOTAL_DECISIONS);
    assert!(decision_rows.iter().all(|row| {
        replay_decision_pending(row).is_ok_and(|decision| {
            decision.continuation.facts.gate_result == "admitted"
                && decision.continuation.version() == 5
                && decision.continuation.read_commitment.is_some()
                && decision.continuation.facts.provenance == TradeProvenance::ActivityWs
                && decision.post_boundary.body.terminal.final_receipt.is_some()
                    == (Some(&row.source_trade_id) != conflict_trade_id.as_ref()
                        && Some(&row.source_trade_id) != blocked_trade_id.as_ref())
        })
    }));
    let prepared_fills = sealed_era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(
                record @ PaperLogRecord::FinancialPrepared {
                    payload: pe_service::paper_recovery::FinancialPayload::Fill { economic, .. },
                    ..
                },
            ) => Some((record, economic)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(prepared_fills.len(), TOTAL_FILLS);
    assert_eq!(paper.settled_count().unwrap(), TOTAL_FILLS);
    for (record, _) in &prepared_fills {
        let PaperLogRecord::FinancialPrepared {
            payload: pe_service::paper_recovery::FinancialPayload::Fill { operation, .. },
            ..
        } = record
        else {
            panic!("prepared fill had a different payload");
        };
        assert_ne!(Some(&operation.source_trade_id), conflict_trade_id.as_ref());
        assert_ne!(Some(&operation.source_trade_id), blocked_trade_id.as_ref());
    }
    for (_, economic) in &prepared_fills {
        assert!(economic.observation.is_some());
        assert_eq!(
            economic.applied_configuration_hash,
            applied_configuration_hash
        );
        assert_eq!(economic.risk.decision, RiskDecisionAudit::Approved);
        for (field, actual) in [
            (
                "minimum_shares",
                economic.sizing.minimum_shares.to_decimal(),
            ),
            ("principal", economic.sizing.principal.to_decimal()),
            ("expected_fee", economic.fee.expected_fee.to_decimal()),
            ("fee_reserve", economic.fee.reserve.to_decimal()),
            ("all_in_price", economic.sizing.all_in_price.0),
            (
                "all_in_debit",
                economic.all_in_debit().unwrap().to_decimal(),
            ),
        ] {
            assert_eq!(
                actual.normalize().to_string(),
                expected[field].as_str().unwrap(),
                "golden exact arithmetic field {field}"
            );
        }
    }
    let runtime_core_hashes = prepared_fills
        .iter()
        .map(|(_, economic)| economic.core_hash().unwrap())
        .collect::<Vec<_>>();
    let runtime_risks = prepared_fills
        .iter()
        .map(|(_, economic)| economic.risk.clone())
        .collect::<Vec<_>>();
    let first_prepared_record = prepared_fills[0].0.clone();
    let sealed_cutoff = anchor_cutoff + i64::try_from(QUALIFICATION_DAYS).unwrap() * DAY_SECS;
    let (seal_receipt, seal) = sealed_era
        .frames
        .iter()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::QualificationSealed(seal)) => {
                Some((frame.receipt, seal.as_ref()))
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(seal.start_receipt, start_receipt);
    assert_eq!(seal.sealed_cutoff_unix, sealed_cutoff);
    assert_eq!(seal.reason, SealReason::Complete);
    assert_eq!(
        seal.live_prefix,
        TailBinding::from(&LiveJournal::verified_tail(&live_path).unwrap())
    );
    assert_eq!(
        sealed_era
            .frames
            .iter()
            .filter(|frame| matches!(
                frame.frame,
                PaperLogFrame::Record(PaperLogRecord::PortfolioMark(_))
            ))
            .count(),
        QUALIFICATION_DAYS + 1
    );
    assert_eq!(authority.mutations(), TOTAL_FILLS * 2);
    eprintln!(
        "PERF golden_stream phase=seal elapsed={:?} total={:?}",
        seal_started.elapsed(),
        scenario_started.elapsed()
    );

    let output_path = dir.path().join("qualification.json");
    let qualify_started = Instant::now();
    let output = run_qualify_cli(
        &paper_path,
        &source_path,
        &live_path,
        &state_path,
        seal_receipt,
        &output_path,
    );
    eprintln!(
        "PERF golden_stream phase=qualify elapsed={:?} total={:?}",
        qualify_started.elapsed(),
        scenario_started.elapsed()
    );
    assert!(
        output.status.success(),
        "pe-service --qualify stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report_bytes = std::fs::read(&output_path).unwrap();
    let report: QualificationReport = serde_json::from_slice(&report_bytes).unwrap();
    assert_eq!(
        report.verdict,
        QualificationVerdict::Pass,
        "pe-service --qualify stdout: {}; reasons: {:?}",
        String::from_utf8_lossy(&output.stdout),
        report.reasons
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("verdict=Pass"));
    assert!(report.replay.exact);
    assert_eq!(report.replay.decisions, TOTAL_DECISIONS);
    assert_eq!(report.replay.financial_prepared, TOTAL_FILLS * 2);
    assert_eq!(report.replay.financial_final, TOTAL_FILLS * 2);
    assert_eq!(report.replay.fills, TOTAL_FILLS);
    assert_eq!(report.replay.no_fills, 2);
    assert_eq!(report.replay.membership_changes, 1);
    assert_eq!(report.replay.final_membership_count, wallets.len() - 1);
    assert_eq!(report.demotions, 0);
    assert_eq!(report.promotion_anchor_mark_unix, Some(anchor_cutoff));
    assert_eq!(report.absolute_profit_loss, Some(dec!(196.05)));
    assert_eq!(report.complete_days, QUALIFICATION_DAYS);
    assert_eq!(report.closed_copies, TOTAL_FILLS);
    assert_eq!(report.paper_p95_delay_ms, Some(0));
    assert_eq!(
        report.evidence.economic_core_hashes,
        runtime_core_hashes
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(runtime_risks.len(), TOTAL_FILLS);
    assert!(
        runtime_risks
            .iter()
            .all(|risk| risk.decision == RiskDecisionAudit::Approved)
    );

    let PaperLogRecord::FinancialPrepared {
        payload:
            pe_service::paper_recovery::FinancialPayload::Fill {
                operation: first_operation,
                economic: first_economic,
            },
        ..
    } = &first_prepared_record
    else {
        panic!("first production Prepared record was not a fill");
    };
    let paper_wrapper = serde_json::to_vec(&first_prepared_record).unwrap();
    let live_wrapper = sealed_live_wrapper.unwrap();
    assert_eq!(
        blake3::hash(&paper_wrapper) != blake3::hash(&live_wrapper),
        expected["wrapper_hashes_differ"].as_bool().unwrap()
    );
    live_journal
        .append(
            live_account_id.clone(),
            OffsetDateTime::from_unix_timestamp(sealed_cutoff + 1).unwrap(),
            pe_execution_core::LiveJournalPayload::CredentialBindingMismatch {
                frozen: live_binding.clone(),
                current: live_binding,
            },
        )
        .unwrap();
    let output = run_qualify_cli(
        &paper_path,
        &source_path,
        &live_path,
        &state_path,
        seal_receipt,
        &output_path,
    );
    assert!(
        output.status.success(),
        "pe-service --qualify stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let repeated_report_bytes = std::fs::read(&output_path).unwrap();
    assert_eq!(repeated_report_bytes, report_bytes);
    let report: QualificationReport = serde_json::from_slice(&repeated_report_bytes).unwrap();
    assert_eq!(report.verdict, QualificationVerdict::Pass);
    assert_eq!(report.evidence.live_wrapper_facts.len(), 1);
    let live_fact = report.evidence.live_wrapper_facts.first().unwrap();
    assert_eq!(live_fact.account_id, "golden-account");
    assert_eq!(
        live_fact.source_trade_id,
        first_operation.source_trade_id.0.as_str()
    );
    assert_eq!(
        live_fact.economic_core_hash,
        first_economic.core_hash().unwrap()
    );
    assert_eq!(
        live_fact.paper_wrapper_hash,
        blake3::hash(&paper_wrapper).to_hex().to_string()
    );
    assert_eq!(
        live_fact.live_wrapper_hash,
        blake3::hash(&live_wrapper).to_hex().to_string()
    );
    assert_ne!(live_fact.paper_wrapper_hash, live_fact.live_wrapper_hash);

    let tamper_started = Instant::now();
    let tamper_root = dir.path().join("preimage-corpora");
    std::fs::create_dir(&tamper_root).unwrap();
    let last_recorded = &recorded_trades[ORIGINAL_COPIES - 1];
    let last_decision = decision_rows.last().unwrap();
    qualification_replays_source_age_and_seal_binds_policy_and_clock(
        &tamper_root,
        &paper_path,
        &source_path,
        &state_path,
        &live_path,
        seal_receipt,
        &first_operation.source_trade_id,
    );
    let price_receipt = sealed_era
        .frames
        .iter()
        .find_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => {
                mark.prices.first().and_then(|price| price.receipt)
            }
            _ => None,
        })
        .unwrap();
    for mutation in [PreimageMutation::Delete, PreimageMutation::Tamper] {
        assert_source_preimage_rejected(
            &tamper_root,
            "admission",
            mutation,
            &paper_path,
            &source_path,
            &state_path,
            &live_path,
            last_recorded.admission.receipts.gamma,
            "polymarket.gamma.markets receipt is absent from the sealed source prefix",
        );
        assert_source_preimage_rejected(
            &tamper_root,
            "book",
            mutation,
            &paper_path,
            &source_path,
            &state_path,
            &live_path,
            last_recorded.book.source_receipt.unwrap(),
            "polymarket.clob.book receipt is absent from the sealed source prefix",
        );
        assert_source_preimage_rejected(
            &tamper_root,
            "price",
            mutation,
            &paper_path,
            &source_path,
            &state_path,
            &live_path,
            price_receipt,
            "PortfolioMark receipt is absent from sealed source prefix",
        );
        assert_source_preimage_rejected(
            &tamper_root,
            "resolution",
            mutation,
            &paper_path,
            &source_path,
            &state_path,
            &live_path,
            last_recorded.resolution_receipt,
            "resolution receipt is absent from the sealed source prefix",
        );
        assert_decision_preimage_rejected(
            &tamper_root,
            mutation,
            &paper_path,
            &source_path,
            &state_path,
            &live_path,
            seal_receipt,
            &last_decision.source_trade_id,
        );
        assert_live_wrapper_preimage_rejected(
            &tamper_root,
            mutation,
            &paper_path,
            &source_path,
            &state_path,
            &live_path,
            seal_receipt,
        );
    }

    assert!(!fixture("prices_history").is_empty());
    assert_eq!(LIVE_MARKET_SCHEMA_VERSION, 1);
    assert_eq!(LIVE_MARKET_PARSER_VERSION, 1);
    assert_eq!(CLOB_RESOLUTION_PARSER_VERSION, 2);
    eprintln!(
        "PERF golden_stream phase=tamper elapsed={:?} total={:?}",
        tamper_started.elapsed(),
        scenario_started.elapsed()
    );

    println!(
        "PASS: I16-GOLDEN-SOURCE-ECONOMIC-V1 — {} exact decisions, {} exact fills, {} complete days",
        report.replay.decisions, report.replay.fills, report.complete_days
    );
    println!(
        "PASS: I16-GOLDEN-PREIMAGE-V1 — deleted and one-byte-tampered preimages fail with exact typed reasons"
    );
}

/// Reuse the golden financial owners for the causal bracket's short continuation scenarios.
#[allow(dead_code)]
pub(crate) struct BracketFinancialHarness {
    pub(crate) control: mpsc::Sender<OrchestratorControl>,
    pub(crate) source: SourceLogHandle,
    pub(crate) live: LiveWatchlist,
    pub(crate) writer_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) paper_path: std::path::PathBuf,
    pub(crate) source_path: std::path::PathBuf,
    pub(crate) initial: Watchlist,
    runtime: RuntimeConfig,
    receipts: SourceReceiptIndex,
    accounts: pe_service::live_accounts::LiveAccounts,
    price_gate: Arc<support::PriceGate>,
    hooks: Arc<ScenarioHooks>,
    books: Arc<GoldenBookFetcher>,
    authority: GoldenAuthority,
    terminal_at: OffsetDateTime,
    actor: tokio::task::JoinHandle<()>,
    coordinator: tokio::task::JoinHandle<()>,
    _triggers: mpsc::Receiver<pe_service::activity_ingest::ReconciliationTrigger>,
}

#[allow(dead_code)]
impl BracketFinancialHarness {
    pub(crate) async fn new(
        dir: &std::path::Path,
        paper: Arc<PaperStateDb>,
        initial_wallets: &[WalletAddress],
    ) -> Self {
        Self::new_with_deployed_flow(dir, paper, initial_wallets, false).await
    }

    async fn new_with_deployed_flow(
        dir: &std::path::Path,
        paper: Arc<PaperStateDb>,
        initial_wallets: &[WalletAddress],
        deployed: bool,
    ) -> Self {
        let start_unix = if deployed {
            FIXED_UNIX - FIXED_UNIX.rem_euclid(DAY_SECS) - 1
        } else {
            1
        };
        let paper_path = dir.join("paper.log");
        let source_path = dir.join("source.log");
        let live_path = dir.join("live_journal.log");
        drop(Writer::open(&paper_path).unwrap());
        drop(Writer::open(&source_path).unwrap());
        drop(LiveJournal::open(&live_path).unwrap());
        let mut runtime =
            RuntimeConfig::from_service_config(&pe_service::config::ServiceConfig::default());
        runtime.mode = "paper".to_owned();
        runtime.min_resolution_horizon_secs = 0;
        runtime.max_resolution_horizon_secs = 0;
        runtime.price_impact_cap_bps = 300;
        runtime.per_trade_cap = PerTradeCap::Bps(1_000);
        runtime.slippage_rate = Decimal::ZERO;
        runtime.sizing_mode = SizingMode::Contract { contracts: 5 };
        runtime.sizing_contracts = 5;
        let preparation = derive_golden_start(
            dir,
            &paper_path,
            &source_path,
            &live_path,
            &dir.join("paper.db"),
            start_unix,
            initial_wallets,
            &runtime,
        );
        let mut writer = Writer::open(&paper_path).unwrap();
        let start = append_qualification_start(&mut writer, &preparation.start, start_unix);
        drop(writer);
        paper
            .reset_financial_era(
                start,
                CollateralAmount::from_decimal_exact(STARTING_BANKROLL).unwrap(),
            )
            .unwrap();
        let authority = GoldenAuthority::new(start);
        let receipts = SourceReceiptIndex::replay(&source_path).unwrap();
        let (source, source_rx) = SourceLogHandle::channel(64);
        let (trigger_tx, trigger_rx) = mpsc::channel(1);
        let coordinator = tokio::spawn(
            ActivityIngest::poll_only(
                SourceEventSink::open(&source_path).unwrap(),
                source_rx,
                trigger_tx,
                new_shared_health_with_ws(false, true, 90),
            )
            .with_source_receipt_index(receipts.clone())
            .run(),
        );
        let accounts = pe_service::live_accounts::LiveAccounts::new(Default::default());
        let price_gate = Arc::new(support::PriceGate::default());
        let mut prices = golden_mid_cache(0);
        if deployed {
            let markets = (0..TOTAL_DECISIONS)
                .map(|index| {
                    let bodies =
                        golden_trade_bodies(index, golden_source_unix(start_unix + 1, index));
                    let mut rows: serde_json::Value =
                        serde_json::from_slice(&bodies.gamma).unwrap();
                    rows[0]["outcomePrices"] = "[\"0.50\",\"0.50\"]".into();
                    (bodies.condition.0, rows[0].take())
                })
                .collect();
            prices = MidPriceCache::with_fetcher(
                GoldenGammaFetcher {
                    markets,
                    gate: Some(price_gate.clone()),
                },
                "fixture://gamma".to_owned(),
            );
        }
        let hooks = Arc::new(ScenarioHooks::default());
        hooks
            .financial_clock_unix
            .store(100, std::sync::atomic::Ordering::SeqCst);
        let books = Arc::new(GoldenBookFetcher::default());
        let initial = golden_watchlist(initial_wallets);
        let live = LiveWatchlist::new(initial.clone());
        let writer_lock = Arc::new(tokio::sync::Mutex::new(()));
        let (control, control_rx) = mpsc::channel(4);
        let mut orchestrator = Orchestrator::new_with_authority(
            live.clone(),
            OrchestratorConfig {
                bankroll: STARTING_BANKROLL,
                mode: ExecutionMode::Paper,
                signal_config: SignalConfig::default(),
                max_resolution_horizon_secs: 0,
                min_resolution_horizon_secs: 0,
                max_fill_price: runtime.max_fill_price,
                min_fill_price: runtime.min_fill_price,
                price_impact_cap_bps: 300,
                entry_gate_config: CopyEntryGateConfig,
                runtime_config: None,
                live_accounts: deployed.then(|| accounts.clone()),
                activity_ws_enabled: deployed,
                copy_latency_budget_secs: 2,
                watchlist_writer_lock: Some(writer_lock.clone()),
            },
            WinnerFollowStrategy::new(runtime.winner_follow_config()),
            Writer::open(&paper_path).unwrap(),
            paper.clone(),
            pe_service::paper_recovery::build_leader_ledger(&paper).unwrap(),
            new_shared_health_with_ws(false, true, 90),
            prices.with_source_log(source.clone()).with_clock({
                let hooks = hooks.clone();
                Arc::new(move || {
                    OffsetDateTime::from_unix_timestamp(
                        hooks
                            .financial_clock_unix
                            .load(std::sync::atomic::Ordering::SeqCst),
                    )
                    .unwrap()
                })
            }),
            control_rx,
            None,
            authority.clone(),
            books.clone(),
        )
        .unwrap();
        orchestrator.set_scenario_hooks(hooks.clone());
        orchestrator
            .configure_financial_log_paths(
                paper_path.clone(),
                source_path.clone(),
                LiveAdmissionBuilder::new(
                    reqwest::Client::new(),
                    "http://unused.invalid",
                    "http://unused.invalid",
                    source.clone(),
                ),
                Arc::new(HistoricalMarkAdapter::new(
                    reqwest::Client::new(),
                    "http://unused.invalid",
                    source.clone(),
                )),
                receipts.clone(),
            )
            .unwrap();
        // One fixed terminal instant at the end of the recorded financial window; the short
        // bracket cases use their existing financial clock. Admission/Prepared clocks remain
        // independently controlled by each attempt's hooks.
        let terminal_at = OffsetDateTime::from_unix_timestamp(if deployed {
            start_unix + 1 + i64::try_from(QUALIFICATION_DAYS).unwrap() * DAY_SECS
        } else {
            100
        })
        .unwrap();
        let actor = tokio::spawn(
            pe_service::orchestrator::SCENARIO_TERMINAL_CLOCK
                .scope(terminal_at, orchestrator.run(std::future::pending::<()>())),
        );
        Self {
            control,
            source,
            live,
            writer_lock,
            paper_path,
            source_path,
            initial,
            runtime,
            receipts,
            accounts,
            price_gate,
            hooks,
            books,
            authority,
            terminal_at,
            actor,
            coordinator,
            _triggers: trigger_rx,
        }
    }

    pub(crate) fn entry_payload(wallet: WalletAddress, epoch: i64) -> Vec<u8> {
        let mut rows: serde_json::Value =
            serde_json::from_slice(&golden_trade_bodies(1, epoch).activity).unwrap();
        rows[0]["proxyWallet"] = wallet.to_string().into();
        serde_json::to_vec(&rows).unwrap()
    }

    pub(crate) fn entry_gamma() -> Vec<u8> {
        golden_trade_bodies(1, 100).gamma
    }

    pub(crate) fn entries(wallets: &[WalletAddress]) -> Vec<WatchlistEntry> {
        golden_watchlist(wallets).entries
    }

    pub(crate) async fn ordinary_entry(
        &self,
        wallet: WalletAddress,
        epoch: i64,
    ) -> pe_service::bucket_commit::BucketCommitResult {
        self.queue_entry(wallet, epoch)
            .await
            .await
            .unwrap()
            .unwrap()
    }

    pub(crate) async fn queue_entry(
        &self,
        wallet: WalletAddress,
        epoch: i64,
    ) -> oneshot::Receiver<Result<pe_service::bucket_commit::BucketCommitResult, String>> {
        let bodies = golden_trade_bodies(1, epoch);
        self.record_entry_admission(&bodies, epoch).await;
        let payload = Self::entry_payload(wallet, epoch);
        let page = append_source_at(&self.source, ACTIVITY_POLL_SOURCE_ID, &payload, epoch).await;
        let read = support::producer_shaped_read_v2(wallet, &payload, epoch, epoch, page);
        let commitment = append_source_at(
            &self.source,
            ACTIVITY_READ_COMMITMENT_SOURCE_ID,
            &read.commitment_payload,
            epoch,
        )
        .await;
        let mut context = support::read_context(&read, commitment, epoch);
        context.applied_configuration = self.runtime.clone();
        assert!(context.history_status.is_none());
        let (committed, ack) = oneshot::channel();
        self.control
            .send(OrchestratorControl::CommitActivityBucket {
                aggregates: read.aggregates,
                context: Arc::new(context),
                committed,
            })
            .await
            .unwrap();
        ack
    }

    pub(crate) fn mutations(&self) -> usize {
        self.authority.mutations()
    }

    pub(crate) async fn shutdown(self) {
        drop(self.control);
        self.actor.await.unwrap();
        drop(self.source);
        self.coordinator.await.unwrap();
    }
}

/// PASS: internally consistent row edits to disabled policy or exact clock still fail the unchanged seal digest.
#[allow(clippy::too_many_arguments)]
fn qualification_replays_source_age_and_seal_binds_policy_and_clock(
    root: &std::path::Path,
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    state_path: &std::path::Path,
    live_path: &std::path::Path,
    seal_receipt: AppendReceipt,
    source_trade_id: &pe_core_types::SourceTradeId,
) {
    let original_digest = decision_evidence_digest(paper_path, state_path);
    for mutation in ["paper-policy", "paper-clock"] {
        let case = root.join(mutation);
        std::fs::create_dir(&case).unwrap();
        let cloned_state = case.join("paper.db");
        clone_state(state_path, &cloned_state);
        let connection = rusqlite::Connection::open(&cloned_state).unwrap();
        let (frozen, post): (String, String) = connection.query_row(
            "SELECT frozen_inputs_json, post_commit_inputs_json FROM decision_pending WHERE source_trade_id = ?1",
            rusqlite::params![source_trade_id.0], |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
        let mut continuation: DecisionContinuationV3 = serde_json::from_str(&frozen).unwrap();
        let mut evidence: DecisionPostBoundaryEvidence = serde_json::from_str(&post).unwrap();
        assert!(
            !continuation
                .facts
                .paper_freshness_policy
                .unwrap()
                .activity_ws_enabled
        );
        if mutation == "paper-policy" {
            continuation
                .facts
                .paper_freshness_policy
                .as_mut()
                .unwrap()
                .copy_latency_budget_secs = 3;
        } else {
            let clock = evidence
                .body
                .clocks
                .iter_mut()
                .find(|clock| clock.purpose == "paper_prepared_staleness_gate")
                .unwrap();
            assert_eq!(clock.submillisecond_nanos, Some(0));
            clock.submillisecond_nanos = Some(1);
        }
        connection.execute(
            "UPDATE decision_pending SET frozen_inputs_json = ?2, post_commit_inputs_json = ?3 WHERE source_trade_id = ?1",
            rusqlite::params![source_trade_id.0, serde_json::to_string(&continuation).unwrap(),
                serde_json::to_string(&DecisionPostBoundaryEvidence::from_body(evidence.body).unwrap()).unwrap()],
        ).unwrap();
        drop(connection);
        let state = PaperStateDb::open_read_only(&cloned_state).unwrap();
        replay_decision_pending(
            &state
                .decision_pending_for(source_trade_id)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        drop(state);
        let changed_digest = decision_evidence_digest(paper_path, &cloned_state);
        assert_ne!(changed_digest, original_digest);
        assert_qualify_insufficient(
            paper_path,
            source_path,
            live_path,
            &cloned_state,
            seal_receipt,
            &case.join("report.json"),
            &format!(
                "decision evidence digest mismatch: sealed {original_digest}, replayed {changed_digest}"
            ),
        );
    }
}

// Use the existing golden corpus with a single runtime newcomer and distinct first-entry markets.
fn deployed_trade(index: usize, epoch: i64, corrected: bool) -> GoldenTradeBodies {
    let mut bodies = golden_trade_bodies(index, epoch);
    bodies.wallet = WalletAddress::from_hex(WALLET).unwrap();
    let mut activity: serde_json::Value = serde_json::from_slice(&bodies.activity).unwrap();
    activity[0]["proxyWallet"] = WALLET.into();
    activity[0]["timestamp"] = epoch.into();
    bodies.activity = serde_json::to_vec(&activity).unwrap();
    let mut stream: serde_json::Value = serde_json::from_slice(&bodies.websocket).unwrap();
    stream["proxyWallet"] = WALLET.into();
    stream["timestamp"] = epoch.to_string().into();
    if corrected {
        stream["conditionId"] = format!("0x{:064x}", 999_999).into();
        stream["outcomeIndex"] = "1".into();
    }
    bodies.websocket = serde_json::to_vec(&stream).unwrap();
    bodies
}

impl BracketFinancialHarness {
    async fn record_entry_admission(&self, bodies: &GoldenTradeBodies, epoch: i64) {
        let receipts = AdmissionReceipts {
            gamma: append_source_at(
                &self.source,
                "polymarket.gamma.markets",
                &bodies.gamma,
                epoch,
            )
            .await,
            clob_long: append_source_at(
                &self.source,
                "polymarket.clob.markets",
                &bodies.clob_long,
                epoch,
            )
            .await,
            clob_compact: append_source_at(
                &self.source,
                "polymarket.clob.compact-market",
                &bodies.compact,
                epoch,
            )
            .await,
        };
        let book_receipt =
            append_source_at(&self.source, "polymarket.clob.book", &bodies.book, epoch).await;
        let (admission, mut book) = admission_and_book(
            &bodies.condition,
            epoch,
            GoldenAdmissionBodies {
                gamma: &bodies.gamma,
                clob: &bodies.clob_long,
                compact: &bodies.compact,
                book: &bodies.book,
            },
            receipts,
            book_receipt,
        );
        book.fetched_at_ms = u64::try_from(epoch * 1_000).unwrap();
        self.books.insert(
            admission.market.ordered_outcome_token_ids[0].to_string(),
            book,
        );
        self.hooks
            .admission_artifacts
            .lock()
            .unwrap()
            .push_back(admission);
        self.hooks
            .financial_clock_unix
            .store(epoch, std::sync::atomic::Ordering::SeqCst);
    }

    async fn deployed_boundary(&self, cutoff: i64) {
        self.hooks
            .financial_clock_unix
            .store(cutoff, std::sync::atomic::Ordering::SeqCst);
        let receipt = append_source_at(
            &self.source,
            "pe-service.boundary",
            &serde_json::to_vec(&serde_json::json!({
                "kind": "daily_boundary", "cutoff_unix": cutoff,
            }))
            .unwrap(),
            cutoff,
        )
        .await;
        let (acknowledged, ack) = oneshot::channel();
        self.control
            .send(OrchestratorControl::DailyBoundary {
                cutoff_unix: cutoff,
                boundary_receipt: receipt,
                acknowledged,
            })
            .await
            .unwrap();
        ack.await.unwrap().unwrap();
    }

    async fn deployed_prepare_membership(
        &self,
        paper: &Arc<PaperStateDb>,
        wallets: &[WalletAddress],
        epoch: i64,
    ) {
        use pe_service::asset_identity::AssetIdentityResolver;
        use pe_service::position_seeder::CausalPositionValidator;
        use pe_service::watchlist_admission::AdmissionPreparer;
        use pe_source_polymarket_public::{GAMMA_BATCH_SIZE, ReconciliationFetcher};
        for wallet in wallets {
            assert!(paper.wallet_history_status(wallet).unwrap().is_none());
            assert!(paper.cursor(wallet).unwrap().is_none());
        }
        let history = golden_trade_bodies(TOTAL_DECISIONS + 1, epoch - 1);
        let (requests, mut requested) = mpsc::channel(8);
        let fetcher: Arc<dyn ReconciliationFetcher> = Arc::new(support::GatedFetcher { requests });
        let identity = Arc::new(AssetIdentityResolver::new_runtime(
            fetcher.clone(),
            "fixture://gamma".to_owned(),
            GAMMA_BATCH_SIZE,
            self.source.clone(),
        ));
        let preparer = AdmissionPreparer::with_validator(
            self.control.clone(),
            paper.clone(),
            CausalPositionValidator::new(
                fetcher,
                "fixture://activity",
                "golden-stream-v1",
                identity,
            )
            .with_clock(Arc::new(move || epoch)),
        )
        .with_source_log(self.source.clone());
        let preparing = preparer.prepare(wallets);
        tokio::pin!(preparing);
        loop {
            tokio::select! {
                result = &mut preparing => { result.unwrap(); break; }
                request = requested.recv() => {
                    let request = request.unwrap();
                    let payload = if request.url.contains("/markets?") {
                        history.gamma.clone()
                    } else {
                        let wallet = wallets.iter().find(|wallet| request.url.contains(&wallet.to_string())).unwrap();
                        let mut rows: serde_json::Value = serde_json::from_slice(&history.activity).unwrap();
                        rows[0]["proxyWallet"] = wallet.to_string().into();
                        if request.url.contains("/activity?") {
                            assert!(request.url.ends_with("&start=1"), "runtime validation requires full history");
                            serde_json::to_vec(&rows).unwrap()
                        } else {
                            assert!(request.url.contains("/positions?"), "{}", request.url);
                            if request.url.contains("redeemable=true") { b"[]".to_vec() } else {
                                serde_json::to_vec(&serde_json::json!([{
                                    "proxyWallet": wallet, "asset": rows[0]["asset"],
                                    "conditionId": history.condition.0, "size": "5", "outcomeIndex": 0,
                                    "negativeRisk": false,
                                }])).unwrap()
                            }
                        }
                    };
                    request.respond.send(payload).unwrap();
                }
            }
        }
        assert!(
            self.live.snapshot().entries.is_empty(),
            "validation must precede publication"
        );
        assert!(paper.decision_pending_history().unwrap().is_empty());
        for wallet in wallets {
            let status = paper.wallet_history_status(wallet).unwrap().unwrap();
            assert!(status.complete);
            assert_eq!(
                status.proof_json,
                paper
                    .position_validation(wallet)
                    .unwrap()
                    .unwrap()
                    .proof_json
            );
            let proof: serde_json::Value = serde_json::from_str(&status.proof_json).unwrap();
            assert_eq!(proof["activity_walks"].as_array().unwrap().len(), 3);
            assert!(
                paper.gate_history().unwrap()[wallet].contains(&pe_core_types::MarketId(
                    pe_core_types::VenueMarketId(history.condition.0.clone())
                ))
            );
        }
        preparer
            .scenario_publish_ranking(
                &self.live,
                &self.writer_lock,
                golden_watchlist(wallets).entries,
                &wallets.iter().map(|wallet| (*wallet, epoch - 1)).collect(),
                wallets.len(),
            )
            .await
            .unwrap();
        assert_eq!(
            self.live
                .snapshot()
                .entries
                .iter()
                .map(|entry| entry.wallet)
                .collect::<Vec<_>>(),
            wallets
        );
    }

    // Reuse S1's held response barriers. The background slot stays occupied until the urgent
    // wallet has a durable terminal; all source pages and commitments are authored by TradePoller.
    async fn deployed_poll(
        &self,
        paper: Arc<PaperStateDb>,
        bodies: &GoldenTradeBodies,
        epoch: i64,
    ) -> pe_paper_state::DecisionPendingRow {
        use pe_service::activity_ingest::{ACTIVITY_WS_SOURCE_ID, ReconciliationTrigger};
        use pe_service::asset_identity::AssetIdentityResolver;
        use pe_service::trade_poller::{
            PollerProgress, ReconciliationObligations, TRADE_RECONCILIATION_CONCURRENCY,
            TradePoller, TradePollerConfig,
        };
        use pe_source_polymarket_public::{GAMMA_BATCH_SIZE, parse_activity_trade_observation};
        let observation = parse_activity_trade_observation(&bodies.websocket).unwrap();
        let now = OffsetDateTime::from_unix_timestamp(epoch).unwrap();
        let receipt = append_source_at(
            &self.source,
            ACTIVITY_WS_SOURCE_ID,
            &bodies.websocket,
            epoch,
        )
        .await;
        let (triggers, receiver) = mpsc::channel(8);
        triggers
            .send(ReconciliationTrigger {
                wallet: bodies.wallet,
                source_time: observation.source_time.0,
                source_trade_id: observation.group_id.key().clone(),
                provenance: TradeProvenance::ActivityWs,
                received_at: now,
                receipt,
            })
            .await
            .unwrap();
        let (requests, mut requested) = mpsc::channel(8);
        let (progress, mut progressed) = mpsc::channel(16);
        let (stop, stopped) = oneshot::channel();
        let poller = TradePoller::new(
            TradePollerConfig {
                base_url: "fixture://activity".to_owned(),
                poll_interval_secs: 30,
                activity_ws_enabled: true,
                copy_latency_budget_secs: 2,
            },
            self.live.clone(),
            Arc::new(support::GatedFetcher { requests }),
            Arc::new(AssetIdentityResolver::new_runtime(
                Arc::new(support::Page(bodies.gamma.clone())),
                "fixture://gamma".to_owned(),
                GAMMA_BATCH_SIZE,
                self.source.clone(),
            )),
            self.source.clone(),
            receiver,
            self.control.clone(),
            paper.clone(),
            new_shared_health_with_ws(false, true, 90),
            SignalConfig::default(),
            pe_service::runtime_config::LiveRuntimeConfig::new(self.runtime.clone()),
            ReconciliationObligations::default(),
            None,
        )
        .with_source_receipt_index(self.receipts.clone())
        .with_progress(progress)
        .with_clock(Arc::new(move || now));
        let task = tokio::spawn(poller.run_until(async {
            let _ = stopped.await;
        }));
        let mut background = None;
        let mut urgent = None;
        for _ in 0..TRADE_RECONCILIATION_CONCURRENCY {
            let request = requested.recv().await.unwrap();
            assert!(
                request.url.contains(&format!("end={epoch}")),
                "{}",
                request.url
            );
            if request.url.contains(WALLET) {
                urgent = Some(request);
            } else {
                background = Some(request);
            }
        }
        urgent
            .unwrap()
            .respond
            .send(bodies.activity.clone())
            .unwrap();
        let mut active = std::collections::HashSet::new();
        let mut maximum = 0;
        loop {
            let progress = progressed.recv().await.unwrap();
            match progress {
                PollerProgress::Started { wallet, .. } => {
                    assert!(active.insert(wallet));
                    maximum = maximum.max(active.len());
                    assert!(active.len() <= TRADE_RECONCILIATION_CONCURRENCY);
                }
                PollerProgress::Completed { wallet, selected } => {
                    assert!(active.remove(&wallet));
                    assert_eq!(
                        wallet, bodies.wallet,
                        "held background read cannot complete"
                    );
                    assert_eq!(selected, vec![receipt]);
                    break;
                }
                PollerProgress::RoundCompleted => {
                    panic!("round finished with held background response")
                }
            }
        }
        assert_eq!(maximum, TRADE_RECONCILIATION_CONCURRENCY);
        assert!(
            matches!(requested.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "no third operation"
        );
        let row = paper
            .decision_pending_history()
            .unwrap()
            .into_iter()
            .find(|row| {
                let continuation = DecisionContinuationV3::from_durable(row).unwrap();
                continuation.facts.source_epoch == observation.source_time.0.unix_timestamp()
            })
            .unwrap();
        assert_eq!(row.state, pe_paper_state::DecisionPendingState::Terminal);
        let replay = replay_decision_pending(&row).unwrap();
        assert_eq!(replay.continuation.version(), 5);
        assert_eq!(row.updated_at_unix, self.terminal_at.unix_timestamp());
        assert_eq!(
            replay
                .post_boundary
                .body
                .clocks
                .iter()
                .filter(|clock| clock.purpose == "terminal_transition")
                .collect::<Vec<_>>(),
            vec![&pe_service::decision_replay::DecisionClockEvidence {
                purpose: "terminal_transition".to_owned(),
                unix_millis: self.terminal_at.unix_timestamp() * 1_000,
                submillisecond_nanos: None,
            }]
        );
        assert_eq!(
            replay.continuation.facts.provenance,
            TradeProvenance::ActivityWs
        );
        assert_eq!(
            replay.continuation.facts.market_id.to_string(),
            bodies.condition.0
        );
        assert_eq!(replay.continuation.facts.outcome_id, OutcomeId(0));
        assert_eq!(replay.continuation.observed_source_receipt, Some(receipt));
        assert!(
            replay
                .continuation
                .observation_from_receipt_index(&self.receipts)
                .unwrap()
                .is_some()
        );
        let policy = replay.continuation.facts.paper_freshness_policy.unwrap();
        assert!(policy.activity_ws_enabled);
        assert_eq!(policy.copy_latency_budget_secs, 2);
        let commitment = replay.continuation.read_commitment.unwrap();
        let envelope = Reader::replay(&self.source_path)
            .unwrap()
            .map(Result::unwrap)
            .map(|(_, envelope)| envelope)
            .find(|envelope| envelope.seq == commitment.sequence)
            .unwrap();
        assert_eq!(envelope.this_hash, commitment.this_hash);
        assert_eq!((envelope.schema_version, envelope.parser_version), (2, 1));
        let bound: ActivityReadCommitment = serde_json::from_slice(&envelope.payload).unwrap();
        assert_eq!(bound.version, 2);
        let stream: serde_json::Value = serde_json::from_slice(&bodies.websocket).unwrap();
        if stream["conditionId"] != bodies.condition.0 {
            assert_ne!(observation.group_id.key(), &row.source_trade_id);
            let bindings = bound.bindings.as_ref().unwrap();
            assert_eq!(bindings.len(), 1);
            assert_eq!(bindings[0].stream_receipt, receipt);
            assert_eq!(&bindings[0].stream_group_id, observation.group_id.key());
            assert_eq!(bindings[0].history_group_id, row.source_trade_id);
        } else {
            assert_eq!(observation.group_id.key(), &row.source_trade_id);
        }
        stop.send(()).unwrap();
        background.unwrap().respond.send(b"[]".to_vec()).unwrap();
        task.await.unwrap().unwrap();
        row
    }
}

/// Composed recorded-policy proof: runtime full-history bracket validation/publication, stream
/// observation through the real two-slot poller, orchestrator gates, fresh/corrected paper fills,
/// cold-portfolio expiry with staged-target release, completed-state restart, and sealed CLI replay.
/// Uses the golden corpus's 2030 end dates, disabled min/max resolution horizons, impact cap 300,
/// and recorded admission artifacts queued through scenario hooks. The poller has no admission
/// preparer; runtime bracket preparation completes separately before polling. No fanout consumer
/// runs: dispatch proof ends at durable readiness with the stored target unchanged.
///
/// Related `scenario_execution_gates` fixtures run before Start with horizons disabled and zero
/// book requests in the mandatory-cap case; they do not prove unchanged-policy horizon/impact
/// admission. Pure horizon boundaries have `orchestrator::tests::{rejects_too_far_out,
/// rejects_too_soon, bounds_are_inclusive_at_edges}` coverage. Real `LiveAdmissionBuilder` over
/// mocked HTTP is covered by `scenario_paper_prepared_freshness`'s
/// `reverse_completion_admission_executes_and_qualifies_exactly` and `scenario_boot_order`'s
/// `staged_recovery_records_admission_before_observation_producers_start`; released-target
/// consumption by `live_fanout::tests::ordered_execution_is_primary_first_and_next_waits_for_terminal`.
/// A naturally eligible production opportunity remains operator acceptance under issue #588 and
/// the identifier-bound check in `docs/35-PE-SERVICE-DEPLOY-RUNBOOK.md`, step 6.
///
/// Terminal clocks are fixed and asserted for fills and expiry. Equality is same-run replay:
/// `AdmissionPreparer::record_artifact` still stamps membership source envelopes with wall time,
/// so source receipts, membership bindings and sealed digests are not cross-run byte fixtures.
pub(crate) async fn deployed_flow_replays_exactly_and_qualifies() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("paper.db");
    let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
    let newcomer = WalletAddress::from_hex(WALLET).unwrap();
    let background = WalletAddress([0xbb; 20]);
    let h =
        BracketFinancialHarness::new_with_deployed_flow(dir.path(), paper.clone(), &[], true).await;
    let cutoff = FIXED_UNIX - FIXED_UNIX.rem_euclid(DAY_SECS);
    h.hooks.financial_clock_unix.store(cutoff, Ordering::SeqCst);
    h.deployed_prepare_membership(&paper, &[newcomer, background], cutoff)
        .await;
    let completion = paper.wallet_history_status(&newcomer).unwrap();
    h.deployed_boundary(cutoff).await;
    let shares = ShareAmount::from_decimal_exact(dec!(5.04)).unwrap();
    let credit = CollateralAmount::from_decimal_exact(dec!(5.04)).unwrap();
    let mut expected = Vec::new();
    let mut expired_id = None;
    for day in 0..QUALIFICATION_DAYS {
        for within_day in 0..COPIES_PER_DAY {
            let ordinal = day * COPIES_PER_DAY + within_day;
            let epoch =
                golden_source_unix(cutoff, ordinal) + i64::try_from(within_day).unwrap() * 180;
            if within_day == 0 {
                eprintln!("deployed flow day={day}");
            }
            let bodies = deployed_trade(ordinal, epoch, ordinal == 1);
            h.record_entry_admission(&bodies, epoch).await;
            h.hooks
                .age_clock
                .lock()
                .unwrap()
                .extend(std::iter::repeat_n(
                    OffsetDateTime::from_unix_timestamp(epoch).unwrap(),
                    3,
                ));
            let row = h.deployed_poll(paper.clone(), &bodies, epoch).await;
            let replay = replay_decision_pending(&row).unwrap();
            assert_eq!(row.terminal_disposition.as_deref(), Some("fill"));
            assert_eq!(
                replay.post_boundary.body.terminal.reason,
                "paper_fill_committed"
            );
            let final_receipt = replay.post_boundary.body.terminal.final_receipt.unwrap();
            let frames = scan_paper_log(&h.paper_path).unwrap();
            let finals = frames
                .iter()
                .filter(|frame| frame.receipt == final_receipt)
                .collect::<Vec<_>>();
            assert_eq!(finals.len(), 1);
            assert!(matches!(
                finals[0].frame,
                PaperLogFrame::Record(PaperLogRecord::FinancialFinal { .. })
            ));
            let prepared = frames
                .iter()
                .filter_map(|frame| match &frame.frame {
                    PaperLogFrame::Record(PaperLogRecord::FinancialPrepared {
                        payload:
                            pe_service::paper_recovery::FinancialPayload::Fill {
                                operation,
                                economic,
                            },
                        ..
                    }) if operation.source_trade_id == row.source_trade_id => {
                        Some((frame.receipt, economic))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(prepared.len(), 1);
            assert_eq!(
                prepared[0].1.sizing.minimum_shares,
                ShareAmount::from_whole(5).unwrap()
            );
            assert_eq!(
                prepared[0].1.all_in_debit().unwrap().to_decimal(),
                dec!(2.55)
            );
            let PaperLogFrame::Record(PaperLogRecord::FinancialFinal {
                prepared_receipt, ..
            }) = &finals[0].frame
            else {
                panic!("fill terminal does not reference a FinancialFinal");
            };
            assert_eq!(*prepared_receipt, prepared[0].0);
            assert_eq!(
                replay
                    .post_boundary
                    .body
                    .clocks
                    .iter()
                    .filter(|clock| clock.purpose == "paper_prepared_staleness_gate")
                    .collect::<Vec<_>>(),
                vec![
                    &pe_service::decision_replay::DecisionClockEvidence::precise(
                        "paper_prepared_staleness_gate",
                        OffsetDateTime::from_unix_timestamp(epoch)
                            .unwrap()
                            .unix_timestamp_nanos()
                    )
                    .unwrap()
                ]
            );
            expected.push((
                row.source_trade_id.clone(),
                prepared[0].0,
                final_receipt,
                prepared[0].1.core_hash().unwrap().to_string(),
            ));
            assert_eq!(paper.wallet_history_status(&newcomer).unwrap(), completion);
            if ordinal == 0 {
                // A later fresh observation encounters an aged portfolio cache, then its
                // held response consumes the copy budget while the first position remains open.
                let expiry_epoch = epoch + 120;
                let expired = deployed_trade(PRICE_CONFLICT_DECISION, expiry_epoch, false);
                h.accounts
                    .store(pe_service::live_accounts::LiveAccountsSnapshot {
                        accounts: vec![pe_service::live_accounts::AccountContext {
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
                        fetched_at_unix: Some(expiry_epoch),
                    });
                h.record_entry_admission(&expired, expiry_epoch).await;
                let start = OffsetDateTime::from_unix_timestamp(expiry_epoch).unwrap();
                h.hooks.age_clock.lock().unwrap().extend([
                    start,
                    start,
                    start + time::Duration::seconds(2) + time::Duration::nanoseconds(1),
                ]);
                *h.price_gate.market.lock().unwrap() = Some(bodies.condition.0.clone());
                h.price_gate.blocked.store(true, Ordering::SeqCst);
                let before = paper.financial_snapshot(expiry_epoch).unwrap();
                let mutations = h.mutations();
                let attempt = h.deployed_poll(paper.clone(), &expired, expiry_epoch);
                tokio::pin!(attempt);
                tokio::select! { biased;
                    _ = h.price_gate.started.notified() => {},
                    row = &mut attempt => panic!("attempt bypassed cold portfolio price read: {:?}", replay_decision_pending(&row).unwrap()),
                }
                let seeds = paper.pending_dispatch_seeds().unwrap();
                assert_eq!(seeds.len(), 1);
                let targets = paper.dispatch_targets(&seeds[0].dispatch_id).unwrap();
                assert_eq!(targets.len(), 1);
                eprintln!("deployed flow releasing portfolio price");
                h.price_gate.release.notify_one();
                let expired_row = attempt.await;
                let replay = replay_decision_pending(&expired_row).unwrap();
                assert_eq!(
                    replay.post_boundary.body.terminal.reason,
                    "paper_stale_before_prepared"
                );
                assert_eq!(expired_row.terminal_disposition.as_deref(), Some("no_fill"));
                assert_eq!(
                    replay
                        .post_boundary
                        .body
                        .clocks
                        .iter()
                        .filter(|clock| clock.purpose == "paper_prepared_staleness_gate")
                        .collect::<Vec<_>>(),
                    vec![
                        &pe_service::decision_replay::DecisionClockEvidence::precise(
                            "paper_prepared_staleness_gate",
                            (start + time::Duration::seconds(2) + time::Duration::nanoseconds(1))
                                .unix_timestamp_nanos()
                        )
                        .unwrap()
                    ]
                );
                assert!(scan_paper_log(&h.paper_path).unwrap().iter().all(|frame| !matches!(&frame.frame,
                    PaperLogFrame::Record(PaperLogRecord::FinancialPrepared { payload: pe_service::paper_recovery::FinancialPayload::Fill { operation, .. }, .. }) if operation.source_trade_id == expired_row.source_trade_id
                )));
                assert!(replay.post_boundary.body.terminal.final_receipt.is_none());
                assert_eq!(paper.financial_snapshot(expiry_epoch).unwrap(), before);
                assert_eq!(h.mutations(), mutations);
                let ready = paper.dispatch_seed(&seeds[0].dispatch_id).unwrap().unwrap();
                assert_eq!(ready.state, "ready");
                assert_eq!(
                    ready.paper_outcome.as_deref(),
                    Some("no_fill:paper_stale_before_prepared")
                );
                assert_eq!(paper.dispatch_targets(&ready.dispatch_id).unwrap(), targets);
                assert!(paper.gate_history().unwrap()[&newcomer].contains(
                    &pe_core_types::MarketId(pe_core_types::VenueMarketId(
                        expired.condition.0.clone()
                    ))
                ));
                expired_id = Some(expired_row.source_trade_id);
                h.accounts.store(Default::default());
            }
            let resolution_epoch = epoch + if ordinal == 0 { 125 } else { 5 };
            let resolution = append_source_at(
                &h.source,
                "polymarket.clob.market",
                &bodies.resolution,
                resolution_epoch,
            )
            .await;
            resolve_golden_trade(
                &h.control,
                &h.hooks,
                &bodies,
                resolution,
                shares,
                credit,
                resolution_epoch,
            )
            .await;
        }
        h.deployed_boundary(cutoff + i64::try_from(day + 1).unwrap() * DAY_SECS)
            .await;
    }
    eprintln!("deployed flow sealed; reopening");
    let paths = (h.paper_path.clone(), h.source_path.clone());
    let initial = h.initial.clone();
    let membership = serde_json::to_vec(&h.live.snapshot().entries).unwrap();
    let history = paper.gate_history().unwrap();
    let rows = paper.decision_pending_history().unwrap();
    let financial = paper
        .financial_snapshot(cutoff + i64::try_from(QUALIFICATION_DAYS).unwrap() * DAY_SECS)
        .unwrap();
    let authority = h.authority.clone();
    h.shutdown().await;
    drop(paper);
    let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
    let index = SourceReceiptIndex::replay(&paths.1).unwrap();
    assert_eq!(
        pe_service::bucket_commit::validate_open_continuations(&paper, &index).unwrap(),
        0
    );
    let before_recovery = std::fs::read(&paths.0).unwrap();
    let mutations = authority.mutations();
    for _ in 0..2 {
        let mut writer = Writer::open(&paths.0).unwrap();
        assert_eq!(
            pe_service::supabase_state::reconcile_active_financial_frames(
                &authority,
                &paper,
                &paths.0,
                pe_service::supabase_state::SourceEvidence::Index(&index),
                &mut writer,
            )
            .await
            .unwrap(),
            0
        );
    }
    assert_eq!(authority.mutations(), mutations);
    assert_eq!(std::fs::read(&paths.0).unwrap(), before_recovery);
    assert!(
        pe_service::trade_poller::rebuild_reconciliation_obligations(&paths.1, &paper)
            .unwrap()
            .is_empty()
    );
    let ledger = pe_service::paper_recovery::build_leader_ledger(&paper).unwrap();
    let replayed_ledger =
        pe_service::paper_recovery::replay_wallet_ledger(&paper, newcomer).unwrap();
    assert_eq!(
        ledger_capture(&ledger, &paper, newcomer).unwrap().hash,
        ledger_capture(&replayed_ledger, &paper, newcomer)
            .unwrap()
            .hash
    );
    let engine = BucketCommitEngine::load(paper.clone(), ledger).unwrap();
    assert!(engine.history_complete(&newcomer));
    assert_eq!(paper.gate_history().unwrap(), history);
    assert_eq!(paper.wallet_history_status(&newcomer).unwrap(), completion);
    assert_eq!(paper.decision_pending_history().unwrap(), rows);
    assert_eq!(
        paper
            .financial_snapshot(cutoff + i64::try_from(QUALIFICATION_DAYS).unwrap() * DAY_SECS)
            .unwrap(),
        financial
    );
    let era = paper_era(scan_paper_log(&paths.0).unwrap());
    let restored = pe_service::paper_recovery::replay_membership(&era, initial, &paths.1)
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&restored.watchlist.entries).unwrap(),
        membership
    );
    let seal = era
        .frames
        .iter()
        .find(|frame| {
            matches!(
                frame.frame,
                PaperLogFrame::Record(PaperLogRecord::QualificationSealed(_))
            )
        })
        .unwrap();
    let output_path = dir.path().join("deployed-qualification.json");
    let output = run_qualify_cli(
        &paths.0,
        &paths.1,
        &dir.path().join("live_journal.log"),
        &state_path,
        seal.receipt,
        &output_path,
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: QualificationReport =
        serde_json::from_slice(&std::fs::read(output_path).unwrap()).unwrap();
    assert_eq!(
        report.verdict,
        QualificationVerdict::Pass,
        "{:?}",
        report.reasons
    );
    assert!(report.replay.exact);
    assert_eq!(report.replay.fills, ORIGINAL_COPIES);
    assert_eq!(report.replay.financial_prepared, ORIGINAL_COPIES * 2);
    assert_eq!(report.replay.financial_final, ORIGINAL_COPIES * 2);
    assert_eq!(report.replay.no_fills, 1);
    assert_eq!(report.replay.membership_changes, 1);
    assert_eq!(
        report.evidence.economic_core_hashes,
        expected
            .iter()
            .map(|(_, _, _, hash)| hash.clone())
            .collect::<Vec<_>>()
    );
    assert!(
        expected
            .iter()
            .all(|(id, _, _, _)| Some(id) != expired_id.as_ref())
    );
}
