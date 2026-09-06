//! I16 golden future-stream source and economic replay scenario.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pe_copy_signal_engine::SignalConfig;
use pe_core_types::{
    AccountId, BasisPoints, CollateralAmount, EventSeq, OutcomeId, PolymarketConditionId,
    ReceivedAt, ReconstructionQuality, ShareAmount, Side, SourceId, SourceTimestamp, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Scanner, Writer};
use pe_execution_core::{
    AdmissionReceipts, CredentialBindingIdentity, EconomicPrepared, FrozenLiveTarget,
    LiveAdmissionArtifact, LiveControlMode, LiveExecutor, LiveJournal, LiveModeSnapshot,
    LiveOrderIdentity, LiveOrderPreparedAudit, LiveOrderRequest, LiveOrderVenue,
    LivePostClassification, LivePostFuture, LivePostParseError, LivePrepareResult,
    LiveReconciliationFuture, LiveVenueAccountReadError, LiveVenueAccountState,
    LiveVenuePrepareFuture, LiveVenuePrepareRequest, LiveVenuePrepared, RiskDecisionAudit,
};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_risk_engine::{BinaryPayout, aggregate_resolution_credit};
use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
use pe_service::bucket_commit::{
    BucketDecisionContext, DecisionContinuationError, DecisionContinuationV3, PageOccurrence,
};
use pe_service::clob_book::{ClobBookError, ClobBookFetcher, OrderBook};
use pe_service::decision_replay::replay_decision_pending;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health_with_ws;
use pe_service::live_venue_adapter::LiveAdmissionBuilder;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mark_prices::HistoricalMarkAdapter;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig, ScenarioHooks};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{
    CanonicalFillResult, CanonicalResolutionResult, FINANCIAL_SEMANTIC_VERSION,
    PAPER_LOG_SCHEMA_VERSION, PaperLogFrame, PaperLogRecord, QualificationStarted, SealReason,
    TailBinding, paper_era, scan_paper_log,
};
use pe_service::position_seeder::{AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture};
use pe_service::qualification::{
    QualificationReport, QualificationVerdict, qualification_completion,
};
use pe_service::risk_inputs::SourceReceiptMillisIndex;
use pe_service::runtime_config::RuntimeConfig;
use pe_service::source_event_sink::SourceEventSink;
use pe_service::supabase_sink::SupabaseFillRow;
use pe_service::supabase_state::{
    FillV2Outcome, PreparedFillRequest, PreparedResolutionRequest, SupabaseStateError,
    SupabaseStateTrait,
};
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityTransport, BinaryPayoutVector, CLOB_RESOLUTION_PARSER_VERSION,
    CLOB_RESOLUTION_SCHEMA_VERSION, FixtureFetcher, LIVE_MARKET_PARSER_VERSION,
    LIVE_MARKET_SCHEMA_VERSION, parse_activity_response, validate_live_market,
};
use pe_strategy_winner_follow::{ExecutionMode, PerTradeCap, SizingMode, WinnerFollowStrategy};
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
const STARTING_BANKROLL: Decimal = dec!(1_000);

fn fixture(name: &str) -> &'static [u8] {
    match name {
        "activity_page" => include_bytes!("fixtures/golden_stream_v1/activity_page.json"),
        "websocket_trigger" => {
            include_bytes!("fixtures/golden_stream_v1/websocket_trigger.json")
        }
        "gamma_long" => include_bytes!("fixtures/golden_stream_v1/gamma_long.json"),
        "clob_long" => include_bytes!("fixtures/golden_stream_v1/clob_long.json"),
        "clob_compact" => include_bytes!("fixtures/golden_stream_v1/clob_compact.json"),
        "book" => include_bytes!("fixtures/golden_stream_v1/book.json"),
        "prices_history" => include_bytes!("fixtures/golden_stream_v1/prices_history.json"),
        "clob_resolution" => include_bytes!("fixtures/golden_stream_v1/clob_resolution.json"),
        "expected" => include_bytes!("fixtures/golden_stream_v1/expected.json"),
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
    source_log
        .append(EnvelopeIn {
            source_id: SourceId(source_id.to_owned()),
            schema_version: version,
            parser_version: version,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        })
        .await
        .unwrap()
}

fn append_source_direct(
    writer: &mut Writer,
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
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(source_id.to_owned()),
            schema_version: version,
            parser_version: version,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        })
        .unwrap()
}

fn append_paper_record(
    writer: &mut Writer,
    record: &PaperLogRecord,
    received_at_unix: i64,
) -> AppendReceipt {
    let timestamp = OffsetDateTime::from_unix_timestamp(received_at_unix).unwrap();
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId("pe-service.paper".to_owned()),
            schema_version: PAPER_LOG_SCHEMA_VERSION,
            parser_version: 1,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: serde_json::to_vec(record).unwrap(),
        })
        .unwrap()
}

fn empty_tail(path: &std::path::Path) -> TailBinding {
    TailBinding::from(&Scanner::verify(path).unwrap())
}

fn golden_source_unix(anchor_cutoff: i64, index: usize) -> i64 {
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

    let wallet_hex = format!("0x{:040x}", index + 1);
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
    async fn fetch_book(&self, token_id: &str) -> Result<OrderBook, ClobBookError> {
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

fn golden_mid_cache(anchor_cutoff: i64) -> MidPriceCache<FixtureFetcher> {
    const BASE: &str = "fixture://gamma";
    let responses = (0..QUALIFICATION_DAYS * COPIES_PER_DAY)
        .map(|index| {
            let source_unix = golden_source_unix(anchor_cutoff, index);
            let bodies = golden_trade_bodies(index, source_unix);
            let mut gamma: serde_json::Value = serde_json::from_slice(&bodies.gamma).unwrap();
            gamma[0]["outcomePrices"] = "[\"0.50\",\"0.50\"]".into();
            (
                format!(
                    "{BASE}/markets?condition_ids={}&limit=500",
                    bodies.condition.0
                ),
                serde_json::to_vec(&gamma).unwrap(),
            )
        })
        .collect();
    MidPriceCache::with_fetcher(FixtureFetcher::new(responses), BASE.to_owned())
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
        .arg("--paper-state")
        .arg(state_path)
        .arg("--seal-hash")
        .arg(seal_receipt.this_hash.to_hex().as_str())
        .arg("--output")
        .arg(output_path)
        .output()
        .unwrap()
}

/// I16-GOLDEN-SOURCE-ECONOMIC-V1
///
/// Preconditions: the checked-in v1 corpus seeds one deterministic 30-day future stream; every
/// websocket and complete-page receipt enters the source log before its acknowledged bucket commit.
/// PASS: runtime writes Start, 90 exact fill/resolution pairs, 31 marks, and Complete seal; the
/// network-free `pe-service --qualify` replay is exact and Pass with identical classifications,
/// admission audits, economic core hashes, risk snapshots/decisions, and exact arithmetic while
/// paper/live wrapper hashes differ.
/// FAIL: any semantic output diverges, the CLI constructs a client, replay is inexact/non-Pass, or
/// a wrapper collision hides its distinct outer protocol.
///
/// I16-GOLDEN-PREIMAGE-V1
///
/// Preconditions: the first admitted continuation is replayed once with exactly its activity-page
/// envelope omitted and once with that one raw page changed at the same sequence.
/// PASS: omission returns `SourceReceiptMissing` with the exact sequence/reason and mutation returns
/// `SourceReceiptMismatch` with the exact sequence/reason; both altered logs also produce
/// `InsufficientEvidence` with inexact replay through the real `pe-service --qualify` command.
/// FAIL: either incomplete source log replays, returns an untyped error, or names another reason.
#[tokio::test]
async fn golden_source_stream_replays_exact_economic_core() {
    let scenario_started = Instant::now();
    let start_phase_started = Instant::now();
    assert!(std::path::Path::new(FIXTURE).is_relative());
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let paper_path = dir.path().join("paper.log");
    let live_path = dir.path().join("live.log");
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

    let start = QualificationStarted {
        starting_bankroll: CollateralAmount::from_decimal_exact(STARTING_BANKROLL).unwrap(),
        paper_prefix: empty_tail(&paper_path),
        source_prefix: empty_tail(&source_path),
        live_prefix: empty_tail(&live_path),
        artifact_blake3: "golden-artifact-v1".to_owned(),
        static_config_hash: "golden-static-v1".to_owned(),
        hot_config_hash: applied_configuration_hash.clone(),
        generation: "golden-stream-v1".to_owned(),
        activation_id: "golden-stream-v1".to_owned(),
        ranking_batch_id: 545,
        policy_hash: "golden-policy-v1".to_owned(),
        membership: wallets.clone(),
        membership_proofs_hash: "golden-membership-v1".to_owned(),
        schema_version: 2,
        parser_version: 1,
        financial_semantic_version: FINANCIAL_SEMANTIC_VERSION,
    };
    let mut paper_writer = Writer::open(&paper_path).unwrap();
    let start_receipt = append_paper_record(
        &mut paper_writer,
        &PaperLogRecord::QualificationStarted(Box::new(start)),
        start_unix,
    );
    drop(paper_writer);

    let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
    paper
        .reset_financial_era(
            start_receipt,
            CollateralAmount::from_decimal_exact(STARTING_BANKROLL).unwrap(),
        )
        .unwrap();
    let authority = GoldenAuthority::new(start_receipt);

    let source_sink = SourceEventSink::open(&source_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(64);
    let (trigger_tx, _trigger_rx) = mpsc::channel(1);
    let coordinator = tokio::spawn(
        ActivityIngest::poll_only(
            source_sink,
            source_rx,
            trigger_tx,
            new_shared_health_with_ws(false, true, 90),
        )
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

    for wallet in &wallets {
        paper.set_cursor(wallet, 0).unwrap();
    }
    let leader_ledger = PositionLedger::new();
    let installs = wallets
        .iter()
        .map(|wallet| {
            let captured = ledger_capture(&leader_ledger, &paper, *wallet).unwrap();
            AnchorInstall {
                wallet: *wallet,
                balances: Vec::new(),
                cutoff: 0,
                proof: AnchorProof {
                    positions_proof_hash: format!("golden-empty-{wallet}"),
                    activity_bounds_json: "[]".to_owned(),
                    source_log_generation: "golden-stream-v1".to_owned(),
                    document: "{}".to_owned(),
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
        golden_mid_cache(anchor_cutoff),
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
            SourceReceiptMillisIndex::replay(&source_path).unwrap(),
        )
        .unwrap();
    let control = tokio::spawn(orchestrator.run(std::future::pending::<()>()));
    let (installed, installation) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::InstallAnchors {
            installs,
            acknowledged: installed,
        })
        .await
        .unwrap();
    installation.await.unwrap().unwrap();
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
    let mut first_source_trade_id = None;
    let mut first_bodies = None;
    let mut first_admission = None;

    eprintln!(
        "PERF golden_stream phase=start elapsed={:?} total={:?}",
        start_phase_started.elapsed(),
        scenario_started.elapsed()
    );
    let stream_started = Instant::now();
    let mut source_append_elapsed = Duration::ZERO;
    let mut bucket_commit_elapsed = Duration::ZERO;
    let mut financial_commit_elapsed = Duration::ZERO;
    let mut mark_elapsed = Duration::ZERO;

    for day in 0..QUALIFICATION_DAYS {
        for within_day in 0..COPIES_PER_DAY {
            let index = day * COPIES_PER_DAY + within_day;
            let source_unix = golden_source_unix(anchor_cutoff, index);
            let bodies = golden_trade_bodies(index, source_unix);
            let now = OffsetDateTime::from_unix_timestamp(source_unix).unwrap();
            let source_append_started = Instant::now();
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
            source_append_elapsed += source_append_started.elapsed();
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
            hooks
                .admission_artifacts
                .lock()
                .unwrap()
                .push_back(admission.clone());
            book_fetcher.insert(
                admission.market.ordered_outcome_token_ids[0].to_string(),
                book,
            );
            hooks
                .financial_clock_unix
                .store(source_unix, std::sync::atomic::Ordering::SeqCst);

            let parse_context = ActivityParseContext {
                source_id: SourceId("polymarket-public.activity-reconciliation".to_owned()),
                observed_at: SourceTimestamp(now),
                received_at: ReceivedAt(now),
                transport: ActivityTransport::Rest,
            };
            let aggregate =
                parse_activity_response(&bodies.activity, bodies.wallet, &parse_context)
                    .unwrap()
                    .aggregates()
                    .unwrap()
                    .remove(0);
            let source_trade_id = aggregate.group_id.key().clone();
            if index == 0 {
                first_source_trade_id = Some(source_trade_id.clone());
                first_bodies = Some(bodies.clone());
                first_admission = Some(admission.clone());
            }
            let context = BucketDecisionContext {
                applied_configuration: runtime_config.clone(),
                decision_inputs_json: format!(
                    "{{\"fixture\":\"golden_stream_v1\",\"ordinal\":{index}}}"
                ),
                page_occurrences: vec![PageOccurrence {
                    request_url: format!("fixture://golden_stream_v1/activity_page/{index}"),
                    raw_hash: blake3::hash(&bodies.activity).to_hex().to_string(),
                    receipt: page_receipt,
                }],
                observed_source_receipts: HashMap::from([(
                    source_trade_id.clone(),
                    websocket_receipt,
                )]),
                reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
                signal_config: Default::default(),
                copy_eligible: true,
                bracket_commit: false,
                recorded_at_unix: source_unix,
                observation_provenance: HashMap::new(),
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
            let after_fill = paper.financial_snapshot(source_unix).unwrap();
            assert_eq!(after_fill.positions.len(), 1);
            assert_eq!(after_fill.positions[0].long, expected_shares);

            let resolution_unix = source_unix + 1;
            let resolution_receipt = append_source_at(
                &source_log,
                "polymarket.clob.market",
                &bodies.resolution,
                resolution_unix,
            )
            .await;
            let parsed_resolution =
                pe_source_polymarket_public::parse_clob_market(&bodies.resolution).unwrap();
            assert_eq!(
                parsed_resolution.condition_id.as_deref(),
                Some(bodies.condition.0.as_str())
            );
            let payout = BinaryPayoutVector::winner(0).unwrap();
            let payout_json = payout.canonical_json();
            let binary_payout =
                BinaryPayout::new(payout.decimals()[0], payout.decimals()[1]).unwrap();
            let payout_credit =
                aggregate_resolution_credit(&[(0, expected_shares)], &binary_payout).unwrap();
            assert_eq!(
                payout_credit.to_decimal().normalize().to_string(),
                expected["payout_credit"].as_str().unwrap()
            );
            hooks
                .financial_clock_unix
                .store(resolution_unix, std::sync::atomic::Ordering::SeqCst);
            let (resolved, resolution_acknowledgement) = oneshot::channel();
            control_tx
                .send(OrchestratorControl::ResolutionCandidate {
                    condition: bodies.condition.clone(),
                    payout_by_outcome_index_json: payout_json,
                    receipt: resolution_receipt,
                    acknowledged: resolved,
                })
                .await
                .unwrap();
            resolution_acknowledgement.await.unwrap().unwrap();
            assert!(
                paper
                    .financial_snapshot(resolution_unix)
                    .unwrap()
                    .positions
                    .is_empty()
            );
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
        assert!(snapshot.positions.is_empty());
        hooks
            .financial_clock_unix
            .store(cutoff, std::sync::atomic::Ordering::SeqCst);
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
    }

    eprintln!(
        "PERF golden_stream phase=stream wall={:?} source_append={:?} bucket_commit={:?} financial_commit={:?} marks={:?} total={:?}",
        stream_started.elapsed(),
        source_append_elapsed,
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
    assert_eq!(
        completion.causal_closes,
        QUALIFICATION_DAYS * COPIES_PER_DAY
    );

    let decision_rows = paper.decision_pending_history().unwrap();
    assert_eq!(decision_rows.len(), QUALIFICATION_DAYS * COPIES_PER_DAY);
    assert!(decision_rows.iter().all(|row| {
        replay_decision_pending(row).is_ok_and(|decision| {
            decision.continuation.prior.gate_result == "admitted"
                && decision.post_boundary.body.terminal.final_receipt.is_some()
        })
    }));
    let first_source_trade_id = first_source_trade_id.unwrap();
    let first_continuation = DecisionContinuationV3::from_durable(
        decision_rows
            .iter()
            .find(|row| row.source_trade_id == first_source_trade_id)
            .unwrap(),
    )
    .unwrap();
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
    assert_eq!(prepared_fills.len(), QUALIFICATION_DAYS * COPIES_PER_DAY);
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
    assert_eq!(
        authority.mutations(),
        QUALIFICATION_DAYS * COPIES_PER_DAY * 2
    );
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
    assert_eq!(report.replay.decisions, QUALIFICATION_DAYS * COPIES_PER_DAY);
    assert_eq!(
        report.replay.financial_prepared,
        QUALIFICATION_DAYS * COPIES_PER_DAY * 2
    );
    assert_eq!(
        report.replay.financial_final,
        QUALIFICATION_DAYS * COPIES_PER_DAY * 2
    );
    assert_eq!(report.replay.fills, QUALIFICATION_DAYS * COPIES_PER_DAY);
    assert_eq!(report.complete_days, QUALIFICATION_DAYS);
    assert_eq!(report.closed_copies, QUALIFICATION_DAYS * COPIES_PER_DAY);
    assert_eq!(report.paper_p95_delay_ms, Some(0));
    assert_eq!(
        report.evidence.economic_core_hashes,
        runtime_core_hashes
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
    );
    assert_eq!(runtime_risks.len(), QUALIFICATION_DAYS * COPIES_PER_DAY);
    assert!(
        runtime_risks
            .iter()
            .all(|risk| risk.decision == RiskDecisionAudit::Approved)
    );

    let first_admission = first_admission.unwrap();
    let PaperLogRecord::FinancialPrepared {
        payload:
            pe_service::paper_recovery::FinancialPayload::Fill {
                economic: first_economic,
                ..
            },
        ..
    } = &first_prepared_record
    else {
        panic!("first production Prepared record was not a fill");
    };
    let binding = CredentialBindingIdentity {
        version: 1,
        key_id: "golden-key".to_owned(),
    };
    let token_id = first_economic.market.token_id.clone();
    let live_request = LiveOrderRequest {
        target: FrozenLiveTarget {
            account_id: AccountId::new("golden-account").unwrap(),
            credential_binding: binding.clone(),
        },
        current_credential_binding: binding,
        mode: LiveModeSnapshot {
            requested: LiveControlMode::LiveTiny,
            effective: LiveControlMode::LiveTiny,
        },
        identity: LiveOrderIdentity {
            dispatch_id: "golden-dispatch".to_owned(),
            idempotency_key: "golden-live-idempotency".to_owned(),
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
            fill_projection: None,
            schema_version: 1,
            parser_version: 1,
        },
        condition_id: first_economic.market.condition_id.clone(),
        outcome_id: OutcomeId(u16::from(first_economic.market.outcome_index)),
        token_id,
        admission: first_admission.clone(),
        ladder: ladder_from_economic(first_economic),
        economic: first_economic.clone(),
    };
    let live_now =
        OffsetDateTime::from_unix_timestamp(first_admission.market.observed_at_unix).unwrap();
    let live_venue = GoldenLiveVenue;
    let live_executor = LiveExecutor::new(&live_venue, &live_journal);
    let live_prepared = match live_executor.prepare(live_request, live_now).await.unwrap() {
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
    let paper_wrapper = serde_json::to_vec(&first_prepared_record).unwrap();
    let live_wrapper = serde_json::to_vec(live_audit).unwrap();
    assert_eq!(
        blake3::hash(&paper_wrapper) != blake3::hash(&live_wrapper),
        expected["wrapper_hashes_differ"].as_bool().unwrap()
    );

    let continuation = first_continuation;
    let first_bodies = first_bodies.unwrap();
    let tamper_started = Instant::now();
    let missing_path = dir.path().join("missing-source.log");
    let mut missing_writer = Writer::open(&missing_path).unwrap();
    append_source_direct(
        &mut missing_writer,
        "pe-service.boundary",
        &initial_boundary_payload,
        anchor_cutoff,
    );
    append_source_direct(
        &mut missing_writer,
        "polymarket-activity-ws",
        &first_bodies.websocket,
        FIXED_UNIX,
    );
    drop(missing_writer);
    let missing_error = continuation
        .observation_from_source_log(&missing_path)
        .unwrap_err();
    assert!(matches!(
        missing_error,
        DecisionContinuationError::SourceReceiptMissing { sequence }
            if sequence == continuation.page_occurrences[0].receipt.sequence.0
    ));
    assert_eq!(
        missing_error.to_string(),
        format!(
            "source receipt sequence {} is absent",
            continuation.page_occurrences[0].receipt.sequence.0
        )
    );
    let missing_output_path = dir.path().join("missing-qualification.json");
    let missing_output = run_qualify_cli(
        &paper_path,
        &missing_path,
        &state_path,
        seal_receipt,
        &missing_output_path,
    );
    assert!(missing_output.status.success());
    assert!(
        String::from_utf8_lossy(&missing_output.stdout).contains("verdict=InsufficientEvidence")
    );
    let missing_report: QualificationReport =
        serde_json::from_slice(&std::fs::read(missing_output_path).unwrap()).unwrap();
    assert_eq!(
        missing_report.verdict,
        QualificationVerdict::InsufficientEvidence
    );
    assert!(!missing_report.replay.exact);

    let tampered_path = dir.path().join("tampered-source.log");
    let mut tampered_writer = Writer::open(&tampered_path).unwrap();
    append_source_direct(
        &mut tampered_writer,
        "pe-service.boundary",
        &initial_boundary_payload,
        anchor_cutoff,
    );
    append_source_direct(
        &mut tampered_writer,
        "polymarket-activity-ws",
        &first_bodies.websocket,
        FIXED_UNIX,
    );
    let mut tampered_activity: serde_json::Value =
        serde_json::from_slice(&first_bodies.activity).unwrap();
    tampered_activity[0]["price"] = "0.51".into();
    append_source_direct(
        &mut tampered_writer,
        "polymarket-public.activity-reconciliation",
        &serde_json::to_vec(&tampered_activity).unwrap(),
        FIXED_UNIX,
    );
    drop(tampered_writer);
    let tampered_error = continuation
        .observation_from_source_log(&tampered_path)
        .unwrap_err();
    assert!(matches!(
        tampered_error,
        DecisionContinuationError::SourceReceiptMismatch { sequence }
            if sequence == continuation.page_occurrences[0].receipt.sequence.0
    ));
    assert_eq!(
        tampered_error.to_string(),
        format!(
            "source receipt sequence {} does not match its frozen evidence",
            continuation.page_occurrences[0].receipt.sequence.0
        )
    );
    let tampered_output_path = dir.path().join("tampered-qualification.json");
    let tampered_output = run_qualify_cli(
        &paper_path,
        &tampered_path,
        &state_path,
        seal_receipt,
        &tampered_output_path,
    );
    assert!(tampered_output.status.success());
    assert!(
        String::from_utf8_lossy(&tampered_output.stdout).contains("verdict=InsufficientEvidence")
    );
    let tampered_report: QualificationReport =
        serde_json::from_slice(&std::fs::read(tampered_output_path).unwrap()).unwrap();
    assert_eq!(
        tampered_report.verdict,
        QualificationVerdict::InsufficientEvidence
    );
    assert!(!tampered_report.replay.exact);

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
        "PASS: I16-GOLDEN-PREIMAGE-V1 — missing and tampered sequence {} fail with exact typed reasons",
        continuation.page_occurrences[0].receipt.sequence.0
    );
}
