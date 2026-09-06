//! I16 golden future-stream source and economic replay scenario.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;

use pe_core_types::{
    BasisPoints, CollateralAmount, MarketId, OutcomeId, PolymarketConditionId, Price, Probability,
    ReceivedAt, ReconstructionQuality, Side, SourceId, SourceTimestamp, TraderId, VenueMarketId,
    WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Reader, Scanner, Writer};
use pe_execution_core::{
    AdmissionReceipts, EconomicInputs, EconomicPrepared, LiveAdmissionArtifact, MarketSelection,
    ObservationEvidence, RiskAudit, RiskDecisionAudit, SizingModeAudit,
};
use pe_paper_state::{FillRecord, PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_risk_engine::{
    BinaryPayout, RiskDecision, RiskSnapshot, aggregate_resolution_credit, evaluate_risk,
    exposure_bps_ceil,
};
use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
use pe_service::bucket_commit::{
    BucketCommitEngine, BucketDecisionContext, DecisionContinuationError, DecisionContinuationV2,
    FrozenDecisionBasis, PageOccurrence,
};
use pe_service::clob_book::OrderBook;
use pe_service::decision_replay::{
    AuthorityEvidence, DecisionPostBoundaryEvidence, DecisionPostBoundaryEvidenceBody,
    TERMINAL_EVIDENCE_VERSION, TerminalDispositionEvidence, replay_decision_pending,
};
use pe_service::health::new_shared_health_with_ws;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{
    CanonicalFillResult, CanonicalResolutionResult, ExpectedAuthority, FINANCIAL_SEMANTIC_VERSION,
    FinancialPayload, FinancialResult, PAPER_LOG_SCHEMA_VERSION, PaperFillOperationIdentity,
    PaperLogRecord, PaperMarkPrice, PortfolioMark, QualificationSealed, QualificationStarted,
    SealReason, TailBinding, paper_era, scan_paper_log,
};
use pe_service::position_seeder::{AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture};
use pe_service::qualification::{
    QualificationReport, QualificationVerdict, qualification_completion,
};
use pe_service::risk_inputs::{build_paper_risk_snapshot, latest_completed_prepared};
use pe_service::runtime_config::RuntimeConfig;
use pe_service::source_event_sink::SourceEventSink;
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityTransport, BinaryPayoutVector, CLOB_RESOLUTION_PARSER_VERSION,
    CLOB_RESOLUTION_SCHEMA_VERSION, LIVE_MARKET_PARSER_VERSION, LIVE_MARKET_SCHEMA_VERSION,
    parse_activity_response, validate_live_market,
};
use pe_venue_polymarket::{BuySizing, SizedBuyPlan, parse_compact_market, plan_sized_buy};
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

fn current_tail(path: &std::path::Path) -> TailBinding {
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

fn risk_audit(snapshot: RiskSnapshot, evaluated_at_unix: i64) -> RiskAudit {
    let decision = match evaluate_risk(&snapshot) {
        RiskDecision::Approved => RiskDecisionAudit::Approved,
        RiskDecision::Blocked(reason) => RiskDecisionAudit::Blocked { reason },
    };
    RiskAudit {
        snapshot,
        decision,
        price_receipts: Vec::new(),
        evaluated_at_unix_ms: evaluated_at_unix.checked_mul(1_000).unwrap(),
    }
}

fn golden_plan(
    admission: &LiveAdmissionArtifact,
    book: &OrderBook,
    cash_before: CollateralAmount,
) -> SizedBuyPlan {
    let ladder = book.ladder().unwrap();
    let proportional_cap = CollateralAmount::from_decimal_exact(
        cash_before
            .to_decimal()
            .checked_mul(dec!(0.1))
            .unwrap()
            .round_dp_with_strategy(6, rust_decimal::RoundingStrategy::ToZero),
    )
    .unwrap();
    plan_sized_buy(
        &ladder,
        admission.fee_schedule,
        BuySizing::Contract { contracts: 5 },
        &[cash_before, proportional_cap],
        admission.market.minimum_order_size,
        admission.market.minimum_tick_size,
        Price::new(dec!(0.15)).unwrap(),
        Price::new(dec!(0.85)).unwrap(),
        Price::new(dec!(0.50)).unwrap(),
        Price::new(dec!(0.50)).unwrap(),
    )
    .unwrap()
}

fn compose(
    admission: &LiveAdmissionArtifact,
    book: &OrderBook,
    observation: ObservationEvidence,
    risk: RiskAudit,
    cash_before: CollateralAmount,
    applied_configuration_hash: String,
) -> EconomicPrepared {
    let plan = golden_plan(admission, book, cash_before);
    EconomicPrepared::compose(EconomicInputs {
        market: MarketSelection {
            condition_id: admission.market.condition_id.clone(),
            outcome_index: 0,
            token_id: admission.market.ordered_outcome_token_ids[0].clone(),
            side: Side::Buy,
            market_id: admission.market.condition_id.0.clone(),
        },
        admission,
        plan: &plan.ladder,
        book_receipt: book.source_receipt.unwrap(),
        observation: Some(observation),
        sizing_mode: SizingModeAudit::Contract { contracts: 5 },
        budget: plan.budget,
        slippage_rate: Decimal::ZERO,
        risk,
        cash_before,
        price_impact_cap_bps: 300,
        chase_ceiling: Price::new(dec!(0.50)).unwrap(),
        band_floor: Price::new(dec!(0.15)).unwrap(),
        band_ceiling_exclusive: Price::new(dec!(0.85)).unwrap(),
        applied_configuration_hash,
    })
    .unwrap()
}

fn replay_payload(
    source_path: &std::path::Path,
    receipt: AppendReceipt,
) -> (Vec<u8>, pe_event_log::EventEnvelope) {
    let frames = Reader::replay(source_path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let (_, envelope) = frames
        .into_iter()
        .find(|(sequence, envelope)| {
            *sequence == receipt.sequence && envelope.this_hash == receipt.this_hash
        })
        .unwrap();
    (envelope.payload.clone(), envelope)
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

fn replay_admission_and_book(
    source_path: &std::path::Path,
    condition: &PolymarketConditionId,
    observed_at_unix: i64,
    receipts: AdmissionReceipts,
    book_receipt: AppendReceipt,
) -> (LiveAdmissionArtifact, OrderBook) {
    let (gamma, _) = replay_payload(source_path, receipts.gamma);
    let (clob, _) = replay_payload(source_path, receipts.clob_long);
    let (compact, _) = replay_payload(source_path, receipts.clob_compact);
    let (book, _) = replay_payload(source_path, book_receipt);
    admission_and_book(
        condition,
        observed_at_unix,
        GoldenAdmissionBodies {
            gamma: &gamma,
            clob: &clob,
            compact: &compact,
            book: &book,
        },
        receipts,
        book_receipt,
    )
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
/// `SourceReceiptMismatch` with the exact sequence/reason.
/// FAIL: either incomplete source log replays, returns an untyped error, or names another reason.
#[tokio::test]
async fn golden_source_stream_replays_exact_economic_core() {
    assert!(std::path::Path::new(FIXTURE).is_relative());
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let paper_path = dir.path().join("paper.log");
    let live_path = dir.path().join("live.log");
    let state_path = dir.path().join("paper.db");
    drop(Writer::open(&source_path).unwrap());
    drop(Writer::open(&paper_path).unwrap());
    drop(Writer::open(&live_path).unwrap());

    let anchor_cutoff = FIXED_UNIX - FIXED_UNIX.rem_euclid(DAY_SECS);
    let start_unix = anchor_cutoff - 1;
    let runtime_config =
        RuntimeConfig::from_service_config(&pe_service::config::ServiceConfig::default());
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

    let paper = Arc::new(PaperStateDb::open(&state_path).unwrap());
    paper
        .reset_financial_era(
            start_receipt,
            CollateralAmount::from_decimal_exact(STARTING_BANKROLL).unwrap(),
        )
        .unwrap();

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
    append_paper_record(
        &mut paper_writer,
        &PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
            boundary_receipt: initial_boundary,
            cutoff_unix: anchor_cutoff,
            source_tail: current_tail(&source_path),
            financial_prefix_seq: None,
            prices: Vec::<PaperMarkPrice>::new(),
            cash: STARTING_BANKROLL,
            equity: STARTING_BANKROLL,
            invalid: None,
        })),
        anchor_cutoff,
    );

    for wallet in &wallets {
        paper.set_cursor(wallet, 0).unwrap();
    }
    let mut engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    let installs = wallets
        .iter()
        .map(|wallet| {
            let captured = ledger_capture(engine.ledger(), &paper, *wallet).unwrap();
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
    engine.install_anchors(&installs).unwrap();
    let (control_tx, mut control_rx) = mpsc::channel(4);
    let control = tokio::spawn(async move {
        while let Some(command) = control_rx.recv().await {
            if let OrchestratorControl::CommitActivityBucket {
                aggregates,
                context,
                committed,
            } = command
            {
                let result = engine
                    .commit(
                        aggregates,
                        context.as_ref(),
                        FrozenDecisionBasis {
                            win_rate_p: Probability::new(dec!(0.6)).unwrap(),
                            bankroll: STARTING_BANKROLL,
                        },
                    )
                    .map_err(|error| error.to_string());
                let _ = committed.send(result);
            }
        }
    });

    let expected: serde_json::Value = serde_json::from_slice(fixture("expected")).unwrap();
    let bankroll_amount = CollateralAmount::from_decimal_exact(STARTING_BANKROLL).unwrap();
    let mut prior_completed_prepared_sequence = None;
    let mut runtime_core_hashes = Vec::new();
    let mut runtime_risks = Vec::new();
    let mut first_continuation = None;
    let mut first_bodies = None;

    for day in 0..QUALIFICATION_DAYS {
        for within_day in 0..COPIES_PER_DAY {
            let index = day * COPIES_PER_DAY + within_day;
            let source_unix = golden_source_unix(anchor_cutoff, index);
            let bodies = golden_trade_bodies(index, source_unix);
            let now = OffsetDateTime::from_unix_timestamp(source_unix).unwrap();
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
            control_tx
                .send(OrchestratorControl::CommitActivityBucket {
                    aggregates: vec![aggregate],
                    context: Arc::new(context),
                    committed,
                })
                .await
                .unwrap();
            let result = acknowledgement.await.unwrap().unwrap();
            assert_eq!(result.dispositions[&source_trade_id.0], "decision_pending");
            let row = paper
                .open_decision_pending()
                .unwrap()
                .into_iter()
                .find(|row| row.source_trade_id == source_trade_id)
                .unwrap();
            let continuation = DecisionContinuationV2::from_durable(&row).unwrap();
            assert_eq!(continuation.gate_result, "admitted");
            let observation = continuation
                .observation_from_source_log(&source_path)
                .unwrap()
                .unwrap();

            let financial_snapshot = paper.financial_snapshot(source_unix).unwrap();
            assert!(financial_snapshot.positions.is_empty());
            let cash_before =
                CollateralAmount::from_decimal_exact(financial_snapshot.cash).unwrap();
            let plan = golden_plan(&admission, &book, cash_before);
            let proposed_debit = plan.worst_case_all_in_debit().unwrap();
            let base_risk = RiskSnapshot {
                leader_exposure_bps: BasisPoints::ZERO,
                market_exposure_bps: BasisPoints::ZERO,
                family_exposure_bps: BasisPoints::ZERO,
                total_copy_exposure_bps: BasisPoints::ZERO,
                intraday_pnl_bps: BasisPoints::ZERO,
                rolling_7d_pnl_bps: BasisPoints::ZERO,
                absolute_pnl_bps: BasisPoints::ZERO,
                copy_latency_kill_switch_active: false,
                proposed_trade_bps: exposure_bps_ceil(proposed_debit, bankroll_amount).unwrap(),
                per_trade_cap_bps: 1_000,
                concentration_caps: None,
            };
            let era = paper_era(scan_paper_log(&paper_path).unwrap());
            assert_eq!(
                financial_snapshot.start,
                era.start
                    .as_ref()
                    .map(|(receipt, _)| (receipt.sequence, receipt.this_hash)),
                "financial Start mismatch at ordinal {index}"
            );
            assert_eq!(
                financial_snapshot.last_prepared_seq,
                latest_completed_prepared(&era),
                "financial sequence mismatch at ordinal {index}"
            );
            let risk_snapshot = build_paper_risk_snapshot(
                &base_risk,
                &financial_snapshot,
                &era,
                &HashMap::new(),
                &source_path,
                source_unix,
                false,
            )
            .unwrap();
            let risk = risk_audit(risk_snapshot, source_unix);
            assert_eq!(risk.decision, RiskDecisionAudit::Approved);
            let economic = compose(
                &admission,
                &book,
                observation.clone(),
                risk.clone(),
                cash_before,
                applied_configuration_hash.clone(),
            );
            runtime_risks.push(risk);
            runtime_core_hashes.push(economic.core_hash().unwrap());

            if index == 0 {
                let (replayed_admission, replayed_book) = replay_admission_and_book(
                    &source_path,
                    &bodies.condition,
                    source_unix,
                    receipts,
                    book_receipt,
                );
                assert_eq!(admission, replayed_admission);
                let replayed = compose(
                    &replayed_admission,
                    &replayed_book,
                    observation.clone(),
                    runtime_risks[0].clone(),
                    CollateralAmount::from_decimal_exact(financial_snapshot.cash).unwrap(),
                    applied_configuration_hash.clone(),
                );
                assert_eq!(economic, replayed);
                assert_eq!(economic.core_hash().unwrap(), replayed.core_hash().unwrap());
                first_continuation = Some(continuation.clone());
                first_bodies = Some(bodies.clone());
            }

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

            let operation = PaperFillOperationIdentity {
                leader_wallet: bodies.wallet,
                source_trade_id: source_trade_id.clone(),
                observed_at_bucket: source_unix,
            };
            let expected_authority = ExpectedAuthority {
                qualification_start_receipt: start_receipt,
                prior_completed_prepared_sequence,
            };
            let prepared_record = PaperLogRecord::FinancialPrepared {
                expected_authority: expected_authority.clone(),
                payload: FinancialPayload::Fill {
                    operation: operation.clone(),
                    economic: economic.clone(),
                },
            };
            let paper_wrapper = serde_json::to_vec(&prepared_record).unwrap();
            let live_wrapper = serde_json::to_vec(&serde_json::json!({
                "event": "order_prepared",
                "economic": &economic,
            }))
            .unwrap();
            assert_ne!(blake3::hash(&paper_wrapper), blake3::hash(&live_wrapper));
            let prepared_receipt =
                append_paper_record(&mut paper_writer, &prepared_record, source_unix);
            let cash_after_fill = financial_snapshot
                .cash
                .checked_sub(economic.all_in_debit().unwrap().to_decimal())
                .unwrap();
            let canonical_fill = CanonicalFillResult {
                outcome: "applied".to_owned(),
                bankroll: cash_after_fill,
                applied_prepared_seq: prepared_receipt.sequence,
                quantity: economic.sizing.expected_shares,
                principal: economic.sizing.principal,
                fee: economic.fee.expected_fee,
                fill_price: economic.sizing.expected_vwap,
            };
            let idempotency_key = pe_strategy_winner_follow::evaluate::build_idempotency_key_parts(
                &TraderId(bodies.wallet).to_string(),
                &source_trade_id.0,
                &economic.market.market_id,
                u16::from(economic.market.outcome_index),
                economic.market.side,
                source_unix,
            );
            paper
                .apply_financial_fill(
                    start_receipt,
                    prior_completed_prepared_sequence,
                    prepared_receipt.sequence,
                    observation.source_receipt,
                    source_unix,
                    &FillRecord {
                        idempotency_key,
                        market_id: MarketId(VenueMarketId(economic.market.market_id.clone())),
                        outcome_id: OutcomeId(u16::from(economic.market.outcome_index)),
                        side: economic.market.side,
                        quantity: canonical_fill.quantity,
                        fill_price: canonical_fill.fill_price,
                        principal: canonical_fill.principal,
                        fee: canonical_fill.fee,
                    },
                    cash_after_fill,
                )
                .unwrap();
            let final_receipt = append_paper_record(
                &mut paper_writer,
                &PaperLogRecord::FinancialFinal {
                    prepared_receipt,
                    result: FinancialResult::Fill {
                        canonical: canonical_fill.clone(),
                    },
                },
                source_unix,
            );
            let terminal =
                DecisionPostBoundaryEvidence::from_body(DecisionPostBoundaryEvidenceBody {
                    version: TERMINAL_EVIDENCE_VERSION,
                    owners: vec!["source_log".to_owned(), "paper_log".to_owned()],
                    source_trade_id: source_trade_id.clone(),
                    applied_configuration_hash: continuation.applied_configuration_hash.clone(),
                    market_end: None,
                    market_price: None,
                    book: None,
                    clocks: Vec::new(),
                    authority: AuthorityEvidence {
                        kind: "commit_fill_v2".to_owned(),
                        outcome: "applied".to_owned(),
                        bankroll: Some(cash_after_fill.normalize().to_string()),
                    },
                    terminal: TerminalDispositionEvidence::final_fill(final_receipt),
                })
                .unwrap();
            paper
                .close_decision_pending(
                    &source_trade_id,
                    &serde_json::to_string(&terminal).unwrap(),
                    "fill",
                    source_unix,
                )
                .unwrap();
            let terminal_row = paper
                .decision_pending_history()
                .unwrap()
                .into_iter()
                .find(|row| row.source_trade_id == source_trade_id)
                .unwrap();
            let replayed_decision = replay_decision_pending(&terminal_row).unwrap();
            assert_eq!(replayed_decision.continuation, continuation);
            assert_eq!(
                replayed_decision.post_boundary.body.terminal.final_receipt,
                Some(final_receipt)
            );
            prior_completed_prepared_sequence = Some(prepared_receipt.sequence);

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
            let payout_credit = aggregate_resolution_credit(
                &[(0, economic.sizing.expected_shares)],
                &binary_payout,
            )
            .unwrap();
            assert_eq!(
                payout_credit.to_decimal().normalize().to_string(),
                expected["payout_credit"].as_str().unwrap()
            );
            let resolution_prepared = append_paper_record(
                &mut paper_writer,
                &PaperLogRecord::FinancialPrepared {
                    expected_authority: ExpectedAuthority {
                        qualification_start_receipt: start_receipt,
                        prior_completed_prepared_sequence,
                    },
                    payload: FinancialPayload::Resolution {
                        condition_id: bodies.condition.clone(),
                        payout_by_outcome_index_json: payout_json.clone(),
                        resolution_source_receipt: resolution_receipt,
                    },
                },
                resolution_unix,
            );
            let cash_after_resolution = cash_after_fill
                .checked_add(payout_credit.to_decimal())
                .unwrap();
            paper
                .apply_financial_resolution(
                    start_receipt,
                    prior_completed_prepared_sequence,
                    resolution_prepared.sequence,
                    &MarketId(VenueMarketId(bodies.condition.0.clone())),
                    &payout_json,
                    resolution_receipt,
                    resolution_unix,
                    payout_credit,
                    cash_after_resolution,
                )
                .unwrap();
            append_paper_record(
                &mut paper_writer,
                &PaperLogRecord::FinancialFinal {
                    prepared_receipt: resolution_prepared,
                    result: FinancialResult::Resolution {
                        canonical: CanonicalResolutionResult {
                            outcome: "applied".to_owned(),
                            bankroll: cash_after_resolution,
                            applied_prepared_seq: resolution_prepared.sequence,
                            credit: payout_credit,
                            settled_at_unix: resolution_unix,
                        },
                    },
                },
                resolution_unix,
            );
            prior_completed_prepared_sequence = Some(resolution_prepared.sequence);
        }

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
        append_paper_record(
            &mut paper_writer,
            &PaperLogRecord::PortfolioMark(Box::new(PortfolioMark {
                boundary_receipt,
                cutoff_unix: cutoff,
                source_tail: current_tail(&source_path),
                financial_prefix_seq: prior_completed_prepared_sequence,
                prices: Vec::new(),
                cash: snapshot.cash,
                equity: snapshot.cash,
                invalid: None,
            })),
            cutoff,
        );
    }

    drop(control_tx);
    control.await.unwrap();
    drop(source_log);
    coordinator.await.unwrap();
    assert!(paper.open_decision_pending().unwrap().is_empty());
    let before_seal = paper_era(scan_paper_log(&paper_path).unwrap());
    let completion = qualification_completion(&before_seal).unwrap();
    assert_eq!(completion.complete_days, QUALIFICATION_DAYS);
    assert_eq!(
        completion.causal_closes,
        QUALIFICATION_DAYS * COPIES_PER_DAY
    );

    let decision_rows = paper.decision_pending_history().unwrap();
    assert_eq!(decision_rows.len(), QUALIFICATION_DAYS * COPIES_PER_DAY);
    assert!(decision_rows.iter().all(|row| {
        replay_decision_pending(row)
            .is_ok_and(|decision| decision.continuation.gate_result == "admitted")
    }));
    let decision_keys = decision_rows
        .iter()
        .map(|row| (row.source_trade_id.clone(), row.semantic_revision.clone()))
        .collect::<Vec<_>>();
    let decision_digest = blake3::hash(&paper.seal_decision_evidence(&decision_keys).unwrap())
        .to_hex()
        .to_string();
    let financial_prefix = current_tail(&paper_path);
    let source_prefix = current_tail(&source_path);
    let sealed_cutoff = anchor_cutoff + i64::try_from(QUALIFICATION_DAYS).unwrap() * DAY_SECS;
    let seal_receipt = append_paper_record(
        &mut paper_writer,
        &PaperLogRecord::QualificationSealed(Box::new(QualificationSealed {
            start_receipt,
            source_prefix,
            financial_prefix,
            decision_evidence_digest: decision_digest,
            sealed_cutoff_unix: sealed_cutoff,
            reason: SealReason::Complete,
        })),
        sealed_cutoff,
    );
    drop(paper_writer);

    let output_path = dir.path().join("qualification.json");
    let output = Command::new(env!("CARGO_BIN_EXE_pe-service"))
        .arg("--qualify")
        .arg("--paper-log")
        .arg(&paper_path)
        .arg("--source-log")
        .arg(&source_path)
        .arg("--paper-state")
        .arg(&state_path)
        .arg("--seal-hash")
        .arg(seal_receipt.this_hash.to_hex().as_str())
        .arg("--output")
        .arg(&output_path)
        .output()
        .unwrap();
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

    let continuation = first_continuation.unwrap();
    let first_bodies = first_bodies.unwrap();
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

    assert!(!fixture("prices_history").is_empty());
    assert_eq!(LIVE_MARKET_SCHEMA_VERSION, 1);
    assert_eq!(LIVE_MARKET_PARSER_VERSION, 1);
    assert_eq!(CLOB_RESOLUTION_PARSER_VERSION, 2);

    println!(
        "PASS: I16-GOLDEN-SOURCE-ECONOMIC-V1 — {} exact decisions, {} exact fills, {} complete days",
        report.replay.decisions, report.replay.fills, report.complete_days
    );
    println!(
        "PASS: I16-GOLDEN-PREIMAGE-V1 — missing and tampered sequence {} fail with exact typed reasons",
        continuation.page_occurrences[0].receipt.sequence.0
    );
}
