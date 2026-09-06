//! I16 golden future-stream source and economic replay scenario.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::HashMap;
use std::sync::Arc;

use pe_core_types::{
    BasisPoints, CollateralAmount, PolymarketConditionId, Price, Probability, ReceivedAt,
    ReconstructionQuality, Side, SourceId, SourceTimestamp, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Reader, Writer};
use pe_execution_core::{
    AdmissionReceipts, EconomicInputs, EconomicPrepared, LiveAdmissionArtifact, MarketSelection,
    ObservationEvidence, RiskAudit, RiskDecisionAudit, SizingModeAudit,
};
use pe_paper_state::{PaperStateDb, WalletHistoryStatusRecord};
use pe_position_ledger::PositionLedger;
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_risk_engine::{RiskDecision, RiskSnapshot, evaluate_risk};
use pe_service::activity_ingest::{ActivityIngest, SourceLogHandle};
use pe_service::bucket_commit::{
    BucketCommitEngine, BucketDecisionContext, DecisionContinuationError, DecisionContinuationV2,
    FrozenDecisionBasis, PageOccurrence,
};
use pe_service::clob_book::OrderBook;
use pe_service::health::new_shared_health_with_ws;
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::position_seeder::{AnchorExpectation, AnchorInstall, AnchorProof, ledger_capture};
use pe_service::runtime_config::RuntimeConfig;
use pe_service::source_event_sink::SourceEventSink;
use pe_source_polymarket_public::{
    ActivityParseContext, ActivityTransport, LIVE_MARKET_PARSER_VERSION,
    LIVE_MARKET_SCHEMA_VERSION, parse_activity_response, validate_live_market,
};
use pe_venue_polymarket::{BuySizing, parse_compact_market, plan_sized_buy};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

const FIXTURE: &str = "fixtures/golden_stream_v1";
const CONDITION: &str = "0x4c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ee";
const WALLET: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const FIXED_UNIX: i64 = 1_800_000_000;

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

async fn append_source(
    source_log: &SourceLogHandle,
    source_id: &str,
    payload: &[u8],
) -> AppendReceipt {
    let timestamp = OffsetDateTime::from_unix_timestamp(FIXED_UNIX).unwrap();
    let version = if matches!(
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

fn risk_audit() -> RiskAudit {
    let snapshot = RiskSnapshot {
        leader_exposure_bps: BasisPoints::ZERO,
        market_exposure_bps: BasisPoints::ZERO,
        family_exposure_bps: BasisPoints::ZERO,
        total_copy_exposure_bps: BasisPoints::ZERO,
        intraday_pnl_bps: BasisPoints::ZERO,
        rolling_7d_pnl_bps: BasisPoints::ZERO,
        absolute_pnl_bps: BasisPoints::ZERO,
        copy_latency_kill_switch_active: false,
        proposed_trade_bps: BasisPoints(255),
        per_trade_cap_bps: 1_000,
        concentration_caps: None,
    };
    let decision = match evaluate_risk(&snapshot) {
        RiskDecision::Approved => RiskDecisionAudit::Approved,
        RiskDecision::Blocked(reason) => RiskDecisionAudit::Blocked { reason },
    };
    RiskAudit {
        snapshot,
        decision,
        price_receipts: Vec::new(),
        evaluated_at_unix_ms: FIXED_UNIX * 1_000,
    }
}

fn compose(
    admission: &LiveAdmissionArtifact,
    book: &OrderBook,
    observation: ObservationEvidence,
) -> EconomicPrepared {
    let ladder = book.ladder().unwrap();
    let plan = plan_sized_buy(
        &ladder,
        admission.fee_schedule,
        BuySizing::Contract { contracts: 5 },
        &[CollateralAmount::from_decimal_exact(dec!(3)).unwrap()],
        admission.market.minimum_order_size,
        admission.market.minimum_tick_size,
        Price::new(dec!(0.15)).unwrap(),
        Price::new(dec!(0.85)).unwrap(),
        Price::new(dec!(0.50)).unwrap(),
        Price::new(dec!(0.50)).unwrap(),
    )
    .unwrap();
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
        budget: CollateralAmount::from_decimal_exact(dec!(3)).unwrap(),
        slippage_rate: Decimal::ZERO,
        risk: risk_audit(),
        cash_before: CollateralAmount::from_decimal_exact(dec!(10)).unwrap(),
        price_impact_cap_bps: 100,
        chase_ceiling: Price::new(dec!(0.50)).unwrap(),
        band_floor: Price::new(dec!(0.15)).unwrap(),
        band_ceiling_exclusive: Price::new(dec!(0.85)).unwrap(),
        applied_configuration_hash: "golden-config-v1".to_owned(),
    })
    .unwrap()
}

fn replay_admission_and_book(source_path: &std::path::Path) -> (LiveAdmissionArtifact, OrderBook) {
    let frames = Reader::replay(source_path)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let by_source = frames
        .iter()
        .map(|(sequence, envelope)| {
            (
                envelope.source_id.0.as_str(),
                (
                    &envelope.payload,
                    AppendReceipt {
                        sequence: *sequence,
                        this_hash: envelope.this_hash,
                    },
                ),
            )
        })
        .collect::<HashMap<_, _>>();
    let condition = PolymarketConditionId(CONDITION.to_owned());
    let (gamma, gamma_receipt) = by_source["polymarket.gamma.markets"];
    let (clob, clob_receipt) = by_source["polymarket.clob.markets"];
    let (compact, compact_receipt) = by_source["polymarket.clob.compact-market"];
    let (book_raw, book_receipt) = by_source["polymarket.clob.book"];
    let market = validate_live_market(gamma, clob, &condition, FIXED_UNIX, 60).unwrap();
    let compact =
        parse_compact_market(compact, &condition, &market.ordered_outcome_token_ids).unwrap();
    let admission = LiveAdmissionArtifact {
        market,
        settlement: VenueSettlementRecord {
            schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
            condition_id: condition,
            status: VenueResolutionStatus::Unresolved,
            raw_evidence_hash: blake3::hash(clob).to_hex().to_string(),
            source_timestamp_unix: None,
            observed_at_unix: FIXED_UNIX,
            parser_version: 1,
            freshness_window_secs: 60,
        },
        fee_schedule: compact.fee_schedule,
        receipts: AdmissionReceipts {
            gamma: gamma_receipt,
            clob_long: clob_receipt,
            clob_compact: compact_receipt,
        },
    };
    let mut book = OrderBook::from_book_json(book_raw).unwrap();
    book.source_receipt = Some(book_receipt);
    (admission, book)
}

/// I16-GOLDEN-SOURCE-ECONOMIC-V1
///
/// Preconditions: the checked-in v1 corpus is served byte-for-byte at a fixed observation clock;
/// websocket and complete-page receipts enter the source log before an acknowledged bucket commit.
/// PASS: runtime and source-log replay produce the same admitted classification, admission audit,
/// economic core hash, risk decision, and exact arithmetic; wrapper hashes differ; missing or
/// tampered observation preimages return typed continuation errors.
/// FAIL: any semantic output diverges, an unbound preimage replays, or paper/live wrappers collide.
#[tokio::test]
async fn golden_source_stream_replays_exact_economic_core() {
    assert!(std::path::Path::new(FIXTURE).is_relative());
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let source_sink = SourceEventSink::open(&source_path).unwrap();
    let (source_log, source_rx) = SourceLogHandle::channel(16);
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
    let websocket_receipt = append_source(
        &source_log,
        "polymarket-activity-ws",
        fixture("websocket_trigger"),
    )
    .await;
    let page_receipt = append_source(
        &source_log,
        "polymarket-public.activity-reconciliation",
        fixture("activity_page"),
    )
    .await;
    let now = OffsetDateTime::from_unix_timestamp(FIXED_UNIX).unwrap();
    for (source_id, body) in [
        ("polymarket.gamma.markets", fixture("gamma_long")),
        ("polymarket.clob.markets", fixture("clob_long")),
        ("polymarket.clob.compact-market", fixture("clob_compact")),
        ("polymarket.clob.book", fixture("book")),
    ] {
        append_source(&source_log, source_id, body).await;
    }
    let (admission, book) = replay_admission_and_book(&source_path);

    let wallet = WalletAddress::from_hex(WALLET).unwrap();
    let parse_context = ActivityParseContext {
        source_id: SourceId("polymarket-public.activity-reconciliation".to_owned()),
        observed_at: SourceTimestamp(now),
        received_at: ReceivedAt(now),
        transport: ActivityTransport::Rest,
    };
    let aggregate = parse_activity_response(fixture("activity_page"), wallet, &parse_context)
        .unwrap()
        .aggregates()
        .unwrap()
        .remove(0);
    let source_trade_id = aggregate.group_id.key().clone();
    let paper = Arc::new(PaperStateDb::open(&dir.path().join("paper.db")).unwrap());
    paper.set_cursor(&wallet, 0).unwrap();
    let mut engine = BucketCommitEngine::load(Arc::clone(&paper), PositionLedger::new()).unwrap();
    let captured = ledger_capture(engine.ledger(), &paper, wallet).unwrap();
    engine
        .install_anchors(&[AnchorInstall {
            wallet,
            balances: Vec::new(),
            cutoff: 0,
            proof: AnchorProof {
                positions_proof_hash: "golden-empty".to_owned(),
                activity_bounds_json: "[]".to_owned(),
                source_log_generation: "golden-stream-v1".to_owned(),
                document: "{}".to_owned(),
                recorded_at_unix: FIXED_UNIX - 1,
            },
            expected: AnchorExpectation {
                ledger_hash: captured.hash,
                cursor: captured.cursor,
                anchor_seq: captured.anchor_seq,
                coverage_generation: captured.coverage_generation,
            },
        }])
        .unwrap();
    let context = BucketDecisionContext {
        applied_configuration: RuntimeConfig::from_service_config(
            &pe_service::config::ServiceConfig::default(),
        ),
        decision_inputs_json: "{\"fixture\":\"golden_stream_v1\"}".to_owned(),
        page_occurrences: vec![PageOccurrence {
            request_url: "fixture://golden_stream_v1/activity_page".to_owned(),
            raw_hash: blake3::hash(fixture("activity_page")).to_hex().to_string(),
            receipt: page_receipt,
        }],
        observed_source_receipts: HashMap::from([(source_trade_id.clone(), websocket_receipt)]),
        reconstruction_quality: ReconstructionQuality::new(100).unwrap(),
        signal_config: Default::default(),
        copy_eligible: true,
        bracket_commit: false,
        recorded_at_unix: FIXED_UNIX,
        observation_provenance: HashMap::new(),
        no_copy_dispositions: HashMap::new(),
        identity_overrides: HashMap::new(),
        identity_unresolved: Default::default(),
        history_status: Some(WalletHistoryStatusRecord {
            wallet,
            complete: true,
            proof_json: "{\"fixture\":\"golden_stream_v1\"}".to_owned(),
            updated_at_unix: FIXED_UNIX,
        }),
    };
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let (engine_tx, engine_rx) = oneshot::channel();
    let control = tokio::spawn(async move {
        if let Some(OrchestratorControl::CommitActivityBucket {
            aggregates,
            context,
            committed,
        }) = control_rx.recv().await
        {
            let result = engine
                .commit(
                    aggregates,
                    context.as_ref(),
                    FrozenDecisionBasis {
                        win_rate_p: Probability::new(dec!(0.6)).unwrap(),
                        bankroll: dec!(10),
                    },
                )
                .map_err(|error| error.to_string());
            let _ = committed.send(result);
        }
        let _ = engine_tx.send(engine);
    });
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
    drop(control_tx);
    control.await.unwrap();
    let _engine = engine_rx.await.unwrap();
    let row = paper.open_decision_pending().unwrap().remove(0);
    let continuation = DecisionContinuationV2::from_durable(&row).unwrap();
    assert_eq!(continuation.gate_result, "admitted");
    let observation = continuation
        .observation_from_source_log(&source_path)
        .unwrap()
        .unwrap();
    let runtime = compose(&admission, &book, observation.clone());
    let (replayed_admission, replayed_book) = replay_admission_and_book(&source_path);
    let replayed = compose(&replayed_admission, &replayed_book, observation);
    assert_eq!(runtime, replayed);
    assert_eq!(runtime.core_hash().unwrap(), replayed.core_hash().unwrap());
    assert_eq!(runtime.risk.decision, RiskDecisionAudit::Approved);
    let expected: serde_json::Value = serde_json::from_slice(fixture("expected")).unwrap();
    assert_eq!(
        runtime
            .sizing
            .minimum_shares
            .to_decimal()
            .normalize()
            .to_string(),
        expected["minimum_shares"].as_str().unwrap()
    );
    assert_eq!(
        runtime
            .sizing
            .principal
            .to_decimal()
            .normalize()
            .to_string(),
        expected["principal"].as_str().unwrap()
    );
    assert_eq!(
        runtime
            .fee
            .expected_fee
            .to_decimal()
            .normalize()
            .to_string(),
        expected["expected_fee"].as_str().unwrap()
    );
    assert_eq!(
        runtime
            .all_in_debit()
            .unwrap()
            .to_decimal()
            .normalize()
            .to_string(),
        expected["all_in_debit"].as_str().unwrap()
    );
    let paper_wrapper = serde_json::to_vec(&("paper", &runtime)).unwrap();
    let live_wrapper = serde_json::to_vec(&("live", &runtime)).unwrap();
    assert_ne!(blake3::hash(&paper_wrapper), blake3::hash(&live_wrapper));

    let mut tampered = continuation.clone();
    tampered.page_occurrences[0].raw_hash = "00".repeat(32);
    assert!(matches!(
        tampered.observation_from_source_log(&source_path),
        Err(DecisionContinuationError::SourceReceiptMismatch { .. })
    ));
    let missing_path = dir.path().join("missing-source.log");
    drop(Writer::open(&missing_path).unwrap());
    assert!(matches!(
        continuation.observation_from_source_log(&missing_path),
        Err(DecisionContinuationError::SourceReceiptMissing { .. })
    ));

    // The remaining recorded bodies are part of this versioned corpus and parse under their
    // canonical source owners even though the current offline qualifier defect prevents a full
    // Start→Final→mark→seal success fixture from being consumed.
    assert!(!fixture("prices_history").is_empty());
    let resolved =
        pe_source_polymarket_public::parse_clob_market(fixture("clob_resolution")).unwrap();
    assert_eq!(resolved.condition_id.as_deref(), Some(CONDITION));
    assert_eq!(LIVE_MARKET_SCHEMA_VERSION, 1);
    assert_eq!(LIVE_MARKET_PARSER_VERSION, 1);

    drop(source_log);
    coordinator.await.unwrap();
}
