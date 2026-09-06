//! #545 causal boundary membership for late fill completion and replay.

#![cfg(feature = "scenario")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects
)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use pe_core_types::{
    BasisPoints, CollateralAmount, KellyFraction, MarketId, OutcomeId, PolymarketConditionId,
    PolymarketTokenId, Price, Probability, ReceivedAt, ShareAmount, Side, SourceId,
    SourceTimestamp, VenueMarketId, WalletAddress,
};
use pe_event_log::{AppendReceipt, ContentType, EnvelopeIn, Scanner, Writer};
use pe_execution_core::{
    AdmissionReceipts, BalanceAudit, ECONOMIC_PREPARED_VERSION, EconomicPrepared, FeeAudit,
    LadderAskAudit, LadderPlanAudit, LiveAdmissionArtifactAudit, LiveMarketEvidenceAudit,
    MarketSelection, ObservationEvidence, RiskAudit, RiskDecisionAudit, SizingAudit,
    SizingModeAudit,
};
use pe_paper_state::{ActivityBucketCommit, ActivityDispositionRecord, FillRecord, PaperStateDb};
use pe_position_ledger::PositionLedger;
use pe_resolver_card::{
    VENUE_SETTLEMENT_SCHEMA_VERSION, VenueResolutionStatus, VenueSettlementRecord,
};
use pe_risk_engine::RiskSnapshot;
use pe_service::activity_ingest::SourceLogHandle;
use pe_service::clob_book::FixtureClobBookFetcher;
use pe_service::entry_gate::CopyEntryGateConfig;
use pe_service::health::new_shared_health;
use pe_service::live_venue_adapter::LiveAdmissionBuilder;
use pe_service::live_watchlist::LiveWatchlist;
use pe_service::mark_prices::HistoricalMarkAdapter;
use pe_service::mid_price_cache::MidPriceCache;
use pe_service::orchestrator::{Orchestrator, OrchestratorConfig};
use pe_service::orchestrator_control::OrchestratorControl;
use pe_service::paper_recovery::{
    CanonicalFillResult, CanonicalResolutionResult, ExpectedAuthority, FinancialPayload,
    FinancialResult, PAPER_LOG_SCHEMA_VERSION, PaperFillOperationIdentity, PaperLogFrame,
    PaperLogRecord, QualificationStarted, TailBinding, paper_era, scan_paper_log,
};
use pe_service::risk_inputs::{SourceReceiptMillisIndex, completed_prepared_before_boundary};
use pe_service::supabase_state::SupabaseStateClient;
use pe_service::trade_poller::{
    ACTIVITY_POLL_SOURCE_ID, DAILY_BOUNDARY_SOURCE_ID, PendingBoundary,
    rebuild_reconciliation_obligations, recover_daily_boundary,
};
use pe_source_polymarket_public::{
    ACTIVITY_PARSER_VERSION, ACTIVITY_SCHEMA_VERSION, CLOB_RESOLUTION_PARSER_VERSION,
    CLOB_RESOLUTION_SCHEMA_VERSION, FixtureFetcher, parse_activity_trade_observation,
};
use pe_strategy_winner_follow::{ExecutionMode, WinnerFollowConfig, WinnerFollowStrategy};
use pe_trader_index::{Watchlist, WatchlistEntry, WatchlistTier};
use pe_venue_polymarket::CompactFeeSchedule;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};

const START_UNIX: i64 = 1_800_000_000;
const CUTOFF_UNIX: i64 = 1_800_057_600;
const WS_PAYLOAD: &[u8] = include_bytes!("fixtures/golden_stream_v1/websocket_trigger.json");
const PAGE_PAYLOAD: &[u8] = include_bytes!("fixtures/golden_stream_v1/activity_page.json");
const RESOLUTION_PAYLOAD: &[u8] = include_bytes!("fixtures/golden_stream_v1/clob_resolution.json");

fn append_source(
    writer: &mut Writer,
    source_id: &str,
    payload: &[u8],
    received_at_unix: i64,
    schema_version: u32,
    parser_version: u32,
) -> AppendReceipt {
    let timestamp = OffsetDateTime::from_unix_timestamp(received_at_unix).unwrap();
    writer
        .append_synced(EnvelopeIn {
            source_id: SourceId(source_id.to_owned()),
            schema_version,
            parser_version,
            observed_at: SourceTimestamp(timestamp),
            received_at: ReceivedAt(timestamp),
            content_type: ContentType::Json,
            payload: payload.to_vec(),
        })
        .unwrap()
}

fn append_paper(
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

fn start_record(source_path: &std::path::Path, paper_path: &std::path::Path) -> PaperLogRecord {
    PaperLogRecord::QualificationStarted(Box::new(QualificationStarted {
        starting_bankroll: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
        paper_prefix: empty_tail(paper_path),
        source_prefix: empty_tail(source_path),
        live_prefix: TailBinding {
            physical_tail: 0,
            last_sequence: None,
            last_hash: blake3::Hash::from_bytes([0; 32]).to_hex().to_string(),
        },
        artifact_blake3: "artifact".to_owned(),
        static_config_hash: "static".to_owned(),
        hot_config_hash: "hot".to_owned(),
        generation: "generation".to_owned(),
        activation_id: "activation".to_owned(),
        ranking_batch_id: 545,
        membership: vec![
            WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
        ],
        membership_proofs_hash: "membership".to_owned(),
        schema_version: 3,
        parser_version: 1,
        financial_semantic_version: 1,
    }))
}

fn economic(
    source_receipt: AppendReceipt,
    complete_bound_receipt: AppendReceipt,
) -> EconomicPrepared {
    let condition = PolymarketConditionId(
        "0x4c27acaae6b9528e6121c226f0c7e253073c0ecdee87eed1bca5b2fe4028e6ee".to_owned(),
    );
    let price = Price::new(dec!(0.5)).unwrap();
    let shares = ShareAmount::from_whole(2).unwrap();
    let principal = CollateralAmount::from_decimal_exact(dec!(1)).unwrap();
    EconomicPrepared {
        version: ECONOMIC_PREPARED_VERSION,
        market: MarketSelection {
            condition_id: condition.clone(),
            outcome_index: 0,
            token_id: PolymarketTokenId("11".to_owned()),
            side: Side::Buy,
            market_id: condition.0.clone(),
        },
        admission: LiveAdmissionArtifactAudit {
            market: LiveMarketEvidenceAudit {
                condition_id: condition.clone(),
                ordered_outcome_token_ids: [
                    PolymarketTokenId("11".to_owned()),
                    PolymarketTokenId("12".to_owned()),
                ],
                neg_risk: false,
                minimum_tick_size: Price::new(dec!(0.01)).unwrap(),
                minimum_order_size: shares,
                observed_at_unix: START_UNIX,
                schema_version: 1,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            settlement: VenueSettlementRecord {
                schema_version: VENUE_SETTLEMENT_SCHEMA_VERSION,
                condition_id: condition,
                status: VenueResolutionStatus::Unresolved,
                raw_evidence_hash: "settlement".to_owned(),
                source_timestamp_unix: Some(START_UNIX),
                observed_at_unix: START_UNIX,
                parser_version: 1,
                freshness_window_secs: 60,
            },
            fee_schedule: CompactFeeSchedule::Zero,
            scheduled_end_unix: Some(CUTOFF_UNIX + 3_600),
            receipts: AdmissionReceipts {
                gamma: source_receipt,
                clob_long: source_receipt,
                clob_compact: source_receipt,
            },
        },
        ladder: LadderPlanAudit {
            used_asks: vec![LadderAskAudit { price, shares }],
            best_ask: price,
            limit_price: price,
            minimum_shares: shares,
            principal,
        },
        book_receipt: source_receipt,
        observation: Some(ObservationEvidence {
            source_receipt,
            complete_bound_receipt,
            observed_unix_ms: (CUTOFF_UNIX - 1) * 1_000,
            provenance: "activity_ws".to_owned(),
        }),
        sizing: SizingAudit {
            mode: SizingModeAudit::Kelly {
                fraction: KellyFraction::new(dec!(0.25)).unwrap(),
                probability: Probability::new(dec!(0.6)).unwrap(),
            },
            budget: principal,
            principal,
            minimum_shares: shares,
            expected_shares: shares,
            expected_vwap: price,
            all_in_price: price,
            slippage_rate: Decimal::ZERO,
        },
        fee: FeeAudit {
            schedule: CompactFeeSchedule::Zero,
            expected_fee: CollateralAmount::ZERO,
            reserve: CollateralAmount::ZERO,
        },
        risk: RiskAudit {
            snapshot: RiskSnapshot {
                leader_exposure_bps: BasisPoints::ZERO,
                market_exposure_bps: BasisPoints::ZERO,
                family_exposure_bps: BasisPoints::ZERO,
                total_copy_exposure_bps: BasisPoints::ZERO,
                intraday_pnl_bps: BasisPoints::ZERO,
                rolling_7d_pnl_bps: BasisPoints::ZERO,
                absolute_pnl_bps: BasisPoints::ZERO,
                copy_latency_kill_switch_active: false,
                proposed_trade_bps: BasisPoints(10),
                per_trade_cap_bps: 25,
                concentration_caps: None,
            },
            decision: RiskDecisionAudit::Approved,
            price_receipts: Vec::new(),
            evaluated_at_unix_ms: CUTOFF_UNIX * 1_000,
        },
        balance: BalanceAudit {
            cash_before: CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
            worst_case_debit: principal,
            price_impact_cap_bps: 100,
            chase_ceiling: price,
            band_floor: Price::ZERO,
            band_ceiling_exclusive: Price::ONE,
        },
        applied_configuration_hash: "config".to_owned(),
    }
}

fn make_watchlist() -> Watchlist {
    let wallet = WalletAddress::from_hex("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let score = BasisPoints(200);
    Watchlist {
        entries: vec![WatchlistEntry {
            wallet,
            tier: WatchlistTier::Active,
            leader_score_bps: score,
            lcb_5pct_bps: score,
            win_rate_bps: BasisPoints(7_000),
            closed_trades_in_window: 0,
            reconstruction_quality: pe_core_types::ReconstructionQuality::new(100).unwrap(),
        }],
        snapshot_at: SourceTimestamp(OffsetDateTime::from_unix_timestamp(START_UNIX).unwrap()),
        active_count: 1,
        incubator_count: 0,
    }
}

async fn run_real_boundary(
    paper_path: &std::path::Path,
    source_path: &std::path::Path,
    paper_writer: Writer,
    state: Arc<PaperStateDb>,
    boundary_receipt: AppendReceipt,
) {
    let (control_tx, control_rx) = mpsc::channel(4);
    let (source_log, _source_rx) = SourceLogHandle::channel(4);
    let http = reqwest::Client::new();
    let mut orchestrator = Orchestrator::new(
        LiveWatchlist::new(make_watchlist()),
        OrchestratorConfig {
            bankroll: dec!(100),
            mode: ExecutionMode::Paper,
            signal_config: pe_copy_signal_engine::SignalConfig::default(),
            max_resolution_horizon_secs: 0,
            min_resolution_horizon_secs: 0,
            max_fill_price: Decimal::ZERO,
            min_fill_price: Decimal::ZERO,
            price_impact_cap_bps: 100,
            entry_gate_config: CopyEntryGateConfig,
            runtime_config: None,
            live_accounts: None,
            activity_ws_enabled: false,
            copy_latency_budget_secs: 2,
            watchlist_writer_lock: None,
        },
        WinnerFollowStrategy::new(WinnerFollowConfig::default()),
        paper_writer,
        state,
        PositionLedger::new(),
        new_shared_health(false),
        MidPriceCache::with_fetcher(FixtureFetcher::new(HashMap::new()), String::new()),
        control_rx,
        None,
        None,
        Some(SupabaseStateClient::new(
            http.clone(),
            "https://offline.invalid",
            "scenario-anon",
            "scenario-secret",
        )),
        Arc::new(FixtureClobBookFetcher::new(HashMap::new())),
    )
    .unwrap();
    orchestrator
        .configure_financial_log_paths(
            paper_path.to_path_buf(),
            source_path.to_path_buf(),
            LiveAdmissionBuilder::new(
                http.clone(),
                "https://offline.invalid",
                "https://offline.invalid",
                source_log.clone(),
            ),
            Arc::new(HistoricalMarkAdapter::new(
                http,
                "https://offline.invalid",
                source_log,
            )),
            SourceReceiptMillisIndex::replay(source_path).unwrap(),
        )
        .unwrap();

    let run = tokio::spawn(orchestrator.run(std::future::pending::<()>()));
    let (acknowledged, acknowledgement) = oneshot::channel();
    control_tx
        .send(OrchestratorControl::DailyBoundary {
            cutoff_unix: CUTOFF_UNIX,
            boundary_receipt,
            acknowledged,
        })
        .await
        .unwrap();
    acknowledgement.await.unwrap().unwrap();
    drop(control_tx);
    run.await.unwrap();
}

fn emitted_marks(paper_path: &std::path::Path) -> Vec<pe_service::paper_recovery::PortfolioMark> {
    paper_era(scan_paper_log(paper_path).unwrap())
        .frames
        .into_iter()
        .filter_map(|frame| match frame.frame {
            PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => Some(*mark),
            _ => None,
        })
        .collect()
}

/// FAIL: the quiet-day mark records the paper-log tail (or any Prepared sequence) instead of `None`,
/// or its equity drifts from the prior mark with no financial fact in between.
/// PASS: a real orchestrator `DailyBoundary` on a quiet day emits a mark whose financial prefix
/// is `None`, even though `QualificationStarted` is the current physical paper-log tail.
#[tokio::test]
async fn quiet_boundary_records_no_causal_prepared_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let paper_path = dir.path().join("paper.log");
    let state_path = dir.path().join("paper.db");
    let mut source_writer = Writer::open(&source_path).unwrap();
    let mut paper_writer = Writer::open(&paper_path).unwrap();
    let start = append_paper(
        &mut paper_writer,
        &start_record(&source_path, &paper_path),
        START_UNIX,
    );
    let state = Arc::new(PaperStateDb::open(&state_path).unwrap());
    state
        .reset_financial_era(
            start,
            CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
        )
        .unwrap();
    let boundary_payload = serde_json::to_vec(&serde_json::json!({
        "kind": "daily_boundary",
        "cutoff_unix": CUTOFF_UNIX,
    }))
    .unwrap();
    let boundary = append_source(
        &mut source_writer,
        DAILY_BOUNDARY_SOURCE_ID,
        &boundary_payload,
        CUTOFF_UNIX,
        1,
        1,
    );
    drop(source_writer);

    run_real_boundary(&paper_path, &source_path, paper_writer, state, boundary).await;

    let marks = emitted_marks(&paper_path);
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].boundary_receipt, boundary);
    assert_eq!(marks[0].financial_prefix_seq, None);
    assert_eq!(marks[0].cash, dec!(100));
    assert_eq!(marks[0].equity, dec!(100));
    assert!(marks[0].prices.is_empty());
    assert_eq!(marks[0].invalid, None);
    let era = paper_era(scan_paper_log(&paper_path).unwrap());
    let completed = completed_prepared_before_boundary(
        &era,
        &source_path,
        CUTOFF_UNIX,
        marks[0].boundary_receipt,
    )
    .unwrap();
    assert!(completed.is_empty());
    let offline = PaperStateDb::open_read_only(&state_path)
        .unwrap()
        .financial_snapshot_before_source_bound(CUTOFF_UNIX, boundary.sequence, &[])
        .unwrap();
    assert_eq!(marks[0].financial_prefix_seq, offline.last_prepared_seq);
}

/// FAIL: the pre-cutoff observation's fill is missing from the mark's debit or positions, or the
/// mark's `financial_prefix_seq` is the Final/tail sequence rather than the causal Prepared sequence.
/// PASS: W at C-2 blocks boundary B until its durable bucket acknowledgement; after a complete
/// page P>B, a causal resolution observation, and late Fill/Resolution Finals, the real boundary
/// handler and a fresh offline reconstruction both select the last causal Prepared sequence.
#[tokio::test]
async fn acknowledged_pre_cutoff_websocket_fill_survives_late_final_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let source_path = dir.path().join("source.log");
    let paper_path = dir.path().join("paper.log");
    let state_path = dir.path().join("paper.db");
    let mut source_writer = Writer::open(&source_path).unwrap();
    let mut paper_writer = Writer::open(&paper_path).unwrap();
    let start = append_paper(
        &mut paper_writer,
        &start_record(&source_path, &paper_path),
        START_UNIX,
    );
    let state = PaperStateDb::open(&state_path).unwrap();
    state
        .reset_financial_era(
            start,
            CollateralAmount::from_decimal_exact(dec!(100)).unwrap(),
        )
        .unwrap();

    let websocket = append_source(
        &mut source_writer,
        "polymarket-activity-ws",
        WS_PAYLOAD,
        CUTOFF_UNIX - 2,
        ACTIVITY_SCHEMA_VERSION,
        ACTIVITY_PARSER_VERSION,
    );
    let resolution = append_source(
        &mut source_writer,
        "polymarket.clob.market",
        RESOLUTION_PAYLOAD,
        CUTOFF_UNIX - 1,
        CLOB_RESOLUTION_SCHEMA_VERSION,
        CLOB_RESOLUTION_PARSER_VERSION,
    );
    let boundary_payload = serde_json::to_vec(&serde_json::json!({
        "kind": "daily_boundary",
        "cutoff_unix": CUTOFF_UNIX,
    }))
    .unwrap();
    let boundary = append_source(
        &mut source_writer,
        DAILY_BOUNDARY_SOURCE_ID,
        &boundary_payload,
        CUTOFF_UNIX,
        1,
        1,
    );
    let complete_page = append_source(
        &mut source_writer,
        ACTIVITY_POLL_SOURCE_ID,
        PAGE_PAYLOAD,
        CUTOFF_UNIX + 1,
        ACTIVITY_SCHEMA_VERSION,
        ACTIVITY_PARSER_VERSION,
    );
    drop(source_writer);

    let mut waiting = rebuild_reconciliation_obligations(&source_path, &state).unwrap();
    recover_daily_boundary(&source_path, &paper_path, &mut waiting).unwrap();
    assert_eq!(
        waiting.pending_boundary(),
        Some(PendingBoundary {
            cutoff_unix: CUTOFF_UNIX,
            receipt: boundary,
        })
    );
    assert!(!waiting.boundary_ready());

    let observation = parse_activity_trade_observation(WS_PAYLOAD).unwrap();
    state
        .commit_activity_bucket(&ActivityBucketCommit {
            wallet: observation.wallet,
            source_epoch: observation.source_time.0.unix_timestamp(),
            dispositions: vec![ActivityDispositionRecord {
                source_trade_id: observation.group_id.key().clone(),
                transaction_hash: "0xabc".to_owned(),
                wallet: observation.wallet,
                source_epoch: observation.source_time.0.unix_timestamp(),
                semantic_revision: "scenario-causal-mark-v1".to_owned(),
                activity_type: "TRADE".to_owned(),
                disposition: "decision_pending".to_owned(),
                proof_json: "{\"version\":1}".to_owned(),
                no_copy: None,
            }],
            leader_positions: Vec::new(),
            gate_results: Vec::new(),
            history_effects: Vec::new(),
            history_status: None,
            pending: Vec::new(),
            fence: None,
            reanchor: None,
            advance_cursor: false,
        })
        .unwrap();
    let mut acknowledged = rebuild_reconciliation_obligations(&source_path, &state).unwrap();
    recover_daily_boundary(&source_path, &paper_path, &mut acknowledged).unwrap();
    assert_eq!(
        acknowledged.take_ready_boundary(),
        Some(PendingBoundary {
            cutoff_unix: CUTOFF_UNIX,
            receipt: boundary,
        })
    );

    let economic = economic(websocket, complete_page);
    let prepared = append_paper(
        &mut paper_writer,
        &PaperLogRecord::FinancialPrepared {
            expected_authority: ExpectedAuthority {
                qualification_start_receipt: start,
                prior_completed_prepared_sequence: None,
            },
            payload: FinancialPayload::Fill {
                operation: PaperFillOperationIdentity {
                    leader_wallet: observation.wallet,
                    source_trade_id: observation.group_id.key().clone(),
                    observed_at_bucket: observation.source_time.0.unix_timestamp(),
                },
                economic: economic.clone(),
            },
        },
        CUTOFF_UNIX + 1,
    );
    let canonical = CanonicalFillResult {
        outcome: "applied".to_owned(),
        bankroll: dec!(99),
        applied_prepared_seq: prepared.sequence,
        quantity: economic.sizing.expected_shares,
        principal: economic.sizing.principal,
        fee: economic.fee.expected_fee,
        fill_price: economic.sizing.expected_vwap,
    };
    state
        .apply_financial_fill(
            start,
            None,
            prepared.sequence,
            websocket,
            CUTOFF_UNIX - 2,
            &FillRecord {
                idempotency_key: "causal-late-fill".to_owned(),
                market_id: MarketId(VenueMarketId(economic.market.market_id.clone())),
                outcome_id: OutcomeId(0),
                side: Side::Buy,
                quantity: canonical.quantity,
                fill_price: canonical.fill_price,
                principal: canonical.principal,
                fee: canonical.fee,
            },
            canonical.bankroll,
        )
        .unwrap();
    append_paper(
        &mut paper_writer,
        &PaperLogRecord::FinancialFinal {
            prepared_receipt: prepared,
            result: FinancialResult::Fill { canonical },
        },
        CUTOFF_UNIX + 1,
    );

    let payout_json = "[\"1\",\"0\"]";
    let resolution_prepared = append_paper(
        &mut paper_writer,
        &PaperLogRecord::FinancialPrepared {
            expected_authority: ExpectedAuthority {
                qualification_start_receipt: start,
                prior_completed_prepared_sequence: Some(prepared.sequence),
            },
            payload: FinancialPayload::Resolution {
                condition_id: economic.market.condition_id.clone(),
                payout_by_outcome_index_json: payout_json.to_owned(),
                resolution_source_receipt: resolution,
            },
        },
        CUTOFF_UNIX + 1,
    );
    let resolution_credit = CollateralAmount::from_decimal_exact(dec!(2)).unwrap();
    state
        .apply_financial_resolution(
            start,
            Some(prepared.sequence),
            resolution_prepared.sequence,
            &MarketId(VenueMarketId(economic.market.market_id.clone())),
            payout_json,
            resolution,
            CUTOFF_UNIX - 1,
            resolution_credit,
            dec!(101),
        )
        .unwrap();
    append_paper(
        &mut paper_writer,
        &PaperLogRecord::FinancialFinal {
            prepared_receipt: resolution_prepared,
            result: FinancialResult::Resolution {
                canonical: CanonicalResolutionResult {
                    outcome: "applied".to_owned(),
                    bankroll: dec!(101),
                    applied_prepared_seq: resolution_prepared.sequence,
                    credit: resolution_credit,
                    settled_at_unix: CUTOFF_UNIX - 1,
                },
            },
        },
        CUTOFF_UNIX + 1,
    );

    let era = paper_era(scan_paper_log(&paper_path).unwrap());
    let completed =
        completed_prepared_before_boundary(&era, &source_path, CUTOFF_UNIX, boundary).unwrap();
    assert_eq!(
        completed,
        HashSet::from([prepared.sequence, resolution_prepared.sequence])
    );
    let snapshot = state
        .financial_snapshot_before_source_bound(
            CUTOFF_UNIX,
            boundary.sequence,
            &[prepared.sequence, resolution_prepared.sequence],
        )
        .unwrap();
    assert_eq!(snapshot.cash, dec!(101));
    assert!(snapshot.positions.is_empty());
    assert_eq!(
        snapshot.last_prepared_seq,
        Some(resolution_prepared.sequence)
    );

    let source_tail = TailBinding::from(&Scanner::verify(&source_path).unwrap());
    assert_eq!(source_tail.last_sequence, Some(complete_page.sequence));

    run_real_boundary(
        &paper_path,
        &source_path,
        paper_writer,
        Arc::new(state),
        boundary,
    )
    .await;

    let offline_era = paper_era(scan_paper_log(&paper_path).unwrap());
    let offline_completed =
        completed_prepared_before_boundary(&offline_era, &source_path, CUTOFF_UNIX, boundary)
            .unwrap();
    let mut offline_sequences = offline_completed.iter().copied().collect::<Vec<_>>();
    offline_sequences.sort_by_key(|sequence| sequence.0);
    let offline = PaperStateDb::open_read_only(&state_path)
        .unwrap()
        .financial_snapshot_before_source_bound(CUTOFF_UNIX, boundary.sequence, &offline_sequences)
        .unwrap();
    assert_eq!(offline.cash, snapshot.cash);
    assert_eq!(offline.positions, snapshot.positions);
    let marks = offline_era
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame {
            PaperLogFrame::Record(PaperLogRecord::PortfolioMark(mark)) => Some(mark.as_ref()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(marks.len(), 1);
    assert_eq!(
        marks[0].source_tail.last_sequence,
        Some(complete_page.sequence)
    );
    assert_eq!(
        marks[0].financial_prefix_seq,
        Some(resolution_prepared.sequence)
    );
    assert_eq!(marks[0].cash, dec!(101));
    assert_eq!(marks[0].equity, dec!(101));
    assert!(marks[0].prices.is_empty());
    assert_eq!(marks[0].invalid, None);
    assert_eq!(marks[0].financial_prefix_seq, offline.last_prepared_seq);
}
